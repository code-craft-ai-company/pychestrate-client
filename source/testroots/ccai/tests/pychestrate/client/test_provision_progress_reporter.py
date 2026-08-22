"""
    TestBase tests for the pychestrate-client provision progress reporter.
    The REST client is mocked and the local provision record is written into
    a temporary state directory; no real network traffic or provisioning is
    performed.
"""

import json
import os
import tempfile

from ccai import testbase

from ccai.pychestrate.client.progress_reporter import (
    FRAME_KIND_PROVISION,
    ProgressReporterAspects,
)
from ccai.pychestrate.client.provision_progress_reporter import (
    DIRNAME_PROVISIONING,
    PROVISION_PROGRESS_FAILED,
    PROVISION_PROGRESS_RUNNING,
    PROVISION_PROGRESS_STARTED,
    PROVISION_PROGRESS_SUCCEEDED,
    ProvisionProgressReporter,
)
from ccai.pychestrate.client.state import ClientState


RUN_ID = "88888888-8888-8888-8888-888888888888"
CLIENT_ID = "99999999-9999-9999-9999-999999999999"


class _FakeProgressGroup:
    """
        Records every progress frame posted to the reporter's REST client.

        ``fail_posts`` fails the named 1-based POST ordinals so a test can
        observe how an undeliverable frame is carried forward.
    """

    def __init__(self, *, fail_posts=None):
        failing_ordinals = fail_posts
        if failing_ordinals is None:
            failing_ordinals = []

        self.frames = []
        self._fail_posts = list(failing_ordinals)
        self._post_count = 0
        return

    def progress_post_frame(self, run_id, body, **kwargs):
        self.frames.append((run_id, body))
        self._post_count += 1
        if self._post_count in self._fail_posts:
            raise RuntimeError("simulated progress transport failure")
        return None


class _FakeRestClient:
    """
        Minimal stand-in carrying only the ``progress`` group.
    """

    def __init__(self, *, fail_posts=None):
        self.progress = _FakeProgressGroup(fail_posts=fail_posts)
        return


def _no_delay_aspects():
    """
        Returns aspects whose retry never really sleeps.
    """
    def _fake_sleep(seconds):
        return None

    aspects = ProgressReporterAspects(
        poll_interval_seconds=0.01, max_post_attempts=1,
        post_retry_delay_seconds=0.0, sleep_func=_fake_sleep)
    return aspects


def _posted_frames(client):
    """
        Returns the posted frame bodies as ``(frame_kind, frame)`` tuples.
    """
    rtnval = []
    for _, body in client.progress.frames:
        posted = body.to_dict()
        rtnval.append((posted["frame_kind"], posted["frame"]))
    return rtnval


def _read_record(state_dir):
    """
        Reads the local provision record written for ``RUN_ID``.
    """
    record_path = os.path.join(
        state_dir, DIRNAME_PROVISIONING, RUN_ID, "provision-record.json")
    with open(record_path, "r", encoding="utf-8") as recf:
        rtnval = json.load(recf)
    return rtnval


def test_provision_reporter_posts_a_frame_for_every_lifecycle_transition():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        client = _FakeRestClient()

        reporter = ProvisionProgressReporter(
            state=state, run_id=RUN_ID, client_id=CLIENT_ID, client=client,
            aspects=_no_delay_aspects())
        reporter.report_running()
        reporter.report_finished(succeeded=True)

        posted = _posted_frames(client)
        testbase.assert_equal(len(posted), 3)

        for frame_kind, frame in posted:
            testbase.assert_equal(frame_kind, FRAME_KIND_PROVISION)
            testbase.assert_equal(frame["run_id"], RUN_ID)
            testbase.assert_equal(frame["client_id"], CLIENT_ID)
            testbase.assert_equal(frame["run_kind"], FRAME_KIND_PROVISION)

        _, started_frame = posted[0]
        _, running_frame = posted[1]
        _, finished_frame = posted[2]

        testbase.assert_equal(started_frame["frame_index"], 0)
        testbase.assert_equal(
            started_frame["phase"], PROVISION_PROGRESS_STARTED)
        testbase.assert_equal(started_frame["status"], "running")

        testbase.assert_equal(running_frame["frame_index"], 1)
        testbase.assert_equal(
            running_frame["phase"], PROVISION_PROGRESS_RUNNING)
        testbase.assert_equal(running_frame["status"], "running")

        testbase.assert_equal(finished_frame["frame_index"], 2)
        testbase.assert_equal(
            finished_frame["phase"], PROVISION_PROGRESS_SUCCEEDED)
        testbase.assert_equal(finished_frame["status"], "succeeded")
    return


def test_provision_reporter_reports_a_failed_run_as_a_failed_frame():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        client = _FakeRestClient()

        reporter = ProvisionProgressReporter(
            state=state, run_id=RUN_ID, client_id=CLIENT_ID, client=client,
            aspects=_no_delay_aspects())
        reporter.report_running()
        reporter.report_finished(succeeded=False)

        posted = _posted_frames(client)
        _, finished_frame = posted[2]

        testbase.assert_equal(
            finished_frame["phase"], PROVISION_PROGRESS_FAILED)
        testbase.assert_equal(finished_frame["status"], "failed")
    return


def test_provision_reporter_omits_a_percentage_it_does_not_know():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        client = _FakeRestClient()

        reporter = ProvisionProgressReporter(
            state=state, run_id=RUN_ID, client_id=CLIENT_ID, client=client,
            aspects=_no_delay_aspects())
        reporter.report_running()

        posted = _posted_frames(client)
        for _, frame in posted:
            testbase.assert_false("percent_complete" in frame)

        # A caller that genuinely knows the ratio can still carry it.
        reporter.report_finished(succeeded=True, percent_complete=100.0)
        posted = _posted_frames(client)
        _, finished_frame = posted[2]
        testbase.assert_equal(finished_frame["percent_complete"], 100.0)
    return


def test_provision_reporter_resends_an_undelivered_frame_unchanged():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        # The first POST (the 'started' frame) fails.
        client = _FakeRestClient(fail_posts=[1])

        reporter = ProvisionProgressReporter(
            state=state, run_id=RUN_ID, client_id=CLIENT_ID, client=client,
            aspects=_no_delay_aspects())
        testbase.assert_equal(reporter.pending_frame_indexes, [0])

        reporter.report_running()

        posted = _posted_frames(client)
        testbase.assert_equal(len(posted), 3)

        _, failed_attempt = posted[0]
        _, resent_frame = posted[1]
        _, running_frame = posted[2]

        testbase.assert_equal(failed_attempt["frame_index"], 0)
        testbase.assert_equal(resent_frame["frame_index"], 0)
        testbase.assert_equal(
            resent_frame["phase"], PROVISION_PROGRESS_STARTED)
        testbase.assert_equal(
            resent_frame["recorded_at"], failed_attempt["recorded_at"])
        testbase.assert_equal(running_frame["frame_index"], 1)

        testbase.assert_equal(reporter.pending_frame_indexes, [])
    return


def test_provision_reporter_keeps_the_local_record_as_the_audit_trail():
    with tempfile.TemporaryDirectory() as tmpdir:
        state_dir = os.path.join(tmpdir, "state")
        state = ClientState(state_dir=state_dir)
        client = _FakeRestClient(fail_posts=[1, 2])

        reporter = ProvisionProgressReporter(
            state=state, run_id=RUN_ID, client_id=CLIENT_ID, client=client,
            aspects=_no_delay_aspects())
        reporter.report_running()

        record = _read_record(state_dir)
        progress = record["progress"]

        testbase.assert_equal(record["run_id"], RUN_ID)
        testbase.assert_equal(record["client_id"], CLIENT_ID)
        testbase.assert_equal(progress["frame_kind"], FRAME_KIND_PROVISION)
        testbase.assert_equal(len(progress["transitions"]), 2)
        testbase.assert_equal(
            progress["transitions"][0]["state"], PROVISION_PROGRESS_STARTED)
        testbase.assert_equal(progress["transitions"][0]["frame_index"], 0)

        # The frames the service never accepted are named locally so an
        # operator can tell the live view is missing them.
        testbase.assert_equal(progress["undelivered_frame_indexes"], [0, 1])
    return


def test_provision_reporter_without_a_client_records_locally_only():
    with tempfile.TemporaryDirectory() as tmpdir:
        state_dir = os.path.join(tmpdir, "state")
        state = ClientState(state_dir=state_dir)

        reporter = ProvisionProgressReporter(
            state=state, run_id=RUN_ID, client_id=CLIENT_ID)
        reporter.report_running()
        reporter.report_finished(succeeded=True)

        testbase.assert_equal(len(reporter.transitions), 3)
        testbase.assert_equal(reporter.pending_frame_indexes, [])

        record = _read_record(state_dir)
        testbase.assert_equal(len(record["progress"]["transitions"]), 3)
    return

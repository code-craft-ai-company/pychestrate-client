"""
    TestBase tests for the pychestrate-client provision output upload.
    These tests verify the M4 presigned-upload machinery is reused as-is
    for a provision run's output directory and that a provision-flavored
    ``ResultManifest`` is posted. No real HTTP traffic is performed -- the
    result-store driver is injected.
"""

import os
import tempfile

from ccai import testbase

from ccai.pychestrate.client.result_uploader import (
    UPLOAD_KIND_PROVISION,
    UPLOAD_STATUS_COMPLETE,
    PROVISION_RESULT_FORMAT_TAG,
    ResultStoreDriver,
    ResultUploaderAspects,
    find_pending_uploads,
    retry_pending_uploads,
    upload_provision_results,
)
from ccai.pychestrate.client.state import ClientState


def _fast_retry_aspects(*, store_driver=None):
    """
        Returns aspects whose retry backoff is zero so a retry is due
        immediately and the tests never sleep.
    """
    aspects = ResultUploaderAspects(
        store_driver=store_driver,
        retry_interval_seconds=0.0,
        retry_backoff_factor=1.0,
        retry_max_interval_seconds=0.0)
    return aspects


def _provision_envelope():
    """
        Returns a verified ``job_kind=provision`` envelope dict.
    """
    envelope = {
        "envelope_id": "11111111-1111-1111-1111-111111111111",
        "run_id": "44444444-4444-4444-4444-444444444444",
        "client_id": "55555555-5555-5555-5555-555555555555",
        "job_kind": "provision",
        "params": {
            "environment_id": "env-prod-1",
            "action": "apply",
            "driver_name": "terraform",
            "driver_inputs": {"working_directory": "/opt/infra"},
        },
        "results": {
            "backend_key_prefix": "provisioning/run-44444444",
            "backend_driver": "s3",
        },
    }
    return envelope


class _RecordingStoreDriver(ResultStoreDriver):
    """
        Records the tree it was asked to transfer; never touches a network.
    """

    def __init__(self):
        self.put_tree_calls = []
        return

    def put_tree(self, *, run_id, backend_driver, backend_key_prefix,
                 entries):
        self.put_tree_calls.append({
            "run_id": run_id,
            "backend_driver": backend_driver,
            "backend_key_prefix": backend_key_prefix,
            "entry_count": len(entries),
        })
        return


class _FakeResultsGroup:
    def __init__(self):
        self.manifests = []
        return

    def results_post_complete(self, run_id, body, **kwargs):
        self.manifests.append((run_id, body))
        return None


class _FakeRestClient:
    def __init__(self):
        self.results = _FakeResultsGroup()
        self._session = object()
        return

    def authenticate(self):
        return


def test_upload_provision_results_reuses_driver_and_posts_manifest():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        # A representative provision output directory tree.
        output_dir = os.path.join(tmpdir, "provision-out")
        os.makedirs(output_dir)
        with open(os.path.join(output_dir, "provision-stdout.log"),
                  "w") as logf:
            logf.write("Apply complete!\n")
        with open(os.path.join(output_dir, "provision-summary.json"),
                  "w") as sumf:
            sumf.write("{}\n")

        driver = _RecordingStoreDriver()
        client = _FakeRestClient()
        aspects = ResultUploaderAspects(store_driver=driver)

        outcome = upload_provision_results(
            client=client, state=state, envelope=_provision_envelope(),
            result_dir=output_dir, run_succeeded=True, aspects=aspects)

        testbase.assert_true(outcome.succeeded)
        testbase.assert_equal(outcome.status, UPLOAD_STATUS_COMPLETE)
        testbase.assert_equal(outcome.file_count, 2)

        # The presigned-upload machinery (put_tree) was reused as-is.
        testbase.assert_equal(len(driver.put_tree_calls), 1)
        testbase.assert_equal(driver.put_tree_calls[0]["entry_count"], 2)

        # A provision-flavored ResultManifest was posted.
        testbase.assert_equal(len(client.results.manifests), 1)
        _, manifest = client.results.manifests[0]
        testbase.assert_equal(
            manifest.format_tag, PROVISION_RESULT_FORMAT_TAG)
        testbase.assert_equal(manifest.terminal_outcome, "success")

        # The upload outcome is recorded on the provision record.
        record_path = os.path.join(
            state.state_dir, "provisioning",
            "44444444-4444-4444-4444-444444444444", "provision-record.json")
        testbase.assert_true(os.path.isfile(record_path))
    return


def test_upload_provision_results_marks_failed_on_transfer_error():
    class _FailingDriver(ResultStoreDriver):
        def put_tree(self, *, run_id, backend_driver, backend_key_prefix,
                     entries):
            raise RuntimeError("object store unreachable")

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        output_dir = os.path.join(tmpdir, "provision-out")
        os.makedirs(output_dir)
        with open(os.path.join(output_dir, "provision-summary.json"),
                  "w") as sumf:
            sumf.write("{}\n")

        client = _FakeRestClient()
        aspects = _fast_retry_aspects(store_driver=_FailingDriver())

        # Must NOT raise -- a failed transfer is captured as upload_failed.
        outcome = upload_provision_results(
            client=client, state=state, envelope=_provision_envelope(),
            result_dir=output_dir, run_succeeded=True, aspects=aspects)

        testbase.assert_false(outcome.succeeded)

        # And no provision completion is claimed against output files that
        # never reached the object store.
        testbase.assert_false(outcome.manifest_posted)
        testbase.assert_equal(len(client.results.manifests), 0)
    return


class _FlakyStoreDriver(ResultStoreDriver):
    """
        Fails its first ``fail_times`` transfers, then succeeds.
    """

    def __init__(self, *, fail_times):
        self.attempts = 0
        self._fail_times = fail_times
        return

    def put_tree(self, *, run_id, backend_driver, backend_key_prefix,
                 entries):
        self.attempts += 1
        if self.attempts <= self._fail_times:
            raise RuntimeError("simulated transient object-store failure")
        return None


def test_retry_pending_uploads_recovers_a_stranded_provision_run():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        output_dir = os.path.join(tmpdir, "provision-out")
        os.makedirs(output_dir)
        with open(os.path.join(output_dir, "provision-stdout.log"),
                  "w") as logf:
            logf.write("applying...\n")

        client = _FakeRestClient()
        driver = _FlakyStoreDriver(fail_times=1)
        aspects = _fast_retry_aspects(store_driver=driver)

        first = upload_provision_results(
            client=client, state=state, envelope=_provision_envelope(),
            result_dir=output_dir, run_succeeded=True, aspects=aspects)
        testbase.assert_false(first.succeeded)
        testbase.assert_equal(len(client.results.manifests), 0)

        pending_uploads = find_pending_uploads(state=state)
        testbase.assert_equal(len(pending_uploads), 1)
        testbase.assert_equal(pending_uploads[0].kind, UPLOAD_KIND_PROVISION)

        outcomes = retry_pending_uploads(
            client=client, state=state, aspects=aspects)

        testbase.assert_equal(len(outcomes), 1)
        testbase.assert_true(outcomes[0].succeeded)
        testbase.assert_equal(outcomes[0].status, UPLOAD_STATUS_COMPLETE)

        testbase.assert_equal(len(client.results.manifests), 1)
        _, manifest = client.results.manifests[0]
        testbase.assert_equal(
            manifest.format_tag, PROVISION_RESULT_FORMAT_TAG)

        testbase.assert_equal(len(find_pending_uploads(state=state)), 0)
    return


def test_upload_provision_results_marks_failure_terminal_outcome():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        output_dir = os.path.join(tmpdir, "provision-out")
        os.makedirs(output_dir)
        with open(os.path.join(output_dir, "provision-stderr.log"),
                  "w") as errf:
            errf.write("Error: apply failed\n")

        driver = _RecordingStoreDriver()
        client = _FakeRestClient()
        aspects = ResultUploaderAspects(store_driver=driver)

        outcome = upload_provision_results(
            client=client, state=state, envelope=_provision_envelope(),
            result_dir=output_dir, run_succeeded=False, aspects=aspects)

        testbase.assert_true(outcome.succeeded)
        _, manifest = client.results.manifests[0]
        testbase.assert_equal(manifest.terminal_outcome, "failure")
    return

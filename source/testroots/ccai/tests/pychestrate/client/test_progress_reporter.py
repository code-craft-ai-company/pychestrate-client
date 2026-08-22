"""
    TestBase tests for the pychestrate-client progress reporter. The REST
    client is mocked and the testbase JSOS stream file is written by the
    test; no real network traffic or testbase invocation is performed.
"""

import os
import tempfile
import time

from ccai import testbase

from ccai.errors.exceptions import BadParameterError

from ccai.pychestrate.client.progress_reporter import (
    CONSECUTIVE_FAILURE_ALERT_THRESHOLD,
    FRAME_KIND_BUILD,
    FRAME_KIND_PROVISION,
    ProgressReporter,
    ProgressReporterAspects,
    TESTBASE_RESULTS_STREAM_FILENAME,
    build_generalized_frame,
    build_progress_frame,
    flush_progress_frames,
    parse_stream_records,
    poll_and_post_progress,
    post_progress_frame,
    post_progress_frame_with_retry,
    translate_record,
)


#: ASCII Record Separator the testbase JSOS format uses to delimit records.
_RS = "\x1e"

RUN_ID = "44444444-4444-4444-4444-444444444444"
CLIENT_ID = "55555555-5555-5555-5555-555555555555"


def _jsos_test_record(name, result, start="2026-05-19T12:00:00",
                       stop="2026-05-19T12:00:01"):
    """
        Returns a testbase-shaped JSOS test record dict.
    """
    record = {
        "name": name,
        "rtype": "TEST",
        "result": result,
        "start": start,
        "stop": stop,
    }
    return record


def _write_stream(output_dir, records):
    """
        Writes a list of JSOS record dicts to a ``testrun_results_stream.jsos``
        file under ``output_dir`` using the RS-delimited format.
    """
    import json

    stream_path = os.path.join(output_dir, TESTBASE_RESULTS_STREAM_FILENAME)
    chunks = []
    for record in records:
        chunks.append(json.dumps(record))

    with open(stream_path, "w", encoding="utf-8") as streamf:
        streamf.write(_RS.join(chunks))

    return stream_path


class _FakeProgressGroup:
    """
        Records every progress frame posted to the reporter's REST client.

        ``raise_on_post`` fails every POST; ``fail_first`` fails only the
        leading N POSTs so a test can observe a recovery; ``fail_posts``
        fails the named 1-based POST ordinals.
    """

    def __init__(self, *, raise_on_post=False, fail_first=0,
                 fail_posts=None):
        failing_ordinals = fail_posts
        if failing_ordinals is None:
            failing_ordinals = []

        self.frames = []
        self._raise_on_post = raise_on_post
        self._fail_first = fail_first
        self._fail_posts = list(failing_ordinals)
        self._post_count = 0
        return

    def progress_post_frame(self, run_id, body, **kwargs):
        self.frames.append((run_id, body))
        self._post_count += 1
        if self._raise_on_post:
            raise RuntimeError("simulated progress transport failure")
        if self._post_count <= self._fail_first:
            raise RuntimeError("simulated transient progress failure")
        if self._post_count in self._fail_posts:
            raise RuntimeError("simulated transient progress failure")
        return None


class _FakeRestClient:
    """
        Minimal stand-in carrying only the ``progress`` group.
    """

    def __init__(self, *, raise_on_post=False, fail_first=0,
                 fail_posts=None):
        self.progress = _FakeProgressGroup(
            raise_on_post=raise_on_post, fail_first=fail_first,
            fail_posts=fail_posts)
        return


def _no_retry_aspects(*, max_post_attempts=1):
    """
        Returns aspects that never really sleep, so a retry test runs at
        full speed.
    """
    def _fake_sleep(seconds):
        return None

    aspects = ProgressReporterAspects(
        poll_interval_seconds=0.01, max_post_attempts=max_post_attempts,
        post_retry_delay_seconds=0.0, sleep_func=_fake_sleep)
    return aspects


def test_aspects_rejects_bad_poll_interval():
    raised = False
    try:
        ProgressReporterAspects(poll_interval_seconds=0)
    except BadParameterError:
        raised = True
    testbase.assert_true(raised)
    return


def test_parse_stream_records_handles_missing_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        stream_path = os.path.join(tmpdir, TESTBASE_RESULTS_STREAM_FILENAME)
        entries = parse_stream_records(stream_path=stream_path)
        testbase.assert_equal(len(entries), 0)
    return


def test_parse_stream_records_returns_only_test_entries():
    with tempfile.TemporaryDirectory() as tmpdir:
        records = [
            {"name": "job", "rtype": "JOB", "result": "RUNNING"},
            _jsos_test_record("test_a", "PASSED"),
            _jsos_test_record("test_b", "FAILED"),
        ]
        stream_path = _write_stream(tmpdir, records)
        entries = parse_stream_records(stream_path=stream_path)
        testbase.assert_equal(len(entries), 2)
        testbase.assert_equal(entries[0]["name"], "test_a")
        testbase.assert_equal(entries[1]["name"], "test_b")
    return


def test_translate_record_maps_jsos_fields_to_progress_record():
    jsos_entry = _jsos_test_record("test_alpha", "PASSED")
    record = translate_record(
        run_id=RUN_ID, client_id=CLIENT_ID, record_index=3,
        jsos_entry=jsos_entry)

    testbase.assert_equal(record.run_id, RUN_ID)
    testbase.assert_equal(record.client_id, CLIENT_ID)
    testbase.assert_equal(record.record_index, 3)
    testbase.assert_equal(record.test_name, "test_alpha")
    testbase.assert_equal(record.result, "PASSED")
    testbase.assert_equal(record.started_at, "2026-05-19T12:00:00")
    testbase.assert_equal(record.finished_at, "2026-05-19T12:00:01")
    return


def test_build_progress_frame_summarizes_and_batches():
    all_entries = [
        _jsos_test_record("t1", "PASSED"),
        _jsos_test_record("t2", "FAILED"),
        _jsos_test_record("t3", "SKIPPED"),
        _jsos_test_record("t4", "ERRORED"),
    ]
    new_entries = all_entries[2:]

    frame = build_progress_frame(
        run_id=RUN_ID, client_id=CLIENT_ID, all_entries=all_entries,
        new_entries=new_entries, first_new_index=2)

    testbase.assert_equal(frame.summary.tests_total, 4)
    testbase.assert_equal(frame.summary.tests_passed, 1)
    testbase.assert_equal(frame.summary.tests_failed, 1)
    testbase.assert_equal(frame.summary.tests_skipped, 1)
    testbase.assert_equal(frame.summary.tests_errored, 1)

    testbase.assert_equal(len(frame.records), 2)
    testbase.assert_equal(frame.records[0].record_index, 2)
    testbase.assert_equal(frame.records[0].test_name, "t3")
    testbase.assert_equal(frame.records[1].record_index, 3)
    return


def test_poll_and_post_progress_posts_only_new_records():
    with tempfile.TemporaryDirectory() as tmpdir:
        client = _FakeRestClient()

        _write_stream(tmpdir, [
            _jsos_test_record("t1", "PASSED"),
            _jsos_test_record("t2", "PASSED"),
        ])
        outcome = poll_and_post_progress(
            client=client, run_id=RUN_ID, client_id=CLIENT_ID,
            output_dir=tmpdir, already_posted=0)
        testbase.assert_equal(outcome.cursor, 2)
        testbase.assert_true(outcome.attempted)
        testbase.assert_true(outcome.accepted)
        testbase.assert_equal(len(client.progress.frames), 1)

        # A second poll with no new records posts nothing.
        outcome = poll_and_post_progress(
            client=client, run_id=RUN_ID, client_id=CLIENT_ID,
            output_dir=tmpdir, already_posted=outcome.cursor)
        testbase.assert_equal(outcome.cursor, 2)
        testbase.assert_false(outcome.attempted)
        testbase.assert_equal(len(client.progress.frames), 1)

        # A third poll after new records appended posts only the delta.
        _write_stream(tmpdir, [
            _jsos_test_record("t1", "PASSED"),
            _jsos_test_record("t2", "PASSED"),
            _jsos_test_record("t3", "FAILED"),
        ])
        outcome = poll_and_post_progress(
            client=client, run_id=RUN_ID, client_id=CLIENT_ID,
            output_dir=tmpdir, already_posted=outcome.cursor)
        testbase.assert_equal(outcome.cursor, 3)
        testbase.assert_equal(len(client.progress.frames), 2)
        _, second_frame = client.progress.frames[1]
        testbase.assert_equal(len(second_frame.records), 1)
        testbase.assert_equal(second_frame.records[0].test_name, "t3")
    return


def test_poll_cursor_does_not_advance_when_the_post_fails():
    # The bug this guards: the cursor used to advance whether or not the
    # POST landed, so a transient failure dropped that batch of records
    # from the operator's live view permanently.
    with tempfile.TemporaryDirectory() as tmpdir:
        client = _FakeRestClient(raise_on_post=True)
        aspects = _no_retry_aspects()

        _write_stream(tmpdir, [
            _jsos_test_record("t1", "PASSED"),
            _jsos_test_record("t2", "PASSED"),
        ])
        outcome = poll_and_post_progress(
            client=client, run_id=RUN_ID, client_id=CLIENT_ID,
            output_dir=tmpdir, already_posted=0, aspects=aspects)

        testbase.assert_equal(outcome.cursor, 0)
        testbase.assert_true(outcome.attempted)
        testbase.assert_false(outcome.accepted)
        testbase.assert_equal(len(client.progress.frames), 1)
    return


def test_failed_batch_is_resent_with_the_same_record_indexes():
    # The service dedupes a progress record on (run_id, record_index), so a
    # re-send must carry the indexes the failed attempt carried.
    with tempfile.TemporaryDirectory() as tmpdir:
        client = _FakeRestClient(fail_first=1)
        aspects = _no_retry_aspects()

        _write_stream(tmpdir, [
            _jsos_test_record("t1", "PASSED"),
            _jsos_test_record("t2", "PASSED"),
        ])
        failed_outcome = poll_and_post_progress(
            client=client, run_id=RUN_ID, client_id=CLIENT_ID,
            output_dir=tmpdir, already_posted=0, aspects=aspects)
        testbase.assert_equal(failed_outcome.cursor, 0)

        # A record appended between the failed cycle and the retry must not
        # displace the records the failed frame carried.
        _write_stream(tmpdir, [
            _jsos_test_record("t1", "PASSED"),
            _jsos_test_record("t2", "PASSED"),
            _jsos_test_record("t3", "FAILED"),
        ])
        retry_outcome = poll_and_post_progress(
            client=client, run_id=RUN_ID, client_id=CLIENT_ID,
            output_dir=tmpdir, already_posted=failed_outcome.cursor,
            aspects=aspects)

        testbase.assert_equal(retry_outcome.cursor, 3)
        testbase.assert_true(retry_outcome.accepted)
        testbase.assert_equal(len(client.progress.frames), 2)

        _, failed_frame = client.progress.frames[0]
        _, resent_frame = client.progress.frames[1]

        testbase.assert_equal(failed_frame.records[0].record_index, 0)
        testbase.assert_equal(failed_frame.records[0].test_name, "t1")

        # The re-send starts at the same index the dropped frame started at
        # and carries the records that were never accepted.
        testbase.assert_equal(resent_frame.records[0].record_index, 0)
        testbase.assert_equal(resent_frame.records[0].test_name, "t1")
        testbase.assert_equal(len(resent_frame.records), 3)
        testbase.assert_equal(resent_frame.records[2].record_index, 2)
    return


def test_post_progress_frame_with_retry_is_bounded():
    frame = build_progress_frame(
        run_id=RUN_ID, client_id=CLIENT_ID,
        all_entries=[_jsos_test_record("t1", "PASSED")],
        new_entries=[_jsos_test_record("t1", "PASSED")],
        first_new_index=0)

    client = _FakeRestClient(raise_on_post=True)
    aspects = _no_retry_aspects(max_post_attempts=3)

    accepted = post_progress_frame_with_retry(
        client=client, run_id=RUN_ID, frame=frame, aspects=aspects)

    testbase.assert_false(accepted)
    testbase.assert_equal(len(client.progress.frames), 3)
    return


def test_post_progress_frame_with_retry_recovers_from_a_transient_failure():
    frame = build_progress_frame(
        run_id=RUN_ID, client_id=CLIENT_ID,
        all_entries=[_jsos_test_record("t1", "PASSED")],
        new_entries=[_jsos_test_record("t1", "PASSED")],
        first_new_index=0)

    client = _FakeRestClient(fail_first=1)
    aspects = _no_retry_aspects(max_post_attempts=3)

    accepted = post_progress_frame_with_retry(
        client=client, run_id=RUN_ID, frame=frame, aspects=aspects)

    testbase.assert_true(accepted)
    testbase.assert_equal(len(client.progress.frames), 2)
    return


def test_build_generalized_frame_carries_the_service_frame_contract():
    frame = build_generalized_frame(
        run_id=RUN_ID, client_id=CLIENT_ID, frame_kind=FRAME_KIND_BUILD,
        frame_index=4, phase="running", status="running",
        payload={"run_kind": FRAME_KIND_BUILD})

    body = frame.to_dict()
    testbase.assert_equal(body["frame_kind"], FRAME_KIND_BUILD)

    posted_frame = body["frame"]
    testbase.assert_equal(posted_frame["client_id"], CLIENT_ID)
    testbase.assert_equal(posted_frame["frame_index"], 4)
    testbase.assert_equal(posted_frame["phase"], "running")
    testbase.assert_equal(posted_frame["status"], "running")
    testbase.assert_equal(posted_frame["run_kind"], FRAME_KIND_BUILD)
    testbase.assert_true("recorded_at" in posted_frame)

    # A percentage the caller does not know is omitted, never invented.
    testbase.assert_false("percent_complete" in posted_frame)
    return


def test_build_generalized_frame_carries_a_known_percentage():
    frame = build_generalized_frame(
        run_id=RUN_ID, client_id=CLIENT_ID, frame_kind=FRAME_KIND_PROVISION,
        frame_index=1, phase="running", status="running",
        percent_complete=42.5, notes="apply step 2 of 5")

    posted_frame = frame.to_dict()["frame"]
    testbase.assert_equal(posted_frame["percent_complete"], 42.5)
    testbase.assert_equal(posted_frame["notes"], "apply step 2 of 5")
    return


def test_build_generalized_frame_rejects_a_test_frame_kind():
    raised = False
    try:
        build_generalized_frame(
            run_id=RUN_ID, client_id=CLIENT_ID, frame_kind="test",
            frame_index=0, phase="running", status="running")
    except BadParameterError:
        raised = True

    testbase.assert_true(raised)
    return


def test_flush_progress_frames_returns_the_undelivered_tail():
    frames = []
    for frame_index in range(3):
        frames.append(build_generalized_frame(
            run_id=RUN_ID, client_id=CLIENT_ID, frame_kind=FRAME_KIND_BUILD,
            frame_index=frame_index, phase="running", status="running"))

    client = _FakeRestClient(fail_posts=[2])
    aspects = _no_retry_aspects()

    pending = flush_progress_frames(
        client=client, run_id=RUN_ID, frames=frames, aspects=aspects)

    # Frame 0 landed, frame 1 did not, so the flush stopped there: posting
    # frame 2 ahead of frame 1 would reorder the run's frame stream.
    testbase.assert_equal(len(pending), 2)
    testbase.assert_equal(len(client.progress.frames), 2)
    testbase.assert_equal(pending[0].frame["frame_index"], 1)
    testbase.assert_equal(pending[1].frame["frame_index"], 2)

    pending = flush_progress_frames(
        client=client, run_id=RUN_ID, frames=pending, aspects=aspects)
    testbase.assert_equal(len(pending), 0)

    # The re-sent frame carries the index its failed attempt carried, which
    # is what makes the service-side dedupe a no-op instead of a duplicate.
    _, resent_body = client.progress.frames[2]
    testbase.assert_equal(resent_body.frame["frame_index"], 1)
    return


def test_post_progress_frame_failure_is_swallowed():
    # A progress POST that raises must NOT propagate -- progress is
    # best-effort and a failure must never abort the run.
    with tempfile.TemporaryDirectory() as tmpdir:
        client = _FakeRestClient(raise_on_post=True)
        frame = build_progress_frame(
            run_id=RUN_ID, client_id=CLIENT_ID,
            all_entries=[_jsos_test_record("t1", "PASSED")],
            new_entries=[_jsos_test_record("t1", "PASSED")],
            first_new_index=0)

        accepted = post_progress_frame(
            client=client, run_id=RUN_ID, frame=frame)
        testbase.assert_false(accepted)
        # The frame was still attempted.
        testbase.assert_equal(len(client.progress.frames), 1)
    return


def test_progress_reporter_streams_records_across_start_stop():
    # A failing progress transport must not prevent the reporter from
    # running or stopping cleanly.
    with tempfile.TemporaryDirectory() as tmpdir:
        client = _FakeRestClient()

        sleep_calls = {"count": 0}

        def _fake_sleep(seconds):
            sleep_calls["count"] += 1
            return None

        aspects = ProgressReporterAspects(
            poll_interval_seconds=0.01, sleep_func=_fake_sleep)
        reporter = ProgressReporter(
            client=client, run_id=RUN_ID, client_id=CLIENT_ID,
            output_dir=tmpdir, aspects=aspects)

        _write_stream(tmpdir, [_jsos_test_record("t1", "PASSED")])
        reporter.start()

        # Append more records, then stop -- stop performs a final poll so the
        # last records are not lost.
        _write_stream(tmpdir, [
            _jsos_test_record("t1", "PASSED"),
            _jsos_test_record("t2", "FAILED"),
        ])
        forwarded = reporter.stop()

        testbase.assert_equal(forwarded, 2)
        testbase.assert_true(len(client.progress.frames) >= 1)
    return


def test_progress_reporter_keeps_records_queued_while_delivery_fails():
    # With every POST failing the reporter must forward nothing, keep
    # re-sending the same batch from index 0, and surface the failure run
    # through its consecutive-failure count.
    with tempfile.TemporaryDirectory() as tmpdir:
        client = _FakeRestClient(raise_on_post=True)

        def _paced_sleep(seconds):
            # Paces the spin so the failing loop does not run unbounded
            # between start() and stop().
            time.sleep(0.01)
            return None

        aspects = ProgressReporterAspects(
            poll_interval_seconds=0.01, max_post_attempts=1,
            post_retry_delay_seconds=0.0, sleep_func=_paced_sleep)
        reporter = ProgressReporter(
            client=client, run_id=RUN_ID, client_id=CLIENT_ID,
            output_dir=tmpdir, aspects=aspects)

        _write_stream(tmpdir, [
            _jsos_test_record("t1", "PASSED"),
            _jsos_test_record("t2", "PASSED"),
        ])
        reporter.start()
        forwarded = reporter.stop()

        testbase.assert_equal(forwarded, 0)
        testbase.assert_equal(reporter.posted_count, 0)
        testbase.assert_true(reporter.consecutive_failures >= 1)
        testbase.assert_true(len(client.progress.frames) >= 1)
        testbase.assert_true(CONSECUTIVE_FAILURE_ALERT_THRESHOLD >= 1)

        # Every attempt re-sent the same batch from the same starting
        # index, so nothing was skipped and nothing is duplicated once the
        # service dedupes on (run_id, record_index).
        for _, attempted_frame in client.progress.frames:
            testbase.assert_equal(attempted_frame.records[0].record_index, 0)
            testbase.assert_equal(len(attempted_frame.records), 2)
    return

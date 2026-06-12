"""
    TestBase tests for the pychestrate-client progress reporter. The REST
    client is mocked and the testbase JSOS stream file is written by the
    test; no real network traffic or testbase invocation is performed.
"""

import os
import tempfile

from ccai import testbase

from ccai.errors.exceptions import BadParameterError

from ccai.pychestrate.client.progress_reporter import (
    ProgressReporter,
    ProgressReporterAspects,
    TESTBASE_RESULTS_STREAM_FILENAME,
    build_progress_frame,
    parse_stream_records,
    poll_and_post_progress,
    post_progress_frame,
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
    """

    def __init__(self, *, raise_on_post=False):
        self.frames = []
        self._raise_on_post = raise_on_post
        return

    def progress_post_frame(self, run_id, body, **kwargs):
        self.frames.append((run_id, body))
        if self._raise_on_post:
            raise RuntimeError("simulated progress transport failure")
        return None


class _FakeRestClient:
    """
        Minimal stand-in carrying only the ``progress`` group.
    """

    def __init__(self, *, raise_on_post=False):
        self.progress = _FakeProgressGroup(raise_on_post=raise_on_post)
        return


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
        cursor = poll_and_post_progress(
            client=client, run_id=RUN_ID, client_id=CLIENT_ID,
            output_dir=tmpdir, already_posted=0)
        testbase.assert_equal(cursor, 2)
        testbase.assert_equal(len(client.progress.frames), 1)

        # A second poll with no new records posts nothing.
        cursor = poll_and_post_progress(
            client=client, run_id=RUN_ID, client_id=CLIENT_ID,
            output_dir=tmpdir, already_posted=cursor)
        testbase.assert_equal(cursor, 2)
        testbase.assert_equal(len(client.progress.frames), 1)

        # A third poll after new records appended posts only the delta.
        _write_stream(tmpdir, [
            _jsos_test_record("t1", "PASSED"),
            _jsos_test_record("t2", "PASSED"),
            _jsos_test_record("t3", "FAILED"),
        ])
        cursor = poll_and_post_progress(
            client=client, run_id=RUN_ID, client_id=CLIENT_ID,
            output_dir=tmpdir, already_posted=cursor)
        testbase.assert_equal(cursor, 3)
        testbase.assert_equal(len(client.progress.frames), 2)
        _, second_frame = client.progress.frames[1]
        testbase.assert_equal(len(second_frame.records), 1)
        testbase.assert_equal(second_frame.records[0].test_name, "t3")
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

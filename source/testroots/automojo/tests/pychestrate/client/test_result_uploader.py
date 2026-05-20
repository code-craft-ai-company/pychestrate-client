"""
    TestBase tests for the pychestrate-client result uploader. The REST
    client and the shared-results object store are mocked; the testbase
    result tree is written by the test. No real network traffic or storage
    transfer is performed.
"""

import json
import os
import tempfile

from automojo import testbase

from automojo.pychestrate.client.result_uploader import (
    RESULT_FORMAT_TAG,
    ResultStoreDriver,
    ResultUploaderAspects,
    TERMINAL_OUTCOME_FAILURE,
    TERMINAL_OUTCOME_SUCCESS,
    UPLOAD_STATUS_COMPLETE,
    UPLOAD_STATUS_FAILED,
    build_result_manifest,
    run_needs_upload_retry,
    upload_result_tree,
    upload_run_results,
    walk_result_tree,
)
from automojo.pychestrate.client.state import ClientState


RUN_ID = "44444444-4444-4444-4444-444444444444"
CLIENT_ID = "55555555-5555-5555-5555-555555555555"


def _sample_envelope():
    """
        Returns a job-envelope dict carrying a results locator.
    """
    envelope = {
        "envelope_id": "11111111-1111-1111-1111-111111111111",
        "run_id": RUN_ID,
        "client_id": CLIENT_ID,
        "results": {
            "backend_key_prefix": "proj/results/2026/05/19/testbase/run-x",
            "backend_driver": "s3",
        },
    }
    return envelope


def _write_result_tree(result_dir):
    """
        Writes a representative testbase result tree under ``result_dir``.
    """
    os.makedirs(os.path.join(result_dir, "artifacts"), exist_ok=True)
    os.makedirs(os.path.join(result_dir, "diagnostics"), exist_ok=True)

    summary = {
        "result": "FAILED",
        "detail": {
            "passed": 7, "failed": 2, "skipped": 1, "errors": 1, "total": 11,
        },
    }
    with open(os.path.join(result_dir, "testrun_summary.json"), "w",
              encoding="utf-8") as sumf:
        json.dump(summary, sumf)

    with open(os.path.join(result_dir, "testrun_results_stream.jsos"), "w",
              encoding="utf-8") as streamf:
        streamf.write("{}")

    with open(os.path.join(result_dir, "testrun_results.json"), "w",
              encoding="utf-8") as resf:
        resf.write("{}")

    with open(os.path.join(result_dir, "testsummary.html"), "w",
              encoding="utf-8") as htmlf:
        htmlf.write("<html></html>")

    with open(os.path.join(result_dir, "artifacts", "console.log"), "w",
              encoding="utf-8") as logf:
        logf.write("log line")

    with open(os.path.join(result_dir, "diagnostics", "trace.txt"), "w",
              encoding="utf-8") as tracef:
        tracef.write("trace")

    return result_dir


class _RecordingStoreDriver(ResultStoreDriver):
    """
        A :class:`ResultStoreDriver` that records every transfer instead of
        touching a real object store.
    """

    def __init__(self, *, raise_on_path=None):
        self.transfers = []
        self._raise_on_path = raise_on_path
        return

    def put_object(self, *, backend_driver, backend_key_prefix,
                   relative_path, local_path):
        if self._raise_on_path is not None and \
                relative_path == self._raise_on_path:
            raise RuntimeError("simulated object-store failure")
        self.transfers.append((backend_key_prefix, relative_path))
        return None


class _FakeResultsGroup:
    """
        Records every result manifest posted, optionally failing the POST.
    """

    def __init__(self, *, raise_on_post=False):
        self.manifests = []
        self._raise_on_post = raise_on_post
        return

    def results_post_complete(self, run_id, body, **kwargs):
        self.manifests.append((run_id, body))
        if self._raise_on_post:
            raise RuntimeError("simulated results/complete failure")
        return None


class _FakeRestClient:
    """
        Minimal stand-in carrying only the ``results`` group.
    """

    def __init__(self, *, raise_on_post=False):
        self.results = _FakeResultsGroup(raise_on_post=raise_on_post)
        return


def _make_state(state_dir):
    """
        Returns a :class:`ClientState` bound to a fresh state directory.
    """
    state = ClientState(state_dir=state_dir)
    state.ensure_dir()
    return state


def test_walk_result_tree_handles_missing_directory():
    with tempfile.TemporaryDirectory() as tmpdir:
        entries = walk_result_tree(result_dir=os.path.join(tmpdir, "absent"))
        testbase.assert_equal(len(entries), 0)
    return


def test_walk_result_tree_enumerates_files_with_digests():
    with tempfile.TemporaryDirectory() as tmpdir:
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        entries = walk_result_tree(result_dir=result_dir)

        relative_paths = [entry.relative_path for entry in entries]
        testbase.assert_equal(len(entries), 6)
        testbase.assert_true("testrun_summary.json" in relative_paths)
        testbase.assert_true("artifacts/console.log" in relative_paths)
        testbase.assert_true("diagnostics/trace.txt" in relative_paths)

        for entry in entries:
            testbase.assert_true(entry.size_bytes >= 0)
            testbase.assert_equal(len(entry.sha256), 64)
    return


def test_build_result_manifest_carries_counts_and_paths():
    with tempfile.TemporaryDirectory() as tmpdir:
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        entries = walk_result_tree(result_dir=result_dir)

        counts = {"passed": 7, "failed": 2, "skipped": 1, "errored": 1}
        manifest = build_result_manifest(
            run_id=RUN_ID, client_id=CLIENT_ID,
            terminal_outcome=TERMINAL_OUTCOME_FAILURE,
            backend_driver="s3", backend_key_prefix="proj/run-x",
            counts=counts, entries=entries)

        testbase.assert_equal(manifest.run_id, RUN_ID)
        testbase.assert_equal(manifest.client_id, CLIENT_ID)
        testbase.assert_equal(manifest.format_tag, RESULT_FORMAT_TAG)
        testbase.assert_equal(manifest.backend_driver, "s3")
        testbase.assert_equal(manifest.backend_key_prefix, "proj/run-x")
        testbase.assert_equal(manifest.terminal_outcome,
                              TERMINAL_OUTCOME_FAILURE)
        testbase.assert_equal(manifest.tests_passed, 7)
        testbase.assert_equal(manifest.tests_failed, 2)
        testbase.assert_equal(manifest.summary_artifact_path,
                              "testrun_summary.json")
        testbase.assert_equal(manifest.extras["file_count"], 6)
    return


def test_upload_result_tree_without_driver_reports_failure():
    with tempfile.TemporaryDirectory() as tmpdir:
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        entries = walk_result_tree(result_dir=result_dir)

        uploaded = upload_result_tree(
            entries=entries, backend_driver="s3",
            backend_key_prefix="proj/run-x", store_driver=None)
        testbase.assert_false(uploaded)
    return


def test_upload_result_tree_with_driver_transfers_every_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        entries = walk_result_tree(result_dir=result_dir)
        driver = _RecordingStoreDriver()

        uploaded = upload_result_tree(
            entries=entries, backend_driver="s3",
            backend_key_prefix="proj/run-x", store_driver=driver)
        testbase.assert_true(uploaded)
        testbase.assert_equal(len(driver.transfers), 6)
    return


def test_upload_result_tree_failed_transfer_reports_failure():
    with tempfile.TemporaryDirectory() as tmpdir:
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        entries = walk_result_tree(result_dir=result_dir)
        driver = _RecordingStoreDriver(raise_on_path="diagnostics/trace.txt")

        uploaded = upload_result_tree(
            entries=entries, backend_driver="s3",
            backend_key_prefix="proj/run-x", store_driver=driver)
        testbase.assert_false(uploaded)
    return


def test_upload_run_results_complete_path_posts_manifest():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        client = _FakeRestClient()
        driver = _RecordingStoreDriver()
        aspects = ResultUploaderAspects(store_driver=driver)

        outcome = upload_run_results(
            client=client, state=state, envelope=_sample_envelope(),
            result_dir=result_dir, run_succeeded=True, aspects=aspects)

        testbase.assert_true(outcome.succeeded)
        testbase.assert_equal(outcome.status, UPLOAD_STATUS_COMPLETE)
        testbase.assert_true(outcome.tree_uploaded)
        testbase.assert_true(outcome.manifest_posted)
        testbase.assert_equal(outcome.file_count, 6)

        testbase.assert_equal(len(client.results.manifests), 1)
        posted_run_id, manifest = client.results.manifests[0]
        testbase.assert_equal(posted_run_id, RUN_ID)
        testbase.assert_equal(manifest.terminal_outcome,
                              TERMINAL_OUTCOME_SUCCESS)
        testbase.assert_equal(manifest.tests_passed, 7)

        # The local run record is NOT marked for retry on success.
        testbase.assert_false(run_needs_upload_retry(state=state,
                                                     run_id=RUN_ID))
    return


def test_upload_run_results_no_driver_marks_local_state_for_retry():
    # The result-upload mechanism gap: with no object-store driver wired the
    # byte transfer cannot complete; the run must be marked upload_failed so
    # the next 'run' invocation can retry.
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        client = _FakeRestClient()

        outcome = upload_run_results(
            client=client, state=state, envelope=_sample_envelope(),
            result_dir=result_dir, run_succeeded=True)

        testbase.assert_false(outcome.succeeded)
        testbase.assert_equal(outcome.status, UPLOAD_STATUS_FAILED)
        testbase.assert_false(outcome.tree_uploaded)
        # The completion manifest is still posted for reconciliation.
        testbase.assert_true(outcome.manifest_posted)

        testbase.assert_true(run_needs_upload_retry(state=state,
                                                    run_id=RUN_ID))
    return


def test_upload_run_results_failed_completion_post_marks_retry():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        client = _FakeRestClient(raise_on_post=True)
        driver = _RecordingStoreDriver()
        aspects = ResultUploaderAspects(store_driver=driver)

        outcome = upload_run_results(
            client=client, state=state, envelope=_sample_envelope(),
            result_dir=result_dir, run_succeeded=False, aspects=aspects)

        testbase.assert_false(outcome.succeeded)
        testbase.assert_equal(outcome.status, UPLOAD_STATUS_FAILED)
        testbase.assert_false(outcome.manifest_posted)
        testbase.assert_true(run_needs_upload_retry(state=state,
                                                    run_id=RUN_ID))
    return

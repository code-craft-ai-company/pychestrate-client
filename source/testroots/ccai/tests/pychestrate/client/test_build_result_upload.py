"""
    TestBase tests for the pychestrate-client build artifact upload. These
    tests verify the M4 presigned-upload machinery is reused as-is for a
    build's output directory and that a build-flavored ``ResultManifest``
    is posted. No real HTTP traffic is performed -- the result-store driver
    is injected.
"""

import json
import os
import tempfile

from ccai import testbase

from ccai.pychestrate.client.result_uploader import (
    RECORD_SECTION_UPLOAD_RETRY,
    UPLOAD_KIND_BUILD,
    UPLOAD_STATUS_COMPLETE,
    BUILD_RESULT_FORMAT_TAG,
    ResultStoreDriver,
    ResultUploaderAspects,
    find_pending_uploads,
    retry_pending_uploads,
    upload_build_results,
)
from ccai.pychestrate.client.state import ClientState


BUILD_RUN_ID = "44444444-4444-4444-4444-444444444444"


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


def _read_build_record(state):
    """
        Reads the local build record written by the uploader.
    """
    record_path = os.path.join(
        state.state_dir, "builds", BUILD_RUN_ID, "build-record.json")
    with open(record_path, "r", encoding="utf-8") as recf:
        record = json.load(recf)
    return record


def _write_build_output_tree(output_dir):
    """
        Writes a representative build output tree under ``output_dir``.
    """
    os.makedirs(os.path.join(output_dir, "artifacts"), exist_ok=True)
    with open(os.path.join(output_dir, "build-stdout.log"), "w") as logf:
        logf.write("compiling...\n")
    with open(os.path.join(output_dir, "artifacts", "app.tar"), "wb") as artf:
        artf.write(b"\x00\x01\x02")
    return output_dir


def _build_envelope():
    """
        Returns a verified ``job_kind=build`` envelope dict.
    """
    envelope = {
        "envelope_id": "11111111-1111-1111-1111-111111111111",
        "run_id": "44444444-4444-4444-4444-444444444444",
        "client_id": "55555555-5555-5555-5555-555555555555",
        "job_kind": "build",
        "params": {"runner_kind": "makefile"},
        "results": {
            "backend_key_prefix": "builds/run-44444444",
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


def test_upload_build_results_reuses_store_driver_and_posts_build_manifest():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        # A representative build output directory tree.
        output_dir = os.path.join(tmpdir, "build-out")
        os.makedirs(os.path.join(output_dir, "artifacts"))
        with open(os.path.join(output_dir, "build-stdout.log"), "w") as logf:
            logf.write("compiling...\n")
        with open(os.path.join(output_dir, "artifacts", "app.tar"),
                  "wb") as artf:
            artf.write(b"\x00\x01\x02")

        driver = _RecordingStoreDriver()
        client = _FakeRestClient()
        aspects = ResultUploaderAspects(store_driver=driver)

        outcome = upload_build_results(
            client=client, state=state, envelope=_build_envelope(),
            result_dir=output_dir, run_succeeded=True, aspects=aspects)

        testbase.assert_true(outcome.succeeded)
        testbase.assert_equal(outcome.status, UPLOAD_STATUS_COMPLETE)
        testbase.assert_equal(outcome.file_count, 2)

        # The presigned-upload machinery (put_tree) was reused as-is.
        testbase.assert_equal(len(driver.put_tree_calls), 1)
        testbase.assert_equal(driver.put_tree_calls[0]["entry_count"], 2)

        # A build-flavored ResultManifest was posted.
        testbase.assert_equal(len(client.results.manifests), 1)
        _, manifest = client.results.manifests[0]
        testbase.assert_equal(manifest.format_tag, BUILD_RESULT_FORMAT_TAG)
        testbase.assert_equal(manifest.terminal_outcome, "success")

        # The upload outcome is recorded on the build record.
        record_path = os.path.join(
            state.state_dir, "builds", "44444444-4444-4444-4444-444444444444",
            "build-record.json")
        testbase.assert_true(os.path.isfile(record_path))
    return


def test_upload_build_results_marks_failed_on_transfer_error():
    class _FailingDriver(ResultStoreDriver):
        def put_tree(self, *, run_id, backend_driver, backend_key_prefix,
                     entries):
            raise RuntimeError("object store unreachable")

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        output_dir = os.path.join(tmpdir, "build-out")
        os.makedirs(output_dir)
        with open(os.path.join(output_dir, "build-summary.json"),
                  "w") as sumf:
            sumf.write("{}\n")

        client = _FakeRestClient()
        aspects = _fast_retry_aspects(store_driver=_FailingDriver())

        # Must NOT raise -- a failed transfer is captured as upload_failed.
        outcome = upload_build_results(
            client=client, state=state, envelope=_build_envelope(),
            result_dir=output_dir, run_succeeded=True, aspects=aspects)

        testbase.assert_false(outcome.succeeded)

        # And no build completion is claimed against artifacts that never
        # reached the object store.
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


def test_retry_pending_uploads_recovers_a_stranded_build():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        output_dir = _write_build_output_tree(
            os.path.join(tmpdir, "build-out"))

        client = _FakeRestClient()
        driver = _FlakyStoreDriver(fail_times=1)
        aspects = _fast_retry_aspects(store_driver=driver)

        first = upload_build_results(
            client=client, state=state, envelope=_build_envelope(),
            result_dir=output_dir, run_succeeded=True, aspects=aspects)
        testbase.assert_false(first.succeeded)
        testbase.assert_equal(len(client.results.manifests), 0)

        record = _read_build_record(state=state)
        testbase.assert_equal(
            record[RECORD_SECTION_UPLOAD_RETRY]["kind"], UPLOAD_KIND_BUILD)

        pending_uploads = find_pending_uploads(state=state)
        testbase.assert_equal(len(pending_uploads), 1)
        testbase.assert_equal(pending_uploads[0].kind, UPLOAD_KIND_BUILD)

        outcomes = retry_pending_uploads(
            client=client, state=state, aspects=aspects)

        testbase.assert_equal(len(outcomes), 1)
        testbase.assert_true(outcomes[0].succeeded)
        testbase.assert_equal(outcomes[0].status, UPLOAD_STATUS_COMPLETE)

        testbase.assert_equal(len(client.results.manifests), 1)
        _, manifest = client.results.manifests[0]
        testbase.assert_equal(manifest.format_tag, BUILD_RESULT_FORMAT_TAG)

        testbase.assert_equal(len(find_pending_uploads(state=state)), 0)
    return

"""
    TestBase tests for the pychestrate-client result uploader. The REST
    client and the shared-results object store are mocked; the testbase
    result tree is written by the test. No real network traffic or storage
    transfer is performed.
"""

import json
import os
import tempfile

from ccai import testbase

from ccai.pychestrate.client.result_uploader import (
    RESULT_FORMAT_TAG,
    PresignedUploadDriver,
    ResultStoreDriver,
    ResultUploaderAspects,
    TERMINAL_OUTCOME_FAILURE,
    TERMINAL_OUTCOME_SUCCESS,
    UPLOAD_STATUS_COMPLETE,
    UPLOAD_STATUS_FAILED,
    build_result_manifest,
    run_needs_upload_retry,
    upload_result_tree,
    upload_rpa_results,
    upload_run_results,
    walk_result_tree,
)
from ccai.pychestrate.client.state import ClientState


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


class _FakeUploadTarget:
    """
        Mirrors the codegen'd ``Artifactuploadtarget`` shape.
    """

    def __init__(self, *, path, put_url, headers):
        self.path = path
        self.put_url = put_url
        self.expires_at = "2026-05-19T13:00:00+00:00"
        self.headers = headers
        return


class _FakePresignResponse:
    """
        Mirrors the codegen'd ``Artifactpresignresponse`` shape.
    """

    def __init__(self, *, run_id, uploads):
        self.run_id = run_id
        self.uploads = uploads
        return


class _FakeResultsGroup:
    """
        Records every result manifest posted and every presign request,
        optionally failing the POST or the presign call.
    """

    def __init__(self, *, raise_on_post=False, raise_on_presign=False):
        self.manifests = []
        self.presign_requests = []
        self._raise_on_post = raise_on_post
        self._raise_on_presign = raise_on_presign
        return

    def results_post_complete(self, run_id, body, **kwargs):
        self.manifests.append((run_id, body))
        if self._raise_on_post:
            raise RuntimeError("simulated results/complete failure")
        return None

    def artifacts_presign_upload(self, run_id, body, **kwargs):
        paths = list(body.paths)
        self.presign_requests.append((run_id, paths))
        if self._raise_on_presign:
            raise RuntimeError("simulated presign 5xx")
        uploads = []
        for nxtpath in paths:
            target = _FakeUploadTarget(
                path=nxtpath,
                put_url="https://pychestrate.example.com/api/v1/runs/{}/"
                        "artifacts/_local-upload?path={}".format(
                            run_id, nxtpath),
                headers={})
            uploads.append(target)
        rtnval = _FakePresignResponse(run_id=run_id, uploads=uploads)
        return rtnval


class _FakeHttpResponse:
    """
        Minimal stand-in for a :class:`requests.Response`.
    """

    def __init__(self, status_code):
        self.status_code = status_code
        return


class _FakeMtlsSession:
    """
        Records every PUT issued by the presigned upload driver.
    """

    def __init__(self, *, status_code=201):
        self.puts = []
        self._status_code = status_code
        return

    def put(self, url, data=None, headers=None):
        self.puts.append((url, data, headers))
        rtnval = _FakeHttpResponse(status_code=self._status_code)
        return rtnval


class _FakeRestClient:
    """
        Minimal stand-in for :class:`PychestrateMtlsClient` carrying the
        ``results`` group and an mTLS-style session for presigned PUTs.
    """

    def __init__(self, *, raise_on_post=False, raise_on_presign=False,
                 put_status_code=201):
        self.results = _FakeResultsGroup(
            raise_on_post=raise_on_post, raise_on_presign=raise_on_presign)
        self._session = _FakeMtlsSession(status_code=put_status_code)
        return

    def authenticate(self):
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


def test_walk_result_tree_leaves_out_python_bytecode():
    with tempfile.TemporaryDirectory() as tmpdir:
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        os.makedirs(os.path.join(result_dir, "__pycache__"))
        for name in (os.path.join("__pycache__", "testrun_sequence.cpython-312.pyc"), "loose.pyc"):
            with open(os.path.join(result_dir, name), "wb") as handle:
                handle.write(b"\x00\x01")
        with open(os.path.join(result_dir, "testrun_sequence.py"), "w", encoding="utf-8") as handle:
            handle.write("# generated\n")

        relative_paths = [entry.relative_path for entry in walk_result_tree(result_dir=result_dir)]

        testbase.assert_equal(
            ("testrun_sequence.py" in relative_paths, [path for path in relative_paths if path.endswith(".pyc") or "__pycache__" in path], len(relative_paths)),
            (True, [], 7),
        )
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


def test_upload_result_tree_per_file_driver_uses_put_tree_default():
    # A per-file site driver (one that overrides only put_object) must
    # transfer every file via the inherited put_tree default loop.
    with tempfile.TemporaryDirectory() as tmpdir:
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        entries = walk_result_tree(result_dir=result_dir)

        driver = _RecordingStoreDriver()
        uploaded = upload_result_tree(
            run_id=RUN_ID, entries=entries, backend_driver="s3",
            backend_key_prefix="proj/run-x", store_driver=driver)
        testbase.assert_true(uploaded)
        testbase.assert_equal(len(driver.transfers), 6)
    return


def test_upload_result_tree_with_driver_transfers_every_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        entries = walk_result_tree(result_dir=result_dir)
        driver = _RecordingStoreDriver()

        uploaded = upload_result_tree(
            run_id=RUN_ID, entries=entries, backend_driver="s3",
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
            run_id=RUN_ID, entries=entries, backend_driver="s3",
            backend_key_prefix="proj/run-x", store_driver=driver)
        testbase.assert_false(uploaded)
    return


def test_presigned_driver_presigns_and_puts_every_file():
    # The default PresignedUploadDriver must batch-presign every relative
    # path and PUT each artifact's bytes to its minted target.
    with tempfile.TemporaryDirectory() as tmpdir:
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        entries = walk_result_tree(result_dir=result_dir)
        client = _FakeRestClient()
        driver = PresignedUploadDriver(client=client)

        driver.put_tree(
            run_id=RUN_ID, backend_driver="local", backend_key_prefix="p",
            entries=entries)

        # Exactly one batch presign call, carrying every relative path.
        testbase.assert_equal(len(client.results.presign_requests), 1)
        presign_run_id, presign_paths = client.results.presign_requests[0]
        testbase.assert_equal(presign_run_id, RUN_ID)
        testbase.assert_equal(len(presign_paths), 6)

        # Every artifact's bytes were PUT through the mTLS session.
        testbase.assert_equal(len(client._session.puts), 6)
    return


def test_presigned_driver_presign_failure_raises():
    # A presign 5xx must surface as an exception so upload_result_tree
    # reports the failure and the run is marked for retry.
    with tempfile.TemporaryDirectory() as tmpdir:
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        entries = walk_result_tree(result_dir=result_dir)
        client = _FakeRestClient(raise_on_presign=True)
        driver = PresignedUploadDriver(client=client)

        uploaded = upload_result_tree(
            run_id=RUN_ID, entries=entries, backend_driver="local",
            backend_key_prefix="p", store_driver=driver)
        testbase.assert_false(uploaded)
        testbase.assert_equal(len(client._session.puts), 0)
    return


def test_presigned_driver_rejects_non_2xx_put():
    # A presigned PUT that answers a non-2xx status must surface as a
    # failure.
    with tempfile.TemporaryDirectory() as tmpdir:
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        entries = walk_result_tree(result_dir=result_dir)
        client = _FakeRestClient(put_status_code=500)
        driver = PresignedUploadDriver(client=client)

        uploaded = upload_result_tree(
            run_id=RUN_ID, entries=entries, backend_driver="local",
            backend_key_prefix="p", store_driver=driver)
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


def test_an_rpa_failure_is_explained_in_the_manifest_notes():
    """
        The service records the manifest's notes as the job's reason; the RPA upload sent none, so it read "rpa result
        manifest: outcome=failure" whatever went wrong. The failure detail now travels as the notes, and a job that
        succeeded sends none.
    """
    def _notes(*, succeeded: bool, run_notes) -> object:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = _make_state(os.path.join(tmpdir, "state"))
            result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
            client = _FakeRestClient()
            upload_rpa_results(
                client=client, state=state, envelope=_sample_envelope(), result_dir=result_dir, run_succeeded=succeeded, exit_code=3 if not succeeded else 0,
                execution_mode="process", rpa_status="failed" if not succeeded else "succeeded", run_notes=run_notes,
                aspects=ResultUploaderAspects(store_driver=_RecordingStoreDriver()),
            )
            return client.results.manifests[0][1].notes

    testbase.assert_equal(
        (_notes(succeeded=False, run_notes="RPA job exited with code 3: boom"), _notes(succeeded=False, run_notes=None), _notes(succeeded=True, run_notes=None)),
        ("RPA job exited with code 3: boom", None, None),
    )
    return


def test_upload_run_results_defaults_to_presigned_driver():
    # With no driver explicitly wired the uploader defaults to the
    # PresignedUploadDriver: it presigns + PUTs the tree and posts the
    # completion manifest, all without a site-provisioned object store.
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        client = _FakeRestClient()

        outcome = upload_run_results(
            client=client, state=state, envelope=_sample_envelope(),
            result_dir=result_dir, run_succeeded=True)

        testbase.assert_true(outcome.succeeded)
        testbase.assert_equal(outcome.status, UPLOAD_STATUS_COMPLETE)
        testbase.assert_true(outcome.tree_uploaded)
        testbase.assert_true(outcome.manifest_posted)

        # The presigned protocol ran: one presign batch + per-file PUTs.
        testbase.assert_equal(len(client.results.presign_requests), 1)
        testbase.assert_equal(len(client._session.puts), outcome.file_count)
        testbase.assert_equal(len(client.results.manifests), 1)

        testbase.assert_false(run_needs_upload_retry(state=state,
                                                     run_id=RUN_ID))
    return


def test_upload_run_results_presign_failure_marks_retry():
    # A genuine presign failure (5xx) must mark the run upload_failed for
    # retry while still posting the completion manifest for reconciliation.
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        client = _FakeRestClient(raise_on_presign=True)

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


def test_upload_run_results_reports_a_cancelled_run_as_cancelled():
    """
    **Feature: cancel (E2E-09)**
    **Property: the manifest of a run an operator cancel stopped carries
    terminal_outcome "cancelled", which the service keeps out of FAILED.**
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        client = _FakeRestClient()
        aspects = ResultUploaderAspects(store_driver=_RecordingStoreDriver())

        upload_run_results(
            client=client, state=state, envelope=_sample_envelope(),
            result_dir=result_dir, run_succeeded=False, run_cancelled=True, aspects=aspects)

        _, manifest = client.results.manifests[0]
        testbase.assert_equal(manifest.terminal_outcome, "cancelled")
    return


def test_upload_run_results_sends_the_failure_detail_as_manifest_notes():
    """
    **Feature: pod failure reasons (E2E-15)**
    **Property: a failed run's detail travels as the manifest's notes, so
    the service can show why it failed.**
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        client = _FakeRestClient()
        aspects = ResultUploaderAspects(store_driver=_RecordingStoreDriver())

        upload_run_results(
            client=client, state=state, envelope=_sample_envelope(),
            result_dir=result_dir, run_succeeded=False,
            run_notes="testbase run exited with code 2.", aspects=aspects)

        _, manifest = client.results.manifests[0]
        testbase.assert_equal(manifest.notes, "testbase run exited with code 2.")
    return

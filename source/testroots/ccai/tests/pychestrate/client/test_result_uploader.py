"""
    TestBase tests for the pychestrate-client result uploader. The REST
    client and the shared-results object store are mocked; the testbase
    result tree is written by the test. No real network traffic or storage
    transfer is performed.
"""

import hashlib
import json
import os
import tempfile

from datetime import datetime, timedelta, timezone

from ccai import testbase

from ccai.errors.exceptions import SemanticError

from ccai.pychestrate.client.result_uploader import (
    EXTRAS_KEY_FAILURE_CLASSIFICATION,
    RECORD_SECTION_UPLOAD_RETRY,
    RESULT_FORMAT_TAG,
    TERMINAL_OUTCOME_CANCELLED,
    UPLOAD_KIND_TESTBASE,
    ChunkedFilePayload,
    PresignedUploadDriver,
    ResultStoreDriver,
    ResultUploaderAspects,
    TERMINAL_OUTCOME_FAILURE,
    TERMINAL_OUTCOME_SUCCESS,
    UPLOAD_STATUS_COMPLETE,
    UPLOAD_STATUS_FAILED,
    build_result_manifest,
    find_pending_uploads,
    retry_pending_uploads,
    run_needs_upload_retry,
    upload_result_tree,
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


class _FlakyStoreDriver(ResultStoreDriver):
    """
        A :class:`ResultStoreDriver` that fails its first ``fail_times``
        transfers and succeeds afterwards -- a transient network blip.
    """

    def __init__(self, *, fail_times):
        self.attempts = 0
        self.delivered = []
        self._fail_times = fail_times
        return

    def put_tree(self, *, run_id, backend_driver, backend_key_prefix,
                 entries):
        self.attempts += 1
        if self.attempts <= self._fail_times:
            raise RuntimeError("simulated transient object-store failure")
        self.delivered.append(
            [entry.relative_path for entry in entries])
        return None


def _fast_retry_aspects(*, store_driver=None, max_upload_attempts=0):
    """
        Returns aspects whose retry backoff is zero so a retry is due
        immediately and the tests never sleep.
    """
    aspects = ResultUploaderAspects(
        store_driver=store_driver,
        max_upload_attempts=max_upload_attempts,
        retry_interval_seconds=0.0,
        retry_backoff_factor=1.0,
        retry_max_interval_seconds=0.0)
    return aspects


def _read_run_record(state, run_id):
    """
        Reads the local run record written by the uploader.
    """
    record_path = os.path.join(
        state.runs_dir, run_id, "run-record.json")
    with open(record_path, "r", encoding="utf-8") as recf:
        record = json.load(recf)
    return record


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


class _StreamingMtlsSession:
    """
        Drains every PUT body as an iterator, recording the chunk sizes it
        saw and the bytes it reassembled. This is what a streaming
        transport does, so it is what proves the client streams rather than
        buffering an artifact whole.
    """

    def __init__(self, *, status_code=201):
        self.puts = []
        self._status_code = status_code
        return

    def put(self, url, data=None, headers=None):
        declared_length = len(data)

        chunk_sizes = []
        body = bytearray()
        for chunk in data:
            chunk_sizes.append(len(chunk))
            body.extend(chunk)

        self.puts.append({
            "url": url,
            "declared_length": declared_length,
            "chunk_sizes": chunk_sizes,
            "body": bytes(body),
        })
        rtnval = _FakeHttpResponse(status_code=self._status_code)
        return rtnval


class _FakeRestClient:
    """
        Minimal stand-in for :class:`PychestrateMtlsClient` carrying the
        ``results`` group and an mTLS-style session for presigned PUTs.
    """

    def __init__(self, *, raise_on_post=False, raise_on_presign=False,
                 put_status_code=201, streaming_session=False):
        self.results = _FakeResultsGroup(
            raise_on_post=raise_on_post, raise_on_presign=raise_on_presign)
        if streaming_session:
            self._session = _StreamingMtlsSession(
                status_code=put_status_code)
        else:
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


def test_upload_run_results_presign_failure_posts_no_completion():
    # A genuine presign failure (5xx) means the artifact bytes never landed.
    # The client must NOT post a completion manifest in that case -- the
    # service would mark the run complete against artifacts that do not
    # exist and has no way to detect it. The run is marked upload_failed
    # for retry instead.
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        client = _FakeRestClient(raise_on_presign=True)

        outcome = upload_run_results(
            client=client, state=state, envelope=_sample_envelope(),
            result_dir=result_dir, run_succeeded=True,
            aspects=_fast_retry_aspects())

        testbase.assert_false(outcome.succeeded)
        testbase.assert_equal(outcome.status, UPLOAD_STATUS_FAILED)
        testbase.assert_false(outcome.tree_uploaded)

        # No completion was claimed, on the outcome or on the wire.
        testbase.assert_false(outcome.manifest_posted)
        testbase.assert_equal(len(client.results.manifests), 0)

        testbase.assert_true(run_needs_upload_retry(state=state,
                                                    run_id=RUN_ID))
    return


def test_upload_run_results_transfer_failure_records_pending_retry():
    # A failed transfer must persist everything a later pass needs to
    # finish the delivery without the original job envelope.
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        client = _FakeRestClient()
        aspects = _fast_retry_aspects(
            store_driver=_FlakyStoreDriver(fail_times=1))

        upload_run_results(
            client=client, state=state, envelope=_sample_envelope(),
            result_dir=result_dir, run_succeeded=True, aspects=aspects)

        record = _read_run_record(state=state, run_id=RUN_ID)
        retry_section = record[RECORD_SECTION_UPLOAD_RETRY]

        testbase.assert_equal(retry_section["kind"], UPLOAD_KIND_TESTBASE)
        testbase.assert_equal(retry_section["run_id"], RUN_ID)
        testbase.assert_equal(retry_section["client_id"], CLIENT_ID)
        testbase.assert_equal(retry_section["result_dir"], result_dir)
        testbase.assert_equal(retry_section["backend_driver"], "s3")
        testbase.assert_equal(
            retry_section["backend_key_prefix"],
            "proj/results/2026/05/19/testbase/run-x")
        testbase.assert_true(retry_section["run_succeeded"])
        testbase.assert_false(retry_section["tree_delivered"])
        testbase.assert_equal(retry_section["attempts"], 1)
        testbase.assert_false(retry_section["abandoned"])
        testbase.assert_true(retry_section["next_attempt_at"] is not None)
    return


def test_retry_pending_uploads_recovers_from_transient_failure():
    # The whole point of the retry path: a network blip during the artifact
    # transfer must not strand the results on the pod forever.
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        client = _FakeRestClient()
        driver = _FlakyStoreDriver(fail_times=1)
        aspects = _fast_retry_aspects(store_driver=driver)

        first = upload_run_results(
            client=client, state=state, envelope=_sample_envelope(),
            result_dir=result_dir, run_succeeded=True, aspects=aspects)

        testbase.assert_false(first.succeeded)
        testbase.assert_equal(len(client.results.manifests), 0)

        # The periodic loop calls this; the second attempt succeeds.
        outcomes = retry_pending_uploads(
            client=client, state=state, aspects=aspects)

        testbase.assert_equal(len(outcomes), 1)
        testbase.assert_true(outcomes[0].succeeded)
        testbase.assert_equal(outcomes[0].status, UPLOAD_STATUS_COMPLETE)
        testbase.assert_equal(outcomes[0].file_count, 6)

        # The completion manifest is posted exactly once, and only now that
        # the artifacts are really there.
        testbase.assert_equal(len(client.results.manifests), 1)
        testbase.assert_equal(driver.attempts, 2)
        testbase.assert_equal(len(driver.delivered[0]), 6)

        # The run no longer owes anything.
        testbase.assert_false(run_needs_upload_retry(state=state,
                                                     run_id=RUN_ID))
        testbase.assert_equal(len(find_pending_uploads(state=state)), 0)

        record = _read_run_record(state=state, run_id=RUN_ID)
        testbase.assert_false(RECORD_SECTION_UPLOAD_RETRY in record)
    return


def test_retry_pending_uploads_resumes_after_process_restart():
    # The retry descriptor lives on disk, so a client that is restarted
    # picks the stranded results back up from a fresh ClientState.
    with tempfile.TemporaryDirectory() as tmpdir:
        state_dir = os.path.join(tmpdir, "state")
        state = _make_state(state_dir)
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        driver = _FlakyStoreDriver(fail_times=1)
        aspects = _fast_retry_aspects(store_driver=driver)

        upload_run_results(
            client=_FakeRestClient(), state=state,
            envelope=_sample_envelope(), result_dir=result_dir,
            run_succeeded=True, aspects=aspects)

        # A brand-new process: nothing in memory, only the state directory.
        restarted_state = ClientState(state_dir=state_dir)
        restarted_client = _FakeRestClient()

        pending_uploads = find_pending_uploads(state=restarted_state)
        testbase.assert_equal(len(pending_uploads), 1)
        testbase.assert_equal(pending_uploads[0].run_id, RUN_ID)
        testbase.assert_equal(pending_uploads[0].result_dir, result_dir)

        outcomes = retry_pending_uploads(
            client=restarted_client, state=restarted_state, aspects=aspects)

        testbase.assert_equal(len(outcomes), 1)
        testbase.assert_true(outcomes[0].succeeded)
        testbase.assert_equal(len(restarted_client.results.manifests), 1)
    return


def test_retry_pending_uploads_honors_the_backoff_schedule():
    # A pending upload whose backoff has not elapsed is left for a later
    # pass instead of being hammered once per loop iteration.
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        driver = _FlakyStoreDriver(fail_times=1)
        aspects = ResultUploaderAspects(
            store_driver=driver, retry_interval_seconds=600.0)

        upload_run_results(
            client=_FakeRestClient(), state=state,
            envelope=_sample_envelope(), result_dir=result_dir,
            run_succeeded=True, aspects=aspects)
        testbase.assert_equal(driver.attempts, 1)

        client = _FakeRestClient()
        too_soon = datetime.now(timezone.utc)
        outcomes = retry_pending_uploads(
            client=client, state=state, aspects=aspects, now=too_soon)

        testbase.assert_equal(len(outcomes), 0)
        testbase.assert_equal(driver.attempts, 1)
        testbase.assert_equal(len(client.results.manifests), 0)

        # Once the backoff has elapsed the attempt is made.
        later = too_soon + timedelta(seconds=601)
        outcomes = retry_pending_uploads(
            client=client, state=state, aspects=aspects, now=later)

        testbase.assert_equal(len(outcomes), 1)
        testbase.assert_true(outcomes[0].succeeded)
        testbase.assert_equal(driver.attempts, 2)
    return


def test_retry_pending_uploads_abandons_after_the_attempt_cap():
    # An operator who caps the attempts gets a bounded retry, and the run
    # is left visibly undelivered rather than silently reported complete.
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        client = _FakeRestClient()
        driver = _FlakyStoreDriver(fail_times=99)
        aspects = _fast_retry_aspects(
            store_driver=driver, max_upload_attempts=2)

        upload_run_results(
            client=client, state=state, envelope=_sample_envelope(),
            result_dir=result_dir, run_succeeded=True, aspects=aspects)

        outcomes = retry_pending_uploads(
            client=client, state=state, aspects=aspects)
        testbase.assert_equal(len(outcomes), 1)
        testbase.assert_false(outcomes[0].succeeded)

        record = _read_run_record(state=state, run_id=RUN_ID)
        testbase.assert_true(
            record[RECORD_SECTION_UPLOAD_RETRY]["abandoned"])

        # Abandoned uploads are no longer attempted...
        testbase.assert_equal(len(find_pending_uploads(state=state)), 0)
        outcomes = retry_pending_uploads(
            client=client, state=state, aspects=aspects)
        testbase.assert_equal(len(outcomes), 0)
        testbase.assert_equal(driver.attempts, 2)

        # ...but the run is still on record as never delivered, and no
        # completion was ever claimed.
        testbase.assert_true(run_needs_upload_retry(state=state,
                                                    run_id=RUN_ID))
        testbase.assert_equal(len(client.results.manifests), 0)
    return


def test_upload_run_results_failed_completion_post_marks_retry():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        client = _FakeRestClient(raise_on_post=True)
        driver = _RecordingStoreDriver()
        aspects = _fast_retry_aspects(store_driver=driver)

        outcome = upload_run_results(
            client=client, state=state, envelope=_sample_envelope(),
            result_dir=result_dir, run_succeeded=False, aspects=aspects)

        testbase.assert_false(outcome.succeeded)
        testbase.assert_equal(outcome.status, UPLOAD_STATUS_FAILED)
        testbase.assert_false(outcome.manifest_posted)
        testbase.assert_true(run_needs_upload_retry(state=state,
                                                    run_id=RUN_ID))

        # The bytes DID land, so the retry owes only the completion POST.
        record = _read_run_record(state=state, run_id=RUN_ID)
        testbase.assert_true(
            record[RECORD_SECTION_UPLOAD_RETRY]["tree_delivered"])
    return


def test_retry_after_failed_completion_post_does_not_reupload_the_tree():
    # When only the completion POST failed, the artifacts are already in
    # the object store; re-sending every byte would be pure waste.
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        driver = _RecordingStoreDriver()
        aspects = _fast_retry_aspects(store_driver=driver)

        upload_run_results(
            client=_FakeRestClient(raise_on_post=True), state=state,
            envelope=_sample_envelope(), result_dir=result_dir,
            run_succeeded=True, aspects=aspects)
        testbase.assert_equal(len(driver.transfers), 6)

        healthy_client = _FakeRestClient()
        outcomes = retry_pending_uploads(
            client=healthy_client, state=state, aspects=aspects)

        testbase.assert_equal(len(outcomes), 1)
        testbase.assert_true(outcomes[0].succeeded)
        testbase.assert_equal(len(healthy_client.results.manifests), 1)

        # No second pass over the bytes.
        testbase.assert_equal(len(driver.transfers), 6)
    return


def test_retry_after_delivery_describes_what_was_delivered():
    # Once the bytes are in the object store, the manifest must describe
    # what was DELIVERED. Re-walking the local directory could disagree
    # with it, so the file index recorded at delivery time is what wins.
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        driver = _RecordingStoreDriver()
        aspects = _fast_retry_aspects(store_driver=driver)

        upload_run_results(
            client=_FakeRestClient(raise_on_post=True), state=state,
            envelope=_sample_envelope(), result_dir=result_dir,
            run_succeeded=True, aspects=aspects)

        record = _read_run_record(state=state, run_id=RUN_ID)
        delivered_files = record[RECORD_SECTION_UPLOAD_RETRY][
            "delivered_files"]
        testbase.assert_equal(len(delivered_files), 6)

        # The pod's copy of the tree is cleaned up before the retry pass.
        for current_dir, _subdirs, filenames in os.walk(result_dir,
                                                        topdown=False):
            for filename in filenames:
                os.remove(os.path.join(current_dir, filename))
            os.rmdir(current_dir)

        client = _FakeRestClient()
        outcomes = retry_pending_uploads(
            client=client, state=state, aspects=aspects)

        testbase.assert_equal(len(outcomes), 1)
        testbase.assert_true(outcomes[0].succeeded)
        testbase.assert_equal(outcomes[0].file_count, 6)

        _, manifest = client.results.manifests[0]
        testbase.assert_equal(manifest.extras["file_count"], 6)
        testbase.assert_equal(len(manifest.extras["files"]), 6)

        # The counters read at delivery time survive too, so the manifest
        # does not report an all-zero run just because the tree is gone.
        testbase.assert_equal(manifest.tests_passed, 7)
        testbase.assert_equal(manifest.tests_failed, 2)
        testbase.assert_equal(manifest.tests_skipped, 1)
        testbase.assert_equal(manifest.tests_errored, 1)
    return


def test_retry_of_a_vanished_result_tree_posts_no_completion():
    # If the result tree is gone from the pod there is nothing left to
    # deliver. Posting a completion would assert artifacts that exist
    # nowhere, so the failure stays visible on the record instead.
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        client = _FakeRestClient()
        aspects = _fast_retry_aspects(
            store_driver=_FlakyStoreDriver(fail_times=1))

        upload_run_results(
            client=client, state=state, envelope=_sample_envelope(),
            result_dir=result_dir, run_succeeded=True, aspects=aspects)

        # The results are wiped off the pod before the retry pass runs.
        for current_dir, _subdirs, filenames in os.walk(result_dir,
                                                        topdown=False):
            for filename in filenames:
                os.remove(os.path.join(current_dir, filename))
            os.rmdir(current_dir)

        outcomes = retry_pending_uploads(
            client=client, state=state, aspects=aspects)

        testbase.assert_equal(len(outcomes), 1)
        testbase.assert_false(outcomes[0].succeeded)
        testbase.assert_false(outcomes[0].manifest_posted)
        testbase.assert_equal(len(client.results.manifests), 0)

        record = _read_run_record(state=state, run_id=RUN_ID)
        retry_section = record[RECORD_SECTION_UPLOAD_RETRY]
        testbase.assert_equal(retry_section["attempts"], 2)
        testbase.assert_true(
            "no longer on disk" in retry_section["last_error"])
    return


def test_upload_run_results_defaults_keep_deriving_the_terminal_outcome():
    # Omitting terminal_outcome must behave exactly as before: derived from
    # run_succeeded, and no failure_classification member at all.
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        client = _FakeRestClient()
        aspects = _fast_retry_aspects(store_driver=_RecordingStoreDriver())

        upload_run_results(
            client=client, state=state, envelope=_sample_envelope(),
            result_dir=result_dir, run_succeeded=False, aspects=aspects)

        _, manifest = client.results.manifests[0]
        testbase.assert_equal(manifest.terminal_outcome,
                              TERMINAL_OUTCOME_FAILURE)
        testbase.assert_false(
            EXTRAS_KEY_FAILURE_CLASSIFICATION in manifest.extras)
    return


def test_upload_run_results_carries_an_explicit_cancelled_outcome():
    # A cancelled run is not a failed run. The caller-supplied outcome is
    # used verbatim and the structured reason rides in extras.
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        client = _FakeRestClient()
        aspects = _fast_retry_aspects(store_driver=_RecordingStoreDriver())

        outcome = upload_run_results(
            client=client, state=state, envelope=_sample_envelope(),
            result_dir=result_dir, run_succeeded=False, aspects=aspects,
            terminal_outcome=TERMINAL_OUTCOME_CANCELLED,
            failure_classification="cancelled")

        testbase.assert_true(outcome.succeeded)

        _, manifest = client.results.manifests[0]
        testbase.assert_equal(manifest.terminal_outcome,
                              TERMINAL_OUTCOME_CANCELLED)
        testbase.assert_equal(
            manifest.extras[EXTRAS_KEY_FAILURE_CLASSIFICATION], "cancelled")

        # The file index still rides alongside it.
        testbase.assert_equal(manifest.extras["file_count"], 6)
    return


def test_retry_preserves_an_explicit_outcome_and_classification():
    # The retry path must not re-derive the outcome. A cancelled run whose
    # first upload failed must still be reported as cancelled, not failure.
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))
        client = _FakeRestClient()
        aspects = _fast_retry_aspects(
            store_driver=_FlakyStoreDriver(fail_times=1))

        first = upload_run_results(
            client=client, state=state, envelope=_sample_envelope(),
            result_dir=result_dir, run_succeeded=False, aspects=aspects,
            terminal_outcome=TERMINAL_OUTCOME_CANCELLED,
            failure_classification="cancelled")
        testbase.assert_false(first.succeeded)
        testbase.assert_equal(len(client.results.manifests), 0)

        record = _read_run_record(state=state, run_id=RUN_ID)
        retry_section = record[RECORD_SECTION_UPLOAD_RETRY]
        testbase.assert_equal(retry_section["terminal_outcome"],
                              TERMINAL_OUTCOME_CANCELLED)
        testbase.assert_equal(retry_section["failure_classification"],
                              "cancelled")

        # A restarted client reconstructs both from disk.
        restarted_state = ClientState(state_dir=state.state_dir)
        outcomes = retry_pending_uploads(
            client=client, state=restarted_state, aspects=aspects)

        testbase.assert_equal(len(outcomes), 1)
        testbase.assert_true(outcomes[0].succeeded)

        _, manifest = client.results.manifests[0]
        testbase.assert_equal(manifest.terminal_outcome,
                              TERMINAL_OUTCOME_CANCELLED)
        testbase.assert_equal(
            manifest.extras[EXTRAS_KEY_FAILURE_CLASSIFICATION], "cancelled")
    return


def test_upload_run_results_rejects_an_empty_terminal_outcome():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))

        raised = False
        try:
            upload_run_results(
                client=_FakeRestClient(), state=state,
                envelope=_sample_envelope(), result_dir=result_dir,
                run_succeeded=True, terminal_outcome="")
        except SemanticError:
            raised = True

        testbase.assert_true(raised)
    return


def test_presigned_driver_streams_an_artifact_larger_than_one_chunk():
    # An artifact must never be read whole into memory. It is handed to the
    # transport as a bounded chunk iterator that also declares its length.
    with tempfile.TemporaryDirectory() as tmpdir:
        result_dir = os.path.join(tmpdir, "output")
        os.makedirs(result_dir)

        chunk_size = 4096
        payload_bytes = os.urandom(chunk_size * 3 + 17)
        large_path = os.path.join(result_dir, "large.log")
        with open(large_path, "wb") as bigf:
            bigf.write(payload_bytes)

        entries = walk_result_tree(result_dir=result_dir)
        testbase.assert_equal(len(entries), 1)

        expected_digest = hashlib.sha256(payload_bytes).hexdigest()
        testbase.assert_equal(entries[0].sha256, expected_digest)
        testbase.assert_equal(entries[0].size_bytes, len(payload_bytes))

        client = _FakeRestClient(streaming_session=True)
        driver = PresignedUploadDriver(client=client, chunk_size=chunk_size)

        driver.put_tree(
            run_id=RUN_ID, backend_driver="local", backend_key_prefix="p",
            entries=entries)

        testbase.assert_equal(len(client._session.puts), 1)
        put = client._session.puts[0]

        # The transport was told the exact length, so the PUT carries a
        # Content-Length rather than falling back to chunked encoding.
        testbase.assert_equal(put["declared_length"], len(payload_bytes))

        # The bytes arrived in bounded chunks, more than one of them.
        testbase.assert_true(len(put["chunk_sizes"]) > 1)
        testbase.assert_equal(len(put["chunk_sizes"]), 4)
        for nxtsize in put["chunk_sizes"]:
            testbase.assert_true(nxtsize <= chunk_size)

        # And they reassemble to exactly the file, hash included.
        testbase.assert_equal(put["body"], payload_bytes)
        testbase.assert_equal(
            hashlib.sha256(put["body"]).hexdigest(), expected_digest)
    return


def test_chunked_file_payload_hashes_while_it_streams():
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "artifact.bin")
        payload_bytes = os.urandom(70000)
        with open(file_path, "wb") as artf:
            artf.write(payload_bytes)

        expected_digest = hashlib.sha256(payload_bytes).hexdigest()

        payload = ChunkedFilePayload(
            local_path=file_path, size_bytes=len(payload_bytes),
            chunk_size=8192, expected_sha256=expected_digest)

        testbase.assert_equal(len(payload), len(payload_bytes))

        streamed = bytearray()
        chunk_count = 0
        for chunk in payload:
            chunk_count += 1
            testbase.assert_true(len(chunk) <= 8192)
            streamed.extend(chunk)

        testbase.assert_true(chunk_count > 1)
        testbase.assert_equal(bytes(streamed), payload_bytes)
        testbase.assert_equal(payload.bytes_streamed, len(payload_bytes))
        testbase.assert_equal(payload.streamed_sha256, expected_digest)
    return


def test_chunked_file_payload_rejects_a_file_that_changed():
    # A file rewritten between the tree walk and the upload would ship
    # bytes that do not match the digest in the manifest's file index.
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, "artifact.bin")
        original_bytes = b"a" * 5000
        with open(file_path, "wb") as artf:
            artf.write(original_bytes)

        walked_digest = hashlib.sha256(original_bytes).hexdigest()

        with open(file_path, "wb") as artf:
            artf.write(b"b" * 5000)

        payload = ChunkedFilePayload(
            local_path=file_path, size_bytes=len(original_bytes),
            chunk_size=1024, expected_sha256=walked_digest)

        raised = False
        try:
            for _chunk in payload:
                continue
        except Exception:
            raised = True

        testbase.assert_true(raised)
    return


def test_upload_run_results_streams_through_the_default_driver():
    # End to end: the default presigned driver streams every artifact of a
    # real result tree and the completion manifest still lands.
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))
        result_dir = _write_result_tree(os.path.join(tmpdir, "output"))

        big_bytes = os.urandom(200000)
        with open(os.path.join(result_dir, "artifacts", "capture.bin"),
                  "wb") as bigf:
            bigf.write(big_bytes)

        client = _FakeRestClient(streaming_session=True)
        aspects = ResultUploaderAspects(artifact_chunk_size=65536)

        outcome = upload_run_results(
            client=client, state=state, envelope=_sample_envelope(),
            result_dir=result_dir, run_succeeded=True, aspects=aspects)

        testbase.assert_true(outcome.succeeded)
        testbase.assert_equal(outcome.file_count, 7)
        testbase.assert_equal(len(client._session.puts), 7)

        capture_put = None
        for nxtput in client._session.puts:
            if "capture.bin" in nxtput["url"]:
                capture_put = nxtput

        testbase.assert_true(capture_put is not None)
        testbase.assert_equal(len(capture_put["chunk_sizes"]), 4)
        testbase.assert_equal(capture_put["body"], big_bytes)
    return

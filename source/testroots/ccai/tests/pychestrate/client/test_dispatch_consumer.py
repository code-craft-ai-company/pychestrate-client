"""
    TestBase tests for the pychestrate-client dispatch consumer. The REST
    client, the testbase executor, and the envelope verifier inputs are all
    mocked; no real network traffic or testbase invocation is performed.
"""

import os
import tempfile

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from ccai import testbase

from ccai.errors.exceptions import BadParameterError

from ccai.pychestrate.client import dispatch_consumer as dc_mod
from ccai.pychestrate.client.dispatch_consumer import (
    DispatchConsumerAspects,
    handle_one_dispatch,
    run_dispatch_consumer_loop,
)
from ccai.pychestrate.client.envelope_verifier import serialize_envelope_canonical
from ccai.pychestrate.client.state import ClientState
from ccai.pychestrate.client.testbase_executor import (
    RUN_STATUS_SUCCEEDED,
    TestbaseRunOutcome,
)


SIGNING_KEY_ID = "envelope-key-2026"


def _sample_envelope():
    """
        Returns a representative job-envelope dict.
    """
    envelope = {
        "schema_version": "2026-04-24",
        "envelope_id": "11111111-1111-1111-1111-111111111111",
        "issued_at": "2026-05-19T12:00:00+00:00",
        "issuer": "pychestrate.example.com/0.3.1",
        "project_id": "22222222-2222-2222-2222-222222222222",
        "job_id": "33333333-3333-3333-3333-333333333333",
        "job_name": "smoke-suite",
        "job_type": "testbase-singlehost",
        "pipeline": None,
        "run_id": "44444444-4444-4444-4444-444444444444",
        "client_id": "55555555-5555-5555-5555-555555555555",
        "pod_id": "66666666-6666-6666-6666-666666666666",
        "refs": {"branch": "main", "build": "1", "flavor": "rel"},
        "params": {"test_root": "/opt/project"},
        "scoped_secrets": [],
        "results": {"backend_key_prefix": "p", "backend_driver": "s3"},
        "forwarding": {"progress_url": "u", "summary_url": "u", "auth_method": "mtls"},
        "deadline": {
            "soft_deadline_at": "2026-05-19T13:00:00+00:00",
            "hard_deadline_at": "2026-05-19T14:00:00+00:00",
        },
        "extension_claims": {},
    }
    return envelope


def _pending_dict(envelope, signature_hex, key_id=SIGNING_KEY_ID):
    """
        Builds a PendingDispatch-shaped dict.
    """
    pending = {
        "dispatch_attempt_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "run_id": envelope["run_id"],
        "client_id": envelope["client_id"],
        "pod_id": envelope["pod_id"],
        "envelope_id": envelope["envelope_id"],
        "enqueued_at": "2026-05-19T12:00:01+00:00",
        "ack_deadline_at": "2026-05-19T12:05:00+00:00",
        "envelope": envelope,
        "signature": signature_hex,
        "signature_key_id": key_id,
        "signed_at": "2026-05-19T12:00:00+00:00",
        "transport_name": "http-long-poll",
    }
    return pending


class _FakeSigningKeySet:
    """
        Mirrors the codegen'd ``Signingkeyset`` shape (a ``keys`` list).
    """

    def __init__(self, keys):
        self.keys = list(keys)
        return


class _FakeDispatchGroup:
    """
        Records every dispatch-ack posted by the consumer and serves the
        envelope signing-key set.
    """

    def __init__(self, signing_keys=None):
        self.acks = []
        self._signing_keys = signing_keys if signing_keys is not None else []
        self.signing_key_calls = 0
        return

    def dispatch_ack_run(self, run_id, body, **kwargs):
        self.acks.append((run_id, body))
        return None

    def envelope_signing_keys_list(self, **kwargs):
        self.signing_key_calls += 1
        rtnval = _FakeSigningKeySet(keys=self._signing_keys)
        return rtnval


class _FakeProgressGroup:
    """
        Records every progress frame posted by the consumer.
    """

    def __init__(self):
        self.frames = []
        return

    def progress_post_frame(self, run_id, body, **kwargs):
        self.frames.append((run_id, body))
        return None


class _FakeResultsGroup:
    """
        Records every result manifest posted by the consumer.
    """

    def __init__(self):
        self.manifests = []
        return

    def results_post_complete(self, run_id, body, **kwargs):
        self.manifests.append((run_id, body))
        return None


class _FakeClientsGroup:
    """
        Serves a scripted sequence of dispatch results.
    """

    def __init__(self, sequence):
        self._sequence = list(sequence)
        self._cursor = 0
        self.poll_count = 0
        return

    def clients_dispatches_next(self, client_id, timeout_seconds=30, **kwargs):
        self.poll_count += 1
        if self._cursor >= len(self._sequence):
            # Past the script: behave like a long-poll timeout (204).
            raise ValueError("No content")
        item = self._sequence[self._cursor]
        self._cursor += 1
        if item is None:
            raise ValueError("No content")
        return item


class _FakeRestClient:
    """
        Minimal stand-in for :class:`PychestrateMtlsClient`.
    """

    def __init__(self, sequence, signing_keys=None):
        self.clients = _FakeClientsGroup(sequence)
        self.dispatch = _FakeDispatchGroup(signing_keys=signing_keys)
        self.progress = _FakeProgressGroup()
        self.results = _FakeResultsGroup()
        self._session = object()
        return

    def authenticate(self):
        return


def _signing_public_pem(signing_private_key):
    """
        Returns the PEM SubjectPublicKeyInfo text for a private key.
    """
    rtnval = signing_private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode("ascii")
    return rtnval


def _signing_key_entry(signing_private_key, key_id=SIGNING_KEY_ID):
    """
        Returns a JWKS-style signing-key dict for a private key.
    """
    rtnval = {
        "key_id": key_id,
        "algorithm": "ed25519",
        "public_key_pem": _signing_public_pem(signing_private_key),
        "status": "active",
    }
    return rtnval


def _make_enrolled_state(state_dir, signing_private_key,
                         persist_signing_keys=True):
    """
        Writes a fake enrolled state directory and returns the bound
        :class:`ClientState`. The JWKS-style signing-key set is persisted
        unless ``persist_signing_keys`` is False (used to exercise the
        lazy refresh-on-unknown-key path).
    """
    state = ClientState(state_dir=state_dir)
    state.ensure_dir()
    state.write_private_key(key_pem=b"-----BEGIN PRIVATE KEY-----\nXX\n-----END PRIVATE KEY-----\n")
    state.write_client_cert(cert_pem="-----BEGIN CERTIFICATE-----\nCC\n-----END CERTIFICATE-----\n")
    state.write_service_ca(ca_pem="-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----\n")
    state.write_client_id(client_id="55555555-5555-5555-5555-555555555555")

    if persist_signing_keys:
        state.write_envelope_signing_keys(
            keys=[_signing_key_entry(signing_private_key)])

    state.write_config(config={
        "service_url": "https://pychestrate.example.com",
        "host": "pychestrate.example.com",
        "scheme": "https",
        "port": None,
        "client_name": "unit-client",
    })
    return state


def test_aspects_rejects_bad_values():
    raised = False
    try:
        DispatchConsumerAspects(base_backoff_seconds=10.0, max_backoff_seconds=5.0)
    except BadParameterError:
        raised = True
    testbase.assert_true(raised)
    return


def test_handle_one_dispatch_verifies_executes_and_acks():
    signing_key = Ed25519PrivateKey.generate()
    envelope = _sample_envelope()
    signature_hex = signing_key.sign(
        serialize_envelope_canonical(envelope)).hex()
    pending = _pending_dict(envelope, signature_hex)

    executed = {"count": 0}

    def _fake_execute(*, state, envelope, local_run_id, aspects, progress_reporter=None):
        executed["count"] += 1
        outcome = TestbaseRunOutcome(
            status=RUN_STATUS_SUCCEEDED, exit_code=0,
            output_dir="/tmp/out", command=["testbase"], detail="ok")
        return outcome

    original_execute = dc_mod.execute_job_envelope
    dc_mod.execute_job_envelope = _fake_execute
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = _make_enrolled_state(
                os.path.join(tmpdir, "state"), signing_key)
            client = _FakeRestClient(sequence=[])

            executed_flag = handle_one_dispatch(
                client=client,
                state=state,
                pending=pending,
                aspects=DispatchConsumerAspects(),
            )

            testbase.assert_true(executed_flag)
            testbase.assert_equal(executed["count"], 1)
            testbase.assert_equal(len(client.dispatch.acks), 1)

            ack_run_id, ack_body = client.dispatch.acks[0]
            testbase.assert_equal(ack_run_id, envelope["run_id"])
            testbase.assert_equal(ack_body.envelope_id, envelope["envelope_id"])
            testbase.assert_true("ACCEPTED" in ack_body.notes)
    finally:
        dc_mod.execute_job_envelope = original_execute
    return


def test_handle_one_dispatch_rejects_tampered_envelope_without_executing():
    signing_key = Ed25519PrivateKey.generate()
    envelope = _sample_envelope()
    signature_hex = signing_key.sign(
        serialize_envelope_canonical(envelope)).hex()

    # Tamper after signing.
    envelope["job_name"] = "malicious"
    pending = _pending_dict(envelope, signature_hex)

    executed = {"count": 0}

    def _fake_execute(*, state, envelope, local_run_id, aspects, progress_reporter=None):
        executed["count"] += 1
        return TestbaseRunOutcome(
            status=RUN_STATUS_SUCCEEDED, exit_code=0,
            output_dir="/tmp/out", command=["testbase"], detail="ok")

    original_execute = dc_mod.execute_job_envelope
    dc_mod.execute_job_envelope = _fake_execute
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = _make_enrolled_state(
                os.path.join(tmpdir, "state"), signing_key)
            client = _FakeRestClient(sequence=[])

            executed_flag = handle_one_dispatch(
                client=client,
                state=state,
                pending=pending,
                aspects=DispatchConsumerAspects(),
            )

            testbase.assert_false(executed_flag)
            testbase.assert_equal(executed["count"], 0)
            # A rejected dispatch is still acked (with a rejection note).
            testbase.assert_equal(len(client.dispatch.acks), 1)
            _, ack_body = client.dispatch.acks[0]
            testbase.assert_true("REJECTED" in ack_body.notes)
    finally:
        dc_mod.execute_job_envelope = original_execute
    return


def test_handle_one_dispatch_refreshes_signing_keys_on_unknown_key():
    # An envelope signed by a key the persisted set does not carry must
    # trigger a single refresh from GET /envelope-signing-keys; once the
    # refreshed set carries the key the envelope verifies and executes.
    from ccai.pychestrate.client.result_uploader import (
        ResultStoreDriver,
        ResultUploaderAspects,
    )

    signing_key = Ed25519PrivateKey.generate()
    envelope = _sample_envelope()
    signature_hex = signing_key.sign(
        serialize_envelope_canonical(envelope)).hex()
    pending = _pending_dict(envelope, signature_hex)

    class _NoopDriver(ResultStoreDriver):
        def put_tree(self, *, run_id, backend_driver, backend_key_prefix,
                     entries):
            return

    def _fake_execute(*, state, envelope, local_run_id, aspects,
                      progress_reporter=None):
        return TestbaseRunOutcome(
            status=RUN_STATUS_SUCCEEDED, exit_code=0,
            output_dir="/tmp/out", command=["testbase"], detail="ok")

    original_execute = dc_mod.execute_job_envelope
    dc_mod.execute_job_envelope = _fake_execute
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            # The state is enrolled but carries NO persisted signing keys.
            state = _make_enrolled_state(
                os.path.join(tmpdir, "state"), signing_key,
                persist_signing_keys=False)
            # The service serves the signing key through the refresh
            # endpoint.
            client = _FakeRestClient(
                sequence=[],
                signing_keys=[_signing_key_entry(signing_key)])

            aspects = DispatchConsumerAspects(
                result_uploader_aspects=ResultUploaderAspects(
                    store_driver=_NoopDriver()),
            )
            executed_flag = handle_one_dispatch(
                client=client,
                state=state,
                pending=pending,
                aspects=aspects,
            )

            testbase.assert_true(executed_flag)
            # The refresh endpoint was hit exactly once.
            testbase.assert_equal(client.dispatch.signing_key_calls, 1)
            # The refreshed set is now persisted to disk.
            testbase.assert_true(state.has_envelope_signing_keys())
            _, ack_body = client.dispatch.acks[0]
            testbase.assert_true("ACCEPTED" in ack_body.notes)
    finally:
        dc_mod.execute_job_envelope = original_execute
    return


def test_handle_one_dispatch_rejects_unknown_key_after_refresh():
    # When even a refresh cannot resolve the envelope's signature key id
    # the dispatch is rejected without execution.
    signing_key = Ed25519PrivateKey.generate()
    other_key = Ed25519PrivateKey.generate()
    envelope = _sample_envelope()
    signature_hex = signing_key.sign(
        serialize_envelope_canonical(envelope)).hex()
    pending = _pending_dict(envelope, signature_hex,
                            key_id="absent-key-id")

    executed = {"count": 0}

    def _fake_execute(*, state, envelope, local_run_id, aspects,
                      progress_reporter=None):
        executed["count"] += 1
        return TestbaseRunOutcome(
            status=RUN_STATUS_SUCCEEDED, exit_code=0,
            output_dir="/tmp/out", command=["testbase"], detail="ok")

    original_execute = dc_mod.execute_job_envelope
    dc_mod.execute_job_envelope = _fake_execute
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = _make_enrolled_state(
                os.path.join(tmpdir, "state"), signing_key,
                persist_signing_keys=False)
            # The refresh serves only an unrelated key id.
            client = _FakeRestClient(
                sequence=[],
                signing_keys=[_signing_key_entry(
                    other_key, key_id="some-other-key")])

            executed_flag = handle_one_dispatch(
                client=client,
                state=state,
                pending=pending,
                aspects=DispatchConsumerAspects(),
            )

            testbase.assert_false(executed_flag)
            testbase.assert_equal(executed["count"], 0)
            testbase.assert_equal(client.dispatch.signing_key_calls, 1)
            _, ack_body = client.dispatch.acks[0]
            testbase.assert_true("REJECTED" in ack_body.notes)
    finally:
        dc_mod.execute_job_envelope = original_execute
    return


def test_consumer_loop_handles_dispatch_then_timeouts():
    signing_key = Ed25519PrivateKey.generate()
    envelope = _sample_envelope()
    signature_hex = signing_key.sign(
        serialize_envelope_canonical(envelope)).hex()
    pending = _pending_dict(envelope, signature_hex)

    # One real dispatch, then two long-poll timeouts (None -> ValueError).
    sequence = [pending, None, None]

    executed = {"count": 0}

    def _fake_execute(*, state, envelope, local_run_id, aspects, progress_reporter=None):
        executed["count"] += 1
        return TestbaseRunOutcome(
            status=RUN_STATUS_SUCCEEDED, exit_code=0,
            output_dir="/tmp/out", command=["testbase"], detail="ok")

    original_execute = dc_mod.execute_job_envelope
    original_build = dc_mod.build_client_from_state

    fake_client = _FakeRestClient(sequence=sequence)
    dc_mod.execute_job_envelope = _fake_execute
    dc_mod.build_client_from_state = lambda state: fake_client
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = _make_enrolled_state(
                os.path.join(tmpdir, "state"), signing_key)

            aspects = DispatchConsumerAspects(
                poll_timeout_seconds=1,
                max_iterations=3,
                sleep_func=lambda seconds: None,
            )

            iterations = run_dispatch_consumer_loop(state=state, aspects=aspects)

            testbase.assert_equal(iterations, 3)
            testbase.assert_equal(executed["count"], 1)
            testbase.assert_equal(len(fake_client.dispatch.acks), 1)
    finally:
        dc_mod.execute_job_envelope = original_execute
        dc_mod.build_client_from_state = original_build
    return


def test_consumer_loop_stops_on_predicate():
    signing_key = Ed25519PrivateKey.generate()

    original_build = dc_mod.build_client_from_state
    fake_client = _FakeRestClient(sequence=[None, None, None, None, None])
    dc_mod.build_client_from_state = lambda state: fake_client
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = _make_enrolled_state(
                os.path.join(tmpdir, "state"), signing_key)

            counter = {"value": 0}

            def _stop_after_two():
                counter["value"] += 1
                return counter["value"] > 2

            aspects = DispatchConsumerAspects(
                poll_timeout_seconds=1,
                max_iterations=100,
                sleep_func=lambda seconds: None,
            )

            iterations = run_dispatch_consumer_loop(
                state=state, aspects=aspects, stop_predicate=_stop_after_two)

            testbase.assert_equal(iterations, 2)
    finally:
        dc_mod.build_client_from_state = original_build
    return


def test_handle_one_dispatch_streams_progress_uploads_and_completes():
    # M3 end-to-end: a verified dispatch must run with progress streaming,
    # upload its result tree, POST results/complete, and finally ack.
    from ccai.pychestrate.client.result_uploader import (
        ResultStoreDriver,
        ResultUploaderAspects,
    )
    from ccai.pychestrate.client.testbase_executor import (
        resolve_run_output_dir,
    )

    signing_key = Ed25519PrivateKey.generate()
    envelope = _sample_envelope()
    signature_hex = signing_key.sign(
        serialize_envelope_canonical(envelope)).hex()
    pending = _pending_dict(envelope, signature_hex)

    class _RecordingDriver(ResultStoreDriver):
        def __init__(self):
            self.transfers = []
            return

        def put_object(self, *, backend_driver, backend_key_prefix,
                       relative_path, local_path):
            self.transfers.append(relative_path)
            return None

    driver = _RecordingDriver()

    def _fake_execute(*, state, envelope, local_run_id, aspects,
                      progress_reporter=None):
        # Simulate testbase writing its result tree into the run output dir.
        run_id = str(envelope.get("run_id", ""))
        output_dir = resolve_run_output_dir(state=state, run_id=run_id)

        import json as _json

        summary = {"result": "PASSED", "detail": {
            "passed": 3, "failed": 0, "skipped": 0, "errors": 0, "total": 3}}
        with open(os.path.join(output_dir, "testrun_summary.json"), "w",
                  encoding="utf-8") as sumf:
            _json.dump(summary, sumf)
        with open(os.path.join(output_dir, "testrun_results.json"), "w",
                  encoding="utf-8") as resf:
            resf.write("{}")

        # Drive the progress reporter once so a frame is forwarded.
        if progress_reporter is not None:
            progress_reporter.start()
            progress_reporter.stop()

        outcome = TestbaseRunOutcome(
            status=RUN_STATUS_SUCCEEDED, exit_code=0,
            output_dir=output_dir, command=["testbase"], detail="ok")
        return outcome

    original_execute = dc_mod.execute_job_envelope
    dc_mod.execute_job_envelope = _fake_execute
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = _make_enrolled_state(
                os.path.join(tmpdir, "state"), signing_key)
            client = _FakeRestClient(sequence=[])

            aspects = DispatchConsumerAspects(
                result_uploader_aspects=ResultUploaderAspects(
                    store_driver=driver),
            )

            executed_flag = handle_one_dispatch(
                client=client,
                state=state,
                pending=pending,
                aspects=aspects,
            )

            testbase.assert_true(executed_flag)

            # The result manifest was posted to results/complete.
            testbase.assert_equal(len(client.results.manifests), 1)
            posted_run_id, manifest = client.results.manifests[0]
            testbase.assert_equal(posted_run_id, envelope["run_id"])
            testbase.assert_equal(manifest.terminal_outcome, "success")

            # The result tree was transferred via the store driver.
            testbase.assert_true(len(driver.transfers) >= 2)

            # The dispatch was acked last, after the upload completed.
            testbase.assert_equal(len(client.dispatch.acks), 1)
            _, ack_body = client.dispatch.acks[0]
            testbase.assert_true("results uploaded" in ack_body.notes)
    finally:
        dc_mod.execute_job_envelope = original_execute
    return


def test_handle_one_dispatch_upload_failure_still_acks():
    # A genuine result-upload failure (the store driver raises) must not
    # abort the consumer: the run is still acked and the completion
    # manifest posted, and the run is marked locally for retry.
    signing_key = Ed25519PrivateKey.generate()
    envelope = _sample_envelope()
    signature_hex = signing_key.sign(
        serialize_envelope_canonical(envelope)).hex()
    pending = _pending_dict(envelope, signature_hex)

    from ccai.pychestrate.client.result_uploader import (
        ResultStoreDriver,
        ResultUploaderAspects,
        run_needs_upload_retry,
    )
    from ccai.pychestrate.client.testbase_executor import (
        resolve_run_output_dir,
    )

    class _FailingDriver(ResultStoreDriver):
        def put_tree(self, *, run_id, backend_driver, backend_key_prefix,
                     entries):
            raise RuntimeError("simulated presign 5xx")

    def _fake_execute(*, state, envelope, local_run_id, aspects,
                      progress_reporter=None):
        run_id = str(envelope.get("run_id", ""))
        output_dir = resolve_run_output_dir(state=state, run_id=run_id)
        with open(os.path.join(output_dir, "testrun_results.json"), "w",
                  encoding="utf-8") as resf:
            resf.write("{}")
        outcome = TestbaseRunOutcome(
            status=RUN_STATUS_SUCCEEDED, exit_code=0,
            output_dir=output_dir, command=["testbase"], detail="ok")
        return outcome

    original_execute = dc_mod.execute_job_envelope
    dc_mod.execute_job_envelope = _fake_execute
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = _make_enrolled_state(
                os.path.join(tmpdir, "state"), signing_key)
            client = _FakeRestClient(sequence=[])

            # The store driver raises -> upload cannot complete.
            aspects = DispatchConsumerAspects(
                result_uploader_aspects=ResultUploaderAspects(
                    store_driver=_FailingDriver()),
            )
            executed_flag = handle_one_dispatch(
                client=client,
                state=state,
                pending=pending,
                aspects=aspects,
            )

            testbase.assert_true(executed_flag)
            # The dispatch is still acked.
            testbase.assert_equal(len(client.dispatch.acks), 1)
            _, ack_body = client.dispatch.acks[0]
            testbase.assert_true("ACCEPTED" in ack_body.notes)
            testbase.assert_true("INCOMPLETE" in ack_body.notes)

            # The run is marked locally for upload retry.
            testbase.assert_true(run_needs_upload_retry(
                state=state, run_id=envelope["run_id"]))
    finally:
        dc_mod.execute_job_envelope = original_execute
    return

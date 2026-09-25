"""
    TestBase tests for the pychestrate-client dispatch consumer. The REST
    client, the testbase executor, and the envelope verifier inputs are all
    mocked; no real network traffic or testbase invocation is performed.
"""

import base64
import os
import tempfile
import threading
import time

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from ccai import testbase

from ccai.errors.exceptions import BadParameterError

from ccai.pychestrate.client import dispatch_consumer as dc_mod
from ccai.pychestrate.client.dispatch_consumer import (
    DispatchConsumerAspects,
    DispatchPollAnomaly,
    handle_one_dispatch,
    poll_next_dispatch,
    run_dispatch_consumer_loop,
)
from ccai.pychestrate.client.envelope_verifier import serialize_envelope_canonical
from ccai.pychestrate.client.shutdown import ShutdownController
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
        Serves a scripted sequence of dispatch results. ``None`` is a
        long-poll timeout (the codegen'd method returns None for a 204); an
        exception instance is raised; anything else is returned as the
        pending dispatch. ``on_poll`` (optional) runs inside every poll.
    """

    def __init__(self, sequence, on_poll=None):
        self._sequence = list(sequence)
        self._cursor = 0
        self._on_poll = on_poll
        self.poll_count = 0
        return

    def clients_dispatches_next(self, client_id, timeout_seconds=30, **kwargs):
        self.poll_count += 1
        if self._on_poll is not None:
            self._on_poll()
        if self._cursor >= len(self._sequence):
            # Past the script: behave like a long-poll timeout (204).
            return None
        item = self._sequence[self._cursor]
        self._cursor += 1
        if isinstance(item, Exception):
            raise item
        return item


class _FakeRestClient:
    """
        Minimal stand-in for :class:`PychestrateMtlsClient`.
    """

    def __init__(self, sequence, signing_keys=None, on_poll=None):
        self.clients = _FakeClientsGroup(sequence, on_poll=on_poll)
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


def _jwk_for(signing_private_key, key_id=SIGNING_KEY_ID):
    """
        Returns an RFC 7517 / RFC 8037 OKP Ed25519 JWK for a private key.
    """
    raw_public = signing_private_key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    encoded = base64.urlsafe_b64encode(raw_public).decode("ascii")

    rtnval = {
        "kty": "OKP",
        "crv": "Ed25519",
        "alg": "EdDSA",
        "use": "sig",
        "kid": key_id,
        "x": encoded.rstrip("="),
    }
    return rtnval


class _FakeJwksDispatchGroup(_FakeDispatchGroup):
    """
        A dispatch group that additionally serves the service's RFC 7517
        JWK Set, as a codegen'd client generated against a JWKS-enabled
        service would.
    """

    def __init__(self, jwks_document, signing_keys=None):
        super().__init__(signing_keys=signing_keys)
        self._jwks_document = jwks_document
        self.jwks_calls = 0
        return

    def envelope_signing_keys_jwks(self, **kwargs):
        self.jwks_calls += 1
        rtnval = self._jwks_document
        return rtnval


class _FakeJwksRestClient(_FakeRestClient):
    """
        Stand-in for a client talking to a JWKS-enabled service.
    """

    def __init__(self, sequence, jwks_document, signing_keys=None):
        super().__init__(sequence=sequence, signing_keys=signing_keys)
        self.dispatch = _FakeJwksDispatchGroup(
            jwks_document=jwks_document, signing_keys=signing_keys)
        return


def test_handle_one_dispatch_resolves_the_key_from_the_service_jwks():
    # The first-class fetched source: with no local key material at all the
    # consumer fetches the service JWK Set, caches it, and executes.
    signing_key = Ed25519PrivateKey.generate()
    envelope = _sample_envelope()
    signature_hex = signing_key.sign(
        serialize_envelope_canonical(envelope)).hex()
    pending = _pending_dict(envelope, signature_hex)

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
            client = _FakeJwksRestClient(
                sequence=[],
                jwks_document={"keys": [_jwk_for(signing_key)]})

            executed_flag = handle_one_dispatch(
                client=client,
                state=state,
                pending=pending,
                aspects=DispatchConsumerAspects(),
            )

            testbase.assert_true(executed_flag)
            testbase.assert_equal(executed["count"], 1)

            # The JWK Set answered, so the pre-JWKS signing-keys endpoint
            # was never called.
            testbase.assert_equal(client.dispatch.jwks_calls, 1)
            testbase.assert_equal(client.dispatch.signing_key_calls, 0)

            # The fetched JWK Set is cached for subsequent dispatches.
            testbase.assert_true(state.has_envelope_signing_jwks())

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

    # One real dispatch, then two long-poll timeouts (None).
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

            # The dispatch was acked once, as the receipt that preceded the
            # job; the outcome travels in the manifest above.
            testbase.assert_equal(len(client.dispatch.acks), 1)
            _, ack_body = client.dispatch.acks[0]
            testbase.assert_equal(ack_body.notes, "ACCEPTED: starting job_kind=test")
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
            testbase.assert_true(ack_body.notes.startswith("ACCEPTED"))

            # The run is marked locally for upload retry.
            testbase.assert_true(run_needs_upload_retry(
                state=state, run_id=envelope["run_id"]))
    finally:
        dc_mod.execute_job_envelope = original_execute
    return


class _PauseRecorder:
    """
        Records every pause the consumer loop requests.
    """

    def __init__(self):
        self.calls = []
        return

    def __call__(self, seconds):
        self.calls.append(seconds)
        return


def _run_loop_with_fake_client(fake_client, aspects, shutdown=None):
    """
        Runs the consumer loop against ``fake_client`` with a throw-away
        enrolled state and returns the iteration count.
    """
    signing_key = Ed25519PrivateKey.generate()
    original_build = dc_mod.build_client_from_state
    dc_mod.build_client_from_state = lambda state: fake_client
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = _make_enrolled_state(
                os.path.join(tmpdir, "state"), signing_key)
            rtnval = run_dispatch_consumer_loop(
                state=state, aspects=aspects, shutdown=shutdown)
    finally:
        dc_mod.build_client_from_state = original_build
    return rtnval


def test_poll_next_dispatch_raises_anomaly_on_non_json_answer():
    """
    **Feature: dispatch polling (CLI-06)**
    **Property: A 200 answer whose body is not JSON (the codegen'd method
    raises ValueError) is reported as a DispatchPollAnomaly, not as a
    long-poll timeout.**
    """
    fake_client = _FakeRestClient(
        sequence=[ValueError("Expecting value: line 1 column 1 (char 0)")])

    raised = None
    try:
        poll_next_dispatch(client=fake_client, client_id="client-1", timeout_seconds=30)
    except DispatchPollAnomaly as anomaly:
        raised = anomaly

    testbase.assert_true(raised is not None)
    return


def test_poll_next_dispatch_returns_none_on_timeout():
    """
    **Feature: dispatch polling (CLI-06)**
    **Property: A 204 long-poll timeout (the codegen'd method returns
    None) yields None.**
    """
    fake_client = _FakeRestClient(sequence=[None])

    pending = poll_next_dispatch(client=fake_client, client_id="client-1", timeout_seconds=30)

    testbase.assert_true(pending is None)
    return


def test_consumer_loop_pauses_after_an_early_poll():
    """
    **Feature: dispatch polling (CLI-06)**
    **Property: A poll that returns before min_poll_interval_seconds is
    followed by a pause of the minimum interval plus jitter, so the loop
    cannot spin against the server.**
    """
    pauses = _PauseRecorder()
    aspects = DispatchConsumerAspects(
        poll_timeout_seconds=30,
        max_iterations=3,
        sleep_func=pauses,
        min_poll_interval_seconds=1.0,
        poll_jitter_seconds=0.5,
        advertise_capabilities=False,
    )

    _run_loop_with_fake_client(
        fake_client=_FakeRestClient(sequence=[None, None, None]), aspects=aspects)

    testbase.assert_equal(len(pauses.calls), 3)
    testbase.assert_true(all(1.0 <= seconds <= 1.5 for seconds in pauses.calls))
    return


def test_consumer_loop_backs_off_on_poll_anomaly():
    """
    **Feature: dispatch polling (CLI-06)**
    **Property: Repeated non-JSON answers back off exponentially from
    base_backoff_seconds instead of re-polling at once.**
    """
    pauses = _PauseRecorder()
    aspects = DispatchConsumerAspects(
        poll_timeout_seconds=30,
        base_backoff_seconds=5.0,
        max_backoff_seconds=300.0,
        max_iterations=3,
        sleep_func=pauses,
        advertise_capabilities=False,
    )
    sequence = [ValueError("not json"), ValueError("not json"), ValueError("not json")]

    _run_loop_with_fake_client(
        fake_client=_FakeRestClient(sequence=sequence), aspects=aspects)

    testbase.assert_equal(pauses.calls, [5.0, 10.0, 20.0])
    return


def test_consumer_loop_rejects_dispatch_that_arrives_while_stopping():
    """
    **Feature: graceful stop (CLI-07)**
    **Property: A dispatch handed out by the poll that was in flight when
    the stop was requested is acked as rejected, and its job is not
    started.**
    """
    signing_key = Ed25519PrivateKey.generate()
    envelope = _sample_envelope()
    signature_hex = signing_key.sign(serialize_envelope_canonical(envelope)).hex()
    pending = _pending_dict(envelope, signature_hex)

    shutdown = ShutdownController()
    fake_client = _FakeRestClient(
        sequence=[pending],
        on_poll=lambda: shutdown.request_stop(reason="test stop during poll"))

    executed = {"count": 0}

    def _fake_execute(*, state, envelope, local_run_id, aspects, progress_reporter=None):
        executed["count"] += 1
        return TestbaseRunOutcome(
            status=RUN_STATUS_SUCCEEDED, exit_code=0,
            output_dir="/tmp/out", command=["testbase"], detail="ok")

    original_execute = dc_mod.execute_job_envelope
    dc_mod.execute_job_envelope = _fake_execute
    try:
        aspects = DispatchConsumerAspects(
            poll_timeout_seconds=30,
            max_iterations=5,
            sleep_func=lambda seconds: None,
            advertise_capabilities=False,
        )
        _run_loop_with_fake_client(
            fake_client=fake_client, aspects=aspects, shutdown=shutdown)
    finally:
        dc_mod.execute_job_envelope = original_execute

    testbase.assert_equal(executed["count"], 0)
    testbase.assert_equal(len(fake_client.dispatch.acks), 1)
    ack_notes = fake_client.dispatch.acks[0][1].notes
    testbase.assert_true(ack_notes.startswith("REJECTED: pychestrate-client is shutting down"))
    return


def test_consumer_loop_stop_ends_a_pause_promptly():
    """
    **Feature: graceful stop (CLI-07)**
    **Property: A stop requested while the loop pauses between polls ends
    the pause at once rather than after the rest of the interval.**
    """
    shutdown = ShutdownController()
    fake_client = _FakeRestClient(sequence=[None])
    aspects = DispatchConsumerAspects(
        poll_timeout_seconds=30,
        min_poll_interval_seconds=30.0,
        poll_jitter_seconds=0.0,
        advertise_capabilities=False,
    )

    loop_thread = threading.Thread(
        target=_run_loop_with_fake_client,
        kwargs={"fake_client": fake_client, "aspects": aspects, "shutdown": shutdown},
        daemon=True)
    loop_thread.start()

    deadline = time.monotonic() + 5.0
    while fake_client.clients.poll_count == 0 and time.monotonic() < deadline:
        time.sleep(0.01)

    stop_requested_at = time.monotonic()
    shutdown.request_stop(reason="test stop during pause")
    loop_thread.join(timeout=5.0)
    stop_seconds = time.monotonic() - stop_requested_at

    testbase.assert_true(loop_thread.is_alive() is False)
    testbase.assert_true(stop_seconds < 2.0)
    return


def test_handle_one_dispatch_acks_before_running_the_job():
    """
    **Feature: dispatch protocol (E2E-08)**
    **Property: A verified dispatch is acked before the job starts, because
    the service refuses progress and artifact uploads for a run it holds
    no dispatch receipt for.**
    """
    signing_key = Ed25519PrivateKey.generate()
    envelope = _sample_envelope()
    signature_hex = signing_key.sign(serialize_envelope_canonical(envelope)).hex()
    pending = _pending_dict(envelope, signature_hex)

    acks_seen_by_job = {"count": None}
    client_holder = {}

    def _fake_execute(*, state, envelope, local_run_id, aspects, progress_reporter=None):
        acks_seen_by_job["count"] = len(client_holder["client"].dispatch.acks)
        return TestbaseRunOutcome(
            status=RUN_STATUS_SUCCEEDED, exit_code=0,
            output_dir="/tmp/out", command=["testbase"], detail="ok")

    original_execute = dc_mod.execute_job_envelope
    dc_mod.execute_job_envelope = _fake_execute
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = _make_enrolled_state(os.path.join(tmpdir, "state"), signing_key)
            client = _FakeRestClient(sequence=[])
            client_holder["client"] = client

            handle_one_dispatch(
                client=client, state=state, pending=pending,
                aspects=DispatchConsumerAspects())
    finally:
        dc_mod.execute_job_envelope = original_execute

    testbase.assert_equal(acks_seen_by_job["count"], 1)
    return

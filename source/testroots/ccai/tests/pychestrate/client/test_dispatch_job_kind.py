"""
    TestBase tests for the pychestrate-client dispatch-consumer
    ``job_kind`` branching: a ``test`` envelope routes to the testbase
    executor, a ``build`` envelope routes to the build executor, a
    ``provision`` envelope routes to the provision executor, and a
    missing / unrecognized ``job_kind`` defaults to ``test``.

    The REST client, all three executors, and all three uploaders are
    mocked; no real network traffic, testbase run, build, or provisioning
    is performed.
"""

import os
import tempfile

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from ccai import testbase

from ccai.pychestrate.client import dispatch_consumer as dc_mod
from ccai.pychestrate.client.dispatch_consumer import (
    DispatchConsumerAspects,
    handle_one_dispatch,
)
from ccai.pychestrate.client.build_executor import (
    BUILD_STATUS_SUCCEEDED,
    BuildRunOutcome,
)
from ccai.pychestrate.client.provision_executor import (
    PROVISION_STATUS_SUCCEEDED,
    ProvisionRunOutcome,
)
from ccai.pychestrate.client.rpa_executor import (
    RPA_STATUS_SUCCEEDED,
    RpaRunOutcome,
)
from ccai.pychestrate.client.envelope_verifier import (
    serialize_envelope_canonical,
)
from ccai.pychestrate.client.result_uploader import (
    UPLOAD_STATUS_COMPLETE,
    ResultUploadOutcome,
)
from ccai.pychestrate.client.state import ClientState
from ccai.pychestrate.client.testbase_executor import (
    RUN_STATUS_SUCCEEDED,
    TestbaseRunOutcome,
)


SIGNING_KEY_ID = "envelope-key-2026"


def _base_envelope():
    """
        Returns a representative job-envelope dict (no ``job_kind`` set).
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
        "forwarding": {
            "progress_url": "u", "summary_url": "u", "auth_method": "mtls"},
        "deadline": {
            "soft_deadline_at": "2026-05-19T13:00:00+00:00",
            "hard_deadline_at": "2026-05-19T14:00:00+00:00",
        },
        "extension_claims": {},
    }
    return envelope


def _pending_dict(envelope, signature_hex):
    """
        Builds a PendingDispatch-shaped dict.
    """
    pending = {
        "dispatch_attempt_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "run_id": envelope["run_id"],
        "client_id": envelope["client_id"],
        "pod_id": envelope["pod_id"],
        "envelope_id": envelope["envelope_id"],
        "envelope": envelope,
        "signature": signature_hex,
        "signature_key_id": SIGNING_KEY_ID,
        "signed_at": "2026-05-19T12:00:00+00:00",
    }
    return pending


class _FakeDispatchGroup:
    def __init__(self):
        self.acks = []
        return

    def dispatch_ack_run(self, run_id, body, **kwargs):
        self.acks.append((run_id, body))
        return None


class _FakeRestClient:
    def __init__(self):
        self.dispatch = _FakeDispatchGroup()
        self._session = object()
        return

    def authenticate(self):
        return


def _signing_key_entry(signing_private_key):
    public_pem = signing_private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode("ascii")
    rtnval = {
        "key_id": SIGNING_KEY_ID,
        "algorithm": "ed25519",
        "public_key_pem": public_pem,
        "status": "active",
    }
    return rtnval


def _make_enrolled_state(state_dir, signing_private_key):
    state = ClientState(state_dir=state_dir)
    state.ensure_dir()
    state.write_private_key(
        key_pem=b"-----BEGIN PRIVATE KEY-----\nXX\n"
                b"-----END PRIVATE KEY-----\n")
    state.write_client_cert(
        cert_pem="-----BEGIN CERTIFICATE-----\nCC\n"
                 "-----END CERTIFICATE-----\n")
    state.write_service_ca(
        ca_pem="-----BEGIN CERTIFICATE-----\nCA\n"
               "-----END CERTIFICATE-----\n")
    state.write_client_id(client_id="55555555-5555-5555-5555-555555555555")
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


def _signed_pending(envelope, signing_key):
    signature_hex = signing_key.sign(
        serialize_envelope_canonical(envelope)).hex()
    rtnval = _pending_dict(envelope, signature_hex)
    return rtnval


class _Routing:
    """
        Records which executor / uploader the consumer invoked.
    """

    def __init__(self):
        self.test_runs = 0
        self.build_runs = 0
        self.provision_runs = 0
        self.test_uploads = 0
        self.build_uploads = 0
        self.provision_uploads = 0
        self.rpa_runs = 0
        self.rpa_uploads = 0
        return


def _install_fakes(routing):
    """
        Patches both executors + both uploaders on the dispatch_consumer
        module and returns the originals so the caller can restore them.
    """
    originals = {
        "execute_job_envelope": dc_mod.execute_job_envelope,
        "execute_build_envelope": dc_mod.execute_build_envelope,
        "execute_provision_envelope": dc_mod.execute_provision_envelope,
        "upload_run_results": dc_mod.upload_run_results,
        "upload_build_results": dc_mod.upload_build_results,
        "upload_provision_results": dc_mod.upload_provision_results,
        "execute_rpa_envelope": dc_mod.execute_rpa_envelope,
        "upload_rpa_results": dc_mod.upload_rpa_results,
    }

    def _fake_test_execute(*, state, envelope, local_run_id, aspects,
                           progress_reporter=None):
        routing.test_runs += 1
        rtnval = TestbaseRunOutcome(
            status=RUN_STATUS_SUCCEEDED, exit_code=0,
            output_dir="/tmp/test-out", command=["testbase"], detail="ok")
        return rtnval

    def _fake_build_execute(*, state, envelope, local_run_id, aspects,
                            progress_reporter=None):
        routing.build_runs += 1
        rtnval = BuildRunOutcome(
            status=BUILD_STATUS_SUCCEEDED, exit_code=0,
            output_dir="/tmp/build-out", source_dir="/tmp/build-src",
            command=["make", "all"], runner_kind="makefile", detail="ok")
        return rtnval

    def _fake_provision_execute(*, state, envelope, local_run_id, aspects,
                                progress_reporter=None):
        routing.provision_runs += 1
        rtnval = ProvisionRunOutcome(
            status=PROVISION_STATUS_SUCCEEDED, exit_code=0,
            output_dir="/tmp/provision-out", work_dir="/tmp/provision-work",
            command=["terraform", "apply"], driver_name="terraform",
            action="apply", detail="ok")
        return rtnval

    def _fake_test_upload(*, client, state, envelope, result_dir,
                          run_succeeded, aspects, run_cancelled=False, run_notes=None):
        routing.test_uploads += 1
        rtnval = ResultUploadOutcome(
            status=UPLOAD_STATUS_COMPLETE, manifest_posted=True,
            tree_uploaded=True, file_count=3, detail="ok")
        return rtnval

    def _fake_build_upload(*, client, state, envelope, result_dir,
                           run_succeeded, aspects, run_cancelled=False, run_notes=None):
        routing.build_uploads += 1
        rtnval = ResultUploadOutcome(
            status=UPLOAD_STATUS_COMPLETE, manifest_posted=True,
            tree_uploaded=True, file_count=2, detail="ok")
        return rtnval

    def _fake_provision_upload(*, client, state, envelope, result_dir,
                               run_succeeded, aspects, run_cancelled=False, run_notes=None):
        routing.provision_uploads += 1
        rtnval = ResultUploadOutcome(
            status=UPLOAD_STATUS_COMPLETE, manifest_posted=True,
            tree_uploaded=True, file_count=4, detail="ok")
        return rtnval

    def _fake_rpa_execute(*, state, envelope, local_run_id, aspects,
                          progress_reporter=None):
        routing.rpa_runs += 1
        rtnval = RpaRunOutcome(
            status=RPA_STATUS_SUCCEEDED, exit_code=0,
            output_dir="/tmp/rpa-out", work_dir="/tmp/rpa-work",
            command=["python", "-m", "jobs.invoice"], execution_mode="process",
            type_id="invoice-import", version="1.0.0", detail="ok")
        return rtnval

    def _fake_rpa_upload(*, client, state, envelope, result_dir,
                         run_succeeded, aspects, exit_code=None,
                         execution_mode=None, rpa_status=None):
        routing.rpa_uploads += 1
        rtnval = ResultUploadOutcome(
            status=UPLOAD_STATUS_COMPLETE, manifest_posted=True,
            tree_uploaded=True, file_count=2, detail="ok")
        return rtnval

    dc_mod.execute_job_envelope = _fake_test_execute
    dc_mod.execute_build_envelope = _fake_build_execute
    dc_mod.execute_provision_envelope = _fake_provision_execute
    dc_mod.upload_run_results = _fake_test_upload
    dc_mod.upload_build_results = _fake_build_upload
    dc_mod.upload_provision_results = _fake_provision_upload
    dc_mod.execute_rpa_envelope = _fake_rpa_execute
    dc_mod.upload_rpa_results = _fake_rpa_upload
    return originals


def _restore_fakes(originals):
    dc_mod.execute_job_envelope = originals["execute_job_envelope"]
    dc_mod.execute_build_envelope = originals["execute_build_envelope"]
    dc_mod.execute_provision_envelope = originals["execute_provision_envelope"]
    dc_mod.upload_run_results = originals["upload_run_results"]
    dc_mod.upload_build_results = originals["upload_build_results"]
    dc_mod.upload_provision_results = originals["upload_provision_results"]
    dc_mod.execute_rpa_envelope = originals["execute_rpa_envelope"]
    dc_mod.upload_rpa_results = originals["upload_rpa_results"]
    return


def test_job_kind_test_routes_to_testbase_executor():
    signing_key = Ed25519PrivateKey.generate()
    envelope = _base_envelope()
    envelope["job_kind"] = "test"
    pending = _signed_pending(envelope, signing_key)

    routing = _Routing()
    originals = _install_fakes(routing)
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = _make_enrolled_state(
                os.path.join(tmpdir, "state"), signing_key)
            client = _FakeRestClient()

            executed = handle_one_dispatch(
                client=client, state=state, pending=pending,
                aspects=DispatchConsumerAspects())

            testbase.assert_true(executed)
            testbase.assert_equal(routing.test_runs, 1)
            testbase.assert_equal(routing.build_runs, 0)
            testbase.assert_equal(routing.test_uploads, 1)
    finally:
        _restore_fakes(originals)
    return


def test_job_kind_build_routes_to_build_executor():
    signing_key = Ed25519PrivateKey.generate()
    envelope = _base_envelope()
    envelope["job_kind"] = "build"
    envelope["params"] = {
        "runner_kind": "makefile",
        "runner_inputs": {"targets": ["all"]},
    }
    pending = _signed_pending(envelope, signing_key)

    routing = _Routing()
    originals = _install_fakes(routing)
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = _make_enrolled_state(
                os.path.join(tmpdir, "state"), signing_key)
            client = _FakeRestClient()

            executed = handle_one_dispatch(
                client=client, state=state, pending=pending,
                aspects=DispatchConsumerAspects())

            testbase.assert_true(executed)
            testbase.assert_equal(routing.build_runs, 1)
            testbase.assert_equal(routing.test_runs, 0)
            testbase.assert_equal(routing.build_uploads, 1)

            _, ack_body = client.dispatch.acks[0]
            testbase.assert_true("ACCEPTED" in ack_body.notes)
    finally:
        _restore_fakes(originals)
    return


def test_job_kind_provision_routes_to_provision_executor():
    signing_key = Ed25519PrivateKey.generate()
    envelope = _base_envelope()
    envelope["job_kind"] = "provision"
    envelope["params"] = {
        "environment_id": "env-1",
        "action": "apply",
        "driver_name": "terraform",
        "driver_inputs": {"working_directory": "/opt/infra"},
        "inputs_canonical_sha256": "abc123",
    }
    pending = _signed_pending(envelope, signing_key)

    routing = _Routing()
    originals = _install_fakes(routing)
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = _make_enrolled_state(
                os.path.join(tmpdir, "state"), signing_key)
            client = _FakeRestClient()

            executed = handle_one_dispatch(
                client=client, state=state, pending=pending,
                aspects=DispatchConsumerAspects())

            testbase.assert_true(executed)
            testbase.assert_equal(routing.provision_runs, 1)
            testbase.assert_equal(routing.test_runs, 0)
            testbase.assert_equal(routing.build_runs, 0)
            testbase.assert_equal(routing.provision_uploads, 1)

            # The dispatch is acked with an acceptance note.
            testbase.assert_equal(len(client.dispatch.acks), 1)
            _, ack_body = client.dispatch.acks[0]
            testbase.assert_true("ACCEPTED" in ack_body.notes)
    finally:
        _restore_fakes(originals)
    return


def test_job_kind_rpa_routes_to_rpa_executor():
    signing_key = Ed25519PrivateKey.generate()
    envelope = _base_envelope()
    envelope["job_kind"] = "rpa"
    envelope["params"] = {
        "type_id": "invoice-import",
        "version": "1.0.0",
        "execution_mode": "process",
        "entrypoint": {"command": ["python", "-m", "jobs.invoice"]},
        "work_packet": {"customer_id": 42},
    }
    pending = _signed_pending(envelope, signing_key)

    routing = _Routing()
    originals = _install_fakes(routing)
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = _make_enrolled_state(
                os.path.join(tmpdir, "state"), signing_key)
            client = _FakeRestClient()

            executed = handle_one_dispatch(
                client=client, state=state, pending=pending,
                aspects=DispatchConsumerAspects())

            testbase.assert_true(executed)
            testbase.assert_equal(routing.rpa_runs, 1)
            testbase.assert_equal(routing.rpa_uploads, 1)
            testbase.assert_equal(routing.test_runs, 0)
            testbase.assert_equal(routing.build_runs, 0)

            _, ack_body = client.dispatch.acks[0]
            testbase.assert_equal(ack_body.notes, "ACCEPTED: starting job_kind=rpa")
    finally:
        _restore_fakes(originals)
    return


def test_missing_job_kind_defaults_to_test():
    signing_key = Ed25519PrivateKey.generate()
    envelope = _base_envelope()
    # No job_kind key at all.
    pending = _signed_pending(envelope, signing_key)

    routing = _Routing()
    originals = _install_fakes(routing)
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = _make_enrolled_state(
                os.path.join(tmpdir, "state"), signing_key)
            client = _FakeRestClient()

            executed = handle_one_dispatch(
                client=client, state=state, pending=pending,
                aspects=DispatchConsumerAspects())

            testbase.assert_true(executed)
            testbase.assert_equal(routing.test_runs, 1)
            testbase.assert_equal(routing.build_runs, 0)
    finally:
        _restore_fakes(originals)
    return


def test_unknown_job_kind_defaults_to_test():
    signing_key = Ed25519PrivateKey.generate()
    envelope = _base_envelope()
    envelope["job_kind"] = "wat-is-this"
    pending = _signed_pending(envelope, signing_key)

    routing = _Routing()
    originals = _install_fakes(routing)
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = _make_enrolled_state(
                os.path.join(tmpdir, "state"), signing_key)
            client = _FakeRestClient()

            executed = handle_one_dispatch(
                client=client, state=state, pending=pending,
                aspects=DispatchConsumerAspects())

            testbase.assert_true(executed)
            testbase.assert_equal(routing.test_runs, 1)
            testbase.assert_equal(routing.build_runs, 0)
    finally:
        _restore_fakes(originals)
    return

"""
    TestBase tests for the pychestrate-client provision executor. These
    tests mock the subprocess layer (the infra-tool invocation); no real
    provisioning, ``terraform`` invocation, or shell command is performed.
"""

import os
import subprocess
import tempfile

from ccai import testbase

from ccai.errors.exceptions import BadParameterError

from ccai.pychestrate.client.state import ClientState
from ccai.pychestrate.client.provision_executor import (
    PROVISION_STATUS_FAILED,
    PROVISION_STATUS_SUCCEEDED,
    ProvisionExecutorAspects,
    build_provision_command,
    execute_provision_envelope,
    resolve_provision_dirs,
)


def _sample_terraform_envelope():
    """
        Returns a representative verified ``job_kind=provision`` envelope
        carrying a ``terraform`` provisioning spec.
    """
    envelope = {
        "envelope_id": "11111111-1111-1111-1111-111111111111",
        "run_id": "44444444-4444-4444-4444-444444444444",
        "client_id": "55555555-5555-5555-5555-555555555555",
        "job_id": "33333333-3333-3333-3333-333333333333",
        "job_name": "provision:terraform",
        "job_type": "provision-terraform",
        "job_kind": "provision",
        "params": {
            "environment_id": "env-prod-1",
            "action": "apply",
            "driver_name": "terraform",
            "driver_inputs": {
                "working_directory": "/opt/infra/prod",
                "var_file": "prod.tfvars",
                "parallelism": 4,
                "backend_config": {"bucket": "tfstate", "region": "us-east-1"},
                "env": {"TF_LOG": "INFO"},
            },
            "inputs_canonical_sha256": "abc123",
        },
        "results": {"backend_key_prefix": "p", "backend_driver": "s3"},
    }
    return envelope


def _sample_command_envelope():
    """
        Returns a representative verified ``job_kind=provision`` envelope
        carrying a generic ``command`` provisioning spec.
    """
    envelope = {
        "envelope_id": "22222222-2222-2222-2222-222222222222",
        "run_id": "77777777-7777-7777-7777-777777777777",
        "client_id": "55555555-5555-5555-5555-555555555555",
        "job_kind": "provision",
        "params": {
            "environment_id": "env-prod-2",
            "action": "apply",
            "driver_name": "command",
            "driver_inputs": {
                "command": "/usr/bin/provision.sh",
                "args": ["--env", "prod"],
                "working_dir": "/opt/scripts",
            },
            "inputs_canonical_sha256": "def456",
        },
        "results": {"backend_key_prefix": "p", "backend_driver": "s3"},
    }
    return envelope


class _CompletedStub:
    """
        Minimal ``CompletedProcess`` stand-in.
    """

    def __init__(self, returncode, stdout=b"", stderr=b""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        return


class _RunRecorder:
    """
        Records every ``run_func`` call and returns scripted results.
    """

    def __init__(self, *, returncode=0, raise_exc=None):
        self._returncode = returncode
        self._raise_exc = raise_exc
        self.calls = []
        return

    def __call__(self, command, **kwargs):
        self.calls.append({"command": list(command), "kwargs": dict(kwargs)})

        if self._raise_exc is not None:
            raise self._raise_exc

        rtnval = _CompletedStub(
            returncode=self._returncode, stdout=b"provision log")
        return rtnval


def test_build_provision_command_translates_terraform_envelope():
    envelope = _sample_terraform_envelope()

    command = build_provision_command(envelope=envelope)

    testbase.assert_equal(command[0], "terraform")
    testbase.assert_equal(command[1], "apply")
    testbase.assert_true("-auto-approve" in command)
    testbase.assert_true("-input=false" in command)
    testbase.assert_true("-var-file=prod.tfvars" in command)
    testbase.assert_true("-parallelism=4" in command)
    testbase.assert_true("-backend-config=bucket=tfstate" in command)
    testbase.assert_true("-backend-config=region=us-east-1" in command)
    return


def test_build_provision_command_translates_plan_action():
    envelope = _sample_terraform_envelope()
    envelope["params"]["action"] = "plan"

    command = build_provision_command(envelope=envelope)

    testbase.assert_equal(command[1], "plan")
    # plan must not carry auto-approve.
    testbase.assert_false("-auto-approve" in command)
    return


def test_build_provision_command_translates_command_driver():
    envelope = _sample_command_envelope()

    command = build_provision_command(envelope=envelope)

    testbase.assert_equal(command[0], "/usr/bin/provision.sh")
    testbase.assert_true("--env" in command)
    testbase.assert_true("prod" in command)
    return


def test_build_provision_command_rejects_missing_working_directory():
    envelope = _sample_terraform_envelope()
    del envelope["params"]["driver_inputs"]["working_directory"]

    raised = False
    try:
        build_provision_command(envelope=envelope)
    except BadParameterError:
        raised = True
    testbase.assert_true(raised)
    return


def test_build_provision_command_rejects_unsupported_action():
    envelope = _sample_terraform_envelope()
    envelope["params"]["action"] = "teleport"

    raised = False
    try:
        build_provision_command(envelope=envelope)
    except BadParameterError:
        raised = True
    testbase.assert_true(raised)
    return


def test_build_provision_command_rejects_untranslatable_driver():
    envelope = _sample_terraform_envelope()
    envelope["params"]["driver_name"] = "ansible"
    envelope["params"]["driver_inputs"] = {}

    raised = False
    try:
        build_provision_command(envelope=envelope)
    except BadParameterError:
        raised = True
    testbase.assert_true(raised)
    return


def test_build_provision_command_unknown_driver_falls_back_to_command():
    envelope = _sample_terraform_envelope()
    envelope["params"]["driver_name"] = "ansible"
    envelope["params"]["driver_inputs"] = {
        "command": "ansible-playbook",
        "args": ["site.yml"],
    }

    command = build_provision_command(envelope=envelope)

    testbase.assert_equal(command[0], "ansible-playbook")
    testbase.assert_true("site.yml" in command)
    return


def test_execute_provision_envelope_runs_terraform_with_env_and_workdir():
    recorder = _RunRecorder()

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        aspects = ProvisionExecutorAspects(run_func=recorder)
        envelope = _sample_terraform_envelope()

        outcome = execute_provision_envelope(
            state=state, envelope=envelope, local_run_id="local-provision-1",
            aspects=aspects)

        testbase.assert_true(outcome.succeeded)
        testbase.assert_equal(outcome.status, PROVISION_STATUS_SUCCEEDED)
        testbase.assert_equal(outcome.exit_code, 0)

        # The terraform invocation runs in the declared working directory.
        provision_call = recorder.calls[-1]
        testbase.assert_equal(
            os.path.basename(provision_call["command"][0]), "terraform")
        testbase.assert_equal(
            provision_call["kwargs"]["cwd"], "/opt/infra/prod")
        # The provision env overlay is applied.
        run_env = provision_call["kwargs"]["env"]
        testbase.assert_equal(run_env.get("TF_LOG"), "INFO")

        # A local provision record was written.
        provision_dirs = resolve_provision_dirs(
            state=state, run_id=envelope["run_id"])
        record_path = os.path.join(
            provision_dirs["record_dir"], "provision-record.json")
        testbase.assert_true(os.path.isfile(record_path))
    return


def test_execute_provision_envelope_marks_failed_on_nonzero_exit():
    recorder = _RunRecorder(returncode=1)

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        aspects = ProvisionExecutorAspects(run_func=recorder)
        envelope = _sample_terraform_envelope()

        outcome = execute_provision_envelope(
            state=state, envelope=envelope, local_run_id="local-provision-2",
            aspects=aspects)

        testbase.assert_false(outcome.succeeded)
        testbase.assert_equal(outcome.status, PROVISION_STATUS_FAILED)
        testbase.assert_equal(outcome.exit_code, 1)
    return


def test_execute_provision_envelope_survives_missing_infra_tool():
    recorder = _RunRecorder(
        raise_exc=FileNotFoundError("terraform not found on this pod"))

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        aspects = ProvisionExecutorAspects(run_func=recorder)
        envelope = _sample_terraform_envelope()

        # Must NOT raise -- a missing infra tool is a failed provision.
        outcome = execute_provision_envelope(
            state=state, envelope=envelope, local_run_id="local-provision-3",
            aspects=aspects)

        testbase.assert_false(outcome.succeeded)
        testbase.assert_equal(outcome.status, PROVISION_STATUS_FAILED)
        testbase.assert_true(outcome.exit_code is None)
    return


def test_execute_provision_envelope_survives_timeout():
    recorder = _RunRecorder(
        raise_exc=subprocess.TimeoutExpired(cmd="terraform", timeout=1.0))

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        aspects = ProvisionExecutorAspects(run_func=recorder)
        envelope = _sample_terraform_envelope()

        # Must NOT raise -- a timeout is a failed provision.
        outcome = execute_provision_envelope(
            state=state, envelope=envelope, local_run_id="local-provision-4",
            aspects=aspects)

        testbase.assert_false(outcome.succeeded)
        testbase.assert_equal(outcome.status, PROVISION_STATUS_FAILED)
    return


def test_execute_provision_envelope_survives_untranslatable_envelope():
    recorder = _RunRecorder()

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        aspects = ProvisionExecutorAspects(run_func=recorder)
        envelope = _sample_terraform_envelope()
        del envelope["params"]["driver_inputs"]["working_directory"]

        # An untranslatable envelope is a failed provision, not a crash.
        outcome = execute_provision_envelope(
            state=state, envelope=envelope, local_run_id="local-provision-5",
            aspects=aspects)

        testbase.assert_false(outcome.succeeded)
        testbase.assert_equal(outcome.status, PROVISION_STATUS_FAILED)
        # The infra tool was never invoked.
        testbase.assert_equal(len(recorder.calls), 0)
    return

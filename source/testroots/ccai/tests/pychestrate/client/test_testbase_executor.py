"""
    TestBase tests for the pychestrate-client testbase executor. These
    tests mock the subprocess layer; no real ``testbase`` invocation is
    performed.
"""

import os
import tempfile

from ccai import testbase

from ccai.errors.exceptions import BadParameterError

from ccai.pychestrate.client.state import ClientState
from ccai.pychestrate.client.testbase_executor import (
    RUN_STATUS_FAILED,
    RUN_STATUS_SUCCEEDED,
    TestbaseExecutorAspects,
    build_testbase_command,
    execute_job_envelope,
)


def _sample_envelope():
    """
        Returns a representative verified job-envelope dict.
    """
    envelope = {
        "envelope_id": "11111111-1111-1111-1111-111111111111",
        "run_id": "44444444-4444-4444-4444-444444444444",
        "job_id": "33333333-3333-3333-3333-333333333333",
        "job_name": "smoke-suite",
        "job_type": "testbase-singlehost",
        "refs": {"branch": "main", "build": "1234", "flavor": "release"},
        "params": {
            "test_root": "/opt/project/source/testroots/ccai",
            "includes": ["ccai.tests.singlehost"],
        },
    }
    return envelope


class _FakeCompleted:
    """
        Minimal stand-in for a :class:`subprocess.CompletedProcess`.
    """

    def __init__(self, returncode):
        self.returncode = returncode
        return


class _RunRecorder:
    """
        Records the argv passed to the subprocess-runner and returns a
        scripted exit code.
    """

    def __init__(self, returncode):
        self._returncode = returncode
        self.calls = []
        return

    def __call__(self, command, **kwargs):
        self.calls.append((command, kwargs))
        return _FakeCompleted(self._returncode)


def test_build_command_translates_envelope():
    envelope = _sample_envelope()

    command = build_testbase_command(
        envelope=envelope,
        output_dir="/tmp/out",
        testbase_binary="testbase",
    )

    testbase.assert_equal(command[0], "testbase")
    testbase.assert_equal(command[1], "testing")
    testbase.assert_equal(command[2], "run")
    testbase.assert_true("--root" in command)
    testbase.assert_true("/opt/project/source/testroots/ccai" in command)
    testbase.assert_true("--output" in command)
    testbase.assert_true("/tmp/out" in command)
    testbase.assert_true("--includes=ccai.tests.singlehost" in command)
    testbase.assert_true("--branch" in command)
    testbase.assert_true("--runid" in command)
    return


def test_build_command_rejects_missing_test_root():
    envelope = _sample_envelope()
    del envelope["params"]["test_root"]

    raised = False
    try:
        build_testbase_command(
            envelope=envelope, output_dir="/tmp/out", testbase_binary="testbase")
    except BadParameterError:
        raised = True
    testbase.assert_true(raised)
    return


def test_execute_job_envelope_records_success():
    recorder = _RunRecorder(returncode=0)

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        aspects = TestbaseExecutorAspects(run_func=recorder)
        envelope = _sample_envelope()

        outcome = execute_job_envelope(
            state=state,
            envelope=envelope,
            local_run_id="local-1",
            aspects=aspects,
        )

        testbase.assert_true(outcome.succeeded)
        testbase.assert_equal(outcome.status, RUN_STATUS_SUCCEEDED)
        testbase.assert_equal(outcome.exit_code, 0)
        testbase.assert_equal(len(recorder.calls), 1)

        invoked_command = recorder.calls[0][0]
        testbase.assert_equal(invoked_command[0], "testbase")

        record_path = os.path.join(
            state.runs_dir, envelope["run_id"], "run-record.json")
        testbase.assert_true(os.path.isfile(record_path))
    return


def test_execute_job_envelope_records_failure_on_nonzero_exit():
    recorder = _RunRecorder(returncode=7)

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        aspects = TestbaseExecutorAspects(run_func=recorder)
        envelope = _sample_envelope()

        outcome = execute_job_envelope(
            state=state,
            envelope=envelope,
            local_run_id="local-2",
            aspects=aspects,
        )

        testbase.assert_false(outcome.succeeded)
        testbase.assert_equal(outcome.status, RUN_STATUS_FAILED)
        testbase.assert_equal(outcome.exit_code, 7)
    return


def test_execute_job_envelope_survives_missing_binary():
    def _missing_binary(command, **kwargs):
        raise FileNotFoundError("testbase: command not found")

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        aspects = TestbaseExecutorAspects(run_func=_missing_binary)
        envelope = _sample_envelope()

        # Must NOT raise — a missing binary is recorded as a failed run.
        outcome = execute_job_envelope(
            state=state,
            envelope=envelope,
            local_run_id="local-3",
            aspects=aspects,
        )

        testbase.assert_false(outcome.succeeded)
        testbase.assert_equal(outcome.status, RUN_STATUS_FAILED)
        testbase.assert_true(outcome.exit_code is None)
    return


def test_execute_job_envelope_survives_unbuildable_envelope():
    recorder = _RunRecorder(returncode=0)

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        aspects = TestbaseExecutorAspects(run_func=recorder)
        envelope = _sample_envelope()
        del envelope["params"]["test_root"]

        # Must NOT raise — an unbuildable command is a failed run.
        outcome = execute_job_envelope(
            state=state,
            envelope=envelope,
            local_run_id="local-4",
            aspects=aspects,
        )

        testbase.assert_false(outcome.succeeded)
        testbase.assert_equal(outcome.status, RUN_STATUS_FAILED)
        testbase.assert_equal(len(recorder.calls), 0)
    return

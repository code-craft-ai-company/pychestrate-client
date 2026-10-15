"""
    TestBase tests for the pychestrate-client testbase executor. These
    tests mock the subprocess layer; no real ``testbase`` invocation is
    performed.
"""

import json
import os
import tempfile

from ccai import testbase

from ccai.errors.exceptions import BadParameterError

from ccai.pychestrate.client.state import ClientState
from ccai.pychestrate.client.cancellable_run import RunCancelled
from ccai.pychestrate.client.testbase_executor import (
    describe_failed_run,
    RUN_STATUS_CANCELLED,
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


def test_execute_job_envelope_records_a_cancelled_run():
    """
    **Feature: cancel (E2E-09)**
    **Property: a run the cancellable runner stopped is recorded as
    cancelled, not failed.**
    """
    def _cancelled_runner(command, **kwargs):
        raise RunCancelled("The run was cancelled by an operator; the job was stopped.",
                           returncode=-15)

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()
        outcome = execute_job_envelope(
            state=state, envelope=_sample_envelope(), local_run_id="local-c",
            aspects=TestbaseExecutorAspects(run_func=_cancelled_runner))

    testbase.assert_equal(outcome.status, RUN_STATUS_CANCELLED)
    testbase.assert_true(outcome.cancelled)
    testbase.assert_false(outcome.succeeded)
    return


def _summary(directory, **detail):
    with open(os.path.join(directory, "testrun_summary.json"), "w", encoding="utf-8") as handle:
        json.dump({"result": "FAILED", "detail": detail}, handle)
    return


def test_a_failed_run_is_described_by_what_failed_not_by_an_exit_code():
    """
        testbase exits non-zero when a test fails; the reason names the counts from its own summary, and falls back to the exit code only when it has none.
    """
    with tempfile.TemporaryDirectory() as empty, tempfile.TemporaryDirectory() as one, tempfile.TemporaryDirectory() as errs, tempfile.TemporaryDirectory() as both, \
            tempfile.TemporaryDirectory() as clean, tempfile.TemporaryDirectory() as broken, tempfile.TemporaryDirectory() as single:
        _summary(one, errors=0, failed=1, skipped=0, passed=1, total=2)
        _summary(errs, errors=2, failed=0, skipped=0, passed=3, total=5)
        _summary(both, errors=1, failed=2, skipped=0, passed=2, total=5)
        _summary(clean, errors=0, failed=0, skipped=0, passed=2, total=2)
        _summary(single, errors=0, failed=1, skipped=0, passed=0, total=1)
        with open(os.path.join(broken, "testrun_summary.json"), "w", encoding="utf-8") as handle:
            handle.write("{not json")
        answers = [
            describe_failed_run(one, 255), describe_failed_run(errs, 255), describe_failed_run(both, 255), describe_failed_run(single, 255),
            describe_failed_run(clean, 255), describe_failed_run(broken, 255), describe_failed_run(empty, 255), describe_failed_run(os.path.join(empty, "missing"), 7),
        ]

    testbase.assert_equal(
        answers,
        [
            "1 of 2 tests failed.", "2 of 5 tests errored.", "3 of 5 tests did not pass (2 failed, 1 errored).", "1 of 1 test failed.",
            "testbase run exited with code 255.", "testbase run exited with code 255.", "testbase run exited with code 255.", "testbase run exited with code 7.",
        ],
    )
    return


def test_the_executor_records_the_failure_reason_from_testbases_summary():
    """
        A run whose testbase exits non-zero with a summary in the output directory records the described reason as the outcome's detail.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()
        envelope = _sample_envelope()

        def run_and_write_summary(command, **kwargs):
            output = os.path.join(state.runs_dir, str(envelope.get("run_id", "")), "output")
            os.makedirs(output, exist_ok=True)
            _summary(output, errors=0, failed=1, skipped=0, passed=1, total=2)
            return _FakeCompleted(255)

        outcome = execute_job_envelope(state=state, envelope=envelope, local_run_id="local-9", aspects=TestbaseExecutorAspects(run_func=run_and_write_summary))

        testbase.assert_equal((outcome.status, outcome.exit_code, outcome.detail), (RUN_STATUS_FAILED, 255, "1 of 2 tests failed."))
    return

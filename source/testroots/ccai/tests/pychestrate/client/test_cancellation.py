"""
    TestBase tests for pychestrate-client run cancellation.

    Covers the three pieces the cancel path is built from:

    * :class:`CancellationRegistry` -- the hand-off between the
      heartbeat thread that receives a cancel and the dispatch thread
      that has to act on it.
    * :func:`run_supervised_process` -- terminating a real process tree
      on cancel, refusing to launch a run that is already cancelled, and
      enforcing the wall-clock cap.
    * The heartbeat delivery path -- reading cancellations off a
      ``ClientHeartbeatAck`` body and recording them.

    Also covers the terminal-outcome classification the executors report
    now that a timeout, a missing binary, a cancel, and a genuine
    non-zero exit are distinguishable.

    No real network traffic is performed. The process tests DO spawn
    real short-lived ``python`` children, because process-group
    termination is the behaviour under test and a mock cannot exhibit
    it.
"""

import os
import subprocess
import sys
import tempfile
import time

from ccai import testbase

from ccai.pychestrate.client import heartbeat as heartbeat_mod
from ccai.pychestrate.client.cancellationregistry import (
    DEFAULT_CANCEL_REASON,
    CancellationRegistry,
    active_cancellation_registry,
    set_active_cancellation_registry,
)
from ccai.pychestrate.client.process_supervisor import (
    RunCancelledError,
    SupervisedProcessAspects,
    run_supervised_process,
)
from ccai.pychestrate.client.run_outcomes import (
    FAILURE_CLASSIFICATION_CANCELLED,
    FAILURE_CLASSIFICATION_EXECUTABLE_NOT_FOUND,
    FAILURE_CLASSIFICATION_NONZERO_EXIT,
    FAILURE_CLASSIFICATION_SETUP_ERROR,
    FAILURE_CLASSIFICATION_TIMEOUT,
    TERMINAL_OUTCOME_CANCELLED,
    TERMINAL_OUTCOME_FAILURE,
    TERMINAL_OUTCOME_SUCCESS,
)
from ccai.pychestrate.client.state import ClientState
from ccai.pychestrate.client.testbase_executor import (
    RUN_STATUS_CANCELLED,
    RUN_STATUS_FAILED,
    RUN_STATUS_SUCCEEDED,
    TestbaseExecutorAspects,
    TestbaseRunOutcome,
    execute_job_envelope,
)


RUN_ID = "44444444-4444-4444-4444-444444444444"
OTHER_RUN_ID = "99999999-9999-9999-9999-999999999999"

#: Aspects tuned for tests: a short grace period and a tight poll so a
#: cancellation is observed in well under a second.
FAST_SUPERVISION = SupervisedProcessAspects(
    cancel_grace_seconds=2.0, poll_interval_seconds=0.05)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _sleep_command(seconds):
    """
        Builds an argv list for a child that sleeps and then exits 0.
    """
    rtnval = [sys.executable, "-c",
              "import time; time.sleep({})".format(seconds)]
    return rtnval


def _cancelling_registry(*, run_id, after_seconds):
    """
        Returns a registry that reports ``run_id`` cancelled once
        ``after_seconds`` have elapsed since it was built.

        Driving the cancel from elapsed time rather than a background
        thread keeps the test deterministic while still exercising the
        supervisor's poll loop.
    """

    target_run_id = run_id

    class _TimedRegistry(CancellationRegistry):

        def __init__(self):
            super().__init__()
            self._started_at = time.monotonic()
            return

        def is_cancelled(self, *, run_id):
            if run_id != target_run_id:
                rtnval = False
                return rtnval
            elapsed = time.monotonic() - self._started_at
            rtnval = elapsed >= after_seconds
            return rtnval

    rtnval = _TimedRegistry()
    rtnval.record(run_id=run_id, request_id="req-1", reason="operator asked")
    return rtnval


def _make_state(state_dir):
    """
        Builds a minimally-populated :class:`ClientState`.
    """
    state = ClientState(state_dir=state_dir)
    state.ensure_dir()
    rtnval = state
    return rtnval


def _envelope(test_root):
    """
        Returns a job envelope carrying a resolvable testbase root.
    """
    rtnval = {
        "envelope_id": "11111111-1111-1111-1111-111111111111",
        "run_id": RUN_ID,
        "job_type": "testbase-singlehost",
        "params": {"test_root": test_root},
        "refs": {},
    }
    return rtnval


# ---------------------------------------------------------------------
# CancellationRegistry
# ---------------------------------------------------------------------


def test_registry_reports_a_recorded_run_as_cancelled():
    """
        **Feature: run cancellation**
        **Property: a recorded run reads back as cancelled.**
    """
    registry = CancellationRegistry()
    registry.record(run_id=RUN_ID, request_id="req-1", reason="stop it")
    testbase.assert_true(registry.is_cancelled(run_id=RUN_ID))
    return


def test_registry_reports_an_unrecorded_run_as_not_cancelled():
    """
        **Feature: run cancellation**
        **Property: a run nobody cancelled is not cancelled.**
    """
    registry = CancellationRegistry()
    registry.record(run_id=RUN_ID, request_id="req-1", reason="stop it")
    testbase.assert_equal(registry.is_cancelled(run_id=OTHER_RUN_ID), False)
    return


def test_registry_keeps_the_first_reason_on_a_repeat_delivery():
    """
        **Feature: run cancellation**
        **Property: at-least-once re-delivery does not overwrite the
        reason recorded the first time.**
    """
    registry = CancellationRegistry()
    registry.record(run_id=RUN_ID, request_id="req-1", reason="first")
    registry.record(run_id=RUN_ID, request_id="req-2", reason="second")
    record = registry.find(run_id=RUN_ID)
    testbase.assert_equal(record.reason, "first")
    return


def test_registry_reports_a_repeat_delivery_as_not_newly_recorded():
    """
        **Feature: run cancellation**
        **Property: record() distinguishes a first delivery from a
        repeat so the caller can log the difference.**
    """
    registry = CancellationRegistry()
    first = registry.record(run_id=RUN_ID, reason="stop")
    repeat = registry.record(run_id=RUN_ID, reason="stop")
    testbase.assert_true(first)
    testbase.assert_equal(repeat, False)
    return


def test_registry_supplies_a_default_reason_when_none_was_delivered():
    """
        **Feature: run cancellation**
        **Property: a cancellation delivered with no reason still
        records something an operator can read.**
    """
    registry = CancellationRegistry()
    registry.record(run_id=RUN_ID, request_id=None, reason=None)
    record = registry.find(run_id=RUN_ID)
    testbase.assert_equal(record.reason, DEFAULT_CANCEL_REASON)
    return


def test_registry_forget_clears_the_record():
    """
        **Feature: run cancellation**
        **Property: forgetting a finished run stops it reading as
        cancelled, so the registry does not grow without bound.**
    """
    registry = CancellationRegistry()
    registry.record(run_id=RUN_ID, reason="stop")
    dropped = registry.forget(run_id=RUN_ID)
    testbase.assert_true(dropped)
    testbase.assert_equal(registry.is_cancelled(run_id=RUN_ID), False)
    return


def test_registry_forget_of_an_unknown_run_is_a_no_op():
    """
        **Feature: run cancellation**
        **Property: forgetting a run that was never cancelled reports
        False rather than raising.**
    """
    registry = CancellationRegistry()
    dropped = registry.forget(run_id=OTHER_RUN_ID)
    testbase.assert_equal(dropped, False)
    return


def test_registry_ignores_an_empty_run_id():
    """
        **Feature: run cancellation**
        **Property: a cancellation with no run id is not recorded under
        a blank key, where it would match nothing.**
    """
    registry = CancellationRegistry()
    recorded = registry.record(run_id="", reason="stop")
    testbase.assert_equal(recorded, False)
    testbase.assert_equal(len(registry.cancelled_run_ids()), 0)
    return


def test_active_registry_accessor_returns_the_published_instance():
    """
        **Feature: run cancellation**
        **Property: the process-wide registry accessor returns whatever
        was last published, so the two loops share one instance.**
    """
    original = active_cancellation_registry()
    replacement = CancellationRegistry()
    try:
        set_active_cancellation_registry(registry=replacement)
        testbase.assert_true(active_cancellation_registry() is replacement)
    finally:
        set_active_cancellation_registry(registry=original)
    return


# ---------------------------------------------------------------------
# run_supervised_process
# ---------------------------------------------------------------------


def test_supervised_process_returns_the_exit_code_of_a_clean_run():
    """
        **Feature: run cancellation**
        **Property: supervision does not change the outcome of a run
        nobody cancels.**
    """
    registry = CancellationRegistry()
    result = run_supervised_process(
        command=[sys.executable, "-c", "raise SystemExit(0)"],
        run_id=RUN_ID, cancellation=registry, aspects=FAST_SUPERVISION)
    testbase.assert_equal(result.returncode, 0)
    return


def test_supervised_process_reports_a_nonzero_exit_code():
    """
        **Feature: run cancellation**
        **Property: a supervised run that exits non-zero reports that
        code rather than being mistaken for a cancel.**
    """
    registry = CancellationRegistry()
    result = run_supervised_process(
        command=[sys.executable, "-c", "raise SystemExit(7)"],
        run_id=RUN_ID, cancellation=registry, aspects=FAST_SUPERVISION)
    testbase.assert_equal(result.returncode, 7)
    return


def test_supervised_process_captures_stdout():
    """
        **Feature: run cancellation**
        **Property: output is captured despite the supervision poll
        loop -- the capture uses files, not pipes, so a chatty job
        cannot deadlock the loop that has to be able to cancel it.**
    """
    registry = CancellationRegistry()
    result = run_supervised_process(
        command=[sys.executable, "-c", "print('hello-from-child')"],
        run_id=RUN_ID, cancellation=registry, aspects=FAST_SUPERVISION)
    testbase.assert_true(b"hello-from-child" in result.stdout)
    return


def test_supervised_process_refuses_to_launch_an_already_cancelled_run():
    """
        **Feature: run cancellation**
        **Property: a cancel that lands between dispatch and execution
        prevents the execution entirely.**
    """
    registry = CancellationRegistry()
    registry.record(run_id=RUN_ID, reason="cancelled before launch")

    launched = {"count": 0}

    def _tracking_popen(*args, **kwargs):
        launched["count"] += 1
        raise AssertionError("the process must not be launched")

    aspects = SupervisedProcessAspects(
        cancel_grace_seconds=1.0, poll_interval_seconds=0.05,
        popen_func=_tracking_popen)

    raised = False
    try:
        run_supervised_process(
            command=_sleep_command(30), run_id=RUN_ID,
            cancellation=registry, aspects=aspects)
    except RunCancelledError:
        raised = True

    testbase.assert_true(raised)
    testbase.assert_equal(launched["count"], 0)
    return


def test_supervised_process_cancel_carries_the_operator_reason():
    """
        **Feature: run cancellation**
        **Property: the reason the operator gave survives to the
        exception, so it can reach the ack note and the result
        manifest.**
    """
    registry = CancellationRegistry()
    registry.record(run_id=RUN_ID, reason="pod needed for a hotfix")

    captured = {"reason": None}
    try:
        run_supervised_process(
            command=_sleep_command(30), run_id=RUN_ID,
            cancellation=registry, aspects=FAST_SUPERVISION)
    except RunCancelledError as cancelerr:
        captured["reason"] = cancelerr.reason

    testbase.assert_equal(captured["reason"], "pod needed for a hotfix")
    return


def test_supervised_process_terminates_a_running_child_on_cancel():
    """
        **Feature: run cancellation**
        **Property: a cancel arriving while the job runs stops it,
        rather than waiting for it to finish on its own.**
    """
    registry = _cancelling_registry(run_id=RUN_ID, after_seconds=0.3)

    started = time.monotonic()
    raised = False
    try:
        run_supervised_process(
            command=_sleep_command(60), run_id=RUN_ID,
            cancellation=registry, aspects=FAST_SUPERVISION)
    except RunCancelledError:
        raised = True
    elapsed = time.monotonic() - started

    testbase.assert_true(raised)
    # The child was told to sleep for a minute; returning in a couple of
    # seconds is only possible if it was actually killed.
    testbase.assert_true(elapsed < 20.0)
    return


def test_supervised_process_kills_the_whole_process_tree_on_cancel():
    """
        **Feature: run cancellation**
        **Property: the job's grandchildren die too. Signalling only
        the direct child would leave orphans holding the devices,
        ports, and licences the next job needs.**
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        pid_path = os.path.join(tmpdir, "grandchild.pid")
        script = (
            "import subprocess, sys, time\n"
            "kid = subprocess.Popen([sys.executable, '-c',"
            " 'import time; time.sleep(120)'])\n"
            "handle = open({!r}, 'w')\n"
            "handle.write(str(kid.pid))\n"
            "handle.close()\n"
            "time.sleep(120)\n"
        ).format(pid_path)

        registry = _cancelling_registry(run_id=RUN_ID, after_seconds=1.0)
        try:
            run_supervised_process(
                command=[sys.executable, "-c", script], run_id=RUN_ID,
                cancellation=registry, aspects=FAST_SUPERVISION)
        except RunCancelledError:
            pass

        # Give the kernel a moment to finish reaping the group.
        time.sleep(0.5)

        raw = ""
        if os.path.isfile(pid_path):
            with open(pid_path, "r", encoding="utf-8") as pidf:
                raw = pidf.read().strip()

        testbase.assert_true(len(raw) > 0)
        grandchild_pid = int(raw)

        alive = True
        try:
            os.kill(grandchild_pid, 0)
        except OSError:
            alive = False

        testbase.assert_equal(alive, False)
    return


def test_supervised_process_raises_timeout_when_the_cap_elapses():
    """
        **Feature: terminal outcomes**
        **Property: the wall-clock cap still fires under supervision
        and is reported as a timeout, not as a cancel.**
    """
    registry = CancellationRegistry()

    raised = False
    try:
        run_supervised_process(
            command=_sleep_command(60), run_id=RUN_ID,
            cancellation=registry, timeout_seconds=0.4,
            aspects=FAST_SUPERVISION)
    except subprocess.TimeoutExpired:
        raised = True

    testbase.assert_true(raised)
    return


def test_supervised_process_raises_file_not_found_for_a_missing_binary():
    """
        **Feature: terminal outcomes**
        **Property: a binary that is not installed surfaces as
        FileNotFoundError, so the executor can classify it as a pod
        provisioning problem.**
    """
    registry = CancellationRegistry()

    raised = False
    try:
        run_supervised_process(
            command=["pychestrate-no-such-binary-anywhere"], run_id=RUN_ID,
            cancellation=registry, aspects=FAST_SUPERVISION)
    except FileNotFoundError:
        raised = True

    testbase.assert_true(raised)
    return


# ---------------------------------------------------------------------
# Heartbeat delivery
# ---------------------------------------------------------------------


def test_heartbeat_records_a_delivered_cancellation():
    """
        **Feature: cancel delivery**
        **Property: a cancellation on the heartbeat response lands in
        the registry the dispatch loop reads.**
    """
    registry = CancellationRegistry()
    entries = [{
        "request_id": "req-1",
        "run_id": RUN_ID,
        "reason": "operator cancelled",
    }]
    recorded = heartbeat_mod.record_delivered_cancellations(
        entries=entries, registry=registry)

    testbase.assert_equal(recorded, 1)
    testbase.assert_true(registry.is_cancelled(run_id=RUN_ID))
    return


def test_heartbeat_ignores_a_cancellation_with_no_run_id():
    """
        **Feature: cancel delivery**
        **Property: a malformed entry is skipped rather than recorded
        under a blank key or allowed to raise.**
    """
    registry = CancellationRegistry()
    entries = [{"request_id": "req-1", "reason": "no run here"}]
    recorded = heartbeat_mod.record_delivered_cancellations(
        entries=entries, registry=registry)

    testbase.assert_equal(recorded, 0)
    testbase.assert_equal(len(registry.cancelled_run_ids()), 0)
    return


def test_heartbeat_extracts_cancellations_from_an_ack_body():
    """
        **Feature: cancel delivery**
        **Property: the ClientHeartbeatAck body's cancellations array is
        read off the response.**
    """
    payload = {
        "client_id": "55555555-5555-5555-5555-555555555555",
        "cancellations": [{"run_id": RUN_ID, "reason": "stop"}],
    }
    entries = heartbeat_mod.extract_cancellations(payload=payload)
    testbase.assert_equal(len(entries), 1)
    testbase.assert_equal(entries[0]["run_id"], RUN_ID)
    return


def test_heartbeat_extracts_nothing_from_a_body_that_is_not_a_mapping():
    """
        **Feature: cancel delivery**
        **Property: an unexpected body shape yields no cancellations
        rather than raising -- a heartbeat that cannot be parsed must
        still count as a heartbeat.**
    """
    entries = heartbeat_mod.extract_cancellations(payload="not-a-dict")
    testbase.assert_equal(len(entries), 0)
    return


def test_heartbeat_reports_204_when_the_body_carries_no_cancellations():
    """
        **Feature: cancel delivery**
        **Property: an empty cancellations array is reported the same
        as the no-body 204, so nothing downstream treats it as work.**
    """

    class _Clients:
        def clients_heartbeat(self, client_id, exp_status=None, **kwargs):
            rtnval = {"client_id": client_id, "cancellations": []}
            return rtnval

    class _Client:
        def __init__(self):
            self.clients = _Clients()
            self._session = object()
            return

        def authenticate(self):
            return

    registry = CancellationRegistry()
    status = heartbeat_mod.send_one_heartbeat(
        client=_Client(), client_id="55555555-5555-5555-5555-555555555555",
        registry=registry)

    testbase.assert_equal(status, 204)
    return


def test_heartbeat_reports_200_and_records_when_cancellations_arrive():
    """
        **Feature: cancel delivery**
        **Property: a 200 ClientHeartbeatAck is recognised and its
        cancellations are recorded.**
    """

    class _Clients:
        def clients_heartbeat(self, client_id, exp_status=None, **kwargs):
            rtnval = {
                "client_id": client_id,
                "cancellations": [
                    {"request_id": "req-9", "run_id": RUN_ID,
                     "reason": "operator cancelled"},
                ],
            }
            return rtnval

    class _Client:
        def __init__(self):
            self.clients = _Clients()
            self._session = object()
            return

        def authenticate(self):
            return

    registry = CancellationRegistry()
    status = heartbeat_mod.send_one_heartbeat(
        client=_Client(), client_id="55555555-5555-5555-5555-555555555555",
        registry=registry)

    testbase.assert_equal(status, 200)
    testbase.assert_true(registry.is_cancelled(run_id=RUN_ID))
    return


def test_heartbeat_passes_both_success_statuses_to_the_rest_client():
    """
        **Feature: cancel delivery**
        **Property: the generated clients_heartbeat defaults to
        expecting 204 alone, so the 200 must be declared explicitly or
        perform_call would treat the cancel delivery as an error.**
    """
    captured = {"exp_status": None}

    class _Clients:
        def clients_heartbeat(self, client_id, exp_status=None, **kwargs):
            captured["exp_status"] = exp_status
            raise ValueError("empty body")

    class _Client:
        def __init__(self):
            self.clients = _Clients()
            self._session = object()
            return

        def authenticate(self):
            return

    heartbeat_mod.send_one_heartbeat(
        client=_Client(), client_id="55555555-5555-5555-5555-555555555555",
        registry=CancellationRegistry())

    testbase.assert_true(200 in captured["exp_status"])
    testbase.assert_true(204 in captured["exp_status"])
    return


def test_heartbeat_for_an_unknown_run_records_it_without_raising():
    """
        **Feature: cancel delivery**
        **Property: a cancellation for a run this pod never heard of is
        harmless -- it is recorded and simply never matches a job.**
    """
    registry = CancellationRegistry()
    entries = [{"request_id": "req-1", "run_id": OTHER_RUN_ID,
                "reason": "stale cancel"}]
    heartbeat_mod.record_delivered_cancellations(
        entries=entries, registry=registry)

    testbase.assert_equal(registry.is_cancelled(run_id=RUN_ID), False)
    testbase.assert_true(registry.is_cancelled(run_id=OTHER_RUN_ID))
    return


# ---------------------------------------------------------------------
# Terminal outcomes on TestbaseRunOutcome
# ---------------------------------------------------------------------


def test_outcome_maps_a_succeeded_status_to_the_success_outcome():
    """
        **Feature: terminal outcomes**
        **Property: a successful run reports TerminalOutcome.SUCCESS.**
    """
    outcome = TestbaseRunOutcome(
        status=RUN_STATUS_SUCCEEDED, exit_code=0, output_dir="/tmp/o",
        command=["testbase"], detail="ok")
    testbase.assert_equal(outcome.terminal_outcome, TERMINAL_OUTCOME_SUCCESS)
    return


def test_outcome_maps_a_failed_status_to_the_failure_outcome():
    """
        **Feature: terminal outcomes**
        **Property: a failed run reports TerminalOutcome.FAILURE.**
    """
    outcome = TestbaseRunOutcome(
        status=RUN_STATUS_FAILED, exit_code=1, output_dir="/tmp/o",
        command=["testbase"], detail="nope")
    testbase.assert_equal(outcome.terminal_outcome, TERMINAL_OUTCOME_FAILURE)
    return


def test_outcome_maps_a_cancelled_status_to_the_cancelled_outcome():
    """
        **Feature: terminal outcomes**
        **Property: a cancelled run reports TerminalOutcome.CANCELLED,
        NOT FAILURE. Folding the two together would make an operator's
        deliberate stop indistinguishable from a broken job in every
        downstream metric.**
    """
    outcome = TestbaseRunOutcome(
        status=RUN_STATUS_CANCELLED, exit_code=None, output_dir="/tmp/o",
        command=["testbase"], detail="cancelled")
    testbase.assert_equal(outcome.terminal_outcome, TERMINAL_OUTCOME_CANCELLED)
    testbase.assert_true(outcome.cancelled)
    testbase.assert_equal(outcome.succeeded, False)
    return


def test_outcome_carries_the_failure_classification_in_its_dict():
    """
        **Feature: terminal outcomes**
        **Property: the classification reaches the local run record, so
        it survives for reconciliation and debugging.**
    """
    outcome = TestbaseRunOutcome(
        status=RUN_STATUS_FAILED, exit_code=None, output_dir="/tmp/o",
        command=["testbase"], detail="timed out",
        failure_classification=FAILURE_CLASSIFICATION_TIMEOUT)
    as_dict = outcome.as_dict()
    testbase.assert_equal(
        as_dict["failure_classification"], FAILURE_CLASSIFICATION_TIMEOUT)
    return


# ---------------------------------------------------------------------
# Executor classification
# ---------------------------------------------------------------------


def test_executor_classifies_a_nonzero_exit():
    """
        **Feature: terminal outcomes**
        **Property: a job that ran and exited non-zero is classified as
        a non-zero exit, not as a timeout or a missing binary.**
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))

        def _fake_run(command, **kwargs):
            class _Completed:
                returncode = 3
            rtnval = _Completed()
            return rtnval

        aspects = TestbaseExecutorAspects(run_func=_fake_run)
        outcome = execute_job_envelope(
            state=state, envelope=_envelope(tmpdir), local_run_id="local-1",
            aspects=aspects)

        testbase.assert_equal(
            outcome.failure_classification,
            FAILURE_CLASSIFICATION_NONZERO_EXIT)
    return


def test_executor_classifies_a_missing_binary():
    """
        **Feature: terminal outcomes**
        **Property: a testbase binary that is not installed is
        classified as a pod provisioning problem, distinct from a job
        that failed.**
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))

        def _fake_run(command, **kwargs):
            raise FileNotFoundError("no testbase here")

        aspects = TestbaseExecutorAspects(run_func=_fake_run)
        outcome = execute_job_envelope(
            state=state, envelope=_envelope(tmpdir), local_run_id="local-1",
            aspects=aspects)

        testbase.assert_equal(
            outcome.failure_classification,
            FAILURE_CLASSIFICATION_EXECUTABLE_NOT_FOUND)
    return


def test_executor_classifies_a_timeout():
    """
        **Feature: terminal outcomes**
        **Property: a run that blew its wall-clock cap is classified as
        a timeout, distinct from a job that ran and failed.**
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))

        def _fake_run(command, **kwargs):
            raise subprocess.TimeoutExpired(cmd=command, timeout=1.0)

        aspects = TestbaseExecutorAspects(run_func=_fake_run)
        outcome = execute_job_envelope(
            state=state, envelope=_envelope(tmpdir), local_run_id="local-1",
            aspects=aspects)

        testbase.assert_equal(
            outcome.failure_classification, FAILURE_CLASSIFICATION_TIMEOUT)
        testbase.assert_equal(
            outcome.terminal_outcome, TERMINAL_OUTCOME_FAILURE)
    return


def test_executor_classifies_an_unbuildable_envelope_as_a_setup_error():
    """
        **Feature: terminal outcomes**
        **Property: an envelope the executor cannot turn into a command
        is a setup error -- nothing ever ran, so it is not a test
        failure.**
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))

        envelope = _envelope(tmpdir)
        envelope["params"] = {}

        outcome = execute_job_envelope(
            state=state, envelope=envelope, local_run_id="local-1",
            aspects=TestbaseExecutorAspects())

        testbase.assert_equal(
            outcome.failure_classification, FAILURE_CLASSIFICATION_SETUP_ERROR)
    return


def test_executor_reports_cancelled_rather_than_failed():
    """
        **Feature: run cancellation**
        **Property: a run stopped by an operator cancel is reported
        CANCELLED with a cancelled classification -- the whole point of
        the feature.**
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))
        registry = CancellationRegistry()
        registry.record(run_id=RUN_ID, reason="operator cancelled")

        aspects = TestbaseExecutorAspects(
            testbase_binary=sys.executable,
            supervision_aspects=FAST_SUPERVISION)

        outcome = execute_job_envelope(
            state=state, envelope=_envelope(tmpdir), local_run_id="local-1",
            aspects=aspects, cancellation=registry)

        testbase.assert_equal(outcome.status, RUN_STATUS_CANCELLED)
        testbase.assert_equal(
            outcome.terminal_outcome, TERMINAL_OUTCOME_CANCELLED)
        testbase.assert_equal(
            outcome.failure_classification, FAILURE_CLASSIFICATION_CANCELLED)
    return


def test_executor_stops_the_progress_reporter_on_a_cancelled_run():
    """
        **Feature: run cancellation**
        **Property: a cancelled run stops reporting progress. Leaving
        the reporter running would keep streaming frames from a process
        tree that no longer exists.**
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))
        registry = CancellationRegistry()
        registry.record(run_id=RUN_ID, reason="operator cancelled")

        calls = {"started": 0, "stopped": 0}

        class _Reporter:
            def start(self):
                calls["started"] += 1
                return

            def stop(self):
                calls["stopped"] += 1
                return

        aspects = TestbaseExecutorAspects(
            testbase_binary=sys.executable,
            supervision_aspects=FAST_SUPERVISION)

        execute_job_envelope(
            state=state, envelope=_envelope(tmpdir), local_run_id="local-1",
            aspects=aspects, progress_reporter=_Reporter(),
            cancellation=registry)

        testbase.assert_equal(calls["started"], 1)
        testbase.assert_equal(calls["stopped"], 1)
    return


def test_executor_leaves_an_uncancelled_run_alone():
    """
        **Feature: run cancellation**
        **Property: supplying a registry that holds a DIFFERENT run's
        cancellation does not stop this run.**
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))
        registry = CancellationRegistry()
        registry.record(run_id=OTHER_RUN_ID, reason="a different run")

        def _fake_run(command, **kwargs):
            class _Completed:
                returncode = 0
            rtnval = _Completed()
            return rtnval

        aspects = TestbaseExecutorAspects(run_func=_fake_run)
        outcome = execute_job_envelope(
            state=state, envelope=_envelope(tmpdir), local_run_id="local-1",
            aspects=aspects)

        testbase.assert_equal(outcome.status, RUN_STATUS_SUCCEEDED)
        testbase.assert_equal(
            outcome.terminal_outcome, TERMINAL_OUTCOME_SUCCESS)
        testbase.assert_true(outcome.failure_classification is None)
    return

"""
    TestBase tests for the cancellable subprocess runner the test, build and
    provision executors use so an operator cancel stops the job (E2E-09).
    These run real short-lived processes.
"""

import subprocess
import sys
import time

from ccai import testbase

from ccai.pychestrate.client import dispatch_consumer as dc_mod
from ccai.pychestrate.client.cancellable_run import RunCancelled, make_cancellable_run
from ccai.pychestrate.client.testbase_executor import TestbaseExecutorAspects


def test_runner_returns_the_completed_process():
    """
    **Feature: cancellable runner**
    **Property: a job that is not cancelled runs to completion and its exit
    code and output come back as with subprocess.run.**
    """
    run = make_cancellable_run(cancel_check=lambda: False, poll_seconds=0.1)

    completed = run([sys.executable, "-c", "print('hello'); raise SystemExit(3)"],
                    capture_output=True, timeout=30)

    testbase.assert_equal(completed.returncode, 3)
    testbase.assert_true(b"hello" in completed.stdout)
    return


def test_cancel_stops_the_job_promptly():
    """
    **Feature: cancellable runner (E2E-09)**
    **Property: once the cancel check turns true the job's process group is
    stopped and RunCancelled is raised -- the job does not run on to its
    natural end.**
    """
    cancel_at = time.monotonic() + 0.5
    run = make_cancellable_run(cancel_check=lambda: time.monotonic() >= cancel_at,
                               poll_seconds=0.1, grace_seconds=2.0)

    started = time.monotonic()
    raised = None
    try:
        run([sys.executable, "-c", "import time; time.sleep(60)"], capture_output=True, timeout=120)
    except RunCancelled as cancelled:
        raised = cancelled
    elapsed = time.monotonic() - started

    testbase.assert_true(raised is not None)
    testbase.assert_true(elapsed < 10.0)
    return


def test_timeout_still_raises_timeout_expired():
    """
    **Feature: cancellable runner**
    **Property: a job that outlives its timeout is stopped and
    subprocess.TimeoutExpired is raised, as subprocess.run does.**
    """
    run = make_cancellable_run(cancel_check=lambda: False, poll_seconds=0.1, grace_seconds=2.0)

    raised = None
    try:
        run([sys.executable, "-c", "import time; time.sleep(60)"], capture_output=True, timeout=0.5)
    except subprocess.TimeoutExpired as expired:
        raised = expired

    testbase.assert_true(raised is not None)
    return


def test_bind_cancel_check_replaces_only_the_default_runner():
    """
    **Feature: cancel binding (E2E-09)**
    **Property: executor aspects using subprocess.run get a cancellable
    runner for the run; aspects with an injected runner keep it.**
    """
    default_aspects = TestbaseExecutorAspects()

    def _injected(command, **kwargs):
        return None

    injected_aspects = TestbaseExecutorAspects(run_func=_injected)

    bound_default = dc_mod._bind_cancel_check(executor_aspects=default_aspects, run_id="run-1")
    bound_injected = dc_mod._bind_cancel_check(executor_aspects=injected_aspects, run_id="run-1")

    testbase.assert_true(bound_default.run_func is not subprocess.run)
    testbase.assert_true(default_aspects.run_func is subprocess.run)
    testbase.assert_true(bound_injected.run_func is _injected)
    return

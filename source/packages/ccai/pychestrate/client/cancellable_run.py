"""
    .. module:: cancellable_run
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: A ``subprocess.run`` stand-in that stops the job's process
                   tree when an operator cancel arrives.

                   The test, build and provision executors take their
                   subprocess runner from their aspects (``run_func``,
                   ``subprocess.run`` by default), which blocks until the
                   job ends, so a cancel the heartbeat delivered
                   (:mod:`cancel_registry`) never stopped them: the run
                   finished and reported success after the operator
                   cancelled it (E2E-09). :func:`make_cancellable_run`
                   returns a runner with the same call shape that starts
                   the job in its own process group, checks the cancel
                   predicate while it runs, and on a cancel sends the group
                   SIGTERM, then SIGKILL after a grace period, and raises
                   :class:`RunCancelled`.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2026, Code Craft AI LLC"
__credits__ = []


from typing import Any, Callable, List, Optional, Sequence

import logging
import os
import signal
import subprocess
import time


#: Seconds between cancel checks while a job runs.
DEFAULT_CANCEL_POLL_SECONDS = 1.0
#: Seconds a cancelled job's process group gets after SIGTERM before SIGKILL.
DEFAULT_CANCEL_GRACE_SECONDS = 10.0

logger = logging.getLogger(__name__)


class RunCancelled(Exception):
    """
        Raised by a cancellable runner when the job was stopped because an
        operator cancelled its run.
    """

    def __init__(self, message: str, *, returncode: Optional[int] = None,
                 stdout: Any = None, stderr: Any = None):
        super().__init__(message)
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        return


def _stop_process_group(*, process: Any, grace_seconds: float) -> None:
    """
        Sends the process's group SIGTERM, then SIGKILL if it is still
        running after ``grace_seconds``.

        :param process: The started :class:`subprocess.Popen`.
        :param grace_seconds: Seconds to wait between the two signals.

        :returns: None
    """
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        return
    try:
        process.wait(timeout=grace_seconds)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        process.wait()
    return


def make_cancellable_run(*, cancel_check: Callable[[], bool],
                         poll_seconds: float = DEFAULT_CANCEL_POLL_SECONDS,
                         grace_seconds: float = DEFAULT_CANCEL_GRACE_SECONDS,
                         popen_func: Callable[..., Any] = subprocess.Popen,
                         clock: Callable[[], float] = time.monotonic
                         ) -> Callable[..., subprocess.CompletedProcess]:
    """
        Builds a runner with ``subprocess.run``'s call shape for the
        arguments the executors pass (``args``, ``cwd``, ``env``,
        ``timeout``, ``capture_output``).

        :param cancel_check: Returns True once the run has been cancelled.
        :param poll_seconds: Seconds between cancel checks.
        :param grace_seconds: SIGTERM-to-SIGKILL grace period.
        :param popen_func: :class:`subprocess.Popen` override (tests).
        :param clock: Monotonic clock override (tests).

        :returns: The runner.
    """

    def _run(args: Sequence[str], *, cwd: Optional[str] = None,
             env: Optional[dict] = None, timeout: Optional[float] = None,
             capture_output: bool = False, **unused: Any) -> subprocess.CompletedProcess:
        stream = subprocess.PIPE if capture_output else None
        process = popen_func(list(args), cwd=cwd, env=env, stdout=stream,
                             stderr=stream, start_new_session=True)
        give_up_at: Optional[float] = None
        if timeout is not None:
            give_up_at = clock() + float(timeout)

        while True:
            wait_seconds = poll_seconds
            if give_up_at is not None:
                wait_seconds = max(0.0, min(poll_seconds, give_up_at - clock()))
            try:
                stdout, stderr = process.communicate(timeout=wait_seconds)
                rtnval = subprocess.CompletedProcess(list(args), process.returncode, stdout, stderr)
                return rtnval
            except subprocess.TimeoutExpired:
                pass

            if cancel_check():
                logger.info("pychestrate-client: cancel received; stopping %s (pid=%s).",
                            args[0] if len(args) > 0 else "?", process.pid)
                _stop_process_group(process=process, grace_seconds=grace_seconds)
                stdout, stderr = process.communicate()
                raise RunCancelled(
                    "The run was cancelled by an operator; the job was stopped.",
                    returncode=process.returncode, stdout=stdout, stderr=stderr)

            if give_up_at is not None and clock() >= give_up_at:
                _stop_process_group(process=process, grace_seconds=grace_seconds)
                stdout, stderr = process.communicate()
                raise subprocess.TimeoutExpired(list(args), timeout, output=stdout, stderr=stderr)

    rtnval = _run
    return rtnval


__all__ = [
    "DEFAULT_CANCEL_GRACE_SECONDS",
    "DEFAULT_CANCEL_POLL_SECONDS",
    "RunCancelled",
    "make_cancellable_run",
]

"""
    .. module:: process_supervisor
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Runs a job subprocess under supervision so it can be
                   cancelled mid-flight, and terminates the whole
                   process tree when it is.

                   All three executors shell out with
                   :func:`subprocess.run`, which blocks until the child
                   exits. That is fine when nothing can interrupt a
                   run, but a cancel arriving from the service has no
                   way to reach a blocked ``subprocess.run`` -- so the
                   supervised path here replaces it with a
                   :class:`subprocess.Popen` plus a poll loop that
                   watches for the child exiting, the wall-clock cap
                   elapsing, and the cancellation flag being set.

                   Killing the child alone is not enough. ``testbase``,
                   ``make``, and the infra tools all fork further
                   processes, and signalling only the direct child
                   leaves those orphans running -- still holding the
                   devices, ports, and licences the next job needs. The
                   supervised process is therefore started in its own
                   process group and the whole group is signalled:
                   SIGTERM first so a well-behaved tool can clean up,
                   then SIGKILL once the grace period from
                   :class:`SupervisedProcessAspects` expires.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2025, Automation Mojo LLC"
__credits__ = []


from typing import Any, Callable, Dict, List, Optional

import logging
import os
import signal
import subprocess
import tempfile
import time

from ccai.errors.exceptions import BadParameterError


logger = logging.getLogger(__name__)


#: Seconds a cancelled process tree is given to exit on SIGTERM before
#: SIGKILL is sent. Long enough for a test framework to flush its
#: result tree, short enough that an operator's cancel feels like one.
DEFAULT_CANCEL_GRACE_SECONDS = 15.0

#: Seconds between checks of the child's exit status, the wall-clock
#: cap, and the cancellation flag.
DEFAULT_SUPERVISION_POLL_SECONDS = 0.5

#: The graceful-stop and forced-stop signals, resolved once at import.
#: Windows defines neither ``SIGKILL`` nor process groups, so the
#: constants fall back to ``SIGTERM`` there and the delivery path
#: degrades to :meth:`subprocess.Popen.terminate` /
#: :meth:`subprocess.Popen.kill`.
TERM_SIGNAL = signal.SIGTERM
KILL_SIGNAL = getattr(signal, "SIGKILL", signal.SIGTERM)


class RunCancelledError(Exception):
    """
        Raised by :func:`run_supervised_process` when the run was
        stopped because a cancellation was recorded for it.

        A cancelled run is not a failed run, and the two must not be
        reported the same way. Raising a distinct type is what lets
        the executors record ``CANCELLED`` rather than folding the
        cancel into their generic failure branch.
    """

    def __init__(self, message: str, *, run_id: str, reason: str):
        """
            Initializes a :class:`RunCancelledError`.

            :param message: The multi-line explanation.
            :param run_id: The run UUID that was cancelled.
            :param reason: The operator-supplied cancel reason.
        """
        super().__init__(message)
        self.run_id = run_id
        self.reason = reason
        return


class SupervisedProcessAspects:
    """
        The :class:`SupervisedProcessAspects` packages caller-controlled
        behavior for supervised subprocess execution (the SIGTERM to
        SIGKILL grace period, the supervision poll interval, and the
        process-launcher override used by tests).

        Following the project's Aspects pattern, behavior modification
        is carried as data on a single parameter rather than via
        decorators.
    """

    def __init__(self, *,
                 cancel_grace_seconds: float = DEFAULT_CANCEL_GRACE_SECONDS,
                 poll_interval_seconds: float =
                 DEFAULT_SUPERVISION_POLL_SECONDS,
                 popen_func: Optional[Callable[..., Any]] = None,
                 sleep_func: Optional[Callable[[float], None]] = None):
        """
            Initializes a :class:`SupervisedProcessAspects` instance.

            :param cancel_grace_seconds: Seconds a cancelled process
                                         tree is given to exit on
                                         SIGTERM before SIGKILL.
            :param poll_interval_seconds: Seconds between supervision
                                          checks.
            :param popen_func: Optional process-launcher override with
                               the :class:`subprocess.Popen` signature.
                               Injected for testability; defaults to
                               :class:`subprocess.Popen`.
            :param sleep_func: Optional sleep override; defaults to
                               :func:`time.sleep`.

            :raises: BadParameterError
        """
        if cancel_grace_seconds < 0:
            raise BadParameterError(
                "SupervisedProcessAspects requires 'cancel_grace_seconds' "
                ">= 0.",
                "cancel_grace_seconds", "Pass a non-negative number of "
                                        "seconds.")
        if poll_interval_seconds <= 0:
            raise BadParameterError(
                "SupervisedProcessAspects requires 'poll_interval_seconds' "
                "> 0.",
                "poll_interval_seconds", "Pass a positive number of seconds.")

        self._cancel_grace_seconds = cancel_grace_seconds
        self._poll_interval_seconds = poll_interval_seconds
        self._popen_func = popen_func if popen_func is not None \
            else subprocess.Popen
        self._sleep_func = sleep_func if sleep_func is not None else time.sleep
        return

    @property
    def cancel_grace_seconds(self) -> float:
        return self._cancel_grace_seconds

    @property
    def poll_interval_seconds(self) -> float:
        return self._poll_interval_seconds

    @property
    def popen_func(self) -> Callable[..., Any]:
        return self._popen_func

    @property
    def sleep_func(self) -> Callable[[float], None]:
        return self._sleep_func


DEFAULT_SUPERVISED_PROCESS_ASPECTS = SupervisedProcessAspects()


class SupervisedRunResult:
    """
        The :class:`SupervisedRunResult` mirrors the members of
        :class:`subprocess.CompletedProcess` the executors read, so a
        supervised run and an unsupervised one can be consumed by the
        same code.
    """

    def __init__(self, *, args: List[str], returncode: Optional[int],
                 stdout: bytes, stderr: bytes):
        """
            Initializes a :class:`SupervisedRunResult`.

            :param args: The argv list used for the invocation.
            :param returncode: The process exit code.
            :param stdout: Captured standard output bytes.
            :param stderr: Captured standard error bytes.
        """
        self._args = list(args)
        self._returncode = returncode
        self._stdout = stdout
        self._stderr = stderr
        return

    @property
    def args(self) -> List[str]:
        return self._args

    @property
    def returncode(self) -> Optional[int]:
        return self._returncode

    @property
    def stdout(self) -> bytes:
        return self._stdout

    @property
    def stderr(self) -> bytes:
        return self._stderr


def _supports_process_groups() -> bool:
    """
        Reports whether this platform can signal a whole process
        group, which is how the supervised path reaches a job's
        grandchildren.

        :returns: True on platforms exposing ``os.killpg`` and
                  ``os.getpgid``.
    """
    has_killpg = hasattr(os, "killpg")
    has_getpgid = hasattr(os, "getpgid")

    rtnval = has_killpg and has_getpgid
    return rtnval


def _signal_process_tree(*, process: Any, sig: int) -> bool:
    """
        Sends ``sig`` to the supervised process and everything it
        forked.

        Signalling only the direct child would leave its children
        running -- the exact orphans a cancel is supposed to clear --
        so the process group is the target wherever the platform has
        one. Where it does not, the direct child is signalled and the
        limitation is logged rather than hidden.

        :param process: The :class:`subprocess.Popen` handle.
        :param sig: The signal number to deliver.

        :returns: True when a signal was delivered.
    """
    pid = getattr(process, "pid", None)
    if pid is None:
        rtnval = False
        return rtnval

    if _supports_process_groups() and hasattr(signal, "SIGKILL"):
        try:
            group_id = os.getpgid(pid)
            os.killpg(group_id, sig)
            rtnval = True
            return rtnval
        except (ProcessLookupError, PermissionError, OSError) as grouperr:
            # The group is already gone, or this platform refused the
            # group signal. Fall through to signalling the child
            # directly -- a partial kill beats no kill.
            logger.debug(
                "pychestrate-client: process-group signal %d failed for "
                "pid=%s (%s); falling back to the direct child.",
                sig, pid, grouperr,
            )

    try:
        if sig == KILL_SIGNAL and KILL_SIGNAL != TERM_SIGNAL:
            process.kill()
        else:
            process.terminate()
        rtnval = True
        return rtnval
    except (ProcessLookupError, OSError) as childerr:
        logger.debug(
            "pychestrate-client: direct signal %d failed for pid=%s: %s",
            sig, pid, childerr,
        )

    rtnval = False
    return rtnval


def terminate_process_tree(*, process: Any, grace_seconds: float,
                           aspects: SupervisedProcessAspects =
                           DEFAULT_SUPERVISED_PROCESS_ASPECTS) -> None:
    """
        Stops a supervised process and everything it forked.

        SIGTERM goes out first so a tool that installs a handler can
        flush its output and unwind cleanly. If the tree is still
        alive after ``grace_seconds`` it is SIGKILLed, because an
        operator who asked for a cancel is owed one whether or not the
        job cooperates.

        :param process: The :class:`subprocess.Popen` handle.
        :param grace_seconds: Seconds to wait between SIGTERM and
                              SIGKILL.
        :param aspects: Behavior-shaping aspects (supplies the sleep
                        override and the poll cadence).

        :returns: None
    """
    _signal_process_tree(process=process, sig=TERM_SIGNAL)

    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        exited = process.poll()
        if exited is not None:
            logger.info(
                "pychestrate-client: process tree exited on SIGTERM "
                "(returncode=%s).", exited,
            )
            return
        aspects.sleep_func(aspects.poll_interval_seconds)

    exited = process.poll()
    if exited is not None:
        return

    logger.warning(
        "pychestrate-client: process tree did not exit within the %.1fs "
        "grace period; sending SIGKILL.", grace_seconds,
    )
    _signal_process_tree(process=process, sig=KILL_SIGNAL)

    try:
        process.wait(timeout=grace_seconds)
    except Exception as waiterr:
        logger.warning(
            "pychestrate-client: process tree did not reap cleanly after "
            "SIGKILL: %s", waiterr,
        )
    return


def _close_capture_handles(*, handles) -> None:
    """
        Closes the parent's copies of the capture file descriptors.

        :param handles: Iterable of OS-level file descriptors.

        :returns: None
    """
    for handle in handles:
        try:
            os.close(handle)
        except OSError:
            pass
    return


def _unlink_capture_files(*, paths) -> None:
    """
        Removes the temporary capture files.

        :param paths: Iterable of capture-file paths.

        :returns: None
    """
    for path in paths:
        try:
            os.unlink(path)
        except OSError:
            pass
    return


def _read_capture_file(*, path: str) -> bytes:
    """
        Reads a capture file back, tolerating a file the process never
        wrote to.

        :param path: Absolute path of the capture file.

        :returns: The captured bytes (empty when unreadable).
    """
    rtnval = b""

    try:
        with open(path, "rb") as capf:
            rtnval = capf.read()
    except OSError as readerr:
        logger.debug(
            "pychestrate-client: could not read capture file %s: %s",
            path, readerr,
        )

    return rtnval


def run_supervised_process(*, command: List[str], run_id: str,
                           cancellation: Any,
                           cwd: Optional[str] = None,
                           env: Optional[Dict[str, str]] = None,
                           timeout_seconds: Optional[float] = None,
                           aspects: SupervisedProcessAspects =
                           DEFAULT_SUPERVISED_PROCESS_ASPECTS
                           ) -> SupervisedRunResult:
    """
        Runs ``command`` under supervision so a cancellation recorded
        for ``run_id`` can stop it mid-flight.

        The child is launched in its own process group and polled on
        ``aspects.poll_interval_seconds``. Three things end the wait:
        the child exits, the wall-clock cap elapses, or a cancellation
        appears in the registry. The last two both terminate the whole
        process tree before returning.

        Output is captured to temporary files rather than pipes. A
        job's output is unbounded and a poll loop that is not draining
        a pipe will deadlock the moment the OS buffer fills, which
        would hang the very loop that is supposed to be able to
        interrupt the job.

        The exceptions raised match the unsupervised
        :func:`subprocess.run` path, so an executor's existing
        error branches apply unchanged:

        * :class:`FileNotFoundError` -- the binary is not installed.
        * :class:`subprocess.TimeoutExpired` -- the wall-clock cap was
          exceeded (the tree is terminated first).
        * :class:`RunCancelledError` -- the run was cancelled (the tree
          is terminated first). This one is new, and is what keeps a
          cancel from being reported as a failure.

        :param command: The argv list to execute.
        :param run_id: The server-assigned run UUID, used to look the
                       run up in the cancellation registry.
        :param cancellation: A cancellation registry exposing
                             ``is_cancelled(run_id=...)`` and
                             ``find(run_id=...)``.
        :param cwd: Optional working directory for the child.
        :param env: Optional environment mapping for the child.
        :param timeout_seconds: Optional wall-clock cap.
        :param aspects: Behavior-shaping aspects.

        :returns: A :class:`SupervisedRunResult`.
        :raises: FileNotFoundError, subprocess.TimeoutExpired,
                 RunCancelledError
    """
    if len(command) == 0:
        raise BadParameterError(
            "run_supervised_process requires a non-empty 'command'.",
            "command", "Pass the argv list for the process to run.")

    # A cancel that lands between dispatch and launch must prevent the
    # launch outright. Checking here rather than only inside the poll
    # loop is what closes that window.
    already_cancelled = cancellation.is_cancelled(run_id=run_id)
    if already_cancelled:
        record = cancellation.find(run_id=run_id)
        reason = record.reason if record is not None else "cancelled"
        errmsg_lines = [
            "The run was cancelled before its process was launched.",
            "    run_id: {}".format(run_id),
            "    reason: {}".format(reason),
            "    command: {}".format(" ".join(command)),
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise RunCancelledError(errmsg, run_id=run_id, reason=reason)

    stdout_handle, stdout_path = tempfile.mkstemp(
        prefix="pychestrate-stdout-", suffix=".log")
    stderr_handle, stderr_path = tempfile.mkstemp(
        prefix="pychestrate-stderr-", suffix=".log")

    popen_kwargs: Dict[str, Any] = {
        "cwd": cwd,
        "stdout": stdout_handle,
        "stderr": stderr_handle,
    }
    if env is not None:
        popen_kwargs["env"] = env
    if _supports_process_groups():
        popen_kwargs["start_new_session"] = True

    process = None
    try:
        process = aspects.popen_func(command, **popen_kwargs)
    except BaseException:
        # A launch failure (most often a missing binary) propagates to
        # the executor, which classifies it. Clean up first so a pod
        # that keeps being handed unrunnable jobs does not accumulate
        # capture files forever.
        _close_capture_handles(handles=(stdout_handle, stderr_handle))
        _unlink_capture_files(paths=(stdout_path, stderr_path))
        raise
    else:
        # The child holds its own duplicates of these descriptors, so
        # the parent's copies are dead weight from here on.
        _close_capture_handles(handles=(stdout_handle, stderr_handle))

    started_at = time.monotonic()
    cancel_reason: Optional[str] = None
    timed_out = False

    while True:
        exit_code = process.poll()
        if exit_code is not None:
            break

        if cancellation.is_cancelled(run_id=run_id):
            record = cancellation.find(run_id=run_id)
            cancel_reason = record.reason if record is not None else "cancelled"
            logger.info(
                "pychestrate-client: cancelling run_id=%s; terminating the "
                "process tree (%s).", run_id, cancel_reason,
            )
            terminate_process_tree(
                process=process,
                grace_seconds=aspects.cancel_grace_seconds,
                aspects=aspects,
            )
            break

        if timeout_seconds is not None:
            elapsed = time.monotonic() - started_at
            if elapsed >= timeout_seconds:
                timed_out = True
                logger.error(
                    "pychestrate-client: run_id=%s exceeded its %.1fs cap; "
                    "terminating the process tree.", run_id, timeout_seconds,
                )
                terminate_process_tree(
                    process=process,
                    grace_seconds=aspects.cancel_grace_seconds,
                    aspects=aspects,
                )
                break

        aspects.sleep_func(aspects.poll_interval_seconds)

    captured_stdout = _read_capture_file(path=stdout_path)
    captured_stderr = _read_capture_file(path=stderr_path)
    _unlink_capture_files(paths=(stdout_path, stderr_path))

    if cancel_reason is not None:
        errmsg_lines = [
            "The run was cancelled and its process tree was terminated.",
            "    run_id: {}".format(run_id),
            "    reason: {}".format(cancel_reason),
            "    command: {}".format(" ".join(command)),
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise RunCancelledError(errmsg, run_id=run_id, reason=cancel_reason)

    if timed_out:
        raise subprocess.TimeoutExpired(
            cmd=command, timeout=timeout_seconds,
            output=captured_stdout, stderr=captured_stderr)

    final_code = process.poll()
    rtnval = SupervisedRunResult(
        args=command, returncode=final_code,
        stdout=captured_stdout, stderr=captured_stderr)
    return rtnval

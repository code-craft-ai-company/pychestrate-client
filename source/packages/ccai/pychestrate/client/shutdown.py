"""
    .. module:: shutdown
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: The stop request the pod client's loops share, and the
                   SIGINT / SIGTERM handlers that raise it.

                   Python delivers signals only to the main thread, and
                   ``signal.signal`` refuses to install a handler anywhere
                   else. ``run`` drives the heartbeat and dispatch loops on
                   worker threads, so the handlers are installed once, on
                   the main thread, by :meth:`ShutdownController.
                   install_signal_handlers`; the loops only read the
                   request. Their pauses go through
                   :meth:`ShutdownController.wait`, so a stop request wakes
                   them at once instead of after the rest of an interval or
                   back-off.

                   The first SIGINT / SIGTERM asks for a graceful stop: no
                   new work is taken and the loops exit once the current
                   step is done. A second SIGINT / SIGTERM raises
                   :class:`SystemExit` with :data:`FORCED_EXIT_CODE` on the
                   main thread, which ends the process without waiting.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2026, Code Craft AI LLC"
__credits__ = []


from typing import Any, Callable, Dict, Optional

import logging
import signal
import threading


#: Exit code for a stop forced by a second signal (128 + SIGINT).
FORCED_EXIT_CODE = 130

logger = logging.getLogger(__name__)


class ShutdownController:
    """
        A thread-safe, one-way stop request shared by the client's loops.
    """

    def __init__(self) -> None:
        self._event = threading.Event()
        self._lock = threading.Lock()
        self._signal_count = 0
        self._reason = ""
        return

    @property
    def reason(self) -> str:
        """
            The reason given with the first stop request, or an empty
            string while no stop has been requested.
        """
        with self._lock:
            rtnval = self._reason
        return rtnval

    def request_stop(self, *, reason: str) -> None:
        """
            Asks every loop sharing this controller to stop. Only the first
            request's reason is kept; later requests are harmless.

            :param reason: Why the stop was requested (logged).

            :returns: None
        """
        with self._lock:
            if len(self._reason) == 0:
                self._reason = reason
        self._event.set()
        return

    def is_stop_requested(self) -> bool:
        """
            :returns: True once a stop has been requested.
        """
        rtnval = self._event.is_set()
        return rtnval

    def wait(self, timeout_seconds: float) -> bool:
        """
            Pauses for up to ``timeout_seconds``, returning early when a stop
            is requested.

            :param timeout_seconds: Longest pause, in seconds.

            :returns: True when a stop has been requested.
        """
        wait_seconds = timeout_seconds
        if wait_seconds < 0:
            wait_seconds = 0
        rtnval = self._event.wait(timeout=wait_seconds)
        return rtnval

    def handle_signal(self, signum: int, frame: Any) -> None:
        """
            Signal handler: the first signal requests a graceful stop, the
            second forces the process to exit.

            :param signum: The signal number received.
            :param frame: The interrupted stack frame (unused).

            :returns: None
            :raises: SystemExit on the second signal.
        """
        with self._lock:
            self._signal_count += 1
            signal_count = self._signal_count

        signal_name = _signal_name(signum=signum)
        if signal_count == 1:
            logger.info(
                "pychestrate-client: received %s; stopping after the current "
                "step (send it again to exit immediately).", signal_name)
            self.request_stop(reason="received {}".format(signal_name))
        else:
            logger.warning(
                "pychestrate-client: received %s again; exiting immediately.",
                signal_name)
            raise SystemExit(FORCED_EXIT_CODE)
        return

    def install_signal_handlers(self) -> Callable[[], None]:
        """
            Routes SIGINT and SIGTERM to :meth:`handle_signal`. Must be
            called on the main thread.

            :returns: A callable that restores the previous handlers.
            :raises: RuntimeError when called off the main thread.
        """
        if threading.current_thread() is not threading.main_thread():
            raise RuntimeError(
                "ShutdownController.install_signal_handlers must run on the "
                "main thread; Python delivers signals only there.")

        previous: Dict[int, Any] = {}
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.signal(signum, self.handle_signal)

        def _restore() -> None:
            for restore_signum, handler in previous.items():
                signal.signal(restore_signum, handler)
            return

        rtnval = _restore
        return rtnval


def _signal_name(*, signum: int) -> str:
    """
        :param signum: A signal number.

        :returns: The signal's name (``SIGINT``) or its number as text.
    """
    try:
        rtnval = signal.Signals(signum).name
    except ValueError:
        rtnval = str(signum)
    return rtnval


def install_handlers_if_main_thread(shutdown: ShutdownController) -> Optional[Callable[[], None]]:
    """
        Installs the controller's signal handlers when running on the main
        thread (a loop driven directly, as ``pychestrate-client heartbeat``
        does); does nothing on a worker thread, where ``run`` has already
        installed them on the main thread.

        :param shutdown: The controller the handlers should drive.

        :returns: The restore callable, or None when nothing was installed.
    """
    rtnval = None
    if threading.current_thread() is threading.main_thread():
        rtnval = shutdown.install_signal_handlers()
    return rtnval


__all__ = [
    "FORCED_EXIT_CODE",
    "ShutdownController",
    "install_handlers_if_main_thread",
]

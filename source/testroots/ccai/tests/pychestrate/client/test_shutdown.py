"""
    TestBase tests for the pod client's shared stop request and its
    SIGINT / SIGTERM handling.
"""

import os
import signal
import threading

from ccai import testbase

from ccai.pychestrate.client.shutdown import (
    FORCED_EXIT_CODE,
    ShutdownController,
)


def test_wait_returns_true_once_stop_is_requested():
    """
    **Feature: graceful stop (CLI-07)**
    **Property: wait() returns True at once after a stop is requested.**
    """
    shutdown = ShutdownController()
    shutdown.request_stop(reason="unit test")

    testbase.assert_true(shutdown.wait(timeout_seconds=30.0))
    return


def test_wait_returns_false_when_no_stop_arrives():
    """
    **Feature: graceful stop (CLI-07)**
    **Property: wait() returns False when the timeout passes with no stop
    request.**
    """
    shutdown = ShutdownController()

    testbase.assert_true(shutdown.wait(timeout_seconds=0.05) is False)
    return


def test_first_signal_requests_a_stop():
    """
    **Feature: graceful stop (CLI-07)**
    **Property: The first SIGINT/SIGTERM requests a graceful stop and does
    not end the process.**
    """
    shutdown = ShutdownController()

    shutdown.handle_signal(signal.SIGTERM, None)

    testbase.assert_true(shutdown.is_stop_requested())
    return


def test_second_signal_forces_exit():
    """
    **Feature: graceful stop (CLI-07)**
    **Property: A second SIGINT/SIGTERM raises SystemExit with the forced
    exit code.**
    """
    shutdown = ShutdownController()
    shutdown.handle_signal(signal.SIGINT, None)

    exit_code = None
    try:
        shutdown.handle_signal(signal.SIGINT, None)
    except SystemExit as exit_err:
        exit_code = exit_err.code

    testbase.assert_equal(exit_code, FORCED_EXIT_CODE)
    return


def test_installing_handlers_off_the_main_thread_is_refused():
    """
    **Feature: graceful stop (CLI-07)**
    **Property: install_signal_handlers raises RuntimeError on a worker
    thread, where Python cannot deliver signals, instead of silently
    installing nothing.**
    """
    shutdown = ShutdownController()
    outcome = {}

    def _install_off_main():
        try:
            shutdown.install_signal_handlers()
            outcome["error"] = None
        except RuntimeError as install_err:
            outcome["error"] = install_err
        return

    worker = threading.Thread(target=_install_off_main)
    worker.start()
    worker.join()

    testbase.assert_true(isinstance(outcome["error"], RuntimeError))
    return


def test_installed_handler_turns_sigterm_into_a_stop_request():
    """
    **Feature: graceful stop (CLI-07)**
    **Property: With the handlers installed on the main thread, a real
    SIGTERM delivered to the process becomes a stop request.** testbase
    runs tests on the main thread, where handlers can be installed.
    """
    shutdown = ShutdownController()
    restore = shutdown.install_signal_handlers()
    try:
        os.kill(os.getpid(), signal.SIGTERM)
        stopped = shutdown.wait(timeout_seconds=5.0)
    finally:
        restore()

    testbase.assert_true(stopped)
    return

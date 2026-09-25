"""
    TestBase tests for the pychestrate-client heartbeat loop's interval and
    exponential back-off behavior. These tests mock the REST/HTTP layer; no
    real network traffic is performed.
"""

import os
import tempfile
import threading
import time

from ccai import testbase

from ccai.errors.exceptions import BadParameterError

from ccai.pychestrate.client import heartbeat as heartbeat_mod
from ccai.pychestrate.client.heartbeat import HeartbeatAspects, run_heartbeat_loop
from ccai.pychestrate.client.shutdown import ShutdownController
from ccai.pychestrate.client.state import ClientState


def _make_enrolled_state(state_dir):
    """
        Writes a complete (fake) enrolled state directory and returns the
        :class:`ClientState` bound to it.
    """
    state = ClientState(state_dir=state_dir)
    state.ensure_dir()
    state.write_private_key(key_pem=b"-----BEGIN PRIVATE KEY-----\nXX\n-----END PRIVATE KEY-----\n")
    state.write_client_cert(cert_pem="-----BEGIN CERTIFICATE-----\nCC\n-----END CERTIFICATE-----\n")
    state.write_service_ca(ca_pem="-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----\n")
    state.write_client_id(client_id="abcd-client-id")
    state.write_config(config={
        "service_url": "https://pychestrate.example.com",
        "host": "pychestrate.example.com",
        "scheme": "https",
        "port": None,
        "client_name": "unit-client",
    })
    return state


class _SleepRecorder:
    """
        Records every sleep duration requested by the heartbeat loop.
    """

    def __init__(self):
        self.calls = []
        return

    def __call__(self, seconds):
        self.calls.append(seconds)
        return


def test_heartbeat_aspects_rejects_bad_values():
    raised = False
    try:
        HeartbeatAspects(interval_seconds=0.0)
    except BadParameterError:
        raised = True
    testbase.assert_true(raised)

    raised = False
    try:
        HeartbeatAspects(base_backoff_seconds=10.0, max_backoff_seconds=5.0)
    except BadParameterError:
        raised = True
    testbase.assert_true(raised)
    return


def test_heartbeat_loop_uses_interval_on_success():
    sleeper = _SleepRecorder()
    original_build = heartbeat_mod.build_client_from_state
    original_send = heartbeat_mod.send_one_heartbeat

    heartbeat_mod.build_client_from_state = lambda state: object()
    heartbeat_mod.send_one_heartbeat = lambda client, client_id: 204

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = _make_enrolled_state(os.path.join(tmpdir, "state"))

            aspects = HeartbeatAspects(
                interval_seconds=30.0,
                base_backoff_seconds=5.0,
                max_backoff_seconds=300.0,
                max_iterations=3,
                sleep_func=sleeper,
            )

            iterations = run_heartbeat_loop(state=state, aspects=aspects)

            testbase.assert_equal(iterations, 3)
            testbase.assert_equal(sleeper.calls, [30.0, 30.0, 30.0])
    finally:
        heartbeat_mod.build_client_from_state = original_build
        heartbeat_mod.send_one_heartbeat = original_send
    return


def test_heartbeat_loop_backs_off_exponentially_on_failure():
    sleeper = _SleepRecorder()
    original_build = heartbeat_mod.build_client_from_state
    original_send = heartbeat_mod.send_one_heartbeat

    def _always_raise(client, client_id):
        raise RuntimeError("simulated transient failure")

    heartbeat_mod.build_client_from_state = lambda state: object()
    heartbeat_mod.send_one_heartbeat = _always_raise

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = _make_enrolled_state(os.path.join(tmpdir, "state"))

            aspects = HeartbeatAspects(
                interval_seconds=30.0,
                base_backoff_seconds=5.0,
                max_backoff_seconds=20.0,
                max_iterations=5,
                sleep_func=sleeper,
            )

            iterations = run_heartbeat_loop(state=state, aspects=aspects)

            testbase.assert_equal(iterations, 5)
            # base 5 -> 10 -> 20 -> clamped at 20 -> 20
            testbase.assert_equal(sleeper.calls, [5.0, 10.0, 20.0, 20.0, 20.0])
    finally:
        heartbeat_mod.build_client_from_state = original_build
        heartbeat_mod.send_one_heartbeat = original_send
    return


def test_heartbeat_loop_resets_backoff_after_recovery():
    sleeper = _SleepRecorder()
    original_build = heartbeat_mod.build_client_from_state
    original_send = heartbeat_mod.send_one_heartbeat

    outcomes = {"sequence": [500, 500, 204, 500]}
    cursor = {"index": 0}

    def _scripted_send(client, client_id):
        idx = cursor["index"]
        cursor["index"] = idx + 1
        return outcomes["sequence"][idx]

    heartbeat_mod.build_client_from_state = lambda state: object()
    heartbeat_mod.send_one_heartbeat = _scripted_send

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = _make_enrolled_state(os.path.join(tmpdir, "state"))

            aspects = HeartbeatAspects(
                interval_seconds=30.0,
                base_backoff_seconds=5.0,
                max_backoff_seconds=300.0,
                max_iterations=4,
                sleep_func=sleeper,
            )

            iterations = run_heartbeat_loop(state=state, aspects=aspects)

            testbase.assert_equal(iterations, 4)
            # fail 5 -> fail 10 -> success interval 30 -> fail back to base 5
            testbase.assert_equal(sleeper.calls, [5.0, 10.0, 30.0, 5.0])
    finally:
        heartbeat_mod.build_client_from_state = original_build
        heartbeat_mod.send_one_heartbeat = original_send
    return


def test_heartbeat_loop_stops_on_predicate():
    sleeper = _SleepRecorder()
    original_build = heartbeat_mod.build_client_from_state
    original_send = heartbeat_mod.send_one_heartbeat

    heartbeat_mod.build_client_from_state = lambda state: object()
    heartbeat_mod.send_one_heartbeat = lambda client, client_id: 204

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = _make_enrolled_state(os.path.join(tmpdir, "state"))

            aspects = HeartbeatAspects(max_iterations=100, sleep_func=sleeper)

            counter = {"value": 0}

            def _stop_after_two():
                counter["value"] += 1
                return counter["value"] > 2

            iterations = run_heartbeat_loop(
                state=state, aspects=aspects, stop_predicate=_stop_after_two)

            testbase.assert_equal(iterations, 2)
    finally:
        heartbeat_mod.build_client_from_state = original_build
        heartbeat_mod.send_one_heartbeat = original_send
    return


def test_heartbeat_loop_stop_ends_the_interval_pause_promptly():
    """
    **Feature: graceful stop (CLI-07)**
    **Property: A stop requested while the heartbeat loop waits out its
    interval ends the wait at once rather than after the rest of the
    interval.**
    """
    sent = {"count": 0}

    def _fake_send(client, client_id):
        sent["count"] += 1
        return 204

    original_build = heartbeat_mod.build_client_from_state
    original_send = heartbeat_mod.send_one_heartbeat
    heartbeat_mod.build_client_from_state = lambda state: object()
    heartbeat_mod.send_one_heartbeat = _fake_send

    shutdown = ShutdownController()
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            state = _make_enrolled_state(os.path.join(tmpdir, "state"))
            aspects = HeartbeatAspects(interval_seconds=30.0)

            loop_thread = threading.Thread(
                target=run_heartbeat_loop,
                kwargs={"state": state, "aspects": aspects, "shutdown": shutdown},
                daemon=True)
            loop_thread.start()

            deadline = time.monotonic() + 5.0
            while sent["count"] == 0 and time.monotonic() < deadline:
                time.sleep(0.01)

            stop_requested_at = time.monotonic()
            shutdown.request_stop(reason="test stop during interval")
            loop_thread.join(timeout=5.0)
            stop_seconds = time.monotonic() - stop_requested_at

            testbase.assert_true(loop_thread.is_alive() is False)
            testbase.assert_true(stop_seconds < 2.0)
    finally:
        heartbeat_mod.build_client_from_state = original_build
        heartbeat_mod.send_one_heartbeat = original_send
    return

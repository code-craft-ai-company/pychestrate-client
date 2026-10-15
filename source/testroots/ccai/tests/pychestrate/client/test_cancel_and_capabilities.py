"""
    TestBase tests for the pod-side cancel channel and capability
    advertisement (6-rpa-orchestration): the heartbeat collects
    ``ClientHeartbeatAck.cancellations`` into the shared registry, the RPA
    executor polls that registry per run, and the dispatch loop advertises
    the ``rpa`` capability through the generated client.
"""

import json
import os
import tempfile

from ccai import testbase

from ccai.pychestrate.client import capabilities as cap_mod
from ccai.pychestrate.client import heartbeat as heartbeat_mod
from ccai.pychestrate.client.cancel_registry import (
    CANCEL_REGISTRY,
    CancelRegistry,
    extract_cancellations,
    register_cancellations,
)
from ccai.pychestrate.client.rpa_executor import RpaExecutorAspects
from ccai.pychestrate.client.state import ClientState


class _Model:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
        return


def test_registry_marks_reads_and_clears_thread_safely():
    registry = CancelRegistry()
    testbase.assert_true(not registry.is_cancelled("run-1"))
    testbase.assert_true(registry.mark("run-1", reason="operator", request_id="req-1"))
    testbase.assert_true(not registry.mark("run-1", reason="again"))
    testbase.assert_true(registry.is_cancelled("run-1"))
    testbase.assert_equal(registry.get("run-1")["request_id"], "req-1")
    testbase.assert_equal(registry.pending_run_ids(), ["run-1"])
    registry.clear("run-1")
    testbase.assert_true(not registry.is_cancelled("run-1"))
    testbase.assert_true(not registry.mark(""))
    return


def test_extract_cancellations_handles_dicts_models_and_empty():
    testbase.assert_equal(extract_cancellations(None), [])
    testbase.assert_equal(extract_cancellations({"client_id": "c"}), [])
    as_dict = {"cancellations": [{"run_id": "r1", "request_id": "q1", "reason": "stop"}, {"reason": "no run id"}]}
    testbase.assert_equal(extract_cancellations(as_dict), [{"run_id": "r1", "request_id": "q1", "reason": "stop"}])
    as_model = _Model(cancellations=[_Model(run_id="r2", request_id="q2", reason="halt")])
    testbase.assert_equal(extract_cancellations(as_model)[0]["run_id"], "r2")
    registry = CancelRegistry()
    testbase.assert_equal(register_cancellations(registry, extract_cancellations(as_dict)), 1)
    testbase.assert_equal(register_cancellations(registry, extract_cancellations(as_dict)), 0)
    return


class _FakeClientsGroup:
    def __init__(self, answer):
        self._answer = answer
        self.calls = []
        return

    def clients_heartbeat(self, client_id, **kwargs):
        self.calls.append(client_id)
        if isinstance(self._answer, Exception):
            raise self._answer
        return self._answer


class _FakeClient:
    def __init__(self, answer):
        self.clients = _FakeClientsGroup(answer)
        self._session = object()
        return


def test_heartbeat_records_cancellations_from_the_acknowledgement():
    CANCEL_REGISTRY.clear("44444444-4444-4444-4444-444444444444")
    client = _FakeClient({"client_id": "c-1", "cancellations": [
        {"run_id": "44444444-4444-4444-4444-444444444444", "request_id": "q", "reason": "operator"}]})
    status = heartbeat_mod.send_one_heartbeat(client=client, client_id="c-1")
    testbase.assert_equal(status, 200)
    testbase.assert_true(CANCEL_REGISTRY.is_cancelled("44444444-4444-4444-4444-444444444444"))
    CANCEL_REGISTRY.clear("44444444-4444-4444-4444-444444444444")

    # The generated client returns None for the empty 204 answer.
    client = _FakeClient(None)
    status = heartbeat_mod.send_one_heartbeat(client=client, client_id="c-1")
    testbase.assert_equal(status, 204)
    return


def test_executor_aspects_bind_a_per_run_cancel_check():
    base = RpaExecutorAspects(taskplus_binary="tp", cancel_grace_seconds=1.5, stream_output=False)
    bound = base.with_cancel_check(lambda: True)
    testbase.assert_equal(bound.taskplus_binary, "tp")
    testbase.assert_equal(bound.cancel_grace_seconds, 1.5)
    testbase.assert_true(not bound.stream_output)
    testbase.assert_true(bound.cancel_check_func())
    testbase.assert_true(base.cancel_check_func is None)
    return


def test_detect_taskplus_version_reports_absent_present_and_version():
    testbase.assert_true(cap_mod.detect_taskplus_version(which_func=lambda name: None) is None)

    class _Completed:
        stdout = b"taskplus 1.2.3\n"

    version = cap_mod.detect_taskplus_version(
        which_func=lambda name: "/usr/bin/taskplus", run_func=lambda cmd, **kw: _Completed())
    testbase.assert_equal(version, "taskplus 1.2.3")

    def _boom(cmd, **kw):
        raise OSError("cannot run")

    version = cap_mod.detect_taskplus_version(which_func=lambda name: "/usr/bin/taskplus", run_func=_boom)
    testbase.assert_equal(version, "installed")
    return


def test_build_rpa_capability_reads_config_and_advertises():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()
        with open(os.path.join(state.state_dir, "config.json"), "w", encoding="utf-8") as fh:
            json.dump({"host": "svc", "rpa_job_types": ["invoice-import@1.0.0"], "capability_tags": ["browser"]}, fh)
        advertisement = cap_mod.build_rpa_capability(state=state, taskplus_version="1.0.2")
        testbase.assert_equal(advertisement["kind"], "rpa")
        testbase.assert_equal(advertisement["value"]["job_types"], ["invoice-import@1.0.0"])
        testbase.assert_equal(advertisement["value"]["tags"], ["browser"])
        testbase.assert_equal(advertisement["value"]["taskplus"], "1.0.2")

    class _Agents:
        def __init__(self):
            self.calls = []

        def agents_put_client_capabilities(self, client_id, body):
            self.calls.append((client_id, body))
            return []

    class _Client:
        def __init__(self):
            self.agents = _Agents()

    client = _Client()
    ok = cap_mod.advertise_capabilities(client=client, client_id="c-1", advertisements=[advertisement])
    testbase.assert_true(ok)
    testbase.assert_equal(client.agents.calls[0][0], "c-1")
    testbase.assert_equal(client.agents.calls[0][1]["advertisements"][0]["kind"], "rpa")

    class _OldClient:
        pass

    testbase.assert_true(not cap_mod.advertise_capabilities(client=_OldClient(), client_id="c-1", advertisements=[advertisement]))
    return


def test_a_rest_client_without_the_operation_advertises_through_a_direct_call():
    """
        The published REST client package has no ``agents_put_client_capabilities``, so the
        pod advertised nothing (its capabilities and runtime version showed empty on the Clients
        page). It now makes the same ``PUT /clients/{id}/capabilities`` through ``perform_call``.
    """
    advertisement = {"kind": "rpa", "value": {"job_types": ["invoice-import@1.0.0"], "taskplus": None, "tags": []}}

    class _Calls:
        def __init__(self):
            self.calls = []

        def perform_call(self, **kwargs):
            self.calls.append(kwargs)
            return None

    class _Refused(_Calls):
        def perform_call(self, **kwargs):
            raise RuntimeError("403 forbidden")

    class _NoCalls:
        pass

    client = _Calls()
    ok = cap_mod.advertise_capabilities(client=client, client_id="c-9", advertisements=[advertisement])
    refused = cap_mod.advertise_capabilities(client=_Refused(), client_id="c-9", advertisements=[advertisement])
    cannot = cap_mod.advertise_capabilities(client=_NoCalls(), client_id="c-9", advertisements=[advertisement])

    call = client.calls[0] if len(client.calls) == 1 else {}
    testbase.assert_equal(
        (ok, refused, cannot, call.get("leaf"), call.get("method"), call.get("body"), call.get("exp_status")),
        (True, False, False, "/clients/c-9/capabilities", "PUT", {"advertisements": [advertisement]}, 200),
    )
    return

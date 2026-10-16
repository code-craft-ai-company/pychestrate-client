"""
.. module:: test_service_contract
    :platform: Darwin, Linux, Unix, Windows
    :synopsis: The pod accepts what the service sends (E2E-14).

.. moduleauthor:: Code Craft AI LLC

A valid REST test run was refused by the pod ("needs params.test_root") because
the service sent ``root``, and the pod's progress tag differed from the
service's. Both were fixed on the client side; nothing kept them fixed. The
contract file ``contracts/pod-job-contract.json`` (the original lives in the
``pychestrate-service`` repository, ``specs/contracts/pod-job-contract.json``)
records, for each case, the ``params`` the service's test runner produces and
the ``testbase`` command a pod must build from them, and the progress format tag
the two sides share. The service's tests check the service produces exactly
those params; these tests check the pod turns them into that command.
"""

__author__ = "Code Craft AI LLC"
__copyright__ = "Copyright 2026, Code Craft AI LLC"
__credits__ = []
__version__ = "1.0.0"


import json
import os

from typing import Any, Dict

from ccai import testbase

from ccai.pychestrate.client.build_executor import (
    _extract_build_env,
    _extract_build_spec,
    _extract_make_targets,
    _extract_runner_kind,
    plan_source_acquisition,
)
from ccai.pychestrate.client.provision_executor import (
    _extract_action,
    _extract_driver_inputs,
    _extract_driver_name,
    _extract_provision_env,
    _extract_provision_spec,
    _resolve_work_dir,
    build_provision_command,
)
from ccai.pychestrate.client.progress_reporter import PROGRESS_FORMAT_TAG
from ccai.pychestrate.client.rpa_executor import (
    RpaExecutorAspects,
    _payload_dict,
    _payload_env,
    extract_rpa_payload,
    plan_process_entrypoint,
    plan_rpa_source,
    read_rpa_job_identity,
    resolve_timeout_seconds,
    tasker_entrypoint,
)
from ccai.pychestrate.client.testbase_executor import build_testbase_command


CONTRACT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "contracts", "pod-job-contract.json")


def _contract() -> Dict[str, Any]:
    with open(CONTRACT_PATH, encoding="utf-8") as handle:
        return json.load(handle)


def test_the_pod_builds_the_command_the_contract_says_for_every_test_run_the_service_sends():
    """
    **Feature: one contract for both sides (E2E-14)**
    **Property: for each case in the contract, the pod builds a testbase
    command that holds the contract's ``--root`` and ``--includes`` arguments,
    from the params exactly as the service sends them (``root``, not
    ``test_root``).**
    """
    contract = _contract()
    missing: Dict[str, Any] = {}
    for case in contract["test_run_envelope_params"]:
        envelope = {"envelope_id": "env-1", "job_type": "test", "params": case["params"], "refs": {}}
        command = build_testbase_command(envelope=envelope, output_dir="/tmp/out")
        absent = [part for part in case["pod_command_contains"] if part not in command]
        if len(absent) > 0:
            missing[case["name"]] = absent

    testbase.assert_equal((len(contract["test_run_envelope_params"]) >= 2, missing), (True, {}))
    return


def test_the_pod_reports_progress_under_the_tag_the_service_registers():
    """
    **Feature: one contract for both sides (E2E-14)**
    **Property: the progress format tag the pod sends is the one in the contract
    (the service refuses any other with "No progress_parser registered").**
    """
    testbase.assert_equal(PROGRESS_FORMAT_TAG, _contract()["progress_format_tag"])
    return


def test_the_pod_reads_what_the_contract_says_for_every_build_the_service_sends():
    """
    **Feature: one contract for both sides (E2E-14)**
    **Property: for each build case in the contract, from the envelope's params and source block exactly as the service sends them the pod derives the contract's runner kind, make targets
    and environment, and plans the contract's source acquisition: the structured ``source`` block first (with its credentials reference), else ``git_url`` with its ``git_ref``, else
    ``source_dir``, else nothing.**
    """
    derived: Dict[str, Any] = {}
    expected: Dict[str, Any] = {}
    for case in _contract()["build_envelope_params"]:
        envelope = {"envelope_id": "env-1", "job_type": "build-makefile", "job_kind": "build", "params": case["params"], "source": case["envelope_source"], "refs": {}}
        build_spec = _extract_build_spec(envelope)
        derived[case["name"]] = {
            "runner_kind": _extract_runner_kind(build_spec),
            "targets": _extract_make_targets(build_spec.get("runner_inputs", {})),
            "env": _extract_build_env(build_spec),
            "source": plan_source_acquisition(envelope=envelope, build_spec=build_spec),
        }
        expected[case["name"]] = case["pod"]

    testbase.assert_equal((len(derived) >= 5, derived), (True, expected))
    return


def test_the_pod_reads_what_the_contract_says_for_every_rpa_job_the_service_sends():
    """
    **Feature: one contract for both sides (E2E-14)**
    **Property: for each RPA case in the contract, from the envelope's params and source block exactly as the service sends them the pod derives the contract's job identity and execution
    mode, environment, timeout, work packet, the process command (with its working directory and how the packet is delivered) or the tasker workflow or tasking, and plans the contract's
    source acquisition (a git source block, a directory on the pod, or an installed package).**
    """
    derived: Dict[str, Any] = {}
    expected: Dict[str, Any] = {}
    for case in _contract()["rpa_envelope_params"]:
        envelope = {"envelope_id": "env-1", "run_id": "run-1", "job_kind": "rpa", "params": case["params"], "source": case["envelope_source"], "refs": {}}
        payload = extract_rpa_payload(envelope)
        identity = read_rpa_job_identity(payload)
        entrypoint = _payload_dict(payload, "entrypoint")
        derived[case["name"]] = {
            "execution_mode": identity["execution_mode"], "type_id": identity["type_id"], "version": identity["version"],
            "environment": _payload_env(payload), "timeout_seconds": resolve_timeout_seconds(payload, RpaExecutorAspects()),
            "work_packet": _payload_dict(payload, "work_packet"),
            "process": plan_process_entrypoint(entrypoint) if identity["execution_mode"] == "process" else None,
            "tasker": tasker_entrypoint(entrypoint) if identity["execution_mode"] == "tasker" else None,
            "source": plan_rpa_source(envelope=envelope, payload=payload),
        }
        expected[case["name"]] = case["pod"]

    testbase.assert_equal((len(derived) >= 6, derived), (True, expected))
    return


def test_the_pod_builds_what_the_contract_says_for_every_provisioning_run_the_service_sends():
    """
    **Feature: one contract for both sides (E2E-14)**
    **Property: for each provisioning case in the contract, from the envelope's params exactly as the service sends them the pod derives the contract's driver and action and builds the
    recorded ``terraform`` command (``-auto-approve`` only for apply and destroy, the var file, the parallelism and the sorted backend config), runs it in the recorded working directory
    and passes the recorded environment overlay.**
    """
    derived: Dict[str, Any] = {}
    expected: Dict[str, Any] = {}
    for case in _contract()["provision_envelope_params"]:
        envelope = {"envelope_id": "env-1", "run_id": "run-1", "job_kind": "provision", "params": case["params"], "refs": {}}
        spec = _extract_provision_spec(envelope)
        driver_name = _extract_driver_name(spec)
        driver_inputs = _extract_driver_inputs(spec)
        derived[case["name"]] = {
            "driver_name": driver_name,
            "action": _extract_action(spec),
            "command": build_provision_command(envelope=envelope),
            "working_dir": _resolve_work_dir(driver_name=driver_name, driver_inputs=driver_inputs, fallback_dir="/not/used"),
            "env": _extract_provision_env(driver_inputs),
        }
        expected[case["name"]] = case["pod"]

    testbase.assert_equal((len(derived) >= 3, derived), (True, expected))
    return

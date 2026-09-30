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

from ccai.pychestrate.client.progress_reporter import PROGRESS_FORMAT_TAG
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

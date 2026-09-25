"""
    .. module:: provision_progress_reporter
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Records provision-run state transitions for a dispatched
                   ``job_kind=provision`` job.

                   M6 scope -- the M3 :mod:`progress_reporter` tails the
                   testbase ``testrun_results_stream.jsos`` file and forwards
                   per-test JSOS records inside a ``ProgressFrame``. That
                   schema is test-shaped: ``ProgressSummary`` carries
                   ``tests_passed`` / ``tests_failed`` / ``tests_skipped`` /
                   ``tests_errored`` counters and a ``format_tag`` parser
                   tag, and each ``ProgressRecord`` carries a ``test_name``
                   and a per-test ``result`` code. A provision run has no
                   per-test records and no pass/fail tally -- forcing a
                   provision run's coarse ``started`` / ``running`` /
                   ``succeeded`` | ``failed`` lifecycle into the test-shaped
                   frame would misrepresent it.

                   Rather than force a bad fit, M6 scopes provision progress
                   to a minimal local start/end report -- exactly as M5 did
                   for build progress. The
                   :class:`ProvisionProgressReporter` records each
                   provision-run state transition into the provision
                   record's ``progress`` section and logs it. Live
                   over-the-wire provision progress is a documented
                   follow-up -- it waits on the service exposing a
                   provision-shaped progress frame; the authoritative
                   provision-completion signal remains the output upload +
                   ``results/complete`` POST.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2026, Automation Mojo LLC"
__credits__ = []


from typing import Any, Dict, List

import json
import logging
import os

from datetime import datetime, timezone

from ccai.pychestrate.client.state import ClientState


logger = logging.getLogger(__name__)

#: Provision-run lifecycle states the reporter records.
PROVISION_PROGRESS_STARTED = "started"
PROVISION_PROGRESS_RUNNING = "running"
PROVISION_PROGRESS_SUCCEEDED = "succeeded"
PROVISION_PROGRESS_FAILED = "failed"

#: Sub-directory (under the client state directory) holding per-provision
#: local execution records.
DIRNAME_PROVISIONING = "provisioning"


def _now_rfc3339() -> str:
    """
        Produces the current UTC time as an RFC 3339 string.

        :returns: An RFC 3339 timestamp string.
    """
    rtnval = datetime.now(timezone.utc).isoformat()
    return rtnval


class ProvisionProgressReporter:
    """
        The :class:`ProvisionProgressReporter` records the coarse state
        transitions of a dispatched provision run.

        The owning caller (the provision executor) constructs the reporter,
        calls :meth:`report_running` immediately before launching the
        infra-tool subprocess, and calls :meth:`report_finished` once the
        subprocess exits. Each transition is appended to the provision
        record's ``progress`` section and logged.

        Progress recording is strictly best-effort: a failure to persist a
        transition is logged and swallowed -- it must never affect the
        provision outcome.
    """

    def __init__(self, *, state: ClientState, run_id: str, client_id: str):
        """
            Initializes a :class:`ProvisionProgressReporter`.

            :param state: The :class:`ClientState` used to place the
                          provision record.
            :param run_id: The server-assigned run UUID.
            :param client_id: The assigned client UUID.
        """
        self._state = state
        self._run_id = run_id
        self._client_id = client_id
        self._transitions: List[Dict[str, Any]] = []
        self._record_started()
        return

    @property
    def transitions(self) -> List[Dict[str, Any]]:
        """
            Gets the list of state transitions recorded so far.

            :returns: A list of transition dicts.
        """
        return list(self._transitions)

    def _record_started(self) -> None:
        """
            Records the initial ``started`` transition.

            :returns: None
        """
        self._append_transition(state=PROVISION_PROGRESS_STARTED)
        return

    def report_running(self) -> None:
        """
            Records the ``running`` transition -- the infra-tool subprocess
            is about to launch.

            :returns: None
        """
        self._append_transition(state=PROVISION_PROGRESS_RUNNING)
        return

    def report_finished(self, *, succeeded: bool) -> None:
        """
            Records the terminal transition -- the infra-tool subprocess
            exited.

            :param succeeded: True when the provision run completed
                              successfully.

            :returns: None
        """
        if succeeded:
            terminal_state = PROVISION_PROGRESS_SUCCEEDED
        else:
            terminal_state = PROVISION_PROGRESS_FAILED
        self._append_transition(state=terminal_state)
        return

    def _append_transition(self, *, state: str) -> None:
        """
            Appends a state transition and persists the progress section
            onto the local provision record.

            :param state: The provision-run state being entered.

            :returns: None
        """
        transition = {
            "state": state,
            "recorded_at": _now_rfc3339(),
        }
        self._transitions.append(transition)

        logger.info(
            "pychestrate-client: provision run_id=%s entered state '%s'.",
            self._run_id, state,
        )

        try:
            self._persist()
        except OSError as persisterr:
            logger.warning(
                "pychestrate-client: failed to persist provision progress "
                "for run_id=%s (best-effort, provision continues): %s",
                self._run_id, persisterr,
            )

        return

    def _persist(self) -> None:
        """
            Merges the recorded transitions into the provision record's
            ``progress`` section.

            :returns: None
            :raises: OSError
        """
        safe_run_id = self._run_id if len(self._run_id) > 0 \
            else "unknown-provision"
        record_dir = os.path.join(
            self._state.state_dir, DIRNAME_PROVISIONING, safe_run_id)
        os.makedirs(record_dir, mode=0o755, exist_ok=True)

        record_path = os.path.join(record_dir, "provision-record.json")

        record: Dict[str, Any] = {}
        if os.path.isfile(record_path):
            try:
                with open(record_path, "r", encoding="utf-8") as recf:
                    loaded = json.load(recf)
                if isinstance(loaded, dict):
                    record = loaded
            except (ValueError, OSError) as readerr:
                logger.warning(
                    "pychestrate-client: could not read provision record "
                    "%s, rewriting: %s", record_path, readerr,
                )
                record = {}

        record["run_id"] = self._run_id
        record["client_id"] = self._client_id
        record["progress"] = {
            "transitions": list(self._transitions),
        }

        with open(record_path, "w", encoding="utf-8") as recf:
            json.dump(record, recf, indent=4, sort_keys=True)
            recf.write(os.linesep)

        return

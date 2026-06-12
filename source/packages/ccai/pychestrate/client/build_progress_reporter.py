"""
    .. module:: build_progress_reporter
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Records build-run state transitions for a dispatched
                   ``job_kind=build`` job.

                   M5 scope -- the M3 :mod:`progress_reporter` tails the
                   testbase ``testrun_results_stream.jsos`` file and forwards
                   per-test JSOS records inside a ``ProgressFrame``. That
                   schema is test-shaped: ``ProgressSummary`` carries
                   ``tests_passed`` / ``tests_failed`` / ``tests_skipped`` /
                   ``tests_errored`` counters and a ``format_tag`` parser
                   tag, and each ``ProgressRecord`` carries a ``test_name``
                   and a per-test ``result`` code. A build has no per-test
                   records and no pass/fail tally -- forcing a build's
                   coarse ``started`` / ``running`` / ``succeeded`` |
                   ``failed`` lifecycle into the test-shaped frame would
                   misrepresent it.

                   Rather than force a bad fit, M5 scopes build progress to
                   a minimal local start/end report: the
                   :class:`BuildProgressReporter` records each build-run
                   state transition into the build record's ``progress``
                   section and logs it. Live over-the-wire build progress is
                   a documented follow-up -- it waits on the service
                   exposing a build-shaped progress frame; the authoritative
                   build-completion signal remains the artifact upload +
                   ``results/complete`` POST.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2025, Code Craft AI LLC"
__credits__ = []


from typing import Any, Dict, List, Optional

import json
import logging
import os

from datetime import datetime, timezone

from ccai.pychestrate.client.state import ClientState


logger = logging.getLogger(__name__)

#: Build-run lifecycle states the reporter records (mirrors the server's
#: ``BuildRunState`` enum values the client can observe locally).
BUILD_PROGRESS_STARTED = "started"
BUILD_PROGRESS_RUNNING = "running"
BUILD_PROGRESS_SUCCEEDED = "succeeded"
BUILD_PROGRESS_FAILED = "failed"

#: Sub-directory (under the client state directory) holding per-build
#: local execution records.
DIRNAME_BUILDS = "builds"


def _now_rfc3339() -> str:
    """
        Produces the current UTC time as an RFC 3339 string.

        :returns: An RFC 3339 timestamp string.
    """
    rtnval = datetime.now(timezone.utc).isoformat()
    return rtnval


class BuildProgressReporter:
    """
        The :class:`BuildProgressReporter` records the coarse state
        transitions of a dispatched build run.

        The owning caller (the build executor) constructs the reporter,
        calls :meth:`report_running` immediately before launching the
        ``make`` subprocess, and calls :meth:`report_finished` once the
        subprocess exits. Each transition is appended to the build record's
        ``progress`` section and logged.

        Progress recording is strictly best-effort: a failure to persist a
        transition is logged and swallowed -- it must never affect the
        build outcome.
    """

    def __init__(self, *, state: ClientState, run_id: str, client_id: str):
        """
            Initializes a :class:`BuildProgressReporter`.

            :param state: The :class:`ClientState` used to place the build
                          record.
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
        self._append_transition(state=BUILD_PROGRESS_STARTED)
        return

    def report_running(self) -> None:
        """
            Records the ``running`` transition -- the build subprocess is
            about to launch.

            :returns: None
        """
        self._append_transition(state=BUILD_PROGRESS_RUNNING)
        return

    def report_finished(self, *, succeeded: bool) -> None:
        """
            Records the terminal transition -- the build subprocess exited.

            :param succeeded: True when the build completed successfully.

            :returns: None
        """
        if succeeded:
            terminal_state = BUILD_PROGRESS_SUCCEEDED
        else:
            terminal_state = BUILD_PROGRESS_FAILED
        self._append_transition(state=terminal_state)
        return

    def _append_transition(self, *, state: str) -> None:
        """
            Appends a state transition and persists the progress section
            onto the local build record.

            :param state: The build-run state being entered.

            :returns: None
        """
        transition = {
            "state": state,
            "recorded_at": _now_rfc3339(),
        }
        self._transitions.append(transition)

        logger.info(
            "pychestrate-client: build run_id=%s entered state '%s'.",
            self._run_id, state,
        )

        try:
            self._persist()
        except OSError as persisterr:
            logger.warning(
                "pychestrate-client: failed to persist build progress for "
                "run_id=%s (best-effort, build continues): %s",
                self._run_id, persisterr,
            )

        return

    def _persist(self) -> None:
        """
            Merges the recorded transitions into the build record's
            ``progress`` section.

            :returns: None
            :raises: OSError
        """
        safe_run_id = self._run_id if len(self._run_id) > 0 \
            else "unknown-build"
        record_dir = os.path.join(
            self._state.state_dir, DIRNAME_BUILDS, safe_run_id)
        os.makedirs(record_dir, mode=0o755, exist_ok=True)

        record_path = os.path.join(record_dir, "build-record.json")

        record: Dict[str, Any] = {}
        if os.path.isfile(record_path):
            try:
                with open(record_path, "r", encoding="utf-8") as recf:
                    loaded = json.load(recf)
                if isinstance(loaded, dict):
                    record = loaded
            except (ValueError, OSError) as readerr:
                logger.warning(
                    "pychestrate-client: could not read build record %s, "
                    "rewriting: %s", record_path, readerr,
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

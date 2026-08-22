"""
    .. module:: provision_progress_reporter
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Records provision-run state transitions for a dispatched
                   ``job_kind=provision`` job.

                   The M3 :mod:`progress_reporter` tails the testbase
                   ``testrun_results_stream.jsos`` file and forwards per-test
                   JSOS records inside a test-shaped ``ProgressFrame``. A
                   provision run has no per-test records and no pass/fail
                   tally, so it does not use that shape. The service accepts
                   a generalized frame for exactly this case: a
                   ``frame_kind=provision`` frame carrying phase / status /
                   optional percent-complete plus a free-form payload,
                   deduped on ``(run_id, frame_kind, frame_index)``.

                   The :class:`ProvisionProgressReporter` therefore does two
                   things at every provision-run state transition. It appends
                   the transition to the provision record's ``progress``
                   section on the pod's disk -- the local audit trail, which
                   survives a service outage -- and it posts a generalized
                   progress frame so an operator watching a long provision
                   run sees it move instead of seeing nothing until it ends.

                   Posting reuses :mod:`progress_reporter`'s bounded-retry
                   helper, so an undeliverable frame is not dropped: it stays
                   pending, is re-sent unchanged at the next transition, and
                   the service's frame-index dedupe makes the re-send a
                   no-op. A frame still undelivered when the run ends is
                   named in the local record. The reporter never affects the
                   provision outcome, and the authoritative
                   provision-completion signal remains the output upload +
                   ``results/complete`` POST.

                   ``percent_complete`` is only posted when a caller supplies
                   one. The reporter observes coarse lifecycle transitions,
                   not plan/apply steps, so it never invents a percentage.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2026, Automation Mojo LLC"
__credits__ = []


from typing import Any, Dict, List, Optional

import json
import logging
import os

from datetime import datetime, timezone

from ccai.pychestrate.client.progress_reporter import (
    DEFAULT_PROGRESS_REPORTER_ASPECTS,
    FRAME_KIND_PROVISION,
    GeneralizedProgressFrame,
    ProgressReporterAspects,
    build_generalized_frame,
    flush_progress_frames,
)
from ccai.pychestrate.client.pychestratemtlsclient import PychestrateMtlsClient
from ccai.pychestrate.client.state import ClientState


logger = logging.getLogger(__name__)

#: Provision-run lifecycle states the reporter records.
PROVISION_PROGRESS_STARTED = "started"
PROVISION_PROGRESS_RUNNING = "running"
PROVISION_PROGRESS_SUCCEEDED = "succeeded"
PROVISION_PROGRESS_FAILED = "failed"

#: Maps a provision-run lifecycle state onto the ``status`` vocabulary the
#: service's generalized progress frame documents (``running`` /
#: ``succeeded`` / ``failed``). The lifecycle state itself is posted as the
#: frame's ``phase`` -- it is the only phase a lifecycle-level reporter
#: genuinely observes.
PROVISION_STATE_FRAME_STATUS = {
    PROVISION_PROGRESS_STARTED: "running",
    PROVISION_PROGRESS_RUNNING: "running",
    PROVISION_PROGRESS_SUCCEEDED: "succeeded",
    PROVISION_PROGRESS_FAILED: "failed",
}

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
        transitions of a dispatched provision run and streams them to the
        service as generalized ``frame_kind=provision`` progress frames.

        The owning caller (the provision executor) constructs the reporter,
        calls :meth:`report_running` immediately before launching the
        infra-tool subprocess, and calls :meth:`report_finished` once the
        subprocess exits. Each transition is appended to the provision
        record's ``progress`` section, logged, and posted.

        Both halves are best-effort with respect to the provision run: a
        failure to persist a transition, and a frame the service does not
        accept, are logged and never affect the provision outcome. Delivery
        itself is not best-effort in the lossy sense -- an unaccepted frame
        stays pending and is re-sent, unchanged, at the next transition.

        A reporter constructed without a ``client`` keeps only the local
        record. That is the degraded mode for a caller that has no REST
        client wired; live progress is unavailable for that run.
    """

    def __init__(self, *, state: ClientState, run_id: str, client_id: str,
                 client: Optional[PychestrateMtlsClient] = None,
                 aspects: ProgressReporterAspects =
                 DEFAULT_PROGRESS_REPORTER_ASPECTS):
        """
            Initializes a :class:`ProvisionProgressReporter`.

            :param state: The :class:`ClientState` used to place the
                          provision record.
            :param run_id: The server-assigned run UUID.
            :param client_id: The assigned client UUID.
            :param client: Optional configured
                           :class:`PychestrateMtlsClient`. When None the
                           reporter writes the local record only and posts
                           no frames.
            :param aspects: Behavior-shaping aspects supplying the frame
                            post-retry budget.
        """
        self._state = state
        self._run_id = run_id
        self._client_id = client_id
        self._client = client
        self._aspects = aspects
        self._transitions: List[Dict[str, Any]] = []
        self._pending_frames: List[GeneralizedProgressFrame] = []
        self._next_frame_index = 0

        if client is None:
            logger.info(
                "pychestrate-client: provision run_id=%s has no REST client "
                "wired; progress is recorded locally only.", self._run_id,
            )

        self._record_started()
        return

    @property
    def transitions(self) -> List[Dict[str, Any]]:
        """
            Gets the list of state transitions recorded so far.

            :returns: A list of transition dicts.
        """
        return list(self._transitions)

    @property
    def pending_frame_indexes(self) -> List[int]:
        """
            Gets the frame indexes of the progress frames the service has
            not accepted yet.

            :returns: A list of undelivered frame indexes, oldest first.
        """
        rtnval: List[int] = []
        for frame in self._pending_frames:
            rtnval.append(frame.frame["frame_index"])
        return rtnval

    def _record_started(self) -> None:
        """
            Records the initial ``started`` transition.

            :returns: None
        """
        self._append_transition(state=PROVISION_PROGRESS_STARTED)
        return

    def report_running(self, *, percent_complete: Optional[float] = None,
                       notes: Optional[str] = None) -> None:
        """
            Records the ``running`` transition -- the infra-tool subprocess
            is about to launch.

            :param percent_complete: Optional completion percentage. Pass it
                                     only when the caller genuinely knows
                                     it; an omitted percentage reads as
                                     unknown, a fabricated one reads as
                                     fact.
            :param notes: Optional free-form note carried on the frame.

            :returns: None
        """
        self._append_transition(
            state=PROVISION_PROGRESS_RUNNING,
            percent_complete=percent_complete, notes=notes)
        return

    def report_finished(self, *, succeeded: bool,
                        percent_complete: Optional[float] = None,
                        notes: Optional[str] = None) -> None:
        """
            Records the terminal transition -- the infra-tool subprocess
            exited.

            :param succeeded: True when the provision run completed
                              successfully.
            :param percent_complete: Optional completion percentage; see
                                     :meth:`report_running`.
            :param notes: Optional free-form note carried on the frame.

            :returns: None
        """
        if succeeded:
            terminal_state = PROVISION_PROGRESS_SUCCEEDED
        else:
            terminal_state = PROVISION_PROGRESS_FAILED
        self._append_transition(
            state=terminal_state, percent_complete=percent_complete,
            notes=notes)
        return

    def _append_transition(self, *, state: str,
                           percent_complete: Optional[float] = None,
                           notes: Optional[str] = None) -> None:
        """
            Appends a state transition, persists the progress section onto
            the local provision record, and posts the transition as a
            generalized progress frame.

            The local record is written before the post so the on-pod audit
            trail is durable even if the post is slow or the process dies
            during it, and again afterwards so the record names the frames
            that have not been delivered.

            :param state: The provision-run state being entered.
            :param percent_complete: Optional completion percentage.
            :param notes: Optional free-form note carried on the frame.

            :returns: None
        """
        frame_index = self._next_frame_index
        self._next_frame_index += 1

        transition = {
            "state": state,
            "frame_index": frame_index,
            "recorded_at": _now_rfc3339(),
        }
        self._transitions.append(transition)

        logger.info(
            "pychestrate-client: provision run_id=%s entered state '%s'.",
            self._run_id, state,
        )

        self._persist_guarded()

        self._post_transition_frame(
            transition=transition, percent_complete=percent_complete,
            notes=notes)

        self._persist_guarded()

        return

    def _post_transition_frame(self, *, transition: Dict[str, Any],
                               percent_complete: Optional[float] = None,
                               notes: Optional[str] = None) -> None:
        """
            Queues the generalized progress frame for a transition and
            flushes every pending frame to the service.

            :param transition: The transition dict just appended.
            :param percent_complete: Optional completion percentage.
            :param notes: Optional free-form note carried on the frame.

            :returns: None
        """
        if self._client is None:
            return

        state = transition["state"]
        frame_index = transition["frame_index"]
        recorded_at = transition["recorded_at"]
        status = PROVISION_STATE_FRAME_STATUS[state]

        frame = build_generalized_frame(
            run_id=self._run_id, client_id=self._client_id,
            frame_kind=FRAME_KIND_PROVISION, frame_index=frame_index,
            phase=state, status=status, percent_complete=percent_complete,
            notes=notes, payload={"run_kind": FRAME_KIND_PROVISION},
            recorded_at=recorded_at)
        self._pending_frames.append(frame)

        self._pending_frames = flush_progress_frames(
            client=self._client, run_id=self._run_id,
            frames=self._pending_frames, aspects=self._aspects)

        if len(self._pending_frames) > 0:
            logger.warning(
                "pychestrate-client: provision run_id=%s has %d undelivered "
                "progress frame(s) %s; they are re-sent unchanged at the "
                "next transition (the provision run is unaffected).",
                self._run_id, len(self._pending_frames),
                self.pending_frame_indexes,
            )

        return

    def _persist_guarded(self) -> None:
        """
            Persists the local provision record, logging and swallowing an
            I/O failure -- a record that cannot be written must never affect
            the provision outcome.

            :returns: None
        """
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
            Merges the recorded transitions -- and which of their progress
            frames are still undelivered -- into the provision record's
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
            "frame_kind": FRAME_KIND_PROVISION,
            "transitions": list(self._transitions),
            "undelivered_frame_indexes": self.pending_frame_indexes,
        }

        with open(record_path, "w", encoding="utf-8") as recf:
            json.dump(record, recf, indent=4, sort_keys=True)
            recf.write(os.linesep)

        return

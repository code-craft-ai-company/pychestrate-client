"""
    .. module:: rpa_progress_reporter
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Streams RPA job progress for a dispatched ``job_kind=rpa``
                   job: coarse state transitions plus batches of captured
                   output lines, posted as ``frame_kind=rpa`` generalized
                   progress frames (``rpa.process.v1``) to
                   ``POST /api/v1/runs/{run_id}/progress``.

                   Every frame carries ``client_id``, a monotonically
                   increasing ``frame_index``, ``phase`` / ``status`` /
                   ``notes`` and a ``lines`` array of
                   ``{"line": str, "stream": "stdout"|"stderr", "ts": iso}``
                   records. Posting is best-effort -- a failure is logged and
                   the job continues -- and every transition is also mirrored
                   into the local ``rpa-record.json`` so the record survives a
                   service outage.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2026, Automation Mojo LLC"
__credits__ = []


from typing import Any, Callable, Dict, List, Optional

import json
import logging
import os
import threading
import time

from datetime import datetime, timezone

from automojo.pychestrate.client.state import ClientState


logger = logging.getLogger(__name__)

RPA_PROGRESS_STARTED = "started"
RPA_PROGRESS_RUNNING = "running"
RPA_PROGRESS_SUCCEEDED = "succeeded"
RPA_PROGRESS_FAILED = "failed"

#: ``frame_kind`` discriminator the service routes to the generalized
#: frame ingest.
RPA_FRAME_KIND = "rpa"

#: ``format_tag`` stamped on every RPA progress frame.
RPA_PROGRESS_FORMAT_TAG = "rpa.process.v1"

#: Marker a job may print to report structured progress
#: (``PYC-PROGRESS {"percent": 40, "message": "..."}``).
PROGRESS_MARKER = "PYC-PROGRESS "

#: Sub-directory (under the client state directory) holding per-job local
#: execution records. Mirrors :mod:`rpa_executor.DIRNAME_RPA`.
DIRNAME_RPA = "rpa"
RECORD_FILENAME = "rpa-record.json"

DEFAULT_FLUSH_INTERVAL_SECONDS = 1.0
DEFAULT_MAX_LINES_PER_FRAME = 200


def _now_rfc3339() -> str:
    rtnval = datetime.now(timezone.utc).isoformat()
    return rtnval


def post_rpa_progress_frames(*, client: Any, run_id: str, frames: List[Dict[str, Any]]) -> bool:
    """
        Posts a batch of generalized ``rpa`` frames through the REST
        client's ``progress.progress_post_frame`` method as a plain JSON
        body ``{"frame_kind": "rpa", "frames": [...]}``.

        The endpoint answers HTTP 202; an empty-body JSON decode failure
        after a non-raising call is treated as success. Any other failure
        is logged and reported as ``False``.

        :returns: True when the batch was accepted.
    """
    if client is None:
        return False
    body = {"frame_kind": RPA_FRAME_KIND, "frames": list(frames)}
    try:
        client.progress.progress_post_frame(run_id=run_id, body=body)
    except ValueError:
        return True
    except Exception as posterr:
        logger.warning(
            "pychestrate-client: rpa progress frame POST failed for run_id=%s "
            "(best-effort, job continues): %s", run_id, posterr,
        )
        return False
    return True


class RpaProgressReporter:
    """
        The :class:`RpaProgressReporter` records coarse state transitions
        and streams captured output lines for a dispatched RPA job.

        The executor calls :meth:`report_running` before launching the
        subprocess, :meth:`report_output_line` for every captured line
        (batched and flushed at most every ``flush_interval_seconds`` or
        ``max_lines_per_frame`` lines), and :meth:`report_finished` once
        the process exits. Frame posting uses ``post_func`` when supplied
        (tests) else :func:`post_rpa_progress_frames` with ``client``.
    """

    def __init__(self, *, state: ClientState, run_id: str, client_id: str,
                 client: Optional[Any] = None,
                 post_func: Optional[Callable[..., bool]] = None,
                 flush_interval_seconds: float = DEFAULT_FLUSH_INTERVAL_SECONDS,
                 max_lines_per_frame: int = DEFAULT_MAX_LINES_PER_FRAME,
                 time_func: Optional[Callable[[], float]] = None):
        self._state = state
        self._run_id = run_id
        self._client_id = client_id
        self._client = client
        self._post_func = post_func
        self._flush_interval_seconds = max(0.0, float(flush_interval_seconds))
        self._max_lines_per_frame = max(1, int(max_lines_per_frame))
        self._time_func = time_func if time_func is not None else time.monotonic
        self._lock = threading.Lock()
        self._transitions: List[Dict[str, Any]] = []
        self._pending_lines: List[Dict[str, Any]] = []
        self._pending_progress: Optional[Dict[str, Any]] = None
        self._last_flush_at = self._time_func()
        self._frame_index = 0
        self._frames_posted = 0
        self._frames_failed = 0
        self._current_phase = RPA_PROGRESS_STARTED
        self._append_transition(state=RPA_PROGRESS_STARTED, post=True)
        return

    @property
    def transitions(self) -> List[Dict[str, Any]]:
        return list(self._transitions)

    @property
    def frames_posted(self) -> int:
        return self._frames_posted

    @property
    def frames_failed(self) -> int:
        return self._frames_failed

    def report_running(self) -> None:
        self._append_transition(state=RPA_PROGRESS_RUNNING, post=True)
        return

    def report_output_line(self, line: str, *, stream: str = "stdout") -> None:
        """
            Queue one captured output line. A ``PYC-PROGRESS <json>`` line
            is also lifted into the frame's structured ``progress`` record
            (``percent_complete`` / ``notes``).
        """
        text = line.rstrip("\r\n")
        with self._lock:
            self._pending_lines.append({"line": text, "stream": stream, "ts": _now_rfc3339()})
            if text.startswith(PROGRESS_MARKER):
                try:
                    parsed = json.loads(text[len(PROGRESS_MARKER):])
                    if isinstance(parsed, dict):
                        self._pending_progress = parsed
                except ValueError:
                    pass
            due = len(self._pending_lines) >= self._max_lines_per_frame
            elapsed = self._time_func() - self._last_flush_at
            if due is False and elapsed >= self._flush_interval_seconds:
                due = True
        if due:
            self.flush()
        return

    def flush(self) -> bool:
        """
            Post the pending lines (if any) as one frame.

            :returns: True when nothing was pending or the frame was accepted.
        """
        with self._lock:
            if len(self._pending_lines) == 0 and self._pending_progress is None:
                self._last_flush_at = self._time_func()
                return True
            lines = list(self._pending_lines)
            progress = self._pending_progress
            self._pending_lines = []
            self._pending_progress = None
            self._last_flush_at = self._time_func()
        frame = self._build_frame(
            phase=self._current_phase, status="running", lines=lines, progress=progress,
        )
        rtnval = self._post([frame])
        return rtnval

    def report_finished(self, *, succeeded: bool) -> None:
        terminal_state = RPA_PROGRESS_SUCCEEDED if succeeded else RPA_PROGRESS_FAILED
        self.flush()
        self._append_transition(state=terminal_state, post=True)
        return

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------

    def _build_frame(self, *, phase: str, status: str, lines: List[Dict[str, Any]],
                     progress: Optional[Dict[str, Any]] = None,
                     notes: Optional[str] = None) -> Dict[str, Any]:
        with self._lock:
            index = self._frame_index
            self._frame_index += 1
        frame: Dict[str, Any] = {
            "client_id": self._client_id,
            "frame_index": index,
            "format_tag": RPA_PROGRESS_FORMAT_TAG,
            "phase": phase,
            "status": status,
            "recorded_at": _now_rfc3339(),
            "lines": lines,
        }
        if notes is not None:
            frame["notes"] = notes
        if isinstance(progress, dict):
            percent = progress.get("percent", None)
            if isinstance(percent, (int, float)):
                frame["percent_complete"] = float(percent)
            message = progress.get("message", None)
            if isinstance(message, str) and "notes" not in frame:
                frame["notes"] = message
            frame["progress"] = progress
        return frame

    def _post(self, frames: List[Dict[str, Any]]) -> bool:
        if self._post_func is not None:
            try:
                ok = bool(self._post_func(run_id=self._run_id, frames=frames))
            except Exception as posterr:
                logger.warning(
                    "pychestrate-client: rpa progress post_func failed for run_id=%s: %s",
                    self._run_id, posterr,
                )
                ok = False
        else:
            ok = post_rpa_progress_frames(client=self._client, run_id=self._run_id, frames=frames)
        if ok:
            self._frames_posted += 1
        else:
            self._frames_failed += 1
        return ok

    def _append_transition(self, *, state: str, post: bool) -> None:
        transition = {"state": state, "recorded_at": _now_rfc3339()}
        self._transitions.append(transition)
        self._current_phase = state
        logger.info(
            "pychestrate-client: RPA job run_id=%s entered state '%s'.", self._run_id, state,
        )
        try:
            self._persist()
        except OSError as persisterr:
            logger.warning(
                "pychestrate-client: failed to persist RPA progress for run_id=%s"
                " (best-effort, job continues): %s", self._run_id, persisterr,
            )
        if post and (self._client is not None or self._post_func is not None):
            status = "running" if state in (RPA_PROGRESS_STARTED, RPA_PROGRESS_RUNNING) else state
            frame = self._build_frame(
                phase=state, status=status, lines=[], notes="job {}".format(state),
            )
            self._post([frame])
        return

    def _persist(self) -> None:
        safe_run_id = self._run_id if len(self._run_id) > 0 else "unknown-rpa"
        record_dir = os.path.join(self._state.state_dir, DIRNAME_RPA, safe_run_id)
        os.makedirs(record_dir, mode=0o755, exist_ok=True)
        record_path = os.path.join(record_dir, RECORD_FILENAME)

        record: Dict[str, Any] = {}
        if os.path.isfile(record_path):
            try:
                with open(record_path, "r", encoding="utf-8") as recf:
                    loaded = json.load(recf)
                if isinstance(loaded, dict):
                    record = loaded
            except (ValueError, OSError):
                record = {}

        record["run_id"] = self._run_id
        record["client_id"] = self._client_id
        record["progress"] = {
            "transitions": list(self._transitions),
            "frames_posted": self._frames_posted,
            "frames_failed": self._frames_failed,
        }

        with open(record_path, "w", encoding="utf-8") as recf:
            json.dump(record, recf, indent=4, sort_keys=True)
            recf.write(os.linesep)
        return

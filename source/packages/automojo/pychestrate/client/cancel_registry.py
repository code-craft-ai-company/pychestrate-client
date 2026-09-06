"""
    .. module:: cancel_registry
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Process-wide registry of operator cancel requests the
                   heartbeat loop collects and the job executors poll.

                   The service has no inbound channel to a pod, so a cancel
                   rides the heartbeat response (``ClientHeartbeatAck.
                   cancellations``). The heartbeat thread records each
                   ``run_id`` here; a running executor polls
                   :meth:`CancelRegistry.is_cancelled` and terminates its
                   process group when the run is marked. Delivery is
                   at-least-once and the service re-sends until the pod
                   reports a ``cancelled`` terminal outcome, so repeats and
                   unknown runs are harmless no-ops.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2026, Automation Mojo LLC"
__credits__ = []


from typing import Any, Dict, Iterable, List, Optional

import logging
import threading


logger = logging.getLogger(__name__)


class CancelRegistry:
    """
        Thread-safe ``run_id -> cancel request`` map.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._requests: Dict[str, Dict[str, Any]] = {}
        return

    def mark(self, run_id: str, *, reason: str = "", request_id: str = "") -> bool:
        """
            Record a cancel request for ``run_id``.

            :returns: True when the run was not already marked.
        """
        key = str(run_id or "")
        if len(key) == 0:
            return False
        with self._lock:
            fresh = key not in self._requests
            if fresh:
                # Keep the first request's details; re-deliveries are no-ops.
                self._requests[key] = {"reason": reason, "request_id": request_id}
        if fresh:
            logger.info(
                "pychestrate-client: cancel requested for run_id=%s (request_id=%s): %s",
                key, request_id, reason,
            )
        return fresh

    def is_cancelled(self, run_id: str) -> bool:
        with self._lock:
            rtnval = str(run_id or "") in self._requests
        return rtnval

    def get(self, run_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            entry = self._requests.get(str(run_id or ""), None)
            rtnval = dict(entry) if entry is not None else None
        return rtnval

    def clear(self, run_id: str) -> None:
        with self._lock:
            self._requests.pop(str(run_id or ""), None)
        return

    def pending_run_ids(self) -> List[str]:
        with self._lock:
            rtnval = sorted(self._requests.keys())
        return rtnval


def extract_cancellations(payload: Any) -> List[Dict[str, str]]:
    """
        Pull the ``cancellations`` array out of a heartbeat acknowledgement,
        whatever shape the REST client handed back (a dict, a generated
        model with attributes, or ``None`` for the empty 204 answer).

        :returns: A list of ``{"run_id", "request_id", "reason"}`` dicts.
    """
    if payload is None:
        return []
    raw: Any = None
    if isinstance(payload, dict):
        raw = payload.get("cancellations", None)
    else:
        raw = getattr(payload, "cancellations", None)
        if raw is None and hasattr(payload, "to_dict"):
            try:
                raw = payload.to_dict().get("cancellations", None)
            except Exception:
                raw = None
    if not isinstance(raw, (list, tuple)):
        return []

    def _field(entry: Any, name: str) -> str:
        if isinstance(entry, dict):
            value = entry.get(name, "")
        else:
            value = getattr(entry, name, "")
        rtnval = str(value) if value is not None else ""
        return rtnval

    rtnval: List[Dict[str, str]] = []
    for entry in raw:
        run_id = _field(entry, "run_id")
        if len(run_id) == 0:
            continue
        rtnval.append({
            "run_id": run_id,
            "request_id": _field(entry, "request_id"),
            "reason": _field(entry, "reason"),
        })
    return rtnval


def register_cancellations(registry: "CancelRegistry", cancellations: Iterable[Dict[str, str]]) -> int:
    """
        Mark every cancellation in ``registry``.

        :returns: The number of runs newly marked.
    """
    fresh = 0
    for entry in cancellations:
        if registry.mark(entry.get("run_id", ""), reason=entry.get("reason", ""),
                         request_id=entry.get("request_id", "")):
            fresh += 1
    return fresh


#: The registry shared by the heartbeat thread and the dispatch consumer.
CANCEL_REGISTRY = CancelRegistry()

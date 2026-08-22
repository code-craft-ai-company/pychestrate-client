"""
    .. module:: cancellationregistry
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Process-wide record of runs the service has asked
                   this pod to stop.

                   The pod client runs two loops on two threads: the
                   heartbeat loop and the dispatch-consumer loop. The
                   cancel signal arrives on the heartbeat thread (the
                   service piggybacks pending cancellations on the
                   heartbeat response) but the thing that has to act on
                   it -- the running job's process tree -- is owned by
                   the dispatch thread. This registry is the hand-off
                   between them.

                   It has to be a process-wide singleton for the same
                   reason: neither loop constructs the other, so there
                   is no object either could be handed at construction
                   time. :func:`active_cancellation_registry` resolves
                   the shared instance; tests substitute their own with
                   :func:`set_active_cancellation_registry`.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2025, Automation Mojo LLC"
__credits__ = []


from typing import Dict, List, Optional

import logging
import threading


logger = logging.getLogger(__name__)


#: Reason recorded when the service delivered a cancellation with no
#: reason text of its own.
DEFAULT_CANCEL_REASON = "cancel requested by the service"


class CancellationRecord:
    """
        The :class:`CancellationRecord` captures one delivered
        cancellation: which run the service wants stopped, the
        cancel-request it came from, and why.
    """

    def __init__(self, *, run_id: str, request_id: Optional[str],
                 reason: str):
        """
            Initializes a :class:`CancellationRecord`.

            :param run_id: The server-assigned run UUID to stop.
            :param request_id: The server's cancel-request UUID, or
                               ``None`` when the delivery carried
                               none.
            :param reason: Operator-supplied cancel reason.
        """
        self._run_id = run_id
        self._request_id = request_id
        self._reason = reason
        return

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def request_id(self) -> Optional[str]:
        return self._request_id

    @property
    def reason(self) -> str:
        return self._reason


class CancellationRegistry:
    """
        The :class:`CancellationRegistry` holds the set of run ids the
        service has asked this pod to stop.

        Every method is safe to call from either loop's thread. The
        registry is deliberately dumb: it records and reports, and
        knows nothing about processes, jobs, or the REST surface.
    """

    def __init__(self):
        """
            Initializes an empty :class:`CancellationRegistry`.
        """
        self._lock = threading.Lock()
        self._records: Dict[str, CancellationRecord] = {}
        return

    def record(self, *, run_id: str, request_id: Optional[str] = None,
               reason: Optional[str] = None) -> bool:
        """
            Records a delivered cancellation for ``run_id``.

            Recording the same run twice is normal, not an error: the
            service re-delivers a cancellation on every heartbeat
            until the pod reports the run terminal, so at-least-once
            delivery is the contract. The first record wins; a repeat
            leaves the stored reason alone.

            :param run_id: The server-assigned run UUID to stop.
            :param request_id: The server's cancel-request UUID.
            :param reason: Operator-supplied cancel reason.

            :returns: True when this call recorded a cancellation the
                      registry had not seen before.
        """
        if run_id is None or len(run_id) == 0:
            rtnval = False
            return rtnval

        reason_text = reason
        if reason_text is None or len(reason_text) == 0:
            reason_text = DEFAULT_CANCEL_REASON

        with self._lock:
            existing = self._records.get(run_id, None)
            if existing is not None:
                rtnval = False
                return rtnval

            self._records[run_id] = CancellationRecord(
                run_id=run_id, request_id=request_id, reason=reason_text)

        logger.info(
            "pychestrate-client: recorded cancellation for run_id=%s "
            "(request_id=%s): %s",
            run_id, request_id, reason_text,
        )

        rtnval = True
        return rtnval

    def is_cancelled(self, *, run_id: str) -> bool:
        """
            Reports whether a cancellation has been recorded for
            ``run_id``.

            :param run_id: The server-assigned run UUID.

            :returns: True when the run is under cancellation.
        """
        if run_id is None or len(run_id) == 0:
            rtnval = False
            return rtnval

        with self._lock:
            found = self._records.get(run_id, None)

        rtnval = found is not None
        return rtnval

    def find(self, *, run_id: str) -> Optional[CancellationRecord]:
        """
            Returns the recorded cancellation for ``run_id``.

            :param run_id: The server-assigned run UUID.

            :returns: The :class:`CancellationRecord`, or ``None``
                      when the run is not under cancellation.
        """
        if run_id is None or len(run_id) == 0:
            rtnval = None
            return rtnval

        with self._lock:
            rtnval = self._records.get(run_id, None)

        return rtnval

    def forget(self, *, run_id: str) -> bool:
        """
            Drops the recorded cancellation for ``run_id``.

            Called once the run has been stopped and reported, so the
            registry does not grow without bound over the lifetime of
            a long-lived pod process.

            :param run_id: The server-assigned run UUID.

            :returns: True when a record was dropped.
        """
        if run_id is None or len(run_id) == 0:
            rtnval = False
            return rtnval

        with self._lock:
            existing = self._records.pop(run_id, None)

        rtnval = existing is not None
        return rtnval

    def cancelled_run_ids(self) -> List[str]:
        """
            Returns the run ids currently under cancellation.

            :returns: A list of run UUID strings.
        """
        with self._lock:
            rtnval = list(self._records.keys())

        return rtnval


#: The process-wide registry the heartbeat loop writes to and the
#: dispatch-consumer loop reads from. Resolved through
#: :func:`active_cancellation_registry` rather than imported directly
#: so tests can swap it.
ACTIVE_CANCELLATION_REGISTRY = CancellationRegistry()


def active_cancellation_registry() -> CancellationRegistry:
    """
        Returns the process-wide :class:`CancellationRegistry`.

        :returns: The active registry instance.
    """
    rtnval = ACTIVE_CANCELLATION_REGISTRY
    return rtnval


def set_active_cancellation_registry(*, registry: CancellationRegistry) -> None:
    """
        Publishes the process-wide :class:`CancellationRegistry`.

        :param registry: The registry instance to publish.

        :returns: None
    """
    global ACTIVE_CANCELLATION_REGISTRY
    ACTIVE_CANCELLATION_REGISTRY = registry
    return

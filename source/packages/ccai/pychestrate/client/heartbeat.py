"""
    .. module:: heartbeat
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Periodic heartbeat loop. The pod-side client sends a
                   liveness ping to ``GET /api/v1/clients/{client_id}/heartbeat``
                   over mTLS so the service registry can keep ``last_seen``
                   current.

                   As of M2 the heartbeat is performed through the codegen'd
                   ``ccai.pychestrate.rest.client.groups.clients`` method
                   ``clients_heartbeat``. The M1-era raw ``requests.Session``
                   workaround has been removed.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2025, Automation Mojo LLC"
__credits__ = []


from typing import Any, Callable, Optional

import logging
import os
import time

from ccai.errors.exceptions import BadParameterError, ConfigurationError

from ccai.pychestrate.client.mtlsclientcredential import MtlsClientCredential
from ccai.pychestrate.client.pychestratemtlsclient import PychestrateMtlsClient
from ccai.pychestrate.client.cancel_registry import (
    CANCEL_REGISTRY,
    extract_cancellations,
    register_cancellations,
)
from ccai.pychestrate.client.shutdown import (
    ShutdownController,
    install_handlers_if_main_thread,
)
from ccai.pychestrate.client.state import ClientState


DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 30.0
DEFAULT_HEARTBEAT_BASE_BACKOFF_SECONDS = 5.0
DEFAULT_HEARTBEAT_MAX_BACKOFF_SECONDS = 300.0

logger = logging.getLogger(__name__)


class HeartbeatAnomaly(Exception):
    """
        Raised when the heartbeat gets an answer that is neither the
        documented 204 nor a JSON ``ClientHeartbeatAck`` -- typically a
        non-JSON 200 body from a login page, proxy or wrong base URL.
    """


class HeartbeatAspects:
    """
        The :class:`HeartbeatAspects` packages caller-controlled behavior for
        the heartbeat loop (interval, back-off bounds, max iterations).

        Following the project's Aspects pattern, behavior modification is
        carried as data on a single parameter rather than via decorators.
    """

    def __init__(self, *, interval_seconds: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
                 base_backoff_seconds: float = DEFAULT_HEARTBEAT_BASE_BACKOFF_SECONDS,
                 max_backoff_seconds: float = DEFAULT_HEARTBEAT_MAX_BACKOFF_SECONDS,
                 max_iterations: Optional[int] = None,
                 sleep_func: Optional[Callable[[float], None]] = None):
        """
            Initializes a :class:`HeartbeatAspects` instance.

            :param interval_seconds: Seconds between successful heartbeats.
            :param base_backoff_seconds: Initial sleep applied after a failure
                                          (doubles on each consecutive failure).
            :param max_backoff_seconds: Upper bound on the exponential back-off
                                         interval.
            :param max_iterations: Optional iteration cap (primarily for tests).
                                    ``None`` runs until interrupted by a signal.
            :param sleep_func: Optional sleep override (tests). Without one the
                               loop pauses on its :class:`ShutdownController`,
                               so a stop request ends the pause at once.

            :raises: BadParameterError
        """
        if interval_seconds <= 0:
            raise BadParameterError(
                "HeartbeatAspects requires 'interval_seconds' > 0.",
                "interval_seconds", "Pass a positive number of seconds.")
        if base_backoff_seconds <= 0:
            raise BadParameterError(
                "HeartbeatAspects requires 'base_backoff_seconds' > 0.",
                "base_backoff_seconds", "Pass a positive number of seconds.")
        if max_backoff_seconds < base_backoff_seconds:
            raise BadParameterError(
                "HeartbeatAspects requires 'max_backoff_seconds' >= 'base_backoff_seconds'.",
                "max_backoff_seconds", "Pass a value not smaller than 'base_backoff_seconds'.")

        self._interval_seconds = interval_seconds
        self._base_backoff_seconds = base_backoff_seconds
        self._max_backoff_seconds = max_backoff_seconds
        self._max_iterations = max_iterations
        self._sleep_overridden = sleep_func is not None
        self._sleep_func = sleep_func if sleep_func is not None else time.sleep
        return

    @property
    def interval_seconds(self) -> float:
        return self._interval_seconds

    @property
    def base_backoff_seconds(self) -> float:
        return self._base_backoff_seconds

    @property
    def max_backoff_seconds(self) -> float:
        return self._max_backoff_seconds

    @property
    def max_iterations(self) -> Optional[int]:
        return self._max_iterations

    @property
    def sleep_func(self) -> Callable[[float], None]:
        return self._sleep_func

    @property
    def sleep_overridden(self) -> bool:
        return self._sleep_overridden


DEFAULT_HEARTBEAT_ASPECTS = HeartbeatAspects()


def build_client_from_state(state: ClientState) -> PychestrateMtlsClient:
    """
        Constructs a :class:`PychestrateMtlsClient` configured from an enrolled
        :class:`ClientState`.

        :param state: A :class:`ClientState` instance for which
                      :meth:`ClientState.is_enrolled` returns ``True``.

        :returns: A ready-to-use :class:`PychestrateMtlsClient`.
        :raises: ConfigurationError
    """
    if not state.is_enrolled():
        errmsg_lines = [
            "The pychestrate-client state directory is not fully enrolled.",
            "    state_dir: {}".format(state.state_dir),
            "Run 'pychestrate-client enroll ...' before starting the heartbeat.",
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise ConfigurationError(errmsg)

    config = state.read_config()

    host = config.get("host")
    scheme = config.get("scheme", "https")
    port = config.get("port")
    client_id = state.read_client_id()

    if host is None or len(host) == 0:
        raise ConfigurationError("Persisted config.json is missing the 'host' field.")

    credential = MtlsClientCredential(
        identifier="pychestrate-client-{}".format(client_id),
        client_cert_path=state.client_cert_path,
        client_key_path=state.client_key_path,
        service_ca_path=state.service_ca_path,
    )

    # Enrollments made before base_path was recorded get the default
    # (/api/v1), which is where the service mounts its API.
    base_path = config.get("base_path")

    rtnval = PychestrateMtlsClient(
        host=host, credential=credential, port=port, scheme=scheme, base_path=base_path)
    return rtnval


def send_one_heartbeat(client: PychestrateMtlsClient, client_id: str) -> int:
    """
        Performs a single heartbeat ping against
        ``GET /api/v1/clients/{client_id}/heartbeat`` through the
        codegen'd ``clients.clients_heartbeat`` method.

        The service answers a heartbeat with HTTP 204 and no body, or with
        HTTP 200 and a ``ClientHeartbeatAck`` carrying queued operator
        cancellations. The codegen'd method returns None for the 204 and
        the decoded body for the 200, so a :class:`ValueError` from it
        means a 200 whose body is not JSON -- a login page, a proxy, a base
        URL missing ``/api/v1`` -- which is raised as
        :class:`HeartbeatAnomaly` instead of being counted as a success.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param client_id: The assigned client UUID.

        :returns: The HTTP status code of the heartbeat (204, or 200 when
                  the answer carried cancellations).
        :raises: HeartbeatAnomaly, requests.RequestException, codecraftai
                 HTTP error types
    """
    if client._session is None:
        client.authenticate()

    try:
        acknowledgement = client.clients.clients_heartbeat(
            client_id=client_id, exp_status=[200, 204])
    except ValueError as decode_err:
        errmsg = (
            "the heartbeat answered with a body that is not JSON ({}); the "
            "configured service URL may be wrong (for example missing /api/v1) "
            "or a proxy or login page answered instead of the service".format(
                decode_err)
        )
        raise HeartbeatAnomaly(errmsg) from decode_err

    if acknowledgement is None:
        rtnval = 204
        return rtnval

    # A 200 answer carries queued operator cancellations
    # (``ClientHeartbeatAck.cancellations``); record them so the dispatch
    # consumer's executors can stop the affected runs.
    cancellations = extract_cancellations(acknowledgement)
    if len(cancellations) > 0:
        fresh = register_cancellations(CANCEL_REGISTRY, cancellations)
        logger.info(
            "pychestrate-client: heartbeat delivered %d cancellation(s) (%d new) "
            "for client_id=%s.", len(cancellations), fresh, client_id,
        )

    rtnval = 200
    return rtnval


def run_heartbeat_loop(state: ClientState,
                       aspects: HeartbeatAspects = DEFAULT_HEARTBEAT_ASPECTS,
                       stop_predicate: Optional[Callable[[], bool]] = None,
                       shutdown: Optional[ShutdownController] = None) -> int:
    """
        Runs the periodic heartbeat loop until a stop is requested
        (SIGTERM/SIGINT, or the ``shutdown`` controller), the
        ``stop_predicate`` returns ``True``, or ``aspects.max_iterations``
        is exhausted.

        On a successful heartbeat (HTTP 2xx) the loop pauses for
        ``interval_seconds``. On a failure it logs the error, pauses for an
        exponentially-doubling back-off (clamped at ``max_backoff_seconds``),
        and retries. Pauses end as soon as a stop is requested.

        :param state: The :class:`ClientState` to drive the client from.
        :param aspects: Behavior-shaping aspects.
        :param stop_predicate: Optional callable returning ``True`` to halt
                                the loop. Useful for tests.
        :param shutdown: The stop request to honor. ``run`` passes the one
                         whose signal handlers it installed on the main
                         thread. When omitted the loop makes its own and, if
                         it is on the main thread, installs SIGINT/SIGTERM
                         handlers for it.

        :returns: The number of completed iterations.
    """
    client = build_client_from_state(state=state)
    client_id = state.read_client_id()

    restore_handlers = None
    if shutdown is None:
        shutdown = ShutdownController()
        restore_handlers = install_handlers_if_main_thread(shutdown=shutdown)

    def _pause(seconds: float) -> None:
        if aspects.sleep_overridden:
            aspects.sleep_func(seconds)
        else:
            shutdown.wait(timeout_seconds=seconds)
        return

    iterations = 0
    backoff = aspects.base_backoff_seconds

    try:
        while True:
            if shutdown.is_stop_requested():
                logger.info("pychestrate-client: heartbeat loop stopping (%s).", shutdown.reason)
                break
            if stop_predicate is not None and stop_predicate():
                break
            if aspects.max_iterations is not None and iterations >= aspects.max_iterations:
                break

            iterations += 1

            success = False
            try:
                status_code = send_one_heartbeat(client=client, client_id=client_id)

                if 200 <= status_code < 300:
                    success = True
                    logger.info(
                        "pychestrate-client: heartbeat ok (iteration=%d, status=%d).",
                        iterations, status_code,
                    )
                else:
                    logger.warning(
                        "pychestrate-client: heartbeat returned non-success status "
                        "(iteration=%d, status=%d).",
                        iterations, status_code,
                    )

            except Exception as failerr:
                logger.warning(
                    "pychestrate-client: heartbeat raised an exception "
                    "(iteration=%d): %s",
                    iterations, failerr,
                )

            if success:
                backoff = aspects.base_backoff_seconds
                next_sleep = aspects.interval_seconds
            else:
                next_sleep = backoff
                doubled = backoff * 2.0
                if doubled > aspects.max_backoff_seconds:
                    doubled = aspects.max_backoff_seconds
                backoff = doubled

            _pause(next_sleep)

    finally:
        if restore_handlers is not None:
            restore_handlers()

    rtnval = iterations
    return rtnval

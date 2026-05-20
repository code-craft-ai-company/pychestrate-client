"""
    .. module:: heartbeat
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Periodic heartbeat loop. The pod-side client sends a
                   liveness ping to ``GET /api/v1/clients/{client_id}/heartbeat``
                   over mTLS so the service registry can keep ``last_seen``
                   current.

                   NOTE: the codegen'd ``automojo.pychestrate.rest.client.groups.
                   clients`` module does NOT yet expose a ``clients_heartbeat``
                   method (the codegen source spec is older than the service
                   spec). Until the REST client is regenerated, this module
                   performs the heartbeat by directly invoking the mTLS-enabled
                   ``requests.Session`` exposed by the :class:`PychestrateMtlsClient`.
                   When the REST client gains a generated ``clients_heartbeat``,
                   this module should be migrated to call it.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2025, Automation Mojo LLC"
__credits__ = []


from typing import Any, Callable, Optional

import logging
import os
import signal
import time

from automojo.errors.exceptions import BadParameterError, ConfigurationError

from automojo.pychestrate.client.mtlsclientcredential import MtlsClientCredential
from automojo.pychestrate.client.pychestratemtlsclient import PychestrateMtlsClient
from automojo.pychestrate.client.state import ClientState


DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 30.0
DEFAULT_HEARTBEAT_BASE_BACKOFF_SECONDS = 5.0
DEFAULT_HEARTBEAT_MAX_BACKOFF_SECONDS = 300.0
DEFAULT_HEARTBEAT_PATH_TEMPLATE = "/api/v1/clients/{client_id}/heartbeat"

logger = logging.getLogger(__name__)


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
            :param sleep_func: Optional sleep override; defaults to ``time.sleep``.

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

    rtnval = PychestrateMtlsClient(host=host, credential=credential, port=port, scheme=scheme)
    return rtnval


def send_one_heartbeat(client: PychestrateMtlsClient, client_id: str) -> int:
    """
        Performs a single heartbeat ping against
        ``GET /api/v1/clients/{client_id}/heartbeat``.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param client_id: The assigned client UUID.

        :returns: The HTTP status code returned by the service.
        :raises: requests.RequestException
    """
    leaf = DEFAULT_HEARTBEAT_PATH_TEMPLATE.format(client_id=client_id)
    target_url = "{}{}".format(client._host_url, leaf)

    if client._session is None:
        client.authenticate()

    session = client._session
    response = session.get(target_url)

    rtnval = response.status_code
    return rtnval


def run_heartbeat_loop(state: ClientState,
                       aspects: HeartbeatAspects = DEFAULT_HEARTBEAT_ASPECTS,
                       stop_predicate: Optional[Callable[[], bool]] = None) -> int:
    """
        Runs the periodic heartbeat loop until interrupted by SIGTERM/SIGINT,
        the ``stop_predicate`` returns ``True``, or ``aspects.max_iterations``
        is exhausted.

        On a successful heartbeat (HTTP 2xx) the loop sleeps for
        ``interval_seconds``. On a failure it logs the error, sleeps for an
        exponentially-doubling back-off (clamped at ``max_backoff_seconds``),
        and retries.

        :param state: The :class:`ClientState` to drive the client from.
        :param aspects: Behavior-shaping aspects.
        :param stop_predicate: Optional callable returning ``True`` to halt
                                the loop. Useful for tests.

        :returns: The number of completed iterations.
    """
    client = build_client_from_state(state=state)
    client_id = state.read_client_id()
    sleep_func = aspects.sleep_func

    stop_flag = {"requested": False}

    def _on_signal(signum: int, frame: Any) -> None:
        logger.info("pychestrate-client: received signal %d; stopping heartbeat loop.", signum)
        stop_flag["requested"] = True
        return

    previous_term = None
    previous_int = None
    try:
        previous_term = signal.signal(signal.SIGTERM, _on_signal)
    except (ValueError, OSError):
        previous_term = None
    try:
        previous_int = signal.signal(signal.SIGINT, _on_signal)
    except (ValueError, OSError):
        previous_int = None

    iterations = 0
    backoff = aspects.base_backoff_seconds

    try:
        while True:
            if stop_flag["requested"]:
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

            sleep_func(next_sleep)

    finally:
        if previous_term is not None:
            try:
                signal.signal(signal.SIGTERM, previous_term)
            except (ValueError, OSError):
                pass
        if previous_int is not None:
            try:
                signal.signal(signal.SIGINT, previous_int)
            except (ValueError, OSError):
                pass

    rtnval = iterations
    return rtnval

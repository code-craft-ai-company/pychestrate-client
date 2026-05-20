"""
    .. module:: dispatch_consumer
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Long-poll dispatch consumer. Pulls signed job envelopes
                   from ``GET /clients/{client_id}/dispatches:next``, verifies
                   the Ed25519 signature, shells out to ``automojo-testbase``
                   to run the job, and acknowledges the dispatch.

                   M2 scope: poll -> verify -> execute -> ack. Progress
                   streaming and full result-tree upload are M3.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2025, Automation Mojo LLC"
__credits__ = []


from typing import Any, Callable, Dict, Optional

import logging
import os
import signal
import time
import uuid

from datetime import datetime, timezone

from automojo.errors.exceptions import BadParameterError, ConfigurationError

from automojo.pychestrate.client.envelope_verifier import (
    EnvelopeVerificationResult,
    load_signing_public_key,
    verify_signed_envelope,
)
from automojo.pychestrate.client.heartbeat import build_client_from_state
from automojo.pychestrate.client.pychestratemtlsclient import PychestrateMtlsClient
from automojo.pychestrate.client.state import ClientState
from automojo.pychestrate.client.testbase_executor import (
    DEFAULT_TESTBASE_EXECUTOR_ASPECTS,
    TestbaseExecutorAspects,
    TestbaseRunOutcome,
    execute_job_envelope,
)


DEFAULT_DISPATCH_POLL_TIMEOUT_SECONDS = 30
DEFAULT_DISPATCH_BASE_BACKOFF_SECONDS = 5.0
DEFAULT_DISPATCH_MAX_BACKOFF_SECONDS = 300.0

logger = logging.getLogger(__name__)


class DispatchConsumerAspects:
    """
        The :class:`DispatchConsumerAspects` packages caller-controlled
        behavior for the dispatch-consumer loop (long-poll timeout,
        failure back-off bounds, iteration cap, sleep override, and the
        testbase-executor aspects forwarded to each job run).

        Following the project's Aspects pattern, behavior modification is
        carried as data on a single parameter rather than via decorators.
    """

    def __init__(self, *,
                 poll_timeout_seconds: int = DEFAULT_DISPATCH_POLL_TIMEOUT_SECONDS,
                 base_backoff_seconds: float = DEFAULT_DISPATCH_BASE_BACKOFF_SECONDS,
                 max_backoff_seconds: float = DEFAULT_DISPATCH_MAX_BACKOFF_SECONDS,
                 max_iterations: Optional[int] = None,
                 sleep_func: Optional[Callable[[float], None]] = None,
                 executor_aspects: TestbaseExecutorAspects =
                 DEFAULT_TESTBASE_EXECUTOR_ASPECTS):
        """
            Initializes a :class:`DispatchConsumerAspects` instance.

            :param poll_timeout_seconds: Long-poll wait window passed to
                                         ``clients_dispatches_next``.
            :param base_backoff_seconds: Initial sleep applied after a
                                         poll failure (doubles on each
                                         consecutive failure).
            :param max_backoff_seconds: Upper bound on the exponential
                                        back-off interval.
            :param max_iterations: Optional iteration cap (primarily for
                                   tests). ``None`` runs until signalled.
            :param sleep_func: Optional sleep override; defaults to
                               :func:`time.sleep`.
            :param executor_aspects: Aspects forwarded to the testbase
                                     executor for each dispatched job.

            :raises: BadParameterError
        """
        if poll_timeout_seconds < 0:
            raise BadParameterError(
                "DispatchConsumerAspects requires 'poll_timeout_seconds' >= 0.",
                "poll_timeout_seconds", "Pass a non-negative integer.")
        if base_backoff_seconds <= 0:
            raise BadParameterError(
                "DispatchConsumerAspects requires 'base_backoff_seconds' > 0.",
                "base_backoff_seconds", "Pass a positive number of seconds.")
        if max_backoff_seconds < base_backoff_seconds:
            raise BadParameterError(
                "DispatchConsumerAspects requires 'max_backoff_seconds' >= "
                "'base_backoff_seconds'.",
                "max_backoff_seconds",
                "Pass a value not smaller than 'base_backoff_seconds'.")

        self._poll_timeout_seconds = poll_timeout_seconds
        self._base_backoff_seconds = base_backoff_seconds
        self._max_backoff_seconds = max_backoff_seconds
        self._max_iterations = max_iterations
        self._sleep_func = sleep_func if sleep_func is not None else time.sleep
        self._executor_aspects = executor_aspects
        return

    @property
    def poll_timeout_seconds(self) -> int:
        return self._poll_timeout_seconds

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
    def executor_aspects(self) -> TestbaseExecutorAspects:
        return self._executor_aspects


DEFAULT_DISPATCH_CONSUMER_ASPECTS = DispatchConsumerAspects()


def _now_rfc3339() -> str:
    """
        Produces the current UTC time as an RFC 3339 string.

        :returns: An RFC 3339 timestamp string.
    """
    rtnval = datetime.now(timezone.utc).isoformat()
    return rtnval


def poll_next_dispatch(client: PychestrateMtlsClient, client_id: str,
                       timeout_seconds: int) -> Optional[Any]:
    """
        Performs a single long-poll for the next dispatch via the
        codegen'd ``clients.clients_dispatches_next`` method.

        The endpoint answers HTTP 204 with an empty body when the wait
        window elapses with no work. The codegen'd method decodes the
        response body as JSON unconditionally, so an empty 204 surfaces
        as a :class:`ValueError`. Because 204 IS the documented timeout
        contract, a decode failure following a clean (non-raising)
        ``perform_call`` is treated as "no dispatch available".

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param client_id: The assigned client UUID.
        :param timeout_seconds: Long-poll wait window in seconds.

        :returns: The ``Pendingdispatch`` object on a dispatch, or
                  ``None`` on a timeout.
        :raises: requests.RequestException, automojo HTTP error types
    """
    try:
        pending = client.clients.clients_dispatches_next(
            client_id=client_id,
            timeout_seconds=timeout_seconds,
        )
    except ValueError:
        # An expected 204 with an empty body; perform_call did not raise
        # an HTTP error, so this is simply a long-poll timeout.
        logger.debug(
            "pychestrate-client: dispatch long-poll timed out (204) for "
            "client_id=%s.", client_id,
        )
        rtnval = None
        return rtnval

    rtnval = pending
    return rtnval


def _pending_to_dict(pending: Any) -> Dict[str, Any]:
    """
        Coerces a ``Pendingdispatch`` schema object (or a raw dict) into a
        plain dict the consumer can read uniformly.

        :param pending: The object returned by ``poll_next_dispatch``.

        :returns: A dict view of the pending dispatch.
    """
    if isinstance(pending, dict):
        rtnval = pending
        return rtnval

    to_dict = getattr(pending, "to_dict", None)
    if callable(to_dict):
        rtnval = to_dict()
        return rtnval

    # Fall back to attribute access for the documented required fields.
    rtnval = {
        "dispatch_attempt_id": getattr(pending, "dispatch_attempt_id", None),
        "run_id": getattr(pending, "run_id", None),
        "client_id": getattr(pending, "client_id", None),
        "pod_id": getattr(pending, "pod_id", None),
        "envelope_id": getattr(pending, "envelope_id", None),
        "envelope": getattr(pending, "envelope", None),
        "signature": getattr(pending, "signature", None),
        "signature_key_id": getattr(pending, "signature_key_id", None),
        "signed_at": getattr(pending, "signed_at", None),
    }
    return rtnval


def acknowledge_dispatch(client: PychestrateMtlsClient, run_id: str,
                         envelope_id: str, dispatch_attempt_id: str,
                         client_id: str, local_run_id: str,
                         notes: Optional[str] = None) -> None:
    """
        Posts a dispatch acknowledgement via the codegen'd
        ``dispatch.dispatch_ack_run`` method.

        The ``DispatchAck`` schema models receipt confirmation only — it
        carries no accepted/rejected flag. A rejection (failed signature
        verification) or an execution failure is therefore conveyed in
        the free-form ``notes`` field so the service operator can see
        why the run will not progress.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param run_id: The server-assigned run UUID.
        :param envelope_id: The dispatched envelope id.
        :param dispatch_attempt_id: The dispatch attempt UUID.
        :param client_id: The assigned client UUID.
        :param local_run_id: The client-minted local run identifier.
        :param notes: Optional free-form note (rejection / failure
                      reason).

        :returns: None
        :raises: requests.RequestException, automojo HTTP error types
    """
    from automojo.pychestrate.rest.client.models.dispatch import Dispatchack

    ack = Dispatchack(
        envelope_id=envelope_id,
        dispatch_attempt_id=dispatch_attempt_id,
        client_id=client_id,
        local_run_id=local_run_id,
        accepted_at=_now_rfc3339(),
    )
    if notes is not None and len(notes) > 0:
        ack.notes = notes

    try:
        client.dispatch.dispatch_ack_run(run_id=run_id, body=ack)
    except ValueError:
        # dispatch-ack answers HTTP 204 with an empty body; an empty-body
        # JSON decode failure after a non-raising perform_call means the
        # ack was accepted.
        logger.debug(
            "pychestrate-client: dispatch-ack 204 for run_id=%s.", run_id,
        )
    return


def handle_one_dispatch(client: PychestrateMtlsClient, state: ClientState,
                        pending: Any, signing_public_key: Any,
                        signing_key_id: str,
                        aspects: DispatchConsumerAspects) -> bool:
    """
        Verifies, executes, and acknowledges a single pending dispatch.

        The flow is:

            1. Verify the envelope's Ed25519 signature. On failure: log
               loudly, ack with a rejection note, do NOT execute.
            2. On a verified envelope: shell out to testbase to run the
               job (resilient — never crashes the consumer).
            3. Ack the dispatch with the execution outcome.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param state: The :class:`ClientState`.
        :param pending: The ``Pendingdispatch`` object.
        :param signing_public_key: The trusted Ed25519 public key.
        :param signing_key_id: The key id the trusted key was installed
                               under.
        :param aspects: Behavior-shaping aspects.

        :returns: True when the dispatch was executed; False when it was
                  rejected before execution.
    """
    pending_dict = _pending_to_dict(pending=pending)

    run_id = str(pending_dict.get("run_id", ""))
    envelope_id = str(pending_dict.get("envelope_id", ""))
    dispatch_attempt_id = str(pending_dict.get("dispatch_attempt_id", ""))
    client_id = str(pending_dict.get("client_id", ""))
    envelope = pending_dict.get("envelope", None)
    signature_hex = str(pending_dict.get("signature", ""))
    signature_key_id = str(pending_dict.get("signature_key_id", ""))

    local_run_id = str(uuid.uuid4())

    if not isinstance(envelope, dict):
        detail = (
            "Dispatch run_id={} carries no envelope object; rejecting "
            "without execution.".format(run_id)
        )
        logger.error("pychestrate-client: %s", detail)
        acknowledge_dispatch(
            client=client, run_id=run_id, envelope_id=envelope_id,
            dispatch_attempt_id=dispatch_attempt_id, client_id=client_id,
            local_run_id=local_run_id,
            notes="REJECTED: malformed dispatch (no envelope).")
        rtnval = False
        return rtnval

    verification: EnvelopeVerificationResult = verify_signed_envelope(
        envelope=envelope,
        signature_hex=signature_hex,
        signature_key_id=signature_key_id,
        signing_public_key=signing_public_key,
        signing_key_id=signing_key_id,
    )

    if not verification.valid:
        detail = verification.detail
        if detail is None:
            detail = "envelope signature verification failed"
        logger.error(
            "pychestrate-client: REJECTING dispatch run_id=%s envelope_id=%s "
            "reason=%s :: %s",
            run_id, envelope_id, verification.reason, detail,
        )
        acknowledge_dispatch(
            client=client, run_id=run_id, envelope_id=envelope_id,
            dispatch_attempt_id=dispatch_attempt_id, client_id=client_id,
            local_run_id=local_run_id,
            notes="REJECTED: envelope verification failed ({}): {}".format(
                verification.reason, detail))
        rtnval = False
        return rtnval

    outcome: TestbaseRunOutcome = execute_job_envelope(
        state=state,
        envelope=envelope,
        local_run_id=local_run_id,
        aspects=aspects.executor_aspects,
    )

    if outcome.succeeded:
        notes = "ACCEPTED: testbase run succeeded (output={}).".format(
            outcome.output_dir)
    else:
        notes = "ACCEPTED: testbase run FAILED ({}).".format(outcome.detail)

    acknowledge_dispatch(
        client=client, run_id=run_id, envelope_id=envelope_id,
        dispatch_attempt_id=dispatch_attempt_id, client_id=client_id,
        local_run_id=local_run_id, notes=notes)

    logger.info(
        "pychestrate-client: dispatch run_id=%s handled (status=%s).",
        run_id, outcome.status,
    )

    rtnval = True
    return rtnval


def run_dispatch_consumer_loop(state: ClientState,
                               aspects: DispatchConsumerAspects =
                               DEFAULT_DISPATCH_CONSUMER_ASPECTS,
                               stop_predicate: Optional[Callable[[], bool]] = None
                               ) -> int:
    """
        Runs the dispatch-consumer loop until interrupted by
        SIGTERM/SIGINT, the ``stop_predicate`` returns ``True``, or
        ``aspects.max_iterations`` is exhausted.

        Each iteration long-polls for the next dispatch. On a timeout the
        loop simply re-polls. On a dispatch it verifies, executes, and
        acks. On a transport failure it logs and applies an exponential
        back-off (clamped at ``max_backoff_seconds``).

        The consumer fails closed on the envelope signing public key: if
        no key is installed it raises :class:`ConfigurationError` before
        the loop starts rather than executing unverified envelopes.

        :param state: The :class:`ClientState` to drive the client from.
        :param aspects: Behavior-shaping aspects.
        :param stop_predicate: Optional callable returning ``True`` to
                               halt the loop. Useful for tests.

        :returns: The number of completed iterations.
        :raises: ConfigurationError
    """
    client = build_client_from_state(state=state)
    client_id = state.read_client_id()

    signing_public_key = load_signing_public_key(state=state)
    config = state.read_config()
    signing_key_id = config.get("envelope_signing_key_id", None)
    if signing_key_id is None or len(signing_key_id) == 0:
        errmsg_lines = [
            "The pychestrate-client config does not record the envelope "
            "signing key id.",
            "    config: {}".format(state.config_path),
            "Install the signing key with 'pychestrate-client "
            "install-signing-key --key-id <id> ...'.",
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise ConfigurationError(errmsg)

    sleep_func = aspects.sleep_func
    stop_flag = {"requested": False}

    def _on_signal(signum: int, frame: Any) -> None:
        logger.info(
            "pychestrate-client: received signal %d; stopping dispatch "
            "consumer loop.", signum,
        )
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
            if aspects.max_iterations is not None and \
                    iterations >= aspects.max_iterations:
                break

            iterations += 1

            poll_failed = False
            try:
                pending = poll_next_dispatch(
                    client=client,
                    client_id=client_id,
                    timeout_seconds=aspects.poll_timeout_seconds,
                )

                if pending is None:
                    logger.debug(
                        "pychestrate-client: no dispatch available "
                        "(iteration=%d).", iterations,
                    )
                else:
                    handle_one_dispatch(
                        client=client,
                        state=state,
                        pending=pending,
                        signing_public_key=signing_public_key,
                        signing_key_id=signing_key_id,
                        aspects=aspects,
                    )

            except Exception as polerr:
                poll_failed = True
                logger.warning(
                    "pychestrate-client: dispatch poll/handle raised an "
                    "exception (iteration=%d): %s",
                    iterations, polerr,
                )

            if poll_failed:
                next_sleep = backoff
                doubled = backoff * 2.0
                if doubled > aspects.max_backoff_seconds:
                    doubled = aspects.max_backoff_seconds
                backoff = doubled
                sleep_func(next_sleep)
            else:
                backoff = aspects.base_backoff_seconds

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

"""
    .. module:: dispatch_consumer
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Long-poll dispatch consumer. Pulls signed job envelopes
                   from ``GET /clients/{client_id}/dispatches:next``, verifies
                   the Ed25519 signature, shells out to ``automojo-testbase``
                   to run the job, and acknowledges the dispatch.

                   M2 scope: poll -> verify -> execute -> ack.

                   M3 scope: the loop now closes the orchestration loop on
                   the client side --
                   poll -> verify -> execute (with live progress streaming)
                   -> upload result tree -> POST results/complete -> ack.
                   Progress streaming is best-effort; the result-tree upload
                   + ``results/complete`` POST are the authoritative run
                   completion signal.

                   M5 scope: after verification the consumer branches on
                   the envelope's ``job_kind`` discriminator. A
                   ``job_kind=test`` envelope runs the testbase executor (the
                   unchanged M3 path); a ``job_kind=build`` envelope runs the
                   build executor (source acquisition -> makefile runner ->
                   artifact upload); a ``job_kind=provision`` envelope is not
                   yet supported -- it is acked with a rejection note rather
                   than crashing the consumer. A missing / unrecognized
                   ``job_kind`` defaults to ``test`` for backward
                   compatibility.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2025, Automation Mojo LLC"
__credits__ = []


from typing import Any, Callable, Dict, Optional

import logging
import signal
import time
import uuid

from datetime import datetime, timezone

from automojo.errors.exceptions import BadParameterError, ConfigurationError

from automojo.pychestrate.client.build_executor import (
    DEFAULT_BUILD_EXECUTOR_ASPECTS,
    BuildExecutorAspects,
    BuildRunOutcome,
    execute_build_envelope,
)
from automojo.pychestrate.client.build_progress_reporter import (
    BuildProgressReporter,
)
from automojo.pychestrate.client.envelope_verifier import (
    EnvelopeVerificationResult,
    verify_dispatch_envelope,
)
from automojo.pychestrate.client.heartbeat import build_client_from_state
from automojo.pychestrate.client.progress_reporter import (
    DEFAULT_PROGRESS_REPORTER_ASPECTS,
    ProgressReporter,
    ProgressReporterAspects,
)
from automojo.pychestrate.client.pychestratemtlsclient import PychestrateMtlsClient
from automojo.pychestrate.client.result_uploader import (
    DEFAULT_RESULT_UPLOADER_ASPECTS,
    ResultUploaderAspects,
    upload_build_results,
    upload_run_results,
)
from automojo.pychestrate.client.signing_keys import refresh_signing_keys
from automojo.pychestrate.client.state import ClientState
from automojo.pychestrate.client.testbase_executor import (
    DEFAULT_TESTBASE_EXECUTOR_ASPECTS,
    TestbaseExecutorAspects,
    TestbaseRunOutcome,
    execute_job_envelope,
    resolve_run_output_dir,
)


#: ``job_kind`` discriminator values carried on a dispatched envelope. The
#: consumer branches on this before resolving the kind-specific executor.
#: These mirror the server's ``jobs.envelope.schema`` constants.
JOB_KIND_TEST = "test"
JOB_KIND_BUILD = "build"
JOB_KIND_PROVISION = "provision"

#: The ``job_kind`` an envelope defaults to when the discriminator is
#: missing or unrecognized -- matches the server's ``DEFAULT_JOB_KIND``.
DEFAULT_JOB_KIND = JOB_KIND_TEST


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
                 DEFAULT_TESTBASE_EXECUTOR_ASPECTS,
                 build_executor_aspects: BuildExecutorAspects =
                 DEFAULT_BUILD_EXECUTOR_ASPECTS,
                 progress_reporter_aspects: ProgressReporterAspects =
                 DEFAULT_PROGRESS_REPORTER_ASPECTS,
                 result_uploader_aspects: ResultUploaderAspects =
                 DEFAULT_RESULT_UPLOADER_ASPECTS):
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
                                     executor for each dispatched
                                     ``job_kind=test`` job.
            :param build_executor_aspects: Aspects forwarded to the build
                                           executor for each dispatched
                                           ``job_kind=build`` job.
            :param progress_reporter_aspects: Aspects forwarded to the
                                              progress reporter created for
                                              each dispatched job.
            :param result_uploader_aspects: Aspects forwarded to the result
                                            uploader run after each
                                            dispatched job.

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
        self._build_executor_aspects = build_executor_aspects
        self._progress_reporter_aspects = progress_reporter_aspects
        self._result_uploader_aspects = result_uploader_aspects
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

    @property
    def build_executor_aspects(self) -> BuildExecutorAspects:
        return self._build_executor_aspects

    @property
    def progress_reporter_aspects(self) -> ProgressReporterAspects:
        return self._progress_reporter_aspects

    @property
    def result_uploader_aspects(self) -> ResultUploaderAspects:
        return self._result_uploader_aspects


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


def _resolve_job_kind(envelope: Dict[str, Any]) -> str:
    """
        Resolves the ``job_kind`` discriminator from a verified envelope.

        An envelope that omits ``job_kind`` (or carries an unrecognized
        value) defaults to ``test`` for backward compatibility -- this
        matches the server's ``DEFAULT_JOB_KIND``: envelopes minted before
        the discriminator was introduced still route to the testbase
        executor.

        :param envelope: The verified envelope dict.

        :returns: One of ``test`` / ``build`` / ``provision``.
    """
    raw = envelope.get("job_kind", None)
    if raw is None:
        rtnval = DEFAULT_JOB_KIND
        return rtnval

    candidate = str(raw)
    if candidate in (JOB_KIND_TEST, JOB_KIND_BUILD, JOB_KIND_PROVISION):
        rtnval = candidate
        return rtnval

    logger.warning(
        "pychestrate-client: envelope carries an unrecognized job_kind=%r; "
        "defaulting to '%s' for backward compatibility.",
        candidate, DEFAULT_JOB_KIND,
    )
    rtnval = DEFAULT_JOB_KIND
    return rtnval


def _handle_test_job(*, client: PychestrateMtlsClient, state: ClientState,
                      envelope: Dict[str, Any], run_id: str, client_id: str,
                      local_run_id: str,
                      aspects: DispatchConsumerAspects) -> str:
    """
        Runs a verified ``job_kind=test`` envelope through the testbase
        executor with live progress streaming, uploads the result tree,
        and returns the ack note.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param state: The :class:`ClientState`.
        :param envelope: The verified envelope dict.
        :param run_id: The server-assigned run UUID.
        :param client_id: The assigned client UUID.
        :param local_run_id: The client-minted local run identifier.
        :param aspects: Behavior-shaping aspects.

        :returns: The free-form ack note describing the run + upload.
    """
    progress_reporter = ProgressReporter(
        client=client,
        run_id=run_id,
        client_id=client_id,
        output_dir=resolve_run_output_dir(state=state, run_id=run_id),
        aspects=aspects.progress_reporter_aspects,
    )

    outcome: TestbaseRunOutcome = execute_job_envelope(
        state=state,
        envelope=envelope,
        local_run_id=local_run_id,
        aspects=aspects.executor_aspects,
        progress_reporter=progress_reporter,
    )

    upload_outcome = upload_run_results(
        client=client,
        state=state,
        envelope=envelope,
        result_dir=outcome.output_dir,
        run_succeeded=outcome.succeeded,
        aspects=aspects.result_uploader_aspects,
    )

    if outcome.succeeded:
        run_note = "testbase run succeeded (output={})".format(
            outcome.output_dir)
    else:
        run_note = "testbase run FAILED ({})".format(outcome.detail)

    if upload_outcome.succeeded:
        upload_note = "results uploaded ({} files)".format(
            upload_outcome.file_count)
    else:
        upload_note = "result upload INCOMPLETE ({})".format(
            upload_outcome.detail)

    rtnval = "ACCEPTED: {}; {}.".format(run_note, upload_note)
    return rtnval


def _handle_build_job(*, client: PychestrateMtlsClient, state: ClientState,
                       envelope: Dict[str, Any], run_id: str, client_id: str,
                       local_run_id: str,
                       aspects: DispatchConsumerAspects) -> str:
    """
        Runs a verified ``job_kind=build`` envelope through the build
        executor (source acquisition + makefile runner), uploads the build
        artifact tree, and returns the ack note.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param state: The :class:`ClientState`.
        :param envelope: The verified envelope dict.
        :param run_id: The server-assigned run UUID.
        :param client_id: The assigned client UUID.
        :param local_run_id: The client-minted local run identifier.
        :param aspects: Behavior-shaping aspects.

        :returns: The free-form ack note describing the build + upload.
    """
    progress_reporter = BuildProgressReporter(
        state=state, run_id=run_id, client_id=client_id)

    outcome: BuildRunOutcome = execute_build_envelope(
        state=state,
        envelope=envelope,
        local_run_id=local_run_id,
        aspects=aspects.build_executor_aspects,
        progress_reporter=progress_reporter,
    )

    upload_outcome = upload_build_results(
        client=client,
        state=state,
        envelope=envelope,
        result_dir=outcome.output_dir,
        run_succeeded=outcome.succeeded,
        aspects=aspects.result_uploader_aspects,
    )

    if outcome.succeeded:
        run_note = "build run succeeded (output={})".format(
            outcome.output_dir)
    else:
        run_note = "build run FAILED ({})".format(outcome.detail)

    if upload_outcome.succeeded:
        upload_note = "artifacts uploaded ({} files)".format(
            upload_outcome.file_count)
    else:
        upload_note = "artifact upload INCOMPLETE ({})".format(
            upload_outcome.detail)

    rtnval = "ACCEPTED: {}; {}.".format(run_note, upload_note)
    return rtnval


def handle_one_dispatch(client: PychestrateMtlsClient, state: ClientState,
                        pending: Any,
                        aspects: DispatchConsumerAspects) -> bool:
    """
        Verifies, executes, and acknowledges a single pending dispatch.

        The flow is:

            1. Verify the envelope's Ed25519 signature. The signing key is
               resolved from the persisted JWKS-style signing-key set by
               the envelope's ``signature_key_id``; an unknown key id
               triggers a single refresh from
               ``GET /api/v1/envelope-signing-keys`` before failing. On a
               verification failure: log loudly, ack with a rejection note,
               do NOT execute.
            2. On a verified envelope: shell out to testbase to run the
               job (resilient — never crashes the consumer), streaming live
               progress to the service for the lifetime of the run.
            3. Upload the result tree to the shared results backend and POST
               ``results/complete`` (resilient — a failed upload is marked
               on the local run record for retry).
            4. Ack the dispatch with the execution + upload outcome.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param state: The :class:`ClientState`.
        :param pending: The ``Pendingdispatch`` object.
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

    def _refresh_signing_keys() -> Any:
        rtnval = refresh_signing_keys(client=client, state=state)
        return rtnval

    verification: EnvelopeVerificationResult = verify_dispatch_envelope(
        state=state,
        envelope=envelope,
        signature_hex=signature_hex,
        signature_key_id=signature_key_id,
        refresh_func=_refresh_signing_keys,
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

    job_kind = _resolve_job_kind(envelope=envelope)

    if job_kind == JOB_KIND_PROVISION:
        # The provisioning executor is a later PR. A provision envelope
        # must NOT crash the consumer -- it is acked with a rejection note
        # so the service operator sees this client cannot run it.
        logger.warning(
            "pychestrate-client: provision jobs not yet supported by this "
            "client (run_id=%s envelope_id=%s); acking with rejection.",
            run_id, envelope_id,
        )
        acknowledge_dispatch(
            client=client, run_id=run_id, envelope_id=envelope_id,
            dispatch_attempt_id=dispatch_attempt_id, client_id=client_id,
            local_run_id=local_run_id,
            notes="REJECTED: provision jobs not yet supported by this "
                  "client.")
        rtnval = False
        return rtnval

    if job_kind == JOB_KIND_BUILD:
        notes = _handle_build_job(
            client=client, state=state, envelope=envelope, run_id=run_id,
            client_id=client_id, local_run_id=local_run_id, aspects=aspects)
    else:
        notes = _handle_test_job(
            client=client, state=state, envelope=envelope, run_id=run_id,
            client_id=client_id, local_run_id=local_run_id, aspects=aspects)

    acknowledge_dispatch(
        client=client, run_id=run_id, envelope_id=envelope_id,
        dispatch_attempt_id=dispatch_attempt_id, client_id=client_id,
        local_run_id=local_run_id, notes=notes)

    logger.info(
        "pychestrate-client: dispatch run_id=%s handled (job_kind=%s).",
        run_id, job_kind,
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

        The consumer fails closed on the envelope signing keys: each
        dispatched envelope is verified against the persisted JWKS-style
        signing-key set, resolved by the envelope's ``signature_key_id``.
        An unknown key id triggers a single refresh from
        ``GET /api/v1/envelope-signing-keys``; an envelope whose key still
        cannot be resolved is rejected without execution.

        :param state: The :class:`ClientState` to drive the client from.
        :param aspects: Behavior-shaping aspects.
        :param stop_predicate: Optional callable returning ``True`` to
                               halt the loop. Useful for tests.

        :returns: The number of completed iterations.
        :raises: ConfigurationError
    """
    client = build_client_from_state(state=state)
    client_id = state.read_client_id()

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

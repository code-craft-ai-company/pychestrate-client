"""
    .. module:: dispatch_consumer
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Long-poll dispatch consumer. Pulls signed job envelopes
                   from ``GET /clients/{client_id}/dispatches:next``, verifies
                   the Ed25519 signature, shells out to ``codecraftai-testbase``
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
                   artifact upload); a ``job_kind=provision`` envelope runs
                   the provision executor. An envelope that OMITS
                   ``job_kind`` entirely defaults to ``test`` for backward
                   compatibility with envelopes minted before the
                   discriminator existed; an envelope carrying a job kind
                   this client does not recognize is REFUSED and acked with
                   a rejection note, never coerced into a kind it happens
                   to know. Guessing there would run a future or foreign
                   job's parameters through the testbase executor, which is
                   a worse failure than declining the work.

                   Cancellation: each dispatched job is executed with the
                   process-wide cancellation registry the heartbeat loop
                   feeds. A cancel that lands while the job is running
                   terminates its process tree and the run is reported
                   ``CANCELLED``; a cancel that lands between dispatch and
                   execution prevents the execution outright.

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

from ccai.errors.exceptions import BadParameterError, ConfigurationError

from ccai.pychestrate.client.cancellationregistry import (
    CancellationRegistry,
    active_cancellation_registry,
)
from ccai.pychestrate.client.build_executor import (
    DEFAULT_BUILD_EXECUTOR_ASPECTS,
    BuildExecutorAspects,
    BuildRunOutcome,
    execute_build_envelope,
)
from ccai.pychestrate.client.build_progress_reporter import (
    BuildProgressReporter,
)
from ccai.pychestrate.client.provision_executor import (
    DEFAULT_PROVISION_EXECUTOR_ASPECTS,
    ProvisionExecutorAspects,
    ProvisionRunOutcome,
    execute_provision_envelope,
)
from ccai.pychestrate.client.provision_progress_reporter import (
    ProvisionProgressReporter,
)
from ccai.pychestrate.client.envelope_verifier import (
    EnvelopeVerificationResult,
    verify_dispatch_envelope,
)
from ccai.pychestrate.client.heartbeat import build_client_from_state
from ccai.pychestrate.client.jwks import (
    DEFAULT_JWKS_ASPECTS,
    JwksAspects,
    refresh_jwks,
)
from ccai.pychestrate.client.progress_reporter import (
    DEFAULT_PROGRESS_REPORTER_ASPECTS,
    ProgressReporter,
    ProgressReporterAspects,
)
from ccai.pychestrate.client.pychestratemtlsclient import PychestrateMtlsClient
from ccai.pychestrate.client.result_uploader import (
    DEFAULT_RESULT_UPLOADER_ASPECTS,
    ResultUploaderAspects,
    retry_pending_uploads,
    upload_build_results,
    upload_provision_results,
    upload_run_results,
)
from ccai.pychestrate.client.run_outcomes import (
    FAILURE_CLASSIFICATION_SETUP_ERROR,
    TERMINAL_OUTCOME_CANCELLED,
)
from ccai.pychestrate.client.signing_keys import refresh_signing_keys
from ccai.pychestrate.client.state import ClientState
from ccai.pychestrate.client.testbase_executor import (
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
#: ABSENT -- matches the server's ``DEFAULT_JOB_KIND``. This covers
#: envelopes minted before the discriminator existed. It deliberately
#: does NOT cover an envelope that carries a job kind this client does
#: not recognize: that one is refused, because running a future or
#: foreign job kind's parameters through the testbase executor would
#: execute something nobody asked for.
DEFAULT_JOB_KIND = JOB_KIND_TEST

#: The job kinds this client knows how to execute.
SUPPORTED_JOB_KINDS = (JOB_KIND_TEST, JOB_KIND_BUILD, JOB_KIND_PROVISION)


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
                 provision_executor_aspects: ProvisionExecutorAspects =
                 DEFAULT_PROVISION_EXECUTOR_ASPECTS,
                 progress_reporter_aspects: ProgressReporterAspects =
                 DEFAULT_PROGRESS_REPORTER_ASPECTS,
                 result_uploader_aspects: ResultUploaderAspects =
                 DEFAULT_RESULT_UPLOADER_ASPECTS,
                 jwks_aspects: JwksAspects = DEFAULT_JWKS_ASPECTS,
                 cancellation_registry: Optional[CancellationRegistry] = None):
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
            :param provision_executor_aspects: Aspects forwarded to the
                                               provision executor for each
                                               dispatched
                                               ``job_kind=provision`` job.
            :param progress_reporter_aspects: Aspects forwarded to the
                                              progress reporter created for
                                              each dispatched job.
            :param result_uploader_aspects: Aspects forwarded to the result
                                            uploader run after each
                                            dispatched job.
            :param jwks_aspects: Aspects controlling retrieval and caching
                                 of the service's RFC 7517 JWK Set of
                                 envelope signing public keys.
            :param cancellation_registry: Optional registry consulted
                                          while a job runs so an
                                          operator cancel delivered on
                                          the heartbeat can stop it.
                                          Defaults to the process-wide
                                          registry the heartbeat loop
                                          writes to.

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
        self._provision_executor_aspects = provision_executor_aspects
        self._progress_reporter_aspects = progress_reporter_aspects
        self._result_uploader_aspects = result_uploader_aspects
        self._jwks_aspects = jwks_aspects
        self._cancellation_registry = cancellation_registry
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
    def provision_executor_aspects(self) -> ProvisionExecutorAspects:
        return self._provision_executor_aspects

    @property
    def progress_reporter_aspects(self) -> ProgressReporterAspects:
        return self._progress_reporter_aspects

    @property
    def result_uploader_aspects(self) -> ResultUploaderAspects:
        return self._result_uploader_aspects

    @property
    def jwks_aspects(self) -> JwksAspects:
        return self._jwks_aspects

    @property
    def cancellation_registry(self) -> CancellationRegistry:
        """
            Gets the registry consulted while a job runs.

            Resolved lazily rather than captured at construction so a
            test that swaps the process-wide registry is honored by
            aspects built before the swap.

            :returns: The :class:`CancellationRegistry` to consult.
        """
        if self._cancellation_registry is not None:
            rtnval = self._cancellation_registry
            return rtnval

        rtnval = active_cancellation_registry()
        return rtnval


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
        :raises: requests.RequestException, ccai HTTP error types
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
        :raises: requests.RequestException, ccai HTTP error types
    """
    from ccai.pychestrate.rest.client.models.dispatch import Dispatchack

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


def _resolve_job_kind(envelope: Dict[str, Any]) -> Optional[str]:
    """
        Resolves the ``job_kind`` discriminator from a verified envelope.

        An envelope that OMITS ``job_kind`` defaults to ``test``, which
        matches the server's ``DEFAULT_JOB_KIND`` and keeps envelopes
        minted before the discriminator existed routing to the testbase
        executor.

        An envelope that carries a job kind this client does not
        recognize resolves to ``None`` -- a refusal. Coercing it to
        ``test`` would take a future or foreign job kind's parameters
        and run them through the testbase executor, executing something
        nobody asked for on a pod that has no idea what the job wanted.
        Declining work this client is too old to understand is the
        safer failure, and the rejection note tells the operator
        exactly which kind was refused.

        :param envelope: The verified envelope dict.

        :returns: One of ``test`` / ``build`` / ``provision``, or
                  ``None`` when the declared job kind is unrecognized.
    """
    raw = envelope.get("job_kind", None)
    if raw is None:
        rtnval = DEFAULT_JOB_KIND
        return rtnval

    candidate = str(raw)
    if candidate in SUPPORTED_JOB_KINDS:
        rtnval = candidate
        return rtnval

    logger.error(
        "pychestrate-client: envelope carries an unrecognized job_kind=%r; "
        "refusing the dispatch. Supported kinds: %s.",
        candidate, ", ".join(SUPPORTED_JOB_KINDS),
    )
    rtnval = None
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
        cancellation=aspects.cancellation_registry,
    )

    upload_outcome = upload_run_results(
        client=client,
        state=state,
        envelope=envelope,
        result_dir=outcome.output_dir,
        run_succeeded=outcome.succeeded,
        terminal_outcome=outcome.terminal_outcome,
        failure_classification=outcome.failure_classification,
        aspects=aspects.result_uploader_aspects,
    )

    if outcome.succeeded:
        run_note = "testbase run succeeded (output={})".format(
            outcome.output_dir)
    elif outcome.cancelled:
        run_note = "testbase run CANCELLED ({})".format(outcome.detail)
    else:
        run_note = "testbase run FAILED ({}; classification={})".format(
            outcome.detail, outcome.failure_classification)

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
        state=state, run_id=run_id, client_id=client_id,
        client=client, aspects=aspects.progress_reporter_aspects)

    outcome: BuildRunOutcome = execute_build_envelope(
        state=state,
        envelope=envelope,
        local_run_id=local_run_id,
        aspects=aspects.build_executor_aspects,
        progress_reporter=progress_reporter,
        cancellation=aspects.cancellation_registry,
    )

    upload_outcome = upload_build_results(
        client=client,
        state=state,
        envelope=envelope,
        result_dir=outcome.output_dir,
        run_succeeded=outcome.succeeded,
        terminal_outcome=outcome.terminal_outcome,
        failure_classification=outcome.failure_classification,
        aspects=aspects.result_uploader_aspects,
    )

    if outcome.succeeded:
        run_note = "build run succeeded (output={})".format(
            outcome.output_dir)
    elif outcome.cancelled:
        run_note = "build run CANCELLED ({})".format(outcome.detail)
    else:
        run_note = "build run FAILED ({}; classification={})".format(
            outcome.detail, outcome.failure_classification)

    if upload_outcome.succeeded:
        upload_note = "artifacts uploaded ({} files)".format(
            upload_outcome.file_count)
    else:
        upload_note = "artifact upload INCOMPLETE ({})".format(
            upload_outcome.detail)

    rtnval = "ACCEPTED: {}; {}.".format(run_note, upload_note)
    return rtnval


def _handle_provision_job(*, client: PychestrateMtlsClient,
                           state: ClientState, envelope: Dict[str, Any],
                           run_id: str, client_id: str, local_run_id: str,
                           aspects: DispatchConsumerAspects) -> str:
    """
        Runs a verified ``job_kind=provision`` envelope through the
        provision executor (provisioning-spec -> infra-tool invocation),
        uploads the provision output tree, and returns the ack note.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param state: The :class:`ClientState`.
        :param envelope: The verified envelope dict.
        :param run_id: The server-assigned run UUID.
        :param client_id: The assigned client UUID.
        :param local_run_id: The client-minted local run identifier.
        :param aspects: Behavior-shaping aspects.

        :returns: The free-form ack note describing the provision + upload.
    """
    progress_reporter = ProvisionProgressReporter(
        state=state, run_id=run_id, client_id=client_id,
        client=client, aspects=aspects.progress_reporter_aspects)

    outcome: ProvisionRunOutcome = execute_provision_envelope(
        state=state,
        envelope=envelope,
        local_run_id=local_run_id,
        aspects=aspects.provision_executor_aspects,
        progress_reporter=progress_reporter,
        cancellation=aspects.cancellation_registry,
    )

    upload_outcome = upload_provision_results(
        client=client,
        state=state,
        envelope=envelope,
        result_dir=outcome.output_dir,
        run_succeeded=outcome.succeeded,
        terminal_outcome=outcome.terminal_outcome,
        failure_classification=outcome.failure_classification,
        aspects=aspects.result_uploader_aspects,
    )

    if outcome.succeeded:
        run_note = "provision run succeeded (driver={} action={})".format(
            outcome.driver_name, outcome.action)
    elif outcome.cancelled:
        run_note = "provision run CANCELLED ({})".format(outcome.detail)
    else:
        run_note = "provision run FAILED ({}; classification={})".format(
            outcome.detail, outcome.failure_classification)

    if upload_outcome.succeeded:
        upload_note = "output uploaded ({} files)".format(
            upload_outcome.file_count)
    else:
        upload_note = "output upload INCOMPLETE ({})".format(
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
               resolved for the envelope's ``signature_key_id`` through the
               documented order in
               :func:`envelope_verifier.resolve_envelope_signing_key`: an
               operator-installed key overrides everything, otherwise the
               RFC 7517 JWK Set fetched and cached from the service's JWKS
               endpoint is used. On a verification failure: log loudly, ack
               with a rejection note, do NOT execute.
            2. Resolve the envelope's ``job_kind``. An absent
               discriminator defaults to ``test``; a job kind this
               client does not recognize is REFUSED and acked with a
               rejection note, never coerced into a kind it knows.
            3. Check the cancellation registry. A cancel that landed
               between dispatch and execution stops the job from ever
               starting -- there is no point launching work an operator
               has already called off.
            4. On a verified, supported, uncancelled envelope: shell out
               to the kind's executor (resilient — never crashes the
               consumer), streaming live progress to the service for the
               lifetime of the run. A cancel arriving DURING the run
               terminates the job's whole process tree and the run is
               reported ``CANCELLED``.
            5. Upload the result tree to the shared results backend and
               POST ``results/complete`` carrying the run's terminal
               outcome and its structured failure classification
               (resilient — a failed upload is marked on the local run
               record for retry).
            6. Ack the dispatch with the execution + upload outcome.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param state: The :class:`ClientState`.
        :param pending: The ``Pendingdispatch`` object.
        :param aspects: Behavior-shaping aspects.

        :returns: True when the dispatch was executed; False when it was
                  rejected before execution (unverifiable envelope,
                  unsupported job kind, or a cancel that arrived first).
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

    def _refresh_jwks() -> Any:
        rtnval = refresh_jwks(
            client=client, state=state, aspects=aspects.jwks_aspects)
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
        jwks_refresh_func=_refresh_jwks,
        cache_ttl_seconds=aspects.jwks_aspects.cache_ttl_seconds,
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

    if job_kind is None:
        declared = envelope.get("job_kind", None)
        detail = (
            "Dispatch run_id={} declares job_kind={!r}, which this client "
            "does not support; refusing without execution.".format(
                run_id, declared)
        )
        logger.error("pychestrate-client: %s", detail)
        acknowledge_dispatch(
            client=client, run_id=run_id, envelope_id=envelope_id,
            dispatch_attempt_id=dispatch_attempt_id, client_id=client_id,
            local_run_id=local_run_id,
            notes="REJECTED: unsupported job_kind={!r}; this client "
                  "supports {}. (classification={})".format(
                      declared, ", ".join(SUPPORTED_JOB_KINDS),
                      FAILURE_CLASSIFICATION_SETUP_ERROR))
        rtnval = False
        return rtnval

    registry = aspects.cancellation_registry
    if registry.is_cancelled(run_id=run_id):
        record = registry.find(run_id=run_id)
        reason = record.reason if record is not None else "cancelled"
        detail = (
            "Dispatch run_id={} was cancelled before execution began; "
            "not starting the job ({}).".format(run_id, reason)
        )
        logger.info("pychestrate-client: %s", detail)
        acknowledge_dispatch(
            client=client, run_id=run_id, envelope_id=envelope_id,
            dispatch_attempt_id=dispatch_attempt_id, client_id=client_id,
            local_run_id=local_run_id,
            notes="REJECTED: run cancelled before execution ({}); "
                  "terminal_outcome={}.".format(
                      reason, TERMINAL_OUTCOME_CANCELLED))
        rtnval = False
        return rtnval

    try:
        if job_kind == JOB_KIND_PROVISION:
            notes = _handle_provision_job(
                client=client, state=state, envelope=envelope, run_id=run_id,
                client_id=client_id, local_run_id=local_run_id,
                aspects=aspects)
        elif job_kind == JOB_KIND_BUILD:
            notes = _handle_build_job(
                client=client, state=state, envelope=envelope, run_id=run_id,
                client_id=client_id, local_run_id=local_run_id,
                aspects=aspects)
        else:
            notes = _handle_test_job(
                client=client, state=state, envelope=envelope, run_id=run_id,
                client_id=client_id, local_run_id=local_run_id,
                aspects=aspects)
    finally:
        # The run is over one way or another, so the pod no longer needs
        # to remember it was asked to stop it. Dropping the record here
        # keeps the registry from growing over the lifetime of a
        # long-running pod process, and is safe because the service
        # re-delivers a cancellation it has not seen acknowledged.
        registry.forget(run_id=run_id)

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

        Each iteration first sweeps the local state directory for runs
        whose results never made it upstream and re-attempts them, then
        long-polls for the next dispatch. On a timeout the loop simply
        re-polls. On a dispatch it verifies, executes, and acks. On a
        transport failure it logs and applies an exponential back-off
        (clamped at ``max_backoff_seconds``).

        The retry sweep runs every iteration rather than only after a
        dispatch: a stranded result is stranded whether or not new work
        arrives, and on an idle pod the dispatch branch never runs at
        all.

        The consumer fails closed on the envelope signing keys: each
        dispatched envelope is verified against a key resolved for its
        ``signature_key_id``, normally from the RFC 7517 JWK Set fetched and
        cached from the service's JWKS endpoint (an operator-installed key
        overrides it). An envelope whose key no source can resolve is
        rejected without execution.

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

            # Rescue any run whose results are still stranded on this
            # pod. An upload that failed mid-run leaves a durable
            # descriptor behind and returns; this sweep is the only
            # thing that ever retries it, so it has to run every
            # iteration rather than only when a dispatch arrives -- an
            # idle pod never reaches the dispatch branch at all.
            #
            # The sweep is non-raising by contract (a poisoned record is
            # logged and skipped). It is still guarded here, because
            # this loop is the pod's lifeline: a bug on the far side of
            # that contract must cost the pod one retry pass, not its
            # ability to accept work. Losing the sweep leaves results
            # stranded; losing the loop takes the pod offline.
            try:
                retry_pending_uploads(
                    client=client,
                    state=state,
                    aspects=aspects.result_uploader_aspects)
            except Exception as retryerr:
                logger.warning(
                    "pychestrate-client: pending-upload retry sweep raised "
                    "an exception (iteration=%d); continuing to poll for "
                    "dispatches: %s",
                    iterations, retryerr,
                )

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

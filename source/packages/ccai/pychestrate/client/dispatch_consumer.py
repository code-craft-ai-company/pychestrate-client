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
import random
import time
import uuid

from datetime import datetime, timezone

from ccai.errors.exceptions import BadParameterError, ConfigurationError

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
from ccai.pychestrate.client.rpa_executor import (
    DEFAULT_RPA_EXECUTOR_ASPECTS,
    RpaExecutorAspects,
    RpaRunOutcome,
    execute_rpa_envelope,
)
from ccai.pychestrate.client.rpa_progress_reporter import (
    RpaProgressReporter,
)
from ccai.pychestrate.client.cancel_registry import CANCEL_REGISTRY
from ccai.pychestrate.client.capabilities import advertise_pod_capabilities
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
    upload_build_results,
    upload_provision_results,
    upload_rpa_results,
    upload_run_results,
)
from ccai.pychestrate.client.shutdown import (
    ShutdownController,
    install_handlers_if_main_thread,
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
JOB_KIND_RPA = "rpa"

#: The ``job_kind`` an envelope defaults to when the discriminator is
#: missing or unrecognized -- matches the server's ``DEFAULT_JOB_KIND``.
DEFAULT_JOB_KIND = JOB_KIND_TEST


DEFAULT_DISPATCH_POLL_TIMEOUT_SECONDS = 30
DEFAULT_DISPATCH_BASE_BACKOFF_SECONDS = 5.0
DEFAULT_DISPATCH_MAX_BACKOFF_SECONDS = 300.0

#: Shortest time between the starts of two polls. A poll that comes back
#: sooner (an immediate 204, a proxy answering for the service) is followed
#: by a pause, so the loop cannot spin against the server.
DEFAULT_DISPATCH_MIN_POLL_INTERVAL_SECONDS = 1.0
#: Random extra pause (0 to this many seconds) added after an early poll so
#: many pods do not re-poll in lock step.
DEFAULT_DISPATCH_POLL_JITTER_SECONDS = 0.5
#: After this many polls in a row come back early, say so at INFO.
EARLY_POLL_NOTICE_THRESHOLD = 5

logger = logging.getLogger(__name__)


class DispatchPollAnomaly(Exception):
    """
        Raised when the dispatch poll gets an answer that is neither a
        dispatch nor the documented 204 timeout -- typically a non-JSON
        200 body from a login page, proxy or wrong base URL.
    """


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
                 min_poll_interval_seconds: float =
                 DEFAULT_DISPATCH_MIN_POLL_INTERVAL_SECONDS,
                 poll_jitter_seconds: float = DEFAULT_DISPATCH_POLL_JITTER_SECONDS,
                 executor_aspects: TestbaseExecutorAspects =
                 DEFAULT_TESTBASE_EXECUTOR_ASPECTS,
                 build_executor_aspects: BuildExecutorAspects =
                 DEFAULT_BUILD_EXECUTOR_ASPECTS,
                 provision_executor_aspects: ProvisionExecutorAspects =
                 DEFAULT_PROVISION_EXECUTOR_ASPECTS,
                 rpa_executor_aspects: RpaExecutorAspects =
                 DEFAULT_RPA_EXECUTOR_ASPECTS,
                 advertise_capabilities: bool = True,
                 progress_reporter_aspects: ProgressReporterAspects =
                 DEFAULT_PROGRESS_REPORTER_ASPECTS,
                 result_uploader_aspects: ResultUploaderAspects =
                 DEFAULT_RESULT_UPLOADER_ASPECTS,
                 jwks_aspects: JwksAspects = DEFAULT_JWKS_ASPECTS):
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
            :param sleep_func: Optional sleep override (tests). Without one
                               the loop pauses on its
                               :class:`ShutdownController`, so a stop
                               request ends the pause at once.
            :param min_poll_interval_seconds: Shortest time between the
                                              starts of two polls; a poll
                                              that returns sooner is
                                              followed by a pause.
            :param poll_jitter_seconds: Upper bound of the random extra
                                        pause added after an early poll.
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
        if min_poll_interval_seconds < 0:
            raise BadParameterError(
                "DispatchConsumerAspects requires 'min_poll_interval_seconds' >= 0.",
                "min_poll_interval_seconds", "Pass a non-negative number of seconds.")
        if poll_jitter_seconds < 0:
            raise BadParameterError(
                "DispatchConsumerAspects requires 'poll_jitter_seconds' >= 0.",
                "poll_jitter_seconds", "Pass a non-negative number of seconds.")

        self._poll_timeout_seconds = poll_timeout_seconds
        self._base_backoff_seconds = base_backoff_seconds
        self._max_backoff_seconds = max_backoff_seconds
        self._max_iterations = max_iterations
        self._sleep_overridden = sleep_func is not None
        self._sleep_func = sleep_func if sleep_func is not None else time.sleep
        self._min_poll_interval_seconds = min_poll_interval_seconds
        self._poll_jitter_seconds = poll_jitter_seconds
        self._executor_aspects = executor_aspects
        self._build_executor_aspects = build_executor_aspects
        self._provision_executor_aspects = provision_executor_aspects
        self._rpa_executor_aspects = rpa_executor_aspects
        self._advertise_capabilities = bool(advertise_capabilities)
        self._progress_reporter_aspects = progress_reporter_aspects
        self._result_uploader_aspects = result_uploader_aspects
        self._jwks_aspects = jwks_aspects
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
    def sleep_overridden(self) -> bool:
        return self._sleep_overridden

    @property
    def min_poll_interval_seconds(self) -> float:
        return self._min_poll_interval_seconds

    @property
    def poll_jitter_seconds(self) -> float:
        return self._poll_jitter_seconds

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
    def rpa_executor_aspects(self) -> RpaExecutorAspects:
        return self._rpa_executor_aspects

    @property
    def advertise_capabilities(self) -> bool:
        return self._advertise_capabilities

    @property
    def progress_reporter_aspects(self) -> ProgressReporterAspects:
        return self._progress_reporter_aspects

    @property
    def result_uploader_aspects(self) -> ResultUploaderAspects:
        return self._result_uploader_aspects

    @property
    def jwks_aspects(self) -> JwksAspects:
        return self._jwks_aspects


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
        window elapses with no work, and the codegen'd method returns
        ``None`` for it. A :class:`ValueError` therefore means the body of
        a 200 answer was not JSON -- a login page, a proxy's page, or a
        base URL missing ``/api/v1`` -- and is raised as
        :class:`DispatchPollAnomaly` so the loop backs off instead of
        re-polling at once.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param client_id: The assigned client UUID.
        :param timeout_seconds: Long-poll wait window in seconds.

        :returns: The ``Pendingdispatch`` object on a dispatch, or
                  ``None`` on a timeout.
        :raises: DispatchPollAnomaly, requests.RequestException,
                 codecraftai HTTP error types
    """
    try:
        pending = client.clients.clients_dispatches_next(
            client_id=client_id,
            timeout_seconds=timeout_seconds,
        )
    except ValueError as decode_err:
        errmsg = (
            "the dispatch poll answered with a body that is not JSON ({}); the "
            "configured service URL may be wrong (for example missing /api/v1) "
            "or a proxy or login page answered instead of the service".format(
                decode_err)
        )
        raise DispatchPollAnomaly(errmsg) from decode_err

    if pending is None:
        logger.debug(
            "pychestrate-client: dispatch long-poll timed out (204) for "
            "client_id=%s.", client_id,
        )

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
        :raises: requests.RequestException, codecraftai HTTP error types
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


def _resolve_job_kind(envelope: Dict[str, Any]) -> str:
    """
        Resolves the ``job_kind`` discriminator from a verified envelope.

        An envelope that omits ``job_kind`` (or carries an unrecognized
        value) defaults to ``test`` for backward compatibility -- this
        matches the server's ``DEFAULT_JOB_KIND``: envelopes minted before
        the discriminator was introduced still route to the testbase
        executor.

        :param envelope: The verified envelope dict.

        :returns: One of ``test`` / ``build`` / ``provision`` / ``rpa``.
    """
    raw = envelope.get("job_kind", None)
    if raw is None:
        rtnval = DEFAULT_JOB_KIND
        return rtnval

    candidate = str(raw)
    if candidate in (JOB_KIND_TEST, JOB_KIND_BUILD, JOB_KIND_PROVISION,
                     JOB_KIND_RPA):
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
        state=state, run_id=run_id, client_id=client_id)

    outcome: ProvisionRunOutcome = execute_provision_envelope(
        state=state,
        envelope=envelope,
        local_run_id=local_run_id,
        aspects=aspects.provision_executor_aspects,
        progress_reporter=progress_reporter,
    )

    upload_outcome = upload_provision_results(
        client=client,
        state=state,
        envelope=envelope,
        result_dir=outcome.output_dir,
        run_succeeded=outcome.succeeded,
        aspects=aspects.result_uploader_aspects,
    )

    if outcome.succeeded:
        run_note = "provision run succeeded (driver={} action={})".format(
            outcome.driver_name, outcome.action)
    else:
        run_note = "provision run FAILED ({})".format(outcome.detail)

    if upload_outcome.succeeded:
        upload_note = "output uploaded ({} files)".format(
            upload_outcome.file_count)
    else:
        upload_note = "output upload INCOMPLETE ({})".format(
            upload_outcome.detail)

    rtnval = "ACCEPTED: {}; {}.".format(run_note, upload_note)
    return rtnval


def _handle_rpa_job(*, client: PychestrateMtlsClient, state: ClientState,
                    envelope: Dict[str, Any], run_id: str, client_id: str,
                    local_run_id: str,
                    aspects: DispatchConsumerAspects) -> str:
    """
        Runs a verified ``job_kind=rpa`` envelope through the RPA executor
        (source acquisition + process / tasker subprocess), uploads the
        results directory, and returns the ack note.

        :returns: The free-form ack note describing the job + upload.
    """
    progress_reporter = RpaProgressReporter(
        state=state, run_id=run_id, client_id=client_id, client=client)

    # The heartbeat thread records operator cancel requests in the
    # shared registry; the executor polls it while the job runs.
    run_aspects = aspects.rpa_executor_aspects.with_cancel_check(
        lambda: CANCEL_REGISTRY.is_cancelled(run_id))
    try:
        outcome: RpaRunOutcome = execute_rpa_envelope(
            state=state,
            envelope=envelope,
            local_run_id=local_run_id,
            aspects=run_aspects,
            progress_reporter=progress_reporter,
        )
    finally:
        CANCEL_REGISTRY.clear(run_id)

    upload_outcome = upload_rpa_results(
        client=client,
        state=state,
        envelope=envelope,
        result_dir=outcome.output_dir,
        run_succeeded=outcome.succeeded,
        exit_code=outcome.exit_code,
        execution_mode=outcome.execution_mode,
        rpa_status=outcome.status,
        aspects=aspects.result_uploader_aspects,
    )

    if outcome.succeeded:
        run_note = "rpa job succeeded (type={} version={} mode={})".format(
            outcome.type_id, outcome.version, outcome.execution_mode)
    elif outcome.cancelled:
        run_note = "rpa job CANCELLED by operator request"
    elif outcome.timed_out:
        run_note = "rpa job TIMED OUT ({})".format(outcome.detail)
    else:
        run_note = "rpa job FAILED ({})".format(outcome.detail)

    if upload_outcome.succeeded:
        upload_note = "results uploaded ({} files)".format(
            upload_outcome.file_count)
    else:
        upload_note = "result upload INCOMPLETE ({})".format(
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
            2. On a verified envelope: ack the dispatch at once. The ack is
               the receipt the service requires before it accepts progress
               frames or artifact uploads for the run, so it must precede
               them. If the ack fails the job is not started.
            3. Run the job (resilient — never crashes the consumer),
               streaming live progress to the service for the lifetime of
               the run.
            4. Upload the result tree and POST ``results/complete``, which
               carries the outcome (resilient — a failed upload is marked
               on the local run record for retry).

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

    # Receipt first: the service refuses progress (404) and artifact
    # presigning (403) for a run it holds no dispatch receipt for.
    acknowledge_dispatch(
        client=client, run_id=run_id, envelope_id=envelope_id,
        dispatch_attempt_id=dispatch_attempt_id, client_id=client_id,
        local_run_id=local_run_id,
        notes="ACCEPTED: starting job_kind={}".format(job_kind))

    if job_kind == JOB_KIND_RPA:
        notes = _handle_rpa_job(
            client=client, state=state, envelope=envelope, run_id=run_id,
            client_id=client_id, local_run_id=local_run_id, aspects=aspects)
    elif job_kind == JOB_KIND_PROVISION:
        notes = _handle_provision_job(
            client=client, state=state, envelope=envelope, run_id=run_id,
            client_id=client_id, local_run_id=local_run_id, aspects=aspects)
    elif job_kind == JOB_KIND_BUILD:
        notes = _handle_build_job(
            client=client, state=state, envelope=envelope, run_id=run_id,
            client_id=client_id, local_run_id=local_run_id, aspects=aspects)
    else:
        notes = _handle_test_job(
            client=client, state=state, envelope=envelope, run_id=run_id,
            client_id=client_id, local_run_id=local_run_id, aspects=aspects)

    logger.info(
        "pychestrate-client: dispatch run_id=%s handled (job_kind=%s): %s",
        run_id, job_kind, notes,
    )

    rtnval = True
    return rtnval


def run_dispatch_consumer_loop(state: ClientState,
                               aspects: DispatchConsumerAspects =
                               DEFAULT_DISPATCH_CONSUMER_ASPECTS,
                               stop_predicate: Optional[Callable[[], bool]] = None,
                               shutdown: Optional[ShutdownController] = None
                               ) -> int:
    """
        Runs the dispatch-consumer loop until a stop is requested
        (SIGTERM/SIGINT, or the ``shutdown`` controller), the
        ``stop_predicate`` returns ``True``, or ``aspects.max_iterations``
        is exhausted.

        Each iteration long-polls for the next dispatch. On a timeout the
        loop re-polls, but never sooner than ``min_poll_interval_seconds``
        (plus jitter) after the previous poll started. On a dispatch it
        verifies, executes, and acks. On a transport failure or an answer
        that is not a dispatch (:class:`DispatchPollAnomaly`) it logs a
        warning and applies an exponential back-off (clamped at
        ``max_backoff_seconds``).

        A stop request is honored between iterations: a job already
        running finishes and is acked first, and a dispatch handed out by
        the last poll is acked as rejected rather than started. Pauses end
        as soon as a stop is requested.

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
        :param shutdown: The stop request to honor. ``run`` passes the one
                         whose signal handlers it installed on the main
                         thread. When omitted the loop makes its own and, if
                         it is on the main thread, installs SIGINT/SIGTERM
                         handlers for it.

        :returns: The number of completed iterations.
        :raises: ConfigurationError
    """
    client = build_client_from_state(state=state)
    client_id = state.read_client_id()
    if aspects.advertise_capabilities:
        try:
            advertise_pod_capabilities(
                client=client, state=state, client_id=client_id,
                taskplus_binary=aspects.rpa_executor_aspects.taskplus_binary)
        except Exception as adverr:
            logger.warning(
                "pychestrate-client: capability advertisement raised (loop continues): %s", adverr)

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
    early_polls_in_a_row = 0

    try:
        while True:
            if shutdown.is_stop_requested():
                logger.info(
                    "pychestrate-client: dispatch consumer stopping (%s).",
                    shutdown.reason)
                break
            if stop_predicate is not None and stop_predicate():
                break
            if aspects.max_iterations is not None and \
                    iterations >= aspects.max_iterations:
                break

            iterations += 1

            poll_failed = False
            returned_early = False
            poll_started = time.monotonic()
            try:
                pending = poll_next_dispatch(
                    client=client,
                    client_id=client_id,
                    timeout_seconds=aspects.poll_timeout_seconds,
                )
                poll_elapsed = time.monotonic() - poll_started

                if pending is None:
                    logger.debug(
                        "pychestrate-client: no dispatch available "
                        "(iteration=%d).", iterations,
                    )
                    returned_early = poll_elapsed < aspects.min_poll_interval_seconds
                elif shutdown.is_stop_requested():
                    # The last poll handed out a job after the stop request;
                    # tell the service instead of dropping it silently.
                    reject_dispatch_for_shutdown(client=client, pending=pending)
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
                    "pychestrate-client: dispatch poll/handle failed "
                    "(iteration=%d): %s; backing off %.1f s.",
                    iterations, polerr, backoff,
                )

            if poll_failed:
                next_sleep = backoff
                doubled = backoff * 2.0
                if doubled > aspects.max_backoff_seconds:
                    doubled = aspects.max_backoff_seconds
                backoff = doubled
                _pause(next_sleep)
            else:
                backoff = aspects.base_backoff_seconds

            if returned_early:
                early_polls_in_a_row += 1
                if early_polls_in_a_row == EARLY_POLL_NOTICE_THRESHOLD:
                    logger.info(
                        "pychestrate-client: the last %d dispatch polls returned "
                        "without waiting (poll timeout %d s); pausing between "
                        "polls. The service may not be long-polling.",
                        early_polls_in_a_row, aspects.poll_timeout_seconds)
                gap = aspects.min_poll_interval_seconds + random.uniform(
                    0, aspects.poll_jitter_seconds)
                _pause(gap)
            else:
                early_polls_in_a_row = 0

    finally:
        if restore_handlers is not None:
            restore_handlers()

    rtnval = iterations
    return rtnval


def reject_dispatch_for_shutdown(client: PychestrateMtlsClient, pending: Any) -> None:
    """
        Acknowledges a dispatch that arrived after a stop was requested,
        with a note saying it was not started, so the run does not sit in
        the dispatched state waiting for a pod that has gone.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param pending: The ``Pendingdispatch`` returned by the last poll.

        :returns: None
        :raises: requests.RequestException, codecraftai HTTP error types
    """
    pending_dict = _pending_to_dict(pending=pending)
    run_id = str(pending_dict.get("run_id", ""))
    logger.warning(
        "pychestrate-client: dispatch run_id=%s arrived while stopping; "
        "rejecting it without starting the job.", run_id)
    acknowledge_dispatch(
        client=client, run_id=run_id,
        envelope_id=str(pending_dict.get("envelope_id", "")),
        dispatch_attempt_id=str(pending_dict.get("dispatch_attempt_id", "")),
        client_id=str(pending_dict.get("client_id", "")),
        local_run_id=str(uuid.uuid4()),
        notes="REJECTED: pychestrate-client is shutting down; the job was not started.")
    return

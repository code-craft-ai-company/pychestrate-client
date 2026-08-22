"""
    .. module:: progress_reporter
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Streams live job progress from a running ``codecraftai-testbase``
                   invocation up to the Pychestrate service.

                   M3 scope: while the :mod:`testbase_executor` runs a
                   dispatched job, the progress reporter tails the testbase
                   ``testrun_results_stream.jsos`` file the run writes
                   incrementally, translates the freshly-appended JSOS test
                   records into the ``ProgressFrame`` OpenAPI schema, and POSTs
                   them to ``POST /api/v1/runs/{run_id}/progress`` via the
                   codegen'd ``progress`` REST group.

                   Progress capture approach -- the testbase executor shells
                   out to the ``testbase`` console-script as a child process
                   (see :mod:`testbase_executor`). The reporter therefore
                   cannot register an in-process testbase ``ForwardingInfo``
                   HTTP-forwarding callback against the run -- that mechanism
                   is only reachable from inside the testbase process. Of the
                   two viable approaches the reporter takes the file-tail
                   approach: testbase writes ``testrun_results_stream.jsos``
                   incrementally as each test scope completes, so the reporter
                   periodically re-parses the stream file (reusing testbase's
                   own ``jsosparser``) and forwards the records that appeared
                   since the previous poll.

                   Progress delivery never aborts a run, but it also never
                   silently discards records. A failed POST leaves the
                   forwarding cursor where it was, so the very next cycle
                   re-sends the same batch carrying the same record indexes.
                   The service ingester is idempotent -- a progress record is
                   unique on ``(run_id, record_index)`` and a generalized
                   frame is unique on ``(run_id, frame_kind, frame_index)`` --
                   so a re-send is a no-op on the service side rather than a
                   duplicate. The authoritative signal that a run completed is
                   the result upload + ``results/complete`` POST, not the
                   progress stream.

                   This module also carries the generalized (non test-shaped)
                   progress frame the ``build`` and ``provision`` reporters
                   post. Those runs have no per-test tally, so they post a
                   ``frame_kind`` of ``build`` / ``provision`` with a
                   phase / status / optional percent-complete frame instead of
                   the test-shaped summary + records body. Both bodies travel
                   through the same codegen'd ``progress.progress_post_frame``
                   operation -- the client never hand-rolls an HTTP call.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2025, Automation Mojo LLC"
__credits__ = []


from typing import Any, Callable, Dict, List, Optional

import logging
import os
import threading

from datetime import datetime, timezone

from ccai.errors.exceptions import BadParameterError

from ccai.testbase.cli.cmdtree.agents import jsosparser

from ccai.pychestrate.rest.client.models.progress import Progressframe
from ccai.pychestrate.rest.client.models.progressrecord import Progressrecord
from ccai.pychestrate.rest.client.models.progresssummary import Progresssummary

from ccai.restclient.base.schemafield import Field

from ccai.pychestrate.client.pychestratemtlsclient import PychestrateMtlsClient


logger = logging.getLogger(__name__)

#: Canonical name of the incremental JSOS stream file an ``codecraftai-testbase``
#: run writes into its output directory (see ``ccai.testbase.testjob``).
TESTBASE_RESULTS_STREAM_FILENAME = "testrun_results_stream.jsos"

#: Parser tag the progress ingester uses to select the testbase parser.
PROGRESS_FORMAT_TAG = "testbase"

#: Default seconds between consecutive stream-file polls.
DEFAULT_PROGRESS_POLL_INTERVAL_SECONDS = 5.0

#: Default number of POST attempts made for a single progress frame before
#: the frame is left pending for the next cycle.
DEFAULT_MAX_POST_ATTEMPTS = 3

#: Default seconds waited between two POST attempts of the same frame.
DEFAULT_POST_RETRY_DELAY_SECONDS = 1.0

#: Number of consecutive failed poll cycles after which the reporter escalates
#: from a per-cycle warning to an error, so an operator sees that live progress
#: has been down for a while instead of the reporter spinning quietly.
CONSECUTIVE_FAILURE_ALERT_THRESHOLD = 3

#: ``frame_kind`` discriminator values the service's progress route accepts.
#: ``test`` selects the test-shaped ``summary`` / ``records`` body; ``build``
#: and ``provision`` select the generalized ``frame`` body.
FRAME_KIND_TEST = "test"
FRAME_KIND_BUILD = "build"
FRAME_KIND_PROVISION = "provision"

#: The frame kinds :func:`build_generalized_frame` will mint a frame for.
GENERALIZED_FRAME_KINDS = (FRAME_KIND_BUILD, FRAME_KIND_PROVISION)

#: testbase JSOS ``result`` codes that count as a non-passing test.
_RESULT_FAILED = "FAILED"
_RESULT_ERRORED = "ERRORED"
_RESULT_SKIPPED = "SKIPPED"
_RESULT_PASSED = "PASSED"


class ProgressReporterAspects:
    """
        The :class:`ProgressReporterAspects` packages caller-controlled
        behavior for the progress reporter (the stream-file poll interval,
        the bounded post-retry budget, and the sleep override used by
        tests).

        Following the project's Aspects pattern, behavior modification is
        carried as data on a single parameter rather than via decorators.
    """

    def __init__(self, *,
                 poll_interval_seconds: float = DEFAULT_PROGRESS_POLL_INTERVAL_SECONDS,
                 max_post_attempts: int = DEFAULT_MAX_POST_ATTEMPTS,
                 post_retry_delay_seconds: float = DEFAULT_POST_RETRY_DELAY_SECONDS,
                 sleep_func: Optional[Callable[[float], None]] = None):
        """
            Initializes a :class:`ProgressReporterAspects` instance.

            :param poll_interval_seconds: Seconds the reporter waits between
                                          consecutive polls of the testbase
                                          ``testrun_results_stream.jsos``
                                          file.
            :param max_post_attempts: Number of POST attempts made for a
                                      single progress frame before the frame
                                      is left pending. Bounds the retry so a
                                      down service cannot stall the run.
            :param post_retry_delay_seconds: Seconds waited between two POST
                                             attempts of the same frame.
            :param sleep_func: Optional sleep override; defaults to
                               :func:`time.sleep`. Injected for testability.

            :raises: BadParameterError
        """
        if poll_interval_seconds <= 0:
            raise BadParameterError(
                "ProgressReporterAspects requires 'poll_interval_seconds' > 0.",
                "poll_interval_seconds", "Pass a positive number of seconds.")

        if max_post_attempts < 1:
            raise BadParameterError(
                "ProgressReporterAspects requires 'max_post_attempts' >= 1.",
                "max_post_attempts", "Pass at least one attempt.")

        if post_retry_delay_seconds < 0:
            raise BadParameterError(
                "ProgressReporterAspects requires "
                "'post_retry_delay_seconds' >= 0.",
                "post_retry_delay_seconds",
                "Pass a non-negative number of seconds.")

        sleep_resolved = sleep_func
        if sleep_resolved is None:
            import time as _time

            sleep_resolved = _time.sleep

        self._poll_interval_seconds = poll_interval_seconds
        self._max_post_attempts = max_post_attempts
        self._post_retry_delay_seconds = post_retry_delay_seconds
        self._sleep_func = sleep_resolved
        return

    @property
    def poll_interval_seconds(self) -> float:
        return self._poll_interval_seconds

    @property
    def max_post_attempts(self) -> int:
        return self._max_post_attempts

    @property
    def post_retry_delay_seconds(self) -> float:
        return self._post_retry_delay_seconds

    @property
    def sleep_func(self) -> Callable[[float], None]:
        return self._sleep_func


DEFAULT_PROGRESS_REPORTER_ASPECTS = ProgressReporterAspects()


def _now_rfc3339() -> str:
    """
        Produces the current UTC time as an RFC 3339 string.

        :returns: An RFC 3339 timestamp string.
    """
    rtnval = datetime.now(timezone.utc).isoformat()
    return rtnval


def stream_file_path(output_dir: str) -> str:
    """
        Builds the path of the testbase incremental JSOS stream file for a
        run output directory.

        :param output_dir: The testbase run output directory.

        :returns: The absolute path of ``testrun_results_stream.jsos``.
    """
    rtnval = os.path.join(output_dir, TESTBASE_RESULTS_STREAM_FILENAME)
    return rtnval


def parse_stream_records(stream_path: str) -> List[Dict[str, Any]]:
    """
        Parses a testbase ``testrun_results_stream.jsos`` file into the list
        of test-typed JSOS records it currently contains.

        The reporter reuses testbase's own :mod:`jsosparser` so the JSOS
        record-separator handling is not re-implemented here. A stream file
        that does not yet exist (the run has not produced output) yields an
        empty list rather than an error.

        :param stream_path: Path to the ``testrun_results_stream.jsos`` file.

        :returns: A list of test-typed JSOS record dicts (possibly empty).
    """
    rtnval: List[Dict[str, Any]] = []

    if not os.path.isfile(stream_path):
        return rtnval

    try:
        entries = jsosparser.parse_file(stream_path)
    except ValueError as parseerr:
        # An incremental file may be observed mid-write with a partial final
        # record; that surfaces as a ValueError from the parser. Progress is
        # best-effort, so a partial parse is logged and skipped this cycle.
        logger.debug(
            "pychestrate-client: progress stream parse skipped (partial "
            "write?) for %s: %s", stream_path, parseerr,
        )
        return rtnval

    test_entries = jsosparser.filter_tests(entries)
    for entry in test_entries:
        rtnval.append(entry)

    return rtnval


def translate_record(*, run_id: str, client_id: str, record_index: int,
                      jsos_entry: Dict[str, Any]) -> Progressrecord:
    """
        Translates a single testbase JSOS test record into the
        ``ProgressRecord`` OpenAPI schema object.

        :param run_id: The server-assigned run UUID.
        :param client_id: The assigned client UUID.
        :param record_index: The zero-based ordinal of this record within the
                              run's progress stream.
        :param jsos_entry: One test-typed JSOS record dict.

        :returns: A populated :class:`Progressrecord`.
    """
    test_name = str(jsos_entry.get("name", ""))
    result_code = str(jsos_entry.get("result", ""))

    started_at = jsos_entry.get("start", None)
    if started_at is not None and len(str(started_at)) == 0:
        started_at = None

    finished_at = jsos_entry.get("stop", None)
    if finished_at is not None and len(str(finished_at)) == 0:
        finished_at = None

    record = Progressrecord(
        run_id=run_id,
        client_id=client_id,
        record_index=record_index,
        test_name=test_name,
        result=result_code,
        raw=dict(jsos_entry),
    )
    if started_at is not None:
        record.started_at = str(started_at)
    if finished_at is not None:
        record.finished_at = str(finished_at)

    return record


def _tally(jsos_entries: List[Dict[str, Any]]) -> Dict[str, int]:
    """
        Tallies pass / fail / skip / error counts across a set of testbase
        JSOS test records.

        :param jsos_entries: The full list of test-typed JSOS records.

        :returns: A dict with ``passed``, ``failed``, ``skipped``,
                  ``errored`` and ``total`` integer counts.
    """
    passed = 0
    failed = 0
    skipped = 0
    errored = 0

    for entry in jsos_entries:
        result_code = str(entry.get("result", ""))
        if result_code == _RESULT_PASSED:
            passed += 1
        elif result_code == _RESULT_FAILED:
            failed += 1
        elif result_code == _RESULT_SKIPPED:
            skipped += 1
        elif result_code == _RESULT_ERRORED:
            errored += 1

    total = len(jsos_entries)
    rtnval = {
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "errored": errored,
        "total": total,
    }
    return rtnval


def build_progress_summary(*, run_id: str, client_id: str,
                            jsos_entries: List[Dict[str, Any]]
                            ) -> Progresssummary:
    """
        Builds a ``ProgressSummary`` roll-up from every test record seen so
        far in the run's JSOS stream.

        :param run_id: The server-assigned run UUID.
        :param client_id: The assigned client UUID.
        :param jsos_entries: The full list of test-typed JSOS records.

        :returns: A populated :class:`Progresssummary`.
    """
    counts = _tally(jsos_entries=jsos_entries)

    summary = Progresssummary(
        run_id=run_id,
        client_id=client_id,
        format_tag=PROGRESS_FORMAT_TAG,
        tests_passed=counts["passed"],
        tests_failed=counts["failed"],
        tests_skipped=counts["skipped"],
        tests_errored=counts["errored"],
        recorded_at=_now_rfc3339(),
        raw={"counts": counts},
    )
    summary.tests_total = counts["total"]

    return summary


def build_progress_frame(*, run_id: str, client_id: str,
                          all_entries: List[Dict[str, Any]],
                          new_entries: List[Dict[str, Any]],
                          first_new_index: int) -> Progressframe:
    """
        Builds a ``ProgressFrame`` carrying a summary roll-up plus the batch
        of per-test records that appeared since the previous poll.

        :param run_id: The server-assigned run UUID.
        :param client_id: The assigned client UUID.
        :param all_entries: Every test-typed JSOS record seen so far (used
                             for the summary roll-up).
        :param new_entries: The test-typed JSOS records that appeared since
                            the previous poll (carried as the record batch).
        :param first_new_index: The progress-stream ordinal of the first
                                 record in ``new_entries``.

        :returns: A populated :class:`Progressframe`.
    """
    summary = build_progress_summary(
        run_id=run_id, client_id=client_id, jsos_entries=all_entries)

    records: List[Progressrecord] = []
    cursor = first_new_index
    for entry in new_entries:
        record = translate_record(
            run_id=run_id, client_id=client_id, record_index=cursor,
            jsos_entry=entry)
        records.append(record)
        cursor += 1

    frame = Progressframe(summary=summary, records=records)
    return frame


class GeneralizedProgressFrame(Progressframe):
    """
        The :class:`GeneralizedProgressFrame` is the ``frame_kind`` !=
        ``test`` body of ``POST /api/v1/runs/{run_id}/progress``.

        The service's progress route discriminates on ``frame_kind``: a
        ``test`` frame carries the test-shaped ``summary`` / ``records``
        pair, while a ``build`` / ``provision`` frame carries a single
        generalized ``frame`` object (phase / status / percent_complete /
        free-form payload). The codegen'd :class:`Progressframe` was
        generated before the service grew the generalized shape and so
        declares only ``summary`` and ``records``.

        Rather than hand-roll an HTTP call for the newer body -- which the
        repository forbids -- the generalized body is expressed as a
        :class:`Progressframe` subclass declaring the generalized fields.
        It therefore travels through the same codegen'd
        ``progress.progress_post_frame`` operation and satisfies that
        operation's body type. Once ``codecraftai-restclient-pychestrate`` is
        regenerated against the current OpenAPI document, this class is
        replaced by the generated model.
    """

    frame_kind = Field(str, required=True)
    frame = Field(Dict[str, Any], required=True)


def build_generalized_frame(*, run_id: str, client_id: str, frame_kind: str,
                             frame_index: int, phase: str, status: str,
                             percent_complete: Optional[float] = None,
                             notes: Optional[str] = None,
                             payload: Optional[Dict[str, Any]] = None,
                             recorded_at: Optional[str] = None
                             ) -> GeneralizedProgressFrame:
    """
        Builds the generalized (non test-shaped) progress frame a ``build``
        or ``provision`` run posts at a lifecycle transition.

        ``percent_complete`` is optional on purpose. A caller passes it only
        when the executor genuinely knows the completion ratio -- an omitted
        percentage reads as "unknown" on the operator's live view, while a
        fabricated one reads as fact.

        :param run_id: The server-assigned run UUID.
        :param client_id: The assigned client UUID.
        :param frame_kind: Either ``build`` or ``provision``.
        :param frame_index: The monotonic ordinal of this frame within the
                            run's frame stream. The service is idempotent on
                            ``(run_id, frame_kind, frame_index)``, so
                            re-posting a frame that carries the same index is
                            a no-op rather than a duplicate.
        :param phase: The phase / step the run entered.
        :param status: The status of that phase (``running`` /
                       ``succeeded`` / ``failed``).
        :param percent_complete: Optional completion percentage; omitted from
                                 the frame when None.
        :param notes: Optional free-form note; omitted when None.
        :param payload: Optional kind-specific passthrough entries merged
                        into the frame.
        :param recorded_at: Optional RFC 3339 timestamp; defaults to now.

        :returns: A populated :class:`GeneralizedProgressFrame`.
        :raises: BadParameterError
    """
    if frame_kind not in GENERALIZED_FRAME_KINDS:
        raise BadParameterError(
            "build_generalized_frame only mints '{}' frames.".format(
                ", ".join(GENERALIZED_FRAME_KINDS)),
            "frame_kind", "Got '{}'.".format(frame_kind))

    recorded_at_resolved = recorded_at
    if recorded_at_resolved is None:
        recorded_at_resolved = _now_rfc3339()

    frame_body: Dict[str, Any] = {}
    if payload is not None:
        for payload_key in payload:
            frame_body[payload_key] = payload[payload_key]

    frame_body["run_id"] = run_id
    frame_body["client_id"] = client_id
    frame_body["frame_index"] = frame_index
    frame_body["phase"] = phase
    frame_body["status"] = status
    frame_body["recorded_at"] = recorded_at_resolved

    if percent_complete is not None:
        frame_body["percent_complete"] = percent_complete

    if notes is not None:
        frame_body["notes"] = notes

    rtnval = GeneralizedProgressFrame(
        frame_kind=frame_kind, frame=frame_body)
    return rtnval


def post_progress_frame(*, client: PychestrateMtlsClient, run_id: str,
                         frame: Progressframe) -> bool:
    """
        Posts one ``ProgressFrame`` via the codegen'd
        ``progress.progress_post_frame`` method.

        A transport failure, an HTTP error, or any other exception is logged
        and reported through the return value -- it must NOT abort the run,
        and it must NOT be mistaken for an accepted frame. Callers decide
        what an unaccepted frame means; they must not advance a forwarding
        cursor past it. The endpoint answers HTTP 202 with an empty body, so
        an empty-body JSON decode failure following a non-raising
        ``perform_call`` is treated as success.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param run_id: The server-assigned run UUID.
        :param frame: The :class:`Progressframe` (test-shaped) or
                      :class:`GeneralizedProgressFrame` to post.

        :returns: True when the frame was accepted; False when the POST
                  failed (logged, run continues).
    """
    try:
        client.progress.progress_post_frame(run_id=run_id, body=frame)
    except ValueError:
        # An expected HTTP 202 with an empty body decodes as a ValueError.
        logger.debug(
            "pychestrate-client: progress frame 202 for run_id=%s.", run_id,
        )
        rtnval = True
        return rtnval
    except Exception as posterr:
        logger.warning(
            "pychestrate-client: progress frame POST failed for run_id=%s "
            "(best-effort, run continues): %s", run_id, posterr,
        )
        rtnval = False
        return rtnval

    rtnval = True
    return rtnval


def post_progress_frame_with_retry(*, client: PychestrateMtlsClient,
                                    run_id: str, frame: Progressframe,
                                    aspects: ProgressReporterAspects =
                                    DEFAULT_PROGRESS_REPORTER_ASPECTS
                                    ) -> bool:
    """
        Posts one progress frame, retrying a failed POST up to the aspects'
        attempt budget.

        The retry is bounded (``aspects.max_post_attempts``) so a service
        outage delays a poll cycle by a known amount instead of stalling the
        run, and it is non-duplicating: every attempt posts the identical
        frame, and the service is idempotent on the record / frame indexes
        the frame carries.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param run_id: The server-assigned run UUID.
        :param frame: The frame to post.
        :param aspects: Behavior-shaping aspects supplying the attempt
                        budget, the inter-attempt delay, and the sleep
                        function.

        :returns: True when the frame was accepted on some attempt; False
                  when every attempt failed.
    """
    max_attempts = aspects.max_post_attempts
    retry_delay = aspects.post_retry_delay_seconds
    sleep_func = aspects.sleep_func

    accepted = False
    attempt = 1

    while attempt <= max_attempts:
        accepted = post_progress_frame(
            client=client, run_id=run_id, frame=frame)
        if accepted:
            break

        if attempt < max_attempts:
            logger.debug(
                "pychestrate-client: progress frame POST attempt %d of %d "
                "failed for run_id=%s, retrying in %.2fs.",
                attempt, max_attempts, run_id, retry_delay,
            )
            sleep_func(retry_delay)

        attempt += 1

    if not accepted:
        logger.warning(
            "pychestrate-client: progress frame not accepted for run_id=%s "
            "after %d attempt(s); it stays pending and is re-sent unchanged "
            "on the next cycle.", run_id, max_attempts,
        )

    rtnval = accepted
    return rtnval


def flush_progress_frames(*, client: PychestrateMtlsClient, run_id: str,
                           frames: List[Progressframe],
                           aspects: ProgressReporterAspects =
                           DEFAULT_PROGRESS_REPORTER_ASPECTS
                           ) -> List[Progressframe]:
    """
        Posts a list of pending progress frames in order, stopping at the
        first frame the service does not accept.

        Stopping preserves the frames' order on the service side and keeps
        the undelivered tail intact for the caller to retry. Because every
        frame carries its own stable index, a retried frame is deduped by
        the service rather than duplicated.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param run_id: The server-assigned run UUID.
        :param frames: The pending frames, oldest first.
        :param aspects: Behavior-shaping aspects.

        :returns: The frames that were NOT accepted (oldest first); an empty
                  list when every frame landed.
    """
    rtnval: List[Progressframe] = []

    for position, frame in enumerate(frames):
        accepted = post_progress_frame_with_retry(
            client=client, run_id=run_id, frame=frame, aspects=aspects)
        if not accepted:
            rtnval = list(frames[position:])
            break

    return rtnval


class ProgressPollOutcome:
    """
        The :class:`ProgressPollOutcome` carries what a single progress-poll
        cycle did: where the forwarding cursor now stands, whether a frame
        was posted at all, and whether the service accepted it.

        The cursor and the acceptance flag are separate on purpose. A cycle
        that posted nothing and a cycle whose POST failed both leave the
        cursor unmoved, but only the second one means live progress is
        degraded and worth logging.
    """

    def __init__(self, *, cursor: int, attempted: bool, accepted: bool):
        """
            Initializes a :class:`ProgressPollOutcome`.

            :param cursor: The number of records forwarded so far -- the
                           ``already_posted`` value for the next cycle.
            :param attempted: True when the cycle had new records and posted
                              a frame.
            :param accepted: True when the posted frame was accepted.
        """
        self._cursor = cursor
        self._attempted = attempted
        self._accepted = accepted
        return

    @property
    def cursor(self) -> int:
        return self._cursor

    @property
    def attempted(self) -> bool:
        return self._attempted

    @property
    def accepted(self) -> bool:
        return self._accepted


def poll_and_post_progress(*, client: PychestrateMtlsClient, run_id: str,
                           client_id: str, output_dir: str,
                           already_posted: int,
                           aspects: ProgressReporterAspects =
                           DEFAULT_PROGRESS_REPORTER_ASPECTS
                           ) -> ProgressPollOutcome:
    """
        Performs a single progress-poll cycle: re-parses the testbase JSOS
        stream file, and -- when new records have appeared since the previous
        cycle -- builds and posts a ``ProgressFrame``.

        The forwarding cursor advances ONLY for records the service actually
        accepted. When the POST fails the cursor stays put, so the next cycle
        re-sends the same records carrying the same ``record_index`` values;
        the service is idempotent on ``(run_id, record_index)`` and drops the
        re-send instead of double-counting it. Advancing unconditionally
        would drop that batch from the operator's live view forever.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param run_id: The server-assigned run UUID.
        :param client_id: The assigned client UUID.
        :param output_dir: The testbase run output directory.
        :param already_posted: The number of records forwarded in previous
                               cycles.
        :param aspects: Behavior-shaping aspects supplying the post-retry
                        budget.

        :returns: The :class:`ProgressPollOutcome` for the cycle.
    """
    stream_path = stream_file_path(output_dir=output_dir)
    all_entries = parse_stream_records(stream_path=stream_path)

    if len(all_entries) <= already_posted:
        # No records have appeared since the previous cycle.
        rtnval = ProgressPollOutcome(
            cursor=already_posted, attempted=False, accepted=False)
        return rtnval

    new_entries = all_entries[already_posted:]
    frame = build_progress_frame(
        run_id=run_id, client_id=client_id, all_entries=all_entries,
        new_entries=new_entries, first_new_index=already_posted)

    accepted = post_progress_frame_with_retry(
        client=client, run_id=run_id, frame=frame, aspects=aspects)

    cursor = already_posted
    if accepted:
        cursor = len(all_entries)

    rtnval = ProgressPollOutcome(
        cursor=cursor, attempted=True, accepted=accepted)
    return rtnval


class ProgressReporter:
    """
        The :class:`ProgressReporter` drives a background daemon thread that
        tails a running testbase job's ``testrun_results_stream.jsos`` file
        and streams progress frames to the service for the lifetime of the
        run.

        The owning caller (the testbase executor) constructs the reporter,
        calls :meth:`start` immediately before launching the testbase
        subprocess, and calls :meth:`stop` once the subprocess exits. The
        :meth:`stop` call performs one final poll so the last records the
        run wrote are not lost in the gap between the last interval tick and
        process exit.
    """

    def __init__(self, *, client: PychestrateMtlsClient, run_id: str,
                 client_id: str, output_dir: str,
                 aspects: ProgressReporterAspects =
                 DEFAULT_PROGRESS_REPORTER_ASPECTS):
        """
            Initializes a :class:`ProgressReporter`.

            :param client: A configured :class:`PychestrateMtlsClient`.
            :param run_id: The server-assigned run UUID.
            :param client_id: The assigned client UUID.
            :param output_dir: The testbase run output directory the run
                               writes its JSOS stream into.
            :param aspects: Behavior-shaping aspects.
        """
        self._client = client
        self._run_id = run_id
        self._client_id = client_id
        self._output_dir = output_dir
        self._aspects = aspects
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._posted_count = 0
        self._consecutive_failures = 0
        return

    @property
    def posted_count(self) -> int:
        """
            Gets the number of progress records forwarded to the service so
            far.

            :returns: The forwarded-record count.
        """
        return self._posted_count

    @property
    def consecutive_failures(self) -> int:
        """
            Gets the number of consecutive poll cycles whose progress POST
            was not accepted. Resets to zero as soon as a frame lands.

            :returns: The consecutive unaccepted-cycle count.
        """
        return self._consecutive_failures

    def _run_cycle(self) -> None:
        """
            Runs one poll cycle and folds its outcome into the reporter's
            cursor and failure tracking.

            The cursor only moves for records the service accepted, so a
            failed cycle leaves the same records queued for the next one.
            Repeated failures escalate in the log so a prolonged progress
            outage is visible instead of silently spinning.

            :returns: None
        """
        outcome = poll_and_post_progress(
            client=self._client, run_id=self._run_id,
            client_id=self._client_id, output_dir=self._output_dir,
            already_posted=self._posted_count, aspects=self._aspects)

        self._posted_count = outcome.cursor

        if not outcome.attempted:
            return

        if outcome.accepted:
            self._consecutive_failures = 0
            return

        self._consecutive_failures += 1

        if self._consecutive_failures >= CONSECUTIVE_FAILURE_ALERT_THRESHOLD:
            logger.error(
                "pychestrate-client: live progress has been undeliverable "
                "for %d consecutive cycles for run_id=%s; %d record(s) are "
                "confirmed delivered and the undelivered tail is re-sent "
                "unchanged each cycle. The run itself is unaffected.",
                self._consecutive_failures, self._run_id, self._posted_count,
            )
        else:
            logger.warning(
                "pychestrate-client: progress cycle %d for run_id=%s did not "
                "land; the same records are re-sent on the next cycle.",
                self._consecutive_failures, self._run_id,
            )

        return

    def _run_loop(self) -> None:
        """
            The background-thread body: polls the stream file on the
            configured interval until :meth:`stop` is signalled.

            :returns: None
        """
        sleep_func = self._aspects.sleep_func
        interval = self._aspects.poll_interval_seconds

        while not self._stop_event.is_set():
            try:
                self._run_cycle()
            except Exception as cycleerr:
                # Best-effort: a poll-cycle error must not kill the thread.
                logger.warning(
                    "pychestrate-client: progress poll cycle raised an "
                    "error for run_id=%s (run continues): %s",
                    self._run_id, cycleerr,
                )

            if self._stop_event.is_set():
                break

            # The injected ``sleep_func`` paces the loop. Tests supply an
            # override that records the call and returns immediately so the
            # loop can be driven deterministically alongside a stop signal.
            sleep_func(interval)
        return

    def start(self) -> None:
        """
            Starts the background progress-streaming thread.

            :returns: None
        """
        if self._thread is not None:
            logger.debug(
                "pychestrate-client: progress reporter already started for "
                "run_id=%s.", self._run_id,
            )
            return

        self._stop_event.clear()
        thread = threading.Thread(
            target=self._run_loop, name="pychestrate-progress", daemon=True)
        self._thread = thread
        thread.start()
        logger.info(
            "pychestrate-client: progress reporter started for run_id=%s.",
            self._run_id,
        )
        return

    def stop(self) -> int:
        """
            Signals the background thread to stop, joins it, and performs one
            final synchronous poll so the records written between the last
            interval tick and process exit are not lost.

            :returns: The total number of progress records forwarded.
        """
        self._stop_event.set()

        thread = self._thread
        if thread is not None:
            thread.join(timeout=self._aspects.poll_interval_seconds + 5.0)
            self._thread = None

        try:
            self._run_cycle()
        except Exception as finalerr:
            logger.warning(
                "pychestrate-client: final progress poll raised an error "
                "for run_id=%s (best-effort): %s", self._run_id, finalerr,
            )

        if self._consecutive_failures > 0:
            logger.warning(
                "pychestrate-client: progress reporter stopped for run_id=%s "
                "with the last %d cycle(s) undelivered; %d record(s) reached "
                "the service. The authoritative completion signal is the "
                "result upload, not the progress stream.",
                self._run_id, self._consecutive_failures, self._posted_count,
            )
        else:
            logger.info(
                "pychestrate-client: progress reporter stopped for run_id=%s "
                "(forwarded %d records).", self._run_id, self._posted_count,
            )

        rtnval = self._posted_count
        return rtnval

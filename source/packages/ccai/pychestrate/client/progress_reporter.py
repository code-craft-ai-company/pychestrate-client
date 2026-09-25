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

                   Progress is strictly best-effort: a failed progress POST is
                   logged and the run continues. The authoritative signal that
                   a run completed is the result upload + ``results/complete``
                   POST, not the progress stream.

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

from ccai.pychestrate.client.pychestratemtlsclient import PychestrateMtlsClient


logger = logging.getLogger(__name__)

#: Canonical name of the incremental JSOS stream file an ``codecraftai-testbase``
#: run writes into its output directory (see ``ccai.testbase.testjob``).
TESTBASE_RESULTS_STREAM_FILENAME = "testrun_results_stream.jsos"

#: Parser tag the progress ingester uses to select the testbase parser.
PROGRESS_FORMAT_TAG = "testbase"

#: Default seconds between consecutive stream-file polls.
DEFAULT_PROGRESS_POLL_INTERVAL_SECONDS = 5.0

#: testbase JSOS ``result`` codes that count as a non-passing test.
_RESULT_FAILED = "FAILED"
_RESULT_ERRORED = "ERRORED"
_RESULT_SKIPPED = "SKIPPED"
_RESULT_PASSED = "PASSED"


class ProgressReporterAspects:
    """
        The :class:`ProgressReporterAspects` packages caller-controlled
        behavior for the progress reporter (the stream-file poll interval
        and the sleep override used by tests).

        Following the project's Aspects pattern, behavior modification is
        carried as data on a single parameter rather than via decorators.
    """

    def __init__(self, *,
                 poll_interval_seconds: float = DEFAULT_PROGRESS_POLL_INTERVAL_SECONDS,
                 sleep_func: Optional[Callable[[float], None]] = None):
        """
            Initializes a :class:`ProgressReporterAspects` instance.

            :param poll_interval_seconds: Seconds the reporter waits between
                                          consecutive polls of the testbase
                                          ``testrun_results_stream.jsos``
                                          file.
            :param sleep_func: Optional sleep override; defaults to
                               :func:`time.sleep`. Injected for testability.

            :raises: BadParameterError
        """
        if poll_interval_seconds <= 0:
            raise BadParameterError(
                "ProgressReporterAspects requires 'poll_interval_seconds' > 0.",
                "poll_interval_seconds", "Pass a positive number of seconds.")

        sleep_resolved = sleep_func
        if sleep_resolved is None:
            import time as _time

            sleep_resolved = _time.sleep

        self._poll_interval_seconds = poll_interval_seconds
        self._sleep_func = sleep_resolved
        return

    @property
    def poll_interval_seconds(self) -> float:
        return self._poll_interval_seconds

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


def post_progress_frame(*, client: PychestrateMtlsClient, run_id: str,
                         frame: Progressframe) -> bool:
    """
        Posts one ``ProgressFrame`` via the codegen'd
        ``progress.progress_post_frame`` method.

        Progress is best-effort. A transport failure, an HTTP error, or any
        other exception is logged and swallowed -- it must NOT abort the
        run. The endpoint answers HTTP 202 with an empty body, so an
        empty-body JSON decode failure following a non-raising
        ``perform_call`` is treated as success.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param run_id: The server-assigned run UUID.
        :param frame: The :class:`Progressframe` to post.

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


def poll_and_post_progress(*, client: PychestrateMtlsClient, run_id: str,
                           client_id: str, output_dir: str,
                           already_posted: int) -> int:
    """
        Performs a single progress-poll cycle: re-parses the testbase JSOS
        stream file, and -- when new records have appeared since the previous
        cycle -- builds and posts a ``ProgressFrame``.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param run_id: The server-assigned run UUID.
        :param client_id: The assigned client UUID.
        :param output_dir: The testbase run output directory.
        :param already_posted: The number of records forwarded in previous
                               cycles.

        :returns: The new total number of records forwarded (the cursor for
                  the next cycle).
    """
    stream_path = stream_file_path(output_dir=output_dir)
    all_entries = parse_stream_records(stream_path=stream_path)

    if len(all_entries) <= already_posted:
        # No records have appeared since the previous cycle.
        rtnval = already_posted
        return rtnval

    new_entries = all_entries[already_posted:]
    frame = build_progress_frame(
        run_id=run_id, client_id=client_id, all_entries=all_entries,
        new_entries=new_entries, first_new_index=already_posted)

    post_progress_frame(client=client, run_id=run_id, frame=frame)

    rtnval = len(all_entries)
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
        return

    @property
    def posted_count(self) -> int:
        """
            Gets the number of progress records forwarded to the service so
            far.

            :returns: The forwarded-record count.
        """
        return self._posted_count

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
                self._posted_count = poll_and_post_progress(
                    client=self._client, run_id=self._run_id,
                    client_id=self._client_id, output_dir=self._output_dir,
                    already_posted=self._posted_count)
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
            self._posted_count = poll_and_post_progress(
                client=self._client, run_id=self._run_id,
                client_id=self._client_id, output_dir=self._output_dir,
                already_posted=self._posted_count)
        except Exception as finalerr:
            logger.warning(
                "pychestrate-client: final progress poll raised an error "
                "for run_id=%s (best-effort): %s", self._run_id, finalerr,
            )

        logger.info(
            "pychestrate-client: progress reporter stopped for run_id=%s "
            "(forwarded %d records).", self._run_id, self._posted_count,
        )

        rtnval = self._posted_count
        return rtnval

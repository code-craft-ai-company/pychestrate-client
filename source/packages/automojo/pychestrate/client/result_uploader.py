"""
    .. module:: result_uploader
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Walks a finished ``automojo-testbase`` result tree, uploads
                   it to the shared results backend, and posts the
                   ``ResultManifest`` that signals run completion to the
                   Pychestrate service.

                   M3 scope: once :mod:`testbase_executor` finishes a job, the
                   client owns a result-tree directory under
                   ``<state>/runs/<run_id>/output``. The uploader walks the
                   testbase result tree (``testrun_summary.json``,
                   ``testrun_results_stream.jsos``, ``testrun_results.json``,
                   ``testsummary.html`` plus the ``artifacts/``, ``taskings/``,
                   ``tc-by-products/`` and ``diagnostics/`` subtrees), uploads
                   it to the shared results object store under the per-run
                   prefix the signed envelope carries in its
                   ``JobResultsLocator`` (``backend_key_prefix`` +
                   ``backend_driver``), and then POSTs the ``ResultManifest``
                   to ``POST /api/v1/runs/{run_id}/results/complete``.

                   Upload mechanism / server gap -- per the
                   ``5-shared-results-backend`` design the client is expected
                   to write the result tree DIRECTLY to the shared object
                   store: "The client receives ``backend_key_prefix`` and
                   ``backend_driver`` inside the signed envelope; it uses its
                   own object-store credentials to write." There is NO
                   per-artifact HTTP upload endpoint -- the codegen'd
                   ``artifacts`` REST group exposes GET endpoints only. Two
                   pieces the design references are NOT yet delivered to the
                   client: (1) the envelope's ``JobResultsLocator`` carries
                   only ``backend_key_prefix`` + ``backend_driver`` and no
                   object-store endpoint / credentials, and (2) the
                   ``presign_put`` path on ``SharedResultsBackend`` is an
                   internal API with no HTTP surface. The uploader therefore
                   abstracts the byte transfer behind an injectable
                   :class:`ResultStoreDriver` (Aspects pattern): a site that
                   provisions an object-store driver out-of-band wires it in;
                   when no driver is wired the tree-walk + manifest are still
                   produced, the artifact bytes are simply not transferred,
                   and the local run record is marked ``upload_failed`` so the
                   next ``run`` invocation can retry. The
                   ``results/complete`` POST itself is fully implemented.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2025, Automation Mojo LLC"
__credits__ = []


from typing import Any, Dict, List, Optional

import hashlib
import json
import logging
import os

from datetime import datetime, timezone

from automojo.pychestrate.rest.client.models.results import Resultmanifest

from automojo.pychestrate.client.pychestratemtlsclient import PychestrateMtlsClient
from automojo.pychestrate.client.state import ClientState


logger = logging.getLogger(__name__)

#: Parser tag the result ingester uses to select the testbase result-tree
#: parser chain.
RESULT_FORMAT_TAG = "testbase"

#: Canonical top-level files an ``automojo-testbase`` result tree contains.
TESTBASE_SUMMARY_FILENAME = "testrun_summary.json"
TESTBASE_RESULTS_STREAM_FILENAME = "testrun_results_stream.jsos"
TESTBASE_RESULTS_FILENAME = "testrun_results.json"
TESTBASE_HTML_SUMMARY_FILENAME = "testsummary.html"

#: ``TerminalOutcome`` enum values from the OpenAPI spec.
TERMINAL_OUTCOME_SUCCESS = "success"
TERMINAL_OUTCOME_FAILURE = "failure"

#: Local run-record upload-status markers.
UPLOAD_STATUS_COMPLETE = "upload_complete"
UPLOAD_STATUS_FAILED = "upload_failed"


class ResultStoreDriver:
    """
        The :class:`ResultStoreDriver` is the seam through which the result
        uploader transfers result-tree bytes to the shared results object
        store.

        It is intentionally abstract: the ``5-shared-results-backend`` design
        has the client write directly to the object store with its own
        credentials, but the concrete object-store binding is provisioned at
        the site / pod level and is not delivered through the Pychestrate
        REST surface. A site supplies a concrete driver via
        :class:`ResultUploaderAspects`; the uploader never hand-rolls a
        storage protocol.
    """

    def put_object(self, *, backend_driver: str, backend_key_prefix: str,
                   relative_path: str, local_path: str) -> None:
        """
            Transfers one local file to the shared object store under
            ``<backend_key_prefix>/<relative_path>``.

            :param backend_driver: The object-store driver name the envelope
                                   declared.
            :param backend_key_prefix: The per-run key prefix the envelope
                                       declared.
            :param relative_path: The slash-delimited path of the file
                                   relative to the result-tree root.
            :param local_path: The absolute path of the local file to
                               transfer.

            :returns: None
            :raises: NotImplementedError
        """
        errmsg_lines = [
            "No concrete ResultStoreDriver is wired into the "
            "pychestrate-client result uploader.",
            "    backend_driver: {}".format(backend_driver),
            "    backend_key_prefix: {}".format(backend_key_prefix),
            "    relative_path: {}".format(relative_path),
            "The shared-results object store is provisioned at the site / "
            "pod level; pass a concrete driver via ResultUploaderAspects.",
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise NotImplementedError(errmsg)


class ResultUploaderAspects:
    """
        The :class:`ResultUploaderAspects` packages caller-controlled
        behavior for the result uploader (the object-store driver used to
        transfer the result tree).

        Following the project's Aspects pattern, behavior modification is
        carried as data on a single parameter rather than via decorators.
    """

    def __init__(self, *, store_driver: Optional[ResultStoreDriver] = None):
        """
            Initializes a :class:`ResultUploaderAspects` instance.

            :param store_driver: The :class:`ResultStoreDriver` used to
                                 transfer result-tree bytes to the shared
                                 object store. ``None`` leaves no driver
                                 wired -- the tree-walk and manifest are
                                 still produced and the local run record is
                                 marked ``upload_failed`` for retry.
        """
        self._store_driver = store_driver
        return

    @property
    def store_driver(self) -> Optional[ResultStoreDriver]:
        return self._store_driver


DEFAULT_RESULT_UPLOADER_ASPECTS = ResultUploaderAspects()


def _now_rfc3339() -> str:
    """
        Produces the current UTC time as an RFC 3339 string.

        :returns: An RFC 3339 timestamp string.
    """
    rtnval = datetime.now(timezone.utc).isoformat()
    return rtnval


def _sha256_of_file(file_path: str) -> str:
    """
        Computes the hex SHA-256 digest of a file.

        :param file_path: The absolute path of the file.

        :returns: The lowercase hex digest string.
    """
    hasher = hashlib.sha256()
    with open(file_path, "rb") as digf:
        while True:
            chunk = digf.read(65536)
            if len(chunk) == 0:
                break
            hasher.update(chunk)

    rtnval = hasher.hexdigest()
    return rtnval


class ResultTreeEntry:
    """
        The :class:`ResultTreeEntry` describes one file discovered while
        walking a testbase result tree.
    """

    def __init__(self, *, relative_path: str, absolute_path: str,
                 size_bytes: int, sha256: str):
        """
            Initializes a :class:`ResultTreeEntry`.

            :param relative_path: Slash-delimited path relative to the
                                  result-tree root.
            :param absolute_path: The absolute path of the file on disk.
            :param size_bytes: The file size in bytes.
            :param sha256: The hex SHA-256 digest of the file.
        """
        self._relative_path = relative_path
        self._absolute_path = absolute_path
        self._size_bytes = size_bytes
        self._sha256 = sha256
        return

    @property
    def relative_path(self) -> str:
        return self._relative_path

    @property
    def absolute_path(self) -> str:
        return self._absolute_path

    @property
    def size_bytes(self) -> int:
        return self._size_bytes

    @property
    def sha256(self) -> str:
        return self._sha256

    def as_dict(self) -> Dict[str, Any]:
        """
            Returns a JSON-serializable dictionary representation.

            :returns: A dict describing the tree entry.
        """
        rtnval = {
            "relative_path": self._relative_path,
            "size_bytes": self._size_bytes,
            "sha256": self._sha256,
        }
        return rtnval


def walk_result_tree(result_dir: str) -> List[ResultTreeEntry]:
    """
        Walks a testbase result-tree directory and returns the list of files
        it contains, each with its size and SHA-256 digest.

        Paths are normalized to forward-slash form so they are portable
        across pod operating systems. A result directory that does not exist
        yields an empty list rather than an error.

        :param result_dir: The absolute path of the testbase result-tree
                            root directory.

        :returns: A list of :class:`ResultTreeEntry`, sorted by relative
                  path for deterministic ordering.
    """
    rtnval: List[ResultTreeEntry] = []

    if not os.path.isdir(result_dir):
        logger.warning(
            "pychestrate-client: result tree directory does not exist: %s",
            result_dir,
        )
        return rtnval

    for current_dir, _subdirs, filenames in os.walk(result_dir):
        for filename in filenames:
            absolute_path = os.path.join(current_dir, filename)
            if not os.path.isfile(absolute_path):
                continue

            relative_native = os.path.relpath(absolute_path, result_dir)
            relative_path = relative_native.replace(os.sep, "/")

            size_bytes = os.path.getsize(absolute_path)
            digest = _sha256_of_file(file_path=absolute_path)

            entry = ResultTreeEntry(
                relative_path=relative_path, absolute_path=absolute_path,
                size_bytes=size_bytes, sha256=digest)
            rtnval.append(entry)

    rtnval.sort(key=lambda treeentry: treeentry.relative_path)
    return rtnval


def _read_summary_counts(result_dir: str) -> Dict[str, int]:
    """
        Reads the four test counters from a testbase ``testrun_summary.json``
        file.

        The testbase summary places final counts under a ``detail`` map
        (``errors`` / ``failed`` / ``skipped`` / ``passed``). A missing or
        unparseable summary yields all-zero counts -- the manifest is still
        produced; the server's result-tree parser is the authority on the
        full result set.

        :param result_dir: The result-tree root directory.

        :returns: A dict with ``passed`` / ``failed`` / ``skipped`` /
                  ``errored`` integer counts.
    """
    rtnval = {"passed": 0, "failed": 0, "skipped": 0, "errored": 0}

    summary_path = os.path.join(result_dir, TESTBASE_SUMMARY_FILENAME)
    if not os.path.isfile(summary_path):
        logger.debug(
            "pychestrate-client: no %s in result tree %s.",
            TESTBASE_SUMMARY_FILENAME, result_dir,
        )
        return rtnval

    try:
        with open(summary_path, "r", encoding="utf-8") as sumf:
            summary = json.load(sumf)
    except (ValueError, OSError) as readerr:
        logger.warning(
            "pychestrate-client: could not read %s: %s",
            summary_path, readerr,
        )
        return rtnval

    detail = summary.get("detail", None)
    if not isinstance(detail, dict):
        detail = {}

    rtnval["passed"] = int(detail.get("passed", 0))
    rtnval["failed"] = int(detail.get("failed", 0))
    rtnval["skipped"] = int(detail.get("skipped", 0))
    rtnval["errored"] = int(detail.get("errors", 0))

    return rtnval


def _extract_results_locator(envelope: Dict[str, Any]) -> Dict[str, str]:
    """
        Pulls the shared-results object-store locator out of a verified job
        envelope.

        :param envelope: The verified envelope dict.

        :returns: A dict with ``backend_key_prefix`` and ``backend_driver``
                  string values (possibly empty when the envelope omits
                  them).
    """
    results = envelope.get("results", {})
    if not isinstance(results, dict):
        results = {}

    backend_key_prefix = str(results.get("backend_key_prefix", ""))
    backend_driver = str(results.get("backend_driver", ""))

    rtnval = {
        "backend_key_prefix": backend_key_prefix,
        "backend_driver": backend_driver,
    }
    return rtnval


def upload_result_tree(*, entries: List[ResultTreeEntry], backend_driver: str,
                        backend_key_prefix: str,
                        store_driver: Optional[ResultStoreDriver]) -> bool:
    """
        Transfers every file in a walked result tree to the shared results
        object store via the supplied :class:`ResultStoreDriver`.

        When no driver is wired the transfer is skipped and ``False`` is
        returned so the caller can mark the run ``upload_failed`` and retry.
        A driver that raises on any single file is logged and also yields a
        ``False`` (failed) result -- partial uploads are not treated as
        success.

        :param entries: The walked result-tree entries.
        :param backend_driver: The object-store driver name from the
                               envelope's ``JobResultsLocator``.
        :param backend_key_prefix: The per-run key prefix from the envelope's
                                   ``JobResultsLocator``.
        :param store_driver: The :class:`ResultStoreDriver` to transfer with,
                             or ``None`` when none is wired.

        :returns: True when every file transferred; False when no driver is
                  wired or any transfer failed.
    """
    if store_driver is None:
        logger.warning(
            "pychestrate-client: no ResultStoreDriver wired; result-tree "
            "byte transfer skipped (%d files) for prefix %s. The run will "
            "be marked upload_failed for retry.",
            len(entries), backend_key_prefix,
        )
        rtnval = False
        return rtnval

    transferred = 0
    for entry in entries:
        try:
            store_driver.put_object(
                backend_driver=backend_driver,
                backend_key_prefix=backend_key_prefix,
                relative_path=entry.relative_path,
                local_path=entry.absolute_path)
            transferred += 1
        except Exception as puterr:
            logger.error(
                "pychestrate-client: result-tree upload failed at %s "
                "(prefix=%s): %s",
                entry.relative_path, backend_key_prefix, puterr,
            )
            rtnval = False
            return rtnval

    logger.info(
        "pychestrate-client: uploaded %d result-tree files to prefix %s.",
        transferred, backend_key_prefix,
    )
    rtnval = True
    return rtnval


def build_result_manifest(*, run_id: str, client_id: str,
                           terminal_outcome: str, backend_driver: str,
                           backend_key_prefix: str,
                           counts: Dict[str, int],
                           entries: List[ResultTreeEntry],
                           notes: Optional[str] = None) -> Resultmanifest:
    """
        Builds the ``ResultManifest`` describing an uploaded testbase result
        tree.

        :param run_id: The server-assigned run UUID.
        :param client_id: The assigned client UUID.
        :param terminal_outcome: A ``TerminalOutcome`` enum value
                                 (``success`` / ``failure``).
        :param backend_driver: The object-store driver name from the
                               envelope's ``JobResultsLocator``.
        :param backend_key_prefix: The per-run key prefix from the envelope's
                                   ``JobResultsLocator``.
        :param counts: The pass/fail/skip/error counts from the testbase
                       summary.
        :param entries: The walked result-tree entries (carried as an
                        ``extras`` index for server-side reconciliation).
        :param notes: Optional free-form note.

        :returns: A populated :class:`Resultmanifest`.
    """
    file_index = [entry.as_dict() for entry in entries]

    manifest = Resultmanifest(
        run_id=run_id,
        client_id=client_id,
        format_tag=RESULT_FORMAT_TAG,
        backend_key_prefix=backend_key_prefix,
        completed_at=_now_rfc3339(),
        terminal_outcome=terminal_outcome,
    )
    manifest.backend_driver = backend_driver
    manifest.summary_artifact_path = TESTBASE_SUMMARY_FILENAME
    manifest.results_stream_artifact_path = TESTBASE_RESULTS_STREAM_FILENAME
    manifest.results_artifact_path = TESTBASE_RESULTS_FILENAME
    manifest.html_summary_artifact_path = TESTBASE_HTML_SUMMARY_FILENAME
    manifest.tests_passed = counts["passed"]
    manifest.tests_failed = counts["failed"]
    manifest.tests_skipped = counts["skipped"]
    manifest.tests_errored = counts["errored"]
    manifest.extras = {
        "file_count": len(entries),
        "files": file_index,
    }
    if notes is not None and len(notes) > 0:
        manifest.notes = notes

    return manifest


def post_results_complete(*, client: PychestrateMtlsClient, run_id: str,
                          manifest: Resultmanifest) -> None:
    """
        Posts a ``ResultManifest`` via the codegen'd
        ``results.results_post_complete`` method.

        The endpoint answers HTTP 202 with an empty body, so an empty-body
        JSON decode failure following a non-raising ``perform_call`` is
        treated as acceptance. Any other exception is allowed to bubble so
        the caller can mark the run ``upload_failed`` and retry.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param run_id: The server-assigned run UUID.
        :param manifest: The :class:`Resultmanifest` to post.

        :returns: None
        :raises: requests.RequestException, automojo HTTP error types
    """
    try:
        client.results.results_post_complete(run_id=run_id, body=manifest)
    except ValueError:
        # An expected HTTP 202 with an empty body decodes as a ValueError.
        logger.debug(
            "pychestrate-client: results/complete 202 for run_id=%s.", run_id,
        )
    return


class ResultUploadOutcome:
    """
        The :class:`ResultUploadOutcome` captures the outcome of a single
        result-tree upload + completion post.
    """

    def __init__(self, *, status: str, manifest_posted: bool,
                 tree_uploaded: bool, file_count: int,
                 detail: Optional[str] = None):
        """
            Initializes a :class:`ResultUploadOutcome`.

            :param status: ``UPLOAD_STATUS_COMPLETE`` or
                           ``UPLOAD_STATUS_FAILED``.
            :param manifest_posted: True when ``results/complete`` was
                                    accepted.
            :param tree_uploaded: True when every result-tree file
                                  transferred.
            :param file_count: The number of files in the walked tree.
            :param detail: Optional human-readable detail for logging.
        """
        self._status = status
        self._manifest_posted = manifest_posted
        self._tree_uploaded = tree_uploaded
        self._file_count = file_count
        self._detail = detail
        return

    @property
    def status(self) -> str:
        return self._status

    @property
    def succeeded(self) -> bool:
        """
            Gets whether the upload completed successfully.

            :returns: True when ``status`` is ``UPLOAD_STATUS_COMPLETE``.
        """
        rtnval = self._status == UPLOAD_STATUS_COMPLETE
        return rtnval

    @property
    def manifest_posted(self) -> bool:
        return self._manifest_posted

    @property
    def tree_uploaded(self) -> bool:
        return self._tree_uploaded

    @property
    def file_count(self) -> int:
        return self._file_count

    @property
    def detail(self) -> Optional[str]:
        return self._detail

    def as_dict(self) -> Dict[str, Any]:
        """
            Returns a JSON-serializable dictionary representation.

            :returns: A dict describing the upload outcome.
        """
        rtnval = {
            "status": self._status,
            "manifest_posted": self._manifest_posted,
            "tree_uploaded": self._tree_uploaded,
            "file_count": self._file_count,
            "detail": self._detail,
        }
        return rtnval


def mark_run_upload_status(state: ClientState, run_id: str,
                           upload_outcome: ResultUploadOutcome) -> str:
    """
        Records the result-upload outcome onto the local run record so a
        failed upload can be retried on the next ``run`` invocation.

        The run-record file is the one :mod:`testbase_executor` wrote; this
        function reads it, merges in an ``upload`` section, and rewrites it.
        A missing run record is created with just the upload section.

        :param state: The :class:`ClientState`.
        :param run_id: The server-assigned run UUID.
        :param upload_outcome: The :class:`ResultUploadOutcome` to record.

        :returns: The absolute path of the run-record file.
    """
    safe_run_id = run_id if len(run_id) > 0 else "unknown-run"
    record_dir = os.path.join(state.runs_dir, safe_run_id)
    os.makedirs(record_dir, mode=0o755, exist_ok=True)

    record_path = os.path.join(record_dir, "run-record.json")

    record: Dict[str, Any] = {}
    if os.path.isfile(record_path):
        try:
            with open(record_path, "r", encoding="utf-8") as recf:
                loaded = json.load(recf)
            if isinstance(loaded, dict):
                record = loaded
        except (ValueError, OSError) as readerr:
            logger.warning(
                "pychestrate-client: could not read run record %s, "
                "rewriting: %s", record_path, readerr,
            )
            record = {}

    record["run_id"] = run_id
    record["upload"] = upload_outcome.as_dict()

    with open(record_path, "w", encoding="utf-8") as recf:
        json.dump(record, recf, indent=4, sort_keys=True)
        recf.write(os.linesep)

    rtnval = record_path
    return rtnval


def run_needs_upload_retry(state: ClientState, run_id: str) -> bool:
    """
        Returns True when a local run record exists whose ``upload`` section
        is marked ``upload_failed`` -- i.e. the run completed locally but its
        results never landed on the service and a retry is owed.

        :param state: The :class:`ClientState`.
        :param run_id: The server-assigned run UUID.

        :returns: True when an upload retry is owed for the run.
    """
    safe_run_id = run_id if len(run_id) > 0 else "unknown-run"
    record_path = os.path.join(state.runs_dir, safe_run_id, "run-record.json")

    if not os.path.isfile(record_path):
        rtnval = False
        return rtnval

    try:
        with open(record_path, "r", encoding="utf-8") as recf:
            record = json.load(recf)
    except (ValueError, OSError):
        rtnval = False
        return rtnval

    if not isinstance(record, dict):
        rtnval = False
        return rtnval

    upload = record.get("upload", None)
    if not isinstance(upload, dict):
        rtnval = False
        return rtnval

    rtnval = upload.get("status", None) == UPLOAD_STATUS_FAILED
    return rtnval


def upload_run_results(*, client: PychestrateMtlsClient, state: ClientState,
                        envelope: Dict[str, Any], result_dir: str,
                        run_succeeded: bool,
                        aspects: ResultUploaderAspects =
                        DEFAULT_RESULT_UPLOADER_ASPECTS
                        ) -> ResultUploadOutcome:
    """
        Walks a finished testbase result tree, uploads it to the shared
        results object store, posts the ``ResultManifest`` to
        ``results/complete``, and records the upload outcome on the local run
        record.

        The function is resilient: a failed byte transfer or a failed
        completion POST is captured as an ``upload_failed``
        :class:`ResultUploadOutcome` (and marked on the local run record for
        retry) rather than raised -- the client process must not crash on a
        bad upload.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param state: The :class:`ClientState`.
        :param envelope: The verified job envelope dict (source of the
                         ``JobResultsLocator``).
        :param result_dir: The absolute path of the testbase result-tree
                           root directory.
        :param run_succeeded: Whether the testbase run itself succeeded
                              (drives the manifest ``terminal_outcome``).
        :param aspects: Behavior-shaping aspects.

        :returns: A :class:`ResultUploadOutcome`.
    """
    run_id = str(envelope.get("run_id", ""))
    client_id = str(envelope.get("client_id", ""))

    locator = _extract_results_locator(envelope=envelope)
    backend_key_prefix = locator["backend_key_prefix"]
    backend_driver = locator["backend_driver"]

    if run_succeeded:
        terminal_outcome = TERMINAL_OUTCOME_SUCCESS
    else:
        terminal_outcome = TERMINAL_OUTCOME_FAILURE

    entries = walk_result_tree(result_dir=result_dir)
    counts = _read_summary_counts(result_dir=result_dir)

    tree_uploaded = upload_result_tree(
        entries=entries, backend_driver=backend_driver,
        backend_key_prefix=backend_key_prefix,
        store_driver=aspects.store_driver)

    notes = None
    if not tree_uploaded:
        notes = ("Result-tree byte transfer incomplete; manifest posted for "
                 "reconciliation. See client-side run record.")

    manifest = build_result_manifest(
        run_id=run_id, client_id=client_id, terminal_outcome=terminal_outcome,
        backend_driver=backend_driver, backend_key_prefix=backend_key_prefix,
        counts=counts, entries=entries, notes=notes)

    manifest_posted = False
    detail: Optional[str] = None
    try:
        post_results_complete(client=client, run_id=run_id, manifest=manifest)
        manifest_posted = True
    except Exception as posterr:
        detail = "results/complete POST failed: {}".format(posterr)
        logger.error("pychestrate-client: %s", detail)

    if tree_uploaded and manifest_posted:
        status = UPLOAD_STATUS_COMPLETE
        if detail is None:
            detail = "Result tree uploaded and results/complete accepted."
    else:
        status = UPLOAD_STATUS_FAILED
        if detail is None:
            detail = ("Result-tree byte transfer did not complete; run "
                      "marked for upload retry.")

    outcome = ResultUploadOutcome(
        status=status, manifest_posted=manifest_posted,
        tree_uploaded=tree_uploaded, file_count=len(entries), detail=detail)

    record_path = mark_run_upload_status(
        state=state, run_id=run_id, upload_outcome=outcome)
    logger.info(
        "pychestrate-client: recorded result-upload outcome (status=%s) at "
        "%s", status, record_path,
    )

    return outcome

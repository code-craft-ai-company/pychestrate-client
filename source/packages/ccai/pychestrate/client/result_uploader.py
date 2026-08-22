"""
    .. module:: result_uploader
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Walks a finished ``codecraftai-testbase`` result tree, uploads
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

                   Upload mechanism (M4) -- the service now exposes
                   ``POST /api/v1/runs/{run_id}/artifacts:presign``, which
                   mints short-lived presigned PUT URLs for the relative
                   artifact paths the client discovered while walking its
                   result tree. The default :class:`PresignedUploadDriver`
                   walks the tree, batch-requests presigned targets, PUTs
                   each artifact's bytes to its ``put_url`` (carrying the
                   target's verbatim ``headers``), and then posts the
                   ``ResultManifest`` to ``results/complete``. Object-store
                   credentials never leave the server -- the credentialed
                   transfer is encoded entirely in the presigned URL.

                   The byte transfer stays abstracted behind an injectable
                   :class:`ResultStoreDriver` (Aspects pattern) so a site
                   that provisions a direct object-store driver can still
                   wire one in; the presigned driver is simply the default.
                   The ``results/complete`` POST is fully implemented.

                   Artifact bytes are streamed. Every artifact is handed to
                   the transport as a :class:`ChunkedFilePayload` -- a
                   bounded chunk iterator that hashes as it reads and
                   refuses to transfer a file whose bytes no longer match
                   the SHA-256 recorded while walking the tree. No artifact
                   is ever fully materialized in memory, so a multi-gigabyte
                   log or screen recording cannot exhaust the client's
                   address space and there is no documented size ceiling.

                   Delivery honesty -- a run whose artifact bytes did NOT
                   land is NEVER reported to the service as complete. The
                   ``ResultManifest`` is posted only after the whole tree
                   transferred; otherwise the run is recorded locally as
                   ``upload_failed`` with a :class:`PendingUpload` retry
                   descriptor and nothing is asserted upstream. This is a
                   deliberate choice: the ``ResultManifest`` schema carries
                   no field for "artifacts partially delivered", and the
                   service's ``results/complete`` ingest advances the run to
                   a terminal state purely from ``terminal_outcome``, so any
                   completion posted for an undelivered tree is a false
                   claim the service cannot detect.

                   Retry -- :func:`retry_pending_uploads` is the public
                   entry point that drains those pending uploads. It MUST be
                   called from the client's periodic loop. Retry counts, the
                   base interval, the backoff factor and the interval ceiling
                   are all caller-controlled through
                   :class:`ResultUploaderAspects`; the schedule is persisted
                   on the local run record, so a pod that restarts mid-backoff
                   resumes on the same schedule instead of stranding the
                   results on disk forever.

                   mTLS -- the ``:presign`` request and (for the
                   local-filesystem fallback) the artifact PUTs flow over
                   the client's mTLS-authenticated session. The presigned
                   PUT may point back at an mTLS-gated local-filesystem
                   upload sink on the service, so the PUT reuses the REST
                   client's authenticated :class:`requests.Session`; for an
                   S3-backed deploy the client certificate is simply
                   ignored by the object store.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2025, Automation Mojo LLC"
__credits__ = []


from typing import Any, Dict, Iterator, List, Optional

import hashlib
import json
import logging
import os

from datetime import datetime, timedelta, timezone

from ccai.errors.exceptions import HTTPRequestError, SemanticError

from ccai.pychestrate.rest.client.models.results import (
    Artifactpresignrequest,
    Resultmanifest,
)

from ccai.pychestrate.client.pychestratemtlsclient import PychestrateMtlsClient
from ccai.pychestrate.client.state import ClientState


logger = logging.getLogger(__name__)

#: Parser tag the result ingester uses to select the testbase result-tree
#: parser chain.
RESULT_FORMAT_TAG = "testbase"

#: Parser tag the result ingester uses to select the build artifact-tree
#: handling. A build artifact tree carries no per-test result records, so
#: a build ``ResultManifest`` leaves the testbase-specific
#: ``summary_artifact_path`` / ``tests_*`` fields unset.
BUILD_RESULT_FORMAT_TAG = "build"

#: Parser tag the result ingester uses to select the provision output-tree
#: handling. A provision output tree carries no per-test result records, so
#: a provision ``ResultManifest`` leaves the testbase-specific
#: ``summary_artifact_path`` / ``tests_*`` fields unset.
PROVISION_RESULT_FORMAT_TAG = "provision"

#: Canonical top-level files an ``codecraftai-testbase`` result tree contains.
TESTBASE_SUMMARY_FILENAME = "testrun_summary.json"
TESTBASE_RESULTS_STREAM_FILENAME = "testrun_results_stream.jsos"
TESTBASE_RESULTS_FILENAME = "testrun_results.json"
TESTBASE_HTML_SUMMARY_FILENAME = "testsummary.html"

#: ``TerminalOutcome`` enum values from the OpenAPI spec. The uploader
#: derives ``success`` / ``failure`` from the run's exit status, but a
#: caller that knows better (the dispatch consumer, which can tell a
#: cancellation from a failure) may pass any of these verbatim. The
#: uploader deliberately does NOT validate an explicitly supplied outcome
#: against this list -- the service's enum is the authority and may grow.
TERMINAL_OUTCOME_SUCCESS = "success"
TERMINAL_OUTCOME_FAILURE = "failure"
TERMINAL_OUTCOME_CANCELLED = "cancelled"
TERMINAL_OUTCOME_DISPATCH_FAILED = "dispatch_failed"
TERMINAL_OUTCOME_DISPATCH_LOST = "dispatch_lost"

#: ``extras`` member carrying the structured reason a run did not succeed
#: (``timeout`` / ``executable_not_found`` / ``nonzero_exit`` /
#: ``cancelled`` / ``launch_error`` / ``setup_error``). The codegen'd
#: ``Resultmanifest`` has no top-level field for this, and ``extras`` is a
#: codegen'd ``Dict[str, Any]``, so it is the structured carrier available
#: without regenerating the REST client.
EXTRAS_KEY_FAILURE_CLASSIFICATION = "failure_classification"

#: Local run-record upload-status markers.
UPLOAD_STATUS_COMPLETE = "upload_complete"
UPLOAD_STATUS_FAILED = "upload_failed"

#: Job-kind discriminators for a pending upload. The kind selects which
#: local record file carries the retry descriptor and which
#: ``ResultManifest`` flavor is posted once the tree lands.
UPLOAD_KIND_TESTBASE = "testbase"
UPLOAD_KIND_BUILD = "build"
UPLOAD_KIND_PROVISION = "provision"

#: Every job kind a pending upload may carry.
UPLOAD_KINDS = (UPLOAD_KIND_TESTBASE, UPLOAD_KIND_BUILD, UPLOAD_KIND_PROVISION)

#: Section names inside a local run / build / provision record file.
RECORD_SECTION_UPLOAD = "upload"
RECORD_SECTION_UPLOAD_RETRY = "upload_retry"

#: Sub-directory (under the client state directory) holding per-build
#: local execution records. Mirrors :mod:`build_executor.DIRNAME_BUILDS`.
DIRNAME_BUILDS = "builds"

#: Sub-directory (under the client state directory) holding per-provision
#: local execution records. Mirrors
#: :mod:`provision_executor.DIRNAME_PROVISIONING`.
DIRNAME_PROVISIONING = "provisioning"

#: Per-kind record filename and fallback run-id, indexed by job kind.
RECORD_FILENAME_BY_KIND = {
    UPLOAD_KIND_TESTBASE: "run-record.json",
    UPLOAD_KIND_BUILD: "build-record.json",
    UPLOAD_KIND_PROVISION: "provision-record.json",
}

#: Placeholder directory name used when a job kind's record must be written
#: for a job whose envelope carried no run id.
FALLBACK_RUN_ID_BY_KIND = {
    UPLOAD_KIND_TESTBASE: "unknown-run",
    UPLOAD_KIND_BUILD: "unknown-build",
    UPLOAD_KIND_PROVISION: "unknown-provision",
}

#: Bytes read per chunk while computing an artifact's SHA-256 digest.
SHA256_READ_CHUNK_SIZE = 65536

#: Bytes handed to the transport per chunk while streaming an artifact.
#: One mebibyte bounds the client's per-artifact memory cost regardless of
#: how large the artifact is.
DEFAULT_ARTIFACT_CHUNK_SIZE = 1024 * 1024

#: Default cap on the number of delivery attempts a single pending upload
#: is given. Zero means "no cap" -- results are retried for as long as the
#: client runs rather than being silently discarded.
DEFAULT_MAX_UPLOAD_ATTEMPTS = 0

#: Default delay before the first retry of a failed upload, in seconds.
DEFAULT_UPLOAD_RETRY_INTERVAL_SECONDS = 30.0

#: Default multiplier applied to the retry delay after each failed attempt.
DEFAULT_UPLOAD_RETRY_BACKOFF_FACTOR = 2.0

#: Default ceiling on the retry delay, in seconds (15 minutes).
DEFAULT_UPLOAD_RETRY_MAX_INTERVAL_SECONDS = 900.0


class ResultStoreDriver:
    """
        The :class:`ResultStoreDriver` is the seam through which the result
        uploader transfers result-tree bytes to the shared results object
        store.

        The default driver wired by :class:`ResultUploaderAspects` is the
        :class:`PresignedUploadDriver`, which obtains presigned PUT URLs
        from ``POST /api/v1/runs/{run_id}/artifacts:presign`` and PUTs the
        bytes directly. A site that provisions its own direct object-store
        binding may instead supply a custom driver: such a driver overrides
        :meth:`put_object` (per-file transfer) and inherits the default
        :meth:`put_tree`, which loops :meth:`put_object` over the tree.

        A driver that transfers the whole tree in a single batch (the
        presigned driver does) overrides :meth:`put_tree` directly.

        Whichever method a driver overrides, it must NOT read an artifact
        fully into memory -- an artifact is an arbitrarily large log, core
        file or screen recording. :class:`ChunkedFilePayload` is the
        supported way to hand a local file to a transport: it is a bounded
        chunk iterator that also reports its length, so ``requests`` sends
        it with a ``Content-Length`` instead of buffering it.
    """

    def put_object(self, *, backend_driver: str, backend_key_prefix: str,
                   relative_path: str, local_path: str) -> None:
        """
            Transfers one local file to the shared object store under
            ``<backend_key_prefix>/<relative_path>``.

            The base implementation raises :class:`NotImplementedError`; a
            per-file site driver overrides this method.

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
            "ResultStoreDriver.put_object is not implemented by this driver.",
            "    backend_driver: {}".format(backend_driver),
            "    backend_key_prefix: {}".format(backend_key_prefix),
            "    relative_path: {}".format(relative_path),
            "Override put_object (per-file) or put_tree (batch) on the "
            "concrete ResultStoreDriver.",
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise NotImplementedError(errmsg)

    def put_tree(self, *, run_id: str, backend_driver: str,
                 backend_key_prefix: str,
                 entries: List["ResultTreeEntry"]) -> None:
        """
            Transfers every file of a walked result tree to the shared
            results store.

            The base implementation loops :meth:`put_object` over the
            entries so a per-file site driver works without overriding this
            method. A batch-oriented driver (such as the
            :class:`PresignedUploadDriver`) overrides :meth:`put_tree`
            directly.

            :param run_id: The server-assigned run UUID.
            :param backend_driver: The object-store driver name the envelope
                                   declared.
            :param backend_key_prefix: The per-run key prefix the envelope
                                       declared.
            :param entries: The walked result-tree entries to transfer.

            :returns: None
            :raises: Exception on any transfer failure.
        """
        for entry in entries:
            self.put_object(
                backend_driver=backend_driver,
                backend_key_prefix=backend_key_prefix,
                relative_path=entry.relative_path,
                local_path=entry.absolute_path)
        return


class ResultUploaderAspects:
    """
        The :class:`ResultUploaderAspects` packages caller-controlled
        behavior for the result uploader: the driver used to transfer the
        result tree, the chunk size artifacts are streamed with, and the
        retry schedule a failed upload is re-attempted on.

        Following the project's Aspects pattern, behavior modification is
        carried as data on a single parameter rather than via decorators.
        In particular there is no ``@retry`` decorator anywhere in this
        module -- the caller decides how persistent the client is.

        The retry schedule is applied BETWEEN passes, never inside one.
        A single upload attempt never blocks the calling loop on a sleep;
        instead the computed ``next_attempt_at`` is persisted on the local
        run record and :func:`retry_pending_uploads` honors it on a later
        pass. That is what lets a backoff survive a client restart.
    """

    def __init__(self, *, store_driver: Optional[ResultStoreDriver] = None,
                 artifact_chunk_size: int = DEFAULT_ARTIFACT_CHUNK_SIZE,
                 max_upload_attempts: int = DEFAULT_MAX_UPLOAD_ATTEMPTS,
                 retry_interval_seconds: float =
                 DEFAULT_UPLOAD_RETRY_INTERVAL_SECONDS,
                 retry_backoff_factor: float =
                 DEFAULT_UPLOAD_RETRY_BACKOFF_FACTOR,
                 retry_max_interval_seconds: float =
                 DEFAULT_UPLOAD_RETRY_MAX_INTERVAL_SECONDS):
        """
            Initializes a :class:`ResultUploaderAspects` instance.

            :param store_driver: The :class:`ResultStoreDriver` used to
                                 transfer result-tree bytes. ``None`` (the
                                 default) selects the default
                                 :class:`PresignedUploadDriver`, which
                                 uploads through the service's presigned-URL
                                 protocol. A site that provisions its own
                                 direct object-store binding may wire a
                                 custom driver here instead.
            :param artifact_chunk_size: The number of bytes streamed to the
                                        transport per chunk. Bounds the
                                        client's per-artifact memory cost.
            :param max_upload_attempts: The maximum number of delivery
                                        attempts a pending upload is given
                                        before it is abandoned. ``0`` (the
                                        default) means the upload is retried
                                        for as long as the client runs.
            :param retry_interval_seconds: The delay before the first retry
                                           of a failed upload, in seconds.
            :param retry_backoff_factor: The multiplier applied to the retry
                                         delay after each failed attempt.
            :param retry_max_interval_seconds: The ceiling on the retry
                                               delay, in seconds.

            :raises: SemanticError when a behavior value is out of range.
        """
        if artifact_chunk_size <= 0:
            errmsg_lines = [
                "ResultUploaderAspects 'artifact_chunk_size' must be "
                "greater than zero.",
                "    artifact_chunk_size: {}".format(artifact_chunk_size),
            ]
            errmsg = os.linesep.join(errmsg_lines)
            raise SemanticError(errmsg)

        if max_upload_attempts < 0:
            errmsg_lines = [
                "ResultUploaderAspects 'max_upload_attempts' must not be "
                "negative. Use 0 for an uncapped retry policy.",
                "    max_upload_attempts: {}".format(max_upload_attempts),
            ]
            errmsg = os.linesep.join(errmsg_lines)
            raise SemanticError(errmsg)

        if retry_interval_seconds < 0.0:
            errmsg_lines = [
                "ResultUploaderAspects 'retry_interval_seconds' must not be "
                "negative.",
                "    retry_interval_seconds: {}".format(
                    retry_interval_seconds),
            ]
            errmsg = os.linesep.join(errmsg_lines)
            raise SemanticError(errmsg)

        if retry_backoff_factor < 1.0:
            errmsg_lines = [
                "ResultUploaderAspects 'retry_backoff_factor' must be at "
                "least 1.0 so the retry delay never shrinks.",
                "    retry_backoff_factor: {}".format(retry_backoff_factor),
            ]
            errmsg = os.linesep.join(errmsg_lines)
            raise SemanticError(errmsg)

        if retry_max_interval_seconds < retry_interval_seconds:
            errmsg_lines = [
                "ResultUploaderAspects 'retry_max_interval_seconds' must "
                "not be smaller than 'retry_interval_seconds'.",
                "    retry_interval_seconds: {}".format(
                    retry_interval_seconds),
                "    retry_max_interval_seconds: {}".format(
                    retry_max_interval_seconds),
            ]
            errmsg = os.linesep.join(errmsg_lines)
            raise SemanticError(errmsg)

        self._store_driver = store_driver
        self._artifact_chunk_size = artifact_chunk_size
        self._max_upload_attempts = max_upload_attempts
        self._retry_interval_seconds = retry_interval_seconds
        self._retry_backoff_factor = retry_backoff_factor
        self._retry_max_interval_seconds = retry_max_interval_seconds
        return

    @property
    def store_driver(self) -> Optional[ResultStoreDriver]:
        return self._store_driver

    @property
    def artifact_chunk_size(self) -> int:
        return self._artifact_chunk_size

    @property
    def max_upload_attempts(self) -> int:
        return self._max_upload_attempts

    @property
    def retry_interval_seconds(self) -> float:
        return self._retry_interval_seconds

    @property
    def retry_backoff_factor(self) -> float:
        return self._retry_backoff_factor

    @property
    def retry_max_interval_seconds(self) -> float:
        return self._retry_max_interval_seconds


DEFAULT_RESULT_UPLOADER_ASPECTS = ResultUploaderAspects()


def _now_rfc3339() -> str:
    """
        Produces the current UTC time as an RFC 3339 string.

        :returns: An RFC 3339 timestamp string.
    """
    rtnval = datetime.now(timezone.utc).isoformat()
    return rtnval


def _parse_rfc3339(timestamp: str) -> Optional[datetime]:
    """
        Parses an RFC 3339 timestamp string into a timezone-aware
        :class:`datetime`.

        A value written by an older client (or hand-edited by an operator)
        that cannot be parsed yields ``None`` so the caller can treat the
        schedule as "due now" rather than crash the client loop.

        :param timestamp: The RFC 3339 timestamp string.

        :returns: The parsed timezone-aware datetime, or ``None`` when the
                  value is not a parseable timestamp.
    """
    if timestamp is None:
        rtnval = None
        return rtnval

    if len(timestamp) == 0:
        rtnval = None
        return rtnval

    try:
        parsed = datetime.fromisoformat(timestamp)
    except ValueError:
        logger.warning(
            "pychestrate-client: could not parse timestamp '%s'; treating "
            "the schedule as due now.", timestamp,
        )
        rtnval = None
        return rtnval

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    rtnval = parsed
    return rtnval


def _sha256_of_file(file_path: str) -> str:
    """
        Computes the hex SHA-256 digest of a file.

        The file is hashed in bounded reads so an arbitrarily large
        artifact never has to fit in memory.

        :param file_path: The absolute path of the file.

        :returns: The lowercase hex digest string.
    """
    hasher = hashlib.sha256()
    with open(file_path, "rb") as digf:
        while True:
            chunk = digf.read(SHA256_READ_CHUNK_SIZE)
            if len(chunk) == 0:
                break
            hasher.update(chunk)

    rtnval = hasher.hexdigest()
    return rtnval


class ChunkedFilePayload:
    """
        The :class:`ChunkedFilePayload` streams one local artifact to a
        transport in bounded chunks.

        It exists because an artifact is an arbitrarily large file -- a
        multi-gigabyte console log, a core file, a screen recording. Reading
        one fully into memory to hand it to ``requests`` is what made the
        client OOM-prone, so every artifact is handed over as one of these
        instead.

        Two properties make it a drop-in ``requests`` body:

            * ``__len__`` reports the byte count, so ``requests`` sends a
              ``Content-Length`` rather than falling back to chunked
              transfer encoding (object stores such as S3 require the
              former on a presigned PUT).
            * ``__iter__`` yields at most ``chunk_size`` bytes at a time,
              so the peak memory cost is one chunk regardless of file size.

        The payload also re-hashes the bytes as it reads them. When an
        ``expected_sha256`` is supplied (the digest recorded while walking
        the result tree) a file that changed underneath the walk is
        detected and the transfer is failed rather than silently shipping
        bytes that do not match the manifest's file index.
    """

    def __init__(self, *, local_path: str, size_bytes: int,
                 chunk_size: int = DEFAULT_ARTIFACT_CHUNK_SIZE,
                 expected_sha256: Optional[str] = None):
        """
            Initializes a :class:`ChunkedFilePayload`.

            :param local_path: The absolute path of the local file to
                               stream.
            :param size_bytes: The byte count recorded for the file. This
                               is what the transport is told to expect.
            :param chunk_size: The maximum number of bytes yielded per
                               chunk.
            :param expected_sha256: The hex SHA-256 digest the streamed
                                    bytes must hash to. ``None`` disables
                                    the check.

            :raises: SemanticError when ``chunk_size`` is not positive.
        """
        if chunk_size <= 0:
            errmsg_lines = [
                "ChunkedFilePayload 'chunk_size' must be greater than zero.",
                "    local_path: {}".format(local_path),
                "    chunk_size: {}".format(chunk_size),
            ]
            errmsg = os.linesep.join(errmsg_lines)
            raise SemanticError(errmsg)

        self._local_path = local_path
        self._size_bytes = size_bytes
        self._chunk_size = chunk_size
        self._expected_sha256 = expected_sha256
        self._bytes_streamed = 0
        self._streamed_sha256: Optional[str] = None
        return

    @property
    def local_path(self) -> str:
        return self._local_path

    @property
    def size_bytes(self) -> int:
        return self._size_bytes

    @property
    def chunk_size(self) -> int:
        return self._chunk_size

    @property
    def expected_sha256(self) -> Optional[str]:
        return self._expected_sha256

    @property
    def bytes_streamed(self) -> int:
        return self._bytes_streamed

    @property
    def streamed_sha256(self) -> Optional[str]:
        return self._streamed_sha256

    def __len__(self) -> int:
        """
            Gets the number of bytes this payload will stream.

            ``requests`` reads this to set the ``Content-Length`` header.

            :returns: The recorded byte count of the file.
        """
        return self._size_bytes

    def __iter__(self) -> Iterator[bytes]:
        """
            Streams the file in bounded chunks, hashing as it reads.

            The iteration state is reset on every pass so a transport that
            re-sends the body (a redirect, a connection-level retry) gets a
            correct digest rather than a continuation of the previous pass.

            :returns: An iterator over the file's bytes, at most
                      ``chunk_size`` bytes per chunk.
            :raises: HTTPRequestError when the file is shorter than its
                     recorded size or its bytes no longer hash to
                     ``expected_sha256``.
        """
        hasher = hashlib.sha256()
        self._bytes_streamed = 0
        self._streamed_sha256 = None

        with open(self._local_path, "rb") as artf:
            while self._bytes_streamed < self._size_bytes:
                remaining = self._size_bytes - self._bytes_streamed
                read_size = min(self._chunk_size, remaining)

                chunk = artf.read(read_size)
                if len(chunk) == 0:
                    break

                self._bytes_streamed += len(chunk)
                hasher.update(chunk)
                yield chunk

        self._streamed_sha256 = hasher.hexdigest()

        if self._bytes_streamed != self._size_bytes:
            errmsg_lines = [
                "An artifact shrank while it was being uploaded.",
                "    local_path: {}".format(self._local_path),
                "    expected_bytes: {}".format(self._size_bytes),
                "    streamed_bytes: {}".format(self._bytes_streamed),
            ]
            errmsg = os.linesep.join(errmsg_lines)
            raise HTTPRequestError(errmsg)

        if self._expected_sha256 is not None and \
                len(self._expected_sha256) > 0 and \
                self._streamed_sha256 != self._expected_sha256:
            errmsg_lines = [
                "An artifact changed while it was being uploaded; the "
                "streamed bytes do not match the digest recorded when the "
                "result tree was walked.",
                "    local_path: {}".format(self._local_path),
                "    expected_sha256: {}".format(self._expected_sha256),
                "    streamed_sha256: {}".format(self._streamed_sha256),
            ]
            errmsg = os.linesep.join(errmsg_lines)
            raise HTTPRequestError(errmsg)

        return


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


#: Content type sent on every presigned artifact PUT.
ARTIFACT_PUT_CONTENT_TYPE = "application/octet-stream"


def _upload_target_to_dict(target: Any) -> Dict[str, Any]:
    """
        Coerces an ``Artifactuploadtarget`` schema object (or a raw dict)
        into a plain dict.

        :param target: The upload-target object from an
                       ``ArtifactPresignResponse``.

        :returns: A dict view of the upload target.
    """
    if isinstance(target, dict):
        rtnval = target
        return rtnval

    to_dict = getattr(target, "to_dict", None)
    if callable(to_dict):
        rtnval = to_dict()
        return rtnval

    rtnval = {
        "path": getattr(target, "path", None),
        "put_url": getattr(target, "put_url", None),
        "expires_at": getattr(target, "expires_at", None),
        "headers": getattr(target, "headers", None),
    }
    return rtnval


class PresignedUploadDriver(ResultStoreDriver):
    """
        The :class:`PresignedUploadDriver` is the default
        :class:`ResultStoreDriver`. It transfers a run's result tree using
        the service's presigned-URL upload protocol:

            1. Batch-request presigned PUT targets from
               ``POST /api/v1/runs/{run_id}/artifacts:presign`` for every
               relative path in the walked result tree.
            2. Stream each artifact's bytes to its ``put_url`` in bounded
               chunks, sending the target's ``headers`` verbatim.

        The presign request flows over the client's mTLS-authenticated REST
        client. The PUT reuses that same authenticated
        :class:`requests.Session` because the local-filesystem fallback
        ``put_url`` points back at an mTLS-gated upload sink on the service;
        for an S3-backed deploy the pinned client certificate is simply
        ignored by the object store.
    """

    def __init__(self, *, client: PychestrateMtlsClient,
                 chunk_size: int = DEFAULT_ARTIFACT_CHUNK_SIZE):
        """
            Initializes a :class:`PresignedUploadDriver`.

            :param client: The configured :class:`PychestrateMtlsClient`
                           used for both the ``:presign`` request and the
                           artifact PUTs.
            :param chunk_size: The number of bytes streamed to the transport
                               per chunk on each artifact PUT.
        """
        self._client = client
        self._chunk_size = chunk_size
        return

    @property
    def client(self) -> PychestrateMtlsClient:
        """
            Gets the mTLS client this driver transfers through.

            :returns: The :class:`PychestrateMtlsClient`.
        """
        return self._client

    @property
    def chunk_size(self) -> int:
        """
            Gets the number of bytes streamed per chunk on an artifact PUT.

            :returns: The artifact streaming chunk size in bytes.
        """
        return self._chunk_size

    def _presign_paths(self, run_id: str,
                       relative_paths: List[str]) -> List[Dict[str, Any]]:
        """
            Calls ``POST /api/v1/runs/{run_id}/artifacts:presign`` for the
            supplied relative paths and returns the minted upload targets.

            :param run_id: The server-assigned run UUID.
            :param relative_paths: The relative artifact paths to presign.

            :returns: A list of upload-target dicts (``path`` / ``put_url``
                      / ``headers`` / ``expires_at``).
            :raises: requests.RequestException, ccai HTTP error types
        """
        request_body = Artifactpresignrequest(paths=relative_paths)

        response = self._client.results.artifacts_presign_upload(
            run_id=run_id, body=request_body)

        raw_uploads = getattr(response, "uploads", None)
        if raw_uploads is None and isinstance(response, dict):
            raw_uploads = response.get("uploads", None)
        if raw_uploads is None:
            raw_uploads = []

        rtnval = [_upload_target_to_dict(target=nxt) for nxt in raw_uploads]
        return rtnval

    def _put_artifact(self, *, target: Dict[str, Any],
                      entry: "ResultTreeEntry") -> None:
        """
            Streams one artifact's bytes to a presigned ``put_url``.

            The artifact is never fully materialized in memory. It is handed
            to the session as a :class:`ChunkedFilePayload`, which yields at
            most ``chunk_size`` bytes at a time and reports its length so
            the PUT carries a ``Content-Length``.

            :param target: The upload-target dict for the artifact.
            :param entry: The walked result-tree entry naming the local
                          file, its size and the digest its bytes must
                          still hash to.

            :returns: None
            :raises: requests.RequestException, HTTPRequestError
        """
        put_url = target.get("put_url", None)
        if put_url is None or len(put_url) == 0:
            raise HTTPRequestError(
                "Presigned upload target carries no 'put_url' for "
                "{}.".format(target.get("path")))

        headers: Dict[str, str] = {"Content-Type": ARTIFACT_PUT_CONTENT_TYPE}
        target_headers = target.get("headers", None)
        if isinstance(target_headers, dict):
            for header_name, header_value in target_headers.items():
                headers[header_name] = str(header_value)

        if self._client._session is None:
            self._client.authenticate()
        session = self._client._session

        payload = ChunkedFilePayload(
            local_path=entry.absolute_path, size_bytes=entry.size_bytes,
            chunk_size=self._chunk_size, expected_sha256=entry.sha256)

        response = session.put(put_url, data=payload, headers=headers)

        if response.status_code not in (200, 201, 204):
            errmsg_lines = [
                "Presigned artifact PUT failed.",
                "    path: {}".format(target.get("path")),
                "    url: {}".format(put_url),
                "    status: {}".format(response.status_code),
            ]
            errmsg = os.linesep.join(errmsg_lines)
            raise HTTPRequestError(errmsg)

        return

    def put_tree(self, *, run_id: str, backend_driver: str,
                 backend_key_prefix: str,
                 entries: List["ResultTreeEntry"]) -> None:
        """
            Transfers a walked result tree using the presigned-URL upload
            protocol: batch-presign every relative path, then PUT each
            artifact's bytes to its minted target.

            :param run_id: The server-assigned run UUID.
            :param backend_driver: The object-store driver name (carried
                                   for parity; the transfer is fully
                                   described by the presigned targets).
            :param backend_key_prefix: The per-run key prefix (carried for
                                       parity).
            :param entries: The walked result-tree entries to transfer.

            :returns: None
            :raises: requests.RequestException, ccai HTTP error types
        """
        if len(entries) == 0:
            logger.info(
                "pychestrate-client: result tree for run_id=%s is empty; "
                "no artifacts to presign.", run_id,
            )
            return

        relative_paths = [entry.relative_path for entry in entries]

        targets = self._presign_paths(
            run_id=run_id, relative_paths=relative_paths)

        targets_by_path: Dict[str, Dict[str, Any]] = {}
        for nxttarget in targets:
            target_path = nxttarget.get("path", None)
            if target_path is not None:
                targets_by_path[target_path] = nxttarget

        for entry in entries:
            target = targets_by_path.get(entry.relative_path, None)
            if target is None:
                errmsg_lines = [
                    "The presign response is missing an upload target.",
                    "    run_id: {}".format(run_id),
                    "    relative_path: {}".format(entry.relative_path),
                ]
                errmsg = os.linesep.join(errmsg_lines)
                raise HTTPRequestError(errmsg)

            self._put_artifact(target=target, entry=entry)

        logger.info(
            "pychestrate-client: PUT %d presigned artifact(s) for run_id=%s.",
            len(entries), run_id,
        )
        return


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


def upload_result_tree(*, run_id: str, entries: List[ResultTreeEntry],
                        backend_driver: str, backend_key_prefix: str,
                        store_driver: ResultStoreDriver) -> bool:
    """
        Transfers every file in a walked result tree to the shared results
        store via the supplied :class:`ResultStoreDriver`.

        The transfer is delegated to :meth:`ResultStoreDriver.put_tree`. A
        driver that raises during the transfer is logged and yields a
        ``False`` (failed) result -- partial uploads are not treated as
        success, and the caller marks the run ``upload_failed`` for retry.

        :param run_id: The server-assigned run UUID.
        :param entries: The walked result-tree entries.
        :param backend_driver: The object-store driver name from the
                               envelope's ``JobResultsLocator``.
        :param backend_key_prefix: The per-run key prefix from the envelope's
                                   ``JobResultsLocator``.
        :param store_driver: The :class:`ResultStoreDriver` to transfer with.

        :returns: True when every file transferred; False when the transfer
                  failed.
    """
    try:
        store_driver.put_tree(
            run_id=run_id,
            backend_driver=backend_driver,
            backend_key_prefix=backend_key_prefix,
            entries=entries)
    except Exception as puterr:
        logger.error(
            "pychestrate-client: result-tree upload failed for run_id=%s "
            "(prefix=%s): %s",
            run_id, backend_key_prefix, puterr,
        )
        rtnval = False
        return rtnval

    logger.info(
        "pychestrate-client: uploaded %d result-tree file(s) for run_id=%s "
        "(prefix=%s).",
        len(entries), run_id, backend_key_prefix,
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
        :raises: requests.RequestException, ccai HTTP error types
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


class PendingUpload:
    """
        The :class:`PendingUpload` is the durable description of a run whose
        results have NOT reached the service yet.

        It is written into the ``upload_retry`` section of the same local
        record file that carries the ``upload`` outcome, and it carries
        everything a later pass needs to finish the job without the original
        job envelope: which flavor of manifest to build, where the result
        tree still lives on disk, where it belongs in the object store,
        whether the bytes already landed, and when the next attempt is due.

        Because it lives on disk rather than in memory, a client that is
        restarted (or a pod that reboots) picks the retry back up on the
        same schedule instead of stranding the results forever.
    """

    def __init__(self, *, kind: str, run_id: str, client_id: str,
                 result_dir: str, run_succeeded: bool, backend_driver: str,
                 backend_key_prefix: str, record_path: str,
                 tree_delivered: bool = False, attempts: int = 0,
                 first_failed_at: Optional[str] = None,
                 last_attempt_at: Optional[str] = None,
                 next_attempt_at: Optional[str] = None,
                 last_error: Optional[str] = None,
                 abandoned: bool = False,
                 delivered_files: Optional[List[Dict[str, Any]]] = None,
                 summary_counts: Optional[Dict[str, int]] = None,
                 terminal_outcome: Optional[str] = None,
                 failure_classification: Optional[str] = None):
        """
            Initializes a :class:`PendingUpload`.

            :param kind: The job kind (:data:`UPLOAD_KIND_TESTBASE`,
                         :data:`UPLOAD_KIND_BUILD` or
                         :data:`UPLOAD_KIND_PROVISION`).
            :param run_id: The server-assigned run UUID.
            :param client_id: The assigned client UUID.
            :param result_dir: The absolute path of the local result tree
                               still awaiting delivery.
            :param run_succeeded: Whether the job itself succeeded (drives
                                  the manifest ``terminal_outcome``).
            :param backend_driver: The object-store driver name from the
                                   envelope's ``JobResultsLocator``.
            :param backend_key_prefix: The per-run key prefix from the
                                       envelope's ``JobResultsLocator``.
            :param record_path: The absolute path of the local record file
                                this descriptor is persisted in.
            :param tree_delivered: True when the artifact bytes already
                                   landed and only the completion POST is
                                   still owed.
            :param attempts: The number of delivery attempts made so far.
            :param first_failed_at: RFC 3339 stamp of the first failure.
            :param last_attempt_at: RFC 3339 stamp of the most recent
                                    attempt.
            :param next_attempt_at: RFC 3339 stamp naming the earliest time
                                    the next attempt may be made.
            :param last_error: The most recent failure detail.
            :param abandoned: True when the attempt cap was reached and no
                              further attempt will be made.
            :param delivered_files: The file index (as written by
                                    :meth:`ResultTreeEntry.as_dict`) that
                                    actually reached the object store. It is
                                    recorded once the bytes land so a later
                                    completion manifest describes what was
                                    delivered rather than whatever the local
                                    directory happens to hold by then.
            :param summary_counts: The testbase pass/fail/skip/error counts
                                   read from the result tree at delivery
                                   time, recorded for the same reason as
                                   ``delivered_files``. Unused by the build
                                   and provision kinds.
            :param terminal_outcome: A ``TerminalOutcome`` value supplied by
                                     the caller, used verbatim instead of
                                     deriving one from ``run_succeeded``.
                                     ``None`` keeps the derived behavior.
                                     Persisted so a retry of a cancelled run
                                     still reports ``cancelled`` rather than
                                     re-deriving ``failure``.
            :param failure_classification: The structured reason the run did
                                           not succeed, carried on the
                                           manifest under
                                           ``extras.failure_classification``.
                                           ``None`` omits the member.
        """
        self._kind = kind
        self._run_id = run_id
        self._client_id = client_id
        self._result_dir = result_dir
        self._run_succeeded = run_succeeded
        self._backend_driver = backend_driver
        self._backend_key_prefix = backend_key_prefix
        self._record_path = record_path
        self._tree_delivered = tree_delivered
        self._attempts = attempts
        self._first_failed_at = first_failed_at
        self._last_attempt_at = last_attempt_at
        self._next_attempt_at = next_attempt_at
        self._last_error = last_error
        self._abandoned = abandoned
        self._delivered_files = delivered_files
        self._summary_counts = summary_counts
        self._terminal_outcome = terminal_outcome
        self._failure_classification = failure_classification
        return

    @property
    def kind(self) -> str:
        return self._kind

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def client_id(self) -> str:
        return self._client_id

    @property
    def result_dir(self) -> str:
        return self._result_dir

    @property
    def run_succeeded(self) -> bool:
        return self._run_succeeded

    @property
    def backend_driver(self) -> str:
        return self._backend_driver

    @property
    def backend_key_prefix(self) -> str:
        return self._backend_key_prefix

    @property
    def record_path(self) -> str:
        return self._record_path

    @property
    def tree_delivered(self) -> bool:
        return self._tree_delivered

    @property
    def attempts(self) -> int:
        return self._attempts

    @property
    def first_failed_at(self) -> Optional[str]:
        return self._first_failed_at

    @property
    def last_attempt_at(self) -> Optional[str]:
        return self._last_attempt_at

    @property
    def next_attempt_at(self) -> Optional[str]:
        return self._next_attempt_at

    @property
    def last_error(self) -> Optional[str]:
        return self._last_error

    @property
    def abandoned(self) -> bool:
        return self._abandoned

    @property
    def delivered_files(self) -> Optional[List[Dict[str, Any]]]:
        return self._delivered_files

    @property
    def summary_counts(self) -> Optional[Dict[str, int]]:
        return self._summary_counts

    @property
    def terminal_outcome(self) -> Optional[str]:
        return self._terminal_outcome

    @property
    def failure_classification(self) -> Optional[str]:
        return self._failure_classification

    def as_dict(self) -> Dict[str, Any]:
        """
            Returns a JSON-serializable dictionary representation.

            The ``record_path`` is deliberately omitted -- it is where the
            descriptor lives, not part of the descriptor.

            :returns: A dict describing the pending upload.
        """
        rtnval = {
            "kind": self._kind,
            "run_id": self._run_id,
            "client_id": self._client_id,
            "result_dir": self._result_dir,
            "run_succeeded": self._run_succeeded,
            "backend_driver": self._backend_driver,
            "backend_key_prefix": self._backend_key_prefix,
            "tree_delivered": self._tree_delivered,
            "attempts": self._attempts,
            "first_failed_at": self._first_failed_at,
            "last_attempt_at": self._last_attempt_at,
            "next_attempt_at": self._next_attempt_at,
            "last_error": self._last_error,
            "abandoned": self._abandoned,
            "delivered_files": self._delivered_files,
            "summary_counts": self._summary_counts,
            "terminal_outcome": self._terminal_outcome,
            "failure_classification": self._failure_classification,
        }
        return rtnval

    def is_due(self, now: datetime) -> bool:
        """
            Returns whether this pending upload may be attempted again.

            A descriptor whose ``next_attempt_at`` cannot be parsed is
            treated as due so an unreadable schedule delays results rather
            than stranding them.

            :param now: The timezone-aware moment to evaluate the schedule
                        against.

            :returns: True when the next attempt is due.
        """
        if self._abandoned:
            rtnval = False
            return rtnval

        scheduled = _parse_rfc3339(timestamp=self._next_attempt_at)
        if scheduled is None:
            rtnval = True
            return rtnval

        rtnval = now >= scheduled
        return rtnval


def _resolve_record_dir(*, state: ClientState, kind: str,
                        run_id: str) -> str:
    """
        Resolves the directory holding the local record for a job.

        :param state: The :class:`ClientState`.
        :param kind: The job kind.
        :param run_id: The server-assigned run UUID.

        :returns: The absolute path of the record directory.
        :raises: SemanticError when the job kind is not recognized.
    """
    if kind not in UPLOAD_KINDS:
        errmsg_lines = [
            "The result uploader was handed an unrecognized job kind.",
            "    kind: {}".format(kind),
            "    known kinds: {}".format(", ".join(UPLOAD_KINDS)),
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise SemanticError(errmsg)

    fallback_run_id = FALLBACK_RUN_ID_BY_KIND[kind]
    safe_run_id = run_id if len(run_id) > 0 else fallback_run_id

    if kind == UPLOAD_KIND_TESTBASE:
        rtnval = os.path.join(state.runs_dir, safe_run_id)
        return rtnval

    if kind == UPLOAD_KIND_BUILD:
        rtnval = os.path.join(state.state_dir, DIRNAME_BUILDS, safe_run_id)
        return rtnval

    rtnval = os.path.join(state.state_dir, DIRNAME_PROVISIONING, safe_run_id)
    return rtnval


def _resolve_record_path(*, state: ClientState, kind: str,
                         run_id: str) -> str:
    """
        Resolves the local record file a job kind persists its upload state
        in.

        :param state: The :class:`ClientState`.
        :param kind: The job kind.
        :param run_id: The server-assigned run UUID.

        :returns: The absolute path of the record file.
        :raises: SemanticError when the job kind is not recognized.
    """
    record_dir = _resolve_record_dir(state=state, kind=kind, run_id=run_id)
    record_filename = RECORD_FILENAME_BY_KIND[kind]

    rtnval = os.path.join(record_dir, record_filename)
    return rtnval


def _read_local_record(record_path: str) -> Dict[str, Any]:
    """
        Reads a local run / build / provision record file.

        A missing or unparseable record yields an empty dict -- the record
        is a local bookkeeping file, and losing it must not stop the client
        from recording the current outcome.

        :param record_path: The absolute path of the record file.

        :returns: The record dict, empty when the file is absent or
                  unreadable.
    """
    rtnval: Dict[str, Any] = {}

    if not os.path.isfile(record_path):
        return rtnval

    try:
        with open(record_path, "r", encoding="utf-8") as recf:
            loaded = json.load(recf)
    except (ValueError, OSError) as readerr:
        logger.warning(
            "pychestrate-client: could not read local record %s, "
            "rewriting: %s", record_path, readerr,
        )
        return rtnval

    if isinstance(loaded, dict):
        rtnval = loaded

    return rtnval


def _write_upload_record(*, record_path: str, run_id: str,
                         upload_outcome: ResultUploadOutcome,
                         pending: Optional[PendingUpload]) -> str:
    """
        Merges the upload outcome (and, when one is owed, the pending-upload
        retry descriptor) into a local record file and rewrites it.

        Passing ``pending=None`` clears any retry descriptor a previous
        failure left behind, which is what marks the results as finally
        delivered.

        :param record_path: The absolute path of the record file.
        :param run_id: The server-assigned run UUID.
        :param upload_outcome: The :class:`ResultUploadOutcome` to record.
        :param pending: The :class:`PendingUpload` to persist, or ``None``
                        when no retry is owed.

        :returns: The absolute path of the record file.
    """
    record_dir = os.path.dirname(record_path)
    os.makedirs(record_dir, mode=0o755, exist_ok=True)

    record = _read_local_record(record_path=record_path)

    record["run_id"] = run_id
    record[RECORD_SECTION_UPLOAD] = upload_outcome.as_dict()

    if pending is None:
        record.pop(RECORD_SECTION_UPLOAD_RETRY, None)
    else:
        record[RECORD_SECTION_UPLOAD_RETRY] = pending.as_dict()

    with open(record_path, "w", encoding="utf-8") as recf:
        json.dump(record, recf, indent=4, sort_keys=True)
        recf.write(os.linesep)

    rtnval = record_path
    return rtnval


def _pending_from_record(*, record: Dict[str, Any],
                         record_path: str) -> Optional[PendingUpload]:
    """
        Rebuilds a :class:`PendingUpload` from a local record file's
        ``upload_retry`` section.

        A record that carries no retry section, or one whose section is not
        a usable descriptor, yields ``None``.

        :param record: The loaded record dict.
        :param record_path: The absolute path the record was loaded from.

        :returns: The :class:`PendingUpload`, or ``None``.
    """
    retry_section = record.get(RECORD_SECTION_UPLOAD_RETRY, None)
    if not isinstance(retry_section, dict):
        rtnval = None
        return rtnval

    kind = retry_section.get("kind", None)
    if kind not in UPLOAD_KINDS:
        logger.warning(
            "pychestrate-client: local record %s carries an upload retry "
            "descriptor with an unusable job kind (%s); ignoring it.",
            record_path, kind,
        )
        rtnval = None
        return rtnval

    result_dir = retry_section.get("result_dir", None)
    if not isinstance(result_dir, str) or len(result_dir) == 0:
        logger.warning(
            "pychestrate-client: local record %s carries an upload retry "
            "descriptor with no result directory; ignoring it.", record_path,
        )
        rtnval = None
        return rtnval

    delivered_files = retry_section.get("delivered_files", None)
    if not isinstance(delivered_files, list):
        delivered_files = None

    summary_counts = retry_section.get("summary_counts", None)
    if not isinstance(summary_counts, dict):
        summary_counts = None

    rtnval = PendingUpload(
        kind=kind,
        run_id=str(retry_section.get("run_id", "")),
        client_id=str(retry_section.get("client_id", "")),
        result_dir=result_dir,
        run_succeeded=bool(retry_section.get("run_succeeded", False)),
        backend_driver=str(retry_section.get("backend_driver", "")),
        backend_key_prefix=str(retry_section.get("backend_key_prefix", "")),
        record_path=record_path,
        tree_delivered=bool(retry_section.get("tree_delivered", False)),
        attempts=int(retry_section.get("attempts", 0)),
        first_failed_at=retry_section.get("first_failed_at", None),
        last_attempt_at=retry_section.get("last_attempt_at", None),
        next_attempt_at=retry_section.get("next_attempt_at", None),
        last_error=retry_section.get("last_error", None),
        abandoned=bool(retry_section.get("abandoned", False)),
        delivered_files=delivered_files,
        summary_counts=summary_counts,
        terminal_outcome=retry_section.get("terminal_outcome", None),
        failure_classification=retry_section.get(
            "failure_classification", None))
    return rtnval


def _entries_from_file_index(*, result_dir: str,
                             file_index: List[Dict[str, Any]]
                             ) -> List[ResultTreeEntry]:
    """
        Rebuilds walked result-tree entries from a persisted file index.

        This is how a retry describes a tree whose bytes already reached
        the object store: the index recorded at delivery time is the truth
        about what was delivered, and re-walking the local directory could
        disagree with it (or find nothing at all).

        :param result_dir: The result-tree root the relative paths hang off.
        :param file_index: The persisted file index entries.

        :returns: The rebuilt :class:`ResultTreeEntry` list.
    """
    rtnval: List[ResultTreeEntry] = []

    for indexed in file_index:
        if not isinstance(indexed, dict):
            continue

        relative_path = indexed.get("relative_path", None)
        if not isinstance(relative_path, str) or len(relative_path) == 0:
            continue

        native_relative = relative_path.replace("/", os.sep)
        absolute_path = os.path.join(result_dir, native_relative)

        entry = ResultTreeEntry(
            relative_path=relative_path, absolute_path=absolute_path,
            size_bytes=int(indexed.get("size_bytes", 0)),
            sha256=str(indexed.get("sha256", "")))
        rtnval.append(entry)

    return rtnval


def _compute_next_attempt_at(*, attempts: int,
                             aspects: ResultUploaderAspects,
                             now: datetime) -> str:
    """
        Computes the RFC 3339 stamp naming the earliest moment the next
        delivery attempt may be made.

        The delay grows geometrically with the number of attempts already
        made and is clamped at the aspects' ceiling, so a service that is
        down for hours is not hammered once per loop iteration.

        :param attempts: The number of attempts made so far (at least one).
        :param aspects: Behavior-shaping aspects supplying the schedule.
        :param now: The moment the failing attempt completed.

        :returns: An RFC 3339 timestamp string.
    """
    exponent = attempts - 1
    if exponent < 0:
        exponent = 0

    delay_seconds = aspects.retry_interval_seconds * \
        (aspects.retry_backoff_factor ** exponent)

    if delay_seconds > aspects.retry_max_interval_seconds:
        delay_seconds = aspects.retry_max_interval_seconds

    scheduled = now + timedelta(seconds=delay_seconds)

    rtnval = scheduled.isoformat()
    return rtnval


def mark_run_upload_status(state: ClientState, run_id: str,
                           upload_outcome: ResultUploadOutcome,
                           pending: Optional[PendingUpload] = None) -> str:
    """
        Records the result-upload outcome onto the local run record so a
        failed upload can be retried.

        The run-record file is the one :mod:`testbase_executor` wrote; this
        function reads it, merges in an ``upload`` section (and an
        ``upload_retry`` section when a retry is owed), and rewrites it. A
        missing run record is created with just those sections.

        :param state: The :class:`ClientState`.
        :param run_id: The server-assigned run UUID.
        :param upload_outcome: The :class:`ResultUploadOutcome` to record.
        :param pending: The :class:`PendingUpload` describing the retry that
                        is owed. ``None`` clears any retry descriptor a
                        previous failure left behind.

        :returns: The absolute path of the run-record file.
    """
    record_path = _resolve_record_path(
        state=state, kind=UPLOAD_KIND_TESTBASE, run_id=run_id)

    rtnval = _write_upload_record(
        record_path=record_path, run_id=run_id,
        upload_outcome=upload_outcome, pending=pending)
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
    record_path = _resolve_record_path(
        state=state, kind=UPLOAD_KIND_TESTBASE, run_id=run_id)

    record = _read_local_record(record_path=record_path)

    upload = record.get(RECORD_SECTION_UPLOAD, None)
    if not isinstance(upload, dict):
        rtnval = False
        return rtnval

    rtnval = upload.get("status", None) == UPLOAD_STATUS_FAILED
    return rtnval


def _build_manifest_for_kind(*, kind: str, run_id: str, client_id: str,
                             terminal_outcome: str, backend_driver: str,
                             backend_key_prefix: str, result_dir: str,
                             entries: List[ResultTreeEntry],
                             counts: Optional[Dict[str, int]] = None,
                             failure_classification: Optional[str] = None
                             ) -> Resultmanifest:
    """
        Builds the ``ResultManifest`` flavor a job kind calls for.

        A testbase manifest carries the per-test counters read from the
        result tree's ``testrun_summary.json``; the build and provision
        flavors carry none.

        :param kind: The job kind.
        :param run_id: The server-assigned run UUID.
        :param client_id: The assigned client UUID.
        :param terminal_outcome: A ``TerminalOutcome`` enum value.
        :param backend_driver: The object-store driver name.
        :param backend_key_prefix: The per-run key prefix.
        :param result_dir: The absolute path of the result-tree root.
        :param entries: The walked result-tree entries.
        :param counts: Pre-read testbase counters to use instead of reading
                       the result tree. Supplied by a retry whose counters
                       were captured when the tree was delivered.
        :param failure_classification: The structured reason the run did not
                                       succeed. When supplied it is stamped
                                       onto the manifest under
                                       ``extras.failure_classification``.

        :returns: A populated :class:`Resultmanifest`.
        :raises: SemanticError when the job kind is not recognized.
    """
    if kind == UPLOAD_KIND_TESTBASE:
        if counts is None:
            counts = _read_summary_counts(result_dir=result_dir)
        manifest = build_result_manifest(
            run_id=run_id, client_id=client_id,
            terminal_outcome=terminal_outcome, backend_driver=backend_driver,
            backend_key_prefix=backend_key_prefix, counts=counts,
            entries=entries)
    elif kind == UPLOAD_KIND_BUILD:
        manifest = build_build_result_manifest(
            run_id=run_id, client_id=client_id,
            terminal_outcome=terminal_outcome, backend_driver=backend_driver,
            backend_key_prefix=backend_key_prefix, entries=entries)
    elif kind == UPLOAD_KIND_PROVISION:
        manifest = build_provision_result_manifest(
            run_id=run_id, client_id=client_id,
            terminal_outcome=terminal_outcome, backend_driver=backend_driver,
            backend_key_prefix=backend_key_prefix, entries=entries)
    else:
        errmsg_lines = [
            "The result uploader cannot build a manifest for this job kind.",
            "    kind: {}".format(kind),
            "    known kinds: {}".format(", ".join(UPLOAD_KINDS)),
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise SemanticError(errmsg)

    if failure_classification is not None and \
            len(failure_classification) > 0:
        manifest.extras[EXTRAS_KEY_FAILURE_CLASSIFICATION] = \
            failure_classification

    rtnval = manifest
    return rtnval


def deliver_pending_upload(*, client: PychestrateMtlsClient,
                           state: ClientState, pending: PendingUpload,
                           aspects: ResultUploaderAspects =
                           DEFAULT_RESULT_UPLOADER_ASPECTS
                           ) -> ResultUploadOutcome:
    """
        Makes one delivery attempt for a pending upload: transfers the
        result tree (unless a previous attempt already delivered the bytes),
        posts the ``ResultManifest``, and rewrites the local record.

        This is the single place the "did the artifacts actually land?"
        decision is made, for every job kind. The rule it enforces is that
        the completion manifest is posted ONLY after the whole tree
        transferred. A run whose bytes did not land is left recorded as
        ``upload_failed`` with an updated :class:`PendingUpload`, and
        NOTHING is asserted to the service -- posting a manifest for a tree
        that is not there would mark the run complete against artifacts that
        do not exist, and the service has no way to detect that.

        The attempt is non-raising: every failure is captured on the
        returned :class:`ResultUploadOutcome` so the caller's loop is never
        taken down by a bad upload.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param state: The :class:`ClientState`.
        :param pending: The :class:`PendingUpload` to attempt.
        :param aspects: Behavior-shaping aspects (store driver, streaming
                        chunk size, retry schedule).

        :returns: A :class:`ResultUploadOutcome` describing the attempt.
    """
    run_id = pending.run_id
    attempted_at = datetime.now(timezone.utc)
    attempts = pending.attempts + 1

    terminal_outcome = pending.terminal_outcome
    if terminal_outcome is None:
        if pending.run_succeeded:
            terminal_outcome = TERMINAL_OUTCOME_SUCCESS
        else:
            terminal_outcome = TERMINAL_OUTCOME_FAILURE

    if not pending.tree_delivered and \
            not os.path.isdir(pending.result_dir):
        errmsg_lines = [
            "The result tree awaiting upload is no longer on disk, so its "
            "artifacts can never be delivered. No completion manifest is "
            "posted -- claiming the run finished would assert artifacts "
            "that do not exist anywhere.",
            "    kind: {}".format(pending.kind),
            "    run_id: {}".format(run_id),
            "    result_dir: {}".format(pending.result_dir),
        ]
        errmsg = os.linesep.join(errmsg_lines)
        logger.error("pychestrate-client: %s", errmsg)

        rtnval = _record_failed_attempt(
            pending=pending, attempts=attempts, attempted_at=attempted_at,
            tree_delivered=False, manifest_posted=False, file_count=0,
            detail=errmsg, aspects=aspects, delivered_files=None,
            summary_counts=None)
        return rtnval

    store_driver = aspects.store_driver
    if store_driver is None:
        store_driver = PresignedUploadDriver(
            client=client, chunk_size=aspects.artifact_chunk_size)

    if pending.tree_delivered:
        logger.info(
            "pychestrate-client: result-tree bytes for run_id=%s already "
            "landed on a previous attempt; only the completion manifest is "
            "still owed.", run_id,
        )
        if pending.delivered_files is not None:
            entries = _entries_from_file_index(
                result_dir=pending.result_dir,
                file_index=pending.delivered_files)
        else:
            entries = walk_result_tree(result_dir=pending.result_dir)
        manifest_counts = pending.summary_counts
        tree_uploaded = True
    else:
        entries = walk_result_tree(result_dir=pending.result_dir)
        manifest_counts = None
        tree_uploaded = upload_result_tree(
            run_id=run_id, entries=entries,
            backend_driver=pending.backend_driver,
            backend_key_prefix=pending.backend_key_prefix,
            store_driver=store_driver)

    if not tree_uploaded:
        detail = ("Result-tree byte transfer did not complete on attempt "
                  "{}; no completion manifest was posted because the "
                  "artifacts are not on the server.".format(attempts))
        rtnval = _record_failed_attempt(
            pending=pending, attempts=attempts, attempted_at=attempted_at,
            tree_delivered=False, manifest_posted=False,
            file_count=len(entries), detail=detail, aspects=aspects,
            delivered_files=None, summary_counts=None)
        return rtnval

    if manifest_counts is None and pending.kind == UPLOAD_KIND_TESTBASE:
        manifest_counts = _read_summary_counts(result_dir=pending.result_dir)

    manifest = _build_manifest_for_kind(
        kind=pending.kind, run_id=run_id, client_id=pending.client_id,
        terminal_outcome=terminal_outcome,
        backend_driver=pending.backend_driver,
        backend_key_prefix=pending.backend_key_prefix,
        result_dir=pending.result_dir, entries=entries,
        counts=manifest_counts,
        failure_classification=pending.failure_classification)

    try:
        post_results_complete(client=client, run_id=run_id, manifest=manifest)
    except Exception as posterr:
        detail = ("Result tree delivered but the results/complete POST "
                  "failed on attempt {}: {}".format(attempts, posterr))
        logger.error("pychestrate-client: %s", detail)
        delivered_files = [entry.as_dict() for entry in entries]
        rtnval = _record_failed_attempt(
            pending=pending, attempts=attempts, attempted_at=attempted_at,
            tree_delivered=True, manifest_posted=False,
            file_count=len(entries), detail=detail, aspects=aspects,
            delivered_files=delivered_files,
            summary_counts=manifest_counts)
        return rtnval

    detail = ("Result tree uploaded and results/complete accepted on "
              "attempt {}.".format(attempts))

    outcome = ResultUploadOutcome(
        status=UPLOAD_STATUS_COMPLETE, manifest_posted=True,
        tree_uploaded=True, file_count=len(entries), detail=detail)

    record_path = _write_upload_record(
        record_path=pending.record_path, run_id=run_id,
        upload_outcome=outcome, pending=None)
    logger.info(
        "pychestrate-client: recorded %s-upload outcome (status=%s) at %s",
        pending.kind, UPLOAD_STATUS_COMPLETE, record_path,
    )

    rtnval = outcome
    return rtnval


def _record_failed_attempt(*, pending: PendingUpload, attempts: int,
                           attempted_at: datetime, tree_delivered: bool,
                           manifest_posted: bool, file_count: int,
                           detail: str,
                           aspects: ResultUploaderAspects,
                           delivered_files: Optional[List[Dict[str, Any]]],
                           summary_counts: Optional[Dict[str, int]]
                           ) -> ResultUploadOutcome:
    """
        Records a failed delivery attempt onto the local record and returns
        the matching :class:`ResultUploadOutcome`.

        The refreshed :class:`PendingUpload` carries the attempt count, the
        failure detail, and the moment the next attempt becomes due, so the
        schedule survives a client restart. When the aspects' attempt cap is
        reached the descriptor is marked ``abandoned`` and no further
        attempt is made; the ``upload_failed`` status stays on the record so
        an operator can still see the results were never delivered.

        :param pending: The :class:`PendingUpload` that was attempted.
        :param attempts: The attempt number that just failed.
        :param attempted_at: The moment the attempt was made.
        :param tree_delivered: Whether the artifact bytes landed.
        :param manifest_posted: Whether the completion manifest was
                                accepted.
        :param file_count: The number of files in the walked tree.
        :param detail: The human-readable failure detail.
        :param aspects: Behavior-shaping aspects supplying the schedule.
        :param delivered_files: The file index that actually reached the
                                object store, or ``None`` when nothing
                                landed on this attempt.
        :param summary_counts: The testbase counters captured alongside
                               ``delivered_files``, or ``None``.

        :returns: A :class:`ResultUploadOutcome` for the failed attempt.
    """
    attempted_at_text = attempted_at.isoformat()

    first_failed_at = pending.first_failed_at
    if first_failed_at is None:
        first_failed_at = attempted_at_text

    abandoned = False
    if aspects.max_upload_attempts > 0 and \
            attempts >= aspects.max_upload_attempts:
        abandoned = True

    next_attempt_at = _compute_next_attempt_at(
        attempts=attempts, aspects=aspects, now=attempted_at)

    refreshed = PendingUpload(
        kind=pending.kind, run_id=pending.run_id,
        client_id=pending.client_id, result_dir=pending.result_dir,
        run_succeeded=pending.run_succeeded,
        backend_driver=pending.backend_driver,
        backend_key_prefix=pending.backend_key_prefix,
        record_path=pending.record_path, tree_delivered=tree_delivered,
        attempts=attempts, first_failed_at=first_failed_at,
        last_attempt_at=attempted_at_text, next_attempt_at=next_attempt_at,
        last_error=detail, abandoned=abandoned,
        delivered_files=delivered_files, summary_counts=summary_counts,
        terminal_outcome=pending.terminal_outcome,
        failure_classification=pending.failure_classification)

    outcome = ResultUploadOutcome(
        status=UPLOAD_STATUS_FAILED, manifest_posted=manifest_posted,
        tree_uploaded=tree_delivered, file_count=file_count, detail=detail)

    record_path = _write_upload_record(
        record_path=pending.record_path, run_id=pending.run_id,
        upload_outcome=outcome, pending=refreshed)

    if abandoned:
        errmsg_lines = [
            "A run's results were abandoned after exhausting the delivery "
            "attempt cap. The results remain on this pod and were never "
            "reported to the service.",
            "    kind: {}".format(pending.kind),
            "    run_id: {}".format(pending.run_id),
            "    result_dir: {}".format(pending.result_dir),
            "    attempts: {}".format(attempts),
            "    last_error: {}".format(detail),
            "    record: {}".format(record_path),
        ]
        errmsg = os.linesep.join(errmsg_lines)
        logger.error("pychestrate-client: %s", errmsg)
    else:
        logger.warning(
            "pychestrate-client: recorded %s-upload failure (attempt=%d, "
            "next_attempt_at=%s) at %s",
            pending.kind, attempts, refreshed.next_attempt_at, record_path,
        )

    rtnval = outcome
    return rtnval


def _upload_job_results(*, client: PychestrateMtlsClient, state: ClientState,
                        kind: str, envelope: Dict[str, Any], result_dir: str,
                        run_succeeded: bool,
                        aspects: ResultUploaderAspects,
                        terminal_outcome: Optional[str],
                        failure_classification: Optional[str]
                        ) -> ResultUploadOutcome:
    """
        Performs the first delivery attempt for a freshly finished job.

        The verified job envelope is distilled into a
        :class:`PendingUpload` (which is what a later retry works from,
        since the envelope itself is not kept) and handed to
        :func:`deliver_pending_upload`. The caller-supplied outcome and
        failure classification travel on that descriptor, so a retry of a
        cancelled run still reports it as cancelled.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param state: The :class:`ClientState`.
        :param kind: The job kind.
        :param envelope: The verified job envelope dict (source of the
                         ``JobResultsLocator``).
        :param result_dir: The absolute path of the result-tree root.
        :param run_succeeded: Whether the job itself succeeded.
        :param aspects: Behavior-shaping aspects.
        :param terminal_outcome: An explicit ``TerminalOutcome``, or
                                 ``None`` to derive one from
                                 ``run_succeeded``.
        :param failure_classification: The structured reason the run did not
                                       succeed, or ``None``.

        :returns: A :class:`ResultUploadOutcome`.
        :raises: SemanticError when ``terminal_outcome`` is an empty string.
    """
    if terminal_outcome is not None and len(terminal_outcome) == 0:
        errmsg_lines = [
            "The result uploader was handed an empty 'terminal_outcome'. "
            "Pass None to derive the outcome from 'run_succeeded', or a "
            "TerminalOutcome value.",
            "    kind: {}".format(kind),
            "    run_id: {}".format(envelope.get("run_id")),
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise SemanticError(errmsg)

    run_id = str(envelope.get("run_id", ""))
    client_id = str(envelope.get("client_id", ""))

    locator = _extract_results_locator(envelope=envelope)

    record_path = _resolve_record_path(
        state=state, kind=kind, run_id=run_id)

    pending = PendingUpload(
        kind=kind, run_id=run_id, client_id=client_id,
        result_dir=result_dir, run_succeeded=run_succeeded,
        backend_driver=locator["backend_driver"],
        backend_key_prefix=locator["backend_key_prefix"],
        record_path=record_path, terminal_outcome=terminal_outcome,
        failure_classification=failure_classification)

    rtnval = deliver_pending_upload(
        client=client, state=state, pending=pending, aspects=aspects)
    return rtnval


def upload_run_results(*, client: PychestrateMtlsClient, state: ClientState,
                        envelope: Dict[str, Any], result_dir: str,
                        run_succeeded: bool,
                        aspects: ResultUploaderAspects =
                        DEFAULT_RESULT_UPLOADER_ASPECTS,
                        terminal_outcome: Optional[str] = None,
                        failure_classification: Optional[str] = None
                        ) -> ResultUploadOutcome:
    """
        Walks a finished testbase result tree, uploads it to the shared
        results object store, posts the ``ResultManifest`` to
        ``results/complete``, and records the upload outcome on the local run
        record.

        The completion manifest is posted only when the whole result tree
        transferred. When the transfer fails, the run is recorded as
        ``upload_failed`` together with a :class:`PendingUpload` retry
        descriptor and nothing is posted -- the service is never told a run
        is complete against artifacts that never arrived.
        :func:`retry_pending_uploads` finishes the job later.

        The function is resilient: a failed byte transfer or a failed
        completion POST is captured as an ``upload_failed``
        :class:`ResultUploadOutcome` rather than raised -- the client
        process must not crash on a bad upload.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param state: The :class:`ClientState`.
        :param envelope: The verified job envelope dict (source of the
                         ``JobResultsLocator``).
        :param result_dir: The absolute path of the testbase result-tree
                           root directory.
        :param run_succeeded: Whether the testbase run itself succeeded
                              (drives the manifest ``terminal_outcome``).
        :param aspects: Behavior-shaping aspects.
        :param terminal_outcome: An explicit ``TerminalOutcome`` value used
                                 verbatim on the manifest. ``None`` (the
                                 default) preserves the historical behavior
                                 of deriving ``success`` / ``failure`` from
                                 ``run_succeeded``. Pass ``"cancelled"`` for
                                 a run the client cancelled -- a
                                 cancellation is not a failure. The value is
                                 NOT validated against a fixed pair; the
                                 service's ``TerminalOutcome`` enum is the
                                 authority.
        :param failure_classification: The structured reason the run did not
                                       succeed (``timeout``,
                                       ``executable_not_found``,
                                       ``nonzero_exit``, ``cancelled``,
                                       ``launch_error``, ``setup_error``).
                                       When supplied it is carried on the
                                       manifest under
                                       ``extras.failure_classification``;
                                       ``None`` (the default) omits the
                                       member entirely.

        :returns: A :class:`ResultUploadOutcome`.
    """
    rtnval = _upload_job_results(
        client=client, state=state, kind=UPLOAD_KIND_TESTBASE,
        envelope=envelope, result_dir=result_dir,
        run_succeeded=run_succeeded, aspects=aspects,
        terminal_outcome=terminal_outcome,
        failure_classification=failure_classification)
    return rtnval


def build_build_result_manifest(*, run_id: str, client_id: str,
                                 terminal_outcome: str, backend_driver: str,
                                 backend_key_prefix: str,
                                 entries: List[ResultTreeEntry],
                                 notes: Optional[str] = None
                                 ) -> Resultmanifest:
    """
        Builds the ``ResultManifest`` describing an uploaded build artifact
        tree.

        A build artifact tree carries no per-test result records, so this
        manifest sets ``format_tag`` to :data:`BUILD_RESULT_FORMAT_TAG` and
        leaves the testbase-specific ``summary_artifact_path`` / ``tests_*``
        fields unset. The walked file index is still carried under
        ``extras`` for server-side reconciliation.

        :param run_id: The server-assigned run UUID.
        :param client_id: The assigned client UUID.
        :param terminal_outcome: A ``TerminalOutcome`` enum value
                                 (``success`` / ``failure``).
        :param backend_driver: The object-store driver name from the
                               envelope's ``JobResultsLocator``.
        :param backend_key_prefix: The per-run key prefix from the
                                   envelope's ``JobResultsLocator``.
        :param entries: The walked artifact-tree entries (carried as an
                        ``extras`` index for server-side reconciliation).
        :param notes: Optional free-form note.

        :returns: A populated :class:`Resultmanifest`.
    """
    file_index = [entry.as_dict() for entry in entries]

    manifest = Resultmanifest(
        run_id=run_id,
        client_id=client_id,
        format_tag=BUILD_RESULT_FORMAT_TAG,
        backend_key_prefix=backend_key_prefix,
        completed_at=_now_rfc3339(),
        terminal_outcome=terminal_outcome,
    )
    manifest.backend_driver = backend_driver
    manifest.extras = {
        "file_count": len(entries),
        "files": file_index,
    }
    if notes is not None and len(notes) > 0:
        manifest.notes = notes

    return manifest


def mark_build_upload_status(state: ClientState, run_id: str,
                             upload_outcome: ResultUploadOutcome,
                             pending: Optional[PendingUpload] = None) -> str:
    """
        Records the artifact-upload outcome onto the local build record so
        a failed upload can be retried.

        This is the build-tree counterpart of :func:`mark_run_upload_status`
        -- it merges an ``upload`` section (and an ``upload_retry`` section
        when a retry is owed) into the
        ``<state>/builds/<run_id>/build-record.json`` file the build
        executor wrote.

        :param state: The :class:`ClientState`.
        :param run_id: The server-assigned run UUID.
        :param upload_outcome: The :class:`ResultUploadOutcome` to record.
        :param pending: The :class:`PendingUpload` describing the retry that
                        is owed. ``None`` clears any retry descriptor a
                        previous failure left behind.

        :returns: The absolute path of the build-record file.
    """
    record_path = _resolve_record_path(
        state=state, kind=UPLOAD_KIND_BUILD, run_id=run_id)

    rtnval = _write_upload_record(
        record_path=record_path, run_id=run_id,
        upload_outcome=upload_outcome, pending=pending)
    return rtnval


def upload_build_results(*, client: PychestrateMtlsClient, state: ClientState,
                          envelope: Dict[str, Any], result_dir: str,
                          run_succeeded: bool,
                          aspects: ResultUploaderAspects =
                          DEFAULT_RESULT_UPLOADER_ASPECTS,
                          terminal_outcome: Optional[str] = None,
                          failure_classification: Optional[str] = None
                          ) -> ResultUploadOutcome:
    """
        Walks a finished build artifact tree, uploads it to the shared
        results object store, posts the build ``ResultManifest`` to
        ``results/complete``, and records the upload outcome on the local
        build record.

        The byte transfer reuses the M4 :class:`PresignedUploadDriver`
        without change -- a build's output directory is just a directory
        tree of files, exactly what the presigned-URL upload protocol
        transfers. Only the manifest differs (build-flavored
        ``format_tag``, no test counters), built by
        :func:`build_build_result_manifest`.

        The completion manifest is posted only when the whole artifact tree
        transferred. When the transfer fails, the build is recorded as
        ``upload_failed`` together with a :class:`PendingUpload` retry
        descriptor and nothing is posted -- the service is never told a
        build is complete against artifacts that never arrived.
        :func:`retry_pending_uploads` finishes the job later.

        The function is resilient: a failed byte transfer or a failed
        completion POST is captured as an ``upload_failed``
        :class:`ResultUploadOutcome` rather than raised -- the client
        process must not crash on a bad upload.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param state: The :class:`ClientState`.
        :param envelope: The verified ``job_kind=build`` envelope dict
                         (source of the ``JobResultsLocator``).
        :param result_dir: The absolute path of the build output directory.
        :param run_succeeded: Whether the build itself succeeded (drives
                              the manifest ``terminal_outcome``).
        :param aspects: Behavior-shaping aspects.
        :param terminal_outcome: An explicit ``TerminalOutcome`` value used
                                 verbatim on the manifest. ``None`` (the
                                 default) preserves the historical behavior
                                 of deriving ``success`` / ``failure`` from
                                 ``run_succeeded``. Pass ``"cancelled"`` for
                                 a run the client cancelled -- a
                                 cancellation is not a failure. The value is
                                 NOT validated against a fixed pair; the
                                 service's ``TerminalOutcome`` enum is the
                                 authority.
        :param failure_classification: The structured reason the run did not
                                       succeed (``timeout``,
                                       ``executable_not_found``,
                                       ``nonzero_exit``, ``cancelled``,
                                       ``launch_error``, ``setup_error``).
                                       When supplied it is carried on the
                                       manifest under
                                       ``extras.failure_classification``;
                                       ``None`` (the default) omits the
                                       member entirely.

        :returns: A :class:`ResultUploadOutcome`.
    """
    rtnval = _upload_job_results(
        client=client, state=state, kind=UPLOAD_KIND_BUILD,
        envelope=envelope, result_dir=result_dir,
        run_succeeded=run_succeeded, aspects=aspects,
        terminal_outcome=terminal_outcome,
        failure_classification=failure_classification)
    return rtnval


def build_provision_result_manifest(*, run_id: str, client_id: str,
                                     terminal_outcome: str,
                                     backend_driver: str,
                                     backend_key_prefix: str,
                                     entries: List[ResultTreeEntry],
                                     notes: Optional[str] = None
                                     ) -> Resultmanifest:
    """
        Builds the ``ResultManifest`` describing an uploaded provision
        output tree.

        A provision output tree carries no per-test result records, so this
        manifest sets ``format_tag`` to :data:`PROVISION_RESULT_FORMAT_TAG`
        and leaves the testbase-specific ``summary_artifact_path`` /
        ``tests_*`` fields unset. The walked file index is still carried
        under ``extras`` for server-side reconciliation.

        :param run_id: The server-assigned run UUID.
        :param client_id: The assigned client UUID.
        :param terminal_outcome: A ``TerminalOutcome`` enum value
                                 (``success`` / ``failure``).
        :param backend_driver: The object-store driver name from the
                               envelope's ``JobResultsLocator``.
        :param backend_key_prefix: The per-run key prefix from the
                                   envelope's ``JobResultsLocator``.
        :param entries: The walked output-tree entries (carried as an
                        ``extras`` index for server-side reconciliation).
        :param notes: Optional free-form note.

        :returns: A populated :class:`Resultmanifest`.
    """
    file_index = [entry.as_dict() for entry in entries]

    manifest = Resultmanifest(
        run_id=run_id,
        client_id=client_id,
        format_tag=PROVISION_RESULT_FORMAT_TAG,
        backend_key_prefix=backend_key_prefix,
        completed_at=_now_rfc3339(),
        terminal_outcome=terminal_outcome,
    )
    manifest.backend_driver = backend_driver
    manifest.extras = {
        "file_count": len(entries),
        "files": file_index,
    }
    if notes is not None and len(notes) > 0:
        manifest.notes = notes

    return manifest


def mark_provision_upload_status(state: ClientState, run_id: str,
                                 upload_outcome: ResultUploadOutcome,
                                 pending: Optional[PendingUpload] = None
                                 ) -> str:
    """
        Records the output-upload outcome onto the local provision record
        so a failed upload can be retried.

        This is the provision-tree counterpart of
        :func:`mark_build_upload_status` -- it merges an ``upload`` section
        (and an ``upload_retry`` section when a retry is owed) into the
        ``<state>/provisioning/<run_id>/provision-record.json`` file the
        provision executor wrote.

        :param state: The :class:`ClientState`.
        :param run_id: The server-assigned run UUID.
        :param upload_outcome: The :class:`ResultUploadOutcome` to record.
        :param pending: The :class:`PendingUpload` describing the retry that
                        is owed. ``None`` clears any retry descriptor a
                        previous failure left behind.

        :returns: The absolute path of the provision-record file.
    """
    record_path = _resolve_record_path(
        state=state, kind=UPLOAD_KIND_PROVISION, run_id=run_id)

    rtnval = _write_upload_record(
        record_path=record_path, run_id=run_id,
        upload_outcome=upload_outcome, pending=pending)
    return rtnval


def upload_provision_results(*, client: PychestrateMtlsClient,
                              state: ClientState, envelope: Dict[str, Any],
                              result_dir: str, run_succeeded: bool,
                              aspects: ResultUploaderAspects =
                              DEFAULT_RESULT_UPLOADER_ASPECTS,
                              terminal_outcome: Optional[str] = None,
                              failure_classification: Optional[str] = None
                              ) -> ResultUploadOutcome:
    """
        Walks a finished provision output tree, uploads it to the shared
        results object store, posts the provision ``ResultManifest`` to
        ``results/complete``, and records the upload outcome on the local
        provision record.

        The byte transfer reuses the M4 :class:`PresignedUploadDriver`
        without change -- a provision run's output directory is just a
        directory tree of files (captured stdout/stderr, state files, plan
        output), exactly what the presigned-URL upload protocol transfers.
        Only the manifest differs (provision-flavored ``format_tag``, no
        test counters), built by :func:`build_provision_result_manifest`.

        The completion manifest is posted only when the whole output tree
        transferred. When the transfer fails, the provision run is recorded
        as ``upload_failed`` together with a :class:`PendingUpload` retry
        descriptor and nothing is posted -- the service is never told a
        provision run is complete against artifacts that never arrived.
        :func:`retry_pending_uploads` finishes the job later.

        The function is resilient: a failed byte transfer or a failed
        completion POST is captured as an ``upload_failed``
        :class:`ResultUploadOutcome` rather than raised -- the client
        process must not crash on a bad upload.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param state: The :class:`ClientState`.
        :param envelope: The verified ``job_kind=provision`` envelope dict
                         (source of the ``JobResultsLocator``).
        :param result_dir: The absolute path of the provision output
                           directory.
        :param run_succeeded: Whether the provision run itself succeeded
                              (drives the manifest ``terminal_outcome``).
        :param aspects: Behavior-shaping aspects.
        :param terminal_outcome: An explicit ``TerminalOutcome`` value used
                                 verbatim on the manifest. ``None`` (the
                                 default) preserves the historical behavior
                                 of deriving ``success`` / ``failure`` from
                                 ``run_succeeded``. Pass ``"cancelled"`` for
                                 a run the client cancelled -- a
                                 cancellation is not a failure. The value is
                                 NOT validated against a fixed pair; the
                                 service's ``TerminalOutcome`` enum is the
                                 authority.
        :param failure_classification: The structured reason the run did not
                                       succeed (``timeout``,
                                       ``executable_not_found``,
                                       ``nonzero_exit``, ``cancelled``,
                                       ``launch_error``, ``setup_error``).
                                       When supplied it is carried on the
                                       manifest under
                                       ``extras.failure_classification``;
                                       ``None`` (the default) omits the
                                       member entirely.

        :returns: A :class:`ResultUploadOutcome`.
    """
    rtnval = _upload_job_results(
        client=client, state=state, kind=UPLOAD_KIND_PROVISION,
        envelope=envelope, result_dir=result_dir,
        run_succeeded=run_succeeded, aspects=aspects,
        terminal_outcome=terminal_outcome,
        failure_classification=failure_classification)
    return rtnval


def find_pending_uploads(state: ClientState) -> List[PendingUpload]:
    """
        Scans the client state directory for every run, build and provision
        record that still owes the service its results.

        A record qualifies when its ``upload`` section is marked
        ``upload_failed`` and it carries a usable ``upload_retry``
        descriptor that has not been abandoned. The scan is tolerant: an
        unreadable or half-written record is logged and skipped rather than
        stopping the sweep, because one bad record must not block every
        other run's results from being delivered.

        :param state: The :class:`ClientState`.

        :returns: The pending uploads found, sorted by the moment their next
                  attempt is due so the longest-waiting results go first.
    """
    rtnval: List[PendingUpload] = []

    for kind in UPLOAD_KINDS:
        if kind == UPLOAD_KIND_TESTBASE:
            kind_dir = state.runs_dir
        elif kind == UPLOAD_KIND_BUILD:
            kind_dir = os.path.join(state.state_dir, DIRNAME_BUILDS)
        else:
            kind_dir = os.path.join(state.state_dir, DIRNAME_PROVISIONING)

        if not os.path.isdir(kind_dir):
            continue

        record_filename = RECORD_FILENAME_BY_KIND[kind]

        for entry_name in sorted(os.listdir(kind_dir)):
            record_path = os.path.join(kind_dir, entry_name, record_filename)
            if not os.path.isfile(record_path):
                continue

            record = _read_local_record(record_path=record_path)

            upload = record.get(RECORD_SECTION_UPLOAD, None)
            if not isinstance(upload, dict):
                continue

            if upload.get("status", None) != UPLOAD_STATUS_FAILED:
                continue

            pending = _pending_from_record(
                record=record, record_path=record_path)
            if pending is None:
                continue

            if pending.abandoned:
                continue

            rtnval.append(pending)

    rtnval.sort(key=lambda nxtpending: str(nxtpending.next_attempt_at))
    return rtnval


def retry_pending_uploads(*, client: PychestrateMtlsClient,
                          state: ClientState,
                          aspects: ResultUploaderAspects =
                          DEFAULT_RESULT_UPLOADER_ASPECTS,
                          now: Optional[datetime] = None
                          ) -> List[ResultUploadOutcome]:
    """
        Re-attempts delivery of every run whose results are still stranded
        on this pod, and returns the outcome of each attempt.

        THIS FUNCTION MUST BE CALLED FROM THE CLIENT'S PERIODIC LOOP. It is
        the only thing that rescues results after a transient failure: an
        upload that fails during :func:`upload_run_results` (or its build /
        provision siblings) leaves a :class:`PendingUpload` descriptor on
        the local record and returns; without a periodic caller, those
        results sit on the pod's disk forever and the service never learns
        the run finished. The dispatch-consumer loop is the intended caller
        -- one call per iteration, before polling for the next dispatch.

        Only uploads whose backoff has elapsed are attempted; the rest are
        left for a later pass. The retry schedule is caller-controlled
        through ``aspects`` and is persisted on the record, so it survives a
        client restart.

        The sweep is non-raising. A pending upload whose attempt blows up
        unexpectedly is logged and skipped so one poisoned record cannot
        take down the client's loop or block the other pending uploads.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param state: The :class:`ClientState`.
        :param aspects: Behavior-shaping aspects (store driver, streaming
                        chunk size, retry schedule).
        :param now: The moment to evaluate the retry schedule against.
                    Defaults to the current UTC time.

        :returns: One :class:`ResultUploadOutcome` per attempted upload.
    """
    if now is None:
        now = datetime.now(timezone.utc)

    rtnval: List[ResultUploadOutcome] = []

    pending_uploads = find_pending_uploads(state=state)
    if len(pending_uploads) == 0:
        return rtnval

    for pending in pending_uploads:
        if not pending.is_due(now=now):
            logger.debug(
                "pychestrate-client: pending %s upload for run_id=%s is not "
                "due until %s; skipping this pass.",
                pending.kind, pending.run_id, pending.next_attempt_at,
            )
            continue

        logger.info(
            "pychestrate-client: retrying stranded %s results for run_id=%s "
            "(attempt %d, tree_delivered=%s).",
            pending.kind, pending.run_id, pending.attempts + 1,
            pending.tree_delivered,
        )

        try:
            outcome = deliver_pending_upload(
                client=client, state=state, pending=pending, aspects=aspects)
        except Exception as retryerr:
            logger.error(
                "pychestrate-client: retrying the pending %s upload for "
                "run_id=%s raised an unexpected exception; the record at %s "
                "is left for the next pass: %s",
                pending.kind, pending.run_id, pending.record_path, retryerr,
            )
            continue

        rtnval.append(outcome)

    return rtnval

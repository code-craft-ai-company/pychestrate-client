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
                   A genuine failure (network error, presign 5xx) marks the
                   local run record ``upload_failed`` so the next ``run``
                   invocation can retry. The ``results/complete`` POST is
                   fully implemented.

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


from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import hashlib
import json
import logging
import os

from datetime import datetime, timezone

from ccai.errors.exceptions import HTTPRequestError

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

#: ``format_tag`` stamped on the ``ResultManifest`` for an uploaded RPA
#: job result tree (``job_kind=rpa``). The server's result-ingest
#: completion path branches on this tag.
RPA_RESULT_FORMAT_TAG = "rpa.result.v1"

#: Canonical top-level files an ``codecraftai-testbase`` result tree contains.
TESTBASE_SUMMARY_FILENAME = "testrun_summary.json"
TESTBASE_RESULTS_STREAM_FILENAME = "testrun_results_stream.jsos"
TESTBASE_RESULTS_FILENAME = "testrun_results.json"
TESTBASE_HTML_SUMMARY_FILENAME = "testsummary.html"

#: ``TerminalOutcome`` enum values from the OpenAPI spec.
TERMINAL_OUTCOME_SUCCESS = "success"
TERMINAL_OUTCOME_FAILURE = "failure"
#: Terminal outcome for a run an operator cancel stopped; the service keeps
#: it out of FAILED.
TERMINAL_OUTCOME_CANCELLED = "cancelled"

#: Local run-record upload-status markers.
UPLOAD_STATUS_COMPLETE = "upload_complete"
UPLOAD_STATUS_FAILED = "upload_failed"


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
        behavior for the result uploader (the driver used to transfer the
        result tree).

        Following the project's Aspects pattern, behavior modification is
        carried as data on a single parameter rather than via decorators.
    """

    def __init__(self, *, store_driver: Optional[ResultStoreDriver] = None):
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
        yields an empty list rather than an error. Python bytecode (``__pycache__``
        directories and ``.pyc`` files) is not a result and is left out.

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

    for current_dir, subdirs, filenames in os.walk(result_dir):
        # testbase leaves Python bytecode of its generated run sequence in the output directory; it is an artifact of the interpreter, not a result,
        # and listed beside the real files it only confuses the run page.
        subdirs[:] = [name for name in subdirs if name != "__pycache__"]
        for filename in filenames:
            if filename.endswith(".pyc"):
                continue
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
            2. PUT each artifact's bytes to its ``put_url``, sending the
               target's ``headers`` verbatim.

        The presign request flows over the client's mTLS-authenticated REST
        client. The PUT reuses that same authenticated
        :class:`requests.Session` because the local-filesystem fallback
        ``put_url`` points back at an mTLS-gated upload sink on the service;
        for an S3-backed deploy the pinned client certificate is simply
        ignored by the object store.
    """

    def __init__(self, *, client: PychestrateMtlsClient):
        """
            Initializes a :class:`PresignedUploadDriver`.

            :param client: The configured :class:`PychestrateMtlsClient`
                           used for both the ``:presign`` request and the
                           artifact PUTs.
        """
        self._client = client
        return

    @property
    def client(self) -> PychestrateMtlsClient:
        """
            Gets the mTLS client this driver transfers through.

            :returns: The :class:`PychestrateMtlsClient`.
        """
        return self._client

    def _presign_paths(self, run_id: str,
                       relative_paths: List[str]) -> List[Dict[str, Any]]:
        """
            Calls ``POST /api/v1/runs/{run_id}/artifacts:presign`` for the
            supplied relative paths and returns the minted upload targets.

            :param run_id: The server-assigned run UUID.
            :param relative_paths: The relative artifact paths to presign.

            :returns: A list of upload-target dicts (``path`` / ``put_url``
                      / ``headers`` / ``expires_at``).
            :raises: requests.RequestException, codecraftai HTTP error types
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
                      local_path: str) -> None:
        """
            PUTs one artifact's bytes to a presigned ``put_url``.

            :param target: The upload-target dict for the artifact.
            :param local_path: The absolute path of the local file.

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

        # The service's local-filesystem backend mints service-rooted URLs
        # ("/api/v1/runs/{id}/artifacts/_local-upload?..."); an object-store
        # backend mints absolute ones. urljoin resolves the former against
        # the service origin and leaves the latter untouched.
        origin_url = getattr(self._client, "origin_url", None)
        if origin_url is not None and not put_url.lower().startswith(("http://", "https://")):
            put_url = urljoin(origin_url + "/", put_url)

        if self._client._session is None:
            self._client.authenticate()
        session = self._client._session

        with open(local_path, "rb") as artf:
            artifact_bytes = artf.read()

        response = session.put(put_url, data=artifact_bytes, headers=headers)

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
            :raises: requests.RequestException, codecraftai HTTP error types
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

            self._put_artifact(target=target, local_path=entry.absolute_path)

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
        :raises: requests.RequestException, codecraftai HTTP error types
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
                        run_cancelled: bool = False,
                        run_notes: Optional[str] = None,
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
        :param run_notes: Why the job failed or was stopped, sent as the
                          manifest's ``notes`` so the run detail can say.
        :param run_cancelled: True when an operator cancel stopped the job
                              (manifest outcome ``cancelled``).
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

    if run_cancelled:
        terminal_outcome = TERMINAL_OUTCOME_CANCELLED
    elif run_succeeded:
        terminal_outcome = TERMINAL_OUTCOME_SUCCESS
    else:
        terminal_outcome = TERMINAL_OUTCOME_FAILURE

    entries = walk_result_tree(result_dir=result_dir)
    counts = _read_summary_counts(result_dir=result_dir)

    store_driver = aspects.store_driver
    if store_driver is None:
        store_driver = PresignedUploadDriver(client=client)

    tree_uploaded = upload_result_tree(
        run_id=run_id, entries=entries, backend_driver=backend_driver,
        backend_key_prefix=backend_key_prefix,
        store_driver=store_driver)

    notes = run_notes if run_notes else None
    if not tree_uploaded:
        upload_note = ("Result-tree byte transfer incomplete; manifest posted for "
                 "reconciliation. See client-side run record.")
        notes = upload_note if notes is None else "{} {}".format(notes, upload_note)

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


#: Sub-directory (under the client state directory) holding per-build
#: local execution records. Mirrors :mod:`build_executor.DIRNAME_BUILDS`.
DIRNAME_BUILDS = "builds"


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
                             upload_outcome: ResultUploadOutcome) -> str:
    """
        Records the artifact-upload outcome onto the local build record so
        a failed upload can be retried on the next ``run`` invocation.

        This is the build-tree counterpart of :func:`mark_run_upload_status`
        -- it merges an ``upload`` section into the
        ``<state>/builds/<run_id>/build-record.json`` file the build
        executor wrote.

        :param state: The :class:`ClientState`.
        :param run_id: The server-assigned run UUID.
        :param upload_outcome: The :class:`ResultUploadOutcome` to record.

        :returns: The absolute path of the build-record file.
    """
    safe_run_id = run_id if len(run_id) > 0 else "unknown-build"
    record_dir = os.path.join(state.state_dir, DIRNAME_BUILDS, safe_run_id)
    os.makedirs(record_dir, mode=0o755, exist_ok=True)

    record_path = os.path.join(record_dir, "build-record.json")

    record: Dict[str, Any] = {}
    if os.path.isfile(record_path):
        try:
            with open(record_path, "r", encoding="utf-8") as recf:
                loaded = json.load(recf)
            if isinstance(loaded, dict):
                record = loaded
        except (ValueError, OSError) as readerr:
            logger.warning(
                "pychestrate-client: could not read build record %s, "
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


def upload_build_results(*, client: PychestrateMtlsClient, state: ClientState,
                          envelope: Dict[str, Any], result_dir: str,
                          run_succeeded: bool,
                        run_cancelled: bool = False,
                        run_notes: Optional[str] = None,
                          aspects: ResultUploaderAspects =
                          DEFAULT_RESULT_UPLOADER_ASPECTS
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

        The function is resilient: a failed byte transfer or a failed
        completion POST is captured as an ``upload_failed``
        :class:`ResultUploadOutcome` (and marked on the local build record
        for retry) rather than raised -- the client process must not crash
        on a bad upload.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param state: The :class:`ClientState`.
        :param envelope: The verified ``job_kind=build`` envelope dict
                         (source of the ``JobResultsLocator``).
        :param result_dir: The absolute path of the build output directory.
        :param run_notes: Why the job failed or was stopped, sent as the
                          manifest's ``notes`` so the run detail can say.
        :param run_cancelled: True when an operator cancel stopped the job
                              (manifest outcome ``cancelled``).
        :param run_succeeded: Whether the build itself succeeded (drives
                              the manifest ``terminal_outcome``).
        :param aspects: Behavior-shaping aspects.

        :returns: A :class:`ResultUploadOutcome`.
    """
    run_id = str(envelope.get("run_id", ""))
    client_id = str(envelope.get("client_id", ""))

    locator = _extract_results_locator(envelope=envelope)
    backend_key_prefix = locator["backend_key_prefix"]
    backend_driver = locator["backend_driver"]

    if run_cancelled:
        terminal_outcome = TERMINAL_OUTCOME_CANCELLED
    elif run_succeeded:
        terminal_outcome = TERMINAL_OUTCOME_SUCCESS
    else:
        terminal_outcome = TERMINAL_OUTCOME_FAILURE

    entries = walk_result_tree(result_dir=result_dir)

    store_driver = aspects.store_driver
    if store_driver is None:
        store_driver = PresignedUploadDriver(client=client)

    tree_uploaded = upload_result_tree(
        run_id=run_id, entries=entries, backend_driver=backend_driver,
        backend_key_prefix=backend_key_prefix,
        store_driver=store_driver)

    notes = run_notes if run_notes else None
    if not tree_uploaded:
        upload_note = ("Build artifact-tree byte transfer incomplete; manifest "
                 "posted for reconciliation. See client-side build record.")
        notes = upload_note if notes is None else "{} {}".format(notes, upload_note)

    manifest = build_build_result_manifest(
        run_id=run_id, client_id=client_id, terminal_outcome=terminal_outcome,
        backend_driver=backend_driver, backend_key_prefix=backend_key_prefix,
        entries=entries, notes=notes)

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
            detail = ("Build artifact tree uploaded and results/complete "
                      "accepted.")
    else:
        status = UPLOAD_STATUS_FAILED
        if detail is None:
            detail = ("Build artifact-tree byte transfer did not complete; "
                      "build marked for upload retry.")

    outcome = ResultUploadOutcome(
        status=status, manifest_posted=manifest_posted,
        tree_uploaded=tree_uploaded, file_count=len(entries), detail=detail)

    record_path = mark_build_upload_status(
        state=state, run_id=run_id, upload_outcome=outcome)
    logger.info(
        "pychestrate-client: recorded build-upload outcome (status=%s) at "
        "%s", status, record_path,
    )

    return outcome


#: Sub-directory (under the client state directory) holding per-provision
#: local execution records. Mirrors
#: :mod:`provision_executor.DIRNAME_PROVISIONING`.
DIRNAME_PROVISIONING = "provisioning"


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
                                 upload_outcome: ResultUploadOutcome) -> str:
    """
        Records the output-upload outcome onto the local provision record
        so a failed upload can be retried on the next ``run`` invocation.

        This is the provision-tree counterpart of
        :func:`mark_build_upload_status` -- it merges an ``upload`` section
        into the ``<state>/provisioning/<run_id>/provision-record.json``
        file the provision executor wrote.

        :param state: The :class:`ClientState`.
        :param run_id: The server-assigned run UUID.
        :param upload_outcome: The :class:`ResultUploadOutcome` to record.

        :returns: The absolute path of the provision-record file.
    """
    safe_run_id = run_id if len(run_id) > 0 else "unknown-provision"
    record_dir = os.path.join(
        state.state_dir, DIRNAME_PROVISIONING, safe_run_id)
    os.makedirs(record_dir, mode=0o755, exist_ok=True)

    record_path = os.path.join(record_dir, "provision-record.json")

    record: Dict[str, Any] = {}
    if os.path.isfile(record_path):
        try:
            with open(record_path, "r", encoding="utf-8") as recf:
                loaded = json.load(recf)
            if isinstance(loaded, dict):
                record = loaded
        except (ValueError, OSError) as readerr:
            logger.warning(
                "pychestrate-client: could not read provision record %s, "
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


def upload_provision_results(*, client: PychestrateMtlsClient,
                              state: ClientState, envelope: Dict[str, Any],
                              result_dir: str, run_succeeded: bool,
                        run_cancelled: bool = False,
                        run_notes: Optional[str] = None,
                              aspects: ResultUploaderAspects =
                              DEFAULT_RESULT_UPLOADER_ASPECTS
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

        The function is resilient: a failed byte transfer or a failed
        completion POST is captured as an ``upload_failed``
        :class:`ResultUploadOutcome` (and marked on the local provision
        record for retry) rather than raised -- the client process must not
        crash on a bad upload.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param state: The :class:`ClientState`.
        :param envelope: The verified ``job_kind=provision`` envelope dict
                         (source of the ``JobResultsLocator``).
        :param result_dir: The absolute path of the provision output
                           directory.
        :param run_notes: Why the job failed or was stopped, sent as the
                          manifest's ``notes`` so the run detail can say.
        :param run_cancelled: True when an operator cancel stopped the job
                              (manifest outcome ``cancelled``).
        :param run_succeeded: Whether the provision run itself succeeded
                              (drives the manifest ``terminal_outcome``).
        :param aspects: Behavior-shaping aspects.

        :returns: A :class:`ResultUploadOutcome`.
    """
    run_id = str(envelope.get("run_id", ""))
    client_id = str(envelope.get("client_id", ""))

    locator = _extract_results_locator(envelope=envelope)
    backend_key_prefix = locator["backend_key_prefix"]
    backend_driver = locator["backend_driver"]

    if run_cancelled:
        terminal_outcome = TERMINAL_OUTCOME_CANCELLED
    elif run_succeeded:
        terminal_outcome = TERMINAL_OUTCOME_SUCCESS
    else:
        terminal_outcome = TERMINAL_OUTCOME_FAILURE

    entries = walk_result_tree(result_dir=result_dir)

    store_driver = aspects.store_driver
    if store_driver is None:
        store_driver = PresignedUploadDriver(client=client)

    tree_uploaded = upload_result_tree(
        run_id=run_id, entries=entries, backend_driver=backend_driver,
        backend_key_prefix=backend_key_prefix,
        store_driver=store_driver)

    notes = run_notes if run_notes else None
    if not tree_uploaded:
        upload_note = ("Provision output-tree byte transfer incomplete; manifest "
                 "posted for reconciliation. See client-side provision "
                 "record.")
        notes = upload_note if notes is None else "{} {}".format(notes, upload_note)

    manifest = build_provision_result_manifest(
        run_id=run_id, client_id=client_id, terminal_outcome=terminal_outcome,
        backend_driver=backend_driver, backend_key_prefix=backend_key_prefix,
        entries=entries, notes=notes)

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
            detail = ("Provision output tree uploaded and results/complete "
                      "accepted.")
    else:
        status = UPLOAD_STATUS_FAILED
        if detail is None:
            detail = ("Provision output-tree byte transfer did not "
                      "complete; provision marked for upload retry.")

    outcome = ResultUploadOutcome(
        status=status, manifest_posted=manifest_posted,
        tree_uploaded=tree_uploaded, file_count=len(entries), detail=detail)

    record_path = mark_provision_upload_status(
        state=state, run_id=run_id, upload_outcome=outcome)
    logger.info(
        "pychestrate-client: recorded provision-upload outcome (status=%s) "
        "at %s", status, record_path,
    )

    return outcome


#: Sub-directory (under the client state directory) holding per-RPA-job
#: local execution records. Mirrors :mod:`rpa_executor.DIRNAME_RPA`.
DIRNAME_RPA = "rpa"
RPA_RECORD_FILENAME = "rpa-record.json"


def build_rpa_result_manifest(*, run_id: str, client_id: str,
                              terminal_outcome: str, backend_driver: str,
                              backend_key_prefix: str,
                              entries: List[ResultTreeEntry],
                              exit_code: Optional[int] = None,
                              execution_mode: Optional[str] = None,
                              rpa_status: Optional[str] = None,
                              notes: Optional[str] = None) -> Resultmanifest:
    """
        Builds the ``ResultManifest`` describing an uploaded RPA job result
        tree. Like builds, an RPA result tree carries no per-test records;
        ``format_tag`` is :data:`RPA_RESULT_FORMAT_TAG` and the walked file
        index plus the process exit code ride under ``extras``.

        :returns: A populated :class:`Resultmanifest`.
    """
    file_index = [entry.as_dict() for entry in entries]

    manifest = Resultmanifest(
        run_id=run_id,
        client_id=client_id,
        format_tag=RPA_RESULT_FORMAT_TAG,
        backend_key_prefix=backend_key_prefix,
        completed_at=_now_rfc3339(),
        terminal_outcome=terminal_outcome,
    )
    manifest.backend_driver = backend_driver
    manifest.extras = {
        "file_count": len(entries),
        "files": file_index,
        "exit_code": exit_code,
        "execution_mode": execution_mode,
        "rpa_status": rpa_status,
    }
    if notes is not None and len(notes) > 0:
        manifest.notes = notes

    return manifest


def mark_rpa_upload_status(state: ClientState, run_id: str,
                           upload_outcome: ResultUploadOutcome) -> str:
    """
        Records the result-upload outcome onto the local RPA job record so
        a failed upload can be retried.

        :returns: The absolute path of the record file.
    """
    safe_run_id = run_id if len(run_id) > 0 else "unknown-rpa"
    record_dir = os.path.join(state.state_dir, DIRNAME_RPA, safe_run_id)
    os.makedirs(record_dir, mode=0o755, exist_ok=True)

    record_path = os.path.join(record_dir, RPA_RECORD_FILENAME)

    record: Dict[str, Any] = {}
    if os.path.isfile(record_path):
        try:
            with open(record_path, "r", encoding="utf-8") as recf:
                loaded = json.load(recf)
            if isinstance(loaded, dict):
                record = loaded
        except (ValueError, OSError) as readerr:
            logger.warning(
                "pychestrate-client: could not read RPA record %s, rewriting: %s",
                record_path, readerr,
            )
            record = {}

    record["run_id"] = run_id
    record["upload"] = upload_outcome.as_dict()

    with open(record_path, "w", encoding="utf-8") as recf:
        json.dump(record, recf, indent=4, sort_keys=True)
        recf.write(os.linesep)

    rtnval = record_path
    return rtnval


def upload_rpa_results(*, client: PychestrateMtlsClient, state: ClientState,
                       envelope: Dict[str, Any], result_dir: str,
                       run_succeeded: bool,
                       exit_code: Optional[int] = None,
                       execution_mode: Optional[str] = None,
                       rpa_status: Optional[str] = None,
                       aspects: ResultUploaderAspects =
                       DEFAULT_RESULT_UPLOADER_ASPECTS
                       ) -> ResultUploadOutcome:
    """
        Walks a finished RPA results directory, uploads it to the shared
        results object store, posts the RPA ``ResultManifest`` to
        ``results/complete``, and records the upload outcome on the local
        RPA job record. Resilient: a failed transfer or POST is captured as
        an ``upload_failed`` outcome rather than raised.

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

    store_driver = aspects.store_driver
    if store_driver is None:
        store_driver = PresignedUploadDriver(client=client)

    tree_uploaded = upload_result_tree(
        run_id=run_id, entries=entries, backend_driver=backend_driver,
        backend_key_prefix=backend_key_prefix,
        store_driver=store_driver)

    notes = None
    if not tree_uploaded:
        notes = ("RPA result-tree byte transfer incomplete; manifest posted "
                 "for reconciliation. See client-side RPA record.")

    manifest = build_rpa_result_manifest(
        run_id=run_id, client_id=client_id, terminal_outcome=terminal_outcome,
        backend_driver=backend_driver, backend_key_prefix=backend_key_prefix,
        entries=entries, exit_code=exit_code, execution_mode=execution_mode,
        rpa_status=rpa_status, notes=notes)

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
            detail = "RPA result tree uploaded and results/complete accepted."
    else:
        status = UPLOAD_STATUS_FAILED
        if detail is None:
            detail = ("RPA result-tree byte transfer did not complete; job "
                      "marked for upload retry.")

    outcome = ResultUploadOutcome(
        status=status, manifest_posted=manifest_posted,
        tree_uploaded=tree_uploaded, file_count=len(entries), detail=detail)

    record_path = mark_rpa_upload_status(
        state=state, run_id=run_id, upload_outcome=outcome)
    logger.info(
        "pychestrate-client: recorded RPA-upload outcome (status=%s) at %s",
        status, record_path,
    )

    return outcome

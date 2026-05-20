"""
    .. module:: build_executor
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Translates a verified ``job_kind=build`` job envelope into
                   a build-runner invocation and shells out to run it.

                   M5 scope: the executor consumes a ``job_kind=build``
                   envelope dispatched by ``pychestrate-service``. The
                   envelope's ``params`` section carries the build-spec
                   payload (``runner_kind``, ``runner_inputs``,
                   ``repro_digest_sha256``, ``input_files``, ``env``,
                   ``toolchain``). The executor supports the ``makefile``
                   runner kind: it acquires the source tree, shells out to
                   ``make`` with the requested target(s) + env in the build
                   working directory, captures the exit code + output, and
                   records a local build run record under
                   ``<state>/builds/<run_id>/``.

                   Source acquisition -- the build needs a source tree to
                   run ``make`` against. ``pychestrate-service`` PR #241
                   added a first-class ``source`` block to the build job
                   envelope -- a ``JobSourceRef`` carrying
                   ``{ kind, url, ref, integration_id, credentials_ref }``.
                   The executor prefers the structured ``envelope.source``
                   block when present: it uses ``source.url`` +
                   ``source.ref`` for the ``git clone``/``git checkout``.
                   When ``source`` is absent (pre-#241 envelopes) the
                   executor falls back to the legacy
                   ``runner_inputs.git_url`` + optional ``git_ref``
                   convention, and finally to an already-present local
                   path (``runner_inputs.source_dir``).

                   ``credentials_ref`` -- the structured ``source`` block
                   may carry a symbolic ``credentials_ref`` (NOT the
                   credential value) the client agent would resolve to
                   clone a private repository. The client has no
                   secret-fetch path today (the M4 signing-key delivery
                   work fetches only public Ed25519 keys; there is no
                   ``secrets`` REST group), so this milestone scopes
                   source acquisition to public repositories: a non-empty
                   ``credentials_ref`` is logged with a clear
                   "private-repo credentials not yet supported" message
                   and treated as a failed acquisition. Wiring a
                   credential-resolution path is a documented follow-up.

                   The executor never crashes the client: a missing
                   ``make``/``git`` binary, a clone failure, a timeout, or a
                   non-zero exit are each captured as a failed
                   :class:`BuildRunOutcome` rather than raised -- the client
                   process must not crash on a bad dispatch.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2025, Automation Mojo LLC"
__credits__ = []


from typing import Any, Callable, Dict, List, Optional

import json
import logging
import os
import subprocess

from automojo.errors.exceptions import BadParameterError

from automojo.pychestrate.client.state import ClientState


logger = logging.getLogger(__name__)

#: Name of the ``make`` binary the makefile runner shells out to.
DEFAULT_MAKE_BINARY = "make"

#: Name of the ``git`` binary used for source acquisition.
DEFAULT_GIT_BINARY = "git"

#: Default per-build wall-clock cap (seconds) for the build subprocess.
DEFAULT_BUILD_TIMEOUT_SECONDS = 14400.0

#: Default per-clone wall-clock cap (seconds) for source acquisition.
DEFAULT_CLONE_TIMEOUT_SECONDS = 1800.0

#: The only runner kind M5 supports (mirrors the server's
#: ``makefile_runner_kind.MAKEFILE_RUNNER_KIND_NAME``).
MAKEFILE_RUNNER_KIND = "makefile"

#: The only ``source`` block kind the executor acquires today (mirrors
#: the server's ``jobs.envelope.schema.SOURCE_KIND_GIT``). Non-git
#: source kinds are a documented follow-up.
SOURCE_KIND_GIT = "git"

#: Local build-run-record status codes.
BUILD_STATUS_SUCCEEDED = "succeeded"
BUILD_STATUS_FAILED = "failed"

#: Sub-directory (under the client state directory) holding per-build
#: local execution records. Mirrors testbase's ``runs`` directory.
DIRNAME_BUILDS = "builds"


class BuildExecutorAspects:
    """
        The :class:`BuildExecutorAspects` packages caller-controlled
        behavior for the build executor (binary names, timeouts, and the
        subprocess-runner override used by tests).

        Following the project's Aspects pattern, behavior modification is
        carried as data on a single parameter rather than via decorators.
    """

    def __init__(self, *, make_binary: str = DEFAULT_MAKE_BINARY,
                 git_binary: str = DEFAULT_GIT_BINARY,
                 build_timeout_seconds: float = DEFAULT_BUILD_TIMEOUT_SECONDS,
                 clone_timeout_seconds: float = DEFAULT_CLONE_TIMEOUT_SECONDS,
                 run_func: Optional[Callable[..., Any]] = None):
        """
            Initializes a :class:`BuildExecutorAspects` instance.

            :param make_binary: The ``make`` console-script name or
                                absolute path.
            :param git_binary: The ``git`` console-script name or absolute
                               path used for source acquisition.
            :param build_timeout_seconds: Wall-clock cap for the ``make``
                                          subprocess.
            :param clone_timeout_seconds: Wall-clock cap for each git
                                          source-acquisition subprocess.
            :param run_func: Optional subprocess-runner override with the
                             :func:`subprocess.run` signature. Injected
                             for testability; defaults to
                             :func:`subprocess.run`.

            :raises: BadParameterError
        """
        if len(make_binary) == 0:
            raise BadParameterError(
                "BuildExecutorAspects requires a non-empty 'make_binary'.",
                "make_binary", "Pass the make console-script name.")
        if len(git_binary) == 0:
            raise BadParameterError(
                "BuildExecutorAspects requires a non-empty 'git_binary'.",
                "git_binary", "Pass the git console-script name.")
        if build_timeout_seconds <= 0:
            raise BadParameterError(
                "BuildExecutorAspects requires 'build_timeout_seconds' > 0.",
                "build_timeout_seconds", "Pass a positive number of seconds.")
        if clone_timeout_seconds <= 0:
            raise BadParameterError(
                "BuildExecutorAspects requires 'clone_timeout_seconds' > 0.",
                "clone_timeout_seconds", "Pass a positive number of seconds.")

        self._make_binary = make_binary
        self._git_binary = git_binary
        self._build_timeout_seconds = build_timeout_seconds
        self._clone_timeout_seconds = clone_timeout_seconds
        self._run_func = run_func if run_func is not None else subprocess.run
        return

    @property
    def make_binary(self) -> str:
        return self._make_binary

    @property
    def git_binary(self) -> str:
        return self._git_binary

    @property
    def build_timeout_seconds(self) -> float:
        return self._build_timeout_seconds

    @property
    def clone_timeout_seconds(self) -> float:
        return self._clone_timeout_seconds

    @property
    def run_func(self) -> Callable[..., Any]:
        return self._run_func


DEFAULT_BUILD_EXECUTOR_ASPECTS = BuildExecutorAspects()


class BuildRunOutcome:
    """
        The :class:`BuildRunOutcome` captures the local result of a single
        dispatched build execution.
    """

    def __init__(self, *, status: str, exit_code: Optional[int],
                 output_dir: str, source_dir: str, command: List[str],
                 runner_kind: str, detail: Optional[str] = None):
        """
            Initializes a :class:`BuildRunOutcome`.

            :param status: ``BUILD_STATUS_SUCCEEDED`` or
                           ``BUILD_STATUS_FAILED``.
            :param exit_code: The ``make`` process exit code, or ``None``
                              when the process could not be started.
            :param output_dir: Absolute path of the build output directory
                               (the directory the result uploader walks).
            :param source_dir: Absolute path of the build working
                               directory ``make`` was run in.
            :param command: The argv list used for the ``make`` invocation.
            :param runner_kind: The runner kind the envelope declared.
            :param detail: Optional human-readable detail for logging.
        """
        self._status = status
        self._exit_code = exit_code
        self._output_dir = output_dir
        self._source_dir = source_dir
        self._command = list(command)
        self._runner_kind = runner_kind
        self._detail = detail
        return

    @property
    def status(self) -> str:
        return self._status

    @property
    def succeeded(self) -> bool:
        """
            Gets whether the build completed successfully.

            :returns: True when ``status`` is ``BUILD_STATUS_SUCCEEDED``.
        """
        rtnval = self._status == BUILD_STATUS_SUCCEEDED
        return rtnval

    @property
    def exit_code(self) -> Optional[int]:
        return self._exit_code

    @property
    def output_dir(self) -> str:
        return self._output_dir

    @property
    def source_dir(self) -> str:
        return self._source_dir

    @property
    def command(self) -> List[str]:
        return self._command

    @property
    def runner_kind(self) -> str:
        return self._runner_kind

    @property
    def detail(self) -> Optional[str]:
        return self._detail

    def as_dict(self) -> Dict[str, Any]:
        """
            Returns a JSON-serializable dictionary representation.

            :returns: A dict describing the build run outcome.
        """
        rtnval = {
            "status": self._status,
            "exit_code": self._exit_code,
            "output_dir": self._output_dir,
            "source_dir": self._source_dir,
            "command": list(self._command),
            "runner_kind": self._runner_kind,
            "detail": self._detail,
        }
        return rtnval


def _extract_build_spec(envelope: Dict[str, Any]) -> Dict[str, Any]:
    """
        Pulls the build-spec payload out of a verified ``job_kind=build``
        envelope's ``params`` section.

        :param envelope: The verified envelope dict.

        :returns: The ``params`` map (an empty dict when absent or
                  malformed).
    """
    params = envelope.get("params", {})
    if not isinstance(params, dict):
        rtnval: Dict[str, Any] = {}
        return rtnval

    rtnval = params
    return rtnval


def _extract_runner_kind(build_spec: Dict[str, Any]) -> str:
    """
        Resolves the runner kind from a build-spec payload.

        :param build_spec: The envelope ``params`` build-spec map.

        :returns: The runner-kind string (possibly empty).
    """
    raw = build_spec.get("runner_kind", "")
    rtnval = str(raw)
    return rtnval


def _extract_make_targets(runner_inputs: Dict[str, Any]) -> List[str]:
    """
        Extracts the make ``targets`` selection from the runner inputs.

        The server's ``MakefileRunnerKind`` validates that ``targets`` is a
        non-empty list of non-empty strings before the envelope is signed.

        :param runner_inputs: The build-spec ``runner_inputs`` map.

        :returns: A list of make-target strings (possibly empty).
    """
    rtnval: List[str] = []

    raw_targets = runner_inputs.get("targets", None)
    if raw_targets is None:
        return rtnval

    if isinstance(raw_targets, list):
        for entry in raw_targets:
            rtnval.append(str(entry))
    else:
        rtnval.append(str(raw_targets))

    return rtnval


def _extract_build_env(build_spec: Dict[str, Any]) -> Dict[str, str]:
    """
        Extracts the build environment overlay from the build spec.

        :param build_spec: The envelope ``params`` build-spec map.

        :returns: A dict of string-to-string environment entries.
    """
    rtnval: Dict[str, str] = {}

    raw_env = build_spec.get("env", None)
    if raw_env is None:
        return rtnval
    if not isinstance(raw_env, dict):
        return rtnval

    for env_key, env_value in raw_env.items():
        rtnval[str(env_key)] = str(env_value)

    return rtnval


def _extract_source_ref(envelope: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
        Pulls the structured ``source`` block off a verified
        ``job_kind=build`` envelope.

        ``pychestrate-service`` PR #241 added the first-class ``source``
        block (a ``JobSourceRef``: ``kind``, ``url``, ``ref``,
        ``integration_id``, ``credentials_ref``) to the build envelope.
        The block is optional -- envelopes minted before #241 (and test /
        provision envelopes) carry no ``source`` -- so this helper returns
        ``None`` when the block is absent or malformed and the caller
        falls back to the legacy ``runner_inputs.git_url`` convention.

        :param envelope: The verified envelope dict.

        :returns: The ``source`` block map, or ``None`` when absent or
                  malformed.
    """
    raw_source = envelope.get("source", None)
    if raw_source is None:
        rtnval: Optional[Dict[str, Any]] = None
        return rtnval

    if not isinstance(raw_source, dict):
        logger.warning(
            "pychestrate-client: the build envelope's 'source' block is "
            "not an object (%s); ignoring it and falling back to the "
            "legacy runner_inputs.git_url convention.",
            type(raw_source).__name__,
        )
        rtnval = None
        return rtnval

    rtnval = raw_source
    return rtnval


def resolve_build_dirs(state: ClientState, run_id: str) -> Dict[str, str]:
    """
        Builds and creates the per-build directories under the client
        state ``builds`` directory.

        The layout mirrors the testbase ``runs`` layout::

            <state>/builds/<run_id>/
                source/   - the build working directory (source tree)
                output/   - the build output directory (uploaded artifacts)

        :param state: The :class:`ClientState`.
        :param run_id: The server-assigned run UUID.

        :returns: A dict with ``record_dir``, ``source_dir`` and
                  ``output_dir`` absolute paths.
    """
    safe_run_id = run_id if len(run_id) > 0 else "unknown-build"
    record_dir = os.path.join(state.state_dir, DIRNAME_BUILDS, safe_run_id)
    source_dir = os.path.join(record_dir, "source")
    output_dir = os.path.join(record_dir, "output")

    os.makedirs(source_dir, mode=0o755, exist_ok=True)
    os.makedirs(output_dir, mode=0o755, exist_ok=True)

    rtnval = {
        "record_dir": record_dir,
        "source_dir": source_dir,
        "output_dir": output_dir,
    }
    return rtnval


class SourceAcquisitionResult:
    """
        The :class:`SourceAcquisitionResult` captures the outcome of a
        source-tree acquisition step.
    """

    def __init__(self, *, acquired: bool, source_dir: str,
                 detail: Optional[str] = None):
        """
            Initializes a :class:`SourceAcquisitionResult`.

            :param acquired: True when a usable source tree is in place.
            :param source_dir: Absolute path of the build working
                               directory the build runs in.
            :param detail: Optional human-readable detail for logging.
        """
        self._acquired = acquired
        self._source_dir = source_dir
        self._detail = detail
        return

    @property
    def acquired(self) -> bool:
        return self._acquired

    @property
    def source_dir(self) -> str:
        return self._source_dir

    @property
    def detail(self) -> Optional[str]:
        return self._detail


def _acquire_structured_source(*, source_ref: Dict[str, Any],
                               source_dir: str,
                               aspects: BuildExecutorAspects
                               ) -> SourceAcquisitionResult:
    """
        Acquires the build's source tree from the envelope's structured
        ``source`` block (a ``JobSourceRef``).

        Only the ``git`` source kind is acquired today. A ``source`` block
        carrying a non-empty ``credentials_ref`` references a private
        repository; the client has no secret-fetch path yet (a documented
        follow-up), so a non-empty ``credentials_ref`` is treated as a
        non-acquired result with a clear log message.

        :param source_ref: The envelope's ``source`` block map.
        :param source_dir: The build working directory the source lands in.
        :param aspects: Behavior-shaping aspects.

        :returns: A :class:`SourceAcquisitionResult`.
    """
    source_kind = str(source_ref.get("kind", "")).strip()
    if len(source_kind) == 0:
        source_kind = SOURCE_KIND_GIT

    if source_kind != SOURCE_KIND_GIT:
        detail = (
            "The build envelope's structured 'source' block declares an "
            "unsupported source kind {!r}; only the 'git' kind is "
            "acquired today. Non-git source kinds are a follow-up.".format(
                source_kind)
        )
        logger.error("pychestrate-client: %s", detail)
        rtnval = SourceAcquisitionResult(
            acquired=False, source_dir=source_dir, detail=detail)
        return rtnval

    source_url = str(source_ref.get("url", "")).strip()
    if len(source_url) == 0:
        detail = (
            "The build envelope's structured 'source' block carries an "
            "empty 'url'; there is nothing to clone."
        )
        logger.error("pychestrate-client: %s", detail)
        rtnval = SourceAcquisitionResult(
            acquired=False, source_dir=source_dir, detail=detail)
        return rtnval

    credentials_ref = str(source_ref.get("credentials_ref", "")).strip()
    if len(credentials_ref) > 0:
        detail = (
            "The build envelope's structured 'source' block carries a "
            "non-empty 'credentials_ref' ({!r}) -- it references a "
            "private repository. The pychestrate-client has no "
            "credential-resolution path yet, so private-repo source "
            "credentials are not yet supported; this acquisition is "
            "scoped to public repositories. Wiring a credential-fetch "
            "path is a documented follow-up.".format(credentials_ref)
        )
        logger.error("pychestrate-client: %s", detail)
        rtnval = SourceAcquisitionResult(
            acquired=False, source_dir=source_dir, detail=detail)
        return rtnval

    source_kind_ref = source_ref.get("ref", None)
    git_ref = None
    if source_kind_ref is not None and len(str(source_kind_ref)) > 0:
        git_ref = str(source_kind_ref)

    logger.info(
        "pychestrate-client: acquiring build source from the structured "
        "envelope 'source' block (url=%s, ref=%s).", source_url, git_ref,
    )

    rtnval = _acquire_git_source(
        git_url=source_url, git_ref=git_ref, source_dir=source_dir,
        aspects=aspects)
    return rtnval


def acquire_source(*, envelope: Dict[str, Any], build_spec: Dict[str, Any],
                    source_dir: str,
                    aspects: BuildExecutorAspects =
                    DEFAULT_BUILD_EXECUTOR_ASPECTS
                    ) -> SourceAcquisitionResult:
    """
        Acquires the build's source tree into the build working directory.

        The executor resolves the source in this precedence order:

            1. ``envelope.source`` -- the first-class ``source`` block
               (a ``JobSourceRef``) added by ``pychestrate-service``
               PR #241. When present the executor uses ``source.url`` +
               ``source.ref`` for the ``git clone``/``git checkout``.
               Only the ``git`` source kind is acquired; a non-empty
               ``source.credentials_ref`` (a private repository) is not
               yet supported and yields a non-acquired result.
            2. ``runner_inputs.git_url`` (+ optional ``git_ref``) -- the
               legacy ad-hoc convention, used only when the envelope
               carries no structured ``source`` block (pre-#241
               envelopes). The executor ``git clone``s the repository
               into the build working directory and, when a ref is
               supplied, ``git checkout``s it.
            3. ``runner_inputs.source_dir`` -- an already-present local
               path on the pod. The build runs directly in that directory
               (no copy).

        When none of these is present the caller decides whether a bare
        ``make`` in the empty working directory is meaningful.

        The function is resilient: a clone/checkout failure is captured as
        a non-acquired :class:`SourceAcquisitionResult` rather than raised.

        :param envelope: The verified envelope dict (carries the optional
                         structured ``source`` block).
        :param build_spec: The envelope ``params`` build-spec map.
        :param source_dir: The build working directory the source lands in.
        :param aspects: Behavior-shaping aspects.

        :returns: A :class:`SourceAcquisitionResult`.
    """
    runner_inputs = build_spec.get("runner_inputs", {})
    if not isinstance(runner_inputs, dict):
        runner_inputs = {}

    source_ref = _extract_source_ref(envelope=envelope)
    if source_ref is not None:
        rtnval = _acquire_structured_source(
            source_ref=source_ref, source_dir=source_dir, aspects=aspects)
        return rtnval

    git_url = runner_inputs.get("git_url", None)
    if git_url is not None and len(str(git_url)) > 0:
        logger.info(
            "pychestrate-client: the build envelope carries no structured "
            "'source' block; falling back to the legacy "
            "runner_inputs.git_url convention.",
        )
        rtnval = _acquire_git_source(
            git_url=str(git_url),
            git_ref=runner_inputs.get("git_ref", None),
            source_dir=source_dir,
            aspects=aspects)
        return rtnval

    local_source = runner_inputs.get("source_dir", None)
    if local_source is not None and len(str(local_source)) > 0:
        local_path = str(local_source)
        if os.path.isdir(local_path):
            logger.info(
                "pychestrate-client: build using already-present local "
                "source tree at %s.", local_path,
            )
            rtnval = SourceAcquisitionResult(
                acquired=True, source_dir=local_path,
                detail="Local source tree present.")
            return rtnval

        detail = (
            "The build envelope referenced a local source_dir that does "
            "not exist on this pod: {}".format(local_path)
        )
        logger.error("pychestrate-client: %s", detail)
        rtnval = SourceAcquisitionResult(
            acquired=False, source_dir=source_dir, detail=detail)
        return rtnval

    detail = (
        "The build envelope carries no structured 'source' block, no "
        "'runner_inputs.git_url', and no local 'runner_inputs.source_dir'; "
        "source acquisition has nothing to fetch. Richer "
        "source-integration handling is a follow-up."
    )
    logger.warning("pychestrate-client: %s", detail)
    rtnval = SourceAcquisitionResult(
        acquired=False, source_dir=source_dir, detail=detail)
    return rtnval


def _acquire_git_source(*, git_url: str, git_ref: Optional[Any],
                        source_dir: str,
                        aspects: BuildExecutorAspects
                        ) -> SourceAcquisitionResult:
    """
        Clones a git repository into the build working directory and, when
        a ref is supplied, checks it out.

        :param git_url: The git remote URL to clone.
        :param git_ref: Optional branch / tag / commit to check out.
        :param source_dir: The build working directory the clone lands in.
        :param aspects: Behavior-shaping aspects.

        :returns: A :class:`SourceAcquisitionResult`.
    """
    clone_command = [
        aspects.git_binary,
        "clone",
        git_url,
        source_dir,
    ]

    logger.info(
        "pychestrate-client: cloning build source -> %s",
        " ".join(clone_command),
    )

    try:
        clone_completed = aspects.run_func(
            clone_command,
            cwd=None,
            timeout=aspects.clone_timeout_seconds,
            capture_output=True,
        )
    except FileNotFoundError as notfounderr:
        detail = (
            "The 'git' binary ({}) could not be found on this pod: "
            "{}".format(aspects.git_binary, notfounderr)
        )
        logger.error("pychestrate-client: %s", detail)
        rtnval = SourceAcquisitionResult(
            acquired=False, source_dir=source_dir, detail=detail)
        return rtnval
    except subprocess.TimeoutExpired as timeouterr:
        detail = (
            "The git clone exceeded the {}s timeout: {}".format(
                aspects.clone_timeout_seconds, timeouterr)
        )
        logger.error("pychestrate-client: %s", detail)
        rtnval = SourceAcquisitionResult(
            acquired=False, source_dir=source_dir, detail=detail)
        return rtnval
    except Exception as cloneerr:
        detail = "The git clone raised an unexpected error: {}".format(
            cloneerr)
        logger.error("pychestrate-client: %s", detail)
        rtnval = SourceAcquisitionResult(
            acquired=False, source_dir=source_dir, detail=detail)
        return rtnval

    clone_exit = getattr(clone_completed, "returncode", None)
    if clone_exit != 0:
        detail = "git clone of {} exited with code {}.".format(
            git_url, clone_exit)
        logger.error("pychestrate-client: %s", detail)
        rtnval = SourceAcquisitionResult(
            acquired=False, source_dir=source_dir, detail=detail)
        return rtnval

    if git_ref is None or len(str(git_ref)) == 0:
        rtnval = SourceAcquisitionResult(
            acquired=True, source_dir=source_dir,
            detail="Cloned {} (default ref).".format(git_url))
        return rtnval

    checkout_command = [
        aspects.git_binary,
        "checkout",
        str(git_ref),
    ]

    logger.info(
        "pychestrate-client: checking out build ref -> %s",
        " ".join(checkout_command),
    )

    try:
        checkout_completed = aspects.run_func(
            checkout_command,
            cwd=source_dir,
            timeout=aspects.clone_timeout_seconds,
            capture_output=True,
        )
    except Exception as checkouterr:
        detail = "git checkout of ref {} raised an error: {}".format(
            git_ref, checkouterr)
        logger.error("pychestrate-client: %s", detail)
        rtnval = SourceAcquisitionResult(
            acquired=False, source_dir=source_dir, detail=detail)
        return rtnval

    checkout_exit = getattr(checkout_completed, "returncode", None)
    if checkout_exit != 0:
        detail = "git checkout of ref {} exited with code {}.".format(
            git_ref, checkout_exit)
        logger.error("pychestrate-client: %s", detail)
        rtnval = SourceAcquisitionResult(
            acquired=False, source_dir=source_dir, detail=detail)
        return rtnval

    rtnval = SourceAcquisitionResult(
        acquired=True, source_dir=source_dir,
        detail="Cloned {} and checked out ref {}.".format(git_url, git_ref))
    return rtnval


def build_make_command(*, envelope: Dict[str, Any],
                        make_binary: str = DEFAULT_MAKE_BINARY) -> List[str]:
    """
        Translates a verified ``job_kind=build`` envelope into a ``make``
        argv list for the ``makefile`` runner kind.

        The argv carries: the ``makefile_path`` (``-f``, when the runner
        inputs override the default ``Makefile``), the ``parallelism``
        (``-j``, when non-zero), and the requested make target(s).

        :param envelope: The verified envelope dict.
        :param make_binary: The ``make`` console-script name or path.

        :returns: The argv list for the ``make`` invocation.
        :raises: BadParameterError when the envelope is not a ``makefile``
                 build or omits the required make targets.
    """
    build_spec = _extract_build_spec(envelope=envelope)
    runner_kind = _extract_runner_kind(build_spec=build_spec)

    if runner_kind != MAKEFILE_RUNNER_KIND:
        errmsg_lines = [
            "The dispatched build envelope declares an unsupported runner "
            "kind.",
            "    envelope_id: {}".format(envelope.get("envelope_id")),
            "    runner_kind: {}".format(runner_kind),
            "M5 supports only the 'makefile' runner kind; other kinds are "
            "a follow-up.",
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise BadParameterError(
            errmsg, "envelope", "Dispatch a 'makefile' build envelope.")

    runner_inputs = build_spec.get("runner_inputs", {})
    if not isinstance(runner_inputs, dict):
        runner_inputs = {}

    targets = _extract_make_targets(runner_inputs=runner_inputs)
    if len(targets) == 0:
        errmsg_lines = [
            "The dispatched makefile build envelope carries no make "
            "targets.",
            "    envelope_id: {}".format(envelope.get("envelope_id")),
            "The server's MakefileRunnerKind requires a non-empty "
            "'runner_inputs.targets' list.",
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise BadParameterError(
            errmsg, "envelope",
            "Include 'params.runner_inputs.targets' in the envelope.")

    rtnval: List[str] = [make_binary]

    makefile_path = runner_inputs.get("makefile_path", None)
    if makefile_path is not None and len(str(makefile_path)) > 0 and \
            str(makefile_path) != "Makefile":
        rtnval.append("-f")
        rtnval.append(str(makefile_path))

    parallelism = runner_inputs.get("parallelism", 0)
    try:
        parallelism_int = int(parallelism)
    except (TypeError, ValueError):
        parallelism_int = 0
    if parallelism_int > 0:
        rtnval.append("-j")
        rtnval.append(str(parallelism_int))

    for target in targets:
        rtnval.append(target)

    return rtnval


def write_local_build_record(state: ClientState, run_id: str,
                              local_run_id: str, envelope_id: str,
                              outcome: BuildRunOutcome) -> str:
    """
        Persists a local build run record describing the dispatched build
        job's execution outcome.

        The record file is the per-build counterpart of the testbase
        ``run-record.json``; the result uploader merges its ``upload``
        section into the same file after the artifact upload.

        :param state: The :class:`ClientState`.
        :param run_id: The server-assigned run UUID.
        :param local_run_id: The client-minted local run identifier.
        :param envelope_id: The dispatched envelope id.
        :param outcome: The :class:`BuildRunOutcome` to record.

        :returns: The absolute path of the written build-record file.
    """
    safe_run_id = run_id if len(run_id) > 0 else "unknown-build"
    record_dir = os.path.join(state.state_dir, DIRNAME_BUILDS, safe_run_id)
    os.makedirs(record_dir, mode=0o755, exist_ok=True)

    record = {
        "run_id": run_id,
        "local_run_id": local_run_id,
        "envelope_id": envelope_id,
        "job_kind": "build",
        "outcome": outcome.as_dict(),
    }

    record_path = os.path.join(record_dir, "build-record.json")
    with open(record_path, "w", encoding="utf-8") as recf:
        json.dump(record, recf, indent=4, sort_keys=True)
        recf.write(os.linesep)

    rtnval = record_path
    return rtnval


def execute_build_envelope(*, state: ClientState, envelope: Dict[str, Any],
                           local_run_id: str,
                           aspects: BuildExecutorAspects =
                           DEFAULT_BUILD_EXECUTOR_ASPECTS,
                           progress_reporter: Optional[Any] = None
                           ) -> BuildRunOutcome:
    """
        Executes a verified ``job_kind=build`` envelope by acquiring the
        source tree, shelling out to the makefile runner, and recording a
        local build run record.

        The executor is resilient: an unbuildable envelope, a source
        acquisition failure, a missing ``make`` binary, a timeout, or a
        non-zero exit are each captured as a failed
        :class:`BuildRunOutcome` rather than raised -- the client process
        must not crash on a bad dispatch.

        When a ``progress_reporter`` is supplied (a
        :class:`automojo.pychestrate.client.build_progress_reporter.BuildProgressReporter`)
        its ``report_running`` / ``report_finished`` transitions are
        invoked around the build. Progress reporting is best-effort and
        never affects the build outcome.

        :param state: The :class:`ClientState` used to place build output.
        :param envelope: The verified envelope dict.
        :param local_run_id: The client-minted local run identifier.
        :param aspects: Behavior-shaping aspects.
        :param progress_reporter: Optional build progress reporter driven
                                  around the build.

        :returns: A :class:`BuildRunOutcome`.
    """
    run_id = str(envelope.get("run_id", ""))
    envelope_id = str(envelope.get("envelope_id", ""))

    build_dirs = resolve_build_dirs(state=state, run_id=run_id)
    output_dir = build_dirs["output_dir"]
    work_dir = build_dirs["source_dir"]

    build_spec = _extract_build_spec(envelope=envelope)
    runner_kind = _extract_runner_kind(build_spec=build_spec)

    def _fail(detail: str, command: List[str],
              source_dir: str) -> BuildRunOutcome:
        logger.error("pychestrate-client: %s", detail)
        failed = BuildRunOutcome(
            status=BUILD_STATUS_FAILED, exit_code=None,
            output_dir=output_dir, source_dir=source_dir, command=command,
            runner_kind=runner_kind, detail=detail)
        write_local_build_record(
            state=state, run_id=run_id, local_run_id=local_run_id,
            envelope_id=envelope_id, outcome=failed)
        return failed

    try:
        command = build_make_command(
            envelope=envelope, make_binary=aspects.make_binary)
    except BadParameterError as builderr:
        rtnval = _fail(
            detail="Failed to build make command: {}".format(builderr),
            command=[], source_dir=work_dir)
        return rtnval

    acquisition = acquire_source(
        envelope=envelope, build_spec=build_spec, source_dir=work_dir,
        aspects=aspects)
    if not acquisition.acquired:
        detail = "Build source acquisition failed: {}".format(
            acquisition.detail)
        rtnval = _fail(detail=detail, command=command, source_dir=work_dir)
        return rtnval

    source_dir = acquisition.source_dir

    logger.info(
        "pychestrate-client: executing makefile build for run_id=%s in %s "
        "-> %s", run_id, source_dir, " ".join(command),
    )

    build_env = dict(os.environ)
    for env_key, env_value in _extract_build_env(build_spec=build_spec).items():
        build_env[env_key] = env_value

    if progress_reporter is not None:
        try:
            progress_reporter.report_running()
        except Exception as progerr:
            logger.warning(
                "pychestrate-client: build progress 'running' report failed "
                "for run_id=%s (build continues): %s", run_id, progerr,
            )

    exit_code: Optional[int] = None
    status = BUILD_STATUS_FAILED
    detail: Optional[str] = None
    completed: Optional[Any] = None

    try:
        completed = aspects.run_func(
            command,
            cwd=source_dir,
            env=build_env,
            timeout=aspects.build_timeout_seconds,
            capture_output=True,
        )
        exit_code = getattr(completed, "returncode", None)
        if exit_code == 0:
            status = BUILD_STATUS_SUCCEEDED
            detail = "makefile build completed with exit code 0."
        else:
            status = BUILD_STATUS_FAILED
            detail = "makefile build exited with code {}.".format(exit_code)
    except FileNotFoundError as notfounderr:
        detail = (
            "The 'make' binary ({}) could not be found on this pod: "
            "{}".format(aspects.make_binary, notfounderr)
        )
        logger.error("pychestrate-client: %s", detail)
    except subprocess.TimeoutExpired as timeouterr:
        detail = (
            "The makefile build exceeded the {}s timeout: {}".format(
                aspects.build_timeout_seconds, timeouterr)
        )
        logger.error("pychestrate-client: %s", detail)
    except Exception as runerr:
        detail = (
            "The makefile build invocation raised an unexpected error: "
            "{}".format(runerr)
        )
        logger.error("pychestrate-client: %s", detail)

    _capture_build_log(
        output_dir=output_dir, completed=completed, command=command,
        exit_code=exit_code, detail=detail)

    outcome = BuildRunOutcome(
        status=status, exit_code=exit_code, output_dir=output_dir,
        source_dir=source_dir, command=command, runner_kind=runner_kind,
        detail=detail)

    record_path = write_local_build_record(
        state=state, run_id=run_id, local_run_id=local_run_id,
        envelope_id=envelope_id, outcome=outcome)
    logger.info(
        "pychestrate-client: recorded local build outcome (status=%s) at %s",
        status, record_path,
    )

    if progress_reporter is not None:
        try:
            progress_reporter.report_finished(succeeded=outcome.succeeded)
        except Exception as progerr:
            logger.warning(
                "pychestrate-client: build progress 'finished' report failed "
                "for run_id=%s: %s", run_id, progerr,
            )

    rtnval = outcome
    return rtnval


def _capture_build_log(*, output_dir: str, completed: Optional[Any],
                        command: List[str], exit_code: Optional[int],
                        detail: Optional[str]) -> None:
    """
        Writes the build's captured stdout/stderr + an invocation summary
        into the build output directory so the result uploader has an
        artifact tree to walk.

        A capture failure is logged and swallowed -- it must not turn a
        successful build into a failed one.

        :param output_dir: The build output directory.
        :param completed: The ``CompletedProcess`` (or ``None`` when the
                          process never started).
        :param command: The argv list used for the invocation.
        :param exit_code: The build process exit code, or ``None``.
        :param detail: The human-readable outcome detail.

        :returns: None
    """
    try:
        stdout_bytes = b""
        stderr_bytes = b""
        if completed is not None:
            raw_stdout = getattr(completed, "stdout", None)
            raw_stderr = getattr(completed, "stderr", None)
            if isinstance(raw_stdout, bytes):
                stdout_bytes = raw_stdout
            elif isinstance(raw_stdout, str):
                stdout_bytes = raw_stdout.encode("utf-8")
            if isinstance(raw_stderr, bytes):
                stderr_bytes = raw_stderr
            elif isinstance(raw_stderr, str):
                stderr_bytes = raw_stderr.encode("utf-8")

        stdout_path = os.path.join(output_dir, "build-stdout.log")
        with open(stdout_path, "wb") as outf:
            outf.write(stdout_bytes)

        stderr_path = os.path.join(output_dir, "build-stderr.log")
        with open(stderr_path, "wb") as errf:
            errf.write(stderr_bytes)

        summary = {
            "command": list(command),
            "exit_code": exit_code,
            "detail": detail,
        }
        summary_path = os.path.join(output_dir, "build-summary.json")
        with open(summary_path, "w", encoding="utf-8") as sumf:
            json.dump(summary, sumf, indent=4, sort_keys=True)
            sumf.write(os.linesep)
    except OSError as captureerr:
        logger.warning(
            "pychestrate-client: failed to capture build log into %s: %s",
            output_dir, captureerr,
        )

    return

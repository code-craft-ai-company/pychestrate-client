"""
    .. module:: testbase_executor
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Translates a verified job envelope into an
                   ``codecraftai-testbase`` invocation and shells out to run it.

                   M2 scope: the executor builds and runs a
                   ``testbase testing run ...`` command via :mod:`subprocess`,
                   captures the exit code and the output directory, and
                   records a local run record under the client state dir.

                   M3 scope: the executor accepts an optional
                   :class:`ccai.pychestrate.client.progress_reporter.ProgressReporter`.
                   When one is supplied it is started immediately before the
                   testbase subprocess launches and stopped once the process
                   exits, so live progress streams to the service for the
                   lifetime of the run. The result-tree upload itself is
                   driven by the dispatch consumer after the executor returns.

                   The executor never crashes the client: a missing
                   ``testbase`` binary or a non-zero exit is recorded as a
                   failed local run and surfaced to the caller.

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

from ccai.errors.exceptions import BadParameterError

from ccai.pychestrate.client.state import ClientState


logger = logging.getLogger(__name__)

#: Name of the testbase console-script the executor shells out to.
DEFAULT_TESTBASE_BINARY = "testbase"

#: Default per-run wall-clock cap (seconds) for the testbase subprocess.
DEFAULT_RUN_TIMEOUT_SECONDS = 14400.0

#: Local run-record status codes.
RUN_STATUS_SUCCEEDED = "succeeded"
RUN_STATUS_FAILED = "failed"


class TestbaseExecutorAspects:
    """
        The :class:`TestbaseExecutorAspects` packages caller-controlled
        behavior for the testbase executor (binary name, timeout, and the
        subprocess-runner override used by tests).

        Following the project's Aspects pattern, behavior modification is
        carried as data on a single parameter rather than via decorators.
    """

    def __init__(self, *, testbase_binary: str = DEFAULT_TESTBASE_BINARY,
                 run_timeout_seconds: float = DEFAULT_RUN_TIMEOUT_SECONDS,
                 run_func: Optional[Callable[..., Any]] = None):
        """
            Initializes a :class:`TestbaseExecutorAspects` instance.

            :param testbase_binary: The testbase console-script name or
                                    absolute path.
            :param run_timeout_seconds: Wall-clock cap for the testbase
                                        subprocess.
            :param run_func: Optional subprocess-runner override with the
                             :func:`subprocess.run` signature. Injected
                             for testability; defaults to
                             :func:`subprocess.run`.

            :raises: BadParameterError
        """
        if len(testbase_binary) == 0:
            raise BadParameterError(
                "TestbaseExecutorAspects requires a non-empty 'testbase_binary'.",
                "testbase_binary", "Pass the testbase console-script name.")
        if run_timeout_seconds <= 0:
            raise BadParameterError(
                "TestbaseExecutorAspects requires 'run_timeout_seconds' > 0.",
                "run_timeout_seconds", "Pass a positive number of seconds.")

        self._testbase_binary = testbase_binary
        self._run_timeout_seconds = run_timeout_seconds
        self._run_func = run_func if run_func is not None else subprocess.run
        return

    @property
    def testbase_binary(self) -> str:
        return self._testbase_binary

    @property
    def run_timeout_seconds(self) -> float:
        return self._run_timeout_seconds

    @property
    def run_func(self) -> Callable[..., Any]:
        return self._run_func


DEFAULT_TESTBASE_EXECUTOR_ASPECTS = TestbaseExecutorAspects()


class TestbaseRunOutcome:
    """
        The :class:`TestbaseRunOutcome` captures the local result of a
        single dispatched job execution.
    """

    def __init__(self, *, status: str, exit_code: Optional[int],
                 output_dir: str, command: List[str],
                 detail: Optional[str] = None):
        """
            Initializes a :class:`TestbaseRunOutcome`.

            :param status: ``RUN_STATUS_SUCCEEDED`` or ``RUN_STATUS_FAILED``.
            :param exit_code: The testbase process exit code, or ``None``
                              when the process could not be started.
            :param output_dir: Absolute path of the run output directory.
            :param command: The argv list used for the invocation.
            :param detail: Optional human-readable detail for logging.
        """
        self._status = status
        self._exit_code = exit_code
        self._output_dir = output_dir
        self._command = list(command)
        self._detail = detail
        return

    @property
    def status(self) -> str:
        return self._status

    @property
    def succeeded(self) -> bool:
        """
            Gets whether the run completed successfully.

            :returns: True when ``status`` is ``RUN_STATUS_SUCCEEDED``.
        """
        rtnval = self._status == RUN_STATUS_SUCCEEDED
        return rtnval

    @property
    def exit_code(self) -> Optional[int]:
        return self._exit_code

    @property
    def output_dir(self) -> str:
        return self._output_dir

    @property
    def command(self) -> List[str]:
        return self._command

    @property
    def detail(self) -> Optional[str]:
        return self._detail

    def as_dict(self) -> Dict[str, Any]:
        """
            Returns a JSON-serializable dictionary representation.

            :returns: A dict describing the run outcome.
        """
        rtnval = {
            "status": self._status,
            "exit_code": self._exit_code,
            "output_dir": self._output_dir,
            "command": list(self._command),
            "detail": self._detail,
        }
        return rtnval


def _extract_includes(params: Dict[str, Any]) -> List[str]:
    """
        Extracts the testbase ``includes`` selection from envelope params.
        Accepts either an ``includes`` list or a single ``suite`` string.

        :param params: The envelope ``params`` map.

        :returns: A list of include selectors (possibly empty).
    """
    rtnval: List[str] = []

    raw_includes = params.get("includes", None)
    if raw_includes is not None:
        if isinstance(raw_includes, list):
            for entry in raw_includes:
                rtnval.append(str(entry))
        else:
            rtnval.append(str(raw_includes))

    raw_suite = params.get("suite", None)
    if raw_suite is not None and len(rtnval) == 0:
        rtnval.append(str(raw_suite))

    return rtnval


def _extract_excludes(params: Dict[str, Any]) -> List[str]:
    """
        Extracts the testbase ``excludes`` selection from envelope params.

        :param params: The envelope ``params`` map.

        :returns: A list of exclude selectors (possibly empty).
    """
    rtnval: List[str] = []

    raw_excludes = params.get("excludes", None)
    if raw_excludes is not None:
        if isinstance(raw_excludes, list):
            for entry in raw_excludes:
                rtnval.append(str(entry))
        else:
            rtnval.append(str(raw_excludes))

    return rtnval


def _extract_test_root(envelope: Dict[str, Any]) -> Optional[str]:
    """
        Resolves the testbase project root from the envelope. The root
        may be supplied as ``params.test_root`` (an already-present local
        path). M2 does not fetch a remote project; if only a remote
        reference is present the root is left unresolved.

        :param envelope: The verified envelope dict.

        :returns: The test-root path, or ``None`` when unresolved.
    """
    params = envelope.get("params", {})
    if not isinstance(params, dict):
        rtnval = None
        return rtnval

    candidate = params.get("test_root", None)
    if candidate is None:
        candidate = params.get("testbase_root", None)

    rtnval = None
    if candidate is not None and len(str(candidate)) > 0:
        rtnval = str(candidate)
    return rtnval


def build_testbase_command(*, envelope: Dict[str, Any], output_dir: str,
                            testbase_binary: str = DEFAULT_TESTBASE_BINARY
                            ) -> List[str]:
    """
        Translates a verified job envelope into a ``testbase testing run``
        argv list.

        :param envelope: The verified envelope dict.
        :param output_dir: Absolute path the run writes its results to.
        :param testbase_binary: The testbase console-script name or path.

        :returns: The argv list for the testbase invocation.
        :raises: BadParameterError when the envelope omits a resolvable
                 testbase project root.
    """
    test_root = _extract_test_root(envelope=envelope)
    if test_root is None:
        errmsg_lines = [
            "The dispatched job envelope does not carry a resolvable "
            "testbase project root.",
            "    envelope_id: {}".format(envelope.get("envelope_id")),
            "    job_type: {}".format(envelope.get("job_type")),
            "M2 expects the envelope 'params' to include a local "
            "'test_root' path; remote project fetch is out of scope.",
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise BadParameterError(
            errmsg, "envelope", "Include 'params.test_root' in the envelope.")

    params = envelope.get("params", {})
    if not isinstance(params, dict):
        params = {}

    includes = _extract_includes(params=params)
    excludes = _extract_excludes(params=params)
    refs = envelope.get("refs", {})
    if not isinstance(refs, dict):
        refs = {}

    rtnval: List[str] = [
        testbase_binary,
        "testing",
        "run",
        "--root",
        test_root,
        "--output",
        output_dir,
    ]

    for include_sel in includes:
        rtnval.append("--includes={}".format(include_sel))
    for exclude_sel in excludes:
        rtnval.append("--excludes={}".format(exclude_sel))

    branch = refs.get("branch", None)
    if branch is not None and len(str(branch)) > 0:
        rtnval.append("--branch")
        rtnval.append(str(branch))

    build = refs.get("build", None)
    if build is not None and len(str(build)) > 0:
        rtnval.append("--build")
        rtnval.append(str(build))

    flavor = refs.get("flavor", None)
    if flavor is not None and len(str(flavor)) > 0:
        rtnval.append("--flavor")
        rtnval.append(str(flavor))

    run_id = envelope.get("run_id", None)
    if run_id is not None and len(str(run_id)) > 0:
        rtnval.append("--runid")
        rtnval.append(str(run_id))

    job_id = envelope.get("job_id", None)
    if job_id is not None and len(str(job_id)) > 0:
        rtnval.append("--job-id")
        rtnval.append(str(job_id))

    job_name = envelope.get("job_name", None)
    if job_name is not None and len(str(job_name)) > 0:
        rtnval.append("--job-name")
        rtnval.append(str(job_name))

    return rtnval


def resolve_run_output_dir(state: ClientState, run_id: str) -> str:
    """
        Builds and creates the per-run output directory under the client
        state runs directory.

        The directory is deterministic for a given ``(state, run_id)`` pair
        so a caller (the dispatch consumer) can resolve the testbase output
        directory ahead of the run -- e.g. to point a progress reporter at
        the JSOS stream file the run will write into it.

        :param state: The :class:`ClientState`.
        :param run_id: The server-assigned run UUID.

        :returns: The absolute output directory path.
    """
    safe_run_id = run_id if len(run_id) > 0 else "unknown-run"
    output_dir = os.path.join(state.runs_dir, safe_run_id, "output")
    os.makedirs(output_dir, mode=0o755, exist_ok=True)
    rtnval = output_dir
    return rtnval


def write_local_run_record(state: ClientState, run_id: str, local_run_id: str,
                            envelope_id: str,
                            outcome: TestbaseRunOutcome) -> str:
    """
        Persists a local run record describing the dispatched job's
        execution outcome. M3 will replace this with an upstream result
        push; M2 records it on disk for reconciliation + debugging.

        :param state: The :class:`ClientState`.
        :param run_id: The server-assigned run UUID.
        :param local_run_id: The client-minted local run identifier.
        :param envelope_id: The dispatched envelope id.
        :param outcome: The :class:`TestbaseRunOutcome` to record.

        :returns: The absolute path of the written run-record file.
    """
    safe_run_id = run_id if len(run_id) > 0 else "unknown-run"
    record_dir = os.path.join(state.runs_dir, safe_run_id)
    os.makedirs(record_dir, mode=0o755, exist_ok=True)

    record = {
        "run_id": run_id,
        "local_run_id": local_run_id,
        "envelope_id": envelope_id,
        "outcome": outcome.as_dict(),
    }

    record_path = os.path.join(record_dir, "run-record.json")
    with open(record_path, "w", encoding="utf-8") as recf:
        json.dump(record, recf, indent=4, sort_keys=True)
        recf.write(os.linesep)

    rtnval = record_path
    return rtnval


def execute_job_envelope(*, state: ClientState, envelope: Dict[str, Any],
                         local_run_id: str,
                         aspects: TestbaseExecutorAspects =
                         DEFAULT_TESTBASE_EXECUTOR_ASPECTS,
                         progress_reporter: Optional[Any] = None
                         ) -> TestbaseRunOutcome:
    """
        Executes a verified job envelope by shelling out to
        ``codecraftai-testbase`` and recording a local run record.

        The executor is resilient: a build error, a missing ``testbase``
        binary, a timeout, or a non-zero exit are each captured as a
        failed :class:`TestbaseRunOutcome` rather than raised — the
        client process must not crash on a bad dispatch.

        When a ``progress_reporter`` is supplied (an
        :class:`ccai.pychestrate.client.progress_reporter.ProgressReporter`)
        it is started immediately before the testbase subprocess launches
        and stopped once the process exits, so progress is streamed live to
        the service. Progress streaming is best-effort and never affects the
        run outcome.

        :param state: The :class:`ClientState` used to place run output.
        :param envelope: The verified envelope dict.
        :param local_run_id: The client-minted local run identifier.
        :param aspects: Behavior-shaping aspects.
        :param progress_reporter: Optional progress reporter started around
                                  the testbase subprocess.

        :returns: A :class:`TestbaseRunOutcome`.
    """
    run_id = str(envelope.get("run_id", ""))
    envelope_id = str(envelope.get("envelope_id", ""))

    output_dir = resolve_run_output_dir(state=state, run_id=run_id)

    try:
        command = build_testbase_command(
            envelope=envelope,
            output_dir=output_dir,
            testbase_binary=aspects.testbase_binary,
        )
    except BadParameterError as builderr:
        detail = "Failed to build testbase command: {}".format(builderr)
        logger.error("pychestrate-client: %s", detail)
        outcome = TestbaseRunOutcome(
            status=RUN_STATUS_FAILED, exit_code=None,
            output_dir=output_dir, command=[], detail=detail)
        write_local_run_record(
            state=state, run_id=run_id, local_run_id=local_run_id,
            envelope_id=envelope_id, outcome=outcome)
        return outcome

    logger.info(
        "pychestrate-client: executing testbase for run_id=%s -> %s",
        run_id, " ".join(command),
    )

    exit_code: Optional[int] = None
    status = RUN_STATUS_FAILED
    detail: Optional[str] = None

    if progress_reporter is not None:
        try:
            progress_reporter.start()
        except Exception as startt_err:
            # Progress is best-effort; a reporter start failure must not
            # prevent the run from executing.
            logger.warning(
                "pychestrate-client: progress reporter failed to start for "
                "run_id=%s (run continues): %s", run_id, startt_err,
            )

    try:
        completed = aspects.run_func(
            command,
            cwd=None,
            timeout=aspects.run_timeout_seconds,
            capture_output=True,
        )
        exit_code = getattr(completed, "returncode", None)
        if exit_code == 0:
            status = RUN_STATUS_SUCCEEDED
            detail = "testbase run completed with exit code 0."
        else:
            status = RUN_STATUS_FAILED
            detail = "testbase run exited with code {}.".format(exit_code)
    except FileNotFoundError as notfounderr:
        detail = (
            "The 'testbase' binary ({}) could not be found on this pod: "
            "{}".format(aspects.testbase_binary, notfounderr)
        )
        logger.error("pychestrate-client: %s", detail)
    except subprocess.TimeoutExpired as timeouterr:
        detail = (
            "The testbase run exceeded the {}s timeout: {}".format(
                aspects.run_timeout_seconds, timeouterr)
        )
        logger.error("pychestrate-client: %s", detail)
    except Exception as runerr:
        detail = "The testbase invocation raised an unexpected error: {}".format(
            runerr)
        logger.error("pychestrate-client: %s", detail)
    finally:
        if progress_reporter is not None:
            try:
                progress_reporter.stop()
            except Exception as stop_err:
                logger.warning(
                    "pychestrate-client: progress reporter failed to stop "
                    "cleanly for run_id=%s: %s", run_id, stop_err,
                )

    outcome = TestbaseRunOutcome(
        status=status, exit_code=exit_code, output_dir=output_dir,
        command=command, detail=detail)

    record_path = write_local_run_record(
        state=state, run_id=run_id, local_run_id=local_run_id,
        envelope_id=envelope_id, outcome=outcome)
    logger.info(
        "pychestrate-client: recorded local run outcome (status=%s) at %s",
        status, record_path,
    )

    rtnval = outcome
    return rtnval

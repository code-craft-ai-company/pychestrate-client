"""
    .. module:: rpa_executor
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Executes a dispatched ``job_kind=rpa`` envelope: a
                   Robotic Process Automation job identified by a job type
                   and version that receives a work packet and performs
                   the work in a subprocess.

                   Two execution modes share one path:

                   * ``process`` -- a simple process job. The work packet
                     is written to ``work_packet.json`` in the run
                     directory, exported through ``PYC_WORK_PACKET`` (path),
                     ``PYC_RUN_ID``, ``PYC_JOB_TYPE``, ``PYC_JOB_VERSION``,
                     and ``PYC_RESULTS_DIR``, delivered per
                     ``entrypoint.packet_delivery`` (``file`` / ``stdin`` /
                     ``env``), and the declared ``entrypoint.command`` runs
                     with the declared environment and timeout.
                   * ``tasker`` -- a tasker-based job. The work packet is
                     rendered into a taskplus workflow (``parameters``) and
                     the version's environment into its ``environment``;
                     the ``taskplus`` console script runs the rendered
                     workflow as a subprocess. The engine is never imported
                     into the agent process.

                   Exit code ``0`` maps to ``succeeded``, a timeout to
                   ``timeout``, and anything else to ``failed``. Captured
                   stdout / stderr and everything the job writes under
                   ``PYC_RESULTS_DIR`` form the result tree the uploader
                   walks.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2026, Automation Mojo LLC"
__credits__ = []


from typing import Any, Callable, Dict, List, Optional

import json
import logging
import os
import signal
import subprocess
import threading
import time

from ccai.errors.exceptions import BadParameterError

from ccai.pychestrate.client.build_executor import (
    DEFAULT_CLONE_TIMEOUT_SECONDS,
    DEFAULT_GIT_BINARY,
    SourceAcquisitionResult,
    acquire_source,
)
from ccai.pychestrate.client.state import ClientState


logger = logging.getLogger(__name__)

#: The ``taskplus`` console script the tasker mode shells out to.
DEFAULT_TASKPLUS_BINARY = "taskplus"

#: Wall-clock cap applied when the envelope declares no timeout.
DEFAULT_RPA_TIMEOUT_SECONDS = 3600.0

#: Local RPA run-record status codes.
RPA_STATUS_SUCCEEDED = "succeeded"
RPA_STATUS_FAILED = "failed"
RPA_STATUS_TIMEOUT = "timeout"
RPA_STATUS_CANCELLED = "cancelled"

#: Seconds between cancel checks while streaming a job's output.
DEFAULT_CANCEL_POLL_SECONDS = 2.0

#: Seconds a cancelled process gets to exit after SIGTERM before SIGKILL.
DEFAULT_CANCEL_GRACE_SECONDS = 10.0

#: Execution modes the executor understands.
EXECUTION_MODE_PROCESS = "process"
EXECUTION_MODE_TASKER = "tasker"

#: Packet delivery choices for process jobs.
PACKET_DELIVERY_FILE = "file"
PACKET_DELIVERY_STDIN = "stdin"
PACKET_DELIVERY_ENV = "env"

#: Sub-directory (under the client state directory) holding per-job local
#: execution records. Mirrors the ``builds`` layout.
DIRNAME_RPA = "rpa"

#: File names inside the run directory.
WORK_PACKET_FILENAME = "work_packet.json"
RENDERED_WORKFLOW_FILENAME = "workflow.rendered.yaml"
RECORD_FILENAME = "rpa-record.json"


class RpaExecutorAspects:
    """
        The :class:`RpaExecutorAspects` packages caller-controlled behavior
        for the RPA executor (binary names, timeouts, and the
        subprocess-runner override used by tests).
    """

    def __init__(self, *, taskplus_binary: str = DEFAULT_TASKPLUS_BINARY,
                 git_binary: str = DEFAULT_GIT_BINARY,
                 default_timeout_seconds: float = DEFAULT_RPA_TIMEOUT_SECONDS,
                 clone_timeout_seconds: float = DEFAULT_CLONE_TIMEOUT_SECONDS,
                 run_func: Optional[Callable[..., Any]] = None,
                 popen_func: Optional[Callable[..., Any]] = None,
                 stream_output: bool = True,
                 cancel_check_func: Optional[Callable[[], bool]] = None,
                 cancel_poll_seconds: float = DEFAULT_CANCEL_POLL_SECONDS,
                 cancel_grace_seconds: float = DEFAULT_CANCEL_GRACE_SECONDS):
        """
            Initializes a :class:`RpaExecutorAspects` instance.

            :param taskplus_binary: The ``taskplus`` console-script name or
                                    absolute path used by tasker jobs.
            :param git_binary: The ``git`` console-script name or absolute
                               path used for source acquisition.
            :param default_timeout_seconds: Wall-clock cap applied when the
                                            envelope declares no
                                            ``timeout_minutes``.
            :param clone_timeout_seconds: Wall-clock cap for each git
                                          source-acquisition subprocess.
            :param run_func: Optional subprocess-runner override with the
                             :func:`subprocess.run` signature. Injected
                             for testability; defaults to
                             :func:`subprocess.run`. When supplied the
                             executor uses it (no streaming).
            :param popen_func: Optional :class:`subprocess.Popen` override
                               used for the streaming path.
            :param stream_output: Stream captured output lines to the
                                  progress reporter while the job runs
                                  (default). ``False`` captures at exit.
            :param cancel_check_func: Optional callable polled while the
                                      job runs; returning ``True``
                                      terminates the process group.
            :param cancel_poll_seconds: Seconds between cancel checks.
            :param cancel_grace_seconds: Seconds a cancelled process gets
                                         after SIGTERM before SIGKILL.

            :raises: BadParameterError
        """
        if len(taskplus_binary) == 0:
            raise BadParameterError(
                "RpaExecutorAspects requires a non-empty 'taskplus_binary'.",
                "taskplus_binary", "Pass the taskplus console-script name.")
        if len(git_binary) == 0:
            raise BadParameterError(
                "RpaExecutorAspects requires a non-empty 'git_binary'.",
                "git_binary", "Pass the git console-script name.")
        if default_timeout_seconds <= 0:
            raise BadParameterError(
                "RpaExecutorAspects requires 'default_timeout_seconds' > 0.",
                "default_timeout_seconds", "Pass a positive number of seconds.")
        if clone_timeout_seconds <= 0:
            raise BadParameterError(
                "RpaExecutorAspects requires 'clone_timeout_seconds' > 0.",
                "clone_timeout_seconds", "Pass a positive number of seconds.")

        self._taskplus_binary = taskplus_binary
        self._git_binary = git_binary
        self._default_timeout_seconds = default_timeout_seconds
        self._clone_timeout_seconds = clone_timeout_seconds
        self._run_func = run_func if run_func is not None else subprocess.run
        self._run_func_injected = run_func is not None
        self._popen_func = popen_func if popen_func is not None else subprocess.Popen
        self._stream_output = bool(stream_output)
        self._cancel_check_func = cancel_check_func
        self._cancel_poll_seconds = max(0.1, float(cancel_poll_seconds))
        self._cancel_grace_seconds = max(0.0, float(cancel_grace_seconds))
        return

    @property
    def taskplus_binary(self) -> str:
        return self._taskplus_binary

    @property
    def git_binary(self) -> str:
        return self._git_binary

    @property
    def default_timeout_seconds(self) -> float:
        return self._default_timeout_seconds

    @property
    def clone_timeout_seconds(self) -> float:
        return self._clone_timeout_seconds

    @property
    def run_func(self) -> Callable[..., Any]:
        return self._run_func

    @property
    def popen_func(self) -> Callable[..., Any]:
        return self._popen_func

    @property
    def stream_output(self) -> bool:
        return self._stream_output

    @property
    def uses_streaming(self) -> bool:
        """
            Whether the executor streams through ``popen_func``: streaming
            is on and no ``run_func`` override was injected.
        """
        rtnval = self._stream_output and not self._run_func_injected
        return rtnval

    @property
    def cancel_check_func(self) -> Optional[Callable[[], bool]]:
        return self._cancel_check_func

    @property
    def cancel_poll_seconds(self) -> float:
        return self._cancel_poll_seconds

    @property
    def cancel_grace_seconds(self) -> float:
        return self._cancel_grace_seconds

    def with_cancel_check(self, cancel_check_func: Optional[Callable[[], bool]]) -> "RpaExecutorAspects":
        """
            A copy of these aspects bound to a per-run cancel predicate.
        """
        rtnval = RpaExecutorAspects(
            taskplus_binary=self._taskplus_binary,
            git_binary=self._git_binary,
            default_timeout_seconds=self._default_timeout_seconds,
            clone_timeout_seconds=self._clone_timeout_seconds,
            run_func=self._run_func if self._run_func_injected else None,
            popen_func=self._popen_func,
            stream_output=self._stream_output,
            cancel_check_func=cancel_check_func,
            cancel_poll_seconds=self._cancel_poll_seconds,
            cancel_grace_seconds=self._cancel_grace_seconds,
        )
        return rtnval


DEFAULT_RPA_EXECUTOR_ASPECTS = RpaExecutorAspects()


class RpaRunOutcome:
    """
        The :class:`RpaRunOutcome` captures the local result of a single
        dispatched RPA job execution.
    """

    def __init__(self, *, status: str, exit_code: Optional[int],
                 output_dir: str, work_dir: str, command: List[str],
                 execution_mode: str, type_id: str, version: str,
                 detail: Optional[str] = None):
        """
            Initializes a :class:`RpaRunOutcome`.

            :param status: ``RPA_STATUS_SUCCEEDED``, ``RPA_STATUS_FAILED``,
                           or ``RPA_STATUS_TIMEOUT``.
            :param exit_code: The job process exit code, or ``None`` when
                              the process could not be started.
            :param output_dir: Absolute path of the results directory (the
                               directory the result uploader walks).
            :param work_dir: Absolute path the job process ran in.
            :param command: The argv list used for the invocation.
            :param execution_mode: ``process`` or ``tasker``.
            :param type_id: The job type the envelope declared.
            :param version: The job type version the envelope declared.
            :param detail: Optional human-readable detail for logging.
        """
        self._status = status
        self._exit_code = exit_code
        self._output_dir = output_dir
        self._work_dir = work_dir
        self._command = list(command)
        self._execution_mode = execution_mode
        self._type_id = type_id
        self._version = version
        self._detail = detail
        return

    @property
    def status(self) -> str:
        return self._status

    @property
    def succeeded(self) -> bool:
        rtnval = self._status == RPA_STATUS_SUCCEEDED
        return rtnval

    @property
    def timed_out(self) -> bool:
        rtnval = self._status == RPA_STATUS_TIMEOUT
        return rtnval

    @property
    def cancelled(self) -> bool:
        rtnval = self._status == RPA_STATUS_CANCELLED
        return rtnval

    @property
    def exit_code(self) -> Optional[int]:
        return self._exit_code

    @property
    def output_dir(self) -> str:
        return self._output_dir

    @property
    def work_dir(self) -> str:
        return self._work_dir

    @property
    def command(self) -> List[str]:
        return self._command

    @property
    def execution_mode(self) -> str:
        return self._execution_mode

    @property
    def type_id(self) -> str:
        return self._type_id

    @property
    def version(self) -> str:
        return self._version

    @property
    def detail(self) -> Optional[str]:
        return self._detail

    def as_dict(self) -> Dict[str, Any]:
        rtnval = {
            "status": self._status,
            "exit_code": self._exit_code,
            "output_dir": self._output_dir,
            "work_dir": self._work_dir,
            "command": list(self._command),
            "execution_mode": self._execution_mode,
            "type_id": self._type_id,
            "version": self._version,
            "detail": self._detail,
        }
        return rtnval


# ---------------------------------------------------------------------------
# Envelope helpers
# ---------------------------------------------------------------------------


def extract_rpa_payload(envelope: Dict[str, Any]) -> Dict[str, Any]:
    """
        Pulls the RPA payload out of a verified ``job_kind=rpa`` envelope's
        ``params`` section.

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


def _payload_dict(payload: Dict[str, Any], key: str) -> Dict[str, Any]:
    raw = payload.get(key, None)
    if isinstance(raw, dict):
        return raw
    rtnval: Dict[str, Any] = {}
    return rtnval


def _payload_env(payload: Dict[str, Any]) -> Dict[str, str]:
    rtnval: Dict[str, str] = {}
    for key, value in _payload_dict(payload, "environment").items():
        rtnval[str(key)] = str(value)
    return rtnval


def resolve_timeout_seconds(payload: Dict[str, Any], aspects: RpaExecutorAspects) -> float:
    """
        The wall-clock cap for the job process: ``timeout_minutes`` from
        the payload, else the aspects default.
    """
    raw = payload.get("timeout_minutes", None)
    try:
        minutes = float(raw) if raw is not None else 0.0
    except (TypeError, ValueError):
        minutes = 0.0
    if minutes <= 0:
        rtnval = aspects.default_timeout_seconds
        return rtnval
    rtnval = minutes * 60.0
    return rtnval


def resolve_rpa_dirs(state: ClientState, run_id: str) -> Dict[str, str]:
    """
        Builds and creates the per-job directories under the client state
        ``rpa`` directory::

            <state>/rpa/<run_id>/
                source/   - the acquired source tree (git versions)
                work/     - the run directory (packet, rendered workflow)
                results/  - PYC_RESULTS_DIR, uploaded as the result tree

        :param state: The :class:`ClientState`.
        :param run_id: The server-assigned run UUID.

        :returns: A dict with ``record_dir``, ``source_dir``, ``work_dir``
                  and ``output_dir`` absolute paths.
    """
    safe_run_id = run_id if len(run_id) > 0 else "unknown-rpa"
    record_dir = os.path.join(state.state_dir, DIRNAME_RPA, safe_run_id)
    source_dir = os.path.join(record_dir, "source")
    work_dir = os.path.join(record_dir, "work")
    output_dir = os.path.join(record_dir, "results")

    os.makedirs(source_dir, mode=0o755, exist_ok=True)
    os.makedirs(work_dir, mode=0o755, exist_ok=True)
    os.makedirs(output_dir, mode=0o755, exist_ok=True)

    rtnval = {
        "record_dir": record_dir,
        "source_dir": source_dir,
        "work_dir": work_dir,
        "output_dir": output_dir,
    }
    return rtnval


def write_work_packet(*, work_dir: str, work_packet: Dict[str, Any]) -> str:
    """
        Materialises the work packet as ``work_packet.json`` in the run
        directory.

        :returns: The absolute path of the packet file.
    """
    packet_path = os.path.join(work_dir, WORK_PACKET_FILENAME)
    with open(packet_path, "w", encoding="utf-8") as pf:
        json.dump(work_packet, pf, indent=2, sort_keys=True)
        pf.write(os.linesep)
    rtnval = packet_path
    return rtnval


def acquire_rpa_source(*, envelope: Dict[str, Any], payload: Dict[str, Any],
                       source_dir: str, work_dir: str,
                       aspects: RpaExecutorAspects) -> SourceAcquisitionResult:
    """
        Puts the job type version's code in place.

        * ``git`` -- the envelope's structured ``source`` block is cloned
          and checked out through the build executor's acquisition path.
        * ``local`` -- ``source.ref`` names a directory already present on
          the pod; it is used as the working directory.
        * ``package`` -- the code is expected to be installed on the pod;
          the run directory is the working directory.

        :returns: A :class:`SourceAcquisitionResult`.
    """
    source = _payload_dict(payload, "source")
    kind = str(source.get("kind", "git") or "git")
    ref = str(source.get("ref", "") or "")

    if kind == "local":
        if len(ref) > 0 and os.path.isdir(ref):
            rtnval = SourceAcquisitionResult(
                acquired=True, source_dir=ref,
                detail="using local source tree at {}".format(ref))
            return rtnval
        rtnval = SourceAcquisitionResult(
            acquired=False, source_dir=work_dir,
            detail="the local source directory does not exist on this pod: {}".format(ref))
        return rtnval

    if kind == "package":
        rtnval = SourceAcquisitionResult(
            acquired=True, source_dir=work_dir,
            detail="package source '{}' is expected to be installed on the pod".format(ref))
        return rtnval

    if envelope.get("source", None) is None:
        rtnval = SourceAcquisitionResult(
            acquired=False, source_dir=source_dir,
            detail="git source declared but the envelope carries no 'source' block")
        return rtnval

    rtnval = acquire_source(
        envelope=envelope, build_spec={}, source_dir=source_dir, aspects=aspects)
    return rtnval


def _resolve_working_dir(*, entrypoint: Dict[str, Any], source_dir: str) -> str:
    working_dir = str(entrypoint.get("working_dir", ".") or ".")
    if os.path.isabs(working_dir):
        rtnval = working_dir
    else:
        rtnval = os.path.normpath(os.path.join(source_dir, working_dir))
    return rtnval


def build_process_command(*, entrypoint: Dict[str, Any]) -> List[str]:
    """
        The argv for a process job.

        :raises: BadParameterError when ``entrypoint.command`` is missing
                 or empty.
    """
    command = entrypoint.get("command", None)
    if not isinstance(command, list) or len(command) == 0:
        raise BadParameterError(
            "A process RPA job requires a non-empty 'entrypoint.command'.",
            "entrypoint.command", "Publish the job type version with an argv list.")
    rtnval = [str(part) for part in command]
    return rtnval


def _yaml_module() -> Any:
    try:
        import yaml  # noqa: E402
    except ImportError as importerr:  # pragma: no cover - pyyaml is a dependency
        raise BadParameterError(
            "The tasker execution mode requires PyYAML to render workflows.",
            "yaml", "Install pyyaml on the pod.") from importerr
    return yaml


def render_taskplus_workflow(*, entrypoint: Dict[str, Any], payload: Dict[str, Any],
                             work_packet: Dict[str, Any], source_dir: str,
                             work_dir: str) -> str:
    """
        Renders the taskplus workflow a tasker job runs.

        With ``entrypoint.workflow`` the YAML file inside the source tree
        is loaded and its ``parameters`` / ``environment`` maps are merged
        with the work packet and the version's environment (packet keys
        win). With ``entrypoint.tasking`` a one-task workflow wrapping the
        ``module@Class`` tasking is generated.

        :returns: The absolute path of ``workflow.rendered.yaml``.
        :raises: BadParameterError on a malformed entrypoint or workflow.
    """
    yaml = _yaml_module()
    workflow_rel = entrypoint.get("workflow", None)
    tasking = entrypoint.get("tasking", None)

    document: Dict[str, Any]
    if workflow_rel is not None and len(str(workflow_rel)) > 0:
        workflow_path = os.path.normpath(os.path.join(source_dir, str(workflow_rel)))
        if not os.path.isfile(workflow_path):
            raise BadParameterError(
                "The tasker workflow file was not found in the source tree: {}".format(
                    workflow_path),
                "entrypoint.workflow", "Publish a version whose workflow path exists.")
        with open(workflow_path, "r", encoding="utf-8") as wf:
            loaded = yaml.safe_load(wf)
        if not isinstance(loaded, dict):
            raise BadParameterError(
                "The tasker workflow file is not a YAML mapping: {}".format(workflow_path),
                "entrypoint.workflow", "Fix the workflow document.")
        document = loaded
    elif tasking is not None and len(str(tasking)) > 0:
        identity = str(tasking)
        class_name = identity.split("@", 1)[1] if "@" in identity else identity
        document = {
            "name": "rpa:{}@{}".format(
                str(payload.get("type_id", "job")), str(payload.get("version", ""))),
            "tasklist": [
                {"task": class_name, "tasktype": identity},
            ],
        }
    else:
        raise BadParameterError(
            "A tasker RPA job requires 'entrypoint.workflow' or 'entrypoint.tasking'.",
            "entrypoint", "Publish the job type version with one of them.")

    parameters = document.get("parameters", None)
    if not isinstance(parameters, dict):
        parameters = {}
    merged_parameters = dict(parameters)
    merged_parameters.update(work_packet)
    document["parameters"] = merged_parameters

    environment = document.get("environment", None)
    if not isinstance(environment, dict):
        environment = {}
    merged_env = dict(environment)
    merged_env.update(_payload_env(payload))
    document["environment"] = merged_env

    rendered_path = os.path.join(work_dir, RENDERED_WORKFLOW_FILENAME)
    with open(rendered_path, "w", encoding="utf-8") as rf:
        yaml.safe_dump(document, rf, sort_keys=False)
    rtnval = rendered_path
    return rtnval


def build_taskplus_command(*, taskplus_binary: str, rendered_workflow: str) -> List[str]:
    """
        The argv for a tasker job: ``taskplus workflow run <rendered>``.
    """
    rtnval = [taskplus_binary, "workflow", "run", rendered_workflow]
    return rtnval


def write_local_rpa_record(state: ClientState, run_id: str, local_run_id: str,
                           envelope_id: str, outcome: RpaRunOutcome) -> str:
    """
        Persists a local run record describing the dispatched RPA job's
        execution outcome. The result uploader merges its ``upload``
        section into the same file.

        :returns: The absolute path of the written record file.
    """
    safe_run_id = run_id if len(run_id) > 0 else "unknown-rpa"
    record_dir = os.path.join(state.state_dir, DIRNAME_RPA, safe_run_id)
    os.makedirs(record_dir, mode=0o755, exist_ok=True)

    record = {
        "run_id": run_id,
        "local_run_id": local_run_id,
        "envelope_id": envelope_id,
        "job_kind": "rpa",
        "outcome": outcome.as_dict(),
    }

    record_path = os.path.join(record_dir, RECORD_FILENAME)
    with open(record_path, "w", encoding="utf-8") as recf:
        json.dump(record, recf, indent=4, sort_keys=True)
        recf.write(os.linesep)

    rtnval = record_path
    return rtnval


class _StreamedCompletion:
    """
        ``CompletedProcess`` stand-in produced by the streaming runner.
    """

    def __init__(self, *, returncode: Optional[int], stdout: bytes, stderr: bytes,
                 timed_out: bool = False, cancelled: bool = False):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.timed_out = timed_out
        self.cancelled = cancelled
        return


def _terminate_process(process: Any, *, grace_seconds: float) -> None:
    """
        SIGTERM the process group (falling back to the process), wait
        ``grace_seconds``, then SIGKILL whatever is left.
    """
    def _signal(sig: int) -> None:
        try:
            pgid = os.getpgid(process.pid)
            os.killpg(pgid, sig)
        except Exception:
            try:
                process.send_signal(sig)
            except Exception:
                pass
    _signal(signal.SIGTERM)
    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        if process.poll() is not None:
            return
        time.sleep(0.1)
    if process.poll() is None:
        _signal(signal.SIGKILL)
        try:
            process.wait(timeout=5)
        except Exception:
            pass
    return


def run_streaming(*, command: List[str], cwd: str, env: Dict[str, str],
                  timeout_seconds: float, stdin_bytes: Optional[bytes],
                  on_line: Optional[Callable[[str, str], None]],
                  aspects: RpaExecutorAspects) -> _StreamedCompletion:
    """
        Runs ``command`` through ``aspects.popen_func`` with stderr merged
        into stdout, forwarding every line to ``on_line(line, stream)`` as
        it arrives, enforcing ``timeout_seconds``, and honouring
        ``aspects.cancel_check_func`` (polled every
        ``cancel_poll_seconds``).

        :returns: A :class:`_StreamedCompletion`.
        :raises: FileNotFoundError when the binary is missing (from Popen).
    """
    popen_kwargs: Dict[str, Any] = {
        "cwd": cwd,
        "env": env,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.STDOUT,
        "stdin": subprocess.PIPE if stdin_bytes is not None else subprocess.DEVNULL,
        "bufsize": 1,
        "text": False,
    }
    if os.name == "posix":
        popen_kwargs["start_new_session"] = True

    process = aspects.popen_func(command, **popen_kwargs)

    if stdin_bytes is not None:
        try:
            process.stdin.write(stdin_bytes)
            process.stdin.close()
        except Exception:
            pass

    captured: List[bytes] = []
    reader_done = threading.Event()

    def _reader() -> None:
        try:
            stream = process.stdout
            if stream is not None:
                for raw in iter(stream.readline, b""):
                    captured.append(raw)
                    if on_line is not None:
                        try:
                            on_line(raw.decode("utf-8", errors="replace").rstrip("\r\n"), "stdout")
                        except Exception:
                            pass
        finally:
            reader_done.set()

    reader = threading.Thread(target=_reader, name="rpa-output-reader", daemon=True)
    reader.start()

    deadline = time.monotonic() + timeout_seconds
    timed_out = False
    cancelled = False
    next_cancel_check = time.monotonic() + aspects.cancel_poll_seconds
    while process.poll() is None:
        now = time.monotonic()
        if now >= deadline:
            timed_out = True
            _terminate_process(process, grace_seconds=aspects.cancel_grace_seconds)
            break
        if aspects.cancel_check_func is not None and now >= next_cancel_check:
            next_cancel_check = now + aspects.cancel_poll_seconds
            try:
                if aspects.cancel_check_func() is True:
                    cancelled = True
                    _terminate_process(process, grace_seconds=aspects.cancel_grace_seconds)
                    break
            except Exception:
                pass
        time.sleep(0.05)

    reader_done.wait(timeout=5.0)
    returncode = process.poll()
    if returncode is None:
        try:
            returncode = process.wait(timeout=5)
        except Exception:
            returncode = None
    rtnval = _StreamedCompletion(
        returncode=returncode, stdout=b"".join(captured), stderr=b"",
        timed_out=timed_out, cancelled=cancelled,
    )
    return rtnval


def _last_line_of(path: str, limit: int = 200) -> str:
    """
        The last non-empty line of a text file (read from its last 4 KB), cut to ``limit`` characters; "" when the
        file is missing or empty.
    """
    try:
        with open(path, "rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(0, size - 4096))
            tail = handle.read().decode("utf-8", errors="replace")
    except OSError:
        return ""
    lines = [line.strip() for line in tail.splitlines() if len(line.strip()) > 0]
    if len(lines) == 0:
        return ""
    rtnval = lines[-1] if len(lines[-1]) <= limit else lines[-1][: limit - 1] + "\u2026"
    return rtnval


def _capture_process_output(*, output_dir: str, completed: Optional[Any],
                            command: List[str], exit_code: Optional[int],
                            detail: Optional[str], execution_mode: str) -> None:
    """
        Writes ``stdout.log`` / ``stderr.log`` and an invocation summary
        into the results directory. A capture failure is logged and
        swallowed -- it must not change the job outcome.
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

        with open(os.path.join(output_dir, "stdout.log"), "wb") as outf:
            outf.write(stdout_bytes)
        with open(os.path.join(output_dir, "stderr.log"), "wb") as errf:
            errf.write(stderr_bytes)
        summary = {
            "command": list(command),
            "exit_code": exit_code,
            "execution_mode": execution_mode,
            "detail": detail,
        }
        with open(os.path.join(output_dir, "rpa-invocation.json"), "w",
                  encoding="utf-8") as sf:
            json.dump(summary, sf, indent=4, sort_keys=True)
            sf.write(os.linesep)
    except OSError as caperr:
        logger.warning(
            "pychestrate-client: failed to capture RPA output in %s: %s",
            output_dir, caperr,
        )
    return


def execute_rpa_envelope(*, state: ClientState, envelope: Dict[str, Any],
                         local_run_id: str,
                         aspects: RpaExecutorAspects = DEFAULT_RPA_EXECUTOR_ASPECTS,
                         progress_reporter: Optional[Any] = None) -> RpaRunOutcome:
    """
        Executes a verified ``job_kind=rpa`` envelope.

        The executor is resilient: a malformed payload, a source
        acquisition failure, a missing binary, a timeout, or a non-zero
        exit are each captured as a failed :class:`RpaRunOutcome` rather
        than raised -- the client process must not crash on a bad
        dispatch.

        :param state: The :class:`ClientState` used to place job output.
        :param envelope: The verified envelope dict.
        :param local_run_id: The client-minted local run identifier.
        :param aspects: Behavior-shaping aspects.
        :param progress_reporter: Optional reporter with ``report_running``
                                  / ``report_finished(succeeded=...)``.

        :returns: An :class:`RpaRunOutcome`.
    """
    run_id = str(envelope.get("run_id", ""))
    envelope_id = str(envelope.get("envelope_id", ""))

    dirs = resolve_rpa_dirs(state=state, run_id=run_id)
    output_dir = dirs["output_dir"]
    work_dir = dirs["work_dir"]
    source_dir = dirs["source_dir"]

    payload = extract_rpa_payload(envelope=envelope)
    execution_mode = str(payload.get("execution_mode", EXECUTION_MODE_PROCESS) or EXECUTION_MODE_PROCESS)
    type_id = str(payload.get("type_id", "") or "")
    version = str(payload.get("version", "") or "")
    entrypoint = _payload_dict(payload, "entrypoint")
    work_packet = _payload_dict(payload, "work_packet")

    def _fail(detail: str, command: List[str], cwd: str,
              status: str = RPA_STATUS_FAILED, exit_code: Optional[int] = None) -> RpaRunOutcome:
        logger.error("pychestrate-client: %s", detail)
        failed = RpaRunOutcome(
            status=status, exit_code=exit_code, output_dir=output_dir, work_dir=cwd,
            command=command, execution_mode=execution_mode, type_id=type_id,
            version=version, detail=detail)
        write_local_rpa_record(
            state=state, run_id=run_id, local_run_id=local_run_id,
            envelope_id=envelope_id, outcome=failed)
        if progress_reporter is not None:
            try:
                progress_reporter.report_finished(succeeded=False)
            except Exception:
                pass
        return failed

    if execution_mode not in (EXECUTION_MODE_PROCESS, EXECUTION_MODE_TASKER):
        rtnval = _fail(
            detail="Unsupported RPA execution mode '{}'.".format(execution_mode),
            command=[], cwd=work_dir)
        return rtnval

    acquisition = acquire_rpa_source(
        envelope=envelope, payload=payload, source_dir=source_dir, work_dir=work_dir,
        aspects=aspects)
    if not acquisition.acquired:
        rtnval = _fail(
            detail="RPA source acquisition failed: {}".format(acquisition.detail),
            command=[], cwd=work_dir)
        return rtnval
    resolved_source_dir = acquisition.source_dir

    try:
        packet_path = write_work_packet(work_dir=work_dir, work_packet=work_packet)
    except OSError as packeterr:
        rtnval = _fail(
            detail="Failed to write the work packet: {}".format(packeterr),
            command=[], cwd=work_dir)
        return rtnval

    job_env = dict(os.environ)
    for env_key, env_value in _payload_env(payload).items():
        job_env[env_key] = env_value
    job_env["PYC_WORK_PACKET"] = packet_path
    job_env["PYC_RUN_ID"] = run_id
    job_env["PYC_JOB_TYPE"] = type_id
    job_env["PYC_JOB_VERSION"] = version
    job_env["PYC_RESULTS_DIR"] = output_dir
    job_env["PYC_EXECUTION_MODE"] = execution_mode

    stdin_bytes: Optional[bytes] = None
    try:
        if execution_mode == EXECUTION_MODE_PROCESS:
            command = build_process_command(entrypoint=entrypoint)
            cwd = _resolve_working_dir(entrypoint=entrypoint, source_dir=resolved_source_dir)
            delivery = str(entrypoint.get("packet_delivery", PACKET_DELIVERY_FILE) or PACKET_DELIVERY_FILE)
            packet_json = json.dumps(work_packet, sort_keys=True)
            if delivery == PACKET_DELIVERY_STDIN:
                stdin_bytes = packet_json.encode("utf-8")
            elif delivery == PACKET_DELIVERY_ENV:
                job_env["PYC_WORK_PACKET_JSON"] = packet_json
        else:
            rendered = render_taskplus_workflow(
                entrypoint=entrypoint, payload=payload, work_packet=work_packet,
                source_dir=resolved_source_dir, work_dir=work_dir)
            command = build_taskplus_command(
                taskplus_binary=aspects.taskplus_binary, rendered_workflow=rendered)
            cwd = resolved_source_dir
    except BadParameterError as prepareerr:
        rtnval = _fail(
            detail="Failed to prepare the RPA job: {}".format(prepareerr),
            command=[], cwd=work_dir)
        return rtnval

    if not os.path.isdir(cwd):
        rtnval = _fail(
            detail="The RPA working directory does not exist: {}".format(cwd),
            command=command, cwd=cwd)
        return rtnval

    timeout_seconds = resolve_timeout_seconds(payload=payload, aspects=aspects)

    logger.info(
        "pychestrate-client: executing %s RPA job type=%s version=%s run_id=%s in %s -> %s",
        execution_mode, type_id, version, run_id, cwd, " ".join(command),
    )

    if progress_reporter is not None:
        try:
            progress_reporter.report_running()
        except Exception as progerr:
            logger.warning(
                "pychestrate-client: RPA progress 'running' report failed for run_id=%s"
                " (job continues): %s", run_id, progerr,
            )

    exit_code: Optional[int] = None
    status = RPA_STATUS_FAILED
    detail: Optional[str] = None
    completed: Optional[Any] = None

    run_kwargs: Dict[str, Any] = {
        "cwd": cwd,
        "env": job_env,
        "timeout": timeout_seconds,
        "capture_output": True,
    }
    if stdin_bytes is not None:
        run_kwargs["input"] = stdin_bytes

    def _forward_line(line: str, stream: str) -> None:
        progress_reporter.report_output_line(line, stream=stream)

    line_sink: Optional[Callable[[str, str], None]] = None
    if progress_reporter is not None and hasattr(progress_reporter, "report_output_line"):
        line_sink = _forward_line

    try:
        if aspects.uses_streaming:
            completed = run_streaming(
                command=command, cwd=cwd, env=job_env, timeout_seconds=timeout_seconds,
                stdin_bytes=stdin_bytes, on_line=line_sink, aspects=aspects)
            exit_code = completed.returncode
            if completed.cancelled:
                status = RPA_STATUS_CANCELLED
                detail = "RPA job cancelled by operator request."
            elif completed.timed_out:
                status = RPA_STATUS_TIMEOUT
                detail = "The RPA job exceeded the {}s timeout.".format(timeout_seconds)
            elif exit_code == 0:
                status = RPA_STATUS_SUCCEEDED
                detail = "RPA job completed with exit code 0."
            else:
                status = RPA_STATUS_FAILED
                detail = "RPA job exited with code {}.".format(exit_code)
        else:
            completed = aspects.run_func(command, **run_kwargs)
            exit_code = getattr(completed, "returncode", None)
            if line_sink is not None:
                raw_out = getattr(completed, "stdout", b"") or b""
                if isinstance(raw_out, str):
                    raw_out = raw_out.encode("utf-8")
                for raw_line in raw_out.splitlines():
                    line_sink(raw_line.decode("utf-8", errors="replace"), "stdout")
            if exit_code == 0:
                status = RPA_STATUS_SUCCEEDED
                detail = "RPA job completed with exit code 0."
            else:
                status = RPA_STATUS_FAILED
                detail = "RPA job exited with code {}.".format(exit_code)
    except FileNotFoundError as notfounderr:
        detail = (
            "The RPA job binary ({}) could not be found on this pod: {}".format(
                command[0], notfounderr)
        )
        logger.error("pychestrate-client: %s", detail)
    except subprocess.TimeoutExpired as timeouterr:
        status = RPA_STATUS_TIMEOUT
        detail = "The RPA job exceeded the {}s timeout: {}".format(timeout_seconds, timeouterr)
        logger.error("pychestrate-client: %s", detail)
    except Exception as runerr:
        detail = "The RPA job invocation raised an unexpected error: {}".format(runerr)
        logger.error("pychestrate-client: %s", detail)

    _capture_process_output(
        output_dir=output_dir, completed=completed, command=command, exit_code=exit_code,
        detail=detail, execution_mode=execution_mode)

    # "exited with code 3" says little; the job's own last line of error output says why.
    if status == RPA_STATUS_FAILED and isinstance(exit_code, int) and exit_code != 0:
        last_error = _last_line_of(os.path.join(output_dir, "stderr.log"))
        if len(last_error) > 0:
            detail = "RPA job exited with code {}: {}".format(exit_code, last_error)

    outcome = RpaRunOutcome(
        status=status, exit_code=exit_code, output_dir=output_dir, work_dir=cwd,
        command=command, execution_mode=execution_mode, type_id=type_id, version=version,
        detail=detail)

    record_path = write_local_rpa_record(
        state=state, run_id=run_id, local_run_id=local_run_id, envelope_id=envelope_id,
        outcome=outcome)
    logger.info(
        "pychestrate-client: recorded local RPA outcome (status=%s) at %s", status, record_path,
    )

    if progress_reporter is not None:
        try:
            if hasattr(progress_reporter, "flush"):
                progress_reporter.flush()
            progress_reporter.report_finished(succeeded=outcome.succeeded)
        except Exception as progerr:
            logger.warning(
                "pychestrate-client: RPA progress 'finished' report failed for run_id=%s: %s",
                run_id, progerr,
            )

    rtnval = outcome
    return rtnval

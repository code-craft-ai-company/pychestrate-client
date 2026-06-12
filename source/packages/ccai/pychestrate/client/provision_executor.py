"""
    .. module:: provision_executor
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Translates a verified ``job_kind=provision`` job envelope
                   into an infra-tool invocation and shells out to run it.

                   M6 scope: the executor consumes a ``job_kind=provision``
                   envelope dispatched by ``pychestrate-service``. The
                   envelope's ``params`` section carries the
                   provisioning-spec payload built by the service's
                   ``trigger_run`` -- ``environment_id``, ``action``
                   (``plan`` / ``apply`` / ``destroy``), ``driver_name``
                   (the infra tool), ``driver_inputs`` (the driver-specific
                   input map), and ``inputs_canonical_sha256``.

                   The executor resolves the ``driver_name`` to one of two
                   concrete driver shapes:

                       1. ``terraform`` -- the reference Terraform driver.
                          The ``driver_inputs`` carry ``working_directory``
                          (required), ``var_file``, ``backend_config`` and
                          ``parallelism``. The executor shells out to
                          ``terraform <action>`` in the working directory
                          with the requested var-file / parallelism / backend
                          config flags.
                       2. A generic ``command`` driver -- the
                          ``driver_inputs`` carry an explicit ``command``
                          (+ ``args``, ``working_dir``, ``env``); the
                          executor shells out exactly as specified. This is
                          the fallback for any driver that does not need a
                          tool-aware translation.

                   Richer per-tool driver support (ansible, pulumi, ...) is
                   a documented follow-up -- the generic command driver
                   already covers any tool that can be expressed as an argv.

                   The executor never crashes the client: an unknown driver,
                   a missing infra-tool binary, a timeout, or a non-zero
                   exit are each captured as a failed
                   :class:`ProvisionRunOutcome` rather than raised -- the
                   client process must not crash on a bad dispatch.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2026, Code Craft AI LLC"
__credits__ = []


from typing import Any, Callable, Dict, List, Optional

import json
import logging
import os
import subprocess

from ccai.errors.exceptions import BadParameterError

from ccai.pychestrate.client.state import ClientState


logger = logging.getLogger(__name__)

#: Name of the ``terraform`` binary the terraform driver shells out to.
DEFAULT_TERRAFORM_BINARY = "terraform"

#: Default per-provision wall-clock cap (seconds) for the infra-tool
#: subprocess.
DEFAULT_PROVISION_TIMEOUT_SECONDS = 7200.0

#: The Terraform driver name (mirrors the server's
#: ``terraform_driver.TERRAFORM_DRIVER_NAME``).
TERRAFORM_DRIVER_NAME = "terraform"

#: The generic command driver name -- the fallback shape for any infra
#: tool that does not need a tool-aware translation. The ``driver_inputs``
#: carry an explicit ``command`` argv.
COMMAND_DRIVER_NAME = "command"

#: The provisioning actions the executor understands (mirrors the server's
#: ``InfraAction`` enum values).
PROVISION_ACTION_PLAN = "plan"
PROVISION_ACTION_APPLY = "apply"
PROVISION_ACTION_DESTROY = "destroy"
PROVISION_ACTIONS = (
    PROVISION_ACTION_PLAN,
    PROVISION_ACTION_APPLY,
    PROVISION_ACTION_DESTROY,
)

#: Local provision-run-record status codes.
PROVISION_STATUS_SUCCEEDED = "succeeded"
PROVISION_STATUS_FAILED = "failed"

#: Sub-directory (under the client state directory) holding per-provision
#: local execution records. Mirrors the build executor's ``builds``
#: directory.
DIRNAME_PROVISIONING = "provisioning"


class ProvisionExecutorAspects:
    """
        The :class:`ProvisionExecutorAspects` packages caller-controlled
        behavior for the provision executor (binary names, timeout, and
        the subprocess-runner override used by tests).

        Following the project's Aspects pattern, behavior modification is
        carried as data on a single parameter rather than via decorators.
    """

    def __init__(self, *, terraform_binary: str = DEFAULT_TERRAFORM_BINARY,
                 provision_timeout_seconds: float =
                 DEFAULT_PROVISION_TIMEOUT_SECONDS,
                 run_func: Optional[Callable[..., Any]] = None):
        """
            Initializes a :class:`ProvisionExecutorAspects` instance.

            :param terraform_binary: The ``terraform`` console-script name
                                     or absolute path.
            :param provision_timeout_seconds: Wall-clock cap for the
                                              infra-tool subprocess.
            :param run_func: Optional subprocess-runner override with the
                             :func:`subprocess.run` signature. Injected
                             for testability; defaults to
                             :func:`subprocess.run`.

            :raises: BadParameterError
        """
        if len(terraform_binary) == 0:
            raise BadParameterError(
                "ProvisionExecutorAspects requires a non-empty "
                "'terraform_binary'.",
                "terraform_binary", "Pass the terraform console-script name.")
        if provision_timeout_seconds <= 0:
            raise BadParameterError(
                "ProvisionExecutorAspects requires "
                "'provision_timeout_seconds' > 0.",
                "provision_timeout_seconds",
                "Pass a positive number of seconds.")

        self._terraform_binary = terraform_binary
        self._provision_timeout_seconds = provision_timeout_seconds
        self._run_func = run_func if run_func is not None else subprocess.run
        return

    @property
    def terraform_binary(self) -> str:
        return self._terraform_binary

    @property
    def provision_timeout_seconds(self) -> float:
        return self._provision_timeout_seconds

    @property
    def run_func(self) -> Callable[..., Any]:
        return self._run_func


DEFAULT_PROVISION_EXECUTOR_ASPECTS = ProvisionExecutorAspects()


class ProvisionRunOutcome:
    """
        The :class:`ProvisionRunOutcome` captures the local result of a
        single dispatched provision execution.
    """

    def __init__(self, *, status: str, exit_code: Optional[int],
                 output_dir: str, work_dir: str, command: List[str],
                 driver_name: str, action: str,
                 detail: Optional[str] = None):
        """
            Initializes a :class:`ProvisionRunOutcome`.

            :param status: ``PROVISION_STATUS_SUCCEEDED`` or
                           ``PROVISION_STATUS_FAILED``.
            :param exit_code: The infra-tool process exit code, or ``None``
                              when the process could not be started.
            :param output_dir: Absolute path of the provision output
                               directory (the directory the result
                               uploader walks).
            :param work_dir: Absolute path of the working directory the
                             infra tool was run in.
            :param command: The argv list used for the infra-tool
                            invocation.
            :param driver_name: The driver name the envelope declared.
            :param action: The provisioning action the envelope declared.
            :param detail: Optional human-readable detail for logging.
        """
        self._status = status
        self._exit_code = exit_code
        self._output_dir = output_dir
        self._work_dir = work_dir
        self._command = list(command)
        self._driver_name = driver_name
        self._action = action
        self._detail = detail
        return

    @property
    def status(self) -> str:
        return self._status

    @property
    def succeeded(self) -> bool:
        """
            Gets whether the provision run completed successfully.

            :returns: True when ``status`` is
                      ``PROVISION_STATUS_SUCCEEDED``.
        """
        rtnval = self._status == PROVISION_STATUS_SUCCEEDED
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
    def driver_name(self) -> str:
        return self._driver_name

    @property
    def action(self) -> str:
        return self._action

    @property
    def detail(self) -> Optional[str]:
        return self._detail

    def as_dict(self) -> Dict[str, Any]:
        """
            Returns a JSON-serializable dictionary representation.

            :returns: A dict describing the provision run outcome.
        """
        rtnval = {
            "status": self._status,
            "exit_code": self._exit_code,
            "output_dir": self._output_dir,
            "work_dir": self._work_dir,
            "command": list(self._command),
            "driver_name": self._driver_name,
            "action": self._action,
            "detail": self._detail,
        }
        return rtnval


def _extract_provision_spec(envelope: Dict[str, Any]) -> Dict[str, Any]:
    """
        Pulls the provisioning-spec payload out of a verified
        ``job_kind=provision`` envelope's ``params`` section.

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


def _extract_driver_name(provision_spec: Dict[str, Any]) -> str:
    """
        Resolves the driver name from a provisioning-spec payload.

        :param provision_spec: The envelope ``params`` provisioning-spec
                               map.

        :returns: The driver-name string (possibly empty).
    """
    raw = provision_spec.get("driver_name", "")
    rtnval = str(raw)
    return rtnval


def _extract_action(provision_spec: Dict[str, Any]) -> str:
    """
        Resolves the provisioning action from a provisioning-spec payload.

        :param provision_spec: The envelope ``params`` provisioning-spec
                               map.

        :returns: The action string (possibly empty).
    """
    raw = provision_spec.get("action", "")
    rtnval = str(raw)
    return rtnval


def _extract_driver_inputs(provision_spec: Dict[str, Any]) -> Dict[str, Any]:
    """
        Extracts the driver-specific input map from a provisioning-spec
        payload.

        :param provision_spec: The envelope ``params`` provisioning-spec
                               map.

        :returns: The ``driver_inputs`` map (an empty dict when absent or
                  malformed).
    """
    raw = provision_spec.get("driver_inputs", None)
    if not isinstance(raw, dict):
        rtnval: Dict[str, Any] = {}
        return rtnval

    rtnval = raw
    return rtnval


def _extract_provision_env(driver_inputs: Dict[str, Any]) -> Dict[str, str]:
    """
        Extracts the provisioning environment overlay from the driver
        inputs.

        :param driver_inputs: The provisioning-spec ``driver_inputs`` map.

        :returns: A dict of string-to-string environment entries.
    """
    rtnval: Dict[str, str] = {}

    raw_env = driver_inputs.get("env", None)
    if raw_env is None:
        return rtnval
    if not isinstance(raw_env, dict):
        return rtnval

    for env_key, env_value in raw_env.items():
        rtnval[str(env_key)] = str(env_value)

    return rtnval


def resolve_provision_dirs(state: ClientState, run_id: str) -> Dict[str, str]:
    """
        Builds and creates the per-provision directories under the client
        state ``provisioning`` directory.

        The layout mirrors the build ``builds`` layout::

            <state>/provisioning/<run_id>/
                output/   - the provision output directory (uploaded tree)

        The infra tool runs in its own ``working_directory`` (carried on
        the driver inputs); only the output directory is created here.

        :param state: The :class:`ClientState`.
        :param run_id: The server-assigned run UUID.

        :returns: A dict with ``record_dir`` and ``output_dir`` absolute
                  paths.
    """
    safe_run_id = run_id if len(run_id) > 0 else "unknown-provision"
    record_dir = os.path.join(
        state.state_dir, DIRNAME_PROVISIONING, safe_run_id)
    output_dir = os.path.join(record_dir, "output")

    os.makedirs(output_dir, mode=0o755, exist_ok=True)

    rtnval = {
        "record_dir": record_dir,
        "output_dir": output_dir,
    }
    return rtnval


def _build_terraform_command(*, action: str, driver_inputs: Dict[str, Any],
                             terraform_binary: str) -> List[str]:
    """
        Translates a ``terraform`` provisioning spec into a ``terraform``
        argv list.

        The argv carries: the ``terraform <action>`` sub-command, the
        ``var_file`` (``-var-file``, when supplied), the ``parallelism``
        (``-parallelism``, when non-zero), and a non-interactive auto-approve
        flag for ``apply`` / ``destroy``. ``backend_config`` entries are
        passed as ``-backend-config`` flags.

        :param action: The provisioning action.
        :param driver_inputs: The provisioning-spec ``driver_inputs`` map.
        :param terraform_binary: The ``terraform`` console-script name or
                                 path.

        :returns: The argv list for the ``terraform`` invocation.
        :raises: BadParameterError when the action is unsupported or the
                 required ``working_directory`` is missing.
    """
    if action not in PROVISION_ACTIONS:
        errmsg_lines = [
            "The dispatched terraform provision envelope declares an "
            "unsupported action.",
            "    action: {}".format(action),
            "Supported actions: {}.".format(", ".join(PROVISION_ACTIONS)),
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise BadParameterError(
            errmsg, "action", "Dispatch a plan / apply / destroy action.")

    working_directory = driver_inputs.get("working_directory", None)
    if working_directory is None or len(str(working_directory)) == 0:
        errmsg_lines = [
            "The dispatched terraform provision envelope carries no "
            "'working_directory'.",
            "The server's TerraformInfraDriver requires a non-empty "
            "'working_directory' in 'driver_inputs'.",
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise BadParameterError(
            errmsg, "driver_inputs",
            "Include 'driver_inputs.working_directory' in the envelope.")

    rtnval: List[str] = [terraform_binary, action]

    if action in (PROVISION_ACTION_APPLY, PROVISION_ACTION_DESTROY):
        rtnval.append("-auto-approve")

    rtnval.append("-input=false")

    var_file = driver_inputs.get("var_file", None)
    if var_file is not None and len(str(var_file)) > 0:
        rtnval.append("-var-file={}".format(str(var_file)))

    parallelism = driver_inputs.get("parallelism", 0)
    try:
        parallelism_int = int(parallelism)
    except (TypeError, ValueError):
        parallelism_int = 0
    if parallelism_int > 0:
        rtnval.append("-parallelism={}".format(parallelism_int))

    backend_config = driver_inputs.get("backend_config", None)
    if isinstance(backend_config, dict):
        for backend_key in sorted(backend_config.keys()):
            backend_value = backend_config[backend_key]
            rtnval.append("-backend-config={}={}".format(
                str(backend_key), str(backend_value)))

    return rtnval


def _build_command_driver_command(
        *, driver_inputs: Dict[str, Any]) -> List[str]:
    """
        Translates a generic ``command`` provisioning spec into an argv
        list.

        The ``driver_inputs`` carry an explicit ``command`` (a string or
        an argv list) plus an optional ``args`` list. This is the fallback
        shape for any infra tool that does not need a tool-aware
        translation.

        :param driver_inputs: The provisioning-spec ``driver_inputs`` map.

        :returns: The argv list for the generic command invocation.
        :raises: BadParameterError when no ``command`` is supplied.
    """
    raw_command = driver_inputs.get("command", None)
    if raw_command is None:
        errmsg_lines = [
            "The dispatched generic-command provision envelope carries no "
            "'command'.",
            "The generic command driver requires a 'command' (a string or "
            "an argv list) in 'driver_inputs'.",
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise BadParameterError(
            errmsg, "driver_inputs",
            "Include 'driver_inputs.command' in the envelope.")

    rtnval: List[str] = []

    if isinstance(raw_command, list):
        for entry in raw_command:
            rtnval.append(str(entry))
    else:
        rtnval.append(str(raw_command))

    if len(rtnval) == 0:
        errmsg_lines = [
            "The dispatched generic-command provision envelope carries an "
            "empty 'command'.",
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise BadParameterError(
            errmsg, "driver_inputs",
            "Include a non-empty 'driver_inputs.command' in the envelope.")

    raw_args = driver_inputs.get("args", None)
    if isinstance(raw_args, list):
        for arg_entry in raw_args:
            rtnval.append(str(arg_entry))

    return rtnval


def build_provision_command(*, envelope: Dict[str, Any],
                            aspects: ProvisionExecutorAspects =
                            DEFAULT_PROVISION_EXECUTOR_ASPECTS
                            ) -> List[str]:
    """
        Translates a verified ``job_kind=provision`` envelope into an
        infra-tool argv list.

        The ``driver_name`` selects the translation: ``terraform`` builds a
        ``terraform <action>`` invocation; any other driver name falls
        through to the generic command driver, which shells out the
        explicit ``command`` carried on the driver inputs.

        :param envelope: The verified envelope dict.
        :param aspects: Behavior-shaping aspects.

        :returns: The argv list for the infra-tool invocation.
        :raises: BadParameterError when the envelope cannot be translated.
    """
    provision_spec = _extract_provision_spec(envelope=envelope)
    driver_name = _extract_driver_name(provision_spec=provision_spec)
    action = _extract_action(provision_spec=provision_spec)
    driver_inputs = _extract_driver_inputs(provision_spec=provision_spec)

    if driver_name == TERRAFORM_DRIVER_NAME:
        rtnval = _build_terraform_command(
            action=action, driver_inputs=driver_inputs,
            terraform_binary=aspects.terraform_binary)
        return rtnval

    if driver_name == COMMAND_DRIVER_NAME:
        rtnval = _build_command_driver_command(driver_inputs=driver_inputs)
        return rtnval

    # An unrecognized driver name still has a generic fallback: when the
    # driver inputs carry an explicit 'command' the executor shells it out
    # verbatim. Richer per-tool driver support is a documented follow-up.
    if driver_inputs.get("command", None) is not None:
        logger.info(
            "pychestrate-client: provision driver '%s' has no tool-aware "
            "translation; falling back to the generic command driver.",
            driver_name,
        )
        rtnval = _build_command_driver_command(driver_inputs=driver_inputs)
        return rtnval

    errmsg_lines = [
        "The dispatched provision envelope declares a driver the client "
        "cannot translate.",
        "    envelope_id: {}".format(envelope.get("envelope_id")),
        "    driver_name: {}".format(driver_name),
        "M6 supports the 'terraform' driver and a generic 'command' driver; "
        "other drivers must carry an explicit 'driver_inputs.command'. "
        "Richer per-tool driver support is a follow-up.",
    ]
    errmsg = os.linesep.join(errmsg_lines)
    raise BadParameterError(
        errmsg, "envelope",
        "Dispatch a 'terraform' / 'command' provision envelope, or include "
        "'driver_inputs.command'.")


def _resolve_work_dir(*, driver_name: str, driver_inputs: Dict[str, Any],
                      fallback_dir: str) -> str:
    """
        Resolves the working directory the infra tool runs in.

        The ``terraform`` driver runs in its ``working_directory``; the
        generic command driver runs in its ``working_dir`` when supplied.
        When neither is present the provision output directory is used as
        the working directory.

        :param driver_name: The provisioning driver name.
        :param driver_inputs: The provisioning-spec ``driver_inputs`` map.
        :param fallback_dir: The directory used when no working directory
                             is carried on the driver inputs.

        :returns: The absolute working-directory path.
    """
    if driver_name == TERRAFORM_DRIVER_NAME:
        working_directory = driver_inputs.get("working_directory", None)
        if working_directory is not None and len(str(working_directory)) > 0:
            rtnval = str(working_directory)
            return rtnval

    working_dir = driver_inputs.get("working_dir", None)
    if working_dir is not None and len(str(working_dir)) > 0:
        rtnval = str(working_dir)
        return rtnval

    rtnval = fallback_dir
    return rtnval


def write_local_provision_record(state: ClientState, run_id: str,
                                  local_run_id: str, envelope_id: str,
                                  outcome: ProvisionRunOutcome) -> str:
    """
        Persists a local provision run record describing the dispatched
        provision job's execution outcome.

        The record file is the per-provision counterpart of the build
        ``build-record.json``; the result uploader merges its ``upload``
        section into the same file after the output upload.

        :param state: The :class:`ClientState`.
        :param run_id: The server-assigned run UUID.
        :param local_run_id: The client-minted local run identifier.
        :param envelope_id: The dispatched envelope id.
        :param outcome: The :class:`ProvisionRunOutcome` to record.

        :returns: The absolute path of the written provision-record file.
    """
    safe_run_id = run_id if len(run_id) > 0 else "unknown-provision"
    record_dir = os.path.join(
        state.state_dir, DIRNAME_PROVISIONING, safe_run_id)
    os.makedirs(record_dir, mode=0o755, exist_ok=True)

    record = {
        "run_id": run_id,
        "local_run_id": local_run_id,
        "envelope_id": envelope_id,
        "job_kind": "provision",
        "outcome": outcome.as_dict(),
    }

    record_path = os.path.join(record_dir, "provision-record.json")
    with open(record_path, "w", encoding="utf-8") as recf:
        json.dump(record, recf, indent=4, sort_keys=True)
        recf.write(os.linesep)

    rtnval = record_path
    return rtnval


def _capture_provision_log(*, output_dir: str, completed: Optional[Any],
                           command: List[str], exit_code: Optional[int],
                           detail: Optional[str]) -> None:
    """
        Writes the provision run's captured stdout/stderr + an invocation
        summary into the provision output directory so the result uploader
        has a tree to walk.

        A capture failure is logged and swallowed -- it must not turn a
        successful provision run into a failed one.

        :param output_dir: The provision output directory.
        :param completed: The ``CompletedProcess`` (or ``None`` when the
                          process never started).
        :param command: The argv list used for the invocation.
        :param exit_code: The provision process exit code, or ``None``.
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

        stdout_path = os.path.join(output_dir, "provision-stdout.log")
        with open(stdout_path, "wb") as outf:
            outf.write(stdout_bytes)

        stderr_path = os.path.join(output_dir, "provision-stderr.log")
        with open(stderr_path, "wb") as errf:
            errf.write(stderr_bytes)

        summary = {
            "command": list(command),
            "exit_code": exit_code,
            "detail": detail,
        }
        summary_path = os.path.join(output_dir, "provision-summary.json")
        with open(summary_path, "w", encoding="utf-8") as sumf:
            json.dump(summary, sumf, indent=4, sort_keys=True)
            sumf.write(os.linesep)
    except OSError as captureerr:
        logger.warning(
            "pychestrate-client: failed to capture provision log into %s: %s",
            output_dir, captureerr,
        )

    return


def execute_provision_envelope(*, state: ClientState,
                                envelope: Dict[str, Any],
                                local_run_id: str,
                                aspects: ProvisionExecutorAspects =
                                DEFAULT_PROVISION_EXECUTOR_ASPECTS,
                                progress_reporter: Optional[Any] = None
                                ) -> ProvisionRunOutcome:
    """
        Executes a verified ``job_kind=provision`` envelope by translating
        the provisioning spec into an infra-tool invocation, shelling out
        to run it, and recording a local provision run record.

        The executor is resilient: an untranslatable envelope, a missing
        infra-tool binary, a timeout, or a non-zero exit are each captured
        as a failed :class:`ProvisionRunOutcome` rather than raised -- the
        client process must not crash on a bad dispatch.

        When a ``progress_reporter`` is supplied (a
        :class:`ccai.pychestrate.client.provision_progress_reporter.ProvisionProgressReporter`)
        its ``report_running`` / ``report_finished`` transitions are
        invoked around the provision run. Progress reporting is best-effort
        and never affects the provision outcome.

        :param state: The :class:`ClientState` used to place provision
                      output.
        :param envelope: The verified envelope dict.
        :param local_run_id: The client-minted local run identifier.
        :param aspects: Behavior-shaping aspects.
        :param progress_reporter: Optional provision progress reporter
                                  driven around the run.

        :returns: A :class:`ProvisionRunOutcome`.
    """
    run_id = str(envelope.get("run_id", ""))
    envelope_id = str(envelope.get("envelope_id", ""))

    provision_dirs = resolve_provision_dirs(state=state, run_id=run_id)
    output_dir = provision_dirs["output_dir"]

    provision_spec = _extract_provision_spec(envelope=envelope)
    driver_name = _extract_driver_name(provision_spec=provision_spec)
    action = _extract_action(provision_spec=provision_spec)
    driver_inputs = _extract_driver_inputs(provision_spec=provision_spec)

    work_dir = _resolve_work_dir(
        driver_name=driver_name, driver_inputs=driver_inputs,
        fallback_dir=output_dir)

    def _fail(detail: str, command: List[str]) -> ProvisionRunOutcome:
        logger.error("pychestrate-client: %s", detail)
        failed = ProvisionRunOutcome(
            status=PROVISION_STATUS_FAILED, exit_code=None,
            output_dir=output_dir, work_dir=work_dir, command=command,
            driver_name=driver_name, action=action, detail=detail)
        write_local_provision_record(
            state=state, run_id=run_id, local_run_id=local_run_id,
            envelope_id=envelope_id, outcome=failed)
        return failed

    try:
        command = build_provision_command(envelope=envelope, aspects=aspects)
    except BadParameterError as builderr:
        rtnval = _fail(
            detail="Failed to build provision command: {}".format(builderr),
            command=[])
        return rtnval

    logger.info(
        "pychestrate-client: executing provision (driver=%s action=%s) for "
        "run_id=%s in %s -> %s",
        driver_name, action, run_id, work_dir, " ".join(command),
    )

    provision_env = dict(os.environ)
    for env_key, env_value in _extract_provision_env(
            driver_inputs=driver_inputs).items():
        provision_env[env_key] = env_value

    if progress_reporter is not None:
        try:
            progress_reporter.report_running()
        except Exception as progerr:
            logger.warning(
                "pychestrate-client: provision progress 'running' report "
                "failed for run_id=%s (provision continues): %s",
                run_id, progerr,
            )

    exit_code: Optional[int] = None
    status = PROVISION_STATUS_FAILED
    detail: Optional[str] = None
    completed: Optional[Any] = None

    try:
        completed = aspects.run_func(
            command,
            cwd=work_dir,
            env=provision_env,
            timeout=aspects.provision_timeout_seconds,
            capture_output=True,
        )
        exit_code = getattr(completed, "returncode", None)
        if exit_code == 0:
            status = PROVISION_STATUS_SUCCEEDED
            detail = "provision run completed with exit code 0."
        else:
            status = PROVISION_STATUS_FAILED
            detail = "provision run exited with code {}.".format(exit_code)
    except FileNotFoundError as notfounderr:
        detail = (
            "The infra-tool binary ({}) could not be found on this pod: "
            "{}".format(command[0], notfounderr)
        )
        logger.error("pychestrate-client: %s", detail)
    except subprocess.TimeoutExpired as timeouterr:
        detail = (
            "The provision run exceeded the {}s timeout: {}".format(
                aspects.provision_timeout_seconds, timeouterr)
        )
        logger.error("pychestrate-client: %s", detail)
    except Exception as runerr:
        detail = (
            "The provision invocation raised an unexpected error: "
            "{}".format(runerr)
        )
        logger.error("pychestrate-client: %s", detail)

    _capture_provision_log(
        output_dir=output_dir, completed=completed, command=command,
        exit_code=exit_code, detail=detail)

    outcome = ProvisionRunOutcome(
        status=status, exit_code=exit_code, output_dir=output_dir,
        work_dir=work_dir, command=command, driver_name=driver_name,
        action=action, detail=detail)

    record_path = write_local_provision_record(
        state=state, run_id=run_id, local_run_id=local_run_id,
        envelope_id=envelope_id, outcome=outcome)
    logger.info(
        "pychestrate-client: recorded local provision outcome (status=%s) "
        "at %s", status, record_path,
    )

    if progress_reporter is not None:
        try:
            progress_reporter.report_finished(succeeded=outcome.succeeded)
        except Exception as progerr:
            logger.warning(
                "pychestrate-client: provision progress 'finished' report "
                "failed for run_id=%s: %s", run_id, progerr,
            )

    rtnval = outcome
    return rtnval

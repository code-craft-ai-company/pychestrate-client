"""
    .. module:: capabilities
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Advertises the pod client's capabilities to the service
                   (``PUT /api/v1/clients/{client_id}/capabilities``).

                   The first advertisement is the ``rpa`` kind: whether the
                   ``taskplus`` engine is on ``PATH`` (and its version), the
                   RPA job types the operator declared as installed on this
                   pod (``config.json`` ``rpa_job_types``), and free-form
                   capability tags (``config.json`` ``capability_tags``).
                   The service turns these into dispatch-matcher tags so a
                   job type or specification can require them.

                   Advertisement is best-effort: a REST client generated
                   before the operation existed, or a service that refuses
                   the body, is logged and the loops carry on.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2026, Automation Mojo LLC"
__credits__ = []


from typing import Any, Callable, Dict, List, Optional

import logging
import shutil
import subprocess

from automojo.pychestrate.client.state import ClientState


logger = logging.getLogger(__name__)

CAPABILITY_KIND_RPA = "rpa"
DEFAULT_TASKPLUS_BINARY = "taskplus"


def detect_taskplus_version(*, taskplus_binary: str = DEFAULT_TASKPLUS_BINARY,
                            run_func: Optional[Callable[..., Any]] = None,
                            which_func: Optional[Callable[[str], Optional[str]]] = None) -> Optional[str]:
    """
        The taskplus engine version on this pod, or ``None`` when the
        console script is absent. ``taskplus --version`` is tried; when it
        prints nothing usable the version is reported as ``"installed"``.
    """
    resolver = which_func if which_func is not None else shutil.which
    path = resolver(taskplus_binary)
    if path is None:
        return None
    runner = run_func if run_func is not None else subprocess.run
    try:
        completed = runner([path, "--version"], capture_output=True, timeout=10)
        output = getattr(completed, "stdout", b"") or b""
        if isinstance(output, bytes):
            output = output.decode("utf-8", errors="replace")
        text = str(output).strip().splitlines()
        version = text[-1].strip() if len(text) > 0 else ""
    except Exception:
        version = ""
    rtnval = version if len(version) > 0 else "installed"
    return rtnval


def build_rpa_capability(*, state: ClientState, taskplus_version: Optional[str]) -> Dict[str, Any]:
    """
        The ``rpa`` capability advertisement for this pod.
    """
    job_types: List[str] = []
    tags: List[str] = []
    try:
        config = state.read_config()
    except Exception:
        config = {}
    if isinstance(config, dict):
        raw_types = config.get("rpa_job_types", []) or []
        if isinstance(raw_types, list):
            job_types = [str(item) for item in raw_types if len(str(item)) > 0]
        raw_tags = config.get("capability_tags", []) or []
        if isinstance(raw_tags, list):
            tags = [str(item) for item in raw_tags if len(str(item)) > 0]
    rtnval = {
        "kind": CAPABILITY_KIND_RPA,
        "value": {
            "job_types": job_types,
            "taskplus": taskplus_version,
            "tags": tags,
        },
    }
    return rtnval


def advertise_capabilities(*, client: Any, client_id: str,
                           advertisements: List[Dict[str, Any]]) -> bool:
    """
        Send ``advertisements`` through the generated
        ``agents.agents_put_client_capabilities`` operation.

        :returns: True when the service accepted the advertisement.
    """
    body = {"advertisements": list(advertisements)}
    try:
        group = getattr(client, "agents", None)
        method = getattr(group, "agents_put_client_capabilities", None) if group is not None else None
        if method is None:
            logger.warning(
                "pychestrate-client: the REST client has no agents_put_client_capabilities operation;"
                " regenerate it from the current OpenAPI document to advertise capabilities.",
            )
            return False
        method(client_id=client_id, body=body)
    except ValueError:
        # An empty-body success decodes as ValueError in the generated client.
        return True
    except Exception as adverr:
        logger.warning(
            "pychestrate-client: capability advertisement failed for client_id=%s"
            " (best-effort, loops continue): %s", client_id, adverr,
        )
        return False
    logger.info(
        "pychestrate-client: advertised %d capability kind(s) for client_id=%s.",
        len(advertisements), client_id,
    )
    return True


def advertise_pod_capabilities(*, client: Any, state: ClientState, client_id: str,
                               taskplus_binary: str = DEFAULT_TASKPLUS_BINARY) -> bool:
    """
        Detect and advertise this pod's ``rpa`` capability.
    """
    version = detect_taskplus_version(taskplus_binary=taskplus_binary)
    advertisement = build_rpa_capability(state=state, taskplus_version=version)
    rtnval = advertise_capabilities(client=client, client_id=client_id, advertisements=[advertisement])
    return rtnval

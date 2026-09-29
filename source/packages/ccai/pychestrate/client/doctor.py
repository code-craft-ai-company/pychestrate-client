"""
    .. module:: doctor
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: The checks behind ``pychestrate-client doctor``: is this
                   machine's state directory a working enrollment, and can
                   the service be reached.

    Each check gives a level (``ok``, ``warn`` or ``fail``) and one line an
    operator can act on. The checks read only local files, plus one GET of
    the service's address; nothing is written except the trust bundle the
    client itself keeps beside the service CA.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2026, Automation Mojo LLC"
__credits__ = []


from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

import os

import requests

from cryptography import x509

from ccai.errors.exceptions import ConfigurationError

from ccai.pychestrate.client.enrollment import describe_request_failure
from ccai.pychestrate.client.pychestratemtlsclient import build_server_trust_bundle
from ccai.pychestrate.client.state import ClientState


LEVEL_OK = "ok"
LEVEL_WARN = "warn"
LEVEL_FAIL = "fail"

#: A client certificate with less life than this is reported as a warning.
CERTIFICATE_WARNING_DAYS = 14

#: How long the reachability check waits for the service.
REACHABILITY_TIMEOUT_SECONDS = 5.0


@dataclass
class Check:
    """
        One line of the report.

        :param level: ``ok``, ``warn`` or ``fail``.
        :param message: What was found, and what to do about it.
    """
    level: str
    message: str


def _certificate_check(*, state: ClientState, now: datetime) -> Check:
    """How long the client certificate has left."""
    try:
        with open(state.client_cert_path, "rb") as certfile:
            certificate = x509.load_pem_x509_certificate(certfile.read())
    except (OSError, ValueError) as readerr:
        return Check(LEVEL_FAIL, "the client certificate ({}) cannot be read as a certificate: {}. "
                                 "Enroll again with --force.".format(state.client_cert_path, readerr))
    expires = certificate.not_valid_after_utc
    days = (expires - now).days
    if expires <= now:
        return Check(LEVEL_FAIL, "the client certificate expired on {}. Enroll again with a new token and --force."
                     .format(expires.strftime("%Y-%m-%d")))
    if days < CERTIFICATE_WARNING_DAYS:
        return Check(LEVEL_WARN, "the client certificate expires in {} day(s), on {}. Enroll again before then."
                     .format(days, expires.strftime("%Y-%m-%d")))
    return Check(LEVEL_OK, "the client certificate is valid for {} more day(s) (until {}).".format(
        days, expires.strftime("%Y-%m-%d")))


def _reachability_check(*, state: ClientState, timeout_seconds: float) -> Check:
    """Whether the service answers at the address the client enrolled with."""
    try:
        config = state.read_config()
    except ConfigurationError as configerr:
        return Check(LEVEL_FAIL, "config.json cannot be read: {}".format(configerr))
    service_url = str(config.get("service_url", "") or "")
    if len(service_url) == 0:
        return Check(LEVEL_FAIL, "config.json has no service_url, so the service cannot be checked. Enroll again.")
    verify: object = True
    if os.path.isfile(state.service_ca_path):
        try:
            verify = build_server_trust_bundle(service_ca_path=state.service_ca_path)
        except OSError:
            verify = True
    try:
        response = requests.get(service_url, timeout=timeout_seconds, verify=verify, allow_redirects=False)
    except requests.exceptions.RequestException as requesterr:
        host = str(config.get("host", "") or service_url)
        return Check(LEVEL_FAIL, describe_request_failure(
            failure=requesterr, redeem_url=service_url, host=host, port=config.get("port"),
            timeout_seconds=timeout_seconds))
    return Check(LEVEL_OK, "the service at {} answered (HTTP {}).".format(service_url, response.status_code))


def run_checks(*, state: ClientState, check_service: bool = True, now: Optional[datetime] = None,
               timeout_seconds: float = REACHABILITY_TIMEOUT_SECONDS) -> List[Check]:
    """
        Examine a state directory.

        :param state: The state directory to examine.
        :param check_service: Also ask the service's address for an answer.
        :param now: The time to judge certificate expiry against (default now).
        :param timeout_seconds: How long the service may take to answer.

        :returns: The checks, in order.
    """
    when = now if now is not None else datetime.now(timezone.utc)
    checks: List[Check] = []

    if not os.path.isdir(state.state_dir):
        checks.append(Check(LEVEL_FAIL, "the state directory {} does not exist. This machine is not enrolled: "
                                        "run 'pychestrate-client enroll ...'.".format(state.state_dir)))
        return checks
    checks.append(Check(LEVEL_OK, "the state directory is {}.".format(state.state_dir)))

    required = [
        ("client id", state.client_id_path), ("private key", state.client_key_path),
        ("client certificate", state.client_cert_path), ("service CA", state.service_ca_path),
        ("config.json", state.config_path),
    ]
    missing = [name for name, path in required if not os.path.isfile(path)]
    if len(missing) > 0:
        checks.append(Check(LEVEL_FAIL, "not fully enrolled: missing {}. Run 'pychestrate-client enroll ...' "
                                        "(add --force to replace a partial enrollment).".format(", ".join(missing))))
        return checks
    checks.append(Check(LEVEL_OK, "the enrollment files are all present (client id {}).".format(
        state.read_client_id())))

    mode = os.stat(state.client_key_path).st_mode & 0o777
    if mode & 0o077 != 0:
        checks.append(Check(LEVEL_WARN, "the private key {} is readable by other users (mode {:o}). "
                                        "Run: chmod 600 {}".format(state.client_key_path, mode, state.client_key_path)))
    else:
        checks.append(Check(LEVEL_OK, "the private key is readable only by its owner."))

    checks.append(_certificate_check(state=state, now=when))

    if state.has_envelope_signing_jwks() or state.has_envelope_signing_keys() or state.has_envelope_signing_key():
        checks.append(Check(LEVEL_OK, "envelope signing keys are cached."))
    else:
        checks.append(Check(LEVEL_WARN, "no envelope signing keys are cached; the client fetches them from the "
                                        "service when it first runs, so this is expected on a machine that has "
                                        "not run yet."))

    if check_service:
        checks.append(_reachability_check(state=state, timeout_seconds=timeout_seconds))
    return checks


def exit_code_for(*, checks: List[Check]) -> int:
    """0 when nothing failed (warnings are allowed), 1 when any check failed."""
    for check in checks:
        if check.level == LEVEL_FAIL:
            return 1
    return 0

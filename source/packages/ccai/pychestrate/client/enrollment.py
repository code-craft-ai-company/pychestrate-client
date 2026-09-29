"""
    .. module:: enrollment
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Implements the one-time enrollment-token redemption flow.
                   Generates a fresh RSA keypair, builds a CSR, POSTs it (with
                   the bootstrap secret) to the Pychestrate service, then
                   persists the signed certificate + CA chain + assigned
                   client_id into the local state directory.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2025, Automation Mojo LLC"
__credits__ = []


from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

import logging
import re
import os

import requests

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509 import CertificateSigningRequestBuilder, Name, NameAttribute
from cryptography.x509.oid import NameOID

from ccai.errors.exceptions import BadParameterError, ConfigurationError, HTTPRequestError

from ccai.pychestrate.client.pychestratemtlsclient import DEFAULT_API_BASE_PATH, normalize_base_path
from ccai.pychestrate.client.signing_keys import persist_signing_keys_from_enrollment
from ccai.pychestrate.client.state import ClientState


DEFAULT_RSA_KEY_SIZE = 2048

#: How long the redeem call may take before enrollment gives up. Without a
#: limit a service that accepts the connection and never answers held the
#: command until the operator interrupted it.
DEFAULT_ENROLLMENT_TIMEOUT_SECONDS = 30.0

#: The longest slice of a service's error body an enrollment error repeats.
MAX_ERROR_BODY_CHARACTERS = 500

#: The most lines of a problem document's detail printed for a refused enrollment.
MAX_REFUSAL_DETAIL_LINES = 12

#: The redeem endpoint, relative to the API base path.
ENROLLMENT_REDEEM_LEAF = "/enrollment-tokens/redeem"
ENROLLMENT_REDEEM_PATH = DEFAULT_API_BASE_PATH + ENROLLMENT_REDEEM_LEAF

logger = logging.getLogger(__name__)


def generate_keypair_and_csr(client_name: str,
                             key_size: int = DEFAULT_RSA_KEY_SIZE) -> Tuple[bytes, bytes]:
    """
        Generates a fresh RSA private key and a PKCS#10 certificate signing
        request bound to the provided ``client_name`` as the X.509 common name.

        :param client_name: Pod label (the human-readable name) used as the CSR's
                            subject common name.
        :param key_size: RSA modulus size in bits. Default is 2048.

        :returns: A tuple of ``(private_key_pem_bytes, csr_pem_bytes)``.
    """
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=key_size)

    subject_name = Name([
        NameAttribute(NameOID.COMMON_NAME, client_name),
    ])

    csr_builder = CertificateSigningRequestBuilder()
    csr_builder = csr_builder.subject_name(subject_name)
    csr = csr_builder.sign(private_key=private_key, algorithm=hashes.SHA256())

    private_key_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    csr_pem = csr.public_bytes(encoding=serialization.Encoding.PEM)

    rtnval = (private_key_pem, csr_pem)
    return rtnval


def build_redeem_payload(bootstrap_secret: str, csr_pem: str, client_name: str,
                         attributes: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
        Constructs the request body for ``POST /enrollment-tokens/redeem``.

        :param bootstrap_secret: The one-time enrollment token secret obtained
                                 out-of-band from a Pychestrate administrator.
        :param csr_pem: PEM-encoded PKCS#10 certificate signing request.
        :param client_name: Pod label (human-readable name) to register.
        :param attributes: Optional attributes dictionary stored on the
                           registry row for this client.

        :returns: The JSON-serializable request body dictionary.
    """
    payload: Dict[str, Any] = {
        "bootstrap_secret": bootstrap_secret,
        "csr_pem": csr_pem,
        "display_name": client_name,
    }

    if attributes is not None:
        payload["attributes"] = attributes

    rtnval = payload
    return rtnval


def _derive_host_and_scheme(service_url: str) -> Tuple[str, str, Optional[int]]:
    """
        Splits the supplied service URL into ``(host, scheme, port)`` components.

        :param service_url: A full URL such as ``https://pychestrate.example.com``.

        :returns: A tuple of ``(host, scheme, port_or_none)``.
        :raises: BadParameterError
    """
    parsed = urlparse(service_url)

    if parsed.scheme not in ("http", "https"):
        errmsg_lines = [
            "The supplied service URL has an unsupported scheme.",
            "    service_url: {}".format(service_url),
            "    scheme: {!r}".format(parsed.scheme),
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise BadParameterError(
            errmsg, "service_url", "Expected an 'https' (or 'http' for development) URL.")

    if parsed.hostname is None or len(parsed.hostname) == 0:
        errmsg_lines = [
            "The supplied service URL does not include a hostname.",
            "    service_url: {}".format(service_url),
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise BadParameterError(
            errmsg, "service_url", "Expected a URL that includes a hostname.")

    host = parsed.hostname
    scheme = parsed.scheme
    port = parsed.port

    rtnval = (host, scheme, port)
    return rtnval


def derive_api_base_path(service_url: str) -> str:
    """
        Derives the REST API base path from the operator's service URL.
        A URL with no path means the API is at ``/api/v1``; a URL that
        carries a prefix (the service behind a proxy under ``/pychestrate``)
        keeps it; a URL that already ends in ``/api/v1`` is used as given.

        :param service_url: The ``--server-url`` the client enrolled with.

        :returns: The normalized base path, e.g. ``/api/v1`` or
                  ``/pychestrate/api/v1``.
    """
    path = urlparse(service_url).path.strip("/ ")
    if len(path) == 0:
        rtnval = DEFAULT_API_BASE_PATH
    elif ("/" + path).endswith(DEFAULT_API_BASE_PATH):
        rtnval = normalize_base_path(base_path=path)
    else:
        rtnval = normalize_base_path(base_path=path + DEFAULT_API_BASE_PATH)
    return rtnval


def describe_refusal(*, response: Any, redeem_url: str) -> str:
    """
        Turns the service's refusal of the redeem call into text an operator
        can act on. The service answers with a problem document whose detail
        lists what was wrong (and, for a wrong ``--name``, which name the
        token expects); that is printed line by line instead of as raw JSON.
        An answer that is not a problem document keeps the raw, cut-short body.

        :param response: The non-200 HTTP response.
        :param redeem_url: The URL that was called.

        :returns: The message, one line per line of detail.
    """
    try:
        document = response.json()
    except ValueError:
        document = None
    if isinstance(document, dict) and isinstance(document.get("detail"), str) and len(document["detail"].strip()) > 0:
        title = document.get("title")
        heading = "the service refused the enrollment (HTTP {}{})".format(
            response.status_code, ": {}".format(title) if isinstance(title, str) and len(title) > 0 else "")
        detail_lines = [line.strip() for line in document["detail"].splitlines() if len(line.strip()) > 0]
        shown = ["    {}".format(line) for line in detail_lines[:MAX_REFUSAL_DETAIL_LINES]]
        if len(detail_lines) > MAX_REFUSAL_DETAIL_LINES:
            shown.append("    ...")
        return os.linesep.join([heading] + shown)

    body = response.text
    if len(body) > MAX_ERROR_BODY_CHARACTERS:
        body = body[:MAX_ERROR_BODY_CHARACTERS] + "..."
    errmsg_lines = [
        "Enrollment redeem call failed.",
        "    url: {}".format(redeem_url),
        "    status: {}".format(response.status_code),
        "    body:",
        body,
    ]
    return os.linesep.join(errmsg_lines)


def describe_request_failure(*, failure: Exception, redeem_url: str, host: str,
                             port: Optional[int], timeout_seconds: float) -> str:
    """
        Turns an error from the HTTP library into one line an operator can
        act on (a refused connection used to print a page of traceback).

        :param failure: The exception ``requests`` raised.
        :param redeem_url: The URL that was called.
        :param host: The service host.
        :param port: The service port, or None for the scheme's default.
        :param timeout_seconds: The timeout that applied.

        :returns: The message, without a trailing period.
    """
    where = host if port is None else "{}:{}".format(host, port)
    if isinstance(failure, requests.exceptions.SSLError):
        return (
            "the TLS certificate of {} could not be verified ({}). For a development service "
            "with a self-signed certificate, add --insecure".format(where, _short_reason(failure=failure))
        )
    if isinstance(failure, requests.exceptions.Timeout):
        return "the service at {} did not answer within {:g} seconds".format(where, timeout_seconds)
    if isinstance(failure, requests.exceptions.ConnectionError):
        return "could not connect to {} ({}); check --server-url and that the service is running".format(
            where, _short_reason(failure=failure))
    return "the request to {} failed ({})".format(redeem_url, _short_reason(failure=failure))


def _short_reason(*, failure: Exception) -> str:
    """The part of a ``requests`` error an operator needs: the OS error when there is one, else a short text."""
    text = str(failure)
    found = re.search(r"\[Errno -?\d+\] ([^'\")]+)", text)
    if found is not None:
        return found.group(1).strip()
    found = re.search(r"\((?:certificate verify failed|hostname)[^)]*\)", text)
    if found is not None:
        return found.group(0).strip("()")
    first_line = text.splitlines()[0] if len(text) > 0 else type(failure).__name__
    return first_line[:160]


def perform_enrollment(service_url: str, bootstrap_token: str, client_name: str,
                       state: ClientState, attributes: Optional[Dict[str, Any]] = None,
                       verify_tls: bool = True,
                       session: Optional[requests.Session] = None,
                       timeout_seconds: float = DEFAULT_ENROLLMENT_TIMEOUT_SECONDS,
                       overwrite: bool = False) -> Dict[str, Any]:
    """
        Executes the full enrollment flow:

            1. Validate the service URL.
            2. Generate a keypair + CSR.
            3. POST the CSR + bootstrap secret to ``/enrollment-tokens/redeem``.
            4. Persist the assigned ``client_id``, the signed certificate, the
               service CA chain, the private key, and the bundled envelope
               signing-key set into the state directory.

        :param service_url: Full URL of the Pychestrate service
                            (e.g. ``https://pychestrate.example.com``).
        :param bootstrap_token: The one-time enrollment-token secret.
        :param client_name: Pod label (human-readable name).
        :param state: A :class:`ClientState` bound to the on-disk state dir.
        :param attributes: Optional attributes dictionary stored on the registry row.
        :param verify_tls: Whether to verify the service's TLS certificate during
                           enrollment. Set to ``False`` for self-signed dev
                           setups; defaults to ``True``.
        :param session: Optional pre-built :class:`requests.Session` to use for
                        the HTTP call. Injected for testability.
        :param timeout_seconds: How long the redeem call may take (default 30).
        :param overwrite: Replace the identity of a state directory that is
                          already enrolled. Without it an enrolled directory
                          is refused, since a new identity would silently
                          replace its client id and key.

        :returns: A dictionary describing the persisted enrollment result with
                  keys ``client_id``, ``client_cert_path``, ``client_key_path``,
                  ``service_ca_path``, and ``config_path``.

        :raises: BadParameterError, ConfigurationError, HTTPRequestError
    """
    if len(service_url) == 0:
        raise BadParameterError(
            "perform_enrollment requires a non-empty 'service_url'.",
            "service_url", "Pass the full URL of the Pychestrate service.")
    if len(bootstrap_token) == 0:
        raise BadParameterError(
            "perform_enrollment requires a non-empty 'bootstrap_token'.",
            "bootstrap_token", "Pass the one-time enrollment token secret.")
    if len(client_name) == 0:
        raise BadParameterError(
            "perform_enrollment requires a non-empty 'client_name'.",
            "client_name", "Pass the pod label the enrollment token was minted for.")

    host, scheme, port = _derive_host_and_scheme(service_url=service_url)

    if overwrite is False and state.is_enrolled():
        try:
            existing_id = state.read_client_id()
        except ConfigurationError:
            existing_id = "(unreadable)"
        raise ConfigurationError(
            "this state directory ({}) is already enrolled as client {}; enrolling again would replace "
            "its identity and key. Use --force to replace it, or pass another --state-dir".format(
                state.state_dir, existing_id))

    state.ensure_dir()

    private_key_pem, csr_pem_bytes = generate_keypair_and_csr(client_name=client_name)
    csr_pem_str = csr_pem_bytes.decode("ascii")

    payload = build_redeem_payload(
        bootstrap_secret=bootstrap_token,
        csr_pem=csr_pem_str,
        client_name=client_name,
        attributes=attributes,
    )

    base_path = derive_api_base_path(service_url=service_url)
    redeem_url = "{}://{}".format(scheme, host)
    if port is not None:
        redeem_url = "{}:{}".format(redeem_url, port)
    redeem_url = "{}{}{}".format(redeem_url, base_path, ENROLLMENT_REDEEM_LEAF)

    logger.info("pychestrate-client: redeeming enrollment token at %s", redeem_url)

    http = session
    if http is None:
        http = requests.Session()

    try:
        response = http.post(
            redeem_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            verify=verify_tls,
            timeout=timeout_seconds,
        )
    except requests.exceptions.RequestException as requesterr:
        raise HTTPRequestError(describe_request_failure(
            failure=requesterr, redeem_url=redeem_url, host=host, port=port,
            timeout_seconds=timeout_seconds)) from None

    if response.status_code != 200:
        raise HTTPRequestError(describe_refusal(response=response, redeem_url=redeem_url))

    try:
        raw_body = response.json()
    except ValueError:
        raise HTTPRequestError(
            "the service at {} answered with something that is not JSON (is --server-url the "
            "Pychestrate service?)".format(redeem_url)) from None

    client_id = raw_body.get("client_id")
    client_cert_pem = raw_body.get("client_certificate_pem")
    ca_chain_pem = raw_body.get("ca_chain_pem")

    if client_id is None or len(client_id) == 0:
        raise HTTPRequestError("Enrollment response is missing 'client_id'.")
    if client_cert_pem is None or len(client_cert_pem) == 0:
        raise HTTPRequestError("Enrollment response is missing 'client_certificate_pem'.")
    if ca_chain_pem is None or len(ca_chain_pem) == 0:
        raise HTTPRequestError("Enrollment response is missing 'ca_chain_pem'.")

    state.write_private_key(key_pem=private_key_pem)
    state.write_client_cert(cert_pem=client_cert_pem)
    state.write_service_ca(ca_pem=ca_chain_pem)
    state.write_client_id(client_id=client_id)

    signing_key_count = persist_signing_keys_from_enrollment(
        state=state, redeem_body=raw_body)

    config: Dict[str, Any] = {
        "service_url": service_url,
        "host": host,
        "scheme": scheme,
        "port": port,
        "base_path": base_path,
        "client_name": client_name,
    }
    if attributes is not None:
        config["attributes"] = attributes

    state.write_config(config=config)

    rtnval = {
        "client_id": client_id,
        "client_cert_path": state.client_cert_path,
        "client_key_path": state.client_key_path,
        "service_ca_path": state.service_ca_path,
        "config_path": state.config_path,
        "signing_key_count": signing_key_count,
    }
    return rtnval

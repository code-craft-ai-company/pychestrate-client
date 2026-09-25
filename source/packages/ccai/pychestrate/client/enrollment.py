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
import os

import requests

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509 import CertificateSigningRequestBuilder, Name, NameAttribute
from cryptography.x509.oid import NameOID

from ccai.errors.exceptions import BadParameterError, HTTPRequestError

from ccai.pychestrate.client.signing_keys import persist_signing_keys_from_enrollment
from ccai.pychestrate.client.state import ClientState


DEFAULT_RSA_KEY_SIZE = 2048

ENROLLMENT_REDEEM_PATH = "/api/v1/enrollment-tokens/redeem"

logger = logging.getLogger(__name__)


def generate_keypair_and_csr(client_name: str,
                             key_size: int = DEFAULT_RSA_KEY_SIZE) -> Tuple[bytes, bytes]:
    """
        Generates a fresh RSA private key and a PKCS#10 certificate signing
        request bound to the provided ``client_name`` as the X.509 common name.

        :param client_name: Human-readable client name used as the CSR's
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
        :param client_name: Human-readable client name to register.
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


def perform_enrollment(service_url: str, bootstrap_token: str, client_name: str,
                       state: ClientState, attributes: Optional[Dict[str, Any]] = None,
                       verify_tls: bool = True,
                       session: Optional[requests.Session] = None) -> Dict[str, Any]:
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
        :param client_name: Human-readable client name.
        :param state: A :class:`ClientState` bound to the on-disk state dir.
        :param attributes: Optional attributes dictionary stored on the registry row.
        :param verify_tls: Whether to verify the service's TLS certificate during
                           enrollment. Set to ``False`` for self-signed dev
                           setups; defaults to ``True``.
        :param session: Optional pre-built :class:`requests.Session` to use for
                        the HTTP call. Injected for testability.

        :returns: A dictionary describing the persisted enrollment result with
                  keys ``client_id``, ``client_cert_path``, ``client_key_path``,
                  ``service_ca_path``, and ``config_path``.

        :raises: BadParameterError, HTTPRequestError
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
            "client_name", "Pass a human-readable client name.")

    host, scheme, port = _derive_host_and_scheme(service_url=service_url)

    state.ensure_dir()

    private_key_pem, csr_pem_bytes = generate_keypair_and_csr(client_name=client_name)
    csr_pem_str = csr_pem_bytes.decode("ascii")

    payload = build_redeem_payload(
        bootstrap_secret=bootstrap_token,
        csr_pem=csr_pem_str,
        client_name=client_name,
        attributes=attributes,
    )

    redeem_url = "{}://{}".format(scheme, host)
    if port is not None:
        redeem_url = "{}:{}".format(redeem_url, port)
    redeem_url = "{}{}".format(redeem_url, ENROLLMENT_REDEEM_PATH)

    logger.info("pychestrate-client: redeeming enrollment token at %s", redeem_url)

    http = session
    if http is None:
        http = requests.Session()

    response = http.post(
        redeem_url,
        json=payload,
        headers={"Content-Type": "application/json"},
        verify=verify_tls,
    )

    if response.status_code != 200:
        errmsg_lines = [
            "Enrollment redeem call failed.",
            "    url: {}".format(redeem_url),
            "    status: {}".format(response.status_code),
            "    body:",
            response.text,
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise HTTPRequestError(errmsg)

    raw_body = response.json()

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

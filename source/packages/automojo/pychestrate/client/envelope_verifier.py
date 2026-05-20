"""
    .. module:: envelope_verifier
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Verifies the detached Ed25519 signature on a signed job
                   envelope delivered by ``GET /clients/{id}/dispatches:next``.

                   A pod client must NEVER execute a job envelope it has not
                   verified. The service signs every envelope with its
                   Ed25519 envelope signing key; this module re-derives the
                   canonical payload bytes from the delivered ``envelope``
                   object and checks the signature against the service's
                   signing public key persisted in the client state dir.

                   IMPORTANT — public-key delivery gap (M2):
                   The Pychestrate REST API (OpenAPI ``pychestrate.yaml``)
                   exposes no JWKS / signing-public-key endpoint, and the
                   enrollment-redeem response carries only the mTLS identity
                   (client cert, CA chain, client_id) — NOT the envelope
                   signing public key. Until the service is extended to
                   deliver that key, the operator must install it
                   out-of-band into the client state directory via
                   ``pychestrate-client install-signing-key``. The verifier
                   fails closed: no key installed -> verification cannot run
                   -> the dispatch consumer refuses to execute the job.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2025, Automation Mojo LLC"
__credits__ = []


from typing import Any, Dict, Optional

import json
import logging
import os

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from cryptography.hazmat.primitives.serialization import load_pem_public_key

from automojo.errors.exceptions import BadParameterError, ConfigurationError

from automojo.pychestrate.client.state import ClientState


logger = logging.getLogger(__name__)

#: Outcome reason codes mirrored from the service-side
#: ``jobs.envelope.signer.VerificationResult``.
REASON_OK = "ok"
REASON_SIGNATURE_MISMATCH = "signature_mismatch"
REASON_UNKNOWN_KEY = "unknown_key"
REASON_MALFORMED = "malformed"


class EnvelopeVerificationResult:
    """
        The :class:`EnvelopeVerificationResult` captures the outcome of a
        single envelope signature verification.

        The verifier never raises on a signature problem — every failure
        mode is encoded as a structured result so the dispatch consumer
        can log the rejection reason and ack the dispatch without
        wrapping the call in exception handling.
    """

    def __init__(self, *, valid: bool, reason: str,
                 envelope: Optional[Dict[str, Any]] = None,
                 detail: Optional[str] = None):
        """
            Initializes an :class:`EnvelopeVerificationResult`.

            :param valid: True only when the signature verified cleanly.
            :param reason: A short reason code (see ``REASON_*``).
            :param envelope: The parsed envelope dict when ``valid`` is
                             True; ``None`` otherwise.
            :param detail: Optional human-readable detail for logging.
        """
        self._valid = valid
        self._reason = reason
        self._envelope = envelope
        self._detail = detail
        return

    @property
    def valid(self) -> bool:
        """
            Gets whether the envelope signature verified.

            :returns: True on a verified signature.
        """
        return self._valid

    @property
    def reason(self) -> str:
        """
            Gets the short reason code describing the outcome.

            :returns: The reason code string.
        """
        return self._reason

    @property
    def envelope(self) -> Optional[Dict[str, Any]]:
        """
            Gets the parsed, verified envelope dict.

            :returns: The envelope dict, or ``None`` when not verified.
        """
        return self._envelope

    @property
    def detail(self) -> Optional[str]:
        """
            Gets the optional human-readable detail string.

            :returns: The detail string, or ``None``.
        """
        return self._detail


def load_signing_public_key(state: ClientState) -> Ed25519PublicKey:
    """
        Loads the service's Ed25519 job-envelope signing public key from
        the client state directory.

        :param state: The :class:`ClientState` bound to the on-disk state.

        :returns: The loaded :class:`Ed25519PublicKey`.
        :raises: ConfigurationError when the key is missing or not a
                 valid Ed25519 PEM public key.
    """
    key_pem = state.read_envelope_signing_key()

    try:
        loaded = load_pem_public_key(key_pem.encode("ascii"))
    except Exception as loaderr:
        errmsg_lines = [
            "The installed envelope signing public key is not a valid PEM "
            "public key.",
            "    path: {}".format(state.envelope_signing_key_path),
            "    reason: {}".format(loaderr),
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise ConfigurationError(errmsg) from loaderr

    if not isinstance(loaded, Ed25519PublicKey):
        errmsg_lines = [
            "The installed envelope signing public key is not an Ed25519 key.",
            "    path: {}".format(state.envelope_signing_key_path),
            "    type: {}".format(type(loaded).__name__),
            "Pychestrate signs job envelopes with Ed25519.",
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise ConfigurationError(errmsg)

    rtnval = loaded
    return rtnval


def serialize_envelope_canonical(envelope: Dict[str, Any]) -> bytes:
    """
        Re-derives the canonical JSON payload bytes for an envelope dict,
        matching the service-side
        ``jobs.envelope.canonical.serialize_canonical`` exactly:
        UTF-16-ordered keys, no inter-token whitespace, non-ASCII left
        unescaped, encoded as UTF-8.

        The signature was computed by the service over these exact bytes,
        so the client MUST reproduce them byte-for-byte to verify.

        :param envelope: The envelope object as delivered inside the
                         ``PendingDispatch`` body.

        :returns: The canonical UTF-8 JSON bytes.
        :raises: BadParameterError when the envelope is not serializable.
    """
    if envelope is None:
        raise BadParameterError(
            "serialize_envelope_canonical requires a non-None 'envelope'.",
            "envelope", "Pass the envelope dict from the PendingDispatch body.")

    try:
        text = json.dumps(
            envelope,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as serr:
        errmsg_lines = [
            "The dispatched envelope could not be canonicalized for "
            "signature verification.",
            "    reason: {}".format(serr),
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise BadParameterError(
            errmsg, "envelope", "The envelope object is not JSON-serializable.")

    rtnval = text.encode("utf-8")
    return rtnval


def verify_signed_envelope(*, envelope: Dict[str, Any], signature_hex: str,
                           signature_key_id: str,
                           signing_public_key: Ed25519PublicKey,
                           signing_key_id: str) -> EnvelopeVerificationResult:
    """
        Verifies the detached Ed25519 signature on a delivered job
        envelope. Never raises on a signature problem — every failure is
        returned as an :class:`EnvelopeVerificationResult`.

        :param envelope: The envelope object from the ``PendingDispatch``.
        :param signature_hex: The hex-encoded detached signature.
        :param signature_key_id: The ``signature_key_id`` claimed by the
                                 dispatch (the JWKS key id of the signer).
        :param signing_public_key: The locally-trusted Ed25519 public key.
        :param signing_key_id: The key id the locally-trusted key was
                               installed under.

        :returns: An :class:`EnvelopeVerificationResult`.
    """
    if signature_key_id != signing_key_id:
        detail = (
            "Dispatch signature_key_id {!r} does not match the installed "
            "signing key id {!r}.".format(signature_key_id, signing_key_id)
        )
        logger.error("pychestrate-client: %s", detail)
        rtnval = EnvelopeVerificationResult(
            valid=False, reason=REASON_UNKNOWN_KEY, detail=detail)
        return rtnval

    try:
        signature_bytes = bytes.fromhex(signature_hex)
    except (ValueError, TypeError) as hexerr:
        detail = "Dispatch signature is not valid hex: {}".format(hexerr)
        logger.error("pychestrate-client: %s", detail)
        rtnval = EnvelopeVerificationResult(
            valid=False, reason=REASON_MALFORMED, detail=detail)
        return rtnval

    try:
        payload_bytes = serialize_envelope_canonical(envelope=envelope)
    except BadParameterError as canonerr:
        detail = "Dispatch envelope could not be canonicalized: {}".format(canonerr)
        logger.error("pychestrate-client: %s", detail)
        rtnval = EnvelopeVerificationResult(
            valid=False, reason=REASON_MALFORMED, detail=detail)
        return rtnval

    try:
        signing_public_key.verify(signature_bytes, payload_bytes)
    except InvalidSignature:
        detail = (
            "Ed25519 signature verification FAILED for envelope_id={!r}; "
            "the dispatch will NOT be executed.".format(
                envelope.get("envelope_id"))
        )
        logger.error("pychestrate-client: %s", detail)
        rtnval = EnvelopeVerificationResult(
            valid=False, reason=REASON_SIGNATURE_MISMATCH, detail=detail)
        return rtnval

    logger.info(
        "pychestrate-client: envelope signature verified (envelope_id=%s).",
        envelope.get("envelope_id"),
    )
    rtnval = EnvelopeVerificationResult(
        valid=True, reason=REASON_OK, envelope=envelope)
    return rtnval

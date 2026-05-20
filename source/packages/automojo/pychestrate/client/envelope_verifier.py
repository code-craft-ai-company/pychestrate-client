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

                   M4 — automatic signing-key delivery:
                   The Pychestrate service now delivers its JWKS-style
                   Ed25519 envelope signing public-key set both inside the
                   enrollment-redeem response and through
                   ``GET /api/v1/envelope-signing-keys``. The verifier loads
                   the key it needs from the persisted
                   ``envelope-signing-keys.json`` set, indexed by the
                   ``signature_key_id`` each dispatched envelope carries.
                   When an envelope is signed by a ``key_id`` the persisted
                   set does not carry, the verifier triggers a single
                   refresh from the service before failing closed. The
                   manual ``install-signing-key`` command remains only for
                   air-gapped / fallback installs -- it is no longer part of
                   the happy path. The verifier still fails closed: a
                   ``key_id`` that cannot be resolved even after a refresh
                   -> verification cannot run -> the dispatch consumer
                   refuses to execute the job.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2025, Automation Mojo LLC"
__credits__ = []


from typing import Any, Callable, Dict, List, Optional

import json
import logging
import os

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from cryptography.hazmat.primitives.serialization import load_pem_public_key

from automojo.errors.exceptions import BadParameterError, ConfigurationError

from automojo.pychestrate.client.signing_keys import find_signing_key
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


def parse_ed25519_public_key(key_pem: str, source_label: str) -> Ed25519PublicKey:
    """
        Parses a PEM-encoded public key string into an
        :class:`Ed25519PublicKey`.

        :param key_pem: The PEM-encoded public key text.
        :param source_label: A short label naming where the key came from,
                              used in error messages for debuggability.

        :returns: The loaded :class:`Ed25519PublicKey`.
        :raises: ConfigurationError when the key is not a valid Ed25519
                 PEM public key.
    """
    try:
        loaded = load_pem_public_key(key_pem.encode("ascii"))
    except Exception as loaderr:
        errmsg_lines = [
            "An envelope signing public key is not a valid PEM public key.",
            "    source: {}".format(source_label),
            "    reason: {}".format(loaderr),
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise ConfigurationError(errmsg) from loaderr

    if not isinstance(loaded, Ed25519PublicKey):
        errmsg_lines = [
            "An envelope signing public key is not an Ed25519 key.",
            "    source: {}".format(source_label),
            "    type: {}".format(type(loaded).__name__),
            "Pychestrate signs job envelopes with Ed25519.",
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise ConfigurationError(errmsg)

    rtnval = loaded
    return rtnval


def load_signing_public_key(state: ClientState) -> Ed25519PublicKey:
    """
        Loads the legacy single-file envelope signing public key from the
        client state directory (the file the manual ``install-signing-key``
        command writes).

        Retained for the air-gapped / fallback install path. The normal
        happy path resolves keys from the JWKS-style key set via
        :func:`resolve_signing_public_key`.

        :param state: The :class:`ClientState` bound to the on-disk state.

        :returns: The loaded :class:`Ed25519PublicKey`.
        :raises: ConfigurationError when the key is missing or not a valid
                 Ed25519 PEM public key.
    """
    key_pem = state.read_envelope_signing_key()
    rtnval = parse_ed25519_public_key(
        key_pem=key_pem, source_label=state.envelope_signing_key_path)
    return rtnval


def resolve_signing_public_key(state: ClientState, key_id: str,
                               refresh_func: Optional[Callable[[], List[Dict[str, Any]]]]
                               = None) -> Ed25519PublicKey:
    """
        Resolves the Ed25519 envelope signing public key for a given
        ``key_id`` from the persisted JWKS-style signing-key set.

        When the persisted set does not carry the requested ``key_id`` and
        a ``refresh_func`` is supplied, the set is refreshed from the
        service exactly once and the lookup is retried. The verifier still
        fails closed: a ``key_id`` unresolved even after a refresh raises
        :class:`ConfigurationError`.

        :param state: The :class:`ClientState` bound to the on-disk state.
        :param key_id: The JWKS-style key id the dispatch envelope claims.
        :param refresh_func: Optional callable that refreshes the persisted
                             signing-key set from the service and returns
                             the refreshed list. ``None`` disables the lazy
                             refresh (air-gapped operation).

        :returns: The resolved :class:`Ed25519PublicKey`.
        :raises: ConfigurationError when no signing key matches ``key_id``.
    """
    if key_id is None or len(key_id) == 0:
        raise ConfigurationError(
            "resolve_signing_public_key requires a non-empty 'key_id'.")

    keys: List[Dict[str, Any]] = []
    if state.has_envelope_signing_keys():
        keys = state.read_envelope_signing_keys()

    entry = find_signing_key(keys=keys, key_id=key_id)

    if entry is None and refresh_func is not None:
        logger.info(
            "pychestrate-client: envelope signing key_id %r not in the "
            "persisted set; refreshing from the service.", key_id,
        )
        keys = refresh_func()
        entry = find_signing_key(keys=keys, key_id=key_id)

    if entry is None:
        errmsg_lines = [
            "No envelope signing public key matches the dispatched "
            "envelope's signature key id.",
            "    key_id: {}".format(key_id),
            "    persisted set: {}".format(state.envelope_signing_keys_path),
            "    known key ids: {}".format(
                [k.get("key_id") for k in keys]),
            "The dispatch consumer refuses to execute envelopes it cannot "
            "verify.",
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise ConfigurationError(errmsg)

    public_key_pem = entry.get("public_key_pem", None)
    if public_key_pem is None or len(public_key_pem) == 0:
        errmsg_lines = [
            "The persisted envelope signing key carries no public key PEM.",
            "    key_id: {}".format(key_id),
            "    persisted set: {}".format(state.envelope_signing_keys_path),
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise ConfigurationError(errmsg)

    rtnval = parse_ed25519_public_key(
        key_pem=public_key_pem,
        source_label="signing key {!r} from {}".format(
            key_id, state.envelope_signing_keys_path))
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


def verify_dispatch_envelope(*, state: ClientState, envelope: Dict[str, Any],
                             signature_hex: str, signature_key_id: str,
                             refresh_func: Optional[Callable[[], List[Dict[str, Any]]]]
                             = None) -> EnvelopeVerificationResult:
    """
        Verifies a dispatched job envelope end-to-end: resolves the
        Ed25519 signing public key for the envelope's claimed
        ``signature_key_id`` from the persisted JWKS-style signing-key set
        (refreshing once from the service on an unknown ``key_id`` when a
        ``refresh_func`` is supplied), then checks the detached signature.

        This is the entry point the dispatch consumer uses; it never
        raises on a signature problem -- a key that cannot be resolved is
        returned as an ``unknown_key`` :class:`EnvelopeVerificationResult`.

        :param state: The :class:`ClientState` bound to the on-disk state.
        :param envelope: The envelope object from the ``PendingDispatch``.
        :param signature_hex: The hex-encoded detached signature.
        :param signature_key_id: The ``signature_key_id`` claimed by the
                                 dispatch.
        :param refresh_func: Optional callable that refreshes the persisted
                             signing-key set from the service. ``None``
                             disables the lazy refresh.

        :returns: An :class:`EnvelopeVerificationResult`.
    """
    try:
        signing_public_key = resolve_signing_public_key(
            state=state, key_id=signature_key_id, refresh_func=refresh_func)
    except ConfigurationError as resolveerr:
        detail = (
            "Could not resolve the envelope signing key for key_id {!r}: "
            "{}".format(signature_key_id, resolveerr)
        )
        logger.error("pychestrate-client: %s", detail)
        rtnval = EnvelopeVerificationResult(
            valid=False, reason=REASON_UNKNOWN_KEY, detail=detail)
        return rtnval

    rtnval = verify_signed_envelope(
        envelope=envelope,
        signature_hex=signature_hex,
        signature_key_id=signature_key_id,
        signing_public_key=signing_public_key,
        signing_key_id=signature_key_id,
    )
    return rtnval

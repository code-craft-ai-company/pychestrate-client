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

                   The service publishes its Ed25519 envelope signing public
                   keys as an RFC 7517 JWK Set (JWKS). Retrieving that JWK
                   Set is the first-class way the client obtains a
                   verification key; an operator-installed key is an
                   explicit override. The full, ordered resolution is
                   documented on :func:`resolve_envelope_signing_key`.

                   Whatever the source, the verifier fails closed: a
                   ``signature_key_id`` that no source can resolve means
                   verification cannot run, and the dispatch consumer
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

from automojo.pychestrate.client.jwks import (
    DEFAULT_JWKS_CACHE_TTL_SECONDS,
    is_cache_fresh,
    load_cached_jwks,
)
from automojo.pychestrate.client.signing_keys import find_signing_key
from automojo.pychestrate.client.state import ClientState


logger = logging.getLogger(__name__)

#: Outcome reason codes mirrored from the service-side
#: ``jobs.envelope.signer.VerificationResult``.
REASON_OK = "ok"
REASON_SIGNATURE_MISMATCH = "signature_mismatch"
REASON_UNKNOWN_KEY = "unknown_key"
REASON_MALFORMED = "malformed"

#: Key-source markers recorded on a :class:`ResolvedSigningKey`, naming
#: which step of the resolution order supplied the verification key. These
#: are logged on every verification so an operator can tell at a glance
#: whether a pod is running on an override, on the fetched JWK Set, or on
#: the enrollment-bundled fallback.
KEY_SOURCE_INSTALLED_OVERRIDE = "installed-override"
KEY_SOURCE_JWKS_CACHE = "jwks-cache"
KEY_SOURCE_JWKS_CACHE_STALE = "jwks-cache-stale"
KEY_SOURCE_JWKS_FETCH = "jwks-fetch"
KEY_SOURCE_ENROLLMENT_SET = "enrollment-set"
KEY_SOURCE_ENROLLMENT_REFRESH = "enrollment-refresh"


class ResolvedSigningKey:
    """
        The :class:`ResolvedSigningKey` carries the outcome of envelope
        signing-key resolution: the Ed25519 public key, the key id it was
        resolved for, and which source in the documented resolution order
        supplied it.

        The source is retained purely for debuggability -- a pod that
        rejects envelopes is far easier to diagnose when the log says which
        key source answered.
    """

    def __init__(self, *, public_key: Ed25519PublicKey, key_id: str,
                 source: str):
        """
            Initializes a :class:`ResolvedSigningKey`.

            :param public_key: The resolved Ed25519 public key.
            :param key_id: The key id the key was resolved for.
            :param source: One of the ``KEY_SOURCE_*`` markers.
        """
        self._public_key = public_key
        self._key_id = key_id
        self._source = source
        return

    @property
    def public_key(self) -> Ed25519PublicKey:
        """
            Gets the resolved Ed25519 public key.

            :returns: The :class:`Ed25519PublicKey`.
        """
        return self._public_key

    @property
    def key_id(self) -> str:
        """
            Gets the key id the key was resolved for.

            :returns: The key id string.
        """
        return self._key_id

    @property
    def source(self) -> str:
        """
            Gets the marker naming which resolution step supplied the key.

            :returns: One of the ``KEY_SOURCE_*`` markers.
        """
        return self._source


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
        Loads the single-file envelope signing public key an operator
        installed with ``pychestrate-client install-signing-key``.

        The presence of that file is an explicit operator override: it
        pins envelope verification to that one key and takes precedence
        over every key source fetched from the service. See
        :func:`resolve_envelope_signing_key` for the full order.

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
        ``key_id`` from the enrollment-bundled signing-key set alone.

        This is a single-source lookup against
        ``envelope-signing-keys.json``. It does not consult the operator
        override or the JWK Set fetched from the service -- use
        :func:`resolve_envelope_signing_key` for the full, documented
        resolution order the dispatch consumer runs.

        When the persisted set does not carry the requested ``key_id`` and
        a ``refresh_func`` is supplied, the set is refreshed from the
        service exactly once and the lookup is retried. The lookup still
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


def signing_key_entry_to_public_key(entry: Dict[str, Any], key_id: str,
                                    source_label: str) -> Ed25519PublicKey:
    """
        Converts a persisted signing-key entry into an
        :class:`Ed25519PublicKey`.

        :param entry: The signing-key dictionary carrying
                      ``public_key_pem``.
        :param key_id: The key id the entry was looked up under, used in
                       error messages.
        :param source_label: A short label naming which source supplied the
                             entry, used in error messages.

        :returns: The loaded :class:`Ed25519PublicKey`.
        :raises: ConfigurationError when the entry carries no usable PEM.
    """
    public_key_pem = entry.get("public_key_pem", None)
    if public_key_pem is None or len(public_key_pem) == 0:
        errmsg_lines = [
            "An envelope signing key carries no public key PEM.",
            "    key_id: {}".format(key_id),
            "    source: {}".format(source_label),
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise ConfigurationError(errmsg)

    rtnval = parse_ed25519_public_key(
        key_pem=public_key_pem,
        source_label="signing key {!r} from {}".format(key_id, source_label))
    return rtnval


def resolve_installed_override(state: ClientState,
                               key_id: str) -> Optional[ResolvedSigningKey]:
    """
        Resolves the operator-installed envelope signing key override.

        Installing a key with ``pychestrate-client install-signing-key`` is
        an explicit operator decision to pin envelope verification to one
        key. While that key file is present it is the ONLY trust anchor --
        the service JWK Set is not consulted, and an envelope signed by any
        other key id is refused. A pin that silently deferred to a
        service-fetched key would not be an override at all.

        :param state: The :class:`ClientState` bound to the on-disk state.
        :param key_id: The ``signature_key_id`` the dispatched envelope
                       claims.

        :returns: The :class:`ResolvedSigningKey` when an override is
                  installed and covers ``key_id``; ``None`` when no
                  override is installed at all.
        :raises: ConfigurationError when an override is installed but does
                 not cover ``key_id``, or was installed without a key id.
    """
    if not state.has_envelope_signing_key():
        rtnval = None
        return rtnval

    installed_key_id = state.read_installed_signing_key_id()

    if installed_key_id is None:
        errmsg_lines = [
            "An envelope signing public key is installed as an operator "
            "override but no key id was recorded for it.",
            "    key file: {}".format(state.envelope_signing_key_path),
            "    config: {}".format(state.config_path),
            "The client cannot tell which envelopes the pinned key is meant "
            "to verify, so it refuses to execute any of them.",
            "Re-install the key with 'pychestrate-client install-signing-key "
            "--key-file <pem> --key-id <id>', or remove the key file to "
            "resolve keys from the service JWK Set instead.",
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise ConfigurationError(errmsg)

    if installed_key_id != key_id:
        errmsg_lines = [
            "The dispatched envelope is signed by a key id the operator's "
            "pinned envelope signing key does not cover.",
            "    envelope signature_key_id: {}".format(key_id),
            "    installed (pinned) key_id: {}".format(installed_key_id),
            "    key file: {}".format(state.envelope_signing_key_path),
            "An explicitly installed signing key is an operator override: "
            "while it is present it is the only trust anchor and the "
            "service JWK Set is NOT consulted.",
            "Install the service's current signing key, or remove {} to "
            "resolve keys from the service JWK Set instead.".format(
                state.envelope_signing_key_path),
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise ConfigurationError(errmsg)

    public_key = load_signing_public_key(state=state)

    logger.info(
        "pychestrate-client: verifying envelope key_id=%r against the "
        "operator-installed signing key override at %s.",
        key_id, state.envelope_signing_key_path,
    )

    rtnval = ResolvedSigningKey(
        public_key=public_key, key_id=key_id,
        source=KEY_SOURCE_INSTALLED_OVERRIDE)
    return rtnval


def _attempt_refresh(refresh_func: Callable[[], List[Dict[str, Any]]],
                     source_label: str) -> Optional[List[Dict[str, Any]]]:
    """
        Runs a key-set refresh callable, treating an unusable key source as
        a reason to continue down the resolution order rather than as a
        fatal error.

        A :class:`ConfigurationError` here means the source itself is
        unusable -- the codegen'd REST client exposes no JWKS operation, or
        the service answered with a malformed document. That is recoverable:
        the next source in the order may still resolve the key. Transport
        failures are NOT caught; a transient outage must surface so the
        dispatch-consumer loop backs off and retries instead of permanently
        rejecting a legitimate job.

        :param refresh_func: The callable that refreshes and returns a key
                             set.
        :param source_label: A short label naming the source, used in log
                             messages.

        :returns: The refreshed key list, or ``None`` when the source is
                  unusable.
    """
    try:
        refreshed = refresh_func()
    except ConfigurationError as refresherr:
        logger.warning(
            "pychestrate-client: the %s key source is unusable and will be "
            "skipped: %s", source_label, refresherr,
        )
        rtnval = None
        return rtnval

    rtnval = refreshed
    return rtnval


def resolve_envelope_signing_key(*, state: ClientState, key_id: str,
                                 jwks_refresh_func:
                                 Optional[Callable[[], List[Dict[str, Any]]]] = None,
                                 enrollment_refresh_func:
                                 Optional[Callable[[], List[Dict[str, Any]]]] = None,
                                 cache_ttl_seconds: float =
                                 DEFAULT_JWKS_CACHE_TTL_SECONDS
                                 ) -> ResolvedSigningKey:
    """
        Resolves the Ed25519 envelope signing public key for a dispatched
        envelope's ``signature_key_id``, in this documented order:

            1. **Operator-installed key** -- the key an operator installed
               with ``pychestrate-client install-signing-key``. Its presence
               is an explicit override: it pins verification to that one key
               and no service-fetched source is consulted. An envelope
               signed by a different key id is refused.
            2. **Fresh cached JWK Set** -- the RFC 7517 JWK Set fetched from
               the service's JWKS endpoint and cached in
               ``envelope-signing-jwks.json``, while the cache is within its
               time-to-live.
            3. **Enrollment-bundled signing-key set** -- the set the service
               delivered in the enrollment-redeem response
               (``envelope-signing-keys.json``). Checked before any network
               call because it is local and already service-delivered.
            4. **Live JWK Set fetch** -- one fetch from the service's JWKS
               endpoint, persisted into the cache. This is what picks up a
               signing-key rotation.
            5. **Stale cached JWK Set** -- a cached key whose id matches but
               whose cache aged out, served only because the live fetch did
               not resolve the id. A pod should not stop running jobs
               because the JWKS endpoint blipped, and an Ed25519 public key
               does not expire.
            6. **Enrollment key-set endpoint refresh** -- the pre-JWKS
               ``GET /api/v1/envelope-signing-keys`` refresh, retained so
               clients pointed at a service without a JWKS endpoint keep
               working.

        When no source resolves the key id this raises, and the caller
        refuses to execute the envelope. The client never executes a job it
        cannot verify.

        :param state: The :class:`ClientState` bound to the on-disk state.
        :param key_id: The ``signature_key_id`` the dispatched envelope
                       claims.
        :param jwks_refresh_func: Optional callable that fetches the
                                  service's JWK Set, persists it into the
                                  cache, and returns the normalized key
                                  list. ``None`` disables step 4
                                  (air-gapped operation).
        :param enrollment_refresh_func: Optional callable that refreshes the
                                        enrollment-style signing-key set
                                        from the service. ``None`` disables
                                        step 6.
        :param cache_ttl_seconds: How long the cached JWK Set is served
                                  before a re-fetch is attempted.

        :returns: The :class:`ResolvedSigningKey`.
        :raises: ConfigurationError when no source resolves ``key_id``.
    """
    if key_id is None or len(key_id) == 0:
        raise ConfigurationError(
            "resolve_envelope_signing_key requires a non-empty 'key_id'.")

    override = resolve_installed_override(state=state, key_id=key_id)
    if override is not None:
        rtnval = override
        return rtnval

    cached_keys, fetched_at = load_cached_jwks(state=state)
    cached_entry = find_signing_key(keys=cached_keys, key_id=key_id)
    cache_fresh = is_cache_fresh(
        fetched_at=fetched_at, ttl_seconds=cache_ttl_seconds)

    if cached_entry is not None and cache_fresh:
        public_key = signing_key_entry_to_public_key(
            entry=cached_entry, key_id=key_id,
            source_label=state.envelope_signing_jwks_path)
        rtnval = ResolvedSigningKey(
            public_key=public_key, key_id=key_id,
            source=KEY_SOURCE_JWKS_CACHE)
        return rtnval

    enrollment_keys: List[Dict[str, Any]] = []
    if state.has_envelope_signing_keys():
        enrollment_keys = state.read_envelope_signing_keys()

    enrollment_entry = find_signing_key(keys=enrollment_keys, key_id=key_id)
    if enrollment_entry is not None:
        public_key = signing_key_entry_to_public_key(
            entry=enrollment_entry, key_id=key_id,
            source_label=state.envelope_signing_keys_path)
        rtnval = ResolvedSigningKey(
            public_key=public_key, key_id=key_id,
            source=KEY_SOURCE_ENROLLMENT_SET)
        return rtnval

    if jwks_refresh_func is not None:
        logger.info(
            "pychestrate-client: envelope signing key_id %r is not in a "
            "current local source; fetching the service JWK Set.", key_id,
        )
        refreshed = _attempt_refresh(
            refresh_func=jwks_refresh_func, source_label="service JWK Set")

        if refreshed is not None:
            refreshed_entry = find_signing_key(keys=refreshed, key_id=key_id)
            if refreshed_entry is not None:
                public_key = signing_key_entry_to_public_key(
                    entry=refreshed_entry, key_id=key_id,
                    source_label="the service JWK Set endpoint")
                rtnval = ResolvedSigningKey(
                    public_key=public_key, key_id=key_id,
                    source=KEY_SOURCE_JWKS_FETCH)
                return rtnval

    if cached_entry is not None:
        logger.warning(
            "pychestrate-client: serving envelope signing key_id %r from the "
            "STALE JWK Set cache at %s; the service JWK Set could not be "
            "re-fetched.", key_id, state.envelope_signing_jwks_path,
        )
        public_key = signing_key_entry_to_public_key(
            entry=cached_entry, key_id=key_id,
            source_label=state.envelope_signing_jwks_path)
        rtnval = ResolvedSigningKey(
            public_key=public_key, key_id=key_id,
            source=KEY_SOURCE_JWKS_CACHE_STALE)
        return rtnval

    if enrollment_refresh_func is not None:
        logger.info(
            "pychestrate-client: falling back to the envelope-signing-keys "
            "endpoint for key_id %r.", key_id,
        )
        refreshed = _attempt_refresh(
            refresh_func=enrollment_refresh_func,
            source_label="envelope-signing-keys endpoint")

        if refreshed is not None:
            refreshed_entry = find_signing_key(keys=refreshed, key_id=key_id)
            if refreshed_entry is not None:
                public_key = signing_key_entry_to_public_key(
                    entry=refreshed_entry, key_id=key_id,
                    source_label="the envelope-signing-keys endpoint")
                rtnval = ResolvedSigningKey(
                    public_key=public_key, key_id=key_id,
                    source=KEY_SOURCE_ENROLLMENT_REFRESH)
                return rtnval

    cached_key_ids = [k.get("key_id") for k in cached_keys]
    enrollment_key_ids = [k.get("key_id") for k in enrollment_keys]

    errmsg_lines = [
        "No envelope signing public key matches the dispatched envelope's "
        "signature key id.",
        "    key_id: {}".format(key_id),
        "    JWK Set cache: {}".format(state.envelope_signing_jwks_path),
        "        cached key ids: {}".format(cached_key_ids),
        "        cache fresh: {}".format(cache_fresh),
        "    enrollment-bundled set: {}".format(
            state.envelope_signing_keys_path),
        "        known key ids: {}".format(enrollment_key_ids),
        "    JWK Set fetch attempted: {}".format(jwks_refresh_func is not None),
        "    envelope-signing-keys refresh attempted: {}".format(
            enrollment_refresh_func is not None),
        "The dispatch consumer refuses to execute envelopes it cannot "
        "verify.",
    ]
    errmsg = os.linesep.join(errmsg_lines)
    raise ConfigurationError(errmsg)


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
                             = None,
                             jwks_refresh_func:
                             Optional[Callable[[], List[Dict[str, Any]]]] = None,
                             cache_ttl_seconds: float =
                             DEFAULT_JWKS_CACHE_TTL_SECONDS
                             ) -> EnvelopeVerificationResult:
    """
        Verifies a dispatched job envelope end-to-end: resolves the
        Ed25519 signing public key for the envelope's claimed
        ``signature_key_id`` through the documented resolution order (see
        :func:`resolve_envelope_signing_key`), then checks the detached
        signature.

        This is the entry point the dispatch consumer uses; it never
        raises on a signature problem -- a key that cannot be resolved is
        returned as an ``unknown_key`` :class:`EnvelopeVerificationResult`.
        Transport failures reaching the service DO propagate, so a
        transient outage backs the consumer loop off and retries rather
        than permanently rejecting a legitimate job.

        :param state: The :class:`ClientState` bound to the on-disk state.
        :param envelope: The envelope object from the ``PendingDispatch``.
        :param signature_hex: The hex-encoded detached signature.
        :param signature_key_id: The ``signature_key_id`` claimed by the
                                 dispatch.
        :param refresh_func: Optional callable that refreshes the
                             enrollment-style signing-key set from
                             ``GET /api/v1/envelope-signing-keys``. ``None``
                             disables that fallback.
        :param jwks_refresh_func: Optional callable that fetches and caches
                                  the service's RFC 7517 JWK Set. ``None``
                                  disables the JWKS fetch (air-gapped
                                  operation).
        :param cache_ttl_seconds: How long the cached JWK Set is served
                                  before a re-fetch is attempted.

        :returns: An :class:`EnvelopeVerificationResult`.
    """
    try:
        resolved = resolve_envelope_signing_key(
            state=state,
            key_id=signature_key_id,
            jwks_refresh_func=jwks_refresh_func,
            enrollment_refresh_func=refresh_func,
            cache_ttl_seconds=cache_ttl_seconds,
        )
    except ConfigurationError as resolveerr:
        detail = (
            "Could not resolve the envelope signing key for key_id {!r}: "
            "{}".format(signature_key_id, resolveerr)
        )
        logger.error("pychestrate-client: %s", detail)
        rtnval = EnvelopeVerificationResult(
            valid=False, reason=REASON_UNKNOWN_KEY, detail=detail)
        return rtnval

    logger.debug(
        "pychestrate-client: resolved envelope signing key_id=%r from "
        "source=%s.", resolved.key_id, resolved.source,
    )

    rtnval = verify_signed_envelope(
        envelope=envelope,
        signature_hex=signature_hex,
        signature_key_id=signature_key_id,
        signing_public_key=resolved.public_key,
        signing_key_id=resolved.key_id,
    )
    return rtnval

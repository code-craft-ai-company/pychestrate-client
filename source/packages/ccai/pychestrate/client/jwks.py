"""
    .. module:: jwks
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Fetches and caches the Pychestrate service's RFC 7517 JWK
                   Set (JWKS) of Ed25519 envelope signing public keys, so the
                   dispatch consumer can verify signed job envelopes against
                   keys retrieved from the service rather than keys seeded
                   out-of-band by an operator.

                   The service publishes its envelope signing public keys as
                   an RFC 7517 JWK Set of OKP / Ed25519 keys (per the layer-5
                   ``5-job-envelope`` / ``5-job-dispatch`` dispatch specs).
                   Each JWK carries ``kty=OKP``, ``crv=Ed25519``,
                   ``alg=EdDSA``, ``use=sig``, a ``kid`` matching the
                   ``signature_key_id`` stamped on every dispatched envelope,
                   and the raw 32-byte public key base64url-encoded in ``x``.

                   This module owns three things:

                       1. Decoding an RFC 7517 OKP / Ed25519 JWK into an
                          :class:`Ed25519PublicKey`.
                       2. Fetching the JWK Set from the service THROUGH the
                          codegen'd ``codecraftai-restclient-pychestrate``
                          package -- this module never hand-rolls an HTTP
                          call against the service.
                       3. Persisting the fetched set into the client state
                          directory as a TTL-stamped cache
                          (``envelope-signing-jwks.json``).

                   JWKs are normalized on ingest into the same
                   ``key_id`` / ``algorithm`` / ``public_key_pem`` / ``status``
                   entry shape the enrollment-bundled signing-key set uses.
                   Storing the PEM rather than the raw base64url ``x`` value
                   keeps the on-disk cache readable when an operator has to
                   diagnose a verification refusal, and lets the verifier
                   resolve every key source through one code path.

                   The verifier's key-resolution order is documented on
                   :func:`ccai.pychestrate.client.envelope_verifier.resolve_envelope_signing_key`.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2025, Automation Mojo LLC"
__credits__ = []


from typing import Any, Callable, Dict, List, Optional, Tuple

import base64
import binascii
import logging
import os

from datetime import datetime, timezone

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from ccai.errors.exceptions import BadParameterError, ConfigurationError

from ccai.pychestrate.client.pychestratemtlsclient import PychestrateMtlsClient
from ccai.pychestrate.client.state import ClientState


logger = logging.getLogger(__name__)


#: RFC 8037 key type for Edwards-curve keys published in a JWK Set.
JWK_KEY_TYPE_OKP = "OKP"

#: RFC 8037 curve name for the Ed25519 signature scheme.
JWK_CURVE_ED25519 = "Ed25519"

#: RFC 8037 ``alg`` value for Ed25519 signatures.
JWK_ALGORITHM_EDDSA = "EdDSA"

#: RFC 7517 ``use`` value marking a key usable for signature verification.
JWK_USE_SIGNATURE = "sig"

#: RFC 7517 ``key_ops`` value marking a key usable for verification.
JWK_KEY_OP_VERIFY = "verify"

#: Raw Ed25519 public keys are exactly 32 bytes.
ED25519_PUBLIC_KEY_LENGTH = 32

#: ``algorithm`` value stamped on a normalized signing-key entry. This
#: matches the value the service's ``SigningKey`` schema carries so the
#: JWKS cache and the enrollment-bundled set share one entry shape.
SIGNING_KEY_ALGORITHM_ED25519 = "ed25519"

#: ``status`` value stamped on a normalized signing-key entry. A JWK Set
#: carries no lifecycle field -- RFC 7517 publishes only keys a relying
#: party should be willing to use -- so every published key is recorded as
#: active.
SIGNING_KEY_STATUS_ACTIVE = "active"

#: ``source`` marker recorded on entries derived from a fetched JWK Set,
#: so an operator reading the cache can tell where a key came from.
SIGNING_KEY_SOURCE_JWKS = "jwks"

#: How long a cached JWK Set is served without re-fetching from the
#: service. Ed25519 public keys do not expire, so the TTL only controls how
#: quickly a rotation or revocation on the service side is picked up.
DEFAULT_JWKS_CACHE_TTL_SECONDS = 3600.0

#: Candidate ``(group, method)`` pairs on the codegen'd
#: ``PychestrateRestClient`` that may carry the service's JWKS operation.
#: The exact name depends on how ``codecraftai-restclient-pychestrate`` was
#: generated from the service's OpenAPI document, so the fetch probes these
#: in order and reports a precise, actionable error when none is present
#: rather than hand-rolling an HTTP call against the service.
DEFAULT_JWKS_OPERATION_CANDIDATES = (
    ("dispatch", "envelope_signing_keys_jwks"),
    ("dispatch", "envelope_signing_keys_jwks_list"),
    ("dispatch", "envelope_signing_jwks_list"),
    ("dispatch", "jwks_list"),
    ("dispatch", "jwks"),
    ("wellknown", "jwks_json_list"),
    ("wellknown", "jwks_list"),
    ("wellknown", "jwks"),
)


class JwksAspects:
    """
        The :class:`JwksAspects` packages caller-controlled behavior for
        JWKS retrieval and caching (cache TTL, the codegen'd operation
        names to probe, and an optional fetch override).

        Following the project's Aspects pattern, behavior modification is
        carried as data on a single parameter rather than via decorators,
        so a caller -- or a test -- can redirect the fetch without the
        module reaching for a decorator or a module-level switch.
    """

    def __init__(self, *,
                 cache_ttl_seconds: float = DEFAULT_JWKS_CACHE_TTL_SECONDS,
                 operation_candidates: Optional[Tuple[Tuple[str, str], ...]] = None,
                 fetch_func: Optional[Callable[[Any], Dict[str, Any]]] = None):
        """
            Initializes a :class:`JwksAspects` instance.

            :param cache_ttl_seconds: How long a cached JWK Set is served
                                      before the client re-fetches it from
                                      the service.
            :param operation_candidates: Optional ordered ``(group, method)``
                                         pairs probed on the codegen'd REST
                                         client for the JWKS operation.
                                         ``None`` uses
                                         :data:`DEFAULT_JWKS_OPERATION_CANDIDATES`.
            :param fetch_func: Optional callable taking the REST client and
                               returning the raw JWK Set document. ``None``
                               resolves the operation off the codegen'd REST
                               client.

            :raises: BadParameterError
        """
        if cache_ttl_seconds <= 0:
            raise BadParameterError(
                "JwksAspects requires 'cache_ttl_seconds' > 0.",
                "cache_ttl_seconds", "Pass a positive number of seconds.")

        if operation_candidates is None:
            operation_candidates = DEFAULT_JWKS_OPERATION_CANDIDATES

        self._cache_ttl_seconds = cache_ttl_seconds
        self._operation_candidates = operation_candidates
        self._fetch_func = fetch_func
        return

    @property
    def cache_ttl_seconds(self) -> float:
        """
            Gets the cache time-to-live in seconds.

            :returns: The TTL in seconds.
        """
        return self._cache_ttl_seconds

    @property
    def operation_candidates(self) -> Tuple[Tuple[str, str], ...]:
        """
            Gets the ordered ``(group, method)`` pairs probed on the
            codegen'd REST client for the JWKS operation.

            :returns: The candidate operation pairs.
        """
        return self._operation_candidates

    @property
    def fetch_func(self) -> Optional[Callable[[Any], Dict[str, Any]]]:
        """
            Gets the optional JWKS fetch override.

            :returns: The fetch callable, or ``None``.
        """
        return self._fetch_func


DEFAULT_JWKS_ASPECTS = JwksAspects()


def _now_utc() -> datetime:
    """
        Produces the current time as a timezone-aware UTC datetime.

        :returns: The current UTC :class:`datetime`.
    """
    rtnval = datetime.now(timezone.utc)
    return rtnval


def decode_base64url(value: str, field_label: str) -> bytes:
    """
        Decodes an unpadded base64url string as used by RFC 7515 / 7517
        for JWK member values.

        :param value: The base64url-encoded text.
        :param field_label: A short label naming the field being decoded,
                            used in error messages for debuggability.

        :returns: The decoded bytes.
        :raises: ConfigurationError when the value is not valid base64url.
    """
    padding_needed = (-len(value)) % 4
    padded = value + ("=" * padding_needed)

    try:
        decoded = base64.urlsafe_b64decode(padded.encode("ascii"))
    except (binascii.Error, UnicodeEncodeError, ValueError) as decodeerr:
        errmsg_lines = [
            "A JWK member is not valid base64url.",
            "    field: {}".format(field_label),
            "    reason: {}".format(decodeerr),
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise ConfigurationError(errmsg) from decodeerr

    rtnval = decoded
    return rtnval


def decode_jwk_ed25519_public_key(jwk: Dict[str, Any],
                                  source_label: str) -> Ed25519PublicKey:
    """
        Decodes an RFC 7517 / RFC 8037 OKP Ed25519 JWK into an
        :class:`Ed25519PublicKey`.

        :param jwk: The JWK object as published in the service's JWK Set.
        :param source_label: A short label naming where the JWK came from,
                             used in error messages for debuggability.

        :returns: The decoded :class:`Ed25519PublicKey`.
        :raises: ConfigurationError when the JWK is not a well-formed OKP
                 Ed25519 public key.
    """
    key_type = jwk.get("kty", None)
    if key_type != JWK_KEY_TYPE_OKP:
        errmsg_lines = [
            "A JWK in the service JWK Set is not an Edwards-curve key.",
            "    source: {}".format(source_label),
            "    kty: {!r}".format(key_type),
            "    expected kty: {!r}".format(JWK_KEY_TYPE_OKP),
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise ConfigurationError(errmsg)

    curve = jwk.get("crv", None)
    if curve != JWK_CURVE_ED25519:
        errmsg_lines = [
            "A JWK in the service JWK Set is not an Ed25519 key.",
            "    source: {}".format(source_label),
            "    crv: {!r}".format(curve),
            "    expected crv: {!r}".format(JWK_CURVE_ED25519),
            "Pychestrate signs job envelopes with Ed25519.",
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise ConfigurationError(errmsg)

    encoded_public = jwk.get("x", None)
    if encoded_public is None:
        errmsg_lines = [
            "A JWK in the service JWK Set carries no 'x' public-key value.",
            "    source: {}".format(source_label),
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise ConfigurationError(errmsg)

    if not isinstance(encoded_public, str):
        errmsg_lines = [
            "A JWK in the service JWK Set carries a non-string 'x' value.",
            "    source: {}".format(source_label),
            "    type: {}".format(type(encoded_public).__name__),
            "RFC 7517 encodes 'x' as a base64url string.",
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise ConfigurationError(errmsg)

    if len(encoded_public) == 0:
        errmsg_lines = [
            "A JWK in the service JWK Set carries an empty 'x' public-key "
            "value.",
            "    source: {}".format(source_label),
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise ConfigurationError(errmsg)

    public_bytes = decode_base64url(
        value=encoded_public, field_label="{} 'x'".format(source_label))

    if len(public_bytes) != ED25519_PUBLIC_KEY_LENGTH:
        errmsg_lines = [
            "A JWK in the service JWK Set carries an 'x' value that is not a "
            "raw Ed25519 public key.",
            "    source: {}".format(source_label),
            "    decoded length: {}".format(len(public_bytes)),
            "    expected length: {}".format(ED25519_PUBLIC_KEY_LENGTH),
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise ConfigurationError(errmsg)

    try:
        loaded = Ed25519PublicKey.from_public_bytes(public_bytes)
    except Exception as loaderr:
        errmsg_lines = [
            "A JWK in the service JWK Set could not be loaded as an Ed25519 "
            "public key.",
            "    source: {}".format(source_label),
            "    reason: {}".format(loaderr),
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise ConfigurationError(errmsg) from loaderr

    rtnval = loaded
    return rtnval


def is_verification_jwk(jwk: Dict[str, Any]) -> bool:
    """
        Returns True when a JWK is published for signature verification.

        RFC 7517 says a relying party must ignore keys it cannot or should
        not use, so a JWK Set that mixes in encryption keys (or keys whose
        ``key_ops`` exclude ``verify``) is not an error -- those entries are
        simply skipped.

        :param jwk: The JWK object to inspect.

        :returns: True when the JWK may be used to verify signatures.
    """
    use = jwk.get("use", None)
    if use is not None and use != JWK_USE_SIGNATURE:
        rtnval = False
        return rtnval

    key_ops = jwk.get("key_ops", None)
    if key_ops is not None and JWK_KEY_OP_VERIFY not in key_ops:
        rtnval = False
        return rtnval

    rtnval = True
    return rtnval


def jwk_to_signing_key_entry(jwk: Dict[str, Any],
                             source_label: str) -> Dict[str, Any]:
    """
        Normalizes an RFC 7517 OKP / Ed25519 JWK into the signing-key entry
        shape the verifier resolves keys from (the same shape the
        enrollment-bundled ``envelope_signing_keys`` set uses).

        :param jwk: The JWK object as published in the service's JWK Set.
        :param source_label: A short label naming where the JWK came from,
                             used in error messages for debuggability.

        :returns: A normalized signing-key dictionary carrying ``key_id``,
                  ``algorithm``, ``public_key_pem``, ``status``, and
                  ``source``.
        :raises: ConfigurationError when the JWK is malformed or carries no
                 ``kid``.
    """
    key_id = jwk.get("kid", None)
    if key_id is None or not isinstance(key_id, str) or len(key_id) == 0:
        errmsg_lines = [
            "A JWK in the service JWK Set carries no usable 'kid'.",
            "    source: {}".format(source_label),
            "    kid: {!r}".format(key_id),
            "Every dispatched envelope names its signer by key id, so a JWK "
            "without a string 'kid' can never be selected.",
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise ConfigurationError(errmsg)

    public_key = decode_jwk_ed25519_public_key(
        jwk=jwk, source_label="{} kid={!r}".format(source_label, key_id))

    public_key_pem = public_key.public_bytes(
        encoding=Encoding.PEM,
        format=PublicFormat.SubjectPublicKeyInfo,
    ).decode("ascii")

    rtnval = {
        "key_id": key_id,
        "algorithm": SIGNING_KEY_ALGORITHM_ED25519,
        "public_key_pem": public_key_pem,
        "status": SIGNING_KEY_STATUS_ACTIVE,
        "source": SIGNING_KEY_SOURCE_JWKS,
    }
    return rtnval


def jwk_to_dict(jwk: Any) -> Dict[str, Any]:
    """
        Coerces a single JWK (a codegen'd schema object or a plain dict)
        into a plain dictionary.

        :param jwk: The JWK object from a fetched JWK Set.

        :returns: A plain dictionary view of the JWK.
        :raises: ConfigurationError when the JWK is not dictionary-like.
    """
    if isinstance(jwk, dict):
        rtnval = dict(jwk)
        return rtnval

    to_dict = getattr(jwk, "to_dict", None)
    if callable(to_dict):
        rtnval = to_dict()
        return rtnval

    # Fall back to attribute access for the documented RFC 7517 members.
    member_names = ["kty", "crv", "kid", "alg", "use", "key_ops", "x"]

    collected: Dict[str, Any] = {}
    for nxtname in member_names:
        value = getattr(jwk, nxtname, None)
        if value is not None:
            collected[nxtname] = value

    if len(collected) == 0:
        errmsg_lines = [
            "An entry in the service JWK Set is not a JWK object.",
            "    type: {}".format(type(jwk).__name__),
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise ConfigurationError(errmsg)

    rtnval = collected
    return rtnval


def jwks_response_to_document(response: Any) -> Dict[str, Any]:
    """
        Coerces the object returned by the codegen'd JWKS operation into a
        plain JWK Set document dictionary.

        :param response: The value returned by the JWKS operation.

        :returns: A dictionary carrying at least a ``keys`` member.
        :raises: ConfigurationError when the response is not a JWK Set.
    """
    if isinstance(response, dict):
        rtnval = dict(response)
        return rtnval

    to_dict = getattr(response, "to_dict", None)
    if callable(to_dict):
        converted = to_dict()
        if not isinstance(converted, dict):
            errmsg_lines = [
                "The service JWKS response did not convert to a JWK Set "
                "document.",
                "    type: {}".format(type(converted).__name__),
            ]
            errmsg = os.linesep.join(errmsg_lines)
            raise ConfigurationError(errmsg)
        rtnval = converted
        return rtnval

    raw_keys = getattr(response, "keys", None)
    if raw_keys is None:
        errmsg_lines = [
            "The service JWKS response is not a JWK Set document.",
            "    type: {}".format(type(response).__name__),
            "Expected an RFC 7517 document carrying a 'keys' array.",
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise ConfigurationError(errmsg)

    rtnval = {"keys": raw_keys}
    return rtnval


def normalize_jwks_document(document: Dict[str, Any],
                            source_label: str) -> List[Dict[str, Any]]:
    """
        Normalizes an RFC 7517 JWK Set document into the list of
        signing-key entries the verifier resolves keys from.

        JWKs that are not published for signature verification are skipped
        (RFC 7517 requires a relying party to ignore keys it cannot use).
        A JWK that IS published for verification but is malformed is an
        error -- silently dropping it would turn a service-side mistake
        into an unexplained "unknown key" refusal on the pod.

        :param document: The decoded JWK Set document.
        :param source_label: A short label naming where the document came
                             from, used in error messages for debuggability.

        :returns: The list of normalized signing-key dictionaries.
        :raises: ConfigurationError when the document carries no ``keys``
                 array or a usable JWK is malformed.
    """
    raw_keys = document.get("keys", None)
    if raw_keys is None:
        errmsg_lines = [
            "The service JWK Set document carries no 'keys' array.",
            "    source: {}".format(source_label),
            "    members: {}".format(sorted(document.keys())),
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise ConfigurationError(errmsg)

    if not isinstance(raw_keys, list):
        errmsg_lines = [
            "The service JWK Set document's 'keys' member is not an array.",
            "    source: {}".format(source_label),
            "    type: {}".format(type(raw_keys).__name__),
        ]
        errmsg = os.linesep.join(errmsg_lines)
        raise ConfigurationError(errmsg)

    rtnval: List[Dict[str, Any]] = []
    skipped = 0

    for nxtraw in raw_keys:
        jwk = jwk_to_dict(jwk=nxtraw)

        if not is_verification_jwk(jwk=jwk):
            skipped += 1
            logger.debug(
                "pychestrate-client: skipping JWK kid=%r from %s; it is not "
                "published for signature verification.",
                jwk.get("kid", None), source_label,
            )
            continue

        entry = jwk_to_signing_key_entry(jwk=jwk, source_label=source_label)
        rtnval.append(entry)

    if len(rtnval) == 0 and len(raw_keys) > 0:
        logger.warning(
            "pychestrate-client: the service JWK Set from %s carried %d "
            "key(s) but none are usable for Ed25519 envelope signature "
            "verification (%d skipped).",
            source_label, len(raw_keys), skipped,
        )

    return rtnval


def resolve_jwks_operation(client: PychestrateMtlsClient,
                           operation_candidates: Tuple[Tuple[str, str], ...]
                           ) -> Tuple[str, str, Callable[..., Any]]:
    """
        Resolves the JWKS operation exposed by the codegen'd
        ``codecraftai-restclient-pychestrate`` package.

        The client never hand-rolls an HTTP call against the service, so
        when the codegen'd package does not expose a JWKS operation this
        halts with an actionable :class:`ConfigurationError` naming the
        candidates that were probed.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param operation_candidates: Ordered ``(group, method)`` pairs to
                                     probe on the client.

        :returns: A tuple of ``(group_name, method_name, bound_method)``.
        :raises: ConfigurationError when no candidate operation is present.
    """
    probed: List[str] = []

    for nxtgroup, nxtmethod in operation_candidates:
        probed.append("{}.{}".format(nxtgroup, nxtmethod))

        group = getattr(client, nxtgroup, None)
        if group is None:
            continue

        operation = getattr(group, nxtmethod, None)
        if operation is None:
            continue
        if not callable(operation):
            continue

        rtnval = (nxtgroup, nxtmethod, operation)
        return rtnval

    errmsg_lines = [
        "The codegen'd 'codecraftai-restclient-pychestrate' package does not "
        "expose a JWKS operation for the service's envelope signing keys.",
        "    probed operations:",
    ]
    for nxtprobed in probed:
        errmsg_lines.append("        {}".format(nxtprobed))
    errmsg_lines.append(
        "The client never hand-rolls HTTP calls against the service. "
        "Regenerate 'codecraftai-restclient-pychestrate' against the service's "
        "OpenAPI document, then add the generated operation name to "
        "'DEFAULT_JWKS_OPERATION_CANDIDATES' in "
        "ccai.pychestrate.client.jwks.")

    errmsg = os.linesep.join(errmsg_lines)
    raise ConfigurationError(errmsg)


def fetch_jwks_document(client: PychestrateMtlsClient,
                        aspects: JwksAspects = DEFAULT_JWKS_ASPECTS
                        ) -> Dict[str, Any]:
    """
        Fetches the service's RFC 7517 JWK Set through the codegen'd REST
        client (or through the caller-supplied ``aspects.fetch_func``).

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param aspects: Behavior-shaping aspects.

        :returns: The decoded JWK Set document.
        :raises: ConfigurationError, requests.RequestException, ccai
                 HTTP error types
    """
    fetch_func = aspects.fetch_func

    if fetch_func is not None:
        response = fetch_func(client)
        rtnval = jwks_response_to_document(response=response)
        return rtnval

    if client._session is None:
        client.authenticate()

    group_name, method_name, operation = resolve_jwks_operation(
        client=client, operation_candidates=aspects.operation_candidates)

    logger.debug(
        "pychestrate-client: fetching the service JWK Set via the codegen'd "
        "%s.%s operation.", group_name, method_name,
    )

    response = operation()

    rtnval = jwks_response_to_document(response=response)
    return rtnval


def refresh_jwks(client: PychestrateMtlsClient, state: ClientState,
                 aspects: JwksAspects = DEFAULT_JWKS_ASPECTS
                 ) -> List[Dict[str, Any]]:
    """
        Fetches the service's JWK Set, normalizes it into signing-key
        entries, and rewrites the TTL-stamped on-disk cache.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param state: The :class:`ClientState` to persist the cache into.
        :param aspects: Behavior-shaping aspects.

        :returns: The refreshed list of signing-key dictionaries.
        :raises: ConfigurationError, requests.RequestException, ccai
                 HTTP error types
    """
    document = fetch_jwks_document(client=client, aspects=aspects)

    keys = normalize_jwks_document(
        document=document, source_label="the service JWK Set endpoint")

    fetched_at = _now_utc().isoformat()
    state.write_envelope_signing_jwks(keys=keys, fetched_at=fetched_at)

    logger.info(
        "pychestrate-client: refreshed the envelope signing JWK Set from the "
        "service (%d key(s), cached at %s).", len(keys), state.envelope_signing_jwks_path,
    )

    rtnval = keys
    return rtnval


def parse_rfc3339_timestamp(value: Optional[str]) -> Optional[datetime]:
    """
        Parses an RFC 3339 timestamp string into a timezone-aware UTC
        :class:`datetime`.

        A cache stamp that cannot be parsed is not an error -- it simply
        makes the cache untrustworthy for freshness purposes -- so this
        returns ``None`` rather than raising.

        :param value: The RFC 3339 timestamp text, or ``None``.

        :returns: The parsed :class:`datetime`, or ``None`` when the value
                  is absent or unparsable.
    """
    if value is None:
        rtnval = None
        return rtnval
    if len(value) == 0:
        rtnval = None
        return rtnval

    normalized = value
    if normalized.endswith("Z"):
        normalized = "{}+00:00".format(normalized[:-1])

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as parseerr:
        logger.warning(
            "pychestrate-client: could not parse the JWKS cache timestamp "
            "%r: %s", value, parseerr,
        )
        rtnval = None
        return rtnval

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    rtnval = parsed
    return rtnval


def load_cached_jwks(state: ClientState
                     ) -> Tuple[List[Dict[str, Any]], Optional[datetime]]:
    """
        Loads the cached JWK Set signing-key entries and the time the cache
        was written.

        A missing or corrupt cache is treated as an empty cache: the client
        logs the problem and re-fetches from the service rather than
        refusing every dispatch because a cache file was truncated.

        :param state: The :class:`ClientState` bound to the on-disk state.

        :returns: A tuple of ``(signing_key_entries, fetched_at_or_none)``.
    """
    if not state.has_envelope_signing_jwks():
        rtnval = ([], None)
        return rtnval

    try:
        document = state.read_envelope_signing_jwks()
    except ConfigurationError as readerr:
        logger.warning(
            "pychestrate-client: the cached JWK Set at %s could not be read "
            "and will be re-fetched: %s",
            state.envelope_signing_jwks_path, readerr,
        )
        rtnval = ([], None)
        return rtnval

    keys = document.get("keys", [])
    fetched_at = parse_rfc3339_timestamp(
        value=document.get("fetched_at", None))

    rtnval = (keys, fetched_at)
    return rtnval


def is_cache_fresh(fetched_at: Optional[datetime], ttl_seconds: float) -> bool:
    """
        Returns True when a cache written at ``fetched_at`` is still within
        its time-to-live.

        A cache stamped in the future (pod clock skew) is treated as fresh
        rather than as an error -- the alternative would be a re-fetch on
        every single dispatch.

        :param fetched_at: When the cache was written, or ``None``.
        :param ttl_seconds: The cache time-to-live in seconds.

        :returns: True when the cache is still fresh.
    """
    if fetched_at is None:
        rtnval = False
        return rtnval

    age_seconds = (_now_utc() - fetched_at).total_seconds()
    if age_seconds < 0:
        rtnval = True
        return rtnval

    rtnval = age_seconds < ttl_seconds
    return rtnval

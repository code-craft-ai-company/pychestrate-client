"""
    TestBase tests for the pychestrate-client JWKS retrieval + cache module.
    An RFC 7517 OKP / Ed25519 JWK must decode into a key that verifies a
    real Ed25519 signature, malformed JWKs must be refused, and the fetched
    set must be cached with a freshness stamp. The REST client is mocked; no
    real network traffic is performed.
"""

import base64
import os
import tempfile

from datetime import datetime, timedelta, timezone

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from automojo import testbase

from automojo.errors.exceptions import ConfigurationError

from automojo.pychestrate.client.jwks import (
    DEFAULT_JWKS_OPERATION_CANDIDATES,
    JwksAspects,
    decode_jwk_ed25519_public_key,
    fetch_jwks_document,
    is_cache_fresh,
    is_verification_jwk,
    jwk_to_signing_key_entry,
    load_cached_jwks,
    normalize_jwks_document,
    refresh_jwks,
    resolve_jwks_operation,
)
from automojo.pychestrate.client.state import ClientState


SIGNING_KEY_ID = "envelope-key-2026"


def _base64url(raw):
    """
        Returns the unpadded base64url encoding of raw bytes.
    """
    encoded = base64.urlsafe_b64encode(raw).decode("ascii")
    rtnval = encoded.rstrip("=")
    return rtnval


def _jwk_for(private_key, key_id=SIGNING_KEY_ID):
    """
        Returns an RFC 7517 / RFC 8037 OKP Ed25519 JWK for a private key.
    """
    raw_public = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )

    rtnval = {
        "kty": "OKP",
        "crv": "Ed25519",
        "alg": "EdDSA",
        "use": "sig",
        "kid": key_id,
        "x": _base64url(raw_public),
    }
    return rtnval


def _make_state(tmpdir):
    """
        Returns a :class:`ClientState` bound to a fresh state directory.
    """
    state = ClientState(state_dir=os.path.join(tmpdir, "state"))
    state.ensure_dir()
    return state


class _FakeJwksDispatchGroup:
    """
        Serves the service JWK Set through a codegen'd-style operation name.
    """

    def __init__(self, document):
        self._document = document
        self.calls = 0
        return

    def envelope_signing_keys_jwks(self, **kwargs):
        self.calls += 1
        rtnval = self._document
        return rtnval


class _FakeJwksRestClient:
    """
        Minimal stand-in carrying a ``dispatch`` group that exposes a JWKS
        operation plus an mTLS session attribute.
    """

    def __init__(self, document):
        self.dispatch = _FakeJwksDispatchGroup(document=document)
        self._session = object()
        return

    def authenticate(self):
        return


class _FakeBareDispatchGroup:
    """
        A dispatch group with no JWKS operation at all.
    """

    def envelope_signing_keys_list(self, **kwargs):
        rtnval = None
        return rtnval


class _FakeNoJwksRestClient:
    """
        Stand-in for a codegen'd REST client generated before the service
        exposed its JWKS endpoint.
    """

    def __init__(self):
        self.dispatch = _FakeBareDispatchGroup()
        self._session = object()
        return

    def authenticate(self):
        return


def test_jwk_decodes_into_a_key_that_verifies_a_real_signature():
    private_key = Ed25519PrivateKey.generate()
    jwk = _jwk_for(private_key)

    public_key = decode_jwk_ed25519_public_key(jwk=jwk, source_label="test")

    payload = b"the canonical envelope bytes"
    signature = private_key.sign(payload)

    # verify() raises InvalidSignature on a mismatch; reaching the assert
    # means the JWK decoded into the correct public key.
    public_key.verify(signature, payload)

    testbase.assert_true(public_key is not None)
    return


def test_jwk_to_signing_key_entry_normalizes_to_pem():
    private_key = Ed25519PrivateKey.generate()
    jwk = _jwk_for(private_key)

    entry = jwk_to_signing_key_entry(jwk=jwk, source_label="test")

    testbase.assert_equal(entry["key_id"], SIGNING_KEY_ID)
    testbase.assert_equal(entry["algorithm"], "ed25519")
    testbase.assert_equal(entry["status"], "active")
    testbase.assert_equal(entry["source"], "jwks")

    expected_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode("ascii")
    testbase.assert_equal(entry["public_key_pem"], expected_pem)
    return


def test_non_okp_jwk_is_refused():
    private_key = Ed25519PrivateKey.generate()
    jwk = _jwk_for(private_key)
    jwk["kty"] = "RSA"

    raised = False
    try:
        decode_jwk_ed25519_public_key(jwk=jwk, source_label="test")
    except ConfigurationError:
        raised = True

    testbase.assert_true(raised)
    return


def test_non_ed25519_curve_jwk_is_refused():
    private_key = Ed25519PrivateKey.generate()
    jwk = _jwk_for(private_key)
    jwk["crv"] = "X25519"

    raised = False
    try:
        decode_jwk_ed25519_public_key(jwk=jwk, source_label="test")
    except ConfigurationError:
        raised = True

    testbase.assert_true(raised)
    return


def test_jwk_with_wrong_length_public_key_is_refused():
    jwk = {
        "kty": "OKP",
        "crv": "Ed25519",
        "kid": SIGNING_KEY_ID,
        "x": _base64url(b"too-short"),
    }

    raised = False
    try:
        decode_jwk_ed25519_public_key(jwk=jwk, source_label="test")
    except ConfigurationError:
        raised = True

    testbase.assert_true(raised)
    return


def test_jwk_without_kid_is_refused():
    private_key = Ed25519PrivateKey.generate()
    jwk = _jwk_for(private_key)
    del jwk["kid"]

    raised = False
    try:
        jwk_to_signing_key_entry(jwk=jwk, source_label="test")
    except ConfigurationError:
        raised = True

    testbase.assert_true(raised)
    return


def test_is_verification_jwk_honors_use_and_key_ops():
    private_key = Ed25519PrivateKey.generate()

    signing_jwk = _jwk_for(private_key)
    testbase.assert_true(is_verification_jwk(jwk=signing_jwk))

    encryption_jwk = _jwk_for(private_key)
    encryption_jwk["use"] = "enc"
    testbase.assert_false(is_verification_jwk(jwk=encryption_jwk))

    ops_jwk = _jwk_for(private_key)
    ops_jwk["key_ops"] = ["deriveKey"]
    testbase.assert_false(is_verification_jwk(jwk=ops_jwk))

    verify_ops_jwk = _jwk_for(private_key)
    verify_ops_jwk["key_ops"] = ["verify"]
    testbase.assert_true(is_verification_jwk(jwk=verify_ops_jwk))
    return


def test_normalize_jwks_document_skips_unusable_keys():
    signing_key = Ed25519PrivateKey.generate()
    encryption_key = Ed25519PrivateKey.generate()

    encryption_jwk = _jwk_for(encryption_key, key_id="enc-key")
    encryption_jwk["use"] = "enc"

    document = {"keys": [_jwk_for(signing_key), encryption_jwk]}

    entries = normalize_jwks_document(
        document=document, source_label="test")

    testbase.assert_equal(len(entries), 1)
    testbase.assert_equal(entries[0]["key_id"], SIGNING_KEY_ID)
    return


def test_normalize_jwks_document_requires_a_keys_array():
    raised = False
    try:
        normalize_jwks_document(
            document={"not_keys": []}, source_label="test")
    except ConfigurationError:
        raised = True

    testbase.assert_true(raised)
    return


def test_malformed_verification_jwk_is_not_silently_dropped():
    # A key published FOR verification that is malformed must surface as an
    # error rather than becoming an unexplained "unknown key" refusal.
    document = {
        "keys": [
            {
                "kty": "OKP",
                "crv": "Ed25519",
                "use": "sig",
                "kid": "broken-key",
                "x": _base64url(b"short"),
            },
        ],
    }

    raised = False
    try:
        normalize_jwks_document(document=document, source_label="test")
    except ConfigurationError:
        raised = True

    testbase.assert_true(raised)
    return


def test_refresh_jwks_fetches_through_the_codegen_operation_and_caches():
    private_key = Ed25519PrivateKey.generate()
    document = {"keys": [_jwk_for(private_key)]}

    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(tmpdir)
        client = _FakeJwksRestClient(document=document)

        keys = refresh_jwks(client=client, state=state)

        testbase.assert_equal(client.dispatch.calls, 1)
        testbase.assert_equal(len(keys), 1)
        testbase.assert_equal(keys[0]["key_id"], SIGNING_KEY_ID)

        # The fetched set is cached with a freshness stamp.
        testbase.assert_true(state.has_envelope_signing_jwks())
        cached_keys, fetched_at = load_cached_jwks(state=state)
        testbase.assert_equal(len(cached_keys), 1)
        testbase.assert_equal(cached_keys[0]["key_id"], SIGNING_KEY_ID)
        testbase.assert_true(fetched_at is not None)
        testbase.assert_true(is_cache_fresh(
            fetched_at=fetched_at, ttl_seconds=60.0))
    return


def test_refresh_jwks_honors_an_injected_fetch_func():
    private_key = Ed25519PrivateKey.generate()
    document = {"keys": [_jwk_for(private_key)]}

    fetches = {"count": 0}

    def _fetch(client):
        fetches["count"] += 1
        rtnval = document
        return rtnval

    aspects = JwksAspects(fetch_func=_fetch)

    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(tmpdir)

        keys = refresh_jwks(
            client=_FakeNoJwksRestClient(), state=state, aspects=aspects)

        testbase.assert_equal(fetches["count"], 1)
        testbase.assert_equal(len(keys), 1)
        testbase.assert_equal(keys[0]["key_id"], SIGNING_KEY_ID)
    return


def test_missing_codegen_jwks_operation_is_reported_not_hand_rolled():
    # The client never hand-rolls HTTP against the service. When the
    # codegen'd package carries no JWKS operation the failure must be an
    # actionable ConfigurationError.
    raised = False
    detail = ""
    try:
        resolve_jwks_operation(
            client=_FakeNoJwksRestClient(),
            operation_candidates=DEFAULT_JWKS_OPERATION_CANDIDATES)
    except ConfigurationError as opserr:
        raised = True
        detail = str(opserr)

    testbase.assert_true(raised)
    testbase.assert_true("automojo-restclient-pychestrate" in detail)
    return


def test_fetch_jwks_document_accepts_a_schema_style_response():
    private_key = Ed25519PrivateKey.generate()

    class _SchemaJwks:
        def __init__(self, keys):
            self.keys = list(keys)
            return

    def _fetch(client):
        rtnval = _SchemaJwks(keys=[_jwk_for(private_key)])
        return rtnval

    aspects = JwksAspects(fetch_func=_fetch)

    document = fetch_jwks_document(
        client=_FakeNoJwksRestClient(), aspects=aspects)

    entries = normalize_jwks_document(
        document=document, source_label="test")

    testbase.assert_equal(len(entries), 1)
    testbase.assert_equal(entries[0]["key_id"], SIGNING_KEY_ID)
    return


def test_cache_freshness_respects_the_ttl():
    now = datetime.now(timezone.utc)

    recent = now - timedelta(seconds=10)
    testbase.assert_true(is_cache_fresh(fetched_at=recent, ttl_seconds=60.0))

    aged = now - timedelta(seconds=120)
    testbase.assert_false(is_cache_fresh(fetched_at=aged, ttl_seconds=60.0))

    # An unstamped cache is never treated as fresh.
    testbase.assert_false(is_cache_fresh(fetched_at=None, ttl_seconds=60.0))

    # Pod clock skew (a future stamp) is treated as fresh rather than
    # forcing a re-fetch on every dispatch.
    skewed = now + timedelta(seconds=120)
    testbase.assert_true(is_cache_fresh(fetched_at=skewed, ttl_seconds=60.0))
    return


def test_corrupt_cache_is_treated_as_empty_rather_than_fatal():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(tmpdir)

        with open(state.envelope_signing_jwks_path, "w",
                  encoding="utf-8") as jwksf:
            jwksf.write("{ this is not json")

        cached_keys, fetched_at = load_cached_jwks(state=state)

        testbase.assert_equal(len(cached_keys), 0)
        testbase.assert_true(fetched_at is None)
    return


def test_absent_cache_loads_as_empty():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(tmpdir)

        cached_keys, fetched_at = load_cached_jwks(state=state)

        testbase.assert_equal(len(cached_keys), 0)
        testbase.assert_true(fetched_at is None)
    return

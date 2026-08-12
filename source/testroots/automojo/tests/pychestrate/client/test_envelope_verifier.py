"""
    TestBase tests for the pychestrate-client envelope signature verifier.
    A valid Ed25519 signature must verify; any tampering with the envelope
    payload, the signature, or the key id must be rejected. No real network
    or service interaction is performed.
"""

import os
import tempfile

from datetime import datetime, timedelta, timezone

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from automojo import testbase

from automojo.errors.exceptions import ConfigurationError

from automojo.pychestrate.client.envelope_verifier import (
    KEY_SOURCE_ENROLLMENT_REFRESH,
    KEY_SOURCE_ENROLLMENT_SET,
    KEY_SOURCE_INSTALLED_OVERRIDE,
    KEY_SOURCE_JWKS_CACHE,
    KEY_SOURCE_JWKS_CACHE_STALE,
    KEY_SOURCE_JWKS_FETCH,
    REASON_OK,
    REASON_SIGNATURE_MISMATCH,
    REASON_UNKNOWN_KEY,
    load_signing_public_key,
    resolve_envelope_signing_key,
    resolve_signing_public_key,
    serialize_envelope_canonical,
    verify_dispatch_envelope,
    verify_signed_envelope,
)
from automojo.pychestrate.client.state import ClientState


SIGNING_KEY_ID = "envelope-key-2026"


def _public_pem(private_key):
    """
        Returns the PEM SubjectPublicKeyInfo text for a private key.
    """
    rtnval = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode("ascii")
    return rtnval


def _key_entry(private_key, key_id):
    """
        Returns a JWKS-style signing-key dict for a private key.
    """
    rtnval = {
        "key_id": key_id,
        "algorithm": "ed25519",
        "public_key_pem": _public_pem(private_key),
        "status": "active",
    }
    return rtnval


def _sample_envelope():
    """
        Returns a representative job-envelope dict matching the canonical
        schema delivered inside a ``PendingDispatch`` body.
    """
    envelope = {
        "schema_version": "2026-04-24",
        "envelope_id": "11111111-1111-1111-1111-111111111111",
        "issued_at": "2026-05-19T12:00:00+00:00",
        "issuer": "pychestrate.example.com/0.3.1",
        "project_id": "22222222-2222-2222-2222-222222222222",
        "job_id": "33333333-3333-3333-3333-333333333333",
        "job_name": "smoke-suite",
        "job_type": "testbase-singlehost",
        "pipeline": None,
        "run_id": "44444444-4444-4444-4444-444444444444",
        "client_id": "55555555-5555-5555-5555-555555555555",
        "pod_id": "66666666-6666-6666-6666-666666666666",
        "refs": {
            "branch": "main",
            "build": "1234",
            "flavor": "release",
            "release": "0.3.1",
            "commit_sha": "abcdef0",
        },
        "params": {"test_root": "/opt/project/source/testroots/automojo"},
        "scoped_secrets": [],
        "results": {"backend_key_prefix": "runs/44444444", "backend_driver": "s3"},
        "forwarding": {
            "progress_url": "https://pychestrate.example.com/progress",
            "summary_url": "https://pychestrate.example.com/summary",
            "auth_method": "mtls",
        },
        "deadline": {
            "soft_deadline_at": "2026-05-19T13:00:00+00:00",
            "hard_deadline_at": "2026-05-19T14:00:00+00:00",
        },
        "extension_claims": {},
    }
    return envelope


def _sign(envelope, private_key):
    """
        Signs the canonical payload of ``envelope`` and returns the hex
        signature.
    """
    payload = serialize_envelope_canonical(envelope)
    signature = private_key.sign(payload)
    return signature.hex()


def test_valid_signature_verifies():
    private_key = Ed25519PrivateKey.generate()
    public_key = private_key.public_key()

    envelope = _sample_envelope()
    signature_hex = _sign(envelope, private_key)

    result = verify_signed_envelope(
        envelope=envelope,
        signature_hex=signature_hex,
        signature_key_id=SIGNING_KEY_ID,
        signing_public_key=public_key,
        signing_key_id=SIGNING_KEY_ID,
    )

    testbase.assert_true(result.valid)
    testbase.assert_equal(result.reason, REASON_OK)
    testbase.assert_equal(result.envelope, envelope)
    return


def test_tampered_envelope_is_rejected():
    private_key = Ed25519PrivateKey.generate()
    public_key = private_key.public_key()

    envelope = _sample_envelope()
    signature_hex = _sign(envelope, private_key)

    # Tamper with the envelope AFTER signing.
    envelope["job_name"] = "malicious-suite"

    result = verify_signed_envelope(
        envelope=envelope,
        signature_hex=signature_hex,
        signature_key_id=SIGNING_KEY_ID,
        signing_public_key=public_key,
        signing_key_id=SIGNING_KEY_ID,
    )

    testbase.assert_false(result.valid)
    testbase.assert_equal(result.reason, REASON_SIGNATURE_MISMATCH)
    testbase.assert_true(result.envelope is None)
    return


def test_tampered_signature_is_rejected():
    private_key = Ed25519PrivateKey.generate()
    public_key = private_key.public_key()

    envelope = _sample_envelope()
    signature_hex = _sign(envelope, private_key)

    # Flip the final hex nibble of the signature.
    last_char = signature_hex[-1]
    flipped = "0" if last_char != "0" else "1"
    tampered_signature = signature_hex[:-1] + flipped

    result = verify_signed_envelope(
        envelope=envelope,
        signature_hex=tampered_signature,
        signature_key_id=SIGNING_KEY_ID,
        signing_public_key=public_key,
        signing_key_id=SIGNING_KEY_ID,
    )

    testbase.assert_false(result.valid)
    testbase.assert_equal(result.reason, REASON_SIGNATURE_MISMATCH)
    return


def test_wrong_key_signature_is_rejected():
    signing_key = Ed25519PrivateKey.generate()
    other_key = Ed25519PrivateKey.generate()

    envelope = _sample_envelope()
    # Sign with a key that is NOT the locally-trusted one.
    signature_hex = _sign(envelope, other_key)

    result = verify_signed_envelope(
        envelope=envelope,
        signature_hex=signature_hex,
        signature_key_id=SIGNING_KEY_ID,
        signing_public_key=signing_key.public_key(),
        signing_key_id=SIGNING_KEY_ID,
    )

    testbase.assert_false(result.valid)
    testbase.assert_equal(result.reason, REASON_SIGNATURE_MISMATCH)
    return


def test_unknown_key_id_is_rejected():
    private_key = Ed25519PrivateKey.generate()
    public_key = private_key.public_key()

    envelope = _sample_envelope()
    signature_hex = _sign(envelope, private_key)

    result = verify_signed_envelope(
        envelope=envelope,
        signature_hex=signature_hex,
        signature_key_id="some-other-key-id",
        signing_public_key=public_key,
        signing_key_id=SIGNING_KEY_ID,
    )

    testbase.assert_false(result.valid)
    testbase.assert_equal(result.reason, REASON_UNKNOWN_KEY)
    return


def test_load_signing_public_key_round_trip():
    private_key = Ed25519PrivateKey.generate()
    public_key = private_key.public_key()
    public_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode("ascii")

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()
        state.write_envelope_signing_key(key_pem=public_pem)

        loaded = load_signing_public_key(state=state)

        # The loaded key must verify a signature from the matching
        # private key.
        envelope = _sample_envelope()
        signature_hex = _sign(envelope, private_key)
        result = verify_signed_envelope(
            envelope=envelope,
            signature_hex=signature_hex,
            signature_key_id=SIGNING_KEY_ID,
            signing_public_key=loaded,
            signing_key_id=SIGNING_KEY_ID,
        )
        testbase.assert_true(result.valid)
    return


def test_resolve_signing_public_key_from_persisted_set():
    # The verifier resolves the key by key_id from the persisted
    # JWKS-style signing-key set.
    private_key = Ed25519PrivateKey.generate()

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()
        state.write_envelope_signing_keys(
            keys=[_key_entry(private_key, SIGNING_KEY_ID)])

        resolved = resolve_signing_public_key(
            state=state, key_id=SIGNING_KEY_ID)

        envelope = _sample_envelope()
        signature_hex = _sign(envelope, private_key)
        result = verify_signed_envelope(
            envelope=envelope, signature_hex=signature_hex,
            signature_key_id=SIGNING_KEY_ID, signing_public_key=resolved,
            signing_key_id=SIGNING_KEY_ID)
        testbase.assert_true(result.valid)
    return


def test_resolve_signing_public_key_refreshes_on_unknown_key():
    # An unknown key_id triggers a single refresh; once the refreshed set
    # carries the key the resolution succeeds.
    private_key = Ed25519PrivateKey.generate()

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()
        # The persisted set is initially empty.
        state.write_envelope_signing_keys(keys=[])

        refresh_calls = {"count": 0}

        def _refresh():
            refresh_calls["count"] += 1
            refreshed = [_key_entry(private_key, SIGNING_KEY_ID)]
            state.write_envelope_signing_keys(keys=refreshed)
            return refreshed

        resolved = resolve_signing_public_key(
            state=state, key_id=SIGNING_KEY_ID, refresh_func=_refresh)

        testbase.assert_equal(refresh_calls["count"], 1)

        envelope = _sample_envelope()
        signature_hex = _sign(envelope, private_key)
        result = verify_signed_envelope(
            envelope=envelope, signature_hex=signature_hex,
            signature_key_id=SIGNING_KEY_ID, signing_public_key=resolved,
            signing_key_id=SIGNING_KEY_ID)
        testbase.assert_true(result.valid)
    return


def test_verify_dispatch_envelope_unknown_key_after_refresh_is_rejected():
    # When even a refresh cannot resolve the key the verifier fails
    # closed with an unknown_key result.
    private_key = Ed25519PrivateKey.generate()

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()
        state.write_envelope_signing_keys(keys=[])

        def _refresh():
            # The refresh still does not carry the requested key.
            state.write_envelope_signing_keys(keys=[])
            return []

        envelope = _sample_envelope()
        signature_hex = _sign(envelope, private_key)

        result = verify_dispatch_envelope(
            state=state, envelope=envelope, signature_hex=signature_hex,
            signature_key_id=SIGNING_KEY_ID, refresh_func=_refresh)

        testbase.assert_false(result.valid)
        testbase.assert_equal(result.reason, REASON_UNKNOWN_KEY)
    return


def test_verify_dispatch_envelope_valid_signature_verifies():
    # End-to-end: resolve the key from the persisted set and verify.
    private_key = Ed25519PrivateKey.generate()

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()
        state.write_envelope_signing_keys(
            keys=[_key_entry(private_key, SIGNING_KEY_ID)])

        envelope = _sample_envelope()
        signature_hex = _sign(envelope, private_key)

        result = verify_dispatch_envelope(
            state=state, envelope=envelope, signature_hex=signature_hex,
            signature_key_id=SIGNING_KEY_ID)

        testbase.assert_true(result.valid)
        testbase.assert_equal(result.reason, REASON_OK)
    return


#
# Key-resolution order
#
# 1. operator-installed override  2. fresh JWK Set cache
# 3. enrollment-bundled set       4. live JWK Set fetch
# 5. stale JWK Set cache          6. envelope-signing-keys refresh
#

def _rfc3339_ago(seconds):
    """
        Returns an RFC 3339 stamp for a moment ``seconds`` in the past.
    """
    moment = datetime.now(timezone.utc) - timedelta(seconds=seconds)
    rtnval = moment.isoformat()
    return rtnval


def _install_override(state, private_key, key_id):
    """
        Installs a single-file operator signing-key override, exactly as
        ``pychestrate-client install-signing-key`` does.
    """
    state.write_envelope_signing_key(key_pem=_public_pem(private_key))
    state.write_config(config={"envelope_signing_key_id": key_id})
    return


def test_fresh_jwks_cache_is_used_without_any_network_call():
    private_key = Ed25519PrivateKey.generate()

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()
        state.write_envelope_signing_jwks(
            keys=[_key_entry(private_key, SIGNING_KEY_ID)],
            fetched_at=_rfc3339_ago(seconds=5))

        fetches = {"count": 0}

        def _fetch_jwks():
            fetches["count"] += 1
            return []

        resolved = resolve_envelope_signing_key(
            state=state, key_id=SIGNING_KEY_ID,
            jwks_refresh_func=_fetch_jwks)

        testbase.assert_equal(resolved.source, KEY_SOURCE_JWKS_CACHE)
        testbase.assert_equal(fetches["count"], 0)

        envelope = _sample_envelope()
        signature_hex = _sign(envelope, private_key)
        result = verify_dispatch_envelope(
            state=state, envelope=envelope, signature_hex=signature_hex,
            signature_key_id=SIGNING_KEY_ID, jwks_refresh_func=_fetch_jwks)
        testbase.assert_true(result.valid)
    return


def test_installed_key_overrides_the_fetched_jwks():
    # The operator pinned key A; the service JWK Set publishes key B under
    # the SAME key id. An envelope signed by A must verify, and one signed
    # by B must not -- the override is the only trust anchor.
    pinned_key = Ed25519PrivateKey.generate()
    service_key = Ed25519PrivateKey.generate()

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()
        _install_override(
            state=state, private_key=pinned_key, key_id=SIGNING_KEY_ID)
        state.write_envelope_signing_jwks(
            keys=[_key_entry(service_key, SIGNING_KEY_ID)],
            fetched_at=_rfc3339_ago(seconds=5))

        resolved = resolve_envelope_signing_key(
            state=state, key_id=SIGNING_KEY_ID)
        testbase.assert_equal(
            resolved.source, KEY_SOURCE_INSTALLED_OVERRIDE)

        envelope = _sample_envelope()

        pinned_result = verify_dispatch_envelope(
            state=state, envelope=envelope,
            signature_hex=_sign(envelope, pinned_key),
            signature_key_id=SIGNING_KEY_ID)
        testbase.assert_true(pinned_result.valid)

        service_result = verify_dispatch_envelope(
            state=state, envelope=envelope,
            signature_hex=_sign(envelope, service_key),
            signature_key_id=SIGNING_KEY_ID)
        testbase.assert_false(service_result.valid)
        testbase.assert_equal(
            service_result.reason, REASON_SIGNATURE_MISMATCH)
    return


def test_installed_override_refuses_an_uncovered_key_id():
    # A pinned pod must NOT quietly fall through to the JWK Set when the
    # service rotates to a key id the pin does not cover.
    pinned_key = Ed25519PrivateKey.generate()
    rotated_key = Ed25519PrivateKey.generate()

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()
        _install_override(
            state=state, private_key=pinned_key, key_id="pinned-key-2026")
        state.write_envelope_signing_jwks(
            keys=[_key_entry(rotated_key, "rotated-key-2027")],
            fetched_at=_rfc3339_ago(seconds=5))

        envelope = _sample_envelope()
        signature_hex = _sign(envelope, rotated_key)

        result = verify_dispatch_envelope(
            state=state, envelope=envelope, signature_hex=signature_hex,
            signature_key_id="rotated-key-2027")

        testbase.assert_false(result.valid)
        testbase.assert_equal(result.reason, REASON_UNKNOWN_KEY)
    return


def test_installed_override_without_a_key_id_is_refused():
    # A hand-placed key file with no recorded key id cannot be scoped to
    # any envelope, so the client fails closed rather than guessing.
    pinned_key = Ed25519PrivateKey.generate()

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()
        state.write_envelope_signing_key(key_pem=_public_pem(pinned_key))
        state.write_config(config={"service_url": "https://svc"})

        raised = False
        try:
            resolve_envelope_signing_key(
                state=state, key_id=SIGNING_KEY_ID)
        except ConfigurationError:
            raised = True

        testbase.assert_true(raised)
    return


def test_enrollment_bundled_set_is_used_before_any_network_fetch():
    private_key = Ed25519PrivateKey.generate()

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()
        state.write_envelope_signing_keys(
            keys=[_key_entry(private_key, SIGNING_KEY_ID)])

        fetches = {"count": 0}

        def _fetch_jwks():
            fetches["count"] += 1
            return []

        resolved = resolve_envelope_signing_key(
            state=state, key_id=SIGNING_KEY_ID,
            jwks_refresh_func=_fetch_jwks)

        testbase.assert_equal(resolved.source, KEY_SOURCE_ENROLLMENT_SET)
        testbase.assert_equal(fetches["count"], 0)
    return


def test_unknown_key_id_is_resolved_by_fetching_the_jwks():
    # Signing-key rotation: the new key id is in no local source, so the
    # client fetches the service JWK Set exactly once.
    rotated_key = Ed25519PrivateKey.generate()

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        fetches = {"count": 0}

        def _fetch_jwks():
            fetches["count"] += 1
            refreshed = [_key_entry(rotated_key, "rotated-key-2027")]
            state.write_envelope_signing_jwks(
                keys=refreshed,
                fetched_at=datetime.now(timezone.utc).isoformat())
            return refreshed

        envelope = _sample_envelope()
        signature_hex = _sign(envelope, rotated_key)

        result = verify_dispatch_envelope(
            state=state, envelope=envelope, signature_hex=signature_hex,
            signature_key_id="rotated-key-2027",
            jwks_refresh_func=_fetch_jwks)

        testbase.assert_true(result.valid)
        testbase.assert_equal(result.reason, REASON_OK)
        testbase.assert_equal(fetches["count"], 1)

        # The fetched set was cached, so a second resolution needs no fetch.
        resolved = resolve_envelope_signing_key(
            state=state, key_id="rotated-key-2027",
            jwks_refresh_func=_fetch_jwks)
        testbase.assert_equal(resolved.source, KEY_SOURCE_JWKS_CACHE)
        testbase.assert_equal(fetches["count"], 1)
    return


def test_stale_jwks_cache_serves_when_the_refetch_source_is_unusable():
    # A pod must not stop running jobs because the JWKS endpoint is
    # unreachable; an Ed25519 public key does not expire.
    private_key = Ed25519PrivateKey.generate()

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()
        state.write_envelope_signing_jwks(
            keys=[_key_entry(private_key, SIGNING_KEY_ID)],
            fetched_at=_rfc3339_ago(seconds=86400))

        def _fetch_jwks():
            raise ConfigurationError("no JWKS operation is available")

        resolved = resolve_envelope_signing_key(
            state=state, key_id=SIGNING_KEY_ID,
            jwks_refresh_func=_fetch_jwks, cache_ttl_seconds=60.0)

        testbase.assert_equal(resolved.source, KEY_SOURCE_JWKS_CACHE_STALE)

        envelope = _sample_envelope()
        signature_hex = _sign(envelope, private_key)
        result = verify_dispatch_envelope(
            state=state, envelope=envelope, signature_hex=signature_hex,
            signature_key_id=SIGNING_KEY_ID,
            jwks_refresh_func=_fetch_jwks, cache_ttl_seconds=60.0)
        testbase.assert_true(result.valid)
    return


def test_stale_cache_is_refreshed_from_the_jwks_when_it_can_be_fetched():
    private_key = Ed25519PrivateKey.generate()

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()
        state.write_envelope_signing_jwks(
            keys=[_key_entry(private_key, SIGNING_KEY_ID)],
            fetched_at=_rfc3339_ago(seconds=86400))

        fetches = {"count": 0}

        def _fetch_jwks():
            fetches["count"] += 1
            refreshed = [_key_entry(private_key, SIGNING_KEY_ID)]
            state.write_envelope_signing_jwks(
                keys=refreshed,
                fetched_at=datetime.now(timezone.utc).isoformat())
            return refreshed

        resolved = resolve_envelope_signing_key(
            state=state, key_id=SIGNING_KEY_ID,
            jwks_refresh_func=_fetch_jwks, cache_ttl_seconds=60.0)

        testbase.assert_equal(resolved.source, KEY_SOURCE_JWKS_FETCH)
        testbase.assert_equal(fetches["count"], 1)
    return


def test_unusable_jwks_source_falls_back_to_the_signing_keys_endpoint():
    # A client pointed at a service without a JWKS endpoint keeps working
    # through the pre-JWKS envelope-signing-keys refresh.
    private_key = Ed25519PrivateKey.generate()

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        def _fetch_jwks():
            raise ConfigurationError("no JWKS operation is available")

        refreshes = {"count": 0}

        def _refresh_signing_keys():
            refreshes["count"] += 1
            refreshed = [_key_entry(private_key, SIGNING_KEY_ID)]
            state.write_envelope_signing_keys(keys=refreshed)
            return refreshed

        envelope = _sample_envelope()
        signature_hex = _sign(envelope, private_key)

        result = verify_dispatch_envelope(
            state=state, envelope=envelope, signature_hex=signature_hex,
            signature_key_id=SIGNING_KEY_ID,
            refresh_func=_refresh_signing_keys,
            jwks_refresh_func=_fetch_jwks)

        testbase.assert_true(result.valid)
        testbase.assert_equal(refreshes["count"], 1)

        resolved = resolve_envelope_signing_key(
            state=state, key_id=SIGNING_KEY_ID,
            jwks_refresh_func=_fetch_jwks,
            enrollment_refresh_func=_refresh_signing_keys)
        testbase.assert_equal(resolved.source, KEY_SOURCE_ENROLLMENT_SET)
    return


def test_enrollment_refresh_source_is_reported_when_it_answers():
    private_key = Ed25519PrivateKey.generate()

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        def _refresh_signing_keys():
            refreshed = [_key_entry(private_key, SIGNING_KEY_ID)]
            return refreshed

        resolved = resolve_envelope_signing_key(
            state=state, key_id=SIGNING_KEY_ID,
            enrollment_refresh_func=_refresh_signing_keys)

        testbase.assert_equal(
            resolved.source, KEY_SOURCE_ENROLLMENT_REFRESH)
    return


def test_envelope_is_refused_when_no_source_resolves_the_key():
    # Fail closed: an unverifiable envelope is never executed.
    private_key = Ed25519PrivateKey.generate()

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        def _fetch_jwks():
            return []

        def _refresh_signing_keys():
            return []

        envelope = _sample_envelope()
        signature_hex = _sign(envelope, private_key)

        result = verify_dispatch_envelope(
            state=state, envelope=envelope, signature_hex=signature_hex,
            signature_key_id=SIGNING_KEY_ID,
            refresh_func=_refresh_signing_keys,
            jwks_refresh_func=_fetch_jwks)

        testbase.assert_false(result.valid)
        testbase.assert_equal(result.reason, REASON_UNKNOWN_KEY)
    return


def test_transport_failures_propagate_instead_of_rejecting_the_envelope():
    # A transient outage must back the consumer loop off and retry rather
    # than permanently rejecting a legitimate job.
    private_key = Ed25519PrivateKey.generate()

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        def _fetch_jwks():
            raise IOError("connection reset by peer")

        envelope = _sample_envelope()
        signature_hex = _sign(envelope, private_key)

        raised = False
        try:
            verify_dispatch_envelope(
                state=state, envelope=envelope,
                signature_hex=signature_hex,
                signature_key_id=SIGNING_KEY_ID,
                jwks_refresh_func=_fetch_jwks)
        except IOError:
            raised = True

        testbase.assert_true(raised)
    return

"""
    TestBase tests for the pychestrate-client envelope signature verifier.
    A valid Ed25519 signature must verify; any tampering with the envelope
    payload, the signature, or the key id must be rejected. No real network
    or service interaction is performed.
"""

import os
import tempfile

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from automojo import testbase

from automojo.pychestrate.client.envelope_verifier import (
    REASON_OK,
    REASON_SIGNATURE_MISMATCH,
    REASON_UNKNOWN_KEY,
    load_signing_public_key,
    serialize_envelope_canonical,
    verify_signed_envelope,
)
from automojo.pychestrate.client.state import ClientState


SIGNING_KEY_ID = "envelope-key-2026"


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

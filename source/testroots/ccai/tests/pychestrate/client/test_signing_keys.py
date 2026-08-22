"""
    TestBase tests for the pychestrate-client signing-key auto-fetch /
    refresh module. The REST client is mocked; no real network traffic is
    performed.
"""

import os
import tempfile

from ccai import testbase

from ccai.pychestrate.client.signing_keys import (
    find_signing_key,
    normalize_signing_keys,
    persist_signing_keys_from_enrollment,
    refresh_signing_keys,
)
from ccai.pychestrate.client.state import ClientState


SIGNING_KEY_ID = "envelope-key-2026"


def _key_dict(key_id=SIGNING_KEY_ID):
    """
        Returns a JWKS-style signing-key dict.
    """
    rtnval = {
        "key_id": key_id,
        "algorithm": "ed25519",
        "public_key_pem": "-----BEGIN PUBLIC KEY-----\nKK\n"
                          "-----END PUBLIC KEY-----",
        "status": "active",
    }
    return rtnval


class _FakeSigningKey:
    """
        A schema-object-style signing key exposing a ``to_dict`` method.
    """

    def __init__(self, payload):
        self._payload = payload
        return

    def to_dict(self):
        rtnval = dict(self._payload)
        return rtnval


class _FakeSigningKeySet:
    """
        Mirrors the codegen'd ``Signingkeyset`` shape.
    """

    def __init__(self, keys):
        self.keys = list(keys)
        return


class _FakeDispatchGroup:
    """
        Serves the envelope signing-key set.
    """

    def __init__(self, keys):
        self._keys = keys
        self.calls = 0
        return

    def envelope_signing_keys_list(self, **kwargs):
        self.calls += 1
        rtnval = _FakeSigningKeySet(keys=self._keys)
        return rtnval


class _FakeRestClient:
    """
        Minimal stand-in carrying the ``dispatch`` group and an mTLS
        session attribute.
    """

    def __init__(self, keys):
        self.dispatch = _FakeDispatchGroup(keys=keys)
        self._session = object()
        return

    def authenticate(self):
        return


def _make_state(state_dir):
    """
        Returns a :class:`ClientState` bound to a fresh state directory.
    """
    state = ClientState(state_dir=state_dir)
    state.ensure_dir()
    return state


def test_normalize_signing_keys_handles_dicts_and_schema_objects():
    raw = [_key_dict("k1"), _FakeSigningKey(_key_dict("k2"))]
    normalized = normalize_signing_keys(raw_keys=raw)

    testbase.assert_equal(len(normalized), 2)
    testbase.assert_equal(normalized[0]["key_id"], "k1")
    testbase.assert_equal(normalized[1]["key_id"], "k2")
    return


def test_normalize_signing_keys_handles_none():
    normalized = normalize_signing_keys(raw_keys=None)
    testbase.assert_equal(len(normalized), 0)
    return


def test_persist_signing_keys_from_enrollment_writes_set():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))
        redeem_body = {"envelope_signing_keys": [_key_dict()]}

        count = persist_signing_keys_from_enrollment(
            state=state, redeem_body=redeem_body)

        testbase.assert_equal(count, 1)
        testbase.assert_true(state.has_envelope_signing_keys())
        persisted = state.read_envelope_signing_keys()
        testbase.assert_equal(persisted[0]["key_id"], SIGNING_KEY_ID)
    return


def test_persist_signing_keys_from_enrollment_empty_set_writes_nothing():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))

        count = persist_signing_keys_from_enrollment(
            state=state, redeem_body={})

        testbase.assert_equal(count, 0)
        testbase.assert_false(state.has_envelope_signing_keys())
    return


def test_refresh_signing_keys_calls_endpoint_and_persists():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _make_state(os.path.join(tmpdir, "state"))
        client = _FakeRestClient(keys=[_FakeSigningKey(_key_dict())])

        keys = refresh_signing_keys(client=client, state=state)

        testbase.assert_equal(client.dispatch.calls, 1)
        testbase.assert_equal(len(keys), 1)
        testbase.assert_equal(keys[0]["key_id"], SIGNING_KEY_ID)

        # The refreshed set is persisted to disk.
        persisted = state.read_envelope_signing_keys()
        testbase.assert_equal(persisted[0]["key_id"], SIGNING_KEY_ID)
    return


def test_find_signing_key_matches_by_key_id():
    keys = [_key_dict("k1"), _key_dict("k2")]

    found = find_signing_key(keys=keys, key_id="k2")
    testbase.assert_true(found is not None)
    testbase.assert_equal(found["key_id"], "k2")

    missing = find_signing_key(keys=keys, key_id="absent")
    testbase.assert_true(missing is None)
    return

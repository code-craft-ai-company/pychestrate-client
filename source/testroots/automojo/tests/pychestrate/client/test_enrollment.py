"""
    TestBase tests for the pychestrate-client enrollment-payload construction
    and the redeem flow. These tests mock the HTTP layer; no real network
    traffic is performed.
"""

import os
import tempfile

from automojo import testbase

from automojo.errors.exceptions import BadParameterError, HTTPRequestError

from automojo.pychestrate.client.enrollment import (
    build_redeem_payload,
    generate_keypair_and_csr,
    perform_enrollment,
)
from automojo.pychestrate.client.state import ClientState


class FakeResponse:
    """
        Minimal stand-in for a :class:`requests.Response`.
    """

    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body
        self.text = str(body)
        return

    def json(self):
        return self._body


class FakeSession:
    """
        Minimal stand-in for a :class:`requests.Session` that records the
        single POST made by the enrollment flow.
    """

    def __init__(self, response):
        self._response = response
        self.last_url = None
        self.last_json = None
        self.last_verify = None
        return

    def post(self, url, json=None, headers=None, verify=None):
        self.last_url = url
        self.last_json = json
        self.last_verify = verify
        return self._response


def test_generate_keypair_and_csr_produces_pem():
    private_key_pem, csr_pem = generate_keypair_and_csr(client_name="unit-client")

    testbase.assert_true(private_key_pem.startswith(b"-----BEGIN PRIVATE KEY-----"))
    testbase.assert_true(csr_pem.startswith(b"-----BEGIN CERTIFICATE REQUEST-----"))
    return


def test_build_redeem_payload_carries_token_and_csr():
    payload = build_redeem_payload(
        bootstrap_secret="secret-abc",
        csr_pem="-----BEGIN CERTIFICATE REQUEST-----\nXX\n-----END CERTIFICATE REQUEST-----",
        client_name="unit-client",
    )

    testbase.assert_equal(payload["bootstrap_secret"], "secret-abc")
    testbase.assert_equal(payload["display_name"], "unit-client")
    testbase.assert_true(payload["csr_pem"].startswith("-----BEGIN CERTIFICATE REQUEST-----"))
    testbase.assert_true("attributes" not in payload)
    return


def test_build_redeem_payload_includes_attributes_when_supplied():
    payload = build_redeem_payload(
        bootstrap_secret="secret-abc",
        csr_pem="csr",
        client_name="unit-client",
        attributes={"zone": "lab-1"},
    )

    testbase.assert_equal(payload["attributes"]["zone"], "lab-1")
    return


def test_perform_enrollment_persists_state():
    fake_body = {
        "client_id": "11111111-2222-3333-4444-555555555555",
        "client_certificate_pem": "-----BEGIN CERTIFICATE-----\nCC\n-----END CERTIFICATE-----",
        "ca_chain_pem": "-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----",
        "not_after": "2030-01-01T00:00:00Z",
    }
    session = FakeSession(response=FakeResponse(status_code=200, body=fake_body))

    with tempfile.TemporaryDirectory() as tmpdir:
        state_dir = os.path.join(tmpdir, "state")
        state = ClientState(state_dir=state_dir)

        result = perform_enrollment(
            service_url="https://pychestrate.example.com",
            bootstrap_token="boot-secret",
            client_name="unit-client",
            state=state,
            session=session,
        )

        testbase.assert_equal(result["client_id"], fake_body["client_id"])
        testbase.assert_true(session.last_url.endswith("/api/v1/enrollment-tokens/redeem"))
        testbase.assert_equal(session.last_json["bootstrap_secret"], "boot-secret")

        testbase.assert_true(state.is_enrolled())
        testbase.assert_equal(state.read_client_id(), fake_body["client_id"])

        config = state.read_config()
        testbase.assert_equal(config["host"], "pychestrate.example.com")
        testbase.assert_equal(config["client_name"], "unit-client")

        key_mode = os.stat(state.client_key_path).st_mode & 0o777
        testbase.assert_equal(key_mode, 0o600)
    return


def test_perform_enrollment_raises_on_non_200():
    session = FakeSession(response=FakeResponse(status_code=403, body={"detail": "denied"}))

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))

        raised = False
        try:
            perform_enrollment(
                service_url="https://pychestrate.example.com",
                bootstrap_token="boot-secret",
                client_name="unit-client",
                state=state,
                session=session,
            )
        except HTTPRequestError:
            raised = True

        testbase.assert_true(raised)
        testbase.assert_false(state.is_enrolled())
    return


def test_perform_enrollment_rejects_bad_service_url():
    session = FakeSession(response=FakeResponse(status_code=200, body={}))

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))

        raised = False
        try:
            perform_enrollment(
                service_url="ftp://nope",
                bootstrap_token="boot-secret",
                client_name="unit-client",
                state=state,
                session=session,
            )
        except BadParameterError:
            raised = True

        testbase.assert_true(raised)
    return

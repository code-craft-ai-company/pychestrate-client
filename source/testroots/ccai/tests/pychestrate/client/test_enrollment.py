"""
    TestBase tests for the pychestrate-client enrollment-payload construction
    and the redeem flow. These tests mock the HTTP layer; no real network
    traffic is performed.
"""

import os
import tempfile

import requests

from ccai import testbase

from ccai.errors.exceptions import BadParameterError, ConfigurationError, HTTPRequestError

from ccai.pychestrate.client.enrollment import (
    build_redeem_payload,
    generate_keypair_and_csr,
    perform_enrollment,
)
from ccai.pychestrate.client.state import ClientState


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

    def __init__(self, response=None, failure=None):
        self._response = response
        self._failure = failure
        self.last_url = None
        self.last_json = None
        self.last_verify = None
        self.last_timeout = None
        return

    def post(self, url, json=None, headers=None, verify=None, timeout=None):
        self.last_url = url
        self.last_json = json
        self.last_verify = verify
        self.last_timeout = timeout
        if self._failure is not None:
            raise self._failure
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


def test_perform_enrollment_persists_bundled_signing_keys():
    # The enrollment-redeem response now bundles the JWKS-style envelope
    # signing-key set; it must be persisted alongside the mTLS material.
    fake_body = {
        "client_id": "11111111-2222-3333-4444-555555555555",
        "client_certificate_pem": "-----BEGIN CERTIFICATE-----\nCC\n-----END CERTIFICATE-----",
        "ca_chain_pem": "-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----",
        "not_after": "2030-01-01T00:00:00Z",
        "envelope_signing_keys": [
            {
                "key_id": "envelope-key-2026",
                "algorithm": "ed25519",
                "public_key_pem": "-----BEGIN PUBLIC KEY-----\nKK\n"
                                  "-----END PUBLIC KEY-----",
                "status": "active",
            },
        ],
    }
    session = FakeSession(response=FakeResponse(status_code=200, body=fake_body))

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))

        result = perform_enrollment(
            service_url="https://pychestrate.example.com",
            bootstrap_token="boot-secret",
            client_name="unit-client",
            state=state,
            session=session,
        )

        testbase.assert_equal(result["signing_key_count"], 1)
        testbase.assert_true(state.has_envelope_signing_keys())

        persisted = state.read_envelope_signing_keys()
        testbase.assert_equal(len(persisted), 1)
        testbase.assert_equal(persisted[0]["key_id"], "envelope-key-2026")
    return


def test_perform_enrollment_without_bundled_signing_keys_persists_none():
    # When the redeem response omits the signing-key set the enrollment
    # still succeeds; the set is refreshed lazily later.
    fake_body = {
        "client_id": "11111111-2222-3333-4444-555555555555",
        "client_certificate_pem": "-----BEGIN CERTIFICATE-----\nCC\n-----END CERTIFICATE-----",
        "ca_chain_pem": "-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----",
        "not_after": "2030-01-01T00:00:00Z",
    }
    session = FakeSession(response=FakeResponse(status_code=200, body=fake_body))

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))

        result = perform_enrollment(
            service_url="https://pychestrate.example.com",
            bootstrap_token="boot-secret",
            client_name="unit-client",
            state=state,
            session=session,
        )

        testbase.assert_equal(result["signing_key_count"], 0)
        testbase.assert_false(state.has_envelope_signing_keys())
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


GOOD_BODY = {
    "client_id": "11111111-2222-3333-4444-555555555555",
    "client_certificate_pem": "-----BEGIN CERTIFICATE-----\nCC\n-----END CERTIFICATE-----",
    "ca_chain_pem": "-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----",
}


def _enroll(state, session, **overrides):
    arguments = dict(
        service_url="https://pychestrate.example.com:8443",
        bootstrap_token="boot-secret",
        client_name="unit-client",
        state=state,
        session=session,
    )
    arguments.update(overrides)
    return perform_enrollment(**arguments)


def _failure_message(session, **overrides):
    """The one-line message enrollment raises when the request itself fails."""
    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        try:
            _enroll(state, session, **overrides)
        except HTTPRequestError as failure:
            return (str(failure), state.is_enrolled())
    return (None, None)


def test_enrollment_gives_the_redeem_call_a_thirty_second_timeout_by_default():
    session = FakeSession(response=FakeResponse(status_code=200, body=GOOD_BODY))
    with tempfile.TemporaryDirectory() as tmpdir:
        _enroll(ClientState(state_dir=os.path.join(tmpdir, "a")), session)
        default_timeout = session.last_timeout
        _enroll(ClientState(state_dir=os.path.join(tmpdir, "b")), session, timeout_seconds=5)

    testbase.assert_equal((default_timeout, session.last_timeout), (30.0, 5))
    return


def test_a_refused_connection_is_one_line_naming_the_host_and_the_os_reason():
    refused = requests.exceptions.ConnectionError(
        "HTTPSConnectionPool(host='pychestrate.example.com', port=8443): Max retries exceeded with url: "
        "/api/v1/enrollment-tokens/redeem (Caused by NewConnectionError('<urllib3.connection.HTTPSConnection "
        "object at 0x1>: Failed to establish a new connection: [Errno 111] Connection refused'))")
    message, enrolled = _failure_message(FakeSession(failure=refused))

    testbase.assert_equal(
        (len(message.splitlines()), "could not connect to pychestrate.example.com:8443" in message,
         "Connection refused" in message, "Traceback" in message, "HTTPSConnectionPool" in message, enrolled),
        (1, True, True, False, False, False),
    )
    return


def test_a_service_that_does_not_answer_is_reported_with_the_time_waited():
    message, _enrolled = _failure_message(
        FakeSession(failure=requests.exceptions.ReadTimeout("read timed out")), timeout_seconds=7)

    testbase.assert_equal(
        (message, ), ("the service at pychestrate.example.com:8443 did not answer within 7 seconds", ))
    return


def test_a_certificate_the_client_cannot_verify_suggests_insecure_for_development_only():
    message, _enrolled = _failure_message(FakeSession(failure=requests.exceptions.SSLError(
        "HTTPSConnectionPool(host='x', port=443): Max retries exceeded (Caused by SSLError(SSLCertVerificationError(1, "
        "'[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: self-signed certificate (_ssl.c:1000)')))")))

    testbase.assert_equal(
        (len(message.splitlines()), "could not be verified" in message, "--insecure" in message),
        (1, True, True),
    )
    return


def test_an_answer_that_is_not_json_is_named_as_such():
    class NotJson(FakeResponse):
        def json(self):
            raise ValueError("Expecting value")

    message, _enrolled = _failure_message(FakeSession(response=NotJson(status_code=200, body="<html>")))

    testbase.assert_equal(("is not JSON" in message, len(message.splitlines())), (True, 1))
    return


def test_a_long_error_body_is_cut_short():
    session = FakeSession(response=FakeResponse(status_code=500, body="x" * 5000))
    message, _enrolled = _failure_message(session)

    testbase.assert_equal((len(message) < 800, message.endswith("...")), (True, True))
    return


def test_an_enrolled_state_directory_is_not_replaced_without_asking():
    session = FakeSession(response=FakeResponse(status_code=200, body=GOOD_BODY))
    other = dict(GOOD_BODY, client_id="99999999-8888-7777-6666-555555555555")
    second = FakeSession(response=FakeResponse(status_code=200, body=other))

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        _enroll(state, session)
        refused = None
        try:
            _enroll(state, second)
        except ConfigurationError as refusal:
            refused = str(refusal)
        after_refusal = state.read_client_id()
        called_service = second.last_url is not None
        _enroll(state, second, overwrite=True)
        after_force = state.read_client_id()

    testbase.assert_equal(
        (refused is not None and GOOD_BODY["client_id"] in refused and "--force" in refused, after_refusal,
         called_service, after_force),
        (True, GOOD_BODY["client_id"], False, other["client_id"]),
    )
    return


_REJECTION_DETAIL = "\n".join([
    "Enrollment rejected by identity verifier.",
    "TOKEN_ID: 0f6c2c1e-1111-4000-8000-000000000001",
    "REASON: scope_mismatch",
    "EXPECTED_POD_NAME: lab-pod-01",
    "HINT: this token was minted for the pod name above and has not been used up; enroll again with exactly that name.",
])


def test_a_refused_enrollment_prints_the_services_detail_line_by_line_with_the_expected_name():
    """The 403 for a wrong --name names the label the token expects, as readable lines and not as raw JSON."""
    session = FakeSession(response=FakeResponse(status_code=403, body={
        "type": "/errors/forbidden", "title": "Forbidden", "status": 403, "detail": _REJECTION_DETAIL,
        "instance": "/api/v1/enrollment/redeem",
    }))
    message, enrolled = _failure_message(session)
    lines = message.splitlines()

    testbase.assert_equal(
        (lines[0], "    EXPECTED_POD_NAME: lab-pod-01" in lines, any("enroll again with exactly that name" in line for line in lines),
         "{" in message, len(lines), enrolled),
        ("the service refused the enrollment (HTTP 403: Forbidden)", True, True, False, 6, False),
    )
    return


def test_a_very_long_refusal_detail_is_cut_short():
    session = FakeSession(response=FakeResponse(status_code=400, body={"detail": "\n".join("line {}".format(n) for n in range(40))}))
    message, _enrolled = _failure_message(session)

    testbase.assert_equal((len(message.splitlines()), message.splitlines()[-1]), (14, "    ..."))
    return


def test_a_refusal_that_is_not_a_problem_document_keeps_the_raw_body():
    session = FakeSession(response=FakeResponse(status_code=502, body={"message": "bad gateway"}))
    message, _enrolled = _failure_message(session)

    testbase.assert_equal(("Enrollment redeem call failed." in message, "status: 502" in message, "bad gateway" in message), (True, True, True))
    return

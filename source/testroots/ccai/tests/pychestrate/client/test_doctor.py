"""
    TestBase tests for ``pychestrate-client doctor``: an empty machine, a
    partly enrolled one, a healthy one, certificates that are expiring or
    expired, a key anyone can read, and a service that answers or does not.
"""

import http.server
import os
import tempfile
import threading

from datetime import datetime, timedelta, timezone
from typing import Iterator, List, Optional

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from ccai import testbase

from ccai.pychestrate.client.doctor import (
    LEVEL_FAIL,
    LEVEL_OK,
    LEVEL_WARN,
    Check,
    exit_code_for,
    run_checks,
)
from ccai.pychestrate.client.state import ClientState


def _certificate(*, days_left: float) -> bytes:
    """A self-signed certificate with ``days_left`` days of life (negative: already expired)."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "pod-a")])
    now = datetime.now(timezone.utc)
    certificate = (
        x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=400)).not_valid_after(now + timedelta(days=days_left))
        .sign(key, hashes.SHA256())
    )
    return certificate.public_bytes(serialization.Encoding.PEM)


def _enrolled(tmpdir: str, *, days_left: float = 90, service_url: str = "http://127.0.0.1:1",
              host: str = "127.0.0.1", port: Optional[int] = 1, key_mode: int = 0o600,
              signing_keys: bool = True) -> ClientState:
    state = ClientState(state_dir=os.path.join(tmpdir, "state"))
    state.ensure_dir()
    state.write_client_id("11111111-2222-3333-4444-555555555555")
    state.write_private_key(b"-----BEGIN PRIVATE KEY-----\nAA\n-----END PRIVATE KEY-----\n")
    os.chmod(state.client_key_path, key_mode)
    certificate = _certificate(days_left=days_left).decode("ascii")
    state.write_client_cert(certificate)
    state.write_service_ca(certificate)
    state.write_config({"service_url": service_url, "host": host, "port": port, "scheme": "http"})
    if signing_keys:
        state.write_envelope_signing_keys([{"key_id": "k1", "public_key_pem": "x"}])
    return state


class _Answering(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        self.send_response(200)
        self.end_headers()
        return

    def log_message(self, *args) -> None:
        return


def _serve() -> Iterator[int]:
    server = http.server.HTTPServer(("127.0.0.1", 0), _Answering)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_address[1]
    finally:
        server.shutdown()
        server.server_close()


def _levels(checks: List[Check]) -> List[str]:
    return [check.level for check in checks]


def _find(checks: List[Check], text: str) -> Check:
    return next(check for check in checks if text in check.message)


def test_a_machine_that_was_never_enrolled_fails_with_the_command_to_run():
    with tempfile.TemporaryDirectory() as tmpdir:
        checks = run_checks(state=ClientState(state_dir=os.path.join(tmpdir, "nothing")))

    testbase.assert_equal(
        (_levels(checks), "pychestrate-client enroll" in checks[0].message, exit_code_for(checks=checks)),
        ([LEVEL_FAIL], True, 1),
    )
    return


def test_a_partly_enrolled_directory_names_what_is_missing():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _enrolled(tmpdir)
        os.remove(state.client_cert_path)
        os.remove(state.config_path)
        checks = run_checks(state=state, check_service=False)

    testbase.assert_equal(
        (checks[-1].level, "client certificate" in checks[-1].message, "config.json" in checks[-1].message,
         "--force" in checks[-1].message, exit_code_for(checks=checks)),
        (LEVEL_FAIL, True, True, True, 1),
    )
    return


def test_a_healthy_enrollment_with_an_answering_service_passes_every_check():
    with tempfile.TemporaryDirectory() as tmpdir:
        served = _serve()
        port = next(served)
        try:
            state = _enrolled(tmpdir, service_url="http://127.0.0.1:{}".format(port), port=port)
            checks = run_checks(state=state)
        finally:
            for _ in served:
                pass

    testbase.assert_equal(
        (set(_levels(checks)), "answered (HTTP 200)" in checks[-1].message, exit_code_for(checks=checks)),
        ({LEVEL_OK}, True, 0),
    )
    return


def test_a_certificate_close_to_expiry_warns_and_an_expired_one_fails():
    with tempfile.TemporaryDirectory() as tmpdir:
        os.makedirs(os.path.join(tmpdir, "soon"))
        os.makedirs(os.path.join(tmpdir, "gone"))
        soon = run_checks(state=_enrolled(os.path.join(tmpdir, "soon"), days_left=5), check_service=False)
        gone = run_checks(state=_enrolled(os.path.join(tmpdir, "gone"), days_left=-3), check_service=False)

    testbase.assert_equal(
        (_find(soon, "certificate").level, exit_code_for(checks=soon), "expires in" in _find(soon, "certificate").message,
         _find(gone, "certificate").level, exit_code_for(checks=gone), "expired on" in _find(gone, "certificate").message),
        (LEVEL_WARN, 0, True, LEVEL_FAIL, 1, True),
    )
    return


def test_a_private_key_other_users_can_read_is_a_warning_with_the_fix():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _enrolled(tmpdir, key_mode=0o644)
        checks = run_checks(state=state, check_service=False)
    key = _find(checks, "private key")

    testbase.assert_equal((key.level, "chmod 600" in key.message, "644" in key.message), (LEVEL_WARN, True, True))
    return


def test_a_service_that_is_not_listening_fails_in_one_line_and_offline_skips_the_check():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _enrolled(tmpdir)
        online = run_checks(state=state)
        offline = run_checks(state=state, check_service=False)

    testbase.assert_equal(
        (online[-1].level, "could not connect to 127.0.0.1:1" in online[-1].message,
         len(online[-1].message.splitlines()), exit_code_for(checks=online), len(offline) == len(online) - 1,
         exit_code_for(checks=offline)),
        (LEVEL_FAIL, True, 1, 1, True, 0),
    )
    return


def test_missing_signing_keys_are_only_a_warning():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = _enrolled(tmpdir, signing_keys=False)
        checks = run_checks(state=state, check_service=False)
    keys = _find(checks, "signing keys")

    testbase.assert_equal((keys.level, exit_code_for(checks=checks)), (LEVEL_WARN, 0))
    return

"""
    TestBase tests for the pychestrate-client on-disk state directory helpers.
"""

import os
import tempfile

from automojo import testbase

from automojo.pychestrate.client.state import ClientState


def test_state_is_not_enrolled_when_empty():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        testbase.assert_false(state.is_enrolled())
    return


def test_state_round_trips_client_id_and_config():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        state.write_client_id(client_id="client-xyz")
        testbase.assert_equal(state.read_client_id(), "client-xyz")

        state.write_config(config={"service_url": "https://svc", "client_name": "cn"})
        config = state.read_config()
        testbase.assert_equal(config["service_url"], "https://svc")
        testbase.assert_equal(config["client_name"], "cn")
    return


def test_state_private_key_written_with_0600():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        state.write_private_key(key_pem=b"KEYDATA")
        mode = os.stat(state.client_key_path).st_mode & 0o777
        testbase.assert_equal(mode, 0o600)
    return


def test_state_is_enrolled_when_all_files_present():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        state.write_private_key(key_pem=b"KEYDATA")
        state.write_client_cert(cert_pem="CERTDATA")
        state.write_service_ca(ca_pem="CADATA")
        state.write_client_id(client_id="client-xyz")
        state.write_config(config={"host": "svc"})

        testbase.assert_true(state.is_enrolled())
    return

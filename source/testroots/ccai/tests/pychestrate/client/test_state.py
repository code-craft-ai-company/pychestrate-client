"""
    TestBase tests for the pychestrate-client on-disk state directory helpers.
"""

import os
import tempfile

from ccai import testbase

from ccai.pychestrate.client.state import ClientState, default_state_dir


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


def test_state_round_trips_the_jwks_cache():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        testbase.assert_false(state.has_envelope_signing_jwks())

        entry = {
            "key_id": "envelope-key-2026",
            "algorithm": "ed25519",
            "public_key_pem": "-----BEGIN PUBLIC KEY-----\nKK\n"
                              "-----END PUBLIC KEY-----",
            "status": "active",
            "source": "jwks",
        }
        state.write_envelope_signing_jwks(
            keys=[entry], fetched_at="2026-08-12T00:00:00+00:00")

        testbase.assert_true(state.has_envelope_signing_jwks())

        document = state.read_envelope_signing_jwks()
        testbase.assert_equal(
            document["fetched_at"], "2026-08-12T00:00:00+00:00")
        testbase.assert_equal(len(document["keys"]), 1)
        testbase.assert_equal(
            document["keys"][0]["key_id"], "envelope-key-2026")
    return


def test_state_reads_the_installed_signing_key_id():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        # A client that was never enrolled has no config at all.
        testbase.assert_true(state.read_installed_signing_key_id() is None)

        state.write_config(config={"host": "svc"})
        testbase.assert_true(state.read_installed_signing_key_id() is None)

        state.write_config(config={
            "host": "svc",
            "envelope_signing_key_id": "envelope-key-2026",
        })
        testbase.assert_equal(
            state.read_installed_signing_key_id(), "envelope-key-2026")
    return


def test_default_state_dir_is_under_xdg_data_home_or_local_share():
    """New enrollments go to $XDG_DATA_HOME/pychestrate-client; unset, empty or relative falls back to ~/.local/share."""
    with tempfile.TemporaryDirectory() as home:
        chosen = os.path.join(home, "data")
        results = (
            default_state_dir(environ={"XDG_DATA_HOME": chosen}, home=home),
            default_state_dir(environ={}, home=home),
            default_state_dir(environ={"XDG_DATA_HOME": ""}, home=home),
            default_state_dir(environ={"XDG_DATA_HOME": "relative/dir"}, home=home),
        )
        fallback = os.path.join(home, ".local", "share", "pychestrate-client")

        testbase.assert_equal(
            results, (os.path.join(chosen, "pychestrate-client"), fallback, fallback, fallback))
    return


def test_an_already_enrolled_pod_keeps_its_old_state_dir_until_the_new_one_exists():
    """The old ~/.pychestrate-client is used only while it exists and the XDG directory does not."""
    with tempfile.TemporaryDirectory() as home:
        current = os.path.join(home, ".local", "share", "pychestrate-client")
        legacy = os.path.join(home, ".pychestrate-client")

        fresh = default_state_dir(environ={}, home=home)
        os.makedirs(legacy)
        upgraded = default_state_dir(environ={}, home=home)
        os.makedirs(current)
        both = default_state_dir(environ={}, home=home)

        testbase.assert_equal((fresh, upgraded, both), (current, legacy, current))
    return


def test_a_new_state_dir_is_created_private_under_a_missing_parent():
    """ensure_dir makes the whole path and leaves the state directory itself 0700."""
    with tempfile.TemporaryDirectory() as home:
        state = ClientState(state_dir=default_state_dir(environ={}, home=home))
        state.ensure_dir()

        testbase.assert_equal(
            (os.path.isdir(state.state_dir), os.stat(state.state_dir).st_mode & 0o777),
            (True, 0o700),
        )
    return

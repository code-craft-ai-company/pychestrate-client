"""
    .. module:: state
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Read/write helpers for the pychestrate-client local state
                   directory (mTLS material, client identity, configuration).

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2025, Automation Mojo LLC"
__credits__ = []


from typing import Any, Dict, List, Optional

import json
import os

from automojo.errors.exceptions import ConfigurationError


DEFAULT_STATE_DIR = os.path.expanduser("~/.pychestrate-client")

FILENAME_CLIENT_ID = "client_id"
FILENAME_CLIENT_KEY = "client.key"
FILENAME_CLIENT_CERT = "client.crt"
FILENAME_SERVICE_CA = "service-ca.crt"
FILENAME_CONFIG = "config.json"
FILENAME_ENVELOPE_SIGNING_KEY = "envelope-signing-public-key.pem"
FILENAME_ENVELOPE_SIGNING_KEYS = "envelope-signing-keys.json"

#: Sub-directory under the state directory where the dispatch consumer
#: records the local outcome of each executed job (M2 records the
#: testbase output directory + exit code; M3 will push results upstream).
DIRNAME_RUNS = "runs"


class ClientState:
    """
        The :class:`ClientState` object encapsulates read/write access to the
        on-disk state directory used by the pychestrate-client runtime.

        State layout::

            <state_dir>/
                client_id       - the assigned client UUID (text)
                client.key      - PEM-encoded private key (mode 0600)
                client.crt      - PEM-encoded signed client certificate (mode 0644)
                service-ca.crt  - PEM-encoded CA chain used to verify the server
                config.json     - service URL, client name, other settings
    """

    def __init__(self, state_dir: Optional[str] = None):
        """
            Initializes a :class:`ClientState` instance bound to a state directory.

            :param state_dir: Path to the state directory. Defaults to
                              ``~/.pychestrate-client`` when ``None``.
        """
        if state_dir is None:
            state_dir = DEFAULT_STATE_DIR

        self._state_dir = os.path.abspath(state_dir)
        return

    @property
    def state_dir(self) -> str:
        """
            Gets the absolute path to the state directory.

            :returns: The state directory path.
        """
        return self._state_dir

    @property
    def client_id_path(self) -> str:
        """
            Gets the on-disk path where the assigned ``client_id`` is stored.

            :returns: The full path to the ``client_id`` file.
        """
        rtnval = os.path.join(self._state_dir, FILENAME_CLIENT_ID)
        return rtnval

    @property
    def client_key_path(self) -> str:
        """
            Gets the on-disk path where the private key is stored.

            :returns: The full path to the ``client.key`` file.
        """
        rtnval = os.path.join(self._state_dir, FILENAME_CLIENT_KEY)
        return rtnval

    @property
    def client_cert_path(self) -> str:
        """
            Gets the on-disk path where the signed client certificate is stored.

            :returns: The full path to the ``client.crt`` file.
        """
        rtnval = os.path.join(self._state_dir, FILENAME_CLIENT_CERT)
        return rtnval

    @property
    def service_ca_path(self) -> str:
        """
            Gets the on-disk path where the service CA chain is stored.

            :returns: The full path to the ``service-ca.crt`` file.
        """
        rtnval = os.path.join(self._state_dir, FILENAME_SERVICE_CA)
        return rtnval

    @property
    def config_path(self) -> str:
        """
            Gets the on-disk path where the JSON configuration file is stored.

            :returns: The full path to the ``config.json`` file.
        """
        rtnval = os.path.join(self._state_dir, FILENAME_CONFIG)
        return rtnval

    @property
    def envelope_signing_key_path(self) -> str:
        """
            Gets the on-disk path where the service's Ed25519 job-envelope
            signing public key (PEM) is stored.

            :returns: The full path to the envelope signing public-key file.
        """
        rtnval = os.path.join(self._state_dir, FILENAME_ENVELOPE_SIGNING_KEY)
        return rtnval

    @property
    def envelope_signing_keys_path(self) -> str:
        """
            Gets the on-disk path where the JWKS-style envelope signing
            public-key set (JSON) is stored.

            The service exposes its Ed25519 envelope signing public keys
            both inside the enrollment-redeem response and through
            ``GET /api/v1/envelope-signing-keys``. This file is the
            persisted, ``key_id``-indexed copy of that set; it is the
            normal source the dispatch consumer verifies envelopes against.

            :returns: The full path to the ``envelope-signing-keys.json``
                      file.
        """
        rtnval = os.path.join(self._state_dir, FILENAME_ENVELOPE_SIGNING_KEYS)
        return rtnval

    @property
    def runs_dir(self) -> str:
        """
            Gets the on-disk path of the directory that holds per-run
            local execution records written by the dispatch consumer.

            :returns: The full path to the runs directory.
        """
        rtnval = os.path.join(self._state_dir, DIRNAME_RUNS)
        return rtnval

    def ensure_dir(self) -> None:
        """
            Creates the state directory if it does not already exist. The
            directory is created with restrictive permissions (0700) because
            it contains a private key.

            :returns: None
        """
        if not os.path.isdir(self._state_dir):
            os.makedirs(self._state_dir, mode=0o700, exist_ok=True)
        else:
            os.chmod(self._state_dir, 0o700)
        return

    def write_client_id(self, client_id: str) -> None:
        """
            Persists the assigned ``client_id`` to disk.

            :param client_id: The client UUID assigned by the service.

            :returns: None
        """
        path = self.client_id_path
        with open(path, "w", encoding="utf-8") as cidf:
            cidf.write(client_id)
            cidf.write(os.linesep)
        os.chmod(path, 0o644)
        return

    def read_client_id(self) -> str:
        """
            Reads the assigned ``client_id`` from disk.

            :returns: The client UUID string.
            :raises: ConfigurationError
        """
        path = self.client_id_path
        if not os.path.isfile(path):
            errmsg_lines = [
                "The pychestrate-client state directory does not contain a 'client_id' file.",
                "    expected: {}".format(path),
                "Has this client been enrolled? Run 'pychestrate-client enroll ...' first.",
            ]
            errmsg = os.linesep.join(errmsg_lines)
            raise ConfigurationError(errmsg)

        with open(path, "r", encoding="utf-8") as cidf:
            content = cidf.read().strip()

        rtnval = content
        return rtnval

    def write_private_key(self, key_pem: bytes) -> None:
        """
            Persists the PEM-encoded private key to disk with mode 0600.

            :param key_pem: PEM-encoded private key bytes.

            :returns: None
        """
        path = self.client_key_path
        with open(path, "wb") as keyf:
            keyf.write(key_pem)
        os.chmod(path, 0o600)
        return

    def write_client_cert(self, cert_pem: str) -> None:
        """
            Persists the PEM-encoded signed client certificate to disk.

            :param cert_pem: PEM-encoded certificate text.

            :returns: None
        """
        path = self.client_cert_path
        with open(path, "w", encoding="utf-8") as certf:
            certf.write(cert_pem)
        os.chmod(path, 0o644)
        return

    def write_service_ca(self, ca_pem: str) -> None:
        """
            Persists the PEM-encoded service CA chain to disk.

            :param ca_pem: PEM-encoded CA chain text.

            :returns: None
        """
        path = self.service_ca_path
        with open(path, "w", encoding="utf-8") as caf:
            caf.write(ca_pem)
        os.chmod(path, 0o644)
        return

    def write_config(self, config: Dict[str, Any]) -> None:
        """
            Persists the JSON-serializable configuration dictionary to disk.

            :param config: Configuration dictionary to persist.

            :returns: None
        """
        path = self.config_path
        with open(path, "w", encoding="utf-8") as cfgf:
            json.dump(config, cfgf, indent=4, sort_keys=True)
            cfgf.write(os.linesep)
        os.chmod(path, 0o644)
        return

    def write_envelope_signing_key(self, key_pem: str) -> None:
        """
            Persists the PEM-encoded Ed25519 envelope signing public key
            to disk. This key is used by the dispatch consumer to verify
            the signature on every signed job envelope before execution.

            :param key_pem: PEM-encoded Ed25519 public key text.

            :returns: None
        """
        path = self.envelope_signing_key_path
        with open(path, "w", encoding="utf-8") as keyf:
            keyf.write(key_pem)
        os.chmod(path, 0o644)
        return

    def read_envelope_signing_key(self) -> str:
        """
            Reads the PEM-encoded Ed25519 envelope signing public key
            from disk.

            :returns: The PEM-encoded public key text.
            :raises: ConfigurationError
        """
        path = self.envelope_signing_key_path
        if not os.path.isfile(path):
            errmsg_lines = [
                "The pychestrate-client state directory does not contain an "
                "envelope signing public key.",
                "    expected: {}".format(path),
                "The dispatch consumer refuses to execute unverified job "
                "envelopes. Install the service's Ed25519 envelope signing "
                "public key with 'pychestrate-client install-signing-key ...'.",
            ]
            errmsg = os.linesep.join(errmsg_lines)
            raise ConfigurationError(errmsg)

        with open(path, "r", encoding="utf-8") as keyf:
            content = keyf.read()

        rtnval = content
        return rtnval

    def has_envelope_signing_key(self) -> bool:
        """
            Returns True when an envelope signing public key has been
            installed into the state directory.

            :returns: True if the signing public key file is present.
        """
        rtnval = os.path.isfile(self.envelope_signing_key_path)
        return rtnval

    def write_envelope_signing_keys(self, keys: List[Dict[str, Any]]) -> None:
        """
            Persists the JWKS-style envelope signing public-key set to disk
            as a JSON document.

            Each entry mirrors the service ``SigningKey`` schema
            (``key_id``, ``algorithm``, ``public_key_pem``, ``status``).
            The dispatch consumer indexes this set by ``key_id`` to verify
            the signature on every signed job envelope before execution.

            :param keys: A list of signing-key dictionaries to persist.

            :returns: None
        """
        path = self.envelope_signing_keys_path
        document = {"keys": keys}
        with open(path, "w", encoding="utf-8") as keysf:
            json.dump(document, keysf, indent=4, sort_keys=True)
            keysf.write(os.linesep)
        os.chmod(path, 0o644)
        return

    def read_envelope_signing_keys(self) -> List[Dict[str, Any]]:
        """
            Reads the JWKS-style envelope signing public-key set from disk.

            :returns: A list of signing-key dictionaries.
            :raises: ConfigurationError when the file is missing or is not a
                     valid signing-key-set JSON document.
        """
        path = self.envelope_signing_keys_path
        if not os.path.isfile(path):
            errmsg_lines = [
                "The pychestrate-client state directory does not contain an "
                "envelope signing-key set.",
                "    expected: {}".format(path),
                "The set is normally persisted during 'pychestrate-client "
                "enroll' or refreshed automatically from "
                "'GET /api/v1/envelope-signing-keys'.",
            ]
            errmsg = os.linesep.join(errmsg_lines)
            raise ConfigurationError(errmsg)

        try:
            with open(path, "r", encoding="utf-8") as keysf:
                loaded = json.load(keysf)
        except (ValueError, OSError) as readerr:
            errmsg_lines = [
                "The pychestrate-client envelope signing-key set could not "
                "be read.",
                "    path: {}".format(path),
                "    reason: {}".format(readerr),
            ]
            errmsg = os.linesep.join(errmsg_lines)
            raise ConfigurationError(errmsg) from readerr

        if not isinstance(loaded, dict):
            errmsg = ("The envelope signing-key set at {} is not a JSON "
                      "object.".format(path))
            raise ConfigurationError(errmsg)

        keys = loaded.get("keys", None)
        if not isinstance(keys, list):
            errmsg = ("The envelope signing-key set at {} does not carry a "
                      "'keys' list.".format(path))
            raise ConfigurationError(errmsg)

        rtnval = keys
        return rtnval

    def has_envelope_signing_keys(self) -> bool:
        """
            Returns True when a JWKS-style envelope signing-key set has been
            persisted into the state directory.

            :returns: True if the signing-key-set file is present.
        """
        rtnval = os.path.isfile(self.envelope_signing_keys_path)
        return rtnval

    def read_config(self) -> Dict[str, Any]:
        """
            Reads the JSON configuration dictionary from disk.

            :returns: The configuration dictionary.
            :raises: ConfigurationError
        """
        path = self.config_path
        if not os.path.isfile(path):
            errmsg_lines = [
                "The pychestrate-client state directory does not contain a 'config.json' file.",
                "    expected: {}".format(path),
                "Has this client been enrolled? Run 'pychestrate-client enroll ...' first.",
            ]
            errmsg = os.linesep.join(errmsg_lines)
            raise ConfigurationError(errmsg)

        with open(path, "r", encoding="utf-8") as cfgf:
            loaded = json.load(cfgf)

        rtnval = loaded
        return rtnval

    def is_enrolled(self) -> bool:
        """
            Returns True only when every state file required to operate as an
            enrolled client is present on disk.

            :returns: True if the client appears to be fully enrolled.
        """
        required_paths = [
            self.client_id_path,
            self.client_key_path,
            self.client_cert_path,
            self.service_ca_path,
            self.config_path,
        ]

        rtnval = True
        for nxtpath in required_paths:
            if not os.path.isfile(nxtpath):
                rtnval = False
                break

        return rtnval

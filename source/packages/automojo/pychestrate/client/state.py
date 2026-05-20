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


from typing import Any, Dict, Optional

import json
import os

from automojo.errors.exceptions import ConfigurationError


DEFAULT_STATE_DIR = os.path.expanduser("~/.pychestrate-client")

FILENAME_CLIENT_ID = "client_id"
FILENAME_CLIENT_KEY = "client.key"
FILENAME_CLIENT_CERT = "client.crt"
FILENAME_SERVICE_CA = "service-ca.crt"
FILENAME_CONFIG = "config.json"


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

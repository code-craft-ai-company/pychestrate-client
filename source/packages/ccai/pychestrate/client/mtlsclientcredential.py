"""
    .. module:: mtlsclientcredential
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Module that contains the :class:`MtlsClientCredential` which
                   represents a client mTLS identity (client cert + private key +
                   service CA chain) used by the pychestrate-client runtime to
                   authenticate against the Pychestrate REST service.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2025, Automation Mojo LLC"
__credits__ = []


from typing import Any, Dict, List, Optional

from ccai.credentials.basecredential import BaseCredential
from ccai.errors.exceptions import BadParameterError


class MtlsClientCredential(BaseCredential):
    """
        The :class:`MtlsClientCredential` object holds the paths to the on-disk
        materials needed for mutual TLS against the Pychestrate REST service:

            - PEM-encoded client certificate file
            - PEM-encoded private key file
            - PEM-encoded service CA chain file (used for server verification)
    """

    def __init__(self, *, identifier: str, client_cert_path: str, client_key_path: str,
                 service_ca_path: str, categories: Optional[List[str]] = None,
                 role: Optional[str] = "priv"):
        """
            Initializes an :class:`MtlsClientCredential` instance.

            :param identifier: The identifier that is used to reference this credential.
            :param client_cert_path: Filesystem path to the PEM-encoded client certificate.
            :param client_key_path: Filesystem path to the PEM-encoded private key.
            :param service_ca_path: Filesystem path to the PEM-encoded service CA chain.
            :param categories: Optional credential categories. Defaults to ``["mtls"]``.
            :param role: Role label for the credential. Defaults to ``"priv"``.

            :raises: BadParameterError
        """
        if categories is None:
            categories = ["mtls"]

        super().__init__(identifier=identifier, categories=categories, role=role)

        if len(client_cert_path) == 0:
            raise BadParameterError(
                "MtlsClientCredential requires a non-empty 'client_cert_path'.",
                "client_cert_path", "Pass the path to the PEM-encoded client certificate.")
        if len(client_key_path) == 0:
            raise BadParameterError(
                "MtlsClientCredential requires a non-empty 'client_key_path'.",
                "client_key_path", "Pass the path to the PEM-encoded private key.")
        if len(service_ca_path) == 0:
            raise BadParameterError(
                "MtlsClientCredential requires a non-empty 'service_ca_path'.",
                "service_ca_path", "Pass the path to the PEM-encoded service CA chain.")

        self._client_cert_path = client_cert_path
        self._client_key_path = client_key_path
        self._service_ca_path = service_ca_path
        return

    @property
    def client_cert_path(self) -> str:
        """
            Gets the path to the PEM-encoded client certificate file.

            :returns: The file system path.
        """
        return self._client_cert_path

    @property
    def client_key_path(self) -> str:
        """
            Gets the path to the PEM-encoded private key file.

            :returns: The file system path.
        """
        return self._client_key_path

    @property
    def service_ca_path(self) -> str:
        """
            Gets the path to the PEM-encoded service CA chain file.

            :returns: The file system path.
        """
        return self._service_ca_path

    def as_dict(self) -> Dict[str, Any]:
        """
            Returns a dictionary representation of this credential.

            :returns: A dict containing identifier, categories, role, and the
                      on-disk paths to the mTLS material.
        """
        rtnval = super().as_dict()
        rtnval["client_cert_path"] = self._client_cert_path
        rtnval["client_key_path"] = self._client_key_path
        rtnval["service_ca_path"] = self._service_ca_path
        return rtnval

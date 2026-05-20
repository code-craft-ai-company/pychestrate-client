"""
    .. module:: pychestratemtlsclient
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Module that contains the :class:`PychestrateMtlsClient`, a
                   thin subclass of :class:`PychestrateRestClient` that wires a
                   :class:`MtlsClientCredential` into the underlying
                   :class:`requests.Session` so all calls flow over mTLS.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2025, Automation Mojo LLC"
__credits__ = []


from typing import Optional

from requests import Session

from automojo.pychestrate.client.mtlsclientcredential import MtlsClientCredential
from automojo.pychestrate.rest.client.pychestraterestclient import PychestrateRestClient
from automojo.restclient.base.aspectsrest import AspectsRest, DEFAULT_REST_ASPECTS


class PychestrateMtlsClient(PychestrateRestClient):
    """
        The :class:`PychestrateMtlsClient` extends the codegen'd
        :class:`PychestrateRestClient` to install mTLS material on the
        underlying ``requests.Session`` instead of using a bearer-token
        Authorization header.

        Calls to ``authenticate()`` create a session that pins the client
        certificate + private key for outbound mTLS and pins the service CA
        chain for server verification.
    """

    def __init__(self, host: str, credential: MtlsClientCredential, port: Optional[int] = None,
                 scheme: Optional[str] = "https", aspects: AspectsRest = DEFAULT_REST_ASPECTS):
        """
            Initializes a :class:`PychestrateMtlsClient` instance.

            :param host: Hostname of the Pychestrate service.
            :param credential: An :class:`MtlsClientCredential` carrying paths to
                               the client cert/key and the service CA chain.
            :param port: Optional port number.
            :param scheme: URL scheme. Defaults to ``https``.
            :param aspects: Default REST aspects for the client.
        """
        super().__init__(host=host, credential=credential, port=port, scheme=scheme, aspects=aspects)
        return

    def authenticate(self) -> None:
        """
            Overrides :meth:`RestClientBase.authenticate` to build a session that
            uses mutual TLS instead of a bearer token. The client certificate +
            key are mounted via :attr:`Session.cert`, and the service CA chain is
            mounted via :attr:`Session.verify`.

            :returns: None
        """
        credential: MtlsClientCredential = self._credential

        session = Session()
        session.cert = (credential.client_cert_path, credential.client_key_path)
        session.verify = credential.service_ca_path

        self._verify_certificate = credential.service_ca_path
        self._session = session
        return

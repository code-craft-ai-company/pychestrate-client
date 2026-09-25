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


from typing import Any, Optional

from requests import Session

from ccai.pychestrate.client.mtlsclientcredential import MtlsClientCredential
from ccai.pychestrate.rest.client.pychestraterestclient import PychestrateRestClient
from ccai.restclient.base.aspectsrest import AspectsRest, DEFAULT_REST_ASPECTS


#: Where the service mounts its REST API. The codegen'd client's leaf paths
#: ("/clients/{id}/heartbeat") are relative to it, but its base class builds
#: only ``scheme://host[:port]``.
DEFAULT_API_BASE_PATH = "/api/v1"


def normalize_base_path(base_path: Optional[str]) -> str:
    """
        :param base_path: A base path such as ``/api/v1``, ``api/v1/`` or
                          ``/pychestrate/api/v1``; None or empty means the
                          default.

        :returns: The path with one leading slash and no trailing slash.
    """
    if base_path is None or len(base_path.strip("/ ")) == 0:
        rtnval = DEFAULT_API_BASE_PATH
    else:
        rtnval = "/" + base_path.strip("/ ")
    return rtnval


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
                 scheme: Optional[str] = "https", aspects: AspectsRest = DEFAULT_REST_ASPECTS,
                 base_path: Optional[str] = DEFAULT_API_BASE_PATH):
        """
            Initializes a :class:`PychestrateMtlsClient` instance.

            :param host: Hostname of the Pychestrate service.
            :param credential: An :class:`MtlsClientCredential` carrying paths to
                               the client cert/key and the service CA chain.
            :param port: Optional port number.
            :param scheme: URL scheme. Defaults to ``https``.
            :param aspects: Default REST aspects for the client.
            :param base_path: Path the REST API is mounted under; every
                              codegen'd leaf path is appended to it.
                              Defaults to ``/api/v1``.
        """
        super().__init__(host=host, credential=credential, port=port, scheme=scheme, aspects=aspects)
        # The base class calls ``f"{self._host_url}{leaf}"``; keep the bare
        # origin for URLs the service hands back already rooted
        # (presigned uploads), and point every generated call at the API.
        self._origin_url = self._host_url
        self._base_path = normalize_base_path(base_path=base_path)
        self._host_url = "{}{}".format(self._origin_url, self._base_path)
        return

    @property
    def origin_url(self) -> str:
        """
            ``scheme://host[:port]`` of the service, without the API base
            path. Join service-rooted paths (``/api/v1/...``) onto this.
        """
        return self._origin_url

    @property
    def api_url(self) -> str:
        """
            The origin plus the API base path; every generated call starts
            here.
        """
        return self._host_url

    def perform_call(self, *, body: Any = None, **kwargs: Any) -> Any:
        """
            Overrides :meth:`RestClientBase.perform_call` to send codegen'd
            model bodies as their ``to_dict()`` form. The base class hands the
            body straight to ``requests`` as ``json=``, which cannot encode a
            model object and raises TypeError before anything is sent.

            :param body: The request body: a codegen'd model, a dict, or None.
            :param kwargs: Every other ``perform_call`` argument, unchanged.

            :returns: The response from :meth:`RestClientBase.perform_call`.
        """
        wire_body = body
        to_dict = getattr(body, "to_dict", None)
        if callable(to_dict):
            wire_body = to_dict()
        rtnval = super().perform_call(body=wire_body, **kwargs)
        return rtnval

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

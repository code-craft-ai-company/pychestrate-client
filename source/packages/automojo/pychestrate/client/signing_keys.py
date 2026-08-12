"""
    .. module:: signing_keys
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Manages the JWKS-style Ed25519 envelope signing public-key
                   set the dispatch consumer verifies signed job envelopes
                   against.

                   This is the pre-JWKS delivery path: the service bundles
                   ``envelope_signing_keys`` inside the enrollment-redeem
                   response and serves the same set from
                   ``GET /api/v1/envelope-signing-keys``. This module owns
                   persisting that set to the client state directory and
                   refreshing it on demand.

                   The service's RFC 7517 JWK Set is now the first-class
                   source for envelope verification keys -- see
                   :mod:`automojo.pychestrate.client.jwks`. The set this
                   module manages is still used: the enrollment-bundled copy
                   lets a freshly-enrolled client verify envelopes with no
                   network call at all, and the endpoint refresh is the
                   fallback for services that expose no JWKS endpoint. The
                   full resolution order lives on
                   :func:`envelope_verifier.resolve_envelope_signing_key`.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2025, Automation Mojo LLC"
__credits__ = []


from typing import Any, Dict, List, Optional

import logging

from automojo.pychestrate.client.pychestratemtlsclient import PychestrateMtlsClient
from automojo.pychestrate.client.state import ClientState


logger = logging.getLogger(__name__)


def normalize_signing_keys(raw_keys: Optional[List[Any]]) -> List[Dict[str, Any]]:
    """
        Coerces a list of signing-key objects (codegen'd ``Signingkey``
        schema objects or plain dicts) into a list of plain
        JSON-serializable dictionaries.

        :param raw_keys: The list of signing-key objects, or ``None``.

        :returns: A list of plain signing-key dictionaries. An empty list
                  when ``raw_keys`` is ``None`` or empty.
    """
    rtnval: List[Dict[str, Any]] = []

    if raw_keys is None:
        return rtnval
    if len(raw_keys) == 0:
        return rtnval

    for nxtkey in raw_keys:
        if isinstance(nxtkey, dict):
            rtnval.append(dict(nxtkey))
            continue

        to_dict = getattr(nxtkey, "to_dict", None)
        if callable(to_dict):
            rtnval.append(to_dict())
            continue

        # Fall back to attribute access for the documented schema fields.
        entry = {
            "key_id": getattr(nxtkey, "key_id", None),
            "algorithm": getattr(nxtkey, "algorithm", None),
            "public_key_pem": getattr(nxtkey, "public_key_pem", None),
            "status": getattr(nxtkey, "status", None),
        }
        rtnval.append(entry)

    return rtnval


def persist_signing_keys_from_enrollment(state: ClientState,
                                         redeem_body: Dict[str, Any]) -> int:
    """
        Persists the envelope signing-key set carried by an
        enrollment-redeem response into the client state directory.

        The redeem response now bundles ``envelope_signing_keys`` so a
        freshly-enrolled client can verify signed envelopes without an
        extra call to ``GET /api/v1/envelope-signing-keys``. When the field
        is absent or empty the persisted set is left untouched -- the
        client refreshes lazily from the dedicated endpoint instead.

        :param state: The :class:`ClientState` bound to the on-disk state.
        :param redeem_body: The decoded enrollment-redeem response body.

        :returns: The number of signing keys persisted.
    """
    raw_keys = redeem_body.get("envelope_signing_keys", None)
    keys = normalize_signing_keys(raw_keys=raw_keys)

    if len(keys) == 0:
        logger.info(
            "pychestrate-client: enrollment-redeem carried no envelope "
            "signing keys; the set will be refreshed lazily from "
            "GET /api/v1/envelope-signing-keys.",
        )
        rtnval = 0
        return rtnval

    state.write_envelope_signing_keys(keys=keys)
    logger.info(
        "pychestrate-client: persisted %d envelope signing key(s) from the "
        "enrollment-redeem response.", len(keys),
    )

    rtnval = len(keys)
    return rtnval


def refresh_signing_keys(client: PychestrateMtlsClient,
                         state: ClientState) -> List[Dict[str, Any]]:
    """
        Refreshes the persisted envelope signing-key set by calling
        ``GET /api/v1/envelope-signing-keys`` through the codegen'd
        ``dispatch.envelope_signing_keys_list`` method, then rewriting
        ``envelope-signing-keys.json``.

        :param client: A configured :class:`PychestrateMtlsClient`.
        :param state: The :class:`ClientState` to persist into.

        :returns: The refreshed list of signing-key dictionaries.
        :raises: requests.RequestException, automojo HTTP error types
    """
    if client._session is None:
        client.authenticate()

    keyset = client.dispatch.envelope_signing_keys_list()

    raw_keys = getattr(keyset, "keys", None)
    keys = normalize_signing_keys(raw_keys=raw_keys)

    state.write_envelope_signing_keys(keys=keys)
    logger.info(
        "pychestrate-client: refreshed envelope signing-key set from the "
        "service (%d key(s)).", len(keys),
    )

    rtnval = keys
    return rtnval


def find_signing_key(keys: List[Dict[str, Any]],
                     key_id: str) -> Optional[Dict[str, Any]]:
    """
        Returns the signing-key dictionary whose ``key_id`` matches the
        requested id, or ``None`` when no such key is in the set.

        :param keys: The persisted list of signing-key dictionaries.
        :param key_id: The JWKS-style key id to look up.

        :returns: The matching signing-key dictionary, or ``None``.
    """
    rtnval: Optional[Dict[str, Any]] = None

    for nxtkey in keys:
        if nxtkey.get("key_id", None) == key_id:
            rtnval = nxtkey
            break

    return rtnval

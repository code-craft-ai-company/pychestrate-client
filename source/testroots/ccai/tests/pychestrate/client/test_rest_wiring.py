"""
    TestBase tests for how the pod client reaches the service's REST API:
    the /api/v1 base path, model request bodies, the enrollment base path,
    service-rooted upload URLs, and the envelope's test-root field.
"""

import tempfile

from ccai import testbase

from ccai.pychestrate.client.enrollment import derive_api_base_path
from ccai.pychestrate.client.heartbeat import HeartbeatAnomaly, send_one_heartbeat
from ccai.pychestrate.client.mtlsclientcredential import MtlsClientCredential
from ccai.pychestrate.client.pychestratemtlsclient import PychestrateMtlsClient
from ccai.pychestrate.client.result_uploader import PresignedUploadDriver
from ccai.pychestrate.client.testbase_executor import build_testbase_command
from ccai.pychestrate.rest.client.models.dispatch import Dispatchack
from ccai.restclient.base.restclientbase import RestClientBase


def _make_client(**kwargs):
    credential = MtlsClientCredential(
        identifier="unit", client_cert_path="/nonexistent/cert.pem",
        client_key_path="/nonexistent/key.pem", service_ca_path="/nonexistent/ca.pem")
    rtnval = PychestrateMtlsClient(
        host="pychestrate.example.test", credential=credential, port=8443, **kwargs)
    return rtnval


def test_generated_calls_go_to_the_api_base_path():
    """
    **Feature: REST wiring (E2E-01)**
    **Property: The client's call URLs start with the /api/v1 base path the
    service mounts its REST API under, not at the bare host.**
    """
    client = _make_client()

    testbase.assert_equal(client.api_url, "https://pychestrate.example.test:8443/api/v1")
    return


def test_origin_url_excludes_the_base_path():
    """
    **Feature: REST wiring (E2E-08)**
    **Property: origin_url is scheme://host:port with no path, so a
    service-rooted URL can be joined onto it.**
    """
    client = _make_client(base_path="/pychestrate/api/v1/")

    testbase.assert_equal(client.origin_url, "https://pychestrate.example.test:8443")
    return


def test_model_bodies_are_sent_as_dicts():
    """
    **Feature: REST wiring (E2E-08)**
    **Property: A codegen'd model body reaches the base perform_call as its
    to_dict() form, which requests can JSON-encode.**
    """
    client = _make_client()
    captured = {}
    original = RestClientBase.perform_call

    def _capture(self, *, body=None, **kwargs):
        captured["body"] = body
        return None

    RestClientBase.perform_call = _capture
    try:
        ack = Dispatchack(envelope_id="e-1", dispatch_attempt_id="a-1", client_id="c-1",
                          local_run_id="l-1", accepted_at="2026-09-25T00:00:00+00:00")
        client.perform_call(leaf="/runs/r-1/dispatch-ack", method="POST", params=None,
                            body=ack, exp_status=[204], headers=None, context_msg=None,
                            aspects=None)
    finally:
        RestClientBase.perform_call = original

    testbase.assert_equal(captured["body"]["dispatch_attempt_id"], "a-1")
    return


def test_enrollment_base_path_defaults_to_api_v1():
    """
    **Feature: enrollment (E2E-01)**
    **Property: A service URL with no path yields the /api/v1 base path.**
    """
    testbase.assert_equal(derive_api_base_path(service_url="https://host:8443"), "/api/v1")
    return


def test_enrollment_base_path_keeps_a_proxy_prefix():
    """
    **Feature: enrollment (E2E-01)**
    **Property: A service URL behind a path prefix keeps the prefix in front
    of /api/v1.**
    """
    testbase.assert_equal(
        derive_api_base_path(service_url="https://host/pychestrate/"), "/pychestrate/api/v1")
    return


def test_enrollment_base_path_accepts_an_explicit_api_v1():
    """
    **Feature: enrollment (E2E-01)**
    **Property: A service URL that already ends in /api/v1 is not given a
    second /api/v1.**
    """
    testbase.assert_equal(
        derive_api_base_path(service_url="https://host/api/v1"), "/api/v1")
    return


class _PutSession:
    def __init__(self):
        self.urls = []
        return

    def put(self, url, data=None, headers=None):
        self.urls.append(url)
        return type("Response", (), {"status_code": 204})()


def test_service_rooted_upload_url_is_joined_onto_the_origin():
    """
    **Feature: result upload (E2E-08)**
    **Property: A presigned put_url that is a service-rooted path is sent to
    the service origin, not handed to requests without a scheme.**
    """
    client = _make_client()
    session = _PutSession()
    client._session = session
    driver = PresignedUploadDriver(client=client)

    with tempfile.NamedTemporaryFile() as artifact:
        artifact.write(b"x")
        artifact.flush()
        driver._put_artifact(
            target={"path": "a.txt", "put_url": "/api/v1/runs/r-1/artifacts/_local-upload?path=a.txt"},
            local_path=artifact.name)

    testbase.assert_equal(
        session.urls[0],
        "https://pychestrate.example.test:8443/api/v1/runs/r-1/artifacts/_local-upload?path=a.txt")
    return


def test_testbase_command_reads_the_service_root_field():
    """
    **Feature: envelope contract (E2E-14)**
    **Property: The testbase command is built from params.root, the field the
    service's testbase runner writes.**
    """
    envelope = {"envelope_id": "e-1", "params": {"root": "/srv/tests", "includes": ["ccai.tests"]}}

    command = build_testbase_command(envelope=envelope, output_dir="/tmp/out")

    testbase.assert_true("/srv/tests" in " ".join(command))
    return


class _NonJsonHeartbeatClients:
    def clients_heartbeat(self, client_id, **kwargs):
        raise ValueError("Expecting value: line 1 column 1 (char 0)")


class _NonJsonHeartbeatClient:
    def __init__(self):
        self._session = object()
        self.clients = _NonJsonHeartbeatClients()
        return


def test_non_json_heartbeat_answer_is_not_counted_as_success():
    """
    **Feature: heartbeat (E2E-02)**
    **Property: A heartbeat answered with a non-JSON body (a login page)
    raises HeartbeatAnomaly instead of reporting "heartbeat ok".**
    """
    raised = None
    try:
        send_one_heartbeat(client=_NonJsonHeartbeatClient(), client_id="c-1")
    except HeartbeatAnomaly as anomaly:
        raised = anomaly

    testbase.assert_true(raised is not None)
    return

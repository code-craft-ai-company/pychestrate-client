"""
    TestBase tests for the pychestrate-client RPA executor. These tests
    mock the subprocess layer (``git``, the job command, and ``taskplus``);
    no real clone, job, or workflow engine is invoked.
"""

import json
import os
import tempfile

from ccai import testbase

from ccai.pychestrate.client.state import ClientState
from ccai.pychestrate.client.rpa_executor import (
    RPA_STATUS_FAILED,
    RPA_STATUS_SUCCEEDED,
    RPA_STATUS_TIMEOUT,
    RENDERED_WORKFLOW_FILENAME,
    WORK_PACKET_FILENAME,
    RpaExecutorAspects,
    build_process_command,
    execute_rpa_envelope,
    render_taskplus_workflow,
    resolve_rpa_dirs,
    resolve_timeout_seconds,
)


def _sample_rpa_envelope(*, execution_mode="process", source_kind="git",
                         source_ref="https://example.com/rpa-jobs.git#v1.0.0",
                         entrypoint=None, packet=None, timeout_minutes=30):
    """
        Returns a representative verified ``job_kind=rpa`` envelope dict.
    """
    if entrypoint is None:
        entrypoint = {"command": ["python", "-m", "jobs.invoice_import"]}
    if packet is None:
        packet = {"customer_id": 42, "region": "eu"}
    envelope = {
        "envelope_id": "11111111-1111-1111-1111-111111111111",
        "run_id": "44444444-4444-4444-4444-444444444444",
        "client_id": "55555555-5555-5555-5555-555555555555",
        "job_id": "44444444-4444-4444-4444-444444444444",
        "job_name": "rpa:invoice-import",
        "job_type": "rpa-invoice-import",
        "job_kind": "rpa",
        "refs": {"branch": "v1.0.0", "build": "", "flavor": execution_mode},
        "params": {
            "type_id": "invoice-import",
            "version": "1.0.0",
            "execution_mode": execution_mode,
            "source": {"kind": source_kind, "ref": source_ref},
            "entrypoint": entrypoint,
            "environment": {"LOG_LEVEL": "debug"},
            "work_packet": packet,
            "packet_digest": "abc",
            "timeout_minutes": timeout_minutes,
        },
        "results": {"backend_key_prefix": "p", "backend_driver": "s3"},
    }
    if source_kind == "git":
        envelope["source"] = {
            "kind": "git",
            "url": "https://example.com/rpa-jobs.git",
            "ref": "v1.0.0",
            "integration_id": "",
            "credentials_ref": "",
        }
    return envelope


class _CompletedStub:
    def __init__(self, returncode, stdout=b"", stderr=b""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        return


class _RunRecorder:
    """
        Records every ``run_func`` call and scripts results by the argv
        head (``git`` vs everything else).
    """

    def __init__(self, *, job_returncode=0, raise_for=None, timeout_for=None):
        self._job_returncode = job_returncode
        self._raise_for = raise_for
        self._timeout_for = timeout_for
        self.calls = []
        return

    def __call__(self, command, **kwargs):
        import subprocess
        self.calls.append({"command": list(command), "kwargs": dict(kwargs)})
        binary = os.path.basename(str(command[0]))
        if self._raise_for is not None and binary == self._raise_for:
            raise FileNotFoundError("{} not found on this pod".format(binary))
        if self._timeout_for is not None and binary == self._timeout_for:
            raise subprocess.TimeoutExpired(cmd=command, timeout=kwargs.get("timeout", 0))
        if binary == "git":
            rtnval = _CompletedStub(returncode=0)
            return rtnval
        rtnval = _CompletedStub(
            returncode=self._job_returncode, stdout=b"job output", stderr=b"")
        return rtnval


def test_build_process_command_requires_argv():
    command = build_process_command(entrypoint={"command": ["python", "-m", "x"]})
    testbase.assert_equal(command, ["python", "-m", "x"])

    raised = False
    try:
        build_process_command(entrypoint={})
    except Exception:
        raised = True
    testbase.assert_true(raised)
    return


def test_resolve_timeout_prefers_payload_minutes():
    aspects = RpaExecutorAspects(default_timeout_seconds=99.0)
    testbase.assert_equal(
        resolve_timeout_seconds(payload={"timeout_minutes": 2}, aspects=aspects), 120.0)
    testbase.assert_equal(
        resolve_timeout_seconds(payload={}, aspects=aspects), 99.0)
    testbase.assert_equal(
        resolve_timeout_seconds(payload={"timeout_minutes": "bad"}, aspects=aspects), 99.0)
    return


def test_execute_process_job_writes_packet_exports_env_and_records():
    recorder = _RunRecorder()

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()
        aspects = RpaExecutorAspects(run_func=recorder)
        envelope = _sample_rpa_envelope()

        outcome = execute_rpa_envelope(
            state=state, envelope=envelope, local_run_id="local-rpa-1", aspects=aspects)

        testbase.assert_true(outcome.succeeded)
        testbase.assert_equal(outcome.status, RPA_STATUS_SUCCEEDED)
        testbase.assert_equal(outcome.exit_code, 0)
        testbase.assert_equal(outcome.execution_mode, "process")
        testbase.assert_equal(outcome.type_id, "invoice-import")

        dirs = resolve_rpa_dirs(state=state, run_id=envelope["run_id"])
        # git clone + checkout precede the job command.
        job_call = recorder.calls[-1]
        testbase.assert_equal(job_call["command"], ["python", "-m", "jobs.invoice_import"])
        testbase.assert_equal(job_call["kwargs"]["cwd"], dirs["source_dir"])
        testbase.assert_equal(job_call["kwargs"]["timeout"], 30 * 60.0)
        env = job_call["kwargs"]["env"]
        testbase.assert_equal(env["LOG_LEVEL"], "debug")
        testbase.assert_equal(env["PYC_RUN_ID"], envelope["run_id"])
        testbase.assert_equal(env["PYC_JOB_TYPE"], "invoice-import")
        testbase.assert_equal(env["PYC_JOB_VERSION"], "1.0.0")
        testbase.assert_equal(env["PYC_RESULTS_DIR"], dirs["output_dir"])
        packet_path = env["PYC_WORK_PACKET"]
        testbase.assert_equal(os.path.basename(packet_path), WORK_PACKET_FILENAME)
        with open(packet_path, "r", encoding="utf-8") as pf:
            packet = json.load(pf)
        testbase.assert_equal(packet, {"customer_id": 42, "region": "eu"})

        testbase.assert_true(os.path.isfile(os.path.join(dirs["output_dir"], "stdout.log")))
        testbase.assert_true(os.path.isfile(os.path.join(dirs["record_dir"], "rpa-record.json")))
    return


def test_execute_process_job_delivers_packet_via_stdin_and_env():
    for delivery, expect_key in (("stdin", "input"), ("env", "PYC_WORK_PACKET_JSON")):
        recorder = _RunRecorder()
        with tempfile.TemporaryDirectory() as tmpdir:
            state = ClientState(state_dir=os.path.join(tmpdir, "state"))
            state.ensure_dir()
            aspects = RpaExecutorAspects(run_func=recorder)
            envelope = _sample_rpa_envelope(
                entrypoint={"command": ["cat"], "packet_delivery": delivery})

            outcome = execute_rpa_envelope(
                state=state, envelope=envelope, local_run_id="local", aspects=aspects)
            testbase.assert_true(outcome.succeeded)
            job_call = recorder.calls[-1]
            if delivery == "stdin":
                testbase.assert_true(expect_key in job_call["kwargs"])
                testbase.assert_equal(
                    json.loads(job_call["kwargs"]["input"].decode("utf-8"))["customer_id"], 42)
            else:
                testbase.assert_true(expect_key in job_call["kwargs"]["env"])
    return


def test_execute_process_job_local_source_and_failure_exit_code():
    with tempfile.TemporaryDirectory() as tmpdir:
        local_source = os.path.join(tmpdir, "jobs")
        os.makedirs(local_source)
        recorder = _RunRecorder(job_returncode=3)
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()
        aspects = RpaExecutorAspects(run_func=recorder)
        envelope = _sample_rpa_envelope(source_kind="local", source_ref=local_source)

        outcome = execute_rpa_envelope(
            state=state, envelope=envelope, local_run_id="local", aspects=aspects)

        testbase.assert_true(not outcome.succeeded)
        testbase.assert_equal(outcome.status, RPA_STATUS_FAILED)
        testbase.assert_equal(outcome.exit_code, 3)
        # No git calls for a local source; the job ran inside it.
        testbase.assert_equal(len(recorder.calls), 1)
        testbase.assert_equal(recorder.calls[0]["kwargs"]["cwd"], local_source)
    return


def test_execute_process_job_missing_local_source_fails_without_running():
    recorder = _RunRecorder()
    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()
        aspects = RpaExecutorAspects(run_func=recorder)
        envelope = _sample_rpa_envelope(
            source_kind="local", source_ref=os.path.join(tmpdir, "missing"))

        outcome = execute_rpa_envelope(
            state=state, envelope=envelope, local_run_id="local", aspects=aspects)

        testbase.assert_equal(outcome.status, RPA_STATUS_FAILED)
        testbase.assert_equal(len(recorder.calls), 0)
        testbase.assert_true("does not exist" in outcome.detail)
    return


def test_execute_job_timeout_and_missing_binary_are_captured():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        recorder = _RunRecorder(timeout_for="python")
        envelope = _sample_rpa_envelope(source_kind="package", source_ref="rpa-jobs==1.0.0")
        outcome = execute_rpa_envelope(
            state=state, envelope=envelope, local_run_id="local",
            aspects=RpaExecutorAspects(run_func=recorder))
        testbase.assert_equal(outcome.status, RPA_STATUS_TIMEOUT)
        testbase.assert_true(outcome.timed_out)

        recorder = _RunRecorder(raise_for="python")
        outcome = execute_rpa_envelope(
            state=state, envelope=envelope, local_run_id="local",
            aspects=RpaExecutorAspects(run_func=recorder))
        testbase.assert_equal(outcome.status, RPA_STATUS_FAILED)
        testbase.assert_true("could not be found" in outcome.detail)
    return


def test_tasker_job_renders_workflow_and_runs_taskplus():
    recorder = _RunRecorder()
    with tempfile.TemporaryDirectory() as tmpdir:
        source = os.path.join(tmpdir, "jobs")
        os.makedirs(os.path.join(source, "workflows"))
        with open(os.path.join(source, "workflows", "invoice.yaml"), "w", encoding="utf-8") as wf:
            wf.write(
                "name: Invoice flow\n"
                "parameters:\n  region: us\n  batch: 10\n"
                "environment:\n  AKIT_JOBTYPE: rpa\n"
                "tasklist:\n  - task: Import\n    tasktype: jobs.invoice@Import\n"
            )
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()
        aspects = RpaExecutorAspects(run_func=recorder, taskplus_binary="taskplus")
        envelope = _sample_rpa_envelope(
            execution_mode="tasker", source_kind="local", source_ref=source,
            entrypoint={"workflow": "workflows/invoice.yaml"},
            packet={"region": "eu", "customer_id": 7})

        outcome = execute_rpa_envelope(
            state=state, envelope=envelope, local_run_id="local", aspects=aspects)

        testbase.assert_true(outcome.succeeded)
        testbase.assert_equal(outcome.execution_mode, "tasker")
        job_call = recorder.calls[-1]
        testbase.assert_equal(job_call["command"][:3], ["taskplus", "workflow", "run"])
        rendered = job_call["command"][3]
        testbase.assert_equal(os.path.basename(rendered), RENDERED_WORKFLOW_FILENAME)

        import yaml
        with open(rendered, "r", encoding="utf-8") as rf:
            document = yaml.safe_load(rf)
        # Packet keys win over the workflow's own parameters; untouched
        # keys survive; the version environment overlays the workflow's.
        testbase.assert_equal(document["parameters"]["region"], "eu")
        testbase.assert_equal(document["parameters"]["customer_id"], 7)
        testbase.assert_equal(document["parameters"]["batch"], 10)
        testbase.assert_equal(document["environment"]["LOG_LEVEL"], "debug")
        testbase.assert_equal(document["environment"]["AKIT_JOBTYPE"], "rpa")
        testbase.assert_equal(document["tasklist"][0]["tasktype"], "jobs.invoice@Import")
    return


def test_tasker_job_wraps_single_tasking_in_one_task_workflow():
    with tempfile.TemporaryDirectory() as tmpdir:
        work_dir = os.path.join(tmpdir, "work")
        os.makedirs(work_dir)
        rendered = render_taskplus_workflow(
            entrypoint={"tasking": "jobs.invoice@InvoiceImport"},
            payload={"type_id": "invoice-import", "version": "1.2.0", "environment": {"A": "1"}},
            work_packet={"customer_id": 1},
            source_dir=tmpdir,
            work_dir=work_dir,
        )
        import yaml
        with open(rendered, "r", encoding="utf-8") as rf:
            document = yaml.safe_load(rf)
        testbase.assert_equal(document["name"], "rpa:invoice-import@1.2.0")
        testbase.assert_equal(len(document["tasklist"]), 1)
        testbase.assert_equal(document["tasklist"][0]["task"], "InvoiceImport")
        testbase.assert_equal(document["tasklist"][0]["tasktype"], "jobs.invoice@InvoiceImport")
        testbase.assert_equal(document["parameters"], {"customer_id": 1})
        testbase.assert_equal(document["environment"], {"A": "1"})

        raised = False
        try:
            render_taskplus_workflow(
                entrypoint={}, payload={}, work_packet={}, source_dir=tmpdir, work_dir=work_dir)
        except Exception:
            raised = True
        testbase.assert_true(raised)
    return


def test_unknown_execution_mode_fails_cleanly():
    recorder = _RunRecorder()
    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()
        envelope = _sample_rpa_envelope(execution_mode="teleport")
        outcome = execute_rpa_envelope(
            state=state, envelope=envelope, local_run_id="local",
            aspects=RpaExecutorAspects(run_func=recorder))
        testbase.assert_equal(outcome.status, RPA_STATUS_FAILED)
        testbase.assert_equal(len(recorder.calls), 0)
    return


# ---------------------------------------------------------------------------
# M2: streaming, cancel, and the progress reporter
# ---------------------------------------------------------------------------


import io
import threading
import time as _time

from ccai.pychestrate.client.rpa_executor import (
    RPA_STATUS_CANCELLED,
    run_streaming,
)
from ccai.pychestrate.client.rpa_progress_reporter import RpaProgressReporter


class _FakePopen:
    """
        Popen stand-in: emits scripted lines then exits with ``returncode``
        after ``duration`` seconds; honours SIGTERM via ``send_signal``.
    """

    instances = []

    def __init__(self, command, **kwargs):
        self.command = list(command)
        self.kwargs = dict(kwargs)
        self.pid = 4242
        self.returncode = None
        script = _FakePopen.script
        self._lines = list(script.get("lines", []))
        self._final_code = script.get("returncode", 0)
        self._duration = script.get("duration", 0.0)
        self._started = _time.monotonic()
        self.stdout = io.BytesIO(b"".join((l + "\n").encode("utf-8") for l in self._lines))
        self.stdin = io.BytesIO()
        self.signals = []
        _FakePopen.instances.append(self)
        return

    def poll(self):
        if self.returncode is not None:
            return self.returncode
        if _time.monotonic() - self._started >= self._duration:
            self.returncode = self._final_code
        return self.returncode

    def wait(self, timeout=None):
        return self.poll() if self.poll() is not None else self._final_code

    def send_signal(self, sig):
        self.signals.append(sig)
        self.returncode = -int(sig)
        return


def _no_killpg(*args, **kwargs):
    raise OSError("no process group in tests")


def test_streaming_runner_forwards_lines_and_exit_code():
    import os as _os
    _FakePopen.script = {"lines": ["hello", "PYC-PROGRESS {\"percent\": 50, \"message\": \"half\"}", "done"], "returncode": 0}
    _FakePopen.instances = []
    original_killpg = getattr(_os, "killpg", None)
    _os.killpg = _no_killpg
    try:
        seen = []
        aspects = RpaExecutorAspects(popen_func=_FakePopen)
        completion = run_streaming(
            command=["python", "-m", "x"], cwd="/", env={}, timeout_seconds=5.0,
            stdin_bytes=b"{}", on_line=lambda line, stream: seen.append((line, stream)), aspects=aspects)
        testbase.assert_equal(completion.returncode, 0)
        testbase.assert_equal([l for l, _s in seen], ["hello", "PYC-PROGRESS {\"percent\": 50, \"message\": \"half\"}", "done"])
        testbase.assert_true(b"hello" in completion.stdout)
        testbase.assert_equal(_FakePopen.instances[0].kwargs["stderr"], __import__("subprocess").STDOUT)
    finally:
        if original_killpg is not None:
            _os.killpg = original_killpg
    return


def test_streaming_runner_times_out_and_cancels():
    import os as _os
    original_killpg = getattr(_os, "killpg", None)
    _os.killpg = _no_killpg
    try:
        _FakePopen.script = {"lines": [], "returncode": 0, "duration": 30.0}
        aspects = RpaExecutorAspects(popen_func=_FakePopen, cancel_grace_seconds=0.1)
        completion = run_streaming(
            command=["sleep"], cwd="/", env={}, timeout_seconds=0.2, stdin_bytes=None,
            on_line=None, aspects=aspects)
        testbase.assert_true(completion.timed_out)

        flag = {"cancel": False}
        _FakePopen.script = {"lines": [], "returncode": 0, "duration": 30.0}
        aspects = RpaExecutorAspects(
            popen_func=_FakePopen, cancel_grace_seconds=0.1, cancel_poll_seconds=0.1,
            cancel_check_func=lambda: flag["cancel"])
        threading.Timer(0.15, lambda: flag.__setitem__("cancel", True)).start()
        completion = run_streaming(
            command=["sleep"], cwd="/", env={}, timeout_seconds=10.0, stdin_bytes=None,
            on_line=None, aspects=aspects)
        testbase.assert_true(completion.cancelled)
    finally:
        if original_killpg is not None:
            _os.killpg = original_killpg
    return


def test_execute_streams_through_popen_and_reports_cancelled_status():
    import os as _os
    original_killpg = getattr(_os, "killpg", None)
    _os.killpg = _no_killpg
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            local_source = os.path.join(tmpdir, "jobs")
            os.makedirs(local_source)
            state = ClientState(state_dir=os.path.join(tmpdir, "state"))
            state.ensure_dir()

            posted = []
            reporter = RpaProgressReporter(
                state=state, run_id="44444444-4444-4444-4444-444444444444", client_id="c-1",
                post_func=lambda run_id, frames: posted.append(frames) or True,
                flush_interval_seconds=0.0)

            _FakePopen.script = {"lines": ["step one", "step two"], "returncode": 0}
            aspects = RpaExecutorAspects(popen_func=_FakePopen)
            testbase.assert_true(aspects.uses_streaming)
            envelope = _sample_rpa_envelope(source_kind="local", source_ref=local_source)
            outcome = execute_rpa_envelope(
                state=state, envelope=envelope, local_run_id="local", aspects=aspects,
                progress_reporter=reporter)
            testbase.assert_true(outcome.succeeded)
            all_lines = [entry["line"] for frames in posted for frame in frames for entry in frame.get("lines", [])]
            testbase.assert_equal(all_lines, ["step one", "step two"])
            phases = [frame["phase"] for frames in posted for frame in frames]
            testbase.assert_true("started" in phases and "running" in phases and "succeeded" in phases)
            testbase.assert_true(os.path.isfile(os.path.join(
                state.state_dir, "rpa", envelope["run_id"], "results", "stdout.log")))

            flag = {"cancel": True}
            _FakePopen.script = {"lines": [], "returncode": 0, "duration": 30.0}
            aspects = RpaExecutorAspects(
                popen_func=_FakePopen, cancel_grace_seconds=0.1, cancel_poll_seconds=0.1,
                cancel_check_func=lambda: flag["cancel"])
            outcome = execute_rpa_envelope(
                state=state, envelope=envelope, local_run_id="local-2", aspects=aspects)
            testbase.assert_equal(outcome.status, RPA_STATUS_CANCELLED)
            testbase.assert_true(outcome.cancelled)
    finally:
        if original_killpg is not None:
            _os.killpg = original_killpg
    return


def test_progress_reporter_batches_lines_and_lifts_progress_marker():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()
        posted = []
        clock = {"now": 100.0}
        reporter = RpaProgressReporter(
            state=state, run_id="run-1", client_id="c-1",
            post_func=lambda run_id, frames: posted.append((run_id, frames)) or True,
            flush_interval_seconds=60.0, max_lines_per_frame=3,
            time_func=lambda: clock["now"])
        # started transition posted immediately
        testbase.assert_equal(len(posted), 1)
        testbase.assert_equal(posted[0][1][0]["phase"], "started")
        testbase.assert_equal(posted[0][1][0]["format_tag"], "rpa.process.v1")

        reporter.report_running()
        reporter.report_output_line("a")
        reporter.report_output_line("PYC-PROGRESS {\"percent\": 25, \"message\": \"quarter\"}")
        testbase.assert_equal(len(posted), 2)  # not flushed yet (interval not elapsed, < 3 lines)
        reporter.report_output_line("c")       # third line -> flush
        testbase.assert_equal(len(posted), 3)
        frame = posted[-1][1][0]
        testbase.assert_equal(len(frame["lines"]), 3)
        testbase.assert_equal(frame["percent_complete"], 25.0)
        testbase.assert_equal(frame["notes"], "quarter")
        testbase.assert_equal(frame["frame_index"], 2)

        reporter.report_output_line("d")
        clock["now"] = 200.0
        reporter.report_output_line("e")       # interval elapsed -> flush
        testbase.assert_equal(len(posted), 4)
        reporter.report_finished(succeeded=False)
        testbase.assert_equal(posted[-1][1][0]["phase"], "failed")
        testbase.assert_equal(reporter.frames_posted, len(posted))
        testbase.assert_equal(reporter.frames_failed, 0)

        record_path = os.path.join(state.state_dir, "rpa", "run-1", "rpa-record.json")
        testbase.assert_true(os.path.isfile(record_path))
    return


def test_progress_reporter_tolerates_post_failures():
    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        def _boom(run_id, frames):
            raise RuntimeError("network down")

        reporter = RpaProgressReporter(state=state, run_id="run-2", client_id="c-1", post_func=_boom)
        reporter.report_running()
        reporter.report_output_line("x")
        reporter.report_finished(succeeded=True)
        testbase.assert_true(reporter.frames_failed >= 3)
        testbase.assert_equal(reporter.frames_posted, 0)
    return

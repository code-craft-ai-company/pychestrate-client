"""
    TestBase tests for the pychestrate-client build executor. These tests
    mock the subprocess layer (both ``make`` and ``git``); no real build,
    git clone, or ``make`` invocation is performed.
"""

import os
import tempfile

from automojo import testbase

from automojo.errors.exceptions import BadParameterError

from automojo.pychestrate.client.state import ClientState
from automojo.pychestrate.client.build_executor import (
    BUILD_STATUS_FAILED,
    BUILD_STATUS_SUCCEEDED,
    BuildExecutorAspects,
    acquire_source,
    build_make_command,
    execute_build_envelope,
    resolve_build_dirs,
)


def _sample_build_envelope():
    """
        Returns a representative verified ``job_kind=build`` envelope dict.
    """
    envelope = {
        "envelope_id": "11111111-1111-1111-1111-111111111111",
        "run_id": "44444444-4444-4444-4444-444444444444",
        "client_id": "55555555-5555-5555-5555-555555555555",
        "job_id": "33333333-3333-3333-3333-333333333333",
        "job_name": "build:makefile",
        "job_type": "build-makefile",
        "job_kind": "build",
        "refs": {"branch": "main", "build": "1234", "flavor": "release"},
        "params": {
            "runner_kind": "makefile",
            "runner_inputs": {
                "targets": ["all", "dist"],
                "git_url": "https://example.com/repo.git",
                "git_ref": "v1.2.3",
            },
            "repro_digest_sha256": "abc123",
            "input_files": {},
            "env": {"BUILD_PROFILE": "release"},
            "toolchain": [],
        },
        "results": {"backend_key_prefix": "p", "backend_driver": "s3"},
    }
    return envelope


class _CompletedStub:
    """
        Minimal ``CompletedProcess`` stand-in.
    """

    def __init__(self, returncode, stdout=b"", stderr=b""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        return


class _RunRecorder:
    """
        Records every ``run_func`` call and returns scripted results keyed
        by the first argv element (``git`` vs ``make``).
    """

    def __init__(self, *, git_returncode=0, make_returncode=0,
                 raise_for=None):
        self._git_returncode = git_returncode
        self._make_returncode = make_returncode
        self._raise_for = raise_for
        self.calls = []
        return

    def __call__(self, command, **kwargs):
        self.calls.append({"command": list(command), "kwargs": dict(kwargs)})

        binary = os.path.basename(str(command[0]))
        if self._raise_for is not None and binary == self._raise_for:
            raise FileNotFoundError(
                "{} not found on this pod".format(binary))

        if binary == "git":
            rtnval = _CompletedStub(returncode=self._git_returncode)
            return rtnval

        rtnval = _CompletedStub(
            returncode=self._make_returncode, stdout=b"build log")
        return rtnval


def test_build_make_command_translates_makefile_envelope():
    envelope = _sample_build_envelope()
    envelope["params"]["runner_inputs"]["parallelism"] = 4

    command = build_make_command(envelope=envelope, make_binary="make")

    testbase.assert_equal(command[0], "make")
    testbase.assert_true("-j" in command)
    testbase.assert_true("4" in command)
    testbase.assert_true("all" in command)
    testbase.assert_true("dist" in command)
    return


def test_build_make_command_honors_custom_makefile_path():
    envelope = _sample_build_envelope()
    envelope["params"]["runner_inputs"]["makefile_path"] = "build/Build.mk"

    command = build_make_command(envelope=envelope, make_binary="make")

    testbase.assert_true("-f" in command)
    testbase.assert_true("build/Build.mk" in command)
    return


def test_build_make_command_rejects_unsupported_runner_kind():
    envelope = _sample_build_envelope()
    envelope["params"]["runner_kind"] = "docker"

    raised = False
    try:
        build_make_command(envelope=envelope, make_binary="make")
    except BadParameterError:
        raised = True
    testbase.assert_true(raised)
    return


def test_build_make_command_rejects_missing_targets():
    envelope = _sample_build_envelope()
    del envelope["params"]["runner_inputs"]["targets"]

    raised = False
    try:
        build_make_command(envelope=envelope, make_binary="make")
    except BadParameterError:
        raised = True
    testbase.assert_true(raised)
    return


def test_acquire_source_falls_back_to_runner_inputs_git_url():
    recorder = _RunRecorder()

    with tempfile.TemporaryDirectory() as tmpdir:
        source_dir = os.path.join(tmpdir, "source")
        os.makedirs(source_dir)

        aspects = BuildExecutorAspects(run_func=recorder)
        # A pre-#241 envelope carries no structured 'source' block.
        envelope = _sample_build_envelope()
        build_spec = envelope["params"]

        result = acquire_source(
            envelope=envelope, build_spec=build_spec, source_dir=source_dir,
            aspects=aspects)

        testbase.assert_true(result.acquired)
        # One clone + one checkout call.
        testbase.assert_equal(len(recorder.calls), 2)
        clone_call = recorder.calls[0]["command"]
        testbase.assert_equal(clone_call[1], "clone")
        testbase.assert_true(
            "https://example.com/repo.git" in clone_call)
        checkout_call = recorder.calls[1]["command"]
        testbase.assert_equal(checkout_call[1], "checkout")
        testbase.assert_true("v1.2.3" in checkout_call)
    return


def test_acquire_source_prefers_structured_source_block():
    recorder = _RunRecorder()

    with tempfile.TemporaryDirectory() as tmpdir:
        source_dir = os.path.join(tmpdir, "source")
        os.makedirs(source_dir)

        aspects = BuildExecutorAspects(run_func=recorder)
        envelope = _sample_build_envelope()
        # The structured 'source' block must win over runner_inputs.git_url.
        envelope["source"] = {
            "kind": "git",
            "url": "https://example.com/structured-repo.git",
            "ref": "release-9.9",
            "integration_id": "66666666-6666-6666-6666-666666666666",
            "credentials_ref": "",
        }
        build_spec = envelope["params"]

        result = acquire_source(
            envelope=envelope, build_spec=build_spec, source_dir=source_dir,
            aspects=aspects)

        testbase.assert_true(result.acquired)
        testbase.assert_equal(len(recorder.calls), 2)
        clone_call = recorder.calls[0]["command"]
        testbase.assert_equal(clone_call[1], "clone")
        # The structured source url is cloned, NOT the legacy git_url.
        testbase.assert_true(
            "https://example.com/structured-repo.git" in clone_call)
        testbase.assert_false(
            "https://example.com/repo.git" in clone_call)
        checkout_call = recorder.calls[1]["command"]
        testbase.assert_equal(checkout_call[1], "checkout")
        testbase.assert_true("release-9.9" in checkout_call)
    return


def test_acquire_source_structured_block_without_ref_skips_checkout():
    recorder = _RunRecorder()

    with tempfile.TemporaryDirectory() as tmpdir:
        source_dir = os.path.join(tmpdir, "source")
        os.makedirs(source_dir)

        aspects = BuildExecutorAspects(run_func=recorder)
        envelope = _sample_build_envelope()
        envelope["source"] = {
            "kind": "git",
            "url": "https://example.com/structured-repo.git",
            "ref": "",
            "integration_id": "",
            "credentials_ref": "",
        }
        build_spec = envelope["params"]

        result = acquire_source(
            envelope=envelope, build_spec=build_spec, source_dir=source_dir,
            aspects=aspects)

        testbase.assert_true(result.acquired)
        # Only a clone -- no ref means no checkout.
        testbase.assert_equal(len(recorder.calls), 1)
        testbase.assert_equal(recorder.calls[0]["command"][1], "clone")
    return


def test_acquire_source_structured_block_private_repo_not_supported():
    recorder = _RunRecorder()

    with tempfile.TemporaryDirectory() as tmpdir:
        source_dir = os.path.join(tmpdir, "source")
        os.makedirs(source_dir)

        aspects = BuildExecutorAspects(run_func=recorder)
        envelope = _sample_build_envelope()
        # A non-empty credentials_ref references a private repository.
        envelope["source"] = {
            "kind": "git",
            "url": "https://example.com/private-repo.git",
            "ref": "main",
            "integration_id": "",
            "credentials_ref": "GIT_CLONE_TOKEN",
        }
        build_spec = envelope["params"]

        result = acquire_source(
            envelope=envelope, build_spec=build_spec, source_dir=source_dir,
            aspects=aspects)

        # Private-repo credentials are not yet supported -- non-acquired.
        testbase.assert_false(result.acquired)
        # No clone attempted for an unsupported private repo.
        testbase.assert_equal(len(recorder.calls), 0)
        testbase.assert_true(
            "credentials_ref" in str(result.detail))
    return


def test_acquire_source_structured_block_rejects_non_git_kind():
    recorder = _RunRecorder()

    with tempfile.TemporaryDirectory() as tmpdir:
        source_dir = os.path.join(tmpdir, "source")
        os.makedirs(source_dir)

        aspects = BuildExecutorAspects(run_func=recorder)
        envelope = _sample_build_envelope()
        envelope["source"] = {
            "kind": "mercurial",
            "url": "https://example.com/repo",
            "ref": "tip",
            "integration_id": "",
            "credentials_ref": "",
        }
        build_spec = envelope["params"]

        result = acquire_source(
            envelope=envelope, build_spec=build_spec, source_dir=source_dir,
            aspects=aspects)

        testbase.assert_false(result.acquired)
        testbase.assert_equal(len(recorder.calls), 0)
    return


def test_acquire_source_uses_present_local_source_dir():
    recorder = _RunRecorder()

    with tempfile.TemporaryDirectory() as tmpdir:
        local_source = os.path.join(tmpdir, "checkout")
        os.makedirs(local_source)
        work_dir = os.path.join(tmpdir, "work")
        os.makedirs(work_dir)

        aspects = BuildExecutorAspects(run_func=recorder)
        build_spec = {
            "runner_kind": "makefile",
            "runner_inputs": {
                "targets": ["all"],
                "source_dir": local_source,
            },
        }
        envelope = {"params": build_spec}

        result = acquire_source(
            envelope=envelope, build_spec=build_spec, source_dir=work_dir,
            aspects=aspects)

        testbase.assert_true(result.acquired)
        testbase.assert_equal(result.source_dir, local_source)
        # No git invocation for an already-present local source tree.
        testbase.assert_equal(len(recorder.calls), 0)
    return


def test_acquire_source_fails_when_clone_fails():
    recorder = _RunRecorder(git_returncode=128)

    with tempfile.TemporaryDirectory() as tmpdir:
        source_dir = os.path.join(tmpdir, "source")
        os.makedirs(source_dir)

        aspects = BuildExecutorAspects(run_func=recorder)
        envelope = _sample_build_envelope()
        build_spec = envelope["params"]

        result = acquire_source(
            envelope=envelope, build_spec=build_spec, source_dir=source_dir,
            aspects=aspects)

        testbase.assert_false(result.acquired)
    return


def test_execute_build_envelope_runs_make_with_env_and_workdir():
    recorder = _RunRecorder()

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        aspects = BuildExecutorAspects(run_func=recorder)
        envelope = _sample_build_envelope()

        outcome = execute_build_envelope(
            state=state, envelope=envelope, local_run_id="local-build-1",
            aspects=aspects)

        testbase.assert_true(outcome.succeeded)
        testbase.assert_equal(outcome.status, BUILD_STATUS_SUCCEEDED)
        testbase.assert_equal(outcome.exit_code, 0)

        # The last call is the make invocation.
        make_call = recorder.calls[-1]
        testbase.assert_equal(
            os.path.basename(make_call["command"][0]), "make")
        # make runs inside the acquired source directory.
        build_dirs = resolve_build_dirs(state=state, run_id=envelope["run_id"])
        testbase.assert_equal(
            make_call["kwargs"]["cwd"], build_dirs["source_dir"])
        # The build env overlay is applied.
        run_env = make_call["kwargs"]["env"]
        testbase.assert_equal(run_env.get("BUILD_PROFILE"), "release")

        # A local build record was written.
        record_path = os.path.join(
            build_dirs["record_dir"], "build-record.json")
        testbase.assert_true(os.path.isfile(record_path))
    return


def test_execute_build_envelope_uses_structured_source_block():
    recorder = _RunRecorder()

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        aspects = BuildExecutorAspects(run_func=recorder)
        envelope = _sample_build_envelope()
        envelope["source"] = {
            "kind": "git",
            "url": "https://example.com/structured-repo.git",
            "ref": "v2.0.0",
            "integration_id": "66666666-6666-6666-6666-666666666666",
            "credentials_ref": "",
        }

        outcome = execute_build_envelope(
            state=state, envelope=envelope, local_run_id="local-build-src",
            aspects=aspects)

        testbase.assert_true(outcome.succeeded)
        clone_calls = [
            call for call in recorder.calls
            if os.path.basename(call["command"][0]) == "git"
            and call["command"][1] == "clone"
        ]
        testbase.assert_equal(len(clone_calls), 1)
        testbase.assert_true(
            "https://example.com/structured-repo.git"
            in clone_calls[0]["command"])
    return


def test_execute_build_envelope_fails_on_private_repo_source():
    recorder = _RunRecorder()

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        aspects = BuildExecutorAspects(run_func=recorder)
        envelope = _sample_build_envelope()
        envelope["source"] = {
            "kind": "git",
            "url": "https://example.com/private-repo.git",
            "ref": "main",
            "integration_id": "",
            "credentials_ref": "GIT_CLONE_TOKEN",
        }

        # A private-repo source is a failed build, not a crash.
        outcome = execute_build_envelope(
            state=state, envelope=envelope, local_run_id="local-build-priv",
            aspects=aspects)

        testbase.assert_false(outcome.succeeded)
        testbase.assert_equal(outcome.status, BUILD_STATUS_FAILED)
        make_calls = [
            call for call in recorder.calls
            if os.path.basename(call["command"][0]) == "make"
        ]
        testbase.assert_equal(len(make_calls), 0)
    return


def test_execute_build_envelope_marks_failed_on_nonzero_exit():
    recorder = _RunRecorder(make_returncode=2)

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        aspects = BuildExecutorAspects(run_func=recorder)
        envelope = _sample_build_envelope()

        outcome = execute_build_envelope(
            state=state, envelope=envelope, local_run_id="local-build-2",
            aspects=aspects)

        testbase.assert_false(outcome.succeeded)
        testbase.assert_equal(outcome.status, BUILD_STATUS_FAILED)
        testbase.assert_equal(outcome.exit_code, 2)
    return


def test_execute_build_envelope_survives_missing_make_binary():
    recorder = _RunRecorder(raise_for="make")

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        aspects = BuildExecutorAspects(run_func=recorder)
        envelope = _sample_build_envelope()

        # Must NOT raise -- a missing make binary is a failed build.
        outcome = execute_build_envelope(
            state=state, envelope=envelope, local_run_id="local-build-3",
            aspects=aspects)

        testbase.assert_false(outcome.succeeded)
        testbase.assert_equal(outcome.status, BUILD_STATUS_FAILED)
        testbase.assert_true(outcome.exit_code is None)
    return


def test_execute_build_envelope_survives_clone_failure():
    recorder = _RunRecorder(git_returncode=128)

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        aspects = BuildExecutorAspects(run_func=recorder)
        envelope = _sample_build_envelope()

        # A clone failure is a failed build, not a crash.
        outcome = execute_build_envelope(
            state=state, envelope=envelope, local_run_id="local-build-4",
            aspects=aspects)

        testbase.assert_false(outcome.succeeded)
        testbase.assert_equal(outcome.status, BUILD_STATUS_FAILED)
        # make was never invoked because source acquisition failed.
        make_calls = [
            call for call in recorder.calls
            if os.path.basename(call["command"][0]) == "make"
        ]
        testbase.assert_equal(len(make_calls), 0)
    return


def test_execute_build_envelope_survives_unbuildable_envelope():
    recorder = _RunRecorder()

    with tempfile.TemporaryDirectory() as tmpdir:
        state = ClientState(state_dir=os.path.join(tmpdir, "state"))
        state.ensure_dir()

        aspects = BuildExecutorAspects(run_func=recorder)
        envelope = _sample_build_envelope()
        del envelope["params"]["runner_inputs"]["targets"]

        # An unbuildable envelope is a failed build, not a crash.
        outcome = execute_build_envelope(
            state=state, envelope=envelope, local_run_id="local-build-5",
            aspects=aspects)

        testbase.assert_false(outcome.succeeded)
        testbase.assert_equal(outcome.status, BUILD_STATUS_FAILED)
        testbase.assert_equal(len(recorder.calls), 0)
    return

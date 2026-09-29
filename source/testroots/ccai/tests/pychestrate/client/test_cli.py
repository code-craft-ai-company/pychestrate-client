"""
    TestBase tests for the pychestrate-client command line: global options
    before or after the command, ``--version``, where the enrollment token
    comes from, the enroll flags, one-line failures, ``heartbeat --once`` and
    ``python -m``. No test contacts a real service; the one that meets a
    closed port uses a port nothing listens on.
"""

import contextlib
import io
import json
import logging
import os
import subprocess
import sys
import tempfile
import time

from typing import Any, Dict, List, Tuple

from ccai import testbase

from ccai.errors.exceptions import ConfigurationError, HTTPRequestError

from ccai.pychestrate.client import VERSION, cli


def _run(argv: List[str], *, stdin: str = "") -> Tuple[int, str, str]:
    """Run ``main(argv)``; returns ``(exit code, stdout, stderr)``."""
    out, err = io.StringIO(), io.StringIO()
    original_stdin = sys.stdin
    sys.stdin = io.StringIO(stdin)
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            try:
                code = cli.main(argv)
            except SystemExit as leaving:
                code = int(leaving.code) if leaving.code is not None else 0
    finally:
        sys.stdin = original_stdin
    return (code, out.getvalue(), err.getvalue())


class _Enroll:
    """Stands in for ``perform_enrollment`` and remembers how it was called."""

    def __init__(self, *, failure: Exception = None) -> None:
        self.calls: List[Dict[str, Any]] = []
        self._failure = failure
        self._original = None
        return

    def __enter__(self) -> "_Enroll":
        self._original = cli.perform_enrollment
        cli.perform_enrollment = self
        return self

    def __exit__(self, *exc_info: Any) -> None:
        cli.perform_enrollment = self._original
        return

    def __call__(self, **kwargs: Any) -> Dict[str, Any]:
        self.calls.append(kwargs)
        if self._failure is not None:
            raise self._failure
        return {"client_id": "abc", "config_path": "/x/config.json"}


BASE = ["enroll", "--server-url", "https://pychestrate.example.com", "--name", "pod-a"]


def test_global_options_work_before_and_after_the_command():
    parser = cli._build_parser()
    after = parser.parse_args(["run", "--state-dir", "/a", "--verbose", "--log-format", "json"])
    before = parser.parse_args(["--state-dir", "/b", "--quiet", "run"])
    mixed = parser.parse_args(["--state-dir", "/c", "--verbose", "doctor"])
    plain = parser.parse_args(["doctor"])

    testbase.assert_equal(
        ((after.state_dir, after.verbose, after.log_format), (before.state_dir, before.quiet),
         (mixed.state_dir, mixed.verbose), (plain.state_dir, plain.verbose, plain.quiet, plain.log_format)),
        (("/a", True, "json"), ("/b", True), ("/c", True), (None, False, False, "text")),
    )
    return


def test_version_is_a_flag_and_still_a_command():
    flag = _run(["--version"])
    command = _run(["version"])

    testbase.assert_equal(
        (flag[0], flag[1].strip(), command[0], command[1].strip()),
        (0, "pychestrate-client {}".format(VERSION), 0, "pychestrate-client {}".format(VERSION)),
    )
    return


def test_help_ends_with_the_recipe_for_bringing_a_pod_into_service():
    help_text = cli._build_parser().format_help()

    testbase.assert_equal(
        ("bringing a pod into service" in help_text, "--token-file token.txt" in help_text,
         "pychestrate-client doctor" in help_text, "before or after the" in help_text),
        (True, True, True, True),
    )
    return


def test_the_token_comes_from_a_file_and_is_trimmed():
    with tempfile.TemporaryDirectory() as tmpdir, _Enroll() as enroll:
        path = os.path.join(tmpdir, "token.txt")
        with open(path, "w", encoding="utf-8") as tokenf:
            tokenf.write("  secret-from-file\n")
        code, out, err = _run(BASE + ["--token-file", path, "--state-dir", tmpdir])

    testbase.assert_equal(
        (code, enroll.calls[0]["bootstrap_token"], "process list" in err), (0, "secret-from-file", False))
    return


def test_the_token_can_come_from_standard_input_or_the_environment():
    with _Enroll() as from_stdin:
        _run(BASE + ["--token-file", "-"], stdin="stdin-secret\n")
    os.environ[cli.ENV_ENROLLMENT_TOKEN] = "env-secret"
    try:
        with _Enroll() as from_env:
            _run(BASE)
    finally:
        del os.environ[cli.ENV_ENROLLMENT_TOKEN]

    testbase.assert_equal(
        (from_stdin.calls[0]["bootstrap_token"], from_env.calls[0]["bootstrap_token"]), ("stdin-secret", "env-secret"))
    return


def test_a_token_on_the_command_line_works_but_says_where_it_is_visible():
    with _Enroll() as enroll:
        code, out, err = _run(BASE + ["--token", "on-argv"])

    testbase.assert_equal(
        (code, enroll.calls[0]["bootstrap_token"], "process list" in err and "--token-file" in err), (0, "on-argv", True))
    return


def test_no_token_and_two_tokens_are_refused_with_one_line_and_nothing_is_called():
    os.environ.pop(cli.ENV_ENROLLMENT_TOKEN, None)
    with tempfile.TemporaryDirectory() as tmpdir, _Enroll() as enroll:
        path = os.path.join(tmpdir, "t")
        with open(path, "w", encoding="utf-8") as tokenf:
            tokenf.write("x")
        none = _run(BASE)
        both = _run(BASE + ["--token", "a", "--token-file", path])
        empty_file = os.path.join(tmpdir, "empty")
        open(empty_file, "w").close()
        empty = _run(BASE + ["--token-file", empty_file])
        missing = _run(BASE + ["--token-file", os.path.join(tmpdir, "nope")])

    testbase.assert_equal(
        ([result[0] for result in (none, both, empty, missing)], enroll.calls,
         "an enrollment token is needed" in none[2], "not both" in both[2], "is empty" in empty[2],
         "cannot be read" in missing[2], [len(result[2].strip().splitlines()) for result in (none, both, empty, missing)]),
        ([2, 2, 2, 2], [], True, True, True, True, [1, 1, 1, 1]),
    )
    return


def test_force_and_timeout_reach_the_enrollment():
    with _Enroll() as plain:
        _run(BASE + ["--token", "t"])
    with _Enroll() as forced:
        _run(BASE + ["--token", "t", "--force", "--timeout", "5"])

    testbase.assert_equal(
        ((plain.calls[0]["overwrite"], plain.calls[0]["timeout_seconds"]),
         (forced.calls[0]["overwrite"], forced.calls[0]["timeout_seconds"])),
        ((False, 30.0), (True, 5.0)),
    )
    return


def test_json_prints_only_the_result_on_standard_output():
    with _Enroll():
        with_prose = _run(BASE + ["--token", "t"])
        as_json = _run(BASE + ["--token", "t", "--json"])

    parsed = json.loads(as_json[1])
    testbase.assert_equal(
        (with_prose[1].startswith("pychestrate-client: enrollment succeeded."), parsed["client_id"],
         as_json[1].lstrip().startswith("{")),
        (True, "abc", True),
    )
    return


def test_a_failed_enrollment_is_one_line_on_standard_error_and_nothing_on_standard_output():
    with _Enroll(failure=HTTPRequestError("could not connect to x:1 (Connection refused)")):
        code, out, err = _run(BASE + ["--token", "t"])
    with _Enroll(failure=ConfigurationError("this state directory is already enrolled as client 1; use --force")):
        refused = _run(BASE + ["--token", "t"])

    testbase.assert_equal(
        (code, out, err.strip().splitlines()[-1], "Traceback" in err, refused[0], "already enrolled" in refused[2]),
        (2, "", "pychestrate-client: enrollment failed: could not connect to x:1 (Connection refused)", False, 2, True),
    )
    return


def test_a_closed_port_is_one_quick_line_not_a_traceback():
    started = time.monotonic()
    with tempfile.TemporaryDirectory() as tmpdir:
        code, out, err = _run(["enroll", "--service-url", "http://127.0.0.1:1", "--name", "pod-a", "--token", "t",
                               "--state-dir", tmpdir])
    took = time.monotonic() - started
    failure = [line for line in err.splitlines() if "enrollment failed" in line]

    testbase.assert_equal(
        (code, len(failure), "could not connect to 127.0.0.1:1" in failure[0], "Traceback" in err, took < 10),
        (2, 1, True, False, True),
    )
    return


def test_verbose_and_quiet_together_are_refused():
    code, _out, err = _run(["--verbose", "--quiet", "version"])

    testbase.assert_equal((code, "cannot be used together" in err), (2, True))
    return


def test_heartbeat_once_sends_one_heartbeat_and_reports_the_result():
    original = (cli.build_client_from_state, cli.send_one_heartbeat)
    results: List[Tuple[int, str]] = []
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)
            with open(os.path.join(tmpdir, "client_id"), "w", encoding="utf-8") as idfile:
                idfile.write("cid-1")
            cli.build_client_from_state = lambda state: object()
            for status in (204, 500):
                cli.send_one_heartbeat = lambda client, client_id, status=status: status
                code, out, _err = _run(["heartbeat", "--once", "--state-dir", tmpdir])
                results.append((code, out.strip()))

            def not_enrolled(state):
                raise ConfigurationError("not fully enrolled")

            cli.build_client_from_state = not_enrolled
            code, _out, err = _run(["heartbeat", "--once", "--state-dir", tmpdir])
            results.append((code, "not fully enrolled" in err))
    finally:
        cli.build_client_from_state, cli.send_one_heartbeat = original

    testbase.assert_equal(
        results,
        [(0, "pychestrate-client: heartbeat sent (HTTP 204)."), (1, "pychestrate-client: heartbeat sent (HTTP 500)."), (2, True)],
    )
    return


def test_python_dash_m_runs_the_same_command_line():
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join(path for path in sys.path if path)
    finished = subprocess.run(
        [sys.executable, "-m", "ccai.pychestrate.client", "--version"], capture_output=True, text=True, env=environment)

    testbase.assert_equal(
        (finished.returncode, finished.stdout.strip()), (0, "pychestrate-client {}".format(VERSION)))
    return


def test_the_json_log_format_writes_one_object_per_line():
    record = logging.LogRecord("ccai.demo", logging.WARNING, __file__, 1, "heartbeat failed: %s", ("boom",), None)
    entry = json.loads(cli._JsonLogFormatter().format(record))

    testbase.assert_equal(
        (entry["level"], entry["logger"], entry["message"], sorted(entry)),
        ("WARNING", "ccai.demo", "heartbeat failed: boom", ["level", "logger", "message", "time"]),
    )
    return


def test_enroll_takes_server_url_like_pyc_and_still_takes_the_older_service_url():
    """pyc calls the option --server-url; the client's --service-url keeps working as the same option."""
    with _Enroll() as new_spelling:
        new_code, _out, _err = _run(["enroll", "--server-url", "https://a.example.com", "--name", "pod-a", "--token", "t"])
    with _Enroll() as old_spelling:
        old_code, _out, _err = _run(["enroll", "--service-url", "https://b.example.com", "--name", "pod-a", "--token", "t"])

    testbase.assert_equal(
        (new_code, new_spelling.calls[0]["service_url"], old_code, old_spelling.calls[0]["service_url"]),
        (0, "https://a.example.com", 0, "https://b.example.com"),
    )
    return


def test_enroll_help_names_the_shared_spelling_first_and_the_state_default():
    code, out, _err = _run(["enroll", "--help"])
    top_code, top_out, _top_err = _run(["--help"])

    testbase.assert_equal(
        (code, "--server-url URL, --service-url URL" in out, "pyc login --server-url" in out,
         top_code, "--server-url" in top_out and "--service-url" not in top_out, ".local/share/pychestrate-client" in out),
        (0, True, True, 0, True, True),
    )
    return

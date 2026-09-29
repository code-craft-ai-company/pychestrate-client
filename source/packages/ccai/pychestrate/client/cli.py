"""
    .. module:: cli
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Argparse-based entry point for the ``pychestrate-client``
                   binary. Exposes the M1 subcommands (``enroll``,
                   ``heartbeat``, ``version``) plus the M2 subcommands
                   (``install-signing-key``, ``run``).

                   The client fetches the envelope signing keys from the
                   service automatically: the RFC 7517 JWK Set from the
                   service's JWKS endpoint is the first-class source, backed
                   by the set bundled in the enrollment-redeem response.
                   ``install-signing-key`` is therefore an operator override
                   for air-gapped or pinned pods, not the happy path.

                   The ``heartbeat`` subcommand is retained for operators
                   who want a liveness-only process; the ``run`` subcommand
                   is the normal pod-side mode — it runs the heartbeat loop
                   and the dispatch-consumer loop concurrently on two
                   threads. As of M3 the dispatch-consumer loop closes the
                   orchestration loop on the client side: each dispatched job
                   runs with live progress streaming and pushes its result
                   tree + completion manifest back to the service.

                   As of M5 the dispatch-consumer loop branches on the
                   envelope's ``job_kind``: a ``test`` job runs the testbase
                   executor, a ``build`` job runs the build executor (source
                   acquisition -> makefile runner -> artifact upload). Builds
                   run through the same dispatch loop as tests — no new
                   subcommand is needed; ``run`` exercises the full loop for
                   both job kinds.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2025, Automation Mojo LLC"
__credits__ = []


from typing import List, Optional

import argparse
import json
import logging
import os
import sys
import threading

from ccai.errors.exceptions import BadParameterError, ConfigurationError, HTTPRequestError

from ccai.pychestrate.client import VERSION
from ccai.pychestrate.client.doctor import LEVEL_FAIL, LEVEL_WARN, exit_code_for, run_checks
from ccai.pychestrate.client.dispatch_consumer import (
    DEFAULT_DISPATCH_POLL_TIMEOUT_SECONDS,
    DispatchConsumerAspects,
    run_dispatch_consumer_loop,
)
from ccai.pychestrate.client.enrollment import DEFAULT_ENROLLMENT_TIMEOUT_SECONDS, perform_enrollment
from ccai.pychestrate.client.heartbeat import (
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    HeartbeatAspects,
    build_client_from_state,
    run_heartbeat_loop,
    send_one_heartbeat,
)
from ccai.pychestrate.client.shutdown import ShutdownController
from ccai.pychestrate.client.state import DEFAULT_STATE_DIR_HELP, ClientState


#: How long ``run``'s main thread blocks in each join before checking
#: again, so it keeps servicing SIGINT/SIGTERM.
RUN_JOIN_SLICE_SECONDS = 0.5


#: The environment variable an enrollment token may be given in, so it stays
#: out of the process list and the shell history.
ENV_ENROLLMENT_TOKEN = "PYCHESTRATE_ENROLLMENT_TOKEN"

#: What ``--help`` ends with: the whole path from a new machine to a working
#: pod, with the options that keep the token off the command line.
HELP_EPILOG = """\
bringing a pod into service:

  1. In the service, choose Admin, Enrollment Tokens and mint a token for the
     pod (the label you give it there is the --name below).
  2. Save the token to a file, then on this machine:
       pychestrate-client enroll --server-url https://SERVICE --name POD-LABEL --token-file token.txt
  3. pychestrate-client doctor      check the enrollment and that the service answers
  4. pychestrate-client run         start taking jobs

--state-dir, --verbose, --quiet and --log-format may come before or after the
command. Run "pychestrate-client COMMAND --help" for a command's options.
"""


class _UsageError(Exception):
    """The command line asked for something that cannot be done (the message says what to change)."""


class _JsonLogFormatter(logging.Formatter):
    """One JSON object per log line, for log shippers."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "time": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        rtnval = json.dumps(entry, sort_keys=True)
        return rtnval


def _configure_logging(verbose: bool, quiet: bool = False, log_format: str = "text") -> None:
    """
        Configures the root logger for CLI usage.

        :param verbose: When True, sets DEBUG level.
        :param quiet: When True (and not verbose), only warnings and errors.
        :param log_format: ``text`` (the default) or ``json``.

        :returns: None
    """
    level = logging.DEBUG if verbose else (logging.WARNING if quiet else logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )
    if log_format == "json":
        for handler in logging.getLogger().handlers:
            handler.setFormatter(_JsonLogFormatter())
    return


def _read_token(args: argparse.Namespace) -> str:
    """
        Finds the enrollment token: ``--token``, ``--token-file`` (``-`` reads
        standard input) or the ``PYCHESTRATE_ENROLLMENT_TOKEN`` variable, in
        that order of asking; giving two of the first two is refused.

        :param args: Parsed CLI arguments.

        :returns: The token.
        :raises: _UsageError
    """
    if args.token is not None and args.token_file is not None:
        raise _UsageError("give the token once: use either --token or --token-file, not both")
    if args.token is not None:
        print("pychestrate-client: note: a token given with --token is visible in the process list and your "
              "shell history; --token-file or {} keeps it out.".format(ENV_ENROLLMENT_TOKEN), file=sys.stderr)
        token = args.token
    elif args.token_file is not None:
        try:
            if args.token_file == "-":
                token = sys.stdin.read()
            else:
                with open(args.token_file, "r", encoding="utf-8") as tokenf:
                    token = tokenf.read()
        except OSError as readerr:
            raise _UsageError("the token file {} cannot be read: {}".format(args.token_file, readerr.strerror or readerr))
    else:
        token = os.environ.get(ENV_ENROLLMENT_TOKEN, "")
        if len(token.strip()) == 0:
            raise _UsageError(
                "an enrollment token is needed: give --token-file PATH (or - for standard input), set {}, "
                "or use --token".format(ENV_ENROLLMENT_TOKEN))
    token = token.strip()
    if len(token) == 0:
        raise _UsageError("the enrollment token is empty")
    return token


def _cmd_enroll(args: argparse.Namespace) -> int:
    """
        Implements the ``enroll`` subcommand.

        :param args: Parsed CLI arguments.

        :returns: Process exit code (0 on success).
    """
    state = ClientState(state_dir=args.state_dir)

    try:
        token = _read_token(args)
        result = perform_enrollment(
            service_url=args.service_url,
            bootstrap_token=token,
            client_name=args.name,
            state=state,
            attributes=None,
            verify_tls=not args.insecure,
            timeout_seconds=args.timeout,
            overwrite=args.force,
        )
    except (_UsageError, BadParameterError, ConfigurationError, HTTPRequestError, OSError) as enrollerr:
        print("pychestrate-client: enrollment failed: {}".format(enrollerr), file=sys.stderr)
        rtnval = 2
        return rtnval

    if not args.json:
        print("pychestrate-client: enrollment succeeded.")
    print(json.dumps(result, indent=4, sort_keys=True))

    rtnval = 0
    return rtnval


def _cmd_heartbeat(args: argparse.Namespace) -> int:
    """
        Implements the ``heartbeat`` subcommand.

        :param args: Parsed CLI arguments.

        :returns: Process exit code (0 on graceful stop).
    """
    state = ClientState(state_dir=args.state_dir)

    if args.once:
        try:
            client = build_client_from_state(state=state)
            status_code = send_one_heartbeat(client=client, client_id=state.read_client_id())
        except (BadParameterError, ConfigurationError) as hberr:
            print("pychestrate-client: heartbeat failed: {}".format(hberr), file=sys.stderr)
            rtnval = 2
            return rtnval
        except Exception as senderr:  # noqa: BLE001 -- a network failure is a result, not a traceback
            print("pychestrate-client: heartbeat failed: {}".format(senderr), file=sys.stderr)
            rtnval = 1
            return rtnval
        print("pychestrate-client: heartbeat sent (HTTP {}).".format(status_code))
        rtnval = 0 if 200 <= status_code < 300 else 1
        return rtnval

    try:
        aspects = HeartbeatAspects(
            interval_seconds=args.interval,
        )
        iterations = run_heartbeat_loop(state=state, aspects=aspects)
    except (BadParameterError, ConfigurationError) as hberr:
        print("pychestrate-client: heartbeat failed: {}".format(hberr), file=sys.stderr)
        rtnval = 2
        return rtnval

    print("pychestrate-client: heartbeat loop exited after {} iteration(s).".format(iterations))

    rtnval = 0
    return rtnval


def _cmd_install_signing_key(args: argparse.Namespace) -> int:
    """
        Implements the ``install-signing-key`` subcommand.

        Installs the service's Ed25519 job-envelope signing public key
        into the client state directory and records its key id in the
        config.

        NOTE: this subcommand is not the happy path. The client normally
        fetches the service's RFC 7517 JWK Set from its JWKS endpoint and
        caches it locally. Installing a key here is an explicit operator
        OVERRIDE: while the installed key file is present it is the only
        trust anchor the client will use, the service JWK Set is not
        consulted, and envelopes signed by any other key id are refused.
        Use it for air-gapped pods or to pin a pod to one signing key;
        remove the installed key file to return to JWKS-based resolution.

        :param args: Parsed CLI arguments.

        :returns: Process exit code (0 on success).
    """
    state = ClientState(state_dir=args.state_dir)

    if not os.path.isfile(args.key_file):
        print(
            "pychestrate-client: signing key file not found: {}".format(
                args.key_file),
            file=sys.stderr,
        )
        rtnval = 2
        return rtnval

    try:
        with open(args.key_file, "r", encoding="utf-8") as keyf:
            key_pem = keyf.read()

        state.ensure_dir()
        state.write_envelope_signing_key(key_pem=key_pem)

        config = state.read_config()
        config["envelope_signing_key_id"] = args.key_id
        state.write_config(config=config)
    except (ConfigurationError, OSError) as installerr:
        print(
            "pychestrate-client: failed to install signing key: {}".format(
                installerr),
            file=sys.stderr,
        )
        rtnval = 2
        return rtnval

    print("pychestrate-client: envelope signing key installed.")
    print("    key_id: {}".format(args.key_id))
    print("    path: {}".format(state.envelope_signing_key_path))
    print("")
    print("This installed key is an OPERATOR OVERRIDE. While it is present "
          "the client")
    print("verifies job envelopes ONLY against it and does not consult the "
          "service")
    print("JWK Set. Envelopes signed by any other key id will be refused.")
    print("Remove {} to return to".format(state.envelope_signing_key_path))
    print("JWKS-based key resolution.")

    rtnval = 0
    return rtnval


def _cmd_run(args: argparse.Namespace) -> int:
    """
        Implements the ``run`` subcommand — the normal pod-side mode.

        Starts the heartbeat loop and the dispatch-consumer loop on two
        daemon threads. SIGINT/SIGTERM handlers are installed here, on the
        main thread (the only thread Python delivers signals to): the first
        signal stops the dispatch consumer once its current job is done and
        acked, then the heartbeat, and ``run`` exits 0; a second signal
        exits at once with code 130.

        :param args: Parsed CLI arguments.

        :returns: Process exit code (0 on graceful stop, 2 on a
                  configuration failure).
    """
    state = ClientState(state_dir=args.state_dir)

    try:
        heartbeat_aspects = HeartbeatAspects(interval_seconds=args.interval)
        consumer_aspects = DispatchConsumerAspects(
            poll_timeout_seconds=args.poll_timeout)
    except BadParameterError as cfgerr:
        print("pychestrate-client: run failed: {}".format(cfgerr), file=sys.stderr)
        rtnval = 2
        return rtnval

    thread_errors: List[str] = []

    # The heartbeat keeps running until the dispatch consumer is done, so
    # the service still sees the pod alive while a last job finishes.
    shutdown = ShutdownController()
    heartbeat_shutdown = ShutdownController()

    def _heartbeat_thread() -> None:
        try:
            run_heartbeat_loop(
                state=state, aspects=heartbeat_aspects, shutdown=heartbeat_shutdown)
        except (BadParameterError, ConfigurationError) as hberr:
            thread_errors.append("heartbeat loop: {}".format(hberr))
        return

    def _dispatch_thread() -> None:
        try:
            run_dispatch_consumer_loop(
                state=state, aspects=consumer_aspects, shutdown=shutdown)
        except (BadParameterError, ConfigurationError) as dcerr:
            thread_errors.append("dispatch consumer: {}".format(dcerr))
        return

    hb_thread = threading.Thread(
        target=_heartbeat_thread, name="pychestrate-heartbeat", daemon=True)
    dc_thread = threading.Thread(
        target=_dispatch_thread, name="pychestrate-dispatch", daemon=True)

    restore_handlers = shutdown.install_signal_handlers()
    try:
        print("pychestrate-client: starting heartbeat + dispatch-consumer loops.")
        hb_thread.start()
        dc_thread.start()

        # Join in short slices: a blocking join would keep the main thread
        # from running the signal handlers promptly.
        announced_wait = False
        while dc_thread.is_alive():
            dc_thread.join(timeout=RUN_JOIN_SLICE_SECONDS)
            if shutdown.is_stop_requested() and dc_thread.is_alive() and not announced_wait:
                announced_wait = True
                print("pychestrate-client: stopping; waiting for the current dispatch "
                      "poll (up to {} s) or job to finish. Send the signal again to "
                      "exit now.".format(consumer_aspects.poll_timeout_seconds))

        heartbeat_shutdown.request_stop(reason="the dispatch consumer stopped")
        while hb_thread.is_alive():
            hb_thread.join(timeout=RUN_JOIN_SLICE_SECONDS)
    finally:
        restore_handlers()

    if len(thread_errors) > 0:
        for nxterr in thread_errors:
            print("pychestrate-client: {}".format(nxterr), file=sys.stderr)
        rtnval = 2
        return rtnval

    print("pychestrate-client: run loops exited.")
    rtnval = 0
    return rtnval


def _cmd_doctor(args: argparse.Namespace) -> int:
    """
        Implements the ``doctor`` subcommand: checks the state directory and
        (unless ``--offline``) that the service answers.

        :param args: Parsed CLI arguments.

        :returns: 0 when nothing failed (warnings are allowed), 1 otherwise.
    """
    state = ClientState(state_dir=args.state_dir)
    checks = run_checks(state=state, check_service=not args.offline)
    labels = {"ok": "ok  ", "warn": "WARN", "fail": "FAIL"}
    for check in checks:
        print("{} {}".format(labels.get(check.level, check.level), check.message))
    failed = len([check for check in checks if check.level == LEVEL_FAIL])
    warned = len([check for check in checks if check.level == LEVEL_WARN])
    if failed > 0:
        print("pychestrate-client: {} problem(s) found.".format(failed))
    elif warned > 0:
        print("pychestrate-client: no problems, {} warning(s).".format(warned))
    else:
        print("pychestrate-client: everything checked is fine.")
    rtnval = exit_code_for(checks=checks)
    return rtnval


def _cmd_version(args: argparse.Namespace) -> int:
    """
        Implements the ``version`` subcommand.

        :param args: Parsed CLI arguments (unused).

        :returns: Process exit code (0).
    """
    print("pychestrate-client {}".format(VERSION))
    rtnval = 0
    return rtnval


def _common_options(*, subcommand: bool) -> argparse.ArgumentParser:
    """
        The options every command takes. The top-level parser and each
        subcommand share them, so they work before or after the command
        (``run --state-dir X`` as well as ``--state-dir X run``); the
        subcommand copies leave an option unset unless it was given there,
        so a value given before the command is not overwritten by a default.

        :param subcommand: True for the copy a subcommand parser inherits.

        :returns: A parser to pass as a ``parents`` entry.
    """
    unset = argparse.SUPPRESS
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--state-dir",
        default=unset if subcommand else None,
        help="Path to the local state directory (default: {}).".format(DEFAULT_STATE_DIR_HELP),
    )
    common.add_argument(
        "--verbose",
        action="store_true",
        default=unset if subcommand else False,
        help="Enable DEBUG logging.",
    )
    common.add_argument(
        "--quiet",
        action="store_true",
        default=unset if subcommand else False,
        help="Log only warnings and errors.",
    )
    common.add_argument(
        "--log-format",
        choices=("text", "json"),
        default=unset if subcommand else "text",
        help="Log lines as text (default) or one JSON object per line.",
    )
    return common


def _build_parser() -> argparse.ArgumentParser:
    """
        Builds the top-level :class:`argparse.ArgumentParser` for the
        ``pychestrate-client`` CLI.

        :returns: The configured parser.
    """
    parser = argparse.ArgumentParser(
        prog="pychestrate-client",
        description="Pychestrate pod-side runtime client "
                    "(enroll, heartbeat, dispatch consumer + testbase / "
                    "build executors).",
        epilog=HELP_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        parents=[_common_options(subcommand=False)],
    )
    parser.add_argument(
        "--version",
        action="version",
        version="pychestrate-client {}".format(VERSION),
    )

    subparsers = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")
    sub_common = _common_options(subcommand=True)

    enroll_parser = subparsers.add_parser(
        "enroll",
        help="Redeem an enrollment token and persist the resulting mTLS identity.",
        parents=[sub_common],
    )
    enroll_parser.add_argument("--server-url", "--service-url", dest="service_url", required=True,
                               metavar="URL",
                               help="Full URL of the Pychestrate service, for example "
                                    "https://pychestrate.example.com. A path prefix "
                                    "(https://host/pychestrate) is kept. Same option as "
                                    "'pyc login --server-url'; --service-url is the older "
                                    "spelling and still works.")
    enroll_parser.add_argument("--token", default=None,
                               help="One-time enrollment bootstrap token. It is visible in the process "
                                    "list and shell history; prefer --token-file or the {} "
                                    "environment variable.".format(ENV_ENROLLMENT_TOKEN))
    enroll_parser.add_argument("--token-file", default=None, metavar="PATH",
                               help="Read the token from this file (- for standard input).")
    enroll_parser.add_argument("--name", required=True,
                               help="The pod label the token was minted for. It must match exactly: "
                                    "the service refuses a token used under any other name.")
    enroll_parser.add_argument("--insecure", action="store_true",
                               help="Skip TLS verification during the redeem call "
                                    "(useful for self-signed development services).")
    enroll_parser.add_argument("--force", action="store_true",
                               help="Replace the identity of a state directory that is already "
                                    "enrolled (without it, enrolling again is refused).")
    enroll_parser.add_argument("--timeout", type=float, default=DEFAULT_ENROLLMENT_TIMEOUT_SECONDS,
                               help="Seconds to wait for the service (default: {:g}).".format(
                                   DEFAULT_ENROLLMENT_TIMEOUT_SECONDS))
    enroll_parser.add_argument("--json", action="store_true",
                               help="Print only the result, as JSON, on standard output.")
    enroll_parser.set_defaults(handler=_cmd_enroll)

    heartbeat_parser = subparsers.add_parser(
        "heartbeat",
        help="Run the periodic heartbeat loop against the Pychestrate service.",
        parents=[sub_common],
    )
    heartbeat_parser.add_argument("--interval", type=float,
                                  default=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
                                  help="Heartbeat interval in seconds (default: {}).".format(
                                      DEFAULT_HEARTBEAT_INTERVAL_SECONDS))
    heartbeat_parser.add_argument("--once", action="store_true",
                                  help="Send one heartbeat and exit (0 when the service accepted it).")
    heartbeat_parser.set_defaults(handler=_cmd_heartbeat)

    install_key_parser = subparsers.add_parser(
        "install-signing-key",
        help="Pin envelope verification to an out-of-band Ed25519 signing "
             "public key (operator override; the client otherwise fetches "
             "the service's JWK Set automatically).",
        parents=[sub_common],
    )
    install_key_parser.add_argument("--key-file", required=True,
                                    help="Path to the PEM-encoded Ed25519 "
                                         "envelope signing public key.")
    install_key_parser.add_argument("--key-id", required=True,
                                    help="The key id (JWKS key_id) the "
                                         "service stamps on signed envelopes.")
    install_key_parser.set_defaults(handler=_cmd_install_signing_key)

    run_parser = subparsers.add_parser(
        "run",
        help="Run the heartbeat loop and the dispatch-consumer loop "
             "concurrently (normal pod-side mode).",
        parents=[sub_common],
    )
    run_parser.add_argument("--interval", type=float,
                            default=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
                            help="Heartbeat interval in seconds (default: {}).".format(
                                DEFAULT_HEARTBEAT_INTERVAL_SECONDS))
    run_parser.add_argument("--poll-timeout", type=int,
                            default=DEFAULT_DISPATCH_POLL_TIMEOUT_SECONDS,
                            help="Dispatch long-poll wait window in seconds "
                                 "(default: {}).".format(
                                     DEFAULT_DISPATCH_POLL_TIMEOUT_SECONDS))
    run_parser.set_defaults(handler=_cmd_run)

    doctor_parser = subparsers.add_parser(
        "doctor",
        help="Check that this machine is enrolled and the service answers.",
        parents=[sub_common],
    )
    doctor_parser.add_argument("--offline", action="store_true",
                               help="Check the local files only; do not contact the service.")
    doctor_parser.set_defaults(handler=_cmd_doctor)

    version_parser = subparsers.add_parser(
        "version",
        help="Print the pychestrate-client version and exit.",
        parents=[sub_common],
    )
    version_parser.set_defaults(handler=_cmd_version)

    rtnval = parser
    return rtnval


def main(argv: Optional[List[str]] = None) -> int:
    """
        Top-level CLI entry point used by the ``pychestrate-client`` console
        script.

        :param argv: Optional argument vector. Defaults to ``sys.argv[1:]``.

        :returns: Process exit code.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.verbose and args.quiet:
        parser.error("--verbose and --quiet cannot be used together")
    _configure_logging(verbose=args.verbose, quiet=args.quiet, log_format=args.log_format)

    handler = args.handler
    rtnval = handler(args)
    return rtnval


if __name__ == "__main__":
    sys.exit(main())

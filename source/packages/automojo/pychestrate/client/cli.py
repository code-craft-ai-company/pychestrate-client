"""
    .. module:: cli
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Argparse-based entry point for the ``pychestrate-client``
                   binary. Exposes the M1 subcommands (``enroll``,
                   ``heartbeat``, ``version``) plus the M2 subcommands
                   (``install-signing-key``, ``run``).

                   As of M4 the client fetches the envelope signing-key set
                   automatically (bundled in the enrollment-redeem response,
                   refreshed from ``GET /api/v1/envelope-signing-keys`` on an
                   unknown key id), so ``install-signing-key`` is retained
                   only for air-gapped / fallback installs.

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

from automojo.errors.exceptions import BadParameterError, ConfigurationError, HTTPRequestError

from automojo.pychestrate.client import VERSION
from automojo.pychestrate.client.dispatch_consumer import (
    DEFAULT_DISPATCH_POLL_TIMEOUT_SECONDS,
    DispatchConsumerAspects,
    run_dispatch_consumer_loop,
)
from automojo.pychestrate.client.enrollment import perform_enrollment
from automojo.pychestrate.client.heartbeat import (
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    HeartbeatAspects,
    run_heartbeat_loop,
)
from automojo.pychestrate.client.state import ClientState


def _configure_logging(verbose: bool) -> None:
    """
        Configures the root logger for CLI usage.

        :param verbose: When True, sets DEBUG level; otherwise INFO.

        :returns: None
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )
    return


def _cmd_enroll(args: argparse.Namespace) -> int:
    """
        Implements the ``enroll`` subcommand.

        :param args: Parsed CLI arguments.

        :returns: Process exit code (0 on success).
    """
    state = ClientState(state_dir=args.state_dir)

    try:
        result = perform_enrollment(
            service_url=args.service_url,
            bootstrap_token=args.token,
            client_name=args.name,
            state=state,
            attributes=None,
            verify_tls=not args.insecure,
        )
    except (BadParameterError, ConfigurationError, HTTPRequestError) as enrollerr:
        print("pychestrate-client: enrollment failed: {}".format(enrollerr), file=sys.stderr)
        rtnval = 2
        return rtnval

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

        NOTE: this subcommand is no longer part of the happy path. As of
        M4 the client fetches the JWKS-style signing-key set automatically
        — it is bundled in the enrollment-redeem response and refreshed
        from ``GET /api/v1/envelope-signing-keys`` whenever an envelope is
        signed by an unknown key id. This subcommand is retained only for
        air-gapped / fallback installs where the operator must seed the
        single-file signing key out-of-band.

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

    rtnval = 0
    return rtnval


def _cmd_run(args: argparse.Namespace) -> int:
    """
        Implements the ``run`` subcommand — the normal pod-side mode.

        Starts the heartbeat loop and the dispatch-consumer loop on two
        daemon threads and blocks until either loop exits (both honor
        SIGTERM/SIGINT for a graceful shutdown).

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

    def _heartbeat_thread() -> None:
        try:
            run_heartbeat_loop(state=state, aspects=heartbeat_aspects)
        except (BadParameterError, ConfigurationError) as hberr:
            thread_errors.append("heartbeat loop: {}".format(hberr))
        return

    def _dispatch_thread() -> None:
        try:
            run_dispatch_consumer_loop(state=state, aspects=consumer_aspects)
        except (BadParameterError, ConfigurationError) as dcerr:
            thread_errors.append("dispatch consumer: {}".format(dcerr))
        return

    hb_thread = threading.Thread(
        target=_heartbeat_thread, name="pychestrate-heartbeat", daemon=True)
    dc_thread = threading.Thread(
        target=_dispatch_thread, name="pychestrate-dispatch", daemon=True)

    print("pychestrate-client: starting heartbeat + dispatch-consumer loops.")
    hb_thread.start()
    dc_thread.start()

    hb_thread.join()
    dc_thread.join()

    if len(thread_errors) > 0:
        for nxterr in thread_errors:
            print("pychestrate-client: {}".format(nxterr), file=sys.stderr)
        rtnval = 2
        return rtnval

    print("pychestrate-client: run loops exited.")
    rtnval = 0
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
    )
    parser.add_argument(
        "--state-dir",
        default=None,
        help="Path to the local state directory (default: ~/.pychestrate-client).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable DEBUG logging.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    enroll_parser = subparsers.add_parser(
        "enroll",
        help="Redeem an enrollment token and persist the resulting mTLS identity.",
    )
    enroll_parser.add_argument("--service-url", required=True,
                               help="Full URL of the Pychestrate service.")
    enroll_parser.add_argument("--token", required=True,
                               help="One-time enrollment bootstrap token.")
    enroll_parser.add_argument("--name", required=True,
                               help="Human-readable client name for the registry.")
    enroll_parser.add_argument("--insecure", action="store_true",
                               help="Skip TLS verification during the redeem call "
                                    "(useful for self-signed development services).")
    enroll_parser.set_defaults(handler=_cmd_enroll)

    heartbeat_parser = subparsers.add_parser(
        "heartbeat",
        help="Run the periodic heartbeat loop against the Pychestrate service.",
    )
    heartbeat_parser.add_argument("--interval", type=float,
                                  default=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
                                  help="Heartbeat interval in seconds (default: {}).".format(
                                      DEFAULT_HEARTBEAT_INTERVAL_SECONDS))
    heartbeat_parser.set_defaults(handler=_cmd_heartbeat)

    install_key_parser = subparsers.add_parser(
        "install-signing-key",
        help="Install the service's Ed25519 envelope signing public key "
             "out-of-band (air-gapped / fallback only; the client otherwise "
             "fetches the signing-key set automatically).",
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

    version_parser = subparsers.add_parser(
        "version",
        help="Print the pychestrate-client version and exit.",
    )
    version_parser.set_defaults(handler=_cmd_version)

    rtnval = parser
    return rtnval


def main(argv: Optional[List[str]] = None) -> int:
    """
        Top-level CLI entry point used by the ``pychestrate-client`` console
        script and by ``python -m automojo.pychestrate.client``.

        :param argv: Optional argument vector. Defaults to ``sys.argv[1:]``.

        :returns: Process exit code.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    _configure_logging(verbose=args.verbose)

    handler = args.handler
    rtnval = handler(args)
    return rtnval


if __name__ == "__main__":
    sys.exit(main())

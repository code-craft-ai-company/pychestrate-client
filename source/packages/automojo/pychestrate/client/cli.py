"""
    .. module:: cli
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: Argparse-based entry point for the ``pychestrate-client``
                   binary. Exposes the subcommands needed by the M1 milestone:
                   ``enroll``, ``heartbeat``, ``version``.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2025, Automation Mojo LLC"
__credits__ = []


from typing import List, Optional

import argparse
import json
import logging
import sys

from automojo.errors.exceptions import BadParameterError, ConfigurationError, HTTPRequestError

from automojo.pychestrate.client import VERSION
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
        description="Pychestrate pod-side runtime client (M1: enroll + heartbeat).",
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

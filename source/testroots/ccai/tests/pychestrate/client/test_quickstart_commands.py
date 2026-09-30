"""
.. module:: test_quickstart_commands
    :platform: Darwin, Linux, Unix, Windows
    :synopsis: The commands the quickstart gives operators are ones the client accepts (ORG-F18).

.. moduleauthor:: Code Craft AI LLC

The quickstart tells an operator to run ``enroll``, ``doctor`` and ``run`` and
gives a systemd unit whose ``ExecStart`` runs the client. The guide was once out
of date with the code (a feature it called planned had shipped). This test takes
every ``pychestrate-client`` command in the quickstart (and the unit's
``ExecStart``), replaces each ``<placeholder>`` with a value, and parses it with
the client's own argument parser, so a renamed option or subcommand fails here
instead of on an operator's machine.
"""

__author__ = "Code Craft AI LLC"
__copyright__ = "Copyright 2026, Code Craft AI LLC"
__credits__ = []
__version__ = "1.0.0"


import os
import re
import shlex

from typing import List

from ccai import testbase

from ccai.pychestrate.client import cli


def _quickstart() -> str:
    current = os.path.dirname(os.path.abspath(__file__))
    while not os.path.isdir(os.path.join(current, "userguide")) and current != os.path.dirname(current):
        current = os.path.dirname(current)
    with open(os.path.join(current, "userguide", "01-00-quickstart.rst"), encoding="utf-8") as handle:
        return handle.read()


def _commands(text: str) -> List[List[str]]:
    """
        The argument lists (program and ``sudo`` prefix removed) of every line in
        the quickstart's indented blocks that runs ``pychestrate-client``, with a
        trailing backslash joining a line to the next and each ``<placeholder>``
        replaced by ``PLACEHOLDER``.
    """
    lines = text.splitlines()
    found: List[List[str]] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        if line.startswith("   ") and re.search(r"(^|[\s/=])pychestrate-client(\s|$)", line) and "useradd" not in line and "journalctl" not in line and "systemctl" not in line and "pip install" not in line:
            joined = line.strip()
            while joined.endswith("\\") and index + 1 < len(lines):
                index += 1
                joined = joined[:-1].rstrip() + " " + lines[index].strip()
            joined = re.sub(r"^ExecStart=", "", joined)
            joined = re.sub(r"<[^>]+>", "PLACEHOLDER", joined)
            words = shlex.split(joined)
            positions = [number for number, word in enumerate(words) if word == "pychestrate-client" or word.endswith("/bin/pychestrate-client")]
            if len(positions) > 0:
                found.append(words[positions[0] + 1:])
        index += 1
    return found


def test_every_client_command_in_the_quickstart_parses():
    """
    **Feature: a current quickstart (ORG-F18)**
    **Property: each ``pychestrate-client`` command the quickstart shows,
    including the systemd unit's ExecStart, is accepted by the client's argument
    parser, and the quickstart shows at least version, enroll, doctor and run.**
    """
    commands = _commands(_quickstart())
    parser = cli._build_parser()
    refused: List[str] = []
    for arguments in commands:
        try:
            parser.parse_args(arguments)
        except SystemExit:
            refused.append(" ".join(arguments))
    subcommands = {word for arguments in commands for word in arguments if word in ("version", "enroll", "doctor", "run")}

    testbase.assert_equal((refused, sorted(subcommands)), ([], ["doctor", "enroll", "run", "version"]))
    return

"""
    TestBase test that the client's version is written down once.

    ``ccai.pychestrate.client.VERSION`` is what ``--version`` and ``version``
    print. ``pyproject.toml`` (poetry, which cannot read it from code) holds
    the same number for the package metadata; this test fails when they drift.
"""

import os
import tomllib

from ccai import testbase

from ccai.pychestrate.client import VERSION


def _pyproject_version() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    directory = here
    while directory != os.path.dirname(directory):
        candidate = os.path.join(directory, "pyproject.toml")
        if os.path.isfile(candidate):
            with open(candidate, "rb") as handle:
                return str(tomllib.load(handle)["tool"]["poetry"]["version"])
        directory = os.path.dirname(directory)
    raise AssertionError("pyproject.toml not found above {}".format(here))


def test_the_package_metadata_and_the_printed_version_are_the_same_number():
    testbase.assert_equal(_pyproject_version(), VERSION)
    return

"""
    .. module:: __main__
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: ``python -m ccai.pychestrate.client`` runs the same
                   command line as the ``pychestrate-client`` script.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2026, Automation Mojo LLC"
__credits__ = []


import sys

from ccai.pychestrate.client.cli import main


if __name__ == "__main__":
    sys.exit(main())

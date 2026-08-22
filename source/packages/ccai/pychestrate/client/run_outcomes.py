"""
    .. module:: run_outcomes
        :platform: Darwin, Linux, Unix, Windows
        :synopsis: The vocabulary a pod uses to report how a run ended.

                   Two independent axes, because one is not enough.

                   ``TERMINAL_OUTCOME_*`` mirrors the service's
                   ``TerminalOutcome`` enum. It is what drives the run
                   state machine, so its value set is fixed by the
                   service and this module must never invent members
                   the service does not have.

                   ``FAILURE_CLASSIFICATION_*`` mirrors the service's
                   ``RunFailureClassification`` enum and says WHY. The
                   executors already distinguish a blown wall-clock cap
                   from a missing binary from a job that ran and exited
                   non-zero -- they just had nowhere structured to put
                   it, so the distinction died in a prose ``detail``
                   string. The classification rides on the result
                   manifest under ``extras.failure_classification``.

    .. moduleauthor:: Myron Walker <myron.walker@gmail.com>
"""

__author__ = "Myron Walker"
__copyright__ = "Copyright 2025, Automation Mojo LLC"
__credits__ = []


#: The run finished and reported success.
TERMINAL_OUTCOME_SUCCESS = "success"

#: The run finished and reported failure.
TERMINAL_OUTCOME_FAILURE = "failure"

#: An operator cancelled the run and the pod stopped it. Distinct
#: from failure: nothing broke, somebody asked for it to stop.
TERMINAL_OUTCOME_CANCELLED = "cancelled"


#: The job blew its wall-clock cap and the pod killed it. The job
#: never reported a verdict of its own.
FAILURE_CLASSIFICATION_TIMEOUT = "timeout"

#: The binary the job needed is not installed on this pod. A pod
#: provisioning problem, not a job problem.
FAILURE_CLASSIFICATION_EXECUTABLE_NOT_FOUND = "executable_not_found"

#: The job ran to completion and exited non-zero -- the ordinary
#: "the tests failed" / "the build broke" case.
FAILURE_CLASSIFICATION_NONZERO_EXIT = "nonzero_exit"

#: An operator cancelled the run and the pod terminated the process
#: tree. Pairs with :data:`TERMINAL_OUTCOME_CANCELLED`.
FAILURE_CLASSIFICATION_CANCELLED = "cancelled"

#: The pod could not start the process for a reason other than a
#: missing binary -- permissions, a fork failure, a working directory
#: that vanished.
FAILURE_CLASSIFICATION_LAUNCH_ERROR = "launch_error"

#: The pod refused the job before launching anything -- an envelope
#: missing a required field, an unresolvable project root, an
#: unrecognized job kind.
FAILURE_CLASSIFICATION_SETUP_ERROR = "setup_error"


#: Every classification this client is allowed to report. The service
#: drops a value outside its own enum, so sending one would silently
#: lose the detail rather than record it.
KNOWN_FAILURE_CLASSIFICATIONS = (
    FAILURE_CLASSIFICATION_TIMEOUT,
    FAILURE_CLASSIFICATION_EXECUTABLE_NOT_FOUND,
    FAILURE_CLASSIFICATION_NONZERO_EXIT,
    FAILURE_CLASSIFICATION_CANCELLED,
    FAILURE_CLASSIFICATION_LAUNCH_ERROR,
    FAILURE_CLASSIFICATION_SETUP_ERROR,
)

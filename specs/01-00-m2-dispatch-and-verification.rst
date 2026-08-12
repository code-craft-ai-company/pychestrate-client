==================================================
M2 - Dispatch Consumption and Envelope Verification
==================================================

:Milestone: M2 (shipped)
:Status: Shipped-Documented
:Source of Truth: the code in ``source/packages/automojo/pychestrate/client``
:Derived From: ``dispatch_consumer.py``, ``envelope_verifier.py``,
   ``signing_keys.py``, ``testbase_executor.py``, ``state.py``, ``cli.py``,
   ``README.rst``, ``AGENTS.md``
:Last Verified: 2026-08-12, at commit ``6cb9cd1``

Purpose
=======

M2 gave the pod-side client its consumption half of the orchestration loop:
it asks the Pychestrate service for assigned work, proves the work was
issued by the service before touching it, runs it through
``automojo-testbase``, and tells the service it took the job.

The security property M2 establishes is the important one. A pod client
executes arbitrary commands on an operator's machine, so the client
**fails closed**: an envelope whose Ed25519 signature cannot be verified
against a locally-trusted signing key is never executed, only acknowledged
with a rejection note.

This document records the behavior as it exists in the code today. Where
later milestones (M3-M7) extended a code path that M2 introduced, the
extension is noted so the reader is not misled into thinking the module
still matches its M2 shape, but the extensions themselves are specified
elsewhere.

Scope
=====

- The dispatch-consumer loop: long-poll, back-off, shutdown, iteration cap.
- Signed job envelope verification: canonical serialization, signing-key
  resolution, signature check, and the structured outcome it produces.
- Translation of a verified envelope into an ``automojo-testbase``
  invocation, and execution of that invocation.
- The local run record written for each executed job.
- Dispatch acknowledgement, including how a rejection is conveyed.
- The CLI surface that drives the above (``run``, ``install-signing-key``).

Out of Scope
============

- Enrollment and heartbeat (M1). Referenced only where dispatch depends on
  the state they persist.
- Progress streaming, result-tree upload and the completion manifest (M3).
  See ``02-00-m3-progress-and-result-push.rst``.
- Automatic signing-key delivery and the presigned-URL upload protocol
  (M4), the ``job_kind`` branch and build executor (M5), the provision
  executor (M6), and the structured source block (M7). These are noted
  where they changed an M2 code path but are not specified here.
- The service-side dispatch queue, lease semantics, and envelope minting.

Definitions
===========

Envelope
    The signed job description the service delivers inside a
    ``PendingDispatch``. It is a JSON object; the client treats it as an
    opaque dict for verification purposes and reads named fields from it
    only after verification succeeds.

Detached signature
    The hex-encoded Ed25519 signature delivered alongside the envelope
    (``PendingDispatch.signature``), computed by the service over the
    envelope's canonical bytes rather than embedded in the envelope.

Canonical bytes
    The exact byte sequence the signature was computed over. The client
    must re-derive it byte-for-byte to verify.

``local_run_id``
    A client-minted UUID4 identifying one local execution attempt. Distinct
    from the server-assigned ``run_id``. Minted once per dispatch, before
    verification, so that a rejected dispatch still carries one on its ack
    (``dispatch_consumer.py:626``).

Fail closed
    On any inability to verify - unknown key id, malformed signature,
    non-canonicalizable envelope, signature mismatch - the client refuses
    to execute and acknowledges with a rejection note.

Requirements
============

Dispatch Loop
-------------

**M2-DL-001** - The client MUST obtain the next dispatch through the
codegen'd REST client method ``clients.clients_dispatches_next``, passing
the enrolled ``client_id`` and a caller-supplied long-poll wait window. It
MUST NOT hand-roll the HTTP call.
*Source:* ``dispatch_consumer.py:277-281``; ``AGENTS.md`` "Do not bypass the
codegen'd REST client".

**M2-DL-002** - A long-poll that elapses with no work MUST be treated as
"no dispatch available" and MUST NOT be treated as an error, MUST NOT
trigger back-off, and MUST NOT be acknowledged. The endpoint answers HTTP
204 with an empty body; because the codegen'd method decodes the body as
JSON unconditionally, the empty body surfaces as a ``ValueError`` after a
non-raising ``perform_call``, and that ``ValueError`` is the timeout signal
the loop keys on.
*Source:* ``dispatch_consumer.py:282-290``. See ``OQ-DL-001``.

**M2-DL-003** - The loop MUST apply exponential back-off after an
iteration in which the poll or the dispatch handling raised. The first
back-off sleep is ``base_backoff_seconds`` (default 5.0); each consecutive
failing iteration doubles the interval, clamped at ``max_backoff_seconds``
(default 300.0). A non-failing iteration MUST reset the interval to
``base_backoff_seconds``.
*Source:* ``dispatch_consumer.py:800-808``, ``dispatch_consumer.py:108-110``.

**M2-DL-004** - Handling one dispatch MUST NOT be able to terminate the
loop. Every exception raised by the poll or by ``handle_one_dispatch`` is
caught, logged at WARNING with the iteration number, and converted into a
back-off.
*Source:* ``dispatch_consumer.py:792-798``.

**M2-DL-005** - The loop MUST stop gracefully on ``SIGTERM`` or ``SIGINT``,
on a caller-supplied ``stop_predicate`` returning ``True``, or on
exhausting ``max_iterations``. Previously-installed signal handlers MUST be
restored when the loop exits, including on an exception path.
*Source:* ``dispatch_consumer.py:737-754``, ``760-767``, ``810-820``.

**M2-DL-006** - Signal-handler installation MUST tolerate being called
from a non-main thread. ``signal.signal`` raising ``ValueError`` or
``OSError`` is recorded as "no previous handler" and the loop continues;
this is required because the ``run`` subcommand drives the loop on a
non-main thread.
*Source:* ``dispatch_consumer.py:747-754``; ``cli.py:237-244``.

**M2-DL-007** - The loop MUST process at most one dispatch at a time. Each
iteration polls, handles, and only then polls again; there is no
concurrency within a single consumer loop.
*Source:* ``dispatch_consumer.py:760-798``. See ``OQ-DL-002``.

**M2-DL-008** - Loop behavior MUST be caller-controlled through a single
``DispatchConsumerAspects`` parameter rather than through decorators, per
the repository's Aspects pattern. The aspects object MUST reject invalid
configuration at construction with ``BadParameterError``: a negative
``poll_timeout_seconds``, a non-positive ``base_backoff_seconds``, or a
``max_backoff_seconds`` below ``base_backoff_seconds``.
*Source:* ``dispatch_consumer.py:115-200``.

Envelope Verification
---------------------

**M2-DV-001** - The client MUST verify the Ed25519 signature on every job
envelope before executing it, and MUST NOT execute an envelope it cannot
verify.
*Source:* ``dispatch_consumer.py:646-670``; ``AGENTS.md`` "Project Specific
Rules".

**M2-DV-002** - A dispatch whose ``envelope`` field is not a JSON object
MUST be rejected before verification is attempted, logged at ERROR, and
acknowledged with the note ``REJECTED: malformed dispatch (no envelope).``
*Source:* ``dispatch_consumer.py:628-640``.

**M2-DV-003** - The canonical payload bytes MUST be re-derived from the
delivered envelope object as JSON with sorted keys, no inter-token
whitespace (``separators=(",", ":")``), non-ASCII characters left
unescaped (``ensure_ascii=False``), encoded UTF-8. This MUST match the
service-side ``jobs.envelope.canonical.serialize_canonical`` byte-for-byte.
*Source:* ``envelope_verifier.py:286-304``. See ``OQ-DV-001``.

**M2-DV-004** - An envelope that cannot be canonicalized (not
JSON-serializable) MUST be rejected with reason ``malformed`` rather than
raising out of the verifier.
*Source:* ``envelope_verifier.py:293-301``, ``347-352``.

**M2-DV-005** - The signing public key MUST be resolved from the persisted
JWKS-style signing-key set (``<state_dir>/envelope-signing-keys.json``),
indexed by the ``signature_key_id`` the dispatch carries.
*Source:* ``envelope_verifier.py:215-231``; ``state.py:350-396``.

**M2-DV-006** - When the persisted set does not carry the requested
``key_id``, the client MUST refresh the set from the service exactly once
and retry the lookup before failing. The refresh calls
``dispatch.envelope_signing_keys_list`` (``GET
/api/v1/envelope-signing-keys``) and rewrites the persisted set.
*Source:* ``envelope_verifier.py:225-231``; ``signing_keys.py:121-150``.
This is M4 behavior layered onto the M2 verification path; M2 as originally
shipped resolved a single out-of-band key.

**M2-DV-007** - A ``key_id`` that is still unresolved after the single
refresh MUST fail closed with reason ``unknown_key``. The error message
MUST name the requested ``key_id``, the persisted set's path, and the key
ids that are known, so an operator can diagnose it without adding logging.
*Source:* ``envelope_verifier.py:233-245``, ``402-413``.

**M2-DV-008** - A signing-key entry that is present but carries no
``public_key_pem``, or whose PEM is not a valid Ed25519 public key, MUST be
reported as a ``ConfigurationError`` naming the source of the key. A
non-Ed25519 key MUST be rejected rather than used.
*Source:* ``envelope_verifier.py:145-167``, ``247-255``.

**M2-DV-009** - A signature that is not valid hex MUST be rejected with
reason ``malformed``; a signature that decodes but does not verify MUST be
rejected with reason ``signature_mismatch``. Both MUST be logged at ERROR,
and the mismatch log MUST name the ``envelope_id``.
*Source:* ``envelope_verifier.py:336-365``.

**M2-DV-010** - The verifier MUST NOT raise on a signature problem. Every
failure mode is returned as a structured ``EnvelopeVerificationResult``
carrying ``valid``, a ``reason`` code, and a human-readable ``detail``, so
the consumer can reject and acknowledge without wrapping the call in
exception handling. The reason codes ``ok``, ``signature_mismatch``,
``unknown_key`` and ``malformed`` mirror the service-side
``jobs.envelope.signer.VerificationResult``.
*Source:* ``envelope_verifier.py:58-63``, ``66-129``.

**M2-DV-011** - A rejected dispatch MUST still be acknowledged, with a note
of the form ``REJECTED: envelope verification failed (<reason>): <detail>``,
and MUST NOT be executed. ``handle_one_dispatch`` returns ``False`` for a
rejection.
*Source:* ``dispatch_consumer.py:654-670``.

Job Execution
-------------

**M2-EX-001** - A verified envelope MUST be translated into a ``testbase
testing run`` argv list and executed as a subprocess. The client MUST NOT
import and drive testbase in-process.
*Source:* ``testbase_executor.py:259-343``, ``477-483``.

**M2-EX-002** - The testbase project root MUST be resolved from the
envelope's ``params.test_root``, falling back to ``params.testbase_root``.
An envelope carrying neither MUST fail the command build with
``BadParameterError``; remote project fetch is out of scope for M2.
*Source:* ``testbase_executor.py:233-256``, ``274-286``.

**M2-EX-003** - Test selection MUST be taken from ``params.includes`` (a
list or a single scalar) and ``params.excludes``. When ``params.includes``
yields nothing, a single ``params.suite`` value MUST be used as the
include selector.
*Source:* ``testbase_executor.py:186-230``.

**M2-EX-004** - The invocation MUST carry ``--root`` and ``--output``, then
one ``--includes=<sel>`` per include selector and one ``--excludes=<sel>``
per exclude selector, then ``--branch``, ``--build`` and ``--flavor`` from
the envelope's ``refs`` map, then ``--runid``, ``--job-id`` and
``--job-name`` from the envelope's top level. Each optional argument MUST be
omitted when its source value is absent or empty.
*Source:* ``testbase_executor.py:298-343``.

**M2-EX-005** - Run output MUST be written to
``<state_dir>/runs/<run_id>/output``, created with mode 0755 if absent. The
path MUST be deterministic for a given ``(state, run_id)`` pair so a caller
can resolve it before the run starts. A missing or empty ``run_id`` MUST
fall back to the literal ``unknown-run`` rather than writing to the runs
directory root.
*Source:* ``testbase_executor.py:346-365``.

**M2-EX-006** - The executor MUST NOT crash the client. A failure to build
the command, a missing ``testbase`` binary, a subprocess timeout, a
non-zero exit, and any unexpected exception MUST each be captured as a
failed ``TestbaseRunOutcome`` carrying a human-readable ``detail``.
*Source:* ``testbase_executor.py:446-455``, ``491-506``.

**M2-EX-007** - The subprocess MUST be run with a wall-clock cap, default
14400 seconds (4 hours), overridable through ``TestbaseExecutorAspects``.
Output MUST be captured (``capture_output=True``).
*Source:* ``testbase_executor.py:50``, ``477-483``. See ``OQ-EX-002``.

**M2-EX-008** - Every execution attempt MUST write a local run record to
``<state_dir>/runs/<run_id>/run-record.json`` carrying ``run_id``,
``local_run_id``, ``envelope_id``, and the outcome (``status``,
``exit_code``, ``output_dir``, ``command``, ``detail``). The record MUST be
written on the failure paths as well as the success path.
*Source:* ``testbase_executor.py:368-401``, ``452-455``, ``521-523``.

**M2-EX-009** - Executor behavior (binary name, timeout, subprocess-runner
override) MUST be caller-controlled through a single
``TestbaseExecutorAspects`` parameter. An empty binary name or a
non-positive timeout MUST be rejected at construction with
``BadParameterError``.
*Source:* ``testbase_executor.py:57-108``.

Acknowledgement
---------------

**M2-AK-001** - Every dispatch the client takes off the queue MUST be
acknowledged, whether it was executed or rejected, through the codegen'd
``dispatch.dispatch_ack_run`` method.
*Source:* ``dispatch_consumer.py:329-376``, called at ``634-638``,
``663-668`` and ``687-690``.

**M2-AK-002** - The ``DispatchAck`` body MUST carry ``envelope_id``,
``dispatch_attempt_id``, ``client_id``, ``local_run_id`` and an RFC 3339
``accepted_at`` timestamp.
*Source:* ``dispatch_consumer.py:357-363``, ``246-253``.

**M2-AK-003** - Because the ``DispatchAck`` schema models receipt
confirmation only and carries no accepted/rejected flag, a rejection or an
execution failure MUST be conveyed in the free-form ``notes`` field. The
note MUST be prefixed ``REJECTED:`` for a dispatch that was not executed
and ``ACCEPTED:`` for one that was.
*Source:* ``dispatch_consumer.py:333-341``, ``364-365``, ``469``, ``638``,
``667``. See ``OQ-AK-001``.

**M2-AK-004** - The acknowledgement MUST tolerate the endpoint's HTTP 204
empty-body response: an empty-body decode failure following a non-raising
``perform_call`` is treated as acceptance.
*Source:* ``dispatch_consumer.py:367-375``. See ``OQ-DL-001``.

CLI Surface
-----------

**M2-CLI-001** - The ``run`` subcommand MUST drive the heartbeat loop and
the dispatch-consumer loop concurrently, on two daemon threads, and MUST
return exit code 2 if either loop raised a ``BadParameterError`` or
``ConfigurationError``.
*Source:* ``cli.py:197-257``.

**M2-CLI-002** - ``run`` MUST accept ``--interval`` (heartbeat seconds,
default 30) and ``--poll-timeout`` (dispatch long-poll window, default 30).
The state directory MUST be overridable with the top-level ``--state-dir``.
*Source:* ``cli.py:286-290``, ``343-352``;
``dispatch_consumer.py:108``.

**M2-CLI-003** - ``install-signing-key`` MUST write the supplied PEM to
``<state_dir>/envelope-signing-public-key.pem`` and record the key id in
``config.json`` under ``envelope_signing_key_id``. As of M4 this command is
retained for air-gapped / fallback installs only and is not on the happy
path.
*Source:* ``cli.py:139-194``; ``state.py:275-289``. See ``OQ-DV-003``.

Behavioral Flows
================

Normal dispatch cycle
---------------------

1. Build an mTLS REST client from the persisted enrollment state and read
   the ``client_id`` (``dispatch_consumer.py:731-732``).
2. Long-poll ``clients.clients_dispatches_next`` for
   ``poll_timeout_seconds``.
3. On a timeout (HTTP 204), log at DEBUG and re-poll. No ack, no back-off.
4. On a dispatch, coerce the ``PendingDispatch`` to a dict - via
   ``to_dict()`` when the schema object provides one, otherwise by
   attribute access over the documented fields - and mint a
   ``local_run_id``.
5. Verify the envelope. On failure, log at ERROR, ack with a ``REJECTED:``
   note, and return without executing.
6. On success, resolve the ``job_kind`` and dispatch to the matching
   handler (see "Job-kind branching" below).
7. For a test job: create the run output directory, build the argv, run
   the subprocess under the timeout, and write the local run record.
8. Ack with an ``ACCEPTED:`` note describing the run outcome.
9. Reset the back-off interval and re-poll.

Rejection cycle
---------------

1. Steps 1-4 as above.
2. Verification fails, or the dispatch carried no envelope object.
3. Log at ERROR with ``run_id``, ``envelope_id``, the reason code and the
   detail.
4. Ack with the ``REJECTED:`` note. The envelope is never executed and no
   run output directory is created for it.
5. Return ``False`` and continue the loop. A rejection is not a loop
   failure and does NOT trigger back-off.

Job-kind branching (M5/M6 extension)
------------------------------------

The M2 code path executed every verified envelope through the testbase
executor. As of M5/M6 the consumer branches on the envelope's ``job_kind``
discriminator after verification (``dispatch_consumer.py:672-685``):

- ``test`` - testbase executor (the M2/M3 path specified here),
- ``build`` - build executor,
- ``provision`` - provision executor,
- missing or unrecognized - defaults to ``test`` for backward
  compatibility, with an unrecognized value logged at WARNING
  (``dispatch_consumer.py:379-409``).

The branch is downstream of verification, so **M2-DV-001** holds for every
job kind. The build and provision executors are not specified in this
document.

Interfaces
==========

REST operations consumed
------------------------

All calls go through the codegen'd ``automojo-restclient-pychestrate``
package over the mTLS session established from the enrolled identity.

.. list-table::
   :header-rows: 1
   :widths: 30 34 36

   * - Purpose
     - Codegen'd method
     - Notes
   * - Fetch next dispatch
     - ``clients.clients_dispatches_next``
     - Long-poll; HTTP 204 on timeout.
   * - Acknowledge dispatch
     - ``dispatch.dispatch_ack_run``
     - HTTP 204; rejection conveyed in ``notes``.
   * - Refresh signing keys
     - ``dispatch.envelope_signing_keys_list``
     - ``GET /api/v1/envelope-signing-keys`` (M4).

Fields read from ``PendingDispatch``
------------------------------------

``dispatch_attempt_id``, ``run_id``, ``client_id``, ``pod_id``,
``envelope_id``, ``envelope``, ``signature``, ``signature_key_id``,
``signed_at`` (``dispatch_consumer.py:315-325``).

Of these, ``pod_id`` and ``signed_at`` are read into the dict view but are
not used by any subsequent logic. See ``OQ-DL-003``.

Fields read from the verified envelope
--------------------------------------

``run_id``, ``client_id``, ``envelope_id``, ``job_kind``, ``job_type``,
``job_id``, ``job_name``, ``params`` (``test_root`` / ``testbase_root`` /
``includes`` / ``suite`` / ``excludes``), ``refs`` (``branch`` / ``build``
/ ``flavor``), and ``results`` (consumed by M3; see
``02-00-m3-progress-and-result-push.rst``).

This repository holds no copy of the envelope schema; the field list above
is what the client actually reads, not a published contract. See
``OQ-DV-002``.

On-disk state
-------------

Dispatch and verification add the following to the M1 state directory::

    <state_dir>/
        envelope-signing-keys.json        - JWKS-style signing-key set (M4)
        envelope-signing-public-key.pem   - single key, install-signing-key
        runs/
            <run_id>/
                run-record.json           - local execution record
                output/                   - testbase run output tree

The state directory itself is mode 0700 (it holds the client private key);
the signing-key files are written 0644, and the run directories 0755
(``state.py:166-178``, ``state.py:347``, ``testbase_executor.py:363``).

Error Handling
==============

The repository's exception policy applies: exceptions are not caught unless
the code can recover and continue its intended purpose, or can add context.
The dispatch path uses three distinct strategies, deliberately.

**Structured results instead of exceptions.** Signature problems are an
expected input condition, not an error in the client, so the verifier
returns ``EnvelopeVerificationResult`` and never raises on them
(``envelope_verifier.py:66-75``). The consumer branches on ``valid``.

**Capture-and-record.** The executor converts every subprocess failure mode
into a failed ``TestbaseRunOutcome`` with a ``detail`` string, because the
client must survive a bad dispatch and must leave a diagnosable record
(``testbase_executor.py:491-506``).

**Catch-all at the loop boundary.** ``run_dispatch_consumer_loop`` catches
every exception from an iteration, because the loop is a long-lived pod
daemon whose intended purpose - keep consuming work - is served by backing
off and retrying (``dispatch_consumer.py:792-798``).

``ConfigurationError`` is raised for operator-fixable state problems: a
missing ``client_id``, a missing or malformed signing-key set, a key that is
not a valid Ed25519 public key. These messages are multi-line and name the
expected path plus the command that repairs it
(``state.py:203-210``, ``359-369``; ``envelope_verifier.py:233-245``).

Verification
============

Requirements are covered by TestBase tests under
``source/testroots/automojo/tests/pychestrate/client/``.

.. list-table::
   :header-rows: 1
   :widths: 46 54

   * - Test
     - Covers
   * - ``test_envelope_verifier.test_valid_signature_verifies``
     - M2-DV-003, M2-DV-010
   * - ``test_envelope_verifier.test_tampered_envelope_is_rejected``
     - M2-DV-009 (``signature_mismatch``)
   * - ``test_envelope_verifier.test_tampered_signature_is_rejected``
     - M2-DV-009
   * - ``test_envelope_verifier.test_wrong_key_signature_is_rejected``
     - M2-DV-009
   * - ``test_envelope_verifier.test_unknown_key_id_is_rejected``
     - M2-DV-007
   * - ``test_envelope_verifier.test_resolve_signing_public_key_from_persisted_set``
     - M2-DV-005
   * - ``test_envelope_verifier.test_resolve_signing_public_key_refreshes_on_unknown_key``
     - M2-DV-006
   * - ``test_envelope_verifier.test_verify_dispatch_envelope_unknown_key_after_refresh_is_rejected``
     - M2-DV-007 (fail closed)
   * - ``test_envelope_verifier.test_load_signing_public_key_round_trip``
     - M2-DV-008 (legacy single-file path)
   * - ``test_dispatch_consumer.test_handle_one_dispatch_verifies_executes_and_acks``
     - M2-DV-001, M2-AK-001, M2-AK-002
   * - ``test_dispatch_consumer.test_handle_one_dispatch_rejects_tampered_envelope_without_executing``
     - M2-DV-001, M2-DV-011
   * - ``test_dispatch_consumer.test_handle_one_dispatch_refreshes_signing_keys_on_unknown_key``
     - M2-DV-006
   * - ``test_dispatch_consumer.test_handle_one_dispatch_rejects_unknown_key_after_refresh``
     - M2-DV-007, M2-DV-011
   * - ``test_dispatch_consumer.test_consumer_loop_handles_dispatch_then_timeouts``
     - M2-DL-002
   * - ``test_dispatch_consumer.test_consumer_loop_stops_on_predicate``
     - M2-DL-005
   * - ``test_dispatch_consumer.test_aspects_rejects_bad_values``
     - M2-DL-008
   * - ``test_testbase_executor.test_build_command_translates_envelope``
     - M2-EX-003, M2-EX-004
   * - ``test_testbase_executor.test_build_command_rejects_missing_test_root``
     - M2-EX-002
   * - ``test_testbase_executor.test_execute_job_envelope_records_success``
     - M2-EX-008
   * - ``test_testbase_executor.test_execute_job_envelope_records_failure_on_nonzero_exit``
     - M2-EX-006, M2-EX-008
   * - ``test_testbase_executor.test_execute_job_envelope_survives_missing_binary``
     - M2-EX-006
   * - ``test_testbase_executor.test_execute_job_envelope_survives_unbuildable_envelope``
     - M2-EX-006

Requirements with no direct test coverage: **M2-DL-003** (back-off
doubling and reset), **M2-DL-006** (signal installation from a non-main
thread), **M2-EX-007** (run timeout), **M2-CLI-001** through
**M2-CLI-003**. Run the suite with::

    testbase testing run --root source/testroots/automojo \
        --includes=automojo.tests.pychestrate.client --output=./.output

Open Questions
==============

**OQ-DL-001** - Is treating a ``ValueError`` from the codegen'd decode as
"HTTP 204, no work" safe, given it cannot be distinguished from a genuine
malformed 200 body? The same idiom is used for the dispatch-ack 204
(``dispatch_consumer.py:367-375``) and, in M3, for the 202 responses on
progress and results/complete. A malformed success body would currently be
silently swallowed as a timeout or an acceptance, which conflicts with the
repository's "errors should never pass silently" standard.

*Blocks:* deciding whether the client needs a status-code-aware accessor.
*Owner:* ``automojo-restclient-base`` / ``automojo-restclient-pychestrate``
- does ``perform_call`` expose the response status code to the caller?

**OQ-DL-002** - Is one-job-at-a-time the intended contract for a pod
client, or should a client with capacity for N concurrent runs consume N
dispatches? Nothing in the code or in ``AGENTS.md`` states a concurrency
model, and the service's dispatch-lease semantics are not documented here.

*Blocks:* pod capacity planning; whether ``run`` needs a concurrency flag.
*Owner:* ``pychestrate-service`` (dispatch queue design).

**OQ-DL-003** - ``pod_id`` and ``signed_at`` are read out of the
``PendingDispatch`` into the dict view but never used
(``dispatch_consumer.py:319``, ``324``). Are they intended to be used - for
example, ``signed_at`` for an envelope freshness/expiry check, or ``pod_id``
for a sanity check that the dispatch was routed to the right pod - or is
reading them vestigial?

*Blocks:* whether a replay of an old but validly-signed envelope is a
threat the client is expected to reject. Today it is not rejected: nothing
checks envelope age or one-time use.
*Owner:* ``pychestrate-service`` (envelope schema and threat model).

**OQ-DV-001** - ``serialize_envelope_canonical``'s docstring says the
service orders keys in "UTF-16" order, but the implementation uses Python's
``json.dumps(sort_keys=True)``, which orders ``str`` keys by Unicode code
point. The two orderings differ for keys containing characters above the
BMP. Which ordering does
``jobs.envelope.canonical.serialize_canonical`` actually use?

*Blocks:* correctness of verification for any envelope with non-BMP
characters in a key. Practically unlikely today, but it is a silent
fail-closed rejection if it ever occurs, and the docstring and the code
disagree either way.
*Owner:* ``pychestrate-service``.

**OQ-DV-002** - The job envelope schema is not present in this repository
in any form. The field list in "Interfaces" above was reverse-engineered
from the client's reads. Should this repository carry a copy of (or a
generated binding for) the envelope schema, so a service-side field
addition or rename is caught at build time rather than at dispatch time?

*Blocks:* specifying required-vs-optional envelope fields with authority.
*Owner:* ``pychestrate-service`` / ``automojo-restclient-pychestrate``.

**OQ-DV-003** - The air-gapped fallback path appears to be
non-functional for verification. ``install-signing-key`` writes
``envelope-signing-public-key.pem`` (``cli.py:175``) and
``load_signing_public_key`` reads it (``envelope_verifier.py:170-189``),
but no production code calls ``load_signing_public_key`` - the only caller
is ``test_envelope_verifier.py:232``. ``verify_dispatch_envelope`` resolves
keys exclusively from the JWKS-style set
(``envelope_verifier.py:402-404``). An operator who installs a key
out-of-band on an air-gapped pod would therefore still be rejected with
``unknown_key``.

*Blocks:* whether air-gapped operation is actually supported. Either the
resolver should fall back to the single-file key when the set has no match
and no refresh is possible, or ``install-signing-key`` should write into
the JWKS-style set, or the fallback claim should be removed from
``cli.py``, ``README.rst`` and ``AGENTS.md``.
*Owner:* this repository.

**OQ-DV-004** - In ``verify_signed_envelope`` the guard comparing
``signature_key_id`` against ``signing_key_id``
(``envelope_verifier.py:326-334``) cannot fire on the dispatch path,
because ``verify_dispatch_envelope`` passes
``signing_key_id=signature_key_id`` (``envelope_verifier.py:415-421``). Is
the guard dead code left from the M2 single-key design, or is it intended
to protect a caller that has not been written yet?

*Blocks:* nothing functionally - key identity is enforced upstream by the
resolver - but the guard reads as a live safety check and is not one.
*Owner:* this repository.

**OQ-EX-001** - The executor runs whatever ``params.test_root`` names, and
the envelope's signature is the only thing standing between a dispatch and
arbitrary local execution. Is there an intended additional constraint on
the client side - an allow-list of permitted roots, or a requirement that
the root live under an operator-configured base directory - or is envelope
signing considered sufficient?

*Blocks:* the pod-side threat model. A compromised or over-broad signing
key currently implies arbitrary command execution on every enrolled pod.
*Owner:* ``pychestrate-service`` (envelope threat model) plus this
repository.

**OQ-EX-002** - ``subprocess.run`` is called with ``capture_output=True``
(``testbase_executor.py:477-483``), so the child's stdout and stderr are
buffered in memory for the life of the run and then discarded - they are
not written to the run record, not logged, and not uploaded. For a run
capped at four hours this is an unbounded memory hold and a lost diagnostic.
Should the child's output be streamed to a file in the run output directory
instead?

*Blocks:* debuggability of a failed dispatch, which the repository's
coding standards rank as paramount. Today a non-zero exit records only the
exit code.
*Owner:* this repository.

**OQ-AK-001** - The acknowledgement is sent *after* execution completes
(``dispatch_consumer.py:687-690``), so ``accepted_at`` records the end of
the run, not receipt of the dispatch, and a four-hour run leaves the
service without an ack for four hours. Does the service expect a prompt
receipt ack (and if so, is a second terminal ack expected, or is
``results/complete`` that signal)? Relatedly, ``DispatchAck`` has no
accepted/rejected flag, so a rejection is only machine-readable by string
matching the ``REJECTED:`` prefix in a free-form note - is a structured
rejection field planned?

*Blocks:* whether the service can distinguish "client is still running the
job" from "client died", and whether rejections can be alerted on without
string matching.
*Owner:* ``pychestrate-service`` (``DispatchAck`` schema and dispatch
lease semantics).

**OQ-DOC-001** - ``README.rst`` ("M3 (planned)") and
``userguide/01-00-quickstart.rst`` ("M1 functionality only ... will be
added in M2 and M3") are stale relative to the code, which has shipped
through M7. Should they be refreshed as part of closing this backlog item,
or is that separate work?

*Blocks:* nothing in the code; it does mislead every new reader, and it is
what made this specification's milestone framing ambiguous.
*Owner:* this repository.

Change Log
==========

- **2026-08-12** - Initial version. Derived from the code at commit
  ``6cb9cd1`` for backlog issue ``issue-a7e7db38``. Records M2 behavior as
  it exists today, including the M4-M6 extensions to M2 code paths.

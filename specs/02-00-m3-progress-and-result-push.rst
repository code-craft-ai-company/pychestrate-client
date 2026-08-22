==================================================
M3 - Progress Streaming and Result Push
==================================================

:Milestone: M3
:Status: Skeleton
:Design Authority: ``5-shared-results-backend`` (``pychestrate-service``
   repository) - NOT reproduced here
:Source of Truth (client behavior): ``progress_reporter.py``,
   ``result_uploader.py``
:Derived From: the modules above plus ``dispatch_consumer.py``,
   ``testbase_executor.py``, ``AGENTS.md``
:Last Verified: 2026-08-12, at commit ``6cb9cd1``

How To Read This Document
=========================

This is a **skeleton**, not an approved specification, for two reasons.

First, the design it depends on lives in another repository. The shared
results object store - how a run's result tree is addressed, which driver
bindings exist, how credentials are provisioned, and what the service does
with an uploaded tree - is specified by the **``5-shared-results-backend``**
design in the ``pychestrate-service`` repository. That document is the
authority. This one deliberately does not restate, summarize, or guess at
it; a paraphrase would drift and would then be cited as though it were the
design.

Second, the client-side code for M3 has already shipped (commit
``da4e547``, extended by M4's presigned-upload protocol in ``c615662``),
but it shipped without a specification and it contains behavior that
contradicts the rules recorded in ``AGENTS.md`` (see ``OQ-RP-001``). The
"As-Built Behavior" sections below therefore record what the code does
today, derived from the code and cited to it. They are **observations, not
ratified requirements** - which is precisely why several of them appear
again in the Open Questions section.

Do not treat an as-built observation in this document as a design decision
that has been reviewed. Answer the open questions first.

Purpose
=======

M3 closes the orchestration loop on the client side. Before it, the client
could take a job and run it, but the service learned nothing about the run
until the acknowledgement, and the results stayed on the pod. M3 adds two
flows:

**Progress** - while a job runs, the client streams incremental progress to
the service so an operator can watch a long run advance rather than waiting
hours for a terminal answer. Progress is explicitly best-effort.

**Result push** - when a job finishes, the client uploads its result tree to
the shared results backend and posts a completion manifest. This, not the
progress stream and not the dispatch acknowledgement, is the authoritative
signal that a run completed.

Scope
=====

- Live progress capture from a running ``codecraftai-testbase`` job, and its
  delivery to the service.
- Walking a finished result tree and transferring its bytes to the shared
  results backend.
- The completion manifest that terminates a run.
- The local run-record bookkeeping that records upload success or failure.
- The ordering of progress, upload, manifest and acknowledgement within one
  dispatch cycle.

Out of Scope
============

- The shared results backend design itself - object-store layout, key
  namespace, retention, driver bindings, credential provisioning. Owned by
  ``5-shared-results-backend`` in ``pychestrate-service``.
- The service-side ingestion of progress frames and result manifests.
- Build (M5) and provision (M6) progress and result push, which reuse the
  uploader but report progress locally only; noted here for contrast, not
  specified.
- Dispatch consumption and envelope verification - see
  ``01-00-m2-dispatch-and-verification.rst``.

Definitions
===========

Progress frame
    One ``ProgressFrame`` POST: a ``ProgressSummary`` roll-up over every
    record seen so far in the run, plus the batch of ``ProgressRecord``
    entries that appeared since the previous poll.

Result tree
    The directory ``codecraftai-testbase`` writes its run output into,
    ``<state_dir>/runs/<run_id>/output``. For M3 the whole tree is the unit
    of upload.

``JobResultsLocator``
    The block the signed envelope carries under ``results``, supplying
    ``backend_key_prefix`` and ``backend_driver``. Its meaning is defined by
    ``5-shared-results-backend``, not here.

Completion manifest
    The ``ResultManifest`` posted to ``results/complete``. Carries the
    terminal outcome, the backend locator, test counters, and a file index
    of the uploaded tree.

Best-effort
    A failure is logged and swallowed; the run continues and its outcome is
    unaffected. Progress is best-effort. Result push is not.

As-Built Behavior - Progress
============================

Derived from ``progress_reporter.py``. Observations, not ratified
requirements.

**Capture approach.** The client cannot register an in-process testbase
``ForwardingInfo`` HTTP-forwarding callback, because the testbase executor
shells out to the ``testbase`` console-script as a child process and that
mechanism is only reachable from inside the testbase process. The reporter
instead tails the incremental ``testrun_results_stream.jsos`` file testbase
writes into its output directory as each test scope completes, re-parsing it
with testbase's own ``jsosparser`` rather than re-implementing JSOS
record-separator handling (``progress_reporter.py:1-33``, ``154-189``).

**Lifecycle.** The dispatch consumer constructs a ``ProgressReporter``
pointed at the run output directory - resolvable before the run starts
because ``resolve_run_output_dir`` is deterministic - and hands it to the
executor. The executor calls ``start()`` immediately before launching the
subprocess and ``stop()`` in a ``finally`` once it exits
(``dispatch_consumer.py:431-445``; ``testbase_executor.py:466-475``,
``507-515``). ``start()`` runs the poll loop on a daemon thread named
``pychestrate-progress``; ``stop()`` signals it, joins with a
``poll_interval + 5.0`` timeout, and then performs one final synchronous
poll so records written between the last tick and process exit are not lost
(``progress_reporter.py:495-551``).

**Cursor.** The reporter keeps a count of records already forwarded. Each
cycle re-parses the whole stream file, and forwards only
``all_entries[already_posted:]``, stamping each with a ``record_index``
starting at the cursor (``progress_reporter.py:376-410``, ``301-333``).

**Frame contents.** ``ProgressSummary`` carries ``format_tag="testbase"``,
``tests_passed`` / ``tests_failed`` / ``tests_skipped`` / ``tests_errored``
tallied over every record seen so far, ``tests_total``, an RFC 3339
``recorded_at``, and the raw counts. Each ``ProgressRecord`` carries
``run_id``, ``client_id``, ``record_index``, ``test_name``, the testbase
``result`` code, the raw JSOS entry, and ``started_at`` / ``finished_at``
mapped from the JSOS ``start`` / ``stop`` fields
(``progress_reporter.py:192-298``).

**Delivery.** Frames go to ``progress.progress_post_frame``
(``POST /api/v1/runs/{run_id}/progress``). The endpoint answers HTTP 202
with an empty body, handled with the same empty-body-decode idiom the M2
paths use (``progress_reporter.py:336-373``; see ``OQ-DL-001`` in the M2
specification).

**Best-effort throughout.** A reporter that fails to start does not prevent
the run. A failed frame POST is logged at WARNING and swallowed. A poll
cycle that raises does not kill the thread. A failed final poll is logged
(``testbase_executor.py:466-475``; ``progress_reporter.py:364-370``,
``478-484``, ``534-543``).

**Partial writes.** A stream file observed mid-write can fail to parse; the
parser's ``ValueError`` is logged at DEBUG and the cycle is skipped, on the
assumption the next cycle sees a complete file
(``progress_reporter.py:173-183``).

**Other job kinds.** ``job_kind=build`` (M5) and ``job_kind=provision``
(M6) do not stream progress over the wire. Their reporters record state
transitions (``started`` / ``running`` / ``succeeded`` / ``failed``) into
the local build or provision record only, because ``ProgressFrame`` is
test-shaped - per-test records and pass/fail tallies - and forcing a
coarse build lifecycle into it would misrepresent it
(``build_progress_reporter.py:1-31``;
``provision_progress_reporter.py:1-32``).

As-Built Behavior - Result Push
===============================

Derived from ``result_uploader.py``. Observations, not ratified
requirements.

**Tree walk.** The uploader walks the result directory, recording for each
file a relative path normalized to forward slashes, its size, and its
SHA-256 digest, sorted by relative path for deterministic ordering. A
missing directory yields an empty list and a WARNING rather than an error
(``result_uploader.py:312-354``).

**Locator.** ``backend_key_prefix`` and ``backend_driver`` are read from the
verified envelope's ``results`` block, defaulting to empty strings when
absent (``result_uploader.py:605-627``).

**Transfer seam.** Byte transfer is abstracted behind an injectable
``ResultStoreDriver`` supplied via ``ResultUploaderAspects``, per the
Aspects pattern. A per-file driver overrides ``put_object`` and inherits a
``put_tree`` that loops over it; a batch driver overrides ``put_tree``
(``result_uploader.py:113-226``).

**Default driver (M4).** When no driver is wired, the uploader instantiates
``PresignedUploadDriver``. It batch-requests presigned PUT targets for every
relative path from ``results.artifacts_presign_upload``
(``POST /api/v1/runs/{run_id}/artifacts:presign``), then PUTs each file's
bytes to its ``put_url`` carrying the target's ``headers`` verbatim. A
missing target for a walked path, an absent ``put_url``, or a non-2xx PUT
raises ``HTTPRequestError`` (``result_uploader.py:389-554``,
``964-971``). This contradicts ``AGENTS.md``; see ``OQ-RP-001``.

**Manifest.** ``ResultManifest`` carries ``run_id``, ``client_id``,
``format_tag="testbase"``, the backend locator, an RFC 3339
``completed_at``, a ``terminal_outcome`` of ``success`` or ``failure``
driven by the run's own success, the four canonical testbase artifact
paths, test counters read from ``testrun_summary.json``'s ``detail`` map,
and an ``extras`` file index of every walked entry
(``result_uploader.py:557-602``, ``677-729``).

**Completion.** The manifest is posted to ``results.results_post_complete``
(``POST /api/v1/runs/{run_id}/results/complete``), which answers HTTP 202
(``result_uploader.py:732-757``).

**The manifest is posted even when the byte transfer failed**, annotated
with a note that the transfer was incomplete and the manifest is "posted for
reconciliation" (``result_uploader.py:973-981``). See ``OQ-RP-004``.

**Outcome recording.** The combined outcome is ``upload_complete`` only when
both the tree transferred and the manifest posted; otherwise
``upload_failed``. Either way it is merged as an ``upload`` section into
``<state_dir>/runs/<run_id>/run-record.json``
(``result_uploader.py:834-878``, ``992-1011``).

**Resilience.** A failed transfer or a failed completion POST is captured as
an ``upload_failed`` outcome, never raised - the client must not crash on a
bad upload (``result_uploader.py:653-666``, ``985-990``).

**Other job kinds.** Build (M5) and provision (M6) reuse the same walk,
driver and completion POST unchanged - an output directory is just a tree of
files - differing only in the manifest, which uses ``format_tag`` of
``build`` or ``provision`` and leaves the testbase-specific summary path and
test counters unset (``result_uploader.py:1021-1069``, ``1229-1278``).

Ordering Within One Dispatch Cycle
==================================

For a verified ``job_kind=test`` envelope
(``dispatch_consumer.py:412-470``):

1. Resolve the run output directory and construct the ``ProgressReporter``.
2. ``execute_job_envelope`` starts the reporter, runs testbase to
   completion, stops the reporter (final poll included), and writes the
   local run record.
3. ``upload_run_results`` walks the tree, transfers it, posts the manifest,
   and merges the upload outcome into the run record.
4. The dispatch is acknowledged with a note describing both the run outcome
   and the upload outcome.

The acknowledgement is therefore last, and reports the upload result. A
failed upload does not prevent the ack; it is reported inside it
(``dispatch_consumer.py:462-470``).

Interfaces
==========

REST operations consumed
------------------------

.. list-table::
   :header-rows: 1
   :widths: 30 34 36

   * - Purpose
     - Codegen'd method
     - Notes
   * - Post progress frame
     - ``progress.progress_post_frame``
     - HTTP 202; best-effort.
   * - Presign artifact uploads
     - ``results.artifacts_presign_upload``
     - M4; batch request over relative paths.
   * - Post completion manifest
     - ``results.results_post_complete``
     - HTTP 202; authoritative completion signal.

Envelope fields consumed
------------------------

``results.backend_key_prefix`` and ``results.backend_driver``, plus
``run_id`` and ``client_id``. The meaning of the locator fields is defined
by ``5-shared-results-backend``.

On-disk artifacts
-----------------

::

    <state_dir>/runs/<run_id>/
        output/                        - the result tree that is uploaded
            testrun_summary.json       - source of the manifest's counters
            testrun_results_stream.jsos - tailed for live progress
            testrun_results.json
            testsummary.html
            artifacts/ taskings/ tc-by-products/ diagnostics/
        run-record.json                - gains an "upload" section

Verification
============

Existing coverage under
``source/testroots/ccai/tests/pychestrate/client/``:

.. list-table::
   :header-rows: 1
   :widths: 46 54

   * - Test
     - Covers
   * - ``test_progress_reporter.test_parse_stream_records_handles_missing_file``
     - Stream file absent at run start
   * - ``test_progress_reporter.test_parse_stream_records_returns_only_test_entries``
     - JSOS filtering
   * - ``test_progress_reporter.test_translate_record_maps_jsos_fields_to_progress_record``
     - JSOS to ``ProgressRecord`` mapping
   * - ``test_progress_reporter.test_build_progress_frame_summarizes_and_batches``
     - Summary roll-up plus incremental batch
   * - ``test_progress_reporter.test_poll_and_post_progress_posts_only_new_records``
     - Cursor advance
   * - ``test_progress_reporter.test_post_progress_frame_failure_is_swallowed``
     - Best-effort delivery
   * - ``test_progress_reporter.test_progress_reporter_streams_records_across_start_stop``
     - Thread lifecycle and final poll
   * - ``test_result_uploader.test_walk_result_tree_enumerates_files_with_digests``
     - Tree walk, sizes, digests
   * - ``test_result_uploader.test_build_result_manifest_carries_counts_and_paths``
     - Manifest contents
   * - ``test_result_uploader.test_upload_result_tree_per_file_driver_uses_put_tree_default``
     - Driver seam
   * - ``test_result_uploader.test_presigned_driver_presigns_and_puts_every_file``
     - Presigned protocol happy path
   * - ``test_result_uploader.test_presigned_driver_rejects_non_2xx_put``
     - PUT failure classification
   * - ``test_result_uploader.test_upload_run_results_complete_path_posts_manifest``
     - End-to-end upload plus completion
   * - ``test_result_uploader.test_upload_run_results_presign_failure_marks_retry``
     - ``upload_failed`` marking
   * - ``test_result_uploader.test_upload_run_results_failed_completion_post_marks_retry``
     - ``upload_failed`` marking
   * - ``test_dispatch_consumer.test_handle_one_dispatch_streams_progress_uploads_and_completes``
     - Cycle ordering
   * - ``test_dispatch_consumer.test_handle_one_dispatch_upload_failure_still_acks``
     - Ack despite upload failure

The tests assert that a failed upload is *marked* for retry. No test - and
no production code - performs the retry itself; see ``OQ-RP-002``.

Open Questions
==============

These must be answered before this skeleton can become an approved
specification. Several concern behavior that has already shipped, which
means the answer may be "the code is wrong" rather than "write it down".

Design ownership
----------------

**OQ-RP-001** - ``AGENTS.md`` states, as a project-specific rule, that the
result uploader writes the result tree *directly* to the shared results
object store per ``5-shared-results-backend``, that the concrete
object-store binding and credentials are provisioned at the site or pod
level, that "there is no per-artifact HTTP upload endpoint on the REST
surface", and that "when no driver is wired the run is marked
``upload_failed`` locally for retry".

The shipped code does none of these things on its default path. M4 added
``POST /api/v1/runs/{run_id}/artifacts:presign`` - a per-artifact HTTP
upload path on the REST surface - and made ``PresignedUploadDriver`` the
default, so a run with no site driver wired uploads successfully over HTTP
rather than being marked ``upload_failed``
(``result_uploader.py:964-966``).

Which is current: the ``AGENTS.md`` rule, or the M4 code? Does
``5-shared-results-backend`` sanction the presigned-URL protocol as an
alternative transfer mechanism, or did the client diverge from the design?

*Blocks:* essentially all of this specification's result-push half.
Until it is answered the client's upload path cannot be described as
conforming to anything.
*Owner:* ``pychestrate-service`` (``5-shared-results-backend``), then this
repository to reconcile ``AGENTS.md``.

**OQ-RP-005** - ``PresignedUploadDriver.put_tree`` accepts
``backend_driver`` and ``backend_key_prefix`` but ignores both, documenting
them as "carried for parity" because "the transfer is fully described by
the presigned targets" (``result_uploader.py:500-518``). The envelope's
``JobResultsLocator`` - the client-visible surface of
``5-shared-results-backend`` - therefore has no effect on the default
upload path; the server decides placement when it mints the targets.

Is the locator still meaningful to the client at all, or is it now
server-side-only information that happens to be carried in the envelope? It
is still copied into the manifest (``result_uploader.py:713``), so the
service receives back a prefix the client never used.

*Blocks:* whether the envelope should keep carrying the locator, and
whether a site-provided driver honoring the prefix can coexist with
server-minted targets.
*Owner:* ``pychestrate-service`` (``5-shared-results-backend``).

Client behavior gaps
--------------------

**OQ-RP-002** - The retry that the code's own docstrings promise does not
exist. ``upload_failed`` is written onto the run record so "the next ``run``
invocation can retry" (``result_uploader.py:36-41``, ``834-848``), and
``run_needs_upload_retry`` exists to detect it
(``result_uploader.py:881-916``) - but nothing in
``source/packages/`` calls it. Its only callers are tests. The dispatch
consumer never scans the runs directory for owed uploads at start-up or
between polls.

So a run whose upload fails is recorded, reported in the ack note, and then
never retried; its results stay on the pod indefinitely. What is the
intended retry trigger - a start-up sweep of the runs directory, a periodic
sweep in the consumer loop, an in-cycle retry with back-off, or a service
-driven re-request?

*Blocks:* the durability claim for result push. Today a transient network
failure at the wrong moment silently loses a run's results to the service.
*Owner:* this repository, informed by whether the service can re-request
results for a run.

**OQ-RP-003** - Progress records are lost permanently when a frame POST
fails. ``poll_and_post_progress`` advances the cursor to
``len(all_entries)`` regardless of whether ``post_progress_frame`` returned
``True``, and its return value is discarded
(``progress_reporter.py:407-410``). The next cycle therefore starts after
the records the failed frame carried, and they are never re-sent.

Given progress is best-effort by design, is permanent loss of a batch
acceptable, or should the cursor advance only on a successful POST so a
transient failure self-heals on the next tick? The latter costs nothing -
the whole stream file is re-parsed each cycle anyway.

*Blocks:* whether an operator watching a long run can trust the progress
view to be complete-so-far, or only monotonic.
*Owner:* this repository.

**OQ-RP-004** - A completion manifest is posted even when the result-tree
bytes never landed, carrying a note that the transfer was incomplete
(``result_uploader.py:973-981``). How does the service treat such a
manifest? If ``results/complete`` is the authoritative run-completion
signal, a run whose artifacts do not exist has just been marked complete,
with the only indication being prose in a free-form ``notes`` field. Is
there a structured way to say "terminal outcome known, artifacts missing" -
or should the client withhold the manifest until the bytes land, given that
``OQ-RP-002`` would then owe it a retry?

*Blocks:* whether the service can distinguish a complete run from a
complete-but-artifactless one without string matching.
*Owner:* ``pychestrate-service`` (``ResultManifest`` schema).

**OQ-RP-006** - Each artifact is read fully into memory before being PUT
(``result_uploader.py:483-486``), and each testbase run's captured
subprocess output is separately held in memory (see ``OQ-EX-002`` in the M2
specification). A diagnostics tree or a large captured log could be
substantial. Should the PUT stream from the file handle instead? Relatedly,
there is no chunking, no resume, and no parallelism across artifacts: a
failure on the last file of a large tree re-transfers everything on any
future retry.

*Blocks:* upload behavior for realistically-sized result trees. No size
limit is documented anywhere in this repository.
*Owner:* this repository, with any object-store constraints from
``5-shared-results-backend``.

**OQ-RP-007** - ``PresignedUploadDriver`` reaches into the REST client's
private ``_session`` attribute to perform the PUT, and calls
``authenticate()`` when it is ``None`` (``result_uploader.py:479-481``).
The same private access appears in ``signing_keys.refresh_signing_keys``
(``signing_keys.py:135-136``). Should ``codecraftai-restclient-base`` expose a
public accessor for the authenticated session, given two independent call
sites now need one?

*Blocks:* nothing functionally; it is a layering violation that will break
silently if the base client renames the attribute.
*Owner:* ``codecraftai-restclient-base``, then this repository.

Schema and contract questions
-----------------------------

**OQ-RP-008** - ``TerminalOutcome`` is used with only ``success`` and
``failure``, derived from the testbase process exit code
(``result_uploader.py:105-106``, ``956-959``). A run that timed out, was
killed by a signal, or never started because the ``testbase`` binary was
missing all report ``failure``. Does the enum carry other values
(``timed_out``, ``cancelled``, ``aborted``), and should the client
distinguish them? The distinction is available on the client side - the
executor already separates these cases into distinct ``detail`` strings
(``testbase_executor.py:491-506``).

*Blocks:* whether the service can report *why* a run failed without parsing
prose.
*Owner:* ``pychestrate-service`` (``TerminalOutcome`` enum).

**OQ-RP-009** - ``ProgressRecord.record_index`` is a client-side count of
records forwarded so far for the run
(``progress_reporter.py:323-331``). Is it required to be gap-free and
monotonic from the service's perspective? Under ``OQ-RP-003`` it stays
monotonic but can contain gaps after a failed POST. If the service uses it
for ordering or de-duplication, gaps may matter; if it is advisory, they may
not. Relatedly, is a re-sent frame (which cannot happen today, but would
under a fix to ``OQ-RP-003``) expected to be idempotent server-side?

*Blocks:* the fix for ``OQ-RP-003``.
*Owner:* ``pychestrate-service`` (progress ingestion).

**OQ-RP-010** - Frames are posted on a fixed interval, default 5 seconds
(``progress_reporter.py:70``), with no batching cap: a run that completes
thousands of tests in one interval posts them all in a single frame, and a
long run posts a frame every 5 seconds for hours. Are there service-side
rate limits or a maximum frame size the client should respect? Should the
interval back off for long-running jobs?

*Blocks:* pod-fleet load characterization. With N pods running long jobs
the service sees N frames per 5 seconds indefinitely.
*Owner:* ``pychestrate-service``.

**OQ-RP-011** - Manifest test counters are read from
``testrun_summary.json``'s ``detail`` map, and a missing or unparseable
summary yields all-zero counters with the manifest still posted, on the
stated assumption that "the server's result-tree parser is the authority on
the full result set" (``result_uploader.py:557-602``). Is that assumption
correct - does the service re-parse the uploaded tree and treat the
manifest counters as advisory? If it instead trusts the manifest, a run
whose summary file is missing would be reported as zero tests rather than
as unknown.

*Blocks:* whether all-zero counters are safe to send.
*Owner:* ``pychestrate-service`` (result ingestion).

**OQ-RP-012** - Build and provision jobs report progress only into local
records, pending "the service exposing a build-shaped progress frame"
(``build_progress_reporter.py:20-29``;
``provision_progress_reporter.py:21-31``). Is a kind-neutral or
build-shaped progress frame planned? Until then an operator watching a
build sees nothing until it completes.

*Blocks:* M5/M6 progress parity. Out of scope for this document but noted
because it is the same service-side schema decision.
*Owner:* ``pychestrate-service``.

Change Log
==========

- **2026-08-12** - Initial skeleton. Created for backlog issue
  ``issue-a7e7db38``. Records the as-built M3/M4 client behavior at commit
  ``6cb9cd1`` as observations, names ``5-shared-results-backend`` as the
  design authority without restating it, and raises twelve open questions -
  the most consequential being ``OQ-RP-001`` (``AGENTS.md`` and the shipped
  upload path contradict each other) and ``OQ-RP-002`` (the documented
  upload retry has no implementation).

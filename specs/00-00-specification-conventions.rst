==================================================
Specification Conventions and Index
==================================================

:Status: Approved
:Applies To: every file in ``<repository>/specs``
:Last Reviewed: 2026-08-12

This document defines the specification format used in this repository and
indexes the specifications that exist. ``AGENTS.md`` directs agents to read
``<repository>/specs`` for the specification files that steer development
work; this file is the entry point for that folder.

Why This Folder Exists
======================

The org repository convention calls for a ``specs`` folder for spec-based
development, and this repository's ``AGENTS.md`` tells agents to look there.
Until these documents were written the folder did not exist, and the only
design references for the client runtime were prose in ``AGENTS.md`` and
``README.rst``. Behavior that had already shipped was therefore recorded
nowhere an agent could treat as authoritative.

These specifications close that gap. They are written to two rules:

1. **For shipped behavior, record what IS.** A specification covering
   shipped code is derived from the code, and cites the module and line
   range each requirement came from. Where the code's intent is unclear,
   the specification records an explicit open question instead of guessing.
2. **For unbuilt work, do not invent the design.** A skeleton specification
   names the authoritative design document, states what the client side is
   known to need, and lists open questions. It does not fabricate a design
   the owning repository has not published.

File Naming
===========

Specification files follow the same numbered-prefix convention the
``userguide`` folder uses (``userguide/01-00-quickstart.rst``)::

    specs/
        00-00-specification-conventions.rst   - this file
        NN-MM-<kebab-case-slug>.rst           - one specification per file

``NN`` groups related specifications; ``MM`` orders documents inside a
group. Files are reStructuredText, matching ``README.rst`` and the
``userguide`` documents.

Required Sections
=================

Every specification carries the following sections in this order. A section
with nothing to say is retained with an explicit "None." so a reader can
tell the difference between "considered and empty" and "not thought about".

1. **Status block** - a field list carrying ``Milestone``, ``Status``,
   ``Source of Truth``, ``Derived From`` and ``Last Verified``.
2. **Purpose** - what the specified behavior is for, in a paragraph.
3. **Scope** and **Out of Scope** - explicit boundaries.
4. **Definitions** - terms and identifiers used by the rest of the document.
5. **Requirements** - numbered normative statements (see below).
6. **Behavioral Flows** - the ordered steps the runtime actually takes.
7. **Interfaces** - REST operations, on-disk layout, CLI surface.
8. **Error Handling** - what fails, how it is classified, what survives.
9. **Verification** - the tests that cover the requirements.
10. **Open Questions** - numbered, each with what it blocks (see below).
11. **Change Log** - dated entries.

Status Vocabulary
=================

``Draft``
    Under active authoring. Not yet a reliable steer for development work.

``Skeleton``
    Structure and open questions are recorded; the design is owned
    elsewhere or is not yet settled. A skeleton MUST NOT be treated as a
    green light to implement the unspecified parts.

``Shipped-Documented``
    Records behavior that already exists in the code. The code is the
    source of truth; this document is a derived, verifiable description of
    it. A discrepancy between the two is a defect in this document unless
    the document explicitly marks the code behavior as a defect.

``Approved``
    Settled and normative for work that has not been built yet.

``Superseded``
    Replaced. The replacing document is named in the status block.

Requirement Identifiers
=======================

Requirements are numbered with a stable identifier of the form
``<MILESTONE>-<AREA>-<NNN>``, for example ``M2-DV-004``. Identifiers are
never renumbered or reused; a withdrawn requirement is retained with a
``(WITHDRAWN)`` marker so cross-references stay resolvable.

Requirements use these keywords:

``MUST`` / ``MUST NOT``
    An absolute requirement. For a ``Shipped-Documented`` specification,
    this describes behavior the code enforces today.

``SHOULD``
    A strong recommendation with room for a documented exception.

``MAY``
    An option genuinely left to the caller or operator.

Each requirement cites its source as ``file:line-range``.

Open Questions
==============

Open questions are numbered ``OQ-<AREA>-<NNN>`` and each records:

- the question itself, stated so it can be answered yes/no or with a value,
- **Blocks:** what cannot be correctly specified or built until it is
  answered,
- **Owner:** the repository or role that can answer it.

An open question is the correct output when the code's intent is ambiguous
or when the design lives in another repository. Inventing an answer is not.

Cross-Repository Design References
==================================

Some designs that constrain this client are owned by the Pychestrate
service repository, not this one. They are referenced by name and are NOT
reproduced or paraphrased here, because a paraphrase drifts:

``5-shared-results-backend``
    The shared results object-store design (``pychestrate-service``
    repository). It governs how a run's result tree is addressed and stored,
    and is the authority for the ``JobResultsLocator``
    (``backend_key_prefix`` + ``backend_driver``) the signed envelope
    carries. Referenced by ``AGENTS.md`` and by
    ``02-00-m3-progress-and-result-push.rst``.

``jobs.envelope.schema`` / ``jobs.envelope.canonical`` / ``jobs.envelope.signer``
    The service-side job envelope schema, its canonical serialization, and
    its Ed25519 signer. The client re-derives canonical payload bytes and
    mirrors the signer's reason codes, so these are the authority for
    ``01-00-m2-dispatch-and-verification.rst``.

Index
=====

.. list-table::
   :header-rows: 1
   :widths: 34 12 54

   * - Specification
     - Status
     - Covers
   * - ``01-00-m2-dispatch-and-verification.rst``
     - Shipped-Documented
     - M2 dispatch long-poll, signed-envelope verification, testbase
       invocation, dispatch acknowledgement.
   * - ``02-00-m3-progress-and-result-push.rst``
     - Skeleton
     - M3 live progress streaming and result-tree push + completion
       manifest. Design authority is ``5-shared-results-backend``.

Note on Milestone State
=======================

The repository's own documents disagree about which milestones have
shipped, and readers of these specifications should know that before
trusting any single source:

- ``README.rst`` describes M2 as "this release" and M3 as "(planned)".
- ``userguide/01-00-quickstart.rst`` describes M1 only, and says dispatch
  and result reporting "will be added in M2 and M3".
- ``AGENTS.md`` states that M3 delivered progress streaming, result-tree
  upload, and the completion manifest.
- The code and the git history contain merged work through M7
  (``da4e547`` M3, ``c615662`` M4, ``121a5be`` M5, ``f7c4150`` M6,
  ``4fbfba0`` M7).

The code is the source of truth. ``README.rst`` and
``userguide/01-00-quickstart.rst`` are stale on this point; see
``OQ-DOC-001``.

Change Log
==========

- **2026-08-12** - Initial version. Created the ``specs`` folder, defined
  the format, and seeded it with the M2 and M3 specifications
  (backlog issue ``issue-a7e7db38``).

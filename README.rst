==================================================
Pychestrate Client (Pod-Side Runtime)
==================================================

The ``pychestrate-client`` package is the pod-side runtime for the
Pychestrate test orchestration system. An operator installs it on a
pod's client machine where it enrolls with the central Pychestrate
service, maintains a heartbeat, fetches dispatched jobs via long-poll,
shells out to ``codecraftai-testbase`` to run those jobs, and pushes
progress + results back to the service.

This package is the **client runtime** (always referred to as "client",
never "agent"). For the pure REST API wrapper used by the runtime and
other consumers, see ``codecraftai-restclient-pychestrate``. For the
service itself, see ``pychestrate-service``.

Highlights
==========

- mTLS enrollment via a one-time bootstrap token.
- Periodic heartbeat against the Pychestrate service registry.
- Local persistent state under ``~/.pychestrate-client`` (configurable).
- Signed job envelopes verified against the service's RFC 7517 JWK Set,
  fetched and cached locally; an installed key is an operator override.
- Built on top of the codegen'd ``ccai.pychestrate.rest.client``
  package, so the runtime never hand-rolls HTTP against the service.

Milestone Scope
===============

- **M1** - enrollment + heartbeat.
- **M2 (this release)** - long-poll dispatch consumption, signed-envelope
  verification, and testbase invocation. The ``run`` subcommand drives the
  heartbeat loop and the dispatch-consumer loop concurrently.
- **M3 (planned)** - progress + result push.

Envelope signing keys
=====================

The dispatch consumer verifies the Ed25519 signature on every job envelope
before executing it and refuses to run an envelope it cannot verify.

The service publishes its envelope signing public keys as an RFC 7517 JWK
Set (JWKS) of OKP / Ed25519 keys. Each JWK carries a ``kid`` matching the
``signature_key_id`` stamped on every dispatched envelope. Retrieving that
JWK Set is the first-class way the client obtains a verification key: it is
fetched through the codegen'd REST client, normalized to PEM, and cached in
the state directory as ``envelope-signing-jwks.json`` with the time it was
fetched. The cache is served for one hour by default, after which the
client re-fetches so a service-side key rotation is picked up.

Key resolution order
--------------------

For a dispatched envelope's ``signature_key_id``, the client resolves a
verification key in this order:

1. **Operator-installed key** - the key installed with
   ``pychestrate-client install-signing-key``. Its presence is an explicit
   override: it pins verification to that one key, the service JWK Set is
   not consulted at all, and an envelope signed by any other key id is
   refused.
2. **Fresh cached JWK Set** - ``envelope-signing-jwks.json``, while it is
   within its time-to-live.
3. **Enrollment-bundled signing-key set** - the set the service delivered
   in the enrollment-redeem response (``envelope-signing-keys.json``).
   Checked before any network call because it is local and already
   service-delivered.
4. **Live JWK Set fetch** - one fetch from the service's JWKS endpoint,
   written into the cache. This is what picks up a key rotation.
5. **Stale cached JWK Set** - a cached key whose id matches but whose cache
   aged out, served only because the live fetch did not resolve the id. A
   pod should not stop running jobs because the JWKS endpoint blipped, and
   an Ed25519 public key does not expire.
6. **Pre-JWKS signing-keys endpoint** - a refresh from
   ``GET /api/v1/envelope-signing-keys``, retained so clients pointed at a
   service without a JWKS endpoint keep working.

If no source resolves the key id, the envelope is **refused without being
executed** and the dispatch is acked with a rejection note. The client
never runs a job it cannot verify.

A transport failure reaching the service is not a rejection: it propagates
so the dispatch-consumer loop backs off and retries rather than
permanently refusing a legitimate job.

Pinning a pod to one signing key
--------------------------------

For an air-gapped pod, or to pin a pod to a specific signing key::

   pychestrate-client install-signing-key --key-file <pem> --key-id <id>

While that key is installed it is the only trust anchor. Delete
``<state-dir>/envelope-signing-public-key.pem`` to return to JWKS-based
resolution.

.. note::

   Upgrading a pod that already has a key installed: the installed key now
   takes effect as a pin. If the service has rotated its signing key since
   the install, that pod will refuse the newly-signed envelopes until the
   current key is installed or the key file is removed. Pods that never ran
   ``install-signing-key`` are unaffected.


Repository Structure
====================

- ``development/`` - Runtime environment scripts.
- ``repository-setup/`` - Repository homing and setup scripts.
- ``source/packages/`` - Package source code (``ccai.pychestrate.client.*``).
- ``source/testroots/ccai/`` - TestBase test roots.
- ``specs/`` - Specifications for spec-based development. Start at
  ``specs/00-00-specification-conventions.rst``.
- ``userguide/`` - Standards, conventions, and quick-start docs.

Environment Setup
=================

When working in a Codex container:

.. code-block:: bash

   ./development/setup-agent-container

For local development:

.. code-block:: bash

   ./repository-setup/rehome-repository
   ./development/setup-environment

Quick Start
===========

Get an enrollment token from the Pychestrate admin UI, then:

.. code-block:: bash

   pychestrate-client enroll --service-url https://pychestrate.example.com \
       --token <bootstrap-token> --name my-client

After enrollment, start the heartbeat loop:

.. code-block:: bash

   pychestrate-client heartbeat

Runtime state is persisted under ``~/.pychestrate-client/`` by default
(override with ``--state-dir``).

Running Tests
=============

Activate the virtual environment:

.. code-block:: bash

   source .venv/bin/activate

Run TestBase tests:

.. code-block:: bash

   testbase testing run --root source/testroots/ccai \
       --includes=ccai.tests.pychestrate.client --output=./.output

Standards and Conventions
=========================

- Coding standards follow the Automation Mojo organization standing guidance
  (the org-level ``CLAUDE.md`` that applies to every ``codecraftai-*``
  repository). Repository-local standards and convention documents belong in
  ``userguide/``; see ``AGENTS.md`` for the short form of the naming rules.
- The package namespace is ``ccai.pychestrate.client.*`` (product-first).
- The runtime is "client", never "agent".

Related Packages
================

- ``pychestrate-service`` - the central service this client enrolls with.
- ``codecraftai-restclient-pychestrate`` - codegen'd REST wrapper consumed here.
- ``codecraftai-testbase`` - the test framework invoked for dispatched jobs (M2).

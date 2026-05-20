==================================================
Pychestrate Client (Pod-Side Runtime)
==================================================

The ``pychestrate-client`` package is the pod-side runtime for the
Pychestrate test orchestration system. An operator installs it on a
pod's client machine where it enrolls with the central Pychestrate
service, maintains a heartbeat, fetches dispatched jobs via long-poll,
shells out to ``automojo-testbase`` to run those jobs, and pushes
progress + results back to the service.

This package is the **client runtime** (always referred to as "client",
never "agent"). For the pure REST API wrapper used by the runtime and
other consumers, see ``automojo-restclient-pychestrate``. For the
service itself, see ``pychestrate-service``.

Highlights
==========

- mTLS enrollment via a one-time bootstrap token.
- Periodic heartbeat against the Pychestrate service registry.
- Local persistent state under ``~/.pychestrate-client`` (configurable).
- Built on top of the codegen'd ``automojo.pychestrate.rest.client``
  package, so the runtime never hand-rolls HTTP against the service.

Milestone Scope
===============

- **M1** - enrollment + heartbeat.
- **M2 (this release)** - long-poll dispatch consumption, signed-envelope
  verification, and testbase invocation. The ``run`` subcommand drives the
  heartbeat loop and the dispatch-consumer loop concurrently.
- **M3 (planned)** - progress + result push.

Envelope signing key
====================

The dispatch consumer verifies the Ed25519 signature on every job envelope
before executing it and refuses to run an envelope it cannot verify. The
Pychestrate REST API currently exposes no JWKS / signing-public-key
endpoint, so the operator installs the service's signing public key
out-of-band::

   pychestrate-client install-signing-key --key-file <pem> --key-id <id>

Repository Structure
====================

- ``development/`` - Runtime environment scripts.
- ``repository-setup/`` - Repository homing and setup scripts.
- ``source/packages/`` - Package source code (``automojo.pychestrate.client.*``).
- ``source/testroots/automojo/`` - TestBase test roots.
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

   testbase testing run --root source/testroots/automojo \
       --includes=automojo.tests.pychestrate.client --output=./.output

Standards and Conventions
=========================

- Coding standards live at ``/Users/myron/repos/CLAUDE.md``.
- The package namespace is ``automojo.pychestrate.client.*`` (product-first).
- The runtime is "client", never "agent".

Related Packages
================

- ``pychestrate-service`` - the central service this client enrolls with.
- ``automojo-restclient-pychestrate`` - codegen'd REST wrapper consumed here.
- ``automojo-testbase`` - the test framework invoked for dispatched jobs (M2).

================================
Pychestrate Client - Quick Start
================================

This document describes how a pod operator brings a new ``pychestrate-client``
into service. It covers M1 functionality only (enrollment + heartbeat).
Dispatch consumption and result reporting will be added in M2 and M3.

Step 1 - Obtain an Enrollment Token
====================================

An administrator with access to the Pychestrate admin UI (or the
``/api/v1/enrollment-tokens`` REST endpoint) creates a one-time bootstrap
token and delivers the secret to the operator out-of-band.

Step 2 - Enroll the Client
===========================

On the pod's client machine, run the ``enroll`` subcommand:

.. code-block:: bash

   pychestrate-client enroll \
       --service-url https://pychestrate.example.com \
       --token <bootstrap-token> \
       --name my-pod-client

The runtime will:

1. Create the state directory at ``~/.pychestrate-client/`` (mode 0700).
2. Generate a fresh RSA-2048 keypair and a CSR with ``CN=<name>``.
3. POST the CSR + token to ``/api/v1/enrollment-tokens/redeem``.
4. Persist the signed client certificate, the service CA chain, the
   private key, the assigned ``client_id``, and the JSON configuration
   into the state directory.

On success, the CLI prints a JSON object summarizing the persisted paths
and the assigned ``client_id``.

To override the state directory location, pass ``--state-dir``:

.. code-block:: bash

   pychestrate-client --state-dir /var/lib/pychestrate-client enroll ...

For local development with a self-signed Pychestrate service, the
``--insecure`` flag disables TLS verification for the single redeem call.
Once enrollment completes the service CA chain is pinned for all future
heartbeats, so this flag does NOT affect subsequent operations.

Step 3 - Start the Heartbeat Loop
==================================

Once enrolled, start the heartbeat:

.. code-block:: bash

   pychestrate-client heartbeat

The runtime will ping ``GET /api/v1/clients/{client_id}/heartbeat`` every
30 seconds (override with ``--interval <seconds>``). Failures trigger an
exponentially-doubling back-off (base 5s, capped at 300s). The loop
shuts down gracefully on ``SIGTERM`` or ``SIGINT``.

In M2 and M3 the ``heartbeat`` subcommand will be folded into a full
``run`` subcommand that also long-polls for dispatched jobs, shells out
to ``automojo-testbase`` to execute them, and pushes progress + results
back to the service.

State Directory Layout
======================

After successful enrollment, the state directory contains::

    ~/.pychestrate-client/
        client_id        - the assigned client UUID (text)
        client.key       - PEM-encoded private key (mode 0600)
        client.crt       - PEM-encoded signed client certificate
        service-ca.crt   - PEM-encoded service CA chain
        config.json      - service URL, host, scheme, port, client name

The ``client.key`` file is the only piece of sensitive material on disk.
The directory itself is created with mode 0700 to keep it out of reach of
other local users.

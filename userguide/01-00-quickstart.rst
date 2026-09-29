===================================
Pychestrate Client - Enroll a Pod
===================================

This guide takes a machine from nothing to a pod that takes work from a
Pychestrate service. It is the one place that describes the whole path, and
the token page in the Pychestrate admin UI follows it: install the client,
enroll it with a token, run it, keep it running as a service, and upgrade it.

.. contents::
   :local:
   :depth: 1


Before you start
================

You need:

- **A Pychestrate service reachable over HTTPS.** Every call except
  ``enroll`` is mutual TLS: the service's front end (nginx) checks the
  client certificate the pod received at enrollment. A service reached over
  plain HTTP, such as a development server, accepts ``enroll`` but answers
  every later call with ``401`` and the detail ``Mutual-TLS client
  certificate required``. The operator runbook in ``pychestrate-service``
  (``userguide/13-00-alpha-deployment.rst``, "Put nginx in front") describes
  the front end.
- **A pod machine.** Linux, Python 3.10 or newer, and whatever the jobs need:
  ``git`` and ``make`` for builds, and the ``testbase`` command for test
  jobs. The client starts ``testbase`` by name, so it must be on the ``PATH``
  of the process that runs the client (see `Run it as a service`_).
- **Access to the private package index** that carries the ``codecraftai-*``
  packages this client depends on. ``pychestrate-service``'s
  ``userguide/02-00-getting-started.rst`` explains how to get it.
- **An administrator** who can open **Admin**, **Enrollment Tokens**.


Step 1 - Install the client
===========================

Install it into its own virtual environment so its dependencies stay apart
from the system Python:

.. code-block:: bash

   sudo python3 -m venv /opt/pychestrate-client
   sudo /opt/pychestrate-client/bin/pip install \
       --extra-index-url https://code-craft.ai/licensed-repository/simple/ \
       /path/to/pychestrate-client

Replace ``/path/to/pychestrate-client`` with a checkout of this repository,
or with the package name if your organization publishes the client to a
private index (``make dist-publish REPOSITORY=<name>``, see the Makefile).
The package is proprietary; it is never uploaded to a public index.

Installing creates the ``pychestrate-client`` command inside the environment's
``bin`` directory. It is on your ``PATH`` only if you put that directory
there, so the rest of this guide uses the full path. Check it works:

.. code-block:: bash

   /opt/pychestrate-client/bin/pychestrate-client version

Create a user for it and a directory for its state. The state directory holds
the pod's private key, so only that user may read it:

.. code-block:: bash

   sudo useradd --system --home-dir /var/lib/pychestrate-client pychestrate
   sudo install -d -o pychestrate -g pychestrate -m 0700 /var/lib/pychestrate-client


Step 2 - Mint an enrollment token
=================================

In the Pychestrate web UI, open **Admin**, **Enrollment Tokens** and choose
to mint a token:

- **Project** is the project whose jobs the pod may run.
- **Pod label** is the name of this machine. Choose it now: the token is
  bound to it, and the client must enroll under exactly this label.

The page then shows the secret once, with the commands to run. Copy them
before you leave the page. The secret cannot be shown again, and it works
once.


Step 3 - Enroll
===============

On the pod, run the command the token page printed, as the ``pychestrate``
user and with the state directory from step 1:

.. code-block:: bash

   sudo -u pychestrate /opt/pychestrate-client/bin/pychestrate-client \
       enroll \
       --state-dir /var/lib/pychestrate-client \
       --server-url https://pychestrate.example.com \
       --token-file /root/pod-token.txt \
       --name <pod label>

Put the token in a file (``--token-file -`` reads it from standard input) or
in the ``PYCHESTRATE_ENROLLMENT_TOKEN`` environment variable. ``--token
<secret>`` also works, but a token on the command line is visible in the
process list and in your shell history, and the client says so.

``--state-dir``, ``--verbose``, ``--quiet`` and ``--log-format`` are global
options and may go before or after the command. Without ``--state-dir`` the
state goes to ``$XDG_DATA_HOME/pychestrate-client`` (normally
``~/.local/share/pychestrate-client``) of whoever runs the command. A pod that
was enrolled by an earlier release keeps using ``~/.pychestrate-client`` for as
long as that directory exists and the new one does not, so nothing has to be
moved.

``--server-url`` is the same option that ``pyc login --server-url`` takes.
The older spelling ``--service-url`` still works; the token page in the service
still prints it, so that its command also runs on a pod with an older client.
``pychestrate-client --version`` prints the version, and ``python -m
ccai.pychestrate.client`` runs the same command line.

``enroll`` generates a private key and a certificate request on the pod,
sends the request and the token to the service, and stores what comes back.
On success it prints ``enrollment succeeded`` and a JSON summary with the new
``client_id`` (add ``--json`` to print only the JSON). On failure it prints
one line, ``enrollment failed: ...``, with the reason, and exits with status
2. It waits 30 seconds for the service (``--timeout <seconds>`` changes that).

A state directory that is already enrolled is refused, since a new identity
would silently replace its client id and key: add ``--force`` to replace it on
purpose, or use another ``--state-dir``.

A token is spent by a successful ``enroll``. A rejection for the wrong pod
name does not spend it, so run ``enroll`` again with the right name.

Then check the pod:

.. code-block:: bash

   pychestrate-client --state-dir /var/lib/pychestrate-client doctor

``doctor`` lists the enrollment files, how long the client certificate has
left, whether the private key is readable only by its owner, and whether the
service answers. It exits 1 when something failed and 0 otherwise (warnings
are allowed); ``--offline`` skips the call to the service.

What ``enroll`` leaves in the state directory:

.. list-table::
   :header-rows: 1
   :widths: 30 12 58

   * - File
     - Mode
     - Holds
   * - ``client_id``
     - 0644
     - The client's id, as text.
   * - ``client.key``
     - 0600
     - The private key (PEM). It never leaves the pod.
   * - ``client.crt``
     - 0644
     - The client certificate (PEM). Its common name is the client id, and the
       service issues it for 365 days by default.
   * - ``service-ca.crt``
     - 0644
     - The certificate authority chain that signed the client certificate.
   * - ``config.json``
     - 0644
     - The service URL and the pod label.
   * - ``envelope-signing-keys.json``
     - 0644
     - The public keys the service signs job envelopes with.

Later runs add:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - File
     - Holds
   * - ``server-trust.pem``
     - The certificates the client trusts the service's TLS certificate
       against: the public roots plus ``service-ca.crt``. It is rewritten
       when ``service-ca.crt`` changes.
   * - ``envelope-signing-jwks.json``
     - A cache of the key set the service publishes, refreshed hourly so a
       key rotation is picked up.
   * - ``runs/``
     - A record of each job the client ran.
   * - ``envelope-signing-public-key.pem``
     - Only if you ran ``install-signing-key``. It pins the pod to one
       signing key; see "Envelope signing keys" in the README.

``client.crt`` and ``service-ca.crt`` are ordinary PEM files, so you can read
them with ``openssl x509 -in <file> -noout -subject -dates``.


Step 4 - Run it
===============

``run`` is the normal pod mode. It sends a heartbeat every 30 seconds and
long-polls the service for jobs, runs each job it is given, and sends back
live progress and the result files:

.. code-block:: bash

   sudo -u pychestrate /opt/pychestrate-client/bin/pychestrate-client \
       --state-dir /var/lib/pychestrate-client run

Within a minute the client appears under **Clients** in the web UI as
*Alive*, and its pod under **Pods** stops saying *Declared, waiting for a
client*. A client the service has not heard from for 90 seconds shows as
*Stale*, and after 5 minutes as *Lost*; a heartbeat brings it back.

A job now dispatched to the pod runs here. ``run`` accepts
``--interval <seconds>`` for the heartbeat and ``--poll-timeout <seconds>``
for how long each poll waits (30 by default). Add ``--verbose`` for debug
logging, ``--quiet`` for warnings only, or ``--log-format json`` for one JSON
object per log line. The ``heartbeat`` subcommand runs only the heartbeat, for
a machine that must show as alive but must not take work; ``heartbeat --once``
sends a single heartbeat and exits 0 when the service accepted it, which is
handy in a monitoring check.

Stopping ``run`` with ``SIGTERM`` or Ctrl+C is graceful. The client takes no
new work, lets a job that is running finish and report, waits for the poll
in flight, and exits 0. A job handed out by that last poll is rejected back
to the service instead of started. A second signal exits at once with status
130 and abandons the job.


Run it as a service
===================

The client does not restart itself, so on a pod that should take work
unattended, run it under a service manager. This is a systemd unit for the
layout used above. Save it as ``/etc/systemd/system/pychestrate-client.service``:

.. code-block:: ini

   [Unit]
   Description=Pychestrate pod client
   Wants=network-online.target
   After=network-online.target

   [Service]
   Type=simple
   User=pychestrate
   Group=pychestrate
   # testbase, git and make are found through PATH, so put the client
   # environment's bin directory on it.
   Environment="PATH=/opt/pychestrate-client/bin:/usr/local/bin:/usr/bin:/bin"
   ExecStart=/opt/pychestrate-client/bin/pychestrate-client \
       --state-dir /var/lib/pychestrate-client run
   Restart=on-failure
   RestartSec=10
   # A stop waits for the job in progress. Set this to the longest wait you
   # accept; a job still running when it expires is killed.
   TimeoutStopSec=300

   [Install]
   WantedBy=multi-user.target

Then start it and follow its log:

.. code-block:: bash

   sudo systemctl daemon-reload
   sudo systemctl enable --now pychestrate-client
   journalctl -u pychestrate-client -f

The client logs a warning at start that the REST client has no
``agents_put_client_capabilities`` operation. Until the REST client package
is regenerated, the pod cannot advertise its capabilities. A job that
requires capability tags then finds no eligible pod and is refused; a job that
requires none is not affected.


Upgrade
=======

Install the new version into the same environment and restart. The state
directory is untouched, so the pod keeps its identity:

.. code-block:: bash

   sudo /opt/pychestrate-client/bin/pip install --upgrade \
       --extra-index-url https://code-craft.ai/licensed-repository/simple/ \
       /path/to/pychestrate-client
   sudo systemctl restart pychestrate-client

The restart waits for a running job, as described under `Step 4 - Run it`_.


Replace a pod or renew its certificate
======================================

The client certificate lasts one year by default and the client has no renew
command. To replace it, or to move a pod's label to a new machine, revoke the
old client on the **Clients** page, mint a new token for the same pod label,
and run ``enroll`` again with ``--force`` (or into an empty state directory).
A revoked client no longer receives work and its certificate is refused.
``doctor`` warns when the certificate has fewer than 14 days left.


When something goes wrong
=========================

.. list-table::
   :header-rows: 1
   :widths: 38 62

   * - What you see
     - What it means
   * - ``enrollment failed``: the service refused the enrollment (HTTP 403),
       ``REASON: pod_name_mismatch`` or ``scope_mismatch``
     - The ``--name`` is not the pod label the token was minted for. The
       message names the one it expects (``EXPECTED_POD_NAME``). The token is
       still good: run ``enroll`` again with exactly that label.
   * - ``enrollment failed``, ``cannot be redeemed in its current state``
     - The token was already used, expired, or revoked. Mint a new one.
   * - ``enrollment failed: could not connect to HOST:PORT (Connection
       refused)``
     - Nothing is listening at ``--server-url``, or the address or port is
       wrong. Check the URL and that the service is running.
   * - ``enrollment failed: the service at HOST did not answer within 30
       seconds``
     - The service accepted the connection and did not reply. Check the
       service, or wait longer with ``--timeout``.
   * - ``enrollment failed: the TLS certificate of HOST could not be
       verified``
     - The service's certificate is not signed by a public authority. For a
       development service with a self-signed certificate only, add
       ``--insecure``.
   * - ``enrollment failed: this state directory (...) is already enrolled
       as client ...``
     - Enrolling again would replace that identity. Add ``--force`` to do so,
       or choose another ``--state-dir``.
   * - ``doctor`` prints ``FAIL``
     - The line says what to do: enroll, replace an expired certificate, or
       reach the service.
   * - Heartbeat and dispatch calls fail with ``401`` and ``Mutual-TLS client
       certificate required``
     - The service URL does not lead to a front end that verifies client
       certificates, for example a plain HTTP development address. Enroll
       and run against the HTTPS address.
   * - A dispatch answer that is not a dispatch, logged as a warning and
       backed off
     - Usually a wrong service URL, such as one that returns a login page.
   * - A test job fails with "The 'testbase' binary (testbase) could not be
       found on this pod"
     - ``testbase`` is not on the ``PATH`` of the client process. See the
       ``Environment="PATH=..."`` line in the unit.
   * - The client appears as *Stale* or *Lost*
     - No heartbeat reached the service. Check the client's log and its
       network path to the service.

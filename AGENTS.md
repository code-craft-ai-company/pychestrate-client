# Personality
AI Agents working in this repository should conduct themselves as a Principal Software
Engineer and Architect working at the "Automation Mojo LLC" company. This software engineer
is known as 'AutomationX'. The primary goal of 'AutomationX' is to help the company build
a suite of Python foundation software packages that are well planned out, highly reusable,
modular, and that enable companies to rapidly build advanced automation software systems and
automation pipelines. When building and creating packages, 'AutomationX' always focuses
on reusing modular code from other 'codecraftai-*' packages and always works to look at the big
picture and make sure code and functionality is placed in the correct 'codecraftai-*' package.
AutomationX is a very creative, but very disciplined software engineer that values code stability,
reusability and scalability over quick fixes and fast delivery.

# Rules
- Look in the '<repository>/specs' folder for specification files that help steer development work
- Look in the '<repository>/userguide' folder for files containing standards and conventions
- Look in the '<repository>/prompts' folder for pre-built prompts
- Files inside the .venv folder should not be modified
- Place test results inside the .output folder
- Never work around missing dependencies; halt and ask for assistance
- Avoid using relative imports
- Only write tests to test product code, never write tests to verify specs
- Always prioritize the reuse of code from 'codecraftai-*' packages over rewriting functionality

# Project Overview
The `pychestrate-client` package is the **pod-side runtime client** for the Pychestrate
test orchestration system. It is the binary an operator installs on a pod's client
machine. Its responsibilities are:

1. **Enroll** with the central Pychestrate service using a one-time bootstrap token
   (exchanging a CSR for a signed mTLS client certificate + a `client_id`).
2. **Heartbeat** against the service registry on a periodic interval.
3. *(M2)* Long-poll the dispatch endpoint for assigned jobs.
4. *(M2)* Shell out to `codecraftai-testbase` to execute dispatched jobs.
5. *(M3)* Push progress + final results back to the service.

The runtime consumes the codegen'd `codecraftai-restclient-pychestrate` package for all
HTTP traffic against the Pychestrate REST API - it never hand-rolls HTTP calls against
the service.

# Critical Naming Conventions

- **The runtime is "client", NEVER "agent".** Internal prose, package names, log
  messages, variable names, and commit messages must all use "client" terminology.
  This aligns with the broader `codecraftai-*-client` family.
- **The Python namespace is `ccai.pychestrate.client.*`** (product-first hierarchy).
  Do not use the inverted `ccai.client.pychestrate.*` form.
- The REST client this package consumes is at `ccai.pychestrate.rest.client.*`
  (note: also product-first under `ccai.pychestrate`).

# Testing Conventions
This repository utilizes the 'testbase' test framework. Tests live under
`source/testroots/ccai/` and mirror the source hierarchy under
`source/packages/`.

File to test: `source/packages/ccai/pychestrate/client/enrollment.py`
Test file location: `source/testroots/ccai/tests/pychestrate/client/test_enrollment.py`

# Code Naming Conventions
Follow the Automation Mojo organization standing guidance for Python coding
standards (the org-level `CLAUDE.md` that applies to every `codecraftai-*`
repository). The short form:

- `snake_case` for functions, methods, parameters.
- Class file names: all-lowercase, no underscores, named after the class.
- Always pass named parameters by name.
- Return statements assign to a local `rtnval` before returning.

Repository-local standards and convention documents belong in `userguide/`.
That folder currently carries only `01-00-quickstart.rst` - the `10-0x`
standards documents that the sibling Pychestrate repositories carry have not
been ported here yet, so the org standing guidance is authoritative for
anything the list above does not cover.

# Code Structure
- `source/packages/ccai/pychestrate/client/` - the runtime package
  (regular package with `__init__.py`).
- `ccai/` and `ccai/pychestrate/` are PEP 420 namespace packages
  (no `__init__.py`).

# Coding Standards
This code is intended to be operated by site operators on pod machines and to
interact with a remote service. Therefore the code must prioritize:

* debuggability over performance
* readability over conciseness
* explicit error handling over silent failures

For all the code in this repository, follow the Automation Mojo organization
standing guidance for Python coding standards, together with any standards
documents carried under `userguide/` (see "Code Naming Conventions" above).

# Environment Setup

## Codex Container Setup
Use the script `development/setup-agent-container` as the setup script for Codex containers.

## Development Environments
For local development:

* run the `repository-setup/rehome-repository` script (creates the `.env` file).
* run the `development/setup-environment` script (creates the virtual environment).

# Project Specific Rules

- **M1 delivered enrollment + heartbeat. M2 delivered dispatch consumption +
  testbase invocation. M3 delivers live progress streaming + result-tree
  upload + completion manifest** -- the dispatch-consumer loop now closes the
  orchestration loop on the client side.
- The result uploader writes the testbase result tree DIRECTLY to the shared
  results object store (per the `5-shared-results-backend` design) via an
  injectable `ResultStoreDriver` (Aspects pattern). The signed envelope's
  `JobResultsLocator` supplies only `backend_key_prefix` + `backend_driver`;
  the concrete object-store binding + credentials are provisioned at the
  site / pod level -- there is no per-artifact HTTP upload endpoint on the
  REST surface. When no driver is wired the run is marked `upload_failed`
  locally for retry.
- Do not bypass the codegen'd REST client. If an operation is missing from
  `codecraftai-restclient-pychestrate`, halt and report rather than hand-rolling
  the HTTP call.
- The codegen'd REST client is consumed as a git source dependency in
  `pyproject.toml`, matching the other `codecraftai-*` deps.
- The dispatch consumer MUST verify the Ed25519 signature on every job
  envelope before executing it, and MUST refuse to execute an envelope it
  cannot verify.
- The service publishes its envelope signing public keys as an RFC 7517
  JWK Set (JWKS) of OKP / Ed25519 keys. Fetching that JWK Set is the
  first-class way the client obtains a verification key. It is fetched
  THROUGH the codegen'd REST client (never a hand-rolled HTTP call),
  normalized to PEM, and cached in the state directory as
  `envelope-signing-jwks.json` with the time it was fetched.
- **Key-resolution order** (implemented in
  `envelope_verifier.resolve_envelope_signing_key`, documented in the
  README; keep all three in sync when changing it):
  1. Operator-installed key (`pychestrate-client install-signing-key`) --
     an explicit override. While installed it is the ONLY trust anchor:
     the JWK Set is not consulted and an envelope signed by any other key
     id is refused.
  2. Fresh cached JWK Set (`envelope-signing-jwks.json`).
  3. Enrollment-bundled signing-key set (`envelope-signing-keys.json`) --
     local and already service-delivered, so it is checked before any
     network call.
  4. Live JWK Set fetch from the service's JWKS endpoint (picks up key
     rotation), written into the cache.
  5. Stale cached JWK Set -- availability fallback when the live fetch did
     not resolve the id. Ed25519 public keys do not expire.
  6. Pre-JWKS `GET /api/v1/envelope-signing-keys` refresh, retained for
     services without a JWKS endpoint.
- A key source that is unusable *by configuration* (the codegen'd REST
  client exposes no JWKS operation, the service returned a malformed
  document) is skipped so the next source can answer. Transport failures
  are NOT swallowed -- they propagate so the dispatch-consumer loop backs
  off and retries instead of permanently rejecting a legitimate job.
- **Open item:** the exact JWKS method name on the codegen'd REST client
  depends on how `codecraftai-restclient-pychestrate` is generated from the
  service's OpenAPI document. `jwks.DEFAULT_JWKS_OPERATION_CANDIDATES`
  probes the plausible generated names and raises an actionable
  `ConfigurationError` naming every candidate it tried when none is
  present -- it never falls back to a hand-rolled HTTP call. Once the REST
  client is regenerated against the JWKS endpoint, confirm the generated
  name and pin that tuple to it.

# Pull Request Guidelines

# Packaging
Currently, this package is consumed via a GitHub source reference. It is not
published to PyPI.

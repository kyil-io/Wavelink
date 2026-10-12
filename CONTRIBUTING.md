# Contributing (fork maintainer CI controls)

This fork accepts changes only through pull requests to `main`.

## Maintainer controls for this fork

- GitHub Actions are intentionally disabled in repository settings.
- Required CI context `bootstrap/kyil-ci-trusted` is posted from an external,
  isolated maintainer-run job on `kyil-ci-01` / `kyil-ci-02`.
- Release publishing workflows are intentionally omitted from this fork
  baseline. Build and release operations are handled through reviewed maintainer
  procedures.

## External-only required CI procedure

From a clean checkout of the exact PR head SHA:

1. **Trust gate (mandatory before any install/import/build/test command):**

   - A trusted maintainer must explicitly select and audit the exact commit SHA
     to validate.
   - Do **not** run PR-controlled code on shared CI runners until that audit is
     complete.
   - A unique workspace and a Python venv are operational hygiene only; they
     are **not** a security sandbox.
   - Never schedule this automatically for untrusted external PRs.
   - External contributions must be fully reviewed and adopted onto a
     maintainer-owned branch first.
   - If code is still untrusted, validate only inside a disposable,
     credential-free VM (not on `kyil-ci-01` / `kyil-ci-02`).
   - Keep commit-status credentials scoped to the trusted controller path only.
   - Run as the existing unprivileged CI job user; do not use root for
     validation.

2. Run the helper against the supported interpreter matrix:
   minimum supported Python (`3.10`, per `pyproject.toml`) and a current
   interpreter (`3.12` on the kyil-ci runner fleet today). For each matrix
   entry, create a venv **outside** the repository checkout, install the
   toolchain, run the helper while that venv is active, then deactivate.

   **Python 3.10 lane**

   `python3.10 -m venv ../.venv-extci-wavelink-py310 && . ../.venv-extci-wavelink-py310/bin/activate`

   `python -m pip install --upgrade pip`

   `python -m pip install -r requirements.txt ruff pyright build pytest`

   - Mature branches:

     `tools/bootstrap/validate_external_ci.sh --mode normal`

   - Initial bootstrap PRs before lifecycle tests exist:

     `tools/bootstrap/validate_external_ci.sh --mode bootstrap-only`

   `deactivate`

   **Current lane (3.12 on kyil-ci)**

   `python3.12 -m venv ../.venv-extci-wavelink-current && . ../.venv-extci-wavelink-current/bin/activate`

   `python -m pip install --upgrade pip`

   `python -m pip install -r requirements.txt ruff pyright build pytest`

   - Mature branches:

     `tools/bootstrap/validate_external_ci.sh --mode normal`

   - Initial bootstrap PRs before lifecycle tests exist:

     `tools/bootstrap/validate_external_ci.sh --mode bootstrap-only`

     In this mode the helper prints `LIFECYCLE_SUITE=NOT PRESENT` (never
     "0 tests passed").

   `deactivate`

3. Post the required commit status on the exact head SHA under context
   `bootstrap/kyil-ci-trusted` **only after all matrix helper runs pass**, and
   include a link to the raw execution transcript.

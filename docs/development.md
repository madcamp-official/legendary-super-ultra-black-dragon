# Development and Release Workflow

## Environment

```bash
python3 -m pip install -e '.[test]'
python3 -m unittest discover -v
```

The unit suite uses SQLite, fake host commands, and a FastAPI test client. Keep it runnable without
GPU hardware, Docker, PostgreSQL, or external services.

The base package has no third-party Python dependency so the Ubuntu 22.04 APT node package remains
installable. Control Plane dependencies live in the `server` extra; the `test` extra includes them
for the complete suite.

For a local controller:

```bash
export DURE_DATABASE_URL=sqlite:////tmp/dure-control.db
export DURE_ADMIN_TOKEN=development-only
dure-server --migrate
dure-server --host 127.0.0.1 --port 8081
```

## Git hooks

This repository tracks native hooks in `.githooks/`. Activate them once per clone:

```bash
git config core.hooksPath .githooks
```

- `pre-commit` rejects whitespace errors, conflict markers, `.env`, credentials, and generated
  artifacts; it also compiles Python sources.
- `pre-push` runs the full unit suite, a clean wheel build, and an isolated migration smoke test.

Bypass hooks only for an explicitly documented emergency; run the skipped checks immediately after.

## Schema and release changes

Create a new revision under `src/dure/control/migrations/versions/` for every schema change. Verify
it using a new database and `dure-server --migrate`.

Before releasing, synchronize the project, package, runtime, and Debian versions. Build locally with
`scripts/build-deb.sh`, then tag exactly `v<debian-version>` to trigger the signed APT workflow.
Never place signing material in the repository or workflow logs.

# Dure Repository Guide

## Purpose

Dure is a Python 3.10+ Linux CLI, node agent, and central control plane for trusted community
LLM infrastructure. It probes NVIDIA GPU nodes, builds deterministic Ray/vLLM deployment plans,
applies those plans through Docker, and reports readiness to a FastAPI/PostgreSQL controller.
The Dure project root is `dure/` inside this repository.

## Before changing code

- Read `dure/README.md` and the relevant document under `dure/docs/`.
- Preserve unrelated user changes in a dirty worktree.
- Use `rg`/`rg --files` for discovery and `apply_patch` for edits.
- Never add `.env`, agent credentials, admin tokens, signing keys, model tokens, or generated packages.
- Treat `/etc/dure/agent.json` and `/etc/dure/server.env` as secrets.

## Git workflow

- Never implement changes directly on `main`.
- Start each release line from an up-to-date `main` and create a `version/<semver>` branch before
  editing (for example, `version/0.3.0`). Continue related work on that version branch.
- After every completed requested change, run the required validation, create an intentional commit,
  and push the current version branch to `origin`.
- Never merge a version branch into `main`, create a release tag, publish an APT release, or delete
  the branch until the user explicitly requests the merge or release.
- A version becomes official only when the user requests and completes the merge to `main`.
- If unrelated changes are present, preserve them and stage only files belonging to the requested
  work.

## Architecture invariants

- Keep local CLI commands (`doctor`, `plan`, `init`, `status`, `verify`) usable without a running
  control plane.
- Nodes initiate outbound control-plane connections; the controller never requires inbound SSH.
- `dure join` creates a pending node. Pending nodes may heartbeat but must not claim tasks.
- A central operator must approve a pending node before it can receive work.
- Central tasks are a closed enum. Never introduce arbitrary shell, command, Docker-argument, or
  Python-code execution through task payloads.
- Central deployments require an OCI digest-pinned image. Do not add a remote bypass.
- Match assignments by server-issued node UUID. Hostname support exists only for legacy plan
  normalization.
- One node may execute only one leased task at a time. All task handlers must be retry-safe.
- Stop/restart operations may affect only containers carrying the exact Dure deployment label.
- Never install or alter NVIDIA host drivers automatically.
- Do not expose Ray GCS, dashboard, or worker ports to the public Internet.

## Code organization

- `dure/src/dure/cli.py`: local and administrative CLI surface.
- `dure/src/dure/agent.py`: join flow, polling loop, lease renewal, and safe task execution.
- `dure/src/dure/control/`: API, persistence models, services, and bundled Alembic migrations.
- `dure/src/dure/probe.py`, `planner.py`, `orchestrator.py`, `runtime.py`, `readiness.py`: local node
  discovery and deployment lifecycle.
- `dure/packaging/`, `dure/debian/`, `.github/workflows/`: systemd, Debian, APT, and release integration.

Keep HTTP handlers thin. Put transactional rules in `control/service.py`, host actions in the
runtime/orchestrator layer, and wire-format validation in Pydantic models.

## Validation

Run before handing off a change:

```bash
cd dure
python3 -m compileall -q src tests
python3 -m unittest discover -v
git diff --check
```

For packaging, migration, or entry-point changes also run:

```bash
dure-server --database-url sqlite:////tmp/dure-migration-check.db --migrate
python3 -m pip wheel . --no-deps --no-build-isolation -w /tmp/dure-wheel-check
```

Add tests for both the successful path and the relevant denial/failure path. Use `FakeRunner` for
host commands and FastAPI's test client for API flows. Do not require a real GPU, Docker daemon,
PostgreSQL server, or Internet access in the unit suite.

## Schema, versions, and releases

- Schema changes require a new Alembic revision; never edit a released revision.
- Keep versions synchronized in `dure/pyproject.toml`, `dure/setup.py`,
  `dure/src/dure/__init__.py`, and `dure/debian/changelog`.
- Do not tag or publish a release unless explicitly requested.
- The APT workflow publishes tags matching the Debian version exactly (`v<version>`).
- Preserve existing CLI flags and plan JSON compatibility unless a breaking release is explicit.

## Documentation

Update documentation in the same change when modifying CLI syntax, API routes, enrollment,
security boundaries, lifecycle states, package defaults, or operator procedures. Record known
limitations honestly; do not describe planned control-plane capabilities as already deployed.

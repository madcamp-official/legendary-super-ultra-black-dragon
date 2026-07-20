# Control Plane Operations

## Central server

Install the Control Plane extra on the central host. The APT package is intentionally the portable
node CLI/Agent package and does not install server dependencies or the server systemd unit.

```bash
python3 -m pip install -e '.[server]'
```

Set server secrets outside the repository:

```dotenv
DURE_DATABASE_URL=postgresql+psycopg://dure:password@127.0.0.1/dure
DURE_ADMIN_TOKEN=<random-secret>
```

Apply migrations before starting a new version:

```bash
set -a
source /etc/dure/server.env
set +a
dure-server --migrate
systemctl restart dure-server
```

The packaged development/LAN service listens on `0.0.0.0:8081`. Production should bind the
application to loopback behind a TLS reverse proxy and expose only HTTPS 443. Never expose
PostgreSQL or Ray ports publicly.

Health check:

```bash
curl -fsS http://127.0.0.1:8081/health
```

## Join and approval

The current package default is stored in `/etc/dure/dure-client.env`. Install and join a node:

```bash
sudo apt install dure
sudo dure join
```

The command collects a profile, writes root-only `/etc/dure/agent.json`, enables `dure-agent`, and
returns the pending node UUID. Approve it centrally:

```bash
dure admin nodes --pending
dure admin node show <node-id>
dure admin node approve <node-id>
dure admin probe --nodes <node-id>
```

## Codex-assisted capacity diagnosis

Install and authenticate Codex on the admin computer only:

```bash
codex --version
codex login status
```

Upgrade and restart Agents first so `PROBE` can report installed models and LLM workloads. Then run:

```bash
dure admin diagnose
dure admin diagnose --nodes <node-a> <node-b> --output diagnosis.json
```

By default the command submits a `PROBE` task to every approved online node, waits up to 180
seconds, fetches `GET /v1/admin/inventory`, and calls the local `codex exec`. Use `--no-refresh` to
analyze the last stored profiles, `--timeout` to change the Agent wait, `--codex-timeout` to change
the model wait, and `--model` to select a Codex model. `--json` prints the structured result.

The report is advisory. It does not create or apply a deployment. Review these limitations before
acting on it:

- profiles from offline or stale nodes are never treated as immediately deployable;
- multi-node Ray recommendations require measured RTT/bandwidth and firewall/NCCL validation;
- incomplete model directories are reported but not considered reusable;
- non-Dure LLM containers are observed by name, image and status only and are never stopped;
- CPU-only nodes receive utility-role suggestions because the current Dure runtime uses a GPU Ray
  head and does not assign model layers to CPU nodes.

The inventory sent to the configured Codex provider contains hardware, network addresses, runtime,
model paths/names, and container image/status metadata. It excludes admin/node bearer credentials,
container environment values, container commands, model tokens, and prompt data.

If a node is lost or untrusted:

```bash
dure admin credential revoke <node-id>
```

Credential rotation returns a replacement secret that must be installed on that node immediately.

## Deployment operations

Create a digest-pinned deployment, then submit node-specific tasks:

```bash
dure admin deployment create \
  --profile node-a.json --profile node-b.json --profile node-c.json \
  --model qwen2.5-72b-awq \
  --image registry.example/vllm@sha256:<digest> \
  --accept-model-download --pull

dure admin apply <deployment-id> --nodes <node-a> <node-b> <node-c>
dure admin tasks --watch
dure admin verify <deployment-id> --nodes <node-a> <node-b> <node-c> --api
```

Use `start`, `stop`, and `restart` with the same deployment ID and explicit node list. Bulk requests
create independent tasks, so inspect partial failures rather than assuming an all-or-nothing result.

## Upgrade and recovery

On the controller, back up PostgreSQL, upgrade the package, migrate, and restart the server. Upgrade
Agents in small batches and restart `dure-agent` after package installation.

```bash
sudo apt update
sudo apt install --only-upgrade dure
sudo systemctl daemon-reload
sudo systemctl restart dure-agent
```

An Agent reuses its credential and completed-task journal after restart. Expired task leases may be
redelivered; task handlers must remain idempotent. Do not delete `/var/lib/dure/agent-tasks.json`
during an active deployment.

Useful diagnostics:

```bash
systemctl status dure-server dure-agent
journalctl -u dure-server -u dure-agent --since -1h
dure admin nodes --json
dure admin tasks
```

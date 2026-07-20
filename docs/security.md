# Security Model and Hardening Plan

## Protected assets

- Admin and node bearer credentials
- Host root access and Docker control
- Model artifacts and immutable image identity
- Deployment topology, task history, and node inventory
- Prompts and generated data processed by vLLM

## Principal threats and current controls

| Threat | Current control | Remaining work |
|---|---|---|
| Unauthorized node receives work | New joins are pending until operator approval | Rate-limit and network-limit join requests |
| Stolen node credential | Per-node hash storage and individual revocation | mTLS and automated rotation |
| Controller used as remote shell | Closed task enum and validated payload options | Separate privilege boundary for host actions |
| Image substitution | Central plans require OCI digest | Verify image signatures and provenance |
| Task replay | Lease, generation checks, local completed-task journal | Server-side signed task envelopes |
| Concurrent mutation | Per-node row lock and one active lease | PostgreSQL concurrency stress tests |
| Accidental container damage | Exact deployment-label filtering | Rootless/containerd isolation evaluation |
| Public Ray exposure | Documented private-network requirement | WireGuard automation and firewall validation |
| Join endpoint abuse | Pending nodes have no work authority | Admission rate limit, quotas, and audit alerts |
| Prompt disclosure by host operator | Community workloads declared non-confidential | Confidential-computing/private pool option |

## Operational requirements

- Use HTTPS for every non-development Agent connection.
- Store `DURE_ADMIN_TOKEN`, database credentials, APT signing keys, and model credentials outside Git.
- Restrict join and control-plane endpoints to the trusted LAN or private overlay when possible.
- Review hostname, GPU inventory, address, and operator ownership before approving a node.
- Pin deployment images and model revisions; use the same tested runtime across a Ray pod.
- Collect metadata and errors without logging prompts or credentials.
- Treat `dure admin diagnose` as an explicit external-processing action: it sends the selected node
  inventory to the admin computer's configured Codex provider, but never credentials, container
  environment values, commands, or prompt data.
- Back up PostgreSQL and test credential revocation and restore procedures.

## Pre-public-alpha gates

Before accepting nodes from outside the trusted operator group:

1. Add request rate limiting and abuse controls to tokenless join.
2. Replace bearer-only Agent authentication with mTLS or signed device keys.
3. Deploy a private network overlay and enforced host firewall rules.
4. Verify image signatures and model manifests.
5. Run an independent review of the root Agent and container isolation.
6. Add alerting for join floods, heartbeat loss, repeated failures, and credential misuse.

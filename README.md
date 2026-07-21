# saem

Install-once, role-assign-from-head package for the Saem RAG cluster.

## Setup (once per new VM)

```bash
git clone --branch saem-standalone --single-branch <repo-url> saem
cd saem
pip3 install .

# shared token: generate once, copy the same value to every VM (out of band,
# e.g. scp — this is the only step that still needs your ssh key)
mkdir -p /etc/saem
echo "<shared-secret>" > /etc/saem/token

saem agent &          # or install as a systemd unit so it survives reboot
```

Use `pip3 install .`, not `-e`: the Ubuntu 22.04 VMs ship pip 22.0.2 +
setuptools 59.6, which predates PEP 660 editable installs. That same old
setuptools also cannot read a PEP 621 `[project]` table, which is why
metadata lives in `setup.py` — deleting it makes every install silently
build as `UNKNOWN-0.0.0` with no `saem` command.

## Designate head (once, on one node)

```bash
saem head start --ip <this node's internal IP>
```

## Assign roles (from head, any time — internal IPs only, cloud VPC handles

the routing so 172.10.x external addresses / the 22+443-only security group
don't come into play at all)

```bash
saem head register 192.168.0.252 --role qdrant_primary       # camp-57
saem head register 192.168.0.209 --role retrieval_gateway --port 9000   # camp-59
saem head register 192.168.0.124 --role ingest_coordinator   # camp-60
saem head register 192.168.0.44  --role crawler --port 9200  # camp-18
saem head register 192.168.0.226 --role api_proxy --port 443 # camp-73

saem head status   # lists head itself (role: head) plus every registered node
```

## Attaching an LLM backend (dure)

GPU clusters (the 235B cluster, or a future camp1 head) run their own
separate `dure` package and never install saem — head just needs to know
their URL. Register one from head, any time:

```bash
saem head register-backend cluster235b http://192.168.0.228:8000 --model qwen3-235b

saem head backend-status   # every backend head knows about
```

`--active` (the default) immediately pushes `{url, model}` to every node
currently registered as `retrieval_gateway` or `api_proxy`, which write it
to `/etc/saem/backend.yaml` and restart their role service to pick it up.
Swapping to a new GPU head later — camp1 replacing the 235B cluster, say —
is one more `register-backend` call with `--active`; no code or env-var
edits on any VM.

`register` POSTs to the target's `saem agent` (port 9999), which writes
`/etc/saem/role.yaml` and installs a `saem-role` systemd unit
(`Restart=always`) so the assigned role survives reboots and crashes —
unlike the original `nohup ... &` scripts.

## Adding a 6th role later

1. `saem/roles/<name>.py` with a `run(port)` function
2. one line in `saem/roles/__init__.py` (`ROLE_ENTRYPOINTS`)
3. add the name to `ROLE_CHOICES` in `saem/common/state.py`

Nothing in `cli.py` / `agent.py` / `head.py` needs to change.

## Status

Role bodies (`saem/roles/*.py`) are stubs — each has a `TODO` pointing at
the existing nohup script on the corresponding VM whose logic still needs
to be ported in. The install/register/systemd plumbing around them is
what's new.

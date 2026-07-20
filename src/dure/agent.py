from __future__ import annotations

import argparse
import json
import logging
import os
import secrets
import signal
import threading
import time
from pathlib import Path

from . import __version__
from .command import SubprocessRunner
from .http import APIError, JSONClient
from .models import DeploymentPlan
from .orchestrator import InitOrchestrator
from .probe import NodeProbe
from .readiness import ReadinessVerifier
from .runtime import ContainerRuntime
from .state import StateStore
from .task import TaskType


LOG = logging.getLogger("dure.agent")
DEFAULT_CONFIG = Path("/etc/dure/agent.json")
DEFAULT_CLIENT_CONFIG = Path("/etc/dure/dure-client.env")
DEFAULT_HISTORY = Path("/var/lib/dure/agent-tasks.json")
DEFAULT_STATE = Path("/var/lib/dure/state.json")


def _atomic_json(path: Path, value: dict, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(6)}")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, mode)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _read_json(path: Path, default: dict | None = None) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return dict(default or {})


def _read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return values
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip().strip("'\"")
    return values


def resolve_join_settings(
    *, server: str | None = None, insecure: bool | None = None, client_config: Path = DEFAULT_CLIENT_CONFIG
) -> tuple[str, bool]:
    configured = _read_env_file(client_config)
    if not configured and client_config == DEFAULT_CLIENT_CONFIG:
        # Editable/source installs do not run Debian's conffile installation.
        configured = _read_env_file(Path(__file__).resolve().parents[2] / "packaging" / "dure-client.env")
    resolved_server = server or os.environ.get("DURE_SERVER") or configured.get("DURE_SERVER")
    if not resolved_server:
        raise ValueError(f"central server is not configured; set DURE_SERVER in {client_config}")
    raw_insecure = os.environ.get("DURE_INSECURE", configured.get("DURE_INSECURE", "false"))
    resolved_insecure = insecure if insecure is not None else raw_insecure.lower() in {"1", "true", "yes", "on"}
    if not resolved_insecure and not resolved_server.startswith("https://"):
        raise ValueError("central server must use HTTPS unless DURE_INSECURE=true")
    return resolved_server.rstrip("/"), resolved_insecure


def _enable_agent_service(runner=None) -> None:
    service_runner = runner or SubprocessRunner()
    reloaded = service_runner.run(["systemctl", "daemon-reload"], timeout=30)
    started = service_runner.run(["systemctl", "enable", "--now", "dure-agent"], timeout=60)
    if not reloaded.ok or not started.ok:
        detail = started.stderr or started.stdout or reloaded.stderr or reloaded.stdout
        raise RuntimeError(f"dure-agent could not be started: {detail}")


def join_control_plane(
    *,
    server: str | None = None,
    insecure: bool | None = None,
    config_path: Path = DEFAULT_CONFIG,
    client_config: Path = DEFAULT_CLIENT_CONFIG,
    runner=None,
    start_service: bool = True,
) -> dict:
    if os.geteuid() != 0:
        raise PermissionError("dure join must run as root")
    resolved_server, resolved_insecure = resolve_join_settings(
        server=server, insecure=insecure, client_config=client_config
    )
    install = _read_json(config_path)
    if {"node_id", "credential", "server"} <= set(install):
        if start_service:
            _enable_agent_service(runner)
        return {"node_id": install["node_id"], "status": "already-joined"}
    install_id = install.get("install_id") or secrets.token_hex(16)
    profile = NodeProbe(runner).collect()
    client = JSONClient(resolved_server, verify_tls=not resolved_insecure)
    response = client.request(
        "POST",
        "/v1/nodes/join",
        {"install_id": install_id, "agent_version": __version__, "profile": profile.to_dict()},
    )
    _atomic_json(
        config_path,
        {
            "server": resolved_server,
            "node_id": response["node_id"],
            "credential": response["credential"],
            "install_id": install_id,
            "verify_tls": not resolved_insecure,
            "state_file": str(DEFAULT_STATE),
        },
    )
    if start_service:
        try:
            _enable_agent_service(runner)
        except RuntimeError as exc:
            raise RuntimeError(f"node joined, but {exc}") from exc
    return {"node_id": response["node_id"], "status": response.get("status", "pending")}


class TaskExecutor:
    def __init__(self, node_id: str, *, runner=None, state_path: Path | None = None) -> None:
        self.node_id = node_id
        self.runner = runner
        self.state_path = state_path

    def _profile(self):
        profile = NodeProbe(self.runner).collect()
        profile.node_id = self.node_id
        return profile

    def execute(self, task: dict) -> dict:
        try:
            kind = TaskType(task["type"])
        except (KeyError, ValueError) as exc:
            raise ValueError("unsupported task type") from exc
        payload = task.get("payload") or {}
        if kind == TaskType.PROBE:
            return {"profile": self._profile().to_dict()}
        plan = None
        if kind != TaskType.PROBE:
            if kind == TaskType.VERIFY and "plan" not in payload:
                raise ValueError("VERIFY requires a deployment plan")
            if "plan" in payload:
                plan = DeploymentPlan.from_dict(payload["plan"])
                assignment = plan.assignment_for(self.node_id)
                if assignment is None:
                    raise ValueError("node is not assigned to deployment")
                if payload.get("generation") != plan.generation:
                    raise ValueError("deployment generation mismatch")
                if "@sha256:" not in plan.image:
                    raise ValueError("central deployment image is not digest-pinned")
        profile = self._profile()
        if kind == TaskType.VERIFY:
            verifier = ReadinessVerifier(self.runner, profile.runtime.engine or "docker")
            checks = [verifier.host_gpu(profile), verifier.container_gpu(plan), verifier.ray_cluster(plan)]
            if payload.get("api"):
                checks.append(verifier.api())
            if not all(item.ok for item in checks):
                raise RuntimeError("; ".join(item.detail for item in checks if not item.ok))
            return {"checks": [item.to_dict() for item in checks], "ok": True}
        runtime = ContainerRuntime(self.runner, profile.runtime.engine or "docker")
        if kind == TaskType.STOP_DEPLOYMENT:
            check = runtime.stop_deployment(plan.deployment_id)
            if not check.ok:
                raise RuntimeError(check.detail)
            store = StateStore(self.state_path or DEFAULT_STATE)
            state = store.load()
            state.phase = "PLANNED"
            state.detail = "Deployment containers are stopped"
            store.save(state)
            return {"checks": [check.to_dict()]}
        if kind == TaskType.RESTART_DEPLOYMENT:
            stopped = runtime.stop_deployment(plan.deployment_id)
            if not stopped.ok:
                raise RuntimeError(stopped.detail)
        apply_download = kind == TaskType.APPLY_DEPLOYMENT
        _, _, checks = InitOrchestrator(
            runner=self.runner, state_path=self.state_path or DEFAULT_STATE, node_id=self.node_id
        ).run(
            plan=plan,
            apply=True,
            accept_model_download=bool(payload.get("accept_model_download")) if apply_download else False,
            pull=bool(payload.get("pull_image")) if apply_download else False,
            allow_unpinned_image=False,
            replace=kind in {TaskType.START_DEPLOYMENT, TaskType.RESTART_DEPLOYMENT},
            serve=bool(payload.get("serve")),
        )
        if any(not item.ok and item.blocking for item in checks):
            raise RuntimeError("; ".join(item.detail for item in checks if not item.ok and item.blocking))
        return {"checks": [item.to_dict() for item in checks]}


class Agent:
    def __init__(self, config: dict, *, history_path: Path = DEFAULT_HISTORY, runner=None) -> None:
        self.config = config
        if config.get("verify_tls", True) and not config["server"].startswith("https://"):
            raise ValueError("agent control-plane URL must use HTTPS")
        self.client = JSONClient(config["server"], config["credential"], verify_tls=config.get("verify_tls", True))
        self.history_path = history_path
        self.history = _read_json(history_path, {"completed": {}})
        self.state_path = Path(config.get("state_file", DEFAULT_STATE))
        self.executor = TaskExecutor(config["node_id"], runner=runner, state_path=self.state_path)
        self.running = True

    def stop(self, *_args) -> None:
        self.running = False

    def once(self) -> bool:
        state = StateStore(self.state_path).load().to_dict()
        self.client.request("POST", "/v1/agent/heartbeat", {"state": state})
        task = self.client.request("POST", "/v1/agent/tasks/claim").get("task")
        if task is None:
            return False
        task_id = task["id"]
        previous = self.history.get("completed", {}).get(task_id)
        if previous is not None:
            if previous.get("status") == "failed":
                self.client.request("POST", f"/v1/agent/tasks/{task_id}/fail", {"error": previous["error"]})
            else:
                result = previous.get("result", previous)
                self.client.request("POST", f"/v1/agent/tasks/{task_id}/complete", {"result": result})
            return True
        renewal_stop = threading.Event()

        def renew_lease() -> None:
            while not renewal_stop.wait(60):
                try:
                    self.client.request("POST", f"/v1/agent/tasks/{task_id}/heartbeat")
                except APIError as exc:
                    LOG.warning("could not renew task %s lease: %s", task_id, exc)

        renewal = threading.Thread(target=renew_lease, name=f"dure-lease-{task_id}", daemon=True)
        renewal.start()
        try:
            result = self.executor.execute(task)
            self.history.setdefault("completed", {})[task_id] = {"status": "complete", "result": result}
            self.history["completed"] = dict(list(self.history["completed"].items())[-1000:])
            _atomic_json(self.history_path, self.history)
            self.client.request("POST", f"/v1/agent/tasks/{task_id}/complete", {"result": result})
        except Exception as exc:
            LOG.exception("task %s failed", task_id)
            error = str(exc)[:8192]
            self.history.setdefault("completed", {})[task_id] = {"status": "failed", "error": error}
            self.history["completed"] = dict(list(self.history["completed"].items())[-1000:])
            _atomic_json(self.history_path, self.history)
            self.client.request("POST", f"/v1/agent/tasks/{task_id}/fail", {"error": error})
        finally:
            renewal_stop.set()
            renewal.join(timeout=2)
        return True

    def run(self, interval: float = 10) -> None:
        backoff = interval
        while self.running:
            try:
                self.once()
                backoff = interval
            except APIError as exc:
                LOG.warning("control plane unavailable: %s", exc)
                backoff = min(max(interval, backoff * 2), 300)
            deadline = time.monotonic() + backoff
            while self.running and time.monotonic() < deadline:
                time.sleep(min(1, deadline - time.monotonic()))


def enroll(args) -> int:
    if not args.insecure and not args.server.startswith("https://"):
        raise ValueError("agent control-plane URL must use HTTPS; use --insecure only for development")
    install = _read_json(args.config)
    install_id = install.get("install_id") or secrets.token_hex(16)
    profile = NodeProbe().collect()
    client = JSONClient(args.server, verify_tls=not args.insecure)
    response = client.request("POST", "/v1/enrollments/claim", {
        "token": args.token, "install_id": install_id, "agent_version": __version__,
        "profile": profile.to_dict(),
    })
    _atomic_json(args.config, {
        "server": args.server, "node_id": response["node_id"], "credential": response["credential"],
        "install_id": install_id, "verify_tls": not args.insecure, "state_file": str(DEFAULT_STATE),
    })
    print(f"Enrolled node {response['node_id']}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="dure-agent")
    sub = parser.add_subparsers(dest="command", required=True)
    enroll_parser = sub.add_parser("enroll")
    enroll_parser.add_argument("--server", required=True)
    enroll_parser.add_argument("--token", required=True)
    enroll_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    enroll_parser.add_argument("--insecure", action="store_true", help="Development only: disable TLS verification")
    join_parser = sub.add_parser("join")
    join_parser.add_argument("--server")
    join_parser.add_argument("--insecure", action="store_true", default=None)
    join_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    join_parser.add_argument("--client-config", type=Path, default=DEFAULT_CLIENT_CONFIG)
    run_parser = sub.add_parser("run")
    run_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    run_parser.add_argument("--interval", type=float, default=10)
    run_parser.add_argument("--history", type=Path, default=DEFAULT_HISTORY)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    try:
        if args.command == "enroll":
            return enroll(args)
        if args.command == "join":
            result = join_control_plane(
                server=args.server,
                insecure=args.insecure,
                config_path=args.config,
                client_config=args.client_config,
            )
            print(f"Joined node {result['node_id']} ({result['status']}); waiting for central approval")
            return 0
        config = _read_json(args.config)
        if not {"server", "node_id", "credential"} <= set(config):
            parser.error("agent is not enrolled")
        agent = Agent(config, history_path=args.history)
        signal.signal(signal.SIGTERM, agent.stop)
        signal.signal(signal.SIGINT, agent.stop)
        agent.run(args.interval)
        return 0
    except (APIError, OSError, ValueError) as exc:
        parser.error(str(exc))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

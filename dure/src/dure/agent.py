from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import secrets
import signal
import threading
import time
from dataclasses import replace
from pathlib import Path
from typing import Callable, Protocol

from . import __version__
from .artifact_prepare import (
    ArtifactPreparationExecutor,
    is_artifact_preparation_task,
    preparation_failure_code,
    validate_preparation_history,
    validate_preparation_result,
)
from .cache_quarantine import (
    ARTIFACT_CACHE_QUARANTINE_FAILURE_CODES,
    ArtifactCacheQuarantineExecutor,
    artifact_cache_quarantine_failure_code,
    validate_artifact_cache_quarantine_result,
)
from .command import SubprocessRunner
from .http import APIError, JSONClient
from .host_setup import (
    HOST_SETUP_LOCK_PATH,
    HostSetupLockError,
    acquire_host_setup_lock,
    release_host_setup_lock,
)
from .model_cache import (
    MODEL_CACHE_KIND_FULL_SNAPSHOT,
    MODEL_CACHE_VERIFICATION_VERSION,
)
from .models import DeploymentPlan, InstalledModelProfile, NodeProfile
from .orchestrator import InitOrchestrator
from .pipeline_runtime import (
    is_stage_pipeline_plan,
    is_strict_pipeline_plan,
    validate_strict_pipeline_node,
    validate_strict_pipeline_plan,
    validate_strict_stage_cache,
)
from .probe import DURE_MODEL_ROOT, NodeProbe
from .readiness import ReadinessVerifier
from .runtime import ContainerRuntime
from .state import NodeState, StateStore
from .task import (
    MAX_BENCHMARK_INTEGER,
    BenchmarkTaskPayload,
    TaskType,
    benchmark_profile_fingerprint,
)


LOG = logging.getLogger("dure.agent")
DEFAULT_CONFIG = Path("/etc/dure/agent.json")
DEFAULT_CLIENT_CONFIG = Path("/etc/dure/dure-client.env")
DEFAULT_HISTORY = Path("/var/lib/dure/agent-tasks.json")
DEFAULT_STATE = Path("/var/lib/dure/state.json")
DEFAULT_BUILD_COMMIT = Path("/usr/share/dure/build-commit")
BUILD_COMMIT_ENV = "DURE_BUILD_COMMIT"
_BUILD_COMMIT_UNSET = object()
BENCHMARK_RESULT_FIELDS = frozenset({"benchmark_id", "workload_id", "metrics"})
BENCHMARK_METRIC_FIELDS = frozenset(
    {
        "duration_seconds",
        "request_count",
        "warmup_requests",
        "oom_count",
        "crash_count",
        "restart_count",
        "ttft_p95_ms",
        "tpot_p95_ms",
        "e2e_p95_ms",
        "throughput_tps",
        "success_rate",
        "vram_headroom_pct",
        "quality_score",
        "network_bandwidth_mbps",
        "network_rtt_ms",
        "packet_loss_pct",
        "nccl_all_reduce_ok",
    }
)
BENCHMARK_SINGLE_NODE_NULL_FIELDS = frozenset(
    {
        "network_bandwidth_mbps",
        "network_rtt_ms",
        "packet_loss_pct",
        "nccl_all_reduce_ok",
    }
)
BENCHMARK_AGENT_FAILURE_CODES = frozenset(
    {
        "BENCHMARK_EXECUTION_FAILED",
        "BENCHMARK_PAYLOAD_REJECTED",
        "BENCHMARK_RUNTIME_UNAVAILABLE",
        "BENCHMARK_ARTIFACT_UNAVAILABLE",
    }
)
PENDING_REPORT_TASK_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,254}")
RETRYABLE_REPORT_HTTP_STATUSES = frozenset({408, 425, 429})


class BenchmarkAgentError(ValueError):
    def __init__(self, message: str, *, failure_code: str) -> None:
        if failure_code not in BENCHMARK_AGENT_FAILURE_CODES:
            raise ValueError("unsupported BENCHMARK Agent failure code")
        super().__init__(message)
        self.failure_code = failure_code


class SafeBenchmarkExecutor(Protocol):
    def __call__(
        self,
        payload: BenchmarkTaskPayload,
        profile: NodeProfile,
        cached_model: InstalledModelProfile,
    ) -> dict: ...


def _load_build_commit(path: Path = DEFAULT_BUILD_COMMIT) -> str | None:
    try:
        if path.stat().st_size > 65:
            return None
        value = path.read_text(encoding="ascii").strip()
    except FileNotFoundError:
        value = os.environ.get(BUILD_COMMIT_ENV, "").strip()
    except (OSError, UnicodeError):
        return None
    return value if re.fullmatch(r"[0-9a-f]{40,64}", value) else None


def _require_benchmark_build_commit(
    payload: BenchmarkTaskPayload, build_commit: str | None
) -> None:
    if build_commit is None or payload.dure_commit != build_commit:
        raise BenchmarkAgentError(
            "BENCHMARK Dure commit does not match this Agent build",
            failure_code="BENCHMARK_PAYLOAD_REJECTED",
        )


def _exact_cached_model(
    profile: NodeProfile, payload: BenchmarkTaskPayload
) -> InstalledModelProfile:
    trusted_root = DURE_MODEL_ROOT.resolve()
    matches: list[InstalledModelProfile] = []
    for model in profile.installed_models:
        if (
            model.source != "dure"
            or not model.complete
            or not model.path
            or model.model_id != payload.model_repository
            or model.revision != payload.artifact_revision
            or model.manifest_digest != payload.artifact_manifest_digest
            or model.quantization != payload.quantization
            or model.cache_kind != MODEL_CACHE_KIND_FULL_SNAPSHOT
            or model.verification_version != MODEL_CACHE_VERIFICATION_VERSION
        ):
            continue
        candidate = Path(model.path)
        if not candidate.is_absolute():
            continue
        try:
            resolved = candidate.resolve()
        except (OSError, RuntimeError, ValueError):
            continue
        if not resolved.is_relative_to(trusted_root):
            continue
        matches.append(replace(model, path=str(resolved)))
    if len(matches) != 1:
        raise BenchmarkAgentError(
            "BENCHMARK requires exactly one complete local cache matching repository, "
            "revision, manifest, quantization, and FULL_SNAPSHOT cache kind",
            failure_code="BENCHMARK_ARTIFACT_UNAVAILABLE",
        )
    return matches[0]


def _metric_integer(value, *, minimum: int = 0) -> int:
    if (
        type(value) is not int
        or value < minimum
        or value > MAX_BENCHMARK_INTEGER
    ):
        raise BenchmarkAgentError(
            "BENCHMARK result contains an invalid metric",
            failure_code="BENCHMARK_EXECUTION_FAILED",
        )
    return value


def _metric_number(
    value, *, minimum: float = 0, maximum: float | None = None
) -> float:
    if type(value) not in {int, float}:
        raise BenchmarkAgentError(
            "BENCHMARK result contains an invalid metric",
            failure_code="BENCHMARK_EXECUTION_FAILED",
        )
    normalized = float(value)
    if (
        not math.isfinite(normalized)
        or normalized < minimum
        or (maximum is not None and normalized > maximum)
    ):
        raise BenchmarkAgentError(
            "BENCHMARK result contains an invalid metric",
            failure_code="BENCHMARK_EXECUTION_FAILED",
        )
    return normalized


def _optional_metric_number(
    value, *, minimum: float = 0, maximum: float | None = None
) -> float | None:
    if value is None:
        return None
    return _metric_number(value, minimum=minimum, maximum=maximum)


def _validated_benchmark_result(
    payload: BenchmarkTaskPayload, value
) -> dict:
    if type(value) is not dict or set(value) != BENCHMARK_RESULT_FIELDS:
        raise BenchmarkAgentError(
            "safe BENCHMARK executor returned an invalid result",
            failure_code="BENCHMARK_EXECUTION_FAILED",
        )
    if (
        type(value["benchmark_id"]) is not str
        or value["benchmark_id"] != payload.benchmark_id
        or type(value["workload_id"]) is not str
        or value["workload_id"] != payload.workload_id
        or type(value["metrics"]) is not dict
        or set(value["metrics"]) != BENCHMARK_METRIC_FIELDS
    ):
        raise BenchmarkAgentError(
            "safe BENCHMARK executor returned an invalid result",
            failure_code="BENCHMARK_EXECUTION_FAILED",
        )
    metrics = value["metrics"]
    if any(metrics[field] is not None for field in BENCHMARK_SINGLE_NODE_NULL_FIELDS):
        raise BenchmarkAgentError(
            "single-node BENCHMARK result contains multi-node metrics",
            failure_code="BENCHMARK_EXECUTION_FAILED",
        )
    normalized = {
        "duration_seconds": _metric_number(
            metrics["duration_seconds"], minimum=0.000001
        ),
        "request_count": _metric_integer(metrics["request_count"], minimum=1),
        "warmup_requests": _metric_integer(metrics["warmup_requests"]),
        "oom_count": _metric_integer(metrics["oom_count"]),
        "crash_count": _metric_integer(metrics["crash_count"]),
        "restart_count": _metric_integer(metrics["restart_count"]),
        "ttft_p95_ms": _optional_metric_number(
            metrics["ttft_p95_ms"], minimum=0.000001
        ),
        "tpot_p95_ms": _optional_metric_number(
            metrics["tpot_p95_ms"], minimum=0.000001
        ),
        "e2e_p95_ms": _optional_metric_number(
            metrics["e2e_p95_ms"], minimum=0.000001
        ),
        "throughput_tps": _optional_metric_number(
            metrics["throughput_tps"], minimum=0.000001
        ),
        "success_rate": _metric_number(
            metrics["success_rate"], maximum=1
        ),
        "vram_headroom_pct": _metric_number(
            metrics["vram_headroom_pct"], maximum=100
        ),
        "quality_score": _metric_number(metrics["quality_score"], maximum=1),
        "network_bandwidth_mbps": _optional_metric_number(
            metrics["network_bandwidth_mbps"], minimum=0.000001
        ),
        "network_rtt_ms": _optional_metric_number(metrics["network_rtt_ms"]),
        "packet_loss_pct": _optional_metric_number(
            metrics["packet_loss_pct"], maximum=100
        ),
    }
    if (
        normalized["request_count"] != payload.request_count
        or normalized["warmup_requests"] != payload.warmup_requests
    ):
        raise BenchmarkAgentError(
            "BENCHMARK result does not match the fixed workload",
            failure_code="BENCHMARK_EXECUTION_FAILED",
        )
    nccl = metrics["nccl_all_reduce_ok"]
    if nccl is not None and type(nccl) is not bool:
        raise BenchmarkAgentError(
            "BENCHMARK result contains an invalid metric",
            failure_code="BENCHMARK_EXECUTION_FAILED",
        )
    normalized["nccl_all_reduce_ok"] = nccl
    result = {
        "benchmark_id": payload.benchmark_id,
        "workload_id": payload.workload_id,
        "metrics": normalized,
    }
    try:
        json.dumps(result, allow_nan=False, sort_keys=True)
    except (TypeError, ValueError) as exc:  # pragma: no cover - normalized above
        raise BenchmarkAgentError(
            "safe BENCHMARK executor returned a non-serializable result",
            failure_code="BENCHMARK_EXECUTION_FAILED",
        ) from exc
    return result


def _benchmark_failure_code(exc: Exception) -> str:
    try:
        value = getattr(exc, "failure_code", None)
    except Exception:
        return "BENCHMARK_EXECUTION_FAILED"
    return (
        value
        if type(value) is str and value in BENCHMARK_AGENT_FAILURE_CODES
        else "BENCHMARK_EXECUTION_FAILED"
    )


def _validated_benchmark_payload(value) -> BenchmarkTaskPayload:
    try:
        return BenchmarkTaskPayload.from_dict(value)
    except (TypeError, ValueError) as exc:
        raise BenchmarkAgentError(
            str(exc),
            failure_code="BENCHMARK_PAYLOAD_REJECTED",
        ) from exc


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


def _retire_local_registration(
    config_path: Path,
    config: dict,
    *,
    state_path: Path,
) -> None:
    """Remove credentials while preserving the installation identity for rejoin."""
    install_id = config.get("install_id")
    if install_id:
        _atomic_json(config_path, {"install_id": install_id})
    else:
        config_path.unlink(missing_ok=True)
    StateStore(state_path).save(
        NodeState(
            phase="UNJOINED",
            node_id=config.get("node_id"),
            detail="Node left the Dure control plane and released its deployment GPU",
        )
    )


def unjoin_control_plane(
    *,
    config_path: Path = DEFAULT_CONFIG,
    runner=None,
    setup_lock_path: Path = HOST_SETUP_LOCK_PATH,
) -> dict:
    """Stop this node's exact Dure deployment slice and revoke registration."""
    if os.geteuid() != 0:
        raise PermissionError("dure unjoin must run as root")
    try:
        lock_descriptor = acquire_host_setup_lock(
            setup_lock_path,
            require_root_owner=setup_lock_path == HOST_SETUP_LOCK_PATH,
        )
    except HostSetupLockError as exc:
        raise RuntimeError(f"dure unjoin cannot start: {exc}") from exc
    try:
        config = _read_json(config_path)
        if not {"server", "node_id", "credential"} <= set(config):
            raise ValueError("node is not joined")
        service_runner = runner or SubprocessRunner()
        state_path = Path(config.get("state_file", DEFAULT_STATE))
        state = StateStore(state_path).load()
        if state.deployment_id:
            stopped = ContainerRuntime(
                service_runner, "docker"
            ).stop_registered_node_deployment(
                state.deployment_id,
                generation=state.generation,
                node_id=config["node_id"],
            )
            if not stopped.ok:
                raise RuntimeError(stopped.detail)
        disabled = service_runner.run(
            ["systemctl", "disable", "--now", "dure-agent"], timeout=60
        )
        if not disabled.ok:
            raise RuntimeError(
                disabled.stderr
                or disabled.stdout
                or "dure-agent could not be disabled"
            )
        client = JSONClient(
            config["server"],
            config["credential"],
            verify_tls=config.get("verify_tls", True),
        )
        response = client.request("POST", "/v1/agent/unjoin")
        _retire_local_registration(config_path, config, state_path=state_path)
        return {
            "node_id": response.get("node_id", config["node_id"]),
            "status": "unjoined",
        }
    finally:
        release_host_setup_lock(lock_descriptor)


def join_control_plane(
    *,
    server: str | None = None,
    insecure: bool | None = None,
    config_path: Path = DEFAULT_CONFIG,
    client_config: Path = DEFAULT_CLIENT_CONFIG,
    runner=None,
    start_service: bool = True,
    setup_lock_path: Path = HOST_SETUP_LOCK_PATH,
) -> dict:
    if os.geteuid() != 0:
        raise PermissionError("dure join must run as root")
    try:
        lock_descriptor = acquire_host_setup_lock(
            setup_lock_path,
            require_root_owner=setup_lock_path == HOST_SETUP_LOCK_PATH,
        )
    except HostSetupLockError as exc:
        raise RuntimeError(f"dure join cannot start: {exc}") from exc
    try:
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
    finally:
        release_host_setup_lock(lock_descriptor)


class TaskExecutor:
    def __init__(
        self,
        node_id: str,
        *,
        runner=None,
        state_path: Path | None = None,
        benchmark_executor: SafeBenchmarkExecutor | None = None,
        preparation_executor: ArtifactPreparationExecutor | None = None,
        quarantine_executor: ArtifactCacheQuarantineExecutor | None = None,
        artifact_origin_config: object = None,
        manifest_loader: Callable[[str], dict] | None = None,
        build_commit: str | None | object = _BUILD_COMMIT_UNSET,
    ) -> None:
        self.node_id = node_id
        self.runner = runner
        self.state_path = state_path
        self.build_commit = (
            _load_build_commit(DEFAULT_BUILD_COMMIT)
            if build_commit is _BUILD_COMMIT_UNSET
            else (
                build_commit
                if type(build_commit) is str
                and re.fullmatch(r"[0-9a-f]{40,64}", build_commit)
                else None
            )
        )
        if benchmark_executor is None:
            from .benchmark_runtime import SafeBenchmarkRuntime

            benchmark_executor = SafeBenchmarkRuntime(runner=runner)
        self.benchmark_executor = benchmark_executor
        self.preparation_executor = preparation_executor or ArtifactPreparationExecutor(
            node_id,
            runner=runner,
            origin_config=artifact_origin_config,
            manifest_loader=manifest_loader,
        )
        self.quarantine_executor = quarantine_executor or ArtifactCacheQuarantineExecutor(
            node_id,
            runner=runner,
        )

    def _profile(self):
        profile = NodeProbe(self.runner).collect()
        profile.node_id = self.node_id
        return profile

    def preparation_progress(self, task_id: str) -> dict[str, int] | None:
        reader = getattr(self.preparation_executor, "progress_snapshot", None)
        return reader(task_id) if callable(reader) else None

    def clear_preparation_progress(self, task_id: str) -> None:
        clearer = getattr(self.preparation_executor, "clear_progress", None)
        if callable(clearer):
            clearer(task_id)

    def _deployment_task(self, task: dict, kind: TaskType):
        payload = task.get("payload")
        if type(payload) is not dict:
            raise ValueError("deployment task payload must be an object")
        option_fields = {
            TaskType.APPLY_DEPLOYMENT: {
                "serve",
                "accept_model_download",
                "pull_image",
            },
            TaskType.START_DEPLOYMENT: {
                "serve",
                "accept_model_download",
                "pull_image",
            },
            TaskType.STOP_DEPLOYMENT: {
                "accept_model_download",
                "pull_image",
            },
            TaskType.RESTART_DEPLOYMENT: {
                "serve",
                "accept_model_download",
                "pull_image",
            },
            TaskType.VERIFY: {
                "api",
                "accept_model_download",
                "pull_image",
            },
        }[kind]
        allowed = {"plan", "generation", *option_fields}
        unexpected = sorted(set(payload) - allowed)
        if unexpected:
            raise ValueError("deployment task payload contains unexpected fields")
        if "plan" not in payload or "generation" not in payload:
            raise ValueError("deployment task requires a plan and generation")
        if any(
            field in payload and type(payload[field]) is not bool
            for field in option_fields
        ):
            raise ValueError("deployment task options must be strict booleans")
        if type(payload["generation"]) is not int:
            raise ValueError("deployment generation must be an integer")
        if type(payload["plan"]) is not dict:
            raise ValueError("deployment plan must be an object")
        plan = DeploymentPlan.from_dict(payload["plan"])
        deployment_id = task.get("deployment_id")
        if (
            type(deployment_id) is not str
            or deployment_id != plan.deployment_id
        ):
            raise ValueError("task deployment identity does not match its plan")
        if payload["generation"] != plan.generation:
            raise ValueError("deployment generation mismatch")
        assignment = plan.assignment_for(self.node_id)
        if assignment is None:
            raise ValueError("node is not assigned to deployment")
        if "@sha256:" not in plan.image:
            raise ValueError("central deployment image is not digest-pinned")
        if is_strict_pipeline_plan(plan):
            validate_strict_pipeline_plan(
                plan,
                require_manifest_cache_path=(kind != TaskType.STOP_DEPLOYMENT),
                validate_model_path=(kind != TaskType.STOP_DEPLOYMENT),
            )
        return payload, plan, assignment

    def execute(self, task: dict) -> dict:
        task_type = task.get("type") if type(task) is dict else None
        if is_artifact_preparation_task(task_type):
            return self.preparation_executor.execute(task)
        try:
            kind = TaskType(task["type"])
        except (KeyError, ValueError) as exc:
            raise ValueError("unsupported task type") from exc
        payload = task.get("payload") or {}
        if kind == TaskType.PROBE:
            return {"profile": self._profile().to_dict()}
        if kind == TaskType.UNJOIN_NODE:
            if payload:
                raise ValueError("UNJOIN_NODE payload must be empty")
            store = StateStore(self.state_path or DEFAULT_STATE)
            state = store.load()
            checks = []
            if state.deployment_id:
                check = ContainerRuntime(
                    self.runner, "docker"
                ).stop_registered_node_deployment(
                    state.deployment_id,
                    generation=state.generation,
                    node_id=self.node_id,
                )
                checks.append(check)
                if not check.ok:
                    raise RuntimeError(check.detail)
            state.phase = "UNJOINING"
            state.detail = "Deployment GPU released; waiting for control-plane revocation"
            store.save(state)
            return {"checks": [item.to_dict() for item in checks], "unjoined": True}
        if kind == TaskType.QUARANTINE_ARTIFACT_CACHE:
            return self.quarantine_executor.execute(task)
        if kind == TaskType.BENCHMARK:
            benchmark = _validated_benchmark_payload(payload)
            if not benchmark.apply:
                raise BenchmarkAgentError(
                    "BENCHMARK requires explicit apply approval",
                    failure_code="BENCHMARK_PAYLOAD_REJECTED",
                )
            if benchmark.coordinator_node_id != self.node_id:
                raise BenchmarkAgentError(
                    "BENCHMARK coordinator does not match this node",
                    failure_code="BENCHMARK_PAYLOAD_REJECTED",
                )
            if self.node_id not in benchmark.node_ids:
                raise BenchmarkAgentError(
                    "this node is not assigned to BENCHMARK",
                    failure_code="BENCHMARK_PAYLOAD_REJECTED",
                )
            if len(benchmark.node_ids) != 1:
                raise BenchmarkAgentError(
                    "multi-node BENCHMARK execution is not supported",
                    failure_code="BENCHMARK_PAYLOAD_REJECTED",
                )
            if self.benchmark_executor is None:
                raise BenchmarkAgentError(
                    "safe BENCHMARK executor is not configured",
                    failure_code="BENCHMARK_RUNTIME_UNAVAILABLE",
                )
            reconcile = getattr(self.benchmark_executor, "reconcile", None)
            if callable(reconcile):
                reconcile(benchmark)
            _require_benchmark_build_commit(benchmark, self.build_commit)
            profile = self._profile()
            if (
                benchmark_profile_fingerprint(self.node_id, profile)
                != benchmark.inventory_fingerprint
            ):
                raise BenchmarkAgentError(
                    "BENCHMARK inventory fingerprint mismatch",
                    failure_code="BENCHMARK_PAYLOAD_REJECTED",
                )
            if benchmark.prepare_model or benchmark.pull_image:
                ignored_name = f"dure-benchmark-{benchmark.benchmark_id}"
                active_workloads = [
                    workload
                    for workload in profile.workloads
                    if workload.name != ignored_name
                    and not workload.status.strip().lower().startswith(
                        ("exited", "dead")
                    )
                ]
                if active_workloads:
                    raise BenchmarkAgentError(
                        "BENCHMARK asset preparation is refused while another workload may be active",
                        failure_code="BENCHMARK_RUNTIME_UNAVAILABLE",
                    )
            if benchmark.prepare_model:
                task_id = task.get("id")
                if type(task_id) is not str:
                    raise BenchmarkAgentError(
                        "BENCHMARK task identity is invalid",
                        failure_code="BENCHMARK_PAYLOAD_REJECTED",
                    )
                try:
                    self.preparation_executor.prepare_benchmark_model(
                        task_id,
                        benchmark,
                    )
                except Exception as exc:
                    raise BenchmarkAgentError(
                        "BENCHMARK model preparation failed",
                        failure_code="BENCHMARK_ARTIFACT_UNAVAILABLE",
                    ) from exc
            if benchmark.pull_image:
                try:
                    self.preparation_executor.prepare_benchmark_image(benchmark)
                except Exception as exc:
                    raise BenchmarkAgentError(
                        "BENCHMARK image preparation failed",
                        failure_code="BENCHMARK_RUNTIME_UNAVAILABLE",
                    ) from exc
            if benchmark.prepare_model or benchmark.pull_image:
                profile = self._profile()
                if (
                    benchmark_profile_fingerprint(self.node_id, profile)
                    != benchmark.inventory_fingerprint
                ):
                    raise BenchmarkAgentError(
                        "BENCHMARK inventory fingerprint mismatch",
                        failure_code="BENCHMARK_PAYLOAD_REJECTED",
                    )
            cached_model = _exact_cached_model(profile, benchmark)
            result = self.benchmark_executor(benchmark, profile, cached_model)
            return _validated_benchmark_result(benchmark, result)
        payload, plan, assignment = self._deployment_task(task, kind)
        if kind == TaskType.STOP_DEPLOYMENT and is_strict_pipeline_plan(plan):
            # STOP remains available when probing the GPU, network, cache, or
            # NVIDIA runtime is impossible.  The strict backend is Docker-only
            # and stop_deployment independently verifies every exact label.
            check = ContainerRuntime(self.runner, "docker").stop_deployment(
                plan.deployment_id,
                generation=plan.generation,
                node_id=self.node_id,
                plan=plan,
                assignment=assignment,
            )
            if not check.ok:
                raise RuntimeError(check.detail)
            store = StateStore(self.state_path or DEFAULT_STATE)
            state = store.load()
            state.phase = "PLANNED"
            state.detail = "Deployment containers are stopped"
            store.save(state)
            return {"checks": [check.to_dict()]}
        profile = self._profile()
        if is_strict_pipeline_plan(plan) and kind != TaskType.STOP_DEPLOYMENT:
            validate_strict_pipeline_node(
                plan, assignment, profile, require_model_cache=True
            )
        if kind == TaskType.VERIFY:
            verifier = ReadinessVerifier(
                self.runner,
                profile.runtime.engine or "docker",
                node_id=self.node_id,
            )
            checks = [verifier.host_gpu(profile), verifier.container_gpu(plan, assignment)]
            if is_strict_pipeline_plan(plan):
                checks.append(
                    verifier.pipeline_rank_contract(
                        plan,
                        assignment,
                        profile,
                        require_actors=bool(payload.get("api")),
                    )
                )
            else:
                checks.append(verifier.ray_cluster(plan))
            if payload.get("api") and assignment.role == "ray-head":
                checks.append(verifier.api(plan=plan))
            if not all(item.ok for item in checks):
                raise RuntimeError("; ".join(item.detail for item in checks if not item.ok))
            return {"checks": [item.to_dict() for item in checks], "ok": True}
        runtime = ContainerRuntime(self.runner, profile.runtime.engine or "docker")
        if kind == TaskType.STOP_DEPLOYMENT:
            check = runtime.stop_deployment(
                plan.deployment_id,
                generation=plan.generation,
                node_id=self.node_id,
                plan=plan if is_strict_pipeline_plan(plan) else None,
                assignment=assignment if is_strict_pipeline_plan(plan) else None,
            )
            if not check.ok:
                raise RuntimeError(check.detail)
            store = StateStore(self.state_path or DEFAULT_STATE)
            state = store.load()
            state.phase = "PLANNED"
            state.detail = "Deployment containers are stopped"
            store.save(state)
            return {"checks": [check.to_dict()]}
        if kind == TaskType.RESTART_DEPLOYMENT:
            if is_stage_pipeline_plan(plan):
                # A restart is allowed to stop a healthy container only after
                # the exact immutable replacement cache is known to be usable.
                # Emergency STOP intentionally remains independent of cache IO.
                validate_strict_stage_cache(plan, assignment)
            stopped = runtime.stop_deployment(
                plan.deployment_id,
                generation=plan.generation,
                node_id=self.node_id,
                plan=plan if is_strict_pipeline_plan(plan) else None,
                assignment=assignment if is_strict_pipeline_plan(plan) else None,
            )
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
    def __init__(
        self,
        config: dict,
        *,
        config_path: Path = DEFAULT_CONFIG,
        history_path: Path = DEFAULT_HISTORY,
        runner=None,
        benchmark_executor: SafeBenchmarkExecutor | None = None,
        build_commit: str | None | object = _BUILD_COMMIT_UNSET,
    ) -> None:
        self.config = config
        self.config_path = config_path
        if config.get("verify_tls", True) and not config["server"].startswith("https://"):
            raise ValueError("agent control-plane URL must use HTTPS")
        self.client = JSONClient(config["server"], config["credential"], verify_tls=config.get("verify_tls", True))
        self.history_path = history_path
        history = _read_json(
            history_path,
            {"completed": {}, "pending_reports": {}},
        )
        if type(history) is not dict:
            history = {}
        if type(history.get("completed")) is not dict:
            history["completed"] = {}
        if type(history.get("pending_reports")) is not dict:
            history["pending_reports"] = {}
        self.history = history
        self.state_path = Path(config.get("state_file", DEFAULT_STATE))
        self.build_commit = (
            _load_build_commit(DEFAULT_BUILD_COMMIT)
            if build_commit is _BUILD_COMMIT_UNSET
            else (
                build_commit
                if type(build_commit) is str
                and re.fullmatch(r"[0-9a-f]{40,64}", build_commit)
                else None
            )
        )
        self.runner = runner or SubprocessRunner()
        artifact_origin_config = config.get("artifact_origin")
        if (
            artifact_origin_config is None
            and config.get("verify_tls", True) is True
            and type(config.get("server")) is str
            and config["server"].startswith("https://")
        ):
            artifact_origin_config = {
                "base_url": config["server"].rstrip("/"),
                "allowed_redirect_hosts": [],
            }
        self.executor = TaskExecutor(
            config["node_id"],
            runner=self.runner,
            state_path=self.state_path,
            benchmark_executor=benchmark_executor,
            artifact_origin_config=artifact_origin_config,
            manifest_loader=self._load_artifact_manifest,
            build_commit=self.build_commit,
        )
        self.running = True

    def _save_history(self) -> None:
        completed = self.history["completed"]
        self.history["completed"] = dict(list(completed.items())[-1000:])
        _atomic_json(self.history_path, self.history)

    def _record_terminal_report(
        self,
        task_id: str,
        history_record: dict,
    ) -> None:
        status_value = history_record.get("status")
        if status_value == "complete" and type(history_record.get("result")) is dict:
            pending_report = {
                "status": "complete",
                "result": history_record["result"],
            }
        elif status_value == "failed" and type(history_record.get("error")) is str:
            pending_report = {
                "status": "failed",
                "error": history_record["error"],
            }
        else:
            raise ValueError("terminal task history record is invalid")
        self.history["completed"][task_id] = dict(history_record)
        self.history["pending_reports"][task_id] = pending_report
        self._save_history()

    @staticmethod
    def _pending_report_request(
        task_id,
        report,
    ) -> tuple[str, dict] | None:
        if (
            type(task_id) is not str
            or PENDING_REPORT_TASK_ID_PATTERN.fullmatch(task_id) is None
            or type(report) is not dict
        ):
            return None
        if (
            set(report) == {"status", "result"}
            and report.get("status") == "complete"
            and type(report.get("result")) is dict
        ):
            return (
                f"/v1/agent/tasks/{task_id}/complete",
                {"result": report["result"]},
            )
        if (
            set(report) == {"status", "error"}
            and report.get("status") == "failed"
            and type(report.get("error")) is str
            and len(report["error"]) <= 8192
        ):
            return (
                f"/v1/agent/tasks/{task_id}/fail",
                {"error": report["error"]},
            )
        return None

    @staticmethod
    def _report_error_is_retryable(exc: APIError) -> bool:
        status_code = getattr(exc, "status_code", None)
        return (
            type(status_code) is not int
            or status_code >= 500
            or status_code in RETRYABLE_REPORT_HTTP_STATUSES
        )

    def _drop_pending_report(self, task_id) -> None:
        pending_reports = self.history["pending_reports"]
        if task_id in pending_reports:
            del pending_reports[task_id]
            self._save_history()

    def _send_pending_report(self, task_id) -> bool:
        pending_reports = self.history["pending_reports"]
        if task_id not in pending_reports:
            return False
        request = self._pending_report_request(
            task_id,
            pending_reports[task_id],
        )
        if request is None:
            LOG.error("discarding malformed pending agent task report")
            self._drop_pending_report(task_id)
            return False
        path, payload = request
        try:
            self.client.request("POST", path, payload)
        except APIError as exc:
            if self._report_error_is_retryable(exc):
                raise
            LOG.warning(
                "discarding pending task %s report after terminal HTTP %s",
                task_id,
                exc.status_code,
            )
            self._drop_pending_report(task_id)
            return False
        self._drop_pending_report(task_id)
        return True

    def _flush_pending_reports(self) -> bool:
        task_ids = list(self.history["pending_reports"])
        if not task_ids:
            return False
        for task_id in task_ids:
            self._send_pending_report(task_id)
        return True

    def _load_artifact_manifest(self, task_id: str) -> dict:
        response = self.client.request(
            "GET",
            f"/v1/agent/tasks/{task_id}/artifact-manifest",
        )
        if (
            type(response) is not dict
            or set(response) != {"manifest"}
            or type(response["manifest"]) is not dict
        ):
            raise ValueError("artifact manifest response is invalid")
        return response["manifest"]

    def _task_heartbeat_payload(
        self, task_id: str, *, is_preparation: bool
    ) -> dict | None:
        if not is_preparation:
            return None
        progress_reader = getattr(
            self.executor,
            "preparation_progress",
            None,
        )
        progress = (
            progress_reader(task_id) if callable(progress_reader) else None
        )
        return {"progress": progress} if progress is not None else None

    def stop(self, *_args) -> None:
        self.running = False

    def _finish_unjoin(self) -> None:
        disabled = self.runner.run(
            ["systemctl", "disable", "dure-agent"], timeout=60
        )
        if not disabled.ok:
            LOG.warning(
                "node was unjoined but dure-agent could not be disabled: %s",
                disabled.stderr or disabled.stdout,
            )
        try:
            _retire_local_registration(
                self.config_path, self.config, state_path=self.state_path
            )
        except OSError as exc:
            LOG.error("node was unjoined but local credential cleanup failed: %s", exc)
        self.running = False

    def once(self) -> bool:
        state = StateStore(self.state_path).load().to_dict()
        self.client.request(
            "POST",
            "/v1/agent/heartbeat",
            {"state": state, "agent_version": __version__},
        )
        if self._flush_pending_reports():
            return True
        task = self.client.request("POST", "/v1/agent/tasks/claim").get("task")
        if task is None:
            return False
        task_id = task["id"]
        is_benchmark = task.get("type") == TaskType.BENCHMARK.value
        is_preparation = is_artifact_preparation_task(task.get("type"))
        is_quarantine = (
            task.get("type") == TaskType.QUARANTINE_ARTIFACT_CACHE.value
        )
        previous = self.history.get("completed", {}).get(task_id)
        if previous is not None:
            if is_preparation:
                try:
                    status_value, replay = validate_preparation_history(
                        task,
                        previous,
                        self.executor.node_id,
                    )
                except Exception:
                    status_value = "failed"
                    replay = "PREPARATION_HISTORY_INVALID"
                    self.history.setdefault("completed", {})[task_id] = {
                        "status": status_value,
                        "error": replay,
                    }
                    _atomic_json(self.history_path, self.history)
                if status_value == "failed":
                    self.client.request(
                        "POST",
                        f"/v1/agent/tasks/{task_id}/fail",
                        {"error": replay},
                    )
                else:
                    self.client.request(
                        "POST",
                        f"/v1/agent/tasks/{task_id}/complete",
                        {"result": replay},
                    )
            elif is_benchmark:
                try:
                    benchmark = _validated_benchmark_payload(
                        task.get("payload") or {}
                    )
                except Exception as exc:
                    error = _benchmark_failure_code(exc)
                    self.history.setdefault("completed", {})[task_id] = {
                        "status": "failed",
                        "error": error,
                    }
                    _atomic_json(self.history_path, self.history)
                    self.client.request(
                        "POST",
                        f"/v1/agent/tasks/{task_id}/fail",
                        {"error": error},
                    )
                else:
                    if type(previous) is not dict:
                        error = "BENCHMARK_EXECUTION_FAILED"
                        self.history.setdefault("completed", {})[task_id] = {
                            "status": "failed",
                            "error": error,
                        }
                        _atomic_json(self.history_path, self.history)
                        self.client.request(
                            "POST",
                            f"/v1/agent/tasks/{task_id}/fail",
                            {"error": error},
                        )
                    elif previous.get("status") == "failed":
                        error = previous.get("error")
                        if (
                            type(error) is not str
                            or error not in BENCHMARK_AGENT_FAILURE_CODES
                        ):
                            error = "BENCHMARK_EXECUTION_FAILED"
                            self.history.setdefault("completed", {})[task_id] = {
                                "status": "failed",
                                "error": error,
                            }
                            _atomic_json(self.history_path, self.history)
                        self.client.request(
                            "POST",
                            f"/v1/agent/tasks/{task_id}/fail",
                            {"error": error},
                        )
                    else:
                        try:
                            if (
                                previous.get("executed_dure_commit")
                                != benchmark.dure_commit
                            ):
                                raise BenchmarkAgentError(
                                    "BENCHMARK history does not match the task payload",
                                    failure_code="BENCHMARK_PAYLOAD_REJECTED",
                                )
                            result = _validated_benchmark_result(
                                benchmark,
                                previous.get("result", previous),
                            )
                        except Exception as exc:
                            error = _benchmark_failure_code(exc)
                            self.history.setdefault("completed", {})[task_id] = {
                                "status": "failed",
                                "error": error,
                            }
                            _atomic_json(self.history_path, self.history)
                            self.client.request(
                                "POST",
                                f"/v1/agent/tasks/{task_id}/fail",
                                {"error": error},
                            )
                        else:
                            self.history.setdefault("completed", {})[task_id] = {
                                "status": "complete",
                                "result": result,
                                "executed_dure_commit": benchmark.dure_commit,
                            }
                            _atomic_json(self.history_path, self.history)
                            self.client.request(
                                "POST",
                                f"/v1/agent/tasks/{task_id}/complete",
                                {"result": result},
                            )
            elif is_quarantine:
                if type(previous) is not dict or previous.get("status") == "failed":
                    error = (
                        previous.get("error")
                        if type(previous) is dict
                        else None
                    )
                    if error not in ARTIFACT_CACHE_QUARANTINE_FAILURE_CODES:
                        error = "CACHE_QUARANTINE_EXECUTION_FAILED"
                        self.history.setdefault("completed", {})[task_id] = {
                            "status": "failed",
                            "error": error,
                        }
                        _atomic_json(self.history_path, self.history)
                    self.client.request(
                        "POST",
                        f"/v1/agent/tasks/{task_id}/fail",
                        {"error": error},
                    )
                else:
                    try:
                        result = validate_artifact_cache_quarantine_result(
                            task,
                            previous.get("result", previous),
                            self.executor.node_id,
                        )
                    except Exception:
                        error = "CACHE_QUARANTINE_EXECUTION_FAILED"
                        self.history.setdefault("completed", {})[task_id] = {
                            "status": "failed",
                            "error": error,
                        }
                        _atomic_json(self.history_path, self.history)
                        self.client.request(
                            "POST",
                            f"/v1/agent/tasks/{task_id}/fail",
                            {"error": error},
                        )
                    else:
                        self.client.request(
                            "POST",
                            f"/v1/agent/tasks/{task_id}/complete",
                            {"result": result},
                        )
            elif previous.get("status") == "failed":
                self.client.request("POST", f"/v1/agent/tasks/{task_id}/fail", {"error": previous["error"]})
            else:
                result = previous.get("result", previous)
                self.client.request("POST", f"/v1/agent/tasks/{task_id}/complete", {"result": result})
                if task.get("type") == TaskType.UNJOIN_NODE.value:
                    self._finish_unjoin()
            return True
        renewal_stop = threading.Event()

        def renew_lease() -> None:
            while not renewal_stop.wait(60):
                try:
                    heartbeat_payload = self._task_heartbeat_payload(
                        task_id,
                        is_preparation=is_preparation,
                    )
                    self.client.request(
                        "POST",
                        f"/v1/agent/tasks/{task_id}/heartbeat",
                        heartbeat_payload,
                    )
                except APIError as exc:
                    LOG.warning("could not renew task %s lease: %s", task_id, exc)

        renewal = threading.Thread(target=renew_lease, name=f"dure-lease-{task_id}", daemon=True)
        renewal.start()
        try:
            try:
                result = self.executor.execute(task)
                if is_preparation:
                    result = validate_preparation_result(
                        task,
                        result,
                        self.executor.node_id,
                    )
                elif is_quarantine:
                    result = validate_artifact_cache_quarantine_result(
                        task,
                        result,
                        self.executor.node_id,
                    )
            except Exception as exc:
                if is_benchmark and getattr(exc, "defer_benchmark", False) is True:
                    LOG.warning(
                        "BENCHMARK task %s is already running; deferring completion",
                        task_id,
                    )
                else:
                    if is_benchmark:
                        error = _benchmark_failure_code(exc)
                        LOG.error("BENCHMARK task %s failed with %s", task_id, error)
                    elif is_preparation:
                        error = preparation_failure_code(exc)
                        LOG.error(
                            "artifact preparation task %s failed with %s",
                            task_id,
                            error,
                        )
                    elif is_quarantine:
                        error = artifact_cache_quarantine_failure_code(exc)
                        LOG.error(
                            "artifact cache quarantine task %s failed with %s",
                            task_id,
                            error,
                        )
                    else:
                        LOG.exception("task %s failed", task_id)
                        error = str(exc)[:8192]
                    history_record = {
                        "status": "failed",
                        "error": error,
                    }
                    self._record_terminal_report(task_id, history_record)
                    self._send_pending_report(task_id)
            else:
                history_record = {
                    "status": "complete",
                    "result": result,
                }
                if is_benchmark:
                    benchmark = _validated_benchmark_payload(
                        task.get("payload") or {}
                    )
                    history_record["executed_dure_commit"] = (
                        benchmark.dure_commit
                    )
                self._record_terminal_report(task_id, history_record)
                reported = self._send_pending_report(task_id)
                if reported and task.get("type") == TaskType.UNJOIN_NODE.value:
                    self._finish_unjoin()
        finally:
            renewal_stop.set()
            renewal.join(timeout=2)
            if is_preparation:
                progress_clearer = getattr(
                    self.executor,
                    "clear_preparation_progress",
                    None,
                )
                if callable(progress_clearer):
                    progress_clearer(task_id)
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
        agent = Agent(config, config_path=args.config, history_path=args.history)
        signal.signal(signal.SIGTERM, agent.stop)
        signal.signal(signal.SIGINT, agent.stop)
        agent.run(args.interval)
        return 0
    except (APIError, OSError, ValueError) as exc:
        parser.error(str(exc))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

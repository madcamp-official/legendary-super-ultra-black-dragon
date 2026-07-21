from __future__ import annotations

import json
import math
import os
import re
import stat
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

from .command import Runner, SubprocessRunner
from .model_cache import (
    MODEL_CACHE_KIND_FULL_SNAPSHOT,
    MODEL_CACHE_MARKER_FILE,
    MODEL_CACHE_VERIFICATION_VERSION,
    ModelCacheMarkerError,
    read_model_cache_marker,
)
from .models import InstalledModelProfile, NodeProfile
from .probe import DEFAULT_MODEL_ROOTS
from .task import (
    BENCHMARK_DURATION_SECONDS,
    BENCHMARK_REQUEST_COUNT,
    BENCHMARK_WARMUP_REQUESTS,
    BENCHMARK_WORKLOAD_DIMENSIONS,
    BENCHMARK_WORKLOAD_IDS,
    MAX_BENCHMARK_INTEGER,
    BenchmarkTaskPayload,
)


BENCHMARK_ENTRYPOINT = "dure-benchmark"
BENCHMARK_ENTRYPOINT_HOST_PATH = Path("/usr/lib/dure/dure-benchmark")
BENCHMARK_ENTRYPOINT_CONTAINER_PATH = "/usr/local/bin/dure-benchmark"
MAX_BENCHMARK_ENTRYPOINT_BYTES = 64 * 1024
MAX_SUMMARY_BYTES = 32 * 1024
MAX_BENCHMARK_OUTPUT_BYTES = 64 * 1024
MAX_MODEL_CONFIG_BYTES = 1024 * 1024
MIN_BENCHMARK_MEMORY_LIMIT_MIB = 8 * 1024
MAX_BENCHMARK_MEMORY_LIMIT_MIB = 32 * 1024
MAX_BENCHMARK_CPUS = 8.0
BENCHMARK_CONTAINER_GRACE_SECONDS = 60
NVIDIA_COMPUTE_QUERY_COMMAND = (
    "nvidia-smi",
    "--query-compute-apps=gpu_uuid",
    "--format=csv,noheader,nounits",
)
BENCHMARK_CONTAINER_INSPECT_FORMAT = (
    "{{.Id}}\t{{.State.Status}}\t{{.State.StartedAt}}\t"
    '{{index .Config.Labels "dure.managed"}}\t'
    '{{index .Config.Labels "dure.kind"}}\t'
    '{{index .Config.Labels "dure.benchmark"}}\t'
    '{{index .Config.Labels "dure.release"}}\t'
    '{{index .Config.Labels "dure.placement"}}\t'
    '{{index .Config.Labels "dure.workload"}}\t'
    '{{or (index .Config.Labels "dure.deployment") "-"}}'
)


class BenchmarkRuntimeError(RuntimeError):
    """A fail-closed benchmark runtime error with no raw container output."""

    ALLOWED_CODES = frozenset(
        {
            "BENCHMARK_EXECUTION_FAILED",
            "BENCHMARK_RUNTIME_UNAVAILABLE",
            "BENCHMARK_ARTIFACT_UNAVAILABLE",
        }
    )

    def __init__(
        self, message: str, *, code: str = "BENCHMARK_EXECUTION_FAILED"
    ) -> None:
        if code not in self.ALLOWED_CODES:
            raise ValueError("unsupported benchmark runtime failure code")
        super().__init__(message)
        self.failure_code = code
        self.code = code


class BenchmarkRuntimeDeferred(RuntimeError):
    """Signal that the exact leased benchmark is still running and must be retried."""

    defer_benchmark = True


@dataclass(frozen=True)
class BenchmarkWorkload:
    workload_id: str
    input_tokens: int | None
    output_tokens: int
    concurrency: int
    warmup_requests: int = BENCHMARK_WARMUP_REQUESTS
    request_count: int = BENCHMARK_REQUEST_COUNT
    duration_seconds: float = BENCHMARK_DURATION_SECONDS

    def to_dict(self) -> dict[str, float | int | str | None]:
        return asdict(self)


BENCHMARK_WORKLOADS: Mapping[str, BenchmarkWorkload] = MappingProxyType(
    {
        "short-chat-1k-128": BenchmarkWorkload(
            "short-chat-1k-128",
            *BENCHMARK_WORKLOAD_DIMENSIONS["short-chat-1k-128"],
        ),
        "long-chat-4k-256": BenchmarkWorkload(
            "long-chat-4k-256",
            *BENCHMARK_WORKLOAD_DIMENSIONS["long-chat-4k-256"],
        ),
        "max-context": BenchmarkWorkload(
            "max-context", input_tokens=None, output_tokens=256, concurrency=1
        ),
        "quality-eval": BenchmarkWorkload(
            "quality-eval",
            *BENCHMARK_WORKLOAD_DIMENSIONS["quality-eval"],
        ),
    }
)

if frozenset(BENCHMARK_WORKLOADS) != BENCHMARK_WORKLOAD_IDS:
    raise RuntimeError("benchmark runtime workload allowlist does not match the task contract")


_METRIC_FIELDS = frozenset(
    {
        "warmup_requests",
        "request_count",
        "duration_seconds",
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
    }
)

_SINGLE_NODE_METRIC_FIELDS = (
    "network_bandwidth_mbps",
    "network_rtt_ms",
    "packet_loss_pct",
    "nccl_all_reduce_ok",
)


def _validated_payload(value: BenchmarkTaskPayload) -> BenchmarkTaskPayload:
    if type(value) is not BenchmarkTaskPayload:
        raise TypeError("benchmark runtime requires a validated BenchmarkTaskPayload")
    field_names = {item.name for item in fields(value)}
    if set(vars(value)) != field_names:
        raise ValueError("benchmark task payload contains unexpected state")
    payload = {name: getattr(value, name) for name in field_names}
    payload["node_ids"] = list(value.node_ids)
    normalized = BenchmarkTaskPayload.from_dict(payload)
    if normalized != value:
        raise ValueError("benchmark task payload is not canonical")
    return normalized


def _validated_model_path(
    payload: BenchmarkTaskPayload, cached_model: InstalledModelProfile
) -> Path:
    if type(cached_model) is not InstalledModelProfile:
        raise TypeError("benchmark runtime requires a probed InstalledModelProfile")
    if (
        cached_model.source != "dure"
        or not cached_model.complete
        or cached_model.model_id != payload.model_repository
        or cached_model.revision != payload.artifact_revision
        or cached_model.manifest_digest != payload.artifact_manifest_digest
        or cached_model.quantization != payload.quantization
        or cached_model.cache_kind != MODEL_CACHE_KIND_FULL_SNAPSHOT
        or cached_model.verification_version != MODEL_CACHE_VERIFICATION_VERSION
        or not cached_model.path
    ):
        raise BenchmarkRuntimeError(
            "benchmark artifact does not exactly match the local cache",
            code="BENCHMARK_ARTIFACT_UNAVAILABLE",
        )
    if any(marker in cached_model.path for marker in ("\x00", "\r", "\n", ",")):
        raise BenchmarkRuntimeError(
            "benchmark cache path cannot be represented safely",
            code="BENCHMARK_ARTIFACT_UNAVAILABLE",
        )
    candidate = Path(cached_model.path)
    if not candidate.is_absolute():
        raise BenchmarkRuntimeError(
            "benchmark cache path must be absolute",
            code="BENCHMARK_ARTIFACT_UNAVAILABLE",
        )
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise BenchmarkRuntimeError(
            "benchmark cache path is unavailable",
            code="BENCHMARK_ARTIFACT_UNAVAILABLE",
        ) from exc
    if not _safe_owned_directory(resolved) or not (resolved / "config.json").is_file():
        raise BenchmarkRuntimeError(
            "benchmark cache is incomplete",
            code="BENCHMARK_ARTIFACT_UNAVAILABLE",
        )
    trusted_root = DEFAULT_MODEL_ROOTS[0].resolve()
    if not _safe_owned_directory(trusted_root) or not resolved.is_relative_to(trusted_root):
        raise BenchmarkRuntimeError(
            "benchmark cache is outside the trusted model roots",
            code="BENCHMARK_ARTIFACT_UNAVAILABLE",
        )
    _validated_cache_metadata(resolved, payload)
    return resolved


def _safe_owned_directory(path: Path) -> bool:
    try:
        observed = path.lstat()
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError, ValueError):
        return False
    return (
        stat.S_ISDIR(observed.st_mode)
        and observed.st_uid == os.geteuid()
        and not observed.st_mode & 0o022
        and resolved == Path(os.path.abspath(path))
    )


def _validated_cache_metadata(
    model_path: Path, payload: BenchmarkTaskPayload
) -> None:
    metadata_path = model_path / MODEL_CACHE_MARKER_FILE
    try:
        metadata = read_model_cache_marker(metadata_path)
    except ModelCacheMarkerError as exc:
        raise BenchmarkRuntimeError(
            "benchmark cache metadata is invalid",
            code="BENCHMARK_ARTIFACT_UNAVAILABLE",
        ) from exc
    if (
        metadata.repository != payload.model_repository
        or metadata.revision != payload.artifact_revision
        or metadata.manifest_digest != payload.artifact_manifest_digest
        or metadata.quantization != payload.quantization
        or metadata.cache_kind != MODEL_CACHE_KIND_FULL_SNAPSHOT
        or metadata.verification_version != MODEL_CACHE_VERIFICATION_VERSION
    ):
        raise BenchmarkRuntimeError(
            "benchmark cache metadata does not match the prepared artifact",
            code="BENCHMARK_ARTIFACT_UNAVAILABLE",
        )


def _validated_gpu_uuid(payload: BenchmarkTaskPayload, profile: NodeProfile) -> str:
    if type(profile) is not NodeProfile:
        raise TypeError("benchmark runtime requires a probed NodeProfile")
    if (
        profile.node_id != payload.coordinator_node_id
        or payload.node_ids != (profile.node_id,)
    ):
        raise ValueError("multi-node or mismatched benchmark execution is not supported")
    if (
        profile.runtime.engine != "docker"
        or not profile.runtime.engine_ready
        or not profile.runtime.nvidia_runtime
    ):
        raise BenchmarkRuntimeError(
            "Docker with the NVIDIA runtime is unavailable",
            code="BENCHMARK_RUNTIME_UNAVAILABLE",
        )
    healthy = [gpu for gpu in profile.gpus if gpu.healthy]
    if not healthy:
        raise BenchmarkRuntimeError(
            "no healthy GPU is available for the benchmark",
            code="BENCHMARK_RUNTIME_UNAVAILABLE",
        )
    selected = max(healthy, key=lambda gpu: (gpu.memory_mib, -gpu.index))
    if (
        type(selected.memory_mib) is not int
        or selected.memory_mib <= 0
        or type(selected.index) is not int
        or selected.index < 0
        or type(selected.uuid) is not str
        or re.fullmatch(r"GPU-[0-9A-Fa-f-]{16,64}", selected.uuid) is None
    ):
        raise BenchmarkRuntimeError(
            "selected GPU identity is invalid",
            code="BENCHMARK_RUNTIME_UNAVAILABLE",
        )
    return selected.uuid


def _benchmark_resource_limits(profile: NodeProfile) -> tuple[str, str]:
    if (
        type(profile.memory_mib) is not int
        or type(profile.memory_available_mib) is not int
        or profile.memory_mib <= 0
        or profile.memory_available_mib <= 0
        or type(profile.cpu_count) is not int
        or profile.cpu_count <= 0
    ):
        raise BenchmarkRuntimeError(
            "benchmark host resource capacity is invalid",
            code="BENCHMARK_RUNTIME_UNAVAILABLE",
        )
    usable_memory_mib = min(profile.memory_mib, profile.memory_available_mib)
    memory_limit_mib = min(
        MAX_BENCHMARK_MEMORY_LIMIT_MIB,
        usable_memory_mib // 2,
    )
    if memory_limit_mib < MIN_BENCHMARK_MEMORY_LIMIT_MIB:
        raise BenchmarkRuntimeError(
            "benchmark host has insufficient safely bounded memory",
            code="BENCHMARK_RUNTIME_UNAVAILABLE",
        )
    cpu_limit = min(MAX_BENCHMARK_CPUS, profile.cpu_count / 2)
    return f"{memory_limit_mib}m", f"{cpu_limit:g}"


def _ensure_no_active_workloads(
    profile: NodeProfile, *, ignored_benchmark_id: str | None = None
) -> None:
    ignored_name = (
        f"dure-benchmark-{ignored_benchmark_id}"
        if ignored_benchmark_id is not None
        else None
    )
    active_workloads = [
        workload
        for workload in profile.workloads
        if workload.name != ignored_name
        and not workload.status.strip().lower().startswith(("exited", "dead"))
    ]
    if active_workloads:
        raise BenchmarkRuntimeError(
            "benchmark execution is refused while another workload may be active",
            code="BENCHMARK_RUNTIME_UNAVAILABLE",
        )


def _default_benchmark_entrypoint_path() -> Path:
    installed = BENCHMARK_ENTRYPOINT_HOST_PATH
    if installed.exists():
        return installed
    return Path(__file__).resolve().parents[2] / "packaging" / "dure-benchmark"


def _validated_benchmark_entrypoint(path: Path) -> Path:
    if not isinstance(path, Path) or not path.is_absolute():
        raise BenchmarkRuntimeError(
            "packaged benchmark entrypoint is unavailable",
            code="BENCHMARK_RUNTIME_UNAVAILABLE",
        )
    try:
        metadata = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise BenchmarkRuntimeError(
            "packaged benchmark entrypoint is unavailable",
            code="BENCHMARK_RUNTIME_UNAVAILABLE",
        ) from exc
    if (
        resolved != path
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or not 1 <= metadata.st_size <= MAX_BENCHMARK_ENTRYPOINT_BYTES
        or metadata.st_mode & 0o022
        or metadata.st_mode & 0o111 == 0
    ):
        raise BenchmarkRuntimeError(
            "packaged benchmark entrypoint is unsafe",
            code="BENCHMARK_RUNTIME_UNAVAILABLE",
        )
    return path


def _model_context_limit(model_path: Path) -> int:
    config_path = model_path / "config.json"
    try:
        if config_path.stat().st_size > MAX_MODEL_CONFIG_BYTES:
            raise BenchmarkRuntimeError(
                "benchmark model config is too large",
                code="BENCHMARK_ARTIFACT_UNAVAILABLE",
            )
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise BenchmarkRuntimeError(
            "benchmark model config is invalid",
            code="BENCHMARK_ARTIFACT_UNAVAILABLE",
        ) from exc
    if type(config) is not dict:
        raise BenchmarkRuntimeError(
            "benchmark model config is invalid",
            code="BENCHMARK_ARTIFACT_UNAVAILABLE",
        )
    limit = config.get("max_position_embeddings")
    if type(limit) is not int or limit <= 0:
        raise BenchmarkRuntimeError(
            "benchmark model context limit is unavailable",
            code="BENCHMARK_ARTIFACT_UNAVAILABLE",
        )
    return limit


def _validated_workload_dimensions(
    payload: BenchmarkTaskPayload,
    workload: BenchmarkWorkload,
    *,
    context_limit: int,
) -> None:
    for field in (
        "input_tokens",
        "output_tokens",
        "concurrency",
        "warmup_requests",
        "request_count",
    ):
        value = getattr(payload, field)
        minimum = 0 if field == "warmup_requests" else 1
        if type(value) is not int or value < minimum:
            raise ValueError(f"benchmark {field} must be a fixed integer")
    duration = getattr(payload, "duration_seconds")
    if type(duration) not in {int, float} or not math.isfinite(duration) or duration <= 0:
        raise ValueError("benchmark duration_seconds must be finite and positive")

    fixed_window = (
        payload.warmup_requests,
        payload.request_count,
        float(payload.duration_seconds),
    )
    expected_window = (
        workload.warmup_requests,
        workload.request_count,
        float(workload.duration_seconds),
    )
    if fixed_window != expected_window:
        raise ValueError("benchmark measurement window does not match the fixed workload")

    if payload.workload_id == "max-context":
        if (
            payload.output_tokens != 256
            or payload.concurrency != 1
            or payload.input_tokens + payload.output_tokens != context_limit
        ):
            raise ValueError("max-context dimensions do not match the local model context")
    elif (
        payload.input_tokens,
        payload.output_tokens,
        payload.concurrency,
    ) != (workload.input_tokens, workload.output_tokens, workload.concurrency):
        raise ValueError("benchmark dimensions do not match the fixed workload")

    if payload.input_tokens + payload.output_tokens > context_limit:
        raise ValueError("benchmark workload exceeds the local model context")


def _number(value: Any, field: str, *, minimum: float | None = None) -> float:
    if type(value) not in {int, float} or not math.isfinite(value):
        raise BenchmarkRuntimeError(f"benchmark metric {field} is invalid")
    normalized = float(value)
    if minimum is not None and normalized < minimum:
        raise BenchmarkRuntimeError(f"benchmark metric {field} is out of range")
    return normalized


def _integer(value: Any, field: str, *, minimum: int = 0) -> int:
    if (
        type(value) is not int
        or value < minimum
        or value > MAX_BENCHMARK_INTEGER
    ):
        raise BenchmarkRuntimeError(f"benchmark metric {field} is invalid")
    return value


def _bounded(value: Any, field: str, maximum: float) -> float:
    normalized = _number(value, field, minimum=0)
    if normalized > maximum:
        raise BenchmarkRuntimeError(f"benchmark metric {field} is out of range")
    return normalized


def _optional_number(value: Any, field: str) -> float | None:
    if value is None:
        return None
    return _number(value, field, minimum=0.000001)


def _benchmark_container_identity(
    raw: str, payload: BenchmarkTaskPayload
) -> tuple[str, str, datetime] | None:
    identity = raw.split("\t")
    expected = [
        "true",
        "benchmark",
        payload.benchmark_id,
        payload.release_id,
        payload.placement_id,
        payload.workload_id,
        "-",
    ]
    if (
        len(identity) != 10
        or re.fullmatch(r"[0-9a-f]{64}", identity[0]) is None
        or identity[3:] != expected
    ):
        return None
    timestamp = re.fullmatch(
        r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?Z",
        identity[2],
    )
    if timestamp is None:
        return None
    fraction = timestamp.group(2)
    normalized = timestamp.group(1)
    if fraction is not None:
        normalized += "." + fraction[:6].ljust(6, "0")
    try:
        started_at = datetime.fromisoformat(normalized + "+00:00")
    except ValueError:
        return None
    if started_at.tzinfo is None:
        return None
    return identity[0], identity[1], started_at.astimezone(timezone.utc)


def _validated_metrics(
    raw: str, payload: BenchmarkTaskPayload
) -> dict[str, int | float | bool | None]:
    if not raw or len(raw.encode("utf-8")) > MAX_SUMMARY_BYTES:
        raise BenchmarkRuntimeError("benchmark container returned no valid summary")
    try:
        def unique_object(pairs):
            value = {}
            for key, item in pairs:
                if key in value:
                    raise ValueError("duplicate JSON key")
                value[key] = item
            return value

        value = json.loads(raw, object_pairs_hook=unique_object)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise BenchmarkRuntimeError("benchmark container returned no valid summary") from exc
    if type(value) is not dict or set(value) != _METRIC_FIELDS:
        raise BenchmarkRuntimeError("benchmark container returned an unexpected summary schema")

    metrics: dict[str, int | float | bool | None] = {
        "warmup_requests": _integer(value["warmup_requests"], "warmup_requests"),
        "request_count": _integer(value["request_count"], "request_count", minimum=1),
        "duration_seconds": _number(
            value["duration_seconds"], "duration_seconds", minimum=0.000001
        ),
        "oom_count": _integer(value["oom_count"], "oom_count"),
        "crash_count": _integer(value["crash_count"], "crash_count"),
        "restart_count": _integer(value["restart_count"], "restart_count"),
        "ttft_p95_ms": _optional_number(value["ttft_p95_ms"], "ttft_p95_ms"),
        "tpot_p95_ms": _optional_number(value["tpot_p95_ms"], "tpot_p95_ms"),
        "e2e_p95_ms": _optional_number(value["e2e_p95_ms"], "e2e_p95_ms"),
        "throughput_tps": _optional_number(value["throughput_tps"], "throughput_tps"),
        "success_rate": _bounded(value["success_rate"], "success_rate", 1),
        "vram_headroom_pct": _bounded(
            value["vram_headroom_pct"], "vram_headroom_pct", 100
        ),
        "quality_score": _bounded(value["quality_score"], "quality_score", 1),
    }
    if metrics["warmup_requests"] != payload.warmup_requests:
        raise BenchmarkRuntimeError("benchmark warmup count does not match the fixed workload")
    if metrics["request_count"] != payload.request_count:
        raise BenchmarkRuntimeError("benchmark request count does not match the fixed workload")
    metrics.update({field: None for field in _SINGLE_NODE_METRIC_FIELDS})
    return metrics


class SafeBenchmarkRuntime:
    """Run one fixed, single-node GPU workload without accepting execution knobs."""

    def __init__(
        self,
        runner: Runner | None = None,
        *,
        entrypoint_path: Path | None = None,
    ) -> None:
        self.runner = runner or SubprocessRunner()
        self.entrypoint_path = entrypoint_path or _default_benchmark_entrypoint_path()

    def reconcile(self, payload: BenchmarkTaskPayload) -> None:
        payload = _validated_payload(payload)
        if payload.apply is not True:
            raise ValueError("benchmark reconciliation requires explicit apply approval")
        name = f"dure-benchmark-{payload.benchmark_id}"
        _validated_benchmark_entrypoint(self.entrypoint_path)
        existing = self.runner.run(
            [
                "docker",
                "container",
                "inspect",
                "--format",
                BENCHMARK_CONTAINER_INSPECT_FORMAT,
                name,
            ],
            timeout=10,
        )
        if existing.ok:
            identity = _benchmark_container_identity(existing.stdout, payload)
            if identity is None:
                raise BenchmarkRuntimeError("benchmark container name collision")
            if identity[1] in {"running", "restarting", "paused"}:
                deadline = identity[2] + timedelta(
                    seconds=payload.duration_seconds
                    + BENCHMARK_CONTAINER_GRACE_SECONDS
                )
                if datetime.now(timezone.utc) < deadline:
                    raise BenchmarkRuntimeDeferred(
                        "the exact BENCHMARK container is still active"
                    )
                self._cleanup_failed_container(payload, name=name)
            elif identity[1] in {"created", "exited", "dead"}:
                removed = self.runner.run(["docker", "rm", identity[0]], timeout=30)
                if not removed.ok:
                    raise BenchmarkRuntimeDeferred(
                        "stopped BENCHMARK container removal is not yet confirmed"
                    )
            else:
                raise BenchmarkRuntimeError(
                    "benchmark container is not in a safely removable state"
                )
        elif existing.returncode == 1:
            if not self._container_absence_is_confirmed(name):
                raise BenchmarkRuntimeDeferred(
                    "BENCHMARK container preflight identity is temporarily unavailable"
                )
        else:
            raise BenchmarkRuntimeDeferred(
                "BENCHMARK container preflight identity is temporarily unavailable"
            )

    def __call__(
        self,
        payload: BenchmarkTaskPayload,
        profile: NodeProfile,
        cached_model: InstalledModelProfile,
    ) -> dict[str, Any]:
        if type(payload) is not BenchmarkTaskPayload:
            raise TypeError("benchmark runtime requires a validated BenchmarkTaskPayload")
        return self.execute(payload, profile, cached_model, apply=payload.apply)

    def execute(
        self,
        payload: BenchmarkTaskPayload,
        profile: NodeProfile,
        cached_model: InstalledModelProfile,
        *,
        apply: bool = False,
    ) -> dict[str, Any]:
        if type(apply) is not bool:
            raise ValueError("benchmark apply must be a strict boolean")
        payload = _validated_payload(payload)
        if apply:
            if payload.apply is not True:
                raise ValueError("benchmark execution requires explicit apply approval")
            if (
                len(payload.node_ids) != 1
                or payload.node_ids[0] != payload.coordinator_node_id
            ):
                raise ValueError("multi-node BENCHMARK execution is not supported")
            self.reconcile(payload)
        workload = BENCHMARK_WORKLOADS[payload.workload_id]
        model_path = _validated_model_path(payload, cached_model)
        context_limit = _model_context_limit(model_path)
        _validated_workload_dimensions(
            payload, workload, context_limit=context_limit
        )
        gpu_uuid = _validated_gpu_uuid(payload, profile)
        memory_limit, cpu_limit = _benchmark_resource_limits(profile)
        if not apply:
            _ensure_no_active_workloads(profile)
            return {
                "benchmark_id": payload.benchmark_id,
                "workload_id": payload.workload_id,
                "metrics": {},
            }
        name = f"dure-benchmark-{payload.benchmark_id}"
        entrypoint_path = _validated_benchmark_entrypoint(self.entrypoint_path)

        image = self.runner.run(
            ["docker", "image", "inspect", "--format", "{{.Id}}", payload.runtime_image],
            timeout=15,
        )
        if not image.ok:
            raise BenchmarkRuntimeError(
                "pinned benchmark runtime image is not available locally",
                code="BENCHMARK_RUNTIME_UNAVAILABLE",
            )

        self._ensure_no_other_benchmark_container()
        _ensure_no_active_workloads(
            profile, ignored_benchmark_id=payload.benchmark_id
        )
        self._ensure_selected_gpu_idle(gpu_uuid)

        create_command = self._container_command(
            payload,
            name=name,
            model_path=model_path,
            gpu_uuid=gpu_uuid,
            memory_limit=memory_limit,
            cpu_limit=cpu_limit,
            entrypoint_path=entrypoint_path,
        )
        limited_run = getattr(self.runner, "run_limited_output", None)
        if not callable(limited_run):
            raise BenchmarkRuntimeError(
                "benchmark runner cannot enforce the output limit",
                code="BENCHMARK_RUNTIME_UNAVAILABLE",
            )
        created = self.runner.run(create_command, timeout=60)
        if not created.ok:
            self._cleanup_failed_container(payload, name=name)
            raise BenchmarkRuntimeError("benchmark container creation failed")
        container_id = created.stdout.strip()
        if re.fullmatch(r"[0-9a-f]{64}", container_id) is None:
            self._cleanup_failed_container(payload, name=name)
            raise BenchmarkRuntimeError("benchmark container identity is invalid")
        existing = self.runner.run(
            [
                "docker",
                "container",
                "inspect",
                "--format",
                BENCHMARK_CONTAINER_INSPECT_FORMAT,
                name,
            ],
            timeout=10,
        )
        identity = (
            _benchmark_container_identity(existing.stdout, payload)
            if existing.ok
            else None
        )
        if (
            identity is None
            or identity[0] != container_id
            or identity[1] != "created"
        ):
            self._cleanup_failed_container(payload, name=name)
            raise BenchmarkRuntimeError("benchmark container identity is invalid")

        result = limited_run(
            ["docker", "start", "--attach", container_id],
            timeout=payload.duration_seconds + BENCHMARK_CONTAINER_GRACE_SECONDS,
            max_output_bytes=MAX_BENCHMARK_OUTPUT_BYTES,
        )
        if not result.ok:
            self._cleanup_failed_container(payload, name=name)
            raise BenchmarkRuntimeError("benchmark container execution failed")
        try:
            metrics = _validated_metrics(result.stdout, payload)
        except Exception:
            self._cleanup_failed_container(payload, name=name)
            raise
        self._cleanup_failed_container(payload, name=name)
        return {
            "benchmark_id": payload.benchmark_id,
            "workload_id": payload.workload_id,
            "metrics": metrics,
        }

    def _ensure_no_other_benchmark_container(self) -> None:
        containers = self.runner.run(
            [
                "docker",
                "container",
                "ls",
                "--all",
                "--no-trunc",
                "--filter",
                "label=dure.managed=true",
                "--filter",
                "label=dure.kind=benchmark",
                "--format",
                "{{.ID}}",
            ],
            timeout=10,
        )
        if not containers.ok:
            raise BenchmarkRuntimeError(
                "existing benchmark containers could not be verified",
                code="BENCHMARK_RUNTIME_UNAVAILABLE",
            )
        container_ids = [
            line.strip() for line in containers.stdout.splitlines() if line.strip()
        ]
        if any(re.fullmatch(r"[0-9a-f]{64}", item) is None for item in container_ids):
            raise BenchmarkRuntimeError(
                "existing benchmark container identity is invalid",
                code="BENCHMARK_RUNTIME_UNAVAILABLE",
            )
        if container_ids:
            raise BenchmarkRuntimeError(
                "benchmark execution is refused while another Dure benchmark container exists",
                code="BENCHMARK_RUNTIME_UNAVAILABLE",
            )

    def _ensure_selected_gpu_idle(self, gpu_uuid: str) -> None:
        processes = self.runner.run(NVIDIA_COMPUTE_QUERY_COMMAND, timeout=10)
        if not processes.ok:
            raise BenchmarkRuntimeError(
                "selected GPU compute processes could not be verified",
                code="BENCHMARK_RUNTIME_UNAVAILABLE",
            )
        observed = [
            line.strip() for line in processes.stdout.splitlines() if line.strip()
        ]
        if any(
            re.fullmatch(r"(?:GPU|MIG)-[0-9A-Fa-f-]{16,96}", item) is None
            for item in observed
        ):
            raise BenchmarkRuntimeError(
                "selected GPU compute process identity is invalid",
                code="BENCHMARK_RUNTIME_UNAVAILABLE",
            )
        if gpu_uuid in observed or any(item.startswith("MIG-") for item in observed):
            raise BenchmarkRuntimeError(
                "benchmark execution is refused while the selected GPU is active",
                code="BENCHMARK_RUNTIME_UNAVAILABLE",
            )

    def _container_absence_is_confirmed(self, name: str) -> bool:
        containers = self.runner.run(
            [
                "docker",
                "container",
                "ls",
                "--all",
                "--no-trunc",
                "--filter",
                f"name={name}",
                "--format",
                "{{.ID}}\t{{.Names}}",
            ],
            timeout=10,
        )
        return containers.ok and not containers.stdout.strip()

    def _cleanup_failed_container(
        self, payload: BenchmarkTaskPayload, *, name: str
    ) -> None:
        existing = self.runner.run(
            [
                "docker",
                "container",
                "inspect",
                "--format",
                BENCHMARK_CONTAINER_INSPECT_FORMAT,
                name,
            ],
            timeout=10,
        )
        if existing.returncode == 1:
            if self._container_absence_is_confirmed(name):
                return
            raise BenchmarkRuntimeDeferred(
                "BENCHMARK container cleanup is not yet confirmed"
            )
        if not existing.ok:
            raise BenchmarkRuntimeDeferred(
                "BENCHMARK container cleanup identity is temporarily unavailable"
            )
        identity = _benchmark_container_identity(existing.stdout, payload)
        if identity is None:
            raise BenchmarkRuntimeError(
                "failed benchmark container cleanup identity mismatch"
            )
        container_id, state, _ = identity
        if state in {"running", "restarting", "paused"}:
            stopped = self.runner.run(
                ["docker", "stop", "--timeout", "30", container_id], timeout=45
            )
            if not stopped.ok:
                raise BenchmarkRuntimeDeferred(
                    "BENCHMARK container stop is not yet confirmed"
                )
            existing = self.runner.run(
                [
                    "docker",
                    "container",
                    "inspect",
                    "--format",
                    BENCHMARK_CONTAINER_INSPECT_FORMAT,
                    name,
                ],
                timeout=10,
            )
            if existing.returncode == 1:
                if self._container_absence_is_confirmed(name):
                    return
                raise BenchmarkRuntimeDeferred(
                    "BENCHMARK container removal is not yet confirmed"
                )
            if not existing.ok:
                raise BenchmarkRuntimeDeferred(
                    "stopped BENCHMARK container state is temporarily unavailable"
                )
            identity = _benchmark_container_identity(existing.stdout, payload)
            if (
                identity is None
                or identity[0] != container_id
                or identity[1] not in {"exited", "dead"}
            ):
                raise BenchmarkRuntimeError(
                    "stopped benchmark container identity changed during cleanup"
                )
        elif state not in {"created", "exited", "dead"}:
            raise BenchmarkRuntimeError(
                "failed benchmark container is not in a safely removable state"
            )
        removed = self.runner.run(["docker", "rm", container_id], timeout=30)
        if not removed.ok:
            raise BenchmarkRuntimeDeferred(
                "BENCHMARK container removal is not yet confirmed"
            )

    @staticmethod
    def _container_command(
        payload: BenchmarkTaskPayload,
        *,
        name: str,
        model_path: Path,
        gpu_uuid: str,
        memory_limit: str,
        cpu_limit: str,
        entrypoint_path: Path,
    ) -> list[str]:
        labels = (
            "dure.managed=true",
            "dure.kind=benchmark",
            f"dure.benchmark={payload.benchmark_id}",
            f"dure.release={payload.release_id}",
            f"dure.placement={payload.placement_id}",
            f"dure.workload={payload.workload_id}",
        )
        command = [
            "docker",
            "create",
            "--pull",
            "never",
            "--name",
            name,
            "--log-driver",
            "none",
            "--read-only",
            "--network",
            "none",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges:true",
            "--pids-limit",
            "512",
            "--memory",
            memory_limit,
            "--memory-swap",
            memory_limit,
            "--cpus",
            cpu_limit,
            "--restart",
            "no",
            "--gpus",
            f"device={gpu_uuid}",
            "--shm-size",
            "4g",
            "--tmpfs",
            "/tmp:rw,exec,nosuid,nodev,size=1g",
        ]
        for label in labels:
            command.extend(("--label", label))
        command.extend(
            (
                "--mount",
                f"type=bind,src={model_path},dst=/models/model,readonly",
                "--mount",
                (
                    "type=bind,src="
                    f"{entrypoint_path},dst={BENCHMARK_ENTRYPOINT_CONTAINER_PATH},readonly"
                ),
                "--entrypoint",
                BENCHMARK_ENTRYPOINT,
                payload.runtime_image,
                "run",
                "--suite",
                payload.suite_id,
                "--workload",
                payload.workload_id,
                "--model",
                "/models/model",
                "--artifact-revision",
                payload.artifact_revision,
                "--artifact-manifest-digest",
                payload.artifact_manifest_digest,
                "--quantization",
                payload.quantization,
                "--input-tokens",
                str(payload.input_tokens),
                "--output-tokens",
                str(payload.output_tokens),
                "--concurrency",
                str(payload.concurrency),
                "--warmup-requests",
                str(payload.warmup_requests),
                "--request-count",
                str(payload.request_count),
                "--duration-seconds",
                str(payload.duration_seconds),
                "--output-format",
                "json-summary-v1",
            )
        )
        return command

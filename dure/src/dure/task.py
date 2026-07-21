from __future__ import annotations

import enum
import hashlib
import json
import math
import re
import uuid
from dataclasses import dataclass
from typing import Any


BENCHMARK_SUITE_ID = "dure-serving-slo-v1"
BENCHMARK_POLICY_VERSION = "benchmark-gate-v2"
BENCHMARK_WORKLOAD_IDS = frozenset(
    {
        "short-chat-1k-128",
        "long-chat-4k-256",
        "max-context",
        "quality-eval",
    }
)
BENCHMARK_QUANTIZATIONS = frozenset({"awq", "gptq", "fp8", "fp16", "bf16", "int8"})
BENCHMARK_WORKLOAD_DIMENSIONS = {
    "short-chat-1k-128": (1024, 128, 8),
    "long-chat-4k-256": (4096, 256, 4),
    "quality-eval": (1024, 256, 1),
}
BENCHMARK_WARMUP_REQUESTS = 20
BENCHMARK_REQUEST_COUNT = 200
BENCHMARK_DURATION_SECONDS = 900.0
MAX_BENCHMARK_CONTEXT_TOKENS = 1_048_576
MAX_BENCHMARK_INTEGER = 2_147_483_647
BENCHMARK_PROFILE_IDENTITY_FIELDS = (
    "os_name",
    "os_version",
    "kernel",
    "architecture",
    "virtualization",
    "cpu_model",
    "cpu_count",
    "memory_mib",
    "swap_mib",
    "disk_total_mib",
    "gpus",
    "network",
    "runtime",
)


def _canonical_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _canonical_json(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        normalized = [_canonical_json(item) for item in value]
        return sorted(
            normalized,
            key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")),
        )
    return value


def _benchmark_profile_identity(node_id: str, profile: Any) -> dict[str, Any]:
    _canonical_uuid(node_id, "node_id")
    value = profile.to_dict() if hasattr(profile, "to_dict") else profile
    if not isinstance(value, dict):
        raise ValueError("benchmark profile must be an object")
    try:
        identity = {
            field: value[field] for field in BENCHMARK_PROFILE_IDENTITY_FIELDS
        }
    except KeyError as exc:
        raise ValueError("benchmark profile is missing an identity field") from exc
    return {"node_id": node_id, "profile": identity}


def benchmark_inventory_fingerprint(
    profiles: list[tuple[str, Any]],
) -> str:
    """Fingerprint stable benchmark qualifications, excluding volatile capacity."""
    if not profiles:
        raise ValueError("benchmark inventory must contain at least one node")
    identities = [
        _benchmark_profile_identity(node_id, profile)
        for node_id, profile in profiles
    ]
    encoded = json.dumps(
        _canonical_json(identities),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def benchmark_profile_fingerprint(node_id: str, profile: Any) -> str:
    return benchmark_inventory_fingerprint([(node_id, profile)])


def _canonical_uuid(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a canonical UUID")
    try:
        if str(uuid.UUID(value)) != value:
            raise ValueError
    except ValueError as exc:
        raise ValueError(f"{field} must be a canonical UUID") from exc
    return value


def _required_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value


def _strict_integer(value: Any, field: str, *, minimum: int = 0) -> int:
    if (
        type(value) is not int
        or value < minimum
        or value > MAX_BENCHMARK_INTEGER
    ):
        raise ValueError(f"{field} must be an integer in range")
    return value


def _strict_number(value: Any, field: str, *, minimum: float = 0) -> float:
    if type(value) not in {int, float}:
        raise ValueError(f"{field} must be a finite number in range")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized < minimum:
        raise ValueError(f"{field} must be a finite number in range")
    return normalized


@dataclass(frozen=True)
class BenchmarkTaskPayload:
    benchmark_id: str
    release_id: str
    placement_id: str
    suite_id: str
    policy_version: str
    dure_commit: str
    model_id: str
    model_repository: str
    artifact_revision: str
    artifact_manifest_digest: str
    quantization: str
    runtime_image: str
    coordinator_node_id: str
    node_ids: tuple[str, ...]
    inventory_fingerprint: str
    workload_id: str
    input_tokens: int
    output_tokens: int
    concurrency: int
    warmup_requests: int
    request_count: int
    duration_seconds: float
    prepare_model: bool
    pull_image: bool
    apply: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            field: list(value) if field == "node_ids" else value
            for field, value in self.__dict__.items()
        }

    @classmethod
    def from_dict(cls, value: Any) -> "BenchmarkTaskPayload":
        if not isinstance(value, dict):
            raise ValueError("BENCHMARK payload must be an object")
        allowed = set(cls.__dataclass_fields__)
        optional = {"prepare_model", "pull_image"}
        unexpected = sorted(set(value) - allowed)
        missing = sorted(allowed - optional - set(value))
        if unexpected:
            raise ValueError("unexpected BENCHMARK payload field(s)")
        if missing:
            raise ValueError(f"missing BENCHMARK payload field(s): {', '.join(missing)}")

        benchmark_id = _canonical_uuid(value["benchmark_id"], "benchmark_id")
        release_id = _canonical_uuid(value["release_id"], "release_id")
        placement_id = _canonical_uuid(value["placement_id"], "placement_id")
        coordinator_node_id = _canonical_uuid(
            value["coordinator_node_id"], "coordinator_node_id"
        )
        raw_node_ids = value["node_ids"]
        if (
            not isinstance(raw_node_ids, list)
            or not raw_node_ids
            or len(raw_node_ids) > 64
        ):
            raise ValueError("node_ids must be a non-empty list of canonical UUIDs")
        node_ids = tuple(
            _canonical_uuid(node_id, "node_ids") for node_id in raw_node_ids
        )
        if list(node_ids) != sorted(set(node_ids)):
            raise ValueError("node_ids must be sorted and contain no duplicates")

        suite_id = _required_string(value["suite_id"], "suite_id")
        if suite_id != BENCHMARK_SUITE_ID:
            raise ValueError("unsupported BENCHMARK suite_id")
        policy_version = _required_string(value["policy_version"], "policy_version")
        if policy_version != BENCHMARK_POLICY_VERSION:
            raise ValueError("unsupported BENCHMARK policy_version")
        dure_commit = _required_string(value["dure_commit"], "dure_commit")
        if re.fullmatch(r"[0-9a-f]{40,64}", dure_commit) is None:
            raise ValueError("dure_commit must be an immutable commit hash")
        model_id = _required_string(value["model_id"], "model_id")
        if re.fullmatch(r"[a-z0-9][a-z0-9._-]{1,99}", model_id) is None:
            raise ValueError("invalid BENCHMARK model_id")
        model_repository = _required_string(
            value["model_repository"], "model_repository"
        )
        if (
            len(model_repository) > 255
            or re.fullmatch(
                r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", model_repository
            )
            is None
        ):
            raise ValueError("invalid BENCHMARK model_repository")
        artifact_revision = _required_string(
            value["artifact_revision"], "artifact_revision"
        )
        if re.fullmatch(r"[0-9a-f]{40,64}", artifact_revision) is None:
            raise ValueError("artifact_revision must be an immutable commit hash")
        artifact_manifest_digest = _required_string(
            value["artifact_manifest_digest"], "artifact_manifest_digest"
        )
        if re.fullmatch(r"sha256:[0-9a-f]{64}", artifact_manifest_digest) is None:
            raise ValueError("artifact_manifest_digest must be an immutable sha256 digest")
        quantization = _required_string(value["quantization"], "quantization")
        if quantization not in BENCHMARK_QUANTIZATIONS:
            raise ValueError("unsupported BENCHMARK quantization")
        runtime_image = _required_string(value["runtime_image"], "runtime_image")
        image_name = runtime_image.partition("@sha256:")[0]
        if (
            len(runtime_image) > 512
            or re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9._:/-]*@sha256:[0-9a-f]{64}",
                runtime_image,
            )
            is None
            or any(part in {"", ".", ".."} for part in image_name.split("/"))
        ):
            raise ValueError("runtime_image must be OCI digest-pinned")
        inventory_fingerprint = _required_string(
            value["inventory_fingerprint"], "inventory_fingerprint"
        )
        if re.fullmatch(r"sha256:[0-9a-f]{64}", inventory_fingerprint) is None:
            raise ValueError("inventory_fingerprint must be an immutable sha256 digest")
        workload_id = _required_string(value["workload_id"], "workload_id")
        if workload_id not in BENCHMARK_WORKLOAD_IDS:
            raise ValueError("unsupported BENCHMARK workload_id")
        input_tokens = _strict_integer(value["input_tokens"], "input_tokens", minimum=1)
        output_tokens = _strict_integer(
            value["output_tokens"], "output_tokens", minimum=1
        )
        concurrency = _strict_integer(value["concurrency"], "concurrency", minimum=1)
        warmup_requests = _strict_integer(
            value["warmup_requests"], "warmup_requests", minimum=0
        )
        request_count = _strict_integer(
            value["request_count"], "request_count", minimum=1
        )
        duration_seconds = _strict_number(
            value["duration_seconds"], "duration_seconds", minimum=0.000001
        )
        if input_tokens + output_tokens > MAX_BENCHMARK_CONTEXT_TOKENS:
            raise ValueError("BENCHMARK context length exceeds the safety limit")
        expected_dimensions = BENCHMARK_WORKLOAD_DIMENSIONS.get(workload_id)
        if expected_dimensions is not None and (
            input_tokens,
            output_tokens,
            concurrency,
        ) != expected_dimensions:
            raise ValueError("BENCHMARK workload dimensions do not match the allowlist")
        if workload_id == "max-context" and (output_tokens, concurrency) != (256, 1):
            raise ValueError("BENCHMARK max-context dimensions do not match the allowlist")
        if (
            warmup_requests != BENCHMARK_WARMUP_REQUESTS
            or request_count != BENCHMARK_REQUEST_COUNT
            or duration_seconds != BENCHMARK_DURATION_SECONDS
        ):
            raise ValueError("BENCHMARK measurement dimensions do not match the allowlist")
        prepare_model = value.get("prepare_model", False)
        pull_image = value.get("pull_image", False)
        apply = value["apply"]
        if any(type(item) is not bool for item in (prepare_model, pull_image, apply)):
            raise ValueError("BENCHMARK mutation approval must be a boolean")

        return cls(
            benchmark_id=benchmark_id,
            release_id=release_id,
            placement_id=placement_id,
            suite_id=suite_id,
            policy_version=policy_version,
            dure_commit=dure_commit,
            model_id=model_id,
            model_repository=model_repository,
            artifact_revision=artifact_revision,
            artifact_manifest_digest=artifact_manifest_digest,
            quantization=quantization,
            runtime_image=runtime_image,
            coordinator_node_id=coordinator_node_id,
            node_ids=node_ids,
            inventory_fingerprint=inventory_fingerprint,
            workload_id=workload_id,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            concurrency=concurrency,
            warmup_requests=warmup_requests,
            request_count=request_count,
            duration_seconds=duration_seconds,
            prepare_model=prepare_model,
            pull_image=pull_image,
            apply=apply,
        )


class TaskType(str, enum.Enum):
    PROBE = "PROBE"
    BENCHMARK = "BENCHMARK"
    PREPARE_MODEL = "PREPARE_MODEL"
    PREPARE_IMAGE = "PREPARE_IMAGE"
    QUARANTINE_ARTIFACT_CACHE = "QUARANTINE_ARTIFACT_CACHE"
    VERIFY = "VERIFY"
    APPLY_DEPLOYMENT = "APPLY_DEPLOYMENT"
    START_DEPLOYMENT = "START_DEPLOYMENT"
    STOP_DEPLOYMENT = "STOP_DEPLOYMENT"
    RESTART_DEPLOYMENT = "RESTART_DEPLOYMENT"
    UNJOIN_NODE = "UNJOIN_NODE"


class TaskStatus(str, enum.Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"

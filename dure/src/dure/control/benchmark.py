from __future__ import annotations

import hashlib
import json
import math
import re
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dure.models import NodeProfile
from dure.task import (
    MAX_BENCHMARK_INTEGER,
    benchmark_inventory_fingerprint as contract_inventory_fingerprint,
)

from .models import (
    AuditEvent,
    BenchmarkEvidence,
    BenchmarkRun,
    ModelArtifact,
    ModelRelease,
    Node,
    NodeProfileRecord,
    PlacementProfileRecord,
    RuntimeRelease,
    utcnow,
)


BENCHMARK_SUITE_ID = "dure-serving-slo-v1"
BENCHMARK_POLICY_VERSION = "benchmark-gate-v2"
MIN_WARMUP_REQUESTS = 20
MIN_MEASURED_REQUESTS = 200
MIN_QUALITY_SCORE = 0.80
GPU_COMPUTE_CAPABILITY_ARCHITECTURES = {
    "8.0": "ampere",
    "8.6": "ampere",
    "8.7": "ampere",
    "8.9": "ada",
    "9.0": "hopper",
    "10.0": "blackwell",
    "10.3": "blackwell",
    "11.0": "blackwell",
    "12.0": "blackwell",
    "12.1": "blackwell",
}


class BenchmarkNotFoundError(ValueError):
    pass


class BenchmarkIdentityMismatchError(ValueError):
    pass


class BenchmarkPromotionError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "BENCHMARK_PROMOTION_BLOCKED",
        details: dict[str, Any] | None = None,
    ):
        super().__init__(message)
        self.code = code
        self.details = details or {}


def _bounded_integer(value: Any, *, field: str, minimum: int) -> int:
    if (
        type(value) is not int
        or value < minimum
        or value > MAX_BENCHMARK_INTEGER
    ):
        raise ValueError(f"{field} must be an integer in range")
    return value


def _canonical(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _canonical(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        normalized = [_canonical(item) for item in value]
        return sorted(
            normalized,
            key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")),
        )
    return value


def _digest(value: Any) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _benchmark_node_profiles(
    session: Session, node_ids: list[str]
) -> list[tuple[str, NodeProfile]]:
    normalized_ids = sorted(node_ids)
    nodes = list(
        session.scalars(
            select(Node)
            .where(Node.id.in_(normalized_ids))
            .order_by(Node.id)
            .with_for_update()
        )
    )
    profiles = {
        record.node_id: record
        for record in session.scalars(
            select(NodeProfileRecord)
            .where(NodeProfileRecord.node_id.in_(normalized_ids))
            .order_by(NodeProfileRecord.node_id)
            .with_for_update()
        )
    }
    found = {node.id for node in nodes}
    missing = sorted(set(normalized_ids) - found)
    if missing:
        raise BenchmarkNotFoundError(f"unknown benchmark node(s): {', '.join(missing)}")
    fingerprint_profiles: list[tuple[str, NodeProfile]] = []
    for node in nodes:
        record = profiles.get(node.id)
        if not node.approved:
            raise BenchmarkPromotionError(f"benchmark node is not approved: {node.id}")
        if record is None:
            raise BenchmarkPromotionError(f"benchmark node has no stored profile: {node.id}")
        try:
            profile = NodeProfile.from_dict(record.profile)
        except (KeyError, TypeError, ValueError) as exc:
            raise BenchmarkPromotionError(
                f"benchmark node has an invalid stored profile: {node.id}"
            ) from exc
        profile.node_id = node.id
        fingerprint_profiles.append((node.id, profile))
    return fingerprint_profiles


def _fingerprints_for_profiles(
    profiles: list[tuple[str, NodeProfile]],
) -> tuple[str, str]:
    legacy_payload: list[dict[str, Any]] = []
    for node_id, profile in profiles:
        legacy_payload.append(
            {"node_id": node_id, "profile": _canonical(profile.to_dict())}
        )
    return contract_inventory_fingerprint(profiles), _digest(legacy_payload)


def _benchmark_inventory_fingerprints(
    session: Session, node_ids: list[str]
) -> tuple[str, str]:
    return _fingerprints_for_profiles(_benchmark_node_profiles(session, node_ids))


def _validate_placement_nodes(
    placement: PlacementProfileRecord,
    runtime: RuntimeRelease,
    profiles: list[tuple[str, NodeProfile]],
) -> None:
    if len(profiles) != placement.node_count:
        raise ValueError("benchmark node count does not match placement profile")
    ineligible: list[dict[str, Any]] = []
    for node_id, profile in profiles:
        reasons: list[str] = []
        healthy_gpus = [gpu for gpu in profile.gpus if gpu.healthy]
        if not healthy_gpus:
            reasons.append("HEALTHY_GPU_REQUIRED")
        else:
            selected_gpu = max(
                healthy_gpus, key=lambda gpu: (gpu.memory_mib, -gpu.index)
            )
            if selected_gpu.memory_mib < placement.min_gpu_memory_mib:
                reasons.append("GPU_MEMORY_INSUFFICIENT")
            architecture = GPU_COMPUTE_CAPABILITY_ARCHITECTURES.get(
                selected_gpu.compute_capability or ""
            )
            if architecture is None:
                reasons.append("GPU_ARCHITECTURE_UNKNOWN")
            elif architecture not in runtime.gpu_architectures:
                reasons.append("GPU_ARCHITECTURE_UNSUPPORTED")
        if profile.disk_free_mib < placement.min_disk_free_mib:
            reasons.append("DISK_FREE_INSUFFICIENT")
        if (
            profile.runtime.engine != "docker"
            or not profile.runtime.engine_ready
            or not profile.runtime.nvidia_runtime
        ):
            reasons.append("NVIDIA_DOCKER_UNAVAILABLE")
        if placement.node_count > 1 and (
            not profile.network.default_interface or not profile.network.addresses
        ):
            reasons.append("NETWORK_IDENTITY_UNAVAILABLE")
        if reasons:
            ineligible.append({"node_id": node_id, "reasons": reasons})
    if ineligible:
        raise BenchmarkPromotionError(
            "benchmark node profile does not satisfy the placement requirements",
            code="PLACEMENT_NODE_INELIGIBLE",
            details={"nodes": ineligible},
        )


def benchmark_inventory_fingerprint(session: Session, node_ids: list[str]) -> str:
    return _benchmark_inventory_fingerprints(session, node_ids)[0]


def benchmark_context(
    session: Session,
    *,
    release_id: str,
    placement_id: str,
    node_ids: list[str],
) -> dict[str, Any]:
    if len(node_ids) != len(set(node_ids)):
        raise ValueError("benchmark node_ids must not contain duplicates")
    normalized_node_ids = sorted(node_ids)
    release = session.scalar(
        select(ModelRelease).where(ModelRelease.id == release_id).with_for_update()
    )
    if release is None:
        raise BenchmarkNotFoundError("model release not found")
    if release.status not in {"VALIDATED", "ACTIVE"}:
        raise BenchmarkPromotionError(
            f"benchmark context is not available for {release.status} releases"
        )
    placement = session.get(PlacementProfileRecord, placement_id)
    if placement is None or placement.release_id != release.id:
        raise BenchmarkNotFoundError("placement profile not found for model release")
    if len(normalized_node_ids) != placement.node_count:
        raise ValueError("benchmark node count does not match placement profile")
    artifact = session.get(ModelArtifact, release.artifact_id)
    runtime = session.get(RuntimeRelease, release.runtime_id)
    if artifact is None or runtime is None:
        raise BenchmarkIdentityMismatchError("model release registry binding is incomplete")
    profiles = _benchmark_node_profiles(session, normalized_node_ids)
    _validate_placement_nodes(placement, runtime, profiles)
    inventory_fingerprint, _ = _fingerprints_for_profiles(profiles)
    return {
        "release_id": release.id,
        "placement_id": placement.id,
        "suite_id": BENCHMARK_SUITE_ID,
        "policy_version": BENCHMARK_POLICY_VERSION,
        "node_ids": normalized_node_ids,
        "inventory_fingerprint": inventory_fingerprint,
        "artifact_revision": artifact.revision,
        "artifact_manifest_digest": artifact.manifest_digest,
        "runtime_image": runtime.image,
    }


def _failure_codes(
    placement: PlacementProfileRecord,
    *,
    request_count: int,
    duration_seconds: float,
    oom_count: int,
    crash_count: int,
    restart_count: int,
    input_tokens: int,
    output_tokens: int,
    concurrency: int,
    warmup_requests: int,
    ttft_p95_ms: float | None,
    tpot_p95_ms: float | None,
    e2e_p95_ms: float | None,
    throughput_tps: float | None,
    success_rate: float,
    vram_headroom_pct: float,
    quality_score: float,
    network_bandwidth_mbps: float | None,
    network_rtt_ms: float | None,
    packet_loss_pct: float | None,
    nccl_all_reduce_ok: bool | None,
) -> list[str]:
    if min(input_tokens, output_tokens, concurrency, request_count) <= 0 or duration_seconds <= 0:
        raise ValueError("benchmark workload dimensions and duration must be positive")
    if min(warmup_requests, oom_count, crash_count, restart_count) < 0:
        raise ValueError("benchmark failure counts must be nonnegative")
    finite_values = (
        duration_seconds,
        success_rate,
        vram_headroom_pct,
        quality_score,
        ttft_p95_ms,
        tpot_p95_ms,
        e2e_p95_ms,
        throughput_tps,
        network_bandwidth_mbps,
        network_rtt_ms,
        packet_loss_pct,
    )
    if any(value is not None and not math.isfinite(value) for value in finite_values):
        raise ValueError("benchmark numeric values must be finite")
    if not 0 <= success_rate <= 1:
        raise ValueError("benchmark success_rate is out of range")
    if not 0 <= vram_headroom_pct <= 100:
        raise ValueError("benchmark vram_headroom_pct is out of range")
    if not 0 <= quality_score <= 1:
        raise ValueError("benchmark quality_score is out of range")
    for value in (ttft_p95_ms, tpot_p95_ms, e2e_p95_ms, throughput_tps):
        if value is not None and value <= 0:
            raise ValueError("benchmark latency and throughput values must be positive")
    if network_bandwidth_mbps is not None and network_bandwidth_mbps <= 0:
        raise ValueError("benchmark network bandwidth must be positive")
    if network_rtt_ms is not None and network_rtt_ms < 0:
        raise ValueError("benchmark network RTT must be nonnegative")
    if packet_loss_pct is not None and not 0 <= packet_loss_pct <= 100:
        raise ValueError("benchmark packet loss is out of range")

    failures: list[str] = []
    if oom_count:
        failures.append("OOM")
    if crash_count:
        failures.append("PROCESS_CRASH")
    if restart_count:
        failures.append("RUNTIME_RESTART")
    if warmup_requests < MIN_WARMUP_REQUESTS:
        failures.append("WARMUP_COUNT")
    if request_count < MIN_MEASURED_REQUESTS:
        failures.append("MEASURED_REQUEST_COUNT")
    if quality_score < MIN_QUALITY_SCORE:
        failures.append("QUALITY_SCORE")
    for value, missing_code, exceeded_code, limit in (
        (ttft_p95_ms, "TTFT_MISSING", "TTFT_SLO", placement.max_ttft_p95_ms),
        (tpot_p95_ms, "TPOT_MISSING", "TPOT_SLO", placement.max_tpot_p95_ms),
        (e2e_p95_ms, "E2E_MISSING", "E2E_SLO", placement.max_e2e_p95_ms),
    ):
        if value is None:
            failures.append(missing_code)
        elif value > limit:
            failures.append(exceeded_code)
    if throughput_tps is None:
        failures.append("THROUGHPUT_MISSING")
    elif throughput_tps < placement.min_throughput_tps:
        failures.append("THROUGHPUT_SLO")
    if success_rate < placement.min_success_rate:
        failures.append("SUCCESS_RATE_SLO")
    if vram_headroom_pct < placement.min_vram_headroom_pct:
        failures.append("VRAM_HEADROOM_SLO")

    requires_network = placement.requires_network_evidence or placement.node_count > 1
    if requires_network:
        if (
            network_bandwidth_mbps is None
            or network_rtt_ms is None
            or packet_loss_pct is None
        ):
            failures.append("NETWORK_EVIDENCE_MISSING")
        else:
            if (
                placement.min_bandwidth_mbps is None
                or network_bandwidth_mbps < placement.min_bandwidth_mbps
            ):
                failures.append("NETWORK_BANDWIDTH_SLO")
            if placement.max_rtt_ms is None or network_rtt_ms > placement.max_rtt_ms:
                failures.append("NETWORK_RTT_SLO")
            if (
                placement.max_packet_loss_pct is None
                or packet_loss_pct > placement.max_packet_loss_pct
            ):
                failures.append("NETWORK_PACKET_LOSS_SLO")
        if placement.requires_nccl:
            if nccl_all_reduce_ok is None:
                failures.append("NCCL_EVIDENCE_MISSING")
            elif nccl_all_reduce_ok is False:
                failures.append("NCCL_FAILED")
    return sorted(set(failures))


def register_benchmark_evidence(
    session: Session,
    *,
    release_id: str,
    placement_id: str,
    suite_id: str,
    node_ids: list[str],
    inventory_fingerprint: str,
    artifact_revision: str,
    artifact_manifest_digest: str,
    runtime_image: str,
    dure_commit: str,
    policy_version: str,
    input_tokens: int,
    output_tokens: int,
    concurrency: int,
    warmup_requests: int,
    request_count: int,
    duration_seconds: float,
    oom_count: int,
    crash_count: int,
    restart_count: int,
    ttft_p95_ms: float | None,
    tpot_p95_ms: float | None,
    e2e_p95_ms: float | None,
    throughput_tps: float | None,
    success_rate: float,
    vram_headroom_pct: float,
    quality_score: float,
    network_bandwidth_mbps: float | None,
    network_rtt_ms: float | None,
    packet_loss_pct: float | None,
    nccl_all_reduce_ok: bool | None,
    benchmark_run_id: str | None = None,
    actor: str = "admin",
    commit: bool = True,
) -> BenchmarkEvidence:
    for field, value, minimum in (
        ("input_tokens", input_tokens, 1),
        ("output_tokens", output_tokens, 1),
        ("concurrency", concurrency, 1),
        ("warmup_requests", warmup_requests, 0),
        ("request_count", request_count, 1),
        ("oom_count", oom_count, 0),
        ("crash_count", crash_count, 0),
        ("restart_count", restart_count, 0),
    ):
        _bounded_integer(value, field=field, minimum=minimum)
    if suite_id != BENCHMARK_SUITE_ID:
        raise ValueError("unsupported benchmark suite")
    if policy_version != BENCHMARK_POLICY_VERSION:
        raise ValueError("unsupported benchmark policy version")
    if re.fullmatch(r"[0-9a-f]{40,64}", dure_commit) is None:
        raise ValueError("dure_commit must be an immutable commit hash")
    if re.fullmatch(r"sha256:[0-9a-f]{64}", inventory_fingerprint) is None:
        raise ValueError("inventory_fingerprint must be a sha256 digest")
    if len(node_ids) != len(set(node_ids)):
        raise ValueError("benchmark node_ids must not contain duplicates")
    normalized_node_ids = sorted(node_ids)
    benchmark_run = None
    if benchmark_run_id is not None:
        try:
            if str(uuid.UUID(benchmark_run_id)) != benchmark_run_id:
                raise ValueError
        except (AttributeError, ValueError) as exc:
            raise ValueError("benchmark_run_id must be a canonical UUID") from exc
        benchmark_run = session.get(BenchmarkRun, benchmark_run_id)
        if benchmark_run is None:
            raise BenchmarkNotFoundError("benchmark run not found")
        if (
            benchmark_run.release_id != release_id
            or benchmark_run.placement_id != placement_id
            or list(benchmark_run.node_ids) != normalized_node_ids
            or benchmark_run.suite_id != suite_id
            or benchmark_run.inventory_fingerprint != inventory_fingerprint
            or benchmark_run.artifact_revision != artifact_revision
            or benchmark_run.artifact_manifest_digest
            != artifact_manifest_digest
            or benchmark_run.runtime_image != runtime_image
            or benchmark_run.dure_commit != dure_commit
            or benchmark_run.policy_version != policy_version
            or benchmark_run.input_tokens != input_tokens
            or benchmark_run.output_tokens != output_tokens
            or benchmark_run.concurrency != concurrency
            or benchmark_run.warmup_requests != warmup_requests
            or benchmark_run.request_count != request_count
        ):
            raise BenchmarkIdentityMismatchError(
                "benchmark evidence does not match its benchmark run"
            )
    context = benchmark_context(
        session,
        release_id=release_id,
        placement_id=placement_id,
        node_ids=normalized_node_ids,
    )
    release = session.get(ModelRelease, release_id)
    placement = session.get(PlacementProfileRecord, placement_id)
    if release is None or placement is None:  # pragma: no cover - context locked both
        raise BenchmarkNotFoundError("benchmark registry binding disappeared")
    mismatches: list[str] = []
    if artifact_revision != context["artifact_revision"]:
        mismatches.append("artifact_revision")
    if artifact_manifest_digest != context["artifact_manifest_digest"]:
        mismatches.append("artifact_manifest_digest")
    if runtime_image != context["runtime_image"]:
        mismatches.append("runtime_image")
    if mismatches:
        raise BenchmarkIdentityMismatchError(
            f"benchmark identity does not match registry: {', '.join(mismatches)}"
        )

    current_fingerprint = context["inventory_fingerprint"]
    if inventory_fingerprint != current_fingerprint:
        raise BenchmarkIdentityMismatchError(
            "benchmark inventory fingerprint does not match current node profiles"
        )
    failure_codes = _failure_codes(
        placement,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        concurrency=concurrency,
        warmup_requests=warmup_requests,
        request_count=request_count,
        duration_seconds=duration_seconds,
        oom_count=oom_count,
        crash_count=crash_count,
        restart_count=restart_count,
        ttft_p95_ms=ttft_p95_ms,
        tpot_p95_ms=tpot_p95_ms,
        e2e_p95_ms=e2e_p95_ms,
        throughput_tps=throughput_tps,
        success_rate=success_rate,
        vram_headroom_pct=vram_headroom_pct,
        quality_score=quality_score,
        network_bandwidth_mbps=network_bandwidth_mbps,
        network_rtt_ms=network_rtt_ms,
        packet_loss_pct=packet_loss_pct,
        nccl_all_reduce_ok=nccl_all_reduce_ok,
    )
    status = "FAILED" if failure_codes else "PASSED"
    canonical = {
        "release_id": release.id,
        "placement_id": placement.id,
        "suite_id": suite_id,
        "node_ids": normalized_node_ids,
        "inventory_fingerprint": inventory_fingerprint,
        "artifact_revision": artifact_revision,
        "artifact_manifest_digest": artifact_manifest_digest,
        "runtime_image": runtime_image,
        "dure_commit": dure_commit,
        "policy_version": policy_version,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "concurrency": concurrency,
        "warmup_requests": warmup_requests,
        "request_count": request_count,
        "duration_seconds": duration_seconds,
        "oom_count": oom_count,
        "crash_count": crash_count,
        "restart_count": restart_count,
        "ttft_p95_ms": ttft_p95_ms,
        "tpot_p95_ms": tpot_p95_ms,
        "e2e_p95_ms": e2e_p95_ms,
        "throughput_tps": throughput_tps,
        "success_rate": success_rate,
        "vram_headroom_pct": vram_headroom_pct,
        "quality_score": quality_score,
        "network_bandwidth_mbps": network_bandwidth_mbps,
        "network_rtt_ms": network_rtt_ms,
        "packet_loss_pct": packet_loss_pct,
        "nccl_all_reduce_ok": nccl_all_reduce_ok,
        "status": status,
        "failure_codes": failure_codes,
    }
    if benchmark_run_id is not None:
        canonical["benchmark_run_id"] = benchmark_run_id
    content_digest = _digest(canonical)
    evidence_digest = content_digest
    existing = session.scalar(
        select(BenchmarkEvidence).where(
            BenchmarkEvidence.evidence_digest == evidence_digest
        )
    )
    latest = session.scalar(
        select(BenchmarkEvidence)
        .where(BenchmarkEvidence.placement_id == placement.id)
        .order_by(BenchmarkEvidence.registration_sequence.desc())
        .limit(1)
    )
    later_failed_run_id = None
    if benchmark_run_id is None and latest is not None:
        later_failed_run_id = session.scalar(
            select(BenchmarkRun.id)
            .where(
                BenchmarkRun.release_id == release.id,
                BenchmarkRun.placement_id == placement.id,
                BenchmarkRun.status == "FAILED",
                BenchmarkRun.updated_at > latest.created_at,
            )
            .order_by(BenchmarkRun.updated_at.desc(), BenchmarkRun.id.desc())
            .limit(1)
        )
    if benchmark_run_id is not None:
        if existing is not None:
            return existing
    elif latest is not None:
        latest_matches = latest.benchmark_run_id is None and all(
            getattr(latest, key) == value for key, value in canonical.items()
        )
        if latest_matches and later_failed_run_id is None:
            return latest
        if existing is not None:
            evidence_digest = _digest(
                {
                    "content_digest": content_digest,
                    "after_registration_sequence": latest.registration_sequence,
                    "after_failed_run_id": later_failed_run_id,
                }
            )

    registration_sequence = (latest.registration_sequence if latest else 0) + 1
    record = BenchmarkEvidence(
        **canonical,
        registration_sequence=registration_sequence,
        evidence_digest=evidence_digest,
    )
    session.add(record)
    try:
        session.flush()
    except IntegrityError as exc:
        session.rollback()
        existing = session.scalar(
            select(BenchmarkEvidence).where(
                BenchmarkEvidence.evidence_digest == evidence_digest
            )
        )
        if existing is not None:
            return existing
        raise BenchmarkIdentityMismatchError("benchmark evidence already exists") from exc
    session.add(
        AuditEvent(
            actor=actor,
            action="benchmark_evidence.register",
            target=record.id,
            outcome=status.lower(),
            detail={
                "release_id": release.id,
                "placement_id": placement.id,
                "benchmark_run_id": benchmark_run_id,
                "failure_codes": failure_codes,
            },
        )
    )
    if commit:
        session.commit()
    else:
        session.flush()
    return record


def benchmark_evidence_dict(record: BenchmarkEvidence) -> dict[str, Any]:
    return {
        key: getattr(record, key)
        for key in (
            "id",
            "benchmark_run_id",
            "release_id",
            "placement_id",
            "registration_sequence",
            "suite_id",
            "node_ids",
            "inventory_fingerprint",
            "artifact_revision",
            "artifact_manifest_digest",
            "runtime_image",
            "dure_commit",
            "policy_version",
            "input_tokens",
            "output_tokens",
            "concurrency",
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
            "network_bandwidth_mbps",
            "network_rtt_ms",
            "packet_loss_pct",
            "nccl_all_reduce_ok",
            "status",
            "failure_codes",
            "evidence_digest",
            "created_at",
        )
    }


def _qualifying_evidence_ids(session: Session, release: ModelRelease) -> list[str]:
    artifact = session.get(ModelArtifact, release.artifact_id)
    runtime = session.get(RuntimeRelease, release.runtime_id)
    placements = list(
        session.scalars(
            select(PlacementProfileRecord)
            .where(PlacementProfileRecord.release_id == release.id)
            .order_by(PlacementProfileRecord.id)
        )
    )
    if not placements:
        raise BenchmarkPromotionError(
            "model release requires a placement profile",
            code="PLACEMENT_REQUIRED",
        )
    selected: list[str] = []
    blocked: list[dict[str, Any]] = []
    for placement in placements:
        evidence = session.scalar(
            select(BenchmarkEvidence)
            .where(
                BenchmarkEvidence.release_id == release.id,
                BenchmarkEvidence.placement_id == placement.id,
            )
            .order_by(BenchmarkEvidence.registration_sequence.desc())
            .limit(1)
        )
        detail: dict[str, Any] = {
            "placement_id": placement.id,
            "profile_id": placement.profile_id,
            "evidence_id": evidence.id if evidence else None,
        }
        if evidence is None:
            detail["code"] = "EVIDENCE_MISSING"
            blocked.append(detail)
            continue
        pending_run = session.scalar(
            select(BenchmarkRun)
            .where(
                BenchmarkRun.release_id == release.id,
                BenchmarkRun.placement_id == placement.id,
                BenchmarkRun.status == "QUEUED",
            )
            .order_by(BenchmarkRun.updated_at.desc(), BenchmarkRun.id.desc())
            .limit(1)
        )
        later_failed_run = None
        if pending_run is None:
            later_failed_run = session.scalar(
                select(BenchmarkRun)
                .where(
                    BenchmarkRun.release_id == release.id,
                    BenchmarkRun.placement_id == placement.id,
                    BenchmarkRun.status == "FAILED",
                    BenchmarkRun.updated_at > evidence.created_at,
                )
                .order_by(BenchmarkRun.updated_at.desc(), BenchmarkRun.id.desc())
                .limit(1)
            )
        later_unresolved_run = pending_run or later_failed_run
        if later_unresolved_run is not None:
            detail.update(
                code=(
                    "BENCHMARK_RUN_FAILED"
                    if later_unresolved_run.status == "FAILED"
                    else "BENCHMARK_RUN_PENDING"
                ),
                benchmark_run_id=later_unresolved_run.id,
                benchmark_run_status=later_unresolved_run.status,
                failure_code=later_unresolved_run.failure_code,
            )
            blocked.append(detail)
            continue
        if (
            evidence.artifact_revision != artifact.revision
            or evidence.artifact_manifest_digest != artifact.manifest_digest
            or evidence.runtime_image != runtime.image
        ):
            detail["code"] = "REGISTRY_IDENTITY_CHANGED"
            blocked.append(detail)
            continue
        if evidence.status != "PASSED":
            detail.update(
                code="EVIDENCE_FAILED",
                status=evidence.status,
                failure_codes=list(evidence.failure_codes),
            )
            blocked.append(detail)
            continue
        try:
            profiles = _benchmark_node_profiles(session, list(evidence.node_ids))
            _validate_placement_nodes(placement, runtime, profiles)
            current, legacy_current = _fingerprints_for_profiles(profiles)
        except (BenchmarkNotFoundError, BenchmarkPromotionError) as exc:
            detail.update(
                code=(
                    "PROFILE_INELIGIBLE"
                    if isinstance(exc, BenchmarkPromotionError)
                    and exc.code == "PLACEMENT_NODE_INELIGIBLE"
                    else "PROFILE_UNAVAILABLE"
                ),
                reason=str(exc),
            )
            blocked.append(detail)
            continue
        if evidence.inventory_fingerprint not in {current, legacy_current}:
            detail.update(code="PROFILE_CHANGED", current_fingerprint=current)
            blocked.append(detail)
            continue
        selected.append(evidence.id)
    if blocked:
        raise BenchmarkPromotionError(
            "model release does not satisfy the benchmark promotion gate",
            code="BENCHMARK_GATE_FAILED",
            details={"placements": blocked},
        )
    return selected


def promote_model_release(
    session: Session, release_id: str
) -> tuple[ModelRelease, list[str], bool]:
    release = session.scalar(
        select(ModelRelease).where(ModelRelease.id == release_id).with_for_update()
    )
    if release is None:
        raise BenchmarkNotFoundError("model release not found")
    if release.status == "ACTIVE":
        evidence_ids = list(release.promotion_evidence_ids or [])
        if (
            not evidence_ids
            or release.promotion_evidence_digest != _digest(evidence_ids)
        ):
            raise BenchmarkPromotionError(
                "ACTIVE release has no valid frozen promotion evidence set",
                code="PROMOTION_RECORD_INVALID",
            )
        existing_ids = set(
            session.scalars(
                select(BenchmarkEvidence.id).where(
                    BenchmarkEvidence.id.in_(evidence_ids)
                )
            )
        )
        if existing_ids != set(evidence_ids):
            raise BenchmarkPromotionError(
                "ACTIVE release promotion evidence is missing",
                code="PROMOTION_RECORD_INVALID",
            )
        return release, evidence_ids, False
    if release.status != "VALIDATED":
        raise BenchmarkPromotionError(
            f"only VALIDATED releases can be promoted, got {release.status}",
            code="RELEASE_STATE",
        )
    evidence_ids = _qualifying_evidence_ids(session, release)
    release.status = "ACTIVE"
    release.promotion_evidence_ids = evidence_ids
    release.promotion_evidence_digest = _digest(evidence_ids)
    release.updated_at = utcnow()
    session.add(
        AuditEvent(
            actor="admin",
            action="model_release.promote",
            target=release.id,
            outcome="success",
            detail={"evidence_ids": evidence_ids},
        )
    )
    session.commit()
    return release, evidence_ids, True

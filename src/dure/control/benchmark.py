from __future__ import annotations

import hashlib
import json
import math
import re
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dure.models import NodeProfile

from .models import (
    AuditEvent,
    BenchmarkEvidence,
    ModelArtifact,
    ModelRelease,
    Node,
    NodeProfileRecord,
    PlacementProfileRecord,
    RuntimeRelease,
    utcnow,
)


BENCHMARK_SUITE_ID = "dure-serving-slo-v1"
BENCHMARK_POLICY_VERSION = "benchmark-gate-v1"
MIN_WARMUP_REQUESTS = 20
MIN_MEASURED_REQUESTS = 200
MIN_MEASURED_SECONDS = 900
MIN_QUALITY_SCORE = 0.80


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


def benchmark_inventory_fingerprint(session: Session, node_ids: list[str]) -> str:
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
    payload: list[dict[str, Any]] = []
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
        payload.append({"node_id": node.id, "profile": _canonical(profile.to_dict())})
    return _digest(payload)


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
    return {
        "release_id": release.id,
        "placement_id": placement.id,
        "suite_id": BENCHMARK_SUITE_ID,
        "policy_version": BENCHMARK_POLICY_VERSION,
        "node_ids": normalized_node_ids,
        "inventory_fingerprint": benchmark_inventory_fingerprint(
            session, normalized_node_ids
        ),
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
    if request_count < MIN_MEASURED_REQUESTS and duration_seconds < MIN_MEASURED_SECONDS:
        failures.append("MEASUREMENT_WINDOW")
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
) -> BenchmarkEvidence:
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
    evidence_digest = _digest(canonical)
    existing = session.scalar(
        select(BenchmarkEvidence).where(
            BenchmarkEvidence.evidence_digest == evidence_digest
        )
    )
    if existing is not None:
        return existing

    registration_sequence = (
        session.scalar(
            select(func.max(BenchmarkEvidence.registration_sequence)).where(
                BenchmarkEvidence.placement_id == placement.id
            )
        )
        or 0
    ) + 1
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
            actor="admin",
            action="benchmark_evidence.register",
            target=record.id,
            outcome=status.lower(),
            detail={
                "release_id": release.id,
                "placement_id": placement.id,
                "failure_codes": failure_codes,
            },
        )
    )
    session.commit()
    return record


def benchmark_evidence_dict(record: BenchmarkEvidence) -> dict[str, Any]:
    return {
        key: getattr(record, key)
        for key in (
            "id",
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
            current = benchmark_inventory_fingerprint(
                session, list(evidence.node_ids)
            )
        except (BenchmarkNotFoundError, BenchmarkPromotionError) as exc:
            detail.update(code="PROFILE_UNAVAILABLE", reason=str(exc))
            blocked.append(detail)
            continue
        if current != evidence.inventory_fingerprint:
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

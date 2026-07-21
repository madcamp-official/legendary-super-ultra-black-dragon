from __future__ import annotations

import hashlib
import json
import math
import re
import secrets
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import or_, select
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dure.artifact_manifest import (
    ArtifactManifestLimits,
    canonical_artifact_manifest as parse_canonical_artifact_manifest,
)
from dure.artifact_prepare import validate_digest_pinned_runtime_image
from dure.models import DeploymentPlan, NodeProfile, VLLM_RAY_PP_BACKEND
from dure.pipeline_runtime import validate_strict_pipeline_plan
from dure.profile_generator import (
    AUTO_PROFILE_ORIGIN,
    PLACEMENT_PROFILE_STATUSES,
    generate_auto_placement_profile_specs,
)
from dure.resource_pool import FLEET_MODEL_IDS, FLEET_TENSOR_PARALLEL_SIZE
from dure.task import (
    MAX_BENCHMARK_CONTEXT_TOKENS,
    MAX_BENCHMARK_INTEGER,
    BenchmarkTaskPayload,
)

from .benchmark import (
    BENCHMARK_POLICY_VERSION,
    BENCHMARK_SUITE_ID,
    MIN_MEASURED_REQUESTS,
    MIN_MEASURED_SECONDS,
    MIN_WARMUP_REQUESTS,
    BenchmarkIdentityMismatchError,
    BenchmarkNotFoundError,
    BenchmarkPromotionError,
    benchmark_context,
    register_benchmark_evidence,
)
from .models import (
    ArtifactChunk,
    ArtifactFileChunk,
    ArtifactManifest,
    ArtifactManifestFile,
    AuditEvent,
    BenchmarkRun,
    Deployment,
    DeploymentOperation,
    DeploymentOperationNode,
    EnrollmentToken,
    ModelArtifact,
    ModelRelease,
    Node,
    NodeCredential,
    NodeProfileRecord,
    PlacementProfileRecord,
    RuntimeRelease,
    Task,
    TaskStatus,
    TaskType,
    utcnow,
)
from .qualification import active_profile_qualification_nodes
from .rollout import (
    DeploymentRolloutConflictError,
    PHASE_TASK_TYPES,
    attach_deployment_bulk_operation,
    cancel_operation_task,
    claim_operation_task,
    finish_operation_task,
    valid_deployment_task_success_result,
)


MODEL_RELEASE_TRANSITIONS = {
    "DRAFT": {"VALIDATED", "REVOKED"},
    "VALIDATED": {"ACTIVE", "REVOKED"},
    "ACTIVE": {"DEPRECATED", "REVOKED"},
    "DEPRECATED": {"REVOKED"},
    "REVOKED": set(),
}
STRICT_RAY_AGENT_VERSION = (0, 3, 18)
STAGE_ARTIFACT_AGENT_VERSION = (0, 3, 19)
ARTIFACT_CACHE_QUARANTINE_AGENT_VERSION = (0, 3, 20)
BENCHMARK_PREPARATION_AGENT_VERSION = (0, 3, 25)


def _agent_supports_strict_ray(value: str) -> bool:
    if type(value) is not str:
        return False
    matched = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)(?:\+[0-9A-Za-z.-]+)?", value)
    if matched is None:
        return False
    return tuple(int(part) for part in matched.groups()) >= STRICT_RAY_AGENT_VERSION


def _agent_supports_stage_artifact(value: str) -> bool:
    if type(value) is not str:
        return False
    matched = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)(?:\+[0-9A-Za-z.-]+)?", value)
    if matched is None:
        return False
    return tuple(int(part) for part in matched.groups()) >= STAGE_ARTIFACT_AGENT_VERSION


def _agent_supports_benchmark_preparation(value: str) -> bool:
    if type(value) is not str:
        return False
    matched = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)(?:\+[0-9A-Za-z.-]+)?", value)
    if matched is None:
        return False
    return (
        tuple(int(part) for part in matched.groups())
        >= BENCHMARK_PREPARATION_AGENT_VERSION
    )


QUANTIZATIONS = {"awq", "gptq", "fp8", "fp16", "bf16", "int8"}
GPU_ARCHITECTURES = {"ampere", "ada", "hopper", "blackwell"}
TOPOLOGIES = {"single-gpu", "pipeline"}
BENCHMARK_WORKLOAD_IDS = {
    "short-chat-1k-128",
    "long-chat-4k-256",
    "max-context",
    "quality-eval",
}
BENCHMARK_TASK_FAILURE_CODES = {
    "BENCHMARK_EXECUTION_FAILED",
    "BENCHMARK_PAYLOAD_REJECTED",
    "BENCHMARK_RUNTIME_UNAVAILABLE",
    "BENCHMARK_ARTIFACT_UNAVAILABLE",
    "BENCHMARK_EVIDENCE_REJECTED",
    "BENCHMARK_CANCELED",
}
BENCHMARK_RESULT_METRIC_FIELDS = {
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
ARTIFACT_MANIFEST_SCHEMA_VERSION = 1
MAX_ARTIFACT_MANIFEST_FILES = 100_000
MAX_ARTIFACT_MANIFEST_CHUNKS = 1_000_000
MAX_ARTIFACT_PATH_LENGTH = 1024
MAX_ARTIFACT_FILE_BYTES = 1 << 50
MAX_ARTIFACT_TOTAL_BYTES = 1 << 50
_ARTIFACT_CHUNK_BATCH_SIZE = 200


class RegistryConflictError(ValueError):
    pass


class ArtifactManifestNotFoundError(ValueError):
    pass


class ArtifactManifestConflictError(ValueError):
    pass


class ArtifactCacheControlError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        code: str,
        details: dict | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = details or {}


class BenchmarkRunNotFoundError(ValueError):
    pass


class BenchmarkRunError(ValueError):
    def __init__(self, message: str, *, code: str, details: dict | None = None):
        super().__init__(message)
        self.code = code
        self.details = details or {}


def secret_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def aware(value: datetime | None) -> datetime | None:
    if value is not None and value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _artifact_cache_projection(session: Session, cache_id: str) -> dict:
    """Single adapter between the API surface and cache lifecycle storage."""
    from .cache_lifecycle import (
        ArtifactCacheNotFoundError,
        artifact_cache_projection,
    )

    value = artifact_cache_projection(session, cache_id=cache_id)
    if value is None:
        raise ArtifactCacheNotFoundError()
    return value


def _list_artifact_cache_projections(session: Session) -> list[dict]:
    from .cache_lifecycle import list_artifact_cache_projections

    return list_artifact_cache_projections(session)


def _artifact_cache_reference_projection(session: Session, cache_id: str) -> dict:
    from .cache_lifecycle import artifact_cache_reference_projection

    value = artifact_cache_reference_projection(session, cache_id=cache_id)
    if (
        type(value) is not dict
        or set(value)
        != {"schema_version", "cache_id", "complete", "blocking_references"}
        or value["schema_version"] != 1
        or value["cache_id"] != cache_id
        or type(value["complete"]) is not bool
        or type(value["blocking_references"]) is not list
        or any(type(item) is not dict for item in value["blocking_references"])
    ):
        raise ArtifactCacheControlError(
            "artifact cache reference projection is unavailable",
            code="ARTIFACT_CACHE_REFERENCES_UNKNOWN",
        )
    return value


def list_artifact_caches(session: Session) -> list[dict]:
    return _list_artifact_cache_projections(session)


def artifact_cache_detail(session: Session, cache_id: str) -> dict:
    return _artifact_cache_projection(session, cache_id)


def verify_artifact_cache(session: Session, cache_id: str) -> dict:
    """Return central evidence and blockers without creating tasks or events."""
    cache = _artifact_cache_projection(session, cache_id)
    references = _artifact_cache_reference_projection(session, cache_id)
    return {
        "cache": cache,
        "references": references,
        "eligible_for_quarantine": (
            references["complete"]
            and not references["blocking_references"]
            and cache.get("status") not in {"MISSING", "QUARANTINED"}
        ),
    }


def _validated_cache_control_projection(value: dict, cache_id: str) -> tuple[str, str, str]:
    if type(value) is not dict or value.get("id") != cache_id:
        raise ArtifactCacheControlError(
            "artifact cache projection is invalid",
            code="ARTIFACT_CACHE_PROJECTION_INVALID",
        )
    node_id = value.get("node_id")
    cache_kind = value.get("cache_kind")
    cache_identity_digest = value.get("cache_identity_digest")
    try:
        parsed_node_id = uuid.UUID(node_id) if type(node_id) is str else None
    except ValueError as exc:
        raise ArtifactCacheControlError(
            "artifact cache projection is invalid",
            code="ARTIFACT_CACHE_PROJECTION_INVALID",
        ) from exc
    if (
        parsed_node_id is None
        or str(parsed_node_id) != node_id
        or parsed_node_id.version != 4
        or cache_kind not in {"FULL_SNAPSHOT", "STAGE"}
        or type(cache_identity_digest) is not str
        or re.fullmatch(r"sha256:[0-9a-f]{64}", cache_identity_digest) is None
    ):
        raise ArtifactCacheControlError(
            "artifact cache projection is invalid",
            code="ARTIFACT_CACHE_PROJECTION_INVALID",
        )
    return node_id, cache_kind, cache_identity_digest


def _agent_supports_artifact_cache_quarantine(value: str) -> bool:
    if type(value) is not str:
        return False
    matched = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)(?:\+[0-9A-Za-z.-]+)?", value)
    return (
        matched is not None
        and tuple(int(part) for part in matched.groups())
        >= ARTIFACT_CACHE_QUARANTINE_AGENT_VERSION
    )


def prepare_or_apply_artifact_cache_quarantine(
    session: Session,
    cache_id: str,
    *,
    apply: bool,
) -> tuple[dict, dict, list[Task], bool]:
    """Preview by default; queue one closed task only after explicit apply."""
    if type(apply) is not bool:
        raise ArtifactCacheControlError(
            "artifact cache quarantine apply must be a strict boolean",
            code="ARTIFACT_CACHE_QUARANTINE_REQUEST_INVALID",
        )
    cache = _artifact_cache_projection(session, cache_id)
    node_id, cache_kind, cache_identity_digest = _validated_cache_control_projection(
        cache, cache_id
    )
    references = _artifact_cache_reference_projection(session, cache_id)
    if not apply:
        return cache, references, [], False
    try:
        from .cache_lifecycle import request_cache_quarantine
        from .models import NodeArtifactCache

        locked_node = session.scalar(
            select(Node)
            .join(NodeArtifactCache, NodeArtifactCache.node_id == Node.id)
            .where(NodeArtifactCache.id == cache_id)
            .with_for_update(of=Node)
            .execution_options(populate_existing=True)
        )
        if locked_node is None or locked_node.id != node_id:
            raise ArtifactCacheControlError(
                "artifact cache node is unavailable",
                code="ARTIFACT_CACHE_NODE_UNAVAILABLE",
            )
        cache = _artifact_cache_projection(session, cache_id)
        current_identity = _validated_cache_control_projection(cache, cache_id)
        if current_identity != (node_id, cache_kind, cache_identity_digest):
            raise ArtifactCacheControlError(
                "artifact cache identity changed",
                code="ARTIFACT_CACHE_PROJECTION_INVALID",
            )
        if cache.get("status") == "QUARANTINED":
            return cache, _artifact_cache_reference_projection(session, cache_id), [], False
        if cache.get("status") == "MISSING":
            raise ArtifactCacheControlError(
                "a missing artifact cache cannot be quarantined",
                code="ARTIFACT_CACHE_SOURCE_MISSING",
            )
        active_tasks = list(
            session.scalars(
                select(Task)
                .where(
                    Task.node_id == node_id,
                    Task.status.in_(
                        {TaskStatus.QUEUED.value, TaskStatus.RUNNING.value}
                    ),
                )
                .order_by(Task.created_at, Task.id)
                .with_for_update()
            )
        )
        for current in active_tasks:
            if (
                current.type == TaskType.QUARANTINE_ARTIFACT_CACHE.value
                and current.payload
                == {
                    "node_id": node_id,
                    "cache_kind": cache_kind,
                    "cache_identity_digest": cache_identity_digest,
                }
            ):
                return cache, _artifact_cache_reference_projection(session, cache_id), [current], False
        references = _artifact_cache_reference_projection(session, cache_id)
        if not references["complete"]:
            raise ArtifactCacheControlError(
                "artifact cache references could not be proven complete",
                code="ARTIFACT_CACHE_REFERENCES_UNKNOWN",
            )
        if references["blocking_references"] or active_tasks:
            raise ArtifactCacheControlError(
                "artifact cache is still referenced",
                code="ARTIFACT_CACHE_REFERENCED",
                details={
                    "blocking_references": references["blocking_references"],
                    "active_task_ids": [item.id for item in active_tasks],
                },
            )
        if (
            not locked_node.approved
            or node_status(locked_node.last_seen, utcnow()) != "online"
        ):
            raise ArtifactCacheControlError(
                "artifact cache node must be approved and online",
                code="ARTIFACT_CACHE_NODE_UNAVAILABLE",
            )
        if not _agent_supports_artifact_cache_quarantine(
            locked_node.agent_version
        ):
            raise ArtifactCacheControlError(
                "artifact cache quarantine requires Dure Agent 0.3.20 or newer",
                code="ARTIFACT_CACHE_AGENT_TOO_OLD",
            )
        task = Task(
            id=str(uuid.uuid4()),
            bulk_id=str(uuid.uuid4()),
            node_id=node_id,
            type=TaskType.QUARANTINE_ARTIFACT_CACHE.value,
            payload={
                "node_id": node_id,
                "cache_kind": cache_kind,
                "cache_identity_digest": cache_identity_digest,
            },
        )
        session.add(task)
        session.flush()
        request_cache_quarantine(
            session,
            node_id=node_id,
            cache_identity_digest=cache_identity_digest,
            request_id=task.id,
            source_task_id=task.id,
        )
        locked_node.desired_state = TaskType.QUARANTINE_ARTIFACT_CACHE.value
        audit(
            session,
            "admin",
            "artifact-cache.quarantine",
            cache_id,
            "success",
            task_id=task.id,
        )
        session.commit()
        return (
            _artifact_cache_projection(session, cache_id),
            _artifact_cache_reference_projection(session, cache_id),
            [task],
            True,
        )
    except Exception:
        session.rollback()
        raise


def audit(session: Session, actor: str, action: str, target: str | None, outcome: str, **detail) -> None:
    session.add(AuditEvent(actor=actor, action=action, target=target, outcome=outcome, detail=detail))


def _require_digest(value: str, *, field: str) -> None:
    if re.fullmatch(r"sha256:[0-9a-f]{64}", value) is None:
        raise ValueError(f"{field} must be an immutable sha256 digest")


def _canonical_artifact_manifest(
    manifest: dict,
) -> tuple[dict, str, str, int, int, int]:
    return parse_canonical_artifact_manifest(
        manifest,
        limits=ArtifactManifestLimits(
            max_files=MAX_ARTIFACT_MANIFEST_FILES,
            max_chunks=MAX_ARTIFACT_MANIFEST_CHUNKS,
            max_path_length=MAX_ARTIFACT_PATH_LENGTH,
            max_file_bytes=MAX_ARTIFACT_FILE_BYTES,
            max_total_bytes=MAX_ARTIFACT_TOTAL_BYTES,
        ),
    )


def canonical_artifact_manifest_digest(manifest: dict) -> str:
    return _canonical_artifact_manifest(manifest)[2]


def _artifact_chunks_by_digest(
    session: Session,
    digests: list[str],
) -> dict[str, ArtifactChunk]:
    records: dict[str, ArtifactChunk] = {}
    for start in range(0, len(digests), _ARTIFACT_CHUNK_BATCH_SIZE):
        batch = digests[start : start + _ARTIFACT_CHUNK_BATCH_SIZE]
        records.update(
            {
                record.digest: record
                for record in session.scalars(
                    select(ArtifactChunk).where(ArtifactChunk.digest.in_(batch))
                )
            }
        )
    return records


def _ensure_artifact_chunks(
    session: Session,
    chunk_sizes: dict[str, int],
) -> None:
    digests = sorted(chunk_sizes)
    stored = _artifact_chunks_by_digest(session, digests)
    for digest, record in stored.items():
        if record.size_bytes != chunk_sizes[digest]:
            raise ArtifactManifestConflictError(
                "stored chunk digest has a different immutable size"
            )

    missing = [digest for digest in digests if digest not in stored]
    dialect = session.get_bind().dialect.name
    for start in range(0, len(missing), _ARTIFACT_CHUNK_BATCH_SIZE):
        batch = missing[start : start + _ARTIFACT_CHUNK_BATCH_SIZE]
        values = [
            {
                "digest": digest,
                "size_bytes": chunk_sizes[digest],
                "created_at": utcnow(),
            }
            for digest in batch
        ]
        if dialect == "postgresql":
            statement = postgresql_insert(ArtifactChunk).values(values)
            session.execute(
                statement.on_conflict_do_nothing(index_elements=["digest"])
            )
        elif dialect == "sqlite":
            statement = sqlite_insert(ArtifactChunk).values(values)
            session.execute(
                statement.on_conflict_do_nothing(index_elements=["digest"])
            )
        else:  # pragma: no cover - production and development use PostgreSQL/SQLite
            for value in values:
                try:
                    with session.begin_nested():
                        session.add(ArtifactChunk(**value))
                        session.flush()
                except IntegrityError:
                    pass

    stored = _artifact_chunks_by_digest(session, digests)
    if set(stored) != set(digests):
        raise ArtifactManifestConflictError(
            "artifact chunk registry is incomplete after registration"
        )
    if any(
        stored[digest].size_bytes != size_bytes
        for digest, size_bytes in chunk_sizes.items()
    ):
        raise ArtifactManifestConflictError(
            "stored chunk digest has a different immutable size"
        )


def create_model_artifact(
    session: Session,
    *,
    model_id: str,
    repository: str,
    revision: str,
    manifest_digest: str,
    quantization: str,
    size_mib: int,
    default_max_model_len: int,
    layer_count: int,
    license_id: str,
) -> ModelArtifact:
    if re.fullmatch(r"[a-z0-9][a-z0-9._-]{1,99}", model_id) is None:
        raise ValueError("invalid model_id")
    if re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository) is None:
        raise ValueError("invalid model repository")
    if re.fullmatch(r"[0-9a-f]{40,64}", revision) is None:
        raise ValueError("model revision must be an immutable commit hash")
    _require_digest(manifest_digest, field="manifest_digest")
    if quantization not in QUANTIZATIONS:
        raise ValueError("unsupported quantization")
    if min(size_mib, default_max_model_len, layer_count) <= 0:
        raise ValueError("model sizes and layer count must be positive")
    if not license_id.strip() or len(license_id) > 100:
        raise ValueError("license_id is required")
    existing = session.scalar(
        select(ModelArtifact.id).where(
            or_(
                ModelArtifact.manifest_digest == manifest_digest,
                (
                    (ModelArtifact.repository == repository)
                    & (ModelArtifact.revision == revision)
                    & (ModelArtifact.quantization == quantization)
                ),
            )
        )
    )
    if existing is not None:
        raise RegistryConflictError("model artifact already exists")
    record = ModelArtifact(
        model_id=model_id,
        repository=repository,
        revision=revision,
        manifest_digest=manifest_digest,
        quantization=quantization,
        size_mib=size_mib,
        default_max_model_len=default_max_model_len,
        layer_count=layer_count,
        license_id=license_id,
    )
    session.add(record)
    try:
        session.flush()
    except IntegrityError as exc:
        session.rollback()
        raise RegistryConflictError("model artifact already exists") from exc
    audit(session, "admin", "model_artifact.create", record.id, "success")
    session.commit()
    return record


def _artifact_manifest_record_matches(
    record: ArtifactManifest,
    *,
    artifact_id: str | None,
    canonical_json: str,
    total_size_bytes: int,
    file_count: int,
    chunk_count: int,
) -> bool:
    return (
        record.model_artifact_id == artifact_id
        and record.schema_version == ARTIFACT_MANIFEST_SCHEMA_VERSION
        and record.canonical_json == canonical_json
        and record.total_size_bytes == total_size_bytes
        and record.file_count == file_count
        and record.chunk_count == chunk_count
    )


def register_artifact_manifest(
    session: Session,
    *,
    artifact_id: str,
    manifest: dict,
    commit: bool = True,
) -> tuple[ArtifactManifest, bool]:
    artifact = session.get(ModelArtifact, artifact_id)
    if artifact is None:
        raise ArtifactManifestNotFoundError("model artifact not found")
    (
        canonical,
        canonical_json,
        digest,
        total_size_bytes,
        file_count,
        chunk_count,
    ) = _canonical_artifact_manifest(manifest)
    if artifact.manifest_digest != digest:
        raise ArtifactManifestConflictError(
            "canonical manifest digest does not match the model artifact"
        )

    def existing_is_exact(record: ArtifactManifest) -> bool:
        if not _artifact_manifest_record_matches(
            record,
            artifact_id=artifact.id,
            canonical_json=canonical_json,
            total_size_bytes=total_size_bytes,
            file_count=file_count,
            chunk_count=chunk_count,
        ):
            return False
        try:
            stored = artifact_manifest_dict(session, record)
        except ArtifactManifestConflictError:
            return False
        return (
            stored["schema_version"] == canonical["schema_version"]
            and stored["files"] == canonical["files"]
        )

    existing = session.get(ArtifactManifest, digest)
    if existing is not None:
        if existing_is_exact(existing):
            return existing, False
        raise ArtifactManifestConflictError(
            "manifest digest is already bound to different immutable content"
        )
    artifact_manifest = session.scalar(
        select(ArtifactManifest).where(
            ArtifactManifest.model_artifact_id == artifact.id
        )
    )
    if artifact_manifest is not None:
        raise ArtifactManifestConflictError(
            "model artifact is already bound to a different manifest"
        )

    chunk_sizes: dict[str, int] = {}
    for file_item in canonical["files"]:
        for chunk_item in file_item["chunks"]:
            chunk_sizes[chunk_item["sha256"]] = chunk_item["length_bytes"]
    record = ArtifactManifest(
        digest=digest,
        schema_version=ARTIFACT_MANIFEST_SCHEMA_VERSION,
        model_artifact_id=artifact.id,
        total_size_bytes=total_size_bytes,
        file_count=file_count,
        chunk_count=chunk_count,
        canonical_json=canonical_json,
    )
    file_records: list[ArtifactManifestFile] = []
    link_records: list[ArtifactFileChunk] = []
    for file_ordinal, file_item in enumerate(canonical["files"]):
        file_id = str(uuid.uuid4())
        file_records.append(
            ArtifactManifestFile(
                id=file_id,
                manifest_digest=digest,
                ordinal=file_ordinal,
                path=file_item["path"],
                kind=file_item["kind"],
                size_bytes=file_item["size_bytes"],
                file_digest=file_item["sha256"],
            )
        )
        link_records.extend(
            ArtifactFileChunk(
                file_id=file_id,
                ordinal=chunk_item["ordinal"],
                chunk_digest=chunk_item["sha256"],
                offset_bytes=chunk_item["offset_bytes"],
                length_bytes=chunk_item["length_bytes"],
            )
            for chunk_item in file_item["chunks"]
        )

    try:
        with session.begin_nested():
            _ensure_artifact_chunks(session, chunk_sizes)
            session.add(record)
            session.flush()
            session.add_all(file_records)
            session.flush()
            session.add_all(link_records)
            session.flush()
    except IntegrityError as exc:
        session.expire_all()
        existing = session.get(ArtifactManifest, digest)
        if existing is not None and existing_is_exact(existing):
            return existing, False
        raise ArtifactManifestConflictError(
            "artifact manifest registration conflicts with immutable registry data"
        ) from exc
    if commit:
        session.commit()
    return record, True


def get_artifact_manifest(
    session: Session,
    artifact_id: str,
) -> ArtifactManifest | None:
    if session.get(ModelArtifact, artifact_id) is None:
        raise ArtifactManifestNotFoundError("model artifact not found")
    return session.scalar(
        select(ArtifactManifest).where(
            ArtifactManifest.model_artifact_id == artifact_id
        )
    )


def artifact_manifest_dict(
    session: Session,
    record: ArtifactManifest,
) -> dict:
    files = list(
        session.scalars(
            select(ArtifactManifestFile)
            .where(ArtifactManifestFile.manifest_digest == record.digest)
            .order_by(ArtifactManifestFile.ordinal, ArtifactManifestFile.id)
        )
    )
    link_rows = list(
        session.execute(
            select(ArtifactFileChunk, ArtifactChunk.size_bytes)
            .join(
                ArtifactManifestFile,
                ArtifactManifestFile.id == ArtifactFileChunk.file_id,
            )
            .outerjoin(
                ArtifactChunk,
                ArtifactChunk.digest == ArtifactFileChunk.chunk_digest,
            )
            .where(ArtifactManifestFile.manifest_digest == record.digest)
            .order_by(
                ArtifactManifestFile.ordinal,
                ArtifactFileChunk.ordinal,
            )
        )
    )
    links_by_file: dict[str, list[ArtifactFileChunk]] = {
        item.id: [] for item in files
    }
    invalid_chunk_link = False
    for link, stored_size in link_rows:
        if link.file_id not in links_by_file:
            invalid_chunk_link = True
            continue
        links_by_file[link.file_id].append(link)
        if stored_size is None or stored_size != link.length_bytes:
            invalid_chunk_link = True
    manifest = {
        "schema_version": record.schema_version,
        "files": [
            {
                "path": file_record.path,
                "kind": file_record.kind,
                "size_bytes": file_record.size_bytes,
                "sha256": file_record.file_digest,
                "chunks": [
                    {
                        "ordinal": link.ordinal,
                        "offset_bytes": link.offset_bytes,
                        "length_bytes": link.length_bytes,
                        "sha256": link.chunk_digest,
                    }
                    for link in links_by_file[file_record.id]
                ],
            }
            for file_record in files
        ],
    }
    try:
        (
            canonical,
            canonical_json,
            digest,
            total_size_bytes,
            file_count,
            chunk_count,
        ) = _canonical_artifact_manifest(manifest)
    except ValueError as exc:
        raise ArtifactManifestConflictError(
            "stored artifact manifest is internally inconsistent"
        ) from exc
    files_have_valid_ordinals = all(
        file_record.ordinal == ordinal
        for ordinal, file_record in enumerate(files)
    )
    if not files_have_valid_ordinals or invalid_chunk_link or not _artifact_manifest_record_matches(
        record,
        artifact_id=record.model_artifact_id,
        canonical_json=canonical_json,
        total_size_bytes=total_size_bytes,
        file_count=file_count,
        chunk_count=chunk_count,
    ) or digest != record.digest:
        raise ArtifactManifestConflictError(
            "stored artifact manifest is internally inconsistent"
        )
    created_at = aware(record.created_at)
    return {
        "digest": record.digest,
        "model_artifact_id": record.model_artifact_id,
        "schema_version": canonical["schema_version"],
        "total_size_bytes": record.total_size_bytes,
        "file_count": record.file_count,
        "chunk_count": record.chunk_count,
        "files": canonical["files"],
        "created_at": created_at.isoformat() if created_at is not None else None,
    }


def create_runtime_release(
    session: Session,
    *,
    version: str,
    image: str,
    vllm_version: str,
    cuda_version: str,
    gpu_architectures: list[str],
) -> RuntimeRelease:
    try:
        validate_digest_pinned_runtime_image(image)
    except ValueError:
        raise ValueError("runtime image must be OCI digest-pinned") from None
    if not all(isinstance(item, str) for item in gpu_architectures):
        raise ValueError("unsupported GPU architecture")
    normalized_architectures = sorted(set(gpu_architectures))
    if not normalized_architectures or not set(normalized_architectures) <= GPU_ARCHITECTURES:
        raise ValueError("unsupported GPU architecture")
    if not version.strip() or not vllm_version.strip() or not cuda_version.strip():
        raise ValueError("runtime version fields are required")
    if session.scalar(select(RuntimeRelease.id).where(RuntimeRelease.image == image)) is not None:
        raise RegistryConflictError("runtime release already exists")
    record = RuntimeRelease(
        version=version,
        image=image,
        vllm_version=vllm_version,
        cuda_version=cuda_version,
        gpu_architectures=normalized_architectures,
    )
    session.add(record)
    try:
        session.flush()
    except IntegrityError as exc:
        session.rollback()
        raise RegistryConflictError("runtime release already exists") from exc
    audit(session, "admin", "runtime_release.create", record.id, "success")
    session.commit()
    return record


def create_model_release(
    session: Session, *, artifact_id: str, runtime_id: str, quality_rank: int
) -> ModelRelease:
    if session.get(ModelArtifact, artifact_id) is None:
        raise ValueError("unknown model artifact")
    if session.get(RuntimeRelease, runtime_id) is None:
        raise ValueError("unknown runtime release")
    if quality_rank <= 0:
        raise ValueError("quality_rank must be positive")
    if session.scalar(
        select(ModelRelease.id).where(
            ModelRelease.artifact_id == artifact_id, ModelRelease.runtime_id == runtime_id
        )
    ) is not None:
        raise RegistryConflictError("model release already exists")
    record = ModelRelease(
        artifact_id=artifact_id,
        runtime_id=runtime_id,
        quality_rank=quality_rank,
        status="DRAFT",
    )
    session.add(record)
    try:
        session.flush()
    except IntegrityError as exc:
        session.rollback()
        raise RegistryConflictError("model release already exists") from exc
    audit(session, "admin", "model_release.create", record.id, "success")
    session.commit()
    return record


def add_placement_profile(
    session: Session,
    *,
    release_id: str,
    profile_id: str,
    topology: str,
    node_count: int,
    min_gpu_memory_mib: int,
    min_disk_free_mib: int,
    pipeline_parallel_size: int,
    tensor_parallel_size: int,
    requires_network_evidence: bool,
    requires_nccl: bool,
    min_bandwidth_mbps: int | None,
    max_rtt_ms: float | None,
    max_packet_loss_pct: float | None,
    max_ttft_p95_ms: float,
    max_tpot_p95_ms: float,
    max_e2e_p95_ms: float,
    min_success_rate: float,
    min_vram_headroom_pct: float,
    min_throughput_tps: float,
    max_model_len: int | None = None,
    max_concurrency: int = 1,
    origin: str = "MANUAL",
    status: str = "ACTIVE",
    spec_digest: str | None = None,
    _commit: bool = True,
) -> PlacementProfileRecord:
    release = session.scalar(
        select(ModelRelease).where(ModelRelease.id == release_id).with_for_update()
    )
    if release is None:
        raise ValueError("unknown model release")
    if release.status != "DRAFT":
        raise ValueError("placement profiles can only be added to DRAFT releases")
    artifact = session.get(ModelArtifact, release.artifact_id)
    if artifact is None:
        raise ValueError("unknown model artifact")
    if re.fullmatch(r"[a-z0-9][a-z0-9._-]{1,99}", profile_id) is None:
        raise ValueError("invalid placement profile_id")
    if topology not in TOPOLOGIES:
        raise ValueError("unsupported topology")
    if min(node_count, min_gpu_memory_mib, min_disk_free_mib) <= 0:
        raise ValueError("placement resource requirements must be positive")
    if pipeline_parallel_size <= 0 or tensor_parallel_size <= 0:
        raise ValueError("parallel sizes must be positive")
    if pipeline_parallel_size * tensor_parallel_size != node_count:
        raise ValueError("parallel sizes must match node_count")
    if topology == "single-gpu" and node_count != 1:
        raise ValueError("single-gpu topology requires one node")
    network_values = (min_bandwidth_mbps, max_rtt_ms, max_packet_loss_pct)
    if node_count > 1 and (
        not requires_network_evidence
        or not requires_nccl
        or any(value is None for value in network_values)
    ):
        raise ValueError("multi-node placement requires network and NCCL thresholds")
    if requires_network_evidence and (
        min_bandwidth_mbps is None
        or min_bandwidth_mbps <= 0
        or max_rtt_ms is None
        or max_rtt_ms < 0
        or max_packet_loss_pct is None
        or not 0 <= max_packet_loss_pct <= 100
    ):
        raise ValueError("network thresholds are out of range")
    if any(value <= 0 for value in (max_ttft_p95_ms, max_tpot_p95_ms, max_e2e_p95_ms)):
        raise ValueError("latency SLO values must be positive")
    if not 0 <= min_success_rate <= 1 or not 0 <= min_vram_headroom_pct <= 100:
        raise ValueError("success and VRAM thresholds are out of range")
    if min_throughput_tps <= 0:
        raise ValueError("throughput SLO must be positive")
    if max_model_len is None:
        max_model_len = artifact.default_max_model_len
    if max_model_len <= 0 or max_model_len > artifact.default_max_model_len:
        raise ValueError("max_model_len exceeds the immutable model contract")
    if max_concurrency <= 0:
        raise ValueError("max_concurrency must be positive")
    if origin not in {"MANUAL", AUTO_PROFILE_ORIGIN}:
        raise ValueError("unknown placement profile origin")
    if status not in PLACEMENT_PROFILE_STATUSES:
        raise ValueError("unknown placement profile status")
    if origin == AUTO_PROFILE_ORIGIN:
        if artifact.model_id not in FLEET_MODEL_IDS:
            raise ValueError("automatic profiles support only the Fleet model allowlist")
        if tensor_parallel_size != FLEET_TENSOR_PARALLEL_SIZE:
            raise ValueError("automatic profiles require TP=1")
        if pipeline_parallel_size != node_count:
            raise ValueError("automatic profiles require PP=node_count")
        if status != "DRAFT":
            raise ValueError("automatic profiles must be created as DRAFT")
    elif status != "ACTIVE":
        raise ValueError("manual profiles must be created as ACTIVE")
    if spec_digest is None:
        canonical_spec = {
            "model_id": artifact.model_id,
            "profile_id": profile_id,
            "topology": topology,
            "node_count": node_count,
            "min_gpu_memory_mib": min_gpu_memory_mib,
            "min_disk_free_mib": min_disk_free_mib,
            "pipeline_parallel_size": pipeline_parallel_size,
            "tensor_parallel_size": tensor_parallel_size,
            "max_model_len": max_model_len,
            "max_concurrency": max_concurrency,
            "requires_network_evidence": requires_network_evidence,
            "requires_nccl": requires_nccl,
            "min_bandwidth_mbps": min_bandwidth_mbps,
            "max_rtt_ms": max_rtt_ms,
            "max_packet_loss_pct": max_packet_loss_pct,
            "max_ttft_p95_ms": max_ttft_p95_ms,
            "max_tpot_p95_ms": max_tpot_p95_ms,
            "max_e2e_p95_ms": max_e2e_p95_ms,
            "min_success_rate": min_success_rate,
            "min_vram_headroom_pct": min_vram_headroom_pct,
            "min_throughput_tps": min_throughput_tps,
            "origin": origin,
        }
        encoded = json.dumps(
            canonical_spec, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        spec_digest = "sha256:" + hashlib.sha256(encoded).hexdigest()
    if re.fullmatch(r"sha256:[0-9a-f]{64}", spec_digest) is None:
        raise ValueError("placement spec_digest must be canonical SHA-256")
    if session.scalar(
        select(PlacementProfileRecord.id).where(
            PlacementProfileRecord.release_id == release_id,
            PlacementProfileRecord.profile_id == profile_id,
        )
    ) is not None:
        raise RegistryConflictError("placement profile already exists")
    record = PlacementProfileRecord(
        release_id=release_id,
        profile_id=profile_id,
        topology=topology,
        node_count=node_count,
        min_gpu_memory_mib=min_gpu_memory_mib,
        min_disk_free_mib=min_disk_free_mib,
        pipeline_parallel_size=pipeline_parallel_size,
        tensor_parallel_size=tensor_parallel_size,
        max_model_len=max_model_len,
        max_concurrency=max_concurrency,
        origin=origin,
        status=status,
        spec_digest=spec_digest,
        requires_network_evidence=requires_network_evidence,
        requires_nccl=requires_nccl,
        min_bandwidth_mbps=min_bandwidth_mbps,
        max_rtt_ms=max_rtt_ms,
        max_packet_loss_pct=max_packet_loss_pct,
        max_ttft_p95_ms=max_ttft_p95_ms,
        max_tpot_p95_ms=max_tpot_p95_ms,
        max_e2e_p95_ms=max_e2e_p95_ms,
        min_success_rate=min_success_rate,
        min_vram_headroom_pct=min_vram_headroom_pct,
        min_throughput_tps=min_throughput_tps,
    )
    session.add(record)
    try:
        session.flush()
    except IntegrityError as exc:
        session.rollback()
        raise RegistryConflictError("placement profile already exists") from exc
    audit(session, "admin", "placement_profile.create", record.id, "success")
    if _commit:
        session.commit()
    return record


def generate_auto_placement_profiles(
    session: Session,
    *,
    release_id: str,
    apply: bool,
) -> dict[str, object]:
    """Preview or atomically persist the closed automatic profile set."""

    if type(apply) is not bool:
        raise ValueError("apply must be a boolean")
    release = session.scalar(
        select(ModelRelease).where(ModelRelease.id == release_id).with_for_update()
    )
    if release is None:
        raise ValueError("unknown model release")
    if release.status != "DRAFT":
        raise ValueError("automatic profiles require a DRAFT model release")
    artifact = session.get(ModelArtifact, release.artifact_id)
    if artifact is None:
        raise ValueError("unknown model artifact")
    specs = generate_auto_placement_profile_specs(artifact.model_id)
    existing = {
        record.profile_id: record
        for record in session.scalars(
            select(PlacementProfileRecord).where(
                PlacementProfileRecord.release_id == release.id,
                PlacementProfileRecord.profile_id.in_(
                    [spec.profile_id for spec in specs]
                ),
            )
        )
    }
    profile_results: list[dict[str, object]] = []
    missing = []
    for spec in specs:
        record = existing.get(spec.profile_id)
        if record is None:
            state = "MISSING"
            missing.append(spec)
        elif (
            record.origin == AUTO_PROFILE_ORIGIN
            and record.spec_digest == spec.spec_digest
        ):
            state = "EXISTS"
        else:
            raise RegistryConflictError(
                f"placement profile identity conflicts: {spec.profile_id}"
            )
        profile_results.append({**spec.to_dict(), "state": state})

    created_profile_ids: list[str] = []
    if apply:
        for spec in missing:
            record = add_placement_profile(
                session,
                release_id=release.id,
                _commit=False,
                **spec.create_kwargs(),
            )
            created_profile_ids.append(record.profile_id)
        audit(
            session,
            "admin",
            "placement_profile.generate",
            release.id,
            "success",
            created_profile_ids=created_profile_ids,
        )
        session.commit()

    return {
        "release_id": release.id,
        "model_id": artifact.model_id,
        "apply": apply,
        "profiles": profile_results,
        "created_profile_ids": created_profile_ids,
    }


def transition_model_release(
    session: Session, release_id: str, target_status: str
) -> ModelRelease:
    if target_status == "ACTIVE":
        # ACTIVE is evidence-gated. Keep the existing transition API compatible
        # while routing it through the same promotion service as /promote.
        from .benchmark import promote_model_release

        release, _, _ = promote_model_release(session, release_id)
        return release
    release = session.scalar(
        select(ModelRelease).where(ModelRelease.id == release_id).with_for_update()
    )
    if release is None:
        raise ValueError("unknown model release")
    if target_status not in MODEL_RELEASE_TRANSITIONS:
        raise ValueError("unknown model release status")
    if target_status not in MODEL_RELEASE_TRANSITIONS[release.status]:
        raise ValueError(f"invalid model release transition: {release.status} -> {target_status}")
    if target_status in {"VALIDATED", "ACTIVE"}:
        placement = session.scalar(
            select(PlacementProfileRecord.id).where(
                PlacementProfileRecord.release_id == release.id,
                PlacementProfileRecord.status == "ACTIVE",
            )
        )
        if placement is None:
            raise ValueError("model release requires a placement profile")
    previous = release.status
    release.status = target_status
    release.updated_at = utcnow()
    audit(
        session,
        "admin",
        "model_release.transition",
        release.id,
        "success",
        previous=previous,
        current=target_status,
    )
    session.commit()
    return release


def create_enrollment(session: Session, expires_in: timedelta) -> tuple[EnrollmentToken, str]:
    raw = secrets.token_urlsafe(32)
    record = EnrollmentToken(token_hash=secret_hash(raw), expires_at=utcnow() + expires_in)
    session.add(record)
    audit(session, "admin", "enrollment.create", record.id, "success")
    session.commit()
    return record, raw


def claim_enrollment(
    session: Session, *, token: str, install_id: str, profile: dict, agent_version: str
) -> tuple[Node, str]:
    now = utcnow()
    record = session.scalar(
        select(EnrollmentToken).where(EnrollmentToken.token_hash == secret_hash(token)).with_for_update()
    )
    if record is None or record.used_at is not None or aware(record.expires_at) <= now:
        raise ValueError("invalid, expired, or already used enrollment token")
    parsed = NodeProfile.from_dict(profile)
    if session.scalar(select(Node).where(Node.install_id == install_id)) is not None:
        raise ValueError("installation is already enrolled")
    node = Node(
        install_id=install_id,
        display_name=parsed.hostname,
        hostname=parsed.hostname,
        agent_version=agent_version,
        approved=True,
        last_seen=now,
    )
    session.add(node)
    session.flush()
    session.add(NodeProfileRecord(node_id=node.id, profile=profile))
    raw_credential = secrets.token_urlsafe(48)
    session.add(NodeCredential(node_id=node.id, credential_hash=secret_hash(raw_credential)))
    record.used_at = now
    audit(session, f"node:{node.id}", "enrollment.claim", node.id, "success")
    session.commit()
    return node, raw_credential


def join_node(
    session: Session, *, install_id: str, profile: dict, agent_version: str
) -> tuple[Node, str]:
    """Register an unauthenticated node as pending operator approval."""
    parsed = NodeProfile.from_dict(profile)
    existing = session.scalar(select(Node).where(Node.install_id == install_id))
    if existing is not None:
        active_credential = session.scalar(
            select(NodeCredential.id).where(
                NodeCredential.node_id == existing.id,
                NodeCredential.revoked_at.is_(None),
            )
        )
        if active_credential is not None:
            raise ValueError("installation is already joined")
        existing.display_name = parsed.hostname
        existing.hostname = parsed.hostname
        existing.agent_version = agent_version
        existing.approved = False
        existing.last_seen = utcnow()
        existing.observed_phase = "DISCOVERED"
        existing.observed_role = None
        existing.observed_deployment_id = None
        existing.desired_state = None
        profile_record = session.get(NodeProfileRecord, existing.id)
        if profile_record is None:
            session.add(NodeProfileRecord(node_id=existing.id, profile=profile))
        else:
            profile_record.profile = profile
            profile_record.updated_at = utcnow()
        raw_credential = secrets.token_urlsafe(48)
        session.add(
            NodeCredential(
                node_id=existing.id,
                credential_hash=secret_hash(raw_credential),
            )
        )
        audit(session, f"node:{existing.id}", "node.rejoin", existing.id, "pending")
        session.commit()
        return existing, raw_credential
    node = Node(
        install_id=install_id,
        display_name=parsed.hostname,
        hostname=parsed.hostname,
        agent_version=agent_version,
        approved=False,
        last_seen=utcnow(),
        observed_phase="DISCOVERED",
    )
    session.add(node)
    session.flush()
    session.add(NodeProfileRecord(node_id=node.id, profile=profile))
    raw_credential = secrets.token_urlsafe(48)
    session.add(NodeCredential(node_id=node.id, credential_hash=secret_hash(raw_credential)))
    audit(session, f"node:{node.id}", "node.join", node.id, "pending")
    session.commit()
    return node, raw_credential


def authenticate_node(session: Session, credential: str) -> Node | None:
    digest = secret_hash(credential)
    row = session.execute(
        select(NodeCredential, Node)
        .join(Node, Node.id == NodeCredential.node_id)
        .where(NodeCredential.credential_hash == digest, NodeCredential.revoked_at.is_(None))
    ).first()
    return row[1] if row else None


def approve_node(session: Session, node_id: str) -> bool:
    node = session.get(Node, node_id)
    if node is None:
        return False
    node.approved = True
    audit(session, "admin", "node.approve", node_id, "success")
    session.commit()
    return True


def revoke_node(session: Session, node_id: str) -> bool:
    node = session.scalar(
        select(Node).where(Node.id == node_id).with_for_update()
    )
    if node is None:
        return False
    node.approved = False
    now = utcnow()
    for credential in session.scalars(
        select(NodeCredential).where(NodeCredential.node_id == node_id, NodeCredential.revoked_at.is_(None))
    ):
        credential.revoked_at = now
    from .preparation import revoke_preparation_tasks_for_node

    canceled_preparations = revoke_preparation_tasks_for_node(
        session, node_id
    )
    node.desired_state = None
    audit(
        session,
        "admin",
        "node.revoke",
        node_id,
        "success",
        canceled_preparation_tasks=canceled_preparations,
    )
    session.commit()
    return True


def _mark_node_unjoined(session: Session, node: Node, *, actor: str) -> None:
    node.approved = False
    node.observed_phase = "UNJOINED"
    node.observed_role = None
    node.observed_deployment_id = None
    node.desired_state = None
    now = utcnow()
    for credential in session.scalars(
        select(NodeCredential).where(
            NodeCredential.node_id == node.id,
            NodeCredential.revoked_at.is_(None),
        )
    ):
        credential.revoked_at = now
    from .preparation import revoke_preparation_tasks_for_node

    canceled_preparations = revoke_preparation_tasks_for_node(session, node.id)
    audit(
        session,
        actor,
        "node.unjoin",
        node.id,
        "success",
        canceled_preparation_tasks=canceled_preparations,
    )


def unjoin_node(session: Session, node_id: str) -> bool:
    try:
        node = session.scalar(
            select(Node).where(Node.id == node_id).with_for_update()
        )
        if node is None:
            return False
        _ensure_deployment_node_scope_available(session, [node_id])
        _mark_node_unjoined(session, node, actor=f"node:{node_id}")
        session.commit()
        return True
    except Exception:
        session.rollback()
        raise


def rotate_node_credential(session: Session, node_id: str) -> str | None:
    node = session.get(Node, node_id)
    if node is None:
        return None
    now = utcnow()
    for credential in session.scalars(
        select(NodeCredential).where(NodeCredential.node_id == node_id, NodeCredential.revoked_at.is_(None))
    ):
        credential.revoked_at = now
    raw = secrets.token_urlsafe(48)
    session.add(NodeCredential(node_id=node_id, credential_hash=secret_hash(raw)))
    node.approved = True
    audit(session, "admin", "node.credential.rotate", node_id, "success")
    session.commit()
    return raw


def node_status(last_seen: datetime | None, now: datetime | None = None) -> str:
    if last_seen is None:
        return "stale"
    age = (now or utcnow()) - aware(last_seen)
    if age <= timedelta(seconds=30):
        return "online"
    if age <= timedelta(seconds=90):
        return "offline"
    return "stale"


def _lock_deployment_creation_nodes(
    session: Session, node_ids: list[str]
) -> None:
    """Serialize deployment creation with cache quarantine decisions."""
    normalized = sorted(set(node_ids))
    locked = list(
        session.scalars(
            select(Node)
            .where(Node.id.in_(normalized))
            .order_by(Node.id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    )
    if [node.id for node in locked] != normalized:
        raise ValueError("deployment assignments contain an unknown node")
    quarantine = session.execute(
        select(Task.id, Task.node_id)
        .where(
            Task.node_id.in_(normalized),
            Task.type == TaskType.QUARANTINE_ARTIFACT_CACHE.value,
            Task.status.in_(
                {TaskStatus.QUEUED.value, TaskStatus.RUNNING.value}
            ),
        )
        .order_by(Task.created_at, Task.id)
        .limit(1)
    ).one_or_none()
    if quarantine is not None:
        raise ValueError(
            "deployment assignment has an active artifact cache quarantine: "
            f"{quarantine.node_id}"
        )


def save_heartbeat(
    session: Session,
    node: Node,
    state: dict,
    profile: dict | None = None,
    *,
    agent_version: str | None = None,
) -> None:
    observed_at = utcnow()
    locked_node = session.scalar(
        select(Node)
        .where(Node.id == node.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if locked_node is None:
        raise ValueError("node not found")
    node = locked_node
    node.last_seen = observed_at
    if agent_version is not None:
        node.agent_version = agent_version
    node.observed_phase = state.get("phase")
    node.observed_role = state.get("role")
    node.observed_deployment_id = state.get("deployment_id")
    if profile is not None:
        parsed_profile = NodeProfile.from_dict(profile)
        record = session.get(NodeProfileRecord, node.id)
        if record is None:
            session.add(NodeProfileRecord(node_id=node.id, profile=profile))
        else:
            record.profile = profile
            record.updated_at = observed_at
        if parsed_profile.artifact_cache_observations is not None:
            from .cache_lifecycle import reconcile_probe_observations

            reconcile_probe_observations(
                session,
                node_id=node.id,
                observations=parsed_profile.artifact_cache_observations,
                scan_complete=parsed_profile.artifact_cache_scan_complete,
                source_id=f"heartbeat:{uuid.uuid4()}",
                observed_at=observed_at,
            )
    session.commit()


def save_deployment(
    session: Session, plan_data: dict, *, accept_model_download: bool, pull_image: bool
) -> Deployment:
    plan = DeploymentPlan.from_dict(plan_data)
    strict_ray = plan.execution_backend == VLLM_RAY_PP_BACKEND
    if strict_ray:
        validate_strict_pipeline_plan(
            plan, require_manifest_cache_path=False
        )
        try:
            validate_digest_pinned_runtime_image(plan.image)
        except ValueError as exc:
            raise ValueError(
                "strict Ray deployments require an exact OCI digest-pinned image"
            ) from exc
    elif "@sha256:" not in plan.image:
        raise ValueError("central deployments require an OCI digest-pinned image")
    if not plan.assignments:
        raise ValueError("deployment has no assignments")
    # Local/legacy profiles identify nodes by hostname. Resolve those assignments
    # to stable server UUIDs when the hostname is unambiguous.
    for assignment in plan.assignments:
        if session.get(Node, assignment.node_id) is not None:
            continue
        if strict_ray:
            raise ValueError(
                "strict Ray deployment assignments require server-issued node UUIDs"
            )
        matches = list(session.scalars(select(Node).where(Node.hostname == assignment.node_id, Node.approved.is_(True))))
        if len(matches) != 1:
            raise ValueError(f"unknown or ambiguous node assignment: {assignment.node_id}")
        assignment.node_id = matches[0].id
    if plan.ray_head_node_id not in {item.node_id for item in plan.assignments}:
        if strict_ray:
            raise ValueError(
                "strict Ray deployment head requires a server-issued node UUID"
            )
        head = next((item for item in plan.assignments if item.role == "ray-head"), None)
        if head is None:
            raise ValueError("deployment has no Ray head assignment")
        plan.ray_head_node_id = head.node_id
    _lock_deployment_creation_nodes(
        session,
        [assignment.node_id for assignment in plan.assignments],
    )
    existing = session.get(Deployment, plan.deployment_id)
    if existing is not None:
        raise ValueError("deployment already exists")
    record = Deployment(
        id=plan.deployment_id,
        generation=plan.generation,
        plan=plan.to_dict(),
        accept_model_download=accept_model_download,
        pull_image=pull_image,
    )
    session.add(record)
    audit(session, "admin", "deployment.create", record.id, "success")
    session.commit()
    return record


def _canonical_uuid(value: str, *, field: str) -> str:
    try:
        if str(uuid.UUID(value)) != value:
            raise ValueError
    except (AttributeError, ValueError) as exc:
        raise ValueError(f"{field} must be a canonical UUID") from exc
    return value


def _canonical_digest(value: dict) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _benchmark_request(
    *,
    release_id: str,
    placement_id: str,
    node_ids: list[str],
    workload_id: str,
    dure_commit: str,
) -> dict:
    return {
        "release_id": release_id,
        "placement_id": placement_id,
        "node_ids": sorted(node_ids),
        "workload_id": workload_id,
        "dure_commit": dure_commit,
    }


def _benchmark_workload(artifact: ModelArtifact, workload_id: str) -> dict:
    presets = {
        "short-chat-1k-128": (1024, 128, 8),
        "long-chat-4k-256": (4096, 256, 4),
        "quality-eval": (1024, 256, 1),
    }
    if workload_id == "max-context":
        output_tokens = 256
        input_tokens = artifact.default_max_model_len - output_tokens
        concurrency = 1
    else:
        try:
            input_tokens, output_tokens, concurrency = presets[workload_id]
        except KeyError as exc:
            raise ValueError("unsupported benchmark workload_id") from exc
    if (
        input_tokens <= 0
        or input_tokens + output_tokens > artifact.default_max_model_len
        or input_tokens + output_tokens > MAX_BENCHMARK_CONTEXT_TOKENS
    ):
        raise BenchmarkRunError(
            "model artifact cannot satisfy the selected benchmark context length",
            code="WORKLOAD_CONTEXT_UNSUPPORTED",
            details={
                "workload_id": workload_id,
                "default_max_model_len": artifact.default_max_model_len,
            },
        )
    return {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "concurrency": concurrency,
        "warmup_requests": MIN_WARMUP_REQUESTS,
        "request_count": MIN_MEASURED_REQUESTS,
        "duration_seconds": float(MIN_MEASURED_SECONDS),
    }


def _benchmark_registry(
    session: Session, release_id: str
) -> tuple[ModelRelease, ModelArtifact, RuntimeRelease]:
    release = session.get(ModelRelease, release_id)
    if release is None:
        raise BenchmarkNotFoundError("model release not found")
    artifact = session.get(ModelArtifact, release.artifact_id)
    runtime = session.get(RuntimeRelease, release.runtime_id)
    if artifact is None or runtime is None:
        raise BenchmarkIdentityMismatchError(
            "model release registry binding is incomplete"
        )
    return release, artifact, runtime


def _require_single_node_benchmark(
    session: Session,
    *,
    release_id: str,
    placement_id: str,
    node_ids: list[str],
) -> PlacementProfileRecord:
    placement = session.get(PlacementProfileRecord, placement_id)
    if placement is None or placement.release_id != release_id:
        raise BenchmarkNotFoundError(
            "placement profile not found for model release"
        )
    if (
        placement.topology != "single-gpu"
        or placement.node_count != 1
        or placement.pipeline_parallel_size != 1
        or placement.tensor_parallel_size != 1
        or len(node_ids) != 1
    ):
        raise BenchmarkRunError(
            "automatic benchmark execution supports only a single-GPU placement",
            code="MULTI_NODE_BENCHMARK_UNSUPPORTED",
            details={
                "placement_topology": placement.topology,
                "placement_node_count": placement.node_count,
                "requested_node_count": len(node_ids),
            },
        )
    return placement


def benchmark_run_dict(run: BenchmarkRun) -> dict:
    return {
        key: getattr(run, key)
        for key in (
            "id",
            "request_id",
            "request_digest",
            "release_id",
            "placement_id",
            "coordinator_node_id",
            "node_ids",
            "inventory_fingerprint",
            "suite_id",
            "policy_version",
            "workload_id",
            "dure_commit",
            "model_id",
            "repository",
            "artifact_revision",
            "artifact_manifest_digest",
            "quantization",
            "runtime_image",
            "input_tokens",
            "output_tokens",
            "concurrency",
            "warmup_requests",
            "request_count",
            "duration_seconds",
            "status",
            "task_id",
            "evidence_id",
            "failure_code",
            "created_at",
            "updated_at",
        )
    }


def get_benchmark_run(session: Session, request_id: str) -> BenchmarkRun:
    run = session.scalar(
        select(BenchmarkRun).where(BenchmarkRun.request_id == request_id)
    )
    if run is None:
        raise BenchmarkRunNotFoundError("benchmark run not found")
    return run


def manifest_for_benchmark_task(
    session: Session,
    task_id: str,
    node_id: str,
) -> dict:
    """Return only the immutable manifest bound to one active benchmark lease."""

    node = session.scalar(
        select(Node).where(Node.id == node_id).with_for_update()
    )
    task = session.scalar(
        select(Task).where(Task.id == task_id).with_for_update()
    )
    if node is None or not node.approved or task is None:
        raise BenchmarkRunError(
            "benchmark artifact manifest is unavailable",
            code="BENCHMARK_CONTEXT_CHANGED",
        )
    lease_until = aware(task.lease_until)
    run = session.scalar(
        select(BenchmarkRun)
        .where(BenchmarkRun.task_id == task.id)
        .with_for_update()
    )
    if (
        task.type != TaskType.BENCHMARK.value
        or task.node_id != node_id
        or task.status != TaskStatus.RUNNING.value
        or lease_until is None
        or lease_until < utcnow()
        or run is None
        or run.status != "QUEUED"
        or run.coordinator_node_id != node_id
        or task.payload.get("benchmark_id") != run.id
        or task.payload.get("artifact_manifest_digest")
        != run.artifact_manifest_digest
        or task.payload.get("prepare_model") is not True
    ):
        raise BenchmarkRunError(
            "benchmark artifact manifest is unavailable",
            code="BENCHMARK_CONTEXT_CHANGED",
        )
    manifest = session.get(ArtifactManifest, run.artifact_manifest_digest)
    if manifest is None:
        raise BenchmarkRunError(
            "benchmark artifact manifest is unavailable",
            code="BENCHMARK_CONTEXT_CHANGED",
        )
    try:
        value = artifact_manifest_dict(session, manifest)
    except ValueError as exc:
        raise BenchmarkRunError(
            "benchmark artifact manifest is unavailable",
            code="BENCHMARK_CONTEXT_CHANGED",
        ) from exc
    return {
        "schema_version": value["schema_version"],
        "files": value["files"],
    }


def prepare_benchmark_run(
    session: Session,
    *,
    request_id: str,
    release_id: str,
    placement_id: str,
    node_ids: list[str],
    workload_id: str,
    dure_commit: str,
) -> tuple[BenchmarkRun, bool]:
    _canonical_uuid(request_id, field="request_id")
    _canonical_uuid(release_id, field="release_id")
    _canonical_uuid(placement_id, field="placement_id")
    if len(node_ids) != len(set(node_ids)):
        raise ValueError("benchmark node_ids must not contain duplicates")
    for node_id in node_ids:
        _canonical_uuid(node_id, field="node_id")
    if workload_id not in BENCHMARK_WORKLOAD_IDS:
        raise ValueError("unsupported benchmark workload_id")
    if re.fullmatch(r"[0-9a-f]{40,64}", dure_commit) is None:
        raise ValueError("dure_commit must be an immutable commit hash")
    request = _benchmark_request(
        release_id=release_id,
        placement_id=placement_id,
        node_ids=node_ids,
        workload_id=workload_id,
        dure_commit=dure_commit,
    )
    request_digest = _canonical_digest(request)
    existing = session.scalar(
        select(BenchmarkRun).where(BenchmarkRun.request_id == request_id)
    )
    if existing is not None:
        if existing.request_digest == request_digest:
            return existing, False
        raise BenchmarkRunError(
            "request_id is already bound to a different benchmark request",
            code="BENCHMARK_REQUEST_CONFLICT",
            details={"request_id": request_id},
        )

    normalized_node_ids = request["node_ids"]
    _require_single_node_benchmark(
        session,
        release_id=release_id,
        placement_id=placement_id,
        node_ids=normalized_node_ids,
    )
    context = benchmark_context(
        session,
        release_id=release_id,
        placement_id=placement_id,
        node_ids=normalized_node_ids,
    )
    _, artifact, runtime = _benchmark_registry(session, release_id)
    workload = _benchmark_workload(artifact, workload_id)
    run = BenchmarkRun(
        request_id=request_id,
        request_digest=request_digest,
        release_id=release_id,
        placement_id=placement_id,
        coordinator_node_id=normalized_node_ids[0],
        node_ids=normalized_node_ids,
        inventory_fingerprint=context["inventory_fingerprint"],
        suite_id=context["suite_id"],
        policy_version=context["policy_version"],
        workload_id=workload_id,
        dure_commit=dure_commit,
        model_id=artifact.model_id,
        repository=artifact.repository,
        artifact_revision=artifact.revision,
        artifact_manifest_digest=artifact.manifest_digest,
        quantization=artifact.quantization,
        runtime_image=runtime.image,
        status="PREPARED",
        **workload,
    )
    session.add(run)
    try:
        session.flush()
    except IntegrityError as exc:
        session.rollback()
        existing = session.scalar(
            select(BenchmarkRun).where(BenchmarkRun.request_id == request_id)
        )
        if existing is not None and existing.request_digest == request_digest:
            return existing, False
        raise BenchmarkRunError(
            "request_id is already bound to a different benchmark request",
            code="BENCHMARK_REQUEST_CONFLICT",
            details={"request_id": request_id},
        ) from exc
    audit(
        session,
        "admin",
        "benchmark_run.prepare",
        run.id,
        "success",
        request_id=request_id,
        request_digest=request_digest,
    )
    session.commit()
    return run, True


def _benchmark_task_payload(
    run: BenchmarkRun,
    *,
    prepare_model: bool,
    pull_image: bool,
) -> dict:
    payload = {
        "benchmark_id": run.id,
        "release_id": run.release_id,
        "placement_id": run.placement_id,
        "suite_id": run.suite_id,
        "policy_version": run.policy_version,
        "dure_commit": run.dure_commit,
        "model_id": run.model_id,
        "model_repository": run.repository,
        "artifact_revision": run.artifact_revision,
        "artifact_manifest_digest": run.artifact_manifest_digest,
        "quantization": run.quantization,
        "runtime_image": run.runtime_image,
        "coordinator_node_id": run.coordinator_node_id,
        "node_ids": list(run.node_ids),
        "inventory_fingerprint": run.inventory_fingerprint,
        "workload_id": run.workload_id,
        "input_tokens": run.input_tokens,
        "output_tokens": run.output_tokens,
        "concurrency": run.concurrency,
        "warmup_requests": run.warmup_requests,
        "request_count": run.request_count,
        "duration_seconds": run.duration_seconds,
        "apply": True,
    }
    if prepare_model:
        payload["prepare_model"] = True
    if pull_image:
        payload["pull_image"] = True
    # Keep the producer and node-agent consumer on one exact, closed schema.
    BenchmarkTaskPayload.from_dict(payload)
    return payload


def apply_benchmark_run(
    session: Session,
    request_id: str,
    *,
    prepare_model: bool = False,
    pull_image: bool = False,
) -> tuple[BenchmarkRun, Task, bool]:
    if type(prepare_model) is not bool or type(pull_image) is not bool:
        raise ValueError("benchmark preparation approvals must be booleans")
    identity = session.execute(
        select(
            BenchmarkRun.release_id,
            BenchmarkRun.coordinator_node_id,
        ).where(BenchmarkRun.request_id == request_id)
    ).one_or_none()
    if identity is None:
        raise BenchmarkRunNotFoundError("benchmark run not found")
    locked_release = session.scalar(
        select(ModelRelease)
        .where(ModelRelease.id == identity.release_id)
        .with_for_update()
    )
    locked_node = session.scalar(
        select(Node)
        .where(Node.id == identity.coordinator_node_id)
        .with_for_update()
    )
    run = session.scalar(
        select(BenchmarkRun)
        .where(BenchmarkRun.request_id == request_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if run is None:
        raise BenchmarkRunNotFoundError("benchmark run not found")
    if (
        locked_release is None
        or locked_node is None
        or run.release_id != identity.release_id
        or run.coordinator_node_id != identity.coordinator_node_id
    ):
        raise BenchmarkRunError(
            "prepared benchmark context is no longer available",
            code="BENCHMARK_CONTEXT_CHANGED",
        )
    if run.status != "PREPARED":
        task = session.get(Task, run.task_id) if run.task_id else None
        if task is None:
            raise BenchmarkRunError(
                "benchmark run has no corresponding task",
                code="BENCHMARK_TASK_STATE_INVALID",
            )
        if (
            task.payload.get("prepare_model", False) is not prepare_model
            or task.payload.get("pull_image", False) is not pull_image
        ):
            raise BenchmarkRunError(
                "benchmark apply approvals do not match the existing task",
                code="BENCHMARK_REQUEST_CONFLICT",
                details={"request_id": request_id},
            )
        return run, task, False

    if (prepare_model or pull_image) and not _agent_supports_benchmark_preparation(
        locked_node.agent_version
    ):
        raise BenchmarkRunError(
            "benchmark preparation requires a newer node Agent",
            code="BENCHMARK_CONTEXT_CHANGED",
            details={
                "node_id": locked_node.id,
                "required_agent_version": "0.3.25",
                "actual_agent_version": locked_node.agent_version,
            },
        )

    active_task = session.scalar(
        select(Task)
        .where(
            Task.node_id == locked_node.id,
            Task.type.in_(NODE_EXCLUSIVE_TASK_TYPE_VALUES),
            Task.status.in_(
                {TaskStatus.QUEUED.value, TaskStatus.RUNNING.value}
            ),
        )
        .order_by(Task.created_at, Task.id)
        .limit(1)
    )
    if active_task is not None:
        raise BenchmarkRunError(
            "benchmark coordinator node already has active work",
            code="BENCHMARK_NODE_BUSY",
            details={
                "node_id": locked_node.id,
                "task_id": active_task.id,
                "task_type": active_task.type,
            },
        )
    active_qualification = active_profile_qualification_nodes(
        session, [locked_node.id]
    ).get(locked_node.id)
    if active_qualification is not None:
        raise BenchmarkRunError(
            "benchmark coordinator node belongs to an active profile qualification",
            code="BENCHMARK_NODE_BUSY",
            details={
                "node_id": locked_node.id,
                "qualification_run_id": active_qualification,
            },
        )
    for operation in session.scalars(
        select(DeploymentOperation).where(
            DeploymentOperation.active_lineage_id.is_not(None)
        )
    ):
        if locked_node.id in operation.node_ids:
            raise BenchmarkRunError(
                "benchmark coordinator node belongs to an active deployment operation",
                code="BENCHMARK_NODE_BUSY",
                details={
                    "node_id": locked_node.id,
                    "operation_id": operation.id,
                },
            )

    _require_single_node_benchmark(
        session,
        release_id=run.release_id,
        placement_id=run.placement_id,
        node_ids=list(run.node_ids),
    )
    try:
        context = benchmark_context(
            session,
            release_id=run.release_id,
            placement_id=run.placement_id,
            node_ids=list(run.node_ids),
        )
        _, artifact, runtime = _benchmark_registry(session, run.release_id)
        workload = _benchmark_workload(artifact, run.workload_id)
    except (
        BenchmarkIdentityMismatchError,
        BenchmarkNotFoundError,
        BenchmarkPromotionError,
        ValueError,
    ) as exc:
        raise BenchmarkRunError(
            "prepared benchmark context is no longer eligible",
            code="BENCHMARK_CONTEXT_CHANGED",
        ) from exc
    current = {
        "inventory_fingerprint": context["inventory_fingerprint"],
        "suite_id": context["suite_id"],
        "policy_version": context["policy_version"],
        "model_id": artifact.model_id,
        "repository": artifact.repository,
        "artifact_revision": artifact.revision,
        "artifact_manifest_digest": artifact.manifest_digest,
        "quantization": artifact.quantization,
        "runtime_image": runtime.image,
        **workload,
    }
    changed = sorted(
        key for key, value in current.items() if getattr(run, key) != value
    )
    if changed:
        raise BenchmarkRunError(
            "prepared benchmark context has changed",
            code="BENCHMARK_CONTEXT_CHANGED",
            details={"changed_fields": changed},
        )

    try:
        payload = _benchmark_task_payload(
            run,
            prepare_model=prepare_model,
            pull_image=pull_image,
        )
    except ValueError as exc:  # pragma: no cover - persisted runs use this schema
        raise BenchmarkRunError(
            "prepared benchmark payload no longer matches the closed schema",
            code="BENCHMARK_CONTEXT_CHANGED",
        ) from exc
    task = Task(
        bulk_id=run.request_id,
        node_id=run.coordinator_node_id,
        type=TaskType.BENCHMARK.value,
        deployment_id=None,
        payload=payload,
    )
    session.add(task)
    session.flush()
    run.task_id = task.id
    run.status = "QUEUED"
    run.updated_at = utcnow()
    if not locked_node.approved:  # pragma: no cover - context locked it
        session.rollback()
        raise BenchmarkRunError(
            "prepared benchmark coordinator is no longer approved",
            code="BENCHMARK_CONTEXT_CHANGED",
        )
    locked_node.desired_state = TaskType.BENCHMARK.value
    audit(
        session,
        "admin",
        "benchmark_run.apply",
        run.id,
        "success",
        task_id=task.id,
    )
    session.commit()
    return run, task, True


def _number(value, *, field: str, minimum: float | None = None, maximum: float | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"benchmark metric {field} must be numeric")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError(f"benchmark metric {field} must be finite")
    if minimum is not None and normalized < minimum:
        raise ValueError(f"benchmark metric {field} is out of range")
    if maximum is not None and normalized > maximum:
        raise ValueError(f"benchmark metric {field} is out of range")
    return normalized


def _integer(value, *, field: str, minimum: int) -> int:
    if (
        type(value) is not int
        or value < minimum
        or value > MAX_BENCHMARK_INTEGER
    ):
        raise ValueError(f"benchmark metric {field} must be an integer in range")
    return value


def validate_benchmark_task_result(run: BenchmarkRun, result: dict) -> dict:
    if not isinstance(result, dict):
        raise ValueError("BENCHMARK result must be an object")
    expected_outer = {"benchmark_id", "workload_id", "metrics"}
    if set(result) != expected_outer:
        unexpected = sorted(set(result) - expected_outer)
        missing = sorted(expected_outer - set(result))
        detail = unexpected or missing
        raise ValueError(
            "BENCHMARK result fields do not match the closed schema: "
            + ", ".join(detail)
        )
    if result["benchmark_id"] != run.id:
        raise ValueError("BENCHMARK result benchmark_id does not match the run")
    if result["workload_id"] != run.workload_id:
        raise ValueError("BENCHMARK result workload_id does not match the run")
    metrics = result["metrics"]
    if not isinstance(metrics, dict):
        raise ValueError("BENCHMARK result metrics must be an object")
    if set(metrics) != BENCHMARK_RESULT_METRIC_FIELDS:
        unexpected = sorted(set(metrics) - BENCHMARK_RESULT_METRIC_FIELDS)
        missing = sorted(BENCHMARK_RESULT_METRIC_FIELDS - set(metrics))
        detail = unexpected or missing
        raise ValueError(
            "BENCHMARK metric fields do not match the closed schema: "
            + ", ".join(detail)
        )

    normalized = {
        "duration_seconds": _number(
            metrics["duration_seconds"], field="duration_seconds", minimum=0.000001
        ),
        "request_count": _integer(
            metrics["request_count"], field="request_count", minimum=1
        ),
        "warmup_requests": _integer(
            metrics["warmup_requests"], field="warmup_requests", minimum=0
        ),
        "oom_count": _integer(metrics["oom_count"], field="oom_count", minimum=0),
        "crash_count": _integer(
            metrics["crash_count"], field="crash_count", minimum=0
        ),
        "restart_count": _integer(
            metrics["restart_count"], field="restart_count", minimum=0
        ),
        "success_rate": _number(
            metrics["success_rate"], field="success_rate", minimum=0, maximum=1
        ),
        "vram_headroom_pct": _number(
            metrics["vram_headroom_pct"],
            field="vram_headroom_pct",
            minimum=0,
            maximum=100,
        ),
        "quality_score": _number(
            metrics["quality_score"], field="quality_score", minimum=0, maximum=1
        ),
    }
    if (
        normalized["request_count"] != run.request_count
        or normalized["warmup_requests"] != run.warmup_requests
    ):
        raise ValueError(
            "BENCHMARK result measurement counts do not match the prepared run"
        )
    for field in (
        "ttft_p95_ms",
        "tpot_p95_ms",
        "e2e_p95_ms",
        "throughput_tps",
    ):
        value = metrics[field]
        normalized[field] = (
            None if value is None else _number(value, field=field, minimum=0.000001)
        )
    for field in (
        "network_bandwidth_mbps",
        "network_rtt_ms",
        "packet_loss_pct",
        "nccl_all_reduce_ok",
    ):
        if metrics[field] is not None:
            raise ValueError(
                f"single-node BENCHMARK metric {field} must be null"
            )
        normalized[field] = None
    return {
        "benchmark_id": run.id,
        "workload_id": run.workload_id,
        "metrics": normalized,
    }


def _fail_benchmark_record(
    session: Session,
    *,
    task: Task,
    run: BenchmarkRun,
    node_id: str,
    failure_code: str,
) -> None:
    task.status = TaskStatus.FAILED.value
    task.result = None
    task.error = failure_code
    task.lease_until = None
    run.status = "FAILED"
    run.failure_code = failure_code
    run.updated_at = utcnow()
    node = session.get(Node, node_id)
    if node is not None:
        node.desired_state = None
    audit(
        session,
        f"node:{node_id}",
        "benchmark_run.fail",
        run.id,
        "failed",
        task_id=task.id,
        failure_code=failure_code,
    )
    session.commit()


def _lock_benchmark_terminal(
    session: Session, *, task_id: str, node_id: str
) -> tuple[Node | None, Task | None, BenchmarkRun | None]:
    release_id = session.scalar(
        select(BenchmarkRun.release_id).where(BenchmarkRun.task_id == task_id)
    )
    if release_id is None:
        return None, None, None
    release = session.scalar(
        select(ModelRelease)
        .where(ModelRelease.id == release_id)
        .with_for_update()
    )
    node = session.scalar(
        select(Node).where(Node.id == node_id).with_for_update()
    )
    if release is None:
        return node, None, None
    task = session.scalar(
        select(Task)
        .where(Task.id == task_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    run = session.scalar(
        select(BenchmarkRun)
        .where(BenchmarkRun.task_id == task_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return node, task, run


def complete_benchmark_task(
    session: Session, task: Task, node_id: str, result: dict
) -> tuple[bool, BenchmarkRun | None]:
    if task.type != TaskType.BENCHMARK.value or task.node_id != node_id:
        return False, None
    task_id = task.id
    _, task, run = _lock_benchmark_terminal(
        session, task_id=task_id, node_id=node_id
    )
    if (
        task is None
        or run is None
        or task.type != TaskType.BENCHMARK.value
        or task.node_id != node_id
        or run.coordinator_node_id != node_id
    ):
        return False, None
    normalized = validate_benchmark_task_result(run, result)
    result_digest = _canonical_digest(normalized)
    if task.status == TaskStatus.SUCCEEDED.value:
        if (
            not isinstance(task.result, dict)
            or task.result.get("result_digest") != result_digest
            or task.result.get("evidence_id") != run.evidence_id
            or run.status != "SUCCEEDED"
        ):
            raise BenchmarkRunError(
                "completed benchmark task result cannot be replaced",
                code="BENCHMARK_RESULT_CONFLICT",
            )
        return True, run
    if task.status != TaskStatus.RUNNING.value or run.status != "QUEUED":
        return False, run
    metrics = normalized["metrics"]
    try:
        evidence = register_benchmark_evidence(
            session,
            release_id=run.release_id,
            placement_id=run.placement_id,
            suite_id=run.suite_id,
            node_ids=list(run.node_ids),
            inventory_fingerprint=run.inventory_fingerprint,
            artifact_revision=run.artifact_revision,
            artifact_manifest_digest=run.artifact_manifest_digest,
            runtime_image=run.runtime_image,
            dure_commit=run.dure_commit,
            policy_version=run.policy_version,
            input_tokens=run.input_tokens,
            output_tokens=run.output_tokens,
            concurrency=run.concurrency,
            benchmark_run_id=run.id,
            actor=f"node:{node_id}",
            commit=False,
            **metrics,
        )
    except (
        BenchmarkIdentityMismatchError,
        BenchmarkNotFoundError,
        BenchmarkPromotionError,
        ValueError,
    ):
        session.rollback()
        _, task, run = _lock_benchmark_terminal(
            session, task_id=task_id, node_id=node_id
        )
        if (
            task is not None
            and run is not None
            and task.status == TaskStatus.SUCCEEDED.value
            and run.status == "SUCCEEDED"
        ):
            if (
                isinstance(task.result, dict)
                and task.result.get("result_digest") == result_digest
            ):
                return True, run
            raise BenchmarkRunError(
                "completed benchmark task result cannot be replaced",
                code="BENCHMARK_RESULT_CONFLICT",
            )
        if (
            task is not None
            and run is not None
            and task.status == TaskStatus.RUNNING.value
            and run.status == "QUEUED"
        ):
            _fail_benchmark_record(
                session,
                task=task,
                run=run,
                node_id=node_id,
                failure_code="BENCHMARK_EVIDENCE_REJECTED",
            )
            return True, run
        return False, run

    _, task, run = _lock_benchmark_terminal(
        session, task_id=task_id, node_id=node_id
    )
    if task is None or run is None:  # pragma: no cover - foreign keys preserve both
        return False, None
    task.status = TaskStatus.SUCCEEDED.value
    task.result = {
        "benchmark_id": run.id,
        "workload_id": run.workload_id,
        "evidence_id": evidence.id,
        "result_digest": result_digest,
    }
    task.error = None
    task.lease_until = None
    run.status = "SUCCEEDED"
    run.evidence_id = evidence.id
    run.failure_code = None
    run.updated_at = utcnow()
    node = session.get(Node, node_id)
    if node is not None:
        node.desired_state = None
    audit(
        session,
        f"node:{node_id}",
        "benchmark_run.complete",
        run.id,
        "success",
        task_id=task.id,
        evidence_id=evidence.id,
        evidence_status=evidence.status,
    )
    session.commit()
    return True, run


def fail_benchmark_task(
    session: Session, task: Task, node_id: str, failure_code: str
) -> tuple[bool, BenchmarkRun | None]:
    if (
        task.type != TaskType.BENCHMARK.value
        or task.node_id != node_id
        or failure_code not in BENCHMARK_TASK_FAILURE_CODES
        or failure_code == "BENCHMARK_CANCELED"
    ):
        return False, None
    _, task, run = _lock_benchmark_terminal(
        session, task_id=task.id, node_id=node_id
    )
    if (
        task is None
        or run is None
        or task.type != TaskType.BENCHMARK.value
        or task.node_id != node_id
        or run.coordinator_node_id != node_id
    ):
        return False, None
    if task.status == TaskStatus.FAILED.value:
        if task.error != failure_code or run.failure_code != failure_code:
            raise BenchmarkRunError(
                "failed benchmark task outcome cannot be replaced",
                code="BENCHMARK_RESULT_CONFLICT",
            )
        return True, run
    if task.status != TaskStatus.RUNNING.value or run.status != "QUEUED":
        return False, run
    _fail_benchmark_record(
        session,
        task=task,
        run=run,
        node_id=node_id,
        failure_code=failure_code,
    )
    return True, run


DEPLOYMENT_TASK_TYPES = {
    TaskType.VERIFY,
    TaskType.APPLY_DEPLOYMENT,
    TaskType.START_DEPLOYMENT,
    TaskType.STOP_DEPLOYMENT,
    TaskType.RESTART_DEPLOYMENT,
}
DEPLOYMENT_MUTATION_TASK_TYPES = {
    TaskType.APPLY_DEPLOYMENT.value,
    TaskType.START_DEPLOYMENT.value,
    TaskType.STOP_DEPLOYMENT.value,
    TaskType.RESTART_DEPLOYMENT.value,
    TaskType.PREPARE_MODEL.value,
    TaskType.PREPARE_IMAGE.value,
}
DEPLOYMENT_TASK_TYPE_VALUES = {item.value for item in DEPLOYMENT_TASK_TYPES}
NODE_EXCLUSIVE_TASK_TYPE_VALUES = DEPLOYMENT_TASK_TYPE_VALUES | {
    TaskType.BENCHMARK.value,
    TaskType.PREPARE_MODEL.value,
    TaskType.PREPARE_IMAGE.value,
    TaskType.QUARANTINE_ARTIFACT_CACHE.value,
    TaskType.UNJOIN_NODE.value,
}


def _validate_task_options(task_type: TaskType, options: dict) -> None:
    if type(options) is not dict or any(type(key) is not str for key in options):
        raise ValueError("task options must be an object with string keys")
    allowed_options = (
        {"api"}
        if task_type == TaskType.VERIFY
        else {"serve"}
        if task_type
        in {
            TaskType.APPLY_DEPLOYMENT,
            TaskType.START_DEPLOYMENT,
            TaskType.RESTART_DEPLOYMENT,
        }
        else set()
    )
    unknown = set(options) - allowed_options
    if unknown:
        raise ValueError(f"unsupported task options: {', '.join(sorted(unknown))}")
    if any(type(value) is not bool for value in options.values()):
        raise ValueError("task options must use strict boolean values")


def _lock_deployment_lineage_for_task_creation(
    session: Session, deployment: Deployment
) -> None:
    """Serialize deployment task creation on the lineage root.

    Callers first lock their complete node set in UUID order, then this lineage
    root. That order prevents overlapping lineages from racing onto one GPU.
    """
    lineage_id = deployment.lineage_id or deployment.id
    root = session.scalar(
        select(Deployment)
        .where(Deployment.id == lineage_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if root is None or (root.lineage_id or root.id) != lineage_id:
        raise DeploymentRolloutConflictError(
            "deployment lineage root is invalid",
            code="DEPLOYMENT_LINEAGE_INVALID",
            details={"lineage_id": lineage_id},
        )
    active_operation_id = session.scalar(
        select(DeploymentOperation.id)
        .where(DeploymentOperation.active_lineage_id == lineage_id)
    )
    if active_operation_id is not None:
        raise DeploymentRolloutConflictError(
            "deployment lineage already has an active operation",
            code="DEPLOYMENT_OPERATION_ACTIVE",
            details={"operation_id": active_operation_id},
        )
    active_task_id = session.scalar(
        select(Task.id)
        .join(Deployment, Deployment.id == Task.deployment_id)
        .where(
            Deployment.lineage_id == lineage_id,
            Task.type.in_(DEPLOYMENT_MUTATION_TASK_TYPES),
            Task.status.in_(
                {
                    TaskStatus.QUEUED.value,
                    TaskStatus.RUNNING.value,
                }
            ),
        )
        .order_by(Task.created_at, Task.id)
        .limit(1)
    )
    if active_task_id is not None:
        raise DeploymentRolloutConflictError(
            "deployment lineage already has a queued or running mutation",
            code="DEPLOYMENT_MUTATION_ACTIVE",
            details={"task_id": active_task_id},
        )


def _lock_deployment_task_nodes(
    session: Session, node_ids: list[str]
) -> dict[str, Node]:
    normalized = sorted(set(node_ids))
    if not normalized:
        return {}
    nodes = list(
        session.scalars(
            select(Node)
            .where(Node.id.in_(normalized))
            .order_by(Node.id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    )
    return {node.id: node for node in nodes}


def _ensure_deployment_node_scope_available(
    session: Session, node_ids: list[str]
) -> None:
    requested = set(node_ids)
    if not requested:
        return
    for operation in session.scalars(
        select(DeploymentOperation).where(
            DeploymentOperation.active_lineage_id.is_not(None)
        )
    ):
        overlap = sorted(requested.intersection(operation.node_ids))
        if overlap:
            raise DeploymentRolloutConflictError(
                "assigned nodes already belong to an active operation",
                code="DEPLOYMENT_NODE_OPERATION_ACTIVE",
                details={
                    "operation_id": operation.id,
                    "node_ids": overlap,
                },
            )
    active_task = session.execute(
        select(Task.id, Task.node_id)
        .where(
            Task.node_id.in_(requested),
            Task.type.in_(NODE_EXCLUSIVE_TASK_TYPE_VALUES),
            Task.status.in_(
                {TaskStatus.QUEUED.value, TaskStatus.RUNNING.value}
            ),
        )
        .order_by(Task.created_at, Task.id)
        .limit(1)
    ).one_or_none()
    if active_task is not None:
        raise DeploymentRolloutConflictError(
            "assigned node already has a queued or running deployment task",
            code="DEPLOYMENT_NODE_TASK_ACTIVE",
            details={
                "task_id": active_task.id,
                "node_id": active_task.node_id,
            },
        )
    active_qualifications = active_profile_qualification_nodes(
        session, requested
    )
    if active_qualifications:
        overlap = sorted(active_qualifications)
        raise DeploymentRolloutConflictError(
            "assigned nodes belong to active profile qualification runs",
            code="DEPLOYMENT_NODE_QUALIFICATION_ACTIVE",
            details={
                "node_ids": overlap,
                "qualification_run_ids": sorted(
                    {active_qualifications[node_id] for node_id in overlap}
                ),
            },
        )


def _operation_hook_conflict(action: str, task_id: str) -> DeploymentRolloutConflictError:
    return DeploymentRolloutConflictError(
        f"operation-bound task cannot be {action}",
        code="DEPLOYMENT_OPERATION_TASK_CONFLICT",
        details={"task_id": task_id},
    )


def create_tasks(
    session: Session,
    *,
    node_ids: list[str],
    task_type: TaskType,
    deployment_id: str | None,
    options: dict,
) -> tuple[str, list[Task], dict[str, str]]:
    if task_type == TaskType.BENCHMARK:
        raise ValueError(
            "BENCHMARK tasks require a prepared benchmark run and explicit apply"
        )
    if task_type in {TaskType.PREPARE_MODEL, TaskType.PREPARE_IMAGE}:
        raise ValueError(
            "artifact preparation tasks require the dedicated deployment prepare API"
        )
    if task_type == TaskType.QUARANTINE_ARTIFACT_CACHE:
        raise ValueError(
            "artifact cache quarantine tasks require the dedicated cache API"
        )
    _validate_task_options(task_type, options)
    try:
        bulk_id = str(uuid.uuid4())
        tasks: list[Task] = []
        errors: dict[str, str] = {}
        locked_nodes = _lock_deployment_task_nodes(session, node_ids)
        deployment = (
            session.get(Deployment, deployment_id) if deployment_id else None
        )
        if task_type in DEPLOYMENT_TASK_TYPES and deployment is None:
            raise ValueError("a valid deployment_id is required")
        if deployment is not None and task_type in DEPLOYMENT_TASK_TYPES:
            _lock_deployment_lineage_for_task_creation(session, deployment)
            _ensure_deployment_node_scope_available(session, node_ids)
        elif task_type == TaskType.UNJOIN_NODE:
            _ensure_deployment_node_scope_available(session, node_ids)
        effective_plan = deployment.plan if deployment is not None else None
        if deployment is not None and deployment.source_recommendation_id is not None:
            from .preparation import effective_deployment_plan

            effective_plan = effective_deployment_plan(
                session,
                deployment,
                require_prepared=task_type != TaskType.STOP_DEPLOYMENT,
            )
        assignments = (
            {item["node_id"] for item in effective_plan["assignments"]}
            if effective_plan
            else set()
        )
        strict_ray = False
        stage_artifact = False
        if type(effective_plan) is dict and "execution_backend" in effective_plan:
            if effective_plan.get("execution_backend") != VLLM_RAY_PP_BACKEND:
                raise DeploymentRolloutConflictError(
                    "deployment execution backend is not supported",
                    code="DEPLOYMENT_PLAN_INVALID",
                )
            try:
                strict_plan = DeploymentPlan.from_dict(effective_plan)
                validate_strict_pipeline_plan(
                    strict_plan,
                    require_manifest_cache_path=(
                        task_type != TaskType.STOP_DEPLOYMENT
                    ),
                    validate_model_path=(
                        task_type != TaskType.STOP_DEPLOYMENT
                    ),
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise DeploymentRolloutConflictError(
                    "strict Ray deployment contract is invalid",
                    code="DEPLOYMENT_PLAN_INVALID",
                ) from exc
            strict_ray = True
            stage_artifact = effective_plan.get("model_cache_kind") == "STAGE"
        if strict_ray:
            requested = set(dict.fromkeys(node_ids))
            if (
                task_type
                in {
                    TaskType.APPLY_DEPLOYMENT,
                    TaskType.START_DEPLOYMENT,
                    TaskType.RESTART_DEPLOYMENT,
                }
                and options.get("serve") is not True
            ) or (
                task_type == TaskType.VERIFY
                and options.get("api") is not True
            ):
                raise DeploymentRolloutConflictError(
                    "strict Ray deployment operations require live vLLM API verification",
                    code="DEPLOYMENT_STRICT_RUNTIME_ATTESTATION_REQUIRED",
                )
            if task_type != TaskType.STOP_DEPLOYMENT and (
                requested != assignments or len(node_ids) != len(requested)
            ):
                raise DeploymentRolloutConflictError(
                    "strict Ray deployment operation requires the complete assigned node set",
                    code="DEPLOYMENT_STRICT_NODE_SET_MISMATCH",
                    details={
                        "expected_node_ids": sorted(assignments),
                        "requested_node_ids": sorted(requested),
                    },
                )
            unavailable = sorted(
                node_id
                for node_id in requested
                if (node := locked_nodes.get(node_id)) is None
                or not node.approved
                or node_status(node.last_seen, utcnow()) != "online"
            )
            if task_type != TaskType.STOP_DEPLOYMENT and unavailable:
                raise DeploymentRolloutConflictError(
                    "strict Ray deployment requires every assigned node to be approved and online",
                    code="DEPLOYMENT_STRICT_NODE_UNAVAILABLE",
                    details={"node_ids": unavailable},
                )
            unsupported = sorted(
                node_id
                for node_id in requested
                if (node := locked_nodes.get(node_id)) is not None
                and node.approved
                and not _agent_supports_strict_ray(node.agent_version)
            )
            if unsupported:
                raise DeploymentRolloutConflictError(
                    "strict Ray deployment requires Dure Agent 0.3.18 or newer",
                    code="DEPLOYMENT_STRICT_AGENT_TOO_OLD",
                    details={"node_ids": unsupported},
                )
            stage_unsupported = sorted(
                node_id
                for node_id in requested
                if stage_artifact
                and (node := locked_nodes.get(node_id)) is not None
                and node.approved
                and not _agent_supports_stage_artifact(node.agent_version)
            )
            if stage_unsupported:
                raise DeploymentRolloutConflictError(
                    "stage artifact deployment requires Dure Agent 0.3.19 or newer",
                    code="DEPLOYMENT_STAGE_AGENT_TOO_OLD",
                    details={"node_ids": stage_unsupported},
                )
        for node_id in dict.fromkeys(node_ids):
            node = locked_nodes.get(node_id)
            if node is None or not node.approved:
                errors[node_id] = "unknown, pending, or revoked node"
                continue
            if task_type == TaskType.UNJOIN_NODE:
                profile_record = session.get(NodeProfileRecord, node_id)
                try:
                    gpu_node = (
                        profile_record is not None
                        and bool(NodeProfile.from_dict(profile_record.profile).gpus)
                    )
                except (TypeError, ValueError):
                    gpu_node = False
                if not gpu_node:
                    errors[node_id] = "unjoin is limited to registered GPU nodes"
                    continue
            if deployment is not None and node_id not in assignments:
                errors[node_id] = "node is not assigned to deployment"
                continue
            payload = dict(options)
            if deployment is not None:
                payload.update(
                    plan=effective_plan,
                    generation=deployment.generation,
                    accept_model_download=(
                        deployment.accept_model_download
                        if deployment.source_recommendation_id is None
                        else False
                    ),
                    pull_image=(
                        deployment.pull_image
                        if deployment.source_recommendation_id is None
                        else False
                    ),
                )
            task = Task(
                bulk_id=bulk_id,
                node_id=node_id,
                type=task_type.value,
                deployment_id=deployment_id,
                payload=payload,
            )
            session.add(task)
            tasks.append(task)
            node.desired_state = task_type.value
        if deployment is not None:
            attach_deployment_bulk_operation(
                session,
                deployment=deployment,
                task_type=task_type,
                tasks=tasks,
                options=options,
            )
            if tasks and task_type in {
                TaskType.START_DEPLOYMENT,
                TaskType.RESTART_DEPLOYMENT,
            }:
                deployment.verified_at = None
        audit(
            session,
            "admin",
            "tasks.create",
            bulk_id,
            "success",
            task_type=task_type.value,
            count=len(tasks),
        )
        session.commit()
        return bulk_id, tasks, errors
    except Exception:
        session.rollback()
        raise


def claim_task(session: Session, node_id: str, lease_seconds: int = 300) -> Task | None:
    now = utcnow()
    # Serialize claims per node before inspecting active/queued tasks. This keeps
    # multiple agent processes from leasing different mutations concurrently.
    locked_node = session.scalar(select(Node).where(Node.id == node_id).with_for_update())
    if locked_node is None:
        return None
    active = session.scalar(
        select(Task.id).where(
            Task.node_id == node_id,
            Task.status == TaskStatus.RUNNING.value,
            Task.lease_until >= now,
        ).limit(1)
    )
    if active is not None:
        return None
    task = session.scalar(
        select(Task)
        .where(
            Task.node_id == node_id,
            or_(
                Task.status == TaskStatus.QUEUED.value,
                (Task.status == TaskStatus.RUNNING.value) & (Task.lease_until < now),
            ),
        )
        .order_by(Task.created_at)
        .with_for_update(skip_locked=True)
    )
    if task is None:
        return None
    try:
        if (
            task.status == TaskStatus.RUNNING.value
            and task.operation_node_id is not None
            and task.operation_attempt is not None
        ):
            if not finish_operation_task(
                session,
                task,
                node_id,
                result=None,
                error="TASK_LEASE_EXPIRED",
            ):
                raise _operation_hook_conflict("expired", task.id)
            session.commit()
            return None
        if (
            task.status == TaskStatus.RUNNING.value
            and task.type
            in {TaskType.PREPARE_MODEL.value, TaskType.PREPARE_IMAGE.value}
        ):
            from .preparation import expire_preparation_task

            if not expire_preparation_task(session, task, node_id):
                session.rollback()
                return None
            session.commit()
            return None
        task.status = TaskStatus.RUNNING.value
        task.attempts += 1
        task.lease_until = now + timedelta(seconds=lease_seconds)
        if not claim_operation_task(session, task, node_id):
            raise _operation_hook_conflict("claimed", task.id)
        if task.type in {
            TaskType.PREPARE_MODEL.value,
            TaskType.PREPARE_IMAGE.value,
        }:
            from .preparation import claim_preparation_task

            if not claim_preparation_task(session, task, node_id):
                raise ValueError("preparation-bound task cannot be claimed")
        session.commit()
        return task
    except Exception:
        session.rollback()
        raise


def extend_task(
    session: Session,
    task: Task,
    node_id: str,
    lease_seconds: int = 300,
    *,
    progress: dict | None = None,
) -> bool:
    try:
        node = session.scalar(
            select(Node)
            .where(Node.id == node_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        locked_task = session.scalar(
            select(Task)
            .where(Task.id == task.id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if (
            node is None
            or not node.approved
            or locked_task is None
            or locked_task.node_id != node_id
            or locked_task.status != TaskStatus.RUNNING.value
            or aware(locked_task.lease_until) is None
        ):
            session.rollback()
            return False
        if (
            locked_task.operation_node_id is not None
            or locked_task.operation_attempt is not None
        ):
            record = session.get(
                DeploymentOperationNode, locked_task.operation_node_id
            )
            operation = (
                session.get(DeploymentOperation, record.operation_id)
                if record is not None
                else None
            )
            if (
                record is None
                or operation is None
                or locked_task.operation_attempt != record.attempt_count
                or locked_task.type != PHASE_TASK_TYPES[record.phase].value
                or record.node_id != node_id
                or record.status != "RUNNING"
                or record.phase != operation.phase
            ):
                session.rollback()
                return False
        if locked_task.type in {
            TaskType.PREPARE_MODEL.value,
            TaskType.PREPARE_IMAGE.value,
        }:
            from .preparation import extend_preparation_task

            if not extend_preparation_task(
                session,
                locked_task,
                node_id,
                progress=progress,
            ):
                session.rollback()
                return False
        elif progress is not None:
            session.rollback()
            return False
        # The node, task and any bound operation/preparation rows may all have
        # blocked on locks.  Read the clock only after those locks are held so
        # an already expired lease cannot be revived with a stale timestamp.
        now = utcnow()
        if aware(locked_task.lease_until) < now:
            session.rollback()
            return False
        locked_task.lease_until = now + timedelta(seconds=lease_seconds)
        node.last_seen = now
        session.commit()
        return True
    except Exception:
        session.rollback()
        raise


def _finish_artifact_cache_quarantine_task(
    session: Session,
    task: Task,
    node_id: str,
    *,
    result: dict | None,
    error: str | None,
) -> bool:
    from dure.cache_quarantine import ARTIFACT_CACHE_QUARANTINE_FAILURE_CODES
    from .cache_lifecycle import complete_cache_quarantine

    try:
        locked_node = session.scalar(
            select(Node).where(Node.id == node_id).with_for_update()
        )
        locked_task = session.scalar(
            select(Task)
            .where(Task.id == task.id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if (
            locked_node is None
            or locked_task is None
            or locked_task.node_id != node_id
            or locked_task.type != TaskType.QUARANTINE_ARTIFACT_CACHE.value
        ):
            session.rollback()
            return False
        if locked_task.status in {
            TaskStatus.SUCCEEDED.value,
            TaskStatus.FAILED.value,
        }:
            return True
        if locked_task.status != TaskStatus.RUNNING.value:
            session.rollback()
            return False
        payload = locked_task.payload
        expected_payload_fields = {
            "node_id",
            "cache_kind",
            "cache_identity_digest",
        }
        if (
            type(payload) is not dict
            or set(payload) != expected_payload_fields
            or payload.get("node_id") != node_id
            or payload.get("cache_kind") not in {"FULL_SNAPSHOT", "STAGE"}
            or type(payload.get("cache_identity_digest")) is not str
            or re.fullmatch(
                r"sha256:[0-9a-f]{64}", payload["cache_identity_digest"]
            )
            is None
        ):
            raise ArtifactCacheControlError(
                "artifact cache quarantine task payload is invalid",
                code="ARTIFACT_CACHE_QUARANTINE_TASK_INVALID",
            )
        terminal_error = error
        expected_result = {
            "node_id": node_id,
            "cache_kind": payload["cache_kind"],
            "cache_identity_digest": payload["cache_identity_digest"],
        }
        if terminal_error is None and (
            type(result) is not dict
            or set(result)
            != {
                "node_id",
                "cache_kind",
                "cache_identity_digest",
                "status",
            }
            or any(result.get(key) != value for key, value in expected_result.items())
            or result.get("status")
            not in {"QUARANTINED", "ALREADY_QUARANTINED"}
        ):
            terminal_error = "CACHE_QUARANTINE_EXECUTION_FAILED"
            result = None
        if terminal_error is not None and terminal_error not in ARTIFACT_CACHE_QUARANTINE_FAILURE_CODES:
            terminal_error = "CACHE_QUARANTINE_EXECUTION_FAILED"
        succeeded = terminal_error is None
        locked_task.status = (
            TaskStatus.SUCCEEDED.value
            if succeeded
            else TaskStatus.FAILED.value
        )
        locked_task.result = result if succeeded else None
        locked_task.error = terminal_error
        locked_task.lease_until = None
        locked_node.desired_state = None
        complete_cache_quarantine(
            session,
            node_id=node_id,
            cache_identity_digest=payload["cache_identity_digest"],
            request_id=locked_task.id,
            succeeded=succeeded,
            source_task_id=locked_task.id,
        )
        session.commit()
        return True
    except Exception:
        session.rollback()
        raise


def finish_task(session: Session, task: Task, node_id: str, *, result: dict | None, error: str | None) -> bool:
    if task.type == TaskType.BENCHMARK.value:
        return False
    if task.node_id != node_id:
        return False
    if task.type in {
        TaskType.PREPARE_MODEL.value,
        TaskType.PREPARE_IMAGE.value,
    }:
        from .preparation import finish_preparation_task

        try:
            accepted, _preparation = finish_preparation_task(
                session,
                task,
                node_id,
                result=result,
                error=error,
            )
            if not accepted:
                session.rollback()
                return False
            session.commit()
            return True
        except Exception as exc:
            if getattr(exc, "code", None) == "PREPARATION_RESULT_REJECTED":
                session.commit()
            else:
                session.rollback()
            raise
    if task.type == TaskType.QUARANTINE_ARTIFACT_CACHE.value:
        return _finish_artifact_cache_quarantine_task(
            session,
            task,
            node_id,
            result=result,
            error=error,
        )
    if task.operation_node_id is not None or task.operation_attempt is not None:
        try:
            locked_node = session.scalar(
                select(Node).where(Node.id == node_id).with_for_update()
            )
            if locked_node is None:
                session.rollback()
                return False
            accepted = finish_operation_task(
                session,
                task,
                node_id,
                result=result,
                error=error,
            )
            if not accepted:
                session.rollback()
                return False
            session.commit()
            return True
        except Exception:
            session.rollback()
            raise
    try:
        node = session.scalar(
            select(Node).where(Node.id == node_id).with_for_update()
        )
        locked_task = session.scalar(
            select(Task)
            .where(Task.id == task.id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if (
            node is None
            or locked_task is None
            or locked_task.node_id != node_id
        ):
            session.rollback()
            return False
        task = locked_task
        terminal = {TaskStatus.SUCCEEDED.value, TaskStatus.FAILED.value}
        if task.status in terminal:
            return True
        if task.status != TaskStatus.RUNNING.value:
            return False
        terminal_error = error
        terminal_result = result
        plan = task.payload.get("plan") if type(task.payload) is dict else None
        if (
            terminal_error is None
            and task.type in DEPLOYMENT_TASK_TYPE_VALUES
            and type(plan) is dict
            and plan.get("execution_backend") == VLLM_RAY_PP_BACKEND
            and not valid_deployment_task_success_result(task, result)
        ):
            terminal_error = "TASK_RESULT_INVALID"
            terminal_result = None
        task.status = (
            TaskStatus.FAILED.value
            if terminal_error is not None
            else TaskStatus.SUCCEEDED.value
        )
        task.result = terminal_result
        task.error = terminal_error
        task.lease_until = None
        node.desired_state = None
        if terminal_error is None and task.type == TaskType.UNJOIN_NODE.value:
            _mark_node_unjoined(session, node, actor=f"node:{node_id}")
        if (
            not error
            and task.type == TaskType.PROBE.value
            and result
            and isinstance(result.get("profile"), dict)
        ):
            parsed_profile = NodeProfile.from_dict(result["profile"])
            profile_record = session.get(NodeProfileRecord, node_id)
            if profile_record is None:
                session.add(
                    NodeProfileRecord(node_id=node_id, profile=result["profile"])
                )
            else:
                profile_record.profile = result["profile"]
                profile_record.updated_at = utcnow()
            if parsed_profile.artifact_cache_observations is not None:
                from .cache_lifecycle import reconcile_probe_observations

                reconcile_probe_observations(
                    session,
                    node_id=node_id,
                    observations=parsed_profile.artifact_cache_observations,
                    scan_complete=parsed_profile.artifact_cache_scan_complete,
                    source_id=f"probe:{task.id}",
                    source_task_id=task.id,
                )
        session.commit()
        return True
    except Exception:
        session.rollback()
        raise


def cancel_task(session: Session, task: Task) -> bool:
    identity = session.execute(
        select(
            Task.type,
            Task.node_id,
            Task.status,
            Task.operation_node_id,
            Task.operation_attempt,
        ).where(Task.id == task.id)
    ).one_or_none()
    if identity is None:
        return False
    if identity.type in {
        TaskType.PREPARE_MODEL.value,
        TaskType.PREPARE_IMAGE.value,
    }:
        from .preparation import cancel_preparation_task

        try:
            locked_node = session.scalar(
                select(Node)
                .where(Node.id == identity.node_id)
                .with_for_update()
            )
            if locked_node is None:
                session.rollback()
                return False
            accepted = cancel_preparation_task(session, task)
            if not accepted:
                session.rollback()
                return False
            audit(session, "admin", "task.cancel", task.id, "success")
            session.commit()
            return True
        except Exception:
            session.rollback()
            raise
    if identity.type == TaskType.QUARANTINE_ARTIFACT_CACHE.value:
        from .cache_lifecycle import complete_cache_quarantine

        try:
            locked_node = session.scalar(
                select(Node)
                .where(Node.id == identity.node_id)
                .with_for_update()
            )
            locked_task = session.scalar(
                select(Task)
                .where(Task.id == task.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
            lease_until = aware(locked_task.lease_until) if locked_task else None
            cancelable = locked_task is not None and (
                locked_task.status == TaskStatus.QUEUED.value
                or (
                    locked_task.status == TaskStatus.RUNNING.value
                    and (lease_until is None or lease_until < utcnow())
                )
            )
            if (
                locked_node is None
                or locked_task is None
                or not cancelable
                or type(locked_task.payload) is not dict
                or locked_task.payload.get("node_id") != identity.node_id
                or type(locked_task.payload.get("cache_identity_digest")) is not str
            ):
                session.rollback()
                return False
            locked_task.status = TaskStatus.CANCELED.value
            locked_task.lease_until = None
            locked_node.desired_state = None
            complete_cache_quarantine(
                session,
                node_id=identity.node_id,
                cache_identity_digest=locked_task.payload[
                    "cache_identity_digest"
                ],
                request_id=locked_task.id,
                succeeded=False,
                source_task_id=locked_task.id,
            )
            audit(session, "admin", "task.cancel", task.id, "success")
            session.commit()
            return True
        except Exception:
            session.rollback()
            raise
    if (
        identity.operation_node_id is not None
        or identity.operation_attempt is not None
    ):
        try:
            locked_node = session.scalar(
                select(Node)
                .where(Node.id == identity.node_id)
                .with_for_update()
            )
            current_task = session.scalar(
                select(Task)
                .where(Task.id == task.id)
                .execution_options(populate_existing=True)
            )
            if locked_node is None or current_task is None:
                session.rollback()
                return False
            lease_until = aware(current_task.lease_until)
            expired_running = (
                current_task.status == TaskStatus.RUNNING.value
                and lease_until is not None
                and lease_until < utcnow()
            )
            expired_failure_replay = (
                current_task.status == TaskStatus.FAILED.value
                and current_task.error == "TASK_LEASE_EXPIRED"
            )
            if expired_running or expired_failure_replay:
                accepted = finish_operation_task(
                    session,
                    current_task,
                    identity.node_id,
                    result=None,
                    error="TASK_LEASE_EXPIRED",
                )
            else:
                accepted = cancel_operation_task(session, current_task)
            if not accepted:
                session.rollback()
                return False
            if identity.status not in {
                TaskStatus.CANCELED.value,
                TaskStatus.FAILED.value,
            }:
                audit(
                    session,
                    "admin",
                    "task.cancel",
                    task.id,
                    "success",
                    expired_lease=expired_running or expired_failure_replay,
                )
            session.commit()
            return True
        except Exception:
            session.rollback()
            raise
    run = None
    if identity.type == TaskType.BENCHMARK.value:
        locked_node, locked_task, run = _lock_benchmark_terminal(
            session, task_id=task.id, node_id=identity.node_id
        )
        if (
            locked_node is None
            or locked_task is None
            or run is None
            or locked_task.type != TaskType.BENCHMARK.value
            or locked_task.node_id != identity.node_id
            or run.coordinator_node_id != identity.node_id
            or run.status != "QUEUED"
        ):
            return False
    else:
        locked_node = session.scalar(
            select(Node).where(Node.id == identity.node_id).with_for_update()
        )
        locked_task = session.scalar(
            select(Task)
            .where(Task.id == task.id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if locked_node is None or locked_task is None:
            return False
    task = locked_task
    lease_until = task.lease_until
    if lease_until is not None and lease_until.tzinfo is None:
        lease_until = lease_until.replace(tzinfo=timezone.utc)
    expired_benchmark_lease = (
        task.type == TaskType.BENCHMARK.value
        and task.status == TaskStatus.RUNNING.value
        and (lease_until is None or lease_until < utcnow())
    )
    if task.status != TaskStatus.QUEUED.value and not expired_benchmark_lease:
        return False
    if task.type == TaskType.BENCHMARK.value:
        assert run is not None
        run.status = "FAILED"
        run.failure_code = "BENCHMARK_CANCELED"
        run.updated_at = utcnow()
    task.status = TaskStatus.CANCELED.value
    task.lease_until = None
    locked_node.desired_state = None
    audit(session, "admin", "task.cancel", task.id, "success")
    session.commit()
    return True

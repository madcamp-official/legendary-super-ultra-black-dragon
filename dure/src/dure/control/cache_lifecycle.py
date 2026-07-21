from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import ArtifactCacheObservation
from ..stage_cache import StageCacheError, StageCacheIdentity
from ..task import TaskStatus, TaskType
from .models import (
    ArtifactCacheEvent,
    ArtifactManifest,
    ArtifactPreparationAttempt,
    ArtifactPreparationNode,
    ArtifactPreparation,
    Deployment,
    DeploymentOperation,
    Node,
    NodeArtifactCache,
    StageArtifactRank,
    StageArtifactVariant,
    Task,
    utcnow,
)


FULL_SNAPSHOT = "FULL_SNAPSHOT"
STAGE = "STAGE"

CACHE_STATUSES = frozenset(
    {"READY", "STALE", "MISSING", "CORRUPT", "QUARANTINED"}
)
CACHE_STATUS_PRIORITY = {
    "READY": 1,
    "STALE": 2,
    "MISSING": 3,
    "CORRUPT": 4,
    "QUARANTINED": 5,
}
CACHE_REASON_CODES = frozenset(
    {
        "PREPARATION_SUCCEEDED",
        "PROBE_UNSAFE",
        "PROBE_CORRUPT",
        "PROBE_IDENTITY_MISMATCH",
        "PROBE_MISSING",
        "VARIANT_REVOKED",
        "VERIFICATION_FAILED",
        "QUARANTINE_REQUESTED",
        "QUARANTINE_SUCCEEDED",
        "QUARANTINE_FAILED",
    }
)

_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
_SOURCE_ID = re.compile(r"[^\x00-\x1f\x7f]{1,255}")


class ArtifactCacheLifecycleError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        code: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = details or {}


class ArtifactCacheNotFoundError(ArtifactCacheLifecycleError):
    def __init__(self) -> None:
        super().__init__(
            "artifact cache state was not found",
            code="ARTIFACT_CACHE_NOT_FOUND",
        )


class ArtifactCacheNotReadyError(ArtifactCacheLifecycleError):
    def __init__(self, code: str = "ARTIFACT_CACHE_NOT_READY") -> None:
        super().__init__("artifact cache is not ready", code=code)


class ArtifactCacheConflictError(ArtifactCacheLifecycleError):
    def __init__(self, code: str = "ARTIFACT_CACHE_CONFLICT") -> None:
        super().__init__("artifact cache state conflicts with its source", code=code)


class ArtifactCacheStaleAttemptError(ArtifactCacheLifecycleError):
    def __init__(self) -> None:
        super().__init__(
            "artifact preparation attempt is no longer current",
            code="ARTIFACT_CACHE_STALE_ATTEMPT",
        )


@dataclass(frozen=True)
class ArtifactCacheIdentity:
    cache_kind: str
    cache_identity_digest: str
    manifest_digest: str
    source_manifest_digest: str
    verification_version: int
    artifact_set_digest: str | None = None
    pipeline_rank: int | None = None
    tensor_rank: int | None = None
    tensor_parallel_size: int | None = None
    pipeline_parallel_size: int | None = None
    tensor_keys_digest: str | None = None

    @property
    def artifact_rank(self) -> int | None:
        if self.pipeline_rank is None or self.tensor_rank is None:
            return None
        if self.tensor_parallel_size is None:
            return None
        return self.pipeline_rank * self.tensor_parallel_size + self.tensor_rank

    def validate(self) -> None:
        for field in (
            "cache_identity_digest",
            "manifest_digest",
            "source_manifest_digest",
        ):
            value = getattr(self, field)
            if type(value) is not str or _DIGEST.fullmatch(value) is None:
                raise ArtifactCacheConflictError("ARTIFACT_CACHE_IDENTITY_INVALID")
        if type(self.verification_version) is not int or self.verification_version != 1:
            raise ArtifactCacheConflictError(
                "ARTIFACT_CACHE_VERIFICATION_VERSION_UNSUPPORTED"
            )
        stage_values = (
            self.artifact_set_digest,
            self.pipeline_rank,
            self.tensor_rank,
            self.tensor_parallel_size,
            self.pipeline_parallel_size,
            self.tensor_keys_digest,
        )
        if self.cache_kind == FULL_SNAPSHOT:
            if (
                self.cache_identity_digest != self.manifest_digest
                or self.source_manifest_digest != self.manifest_digest
                or any(value is not None for value in stage_values)
            ):
                raise ArtifactCacheConflictError(
                    "ARTIFACT_CACHE_FULL_IDENTITY_INVALID"
                )
            return
        if self.cache_kind != STAGE:
            raise ArtifactCacheConflictError("ARTIFACT_CACHE_KIND_UNSUPPORTED")
        if (
            type(self.artifact_set_digest) is not str
            or _DIGEST.fullmatch(self.artifact_set_digest) is None
            or type(self.tensor_keys_digest) is not str
            or _DIGEST.fullmatch(self.tensor_keys_digest) is None
            or type(self.tensor_parallel_size) is not int
            or self.tensor_parallel_size != 1
            or type(self.pipeline_parallel_size) is not int
            or not 1 <= self.pipeline_parallel_size <= 64
            or type(self.pipeline_rank) is not int
            or not 0 <= self.pipeline_rank < self.pipeline_parallel_size
            or type(self.tensor_rank) is not int
            or self.tensor_rank != 0
        ):
            raise ArtifactCacheConflictError("ARTIFACT_CACHE_STAGE_IDENTITY_INVALID")

    def event_identity(self) -> dict[str, Any]:
        self.validate()
        value = {
            "cache_kind": self.cache_kind,
            "cache_identity_digest": self.cache_identity_digest,
            "manifest_digest": self.manifest_digest,
            "source_manifest_digest": self.source_manifest_digest,
            "verification_version": self.verification_version,
        }
        if self.cache_kind == STAGE:
            value.update(
                artifact_set_digest=self.artifact_set_digest,
                pipeline_rank=self.pipeline_rank,
                tensor_rank=self.tensor_rank,
                tensor_parallel_size=self.tensor_parallel_size,
                pipeline_parallel_size=self.pipeline_parallel_size,
                tensor_keys_digest=self.tensor_keys_digest,
            )
        return value


def _canonical_digest(value: dict[str, Any]) -> str:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
    except (TypeError, ValueError):
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_EVIDENCE_INVALID") from None
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    aware = _aware(value)
    if aware is None:  # pragma: no cover - type narrowing
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_TIMESTAMP_INVALID")
    return aware.isoformat().replace("+00:00", "Z")


def _canonical_uuid(value: str, *, code: str) -> str:
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, TypeError, ValueError):
        raise ArtifactCacheConflictError(code) from None
    canonical = str(parsed)
    if value != canonical or parsed.version != 4:
        raise ArtifactCacheConflictError(code)
    return canonical


def _source_id(value: str) -> str:
    if type(value) is not str or _SOURCE_ID.fullmatch(value) is None:
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_SOURCE_INVALID")
    return value


def _identity_values(identity: ArtifactCacheIdentity) -> dict[str, Any]:
    identity.validate()
    return {
        "cache_kind": identity.cache_kind,
        "cache_identity_digest": identity.cache_identity_digest,
        "manifest_digest": identity.manifest_digest,
        "source_manifest_digest": identity.source_manifest_digest,
        "artifact_set_digest": identity.artifact_set_digest,
        "artifact_rank": identity.artifact_rank,
        "pipeline_rank": identity.pipeline_rank,
        "tensor_rank": identity.tensor_rank,
        "tensor_parallel_size": identity.tensor_parallel_size,
        "pipeline_parallel_size": identity.pipeline_parallel_size,
        "tensor_keys_digest": identity.tensor_keys_digest,
    }


def _cache_matches_identity(
    cache: NodeArtifactCache, identity: ArtifactCacheIdentity
) -> bool:
    return all(
        getattr(cache, field) == value
        for field, value in _identity_values(identity).items()
    )


def _validate_identity_database(
    session: Session,
    identity: ArtifactCacheIdentity,
    *,
    require_validated_stage: bool,
) -> ArtifactManifest:
    identity.validate()
    manifest = session.get(ArtifactManifest, identity.manifest_digest)
    source = session.get(ArtifactManifest, identity.source_manifest_digest)
    if manifest is None or source is None:
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_MANIFEST_NOT_FOUND")
    if identity.cache_kind == FULL_SNAPSHOT:
        if manifest.model_artifact_id is None:
            raise ArtifactCacheConflictError("ARTIFACT_CACHE_FULL_MANIFEST_INVALID")
        return manifest
    variant_statement = select(StageArtifactVariant).where(
        StageArtifactVariant.artifact_set_digest == identity.artifact_set_digest
    )
    if require_validated_stage:
        variant_statement = (
            variant_statement.with_for_update()
            .execution_options(populate_existing=True)
        )
    variant = session.scalar(variant_statement)
    if (
        variant is None
        or variant.source_manifest_digest != identity.source_manifest_digest
        or variant.tensor_parallel_size != identity.tensor_parallel_size
        or variant.pipeline_parallel_size != identity.pipeline_parallel_size
    ):
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_STAGE_VARIANT_MISMATCH")
    if require_validated_stage and variant.status != "VALIDATED":
        raise ArtifactCacheNotReadyError("ARTIFACT_CACHE_STAGE_VARIANT_UNAVAILABLE")
    rank = session.scalar(
        select(StageArtifactRank).where(
            StageArtifactRank.variant_id == identity.artifact_set_digest,
            StageArtifactRank.rank == identity.artifact_rank,
            StageArtifactRank.pipeline_rank == identity.pipeline_rank,
            StageArtifactRank.tensor_rank == identity.tensor_rank,
            StageArtifactRank.manifest_digest == identity.manifest_digest,
            StageArtifactRank.tensor_keys_digest == identity.tensor_keys_digest,
        )
    )
    if rank is None:
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_STAGE_RANK_MISMATCH")
    return manifest


def _lock_node(session: Session, node_id: str) -> Node:
    node = session.scalar(select(Node).where(Node.id == node_id).with_for_update())
    if node is None:
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_NODE_NOT_FOUND")
    return node


def _lock_source_task(
    session: Session, task_id: str | None, node_id: str
) -> Task | None:
    if task_id is None:
        return None
    task = session.scalar(select(Task).where(Task.id == task_id).with_for_update())
    if task is None or task.node_id != node_id:
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_SOURCE_TASK_INVALID")
    return task


def _lock_cache(
    session: Session, node_id: str, cache_identity_digest: str
) -> NodeArtifactCache | None:
    return session.scalar(
        select(NodeArtifactCache)
        .where(
            NodeArtifactCache.node_id == node_id,
            NodeArtifactCache.cache_identity_digest == cache_identity_digest,
        )
        .with_for_update()
    )


def _event_replay(
    session: Session,
    cache: NodeArtifactCache,
    *,
    source_kind: str,
    source_id: str,
    reason_code: str,
    source_attempt_id: str | None,
    source_task_id: str | None,
    evidence_kind: str,
    evidence_digest: str,
) -> ArtifactCacheEvent | None:
    event = session.scalar(
        select(ArtifactCacheEvent).where(
            ArtifactCacheEvent.cache_id == cache.id,
            ArtifactCacheEvent.source_kind == source_kind,
            ArtifactCacheEvent.source_id == source_id,
            ArtifactCacheEvent.reason_code == reason_code,
        )
    )
    if event is None:
        if source_kind != "QUARANTINE":
            reused_source = session.scalar(
                select(ArtifactCacheEvent.id).where(
                    ArtifactCacheEvent.cache_id == cache.id,
                    ArtifactCacheEvent.source_kind == source_kind,
                    ArtifactCacheEvent.source_id == source_id,
                )
            )
            if reused_source is not None:
                raise ArtifactCacheConflictError("ARTIFACT_CACHE_REPLAY_CONFLICT")
        return None
    if (
        event.source_attempt_id != source_attempt_id
        or event.source_task_id != source_task_id
        or event.evidence_kind != evidence_kind
        or event.evidence_digest != evidence_digest
    ):
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_REPLAY_CONFLICT")
    return event


def _append_event(
    session: Session,
    cache: NodeArtifactCache,
    *,
    previous_status: str | None,
    source_kind: str,
    source_id: str,
    reason_code: str,
    source_attempt_id: str | None,
    source_task_id: str | None,
    evidence_kind: str,
    evidence_digest: str,
    now: datetime,
) -> ArtifactCacheEvent:
    if reason_code not in CACHE_REASON_CODES:
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_REASON_UNSUPPORTED")
    cache.event_sequence += 1
    event = ArtifactCacheEvent(
        cache_id=cache.id,
        sequence=cache.event_sequence,
        previous_status=(None if cache.event_sequence == 1 else previous_status),
        status=cache.status,
        reason_code=reason_code,
        source_kind=source_kind,
        source_id=_source_id(source_id),
        source_attempt_id=source_attempt_id,
        source_task_id=source_task_id,
        evidence_kind=evidence_kind,
        evidence_digest=evidence_digest,
        created_at=now,
    )
    cache.updated_at = now
    session.add(event)
    session.flush()
    return event


def _priority_transition(
    cache: NodeArtifactCache,
    *,
    target_status: str,
    reason_code: str,
) -> None:
    if target_status not in CACHE_STATUSES:
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_STATUS_UNSUPPORTED")
    if CACHE_STATUS_PRIORITY[target_status] < CACHE_STATUS_PRIORITY[cache.status]:
        return
    cache.status = target_status
    cache.reason_code = reason_code
    if target_status != "QUARANTINED":
        cache.quarantined_at = None


def _preparation_context(
    session: Session,
    attempt_id: str,
    identity: ArtifactCacheIdentity,
) -> tuple[
    Node,
    Task,
    ArtifactPreparationAttempt,
    ArtifactPreparationNode,
    dict[str, Any],
    bool,
]:
    discovered_attempt = session.get(ArtifactPreparationAttempt, attempt_id)
    if discovered_attempt is None:
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_ATTEMPT_NOT_FOUND")
    discovered_record = session.get(
        ArtifactPreparationNode, discovered_attempt.preparation_node_id
    )
    if discovered_record is None:
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_ATTEMPT_INVALID")

    # This follows the controller's Node -> Task -> attempt state lock order.
    node = _lock_node(session, discovered_record.node_id)
    task = _lock_source_task(session, discovered_attempt.task_id, node.id)
    if task is None:  # pragma: no cover - an attempt always has a task FK
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_SOURCE_TASK_INVALID")
    attempt = session.scalar(
        select(ArtifactPreparationAttempt)
        .where(ArtifactPreparationAttempt.id == attempt_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if attempt is None:
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_ATTEMPT_NOT_FOUND")
    record = session.scalar(
        select(ArtifactPreparationNode)
        .where(ArtifactPreparationNode.id == attempt.preparation_node_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if record is None:
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_ATTEMPT_INVALID")
    if (
        task.type != TaskType.PREPARE_MODEL.value
        or task.status != TaskStatus.SUCCEEDED.value
        or attempt.task_id != task.id
        or attempt.stage != "MODEL"
        or attempt.status != "SUCCEEDED"
        or attempt.completed_at is None
        or record.node_id != node.id
        or record.model_manifest_digest != identity.manifest_digest
    ):
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_ATTEMPT_INVALID")
    result = attempt.result
    if type(result) is not dict:
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_RESULT_INVALID")
    expected = {
        "cache_kind": identity.cache_kind,
        "manifest_digest": identity.manifest_digest,
        "verification_version": identity.verification_version,
    }
    if identity.cache_kind == STAGE:
        expected.update(
            artifact_set_digest=identity.artifact_set_digest,
            pipeline_rank=identity.pipeline_rank,
            tensor_rank=identity.tensor_rank,
            tensor_keys_digest=identity.tensor_keys_digest,
            cache_identity_digest=identity.cache_identity_digest,
        )
    if any(result.get(key) != value for key, value in expected.items()):
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_RESULT_IDENTITY_MISMATCH")
    if (
        type(result.get("bytes_verified")) is not int
        or result["bytes_verified"] < 1
        or type(result.get("file_count")) is not int
        or result["file_count"] < 1
    ):
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_RESULT_INVALID")
    current = (
        node.approved
        and record.model_status == "SUCCEEDED"
        and record.model_current_attempt == attempt.attempt_no
    )
    return node, task, attempt, record, result, current


def record_preparation_success(
    session: Session,
    *,
    attempt_id: str,
    identity: ArtifactCacheIdentity,
) -> tuple[NodeArtifactCache, ArtifactCacheEvent, bool]:
    """Project one current successful MODEL attempt into authoritative READY.

    The caller owns the transaction. This helper flushes but never commits.
    A replay of the same attempt returns the original event without changing a
    later state. A newer current success may recover even CORRUPT or
    QUARANTINED state.
    """

    identity.validate()
    node, task, attempt, record, result, current = _preparation_context(
        session, attempt_id, identity
    )
    verified_at = _aware(attempt.completed_at)
    if verified_at is None:  # pragma: no cover - checked by preparation context
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_ATTEMPT_INVALID")
    evidence = {
        "schema_version": 1,
        "identity": identity.event_identity(),
        "attempt_id": attempt.id,
        "task_id": task.id,
        "attempt_no": attempt.attempt_no,
        "bytes_verified": result["bytes_verified"],
        "file_count": result["file_count"],
        "verified_at": _iso(verified_at),
    }
    evidence_digest = _canonical_digest(evidence)

    # An already accepted source is an immutable replay even if a later
    # attempt, revocation, probe, or quarantine has since made it stale. It
    # returns the original event and must never resurrect READY.
    existing = session.scalar(
        select(NodeArtifactCache).where(
            NodeArtifactCache.node_id == node.id,
            NodeArtifactCache.cache_identity_digest
            == identity.cache_identity_digest,
        )
    )
    if existing is not None and _cache_matches_identity(existing, identity):
        replay = _event_replay(
            session,
            existing,
            source_kind="PREPARATION",
            source_id=attempt.id,
            reason_code="PREPARATION_SUCCEEDED",
            source_attempt_id=attempt.id,
            source_task_id=task.id,
            evidence_kind="PREPARATION_RESULT",
            evidence_digest=evidence_digest,
        )
        if replay is not None:
            return existing, replay, False
    if not current:
        if attempt.attempt_no < record.model_current_attempt:
            raise ArtifactCacheStaleAttemptError()
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_ATTEMPT_INVALID")

    manifest = _validate_identity_database(
        session, identity, require_validated_stage=True
    )
    if (
        result["bytes_verified"] != manifest.total_size_bytes
        or result["file_count"] != manifest.file_count
    ):
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_RESULT_SIZE_MISMATCH")
    cache = _lock_cache(session, node.id, identity.cache_identity_digest)
    created = cache is None
    now = utcnow()
    if cache is None:
        cache = NodeArtifactCache(
            node_id=node.id,
            **_identity_values(identity),
            status="READY",
            reason_code="PREPARATION_SUCCEEDED",
            last_ready_attempt_id=attempt.id,
            verified_at=verified_at,
            verified_size_bytes=result["bytes_verified"],
            verified_file_count=result["file_count"],
            verification_version=identity.verification_version,
            event_sequence=0,
            created_at=now,
            updated_at=now,
        )
        session.add(cache)
        session.flush()
    elif not _cache_matches_identity(cache, identity):
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_IDENTITY_COLLISION")

    replay = _event_replay(
        session,
        cache,
        source_kind="PREPARATION",
        source_id=attempt.id,
        reason_code="PREPARATION_SUCCEEDED",
        source_attempt_id=attempt.id,
        source_task_id=task.id,
        evidence_kind="PREPARATION_RESULT",
        evidence_digest=evidence_digest,
    )
    if replay is not None:
        return cache, replay, False

    previous = None if created else cache.status
    cache.status = "READY"
    cache.reason_code = "PREPARATION_SUCCEEDED"
    cache.last_ready_attempt_id = attempt.id
    cache.verified_at = verified_at
    cache.verified_size_bytes = result["bytes_verified"]
    cache.verified_file_count = result["file_count"]
    cache.verification_version = identity.verification_version
    cache.quarantine_request_id = None
    cache.quarantined_at = None
    event = _append_event(
        session,
        cache,
        previous_status=previous,
        source_kind="PREPARATION",
        source_id=attempt.id,
        reason_code="PREPARATION_SUCCEEDED",
        source_attempt_id=attempt.id,
        source_task_id=task.id,
        evidence_kind="PREPARATION_RESULT",
        evidence_digest=evidence_digest,
        now=now,
    )
    return cache, event, True


def _observation_matches(
    cache: NodeArtifactCache, observation: ArtifactCacheObservation
) -> bool:
    if (
        observation.cache_kind != cache.cache_kind
        or observation.cache_identity_digest != cache.cache_identity_digest
        or observation.manifest_digest != cache.manifest_digest
        or observation.verification_version != cache.verification_version
    ):
        return False
    if cache.cache_kind == FULL_SNAPSHOT:
        return True
    return (
        observation.artifact_set_digest == cache.artifact_set_digest
        and observation.source_manifest_digest == cache.source_manifest_digest
        and observation.pipeline_rank == cache.pipeline_rank
        and observation.tensor_rank == cache.tensor_rank
    )


def reconcile_probe_observations(
    session: Session,
    *,
    node_id: str,
    observations: Iterable[ArtifactCacheObservation | dict[str, Any]] | None,
    scan_complete: bool | None,
    source_id: str,
    observed_at: datetime | None = None,
    source_task_id: str | None = None,
) -> list[ArtifactCacheEvent]:
    """Reconcile a complete, closed cache scan without promoting READY.

    Legacy profiles and incomplete scans are deliberately no-ops. PRESENT only
    refreshes the observation timestamp. Unknown cache identities are ignored;
    only identities established by a successful central preparation are
    tracked.
    """

    if scan_complete is not True or observations is None:
        return []
    source_id = _source_id(source_id)
    at = _aware(observed_at or utcnow())
    if at is None:  # pragma: no cover - default is non-null
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_TIMESTAMP_INVALID")
    parsed: dict[tuple[str, str], ArtifactCacheObservation] = {}
    for raw in observations:
        observation = (
            raw
            if isinstance(raw, ArtifactCacheObservation)
            else ArtifactCacheObservation.from_dict(raw)
        )
        observation.validate()
        key = (observation.cache_kind, observation.cache_identity_digest)
        if key in parsed:
            raise ArtifactCacheConflictError("ARTIFACT_CACHE_PROBE_DUPLICATE")
        parsed[key] = observation

    _lock_node(session, node_id)
    source_task = _lock_source_task(session, source_task_id, node_id)
    if source_task is not None and source_task.type != TaskType.PROBE.value:
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_PROBE_TASK_INVALID")
    caches = list(
        session.scalars(
            select(NodeArtifactCache)
            .where(NodeArtifactCache.node_id == node_id)
            .order_by(NodeArtifactCache.id)
            .with_for_update()
        )
    )
    events: list[ArtifactCacheEvent] = []
    for cache in caches:
        previous_observed = _aware(cache.last_probe_observed_at)
        if previous_observed is not None and at <= previous_observed:
            continue
        observation = parsed.get((cache.cache_kind, cache.cache_identity_digest))
        if observation is None:
            target, reason = "MISSING", "PROBE_MISSING"
            evidence_value: dict[str, Any] = {
                "schema_version": 1,
                "scan_id": source_id,
                "condition": "MISSING",
                "cache_kind": cache.cache_kind,
                "cache_identity_digest": cache.cache_identity_digest,
            }
        else:
            evidence_value = {
                "schema_version": 1,
                "scan_id": source_id,
                "observation": observation.to_dict(),
            }
            if observation.condition == "UNSAFE":
                target, reason = "CORRUPT", "PROBE_UNSAFE"
            elif observation.condition == "CORRUPT":
                target, reason = "CORRUPT", "PROBE_CORRUPT"
            elif observation.condition == "IDENTITY_MISMATCH" or not _observation_matches(
                cache, observation
            ):
                target, reason = "STALE", "PROBE_IDENTITY_MISMATCH"
            else:
                # Probe metadata never proves READY and never heals a worse state.
                cache.last_probe_observed_at = at
                cache.updated_at = at
                continue
        evidence_digest = _canonical_digest(evidence_value)
        replay = _event_replay(
            session,
            cache,
            source_kind="PROBE",
            source_id=source_id,
            reason_code=reason,
            source_attempt_id=None,
            source_task_id=source_task_id,
            evidence_kind="PROBE_OBSERVATION",
            evidence_digest=evidence_digest,
        )
        if replay is not None:
            events.append(replay)
            continue
        previous = cache.status
        _priority_transition(cache, target_status=target, reason_code=reason)
        cache.last_probe_observed_at = at
        events.append(
            _append_event(
                session,
                cache,
                previous_status=previous,
                source_kind="PROBE",
                source_id=source_id,
                reason_code=reason,
                source_attempt_id=None,
                source_task_id=source_task_id,
                evidence_kind="PROBE_OBSERVATION",
                evidence_digest=evidence_digest,
                now=at,
            )
        )
    return events


def mark_stage_variant_revoked(
    session: Session,
    *,
    artifact_set_digest: str,
    revoked_at: datetime | None = None,
) -> list[ArtifactCacheEvent]:
    if type(artifact_set_digest) is not str or _DIGEST.fullmatch(artifact_set_digest) is None:
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_STAGE_VARIANT_INVALID")
    variant = session.scalar(
        select(StageArtifactVariant)
        .where(StageArtifactVariant.artifact_set_digest == artifact_set_digest)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if variant is None or variant.status != "REVOKED":
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_STAGE_VARIANT_NOT_REVOKED")
    at = _aware(revoked_at or variant.revoked_at or utcnow())
    if at is None:  # pragma: no cover - default is non-null
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_TIMESTAMP_INVALID")
    # The stage registry owns the Variant -> Cache order. Do not acquire Node
    # here: preparation completion uses Node -> Variant -> Cache, and adding a
    # Variant -> Node edge would permit a deadlock during revocation.
    caches = list(
        session.scalars(
            select(NodeArtifactCache)
            .where(NodeArtifactCache.artifact_set_digest == artifact_set_digest)
            .order_by(NodeArtifactCache.id)
            .with_for_update()
        )
    )
    evidence_digest = _canonical_digest(
        {
            "schema_version": 1,
            "artifact_set_digest": artifact_set_digest,
            "status": "REVOKED",
            "revoked_at": _iso(at),
        }
    )
    events: list[ArtifactCacheEvent] = []
    for cache in caches:
        replay = _event_replay(
            session,
            cache,
            source_kind="VARIANT",
            source_id=artifact_set_digest,
            reason_code="VARIANT_REVOKED",
            source_attempt_id=None,
            source_task_id=None,
            evidence_kind="STAGE_VARIANT_STATUS",
            evidence_digest=evidence_digest,
        )
        if replay is not None:
            events.append(replay)
            continue
        previous = cache.status
        _priority_transition(
            cache, target_status="STALE", reason_code="VARIANT_REVOKED"
        )
        events.append(
            _append_event(
                session,
                cache,
                previous_status=previous,
                source_kind="VARIANT",
                source_id=artifact_set_digest,
                reason_code="VARIANT_REVOKED",
                source_attempt_id=None,
                source_task_id=None,
                evidence_kind="STAGE_VARIANT_STATUS",
                evidence_digest=evidence_digest,
                now=at,
            )
        )
    return events


def record_verification_failure(
    session: Session,
    *,
    node_id: str,
    identity: ArtifactCacheIdentity,
    source_id: str,
    source_task_id: str | None = None,
    failed_at: datetime | None = None,
) -> tuple[NodeArtifactCache, ArtifactCacheEvent, bool]:
    identity.validate()
    source_id = _source_id(source_id)
    at = _aware(failed_at or utcnow())
    if at is None:  # pragma: no cover
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_TIMESTAMP_INVALID")
    _lock_node(session, node_id)
    _lock_source_task(session, source_task_id, node_id)
    cache = _lock_cache(session, node_id, identity.cache_identity_digest)
    if cache is None:
        raise ArtifactCacheNotFoundError()
    if not _cache_matches_identity(cache, identity):
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_IDENTITY_COLLISION")
    evidence_digest = _canonical_digest(
        {
            "schema_version": 1,
            "identity": identity.event_identity(),
            "verification_id": source_id,
        }
    )
    replay = _event_replay(
        session,
        cache,
        source_kind="VERIFICATION",
        source_id=source_id,
        reason_code="VERIFICATION_FAILED",
        source_attempt_id=None,
        source_task_id=source_task_id,
        evidence_kind="RUNTIME_VERIFICATION",
        evidence_digest=evidence_digest,
    )
    if replay is not None:
        return cache, replay, False
    previous = cache.status
    _priority_transition(
        cache, target_status="CORRUPT", reason_code="VERIFICATION_FAILED"
    )
    event = _append_event(
        session,
        cache,
        previous_status=previous,
        source_kind="VERIFICATION",
        source_id=source_id,
        reason_code="VERIFICATION_FAILED",
        source_attempt_id=None,
        source_task_id=source_task_id,
        evidence_kind="RUNTIME_VERIFICATION",
        evidence_digest=evidence_digest,
        now=at,
    )
    return cache, event, True


def request_cache_quarantine(
    session: Session,
    *,
    node_id: str,
    cache_identity_digest: str,
    request_id: str,
    source_task_id: str | None = None,
    requested_at: datetime | None = None,
) -> tuple[NodeArtifactCache, ArtifactCacheEvent, bool]:
    request_id = _canonical_uuid(
        request_id, code="ARTIFACT_CACHE_QUARANTINE_REQUEST_INVALID"
    )
    at = _aware(requested_at or utcnow())
    if at is None:  # pragma: no cover
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_TIMESTAMP_INVALID")
    _lock_node(session, node_id)
    _lock_source_task(session, source_task_id, node_id)
    cache = _lock_cache(session, node_id, cache_identity_digest)
    if cache is None:
        raise ArtifactCacheNotFoundError()
    evidence_digest = _canonical_digest(
        {
            "schema_version": 1,
            "request_id": request_id,
            "node_id": node_id,
            "cache_identity_digest": cache_identity_digest,
        }
    )
    replay = _event_replay(
        session,
        cache,
        source_kind="QUARANTINE",
        source_id=request_id,
        reason_code="QUARANTINE_REQUESTED",
        source_attempt_id=None,
        source_task_id=source_task_id,
        evidence_kind="QUARANTINE_REQUEST",
        evidence_digest=evidence_digest,
    )
    if replay is not None:
        return cache, replay, False
    if cache.status == "QUARANTINED":
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_ALREADY_QUARANTINED")
    if cache.quarantine_request_id is not None:
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_QUARANTINE_PENDING")
    previous = cache.status
    _priority_transition(
        cache, target_status="STALE", reason_code="QUARANTINE_REQUESTED"
    )
    cache.quarantine_request_id = request_id
    event = _append_event(
        session,
        cache,
        previous_status=previous,
        source_kind="QUARANTINE",
        source_id=request_id,
        reason_code="QUARANTINE_REQUESTED",
        source_attempt_id=None,
        source_task_id=source_task_id,
        evidence_kind="QUARANTINE_REQUEST",
        evidence_digest=evidence_digest,
        now=at,
    )
    return cache, event, True


def complete_cache_quarantine(
    session: Session,
    *,
    node_id: str,
    cache_identity_digest: str,
    request_id: str,
    succeeded: bool,
    source_task_id: str | None = None,
    completed_at: datetime | None = None,
) -> tuple[NodeArtifactCache, ArtifactCacheEvent, bool]:
    request_id = _canonical_uuid(
        request_id, code="ARTIFACT_CACHE_QUARANTINE_REQUEST_INVALID"
    )
    if type(succeeded) is not bool:
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_QUARANTINE_RESULT_INVALID")
    at = _aware(completed_at or utcnow())
    if at is None:  # pragma: no cover
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_TIMESTAMP_INVALID")
    _lock_node(session, node_id)
    _lock_source_task(session, source_task_id, node_id)
    cache = _lock_cache(session, node_id, cache_identity_digest)
    if cache is None:
        raise ArtifactCacheNotFoundError()
    reason = "QUARANTINE_SUCCEEDED" if succeeded else "QUARANTINE_FAILED"
    evidence_digest = _canonical_digest(
        {
            "schema_version": 1,
            "request_id": request_id,
            "node_id": node_id,
            "cache_identity_digest": cache_identity_digest,
            "status": "SUCCEEDED" if succeeded else "FAILED",
        }
    )
    replay = _event_replay(
        session,
        cache,
        source_kind="QUARANTINE",
        source_id=request_id,
        reason_code=reason,
        source_attempt_id=None,
        source_task_id=source_task_id,
        evidence_kind="QUARANTINE_RESULT",
        evidence_digest=evidence_digest,
    )
    if replay is not None:
        return cache, replay, False
    opposite = session.scalar(
        select(ArtifactCacheEvent).where(
            ArtifactCacheEvent.cache_id == cache.id,
            ArtifactCacheEvent.source_kind == "QUARANTINE",
            ArtifactCacheEvent.source_id == request_id,
            ArtifactCacheEvent.reason_code.in_(
                {"QUARANTINE_SUCCEEDED", "QUARANTINE_FAILED"}
            ),
        )
    )
    if opposite is not None:
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_REPLAY_CONFLICT")
    if cache.quarantine_request_id != request_id:
        raise ArtifactCacheConflictError("ARTIFACT_CACHE_QUARANTINE_STALE")
    previous = cache.status
    if succeeded:
        cache.status = "QUARANTINED"
        cache.reason_code = reason
        cache.quarantined_at = at
    elif cache.status == "STALE" and cache.reason_code == "QUARANTINE_REQUESTED":
        cache.reason_code = reason
    cache.quarantine_request_id = None
    event = _append_event(
        session,
        cache,
        previous_status=previous,
        source_kind="QUARANTINE",
        source_id=request_id,
        reason_code=reason,
        source_attempt_id=None,
        source_task_id=source_task_id,
        evidence_kind="QUARANTINE_RESULT",
        evidence_digest=evidence_digest,
        now=at,
    )
    return cache, event, True


def _current_ready_attempt(
    session: Session, cache: NodeArtifactCache
) -> tuple[ArtifactPreparationAttempt, ArtifactPreparationNode, Task]:
    if cache.last_ready_attempt_id is None:
        raise ArtifactCacheNotReadyError("ARTIFACT_CACHE_READY_EVIDENCE_MISSING")
    attempt = session.get(ArtifactPreparationAttempt, cache.last_ready_attempt_id)
    if attempt is None:
        raise ArtifactCacheNotReadyError("ARTIFACT_CACHE_READY_EVIDENCE_MISSING")
    record = session.get(ArtifactPreparationNode, attempt.preparation_node_id)
    task = session.get(Task, attempt.task_id)
    if (
        record is None
        or task is None
        or attempt.stage != "MODEL"
        or attempt.status != "SUCCEEDED"
        or attempt.completed_at is None
        or record.node_id != cache.node_id
        or record.model_manifest_digest != cache.manifest_digest
        or record.model_status != "SUCCEEDED"
        or record.model_current_attempt != attempt.attempt_no
        or task.id != attempt.task_id
        or task.node_id != cache.node_id
        or task.type != TaskType.PREPARE_MODEL.value
        or task.status != TaskStatus.SUCCEEDED.value
    ):
        raise ArtifactCacheNotReadyError("ARTIFACT_CACHE_READY_ATTEMPT_STALE")
    return attempt, record, task


def require_ready_cache(
    session: Session,
    *,
    node_id: str,
    identity: ArtifactCacheIdentity,
    lock: bool = False,
) -> NodeArtifactCache:
    identity.validate()
    if lock:
        _lock_node(session, node_id)
    # A validated STAGE row is locked before its cache row, matching
    # preparation (Node/Task/Attempt -> Variant -> Cache) and revocation
    # (Variant -> Cache). This closes a VALIDATED -> REVOKED TOCTOU window.
    manifest = _validate_identity_database(
        session, identity, require_validated_stage=True
    )
    statement = select(NodeArtifactCache).where(
        NodeArtifactCache.node_id == node_id,
        NodeArtifactCache.cache_identity_digest == identity.cache_identity_digest,
    )
    if lock:
        statement = statement.with_for_update()
    cache = session.scalar(statement)
    if cache is None:
        raise ArtifactCacheNotFoundError()
    if not _cache_matches_identity(cache, identity):
        raise ArtifactCacheNotReadyError("ARTIFACT_CACHE_IDENTITY_MISMATCH")
    if cache.status != "READY":
        raise ArtifactCacheNotReadyError()
    _current_ready_attempt(session, cache)
    if (
        cache.reason_code != "PREPARATION_SUCCEEDED"
        or cache.verified_at is None
        or cache.verified_size_bytes != manifest.total_size_bytes
        or cache.verified_file_count != manifest.file_count
        or cache.verification_version != identity.verification_version
        or cache.quarantine_request_id is not None
        or cache.quarantined_at is not None
    ):
        raise ArtifactCacheNotReadyError("ARTIFACT_CACHE_READY_EVIDENCE_INVALID")
    return cache


def ready_cache_projection(
    session: Session,
    *,
    node_id: str,
    identity: ArtifactCacheIdentity,
) -> dict[str, Any]:
    cache = require_ready_cache(
        session, node_id=node_id, identity=identity, lock=False
    )
    projection: dict[str, Any] = {
        "schema_version": 1,
        "node_id": cache.node_id,
        "cache_kind": cache.cache_kind,
        "cache_identity_digest": cache.cache_identity_digest,
        "manifest_digest": cache.manifest_digest,
        "source_manifest_digest": cache.source_manifest_digest,
        "status": cache.status,
        "reason_code": cache.reason_code,
        "source_attempt_id": cache.last_ready_attempt_id,
        "verified_at": _iso(cache.verified_at),
        "bytes_verified": cache.verified_size_bytes,
        "file_count": cache.verified_file_count,
        "verification_version": cache.verification_version,
    }
    if cache.cache_kind == STAGE:
        projection.update(
            artifact_set_digest=cache.artifact_set_digest,
            pipeline_rank=cache.pipeline_rank,
            tensor_rank=cache.tensor_rank,
            tensor_parallel_size=cache.tensor_parallel_size,
            pipeline_parallel_size=cache.pipeline_parallel_size,
            tensor_keys_digest=cache.tensor_keys_digest,
        )
    return projection


def _cache_projection(cache: NodeArtifactCache) -> dict[str, Any]:
    value: dict[str, Any] = {
        "schema_version": 1,
        "id": cache.id,
        "node_id": cache.node_id,
        "cache_kind": cache.cache_kind,
        "cache_identity_digest": cache.cache_identity_digest,
        "manifest_digest": cache.manifest_digest,
        "source_manifest_digest": cache.source_manifest_digest,
        "status": cache.status,
        "reason_code": cache.reason_code,
        "last_ready_attempt_id": cache.last_ready_attempt_id,
        "verified_at": _iso(cache.verified_at) if cache.verified_at else None,
        "verified_size_bytes": cache.verified_size_bytes,
        "verified_file_count": cache.verified_file_count,
        "verification_version": cache.verification_version,
        "last_probe_observed_at": (
            _iso(cache.last_probe_observed_at)
            if cache.last_probe_observed_at
            else None
        ),
        "quarantine_request_id": cache.quarantine_request_id,
        "quarantined_at": (
            _iso(cache.quarantined_at) if cache.quarantined_at else None
        ),
        "event_sequence": cache.event_sequence,
        "created_at": _iso(cache.created_at),
        "updated_at": _iso(cache.updated_at),
    }
    if cache.cache_kind == STAGE:
        value.update(
            artifact_set_digest=cache.artifact_set_digest,
            pipeline_rank=cache.pipeline_rank,
            tensor_rank=cache.tensor_rank,
            tensor_parallel_size=cache.tensor_parallel_size,
            pipeline_parallel_size=cache.pipeline_parallel_size,
            tensor_keys_digest=cache.tensor_keys_digest,
        )
    return value


def artifact_cache_projection(
    session: Session, cache_id: str
) -> dict[str, Any] | None:
    cache = session.get(NodeArtifactCache, cache_id)
    return _cache_projection(cache) if cache is not None else None


def list_artifact_cache_projections(session: Session) -> list[dict[str, Any]]:
    caches = list(
        session.scalars(
            select(NodeArtifactCache).order_by(
                NodeArtifactCache.node_id,
                NodeArtifactCache.cache_identity_digest,
                NodeArtifactCache.id,
            )
        )
    )
    return [_cache_projection(cache) for cache in caches]


def _snapshot_cache_reference(
    snapshot: object, cache: NodeArtifactCache
) -> bool | None:
    if type(snapshot) is not dict:
        return None
    artifact = snapshot.get("artifact")
    node_ids = snapshot.get("node_ids")
    if type(artifact) is not dict or type(node_ids) is not list:
        return None
    if cache.node_id not in node_ids:
        return False
    cache_kind = artifact.get("cache_kind")
    if cache_kind == FULL_SNAPSHOT:
        manifest = artifact.get("manifest_digest")
        if type(manifest) is not str or _DIGEST.fullmatch(manifest) is None:
            return None
        return (
            cache.cache_kind == FULL_SNAPSHOT
            and cache.cache_identity_digest == manifest
            and cache.manifest_digest == manifest
        )
    if cache_kind != STAGE:
        return None
    stage = snapshot.get("stage_artifact")
    if type(stage) is not dict or type(stage.get("node_bindings")) is not list:
        return None
    bindings = [
        item
        for item in stage["node_bindings"]
        if type(item) is dict and item.get("node_id") == cache.node_id
    ]
    if len(bindings) != 1:
        return None
    binding = bindings[0]
    try:
        identity = StageCacheIdentity(
            repository=artifact["repository"],
            revision=artifact["revision"],
            manifest_digest=binding["manifest_digest"],
            quantization=artifact["quantization"],
            artifact_set_digest=stage["artifact_set_digest"],
            contract_identity_digest=stage["contract_identity_digest"],
            source_manifest_digest=stage["source_manifest_digest"],
            runtime_image=stage["runtime_image"],
            vllm_version=stage["vllm_version"],
            exporter_build_digest=stage["exporter_build_digest"],
            architecture=stage["architecture"],
            loader_format=stage["loader_format"],
            tensor_parallel_size=stage["tensor_parallel_size"],
            pipeline_parallel_size=stage["pipeline_parallel_size"],
            pipeline_rank=binding["pipeline_rank"],
            tensor_rank=binding["tensor_rank"],
            tensor_keys_digest=binding["tensor_keys_digest"],
        )
    except (KeyError, StageCacheError, TypeError, ValueError):
        return None
    return (
        cache.cache_kind == STAGE
        and cache.cache_identity_digest == identity.cache_identity_digest
        and cache.manifest_digest == identity.manifest_digest
        and cache.source_manifest_digest == identity.source_manifest_digest
        and cache.artifact_set_digest == identity.artifact_set_digest
        and cache.pipeline_rank == identity.pipeline_rank
        and cache.tensor_rank == identity.tensor_rank
        and cache.tensor_parallel_size == identity.tensor_parallel_size
        and cache.pipeline_parallel_size == identity.pipeline_parallel_size
        and cache.tensor_keys_digest == identity.tensor_keys_digest
    )


def _deployment_node_assigned(deployment: Deployment, node_id: str) -> bool | None:
    plan = deployment.plan
    if type(plan) is not dict or type(plan.get("assignments")) is not list:
        return None
    assignments = [
        item
        for item in plan["assignments"]
        if type(item) is dict and item.get("node_id") == node_id
    ]
    return bool(assignments)


_EXPLICITLY_CLOSED_OPERATION_STATUSES = frozenset(
    {"SUCCEEDED", "PARTIAL_FAILED", "FAILED"}
)


def _deployment_operation_explicitly_closed(
    operation: DeploymentOperation,
) -> bool:
    """Return whether rollout state proves that an operation is finished.

    Rollback failures intentionally retain ``active_lineage_id`` so an
    operator can retry them.  Consequently FAILED/PARTIAL_FAILED alone is
    not terminal evidence.  Unknown future states also remain open by
    default, which keeps quarantine fail-closed.
    """

    return (
        operation.status in _EXPLICITLY_CLOSED_OPERATION_STATUSES
        and operation.active_lineage_id is None
        and operation.completed_at is not None
    )


def artifact_cache_reference_projection(
    session: Session, cache_id: str
) -> dict[str, Any]:
    """Return a conservative, closed quarantine-reference projection.

    `complete=False` means the caller must refuse quarantine. Completed
    preparation evidence is provenance, not a reference by itself.
    """

    cache = session.get(NodeArtifactCache, cache_id)
    if cache is None:
        raise ArtifactCacheNotFoundError()
    references: set[tuple[str, str]] = set()
    complete = True

    active_task_types = {
        TaskType.BENCHMARK.value,
        TaskType.PREPARE_MODEL.value,
        TaskType.PREPARE_IMAGE.value,
        TaskType.QUARANTINE_ARTIFACT_CACHE.value,
        TaskType.VERIFY.value,
        TaskType.APPLY_DEPLOYMENT.value,
        TaskType.START_DEPLOYMENT.value,
        TaskType.STOP_DEPLOYMENT.value,
        TaskType.RESTART_DEPLOYMENT.value,
    }
    active_tasks = list(
        session.scalars(
            select(Task)
            .where(
                Task.node_id == cache.node_id,
                Task.type.in_(active_task_types),
                Task.status.in_(
                    {TaskStatus.QUEUED.value, TaskStatus.RUNNING.value}
                ),
            )
            .order_by(Task.id)
        )
    )
    references.update(("TASK", task.id) for task in active_tasks)

    operations = list(
        session.scalars(select(DeploymentOperation).order_by(DeploymentOperation.id))
    )
    for operation in operations:
        if _deployment_operation_explicitly_closed(operation):
            continue
        node_ids = operation.node_ids
        if (
            type(node_ids) is not list
            or not node_ids
            or any(type(node_id) is not str or not node_id for node_id in node_ids)
            or len(node_ids) != len(set(node_ids))
        ):
            # An open operation with an untrustworthy node set cannot be
            # proven unrelated to this cache.
            complete = False
            continue
        if cache.node_id in node_ids:
            references.add(("DEPLOYMENT_OPERATION", operation.id))

    preparations = list(
        session.scalars(
            select(ArtifactPreparation).order_by(ArtifactPreparation.id)
        )
    )
    preparation_by_deployment = {
        preparation.deployment_id: preparation for preparation in preparations
    }
    for preparation in preparations:
        if preparation.status not in {"PREPARED", "QUEUED", "RUNNING"}:
            continue
        referenced = _snapshot_cache_reference(preparation.plan_snapshot, cache)
        if referenced is None:
            snapshot = preparation.plan_snapshot
            if type(snapshot) is dict and cache.node_id in snapshot.get(
                "node_ids", []
            ):
                complete = False
        elif referenced:
            references.add(("ARTIFACT_PREPARATION", preparation.id))

    deployments = list(
        session.scalars(
            select(Deployment).order_by(
                Deployment.lineage_id, Deployment.generation, Deployment.id
            )
        )
    )
    by_id = {deployment.id: deployment for deployment in deployments}
    latest_by_lineage: dict[str, Deployment] = {}
    for deployment in deployments:
        lineage_id = deployment.lineage_id or deployment.id
        current = latest_by_lineage.get(lineage_id)
        if current is None or (deployment.generation, deployment.id) > (
            current.generation,
            current.id,
        ):
            latest_by_lineage[lineage_id] = deployment
    candidates: dict[str, Deployment] = {}
    for latest in latest_by_lineage.values():
        candidates[latest.id] = latest
        predecessor = (
            by_id.get(latest.previous_generation_id)
            if latest.previous_generation_id
            else None
        )
        if predecessor is not None and predecessor.status == "VERIFIED":
            candidates[predecessor.id] = predecessor
    for deployment in sorted(candidates.values(), key=lambda item: item.id):
        assigned = _deployment_node_assigned(deployment, cache.node_id)
        if assigned is None:
            complete = False
            continue
        if not assigned:
            continue
        preparation = preparation_by_deployment.get(deployment.id)
        if preparation is None:
            # A manual or malformed generation has no exact central cache
            # binding. Refuse quarantine instead of guessing from a host path.
            complete = False
            continue
        referenced = _snapshot_cache_reference(preparation.plan_snapshot, cache)
        if referenced is None:
            complete = False
        elif referenced:
            references.add(("DEPLOYMENT_GENERATION", deployment.id))

    return {
        "schema_version": 1,
        "cache_id": cache.id,
        "complete": complete,
        "blocking_references": [
            {"kind": kind, "id": reference_id}
            for kind, reference_id in sorted(references)
        ],
    }


def request_cache_quarantine_by_id(
    session: Session,
    *,
    cache_id: str,
    request_id: str,
    source_task_id: str | None = None,
    requested_at: datetime | None = None,
) -> tuple[NodeArtifactCache, ArtifactCacheEvent, bool]:
    cache = session.get(NodeArtifactCache, cache_id)
    if cache is None:
        raise ArtifactCacheNotFoundError()
    return request_cache_quarantine(
        session,
        node_id=cache.node_id,
        cache_identity_digest=cache.cache_identity_digest,
        request_id=request_id,
        source_task_id=source_task_id,
        requested_at=requested_at,
    )


def complete_cache_quarantine_by_id(
    session: Session,
    *,
    cache_id: str,
    request_id: str,
    succeeded: bool,
    source_task_id: str | None = None,
    completed_at: datetime | None = None,
) -> tuple[NodeArtifactCache, ArtifactCacheEvent, bool]:
    cache = session.get(NodeArtifactCache, cache_id)
    if cache is None:
        raise ArtifactCacheNotFoundError()
    return complete_cache_quarantine(
        session,
        node_id=cache.node_id,
        cache_identity_digest=cache.cache_identity_digest,
        request_id=request_id,
        succeeded=succeeded,
        source_task_id=source_task_id,
        completed_at=completed_at,
    )

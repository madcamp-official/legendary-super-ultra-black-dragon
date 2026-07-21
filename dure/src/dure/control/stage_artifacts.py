from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import timezone

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dure.artifact_prepare import validate_digest_pinned_runtime_image

from .models import (
    ArtifactFileChunk,
    ArtifactManifest,
    ArtifactManifestFile,
    ModelArtifact,
    RuntimeRelease,
    StageArtifactRank,
    StageArtifactValidationEvidence,
    StageArtifactValidationRank,
    StageArtifactVariant,
    utcnow,
)
from .cache_lifecycle import mark_stage_variant_revoked
from .service import (
    ArtifactManifestConflictError,
    _canonical_artifact_manifest,
    _ensure_artifact_chunks,
    artifact_manifest_dict,
    audit,
)


STAGE_ARTIFACT_ARCHITECTURE = "Qwen2ForCausalLM"
STAGE_ARTIFACT_QUANTIZATION = "awq"
STAGE_ARTIFACT_LOADER_FORMAT = "VLLM_SHARDED_STATE_V1"
STAGE_ARTIFACT_VLLM_VERSION = "0.9.0"
STAGE_ARTIFACT_MAX_PIPELINE_SIZE = 64
STAGE_ARTIFACT_EVIDENCE_FAILURE_CODES = {
    "STAGE_EXPORT_FAILED",
    "STAGE_LOAD_FAILED",
    "STAGE_TENSOR_COVERAGE_INVALID",
    "STAGE_MANIFEST_MISMATCH",
    "STAGE_TOPOLOGY_MISMATCH",
    "STAGE_GPU_NOT_AVAILABLE",
    "STAGE_VALIDATION_NOT_RUN",
}
STAGE_ARTIFACT_NOT_RUN_CODES = {
    "STAGE_GPU_NOT_AVAILABLE",
    "STAGE_VALIDATION_NOT_RUN",
}
_STAGE_REQUIRED_METADATA = {
    "config.json",
    "tokenizer.json",
    "tokenizer_config.json",
    "dure-stage.json",
}
_STAGE_OPTIONAL_METADATA = {
    "added_tokens.json",
    "chat_template.json",
    "generation_config.json",
    "merges.txt",
    "special_tokens_map.json",
    "vocab.json",
}
_STAGE_ALLOWED_METADATA = _STAGE_REQUIRED_METADATA | _STAGE_OPTIONAL_METADATA
_STAGE_WEIGHT_FILE = re.compile(
    r"model-rank-0-part-(0|[1-9][0-9]*)\.safetensors"
)


class StageArtifactNotFoundError(ValueError):
    pass


class StageArtifactConflictError(ValueError):
    pass


def _aware_iso(value) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def _require_digest(value: str, *, field: str) -> None:
    if re.fullmatch(r"sha256:[0-9a-f]{64}", value) is None:
        raise ValueError(f"{field} must be an immutable sha256 digest")


def _canonical_json(value: dict) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest_json(value: dict) -> tuple[str, str]:
    encoded = _canonical_json(value)
    digest = "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    return digest, encoded


def _manifest_is_exact(
    session: Session,
    record: ArtifactManifest,
    *,
    canonical: dict,
    canonical_json: str,
    total_size_bytes: int,
    file_count: int,
    chunk_count: int,
) -> bool:
    if (
        record.model_artifact_id is not None
        or record.schema_version != 1
        or record.canonical_json != canonical_json
        or record.total_size_bytes != total_size_bytes
        or record.file_count != file_count
        or record.chunk_count != chunk_count
    ):
        return False
    try:
        stored = artifact_manifest_dict(session, record)
    except ArtifactManifestConflictError:
        return False
    return stored["schema_version"] == 1 and stored["files"] == canonical["files"]


def _register_detached_manifest(
    session: Session,
    *,
    manifest: dict,
) -> tuple[ArtifactManifest, bool, dict]:
    (
        canonical,
        canonical_json,
        digest,
        total_size_bytes,
        file_count,
        chunk_count,
    ) = _canonical_artifact_manifest(manifest)
    existing = session.get(ArtifactManifest, digest)
    if existing is not None:
        if _manifest_is_exact(
            session,
            existing,
            canonical=canonical,
            canonical_json=canonical_json,
            total_size_bytes=total_size_bytes,
            file_count=file_count,
            chunk_count=chunk_count,
        ):
            return existing, False, canonical
        raise StageArtifactConflictError(
            "stage manifest digest is already bound to different or attached content"
        )

    chunk_sizes: dict[str, int] = {}
    for file_item in canonical["files"]:
        for chunk_item in file_item["chunks"]:
            chunk_sizes[chunk_item["sha256"]] = chunk_item["length_bytes"]
    record = ArtifactManifest(
        digest=digest,
        schema_version=1,
        model_artifact_id=None,
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
    except ArtifactManifestConflictError as exc:
        raise StageArtifactConflictError(
            "stage manifest chunks conflict with immutable registry data"
        ) from exc
    except IntegrityError as exc:
        session.expire_all()
        existing = session.get(ArtifactManifest, digest)
        if existing is not None and _manifest_is_exact(
            session,
            existing,
            canonical=canonical,
            canonical_json=canonical_json,
            total_size_bytes=total_size_bytes,
            file_count=file_count,
            chunk_count=chunk_count,
        ):
            return existing, False, canonical
        raise StageArtifactConflictError(
            "stage manifest registration conflicts with immutable registry data"
        ) from exc
    return record, True, canonical


def _contract_identity(
    *,
    source_manifest_digest: str,
    runtime_image: str,
    vllm_version: str,
    exporter_build_digest: str,
    architecture: str,
    quantization: str,
    tensor_parallel_size: int,
    pipeline_parallel_size: int,
    loader_format: str,
) -> dict:
    return {
        "schema_version": 1,
        "source_manifest_digest": source_manifest_digest,
        "runtime_image": runtime_image,
        "vllm_version": vllm_version,
        "exporter_build_digest": exporter_build_digest,
        "architecture": architecture,
        "quantization": quantization,
        "tensor_parallel_size": tensor_parallel_size,
        "pipeline_parallel_size": pipeline_parallel_size,
        "loader_format": loader_format,
    }


def _artifact_set_identity(contract: dict, stages: list[dict]) -> dict:
    return {
        "schema_version": 1,
        "contract": contract,
        "stages": [
            {
                "rank": item["rank"],
                "pipeline_rank": item["pipeline_rank"],
                "tensor_rank": item["tensor_rank"],
                "manifest_digest": item["manifest_digest"],
                "tensor_key_count": item["tensor_key_count"],
                "tensor_keys_digest": item["tensor_keys_digest"],
                "weight_size_bytes": item["weight_size_bytes"],
            }
            for item in sorted(stages, key=lambda value: value["rank"])
        ],
    }


def _validate_contract(
    session: Session,
    *,
    source_manifest_digest: str,
    runtime_image: str,
    vllm_version: str,
    exporter_build_digest: str,
    architecture: str,
    quantization: str,
    tensor_parallel_size: int,
    pipeline_parallel_size: int,
    loader_format: str,
) -> tuple[ArtifactManifest, RuntimeRelease, ModelArtifact]:
    _require_digest(source_manifest_digest, field="source_manifest_digest")
    _require_digest(exporter_build_digest, field="exporter_build_digest")
    try:
        validate_digest_pinned_runtime_image(runtime_image)
    except ValueError:
        raise ValueError("runtime_image must be a canonical digest-pinned OCI image") from None
    if vllm_version != STAGE_ARTIFACT_VLLM_VERSION:
        raise ValueError("unsupported stage artifact vLLM version")
    if architecture != STAGE_ARTIFACT_ARCHITECTURE:
        raise ValueError("unsupported stage artifact architecture")
    if quantization != STAGE_ARTIFACT_QUANTIZATION:
        raise ValueError("unsupported stage artifact quantization")
    if tensor_parallel_size != 1:
        raise ValueError("stage artifact export currently requires tensor_parallel_size=1")
    if not 1 <= pipeline_parallel_size <= STAGE_ARTIFACT_MAX_PIPELINE_SIZE:
        raise ValueError("pipeline_parallel_size is outside the supported range")
    if loader_format != STAGE_ARTIFACT_LOADER_FORMAT:
        raise ValueError("unsupported stage artifact loader format")

    source_manifest = session.get(ArtifactManifest, source_manifest_digest)
    if source_manifest is None or source_manifest.model_artifact_id is None:
        raise StageArtifactNotFoundError(
            "source manifest must be registered to a model artifact"
        )
    try:
        artifact_manifest_dict(session, source_manifest)
    except ArtifactManifestConflictError as exc:
        raise StageArtifactConflictError("source manifest is internally inconsistent") from exc
    artifact = session.get(ModelArtifact, source_manifest.model_artifact_id)
    if artifact is None or artifact.manifest_digest != source_manifest.digest:
        raise StageArtifactConflictError("source manifest model binding is inconsistent")
    if artifact.quantization != quantization:
        raise ValueError("source artifact quantization does not match the stage contract")

    runtime = session.scalar(
        select(RuntimeRelease).where(RuntimeRelease.image == runtime_image)
    )
    if runtime is None:
        raise StageArtifactNotFoundError("runtime image is not registered")
    if runtime.vllm_version != vllm_version:
        raise ValueError("runtime vLLM version does not match the stage contract")
    return source_manifest, runtime, artifact


def _canonical_stage_inputs(
    stages: list[dict],
    *,
    tensor_parallel_size: int,
    pipeline_parallel_size: int,
) -> list[dict]:
    expected_count = tensor_parallel_size * pipeline_parallel_size
    if len(stages) != expected_count:
        raise ValueError("stage ranks are incomplete for the requested topology")
    normalized: list[dict] = []
    coordinates: set[tuple[int, int]] = set()
    manifest_digests: set[str] = set()
    for item in stages:
        pipeline_rank = item["pipeline_rank"]
        tensor_rank = item["tensor_rank"]
        if not 0 <= pipeline_rank < pipeline_parallel_size:
            raise ValueError("pipeline rank is outside the requested topology")
        if not 0 <= tensor_rank < tensor_parallel_size:
            raise ValueError("tensor rank is outside the requested topology")
        coordinate = (pipeline_rank, tensor_rank)
        if coordinate in coordinates:
            raise ValueError("duplicate stage rank coordinate")
        coordinates.add(coordinate)
        rank = pipeline_rank * tensor_parallel_size + tensor_rank

        manifest_digest = item["manifest_digest"]
        tensor_keys_digest = item["tensor_keys_digest"]
        _require_digest(manifest_digest, field="manifest_digest")
        _require_digest(tensor_keys_digest, field="tensor_keys_digest")
        if manifest_digest in manifest_digests:
            raise ValueError("a stage manifest cannot be assigned to multiple ranks")
        manifest_digests.add(manifest_digest)
        if item["tensor_key_count"] <= 0 or item["weight_size_bytes"] <= 0:
            raise ValueError("stage tensor count and weight size must be positive")

        (
            canonical_manifest,
            _canonical_manifest_json,
            calculated_manifest_digest,
            total_size_bytes,
            _file_count,
            _chunk_count,
        ) = _canonical_artifact_manifest(item["manifest"])
        if calculated_manifest_digest != manifest_digest:
            raise ValueError("stage manifest digest does not match canonical content")
        weight_size_bytes = _validate_stage_manifest_file_set(canonical_manifest)
        if item["weight_size_bytes"] != weight_size_bytes:
            raise ValueError("stage weight size does not match its canonical manifest")
        if item["weight_size_bytes"] > total_size_bytes:
            raise ValueError("stage weight size exceeds the manifest size")
        normalized.append(
            {
                **item,
                "rank": rank,
                "canonical_manifest": canonical_manifest,
            }
        )

    expected_coordinates = {
        (pipeline_rank, tensor_rank)
        for pipeline_rank in range(pipeline_parallel_size)
        for tensor_rank in range(tensor_parallel_size)
    }
    if coordinates != expected_coordinates:
        raise ValueError("stage ranks are incomplete for the requested topology")
    return sorted(normalized, key=lambda value: value["rank"])


def _validate_stage_manifest_file_set(manifest: dict) -> int:
    metadata: set[str] = set()
    weight_parts: list[tuple[int, int]] = []
    for file_item in manifest["files"]:
        path = file_item["path"]
        if "/" in path or "\\" in path:
            raise ValueError("stage manifest files must be root-level regular files")
        match = _STAGE_WEIGHT_FILE.fullmatch(path)
        if match is not None:
            weight_parts.append((int(match.group(1)), file_item["size_bytes"]))
        elif path in _STAGE_ALLOWED_METADATA:
            metadata.add(path)
        else:
            raise ValueError("stage manifest contains an unsupported file")
    if not _STAGE_REQUIRED_METADATA <= metadata:
        raise ValueError("stage manifest is missing required loader metadata")
    weight_parts.sort()
    if [part for part, _size in weight_parts] != list(range(len(weight_parts))):
        raise ValueError("stage weight parts must be contiguous from zero")
    if not weight_parts:
        raise ValueError("stage manifest must contain native sharded-state weights")
    return sum(size for _part, size in weight_parts)


def _stored_ranks(session: Session, variant_id: str) -> list[StageArtifactRank]:
    return list(
        session.scalars(
            select(StageArtifactRank)
            .where(StageArtifactRank.variant_id == variant_id)
            .order_by(StageArtifactRank.rank)
        )
    )


def _validate_stored_variant(
    session: Session, variant: StageArtifactVariant
) -> tuple[dict, list[StageArtifactRank]]:
    try:
        _source, runtime, _artifact = _validate_contract(
            session,
            source_manifest_digest=variant.source_manifest_digest,
            runtime_image=variant.runtime_image,
            vllm_version=variant.vllm_version,
            exporter_build_digest=variant.exporter_build_digest,
            architecture=variant.architecture,
            quantization=variant.quantization,
            tensor_parallel_size=variant.tensor_parallel_size,
            pipeline_parallel_size=variant.pipeline_parallel_size,
            loader_format=variant.loader_format,
        )
    except (StageArtifactNotFoundError, StageArtifactConflictError, ValueError) as exc:
        raise StageArtifactConflictError(
            "stored stage contract registry binding is inconsistent"
        ) from exc
    if runtime.id != variant.runtime_release_id:
        raise StageArtifactConflictError(
            "stored stage runtime release binding is inconsistent"
        )
    contract = _contract_identity(
        source_manifest_digest=variant.source_manifest_digest,
        runtime_image=variant.runtime_image,
        vllm_version=variant.vllm_version,
        exporter_build_digest=variant.exporter_build_digest,
        architecture=variant.architecture,
        quantization=variant.quantization,
        tensor_parallel_size=variant.tensor_parallel_size,
        pipeline_parallel_size=variant.pipeline_parallel_size,
        loader_format=variant.loader_format,
    )
    contract_digest, _ = _digest_json(contract)
    if contract_digest != variant.contract_identity_digest:
        raise StageArtifactConflictError("stored stage contract identity is inconsistent")
    ranks = _stored_ranks(session, variant.artifact_set_digest)
    if len(ranks) != variant.rank_count:
        raise StageArtifactConflictError("stored stage rank set is incomplete")
    expected_ranks = list(range(variant.rank_count))
    if [item.rank for item in ranks] != expected_ranks:
        raise StageArtifactConflictError("stored stage ranks are duplicated or incomplete")
    stage_values: list[dict] = []
    for item in ranks:
        expected_rank = (
            item.pipeline_rank * variant.tensor_parallel_size + item.tensor_rank
        )
        if (
            item.rank != expected_rank
            or item.tensor_parallel_size != variant.tensor_parallel_size
            or item.pipeline_parallel_size != variant.pipeline_parallel_size
        ):
            raise StageArtifactConflictError("stored stage rank topology is inconsistent")
        manifest = session.get(ArtifactManifest, item.manifest_digest)
        if manifest is None or manifest.model_artifact_id is not None:
            raise StageArtifactConflictError("stored stage manifest is missing or attached")
        try:
            manifest_value = artifact_manifest_dict(session, manifest)
        except ArtifactManifestConflictError as exc:
            raise StageArtifactConflictError(
                "stored stage manifest is internally inconsistent"
            ) from exc
        try:
            weight_size = _validate_stage_manifest_file_set(manifest_value)
        except ValueError as exc:
            raise StageArtifactConflictError(
                "stored stage manifest file set is inconsistent"
            ) from exc
        if weight_size != item.weight_size_bytes:
            raise StageArtifactConflictError("stored stage weight size is inconsistent")
        stage_values.append(
            {
                "rank": item.rank,
                "pipeline_rank": item.pipeline_rank,
                "tensor_rank": item.tensor_rank,
                "manifest_digest": item.manifest_digest,
                "tensor_key_count": item.tensor_key_count,
                "tensor_keys_digest": item.tensor_keys_digest,
                "weight_size_bytes": item.weight_size_bytes,
            }
        )
    identity = _artifact_set_identity(contract, stage_values)
    artifact_set_digest, canonical_json = _digest_json(identity)
    if (
        artifact_set_digest != variant.artifact_set_digest
        or canonical_json != variant.canonical_identity_json
    ):
        raise StageArtifactConflictError("stored stage artifact set identity is inconsistent")
    return identity, ranks


def register_stage_artifact_variant(
    session: Session,
    *,
    source_manifest_digest: str,
    runtime_image: str,
    vllm_version: str,
    exporter_build_digest: str,
    architecture: str,
    quantization: str,
    tensor_parallel_size: int,
    pipeline_parallel_size: int,
    loader_format: str,
    stages: list[dict],
    commit: bool = True,
) -> tuple[StageArtifactVariant, bool]:
    _source, runtime, _artifact = _validate_contract(
        session,
        source_manifest_digest=source_manifest_digest,
        runtime_image=runtime_image,
        vllm_version=vllm_version,
        exporter_build_digest=exporter_build_digest,
        architecture=architecture,
        quantization=quantization,
        tensor_parallel_size=tensor_parallel_size,
        pipeline_parallel_size=pipeline_parallel_size,
        loader_format=loader_format,
    )
    normalized_stages = _canonical_stage_inputs(
        stages,
        tensor_parallel_size=tensor_parallel_size,
        pipeline_parallel_size=pipeline_parallel_size,
    )
    contract = _contract_identity(
        source_manifest_digest=source_manifest_digest,
        runtime_image=runtime_image,
        vllm_version=vllm_version,
        exporter_build_digest=exporter_build_digest,
        architecture=architecture,
        quantization=quantization,
        tensor_parallel_size=tensor_parallel_size,
        pipeline_parallel_size=pipeline_parallel_size,
        loader_format=loader_format,
    )
    contract_digest, _contract_json = _digest_json(contract)
    artifact_set = _artifact_set_identity(contract, normalized_stages)
    artifact_set_digest, artifact_set_json = _digest_json(artifact_set)

    existing_contract = session.scalar(
        select(StageArtifactVariant).where(
            StageArtifactVariant.contract_identity_digest == contract_digest
        )
    )
    if existing_contract is not None:
        if existing_contract.artifact_set_digest != artifact_set_digest:
            raise StageArtifactConflictError(
                "the immutable stage contract is already bound to different stage bytes"
            )
        _validate_stored_variant(session, existing_contract)
        return existing_contract, False

    try:
        with session.begin_nested():
            for item in normalized_stages:
                record, _created, _canonical = _register_detached_manifest(
                    session,
                    manifest=item["manifest"],
                )
                if record.digest != item["manifest_digest"]:
                    raise StageArtifactConflictError(
                        "registered stage manifest digest changed unexpectedly"
                    )
            variant = StageArtifactVariant(
                artifact_set_digest=artifact_set_digest,
                contract_identity_digest=contract_digest,
                source_manifest_digest=source_manifest_digest,
                runtime_release_id=runtime.id,
                runtime_image=runtime_image,
                vllm_version=vllm_version,
                exporter_build_digest=exporter_build_digest,
                architecture=architecture,
                quantization=quantization,
                tensor_parallel_size=tensor_parallel_size,
                pipeline_parallel_size=pipeline_parallel_size,
                rank_count=len(normalized_stages),
                loader_format=loader_format,
                status="DRAFT",
                canonical_identity_json=artifact_set_json,
            )
            session.add(variant)
            session.flush()
            session.add_all(
                StageArtifactRank(
                    variant_id=artifact_set_digest,
                    rank=item["rank"],
                    pipeline_rank=item["pipeline_rank"],
                    tensor_rank=item["tensor_rank"],
                    tensor_parallel_size=tensor_parallel_size,
                    pipeline_parallel_size=pipeline_parallel_size,
                    manifest_digest=item["manifest_digest"],
                    tensor_key_count=item["tensor_key_count"],
                    tensor_keys_digest=item["tensor_keys_digest"],
                    weight_size_bytes=item["weight_size_bytes"],
                )
                for item in normalized_stages
            )
            session.flush()
            audit(
                session,
                "admin",
                "stage_artifact_variant.create",
                artifact_set_digest,
                "success",
                contract_identity_digest=contract_digest,
                rank_count=len(normalized_stages),
            )
        if commit:
            session.commit()
    except IntegrityError as exc:
        session.rollback()
        existing_contract = session.scalar(
            select(StageArtifactVariant).where(
                StageArtifactVariant.contract_identity_digest == contract_digest
            )
        )
        if (
            existing_contract is not None
            and existing_contract.artifact_set_digest == artifact_set_digest
        ):
            _validate_stored_variant(session, existing_contract)
            return existing_contract, False
        raise StageArtifactConflictError(
            "stage artifact variant conflicts with immutable registry data"
        ) from exc
    return variant, True


def get_stage_artifact_variant(
    session: Session, artifact_set_digest: str
) -> StageArtifactVariant:
    variant = session.get(StageArtifactVariant, artifact_set_digest)
    if variant is None:
        raise StageArtifactNotFoundError("stage artifact variant not found")
    return variant


def list_stage_artifact_variants(session: Session) -> list[StageArtifactVariant]:
    return list(
        session.scalars(
            select(StageArtifactVariant).order_by(
                StageArtifactVariant.created_at,
                StageArtifactVariant.artifact_set_digest,
            )
        )
    )


def _evidence_ranks(
    session: Session, evidence_id: str
) -> list[StageArtifactValidationRank]:
    return list(
        session.scalars(
            select(StageArtifactValidationRank)
            .where(StageArtifactValidationRank.evidence_id == evidence_id)
            .order_by(StageArtifactValidationRank.rank)
        )
    )


def stage_artifact_evidence_dict(
    session: Session, evidence: StageArtifactValidationEvidence
) -> dict:
    ranks = _evidence_ranks(session, evidence.identity_digest)
    try:
        value = json.loads(evidence.canonical_evidence_json)
    except (TypeError, ValueError) as exc:
        raise StageArtifactConflictError(
            "stored stage validation evidence is not canonical JSON"
        ) from exc
    if type(value) is not dict or type(value.get("ranks")) is not list:
        raise StageArtifactConflictError(
            "stored stage validation evidence has an invalid closed shape"
        )
    digest, canonical_json = _digest_json(value)
    if digest != evidence.identity_digest or canonical_json != evidence.canonical_evidence_json:
        raise StageArtifactConflictError("stored stage validation evidence is inconsistent")
    if (
        value.get("variant_identity_digest") != evidence.variant_id
        or value.get("validation_run_id") != evidence.validation_run_id
        or value.get("schema_version") != evidence.schema_version
        or value.get("kind") != evidence.kind
        or value.get("status") != evidence.status
        or value.get("validator_version") != evidence.validator_version
        or value.get("validator_build_digest") != evidence.validator_build_digest
        or value.get("failure_code") != evidence.failure_code
    ):
        raise StageArtifactConflictError("stored stage validation evidence binding is inconsistent")
    canonical_ranks = value["ranks"]
    stored_ranks = [
        {
            "rank": item.rank,
            "manifest_digest": item.manifest_digest,
            "tensor_keys_digest": item.tensor_keys_digest,
            "loaded_tensor_count": item.loaded_tensor_count,
            "loaded_weight_size_bytes": item.loaded_weight_size_bytes,
        }
        for item in ranks
    ]
    if canonical_ranks != stored_ranks or evidence.rank_count != len(ranks):
        raise StageArtifactConflictError("stored stage validation ranks are inconsistent")
    return {
        "identity_digest": evidence.identity_digest,
        "variant_id": evidence.variant_id,
        "validation_run_id": evidence.validation_run_id,
        "registration_sequence": evidence.registration_sequence,
        "schema_version": evidence.schema_version,
        "kind": evidence.kind,
        "status": evidence.status,
        "validator_version": evidence.validator_version,
        "validator_build_digest": evidence.validator_build_digest,
        "rank_count": evidence.rank_count,
        "failure_code": evidence.failure_code,
        "ranks": stored_ranks,
        "created_at": _aware_iso(evidence.created_at),
    }


def stage_artifact_variant_dict(
    session: Session, variant: StageArtifactVariant
) -> dict:
    identity, ranks = _validate_stored_variant(session, variant)
    evidence = list(
        session.scalars(
            select(StageArtifactValidationEvidence)
            .where(StageArtifactValidationEvidence.variant_id == variant.artifact_set_digest)
            .order_by(StageArtifactValidationEvidence.registration_sequence)
        )
    )
    return {
        "artifact_set_digest": variant.artifact_set_digest,
        "contract_identity_digest": variant.contract_identity_digest,
        "source_manifest_digest": variant.source_manifest_digest,
        "runtime_release_id": variant.runtime_release_id,
        "runtime_image": variant.runtime_image,
        "vllm_version": variant.vllm_version,
        "exporter_build_digest": variant.exporter_build_digest,
        "architecture": variant.architecture,
        "quantization": variant.quantization,
        "tensor_parallel_size": variant.tensor_parallel_size,
        "pipeline_parallel_size": variant.pipeline_parallel_size,
        "rank_count": variant.rank_count,
        "loader_format": variant.loader_format,
        "status": variant.status,
        "stages": identity["stages"],
        "evidence": [stage_artifact_evidence_dict(session, item) for item in evidence],
        "created_at": _aware_iso(variant.created_at),
        "updated_at": _aware_iso(variant.updated_at),
        "validated_at": _aware_iso(variant.validated_at),
        "revoked_at": _aware_iso(variant.revoked_at),
    }


def validated_stage_artifact_projection(
    session: Session,
    artifact_set_digest: str,
) -> dict:
    """Return the immutable deployment projection for one validated variant.

    A caller may only select a variant by its exact artifact-set digest.  This
    helper deliberately performs no latest/preferred lookup and returns no
    mutable timestamps or evidence history.  It does, however, revalidate the
    complete stored variant and its latest GPU export/load evidence before
    exposing any rank manifest to preparation code.  The exact variant row is
    locked through the caller's transaction so a concurrent REVOKED transition
    cannot pass a deployment mutation gate after this projection is returned.
    """

    _require_digest(artifact_set_digest, field="artifact_set_digest")
    variant = session.scalar(
        select(StageArtifactVariant)
        .where(
            StageArtifactVariant.artifact_set_digest
            == artifact_set_digest
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if variant is None:
        raise StageArtifactNotFoundError("stage artifact variant not found")

    _identity, ranks = _validate_stored_variant(session, variant)
    if (
        variant.status != "VALIDATED"
        or variant.validated_at is None
        or variant.revoked_at is not None
    ):
        raise StageArtifactConflictError(
            "stage artifact variant is not currently VALIDATED"
        )

    latest = session.scalar(
        select(StageArtifactValidationEvidence)
        .where(
            StageArtifactValidationEvidence.variant_id == artifact_set_digest,
            StageArtifactValidationEvidence.kind == "GPU_EXPORT_LOAD",
        )
        .order_by(
            StageArtifactValidationEvidence.registration_sequence.desc(),
            StageArtifactValidationEvidence.identity_digest.desc(),
        )
        .limit(1)
    )
    if latest is None or latest.status != "PASSED":
        raise StageArtifactConflictError(
            "latest GPU export/load validation evidence must be PASSED"
        )
    evidence = stage_artifact_evidence_dict(session, latest)
    expected_evidence_ranks = [
        {
            "rank": item.rank,
            "manifest_digest": item.manifest_digest,
            "tensor_keys_digest": item.tensor_keys_digest,
            "loaded_tensor_count": item.tensor_key_count,
            "loaded_weight_size_bytes": item.weight_size_bytes,
        }
        for item in ranks
    ]
    if (
        evidence["kind"] != "GPU_EXPORT_LOAD"
        or evidence["status"] != "PASSED"
        or evidence["failure_code"] is not None
        or evidence["rank_count"] != len(ranks)
        or evidence["ranks"] != expected_evidence_ranks
    ):
        raise StageArtifactConflictError(
            "latest GPU export/load evidence does not cover every stage rank"
        )

    projected_ranks: list[dict] = []
    for item in ranks:
        manifest = session.get(ArtifactManifest, item.manifest_digest)
        if manifest is None or manifest.model_artifact_id is not None:
            raise StageArtifactConflictError(
                "stored stage manifest is missing or attached"
            )
        try:
            manifest_value = artifact_manifest_dict(session, manifest)
        except ArtifactManifestConflictError as exc:
            raise StageArtifactConflictError(
                "stored stage manifest is internally inconsistent"
            ) from exc
        projected_ranks.append(
            {
                "rank": item.rank,
                "pipeline_rank": item.pipeline_rank,
                "tensor_rank": item.tensor_rank,
                "manifest_digest": item.manifest_digest,
                "tensor_key_count": item.tensor_key_count,
                "tensor_keys_digest": item.tensor_keys_digest,
                "weight_size_bytes": item.weight_size_bytes,
                "total_size_bytes": manifest_value["total_size_bytes"],
                "file_count": manifest_value["file_count"],
            }
        )

    return {
        "artifact_set_digest": variant.artifact_set_digest,
        "contract_identity_digest": variant.contract_identity_digest,
        "source_manifest_digest": variant.source_manifest_digest,
        "runtime_image": variant.runtime_image,
        "vllm_version": variant.vllm_version,
        "exporter_build_digest": variant.exporter_build_digest,
        "architecture": variant.architecture,
        "quantization": variant.quantization,
        "tensor_parallel_size": variant.tensor_parallel_size,
        "pipeline_parallel_size": variant.pipeline_parallel_size,
        "loader_format": variant.loader_format,
        "ranks": projected_ranks,
    }


def _canonical_evidence_ranks(
    ranks: list[dict],
    *,
    stored_ranks: list[StageArtifactRank],
    require_complete: bool,
) -> list[dict]:
    by_coordinate = {
        (item.pipeline_rank, item.tensor_rank): item for item in stored_ranks
    }
    normalized: list[dict] = []
    seen: set[int] = set()
    for item in ranks:
        stored = by_coordinate.get((item["pipeline_rank"], item["tensor_rank"]))
        if stored is None:
            raise ValueError("validation evidence contains an unknown stage rank")
        if stored.rank in seen:
            raise ValueError("validation evidence contains a duplicate stage rank")
        seen.add(stored.rank)
        if (
            item["manifest_digest"] != stored.manifest_digest
            or item["tensor_keys_digest"] != stored.tensor_keys_digest
        ):
            raise ValueError("validation evidence does not match the registered stage identity")
        if (
            item["loaded_tensor_count"] != stored.tensor_key_count
            or item["loaded_weight_size_bytes"] != stored.weight_size_bytes
        ):
            raise ValueError("validation evidence does not cover the complete registered stage")
        normalized.append(
            {
                "rank": stored.rank,
                "manifest_digest": stored.manifest_digest,
                "tensor_keys_digest": stored.tensor_keys_digest,
                "loaded_tensor_count": item["loaded_tensor_count"],
                "loaded_weight_size_bytes": item["loaded_weight_size_bytes"],
            }
        )
    normalized.sort(key=lambda value: value["rank"])
    if require_complete and [item["rank"] for item in normalized] != list(
        range(len(stored_ranks))
    ):
        raise ValueError("passing validation evidence must cover every stage rank")
    return normalized


def register_stage_artifact_evidence(
    session: Session,
    artifact_set_digest: str,
    *,
    schema_version: int,
    variant_identity_digest: str,
    validation_run_id: str,
    kind: str,
    status: str,
    validator_version: str,
    validator_build_digest: str,
    failure_code: str | None,
    ranks: list[dict],
    commit: bool = True,
) -> tuple[StageArtifactValidationEvidence, bool]:
    variant = session.scalar(
        select(StageArtifactVariant)
        .where(StageArtifactVariant.artifact_set_digest == artifact_set_digest)
        .with_for_update()
    )
    if variant is None:
        raise StageArtifactNotFoundError("stage artifact variant not found")
    if schema_version != 1:
        raise ValueError("unsupported stage validation evidence schema")
    if variant_identity_digest != artifact_set_digest:
        raise ValueError("validation evidence is bound to a different artifact set")
    try:
        parsed_run_id = uuid.UUID(validation_run_id)
    except (ValueError, AttributeError):
        raise ValueError("validation_run_id must be a canonical UUIDv4") from None
    if parsed_run_id.version != 4 or str(parsed_run_id) != validation_run_id:
        raise ValueError("validation_run_id must be a canonical UUIDv4")
    if kind not in {"SYNTHETIC", "GPU_EXPORT_LOAD"}:
        raise ValueError("unsupported stage validation evidence kind")
    if status not in {"PASSED", "FAILED", "NOT_RUN"}:
        raise ValueError("unsupported stage validation evidence status")
    if re.fullmatch(r"[0-9A-Za-z][0-9A-Za-z._+-]{0,63}", validator_version) is None:
        raise ValueError("invalid stage validator version")
    _require_digest(validator_build_digest, field="validator_build_digest")
    if status == "PASSED":
        if failure_code is not None:
            raise ValueError("PASSED stage evidence cannot have a failure code")
    else:
        if failure_code not in STAGE_ARTIFACT_EVIDENCE_FAILURE_CODES:
            raise ValueError("failed stage evidence requires a closed failure code")
        if status == "NOT_RUN" and failure_code not in STAGE_ARTIFACT_NOT_RUN_CODES:
            raise ValueError("NOT_RUN stage evidence requires a not-run failure code")

    _identity, stored_ranks = _validate_stored_variant(session, variant)
    normalized_ranks = _canonical_evidence_ranks(
        ranks,
        stored_ranks=stored_ranks,
        require_complete=status == "PASSED",
    )
    evidence_value = {
        "schema_version": 1,
        "variant_identity_digest": artifact_set_digest,
        "validation_run_id": validation_run_id,
        "kind": kind,
        "status": status,
        "validator_version": validator_version,
        "validator_build_digest": validator_build_digest,
        "failure_code": failure_code,
        "ranks": normalized_ranks,
    }
    evidence_digest, evidence_json = _digest_json(evidence_value)
    existing = session.scalar(
        select(StageArtifactValidationEvidence).where(
            StageArtifactValidationEvidence.variant_id == artifact_set_digest,
            StageArtifactValidationEvidence.validation_run_id == validation_run_id,
        )
    )
    if existing is not None:
        if existing.identity_digest != evidence_digest:
            raise StageArtifactConflictError(
                "validation_run_id is already bound to different immutable evidence"
            )
        stage_artifact_evidence_dict(session, existing)
        return existing, False
    if variant.status != "DRAFT":
        raise StageArtifactConflictError(
            "new validation evidence can only be registered for a DRAFT variant"
        )
    digest_collision = session.get(StageArtifactValidationEvidence, evidence_digest)
    if digest_collision is not None:
        raise StageArtifactConflictError(
            "validation evidence digest is already bound to another validation run"
        )

    latest_sequence = session.scalar(
        select(func.max(StageArtifactValidationEvidence.registration_sequence)).where(
            StageArtifactValidationEvidence.variant_id == artifact_set_digest
        )
    )
    sequence = int(latest_sequence or 0) + 1
    evidence = StageArtifactValidationEvidence(
        identity_digest=evidence_digest,
        variant_id=artifact_set_digest,
        validation_run_id=validation_run_id,
        registration_sequence=sequence,
        schema_version=1,
        kind=kind,
        status=status,
        validator_version=validator_version,
        validator_build_digest=validator_build_digest,
        rank_count=len(normalized_ranks),
        failure_code=failure_code,
        canonical_evidence_json=evidence_json,
    )
    session.add(evidence)
    session.flush()
    session.add_all(
        StageArtifactValidationRank(
            evidence_id=evidence_digest,
            rank=item["rank"],
            variant_id=artifact_set_digest,
            manifest_digest=item["manifest_digest"],
            tensor_keys_digest=item["tensor_keys_digest"],
            loaded_tensor_count=item["loaded_tensor_count"],
            loaded_weight_size_bytes=item["loaded_weight_size_bytes"],
        )
        for item in normalized_ranks
    )
    try:
        session.flush()
    except IntegrityError as exc:
        session.rollback()
        raise StageArtifactConflictError(
            "validation evidence conflicts with immutable stage rank data"
        ) from exc
    audit(
        session,
        "admin",
        "stage_artifact_evidence.create",
        evidence_digest,
        "success",
        variant_id=artifact_set_digest,
        kind=kind,
        status=status,
        registration_sequence=sequence,
    )
    if commit:
        session.commit()
    return evidence, True


def transition_stage_artifact_variant(
    session: Session,
    artifact_set_digest: str,
    target_status: str,
    *,
    commit: bool = True,
) -> StageArtifactVariant:
    variant = session.scalar(
        select(StageArtifactVariant)
        .where(StageArtifactVariant.artifact_set_digest == artifact_set_digest)
        .with_for_update()
    )
    if variant is None:
        raise StageArtifactNotFoundError("stage artifact variant not found")
    if target_status not in {"DRAFT", "VALIDATED", "REVOKED"}:
        raise ValueError("unknown stage artifact variant status")
    same_status = target_status == variant.status
    if same_status and target_status == "DRAFT":
        return variant
    if same_status and target_status == "REVOKED":
        mark_stage_variant_revoked(
            session,
            artifact_set_digest=artifact_set_digest,
            revoked_at=variant.revoked_at,
        )
        if commit:
            session.commit()
        return variant
    allowed = {
        "DRAFT": {"VALIDATED", "REVOKED"},
        "VALIDATED": {"REVOKED"},
        "REVOKED": set(),
    }
    if not same_status and target_status not in allowed[variant.status]:
        raise StageArtifactConflictError(
            f"invalid stage artifact transition: {variant.status} -> {target_status}"
        )
    _identity, stored_ranks = _validate_stored_variant(session, variant)
    now = utcnow()
    if target_status == "VALIDATED":
        latest = session.scalar(
            select(StageArtifactValidationEvidence)
            .where(
                StageArtifactValidationEvidence.variant_id == artifact_set_digest,
                StageArtifactValidationEvidence.kind == "GPU_EXPORT_LOAD",
            )
            .order_by(
                StageArtifactValidationEvidence.registration_sequence.desc(),
                StageArtifactValidationEvidence.identity_digest.desc(),
            )
            .limit(1)
        )
        if latest is None or latest.status != "PASSED":
            raise StageArtifactConflictError(
                "latest GPU export/load validation evidence must be PASSED"
            )
        evidence_value = stage_artifact_evidence_dict(session, latest)
        if evidence_value["rank_count"] != len(stored_ranks):
            raise StageArtifactConflictError(
                "GPU validation evidence does not cover every stage rank"
            )
        if same_status:
            return variant
        variant.status = "VALIDATED"
        variant.validated_at = now
    else:
        variant.status = "REVOKED"
        variant.revoked_at = now
    variant.updated_at = now
    if target_status == "REVOKED":
        session.flush()
        mark_stage_variant_revoked(
            session,
            artifact_set_digest=artifact_set_digest,
            revoked_at=now,
        )
    audit(
        session,
        "admin",
        "stage_artifact_variant.transition",
        artifact_set_digest,
        "success",
        current=target_status,
    )
    if commit:
        session.commit()
    return variant

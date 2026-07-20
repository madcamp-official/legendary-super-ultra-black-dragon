from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .artifact_manifest import (
    CanonicalArtifactManifest,
    parse_artifact_manifest,
    require_sha256_digest,
)


STAGE_CACHE_VERIFICATION_VERSION = 1
STAGE_CACHE_IDENTITY_SCHEMA_VERSION = 1
STAGE_CACHE_MANIFEST_FILE = ".dure-stage-manifest.json"
STAGE_CACHE_ROOT_DIRECTORY = "stages"
STAGE_CACHE_KIND = "STAGE"
STAGE_MARKER_FILE = "dure-stage.json"
STAGE_MARKER_KIND = "VLLM_SHARDED_STATE_PIPELINE_STAGE"
STAGE_MARKER_SCHEMA_VERSION = 1
STAGE_NATIVE_LOADER_FORMAT = "sharded_state"
STAGE_CONTROL_LOADER_FORMAT = "VLLM_SHARDED_STATE_V1"
STAGE_VLLM_VERSION = "0.9.0"
STAGE_ARCHITECTURE = "Qwen2ForCausalLM"
STAGE_MODEL_FAMILY = "qwen2.5"
STAGE_MAX_PART_BYTES = 5 * 1024**3
STAGE_MAX_PIPELINE_SIZE = 64
STAGE_MARKER_MAX_BYTES = 4 * 1024 * 1024
STAGE_SIDECAR_MAX_BYTES = 64 * 1024 * 1024

_REPOSITORY = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
_REVISION = re.compile(r"[0-9a-f]{40,64}")
_QUANTIZATION = re.compile(r"[a-z0-9][a-z0-9._-]{1,39}")
_OCI_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
_OCI_NAME = re.compile(r"[a-z0-9](?:[a-z0-9._:/-]*[a-z0-9])?")
_TENSOR_NAME = re.compile(r"[^\x00-\x1f\x7f]{1,1024}")
_TENSOR_DTYPES = frozenset(
    {
        "BOOL",
        "U8",
        "I8",
        "F8_E4M3",
        "F8_E5M2",
        "U16",
        "I16",
        "F16",
        "BF16",
        "U32",
        "I32",
        "F32",
        "U64",
        "I64",
        "F64",
    }
)
_WEIGHT_FILE = re.compile(r"model-rank-0-part-(0|[1-9][0-9]*)\.safetensors")
_REQUIRED_METADATA = frozenset(
    {"config.json", "tokenizer.json", "tokenizer_config.json", STAGE_MARKER_FILE}
)
_OPTIONAL_METADATA = frozenset(
    {
        "added_tokens.json",
        "chat_template.json",
        "generation_config.json",
        "merges.txt",
        "special_tokens_map.json",
        "vocab.json",
    }
)
_ALLOWED_METADATA = _REQUIRED_METADATA | _OPTIONAL_METADATA
_IDENTITY_FIELDS = frozenset(
    {
        "schema_version",
        "cache_kind",
        "repository",
        "revision",
        "manifest_digest",
        "quantization",
        "artifact_set_digest",
        "contract_identity_digest",
        "source_manifest_digest",
        "runtime_image",
        "vllm_version",
        "exporter_build_digest",
        "architecture",
        "loader_format",
        "tensor_parallel_size",
        "pipeline_parallel_size",
        "pipeline_rank",
        "tensor_rank",
        "tensor_keys_digest",
    }
)
_STAGE_MARKER_FIELDS = frozenset(
    {
        "schema_version",
        "kind",
        "contract",
        "pipeline_rank",
        "weight_pattern",
        "metadata_files",
        "tensors",
        "tensor_key_digest",
    }
)
_STAGE_CONTRACT_FIELDS = frozenset(
    {
        "schema_version",
        "source_manifest_digest",
        "runtime_image",
        "exporter_build_digest",
        "model_family",
        "architecture",
        "quantization",
        "tensor_parallel_size",
        "pipeline_parallel_size",
        "loader_format",
        "vllm_version",
        "max_part_bytes",
        "trust_remote_code",
        "enable_lora",
        "is_moe",
        "is_multimodal",
    }
)
_CLOEXEC = getattr(os, "O_CLOEXEC", 0)
_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_NONBLOCK = getattr(os, "O_NONBLOCK", 0)


class StageCacheError(ValueError):
    pass


def _exact_dict(value: object, fields: frozenset[str], field: str) -> dict:
    if (
        type(value) is not dict
        or any(type(key) is not str for key in value)
        or set(value) != fields
    ):
        raise StageCacheError(f"{field} must use the closed schema")
    return value


def _canonical_runtime_image(value: object) -> str:
    if (
        type(value) is not str
        or not 1 <= len(value) <= 512
        or value.count("@") != 1
        or any(ord(character) < 0x21 or ord(character) > 0x7E for character in value)
        or "\\" in value
    ):
        raise StageCacheError("stage runtime image is invalid")
    name, digest = value.rsplit("@", 1)
    segments = name.split("/")
    if (
        _OCI_NAME.fullmatch(name) is None
        or _OCI_DIGEST.fullmatch(digest) is None
        or any(segment in {"", ".", ".."} for segment in segments)
        or ":" in segments[-1]
        or "//" in name
    ):
        raise StageCacheError("stage runtime image is invalid")
    return value


def stage_contract_identity_digest(
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
) -> str:
    document = {
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
    encoded = json.dumps(
        document, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class StageCacheIdentity:
    repository: str
    revision: str
    manifest_digest: str
    quantization: str
    artifact_set_digest: str
    contract_identity_digest: str
    source_manifest_digest: str
    runtime_image: str
    vllm_version: str
    exporter_build_digest: str
    architecture: str
    loader_format: str
    tensor_parallel_size: int
    pipeline_parallel_size: int
    pipeline_rank: int
    tensor_rank: int
    tensor_keys_digest: str

    def __post_init__(self) -> None:
        try:
            for field in (
                "manifest_digest",
                "artifact_set_digest",
                "contract_identity_digest",
                "source_manifest_digest",
                "exporter_build_digest",
                "tensor_keys_digest",
            ):
                require_sha256_digest(getattr(self, field), field=field)
        except ValueError as exc:
            raise StageCacheError("stage cache digest identity is invalid") from exc
        if (
            type(self.repository) is not str
            or _REPOSITORY.fullmatch(self.repository) is None
            or type(self.revision) is not str
            or _REVISION.fullmatch(self.revision) is None
            or type(self.quantization) is not str
            or _QUANTIZATION.fullmatch(self.quantization) is None
        ):
            raise StageCacheError("stage model identity is invalid")
        _canonical_runtime_image(self.runtime_image)
        if (
            self.vllm_version != STAGE_VLLM_VERSION
            or self.architecture != STAGE_ARCHITECTURE
            or self.loader_format != STAGE_CONTROL_LOADER_FORMAT
            or self.quantization != "awq"
            or type(self.tensor_parallel_size) is not int
            or self.tensor_parallel_size != 1
            or type(self.pipeline_parallel_size) is not int
            or not 1 <= self.pipeline_parallel_size <= STAGE_MAX_PIPELINE_SIZE
            or type(self.pipeline_rank) is not int
            or not 0 <= self.pipeline_rank < self.pipeline_parallel_size
            or type(self.tensor_rank) is not int
            or not 0 <= self.tensor_rank < self.tensor_parallel_size
        ):
            raise StageCacheError("stage runtime topology is unsupported")
        calculated_contract = stage_contract_identity_digest(
            source_manifest_digest=self.source_manifest_digest,
            runtime_image=self.runtime_image,
            vllm_version=self.vllm_version,
            exporter_build_digest=self.exporter_build_digest,
            architecture=self.architecture,
            quantization=self.quantization,
            tensor_parallel_size=self.tensor_parallel_size,
            pipeline_parallel_size=self.pipeline_parallel_size,
            loader_format=self.loader_format,
        )
        if self.contract_identity_digest != calculated_contract:
            raise StageCacheError("stage contract identity digest is inconsistent")

    @property
    def cache_kind(self) -> str:
        return STAGE_CACHE_KIND

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": STAGE_CACHE_IDENTITY_SCHEMA_VERSION,
            "cache_kind": STAGE_CACHE_KIND,
            "repository": self.repository,
            "revision": self.revision,
            "manifest_digest": self.manifest_digest,
            "quantization": self.quantization,
            "artifact_set_digest": self.artifact_set_digest,
            "contract_identity_digest": self.contract_identity_digest,
            "source_manifest_digest": self.source_manifest_digest,
            "runtime_image": self.runtime_image,
            "vllm_version": self.vllm_version,
            "exporter_build_digest": self.exporter_build_digest,
            "architecture": self.architecture,
            "loader_format": self.loader_format,
            "tensor_parallel_size": self.tensor_parallel_size,
            "pipeline_parallel_size": self.pipeline_parallel_size,
            "pipeline_rank": self.pipeline_rank,
            "tensor_rank": self.tensor_rank,
            "tensor_keys_digest": self.tensor_keys_digest,
        }

    @classmethod
    def from_dict(cls, value: object) -> "StageCacheIdentity":
        source = _exact_dict(value, _IDENTITY_FIELDS, "stage cache identity")
        if (
            type(source["schema_version"]) is not int
            or source["schema_version"] != STAGE_CACHE_IDENTITY_SCHEMA_VERSION
            or source["cache_kind"] != STAGE_CACHE_KIND
        ):
            raise StageCacheError("stage cache identity version is unsupported")
        return cls(
            repository=source["repository"],
            revision=source["revision"],
            manifest_digest=source["manifest_digest"],
            quantization=source["quantization"],
            artifact_set_digest=source["artifact_set_digest"],
            contract_identity_digest=source["contract_identity_digest"],
            source_manifest_digest=source["source_manifest_digest"],
            runtime_image=source["runtime_image"],
            vllm_version=source["vllm_version"],
            exporter_build_digest=source["exporter_build_digest"],
            architecture=source["architecture"],
            loader_format=source["loader_format"],
            tensor_parallel_size=source["tensor_parallel_size"],
            pipeline_parallel_size=source["pipeline_parallel_size"],
            pipeline_rank=source["pipeline_rank"],
            tensor_rank=source["tensor_rank"],
            tensor_keys_digest=source["tensor_keys_digest"],
        )

    @property
    def cache_identity_digest(self) -> str:
        encoded = json.dumps(
            self.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        return "sha256:" + hashlib.sha256(encoded).hexdigest()


def stage_cache_path(
    identity: StageCacheIdentity,
    *,
    model_root: Path = Path("/var/lib/dure/models"),
) -> Path:
    if type(identity) is not StageCacheIdentity:
        raise StageCacheError("stage cache identity is required")
    root = Path(model_root)
    if not root.is_absolute():
        raise StageCacheError("stage cache root must be absolute")
    return (
        root
        / STAGE_CACHE_ROOT_DIRECTORY
        / f"sha256-{identity.cache_identity_digest.removeprefix('sha256:')}"
    )


def canonical_stage_manifest(
    manifest: dict,
    identity: StageCacheIdentity,
    *,
    reserved_paths: frozenset[str] = frozenset(),
) -> CanonicalArtifactManifest:
    if type(identity) is not StageCacheIdentity:
        raise StageCacheError("stage cache identity is required")
    try:
        parsed = parse_artifact_manifest(
            manifest,
            reserved_paths={STAGE_CACHE_MANIFEST_FILE, *reserved_paths},
        )
    except ValueError as exc:
        raise StageCacheError("stage manifest is invalid") from exc
    paths = [item["path"] for item in parsed.document["files"]]
    metadata: set[str] = set()
    weight_parts: list[tuple[int, int]] = []
    items = {item["path"]: item for item in parsed.document["files"]}
    for path in paths:
        if "/" in path or "\\" in path:
            raise StageCacheError("stage manifest files must be root-level")
        match = _WEIGHT_FILE.fullmatch(path)
        if match is not None:
            weight_parts.append((int(match.group(1)), items[path]["size_bytes"]))
        elif path in _ALLOWED_METADATA:
            metadata.add(path)
        else:
            raise StageCacheError("stage manifest contains an unsupported file")
    weight_parts.sort()
    if (
        parsed.digest != identity.manifest_digest
        or not _REQUIRED_METADATA <= metadata
        or [part for part, _size in weight_parts]
        != list(range(len(weight_parts)))
        or any(
            not 0 < size <= STAGE_MAX_PART_BYTES
            for _part, size in weight_parts
        )
        or not weight_parts
        or len((parsed.canonical_json + "\n").encode("utf-8"))
        > STAGE_SIDECAR_MAX_BYTES
    ):
        raise StageCacheError("stage manifest does not match its identity")
    marker_item = next(
        item
        for item in parsed.document["files"]
        if item["path"] == STAGE_MARKER_FILE
    )
    if marker_item["size_bytes"] > STAGE_MARKER_MAX_BYTES:
        raise StageCacheError("stage marker exceeds the maximum size")
    return parsed


def _tensor_keys_digest(value: object) -> str:
    if type(value) is not list or not value:
        raise StageCacheError("stage tensor metadata is invalid")
    keys: list[str] = []
    for item in value:
        source = _exact_dict(item, frozenset({"name", "dtype", "shape"}), "tensor")
        name = source["name"]
        shape = source["shape"]
        if (
            type(name) is not str
            or _TENSOR_NAME.fullmatch(name) is None
            or name == "__metadata__"
            or source["dtype"] not in _TENSOR_DTYPES
            or type(shape) is not list
            or len(shape) > 16
            or any(type(dimension) is not int or dimension < 0 for dimension in shape)
        ):
            raise StageCacheError("stage tensor metadata is invalid")
        elements = 1
        for dimension in shape:
            elements *= dimension
        if not 1 <= elements <= 1 << 60:
            raise StageCacheError("stage tensor metadata is invalid")
        keys.append(name)
    if keys != sorted(keys) or len(keys) != len(set(keys)):
        raise StageCacheError("stage tensor order is not canonical")
    document = {
        "schema_version": STAGE_MARKER_SCHEMA_VERSION,
        "tensor_keys": keys,
    }
    encoded = json.dumps(
        document, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def validate_stage_marker_document(
    value: object,
    identity: StageCacheIdentity,
    manifest: CanonicalArtifactManifest,
) -> None:
    source = _exact_dict(value, _STAGE_MARKER_FIELDS, "stage marker")
    contract = _exact_dict(
        source["contract"], _STAGE_CONTRACT_FIELDS, "stage marker contract"
    )
    expected_contract = {
        "schema_version": STAGE_MARKER_SCHEMA_VERSION,
        "source_manifest_digest": identity.source_manifest_digest,
        "runtime_image": identity.runtime_image,
        "exporter_build_digest": identity.exporter_build_digest,
        "model_family": STAGE_MODEL_FAMILY,
        "architecture": identity.architecture,
        "quantization": identity.quantization,
        "tensor_parallel_size": identity.tensor_parallel_size,
        "pipeline_parallel_size": identity.pipeline_parallel_size,
        "loader_format": STAGE_NATIVE_LOADER_FORMAT,
        "vllm_version": identity.vllm_version,
        "max_part_bytes": STAGE_MAX_PART_BYTES,
        "trust_remote_code": False,
        "enable_lora": False,
        "is_moe": False,
        "is_multimodal": False,
    }
    metadata_files = source["metadata_files"]
    manifest_metadata = sorted(
        item["path"]
        for item in manifest.document["files"]
        if item["path"] in _ALLOWED_METADATA and item["path"] != STAGE_MARKER_FILE
    )
    calculated_tensor_digest = _tensor_keys_digest(source["tensors"])
    if (
        type(source["schema_version"]) is not int
        or source["schema_version"] != STAGE_MARKER_SCHEMA_VERSION
        or source["kind"] != STAGE_MARKER_KIND
        or type(contract["schema_version"]) is not int
        or type(contract["tensor_parallel_size"]) is not int
        or type(contract["pipeline_parallel_size"]) is not int
        or type(contract["max_part_bytes"]) is not int
        or any(
            type(contract[field]) is not bool
            for field in (
                "trust_remote_code",
                "enable_lora",
                "is_moe",
                "is_multimodal",
            )
        )
        or contract != expected_contract
        or type(source["pipeline_rank"]) is not int
        or source["pipeline_rank"] != identity.pipeline_rank
        or source["weight_pattern"] != "model-rank-0-part-*.safetensors"
        or type(metadata_files) is not list
        or metadata_files != manifest_metadata
        or calculated_tensor_digest != identity.tensor_keys_digest
        or source["tensor_key_digest"] != calculated_tensor_digest
    ):
        raise StageCacheError("stage marker does not match the cache identity")


def decode_unique_json(payload: bytes, *, field: str) -> object:
    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise StageCacheError(f"{field} contains a duplicate JSON key")
            result[key] = value
        return result

    try:
        return json.loads(payload.decode("utf-8"), object_pairs_hook=unique_object)
    except StageCacheError:
        raise
    except (RecursionError, UnicodeError, ValueError) as exc:
        raise StageCacheError(f"{field} is not valid JSON") from exc


def _safe_regular_bytes(path: Path, *, maximum: int) -> bytes:
    descriptor = -1
    try:
        observed = path.lstat()
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != os.geteuid()
            or observed.st_nlink != 1
            or observed.st_mode & 0o022
            or not 0 <= observed.st_size <= maximum
        ):
            raise StageCacheError("stage cache contains an unsafe file")
        descriptor = os.open(path, os.O_RDONLY | _CLOEXEC | _NOFOLLOW | _NONBLOCK)
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or before.st_mode & 0o022
            or (before.st_dev, before.st_ino, before.st_size)
            != (observed.st_dev, observed.st_ino, observed.st_size)
        ):
            raise StageCacheError("stage cache file identity changed")
        payload = bytearray()
        while len(payload) <= maximum:
            block = os.read(descriptor, min(1024 * 1024, maximum + 1 - len(payload)))
            if not block:
                break
            payload.extend(block)
        after = os.fstat(descriptor)
        if (
            len(payload) > maximum
            or len(payload) != before.st_size
            or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
        ):
            raise StageCacheError("stage cache file changed while being read")
        return bytes(payload)
    except StageCacheError:
        raise
    except OSError as exc:
        raise StageCacheError("stage cache file cannot be read safely") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _safe_regular_digest(path: Path, expected_size: int) -> str:
    descriptor = -1
    try:
        observed = path.lstat()
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != os.geteuid()
            or observed.st_nlink != 1
            or observed.st_mode & 0o022
            or observed.st_size != expected_size
        ):
            raise StageCacheError("stage cache contains an unsafe file")
        descriptor = os.open(path, os.O_RDONLY | _CLOEXEC | _NOFOLLOW | _NONBLOCK)
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or before.st_mode & 0o022
            or (before.st_dev, before.st_ino, before.st_size)
            != (observed.st_dev, observed.st_ino, observed.st_size)
        ):
            raise StageCacheError("stage cache file identity changed")
        digest = hashlib.sha256()
        count = 0
        while count <= expected_size:
            block = os.read(
                descriptor,
                min(1024 * 1024, expected_size - count + 1),
            )
            if not block:
                break
            count += len(block)
            if count > expected_size:
                raise StageCacheError("stage cache file size does not match")
            digest.update(block)
        after = os.fstat(descriptor)
        if (
            count != expected_size
            or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
        ):
            raise StageCacheError("stage cache file changed while being read")
        return "sha256:" + digest.hexdigest()
    except StageCacheError:
        raise
    except OSError as exc:
        raise StageCacheError("stage cache file cannot be read safely") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


@dataclass(frozen=True)
class StageCacheValidation:
    path: Path
    identity: StageCacheIdentity
    cache_identity_digest: str
    manifest_digest: str
    total_size_bytes: int
    file_count: int


def validate_materialized_stage_cache(
    path: Path,
    identity: StageCacheIdentity,
    *,
    require_canonical_path: bool = True,
) -> StageCacheValidation:
    candidate = Path(path)
    if type(identity) is not StageCacheIdentity:
        raise StageCacheError("stage cache identity is required")
    if require_canonical_path and candidate != stage_cache_path(
        identity, model_root=candidate.parent.parent
    ):
        raise StageCacheError("stage cache path does not match its identity")
    try:
        observed = candidate.lstat()
        resolved = candidate.resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise StageCacheError("stage cache directory is unavailable") from exc
    if (
        not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or observed.st_mode & 0o022
        or resolved != candidate
    ):
        raise StageCacheError("stage cache directory is unsafe")
    sidecar = _safe_regular_bytes(
        candidate / STAGE_CACHE_MANIFEST_FILE,
        maximum=STAGE_SIDECAR_MAX_BYTES,
    )
    manifest_value = decode_unique_json(sidecar, field="stage cache manifest")
    try:
        manifest = canonical_stage_manifest(
            manifest_value,
            identity,
            reserved_paths=frozenset({".dure-model.json"}),
        )
    except StageCacheError:
        raise
    canonical_payload = (manifest.canonical_json + "\n").encode("utf-8")
    if sidecar != canonical_payload:
        raise StageCacheError("stage cache manifest sidecar is not canonical")
    expected_files = {item["path"] for item in manifest.document["files"]} | {
        STAGE_CACHE_MANIFEST_FILE,
        ".dure-model.json",
    }
    try:
        entries = list(os.scandir(candidate))
    except OSError as exc:
        raise StageCacheError("stage cache directory cannot be scanned") from exc
    actual_files: set[str] = set()
    for entry in entries:
        try:
            state = entry.stat(follow_symlinks=False)
        except OSError as exc:
            raise StageCacheError("stage cache entry cannot be inspected") from exc
        if not stat.S_ISREG(state.st_mode):
            raise StageCacheError("stage cache contains a non-regular entry")
        actual_files.add(entry.name)
    if actual_files != expected_files:
        raise StageCacheError("stage cache tree does not match its manifest")
    for item in manifest.document["files"]:
        if _safe_regular_digest(candidate / item["path"], item["size_bytes"]) != item[
            "sha256"
        ]:
            raise StageCacheError("stage cache file digest does not match")
    marker_document = decode_unique_json(
        _safe_regular_bytes(
            candidate / STAGE_MARKER_FILE,
            maximum=STAGE_MARKER_MAX_BYTES,
        ),
        field="stage marker",
    )
    validate_stage_marker_document(marker_document, identity, manifest)
    from .model_cache import read_model_cache_marker

    cache_marker = read_model_cache_marker(candidate / ".dure-model.json")
    if cache_marker.stage_identity() != identity:
        raise StageCacheError("stage cache marker does not match its identity")
    return StageCacheValidation(
        path=candidate,
        identity=identity,
        cache_identity_digest=identity.cache_identity_digest,
        manifest_digest=identity.manifest_digest,
        total_size_bytes=manifest.total_size_bytes,
        file_count=manifest.file_count,
    )

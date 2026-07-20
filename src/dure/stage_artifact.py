from __future__ import annotations

import argparse
import ctypes
import errno
import hashlib
import importlib.metadata
import json
import os
import re
import stat
import struct
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, Sequence

from .artifact_manifest import parse_artifact_manifest, require_sha256_digest
from .artifact_prepare import validate_digest_pinned_runtime_image
from .model_cache import (
    MODEL_CACHE_KIND_FULL_SNAPSHOT,
    MODEL_CACHE_MARKER_FILE,
    read_model_cache_marker,
)


STAGE_ARTIFACT_SCHEMA_VERSION = 1
STAGE_ARTIFACT_KIND = "VLLM_SHARDED_STATE_PIPELINE_STAGE_SET"
STAGE_MARKER_KIND = "VLLM_SHARDED_STATE_PIPELINE_STAGE"
STAGE_MARKER_FILE = "dure-stage.json"
STAGE_SET_INDEX_FILE = "dure-artifact-set.json"
PINNED_VLLM_VERSION = "0.9.0"
SUPPORTED_MODEL_FAMILY = "qwen2.5"
SUPPORTED_ARCHITECTURE = "Qwen2ForCausalLM"
SUPPORTED_MODEL_TYPE = "qwen2"
SUPPORTED_QUANTIZATION = "awq"
SUPPORTED_LOAD_FORMAT = "sharded_state"
VLLM_NATIVE_LOAD_FORMAT = SUPPORTED_LOAD_FORMAT
CONTROL_LOADER_FORMAT = "VLLM_SHARDED_STATE_V1"
SUPPORTED_TENSOR_PARALLEL_SIZE = 1
DEFAULT_MAX_PART_BYTES = 5 * 1024**3
DEFAULT_CHUNK_BYTES = 8 * 1024**2
MAX_PIPELINE_STAGES = 64
MAX_SOURCE_ENTRIES = 200_000
MAX_JSON_BYTES = 4 * 1024**2
MAX_METADATA_BYTES = 128 * 1024**2
MAX_SAFETENSORS_HEADER_BYTES = 64 * 1024**2
MAX_TENSORS_PER_STAGE = 1_000_000

STAGE_ARTIFACT_FAILURE_CODES = frozenset(
    {
        "STAGE_CONTRACT_REJECTED",
        "STAGE_SOURCE_UNSAFE",
        "STAGE_SOURCE_CONFIG_INVALID",
        "STAGE_SOURCE_UNSUPPORTED",
        "STAGE_DEPENDENCY_UNAVAILABLE",
        "STAGE_VLLM_VERSION_MISMATCH",
        "STAGE_ENGINE_UNSUPPORTED",
        "STAGE_EXPORT_FAILED",
        "STAGE_RANK_SET_INVALID",
        "STAGE_FILE_SET_INVALID",
        "STAGE_SAFETENSORS_INVALID",
        "STAGE_TENSOR_COVERAGE_INVALID",
        "STAGE_DIGEST_MISMATCH",
        "STAGE_TARGET_EXISTS",
        "STAGE_ATOMIC_PUBLISH_UNAVAILABLE",
        "STAGE_IO_FAILED",
    }
)

_REMOTE_CODE_SUFFIXES = frozenset(
    {".py", ".pyc", ".pyo", ".so", ".dll", ".dylib"}
)
_ADAPTER_NAMES = frozenset(
    {
        "adapter_config.json",
        "adapter_model.bin",
        "adapter_model.safetensors",
    }
)
_MULTIMODAL_NAMES = frozenset(
    {
        "preprocessor_config.json",
        "processor_config.json",
        "video_preprocessor_config.json",
    }
)
_REQUIRED_METADATA = frozenset(
    {"config.json", "tokenizer.json", "tokenizer_config.json"}
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
_MOE_CONFIG_KEYS = frozenset(
    {
        "experts_per_token",
        "moe_intermediate_size",
        "n_routed_experts",
        "num_experts",
        "num_experts_per_tok",
        "num_local_experts",
        "router_aux_loss_coef",
    }
)
_MULTIMODAL_CONFIG_KEYS = frozenset(
    {
        "audio_config",
        "image_token_id",
        "multimodal_projector",
        "video_config",
        "vision_config",
        "vision_start_token_id",
    }
)
_WEIGHT_FILE = re.compile(r"model-rank-0-part-(0|[1-9][0-9]*)\.safetensors")
_SAFE_TENSOR_NAME = re.compile(r"[^\x00-\x1f\x7f]{1,1024}")
_DTYPE_BYTES = {
    "BOOL": 1,
    "U8": 1,
    "I8": 1,
    "F8_E4M3": 1,
    "F8_E5M2": 1,
    "U16": 2,
    "I16": 2,
    "F16": 2,
    "BF16": 2,
    "U32": 4,
    "I32": 4,
    "F32": 4,
    "U64": 8,
    "I64": 8,
    "F64": 8,
}
_TORCH_TO_SAFETENSORS = {
    "torch.bool": "BOOL",
    "torch.uint8": "U8",
    "torch.int8": "I8",
    "torch.float8_e4m3fn": "F8_E4M3",
    "torch.float8_e5m2": "F8_E5M2",
    "torch.int16": "I16",
    "torch.float16": "F16",
    "torch.bfloat16": "BF16",
    "torch.int32": "I32",
    "torch.float32": "F32",
    "torch.int64": "I64",
    "torch.float64": "F64",
}


class StageArtifactError(RuntimeError):
    def __init__(self, code: str, message: str | None = None) -> None:
        if code not in STAGE_ARTIFACT_FAILURE_CODES:
            raise ValueError("unsupported stage artifact failure code")
        self.code = code
        super().__init__(message or code.lower().replace("_", " "))


def _fail(code: str, message: str) -> None:
    raise StageArtifactError(code, message)


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _digest_json(value: object) -> str:
    return "sha256:" + hashlib.sha256(
        _canonical_json(value).encode("utf-8")
    ).hexdigest()


def _exact_dict(value: object, fields: frozenset[str], code: str) -> dict:
    if (
        type(value) is not dict
        or any(type(key) is not str for key in value)
        or set(value) != fields
    ):
        _fail(code, "object does not match the closed stage artifact schema")
    return value


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON key")
        value[key] = item
    return value


def _decode_json_bytes(
    payload: bytes,
    *,
    code: str,
    maximum: int = MAX_JSON_BYTES,
) -> object:
    if not payload or len(payload) > maximum:
        _fail(code, "JSON document is empty or exceeds its size limit")
    try:
        return json.loads(
            payload.decode("utf-8"), object_pairs_hook=_unique_object
        )
    except (RecursionError, UnicodeError, ValueError):
        _fail(code, "JSON document is invalid")


def _read_regular(path: Path, *, maximum: int, code: str) -> bytes:
    descriptor = -1
    try:
        observed = path.lstat()
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_nlink != 1
            or observed.st_size < 0
            or observed.st_size > maximum
        ):
            _fail(code, "artifact file is not a bounded single-link regular file")
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
        )
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_dev != observed.st_dev
            or opened.st_ino != observed.st_ino
            or opened.st_size != observed.st_size
            or opened.st_nlink != 1
        ):
            _fail(code, "artifact file identity changed while it was opened")
        value = bytearray()
        while len(value) <= maximum:
            block = os.read(descriptor, min(1024 * 1024, maximum + 1 - len(value)))
            if not block:
                break
            value.extend(block)
        if len(value) > maximum:
            _fail(code, "artifact file exceeds its size limit")
        after = os.fstat(descriptor)
        if (
            (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
            != (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
        ):
            _fail(code, "artifact file changed while it was read")
        return bytes(value)
    except StageArtifactError:
        raise
    except OSError:
        _fail(code, "artifact file could not be read safely")
    finally:
        if descriptor >= 0:
            os.close(descriptor)


@dataclass(frozen=True)
class StageExportContract:
    source_manifest_digest: str
    runtime_image: str
    exporter_build_digest: str
    pipeline_parallel_size: int
    model_family: str = SUPPORTED_MODEL_FAMILY
    architecture: str = SUPPORTED_ARCHITECTURE
    quantization: str = SUPPORTED_QUANTIZATION
    tensor_parallel_size: int = SUPPORTED_TENSOR_PARALLEL_SIZE
    loader_format: str = SUPPORTED_LOAD_FORMAT
    vllm_version: str = PINNED_VLLM_VERSION
    max_part_bytes: int = DEFAULT_MAX_PART_BYTES
    trust_remote_code: bool = False
    enable_lora: bool = False
    is_moe: bool = False
    is_multimodal: bool = False

    def __post_init__(self) -> None:
        try:
            require_sha256_digest(
                self.source_manifest_digest, field="source_manifest_digest"
            )
            require_sha256_digest(
                self.exporter_build_digest, field="exporter_build_digest"
            )
            validate_digest_pinned_runtime_image(self.runtime_image)
        except ValueError:
            _fail("STAGE_CONTRACT_REJECTED", "stage contract digest identity is invalid")
        exact = {
            "model_family": (self.model_family, SUPPORTED_MODEL_FAMILY),
            "architecture": (self.architecture, SUPPORTED_ARCHITECTURE),
            "quantization": (self.quantization, SUPPORTED_QUANTIZATION),
            "tensor_parallel_size": (
                self.tensor_parallel_size,
                SUPPORTED_TENSOR_PARALLEL_SIZE,
            ),
            "loader_format": (self.loader_format, SUPPORTED_LOAD_FORMAT),
            "vllm_version": (self.vllm_version, PINNED_VLLM_VERSION),
            "max_part_bytes": (self.max_part_bytes, DEFAULT_MAX_PART_BYTES),
        }
        if any(actual != expected for actual, expected in exact.values()):
            _fail("STAGE_CONTRACT_REJECTED", "stage contract uses an unsupported export identity")
        if (
            type(self.pipeline_parallel_size) is not int
            or not 1 <= self.pipeline_parallel_size <= MAX_PIPELINE_STAGES
        ):
            _fail("STAGE_CONTRACT_REJECTED", "pipeline_parallel_size is out of range")
        flags = (
            self.trust_remote_code,
            self.enable_lora,
            self.is_moe,
            self.is_multimodal,
        )
        if any(type(value) is not bool or value for value in flags):
            _fail("STAGE_CONTRACT_REJECTED", "unsupported model features must remain disabled")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": STAGE_ARTIFACT_SCHEMA_VERSION,
            "source_manifest_digest": self.source_manifest_digest,
            "runtime_image": self.runtime_image,
            "exporter_build_digest": self.exporter_build_digest,
            "model_family": self.model_family,
            "architecture": self.architecture,
            "quantization": self.quantization,
            "tensor_parallel_size": self.tensor_parallel_size,
            "pipeline_parallel_size": self.pipeline_parallel_size,
            "loader_format": self.loader_format,
            "vllm_version": self.vllm_version,
            "max_part_bytes": self.max_part_bytes,
            "trust_remote_code": self.trust_remote_code,
            "enable_lora": self.enable_lora,
            "is_moe": self.is_moe,
            "is_multimodal": self.is_multimodal,
        }

    @classmethod
    def from_dict(cls, value: object) -> "StageExportContract":
        fields = frozenset(
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
        source = _exact_dict(value, fields, "STAGE_CONTRACT_REJECTED")
        if source["schema_version"] != STAGE_ARTIFACT_SCHEMA_VERSION:
            _fail("STAGE_CONTRACT_REJECTED", "unsupported stage contract schema")
        return cls(
            source_manifest_digest=source["source_manifest_digest"],
            runtime_image=source["runtime_image"],
            exporter_build_digest=source["exporter_build_digest"],
            pipeline_parallel_size=source["pipeline_parallel_size"],
            model_family=source["model_family"],
            architecture=source["architecture"],
            quantization=source["quantization"],
            tensor_parallel_size=source["tensor_parallel_size"],
            loader_format=source["loader_format"],
            vllm_version=source["vllm_version"],
            max_part_bytes=source["max_part_bytes"],
            trust_remote_code=source["trust_remote_code"],
            enable_lora=source["enable_lora"],
            is_moe=source["is_moe"],
            is_multimodal=source["is_multimodal"],
        )

    @property
    def identity_digest(self) -> str:
        return _digest_json(self.to_dict())


@dataclass(frozen=True)
class TensorSpec:
    name: str
    dtype: str
    shape: tuple[int, ...]

    def __post_init__(self) -> None:
        if (
            type(self.name) is not str
            or _SAFE_TENSOR_NAME.fullmatch(self.name) is None
            or self.name == "__metadata__"
            or self.dtype not in _DTYPE_BYTES
            or type(self.shape) is not tuple
            or len(self.shape) > 16
            or any(type(value) is not int or value < 0 for value in self.shape)
        ):
            _fail("STAGE_SAFETENSORS_INVALID", "tensor metadata is invalid")
        elements = 1
        for value in self.shape:
            elements *= value
        if elements <= 0 or elements > 1 << 60:
            _fail("STAGE_SAFETENSORS_INVALID", "zero-sized or oversized tensors are unsupported")

    def to_dict(self) -> dict[str, object]:
        return {"name": self.name, "dtype": self.dtype, "shape": list(self.shape)}

    @classmethod
    def from_dict(cls, value: object) -> "TensorSpec":
        source = _exact_dict(
            value,
            frozenset({"name", "dtype", "shape"}),
            "STAGE_SAFETENSORS_INVALID",
        )
        if type(source["shape"]) is not list:
            _fail("STAGE_SAFETENSORS_INVALID", "tensor shape must be a list")
        return cls(source["name"], source["dtype"], tuple(source["shape"]))


def tensor_key_digest(keys: Sequence[str]) -> str:
    normalized = sorted(keys)
    if (
        not normalized
        or len(normalized) != len(set(normalized))
        or any(type(key) is not str or _SAFE_TENSOR_NAME.fullmatch(key) is None for key in normalized)
    ):
        _fail("STAGE_TENSOR_COVERAGE_INVALID", "tensor keys are empty, duplicated, or invalid")
    return _digest_json(
        {"schema_version": STAGE_ARTIFACT_SCHEMA_VERSION, "tensor_keys": normalized}
    )


@dataclass(frozen=True)
class WorkerStageExport:
    rank: int
    tensors: tuple[TensorSpec, ...]

    def __post_init__(self) -> None:
        if type(self.rank) is not int or self.rank < 0:
            _fail("STAGE_RANK_SET_INVALID", "worker returned an invalid pipeline rank")
        names = [item.name for item in self.tensors]
        if not names or names != sorted(names) or len(names) != len(set(names)):
            _fail("STAGE_TENSOR_COVERAGE_INVALID", "worker tensor coverage is not canonical")


@dataclass(frozen=True)
class StageArtifact:
    rank: int
    artifact_manifest: dict
    artifact_manifest_digest: str
    tensor_keys: tuple[str, ...]
    tensor_key_digest: str

    def to_dict(self) -> dict[str, object]:
        return {
            "rank": self.rank,
            "artifact_manifest": self.artifact_manifest,
            "artifact_manifest_digest": self.artifact_manifest_digest,
            "tensor_keys": list(self.tensor_keys),
            "tensor_key_digest": self.tensor_key_digest,
        }

    def to_registration_dict(self) -> dict[str, object]:
        weight_size_bytes = sum(
            item["size_bytes"]
            for item in self.artifact_manifest["files"]
            if _WEIGHT_FILE.fullmatch(item["path"]) is not None
        )
        if weight_size_bytes <= 0:
            _fail(
                "STAGE_FILE_SET_INVALID",
                "stage registration contains no native weight bytes",
            )
        return {
            "pipeline_rank": self.rank,
            "tensor_rank": 0,
            "manifest_digest": self.artifact_manifest_digest,
            "tensor_key_count": len(self.tensor_keys),
            "tensor_keys_digest": self.tensor_key_digest,
            "weight_size_bytes": weight_size_bytes,
            "manifest": self.artifact_manifest,
        }


@dataclass(frozen=True)
class StageArtifactSet:
    contract: StageExportContract
    stages: tuple[StageArtifact, ...]
    index: dict
    index_digest: str
    root: Path

    def to_dict(self) -> dict[str, object]:
        return {
            "contract": self.contract.to_dict(),
            "index": self.index,
            "index_digest": self.index_digest,
            "stages": [stage.to_dict() for stage in self.stages],
        }

    def registration_payload(self) -> dict[str, object]:
        return {
            "source_manifest_digest": self.contract.source_manifest_digest,
            "runtime_image": self.contract.runtime_image,
            "vllm_version": self.contract.vllm_version,
            "exporter_build_digest": self.contract.exporter_build_digest,
            "architecture": self.contract.architecture,
            "quantization": self.contract.quantization,
            "tensor_parallel_size": self.contract.tensor_parallel_size,
            "pipeline_parallel_size": self.contract.pipeline_parallel_size,
            "loader_format": CONTROL_LOADER_FORMAT,
            "stages": [stage.to_registration_dict() for stage in self.stages],
        }


class NativeStageExporter(Protocol):
    def export(
        self,
        source: Path,
        workspace: Path,
        contract: StageExportContract,
    ) -> Sequence[WorkerStageExport]: ...


def _scan_source_tree(source: Path) -> tuple[set[str], set[str]]:
    count = 0
    files: set[str] = set()
    directories: set[str] = set()
    try:
        resolved_root = source.resolve(strict=True)
    except (OSError, RuntimeError, ValueError):
        _fail("STAGE_SOURCE_UNSAFE", "source model directory cannot be resolved")
    pending: list[tuple[Path, str]] = [(source, "")]
    while pending:
        directory, prefix = pending.pop()
        try:
            directory_state = directory.lstat()
            resolved_directory = directory.resolve(strict=True)
            if (
                not stat.S_ISDIR(directory_state.st_mode)
                or directory_state.st_uid != os.geteuid()
                or directory_state.st_mode & 0o022
                or not resolved_directory.is_relative_to(resolved_root)
            ):
                _fail("STAGE_SOURCE_UNSAFE", "source tree contains an unsafe directory")
            entries = sorted(os.scandir(directory), key=lambda item: item.name)
        except StageArtifactError:
            raise
        except OSError:
            _fail("STAGE_SOURCE_UNSAFE", "source tree cannot be inspected")
        for entry in entries:
            count += 1
            if count > MAX_SOURCE_ENTRIES:
                _fail("STAGE_SOURCE_UNSAFE", "source tree exceeds the entry limit")
            relative = f"{prefix}/{entry.name}".lstrip("/")
            try:
                observed = entry.stat(follow_symlinks=False)
            except OSError:
                _fail("STAGE_SOURCE_UNSAFE", "source entry cannot be inspected")
            if stat.S_ISLNK(observed.st_mode):
                _fail("STAGE_SOURCE_UNSAFE", "source tree contains a symbolic link")
            if stat.S_ISDIR(observed.st_mode):
                directories.add(relative)
                pending.append((Path(entry.path), relative))
                continue
            if (
                not stat.S_ISREG(observed.st_mode)
                or observed.st_uid != os.geteuid()
                or observed.st_nlink != 1
                or observed.st_mode & 0o022
            ):
                _fail("STAGE_SOURCE_UNSAFE", "source tree contains an unsupported file type")
            files.add(relative)
            name = entry.name
            suffix = Path(name).suffix.lower()
            if suffix in _REMOTE_CODE_SUFFIXES:
                _fail("STAGE_SOURCE_UNSUPPORTED", "source tree contains executable remote code")
            if name in _ADAPTER_NAMES:
                _fail("STAGE_SOURCE_UNSUPPORTED", "LoRA or adapter artifacts are unsupported")
            if name in _MULTIMODAL_NAMES:
                _fail("STAGE_SOURCE_UNSUPPORTED", "multimodal processor artifacts are unsupported")
    return files, directories


def _source_manifest_tree(manifest: dict) -> tuple[set[str], set[str]]:
    files = {item["path"] for item in manifest["files"]}
    files.add(MODEL_CACHE_MARKER_FILE)
    directories: set[str] = set()
    for relative in files:
        parts = Path(relative).parts
        for length in range(1, len(parts)):
            directories.add(Path(*parts[:length]).as_posix())
    return files, directories


def _verify_source_manifest_file(path: Path, item: dict) -> None:
    expected_size = item["size_bytes"]
    descriptor = -1
    try:
        observed = path.lstat()
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != os.geteuid()
            or observed.st_nlink != 1
            or observed.st_mode & 0o022
        ):
            _fail("STAGE_SOURCE_UNSAFE", "source manifest file is unsafe")
        if observed.st_size != expected_size:
            _fail("STAGE_DIGEST_MISMATCH", "source manifest file size changed")
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
        )
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_dev != observed.st_dev
            or before.st_ino != observed.st_ino
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or before.st_mode & 0o022
            or before.st_size != expected_size
        ):
            _fail("STAGE_SOURCE_UNSAFE", "source manifest file identity changed")
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
                _fail("STAGE_DIGEST_MISMATCH", "source manifest file grew while read")
            digest.update(block)
        after = os.fstat(descriptor)
        identity_before = (
            before.st_dev,
            before.st_ino,
            before.st_mode,
            before.st_uid,
            before.st_nlink,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        )
        identity_after = (
            after.st_dev,
            after.st_ino,
            after.st_mode,
            after.st_uid,
            after.st_nlink,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        )
        if count != expected_size or identity_before != identity_after:
            _fail("STAGE_DIGEST_MISMATCH", "source manifest file changed while read")
        if "sha256:" + digest.hexdigest() != item["sha256"]:
            _fail("STAGE_DIGEST_MISMATCH", "source manifest file digest changed")
    except StageArtifactError:
        raise
    except OSError:
        _fail("STAGE_SOURCE_UNSAFE", "source manifest file cannot be read safely")
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _validate_source_model(
    source: Path,
    contract: StageExportContract,
    source_manifest: dict,
) -> tuple[str, ...]:
    try:
        observed = source.lstat()
    except OSError:
        _fail("STAGE_SOURCE_UNSAFE", "source model directory is unavailable")
    if (
        not stat.S_ISDIR(observed.st_mode)
        or source.is_symlink()
        or observed.st_uid != os.geteuid()
        or observed.st_mode & 0o022
    ):
        _fail("STAGE_SOURCE_UNSAFE", "source model must be a real local directory")
    try:
        parsed_manifest = parse_artifact_manifest(
            source_manifest,
            reserved_paths=(MODEL_CACHE_MARKER_FILE,),
        )
    except (TypeError, ValueError):
        _fail("STAGE_SOURCE_UNSAFE", "source artifact manifest is invalid")
    if parsed_manifest.digest != contract.source_manifest_digest:
        _fail("STAGE_DIGEST_MISMATCH", "source artifact manifest does not match the contract")
    actual_files, actual_directories = _scan_source_tree(source)
    expected_files, expected_directories = _source_manifest_tree(
        parsed_manifest.document
    )
    if actual_files != expected_files or actual_directories != expected_directories:
        _fail("STAGE_SOURCE_UNSAFE", "source tree does not exactly match its artifact manifest")
    try:
        marker = read_model_cache_marker(source / MODEL_CACHE_MARKER_FILE)
    except (OSError, ValueError):
        _fail("STAGE_SOURCE_UNSAFE", "source model has no trusted FULL_SNAPSHOT marker")
    if (
        marker.cache_kind != MODEL_CACHE_KIND_FULL_SNAPSHOT
        or marker.manifest_digest != contract.source_manifest_digest
        or marker.quantization != contract.quantization
    ):
        _fail("STAGE_SOURCE_UNSUPPORTED", "source cache identity does not match the export contract")
    for item in parsed_manifest.document["files"]:
        _verify_source_manifest_file(source / item["path"], item)
    config_path = source / "config.json"
    config = _decode_json_bytes(
        _read_regular(
            config_path,
            maximum=MAX_JSON_BYTES,
            code="STAGE_SOURCE_CONFIG_INVALID",
        ),
        code="STAGE_SOURCE_CONFIG_INVALID",
    )
    if type(config) is not dict:
        _fail("STAGE_SOURCE_CONFIG_INVALID", "model config must be an object")
    quantization = config.get("quantization_config")
    architectures = config.get("architectures")
    if (
        config.get("model_type") != SUPPORTED_MODEL_TYPE
        or architectures != [SUPPORTED_ARCHITECTURE]
        or type(quantization) is not dict
        or str(quantization.get("quant_method", "")).lower()
        != SUPPORTED_QUANTIZATION
    ):
        _fail("STAGE_SOURCE_UNSUPPORTED", "only the pinned Qwen2.5 AWQ architecture is supported")
    if config.get("auto_map") not in (None, {}) or config.get("trust_remote_code") is True:
        _fail("STAGE_SOURCE_UNSUPPORTED", "remote model code is unsupported")
    if any(config.get(key) is not None for key in _MOE_CONFIG_KEYS):
        _fail("STAGE_SOURCE_UNSUPPORTED", "MoE models are unsupported")
    if any(config.get(key) is not None for key in _MULTIMODAL_CONFIG_KEYS):
        _fail("STAGE_SOURCE_UNSUPPORTED", "multimodal models are unsupported")
    available = {path for path in actual_files if "/" not in path}
    if not _REQUIRED_METADATA.issubset(available):
        _fail("STAGE_SOURCE_UNSUPPORTED", "required Qwen2.5 tokenizer metadata is missing")
    return tuple(sorted(available.intersection(_ALLOWED_METADATA)))


def _safetensors_specs(path: Path) -> tuple[TensorSpec, ...]:
    try:
        observed = path.lstat()
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_nlink != 1
            or observed.st_size < 10
        ):
            _fail("STAGE_SAFETENSORS_INVALID", "weight shard is not a regular safetensors file")
        with path.open("rb", buffering=0) as handle:
            prefix = handle.read(8)
            if len(prefix) != 8:
                _fail("STAGE_SAFETENSORS_INVALID", "safetensors header is truncated")
            header_length = struct.unpack("<Q", prefix)[0]
            if not 2 <= header_length <= MAX_SAFETENSORS_HEADER_BYTES:
                _fail("STAGE_SAFETENSORS_INVALID", "safetensors header length is invalid")
            if header_length > observed.st_size - 8:
                _fail("STAGE_SAFETENSORS_INVALID", "safetensors header exceeds the file")
            header_bytes = handle.read(header_length)
            if len(header_bytes) != header_length:
                _fail("STAGE_SAFETENSORS_INVALID", "safetensors header is truncated")
    except StageArtifactError:
        raise
    except OSError:
        _fail("STAGE_SAFETENSORS_INVALID", "weight shard could not be read")
    header = _decode_json_bytes(
        header_bytes,
        code="STAGE_SAFETENSORS_INVALID",
        maximum=MAX_SAFETENSORS_HEADER_BYTES,
    )
    if type(header) is not dict or len(header) > MAX_TENSORS_PER_STAGE + 1:
        _fail("STAGE_SAFETENSORS_INVALID", "safetensors header is not a bounded object")
    data_size = observed.st_size - 8 - header_length
    ranges: list[tuple[int, int, TensorSpec]] = []
    for name, raw in header.items():
        if name == "__metadata__":
            if type(raw) is not dict or any(
                type(key) is not str or type(value) is not str
                for key, value in raw.items()
            ):
                _fail("STAGE_SAFETENSORS_INVALID", "safetensors metadata is invalid")
            continue
        item = _exact_dict(
            raw,
            frozenset({"dtype", "shape", "data_offsets"}),
            "STAGE_SAFETENSORS_INVALID",
        )
        if type(item["shape"]) is not list or type(item["data_offsets"]) is not list:
            _fail("STAGE_SAFETENSORS_INVALID", "safetensors tensor fields are invalid")
        offsets = item["data_offsets"]
        if (
            len(offsets) != 2
            or any(type(value) is not int for value in offsets)
            or not 0 <= offsets[0] < offsets[1] <= data_size
        ):
            _fail("STAGE_SAFETENSORS_INVALID", "safetensors tensor offsets are invalid")
        spec = TensorSpec(name, item["dtype"], tuple(item["shape"]))
        elements = 1
        for dimension in spec.shape:
            elements *= dimension
        if offsets[1] - offsets[0] != elements * _DTYPE_BYTES[spec.dtype]:
            _fail("STAGE_SAFETENSORS_INVALID", "safetensors tensor byte range is invalid")
        ranges.append((offsets[0], offsets[1], spec))
    if not ranges:
        _fail("STAGE_SAFETENSORS_INVALID", "weight shard contains no tensors")
    ranges.sort(key=lambda value: (value[0], value[1], value[2].name))
    cursor = 0
    for start, end, _spec in ranges:
        if start != cursor:
            _fail("STAGE_SAFETENSORS_INVALID", "safetensors ranges have a gap or overlap")
        cursor = end
    if cursor != data_size:
        _fail("STAGE_SAFETENSORS_INVALID", "safetensors ranges do not cover the data section")
    specs = sorted((item[2] for item in ranges), key=lambda value: value.name)
    if len({item.name for item in specs}) != len(specs):
        _fail("STAGE_SAFETENSORS_INVALID", "weight shard has duplicate tensor names")
    return tuple(specs)


def _artifact_manifest_for_directory(
    root: Path, *, chunk_bytes: int = DEFAULT_CHUNK_BYTES
) -> dict:
    if type(chunk_bytes) is not int or chunk_bytes <= 0:
        raise ValueError("chunk_bytes must be a positive integer")
    raw_files: list[dict[str, object]] = []
    for path in sorted(root.iterdir(), key=lambda item: item.name):
        try:
            observed = path.lstat()
        except OSError:
            _fail("STAGE_IO_FAILED", "stage file cannot be inspected")
        if not stat.S_ISREG(observed.st_mode) or observed.st_nlink != 1:
            _fail("STAGE_FILE_SET_INVALID", "stage contains a non-regular or linked file")
        file_hash = hashlib.sha256()
        chunks: list[dict[str, object]] = []
        offset = 0
        try:
            with path.open("rb") as handle:
                while True:
                    block = handle.read(chunk_bytes)
                    if not block:
                        break
                    file_hash.update(block)
                    chunks.append(
                        {
                            "ordinal": len(chunks),
                            "offset_bytes": offset,
                            "length_bytes": len(block),
                            "sha256": "sha256:" + hashlib.sha256(block).hexdigest(),
                        }
                    )
                    offset += len(block)
        except OSError:
            _fail("STAGE_IO_FAILED", "stage file could not be hashed")
        if offset != observed.st_size:
            _fail("STAGE_IO_FAILED", "stage file changed while it was hashed")
        raw_files.append(
            {
                "path": path.name,
                "kind": "REGULAR",
                "size_bytes": observed.st_size,
                "sha256": "sha256:" + file_hash.hexdigest(),
                "chunks": chunks,
            }
        )
    try:
        return parse_artifact_manifest(
            {"schema_version": 1, "files": raw_files}
        ).document
    except ValueError:
        _fail("STAGE_FILE_SET_INVALID", "stage cannot be represented by the canonical artifact manifest")


def _write_canonical(path: Path, value: object) -> None:
    payload = _canonical_json(value).encode("utf-8")
    try:
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0),
            0o600,
        )
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError:
        _fail("STAGE_FILE_SET_INVALID", "builder attempted to replace an existing artifact file")
    except OSError:
        _fail("STAGE_IO_FAILED", "artifact metadata could not be written")


def _fsync_directory(path: Path) -> None:
    descriptor = -1
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        observed = os.fstat(descriptor)
        if not stat.S_ISDIR(observed.st_mode):
            _fail("STAGE_IO_FAILED", "artifact directory is not durable")
        os.fsync(descriptor)
    except StageArtifactError:
        raise
    except OSError:
        _fail("STAGE_IO_FAILED", "artifact directory could not be synchronized")
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _fsync_artifact_tree(root: Path) -> None:
    """Persist exported weights and directory entries before atomic publication."""

    def raise_walk_error(error: OSError) -> None:
        raise error

    try:
        walked = list(
            os.walk(
                root,
                topdown=False,
                onerror=raise_walk_error,
                followlinks=False,
            )
        )
    except OSError:
        _fail("STAGE_IO_FAILED", "artifact tree could not be inspected for synchronization")
    if not walked:
        _fail("STAGE_IO_FAILED", "artifact tree disappeared before synchronization")
    for directory_name, child_directories, file_names in walked:
        directory = Path(directory_name)
        for name in sorted(child_directories):
            child = directory / name
            try:
                observed = child.lstat()
            except OSError:
                _fail("STAGE_IO_FAILED", "artifact directory changed before publication")
            if not stat.S_ISDIR(observed.st_mode) or child.is_symlink():
                _fail("STAGE_IO_FAILED", "artifact directory became unsafe before publication")
        for name in sorted(file_names):
            path = directory / name
            descriptor = -1
            try:
                before = path.lstat()
                if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
                    _fail("STAGE_IO_FAILED", "artifact file became unsafe before publication")
                descriptor = os.open(
                    path,
                    os.O_RDONLY
                    | getattr(os, "O_CLOEXEC", 0)
                    | getattr(os, "O_NOFOLLOW", 0)
                    | getattr(os, "O_NONBLOCK", 0),
                )
                opened = os.fstat(descriptor)
                if (
                    not stat.S_ISREG(opened.st_mode)
                    or opened.st_nlink != 1
                    or opened.st_dev != before.st_dev
                    or opened.st_ino != before.st_ino
                    or opened.st_size != before.st_size
                ):
                    _fail("STAGE_IO_FAILED", "artifact file changed before publication")
                os.fsync(descriptor)
            except StageArtifactError:
                raise
            except OSError:
                _fail("STAGE_IO_FAILED", "artifact file could not be synchronized")
            finally:
                if descriptor >= 0:
                    os.close(descriptor)
        _fsync_directory(directory)


def _copy_metadata(source: Path, destination: Path, names: Sequence[str]) -> None:
    for name in names:
        payload = _read_regular(
            source / name,
            maximum=MAX_METADATA_BYTES,
            code="STAGE_SOURCE_UNSAFE",
        )
        target = destination / name
        try:
            descriptor = os.open(
                target,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0),
                0o600,
            )
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        except FileExistsError:
            _fail("STAGE_FILE_SET_INVALID", "metadata collides with exported weights")
        except OSError:
            _fail("STAGE_IO_FAILED", "metadata could not be copied")


def _marker_document(
    contract: StageExportContract,
    rank: int,
    tensors: tuple[TensorSpec, ...],
    metadata_files: tuple[str, ...],
) -> dict[str, object]:
    keys = tuple(item.name for item in tensors)
    return {
        "schema_version": STAGE_ARTIFACT_SCHEMA_VERSION,
        "kind": STAGE_MARKER_KIND,
        "contract": contract.to_dict(),
        "pipeline_rank": rank,
        "weight_pattern": "model-rank-0-part-*.safetensors",
        "metadata_files": list(metadata_files),
        "tensors": [item.to_dict() for item in tensors],
        "tensor_key_digest": tensor_key_digest(keys),
    }


def _validate_stage_directory(
    directory: Path,
    contract: StageExportContract,
    rank: int,
    *,
    expected_manifest_digest: str | None = None,
) -> StageArtifact:
    if not directory.is_dir() or directory.is_symlink():
        _fail("STAGE_RANK_SET_INVALID", "pipeline stage directory is missing or unsafe")
    try:
        names = tuple(sorted(item.name for item in directory.iterdir()))
    except OSError:
        _fail("STAGE_FILE_SET_INVALID", "pipeline stage directory cannot be inspected")
    marker = _decode_json_bytes(
        _read_regular(
            directory / STAGE_MARKER_FILE,
            maximum=MAX_JSON_BYTES,
            code="STAGE_FILE_SET_INVALID",
        ),
        code="STAGE_FILE_SET_INVALID",
    )
    source = _exact_dict(
        marker,
        frozenset(
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
        ),
        "STAGE_FILE_SET_INVALID",
    )
    if (
        source["schema_version"] != STAGE_ARTIFACT_SCHEMA_VERSION
        or source["kind"] != STAGE_MARKER_KIND
        or StageExportContract.from_dict(source["contract"]) != contract
        or source["pipeline_rank"] != rank
        or source["weight_pattern"] != "model-rank-0-part-*.safetensors"
        or type(source["metadata_files"]) is not list
        or source["metadata_files"] != sorted(source["metadata_files"])
        or not _REQUIRED_METADATA.issubset(set(source["metadata_files"]))
        or not set(source["metadata_files"]).issubset(_ALLOWED_METADATA)
        or type(source["tensors"]) is not list
    ):
        _fail("STAGE_FILE_SET_INVALID", "stage marker does not match its closed contract")
    expected_tensors = tuple(TensorSpec.from_dict(item) for item in source["tensors"])
    if tuple(sorted(expected_tensors, key=lambda item: item.name)) != expected_tensors:
        _fail("STAGE_TENSOR_COVERAGE_INVALID", "stage marker tensor order is not canonical")
    keys = tuple(item.name for item in expected_tensors)
    if source["tensor_key_digest"] != tensor_key_digest(keys):
        _fail("STAGE_TENSOR_COVERAGE_INVALID", "stage tensor key digest is inconsistent")
    weight_parts: list[tuple[int, str]] = []
    allowed = {STAGE_MARKER_FILE, *source["metadata_files"]}
    for name in names:
        match = _WEIGHT_FILE.fullmatch(name)
        if match is not None:
            weight_parts.append((int(match.group(1)), name))
        elif name not in allowed:
            _fail("STAGE_FILE_SET_INVALID", "stage contains an unexpected file")
    weight_parts.sort()
    if [part for part, _name in weight_parts] != list(range(len(weight_parts))):
        _fail("STAGE_FILE_SET_INVALID", "native weight parts are missing or non-contiguous")
    if not weight_parts:
        _fail("STAGE_FILE_SET_INVALID", "stage contains no native weight shard")
    actual_tensors: list[TensorSpec] = []
    seen: set[str] = set()
    for _part, name in weight_parts:
        for tensor in _safetensors_specs(directory / name):
            if tensor.name in seen:
                _fail("STAGE_TENSOR_COVERAGE_INVALID", "tensor appears in more than one weight part")
            seen.add(tensor.name)
            actual_tensors.append(tensor)
    actual_tensors.sort(key=lambda item: item.name)
    if tuple(actual_tensors) != expected_tensors:
        _fail("STAGE_TENSOR_COVERAGE_INVALID", "native weight tensors do not match worker coverage")
    artifact_manifest = _artifact_manifest_for_directory(directory)
    parsed = parse_artifact_manifest(artifact_manifest)
    if expected_manifest_digest is not None and parsed.digest != expected_manifest_digest:
        _fail("STAGE_DIGEST_MISMATCH", "stage artifact digest does not match the artifact-set index")
    return StageArtifact(
        rank=rank,
        artifact_manifest=artifact_manifest,
        artifact_manifest_digest=parsed.digest,
        tensor_keys=keys,
        tensor_key_digest=source["tensor_key_digest"],
    )


def _index_document(
    contract: StageExportContract, stages: Sequence[StageArtifact]
) -> dict[str, object]:
    return {
        "schema_version": STAGE_ARTIFACT_SCHEMA_VERSION,
        "kind": STAGE_ARTIFACT_KIND,
        "contract": contract.to_dict(),
        "stages": [
            {
                "rank": stage.rank,
                "artifact_manifest_digest": stage.artifact_manifest_digest,
                "tensor_key_digest": stage.tensor_key_digest,
                "tensor_count": len(stage.tensor_keys),
            }
            for stage in stages
        ],
    }


def verify_stage_artifact_set(
    root: Path,
    *,
    expected_contract: StageExportContract | None = None,
    expected_index_digest: str | None = None,
) -> StageArtifactSet:
    root = Path(root)
    if not root.is_dir() or root.is_symlink():
        _fail("STAGE_FILE_SET_INVALID", "artifact-set root is missing or unsafe")
    try:
        root_names = sorted(item.name for item in root.iterdir())
    except OSError:
        _fail("STAGE_FILE_SET_INVALID", "artifact-set root cannot be inspected")
    if root_names != [STAGE_SET_INDEX_FILE, "stages"]:
        _fail("STAGE_FILE_SET_INVALID", "artifact-set root contains unexpected files")
    index_payload = _read_regular(
        root / STAGE_SET_INDEX_FILE,
        maximum=MAX_JSON_BYTES,
        code="STAGE_FILE_SET_INVALID",
    )
    index_digest = "sha256:" + hashlib.sha256(index_payload).hexdigest()
    if expected_index_digest is not None:
        try:
            require_sha256_digest(expected_index_digest, field="expected_index_digest")
        except ValueError:
            _fail("STAGE_CONTRACT_REJECTED", "expected index digest is invalid")
        if index_digest != expected_index_digest:
            _fail("STAGE_DIGEST_MISMATCH", "artifact-set index digest changed")
    index = _decode_json_bytes(index_payload, code="STAGE_FILE_SET_INVALID")
    source = _exact_dict(
        index,
        frozenset({"schema_version", "kind", "contract", "stages"}),
        "STAGE_FILE_SET_INVALID",
    )
    contract = StageExportContract.from_dict(source["contract"])
    if (
        source["schema_version"] != STAGE_ARTIFACT_SCHEMA_VERSION
        or source["kind"] != STAGE_ARTIFACT_KIND
        or expected_contract is not None
        and contract != expected_contract
        or type(source["stages"]) is not list
    ):
        _fail("STAGE_CONTRACT_REJECTED", "artifact-set index contract is invalid")
    expected_ranks = list(range(contract.pipeline_parallel_size))
    stage_rows = source["stages"]
    if len(stage_rows) != len(expected_ranks):
        _fail("STAGE_RANK_SET_INVALID", "artifact-set stage count is incomplete")
    expected_by_rank: dict[int, dict] = {}
    observed_rank_order: list[int] = []
    for row in stage_rows:
        item = _exact_dict(
            row,
            frozenset(
                {
                    "rank",
                    "artifact_manifest_digest",
                    "tensor_key_digest",
                    "tensor_count",
                }
            ),
            "STAGE_RANK_SET_INVALID",
        )
        rank = item["rank"]
        if type(rank) is not int or rank in expected_by_rank:
            _fail("STAGE_RANK_SET_INVALID", "artifact-set has a duplicated or invalid rank")
        observed_rank_order.append(rank)
        try:
            require_sha256_digest(
                item["artifact_manifest_digest"], field="artifact_manifest_digest"
            )
            require_sha256_digest(item["tensor_key_digest"], field="tensor_key_digest")
        except ValueError:
            _fail("STAGE_RANK_SET_INVALID", "artifact-set has an invalid rank digest")
        if type(item["tensor_count"]) is not int or item["tensor_count"] < 1:
            _fail("STAGE_RANK_SET_INVALID", "artifact-set has an invalid tensor count")
        expected_by_rank[rank] = item
    if sorted(expected_by_rank) != expected_ranks or observed_rank_order != expected_ranks:
        _fail("STAGE_RANK_SET_INVALID", "artifact-set rank coverage is incomplete")
    stages_root = root / "stages"
    if not stages_root.is_dir() or stages_root.is_symlink():
        _fail("STAGE_RANK_SET_INVALID", "artifact-set stages root is unsafe")
    try:
        actual_stage_names = {item.name for item in stages_root.iterdir()}
    except OSError:
        _fail("STAGE_RANK_SET_INVALID", "artifact-set stages cannot be inspected")
    if actual_stage_names != {str(rank) for rank in expected_ranks}:
        _fail("STAGE_RANK_SET_INVALID", "artifact-set stage directories do not match the index")
    stages = tuple(
        _validate_stage_directory(
            stages_root / str(rank),
            contract,
            rank,
            expected_manifest_digest=expected_by_rank[rank]["artifact_manifest_digest"],
        )
        for rank in expected_ranks
    )
    for stage in stages:
        expected = expected_by_rank[stage.rank]
        if (
            stage.tensor_key_digest != expected["tensor_key_digest"]
            or len(stage.tensor_keys) != expected["tensor_count"]
        ):
            _fail("STAGE_TENSOR_COVERAGE_INVALID", "artifact-set tensor evidence is inconsistent")
    return StageArtifactSet(contract, stages, index, index_digest, root)


def _publish_noreplace(source: Path, target: Path) -> None:
    if os.name != "posix":
        _fail("STAGE_ATOMIC_PUBLISH_UNAVAILABLE", "stage artifact publishing requires Linux renameat2")
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        _fail("STAGE_ATOMIC_PUBLISH_UNAVAILABLE", "renameat2 is unavailable")
    renameat2.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    renameat2.restype = ctypes.c_int
    encoded_source = os.fsencode(source)
    encoded_target = os.fsencode(target)
    _fsync_directory(target.parent)
    if renameat2(-100, encoded_source, -100, encoded_target, 1) == 0:
        _fsync_directory(target.parent)
        return
    error = ctypes.get_errno()
    if error in {errno.EEXIST, errno.ENOTEMPTY}:
        _fail("STAGE_TARGET_EXISTS", "stage artifact target already exists")
    if error in {errno.ENOSYS, errno.EINVAL, errno.EOPNOTSUPP}:
        _fail("STAGE_ATOMIC_PUBLISH_UNAVAILABLE", "filesystem lacks atomic no-replace publish")
    _fail("STAGE_IO_FAILED", "stage artifact could not be published")


class TrustedStageBuilder:
    def __init__(self, exporter: NativeStageExporter) -> None:
        if not hasattr(exporter, "export"):
            raise ValueError("native stage exporter is invalid")
        self.exporter = exporter

    def build(
        self,
        source: Path,
        output: Path,
        contract: StageExportContract,
        source_manifest: dict,
    ) -> StageArtifactSet:
        source = Path(source)
        output = Path(output)
        if output.exists() or output.is_symlink():
            _fail("STAGE_TARGET_EXISTS", "stage artifact target already exists")
        try:
            parent = output.parent.resolve(strict=True)
        except OSError:
            _fail("STAGE_SOURCE_UNSAFE", "stage artifact parent directory is unavailable")
        if not parent.is_dir() or parent.is_symlink():
            _fail("STAGE_SOURCE_UNSAFE", "stage artifact parent directory is unsafe")
        output = parent / output.name
        if output.exists() or output.is_symlink():
            _fail("STAGE_TARGET_EXISTS", "stage artifact target already exists")
        try:
            temporary = Path(tempfile.mkdtemp(prefix=f".{output.name}.building-", dir=parent))
            stages_root = temporary / "stages"
            stages_root.mkdir(mode=0o700)
            for rank in range(contract.pipeline_parallel_size):
                (stages_root / str(rank)).mkdir(mode=0o700)
        except OSError:
            _fail("STAGE_IO_FAILED", "stage artifact workspace could not be created")
        metadata_files = _validate_source_model(
            source,
            contract,
            source_manifest,
        )
        try:
            exports = tuple(self.exporter.export(source, temporary, contract))
        except StageArtifactError:
            raise
        except Exception:
            _fail("STAGE_EXPORT_FAILED", "native vLLM stage export failed")
        by_rank: dict[int, WorkerStageExport] = {}
        for item in exports:
            if type(item) is not WorkerStageExport or item.rank in by_rank:
                _fail("STAGE_RANK_SET_INVALID", "native exporter returned duplicate or invalid ranks")
            by_rank[item.rank] = item
        if sorted(by_rank) != list(range(contract.pipeline_parallel_size)):
            _fail("STAGE_RANK_SET_INVALID", "native exporter did not return every pipeline rank")
        stages: list[StageArtifact] = []
        for rank in range(contract.pipeline_parallel_size):
            directory = stages_root / str(rank)
            _copy_metadata(source, directory, metadata_files)
            marker = _marker_document(
                contract,
                rank,
                by_rank[rank].tensors,
                metadata_files,
            )
            _write_canonical(directory / STAGE_MARKER_FILE, marker)
            stages.append(_validate_stage_directory(directory, contract, rank))
        index = _index_document(contract, stages)
        _write_canonical(temporary / STAGE_SET_INDEX_FILE, index)
        staged = verify_stage_artifact_set(
            temporary,
            expected_contract=contract,
            expected_index_digest=_digest_json(index),
        )
        _fsync_artifact_tree(temporary)
        _publish_noreplace(temporary, output)
        return StageArtifactSet(
            contract=staged.contract,
            stages=staged.stages,
            index=staged.index,
            index_digest=staged.index_digest,
            root=output,
        )


def _torch_tensor_spec(name: str, tensor: object) -> TensorSpec:
    dtype = _TORCH_TO_SAFETENSORS.get(str(getattr(tensor, "dtype", "")))
    shape = getattr(tensor, "shape", None)
    if dtype is None or shape is None:
        _fail("STAGE_EXPORT_FAILED", "native worker returned an unsupported tensor dtype")
    try:
        normalized_shape = tuple(int(value) for value in shape)
    except (TypeError, ValueError):
        _fail("STAGE_EXPORT_FAILED", "native worker returned an invalid tensor shape")
    return TensorSpec(name, dtype, normalized_shape)


def _vllm_worker_export(
    worker: object,
    *,
    stages_root: str,
    expected_pipeline_size: int,
    max_part_bytes: int,
) -> dict[str, object]:
    """Runs inside each pinned vLLM V0 worker through collective_rpc."""

    try:
        from vllm.distributed import (
            get_pp_group,
            get_tensor_model_parallel_rank,
            get_tensor_model_parallel_world_size,
        )
        from vllm.model_executor.model_loader.sharded_state_loader import (
            ShardedStateLoader,
        )

        pp_group = get_pp_group()
        pp_rank = pp_group.rank_in_group
        if (
            pp_group.world_size != expected_pipeline_size
            or get_tensor_model_parallel_world_size() != 1
            or get_tensor_model_parallel_rank() != 0
        ):
            raise RuntimeError("unexpected vLLM model-parallel rank")
        model_runner = getattr(worker, "model_runner", None)
        model = getattr(model_runner, "model", None)
        if model is None:
            raise RuntimeError("vLLM worker has no initialized model runner")
        state = ShardedStateLoader._filter_subtensors(model.state_dict())
        tensors = sorted(
            (_torch_tensor_spec(name, tensor).to_dict() for name, tensor in state.items()),
            key=lambda item: item["name"],
        )
        if not tensors:
            raise RuntimeError("vLLM worker model has no serializable tensors")
        stage_directory = Path(stages_root) / str(pp_rank)
        if not stage_directory.is_dir() or stage_directory.is_symlink():
            raise RuntimeError("worker stage directory is unavailable")
        ShardedStateLoader.save_model(
            model,
            str(stage_directory),
            pattern=None,
            max_size=max_part_bytes,
        )
        return {"rank": pp_rank, "tensors": tensors}
    except Exception as exc:
        raise RuntimeError("native vLLM worker stage export failed") from exc


class VLLM090NativeStageExporter:
    def _verify_environment(self, contract: StageExportContract) -> None:
        if os.environ.get("DURE_STAGE_RUNTIME_IMAGE") != contract.runtime_image:
            _fail("STAGE_CONTRACT_REJECTED", "runtime image environment does not match the contract")
        if os.environ.get("DURE_STAGE_EXPORTER_BUILD_DIGEST") != contract.exporter_build_digest:
            _fail("STAGE_CONTRACT_REJECTED", "exporter build environment does not match the contract")
        try:
            observed = importlib.metadata.version("vllm")
        except importlib.metadata.PackageNotFoundError:
            _fail("STAGE_DEPENDENCY_UNAVAILABLE", "pinned vLLM is not installed")
        if observed != PINNED_VLLM_VERSION:
            _fail("STAGE_VLLM_VERSION_MISMATCH", "installed vLLM does not match the contract")

    def export(
        self,
        source: Path,
        workspace: Path,
        contract: StageExportContract,
    ) -> Sequence[WorkerStageExport]:
        self._verify_environment(contract)
        if "vllm" in sys.modules and os.environ.get("VLLM_USE_V1") != "0":
            _fail("STAGE_ENGINE_UNSUPPORTED", "vLLM was imported before the V0 engine was pinned")
        os.environ["VLLM_USE_V1"] = "0"
        try:
            from vllm import LLM
        except Exception:
            _fail("STAGE_DEPENDENCY_UNAVAILABLE", "pinned vLLM could not be imported")
        options: dict[str, object] = {
            "model": str(source),
            "tokenizer": str(source),
            "trust_remote_code": False,
            "quantization": SUPPORTED_QUANTIZATION,
            "tensor_parallel_size": 1,
            "pipeline_parallel_size": contract.pipeline_parallel_size,
            "load_format": "auto",
            "enable_lora": False,
            "enforce_eager": True,
        }
        if contract.pipeline_parallel_size > 1:
            options["distributed_executor_backend"] = "ray"
        try:
            llm = LLM(**options)
            engine = llm.llm_engine
            if hasattr(engine, "engine_core"):
                _fail("STAGE_ENGINE_UNSUPPORTED", "only the pinned vLLM V0 executor is supported")
            raw_results = engine.model_executor.collective_rpc(
                _vllm_worker_export,
                kwargs={
                    "stages_root": str(workspace / "stages"),
                    "expected_pipeline_size": contract.pipeline_parallel_size,
                    "max_part_bytes": contract.max_part_bytes,
                },
            )
        except StageArtifactError:
            raise
        except Exception:
            _fail("STAGE_EXPORT_FAILED", "pinned vLLM V0 collective export failed")
        exports: list[WorkerStageExport] = []
        for value in raw_results:
            source_result = _exact_dict(
                value,
                frozenset({"rank", "tensors"}),
                "STAGE_EXPORT_FAILED",
            )
            if type(source_result["tensors"]) is not list:
                _fail("STAGE_EXPORT_FAILED", "native worker result is invalid")
            tensors = tuple(
                sorted(
                    (TensorSpec.from_dict(item) for item in source_result["tensors"]),
                    key=lambda item: item.name,
                )
            )
            exports.append(WorkerStageExport(source_result["rank"], tensors))
        return exports


def build_stage_artifact_set(
    source: Path,
    output: Path,
    contract: StageExportContract,
    source_manifest: dict,
) -> StageArtifactSet:
    return TrustedStageBuilder(VLLM090NativeStageExporter()).build(
        source, output, contract, source_manifest
    )


def _contract_from_args(args: argparse.Namespace) -> StageExportContract:
    runtime_image = os.environ.get("DURE_STAGE_RUNTIME_IMAGE")
    exporter_digest = os.environ.get("DURE_STAGE_EXPORTER_BUILD_DIGEST")
    if runtime_image is None or exporter_digest is None:
        _fail(
            "STAGE_CONTRACT_REJECTED",
            "pinned builder image and exporter build digest environment are required",
        )
    return StageExportContract(
        source_manifest_digest=args.source_manifest_digest,
        runtime_image=runtime_image,
        exporter_build_digest=exporter_digest,
        pipeline_parallel_size=args.pipeline_parallel_size,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m dure.stage_artifact")
    subparsers = parser.add_subparsers(dest="command", required=True)
    build_parser = subparsers.add_parser("build")
    build_parser.add_argument("--source", type=Path, required=True)
    build_parser.add_argument("--source-manifest", type=Path, required=True)
    build_parser.add_argument("--output", type=Path, required=True)
    build_parser.add_argument("--source-manifest-digest", required=True)
    build_parser.add_argument("--pipeline-parallel-size", type=int, required=True)
    verify_parser = subparsers.add_parser("verify")
    verify_parser.add_argument("--artifact-set", type=Path, required=True)
    verify_parser.add_argument("--index-digest")
    args = parser.parse_args(argv)
    try:
        if args.command == "build":
            source_manifest = _decode_json_bytes(
                _read_regular(
                    args.source_manifest,
                    maximum=MAX_METADATA_BYTES,
                    code="STAGE_SOURCE_UNSAFE",
                ),
                code="STAGE_SOURCE_UNSAFE",
                maximum=MAX_METADATA_BYTES,
            )
            result = build_stage_artifact_set(
                args.source,
                args.output,
                _contract_from_args(args),
                source_manifest,
            )
        else:
            result = verify_stage_artifact_set(
                args.artifact_set,
                expected_index_digest=args.index_digest,
            )
    except StageArtifactError as exc:
        print(f"{exc.code}: {exc}", file=sys.stderr)
        return 2
    print(_canonical_json(result.to_dict()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

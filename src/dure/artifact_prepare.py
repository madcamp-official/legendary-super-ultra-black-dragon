from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from typing import Callable, Protocol

from .artifact_download import ArtifactChunkDownloader, TrustedHTTPSOrigin
from .command import CommandResult, Runner, SubprocessRunner
from .model_cache import (
    MODEL_CACHE_KIND_FULL_SNAPSHOT,
    MODEL_CACHE_KIND_STAGE,
    MODEL_CACHE_VERIFICATION_VERSION,
)
from .model_store import (
    MODEL_STORE_FAILURE_CODES,
    CacheIdentity,
    ContentAddressedModelStore,
    ModelCachePreparer,
    ModelStoreError,
    PreparedModelCache,
)
from .stage_cache import (
    STAGE_CACHE_VERIFICATION_VERSION,
    StageCacheError,
    StageCacheIdentity,
)


PREPARE_MODEL_TASK = "PREPARE_MODEL"
PREPARE_IMAGE_TASK = "PREPARE_IMAGE"
ARTIFACT_PREPARATION_TASK_TYPES = frozenset(
    {PREPARE_MODEL_TASK, PREPARE_IMAGE_TASK}
)
PREPARATION_STAGES = {
    PREPARE_MODEL_TASK: "MODEL",
    PREPARE_IMAGE_TASK: "IMAGE",
}
PREPARATION_FAILURE_CODES = frozenset(
    {
        "PREPARATION_PAYLOAD_REJECTED",
        "PREPARATION_NODE_MISMATCH",
        "PREPARATION_BINDING_MISMATCH",
        "PREPARATION_ORIGIN_UNAVAILABLE",
        "PREPARATION_MANIFEST_UNAVAILABLE",
        "PREPARATION_HISTORY_INVALID",
        "PREPARATION_RUNTIME_UNAVAILABLE",
        "PREPARATION_IMAGE_PULL_FAILED",
        "PREPARATION_IMAGE_INSPECT_FAILED",
        "PREPARATION_IMAGE_DIGEST_MISMATCH",
        "PREPARATION_EXECUTION_FAILED",
        *MODEL_STORE_FAILURE_CODES,
    }
)

_COMMON_PAYLOAD_FIELDS = frozenset(
    {
        "preparation_id",
        "preparation_node_id",
        "attempt_id",
        "attempt_no",
        "deployment_id",
        "generation",
        "node_id",
        "apply",
    }
)
_MODEL_BASE_PAYLOAD_FIELDS = _COMMON_PAYLOAD_FIELDS | frozenset(
    {
        "model_id",
        "repository",
        "revision",
        "manifest_digest",
        "quantization",
        "cache_kind",
    }
)
_FULL_MODEL_PAYLOAD_FIELDS = _MODEL_BASE_PAYLOAD_FIELDS
_STAGE_MODEL_PAYLOAD_FIELDS = _MODEL_BASE_PAYLOAD_FIELDS | frozenset(
    {
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
_IMAGE_PAYLOAD_FIELDS = _COMMON_PAYLOAD_FIELDS | frozenset({"runtime_image"})
_COMMON_RESULT_FIELDS = frozenset(
    {
        "preparation_id",
        "preparation_node_id",
        "attempt_id",
        "attempt_no",
        "deployment_id",
        "generation",
        "node_id",
        "stage",
        "reused",
    }
)
_MODEL_BASE_RESULT_FIELDS = _COMMON_RESULT_FIELDS | frozenset(
    {
        "model_id",
        "manifest_digest",
        "cache_kind",
        "verification_version",
        "bytes_verified",
        "file_count",
    }
)
_FULL_MODEL_RESULT_FIELDS = _MODEL_BASE_RESULT_FIELDS
_STAGE_MODEL_RESULT_FIELDS = _MODEL_BASE_RESULT_FIELDS | frozenset(
    {
        "artifact_set_digest",
        "pipeline_rank",
        "tensor_rank",
        "tensor_keys_digest",
        "cache_identity_digest",
    }
)
_IMAGE_RESULT_FIELDS = _COMMON_RESULT_FIELDS | frozenset(
    {"runtime_image", "image_id"}
)

_MODEL_ID = re.compile(r"[a-z0-9][a-z0-9._-]{0,99}")
_OCI_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
_OCI_NAME = re.compile(r"[a-z0-9](?:[a-z0-9._:/-]*[a-z0-9])?")
_MAX_BINDING_INTEGER = (1 << 31) - 1
_MAX_DOCKER_OUTPUT_BYTES = 256 * 1024
_IMAGE_INSPECT_FORMAT = "{{json .RepoDigests}}"


class ArtifactPreparationError(RuntimeError):
    _SAFE_MESSAGES = {
        "PREPARATION_PAYLOAD_REJECTED": "artifact preparation payload was rejected",
        "PREPARATION_NODE_MISMATCH": "artifact preparation node does not match this Agent",
        "PREPARATION_BINDING_MISMATCH": "artifact preparation binding does not match its task",
        "PREPARATION_ORIGIN_UNAVAILABLE": "trusted artifact origin is not configured",
        "PREPARATION_MANIFEST_UNAVAILABLE": "canonical artifact manifest is unavailable",
        "PREPARATION_HISTORY_INVALID": "artifact preparation history is invalid",
        "PREPARATION_RUNTIME_UNAVAILABLE": "container runtime is unavailable",
        "PREPARATION_IMAGE_PULL_FAILED": "digest-pinned image pull failed",
        "PREPARATION_IMAGE_INSPECT_FAILED": "digest-pinned image inspection failed",
        "PREPARATION_IMAGE_DIGEST_MISMATCH": "local image digest did not match",
        "PREPARATION_EXECUTION_FAILED": "artifact preparation execution failed",
    }

    def __init__(self, code: str) -> None:
        if code not in PREPARATION_FAILURE_CODES or code in MODEL_STORE_FAILURE_CODES:
            raise ValueError("unsupported artifact preparation failure code")
        self.code = code
        self.failure_code = code
        super().__init__(self._SAFE_MESSAGES[code])


class FullSnapshotPreparer(Protocol):
    def prepare_full_snapshot(
        self,
        *,
        identity: CacheIdentity,
        manifest: dict,
        origin: object,
    ) -> PreparedModelCache: ...

    def prepare_stage(
        self,
        *,
        identity: StageCacheIdentity,
        manifest: dict,
        origin: object,
    ) -> PreparedModelCache: ...


def is_artifact_preparation_task(value: object) -> bool:
    return type(value) is str and value in ARTIFACT_PREPARATION_TASK_TYPES


def preparation_failure_code(exc: Exception) -> str:
    try:
        value = getattr(exc, "failure_code", None)
    except Exception:
        return "PREPARATION_EXECUTION_FAILED"
    return (
        value
        if type(value) is str and value in PREPARATION_FAILURE_CODES
        else "PREPARATION_EXECUTION_FAILED"
    )


def _canonical_uuid(value: object) -> str:
    if type(value) is not str or len(value) != 36:
        raise ArtifactPreparationError("PREPARATION_PAYLOAD_REJECTED")
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ArtifactPreparationError("PREPARATION_PAYLOAD_REJECTED") from None
    if str(parsed) != value:
        raise ArtifactPreparationError("PREPARATION_PAYLOAD_REJECTED")
    return value


def _positive_integer(value: object) -> int:
    if type(value) is not int or not 1 <= value <= _MAX_BINDING_INTEGER:
        raise ArtifactPreparationError("PREPARATION_PAYLOAD_REJECTED")
    return value


def validate_digest_pinned_runtime_image(value: object) -> tuple[str, str]:
    if (
        type(value) is not str
        or not 1 <= len(value) <= 512
        or value.count("@") != 1
        or any(ord(character) < 0x21 or ord(character) > 0x7E for character in value)
        or "\\" in value
    ):
        raise ValueError("runtime image must be a supported OCI digest reference")
    name, digest = value.rsplit("@", 1)
    segments = name.split("/")
    if (
        _OCI_NAME.fullmatch(name) is None
        or _OCI_DIGEST.fullmatch(digest) is None
        or any(segment in {"", ".", ".."} for segment in segments)
        or ":" in segments[-1]
        or "//" in name
    ):
        raise ValueError("runtime image must be a supported OCI digest reference")
    return value, digest


def _validated_runtime_image(value: object) -> tuple[str, str]:
    try:
        return validate_digest_pinned_runtime_image(value)
    except ValueError:
        raise ArtifactPreparationError(
            "PREPARATION_PAYLOAD_REJECTED"
        ) from None


@dataclass(frozen=True)
class PreparationBinding:
    preparation_id: str
    preparation_node_id: str
    attempt_id: str
    attempt_no: int
    deployment_id: str
    generation: int
    node_id: str

    @classmethod
    def from_task(
        cls,
        task: object,
        *,
        expected_node_id: str,
        expected_type: str,
    ) -> tuple["PreparationBinding", dict]:
        if type(task) is not dict or task.get("type") != expected_type:
            raise ArtifactPreparationError("PREPARATION_PAYLOAD_REJECTED")
        _canonical_uuid(task.get("id"))
        payload = task.get("payload")
        if expected_type == PREPARE_MODEL_TASK:
            cache_kind = payload.get("cache_kind") if type(payload) is dict else None
            if cache_kind == MODEL_CACHE_KIND_FULL_SNAPSHOT:
                expected_fields = _FULL_MODEL_PAYLOAD_FIELDS
            elif cache_kind == MODEL_CACHE_KIND_STAGE:
                expected_fields = _STAGE_MODEL_PAYLOAD_FIELDS
            else:
                raise ArtifactPreparationError("PREPARATION_PAYLOAD_REJECTED")
        else:
            expected_fields = _IMAGE_PAYLOAD_FIELDS
        if (
            type(payload) is not dict
            or any(type(key) is not str for key in payload)
            or set(payload) != expected_fields
            or payload.get("apply") is not True
        ):
            raise ArtifactPreparationError("PREPARATION_PAYLOAD_REJECTED")

        binding = cls(
            preparation_id=_canonical_uuid(payload["preparation_id"]),
            preparation_node_id=_canonical_uuid(
                payload["preparation_node_id"]
            ),
            attempt_id=_canonical_uuid(payload["attempt_id"]),
            attempt_no=_positive_integer(payload["attempt_no"]),
            deployment_id=_canonical_uuid(payload["deployment_id"]),
            generation=_positive_integer(payload["generation"]),
            node_id=_canonical_uuid(payload["node_id"]),
        )
        if binding.node_id != expected_node_id or task.get("node_id") != expected_node_id:
            raise ArtifactPreparationError("PREPARATION_NODE_MISMATCH")
        if task.get("deployment_id") != binding.deployment_id:
            raise ArtifactPreparationError("PREPARATION_BINDING_MISMATCH")
        return binding, payload

    def result_binding(self, *, stage: str, reused: bool) -> dict:
        return {
            "preparation_id": self.preparation_id,
            "preparation_node_id": self.preparation_node_id,
            "attempt_id": self.attempt_id,
            "attempt_no": self.attempt_no,
            "deployment_id": self.deployment_id,
            "generation": self.generation,
            "node_id": self.node_id,
            "stage": stage,
            "reused": reused,
        }


def trusted_origin_from_config(value: object) -> TrustedHTTPSOrigin | None:
    if value is None:
        return None
    if (
        type(value) is not dict
        or any(type(key) is not str for key in value)
        or set(value) != {"base_url", "allowed_redirect_hosts"}
        or type(value["allowed_redirect_hosts"]) is not list
        or any(type(item) is not str for item in value["allowed_redirect_hosts"])
    ):
        raise ValueError("agent artifact_origin must use the closed local schema")
    try:
        return TrustedHTTPSOrigin(
            value["base_url"], tuple(value["allowed_redirect_hosts"])
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("agent artifact_origin is invalid") from None


def _bounded_run(
    runner: Runner,
    argv: list[str],
    *,
    timeout: float,
) -> CommandResult:
    limited = getattr(runner, "run_limited_output", None)
    if callable(limited):
        result = limited(
            argv,
            timeout=timeout,
            max_output_bytes=_MAX_DOCKER_OUTPUT_BYTES,
        )
    else:
        result = runner.run(argv, timeout=timeout)
    if (
        len(result.stdout.encode("utf-8", errors="replace"))
        + len(result.stderr.encode("utf-8", errors="replace"))
        > _MAX_DOCKER_OUTPUT_BYTES
    ):
        return CommandResult(tuple(argv), 125)
    return result


def _inspect_exact_image(runner: Runner, runtime_image: str) -> tuple[bool, bool]:
    result = _bounded_run(
        runner,
        [
            "docker",
            "image",
            "inspect",
            "--format",
            _IMAGE_INSPECT_FORMAT,
            runtime_image,
        ],
        timeout=30,
    )
    if not result.ok:
        return False, False
    try:
        repo_digests = json.loads(result.stdout)
    except (RecursionError, ValueError):
        return True, False
    exact = (
        type(repo_digests) is list
        and 1 <= len(repo_digests) <= 1024
        and all(type(item) is str and len(item) <= 519 for item in repo_digests)
        and runtime_image in repo_digests
    )
    return True, exact


class ArtifactPreparationExecutor:
    def __init__(
        self,
        node_id: str,
        *,
        runner: Runner | None = None,
        origin_config: object = None,
        origin: TrustedHTTPSOrigin | None = None,
        model_preparer: FullSnapshotPreparer | None = None,
        manifest_loader: Callable[[str], dict] | None = None,
    ) -> None:
        if type(node_id) is not str or not node_id:
            raise ValueError("Agent node identity is invalid")
        # Older local tests and profiles may still use hostname-like node IDs.
        # Preparation payload validation nevertheless requires a canonical UUID,
        # so those identities can keep using non-central Agent functionality but
        # can never accept a central preparation task.
        self.node_id = node_id
        self.runner = runner or SubprocessRunner()
        if origin is not None and origin_config is not None:
            raise ValueError("trusted artifact origin is configured twice")
        if origin is not None and type(origin) is not TrustedHTTPSOrigin:
            raise ValueError("trusted artifact origin is invalid")
        self.origin = origin or trusted_origin_from_config(origin_config)
        if model_preparer is None:
            store = ContentAddressedModelStore()
            downloader = ArtifactChunkDownloader(store)
            model_preparer = ModelCachePreparer(store, downloader)
        if not hasattr(model_preparer, "prepare_full_snapshot"):
            raise ValueError("model cache preparer is invalid")
        self.model_preparer = model_preparer
        if manifest_loader is not None and not callable(manifest_loader):
            raise ValueError("artifact manifest loader is invalid")
        self.manifest_loader = manifest_loader

    def _prepare_model(self, task: dict) -> dict:
        binding, payload = PreparationBinding.from_task(
            task,
            expected_node_id=self.node_id,
            expected_type=PREPARE_MODEL_TASK,
        )
        if (
            type(payload["model_id"]) is not str
            or _MODEL_ID.fullmatch(payload["model_id"]) is None
        ):
            raise ArtifactPreparationError("PREPARATION_PAYLOAD_REJECTED")
        try:
            if payload["cache_kind"] == MODEL_CACHE_KIND_FULL_SNAPSHOT:
                identity: CacheIdentity | StageCacheIdentity = CacheIdentity(
                    repository=payload["repository"],
                    revision=payload["revision"],
                    manifest_digest=payload["manifest_digest"],
                    quantization=payload["quantization"],
                    cache_kind=payload["cache_kind"],
                )
            else:
                _validated_runtime_image(payload["runtime_image"])
                identity = StageCacheIdentity(
                    repository=payload["repository"],
                    revision=payload["revision"],
                    manifest_digest=payload["manifest_digest"],
                    quantization=payload["quantization"],
                    artifact_set_digest=payload["artifact_set_digest"],
                    contract_identity_digest=payload[
                        "contract_identity_digest"
                    ],
                    source_manifest_digest=payload[
                        "source_manifest_digest"
                    ],
                    runtime_image=payload["runtime_image"],
                    vllm_version=payload["vllm_version"],
                    exporter_build_digest=payload[
                        "exporter_build_digest"
                    ],
                    architecture=payload["architecture"],
                    loader_format=payload["loader_format"],
                    tensor_parallel_size=payload["tensor_parallel_size"],
                    pipeline_parallel_size=payload[
                        "pipeline_parallel_size"
                    ],
                    pipeline_rank=payload["pipeline_rank"],
                    tensor_rank=payload["tensor_rank"],
                    tensor_keys_digest=payload["tensor_keys_digest"],
                )
        except (ArtifactPreparationError, ModelStoreError, StageCacheError):
            raise ArtifactPreparationError("PREPARATION_PAYLOAD_REJECTED") from None
        if self.origin is None:
            raise ArtifactPreparationError("PREPARATION_ORIGIN_UNAVAILABLE")
        if self.manifest_loader is None:
            raise ArtifactPreparationError("PREPARATION_MANIFEST_UNAVAILABLE")
        try:
            manifest = self.manifest_loader(task["id"])
        except Exception:
            raise ArtifactPreparationError(
                "PREPARATION_MANIFEST_UNAVAILABLE"
            ) from None
        if type(manifest) is not dict:
            raise ArtifactPreparationError(
                "PREPARATION_MANIFEST_UNAVAILABLE"
            )
        try:
            if type(identity) is StageCacheIdentity:
                prepare_stage = getattr(self.model_preparer, "prepare_stage", None)
                if not callable(prepare_stage):
                    raise ArtifactPreparationError(
                        "PREPARATION_EXECUTION_FAILED"
                    )
                prepared = prepare_stage(
                    identity=identity,
                    manifest=manifest,
                    origin=self.origin,
                )
            else:
                prepared = self.model_preparer.prepare_full_snapshot(
                    identity=identity,
                    manifest=manifest,
                    origin=self.origin,
                )
        except ModelStoreError:
            raise
        except Exception:
            raise ArtifactPreparationError("PREPARATION_EXECUTION_FAILED") from None
        if (
            type(prepared) is not PreparedModelCache
            or prepared.identity != identity
            or type(prepared.reused) is not bool
            or type(prepared.file_count) is not int
            or prepared.file_count < 1
            or type(prepared.total_size_bytes) is not int
            or prepared.total_size_bytes < 1
        ):
            raise ArtifactPreparationError("PREPARATION_EXECUTION_FAILED")
        result: dict = {
            **binding.result_binding(stage="MODEL", reused=prepared.reused),
            "model_id": payload["model_id"],
            "manifest_digest": identity.manifest_digest,
            "cache_kind": identity.cache_kind,
            "verification_version": (
                STAGE_CACHE_VERIFICATION_VERSION
                if type(identity) is StageCacheIdentity
                else MODEL_CACHE_VERIFICATION_VERSION
            ),
            "bytes_verified": prepared.total_size_bytes,
            "file_count": prepared.file_count,
        }
        if type(identity) is StageCacheIdentity:
            result.update(
                artifact_set_digest=identity.artifact_set_digest,
                pipeline_rank=identity.pipeline_rank,
                tensor_rank=identity.tensor_rank,
                tensor_keys_digest=identity.tensor_keys_digest,
                cache_identity_digest=identity.cache_identity_digest,
            )
        return validate_preparation_result(task, result, self.node_id)

    def _prepare_image(self, task: dict) -> dict:
        binding, payload = PreparationBinding.from_task(
            task,
            expected_node_id=self.node_id,
            expected_type=PREPARE_IMAGE_TASK,
        )
        runtime_image, image_id = _validated_runtime_image(
            payload["runtime_image"]
        )
        if not self.runner.exists("docker"):
            raise ArtifactPreparationError("PREPARATION_RUNTIME_UNAVAILABLE")
        inspected, exact = _inspect_exact_image(self.runner, runtime_image)
        if inspected:
            if not exact:
                raise ArtifactPreparationError(
                    "PREPARATION_IMAGE_DIGEST_MISMATCH"
                )
            reused = True
        else:
            pulled = _bounded_run(
                self.runner,
                ["docker", "pull", "--quiet", runtime_image],
                timeout=1800,
            )
            if not pulled.ok:
                raise ArtifactPreparationError(
                    "PREPARATION_IMAGE_PULL_FAILED"
                )
            inspected, exact = _inspect_exact_image(self.runner, runtime_image)
            if not inspected:
                raise ArtifactPreparationError(
                    "PREPARATION_IMAGE_INSPECT_FAILED"
                )
            if not exact:
                raise ArtifactPreparationError(
                    "PREPARATION_IMAGE_DIGEST_MISMATCH"
                )
            reused = False
        result = {
            **binding.result_binding(stage="IMAGE", reused=reused),
            "runtime_image": runtime_image,
            "image_id": image_id,
        }
        return validate_preparation_result(task, result, self.node_id)

    def execute(self, task: dict) -> dict:
        task_type = task.get("type") if type(task) is dict else None
        if task_type == PREPARE_MODEL_TASK:
            return self._prepare_model(task)
        if task_type == PREPARE_IMAGE_TASK:
            return self._prepare_image(task)
        raise ArtifactPreparationError("PREPARATION_PAYLOAD_REJECTED")


def validate_preparation_result(
    task: object,
    result: object,
    expected_node_id: str,
) -> dict:
    task_type = task.get("type") if type(task) is dict else None
    if not is_artifact_preparation_task(task_type):
        raise ArtifactPreparationError("PREPARATION_HISTORY_INVALID")
    try:
        binding, payload = PreparationBinding.from_task(
            task,
            expected_node_id=expected_node_id,
            expected_type=task_type,
        )
    except ArtifactPreparationError:
        raise ArtifactPreparationError("PREPARATION_HISTORY_INVALID") from None
    if task_type == PREPARE_MODEL_TASK:
        expected_fields = (
            _STAGE_MODEL_RESULT_FIELDS
            if payload["cache_kind"] == MODEL_CACHE_KIND_STAGE
            else _FULL_MODEL_RESULT_FIELDS
        )
    else:
        expected_fields = _IMAGE_RESULT_FIELDS
    expected_binding = binding.result_binding(
        stage=PREPARATION_STAGES[task_type],
        reused=result.get("reused") if type(result) is dict else False,
    )
    if (
        type(result) is not dict
        or any(type(key) is not str for key in result)
        or set(result) != expected_fields
        or type(result.get("reused")) is not bool
        or any(result.get(key) != value for key, value in expected_binding.items())
    ):
        raise ArtifactPreparationError("PREPARATION_HISTORY_INVALID")
    if task_type == PREPARE_MODEL_TASK:
        if (
            result["model_id"] != payload["model_id"]
            or result["manifest_digest"] != payload["manifest_digest"]
            or result["cache_kind"] != payload["cache_kind"]
            or type(result["verification_version"]) is not int
            or type(result["bytes_verified"]) is not int
            or result["bytes_verified"] < 1
            or type(result["file_count"]) is not int
            or result["file_count"] < 1
        ):
            raise ArtifactPreparationError("PREPARATION_HISTORY_INVALID")
        if payload["cache_kind"] == MODEL_CACHE_KIND_FULL_SNAPSHOT:
            if (
                result["verification_version"]
                != MODEL_CACHE_VERIFICATION_VERSION
            ):
                raise ArtifactPreparationError("PREPARATION_HISTORY_INVALID")
        else:
            try:
                identity = StageCacheIdentity(
                    repository=payload["repository"],
                    revision=payload["revision"],
                    manifest_digest=payload["manifest_digest"],
                    quantization=payload["quantization"],
                    artifact_set_digest=payload["artifact_set_digest"],
                    contract_identity_digest=payload[
                        "contract_identity_digest"
                    ],
                    source_manifest_digest=payload[
                        "source_manifest_digest"
                    ],
                    runtime_image=payload["runtime_image"],
                    vllm_version=payload["vllm_version"],
                    exporter_build_digest=payload[
                        "exporter_build_digest"
                    ],
                    architecture=payload["architecture"],
                    loader_format=payload["loader_format"],
                    tensor_parallel_size=payload["tensor_parallel_size"],
                    pipeline_parallel_size=payload[
                        "pipeline_parallel_size"
                    ],
                    pipeline_rank=payload["pipeline_rank"],
                    tensor_rank=payload["tensor_rank"],
                    tensor_keys_digest=payload["tensor_keys_digest"],
                )
            except StageCacheError:
                raise ArtifactPreparationError(
                    "PREPARATION_HISTORY_INVALID"
                ) from None
            if (
                result["verification_version"]
                != STAGE_CACHE_VERIFICATION_VERSION
                or result["artifact_set_digest"]
                != identity.artifact_set_digest
                or type(result["pipeline_rank"]) is not int
                or result["pipeline_rank"] != identity.pipeline_rank
                or type(result["tensor_rank"]) is not int
                or result["tensor_rank"] != identity.tensor_rank
                or result["tensor_keys_digest"]
                != identity.tensor_keys_digest
                or result["cache_identity_digest"]
                != identity.cache_identity_digest
            ):
                raise ArtifactPreparationError("PREPARATION_HISTORY_INVALID")
    else:
        try:
            runtime_image, image_id = _validated_runtime_image(
                payload["runtime_image"]
            )
        except ArtifactPreparationError:
            raise ArtifactPreparationError("PREPARATION_HISTORY_INVALID") from None
        if (
            result["runtime_image"] != runtime_image
            or result["image_id"] != image_id
        ):
            raise ArtifactPreparationError("PREPARATION_HISTORY_INVALID")
    return dict(result)


def validate_preparation_history(
    task: object,
    previous: object,
    expected_node_id: str,
) -> tuple[str, dict | str]:
    task_type = task.get("type") if type(task) is dict else None
    if not is_artifact_preparation_task(task_type):
        raise ArtifactPreparationError("PREPARATION_HISTORY_INVALID")
    try:
        PreparationBinding.from_task(
            task,
            expected_node_id=expected_node_id,
            expected_type=task_type,
        )
    except ArtifactPreparationError:
        raise ArtifactPreparationError("PREPARATION_HISTORY_INVALID") from None
    if type(previous) is not dict:
        raise ArtifactPreparationError("PREPARATION_HISTORY_INVALID")
    if previous.get("status") == "failed" and set(previous) == {"status", "error"}:
        error = previous.get("error")
        if type(error) is str and error in PREPARATION_FAILURE_CODES:
            return "failed", error
        raise ArtifactPreparationError("PREPARATION_HISTORY_INVALID")
    if previous.get("status") == "complete" and set(previous) == {
        "status",
        "result",
    }:
        return (
            "complete",
            validate_preparation_result(
                task, previous.get("result"), expected_node_id
            ),
        )
    raise ArtifactPreparationError("PREPARATION_HISTORY_INVALID")

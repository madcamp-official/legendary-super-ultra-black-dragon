from __future__ import annotations

import hashlib
import json
import math
import re
import uuid
from pathlib import Path

from .artifact_prepare import validate_digest_pinned_runtime_image
from .model_cache import (
    MODEL_CACHE_KIND_FULL_SNAPSHOT,
    MODEL_CACHE_KIND_STAGE,
    MODEL_CACHE_VERIFICATION_VERSION,
)
from .models import (
    DeploymentPlan,
    NodeAssignment,
    NodeProfile,
    VLLM_RAY_PP_BACKEND,
    VLLM_RAY_PP_RUNTIME_VERSION,
    VLLM_STAGE_ARCHITECTURE,
    VLLM_STAGE_LOADER_FORMAT,
)
from .stage_cache import (
    StageCacheError,
    StageCacheIdentity,
    StageCacheValidation,
    stage_cache_path,
    validate_materialized_stage_cache,
)


PIPELINE_CONTRACT_CHECK = "pipeline-rank-contract"
PIPELINE_CONTRACT_SCHEMA_VERSION = 1
RAY_GCS_PORT = 6379
RAY_MIN_WORKER_PORT = 20000
RAY_MAX_WORKER_PORT = 21000
VLLM_API_HOST = "127.0.0.1"
VLLM_API_PORT = 8000
RAY_SESSION_CONTAINER_PATH = "/tmp/ray"
RAY_COMPONENT = "ray-node"
VLLM_API_COMPONENT = "vllm-api"
STRICT_IDENTITY_COMPONENTS = frozenset({RAY_COMPONENT, VLLM_API_COMPONENT})
RAY_DURE_NODE_RESOURCE_PREFIX = "dure_node_"
STRICT_RUNTIME_CONTRACT_LABEL = "dure.runtime-contract"
STRICT_CACHE_KIND_LABEL = "dure.cache-kind"
STRICT_STAGE_VARIANT_LABEL = "dure.stage-variant"
STRICT_STAGE_MANIFEST_LABEL = "dure.stage-manifest"
STRICT_STAGE_CACHE_IDENTITY_LABEL = "dure.stage-cache-identity"
STAGE_CACHE_CHECK = "stage-cache"

_TRUSTED_MODEL_ROOT = Path("/var/lib/dure/models")
_TRUSTED_STAGE_ROOT = _TRUSTED_MODEL_ROOT / "stages"
_TRUSTED_RUNTIME_ROOT = Path("/var/lib/dure/runtime")
_NETWORK_INTERFACE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,14}")
_MODEL_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_MODEL_REPOSITORY = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
_MODEL_REVISION = re.compile(r"[0-9a-f]{40,64}")
_QUANTIZATION = re.compile(r"[a-z0-9][a-z0-9._-]{1,39}")
_CACHE_DIRECTORY = re.compile(r"sha256-([0-9a-f]{64})")


class PipelineRuntimeContractError(ValueError):
    """A strict pipeline plan cannot be executed without changing its meaning."""


def is_strict_pipeline_plan(plan: DeploymentPlan) -> bool:
    return plan.execution_backend == VLLM_RAY_PP_BACKEND


def is_stage_pipeline_plan(plan: DeploymentPlan) -> bool:
    return (
        is_strict_pipeline_plan(plan)
        and plan.model_cache_kind == MODEL_CACHE_KIND_STAGE
    )


def is_direct_single_gpu_plan(plan: DeploymentPlan) -> bool:
    """Return whether a legacy-compatible plan can run without Ray."""

    if (
        plan.execution_backend is not None
        or plan.pipeline_parallel_size != 1
        or plan.tensor_parallel_size != 1
        or len(plan.assignments) != 1
    ):
        return False
    assignment = plan.assignments[0]
    return (
        assignment.node_id == plan.ray_head_node_id
        and assignment.role == "ray-head"
        and assignment.rank == 0
        and assignment.pipeline_rank == 0
    )


def _canonical_uuid(value: object, field: str) -> str:
    if type(value) is not str:
        raise PipelineRuntimeContractError(f"{field} must be a canonical UUID")
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, ValueError) as exc:
        raise PipelineRuntimeContractError(
            f"{field} must be a canonical UUID"
        ) from exc
    if str(parsed) != value:
        raise PipelineRuntimeContractError(f"{field} must be a canonical UUID")
    return value


def validate_strict_pipeline_plan(
    plan: DeploymentPlan,
    *,
    require_manifest_cache_path: bool = True,
    validate_model_path: bool = True,
) -> None:
    """Validate every fixed input consumed by the strict runtime.

    ``DeploymentPlan.validate_execution_contract`` owns topology ordering.  This
    function adds host-runtime constraints so a central task cannot smuggle an
    image option, mount path, environment value, or unsupported vLLM setting
    into Docker.
    """

    if not is_strict_pipeline_plan(plan):
        return
    try:
        plan.validate_execution_contract()
    except (AttributeError, TypeError, ValueError) as exc:
        raise PipelineRuntimeContractError(str(exc)) from exc

    _canonical_uuid(plan.deployment_id, "deployment_id")
    if type(plan.generation) is not int or plan.generation < 1:
        raise PipelineRuntimeContractError("generation must be a positive integer")
    try:
        validate_digest_pinned_runtime_image(plan.image)
    except ValueError as exc:
        raise PipelineRuntimeContractError(
            "strict pipeline image must be pinned to one OCI sha256 digest"
        ) from exc
    if plan.runtime_vllm_version != VLLM_RAY_PP_RUNTIME_VERSION or (
        plan.model_cache_kind
        not in {MODEL_CACHE_KIND_FULL_SNAPSHOT, MODEL_CACHE_KIND_STAGE}
    ):
        raise PipelineRuntimeContractError(
            "strict pipeline requires the pinned vLLM and a supported cache contract"
        )
    if (
        type(plan.network_interface) is not str
        or _NETWORK_INTERFACE.fullmatch(plan.network_interface) is None
    ):
        raise PipelineRuntimeContractError(
            "network_interface is not a safe Linux interface"
        )
    if (
        type(plan.model.model_id) is not str
        or _MODEL_ID.fullmatch(plan.model.model_id) is None
        or type(plan.model.repository) is not str
        or _MODEL_REPOSITORY.fullmatch(plan.model.repository) is None
        or type(plan.model.quantization) is not str
        or _QUANTIZATION.fullmatch(plan.model.quantization) is None
        or type(plan.model_revision) is not str
        or _MODEL_REVISION.fullmatch(plan.model_revision) is None
    ):
        raise PipelineRuntimeContractError(
            "model identity is not immutable and canonical"
        )
    if (
        type(plan.gpu_memory_utilization) not in {int, float}
        or not math.isfinite(float(plan.gpu_memory_utilization))
        or not 0 < float(plan.gpu_memory_utilization) <= 1
        or type(plan.max_model_len) is not int
        or plan.max_model_len < 1
    ):
        raise PipelineRuntimeContractError("vLLM resource limits are invalid")
    if (
        type(plan.model.checkpoint_gib) not in {int, float}
        or not math.isfinite(float(plan.model.checkpoint_gib))
        or float(plan.model.checkpoint_gib) <= 0
        or type(plan.model.min_gpu_memory_gib) not in {int, float}
        or not math.isfinite(float(plan.model.min_gpu_memory_gib))
        or float(plan.model.min_gpu_memory_gib) <= 0
        or type(plan.model.default_max_model_len) is not int
        or plan.model.default_max_model_len < 1
        or type(plan.model.layer_count) is not int
        or plan.model.layer_count < 1
    ):
        raise PipelineRuntimeContractError("model resource metadata is invalid")

    if type(plan.model_path) is not str or not 1 <= len(plan.model_path) <= 4096:
        raise PipelineRuntimeContractError("model_path must be a trusted absolute path")
    if is_stage_pipeline_plan(plan):
        if (
            plan.model_path != str(_TRUSTED_STAGE_ROOT)
            or plan.stage_artifact is None
            or plan.stage_artifact.architecture != VLLM_STAGE_ARCHITECTURE
            or plan.stage_artifact.loader_format != VLLM_STAGE_LOADER_FORMAT
        ):
            raise PipelineRuntimeContractError(
                "STAGE pipeline requires the fixed Dure stage root and loader contract"
            )
        return
    if not validate_model_path:
        return
    candidate = Path(plan.model_path)
    try:
        resolved_root = _TRUSTED_MODEL_ROOT.resolve(strict=False)
        resolved_candidate = candidate.resolve(strict=False)
    except (OSError, RuntimeError, ValueError) as exc:
        raise PipelineRuntimeContractError("model_path cannot be resolved safely") from exc
    if (
        not candidate.is_absolute()
        or resolved_candidate == resolved_root
        or not resolved_candidate.is_relative_to(resolved_root)
        or str(resolved_candidate) != plan.model_path
    ):
        raise PipelineRuntimeContractError(
            "model_path must be a canonical child of the Dure model root"
        )
    if require_manifest_cache_path and (
        resolved_candidate.parent != resolved_root
        or _CACHE_DIRECTORY.fullmatch(resolved_candidate.name) is None
    ):
        raise PipelineRuntimeContractError(
            "model_path must be the canonical manifest-addressed Dure cache directory"
        )


def validate_strict_pipeline_node(
    plan: DeploymentPlan,
    assignment: NodeAssignment,
    profile: NodeProfile,
    *,
    require_model_cache: bool = True,
) -> None:
    """Bind a structurally valid strict plan to this probed node."""

    if not is_strict_pipeline_plan(plan):
        return
    validate_strict_pipeline_plan(plan)
    if assignment not in plan.assignments or assignment.node_id != profile.node_id:
        raise PipelineRuntimeContractError("assignment does not belong to this node")
    if assignment.runtime_address not in profile.network.addresses:
        raise PipelineRuntimeContractError(
            "planned runtime_address is not present on this node"
        )
    if (
        not profile.network.default_interface_addresses
        or assignment.runtime_address
        not in profile.network.default_interface_addresses
    ):
        raise PipelineRuntimeContractError(
            "planned runtime_address is not bound to the default interface"
        )
    if profile.network.default_interface != plan.network_interface:
        raise PipelineRuntimeContractError(
            "planned network_interface does not match this node"
        )
    healthy_gpus = [gpu for gpu in profile.gpus if gpu.healthy]
    matching_gpus = [
        gpu for gpu in healthy_gpus if gpu.index == assignment.gpu_index
    ]
    if len(matching_gpus) != 1:
        raise PipelineRuntimeContractError(
            "strict pipeline requires exactly one selected healthy GPU on this node"
        )
    if (
        assignment.gpu_uuid is not None
        and matching_gpus[0].uuid != assignment.gpu_uuid
    ):
        raise PipelineRuntimeContractError(
            "selected GPU UUID no longer matches the deployment plan"
        )
    if (
        type(matching_gpus[0].memory_mib) is not int
        or matching_gpus[0].memory_mib
        < float(plan.model.min_gpu_memory_gib) * 1024
    ):
        raise PipelineRuntimeContractError(
            "selected GPU no longer meets the strict pipeline memory requirement"
        )
    if (
        profile.runtime.engine != "docker"
        or not profile.runtime.engine_ready
        or not profile.runtime.nvidia_runtime
    ):
        raise PipelineRuntimeContractError(
            "strict pipeline requires a ready Docker NVIDIA runtime"
        )
    if not require_model_cache or is_stage_pipeline_plan(plan):
        return

    try:
        expected_path = str(Path(plan.model_path).resolve(strict=False))
    except (OSError, RuntimeError, ValueError) as exc:
        raise PipelineRuntimeContractError("model_path cannot be resolved safely") from exc
    matches = []
    cache_match = _CACHE_DIRECTORY.fullmatch(Path(expected_path).name)
    if cache_match is None:
        raise PipelineRuntimeContractError(
            "strict pipeline cache path is not manifest-addressed"
        )
    expected_manifest_digest = f"sha256:{cache_match.group(1)}"
    for model in profile.installed_models:
        if type(model.path) is not str:
            continue
        try:
            observed_path = str(Path(model.path).resolve(strict=False))
        except (OSError, RuntimeError, ValueError):
            continue
        if (
            model.source == "dure"
            and model.complete
            and observed_path == expected_path
            and model.model_id == plan.model.repository
            and model.revision == plan.model_revision
            and model.quantization == plan.model.quantization
            and model.manifest_digest == expected_manifest_digest
            and model.cache_kind == MODEL_CACHE_KIND_FULL_SNAPSHOT
            and model.verification_version == MODEL_CACHE_VERIFICATION_VERSION
        ):
            matches.append(model)
    if len(matches) != 1:
        raise PipelineRuntimeContractError(
            "strict pipeline requires exactly one verified FULL_SNAPSHOT model cache"
        )


def stage_cache_identity(
    plan: DeploymentPlan,
    assignment: NodeAssignment,
) -> StageCacheIdentity:
    """Build the only stage cache identity accepted by the strict runtime."""

    validate_strict_pipeline_plan(plan)
    if not is_stage_pipeline_plan(plan) or assignment not in plan.assignments:
        raise PipelineRuntimeContractError(
            "stage cache identity is not bound to a STAGE plan assignment"
        )
    stage = plan.stage_artifact
    assert stage is not None
    try:
        return StageCacheIdentity(
            repository=plan.model.repository,
            revision=plan.model_revision,
            manifest_digest=assignment.stage_manifest_digest,
            quantization=plan.model.quantization,
            artifact_set_digest=stage.artifact_set_digest,
            contract_identity_digest=stage.contract_identity_digest,
            source_manifest_digest=stage.source_manifest_digest,
            runtime_image=stage.runtime_image,
            vllm_version=stage.vllm_version,
            exporter_build_digest=stage.exporter_build_digest,
            architecture=stage.architecture,
            loader_format=stage.loader_format,
            tensor_parallel_size=stage.tensor_parallel_size,
            pipeline_parallel_size=stage.pipeline_parallel_size,
            pipeline_rank=assignment.pipeline_rank,
            tensor_rank=0,
            tensor_keys_digest=assignment.stage_tensor_keys_digest,
        )
    except (TypeError, ValueError) as exc:
        raise PipelineRuntimeContractError(
            "stage cache identity does not match the strict plan"
        ) from exc


def strict_model_mount_path(
    plan: DeploymentPlan,
    assignment: NodeAssignment,
) -> Path:
    """Resolve a fixed host source; no task payload may provide this path."""

    validate_strict_pipeline_plan(plan)
    if assignment not in plan.assignments:
        raise PipelineRuntimeContractError(
            "model mount is not bound to a plan assignment"
        )
    if not is_stage_pipeline_plan(plan):
        return Path(plan.model_path)
    candidate = stage_cache_path(stage_cache_identity(plan, assignment))
    try:
        expected_parent = _TRUSTED_STAGE_ROOT.resolve(strict=False)
        resolved = Path(candidate).resolve(strict=False)
    except (OSError, RuntimeError, ValueError) as exc:
        raise PipelineRuntimeContractError(
            "stage cache path cannot be resolved safely"
        ) from exc
    if resolved.parent != expected_parent or str(resolved) != str(candidate):
        raise PipelineRuntimeContractError(
            "stage cache resolver returned a path outside the fixed Dure stage root"
        )
    return Path(candidate)


def validate_strict_stage_cache(
    plan: DeploymentPlan,
    assignment: NodeAssignment,
) -> StageCacheValidation | None:
    """Rehash the exact rank-local stage before a container can consume it."""

    if not is_stage_pipeline_plan(plan):
        return None
    identity = stage_cache_identity(plan, assignment)
    path = strict_model_mount_path(plan, assignment)
    try:
        return validate_materialized_stage_cache(path, identity)
    except StageCacheError as exc:
        raise PipelineRuntimeContractError(
            "assigned STAGE cache is missing, swapped, or failed integrity validation"
        ) from exc


def stage_identity_labels(
    plan: DeploymentPlan,
    assignment: NodeAssignment,
) -> dict[str, str]:
    if not is_stage_pipeline_plan(plan):
        return {}
    identity = stage_cache_identity(plan, assignment)
    stage = plan.stage_artifact
    assert stage is not None
    return {
        STRICT_CACHE_KIND_LABEL: MODEL_CACHE_KIND_STAGE,
        STRICT_STAGE_VARIANT_LABEL: stage.artifact_set_digest,
        STRICT_STAGE_MANIFEST_LABEL: assignment.stage_manifest_digest or "",
        STRICT_STAGE_CACHE_IDENTITY_LABEL: identity.cache_identity_digest,
    }


def ordered_pipeline_bindings(plan: DeploymentPlan) -> list[dict[str, object]]:
    validate_strict_pipeline_plan(plan)
    values = []
    for assignment in plan.assignments:
        value: dict[str, object] = {
            "node_id": assignment.node_id,
            "runtime_address": assignment.runtime_address,
            "pipeline_rank": assignment.pipeline_rank,
            "runtime_rank": assignment.expected_runtime_rank,
        }
        if is_stage_pipeline_plan(plan):
            value.update(
                stage_manifest_digest=assignment.stage_manifest_digest,
                stage_tensor_keys_digest=assignment.stage_tensor_keys_digest,
                stage_cache_identity_digest=stage_cache_identity(
                    plan, assignment
                ).cache_identity_digest,
            )
        values.append(value)
    return values


def ray_dure_node_resource(node_id: str) -> str:
    """Return the fixed Ray custom resource that binds one Dure node UUID."""

    canonical = _canonical_uuid(node_id, "node_id")
    return f"{RAY_DURE_NODE_RESOURCE_PREFIX}{uuid.UUID(canonical).hex}"


def ray_dure_node_resources_argument(node_id: str) -> str:
    value = {ray_dure_node_resource(node_id): 1}
    return "--resources=" + json.dumps(
        value, allow_nan=False, separators=(",", ":"), sort_keys=True
    )


def strict_vllm_environment(
    plan: DeploymentPlan, assignment: NodeAssignment
) -> tuple[tuple[str, str], ...]:
    """Return the complete fixed vLLM environment for the strict backend."""

    return (
        ("NCCL_SOCKET_IFNAME", plan.network_interface),
        ("GLOO_SOCKET_IFNAME", plan.network_interface),
        ("VLLM_ATTENTION_BACKEND", "FLASH_ATTN"),
        ("VLLM_HOST_IP", assignment.runtime_address or ""),
        ("VLLM_USE_V1", "0"),
        ("VLLM_USE_RAY_SPMD_WORKER", "0"),
        ("VLLM_RAY_PER_WORKER_GPUS", "1.0"),
        ("VLLM_RAY_BUNDLE_INDICES", ""),
        ("VLLM_USE_RAY_COMPILED_DAG", "0"),
    )


def strict_ray_command(
    plan: DeploymentPlan, assignment: NodeAssignment
) -> tuple[str, ...]:
    command = ["start", "--block"]
    if assignment.role == "ray-head":
        command.extend(
            (
                "--head",
                f"--node-ip-address={assignment.runtime_address}",
                f"--port={RAY_GCS_PORT}",
                f"--temp-dir={RAY_SESSION_CONTAINER_PATH}",
            )
        )
    else:
        command.extend(
            (
                f"--address={plan.ray_head_address}",
                f"--node-ip-address={assignment.runtime_address}",
            )
        )
    command.extend(
        (
            ray_dure_node_resources_argument(assignment.node_id),
            f"--min-worker-port={RAY_MIN_WORKER_PORT}",
            f"--max-worker-port={RAY_MAX_WORKER_PORT}",
        )
    )
    return tuple(command)


def strict_ray_session_path(plan: DeploymentPlan) -> Path:
    """Return the deterministic host directory shared by head Ray and API."""

    deployment_id = _canonical_uuid(plan.deployment_id, "deployment_id")
    if type(plan.generation) is not int or plan.generation < 1:
        raise PipelineRuntimeContractError("generation must be a positive integer")
    return (
        _TRUSTED_RUNTIME_ROOT
        / deployment_id
        / f"generation-{plan.generation}"
        / "ray"
    )


def strict_vllm_api_command(plan: DeploymentPlan) -> tuple[str, ...]:
    command = [
        "serve",
        "/models/model",
        "--distributed-executor-backend",
        "ray",
        "--pipeline-parallel-size",
        str(plan.pipeline_parallel_size),
        "--tensor-parallel-size",
        str(plan.tensor_parallel_size),
        "--quantization",
        plan.model.quantization,
        "--gpu-memory-utilization",
        str(plan.gpu_memory_utilization),
        "--max-model-len",
        str(plan.max_model_len),
        "--served-model-name",
        plan.model.model_id,
        "--host",
        VLLM_API_HOST,
        "--port",
        str(VLLM_API_PORT),
    ]
    if is_stage_pipeline_plan(plan):
        command.extend(("--load-format", "sharded_state"))
    return tuple(command)


def strict_runtime_contract_digest(
    plan: DeploymentPlan,
    assignment: NodeAssignment,
    component: str,
) -> str:
    """Hash every fixed input used to create one strict runtime container."""

    if not is_strict_pipeline_plan(plan) or assignment not in plan.assignments:
        raise PipelineRuntimeContractError(
            "strict runtime contract is not bound to a plan assignment"
        )
    if component == RAY_COMPONENT:
        entrypoint = "ray"
        command = strict_ray_command(plan, assignment)
        shm_size = "16g"
        environment = strict_vllm_environment(plan, assignment)
    elif component == VLLM_API_COMPONENT and assignment.role == "ray-head":
        entrypoint = "vllm"
        command = strict_vllm_api_command(plan)
        shm_size = "4g"
        environment = (
            ("RAY_ADDRESS", plan.ray_head_address),
            *strict_vllm_environment(plan, assignment),
        )
    else:
        raise PipelineRuntimeContractError(
            "strict runtime component is not valid for this assignment"
        )
    mounts = [
        {
            "source": str(strict_model_mount_path(plan, assignment)),
            "target": "/models/model",
            "readonly": True,
        }
    ]
    if assignment.role == "ray-head":
        mounts.append(
            {
                "source": str(strict_ray_session_path(plan)),
                "target": RAY_SESSION_CONTAINER_PATH,
                "readonly": False,
            }
        )
    value = {
        "schema_version": 2,
        "identity": {
            "deployment_id": plan.deployment_id,
            "generation": plan.generation,
            "node_id": assignment.node_id,
            "backend": plan.execution_backend,
            "pipeline_rank": assignment.pipeline_rank,
            "runtime_rank": assignment.expected_runtime_rank,
            "component": component,
        },
        "container": {
            "image": plan.image,
            "restart": "unless-stopped",
            "network": "host",
            "shm_size": shm_size,
            "gpu_device": assignment.gpu_uuid or assignment.gpu_index,
            "mounts": mounts,
            "entrypoint": entrypoint,
            "environment": dict(environment),
            "command": list(command),
        },
    }
    encoded = json.dumps(
        value, allow_nan=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def pipeline_contract_detail(
    plan: DeploymentPlan,
    assignment: NodeAssignment,
) -> str:
    """Return the closed canonical evidence object accepted by the controller."""

    validate_strict_pipeline_plan(plan)
    if assignment not in plan.assignments:
        raise PipelineRuntimeContractError("assignment is not part of the plan")
    value = {
        "schema_version": PIPELINE_CONTRACT_SCHEMA_VERSION,
        "backend": VLLM_RAY_PP_BACKEND,
        "vllm_version": VLLM_RAY_PP_RUNTIME_VERSION,
        "node_id": assignment.node_id,
        "runtime_address": assignment.runtime_address,
        "pipeline_rank": assignment.pipeline_rank,
        "runtime_rank": assignment.expected_runtime_rank,
        "ordered_bindings": ordered_pipeline_bindings(plan),
    }
    if is_stage_pipeline_plan(plan):
        stage = plan.stage_artifact
        assert stage is not None
        value["stage_artifact"] = {
            "artifact_set_digest": stage.artifact_set_digest,
            "contract_identity_digest": stage.contract_identity_digest,
            "source_manifest_digest": stage.source_manifest_digest,
            "loader_format": stage.loader_format,
            "stage_manifest_digest": assignment.stage_manifest_digest,
            "stage_tensor_keys_digest": assignment.stage_tensor_keys_digest,
            "stage_cache_identity_digest": stage_cache_identity(
                plan, assignment
            ).cache_identity_digest,
        }
    return json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True)

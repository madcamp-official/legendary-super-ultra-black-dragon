from __future__ import annotations

import json
import math
import time
import urllib.error
import urllib.request

from .command import Runner, SubprocessRunner
from .models import CheckResult, DeploymentPlan, NodeAssignment, NodeProfile
from .pipeline_runtime import (
    PIPELINE_CONTRACT_CHECK,
    RAY_DURE_NODE_RESOURCE_PREFIX,
    RAY_COMPONENT,
    STRICT_CACHE_KIND_LABEL,
    STRICT_STAGE_CACHE_IDENTITY_LABEL,
    STRICT_STAGE_MANIFEST_LABEL,
    STRICT_STAGE_VARIANT_LABEL,
    VLLM_API_COMPONENT,
    VLLM_RAY_PP_RUNTIME_VERSION,
    is_stage_pipeline_plan,
    is_strict_pipeline_plan,
    pipeline_contract_detail,
    ray_dure_node_resource,
    strict_runtime_contract_digest,
    stage_identity_labels,
    validate_strict_pipeline_node,
    validate_strict_pipeline_plan,
    validate_strict_stage_cache,
)
from .runtime import ContainerRuntime


PIPELINE_SNAPSHOT_MAX_BYTES = 256 * 1024
PIPELINE_SNAPSHOT_SCRIPT = """\
# DURE_PIPELINE_SNAPSHOT_V1
import json
import sys

import ray
import vllm
from ray.util.state import list_actors


def value(item, key):
    if isinstance(item, dict):
        return item.get(key)
    return getattr(item, key, None)


ray.init(address=sys.argv[1], logging_level="ERROR")
nodes = []
for item in ray.nodes():
    if not item.get("Alive"):
        continue
    resources = item.get("Resources") or {}
    nodes.append(
        {
            "node_id": item.get("NodeID"),
            "runtime_address": item.get("NodeManagerAddress"),
            "gpu": resources.get("GPU", 0),
            "alive": item.get("Alive"),
            "dure_node_resources": {
                key: resources[key]
                for key in sorted(resources)
                if key.startswith("dure_node_")
            },
        }
    )
actors = []
for item in list_actors(
    filters=[("state", "=", "ALIVE")], detail=True, limit=10000
):
    actors.append(
        {
            "actor_id": value(item, "actor_id"),
            "class_name": value(item, "class_name"),
            "node_id": value(item, "node_id"),
            "state": value(item, "state"),
        }
    )
payload = json.dumps(
    {
        "schema_version": 1,
        "vllm_version": vllm.__version__,
        "nodes": nodes,
        "actors": actors,
    },
    allow_nan=False,
    separators=(",", ":"),
    sort_keys=True,
)
ray.shutdown()
print(payload)
"""


def _closed_json(value: str):
    def unique_object(pairs):
        result = {}
        for key, item in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = item
        return result

    return json.loads(value, object_pairs_hook=unique_object)


class ReadinessVerifier:
    def __init__(
        self,
        runner: Runner | None = None,
        engine: str = "docker",
        *,
        node_id: str | None = None,
    ) -> None:
        self.runner = runner or SubprocessRunner()
        self.engine = engine
        self.node_id = node_id

    def _container_identity(
        self,
        plan: DeploymentPlan,
        name: str,
        check_name: str,
        *,
        assignment: NodeAssignment | None = None,
        component: str | None = None,
    ) -> tuple[CheckResult | None, str]:
        if self.node_id is None:
            if is_strict_pipeline_plan(plan):
                return (
                    CheckResult(
                        check_name,
                        False,
                        "Strict container identity requires the local node UUID",
                    ),
                    name,
                )
            return None, name
        strict_identity = {}
        if is_strict_pipeline_plan(plan):
            assignment = assignment or plan.assignment_for(self.node_id)
            if assignment is None or component is None:
                return (
                    CheckResult(
                        check_name,
                        False,
                        "Strict container identity is not bound to a plan assignment",
                    ),
                    name,
                )
            strict_identity = {
                "backend": plan.execution_backend,
                "pipeline_rank": assignment.pipeline_rank,
                "runtime_rank": assignment.expected_runtime_rank,
                "component": component,
                "runtime_contract": strict_runtime_contract_digest(
                    plan, assignment, component
                ),
            }
            stage_labels = stage_identity_labels(plan, assignment)
            if stage_labels:
                strict_identity.update(
                    cache_kind=stage_labels[STRICT_CACHE_KIND_LABEL],
                    stage_variant=stage_labels[STRICT_STAGE_VARIANT_LABEL],
                    stage_manifest=stage_labels[STRICT_STAGE_MANIFEST_LABEL],
                    stage_cache_identity=stage_labels[
                        STRICT_STAGE_CACHE_IDENTITY_LABEL
                    ],
                )
        check, identity = ContainerRuntime(
            self.runner, self.engine
        ).running_container_identity(
            name,
            deployment_id=plan.deployment_id,
            generation=plan.generation,
            node_id=self.node_id,
            check_name=check_name,
            **strict_identity,
        )
        return check, identity.container_id if identity is not None else name

    def host_gpu(self, profile: NodeProfile) -> CheckResult:
        if not profile.gpus:
            return CheckResult("host-gpu", False, "No NVIDIA GPU detected")
        result = self.runner.run(
            [
                "nvidia-smi",
                "--query-gpu=index,name,driver_version,memory.total",
                "--format=csv,noheader",
            ],
            timeout=15,
        )
        return CheckResult(
            "host-gpu",
            result.ok,
            result.stdout if result.ok else result.stderr or result.stdout,
        )

    def container_gpu(
        self,
        plan: DeploymentPlan,
        assignment: NodeAssignment | None = None,
    ) -> CheckResult:
        name = f"dure-ray-{plan.deployment_id}"
        identity, container_reference = self._container_identity(
            plan,
            name,
            "container-gpu",
            assignment=assignment,
            component=RAY_COMPONENT if is_strict_pipeline_plan(plan) else None,
        )
        if identity is not None and not identity.ok:
            return identity
        code = (
            "import json,torch; "
            "assert torch.cuda.is_available(); "
            "x=torch.ones((256,256),device='cuda'); "
            "y=x@x; torch.cuda.synchronize(); "
            "print(json.dumps({'gpu':torch.cuda.get_device_name(0),'value':float(y[0,0])}))"
        )
        result = self.runner.run(
            [self.engine, "exec", container_reference, "python3", "-c", code],
            timeout=60,
        )
        return CheckResult(
            "container-gpu",
            result.ok,
            result.stdout if result.ok else result.stderr or result.stdout,
        )

    def ray_cluster(self, plan: DeploymentPlan) -> CheckResult:
        if is_strict_pipeline_plan(plan):
            return CheckResult(
                "ray-cluster",
                False,
                "Strict pipeline requires source-pinned pipeline rank verification",
            )
        name = f"dure-ray-{plan.deployment_id}"
        identity, container_reference = self._container_identity(
            plan, name, "ray-cluster"
        )
        if identity is not None and not identity.ok:
            return identity
        code = (
            "import json,ray; "
            f"ray.init(address='{plan.ray_head_address}',logging_level='ERROR'); "
            "print(json.dumps(ray.cluster_resources(),sort_keys=True)); ray.shutdown()"
        )
        result = self.runner.run(
            [self.engine, "exec", container_reference, "python3", "-c", code],
            timeout=45,
        )
        if not result.ok:
            return CheckResult("ray-cluster", False, result.stderr or result.stdout)
        try:
            resources = json.loads(result.stdout.splitlines()[-1])
        except (json.JSONDecodeError, IndexError):
            return CheckResult("ray-cluster", False, f"Invalid Ray resource response: {result.stdout}")
        gpu_count = float(resources.get("GPU", 0))
        ok = gpu_count >= plan.world_size
        return CheckResult(
            "ray-cluster",
            ok,
            f"GPU resources: {gpu_count:g}/{plan.world_size}; {json.dumps(resources, sort_keys=True)}",
        )

    def pipeline_rank_contract(
        self,
        plan: DeploymentPlan,
        assignment: NodeAssignment,
        profile: NodeProfile,
        *,
        require_actors: bool = False,
    ) -> CheckResult:
        """Run a one-shot contract check with a fresh full cache validation."""

        return self._pipeline_rank_contract(
            plan,
            assignment,
            profile,
            require_actors=require_actors,
            stage_cache_prevalidated=False,
        )

    def _pipeline_rank_contract(
        self,
        plan: DeploymentPlan,
        assignment: NodeAssignment,
        profile: NodeProfile,
        *,
        require_actors: bool,
        stage_cache_prevalidated: bool,
    ) -> CheckResult:
        """Verify the source-pinned vLLM 0.9.0 Ray rank contract.

        Ray does not expose vLLM's internal pipeline rank as a public runtime
        field.  We therefore verify the exact one-GPU node/actor topology and
        the pinned vLLM version from which the deterministic rank order is
        derived.  The returned detail is the controller's closed canonical
        mapping, not a claim that Ray directly reported the rank number.
        """

        try:
            validate_strict_pipeline_node(
                plan, assignment, profile, require_model_cache=True
            )
            if not stage_cache_prevalidated:
                validate_strict_stage_cache(plan, assignment)
        except ValueError as exc:
            return CheckResult(PIPELINE_CONTRACT_CHECK, False, str(exc))
        if self.node_id != assignment.node_id:
            return CheckResult(
                PIPELINE_CONTRACT_CHECK,
                False,
                "Pipeline contract verifier is not bound to the assigned node",
            )

        name = f"dure-ray-{plan.deployment_id}"
        identity, container_reference = self._container_identity(
            plan,
            name,
            PIPELINE_CONTRACT_CHECK,
            assignment=assignment,
            component=RAY_COMPONENT,
        )
        if identity is not None and not identity.ok:
            return identity

        command = [
            self.engine,
            "exec",
            container_reference,
            "python3",
            "-c",
            PIPELINE_SNAPSHOT_SCRIPT,
            plan.ray_head_address,
        ]
        bounded_runner = getattr(self.runner, "run_limited_output", None)
        if callable(bounded_runner):
            result = bounded_runner(
                command,
                timeout=60,
                max_output_bytes=PIPELINE_SNAPSHOT_MAX_BYTES,
            )
        else:  # Compatibility with third-party Runner implementations.
            result = self.runner.run(command, timeout=60)
        if not result.ok:
            return CheckResult(
                PIPELINE_CONTRACT_CHECK,
                False,
                result.stderr or result.stdout or "Ray pipeline snapshot failed",
            )
        try:
            snapshot = _closed_json(result.stdout.splitlines()[-1])
            self._validate_pipeline_snapshot(
                plan, snapshot, require_actors=require_actors
            )
            detail = pipeline_contract_detail(plan, assignment)
        except (IndexError, TypeError, ValueError) as exc:
            return CheckResult(
                PIPELINE_CONTRACT_CHECK,
                False,
                f"Invalid source-pinned pipeline snapshot: {exc}",
            )
        return CheckResult(PIPELINE_CONTRACT_CHECK, True, detail)

    @staticmethod
    def _validate_pipeline_snapshot(
        plan: DeploymentPlan,
        snapshot,
        *,
        require_actors: bool,
    ) -> None:
        validate_strict_pipeline_plan(plan)
        if type(snapshot) is not dict or set(snapshot) != {
            "schema_version",
            "vllm_version",
            "nodes",
            "actors",
        }:
            raise ValueError("snapshot fields do not match schema v1")
        if (
            snapshot["schema_version"] != 1
            or type(snapshot["schema_version"]) is not int
            or snapshot["vllm_version"] != VLLM_RAY_PP_RUNTIME_VERSION
            or type(snapshot["nodes"]) is not list
            or type(snapshot["actors"]) is not list
        ):
            raise ValueError("snapshot version or collection type is invalid")

        nodes = snapshot["nodes"]
        if len(nodes) != len(plan.assignments):
            raise ValueError("Ray node count does not match the pipeline plan")
        expected_addresses = [item.runtime_address for item in plan.assignments]
        expected_by_address = {
            item.runtime_address: item for item in plan.assignments
        }
        observed_addresses: list[str] = []
        observed_node_ids: list[str] = []
        address_to_node_id: dict[str, str] = {}
        for node in nodes:
            if type(node) is not dict or set(node) != {
                "node_id",
                "runtime_address",
                "gpu",
                "alive",
                "dure_node_resources",
            }:
                raise ValueError("Ray node item fields are invalid")
            node_id = node["node_id"]
            address = node["runtime_address"]
            gpu = node["gpu"]
            dure_resources = node["dure_node_resources"]
            if (
                type(node_id) is not str
                or not node_id
                or type(address) is not str
                or type(node["alive"]) is not bool
                or not node["alive"]
                or type(gpu) not in {int, float}
                or not math.isfinite(float(gpu))
                or float(gpu) != 1.0
                or type(dure_resources) is not dict
            ):
                raise ValueError("Ray node identity, liveness, or GPU count is invalid")
            expected_assignment = expected_by_address.get(address)
            expected_resource = (
                ray_dure_node_resource(expected_assignment.node_id)
                if expected_assignment is not None
                else None
            )
            if (
                expected_resource is None
                or set(dure_resources) != {expected_resource}
                or type(dure_resources[expected_resource]) not in {int, float}
                or not math.isfinite(float(dure_resources[expected_resource]))
                or float(dure_resources[expected_resource]) != 1.0
                or any(
                    type(key) is not str
                    or not key.startswith(RAY_DURE_NODE_RESOURCE_PREFIX)
                    for key in dure_resources
                )
            ):
                raise ValueError("Ray node is not bound to its planned Dure node UUID")
            observed_node_ids.append(node_id)
            observed_addresses.append(address)
            address_to_node_id[address] = node_id
        if (
            len(set(observed_node_ids)) != len(observed_node_ids)
            or len(set(observed_addresses)) != len(observed_addresses)
            or set(observed_addresses) != set(expected_addresses)
        ):
            raise ValueError("Ray nodes are missing, extra, duplicated, or swapped")

        worker_actors = []
        for actor in snapshot["actors"]:
            if type(actor) is not dict or set(actor) != {
                "actor_id",
                "class_name",
                "node_id",
                "state",
            }:
                raise ValueError("Ray actor item fields are invalid")
            if (
                type(actor["class_name"]) is str
                and actor["class_name"].endswith("RayWorkerWrapper")
            ):
                worker_actors.append(actor)
        if not worker_actors and not require_actors:
            return
        if len(worker_actors) != len(plan.assignments):
            raise ValueError("vLLM Ray worker actor count does not match the plan")
        actor_ids: list[str] = []
        actor_node_ids: list[str] = []
        for actor in worker_actors:
            if (
                type(actor["actor_id"]) is not str
                or not actor["actor_id"]
                or type(actor["node_id"]) is not str
                or not actor["node_id"]
                or actor["state"] != "ALIVE"
            ):
                raise ValueError("vLLM Ray worker actor identity is invalid")
            actor_ids.append(actor["actor_id"])
            actor_node_ids.append(actor["node_id"])
        if (
            len(set(actor_ids)) != len(actor_ids)
            or len(set(actor_node_ids)) != len(actor_node_ids)
            or set(actor_node_ids) != set(address_to_node_id.values())
        ):
            raise ValueError("vLLM Ray worker actors are missing, extra, or duplicated")

    def wait_pipeline_rank_contract(
        self,
        plan: DeploymentPlan,
        assignment: NodeAssignment,
        profile: NodeProfile,
        *,
        require_actors: bool = False,
        timeout: float = 300,
        interval: float = 5,
    ) -> CheckResult:
        stage = is_stage_pipeline_plan(plan)
        if stage:
            try:
                validate_strict_stage_cache(plan, assignment)
            except ValueError as exc:
                return CheckResult(PIPELINE_CONTRACT_CHECK, False, str(exc))
        deadline = time.monotonic() + timeout
        last = CheckResult(
            PIPELINE_CONTRACT_CHECK,
            False,
            "Pipeline rank contract has not been checked",
        )
        while time.monotonic() < deadline:
            last = self._pipeline_rank_contract(
                plan,
                assignment,
                profile,
                require_actors=require_actors,
                stage_cache_prevalidated=stage,
            )
            if last.ok:
                if stage:
                    try:
                        validate_strict_stage_cache(plan, assignment)
                    except ValueError as exc:
                        return CheckResult(
                            PIPELINE_CONTRACT_CHECK, False, str(exc)
                        )
                return last
            time.sleep(interval)
        return CheckResult(
            PIPELINE_CONTRACT_CHECK,
            False,
            f"Pipeline rank contract was not ready within {timeout:g}s; "
            f"last error: {last.detail}",
        )

    def api(
        self,
        url: str = "http://127.0.0.1:8000",
        *,
        plan: DeploymentPlan | None = None,
    ) -> CheckResult:
        if plan is not None:
            name = f"dure-api-{plan.deployment_id}"
            assignment = (
                plan.assignment_for(self.node_id)
                if self.node_id is not None
                else None
            )
            identity, _ = self._container_identity(
                plan,
                name,
                "vllm-api",
                assignment=assignment,
                component=(
                    VLLM_API_COMPONENT if is_strict_pipeline_plan(plan) else None
                ),
            )
            if identity is not None and not identity.ok:
                return identity
        try:
            with urllib.request.urlopen(f"{url}/health", timeout=10) as response:
                if not 200 <= response.status < 300:
                    return CheckResult("vllm-api", False, f"HTTP {response.status} from /health")
            with urllib.request.urlopen(f"{url}/v1/models", timeout=10) as response:
                payload = json.loads(response.read().decode("utf-8"))
                models = payload.get("data", [])
                valid_models = (
                    type(models) is list
                    and bool(models)
                    and all(
                        type(item) is dict
                        and type(item.get("id")) is str
                        and bool(item["id"])
                        for item in models
                    )
                )
                model_ids = [item["id"] for item in models] if valid_models else []
                ok = 200 <= response.status < 300 and valid_models
                if plan is not None and is_strict_pipeline_plan(plan):
                    ok = ok and model_ids == [plan.model.model_id]
                detail = (
                    f"HTTP {response.status}; models="
                    f"{','.join(model_ids)}"
                )
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            return CheckResult("vllm-api", False, str(exc))
        except (json.JSONDecodeError, AttributeError, TypeError) as exc:
            return CheckResult("vllm-api", False, f"Invalid /v1/models response: {exc}")
        return CheckResult("vllm-api", ok, detail)

    def wait_api(
        self,
        url: str = "http://127.0.0.1:8000",
        *,
        plan: DeploymentPlan | None = None,
        timeout: float = 600,
        interval: float = 5,
    ) -> CheckResult:
        deadline = time.monotonic() + timeout
        last = CheckResult("vllm-api", False, "API has not been checked")
        while time.monotonic() < deadline:
            last = self.api(url, plan=plan)
            if last.ok:
                return last
            time.sleep(interval)
        return CheckResult(
            "vllm-api",
            False,
            f"API was not ready within {timeout:g}s; last error: {last.detail}",
        )

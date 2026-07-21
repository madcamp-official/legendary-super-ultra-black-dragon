from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path

from .command import CommandResult, Runner, SubprocessRunner
from .models import CheckResult, DeploymentPlan, NodeAssignment, NodeProfile
from .pipeline_runtime import (
    RAY_COMPONENT,
    RAY_MAX_WORKER_PORT,
    RAY_MIN_WORKER_PORT,
    STRICT_IDENTITY_COMPONENTS,
    STRICT_CACHE_KIND_LABEL,
    STRICT_RUNTIME_CONTRACT_LABEL,
    STRICT_STAGE_CACHE_IDENTITY_LABEL,
    STRICT_STAGE_MANIFEST_LABEL,
    STRICT_STAGE_VARIANT_LABEL,
    VLLM_API_COMPONENT,
    VLLM_API_HOST,
    VLLM_API_PORT,
    is_strict_pipeline_plan,
    stage_identity_labels,
    strict_model_mount_path,
    strict_ray_command,
    strict_runtime_contract_digest,
    strict_vllm_api_command,
    strict_vllm_environment,
    validate_strict_pipeline_node,
    validate_strict_pipeline_plan,
    validate_strict_stage_cache,
)


class DeploymentError(RuntimeError):
    pass


DEPLOYMENT_IDENTITY_FORMAT = (
    '{{.Id}}\t{{.State.Status}}\t'
    '{{index .Config.Labels "dure.deployment"}}\t'
    '{{index .Config.Labels "dure.generation"}}\t'
    '{{index .Config.Labels "dure.node"}}\t'
    '{{index .Config.Labels "dure.backend"}}\t'
    '{{index .Config.Labels "dure.pipeline-rank"}}\t'
    '{{index .Config.Labels "dure.runtime-rank"}}\t'
    '{{index .Config.Labels "dure.component"}}\t'
    '{{index .Config.Labels "dure.runtime-contract"}}\t'
    '{{index .Config.Labels "dure.cache-kind"}}\t'
    '{{index .Config.Labels "dure.stage-variant"}}\t'
    '{{index .Config.Labels "dure.stage-manifest"}}\t'
    '{{index .Config.Labels "dure.stage-cache-identity"}}'
)
STOPPED_CONTAINER_STATES = frozenset({"created", "exited", "dead"})
STOPPABLE_CONTAINER_STATES = frozenset({"running", "restarting", "paused"})
MISSING_DOCKER_LABEL_VALUES = frozenset({"", "<no value>", "<nil>"})


def _stage_identity_kwargs(
    plan: DeploymentPlan,
    assignment: NodeAssignment,
) -> dict[str, str]:
    labels = stage_identity_labels(plan, assignment)
    if not labels:
        return {}
    return {
        "cache_kind": labels[STRICT_CACHE_KIND_LABEL],
        "stage_variant": labels[STRICT_STAGE_VARIANT_LABEL],
        "stage_manifest": labels[STRICT_STAGE_MANIFEST_LABEL],
        "stage_cache_identity": labels[STRICT_STAGE_CACHE_IDENTITY_LABEL],
    }


def _stage_label_arguments(
    plan: DeploymentPlan,
    assignment: NodeAssignment,
) -> list[str]:
    return [
        value
        for key, item in sorted(stage_identity_labels(plan, assignment).items())
        for value in ("--label", f"{key}={item}")
    ]


@dataclass(frozen=True)
class DeploymentContainerIdentity:
    container_id: str
    state: str
    deployment_id: str
    generation: str
    node_id: str
    backend: str = ""
    pipeline_rank: str = ""
    runtime_rank: str = ""
    component: str = ""
    runtime_contract: str = ""
    cache_kind: str = ""
    stage_variant: str = ""
    stage_manifest: str = ""
    stage_cache_identity: str = ""

    @classmethod
    def parse(cls, value: str) -> "DeploymentContainerIdentity | None":
        parts = value.split("\t")
        # SubprocessRunner strips trailing tabs.  Older Dure containers have
        # none of the strict identity labels and some also lack dure.node.
        if not 4 <= len(parts) <= 14:
            return None
        parts.extend([""] * (14 - len(parts)))
        parts[2:] = [
            "" if part in MISSING_DOCKER_LABEL_VALUES else part
            for part in parts[2:]
        ]
        if any(not part for part in parts[:4]):
            return None
        return cls(*parts)

    def matches(
        self,
        deployment_id: str,
        generation: int,
        node_id: str,
        *,
        backend: str | None = None,
        pipeline_rank: int | None = None,
        runtime_rank: int | None = None,
        component: str | None = None,
        runtime_contract: str | None = None,
        cache_kind: str | None = None,
        stage_variant: str | None = None,
        stage_manifest: str | None = None,
        stage_cache_identity: str | None = None,
    ) -> bool:
        base_matches = (
            self.deployment_id == deployment_id
            and self.generation == str(generation)
        )
        if not base_matches:
            return False
        if backend is None:
            return (
                (not self.node_id or self.node_id == node_id)
                and not self.backend
                and not self.pipeline_rank
                and not self.runtime_rank
                and not self.component
                and not self.runtime_contract
                and not self.cache_kind
                and not self.stage_variant
                and not self.stage_manifest
                and not self.stage_cache_identity
            )
        stage_expected = any(
            value is not None
            for value in (
                cache_kind,
                stage_variant,
                stage_manifest,
                stage_cache_identity,
            )
        )
        stage_matches = (
            self.cache_kind == cache_kind
            and self.stage_variant == stage_variant
            and self.stage_manifest == stage_manifest
            and self.stage_cache_identity == stage_cache_identity
            if stage_expected
            else not any(
                (
                    self.cache_kind,
                    self.stage_variant,
                    self.stage_manifest,
                    self.stage_cache_identity,
                )
            )
        )
        return (
            self.node_id == node_id
            and self.backend == backend
            and self.pipeline_rank == str(pipeline_rank)
            and self.runtime_rank == str(runtime_rank)
            and self.component == component
            and (
                runtime_contract is None
                or self.runtime_contract == runtime_contract
            )
            and stage_matches
        )


class ContainerRuntime:
    def __init__(self, runner: Runner | None = None, engine: str = "docker") -> None:
        self.runner = runner or SubprocessRunner()
        self.engine = engine

    def image_exists(self, image: str) -> bool:
        return self.runner.run([self.engine, "image", "inspect", image], timeout=15).ok

    def ensure_image(self, image: str, *, pull: bool) -> CheckResult:
        if self.image_exists(image):
            return CheckResult("container-image", True, f"Image is available: {image}")
        if not pull:
            return CheckResult(
                "container-image",
                False,
                f"Image is missing: {image}; rerun with --pull",
            )
        result = self.runner.run([self.engine, "pull", image], timeout=1800)
        return CheckResult(
            "container-image",
            result.ok,
            f"Pulled {image}" if result.ok else result.stderr or result.stdout,
        )

    def inspect_container(self, name: str) -> CommandResult:
        return self.runner.run(
            [self.engine, "inspect", "--format", "{{.State.Status}}", name], timeout=10
        )

    def inspect_deployment_container(
        self, reference: str
    ) -> tuple[CommandResult, DeploymentContainerIdentity | None]:
        result = self.runner.run(
            [
                self.engine,
                "inspect",
                "--format",
                DEPLOYMENT_IDENTITY_FORMAT,
                reference,
            ],
            timeout=10,
        )
        identity = (
            DeploymentContainerIdentity.parse(result.stdout) if result.ok else None
        )
        return result, identity

    @staticmethod
    def _container_is_absent(result: CommandResult) -> bool:
        detail = f"{result.stderr}\n{result.stdout}".lower()
        return result.returncode == 1 and (
            "no such object" in detail or "not found" in detail
        )

    def running_container_identity(
        self,
        name: str,
        *,
        deployment_id: str,
        generation: int,
        node_id: str,
        check_name: str,
        backend: str | None = None,
        pipeline_rank: int | None = None,
        runtime_rank: int | None = None,
        component: str | None = None,
        runtime_contract: str | None = None,
        cache_kind: str | None = None,
        stage_variant: str | None = None,
        stage_manifest: str | None = None,
        stage_cache_identity: str | None = None,
    ) -> tuple[CheckResult, DeploymentContainerIdentity | None]:
        result, identity = self.inspect_deployment_container(name)
        if not result.ok:
            return (
                CheckResult(
                    check_name,
                    False,
                    result.stderr
                    or result.stdout
                    or f"Container is unavailable: {name}",
                ),
                None,
            )
        if identity is None or not identity.matches(
            deployment_id,
            generation,
            node_id,
            backend=backend,
            pipeline_rank=pipeline_rank,
            runtime_rank=runtime_rank,
            component=component,
            runtime_contract=runtime_contract,
            cache_kind=cache_kind,
            stage_variant=stage_variant,
            stage_manifest=stage_manifest,
            stage_cache_identity=stage_cache_identity,
        ):
            return (
                CheckResult(
                    check_name,
                    False,
                    "Container identity does not match deployment generation "
                    f"and node: {name}",
                ),
                None,
            )
        if identity.state != "running":
            return (
                CheckResult(
                    check_name,
                    False,
                    f"Deployment container is not running: {name}",
                ),
                None,
            )
        return (
            CheckResult(
                check_name, True, f"Verified Dure container identity: {name}"
            ),
            identity,
        )

    def verify_container_identity(
        self,
        name: str,
        *,
        deployment_id: str,
        generation: int,
        node_id: str,
        check_name: str,
        backend: str | None = None,
        pipeline_rank: int | None = None,
        runtime_rank: int | None = None,
        component: str | None = None,
        runtime_contract: str | None = None,
        cache_kind: str | None = None,
        stage_variant: str | None = None,
        stage_manifest: str | None = None,
        stage_cache_identity: str | None = None,
    ) -> CheckResult:
        check, _ = self.running_container_identity(
            name,
            deployment_id=deployment_id,
            generation=generation,
            node_id=node_id,
            check_name=check_name,
            backend=backend,
            pipeline_rank=pipeline_rank,
            runtime_rank=runtime_rank,
            component=component,
            runtime_contract=runtime_contract,
            cache_kind=cache_kind,
            stage_variant=stage_variant,
            stage_manifest=stage_manifest,
            stage_cache_identity=stage_cache_identity,
        )
        return check

    def _prepare_named_container(
        self,
        name: str,
        *,
        deployment_id: str,
        generation: int,
        node_id: str,
        replace: bool,
        check_name: str,
        backend: str | None = None,
        pipeline_rank: int | None = None,
        runtime_rank: int | None = None,
        component: str | None = None,
        runtime_contract: str | None = None,
        cache_kind: str | None = None,
        stage_variant: str | None = None,
        stage_manifest: str | None = None,
        stage_cache_identity: str | None = None,
    ) -> tuple[bool, CheckResult | None]:
        result, identity = self.inspect_deployment_container(name)
        if not result.ok:
            if self._container_is_absent(result):
                return True, None
            return False, CheckResult(
                check_name,
                False,
                result.stderr or result.stdout or f"Container identity is unavailable: {name}",
            )
        if identity is None or not identity.matches(
            deployment_id,
            generation,
            node_id,
            backend=backend,
            pipeline_rank=pipeline_rank,
            runtime_rank=runtime_rank,
            component=component,
            runtime_contract=runtime_contract,
            cache_kind=cache_kind,
            stage_variant=stage_variant,
            stage_manifest=stage_manifest,
            stage_cache_identity=stage_cache_identity,
        ):
            return False, CheckResult(
                check_name,
                False,
                f"Refusing container name collision with mismatched Dure identity: {name}",
            )
        if identity.state == "running":
            return False, CheckResult(
                check_name, True, f"Container is already running: {name}"
            )
        if identity.state not in STOPPED_CONTAINER_STATES:
            return False, CheckResult(
                check_name,
                False,
                f"Container is not safely replaceable in state {identity.state}: {name}",
            )
        if not replace:
            return False, CheckResult(
                check_name,
                False,
                f"Stopped container exists: {name}; rerun with --replace",
            )
        removed = self.runner.run(
            [self.engine, "rm", identity.container_id], timeout=30
        )
        if not removed.ok:
            return False, CheckResult(
                check_name, False, removed.stderr or removed.stdout
            )
        return True, None

    def stop_deployment(
        self,
        deployment_id: str,
        *,
        generation: int | None = None,
        node_id: str | None = None,
        plan: DeploymentPlan | None = None,
        assignment: NodeAssignment | None = None,
    ) -> CheckResult:
        """Stop only containers carrying the exact Dure deployment label."""
        if self.engine != "docker":
            return CheckResult("deployment-stop", False, "Apply mode currently supports Docker only")
        if plan is not None:
            try:
                plan.validate_execution_contract()
            except (TypeError, ValueError) as exc:
                return CheckResult("deployment-stop", False, str(exc))
        strict = plan is not None and is_strict_pipeline_plan(plan)
        if strict:
            try:
                validate_strict_pipeline_plan(
                    plan,
                    require_manifest_cache_path=False,
                    validate_model_path=False,
                )
            except ValueError as exc:
                return CheckResult("deployment-stop", False, str(exc))
            if (
                assignment is None
                or assignment not in plan.assignments
                or deployment_id != plan.deployment_id
                or generation != plan.generation
                or node_id != assignment.node_id
            ):
                return CheckResult(
                    "deployment-stop",
                    False,
                    "Strict deployment stop is not bound to its exact plan assignment",
                )
        filters = ["--filter", f"label=dure.deployment={deployment_id}"]
        if generation is not None:
            filters.extend(["--filter", f"label=dure.generation={generation}"])
        listed = self.runner.run(
            [
                self.engine,
                "ps",
                "-q",
                *filters,
            ],
            timeout=15,
        )
        if not listed.ok:
            return CheckResult("deployment-stop", False, listed.stderr or listed.stdout)
        container_ids = [item.strip() for item in listed.stdout.splitlines() if item.strip()]
        if not container_ids:
            return CheckResult("deployment-stop", True, "No running Dure deployment containers")
        if len(container_ids) != len(set(container_ids)):
            return CheckResult(
                "deployment-stop", False, "Docker returned duplicate deployment containers"
            )
        verified_ids: list[str] = []
        observed_components: set[str] = set()
        for container_id in container_ids:
            result, identity = self.inspect_deployment_container(container_id)
            if not result.ok or identity is None:
                return CheckResult(
                    "deployment-stop",
                    False,
                    "Deployment container identity could not be verified",
                )
            if not identity.container_id.startswith(container_id):
                return CheckResult(
                    "deployment-stop", False, "Deployment container ID changed during inspection"
                )
            if strict:
                assert plan is not None and assignment is not None
                allowed_components = (
                    STRICT_IDENTITY_COMPONENTS
                    if assignment.role == "ray-head"
                    else frozenset({RAY_COMPONENT})
                )
                if (
                    identity.component not in allowed_components
                    or identity.component in observed_components
                    or not identity.matches(
                        deployment_id,
                        plan.generation,
                        assignment.node_id,
                        backend=plan.execution_backend,
                        pipeline_rank=assignment.pipeline_rank,
                        runtime_rank=assignment.expected_runtime_rank,
                        component=identity.component,
                        **_stage_identity_kwargs(plan, assignment),
                    )
                ):
                    return CheckResult(
                        "deployment-stop",
                        False,
                        "Strict deployment container identity label mismatch",
                    )
                observed_components.add(identity.component)
            elif (
                identity.deployment_id != deployment_id
                or (generation is not None and identity.generation != str(generation))
                or (node_id is not None and identity.node_id and identity.node_id != node_id)
                or identity.backend
                or identity.pipeline_rank
                or identity.runtime_rank
                or identity.component
                or identity.runtime_contract
                or identity.cache_kind
                or identity.stage_variant
                or identity.stage_manifest
                or identity.stage_cache_identity
            ):
                return CheckResult(
                    "deployment-stop", False, "Deployment container label mismatch"
                )
            if identity.state not in STOPPABLE_CONTAINER_STATES:
                return CheckResult(
                    "deployment-stop", False, "Deployment container is not safely stoppable"
                )
            verified_ids.append(identity.container_id)
        stopped = self.runner.run(
            [self.engine, "stop", "--time", "30", *verified_ids], timeout=60
        )
        return CheckResult(
            "deployment-stop",
            stopped.ok,
            f"Stopped {len(container_ids)} Dure container(s)" if stopped.ok else stopped.stderr or stopped.stdout,
        )

    def stop_registered_node_deployment(
        self,
        deployment_id: str,
        *,
        generation: int,
        node_id: str,
    ) -> CheckResult:
        """Stop the exact registered node slice without requiring the original plan."""
        if self.engine != "docker":
            return CheckResult("deployment-stop", False, "Apply mode currently supports Docker only")
        listed = self.runner.run(
            [
                self.engine,
                "ps",
                "-q",
                "--filter",
                f"label=dure.deployment={deployment_id}",
                "--filter",
                f"label=dure.generation={generation}",
                "--filter",
                f"label=dure.node={node_id}",
            ],
            timeout=15,
        )
        if not listed.ok:
            return CheckResult("deployment-stop", False, listed.stderr or listed.stdout)
        container_ids = [item.strip() for item in listed.stdout.splitlines() if item.strip()]
        if not container_ids:
            return CheckResult("deployment-stop", True, "No running Dure deployment containers")
        if len(container_ids) != len(set(container_ids)):
            return CheckResult(
                "deployment-stop", False, "Docker returned duplicate deployment containers"
            )
        verified_ids: list[str] = []
        observed_components: set[str] = set()
        for container_id in container_ids:
            result, identity = self.inspect_deployment_container(container_id)
            if not result.ok or identity is None:
                return CheckResult(
                    "deployment-stop", False, "Deployment container identity could not be verified"
                )
            if (
                not identity.container_id.startswith(container_id)
                or identity.deployment_id != deployment_id
                or identity.generation != str(generation)
                or identity.node_id != node_id
                or identity.state not in STOPPABLE_CONTAINER_STATES
            ):
                return CheckResult("deployment-stop", False, "Deployment container label mismatch")
            component_key = identity.component or identity.container_id
            if component_key in observed_components:
                return CheckResult("deployment-stop", False, "Duplicate deployment component identity")
            observed_components.add(component_key)
            verified_ids.append(identity.container_id)
        stopped = self.runner.run(
            [self.engine, "stop", "--time", "30", *verified_ids], timeout=60
        )
        return CheckResult(
            "deployment-stop",
            stopped.ok,
            f"Stopped {len(verified_ids)} Dure container(s)" if stopped.ok else stopped.stderr or stopped.stdout,
        )

    def start_ray(
        self,
        profile: NodeProfile,
        plan: DeploymentPlan,
        assignment: NodeAssignment,
        *,
        replace: bool,
    ) -> CheckResult:
        if self.engine != "docker":
            return CheckResult("ray-container", False, "Apply mode currently supports Docker only")
        try:
            plan.validate_execution_contract()
        except (TypeError, ValueError) as exc:
            return CheckResult("ray-container", False, str(exc))
        strict = is_strict_pipeline_plan(plan)
        if strict:
            try:
                validate_strict_pipeline_node(
                    plan, assignment, profile, require_model_cache=True
                )
                validate_strict_stage_cache(plan, assignment)
            except ValueError as exc:
                return CheckResult("ray-container", False, str(exc))

        name = f"dure-ray-{plan.deployment_id}"
        strict_identity = (
            {
                "backend": plan.execution_backend,
                "pipeline_rank": assignment.pipeline_rank,
                "runtime_rank": assignment.expected_runtime_rank,
                "component": RAY_COMPONENT,
                "runtime_contract": strict_runtime_contract_digest(
                    plan, assignment, RAY_COMPONENT
                ),
                **_stage_identity_kwargs(plan, assignment),
            }
            if strict
            else {}
        )
        proceed, existing_check = self._prepare_named_container(
            name,
            deployment_id=plan.deployment_id,
            generation=plan.generation,
            node_id=assignment.node_id,
            replace=replace,
            check_name="ray-container",
            **strict_identity,
        )
        if not proceed:
            assert existing_check is not None
            return existing_check

        local_ip = (
            assignment.runtime_address
            if strict
            else (
                profile.network.addresses[0]
                if profile.network.addresses
                else "127.0.0.1"
            )
        )
        if strict:
            ray_command = list(strict_ray_command(plan, assignment))
            environment_args = [
                value
                for name, item in strict_vllm_environment(plan, assignment)
                for value in ("-e", f"{name}={item}")
            ]
        else:
            _, port = _split_address(plan.ray_head_address)
            ray_command = ["start", "--block"]
            if assignment.role == "ray-head":
                ray_command.extend(
                    ["--head", f"--node-ip-address={local_ip}", f"--port={port}"]
                )
            else:
                ray_command.append(f"--address={plan.ray_head_address}")
            ray_command.extend(
                [
                    f"--min-worker-port={RAY_MIN_WORKER_PORT}",
                    f"--max-worker-port={RAY_MAX_WORKER_PORT}",
                ]
            )
            environment_args = [
                "-e",
                f"NCCL_SOCKET_IFNAME={plan.network_interface}",
                "-e",
                f"GLOO_SOCKET_IFNAME={plan.network_interface}",
                "-e",
                "VLLM_ATTENTION_BACKEND=FLASH_ATTN",
            ]

        argv = [
            self.engine,
            "run",
            "-d",
            "--name",
            name,
            "--restart",
            "unless-stopped",
            "--network",
            "host",
            "--shm-size",
            "16g",
            "--gpus",
            f"device={assignment.gpu_index}",
            "--label",
            f"dure.deployment={plan.deployment_id}",
            "--label",
            f"dure.generation={plan.generation}",
            "--label",
            f"dure.node={assignment.node_id}",
            "--label",
            f"dure.model={plan.model.model_id}",
            *(
                [
                    "--label",
                    f"dure.backend={plan.execution_backend}",
                    "--label",
                    f"dure.pipeline-rank={assignment.pipeline_rank}",
                    "--label",
                    f"dure.runtime-rank={assignment.expected_runtime_rank}",
                    "--label",
                    f"dure.component={RAY_COMPONENT}",
                    "--label",
                    f"{STRICT_RUNTIME_CONTRACT_LABEL}={strict_identity['runtime_contract']}",
                    *_stage_label_arguments(plan, assignment),
                ]
                if strict
                else []
            ),
            "--entrypoint",
            "ray",
            "--mount",
            "type=bind,src="
            f"{strict_model_mount_path(plan, assignment) if strict else plan.model_path},"
            "dst=/models/model,readonly",
            *environment_args,
            plan.image,
            *ray_command,
        ]
        result = self.runner.run(argv, timeout=120)
        return CheckResult(
            "ray-container",
            result.ok,
            f"Started {name}" if result.ok else result.stderr or result.stdout,
        )

    def start_api(
        self,
        plan: DeploymentPlan,
        assignment: NodeAssignment,
        *,
        replace: bool,
    ) -> CheckResult:
        try:
            plan.validate_execution_contract()
        except (TypeError, ValueError) as exc:
            return CheckResult("vllm-api-start", False, str(exc))
        strict = is_strict_pipeline_plan(plan)
        if strict:
            try:
                validate_strict_pipeline_plan(plan)
                validate_strict_stage_cache(plan, assignment)
            except ValueError as exc:
                return CheckResult("vllm-api-start", False, str(exc))
            if self.engine != "docker" or assignment not in plan.assignments:
                return CheckResult(
                    "vllm-api-start",
                    False,
                    "Strict API start is not bound to a Docker plan assignment",
                )
        if assignment.role != "ray-head":
            return CheckResult(
                "vllm-api-start",
                True,
                "API runs on the Ray head node",
                blocking=False,
            )
        name = f"dure-api-{plan.deployment_id}"
        strict_identity = (
            {
                "backend": plan.execution_backend,
                "pipeline_rank": assignment.pipeline_rank,
                "runtime_rank": assignment.expected_runtime_rank,
                "component": VLLM_API_COMPONENT,
                "runtime_contract": strict_runtime_contract_digest(
                    plan, assignment, VLLM_API_COMPONENT
                ),
                **_stage_identity_kwargs(plan, assignment),
            }
            if strict
            else {}
        )
        proceed, existing_check = self._prepare_named_container(
            name,
            deployment_id=plan.deployment_id,
            generation=plan.generation,
            node_id=assignment.node_id,
            replace=replace,
            check_name="vllm-api-start",
            **strict_identity,
        )
        if not proceed:
            assert existing_check is not None
            return existing_check

        if strict:
            environment_args = [
                "-e",
                f"RAY_ADDRESS={plan.ray_head_address}",
                *[
                    value
                    for name, item in strict_vllm_environment(plan, assignment)
                    for value in ("-e", f"{name}={item}")
                ],
            ]
            api_command = list(strict_vllm_api_command(plan))
        else:
            environment_args = [
                "-e",
                f"RAY_ADDRESS={plan.ray_head_address}",
                "-e",
                f"NCCL_SOCKET_IFNAME={plan.network_interface}",
                "-e",
                f"GLOO_SOCKET_IFNAME={plan.network_interface}",
                "-e",
                "VLLM_ATTENTION_BACKEND=FLASH_ATTN",
            ]
            api_command = [
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
                "--host",
                VLLM_API_HOST,
                "--port",
                str(VLLM_API_PORT),
            ]

        argv = [
            self.engine,
            "run",
            "-d",
            "--name",
            name,
            "--restart",
            "unless-stopped",
            "--network",
            "host",
            "--shm-size",
            "4g",
            "--gpus",
            f"device={assignment.gpu_index}",
            "--mount",
            "type=bind,src="
            f"{strict_model_mount_path(plan, assignment) if strict else plan.model_path},"
            "dst=/models/model,readonly",
            *environment_args,
            "--label",
            f"dure.deployment={plan.deployment_id}",
            "--label",
            f"dure.generation={plan.generation}",
            "--label",
            f"dure.node={assignment.node_id}",
            "--label",
            f"dure.model={plan.model.model_id}",
            *(
                [
                    "--label",
                    f"dure.backend={plan.execution_backend}",
                    "--label",
                    f"dure.pipeline-rank={assignment.pipeline_rank}",
                    "--label",
                    f"dure.runtime-rank={assignment.expected_runtime_rank}",
                    "--label",
                    f"dure.component={VLLM_API_COMPONENT}",
                    "--label",
                    f"{STRICT_RUNTIME_CONTRACT_LABEL}={strict_identity['runtime_contract']}",
                    *_stage_label_arguments(plan, assignment),
                ]
                if strict
                else []
            ),
            "--entrypoint",
            "vllm",
            plan.image,
            *api_command,
        ]
        result = self.runner.run(argv, timeout=120)
        return CheckResult(
            "vllm-api-start",
            result.ok,
            f"Started {name}" if result.ok else result.stderr or result.stdout,
        )


class ModelStore:
    def __init__(self, runner: Runner | None = None) -> None:
        self.runner = runner or SubprocessRunner()

    def ensure(self, plan: DeploymentPlan, *, accept_download: bool) -> CheckResult:
        target = Path(plan.model_path)
        if (target / "config.json").is_file():
            return CheckResult("model", True, f"Model is available: {target}")
        if not accept_download:
            return CheckResult(
                "model",
                False,
                f"Model is missing at {target}; rerun with --accept-model-download",
            )

        required_bytes = int(plan.model.checkpoint_gib * 1.25 * 1024**3)
        target.parent.mkdir(parents=True, exist_ok=True)
        free_bytes = shutil.disk_usage(target.parent).free
        if free_bytes < required_bytes:
            return CheckResult(
                "model",
                False,
                f"Insufficient disk: need about {required_bytes // 1024**3} GiB, "
                f"have {free_bytes // 1024**3} GiB",
            )

        temporary = target.with_name(target.name + ".partial")
        temporary.mkdir(parents=True, exist_ok=True)
        if self.runner.exists("hf"):
            argv = ["hf", "download", plan.model.repository, "--local-dir", str(temporary)]
        elif self.runner.exists("huggingface-cli"):
            argv = [
                "huggingface-cli",
                "download",
                plan.model.repository,
                "--local-dir",
                str(temporary),
            ]
        else:
            return CheckResult(
                "model",
                False,
                "Neither hf nor huggingface-cli is installed",
            )
        if plan.model_revision:
            argv.extend(["--revision", plan.model_revision])
        result = self.runner.run(argv, timeout=7200)
        if not result.ok:
            return CheckResult("model", False, result.stderr or result.stdout)
        if not (temporary / "config.json").is_file():
            return CheckResult("model", False, "Downloaded model is missing config.json")
        if target.exists():
            return CheckResult("model", False, f"Target appeared during download: {target}")
        os.replace(temporary, target)
        return CheckResult("model", True, f"Downloaded and activated {plan.model.repository}")


def _split_address(address: str) -> tuple[str, int]:
    host, separator, raw_port = address.rpartition(":")
    if not separator or not host:
        raise DeploymentError(f"invalid address: {address}")
    try:
        return host, int(raw_port)
    except ValueError as exc:
        raise DeploymentError(f"invalid port in address: {address}") from exc


def write_plan(path: Path, plan: DeploymentPlan) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(plan.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def read_plan(path: Path) -> DeploymentPlan:
    return DeploymentPlan.from_dict(json.loads(path.read_text(encoding="utf-8")))

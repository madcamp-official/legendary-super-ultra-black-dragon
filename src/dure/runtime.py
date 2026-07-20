from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path

from .command import CommandResult, Runner, SubprocessRunner
from .models import CheckResult, DeploymentPlan, NodeAssignment, NodeProfile


class DeploymentError(RuntimeError):
    pass


DEPLOYMENT_IDENTITY_FORMAT = (
    '{{.Id}}\t{{.State.Status}}\t'
    '{{index .Config.Labels "dure.deployment"}}\t'
    '{{index .Config.Labels "dure.generation"}}\t'
    '{{index .Config.Labels "dure.node"}}'
)
STOPPED_CONTAINER_STATES = frozenset({"created", "exited", "dead"})
MISSING_DOCKER_LABEL_VALUES = frozenset({"", "<no value>", "<nil>"})


@dataclass(frozen=True)
class DeploymentContainerIdentity:
    container_id: str
    state: str
    deployment_id: str
    generation: str
    node_id: str

    @classmethod
    def parse(cls, value: str) -> "DeploymentContainerIdentity | None":
        parts = value.split("\t")
        # SubprocessRunner strips a trailing tab when an older Dure container
        # has no dure.node label. Docker may also render a missing map value as
        # ``<no value>`` depending on the engine version.
        if len(parts) == 4:
            parts.append("")
        if len(parts) != 5:
            return None
        parts[2:] = [
            "" if part in MISSING_DOCKER_LABEL_VALUES else part
            for part in parts[2:]
        ]
        if any(not part for part in parts[:4]):
            return None
        return cls(*parts)

    def matches(self, deployment_id: str, generation: int, node_id: str) -> bool:
        return (
            self.deployment_id == deployment_id
            and self.generation == str(generation)
            and (not self.node_id or self.node_id == node_id)
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
            deployment_id, generation, node_id
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
    ) -> CheckResult:
        check, _ = self.running_container_identity(
            name,
            deployment_id=deployment_id,
            generation=generation,
            node_id=node_id,
            check_name=check_name,
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
            deployment_id, generation, node_id
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
    ) -> CheckResult:
        """Stop only containers carrying the exact Dure deployment label."""
        if self.engine != "docker":
            return CheckResult("deployment-stop", False, "Apply mode currently supports Docker only")
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
            if identity.deployment_id != deployment_id:
                return CheckResult(
                    "deployment-stop", False, "Deployment container label mismatch"
                )
            if generation is not None and identity.generation != str(generation):
                return CheckResult(
                    "deployment-stop", False, "Deployment generation label mismatch"
                )
            if node_id is not None and identity.node_id and identity.node_id != node_id:
                return CheckResult(
                    "deployment-stop", False, "Deployment node label mismatch"
                )
            if identity.state != "running":
                return CheckResult(
                    "deployment-stop", False, "Deployment container is no longer running"
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

        name = f"dure-ray-{plan.deployment_id}"
        proceed, existing_check = self._prepare_named_container(
            name,
            deployment_id=plan.deployment_id,
            generation=plan.generation,
            node_id=assignment.node_id,
            replace=replace,
            check_name="ray-container",
        )
        if not proceed:
            assert existing_check is not None
            return existing_check

        local_ip = profile.network.addresses[0] if profile.network.addresses else "127.0.0.1"
        _, port = _split_address(plan.ray_head_address)
        ray_command = ["start", "--block"]
        if assignment.role == "ray-head":
            ray_command.extend(
                ["--head", f"--node-ip-address={local_ip}", f"--port={port}"]
            )
        else:
            ray_command.append(f"--address={plan.ray_head_address}")
        ray_command.extend(["--min-worker-port=20000", "--max-worker-port=21000"])

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
            "--entrypoint",
            "ray",
            "--mount",
            f"type=bind,src={plan.model_path},dst=/models/model,readonly",
            "-e",
            f"NCCL_SOCKET_IFNAME={plan.network_interface}",
            "-e",
            f"GLOO_SOCKET_IFNAME={plan.network_interface}",
            "-e",
            "VLLM_ATTENTION_BACKEND=FLASH_ATTN",
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
        if assignment.role != "ray-head":
            return CheckResult(
                "vllm-api-start",
                True,
                "API runs on the Ray head node",
                blocking=False,
            )
        name = f"dure-api-{plan.deployment_id}"
        proceed, existing_check = self._prepare_named_container(
            name,
            deployment_id=plan.deployment_id,
            generation=plan.generation,
            node_id=assignment.node_id,
            replace=replace,
            check_name="vllm-api-start",
        )
        if not proceed:
            assert existing_check is not None
            return existing_check

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
            f"type=bind,src={plan.model_path},dst=/models/model,readonly",
            "-e",
            f"RAY_ADDRESS={plan.ray_head_address}",
            "-e",
            f"NCCL_SOCKET_IFNAME={plan.network_interface}",
            "-e",
            f"GLOO_SOCKET_IFNAME={plan.network_interface}",
            "-e",
            "VLLM_ATTENTION_BACKEND=FLASH_ATTN",
            "--label",
            f"dure.deployment={plan.deployment_id}",
            "--label",
            f"dure.generation={plan.generation}",
            "--label",
            f"dure.node={assignment.node_id}",
            "--label",
            f"dure.model={plan.model.model_id}",
            "--entrypoint",
            "vllm",
            plan.image,
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
            "127.0.0.1",
            "--port",
            "8000",
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

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

from .command import CommandResult, Runner, SubprocessRunner
from .models import CheckResult, DeploymentPlan, NodeAssignment, NodeProfile


class DeploymentError(RuntimeError):
    pass


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

    def stop_deployment(self, deployment_id: str) -> CheckResult:
        """Stop only containers carrying the exact Dure deployment label."""
        if self.engine != "docker":
            return CheckResult("deployment-stop", False, "Apply mode currently supports Docker only")
        listed = self.runner.run(
            [
                self.engine,
                "ps",
                "-q",
                "--filter",
                f"label=dure.deployment={deployment_id}",
            ],
            timeout=15,
        )
        if not listed.ok:
            return CheckResult("deployment-stop", False, listed.stderr or listed.stdout)
        container_ids = [item for item in listed.stdout.splitlines() if item]
        if not container_ids:
            return CheckResult("deployment-stop", True, "No running Dure deployment containers")
        stopped = self.runner.run([self.engine, "stop", "--time", "30", *container_ids], timeout=60)
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
        existing = self.inspect_container(name)
        if existing.ok and existing.stdout == "running":
            return CheckResult("ray-container", True, f"Container is already running: {name}")
        if existing.ok:
            if not replace:
                return CheckResult(
                    "ray-container",
                    False,
                    f"Stopped container exists: {name}; rerun with --replace",
                )
            removed = self.runner.run([self.engine, "rm", name], timeout=30)
            if not removed.ok:
                return CheckResult("ray-container", False, removed.stderr or removed.stdout)

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
            return CheckResult("vllm-api", True, "API runs on the Ray head node", blocking=False)
        name = f"dure-api-{plan.deployment_id}"
        existing = self.inspect_container(name)
        if existing.ok and existing.stdout == "running":
            return CheckResult("vllm-api", True, f"Container is already running: {name}")
        if existing.ok:
            if not replace:
                return CheckResult(
                    "vllm-api", False, f"Stopped container exists: {name}; rerun with --replace"
                )
            removed = self.runner.run([self.engine, "rm", name], timeout=30)
            if not removed.ok:
                return CheckResult("vllm-api", False, removed.stderr or removed.stdout)

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
            "vllm-api",
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

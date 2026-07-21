from __future__ import annotations

from dure.command import CommandResult
from dure.model_cache import (
    MODEL_CACHE_KIND_FULL_SNAPSHOT,
    MODEL_CACHE_VERIFICATION_VERSION,
)
from dure.models import (
    DeploymentPlan,
    GPUProfile,
    InstalledModelProfile,
    ModelSpec,
    NetworkProfile,
    NodeAssignment,
    NodeProfile,
    RuntimeProfile,
    VLLM_RAY_PP_BACKEND,
    VLLM_RAY_PP_RUNTIME_VERSION,
)


def profile(
    node_id: str,
    *,
    gpu_memory_mib: int | None = 24576,
    gpu_index: int = 0,
    address: str = "192.168.0.10",
    driver: str = "610.43.02",
    compute_capability: str = "8.6",
) -> NodeProfile:
    gpus = []
    if gpu_memory_mib is not None:
        gpus.append(
            GPUProfile(
                index=gpu_index,
                name="NVIDIA GeForce RTX 3090",
                uuid=f"GPU-{node_id}",
                driver_version=driver,
                memory_mib=gpu_memory_mib,
                compute_capability=compute_capability,
            )
        )
    return NodeProfile(
        node_id=node_id,
        hostname=node_id,
        os_name="Ubuntu 22.04",
        os_version="22.04",
        kernel="5.15.0-test",
        architecture="x86_64",
        virtualization="kvm",
        cpu_model="Test CPU",
        cpu_count=40,
        memory_mib=48000,
        memory_available_mib=40000,
        swap_mib=0,
        disk_total_mib=100000,
        disk_free_mib=80000,
        gpus=gpus,
        network=NetworkProfile(
            default_interface="ens3",
            addresses=[address],
            default_interface_addresses=[address],
        ),
        runtime=RuntimeProfile(
            engine="docker",
            engine_ready=True,
            nvidia_runtime=True,
            ray_available=True,
            ray_version="ray, version 2.56.1",
        ),
    )


def strict_pipeline_fixture():
    head = profile(
        "11111111-1111-4111-8111-111111111111",
        address="192.168.0.10",
    )
    worker = profile(
        "22222222-2222-4222-8222-222222222222",
        address="192.168.0.11",
    )
    revision = "a" * 40
    model_path = "/var/lib/dure/models/sha256-" + "b" * 64
    for item in (head, worker):
        item.installed_models = [
            InstalledModelProfile(
                source="dure",
                model_id="Qwen/Test-AWQ",
                path=model_path,
                revision=revision,
                quantization="awq",
                size_mib=8192,
                complete=True,
                manifest_digest="sha256:" + "b" * 64,
                cache_kind=MODEL_CACHE_KIND_FULL_SNAPSHOT,
                verification_version=MODEL_CACHE_VERIFICATION_VERSION,
            )
        ]
    assignments = [
        NodeAssignment(
            node_id=head.node_id,
            gpu_index=0,
            rank=0,
            pipeline_rank=0,
            layer_start=0,
            layer_end=15,
            role="ray-head",
            expected_runtime_rank=0,
            runtime_address="192.168.0.10",
        ),
        NodeAssignment(
            node_id=worker.node_id,
            gpu_index=0,
            rank=1,
            pipeline_rank=1,
            layer_start=16,
            layer_end=31,
            role="ray-worker",
            expected_runtime_rank=1,
            runtime_address="192.168.0.11",
        ),
    ]
    plan = DeploymentPlan(
        deployment_id="33333333-3333-4333-8333-333333333333",
        generation=2,
        model=ModelSpec(
            model_id="qwen-test-awq",
            repository="Qwen/Test-AWQ",
            quantization="awq",
            checkpoint_gib=8,
            min_gpu_memory_gib=8,
            default_max_model_len=4096,
            layer_count=32,
        ),
        image="registry.example/vllm@sha256:" + "c" * 64,
        pipeline_parallel_size=2,
        tensor_parallel_size=1,
        ray_head_node_id=head.node_id,
        ray_head_address="192.168.0.10:6379",
        network_interface="ens3",
        model_revision=revision,
        model_path=model_path,
        assignments=assignments,
        max_model_len=4096,
        execution_backend=VLLM_RAY_PP_BACKEND,
        runtime_vllm_version=VLLM_RAY_PP_RUNTIME_VERSION,
        model_cache_kind=MODEL_CACHE_KIND_FULL_SNAPSHOT,
    )
    plan.validate_execution_contract()
    return plan, head, worker


class FakeRunner:
    def __init__(self, responses=None, executables=None, response_factory=None):
        self.responses = responses or {}
        self.executables = set(executables or [])
        self.response_factory = response_factory
        self.calls: list[tuple[str, ...]] = []
        self.limited_output_calls: list[tuple[tuple[str, ...], int]] = []

    def exists(self, executable: str) -> bool:
        return executable in self.executables

    def run(self, argv, *, timeout=15, env=None):
        command = tuple(argv)
        self.calls.append(command)
        value = self.responses.get(command)
        if value is None and command == (
            "nvidia-smi",
            "--query-compute-apps=gpu_uuid",
            "--format=csv,noheader,nounits",
        ):
            return CommandResult(command, 0, "")
        if (
            value is None
            and command[:3] == ("docker", "container", "ls")
            and command[-1:] == ("{{.ID}}\t{{.Names}}",)
            and any(part.startswith("name=") for part in command)
        ):
            return CommandResult(command, 0, "")
        if value is None and self.response_factory is not None:
            value = self.response_factory(command)
        if isinstance(value, CommandResult):
            return value
        if value is None:
            return CommandResult(command, 0, "")
        return CommandResult(command, *value)

    def run_limited_output(
        self, argv, *, timeout=15, max_output_bytes, env=None
    ):
        command = tuple(argv)
        self.limited_output_calls.append((command, max_output_bytes))
        result = self.run(argv, timeout=timeout, env=env)
        size = len(result.stdout.encode("utf-8")) + len(result.stderr.encode("utf-8"))
        if size > max_output_bytes:
            return CommandResult(
                command, 125, stderr="command output limit exceeded"
            )
        return result

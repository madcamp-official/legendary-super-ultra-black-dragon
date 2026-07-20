from __future__ import annotations

from dure.command import CommandResult
from dure.models import GPUProfile, NetworkProfile, NodeProfile, RuntimeProfile


def profile(
    node_id: str,
    *,
    gpu_memory_mib: int | None = 24576,
    gpu_index: int = 0,
    address: str = "192.168.0.10",
    driver: str = "610.43.02",
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
                compute_capability="8.6",
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
        network=NetworkProfile(default_interface="ens3", addresses=[address]),
        runtime=RuntimeProfile(
            engine="docker",
            engine_ready=True,
            nvidia_runtime=True,
            ray_available=True,
            ray_version="ray, version 2.56.1",
        ),
    )


class FakeRunner:
    def __init__(self, responses=None, executables=None):
        self.responses = responses or {}
        self.executables = set(executables or [])
        self.calls: list[tuple[str, ...]] = []

    def exists(self, executable: str) -> bool:
        return executable in self.executables

    def run(self, argv, *, timeout=15, env=None):
        command = tuple(argv)
        self.calls.append(command)
        value = self.responses.get(command)
        if isinstance(value, CommandResult):
            return value
        if value is None:
            return CommandResult(command, 0, "")
        return CommandResult(command, *value)


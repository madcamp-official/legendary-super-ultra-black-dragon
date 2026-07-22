from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass

from .resource_pool import FLEET_MODEL_IDS, FLEET_TENSOR_PARALLEL_SIZE


AUTO_PROFILE_GENERATOR_VERSION = "fleet-placement-v3"
AUTO_PROFILE_ORIGIN = "AUTO"
PLACEMENT_PROFILE_STATUSES = frozenset(
    {"DRAFT", "QUALIFYING", "VALIDATED", "ACTIVE", "REVOKED"}
)


@dataclass(frozen=True)
class AutoPlacementProfileSpec:
    model_id: str
    profile_id: str
    topology: str
    node_count: int
    min_gpu_memory_mib: int
    min_disk_free_mib: int
    pipeline_parallel_size: int
    tensor_parallel_size: int
    max_model_len: int
    max_concurrency: int
    requires_network_evidence: bool
    requires_nccl: bool
    min_bandwidth_mbps: int | None
    max_rtt_ms: float | None
    max_packet_loss_pct: float | None
    max_ttft_p95_ms: float
    max_tpot_p95_ms: float
    max_e2e_p95_ms: float
    min_success_rate: float
    min_vram_headroom_pct: float
    min_throughput_tps: float

    @property
    def spec_digest(self) -> str:
        payload = {
            "generator_version": AUTO_PROFILE_GENERATOR_VERSION,
            **asdict(self),
        }
        encoded = json.dumps(
            payload, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return "sha256:" + hashlib.sha256(encoded).hexdigest()

    def to_dict(self) -> dict[str, object]:
        return {
            **asdict(self),
            "origin": AUTO_PROFILE_ORIGIN,
            "status": "DRAFT",
            "generator_version": AUTO_PROFILE_GENERATOR_VERSION,
            "spec_digest": self.spec_digest,
        }

    def create_kwargs(self) -> dict[str, object]:
        values = asdict(self)
        values.pop("model_id")
        return {
            **values,
            "origin": AUTO_PROFILE_ORIGIN,
            "status": "DRAFT",
            "spec_digest": self.spec_digest,
        }


_SINGLE_GPU = {
    "qwen2.5-7b-awq": {
        "min_gpu_memory_mib": 8192,
        "min_disk_free_mib": 6144,
        "max_model_len": 8192,
        "max_concurrency": 4,
        "min_throughput_tps": 20.0,
    },
    "qwen2.5-14b-awq": {
        "min_gpu_memory_mib": 12288,
        "min_disk_free_mib": 12288,
        "max_model_len": 8192,
        "max_concurrency": 2,
        "min_throughput_tps": 12.0,
    },
    "qwen2.5-32b-awq": {
        "min_gpu_memory_mib": 24576,
        "min_disk_free_mib": 25600,
        "max_model_len": 4096,
        "max_concurrency": 1,
        "min_throughput_tps": 6.0,
    },
}


def _base_spec(
    *,
    model_id: str,
    profile_id: str,
    topology: str,
    node_count: int,
    min_gpu_memory_mib: int,
    min_disk_free_mib: int,
    pipeline_parallel_size: int,
    max_model_len: int,
    max_concurrency: int,
    min_throughput_tps: float,
    distributed_min_bandwidth_mbps: int = 10000,
    max_ttft_p95_ms: float = 2000.0,
    max_tpot_p95_ms: float = 100.0,
    max_e2e_p95_ms: float = 10000.0,
) -> AutoPlacementProfileSpec:
    distributed = node_count > 1
    return AutoPlacementProfileSpec(
        model_id=model_id,
        profile_id=profile_id,
        topology=topology,
        node_count=node_count,
        min_gpu_memory_mib=min_gpu_memory_mib,
        min_disk_free_mib=min_disk_free_mib,
        pipeline_parallel_size=pipeline_parallel_size,
        tensor_parallel_size=FLEET_TENSOR_PARALLEL_SIZE,
        max_model_len=max_model_len,
        max_concurrency=max_concurrency,
        requires_network_evidence=distributed,
        requires_nccl=distributed,
        min_bandwidth_mbps=(
            distributed_min_bandwidth_mbps if distributed else None
        ),
        max_rtt_ms=2.0 if distributed else None,
        max_packet_loss_pct=0.1 if distributed else None,
        max_ttft_p95_ms=max_ttft_p95_ms,
        max_tpot_p95_ms=max_tpot_p95_ms,
        max_e2e_p95_ms=max_e2e_p95_ms,
        min_success_rate=0.99,
        min_vram_headroom_pct=10.0,
        min_throughput_tps=min_throughput_tps,
    )


def generate_auto_placement_profile_specs(
    model_id: str,
) -> tuple[AutoPlacementProfileSpec, ...]:
    """Return the closed, deterministic DRAFT profile set for one Fleet model."""

    if model_id not in FLEET_MODEL_IDS:
        raise ValueError("automatic placement profiles support only the Fleet allowlist")
    if model_id in _SINGLE_GPU:
        values = _SINGLE_GPU[model_id]
        return (
            _base_spec(
                model_id=model_id,
                profile_id=f"auto-{model_id}-tp1-pp1-v3",
                topology="single-gpu",
                node_count=1,
                pipeline_parallel_size=1,
                **values,
            ),
        )

    profiles = []
    for pipeline_parallel_size, min_gpu_memory_mib, min_disk_free_mib, throughput in (
        (1, 49152, 51200, 4.0),
        (2, 24576, 51200, 6.0),
        # The PP=3 STAGE delivery gate separately requires 2x each exact rank's
        # bytes plus its fixed margin. Requiring another full 50 GiB here made
        # an already validated rank and pinned runtime impossible to qualify on
        # the supported 100 GiB nodes.
        (3, 24576, 8192, 1.0),
    ):
        profiles.append(
            _base_spec(
                model_id=model_id,
                profile_id=(
                    f"auto-{model_id}-tp1-pp{pipeline_parallel_size}-v3"
                ),
                topology=(
                    "single-gpu"
                    if pipeline_parallel_size == 1
                    else "pipeline"
                ),
                node_count=pipeline_parallel_size,
                min_gpu_memory_mib=min_gpu_memory_mib,
                min_disk_free_mib=min_disk_free_mib,
                pipeline_parallel_size=pipeline_parallel_size,
                max_model_len=8192,
                max_concurrency=1,
                min_throughput_tps=throughput,
                **(
                    {
                        "distributed_min_bandwidth_mbps": 2000,
                        "max_ttft_p95_ms": 30000.0,
                        "max_tpot_p95_ms": 250.0,
                        "max_e2e_p95_ms": 45000.0,
                    }
                    if pipeline_parallel_size == 3
                    else {}
                ),
            )
        )
    return tuple(profiles)

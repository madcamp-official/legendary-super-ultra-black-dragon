from __future__ import annotations

from dataclasses import dataclass

from .models import ModelSpec


CATALOG_VERSION = "2026-07-20.1"
POLICY_VERSION = "quality-within-slo-v1"


@dataclass(frozen=True)
class PlacementProfile:
    profile_id: str
    node_count: int
    min_gpu_memory_mib: int
    min_disk_free_mib: int
    pipeline_parallel_size: int = 1
    tensor_parallel_size: int = 1
    required_engine: str = "docker"
    min_compute_capability: str | None = "7.5"
    requires_engine_ready: bool = True
    requires_nvidia_runtime: bool = True
    requires_network_evidence: bool = False


@dataclass(frozen=True)
class CatalogEntry:
    model: ModelSpec
    placement: PlacementProfile
    quality_rank: int
    artifact_revision: str | None = None


@dataclass(frozen=True)
class ModelCatalog:
    version: str
    policy_version: str
    entries: tuple[CatalogEntry, ...]

    @property
    def models(self) -> dict[str, ModelSpec]:
        return {entry.model.model_id: entry.model for entry in self.entries}

    def entry(self, model_id: str) -> CatalogEntry:
        for entry in self.entries:
            if entry.model.model_id == model_id:
                return entry
        raise KeyError(model_id)


STATIC_CATALOG = ModelCatalog(
    version=CATALOG_VERSION,
    policy_version=POLICY_VERSION,
    entries=(
        CatalogEntry(
            model=ModelSpec(
                model_id="qwen2.5-7b-awq",
                repository="Qwen/Qwen2.5-7B-Instruct-AWQ",
                quantization="awq",
                checkpoint_gib=4.8,
                min_gpu_memory_gib=8,
                default_max_model_len=8192,
                layer_count=28,
            ),
            placement=PlacementProfile(
                profile_id="single-gpu-8g",
                node_count=1,
                min_gpu_memory_mib=8192,
                min_disk_free_mib=6144,
            ),
            quality_rank=7,
        ),
        CatalogEntry(
            model=ModelSpec(
                model_id="qwen2.5-14b-awq",
                repository="Qwen/Qwen2.5-14B-Instruct-AWQ",
                quantization="awq",
                checkpoint_gib=9.5,
                min_gpu_memory_gib=12,
                default_max_model_len=8192,
                layer_count=48,
            ),
            placement=PlacementProfile(
                profile_id="single-gpu-12g",
                node_count=1,
                min_gpu_memory_mib=12288,
                min_disk_free_mib=12288,
            ),
            quality_rank=14,
        ),
        CatalogEntry(
            model=ModelSpec(
                model_id="qwen2.5-32b-awq",
                repository="Qwen/Qwen2.5-32B-Instruct-AWQ",
                quantization="awq",
                checkpoint_gib=19.5,
                min_gpu_memory_gib=24,
                default_max_model_len=4096,
                layer_count=64,
            ),
            placement=PlacementProfile(
                profile_id="single-gpu-24g",
                node_count=1,
                min_gpu_memory_mib=24576,
                min_disk_free_mib=25600,
            ),
            quality_rank=32,
        ),
        CatalogEntry(
            model=ModelSpec(
                model_id="qwen2.5-72b-awq",
                repository="Qwen/Qwen2.5-72B-Instruct-AWQ",
                quantization="awq",
                checkpoint_gib=38.74,
                min_gpu_memory_gib=24,
                default_max_model_len=8192,
                layer_count=80,
            ),
            placement=PlacementProfile(
                profile_id="pipeline-3x24g",
                node_count=3,
                min_gpu_memory_mib=24576,
                min_disk_free_mib=51200,
                pipeline_parallel_size=3,
                requires_network_evidence=True,
            ),
            quality_rank=72,
        ),
    ),
)

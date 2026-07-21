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
    # 중앙에서 생성한 AUTO 프로필은 단일 노드라도 qualification을
    # 통과한 exact node/GPU 결합만 선택해야 한다. 정적 로컬 카탈로그는
    # 기본값을 유지해 기존 오프라인 계획 동작을 보존한다.
    requires_qualification_evidence: bool = False


@dataclass(frozen=True)
class NetworkEvidenceBinding:
    evidence_id: str
    evidence_digest: str
    node_ids: tuple[str, ...]
    registered_at: str
    rank_node_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class StageRankDelivery:
    rank: int
    pipeline_rank: int
    tensor_rank: int
    manifest_digest: str
    tensor_key_count: int
    tensor_keys_digest: str
    weight_size_bytes: int
    total_size_bytes: int
    file_count: int


@dataclass(frozen=True)
class StageArtifactDelivery:
    artifact_set_digest: str
    contract_identity_digest: str
    source_manifest_digest: str
    runtime_image: str
    vllm_version: str
    exporter_build_digest: str
    architecture: str
    quantization: str
    tensor_parallel_size: int
    pipeline_parallel_size: int
    loader_format: str
    ranks: tuple[StageRankDelivery, ...]


@dataclass(frozen=True)
class CatalogEntry:
    model: ModelSpec
    placement: PlacementProfile
    quality_rank: int
    artifact_revision: str | None = None
    candidate_id: str | None = None
    gpu_architectures: tuple[str, ...] = ()
    network_evidence: tuple[NetworkEvidenceBinding, ...] = ()
    stage_artifact: StageArtifactDelivery | None = None
    full_snapshot_size_bytes: int | None = None


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

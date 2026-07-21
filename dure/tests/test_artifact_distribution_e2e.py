from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlalchemy import func, select

from dure.agent import TaskExecutor
from dure.artifact_download import ArtifactChunkDownloader, TrustedHTTPSOrigin
from dure.artifact_prepare import ArtifactPreparationExecutor
from dure.command import CommandResult
from dure.control.api import create_app
from dure.control.benchmark import (
    BENCHMARK_POLICY_VERSION,
    BENCHMARK_SUITE_ID,
    benchmark_inventory_fingerprint,
    promote_model_release,
    register_benchmark_evidence,
)
from dure.control.models import (
    Deployment,
    DeploymentOperation,
    Node,
    NodeArtifactCache,
    RuntimeRelease,
    Task,
)
from dure.control.service import (
    add_placement_profile,
    canonical_artifact_manifest_digest,
    create_model_artifact,
    create_model_release,
    create_runtime_release,
    register_artifact_manifest,
    transition_model_release,
)
from dure.control.stage_artifacts import (
    register_stage_artifact_evidence,
    register_stage_artifact_variant,
    transition_stage_artifact_variant,
)
from dure.model_cache import (
    MODEL_CACHE_MARKER_FILE,
    MODEL_CACHE_KIND_FULL_SNAPSHOT,
    MODEL_CACHE_KIND_STAGE,
    read_model_cache_marker,
)
from dure.model_store import (
    CacheIdentity,
    ContentAddressedModelStore,
    ModelCachePreparer,
)
from dure.models import CheckResult, DeploymentPlan
from dure.pipeline_runtime import pipeline_contract_detail
from dure.stage_cache import (
    StageCacheIdentity,
    validate_materialized_stage_cache,
)

from .helpers import profile


ORIGIN = TrustedHTTPSOrigin("https://artifacts.example.test/models")
RUNTIME_IMAGE = "registry.example/vllm@sha256:" + "f" * 64
SOURCE_RUNTIME_IMAGE = "registry.example/vllm@sha256:" + "e" * 64
EXPORTER_DIGEST = "sha256:" + "1" * 64


def _digest(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _regular_file(path: str, payload: bytes) -> dict:
    return {
        "path": path,
        "kind": "REGULAR",
        "size_bytes": len(payload),
        "sha256": _digest(payload),
        "chunks": [
            {
                "ordinal": 0,
                "offset_bytes": 0,
                "length_bytes": len(payload),
                "sha256": _digest(payload),
            }
        ],
    }


def _tensor_keys_digest(keys: list[str]) -> str:
    encoded = json.dumps(
        {"schema_version": 1, "tensor_keys": keys},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return _digest(encoded)


def _source_manifest(*, suffix: bytes = b"") -> tuple[dict, dict[str, bytes]]:
    files = {
        "config.json": b'{"model_type":"qwen2","quantization_config":{"quant_method":"awq"}}',
        "model.safetensors": b"source-model-for-selection" + suffix,
    }
    manifest = {
        "schema_version": 1,
        "files": [_regular_file(path, payload) for path, payload in files.items()],
    }
    return manifest, {_digest(payload): payload for payload in files.values()}


def _stage_manifest(
    rank: int,
    *,
    source_manifest_digest: str,
) -> tuple[dict, str, dict[str, bytes]]:
    tensor_keys = [f"model.layers.{rank}.weight"]
    tensor_keys_digest = _tensor_keys_digest(tensor_keys)
    contract = {
        "schema_version": 1,
        "source_manifest_digest": source_manifest_digest,
        "runtime_image": RUNTIME_IMAGE,
        "exporter_build_digest": EXPORTER_DIGEST,
        "model_family": "qwen2.5",
        "architecture": "Qwen2ForCausalLM",
        "quantization": "awq",
        "tensor_parallel_size": 1,
        "pipeline_parallel_size": 3,
        "loader_format": "sharded_state",
        "vllm_version": "0.9.0",
        "max_part_bytes": 5 * 1024**3,
        "trust_remote_code": False,
        "enable_lora": False,
        "is_moe": False,
        "is_multimodal": False,
    }
    marker = json.dumps(
        {
            "schema_version": 1,
            "kind": "VLLM_SHARDED_STATE_PIPELINE_STAGE",
            "contract": contract,
            "pipeline_rank": rank,
            "weight_pattern": "model-rank-0-part-*.safetensors",
            "metadata_files": [
                "config.json",
                "tokenizer.json",
                "tokenizer_config.json",
            ],
            "tensors": [
                {
                    "name": tensor_keys[0],
                    "dtype": "F16",
                    "shape": [1],
                }
            ],
            "tensor_key_digest": tensor_keys_digest,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    files = {
        "config.json": b'{"model_type":"qwen2","quantization_config":{"quant_method":"awq"}}',
        "tokenizer.json": b'{"version":"1.0"}',
        "tokenizer_config.json": b'{"model_max_length":4096}',
        "dure-stage.json": marker,
        "model-rank-0-part-0.safetensors": b"rank-local-weight-" + bytes([rank]),
    }
    manifest = {
        "schema_version": 1,
        "files": [_regular_file(path, payload) for path, payload in files.items()],
    }
    return (
        manifest,
        tensor_keys_digest,
        {_digest(payload): payload for payload in files.values()},
    )


class MemoryResponse:
    def __init__(self, payload: bytes, *, offset: int = 0) -> None:
        self.payload = payload[offset:]
        self.status = 200 if offset == 0 else 206
        self.offset = 0
        self.original_size = len(payload)
        self.range_offset = offset

    def header_values(self, name: str) -> tuple[str, ...]:
        lowered = name.lower()
        if lowered == "content-length":
            return (str(len(self.payload)),)
        if lowered == "content-range" and self.range_offset:
            return (
                f"bytes {self.range_offset}-{self.original_size - 1}/{self.original_size}",
            )
        return ()

    def read(self, size: int) -> bytes:
        value = self.payload[self.offset : self.offset + size]
        self.offset += len(value)
        return value

    def close(self) -> None:
        return None


class MemoryTransport:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = dict(objects)
        self.calls: list[tuple[str, dict[str, str]]] = []

    def open(self, origin, object_url, *, headers, timeout_seconds):
        del origin, timeout_seconds
        self.calls.append((object_url, dict(headers)))
        digest = "sha256:" + object_url.rsplit("/", 1)[-1]
        range_header = headers.get("Range")
        offset = 0
        if range_header is not None:
            self.assert_range_header(range_header)
            offset = int(range_header.removeprefix("bytes=").removesuffix("-"))
        return MemoryResponse(self.objects[digest], offset=offset)

    @staticmethod
    def assert_range_header(value: str) -> None:
        if not value.startswith("bytes=") or not value.endswith("-"):
            raise AssertionError("unexpected Range header")


class ImagePullRunner:
    def __init__(self, allowed_images: set[str]) -> None:
        self.allowed_images = frozenset(allowed_images)
        self.pulled_images: set[str] = set()
        self.calls: list[tuple[str, ...]] = []

    def exists(self, executable: str) -> bool:
        return executable == "docker"

    def run(self, argv, *, timeout=15, env=None):
        del timeout, env
        command = tuple(argv)
        self.calls.append(command)
        if command[:3] == ("docker", "image", "inspect"):
            image = command[-1]
            if image not in self.pulled_images:
                return CommandResult(command, 1, stderr="missing")
            return CommandResult(command, 0, json.dumps([image]))
        if command[:3] == ("docker", "pull", "--quiet"):
            image = command[-1]
            if image not in self.allowed_images:
                raise AssertionError(f"unexpected image pull: {command}")
            self.pulled_images.add(image)
            return CommandResult(command, 0, image)
        raise AssertionError(f"unexpected host command: {command}")

    def run_limited_output(
        self, argv, *, timeout=15, max_output_bytes, env=None
    ):
        del max_output_bytes
        return self.run(argv, timeout=timeout, env=env)


class ArtifactDistributionLifecycleE2ETests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        database_url = f"sqlite:///{self.root / 'lifecycle.db'}"
        self.client = TestClient(
            create_app(
                database_url=database_url,
                admin_token="admin-secret",
                create_schema=True,
            )
        )
        self.factory = self.client.app.state.session_factory
        self.admin = {"Authorization": "Bearer admin-secret"}

    def tearDown(self) -> None:
        self.client.close()
        self.temporary.cleanup()

    def _enroll_nodes(self) -> list[dict]:
        enrolled = []
        for rank in range(3):
            enrollment = self.client.post(
                "/v1/admin/enrollments", headers=self.admin, json={}
            )
            self.assertEqual(enrollment.status_code, 200, enrollment.text)
            address = f"10.60.0.{rank + 10}"
            reported = profile(f"lifecycle-{rank}", address=address).to_dict()
            reported["disk_free_mib"] = 80000
            claimed = self.client.post(
                "/v1/enrollments/claim",
                json={
                    "token": enrollment.json()["token"],
                    "install_id": f"install-lifecycle-{rank}-{uuid.uuid4()}",
                    "agent_version": "0.3.20",
                    "profile": reported,
                },
            )
            self.assertEqual(claimed.status_code, 200, claimed.text)
            node_id = claimed.json()["node_id"]
            enrolled.append(
                {
                    "node_id": node_id,
                    "address": address,
                    "headers": {
                        "Authorization": f"Bearer {claimed.json()['credential']}"
                    },
                    "profile": profile(node_id, address=address),
                }
            )
        return enrolled

    def _qualify_pipeline_release(
        self,
        session,
        *,
        release,
        placement,
        artifact,
        runtime,
        nodes: list[Node],
        dure_commit: str,
    ) -> None:
        node_ids = sorted(node.id for node in nodes)
        register_benchmark_evidence(
            session,
            release_id=release.id,
            placement_id=placement.id,
            suite_id=BENCHMARK_SUITE_ID,
            node_ids=node_ids,
            inventory_fingerprint=benchmark_inventory_fingerprint(
                session, node_ids
            ),
            artifact_revision=artifact.revision,
            artifact_manifest_digest=artifact.manifest_digest,
            runtime_image=runtime.image,
            dure_commit=dure_commit,
            policy_version=BENCHMARK_POLICY_VERSION,
            input_tokens=4096,
            output_tokens=256,
            concurrency=8,
            warmup_requests=20,
            request_count=200,
            duration_seconds=900.0,
            oom_count=0,
            crash_count=0,
            restart_count=0,
            ttft_p95_ms=900.0,
            tpot_p95_ms=90.0,
            e2e_p95_ms=4500.0,
            throughput_tps=12.0,
            success_rate=1.0,
            vram_headroom_pct=12.0,
            quality_score=0.9,
            network_bandwidth_mbps=20000.0,
            network_rtt_ms=1.0,
            packet_loss_pct=0.0,
            nccl_all_reduce_ok=True,
        )
        promoted, _, changed = promote_model_release(session, release.id)
        self.assertTrue(changed)
        self.assertEqual(promoted.status, "ACTIVE")

    def _seed_qualified_stage_release(
        self, enrolled: list[dict]
    ) -> tuple[str, dict, dict[str, dict], dict[str, bytes]]:
        source_manifest, source_objects = _source_manifest()
        source_digest = canonical_artifact_manifest_digest(source_manifest)
        stage_manifests: list[dict] = []
        manifests_by_digest: dict[str, dict] = {}
        objects = dict(source_objects)
        for rank in range(3):
            manifest, tensor_keys_digest, rank_objects = _stage_manifest(
                rank, source_manifest_digest=source_digest
            )
            manifest_digest = canonical_artifact_manifest_digest(manifest)
            manifests_by_digest[manifest_digest] = manifest
            objects.update(rank_objects)
            stage_manifests.append(
                {
                    "pipeline_rank": rank,
                    "tensor_rank": 0,
                    "manifest_digest": manifest_digest,
                    "tensor_key_count": 1,
                    "tensor_keys_digest": tensor_keys_digest,
                    "weight_size_bytes": len(b"rank-local-weight-" + bytes([rank])),
                    "manifest": manifest,
                }
            )

        with self.factory() as session:
            nodes = [session.get(Node, item["node_id"]) for item in enrolled]
            self.assertTrue(all(node is not None for node in nodes))
            artifact = create_model_artifact(
                session,
                model_id="qwen-lifecycle-awq",
                repository="Example/Qwen-Lifecycle-AWQ",
                revision="a" * 40,
                manifest_digest=source_digest,
                quantization="awq",
                size_mib=8192,
                default_max_model_len=4096,
                layer_count=32,
                license_id="apache-2.0",
            )
            register_artifact_manifest(
                session, artifact_id=artifact.id, manifest=source_manifest
            )
            runtime = create_runtime_release(
                session,
                version="runtime-lifecycle",
                image=RUNTIME_IMAGE,
                vllm_version="0.9.0",
                cuda_version="12.8",
                gpu_architectures=["ampere"],
            )
            release = create_model_release(
                session,
                artifact_id=artifact.id,
                runtime_id=runtime.id,
                quality_rank=100,
            )
            placement = add_placement_profile(
                session,
                release_id=release.id,
                profile_id="pipeline-3x24g-lifecycle",
                topology="pipeline",
                node_count=3,
                min_gpu_memory_mib=8192,
                min_disk_free_mib=1,
                pipeline_parallel_size=3,
                tensor_parallel_size=1,
                requires_network_evidence=True,
                requires_nccl=True,
                min_bandwidth_mbps=10000,
                max_rtt_ms=5.0,
                max_packet_loss_pct=0.1,
                max_ttft_p95_ms=1000.0,
                max_tpot_p95_ms=100.0,
                max_e2e_p95_ms=5000.0,
                min_success_rate=0.99,
                min_vram_headroom_pct=10.0,
                min_throughput_tps=10.0,
            )
            transition_model_release(session, release.id, "VALIDATED")
            self._qualify_pipeline_release(
                session,
                release=release,
                placement=placement,
                artifact=artifact,
                runtime=runtime,
                nodes=nodes,
                dure_commit="e" * 40,
            )

            variant, _ = register_stage_artifact_variant(
                session,
                source_manifest_digest=source_digest,
                runtime_image=runtime.image,
                vllm_version=runtime.vllm_version,
                exporter_build_digest=EXPORTER_DIGEST,
                architecture="Qwen2ForCausalLM",
                quantization=artifact.quantization,
                tensor_parallel_size=1,
                pipeline_parallel_size=3,
                loader_format="VLLM_SHARDED_STATE_V1",
                stages=stage_manifests,
            )
            register_stage_artifact_evidence(
                session,
                variant.artifact_set_digest,
                schema_version=1,
                variant_identity_digest=variant.artifact_set_digest,
                validation_run_id=str(uuid.uuid4()),
                kind="GPU_EXPORT_LOAD",
                status="PASSED",
                validator_version="validator-1",
                validator_build_digest="sha256:" + "2" * 64,
                failure_code=None,
                ranks=[
                    {
                        "pipeline_rank": item["pipeline_rank"],
                        "tensor_rank": item["tensor_rank"],
                        "manifest_digest": item["manifest_digest"],
                        "tensor_keys_digest": item["tensor_keys_digest"],
                        "loaded_tensor_count": item["tensor_key_count"],
                        "loaded_weight_size_bytes": item["weight_size_bytes"],
                    }
                    for item in stage_manifests
                ],
            )
            validated = transition_stage_artifact_variant(
                session, variant.artifact_set_digest, "VALIDATED"
            )
            return (
                release.id,
                {
                    "artifact_set_digest": validated.artifact_set_digest,
                    "source_manifest_digest": source_digest,
                },
                manifests_by_digest,
                objects,
            )

    def _seed_qualified_full_release(
        self,
        enrolled: list[dict],
        *,
        key: str,
        suffix: bytes,
        revision_character: str,
        quality_rank: int,
        runtime_image: str,
    ) -> tuple[str, str, dict[str, bytes]]:
        source_manifest, objects = _source_manifest(suffix=suffix)
        source_digest = canonical_artifact_manifest_digest(source_manifest)
        with self.factory() as session:
            nodes = [session.get(Node, item["node_id"]) for item in enrolled]
            self.assertTrue(all(node is not None for node in nodes))
            artifact = create_model_artifact(
                session,
                model_id=f"qwen-{key}-awq",
                repository=f"Example/Qwen-{key}-AWQ",
                revision=revision_character * 40,
                manifest_digest=source_digest,
                quantization="awq",
                size_mib=4096,
                default_max_model_len=4096,
                layer_count=32,
                license_id="apache-2.0",
            )
            register_artifact_manifest(
                session, artifact_id=artifact.id, manifest=source_manifest
            )
            runtime = session.scalar(
                select(RuntimeRelease).where(
                    RuntimeRelease.image == runtime_image
                )
            )
            if runtime is None:
                runtime = create_runtime_release(
                    session,
                    version=f"runtime-{key}",
                    image=runtime_image,
                    vllm_version="0.9.0",
                    cuda_version="12.8",
                    gpu_architectures=["ampere"],
                )
            release = create_model_release(
                session,
                artifact_id=artifact.id,
                runtime_id=runtime.id,
                quality_rank=quality_rank,
            )
            placement = add_placement_profile(
                session,
                release_id=release.id,
                profile_id=f"pipeline-3x24g-{key}",
                topology="pipeline",
                node_count=3,
                min_gpu_memory_mib=8192,
                min_disk_free_mib=1,
                pipeline_parallel_size=3,
                tensor_parallel_size=1,
                requires_network_evidence=True,
                requires_nccl=True,
                min_bandwidth_mbps=10000,
                max_rtt_ms=5.0,
                max_packet_loss_pct=0.1,
                max_ttft_p95_ms=1000.0,
                max_tpot_p95_ms=100.0,
                max_e2e_p95_ms=5000.0,
                min_success_rate=0.99,
                min_vram_headroom_pct=10.0,
                min_throughput_tps=10.0,
            )
            transition_model_release(session, release.id, "VALIDATED")
            self._qualify_pipeline_release(
                session,
                release=release,
                placement=placement,
                artifact=artifact,
                runtime=runtime,
                nodes=nodes,
                dure_commit=revision_character * 40,
            )
            return release.id, source_digest, objects

    def _claim(self, enrolled: dict) -> dict:
        response = self.client.post(
            "/v1/agent/tasks/claim", headers=enrolled["headers"]
        )
        self.assertEqual(response.status_code, 200, response.text)
        task = response.json()["task"]
        self.assertIsNotNone(task, response.text)
        return task

    def _complete(self, enrolled: dict, task: dict, result: dict) -> None:
        response = self.client.post(
            f"/v1/agent/tasks/{task['id']}/complete",
            headers=enrolled["headers"],
            json={"result": result},
        )
        self.assertEqual(response.status_code, 200, response.text)

    @staticmethod
    def _stage_identity(task: dict) -> StageCacheIdentity:
        payload = task["payload"]
        return StageCacheIdentity(
            repository=payload["repository"],
            revision=payload["revision"],
            manifest_digest=payload["manifest_digest"],
            quantization=payload["quantization"],
            artifact_set_digest=payload["artifact_set_digest"],
            contract_identity_digest=payload["contract_identity_digest"],
            source_manifest_digest=payload["source_manifest_digest"],
            runtime_image=payload["runtime_image"],
            vllm_version=payload["vllm_version"],
            exporter_build_digest=payload["exporter_build_digest"],
            architecture=payload["architecture"],
            loader_format=payload["loader_format"],
            tensor_parallel_size=payload["tensor_parallel_size"],
            pipeline_parallel_size=payload["pipeline_parallel_size"],
            pipeline_rank=payload["pipeline_rank"],
            tensor_rank=payload["tensor_rank"],
            tensor_keys_digest=payload["tensor_keys_digest"],
        )

    @staticmethod
    def _full_identity(task: dict) -> CacheIdentity:
        payload = task["payload"]
        return CacheIdentity(
            repository=payload["repository"],
            revision=payload["revision"],
            manifest_digest=payload["manifest_digest"],
            quantization=payload["quantization"],
        )

    def _stage_identity_from_plan(
        self, plan: DeploymentPlan, node_id: str
    ) -> StageCacheIdentity:
        binding = plan.stage_artifact
        assignment = plan.assignment_for(node_id)
        self.assertIsNotNone(binding)
        self.assertIsNotNone(assignment)
        self.assertIsNotNone(assignment.stage_manifest_digest)
        self.assertIsNotNone(assignment.stage_tensor_keys_digest)
        return StageCacheIdentity(
            repository=plan.model.repository,
            revision=plan.model_revision,
            manifest_digest=assignment.stage_manifest_digest,
            quantization=plan.model.quantization,
            artifact_set_digest=binding.artifact_set_digest,
            contract_identity_digest=binding.contract_identity_digest,
            source_manifest_digest=binding.source_manifest_digest,
            runtime_image=binding.runtime_image,
            vllm_version=binding.vllm_version,
            exporter_build_digest=binding.exporter_build_digest,
            architecture=binding.architecture,
            loader_format=binding.loader_format,
            tensor_parallel_size=binding.tensor_parallel_size,
            pipeline_parallel_size=binding.pipeline_parallel_size,
            pipeline_rank=assignment.pipeline_rank,
            tensor_rank=0,
            tensor_keys_digest=assignment.stage_tensor_keys_digest,
        )

    def _execute_deployment_task(
        self,
        *,
        task: dict,
        enrolled: dict,
        executor: TaskExecutor,
        store: ContentAddressedModelStore,
        identity: CacheIdentity | StageCacheIdentity | None,
    ) -> dict:
        plan = DeploymentPlan.from_dict(task["payload"]["plan"])
        assignment = plan.assignment_for(enrolled["node_id"])
        self.assertIsNotNone(assignment)
        contract = CheckResult(
            "pipeline-rank-contract",
            True,
            pipeline_contract_detail(plan, assignment),
        )
        successful = {
            name: CheckResult(name, True, "verified")
            for name in (
                "ray-container",
                "host-gpu",
                "container-gpu",
                "vllm-api-start",
                "vllm-api",
                "deployment-stop",
            )
        }

        if task["type"] in {"APPLY_DEPLOYMENT", "START_DEPLOYMENT"}:
            def validate_rank_cache(observed_plan, observed_assignment):
                if type(identity) is not StageCacheIdentity:
                    self.fail("STAGE deployment task has no expected cache identity")
                self.assertEqual(observed_plan, plan)
                self.assertEqual(observed_assignment.node_id, enrolled["node_id"])
                return validate_materialized_stage_cache(
                    store.stage_cache_path(identity), identity
                )

            def validate_full_cache(observed_plan, *, accept_download):
                if type(identity) is not CacheIdentity:
                    self.fail("FULL deployment task has no expected cache identity")
                self.assertFalse(accept_download)
                self.assertEqual(
                    observed_plan.model.repository, identity.repository
                )
                self.assertEqual(observed_plan.model_revision, identity.revision)
                self.assertEqual(
                    observed_plan.model.quantization, identity.quantization
                )
                cache_path = store.model_cache_path(identity.manifest_digest)
                marker = read_model_cache_marker(
                    cache_path / MODEL_CACHE_MARKER_FILE
                )
                self.assertEqual(marker.repository, identity.repository)
                self.assertEqual(marker.revision, identity.revision)
                self.assertEqual(
                    marker.manifest_digest, identity.manifest_digest
                )
                self.assertEqual(marker.cache_kind, identity.cache_kind)
                return CheckResult(
                    "model", True, "verified immutable FULL_SNAPSHOT cache"
                )

            with patch(
                "dure.probe.NodeProbe.collect",
                return_value=enrolled["profile"],
            ), patch(
                "dure.agent.validate_strict_pipeline_node"
            ), patch(
                "dure.orchestrator.validate_strict_pipeline_node"
            ), patch(
                "dure.orchestrator.validate_strict_stage_cache",
                side_effect=validate_rank_cache,
            ), patch(
                "dure.orchestrator.ModelStore.ensure",
                side_effect=validate_full_cache,
            ), patch(
                "dure.orchestrator.ContainerRuntime.start_ray",
                return_value=successful["ray-container"],
            ), patch(
                "dure.orchestrator.ContainerRuntime.start_api",
                return_value=successful["vllm-api-start"],
            ), patch(
                "dure.orchestrator.ReadinessVerifier.host_gpu",
                return_value=successful["host-gpu"],
            ), patch(
                "dure.orchestrator.ReadinessVerifier.container_gpu",
                return_value=successful["container-gpu"],
            ), patch(
                "dure.orchestrator.ReadinessVerifier.wait_pipeline_rank_contract",
                return_value=contract,
            ), patch(
                "dure.orchestrator.ReadinessVerifier.wait_api",
                return_value=successful["vllm-api"],
            ):
                return executor.execute(task)

        if task["type"] == "VERIFY":
            with patch(
                "dure.probe.NodeProbe.collect",
                return_value=enrolled["profile"],
            ), patch(
                "dure.agent.validate_strict_pipeline_node"
            ), patch(
                "dure.agent.ReadinessVerifier.host_gpu",
                return_value=successful["host-gpu"],
            ), patch(
                "dure.agent.ReadinessVerifier.container_gpu",
                return_value=successful["container-gpu"],
            ), patch(
                "dure.agent.ReadinessVerifier.pipeline_rank_contract",
                return_value=contract,
            ), patch(
                "dure.agent.ReadinessVerifier.api",
                return_value=successful["vllm-api"],
            ):
                return executor.execute(task)

        if task["type"] == "STOP_DEPLOYMENT":
            with patch(
                "dure.agent.ContainerRuntime.stop_deployment",
                return_value=successful["deployment-stop"],
            ):
                return executor.execute(task)

        self.fail(f"unexpected deployment task type: {task['type']}")

    def _apply_and_verify_generation(
        self,
        *,
        deployment_id: str,
        node_ids: list[str],
        by_node: dict[str, dict],
        executors: dict[str, TaskExecutor],
        stores: dict[str, ContentAddressedModelStore],
        identities: dict[
            str, CacheIdentity | StageCacheIdentity
        ] | None,
    ) -> dict:
        apply_response = self.client.post(
            "/v1/admin/tasks",
            headers=self.admin,
            json={
                "node_ids": node_ids,
                "type": "APPLY_DEPLOYMENT",
                "deployment_id": deployment_id,
                "options": {"serve": True},
            },
        )
        self.assertEqual(apply_response.status_code, 200, apply_response.text)
        self.assertEqual(len(apply_response.json()["tasks"]), len(node_ids))
        effective_plan = copy.deepcopy(
            apply_response.json()["tasks"][0]["payload"]["plan"]
        )
        for node_id in node_ids:
            task = self._claim(by_node[node_id])
            self.assertEqual(task["type"], "APPLY_DEPLOYMENT")
            result = self._execute_deployment_task(
                task=task,
                enrolled=by_node[node_id],
                executor=executors[node_id],
                store=stores[node_id],
                identity=(identities or {}).get(node_id),
            )
            self._complete(by_node[node_id], task, result)

        ray_head_node_id = effective_plan["ray_head_node_id"]
        for expected_type in ("START_DEPLOYMENT", "VERIFY"):
            task = self._claim(by_node[ray_head_node_id])
            self.assertEqual(task["type"], expected_type)
            result = self._execute_deployment_task(
                task=task,
                enrolled=by_node[ray_head_node_id],
                executor=executors[ray_head_node_id],
                store=stores[ray_head_node_id],
                identity=(identities or {}).get(ray_head_node_id),
            )
            self._complete(by_node[ray_head_node_id], task, result)

        verify_response = self.client.post(
            "/v1/admin/tasks",
            headers=self.admin,
            json={
                "node_ids": node_ids,
                "type": "VERIFY",
                "deployment_id": deployment_id,
                "options": {"api": True},
            },
        )
        self.assertEqual(verify_response.status_code, 200, verify_response.text)
        self.assertEqual(len(verify_response.json()["tasks"]), len(node_ids))
        for node_id in node_ids:
            task = self._claim(by_node[node_id])
            self.assertEqual(task["type"], "VERIFY")
            result = self._execute_deployment_task(
                task=task,
                enrolled=by_node[node_id],
                executor=executors[node_id],
                store=stores[node_id],
                identity=(identities or {}).get(node_id),
            )
            self.assertTrue(result["ok"])
            self._complete(by_node[node_id], task, result)

        with self.factory() as session:
            deployment = session.get(Deployment, deployment_id)
            self.assertIsNotNone(deployment)
            self.assertEqual(deployment.status, "VERIFIED")
            self.assertIsNotNone(deployment.verified_at)
        return effective_plan

    def test_evidence_to_rank_download_ready_deploy_verify_and_offline_rollback(
        self,
    ) -> None:
        enrolled = self._enroll_nodes()
        target_release_id, variant, manifests, objects = (
            self._seed_qualified_stage_release(enrolled)
        )
        node_ids = sorted(item["node_id"] for item in enrolled)
        by_node = {item["node_id"]: item for item in enrolled}

        recommendation_response = self.client.post(
            "/v1/admin/deployment-recommendations",
            headers=self.admin,
            json={
                "node_ids": node_ids,
                "all_online": False,
                "objective": "quality-first",
            },
        )
        self.assertEqual(
            recommendation_response.status_code,
            200,
            recommendation_response.text,
        )
        recommendation = recommendation_response.json()["recommendation"]
        self.assertEqual(
            recommendation["selected"]["model_cache_kind"],
            MODEL_CACHE_KIND_STAGE,
        )
        self.assertEqual(
            recommendation["selected"]["stage_artifact"]["artifact_set_digest"],
            variant["artifact_set_digest"],
        )
        with self.factory() as session:
            self.assertEqual(session.scalar(select(func.count()).select_from(Task)), 0)

        accepted = self.client.post(
            f"/v1/admin/deployment-recommendations/{recommendation['id']}/accept",
            headers=self.admin,
            json={},
        )
        self.assertEqual(accepted.status_code, 200, accepted.text)
        target = accepted.json()["deployment"]
        with self.factory() as session:
            self.assertEqual(session.scalar(select(func.count()).select_from(Task)), 0)

        request_id = str(uuid.uuid4())
        prepare_endpoint = f"/v1/admin/deployments/{target['id']}/prepare"
        preview = self.client.post(
            prepare_endpoint,
            headers=self.admin,
            json={"request_id": request_id, "apply": False},
        )
        self.assertEqual(preview.status_code, 200, preview.text)
        self.assertEqual(preview.json()["tasks"], [])
        self.assertEqual(preview.json()["preparation"]["status"], "PREPARED")
        with self.factory() as session:
            self.assertEqual(session.scalar(select(func.count()).select_from(Task)), 0)

        applied_preparation = self.client.post(
            prepare_endpoint,
            headers=self.admin,
            json={"request_id": request_id, "apply": True},
        )
        self.assertEqual(
            applied_preparation.status_code,
            200,
            applied_preparation.text,
        )
        self.assertEqual(len(applied_preparation.json()["tasks"]), 3)
        self.assertEqual(
            {item["type"] for item in applied_preparation.json()["tasks"]},
            {"PREPARE_MODEL"},
        )

        executors: dict[str, TaskExecutor] = {}
        stores: dict[str, ContentAddressedModelStore] = {}
        transports: dict[str, MemoryTransport] = {}
        image_runners: dict[str, ImagePullRunner] = {}
        identities: dict[str, StageCacheIdentity] = {}
        downloaded_ranks: dict[str, int] = {}

        for node_id in node_ids:
            enrolled_node = by_node[node_id]
            node_root = self.root / f"node-{node_id}"
            node_root.mkdir(mode=0o700)
            store = ContentAddressedModelStore(
                store_root=node_root / "store",
                model_root=node_root / "models",
            )
            transport = MemoryTransport(objects)
            downloader = ArtifactChunkDownloader(
                store, transport=transport, attempts=1
            )
            preparer = ModelCachePreparer(
                store, downloader, disk_reserve_bytes=0
            )
            image_runner = ImagePullRunner(
                {RUNTIME_IMAGE, SOURCE_RUNTIME_IMAGE}
            )

            def load_manifest(task_id: str, *, item=enrolled_node) -> dict:
                response = self.client.get(
                    f"/v1/agent/tasks/{task_id}/artifact-manifest",
                    headers=item["headers"],
                )
                self.assertEqual(response.status_code, 200, response.text)
                return response.json()["manifest"]

            executor = TaskExecutor(
                node_id,
                runner=image_runner,
                state_path=node_root / "state.json",
                preparation_executor=ArtifactPreparationExecutor(
                    node_id,
                    runner=image_runner,
                    origin=ORIGIN,
                    model_preparer=preparer,
                    manifest_loader=load_manifest,
                ),
            )
            executors[node_id] = executor
            stores[node_id] = store
            transports[node_id] = transport
            image_runners[node_id] = image_runner

            model_task = self._claim(enrolled_node)
            self.assertEqual(model_task["type"], "PREPARE_MODEL")
            self.assertEqual(
                model_task["payload"]["cache_kind"], MODEL_CACHE_KIND_STAGE
            )
            self.assertIn(model_task["payload"]["manifest_digest"], manifests)
            identity = self._stage_identity(model_task)
            identities[node_id] = identity
            downloaded_ranks[node_id] = identity.pipeline_rank
            model_result = executor.execute(model_task)
            self.assertFalse(model_result["reused"])
            self._complete(enrolled_node, model_task, model_result)
            validation = validate_materialized_stage_cache(
                store.stage_cache_path(identity), identity
            )
            self.assertEqual(
                validation.cache_identity_digest, identity.cache_identity_digest
            )
            assigned_manifest = manifests[identity.manifest_digest]
            expected_chunk_digests = {
                chunk["sha256"]
                for file_item in assigned_manifest["files"]
                for chunk in file_item["chunks"]
            }
            requested_chunk_digests = {
                "sha256:" + object_url.rsplit("/", 1)[-1]
                for object_url, _headers in transport.calls
            }
            self.assertEqual(requested_chunk_digests, expected_chunk_digests)
            assigned_weight_digest = next(
                item["sha256"]
                for item in assigned_manifest["files"]
                if item["path"] == "model-rank-0-part-0.safetensors"
            )
            other_rank_weight_digests = {
                item["sha256"]
                for manifest in manifests.values()
                if manifest is not assigned_manifest
                for item in manifest["files"]
                if item["path"] == "model-rank-0-part-0.safetensors"
            }
            self.assertIn(assigned_weight_digest, requested_chunk_digests)
            self.assertTrue(
                requested_chunk_digests.isdisjoint(other_rank_weight_digests)
            )

            image_task = self._claim(enrolled_node)
            self.assertEqual(image_task["type"], "PREPARE_IMAGE")
            image_result = executor.execute(image_task)
            self.assertFalse(image_result["reused"])
            self._complete(enrolled_node, image_task, image_result)
            self.assertEqual(
                sum(
                    call[:3] == ("docker", "pull", "--quiet")
                    for call in image_runner.calls
                ),
                1,
            )

        self.assertEqual(set(downloaded_ranks.values()), {0, 1, 2})
        preparation_id = preview.json()["preparation"]["id"]
        shown = self.client.get(
            f"/v1/admin/deployment-preparations/{preparation_id}",
            headers=self.admin,
        )
        self.assertEqual(shown.status_code, 200, shown.text)
        self.assertEqual(shown.json()["preparation"]["status"], "SUCCEEDED")
        with self.factory() as session:
            caches = list(
                session.scalars(
                    select(NodeArtifactCache).order_by(NodeArtifactCache.node_id)
                )
            )
            self.assertEqual(len(caches), 3)
            self.assertEqual({item.status for item in caches}, {"READY"})
            self.assertEqual({item.pipeline_rank for item in caches}, {0, 1, 2})
            caches_by_node = {item.node_id: item for item in caches}
            self.assertEqual(set(caches_by_node), set(node_ids))
            for node_id in node_ids:
                self.assertEqual(
                    caches_by_node[node_id].cache_identity_digest,
                    identities[node_id].cache_identity_digest,
                )
                self.assertEqual(
                    caches_by_node[node_id].manifest_digest,
                    identities[node_id].manifest_digest,
                )

        effective_plan = self._apply_and_verify_generation(
            deployment_id=target["id"],
            node_ids=node_ids,
            by_node=by_node,
            executors=executors,
            stores=stores,
            identities=identities,
        )
        ray_head_node_id = effective_plan["ray_head_node_id"]
        with self.factory() as session:
            target_record = session.get(Deployment, target["id"])
            self.assertIsNotNone(target_record)
            self.assertEqual(target_record.status, "VERIFIED")
            self.assertIsNotNone(target_record.verified_at)
            transition_model_release(
                session, target_release_id, "DEPRECATED"
            )

        source_release_id, source_manifest_digest, source_objects = (
            self._seed_qualified_full_release(
                enrolled,
                key="rollback-source",
                suffix=b"-rollback-source",
                revision_character="d",
                quality_rank=200,
                runtime_image=SOURCE_RUNTIME_IMAGE,
            )
        )
        with self.factory() as session:
            task_count_before_source_recommendation = session.scalar(
                select(func.count()).select_from(Task)
            )
        source_recommendation_response = self.client.post(
            "/v1/admin/deployment-recommendations",
            headers=self.admin,
            json={
                "node_ids": node_ids,
                "all_online": False,
                "objective": "quality-first",
            },
        )
        self.assertEqual(
            source_recommendation_response.status_code,
            200,
            source_recommendation_response.text,
        )
        source_recommendation = source_recommendation_response.json()[
            "recommendation"
        ]
        source_selected = source_recommendation["selected"]
        self.assertEqual(
            source_selected["model_cache_kind"],
            MODEL_CACHE_KIND_FULL_SNAPSHOT,
        )
        self.assertEqual(source_selected["model_release_id"], source_release_id)
        self.assertEqual(
            source_selected["artifact_manifest_digest"],
            source_manifest_digest,
        )
        self.assertEqual(
            source_selected["runtime_image"], SOURCE_RUNTIME_IMAGE
        )
        self.assertNotEqual(
            source_selected["runtime_image"],
            recommendation["selected"]["runtime_image"],
        )
        self.assertNotEqual(
            source_selected["artifact_manifest_digest"],
            recommendation["selected"]["artifact_manifest_digest"],
        )
        source_accepted = self.client.post(
            f"/v1/admin/deployment-recommendations/"
            f"{source_recommendation['id']}/accept",
            headers=self.admin,
            json={"previous_generation_id": target["id"]},
        )
        self.assertEqual(
            source_accepted.status_code, 200, source_accepted.text
        )
        source = source_accepted.json()["deployment"]
        source_id = source["id"]
        self.assertEqual(source["lineage_id"], target["id"])
        self.assertEqual(source["previous_generation_id"], target["id"])
        self.assertEqual(source["generation"], 2)
        self.assertFalse(source["accept_model_download"])
        self.assertFalse(source["pull_image"])
        with self.factory() as session:
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Task)),
                task_count_before_source_recommendation,
            )
        source_plan = DeploymentPlan.from_dict(source["plan"])
        target_plan = DeploymentPlan.from_dict(effective_plan)
        self.assertEqual(source_plan.deployment_id, source_id)
        self.assertEqual(source_plan.generation, 2)
        self.assertEqual(
            source_plan.model_cache_kind, MODEL_CACHE_KIND_FULL_SNAPSHOT
        )
        self.assertIsNone(source_plan.stage_artifact)
        self.assertEqual(source_plan.image, SOURCE_RUNTIME_IMAGE)
        self.assertEqual(target_plan.deployment_id, target["id"])
        self.assertEqual(target_plan.generation, 1)
        self.assertEqual(target_plan.model_cache_kind, MODEL_CACHE_KIND_STAGE)
        self.assertIsNotNone(target_plan.stage_artifact)
        self.assertEqual(target_plan.image, RUNTIME_IMAGE)
        self.assertNotEqual(source_plan.model.model_id, target_plan.model.model_id)

        source_prepare_request_id = str(uuid.uuid4())
        source_prepare_endpoint = (
            f"/v1/admin/deployments/{source_id}/prepare"
        )
        source_preview = self.client.post(
            source_prepare_endpoint,
            headers=self.admin,
            json={
                "request_id": source_prepare_request_id,
                "apply": False,
            },
        )
        self.assertEqual(source_preview.status_code, 200, source_preview.text)
        self.assertEqual(source_preview.json()["tasks"], [])
        source_prepare_apply = self.client.post(
            source_prepare_endpoint,
            headers=self.admin,
            json={
                "request_id": source_prepare_request_id,
                "apply": True,
            },
        )
        self.assertEqual(
            source_prepare_apply.status_code,
            200,
            source_prepare_apply.text,
        )
        self.assertEqual(len(source_prepare_apply.json()["tasks"]), 3)
        self.assertEqual(
            {item["type"] for item in source_prepare_apply.json()["tasks"]},
            {"PREPARE_MODEL"},
        )

        source_identities: dict[str, CacheIdentity] = {}
        source_weight_digest = _digest(
            b"source-model-for-selection-rollback-source"
        )
        for node_id in node_ids:
            transports[node_id].objects.update(source_objects)
            call_offset = len(transports[node_id].calls)
            model_task = self._claim(by_node[node_id])
            self.assertEqual(model_task["type"], "PREPARE_MODEL")
            self.assertEqual(
                model_task["payload"]["cache_kind"],
                MODEL_CACHE_KIND_FULL_SNAPSHOT,
            )
            full_identity = self._full_identity(model_task)
            source_identities[node_id] = full_identity
            self.assertEqual(
                full_identity.manifest_digest, source_manifest_digest
            )
            model_result = executors[node_id].execute(model_task)
            self.assertFalse(model_result["reused"])
            self._complete(by_node[node_id], model_task, model_result)
            marker = read_model_cache_marker(
                stores[node_id].model_cache_path(source_manifest_digest)
                / MODEL_CACHE_MARKER_FILE
            )
            self.assertEqual(marker.manifest_digest, source_manifest_digest)
            source_requests = {
                "sha256:" + object_url.rsplit("/", 1)[-1]
                for object_url, _headers in transports[node_id].calls[
                    call_offset:
                ]
            }
            self.assertIn(source_weight_digest, source_requests)

            image_task = self._claim(by_node[node_id])
            self.assertEqual(image_task["type"], "PREPARE_IMAGE")
            self.assertEqual(
                image_task["payload"]["runtime_image"],
                SOURCE_RUNTIME_IMAGE,
            )
            image_result = executors[node_id].execute(image_task)
            self.assertFalse(image_result["reused"])
            self._complete(by_node[node_id], image_task, image_result)

        source_preparation_id = source_preview.json()["preparation"]["id"]
        source_preparation = self.client.get(
            "/v1/admin/deployment-preparations/"
            f"{source_preparation_id}",
            headers=self.admin,
        )
        self.assertEqual(
            source_preparation.status_code, 200, source_preparation.text
        )
        self.assertEqual(
            source_preparation.json()["preparation"]["status"],
            "SUCCEEDED",
        )
        with self.factory() as session:
            source_caches = list(
                session.scalars(
                    select(NodeArtifactCache).where(
                        NodeArtifactCache.manifest_digest
                        == source_manifest_digest
                    )
                )
            )
            self.assertEqual(len(source_caches), 3)
            self.assertEqual(
                {item.status for item in source_caches}, {"READY"}
            )
            self.assertEqual(
                {item.cache_kind for item in source_caches},
                {MODEL_CACHE_KIND_FULL_SNAPSHOT},
            )

        source_effective_plan = self._apply_and_verify_generation(
            deployment_id=source_id,
            node_ids=node_ids,
            by_node=by_node,
            executors=executors,
            stores=stores,
            identities=source_identities,
        )
        source_plan = DeploymentPlan.from_dict(source_effective_plan)
        self.assertEqual(source_plan.deployment_id, source_id)
        self.assertEqual(
            source_plan.model_cache_kind,
            MODEL_CACHE_KIND_FULL_SNAPSHOT,
        )
        self.assertEqual(source_plan.image, SOURCE_RUNTIME_IMAGE)
        with self.factory() as session:
            source_record = session.get(Deployment, source_id)
            self.assertIsNotNone(source_record)
            self.assertEqual(source_record.status, "VERIFIED")
            self.assertIsNotNone(source_record.verified_at)

        network_calls_before = {
            node_id: len(transports[node_id].calls) for node_id in node_ids
        }
        image_call_offsets = {
            node_id: len(image_runners[node_id].calls) for node_id in node_ids
        }
        image_pull_counts_before = {
            node_id: sum(
                call[:3] == ("docker", "pull", "--quiet")
                for call in image_runners[node_id].calls
            )
            for node_id in node_ids
        }
        rollback_endpoint = f"/v1/admin/deployments/{source_id}/rollback"
        rollback_preview = self.client.post(
            rollback_endpoint,
            headers=self.admin,
            json={"node_ids": node_ids, "apply": False, "serve": True},
        )
        self.assertEqual(
            rollback_preview.status_code, 200, rollback_preview.text
        )
        self.assertEqual(rollback_preview.json()["tasks"], [])
        rollback_apply = self.client.post(
            rollback_endpoint,
            headers=self.admin,
            json={"node_ids": node_ids, "apply": True, "serve": True},
        )
        self.assertEqual(rollback_apply.status_code, 200, rollback_apply.text)
        self.assertEqual(
            {item["type"] for item in rollback_apply.json()["tasks"]},
            {"STOP_DEPLOYMENT"},
        )

        for expected_type in (
            "STOP_DEPLOYMENT",
            "START_DEPLOYMENT",
            "VERIFY",
        ):
            for node_id in node_ids:
                task = self._claim(by_node[node_id])
                self.assertEqual(task["type"], expected_type)
                self.assertFalse(
                    task["payload"].get("accept_model_download", False)
                )
                self.assertFalse(task["payload"].get("pull_image", False))
                task_plan = DeploymentPlan.from_dict(task["payload"]["plan"])
                if expected_type == "STOP_DEPLOYMENT":
                    self.assertEqual(task_plan.deployment_id, source_id)
                    self.assertEqual(task_plan.generation, 2)
                    self.assertEqual(
                        task_plan.model_cache_kind,
                        MODEL_CACHE_KIND_FULL_SNAPSHOT,
                    )
                    self.assertEqual(
                        task_plan.model.model_id, source_plan.model.model_id
                    )
                    self.assertIsNone(task_plan.stage_artifact)
                    self.assertEqual(task_plan.image, SOURCE_RUNTIME_IMAGE)
                else:
                    self.assertEqual(task_plan.deployment_id, target["id"])
                    self.assertEqual(task_plan.generation, 1)
                    self.assertEqual(
                        task_plan.model_cache_kind, MODEL_CACHE_KIND_STAGE
                    )
                    self.assertEqual(task_plan.image, RUNTIME_IMAGE)
                    observed_identity = self._stage_identity_from_plan(
                        task_plan, node_id
                    )
                    self.assertEqual(
                        observed_identity.cache_identity_digest,
                        identities[node_id].cache_identity_digest,
                    )
                    self.assertEqual(observed_identity, identities[node_id])
                result = self._execute_deployment_task(
                    task=task,
                    enrolled=by_node[node_id],
                    executor=executors[node_id],
                    store=stores[node_id],
                    identity=(
                        source_identities[node_id]
                        if expected_type == "STOP_DEPLOYMENT"
                        else identities[node_id]
                    ),
                )
                self._complete(by_node[node_id], task, result)

        for expected_type in ("START_DEPLOYMENT", "VERIFY"):
            task = self._claim(by_node[ray_head_node_id])
            self.assertEqual(task["type"], expected_type)
            self.assertFalse(
                task["payload"].get("accept_model_download", False)
            )
            self.assertFalse(task["payload"].get("pull_image", False))
            task_plan = DeploymentPlan.from_dict(task["payload"]["plan"])
            self.assertEqual(task_plan.deployment_id, target["id"])
            self.assertEqual(task_plan.generation, 1)
            self.assertEqual(
                task_plan.model_cache_kind, MODEL_CACHE_KIND_STAGE
            )
            self.assertEqual(task_plan.image, RUNTIME_IMAGE)
            observed_identity = self._stage_identity_from_plan(
                task_plan, ray_head_node_id
            )
            self.assertEqual(
                observed_identity.cache_identity_digest,
                identities[ray_head_node_id].cache_identity_digest,
            )
            self.assertEqual(
                observed_identity, identities[ray_head_node_id]
            )
            result = self._execute_deployment_task(
                task=task,
                enrolled=by_node[ray_head_node_id],
                executor=executors[ray_head_node_id],
                store=stores[ray_head_node_id],
                identity=identities[ray_head_node_id],
            )
            self._complete(by_node[ray_head_node_id], task, result)

        with self.factory() as session:
            source = session.get(Deployment, source_id)
            target_record = session.get(Deployment, target["id"])
            operation = session.scalar(
                select(DeploymentOperation).where(
                    DeploymentOperation.deployment_id == source_id,
                    DeploymentOperation.kind == "ROLLBACK",
                )
            )
            self.assertIsNotNone(operation)
            self.assertEqual(operation.status, "SUCCEEDED")
            self.assertEqual(operation.phase, "COMPLETE")
            self.assertEqual(source.status, "ROLLED_BACK")
            self.assertEqual(target_record.status, "VERIFIED")
            self.assertIsNotNone(target_record.verified_at)
            self.assertEqual(
                session.scalar(
                    select(func.count())
                    .select_from(Task)
                    .where(
                        Task.bulk_id == operation.id,
                        Task.type.in_(["PREPARE_MODEL", "PREPARE_IMAGE"]),
                    )
                ),
                0,
            )

        self.assertEqual(
            {node_id: len(transports[node_id].calls) for node_id in node_ids},
            network_calls_before,
        )
        self.assertEqual(
            {
                node_id: sum(
                    call[:3] == ("docker", "pull", "--quiet")
                    for call in image_runners[node_id].calls
                )
                for node_id in node_ids
            },
            image_pull_counts_before,
        )
        for node_id in node_ids:
            rollback_image_calls = image_runners[node_id].calls[
                image_call_offsets[node_id] :
            ]
            self.assertEqual(
                len(rollback_image_calls),
                2 if node_id == ray_head_node_id else 1,
            )
            self.assertTrue(
                all(
                    call[:3] == ("docker", "image", "inspect")
                    and call[-1] == RUNTIME_IMAGE
                    for call in rollback_image_calls
                ),
                rollback_image_calls,
            )


if __name__ == "__main__":
    unittest.main()

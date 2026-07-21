from __future__ import annotations

import copy
import hashlib
import tempfile
import unittest
import uuid
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlalchemy import func, select

from dure.control.api import create_app
from dure.control import preparation as preparation_module
from dure.control.benchmark import (
    BENCHMARK_POLICY_VERSION,
    BENCHMARK_SUITE_ID,
    benchmark_inventory_fingerprint,
    promote_model_release,
    register_benchmark_evidence,
)
from dure.control.models import (
    ArtifactManifest,
    ArtifactPreparation,
    ArtifactPreparationAttempt,
    ArtifactPreparationNode,
    Deployment,
    DeploymentOperation,
    Node,
    NodeArtifactCache,
    NodeProfileRecord,
    Task,
    utcnow,
)
from dure.control.preparation import (
    _preparation_stage,
    effective_deployment_plan,
)
from dure.control.rollout import (
    DeploymentRolloutError,
    prepare_or_apply_rollback,
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
from dure.model_cache import (
    MODEL_CACHE_KIND_FULL_SNAPSHOT,
    MODEL_CACHE_KIND_STAGE,
    MODEL_CACHE_VERIFICATION_VERSION,
)
from dure.stage_cache import (
    STAGE_CACHE_VERIFICATION_VERSION,
    StageCacheIdentity,
)

from .helpers import profile
from .test_artifact_manifest_api import _manifest


def _digest(character: str) -> str:
    return "sha256:" + character * 64


def _named_digest(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _stage_manifest(rank: int, *, changed: bool = False) -> dict:
    digests = (
        tuple(
            _named_digest(f"changed-stage-{rank}-{index}")
            for index in range(5)
        )
        if changed
        else (
            tuple(_digest(item) for item in ("0", "1", "2", "3", "4"))
            if rank == 0
            else tuple(
                _digest(item) for item in ("5", "6", "7", "8", "9")
            )
        )
    )
    files = [
        ("model-rank-0-part-0.safetensors", 4 + rank, digests[0]),
        ("config.json", 2, digests[1]),
        ("tokenizer.json", 3, digests[2]),
        ("tokenizer_config.json", 4, digests[3]),
        ("dure-stage.json", 5, digests[4]),
    ]
    return {
        "schema_version": 1,
        "files": [
            {
                "path": path,
                "kind": "REGULAR",
                "size_bytes": size_bytes,
                "sha256": digest,
                "chunks": [
                    {
                        "ordinal": 0,
                        "offset_bytes": 0,
                        "length_bytes": size_bytes,
                        "sha256": digest,
                    }
                ],
            }
            for path, size_bytes, digest in files
        ],
    }


def _oversized_manifest(
    size_bytes: int, *, chunk_character: str = "a"
) -> dict:
    return {
        "schema_version": 1,
        "files": [
            {
                "path": "weights/model.safetensors",
                "kind": "REGULAR",
                "size_bytes": size_bytes,
                "sha256": "sha256:" + "d" * 64,
                "chunks": [
                    {
                        "ordinal": 0,
                        "offset_bytes": 0,
                        "length_bytes": size_bytes,
                        "sha256": "sha256:" + chunk_character * 64,
                    }
                ],
            }
        ],
    }


class ArtifactPreparationControlTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        database_url = (
            f"sqlite:///{Path(self.temporary.name) / 'artifact-preparation.db'}"
        )
        self.client = TestClient(
            create_app(
                database_url=database_url,
                admin_token="admin-secret",
                create_schema=True,
            )
        )
        self.factory = self.client.app.state.session_factory
        self.admin = {"Authorization": "Bearer admin-secret"}

    def tearDown(self):
        self.client.close()
        self.temporary.cleanup()

    def _enroll_nodes(
        self,
        count: int,
        key: str,
        *,
        disk_free_mib: int = 80000,
        agent_version: str = "0.3.20",
    ) -> list[dict]:
        enrolled = []
        for index in range(count):
            enrollment = self.client.post(
                "/v1/admin/enrollments",
                headers=self.admin,
                json={},
            )
            self.assertEqual(enrollment.status_code, 200, enrollment.text)
            reported_profile = profile(
                f"{key}-{index}",
                address=f"10.20.{index}.10",
            ).to_dict()
            reported_profile["disk_free_mib"] = disk_free_mib
            claimed = self.client.post(
                "/v1/enrollments/claim",
                json={
                    "token": enrollment.json()["token"],
                    "install_id": f"install-{key}-{index}-{uuid.uuid4()}",
                    "agent_version": agent_version,
                    "profile": reported_profile,
                },
            )
            self.assertEqual(claimed.status_code, 200, claimed.text)
            enrolled.append(
                {
                    "node_id": claimed.json()["node_id"],
                    "headers": {
                        "Authorization": (
                            f"Bearer {claimed.json()['credential']}"
                        )
                    },
                }
            )
        return enrolled

    def _seed_accepted_generation(
        self,
        key: str,
        *,
        node_count: int = 1,
        manifest: dict | None = None,
        register_manifest: bool = True,
        disk_free_mib: int = 80000,
        agent_version: str = "0.3.20",
        stage_variant_status: str | None = None,
        expect_selection: bool = True,
    ) -> dict:
        enrolled = self._enroll_nodes(
            node_count,
            key,
            disk_free_mib=disk_free_mib,
            agent_version=agent_version,
        )
        manifest_value = copy.deepcopy(manifest or _manifest())
        manifest_digest = canonical_artifact_manifest_digest(manifest_value)
        with self.factory() as session:
            nodes = [session.get(Node, item["node_id"]) for item in enrolled]
            self.assertTrue(all(node is not None for node in nodes))
            artifact = create_model_artifact(
                session,
                model_id=f"prepare-{key}",
                repository=f"Example/Prepare-{key}",
                revision=(key.encode("utf-8").hex() + "1" * 40)[:40],
                manifest_digest=manifest_digest,
                quantization="awq",
                size_mib=1,
                default_max_model_len=1024,
                layer_count=32,
                license_id="apache-2.0",
            )
            # The current selector deliberately excludes an artifact without a
            # registered canonical manifest. The missing-manifest boundary is
            # therefore created only after recommendation acceptance below.
            register_artifact_manifest(
                session,
                artifact_id=artifact.id,
                manifest=manifest_value,
            )
            runtime = create_runtime_release(
                session,
                version=f"runtime-{key}",
                image=(
                    f"registry.example/{key}@sha256:"
                    + canonical_artifact_manifest_digest(
                        {
                            "schema_version": 1,
                            "files": manifest_value["files"],
                        }
                    ).removeprefix("sha256:")
                ),
                vllm_version="0.9.0",
                cuda_version="12.8",
                gpu_architectures=["ampere"],
            )
            release = create_model_release(
                session,
                artifact_id=artifact.id,
                runtime_id=runtime.id,
                quality_rank=10,
            )
            multi_node = node_count > 1
            placement = add_placement_profile(
                session,
                release_id=release.id,
                profile_id=(
                    f"pipeline-{node_count}x24g"
                    if multi_node
                    else "single-24g"
                ),
                topology="pipeline" if multi_node else "single-gpu",
                node_count=node_count,
                min_gpu_memory_mib=8192,
                min_disk_free_mib=1,
                pipeline_parallel_size=node_count,
                tensor_parallel_size=1,
                requires_network_evidence=multi_node,
                requires_nccl=multi_node,
                min_bandwidth_mbps=10000 if multi_node else None,
                max_rtt_ms=5.0 if multi_node else None,
                max_packet_loss_pct=0.1 if multi_node else None,
                max_ttft_p95_ms=1000.0,
                max_tpot_p95_ms=100.0,
                max_e2e_p95_ms=5000.0,
                min_success_rate=0.99,
                min_vram_headroom_pct=10.0,
                min_throughput_tps=10.0,
            )
            transition_model_release(session, release.id, "VALIDATED")
            node_ids = sorted(item["node_id"] for item in enrolled)
            evidence = register_benchmark_evidence(
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
                dure_commit="e" * 40,
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
                quality_score=0.90,
                network_bandwidth_mbps=20000.0 if multi_node else None,
                network_rtt_ms=1.0 if multi_node else None,
                packet_loss_pct=0.0 if multi_node else None,
                nccl_all_reduce_ok=True if multi_node else None,
            )
            evidence.created_at = max(node.last_seen for node in nodes)
            session.commit()
            promoted, _, changed = promote_model_release(session, release.id)
            self.assertTrue(changed)
            self.assertEqual(promoted.status, "ACTIVE")
            artifact_id = artifact.id
            release_id = release.id
            runtime_image = runtime.image

        context = {
            "artifact_id": artifact_id,
            "release_id": release_id,
            "enrolled": enrolled,
            "manifest": manifest_value,
            "manifest_digest": manifest_digest,
            "runtime_image": runtime_image,
        }
        if stage_variant_status is not None:
            if node_count != 2 or stage_variant_status not in {
                "DRAFT",
                "VALIDATED",
                "REVOKED",
            }:
                raise AssertionError("stage seed requires two nodes and a known status")
            stage = self._create_stage_variant(
                context, validate=stage_variant_status != "DRAFT"
            )
            context["stage_variant"] = stage["variant"]
            if stage_variant_status == "REVOKED":
                endpoint = (
                    "/v1/admin/stage-artifact-variants/"
                    + stage["variant"]["artifact_set_digest"]
                    + "/transition"
                )
                revoked = self.client.post(
                    endpoint,
                    headers=self.admin,
                    json={"status": "REVOKED"},
                )
                self.assertEqual(revoked.status_code, 200, revoked.text)
                context["stage_variant"] = revoked.json()["variant"]

        recommendation_response = self.client.post(
            "/v1/admin/deployment-recommendations",
            headers=self.admin,
            json={
                "node_ids": sorted(item["node_id"] for item in enrolled),
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
        if not expect_selection:
            self.assertIsNone(recommendation["selected"])
            context["recommendation"] = recommendation
            return context
        self.assertIsNotNone(recommendation["selected"], recommendation)
        accepted_response = self.client.post(
            f"/v1/admin/deployment-recommendations/{recommendation['id']}/accept",
            headers=self.admin,
            json={},
        )
        self.assertEqual(
            accepted_response.status_code,
            200,
            accepted_response.text,
        )
        if not register_manifest:
            with self.factory() as session:
                record = session.get(ArtifactManifest, manifest_digest)
                self.assertIsNotNone(record)
                session.delete(record)
                session.commit()
        context["recommendation"] = recommendation
        context["deployment"] = accepted_response.json()["deployment"]
        return context

    def _create_stage_variant(
        self,
        context: dict,
        *,
        validate: bool = True,
        changed_rank: int | None = None,
        exporter_character: str = "6",
    ) -> dict:
        manifests: dict[str, dict] = {}
        stages = []
        for rank in range(2):
            manifest = _stage_manifest(
                rank, changed=rank == changed_rank
            )
            manifest_digest = canonical_artifact_manifest_digest(manifest)
            manifests[manifest_digest] = manifest
            stages.append(
                {
                    "pipeline_rank": rank,
                    "tensor_rank": 0,
                    "manifest_digest": manifest_digest,
                    "tensor_key_count": 2 + rank,
                    "tensor_keys_digest": _digest(str(4 + rank)),
                    "weight_size_bytes": 4 + rank,
                    "manifest": manifest,
                }
            )
        response = self.client.post(
            "/v1/admin/stage-artifact-variants",
            headers=self.admin,
            json={
                "source_manifest_digest": context["manifest_digest"],
                "runtime_image": context["runtime_image"],
                "vllm_version": "0.9.0",
                "exporter_build_digest": _digest(exporter_character),
                "architecture": "Qwen2ForCausalLM",
                "quantization": "awq",
                "tensor_parallel_size": 1,
                "pipeline_parallel_size": 2,
                "loader_format": "VLLM_SHARDED_STATE_V1",
                "stages": stages,
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        stage = {
            "variant": response.json()["variant"],
            "manifests": manifests,
        }
        if validate:
            self._validate_stage_variant(stage)
        context["stage_variant"] = stage["variant"]
        context["stage_manifests"] = manifests
        return stage

    def _validate_stage_variant(self, stage: dict) -> None:
        variant = stage["variant"]
        endpoint = (
            "/v1/admin/stage-artifact-variants/"
            + variant["artifact_set_digest"]
        )
        evidence = self.client.post(
            endpoint + "/evidence",
            headers=self.admin,
            json={
                "schema_version": 1,
                "variant_identity_digest": variant[
                    "artifact_set_digest"
                ],
                "validation_run_id": str(uuid.uuid4()),
                "kind": "GPU_EXPORT_LOAD",
                "status": "PASSED",
                "validator_version": "validator-1",
                "validator_build_digest": _digest("7"),
                "failure_code": None,
                "ranks": [
                    {
                        "pipeline_rank": item["pipeline_rank"],
                        "tensor_rank": item["tensor_rank"],
                        "manifest_digest": item["manifest_digest"],
                        "tensor_keys_digest": item[
                            "tensor_keys_digest"
                        ],
                        "loaded_tensor_count": item[
                            "tensor_key_count"
                        ],
                        "loaded_weight_size_bytes": item[
                            "weight_size_bytes"
                        ],
                    }
                    for item in variant["stages"]
                ],
            },
        )
        self.assertEqual(evidence.status_code, 200, evidence.text)
        transition = self.client.post(
            endpoint + "/transition",
            headers=self.admin,
            json={"status": "VALIDATED"},
        )
        self.assertEqual(transition.status_code, 200, transition.text)
        stage["variant"] = transition.json()["variant"]

    def _prepare(
        self,
        context: dict,
        request_id: str,
        *,
        apply: bool,
        artifact_set_digest: str | None = None,
    ):
        body = {"request_id": request_id, "apply": apply}
        if artifact_set_digest is not None:
            body["artifact_set_digest"] = artifact_set_digest
        return self.client.post(
            f"/v1/admin/deployments/{context['deployment']['id']}/prepare",
            headers=self.admin,
            json=body,
        )

    def _claim(self, enrolled: dict) -> dict:
        response = self.client.post(
            "/v1/agent/tasks/claim",
            headers=enrolled["headers"],
        )
        self.assertEqual(response.status_code, 200, response.text)
        task = response.json()["task"]
        self.assertIsNotNone(task, response.text)
        return task

    def _result(self, task: dict, context: dict) -> dict:
        payload = task["payload"]
        result = {
            "preparation_id": payload["preparation_id"],
            "preparation_node_id": payload["preparation_node_id"],
            "attempt_id": payload["attempt_id"],
            "attempt_no": payload["attempt_no"],
            "deployment_id": payload["deployment_id"],
            "generation": payload["generation"],
            "node_id": payload["node_id"],
            "stage": "MODEL" if task["type"] == "PREPARE_MODEL" else "IMAGE",
            "reused": False,
        }
        if task["type"] == "PREPARE_MODEL":
            if payload["cache_kind"] == MODEL_CACHE_KIND_STAGE:
                manifest = context["stage_manifests"][
                    payload["manifest_digest"]
                ]
                identity = StageCacheIdentity(
                    repository=payload["repository"],
                    revision=payload["revision"],
                    manifest_digest=payload["manifest_digest"],
                    quantization=payload["quantization"],
                    artifact_set_digest=payload["artifact_set_digest"],
                    contract_identity_digest=payload[
                        "contract_identity_digest"
                    ],
                    source_manifest_digest=payload[
                        "source_manifest_digest"
                    ],
                    runtime_image=payload["runtime_image"],
                    vllm_version=payload["vllm_version"],
                    exporter_build_digest=payload[
                        "exporter_build_digest"
                    ],
                    architecture=payload["architecture"],
                    loader_format=payload["loader_format"],
                    tensor_parallel_size=payload[
                        "tensor_parallel_size"
                    ],
                    pipeline_parallel_size=payload[
                        "pipeline_parallel_size"
                    ],
                    pipeline_rank=payload["pipeline_rank"],
                    tensor_rank=payload["tensor_rank"],
                    tensor_keys_digest=payload["tensor_keys_digest"],
                )
                result.update(
                    model_id=payload["model_id"],
                    manifest_digest=payload["manifest_digest"],
                    cache_kind=MODEL_CACHE_KIND_STAGE,
                    verification_version=STAGE_CACHE_VERIFICATION_VERSION,
                    bytes_verified=sum(
                        item["size_bytes"] for item in manifest["files"]
                    ),
                    file_count=len(manifest["files"]),
                    artifact_set_digest=payload["artifact_set_digest"],
                    pipeline_rank=payload["pipeline_rank"],
                    tensor_rank=payload["tensor_rank"],
                    tensor_keys_digest=payload["tensor_keys_digest"],
                    cache_identity_digest=identity.cache_identity_digest,
                )
            else:
                result.update(
                    model_id=payload["model_id"],
                    manifest_digest=payload["manifest_digest"],
                    cache_kind=MODEL_CACHE_KIND_FULL_SNAPSHOT,
                    verification_version=MODEL_CACHE_VERIFICATION_VERSION,
                    bytes_verified=sum(
                        item["size_bytes"]
                        for item in context["manifest"]["files"]
                    ),
                    file_count=len(context["manifest"]["files"]),
                )
        else:
            result.update(
                runtime_image=payload["runtime_image"],
                image_id=payload["runtime_image"].rsplit("@", 1)[1],
            )
        return result

    def _complete(self, task: dict, enrolled: dict, context: dict):
        response = self.client.post(
            f"/v1/agent/tasks/{task['id']}/complete",
            headers=enrolled["headers"],
            json={"result": self._result(task, context)},
        )
        self.assertEqual(response.status_code, 200, response.text)
        return response

    def _failure_code(self, response, expected: str) -> None:
        self.assertEqual(response.status_code, 409, response.text)
        self.assertEqual(response.json()["detail"]["code"], expected)

    def test_full_api_flow_gates_recommended_apply_on_exact_preparation(self):
        context = self._seed_accepted_generation("success")
        with self.factory() as session:
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Task)), 0
            )

        request_id = str(uuid.uuid4())
        preview = self._prepare(context, request_id, apply=False)
        self.assertEqual(preview.status_code, 200, preview.text)
        self.assertEqual(preview.json()["preparation"]["status"], "PREPARED")
        self.assertEqual(preview.json()["tasks"], [])
        expected_bytes = sum(
            item["size_bytes"] for item in context["manifest"]["files"]
        )
        download_expected_bytes = sum(
            {
                chunk["sha256"]: chunk["length_bytes"]
                for item in context["manifest"]["files"]
                for chunk in item["chunks"]
            }.values()
        )
        preview_progress = preview.json()["preparation"]["progress"]
        self.assertEqual(
            preview_progress,
            {
                "expected_bytes": expected_bytes,
                "verified_bytes": 0,
                "bytes_source": "COMPLETED_MODEL_VERIFICATION",
                "downloaded_bytes": 0,
                "download_expected_bytes": download_expected_bytes,
                "download_bytes_source": "NOT_STARTED",
                "stage": "MODEL",
                "retrying": False,
                "model_retry_count": 0,
                "image_retry_count": 0,
            },
        )
        preview_node_progress = preview.json()["preparation"]["nodes"][0][
            "progress"
        ]
        self.assertEqual(preview_node_progress["expected_bytes"], expected_bytes)
        self.assertEqual(preview_node_progress["verified_bytes"], 0)
        self.assertEqual(preview_node_progress["stage"], "MODEL")
        self.assertEqual(
            preview_node_progress["model"],
            {
                "status": "PREPARED",
                "current_attempt": 0,
                "retry_count": 0,
                "failure_code": None,
            },
        )
        preparation_id = preview.json()["preparation"]["id"]
        with self.factory() as session:
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Task)), 0
            )

        outsider = self._enroll_nodes(1, "manifest-outsider")[0]
        for task_type in ("PREPARE_MODEL", "PREPARE_IMAGE"):
            rejected = self.client.post(
                "/v1/admin/tasks",
                headers=self.admin,
                json={
                    "node_ids": [context["enrolled"][0]["node_id"]],
                    "type": task_type,
                    "deployment_id": context["deployment"]["id"],
                    "options": {},
                },
            )
            self.assertEqual(rejected.status_code, 400, rejected.text)
            self.assertIn("dedicated deployment prepare API", rejected.text)

        applied = self._prepare(context, request_id, apply=True)
        self.assertEqual(applied.status_code, 200, applied.text)
        self.assertEqual(
            [task["type"] for task in applied.json()["tasks"]],
            ["PREPARE_MODEL"],
        )
        model_task = self._claim(context["enrolled"][0])
        self.assertEqual(model_task["type"], "PREPARE_MODEL")
        self.assertEqual(
            model_task["payload"]["cache_kind"],
            MODEL_CACHE_KIND_FULL_SNAPSHOT,
        )
        self.assertNotIn("artifact_set_digest", model_task["payload"])

        live = self.client.post(
            f"/v1/agent/tasks/{model_task['id']}/heartbeat",
            headers=context["enrolled"][0]["headers"],
            json={"progress": {"downloaded_bytes": 3}},
        )
        self.assertEqual(live.status_code, 200, live.text)
        shown_live = self.client.get(
            f"/v1/admin/deployment-preparations/{preparation_id}",
            headers=self.admin,
        ).json()["preparation"]
        self.assertEqual(shown_live["progress"]["downloaded_bytes"], 3)
        self.assertEqual(
            shown_live["progress"]["download_bytes_source"],
            "MODEL_PREPARATION_HIGH_WATER",
        )
        self.assertEqual(
            shown_live["nodes"][0]["progress"]["downloaded_bytes"], 3
        )

        decreased = self.client.post(
            f"/v1/agent/tasks/{model_task['id']}/heartbeat",
            headers=context["enrolled"][0]["headers"],
            json={"progress": {"downloaded_bytes": 1}},
        )
        self.assertEqual(decreased.status_code, 200, decreased.text)
        shown_after_decrease = self.client.get(
            f"/v1/admin/deployment-preparations/{preparation_id}",
            headers=self.admin,
        ).json()["preparation"]
        self.assertEqual(
            shown_after_decrease["progress"]["downloaded_bytes"], 3
        )

        for invalid_progress in (
            {"downloaded_bytes": True},
            {"downloaded_bytes": 1, "raw_url": "https://secret.invalid"},
        ):
            rejected_progress = self.client.post(
                f"/v1/agent/tasks/{model_task['id']}/heartbeat",
                headers=context["enrolled"][0]["headers"],
                json={"progress": invalid_progress},
            )
            self.assertEqual(
                rejected_progress.status_code,
                422,
                rejected_progress.text,
            )
        oversized_progress = self.client.post(
            f"/v1/agent/tasks/{model_task['id']}/heartbeat",
            headers=context["enrolled"][0]["headers"],
            json={"progress": {"downloaded_bytes": expected_bytes + 1}},
        )
        self.assertEqual(
            oversized_progress.status_code, 409, oversized_progress.text
        )

        manifest = self.client.get(
            f"/v1/agent/tasks/{model_task['id']}/artifact-manifest",
            headers=context["enrolled"][0]["headers"],
        )
        self.assertEqual(manifest.status_code, 200, manifest.text)
        expected_manifest = copy.deepcopy(context["manifest"])
        expected_manifest["files"] = sorted(
            expected_manifest["files"], key=lambda item: item["path"]
        )
        for item in expected_manifest["files"]:
            item["chunks"] = sorted(
                item["chunks"], key=lambda chunk: chunk["ordinal"]
            )
        self.assertEqual(manifest.json()["manifest"], expected_manifest)
        other_node = self.client.get(
            f"/v1/agent/tasks/{model_task['id']}/artifact-manifest",
            headers=outsider["headers"],
        )
        self._failure_code(other_node, "PREPARATION_MANIFEST_UNAVAILABLE")

        self._complete(model_task, context["enrolled"][0], context)
        # A committed completion whose HTTP response was lost is replay-safe.
        self._complete(model_task, context["enrolled"][0], context)
        model_shown = self.client.get(
            f"/v1/admin/deployment-preparations/{preparation_id}",
            headers=self.admin,
        )
        self.assertEqual(model_shown.status_code, 200, model_shown.text)
        model_progress = model_shown.json()["preparation"]["progress"]
        self.assertEqual(model_progress["verified_bytes"], expected_bytes)
        self.assertEqual(
            model_progress["downloaded_bytes"], download_expected_bytes
        )
        self.assertEqual(
            model_progress["download_bytes_source"],
            "MODEL_PREPARATION_HIGH_WATER",
        )
        self.assertEqual(model_progress["stage"], "IMAGE")
        model_node_progress = model_shown.json()["preparation"]["nodes"][0][
            "progress"
        ]
        self.assertEqual(model_node_progress["model"]["status"], "SUCCEEDED")
        self.assertEqual(model_node_progress["model"]["current_attempt"], 1)
        self.assertEqual(model_node_progress["image"]["status"], "QUEUED")
        self.assertEqual(model_node_progress["image"]["current_attempt"], 1)
        image_task = self._claim(context["enrolled"][0])
        self.assertEqual(image_task["type"], "PREPARE_IMAGE")
        self._complete(image_task, context["enrolled"][0], context)

        shown = self.client.get(
            f"/v1/admin/deployment-preparations/{preparation_id}",
            headers=self.admin,
        )
        self.assertEqual(shown.status_code, 200, shown.text)
        self.assertEqual(shown.json()["preparation"]["status"], "SUCCEEDED")
        completed_progress = shown.json()["preparation"]["progress"]
        self.assertEqual(completed_progress["expected_bytes"], expected_bytes)
        self.assertEqual(completed_progress["verified_bytes"], expected_bytes)
        self.assertEqual(completed_progress["stage"], "COMPLETE")
        self.assertFalse(completed_progress["retrying"])

        deployment_apply = self.client.post(
            "/v1/admin/tasks",
            headers=self.admin,
            json={
                "node_ids": [context["enrolled"][0]["node_id"]],
                "type": "APPLY_DEPLOYMENT",
                "deployment_id": context["deployment"]["id"],
                "options": {"serve": False},
            },
        )
        self.assertEqual(deployment_apply.status_code, 200, deployment_apply.text)
        apply_task = deployment_apply.json()["tasks"][0]
        expected_path = (
            "/var/lib/dure/models/sha256-"
            + context["manifest_digest"].removeprefix("sha256:")
        )
        self.assertEqual(apply_task["payload"]["plan"]["model_path"], expected_path)
        self.assertNotIn(
            "stage_artifact", apply_task["payload"]["plan"]
        )
        self.assertEqual(
            apply_task["payload"]["plan"]["model_cache_kind"],
            MODEL_CACHE_KIND_FULL_SNAPSHOT,
        )
        self.assertFalse(apply_task["payload"]["accept_model_download"])
        self.assertFalse(apply_task["payload"]["pull_image"])

    def test_download_progress_projection_bounds_and_legacy_derivation(self):
        context = self._seed_accepted_generation("progress")
        request_id = str(uuid.uuid4())
        preview = self._prepare(context, request_id, apply=False)
        self.assertEqual(preview.status_code, 200, preview.text)
        preparation_id = preview.json()["preparation"]["id"]
        applied = self._prepare(context, request_id, apply=True)
        self.assertEqual(applied.status_code, 200, applied.text)
        task = self._claim(context["enrolled"][0])
        with self.factory() as session:
            attempt = session.scalar(
                select(ArtifactPreparationAttempt).where(
                    ArtifactPreparationAttempt.task_id == task["id"]
                )
            )
            self.assertIsNotNone(attempt)
            expected_download_bytes = attempt.download_progress[
                "expected_bytes"
            ]
            attempt.download_progress = {
                "downloaded_bytes": -1,
                "expected_bytes": expected_download_bytes,
            }
            session.commit()

        malformed = self.client.get(
            f"/v1/admin/deployment-preparations/{preparation_id}",
            headers=self.admin,
        )
        self.assertEqual(malformed.status_code, 200, malformed.text)
        malformed_progress = malformed.json()["preparation"]["progress"]
        self.assertIsNone(malformed_progress["downloaded_bytes"])
        self.assertEqual(
            malformed_progress["download_bytes_source"], "UNAVAILABLE"
        )
        malformed_node = malformed.json()["preparation"]["nodes"][0][
            "progress"
        ]
        self.assertIsNone(malformed_node["downloaded_bytes"])
        self.assertEqual(
            malformed_node["download_bytes_source"], "UNAVAILABLE"
        )

        self._complete(task, context["enrolled"][0], context)
        with self.factory() as session:
            attempt = session.scalar(
                select(ArtifactPreparationAttempt).where(
                    ArtifactPreparationAttempt.task_id == task["id"]
                )
            )
            attempt.download_progress = None
            session.commit()

        legacy = self.client.get(
            f"/v1/admin/deployment-preparations/{preparation_id}",
            headers=self.admin,
        )
        self.assertEqual(legacy.status_code, 200, legacy.text)
        legacy_progress = legacy.json()["preparation"]["progress"]
        self.assertEqual(
            legacy_progress["downloaded_bytes"], expected_download_bytes
        )
        self.assertEqual(
            legacy_progress["download_bytes_source"],
            "DERIVED_FROM_COMPLETED_MODEL_VERIFICATION",
        )

    def test_download_progress_manifest_bytes_are_aggregated_once(self):
        shared_context = self._seed_accepted_generation(
            "progress-query", node_count=3
        )
        with patch(
            "dure.control.preparation._manifest_download_bytes",
            wraps=preparation_module._manifest_download_bytes,
        ) as aggregate:
            shared_preview = self._prepare(
                shared_context, str(uuid.uuid4()), apply=False
            )
        self.assertEqual(
            shared_preview.status_code, 200, shared_preview.text
        )
        self.assertEqual(aggregate.call_count, 1)

    def test_latest_image_and_ready_cache_gate_all_deployment_consumers(self):
        context = self._seed_accepted_generation("cache-gates")
        prepared = self._prepare(
            context,
            str(uuid.uuid4()),
            apply=True,
        )
        self.assertEqual(prepared.status_code, 200, prepared.text)
        model_task = self._claim(context["enrolled"][0])
        self._complete(model_task, context["enrolled"][0], context)
        image_task = self._claim(context["enrolled"][0])
        self._complete(image_task, context["enrolled"][0], context)

        node_id = context["enrolled"][0]["node_id"]
        with self.factory() as session:
            cache = session.scalar(
                select(NodeArtifactCache).where(
                    NodeArtifactCache.node_id == node_id,
                    NodeArtifactCache.cache_kind
                    == MODEL_CACHE_KIND_FULL_SNAPSHOT,
                )
            )
            self.assertIsNotNone(cache)
            self.assertEqual(cache.status, "READY")
            record = session.scalar(
                select(ArtifactPreparationNode)
                .join(
                    ArtifactPreparation,
                    ArtifactPreparation.id
                    == ArtifactPreparationNode.preparation_id,
                )
                .where(
                    ArtifactPreparation.deployment_id
                    == context["deployment"]["id"],
                    ArtifactPreparationNode.node_id == node_id,
                )
            )
            self.assertIsNotNone(record)
            image_attempt = session.scalar(
                select(ArtifactPreparationAttempt).where(
                    ArtifactPreparationAttempt.preparation_node_id
                    == record.id,
                    ArtifactPreparationAttempt.stage == "IMAGE",
                    ArtifactPreparationAttempt.attempt_no
                    == record.image_current_attempt,
                )
            )
            self.assertIsNotNone(image_attempt)
            persisted_image_task = session.get(Task, image_attempt.task_id)
            self.assertIsNotNone(persisted_image_task)
            original_result = copy.deepcopy(persisted_image_task.result)
            corrupted_result = copy.deepcopy(original_result)
            corrupted_result["image_id"] = "sha256:" + "0" * 64
            persisted_image_task.result = corrupted_result
            session.commit()

        stale_image = self.client.post(
            "/v1/admin/tasks",
            headers=self.admin,
            json={
                "node_ids": [node_id],
                "type": "APPLY_DEPLOYMENT",
                "deployment_id": context["deployment"]["id"],
                "options": {"serve": False},
            },
        )
        self._failure_code(stale_image, "DEPLOYMENT_PREPARATION_INVALID")

        with self.factory() as session:
            persisted_image_task = session.get(Task, image_task["id"])
            persisted_image_task.result = original_result
            session.commit()

        verification = self.client.post(
            "/v1/admin/tasks",
            headers=self.admin,
            json={
                "node_ids": [node_id],
                "type": "VERIFY",
                "deployment_id": context["deployment"]["id"],
                "options": {"api": False},
            },
        )
        self.assertEqual(verification.status_code, 200, verification.text)
        verify_task = self._claim(context["enrolled"][0])
        self.assertEqual(verify_task["type"], "VERIFY")
        failed = self.client.post(
            f"/v1/agent/tasks/{verify_task['id']}/fail",
            headers=context["enrolled"][0]["headers"],
            json={"error": "runtime verification failed"},
        )
        self.assertEqual(failed.status_code, 200, failed.text)

        with self.factory() as session:
            cache = session.scalar(
                select(NodeArtifactCache).where(
                    NodeArtifactCache.node_id == node_id,
                    NodeArtifactCache.cache_kind
                    == MODEL_CACHE_KIND_FULL_SNAPSHOT,
                )
            )
            self.assertEqual(cache.status, "CORRUPT")
            task_count = session.scalar(select(func.count()).select_from(Task))

        for task_type, options in (
            ("APPLY_DEPLOYMENT", {"serve": False}),
            ("START_DEPLOYMENT", {"serve": False}),
            ("RESTART_DEPLOYMENT", {"serve": False}),
            ("VERIFY", {"api": False}),
        ):
            with self.subTest(task_type=task_type):
                rejected = self.client.post(
                    "/v1/admin/tasks",
                    headers=self.admin,
                    json={
                        "node_ids": [node_id],
                        "type": task_type,
                        "deployment_id": context["deployment"]["id"],
                        "options": options,
                    },
                )
                self._failure_code(
                    rejected, "DEPLOYMENT_ARTIFACT_CACHE_NOT_READY"
                )

        with self.factory() as session:
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Task)),
                task_count,
            )

    def test_exact_validated_stage_digest_binds_rank_tasks_and_preserves_retry_evidence(
        self,
    ):
        context = self._seed_accepted_generation(
            "stage-success",
            node_count=2,
            agent_version="0.3.19",
            stage_variant_status="VALIDATED",
        )
        selected_digest = context["stage_variant"]["artifact_set_digest"]
        self.assertEqual(
            context["recommendation"]["selected"]["model_cache_kind"],
            MODEL_CACHE_KIND_STAGE,
        )
        self.assertEqual(
            context["recommendation"]["selected"]["stage_artifact"][
                "artifact_set_digest"
            ],
            selected_digest,
        )

        request_id = str(uuid.uuid4())
        preview = self._prepare(
            context,
            request_id,
            apply=False,
            artifact_set_digest=selected_digest,
        )
        self.assertEqual(preview.status_code, 200, preview.text)
        self.assertEqual(preview.json()["tasks"], [])
        snapshot = preview.json()["preparation"]["plan_snapshot"]
        self.assertEqual(
            snapshot["artifact"]["cache_kind"], MODEL_CACHE_KIND_STAGE
        )
        self.assertEqual(
            snapshot["stage_artifact"]["artifact_set_digest"],
            selected_digest,
        )
        self.assertEqual(
            snapshot["effective_plan"]["model_path"],
            "/var/lib/dure/models/stages",
        )
        self.assertEqual(
            snapshot["effective_plan"]["model_cache_kind"],
            MODEL_CACHE_KIND_STAGE,
        )

        omitted_replay = self._prepare(
            context, request_id, apply=False
        )
        self.assertEqual(omitted_replay.status_code, 200, omitted_replay.text)
        self.assertEqual(
            omitted_replay.json()["preparation"]["id"],
            preview.json()["preparation"]["id"],
        )
        self.assertEqual(omitted_replay.json()["tasks"], [])

        applied = self._prepare(
            context,
            request_id,
            apply=True,
            artifact_set_digest=selected_digest,
        )
        self.assertEqual(applied.status_code, 200, applied.text)
        self.assertEqual(len(applied.json()["tasks"]), 2)
        self.assertEqual(
            {item["type"] for item in applied.json()["tasks"]},
            {"PREPARE_MODEL"},
        )
        expected_payload_fields = {
            "preparation_id",
            "preparation_node_id",
            "attempt_id",
            "attempt_no",
            "deployment_id",
            "generation",
            "node_id",
            "apply",
            "model_id",
            "repository",
            "revision",
            "manifest_digest",
            "quantization",
            "cache_kind",
            "artifact_set_digest",
            "contract_identity_digest",
            "source_manifest_digest",
            "runtime_image",
            "vllm_version",
            "exporter_build_digest",
            "architecture",
            "loader_format",
            "tensor_parallel_size",
            "pipeline_parallel_size",
            "pipeline_rank",
            "tensor_rank",
            "tensor_keys_digest",
        }
        binding_by_node = {
            item["node_id"]: item
            for item in snapshot["stage_artifact"]["node_bindings"]
        }
        for task in applied.json()["tasks"]:
            payload = task["payload"]
            binding = binding_by_node[task["node_id"]]
            self.assertEqual(set(payload), expected_payload_fields)
            self.assertEqual(payload["cache_kind"], MODEL_CACHE_KIND_STAGE)
            self.assertEqual(
                payload["artifact_set_digest"], selected_digest
            )
            self.assertEqual(
                payload["manifest_digest"], binding["manifest_digest"]
            )
            self.assertEqual(
                payload["pipeline_rank"], binding["pipeline_rank"]
            )
            self.assertEqual(
                payload["tensor_rank"], binding["tensor_rank"]
            )
            self.assertEqual(
                payload["tensor_keys_digest"],
                binding["tensor_keys_digest"],
            )

        ordered = sorted(
            context["enrolled"], key=lambda item: item["node_id"]
        )
        model_tasks = {
            enrolled["node_id"]: self._claim(enrolled)
            for enrolled in ordered
        }
        for enrolled in ordered:
            task = model_tasks[enrolled["node_id"]]
            manifest_response = self.client.get(
                f"/v1/agent/tasks/{task['id']}/artifact-manifest",
                headers=enrolled["headers"],
            )
            self.assertEqual(
                manifest_response.status_code, 200, manifest_response.text
            )
            self.assertEqual(
                canonical_artifact_manifest_digest(
                    manifest_response.json()["manifest"]
                ),
                task["payload"]["manifest_digest"],
            )

        cross_rank_manifest = self.client.get(
            "/v1/agent/tasks/"
            + model_tasks[ordered[0]["node_id"]]["id"]
            + "/artifact-manifest",
            headers=ordered[1]["headers"],
        )
        self._failure_code(
            cross_rank_manifest, "PREPARATION_MANIFEST_UNAVAILABLE"
        )

        first_model = model_tasks[ordered[0]["node_id"]]
        first_model_result = self._result(first_model, context)
        completed_model = self.client.post(
            f"/v1/agent/tasks/{first_model['id']}/complete",
            headers=ordered[0]["headers"],
            json={"result": first_model_result},
        )
        self.assertEqual(
            completed_model.status_code, 200, completed_model.text
        )
        first_image = self._claim(ordered[0])
        first_image_result = self._result(first_image, context)
        completed_image = self.client.post(
            f"/v1/agent/tasks/{first_image['id']}/complete",
            headers=ordered[0]["headers"],
            json={"result": first_image_result},
        )
        self.assertEqual(
            completed_image.status_code, 200, completed_image.text
        )

        second_model = model_tasks[ordered[1]["node_id"]]
        cross_rank_result = self._result(second_model, context)
        for field in (
            "manifest_digest",
            "pipeline_rank",
            "tensor_rank",
            "tensor_keys_digest",
            "cache_identity_digest",
        ):
            cross_rank_result[field] = first_model_result[field]
        rejected = self.client.post(
            f"/v1/agent/tasks/{second_model['id']}/complete",
            headers=ordered[1]["headers"],
            json={"result": cross_rank_result},
        )
        self.assertEqual(rejected.status_code, 422, rejected.text)
        self.assertEqual(
            rejected.json()["detail"]["code"],
            "PREPARATION_RESULT_REJECTED",
        )

        preparation_id = preview.json()["preparation"]["id"]
        partial = self.client.get(
            f"/v1/admin/deployment-preparations/{preparation_id}",
            headers=self.admin,
        )
        self.assertEqual(partial.status_code, 200, partial.text)
        self.assertEqual(
            partial.json()["preparation"]["status"], "PARTIAL_FAILED"
        )
        partial_by_node = {
            item["node_id"]: item
            for item in partial.json()["preparation"]["nodes"]
        }
        expected_by_node = {
            node_id: sum(
                item["size_bytes"]
                for item in context["stage_manifests"][
                    binding["manifest_digest"]
                ]["files"]
            )
            for node_id, binding in binding_by_node.items()
        }
        partial_progress = partial.json()["preparation"]["progress"]
        self.assertEqual(
            partial_progress["expected_bytes"],
            sum(expected_by_node.values()),
        )
        self.assertEqual(
            partial_progress["verified_bytes"],
            first_model_result["bytes_verified"],
        )
        self.assertEqual(partial_progress["stage"], "FAILED")
        self.assertFalse(partial_progress["retrying"])
        self.assertEqual(
            partial_by_node[ordered[0]["node_id"]]["progress"]["stage"],
            "COMPLETE",
        )
        self.assertEqual(
            partial_by_node[ordered[0]["node_id"]]["progress"][
                "verified_bytes"
            ],
            expected_by_node[ordered[0]["node_id"]],
        )
        failed_progress = partial_by_node[ordered[1]["node_id"]][
            "progress"
        ]
        self.assertEqual(failed_progress["stage"], "FAILED")
        self.assertEqual(failed_progress["verified_bytes"], 0)
        self.assertEqual(failed_progress["model"]["status"], "FAILED")
        self.assertEqual(failed_progress["model"]["current_attempt"], 1)
        self.assertEqual(failed_progress["model"]["retry_count"], 0)
        first_evidence = copy.deepcopy(
            partial_by_node[ordered[0]["node_id"]]["attempts"]
        )
        self.assertEqual(
            [item["result"] for item in first_evidence],
            [first_image_result, first_model_result],
        )
        self.assertEqual(
            partial_by_node[ordered[1]["node_id"]][
                "model_failure_code"
            ],
            "PREPARATION_RESULT_REJECTED",
        )

        retry = self._prepare(
            context,
            request_id,
            apply=True,
            artifact_set_digest=selected_digest,
        )
        self.assertEqual(retry.status_code, 200, retry.text)
        self.assertEqual(len(retry.json()["tasks"]), 1)
        retry_task = retry.json()["tasks"][0]
        self.assertEqual(retry_task["type"], "PREPARE_MODEL")
        self.assertEqual(retry_task["node_id"], ordered[1]["node_id"])
        self.assertEqual(retry_task["payload"]["attempt_no"], 2)
        retry_progress = retry.json()["preparation"]["progress"]
        self.assertEqual(retry_progress["stage"], "MODEL")
        self.assertTrue(retry_progress["retrying"])
        self.assertEqual(retry_progress["model_retry_count"], 1)
        retry_by_node = {
            item["node_id"]: item
            for item in retry.json()["preparation"]["nodes"]
        }
        retry_node_progress = retry_by_node[ordered[1]["node_id"]][
            "progress"
        ]
        self.assertEqual(retry_node_progress["stage"], "MODEL")
        self.assertTrue(retry_node_progress["retrying"])
        self.assertEqual(retry_node_progress["model"]["status"], "QUEUED")
        self.assertEqual(
            retry_node_progress["model"]["current_attempt"], 2
        )
        self.assertEqual(retry_node_progress["model"]["retry_count"], 1)

        retried_model = self._claim(ordered[1])
        self._complete(retried_model, ordered[1], context)
        retried_image = self._claim(ordered[1])
        self.assertEqual(retried_image["payload"]["attempt_no"], 1)
        self._complete(retried_image, ordered[1], context)

        completed = self.client.get(
            f"/v1/admin/deployment-preparations/{preparation_id}",
            headers=self.admin,
        )
        self.assertEqual(completed.status_code, 200, completed.text)
        self.assertEqual(
            completed.json()["preparation"]["status"], "SUCCEEDED"
        )
        completed_progress = completed.json()["preparation"]["progress"]
        self.assertEqual(completed_progress["stage"], "COMPLETE")
        self.assertEqual(
            completed_progress["verified_bytes"],
            completed_progress["expected_bytes"],
        )
        self.assertEqual(completed_progress["model_retry_count"], 1)
        self.assertFalse(completed_progress["retrying"])
        completed_by_node = {
            item["node_id"]: item
            for item in completed.json()["preparation"]["nodes"]
        }
        self.assertEqual(
            completed_by_node[ordered[0]["node_id"]]["attempts"],
            first_evidence,
        )

        with self.factory() as session:
            deployment = session.get(
                Deployment, context["deployment"]["id"]
            )
            plan = effective_deployment_plan(session, deployment)
        self.assertEqual(plan, snapshot["effective_plan"])
        self.assertEqual(
            plan["stage_artifact"]["artifact_set_digest"],
            selected_digest,
        )

        revoked = self.client.post(
            "/v1/admin/stage-artifact-variants/"
            + selected_digest
            + "/transition",
            headers=self.admin,
            json={"status": "REVOKED"},
        )
        self.assertEqual(revoked.status_code, 200, revoked.text)
        with self.factory() as session:
            deployment = session.get(
                Deployment, context["deployment"]["id"]
            )
            containment_plan = effective_deployment_plan(
                session, deployment, require_prepared=False
            )
        self.assertEqual(containment_plan, snapshot["effective_plan"])

        for task_type, options in (
            ("START_DEPLOYMENT", {"serve": True}),
            ("RESTART_DEPLOYMENT", {"serve": True}),
            ("VERIFY", {"api": True}),
        ):
            with self.subTest(task_type=task_type):
                rejected = self.client.post(
                    "/v1/admin/tasks",
                    headers=self.admin,
                    json={
                        "node_ids": [item["node_id"] for item in ordered],
                        "type": task_type,
                        "deployment_id": context["deployment"]["id"],
                        "options": options,
                    },
                )
                self._failure_code(
                    rejected, "DEPLOYMENT_STAGE_VARIANT_UNAVAILABLE"
                )

        emergency_stop = self.client.post(
            "/v1/admin/tasks",
            headers=self.admin,
            json={
                "node_ids": [item["node_id"] for item in ordered],
                "type": "STOP_DEPLOYMENT",
                "deployment_id": context["deployment"]["id"],
                "options": {},
            },
        )
        self.assertEqual(emergency_stop.status_code, 200, emergency_stop.text)
        self.assertEqual(len(emergency_stop.json()["tasks"]), 2)
        for task in emergency_stop.json()["tasks"]:
            self.assertEqual(
                task["payload"]["plan"]["model_cache_kind"],
                MODEL_CACHE_KIND_STAGE,
            )
            self.assertEqual(
                task["payload"]["plan"]["stage_artifact"][
                    "artifact_set_digest"
                ],
                selected_digest,
            )

    def test_rollback_stage_source_requires_agent_and_preserves_labels(self):
        context = self._seed_accepted_generation(
            "stage-rb",
            node_count=2,
            agent_version="0.3.19",
            stage_variant_status="VALIDATED",
        )
        selected_digest = context["stage_variant"]["artifact_set_digest"]
        preview = self._prepare(
            context,
            str(uuid.uuid4()),
            apply=False,
            artifact_set_digest=selected_digest,
        )
        self.assertEqual(preview.status_code, 200, preview.text)

        revoked = self.client.post(
            "/v1/admin/stage-artifact-variants/"
            + selected_digest
            + "/transition",
            headers=self.admin,
            json={"status": "REVOKED"},
        )
        self.assertEqual(revoked.status_code, 200, revoked.text)

        node_ids = sorted(item["node_id"] for item in context["enrolled"])
        with self.factory() as session:
            source = session.get(
                Deployment, context["deployment"]["id"]
            )
            self.assertIsNotNone(source)
            preparation = session.scalar(
                select(ArtifactPreparation).where(
                    ArtifactPreparation.deployment_id == source.id
                )
            )
            self.assertIsNotNone(preparation)

            target_id = str(uuid.uuid4())
            target_plan = copy.deepcopy(source.plan)
            target_plan["deployment_id"] = target_id
            target_plan["generation"] = 1
            target = Deployment(
                id=target_id,
                lineage_id=target_id,
                previous_generation_id=None,
                generation=1,
                plan=target_plan,
                accept_model_download=False,
                pull_image=False,
                status="VERIFIED",
                verified_at=utcnow() - timedelta(hours=1),
            )
            session.add(target)
            session.flush()

            source_plan = copy.deepcopy(source.plan)
            source_plan["generation"] = 2
            source.plan = source_plan
            source.lineage_id = target_id
            source.previous_generation_id = target_id
            source.generation = 2
            source.status = "APPLIED"
            source.verified_at = None

            snapshot = copy.deepcopy(preparation.plan_snapshot)
            snapshot["generation"] = 2
            snapshot["effective_plan"]["generation"] = 2
            preparation.plan_snapshot = snapshot
            session.commit()

            for node_id in node_ids:
                session.get(Node, node_id).agent_version = "0.3.18"
            session.commit()
            with self.assertRaises(DeploymentRolloutError) as rejected:
                prepare_or_apply_rollback(
                    session,
                    source.id,
                    node_ids,
                    apply=True,
                    serve=True,
                )
            self.assertEqual(
                rejected.exception.code, "ROLLBACK_STAGE_AGENT_TOO_OLD"
            )
            self.assertEqual(
                rejected.exception.details, {"node_ids": node_ids}
            )
            session.rollback()

            for node_id in node_ids:
                session.get(Node, node_id).agent_version = "0.3.19"
            session.commit()
            operation, tasks, changed = prepare_or_apply_rollback(
                session,
                source.id,
                node_ids,
                apply=True,
                serve=True,
            )

            self.assertTrue(changed)
            self.assertEqual(operation.phase, "STOP_SOURCE")
            self.assertEqual(len(tasks), 2)
            task_ids = [task.id for task in tasks]
            with self.factory() as verification_session:
                persisted_tasks = [
                    verification_session.get(Task, task_id)
                    for task_id in task_ids
                ]
            self.assertTrue(all(task is not None for task in persisted_tasks))
            for persisted in persisted_tasks:
                self.assertEqual(
                    persisted.payload["plan"], snapshot["effective_plan"]
                )
                self.assertEqual(
                    persisted.payload["plan"]["stage_artifact"][
                        "artifact_set_digest"
                    ],
                    selected_digest,
                )
                assignment = next(
                    item
                    for item in persisted.payload["plan"]["assignments"]
                    if item["node_id"] == persisted.node_id
                )
                self.assertRegex(
                    assignment["stage_manifest_digest"],
                    r"^sha256:[0-9a-f]{64}$",
                )
                self.assertRegex(
                    assignment["stage_tensor_keys_digest"],
                    r"^sha256:[0-9a-f]{64}$",
                )

    def test_stage_prepare_rejects_delivery_override_and_revoked_selection(self):
        context = self._seed_accepted_generation(
            "stage-miss",
            node_count=2,
            agent_version="0.3.19",
            manifest=_oversized_manifest(4, chunk_character="a"),
        )
        self.assertEqual(
            context["recommendation"]["selected"]["model_cache_kind"],
            MODEL_CACHE_KIND_FULL_SNAPSHOT,
        )
        unavailable = self._prepare(
            context,
            str(uuid.uuid4()),
            apply=False,
            artifact_set_digest=_digest("a"),
        )
        self._failure_code(
            unavailable, "PREPARATION_STAGE_VARIANT_MISMATCH"
        )

        draft_context = self._seed_accepted_generation(
            "stage-draft",
            node_count=2,
            agent_version="0.3.19",
            stage_variant_status="DRAFT",
            manifest=_oversized_manifest(4, chunk_character="b"),
        )
        self.assertEqual(
            draft_context["recommendation"]["selected"]["model_cache_kind"],
            MODEL_CACHE_KIND_FULL_SNAPSHOT,
        )
        draft = self._prepare(
            draft_context,
            str(uuid.uuid4()),
            apply=False,
            artifact_set_digest=draft_context["stage_variant"][
                "artifact_set_digest"
            ],
        )
        self._failure_code(draft, "PREPARATION_STAGE_VARIANT_MISMATCH")

        revoked_context = self._seed_accepted_generation(
            "stage-revoke",
            node_count=2,
            agent_version="0.3.19",
            stage_variant_status="VALIDATED",
            manifest=_oversized_manifest(4, chunk_character="c"),
        )
        digest = revoked_context["stage_variant"]["artifact_set_digest"]
        revoked = self.client.post(
            "/v1/admin/stage-artifact-variants/"
            + digest
            + "/transition",
            headers=self.admin,
            json={"status": "REVOKED"},
        )
        self.assertEqual(revoked.status_code, 200, revoked.text)
        revoked_prepare = self._prepare(
            revoked_context,
            str(uuid.uuid4()),
            apply=False,
        )
        self._failure_code(
            revoked_prepare, "PREPARATION_INVENTORY_STALE"
        )

        with self.factory() as session:
            self.assertEqual(
                session.scalar(
                    select(func.count()).select_from(ArtifactPreparation)
                ),
                0,
            )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Task)), 0
            )

    def test_stage_candidate_falls_back_to_full_for_agent_0_3_18(self):
        context = self._seed_accepted_generation(
            "stage-old",
            node_count=2,
            agent_version="0.3.18",
            stage_variant_status="VALIDATED",
        )
        self.assertEqual(
            context["recommendation"]["selected"]["model_cache_kind"],
            MODEL_CACHE_KIND_FULL_SNAPSHOT,
        )
        stage_candidate = next(
            candidate
            for candidate in context["recommendation"]["candidates"]
            if candidate.get("model_cache_kind") == MODEL_CACHE_KIND_STAGE
        )
        self.assertFalse(stage_candidate["feasible"])
        self.assertIn(
            "STAGE_AGENT_VERSION",
            {item["code"] for item in stage_candidate["rejections"]},
        )
        response = self._prepare(
            context,
            str(uuid.uuid4()),
            apply=False,
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["tasks"], [])
        with self.factory() as session:
            self.assertEqual(
                session.scalar(
                    select(func.count()).select_from(ArtifactPreparation)
                ),
                1,
            )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Task)), 0
            )

    def test_partial_failure_retries_only_failed_stage_and_fences_old_task(self):
        context = self._seed_accepted_generation("retry", node_count=3)
        request_id = str(uuid.uuid4())
        applied = self._prepare(context, request_id, apply=True)
        self.assertEqual(applied.status_code, 200, applied.text)
        self.assertEqual(len(applied.json()["tasks"]), 3)
        self.assertEqual(
            {task["type"] for task in applied.json()["tasks"]},
            {"PREPARE_MODEL"},
        )
        by_node = {
            item["node_id"]: item for item in context["enrolled"]
        }
        ordered = sorted(context["enrolled"], key=lambda item: item["node_id"])

        first_model = self._claim(ordered[0])
        self._complete(first_model, ordered[0], context)
        first_image = self._claim(ordered[0])
        image_progress = self.client.post(
            f"/v1/agent/tasks/{first_image['id']}/heartbeat",
            headers=ordered[0]["headers"],
            json={"progress": {"downloaded_bytes": 1}},
        )
        self.assertEqual(image_progress.status_code, 409, image_progress.text)
        self._complete(first_image, ordered[0], context)

        failed_model = self._claim(ordered[1])
        secret = "https://user:SUPERSECRET@example.invalid/private-model"
        model_failure = self.client.post(
            f"/v1/agent/tasks/{failed_model['id']}/fail",
            headers=ordered[1]["headers"],
            json={"error": secret},
        )
        self.assertEqual(model_failure.status_code, 200, model_failure.text)
        self.assertNotIn(secret, model_failure.text)

        third_model = self._claim(ordered[2])
        self._complete(third_model, ordered[2], context)
        failed_image = self._claim(ordered[2])
        image_failure = self.client.post(
            f"/v1/agent/tasks/{failed_image['id']}/fail",
            headers=ordered[2]["headers"],
            json={"error": "PREPARATION_IMAGE_PULL_FAILED"},
        )
        self.assertEqual(image_failure.status_code, 200, image_failure.text)

        preparation_id = applied.json()["preparation"]["id"]
        partial = self.client.get(
            f"/v1/admin/deployment-preparations/{preparation_id}",
            headers=self.admin,
        )
        self.assertEqual(partial.status_code, 200, partial.text)
        self.assertEqual(partial.json()["preparation"]["status"], "PARTIAL_FAILED")
        self.assertNotIn(secret, partial.text)
        node_details = {
            item["node_id"]: item
            for item in partial.json()["preparation"]["nodes"]
        }
        expected_per_node = sum(
            item["size_bytes"] for item in context["manifest"]["files"]
        )
        partial_progress = partial.json()["preparation"]["progress"]
        self.assertEqual(
            partial_progress["expected_bytes"], expected_per_node * 3
        )
        self.assertEqual(
            partial_progress["verified_bytes"], expected_per_node * 2
        )
        self.assertEqual(partial_progress["stage"], "FAILED")
        self.assertFalse(partial_progress["retrying"])
        self.assertEqual(
            node_details[ordered[1]["node_id"]]["model_failure_code"],
            "PREPARATION_EXECUTION_FAILED",
        )
        self.assertEqual(
            node_details[ordered[1]["node_id"]]["progress"]["model"][
                "status"
            ],
            "FAILED",
        )
        self.assertEqual(
            node_details[ordered[2]["node_id"]]["progress"]["image"][
                "status"
            ],
            "FAILED",
        )
        self.assertEqual(
            node_details[ordered[2]["node_id"]]["progress"][
                "verified_bytes"
            ],
            expected_per_node,
        )

        retry_model_response = self._prepare(context, request_id, apply=True)
        self.assertEqual(
            retry_model_response.status_code, 200, retry_model_response.text
        )
        retry_model_tasks = retry_model_response.json()["tasks"]
        self.assertEqual(len(retry_model_tasks), 1)
        self.assertEqual(retry_model_tasks[0]["type"], "PREPARE_MODEL")
        self.assertEqual(retry_model_tasks[0]["node_id"], ordered[1]["node_id"])
        self.assertEqual(retry_model_tasks[0]["payload"]["attempt_no"], 2)
        retry_model_progress = retry_model_response.json()["preparation"][
            "progress"
        ]
        self.assertEqual(retry_model_progress["stage"], "FAILED")
        self.assertTrue(retry_model_progress["retrying"])
        self.assertEqual(retry_model_progress["model_retry_count"], 1)
        self.assertEqual(retry_model_progress["image_retry_count"], 0)

        stale_progress = self.client.post(
            f"/v1/agent/tasks/{failed_model['id']}/heartbeat",
            headers=ordered[1]["headers"],
            json={"progress": {"downloaded_bytes": 1}},
        )
        self.assertEqual(stale_progress.status_code, 409, stale_progress.text)

        late = self.client.post(
            f"/v1/agent/tasks/{failed_model['id']}/complete",
            headers=ordered[1]["headers"],
            json={"result": self._result(failed_model, context)},
        )
        self.assertEqual(late.status_code, 409, late.text)

        retried_model = self._claim(by_node[ordered[1]["node_id"]])
        self._complete(retried_model, ordered[1], context)
        retried_node_image = self._claim(ordered[1])
        self.assertEqual(retried_node_image["payload"]["attempt_no"], 1)
        self._complete(retried_node_image, ordered[1], context)

        retry_image_response = self._prepare(context, request_id, apply=True)
        self.assertEqual(
            retry_image_response.status_code, 200, retry_image_response.text
        )
        retry_image_tasks = retry_image_response.json()["tasks"]
        self.assertEqual(len(retry_image_tasks), 1)
        self.assertEqual(retry_image_tasks[0]["type"], "PREPARE_IMAGE")
        self.assertEqual(retry_image_tasks[0]["node_id"], ordered[2]["node_id"])
        self.assertEqual(retry_image_tasks[0]["payload"]["attempt_no"], 2)
        retry_image_progress = retry_image_response.json()["preparation"][
            "progress"
        ]
        self.assertEqual(retry_image_progress["stage"], "IMAGE")
        self.assertTrue(retry_image_progress["retrying"])
        self.assertEqual(retry_image_progress["model_retry_count"], 1)
        self.assertEqual(retry_image_progress["image_retry_count"], 1)
        retry_image_nodes = {
            item["node_id"]: item
            for item in retry_image_response.json()["preparation"]["nodes"]
        }
        retry_image_node_progress = retry_image_nodes[
            ordered[2]["node_id"]
        ]["progress"]
        self.assertEqual(retry_image_node_progress["stage"], "IMAGE")
        self.assertEqual(
            retry_image_node_progress["image"]["current_attempt"], 2
        )
        self.assertEqual(
            retry_image_node_progress["image"]["retry_count"], 1
        )
        self.assertEqual(
            retry_image_node_progress["image"]["status"], "QUEUED"
        )
        retried_image = self._claim(ordered[2])
        self._complete(retried_image, ordered[2], context)
        completed = self.client.get(
            f"/v1/admin/deployment-preparations/{preparation_id}",
            headers=self.admin,
        )
        self.assertEqual(
            completed.json()["preparation"]["status"], "SUCCEEDED"
        )
        completed_progress = completed.json()["preparation"]["progress"]
        self.assertEqual(completed_progress["stage"], "COMPLETE")
        self.assertEqual(
            completed_progress["verified_bytes"], expected_per_node * 3
        )
        self.assertEqual(completed_progress["model_retry_count"], 1)
        self.assertEqual(completed_progress["image_retry_count"], 1)
        self.assertFalse(completed_progress["retrying"])
        self.assertNotIn(secret, completed.text)

    def test_failed_node_dominates_mixed_image_stage_projection(self):
        preparation = type("Preparation", (), {"status": "RUNNING"})()
        self.assertEqual(
            _preparation_stage(
                preparation,
                [{"stage": "FAILED"}, {"stage": "IMAGE"}],
            ),
            "FAILED",
        )

    def test_removed_manifest_invalidates_inventory_without_tasks_or_preparation(self):
        context = self._seed_accepted_generation(
            "missing-manifest", register_manifest=False
        )

        response = self._prepare(context, str(uuid.uuid4()), apply=False)

        self._failure_code(response, "PREPARATION_INVENTORY_STALE")
        with self.factory() as session:
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Task)), 0
            )
            self.assertEqual(
                session.scalar(
                    select(func.count()).select_from(ArtifactPreparation)
                ),
                0,
            )

    def test_deprecated_release_is_rejected_before_preparation_creation(self):
        context = self._seed_accepted_generation("deprecated")
        with self.factory() as session:
            transition_model_release(
                session, context["release_id"], "DEPRECATED"
            )

        response = self._prepare(context, str(uuid.uuid4()), apply=False)

        self._failure_code(response, "PREPARATION_RECOMMENDATION_STALE")
        with self.factory() as session:
            self.assertEqual(
                session.scalar(
                    select(func.count()).select_from(ArtifactPreparation)
                ),
                0,
            )

    def test_revoked_release_is_rejected_when_failed_stage_is_retried(self):
        context = self._seed_accepted_generation("rev-retry")
        request_id = str(uuid.uuid4())
        applied = self._prepare(context, request_id, apply=True)
        self.assertEqual(applied.status_code, 200, applied.text)
        task = self._claim(context["enrolled"][0])
        failed = self.client.post(
            f"/v1/agent/tasks/{task['id']}/fail",
            headers=context["enrolled"][0]["headers"],
            json={"error": "PREPARATION_ORIGIN_UNAVAILABLE"},
        )
        self.assertEqual(failed.status_code, 200, failed.text)

        with self.factory() as session:
            transition_model_release(
                session, context["release_id"], "DEPRECATED"
            )
            transition_model_release(
                session, context["release_id"], "REVOKED"
            )

        retry = self._prepare(context, request_id, apply=True)
        self._failure_code(retry, "PREPARATION_RECOMMENDATION_STALE")
        with self.factory() as session:
            tasks = list(
                session.scalars(
                    select(Task).where(
                        Task.bulk_id
                        == applied.json()["preparation"]["id"]
                    )
                )
            )
            self.assertEqual(len(tasks), 1)
            self.assertEqual(tasks[0].status, "FAILED")

    def test_revoked_release_blocks_prepared_apply_but_not_emergency_stop(self):
        context = self._seed_accepted_generation("rev-consume")
        applied = self._prepare(context, str(uuid.uuid4()), apply=True)
        self.assertEqual(applied.status_code, 200, applied.text)

        with self.factory() as session:
            transition_model_release(
                session, context["release_id"], "DEPRECATED"
            )
            transition_model_release(
                session, context["release_id"], "REVOKED"
            )

        enrolled = context["enrolled"][0]
        model_task = self._claim(enrolled)
        self._complete(model_task, enrolled, context)
        image_task = self._claim(enrolled)
        self._complete(image_task, enrolled, context)

        deployment_apply = self.client.post(
            "/v1/admin/tasks",
            headers=self.admin,
            json={
                "node_ids": [enrolled["node_id"]],
                "type": "APPLY_DEPLOYMENT",
                "deployment_id": context["deployment"]["id"],
                "options": {"serve": False},
            },
        )
        self._failure_code(
            deployment_apply, "DEPLOYMENT_MODEL_RELEASE_REVOKED"
        )

        emergency_stop = self.client.post(
            "/v1/admin/tasks",
            headers=self.admin,
            json={
                "node_ids": [enrolled["node_id"]],
                "type": "STOP_DEPLOYMENT",
                "deployment_id": context["deployment"]["id"],
                "options": {},
            },
        )
        self.assertEqual(emergency_stop.status_code, 200, emergency_stop.text)

    def test_image_failure_with_no_complete_node_is_failed(self):
        context = self._seed_accepted_generation("no-complete-node")
        applied = self._prepare(context, str(uuid.uuid4()), apply=True)
        self.assertEqual(applied.status_code, 200, applied.text)
        enrolled = context["enrolled"][0]

        model_task = self._claim(enrolled)
        self._complete(model_task, enrolled, context)
        image_task = self._claim(enrolled)
        failed = self.client.post(
            f"/v1/agent/tasks/{image_task['id']}/fail",
            headers=enrolled["headers"],
            json={"error": "PREPARATION_IMAGE_PULL_FAILED"},
        )
        self.assertEqual(failed.status_code, 200, failed.text)

        detail = self.client.get(
            "/v1/admin/deployment-preparations/"
            + applied.json()["preparation"]["id"],
            headers=self.admin,
        )
        self.assertEqual(detail.status_code, 200, detail.text)
        self.assertEqual(detail.json()["preparation"]["status"], "FAILED")

    def test_pending_offline_and_stale_nodes_fail_before_task_creation(self):
        context = self._seed_accepted_generation("node-gates")
        node_id = context["enrolled"][0]["node_id"]
        request_id = str(uuid.uuid4())
        with self.factory() as session:
            node = session.get(Node, node_id)
            profile_record = session.get(NodeProfileRecord, node_id)
            original_last_seen = node.last_seen
            original_profile_updated_at = profile_record.updated_at

            node.approved = False
            session.commit()
        self._failure_code(
            self._prepare(context, request_id, apply=False),
            "PREPARATION_NODE_UNAPPROVED",
        )

        with self.factory() as session:
            node = session.get(Node, node_id)
            node.approved = True
            node.last_seen = utcnow() - timedelta(seconds=31)
            session.commit()
        self._failure_code(
            self._prepare(context, request_id, apply=False),
            "PREPARATION_NODE_OFFLINE",
        )

        with self.factory() as session:
            node = session.get(Node, node_id)
            profile_record = session.get(NodeProfileRecord, node_id)
            node.last_seen = original_last_seen
            profile_record.updated_at = utcnow() - timedelta(seconds=91)
            session.commit()
        self._failure_code(
            self._prepare(context, request_id, apply=False),
            "PREPARATION_PROFILE_STALE",
        )
        with self.factory() as session:
            profile_record = session.get(NodeProfileRecord, node_id)
            profile_record.updated_at = original_profile_updated_at
            session.commit()
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Task)), 0
            )

    def test_invalid_result_is_terminal_and_revocation_fences_queued_work(self):
        context = self._seed_accepted_generation("result-rejection")
        request_id = str(uuid.uuid4())
        applied = self._prepare(context, request_id, apply=True)
        self.assertEqual(applied.status_code, 200, applied.text)
        task = self._claim(context["enrolled"][0])

        # Even a credential that remains otherwise valid cannot extend a
        # preparation lease after central approval is removed.
        with self.factory() as session:
            node = session.get(Node, context["enrolled"][0]["node_id"])
            node.approved = False
            session.commit()
        heartbeat = self.client.post(
            f"/v1/agent/tasks/{task['id']}/heartbeat",
            headers=context["enrolled"][0]["headers"],
        )
        self.assertEqual(heartbeat.status_code, 409, heartbeat.text)
        with self.factory() as session:
            node = session.get(Node, context["enrolled"][0]["node_id"])
            node.approved = True
            session.commit()

        invalid_result = self._result(task, context)
        invalid_result.pop("file_count")
        rejected = self.client.post(
            f"/v1/agent/tasks/{task['id']}/complete",
            headers=context["enrolled"][0]["headers"],
            json={"result": invalid_result},
        )
        self.assertEqual(rejected.status_code, 422, rejected.text)
        self.assertEqual(
            rejected.json()["detail"]["code"],
            "PREPARATION_RESULT_REJECTED",
        )
        with self.factory() as session:
            stored = session.get(Task, task["id"])
            self.assertEqual(stored.status, "FAILED")
            self.assertEqual(stored.error, "PREPARATION_RESULT_REJECTED")
            self.assertIsNone(stored.lease_until)
        retry = self._prepare(context, request_id, apply=True)
        self.assertEqual(retry.status_code, 200, retry.text)
        self.assertEqual(len(retry.json()["tasks"]), 1)
        self.assertEqual(retry.json()["tasks"][0]["payload"]["attempt_no"], 2)

        queued_context = self._seed_accepted_generation(
            "revoke-queued",
            manifest=_oversized_manifest(3, chunk_character="e"),
        )
        queued = self._prepare(
            queued_context, str(uuid.uuid4()), apply=True
        )
        queued_task = queued.json()["tasks"][0]
        revoked = self.client.post(
            "/v1/admin/nodes/"
            f"{queued_context['enrolled'][0]['node_id']}/revoke",
            headers=self.admin,
        )
        self.assertEqual(revoked.status_code, 200, revoked.text)
        with self.factory() as session:
            stored = session.get(Task, queued_task["id"])
            self.assertEqual(stored.status, "CANCELED")
            self.assertEqual(stored.error, "PREPARATION_NODE_REVOKED")

    def test_request_binding_and_node_exclusive_mutation_conflicts_are_closed(self):
        first = self._seed_accepted_generation("request-first")
        request_id = str(uuid.uuid4())
        prepared = self._prepare(first, request_id, apply=False)
        self.assertEqual(prepared.status_code, 200, prepared.text)

        applied = self._prepare(first, request_id, apply=True)
        self.assertEqual(applied.status_code, 200, applied.text)
        stop = self.client.post(
            "/v1/admin/tasks",
            headers=self.admin,
            json={
                "node_ids": [first["enrolled"][0]["node_id"]],
                "type": "STOP_DEPLOYMENT",
                "deployment_id": first["deployment"]["id"],
                "options": {},
            },
        )
        self.assertEqual(stop.status_code, 409, stop.text)
        with self.factory() as session:
            active = list(
                session.scalars(
                    select(Task).where(
                        Task.node_id == first["enrolled"][0]["node_id"],
                        Task.status.in_({"QUEUED", "RUNNING"}),
                    )
                )
            )
            self.assertEqual([task.type for task in active], ["PREPARE_MODEL"])

        second = self._seed_accepted_generation(
            "request-second",
            manifest=_oversized_manifest(4, chunk_character="f"),
        )
        duplicate = self._prepare(second, request_id, apply=False)
        self._failure_code(duplicate, "PREPARATION_REQUEST_CONFLICT")

    def test_disk_capacity_and_busy_node_gates_fail_closed(self):
        too_large = 2 * 1024 * 1024
        disk_context = self._seed_accepted_generation(
            "disk-gate",
            manifest=_oversized_manifest(too_large),
            disk_free_mib=2,
            expect_selection=False,
        )
        full_candidate = next(
            candidate
            for candidate in disk_context["recommendation"]["candidates"]
            if candidate.get("model_cache_kind")
            == MODEL_CACHE_KIND_FULL_SNAPSHOT
        )
        self.assertFalse(full_candidate["feasible"])
        self.assertIn(
            "DISK_SPACE",
            {item["code"] for item in full_candidate["rejections"]},
        )

        # The selector rejects an undersized generation before acceptance. Use
        # an independently accepted generation for the active-task/operation
        # preparation boundaries.
        busy_context = self._seed_accepted_generation(
            "busy-gates",
            manifest=_oversized_manifest(4, chunk_character="e"),
        )
        node_id = busy_context["enrolled"][0]["node_id"]
        busy_task = self.client.post(
            "/v1/admin/tasks",
            headers=self.admin,
            json={
                "node_ids": [node_id],
                "type": "PROBE",
                "options": {},
            },
        )
        self.assertEqual(busy_task.status_code, 200, busy_task.text)
        busy_response = self._prepare(
            busy_context, str(uuid.uuid4()), apply=False
        )
        self._failure_code(busy_response, "PREPARATION_NODE_BUSY")

        with self.factory() as session:
            task = session.scalar(
                select(Task).where(
                    Task.node_id == node_id,
                    Task.type == "PROBE",
                    Task.status == "QUEUED",
                )
            )
            task.status = "CANCELED"
            operation_id = str(uuid.uuid4())
            operation = DeploymentOperation(
                id=operation_id,
                request_digest="sha256:" + "9" * 64,
                lineage_id=busy_context["deployment"]["id"],
                deployment_id=busy_context["deployment"]["id"],
                kind="APPLY",
                status="PARTIAL_FAILED",
                phase="APPLY",
                node_ids=[node_id],
                serve=False,
                api=False,
                active_lineage_id=busy_context["deployment"]["id"],
            )
            session.add(operation)
            session.commit()

        operation_response = self._prepare(
            busy_context, str(uuid.uuid4()), apply=False
        )
        self._failure_code(operation_response, "PREPARATION_NODE_BUSY")
        self.assertEqual(
            operation_response.json()["detail"]["details"],
            {"operation_id": operation_id, "node_ids": [node_id]},
        )


if __name__ == "__main__":
    unittest.main()

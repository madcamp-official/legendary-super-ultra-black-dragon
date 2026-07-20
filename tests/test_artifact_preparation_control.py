from __future__ import annotations

import copy
import tempfile
import unittest
import uuid
from datetime import timedelta
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import func, select

from dure.control.api import create_app
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
    DeploymentOperation,
    Node,
    NodeProfileRecord,
    Task,
    utcnow,
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
from dure.model_cache import MODEL_CACHE_VERIFICATION_VERSION

from .helpers import profile
from .test_artifact_manifest_api import _manifest


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
                    "agent_version": "0.3.16",
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
    ) -> dict:
        enrolled = self._enroll_nodes(
            node_count, key, disk_free_mib=disk_free_mib
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
        self.assertIsNotNone(recommendation["selected"])
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
        return {
            "artifact_id": artifact_id,
            "release_id": release_id,
            "deployment": accepted_response.json()["deployment"],
            "enrolled": enrolled,
            "manifest": manifest_value,
            "manifest_digest": manifest_digest,
            "runtime_image": runtime_image,
        }

    def _prepare(self, context: dict, request_id: str, *, apply: bool):
        return self.client.post(
            f"/v1/admin/deployments/{context['deployment']['id']}/prepare",
            headers=self.admin,
            json={"request_id": request_id, "apply": apply},
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
            result.update(
                model_id=payload["model_id"],
                manifest_digest=payload["manifest_digest"],
                cache_kind="FULL_SNAPSHOT",
                verification_version=MODEL_CACHE_VERIFICATION_VERSION,
                bytes_verified=sum(
                    item["size_bytes"] for item in context["manifest"]["files"]
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
        image_task = self._claim(context["enrolled"][0])
        self.assertEqual(image_task["type"], "PREPARE_IMAGE")
        self._complete(image_task, context["enrolled"][0], context)

        shown = self.client.get(
            f"/v1/admin/deployment-preparations/{preparation_id}",
            headers=self.admin,
        )
        self.assertEqual(shown.status_code, 200, shown.text)
        self.assertEqual(shown.json()["preparation"]["status"], "SUCCEEDED")

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
        self.assertFalse(apply_task["payload"]["accept_model_download"])
        self.assertFalse(apply_task["payload"]["pull_image"])

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
        self.assertEqual(
            node_details[ordered[1]["node_id"]]["model_failure_code"],
            "PREPARATION_EXECUTION_FAILED",
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
        retried_image = self._claim(ordered[2])
        self._complete(retried_image, ordered[2], context)
        completed = self.client.get(
            f"/v1/admin/deployment-preparations/{preparation_id}",
            headers=self.admin,
        )
        self.assertEqual(
            completed.json()["preparation"]["status"], "SUCCEEDED"
        )
        self.assertNotIn(secret, completed.text)

    def test_missing_manifest_is_rejected_without_tasks_or_preparation(self):
        context = self._seed_accepted_generation(
            "missing-manifest", register_manifest=False
        )

        response = self._prepare(context, str(uuid.uuid4()), apply=False)

        self._failure_code(response, "PREPARATION_MANIFEST_REQUIRED")
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
        )
        node_id = disk_context["enrolled"][0]["node_id"]
        disk_response = self._prepare(
            disk_context, str(uuid.uuid4()), apply=False
        )
        self._failure_code(
            disk_response, "PREPARATION_DISK_INSUFFICIENT"
        )

        # A separate accepted generation is not needed for the active-task
        # boundary: the oversized generation's node remains otherwise valid.
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
            disk_context, str(uuid.uuid4()), apply=False
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
                lineage_id=disk_context["deployment"]["id"],
                deployment_id=disk_context["deployment"]["id"],
                kind="APPLY",
                status="PARTIAL_FAILED",
                phase="APPLY",
                node_ids=[node_id],
                serve=False,
                api=False,
                active_lineage_id=disk_context["deployment"]["id"],
            )
            session.add(operation)
            session.commit()

        operation_response = self._prepare(
            disk_context, str(uuid.uuid4()), apply=False
        )
        self._failure_code(operation_response, "PREPARATION_NODE_BUSY")
        self.assertEqual(
            operation_response.json()["detail"]["details"],
            {"operation_id": operation_id, "node_ids": [node_id]},
        )


if __name__ == "__main__":
    unittest.main()

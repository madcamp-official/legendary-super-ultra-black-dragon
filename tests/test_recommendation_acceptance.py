from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import Mock

from fastapi.testclient import TestClient
from sqlalchemy import func, select

from dure.control.api import create_app
from dure.control.models import (
    AuditEvent,
    BenchmarkRun,
    Deployment,
    DeploymentOperation,
    DeploymentRecommendationRecord,
    ModelRelease,
    Node,
    NodeProfileRecord,
    PlacementProfileRecord,
    RuntimeRelease,
    StageArtifactRank,
    Task,
    utcnow,
)
from dure.control.stage_artifacts import transition_stage_artifact_variant
from dure.control.recommendation import (
    RecommendationNotAcceptableError,
    _lock_recommendation_inputs,
    _ray_head_ip,
)
from dure.model_cache import (
    MODEL_CACHE_KIND_FULL_SNAPSHOT,
    MODEL_CACHE_KIND_STAGE,
)
from dure.models import (
    VLLM_RAY_PP_BACKEND,
    VLLM_RAY_PP_RUNTIME_VERSION,
    DeploymentPlan,
)
from dure.task import TaskStatus, TaskType

from .helpers import profile
from .test_recommendation import (
    PIPELINE_OVERRIDES,
    _add_node,
    _add_release,
    _register_validated_stage_variant,
    _source_manifest,
)


class RecommendationAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        url = f"sqlite:///{Path(self.temporary.name) / 'recommendation-accept.db'}"
        self.client = TestClient(
            create_app(
                database_url=url,
                admin_token="admin-secret",
                create_schema=True,
            )
        )
        self.factory = self.client.app.state.session_factory
        self.admin = {"Authorization": "Bearer admin-secret"}

    def tearDown(self):
        self.client.close()
        self.temporary.cleanup()

    def _seed_active_candidate(
        self,
        key: str = "accept",
        *,
        quality_rank: int = 10,
    ) -> tuple[str, str, str]:
        with self.factory() as session:
            node = _add_node(session, f"node-{key}", now=utcnow())
            release, placement = _add_release(
                session,
                key,
                quality_rank=quality_rank,
                evidence_nodes=[node],
            )
            return node.id, release.id, placement.id

    def _seed_pipeline_candidate(
        self,
        key: str,
        *,
        addresses: list[str] | None = None,
        extra_healthy_gpu: bool = False,
        vllm_version: str = VLLM_RAY_PP_RUNTIME_VERSION,
        disk_free_mib: int | None = None,
        source_manifest: dict | None = None,
    ) -> tuple[list[str], str, str]:
        with self.factory() as session:
            now = utcnow()
            nodes = [_add_node(session, f"node-{key}-{index}", now=now) for index in range(3)]
            ordered_nodes = sorted(nodes, key=lambda item: item.id)
            runtime_addresses = addresses or ["10.0.0.9", "10.0.0.2", "10.0.0.11"]
            for index, (node, address) in enumerate(
                zip(ordered_nodes, runtime_addresses)
            ):
                record = session.get(NodeProfileRecord, node.id)
                changed = copy.deepcopy(record.profile)
                changed["network"]["addresses"] = [address]
                changed["network"]["default_interface_addresses"] = [address]
                if disk_free_mib is not None:
                    changed["disk_free_mib"] = disk_free_mib
                if extra_healthy_gpu and index == 1:
                    second_gpu = copy.deepcopy(changed["gpus"][0])
                    second_gpu["index"] = 1
                    second_gpu["uuid"] += "-second"
                    changed["gpus"].append(second_gpu)
                record.profile = changed
                record.updated_at = now
            session.commit()
            release, placement = _add_release(
                session,
                key,
                quality_rank=100,
                placement_overrides={
                    **PIPELINE_OVERRIDES,
                    **(
                        {"min_disk_free_mib": 64}
                        if source_manifest is not None
                        else {}
                    ),
                },
                evidence_nodes=nodes,
                source_manifest=source_manifest,
            )
            if vllm_version != VLLM_RAY_PP_RUNTIME_VERSION:
                runtime = session.get(RuntimeRelease, release.runtime_id)
                runtime.vllm_version = vllm_version
                session.commit()
            return [node.id for node in ordered_nodes], release.id, placement.id

    def _seed_stage_pipeline_candidate(
        self,
        key: str,
        *,
        disk_free_mib: int = 100,
    ) -> tuple[list[str], str, str, dict]:
        node_ids, release_id, placement_id = self._seed_pipeline_candidate(
            key,
            disk_free_mib=disk_free_mib,
            source_manifest=_source_manifest(
                weight_size_bytes=8 * 1024 * 1024 * 1024
            ),
        )
        with self.factory() as session:
            release = session.get(ModelRelease, release_id)
            placement = session.get(PlacementProfileRecord, placement_id)
            assert release is not None
            assert placement is not None
            variant = _register_validated_stage_variant(
                session,
                release=release,
                placement=placement,
            )
        return node_ids, release_id, placement_id, variant

    def _recommend(self, node_id: str) -> dict:
        return self._recommend_nodes([node_id])

    def _recommend_nodes(self, node_ids: list[str]) -> dict:
        response = self.client.post(
            "/v1/admin/deployment-recommendations",
            headers=self.admin,
            json={
                "node_ids": node_ids,
                "all_online": False,
                "objective": "quality-first",
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()["recommendation"]

    def _accept(
        self,
        recommendation_id: str,
        *,
        previous_generation_id: str | None = None,
    ):
        body = {}
        if previous_generation_id is not None:
            body["previous_generation_id"] = previous_generation_id
        return self.client.post(
            f"/v1/admin/deployment-recommendations/{recommendation_id}/accept",
            headers=self.admin,
            json=body,
        )

    def _change_profile(self, node_id: str, disk_free_mib: int) -> None:
        with self.factory() as session:
            record = session.get(NodeProfileRecord, node_id)
            changed = copy.deepcopy(record.profile)
            changed["disk_free_mib"] = disk_free_mib
            record.profile = changed
            record.updated_at = utcnow()
            session.commit()

    def assert_error_code(self, response, status_code: int, code: str) -> None:
        self.assertEqual(response.status_code, status_code, response.text)
        self.assertEqual(response.json()["detail"]["code"], code)

    def test_repeated_recommendation_persists_one_snapshot_without_execution_rows(self):
        node_id, _, _ = self._seed_active_candidate("repeat")

        first = self._recommend(node_id)
        second = self._recommend(node_id)

        self.assertEqual(first, second)
        with self.factory() as session:
            self.assertEqual(
                session.scalar(
                    select(func.count()).select_from(DeploymentRecommendationRecord)
                ),
                1,
            )
            for model in (Deployment, Task, BenchmarkRun):
                self.assertEqual(
                    session.scalar(select(func.count()).select_from(model)),
                    0,
                    model.__tablename__,
                )

    def test_show_requires_authentication_and_returns_404_for_unknown_snapshot(self):
        node_id, _, _ = self._seed_active_candidate("show")
        recommendation = self._recommend(node_id)
        path = f"/v1/admin/deployment-recommendations/{recommendation['id']}"

        self.assertEqual(self.client.get(path).status_code, 401)
        self.assertEqual(
            self.client.post(path + "/accept", json={}).status_code,
            401,
        )
        shown = self.client.get(path, headers=self.admin)
        self.assertEqual(shown.status_code, 200, shown.text)
        self.assertEqual(shown.json()["recommendation"], recommendation)
        self.assertIsNone(shown.json()["deployment"])

        missing_id = "sha256:" + "f" * 64
        missing_path = f"/v1/admin/deployment-recommendations/{missing_id}"
        self.assert_error_code(
            self.client.get(missing_path, headers=self.admin),
            404,
            "RECOMMENDATION_NOT_FOUND",
        )
        self.assert_error_code(
            self.client.post(missing_path + "/accept", headers=self.admin, json={}),
            404,
            "RECOMMENDATION_NOT_FOUND",
        )

    def test_legacy_policy_snapshot_remains_readable_but_cannot_bypass_revalidation(self):
        node_id, _, _ = self._seed_active_candidate("legacy-policy")
        current = self._recommend(node_id)
        with self.factory() as session:
            current_record = session.get(DeploymentRecommendationRecord, current["id"])
            legacy = copy.deepcopy(current)
            legacy["policy_version"] = "central-quality-within-slo-v3"
            core = dict(legacy)
            core.pop("id")
            legacy_id = "sha256:" + hashlib.sha256(
                json.dumps(
                    core,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                ).encode("utf-8")
            ).hexdigest()
            legacy["id"] = legacy_id
            session.add(
                DeploymentRecommendationRecord(
                    id=legacy_id,
                    objective=current_record.objective,
                    selection_mode=current_record.selection_mode,
                    requested_node_ids=current_record.requested_node_ids,
                    catalog_version=current_record.catalog_version,
                    policy_version="central-quality-within-slo-v3",
                    inventory_fingerprint=current_record.inventory_fingerprint,
                    recommendation_snapshot=legacy,
                    inventory_snapshot=current_record.inventory_snapshot,
                )
            )
            session.commit()

        shown = self.client.get(
            f"/v1/admin/deployment-recommendations/{legacy_id}",
            headers=self.admin,
        )
        self.assertEqual(shown.status_code, 200, shown.text)
        self.assertEqual(
            shown.json()["recommendation"]["policy_version"],
            "central-quality-within-slo-v3",
        )
        denied = self._accept(legacy_id)
        self.assert_error_code(denied, 409, "RECOMMENDATION_STALE")

    def test_accept_creates_immutable_plan_with_safe_flags_and_no_execution_rows(self):
        node_id, release_id, placement_id = self._seed_active_candidate("success")
        recommendation = self._recommend(node_id)

        response = self._accept(recommendation["id"])

        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertTrue(body["created"])
        deployment = body["deployment"]
        self.assertEqual(deployment["source_recommendation_id"], recommendation["id"])
        self.assertEqual(deployment["lineage_id"], deployment["id"])
        self.assertIsNone(deployment["previous_generation_id"])
        self.assertEqual(deployment["generation"], 1)
        self.assertEqual(deployment["status"], "CREATED")
        self.assertFalse(deployment["accept_model_download"])
        self.assertFalse(deployment["pull_image"])
        plan = deployment["plan"]
        self.assertEqual(plan["deployment_id"], deployment["id"])
        self.assertEqual(plan["generation"], 1)
        self.assertEqual(plan["image"], recommendation["selected"]["runtime_image"])
        self.assertEqual(
            plan["model_revision"],
            recommendation["selected"]["artifact_revision"],
        )
        self.assertEqual(
            plan["model_path"],
            "/var/lib/dure/models/"
            f"{recommendation['selected']['model_id']}--"
            f"{recommendation['selected']['artifact_revision']}",
        )
        self.assertEqual(
            [item["node_id"] for item in plan["assignments"]],
            recommendation["selected"]["node_ids"],
        )
        self.assertEqual(
            recommendation["selected"]["model_cache_kind"],
            MODEL_CACHE_KIND_FULL_SNAPSHOT,
        )
        self.assertEqual(
            recommendation["selected"]["full_snapshot_total_size_bytes"],
            8192 * 1024 * 1024,
        )
        self.assertEqual(
            recommendation["selected"]["full_snapshot_required_cache_bytes"],
            (8192 * 2 + 64) * 1024 * 1024,
        )
        self.assertEqual(plan["model_cache_kind"], MODEL_CACHE_KIND_FULL_SNAPSHOT)
        self.assertEqual(DeploymentPlan.from_dict(plan).to_dict(), plan)
        legacy_plan = copy.deepcopy(plan)
        legacy_plan.pop("model_cache_kind")
        self.assertNotIn(
            "model_cache_kind",
            DeploymentPlan.from_dict(legacy_plan).to_dict(),
        )

        self.assertEqual(recommendation["selected"]["model_release_id"], release_id)
        self.assertEqual(recommendation["selected"]["placement_id"], placement_id)
        shown = self.client.get(
            f"/v1/admin/deployment-recommendations/{recommendation['id']}",
            headers=self.admin,
        )
        self.assertEqual(shown.status_code, 200, shown.text)
        self.assertEqual(shown.json()["deployment"], deployment)

        frozen_plan = copy.deepcopy(plan)
        self._change_profile(node_id, 79000)
        with self.factory() as session:
            stored = session.get(Deployment, deployment["id"])
            self.assertEqual(stored.plan, frozen_plan)
            self.assertFalse(stored.accept_model_download)
            self.assertFalse(stored.pull_image)
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Task)),
                0,
            )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(BenchmarkRun)),
                0,
            )

    def test_accept_rejects_active_cache_quarantine_without_generation(self):
        node_id, _, _ = self._seed_active_candidate("quarantine-busy")
        recommendation = self._recommend(node_id)
        with self.factory() as session:
            session.add(
                Task(
                    bulk_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                    node_id=node_id,
                    type=TaskType.QUARANTINE_ARTIFACT_CACHE.value,
                    status=TaskStatus.QUEUED.value,
                    payload={
                        "node_id": node_id,
                        "cache_kind": MODEL_CACHE_KIND_FULL_SNAPSHOT,
                        "cache_identity_digest": "sha256:" + "f" * 64,
                    },
                )
            )
            session.commit()

        response = self._accept(recommendation["id"])

        self.assert_error_code(response, 409, "RECOMMENDATION_NODE_BUSY")
        with self.factory() as session:
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Deployment)),
                0,
            )

    def test_multinode_accept_binds_deterministic_strict_runtime_ranks_without_tasks(self):
        node_ids, release_id, placement_id = self._seed_pipeline_candidate("strict")
        recommendation = self._recommend_nodes(list(reversed(node_ids)))

        response = self._accept(recommendation["id"])

        self.assertEqual(response.status_code, 200, response.text)
        selected = recommendation["selected"]
        self.assertEqual(selected["execution_backend"], VLLM_RAY_PP_BACKEND)
        self.assertEqual(
            selected["runtime_vllm_version"], VLLM_RAY_PP_RUNTIME_VERSION
        )
        self.assertEqual(
            selected["model_cache_kind"], MODEL_CACHE_KIND_FULL_SNAPSHOT
        )
        self.assertEqual(selected["model_release_id"], release_id)
        self.assertEqual(selected["placement_id"], placement_id)
        plan = response.json()["deployment"]["plan"]
        self.assertEqual(plan["execution_backend"], VLLM_RAY_PP_BACKEND)
        self.assertEqual(
            plan["runtime_vllm_version"], VLLM_RAY_PP_RUNTIME_VERSION
        )
        self.assertEqual(plan["model_cache_kind"], MODEL_CACHE_KIND_FULL_SNAPSHOT)
        self.assertRegex(
            plan["model_path"],
            r"^/var/lib/dure/models/sha256-[0-9a-f]{64}$",
        )
        self.assertEqual(plan["ray_head_node_id"], node_ids[0])
        self.assertEqual(plan["ray_head_address"], "10.0.0.9:6379")
        self.assertEqual(
            [item["node_id"] for item in plan["assignments"]],
            [node_ids[0], node_ids[2], node_ids[1]],
        )
        self.assertEqual(
            [item["runtime_address"] for item in plan["assignments"]],
            ["10.0.0.9", "10.0.0.11", "10.0.0.2"],
        )
        self.assertEqual(
            [item["expected_runtime_rank"] for item in plan["assignments"]],
            [0, 1, 2],
        )
        self.assertEqual(DeploymentPlan.from_dict(plan).to_dict(), plan)
        with self.factory() as session:
            self.assertEqual(session.scalar(select(func.count()).select_from(Task)), 0)

    def test_stage_accept_freezes_exact_variant_loader_manifests_and_rank_binding(self):
        node_ids, release_id, placement_id, variant = (
            self._seed_stage_pipeline_candidate("stage-accept")
        )
        recommendation = self._recommend_nodes(list(reversed(node_ids)))

        response = self._accept(recommendation["id"])

        self.assertEqual(response.status_code, 200, response.text)
        selected = recommendation["selected"]
        self.assertEqual(selected["model_release_id"], release_id)
        self.assertEqual(selected["placement_id"], placement_id)
        self.assertEqual(selected["model_cache_kind"], MODEL_CACHE_KIND_STAGE)
        self.assertEqual(
            selected["stage_artifact"]["artifact_set_digest"],
            variant["artifact_set_digest"],
        )
        self.assertEqual(selected["stage_validation_evidence"]["status"], "PASSED")
        plan = response.json()["deployment"]["plan"]
        self.assertEqual(plan["model_cache_kind"], MODEL_CACHE_KIND_STAGE)
        self.assertEqual(plan["model_path"], "/var/lib/dure/models/stages")
        self.assertEqual(plan["stage_artifact"], selected["stage_artifact"])
        self.assertEqual(
            [item["node_id"] for item in plan["assignments"]],
            selected["rank_node_ids"],
        )
        self.assertEqual(
            [item["stage_manifest_digest"] for item in plan["assignments"]],
            [item["manifest_digest"] for item in selected["stage_node_bindings"]],
        )
        self.assertEqual(
            [item["stage_tensor_keys_digest"] for item in plan["assignments"]],
            [
                item["tensor_keys_digest"]
                for item in selected["stage_node_bindings"]
            ],
        )
        self.assertEqual(DeploymentPlan.from_dict(plan).to_dict(), plan)
        with self.factory() as session:
            stored = session.get(DeploymentRecommendationRecord, recommendation["id"])
            self.assertEqual(
                stored.recommendation_snapshot["selected"]["stage_node_bindings"],
                selected["stage_node_bindings"],
            )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Task)), 0
            )

    def test_stage_revoke_between_recommend_and_accept_fails_closed_without_tasks(self):
        node_ids, _, _, variant = self._seed_stage_pipeline_candidate("stage-revoke")
        recommendation = self._recommend_nodes(node_ids)
        self.assertEqual(
            recommendation["selected"]["model_cache_kind"],
            MODEL_CACHE_KIND_STAGE,
        )
        with self.factory() as session:
            transition_stage_artifact_variant(
                session, variant["artifact_set_digest"], "REVOKED"
            )

        response = self._accept(recommendation["id"])

        self.assert_error_code(response, 409, "RECOMMENDATION_STALE")
        with self.factory() as session:
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Deployment)), 0
            )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Task)), 0
            )

    def test_stage_rank_mutation_between_recommend_and_accept_fails_closed(self):
        node_ids, _, _, variant = self._seed_stage_pipeline_candidate(
            "stage-rank-change"
        )
        recommendation = self._recommend_nodes(node_ids)
        with self.factory() as session:
            rank = session.scalar(
                select(StageArtifactRank)
                .where(
                    StageArtifactRank.variant_id
                    == variant["artifact_set_digest"]
                )
                .order_by(StageArtifactRank.rank)
            )
            assert rank is not None
            rank.weight_size_bytes += 1
            session.commit()

        response = self._accept(recommendation["id"])

        self.assert_error_code(response, 409, "RECOMMENDATION_STALE")
        with self.factory() as session:
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Deployment)), 0
            )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Task)), 0
            )

    def test_multinode_recommendation_rejects_unsupported_runtime_topologies(self):
        cases = (
            (
                "public-ip",
                {
                    "addresses": ["10.0.0.9", "203.0.113.20", "10.0.0.11"],
                },
                "STRICT_NETWORK",
            ),
            (
                "duplicate-ip",
                {
                    "addresses": ["10.0.0.9", "10.0.0.2", "10.0.0.2"],
                },
                "STRICT_NETWORK",
            ),
            (
                "two-gpus",
                {"extra_healthy_gpu": True},
                "STRICT_GPU_TOPOLOGY",
            ),
            (
                "wrong-vllm",
                {"vllm_version": "0.9.1"},
                "STRICT_RUNTIME_VERSION",
            ),
        )
        for key, options, expected_code in cases:
            with self.subTest(key=key):
                node_ids, _, placement_id = self._seed_pipeline_candidate(
                    key, **options
                )
                recommendation = self._recommend_nodes(node_ids)
                self.assertIsNone(recommendation["selected"])
                candidate = next(
                    item
                    for item in recommendation["candidates"]
                    if item["placement_id"] == placement_id
                )
                codes = {
                    item["code"]
                    for item in candidate["rejections"]
                }
                self.assertIn(expected_code, codes)
                response = self._accept(recommendation["id"])
                self.assert_error_code(
                    response, 409, "RECOMMENDATION_NOT_FEASIBLE"
                )

    def test_postgresql_accept_locks_evidence_registry_and_inventory_tables(self):
        session = Mock()
        session.get_bind.return_value.dialect.name = "postgresql"
        session.scalars.return_value = []
        record = Mock(selection_mode="all_online", requested_node_ids=[])

        _lock_recommendation_inputs(session, record)

        statement = str(session.execute.call_args.args[0])
        self.assertEqual(
            statement,
            "LOCK TABLE artifact_chunks, artifact_manifests, "
            "artifact_manifest_files, artifact_file_chunks, benchmark_evidence, "
            "benchmark_runs, model_artifacts, model_releases, nodes, "
            "node_profiles, placement_profiles, runtime_releases, "
            "stage_artifact_variants, stage_artifact_ranks, "
            "stage_artifact_validation_evidence, "
            "stage_artifact_validation_ranks IN SHARE MODE",
        )
        self.assertLess(
            statement.index("artifact_chunks"),
            statement.index("artifact_manifests"),
        )
        self.assertLess(
            statement.index("artifact_manifests"),
            statement.index("artifact_manifest_files"),
        )
        self.assertLess(
            statement.index("artifact_manifest_files"),
            statement.index("artifact_file_chunks"),
        )
        self.assertLess(
            statement.index("stage_artifact_variants"),
            statement.index("stage_artifact_ranks"),
        )
        self.assertLess(
            statement.index("stage_artifact_ranks"),
            statement.index("stage_artifact_validation_evidence"),
        )
        session.scalars.assert_not_called()

    def test_multinode_ray_head_rejects_public_only_address(self):
        public_only = profile("public-only", address="203.0.113.10")

        with self.assertRaises(RecommendationNotAcceptableError) as raised:
            _ray_head_ip(public_only, multi_node=True)

        self.assertEqual(raised.exception.code, "GENERATION_NETWORK_UNSUPPORTED")
        self.assertEqual(_ray_head_ip(public_only, multi_node=False), "127.0.0.1")

    def test_accept_rejects_changed_profile_content(self):
        node_id, _, _ = self._seed_active_candidate("changed-profile")
        recommendation = self._recommend(node_id)
        self._change_profile(node_id, 79000)

        response = self._accept(recommendation["id"])

        self.assert_error_code(response, 409, "RECOMMENDATION_STALE")

    def test_accept_rejects_stale_profile(self):
        node_id, _, _ = self._seed_active_candidate("stale-profile")
        recommendation = self._recommend(node_id)
        with self.factory() as session:
            record = session.get(NodeProfileRecord, node_id)
            record.updated_at = utcnow() - timedelta(seconds=91)
            session.commit()

        response = self._accept(recommendation["id"])

        self.assert_error_code(response, 409, "RECOMMENDATION_STALE")

    def test_accept_rejects_node_approval_change(self):
        node_id, _, _ = self._seed_active_candidate("approval")
        recommendation = self._recommend(node_id)
        with self.factory() as session:
            node = session.get(Node, node_id)
            node.approved = False
            session.commit()

        response = self._accept(recommendation["id"])

        self.assert_error_code(response, 409, "RECOMMENDATION_STALE")

    def test_all_online_accept_rejects_newly_available_node(self):
        self._seed_active_candidate("all-online")
        response = self.client.post(
            "/v1/admin/deployment-recommendations",
            headers=self.admin,
            json={"all_online": True, "objective": "quality-first"},
        )
        self.assertEqual(response.status_code, 200, response.text)
        recommendation = response.json()["recommendation"]

        with self.factory() as session:
            _add_node(session, "node-added-later", now=utcnow())

        stale = self._accept(recommendation["id"])
        self.assert_error_code(stale, 409, "RECOMMENDATION_STALE")

    def test_accept_rejects_catalog_change_even_when_selected_candidate_is_same(self):
        node_id, selected_release_id, _ = self._seed_active_candidate(
            "catalog-selected", quality_rank=20
        )
        recommendation = self._recommend(node_id)
        with self.factory() as session:
            node = session.get(Node, node_id)
            _add_release(
                session,
                "catalog-lower",
                quality_rank=1,
                evidence_nodes=[node],
            )

        current = self._recommend(node_id)
        self.assertEqual(
            current["selected"]["model_release_id"],
            selected_release_id,
        )
        self.assertNotEqual(current["catalog_version"], recommendation["catalog_version"])
        response = self._accept(recommendation["id"])
        self.assert_error_code(response, 409, "RECOMMENDATION_STALE")

    def test_accept_rejects_changed_or_missing_selected_candidate(self):
        node_id, selected_release_id, _ = self._seed_active_candidate(
            "selection", quality_rank=10
        )
        recommendation = self._recommend(node_id)
        with self.factory() as session:
            node = session.get(Node, node_id)
            higher, _ = _add_release(
                session,
                "selection-higher",
                quality_rank=100,
                evidence_nodes=[node],
            )

        current = self._recommend(node_id)
        self.assertEqual(current["selected"]["model_release_id"], higher.id)
        self.assertNotEqual(higher.id, selected_release_id)
        self.assert_error_code(
            self._accept(recommendation["id"]),
            409,
            "RECOMMENDATION_STALE",
        )

        with self.factory() as session:
            empty_node = _add_node(
                session,
                "node-empty",
                now=utcnow(),
                stored_profile=None,
            )
        empty = self._recommend(empty_node.id)
        self.assertIsNone(empty["selected"])
        self.assert_error_code(
            self._accept(empty["id"]),
            409,
            "RECOMMENDATION_NOT_FEASIBLE",
        )

    def test_same_accept_is_idempotent_and_writes_one_audit_event(self):
        node_id, _, _ = self._seed_active_candidate("idempotent")
        recommendation = self._recommend(node_id)
        with self.factory() as session:
            audit_before = session.scalar(select(func.count()).select_from(AuditEvent))
            accept_audit_before = session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.action == "recommendation.accept")
            )

        first = self._accept(recommendation["id"])
        second = self._accept(recommendation["id"])

        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual(second.status_code, 200, second.text)
        self.assertTrue(first.json()["created"])
        self.assertFalse(second.json()["created"])
        self.assertEqual(
            first.json()["deployment"]["id"],
            second.json()["deployment"]["id"],
        )
        with self.factory() as session:
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Deployment)),
                1,
            )
            self.assertEqual(
                session.scalar(select(func.count()).select_from(AuditEvent)),
                audit_before + 1,
            )
            self.assertEqual(
                session.scalar(
                    select(func.count())
                    .select_from(AuditEvent)
                    .where(AuditEvent.action == "recommendation.accept")
                ),
                accept_audit_before + 1,
            )

    def test_previous_generation_builds_linear_chain_and_rejects_conflicts(self):
        node_id, _, _ = self._seed_active_candidate("chain")
        first_recommendation = self._recommend(node_id)
        first = self._accept(first_recommendation["id"])
        self.assertEqual(first.status_code, 200, first.text)
        first_deployment = first.json()["deployment"]

        self._change_profile(node_id, 79000)
        second_recommendation = self._recommend(node_id)
        second = self._accept(
            second_recommendation["id"],
            previous_generation_id=first_deployment["id"],
        )
        self.assertEqual(second.status_code, 200, second.text)
        second_deployment = second.json()["deployment"]
        self.assertEqual(second_deployment["lineage_id"], first_deployment["lineage_id"])
        self.assertEqual(
            second_deployment["previous_generation_id"],
            first_deployment["id"],
        )
        self.assertEqual(second_deployment["generation"], 2)

        different_previous = self._accept(second_recommendation["id"])
        self.assert_error_code(
            different_previous,
            409,
            "RECOMMENDATION_ALREADY_ACCEPTED",
        )

        self._change_profile(node_id, 78000)
        third_recommendation = self._recommend(node_id)
        stale_previous = self._accept(
            third_recommendation["id"],
            previous_generation_id=first_deployment["id"],
        )
        self.assert_error_code(
            stale_previous,
            409,
            "PREVIOUS_GENERATION_NOT_LATEST",
        )
        with self.factory() as session:
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Deployment)),
                2,
            )

    def test_rolled_back_latest_generation_cannot_continue_the_old_lineage(self):
        node_id, _, _ = self._seed_active_candidate("rolled-back-lineage")
        first_recommendation = self._recommend(node_id)
        first = self._accept(first_recommendation["id"])
        self.assertEqual(first.status_code, 200, first.text)
        first_deployment = first.json()["deployment"]
        with self.factory() as session:
            deployment = session.get(Deployment, first_deployment["id"])
            deployment.status = "ROLLED_BACK"
            deployment.verified_at = None
            session.commit()

        self._change_profile(node_id, 79000)
        second_recommendation = self._recommend(node_id)
        response = self._accept(
            second_recommendation["id"],
            previous_generation_id=first_deployment["id"],
        )

        self.assert_error_code(
            response,
            409,
            "PREVIOUS_GENERATION_ROLLED_BACK",
        )

    def test_legacy_lineage_mutation_blocks_accepting_the_next_generation(self):
        node_id, _, _ = self._seed_active_candidate("legacy-mutation-lineage")
        first_recommendation = self._recommend(node_id)
        first = self._accept(first_recommendation["id"])
        self.assertEqual(first.status_code, 200, first.text)
        first_deployment = first.json()["deployment"]
        with self.factory() as session:
            session.add(
                Task(
                    bulk_id="legacy-lineage-mutation",
                    node_id=first_deployment["plan"]["assignments"][0]["node_id"],
                    type="START_DEPLOYMENT",
                    deployment_id=first_deployment["id"],
                    payload={},
                )
            )
            session.commit()

        self._change_profile(node_id, 79000)
        second_recommendation = self._recommend(node_id)
        response = self._accept(
            second_recommendation["id"],
            previous_generation_id=first_deployment["id"],
        )

        self.assert_error_code(response, 409, "DEPLOYMENT_MUTATION_ACTIVE")

    def test_accept_rejects_a_new_generation_while_lineage_operation_is_active(self):
        node_id, _, _ = self._seed_active_candidate("active-operation")
        first_recommendation = self._recommend(node_id)
        first = self._accept(first_recommendation["id"])
        self.assertEqual(first.status_code, 200, first.text)
        first_deployment = first.json()["deployment"]

        self._change_profile(node_id, 79000)
        second_recommendation = self._recommend(node_id)
        with self.factory() as session:
            session.add(
                DeploymentOperation(
                    request_digest="sha256:" + "a" * 64,
                    lineage_id=first_deployment["lineage_id"],
                    deployment_id=first_deployment["id"],
                    kind="VERIFY",
                    status="RUNNING",
                    phase="VERIFY",
                    node_ids=[node_id],
                    serve=False,
                    api=False,
                    active_lineage_id=first_deployment["lineage_id"],
                )
            )
            session.commit()

        response = self._accept(
            second_recommendation["id"],
            previous_generation_id=first_deployment["id"],
        )

        self.assert_error_code(response, 409, "DEPLOYMENT_OPERATION_ACTIVE")
        with self.factory() as session:
            self.assertEqual(
                session.scalar(select(func.count()).select_from(Deployment)),
                1,
            )

    def test_accept_body_forbids_extra_fields_and_wrong_types(self):
        node_id, _, _ = self._seed_active_candidate("strict")
        recommendation = self._recommend(node_id)
        path = (
            f"/v1/admin/deployment-recommendations/{recommendation['id']}/accept"
        )

        for body in (
            {"apply": True},
            {"previous_generation_id": None, "pull_image": True},
            {"previous_generation_id": 123},
            {"previous_generation_id": ""},
            {"previous_generation_id": "x" * 256},
        ):
            with self.subTest(body=body):
                response = self.client.post(path, headers=self.admin, json=body)
                self.assertEqual(response.status_code, 422, response.text)


if __name__ == "__main__":
    unittest.main()

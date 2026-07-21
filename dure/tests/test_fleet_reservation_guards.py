from __future__ import annotations

import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock

from sqlalchemy import select

from dure.control.db import Base, make_engine, make_session_factory
from dure.control.models import (
    ArtifactManifest,
    Deployment,
    DeploymentOperation,
    FleetRecommendationRecord,
    FleetRecord,
    FleetResourceReservation,
    Node,
    NodeArtifactCache,
    NodeProfileRecord,
    PlacementProfileRecord,
    Task,
    TaskType,
    utcnow,
)
from dure.control.preparation import (
    ArtifactPreparationError,
    prepare_deployment_artifacts,
)
from dure.control.qualification import (
    ProfileQualificationError,
    _qualification_occupancy,
    prepare_profile_qualification,
)
from dure.control.resource_reservation import (
    FLEET_RESERVATION_ADVISORY_LOCK_KEY,
    FleetResourceReservationError,
    ensure_fleet_reservation_scope,
    lock_fleet_reservation_gate,
)
from dure.control.rollout import DeploymentRolloutConflictError
from dure.control.recommendation import (
    RecommendationNotAcceptableError,
    accept_deployment_recommendation,
    recommend_deployment,
)
from dure.control.rollout import _activate_operation
from dure.control.service import (
    ArtifactCacheControlError,
    BenchmarkRunError,
    apply_benchmark_run,
    create_tasks,
    prepare_or_apply_artifact_cache_quarantine,
    prepare_benchmark_run,
)
from dure.selector import InventoryNode

from tests.helpers import profile
from tests.test_benchmark import _node as benchmark_node
from tests.test_benchmark import _release as benchmark_release
from tests.test_recommendation import _add_node as recommendation_node
from tests.test_recommendation import _add_release as recommendation_release
import tests.test_profile_qualification as qualification_fixtures
import tests.test_artifact_preparation_control as preparation_fixtures


class FleetReservationGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        database = Path(self.temporary.name) / "control.db"
        self.engine = make_engine(f"sqlite:///{database}")
        Base.metadata.create_all(self.engine)
        self.factory = make_session_factory(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        self.temporary.cleanup()

    @staticmethod
    def _reserve_existing_node(session, node_id: str, gpu_uuid: str):
        unique = uuid.uuid4().hex
        fleet_id = str(uuid.uuid4())
        deployment_id = str(uuid.uuid4())
        recommendation_id = "sha256:" + unique.ljust(64, "a")
        candidate_id = "sha256:" + unique.ljust(64, "b")
        recommendation = FleetRecommendationRecord(
            id=recommendation_id,
            schema_version=1,
            objective="quality-first",
            selection_mode="explicit_nodes",
            requested_node_ids=[node_id],
            minimum_replicas={},
            minimum_reserve_nodes=0,
            reserve_node_ids=[],
            inventory_fingerprint="sha256:" + unique.ljust(64, "c"),
            source_inventory_fingerprint="sha256:" + unique.ljust(64, "d"),
            catalog_version="sha256:" + unique.ljust(64, "e"),
            catalog_policy_version="catalog-v1",
            candidate_policy_version="candidate-v1",
            scheduler_version="scheduler-v1",
            recommendation_snapshot={},
        )
        fleet = FleetRecord(
            id=fleet_id,
            source_recommendation_id=recommendation_id,
            status="ACCEPTED",
        )
        plan = {
            "assignments": [
                {
                    "node_id": node_id,
                    "gpu_index": 0,
                    "gpu_uuid": gpu_uuid,
                    "rank": 0,
                }
            ]
        }
        deployment = Deployment(
            id=deployment_id,
            lineage_id=deployment_id,
            previous_generation_id=None,
            source_recommendation_id=None,
            fleet_id=fleet_id,
            fleet_candidate_id=candidate_id,
            generation=1,
            plan=plan,
            accept_model_download=False,
            pull_image=False,
            status="CREATED",
        )
        reservation = FleetResourceReservation(
            id=str(uuid.uuid4()),
            fleet_id=fleet_id,
            deployment_id=deployment_id,
            node_id=node_id,
            gpu_index=0,
            gpu_uuid=gpu_uuid,
            rank=0,
        )
        session.add(recommendation)
        session.flush()
        session.add(fleet)
        session.flush()
        session.add(deployment)
        session.flush()
        session.add(reservation)
        session.commit()
        return deployment, reservation

    @staticmethod
    def _add_gpu_node(session, name: str, gpu_uuid: str) -> Node:
        node = Node(
            install_id=f"install-{name}-{uuid.uuid4()}",
            display_name=name,
            hostname=name,
            agent_version="0.3.31",
            approved=True,
            last_seen=utcnow(),
        )
        session.add(node)
        session.flush()
        observed = profile(f"agent-reported-{name}").to_dict()
        observed["gpus"][0]["uuid"] = gpu_uuid
        session.add(
            NodeProfileRecord(
                node_id=node.id,
                profile=observed,
                updated_at=utcnow(),
            )
        )
        session.commit()
        return node

    @classmethod
    def _seed(cls, session):
        node_id = "11111111-1111-4111-8111-111111111111"
        gpu_uuid = f"GPU-{node_id}"
        node = Node(
            id=node_id,
            install_id="install-fleet-reservation-guard",
            display_name="fleet-node",
            hostname="fleet-node",
            agent_version="0.3.31",
            approved=True,
            last_seen=utcnow(),
        )
        session.add(node)
        session.commit()
        deployment, reservation = cls._reserve_existing_node(
            session, node_id, gpu_uuid
        )
        return node, deployment, reservation

    def test_exact_fleet_binding_is_valid_but_generic_runtime_is_closed(self):
        with self.factory() as session:
            node, deployment, _ = self._seed(session)
            ensure_fleet_reservation_scope(
                session,
                node_ids=[node.id],
                deployment=deployment,
            )

            with self.assertRaises(DeploymentRolloutConflictError) as raised:
                create_tasks(
                    session,
                    node_ids=[node.id],
                    task_type=TaskType.STOP_DEPLOYMENT,
                    deployment_id=deployment.id,
                    options={},
                )

            self.assertEqual(
                raised.exception.code,
                "FLEET_RUNTIME_NOT_AVAILABLE",
            )

    def test_non_fleet_task_is_rejected_on_an_active_reserved_node(self):
        with self.factory() as session:
            node, _, _ = self._seed(session)

            with self.assertRaises(DeploymentRolloutConflictError) as raised:
                create_tasks(
                    session,
                    node_ids=[node.id],
                    task_type=TaskType.PROBE,
                    deployment_id=None,
                    options={},
                )

            self.assertEqual(raised.exception.code, "FLEET_RESOURCE_RESERVED")
            self.assertEqual(raised.exception.details["node_ids"], [node.id])
            self.assertEqual(session.query(Task).count(), 0)

    def test_non_fleet_deployment_task_rejects_cross_node_gpu_uuid_collision(self):
        shared_gpu_uuid = "GPU-global-deployment-task-collision"
        with self.factory() as session:
            owner = self._add_gpu_node(
                session,
                "fleet-gpu-owner",
                shared_gpu_uuid,
            )
            target = self._add_gpu_node(
                session,
                "non-fleet-task-target",
                shared_gpu_uuid,
            )
            self._reserve_existing_node(
                session,
                owner.id,
                shared_gpu_uuid,
            )
            deployment_id = str(uuid.uuid4())
            deployment = Deployment(
                id=deployment_id,
                lineage_id=deployment_id,
                generation=1,
                plan={
                    "assignments": [
                        {
                            "node_id": target.id,
                            "gpu_index": 0,
                            "gpu_uuid": shared_gpu_uuid,
                            "rank": 0,
                        }
                    ]
                },
                accept_model_download=False,
                pull_image=False,
                status="CREATED",
            )
            session.add(deployment)
            session.commit()

            with self.assertRaises(DeploymentRolloutConflictError) as raised:
                create_tasks(
                    session,
                    node_ids=[target.id],
                    task_type=TaskType.STOP_DEPLOYMENT,
                    deployment_id=deployment.id,
                    options={},
                )

            self.assertEqual(raised.exception.code, "FLEET_RESOURCE_RESERVED")
            self.assertEqual(
                raised.exception.details["gpu_uuids"],
                [shared_gpu_uuid],
            )
            self.assertEqual(session.query(Task).count(), 0)

    def test_fleet_task_rejects_gpu_index_drift_in_its_own_reservation(self):
        with self.factory() as session:
            node, deployment, reservation = self._seed(session)
            reservation.gpu_index = 1
            session.commit()

            with self.assertRaises(FleetResourceReservationError) as raised:
                ensure_fleet_reservation_scope(
                    session,
                    node_ids=[node.id],
                    deployment=deployment,
                )

            self.assertEqual(
                raised.exception.code,
                "FLEET_DEPLOYMENT_RESERVATION_INVALID",
            )

    def test_fleet_scope_rejects_a_caller_supplied_gpu_outside_its_plan(self):
        with self.factory() as session:
            node, deployment, _ = self._seed(session)

            with self.assertRaises(FleetResourceReservationError) as raised:
                ensure_fleet_reservation_scope(
                    session,
                    node_ids=[node.id],
                    gpu_uuids=["GPU-outside-fleet-plan"],
                    deployment=deployment,
                )

            self.assertEqual(
                raised.exception.code,
                "FLEET_DEPLOYMENT_RESERVATION_INVALID",
            )
            self.assertEqual(
                raised.exception.details["gpu_uuids"],
                ["GPU-outside-fleet-plan"],
            )

    def test_qualification_occupancy_reports_the_active_fleet_owner(self):
        with self.factory() as session:
            node, deployment, _ = self._seed(session)
            inventory = [InventoryNode.local(profile(node.id))]

            reasons = _qualification_occupancy(session, inventory)

            self.assertEqual(
                reasons[node.id],
                "ACTIVE_FLEET_RESERVATION:"
                f"{deployment.fleet_id}:{deployment.id}",
            )

    def test_qualification_occupancy_maps_cross_node_gpu_uuid_collision(self):
        shared_gpu_uuid = "GPU-global-qualification-collision"
        with self.factory() as session:
            owner = self._add_gpu_node(
                session,
                "qualification-gpu-owner",
                shared_gpu_uuid,
            )
            target = self._add_gpu_node(
                session,
                "qualification-target",
                shared_gpu_uuid,
            )
            deployment, _ = self._reserve_existing_node(
                session,
                owner.id,
                shared_gpu_uuid,
            )
            observed = profile(target.id)
            observed.gpus[0].uuid = shared_gpu_uuid

            reasons = _qualification_occupancy(
                session,
                [InventoryNode.local(observed)],
            )

            self.assertEqual(
                reasons[target.id],
                "ACTIVE_FLEET_RESERVATION:"
                f"{deployment.fleet_id}:{deployment.id}",
            )

    def test_qualification_apply_rechecks_reservation_under_the_gate(self):
        helper = qualification_fixtures.ProfileQualificationTests(
            methodName="runTest"
        )
        with self.factory() as session:
            release = helper._release(session, "qwen2.5-7b-awq")
            placement = session.scalar(
                select(PlacementProfileRecord).where(
                    PlacementProfileRecord.release_id == release.id
                )
            )
            node_id = helper._nodes(session, 1)[0]
            self._reserve_existing_node(
                session,
                node_id,
                "GPU-qualification-reserved",
            )

            with self.assertRaises(ProfileQualificationError) as raised:
                prepare_profile_qualification(
                    session,
                    request_id=str(uuid.uuid4()),
                    placement_id=placement.id,
                    node_ids=[node_id],
                    apply=True,
                )

            self.assertEqual(
                raised.exception.code,
                "QUALIFICATION_NODE_INELIGIBLE",
            )
            self.assertEqual(
                raised.exception.details["nodes"][0]["reason"],
                "NODE_OCCUPIED",
            )

    def test_single_recommendation_accept_rejects_a_fleet_reserved_node(self):
        with self.factory() as session:
            node = recommendation_node(
                session,
                "single-reservation-guard",
                now=utcnow(),
            )
            recommendation_release(
                session,
                "single-reservation-guard",
                evidence_nodes=[node],
            )
            response = recommend_deployment(
                session,
                node_ids=[node.id],
                all_online=False,
            )
            recommendation_id = response["recommendation"]["id"]
            self._reserve_existing_node(
                session,
                node.id,
                "GPU-single-recommendation-reserved",
            )

            with self.assertRaises(RecommendationNotAcceptableError) as raised:
                accept_deployment_recommendation(session, recommendation_id)

            self.assertEqual(raised.exception.code, "FLEET_RESOURCE_RESERVED")

    def test_single_recommendation_accept_rejects_cross_node_gpu_uuid_collision(self):
        with self.factory() as session:
            target = recommendation_node(
                session,
                "single-global-gpu-collision",
                now=utcnow(),
            )
            recommendation_release(
                session,
                "single-global-gpu-collision",
                evidence_nodes=[target],
            )
            response = recommend_deployment(
                session,
                node_ids=[target.id],
                all_online=False,
            )
            recommendation_id = response["recommendation"]["id"]
            target_profile = session.get(NodeProfileRecord, target.id)
            shared_gpu_uuid = target_profile.profile["gpus"][0]["uuid"]
            owner = self._add_gpu_node(
                session,
                "single-recommendation-gpu-owner",
                shared_gpu_uuid,
            )
            self._reserve_existing_node(
                session,
                owner.id,
                shared_gpu_uuid,
            )

            with self.assertRaises(RecommendationNotAcceptableError) as raised:
                accept_deployment_recommendation(session, recommendation_id)

            self.assertEqual(raised.exception.code, "FLEET_RESOURCE_RESERVED")
            self.assertEqual(
                raised.exception.details["gpu_uuids"],
                [shared_gpu_uuid],
            )

    def test_benchmark_apply_rejects_a_fleet_reserved_node(self):
        with self.factory() as session:
            node = benchmark_node(session, "fleet-reservation-guard")
            _, _, release, placements = benchmark_release(
                session, "fleet-reservation-guard"
            )
            run, _ = prepare_benchmark_run(
                session,
                request_id=str(uuid.uuid4()),
                release_id=release.id,
                placement_id=placements[0].id,
                node_ids=[node.id],
                workload_id="short-chat-1k-128",
                dure_commit="d" * 40,
            )
            self._reserve_existing_node(
                session,
                node.id,
                "GPU-benchmark-reserved",
            )

            with self.assertRaises(BenchmarkRunError) as raised:
                apply_benchmark_run(session, run.request_id)

            self.assertEqual(raised.exception.code, "FLEET_RESOURCE_RESERVED")
            session.refresh(run)
            self.assertEqual(run.status, "PREPARED")
            self.assertIsNone(run.task_id)

    def test_benchmark_apply_rejects_cross_node_gpu_uuid_collision(self):
        with self.factory() as session:
            target = benchmark_node(session, "global-gpu-collision")
            _, _, release, placements = benchmark_release(
                session,
                "global-gpu-collision",
            )
            run, _ = prepare_benchmark_run(
                session,
                request_id=str(uuid.uuid4()),
                release_id=release.id,
                placement_id=placements[0].id,
                node_ids=[target.id],
                workload_id="short-chat-1k-128",
                dure_commit="d" * 40,
            )
            target_profile = session.get(NodeProfileRecord, target.id)
            shared_gpu_uuid = target_profile.profile["gpus"][0]["uuid"]
            owner = self._add_gpu_node(
                session,
                "benchmark-gpu-owner",
                shared_gpu_uuid,
            )
            self._reserve_existing_node(
                session,
                owner.id,
                shared_gpu_uuid,
            )

            with self.assertRaises(BenchmarkRunError) as raised:
                apply_benchmark_run(session, run.request_id)

            self.assertEqual(raised.exception.code, "FLEET_RESOURCE_RESERVED")
            self.assertEqual(
                raised.exception.details["gpu_uuids"],
                [shared_gpu_uuid],
            )
            session.refresh(run)
            self.assertEqual(run.status, "PREPARED")
            self.assertIsNone(run.task_id)

    def test_cache_quarantine_apply_rejects_a_fleet_reserved_node(self):
        with self.factory() as session:
            node, _, _ = self._seed(session)
            manifest_digest = "sha256:" + "9" * 64
            cache_id = str(uuid.uuid4())
            session.add(
                ArtifactManifest(
                    digest=manifest_digest,
                    schema_version=1,
                    model_artifact_id=None,
                    total_size_bytes=1,
                    file_count=1,
                    chunk_count=1,
                    canonical_json="{}",
                )
            )
            session.flush()
            session.add(
                NodeArtifactCache(
                    id=cache_id,
                    node_id=node.id,
                    cache_kind="FULL_SNAPSHOT",
                    cache_identity_digest=manifest_digest,
                    manifest_digest=manifest_digest,
                    source_manifest_digest=manifest_digest,
                    status="CORRUPT",
                    reason_code="PROBE_CORRUPT",
                    event_sequence=0,
                )
            )
            session.commit()

            with self.assertRaises(ArtifactCacheControlError) as raised:
                prepare_or_apply_artifact_cache_quarantine(
                    session,
                    cache_id,
                    apply=True,
                )

            self.assertEqual(raised.exception.code, "FLEET_RESOURCE_RESERVED")
            self.assertEqual(session.query(Task).count(), 0)

    def test_generic_prepare_rejects_fleet_deployment_runtime(self):
        with self.factory() as session:
            _, deployment, _ = self._seed(session)

            with self.assertRaises(ArtifactPreparationError) as raised:
                prepare_deployment_artifacts(
                    session,
                    deployment.id,
                    request_id=str(uuid.uuid4()),
                    apply=True,
                )

            self.assertEqual(
                raised.exception.code,
                "FLEET_RUNTIME_NOT_AVAILABLE",
            )
            self.assertEqual(session.query(Task).count(), 0)

    def test_artifact_preparation_apply_rejects_a_fleet_reserved_node(self):
        fixture = preparation_fixtures.ArtifactPreparationControlTests(
            methodName="runTest"
        )
        fixture.setUp()
        try:
            context = fixture._seed_accepted_generation(
                "fleet-prep-guard"
            )
            node_id = context["enrolled"][0]["node_id"]
            with fixture.factory() as session:
                self._reserve_existing_node(
                    session,
                    node_id,
                    "GPU-preparation-reserved",
                )

            response = fixture._prepare(
                context,
                str(uuid.uuid4()),
                apply=True,
            )

            self.assertEqual(response.status_code, 409, response.text)
            self.assertEqual(
                response.json()["detail"]["code"],
                "FLEET_RESOURCE_RESERVED",
            )
            with fixture.factory() as session:
                self.assertEqual(session.query(Task).count(), 0)
        finally:
            fixture.tearDown()

    def test_rollout_activation_rejects_foreign_fleet_reservations(self):
        with self.factory() as session:
            node, _, _ = self._seed(session)
            deployment_id = str(uuid.uuid4())
            deployment = Deployment(
                id=deployment_id,
                lineage_id=deployment_id,
                generation=1,
                plan={
                    "assignments": [
                        {
                            "node_id": node.id,
                            "gpu_index": 0,
                            "gpu_uuid": f"GPU-{node.id}",
                            "rank": 0,
                        }
                    ]
                },
                accept_model_download=False,
                pull_image=False,
                status="CREATED",
            )
            session.add(deployment)
            session.flush()
            operation = DeploymentOperation(
                id=str(uuid.uuid4()),
                request_digest="sha256:" + "f" * 64,
                lineage_id=deployment_id,
                deployment_id=deployment_id,
                rollback_target_id=None,
                kind="APPLY",
                status="PREPARED",
                phase="APPLY",
                node_ids=[node.id],
                serve=False,
                api=False,
                active_lineage_id=None,
            )
            session.add(operation)
            session.commit()

            with self.assertRaises(DeploymentRolloutConflictError) as raised:
                _activate_operation(session, operation)

            self.assertEqual(raised.exception.code, "FLEET_RESOURCE_RESERVED")

    def test_postgresql_gate_uses_one_transaction_advisory_lock(self):
        session = Mock()
        session.get_bind.return_value.dialect.name = "postgresql"

        lock_fleet_reservation_gate(session)

        session.execute.assert_called_once()
        statement = str(session.execute.call_args.args[0])
        self.assertEqual(
            " ".join(statement.split()),
            "SELECT pg_advisory_xact_lock(:lock_key)",
        )
        self.assertEqual(
            session.execute.call_args.args[1],
            {"lock_key": FLEET_RESERVATION_ADVISORY_LOCK_KEY},
        )

    def test_direct_non_fleet_scope_error_is_structured(self):
        with self.factory() as session:
            node, _, _ = self._seed(session)

            with self.assertRaises(FleetResourceReservationError) as raised:
                ensure_fleet_reservation_scope(
                    session,
                    node_ids=[node.id],
                )

            self.assertEqual(raised.exception.code, "FLEET_RESOURCE_RESERVED")
            self.assertEqual(
                raised.exception.details["reservations"][0]["node_id"],
                node.id,
            )


if __name__ == "__main__":
    unittest.main()

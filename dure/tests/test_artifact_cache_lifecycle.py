from __future__ import annotations

import unittest
import uuid
from datetime import timedelta

from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError, OperationalError

from dure.control.cache_lifecycle import (
    ArtifactCacheConflictError,
    ArtifactCacheIdentity,
    ArtifactCacheNotReadyError,
    ArtifactCacheStaleAttemptError,
    artifact_cache_projection,
    artifact_cache_reference_projection,
    complete_cache_quarantine,
    list_artifact_cache_projections,
    mark_stage_variant_revoked,
    ready_cache_projection,
    reconcile_probe_observations,
    record_preparation_success,
    record_verification_failure,
    request_cache_quarantine,
    require_ready_cache,
)
from dure.control.db import Base, make_engine, make_session_factory
from dure.control.models import (
    ArtifactCacheEvent,
    ArtifactManifest,
    ArtifactPreparation,
    ArtifactPreparationAttempt,
    ArtifactPreparationNode,
    Deployment,
    DeploymentOperation,
    DeploymentOperationNode,
    ModelArtifact,
    Node,
    NodeArtifactCache,
    RuntimeRelease,
    StageArtifactRank,
    StageArtifactVariant,
    Task,
    utcnow,
)
from dure.models import ArtifactCacheObservation
from dure.task import TaskStatus, TaskType


def _digest(character: str) -> str:
    return "sha256:" + character * 64


class ArtifactCacheLifecycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = make_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.factory = make_session_factory(self.engine)
        self.session = self.factory()
        self.now = utcnow()
        self.node = Node(
            install_id="cache-lifecycle-node",
            display_name="cache-lifecycle-node",
            hostname="cache-lifecycle-node",
            agent_version="0.3.20",
            approved=True,
            last_seen=self.now,
        )
        self.session.add(self.node)
        self.session.flush()

    def tearDown(self) -> None:
        self.session.close()
        self.engine.dispose()

    def _manifest(
        self,
        *,
        suffix: str,
        digest: str,
        model_bound: bool,
        size: int = 11,
        files: int = 2,
    ) -> ArtifactManifest:
        artifact = None
        if model_bound:
            artifact = ModelArtifact(
                model_id=f"cache-{suffix}",
                repository=f"Example/Cache-{suffix}",
                revision=(suffix[0].lower() if suffix else "a") * 40,
                manifest_digest=digest,
                quantization="awq",
                size_mib=1,
                default_max_model_len=1024,
                layer_count=1,
                license_id="apache-2.0",
            )
            self.session.add(artifact)
            self.session.flush()
        manifest = ArtifactManifest(
            digest=digest,
            schema_version=1,
            model_artifact_id=artifact.id if artifact else None,
            total_size_bytes=size,
            file_count=files,
            chunk_count=1,
            canonical_json="{}",
        )
        self.session.add(manifest)
        self.session.flush()
        return manifest

    def _attempt(
        self,
        *,
        suffix: str,
        identity: ArtifactCacheIdentity,
        attempt_no: int = 1,
        bytes_verified: int = 11,
        file_count: int = 2,
    ) -> tuple[ArtifactPreparationAttempt, ArtifactPreparationNode, Task]:
        deployment_id = f"cache-deployment-{suffix}"
        deployment = Deployment(
            id=deployment_id,
            lineage_id=deployment_id,
            generation=1,
            plan={"assignments": [{"node_id": self.node.id}]},
            status="CREATED",
        )
        self.session.add(deployment)
        self.session.flush()
        preparation = ArtifactPreparation(
            request_id=str(uuid.uuid4()),
            request_digest=_digest("1"),
            deployment_id=deployment_id,
            status="SUCCEEDED",
            plan_snapshot=(
                {
                    "artifact": {
                        "cache_kind": "FULL_SNAPSHOT",
                        "manifest_digest": identity.manifest_digest,
                    },
                    "node_ids": [self.node.id],
                }
                if identity.cache_kind == "FULL_SNAPSHOT"
                else {}
            ),
            completed_at=self.now,
        )
        self.session.add(preparation)
        self.session.flush()
        record = ArtifactPreparationNode(
            preparation_id=preparation.id,
            node_id=self.node.id,
            model_manifest_digest=identity.manifest_digest,
            runtime_image="registry.example/vllm@sha256:" + "9" * 64,
            model_status="SUCCEEDED",
            image_status="PREPARED",
            model_current_attempt=attempt_no,
            image_current_attempt=0,
        )
        self.session.add(record)
        self.session.flush()
        result = {
            "cache_kind": identity.cache_kind,
            "manifest_digest": identity.manifest_digest,
            "verification_version": identity.verification_version,
            "bytes_verified": bytes_verified,
            "file_count": file_count,
        }
        payload = dict(result)
        if identity.cache_kind == "STAGE":
            stage = {
                "artifact_set_digest": identity.artifact_set_digest,
                "pipeline_rank": identity.pipeline_rank,
                "tensor_rank": identity.tensor_rank,
                "tensor_keys_digest": identity.tensor_keys_digest,
                "cache_identity_digest": identity.cache_identity_digest,
            }
            result.update(stage)
            payload.update(
                stage,
                source_manifest_digest=identity.source_manifest_digest,
                tensor_parallel_size=identity.tensor_parallel_size,
                pipeline_parallel_size=identity.pipeline_parallel_size,
            )
        task = Task(
            bulk_id=preparation.id,
            node_id=self.node.id,
            type=TaskType.PREPARE_MODEL.value,
            status=TaskStatus.SUCCEEDED.value,
            deployment_id=deployment_id,
            payload=payload,
            attempts=1,
            result=result,
        )
        self.session.add(task)
        self.session.flush()
        attempt = ArtifactPreparationAttempt(
            preparation_node_id=record.id,
            stage="MODEL",
            attempt_no=attempt_no,
            task_id=task.id,
            status="SUCCEEDED",
            result=result,
            completed_at=self.now,
        )
        self.session.add(attempt)
        self.session.commit()
        return attempt, record, task

    def _full(self, suffix: str = "a") -> tuple[ArtifactCacheIdentity, ArtifactPreparationAttempt]:
        manifest = self._manifest(
            suffix=f"full-{suffix}", digest=_digest(suffix), model_bound=True
        )
        identity = ArtifactCacheIdentity(
            cache_kind="FULL_SNAPSHOT",
            cache_identity_digest=manifest.digest,
            manifest_digest=manifest.digest,
            source_manifest_digest=manifest.digest,
            verification_version=1,
        )
        attempt, _record, _task = self._attempt(
            suffix=f"full-{suffix}", identity=identity
        )
        return identity, attempt

    def _stage(self) -> tuple[ArtifactCacheIdentity, ArtifactPreparationAttempt, StageArtifactVariant]:
        source = self._manifest(
            suffix="stage-source", digest=_digest("b"), model_bound=True
        )
        stage = self._manifest(
            suffix="stage-rank", digest=_digest("c"), model_bound=False
        )
        runtime = RuntimeRelease(
            version="cache-stage-runtime",
            image="registry.example/vllm@sha256:" + "9" * 64,
            vllm_version="0.9.0",
            cuda_version="12.4",
            gpu_architectures=["ampere"],
        )
        self.session.add(runtime)
        self.session.flush()
        variant = StageArtifactVariant(
            artifact_set_digest=_digest("d"),
            contract_identity_digest=_digest("e"),
            source_manifest_digest=source.digest,
            runtime_release_id=runtime.id,
            runtime_image=runtime.image,
            vllm_version="0.9.0",
            exporter_build_digest=_digest("f"),
            architecture="Qwen2ForCausalLM",
            quantization="awq",
            tensor_parallel_size=1,
            pipeline_parallel_size=1,
            rank_count=1,
            loader_format="VLLM_SHARDED_STATE_V1",
            status="VALIDATED",
            canonical_identity_json="{}",
            validated_at=self.now,
        )
        self.session.add(variant)
        self.session.flush()
        rank = StageArtifactRank(
            variant_id=variant.artifact_set_digest,
            rank=0,
            pipeline_rank=0,
            tensor_rank=0,
            tensor_parallel_size=1,
            pipeline_parallel_size=1,
            manifest_digest=stage.digest,
            tensor_key_count=1,
            tensor_keys_digest=_digest("7"),
            weight_size_bytes=stage.total_size_bytes,
        )
        self.session.add(rank)
        self.session.flush()
        identity = ArtifactCacheIdentity(
            cache_kind="STAGE",
            cache_identity_digest=_digest("8"),
            manifest_digest=stage.digest,
            source_manifest_digest=source.digest,
            verification_version=1,
            artifact_set_digest=variant.artifact_set_digest,
            pipeline_rank=0,
            tensor_rank=0,
            tensor_parallel_size=1,
            pipeline_parallel_size=1,
            tensor_keys_digest=rank.tensor_keys_digest,
        )
        attempt, _record, _task = self._attempt(
            suffix="stage", identity=identity
        )
        return identity, attempt, variant

    def test_current_preparation_success_is_ready_and_exact_replay_is_idempotent(self):
        identity, attempt = self._full()

        cache, event, changed = record_preparation_success(
            self.session, attempt_id=attempt.id, identity=identity
        )
        self.session.commit()

        self.assertTrue(changed)
        self.assertEqual(cache.status, "READY")
        self.assertEqual(cache.last_ready_attempt_id, attempt.id)
        self.assertEqual(event.previous_status, None)
        self.assertEqual(event.sequence, 1)
        self.assertEqual(
            ready_cache_projection(
                self.session, node_id=self.node.id, identity=identity
            )["cache_identity_digest"],
            identity.cache_identity_digest,
        )

        record = self.session.get(
            ArtifactPreparationNode, attempt.preparation_node_id
        )
        record.model_current_attempt = 2
        record.model_status = "FAILED"
        self.session.commit()

        replay_cache, replay_event, replay_changed = record_preparation_success(
            self.session, attempt_id=attempt.id, identity=identity
        )
        self.session.commit()
        self.assertFalse(replay_changed)
        self.assertEqual(replay_cache.id, cache.id)
        self.assertEqual(replay_event.id, event.id)
        self.assertEqual(
            self.session.scalar(select(func.count()).select_from(ArtifactCacheEvent)),
            1,
        )

    def test_append_only_event_rejects_orm_and_raw_mutation(self):
        identity, attempt = self._full()
        _cache, event, _changed = record_preparation_success(
            self.session, attempt_id=attempt.id, identity=identity
        )
        self.session.commit()
        event_id = event.id
        original_source_id = event.source_id

        with self.assertRaisesRegex(
            OperationalError, "no such column: rowid"
        ):
            self.session.execute(
                text(
                    "SELECT rowid FROM artifact_cache_events "
                    "WHERE id = :event_id"
                ),
                {"event_id": event_id},
            )
        self.session.rollback()

        event.source_id = "orm-update-must-fail"
        with self.assertRaisesRegex(IntegrityError, "append-only"):
            self.session.commit()
        self.session.rollback()

        event = self.session.get(ArtifactCacheEvent, event_id)
        self.assertIsNotNone(event)
        self.session.delete(event)
        with self.assertRaisesRegex(IntegrityError, "append-only"):
            self.session.commit()
        self.session.rollback()

        with self.assertRaisesRegex(IntegrityError, "append-only"):
            self.session.execute(
                text(
                    "UPDATE artifact_cache_events "
                    "SET source_id = :source_id WHERE id = :event_id"
                ),
                {"source_id": "raw-update-must-fail", "event_id": event_id},
            )
        self.session.rollback()

        with self.assertRaisesRegex(IntegrityError, "append-only"):
            self.session.execute(
                text(
                    "INSERT OR REPLACE INTO artifact_cache_events ("
                    "id, cache_id, sequence, previous_status, status, "
                    "reason_code, source_kind, source_id, "
                    "source_attempt_id, source_task_id, evidence_kind, "
                    "evidence_digest, created_at) "
                    "SELECT id, cache_id, sequence, previous_status, status, "
                    "reason_code, source_kind, :source_id, "
                    "source_attempt_id, source_task_id, evidence_kind, "
                    "evidence_digest, created_at "
                    "FROM artifact_cache_events WHERE id = :event_id"
                ),
                {
                    "source_id": "raw-replace-must-fail",
                    "event_id": event_id,
                },
            )
        self.session.rollback()

        with self.assertRaisesRegex(IntegrityError, "append-only"):
            self.session.execute(
                text("DELETE FROM artifact_cache_events WHERE id = :event_id"),
                {"event_id": event_id},
            )
        self.session.rollback()

        preserved = self.session.get(ArtifactCacheEvent, event_id)
        self.assertIsNotNone(preserved)
        self.assertEqual(preserved.source_id, original_source_id)
        self.assertEqual(
            self.session.scalar(
                select(func.count()).select_from(ArtifactCacheEvent)
            ),
            1,
        )

    def test_stale_unrecorded_attempt_cannot_create_or_replace_ready(self):
        identity, attempt = self._full()
        record = self.session.get(
            ArtifactPreparationNode, attempt.preparation_node_id
        )
        record.model_current_attempt = 2
        self.session.commit()

        with self.assertRaises(ArtifactCacheStaleAttemptError):
            record_preparation_success(
                self.session, attempt_id=attempt.id, identity=identity
            )

        self.assertIsNone(
            self.session.scalar(select(NodeArtifactCache).limit(1))
        )

    def test_probe_is_fail_closed_prioritized_and_incomplete_scan_is_noop(self):
        identity, attempt = self._full()
        cache, _event, _changed = record_preparation_success(
            self.session, attempt_id=attempt.id, identity=identity
        )
        self.session.commit()
        corrupt = ArtifactCacheObservation(
            cache_kind="FULL_SNAPSHOT",
            cache_identity_digest=identity.cache_identity_digest,
            condition="CORRUPT",
        )

        self.assertEqual(
            reconcile_probe_observations(
                self.session,
                node_id=self.node.id,
                observations=[corrupt],
                scan_complete=False,
                source_id="scan-incomplete",
                observed_at=self.now + timedelta(seconds=1),
            ),
            [],
        )
        self.assertEqual(cache.status, "READY")

        present = ArtifactCacheObservation(
            cache_kind="FULL_SNAPSHOT",
            cache_identity_digest=identity.cache_identity_digest,
            condition="PRESENT",
            manifest_digest=identity.manifest_digest,
            verification_version=1,
        )
        self.assertEqual(
            reconcile_probe_observations(
                self.session,
                node_id=self.node.id,
                observations=[present],
                scan_complete=True,
                source_id="scan-present",
                observed_at=self.now + timedelta(seconds=2),
            ),
            [],
        )
        self.assertEqual(cache.status, "READY")

        mismatch = ArtifactCacheObservation(
            cache_kind="FULL_SNAPSHOT",
            cache_identity_digest=identity.cache_identity_digest,
            condition="IDENTITY_MISMATCH",
            manifest_digest=_digest("9"),
            verification_version=1,
        )
        reconcile_probe_observations(
            self.session,
            node_id=self.node.id,
            observations=[mismatch],
            scan_complete=True,
            source_id="scan-mismatch",
            observed_at=self.now + timedelta(seconds=3),
        )
        self.assertEqual(cache.status, "STALE")

        reconcile_probe_observations(
            self.session,
            node_id=self.node.id,
            observations=[],
            scan_complete=True,
            source_id="scan-missing",
            observed_at=self.now + timedelta(seconds=4),
        )
        self.assertEqual(cache.status, "MISSING")
        reconcile_probe_observations(
            self.session,
            node_id=self.node.id,
            observations=[corrupt],
            scan_complete=True,
            source_id="scan-corrupt",
            observed_at=self.now + timedelta(seconds=5),
        )
        self.assertEqual(cache.status, "CORRUPT")
        reconcile_probe_observations(
            self.session,
            node_id=self.node.id,
            observations=[mismatch],
            scan_complete=True,
            source_id="scan-lower-priority",
            observed_at=self.now + timedelta(seconds=6),
        )
        self.assertEqual(cache.status, "CORRUPT")
        self.assertEqual(cache.reason_code, "PROBE_CORRUPT")

    def test_older_or_equal_complete_probe_cannot_replace_newer_state_or_event(self):
        identity, attempt = self._full()
        cache, _event, _changed = record_preparation_success(
            self.session, attempt_id=attempt.id, identity=identity
        )
        self.session.commit()
        mismatch = ArtifactCacheObservation(
            cache_kind="FULL_SNAPSHOT",
            cache_identity_digest=identity.cache_identity_digest,
            condition="IDENTITY_MISMATCH",
            manifest_digest=_digest("9"),
            verification_version=1,
        )
        corrupt = ArtifactCacheObservation(
            cache_kind="FULL_SNAPSHOT",
            cache_identity_digest=identity.cache_identity_digest,
            condition="CORRUPT",
        )
        newest_at = self.now + timedelta(seconds=10)
        newest = reconcile_probe_observations(
            self.session,
            node_id=self.node.id,
            observations=[mismatch],
            scan_complete=True,
            source_id="newest-complete-scan",
            observed_at=newest_at,
        )
        self.session.commit()
        self.assertEqual(len(newest), 1)
        self.assertEqual(cache.status, "STALE")
        self.assertEqual(cache.reason_code, "PROBE_IDENTITY_MISMATCH")
        newest_event_sequence = cache.event_sequence
        newest_observed_at = cache.last_probe_observed_at
        event_ids = tuple(
            self.session.scalars(
                select(ArtifactCacheEvent.id).order_by(ArtifactCacheEvent.sequence)
            )
        )

        for source_id, observed_at in (
            ("older-complete-scan", newest_at - timedelta(seconds=1)),
            ("equal-complete-scan", newest_at),
        ):
            self.assertEqual(
                reconcile_probe_observations(
                    self.session,
                    node_id=self.node.id,
                    observations=[corrupt],
                    scan_complete=True,
                    source_id=source_id,
                    observed_at=observed_at,
                ),
                [],
            )
        self.session.commit()

        self.assertEqual(cache.status, "STALE")
        self.assertEqual(cache.reason_code, "PROBE_IDENTITY_MISMATCH")
        self.assertEqual(cache.event_sequence, newest_event_sequence)
        self.assertEqual(cache.last_probe_observed_at, newest_observed_at)
        self.assertEqual(
            tuple(
                self.session.scalars(
                    select(ArtifactCacheEvent.id).order_by(
                        ArtifactCacheEvent.sequence
                    )
                )
            ),
            event_ids,
        )

    def test_probe_source_replay_rejects_changed_closed_evidence(self):
        identity, attempt = self._full()
        record_preparation_success(
            self.session, attempt_id=attempt.id, identity=identity
        )
        self.session.commit()
        first = ArtifactCacheObservation(
            cache_kind="FULL_SNAPSHOT",
            cache_identity_digest=identity.cache_identity_digest,
            condition="CORRUPT",
        )
        reconcile_probe_observations(
            self.session,
            node_id=self.node.id,
            observations=[first],
            scan_complete=True,
            source_id="same-scan",
            observed_at=self.now + timedelta(seconds=1),
        )
        self.session.commit()
        changed = ArtifactCacheObservation(
            cache_kind="FULL_SNAPSHOT",
            cache_identity_digest=identity.cache_identity_digest,
            condition="UNSAFE",
        )
        with self.assertRaises(ArtifactCacheConflictError):
            reconcile_probe_observations(
                self.session,
                node_id=self.node.id,
                observations=[changed],
                scan_complete=True,
                source_id="same-scan",
                observed_at=self.now + timedelta(seconds=2),
            )

    def test_stage_revocation_stales_ready_but_does_not_downgrade_corrupt(self):
        identity, attempt, variant = self._stage()
        cache, _event, _changed = record_preparation_success(
            self.session, attempt_id=attempt.id, identity=identity
        )
        self.session.commit()
        variant.status = "REVOKED"
        variant.revoked_at = self.now + timedelta(seconds=1)
        variant.validated_at = self.now
        self.session.commit()

        events = mark_stage_variant_revoked(
            self.session,
            artifact_set_digest=variant.artifact_set_digest,
            revoked_at=variant.revoked_at,
        )
        self.session.commit()
        self.assertEqual(len(events), 1)
        self.assertEqual(cache.status, "STALE")
        with self.assertRaises(ArtifactCacheNotReadyError):
            require_ready_cache(
                self.session, node_id=self.node.id, identity=identity
            )

        record_verification_failure(
            self.session,
            node_id=self.node.id,
            identity=identity,
            source_id="runtime-verification-1",
        )
        self.session.commit()
        self.assertEqual(cache.status, "CORRUPT")
        mark_stage_variant_revoked(
            self.session,
            artifact_set_digest=variant.artifact_set_digest,
            revoked_at=variant.revoked_at,
        )
        self.assertEqual(cache.status, "CORRUPT")

    def test_quarantine_is_explicit_idempotent_and_failure_remains_nonready(self):
        identity, attempt = self._full()
        cache, _event, _changed = record_preparation_success(
            self.session, attempt_id=attempt.id, identity=identity
        )
        self.session.commit()
        request_id = str(uuid.uuid4())

        _cache, requested, changed = request_cache_quarantine(
            self.session,
            node_id=self.node.id,
            cache_identity_digest=identity.cache_identity_digest,
            request_id=request_id,
        )
        self.session.commit()
        self.assertTrue(changed)
        self.assertEqual(cache.status, "STALE")
        self.assertEqual(cache.reason_code, "QUARANTINE_REQUESTED")
        _cache, replay, changed = request_cache_quarantine(
            self.session,
            node_id=self.node.id,
            cache_identity_digest=identity.cache_identity_digest,
            request_id=request_id,
        )
        self.assertFalse(changed)
        self.assertEqual(replay.id, requested.id)

        _cache, failed, changed = complete_cache_quarantine(
            self.session,
            node_id=self.node.id,
            cache_identity_digest=identity.cache_identity_digest,
            request_id=request_id,
            succeeded=False,
        )
        self.session.commit()
        self.assertTrue(changed)
        self.assertEqual(failed.status, "STALE")
        self.assertEqual(cache.status, "STALE")
        self.assertEqual(cache.reason_code, "QUARANTINE_FAILED")

        second_request = str(uuid.uuid4())
        request_cache_quarantine(
            self.session,
            node_id=self.node.id,
            cache_identity_digest=identity.cache_identity_digest,
            request_id=second_request,
        )
        complete_cache_quarantine(
            self.session,
            node_id=self.node.id,
            cache_identity_digest=identity.cache_identity_digest,
            request_id=second_request,
            succeeded=True,
        )
        self.session.commit()
        self.assertEqual(cache.status, "QUARANTINED")
        self.assertIsNotNone(cache.quarantined_at)

    def test_new_current_success_can_recover_quarantined_cache(self):
        identity, first = self._full()
        cache, _event, _changed = record_preparation_success(
            self.session, attempt_id=first.id, identity=identity
        )
        self.session.commit()
        request_id = str(uuid.uuid4())
        request_cache_quarantine(
            self.session,
            node_id=self.node.id,
            cache_identity_digest=identity.cache_identity_digest,
            request_id=request_id,
        )
        complete_cache_quarantine(
            self.session,
            node_id=self.node.id,
            cache_identity_digest=identity.cache_identity_digest,
            request_id=request_id,
            succeeded=True,
        )
        self.session.commit()

        first_record = self.session.get(
            ArtifactPreparationNode, first.preparation_node_id
        )
        first_record.model_current_attempt = 2
        first_record.model_status = "SUCCEEDED"
        task = Task(
            bulk_id=self.session.get(
                ArtifactPreparation, first_record.preparation_id
            ).id,
            node_id=self.node.id,
            type=TaskType.PREPARE_MODEL.value,
            status=TaskStatus.SUCCEEDED.value,
            deployment_id=self.session.get(
                ArtifactPreparation, first_record.preparation_id
            ).deployment_id,
            payload={},
            attempts=1,
            result={},
        )
        result = {
            "cache_kind": identity.cache_kind,
            "manifest_digest": identity.manifest_digest,
            "verification_version": 1,
            "bytes_verified": 11,
            "file_count": 2,
        }
        task.payload = result
        task.result = result
        self.session.add(task)
        self.session.flush()
        second = ArtifactPreparationAttempt(
            preparation_node_id=first_record.id,
            stage="MODEL",
            attempt_no=2,
            task_id=task.id,
            status="SUCCEEDED",
            result=result,
            completed_at=self.now + timedelta(seconds=10),
        )
        self.session.add(second)
        self.session.commit()

        _cache, event, changed = record_preparation_success(
            self.session, attempt_id=second.id, identity=identity
        )
        self.session.commit()
        self.assertTrue(changed)
        self.assertEqual(event.previous_status, "QUARANTINED")
        self.assertEqual(cache.status, "READY")
        self.assertEqual(cache.last_ready_attempt_id, second.id)
        self.assertIsNone(cache.quarantined_at)

    def test_public_projection_is_closed_and_sorted(self):
        first_identity, first_attempt = self._full("1")
        first, _event, _changed = record_preparation_success(
            self.session, attempt_id=first_attempt.id, identity=first_identity
        )
        self.session.commit()

        value = artifact_cache_projection(self.session, first.id)
        self.assertEqual(value["id"], first.id)
        self.assertEqual(value["status"], "READY")
        self.assertNotIn("path", value)
        self.assertNotIn("result", value)
        self.assertEqual(
            [item["id"] for item in list_artifact_cache_projections(self.session)],
            [first.id],
        )
        references = artifact_cache_reference_projection(self.session, first.id)
        self.assertTrue(references["complete"])
        self.assertEqual(
            references["blocking_references"],
            [
                {
                    "kind": "DEPLOYMENT_GENERATION",
                    "id": "cache-deployment-full-1",
                }
            ],
        )

    def test_retryable_failed_rollback_remains_a_blocking_reference(self):
        identity, attempt = self._full("b")
        cache, _event, _changed = record_preparation_success(
            self.session, attempt_id=attempt.id, identity=identity
        )
        source = self.session.get(
            Deployment, "cache-deployment-full-b"
        )
        source.status = "VERIFIED"
        source.verified_at = self.now
        target = Deployment(
            id="cache-deployment-full-rollback-reference-target",
            lineage_id=source.lineage_id,
            previous_generation_id=source.id,
            generation=2,
            plan={"assignments": [{"node_id": self.node.id}]},
            status="CREATED",
        )
        self.session.add(target)
        self.session.flush()
        self.session.add(
            ArtifactPreparation(
                request_id=str(uuid.uuid4()),
                request_digest=_digest("2"),
                deployment_id=target.id,
                status="SUCCEEDED",
                plan_snapshot={
                    "artifact": {
                        "cache_kind": "FULL_SNAPSHOT",
                        "manifest_digest": identity.manifest_digest,
                    },
                    "node_ids": [self.node.id],
                },
                completed_at=self.now,
            )
        )
        operation = DeploymentOperation(
            request_digest=_digest("3"),
            lineage_id=source.lineage_id,
            deployment_id=target.id,
            rollback_target_id=source.id,
            kind="ROLLBACK",
            status="FAILED",
            phase="STOP_SOURCE",
            node_ids=[self.node.id],
            active_lineage_id=source.lineage_id,
        )
        self.session.add(operation)
        self.session.flush()
        self.session.add(
            DeploymentOperationNode(
                operation_id=operation.id,
                node_id=self.node.id,
                phase="STOP_SOURCE",
                status="FAILED",
                attempt_count=1,
                failure_code="TASK_FAILED",
                completed_at=self.now,
            )
        )
        self.session.commit()

        for status in ("FAILED", "PARTIAL_FAILED"):
            with self.subTest(status=status):
                operation.status = status
                operation.active_lineage_id = source.lineage_id
                operation.completed_at = None
                self.session.commit()
                references = artifact_cache_reference_projection(
                    self.session, cache.id
                )
                self.assertIn(
                    {"kind": "DEPLOYMENT_OPERATION", "id": operation.id},
                    references["blocking_references"],
                )

        operation.active_lineage_id = None
        operation.completed_at = self.now
        self.session.commit()
        references = artifact_cache_reference_projection(self.session, cache.id)
        self.assertNotIn(
            {"kind": "DEPLOYMENT_OPERATION", "id": operation.id},
            references["blocking_references"],
        )

    def test_verified_direct_predecessor_remains_a_blocking_reference(self):
        identity, attempt = self._full("c")
        cache, _event, _changed = record_preparation_success(
            self.session, attempt_id=attempt.id, identity=identity
        )
        predecessor = self.session.get(
            Deployment, "cache-deployment-full-c"
        )
        predecessor.status = "VERIFIED"
        predecessor.verified_at = self.now
        latest = Deployment(
            id="cache-deployment-full-successor",
            lineage_id=predecessor.lineage_id,
            previous_generation_id=predecessor.id,
            generation=2,
            plan={"assignments": [{"node_id": self.node.id}]},
            status="CREATED",
        )
        self.session.add(latest)
        self.session.flush()
        self.session.add(
            ArtifactPreparation(
                request_id=str(uuid.uuid4()),
                request_digest=_digest("2"),
                deployment_id=latest.id,
                status="SUCCEEDED",
                plan_snapshot={
                    "artifact": {
                        "cache_kind": "FULL_SNAPSHOT",
                        "manifest_digest": identity.manifest_digest,
                    },
                    "node_ids": [self.node.id],
                },
                completed_at=self.now,
            )
        )
        self.session.commit()

        references = artifact_cache_reference_projection(self.session, cache.id)
        generation_ids = {
            item["id"]
            for item in references["blocking_references"]
            if item["kind"] == "DEPLOYMENT_GENERATION"
        }
        self.assertTrue(references["complete"])
        self.assertEqual(generation_ids, {predecessor.id, latest.id})


if __name__ == "__main__":
    unittest.main()

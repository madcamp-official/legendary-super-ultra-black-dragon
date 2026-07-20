from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from sqlalchemy.exc import IntegrityError

from dure.control.db import Base, make_engine, make_session_factory
from dure.control.service import (
    add_placement_profile,
    create_model_artifact,
    create_model_release,
    create_runtime_release,
    transition_model_release,
)


class RegistryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.engine = make_engine(f"sqlite:///{Path(self.temporary.name) / 'registry.db'}")
        Base.metadata.create_all(self.engine)
        self.factory = make_session_factory(self.engine)

    def tearDown(self):
        self.engine.dispose()
        self.temporary.cleanup()

    def artifact(self, session):
        return create_model_artifact(
            session,
            model_id="qwen-test-awq",
            repository="Qwen/Test-AWQ",
            revision="a" * 40,
            manifest_digest="sha256:" + "b" * 64,
            quantization="awq",
            size_mib=8192,
            default_max_model_len=8192,
            layer_count=32,
            license_id="apache-2.0",
        )

    def runtime(self, session):
        return create_runtime_release(
            session,
            version="vllm-test",
            image="registry.example/vllm@sha256:" + "c" * 64,
            vllm_version="0.9.0",
            cuda_version="12.8",
            gpu_architectures=["hopper", "ampere", "ampere"],
        )

    def release(self, session):
        artifact = self.artifact(session)
        runtime = self.runtime(session)
        return create_model_release(
            session, artifact_id=artifact.id, runtime_id=runtime.id, quality_rank=10
        )

    def placement(self, session, release_id, **overrides):
        values = {
            "release_id": release_id,
            "profile_id": "single-24g",
            "topology": "single-gpu",
            "node_count": 1,
            "min_gpu_memory_mib": 24576,
            "min_disk_free_mib": 16384,
            "pipeline_parallel_size": 1,
            "tensor_parallel_size": 1,
            "requires_network_evidence": False,
            "requires_nccl": False,
            "min_bandwidth_mbps": None,
            "max_rtt_ms": None,
            "max_packet_loss_pct": None,
            "max_ttft_p95_ms": 1000.0,
            "max_tpot_p95_ms": 100.0,
            "max_e2e_p95_ms": 5000.0,
            "min_success_rate": 0.99,
            "min_vram_headroom_pct": 10.0,
            "min_throughput_tps": 10.0,
        }
        values.update(overrides)
        return add_placement_profile(session, **values)

    def test_release_lifecycle_and_draft_only_placement(self):
        with self.factory() as session:
            release = self.release(session)
            placement = self.placement(session, release.id)
            self.assertEqual(placement.release_id, release.id)

            self.assertEqual(transition_model_release(session, release.id, "VALIDATED").status, "VALIDATED")
            with self.assertRaisesRegex(ValueError, "DRAFT"):
                self.placement(session, release.id, profile_id="late-profile")
            self.assertEqual(transition_model_release(session, release.id, "ACTIVE").status, "ACTIVE")
            self.assertEqual(transition_model_release(session, release.id, "DEPRECATED").status, "DEPRECATED")
            self.assertEqual(transition_model_release(session, release.id, "REVOKED").status, "REVOKED")
            with self.assertRaisesRegex(ValueError, "invalid model release transition"):
                transition_model_release(session, release.id, "ACTIVE")

    def test_validation_requires_placement(self):
        with self.factory() as session:
            release = self.release(session)

            with self.assertRaisesRegex(ValueError, "placement profile"):
                transition_model_release(session, release.id, "VALIDATED")

    def test_artifact_and_runtime_must_be_immutable(self):
        with self.factory() as session:
            with self.assertRaisesRegex(ValueError, "commit hash"):
                create_model_artifact(
                    session,
                    model_id="unsafe-model",
                    repository="Org/Model",
                    revision="main",
                    manifest_digest="sha256:" + "d" * 64,
                    quantization="awq",
                    size_mib=1,
                    default_max_model_len=1,
                    layer_count=1,
                    license_id="apache-2.0",
                )
            with self.assertRaisesRegex(ValueError, "digest"):
                create_model_artifact(
                    session,
                    model_id="unsafe-model",
                    repository="Org/Model",
                    revision="e" * 40,
                    manifest_digest="latest",
                    quantization="awq",
                    size_mib=1,
                    default_max_model_len=1,
                    layer_count=1,
                    license_id="apache-2.0",
                )
            with self.assertRaisesRegex(ValueError, "digest-pinned"):
                create_runtime_release(
                    session,
                    version="unsafe",
                    image="registry.example/vllm:latest",
                    vllm_version="0.9.0",
                    cuda_version="12.8",
                    gpu_architectures=["ampere"],
                )
            with self.assertRaisesRegex(ValueError, "architecture"):
                create_runtime_release(
                    session,
                    version="unsafe",
                    image="registry.example/vllm@sha256:" + "f" * 64,
                    vllm_version="0.9.0",
                    cuda_version="12.8",
                    gpu_architectures=["unknown"],
                )

    def test_multinode_placement_requires_bounded_network_policy(self):
        with self.factory() as session:
            release = self.release(session)
            with self.assertRaisesRegex(ValueError, "network and NCCL"):
                self.placement(
                    session,
                    release.id,
                    profile_id="pipeline",
                    topology="pipeline",
                    node_count=3,
                    pipeline_parallel_size=3,
                )
            with self.assertRaisesRegex(ValueError, "out of range"):
                self.placement(
                    session,
                    release.id,
                    profile_id="pipeline",
                    topology="pipeline",
                    node_count=3,
                    pipeline_parallel_size=3,
                    requires_network_evidence=True,
                    requires_nccl=True,
                    min_bandwidth_mbps=-1,
                    max_rtt_ms=5,
                    max_packet_loss_pct=1,
                )

    def test_database_rejects_unknown_release_status(self):
        with self.factory() as session:
            release = self.release(session)
            release.status = "BROKEN"
            with self.assertRaises(IntegrityError):
                session.commit()
            session.rollback()


if __name__ == "__main__":
    unittest.main()

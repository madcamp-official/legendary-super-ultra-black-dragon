import json
import tempfile
import unittest
from pathlib import Path

from dure.command import CommandResult
from dure.model_cache import (
    MODEL_CACHE_KIND_FULL_SNAPSHOT,
    MODEL_CACHE_KIND_STAGE,
    MODEL_CACHE_VERIFICATION_VERSION,
    build_model_cache_marker,
)
from dure.models import NodeProfile
from dure.probe import NodeProbe

from .helpers import FakeRunner


class ProbeTests(unittest.TestCase):
    def test_parses_nvidia_smi_and_runtime(self):
        gpu_query = (
            "nvidia-smi",
            "--query-gpu=index,name,uuid,driver_version,memory.total",
            "--format=csv,noheader,nounits",
        )
        cap_query = (
            "nvidia-smi",
            "--query-gpu=index,compute_cap",
            "--format=csv,noheader,nounits",
        )
        runner = FakeRunner(
            executables={"nvidia-smi", "docker", "ray"},
            responses={
                gpu_query: CommandResult(
                    gpu_query,
                    0,
                    "0, NVIDIA GeForce RTX 3090, GPU-123, 610.43.02, 24576",
                ),
                cap_query: CommandResult(cap_query, 0, "0, 8.6"),
                ("docker", "version"): CommandResult(("docker", "version"), 0, "ok"),
                ("docker", "info", "--format", "{{json .Runtimes}}"): CommandResult(
                    ("docker", "info"), 0, '{"runc":{},"nvidia":{}}'
                ),
                ("ray", "--version"): CommandResult(
                    ("ray", "--version"), 0, "ray, version 2.56.1"
                ),
            },
        )

        result = NodeProbe(runner).collect()

        self.assertEqual(len(result.gpus), 1)
        self.assertEqual(result.gpus[0].memory_mib, 24576)
        self.assertEqual(result.gpus[0].compute_capability, "8.6")
        self.assertTrue(result.runtime.engine_ready)
        self.assertTrue(result.runtime.nvidia_runtime)
        self.assertTrue(result.runtime.ray_available)

    def test_reports_missing_gpu(self):
        result = NodeProbe(FakeRunner()).collect()
        self.assertEqual(result.gpus, [])
        self.assertIn("No CUDA-capable NVIDIA GPU detected", result.issues)

    def test_detects_installed_models_and_llm_workloads(self):
        with tempfile.TemporaryDirectory() as temporary:
            model_root = Path(temporary) / "models"
            model_path = model_root / "qwen-local"
            model_path.mkdir(parents=True)
            (model_path / "config.json").write_text(
                json.dumps(
                    {
                        "_name_or_path": "Qwen/Qwen2.5-14B-Instruct-AWQ",
                        "quantization_config": {"quant_method": "awq"},
                    }
                ),
                encoding="utf-8",
            )
            (model_path / ".dure-model.json").write_text(
                json.dumps(
                    {
                        "schema": "dure-model-cache-v1",
                        "repository": "Qwen/Qwen2.5-14B-Instruct-AWQ",
                        "revision": "a" * 40,
                        "manifest_digest": "sha256:" + "b" * 64,
                        "quantization": "awq",
                    }
                ),
                encoding="utf-8",
            )
            incomplete = model_root / "partial-model"
            incomplete.mkdir()
            containers = "\n".join(
                [
                    json.dumps(
                        {
                            "Names": "dure-api-deploy-1",
                            "Image": "registry/vllm@sha256:abc",
                            "Status": "Up 2 hours",
                            "Labels": "dure.deployment=deploy-1,dure.generation=2,dure.model=qwen2.5-14b-awq",
                        }
                    ),
                    json.dumps(
                        {
                            "Names": "unrelated-db",
                            "Image": "postgres:16",
                            "Status": "Up 2 hours",
                            "Labels": "",
                        }
                    ),
                ]
            )
            runner = FakeRunner(
                executables={"docker", "du"},
                responses={
                    ("docker", "version"): CommandResult(("docker", "version"), 0, "ok"),
                    ("docker", "info", "--format", "{{json .Runtimes}}"): CommandResult(
                        ("docker", "info"), 0, '{"nvidia":{}}'
                    ),
                    ("docker", "ps", "--all", "--format", "{{json .}}"): CommandResult(
                        ("docker", "ps"), 0, containers
                    ),
                    ("du", "-sm", "--", str(model_path)): CommandResult(
                        ("du", "-sm"), 0, f"10240\t{model_path}"
                    ),
                    ("du", "-sm", "--", str(incomplete)): CommandResult(
                        ("du", "-sm"), 0, f"100\t{incomplete}"
                    ),
                },
            )

            result = NodeProbe(runner, model_roots=[model_root]).collect()

        by_id = {item.model_id: item for item in result.installed_models}
        self.assertTrue(by_id["Qwen/Qwen2.5-14B-Instruct-AWQ"].complete)
        self.assertEqual(by_id["Qwen/Qwen2.5-14B-Instruct-AWQ"].quantization, "awq")
        self.assertEqual(
            by_id["Qwen/Qwen2.5-14B-Instruct-AWQ"].revision, "a" * 40
        )
        self.assertEqual(by_id["Qwen/Qwen2.5-14B-Instruct-AWQ"].size_mib, 10240)
        self.assertEqual(
            by_id["Qwen/Qwen2.5-14B-Instruct-AWQ"].manifest_digest,
            "sha256:" + "b" * 64,
        )
        self.assertEqual(
            by_id["Qwen/Qwen2.5-14B-Instruct-AWQ"].cache_kind,
            MODEL_CACHE_KIND_FULL_SNAPSHOT,
        )
        self.assertEqual(
            by_id["Qwen/Qwen2.5-14B-Instruct-AWQ"].verification_version,
            MODEL_CACHE_VERIFICATION_VERSION,
        )
        self.assertFalse(by_id["partial-model"].complete)
        self.assertEqual(len(result.workloads), 1)
        self.assertEqual(result.workloads[0].deployment_id, "deploy-1")
        self.assertEqual(result.workloads[0].model_id, "qwen2.5-14b-awq")

    def test_old_profile_json_defaults_new_inventory_fields(self):
        value = NodeProbe(FakeRunner()).collect().to_dict()
        value.pop("installed_models")
        value.pop("workloads")

        restored = NodeProfile.from_dict(value)

        self.assertEqual(restored.installed_models, [])
        self.assertEqual(restored.workloads, [])

    def test_v2_full_snapshot_and_stage_markers_are_distinct(self):
        with tempfile.TemporaryDirectory() as temporary:
            model_root = Path(temporary) / "models"
            for name, repository, cache_kind in (
                ("full", "Example/Full-AWQ", MODEL_CACHE_KIND_FULL_SNAPSHOT),
                ("stage", "Example/Stage-AWQ", MODEL_CACHE_KIND_STAGE),
            ):
                model_path = model_root / name
                model_path.mkdir(parents=True)
                (model_path / "config.json").write_text(
                    json.dumps(
                        {
                            "_name_or_path": repository,
                            "quantization_config": {"quant_method": "awq"},
                        }
                    ),
                    encoding="utf-8",
                )
                (model_path / ".dure-model.json").write_text(
                    json.dumps(
                        build_model_cache_marker(
                            repository=repository,
                            revision="a" * 40,
                            manifest_digest="sha256:" + "b" * 64,
                            quantization="awq",
                            cache_kind=cache_kind,
                        )
                    ),
                    encoding="utf-8",
                )

            result = NodeProbe(FakeRunner(), model_roots=[model_root]).collect()

        by_id = {item.model_id: item for item in result.installed_models}
        self.assertEqual(
            by_id["Example/Full-AWQ"].cache_kind,
            MODEL_CACHE_KIND_FULL_SNAPSHOT,
        )
        self.assertEqual(
            by_id["Example/Stage-AWQ"].cache_kind,
            MODEL_CACHE_KIND_STAGE,
        )
        for item in by_id.values():
            self.assertEqual(item.manifest_digest, "sha256:" + "b" * 64)
            self.assertEqual(
                item.verification_version, MODEL_CACHE_VERIFICATION_VERSION
            )

    def test_old_installed_model_json_defaults_cache_identity_fields(self):
        value = NodeProbe(FakeRunner()).collect().to_dict()
        value["installed_models"] = [
            {
                "source": "dure",
                "model_id": "Example/Legacy-AWQ",
                "path": "/var/lib/dure/models/legacy",
                "revision": "a" * 40,
                "quantization": "awq",
                "size_mib": 1024,
                "complete": True,
            }
        ]

        restored = NodeProfile.from_dict(value)

        model = restored.installed_models[0]
        self.assertIsNone(model.manifest_digest)
        self.assertIsNone(model.cache_kind)
        self.assertIsNone(model.verification_version)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import contextlib
import copy
import importlib.util
import io
import json
import os
import subprocess
import sys
import time
import unittest
from dataclasses import replace
from pathlib import Path

from dure.stage_cache import (
    StageCacheIdentity,
    stage_contract_identity_digest,
)


SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "acceptance-vllm-stage-ray-pp.py"
)
SPEC = importlib.util.spec_from_file_location(
    "dure_vllm_stage_ray_pp_acceptance", SCRIPT
)
assert SPEC is not None and SPEC.loader is not None
acceptance = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = acceptance
SPEC.loader.exec_module(acceptance)


NODE_0 = "11111111-1111-4111-8111-111111111111"
NODE_1 = "22222222-2222-4222-8222-222222222222"
NODE_2 = "33333333-3333-4333-8333-333333333333"
RUN_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
DEPLOYMENT_ID = "aaaaaaaa-aaaa-5aaa-8aaa-aaaaaaaaaaab"
RUNTIME_IMAGE = "registry.example/dure/vllm@sha256:" + "1" * 64
REPOSITORY = "Example/StageModel"
REVISION = "a" * 40


def _digest(character: str) -> str:
    return "sha256:" + character * 64


def contract_document(size: int = 2) -> dict:
    identities = (
        (NODE_0, "10.0.0.2"),
        (NODE_1, "10.0.0.3"),
        (NODE_2, "10.0.0.4"),
    )[:size]
    source_manifest_digest = _digest("2")
    exporter_build_digest = _digest("3")
    contract_identity_digest = stage_contract_identity_digest(
        source_manifest_digest=source_manifest_digest,
        runtime_image=RUNTIME_IMAGE,
        vllm_version="0.9.0",
        exporter_build_digest=exporter_build_digest,
        architecture="Qwen2ForCausalLM",
        quantization="awq",
        tensor_parallel_size=1,
        pipeline_parallel_size=size,
        loader_format="VLLM_SHARDED_STATE_V1",
    )
    stage = acceptance.StageArtifactContract(
        artifact_set_digest=_digest("0"),
        contract_identity_digest=contract_identity_digest,
        source_manifest_digest=source_manifest_digest,
        runtime_image=RUNTIME_IMAGE,
        vllm_version="0.9.0",
        exporter_build_digest=exporter_build_digest,
        architecture="Qwen2ForCausalLM",
        quantization="awq",
        tensor_parallel_size=1,
        pipeline_parallel_size=size,
        loader_format="VLLM_SHARDED_STATE_V1",
    )
    bindings = [
        acceptance.ExpectedBinding(
            node_id=node_id,
            runtime_address=address,
            pipeline_rank=rank,
            runtime_rank=rank,
            tensor_rank=0,
            stage_manifest_digest=_digest(str(4 + rank)),
            stage_tensor_key_count=2 + rank,
            stage_tensor_keys_digest=_digest(str(7 + rank)),
            stage_weight_size_bytes=10 + rank,
            stage_total_size_bytes=20 + rank,
            stage_file_count=5,
            stage_cache_identity_digest=_digest("f"),
        )
        for rank, (node_id, address) in enumerate(identities)
    ]
    artifact_set_digest = acceptance._artifact_set_digest(stage, bindings)
    stage = replace(stage, artifact_set_digest=artifact_set_digest)
    final_bindings = []
    for binding in bindings:
        identity = StageCacheIdentity(
            repository=REPOSITORY,
            revision=REVISION,
            manifest_digest=binding.stage_manifest_digest,
            quantization="awq",
            artifact_set_digest=artifact_set_digest,
            contract_identity_digest=contract_identity_digest,
            source_manifest_digest=source_manifest_digest,
            runtime_image=RUNTIME_IMAGE,
            vllm_version="0.9.0",
            exporter_build_digest=exporter_build_digest,
            architecture="Qwen2ForCausalLM",
            loader_format="VLLM_SHARDED_STATE_V1",
            tensor_parallel_size=1,
            pipeline_parallel_size=size,
            pipeline_rank=binding.pipeline_rank,
            tensor_rank=0,
            tensor_keys_digest=binding.stage_tensor_keys_digest,
        )
        final_bindings.append(
            replace(
                binding,
                stage_cache_identity_digest=(
                    identity.cache_identity_digest
                ),
            )
        )
    return {
        "schema_version": 1,
        "backend": "VLLM_RAY_PP_V1",
        "vllm_version": "0.9.0",
        "validation_run_id": RUN_ID,
        "deployment_id": DEPLOYMENT_ID,
        "generation": 1,
        "runtime_image": RUNTIME_IMAGE,
        "repository": REPOSITORY,
        "revision": REVISION,
        "stage_artifact": {
            "artifact_set_digest": stage.artifact_set_digest,
            "contract_identity_digest": stage.contract_identity_digest,
            "source_manifest_digest": stage.source_manifest_digest,
            "runtime_image": stage.runtime_image,
            "vllm_version": stage.vllm_version,
            "exporter_build_digest": stage.exporter_build_digest,
            "architecture": stage.architecture,
            "quantization": stage.quantization,
            "tensor_parallel_size": stage.tensor_parallel_size,
            "pipeline_parallel_size": stage.pipeline_parallel_size,
            "loader_format": stage.loader_format,
        },
        "ordered_bindings": [
            {
                "node_id": item.node_id,
                "runtime_address": item.runtime_address,
                "pipeline_rank": item.pipeline_rank,
                "runtime_rank": item.runtime_rank,
                "tensor_rank": item.tensor_rank,
                "stage_manifest_digest": item.stage_manifest_digest,
                "stage_tensor_key_count": item.stage_tensor_key_count,
                "stage_tensor_keys_digest": item.stage_tensor_keys_digest,
                "stage_weight_size_bytes": item.stage_weight_size_bytes,
                "stage_total_size_bytes": item.stage_total_size_bytes,
                "stage_file_count": item.stage_file_count,
                "stage_cache_identity_digest": (
                    item.stage_cache_identity_digest
                ),
            }
            for item in final_bindings
        ],
    }


class FakeBackend:
    def __init__(
        self,
        *,
        preflight_error: Exception | None = None,
        run_error: Exception | None = None,
        addresses: tuple[str, ...] | None = None,
        mutate_rehash=None,
    ) -> None:
        self.preflight_error = preflight_error
        self.run_error = run_error
        self.addresses = addresses
        self.mutate_rehash = mutate_rehash
        self.preflight_calls = 0
        self.run_calls = 0

    def preflight(self, contract) -> None:
        self.preflight_calls += 1
        if self.preflight_error is not None:
            raise self.preflight_error

    def run(self, contract):
        self.run_calls += 1
        if self.run_error is not None:
            raise self.run_error
        addresses = self.addresses or tuple(
            item.runtime_address for item in contract.ordered_bindings
        )
        evidence = list(acceptance._expected_node_rehashes(contract))
        if self.mutate_rehash is not None:
            self.mutate_rehash(evidence)
        return acceptance.RuntimeResult(addresses, 4, tuple(evidence))


class VllmStageRayPpAcceptanceTests(unittest.TestCase):
    def run_harness(
        self,
        *,
        document=None,
        backend=None,
        argv=None,
        environ=None,
    ):
        parsed = acceptance.AcceptanceContract.parse(
            document if document is not None else contract_document()
        )
        selected_backend = backend or FakeBackend()
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(
            stderr
        ):
            code = acceptance.run_acceptance(
                argv=argv or [str(SCRIPT)],
                environ=(
                    environ
                    if environ is not None
                    else {
                        "DURE_RUN_VLLM_STAGE_RAY_PP_ACCEPTANCE": "1"
                    }
                ),
                contract_loader=lambda: parsed,
                backend_factory=lambda: selected_backend,
            )
        return (
            code,
            stdout.getvalue(),
            stderr.getvalue(),
            selected_backend,
        )

    def test_default_process_is_not_run_with_exit_77(self):
        environment = os.environ.copy()
        environment.pop(
            "DURE_RUN_VLLM_STAGE_RAY_PP_ACCEPTANCE", None
        )
        completed = subprocess.run(
            [sys.executable, str(SCRIPT)],
            check=False,
            capture_output=True,
            text=True,
            env=environment,
        )
        self.assertEqual(completed.returncode, 77)
        self.assertEqual(completed.stderr, "")
        self.assertEqual(json.loads(completed.stdout)["status"], "NOT_RUN")

    def test_exact_two_and_three_node_stage_contracts_pass(self):
        for size in (2, 3):
            with self.subTest(size=size):
                document = contract_document(size)
                contract = acceptance.AcceptanceContract.parse(document)
                self.assertEqual(contract.world_size, size)
                self.assertEqual(
                    [
                        item.pipeline_rank
                        for item in contract.ordered_bindings
                    ],
                    list(range(size)),
                )
                code, stdout, stderr, backend = self.run_harness(
                    document=document
                )
                self.assertEqual(code, 0)
                self.assertEqual(stderr, "")
                self.assertEqual(json.loads(stdout)["status"], "PASSED")
                self.assertEqual(backend.run_calls, 1)

    def test_contract_is_closed_at_root_stage_and_rank(self):
        cases = (
            ("root", "command", ["sh", "-c", "id"]),
            ("root", "model_path", "/tmp/model"),
            ("root", "docker_args", ["--privileged"]),
            ("stage", "environment", {"TOKEN": "secret"}),
            ("rank", "host_path", "/tmp/stage"),
        )
        for location, field, value in cases:
            with self.subTest(location=location, field=field):
                document = contract_document()
                if location == "root":
                    document[field] = value
                elif location == "stage":
                    document["stage_artifact"][field] = value
                else:
                    document["ordered_bindings"][0][field] = value
                with self.assertRaises(
                    acceptance.PrerequisiteMissing
                ) as raised:
                    acceptance.AcceptanceContract.parse(document)
                self.assertEqual(raised.exception.code, "CONTRACT_INVALID")

    def test_contract_rejects_boolean_and_rank_or_digest_mutation(self):
        boolean = contract_document()
        boolean["schema_version"] = True
        with self.assertRaises(acceptance.PrerequisiteMissing):
            acceptance.AcceptanceContract.parse(boolean)

        mutations = (
            (
                "contract",
                lambda value: value["stage_artifact"].update(
                    contract_identity_digest=_digest("e")
                ),
                "STAGE_CONTRACT_MISMATCH",
            ),
            (
                "artifact-set",
                lambda value: value["stage_artifact"].update(
                    artifact_set_digest=_digest("e")
                ),
                "STAGE_ARTIFACT_SET_MISMATCH",
            ),
            (
                "cache",
                lambda value: value["ordered_bindings"][0].update(
                    stage_cache_identity_digest=_digest("e")
                ),
                "STAGE_CACHE_IDENTITY_MISMATCH",
            ),
            (
                "tensor",
                lambda value: value["ordered_bindings"][0].update(
                    stage_tensor_keys_digest=_digest("e")
                ),
                "STAGE_ARTIFACT_SET_MISMATCH",
            ),
            (
                "rank",
                lambda value: value["ordered_bindings"][0].update(
                    pipeline_rank=1
                ),
                "RANK_CONTRACT_INVALID",
            ),
        )
        for name, mutate, expected_code in mutations:
            with self.subTest(name=name):
                document = contract_document()
                mutate(document)
                with self.assertRaises(
                    acceptance.PrerequisiteMissing
                ) as raised:
                    acceptance.AcceptanceContract.parse(document)
                self.assertEqual(raised.exception.code, expected_code)

    def test_contract_rejects_duplicate_swapped_and_public_nodes(self):
        duplicate = contract_document()
        duplicate["ordered_bindings"][1]["node_id"] = NODE_0
        with self.assertRaises(acceptance.PrerequisiteMissing) as raised:
            acceptance.AcceptanceContract.parse(duplicate)
        self.assertEqual(raised.exception.code, "DUPLICATE_NODE")

        swapped = contract_document(3)
        swapped["ordered_bindings"][1], swapped["ordered_bindings"][2] = (
            swapped["ordered_bindings"][2],
            swapped["ordered_bindings"][1],
        )
        with self.assertRaises(acceptance.PrerequisiteMissing):
            acceptance.AcceptanceContract.parse(swapped)

        public = contract_document()
        public["ordered_bindings"][1]["runtime_address"] = "8.8.8.8"
        with self.assertRaises(acceptance.PrerequisiteMissing) as raised:
            acceptance.AcceptanceContract.parse(public)
        self.assertEqual(raised.exception.code, "PUBLIC_RAY_ADDRESS")

    def test_cli_and_unknown_acceptance_environment_are_not_inputs(self):
        cases = (
            {"argv": [str(SCRIPT), "--model-path", "/tmp/model"]},
            {
                "environ": {
                    "DURE_RUN_VLLM_STAGE_RAY_PP_ACCEPTANCE": "1",
                    "DURE_VLLM_STAGE_RAY_PP_ACCEPTANCE_COMMAND": "id",
                }
            },
        )
        for updates in cases:
            with self.subTest(updates=updates):
                backend = FakeBackend()
                code, stdout, stderr, backend = self.run_harness(
                    backend=backend, **updates
                )
                self.assertEqual(code, 77)
                self.assertEqual(stderr, "")
                report = json.loads(stdout)
                self.assertEqual(report["status"], "NOT_RUN")
                self.assertEqual(report["code"], "INPUT_NOT_ALLOWED")
                self.assertEqual(backend.preflight_calls, 0)
                self.assertEqual(backend.run_calls, 0)

        for name, value in (
            ("PYTHONPATH", "/tmp/shadow"),
            ("LD_PRELOAD", "/tmp/inject.so"),
            ("VLLM_USE_V1", "1"),
            ("VLLM_USE_RAY_COMPILED_DAG", "1"),
            ("RAY_ADDRESS", "10.0.0.99:6379"),
        ):
            with self.subTest(environment=name):
                backend = FakeBackend()
                code, stdout, stderr, backend = self.run_harness(
                    backend=backend,
                    environ={
                        "DURE_RUN_VLLM_STAGE_RAY_PP_ACCEPTANCE": "1",
                        name: value,
                    },
                )
                self.assertEqual(code, 77)
                self.assertEqual(stderr, "")
                self.assertEqual(
                    json.loads(stdout)["code"],
                    "PROCESS_ENVIRONMENT_UNTRUSTED",
                )
                self.assertEqual(backend.preflight_calls, 0)

    def test_preflight_shortage_is_not_run_and_does_not_start(self):
        backend = FakeBackend(
            preflight_error=acceptance.PrerequisiteMissing(
                "RUNTIME_MISSING", "고정 runtime이 없습니다."
            )
        )
        code, stdout, stderr, backend = self.run_harness(backend=backend)
        self.assertEqual(code, 77)
        self.assertEqual(stderr, "")
        self.assertEqual(json.loads(stdout)["status"], "NOT_RUN")
        self.assertEqual(backend.preflight_calls, 1)
        self.assertEqual(backend.run_calls, 0)

    def test_error_after_start_is_failed_and_raw_error_is_redacted(self):
        backend = FakeBackend(
            run_error=RuntimeError(
                "/secret/model and TOKEN=do-not-report"
            )
        )
        code, stdout, stderr, backend = self.run_harness(backend=backend)
        self.assertEqual(code, 1)
        self.assertEqual(stdout, "")
        report = json.loads(stderr)
        self.assertEqual(report["status"], "FAILED")
        self.assertEqual(
            report["code"], "VLLM_STAGE_RAY_PP_ACCEPTANCE_FAILED"
        )
        self.assertNotIn("secret", stderr)
        self.assertNotIn("TOKEN", stderr)
        self.assertEqual(backend.run_calls, 1)

    def test_rank_or_rehash_mismatch_after_start_is_failed(self):
        cases = (
            FakeBackend(addresses=("10.0.0.3", "10.0.0.2")),
            FakeBackend(
                mutate_rehash=lambda evidence: evidence.__setitem__(
                    0,
                    replace(
                        evidence[0], manifest_digest=_digest("e")
                    ),
                )
            ),
        )
        for backend in cases:
            with self.subTest(backend=backend):
                code, stdout, stderr, _ = self.run_harness(
                    backend=backend
                )
                self.assertEqual(code, 1)
                self.assertEqual(stdout, "")
                self.assertEqual(json.loads(stderr)["status"], "FAILED")

    def test_passed_result_contains_exact_stage_rank_and_rehash_evidence(self):
        document = contract_document()
        contract = acceptance.AcceptanceContract.parse(document)
        code, stdout, stderr, _ = self.run_harness(document=document)
        self.assertEqual(code, 0)
        self.assertEqual(stderr, "")
        report = json.loads(stdout)
        self.assertEqual(report["status"], "PASSED")
        self.assertEqual(report["model_cache_kind"], "STAGE")
        self.assertIs(report["model_content_rehashed"], True)
        self.assertIs(report["runtime_image_attested"], False)
        self.assertEqual(
            report["artifact_set_digest"],
            document["stage_artifact"]["artifact_set_digest"],
        )
        self.assertEqual(len(report["node_rehashes"]), 2)
        for expected, observed in zip(
            contract.ordered_bindings, report["node_rehashes"]
        ):
            self.assertEqual(observed["node_id"], expected.node_id)
            self.assertEqual(
                observed["manifest_digest"],
                expected.stage_manifest_digest,
            )
            self.assertEqual(
                observed["tensor_keys_digest"],
                expected.stage_tensor_keys_digest,
            )
            self.assertEqual(
                observed["cache_identity_digest"],
                expected.stage_cache_identity_digest,
            )

        self.assertEqual(len(report["checks"]), 1)
        check = report["checks"][0]
        self.assertEqual(
            set(check), {"name", "ok", "detail", "blocking"}
        )
        self.assertEqual(check["name"], "pipeline-rank-contract")
        self.assertIs(check["ok"], True)
        self.assertIs(check["blocking"], True)
        detail = json.loads(check["detail"])
        self.assertEqual(
            set(detail),
            {
                "schema_version",
                "backend",
                "vllm_version",
                "node_id",
                "runtime_address",
                "pipeline_rank",
                "runtime_rank",
                "ordered_bindings",
                "stage_artifact",
            },
        )
        self.assertEqual(
            set(detail["stage_artifact"]),
            {
                "artifact_set_digest",
                "contract_identity_digest",
                "source_manifest_digest",
                "loader_format",
                "stage_manifest_digest",
                "stage_tensor_keys_digest",
                "stage_cache_identity_digest",
            },
        )
        self.assertEqual(
            check["detail"],
            json.dumps(
                detail,
                sort_keys=True,
                ensure_ascii=False,
                separators=(",", ":"),
            ),
        )

    def test_runtime_resource_binding_and_node_rehash_results_are_exact(self):
        contract = acceptance.AcceptanceContract.parse(contract_document())
        raw_nodes = []
        runtime_nodes = []
        raw_results = []
        for index, binding in enumerate(contract.ordered_bindings):
            ray_id = f"ray-{index}"
            raw_nodes.append(
                {
                    "Alive": True,
                    "NodeID": ray_id,
                    "NodeManagerAddress": binding.runtime_address,
                    "Resources": {
                        "GPU": 1.0,
                        "dure_node_"
                        + binding.node_id.replace("-", ""): 1.0,
                    },
                }
            )
            raw_results.append(
                {
                    "status": "PASSED",
                    "ray_node_id": ray_id,
                    "manifest_digest": binding.stage_manifest_digest,
                    "tensor_keys_digest": (
                        binding.stage_tensor_keys_digest
                    ),
                    "cache_identity_digest": (
                        binding.stage_cache_identity_digest
                    ),
                    "total_size_bytes": binding.stage_total_size_bytes,
                    "file_count": binding.stage_file_count,
                }
            )
        runtime_nodes = acceptance._runtime_nodes_from_ray(raw_nodes)
        acceptance._validate_cluster_nodes(contract, runtime_nodes)
        evidence = acceptance._validate_node_rehash_results(
            contract, runtime_nodes, raw_results
        )
        self.assertEqual(
            evidence, acceptance._expected_node_rehashes(contract)
        )

        mismatched = copy.deepcopy(raw_results)
        mismatched[0]["cache_identity_digest"] = _digest("e")
        with self.assertRaises(acceptance.AcceptanceFailure) as raised:
            acceptance._validate_node_rehash_results(
                contract, runtime_nodes, mismatched
            )
        self.assertEqual(raised.exception.code, "STAGE_REHASH_MISMATCH")

        boolean_count = copy.deepcopy(raw_results)
        boolean_count[0]["file_count"] = True
        with self.assertRaises(acceptance.AcceptanceFailure) as raised:
            acceptance._validate_node_rehash_results(
                contract, runtime_nodes, boolean_count
            )
        self.assertEqual(raised.exception.code, "STAGE_REHASH_FAILED")

        raw_nodes[0]["Resources"][
            "dure_node_" + NODE_1.replace("-", "")
        ] = 1.0
        with self.assertRaises(acceptance.AcceptanceFailure) as raised:
            acceptance._runtime_nodes_from_ray(raw_nodes)
        self.assertEqual(
            raised.exception.code, "DURE_NODE_BINDING_INVALID"
        )

    def test_vllm_loader_contract_is_fixed_to_sharded_state_tp1_pp2_or_3(self):
        for size in (2, 3):
            contract = acceptance.AcceptanceContract.parse(
                contract_document(size)
            )
            options = acceptance._vllm_load_kwargs(contract)
            self.assertEqual(options["model"], "/models/model")
            self.assertEqual(options["tokenizer"], "/models/model")
            self.assertEqual(options["load_format"], "sharded_state")
            self.assertEqual(options["quantization"], "awq")
            self.assertEqual(options["tensor_parallel_size"], 1)
            self.assertEqual(options["pipeline_parallel_size"], size)
            self.assertEqual(
                options["distributed_executor_backend"], "ray"
            )
            self.assertIs(options["trust_remote_code"], False)
            self.assertIs(options["enable_lora"], False)

    def test_actor_rank_validation_rejects_missing_duplicate_and_swap(self):
        contract = acceptance.AcceptanceContract.parse(contract_document())
        runtime_nodes = (
            acceptance.RuntimeNode("ray-a", "10.0.0.2", 1.0, NODE_0),
            acceptance.RuntimeNode("ray-b", "10.0.0.3", 1.0, NODE_1),
        )
        self.assertEqual(
            acceptance._validate_actor_ranks(
                contract,
                runtime_nodes,
                [("ray-a", [0]), ("ray-b", [0])],
            ),
            ("10.0.0.2", "10.0.0.3"),
        )
        for pairs, expected_code in (
            ([("ray-a", [0])], "MISSING_RANK"),
            (
                [("ray-a", [0]), ("ray-a", [0])],
                "DUPLICATE_RANK",
            ),
            (
                [("ray-b", [0]), ("ray-a", [0])],
                "RANK_BINDING_MISMATCH",
            ),
        ):
            with self.subTest(pairs=pairs):
                with self.assertRaises(
                    acceptance.AcceptanceFailure
                ) as raised:
                    acceptance._validate_actor_ranks(
                        contract, runtime_nodes, pairs
                    )
                self.assertEqual(raised.exception.code, expected_code)

    def test_supervisor_times_out_and_terminates_gpu_worker(self):
        class SlowBackend:
            def run(self, contract):
                time.sleep(10)

        original = acceptance.RealVllmStageRayBackend
        acceptance.RealVllmStageRayBackend = SlowBackend
        try:
            contract = acceptance.AcceptanceContract.parse(
                contract_document()
            )
            with self.assertRaises(acceptance.AcceptanceFailure) as raised:
                acceptance._run_real_backend_bounded(
                    contract, timeout=0.05
                )
            self.assertEqual(raised.exception.code, "ACCEPTANCE_TIMEOUT")
        finally:
            acceptance.RealVllmStageRayBackend = original


if __name__ == "__main__":
    unittest.main()

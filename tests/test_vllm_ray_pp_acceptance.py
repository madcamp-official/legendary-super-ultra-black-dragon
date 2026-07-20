from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import subprocess
import sys
import time
import unittest
from pathlib import Path


SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "acceptance-vllm-ray-pp.py"
)
SPEC = importlib.util.spec_from_file_location("dure_vllm_ray_pp_acceptance", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
acceptance = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = acceptance
SPEC.loader.exec_module(acceptance)


NODE_0 = "11111111-1111-4111-8111-111111111111"
NODE_1 = "22222222-2222-4222-8222-222222222222"
NODE_2 = "33333333-3333-4333-8333-333333333333"
RUN_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
DEPLOYMENT_ID = "aaaaaaaa-aaaa-5aaa-8aaa-aaaaaaaaaaab"


def contract_document(size: int = 2) -> dict:
    identities = (
        (NODE_0, "10.0.0.2"),
        (NODE_1, "10.0.0.3"),
        (NODE_2, "10.0.0.4"),
    )[:size]
    return {
        "schema_version": 1,
        "backend": "VLLM_RAY_PP_V1",
        "vllm_version": "0.9.0",
        "validation_run_id": RUN_ID,
        "deployment_id": DEPLOYMENT_ID,
        "generation": 1,
        "runtime_image": "registry.example/dure/vllm@sha256:" + "1" * 64,
        "model_manifest_digest": "sha256:" + "2" * 64,
        "ordered_bindings": [
            {
                "node_id": node_id,
                "runtime_address": address,
                "pipeline_rank": rank,
                "runtime_rank": rank,
            }
            for rank, (node_id, address) in enumerate(identities)
        ],
    }


class FakeBackend:
    def __init__(
        self,
        *,
        preflight_error: Exception | None = None,
        run_error: Exception | None = None,
        addresses: tuple[str, ...] = ("10.0.0.2", "10.0.0.3"),
    ) -> None:
        self.preflight_error = preflight_error
        self.run_error = run_error
        self.addresses = addresses
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
        return acceptance.RuntimeResult(self.addresses, 4)


class VllmRayPpAcceptanceTests(unittest.TestCase):
    def run_harness(self, *, document=None, backend=None, argv=None, environ=None):
        parsed = acceptance.AcceptanceContract.parse(
            document if document is not None else contract_document()
        )
        selected_backend = backend or FakeBackend()
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = acceptance.run_acceptance(
                argv=argv or [str(SCRIPT)],
                environ=environ
                if environ is not None
                else {"DURE_RUN_VLLM_RAY_PP_ACCEPTANCE": "1"},
                contract_loader=lambda: parsed,
                backend_factory=lambda: selected_backend,
            )
        return code, stdout.getvalue(), stderr.getvalue(), selected_backend

    def test_default_process_is_not_run_with_exit_77(self):
        environment = os.environ.copy()
        environment.pop("DURE_RUN_VLLM_RAY_PP_ACCEPTANCE", None)
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

    def test_exact_two_and_three_node_contracts_are_accepted(self):
        for size in (2, 3):
            with self.subTest(size=size):
                parsed = acceptance.AcceptanceContract.parse(
                    contract_document(size)
                )
                self.assertEqual(parsed.world_size, size)
                self.assertEqual(
                    [item.pipeline_rank for item in parsed.ordered_bindings],
                    list(range(size)),
                )
                addresses = tuple(
                    item["runtime_address"]
                    for item in contract_document(size)["ordered_bindings"]
                )
                code, stdout, stderr, _ = self.run_harness(
                    document=contract_document(size),
                    backend=FakeBackend(addresses=addresses),
                )
                self.assertEqual(code, 0)
                self.assertEqual(stderr, "")
                self.assertEqual(json.loads(stdout)["status"], "PASSED")

    def test_contract_rejects_boolean_schema_version(self):
        document = contract_document()
        document["schema_version"] = True
        with self.assertRaises(acceptance.PrerequisiteMissing):
            acceptance.AcceptanceContract.parse(document)

    def test_contract_rejects_commands_environment_and_host_paths(self):
        for field, value in (
            ("command", ["sh", "-c", "id"]),
            ("environment", {"TOKEN": "secret"}),
            ("model_path", "/tmp/model"),
            ("docker_args", ["--privileged"]),
        ):
            with self.subTest(field=field):
                document = contract_document()
                document[field] = value
                with self.assertRaises(acceptance.PrerequisiteMissing) as raised:
                    acceptance.AcceptanceContract.parse(document)
                self.assertEqual(raised.exception.code, "CONTRACT_INVALID")

    def test_contract_rejects_rank_swap_duplicate_and_public_address(self):
        swapped = contract_document(3)
        swapped["ordered_bindings"][1], swapped["ordered_bindings"][2] = (
            swapped["ordered_bindings"][2],
            swapped["ordered_bindings"][1],
        )
        with self.assertRaises(acceptance.PrerequisiteMissing):
            acceptance.AcceptanceContract.parse(swapped)

        duplicate = contract_document()
        duplicate["ordered_bindings"][1]["node_id"] = NODE_0
        with self.assertRaises(acceptance.PrerequisiteMissing) as raised:
            acceptance.AcceptanceContract.parse(duplicate)
        self.assertEqual(raised.exception.code, "DUPLICATE_NODE")

        for address in ("8.8.8.8", "203.0.113.30"):
            with self.subTest(address=address):
                public = contract_document()
                public["ordered_bindings"][1]["runtime_address"] = address
                with self.assertRaises(acceptance.PrerequisiteMissing) as raised:
                    acceptance.AcceptanceContract.parse(public)
                self.assertEqual(raised.exception.code, "PUBLIC_RAY_ADDRESS")

    def test_cli_and_unknown_acceptance_environment_are_not_inputs(self):
        cases = (
            ({"argv": [str(SCRIPT), "--model-path", "/tmp/model"]}),
            (
                {
                    "environ": {
                        "DURE_RUN_VLLM_RAY_PP_ACCEPTANCE": "1",
                        "DURE_VLLM_RAY_PP_ACCEPTANCE_COMMAND": "id",
                    }
                }
            ),
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
                        "DURE_RUN_VLLM_RAY_PP_ACCEPTANCE": "1",
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

    def test_error_after_start_is_failed_and_redacted(self):
        backend = FakeBackend(
            run_error=RuntimeError("/secret/model and TOKEN=do-not-report")
        )
        code, stdout, stderr, backend = self.run_harness(backend=backend)
        self.assertEqual(code, 1)
        self.assertEqual(stdout, "")
        report = json.loads(stderr)
        self.assertEqual(report["status"], "FAILED")
        self.assertEqual(report["code"], "VLLM_RAY_PP_ACCEPTANCE_FAILED")
        self.assertNotIn("secret", stderr)
        self.assertNotIn("TOKEN", stderr)
        self.assertEqual(backend.run_calls, 1)

    def test_passed_result_contains_control_compatible_rank_check(self):
        code, stdout, stderr, _ = self.run_harness()
        self.assertEqual(code, 0)
        self.assertEqual(stderr, "")
        report = json.loads(stdout)
        self.assertEqual(report["status"], "PASSED")
        self.assertEqual(report["generated_token_count"], 4)
        self.assertEqual(report["deployment_id"], DEPLOYMENT_ID)
        self.assertEqual(report["generation"], 1)
        self.assertIs(report["runtime_image_attested"], False)
        self.assertIs(report["model_manifest_marker_verified"], True)
        self.assertIs(report["model_content_rehashed"], False)
        self.assertNotIn("rank_attestation", report)
        self.assertEqual(len(report["checks"]), 1)
        check = report["checks"][0]
        self.assertEqual(
            set(check),
            {"name", "ok", "detail", "blocking"},
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
            },
        )
        self.assertEqual(detail["backend"], "VLLM_RAY_PP_V1")
        self.assertEqual(len(detail["ordered_bindings"]), 2)
        self.assertEqual(
            set(detail["ordered_bindings"][0]),
            {"node_id", "runtime_address", "pipeline_rank", "runtime_rank"},
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

    def test_runtime_mapping_rejects_missing_duplicate_and_swapped_ranks(self):
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
        cases = (
            ([("ray-a", [0])], "MISSING_RANK"),
            ([("ray-a", [0]), ("ray-a", [0])], "DUPLICATE_RANK"),
            ([("ray-b", [0]), ("ray-a", [0])], "RANK_BINDING_MISMATCH"),
        )
        for pairs, expected_code in cases:
            with self.subTest(pairs=pairs):
                with self.assertRaises(acceptance.AcceptanceFailure) as raised:
                    acceptance._validate_actor_ranks(
                        contract, runtime_nodes, pairs
                    )
                self.assertEqual(raised.exception.code, expected_code)

    def test_cluster_requires_exact_one_gpu_per_expected_node(self):
        contract = acceptance.AcceptanceContract.parse(contract_document())
        cases = (
            (
                (acceptance.RuntimeNode("ray-a", "10.0.0.2", 1.0, NODE_0),),
                "NODE_SET_MISMATCH",
            ),
            (
                (
                    acceptance.RuntimeNode("ray-a", "10.0.0.2", 2.0, NODE_0),
                    acceptance.RuntimeNode("ray-b", "10.0.0.3", 1.0, NODE_1),
                ),
                "GPU_PLACEMENT_INVALID",
            ),
            (
                (
                    acceptance.RuntimeNode("ray-a", "10.0.0.2", 1.0, NODE_0),
                    acceptance.RuntimeNode("ray-b", "10.0.0.4", 1.0, NODE_1),
                ),
                "NODE_SET_MISMATCH",
            ),
            (
                (
                    acceptance.RuntimeNode("ray-a", "10.0.0.2", 1.0, NODE_1),
                    acceptance.RuntimeNode("ray-b", "10.0.0.3", 1.0, NODE_0),
                ),
                "DURE_NODE_BINDING_INVALID",
            ),
        )
        for nodes, expected_code in cases:
            with self.subTest(nodes=nodes):
                with self.assertRaises(acceptance.AcceptanceFailure) as raised:
                    acceptance._validate_cluster_nodes(contract, nodes)
                self.assertEqual(raised.exception.code, expected_code)

    def test_ray_resource_marker_binds_dure_uuid_and_rejects_extra_marker(self):
        raw = [
            {
                "Alive": True,
                "NodeID": "ray-a",
                "NodeManagerAddress": "10.0.0.2",
                "Resources": {
                    "GPU": 1.0,
                    "dure_node_" + NODE_0.replace("-", ""): 1.0,
                },
            }
        ]
        nodes = acceptance._runtime_nodes_from_ray(raw)
        self.assertEqual(nodes[0].dure_node_id, NODE_0)

        raw[0]["Resources"]["dure_node_" + NODE_1.replace("-", "")] = 1.0
        with self.assertRaises(acceptance.AcceptanceFailure) as raised:
            acceptance._runtime_nodes_from_ray(raw)
        self.assertEqual(raised.exception.code, "DURE_NODE_BINDING_INVALID")

        cpu_only = [
            {
                "Alive": True,
                "NodeID": "ray-cpu",
                "NodeManagerAddress": "10.0.0.9",
                "Resources": {
                    "CPU": 8.0,
                    "dure_node_" + NODE_2.replace("-", ""): 1.0,
                },
            }
        ]
        with self.assertRaises(acceptance.AcceptanceFailure) as raised:
            acceptance._runtime_nodes_from_ray(cpu_only)
        self.assertEqual(raised.exception.code, "GPU_PLACEMENT_INVALID")

    def test_supervisor_times_out_and_terminates_gpu_worker(self):
        class SlowBackend:
            def run(self, contract):
                time.sleep(10)

        original = acceptance.RealVllmRayBackend
        acceptance.RealVllmRayBackend = SlowBackend
        try:
            contract = acceptance.AcceptanceContract.parse(contract_document())
            with self.assertRaises(acceptance.AcceptanceFailure) as raised:
                acceptance._run_real_backend_bounded(contract, timeout=0.05)
            self.assertEqual(raised.exception.code, "ACCEPTANCE_TIMEOUT")
        finally:
            acceptance.RealVllmRayBackend = original


if __name__ == "__main__":
    unittest.main()

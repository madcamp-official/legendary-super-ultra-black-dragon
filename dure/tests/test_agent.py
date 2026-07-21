from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from dure import __version__
from dure.agent import (
    Agent,
    BenchmarkAgentError,
    TaskExecutor,
    _load_build_commit,
    benchmark_profile_fingerprint,
)
from dure.benchmark_runtime import SafeBenchmarkRuntime
from dure.cache_quarantine import (
    ArtifactCacheQuarantineError,
    ArtifactCacheQuarantineExecutor,
)
from dure.command import CommandResult
from dure.http import APIError
from dure.model_cache import (
    MODEL_CACHE_KIND_FULL_SNAPSHOT,
    MODEL_CACHE_KIND_STAGE,
    MODEL_CACHE_VERIFICATION_VERSION,
)
from dure.models import CheckResult, InstalledModelProfile, WorkloadProfile
from dure.pipeline_runtime import (
    RAY_COMPONENT,
    pipeline_contract_detail,
    strict_runtime_contract_digest,
)
from dure.planner import build_plan
from dure.runtime import DEPLOYMENT_IDENTITY_FORMAT
from dure.task import BenchmarkTaskPayload
from tests.helpers import (
    FakeRunner,
    profile,
    strict_pipeline_fixture,
    strict_stage_pipeline_fixture,
)


class QuarantineRunner:
    def __init__(self, *, active_source: Path | None = None):
        self.active_source = active_source
        self.calls: list[tuple[str, ...]] = []

    def exists(self, executable):
        return executable == "docker"

    def run(self, argv, *, timeout=15, env=None):
        command = tuple(argv)
        self.calls.append(command)
        if command == (
            "docker",
            "ps",
            "--filter",
            "label=dure.deployment",
            "--format",
            "{{.ID}}",
        ):
            return CommandResult(
                command,
                0,
                "a" * 12 if self.active_source is not None else "",
            )
        if command[:4] == ("docker", "inspect", "--format", "{{json .Mounts}}"):
            return CommandResult(
                command,
                0,
                json.dumps(
                    [
                        {
                            "Type": "bind",
                            "Source": str(self.active_source),
                            "Destination": "/models/model",
                        }
                    ]
                ),
            )
        return CommandResult(command, 1, stderr="unexpected command")


class AgentRunner:
    def __init__(self):
        self.calls = []
        self.containers = {}
        self.next_container_id = 1

    def exists(self, executable):
        return executable in {"docker", "nvidia-smi"}

    def run(self, argv, *, timeout=15, env=None):
        command = tuple(argv)
        self.calls.append(command)
        if command[:3] == ("docker", "image", "inspect"):
            return CommandResult(command, 0, "available")
        if command[:4] == (
            "docker",
            "inspect",
            "--format",
            DEPLOYMENT_IDENTITY_FORMAT,
        ):
            reference = command[-1]
            container = next(
                (
                    item
                    for item in self.containers.values()
                    if item["name"] == reference or item["id"] == reference
                ),
                None,
            )
            if container is None:
                return CommandResult(command, 1, stderr="No such object")
            labels = container["labels"]
            value = "\t".join(
                (
                    container["id"],
                    container["state"],
                    labels.get("dure.deployment", ""),
                    labels.get("dure.generation", ""),
                    labels.get("dure.node", ""),
                    labels.get("dure.backend", ""),
                    labels.get("dure.pipeline-rank", ""),
                    labels.get("dure.runtime-rank", ""),
                    labels.get("dure.component", ""),
                    labels.get("dure.runtime-contract", ""),
                )
            )
            return CommandResult(command, 0, value)
        if command[:3] == ("docker", "ps", "-q"):
            filters = [
                command[index + 1].removeprefix("label=")
                for index, value in enumerate(command[:-1])
                if value == "--filter" and command[index + 1].startswith("label=")
            ]
            matches = [
                item["id"]
                for item in self.containers.values()
                if item["state"] == "running"
                and all(
                    item["labels"].get(key) == expected
                    for key, expected in (item.split("=", 1) for item in filters)
                )
            ]
            return CommandResult(command, 0, "\n".join(matches))
        if command[:4] == ("docker", "stop", "--time", "30"):
            for container_id in command[4:]:
                if container_id in self.containers:
                    self.containers[container_id]["state"] = "exited"
            return CommandResult(command, 0, "\n".join(command[4:]))
        if command[:2] == ("docker", "rm"):
            container_id = command[-1]
            self.containers.pop(container_id, None)
            return CommandResult(command, 0, container_id)
        if command[:3] == ("docker", "run", "-d"):
            container_id = f"{self.next_container_id:064x}"
            self.next_container_id += 1
            name = command[command.index("--name") + 1]
            labels = {
                command[index + 1].split("=", 1)[0]: command[index + 1].split("=", 1)[1]
                for index, value in enumerate(command[:-1])
                if value == "--label"
            }
            self.containers[container_id] = {
                "id": container_id,
                "name": name,
                "state": "running",
                "labels": labels,
            }
            return CommandResult(command, 0, container_id)
        if command[:2] == ("docker", "exec") and "ray.cluster_resources" in command[-1]:
            return CommandResult(command, 0, json.dumps({"GPU": 1}))
        return CommandResult(command, 0, "ok")


class SafeBenchmarkExecutor:
    def __init__(self, result=None, exception=None):
        self.calls = []
        self.result = result
        self.exception = exception

    def __call__(self, payload, node_profile, cached_model):
        self.calls.append((payload, node_profile, cached_model))
        if self.exception is not None:
            raise self.exception
        return benchmark_result(payload) if self.result is None else self.result


class FakeAgentClient:
    def __init__(self, task, *, fail_complete_once=False):
        self.task = task
        self.requests = []
        self.fail_complete_once = fail_complete_once

    def request(self, method, path, payload=None):
        self.requests.append((method, path, payload))
        if path == "/v1/agent/tasks/claim":
            return {"task": self.task}
        if path.endswith("/complete") and self.fail_complete_once:
            self.fail_complete_once = False
            raise APIError("temporary complete reporting failure")
        return {}


BENCHMARK_NODE_ID = "11111111-1111-4111-8111-111111111111"
BENCHMARK_DURE_COMMIT = "d" * 40


def benchmark_profile():
    node_profile = profile(BENCHMARK_NODE_ID)
    node_profile.installed_models = [
        InstalledModelProfile(
            source="dure",
            model_id="Qwen/Test-AWQ",
            path="/var/lib/dure/models/qwen-test-awq",
            revision="a" * 40,
            quantization="awq",
            size_mib=8192,
            complete=True,
            manifest_digest="sha256:" + "b" * 64,
            cache_kind=MODEL_CACHE_KIND_FULL_SNAPSHOT,
            verification_version=MODEL_CACHE_VERIFICATION_VERSION,
        )
    ]
    return node_profile


def benchmark_payload(node_profile=None):
    node_profile = node_profile or benchmark_profile()
    return {
        "benchmark_id": "22222222-2222-4222-8222-222222222222",
        "release_id": "33333333-3333-4333-8333-333333333333",
        "placement_id": "44444444-4444-4444-8444-444444444444",
        "suite_id": "dure-serving-slo-v1",
        "policy_version": "benchmark-gate-v1",
        "dure_commit": BENCHMARK_DURE_COMMIT,
        "model_id": "qwen-test-awq",
        "model_repository": "Qwen/Test-AWQ",
        "artifact_revision": "a" * 40,
        "artifact_manifest_digest": "sha256:" + "b" * 64,
        "quantization": "awq",
        "runtime_image": "registry.example/vllm@sha256:" + "c" * 64,
        "coordinator_node_id": BENCHMARK_NODE_ID,
        "node_ids": [BENCHMARK_NODE_ID],
        "inventory_fingerprint": benchmark_profile_fingerprint(
            BENCHMARK_NODE_ID, node_profile
        ),
        "workload_id": "short-chat-1k-128",
        "input_tokens": 1024,
        "output_tokens": 128,
        "concurrency": 8,
        "warmup_requests": 20,
        "request_count": 200,
        "duration_seconds": 900.0,
        "apply": True,
    }


def benchmark_metrics():
    return {
        "duration_seconds": 900.0,
        "request_count": 200,
        "warmup_requests": 20,
        "oom_count": 0,
        "crash_count": 0,
        "restart_count": 0,
        "ttft_p95_ms": 120.0,
        "tpot_p95_ms": 18.0,
        "e2e_p95_ms": 900.0,
        "throughput_tps": 42.5,
        "success_rate": 1.0,
        "vram_headroom_pct": 17.5,
        "quality_score": 0.9,
        "network_bandwidth_mbps": None,
        "network_rtt_ms": None,
        "packet_loss_pct": None,
        "nccl_all_reduce_ok": None,
    }


def benchmark_result(payload=None):
    payload = payload or BenchmarkTaskPayload.from_dict(benchmark_payload())
    return {
        "benchmark_id": payload.benchmark_id,
        "workload_id": payload.workload_id,
        "metrics": benchmark_metrics(),
    }


def benchmark_task(task_id="task-benchmark"):
    return {
        "id": task_id,
        "type": "BENCHMARK",
        "payload": benchmark_payload(),
    }


class AgentTaskExecutorTests(unittest.TestCase):
    def test_build_commit_prefers_packaged_metadata_and_validates_fallback(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "build-commit"
            with patch.dict(
                "os.environ", {"DURE_BUILD_COMMIT": "a" * 40}, clear=False
            ):
                self.assertEqual(_load_build_commit(path), "a" * 40)
                path.write_text("b" * 40 + "\n", encoding="ascii")
                self.assertEqual(_load_build_commit(path), "b" * 40)
                path.write_text("not-a-commit\n", encoding="ascii")
                self.assertIsNone(_load_build_commit(path))

    def test_benchmark_fingerprint_ignores_capacity_and_cache_preparation(self):
        baseline = benchmark_profile()
        expected = benchmark_profile_fingerprint(BENCHMARK_NODE_ID, baseline)

        volatile = copy.deepcopy(baseline)
        volatile.memory_available_mib -= 128
        volatile.disk_free_mib -= 256
        volatile.issues.append("temporary-pressure")
        volatile.installed_models = []
        self.assertEqual(
            benchmark_profile_fingerprint(BENCHMARK_NODE_ID, volatile), expected
        )

        changed_identity = copy.deepcopy(baseline)
        changed_identity.gpus[0].driver_version = "999.0"
        self.assertNotEqual(
            benchmark_profile_fingerprint(BENCHMARK_NODE_ID, changed_identity),
            expected,
        )

    def test_task_executor_uses_safe_benchmark_runtime_by_default(self):
        runner = AgentRunner()

        executor = TaskExecutor(
            BENCHMARK_NODE_ID,
            runner=runner,
            build_commit=BENCHMARK_DURE_COMMIT,
        )

        self.assertIsInstance(executor.benchmark_executor, SafeBenchmarkRuntime)
        self.assertIs(executor.benchmark_executor.runner, runner)

    def test_allowed_task_lifecycle_uses_internal_operations(self):
        node_id = "4ec02dee-c5f5-4466-96c5-adc754ef52b8"
        node_profile = profile(node_id)
        runner = AgentRunner()
        with tempfile.TemporaryDirectory() as temporary:
            model_path = Path(temporary) / "model"
            model_path.mkdir()
            (model_path / "config.json").write_text("{}", encoding="utf-8")
            state_path = Path(temporary) / "state.json"
            plan = build_plan([node_profile], image="registry/vllm@sha256:" + "a" * 64)
            plan.model_path = str(model_path)
            payload = {"plan": plan.to_dict(), "generation": plan.generation}
            executor = TaskExecutor(node_id, runner=runner, state_path=state_path)
            with patch("dure.probe.NodeProbe.collect", return_value=node_profile):
                probed = executor.execute({"type": "PROBE", "payload": {}})
                self.assertEqual(probed["profile"]["node_id"], node_id)
                applied = executor.execute(
                    {
                        "type": "APPLY_DEPLOYMENT",
                        "deployment_id": plan.deployment_id,
                        "payload": {**payload, "serve": False},
                    }
                )
                self.assertTrue(applied["checks"])
                verified = executor.execute(
                    {
                        "type": "VERIFY",
                        "deployment_id": plan.deployment_id,
                        "payload": payload,
                    }
                )
                self.assertTrue(verified["ok"])
                stopped = executor.execute(
                    {
                        "type": "STOP_DEPLOYMENT",
                        "deployment_id": plan.deployment_id,
                        "payload": payload,
                    }
                )
                self.assertEqual(stopped["checks"][0]["name"], "deployment-stop")
                restarted = executor.execute(
                    {
                        "type": "RESTART_DEPLOYMENT",
                        "deployment_id": plan.deployment_id,
                        "payload": {**payload, "serve": False},
                    }
                )
                self.assertTrue(restarted["checks"])
        stop_calls = [call for call in runner.calls if call[:2] == ("docker", "stop")]
        self.assertTrue(stop_calls)
        self.assertNotIn("sh", {part for call in runner.calls for part in call})

    def test_stage_apply_and_start_tasks_use_only_the_rank_cache_path(self):
        plan, head, _ = strict_stage_pipeline_fixture()
        assignment = plan.assignments[0]
        successful = CheckResult("test", True, "ok")
        contract = CheckResult(
            "pipeline-rank-contract",
            True,
            pipeline_contract_detail(plan, assignment),
        )
        payload = {
            "plan": plan.to_dict(),
            "generation": plan.generation,
            "serve": False,
            "accept_model_download": True,
            "pull_image": True,
        }

        with tempfile.TemporaryDirectory() as temporary:
            for kind in ("APPLY_DEPLOYMENT", "START_DEPLOYMENT"):
                with self.subTest(kind=kind), patch(
                    "dure.probe.NodeProbe.collect", return_value=head
                ), patch(
                    "dure.orchestrator.validate_strict_stage_cache",
                    return_value=SimpleNamespace(
                        cache_identity_digest="sha256:" + "9" * 64
                    ),
                ) as validate_cache, patch(
                    "dure.orchestrator.ModelStore.ensure",
                    side_effect=AssertionError(
                        "STAGE task must never use legacy model download"
                    ),
                ) as ensure_model, patch(
                    "dure.orchestrator.ContainerRuntime.ensure_image",
                    return_value=successful,
                ), patch(
                    "dure.orchestrator.ContainerRuntime.start_ray",
                    return_value=successful,
                ), patch(
                    "dure.orchestrator.ReadinessVerifier.host_gpu",
                    return_value=successful,
                ), patch(
                    "dure.orchestrator.ReadinessVerifier.container_gpu",
                    return_value=successful,
                ), patch(
                    "dure.orchestrator.ReadinessVerifier.wait_pipeline_rank_contract",
                    return_value=contract,
                ):
                    executor = TaskExecutor(
                        head.node_id,
                        runner=FakeRunner(),
                        state_path=Path(temporary) / f"{kind}.json",
                    )
                    result = executor.execute(
                        {
                            "type": kind,
                            "deployment_id": plan.deployment_id,
                            "payload": payload,
                        }
                    )

                self.assertTrue(all(item["ok"] for item in result["checks"]))
                self.assertIn(
                    "stage-cache", {item["name"] for item in result["checks"]}
                )
                validate_cache.assert_called_once()
                ensure_model.assert_not_called()

    def test_stage_restart_validates_cache_before_any_container_mutation(self):
        plan, head, _ = strict_stage_pipeline_fixture()
        payload = {
            "plan": plan.to_dict(),
            "generation": plan.generation,
            "serve": False,
        }
        runner = FakeRunner()
        with tempfile.TemporaryDirectory() as temporary, patch(
            "dure.probe.NodeProbe.collect", return_value=head
        ), patch(
            "dure.agent.validate_strict_stage_cache",
            side_effect=ValueError("assigned STAGE cache failed integrity validation"),
        ) as validate_cache:
            executor = TaskExecutor(
                head.node_id,
                runner=runner,
                state_path=Path(temporary) / "state.json",
            )
            with self.assertRaisesRegex(ValueError, "integrity"):
                executor.execute(
                    {
                        "type": "RESTART_DEPLOYMENT",
                        "deployment_id": plan.deployment_id,
                        "payload": payload,
                    }
                )

        validate_cache.assert_called_once_with(plan, plan.assignments[0])
        self.assertFalse(
            any(
                call[:2] in {
                    ("docker", "stop"),
                    ("docker", "rm"),
                    ("docker", "run"),
                }
                for call in runner.calls
            )
        )

    def test_stage_emergency_stop_remains_independent_of_cache_validation(self):
        plan, head, _ = strict_stage_pipeline_fixture()
        payload = {"plan": plan.to_dict(), "generation": plan.generation}
        listed = (
            "docker",
            "ps",
            "-q",
            "--filter",
            f"label=dure.deployment={plan.deployment_id}",
            "--filter",
            f"label=dure.generation={plan.generation}",
        )
        runner = FakeRunner(responses={listed: (0, "", "")})
        with tempfile.TemporaryDirectory() as temporary, patch(
            "dure.agent.validate_strict_stage_cache",
            side_effect=AssertionError("emergency STOP must not read cache"),
        ) as validate_cache, patch.object(
            TaskExecutor,
            "_profile",
            side_effect=AssertionError("strict STOP must not probe"),
        ):
            result = TaskExecutor(
                head.node_id,
                runner=runner,
                state_path=Path(temporary) / "state.json",
            ).execute(
                {
                    "type": "STOP_DEPLOYMENT",
                    "deployment_id": plan.deployment_id,
                    "payload": payload,
                }
            )

        validate_cache.assert_not_called()
        self.assertEqual(result["checks"][0]["name"], "deployment-stop")
        self.assertTrue(result["checks"][0]["ok"])

    def test_verify_api_check_runs_only_on_the_assigned_ray_head(self):
        head_id = "4ec02dee-c5f5-4466-96c5-adc754ef52b8"
        worker_id = "4ec02dee-c5f5-4466-96c5-adc754ef52b9"
        worker_two_id = "4ec02dee-c5f5-4466-96c5-adc754ef52ba"
        head = profile(head_id, address="192.168.0.10")
        worker = profile(worker_id, address="192.168.0.11")
        worker_two = profile(worker_two_id, address="192.168.0.12")
        plan = build_plan(
            [head, worker, worker_two],
            model_id="qwen2.5-72b-awq",
            image="registry/vllm@sha256:" + "a" * 64,
        )
        payload = {
            "plan": plan.to_dict(),
            "generation": plan.generation,
            "api": True,
        }
        successful = CheckResult("test", True, "ok")
        executor = TaskExecutor(worker_id, runner=FakeRunner())

        with patch("dure.probe.NodeProbe.collect", return_value=worker), patch(
            "dure.agent.ReadinessVerifier.host_gpu", return_value=successful
        ), patch(
            "dure.agent.ReadinessVerifier.container_gpu", return_value=successful
        ), patch(
            "dure.agent.ReadinessVerifier.ray_cluster", return_value=successful
        ), patch(
            "dure.agent.ReadinessVerifier.api",
            side_effect=AssertionError("worker must not probe the head API"),
        ) as api:
            result = executor.execute(
                {
                    "type": "VERIFY",
                    "deployment_id": plan.deployment_id,
                    "payload": payload,
                }
            )

        self.assertTrue(result["ok"])
        api.assert_not_called()

    def test_start_handler_refuses_foreign_named_container_without_mutation(self):
        node_id = "4ec02dee-c5f5-4466-96c5-adc754ef52b8"
        node_profile = profile(node_id)
        with tempfile.TemporaryDirectory() as temporary:
            model_path = Path(temporary) / "model"
            model_path.mkdir()
            (model_path / "config.json").write_text("{}", encoding="utf-8")
            plan = build_plan(
                [node_profile], image="registry/vllm@sha256:" + "a" * 64
            )
            plan.model_path = str(model_path)
            name = f"dure-ray-{plan.deployment_id}"
            inspect = (
                "docker",
                "inspect",
                "--format",
                DEPLOYMENT_IDENTITY_FORMAT,
                name,
            )
            runner = FakeRunner(
                responses={
                    inspect: (
                        0,
                        f"foreign\texited\tother\t1\t{node_id}",
                        "",
                    )
                }
            )
            executor = TaskExecutor(
                node_id,
                runner=runner,
                state_path=Path(temporary) / "state.json",
            )
            payload = {
                "plan": plan.to_dict(),
                "generation": plan.generation,
                "serve": False,
            }

            with patch("dure.probe.NodeProbe.collect", return_value=node_profile):
                with self.assertRaises(RuntimeError):
                    executor.execute(
                        {
                            "type": "START_DEPLOYMENT",
                            "deployment_id": plan.deployment_id,
                            "payload": payload,
                        }
                    )

        self.assertFalse(any(call[:2] == ("docker", "rm") for call in runner.calls))
        self.assertFalse(any(call[:2] == ("docker", "run") for call in runner.calls))

    def test_verify_and_stop_handlers_require_exact_container_identity(self):
        node_id = "4ec02dee-c5f5-4466-96c5-adc754ef52b8"
        node_profile = profile(node_id)
        plan = build_plan(
            [node_profile], image="registry/vllm@sha256:" + "a" * 64
        )
        payload = {"plan": plan.to_dict(), "generation": plan.generation}
        name = f"dure-ray-{plan.deployment_id}"
        name_inspect = (
            "docker",
            "inspect",
            "--format",
            DEPLOYMENT_IDENTITY_FORMAT,
            name,
        )
        listed = (
            "docker",
            "ps",
            "-q",
            "--filter",
            f"label=dure.deployment={plan.deployment_id}",
            "--filter",
            f"label=dure.generation={plan.generation}",
        )
        id_inspect = (*name_inspect[:-1], "foreign")
        runner = FakeRunner(
            responses={
                name_inspect: (
                    0,
                    f"foreign\trunning\tother\t{plan.generation}\t{node_id}",
                    "",
                ),
                listed: (0, "foreign", ""),
                id_inspect: (
                    0,
                    f"foreign\trunning\tother\t{plan.generation}\t{node_id}",
                    "",
                ),
            }
        )
        executor = TaskExecutor(node_id, runner=runner)

        with patch("dure.probe.NodeProbe.collect", return_value=node_profile):
            with self.assertRaises(RuntimeError):
                executor.execute(
                    {
                        "type": "VERIFY",
                        "deployment_id": plan.deployment_id,
                        "payload": payload,
                    }
                )
            with self.assertRaises(RuntimeError):
                executor.execute(
                    {
                        "type": "STOP_DEPLOYMENT",
                        "deployment_id": plan.deployment_id,
                        "payload": payload,
                    }
                )

        self.assertFalse(any(call[:2] == ("docker", "exec") for call in runner.calls))
        self.assertFalse(any(call[:2] == ("docker", "stop") for call in runner.calls))

    def test_deployment_payload_is_rejected_before_probe_or_host_mutation(self):
        node_id = "4ec02dee-c5f5-4466-96c5-adc754ef52b8"
        node_profile = profile(node_id)
        plan = build_plan(
            [node_profile], image="registry/vllm@sha256:" + "a" * 64
        )
        plan_value = plan.to_dict()
        base = {"plan": plan_value, "generation": plan.generation}
        kinds = (
            "APPLY_DEPLOYMENT",
            "START_DEPLOYMENT",
            "STOP_DEPLOYMENT",
            "RESTART_DEPLOYMENT",
            "VERIFY",
        )
        cases = []
        for kind in kinds:
            cases.extend(
                [
                    (f"{kind}-missing-plan", kind, {"generation": plan.generation}),
                    (f"{kind}-missing-generation", kind, {"plan": plan_value}),
                ]
            )
        cases.extend(
            [
                ("apply-string-serve", "APPLY_DEPLOYMENT", {**base, "serve": "false"}),
                ("start-string-serve", "START_DEPLOYMENT", {**base, "serve": "false"}),
                ("stop-string-pull", "STOP_DEPLOYMENT", {**base, "pull_image": "false"}),
                (
                    "restart-string-download",
                    "RESTART_DEPLOYMENT",
                    {**base, "accept_model_download": "false"},
                ),
                ("verify-string-api", "VERIFY", {**base, "api": "false"}),
                ("unexpected-command", "APPLY_DEPLOYMENT", {**base, "command": ["id"]}),
                (
                    "unexpected-docker-args",
                    "START_DEPLOYMENT",
                    {**base, "docker_args": ["--privileged"]},
                ),
            ]
        )

        with tempfile.TemporaryDirectory() as temporary:
            for index, (name, kind, payload) in enumerate(cases):
                with self.subTest(name=name):
                    runner = AgentRunner()
                    state_path = Path(temporary) / f"state-{index}.json"
                    original_state = b'{"sentinel": true}\n'
                    state_path.write_bytes(original_state)
                    executor = TaskExecutor(
                        node_id,
                        runner=runner,
                        state_path=state_path,
                    )
                    task = {
                        "type": kind,
                        "deployment_id": plan.deployment_id,
                        "payload": payload,
                    }
                    with patch(
                        "dure.probe.NodeProbe.collect",
                        side_effect=AssertionError("invalid payload must not probe"),
                    ) as collect:
                        with self.assertRaises(ValueError):
                            executor.execute(task)
                        collect.assert_not_called()
                    self.assertEqual(runner.calls, [])
                    self.assertEqual(state_path.read_bytes(), original_state)

            runner = AgentRunner()
            state_path = Path(temporary) / "state-identity.json"
            original_state = b'{"sentinel": true}\n'
            state_path.write_bytes(original_state)
            executor = TaskExecutor(node_id, runner=runner, state_path=state_path)
            with patch(
                "dure.probe.NodeProbe.collect",
                side_effect=AssertionError("mismatched identity must not probe"),
            ) as collect:
                with self.assertRaises(ValueError):
                    executor.execute(
                        {
                            "type": "VERIFY",
                            "deployment_id": "00000000-0000-4000-8000-000000000001",
                            "payload": base,
                        }
                    )
                collect.assert_not_called()
            self.assertEqual(runner.calls, [])
            self.assertEqual(state_path.read_bytes(), original_state)

    def test_strict_plan_is_rejected_before_probe_or_host_mutation(self):
        plan, head, _ = strict_pipeline_fixture()
        plan_value = plan.to_dict()
        plan_value["model_path"] = "/etc"
        runner = FakeRunner()
        executor = TaskExecutor(head.node_id, runner=runner)

        with patch(
            "dure.probe.NodeProbe.collect",
            side_effect=AssertionError("invalid strict plan must not probe"),
        ) as collect:
            with self.assertRaises(ValueError):
                executor.execute(
                    {
                        "type": "APPLY_DEPLOYMENT",
                        "deployment_id": plan.deployment_id,
                        "payload": {
                            "plan": plan_value,
                            "generation": plan.generation,
                            "serve": False,
                        },
                    }
                )

        collect.assert_not_called()
        self.assertEqual(runner.calls, [])

    def test_strict_verify_reports_canonical_mapping_and_keeps_api_head_only(self):
        plan, _, worker = strict_pipeline_fixture()
        assignment = plan.assignments[1]
        payload = {
            "plan": plan.to_dict(),
            "generation": plan.generation,
            "api": True,
        }
        host = CheckResult("host-gpu", True, "ok")
        container = CheckResult("container-gpu", True, "ok")
        contract = CheckResult(
            "pipeline-rank-contract",
            True,
            pipeline_contract_detail(plan, assignment),
        )
        executor = TaskExecutor(worker.node_id, runner=FakeRunner())

        with patch("dure.probe.NodeProbe.collect", return_value=worker), patch(
            "dure.agent.ReadinessVerifier.host_gpu", return_value=host
        ), patch(
            "dure.agent.ReadinessVerifier.container_gpu", return_value=container
        ), patch(
            "dure.agent.ReadinessVerifier.pipeline_rank_contract",
            return_value=contract,
        ) as rank_contract, patch(
            "dure.agent.ReadinessVerifier.ray_cluster",
            side_effect=AssertionError("strict verify must not use GPU aggregate"),
        ), patch(
            "dure.agent.ReadinessVerifier.api",
            side_effect=AssertionError("worker must not probe the head API"),
        ) as api:
            result = executor.execute(
                {
                    "type": "VERIFY",
                    "deployment_id": plan.deployment_id,
                    "payload": payload,
                }
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["checks"][-1]["detail"], contract.detail)
        self.assertTrue(rank_contract.call_args.kwargs["require_actors"])
        api.assert_not_called()

    def test_strict_stop_skips_broken_probe_but_rejects_wrong_rank_label(self):
        plan, _, worker = strict_pipeline_fixture()
        assignment = plan.assignments[1]
        original_runtime_contract = strict_runtime_contract_digest(
            plan, assignment, RAY_COMPONENT
        )
        plan.model_path = "/outside/unavailable"
        payload = {"plan": plan.to_dict(), "generation": plan.generation}
        listed = (
            "docker",
            "ps",
            "-q",
            "--filter",
            f"label=dure.deployment={plan.deployment_id}",
            "--filter",
            f"label=dure.generation={plan.generation}",
        )
        inspected = (
            "docker",
            "inspect",
            "--format",
            DEPLOYMENT_IDENTITY_FORMAT,
            "container-id",
        )

        def identity(runtime_rank):
            return "\t".join(
                str(item)
                for item in (
                    "container-id",
                    "running",
                    plan.deployment_id,
                    plan.generation,
                    assignment.node_id,
                    plan.execution_backend,
                    assignment.pipeline_rank,
                    runtime_rank,
                    "ray-node",
                    original_runtime_contract,
                )
            )

        with tempfile.TemporaryDirectory() as temporary:
            runner = FakeRunner(
                responses={
                    listed: (0, "container-id", ""),
                    inspected: (0, identity(1), ""),
                    ("docker", "stop", "--time", "30", "container-id"): (
                        0,
                        "container-id",
                        "",
                    ),
                }
            )
            executor = TaskExecutor(
                worker.node_id,
                runner=runner,
                state_path=Path(temporary) / "state.json",
            )
            with patch.object(
                executor,
                "_profile",
                side_effect=AssertionError("strict STOP must not probe"),
            ) as probe:
                result = executor.execute(
                    {
                        "type": "STOP_DEPLOYMENT",
                        "deployment_id": plan.deployment_id,
                        "payload": payload,
                    }
                )
            probe.assert_not_called()
            self.assertEqual(result["checks"][0]["name"], "deployment-stop")

            rejecting = FakeRunner(
                responses={
                    listed: (0, "container-id", ""),
                    inspected: (0, identity(0), ""),
                }
            )
            rejected_executor = TaskExecutor(
                worker.node_id,
                runner=rejecting,
                state_path=Path(temporary) / "rejected-state.json",
            )
            with patch.object(
                rejected_executor,
                "_profile",
                side_effect=AssertionError("strict STOP must not probe"),
            ) as rejected_probe:
                with self.assertRaises(RuntimeError):
                    rejected_executor.execute(
                        {
                            "type": "STOP_DEPLOYMENT",
                            "deployment_id": plan.deployment_id,
                            "payload": payload,
                        }
                    )
            rejected_probe.assert_not_called()
            self.assertFalse(
                any(call[:2] == ("docker", "stop") for call in rejecting.calls)
            )

    def test_arbitrary_task_type_is_rejected(self):
        with self.assertRaises(ValueError):
            TaskExecutor("node").execute({"type": "SHELL", "payload": {"command": "id"}})

    def test_benchmark_passes_only_validated_contract_and_local_cache_to_safe_executor(self):
        node_profile = benchmark_profile()
        safe_executor = SafeBenchmarkExecutor()
        runner = AgentRunner()
        executor = TaskExecutor(
            BENCHMARK_NODE_ID,
            runner=runner,
            benchmark_executor=safe_executor,
            build_commit=BENCHMARK_DURE_COMMIT,
        )

        with patch("dure.probe.NodeProbe.collect", return_value=node_profile):
            result = executor.execute(
                {"type": "BENCHMARK", "payload": benchmark_payload(node_profile)}
            )

        self.assertEqual(set(result), {"benchmark_id", "workload_id", "metrics"})
        self.assertEqual(result["metrics"], benchmark_metrics())
        self.assertEqual(len(safe_executor.calls), 1)
        validated, observed_profile, cached_model = safe_executor.calls[0]
        self.assertIsInstance(validated, BenchmarkTaskPayload)
        self.assertEqual(validated.node_ids, (BENCHMARK_NODE_ID,))
        self.assertIs(observed_profile, node_profile)
        self.assertEqual(cached_model.path, "/var/lib/dure/models/qwen-test-awq")
        self.assertEqual(runner.calls, [])

    def test_benchmark_can_prepare_exact_assets_before_closed_execution(self):
        node_profile = benchmark_profile()
        payload = benchmark_payload(node_profile)
        payload.update(prepare_model=True, pull_image=True)
        safe_executor = SafeBenchmarkExecutor()
        preparation = Mock()
        executor = TaskExecutor(
            BENCHMARK_NODE_ID,
            benchmark_executor=safe_executor,
            preparation_executor=preparation,
            build_commit=BENCHMARK_DURE_COMMIT,
        )
        task_id = "99999999-9999-4999-8999-999999999999"

        with patch("dure.probe.NodeProbe.collect", return_value=node_profile):
            result = executor.execute(
                {"id": task_id, "type": "BENCHMARK", "payload": payload}
            )

        validated = BenchmarkTaskPayload.from_dict(payload)
        preparation.prepare_benchmark_model.assert_called_once_with(
            task_id, validated
        )
        preparation.prepare_benchmark_image.assert_called_once_with(validated)
        self.assertEqual(result["benchmark_id"], validated.benchmark_id)
        self.assertEqual(len(safe_executor.calls), 1)

    def test_benchmark_build_identity_is_checked_before_asset_mutation(self):
        node_profile = benchmark_profile()
        payload = benchmark_payload(node_profile)
        payload.update(prepare_model=True, pull_image=True)
        preparation = Mock()
        executor = TaskExecutor(
            BENCHMARK_NODE_ID,
            benchmark_executor=SafeBenchmarkExecutor(),
            preparation_executor=preparation,
            build_commit="f" * 40,
        )

        with self.assertRaisesRegex(ValueError, "Agent build"):
            executor.execute(
                {
                    "id": "99999999-9999-4999-8999-999999999999",
                    "type": "BENCHMARK",
                    "payload": payload,
                }
            )

        preparation.prepare_benchmark_model.assert_not_called()
        preparation.prepare_benchmark_image.assert_not_called()

    def test_benchmark_asset_preparation_is_refused_while_workload_is_active(self):
        node_profile = benchmark_profile()
        node_profile.workloads.append(
            WorkloadProfile(
                name="user-container",
                runtime="docker",
                image="example/image:latest",
                status="running",
            )
        )
        payload = benchmark_payload(node_profile)
        payload.update(prepare_model=True, pull_image=True)
        preparation = Mock()
        executor = TaskExecutor(
            BENCHMARK_NODE_ID,
            benchmark_executor=SafeBenchmarkExecutor(),
            preparation_executor=preparation,
            build_commit=BENCHMARK_DURE_COMMIT,
        )

        with patch("dure.probe.NodeProbe.collect", return_value=node_profile):
            with self.assertRaisesRegex(ValueError, "another workload"):
                executor.execute(
                    {
                        "id": "99999999-9999-4999-8999-999999999999",
                        "type": "BENCHMARK",
                        "payload": payload,
                    }
                )

        preparation.prepare_benchmark_model.assert_not_called()
        preparation.prepare_benchmark_image.assert_not_called()

    def test_benchmark_rejects_arbitrary_execution_and_secret_fields(self):
        safe_executor = SafeBenchmarkExecutor()
        executor = TaskExecutor(
            BENCHMARK_NODE_ID,
            benchmark_executor=safe_executor,
            build_commit=BENCHMARK_DURE_COMMIT,
        )
        unsafe_fields = {
            "command": ["id"],
            "docker_args": ["--privileged"],
            "env": {"TOKEN": "secret"},
            "mounts": ["/etc:/host"],
            "python": "import os",
            "prompt": "private prompt",
            "token": "model-token",
            "secret": "secret-value",
            "log": "raw output",
            "logs": ["raw output"],
            "stdout": "raw output",
            "stderr": "raw error",
            "host_path": "/etc",
            "model_path": "/tmp/model",
        }

        for field, value in unsafe_fields.items():
            with self.subTest(field=field):
                payload = benchmark_payload()
                payload[field] = value
                with self.assertRaisesRegex(
                    ValueError, "unexpected BENCHMARK payload field"
                ) as raised:
                    executor.execute({"type": "BENCHMARK", "payload": payload})
                self.assertNotIn(field, str(raised.exception))
                self.assertNotIn(str(value), str(raised.exception))

        self.assertEqual(safe_executor.calls, [])

    def test_benchmark_rejects_executor_result_schema_identity_and_metric_values(self):
        node_profile = benchmark_profile()
        valid_result = benchmark_result()
        invalid_results = []

        extra_outer = copy.deepcopy(valid_result)
        extra_outer["stdout"] = "raw-secret-output"
        invalid_results.append(extra_outer)

        wrong_identity = copy.deepcopy(valid_result)
        wrong_identity["benchmark_id"] = "55555555-5555-4555-8555-555555555555"
        invalid_results.append(wrong_identity)

        extra_metric = copy.deepcopy(valid_result)
        extra_metric["metrics"]["prompt"] = "private prompt"
        invalid_results.append(extra_metric)

        nan_metric = copy.deepcopy(valid_result)
        nan_metric["metrics"]["success_rate"] = float("nan")
        invalid_results.append(nan_metric)

        boolean_integer = copy.deepcopy(valid_result)
        boolean_integer["metrics"]["oom_count"] = True
        invalid_results.append(boolean_integer)

        out_of_range = copy.deepcopy(valid_result)
        out_of_range["metrics"]["vram_headroom_pct"] = 101
        invalid_results.append(out_of_range)

        invalid_nccl = copy.deepcopy(valid_result)
        invalid_nccl["metrics"]["nccl_all_reduce_ok"] = "yes"
        invalid_results.append(invalid_nccl)

        for field, value in (
            ("network_bandwidth_mbps", 1000.0),
            ("network_rtt_ms", 1.0),
            ("packet_loss_pct", 0.0),
            ("nccl_all_reduce_ok", True),
        ):
            multi_node_metric = copy.deepcopy(valid_result)
            multi_node_metric["metrics"][field] = value
            invalid_results.append(multi_node_metric)

        for invalid_result in invalid_results:
            with self.subTest(result=invalid_result):
                safe_executor = SafeBenchmarkExecutor(result=invalid_result)
                executor = TaskExecutor(
                    BENCHMARK_NODE_ID,
                    benchmark_executor=safe_executor,
                    build_commit=BENCHMARK_DURE_COMMIT,
                )
                with patch("dure.probe.NodeProbe.collect", return_value=node_profile):
                    with self.assertRaises(BenchmarkAgentError) as raised:
                        executor.execute(
                            {
                                "type": "BENCHMARK",
                                "payload": benchmark_payload(node_profile),
                            }
                        )
                self.assertEqual(
                    raised.exception.failure_code,
                    "BENCHMARK_EXECUTION_FAILED",
                )
                self.assertNotIn("raw-secret-output", str(raised.exception))
                self.assertNotIn("private prompt", str(raised.exception))

    def test_benchmark_requires_strict_true_apply_before_probe_or_execution(self):
        safe_executor = SafeBenchmarkExecutor()
        executor = TaskExecutor(
            BENCHMARK_NODE_ID,
            benchmark_executor=safe_executor,
            build_commit=BENCHMARK_DURE_COMMIT,
        )

        for value, message in ((False, "explicit apply"), ("true", "must be a boolean")):
            with self.subTest(value=value), patch(
                "dure.probe.NodeProbe.collect",
                side_effect=AssertionError("probe must not run"),
            ):
                payload = benchmark_payload()
                payload["apply"] = value
                with self.assertRaisesRegex(ValueError, message):
                    executor.execute({"type": "BENCHMARK", "payload": payload})

        self.assertEqual(safe_executor.calls, [])

    def test_benchmark_requires_matching_agent_build_before_probe(self):
        safe_executor = SafeBenchmarkExecutor()
        for build_commit in (None, "f" * 40, "invalid"):
            with self.subTest(build_commit=build_commit):
                executor = TaskExecutor(
                    BENCHMARK_NODE_ID,
                    benchmark_executor=safe_executor,
                    build_commit=build_commit,
                )
                with patch(
                    "dure.probe.NodeProbe.collect",
                    side_effect=AssertionError("probe must not run"),
                ):
                    with self.assertRaises(BenchmarkAgentError) as raised:
                        executor.execute(
                            {"type": "BENCHMARK", "payload": benchmark_payload()}
                        )
                self.assertEqual(
                    raised.exception.failure_code,
                    "BENCHMARK_PAYLOAD_REJECTED",
                )
        self.assertEqual(safe_executor.calls, [])

    def test_build_mismatch_still_reconciles_an_expired_exact_container(self):
        inspect_count = 0
        container_id = "a" * 64

        def identity(state):
            return "\t".join(
                (
                    container_id,
                    state,
                    "2000-01-01T00:00:00Z",
                    "true",
                    "benchmark",
                    "22222222-2222-4222-8222-222222222222",
                    "33333333-3333-4333-8333-333333333333",
                    "44444444-4444-4444-8444-444444444444",
                    "short-chat-1k-128",
                    "",
                )
            )

        def respond(command):
            nonlocal inspect_count
            if command[:3] == ("docker", "container", "inspect"):
                inspect_count += 1
                return 0, identity("running" if inspect_count < 3 else "exited"), ""
            if command[:4] == ("docker", "stop", "--time", "30"):
                return 0, container_id, ""
            if command == ("docker", "rm", container_id):
                return 0, container_id, ""
            raise AssertionError(f"unexpected command: {command}")

        runner = FakeRunner(response_factory=respond)
        executor = TaskExecutor(
            BENCHMARK_NODE_ID,
            runner=runner,
            build_commit="f" * 40,
        )

        with self.assertRaises(BenchmarkAgentError) as raised:
            executor.execute(benchmark_task())

        self.assertEqual(
            raised.exception.failure_code, "BENCHMARK_PAYLOAD_REJECTED"
        )
        self.assertIn(
            ("docker", "stop", "--time", "30", container_id), runner.calls
        )
        self.assertIn(("docker", "rm", container_id), runner.calls)
        self.assertFalse(any(call[0] == "nvidia-smi" for call in runner.calls))

    def test_benchmark_rejects_untrusted_contract_values_before_execution(self):
        safe_executor = SafeBenchmarkExecutor()
        executor = TaskExecutor(
            BENCHMARK_NODE_ID,
            benchmark_executor=safe_executor,
            build_commit=BENCHMARK_DURE_COMMIT,
        )
        invalid_values = (
            ("suite_id", "custom-suite"),
            ("policy_version", "custom-policy"),
            ("dure_commit", "main"),
            ("workload_id", "custom-workload"),
            ("artifact_revision", "main"),
            ("artifact_manifest_digest", "latest"),
            ("runtime_image", "registry.example/vllm:latest"),
            ("quantization", "custom"),
            ("release_id", "not-a-uuid"),
        )

        for field, value in invalid_values:
            with self.subTest(field=field):
                payload = benchmark_payload()
                payload[field] = value
                with self.assertRaises(ValueError):
                    executor.execute({"type": "BENCHMARK", "payload": payload})

        other_node = "55555555-5555-4555-8555-555555555555"
        for node_ids in (
            [other_node, BENCHMARK_NODE_ID],
            [BENCHMARK_NODE_ID, BENCHMARK_NODE_ID],
        ):
            with self.subTest(node_ids=node_ids):
                payload = benchmark_payload()
                payload["node_ids"] = node_ids
                with self.assertRaisesRegex(ValueError, "sorted"):
                    executor.execute({"type": "BENCHMARK", "payload": payload})

        self.assertEqual(safe_executor.calls, [])

    def test_benchmark_fails_closed_for_wrong_coordinator_and_multinode(self):
        safe_executor = SafeBenchmarkExecutor()
        executor = TaskExecutor(
            BENCHMARK_NODE_ID,
            benchmark_executor=safe_executor,
            build_commit=BENCHMARK_DURE_COMMIT,
        )
        other_node = "55555555-5555-4555-8555-555555555555"

        wrong_coordinator = benchmark_payload()
        wrong_coordinator["coordinator_node_id"] = other_node
        with self.assertRaisesRegex(ValueError, "coordinator"):
            executor.execute({"type": "BENCHMARK", "payload": wrong_coordinator})

        multinode = benchmark_payload()
        multinode["node_ids"] = [BENCHMARK_NODE_ID, other_node]
        with self.assertRaisesRegex(ValueError, "multi-node"):
            executor.execute({"type": "BENCHMARK", "payload": multinode})

        self.assertEqual(safe_executor.calls, [])

    def test_benchmark_requires_current_fingerprint_and_exact_local_cache(self):
        safe_executor = SafeBenchmarkExecutor()
        executor = TaskExecutor(
            BENCHMARK_NODE_ID,
            benchmark_executor=safe_executor,
            build_commit=BENCHMARK_DURE_COMMIT,
        )
        node_profile = benchmark_profile()

        stale = benchmark_payload(node_profile)
        stale["inventory_fingerprint"] = "sha256:" + "f" * 64
        with patch("dure.probe.NodeProbe.collect", return_value=node_profile):
            with self.assertRaisesRegex(ValueError, "fingerprint mismatch"):
                executor.execute({"type": "BENCHMARK", "payload": stale})

        wrong_cache = benchmark_profile()
        wrong_cache.installed_models[0].revision = "d" * 40
        with patch("dure.probe.NodeProbe.collect", return_value=wrong_cache):
            with self.assertRaisesRegex(ValueError, "exactly one complete local cache"):
                executor.execute(
                    {"type": "BENCHMARK", "payload": benchmark_payload(wrong_cache)}
                )

        hub_cache = benchmark_profile()
        hub_cache.installed_models[0].source = "huggingface-cache"
        with patch("dure.probe.NodeProbe.collect", return_value=hub_cache):
            with self.assertRaisesRegex(ValueError, "exactly one complete local cache"):
                executor.execute(
                    {"type": "BENCHMARK", "payload": benchmark_payload(hub_cache)}
                )

        wrong_digest = benchmark_profile()
        wrong_digest.installed_models[0].manifest_digest = "sha256:" + "f" * 64
        with patch("dure.probe.NodeProbe.collect", return_value=wrong_digest):
            with self.assertRaisesRegex(ValueError, "exactly one complete local cache"):
                executor.execute(
                    {
                        "type": "BENCHMARK",
                        "payload": benchmark_payload(wrong_digest),
                    }
                )

        stage_cache = benchmark_profile()
        stage_cache.installed_models[0].cache_kind = MODEL_CACHE_KIND_STAGE
        with patch("dure.probe.NodeProbe.collect", return_value=stage_cache):
            with self.assertRaisesRegex(ValueError, "FULL_SNAPSHOT"):
                executor.execute(
                    {"type": "BENCHMARK", "payload": benchmark_payload(stage_cache)}
                )

        self.assertEqual(safe_executor.calls, [])

    def test_benchmark_rejects_cache_outside_trusted_root_after_symlink_resolution(self):
        safe_executor = SafeBenchmarkExecutor()
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            trusted_root = base / "trusted"
            outside = base / "outside"
            trusted_root.mkdir()
            outside.mkdir()
            linked_model = trusted_root / "linked-model"
            linked_model.symlink_to(outside, target_is_directory=True)

            node_profile = benchmark_profile()
            node_profile.installed_models[0].path = str(linked_model)
            payload = benchmark_payload(node_profile)
            executor = TaskExecutor(
                BENCHMARK_NODE_ID,
                benchmark_executor=safe_executor,
                build_commit=BENCHMARK_DURE_COMMIT,
            )

            with patch("dure.agent.DURE_MODEL_ROOT", trusted_root), patch(
                "dure.probe.NodeProbe.collect", return_value=node_profile
            ):
                with self.assertRaises(BenchmarkAgentError) as raised:
                    executor.execute({"type": "BENCHMARK", "payload": payload})

        self.assertEqual(
            raised.exception.failure_code,
            "BENCHMARK_ARTIFACT_UNAVAILABLE",
        )
        self.assertEqual(safe_executor.calls, [])


class AgentBenchmarkFailureTests(unittest.TestCase):
    def _agent(self, temporary, task, benchmark_executor=None):
        base = Path(temporary)
        agent = Agent(
            {
                "server": "https://control.example",
                "credential": "credential",
                "node_id": BENCHMARK_NODE_ID,
                "state_file": str(base / "state.json"),
            },
            history_path=base / "history.json",
            benchmark_executor=benchmark_executor,
            build_commit=BENCHMARK_DURE_COMMIT,
        )
        agent.client = FakeAgentClient(task)
        return agent

    def test_benchmark_success_replays_after_reporting_failure_and_agent_upgrade(self):
        with tempfile.TemporaryDirectory() as temporary:
            task = benchmark_task()
            agent = self._agent(temporary, task)
            agent.client = FakeAgentClient(task, fail_complete_once=True)
            executions = []

            def succeed(observed_task):
                executions.append(observed_task)
                return benchmark_result()

            agent.executor.execute = succeed
            with self.assertRaises(APIError):
                agent.once()

            first_history = json.loads(
                agent.history_path.read_text(encoding="utf-8")
            )
            self.assertEqual(
                first_history["completed"]["task-benchmark"]["status"],
                "complete",
            )
            self.assertEqual(
                first_history["completed"]["task-benchmark"][
                    "executed_dure_commit"
                ],
                BENCHMARK_DURE_COMMIT,
            )

            agent.build_commit = "f" * 40
            self.assertTrue(agent.once())
            final_history = json.loads(
                agent.history_path.read_text(encoding="utf-8")
            )
            complete_requests = [
                request
                for request in agent.client.requests
                if request[1].endswith("/complete")
            ]
            heartbeat_requests = [
                request
                for request in agent.client.requests
                if request[1] == "/v1/agent/heartbeat"
            ]

        self.assertEqual(len(executions), 1)
        self.assertEqual(len(complete_requests), 2)
        self.assertEqual(len(heartbeat_requests), 2)
        self.assertTrue(
            all(
                request[2]["agent_version"] == __version__
                for request in heartbeat_requests
            )
        )
        self.assertEqual(
            final_history["completed"]["task-benchmark"]["status"],
            "complete",
        )
        self.assertEqual(
            final_history["completed"]["task-benchmark"][
                "executed_dure_commit"
            ],
            BENCHMARK_DURE_COMMIT,
        )

    def test_non_benchmark_complete_report_failure_also_preserves_success(self):
        task = {"id": "task-probe", "type": "PROBE", "payload": {}}
        with tempfile.TemporaryDirectory() as temporary:
            agent = self._agent(temporary, task)
            agent.client = FakeAgentClient(task, fail_complete_once=True)
            executions = []

            def succeed(observed_task):
                executions.append(observed_task)
                return {"profile": {"node_id": BENCHMARK_NODE_ID}}

            agent.executor.execute = succeed
            with self.assertRaises(APIError):
                agent.once()
            self.assertEqual(
                agent.history["completed"]["task-probe"]["status"],
                "complete",
            )

            self.assertTrue(agent.once())

        self.assertEqual(len(executions), 1)
        self.assertEqual(
            len(
                [
                    request
                    for request in agent.client.requests
                    if request[1].endswith("/complete")
                ]
            ),
            2,
        )

    def test_running_exact_benchmark_is_deferred_without_terminal_report(self):
        class Deferred(RuntimeError):
            defer_benchmark = True

        with tempfile.TemporaryDirectory() as temporary:
            agent = self._agent(temporary, benchmark_task())

            def defer(_task):
                raise Deferred("exact benchmark is still running")

            agent.executor.execute = defer
            with self.assertLogs("dure.agent", level="WARNING"):
                self.assertTrue(agent.once())

            terminal_requests = [
                request
                for request in agent.client.requests
                if request[1].endswith(("/complete", "/fail"))
            ]

        self.assertEqual(terminal_requests, [])
        self.assertNotIn("task-benchmark", agent.history.get("completed", {}))

    def test_benchmark_failure_stores_and_sends_only_closed_failure_code(self):
        with tempfile.TemporaryDirectory() as temporary:
            agent = self._agent(temporary, benchmark_task())

            def fail(_task):
                raise RuntimeError("raw secret runtime output")

            agent.executor.execute = fail
            with self.assertLogs("dure.agent", level="ERROR") as captured:
                self.assertTrue(agent.once())

            history_text = agent.history_path.read_text(encoding="utf-8")
            fail_request = next(
                request
                for request in agent.client.requests
                if request[1].endswith("/fail")
            )

        self.assertEqual(
            fail_request[2], {"error": "BENCHMARK_EXECUTION_FAILED"}
        )
        self.assertIn("BENCHMARK_EXECUTION_FAILED", history_text)
        self.assertNotIn("raw secret runtime output", history_text)
        self.assertNotIn("raw secret runtime output", "\n".join(captured.output))

    def test_benchmark_known_failure_code_is_preserved_without_raw_message(self):
        failure = BenchmarkAgentError(
            "private artifact path",
            failure_code="BENCHMARK_ARTIFACT_UNAVAILABLE",
        )
        with tempfile.TemporaryDirectory() as temporary:
            agent = self._agent(temporary, benchmark_task())

            def fail(_task):
                raise failure

            agent.executor.execute = fail
            agent.once()
            persisted = json.loads(agent.history_path.read_text(encoding="utf-8"))
            fail_request = next(
                request
                for request in agent.client.requests
                if request[1].endswith("/fail")
            )

        self.assertEqual(
            persisted["completed"]["task-benchmark"],
            {
                "status": "failed",
                "error": "BENCHMARK_ARTIFACT_UNAVAILABLE",
            },
        )
        self.assertEqual(
            fail_request[2], {"error": "BENCHMARK_ARTIFACT_UNAVAILABLE"}
        )

    def test_benchmark_replay_sanitizes_legacy_raw_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            history_path = base / "history.json"
            history_path.write_text(
                json.dumps(
                    {
                        "completed": {
                            "task-benchmark": {
                                "status": "failed",
                                "error": "legacy raw secret output",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            agent = self._agent(temporary, benchmark_task())
            agent.once()
            history_text = history_path.read_text(encoding="utf-8")
            fail_request = next(
                request
                for request in agent.client.requests
                if request[1].endswith("/fail")
            )

        self.assertNotIn("legacy raw secret output", history_text)
        self.assertEqual(
            fail_request[2], {"error": "BENCHMARK_EXECUTION_FAILED"}
        )

    def test_benchmark_replay_rejects_success_without_execution_commit(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            history_path = base / "history.json"
            history_path.write_text(
                json.dumps(
                    {
                        "completed": {
                            "task-benchmark": {
                                "status": "complete",
                                "result": benchmark_result(),
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            agent = self._agent(temporary, benchmark_task())
            agent.once()
            persisted = json.loads(history_path.read_text(encoding="utf-8"))
            fail_request = next(
                request
                for request in agent.client.requests
                if request[1].endswith("/fail")
            )

        self.assertEqual(
            persisted["completed"]["task-benchmark"],
            {"status": "failed", "error": "BENCHMARK_PAYLOAD_REJECTED"},
        )
        self.assertEqual(
            fail_request[2], {"error": "BENCHMARK_PAYLOAD_REJECTED"}
        )

    def test_non_benchmark_failure_behavior_is_unchanged(self):
        task = {"id": "task-verify", "type": "VERIFY", "payload": {}}
        with tempfile.TemporaryDirectory() as temporary:
            agent = self._agent(temporary, task)

            def fail(_task):
                raise RuntimeError("ordinary verification failure")

            agent.executor.execute = fail
            with self.assertLogs("dure.agent", level="ERROR"):
                agent.once()
            persisted = json.loads(agent.history_path.read_text(encoding="utf-8"))
            fail_request = next(
                request
                for request in agent.client.requests
                if request[1].endswith("/fail")
            )

        self.assertEqual(
            persisted["completed"]["task-verify"]["error"],
            "ordinary verification failure",
        )
        self.assertEqual(
            fail_request[2], {"error": "ordinary verification failure"}
        )


class ArtifactCacheQuarantineAgentTests(unittest.TestCase):
    node_id = "4ec02dee-c5f5-4466-96c5-adc754ef52b8"
    task_id = "5ec02dee-c5f5-4466-96c5-adc754ef52b8"
    identity = "sha256:" + "b" * 64

    def _task(self, **extra):
        payload = {
            "node_id": self.node_id,
            "cache_kind": "FULL_SNAPSHOT",
            "cache_identity_digest": self.identity,
            **extra,
        }
        return {
            "id": self.task_id,
            "type": "QUARANTINE_ARTIFACT_CACHE",
            "payload": payload,
        }

    def test_exact_inactive_cache_is_atomically_quarantined_and_retry_safe(self):
        with tempfile.TemporaryDirectory() as temporary:
            model_root = Path(temporary) / "models"
            source = model_root / ("sha256-" + "b" * 64)
            source.mkdir(parents=True)
            (source / "damaged-file").write_text("retained", encoding="utf-8")
            quarantine = ArtifactCacheQuarantineExecutor(
                self.node_id,
                runner=QuarantineRunner(),
                model_root=model_root,
            )
            executor = TaskExecutor(
                self.node_id,
                runner=FakeRunner(),
                quarantine_executor=quarantine,
            )

            first = executor.execute(self._task())
            quarantine.runner = FakeRunner()
            second = executor.execute(self._task())

            target = (
                model_root
                / ".dure-quarantine"
                / (self.task_id + "-full_snapshot-sha256-" + "b" * 64)
            )
            self.assertFalse(source.exists())
            self.assertEqual((target / "damaged-file").read_text(), "retained")
            self.assertEqual(first["status"], "QUARANTINED")
            self.assertEqual(second["status"], "ALREADY_QUARANTINED")
            self.assertNotIn("path", first)

    def test_active_cache_mount_is_denied_before_the_rename(self):
        with tempfile.TemporaryDirectory() as temporary:
            model_root = Path(temporary) / "models"
            source = model_root / ("sha256-" + "b" * 64)
            source.mkdir(parents=True)
            executor = ArtifactCacheQuarantineExecutor(
                self.node_id,
                runner=QuarantineRunner(active_source=source),
                model_root=model_root,
            )

            with self.assertRaises(ArtifactCacheQuarantineError) as raised:
                executor.execute(self._task())

            self.assertEqual(
                raised.exception.failure_code, "CACHE_QUARANTINE_CACHE_ACTIVE"
            )
            self.assertTrue(source.exists())

    def test_stage_cache_uses_the_fixed_stage_root_and_same_quarantine_root(self):
        with tempfile.TemporaryDirectory() as temporary:
            model_root = Path(temporary) / "models"
            source = model_root / "stages" / ("sha256-" + "b" * 64)
            source.mkdir(parents=True)
            executor = ArtifactCacheQuarantineExecutor(
                self.node_id,
                runner=QuarantineRunner(),
                model_root=model_root,
            )

            result = executor.execute(self._task(cache_kind="STAGE"))

            target = (
                model_root
                / ".dure-quarantine"
                / (self.task_id + "-stage-sha256-" + "b" * 64)
            )
            self.assertEqual(result["status"], "QUARANTINED")
            self.assertFalse(source.exists())
            self.assertTrue(target.is_dir())

    def test_payload_is_closed_and_cannot_supply_a_host_path(self):
        executor = ArtifactCacheQuarantineExecutor(
            self.node_id,
            runner=QuarantineRunner(),
        )

        for field, value in (
            ("path", "/etc"),
            ("url", "https://example.invalid/model"),
            ("command", "rm"),
            ("env", {"TOKEN": "secret"}),
            ("docker_args", ["--privileged"]),
        ):
            with self.subTest(field=field), self.assertRaises(
                ArtifactCacheQuarantineError
            ) as raised:
                executor.execute(self._task(**{field: value}))
            self.assertEqual(
                raised.exception.failure_code,
                "CACHE_QUARANTINE_PAYLOAD_REJECTED",
            )

from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dure.agent import (
    Agent,
    BenchmarkAgentError,
    TaskExecutor,
    _load_build_commit,
    benchmark_profile_fingerprint,
)
from dure.benchmark_runtime import SafeBenchmarkRuntime
from dure.command import CommandResult
from dure.http import APIError
from dure.models import InstalledModelProfile
from dure.planner import build_plan
from dure.task import BenchmarkTaskPayload
from tests.helpers import FakeRunner, profile


class AgentRunner:
    def __init__(self):
        self.calls = []

    def exists(self, executable):
        return executable in {"docker", "nvidia-smi"}

    def run(self, argv, *, timeout=15, env=None):
        command = tuple(argv)
        self.calls.append(command)
        if command[:3] == ("docker", "image", "inspect"):
            return CommandResult(command, 0, "available")
        if command[:2] == ("docker", "inspect"):
            return CommandResult(command, 1, stderr="not found")
        if command[:3] == ("docker", "ps", "-q"):
            return CommandResult(command, 0, "owned-container")
        if command[:4] == ("docker", "stop", "--time", "30"):
            return CommandResult(command, 0, "owned-container")
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

    def test_benchmark_fingerprint_ignores_volatile_capacity_only(self):
        baseline = benchmark_profile()
        expected = benchmark_profile_fingerprint(BENCHMARK_NODE_ID, baseline)

        volatile = copy.deepcopy(baseline)
        volatile.memory_available_mib -= 128
        volatile.disk_free_mib -= 256
        volatile.issues.append("temporary-pressure")
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
            payload = {"plan": plan.to_dict(), "generation": plan.generation, "serve": False}
            executor = TaskExecutor(node_id, runner=runner, state_path=state_path)
            with patch("dure.probe.NodeProbe.collect", return_value=node_profile):
                probed = executor.execute({"type": "PROBE", "payload": {}})
                self.assertEqual(probed["profile"]["node_id"], node_id)
                verified = executor.execute({"type": "VERIFY", "payload": payload})
                self.assertTrue(verified["ok"])
                applied = executor.execute({"type": "APPLY_DEPLOYMENT", "payload": payload})
                self.assertTrue(applied["checks"])
                stopped = executor.execute({"type": "STOP_DEPLOYMENT", "payload": payload})
                self.assertEqual(stopped["checks"][0]["name"], "deployment-stop")
                restarted = executor.execute({"type": "RESTART_DEPLOYMENT", "payload": payload})
                self.assertTrue(restarted["checks"])
        stop_calls = [call for call in runner.calls if call[:2] == ("docker", "stop")]
        self.assertTrue(stop_calls)
        self.assertNotIn("sh", {part for call in runner.calls for part in call})

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

        self.assertEqual(len(executions), 1)
        self.assertEqual(len(complete_requests), 2)
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

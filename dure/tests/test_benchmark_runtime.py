from __future__ import annotations

import json
import math
import sys
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from dure.benchmark_runtime import (
    BENCHMARK_CONTAINER_GRACE_SECONDS,
    BENCHMARK_ENTRYPOINT_CONTAINER_PATH,
    BENCHMARK_WORKLOADS,
    MAX_BENCHMARK_OUTPUT_BYTES,
    NVIDIA_COMPUTE_QUERY_COMMAND,
    BenchmarkRuntimeDeferred,
    BenchmarkRuntimeError,
    SafeBenchmarkRuntime,
)
from dure.command import CommandResult, SubprocessRunner
from dure.model_cache import (
    MODEL_CACHE_KIND_FULL_SNAPSHOT,
    MODEL_CACHE_KIND_STAGE,
    MODEL_CACHE_VERIFICATION_VERSION,
    build_model_cache_marker,
)
from dure.models import GPUProfile, InstalledModelProfile, WorkloadProfile
from dure.task import BenchmarkTaskPayload

from .helpers import FakeRunner, profile


NODE_ID = "11111111-1111-4111-8111-111111111111"
BENCHMARK_CONTAINER_LIST = (
    "docker",
    "container",
    "ls",
    "--all",
    "--no-trunc",
    "--filter",
    "label=dure.managed=true",
    "--filter",
    "label=dure.kind=benchmark",
    "--format",
    "{{.ID}}",
)


def payload(
    *,
    apply: bool = True,
    node_ids: list[str] | None = None,
    **overrides,
):
    value = {
        "benchmark_id": "22222222-2222-4222-8222-222222222222",
        "release_id": "33333333-3333-4333-8333-333333333333",
        "placement_id": "44444444-4444-4444-8444-444444444444",
        "suite_id": "dure-serving-slo-v1",
        "policy_version": "benchmark-gate-v3",
        "model_id": "qwen-test-awq",
        "model_repository": "Qwen/Test-AWQ",
        "artifact_revision": "a" * 40,
        "artifact_manifest_digest": "sha256:" + "b" * 64,
        "quantization": "awq",
        "runtime_image": "registry.example/vllm@sha256:" + "c" * 64,
        "coordinator_node_id": NODE_ID,
        "node_ids": node_ids or [NODE_ID],
        "inventory_fingerprint": "sha256:" + "d" * 64,
        "workload_id": "short-chat-1k-128",
        "input_tokens": 1024,
        "output_tokens": 128,
        "concurrency": 8,
        "warmup_requests": 2,
        "request_count": 20,
        "duration_seconds": 240.0,
        "apply": apply,
    }
    if "dure_commit" in BenchmarkTaskPayload.__dataclass_fields__:
        value["dure_commit"] = "e" * 40
    value.update(overrides)
    return BenchmarkTaskPayload.from_dict(value)


def metrics(**overrides):
    workload = BENCHMARK_WORKLOADS["short-chat-1k-128"]
    value = {
        "warmup_requests": workload.warmup_requests,
        "request_count": workload.request_count,
        "duration_seconds": 123.5,
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
    }
    value.update(overrides)
    return value


def container_identity(
    *, state: str, deployment: str = "", started_at: str | None = None
) -> str:
    started_at = started_at or datetime.now(timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )
    return "\t".join(
        (
            "a" * 64,
            state,
            started_at,
            "true",
            "benchmark",
            "22222222-2222-4222-8222-222222222222",
            "33333333-3333-4333-8333-333333333333",
            "44444444-4444-4444-8444-444444444444",
            "short-chat-1k-128",
            deployment,
        )
    )


class BenchmarkRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.model_path = Path(self.temporary.name) / "model"
        self.model_path.mkdir()
        (self.model_path / "config.json").write_text(
            json.dumps({"max_position_embeddings": 8192}), encoding="utf-8"
        )
        (self.model_path / ".dure-model.json").write_text(
            json.dumps(
                {
                    "schema": "dure-model-cache-v1",
                    "repository": "Qwen/Test-AWQ",
                    "revision": "a" * 40,
                    "manifest_digest": "sha256:" + "b" * 64,
                    "quantization": "awq",
                }
            ),
            encoding="utf-8",
        )
        self.model_roots = patch(
            "dure.benchmark_runtime.DEFAULT_MODEL_ROOTS", (Path(self.temporary.name),)
        )
        self.model_roots.start()
        self.cached_model = InstalledModelProfile(
            source="dure",
            model_id="Qwen/Test-AWQ",
            path=str(self.model_path),
            revision="a" * 40,
            quantization="awq",
            size_mib=8192,
            complete=True,
            manifest_digest="sha256:" + "b" * 64,
            cache_kind=MODEL_CACHE_KIND_FULL_SNAPSHOT,
            verification_version=MODEL_CACHE_VERIFICATION_VERSION,
        )
        self.profile = profile(NODE_ID)

    def tearDown(self):
        self.model_roots.stop()
        self.temporary.cleanup()

    def test_default_execute_is_a_runner_free_dry_run(self):
        runner = FakeRunner(
            response_factory=lambda command: (_ for _ in ()).throw(
                AssertionError(f"unexpected host command: {command}")
            )
        )

        result = SafeBenchmarkRuntime(runner).execute(
            payload(), self.profile, self.cached_model
        )

        self.assertEqual(
            result,
            {
                "benchmark_id": "22222222-2222-4222-8222-222222222222",
                "workload_id": "short-chat-1k-128",
                "metrics": {},
            },
        )
        self.assertEqual(runner.calls, [])

    def test_callable_runs_only_the_fixed_labeled_container(self):
        expected_metrics = metrics()

        def respond(command):
            if command[:3] == ("docker", "image", "inspect"):
                return 0, "sha256:image", ""
            if command[:3] == ("docker", "container", "inspect"):
                if any(call[:3] == ("docker", "start", "--attach") for call in runner.calls):
                    return 0, container_identity(state="exited"), ""
                if any(call[:2] == ("docker", "create") for call in runner.calls):
                    return 0, container_identity(state="created"), ""
                return 1, "", "not found"
            if command == BENCHMARK_CONTAINER_LIST:
                return 0, "", ""
            if command[:2] == ("docker", "create"):
                return 0, "a" * 64, ""
            if command[:3] == ("docker", "start", "--attach"):
                return 0, json.dumps(expected_metrics), ""
            if command == ("docker", "rm", "a" * 64):
                return 0, "a" * 64, ""
            raise AssertionError(f"unexpected command: {command}")

        runner = FakeRunner(response_factory=respond)
        source_entrypoint = (
            Path(__file__).resolve().parents[1] / "packaging" / "dure-benchmark"
        )
        result = SafeBenchmarkRuntime(
            runner, entrypoint_path=source_entrypoint
        )(payload(), self.profile, self.cached_model)

        self.assertEqual(set(result), {"benchmark_id", "workload_id", "metrics"})
        self.assertEqual(
            result["metrics"],
            {
                **expected_metrics,
                "network_bandwidth_mbps": None,
                "network_rtt_ms": None,
                "packet_loss_pct": None,
                "nccl_all_reduce_ok": None,
            },
        )
        self.assertEqual(len(runner.calls), 10)
        self.assertEqual(runner.calls[3], BENCHMARK_CONTAINER_LIST)
        self.assertEqual(runner.calls[4], NVIDIA_COMPUTE_QUERY_COMMAND)
        command = next(call for call in runner.calls if call[:2] == ("docker", "create"))
        self.assertEqual(command[:2], ("docker", "create"))
        self.assertNotIn("--rm", command)
        pull = command.index("--pull")
        self.assertEqual(command[pull + 1], "never")
        logging = command.index("--log-driver")
        self.assertEqual(command[logging + 1], "none")
        self.assertNotIn("-e", command)
        self.assertNotIn("--privileged", command)
        memory = command.index("--memory")
        self.assertEqual(command[memory + 1], "20000m")
        memory_swap = command.index("--memory-swap")
        self.assertEqual(command[memory_swap + 1], "20000m")
        cpus = command.index("--cpus")
        self.assertEqual(command[cpus + 1], "8")
        restart = command.index("--restart")
        self.assertEqual(command[restart + 1], "no")
        tmpfs = command.index("--tmpfs")
        self.assertEqual(
            command[tmpfs + 1],
            "/tmp:rw,exec,nosuid,nodev,size=1g",
        )
        self.assertNotIn("noexec", command[tmpfs + 1].split(","))
        gpus = command.index("--gpus")
        self.assertEqual(command[gpus + 1], f"device=GPU-{NODE_ID}")
        self.assertNotIn("--user", command)
        self.assertNotIn("all", command)
        self.assertNotIn("dure.deployment", " ".join(command))
        labels = {
            command[index + 1]
            for index, part in enumerate(command[:-1])
            if part == "--label"
        }
        self.assertEqual(
            labels,
            {
                "dure.managed=true",
                "dure.kind=benchmark",
                "dure.benchmark=22222222-2222-4222-8222-222222222222",
                "dure.release=33333333-3333-4333-8333-333333333333",
                "dure.placement=44444444-4444-4444-8444-444444444444",
                "dure.workload=short-chat-1k-128",
            },
        )
        entrypoint = command.index("--entrypoint")
        self.assertEqual(command[entrypoint + 1], "dure-benchmark")
        mount = command.index("--mount")
        self.assertEqual(
            command[mount + 1],
            f"type=bind,src={self.model_path},dst=/models/model,readonly",
        )
        entrypoint_mount = command.index("--mount", mount + 1)
        self.assertEqual(
            command[entrypoint_mount + 1],
            "type=bind,src="
            f"{source_entrypoint},"
            f"dst={BENCHMARK_ENTRYPOINT_CONTAINER_PATH},readonly",
        )
        serialized = json.dumps(result, sort_keys=True)
        self.assertNotIn(str(self.model_path), serialized)
        self.assertNotIn("stdout", serialized)
        self.assertNotIn("stderr", serialized)
        self.assertEqual(
            runner.limited_output_calls,
            [(("docker", "start", "--attach", "a" * 64), MAX_BENCHMARK_OUTPUT_BYTES)],
        )
        self.assertEqual(
            runner.limited_output_timeouts,
            [300.0],
        )
        self.assertEqual(
            BENCHMARK_WORKLOADS["short-chat-1k-128"].duration_seconds
            + BENCHMARK_CONTAINER_GRACE_SECONDS,
            300.0,
        )

    def test_unsafe_packaged_entrypoint_is_rejected_before_docker(self):
        path = Path(self.temporary.name) / "unsafe-benchmark"
        path.write_text("#!/bin/sh\n", encoding="utf-8")
        path.chmod(0o777)
        runner = FakeRunner()

        with self.assertRaisesRegex(BenchmarkRuntimeError, "entrypoint is unsafe"):
            SafeBenchmarkRuntime(runner, entrypoint_path=path).reconcile(payload())

        self.assertEqual(runner.calls, [])

    def test_workload_dimensions_and_local_context_are_revalidated(self):
        runtime = SafeBenchmarkRuntime(FakeRunner())

        with self.assertRaisesRegex(ValueError, "dimensions"):
            runtime.execute(
                replace(payload(), input_tokens=2048),
                self.profile,
                self.cached_model,
            )

        max_context = payload(
            workload_id="max-context",
            input_tokens=7936,
            output_tokens=256,
            concurrency=1,
        )
        planned = runtime.execute(max_context, self.profile, self.cached_model)
        self.assertEqual(planned["workload_id"], "max-context")

        with self.assertRaisesRegex(ValueError, "local model context"):
            runtime.execute(
                replace(max_context, input_tokens=7935),
                self.profile,
                self.cached_model,
            )

        (self.model_path / "config.json").write_text(
            json.dumps({"max_position_embeddings": 1024}), encoding="utf-8"
        )
        with self.assertRaisesRegex(ValueError, "exceeds"):
            runtime.execute(payload(), self.profile, self.cached_model)

    def test_host_resources_must_allow_a_bounded_container(self):
        runner = FakeRunner()
        self.profile.memory_available_mib = 15_000

        with self.assertRaisesRegex(
            BenchmarkRuntimeError, "insufficient safely bounded memory"
        ) as raised:
            SafeBenchmarkRuntime(runner).execute(
                payload(), self.profile, self.cached_model
            )

        self.assertEqual(raised.exception.code, "BENCHMARK_RUNTIME_UNAVAILABLE")
        self.assertEqual(runner.calls, [])

    def test_missing_local_context_limit_is_an_artifact_failure(self):
        (self.model_path / "config.json").write_text("{}", encoding="utf-8")

        with self.assertRaisesRegex(
            BenchmarkRuntimeError, "context limit"
        ) as raised:
            SafeBenchmarkRuntime(FakeRunner()).execute(
                payload(), self.profile, self.cached_model
            )

        self.assertEqual(
            raised.exception.failure_code, "BENCHMARK_ARTIFACT_UNAVAILABLE"
        )

    def test_apply_requires_strict_true_and_single_node(self):
        runner = FakeRunner()
        runtime = SafeBenchmarkRuntime(runner)

        with self.assertRaisesRegex(ValueError, "strict boolean"):
            runtime.execute(
                payload(), self.profile, self.cached_model, apply="true"  # type: ignore[arg-type]
            )
        with self.assertRaisesRegex(ValueError, "explicit apply"):
            runtime.execute(
                payload(apply=False), self.profile, self.cached_model, apply=True
            )
        other = "55555555-5555-4555-8555-555555555555"
        with self.assertRaisesRegex(ValueError, "multi-node"):
            runtime.execute(
                payload(node_ids=[NODE_ID, other]),
                self.profile,
                self.cached_model,
                apply=True,
            )
        self.assertEqual(runner.calls, [])

    def test_revalidates_pinned_identities_and_local_cache(self):
        runtime = SafeBenchmarkRuntime(FakeRunner())
        invalid_payloads = (
            replace(payload(), runtime_image="registry.example/vllm:latest"),
            replace(payload(), artifact_revision="main"),
            replace(payload(), artifact_manifest_digest="latest"),
        )
        for invalid in invalid_payloads:
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                runtime.execute(invalid, self.profile, self.cached_model)

        for invalid_cache in (
            replace(self.cached_model, revision="f" * 40),
            replace(self.cached_model, quantization="gptq"),
            replace(self.cached_model, complete=False),
            replace(self.cached_model, source="huggingface-cache"),
            replace(self.cached_model, source="ollama"),
            replace(
                self.cached_model, manifest_digest="sha256:" + "f" * 64
            ),
            replace(self.cached_model, cache_kind=MODEL_CACHE_KIND_STAGE),
            replace(self.cached_model, verification_version=2),
        ):
            with self.subTest(cache=invalid_cache), self.assertRaisesRegex(
                BenchmarkRuntimeError, "artifact"
            ) as raised:
                runtime.execute(payload(), self.profile, invalid_cache)
        self.assertEqual(raised.exception.code, "BENCHMARK_ARTIFACT_UNAVAILABLE")

    def test_v2_full_snapshot_marker_is_accepted(self):
        (self.model_path / ".dure-model.json").write_text(
            json.dumps(
                build_model_cache_marker(
                    repository="Qwen/Test-AWQ",
                    revision="a" * 40,
                    manifest_digest="sha256:" + "b" * 64,
                    quantization="awq",
                )
            ),
            encoding="utf-8",
        )

        result = SafeBenchmarkRuntime(FakeRunner()).execute(
            payload(), self.profile, self.cached_model
        )

        self.assertEqual(result["metrics"], {})

    def test_stage_marker_is_rejected_even_if_profile_claims_full_snapshot(self):
        (self.model_path / ".dure-model.json").write_text(
            json.dumps(
                build_model_cache_marker(
                    repository="Qwen/Test-AWQ",
                    revision="a" * 40,
                    manifest_digest="sha256:" + "b" * 64,
                    quantization="awq",
                    cache_kind=MODEL_CACHE_KIND_STAGE,
                )
            ),
            encoding="utf-8",
        )

        with self.assertRaisesRegex(
            BenchmarkRuntimeError, "metadata does not match"
        ) as raised:
            SafeBenchmarkRuntime(FakeRunner()).execute(
                payload(), self.profile, self.cached_model
            )

        self.assertEqual(raised.exception.code, "BENCHMARK_ARTIFACT_UNAVAILABLE")

    def test_materialized_cache_metadata_must_match_the_prepared_artifact(self):
        metadata_path = self.model_path / ".dure-model.json"
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        metadata["manifest_digest"] = "sha256:" + "f" * 64
        metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

        with self.assertRaisesRegex(
            BenchmarkRuntimeError, "metadata does not match"
        ) as raised:
            SafeBenchmarkRuntime(FakeRunner()).execute(
                payload(), self.profile, self.cached_model
            )

        self.assertEqual(raised.exception.code, "BENCHMARK_ARTIFACT_UNAVAILABLE")

    def test_running_workload_is_refused_before_container_execution(self):
        self.profile.workloads = [
            WorkloadProfile(
                name="production",
                runtime="vllm",
                image="registry.example/production@sha256:" + "f" * 64,
                status="Up 2 hours",
            )
        ]
        def respond(command):
            if command[:3] == ("docker", "image", "inspect"):
                return 0, "sha256:image", ""
            if command[:3] == ("docker", "container", "inspect"):
                return 1, "", "not found"
            if command == BENCHMARK_CONTAINER_LIST:
                return 0, "", ""
            raise AssertionError(f"unexpected command: {command}")

        runner = FakeRunner(response_factory=respond)

        with self.assertRaisesRegex(BenchmarkRuntimeError, "another workload"):
            SafeBenchmarkRuntime(runner)(payload(), self.profile, self.cached_model)

        self.assertEqual(runner.calls[-1], BENCHMARK_CONTAINER_LIST)
        self.assertFalse(any(command[:2] == ("docker", "run") for command in runner.calls))

    def test_missing_image_never_pulls_or_creates_a_container(self):
        runner = FakeRunner(
            response_factory=lambda command: CommandResult(
                command, 1, stderr="image missing"
            )
        )

        with self.assertRaisesRegex(BenchmarkRuntimeError, "not available locally") as raised:
            SafeBenchmarkRuntime(runner)(payload(), self.profile, self.cached_model)

        self.assertEqual(raised.exception.code, "BENCHMARK_RUNTIME_UNAVAILABLE")
        self.assertEqual(
            raised.exception.failure_code, "BENCHMARK_RUNTIME_UNAVAILABLE"
        )
        self.assertEqual(runner.calls[-1][:3], ("docker", "image", "inspect"))
        self.assertFalse(any("pull" in command for command in runner.calls))

    def test_name_collision_fails_closed_without_stop_remove_or_run(self):
        def respond(command):
            if command[:3] == ("docker", "image", "inspect"):
                return 0, "sha256:image", ""
            if command[:3] == ("docker", "container", "inspect"):
                return 0, "existing-container", ""
            raise AssertionError(f"unexpected command: {command}")

        runner = FakeRunner(response_factory=respond)
        with self.assertRaisesRegex(BenchmarkRuntimeError, "name collision"):
            SafeBenchmarkRuntime(runner)(payload(), self.profile, self.cached_model)

        self.assertEqual(len(runner.calls), 1)
        self.assertEqual(runner.calls[0][:3], ("docker", "container", "inspect"))
        self.assertFalse(
            any(command[:2] in {("docker", "stop"), ("docker", "rm"), ("docker", "run")} for command in runner.calls)
        )

    def test_only_exact_active_container_states_are_deferred(self):
        for state in ("running", "restarting", "paused"):
            with self.subTest(state=state):
                def respond(command, state=state):
                    if command[:3] == ("docker", "image", "inspect"):
                        return 0, "sha256:image", ""
                    if command[:3] == ("docker", "container", "inspect"):
                        return 0, container_identity(state=state), ""
                    raise AssertionError(f"unexpected command: {command}")

                runner = FakeRunner(response_factory=respond)
                with self.assertRaises(BenchmarkRuntimeDeferred):
                    SafeBenchmarkRuntime(runner)(
                        payload(), self.profile, self.cached_model
                    )

                self.assertFalse(
                    any(
                        command[:2] in {
                            ("docker", "stop"),
                            ("docker", "rm"),
                            ("docker", "run"),
                        }
                        for command in runner.calls
                    )
                )

    def test_expired_exact_active_container_is_reconciled_before_retry(self):
        inspect_count = 0
        container_id = "a" * 64
        self.profile.workloads = [
            WorkloadProfile(
                name="dure-benchmark-22222222-2222-4222-8222-222222222222",
                runtime="vllm",
                image="registry.example/vllm@sha256:" + "c" * 64,
                status="Up 25 minutes",
            )
        ]

        def respond(command):
            nonlocal inspect_count
            if command[:3] == ("docker", "image", "inspect"):
                return 0, "sha256:image", ""
            if command[:3] == ("docker", "container", "inspect"):
                inspect_count += 1
                states = ("running", "running", "exited", "created", "exited")
                return (
                    0,
                    container_identity(
                        state=states[inspect_count - 1],
                        started_at=(
                            "2000-01-01T00:00:00Z"
                            if inspect_count <= 3
                            else None
                        ),
                    ),
                    "",
                )
            if command[:4] == ("docker", "stop", "--timeout", "30"):
                return 0, container_id, ""
            if command == ("docker", "rm", container_id):
                return 0, container_id, ""
            if command == BENCHMARK_CONTAINER_LIST:
                return 0, "", ""
            if command[:2] == ("docker", "create"):
                return 0, container_id, ""
            if command[:3] == ("docker", "start", "--attach"):
                return 0, json.dumps(metrics()), ""
            raise AssertionError(f"unexpected command: {command}")

        runner = FakeRunner(response_factory=respond)
        result = SafeBenchmarkRuntime(runner)(
            payload(), self.profile, self.cached_model
        )

        self.assertEqual(result["metrics"]["request_count"], 20)
        self.assertIn(
            ("docker", "stop", "--timeout", "30", container_id), runner.calls
        )
        self.assertIn(("docker", "rm", container_id), runner.calls)
        self.assertIn(("docker", "start", "--attach", container_id), runner.calls)

    def test_expired_container_is_cleaned_before_dynamic_memory_rejection(self):
        inspect_count = 0
        container_id = "a" * 64
        self.profile.memory_available_mib = 15_000

        def respond(command):
            nonlocal inspect_count
            if command[:3] == ("docker", "container", "inspect"):
                inspect_count += 1
                return (
                    0,
                    container_identity(
                        state="running" if inspect_count < 3 else "exited",
                        started_at="2000-01-01T00:00:00Z",
                    ),
                    "",
                )
            if command[:4] == ("docker", "stop", "--timeout", "30"):
                return 0, container_id, ""
            if command == ("docker", "rm", container_id):
                return 0, container_id, ""
            raise AssertionError(f"unexpected command: {command}")

        runner = FakeRunner(response_factory=respond)
        with self.assertRaisesRegex(
            BenchmarkRuntimeError, "insufficient safely bounded memory"
        ):
            SafeBenchmarkRuntime(runner)(payload(), self.profile, self.cached_model)

        self.assertIn(
            ("docker", "stop", "--timeout", "30", container_id), runner.calls
        )
        self.assertIn(("docker", "rm", container_id), runner.calls)
        self.assertFalse(
            any(command[:3] == ("docker", "image", "inspect") for command in runner.calls)
        )

    def test_invalid_container_start_time_refuses_mutation(self):
        def respond(command):
            if command[:3] == ("docker", "image", "inspect"):
                return 0, "sha256:image", ""
            if command[:3] == ("docker", "container", "inspect"):
                return 0, container_identity(
                    state="running", started_at="not-a-timestamp"
                ), ""
            raise AssertionError(f"unexpected command: {command}")

        runner = FakeRunner(response_factory=respond)
        with self.assertRaisesRegex(BenchmarkRuntimeError, "name collision"):
            SafeBenchmarkRuntime(runner)(payload(), self.profile, self.cached_model)

        self.assertFalse(
            any(
                command[:2] in {("docker", "stop"), ("docker", "rm")}
                for command in runner.calls
            )
        )

    def test_preflight_inspect_uncertainty_defers_before_image_check(self):
        name = "dure-benchmark-22222222-2222-4222-8222-222222222222"
        absence_check = (
            "docker",
            "container",
            "ls",
            "--all",
            "--no-trunc",
            "--filter",
            f"name={name}",
            "--format",
            "{{.ID}}\t{{.Names}}",
        )

        def respond(command):
            if command[:3] == ("docker", "container", "inspect"):
                return 1, "", "temporary daemon error"
            raise AssertionError(f"unexpected command: {command}")

        runner = FakeRunner(
            responses={absence_check: (1, "", "temporary daemon error")},
            response_factory=respond,
        )
        with self.assertRaises(BenchmarkRuntimeDeferred):
            SafeBenchmarkRuntime(runner)(payload(), self.profile, self.cached_model)

        self.assertEqual(runner.calls[-1], absence_check)
        self.assertFalse(
            any(command[:3] == ("docker", "image", "inspect") for command in runner.calls)
        )

    def test_stopped_exact_container_remove_failure_is_deferred(self):
        def respond(command):
            if command[:3] == ("docker", "container", "inspect"):
                return 0, container_identity(state="exited"), ""
            if command == ("docker", "rm", "a" * 64):
                return 1, "", "temporary daemon error"
            raise AssertionError(f"unexpected command: {command}")

        runner = FakeRunner(response_factory=respond)
        with self.assertRaises(BenchmarkRuntimeDeferred):
            SafeBenchmarkRuntime(runner)(payload(), self.profile, self.cached_model)

        self.assertFalse(
            any(command[:3] == ("docker", "image", "inspect") for command in runner.calls)
        )

    def test_other_exact_labeled_benchmark_container_blocks_execution(self):
        other_container_id = "f" * 64

        def respond(command):
            if command[:3] == ("docker", "image", "inspect"):
                return 0, "sha256:image", ""
            if command[:3] == ("docker", "container", "inspect"):
                return 1, "", "not found"
            if command == BENCHMARK_CONTAINER_LIST:
                return 0, other_container_id, ""
            raise AssertionError(f"unexpected command: {command}")

        runner = FakeRunner(response_factory=respond)
        with self.assertRaisesRegex(
            BenchmarkRuntimeError, "another Dure benchmark"
        ) as raised:
            SafeBenchmarkRuntime(runner)(payload(), self.profile, self.cached_model)

        self.assertEqual(raised.exception.code, "BENCHMARK_RUNTIME_UNAVAILABLE")
        self.assertEqual(runner.calls[-1], BENCHMARK_CONTAINER_LIST)
        self.assertFalse(
            any(
                command[:2] in {("docker", "rm"), ("docker", "run")}
                for command in runner.calls
            )
        )

    def test_exact_unknown_container_state_fails_without_mutation(self):
        def respond(command):
            if command[:3] == ("docker", "image", "inspect"):
                return 0, "sha256:image", ""
            if command[:3] == ("docker", "container", "inspect"):
                return 0, container_identity(state="removing"), ""
            raise AssertionError(f"unexpected command: {command}")

        runner = FakeRunner(response_factory=respond)
        with self.assertRaisesRegex(BenchmarkRuntimeError, "safely removable"):
            SafeBenchmarkRuntime(runner)(payload(), self.profile, self.cached_model)

        self.assertFalse(
            any(
                command[:2] in {
                    ("docker", "stop"),
                    ("docker", "rm"),
                    ("docker", "run"),
                }
                for command in runner.calls
            )
        )

    def test_retry_removes_only_the_exact_stopped_benchmark_container(self):
        expected_metrics = metrics()
        container_id = "a" * 64
        inspect_count = 0

        def respond(command):
            nonlocal inspect_count
            if command[:3] == ("docker", "image", "inspect"):
                return 0, "sha256:image", ""
            if command[:3] == ("docker", "container", "inspect"):
                inspect_count += 1
                return 0, container_identity(
                    state=("exited", "created", "exited")[inspect_count - 1]
                ), ""
            if command == ("docker", "rm", container_id):
                return 0, container_id, ""
            if command == BENCHMARK_CONTAINER_LIST:
                return 0, "", ""
            if command[:2] == ("docker", "create"):
                return 0, container_id, ""
            if command[:3] == ("docker", "start", "--attach"):
                return 0, json.dumps(expected_metrics), ""
            raise AssertionError(f"unexpected command: {command}")

        runner = FakeRunner(response_factory=respond)
        result = SafeBenchmarkRuntime(runner)(payload(), self.profile, self.cached_model)

        self.assertEqual(result["benchmark_id"], payload().benchmark_id)
        self.assertEqual(runner.calls[1], ("docker", "rm", container_id))
        self.assertEqual(runner.calls[3], BENCHMARK_CONTAINER_LIST)
        self.assertEqual(runner.calls[4], NVIDIA_COMPUTE_QUERY_COMMAND)
        self.assertIn(("docker", "start", "--attach", container_id), runner.calls)
        self.assertFalse(any(command[:2] == ("docker", "stop") for command in runner.calls))

    def test_retry_removes_exact_orphan_created_container_then_reruns(self):
        expected_metrics = metrics()
        container_id = "a" * 64
        inspect_count = 0

        def respond(command):
            nonlocal inspect_count
            if command[:3] == ("docker", "image", "inspect"):
                return 0, "sha256:image", ""
            if command[:3] == ("docker", "container", "inspect"):
                inspect_count += 1
                return 0, container_identity(
                    state=("created", "created", "exited")[inspect_count - 1]
                ), ""
            if command == ("docker", "rm", container_id):
                return 0, container_id, ""
            if command == BENCHMARK_CONTAINER_LIST:
                return 0, "", ""
            if command[:2] == ("docker", "create"):
                return 0, container_id, ""
            if command[:3] == ("docker", "start", "--attach"):
                return 0, json.dumps(expected_metrics), ""
            raise AssertionError(f"unexpected command: {command}")

        runner = FakeRunner(response_factory=respond)
        result = SafeBenchmarkRuntime(runner)(
            payload(), self.profile, self.cached_model
        )

        self.assertEqual(result["metrics"]["request_count"], 20)
        self.assertIn(("docker", "rm", container_id), runner.calls)
        self.assertIn(("docker", "start", "--attach", container_id), runner.calls)
        self.assertFalse(
            any(command[:2] == ("docker", "stop") for command in runner.calls)
        )

    def test_largest_healthy_gpu_is_selected_by_uuid(self):
        self.profile.gpus.extend(
            (
                GPUProfile(
                    index=2,
                    name="larger-2",
                    uuid="GPU-22222222-2222-4222-8222-222222222222",
                    driver_version="610.43.02",
                    memory_mib=49152,
                ),
                GPUProfile(
                    index=1,
                    name="larger-1",
                    uuid="GPU-33333333-3333-4333-8333-333333333333",
                    driver_version="610.43.02",
                    memory_mib=49152,
                ),
            )
        )

        def respond(command):
            if command[:3] == ("docker", "image", "inspect"):
                return 0, "sha256:image", ""
            if command[:3] == ("docker", "container", "inspect"):
                if any(call[:3] == ("docker", "start", "--attach") for call in runner.calls):
                    return 0, container_identity(state="exited"), ""
                if any(call[:2] == ("docker", "create") for call in runner.calls):
                    return 0, container_identity(state="created"), ""
                return 1, "", "not found"
            if command == BENCHMARK_CONTAINER_LIST:
                return 0, "", ""
            if command[:2] == ("docker", "create"):
                return 0, "a" * 64, ""
            if command[:3] == ("docker", "start", "--attach"):
                return 0, json.dumps(metrics()), ""
            if command == ("docker", "rm", "a" * 64):
                return 0, "a" * 64, ""
            raise AssertionError(f"unexpected command: {command}")

        runner = FakeRunner(response_factory=respond)
        SafeBenchmarkRuntime(runner)(payload(), self.profile, self.cached_model)

        command = next(call for call in runner.calls if call[:2] == ("docker", "create"))
        gpus = command.index("--gpus")
        self.assertEqual(
            command[gpus + 1],
            "device=GPU-33333333-3333-4333-8333-333333333333",
        )
        self.assertNotIn("--gpus=all", command)

    def test_selected_gpu_compute_process_blocks_container_execution(self):
        for observed in (
            f"GPU-{NODE_ID}",
            "MIG-55555555-5555-4555-8555-555555555555",
        ):
            with self.subTest(observed=observed):
                runner = FakeRunner(
                    responses={
                        NVIDIA_COMPUTE_QUERY_COMMAND: (0, observed, "")
                    },
                    response_factory=lambda command: (
                        (0, "sha256:image", "")
                        if command[:3] == ("docker", "image", "inspect")
                        else (1, "", "not found")
                        if command[:3] == ("docker", "container", "inspect")
                        else (0, "", "")
                        if command == BENCHMARK_CONTAINER_LIST
                        else (_ for _ in ()).throw(
                            AssertionError(f"unexpected command: {command}")
                        )
                    ),
                )

                with self.assertRaisesRegex(
                    BenchmarkRuntimeError, "selected GPU is active"
                ):
                    SafeBenchmarkRuntime(runner)(
                        payload(), self.profile, self.cached_model
                    )

                self.assertEqual(runner.calls[-1], NVIDIA_COMPUTE_QUERY_COMMAND)
                self.assertFalse(
                    any(
                        command[:2] == ("docker", "run")
                        for command in runner.calls
                    )
                )

    def test_subprocess_runner_enforces_combined_output_limit_during_execution(self):
        result = SubprocessRunner().run_limited_output(
            [
                sys.executable,
                "-c",
                "import os; os.write(1, b'q' * 131072); os.write(2, b'z' * 131072)",
            ],
            timeout=5,
            max_output_bytes=1024,
        )

        self.assertEqual(result.returncode, 125)
        self.assertEqual(result.stdout, "")
        self.assertEqual(result.stderr, "command output limit exceeded")
        self.assertNotIn("qqqq", result.stderr)
        self.assertNotIn("zzzz", result.stderr)

    def test_failed_run_stops_and_removes_only_its_exact_container(self):
        inspect_count = 0

        def respond(command):
            nonlocal inspect_count
            if command[:3] == ("docker", "image", "inspect"):
                return 0, "sha256:image", ""
            if command[:3] == ("docker", "container", "inspect"):
                inspect_count += 1
                if inspect_count == 1:
                    return 1, "", "not found"
                return 0, container_identity(
                    state=("created" if inspect_count == 2 else "running" if inspect_count == 3 else "exited")
                ), ""
            if command == BENCHMARK_CONTAINER_LIST:
                return 0, "", ""
            if command[:2] == ("docker", "create"):
                return 0, "a" * 64, ""
            if command[:3] == ("docker", "start", "--attach"):
                return 124, "", "command timed out"
            if command[:4] == ("docker", "stop", "--timeout", "30"):
                return 0, "a" * 64, ""
            if command == ("docker", "rm", "a" * 64):
                return 0, "a" * 64, ""
            raise AssertionError(f"unexpected command: {command}")

        runner = FakeRunner(response_factory=respond)
        with self.assertRaisesRegex(BenchmarkRuntimeError, "execution failed"):
            SafeBenchmarkRuntime(runner)(payload(), self.profile, self.cached_model)

        self.assertIn(
            ("docker", "stop", "--timeout", "30", "a" * 64), runner.calls
        )
        self.assertIn(("docker", "rm", "a" * 64), runner.calls)
        self.assertFalse(any("--force" in command for command in runner.calls))

    def test_unconfirmed_failed_container_absence_defers_terminal_failure(self):
        inspect_count = 0
        name = "dure-benchmark-22222222-2222-4222-8222-222222222222"
        absence_check = (
            "docker",
            "container",
            "ls",
            "--all",
            "--no-trunc",
            "--filter",
            f"name={name}",
            "--format",
            "{{.ID}}\t{{.Names}}",
        )

        def respond(command):
            nonlocal inspect_count
            if command[:3] == ("docker", "container", "inspect"):
                inspect_count += 1
                if inspect_count == 1:
                    return 0, container_identity(state="exited"), ""
                if inspect_count == 2:
                    return 0, container_identity(state="created"), ""
                return 1, "", "temporary daemon error"
            if command == ("docker", "rm", "a" * 64):
                return 0, "a" * 64, ""
            if command[:3] == ("docker", "image", "inspect"):
                return 0, "sha256:image", ""
            if command == BENCHMARK_CONTAINER_LIST:
                return 0, "", ""
            if command[:2] == ("docker", "create"):
                return 0, "a" * 64, ""
            if command[:3] == ("docker", "start", "--attach"):
                return 124, "", "command timed out"
            raise AssertionError(f"unexpected command: {command}")

        runner = FakeRunner(
            responses={absence_check: (1, "", "temporary daemon error")},
            response_factory=respond,
        )
        with self.assertRaises(BenchmarkRuntimeDeferred):
            SafeBenchmarkRuntime(runner)(payload(), self.profile, self.cached_model)

        self.assertEqual(inspect_count, 3)
        self.assertIn(absence_check, runner.calls)
        self.assertFalse(
            any(command[:2] == ("docker", "stop") for command in runner.calls)
        )
        self.assertEqual(
            sum(command[:2] == ("docker", "rm") for command in runner.calls), 1
        )

    def test_failed_run_refuses_cleanup_when_labels_do_not_match(self):
        inspect_count = 0

        def respond(command):
            nonlocal inspect_count
            if command[:3] == ("docker", "image", "inspect"):
                return 0, "sha256:image", ""
            if command[:3] == ("docker", "container", "inspect"):
                inspect_count += 1
                if inspect_count == 1:
                    return 1, "", "not found"
                if inspect_count == 2:
                    return 0, container_identity(state="created"), ""
                return 0, container_identity(state="running", deployment="production"), ""
            if command == BENCHMARK_CONTAINER_LIST:
                return 0, "", ""
            if command[:2] == ("docker", "create"):
                return 0, "a" * 64, ""
            if command[:3] == ("docker", "start", "--attach"):
                return 124, "", "command timed out"
            raise AssertionError(f"unexpected command: {command}")

        runner = FakeRunner(response_factory=respond)
        with self.assertRaisesRegex(BenchmarkRuntimeError, "identity mismatch"):
            SafeBenchmarkRuntime(runner)(payload(), self.profile, self.cached_model)

        self.assertFalse(
            any(
                command[:2] in {("docker", "stop"), ("docker", "rm")}
                for command in runner.calls
            )
        )

    def test_summary_schema_and_numeric_values_are_strict_and_sanitized(self):
        invalid_summaries = (
            dict(metrics(), prompt="secret prompt"),
            metrics(success_rate=math.nan),
            metrics(oom_count=True),
            metrics(oom_count=2**63),
            metrics(request_count=19),
        )
        for summary in invalid_summaries:
            with self.subTest(summary=summary):
                def respond(command, summary=summary):
                    if command[:3] == ("docker", "image", "inspect"):
                        return 0, "sha256:image", ""
                    if command[:3] == ("docker", "container", "inspect"):
                        if any(call[:3] == ("docker", "start", "--attach") for call in runner.calls):
                            return 0, container_identity(state="exited"), ""
                        if any(call[:2] == ("docker", "create") for call in runner.calls):
                            return 0, container_identity(state="created"), ""
                        return 1, "", "not found"
                    if command == BENCHMARK_CONTAINER_LIST:
                        return 0, "", ""
                    if command[:2] == ("docker", "create"):
                        return 0, "a" * 64, ""
                    if command[:3] == ("docker", "start", "--attach"):
                        return 0, json.dumps(summary), "raw secret log"
                    if command == ("docker", "rm", "a" * 64):
                        return 0, "a" * 64, ""
                    raise AssertionError(f"unexpected command: {command}")

                runner = FakeRunner(response_factory=respond)
                with self.assertRaises(BenchmarkRuntimeError) as raised:
                    SafeBenchmarkRuntime(runner)(
                        payload(), self.profile, self.cached_model
                    )
                self.assertNotIn("secret", str(raised.exception))
                self.assertNotIn("raw", str(raised.exception))

        duplicate = json.dumps(metrics()).replace(
            '"warmup_requests": 2',
            '"warmup_requests": 2, "warmup_requests": 2',
            1,
        )

        def duplicate_response(command):
            if command[:3] == ("docker", "image", "inspect"):
                return 0, "sha256:image", ""
            if command[:3] == ("docker", "container", "inspect"):
                if any(call[:3] == ("docker", "start", "--attach") for call in runner.calls):
                    return 0, container_identity(state="exited"), ""
                if any(call[:2] == ("docker", "create") for call in runner.calls):
                    return 0, container_identity(state="created"), ""
                return 1, "", "not found"
            if command == BENCHMARK_CONTAINER_LIST:
                return 0, "", ""
            if command[:2] == ("docker", "create"):
                return 0, "a" * 64, ""
            if command[:3] == ("docker", "start", "--attach"):
                return 0, duplicate, ""
            if command == ("docker", "rm", "a" * 64):
                return 0, "a" * 64, ""
            raise AssertionError(f"unexpected command: {command}")

        runner = FakeRunner(response_factory=duplicate_response)
        with self.assertRaisesRegex(BenchmarkRuntimeError, "valid summary"):
            SafeBenchmarkRuntime(runner)(
                payload(), self.profile, self.cached_model
            )


if __name__ == "__main__":
    unittest.main()

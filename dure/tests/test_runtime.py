import copy
import unittest
from unittest.mock import patch

from dure.command import CommandResult
from dure.pipeline_runtime import (
    RAY_COMPONENT,
    stage_cache_identity,
    stage_identity_labels,
    strict_model_mount_path,
    strict_runtime_contract_digest,
    strict_vllm_api_command,
)
from dure.planner import build_plan
from dure.runtime import ContainerRuntime, DEPLOYMENT_IDENTITY_FORMAT
from dure.stage_cache import StageCacheError

from .helpers import (
    FakeRunner,
    profile,
    strict_pipeline_fixture,
    strict_stage_pipeline_fixture,
)


class RuntimeTests(unittest.TestCase):
    @staticmethod
    def identity(container_id, state, deployment_id, generation, node_id):
        return (
            f"{container_id}\t{state}\t{deployment_id}\t{generation}\t{node_id}"
        )

    @staticmethod
    def strict_identity(
        container_id,
        state,
        deployment_id,
        generation,
        node_id,
        backend,
        pipeline_rank,
        runtime_rank,
        component,
        runtime_contract,
    ):
        return "\t".join(
            str(item)
            for item in (
                container_id,
                state,
                deployment_id,
                generation,
                node_id,
                backend,
                pipeline_rank,
                runtime_rank,
                component,
                runtime_contract,
            )
        )

    @staticmethod
    def strict_stage_identity(
        container_id,
        state,
        plan,
        assignment,
        component,
        *,
        stage_manifest=None,
    ):
        labels = stage_identity_labels(plan, assignment)
        return "\t".join(
            str(item)
            for item in (
                container_id,
                state,
                plan.deployment_id,
                plan.generation,
                assignment.node_id,
                plan.execution_backend,
                assignment.pipeline_rank,
                assignment.expected_runtime_rank,
                component,
                strict_runtime_contract_digest(plan, assignment, component),
                labels["dure.cache-kind"],
                labels["dure.stage-variant"],
                stage_manifest or labels["dure.stage-manifest"],
                labels["dure.stage-cache-identity"],
            )
        )

    def test_stop_filters_generation_and_inspects_exact_node_identity(self):
        node_id = "11111111-1111-4111-8111-111111111111"
        listed = (
            "docker",
            "ps",
            "-q",
            "--filter",
            "label=dure.deployment=deploy-1",
            "--filter",
            "label=dure.generation=2",
        )
        inspect_abc = (
            "docker",
            "inspect",
            "--format",
            DEPLOYMENT_IDENTITY_FORMAT,
            "abc",
        )
        inspect_def = (*inspect_abc[:-1], "def")
        runner = FakeRunner(
            responses={
                listed: (0, "abc\ndef", ""),
                inspect_abc: (
                    0,
                    self.identity("abc", "running", "deploy-1", 2, node_id),
                    "",
                ),
                inspect_def: (
                    0,
                    self.identity("def", "running", "deploy-1", 2, node_id),
                    "",
                ),
                ("docker", "stop", "--time", "30", "abc", "def"): (0, "abc\ndef", ""),
            }
        )
        check = ContainerRuntime(runner).stop_deployment(
            "deploy-1", generation=2, node_id=node_id
        )
        self.assertTrue(check.ok)
        self.assertNotIn(("docker", "stop", "--time", "30"), runner.calls)

    def test_unjoin_stop_accepts_strict_identity_for_exact_registered_node(self):
        plan, head, _worker = strict_pipeline_fixture()
        assignment = plan.assignment_for(head.node_id)
        listed = (
            "docker", "ps", "-q", "--filter",
            f"label=dure.deployment={plan.deployment_id}", "--filter",
            f"label=dure.generation={plan.generation}", "--filter",
            f"label=dure.node={head.node_id}",
        )
        inspected = ("docker", "inspect", "--format", DEPLOYMENT_IDENTITY_FORMAT, "abc")
        runner = FakeRunner(
            responses={
                listed: (0, "abc", ""),
                inspected: (
                    0,
                    self.strict_identity(
                        "abc", "running", plan.deployment_id, plan.generation,
                        head.node_id, plan.execution_backend,
                        assignment.pipeline_rank, assignment.expected_runtime_rank,
                        RAY_COMPONENT,
                        strict_runtime_contract_digest(plan, assignment, RAY_COMPONENT),
                    ),
                    "",
                ),
                ("docker", "stop", "--time", "30", "abc"): (0, "abc", ""),
            }
        )

        check = ContainerRuntime(runner).stop_registered_node_deployment(
            plan.deployment_id, generation=plan.generation, node_id=head.node_id
        )

        self.assertTrue(check.ok)
        self.assertIn(("docker", "stop", "--time", "30", "abc"), runner.calls)

    def test_unjoin_stop_refuses_container_from_another_node(self):
        listed = (
            "docker", "ps", "-q", "--filter", "label=dure.deployment=deploy-1",
            "--filter", "label=dure.generation=2", "--filter", "label=dure.node=node-1",
        )
        inspected = ("docker", "inspect", "--format", DEPLOYMENT_IDENTITY_FORMAT, "abc")
        runner = FakeRunner(
            responses={
                listed: (0, "abc", ""),
                inspected: (0, self.identity("abc", "running", "deploy-1", 2, "node-2"), ""),
            }
        )

        check = ContainerRuntime(runner).stop_registered_node_deployment(
            "deploy-1", generation=2, node_id="node-1"
        )

        self.assertFalse(check.ok)
        self.assertFalse(any(call[:2] == ("docker", "stop") for call in runner.calls))

    def test_stop_refuses_any_label_mismatch_before_stopping(self):
        node_id = "11111111-1111-4111-8111-111111111111"
        listed = (
            "docker",
            "ps",
            "-q",
            "--filter",
            "label=dure.deployment=deploy-1",
            "--filter",
            "label=dure.generation=2",
        )
        inspected = (
            "docker",
            "inspect",
            "--format",
            DEPLOYMENT_IDENTITY_FORMAT,
            "abc",
        )
        runner = FakeRunner(
            responses={
                listed: (0, "abc", ""),
                inspected: (
                    0,
                    self.identity("abc", "running", "deploy-1", 2, "other-node"),
                    "",
                ),
            }
        )

        check = ContainerRuntime(runner).stop_deployment(
            "deploy-1", generation=2, node_id=node_id
        )

        self.assertFalse(check.ok)
        self.assertFalse(any(call[:2] == ("docker", "stop") for call in runner.calls))

    def test_legacy_container_without_node_label_remains_manageable(self):
        node_id = "11111111-1111-4111-8111-111111111111"
        listed = (
            "docker",
            "ps",
            "-q",
            "--filter",
            "label=dure.deployment=deploy-1",
            "--filter",
            "label=dure.generation=2",
        )
        inspected = (
            "docker",
            "inspect",
            "--format",
            DEPLOYMENT_IDENTITY_FORMAT,
            "abc",
        )
        runner = FakeRunner(
            responses={
                listed: (0, "abc", ""),
                inspected: (
                    0,
                    self.identity("abc", "running", "deploy-1", 2, "<no value>"),
                    "",
                ),
                ("docker", "stop", "--time", "30", "abc"): (0, "abc", ""),
            }
        )
        runtime = ContainerRuntime(runner)

        stopped = runtime.stop_deployment(
            "deploy-1", generation=2, node_id=node_id
        )
        verified = runtime.verify_container_identity(
            "abc",
            deployment_id="deploy-1",
            generation=2,
            node_id=node_id,
            check_name="legacy-identity",
        )

        self.assertTrue(stopped.ok)
        self.assertTrue(verified.ok)

    def test_ray_container_uses_explicit_entrypoint_and_no_shell(self):
        node = profile("camp-7", address="192.168.0.228")
        plan = build_plan(
            [node],
            model_id="qwen2.5-32b-awq",
            image="registry.example/vllm@sha256:abc",
        )
        assert plan is not None
        inspect = (
            "docker",
            "inspect",
            "--format",
            DEPLOYMENT_IDENTITY_FORMAT,
            f"dure-ray-{plan.deployment_id}",
        )
        runner = FakeRunner(
            executables={"docker"},
            responses={inspect: CommandResult(inspect, 1, stderr="not found")},
        )

        result = ContainerRuntime(runner).start_ray(
            node, plan, plan.assignments[0], replace=False
        )

        self.assertTrue(result.ok)
        run = runner.calls[-1]
        self.assertEqual(run[0:3], ("docker", "run", "-d"))
        self.assertIn("dure.node=camp-7", run)
        self.assertIn("dure.model=qwen2.5-32b-awq", run)
        entrypoint = run.index("--entrypoint")
        self.assertEqual(run[entrypoint + 1], "ray")
        image = run.index("registry.example/vllm@sha256:abc")
        self.assertEqual(run[image + 1 : image + 4], ("start", "--block", "--head"))

    def test_api_container_uses_vllm_entrypoint(self):
        node = profile("camp-7", address="192.168.0.228")
        plan = build_plan(
            [node],
            model_id="qwen2.5-32b-awq",
            image="registry.example/vllm@sha256:abc",
        )
        assert plan is not None
        name = f"dure-api-{plan.deployment_id}"
        inspect = (
            "docker",
            "inspect",
            "--format",
            DEPLOYMENT_IDENTITY_FORMAT,
            name,
        )
        runner = FakeRunner(
            responses={inspect: CommandResult(inspect, 1, stderr="not found")}
        )

        result = ContainerRuntime(runner).start_api(
            plan, plan.assignments[0], replace=False
        )

        self.assertTrue(result.ok)
        self.assertEqual(result.name, "vllm-api-start")
        run = runner.calls[-1]
        self.assertIn("dure.generation=1", run)
        self.assertIn("dure.node=camp-7", run)
        self.assertIn("dure.model=qwen2.5-32b-awq", run)
        entrypoint = run.index("--entrypoint")
        self.assertEqual(run[entrypoint + 1], "vllm")
        image = run.index("registry.example/vllm@sha256:abc")
        self.assertEqual(run[image + 1 : image + 3], ("serve", "/models/model"))

    def test_strict_worker_uses_fixed_address_environment_and_exact_labels(self):
        plan, _, worker = strict_pipeline_fixture()
        assignment = plan.assignments[1]
        worker.network.addresses.insert(0, "10.9.8.7")
        name = f"dure-ray-{plan.deployment_id}"
        inspect = (
            "docker",
            "inspect",
            "--format",
            DEPLOYMENT_IDENTITY_FORMAT,
            name,
        )
        runner = FakeRunner(
            responses={inspect: CommandResult(inspect, 1, stderr="not found")}
        )

        result = ContainerRuntime(runner).start_ray(
            worker, plan, assignment, replace=False
        )

        self.assertTrue(result.ok, result.detail)
        run = runner.calls[-1]
        for label in (
            "dure.backend=VLLM_RAY_PP_V1",
            "dure.pipeline-rank=1",
            "dure.runtime-rank=1",
            "dure.component=ray-node",
            "dure.runtime-contract="
            + strict_runtime_contract_digest(plan, assignment, RAY_COMPONENT),
        ):
            self.assertIn(label, run)
        for environment in (
            "VLLM_HOST_IP=192.168.0.11",
            "VLLM_USE_V1=0",
            "VLLM_USE_RAY_SPMD_WORKER=0",
            "VLLM_RAY_PER_WORKER_GPUS=1.0",
            "VLLM_RAY_BUNDLE_INDICES=",
            "VLLM_USE_RAY_COMPILED_DAG=0",
        ):
            self.assertIn(environment, run)
        image = run.index(plan.image)
        self.assertEqual(
            run[image + 1 :],
            (
                "start",
                "--block",
                "--address=192.168.0.10:6379",
                "--node-ip-address=192.168.0.11",
                '--resources={"dure_node_22222222222242228222222222222222":1}',
                "--min-worker-port=20000",
                "--max-worker-port=21000",
            ),
        )
        self.assertNotIn("10.9.8.7", run)
        self.assertNotIn("sh", run)

        head = strict_pipeline_fixture()[1]
        head_name = f"dure-ray-{plan.deployment_id}"
        head_inspect = (
            "docker",
            "inspect",
            "--format",
            DEPLOYMENT_IDENTITY_FORMAT,
            head_name,
        )
        head_runner = FakeRunner(
            responses={
                head_inspect: CommandResult(head_inspect, 1, stderr="not found")
            }
        )
        head_result = ContainerRuntime(head_runner).start_ray(
            head, plan, plan.assignments[0], replace=False
        )
        self.assertTrue(head_result.ok, head_result.detail)
        head_run = head_runner.calls[-1]
        head_image = head_run.index(plan.image)
        self.assertEqual(
            head_run[head_image + 1 :],
            (
                "start",
                "--block",
                "--head",
                "--node-ip-address=192.168.0.10",
                "--port=6379",
                '--resources={"dure_node_11111111111141118111111111111111":1}',
                "--min-worker-port=20000",
                "--max-worker-port=21000",
            ),
        )

    def test_strict_api_is_head_only_and_uses_fixed_v0_non_spmd_mode(self):
        plan, _, _ = strict_pipeline_fixture()
        assignment = plan.assignments[0]
        name = f"dure-api-{plan.deployment_id}"
        inspect = (
            "docker",
            "inspect",
            "--format",
            DEPLOYMENT_IDENTITY_FORMAT,
            name,
        )
        runner = FakeRunner(
            responses={inspect: CommandResult(inspect, 1, stderr="not found")}
        )

        result = ContainerRuntime(runner).start_api(
            plan, assignment, replace=False
        )

        self.assertTrue(result.ok, result.detail)
        run = runner.calls[-1]
        for expected in (
            "RAY_ADDRESS=192.168.0.10:6379",
            "VLLM_HOST_IP=192.168.0.10",
            "VLLM_USE_V1=0",
            "VLLM_USE_RAY_SPMD_WORKER=0",
            "VLLM_RAY_PER_WORKER_GPUS=1.0",
            "VLLM_RAY_BUNDLE_INDICES=",
            "VLLM_USE_RAY_COMPILED_DAG=0",
            "dure.backend=VLLM_RAY_PP_V1",
            "dure.pipeline-rank=0",
            "dure.runtime-rank=0",
            "dure.component=vllm-api",
            "dure.runtime-contract="
            + strict_runtime_contract_digest(plan, assignment, "vllm-api"),
        ):
            self.assertIn(expected, run)
        image = run.index(plan.image)
        self.assertEqual(run[image + 1], "serve")
        self.assertEqual(
            run[run.index("--served-model-name") + 1], "qwen-test-awq"
        )
        self.assertEqual(run[run.index("--host") + 1], "127.0.0.1")
        self.assertEqual(run[run.index("--port") + 1], "8000")

        worker_result = ContainerRuntime(FakeRunner()).start_api(
            plan, plan.assignments[1], replace=False
        )
        self.assertTrue(worker_result.ok)
        self.assertFalse(worker_result.blocking)

    def test_stage_worker_mounts_only_its_derived_cache_with_exact_labels(self):
        plan, _, worker = strict_stage_pipeline_fixture()
        assignment = plan.assignments[1]
        name = f"dure-ray-{plan.deployment_id}"
        inspect = (
            "docker",
            "inspect",
            "--format",
            DEPLOYMENT_IDENTITY_FORMAT,
            name,
        )
        runner = FakeRunner(
            responses={inspect: CommandResult(inspect, 1, stderr="not found")}
        )

        with patch("dure.runtime.validate_strict_stage_cache") as validate_cache:
            result = ContainerRuntime(runner).start_ray(
                worker, plan, assignment, replace=False
            )

        self.assertTrue(result.ok, result.detail)
        validate_cache.assert_called_once_with(plan, assignment)
        run = runner.calls[-1]
        expected_source = strict_model_mount_path(plan, assignment)
        self.assertEqual(
            run[run.index("--mount") + 1],
            f"type=bind,src={expected_source},dst=/models/model,readonly",
        )
        self.assertNotEqual(str(expected_source), plan.model_path)
        self.assertEqual(expected_source.parent.as_posix(), plan.model_path)
        for key, value in stage_identity_labels(plan, assignment).items():
            self.assertIn(f"{key}={value}", run)
        self.assertEqual(
            stage_cache_identity(plan, assignment).pipeline_rank,
            assignment.pipeline_rank,
        )

    def test_stage_api_revalidates_cache_and_uses_native_sharded_loader(self):
        plan, _, _ = strict_stage_pipeline_fixture()
        assignment = plan.assignments[0]
        name = f"dure-api-{plan.deployment_id}"
        inspect = (
            "docker",
            "inspect",
            "--format",
            DEPLOYMENT_IDENTITY_FORMAT,
            name,
        )
        runner = FakeRunner(
            responses={inspect: CommandResult(inspect, 1, stderr="not found")}
        )

        with patch("dure.runtime.validate_strict_stage_cache") as validate_cache:
            result = ContainerRuntime(runner).start_api(
                plan, assignment, replace=False
            )

        self.assertTrue(result.ok, result.detail)
        validate_cache.assert_called_once_with(plan, assignment)
        run = runner.calls[-1]
        self.assertEqual(run[run.index("--load-format") + 1], "sharded_state")
        for key, value in stage_identity_labels(plan, assignment).items():
            self.assertIn(f"{key}={value}", run)

        full_plan, _, _ = strict_pipeline_fixture()
        self.assertNotIn("--load-format", strict_vllm_api_command(full_plan))

    def test_stage_cache_failure_blocks_all_docker_actions(self):
        plan, _, worker = strict_stage_pipeline_fixture()
        assignment = plan.assignments[1]
        runner = FakeRunner()

        with patch(
            "dure.pipeline_runtime.validate_materialized_stage_cache",
            side_effect=StageCacheError("tampered stage cache"),
        ):
            result = ContainerRuntime(runner).start_ray(
                worker, plan, assignment, replace=False
            )

        self.assertFalse(result.ok)
        self.assertIn("integrity", result.detail)
        self.assertEqual(runner.calls, [])

    def test_stage_name_collision_with_swapped_manifest_is_never_replaced(self):
        plan, _, worker = strict_stage_pipeline_fixture()
        assignment = plan.assignments[1]
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
                    self.strict_stage_identity(
                        "swapped-stage",
                        "exited",
                        plan,
                        assignment,
                        RAY_COMPONENT,
                        stage_manifest="sha256:" + "9" * 64,
                    ),
                    "",
                )
            }
        )

        with patch("dure.runtime.validate_strict_stage_cache"):
            result = ContainerRuntime(runner).start_ray(
                worker, plan, assignment, replace=True
            )

        self.assertFalse(result.ok)
        self.assertIn("mismatched Dure identity", result.detail)
        self.assertFalse(any(call[:2] == ("docker", "rm") for call in runner.calls))
        self.assertFalse(any(call[:3] == ("docker", "run", "-d") for call in runner.calls))

    def test_strict_name_collision_with_missing_or_swapped_rank_is_never_replaced(self):
        plan, _, worker = strict_pipeline_fixture()
        assignment = plan.assignments[1]
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
                    self.strict_identity(
                        "foreign-rank",
                        "exited",
                        plan.deployment_id,
                        plan.generation,
                        assignment.node_id,
                        plan.execution_backend,
                        assignment.pipeline_rank,
                        0,
                        "ray-node",
                        strict_runtime_contract_digest(
                            plan, assignment, RAY_COMPONENT
                        ),
                    ),
                    "",
                )
            }
        )

        result = ContainerRuntime(runner).start_ray(
            worker, plan, assignment, replace=True
        )

        self.assertFalse(result.ok)
        self.assertFalse(any(call[:2] == ("docker", "rm") for call in runner.calls))
        self.assertFalse(any(call[:3] == ("docker", "run", "-d") for call in runner.calls))

    def test_strict_running_container_with_wrong_runtime_contract_is_not_reused(self):
        plan, _, worker = strict_pipeline_fixture()
        assignment = plan.assignments[1]
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
                    self.strict_identity(
                        "drifted",
                        "running",
                        plan.deployment_id,
                        plan.generation,
                        assignment.node_id,
                        plan.execution_backend,
                        assignment.pipeline_rank,
                        assignment.expected_runtime_rank,
                        "ray-node",
                        "sha256:" + "0" * 64,
                    ),
                    "",
                )
            }
        )

        result = ContainerRuntime(runner).start_ray(
            worker, plan, assignment, replace=True
        )

        self.assertFalse(result.ok)
        self.assertIn("mismatched Dure identity", result.detail)
        self.assertFalse(any(call[:2] == ("docker", "rm") for call in runner.calls))
        self.assertFalse(any(call[:3] == ("docker", "run", "-d") for call in runner.calls))

    def test_strict_stop_requires_exact_rank_and_component_labels(self):
        plan, _, worker = strict_pipeline_fixture()
        assignment = plan.assignments[1]
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
            "abc",
        )
        runner = FakeRunner(
            responses={
                listed: (0, "abc", ""),
                inspected: (
                    0,
                    self.strict_identity(
                        "abc",
                        "running",
                        plan.deployment_id,
                        plan.generation,
                        assignment.node_id,
                        plan.execution_backend,
                        assignment.pipeline_rank,
                        assignment.expected_runtime_rank,
                        "ray-node",
                        strict_runtime_contract_digest(
                            plan, assignment, RAY_COMPONENT
                        ),
                    ),
                    "",
                ),
                ("docker", "stop", "--time", "30", "abc"): (0, "abc", ""),
            }
        )

        check = ContainerRuntime(runner).stop_deployment(
            plan.deployment_id,
            generation=plan.generation,
            node_id=assignment.node_id,
            plan=plan,
            assignment=assignment,
        )

        self.assertTrue(check.ok, check.detail)
        restarting_responses = copy.deepcopy(runner.responses)
        restarting_responses[inspected] = (
            0,
            self.strict_identity(
                "abc",
                "restarting",
                plan.deployment_id,
                plan.generation,
                assignment.node_id,
                plan.execution_backend,
                assignment.pipeline_rank,
                assignment.expected_runtime_rank,
                "ray-node",
                strict_runtime_contract_digest(plan, assignment, RAY_COMPONENT),
            ),
            "",
        )
        restarting = FakeRunner(responses=restarting_responses)
        restarting_check = ContainerRuntime(restarting).stop_deployment(
            plan.deployment_id,
            generation=plan.generation,
            node_id=assignment.node_id,
            plan=plan,
            assignment=assignment,
        )
        self.assertTrue(restarting_check.ok, restarting_check.detail)
        self.assertIn(
            ("docker", "stop", "--time", "30", "abc"), restarting.calls
        )

        damaged_contract_responses = copy.deepcopy(runner.responses)
        damaged_contract_responses[inspected] = (
            0,
            self.strict_identity(
                "abc",
                "running",
                plan.deployment_id,
                plan.generation,
                assignment.node_id,
                plan.execution_backend,
                assignment.pipeline_rank,
                assignment.expected_runtime_rank,
                "ray-node",
                "damaged",
            ),
            "",
        )
        damaged_contract = FakeRunner(responses=damaged_contract_responses)
        damaged_contract_check = ContainerRuntime(damaged_contract).stop_deployment(
            plan.deployment_id,
            generation=plan.generation,
            node_id=assignment.node_id,
            plan=plan,
            assignment=assignment,
        )
        self.assertTrue(damaged_contract_check.ok, damaged_contract_check.detail)
        self.assertIn(
            ("docker", "stop", "--time", "30", "abc"),
            damaged_contract.calls,
        )

        swapped = copy.deepcopy(runner.responses)
        swapped[inspected] = (
            0,
            self.strict_identity(
                "abc",
                "running",
                plan.deployment_id,
                plan.generation,
                assignment.node_id,
                plan.execution_backend,
                assignment.pipeline_rank,
                0,
                "ray-node",
                strict_runtime_contract_digest(plan, assignment, RAY_COMPONENT),
            ),
            "",
        )
        rejecting = FakeRunner(responses=swapped)
        rejected = ContainerRuntime(rejecting).stop_deployment(
            plan.deployment_id,
            generation=plan.generation,
            node_id=assignment.node_id,
            plan=plan,
            assignment=assignment,
        )
        self.assertFalse(rejected.ok)
        self.assertFalse(any(call[:2] == ("docker", "stop") for call in rejecting.calls))

    def test_stage_stop_uses_exact_labels_without_requiring_cache_access(self):
        plan, _, worker = strict_stage_pipeline_fixture()
        assignment = plan.assignments[1]
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
            "abc",
        )
        runner = FakeRunner(
            responses={
                listed: (0, "abc", ""),
                inspected: (
                    0,
                    self.strict_stage_identity(
                        "abc", "running", plan, assignment, RAY_COMPONENT
                    ),
                    "",
                ),
                ("docker", "stop", "--time", "30", "abc"): (0, "abc", ""),
            }
        )

        with patch("dure.runtime.validate_strict_stage_cache") as validate_cache:
            result = ContainerRuntime(runner).stop_deployment(
                plan.deployment_id,
                generation=plan.generation,
                node_id=assignment.node_id,
                plan=plan,
                assignment=assignment,
            )

        self.assertTrue(result.ok, result.detail)
        validate_cache.assert_not_called()
        self.assertIn(("docker", "stop", "--time", "30", "abc"), runner.calls)

    def test_strict_invalid_contract_is_rejected_before_docker(self):
        plan, _, worker = strict_pipeline_fixture()
        plan.runtime_vllm_version = "0.9.1"
        runner = FakeRunner()

        result = ContainerRuntime(runner).start_ray(
            worker, plan, plan.assignments[1], replace=False
        )

        self.assertFalse(result.ok)
        self.assertEqual(runner.calls, [])

    def test_strict_node_drift_is_rejected_before_docker(self):
        plan, _, worker = strict_pipeline_fixture()
        second_gpu = copy.deepcopy(worker.gpus[0])
        second_gpu.index = 1
        second_gpu.uuid = "GPU-second"
        worker.gpus.append(second_gpu)
        runner = FakeRunner()

        result = ContainerRuntime(runner).start_ray(
            worker, plan, plan.assignments[1], replace=False
        )

        self.assertFalse(result.ok)
        self.assertIn("exactly one selected healthy GPU", result.detail)
        self.assertEqual(runner.calls, [])

    def test_strict_cache_marker_must_match_manifest_addressed_path(self):
        plan, _, worker = strict_pipeline_fixture()
        worker.installed_models[0].manifest_digest = "sha256:" + "d" * 64
        runner = FakeRunner()

        result = ContainerRuntime(runner).start_ray(
            worker, plan, plan.assignments[1], replace=False
        )

        self.assertFalse(result.ok)
        self.assertIn("verified FULL_SNAPSHOT model cache", result.detail)
        self.assertEqual(runner.calls, [])

    def test_foreign_name_collision_is_never_removed_or_started(self):
        node = profile("camp-7", address="192.168.0.228")
        plan = build_plan(
            [node],
            model_id="qwen2.5-32b-awq",
            image="registry.example/vllm@sha256:abc",
        )
        assert plan is not None
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
                    self.identity("foreign", "exited", "other", 1, "camp-7"),
                    "",
                )
            }
        )

        result = ContainerRuntime(runner).start_ray(
            node, plan, plan.assignments[0], replace=True
        )

        self.assertFalse(result.ok)
        self.assertFalse(any(call[:2] == ("docker", "rm") for call in runner.calls))
        self.assertFalse(any(call[:2] == ("docker", "run") for call in runner.calls))

    def test_exact_running_container_is_idempotent_without_remove_or_run(self):
        node = profile("camp-7", address="192.168.0.228")
        plan = build_plan(
            [node],
            model_id="qwen2.5-32b-awq",
            image="registry.example/vllm@sha256:abc",
        )
        assert plan is not None
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
                    self.identity(
                        "exact",
                        "running",
                        plan.deployment_id,
                        plan.generation,
                        "camp-7",
                    ),
                    "",
                )
            }
        )

        result = ContainerRuntime(runner).start_ray(
            node, plan, plan.assignments[0], replace=True
        )

        self.assertTrue(result.ok)
        self.assertFalse(any(call[:2] == ("docker", "rm") for call in runner.calls))
        self.assertFalse(any(call[:2] == ("docker", "run") for call in runner.calls))

    def test_exact_legacy_running_container_is_idempotent(self):
        node = profile("camp-7", address="192.168.0.228")
        plan = build_plan(
            [node],
            model_id="qwen2.5-32b-awq",
            image="registry.example/vllm@sha256:abc",
        )
        assert plan is not None
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
                    self.identity(
                        "legacy",
                        "running",
                        plan.deployment_id,
                        plan.generation,
                        "<no value>",
                    ),
                    "",
                )
            }
        )

        result = ContainerRuntime(runner).start_ray(
            node, plan, plan.assignments[0], replace=True
        )

        self.assertTrue(result.ok)
        self.assertFalse(any(call[:2] == ("docker", "rm") for call in runner.calls))
        self.assertFalse(any(call[:2] == ("docker", "run") for call in runner.calls))

    def test_exact_running_api_preserves_the_start_check_name(self):
        node = profile("camp-7", address="192.168.0.228")
        plan = build_plan(
            [node],
            model_id="qwen2.5-32b-awq",
            image="registry.example/vllm@sha256:abc",
        )
        assert plan is not None
        name = f"dure-api-{plan.deployment_id}"
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
                    self.identity(
                        "exact-api",
                        "running",
                        plan.deployment_id,
                        plan.generation,
                        "camp-7",
                    ),
                    "",
                )
            }
        )

        result = ContainerRuntime(runner).start_api(
            plan, plan.assignments[0], replace=True
        )

        self.assertTrue(result.ok)
        self.assertEqual(result.name, "vllm-api-start")
        self.assertFalse(any(call[:2] == ("docker", "rm") for call in runner.calls))
        self.assertFalse(any(call[:2] == ("docker", "run") for call in runner.calls))

    def test_exact_stopped_container_is_removed_by_id_before_restart(self):
        node = profile("camp-7", address="192.168.0.228")
        plan = build_plan(
            [node],
            model_id="qwen2.5-32b-awq",
            image="registry.example/vllm@sha256:abc",
        )
        assert plan is not None
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
                    self.identity(
                        "exact-id",
                        "exited",
                        plan.deployment_id,
                        plan.generation,
                        "camp-7",
                    ),
                    "",
                ),
                ("docker", "rm", "exact-id"): (0, "exact-id", ""),
            }
        )

        result = ContainerRuntime(runner).start_ray(
            node, plan, plan.assignments[0], replace=True
        )

        self.assertTrue(result.ok)
        self.assertIn(("docker", "rm", "exact-id"), runner.calls)
        self.assertTrue(any(call[:3] == ("docker", "run", "-d") for call in runner.calls))

    def test_inspect_failure_is_not_treated_as_container_absence(self):
        node = profile("camp-7", address="192.168.0.228")
        plan = build_plan(
            [node],
            model_id="qwen2.5-32b-awq",
            image="registry.example/vllm@sha256:abc",
        )
        assert plan is not None
        name = f"dure-ray-{plan.deployment_id}"
        inspect = (
            "docker",
            "inspect",
            "--format",
            DEPLOYMENT_IDENTITY_FORMAT,
            name,
        )
        runner = FakeRunner(responses={inspect: (125, "", "command timed out")})

        result = ContainerRuntime(runner).start_ray(
            node, plan, plan.assignments[0], replace=True
        )

        self.assertFalse(result.ok)
        self.assertFalse(any(call[:2] == ("docker", "rm") for call in runner.calls))
        self.assertFalse(any(call[:2] == ("docker", "run") for call in runner.calls))


if __name__ == "__main__":
    unittest.main()

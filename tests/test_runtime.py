import unittest

from dure.command import CommandResult
from dure.planner import build_plan
from dure.runtime import ContainerRuntime, DEPLOYMENT_IDENTITY_FORMAT

from .helpers import FakeRunner, profile


class RuntimeTests(unittest.TestCase):
    @staticmethod
    def identity(container_id, state, deployment_id, generation, node_id):
        return (
            f"{container_id}\t{state}\t{deployment_id}\t{generation}\t{node_id}"
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

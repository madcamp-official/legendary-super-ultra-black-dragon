import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dure.models import CheckResult
from dure.orchestrator import InitOrchestrator
from dure.pipeline_runtime import pipeline_contract_detail
from dure.planner import build_plan
from tests.helpers import FakeRunner, profile, strict_pipeline_fixture


class OrchestratorIdentityTests(unittest.TestCase):
    def test_central_node_id_overrides_hostname_for_assignment(self):
        central_id = "53c45b65-7f23-41fb-8457-663af742dacc"
        central_profile = profile(central_id)
        plan = build_plan([central_profile], image="registry/vllm@sha256:abc")
        with tempfile.TemporaryDirectory() as temporary:
            orchestrator = InitOrchestrator(
                runner=FakeRunner(), state_path=Path(temporary) / "state.json", node_id=central_id
            )
            orchestrator.probe.collect = lambda: profile("actual-hostname")
            observed, returned_plan, checks = orchestrator.run(plan=plan, apply=False)
        self.assertEqual(observed.node_id, central_id)
        self.assertIs(returned_plan, plan)
        self.assertTrue(all(item.ok for item in checks))

    def test_strict_apply_waits_for_pipeline_contract_instead_of_gpu_aggregate(self):
        plan, head, _ = strict_pipeline_fixture()
        assignment = plan.assignments[0]
        contract = CheckResult(
            "pipeline-rank-contract",
            True,
            pipeline_contract_detail(plan, assignment),
        )
        successful = CheckResult("test", True, "ok")
        with tempfile.TemporaryDirectory() as temporary, patch(
            "dure.orchestrator.ModelStore.ensure", return_value=successful
        ), patch(
            "dure.orchestrator.ContainerRuntime.ensure_image", return_value=successful
        ), patch(
            "dure.orchestrator.ContainerRuntime.start_ray", return_value=successful
        ), patch(
            "dure.orchestrator.ReadinessVerifier.host_gpu", return_value=successful
        ), patch(
            "dure.orchestrator.ReadinessVerifier.container_gpu", return_value=successful
        ), patch(
            "dure.orchestrator.ReadinessVerifier.wait_pipeline_rank_contract",
            return_value=contract,
        ) as wait_contract, patch(
            "dure.orchestrator.ReadinessVerifier.ray_cluster",
            side_effect=AssertionError("strict apply must not use aggregate GPU count"),
        ):
            orchestrator = InitOrchestrator(
                runner=FakeRunner(),
                state_path=Path(temporary) / "state.json",
                node_id=head.node_id,
            )
            orchestrator.probe.collect = lambda: head
            _, _, checks = orchestrator.run(plan=plan, apply=True, serve=False)

        self.assertEqual(
            [item.name for item in checks].count("pipeline-rank-contract"), 1
        )
        self.assertTrue(all(item.ok for item in checks))
        self.assertFalse(wait_contract.call_args.kwargs["require_actors"])

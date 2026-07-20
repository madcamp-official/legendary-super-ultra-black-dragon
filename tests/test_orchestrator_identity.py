import tempfile
import unittest
from pathlib import Path

from dure.orchestrator import InitOrchestrator
from dure.planner import build_plan
from tests.helpers import FakeRunner, profile


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

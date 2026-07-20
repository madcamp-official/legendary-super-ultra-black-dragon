import json
import tempfile
import unittest
from pathlib import Path

from dure.state import NodeState, StateStore


class StateTests(unittest.TestCase):
    def test_atomic_round_trip(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            store = StateStore(path)
            store.save(NodeState(phase="READY", node_id="camp-9", generation=4))

            loaded = store.load()

            self.assertEqual(loaded.phase, "READY")
            self.assertEqual(loaded.node_id, "camp-9")
            self.assertEqual(loaded.generation, 4)
            self.assertEqual(json.loads(path.read_text())["phase"], "READY")


if __name__ == "__main__":
    unittest.main()


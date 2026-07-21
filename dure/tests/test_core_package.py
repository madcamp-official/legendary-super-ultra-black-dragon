from __future__ import annotations

import os
import subprocess
import sys
import unittest
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


class CorePackageTests(unittest.TestCase):
    def _run_without_site_packages(self, code: str) -> subprocess.CompletedProcess[str]:
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(REPOSITORY_ROOT / "src")
        return subprocess.run(
            [sys.executable, "-S", "-c", code],
            cwd=REPOSITORY_ROOT,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )

    def test_node_cli_and_agent_import_without_third_party_packages(self):
        result = self._run_without_site_packages(
            "import dure.agent, dure.bootstrap, dure.cli, dure.diagnostics; "
            "print(dure.__version__)"
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "0.3.23")

    def test_agent_service_waits_for_joined_node_config(self):
        unit = (REPOSITORY_ROOT / "packaging" / "dure-agent.service").read_text(
            encoding="utf-8"
        )

        self.assertEqual(
            unit.count("ConditionPathExists=/etc/dure/agent.json"),
            1,
        )

    def test_packaged_control_plane_uses_production_https(self):
        values = {}
        for line in (REPOSITORY_ROOT / "packaging" / "dure-client.env").read_text(
            encoding="utf-8"
        ).splitlines():
            if line and not line.startswith("#"):
                key, value = line.split("=", 1)
                values[key] = value

        self.assertEqual(values["DURE_SERVER"], "https://api.dure.madcamp-kaist.org")
        self.assertEqual(values["DURE_INSECURE"], "false")

    def test_server_reports_missing_optional_dependencies(self):
        result = self._run_without_site_packages(
            "from dure.server import main; "
            "main(['--migrate', '--database-url', 'sqlite://'])"
        )

        self.assertEqual(result.returncode, 2)
        self.assertIn("install Dure with the server extra", result.stderr)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from dure.bootstrap import (
    BOOTSTRAP_COMMAND_ENV,
    BootstrapCheck,
    BootstrapReport,
    Bootstrapper,
    DOCKER_DAEMON_BACKUP_PATH,
    DOCKER_DAEMON_CONFIG_PATH,
    DOCKER_CONFLICTING_PACKAGES,
    DOCKER_KEY_FINGERPRINT,
    DOCKER_KEY_PATH,
    DOCKER_KEY_URL,
    DOCKER_HOST_ARG,
    DOCKER_PACKAGES,
    DOCKER_SOURCE_PATH,
    MAX_SIGNING_KEY_BYTES,
    NVIDIA_KEY_FINGERPRINT,
    NVIDIA_KEY_URL,
    NVIDIA_PACKAGES,
    NVIDIA_PREFERENCES_PATH,
    NVIDIA_SOURCE_PATH,
    NVIDIA_TOOLKIT_VERSION,
)
from dure.cli import main
from dure.command import CommandResult


class StrictBootstrapRunner:
    def __init__(
        self,
        *,
        docker: bool = False,
        toolkit: bool = False,
        runtime: bool = False,
        running_containers: int = 0,
        conflicts: set[str] | None = None,
        fail_nvidia_config: bool = False,
        fail_docker_restart: bool = False,
        runtime_output: str | None = None,
        local_docker_service: bool = True,
        architecture: str | None = "amd64",
        driver_ready: bool = True,
        toolkit_packages: set[str] | None = None,
        toolkit_versions: dict[str, str] | None = None,
        running_container_counts: list[int] | None = None,
        extra_key_fingerprint: str | None = None,
        register_runtime_on_restart: bool = True,
        docker_engine_name: str = "Docker Engine - Community",
        agent_active: bool = False,
        agent_active_states: list[int] | None = None,
        agent_service_loaded: bool = True,
        docker_packages: set[str] | None = None,
        docker_service_without_cli: bool = False,
        docker_version: str = "29.0.0",
        docker_client_version: str | None = None,
        mutate_unrelated_daemon_config: bool = False,
        docker_service_enabled: bool = True,
        oversized_key_download: bool = False,
    ) -> None:
        self.docker = docker
        self.toolkit = toolkit
        self.runtime = runtime
        self.running_containers = running_containers
        self.conflicts = conflicts or set()
        self.fail_nvidia_config = fail_nvidia_config
        self.fail_docker_restart = fail_docker_restart
        self.runtime_output = runtime_output
        self.local_docker_service = local_docker_service
        self.architecture = architecture
        self.driver_ready = driver_ready
        self.running_container_counts = list(running_container_counts or [])
        self.extra_key_fingerprint = extra_key_fingerprint
        self.register_runtime_on_restart = register_runtime_on_restart
        self.docker_engine_name = docker_engine_name
        self.agent_active = agent_active
        self.agent_active_states = list(agent_active_states or [])
        self.agent_service_loaded = agent_service_loaded
        self.docker_packages = set(docker_packages or ())
        self.docker_service_without_cli = docker_service_without_cli
        self.docker_version = docker_version
        self.docker_client_version = (
            docker_version if docker_client_version is None else docker_client_version
        )
        self.mutate_unrelated_daemon_config = mutate_unrelated_daemon_config
        self.docker_service_enabled = docker_service_enabled
        self.oversized_key_download = oversized_key_download
        self.prerequisites = False
        self.calls: list[tuple[str, ...]] = []
        self.environments: list[dict[str, str] | None] = []
        self.limited_output_calls: list[tuple[tuple[str, ...], int]] = []
        self.installed_packages = (
            set(toolkit_packages)
            if toolkit_packages is not None
            else (set(NVIDIA_PACKAGES) if toolkit else set())
        )
        self.toolkit_versions = toolkit_versions or {
            package: NVIDIA_TOOLKIT_VERSION for package in self.installed_packages
        }
        self.restart_calls = 0

    def exists(self, executable: str) -> bool:
        always = {"apt-get", "dpkg", "dpkg-query", "systemctl", "dure-agent"}
        if executable in always:
            return True
        if executable == "nvidia-smi":
            return self.driver_ready
        if executable in {"curl", "gpg"}:
            return self.prerequisites
        if executable == "docker":
            return self.docker
        if executable in {"nvidia-ctk", "nvidia-container-runtime"}:
            return self.toolkit
        return False

    def run(self, argv, *, timeout=15, env=None):
        command = tuple(str(part) for part in argv)
        self.calls.append(command)
        self.environments.append(dict(env) if env is not None else None)

        if command == ("dpkg", "--print-architecture"):
            if self.architecture is None:
                return CommandResult(command, 1, stderr="dpkg architecture unavailable")
            return CommandResult(command, 0, self.architecture)
        if command == (
            "nvidia-smi",
            "--query-gpu=driver_version",
            "--format=csv,noheader,nounits",
        ):
            return CommandResult(command, 0, "550.54.14")
        if len(command) == 4 and command[:3] == (
            "dpkg-query",
            "-W",
            "-f=${Status}",
        ):
            package = command[-1]
            self.assert_known_package(package)
            if (
                package in self.conflicts
                or package in self.installed_packages
                or package in self.docker_packages
            ):
                return CommandResult(command, 0, "install ok installed")
            return CommandResult(command, 1, stderr="not installed")
        if len(command) == 4 and command[:3] == (
            "dpkg-query",
            "-W",
            "-f=${Version}",
        ):
            package = command[-1]
            self.assert_known_package(package)
            version = self.toolkit_versions.get(package)
            if package in self.installed_packages and version:
                return CommandResult(command, 0, version)
            return CommandResult(command, 1, stderr="not installed")
        if command == (
            "docker",
            DOCKER_HOST_ARG,
            "version",
            "--format",
            "{{json .}}",
        ):
            if self.docker:
                return CommandResult(
                    command,
                    0,
                    json.dumps(
                        {
                            "Client": {"Version": self.docker_client_version},
                            "Server": {
                                "Platform": {"Name": self.docker_engine_name},
                                "Version": self.docker_version,
                            },
                        }
                    ),
                )
            return CommandResult(command, 127, stderr="missing")
        if command == (
            "docker",
            DOCKER_HOST_ARG,
            "info",
            "--format",
            "{{json .Runtimes}}",
        ):
            value = self.runtime_output
            if value is None:
                value = (
                    '{"runc":{"path":"runc"},"nvidia":{"path":"nvidia-container-runtime"}}'
                    if self.runtime
                    else '{"runc":{"path":"runc"}}'
                )
            return CommandResult(command, 0 if self.docker else 1, value)
        if command == (
            "docker",
            DOCKER_HOST_ARG,
            "ps",
            "--quiet",
            "--no-trunc",
        ):
            count = (
                self.running_container_counts.pop(0)
                if self.running_container_counts
                else self.running_containers
            )
            lines = "\n".join(str(index) * 64 for index in range(1, count + 1))
            return CommandResult(command, 0 if self.docker else 1, lines)
        if command == ("nvidia-ctk", "--version"):
            if self.toolkit:
                return CommandResult(command, 0, "NVIDIA Container Toolkit CLI version 1.19.1")
            return CommandResult(command, 127, stderr="missing")
        if command == ("apt-get", "update"):
            return CommandResult(command, 0)
        if command == (
            "apt-get",
            "install",
            "-y",
            "--no-install-recommends",
            "--no-remove",
            "ca-certificates",
            "curl",
            "gpg",
        ):
            self.prerequisites = True
            return CommandResult(command, 0)
        if command == (
            "apt-get",
            "install",
            "-y",
            "--no-install-recommends",
            "--no-remove",
            *DOCKER_PACKAGES,
        ):
            self.docker = True
            return CommandResult(command, 0)
        nvidia_install = (
            "apt-get",
            "install",
            "-y",
            "--no-install-recommends",
            "--no-remove",
            *(f"{package}={NVIDIA_TOOLKIT_VERSION}" for package in NVIDIA_PACKAGES),
        )
        if command == nvidia_install:
            self.toolkit = True
            self.installed_packages.update(NVIDIA_PACKAGES)
            self.toolkit_versions.update(
                {package: NVIDIA_TOOLKIT_VERSION for package in NVIDIA_PACKAGES}
            )
            return CommandResult(command, 0)
        if len(command) == 15 and command[:12] == (
            "curl",
            "--disable",
            "--fail",
            "--silent",
            "--show-error",
            "--location",
            "--proto",
            "=https",
            "--proto-redir",
            "=https",
            "--max-filesize",
            str(MAX_SIGNING_KEY_BYTES),
        ) and command[12:14] == ("--output", "-"):
            url = command[14]
            if url not in {DOCKER_KEY_URL, NVIDIA_KEY_URL}:
                raise AssertionError(f"unexpected bootstrap URL: {url}")
            fingerprint = (
                DOCKER_KEY_FINGERPRINT if url == DOCKER_KEY_URL else NVIDIA_KEY_FINGERPRINT
            )
            return CommandResult(command, 0, fingerprint)
        if (
            len(command) == 8
            and command[:4] == ("gpg", "--batch", "--no-options", "--homedir")
            and command[5:7] == ("--show-keys", "--with-colons")
        ):
            key = command[-1]
            try:
                candidate = Path(key).read_text(encoding="ascii").strip()
            except (OSError, UnicodeError):
                candidate = ""
            fingerprint = (
                candidate
                if candidate in {DOCKER_KEY_FINGERPRINT, NVIDIA_KEY_FINGERPRINT}
                else None
            )
            if fingerprint is None:
                fingerprint = (
                    DOCKER_KEY_FINGERPRINT if "docker" in key else NVIDIA_KEY_FINGERPRINT
                )
            records = (
                f"pub:-:4096:1:0000000000000000:0:0::::::scESC:\nfpr:::::::::{fingerprint}:"
            )
            if self.extra_key_fingerprint:
                records += (
                    "\npub:-:4096:1:1111111111111111:0:0::::::scESC:"
                    f"\nfpr:::::::::{self.extra_key_fingerprint}:"
                )
            return CommandResult(
                command,
                0,
                records,
            )
        if (
            len(command) == 10
            and command[:4] == ("gpg", "--batch", "--no-options", "--homedir")
            and command[5:7] == ("--yes", "--dearmor")
        ):
            output = Path(command[command.index("--output") + 1])
            source = Path(command[-1])
            output.write_bytes(b"dearmored:" + source.read_bytes())
            return CommandResult(command, 0)
        if command == ("systemctl", "enable", "--now", "docker"):
            self.docker = True
            return CommandResult(command, 0)
        if command == (
            "systemctl",
            "show",
            "--property=LoadState",
            "--value",
            "docker.service",
        ):
            value = (
                "loaded"
                if self.local_docker_service
                and (self.docker or self.docker_service_without_cli)
                else "not-found"
            )
            return CommandResult(command, 0, value)
        if command == (
            "systemctl",
            "show",
            "--property=UnitFileState",
            "--value",
            "docker.service",
        ):
            value = (
                "enabled"
                if self.local_docker_service
                and self.docker
                and self.docker_service_enabled
                else "disabled"
            )
            return CommandResult(command, 0, value)
        if command in {
            (
                "systemctl",
                "show",
                "--property=LoadState",
                "--value",
                "docker.socket",
            ),
            (
                "systemctl",
                "show",
                "--property=LoadState",
                "--value",
                "containerd.service",
            ),
        }:
            return CommandResult(command, 0, "not-found")
        if command == (
            "systemctl",
            "show",
            "--property=UnitFileState",
            "--value",
            "docker.socket",
        ):
            return CommandResult(command, 0, "disabled")
        if command == (
            "systemctl",
            "show",
            "--property=ActiveState",
            "--value",
            "docker.service",
        ):
            value = (
                "active"
                if self.local_docker_service
                and (self.docker or self.docker_service_without_cli)
                else "inactive"
            )
            return CommandResult(command, 0, value)
        if command == (
            "systemctl",
            "show",
            "--property=LoadState",
            "--value",
            "dure-agent.service",
        ):
            value = "loaded" if self.agent_service_loaded else "not-found"
            return CommandResult(command, 0, value)
        if command == ("systemctl", "is-active", "--quiet", "dure-agent"):
            returncode = (
                self.agent_active_states.pop(0)
                if self.agent_active_states
                else (0 if self.agent_active else 3)
            )
            return CommandResult(command, returncode)
        if len(command) == 5 and command[:4] == (
            "nvidia-ctk",
            "runtime",
            "configure",
            "--runtime=docker",
        ) and command[4].startswith("--config="):
            config = Path(command[-1].split("=", 1)[1])
            config.parent.mkdir(parents=True, exist_ok=True)
            current = (
                json.loads(config.read_text(encoding="utf-8"))
                if config.exists()
                else {}
            )
            current.setdefault("runtimes", {})["nvidia"] = {
                "path": "nvidia-container-runtime",
                "args": [],
            }
            if self.mutate_unrelated_daemon_config:
                current["log-driver"] = "mutated"
            config.write_text(
                json.dumps(current, separators=(",", ":"), sort_keys=True),
                encoding="utf-8",
            )
            if self.fail_nvidia_config:
                return CommandResult(command, 1, stderr="configuration failed")
            return CommandResult(command, 0)
        if command == ("systemctl", "restart", "docker"):
            self.restart_calls += 1
            if self.fail_docker_restart and self.restart_calls == 1:
                return CommandResult(command, 1, stderr="restart failed")
            if self.register_runtime_on_restart:
                self.runtime = True
            return CommandResult(command, 0)
        raise AssertionError(f"unexpected bootstrap command: {command}")

    def run_limited_output(
        self,
        argv,
        *,
        timeout=15,
        max_output_bytes,
        env=None,
    ):
        command = tuple(str(part) for part in argv)
        self.limited_output_calls.append((command, max_output_bytes))
        result = self.run(argv, timeout=timeout, env=env)
        if self.oversized_key_download and command[0] == "curl":
            return CommandResult(
                command,
                125,
                stderr="command output limit exceeded",
            )
        if (
            len(result.stdout.encode("utf-8")) + len(result.stderr.encode("utf-8"))
            > max_output_bytes
        ):
            return CommandResult(command, 125, stderr="command output limit exceeded")
        return result

    @staticmethod
    def assert_known_package(package: str) -> None:
        known = set(DOCKER_CONFLICTING_PACKAGES) | set(DOCKER_PACKAGES) | set(NVIDIA_PACKAGES)
        if package not in known:
            raise AssertionError(f"unexpected package query: {package}")


def make_root(directory: str, *, version: str = "22.04") -> Path:
    root = Path(directory)
    (root / "etc").mkdir(parents=True)
    (root / "etc" / "os-release").write_text(
        f'ID=ubuntu\nVERSION_ID="{version}"\n', encoding="utf-8"
    )
    return root


class BootstrapTests(unittest.TestCase):
    def test_default_is_read_only_and_returns_a_closed_plan(self):
        with tempfile.TemporaryDirectory() as directory:
            root = make_root(directory)
            runner = StrictBootstrapRunner()

            report = Bootstrapper(runner, root=root).run()

            self.assertFalse(report.blocked)
            self.assertFalse(report.changed)
            self.assertFalse(report.ready)
            self.assertEqual(
                [action.action_id for action in report.actions],
                [
                    "INSTALL_APT_PREREQUISITES",
                    "CONFIGURE_DOCKER_REPOSITORY",
                    "CONFIGURE_NVIDIA_REPOSITORY",
                    "CONFIGURE_NVIDIA_VERSION_PIN",
                    "INSTALL_DOCKER",
                    "START_DOCKER",
                    "INSTALL_NVIDIA_TOOLKIT",
                    "CONFIGURE_NVIDIA_RUNTIME",
                    "RESTART_DOCKER",
                ],
            )
            self.assertFalse(any(call[0] in {"apt-get", "curl", "gpg", "nvidia-ctk"} for call in runner.calls))
            self.assertFalse(
                any(call[:2] in {("systemctl", "restart"), ("systemctl", "enable")} for call in runner.calls)
            )
            self.assertFalse((root / "etc" / "apt" / "sources.list.d").exists())

    def test_apply_requires_root_before_any_host_change(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = StrictBootstrapRunner()
            report = Bootstrapper(
                runner,
                root=make_root(directory),
                effective_uid=lambda: 1000,
            ).run(apply=True)

            self.assertTrue(report.blocked)
            self.assertIn("ROOT_REQUIRED", [check.code for check in report.checks])
            self.assertFalse(any(call[0] == "apt-get" for call in runner.calls))

    def test_concurrent_apply_is_rejected_by_the_local_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            root = make_root(directory)
            first = Bootstrapper(
                StrictBootstrapRunner(), root=root, effective_uid=lambda: 0
            )
            descriptor = first._acquire_apply_lock()
            try:
                report = Bootstrapper(
                    StrictBootstrapRunner(), root=root, effective_uid=lambda: 0
                ).run(apply=True)
            finally:
                os.close(descriptor)

            self.assertTrue(report.blocked)
            self.assertIn("BOOTSTRAP_LOCKED", [check.code for check in report.checks])

    def test_joined_or_active_agent_node_is_not_bootstrapped(self):
        with tempfile.TemporaryDirectory() as directory:
            root = make_root(directory)
            config = root / "etc" / "dure" / "agent.json"
            config.parent.mkdir(parents=True)
            config.write_text("{}", encoding="utf-8")
            runner = StrictBootstrapRunner()

            report = Bootstrapper(
                runner, root=root, effective_uid=lambda: 0
            ).run(apply=True)

            self.assertTrue(report.blocked)
            self.assertIn("NODE_ALREADY_JOINED", [check.code for check in report.checks])
            self.assertFalse(any(call[0] == "apt-get" for call in runner.calls))

    def test_packaged_agent_service_is_required_before_host_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = StrictBootstrapRunner(agent_service_loaded=False)

            report = Bootstrapper(
                runner, root=make_root(directory), effective_uid=lambda: 0
            ).run(apply=True)

            self.assertTrue(report.blocked)
            self.assertIn(
                "DURE_AGENT_SERVICE_MISSING", [check.code for check in report.checks]
            )
            self.assertFalse(report.changed)
            self.assertFalse(any(call[0] == "apt-get" for call in runner.calls))

    def test_agent_activation_race_fails_before_reporting_a_host_change(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = StrictBootstrapRunner(agent_active_states=[3, 3, 0])

            report = Bootstrapper(
                runner, root=make_root(directory), effective_uid=lambda: 0
            ).run(apply=True)

            self.assertTrue(report.blocked)
            self.assertIn("APPLY_FAILED", [check.code for check in report.checks])
            self.assertFalse(report.changed)
            self.assertFalse(any(call[0] == "apt-get" for call in runner.calls))

        with tempfile.TemporaryDirectory() as directory:
            runner = StrictBootstrapRunner(agent_active=True)
            report = Bootstrapper(
                runner, root=make_root(directory), effective_uid=lambda: 0
            ).run(apply=True)

            self.assertTrue(report.blocked)
            self.assertIn("DURE_AGENT_ACTIVE", [check.code for check in report.checks])
            self.assertFalse(any(call[0] == "apt-get" for call in runner.calls))

    def test_unsupported_os_is_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = StrictBootstrapRunner()
            root = make_root(directory, version="20.04")

            report = Bootstrapper(runner, root=root).run(apply=True)

            self.assertTrue(report.blocked)
            self.assertIn("UNSUPPORTED_OS", [check.code for check in report.checks])
            self.assertFalse(any(call[0] == "apt-get" for call in runner.calls))

    def test_unknown_or_unsupported_dpkg_architecture_is_fail_closed(self):
        for architecture in (None, "ppc64el"):
            with self.subTest(architecture=architecture), tempfile.TemporaryDirectory() as directory:
                runner = StrictBootstrapRunner(architecture=architecture)

                report = Bootstrapper(
                    runner, root=make_root(directory), effective_uid=lambda: 0
                ).run(apply=True)

                self.assertTrue(report.blocked)
                self.assertIn(
                    "UNSUPPORTED_ARCHITECTURE", [check.code for check in report.checks]
                )
                self.assertFalse(any(call[0] == "apt-get" for call in runner.calls))

    def test_missing_driver_is_fail_closed_without_install_attempt(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = StrictBootstrapRunner(driver_ready=False)

            report = Bootstrapper(
                runner, root=make_root(directory), effective_uid=lambda: 0
            ).run(apply=True)

            self.assertTrue(report.blocked)
            self.assertIn("NVIDIA_DRIVER_MISSING", [check.code for check in report.checks])
            joined = "\n".join(" ".join(call) for call in runner.calls)
            self.assertNotIn("nvidia-driver", joined)
            self.assertFalse(any(call[0] == "apt-get" for call in runner.calls))

    def test_partial_toolkit_installation_is_not_repaired_implicitly(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = StrictBootstrapRunner(
                toolkit=False,
                toolkit_packages={"libnvidia-container1"},
            )

            report = Bootstrapper(runner, root=make_root(directory)).run()

            self.assertTrue(report.blocked)
            self.assertIn("NVIDIA_TOOLKIT_PARTIAL", [check.code for check in report.checks])

    def test_mixed_or_unverified_toolkit_versions_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            versions = {
                package: NVIDIA_TOOLKIT_VERSION for package in NVIDIA_PACKAGES
            }
            versions["libnvidia-container1"] = "1.18.0-1"
            runner = StrictBootstrapRunner(
                toolkit=True,
                toolkit_versions=versions,
            )

            report = Bootstrapper(runner, root=make_root(directory)).run()

            self.assertTrue(report.blocked)
            self.assertIn(
                "NVIDIA_TOOLKIT_VERSION_UNSUPPORTED",
                [check.code for check in report.checks],
            )

    def test_runtime_json_requires_the_exact_nvidia_key_and_valid_json(self):
        with tempfile.TemporaryDirectory() as directory:
            misleading = StrictBootstrapRunner(
                docker=True,
                toolkit=True,
                runtime_output='{"notnvidia":{"path":"nvidia-container-runtime"}}',
            )
            report = Bootstrapper(misleading, root=make_root(directory)).run()

            self.assertFalse(report.ready)
            self.assertIn(
                "CONFIGURE_NVIDIA_RUNTIME",
                [action.action_id for action in report.actions],
            )

        with tempfile.TemporaryDirectory() as directory:
            malformed = StrictBootstrapRunner(
                docker=True,
                toolkit=True,
                runtime_output="not-json",
            )
            report = Bootstrapper(malformed, root=make_root(directory)).run()

            self.assertTrue(report.blocked)
            self.assertIn(
                "DOCKER_RUNTIME_INSPECTION_INVALID",
                [check.code for check in report.checks],
            )

        invalid_entries = (
            '{"nvidia":{"path":"/tmp/nvidia-container-runtime"}}',
            '{"nvidia":{"path":"nvidia-container-runtime","args":["--debug"]}}',
            '{"nvidia":{"path":"nvidia-container-runtime","command":"sh"}}',
        )
        for runtime_output in invalid_entries:
            with self.subTest(runtime_output=runtime_output), tempfile.TemporaryDirectory() as directory:
                runner = StrictBootstrapRunner(
                    docker=True,
                    toolkit=True,
                    runtime_output=runtime_output,
                )

                report = Bootstrapper(runner, root=make_root(directory)).run()

                self.assertTrue(report.blocked)
                self.assertIn(
                    "NVIDIA_RUNTIME_INVALID", [check.code for check in report.checks]
                )

    def test_non_systemd_or_remote_docker_is_not_reconfigured(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = StrictBootstrapRunner(
                docker=True,
                toolkit=True,
                runtime=False,
                local_docker_service=False,
            )

            report = Bootstrapper(
                runner, root=make_root(directory), effective_uid=lambda: 0
            ).run(apply=True)

            self.assertTrue(report.blocked)
            self.assertIn("DOCKER_SERVICE_UNSUPPORTED", [check.code for check in report.checks])
            self.assertFalse(
                any(call[:3] == ("nvidia-ctk", "runtime", "configure") for call in runner.calls)
            )

    def test_active_but_not_boot_persistent_docker_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = StrictBootstrapRunner(
                docker=True,
                toolkit=True,
                runtime=True,
                docker_service_enabled=False,
            )

            report = Bootstrapper(runner, root=make_root(directory)).run()

            self.assertTrue(report.blocked)
            self.assertIn(
                "DOCKER_SERVICE_UNSUPPORTED", [check.code for check in report.checks]
            )

    def test_docker_compatible_shim_is_not_accepted_as_docker_engine(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = StrictBootstrapRunner(
                docker=True,
                toolkit=True,
                runtime=True,
                docker_engine_name="Podman Engine",
            )

            report = Bootstrapper(runner, root=make_root(directory)).run()

            self.assertTrue(report.blocked)
            self.assertIn("DOCKER_ENGINE_UNSUPPORTED", [check.code for check in report.checks])

    def test_old_or_unparseable_docker_version_is_rejected(self):
        for version in ("18.09.9", "19.03.15", "not-a-version"):
            with self.subTest(version=version), tempfile.TemporaryDirectory() as directory:
                runner = StrictBootstrapRunner(
                    docker=True,
                    toolkit=True,
                    runtime=True,
                    docker_version=version,
                )

                report = Bootstrapper(runner, root=make_root(directory)).run()

                self.assertTrue(report.blocked)
                self.assertIn(
                    "DOCKER_VERSION_UNSUPPORTED"
                    if version != "not-a-version"
                    else "DOCKER_ENGINE_UNSUPPORTED",
                    [check.code for check in report.checks],
                )

        with tempfile.TemporaryDirectory() as directory:
            runner = StrictBootstrapRunner(
                docker=True,
                toolkit=True,
                runtime=True,
                docker_version="29.0.0",
                docker_client_version="18.09.9",
            )

            report = Bootstrapper(runner, root=make_root(directory)).run()

            self.assertTrue(report.blocked)
            self.assertIn(
                "DOCKER_VERSION_UNSUPPORTED", [check.code for check in report.checks]
            )

        with tempfile.TemporaryDirectory() as directory:
            runner = StrictBootstrapRunner(
                docker=True,
                toolkit=True,
                runtime=True,
                docker_version="20.10.0",
                docker_client_version="20.10.0",
            )

            report = Bootstrapper(runner, root=make_root(directory)).run()

            self.assertFalse(report.blocked, report.to_dict())

    def test_missing_cli_with_existing_docker_state_is_never_repaired(self):
        cases = (
            {"docker_packages": {"docker-ce"}},
            {"docker_service_without_cli": True},
        )
        for values in cases:
            with self.subTest(values=values), tempfile.TemporaryDirectory() as directory:
                runner = StrictBootstrapRunner(**values)

                report = Bootstrapper(
                    runner, root=make_root(directory), effective_uid=lambda: 0
                ).run(apply=True)

                self.assertTrue(report.blocked)
                self.assertIn(
                    "DOCKER_INSTALLATION_AMBIGUOUS",
                    [check.code for check in report.checks],
                )
                self.assertFalse(report.changed)
                self.assertFalse(any(call[0] == "apt-get" for call in runner.calls))

        with tempfile.TemporaryDirectory() as directory:
            root = make_root(directory)
            socket = root / "var" / "run" / "docker.sock"
            socket.parent.mkdir(parents=True)
            socket.touch()

            report = Bootstrapper(
                StrictBootstrapRunner(), root=root, effective_uid=lambda: 0
            ).run(apply=True)

            self.assertTrue(report.blocked)
            self.assertIn(
                "DOCKER_INSTALLATION_AMBIGUOUS", [check.code for check in report.checks]
            )

    def test_existing_managed_repository_is_checked_on_a_ready_host(self):
        with tempfile.TemporaryDirectory() as directory:
            root = make_root(directory)
            source = root / DOCKER_SOURCE_PATH.lstrip("/")
            key = root / DOCKER_KEY_PATH.lstrip("/")
            source.parent.mkdir(parents=True)
            key.parent.mkdir(parents=True, exist_ok=True)
            source.write_text("unexpected source\n", encoding="utf-8")
            key.write_text(DOCKER_KEY_FINGERPRINT, encoding="ascii")
            key.chmod(0o644)
            pin = root / NVIDIA_PREFERENCES_PATH.lstrip("/")
            pin.parent.mkdir(parents=True)
            pin.write_text(Bootstrapper._nvidia_preferences(), encoding="utf-8")
            runner = StrictBootstrapRunner(docker=True, toolkit=True, runtime=True)
            runner.prerequisites = True

            report = Bootstrapper(runner, root=root).run()

            self.assertTrue(report.blocked)
            self.assertIn(
                "DOCKER_REPOSITORY_CONFLICT", [check.code for check in report.checks]
            )

    def test_repository_source_without_key_is_blocked_before_apt_update(self):
        with tempfile.TemporaryDirectory() as directory:
            root = make_root(directory)
            source = root / DOCKER_SOURCE_PATH.lstrip("/")
            source.parent.mkdir(parents=True)
            source.write_text(
                Bootstrapper._docker_source("jammy", "amd64"),
                encoding="utf-8",
            )
            runner = StrictBootstrapRunner()

            report = Bootstrapper(
                runner, root=root, effective_uid=lambda: 0
            ).run(apply=True)

            self.assertTrue(report.blocked)
            self.assertIn("DOCKER_REPOSITORY_PARTIAL", [check.code for check in report.checks])
            self.assertNotIn(("apt-get", "update"), runner.calls)

    def test_unreadable_repository_key_or_parent_is_blocked_before_apt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = make_root(directory)
            key = root / "etc" / "apt" / "keyrings" / "docker.asc"
            key.parent.mkdir(parents=True)
            key.write_text(DOCKER_KEY_FINGERPRINT, encoding="ascii")
            key.chmod(0o600)
            runner = StrictBootstrapRunner()
            runner.prerequisites = True

            report = Bootstrapper(
                runner, root=root, effective_uid=lambda: 0
            ).run(apply=True)

            self.assertTrue(report.blocked)
            self.assertIn("DOCKER_KEY_PERMISSIONS", [check.code for check in report.checks])
            self.assertFalse(any(call[0] == "apt-get" for call in runner.calls))

        with tempfile.TemporaryDirectory() as directory:
            root = make_root(directory)
            key = root / DOCKER_KEY_PATH.lstrip("/")
            key.parent.mkdir(parents=True)
            key.write_text(DOCKER_KEY_FINGERPRINT, encoding="ascii")
            key.chmod(0o644)

            report = Bootstrapper(
                StrictBootstrapRunner(), root=root, effective_uid=lambda: 0
            ).run(apply=True)

            self.assertTrue(report.blocked)
            self.assertIn("DOCKER_KEY_UNVERIFIED", [check.code for check in report.checks])

        with tempfile.TemporaryDirectory() as directory:
            root = make_root(directory)
            key_parent = root / "etc" / "apt" / "keyrings"
            key_parent.mkdir(parents=True)
            key_parent.chmod(0o700)

            report = Bootstrapper(
                StrictBootstrapRunner(), root=root, effective_uid=lambda: 0
            ).run(apply=True)

            self.assertTrue(report.blocked)
            self.assertIn(
                "DOCKER_KEY_PATH_PERMISSIONS", [check.code for check in report.checks]
            )

        with tempfile.TemporaryDirectory() as directory:
            root = make_root(directory)
            key = root / DOCKER_KEY_PATH.lstrip("/")
            key.parent.mkdir(parents=True)
            key.write_bytes(b"x" * (MAX_SIGNING_KEY_BYTES + 1))
            key.chmod(0o644)

            report = Bootstrapper(
                StrictBootstrapRunner(), root=root, effective_uid=lambda: 0
            ).run(apply=True)

            self.assertTrue(report.blocked)
            self.assertIn("DOCKER_KEY_SIZE", [check.code for check in report.checks])

    def test_signing_key_download_is_aborted_during_transfer_at_one_mibibyte(self):
        with tempfile.TemporaryDirectory() as directory:
            root = make_root(directory)
            runner = StrictBootstrapRunner(oversized_key_download=True)

            report = Bootstrapper(
                runner, root=root, effective_uid=lambda: 0
            ).run(apply=True)

            self.assertTrue(report.blocked)
            self.assertIn("APPLY_FAILED", [check.code for check in report.checks])
            self.assertEqual(len(runner.limited_output_calls), 1)
            command, limit = runner.limited_output_calls[0]
            self.assertEqual(command[0], "curl")
            self.assertEqual(limit, MAX_SIGNING_KEY_BYTES)
            self.assertFalse((root / DOCKER_KEY_PATH.lstrip("/")).exists())
            self.assertFalse((root / DOCKER_SOURCE_PATH.lstrip("/")).exists())

    def test_conflicting_docker_packages_are_never_removed(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = StrictBootstrapRunner(conflicts={"docker.io", "runc"})

            report = Bootstrapper(
                runner, root=make_root(directory), effective_uid=lambda: 0
            ).run(apply=True)

            self.assertTrue(report.blocked)
            self.assertIn("DOCKER_PACKAGE_CONFLICT", [check.code for check in report.checks])
            self.assertFalse(any(call[:2] == ("apt-get", "remove") for call in runner.calls))
            self.assertFalse(any(call[:2] == ("apt-get", "install") for call in runner.calls))

    def test_running_containers_require_explicit_restart_approval(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = StrictBootstrapRunner(
                docker=True,
                toolkit=True,
                runtime=False,
                running_containers=2,
            )

            report = Bootstrapper(
                runner, root=make_root(directory), effective_uid=lambda: 0
            ).run(apply=True)

            self.assertTrue(report.blocked)
            self.assertIn("DOCKER_RESTART_BLOCKED", [check.code for check in report.checks])
            self.assertNotIn(("systemctl", "restart", "docker"), runner.calls)
            self.assertNotIn(("systemctl", "enable", "--now", "docker"), runner.calls)
            self.assertFalse(
                any(call[:3] == ("nvidia-ctk", "runtime", "configure") for call in runner.calls)
            )

    def test_full_apply_installs_only_closed_packages_and_becomes_ready(self):
        with tempfile.TemporaryDirectory() as directory:
            root = make_root(directory)
            daemon = root / "etc" / "docker" / "daemon.json"
            daemon.parent.mkdir(parents=True)
            original = b'{"log-driver":"json-file"}'
            daemon.write_bytes(original)
            daemon.chmod(0o600)
            runner = StrictBootstrapRunner()

            report = Bootstrapper(
                runner, root=root, effective_uid=lambda: 0
            ).run(apply=True)

            self.assertFalse(report.blocked, report.to_dict())
            self.assertTrue(report.ready)
            self.assertTrue(report.changed)
            self.assertEqual(
                (root / "var" / "lib" / "dure" / "bootstrap" / "daemon.json.before-nvidia-ctk").read_bytes(),
                original,
            )
            configured = json.loads(daemon.read_text(encoding="utf-8"))
            self.assertEqual(configured["log-driver"], "json-file")
            self.assertEqual(
                configured["runtimes"]["nvidia"]["path"],
                "nvidia-container-runtime",
            )
            self.assertEqual(daemon.stat().st_mode & 0o777, 0o600)
            self.assertTrue((root / DOCKER_SOURCE_PATH.lstrip("/")).is_file())
            self.assertTrue((root / NVIDIA_SOURCE_PATH.lstrip("/")).is_file())
            self.assertEqual(
                (root / NVIDIA_PREFERENCES_PATH.lstrip("/")).read_text(
                    encoding="utf-8"
                ),
                Bootstrapper._nvidia_preferences(),
            )
            joined = "\n".join(" ".join(call) for call in runner.calls)
            self.assertNotIn("nvidia-driver", joined)
            self.assertNotIn("ubuntu-drivers", joined)
            self.assertNotIn("apt-get remove", joined)
            self.assertNotIn("docker run", joined)
            self.assertNotIn("docker pull", joined)
            apt_installs = [call for call in runner.calls if call[:2] == ("apt-get", "install")]
            self.assertTrue(apt_installs)
            self.assertTrue(all("--no-remove" in call for call in apt_installs))
            gpg_calls = [call for call in runner.calls if call[0] == "gpg"]
            self.assertTrue(gpg_calls)
            self.assertTrue(
                all("--no-options" in call and "--homedir" in call for call in gpg_calls)
            )
            self.assertTrue(runner.environments)
            self.assertTrue(
                all(environment == BOOTSTRAP_COMMAND_ENV for environment in runner.environments)
            )

    def test_ubuntu_2404_arm64_uses_matching_official_sources(self):
        with tempfile.TemporaryDirectory() as directory:
            root = make_root(directory, version="24.04")
            runner = StrictBootstrapRunner(architecture="arm64")

            report = Bootstrapper(
                runner, root=root, effective_uid=lambda: 0
            ).run(apply=True)

            self.assertTrue(report.ready, report.to_dict())
            docker_source = (root / DOCKER_SOURCE_PATH.lstrip("/")).read_text(
                encoding="utf-8"
            )
            nvidia_source = (root / NVIDIA_SOURCE_PATH.lstrip("/")).read_text(
                encoding="utf-8"
            )
            self.assertIn("Suites: noble", docker_source)
            self.assertIn("Architectures: arm64", docker_source)
            self.assertIn("stable/deb/arm64", nvidia_source)

    def test_repository_parent_symlink_is_blocked_before_apt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = make_root(directory)
            escape = root / "escape"
            escape.mkdir()
            (root / "etc" / "apt").symlink_to(escape, target_is_directory=True)
            runner = StrictBootstrapRunner()

            report = Bootstrapper(
                runner, root=root, effective_uid=lambda: 0
            ).run(apply=True)

            self.assertTrue(report.blocked)
            self.assertIn("DOCKER_REPOSITORY_UNSAFE", [check.code for check in report.checks])
            self.assertFalse(any(call[0] == "apt-get" for call in runner.calls))

    def test_keyring_with_an_extra_primary_key_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = StrictBootstrapRunner(extra_key_fingerprint="A" * 40)

            report = Bootstrapper(
                runner, root=make_root(directory), effective_uid=lambda: 0
            ).run(apply=True)

            self.assertTrue(report.blocked)
            self.assertTrue(report.changed)
            self.assertIn("APPLY_FAILED", [check.code for check in report.checks])
            self.assertFalse(any(call[:2] == ("apt-get", "remove") for call in runner.calls))

    def test_nvidia_configuration_failure_restores_daemon_json_and_skips_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            root = make_root(directory)
            daemon = root / "etc" / "docker" / "daemon.json"
            daemon.parent.mkdir(parents=True)
            original = b'{"log-driver":"local"}'
            daemon.write_bytes(original)
            runner = StrictBootstrapRunner(
                docker=True,
                toolkit=True,
                runtime=False,
                fail_nvidia_config=True,
            )

            report = Bootstrapper(
                runner, root=root, effective_uid=lambda: 0
            ).run(apply=True)

            self.assertTrue(report.blocked)
            self.assertIn("APPLY_FAILED", [check.code for check in report.checks])
            self.assertEqual(daemon.read_bytes(), original)
            self.assertNotIn(("systemctl", "restart", "docker"), runner.calls)

    def test_nvidia_configuration_cannot_change_unrelated_docker_settings(self):
        with tempfile.TemporaryDirectory() as directory:
            root = make_root(directory)
            daemon = root / DOCKER_DAEMON_CONFIG_PATH.lstrip("/")
            daemon.parent.mkdir(parents=True)
            original = b'{"log-driver":"local","runtimes":{"kata":{"path":"kata-runtime"}}}'
            daemon.write_bytes(original)
            runner = StrictBootstrapRunner(
                docker=True,
                toolkit=True,
                runtime=False,
                mutate_unrelated_daemon_config=True,
            )

            report = Bootstrapper(
                runner, root=root, effective_uid=lambda: 0
            ).run(apply=True)

            self.assertTrue(report.blocked)
            self.assertIn("APPLY_FAILED", [check.code for check in report.checks])
            self.assertEqual(daemon.read_bytes(), original)
            self.assertEqual(runner.restart_calls, 0)

    def test_docker_restart_failure_restores_bytes_mode_and_recovers_service(self):
        with tempfile.TemporaryDirectory() as directory:
            root = make_root(directory)
            daemon = root / "etc" / "docker" / "daemon.json"
            daemon.parent.mkdir(parents=True)
            original = b'{"log-driver":"local"}'
            daemon.write_bytes(original)
            daemon.chmod(0o600)
            runner = StrictBootstrapRunner(
                docker=True,
                toolkit=True,
                runtime=False,
                fail_docker_restart=True,
            )

            report = Bootstrapper(
                runner, root=root, effective_uid=lambda: 0
            ).run(apply=True)

            self.assertTrue(report.blocked)
            self.assertIn("APPLY_FAILED", [check.code for check in report.checks])
            self.assertTrue(report.changed)
            self.assertEqual(daemon.read_bytes(), original)
            self.assertEqual(daemon.stat().st_mode & 0o777, 0o600)
            self.assertEqual(runner.restart_calls, 2)

    def test_runtime_missing_after_restart_restores_config_and_recovers_service(self):
        with tempfile.TemporaryDirectory() as directory:
            root = make_root(directory)
            daemon = root / DOCKER_DAEMON_CONFIG_PATH.lstrip("/")
            daemon.parent.mkdir(parents=True)
            original = b'{"log-driver":"local"}'
            daemon.write_bytes(original)
            daemon.chmod(0o600)
            runner = StrictBootstrapRunner(
                docker=True,
                toolkit=True,
                runtime=False,
                register_runtime_on_restart=False,
            )

            report = Bootstrapper(
                runner, root=root, effective_uid=lambda: 0
            ).run(apply=True)

            self.assertTrue(report.blocked)
            self.assertIn("APPLY_FAILED", [check.code for check in report.checks])
            self.assertEqual(daemon.read_bytes(), original)
            self.assertEqual(daemon.stat().st_mode & 0o777, 0o600)
            self.assertEqual(runner.restart_calls, 2)

    def test_orphan_daemon_backup_is_blocked_before_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = make_root(directory)
            backup = root / DOCKER_DAEMON_BACKUP_PATH.lstrip("/")
            backup.parent.mkdir(parents=True)
            backup.write_text("{}", encoding="utf-8")
            backup.chmod(0o600)
            runner = StrictBootstrapRunner(docker=True, toolkit=True, runtime=False)

            report = Bootstrapper(
                runner, root=root, effective_uid=lambda: 0
            ).run(apply=True)

            self.assertTrue(report.blocked)
            self.assertIn(
                "DOCKER_CONFIG_BACKUP_CONFLICT", [check.code for check in report.checks]
            )
            self.assertFalse(
                any(call[:3] == ("nvidia-ctk", "runtime", "configure") for call in runner.calls)
            )

    def test_running_containers_can_be_restarted_only_after_explicit_approval(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = StrictBootstrapRunner(
                docker=True,
                toolkit=True,
                runtime=False,
                running_containers=2,
            )

            report = Bootstrapper(
                runner, root=make_root(directory), effective_uid=lambda: 0
            ).run(apply=True, allow_docker_restart=True)

            self.assertTrue(report.ready, report.to_dict())
            self.assertEqual(runner.restart_calls, 1)

    def test_new_workload_after_preflight_cancels_restart_and_restores_config(self):
        with tempfile.TemporaryDirectory() as directory:
            root = make_root(directory)
            daemon = root / "etc" / "docker" / "daemon.json"
            daemon.parent.mkdir(parents=True)
            original = b'{"log-level":"warn"}'
            daemon.write_bytes(original)
            runner = StrictBootstrapRunner(
                docker=True,
                toolkit=True,
                runtime=False,
                running_container_counts=[0, 0, 1],
            )

            report = Bootstrapper(
                runner, root=root, effective_uid=lambda: 0
            ).run(apply=True)

            self.assertTrue(report.blocked)
            self.assertIn("APPLY_FAILED", [check.code for check in report.checks])
            self.assertEqual(daemon.read_bytes(), original)
            self.assertEqual(runner.restart_calls, 0)

    def test_invalid_daemon_json_is_blocked_before_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = make_root(directory)
            daemon = root / "etc" / "docker" / "daemon.json"
            daemon.parent.mkdir(parents=True)
            daemon.write_text("not-json", encoding="utf-8")
            runner = StrictBootstrapRunner(docker=True, toolkit=True, runtime=False)

            report = Bootstrapper(
                runner, root=root, effective_uid=lambda: 0
            ).run(apply=True)

            self.assertTrue(report.blocked)
            self.assertIn("DOCKER_CONFIG_INVALID", [check.code for check in report.checks])
            self.assertFalse(
                any(call[:3] == ("nvidia-ctk", "runtime", "configure") for call in runner.calls)
            )

    def test_hard_linked_daemon_config_is_blocked_before_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = make_root(directory)
            source = root / "original.json"
            source.write_text("{}", encoding="utf-8")
            daemon = root / "etc" / "docker" / "daemon.json"
            daemon.parent.mkdir(parents=True)
            os.link(source, daemon)
            runner = StrictBootstrapRunner(docker=True, toolkit=True, runtime=False)

            report = Bootstrapper(
                runner, root=root, effective_uid=lambda: 0
            ).run(apply=True)

            self.assertTrue(report.blocked)
            self.assertIn("DOCKER_CONFIG_UNSAFE", [check.code for check in report.checks])
            self.assertFalse(
                any(call[:3] == ("nvidia-ctk", "runtime", "configure") for call in runner.calls)
            )

    def test_existing_ready_runtime_is_an_idempotent_noop(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = StrictBootstrapRunner(docker=True, toolkit=True, runtime=True)
            root = make_root(directory)
            pin = root / NVIDIA_PREFERENCES_PATH.lstrip("/")
            pin.parent.mkdir(parents=True)
            pin.write_text(Bootstrapper._nvidia_preferences(), encoding="utf-8")

            report = Bootstrapper(
                runner, root=root, effective_uid=lambda: 0
            ).run(apply=True)

            self.assertTrue(report.ready)
            self.assertFalse(report.changed)
            self.assertEqual(report.actions, [])
            self.assertFalse(any(call[0] == "apt-get" for call in runner.calls))
            self.assertNotIn(("systemctl", "restart", "docker"), runner.calls)
            self.assertNotIn(("systemctl", "enable", "--now", "docker"), runner.calls)


class BootstrapCLITests(unittest.TestCase):
    def test_cli_is_preview_by_default_and_json_is_machine_readable(self):
        report = BootstrapReport(
            apply=False,
            allow_docker_restart=False,
            os_id="ubuntu",
            os_version="22.04",
            architecture="amd64",
            checks=[BootstrapCheck("DOCKER_INSTALL_REQUIRED", "ACTION_REQUIRED", "missing")],
        )
        output = io.StringIO()
        error = io.StringIO()
        with patch("dure.bootstrap.Bootstrapper.run", return_value=report) as run, redirect_stdout(
            output
        ), redirect_stderr(error):
            result = main(["bootstrap", "--json"])

        self.assertEqual(result, 0)
        self.assertEqual(error.getvalue(), "")
        self.assertEqual(json.loads(output.getvalue())["apply"], False)
        run.assert_called_once_with(apply=False, allow_docker_restart=False)

    def test_cli_forwards_only_explicit_apply_and_restart_approval(self):
        report = BootstrapReport(
            apply=True,
            allow_docker_restart=True,
            checks=[BootstrapCheck("APPLY_FAILED", "BLOCKED", "failed")],
        )
        with patch("dure.bootstrap.Bootstrapper.run", return_value=report) as run, redirect_stdout(
            io.StringIO()
        ):
            result = main(
                ["bootstrap", "--apply", "--allow-docker-restart"]
            )

        self.assertEqual(result, 1)
        run.assert_called_once_with(apply=True, allow_docker_restart=True)


if __name__ == "__main__":
    unittest.main()

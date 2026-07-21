from __future__ import annotations

import json
import os
import platform
import re
import stat
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from .command import CommandResult, Runner, SubprocessRunner
from .host_setup import (
    HOST_SETUP_LOCK_PATH,
    HostSetupLockError,
    acquire_host_setup_lock,
    release_host_setup_lock,
)


BOOTSTRAP_SCHEMA_VERSION = 1
SUPPORTED_UBUNTU_RELEASES = {
    "22.04": "jammy",
    "24.04": "noble",
}
SUPPORTED_ARCHITECTURES = {"amd64", "arm64"}

DOCKER_KEY_URL = "https://download.docker.com/linux/ubuntu/gpg"
DOCKER_KEY_FINGERPRINT = "9DC858229FC7DD38854AE2D88D81803C0EBFCD88"
NVIDIA_KEY_URL = "https://nvidia.github.io/libnvidia-container/gpgkey"
NVIDIA_KEY_FINGERPRINT = "C95B321B61E88C1809C4F759DDCAE044F796ECB0"
NVIDIA_TOOLKIT_VERSION = "1.19.1-1"
DOCKER_HOST_ARG = "--host=unix:///var/run/docker.sock"
MINIMUM_DOCKER_VERSION = (20, 10, 0)
MAX_SIGNING_KEY_BYTES = 1024 * 1024
BOOTSTRAP_COMMAND_ENV = {
    "PATH": "/usr/sbin:/usr/bin:/sbin:/bin",
    "LANG": "C",
    "LC_ALL": "C",
}

DOCKER_KEY_PATH = "/etc/apt/keyrings/docker.asc"
DOCKER_SOURCE_PATH = "/etc/apt/sources.list.d/docker.sources"
NVIDIA_KEY_PATH = "/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg"
NVIDIA_SOURCE_PATH = "/etc/apt/sources.list.d/nvidia-container-toolkit.list"
NVIDIA_PREFERENCES_PATH = "/etc/apt/preferences.d/dure-nvidia-container-toolkit"
DOCKER_DAEMON_CONFIG_PATH = "/etc/docker/daemon.json"
DOCKER_DAEMON_BACKUP_PATH = "/var/lib/dure/bootstrap/daemon.json.before-nvidia-ctk"
DOCKER_SOCKET_PATH = "/var/run/docker.sock"
BOOTSTRAP_LOCK_PATH = str(HOST_SETUP_LOCK_PATH)
DURE_AGENT_CONFIG_PATH = "/etc/dure/agent.json"

DOCKER_PACKAGES = (
    "docker-ce",
    "docker-ce-cli",
    "containerd.io",
    "docker-buildx-plugin",
    "docker-compose-plugin",
)
DOCKER_RELATED_UNITS = (
    "docker.service",
    "docker.socket",
    "containerd.service",
)
DOCKER_CONFLICTING_PACKAGES = (
    "docker.io",
    "docker-compose",
    "docker-compose-v2",
    "docker-doc",
    "podman-docker",
    "containerd",
    "runc",
)
NVIDIA_PACKAGES = (
    "nvidia-container-toolkit",
    "nvidia-container-toolkit-base",
    "libnvidia-container-tools",
    "libnvidia-container1",
)


@dataclass(frozen=True)
class BootstrapCheck:
    code: str
    status: str
    detail: str

    @property
    def blocking(self) -> bool:
        return self.status == "BLOCKED"

    def to_dict(self) -> dict[str, object]:
        return {
            "code": self.code,
            "status": self.status,
            "blocking": self.blocking,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class BootstrapAction:
    action_id: str
    description: str
    requires_docker_restart: bool = False

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.action_id,
            "description": self.description,
            "requires_docker_restart": self.requires_docker_restart,
        }


@dataclass
class BootstrapReport:
    apply: bool
    allow_docker_restart: bool
    os_id: str | None = None
    os_version: str | None = None
    architecture: str | None = None
    checks: list[BootstrapCheck] = field(default_factory=list)
    actions: list[BootstrapAction] = field(default_factory=list)
    executed_actions: list[str] = field(default_factory=list)
    changed: bool = False
    ready: bool = False

    @property
    def blocked(self) -> bool:
        return any(check.blocking for check in self.checks)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": BOOTSTRAP_SCHEMA_VERSION,
            "apply": self.apply,
            "allow_docker_restart": self.allow_docker_restart,
            "platform": {
                "os_id": self.os_id,
                "os_version": self.os_version,
                "architecture": self.architecture,
            },
            "blocked": self.blocked,
            "ready": self.ready,
            "changed": self.changed,
            "checks": [check.to_dict() for check in self.checks],
            "actions": [action.to_dict() for action in self.actions],
            "executed_actions": list(self.executed_actions),
        }


class BootstrapExecutionError(RuntimeError):
    pass


class Bootstrapper:
    """Prepare one local Ubuntu GPU node for Dure without touching its driver.

    Inspection is read-only. Host changes are reachable only through ``run`` with
    ``apply=True`` and are deliberately not exposed through the central task
    protocol.
    """

    def __init__(
        self,
        runner: Runner | None = None,
        *,
        root: Path | str = Path("/"),
        effective_uid: Callable[[], int] = os.geteuid,
    ) -> None:
        self.runner = runner or SubprocessRunner()
        self.root = Path(root)
        self.effective_uid = effective_uid

    def run(
        self,
        *,
        apply: bool = False,
        allow_docker_restart: bool = False,
    ) -> BootstrapReport:
        report = self.inspect(
            apply=apply,
            allow_docker_restart=allow_docker_restart,
        )
        if not apply or report.blocked:
            return report

        try:
            lock_descriptor = self._acquire_apply_lock()
        except BootstrapExecutionError as exc:
            report.checks.append(BootstrapCheck("BOOTSTRAP_LOCKED", "BLOCKED", str(exc)))
            report.ready = False
            return report

        executed: list[str] = []
        attempted_changes: list[str] = []
        try:
            report = self.inspect(
                apply=True,
                allow_docker_restart=allow_docker_restart,
            )
            if report.blocked:
                return report
            try:
                self._execute(report, executed, attempted_changes)
            except (BootstrapExecutionError, OSError) as exc:
                report.executed_actions = executed
                report.changed = bool(attempted_changes)
                report.checks.append(
                    BootstrapCheck("APPLY_FAILED", "BLOCKED", str(exc))
                )
                report.ready = False
                return report

            verified = self.inspect(
                apply=True,
                allow_docker_restart=allow_docker_restart,
            )
            verified.executed_actions = executed
            verified.changed = bool(executed)
            if not verified.blocked and verified.actions:
                verified.checks.append(
                    BootstrapCheck(
                        "POST_APPLY_INCOMPLETE",
                        "BLOCKED",
                        "Bootstrap actions remain after apply; inspect the listed checks before retrying",
                    )
                )
            verified.ready = not verified.blocked and not verified.actions
            return verified
        finally:
            release_host_setup_lock(lock_descriptor)

    def inspect(
        self,
        *,
        apply: bool = False,
        allow_docker_restart: bool = False,
    ) -> BootstrapReport:
        report = BootstrapReport(
            apply=apply,
            allow_docker_restart=allow_docker_restart,
        )

        os_release = self._read_os_release(report)
        report.os_id = os_release.get("ID")
        report.os_version = os_release.get("VERSION_ID")
        codename = SUPPORTED_UBUNTU_RELEASES.get(report.os_version or "")
        if report.os_id != "ubuntu" or codename is None:
            report.checks.append(
                BootstrapCheck(
                    "UNSUPPORTED_OS",
                    "BLOCKED",
                    "Only Ubuntu 22.04 and 24.04 are supported by this bootstrap release",
                )
            )
        else:
            report.checks.append(
                BootstrapCheck(
                    "OS_SUPPORTED",
                    "PASS",
                    f"Ubuntu {report.os_version} ({codename}) is supported",
                )
            )

        architecture = self._architecture()
        report.architecture = architecture
        if architecture not in SUPPORTED_ARCHITECTURES:
            report.checks.append(
                BootstrapCheck(
                    "UNSUPPORTED_ARCHITECTURE",
                    "BLOCKED",
                    f"Supported architectures are amd64 and arm64; found {architecture or 'unknown'}",
                )
            )
        else:
            report.checks.append(
                BootstrapCheck(
                    "ARCHITECTURE_SUPPORTED",
                    "PASS",
                    f"Architecture {architecture} is supported",
                )
            )

        if apply and self.effective_uid() != 0:
            report.checks.append(
                BootstrapCheck(
                    "ROOT_REQUIRED",
                    "BLOCKED",
                    "Apply requires root; run sudo dure bootstrap --apply",
                )
            )

        self._inspect_pre_join_boundary(report)
        self._inspect_host_prerequisites(report)
        if report.blocked:
            return report

        docker_installed, docker_ready = self._inspect_docker(report)
        toolkit_installed = self._inspect_toolkit(report)
        nvidia_runtime_ready = self._inspect_nvidia_runtime(report, docker_ready)
        nvidia_pin_ready = self._inspect_managed_text_target(
            report,
            path=NVIDIA_PREFERENCES_PATH,
            desired=self._nvidia_preferences(),
            code_prefix="NVIDIA_VERSION_PIN",
        )

        if not docker_installed:
            conflicts = self._installed_conflicting_packages()
            if conflicts:
                report.checks.append(
                    BootstrapCheck(
                        "DOCKER_PACKAGE_CONFLICT",
                        "BLOCKED",
                        "Conflicting container packages require a manual migration: "
                        + ", ".join(conflicts),
                    )
                )
        self._inspect_repository_target(
            report,
            key_path=DOCKER_KEY_PATH,
            source_path=DOCKER_SOURCE_PATH,
            expected_fingerprint=DOCKER_KEY_FINGERPRINT,
            desired_source=self._docker_source(codename or "", architecture or ""),
            code_prefix="DOCKER",
        )

        self._inspect_repository_target(
            report,
            key_path=NVIDIA_KEY_PATH,
            source_path=NVIDIA_SOURCE_PATH,
            expected_fingerprint=NVIDIA_KEY_FINGERPRINT,
            desired_source=self._nvidia_source(architecture or ""),
            code_prefix="NVIDIA",
        )

        configure_runtime = not nvidia_runtime_ready
        if configure_runtime:
            self._inspect_daemon_config(report)
        if docker_ready:
            self._inspect_local_docker_service(report)

        if apply:
            self._inspect_safe_target(
                report,
                self._path(BOOTSTRAP_LOCK_PATH),
                "BOOTSTRAP_LOCK_PATH_UNSAFE",
            )

        if report.blocked:
            return report

        if not docker_installed or not toolkit_installed:
            report.actions.append(
                BootstrapAction(
                    "INSTALL_APT_PREREQUISITES",
                    "Install ca-certificates, curl, and gpg from Ubuntu APT",
                )
            )
        if not docker_installed:
            report.actions.append(
                BootstrapAction(
                    "CONFIGURE_DOCKER_REPOSITORY",
                    "Install Docker's verified signing key and stable Ubuntu APT source",
                )
            )
        if not toolkit_installed:
            report.actions.append(
                BootstrapAction(
                    "CONFIGURE_NVIDIA_REPOSITORY",
                    "Install NVIDIA's verified signing key and stable Toolkit APT source",
                )
            )
        if not nvidia_pin_ready:
            report.actions.append(
                BootstrapAction(
                    "CONFIGURE_NVIDIA_VERSION_PIN",
                    f"Pin the closed NVIDIA Container Toolkit package set to {NVIDIA_TOOLKIT_VERSION}",
                )
            )
        if not docker_installed:
            report.actions.extend(
                [
                    BootstrapAction(
                        "INSTALL_DOCKER",
                        "Install the closed Docker Engine package set",
                    ),
                    BootstrapAction(
                        "START_DOCKER",
                        "Enable and start the newly installed Docker service",
                    ),
                ]
            )
        if not toolkit_installed:
            report.actions.append(
                BootstrapAction(
                    "INSTALL_NVIDIA_TOOLKIT",
                    f"Install the closed NVIDIA Container Toolkit {NVIDIA_TOOLKIT_VERSION} package set",
                )
            )
        if configure_runtime:
            report.actions.extend(
                [
                    BootstrapAction(
                        "CONFIGURE_NVIDIA_RUNTIME",
                        "Back up daemon.json and configure Docker with nvidia-ctk",
                    ),
                    BootstrapAction(
                        "RESTART_DOCKER",
                        "Restart Docker to load the NVIDIA runtime",
                        requires_docker_restart=True,
                    ),
                ]
            )

        if configure_runtime and docker_ready:
            running = self._running_container_count(report)
            if running is None:
                return report
            if running and not allow_docker_restart:
                report.checks.append(
                    BootstrapCheck(
                        "DOCKER_RESTART_BLOCKED",
                        "BLOCKED",
                        f"Docker restart would affect {running} running container(s); review them and add --allow-docker-restart",
                    )
                )
            elif running:
                report.checks.append(
                    BootstrapCheck(
                        "DOCKER_RESTART_APPROVED",
                        "ACTION_REQUIRED",
                        f"Operator explicitly allowed a Docker restart with {running} running container(s)",
                    )
                )

        if not configure_runtime:
            report.checks.append(
                BootstrapCheck(
                    "NVIDIA_RUNTIME_READY",
                    "PASS",
                    "Docker reports the NVIDIA runtime",
                )
            )

        report.ready = not report.blocked and not report.actions
        return report

    def _inspect_pre_join_boundary(self, report: BootstrapReport) -> None:
        config = self._path(DURE_AGENT_CONFIG_PATH)
        if self._is_retired_install_config(config):
            report.checks.append(
                BootstrapCheck(
                    "NODE_UNJOINED",
                    "PASS",
                    "The local registration is retired and retains only its installation identity",
                )
            )
        elif config.exists() or config.is_symlink():
            report.checks.append(
                BootstrapCheck(
                    "NODE_ALREADY_JOINED",
                    "BLOCKED",
                    "Bootstrap apply is pre-join only; an existing /etc/dure/agent.json requires a separate drained maintenance procedure",
                )
            )
        active = self._run_command(
            ["systemctl", "is-active", "--quiet", "dure-agent"],
            timeout=10,
        )
        if active.ok:
            report.checks.append(
                BootstrapCheck(
                    "DURE_AGENT_ACTIVE",
                    "BLOCKED",
                    "Stop and drain the registered node before any manual host maintenance",
                )
            )
        elif active.returncode != 3:
            report.checks.append(
                BootstrapCheck(
                    "DURE_AGENT_STATE_UNKNOWN",
                    "BLOCKED",
                    "Could not prove that dure-agent is inactive",
                )
            )

    def _require_pre_join_boundary(self) -> None:
        config = self._path(DURE_AGENT_CONFIG_PATH)
        if (config.exists() or config.is_symlink()) and not self._is_retired_install_config(
            config
        ):
            raise BootstrapExecutionError(
                "Node joined while bootstrap was running; refusing further host changes"
            )
        active = self._run_command(
            ["systemctl", "is-active", "--quiet", "dure-agent"],
            timeout=10,
        )
        if active.ok or active.returncode != 3:
            raise BootstrapExecutionError(
                "Could not prove dure-agent stayed inactive during bootstrap"
            )

    def _is_retired_install_config(self, config: Path) -> bool:
        if config.is_symlink() or not config.exists():
            return False
        try:
            metadata = config.lstat()
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
                return False
            if self.root == Path("/") and (
                metadata.st_uid != 0 or stat.S_IMODE(metadata.st_mode) & 0o022
            ):
                return False
            value = json.loads(config.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            return False
        return (
            type(value) is dict
            and set(value) == {"install_id"}
            and type(value["install_id"]) is str
            and 0 < len(value["install_id"]) <= 64
        )

    def _inspect_host_prerequisites(self, report: BootstrapReport) -> None:
        required = ("apt-get", "dpkg", "dpkg-query", "systemctl")
        missing = [name for name in required if not self.runner.exists(name)]
        if missing:
            report.checks.append(
                BootstrapCheck(
                    "HOST_TOOL_MISSING",
                    "BLOCKED",
                    "Required Ubuntu host tools are missing: " + ", ".join(missing),
                )
            )

        if not self.runner.exists("nvidia-smi"):
            report.checks.append(
                BootstrapCheck(
                    "NVIDIA_DRIVER_MISSING",
                    "BLOCKED",
                    "nvidia-smi is unavailable; Dure never installs or changes the host driver",
                )
            )
        else:
            driver = self._run_command(
                [
                    "nvidia-smi",
                    "--query-gpu=driver_version",
                    "--format=csv,noheader,nounits",
                ],
                timeout=10,
            )
            if not driver.ok or not driver.stdout.strip():
                report.checks.append(
                    BootstrapCheck(
                        "NVIDIA_DRIVER_UNUSABLE",
                        "BLOCKED",
                        "The installed NVIDIA driver did not report a usable GPU",
                    )
                )
            else:
                versions = sorted(set(driver.stdout.splitlines()))
                report.checks.append(
                    BootstrapCheck(
                        "NVIDIA_DRIVER_READY",
                        "PASS",
                        "Existing NVIDIA driver detected: " + ", ".join(versions),
                    )
                )

        if not self.runner.exists("dure-agent"):
            report.checks.append(
                BootstrapCheck(
                    "DURE_AGENT_MISSING",
                    "BLOCKED",
                    "Install the Dure package first; it contains both this CLI and dure-agent",
                )
            )
        else:
            report.checks.append(
                BootstrapCheck(
                    "DURE_AGENT_INSTALLED",
                    "PASS",
                    "dure-agent is installed; dure join will configure and enable it later",
                )
            )
            service = self._run_command(
                [
                    "systemctl",
                    "show",
                    "--property=LoadState",
                    "--value",
                    "dure-agent.service",
                ],
                timeout=10,
            )
            if not service.ok or service.stdout.strip() != "loaded":
                report.checks.append(
                    BootstrapCheck(
                        "DURE_AGENT_SERVICE_MISSING",
                        "BLOCKED",
                        "Install the packaged dure-agent.service before changing GPU host runtime state",
                    )
                )
            else:
                report.checks.append(
                    BootstrapCheck(
                        "DURE_AGENT_SERVICE_INSTALLED",
                        "PASS",
                        "The packaged dure-agent.service is loaded and remains inactive until join",
                    )
                )

    def _inspect_docker(self, report: BootstrapReport) -> tuple[bool, bool]:
        if not self.runner.exists("docker"):
            installed = sorted(
                package for package in DOCKER_PACKAGES if self._package_installed(package)
            )
            socket = self._path(DOCKER_SOCKET_PATH)
            ambiguous = []
            if installed:
                ambiguous.append("installed packages: " + ", ".join(installed))
            for unit in DOCKER_RELATED_UNITS:
                service = self._run_command(
                    [
                        "systemctl",
                        "show",
                        "--property=LoadState",
                        "--value",
                        unit,
                    ],
                    timeout=10,
                )
                if not service.ok:
                    ambiguous.append(f"{unit} state could not be determined")
                elif service.stdout.strip() != "not-found":
                    ambiguous.append(
                        f"{unit} LoadState={service.stdout.strip() or 'unknown'}"
                    )
            if self.runner.exists("dockerd"):
                ambiguous.append("dockerd is present on PATH")
            if socket.exists() or socket.is_symlink():
                ambiguous.append(f"{DOCKER_SOCKET_PATH} exists")
            if ambiguous:
                report.checks.append(
                    BootstrapCheck(
                        "DOCKER_INSTALLATION_AMBIGUOUS",
                        "BLOCKED",
                        "Docker CLI is unavailable but existing Docker state may own workloads; "
                        + "; ".join(ambiguous),
                    )
                )
                return False, False
            report.checks.append(
                BootstrapCheck(
                    "DOCKER_INSTALL_REQUIRED",
                    "ACTION_REQUIRED",
                    "Docker Engine is not installed",
                )
            )
            return False, False
        result = self._run_command(
            [
                "docker",
                DOCKER_HOST_ARG,
                "version",
                "--format",
                "{{json .}}",
            ],
            timeout=10,
        )
        if not result.ok or not result.stdout.strip():
            report.checks.append(
                BootstrapCheck(
                    "DOCKER_DAEMON_UNAVAILABLE",
                    "BLOCKED",
                    "Docker is installed but its daemon is unavailable; recover it manually before bootstrap",
                )
            )
            return True, False
        try:
            version_data = json.loads(result.stdout)
        except json.JSONDecodeError:
            version_data = None
        client = version_data.get("Client") if isinstance(version_data, dict) else None
        server = version_data.get("Server") if isinstance(version_data, dict) else None
        engine_name = self._docker_engine_name(server)
        version = server.get("Version") if isinstance(server, dict) else None
        client_version = client.get("Version") if isinstance(client, dict) else None
        parsed_version = self._parse_docker_version(version)
        parsed_client_version = self._parse_docker_version(client_version)
        if (
            not isinstance(engine_name, str)
            or not engine_name.startswith("Docker Engine")
            or parsed_version is None
            or parsed_client_version is None
        ):
            report.checks.append(
                BootstrapCheck(
                    "DOCKER_ENGINE_UNSUPPORTED",
                    "BLOCKED",
                    "The local socket did not identify a supported Docker Engine server",
                )
            )
            return True, False
        if (
            parsed_version < MINIMUM_DOCKER_VERSION
            or parsed_client_version < MINIMUM_DOCKER_VERSION
        ):
            report.checks.append(
                BootstrapCheck(
                    "DOCKER_VERSION_UNSUPPORTED",
                    "BLOCKED",
                    "Dure requires Docker CLI and Engine 20.10 or newer for the closed GPU runtime contract",
                )
            )
            return True, False
        report.checks.append(
            BootstrapCheck(
                "DOCKER_READY",
                "PASS",
                f"{engine_name} server {version} with Docker CLI {client_version} is ready on /var/run/docker.sock",
            )
        )
        return True, True

    @staticmethod
    def _docker_engine_name(server: object) -> str | None:
        if not isinstance(server, dict):
            return None
        platform_data = server.get("Platform")
        if not isinstance(platform_data, dict):
            return None
        platform_name = platform_data.get("Name")
        if not isinstance(platform_name, str):
            return None
        if platform_name.strip():
            return platform_name if platform_name.startswith("Docker Engine") else None

        version = server.get("Version")
        server_os = server.get("Os")
        server_arch = server.get("Arch")
        components = server.get("Components")
        if (
            not isinstance(version, str)
            or not version
            or server_os != "linux"
            or server_arch not in SUPPORTED_ARCHITECTURES
            or not isinstance(components, list)
        ):
            return None
        engines = [
            component
            for component in components
            if isinstance(component, dict) and component.get("Name") == "Engine"
        ]
        if len(engines) != 1 or engines[0].get("Version") != version:
            return None
        details = engines[0].get("Details")
        if not isinstance(details, dict):
            return None
        if details.get("Os", server_os) != server_os:
            return None
        if details.get("Arch", server_arch) != server_arch:
            return None
        return "Docker Engine"

    @staticmethod
    def _parse_docker_version(value: object) -> tuple[int, int, int] | None:
        if not isinstance(value, str):
            return None
        match = re.match(r"^(\d+)\.(\d+)(?:\.(\d+))?", value.strip())
        if match is None:
            return None
        return tuple(int(part or 0) for part in match.groups())

    def _inspect_toolkit(self, report: BootstrapReport) -> bool:
        package_states = {
            package: self._package_installed(package) for package in NVIDIA_PACKAGES
        }
        installed_packages = sorted(
            package for package, installed in package_states.items() if installed
        )
        ctk_exists = self.runner.exists("nvidia-ctk")
        runtime_exists = self.runner.exists("nvidia-container-runtime")
        if not installed_packages and not ctk_exists and not runtime_exists:
            report.checks.append(
                BootstrapCheck(
                    "NVIDIA_TOOLKIT_INSTALL_REQUIRED",
                    "ACTION_REQUIRED",
                    "NVIDIA Container Toolkit is not installed",
                )
            )
            return False
        if (
            len(installed_packages) != len(NVIDIA_PACKAGES)
            or not ctk_exists
            or not runtime_exists
        ):
            detail = ", ".join(installed_packages) if installed_packages else "none"
            report.checks.append(
                BootstrapCheck(
                    "NVIDIA_TOOLKIT_PARTIAL",
                    "BLOCKED",
                    "Refusing a partial NVIDIA Container Toolkit installation; "
                    f"installed packages: {detail}",
                )
            )
            return False
        installed_versions = {
            package: self._package_version(package) for package in NVIDIA_PACKAGES
        }
        if any(
            version != NVIDIA_TOOLKIT_VERSION
            for version in installed_versions.values()
        ):
            versions = ", ".join(
                f"{package}={version or 'unknown'}"
                for package, version in sorted(installed_versions.items())
            )
            report.checks.append(
                BootstrapCheck(
                    "NVIDIA_TOOLKIT_VERSION_UNSUPPORTED",
                    "BLOCKED",
                    f"Expected the verified Toolkit package set {NVIDIA_TOOLKIT_VERSION}; found {versions}",
                )
            )
            return False
        result = self._run_command(["nvidia-ctk", "--version"], timeout=10)
        if not result.ok:
            report.checks.append(
                BootstrapCheck(
                    "NVIDIA_TOOLKIT_UNUSABLE",
                    "BLOCKED",
                    "nvidia-ctk is installed but cannot report its version",
                )
            )
            return True
        report.checks.append(
            BootstrapCheck(
                "NVIDIA_TOOLKIT_READY",
                "PASS",
                result.stdout.splitlines()[-1] if result.stdout else "nvidia-ctk is available",
            )
        )
        return True

    def _inspect_nvidia_runtime(
        self, report: BootstrapReport, docker_ready: bool
    ) -> bool:
        if not docker_ready:
            return False
        result = self._run_command(
            [
                "docker",
                DOCKER_HOST_ARG,
                "info",
                "--format",
                "{{json .Runtimes}}",
            ],
            timeout=10,
        )
        if not result.ok:
            report.checks.append(
                BootstrapCheck(
                    "DOCKER_RUNTIME_INSPECTION_FAILED",
                    "BLOCKED",
                    "Docker could not report its configured runtimes",
                )
            )
            return False
        try:
            runtimes = json.loads(result.stdout)
        except json.JSONDecodeError:
            report.checks.append(
                BootstrapCheck(
                    "DOCKER_RUNTIME_INSPECTION_INVALID",
                    "BLOCKED",
                    "Docker returned invalid runtime JSON",
                )
            )
            return False
        if not isinstance(runtimes, dict):
            report.checks.append(
                BootstrapCheck(
                    "DOCKER_RUNTIME_INSPECTION_INVALID",
                    "BLOCKED",
                    "Docker runtime data must be a JSON object",
                )
            )
            return False
        if "nvidia" not in runtimes:
            return False
        runtime = runtimes["nvidia"]
        if not self._nvidia_runtime_entry_valid(runtime, allow_status=True):
            report.checks.append(
                BootstrapCheck(
                    "NVIDIA_RUNTIME_INVALID",
                    "BLOCKED",
                    "The Docker nvidia runtime does not use nvidia-container-runtime",
                )
            )
            return False
        return True

    @staticmethod
    def _nvidia_runtime_entry_valid(
        runtime: object,
        *,
        allow_status: bool = False,
    ) -> bool:
        if not isinstance(runtime, dict):
            return False
        allowed_fields = {"path", "args", "runtimeArgs"}
        if allow_status:
            allowed_fields.add("status")
        if not set(runtime) <= allowed_fields:
            return False
        if runtime.get("path") not in {
            "nvidia-container-runtime",
            "/usr/bin/nvidia-container-runtime",
        }:
            return False
        return all(
            field not in runtime or runtime[field] in (None, [])
            for field in ("args", "runtimeArgs")
        )

    def _validate_generated_daemon_config(
        self,
        config: Path,
        original: dict[str, object],
    ) -> None:
        self._require_safe_target(config)
        try:
            value = json.loads(config.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise BootstrapExecutionError(
                "nvidia-ctk did not create a readable Docker JSON configuration"
            ) from exc
        runtimes = value.get("runtimes") if isinstance(value, dict) else None
        runtime = runtimes.get("nvidia") if isinstance(runtimes, dict) else None
        if not self._nvidia_runtime_entry_valid(runtime):
            raise BootstrapExecutionError(
                "nvidia-ctk did not create the closed NVIDIA Docker runtime entry"
            )
        original_runtimes = original.get("runtimes", {})
        if not isinstance(original_runtimes, dict):
            raise BootstrapExecutionError(
                "The original Docker runtimes configuration is not an object"
            )
        generated_without_runtimes = {
            key: item for key, item in value.items() if key != "runtimes"
        }
        original_without_runtimes = {
            key: item for key, item in original.items() if key != "runtimes"
        }
        generated_other_runtimes = {
            key: item for key, item in runtimes.items() if key != "nvidia"
        }
        original_other_runtimes = {
            key: item for key, item in original_runtimes.items() if key != "nvidia"
        }
        if (
            generated_without_runtimes != original_without_runtimes
            or generated_other_runtimes != original_other_runtimes
        ):
            raise BootstrapExecutionError(
                "nvidia-ctk changed Docker settings outside the closed NVIDIA runtime entry"
            )

    def _nvidia_runtime_ready_now(self) -> bool:
        result = self._run_command(
            [
                "docker",
                DOCKER_HOST_ARG,
                "info",
                "--format",
                "{{json .Runtimes}}",
            ],
            timeout=10,
        )
        if not result.ok:
            return False
        try:
            runtimes = json.loads(result.stdout)
        except json.JSONDecodeError:
            return False
        return (
            isinstance(runtimes, dict)
            and "nvidia" in runtimes
            and self._nvidia_runtime_entry_valid(
                runtimes["nvidia"], allow_status=True
            )
        )

    def _inspect_local_docker_service(self, report: BootstrapReport) -> None:
        if not self._local_docker_service_ready():
            report.checks.append(
                BootstrapCheck(
                    "DOCKER_SERVICE_UNSUPPORTED",
                    "BLOCKED",
                    "Bootstrap requires an active local rootful /var/run/docker.sock docker.service and a boot-persistent enabled docker.service or docker.socket",
                )
            )
            return
        report.checks.append(
            BootstrapCheck(
                "DOCKER_SERVICE_SUPPORTED",
                "PASS",
                "Local rootful systemd Docker service is available",
            )
        )

    def _local_docker_service_ready(self) -> bool:
        service = self._run_command(
            [
                "systemctl",
                "show",
                "--property=LoadState",
                "--value",
                "docker.service",
            ],
            timeout=10,
        )
        active = self._run_command(
            [
                "systemctl",
                "show",
                "--property=ActiveState",
                "--value",
                "docker.service",
            ],
            timeout=10,
        )
        if not (
            service.ok
            and service.stdout == "loaded"
            and active.ok
            and active.stdout == "active"
        ):
            return False
        enabled = self._run_command(
            [
                "systemctl",
                "show",
                "--property=UnitFileState",
                "--value",
                "docker.service",
            ],
            timeout=10,
        )
        if enabled.ok and enabled.stdout == "enabled":
            return True
        socket_loaded = self._run_command(
            [
                "systemctl",
                "show",
                "--property=LoadState",
                "--value",
                "docker.socket",
            ],
            timeout=10,
        )
        socket_enabled = self._run_command(
            [
                "systemctl",
                "show",
                "--property=UnitFileState",
                "--value",
                "docker.socket",
            ],
            timeout=10,
        )
        return (
            socket_loaded.ok
            and socket_loaded.stdout == "loaded"
            and socket_enabled.ok
            and socket_enabled.stdout == "enabled"
        )

    def _running_container_count(self, report: BootstrapReport) -> int | None:
        result = self._run_command(
            ["docker", DOCKER_HOST_ARG, "ps", "--quiet", "--no-trunc"],
            timeout=10,
        )
        if not result.ok:
            report.checks.append(
                BootstrapCheck(
                    "DOCKER_WORKLOAD_UNKNOWN",
                    "BLOCKED",
                    "Could not determine whether a Docker restart would affect running containers",
                )
            )
            return None
        return len([line for line in result.stdout.splitlines() if line.strip()])

    def _installed_conflicting_packages(self) -> list[str]:
        return [
            package
            for package in DOCKER_CONFLICTING_PACKAGES
            if self._package_installed(package)
        ]

    def _package_installed(self, package: str) -> bool:
        result = self._run_command(
            ["dpkg-query", "-W", "-f=${Status}", package],
            timeout=5,
        )
        return result.ok and result.stdout.strip() == "install ok installed"

    def _package_version(self, package: str) -> str | None:
        result = self._run_command(
            ["dpkg-query", "-W", "-f=${Version}", package],
            timeout=5,
        )
        return result.stdout.strip() if result.ok and result.stdout.strip() else None

    def _inspect_repository_target(
        self,
        report: BootstrapReport,
        *,
        key_path: str,
        source_path: str,
        expected_fingerprint: str,
        desired_source: str,
        code_prefix: str,
    ) -> None:
        key = self._path(key_path)
        source = self._path(source_path)
        check_count = len(report.checks)
        for target in (key, source):
            self._inspect_safe_target(
                report,
                target,
                f"{code_prefix}_REPOSITORY_UNSAFE",
            )
        self._inspect_apt_key_ancestors(report, key, code_prefix)
        if any(check.blocking for check in report.checks[check_count:]):
            return
        if source.exists():
            try:
                current = source.read_text(encoding="utf-8")
            except (OSError, UnicodeError):
                current = ""
            if current != desired_source:
                report.checks.append(
                    BootstrapCheck(
                        f"{code_prefix}_REPOSITORY_CONFLICT",
                        "BLOCKED",
                        f"Existing {source_path} differs from Dure's closed stable source",
                    )
                )
        if source.exists() and not key.exists():
            report.checks.append(
                BootstrapCheck(
                    f"{code_prefix}_REPOSITORY_PARTIAL",
                    "BLOCKED",
                    f"Existing {source_path} refers to a missing signing key; repair or remove it manually before bootstrap",
                )
            )
        if key.exists() and not self.runner.exists("gpg"):
            report.checks.append(
                BootstrapCheck(
                    f"{code_prefix}_KEY_UNVERIFIED",
                    "BLOCKED",
                    f"Cannot verify existing {key_path} because gpg is unavailable",
                )
            )
        if key.exists():
            try:
                key_metadata = key.stat()
                key_mode = stat.S_IMODE(key_metadata.st_mode)
            except OSError:
                key_metadata = None
                key_mode = -1
            if key_mode not in {0o444, 0o644}:
                report.checks.append(
                    BootstrapCheck(
                        f"{code_prefix}_KEY_PERMISSIONS",
                        "BLOCKED",
                        f"Existing {key_path} must be root-owned and readable by APT with mode 0444 or 0644",
                    )
                )
            if key_metadata is None or not 0 < key_metadata.st_size <= MAX_SIGNING_KEY_BYTES:
                report.checks.append(
                    BootstrapCheck(
                        f"{code_prefix}_KEY_SIZE",
                        "BLOCKED",
                        f"Existing {key_path} must be a non-empty signing key no larger than {MAX_SIGNING_KEY_BYTES} bytes",
                    )
                )
        if key.exists() and self.runner.exists("gpg"):
            result = self._gpg_show_keys(key)
            if not result.ok or self._primary_fingerprints(result) != [
                expected_fingerprint
            ]:
                report.checks.append(
                    BootstrapCheck(
                        f"{code_prefix}_KEY_CONFLICT",
                        "BLOCKED",
                        f"Existing {key_path} does not contain the expected signing key",
                    )
                )

    def _inspect_managed_text_target(
        self,
        report: BootstrapReport,
        *,
        path: str,
        desired: str,
        code_prefix: str,
    ) -> bool:
        target = self._path(path)
        check_count = len(report.checks)
        self._inspect_safe_target(report, target, f"{code_prefix}_UNSAFE")
        if any(check.blocking for check in report.checks[check_count:]):
            return False
        if not target.exists():
            return False
        try:
            current = target.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            current = ""
        if current != desired:
            report.checks.append(
                BootstrapCheck(
                    f"{code_prefix}_CONFLICT",
                    "BLOCKED",
                    f"Existing {path} differs from Dure's closed version pin",
                )
            )
            return False
        return True

    def _inspect_daemon_config(self, report: BootstrapReport) -> None:
        path = self._path(DOCKER_DAEMON_CONFIG_PATH)
        backup = self._path(DOCKER_DAEMON_BACKUP_PATH)
        for target in (path, backup):
            self._inspect_safe_target(
                report,
                target,
                "DOCKER_CONFIG_UNSAFE",
            )
        if report.blocked:
            return
        if path.is_symlink() or backup.is_symlink():
            report.checks.append(
                BootstrapCheck(
                    "DOCKER_CONFIG_UNSAFE",
                    "BLOCKED",
                    "Docker daemon config or Dure backup path is a symbolic link",
                )
            )
            return
        if backup.exists():
            try:
                backup_mode = stat.S_IMODE(backup.stat().st_mode)
            except OSError:
                backup_mode = -1
            if backup_mode != 0o600:
                report.checks.append(
                    BootstrapCheck(
                        "DOCKER_CONFIG_BACKUP_PERMISSIONS",
                        "BLOCKED",
                        "Existing Dure daemon.json backup must have mode 0600",
                    )
                )
            if not path.exists():
                report.checks.append(
                    BootstrapCheck(
                        "DOCKER_CONFIG_BACKUP_CONFLICT",
                        "BLOCKED",
                        "A Dure daemon.json backup exists without a current daemon.json; preserve and review it manually",
                    )
                )
                return
        if path.exists():
            try:
                value = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError):
                report.checks.append(
                    BootstrapCheck(
                        "DOCKER_CONFIG_INVALID",
                        "BLOCKED",
                        "Existing /etc/docker/daemon.json is not a readable JSON object",
                    )
                )
                return
            if not isinstance(value, dict):
                report.checks.append(
                    BootstrapCheck(
                        "DOCKER_CONFIG_INVALID",
                        "BLOCKED",
                        "Existing /etc/docker/daemon.json must contain a JSON object",
                    )
                )
            else:
                runtimes = value.get("runtimes")
                if runtimes is not None and not isinstance(runtimes, dict):
                    report.checks.append(
                        BootstrapCheck(
                            "DOCKER_CONFIG_INVALID",
                            "BLOCKED",
                            "Existing Docker runtimes configuration must be a JSON object",
                        )
                    )
                elif (
                    isinstance(runtimes, dict)
                    and "nvidia" in runtimes
                    and not self._nvidia_runtime_entry_valid(runtimes["nvidia"])
                ):
                    report.checks.append(
                        BootstrapCheck(
                            "DOCKER_CONFIG_NVIDIA_RUNTIME_CONFLICT",
                            "BLOCKED",
                            "Existing daemon.json contains an unsafe NVIDIA runtime entry",
                        )
                    )
            if backup.exists():
                try:
                    backup_bytes = backup.read_bytes()
                    current_bytes = path.read_bytes()
                except OSError:
                    backup_bytes = b""
                    current_bytes = b"different"
                if backup_bytes != current_bytes:
                    report.checks.append(
                        BootstrapCheck(
                            "DOCKER_CONFIG_BACKUP_CONFLICT",
                            "BLOCKED",
                            "A different Dure daemon.json backup already exists; preserve and review it manually",
                        )
                    )

    def _read_os_release(self, report: BootstrapReport) -> dict[str, str]:
        path = self._path("/etc/os-release")
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            report.checks.append(
                BootstrapCheck(
                    "OS_RELEASE_UNAVAILABLE",
                    "BLOCKED",
                    "Cannot read /etc/os-release",
                )
            )
            return {}
        values: dict[str, str] = {}
        for line in text.splitlines():
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if value.startswith(('"', "'")) and value.endswith(value[:1]):
                value = value[1:-1]
            values[key] = value
        return values

    def _architecture(self) -> str | None:
        if self.runner.exists("dpkg"):
            result = self._run_command(["dpkg", "--print-architecture"], timeout=5)
            if result.ok and result.stdout.strip():
                return result.stdout.strip()
            return None
        aliases = {"x86_64": "amd64", "aarch64": "arm64"}
        return aliases.get(platform.machine().lower())

    def _execute(
        self,
        report: BootstrapReport,
        executed: list[str],
        attempted_changes: list[str],
    ) -> None:
        self._require_pre_join_boundary()
        action_ids = {action.action_id for action in report.actions}
        if "INSTALL_APT_PREREQUISITES" in action_ids:
            attempted_changes.append("INSTALL_APT_PREREQUISITES")
            self._run_required(["apt-get", "update"], timeout=300)
            self._run_required(
                [
                    "apt-get",
                    "install",
                    "-y",
                    "--no-install-recommends",
                    "--no-remove",
                    "ca-certificates",
                    "curl",
                    "gpg",
                ],
                timeout=600,
            )
            executed.append("INSTALL_APT_PREREQUISITES")

        repositories_changed = False
        if "CONFIGURE_DOCKER_REPOSITORY" in action_ids:
            attempted_changes.append("CONFIGURE_DOCKER_REPOSITORY")
            self._install_repository(
                key_url=DOCKER_KEY_URL,
                key_path=DOCKER_KEY_PATH,
                fingerprint=DOCKER_KEY_FINGERPRINT,
                source_path=DOCKER_SOURCE_PATH,
                source_text=self._docker_source(
                    SUPPORTED_UBUNTU_RELEASES[report.os_version or ""],
                    report.architecture or "",
                ),
                dearmor=False,
            )
            executed.append("CONFIGURE_DOCKER_REPOSITORY")
            repositories_changed = True

        if "CONFIGURE_NVIDIA_REPOSITORY" in action_ids:
            attempted_changes.append("CONFIGURE_NVIDIA_REPOSITORY")
            self._install_repository(
                key_url=NVIDIA_KEY_URL,
                key_path=NVIDIA_KEY_PATH,
                fingerprint=NVIDIA_KEY_FINGERPRINT,
                source_path=NVIDIA_SOURCE_PATH,
                source_text=self._nvidia_source(report.architecture or ""),
                dearmor=True,
            )
            executed.append("CONFIGURE_NVIDIA_REPOSITORY")
            repositories_changed = True

        if "CONFIGURE_NVIDIA_VERSION_PIN" in action_ids:
            attempted_changes.append("CONFIGURE_NVIDIA_VERSION_PIN")
            self._atomic_write(
                self._path(NVIDIA_PREFERENCES_PATH),
                self._nvidia_preferences().encode("utf-8"),
                0o644,
                allow_existing_same=True,
            )
            executed.append("CONFIGURE_NVIDIA_VERSION_PIN")

        if repositories_changed:
            self._run_required(["apt-get", "update"], timeout=300)

        if "INSTALL_DOCKER" in action_ids:
            attempted_changes.append("INSTALL_DOCKER")
            self._run_required(
                [
                    "apt-get",
                    "install",
                    "-y",
                    "--no-install-recommends",
                    "--no-remove",
                    *DOCKER_PACKAGES,
                ],
                timeout=900,
            )
            executed.append("INSTALL_DOCKER")

        if "START_DOCKER" in action_ids:
            attempted_changes.append("START_DOCKER")
            self._run_required(
                ["systemctl", "enable", "--now", "docker"], timeout=120
            )
            executed.append("START_DOCKER")

        if "INSTALL_NVIDIA_TOOLKIT" in action_ids:
            attempted_changes.append("INSTALL_NVIDIA_TOOLKIT")
            packages = [
                f"{package}={NVIDIA_TOOLKIT_VERSION}" for package in NVIDIA_PACKAGES
            ]
            self._run_required(
                [
                    "apt-get",
                    "install",
                    "-y",
                    "--no-install-recommends",
                    "--no-remove",
                    *packages,
                ],
                timeout=600,
            )
            executed.append("INSTALL_NVIDIA_TOOLKIT")

        if "CONFIGURE_NVIDIA_RUNTIME" in action_ids:
            self._require_pre_join_boundary()
            config = self._path(DOCKER_DAEMON_CONFIG_PATH)
            backup = self._path(DOCKER_DAEMON_BACKUP_PATH)
            if not self._local_docker_service_ready():
                raise BootstrapExecutionError(
                    "Docker service or local socket changed after preflight"
                )
            self._ensure_safe_parent(config)
            self._require_safe_target(config)
            try:
                config_stat = config.stat() if config.exists() else None
                original = config.read_bytes() if config_stat is not None else None
                original_mode = (
                    stat.S_IMODE(config_stat.st_mode) if config_stat is not None else 0o644
                )
                original_owner = (
                    (config_stat.st_uid, config_stat.st_gid)
                    if config_stat is not None
                    else None
                )
                original_config = (
                    json.loads(original.decode("utf-8"))
                    if original is not None
                    else {}
                )
                if not isinstance(original_config, dict):
                    raise BootstrapExecutionError(
                        "Existing Docker daemon config changed after preflight"
                    )
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                raise BootstrapExecutionError(
                    f"Cannot preserve existing {config}: {exc}"
                ) from exc
            attempted_changes.append("CONFIGURE_NVIDIA_RUNTIME")
            if original is not None:
                self._atomic_write(backup, original, 0o600, allow_existing_same=True)
            try:
                self._run_required(
                    [
                        "nvidia-ctk",
                        "runtime",
                        "configure",
                        "--runtime=docker",
                        f"--config={config}",
                    ],
                    timeout=60,
                )
                self._validate_generated_daemon_config(config, original_config)
                generated = config.read_bytes()
                self._atomic_write(
                    config,
                    generated,
                    original_mode,
                    owner=original_owner,
                )
            except (BootstrapExecutionError, OSError) as exc:
                self._restore_daemon_config(
                    config, original, original_mode, original_owner
                )
                if isinstance(exc, BootstrapExecutionError):
                    raise
                raise BootstrapExecutionError(
                    f"Cannot finalize NVIDIA Docker runtime config: {exc}"
                ) from exc
            executed.append("CONFIGURE_NVIDIA_RUNTIME")
            if "RESTART_DOCKER" in action_ids:
                restart_attempted = False
                try:
                    running_result = self._run_required(
                        [
                            "docker",
                            DOCKER_HOST_ARG,
                            "ps",
                            "--quiet",
                            "--no-trunc",
                        ],
                        timeout=10,
                    )
                    running = len(
                        [
                            line
                            for line in running_result.stdout.splitlines()
                            if line.strip()
                        ]
                    )
                    if running and not report.allow_docker_restart:
                        raise BootstrapExecutionError(
                            f"Docker workload changed after preflight; {running} running container(s) now require --allow-docker-restart"
                        )
                    self._require_pre_join_boundary()
                    restart_attempted = True
                    self._run_required(
                        ["systemctl", "restart", "docker"], timeout=120
                    )
                    executed.append("RESTART_DOCKER")
                    if not self._nvidia_runtime_ready_now():
                        raise BootstrapExecutionError(
                            "Docker restarted but did not report the closed NVIDIA runtime"
                        )
                except BootstrapExecutionError as restart_error:
                    self._restore_daemon_config(
                        config, original, original_mode, original_owner
                    )
                    if not restart_attempted:
                        raise
                    recovery = self._run_command(
                        ["systemctl", "restart", "docker"], timeout=120
                    )
                    if recovery.ok:
                        raise BootstrapExecutionError(
                            f"{restart_error}; restored the previous daemon.json and restarted Docker"
                        ) from restart_error
                    detail = recovery.stderr or recovery.stdout or f"exit {recovery.returncode}"
                    raise BootstrapExecutionError(
                        f"{restart_error}; restored the previous daemon.json, but the recovery restart failed: {detail[:500]}"
                    ) from restart_error

    def _install_repository(
        self,
        *,
        key_url: str,
        key_path: str,
        fingerprint: str,
        source_path: str,
        source_text: str,
        dearmor: bool,
    ) -> None:
        destination_key = self._path(key_path)
        destination_source = self._path(source_path)
        self._require_safe_target(destination_key)
        self._require_safe_target(destination_source)
        if destination_source.exists():
            try:
                if destination_source.read_text(encoding="utf-8") != source_text:
                    raise BootstrapExecutionError(
                        f"Refusing to overwrite changed repository source {source_path}"
                    )
            except (OSError, UnicodeError) as exc:
                raise BootstrapExecutionError(
                    f"Refusing unreadable repository source {source_path}"
                ) from exc
        if destination_key.exists():
            self._require_apt_key_access(destination_key)
            result = self._require_result(self._gpg_show_keys(destination_key))
            if self._primary_fingerprints(result) != [fingerprint]:
                raise BootstrapExecutionError(
                    f"Refusing unexpected signing key at {key_path}"
                )
        else:
            with tempfile.TemporaryDirectory(prefix="dure-bootstrap-") as directory:
                downloaded = Path(directory) / "key.asc"
                download = self._run_limited_command(
                    [
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
                        "--output",
                        "-",
                        key_url,
                    ],
                    timeout=60,
                    max_output_bytes=MAX_SIGNING_KEY_BYTES,
                )
                self._require_result(download)
                try:
                    downloaded_bytes = download.stdout.encode("ascii")
                except UnicodeEncodeError as exc:
                    raise BootstrapExecutionError(
                        "Downloaded signing key was not ASCII-armored data"
                    ) from exc
                if not 0 < len(downloaded_bytes) <= MAX_SIGNING_KEY_BYTES:
                    raise BootstrapExecutionError(
                        "Downloaded signing key has an invalid size"
                    )
                try:
                    downloaded.write_bytes(downloaded_bytes)
                except OSError as exc:
                    raise BootstrapExecutionError(
                        "Downloaded signing key was not created"
                    ) from exc
                result = self._require_result(self._gpg_show_keys(downloaded))
                if self._primary_fingerprints(result) != [fingerprint]:
                    raise BootstrapExecutionError(
                        f"Downloaded signing key from {key_url} has an unexpected fingerprint"
                    )
                material = downloaded
                if dearmor:
                    material = Path(directory) / "key.gpg"
                    gpg_home = Path(directory) / "gnupg"
                    gpg_home.mkdir(mode=0o700)
                    self._run_required(
                        [
                            "gpg",
                            "--batch",
                            "--no-options",
                            "--homedir",
                            str(gpg_home),
                            "--yes",
                            "--dearmor",
                            "--output",
                            str(material),
                            str(downloaded),
                        ],
                        timeout=10,
                    )
                try:
                    key_bytes = material.read_bytes()
                except OSError as exc:
                    raise BootstrapExecutionError(
                        "Signing key material was not created"
                    ) from exc
                self._atomic_write(destination_key, key_bytes, 0o644)
                self._require_apt_key_access(destination_key)
        self._atomic_write(
            destination_source,
            source_text.encode("utf-8"),
            0o644,
            allow_existing_same=True,
        )

    def _inspect_apt_key_ancestors(
        self,
        report: BootstrapReport,
        path: Path,
        code_prefix: str,
    ) -> None:
        try:
            relative = path.parent.relative_to(self.root)
        except ValueError:
            return
        cursor = self.root
        for part in relative.parts:
            cursor = cursor / part
            if not cursor.exists():
                continue
            try:
                mode = stat.S_IMODE(cursor.stat().st_mode)
            except OSError:
                mode = 0
            if not mode & stat.S_IXOTH:
                report.checks.append(
                    BootstrapCheck(
                        f"{code_prefix}_KEY_PATH_PERMISSIONS",
                        "BLOCKED",
                        f"APT's unprivileged key reader cannot traverse {cursor}; require other-execute permission",
                    )
                )
                return

    def _require_apt_key_access(self, path: Path) -> None:
        try:
            relative = path.parent.relative_to(self.root)
        except ValueError as exc:
            raise BootstrapExecutionError(f"Signing key escapes bootstrap root: {path}") from exc
        cursor = self.root
        for part in relative.parts:
            cursor = cursor / part
            try:
                mode = stat.S_IMODE(cursor.stat().st_mode)
            except OSError as exc:
                raise BootstrapExecutionError(
                    f"Cannot inspect signing key parent {cursor}"
                ) from exc
            if not mode & stat.S_IXOTH:
                raise BootstrapExecutionError(
                    f"APT's unprivileged key reader cannot traverse {cursor}"
                )
        try:
            metadata = path.stat()
            mode = stat.S_IMODE(metadata.st_mode)
        except OSError as exc:
            raise BootstrapExecutionError(f"Cannot inspect signing key {path}") from exc
        if mode not in {0o444, 0o644}:
            raise BootstrapExecutionError(
                f"Signing key must be readable by APT with mode 0444 or 0644: {path}"
            )
        if not 0 < metadata.st_size <= MAX_SIGNING_KEY_BYTES:
            raise BootstrapExecutionError(
                f"Signing key must be non-empty and no larger than {MAX_SIGNING_KEY_BYTES} bytes: {path}"
            )

    def _restore_daemon_config(
        self,
        config: Path,
        original: bytes | None,
        original_mode: int,
        original_owner: tuple[int, int] | None,
    ) -> None:
        try:
            if original is None:
                if config.is_symlink():
                    raise BootstrapExecutionError(
                        f"Refusing symbolic link while restoring {config}"
                    )
                config.unlink(missing_ok=True)
                return
            self._atomic_write(
                config,
                original,
                original_mode,
                owner=original_owner,
            )
        except OSError as exc:
            raise BootstrapExecutionError(
                f"Could not restore the previous Docker daemon config: {exc}"
            ) from exc

    def _run_required(
        self, argv: list[str], *, timeout: float
    ) -> CommandResult:
        result = self._run_command(argv, timeout=timeout)
        return self._require_result(result)

    def _run_command(
        self, argv: list[str], *, timeout: float
    ) -> CommandResult:
        """Run bootstrap commands with a closed, locale-stable environment."""

        return self.runner.run(argv, timeout=timeout, env=BOOTSTRAP_COMMAND_ENV)

    def _run_limited_command(
        self,
        argv: list[str],
        *,
        timeout: float,
        max_output_bytes: int,
    ) -> CommandResult:
        """Run a command only when its output can be bounded during execution."""

        limited = getattr(self.runner, "run_limited_output", None)
        if not callable(limited):
            raise BootstrapExecutionError(
                "Bootstrap runner cannot enforce the signing key download limit"
            )
        return limited(
            argv,
            timeout=timeout,
            max_output_bytes=max_output_bytes,
            env=BOOTSTRAP_COMMAND_ENV,
        )

    def _require_result(self, result: CommandResult) -> CommandResult:
        if not result.ok:
            detail = result.stderr or result.stdout or f"exit {result.returncode}"
            if len(detail) > 500:
                detail = detail[:500] + "..."
            executable = result.argv[0] if result.argv else "command"
            raise BootstrapExecutionError(f"{executable} failed: {detail}")
        return result

    def _gpg_show_keys(self, path: Path) -> CommandResult:
        with tempfile.TemporaryDirectory(prefix="dure-bootstrap-gpg-") as directory:
            home = Path(directory) / "home"
            home.mkdir(mode=0o700)
            return self._run_command(
                [
                    "gpg",
                    "--batch",
                    "--no-options",
                    "--homedir",
                    str(home),
                    "--show-keys",
                    "--with-colons",
                    str(path),
                ],
                timeout=10,
            )

    def _atomic_write(
        self,
        path: Path,
        content: bytes,
        mode: int,
        *,
        allow_existing_same: bool = False,
        owner: tuple[int, int] | None = None,
    ) -> None:
        self._ensure_safe_parent(path)
        self._require_safe_target(path)
        if path.exists():
            try:
                current = path.read_bytes()
            except OSError as exc:
                raise BootstrapExecutionError(f"Cannot read existing {path}") from exc
            if allow_existing_same and current == content:
                return
            if allow_existing_same:
                raise BootstrapExecutionError(f"Refusing to overwrite changed {path}")
        temporary = path.with_name(f".{path.name}.dure-bootstrap-{os.getpid()}")
        if temporary.exists() or temporary.is_symlink():
            raise BootstrapExecutionError(f"Temporary path already exists: {temporary}")
        try:
            descriptor = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                mode,
            )
            if owner is not None:
                os.fchown(descriptor, owner[0], owner[1])
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            os.chmod(temporary, mode)
            os.replace(temporary, path)
            directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError as exc:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
            raise BootstrapExecutionError(f"Cannot write {path}: {exc}") from exc

    def _ensure_safe_parent(self, path: Path) -> None:
        parent = path.parent
        cursor = self.root
        try:
            relative = parent.relative_to(self.root)
        except ValueError as exc:
            raise BootstrapExecutionError(f"Path escapes bootstrap root: {path}") from exc
        for part in relative.parts:
            cursor = cursor / part
            if cursor.is_symlink():
                raise BootstrapExecutionError(
                    f"Refusing symbolic link parent for {path}"
                )
            if cursor.exists() and not cursor.is_dir():
                raise BootstrapExecutionError(
                    f"Refusing non-directory parent for {path}"
                )
            if cursor.exists() and self._requires_secure_parent(path):
                try:
                    metadata = cursor.stat()
                except OSError as exc:
                    raise BootstrapExecutionError(
                        f"Cannot inspect parent for {path}: {exc}"
                    ) from exc
                if metadata.st_uid != 0 or stat.S_IMODE(metadata.st_mode) & 0o022:
                    raise BootstrapExecutionError(
                        f"Bootstrap parent must be root-owned and not group/world writable: {cursor}"
                    )
            if not cursor.exists():
                try:
                    cursor.mkdir(mode=0o755)
                except OSError as exc:
                    raise BootstrapExecutionError(
                        f"Cannot create safe parent for {path}: {exc}"
                    ) from exc

    def _inspect_safe_target(
        self,
        report: BootstrapReport,
        path: Path,
        code: str,
    ) -> None:
        try:
            relative = path.parent.relative_to(self.root)
        except ValueError:
            report.checks.append(
                BootstrapCheck(code, "BLOCKED", f"Path escapes bootstrap root: {path}")
            )
            return
        cursor = self.root
        if cursor.is_symlink() or not cursor.is_dir():
            report.checks.append(
                BootstrapCheck(code, "BLOCKED", f"Unsafe bootstrap root for {path}")
            )
            return
        for part in relative.parts:
            cursor = cursor / part
            if cursor.is_symlink():
                report.checks.append(
                    BootstrapCheck(
                        code,
                        "BLOCKED",
                        f"Refusing symbolic link parent for {path}",
                    )
                )
                return
            if cursor.exists() and not cursor.is_dir():
                report.checks.append(
                    BootstrapCheck(
                        code,
                        "BLOCKED",
                        f"Refusing non-directory parent for {path}",
                    )
                )
                return
            if cursor.exists() and self._requires_secure_parent(path):
                try:
                    metadata = cursor.stat()
                except OSError as exc:
                    report.checks.append(
                        BootstrapCheck(
                            code,
                            "BLOCKED",
                            f"Cannot inspect parent for {path}: {exc}",
                        )
                    )
                    return
                if metadata.st_uid != 0 or stat.S_IMODE(metadata.st_mode) & 0o022:
                    report.checks.append(
                        BootstrapCheck(
                            code,
                            "BLOCKED",
                            f"Bootstrap parent must be root-owned and not group/world writable: {cursor}",
                        )
                    )
                    return
        if path.is_symlink():
            report.checks.append(
                BootstrapCheck(code, "BLOCKED", f"Refusing symbolic link target {path}")
            )
            return
        if path.exists():
            try:
                metadata = path.lstat()
            except OSError as exc:
                report.checks.append(
                    BootstrapCheck(code, "BLOCKED", f"Cannot inspect {path}: {exc}")
                )
                return
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
                report.checks.append(
                    BootstrapCheck(
                        code,
                        "BLOCKED",
                        f"Refusing non-regular or hard-linked target {path}",
                    )
                )
                return
            if self.root == Path("/") and (
                metadata.st_uid != 0 or stat.S_IMODE(metadata.st_mode) & 0o022
            ):
                report.checks.append(
                    BootstrapCheck(
                        code,
                        "BLOCKED",
                        f"Existing bootstrap target must be root-owned and not group/world writable: {path}",
                    )
                )

    def _require_safe_target(self, path: Path) -> None:
        if path.is_symlink():
            raise BootstrapExecutionError(f"Refusing symbolic link target {path}")
        if not path.exists():
            return
        try:
            metadata = path.lstat()
        except OSError as exc:
            raise BootstrapExecutionError(f"Cannot inspect {path}: {exc}") from exc
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise BootstrapExecutionError(
                f"Refusing non-regular or hard-linked target {path}"
            )
        if self.root == Path("/") and (
            metadata.st_uid != 0 or stat.S_IMODE(metadata.st_mode) & 0o022
        ):
            raise BootstrapExecutionError(
                f"Existing bootstrap target must be root-owned and not group/world writable: {path}"
            )

    def _requires_secure_parent(self, path: Path) -> bool:
        return self.root == Path("/") and path != self._path(BOOTSTRAP_LOCK_PATH)

    def _acquire_apply_lock(self) -> int:
        path = self._path(BOOTSTRAP_LOCK_PATH)
        self._ensure_safe_parent(path)
        try:
            return acquire_host_setup_lock(
                path,
                require_root_owner=self.root == Path("/"),
            )
        except HostSetupLockError as exc:
            raise BootstrapExecutionError(str(exc)) from exc

    def _path(self, absolute: str) -> Path:
        return self.root / absolute.lstrip("/")

    @staticmethod
    def _primary_fingerprints(result: CommandResult) -> list[str]:
        values: list[str] = []
        awaiting_primary_fingerprint = False
        for line in result.stdout.splitlines():
            parts = line.split(":")
            record_type = parts[0] if parts else ""
            if record_type == "pub":
                awaiting_primary_fingerprint = True
                continue
            if record_type in {"sub", "sec", "ssb"}:
                awaiting_primary_fingerprint = False
                continue
            if (
                awaiting_primary_fingerprint
                and len(parts) > 9
                and record_type == "fpr"
            ):
                values.append(parts[9].upper())
                awaiting_primary_fingerprint = False
        return values

    @staticmethod
    def _docker_source(codename: str, architecture: str) -> str:
        return (
            "Types: deb\n"
            "URIs: https://download.docker.com/linux/ubuntu\n"
            f"Suites: {codename}\n"
            "Components: stable\n"
            f"Architectures: {architecture}\n"
            f"Signed-By: {DOCKER_KEY_PATH}\n"
        )

    @staticmethod
    def _nvidia_source(architecture: str) -> str:
        return (
            f"deb [arch={architecture} signed-by={NVIDIA_KEY_PATH}] "
            f"https://nvidia.github.io/libnvidia-container/stable/deb/{architecture} /\n"
        )

    @staticmethod
    def _nvidia_preferences() -> str:
        return (
            "Package: nvidia-container-toolkit nvidia-container-toolkit-base "
            "libnvidia-container-tools libnvidia-container1\n"
            f"Pin: version {NVIDIA_TOOLKIT_VERSION}\n"
            "Pin-Priority: 1001\n"
        )

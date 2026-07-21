from __future__ import annotations

import heapq
import json
import os
import platform
import re
import shutil
import socket
import stat
from pathlib import Path

from .command import Runner, SubprocessRunner
from .model_cache import (
    MODEL_CACHE_KIND_FULL_SNAPSHOT,
    MODEL_CACHE_KIND_STAGE,
    MODEL_CACHE_MARKER_FILE,
    MODEL_CACHE_SCHEMA_V1,
    ModelCacheMarker,
    ModelCacheMarkerError,
    read_model_cache_marker,
)
from .model_store import DURE_MODEL_STAGING_DIRECTORY
from .models import (
    ArtifactCacheObservation,
    GPUProfile,
    InstalledModelProfile,
    NetworkProfile,
    NodeProfile,
    RuntimeProfile,
    WorkloadProfile,
)


DURE_MODEL_ROOT = Path("/var/lib/dure/models")
DURE_STAGE_ROOT = DURE_MODEL_ROOT / "stages"
DEFAULT_MODEL_ROOTS = (
    DURE_MODEL_ROOT,
    DURE_STAGE_ROOT,
    Path.home() / ".cache" / "huggingface" / "hub",
)
MAX_DISCOVERED_MODELS = 100
MAX_ARTIFACT_CACHE_OBSERVATIONS = 256
MAX_MODEL_CONFIG_BYTES = 1024 * 1024
DURE_MODEL_METADATA_FILE = MODEL_CACHE_MARKER_FILE
DURE_MODEL_METADATA_SCHEMA = MODEL_CACHE_SCHEMA_V1
LLM_RUNTIME_MARKERS = {
    "vllm": "vllm",
    "ollama": "ollama",
    "text-generation-inference": "tgi",
    "text_generation_inference": "tgi",
    "llama.cpp": "llama.cpp",
    "llama-cpp": "llama.cpp",
}


def _read_key_values(path: Path, separator: str = "=") -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            if separator not in raw_line:
                continue
            key, raw_value = raw_line.split(separator, 1)
            values[key.strip()] = raw_value.strip().strip('"')
    except OSError:
        pass
    return values


def _memory_info(path: Path = Path("/proc/meminfo")) -> dict[str, int]:
    values: dict[str, int] = {}
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            match = re.match(r"^(\w+):\s+(\d+)\s+kB$", line)
            if match:
                values[match.group(1)] = int(match.group(2)) // 1024
    except OSError:
        pass
    return values


def _cpu_model(path: Path = Path("/proc/cpuinfo")) -> str:
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.lower().startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or "unknown"


class NodeProbe:
    def __init__(
        self,
        runner: Runner | None = None,
        *,
        model_roots: tuple[Path, ...] | list[Path] | None = None,
    ) -> None:
        self.runner = runner or SubprocessRunner()
        self.model_roots = tuple(model_roots) if model_roots is not None else DEFAULT_MODEL_ROOTS

    def collect(self) -> NodeProfile:
        os_release = _read_key_values(Path("/etc/os-release"))
        memory = _memory_info()
        disk = shutil.disk_usage("/")
        hostname = socket.gethostname()
        issues: list[str] = []

        virtualization = None
        if self.runner.exists("systemd-detect-virt"):
            result = self.runner.run(["systemd-detect-virt"], timeout=3)
            if result.ok and result.stdout and result.stdout != "none":
                virtualization = result.stdout.splitlines()[0]

        gpus = self._probe_gpus(issues)
        runtime = self._probe_runtime()
        network = self._probe_network()
        installed_models = self._probe_models()
        cache_observations, cache_scan_complete = self._probe_artifact_caches()
        workloads = self._probe_workloads(runtime)

        if not gpus:
            issues.append("No CUDA-capable NVIDIA GPU detected")
        if gpus and not runtime.nvidia_runtime:
            issues.append("NVIDIA container runtime was not detected")
        if "CUDA_VISIBLE_DEVICES" in os.environ and not os.environ["CUDA_VISIBLE_DEVICES"]:
            issues.append("CUDA_VISIBLE_DEVICES is explicitly set to an empty value")
        if memory.get("SwapTotal", 0) == 0:
            issues.append("Swap is disabled")

        return NodeProfile(
            node_id=hostname,
            hostname=hostname,
            os_name=os_release.get("PRETTY_NAME", platform.system()),
            os_version=os_release.get("VERSION_ID", "unknown"),
            kernel=platform.release(),
            architecture=platform.machine(),
            virtualization=virtualization,
            cpu_model=_cpu_model(),
            cpu_count=os.cpu_count() or 1,
            memory_mib=memory.get("MemTotal", 0),
            memory_available_mib=memory.get("MemAvailable", 0),
            swap_mib=memory.get("SwapTotal", 0),
            disk_total_mib=disk.total // (1024 * 1024),
            disk_free_mib=disk.free // (1024 * 1024),
            gpus=gpus,
            network=network,
            runtime=runtime,
            installed_models=installed_models,
            artifact_cache_observations=cache_observations,
            artifact_cache_scan_complete=cache_scan_complete,
            workloads=workloads,
            issues=issues,
        )

    @staticmethod
    def _model_config(path: Path) -> dict | None:
        descriptor = -1
        try:
            observed = path.lstat()
            if (
                not stat.S_ISREG(observed.st_mode)
                or observed.st_uid != os.geteuid()
                or observed.st_nlink != 1
                or observed.st_mode & 0o022
                or observed.st_size > MAX_MODEL_CONFIG_BYTES
            ):
                return None
            descriptor = os.open(
                path,
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_NONBLOCK", 0),
            )
            before = os.fstat(descriptor)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_dev != observed.st_dev
                or before.st_ino != observed.st_ino
                or before.st_uid != os.geteuid()
                or before.st_nlink != 1
                or before.st_mode & 0o022
                or before.st_size != observed.st_size
            ):
                return None
            payload = bytearray()
            while len(payload) <= MAX_MODEL_CONFIG_BYTES:
                block = os.read(
                    descriptor,
                    min(8192, MAX_MODEL_CONFIG_BYTES + 1 - len(payload)),
                )
                if not block:
                    break
                payload.extend(block)
            after = os.fstat(descriptor)
            if (
                len(payload) != before.st_size
                or (
                    before.st_dev,
                    before.st_ino,
                    before.st_size,
                    before.st_mtime_ns,
                    before.st_ctime_ns,
                )
                != (
                    after.st_dev,
                    after.st_ino,
                    after.st_size,
                    after.st_mtime_ns,
                    after.st_ctime_ns,
                )
            ):
                return None

            def unique_object(pairs: list[tuple[str, object]]) -> dict:
                value: dict = {}
                for key, item in pairs:
                    if key in value:
                        raise ValueError("duplicate model config key")
                    value[key] = item
                return value

            value = json.loads(
                payload.decode("utf-8"),
                object_pairs_hook=unique_object,
            )
            return value if type(value) is dict else None
        except (OSError, RecursionError, UnicodeError, ValueError):
            return None
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    @staticmethod
    def _safe_model_directory(path: Path) -> bool:
        try:
            observed = path.lstat()
            resolved = path.resolve(strict=True)
        except (OSError, RuntimeError, ValueError):
            return False
        return (
            stat.S_ISDIR(observed.st_mode)
            and observed.st_uid == os.geteuid()
            and not observed.st_mode & 0o022
            and resolved == Path(os.path.abspath(path))
        )

    @classmethod
    def _huggingface_model_config(
        cls, path: Path, repository: Path
    ) -> dict | None:
        try:
            resolved_repository = repository.resolve(strict=True)
            resolved_blobs = (resolved_repository / "blobs").resolve(strict=True)
            resolved = path.resolve(strict=True)
        except (OSError, RuntimeError, ValueError):
            return None
        if not resolved.is_relative_to(resolved_blobs):
            return None
        return cls._model_config(resolved)

    def _model_size_mib(self, path: Path) -> int | None:
        if not self.runner.exists("du"):
            return None
        result = self.runner.run(["du", "-sm", "--", str(path)], timeout=30)
        if not result.ok:
            return None
        try:
            return int(result.stdout.split()[0])
        except (IndexError, ValueError):
            return None

    @staticmethod
    def _quantization(config: dict) -> str | None:
        value = config.get("quantization_config")
        if isinstance(value, dict):
            method = value.get("quant_method") or value.get("quantization_method")
            return str(method) if method else None
        return None

    @staticmethod
    def _dure_model_metadata(candidate: Path) -> ModelCacheMarker | None:
        path = candidate / DURE_MODEL_METADATA_FILE
        try:
            return read_model_cache_marker(path)
        except ModelCacheMarkerError:
            return None

    def _probe_dure_models(self, root: Path) -> list[InstalledModelProfile]:
        if not self._safe_model_directory(root):
            return []
        stage_root = root == DURE_STAGE_ROOT or root.name == DURE_STAGE_ROOT.name
        try:
            candidates = []
            for item in root.iterdir():
                if item.name == DURE_MODEL_STAGING_DIRECTORY or (
                    root == DURE_MODEL_ROOT and item == DURE_STAGE_ROOT
                ):
                    continue
                try:
                    state = item.lstat()
                except OSError:
                    continue
                if stat.S_ISDIR(state.st_mode) and self._safe_model_directory(item):
                    candidates.append(item)
            candidates.sort(key=lambda item: item.name)
        except OSError:
            return []
        models: list[InstalledModelProfile] = []
        for candidate in candidates[:MAX_DISCOVERED_MODELS]:
            config_path = candidate / "config.json"
            parsed_config = self._model_config(config_path)
            config = parsed_config or {}
            metadata = (
                self._dure_model_metadata(candidate)
                if parsed_config is not None
                else None
            )
            configured_quantization = self._quantization(config)
            if (
                metadata
                and configured_quantization
                and metadata.quantization != configured_quantization
            ):
                metadata = None
            configured_name = config.get("_name_or_path")
            advisory_stage = stage_root or (
                metadata is not None
                and metadata.cache_kind == MODEL_CACHE_KIND_STAGE
            )
            model_id = (
                metadata.repository
                if metadata
                else (
                    str(configured_name)
                    if configured_name and not str(configured_name).startswith("/")
                    else candidate.name
                )
            )
            models.append(
                InstalledModelProfile(
                    source="dure",
                    model_id=model_id,
                    path=str(candidate),
                    revision=metadata.revision if metadata else None,
                    quantization=(
                        metadata.quantization
                        if metadata
                        else configured_quantization
                    ),
                    size_mib=self._model_size_mib(candidate),
                    # Heartbeat probing does not rehash a potentially huge stage
                    # tree.  The identity is advisory until the start/readiness
                    # gate validates every file through the canonical manifest.
                    complete=parsed_config is not None and not advisory_stage,
                    manifest_digest=metadata.manifest_digest if metadata else None,
                    cache_kind=metadata.cache_kind if metadata else None,
                    verification_version=(
                        metadata.verification_version if metadata else None
                    ),
                    artifact_set_digest=(
                        getattr(metadata, "artifact_set_digest", None)
                        if metadata
                        else None
                    ),
                    contract_identity_digest=(
                        getattr(metadata, "contract_identity_digest", None)
                        if metadata
                        else None
                    ),
                    source_manifest_digest=(
                        getattr(metadata, "source_manifest_digest", None)
                        if metadata
                        else None
                    ),
                    runtime_image=(
                        getattr(metadata, "runtime_image", None)
                        if metadata
                        else None
                    ),
                    vllm_version=(
                        getattr(metadata, "vllm_version", None)
                        if metadata
                        else None
                    ),
                    exporter_build_digest=(
                        getattr(metadata, "exporter_build_digest", None)
                        if metadata
                        else None
                    ),
                    architecture=(
                        getattr(metadata, "architecture", None)
                        if metadata
                        else None
                    ),
                    loader_format=(
                        getattr(metadata, "loader_format", None)
                        if metadata
                        else None
                    ),
                    tensor_parallel_size=(
                        getattr(metadata, "tensor_parallel_size", None)
                        if metadata
                        else None
                    ),
                    pipeline_parallel_size=(
                        getattr(metadata, "pipeline_parallel_size", None)
                        if metadata
                        else None
                    ),
                    pipeline_rank=(
                        getattr(metadata, "pipeline_rank", None)
                        if metadata
                        else None
                    ),
                    tensor_rank=(
                        getattr(metadata, "tensor_rank", None)
                        if metadata
                        else None
                    ),
                    tensor_keys_digest=(
                        getattr(metadata, "tensor_keys_digest", None)
                        if metadata
                        else None
                    ),
                    cache_identity_digest=(
                        getattr(metadata, "cache_identity_digest", None)
                        if metadata
                        else None
                    ),
                )
            )
        return models

    def _probe_huggingface_models(self, root: Path) -> list[InstalledModelProfile]:
        try:
            repositories = sorted(
                (item for item in root.iterdir() if item.is_dir() and item.name.startswith("models--")),
                key=lambda item: item.name,
            )
        except OSError:
            return []
        models: list[InstalledModelProfile] = []
        for repository in repositories[:MAX_DISCOVERED_MODELS]:
            model_id = repository.name.removeprefix("models--").replace("--", "/")
            snapshots_root = repository / "snapshots"
            try:
                snapshots = sorted(
                    (item for item in snapshots_root.iterdir() if item.is_dir()),
                    key=lambda item: item.stat().st_mtime,
                    reverse=True,
                )
            except OSError:
                snapshots = []
            snapshot = snapshots[0] if snapshots else None
            config_path = snapshot / "config.json" if snapshot else None
            parsed_config = (
                self._huggingface_model_config(config_path, repository)
                if config_path
                else None
            )
            config = parsed_config or {}
            models.append(
                InstalledModelProfile(
                    source="huggingface-cache",
                    model_id=model_id,
                    path=str(snapshot or repository),
                    revision=snapshot.name if snapshot else None,
                    quantization=self._quantization(config),
                    size_mib=self._model_size_mib(repository),
                    complete=bool(snapshot and parsed_config is not None),
                )
            )
        return models

    def _probe_ollama_models(self) -> list[InstalledModelProfile]:
        if not self.runner.exists("ollama"):
            return []
        result = self.runner.run(["ollama", "list"], timeout=15)
        if not result.ok:
            return []
        models: list[InstalledModelProfile] = []
        for line in result.stdout.splitlines()[1:MAX_DISCOVERED_MODELS + 1]:
            parts = line.split()
            if not parts:
                continue
            models.append(InstalledModelProfile(source="ollama", model_id=parts[0]))
        return models

    def _probe_models(self) -> list[InstalledModelProfile]:
        models: list[InstalledModelProfile] = []
        for root in self.model_roots:
            if root.name == "hub":
                models.extend(self._probe_huggingface_models(root))
            else:
                models.extend(self._probe_dure_models(root))
            if len(models) >= MAX_DISCOVERED_MODELS:
                break
        if len(models) < MAX_DISCOVERED_MODELS:
            models.extend(self._probe_ollama_models())
        unique: dict[tuple[str, str, str | None], InstalledModelProfile] = {}
        for model in models[:MAX_DISCOVERED_MODELS]:
            unique[(model.source, model.model_id, model.path)] = model
        return list(unique.values())

    @staticmethod
    def _canonical_cache_digest(name: str) -> str | None:
        match = re.fullmatch(r"sha256-([0-9a-f]{64})", name)
        return f"sha256:{match.group(1)}" if match is not None else None

    def _probe_artifact_cache_root(
        self,
        root: Path,
        cache_kind: str,
    ) -> tuple[list[ArtifactCacheObservation], bool]:
        try:
            observed_root = root.lstat()
        except FileNotFoundError:
            return [], True
        except OSError:
            return [], False
        if not stat.S_ISDIR(observed_root.st_mode) or not self._safe_model_directory(root):
            return [], False
        try:
            canonical = heapq.nsmallest(
                MAX_ARTIFACT_CACHE_OBSERVATIONS + 1,
                (
                    (candidate, digest)
                    for candidate in root.iterdir()
                    if (
                        digest := self._canonical_cache_digest(candidate.name)
                    )
                    is not None
                ),
                key=lambda item: item[0].name,
            )
        except OSError:
            return [], False
        complete = len(canonical) <= MAX_ARTIFACT_CACHE_OBSERVATIONS
        observations: list[ArtifactCacheObservation] = []
        for candidate, cache_identity_digest in canonical[
            :MAX_ARTIFACT_CACHE_OBSERVATIONS
        ]:
            if not self._safe_model_directory(candidate):
                observations.append(
                    ArtifactCacheObservation(
                        cache_kind=cache_kind,
                        cache_identity_digest=cache_identity_digest,
                        condition="UNSAFE",
                    )
                )
                continue
            try:
                marker = read_model_cache_marker(candidate / MODEL_CACHE_MARKER_FILE)
            except ModelCacheMarkerError:
                observations.append(
                    ArtifactCacheObservation(
                        cache_kind=cache_kind,
                        cache_identity_digest=cache_identity_digest,
                        condition="CORRUPT",
                    )
                )
                continue
            if marker.cache_kind != cache_kind:
                observations.append(
                    ArtifactCacheObservation(
                        cache_kind=cache_kind,
                        cache_identity_digest=cache_identity_digest,
                        condition="CORRUPT",
                    )
                )
                continue
            if cache_kind == MODEL_CACHE_KIND_STAGE:
                marker_identity = marker.cache_identity_digest
                observations.append(
                    ArtifactCacheObservation(
                        cache_kind=cache_kind,
                        cache_identity_digest=cache_identity_digest,
                        condition=(
                            "PRESENT"
                            if marker_identity == cache_identity_digest
                            else "IDENTITY_MISMATCH"
                        ),
                        manifest_digest=marker.manifest_digest,
                        verification_version=marker.verification_version,
                        artifact_set_digest=marker.artifact_set_digest,
                        source_manifest_digest=marker.source_manifest_digest,
                        pipeline_rank=marker.pipeline_rank,
                        tensor_rank=marker.tensor_rank,
                    )
                )
            else:
                observations.append(
                    ArtifactCacheObservation(
                        cache_kind=cache_kind,
                        cache_identity_digest=cache_identity_digest,
                        condition=(
                            "PRESENT"
                            if marker.manifest_digest == cache_identity_digest
                            else "IDENTITY_MISMATCH"
                        ),
                        manifest_digest=marker.manifest_digest,
                        verification_version=marker.verification_version,
                    )
                )
        return observations, complete

    def _probe_artifact_caches(
        self,
    ) -> tuple[list[ArtifactCacheObservation], bool]:
        observations: dict[tuple[str, str], ArtifactCacheObservation] = {}
        complete = True
        for root in self.model_roots:
            if root.name == "hub":
                continue
            cache_kind = (
                MODEL_CACHE_KIND_STAGE
                if root == DURE_STAGE_ROOT or root.name == DURE_STAGE_ROOT.name
                else MODEL_CACHE_KIND_FULL_SNAPSHOT
            )
            values, root_complete = self._probe_artifact_cache_root(
                root, cache_kind
            )
            complete = complete and root_complete
            for observation in values:
                observations.setdefault(
                    (
                        observation.cache_kind,
                        observation.cache_identity_digest,
                    ),
                    observation,
                )
        ordered = sorted(
            observations.values(),
            key=lambda item: (item.cache_kind, item.cache_identity_digest),
        )
        if len(ordered) > MAX_ARTIFACT_CACHE_OBSERVATIONS:
            complete = False
            ordered = ordered[:MAX_ARTIFACT_CACHE_OBSERVATIONS]
        return ordered, complete

    @staticmethod
    def _labels(value: str) -> dict[str, str]:
        labels: dict[str, str] = {}
        for item in value.split(","):
            key, separator, label_value = item.partition("=")
            if separator and key:
                labels[key] = label_value
        return labels

    def _probe_workloads(self, runtime: RuntimeProfile) -> list[WorkloadProfile]:
        if runtime.engine != "docker" or not runtime.engine_ready:
            return []
        result = self.runner.run(
            ["docker", "ps", "--all", "--format", "{{json .}}"], timeout=15
        )
        if not result.ok:
            return []
        workloads: list[WorkloadProfile] = []
        for line in result.stdout.splitlines()[:200]:
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            labels = self._labels(str(item.get("Labels", "")))
            name = str(item.get("Names", ""))
            image = str(item.get("Image", ""))
            haystack = f"{name} {image}".lower()
            runtime_name = (
                "ray"
                if name.startswith("dure-ray-")
                else next(
                    (value for marker, value in LLM_RUNTIME_MARKERS.items() if marker in haystack),
                    "unknown",
                )
            )
            dure_managed = "dure.deployment" in labels
            if not dure_managed and runtime_name == "unknown":
                continue
            workloads.append(
                WorkloadProfile(
                    name=name,
                    runtime=runtime_name,
                    image=image,
                    status=str(item.get("Status", "unknown")),
                    deployment_id=labels.get("dure.deployment"),
                    generation=labels.get("dure.generation"),
                    model_id=labels.get("dure.model"),
                    dure_managed=dure_managed,
                )
            )
        return workloads

    def _probe_gpus(self, issues: list[str]) -> list[GPUProfile]:
        if not self.runner.exists("nvidia-smi"):
            if self.runner.exists("lspci"):
                pci = self.runner.run(["lspci"], timeout=5)
                if "NVIDIA" in pci.stdout:
                    issues.append("NVIDIA hardware is visible on PCI but nvidia-smi is unavailable")
            return []

        query = self.runner.run(
            [
                "nvidia-smi",
                "--query-gpu=index,name,uuid,driver_version,memory.total",
                "--format=csv,noheader,nounits",
            ],
            timeout=10,
        )
        if not query.ok:
            issues.append(f"nvidia-smi failed: {query.stderr or query.stdout}")
            return []

        compute_caps: dict[int, str] = {}
        cap_result = self.runner.run(
            ["nvidia-smi", "--query-gpu=index,compute_cap", "--format=csv,noheader,nounits"],
            timeout=10,
        )
        if cap_result.ok:
            for line in cap_result.stdout.splitlines():
                parts = [part.strip() for part in line.split(",", 1)]
                if len(parts) == 2 and parts[0].isdigit():
                    compute_caps[int(parts[0])] = parts[1]

        gpus: list[GPUProfile] = []
        for line in query.stdout.splitlines():
            parts = [part.strip() for part in line.split(",")]
            if len(parts) != 5:
                continue
            try:
                index = int(parts[0])
                memory_mib = int(float(parts[4]))
            except ValueError:
                continue
            gpus.append(
                GPUProfile(
                    index=index,
                    name=parts[1],
                    uuid=parts[2],
                    driver_version=parts[3],
                    memory_mib=memory_mib,
                    compute_capability=compute_caps.get(index),
                )
            )
        return gpus

    def _probe_runtime(self) -> RuntimeProfile:
        engine = next((name for name in ("docker", "podman") if self.runner.exists(name)), None)
        engine_ready = False
        nvidia_runtime = False
        if engine:
            version = self.runner.run([engine, "version"], timeout=8)
            engine_ready = version.ok
            if engine == "docker" and engine_ready:
                info = self.runner.run(
                    ["docker", "info", "--format", "{{json .Runtimes}}"], timeout=8
                )
                nvidia_runtime = info.ok and "nvidia" in info.stdout.lower()
            elif engine == "podman" and engine_ready:
                nvidia_runtime = self.runner.exists("nvidia-ctk") or Path(
                    "/etc/cdi/nvidia.yaml"
                ).exists()

        ray_available = self.runner.exists("ray")
        ray_version = None
        if ray_available:
            result = self.runner.run(["ray", "--version"], timeout=5)
            if result.ok:
                ray_version = result.stdout.splitlines()[-1] if result.stdout else None

        return RuntimeProfile(
            engine=engine,
            engine_ready=engine_ready,
            nvidia_runtime=nvidia_runtime,
            ray_available=ray_available,
            ray_version=ray_version,
        )

    def _probe_network(self) -> NetworkProfile:
        addresses: list[str] = []
        addresses_by_interface: dict[str, list[str]] = {}
        default_interface = None
        if self.runner.exists("ip"):
            address_result = self.runner.run(["ip", "-j", "address", "show"], timeout=5)
            if address_result.ok:
                try:
                    for interface in json.loads(address_result.stdout):
                        interface_name = interface.get("ifname")
                        for info in interface.get("addr_info", []):
                            if info.get("family") == "inet" and info.get("local") != "127.0.0.1":
                                address = info["local"]
                                addresses.append(address)
                                if type(interface_name) is str:
                                    addresses_by_interface.setdefault(
                                        interface_name, []
                                    ).append(address)
                except (json.JSONDecodeError, KeyError, TypeError):
                    pass
            route_result = self.runner.run(["ip", "-j", "route", "show", "default"], timeout=5)
            if route_result.ok:
                try:
                    routes = json.loads(route_result.stdout)
                    if routes:
                        default_interface = routes[0].get("dev")
                except (json.JSONDecodeError, TypeError):
                    pass
        return NetworkProfile(
            default_interface=default_interface,
            addresses=addresses,
            default_interface_addresses=(
                addresses_by_interface.get(default_interface, [])
                if type(default_interface) is str
                else []
            ),
        )

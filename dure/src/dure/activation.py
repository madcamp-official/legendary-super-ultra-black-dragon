from __future__ import annotations

import hashlib
import json
import math
import re
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .artifact_manifest import parse_artifact_manifest


_ACTIVATION_NAMESPACE = uuid.UUID("635f4d8f-d10a-4cdd-b74d-adcfc02fc3d3")
_TERMINAL_TASK_STATUSES = frozenset({"SUCCEEDED", "FAILED", "CANCELED"})
_TERMINAL_PREPARATION_STATUSES = frozenset(
    {"SUCCEEDED", "PARTIAL_FAILED", "FAILED"}
)
_ARTIFACT_FIELDS = frozenset(
    {
        "model_id",
        "repository",
        "revision",
        "manifest_digest",
        "quantization",
        "size_mib",
        "default_max_model_len",
        "layer_count",
        "license_id",
    }
)
_RUNTIME_FIELDS = frozenset(
    {"version", "image", "vllm_version", "cuda_version", "gpu_architectures"}
)
_PLACEMENT_FIELDS = frozenset(
    {
        "profile_id",
        "topology",
        "node_count",
        "min_gpu_memory_mib",
        "min_disk_free_mib",
        "pipeline_parallel_size",
        "tensor_parallel_size",
        "requires_network_evidence",
        "requires_nccl",
        "min_bandwidth_mbps",
        "max_rtt_ms",
        "max_packet_loss_pct",
        "max_ttft_p95_ms",
        "max_tpot_p95_ms",
        "max_e2e_p95_ms",
        "min_success_rate",
        "min_vram_headroom_pct",
        "min_throughput_tps",
    }
)


def _exact_object(value: object, expected: frozenset[str], field: str) -> dict:
    if type(value) is not dict:
        raise ValueError(f"activation {field} must be an object")
    if any(type(key) is not str for key in value):
        raise ValueError(f"activation {field} keys must be strings")
    actual = set(value)
    if actual != expected:
        detail = sorted(actual ^ expected)
        raise ValueError(
            f"activation {field} fields do not match the closed schema: "
            + ", ".join(detail)
        )
    return value


def _positive_integer(value: object, field: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"activation {field} must be a positive integer")
    return value


def _agent_supports_preparation(value: object) -> bool:
    if type(value) is not str:
        return False
    match = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)(?:[+.-].*)?", value)
    return match is not None and tuple(int(item) for item in match.groups()) >= (0, 3, 26)


def _positive_number(value: object, field: str) -> float:
    if (
        type(value) not in {int, float}
        or not math.isfinite(value)
        or value <= 0
    ):
        raise ValueError(f"activation {field} must be a finite positive number")
    return float(value)


@dataclass(frozen=True)
class ActivationSpec:
    artifact: dict
    manifest: dict
    runtime: dict
    quality_rank: int
    placement: dict
    workload_id: str
    dure_commit: str
    attempt: int
    digest: str

    @classmethod
    def from_dict(cls, value: object) -> "ActivationSpec":
        source = _exact_object(
            value,
            frozenset(
                {
                    "schema_version",
                    "artifact",
                    "manifest",
                    "runtime",
                    "release",
                    "placement",
                    "benchmark",
                }
            ),
            "document",
        )
        if type(source["schema_version"]) is not int or source["schema_version"] != 1:
            raise ValueError("activation schema_version must be exactly 1")
        artifact = _exact_object(source["artifact"], _ARTIFACT_FIELDS, "artifact")
        runtime = _exact_object(source["runtime"], _RUNTIME_FIELDS, "runtime")
        release = _exact_object(
            source["release"], frozenset({"quality_rank"}), "release"
        )
        placement = _exact_object(
            source["placement"], _PLACEMENT_FIELDS, "placement"
        )
        benchmark = _exact_object(
            source["benchmark"],
            frozenset({"workload_id", "dure_commit", "attempt"}),
            "benchmark",
        )
        canonical_manifest = parse_artifact_manifest(source["manifest"])
        if artifact["manifest_digest"] != canonical_manifest.digest:
            raise ValueError(
                "activation artifact manifest_digest does not match manifest content"
            )
        for field in ("model_id", "repository", "revision", "quantization"):
            if type(artifact[field]) is not str or not artifact[field]:
                raise ValueError(f"activation artifact.{field} is required")
        if re.fullmatch(r"[0-9a-f]{40,64}", artifact["revision"]) is None:
            raise ValueError("activation artifact.revision must be immutable")
        for field in (
            "size_mib",
            "default_max_model_len",
            "layer_count",
        ):
            _positive_integer(artifact[field], f"artifact.{field}")
        if artifact["license_id"] is not None and (
            type(artifact["license_id"]) is not str or not artifact["license_id"]
        ):
            raise ValueError("activation artifact.license_id must be null or a string")
        for field in ("version", "vllm_version", "cuda_version"):
            if type(runtime[field]) is not str or not runtime[field]:
                raise ValueError(f"activation runtime.{field} is required")
        if (
            type(runtime["image"]) is not str
            or re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9._:/-]*@sha256:[0-9a-f]{64}",
                runtime["image"],
            )
            is None
        ):
            raise ValueError("activation runtime.image must be OCI digest-pinned")
        if (
            type(runtime["gpu_architectures"]) is not list
            or not runtime["gpu_architectures"]
            or any(type(item) is not str for item in runtime["gpu_architectures"])
        ):
            raise ValueError("activation runtime.gpu_architectures is invalid")
        if (
            placement["topology"] != "single-gpu"
            or placement["node_count"] != 1
            or placement["pipeline_parallel_size"] != 1
            or placement["tensor_parallel_size"] != 1
            or placement["requires_network_evidence"] is not False
            or placement["requires_nccl"] is not False
        ):
            raise ValueError(
                "automatic activation currently supports only a single-gpu placement"
            )
        if any(
            placement[field] is not None
            for field in (
                "min_bandwidth_mbps",
                "max_rtt_ms",
                "max_packet_loss_pct",
            )
        ):
            raise ValueError(
                "single-gpu activation cannot declare network qualification thresholds"
            )
        if type(placement["profile_id"]) is not str or not placement["profile_id"]:
            raise ValueError("activation placement.profile_id is required")
        for field in (
            "min_gpu_memory_mib",
            "min_disk_free_mib",
            "max_ttft_p95_ms",
            "max_tpot_p95_ms",
            "max_e2e_p95_ms",
            "min_throughput_tps",
        ):
            _positive_number(placement[field], f"placement.{field}")
        for field in ("min_success_rate", "min_vram_headroom_pct"):
            value = placement[field]
            if (
                type(value) not in {int, float}
                or not math.isfinite(value)
                or value < 0
                or value > (1 if field == "min_success_rate" else 100)
            ):
                raise ValueError(f"activation placement.{field} is out of range")
        if benchmark["workload_id"] not in {
            "short-chat-1k-128",
            "long-chat-4k-256",
            "max-context",
            "quality-eval",
        }:
            raise ValueError("activation benchmark.workload_id is unsupported")
        if re.fullmatch(r"[0-9a-f]{40,64}", benchmark["dure_commit"]) is None:
            raise ValueError("activation benchmark.dure_commit must be immutable")
        attempt = _positive_integer(benchmark["attempt"], "benchmark.attempt")
        quality_rank = _positive_integer(release["quality_rank"], "release.quality_rank")
        canonical = {
            **source,
            "manifest": canonical_manifest.document,
        }
        encoded = json.dumps(
            canonical,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        return cls(
            artifact=dict(artifact),
            manifest=canonical_manifest.document,
            runtime={
                **runtime,
                "gpu_architectures": sorted(set(runtime["gpu_architectures"])),
            },
            quality_rank=quality_rank,
            placement=dict(placement),
            workload_id=benchmark["workload_id"],
            dure_commit=benchmark["dure_commit"],
            attempt=attempt,
            digest="sha256:" + hashlib.sha256(encoded).hexdigest(),
        )

    @classmethod
    def from_file(cls, path: Path) -> "ActivationSpec":
        value = json.loads(path.read_text(encoding="utf-8"))
        return cls.from_dict(value)


class ActivationWorkflow:
    def __init__(
        self,
        client,
        *,
        timeout: float = 7200,
        poll_interval: float = 5,
        sleeper: Callable[[float], None] = time.sleep,
        reporter: Callable[[str], None] | None = None,
    ) -> None:
        if (
            type(timeout) not in {int, float}
            or type(poll_interval) not in {int, float}
            or not math.isfinite(timeout)
            or not math.isfinite(poll_interval)
            or timeout <= 0
            or poll_interval <= 0
        ):
            raise ValueError("activation timeout and poll interval must be positive")
        self.client = client
        self.timeout = timeout
        self.poll_interval = poll_interval
        self.sleeper = sleeper
        self.reporter = reporter or (lambda _message: None)

    def _deadline(self) -> float:
        return time.monotonic() + self.timeout

    def _wait_tasks(self, task_ids: list[str], *, stage: str) -> list[dict]:
        pending = set(task_ids)
        completed: dict[str, dict] = {}
        deadline = self._deadline()
        while pending:
            for task_id in list(pending):
                task = self.client.request(
                    "GET", f"/v1/admin/tasks/{task_id}"
                )["task"]
                if task["status"] in _TERMINAL_TASK_STATUSES:
                    completed[task_id] = task
                    pending.remove(task_id)
            if pending:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"activation {stage} timed out")
                self.sleeper(self.poll_interval)
        failed = [
            item for item in completed.values() if item["status"] != "SUCCEEDED"
        ]
        if failed:
            summary = ", ".join(
                f"{item['id']}={item['status']}:{item.get('error') or '-'}"
                for item in failed
            )
            raise RuntimeError(f"activation {stage} failed: {summary}")
        return [completed[task_id] for task_id in task_ids]

    def _wait_preparation(self, preparation_id: str) -> dict:
        deadline = self._deadline()
        while True:
            value = self.client.request(
                "GET", f"/v1/admin/deployment-preparations/{preparation_id}"
            )["preparation"]
            if value["status"] in _TERMINAL_PREPARATION_STATUSES:
                if value["status"] != "SUCCEEDED":
                    raise RuntimeError(
                        "activation artifact preparation failed: " + value["status"]
                    )
                return value
            if time.monotonic() >= deadline:
                raise TimeoutError("activation artifact preparation timed out")
            self.sleeper(self.poll_interval)

    def _wait_benchmark(self, request_id: str) -> dict:
        deadline = self._deadline()
        while True:
            run = self.client.request(
                "GET", f"/v1/admin/benchmark-runs/{request_id}"
            )["benchmark_run"]
            if run["status"] in {"SUCCEEDED", "FAILED"}:
                if run["status"] != "SUCCEEDED":
                    raise RuntimeError(
                        "activation benchmark failed: "
                        + (run.get("failure_code") or "unknown")
                    )
                return run
            if time.monotonic() >= deadline:
                raise TimeoutError("activation benchmark timed out")
            self.sleeper(self.poll_interval)

    @staticmethod
    def _selected_inventory_nodes(
        inventory: dict,
        node_ids: list[str] | None,
    ) -> list[dict]:
        if node_ids is not None and any(type(item) is not str for item in node_ids):
            raise ValueError("activation node IDs must be strings")
        requested = set(node_ids or [])
        values = []
        for node in inventory.get("nodes", []):
            if requested and node.get("id") not in requested:
                continue
            if (
                node.get("approved")
                and node.get("connectivity") == "online"
                and type(node.get("profile")) is dict
            ):
                values.append(node)
        if requested and requested != {item["id"] for item in values}:
            missing = sorted(requested - {item["id"] for item in values})
            raise ValueError(
                "activation nodes are not approved and online: " + ", ".join(missing)
            )
        if not values:
            raise ValueError("activation found no approved online nodes")
        return values

    @staticmethod
    def _benchmark_node(spec: ActivationSpec, nodes: list[dict]) -> dict:
        eligible = []
        for node in nodes:
            profile = node["profile"]
            runtime = profile.get("runtime") or {}
            healthy = [
                gpu
                for gpu in profile.get("gpus", [])
                if gpu.get("healthy") is True
                and type(gpu.get("memory_mib")) is int
                and gpu["memory_mib"] >= spec.placement["min_gpu_memory_mib"]
            ]
            if (
                healthy
                and _agent_supports_preparation(node.get("agent_version"))
                and runtime.get("engine") == "docker"
                and runtime.get("engine_ready") is True
                and runtime.get("nvidia_runtime") is True
                and profile.get("disk_free_mib", 0)
                >= max(
                    spec.placement["min_disk_free_mib"],
                    spec.artifact["size_mib"] * 2 + 64,
                )
            ):
                exact_cache = any(
                    model.get("repository", model.get("model_id"))
                    == spec.artifact["repository"]
                    and model.get("revision") == spec.artifact["revision"]
                    and model.get("manifest_digest")
                    == spec.artifact["manifest_digest"]
                    and model.get("complete") is True
                    for model in profile.get("installed_models", [])
                    if type(model) is dict
                )
                eligible.append(
                    (
                        exact_cache,
                        profile.get("disk_free_mib", 0),
                        max(gpu["memory_mib"] for gpu in healthy),
                        node["id"],
                        node,
                    )
                )
        if not eligible:
            raise ValueError("activation found no node eligible for the placement")
        return max(eligible, key=lambda item: item[:3] + (item[3],))[4]

    def _refresh(self, nodes: list[dict]) -> None:
        value = self.client.request(
            "POST",
            "/v1/admin/tasks",
            {
                "node_ids": sorted(item["id"] for item in nodes),
                "type": "PROBE",
                "deployment_id": None,
                "options": {},
            },
        )
        if value.get("errors"):
            raise RuntimeError(f"activation probe was rejected: {value['errors']}")
        self._wait_tasks(
            [item["id"] for item in value["tasks"]], stage="probe"
        )

    @staticmethod
    def _same(record: dict, expected: dict) -> bool:
        return all(record.get(key) == value for key, value in expected.items())

    def _upsert_registry(self, spec: ActivationSpec) -> tuple[dict, dict, dict, dict]:
        artifacts = self.client.request("GET", "/v1/admin/model-artifacts")[
            "artifacts"
        ]
        matching = [
            item
            for item in artifacts
            if item["repository"] == spec.artifact["repository"]
            and item["revision"] == spec.artifact["revision"]
        ]
        if matching:
            artifact = matching[0]
            if not self._same(artifact, spec.artifact):
                raise RuntimeError("activation artifact identity already has different metadata")
        else:
            artifact = self.client.request(
                "POST", "/v1/admin/model-artifacts", spec.artifact
            )["artifact"]
        self.client.request(
            "POST",
            f"/v1/admin/model-artifacts/{artifact['id']}/manifest",
            spec.manifest,
        )

        runtimes = self.client.request("GET", "/v1/admin/runtime-releases")[
            "runtimes"
        ]
        matching_runtime = [
            item for item in runtimes if item["image"] == spec.runtime["image"]
        ]
        if matching_runtime:
            runtime = matching_runtime[0]
            if not self._same(runtime, spec.runtime):
                raise RuntimeError("activation runtime image already has different metadata")
        else:
            runtime = self.client.request(
                "POST", "/v1/admin/runtime-releases", spec.runtime
            )["runtime"]

        releases = self.client.request("GET", "/v1/admin/model-releases")[
            "releases"
        ]
        matching_release = [
            item
            for item in releases
            if item["artifact"]["id"] == artifact["id"]
            and item["runtime"]["id"] == runtime["id"]
        ]
        if matching_release:
            release = matching_release[0]
            if release["quality_rank"] != spec.quality_rank:
                raise RuntimeError("activation release already has a different quality rank")
        else:
            release = self.client.request(
                "POST",
                "/v1/admin/model-releases",
                {
                    "artifact_id": artifact["id"],
                    "runtime_id": runtime["id"],
                    "quality_rank": spec.quality_rank,
                },
            )["release"]
        placements = [
            item
            for item in release.get("placements", [])
            if item["profile_id"] == spec.placement["profile_id"]
        ]
        if placements:
            placement = placements[0]
            if not self._same(placement, spec.placement):
                raise RuntimeError("activation placement already has different metadata")
        else:
            if release["status"] != "DRAFT":
                raise RuntimeError("activation cannot add a placement after DRAFT")
            placement = self.client.request(
                "POST",
                f"/v1/admin/model-releases/{release['id']}/placements",
                spec.placement,
            )["placement"]
        return artifact, runtime, release, placement

    def preview(
        self,
        spec: ActivationSpec,
        *,
        node_ids: list[str] | None,
    ) -> dict:
        node_ids = list(dict.fromkeys(node_ids)) if node_ids is not None else None
        inventory = self.client.request("GET", "/v1/admin/inventory")
        nodes = self._selected_inventory_nodes(inventory, node_ids)
        benchmark = self._benchmark_node(spec, nodes)
        return {
            "apply": False,
            "spec_digest": spec.digest,
            "benchmark_node_id": benchmark["id"],
            "candidate_node_ids": sorted(item["id"] for item in nodes),
            "steps": [
                "REFRESH_INVENTORY",
                "REGISTER_ARTIFACT_MANIFEST_RUNTIME_RELEASE_PLACEMENT",
                "PREPARE_AND_RUN_BENCHMARK",
                "PROMOTE_ACTIVE",
                "RECOMMEND_AND_ACCEPT",
                "PREPARE_APPLY_AND_VERIFY",
            ],
        }

    def apply(
        self,
        spec: ActivationSpec,
        *,
        node_ids: list[str] | None,
    ) -> dict:
        node_ids = list(dict.fromkeys(node_ids)) if node_ids is not None else None
        self.reporter("Refreshing node inventory")
        inventory = self.client.request("GET", "/v1/admin/inventory")
        nodes = self._selected_inventory_nodes(inventory, node_ids)
        self._refresh(nodes)
        inventory = self.client.request("GET", "/v1/admin/inventory")
        nodes = self._selected_inventory_nodes(inventory, node_ids)
        benchmark_node = self._benchmark_node(spec, nodes)

        self.reporter("Registering immutable model and runtime metadata")
        artifact, runtime, release, placement = self._upsert_registry(spec)
        if release["status"] == "DRAFT":
            release = self.client.request(
                "POST",
                f"/v1/admin/model-releases/{release['id']}/transition",
                {"status": "VALIDATED"},
            )["release"]
        if release["status"] == "VALIDATED":
            self.reporter("Preparing exact model/image and running benchmark")
            request_id = str(
                uuid.uuid5(
                    _ACTIVATION_NAMESPACE,
                    ":".join(
                        (
                            spec.digest,
                            placement["id"],
                            benchmark_node["id"],
                            str(spec.attempt),
                        )
                    ),
                )
            )
            prepared = self.client.request(
                "POST",
                "/v1/admin/benchmark-runs/prepare",
                {
                    "request_id": request_id,
                    "release_id": release["id"],
                    "placement_id": placement["id"],
                    "node_ids": [benchmark_node["id"]],
                    "workload_id": spec.workload_id,
                    "dure_commit": spec.dure_commit,
                },
            )["benchmark_run"]
            if prepared["status"] == "PREPARED":
                applied = self.client.request(
                    "POST",
                    f"/v1/admin/benchmark-runs/{request_id}/apply",
                    {
                        "apply": True,
                        "prepare_model": True,
                        "pull_image": True,
                    },
                )
                self._wait_tasks(
                    [applied["task"]["id"]], stage="benchmark"
                )
            benchmark_run = self._wait_benchmark(request_id)
            self.reporter("Promoting benchmark-qualified release to ACTIVE")
            release = self.client.request(
                "POST", f"/v1/admin/model-releases/{release['id']}/promote"
            )["release"]
        else:
            benchmark_run = None
        if release["status"] != "ACTIVE":
            raise RuntimeError("activation release did not become ACTIVE")

        self.reporter("Refreshing inventory and creating recommendation")
        self._refresh(nodes)
        recommendation_payload = (
            {"node_ids": sorted(node_ids), "all_online": False}
            if node_ids
            else {"node_ids": [], "all_online": True}
        )
        recommendation = self.client.request(
            "POST",
            "/v1/admin/deployment-recommendations",
            {**recommendation_payload, "objective": "quality-first"},
        )["recommendation"]
        selected = recommendation.get("selected")
        if type(selected) is not dict:
            raise RuntimeError(
                "activation produced no feasible recommendation: "
                + json.dumps(recommendation.get("rejections", []), sort_keys=True)
            )
        if (
            selected.get("model_release_id") != release["id"]
            or selected.get("placement_id") != placement["id"]
        ):
            raise RuntimeError(
                "activation recommendation selected a different release; "
                "adjust the target release quality_rank or candidate nodes"
            )
        accepted = self.client.request(
            "POST",
            f"/v1/admin/deployment-recommendations/{recommendation['id']}/accept",
            {},
        )["deployment"]
        deployment_id = accepted["id"]
        selected_node_ids = list(selected["node_ids"])

        self.reporter("Preparing deployment artifacts")
        preparation_request_id = str(
            uuid.uuid5(
                _ACTIVATION_NAMESPACE,
                f"prepare:{spec.digest}:{deployment_id}",
            )
        )
        preview = self.client.request(
            "POST",
            f"/v1/admin/deployments/{deployment_id}/prepare",
            {"request_id": preparation_request_id, "apply": False},
        )
        preparation_id = preview["preparation"]["id"]
        self.client.request(
            "POST",
            f"/v1/admin/deployments/{deployment_id}/prepare",
            {"request_id": preparation_request_id, "apply": True},
        )
        preparation = self._wait_preparation(preparation_id)

        self.reporter("Applying deployment and verifying API readiness")
        applied = self.client.request(
            "POST",
            "/v1/admin/tasks",
            {
                "node_ids": selected_node_ids,
                "type": "APPLY_DEPLOYMENT",
                "deployment_id": deployment_id,
                "options": {"serve": True},
            },
        )
        if applied.get("errors"):
            raise RuntimeError(f"activation deployment apply was rejected: {applied['errors']}")
        self._wait_tasks(
            [item["id"] for item in applied["tasks"]], stage="deployment apply"
        )
        verified = self.client.request(
            "POST",
            "/v1/admin/tasks",
            {
                "node_ids": selected_node_ids,
                "type": "VERIFY",
                "deployment_id": deployment_id,
                "options": {"api": True},
            },
        )
        if verified.get("errors"):
            raise RuntimeError(f"activation verify was rejected: {verified['errors']}")
        verify_tasks = self._wait_tasks(
            [item["id"] for item in verified["tasks"]], stage="verification"
        )
        return {
            "apply": True,
            "spec_digest": spec.digest,
            "artifact_id": artifact["id"],
            "runtime_id": runtime["id"],
            "release_id": release["id"],
            "placement_id": placement["id"],
            "benchmark_node_id": benchmark_node["id"],
            "benchmark_run_id": benchmark_run["id"] if benchmark_run else None,
            "recommendation_id": recommendation["id"],
            "deployment_id": deployment_id,
            "preparation_id": preparation["id"],
            "node_ids": selected_node_ids,
            "verify_task_ids": [item["id"] for item in verify_tasks],
            "status": "READY",
        }

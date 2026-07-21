from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from pathlib import Path

from . import __version__
from .models import CheckResult, NodeProfile
from .orchestrator import InitOrchestrator
from .planner import build_plan, classify_node, recommend_local_model
from .probe import NodeProbe
from .readiness import ReadinessVerifier
from .runtime import read_plan, write_plan
from .state import StateStore


def _add_admin_connection(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--server", default=None, help="Control plane URL (or DURE_SERVER)")
    parser.add_argument("--token", default=None, help="Admin token (or DURE_ADMIN_TOKEN)")


def _canonical_uuid_argument(value: str) -> str:
    try:
        if str(uuid.UUID(value)) != value:
            raise ValueError
    except (AttributeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("must be a canonical UUID") from exc
    return value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dure", description="Community LLM node bootstrapper")
    parser.add_argument("--version", action="version", version=f"dure {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    bootstrap = subparsers.add_parser(
        "bootstrap",
        help="Preview or explicitly install Docker and NVIDIA Container Toolkit",
    )
    bootstrap.add_argument(
        "--apply",
        action="store_true",
        help="Apply the closed installation plan after all preflight checks pass",
    )
    bootstrap.add_argument(
        "--allow-docker-restart",
        action="store_true",
        help="Allow the required Docker restart after reviewing running containers",
    )
    bootstrap.add_argument(
        "--json", action="store_true", help="Print the closed bootstrap report as JSON"
    )

    doctor = subparsers.add_parser("doctor", help="Inspect node hardware and runtime")
    doctor.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    doctor.add_argument("--output", type=Path, help="Write the profile to a JSON file")

    plan = subparsers.add_parser("plan", help="Create a deployment plan")
    plan.add_argument("--profile", type=Path, action="append", default=[])
    plan.add_argument("--model", default="auto")
    plan.add_argument("--image", default="vllm/vllm-openai:latest")
    plan.add_argument("--network-interface")
    plan.add_argument("--output", type=Path, required=True)

    init = subparsers.add_parser("init", help="Initialize and optionally provision this node")
    init.add_argument("--plan", type=Path)
    init.add_argument("--apply", action="store_true")
    init.add_argument("--accept-model-download", action="store_true")
    init.add_argument("--pull", action="store_true")
    init.add_argument("--allow-unpinned-image", action="store_true")
    init.add_argument("--replace", action="store_true")
    init.add_argument("--serve", action="store_true")
    init.add_argument("--state-file", type=Path)
    init.add_argument("--json", action="store_true")

    status = subparsers.add_parser("status", help="Show persisted node state")
    status.add_argument("--state-file", type=Path)
    status.add_argument("--json", action="store_true")

    verify = subparsers.add_parser("verify", help="Verify an applied deployment")
    verify.add_argument("--plan", type=Path, required=True)
    verify.add_argument("--api", action="store_true")

    join = subparsers.add_parser("join", help="Join this machine to the configured control plane")
    join.add_argument("--server", help="Override the packaged central server address")
    join.add_argument("--insecure", action="store_true", default=None, help="Development only")

    subparsers.add_parser(
        "unjoin", help="Release this machine's Dure GPU and leave the control plane"
    )

    admin = subparsers.add_parser("admin", help="Manage nodes through the central control plane")
    _add_admin_connection(admin)
    admin_sub = admin.add_subparsers(dest="admin_command", required=True)
    nodes = admin_sub.add_parser("nodes")
    nodes.add_argument("--online", action="store_true")
    nodes.add_argument("--offline", action="store_true")
    nodes.add_argument("--pending", action="store_true")
    nodes.add_argument("--json", action="store_true")
    node = admin_sub.add_parser("node")
    node_sub = node.add_subparsers(dest="node_command", required=True)
    node_show = node_sub.add_parser("show")
    node_show.add_argument("node_id")
    node_approve = node_sub.add_parser("approve")
    node_approve.add_argument("node_id")
    admin_unjoin = admin_sub.add_parser(
        "unjoin", help="Unjoin one or all registered GPU nodes"
    )
    admin_unjoin_target = admin_unjoin.add_mutually_exclusive_group(required=True)
    admin_unjoin_target.add_argument("--node", dest="node_id")
    admin_unjoin_target.add_argument("--all", dest="all_nodes", action="store_true")
    enrollment = admin_sub.add_parser("enrollment")
    enrollment_sub = enrollment.add_subparsers(dest="enrollment_command", required=True)
    enrollment_create = enrollment_sub.add_parser("create")
    enrollment_create.add_argument("--expires-in", default="1h")
    artifact_manifest = admin_sub.add_parser(
        "artifact-manifest",
        help="Register or inspect a normalized model artifact manifest",
    )
    artifact_manifest_sub = artifact_manifest.add_subparsers(
        dest="artifact_manifest_command",
        required=True,
    )
    artifact_manifest_register = artifact_manifest_sub.add_parser("register")
    artifact_manifest_register.add_argument("artifact_id")
    artifact_manifest_register.add_argument(
        "--file",
        type=Path,
        required=True,
        help="Normalized manifest JSON file",
    )
    artifact_manifest_show = artifact_manifest_sub.add_parser("show")
    artifact_manifest_show.add_argument("artifact_id")
    artifact_cache = admin_sub.add_parser(
        "artifact-cache",
        help="Inspect, verify, or explicitly quarantine one central cache record",
    )
    artifact_cache_sub = artifact_cache.add_subparsers(
        dest="artifact_cache_command",
        required=True,
    )
    artifact_cache_sub.add_parser("list")
    artifact_cache_show = artifact_cache_sub.add_parser("show")
    artifact_cache_show.add_argument("cache_id")
    artifact_cache_verify = artifact_cache_sub.add_parser("verify")
    artifact_cache_verify.add_argument("cache_id")
    artifact_cache_quarantine = artifact_cache_sub.add_parser("quarantine")
    artifact_cache_quarantine.add_argument("cache_id")
    artifact_cache_quarantine.add_argument(
        "--apply",
        action="store_true",
        help="Queue exactly one quarantine task after all references are clear",
    )
    deployment = admin_sub.add_parser("deployment")
    deployment_sub = deployment.add_subparsers(dest="deployment_command", required=True)
    deployment_create = deployment_sub.add_parser("create")
    deployment_create.add_argument("--profile", type=Path, action="append", required=True)
    deployment_create.add_argument("--model", default="auto")
    deployment_create.add_argument("--image", required=True)
    deployment_create.add_argument("--network-interface")
    deployment_create.add_argument("--accept-model-download", action="store_true")
    deployment_create.add_argument("--pull", action="store_true")
    deployment_recommend = deployment_sub.add_parser(
        "recommend", help="Recommend a deployment from stored central inventory"
    )
    recommendation_nodes = deployment_recommend.add_mutually_exclusive_group(required=True)
    recommendation_nodes.add_argument(
        "--all-online", action="store_true", help="Consider all approved online nodes"
    )
    recommendation_nodes.add_argument(
        "--nodes",
        action="append",
        nargs="+",
        metavar="NODE_ID",
        help="Consider only these approved node UUIDs; may be repeated",
    )
    deployment_recommend.add_argument(
        "--objective",
        choices=("quality-first",),
        default="quality-first",
        help="Recommendation policy objective",
    )
    deployment_show = deployment_sub.add_parser(
        "show", help="Show one deployment generation and its operations"
    )
    deployment_show.add_argument("deployment_id")
    deployment_generations = deployment_sub.add_parser(
        "generations", help="List every generation in a deployment lineage"
    )
    deployment_generations.add_argument("deployment_id")
    deployment_prepare = deployment_sub.add_parser(
        "prepare", help="Preview or explicitly start model and image preparation"
    )
    deployment_prepare.add_argument("deployment_id")
    deployment_prepare.add_argument(
        "--request-id",
        required=True,
        type=_canonical_uuid_argument,
        help="Canonical UUID used to retry the same immutable preparation request",
    )
    deployment_prepare.add_argument(
        "--stage-variant",
        dest="artifact_set_digest",
        help=(
            "Use this exact VALIDATED stage artifact-set sha256 digest; "
            "omitting it preserves FULL_SNAPSHOT preparation"
        ),
    )
    deployment_prepare.add_argument(
        "--apply",
        action="store_true",
        help="Queue preparation tasks after all server-side safety checks pass",
    )
    deployment_preparation = deployment_sub.add_parser(
        "preparation", help="Show one model and image preparation operation"
    )
    deployment_preparation.add_argument("preparation_id")
    deployment_rollback = deployment_sub.add_parser(
        "rollback", help="Prepare or explicitly apply a rollback to the previous generation"
    )
    deployment_rollback.add_argument("deployment_id")
    deployment_rollback.add_argument("--nodes", nargs="+", required=True)
    deployment_rollback.add_argument(
        "--apply",
        action="store_true",
        help="Queue the rollback after all server-side safety checks pass",
    )
    deployment_rollback.add_argument(
        "--serve",
        action="store_true",
        help="Start and verify the API after the previous generation is restored",
    )
    recommendation = admin_sub.add_parser(
        "recommendation", help="Inspect or accept a stored deployment recommendation"
    )
    recommendation_sub = recommendation.add_subparsers(
        dest="recommendation_command", required=True
    )
    recommendation_show = recommendation_sub.add_parser(
        "show", help="Show a stored deployment recommendation"
    )
    recommendation_show.add_argument("recommendation_id")
    recommendation_accept = recommendation_sub.add_parser(
        "accept", help="Create an immutable deployment generation"
    )
    recommendation_accept.add_argument("recommendation_id")
    recommendation_accept.add_argument(
        "--previous-generation",
        dest="previous_generation_id",
        help="Link the new generation to an existing deployment generation",
    )
    for command in ("apply", "start", "stop", "restart"):
        operation = admin_sub.add_parser(command)
        operation.add_argument("deployment_id")
        operation.add_argument("--nodes", nargs="+", required=True)
        if command in {"apply", "start", "restart"}:
            operation.add_argument("--serve", action="store_true")
    admin_probe = admin_sub.add_parser("probe")
    admin_probe.add_argument("--nodes", nargs="+", required=True)
    admin_verify = admin_sub.add_parser("verify")
    admin_verify.add_argument("deployment_id")
    admin_verify.add_argument("--nodes", nargs="+", required=True)
    admin_verify.add_argument("--api", action="store_true")
    tasks = admin_sub.add_parser("tasks")
    tasks.add_argument("--watch", action="store_true")
    diagnose = admin_sub.add_parser("diagnose", help="Ask local Codex to assess central inventory")
    diagnose.add_argument("--nodes", nargs="+", help="Limit diagnosis to approved node UUIDs")
    diagnose.add_argument("--no-refresh", action="store_true", help="Use stored profiles without PROBE tasks")
    diagnose.add_argument("--timeout", type=float, default=180, help="Profile refresh timeout in seconds")
    diagnose.add_argument("--codex-timeout", type=float, default=600, help="Codex execution timeout in seconds")
    diagnose.add_argument("--model", help="Optional Codex model override")
    diagnose.add_argument("--output", type=Path, help="Write the structured diagnosis to JSON")
    diagnose.add_argument("--json", action="store_true", help="Print structured JSON")
    credential = admin_sub.add_parser("credential")
    credential_sub = credential.add_subparsers(dest="credential_command", required=True)
    revoke = credential_sub.add_parser("revoke")
    revoke.add_argument("node_id")
    rotate = credential_sub.add_parser("rotate")
    rotate.add_argument("node_id")

    return parser


def _print_checks(checks: list[CheckResult]) -> None:
    for check in checks:
        marker = "✓" if check.ok else ("✗" if check.blocking else "!")
        print(f"{marker} {check.name}: {check.detail}")


def _bootstrap(args: argparse.Namespace) -> int:
    from .bootstrap import Bootstrapper

    report = Bootstrapper().run(
        apply=args.apply,
        allow_docker_restart=args.allow_docker_restart,
    )
    if args.json:
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    else:
        for check in report.checks:
            marker = {
                "PASS": "✓",
                "ACTION_REQUIRED": "!",
                "BLOCKED": "✗",
            }.get(check.status, "!")
            print(f"{marker} {check.code}: {check.detail}")
        if report.actions:
            print("Planned actions:")
            for action in report.actions:
                suffix = " (restarts Docker)" if action.requires_docker_restart else ""
                print(f"- {action.action_id}: {action.description}{suffix}")
        else:
            print("Planned actions: none")
        if report.executed_actions:
            print("Executed actions: " + ", ".join(report.executed_actions))
        if not args.apply and not report.blocked and report.actions:
            print("Preview only; run sudo dure bootstrap --apply after review.")
        if report.ready:
            print("Bootstrap readiness: ready")
        elif report.blocked:
            print("Bootstrap readiness: blocked")
        else:
            print("Bootstrap readiness: changes required")
    return 1 if report.blocked else 0


def _doctor(args: argparse.Namespace) -> int:
    profile = NodeProbe().collect()
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(profile.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    if args.json:
        print(json.dumps(profile.to_dict(), indent=2, sort_keys=True))
        return 0

    role, capabilities = classify_node(profile)
    print(f"Node: {profile.node_id}")
    print(f"OS: {profile.os_name} / {profile.kernel}")
    print(f"CPU: {profile.cpu_model} ({profile.cpu_count} cores)")
    print(
        f"Memory: {profile.memory_available_mib / 1024:.1f}/{profile.memory_mib / 1024:.1f} GiB available"
    )
    print(f"Disk: {profile.disk_free_mib / 1024:.1f}/{profile.disk_total_mib / 1024:.1f} GiB free")
    if profile.gpus:
        for gpu in profile.gpus:
            print(
                f"GPU {gpu.index}: {gpu.name}, {gpu.memory_mib / 1024:.1f} GiB, "
                f"driver {gpu.driver_version}, compute {gpu.compute_capability or 'unknown'}"
            )
    else:
        print("GPU: no CUDA-capable NVIDIA GPU")
    print(
        f"Runtime: {profile.runtime.engine or 'none'}, "
        f"NVIDIA runtime={'yes' if profile.runtime.nvidia_runtime else 'no'}, "
        f"Ray={'yes' if profile.runtime.ray_available else 'no'}"
    )
    print(f"Recommended role: {role}")
    print(f"Capabilities: {', '.join(capabilities)}")
    model = recommend_local_model(profile)
    if model:
        print(f"Recommended local model: {model.model_id}")
    for issue in profile.issues:
        print(f"! {issue}")
    return 0


def _load_profiles(paths: list[Path]) -> list[NodeProfile]:
    if not paths:
        return [NodeProbe().collect()]
    return [NodeProfile.from_dict(json.loads(path.read_text(encoding="utf-8"))) for path in paths]


def _plan(args: argparse.Namespace) -> int:
    profiles = _load_profiles(args.profile)
    try:
        deployment = build_plan(
            profiles,
            model_id=args.model,
            image=args.image,
            network_interface=args.network_interface,
        )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if deployment is None:
        print("No eligible GPU deployment could be planned", file=sys.stderr)
        return 2
    write_plan(args.output, deployment)
    print(f"Wrote {deployment.model.model_id} deployment plan to {args.output}")
    print(
        f"PP={deployment.pipeline_parallel_size}, TP={deployment.tensor_parallel_size}, "
        f"world_size={deployment.world_size}"
    )
    for assignment in deployment.assignments:
        print(
            f"- {assignment.node_id}: rank {assignment.rank}, PP {assignment.pipeline_rank}, "
            f"layers {assignment.layer_start}-{assignment.layer_end}"
        )
    for warning in deployment.warnings:
        print(f"! {warning}")
    return 0


def _init(args: argparse.Namespace) -> int:
    deployment = read_plan(args.plan) if args.plan else None
    profile, plan, checks = InitOrchestrator(state_path=args.state_file).run(
        plan=deployment,
        apply=args.apply,
        accept_model_download=args.accept_model_download,
        pull=args.pull,
        allow_unpinned_image=args.allow_unpinned_image,
        replace=args.replace,
        serve=args.serve,
    )
    if args.json:
        print(
            json.dumps(
                {
                    "profile": profile.to_dict(),
                    "plan": plan.to_dict() if plan else None,
                    "checks": [check.to_dict() for check in checks],
                },
                indent=2,
                sort_keys=True,
            )
        )
    else:
        _print_checks(checks)
        print(f"State: {StateStore(args.state_file).load().phase}")
    return 0 if all(check.ok or not check.blocking for check in checks) else 1


def _status(args: argparse.Namespace) -> int:
    state = StateStore(args.state_file).load()
    if args.json:
        print(json.dumps(state.to_dict(), indent=2, sort_keys=True))
    else:
        print(f"Node: {state.node_id or 'uninitialized'}")
        print(f"Phase: {state.phase}")
        print(f"Role: {state.role or '-'}")
        print(f"Deployment: {state.deployment_id or '-'}")
        print(f"Generation: {state.generation}")
        print(f"Updated: {state.updated_at}")
        if state.detail:
            print(f"Detail: {state.detail}")
    return 0


def _verify(args: argparse.Namespace) -> int:
    plan = read_plan(args.plan)
    profile = NodeProbe().collect()
    verifier = ReadinessVerifier(engine=profile.runtime.engine or "docker")
    checks = [verifier.host_gpu(profile), verifier.container_gpu(plan), verifier.ray_cluster(plan)]
    if args.api:
        checks.append(verifier.api())
    _print_checks(checks)
    return 0 if all(check.ok for check in checks) else 1


def _join(args: argparse.Namespace) -> int:
    from .agent import join_control_plane

    try:
        result = join_control_plane(server=args.server, insecure=args.insecure)
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(f"Joined node {result['node_id']} ({result['status']})")
    print("The agent is running and waiting for central approval.")
    return 0


def _unjoin(_args: argparse.Namespace) -> int:
    from .agent import unjoin_control_plane

    try:
        result = unjoin_control_plane()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(f"Unjoined node {result['node_id']}; Dure GPU resources are released.")
    return 0


def _duration_seconds(value: str) -> int:
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    try:
        if value[-1] in units:
            return int(value[:-1]) * units[value[-1]]
        return int(value)
    except (ValueError, IndexError) as exc:
        raise ValueError(f"invalid duration: {value}") from exc


def _gpu_node_ids_from_inventory(
    inventory: dict, node_id: str | None = None
) -> list[str]:
    gpu_nodes: dict[str, dict] = {}
    for item in inventory.get("nodes", []):
        identifier = item.get("id")
        profile = item.get("profile")
        if not item.get("approved") or not isinstance(profile, dict):
            continue
        try:
            has_gpus = bool(NodeProfile.from_dict(profile).gpus)
        except (TypeError, ValueError):
            has_gpus = False
        if isinstance(identifier, str) and has_gpus:
            gpu_nodes[identifier] = item
    if node_id is not None:
        if node_id not in gpu_nodes:
            raise ValueError(
                f"unknown, pending, revoked, or non-GPU node: {node_id}"
            )
        return [node_id]
    values = sorted(gpu_nodes)
    if not values:
        raise ValueError("no approved GPU nodes are available to unjoin")
    return values


def _admin(args: argparse.Namespace) -> int:
    import os
    from .agent import resolve_join_settings
    from .http import JSONClient

    try:
        configured_server, _ = resolve_join_settings()
    except ValueError:
        configured_server = None
    server = args.server or os.environ.get("DURE_SERVER") or configured_server
    token = args.token or os.environ.get("DURE_ADMIN_TOKEN")
    if not server or not token:
        raise ValueError("--server and --token (or DURE_SERVER and DURE_ADMIN_TOKEN) are required")
    client = JSONClient(server, token)
    if args.admin_command == "nodes":
        values = client.request("GET", "/v1/admin/nodes")["nodes"]
        if args.online:
            values = [item for item in values if item["connectivity"] == "online"]
        if args.offline:
            values = [item for item in values if item["connectivity"] != "online"]
        if args.pending:
            values = [item for item in values if not item["approved"]]
        if args.json:
            print(json.dumps(values, indent=2, sort_keys=True))
        else:
            for item in values:
                availability = item["connectivity"] if item["approved"] else "pending"
                print(f"{item['id']}  {availability:7}  {item['hostname']}  {item['phase'] or '-'}")
        return 0
    if args.admin_command == "node":
        if args.node_command == "approve":
            value = client.request("POST", f"/v1/admin/nodes/{args.node_id}/approve")
            print(f"Approved node {value['node_id']}")
            return 0
        value = client.request("GET", f"/v1/admin/nodes/{args.node_id}")["node"]
        print(json.dumps(value, indent=2, sort_keys=True))
        return 0
    if args.admin_command == "unjoin":
        inventory = client.request("GET", "/v1/admin/inventory")
        node_ids = _gpu_node_ids_from_inventory(inventory, args.node_id)
        value = client.request(
            "POST",
            "/v1/admin/tasks",
            {
                "node_ids": node_ids,
                "type": "UNJOIN_NODE",
                "deployment_id": None,
                "options": {},
            },
        )
        print(json.dumps(value, indent=2, sort_keys=True))
        print(
            "Unjoin is queued. Watch tasks until every target succeeds; rebuild any "
            "remaining pipeline as a replacement deployment.",
            file=sys.stderr,
        )
        return 0 if not value["errors"] else 1
    if args.admin_command == "enrollment":
        value = client.request("POST", "/v1/admin/enrollments", {"expires_in_seconds": _duration_seconds(args.expires_in)})
        print(value["token"])
        print(f"Expires: {value['expires_at']}", file=sys.stderr)
        return 0
    if args.admin_command == "artifact-manifest":
        path = f"/v1/admin/model-artifacts/{args.artifact_id}/manifest"
        if args.artifact_manifest_command == "register":
            manifest = json.loads(args.file.read_text(encoding="utf-8"))
            if not isinstance(manifest, dict):
                raise ValueError("artifact manifest JSON must be an object")
            value = client.request("POST", path, manifest)
        else:
            value = client.request("GET", path)
        print(json.dumps(value, indent=2, sort_keys=True))
        return 0
    if args.admin_command == "artifact-cache":
        if args.artifact_cache_command == "list":
            value = client.request("GET", "/v1/admin/artifact-caches")
        else:
            path = f"/v1/admin/artifact-caches/{args.cache_id}"
            if args.artifact_cache_command == "show":
                value = client.request("GET", path)
            elif args.artifact_cache_command == "verify":
                value = client.request("GET", f"{path}/verify")
            else:
                value = client.request(
                    "POST",
                    f"{path}/quarantine",
                    {"apply": args.apply},
                )
        print(json.dumps(value, indent=2, sort_keys=True))
        return 0
    if args.admin_command == "recommendation":
        path = f"/v1/admin/deployment-recommendations/{args.recommendation_id}"
        if args.recommendation_command == "show":
            value = client.request("GET", path)
        else:
            payload = {}
            if args.previous_generation_id is not None:
                payload["previous_generation_id"] = args.previous_generation_id
            value = client.request("POST", f"{path}/accept", payload)
        print(json.dumps(value, indent=2, sort_keys=True))
        return 0
    if args.admin_command == "deployment":
        if args.deployment_command == "recommend":
            node_ids = list(
                dict.fromkeys(
                    node_id
                    for node_group in (args.nodes or [])
                    for node_id in node_group
                )
            )
            value = client.request(
                "POST",
                "/v1/admin/deployment-recommendations",
                {
                    "node_ids": node_ids,
                    "all_online": args.all_online,
                    "objective": args.objective,
                },
            )
            print(json.dumps(value, indent=2, sort_keys=True))
            return 0
        if args.deployment_command == "show":
            value = client.request(
                "GET", f"/v1/admin/deployments/{args.deployment_id}"
            )
            print(json.dumps(value, indent=2, sort_keys=True))
            return 0
        if args.deployment_command == "generations":
            value = client.request(
                "GET",
                f"/v1/admin/deployments/{args.deployment_id}/generations",
            )
            print(json.dumps(value, indent=2, sort_keys=True))
            return 0
        if args.deployment_command == "prepare":
            body = {
                "request_id": args.request_id,
                "apply": args.apply,
            }
            if args.artifact_set_digest is not None:
                body["artifact_set_digest"] = args.artifact_set_digest
            value = client.request(
                "POST",
                f"/v1/admin/deployments/{args.deployment_id}/prepare",
                body,
            )
            print(json.dumps(value, indent=2, sort_keys=True))
            return 0
        if args.deployment_command == "preparation":
            value = client.request(
                "GET",
                f"/v1/admin/deployment-preparations/{args.preparation_id}",
            )
            print(json.dumps(value, indent=2, sort_keys=True))
            return 0
        if args.deployment_command == "rollback":
            value = client.request(
                "POST",
                f"/v1/admin/deployments/{args.deployment_id}/rollback",
                {
                    "node_ids": list(dict.fromkeys(args.nodes)),
                    "apply": args.apply,
                    "serve": args.serve,
                },
            )
            print(json.dumps(value, indent=2, sort_keys=True))
            return 0
        profiles = _load_profiles(args.profile)
        plan = build_plan(profiles, model_id=args.model, image=args.image, network_interface=args.network_interface)
        if plan is None:
            raise ValueError("no eligible GPU deployment could be planned")
        value = client.request("POST", "/v1/admin/deployments", {
            "plan": plan.to_dict(), "accept_model_download": args.accept_model_download, "pull_image": args.pull,
        })
        print(value["deployment"]["id"])
        return 0
    if args.admin_command in {"apply", "start", "stop", "restart", "verify", "probe"}:
        types = {
            "apply": "APPLY_DEPLOYMENT", "start": "START_DEPLOYMENT", "stop": "STOP_DEPLOYMENT",
            "restart": "RESTART_DEPLOYMENT", "verify": "VERIFY", "probe": "PROBE",
        }
        options = {}
        if hasattr(args, "serve"):
            options["serve"] = args.serve
        if hasattr(args, "api"):
            options["api"] = args.api
        value = client.request("POST", "/v1/admin/tasks", {
            "node_ids": args.nodes, "type": types[args.admin_command],
            "deployment_id": getattr(args, "deployment_id", None), "options": options,
        })
        print(json.dumps(value, indent=2, sort_keys=True))
        return 0 if not value["errors"] else 1
    if args.admin_command == "tasks":
        while True:
            value = client.request("GET", "/v1/admin/tasks")
            print(json.dumps(value["tasks"], indent=2, sort_keys=True))
            if not args.watch:
                return 0
            time.sleep(5)
    if args.admin_command == "diagnose":
        from .diagnostics import (
            CodexDiagnoser,
            refresh_node_profiles,
            render_diagnosis,
            select_inventory_nodes,
        )

        inventory = select_inventory_nodes(
            client.request("GET", "/v1/admin/inventory"), args.nodes
        )
        refresh = None
        if not args.no_refresh:
            online = [
                item["id"] for item in inventory["nodes"] if item["connectivity"] == "online"
            ]
            refresh = refresh_node_profiles(client, online, timeout=args.timeout)
            inventory = select_inventory_nodes(
                client.request("GET", "/v1/admin/inventory"), args.nodes
            )
        inventory["refresh"] = refresh
        print(
            f"Sending hardware and runtime inventory for {len(inventory['nodes'])} node(s) "
            "to the configured Codex provider; credentials are not included.",
            file=sys.stderr,
        )
        diagnosis = CodexDiagnoser().diagnose(
            inventory, model=args.model, timeout=args.codex_timeout
        )
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(
                json.dumps(diagnosis, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
        if args.json:
            print(json.dumps(diagnosis, indent=2, sort_keys=True))
        else:
            print(render_diagnosis(diagnosis))
        return 0
    if args.admin_command == "credential":
        if args.credential_command == "rotate":
            value = client.request("POST", f"/v1/admin/nodes/{args.node_id}/credential")
            print(value["credential"])
            print("Update the agent config with this credential immediately.", file=sys.stderr)
            return 0
        client.request("POST", f"/v1/admin/nodes/{args.node_id}/revoke")
        print(f"Revoked node {args.node_id}")
        return 0
    raise ValueError("unknown admin command")


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    handlers = {
        "bootstrap": _bootstrap,
        "doctor": _doctor,
        "plan": _plan,
        "init": _init,
        "status": _status,
        "verify": _verify,
        "join": _join,
        "unjoin": _unjoin,
        "admin": _admin,
    }
    try:
        return handlers[args.command](args)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import hashlib
import re
import secrets
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dure.models import DeploymentPlan, NodeProfile

from .models import (
    AuditEvent,
    Deployment,
    EnrollmentToken,
    ModelArtifact,
    ModelRelease,
    Node,
    NodeCredential,
    NodeProfileRecord,
    PlacementProfileRecord,
    RuntimeRelease,
    Task,
    TaskStatus,
    TaskType,
    utcnow,
)


MODEL_RELEASE_TRANSITIONS = {
    "DRAFT": {"VALIDATED", "REVOKED"},
    "VALIDATED": {"ACTIVE", "REVOKED"},
    "ACTIVE": {"DEPRECATED", "REVOKED"},
    "DEPRECATED": {"REVOKED"},
    "REVOKED": set(),
}
QUANTIZATIONS = {"awq", "gptq", "fp8", "fp16", "bf16", "int8"}
GPU_ARCHITECTURES = {"ampere", "ada", "hopper", "blackwell"}
TOPOLOGIES = {"single-gpu", "pipeline"}


class RegistryConflictError(ValueError):
    pass


def secret_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def aware(value: datetime | None) -> datetime | None:
    if value is not None and value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def audit(session: Session, actor: str, action: str, target: str | None, outcome: str, **detail) -> None:
    session.add(AuditEvent(actor=actor, action=action, target=target, outcome=outcome, detail=detail))


def _require_digest(value: str, *, field: str) -> None:
    if re.fullmatch(r"sha256:[0-9a-f]{64}", value) is None:
        raise ValueError(f"{field} must be an immutable sha256 digest")


def create_model_artifact(
    session: Session,
    *,
    model_id: str,
    repository: str,
    revision: str,
    manifest_digest: str,
    quantization: str,
    size_mib: int,
    default_max_model_len: int,
    layer_count: int,
    license_id: str,
) -> ModelArtifact:
    if re.fullmatch(r"[a-z0-9][a-z0-9._-]{1,99}", model_id) is None:
        raise ValueError("invalid model_id")
    if re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository) is None:
        raise ValueError("invalid model repository")
    if re.fullmatch(r"[0-9a-f]{40,64}", revision) is None:
        raise ValueError("model revision must be an immutable commit hash")
    _require_digest(manifest_digest, field="manifest_digest")
    if quantization not in QUANTIZATIONS:
        raise ValueError("unsupported quantization")
    if min(size_mib, default_max_model_len, layer_count) <= 0:
        raise ValueError("model sizes and layer count must be positive")
    if not license_id.strip() or len(license_id) > 100:
        raise ValueError("license_id is required")
    existing = session.scalar(
        select(ModelArtifact.id).where(
            or_(
                ModelArtifact.manifest_digest == manifest_digest,
                (
                    (ModelArtifact.repository == repository)
                    & (ModelArtifact.revision == revision)
                    & (ModelArtifact.quantization == quantization)
                ),
            )
        )
    )
    if existing is not None:
        raise RegistryConflictError("model artifact already exists")
    record = ModelArtifact(
        model_id=model_id,
        repository=repository,
        revision=revision,
        manifest_digest=manifest_digest,
        quantization=quantization,
        size_mib=size_mib,
        default_max_model_len=default_max_model_len,
        layer_count=layer_count,
        license_id=license_id,
    )
    session.add(record)
    try:
        session.flush()
    except IntegrityError as exc:
        session.rollback()
        raise RegistryConflictError("model artifact already exists") from exc
    audit(session, "admin", "model_artifact.create", record.id, "success")
    session.commit()
    return record


def create_runtime_release(
    session: Session,
    *,
    version: str,
    image: str,
    vllm_version: str,
    cuda_version: str,
    gpu_architectures: list[str],
) -> RuntimeRelease:
    if (
        re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/-]*@sha256:[0-9a-f]{64}", image)
        is None
        or "/../" in image
        or "//" in image
    ):
        raise ValueError("runtime image must be OCI digest-pinned")
    if not all(isinstance(item, str) for item in gpu_architectures):
        raise ValueError("unsupported GPU architecture")
    normalized_architectures = sorted(set(gpu_architectures))
    if not normalized_architectures or not set(normalized_architectures) <= GPU_ARCHITECTURES:
        raise ValueError("unsupported GPU architecture")
    if not version.strip() or not vllm_version.strip() or not cuda_version.strip():
        raise ValueError("runtime version fields are required")
    if session.scalar(select(RuntimeRelease.id).where(RuntimeRelease.image == image)) is not None:
        raise RegistryConflictError("runtime release already exists")
    record = RuntimeRelease(
        version=version,
        image=image,
        vllm_version=vllm_version,
        cuda_version=cuda_version,
        gpu_architectures=normalized_architectures,
    )
    session.add(record)
    try:
        session.flush()
    except IntegrityError as exc:
        session.rollback()
        raise RegistryConflictError("runtime release already exists") from exc
    audit(session, "admin", "runtime_release.create", record.id, "success")
    session.commit()
    return record


def create_model_release(
    session: Session, *, artifact_id: str, runtime_id: str, quality_rank: int
) -> ModelRelease:
    if session.get(ModelArtifact, artifact_id) is None:
        raise ValueError("unknown model artifact")
    if session.get(RuntimeRelease, runtime_id) is None:
        raise ValueError("unknown runtime release")
    if quality_rank <= 0:
        raise ValueError("quality_rank must be positive")
    if session.scalar(
        select(ModelRelease.id).where(
            ModelRelease.artifact_id == artifact_id, ModelRelease.runtime_id == runtime_id
        )
    ) is not None:
        raise RegistryConflictError("model release already exists")
    record = ModelRelease(
        artifact_id=artifact_id,
        runtime_id=runtime_id,
        quality_rank=quality_rank,
        status="DRAFT",
    )
    session.add(record)
    try:
        session.flush()
    except IntegrityError as exc:
        session.rollback()
        raise RegistryConflictError("model release already exists") from exc
    audit(session, "admin", "model_release.create", record.id, "success")
    session.commit()
    return record


def add_placement_profile(
    session: Session,
    *,
    release_id: str,
    profile_id: str,
    topology: str,
    node_count: int,
    min_gpu_memory_mib: int,
    min_disk_free_mib: int,
    pipeline_parallel_size: int,
    tensor_parallel_size: int,
    requires_network_evidence: bool,
    requires_nccl: bool,
    min_bandwidth_mbps: int | None,
    max_rtt_ms: float | None,
    max_packet_loss_pct: float | None,
    max_ttft_p95_ms: float,
    max_tpot_p95_ms: float,
    max_e2e_p95_ms: float,
    min_success_rate: float,
    min_vram_headroom_pct: float,
    min_throughput_tps: float,
) -> PlacementProfileRecord:
    release = session.scalar(
        select(ModelRelease).where(ModelRelease.id == release_id).with_for_update()
    )
    if release is None:
        raise ValueError("unknown model release")
    if release.status != "DRAFT":
        raise ValueError("placement profiles can only be added to DRAFT releases")
    if re.fullmatch(r"[a-z0-9][a-z0-9._-]{1,99}", profile_id) is None:
        raise ValueError("invalid placement profile_id")
    if topology not in TOPOLOGIES:
        raise ValueError("unsupported topology")
    if min(node_count, min_gpu_memory_mib, min_disk_free_mib) <= 0:
        raise ValueError("placement resource requirements must be positive")
    if pipeline_parallel_size <= 0 or tensor_parallel_size <= 0:
        raise ValueError("parallel sizes must be positive")
    if pipeline_parallel_size * tensor_parallel_size != node_count:
        raise ValueError("parallel sizes must match node_count")
    if topology == "single-gpu" and node_count != 1:
        raise ValueError("single-gpu topology requires one node")
    network_values = (min_bandwidth_mbps, max_rtt_ms, max_packet_loss_pct)
    if node_count > 1 and (
        not requires_network_evidence
        or not requires_nccl
        or any(value is None for value in network_values)
    ):
        raise ValueError("multi-node placement requires network and NCCL thresholds")
    if requires_network_evidence and (
        min_bandwidth_mbps is None
        or min_bandwidth_mbps <= 0
        or max_rtt_ms is None
        or max_rtt_ms < 0
        or max_packet_loss_pct is None
        or not 0 <= max_packet_loss_pct <= 100
    ):
        raise ValueError("network thresholds are out of range")
    if any(value <= 0 for value in (max_ttft_p95_ms, max_tpot_p95_ms, max_e2e_p95_ms)):
        raise ValueError("latency SLO values must be positive")
    if not 0 <= min_success_rate <= 1 or not 0 <= min_vram_headroom_pct <= 100:
        raise ValueError("success and VRAM thresholds are out of range")
    if min_throughput_tps <= 0:
        raise ValueError("throughput SLO must be positive")
    if session.scalar(
        select(PlacementProfileRecord.id).where(
            PlacementProfileRecord.release_id == release_id,
            PlacementProfileRecord.profile_id == profile_id,
        )
    ) is not None:
        raise RegistryConflictError("placement profile already exists")
    record = PlacementProfileRecord(
        release_id=release_id,
        profile_id=profile_id,
        topology=topology,
        node_count=node_count,
        min_gpu_memory_mib=min_gpu_memory_mib,
        min_disk_free_mib=min_disk_free_mib,
        pipeline_parallel_size=pipeline_parallel_size,
        tensor_parallel_size=tensor_parallel_size,
        requires_network_evidence=requires_network_evidence,
        requires_nccl=requires_nccl,
        min_bandwidth_mbps=min_bandwidth_mbps,
        max_rtt_ms=max_rtt_ms,
        max_packet_loss_pct=max_packet_loss_pct,
        max_ttft_p95_ms=max_ttft_p95_ms,
        max_tpot_p95_ms=max_tpot_p95_ms,
        max_e2e_p95_ms=max_e2e_p95_ms,
        min_success_rate=min_success_rate,
        min_vram_headroom_pct=min_vram_headroom_pct,
        min_throughput_tps=min_throughput_tps,
    )
    session.add(record)
    try:
        session.flush()
    except IntegrityError as exc:
        session.rollback()
        raise RegistryConflictError("placement profile already exists") from exc
    audit(session, "admin", "placement_profile.create", record.id, "success")
    session.commit()
    return record


def transition_model_release(
    session: Session, release_id: str, target_status: str
) -> ModelRelease:
    if target_status == "ACTIVE":
        # ACTIVE is evidence-gated. Keep the existing transition API compatible
        # while routing it through the same promotion service as /promote.
        from .benchmark import promote_model_release

        release, _, _ = promote_model_release(session, release_id)
        return release
    release = session.scalar(
        select(ModelRelease).where(ModelRelease.id == release_id).with_for_update()
    )
    if release is None:
        raise ValueError("unknown model release")
    if target_status not in MODEL_RELEASE_TRANSITIONS:
        raise ValueError("unknown model release status")
    if target_status not in MODEL_RELEASE_TRANSITIONS[release.status]:
        raise ValueError(f"invalid model release transition: {release.status} -> {target_status}")
    if target_status in {"VALIDATED", "ACTIVE"}:
        placement = session.scalar(
            select(PlacementProfileRecord.id).where(
                PlacementProfileRecord.release_id == release.id
            )
        )
        if placement is None:
            raise ValueError("model release requires a placement profile")
    previous = release.status
    release.status = target_status
    release.updated_at = utcnow()
    audit(
        session,
        "admin",
        "model_release.transition",
        release.id,
        "success",
        previous=previous,
        current=target_status,
    )
    session.commit()
    return release


def create_enrollment(session: Session, expires_in: timedelta) -> tuple[EnrollmentToken, str]:
    raw = secrets.token_urlsafe(32)
    record = EnrollmentToken(token_hash=secret_hash(raw), expires_at=utcnow() + expires_in)
    session.add(record)
    audit(session, "admin", "enrollment.create", record.id, "success")
    session.commit()
    return record, raw


def claim_enrollment(
    session: Session, *, token: str, install_id: str, profile: dict, agent_version: str
) -> tuple[Node, str]:
    now = utcnow()
    record = session.scalar(
        select(EnrollmentToken).where(EnrollmentToken.token_hash == secret_hash(token)).with_for_update()
    )
    if record is None or record.used_at is not None or aware(record.expires_at) <= now:
        raise ValueError("invalid, expired, or already used enrollment token")
    parsed = NodeProfile.from_dict(profile)
    if session.scalar(select(Node).where(Node.install_id == install_id)) is not None:
        raise ValueError("installation is already enrolled")
    node = Node(
        install_id=install_id,
        display_name=parsed.hostname,
        hostname=parsed.hostname,
        agent_version=agent_version,
        approved=True,
        last_seen=now,
    )
    session.add(node)
    session.flush()
    session.add(NodeProfileRecord(node_id=node.id, profile=profile))
    raw_credential = secrets.token_urlsafe(48)
    session.add(NodeCredential(node_id=node.id, credential_hash=secret_hash(raw_credential)))
    record.used_at = now
    audit(session, f"node:{node.id}", "enrollment.claim", node.id, "success")
    session.commit()
    return node, raw_credential


def join_node(
    session: Session, *, install_id: str, profile: dict, agent_version: str
) -> tuple[Node, str]:
    """Register an unauthenticated node as pending operator approval."""
    parsed = NodeProfile.from_dict(profile)
    if session.scalar(select(Node).where(Node.install_id == install_id)) is not None:
        raise ValueError("installation is already joined")
    node = Node(
        install_id=install_id,
        display_name=parsed.hostname,
        hostname=parsed.hostname,
        agent_version=agent_version,
        approved=False,
        last_seen=utcnow(),
        observed_phase="DISCOVERED",
    )
    session.add(node)
    session.flush()
    session.add(NodeProfileRecord(node_id=node.id, profile=profile))
    raw_credential = secrets.token_urlsafe(48)
    session.add(NodeCredential(node_id=node.id, credential_hash=secret_hash(raw_credential)))
    audit(session, f"node:{node.id}", "node.join", node.id, "pending")
    session.commit()
    return node, raw_credential


def authenticate_node(session: Session, credential: str) -> Node | None:
    digest = secret_hash(credential)
    row = session.execute(
        select(NodeCredential, Node)
        .join(Node, Node.id == NodeCredential.node_id)
        .where(NodeCredential.credential_hash == digest, NodeCredential.revoked_at.is_(None))
    ).first()
    return row[1] if row else None


def approve_node(session: Session, node_id: str) -> bool:
    node = session.get(Node, node_id)
    if node is None:
        return False
    node.approved = True
    audit(session, "admin", "node.approve", node_id, "success")
    session.commit()
    return True


def revoke_node(session: Session, node_id: str) -> bool:
    node = session.get(Node, node_id)
    if node is None:
        return False
    node.approved = False
    now = utcnow()
    for credential in session.scalars(
        select(NodeCredential).where(NodeCredential.node_id == node_id, NodeCredential.revoked_at.is_(None))
    ):
        credential.revoked_at = now
    audit(session, "admin", "node.revoke", node_id, "success")
    session.commit()
    return True


def rotate_node_credential(session: Session, node_id: str) -> str | None:
    node = session.get(Node, node_id)
    if node is None:
        return None
    now = utcnow()
    for credential in session.scalars(
        select(NodeCredential).where(NodeCredential.node_id == node_id, NodeCredential.revoked_at.is_(None))
    ):
        credential.revoked_at = now
    raw = secrets.token_urlsafe(48)
    session.add(NodeCredential(node_id=node_id, credential_hash=secret_hash(raw)))
    node.approved = True
    audit(session, "admin", "node.credential.rotate", node_id, "success")
    session.commit()
    return raw


def node_status(last_seen: datetime | None, now: datetime | None = None) -> str:
    if last_seen is None:
        return "stale"
    age = (now or utcnow()) - aware(last_seen)
    if age <= timedelta(seconds=30):
        return "online"
    if age <= timedelta(seconds=90):
        return "offline"
    return "stale"


def save_heartbeat(session: Session, node: Node, state: dict, profile: dict | None = None) -> None:
    node.last_seen = utcnow()
    node.observed_phase = state.get("phase")
    node.observed_role = state.get("role")
    node.observed_deployment_id = state.get("deployment_id")
    if profile is not None:
        NodeProfile.from_dict(profile)
        record = session.get(NodeProfileRecord, node.id)
        if record is None:
            session.add(NodeProfileRecord(node_id=node.id, profile=profile))
        else:
            record.profile = profile
            record.updated_at = utcnow()
    session.commit()


def save_deployment(
    session: Session, plan_data: dict, *, accept_model_download: bool, pull_image: bool
) -> Deployment:
    plan = DeploymentPlan.from_dict(plan_data)
    if "@sha256:" not in plan.image:
        raise ValueError("central deployments require an OCI digest-pinned image")
    if not plan.assignments:
        raise ValueError("deployment has no assignments")
    # Local/legacy profiles identify nodes by hostname. Resolve those assignments
    # to stable server UUIDs when the hostname is unambiguous.
    for assignment in plan.assignments:
        if session.get(Node, assignment.node_id) is not None:
            continue
        matches = list(session.scalars(select(Node).where(Node.hostname == assignment.node_id, Node.approved.is_(True))))
        if len(matches) != 1:
            raise ValueError(f"unknown or ambiguous node assignment: {assignment.node_id}")
        assignment.node_id = matches[0].id
    if plan.ray_head_node_id not in {item.node_id for item in plan.assignments}:
        head = next((item for item in plan.assignments if item.role == "ray-head"), None)
        if head is None:
            raise ValueError("deployment has no Ray head assignment")
        plan.ray_head_node_id = head.node_id
    existing = session.get(Deployment, plan.deployment_id)
    if existing is not None:
        raise ValueError("deployment already exists")
    record = Deployment(
        id=plan.deployment_id,
        generation=plan.generation,
        plan=plan.to_dict(),
        accept_model_download=accept_model_download,
        pull_image=pull_image,
    )
    session.add(record)
    audit(session, "admin", "deployment.create", record.id, "success")
    session.commit()
    return record


def create_tasks(
    session: Session,
    *,
    node_ids: list[str],
    task_type: TaskType,
    deployment_id: str | None,
    options: dict,
) -> tuple[str, list[Task], dict[str, str]]:
    bulk_id = str(uuid.uuid4())
    tasks: list[Task] = []
    errors: dict[str, str] = {}
    deployment = session.get(Deployment, deployment_id) if deployment_id else None
    deployment_types = {
        TaskType.VERIFY,
        TaskType.APPLY_DEPLOYMENT,
        TaskType.START_DEPLOYMENT,
        TaskType.STOP_DEPLOYMENT,
        TaskType.RESTART_DEPLOYMENT,
    }
    if task_type in deployment_types and deployment is None:
        raise ValueError("a valid deployment_id is required")
    allowed_options = {"api"} if task_type == TaskType.VERIFY else {"serve"} if task_type in {
        TaskType.APPLY_DEPLOYMENT, TaskType.START_DEPLOYMENT, TaskType.RESTART_DEPLOYMENT
    } else set()
    unknown = set(options) - allowed_options
    if unknown:
        raise ValueError(f"unsupported task options: {', '.join(sorted(unknown))}")
    assignments = {item["node_id"] for item in deployment.plan["assignments"]} if deployment else set()
    for node_id in dict.fromkeys(node_ids):
        node = session.get(Node, node_id)
        if node is None or not node.approved:
            errors[node_id] = "unknown, pending, or revoked node"
            continue
        if deployment is not None and node_id not in assignments:
            errors[node_id] = "node is not assigned to deployment"
            continue
        payload = dict(options)
        if deployment is not None:
            payload.update(
                plan=deployment.plan,
                generation=deployment.generation,
                accept_model_download=deployment.accept_model_download,
                pull_image=deployment.pull_image,
            )
        task = Task(
            bulk_id=bulk_id,
            node_id=node_id,
            type=task_type.value,
            deployment_id=deployment_id,
            payload=payload,
        )
        session.add(task)
        tasks.append(task)
        node.desired_state = task_type.value
    audit(session, "admin", "tasks.create", bulk_id, "success", task_type=task_type.value, count=len(tasks))
    session.commit()
    return bulk_id, tasks, errors


def claim_task(session: Session, node_id: str, lease_seconds: int = 300) -> Task | None:
    now = utcnow()
    # Serialize claims per node before inspecting active/queued tasks. This keeps
    # multiple agent processes from leasing different mutations concurrently.
    locked_node = session.scalar(select(Node).where(Node.id == node_id).with_for_update())
    if locked_node is None:
        return None
    active = session.scalar(
        select(Task.id).where(
            Task.node_id == node_id,
            Task.status == TaskStatus.RUNNING.value,
            Task.lease_until >= now,
        ).limit(1)
    )
    if active is not None:
        return None
    task = session.scalar(
        select(Task)
        .where(
            Task.node_id == node_id,
            or_(
                Task.status == TaskStatus.QUEUED.value,
                (Task.status == TaskStatus.RUNNING.value) & (Task.lease_until < now),
            ),
        )
        .order_by(Task.created_at)
        .with_for_update(skip_locked=True)
    )
    if task is None:
        return None
    task.status = TaskStatus.RUNNING.value
    task.attempts += 1
    task.lease_until = now + timedelta(seconds=lease_seconds)
    session.commit()
    return task


def extend_task(session: Session, task: Task, node_id: str, lease_seconds: int = 300) -> bool:
    if task.node_id != node_id or task.status != TaskStatus.RUNNING.value:
        return False
    task.lease_until = utcnow() + timedelta(seconds=lease_seconds)
    node = session.get(Node, node_id)
    if node is not None:
        node.last_seen = utcnow()
    session.commit()
    return True


def finish_task(session: Session, task: Task, node_id: str, *, result: dict | None, error: str | None) -> bool:
    if task.node_id != node_id:
        return False
    terminal = {TaskStatus.SUCCEEDED.value, TaskStatus.FAILED.value}
    if task.status in terminal:
        return True
    if task.status != TaskStatus.RUNNING.value:
        return False
    task.status = TaskStatus.FAILED.value if error else TaskStatus.SUCCEEDED.value
    task.result = result
    task.error = error
    task.lease_until = None
    node = session.get(Node, node_id)
    if node is not None:
        node.desired_state = None
    if not error and task.type == TaskType.PROBE.value and result and isinstance(result.get("profile"), dict):
        NodeProfile.from_dict(result["profile"])
        profile_record = session.get(NodeProfileRecord, node_id)
        if profile_record is None:
            session.add(NodeProfileRecord(node_id=node_id, profile=result["profile"]))
        else:
            profile_record.profile = result["profile"]
            profile_record.updated_at = utcnow()
    session.commit()
    return True


def cancel_task(session: Session, task: Task) -> bool:
    if task.status != TaskStatus.QUEUED.value:
        return False
    task.status = TaskStatus.CANCELED.value
    node = session.get(Node, task.node_id)
    if node is not None:
        node.desired_state = None
    audit(session, "admin", "task.cancel", task.id, "success")
    session.commit()
    return True

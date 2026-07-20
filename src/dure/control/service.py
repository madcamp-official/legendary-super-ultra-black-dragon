from __future__ import annotations

import hashlib
import secrets
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from dure.models import DeploymentPlan, NodeProfile

from .models import (
    AuditEvent,
    Deployment,
    EnrollmentToken,
    Node,
    NodeCredential,
    NodeProfileRecord,
    Task,
    TaskStatus,
    TaskType,
    utcnow,
)


def secret_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def aware(value: datetime | None) -> datetime | None:
    if value is not None and value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def audit(session: Session, actor: str, action: str, target: str | None, outcome: str, **detail) -> None:
    session.add(AuditEvent(actor=actor, action=action, target=target, outcome=outcome, detail=detail))


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

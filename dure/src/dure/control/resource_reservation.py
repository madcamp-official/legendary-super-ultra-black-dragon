from __future__ import annotations

from typing import Any, Iterable

from sqlalchemy import or_, select, text
from sqlalchemy.orm import Session

from .models import Deployment, FleetResourceReservation, Node


class FleetResourceReservationError(ValueError):
    """활성 Fleet 예약의 소유 범위를 위반한 자원 사용 요청."""

    def __init__(
        self,
        message: str,
        *,
        code: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = details or {}


FLEET_RESERVATION_ADVISORY_LOCK_KEY = 0x44555245464C4545


def lock_fleet_reservation_gate(session: Session) -> None:
    """Fleet 수락과 다른 자원 점유 변경을 같은 순서로 직렬화한다.

    PostgreSQL에서는 Dure 자원 소유권 생성자만 공유하는 transaction-level
    advisory lock을 사용한다. task 완료·heartbeat처럼 예약을 새로 만들지
    않는 경로의 테이블 잠금과 충돌하지 않으면서, 활성 예약 검사와 새 작업
    생성 사이에 다른 소유권 생성자가 끼어들 수 없게 한다. SQLite의
    ``FOR UPDATE``는 실질적인 행 잠금이 아니지만 쓰기 트랜잭션과 부분 고유
    인덱스가 최종 충돌을 막는다.
    """

    if session.get_bind().dialect.name == "postgresql":
        session.execute(
            text("SELECT pg_advisory_xact_lock(:lock_key)"),
            {"lock_key": FLEET_RESERVATION_ADVISORY_LOCK_KEY},
        )
        return
    list(
        session.scalars(
            select(FleetResourceReservation)
            .order_by(FleetResourceReservation.id)
            .with_for_update()
        )
    )


def active_fleet_reservations(
    session: Session,
    *,
    node_ids: Iterable[str] = (),
    gpu_uuids: Iterable[str] = (),
    deployment_id: str | None = None,
    lock: bool = False,
) -> list[FleetResourceReservation]:
    """요청한 노드·GPU·배포와 겹치는 활성 Fleet 예약을 반환한다."""

    normalized_nodes = sorted(set(node_ids))
    normalized_gpus = sorted(set(gpu_uuids))
    predicates = []
    if normalized_nodes:
        predicates.append(FleetResourceReservation.node_id.in_(normalized_nodes))
    if normalized_gpus:
        predicates.append(
            FleetResourceReservation.gpu_uuid.in_(normalized_gpus)
        )
    if deployment_id is not None:
        predicates.append(
            FleetResourceReservation.deployment_id == deployment_id
        )
    if not predicates:
        return []
    statement = (
        select(FleetResourceReservation)
        .where(
            FleetResourceReservation.released_at.is_(None),
            or_(*predicates),
        )
        .order_by(
            FleetResourceReservation.node_id,
            FleetResourceReservation.gpu_uuid,
            FleetResourceReservation.id,
        )
    )
    if lock:
        statement = statement.with_for_update()
    return list(session.scalars(statement))


def active_fleet_reservations_by_node(
    session: Session,
    node_ids: Iterable[str],
) -> dict[str, FleetResourceReservation]:
    """점유 판정용으로 노드마다 결정론적인 첫 활성 예약을 반환한다."""

    reservations = active_fleet_reservations(session, node_ids=node_ids)
    result: dict[str, FleetResourceReservation] = {}
    for reservation in reservations:
        result.setdefault(reservation.node_id, reservation)
    return result


def _plan_bindings(plan: Any) -> list[dict[str, Any]]:
    assignments = plan.get("assignments") if isinstance(plan, dict) else None
    if not isinstance(assignments, list) or not assignments:
        raise FleetResourceReservationError(
            "Fleet deployment plan has no exact GPU assignments",
            code="FLEET_DEPLOYMENT_RESERVATION_INVALID",
        )
    normalized: list[dict[str, Any]] = []
    seen_nodes: set[str] = set()
    seen_gpus: set[str] = set()
    seen_ranks: set[int] = set()
    for assignment in assignments:
        if not isinstance(assignment, dict):
            raise FleetResourceReservationError(
                "Fleet deployment assignment is invalid",
                code="FLEET_DEPLOYMENT_RESERVATION_INVALID",
            )
        node_id = assignment.get("node_id")
        gpu_index = assignment.get("gpu_index")
        gpu_uuid = assignment.get("gpu_uuid")
        rank = assignment.get("rank")
        if (
            type(node_id) is not str
            or not node_id
            or type(gpu_index) is not int
            or gpu_index < 0
            or type(gpu_uuid) is not str
            or not gpu_uuid.startswith("GPU-")
            or type(rank) is not int
            or rank < 0
            or node_id in seen_nodes
            or gpu_uuid in seen_gpus
            or rank in seen_ranks
        ):
            raise FleetResourceReservationError(
                "Fleet deployment assignment is not an exact one-GPU binding",
                code="FLEET_DEPLOYMENT_RESERVATION_INVALID",
            )
        seen_nodes.add(node_id)
        seen_gpus.add(gpu_uuid)
        seen_ranks.add(rank)
        normalized.append(
            {
                "node_id": node_id,
                "gpu_index": gpu_index,
                "gpu_uuid": gpu_uuid,
                "rank": rank,
            }
        )
    normalized.sort(key=lambda item: item["rank"])
    if [item["rank"] for item in normalized] != list(range(len(normalized))):
        raise FleetResourceReservationError(
            "Fleet deployment ranks are not contiguous",
            code="FLEET_DEPLOYMENT_RESERVATION_INVALID",
        )
    return normalized


def _reservation_projection(
    reservations: Iterable[FleetResourceReservation],
) -> list[dict[str, Any]]:
    return [
        {
            "fleet_id": item.fleet_id,
            "deployment_id": item.deployment_id,
            "node_id": item.node_id,
            "gpu_index": item.gpu_index,
            "gpu_uuid": item.gpu_uuid,
            "rank": item.rank,
        }
        for item in reservations
    ]


def ensure_fleet_reservation_scope(
    session: Session,
    *,
    node_ids: Iterable[str],
    gpu_uuids: Iterable[str] = (),
    deployment: Deployment | None = None,
    plan: dict[str, Any] | None = None,
    gate_locked: bool = False,
) -> None:
    """활성 Fleet 예약과 요청 작업의 소유·계획 결합을 검증한다.

    Fleet에 속하지 않은 요청은 활성 예약 노드를 하나라도 사용할 수 없다.
    Fleet 배포 요청은 배포 계획 전체와 정확히 같은 활성 예약을 자기 Fleet와
    배포 ID로 소유해야 한다. 일부 노드 작업도 전체 예약 결합을 확인하므로
    예약 누락·해제·GPU index/UUID/rank 변조를 우회할 수 없다.
    """

    normalized_nodes = sorted(set(node_ids))
    normalized_gpus = sorted(set(gpu_uuids))
    if deployment is not None:
        source_plan = plan if plan is not None else deployment.plan
        assignments = (
            source_plan.get("assignments")
            if isinstance(source_plan, dict)
            else None
        )
        if isinstance(assignments, list):
            normalized_gpus = sorted(
                set(normalized_gpus).union(
                    item.get("gpu_uuid")
                    for item in assignments
                    if isinstance(item, dict)
                    and isinstance(item.get("gpu_uuid"), str)
                    and item["gpu_uuid"].startswith("GPU-")
                )
            )
    if not normalized_nodes and not normalized_gpus:
        return
    if not gate_locked:
        # 모든 자원 점유 변경은 gate -> ordered node rows 순서로 잠근다.
        # Fleet 수락과 독립 호출이 같은 순서를 사용해야 PostgreSQL에서
        # node-row/gate 잠금 역전이 생기지 않는다.
        lock_fleet_reservation_gate(session)
        list(
            session.scalars(
                select(Node)
                .where(Node.id.in_(normalized_nodes))
                .order_by(Node.id)
                .with_for_update()
            )
        )

    if deployment is None or deployment.fleet_id is None:
        conflicts = active_fleet_reservations(
            session,
            node_ids=normalized_nodes,
            gpu_uuids=normalized_gpus,
            lock=True,
        )
        if conflicts:
            raise FleetResourceReservationError(
                "requested node belongs to an active Fleet reservation",
                code="FLEET_RESOURCE_RESERVED",
                details={
                    "node_ids": sorted({item.node_id for item in conflicts}),
                    "gpu_uuids": sorted(
                        {item.gpu_uuid for item in conflicts}
                    ),
                    "reservations": _reservation_projection(conflicts),
                },
            )
        return

    if deployment.fleet_candidate_id is None:
        raise FleetResourceReservationError(
            "Fleet deployment identity is incomplete",
            code="FLEET_DEPLOYMENT_RESERVATION_INVALID",
            details={"deployment_id": deployment.id},
        )
    expected = _plan_bindings(plan if plan is not None else deployment.plan)
    expected_nodes = {item["node_id"] for item in expected}
    expected_gpus = {item["gpu_uuid"] for item in expected}
    requested_outside_plan = sorted(set(normalized_nodes) - expected_nodes)
    requested_outside_gpus = sorted(set(normalized_gpus) - expected_gpus)
    if requested_outside_plan or requested_outside_gpus:
        raise FleetResourceReservationError(
            "requested node or GPU is outside the Fleet deployment plan",
            code="FLEET_DEPLOYMENT_RESERVATION_INVALID",
            details={
                "deployment_id": deployment.id,
                "node_ids": requested_outside_plan,
                "gpu_uuids": requested_outside_gpus,
            },
        )

    reservations = active_fleet_reservations(
        session,
        node_ids=expected_nodes,
        gpu_uuids=expected_gpus,
        deployment_id=deployment.id,
        lock=True,
    )
    foreign = [
        item
        for item in reservations
        if item.fleet_id != deployment.fleet_id
        or item.deployment_id != deployment.id
    ]
    actual = sorted(
        (
            {
                "node_id": item.node_id,
                "gpu_index": item.gpu_index,
                "gpu_uuid": item.gpu_uuid,
                "rank": item.rank,
            }
            for item in reservations
            if item.fleet_id == deployment.fleet_id
            and item.deployment_id == deployment.id
        ),
        key=lambda item: item["rank"],
    )
    if foreign or actual != expected:
        raise FleetResourceReservationError(
            "active Fleet reservations do not match the deployment plan",
            code="FLEET_DEPLOYMENT_RESERVATION_INVALID",
            details={
                "fleet_id": deployment.fleet_id,
                "deployment_id": deployment.id,
                "expected_bindings": expected,
                "active_reservations": _reservation_projection(reservations),
            },
        )

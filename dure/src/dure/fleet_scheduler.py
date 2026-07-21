from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass
from itertools import permutations
from typing import Iterable, Mapping

from .resource_pool import FLEET_MODEL_IDS, FLEET_TENSOR_PARALLEL_SIZE


DEFAULT_MAX_CANDIDATES = 512
MAX_SAFE_RECURSIVE_CANDIDATES = 512
DEFAULT_MAX_SEARCH_STATES = 250_000


class FleetSchedulingError(ValueError):
    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


class FleetSchedulingLimitError(FleetSchedulingError):
    pass


@dataclass(frozen=True)
class FleetGpuBinding:
    node_id: str
    gpu_index: int
    gpu_uuid: str
    rank: int

    def identity(self) -> tuple[str, str, int, int]:
        return (self.node_id, self.gpu_uuid, self.gpu_index, self.rank)

    def to_dict(self) -> dict[str, object]:
        return {
            "node_id": self.node_id,
            "gpu_index": self.gpu_index,
            "gpu_uuid": self.gpu_uuid,
            "rank": self.rank,
        }


@dataclass(frozen=True)
class FleetDeploymentCandidate:
    candidate_id: str
    model_id: str
    placement_profile_id: str
    evidence_id: str
    evidence_digest: str
    bindings: tuple[FleetGpuBinding, ...]
    tensor_parallel_size: int
    pipeline_parallel_size: int
    quality_score: float
    throughput_tps: float
    cache_hit_count: int = 0
    network_zone: str | None = None
    zone_penalty: float = 0.0
    imbalance_score: float = 0.0

    @property
    def node_ids(self) -> frozenset[str]:
        return frozenset(binding.node_id for binding in self.bindings)

    @property
    def gpu_uuids(self) -> frozenset[str]:
        return frozenset(binding.gpu_uuid for binding in self.bindings)

    def identity(self) -> tuple[tuple[tuple[str, str, int, int], ...], str]:
        return (
            tuple(sorted(binding.identity() for binding in self.bindings)),
            self.candidate_id,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "model_id": self.model_id,
            "placement_profile_id": self.placement_profile_id,
            "evidence_id": self.evidence_id,
            "evidence_digest": self.evidence_digest,
            "bindings": [
                binding.to_dict()
                for binding in sorted(self.bindings, key=FleetGpuBinding.identity)
            ],
            "tensor_parallel_size": self.tensor_parallel_size,
            "pipeline_parallel_size": self.pipeline_parallel_size,
            "quality_score": self.quality_score,
            "throughput_tps": self.throughput_tps,
            "cache_hit_count": self.cache_hit_count,
            "network_zone": self.network_zone,
            "zone_penalty": self.zone_penalty,
            "imbalance_score": self.imbalance_score,
        }


@dataclass(frozen=True)
class FleetCandidateRejection:
    candidate_id: str
    code: str
    conflicting_candidate_ids: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "code": self.code,
            "conflicting_candidate_ids": list(self.conflicting_candidate_ids),
        }


@dataclass(frozen=True)
class FleetObjectiveScore:
    all_minimum_replicas_met: bool
    minimum_models_met: int
    fulfilled_minimum_replicas: int
    quality_vector: tuple[float, ...]
    quality_score: float
    throughput_tps: float
    utilized_node_count: int
    imbalance_score: float
    reserve_policy_met: bool
    reserved_nodes_left: int
    cache_hit_count: int
    zone_penalty: float

    def comparison_key(self) -> tuple[object, ...]:
        return (
            self.all_minimum_replicas_met,
            self.minimum_models_met,
            self.fulfilled_minimum_replicas,
            self.quality_vector,
            self.throughput_tps,
            self.utilized_node_count,
            -self.imbalance_score,
            self.reserve_policy_met,
            self.reserved_nodes_left,
            self.cache_hit_count,
            -self.zone_penalty,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "all_minimum_replicas_met": self.all_minimum_replicas_met,
            "minimum_models_met": self.minimum_models_met,
            "fulfilled_minimum_replicas": self.fulfilled_minimum_replicas,
            "quality_vector": list(self.quality_vector),
            "quality_score": self.quality_score,
            "throughput_tps": self.throughput_tps,
            "utilized_node_count": self.utilized_node_count,
            "imbalance_score": self.imbalance_score,
            "reserve_policy_met": self.reserve_policy_met,
            "reserved_nodes_left": self.reserved_nodes_left,
            "cache_hit_count": self.cache_hit_count,
            "zone_penalty": self.zone_penalty,
        }


@dataclass(frozen=True)
class FleetScheduleResult:
    selected: tuple[FleetDeploymentCandidate, ...]
    rejections: tuple[FleetCandidateRejection, ...]
    replica_counts: tuple[tuple[str, int], ...]
    unmet_minimum_replicas: tuple[tuple[str, int], ...]
    used_node_ids: tuple[str, ...]
    used_gpu_uuids: tuple[str, ...]
    score: FleetObjectiveScore
    explored_states: int
    search_complete: bool
    search_limit_reached: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "selected": [candidate.to_dict() for candidate in self.selected],
            "rejections": [rejection.to_dict() for rejection in self.rejections],
            "replica_counts": dict(self.replica_counts),
            "unmet_minimum_replicas": dict(self.unmet_minimum_replicas),
            "used_node_ids": list(self.used_node_ids),
            "used_gpu_uuids": list(self.used_gpu_uuids),
            "score": self.score.to_dict(),
            "explored_states": self.explored_states,
            "search_complete": self.search_complete,
            "search_limit_reached": self.search_limit_reached,
        }


def _finite_nonnegative(value: object, *, field: str) -> float:
    if type(value) not in {int, float} or not math.isfinite(value) or value < 0:
        raise FleetSchedulingError(
            f"{field} must be a finite non-negative number",
            code="FLEET_CANDIDATE_INVALID",
        )
    return float(value)


def _validate_candidate(candidate: FleetDeploymentCandidate) -> None:
    if not isinstance(candidate, FleetDeploymentCandidate):
        raise FleetSchedulingError(
            "Fleet candidates must use FleetDeploymentCandidate",
            code="FLEET_CANDIDATE_INVALID",
        )
    for field, value in (
        ("candidate_id", candidate.candidate_id),
        ("placement_profile_id", candidate.placement_profile_id),
        ("evidence_id", candidate.evidence_id),
    ):
        if type(value) is not str or not value:
            raise FleetSchedulingError(
                f"{field} must be a non-empty string",
                code="FLEET_CANDIDATE_INVALID",
            )
    if candidate.model_id not in FLEET_MODEL_IDS:
        raise FleetSchedulingError(
            f"model is outside the Fleet allowlist: {candidate.model_id}",
            code="FLEET_MODEL_NOT_ALLOWED",
        )
    if (
        type(candidate.tensor_parallel_size) is not int
        or candidate.tensor_parallel_size != FLEET_TENSOR_PARALLEL_SIZE
    ):
        raise FleetSchedulingError(
            "Fleet candidates require TP=1",
            code="FLEET_TP_UNSUPPORTED",
        )
    if (
        type(candidate.pipeline_parallel_size) is not int
        or candidate.pipeline_parallel_size < 1
        or type(candidate.bindings) is not tuple
        or candidate.pipeline_parallel_size != len(candidate.bindings)
    ):
        raise FleetSchedulingError(
            "PP must equal the exact node/GPU binding count",
            code="FLEET_BINDING_INVALID",
        )
    if (
        type(candidate.evidence_digest) is not str
        or not candidate.evidence_digest.startswith("sha256:")
        or len(candidate.evidence_digest) != 71
        or any(
            character not in "0123456789abcdef"
            for character in candidate.evidence_digest[7:]
        )
    ):
        raise FleetSchedulingError(
            "evidence_digest must be a canonical SHA-256 digest",
            code="FLEET_EVIDENCE_INVALID",
        )
    if not candidate.bindings:
        raise FleetSchedulingError(
            "a Fleet candidate requires at least one GPU binding",
            code="FLEET_BINDING_INVALID",
        )
    node_ids: set[str] = set()
    gpu_uuids: set[str] = set()
    ranks: set[int] = set()
    for binding in candidate.bindings:
        if not isinstance(binding, FleetGpuBinding):
            raise FleetSchedulingError(
                "candidate bindings must use FleetGpuBinding",
                code="FLEET_BINDING_INVALID",
            )
        if (
            type(binding.node_id) is not str
            or not binding.node_id
            or type(binding.gpu_index) is not int
            or binding.gpu_index < 0
            or type(binding.gpu_uuid) is not str
            or not binding.gpu_uuid.startswith("GPU-")
            or type(binding.rank) is not int
            or binding.rank < 0
        ):
            raise FleetSchedulingError(
                "candidate contains an invalid node/GPU/rank binding",
                code="FLEET_BINDING_INVALID",
            )
        if binding.node_id in node_ids or binding.gpu_uuid in gpu_uuids:
            raise FleetSchedulingError(
                "a candidate may bind exactly one unique GPU per unique node",
                code="FLEET_BINDING_DUPLICATE",
            )
        node_ids.add(binding.node_id)
        gpu_uuids.add(binding.gpu_uuid)
        ranks.add(binding.rank)
    if ranks != set(range(len(candidate.bindings))):
        raise FleetSchedulingError(
            "candidate ranks must be contiguous from zero",
            code="FLEET_BINDING_INVALID",
        )
    _finite_nonnegative(candidate.quality_score, field="quality_score")
    _finite_nonnegative(candidate.throughput_tps, field="throughput_tps")
    _finite_nonnegative(candidate.zone_penalty, field="zone_penalty")
    _finite_nonnegative(candidate.imbalance_score, field="imbalance_score")
    if (
        type(candidate.cache_hit_count) is not int
        or not 0 <= candidate.cache_hit_count <= len(candidate.bindings)
    ):
        raise FleetSchedulingError(
            "cache_hit_count must be within the binding count",
            code="FLEET_CANDIDATE_INVALID",
        )
    if candidate.network_zone is not None and (
        type(candidate.network_zone) is not str or not candidate.network_zone
    ):
        raise FleetSchedulingError(
            "network_zone must be a non-empty string when present",
            code="FLEET_CANDIDATE_INVALID",
        )


def _minimum_score(
    counts: Mapping[str, int], minimum_replicas: Mapping[str, int]
) -> tuple[bool, int, int]:
    if not minimum_replicas:
        return True, 0, 0
    models_met = sum(
        counts.get(model_id, 0) >= minimum
        for model_id, minimum in minimum_replicas.items()
    )
    fulfilled = sum(
        min(counts.get(model_id, 0), minimum)
        for model_id, minimum in minimum_replicas.items()
    )
    return models_met == len(minimum_replicas), models_met, fulfilled


def _objective_score(
    *,
    counts: Mapping[str, int],
    minimum_replicas: Mapping[str, int],
    quality_scores: tuple[float, ...],
    throughput_tps: float,
    used_node_ids: set[str],
    imbalance_score: float,
    available_node_count: int,
    minimum_reserve_nodes: int,
    reserve_node_ids: frozenset[str],
    cache_hit_count: int,
    zone_penalty: float,
) -> FleetObjectiveScore:
    all_met, models_met, fulfilled = _minimum_score(counts, minimum_replicas)
    unused_count = available_node_count - len(used_node_ids)
    reserved_nodes_left = len(reserve_node_ids.difference(used_node_ids))
    reserve_met = (
        unused_count >= minimum_reserve_nodes
        and reserved_nodes_left == len(reserve_node_ids)
    )
    return FleetObjectiveScore(
        all_minimum_replicas_met=all_met,
        minimum_models_met=models_met,
        fulfilled_minimum_replicas=fulfilled,
        quality_vector=tuple(sorted(quality_scores, reverse=True)),
        quality_score=sum(quality_scores),
        throughput_tps=throughput_tps,
        utilized_node_count=len(used_node_ids),
        imbalance_score=imbalance_score,
        reserve_policy_met=reserve_met,
        reserved_nodes_left=reserved_nodes_left,
        cache_hit_count=cache_hit_count,
        zone_penalty=zone_penalty,
    )


def schedule_fleet(
    candidates: Iterable[FleetDeploymentCandidate],
    *,
    minimum_replicas: Mapping[str, int] | None = None,
    available_node_ids: Iterable[str] | None = None,
    minimum_reserve_nodes: int = 0,
    reserve_node_ids: Iterable[str] = (),
    max_candidates: int = DEFAULT_MAX_CANDIDATES,
    max_search_states: int = DEFAULT_MAX_SEARCH_STATES,
) -> FleetScheduleResult:
    """Select a deterministic, resource-disjoint Fleet with bounded search.

    Cluster node count is deliberately unbounded. Candidate cardinality and
    explored search states are bounded instead because exact set packing is
    exponential in the number of overlapping candidates. The lexicographic
    objective is minimum replicas, quality, throughput, utilization, lower
    imbalance, reserve policy, cache/zone locality, then node/GPU identity.
    """

    normalized = list(candidates)
    if (
        type(max_candidates) is not int
        or not 1 <= max_candidates <= MAX_SAFE_RECURSIVE_CANDIDATES
    ):
        raise ValueError(
            "max_candidates must be within the safe recursive search limit"
        )
    if type(max_search_states) is not int or max_search_states < 1:
        raise ValueError("max_search_states must be a positive integer")
    if len(normalized) > max_candidates:
        raise FleetSchedulingLimitError(
            f"candidate count {len(normalized)} exceeds limit {max_candidates}",
            code="FLEET_CANDIDATE_LIMIT",
        )
    ids = [candidate.candidate_id for candidate in normalized]
    if len(ids) != len(set(ids)):
        raise FleetSchedulingError(
            "candidate IDs must be unique",
            code="FLEET_CANDIDATE_DUPLICATE",
        )
    for candidate in normalized:
        _validate_candidate(candidate)
    evidence_identities: dict[
        str,
        tuple[str, str, tuple[tuple[str, str, int, int], ...]],
    ] = {}
    for candidate in normalized:
        identity = (
            candidate.evidence_digest,
            candidate.placement_profile_id,
            tuple(sorted(binding.identity() for binding in candidate.bindings)),
        )
        previous = evidence_identities.setdefault(candidate.evidence_id, identity)
        if previous != identity:
            raise FleetSchedulingError(
                "one evidence ID cannot authorize different profile/node/GPU bindings",
                code="FLEET_EVIDENCE_CONFLICT",
            )
    # Search the strongest candidates first so the first complete solution is
    # a useful lower bound.  The final equality tie-break still uses only the
    # canonical node/GPU identity, so input order cannot affect the result.
    normalized.sort(
        key=lambda candidate: (
            -candidate.quality_score,
            -candidate.throughput_tps,
            candidate.imbalance_score,
            -candidate.cache_hit_count,
            candidate.zone_penalty,
            candidate.identity(),
        )
    )

    requirements = dict(minimum_replicas or {})
    for model_id, minimum in requirements.items():
        if model_id not in FLEET_MODEL_IDS:
            raise FleetSchedulingError(
                f"minimum replica model is outside the allowlist: {model_id}",
                code="FLEET_MODEL_NOT_ALLOWED",
            )
        if type(minimum) is not int or minimum < 0:
            raise FleetSchedulingError(
                "minimum replica counts must be non-negative integers",
                code="FLEET_POLICY_INVALID",
            )
    requirements = {
        model_id: requirements[model_id]
        for model_id in sorted(requirements)
        if requirements[model_id] > 0
    }

    candidate_node_ids = set().union(
        *(candidate.node_ids for candidate in normalized)
    ) if normalized else set()
    if available_node_ids is None:
        available = candidate_node_ids
    else:
        available_list = list(available_node_ids)
        if (
            any(type(node_id) is not str or not node_id for node_id in available_list)
            or len(available_list) != len(set(available_list))
        ):
            raise FleetSchedulingError(
                "available node IDs must be unique non-empty strings",
                code="FLEET_POLICY_INVALID",
            )
        available = set(available_list)
        outside = sorted(candidate_node_ids.difference(available))
        if outside:
            raise FleetSchedulingError(
                "candidate bindings reference nodes outside the available pool: "
                + ", ".join(outside),
                code="FLEET_BINDING_OUTSIDE_POOL",
            )
    if (
        type(minimum_reserve_nodes) is not int
        or minimum_reserve_nodes < 0
        or minimum_reserve_nodes > len(available)
    ):
        raise FleetSchedulingError(
            "minimum_reserve_nodes must fit within the available pool",
            code="FLEET_POLICY_INVALID",
        )
    reserve_list = list(reserve_node_ids)
    if (
        any(type(node_id) is not str or not node_id for node_id in reserve_list)
        or len(reserve_list) != len(set(reserve_list))
    ):
        raise FleetSchedulingError(
            "reserve node IDs must be unique non-empty strings",
            code="FLEET_POLICY_INVALID",
        )
    reserve_nodes = frozenset(reserve_list)
    if not reserve_nodes.issubset(available):
        raise FleetSchedulingError(
            "reserve node IDs must belong to the available pool",
            code="FLEET_POLICY_INVALID",
        )

    size = len(normalized)
    conflict_degrees = [0] * size
    if requirements:
        for left in range(size):
            for right in range(left + 1, size):
                if normalized[left].node_ids.intersection(
                    normalized[right].node_ids
                ) or normalized[left].gpu_uuids.intersection(
                    normalized[right].gpu_uuids
                ):
                    conflict_degrees[left] += 1
                    conflict_degrees[right] += 1
    suffix_counts: list[Counter[str]] = [Counter() for _ in range(size + 1)]
    suffix_quality: list[tuple[float, ...]] = [() for _ in range(size + 1)]
    suffix_throughput = [0.0] * (size + 1)
    suffix_nodes = [0] * (size + 1)
    suffix_cache = [0] * (size + 1)
    for index in range(size - 1, -1, -1):
        candidate = normalized[index]
        suffix_counts[index] = suffix_counts[index + 1].copy()
        suffix_counts[index][candidate.model_id] += 1
        suffix_quality[index] = (
            candidate.quality_score,
            *suffix_quality[index + 1],
        )
        suffix_throughput[index] = (
            suffix_throughput[index + 1] + candidate.throughput_tps
        )
        suffix_nodes[index] = suffix_nodes[index + 1] + len(candidate.bindings)
        suffix_cache[index] = suffix_cache[index + 1] + candidate.cache_hit_count

    best_indexes: tuple[int, ...] | None = None
    best_score: FleetObjectiveScore | None = None
    best_identity: tuple[
        tuple[tuple[str, str, int, int], ...], tuple[str, ...]
    ] | None = None
    explored_states = 0
    search_limit_reached = False

    def selection_identity(indexes: tuple[int, ...]):
        selected_candidates = [normalized[index] for index in indexes]
        return (
            tuple(
                sorted(
                    binding.identity()
                    for candidate in selected_candidates
                    for binding in candidate.bindings
                )
            ),
            tuple(sorted(candidate.candidate_id for candidate in selected_candidates)),
        )

    def consider(indexes: Iterable[int]) -> None:
        nonlocal best_indexes, best_score, best_identity
        normalized_indexes = tuple(sorted(set(indexes)))
        selected_candidates = [normalized[index] for index in normalized_indexes]
        counts = Counter(candidate.model_id for candidate in selected_candidates)
        used_nodes = {
            node_id
            for candidate in selected_candidates
            for node_id in candidate.node_ids
        }
        score = _objective_score(
            counts=counts,
            minimum_replicas=requirements,
            quality_scores=tuple(
                candidate.quality_score for candidate in selected_candidates
            ),
            throughput_tps=sum(
                candidate.throughput_tps for candidate in selected_candidates
            ),
            used_node_ids=used_nodes,
            imbalance_score=sum(
                candidate.imbalance_score for candidate in selected_candidates
            ),
            available_node_count=len(available),
            minimum_reserve_nodes=minimum_reserve_nodes,
            reserve_node_ids=reserve_nodes,
            cache_hit_count=sum(
                candidate.cache_hit_count for candidate in selected_candidates
            ),
            zone_penalty=sum(
                candidate.zone_penalty for candidate in selected_candidates
            ),
        )
        identity = selection_identity(normalized_indexes)
        if (
            best_score is None
            or score.comparison_key() > best_score.comparison_key()
            or (
                score.comparison_key() == best_score.comparison_key()
                and (best_identity is None or identity < best_identity)
            )
        ):
            best_indexes = normalized_indexes
            best_score = score
            best_identity = identity

    def greedy_selection(
        requirement_order: tuple[str, ...],
        *,
        conflict_first: bool = False,
    ) -> tuple[int, ...]:
        selected: list[int] = []
        used_nodes: set[str] = set()
        used_gpus: set[str] = set()
        counts: Counter[str] = Counter()

        def add(index: int) -> bool:
            candidate = normalized[index]
            if candidate.node_ids.intersection(
                used_nodes
            ) or candidate.gpu_uuids.intersection(used_gpus):
                return False
            selected.append(index)
            used_nodes.update(candidate.node_ids)
            used_gpus.update(candidate.gpu_uuids)
            counts[candidate.model_id] += 1
            return True

        # Minimum replicas dominate every later objective.  Prefer smaller
        # placements while satisfying them so one large placement does not
        # needlessly consume slots that could satisfy several replicas.
        for model_id in requirement_order:
            needed = requirements[model_id]
            model_indexes = sorted(
                (
                    index
                    for index, candidate in enumerate(normalized)
                    if candidate.model_id == model_id
                ),
                key=lambda index: (
                    (
                        conflict_degrees[index]
                        if conflict_first
                        else len(normalized[index].bindings)
                    ),
                    (
                        len(normalized[index].bindings)
                        if conflict_first
                        else conflict_degrees[index]
                    ),
                    normalized[index].imbalance_score,
                    -normalized[index].throughput_tps,
                    normalized[index].identity(),
                ),
            )
            for index in model_indexes:
                if counts[model_id] >= needed:
                    break
                add(index)

        # Fill the remaining pool in the canonical quality-first order.
        for index in range(size):
            add(index)
        return tuple(selected)

    # Always keep at least one deterministic feasible answer.  Exact search
    # below is an improvement pass, so a dense overlapping candidate graph
    # cannot turn a usable Fleet into a scheduling error merely by exhausting
    # the configured search-state budget.
    consider(greedy_selection(()))
    if requirements:
        for requirement_order in permutations(tuple(sorted(requirements))):
            consider(greedy_selection(requirement_order))
            consider(
                greedy_selection(
                    requirement_order,
                    conflict_first=True,
                )
            )

    def search(
        index: int,
        selected_indexes: tuple[int, ...],
        used_nodes: set[str],
        used_gpus: set[str],
        counts: Counter[str],
        quality_scores: tuple[float, ...],
        throughput: float,
        imbalance: float,
        cache_hits: int,
        zone_penalty: float,
    ) -> None:
        nonlocal explored_states, search_limit_reached
        if search_limit_reached:
            return
        if explored_states >= max_search_states:
            search_limit_reached = True
            return
        explored_states += 1

        if best_score is not None:
            optimistic_counts = counts + suffix_counts[index]
            optimistic_nodes = set(used_nodes)
            offset = 0
            while len(optimistic_nodes) < len(used_nodes) + suffix_nodes[index]:
                synthetic = f"\x00fleet-optimistic-{offset}"
                offset += 1
                if synthetic not in available and synthetic not in reserve_nodes:
                    optimistic_nodes.add(synthetic)
            optimistic = _objective_score(
                counts=optimistic_counts,
                minimum_replicas=requirements,
                quality_scores=quality_scores + suffix_quality[index],
                throughput_tps=throughput + suffix_throughput[index],
                used_node_ids=optimistic_nodes,
                imbalance_score=imbalance,
                available_node_count=max(
                    len(available), len(used_nodes) + suffix_nodes[index]
                ),
                minimum_reserve_nodes=minimum_reserve_nodes,
                reserve_node_ids=reserve_nodes,
                cache_hit_count=cache_hits + suffix_cache[index],
                zone_penalty=zone_penalty,
            )
            if optimistic.comparison_key() < best_score.comparison_key():
                return

        if index == size:
            consider(selected_indexes)
            return

        candidate = normalized[index]
        if (
            not candidate.node_ids.intersection(used_nodes)
            and not candidate.gpu_uuids.intersection(used_gpus)
        ):
            next_counts = counts.copy()
            next_counts[candidate.model_id] += 1
            search(
                index + 1,
                (*selected_indexes, index),
                used_nodes | set(candidate.node_ids),
                used_gpus | set(candidate.gpu_uuids),
                next_counts,
                (*quality_scores, candidate.quality_score),
                throughput + candidate.throughput_tps,
                imbalance + candidate.imbalance_score,
                cache_hits + candidate.cache_hit_count,
                zone_penalty + candidate.zone_penalty,
            )
        search(
            index + 1,
            selected_indexes,
            used_nodes,
            used_gpus,
            counts,
            quality_scores,
            throughput,
            imbalance,
            cache_hits,
            zone_penalty,
        )

    search(0, (), set(), set(), Counter(), (), 0.0, 0.0, 0, 0.0)
    assert best_indexes is not None and best_score is not None
    selected = tuple(normalized[index] for index in best_indexes)
    selected_ids = {candidate.candidate_id for candidate in selected}
    selected_by_node = {
        node_id: candidate.candidate_id
        for candidate in selected
        for node_id in candidate.node_ids
    }
    selected_by_gpu = {
        gpu_uuid: candidate.candidate_id
        for candidate in selected
        for gpu_uuid in candidate.gpu_uuids
    }
    rejections = []
    for candidate in normalized:
        if candidate.candidate_id in selected_ids:
            continue
        conflicts = sorted(
            {
                selected_by_node[node_id]
                for node_id in candidate.node_ids
                if node_id in selected_by_node
            }
            | {
                selected_by_gpu[gpu_uuid]
                for gpu_uuid in candidate.gpu_uuids
                if gpu_uuid in selected_by_gpu
            }
        )
        rejections.append(
            FleetCandidateRejection(
                candidate_id=candidate.candidate_id,
                code=("RESOURCE_CONFLICT" if conflicts else "OBJECTIVE_NOT_SELECTED"),
                conflicting_candidate_ids=tuple(conflicts),
            )
        )
    counts = Counter(candidate.model_id for candidate in selected)
    unmet = tuple(
        (model_id, minimum - counts.get(model_id, 0))
        for model_id, minimum in requirements.items()
        if counts.get(model_id, 0) < minimum
    )
    used_nodes = tuple(
        sorted(node_id for candidate in selected for node_id in candidate.node_ids)
    )
    used_gpus = tuple(
        sorted(gpu_uuid for candidate in selected for gpu_uuid in candidate.gpu_uuids)
    )
    return FleetScheduleResult(
        selected=selected,
        rejections=tuple(rejections),
        replica_counts=tuple(sorted(counts.items())),
        unmet_minimum_replicas=unmet,
        used_node_ids=used_nodes,
        used_gpu_uuids=used_gpus,
        score=best_score,
        explored_states=explored_states,
        search_complete=not search_limit_reached,
        search_limit_reached=search_limit_reached,
    )

from __future__ import annotations

from pathlib import Path

from .command import Runner, SubprocessRunner
from .models import CheckResult, DeploymentPlan, NodeProfile
from .pipeline_runtime import (
    STAGE_CACHE_CHECK,
    is_stage_pipeline_plan,
    is_strict_pipeline_plan,
    validate_strict_pipeline_node,
    validate_strict_pipeline_plan,
    validate_strict_stage_cache,
)
from .planner import build_plan, classify_node
from .probe import NodeProbe
from .readiness import ReadinessVerifier
from .runtime import ContainerRuntime, ModelStore
from .state import NodeState, StateStore


class InitOrchestrator:
    def __init__(
        self,
        *,
        runner: Runner | None = None,
        state_path: Path | None = None,
        node_id: str | None = None,
    ) -> None:
        self.runner = runner or SubprocessRunner()
        self.store = StateStore(state_path)
        self.probe = NodeProbe(self.runner)
        self.node_id = node_id

    def run(
        self,
        *,
        plan: DeploymentPlan | None = None,
        apply: bool = False,
        accept_model_download: bool = False,
        pull: bool = False,
        allow_unpinned_image: bool = False,
        replace: bool = False,
        serve: bool = False,
    ) -> tuple[NodeProfile, DeploymentPlan | None, list[CheckResult]]:
        if plan is not None:
            plan.validate_execution_contract()
            if is_strict_pipeline_plan(plan):
                # Reject central runtime inputs before state, model, image, or
                # container mutations occur on the host.
                validate_strict_pipeline_plan(plan)
        checks: list[CheckResult] = []
        state = NodeState(phase="PROBING")
        self.store.save(state)

        profile = self.probe.collect()
        if self.node_id is not None:
            profile.node_id = self.node_id
        role, capabilities = classify_node(profile)
        state.node_id = profile.node_id
        state.role = role
        state.phase = "ELIGIBLE"
        state.detail = ", ".join(capabilities)
        self.store.save(state)

        checks.append(
            CheckResult(
                "node-profile",
                True,
                f"Classified {profile.node_id} as {role}: {', '.join(capabilities)}",
            )
        )

        if plan is None:
            plan = build_plan([profile])
        if plan is None:
            state.phase = "READY" if role == "utility" else "ELIGIBLE"
            state.detail = "Utility node is initialized; controller enrollment is not configured"
            self.store.save(state)
            checks.append(CheckResult("deployment-plan", True, state.detail, blocking=False))
            return profile, None, checks

        assignment = plan.assignment_for(profile.node_id)
        if assignment is None:
            state.phase = "FAILED"
            state.detail = f"Node {profile.node_id} is not assigned in deployment plan"
            self.store.save(state)
            checks.append(CheckResult("assignment", False, state.detail))
            return profile, plan, checks

        if is_strict_pipeline_plan(plan):
            try:
                validate_strict_pipeline_node(
                    plan, assignment, profile, require_model_cache=True
                )
            except ValueError as exc:
                checks.append(CheckResult("deployment-plan", False, str(exc)))
                return self._fail(state, profile, plan, checks, str(exc))

        state.deployment_id = plan.deployment_id
        state.generation = plan.generation
        state.role = assignment.role
        state.phase = "PLANNED"
        state.detail = f"Assigned PP rank {assignment.pipeline_rank}"
        self.store.save(state)
        checks.append(
            CheckResult(
                "deployment-plan",
                True,
                f"{plan.model.model_id}, PP={plan.pipeline_parallel_size}, "
                f"rank={assignment.pipeline_rank}, layers={assignment.layer_start}-{assignment.layer_end}",
            )
        )

        if not apply:
            checks.append(
                CheckResult(
                    "apply",
                    True,
                    "Dry run complete; rerun with --apply to mutate this node",
                    blocking=False,
                )
            )
            return profile, plan, checks

        if "@sha256:" not in plan.image and not allow_unpinned_image:
            state.phase = "FAILED"
            state.detail = "Unpinned container image refused"
            self.store.save(state)
            checks.append(
                CheckResult(
                    "image-policy",
                    False,
                    "Refusing unpinned image; use an OCI digest or --allow-unpinned-image",
                )
            )
            return profile, plan, checks

        if not profile.runtime.engine_ready:
            state.phase = "FAILED"
            state.detail = "Container runtime is unavailable"
            self.store.save(state)
            checks.append(CheckResult("container-runtime", False, state.detail))
            return profile, plan, checks
        if not profile.runtime.nvidia_runtime:
            state.phase = "FAILED"
            state.detail = "NVIDIA container runtime is unavailable"
            self.store.save(state)
            checks.append(CheckResult("nvidia-runtime", False, state.detail))
            return profile, plan, checks

        state.phase = "DOWNLOADING"
        self.store.save(state)
        if is_stage_pipeline_plan(plan):
            try:
                stage_cache = validate_strict_stage_cache(plan, assignment)
            except ValueError as exc:
                model_check = CheckResult(STAGE_CACHE_CHECK, False, str(exc))
            else:
                assert stage_cache is not None
                model_check = CheckResult(
                    STAGE_CACHE_CHECK,
                    True,
                    "Verified immutable rank-local STAGE cache "
                    f"{stage_cache.cache_identity_digest}",
                )
        else:
            model_check = ModelStore(self.runner).ensure(
                plan, accept_download=accept_model_download
            )
        checks.append(model_check)
        if not model_check.ok:
            return self._fail(state, profile, plan, checks, model_check.detail)

        runtime = ContainerRuntime(self.runner, profile.runtime.engine or "docker")
        image_check = runtime.ensure_image(plan.image, pull=pull)
        checks.append(image_check)
        if not image_check.ok:
            return self._fail(state, profile, plan, checks, image_check.detail)

        state.phase = "STARTING"
        self.store.save(state)
        ray_check = runtime.start_ray(profile, plan, assignment, replace=replace)
        checks.append(ray_check)
        if not ray_check.ok:
            return self._fail(state, profile, plan, checks, ray_check.detail)

        state.phase = "VERIFYING"
        self.store.save(state)
        verifier = ReadinessVerifier(
            self.runner,
            profile.runtime.engine or "docker",
            node_id=profile.node_id,
        )
        for check in (
            verifier.host_gpu(profile),
            verifier.container_gpu(plan, assignment),
        ):
            checks.append(check)
            if not check.ok:
                return self._fail(state, profile, plan, checks, check.detail)

        if is_strict_pipeline_plan(plan):
            cluster_check = verifier.wait_pipeline_rank_contract(
                plan, assignment, profile, require_actors=False
            )
            if not cluster_check.ok:
                checks.append(cluster_check)
                return self._fail(
                    state, profile, plan, checks, cluster_check.detail
                )
        else:
            cluster_check = verifier.ray_cluster(plan)
            checks.append(cluster_check)
            if not cluster_check.ok:
                cluster_check.blocking = False
                state.phase = "WAITING_FOR_PEERS"
                state.detail = cluster_check.detail
                self.store.save(state)
                return profile, plan, checks

        if serve and assignment.role == "ray-head":
            api_start = runtime.start_api(plan, assignment, replace=replace)
            checks.append(api_start)
            if not api_start.ok:
                return self._fail(state, profile, plan, checks, api_start.detail)
            api_ready = verifier.wait_api(plan=plan)
            checks.append(api_ready)
            if not api_ready.ok:
                return self._fail(state, profile, plan, checks, api_ready.detail)
            if is_strict_pipeline_plan(plan):
                cluster_check = verifier.wait_pipeline_rank_contract(
                    plan, assignment, profile, require_actors=True
                )
                if not cluster_check.ok:
                    checks.append(cluster_check)
                    return self._fail(
                        state, profile, plan, checks, cluster_check.detail
                    )

        if is_strict_pipeline_plan(plan):
            checks.append(cluster_check)

        state.phase = "READY"
        state.detail = "Node and Ray deployment passed readiness checks"
        self.store.save(state)
        return profile, plan, checks

    def _fail(
        self,
        state: NodeState,
        profile: NodeProfile,
        plan: DeploymentPlan,
        checks: list[CheckResult],
        detail: str,
    ) -> tuple[NodeProfile, DeploymentPlan, list[CheckResult]]:
        state.phase = "FAILED"
        state.detail = detail
        self.store.save(state)
        return profile, plan, checks

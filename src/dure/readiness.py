from __future__ import annotations

import json
import time
import urllib.error
import urllib.request

from .command import Runner, SubprocessRunner
from .models import CheckResult, DeploymentPlan, NodeProfile
from .runtime import ContainerRuntime


class ReadinessVerifier:
    def __init__(
        self,
        runner: Runner | None = None,
        engine: str = "docker",
        *,
        node_id: str | None = None,
    ) -> None:
        self.runner = runner or SubprocessRunner()
        self.engine = engine
        self.node_id = node_id

    def _container_identity(
        self, plan: DeploymentPlan, name: str, check_name: str
    ) -> tuple[CheckResult | None, str]:
        if self.node_id is None:
            return None, name
        check, identity = ContainerRuntime(
            self.runner, self.engine
        ).running_container_identity(
            name,
            deployment_id=plan.deployment_id,
            generation=plan.generation,
            node_id=self.node_id,
            check_name=check_name,
        )
        return check, identity.container_id if identity is not None else name

    def host_gpu(self, profile: NodeProfile) -> CheckResult:
        if not profile.gpus:
            return CheckResult("host-gpu", False, "No NVIDIA GPU detected")
        result = self.runner.run(
            [
                "nvidia-smi",
                "--query-gpu=index,name,driver_version,memory.total",
                "--format=csv,noheader",
            ],
            timeout=15,
        )
        return CheckResult(
            "host-gpu",
            result.ok,
            result.stdout if result.ok else result.stderr or result.stdout,
        )

    def container_gpu(self, plan: DeploymentPlan) -> CheckResult:
        name = f"dure-ray-{plan.deployment_id}"
        identity, container_reference = self._container_identity(
            plan, name, "container-gpu"
        )
        if identity is not None and not identity.ok:
            return identity
        code = (
            "import json,torch; "
            "assert torch.cuda.is_available(); "
            "x=torch.ones((256,256),device='cuda'); "
            "y=x@x; torch.cuda.synchronize(); "
            "print(json.dumps({'gpu':torch.cuda.get_device_name(0),'value':float(y[0,0])}))"
        )
        result = self.runner.run(
            [self.engine, "exec", container_reference, "python3", "-c", code],
            timeout=60,
        )
        return CheckResult(
            "container-gpu",
            result.ok,
            result.stdout if result.ok else result.stderr or result.stdout,
        )

    def ray_cluster(self, plan: DeploymentPlan) -> CheckResult:
        name = f"dure-ray-{plan.deployment_id}"
        identity, container_reference = self._container_identity(
            plan, name, "ray-cluster"
        )
        if identity is not None and not identity.ok:
            return identity
        code = (
            "import json,ray; "
            f"ray.init(address='{plan.ray_head_address}',logging_level='ERROR'); "
            "print(json.dumps(ray.cluster_resources(),sort_keys=True)); ray.shutdown()"
        )
        result = self.runner.run(
            [self.engine, "exec", container_reference, "python3", "-c", code],
            timeout=45,
        )
        if not result.ok:
            return CheckResult("ray-cluster", False, result.stderr or result.stdout)
        try:
            resources = json.loads(result.stdout.splitlines()[-1])
        except (json.JSONDecodeError, IndexError):
            return CheckResult("ray-cluster", False, f"Invalid Ray resource response: {result.stdout}")
        gpu_count = float(resources.get("GPU", 0))
        ok = gpu_count >= plan.world_size
        return CheckResult(
            "ray-cluster",
            ok,
            f"GPU resources: {gpu_count:g}/{plan.world_size}; {json.dumps(resources, sort_keys=True)}",
        )

    def api(
        self,
        url: str = "http://127.0.0.1:8000",
        *,
        plan: DeploymentPlan | None = None,
    ) -> CheckResult:
        if plan is not None:
            name = f"dure-api-{plan.deployment_id}"
            identity, _ = self._container_identity(plan, name, "vllm-api")
            if identity is not None and not identity.ok:
                return identity
        try:
            with urllib.request.urlopen(f"{url}/health", timeout=10) as response:
                if not 200 <= response.status < 300:
                    return CheckResult("vllm-api", False, f"HTTP {response.status} from /health")
            with urllib.request.urlopen(f"{url}/v1/models", timeout=10) as response:
                payload = json.loads(response.read().decode("utf-8"))
                models = payload.get("data", [])
                ok = 200 <= response.status < 300 and bool(models)
                detail = (
                    f"HTTP {response.status}; models="
                    f"{','.join(str(item.get('id', '?')) for item in models)}"
                )
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            return CheckResult("vllm-api", False, str(exc))
        except (json.JSONDecodeError, AttributeError, TypeError) as exc:
            return CheckResult("vllm-api", False, f"Invalid /v1/models response: {exc}")
        return CheckResult("vllm-api", ok, detail)

    def wait_api(
        self,
        url: str = "http://127.0.0.1:8000",
        *,
        plan: DeploymentPlan | None = None,
        timeout: float = 600,
        interval: float = 5,
    ) -> CheckResult:
        deadline = time.monotonic() + timeout
        last = CheckResult("vllm-api", False, "API has not been checked")
        while time.monotonic() < deadline:
            last = self.api(url, plan=plan)
            if last.ok:
                return last
            time.sleep(interval)
        return CheckResult(
            "vllm-api",
            False,
            f"API was not ready within {timeout:g}s; last error: {last.detail}",
        )

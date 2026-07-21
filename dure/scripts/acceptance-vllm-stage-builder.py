#!/usr/bin/env python3
"""수동 GPU 환경에서만 실행하는 vLLM stage export/load acceptance harness.

기본 실행은 항상 NOT_RUN입니다. 실제 GPU·Ray·vLLM 0.9.0 builder 이미지에서
`DURE_RUN_STAGE_GPU_ACCEPTANCE=1`과 `DURE_STAGE_ACCEPTANCE_LOAD=1`을 모두
명시해야 source canonical manifest를 검증하고 호스트 자원과 출력 경로를 사용합니다.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from dure.stage_artifact import (
    PINNED_VLLM_VERSION,
    StageExportContract,
    VLLM_NATIVE_LOAD_FORMAT,
    build_stage_artifact_set,
    verify_stage_artifact_set,
)


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"필수 환경 변수 {name}가 없습니다")
    return value


def _emit(status: str, **detail: object) -> None:
    print(
        json.dumps(
            {"status": status, **detail},
            sort_keys=True,
            ensure_ascii=False,
        )
    )


def main() -> int:
    if os.environ.get("DURE_RUN_STAGE_GPU_ACCEPTANCE") != "1":
        _emit(
            "NOT_RUN",
            reason=(
                "DURE_RUN_STAGE_GPU_ACCEPTANCE=1이 아니므로 "
                "GPU stage export/load 검증을 실행하지 않았습니다."
            ),
        )
        return 77
    if os.environ.get("DURE_STAGE_ACCEPTANCE_LOAD") != "1":
        _emit(
            "NOT_RUN",
            reason=(
                "DURE_STAGE_ACCEPTANCE_LOAD=1이 아니므로 export와 native "
                "load를 하나의 acceptance로 실행하지 않았습니다."
            ),
        )
        return 77
    started = False
    export_succeeded = False
    load_succeeded = False
    try:
        source = Path(_required("DURE_STAGE_ACCEPTANCE_SOURCE"))
        source_manifest_path = Path(
            _required("DURE_STAGE_ACCEPTANCE_SOURCE_MANIFEST")
        )
        output = Path(_required("DURE_STAGE_ACCEPTANCE_OUTPUT"))
        pipeline_size = int(_required("DURE_STAGE_ACCEPTANCE_PP"))
        if pipeline_size != 1:
            _emit(
                "NOT_RUN",
                reason=(
                    "PR5 native load acceptance는 PP=1만 지원합니다. "
                    "PP>1 rank 경로 결합은 후속 loader acceptance 범위입니다."
                ),
            )
            return 77
        contract = StageExportContract(
            source_manifest_digest=_required(
                "DURE_STAGE_ACCEPTANCE_SOURCE_MANIFEST_DIGEST"
            ),
            runtime_image=_required("DURE_STAGE_RUNTIME_IMAGE"),
            exporter_build_digest=_required(
                "DURE_STAGE_EXPORTER_BUILD_DIGEST"
            ),
            pipeline_parallel_size=pipeline_size,
        )
        import importlib.metadata

        try:
            observed = importlib.metadata.version("vllm")
        except importlib.metadata.PackageNotFoundError as exc:
            raise ValueError("pinned vLLM이 설치되지 않았습니다") from exc
        if observed != PINNED_VLLM_VERSION:
            raise ValueError(
                f"vLLM 버전 불일치: {observed} != {PINNED_VLLM_VERSION}"
            )
        try:
            source_manifest = json.loads(
                source_manifest_path.read_text(encoding="utf-8")
            )
        except (OSError, UnicodeError, ValueError) as exc:
            raise ValueError("source canonical manifest를 읽을 수 없습니다") from exc
        if type(source_manifest) is not dict:
            raise ValueError("source canonical manifest는 JSON 객체여야 합니다")
        started = True
        built = build_stage_artifact_set(
            source,
            output,
            contract,
            source_manifest,
        )
        verified = verify_stage_artifact_set(
            output,
            expected_contract=contract,
            expected_index_digest=built.index_digest,
        )
        export_succeeded = True
        from vllm import LLM, SamplingParams
        llm = LLM(
            model=str(output / "stages" / "0"),
            tokenizer=str(output / "stages" / "0"),
            load_format=VLLM_NATIVE_LOAD_FORMAT,
            quantization="awq",
            tensor_parallel_size=1,
            pipeline_parallel_size=1,
            trust_remote_code=False,
            enable_lora=False,
            enforce_eager=True,
        )
        prompt = os.environ.get(
            "DURE_STAGE_ACCEPTANCE_PROMPT", "대한민국의 수도는"
        )
        outputs = llm.generate(
            [prompt],
            SamplingParams(temperature=0.0, max_tokens=4),
        )
        if not outputs or not outputs[0].outputs:
            raise RuntimeError("native sharded_state load 추론 결과가 없습니다")
        load_succeeded = True
        if not export_succeeded or not load_succeeded:
            raise RuntimeError("export와 native load 검증이 모두 완료되지 않았습니다")
        _emit(
            "PASSED",
            index_digest=verified.index_digest,
            ranks=[stage.rank for stage in verified.stages],
            generated_token_count=len(outputs[0].outputs[0].token_ids),
        )
        return 0
    except Exception as exc:
        if not started:
            _emit("NOT_RUN", reason=str(exc))
            return 77
        code = getattr(exc, "code", "STAGE_GPU_ACCEPTANCE_FAILED")
        print(
            json.dumps(
                {"status": "FAILED", "code": code, "message": str(exc)},
                sort_keys=True,
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

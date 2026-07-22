# Dure

Dure는 신뢰된 사설망의 GPU 노드를 조사·등록하고, 검증된 모델 배포를 준비·적용·검증하는 Linux CLI,
Agent, 선택형 중앙 Control Plane입니다. 공개 추론 gateway나 누구나 참여하는 GPU 네트워크는 아직
제공하지 않습니다.

## 현재 지원 범위

- 지원 모델: `qwen2.5-7b-awq`, `qwen2.5-14b-awq`, `qwen2.5-32b-awq`, `qwen2.5-72b-awq`
- 단일 GPU: `TP=1`, `PP=1`; Ray 없이 vLLM API container를 사용
- 다중 노드: `VLLM_RAY_PP_V1`, vLLM 0.9.0 V0 Ray, `TP=1`, `PP=2/3`, 노드당 정확히 GPU 한 장
- 다중 노드 GPU/NCCL 수용 결과: 현재 `NOT_RUN`; runbook과 실제 증적을 혼동하면 안 됨
- host bootstrap: Ubuntu 22.04·24.04, Docker와 NVIDIA Container Toolkit을 명시적으로 준비하지만
  NVIDIA driver·커널·CUDA host package는 자동 변경하지 않음

정확한 모델·VRAM·디스크·OS·런타임 제약은 [지원 매트릭스](docs/support-matrix.md)를 기준으로 합니다.

## 빠른 시작

개발 환경에서는 다음처럼 설치합니다.

```bash
cd /path/to/legendary-super-ultra-black-dragon/dure
python3 -m pip install -e '.[test]'
```

GPU 노드에서는 먼저 변경 없는 진단을 실행합니다.

```bash
dure doctor --json
dure bootstrap
```

운영자가 결과를 검토한 뒤에만 host 준비를 명시적으로 적용합니다.

```bash
sudo dure bootstrap --apply
```

노드는 `dure join` 뒤 pending 상태로 등록되며, 중앙 운영자가 승인하기 전에는 task를 받지 못합니다.
명령별 권한·preview·`--apply` 조건은 [CLI 명령 참조](docs/cli-reference.md)를 따릅니다.

## 운영 원칙

- Controller는 node에 inbound SSH를 요구하지 않으며, Agent가 outbound 연결로 작업을 claim합니다.
- 추천과 수락만으로 모델 다운로드, image pull, Docker 실행, 기존 배포 변경은 일어나지 않습니다.
- 중앙 배포에는 OCI digest-pinned image와 server-issued node UUID가 필요합니다.
- Agent task는 닫힌 enum만 허용하며 임의 shell, Docker argument, URL, mount, host path를 받지 않습니다.
- 실제 GPU·NCCL 검증 없이 `NOT_RUN`을 `PASSED`로 표현하지 않습니다.

## 문서 시작점

| 목적 | 문서 |
| --- | --- |
| 설정·환경 변수·파일 권한 | [설정 참조서](docs/configuration-reference.md) |
| 로컬·관리자 명령의 변경 범위 | [CLI 명령 참조](docs/cli-reference.md) |
| 모델·GPU·TP/PP 지원 계약 | [지원 매트릭스](docs/support-matrix.md) |
| Controller, Agent, 네트워크 운영 | [운영 절차 허브](docs/operations.md) |
| 배포·Fleet·준비·롤백 | [운영 절차](docs/operations.md), [지원 매트릭스](docs/support-matrix.md) |
| 모델 artifact·cache·stage | [아티팩트 배포 계약](docs/artifact-distribution.md), [STAGE 아티팩트](docs/stage-artifacts.md) |
| 실제 GPU 수용 검증과 기록 | [릴리스 수용 검증](docs/release-validation.md), [릴리스 증적](docs/release-evidence/README.md) |
| 보안·개인정보·외부 API 경계 | [보안 모델](docs/security.md), [개인정보 정책](docs/data-privacy.md), [외부 API 경계](docs/external-inference-boundary.md) |
| 전체 문서 | [문서 색인](docs/README.md) |

사용자 관점의 변경 사항은 [변경 이력](CHANGELOG.md), package·APT authority는 [APT 배포](docs/apt-distribution.md)와
[릴리스 권한](docs/release-governance.md)에서 확인합니다.

## 검증

```bash
cd dure
python3 -m compileall -q src tests
python3 -m unittest discover -v
python3 scripts/check_docs.py
git diff --check
```

실제 GPU·Docker·Ray·vLLM·NCCL 검증은 unit test와 별도이며, 실행한 결과만
`docs/release-evidence/`에 `PASSED`, `FAILED`, `NOT_RUN`으로 기록합니다.

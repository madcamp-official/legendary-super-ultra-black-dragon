# Dure 0.4.23 작동 시연 보고서

기준일: 2026-07-22

## 한눈에 보는 판정

Dure 0.4.23의 **로컬 조사·자원 기반 계획·무변경 미리보기·상태 저장·실패 안전 검증**과
**Controller/Agent 계약**은 현재 소스에서 정상 동작했다. 현재 검증 노드에서는 실제 RTX 3090과
Docker/NVIDIA/Ray 환경을 읽기 전용으로 탐지했고, 남은 디스크까지 고려해 실행 가능한 14B 단일 GPU
계획을 만들었다. 적용하지 않은 계획을 검증했을 때는 존재하지 않는 컨테이너를 정확히 감지해 준비 완료로
오인하지 않았다.

다만 이 보고서는 실제 모델 컨테이너를 시작하거나 추론 요청을 보낸 결과가 아니다. 3노드
Ray/vLLM/NCCL 수용 검사는 명시적 opt-in과 승인된 격리 노드가 없어 `NOT_RUN`이다. 따라서 현재 판정은
다음과 같다.

| 검증 층 | 판정 | 의미 |
| --- | --- | --- |
| 현재 Python 소스·계약 | `PASSED` | 767개 테스트가 모두 통과함 |
| 로컬 GPU 조사와 계획 | `PASSED` | 실제 호스트의 GPU·runtime·용량을 읽고 결정론적 계획을 생성함 |
| 무변경 시연과 실패 안전성 | `PASSED` | dry-run은 컨테이너를 만들지 않았고 미적용 배포 검증은 실패함 |
| SQLite migration·wheel build | `PASSED` | 새 DB migration과 0.4.23 wheel 생성에 성공함 |
| 실제 단일 GPU 모델 load·추론 | `NOT_RUN` | 이 보고서에서는 `--apply`하지 않음 |
| 실제 3노드 Ray/vLLM/NCCL | `NOT_RUN` | 한 노드만 사용했고 수용 harness에 opt-in하지 않음 |

## 검증 대상

| 항목 | 값 |
| --- | --- |
| 브랜치 | `version/0.4.23` |
| 소스 commit | `4018af870e93fb7f1137448cf18bf3e059a3fef3` |
| Dure version | `0.4.23` |
| Python | `3.10.12` |
| OS·architecture | Linux 5.15, `x86_64` |
| 실행 방식 | source checkout, `PYTHONPATH=src`로 현재 소스 고정 |

`PYTHONPATH=src`를 사용한 이유는 이 호스트에 이전 Dure package가 함께 설치되어 있기 때문이다.
source checkout에서 경로를 고정하지 않은 Python은 `/usr/lib/python3/dist-packages/dure`를 먼저 읽을 수
있다. 아래 명령은 현재 commit을 검증했다는 사실을 명확히 하기 위해 import 대상을 고정한다. editable
설치가 완료된 독립 virtual environment에서는 `dure`와 `python3 -m unittest`를 그대로 사용하면 된다.

## 실제 시연 결과

시연은 다음 흐름으로 수행했다.

```text
doctor ──> 실제 자원 profile ──> plan ──> init dry-run ──> PLANNED
                                                        │
                                                        └── verify → 미적용 상태를 정확히 거부
```

### 1. 호스트 조사

```bash
PYTHONPATH=src python3 -m dure.cli doctor \
  --json --output /tmp/dure-demo-profile.json
```

민감하거나 호스트를 고유 식별할 수 있는 값을 제외한 관측 요약은 다음과 같다.

| 관측 항목 | 결과 |
| --- | --- |
| GPU | NVIDIA GeForce RTX 3090, 24,576 MiB, compute capability 8.6, 정상 |
| Docker | 탐지됨, engine ready |
| NVIDIA runtime | 탐지됨 |
| Ray | 탐지됨, 2.56.1 |
| CPU·가용 메모리 | 40 cores, 44,824 MiB |
| 가용 디스크 | 12,880 MiB |
| Dure model inventory | 3개 |
| 실행 중 LLM workload | 0개 |
| cache scan | complete |
| 진단 issue | swap disabled 1건 |

`doctor`의 표준 출력과 `--output` 파일은 동일한 JSON이었다. 조사 단계는 Docker나 모델 상태를
변경하지 않았다.

### 2. 자원 기반 배포 계획

시연용 image 값은 형식 검증을 보여주기 위한 비운영 placeholder이며 pull하거나 실행하지 않았다.

```bash
PYTHONPATH=src python3 -m dure.cli plan \
  --profile /tmp/dure-demo-profile.json \
  --model auto \
  --image registry.example/vllm@sha256:<64-hex-digest> \
  --output /tmp/dure-demo-plan.json
```

결과는 다음과 같았다.

```text
Wrote qwen2.5-14b-awq deployment plan to /tmp/dure-demo-plan.json
PP=1, TP=1, world_size=1
- <node>: rank 0, PP 0, layers 0-47
```

24 GiB GPU만 보면 32B도 후보가 될 수 있지만, 현재 가용 디스크 12,880 MiB는 지원 매트릭스의 32B
최소 25 GiB를 충족하지 않는다. 계획기는 GPU만 보고 무리한 모델을 고르지 않고, 최소 12 GiB 디스크를
요구하는 14B를 선택했다. 이 결과는 Dure의 핵심 가치인 **실제 자원 제약을 반영한 실행 가능 계획**을
직접 보여준다.

### 3. 변경 없는 초기화 미리보기와 상태

```bash
PYTHONPATH=src python3 -m dure.cli init \
  --plan /tmp/dure-demo-plan.json \
  --state-file /tmp/dure-demo-state.json --json

PYTHONPATH=src python3 -m dure.cli status \
  --state-file /tmp/dure-demo-state.json --json
```

`init`은 종료 코드 0으로 다음 세 검사를 통과했다.

- `node-profile`: 노드를 `gpu-worker`로 분류하고 capability를 제시함
- `deployment-plan`: `qwen2.5-14b-awq`, `PP=1`, rank 0, layers 0-47을 확인함
- `apply`: `Dry run complete; rerun with --apply to mutate this node`라고 명시함

저장된 상태는 `phase=PLANNED`, `role=ray-head`, `generation=1`이었다. `--apply`를 주지 않았으므로
모델 다운로드, image pull, 컨테이너 시작은 발생하지 않았다.

### 4. 미적용 상태의 실패 안전 검증

```bash
PYTHONPATH=src python3 -m dure.cli verify \
  --plan /tmp/dure-demo-plan.json
```

결과는 의도한 대로 종료 코드 1이었다.

```text
✓ host-gpu: 0, NVIDIA GeForce RTX 3090, 610.43.02, 24576 MiB
✗ container-gpu: ... No such container: <expected-dure-container>
✓ ray-cluster: Ray is not required for the direct single-GPU runtime
```

즉, host GPU가 정상이라는 사실과 배포 컨테이너가 준비됐다는 사실을 혼동하지 않았다. 또한 단일 GPU
`PP=1` 경로에서는 Ray를 불필요하게 요구하지 않았다. 이 실패는 제품 오류가 아니라 아직 적용하지 않은
계획을 `READY`로 판정하지 않는 안전 장치의 정상 동작이다.

## 자동 검증 결과

### 소스와 계약 테스트

```bash
PYTHONPATH=src python3 -m compileall -q src tests
PYTHONPATH=src python3 -m unittest discover -v
```

결과:

```text
Ran 767 tests in 132.382s
OK
```

테스트는 GPU나 Docker daemon을 실제 변경하지 않고 `FakeRunner`, SQLite, FastAPI test client를
사용한다. 다음 정상·거부 경로를 함께 다룬다.

- node 조사, 모델 선택, 단일 GPU와 `PP=2/3` 계획 계약
- pending node 승인 전 task claim 차단과 닫힌 task enum
- artifact manifest, 재개 가능한 chunk download, cache 검증·격리
- 추천, qualification, Fleet 예약·스케줄링, 준비·적용·검증
- 부분 실패, stale lease, 늦은 완료, 멱등 재시도와 명시적 rollback
- exact deployment/generation/node label이 다른 컨테이너 조작 거부
- migration upgrade/downgrade 안전 조건과 append-only event 보호
- 실제 GPU 수용 harness의 opt-in, 입력 계약, 실패 코드와 결과 redaction

이 테스트 결과는 코드와 제어 계약이 일관된다는 강한 증거지만 실제 GPU model load, NCCL 통신이나
PostgreSQL 부하 시험을 대신하지 않는다.

### migration·package 재현성

다음 추가 검증도 통과했다.

```bash
PYTHONPATH=src python3 -m dure.server \
  --database-url sqlite:////tmp/<isolated-directory>/control.db --migrate
python3 scripts/check_version_sync.py
python3 -m pip wheel . --no-deps --no-build-isolation -w /tmp/<wheel-directory>
```

- 빈 SQLite DB를 migration head까지 올리는 데 성공함
- `pyproject.toml`, `setup.py`, runtime, Debian changelog version이 모두 `0.4.23`으로 일치함
- `dure-0.4.23-py3-none-any.whl` 생성에 성공함

### 실제 분산 수용 harness

안전한 기본값도 별도로 확인했다.

```bash
PYTHONPATH=src python3 scripts/acceptance-vllm-ray-pp.py
PYTHONPATH=src python3 scripts/acceptance-vllm-stage-ray-pp.py
```

두 명령 모두 종료 코드 77과 다음 의미의 구조화된 결과를 반환했다.

```json
{"code":"OPT_IN_REQUIRED","status":"NOT_RUN"}
```

승인된 실제 GPU 환경과 명시적 환경 변수가 없으면 heavy GPU 검사를 몰래 시작하지 않는 것이 정상
동작이다. 이 결과는 `PASSED`가 아니며 실제 3×24 GiB `PP=3` 수용 증적으로 사용할 수 없다.

## 발표용 5분 시연 순서

1. `dure --version`으로 0.4.23을 확인한다.
2. `dure doctor --json`으로 GPU, Docker, NVIDIA runtime, Ray와 현재 issue를 보여준다.
3. `dure plan --model auto`를 실행해 GPU뿐 아니라 디스크 제약 때문에 14B가 선택되는 이유를 설명한다.
4. `dure init`을 `--apply` 없이 실행하고 `dure status --json`의 `PLANNED`를 보여준다.
5. `dure verify`가 미적용 컨테이너를 정확히 거부하는 모습을 보여준다.
6. 마지막으로 767개 테스트 통과와 실제 GPU 수용 검사의 `NOT_RUN` 경계를 함께 제시한다.

운영 node에서 실제 적용을 시연하려면 별도 격리 환경, 실제 digest-pinned image와 model manifest,
충분한 디스크, 운영자 승인이 필요하다. 그때만 `--apply`를 사용하고 [릴리스 수용 검증](release-validation.md)에
따라 model load·최소 추론·API readiness 결과를 새 증적으로 기록한다.

## 결론

현재 Dure는 “GPU가 보인다”는 수준을 넘어, 자원을 조사하고 실행 가능한 모델을 선택하며, 적용 전에는
호스트를 바꾸지 않고, 실제 배포가 없으면 준비 완료를 거부하는 핵심 운영 흐름을 일관되게 수행한다.
Controller/Agent/Fleet/artifact/rollback 계약도 현재 소스의 767개 자동 테스트에서 통과했다.

따라서 **로컬 계획과 제어면 소프트웨어 시연에는 준비됨**으로 판정한다. 반면 **실제 모델 추론 성능과
3노드 분산 안정성은 아직 이 보고서의 증거 범위 밖**이므로, 공개 성능 주장이나 production 승인 전에
별도의 `PASSED` 수용 기록이 필요하다.

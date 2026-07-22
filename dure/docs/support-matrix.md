# 지원 매트릭스

이 문서는 현재 Dure가 실제로 허용하는 운영 계약의 요약입니다. `DRAFT` profile, stage manifest를
표현할 수 있는 DB schema, 실제 runtime 배포 가능 범위는 서로 다릅니다. 실행 전에 중앙 registry의
정확한 identity와 최신 qualification evidence를 다시 확인합니다.

## 호스트·패키지

| 항목 | 현재 지원 | 제한 |
| --- | --- | --- |
| host bootstrap | Ubuntu 22.04·24.04, `amd64`·`arm64` | NVIDIA host driver는 이미 설치돼 있어야 하며 Dure가 설치·변경하지 않음 |
| APT package mirror | `amd64` | 현재 mirror는 `arm64` package를 게시하지 않음 |
| GPU runtime | Docker Engine 20.10 이상, NVIDIA Container Toolkit | rootless·원격 Docker와 안전하지 않은 설정은 bootstrap이 자동 수정하지 않음 |
| node GPU 배정 | 선택 노드당 정확히 한 GPU | GPU index와 UUID를 함께 고정하며 같은 노드의 다른 GPU는 이 계획에서 사용하지 않음 |
| Fleet 규모 | 제품 노드 수 하드 상한 없음 | 후보 512개, 탐색 상태 250,000개는 운영 안전 한도 |

## 허용 모델과 기준 배치

현재 Fleet allowlist는 다음 네 Qwen2.5 Instruct AWQ 모델뿐입니다. 표의 디스크 값은 최소 여유
공간이며 실제 `STAGE` 소비는 rank별 manifest와 cache identity의 exact gate를 추가로 통과해야 합니다.

| 모델 ID | 기준 구성 | 최소 GPU VRAM | 최소 여유 디스크 | checkpoint |
| --- | --- | ---: | ---: | ---: |
| `qwen2.5-7b-awq` | 1 node, `TP=1`, `PP=1` | 8 GiB | 6 GiB | 4.8 GiB |
| `qwen2.5-14b-awq` | 1 node, `TP=1`, `PP=1` | 12 GiB | 12 GiB | 9.5 GiB |
| `qwen2.5-32b-awq` | 1 node, `TP=1`, `PP=1` | 24 GiB | 25 GiB | 19.5 GiB |
| `qwen2.5-72b-awq` | 3 nodes, node당 1 GPU, `TP=1`, `PP=3` | node당 24 GiB | node당 50 GiB | 38.74 GiB |

자동 placement profile 생성기는 72B에 대해 다음 **qualification 초안**도 만들 수 있습니다.

| 72B 초안 | node당 최소 VRAM | node당 profile 최소 디스크 | 상태 |
| --- | ---: | ---: | --- |
| `TP=1`, `PP=1` | 48 GiB | 50 GiB | `DRAFT`, 실제 evidence 전 배포 불가 |
| `TP=1`, `PP=2` | 24 GiB | 50 GiB | `DRAFT`, exact 2-node evidence 전 배포 불가 |
| `TP=1`, `PP=3` | 24 GiB | 20 GiB | `DRAFT`, rank별 stage bytes·cache gate와 exact 3-node evidence 전 배포 불가 |

`PP=3` 초안의 20 GiB는 profile 생성기의 최소치입니다. 기준 3×24GiB 운영 구성의 50 GiB나
rank별 실제 stage cache 요구량을 대체하지 않습니다.

## 분산 runtime과 검증

| 항목 | 현재 계약 | 의미 |
| --- | --- | --- |
| 단일 GPU | `PP=1`, `TP=1`, Ray 없이 vLLM API container 직접 실행 | 7B·14B·32B 기준 경로 |
| 엄격한 다중 노드 | `VLLM_RAY_PP_V1`, vLLM 0.9.0 V0 Ray, `TP=1`, `PP=2/3` | node당 GPU 한 장, UUID·RFC1918 주소·rank가 고정됨 |
| stage 형식 | schema는 `PP=1~64` identity를 표현 가능 | 형식의 상한일 뿐 현재 runtime이 임의 PP를 실행한다는 뜻이 아님 |
| 다중 노드 자격 | exact node·GPU·runtime·profile·inventory 결합의 최신 network/NCCL evidence | 다른 노드 조합이나 추정 VRAM만으로 통과할 수 없음 |
| 실제 GPU 수용 | 3×24GiB `PP=3` runbook 제공 | 기본은 `NOT_RUN(77)`이며 실제 `PASSED` 기록 전에는 장기 안정성 증거가 아님 |

모든 profile은 `DRAFT → QUALIFYING → VALIDATED → ACTIVE`를 따릅니다. `VALIDATED` stage variant도
노드 설치나 실행 완료를 뜻하지 않으며, 선택 뒤 `prepare`와 `apply`·전체 노드 검증이 별도로 필요합니다.

## 갱신 규칙

모델 allowlist, 최소 VRAM·디스크, OS·architecture, runtime version, TP/PP 실행 범위가 바뀌면
이 문서와 [모델 선택 정책](model-selection.md), [운영 절차](operations.md), [릴리스 수용 검증](release-validation.md)을 같은 변경에서 함께 갱신합니다. 실제 환경에서 얻은 결과는 [릴리스 증적 기록](release-evidence/README.md)에 남깁니다.

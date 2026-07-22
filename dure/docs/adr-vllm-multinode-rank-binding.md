# ADR: vLLM 다중 노드 pipeline rank 결합

- 상태: **채택**
- 결정일: 2026-07-21
- 적용 범위: Dure 0.3.18의 다중 노드 실행 계약
- 새 backend 식별자: `VLLM_RAY_PP_V1`

## 배경

Dure의 기존 `RAY` 실행 경로는 모든 GPU 노드에 `FULL_SNAPSHOT`을 두고 Ray와
vLLM을 시작합니다. 계획에는 node UUID와 pipeline rank가 있지만, vLLM이 실제로
만든 worker의 runtime rank가 그 계획과 같다는 폐쇄형 증적은 없었습니다. 계획의
rank와 실제 rank가 바뀌면 이후 rank별 `STAGE`를 연결할 때 다른 노드의 가중치를
읽거나 일부 rank를 중복하는 심각한 오류가 될 수 있습니다.

vLLM 0.9.0의 native multiprocessing executor는 단일 호스트용입니다. V0 구현은
프로세스 초기화 주소를 loopback에 두며, 공식 분산 실행 문서도 여러 노드에서는
Ray를 사용하도록 안내합니다. `external_launcher`는 이 버전에서 일반 온라인 API
배포 계약이 아니라 실험적 offline 경로입니다. 따라서 이름만 `mp`인 별도 실행기를
만들거나 최신 vLLM의 옵션을 0.9.0에 역으로 적용해서는 안 됩니다.

## 결정

Dure의 새 폐쇄형 다중 노드 backend를 `VLLM_RAY_PP_V1`로 정합니다. 이 backend는
기존 legacy `RAY` 경로와 별도 계약이며 다음 조합만 허용합니다.

| 항목 | 고정 값 |
|---|---|
| vLLM | 정확히 `0.9.0` |
| executor | 공식 Ray distributed executor |
| engine | V0, `VLLM_USE_V1=0` |
| Ray SPMD worker | 사용 안 함 |
| tensor parallel | `TP=1` |
| pipeline parallel | 실제 수용 검사 범위 `PP=2` 또는 `PP=3` |
| GPU 배치 | node UUID당 정확히 GPU 한 장, worker 한 개 |
| 모델 입력 | 검증된 `FULL_SNAPSHOT` AWQ의 고정 `/models/model` mount |
| runtime | OCI SHA-256 digest로 고정한 동일 이미지 |
| driver | 계획의 pipeline rank 0 node |
| 네트워크 | 서로 다른 사설 IPv4 주소를 가진 신뢰 LAN 또는 사설 overlay |

Ray head는 세대별 root 소유 디렉터리를 컨테이너의 `/tmp/ray`에 연결하고
`ray start --temp-dir=/tmp/ray`로 session metadata 위치를 고정합니다. 별도 vLLM API
컨테이너도 head와 같은 디렉터리를 `/tmp/ray`에 연결합니다. 따라서 GCS 주소만 공유한
별도 컨테이너가 head의 `node_ip_address.json`을 찾지 못하는 상태를 허용하지 않으며,
이 mount도 strict runtime contract digest에 포함됩니다. worker의 로컬 Ray 임시
디렉터리는 API 연결 계약에 사용하지 않습니다.

`VLLM_RAY_PP_V1`이라는 이름은 “Ray를 사용한다”는 사실만 나타내지 않습니다.
버전, V0/V1, TP·PP, node 수, driver, node UUID·주소·rank 순서, 이미지와 모델
identity를 함께 고정한 계약입니다. 이 중 하나라도 다르면 같은 backend로 인정하지
않습니다.

0.3.18에서는 계속 각 노드의 검증된 `FULL_SNAPSHOT`을 load합니다. rank별
`STAGE`를 준비하고 `sharded_state`로 load하는 연결은 후속 변경입니다. 따라서 이
결정만으로 모델 파일이 노드별로 작아지거나 `VALIDATED` stage variant가 자동으로
배포되지는 않습니다.

## 결정론적 rank 규칙

vLLM 0.9.0의 Ray executor는 placement group에 actor를 만든 뒤 worker를 다시
정렬합니다. 공식 소스의 정렬 키는 다음 순서입니다.

1. driver와 같은 IP의 worker를 먼저 둡니다.
2. worker가 더 적은 node를 먼저 둡니다.
3. 마지막으로 IP **문자열** 오름차순으로 정렬합니다.

이 ADR은 node마다 GPU와 worker를 정확히 하나만 허용하므로 두 번째 조건은 모두
같습니다. 결과적으로 runtime global rank 순서는 다음처럼 단순해집니다.

```text
rank 0 = vLLM driver node
rank 1..N-1 = 나머지 node의 사설 IPv4 문자열 오름차순
```

일반적인 global rank 공식은 다음과 같습니다.

```text
global_rank = pipeline_rank * tensor_parallel_size + tensor_rank
```

`TP=1`에서는 `tensor_rank=0`이므로 `global_rank == pipeline_rank`입니다. Dure는
계획을 만들 때 다음 불변 목록을 저장하고, 시작 직전에 같은 목록을 다시 계산합니다.

```json
{
  "ordered_bindings": [
    {
      "node_id": "11111111-1111-4111-8111-111111111111",
      "runtime_address": "10.0.0.2",
      "pipeline_rank": 0,
      "runtime_rank": 0
    },
    {
      "node_id": "22222222-2222-4222-8222-222222222222",
      "runtime_address": "10.0.0.3",
      "pipeline_rank": 1,
      "runtime_rank": 1
    }
  ]
}
```

제출 순서로 rank를 정하거나 hostname을 node identity로 사용하지 않습니다. 중앙
배포에서는 서버가 발급한 canonical node UUID가 identity이고, 주소는 해당 세대의
검증된 profile에 결합된 실행 입력입니다. 중복 UUID·주소·rank, 빠진 rank, rank 0과
다른 driver, 공인·loopback·link-local 주소는 시작 전에 거부합니다.

## 실행 전·실행 후 검증 경계

프로덕션 Agent·controller 경로와 별도 GPU 수용 검사 경로의 증명 범위를 구분합니다.

프로덕션 경로는 먼저 저장 계획의 세대·모델·이미지·2/3노드 계약과 최신 profile의
GPU·Docker NVIDIA runtime·기본 interface 주소·검증된 캐시를 검사합니다. 이미지와
캐시는 컨테이너 시작 전에 확인하지만, 실제 container CUDA 접근과 고정 port의 사용
가능성은 Ray 컨테이너를 시작한 뒤 readiness에서 확인됩니다. Dure는 host driver를
설치하거나 변경하지 않습니다. 시작 뒤에는 살아 있는 Ray node의 정확한 주소 집합,
node별 GPU 한 장과 `dure_node_<UUID>` custom resource 결합을 확인합니다. API 경로는
head에서 vLLM worker actor topology까지 요구합니다. 그 관측값과 vLLM 0.9.0 소스에
고정한 head 우선·worker 주소 정렬 규칙을 결합해 `pipeline-rank-contract`를 만들고,
controller가 각 노드의 폐쇄형 정규 JSON을 저장 계획과 다시 비교합니다.

Ray node 목록과 actor topology만으로 vLLM 내부 pipeline rank 숫자를 공개 필드에서
직접 읽을 수는 없습니다. 따라서 프로덕션의 `pipeline-rank-contract`는 정확한 버전의
정렬 규칙과 실제 node·actor topology를 결합한 **source-pinned 간접 증적**입니다.
주소·UUID 집합만 같고 순서가 다르거나 node·actor가 빠지면 실패합니다.

별도 opt-in GPU harness는 이보다 강한 수용 검사를 수행합니다. vLLM 0.9.0의
`RayDistributedExecutor`가 보유한 driver resource actor와 정렬된 worker actor의
소스상 순서에서 `(Ray node ID, GPU IDs)`를 수집해 불변 `ordered_bindings`와 정확히
비교하고, 실제 분산 model load와 고정 최소 추론까지 수행합니다. 이 harness 결과는
controller의 일반 `verified_at` 생성 조건에 자동 연결되지 않으며 controller의 노드별
증적을 대신하지도 않습니다. GPU 권한이 없어 `NOT_RUN`이면 PR을 Draft로 유지하고
운영자는 실제 GPU 검증을 통과했다고 주장해서는 안 됩니다.

성공 결과는 일반 Agent 결과와 같은 `checks` 배열에
`name="pipeline-rank-contract"`, `ok=true`, `blocking=true`인 검사 하나를
기록합니다. `detail`은 정규 JSON 문자열이며 다음 폐쇄형 필드만 가집니다.

```text
schema_version, backend, vllm_version, node_id, runtime_address,
pipeline_rank, runtime_rank, ordered_bindings
```

각 `ordered_bindings` 항목도 `node_id`, `runtime_address`, `pipeline_rank`,
`runtime_rank`만 가집니다. 배포 ID·세대·world size·driver ID 같은 값은 저장된
계획과 task 결합에서 이미 확인하며 이 detail에 중복해 넣지 않습니다. controller는
detail의 필드 집합과 전체 정렬 binding을 저장 계획에서 다시 만들어 정확히
비교합니다. 과거 초안의 top-level `rank_attestation` 형식은 허용하지 않습니다.

raw log, stdout·stderr, prompt, token, credential, command, Docker 인자, 환경 변수,
mount와 host path는 중앙 증적에 넣지 않습니다.

## 실패 대응

환경 차이는 룰베이스 추천만으로 완전히 예측할 수 없습니다. 프로덕션 경로는
**사전 검사 → 실제 컨테이너·Ray·API readiness → 간접 rank 계약 → 명시적 승격**으로
닫고, 별도 GPU harness는 **실제 분산 load → actor 순서 비교 → 최소 추론**을 추가로
검증합니다.

| 실패 시점·상황 | 자동 처리 | 운영자 대응 |
|---|---|---|
| opt-in 없음, 설정·GPU·runtime·모델 전제 부족 | 실행하지 않고 `NOT_RUN`, 종료 코드 77 | 전제 조건을 보완해 새 validation run으로 다시 검사합니다. `NOT_RUN`을 성공으로 바꾸지 않습니다. |
| 지원하지 않는 vLLM·V1·TP·PP·node 수 | Ray·vLLM 시작 전 거부 | 기존 계약을 우회하지 않고 새 backend 버전과 실제 GPU 검증을 추가합니다. |
| 공인 주소, 중복 주소·UUID, driver/rank 불일치 | 시작 전 거부 | 중앙 profile과 사설망 구성을 수정하고 새 세대를 계획합니다. |
| Ray join 누락·중복 node·node당 GPU 수 불일치 | 시작한 검사를 `FAILED`로 닫고 readiness를 주지 않음 | Ray·방화벽·GPU 격리 상태를 조사한 뒤 제한된 횟수로 재시도합니다. |
| vLLM load, NCCL/Gloo, CUDA OOM·호환성 실패 | `FAILED`, 새 세대 비승격 | driver/CUDA/runtime image, VRAM, 네트워크와 모델 identity를 조사합니다. host driver는 Dure가 자동 변경하지 않습니다. |
| source-defined actor 순서에서 추론한 rank가 계획과 다름 | `RANK_BINDING_MISMATCH`, 즉시 비승격 | 해당 backend와 세대를 사용하지 않습니다. IP·driver·Ray 배치를 수정해 새 불변 계획으로 다시 검사합니다. |
| 최소 추론 실패·빈 결과 | `FAILED`, API readiness를 주지 않음 | 모델 load와 분산 통신 로그를 접근 제어 저장소에서 조사합니다. |
| 검사 도중 timeout·Agent 재시작·lease 만료 | 현재 시도를 실패 처리하고 늦은 결과를 fencing | 같은 고정 입력에 새 시도 번호를 발급합니다. 이미 성공한 다른 세대의 증적을 재사용하지 않습니다. |
| 새 세대 일부 container만 시작됨 | 정확한 새 deployment·generation·node label의 container만 정리 가능 | 이름이나 넓은 Docker filter로 다른 workload를 중지하지 않습니다. |

실패한 새 세대 때문에 실행 중인 이전 검증 세대를 자동 중지하거나 교체하지 않습니다.
실제 서비스 전환은 모든 node의 `pipeline-rank-contract`와 API readiness가 성공한 뒤의
별도 명시적 단계입니다. GPU 수용 harness의 최소 추론 성공은 별도로 기록합니다.
전환 뒤 문제가 발견된 경우에는 검증된 직접 직전 세대로의
기존 명시적 rollback 절차를 사용합니다. 자동 driver 설치, 무제한 재시도, 실패한
파일의 해시 재작성, 임의 shell 복구 명령은 허용하지 않습니다.

재시도는 같은 deployment generation과 현재 attempt에 결합하고 횟수·timeout을
제한해야 합니다. 부분 성공을 전체 성공으로 합치거나 다른 node 조합의 과거 증적을
가져오지 않습니다. 오류 원문은 접근 제어된 node 로그에 남길 수 있지만 중앙에는
폐쇄형 failure code와 안전한 요약만 보고합니다.

## 실제 GPU 수용 검사 harness

번들 harness는 `scripts/acceptance-vllm-ray-pp.py`입니다. 이 파일을 추가한 것만으로
검사가 실행되지는 않으며 이 ADR을 작성한 환경에서도 실제 GPU 실행을 수행하지
않습니다.

기본 실행, 명령행 인자가 있는 실행, opt-in이 없는 실행과 시작 전 전제 부족은
`NOT_RUN`과 종료 코드 77입니다. 다음 opt-in 하나만 허용합니다.

```bash
export DURE_RUN_VLLM_RAY_PP_ACCEPTANCE=1
python3 scripts/acceptance-vllm-ray-pp.py
```

설정은 caller가 지정한 경로가 아니라 정확히 다음 root 소유 일반 파일에서만
읽습니다. 심볼릭 링크, group/world 쓰기 권한, 64 KiB 초과, 읽는 도중 identity
변경과 중복 JSON key는 거부합니다.

```text
/etc/dure/acceptance-vllm-ray-pp-v1.json
```

모델도 caller가 지정한 host path가 아니라 고정 `/models/model` read-only mount만
사용합니다. 설정 문서의 허용 필드는 다음뿐입니다.

```text
schema_version, backend, vllm_version, validation_run_id,
deployment_id, generation, runtime_image, model_manifest_digest,
ordered_bindings
```

각 `ordered_bindings` 항목은 `node_id`, `runtime_address`, `pipeline_rank`,
`runtime_rank`만 허용합니다. 첫 항목이 driver와 rank 0을 함께 고정합니다. 알 수
없는 필드가 하나라도 있으면 거부하므로
command, environment, Docker 인자, mount, prompt, URL, credential 또는 host path를
주입할 수 없습니다. script 자체가 `VLLM_USE_V1=0`, Ray non-SPMD, worker당 GPU
한 장, head의 `VLLM_HOST_IP`와 `RAY_ADDRESS`를 고정합니다. `PYTHONPATH`,
`PYTHONHOME`, `LD_PRELOAD`, 알 수 있거나 값이 다른 `VLLM_*`·`RAY_ADDRESS`가
주변 환경에 있으면 실행 전 `NOT_RUN`으로 닫습니다.

실제 실행 시작 경계는 preflight가 끝난 뒤 rank 0의 고정 사설 주소와 표준 GCS
port 6379로 기존 Ray cluster에 연결하기 직전입니다. caller가 Ray endpoint를 입력할
수는 없습니다. 그 전의 부족 조건은 `NOT_RUN`이고, 그 뒤 Ray 연결,
분산 load, rank 검사 또는 추론 오류는 항상 `FAILED`와 종료 코드 1입니다. 예상하지
못한 예외의 원문은 결과에 출력하지 않아 경로·환경·credential 유출을 막습니다.
GPU 작업은 별도 감독 프로세스에서 최대 1,800초만 실행하며, timeout에는
TERM 후 필요하면 KILL하고 executor와 Ray client 연결을 `finally`에서 정리합니다.
Ray cluster 자체를 종료하거나 다른 세대의 actor를 정리하지 않습니다.

2노드와 3노드 각각에서 다음 조건을 모두 충족해야 `PASSED`입니다.

- Ray의 ALIVE GPU node 주소 집합이 설정의 node 주소 집합과 정확히 같습니다.
- 각 node가 GPU 하나만 제공하고 vLLM actor도 node마다 하나, GPU 하나를 사용합니다.
- 각 Ray node의 Dure UUID custom resource와 주소가 계획에 정확히 결합됩니다.
- driver actor가 rank 0 node에 있고 source-defined actor 순서가 전체 `ordered_bindings`와 같습니다.
- 실제 executor가 vLLM 0.9.0 `RayDistributedExecutor`, V0, `TP=1`, 계약 PP입니다.
- 고정 `FULL_SNAPSHOT` AWQ marker와 모델 매니페스트 digest가 일치합니다.
- 실제 분산 load와 최대 4 token의 고정 최소 추론이 성공합니다.

단위 테스트는 실제 GPU·Docker·Ray daemon·Internet을 요구하지 않습니다. 고정 입력
파서, 2/3노드 rank 계산, 누락·중복·순서 바뀜, 임의 입력 거부, `NOT_RUN`/`FAILED`
경계, UUID resource swap, 감독 timeout과 민감 오류 원문 비노출을 독립 fake
backend로 검사합니다.

## 보안 결과와 제한

Ray GCS와 worker 프로토콜은 공개 인터넷에 노출할 인증·격리 경계가 아닙니다. Ray
포트는 신뢰된 LAN 또는 WireGuard 같은 사설 overlay와 host firewall 안에만 두며,
API head 외부에서 임의 Ray actor를 제출할 수 없게 해야 합니다. 컨테이너가 host
driver 차이를 없애 주지는 않으므로 driver/CUDA 호환성은 각 node preflight와 실제
load로 확인합니다.

독립 harness 설정의 `runtime_image` digest는 **선언값**입니다. script가 컨테이너
안에서 자신의 OCI manifest digest를 독립적으로 읽을 수는 없으므로 성공 출력도
`runtime_image_declared`와 `runtime_image_attested=false`로 구분합니다. 반드시
운영자가 신뢰한 wrapper가 정확한 digest-pinned 이미지로 harness를 시작해야 하며,
이 결과는 controller가 인증된 각 Agent에서 모은 노드별 readiness를 대체하지
않습니다. 모델도 안전하게 읽은 Dure marker의 manifest digest를 확인하지만 모든
weight를 다시 해시하지 않으므로 `model_content_rehashed=false`로 기록합니다.

이 결정은 다음 문제를 해결하지 않습니다.

- `TP>1`, node당 여러 GPU, heterogeneous GPU의 안전한 rank 배치
- Ray port 암호화·공개 참여 node 격리
- rank별 `STAGE` 다운로드·원자적 활성화·`sharded_state` load
- P2P 모델 청크 전송과 캐시 퇴출
- 자동 driver 설치 또는 변경
- 24시간 soak, node 상실 중 자동 재배치와 무중단 blue-green 전환

특히 vLLM 0.9.0의 `sharded_state` 파일명 rank는 pipeline rank가 아니라 TP rank입니다.
후속 `STAGE` 연결에서도 `TP=1`의 모든 stage 파일명이 `model-rank-0-*`가 되므로
반드시 `stages/<pipeline-rank>` 디렉터리를 node별로 격리해야 합니다.

## 후속 상태 메모

0.3.19는 이 ADR이 후속으로 남긴 rank별 `STAGE` 다운로드·원자적 활성화와 `sharded_state` load를 구현했습니다. 각 노드는 서로 다른 복합 identity host 경로를 동일한 `/models/model`에 mount하며 원래의 UUID·주소·rank 규칙은 바꾸지 않습니다. 다만 추천의 자동 variant 선택, P2P 전송, `TP>1`, 자동 재배치와 무중단 전환은 여전히 이 ADR의 해결 범위 밖입니다. 위 0.3.18 문장은 당시 결정 범위를 보존한 기록입니다.

## 근거 자료

- [vLLM 0.9.0 분산 추론 및 서빙 문서](https://docs.vllm.ai/en/v0.9.0/serving/distributed_serving.html)
- [vLLM 0.9.0 Ray distributed executor 소스](https://github.com/vllm-project/vllm/blob/v0.9.0/vllm/executor/ray_distributed_executor.py)
- [vLLM 0.9.0 multiprocessing executor 소스](https://github.com/vllm-project/vllm/blob/v0.9.0/vllm/executor/mp_distributed_executor.py)
- [vLLM 0.9.0 parallel state와 rank 계산 소스](https://github.com/vllm-project/vllm/blob/v0.9.0/vllm/distributed/parallel_state.py)
- [vLLM 0.9.0 sharded-state loader 소스](https://github.com/vllm-project/vllm/blob/v0.9.0/vllm/model_executor/model_loader/sharded_state_loader.py)

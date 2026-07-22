# 자동 배치 프로필 qualification

> 상태: **중앙 계약 구현**. 자동 생성된 배치 프로필의 상태 전이, 폐쇄형 증적 스키마, 정책·suite·작업 부하 동결, 정확한 rank·노드·GPU 결합과 추천 재검사는 구현되었습니다. 실제 시험 결과는 신뢰된 외부 executor가 관리자 API로 제출합니다. Dure Agent가 여러 GPU 노드에서 모델 load·NCCL·추론 시험을 자동 실행하고 결과를 모으는 실행기는 아직 구현되지 않았습니다.

## 목적과 범위

qualification은 자동 생성된 배치 프로필을 추정만으로 배포 후보에 넣지 않기 위한 중앙 자격 계약입니다. 현재 자동 프로필은 Qwen2.5 AWQ 7B·14B·32B·72B 네 모델과 `TP=1`로 제한됩니다. 개별 프로필이 요구하는 노드 수와 PP는 생성된 spec에 고정되며, qualification 요청이 임의의 TP·PP나 모델을 추가할 수 없습니다.

이 계약은 다음을 수행합니다.

- 현재 인벤토리와 점유 상태를 검사해 시험 가능한 정확한 노드 집합을 고릅니다.
- 각 rank를 중앙 노드 UUID와 GPU index·UUID 한 쌍에 고정합니다.
- 정책, suite, 단계 순서, 작업 부하, 모델·런타임 식별자를 run에 동결합니다.
- 신뢰된 외부 executor가 제출한 폐쇄형 결과를 서버가 다시 검증하고 증적 digest를 계산합니다.
- 통과한 프로필을 `VALIDATED`로 만들고, 운영자의 별도 요청 뒤에만 `ACTIVE`로 전환합니다.
- 단일·다중 노드 AUTO 추천 시 정확한 노드·rank·GPU 결합과 24시간 이내 증적을 다시 검사합니다.

qualification은 모델 릴리스 자체의 `DRAFT → VALIDATED → ACTIVE` 수명 주기와 구분되는 **배치 프로필 수명 주기**입니다.

## 상태 전이

```text
DRAFT
  └─ qualification 적용 ─→ QUALIFYING
                               ├─ 모든 게이트 통과 ─→ VALIDATED
                               │                       └─ 운영자 활성화 ─→ ACTIVE
                               ├─ 실패 증적 등록 ───→ DRAFT
                               └─ 운영자 취소 ──────→ DRAFT
```

- `DRAFT → QUALIFYING`: `apply=true`로 run과 정규화 binding을 DB에 저장합니다.
- `QUALIFYING → VALIDATED`: 서버가 증적의 모든 단계·수치·결합을 검사해 `PASSED`로 판정한 경우입니다.
- `VALIDATED → ACTIVE`: 운영자가 활성화 API를 명시적으로 호출하고 서버가 현재 인벤토리·GPU·레지스트리 식별자를 다시 검사한 경우입니다.
- `QUALIFYING → DRAFT`: 실패 증적이 등록되거나 운영자가 실행을 취소한 경우입니다.

`VALIDATED`는 자동으로 `ACTIVE`가 되지 않습니다. 추천기는 모델 릴리스와 배치 프로필이 모두 `ACTIVE`인 조합만 읽습니다.

최초 활성화 증적이 만들어진 뒤에는 같은 프로필에 `SUPPLEMENTARY` 실행을 명시해 다른 exact 노드 집합의 증적을 추가할 수 있습니다. `PRIMARY` 실행만 위 상태 전이를 소유하며, 보조 실행은 `VALIDATED` 또는 `ACTIVE` 상태와 최초 `qualification_evidence_id`를 변경하지 않습니다. 따라서 보조 실행의 실패나 취소가 이미 활성화된 프로필을 `DRAFT`로 되돌리지 않습니다. 실행 목적은 run의 서명 대상 workload 계약에 동결되고 조회 응답의 `purpose`에도 표시됩니다.

## 준비와 점유 노드 차단

준비 요청은 정규 UUID인 `request_id`, 자동 배치 프로필 ID와 정확한 노드 UUID 목록을 받습니다. 노드 수는 프로필의 `node_count`와 정확히 같아야 하고 중복할 수 없습니다. 서버는 다음 조건을 실패 안전 방식으로 검사합니다.

- 노드 승인, 최근 heartbeat와 90초 이내 프로필
- 정상 Docker와 NVIDIA runtime
- 정상 GPU, 최소 VRAM, 지원 compute capability·GPU 아키텍처
- 최소 디스크와 다중 노드의 고유한 사설 IPv4 주소
- `QUEUED`·`RUNNING` 중앙 task 부재
- 활성 Fleet 노드·GPU 예약 부재
- 활성 배포 operation 부재
- 다른 `QUALIFYING` 실행의 노드 예약 부재
- Agent 인벤토리에 관측된 실행 중 작업 부하 부재

점유 노드는 `NODE_OCCUPIED`와 구조화된 점유 사유로 거부합니다. 활성 Fleet 예약은 `ACTIVE_FLEET_RESERVATION:<fleet-id>:<deployment-id>`로 식별하며, `apply=true`는 예약 경계와 인벤토리를 잠근 상태에서 다시 검사합니다. qualification이 기존 서비스나 수락된 Fleet와 GPU를 공유할 수 있다고 추측하지 않습니다.

기본 준비는 preview입니다.

```http
POST /v1/admin/profile-qualifications/prepare
Content-Type: application/json

{
  "request_id": "<정규 UUID>",
  "placement_id": "<자동 배치 프로필 UUID>",
  "node_ids": ["<노드 UUID>"],
  "apply": false,
  "purpose": "PRIMARY"
}
```

최초 증적으로 활성화된 프로필의 다른 exact 노드 집합을 검증할 때만 `purpose`를 `SUPPLEMENTARY`로 지정합니다. 생략값은 기존 호출과 호환되는 `PRIMARY`이며, 활성 프로필에서 목적을 생략한 요청은 보조 실행으로 추측하지 않고 거부합니다. 서로 다른 보조 실행은 GPU·노드가 겹치지 않을 때 병행할 수 있습니다.

`apply=false`는 DB run, Agent task, 다운로드, 이미지 pull 또는 컨테이너 변경을 만들지 않습니다. `apply=true`는 run·binding·감사 기록을 저장하며, `PRIMARY`일 때만 프로필을 `QUALIFYING`으로 바꿉니다. `SUPPLEMENTARY`는 기존 `VALIDATED`·`ACTIVE` 상태와 최초 증적 포인터를 유지합니다. 어느 목적도 Agent task나 호스트 변경을 만들지 않습니다. 같은 `request_id`와 같은 프로필·노드 집합·목적의 재요청은 저장된 run을 반환하고, 다른 결합은 충돌로 거부합니다.

## rank·노드·GPU UUID 결합

각 선택 노드는 GPU를 정확히 한 장만 사용합니다. 여러 정상 GPU가 있으면 VRAM이 큰 GPU를 우선하고, 이후 GPU UUID와 index로 결정론적으로 정렬합니다. 선택하지 않은 GPU는 qualification 결합에 포함하지 않습니다.

단일 노드는 정렬된 노드 UUID가 rank 0입니다. 다중 노드는 정렬된 노드 UUID의 첫 노드가 head·rank 0이고, 나머지 worker는 고유 사설 IPv4 주소 순서로 rank를 고정합니다. 각 결합은 다음 필드를 가집니다.

```text
run ID + rank + 중앙 node UUID + GPU index + GPU UUID
+ VRAM + compute capability
```

결합은 run의 불변 JSON 스냅샷과 `profile_qualification_bindings` 정규화 행에 함께 저장합니다. DB는 run 안의 rank, 노드 UUID와 GPU UUID 중복을 거부하고, rank·GPU index 음수와 잘못된 GPU UUID 형식을 거부합니다. 증적 등록·활성화·추천은 정규화 행과 run 스냅샷이 정확히 같은지 다시 확인합니다.

## 동결되는 정책·suite·작업 부하

현재 중앙 계약은 다음 값을 사용합니다.

```text
policy_version = profile-qualification-v2
suite_id       = dure-profile-qualification-v2
```

run에는 다음 항목을 함께 동결합니다.

- 아래 8단계의 정확한 순서
- 모델 릴리스, 배치 프로필과 spec digest
- 모델 revision·manifest digest
- OCI digest 런타임 이미지와 vLLM 버전
- `TP`, `PP`, 노드 수
- 최대 컨텍스트, 동시성
- 입력·출력 토큰, 예열 요청과 최소 측정 요청 수
- 인벤토리 지문과 rank·노드·GPU 결합

v2는 최대 컨텍스트 경계를 그대로 확인하면서 출력 토큰을 최대 32개, 최소 측정 요청을
`max(2, concurrency)`로 제한합니다. 72B `PP=3`의 load·예열·측정·재시작까지 5분 안에 끝내기
위한 계약이며, 요청 수를 늘린 장기 soak test를 대체하지 않습니다.

작업 부하 전체를 정규 JSON으로 만든 SHA-256 `workload_digest`도 저장합니다. 증적의 컨텍스트·동시성·입출력 토큰·예열 횟수는 동결 값과 정확히 같아야 합니다. 정책·suite·단계·작업 부하 digest가 현재 계약 또는 run과 다르면 `QUALIFICATION_POLICY_STALE`로 거부합니다.

## 8단계 폐쇄형 증적

외부 executor는 다음 단계를 이 순서대로 모두 제출해야 합니다. 단계 ID, 상태와 실패 코드는 허용 목록 밖의 값을 받을 수 없고, 실패 코드는 해당 단계와 정확히 대응해야 합니다.

| 순서 | 단계 | 확인 대상 | 실패 코드 |
|---:|---|---|---|
| 1 | `STATIC_COMPATIBILITY` | 모델·런타임·GPU 정적 호환성 | `STATIC_COMPATIBILITY_FAILED` |
| 2 | `CAPACITY_ESTIMATE` | VRAM, KV cache와 작업 공간 여유 | `CAPACITY_ESTIMATE_FAILED` |
| 3 | `ARTIFACT_READY` | exact 모델·STAGE 입력 준비 | `ARTIFACT_NOT_READY` |
| 4 | `NETWORK_NCCL` | exact 다중 노드 네트워크·NCCL | `NETWORK_NCCL_FAILED` |
| 5 | `MODEL_LOAD` | 고정 런타임에서 모델 load | `MODEL_LOAD_FAILED` |
| 6 | `SHORT_INFERENCE` | 짧은 추론과 요청 성공률 | `SHORT_INFERENCE_FAILED` |
| 7 | `CONTEXT_CONCURRENCY` | 동결 컨텍스트·동시성의 SLO | `CONTEXT_CONCURRENCY_FAILED` |
| 8 | `RESTART_STABILITY` | 명시적 재시작 뒤 안정성 | `RESTART_STABILITY_FAILED` |

서버는 모델 load 시간, 요청 수, 재시작 수, TTFT·TPOT·종단 지연, 처리량, 성공률, VRAM 여유를 폐쇄형 수치 스키마로 검사합니다. 다중 노드는 대역폭·RTT·패킷 손실과 NCCL all-reduce 성공도 요구합니다. 단일 노드 증적에는 네트워크·NCCL 값을 넣을 수 없습니다. NaN·무한대, 누락 필드와 임의 metadata는 거부합니다.

증적은 다음 API로 등록합니다.

```http
POST /v1/admin/profile-qualifications/<run-id>/evidence
```

요청은 8단계 결과, 폐쇄형 metrics, digest로 고정된 executor 이미지와 40~64자리 Dure 커밋 표식만 받습니다. 서버는 현재 인벤토리, 점유 상태, rank·GPU 결합, 정책·suite·작업 부하, 모델·런타임 식별자를 다시 계산한 뒤 evidence digest와 최종 `PASSED`·`FAILED`를 결정합니다. 제출자가 최종 상태를 직접 지정할 수 없습니다.

## 운영자 활성화

통과 증적은 프로필을 `VALIDATED`로 만들지만 배포 후보로 즉시 공개하지 않습니다. 운영자가 다음 API를 호출해야 합니다.

```http
POST /v1/admin/placement-profiles/<placement-id>/activate
```

서버는 연결된 run·증적, 정책·suite·작업 부하 digest, 정규화 binding, 현재 GPU UUID·VRAM·compute capability, 모델 revision·manifest와 런타임 이미지·vLLM 버전을 다시 검사합니다. 현재 노드가 여전히 승인·온라인·비점유 상태이고 프로필의 최소 자원 조건도 만족해야 합니다. qualification 자체가 소비한 디스크나 새로 준비한 exact 캐시처럼 안전한 동적 인벤토리 변화는 허용하지만, GPU와 불변 실행 identity가 달라졌으면 활성화를 거부합니다. 통과하면 프로필만 `ACTIVE`로 바꾸고 활성화 시각을 기록합니다.

조회와 취소 API는 다음과 같습니다.

- `GET /v1/admin/profile-qualifications/<run-id>`
- `POST /v1/admin/profile-qualifications/<run-id>/cancel`

취소는 `QUALIFYING` run만 `CANCELED`로 닫고 프로필을 `DRAFT`로 돌립니다.

## 중앙 추천의 exact 단일·다중 노드 재검사

자동 프로필의 qualification 증적은 단일 노드에서도 실제 시험한 정확한 GPU에 결합됩니다. 다음 조건을 모두 만족할 때만 중앙 추천의 qualification 증적으로 사용하며, AUTO 프로필은 일반 네트워크 확인이나 다른 벤치마크 증적으로 이 게이트를 우회하지 않습니다.

1. 증적과 run이 모두 `PASSED`이고 프로필이 연결한 exact 증적입니다.
2. 모델 릴리스·배치 spec·모델 revision·manifest·런타임 이미지·vLLM 버전이 현재 값과 같습니다.
3. policy, suite, 단계와 workload digest가 현재 중앙 계약과 같습니다.
4. 정렬된 노드 UUID 집합과 rank 노드 집합이 프로필의 전체 노드 수와 정확히 일치합니다.
5. 정규화 binding과 run의 rank·node UUID·GPU index·GPU UUID 결합이 같습니다.
6. 현재 선택 GPU 결합이 증적 시점과 같고 노드는 여전히 승인·온라인·비점유이며 최소 자원 조건을 만족합니다. 전체 인벤토리 지문은 감사용으로 보존하지만 qualification이 만든 캐시·디스크 변화만으로 증적을 폐기하지 않습니다.
7. 다중 노드이면 대역폭·RTT·패킷 손실·NCCL 수치가 현재 프로필 임계값을 통과합니다.
8. 등록 시각이 미래가 아니고 24시간 이내입니다.

서로 다른 증적의 노드를 섞거나 부분 집합을 재사용하지 않습니다. 단일 노드에서도 동일 사양의 다른 GPU로 바꿔 선택하지 않습니다. 24시간 TTL은 `ACTIVE` 상태를 자동 취소하는 규칙이 아니라, 오래된 실제 실행 증적을 새 후보에 재사용하지 않기 위한 읽기 전용 추천 게이트입니다.

PRIMARY와 유효한 모든 SUPPLEMENTARY exact 증적은 [Fleet 후보 생성과 결정론적 스케줄러](fleet-scheduler.md)의 독립 배포 후보가 될 수 있습니다. 같은 exact 노드 집합의 새 실행이 진행 중이거나 실패하면 과거 통과 증적을 Fleet 후보로 되살리지 않습니다.

## 호스트 변경이 없는 경계

다음 동작은 중앙 DB 상태와 감사 기록만 읽거나 변경합니다.

- qualification preview
- `apply=true`인 qualification run 준비
- 증적 등록
- 실패 run 취소
- `VALIDATED → ACTIVE` 운영자 활성화
- 추천의 qualification 증적 재검사

이 동작 자체는 Agent task, 모델 다운로드, 이미지 pull, 모델 캐시 준비, Docker 컨테이너 실행·중지, 기존 배포 교체를 수행하지 않습니다. 실제 배포 준비와 적용은 추천·수락 뒤의 별도 명시적 명령입니다.

## 신뢰 경계와 현재 제한

현재 구현은 **신뢰된 외부 executor가 실제 시험을 수행하고 관리자 API로 결과를 제출한다는 중앙 계약**입니다. executor 이미지는 OCI digest로 고정하고 Dure 커밋 표식을 기록하지만, 결과에 Agent 서명이나 원본 로그 provenance를 붙이는 암호학적 검증은 아직 없습니다. 운영자는 executor와 관리자 token, 원본 결과 저장소를 같은 신뢰 경계에서 관리해야 합니다.

Dure Agent의 기존 자동 `BENCHMARK` 작업은 승인된 단일 GPU와 `FULL_SNAPSHOT`용 별도 경로입니다. 이 경로가 다중 노드 profile qualification을 대신하지 않습니다. 현재 Dure는 다음을 자동 수행하지 않습니다.

- 여러 Agent에 qualification task를 분배하고 barrier를 맞추는 일
- exact 노드 집합의 네트워크·NCCL 시험 실행
- 여러 노드의 Ray/vLLM 모델 load·추론·재시작 시험 실행
- executor 결과의 서명·원본 provenance 검증
- 24시간 지속·복구 시험

따라서 다중 노드 profile qualification은 운영자가 신뢰된 별도 harness로 실행하고 폐쇄형 결과를 제출해야 합니다. 구현되지 않은 Agent 자동 실행을 `prepare`, 증적 등록 또는 활성화 API가 수행한다고 해석하면 안 됩니다.

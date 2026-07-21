# Fleet 후보 생성과 결정론적 스케줄러

> 상태: **불변 추천, 원자적 수락·예약과 전용 Fleet runtime 구현**. 네 모델의 ACTIVE 자동 배치 프로필과 현재 유효한 exact qualification 증적을 GPU 풀에 투영해 서로 겹치지 않는 여러 배포 단위를 결정론적으로 조합합니다. 추천과 수락은 호스트를 바꾸지 않으며, 운영자가 전용 준비와 적용 명령을 각각 명시한 뒤에만 모델·이미지 준비와 컨테이너 적용·검증을 수행합니다.

## 고정 계약

Fleet 평가기는 다음 조건을 입력과 결과에서 강제합니다.

```text
허용 모델 = qwen2.5-7b-awq, qwen2.5-14b-awq,
            qwen2.5-32b-awq, qwen2.5-72b-awq
TP = 1
선택 노드당 GPU = 정확히 한 장
한 결과 안의 노드·GPU UUID 중복 = 금지
클러스터 전체 노드 수 = 제품 상한 없음
개별 배포의 PP = exact 검증 프로필의 결합 수와 동일
```

클러스터 노드 수를 2개나 3개로 제한하지 않습니다. 현재 엄격한 다중 노드 실행 단위가 실제로 지원하는 PP는 2·3이고, 이 검증된 단위를 GPU가 겹치지 않게 여러 개 선택합니다. 아직 검증되지 않은 PP를 스케줄러가 새로 만들어내지 않습니다.

## 후보가 만들어지는 과정

중앙 평가기는 요청한 전체 인벤토리에서 다음을 검사합니다.

1. 승인·온라인·프로필 신선도와 Docker·NVIDIA runtime
2. 정상 GPU와 결정론적으로 선택한 GPU index·UUID
3. 중앙 task, 활성 배포 operation, qualification 예약과 관측 작업 부하
4. ACTIVE 모델 릴리스와 ACTIVE AUTO 배치 프로필
5. 24시간 이내의 최신 PRIMARY·SUPPLEMENTARY exact qualification 증적
6. 모델·runtime·GPU 아키텍처·VRAM·디스크·STAGE 또는 FULL 입력 적합성
7. 엄격한 다중 노드 runtime의 rank·주소·Agent 버전 계약

후보 하나는 다음 식별자를 동결합니다.

- 모델 릴리스·배치 프로필과 모델 revision·manifest
- OCI digest 런타임과 vLLM 버전
- `TP=1`, PP, rank별 노드 UUID·GPU index·GPU UUID
- qualification evidence ID·digest·등록 시각
- `STAGE` artifact set·rank 매니페스트 또는 `FULL_SNAPSHOT` 크기 계약
- 실제 증적 처리량, 품질 순위, cache hit, VRAM 불균형과 신뢰된 네트워크 영역 비용

같은 evidence ID가 서로 다른 digest, 프로필 또는 node/GPU/rank 결합을 승인하는 형태는 `FLEET_EVIDENCE_CONFLICT`로 거부합니다. AUTO 프로필은 일반 네트워크 확인이나 다른 벤치마크로 exact qualification을 대체할 수 없습니다.

## 여러 exact 노드 집합 검증

최초 `PRIMARY` 실행은 프로필의 `DRAFT → QUALIFYING → VALIDATED → ACTIVE` 전이를 소유합니다. 활성화 뒤 다른 exact 노드 집합은 명시적인 `SUPPLEMENTARY` 실행으로 검증합니다.

```json
{
  "request_id": "<정규 UUID>",
  "placement_id": "<AUTO 프로필 UUID>",
  "node_ids": ["<exact 노드 UUID>"],
  "purpose": "SUPPLEMENTARY",
  "apply": true
}
```

보조 실행은 PRIMARY 증적 포인터와 프로필 상태를 바꾸지 않습니다. 실패·취소도 이미 ACTIVE인 프로필을 내리지 않습니다. 같은 exact 노드 집합에서는 가장 최근의 실행이 권위가 있으므로, 새 실행이 `QUALIFYING`이거나 `FAILED`이면 과거 통과 증적을 재사용하지 않습니다. 서로 겹치지 않는 보조 실행은 병행할 수 있습니다.

## set-packing 목적 순서

후보는 각각 노드와 GPU UUID 집합을 소비합니다. 스케줄러는 겹치지 않는 부분집합을 탐색하며 다음 값을 사전식으로 비교합니다.

1. 요청한 모델별 최소 복제본 충족
2. 검증된 고품질 모델 우선
3. 검증된 처리량 합계 최대화
4. 사용 가능한 노드 활용률 최대화
5. 후보 내부 VRAM 불균형 최소화
6. 최소 예비 노드와 명시 예비 노드 정책
7. cache hit 최대화와 네트워크 영역 비용 최소화
8. 동일하면 정렬된 노드·GPU UUID·후보 ID

품질은 단순 합계가 아닙니다. 선택한 후보의 품질 순위를 높은 값부터 정렬한 벡터로 비교합니다. 따라서 32B 여러 개의 순위 합계가 더 크다는 이유로 검증된 72B 후보를 밀어내지 않습니다.

예를 들어 24GiB 적격 노드 8개에 두 개의 exact 72B/PP=3 증적과 각 노드의 32B 증적이 있으면 다음 조합을 선택합니다.

```text
72B A → 노드 1·2·3
72B B → 노드 4·5·6
32B   → 노드 7
32B   → 노드 8
```

## 결정성과 운영 한도

같은 인벤토리·점유·카탈로그·증적·정책은 입력 순서와 관계없이 같은 후보 ID, 선택 결과와 미배정 사유를 만듭니다. Fleet 인벤토리 지문은 전체 노드 프로필뿐 아니라 선택 GPU와 현재 점유 사유를 포함합니다.

제품 노드 수에는 하드코딩 상한이 없습니다. 대신 NP-hard set-packing의 운영 안전을 위해 다음 기본 한도를 둡니다.

- 투영 후보 최대 512개
- 탐색 상태 최대 250,000개

후보 수가 한도를 넘으면 `FLEET_CANDIDATE_LIMIT`로 명시적으로 거부합니다. 탐색 상태 한도는 전체 요청을 실패시키는 경계가 아니라 정확 해를 개선하는 계산 예산입니다. 스케줄러는 먼저 최소 복제본 정책을 고려한 결정론적 실행 가능 해를 만든 뒤 제한된 branch-and-bound 탐색으로 개선합니다. 예산을 모두 쓰면 현재 최선의 유효한 해를 반환하고 다음 필드로 최적성 증명이 끝나지 않았음을 표시합니다.

```json
{
  "search_complete": false,
  "search_limit_reached": true,
  "explored_states": 250000
}
```

따라서 대규모 중첩 후보 때문에 전체 Fleet 평가가 사라지지는 않지만, 운영자는 `search_complete=false`인 추천의 품질과 미충족 최소 복제본을 확인해야 합니다. 후속 수락 경계는 `unmet_minimum_replicas`가 하나라도 있으면 탐색 완료 여부와 관계없이 거부해야 하며, 미완료 탐색 결과를 최소 복제본 충족으로 추측할 수 없습니다. 1,000개 가용 노드, 100개 노드의 3종 단일 GPU 후보, 100개 중첩 PP=3 후보와 명백히 가능한 33개 최소 복제본의 제한 탐색을 회귀 검증합니다.

## 이기종 GPU·대규모 풀 수용 테스트

0.4.7의 `tests/test_fleet_scale_acceptance.py`는 실제 하드웨어를 대신하는 성능 시험이 아니라 합성 인벤토리와 검증 후보를 사용한 8개의 결정론적 단위·회귀 검사입니다. 저장·runtime 계층의 기존 회귀 검사는 SQLite를 사용합니다. 다음 경계를 함께 검증합니다.

| 축 | 합성 검사 | 보장하는 계약 |
|---|---|---|
| 이기종 GPU 선택 | 4·8·12·24·48·80GiB 노드와 8·24·80·48GiB GPU가 함께 있는 다중 GPU 노드 | 입력 순서와 무관하게 노드마다 정상 GPU 한 장만 선택하고, 다중 GPU 노드에서는 80GiB GPU의 정확한 index·UUID를 고정함 |
| 모델·토폴로지 | 네 모델의 자동 프로필과 지원 밖 모델·`TP=2`·노드 중복 입력 | allowlist를 네 모델로 닫고 항상 `TP=1`이며, 7B·14B·32B는 `PP=1`, 72B는 `PP=1/2/3` 형식만 생성함 |
| 5노드 조합 | 24GiB 3대, 12GiB 1대, 4GiB 1대와 exact 후보 | 72B/PP=3 하나와 14B/PP=1 하나를 선택하고 4GiB 노드는 정상 미선택으로 남김 |
| 6노드 조합 | 24GiB 6대와 두 비중첩·한 중첩 72B 후보, 단일 GPU 대안 | 입력 순서와 무관하게 서로 겹치지 않는 72B/PP=3 복제본 두 개를 선택함 |
| 1·128노드 풀 | 1노드와 8·12·24·48·80GiB가 반복되는 128노드 | 노드 수에 따른 제품 분기 없이 네 모델의 exact 단일 GPU 후보를 결정론적으로 조합함 |
| 미배정 사유 | 4GiB, pending, runtime 불가, Fleet 점유와 사용 가능한 8GiB 노드 | 낮은 VRAM 노드는 전체 오류가 아니며 `NODE_PENDING`·`RUNTIME_UNAVAILABLE`·`NODE_OCCUPIED`를 구조화해 보존함 |
| 대규모 노드 풀 | 1,000개 가용 노드 ID와 200개 독립 후보 | 하드코딩된 총 노드 수 상한 없이 후보 단위를 조합함 |
| 밀집 중첩 | 100개 노드에 걸친 100개 PP=3 후보와 제한된 탐색 예산 | 예산 안에서 노드·GPU가 겹치지 않는 결정론적 유효 결과를 유지하고 미완료 탐색을 명시함 |
| 최소 복제본 | 탐색 예산이 작아도 명백한 33개 비중첩 PP=3 증거가 있는 입력 | 실행 가능한 최소 복제본 증거를 먼저 보존하며, 미충족 결과를 충족으로 추측하지 않음 |
| 안전 한도 | 513개 후보와 8개 후보·탐색 상태 1 | 513번째 후보는 `FLEET_CANDIDATE_LIMIT`로 거부하고, 탐색 예산 소진은 유효 결과와 미완료 표식을 함께 반환함 |
| 저장·실행 경계 | 기존 추천 반복, 원자 수락·예약, 준비·적용·검증 회귀 | 멱등 추천, 부분 Fleet 수락 금지, 예약 중복 금지와 다른 배포에 대한 실패 격리를 유지함 |

여기서 “대규모 풀”은 한 모델의 PP를 가용 노드 수까지 자동 확장한다는 뜻이 아닙니다. exact 검증 증적이 있는 7B·14B·32B의 `PP=1` 후보와 72B의 `PP=1/2/3` 후보만 여러 개 조합합니다. 현재 엄격한 다중 노드 runtime은 `PP=2/3`만 실행하며, exact 프로필·증적이 없는 `PP=4`, `PP=7` 또는 임의 비균등 레이어 분할을 테스트가 승인하거나 생성하지 않습니다.

제품의 **노드 수**에는 하드 상한이 없지만 계산 입력과 시간은 무제한이 아닙니다. 기본 `max_candidates=512`를 넘으면 `FLEET_CANDIDATE_LIMIT`로 거부하고, `max_search_states=250000`을 소진하면 겹침 없는 현재 최선 결과와 `search_complete=false`, `search_limit_reached=true`를 반환합니다. 1,000개 노드를 받을 수 있다는 말은 그 풀에서 투영된 후보가 512개를 넘어도 모두 탐색한다는 뜻이 아닙니다. 이 한도는 서버 정책이며 관리자 API 입력으로 변경할 수 없습니다.

기본 수용 매트릭스는 실제 NVIDIA GPU, Docker daemon, Ray, vLLM model load, NCCL 통신이나 실제 PostgreSQL 서버의 잠금 경합·장시간 부하를 실행하지 않습니다. 합성 검사가 통과해도 대상 GPU 조합의 qualification 증적과 실제 2·3노드 수용 검사, 운영 규모의 PostgreSQL 동시성·부하 검증은 별도로 수행해야 합니다.

## 미배정 노드

후보가 없는 낮은 사양 노드는 전체 요청 오류가 아닙니다. 각 미배정 노드는 다음과 같은 구조화된 사유를 가집니다.

- `NODE_PENDING`, `NODE_OFFLINE`, `PROFILE_MISSING`, `PROFILE_STALE`
- `RUNTIME_UNAVAILABLE`, `GPU_UNAVAILABLE`, `NODE_OCCUPIED`
- `NO_VALIDATED_CANDIDATE`
- `OBJECTIVE_NOT_SELECTED`

점유 사유, 관련 후보 ID와 투영 거부 코드도 함께 보존합니다.

## 불변 Fleet 추천 API와 CLI

관리자는 전체 온라인 풀 또는 명시 노드 풀 중 하나를 선택해 추천을 저장합니다.

```bash
dure admin fleet recommend --all-online --objective quality-first
dure admin fleet recommend \
  --node <node-a> <node-b> <node-c> \
  --minimum-replica qwen2.5-72b-awq=1 \
  --minimum-reserve-nodes 1 \
  --reserve-node <node-c>
dure admin fleet show sha256:<64-hex>
dure admin fleet accept sha256:<64-hex>
```

대응 API는 다음 경로이며 모두 관리자 인증을 요구합니다.

- `POST /v1/admin/fleet-recommendations`
- `GET /v1/admin/fleet-recommendations/{recommendation-id}`
- `POST /v1/admin/fleet-recommendations/{recommendation-id}/accept`
- `GET /v1/admin/fleets/{fleet-id}`
- `POST /v1/admin/fleets/{fleet-id}/prepare`
- `POST /v1/admin/fleets/{fleet-id}/apply`

저장 행은 요청 정책, 전체 인벤토리·GPU 풀, 모든 후보와 투영 탈락 사유, 선택 배포, 미배정 노드, 증적·카탈로그·스케줄러 버전과 탐색 완료 상태를 포함합니다. ID는 생성 시각을 제외한 전체 정규 스냅샷의 SHA-256입니다. 같은 입력과 관측 상태를 반복하면 같은 ID의 기존 행을 반환하며, `recorded_at`은 ID 바깥의 DB 메타데이터입니다.

`show`는 저장 당시 검토 기록을 보여 줄 뿐 현재 유효성을 다시 증명하지 않습니다. API는 계산 한도나 임의 network zone을 클라이언트 입력으로 받지 않습니다. `accept`는 저장 추천의 무결성을 검사한 뒤 현재 레지스트리·인벤토리·GPU·qualification 증적·점유 상태로 전체 평가를 다시 수행합니다. 정규 스냅샷이 한 필드라도 달라졌거나 최소 복제본·예비 노드 정책이 충족되지 않으면 수락하지 않습니다.

수락은 선택된 모든 노드 행과 활성 예약 경계를 잠근 상태에서 다음을 한 트랜잭션으로 수행합니다.

1. 후보마다 독립적인 generation 1 `CREATED` 배포를 생성합니다.
2. 후보의 exact `(node UUID, GPU index, GPU UUID, rank)`를 배포와 연결합니다.
3. 노드당 정확히 한 GPU, Fleet 안과 전체 활성 Fleet 사이의 노드·GPU 중복 금지를 검사합니다.
4. 모든 배포·예약·감사 기록이 성공할 때만 커밋합니다.

추천을 만들 때 선택 후보마다 deployment ID와 generation을 제외한 전체 실행 plan의 정규 digest도 스냅샷에 고정합니다. 따라서 수락 뒤 주소, 네트워크 인터페이스, 레이어 분할, 컨텍스트, GPU 메모리 비율 같은 유효한 실행 필드 하나만 달라져도 조회와 반복 수락이 닫힙니다.

하나라도 실패하면 Fleet, 배포, 예약을 전부 롤백합니다. 같은 추천을 다시 수락하면 기존 Fleet를 검증해 멱등 반환하며 부분 Fleet를 보완하거나 새 ID로 복제하지 않습니다. 활성 예약은 노드 UUID가 달라도 같은 GPU UUID를 보고하는 경우까지 포함해 다른 단일 배포, qualification, benchmark와 무관한 작업이 같은 노드 또는 GPU를 선점하지 못하게 합니다.

## 전용 Fleet 준비·적용·검증

수락된 Fleet에는 배포마다 독립된 runtime 행이 하나씩 생깁니다. 저장된 추천·배포·예약과 실행 상태를 함께 조회합니다.

```bash
dure admin fleet status <fleet-id>
```

`status`는 읽기 전용입니다. 응답의 `runtime`에는 배포 ID, 현재 상태, 준비 ID, 현재 operation ID와 실패 단계·코드가 포함됩니다.

모델과 이미지는 다음 전용 명령을 명시적으로 실행한 뒤에만 준비합니다.

```bash
dure admin fleet prepare <fleet-id>
```

서버는 수락 당시 후보를 현재 모델·런타임 레지스트리, 정확한 노드·GPU 인벤토리, `STAGE` 또는 `FULL_SNAPSHOT` identity와 최신 exact qualification 증적에 다시 대입합니다. Fleet 자신이 보유한 예약은 자기 점유로 인정하지만 다른 점유를 우회하지 않습니다. 저장 후보와 현재 후보의 모델, 프로필, 노드·GPU·rank, 캐시 종류, 매니페스트, runtime, 증적이 달라졌다면 해당 배포를 `PREPARE_FAILED`로 닫고 호스트 작업을 만들지 않습니다.

검사를 통과한 배포는 저장 선택을 바꾸지 않고 노드별 `PREPARE_MODEL → PREPARE_IMAGE`를 수행합니다. 다른 모델·노드·variant나 `STAGE`↔`FULL_SNAPSHOT`으로 자동 전환하지 않습니다. 모든 노드의 모델·이미지 준비와 exact `READY`·최신 OCI digest 증적이 성공해야 그 배포가 `PREPARED`가 됩니다.

준비된 배포는 다음 명령으로만 적용합니다.

```bash
dure admin fleet apply <fleet-id>
```

각 배포의 하나의 펜싱된 operation 안에서 다음 순서를 강제합니다.

```text
APPLY (전체 배정 노드, serve=false)
  → START_API (Ray head 한 대)
  → VERIFY_API (Ray head 한 대)
  → VERIFY (전체 배정 노드)
  → ACTIVE
```

`apply`는 적용 직전에도 exact 캐시가 현재 `READY`인지, 준비 시도와 OCI 이미지 다이제스트 증적이 최신인지, 저장된 plan·노드·GPU·rank 결합이 그대로인지 검사합니다. 전체 노드 검증과 head API 검증이 모두 성공해야 배포의 `verified_at`과 runtime의 `ACTIVE`를 기록합니다. 현재 operation이 아닌 과거 시도의 claim·완료·취소는 상태를 전진시키지 않습니다.

배포별 runtime 상태는 다음 폐쇄형 값입니다.

```text
ACCEPTED → PREPARING → PREPARED → APPLYING → VERIFYING → ACTIVE
              └─────→ PREPARE_FAILED
                                  └────────→ APPLY_FAILED
                                               └──────→ VERIFY_FAILED
```

Fleet 전체 상태는 배포별 상태를 결정론적으로 집계한 `ACCEPTED`, `PREPARING`, `PREPARED`, `APPLYING`, `VERIFYING`, `ACTIVE`, `PARTIAL_FAILED`, `FAILED` 중 하나입니다. 한 배포가 실패해도 나머지 배포의 준비·적용을 계속 시도하므로 일부만 실패하면 `PARTIAL_FAILED`가 됩니다. 모두 실패한 경우에만 `FAILED`입니다.

## 실패 격리와 무변경 경계

`recommend`와 `show`는 추천 기록 외 상태를 바꾸지 않습니다. `accept`는 중앙 DB의 Fleet, `CREATED` 배포 세대, 배포별 runtime과 활성 예약만 만들며 Agent task, 모델 다운로드, 이미지 pull, 컨테이너 실행·중지 또는 기존 서비스 변경은 수행하지 않습니다. `status`도 읽기 전용입니다. 실제 호스트 변경은 운영자가 `fleet prepare` 또는 `fleet apply`를 명시한 경우에만 발생합니다. 두 POST API의 본문은 빈 JSON 객체만 허용하므로 모델, 노드, 명령, Docker 인자, 다운로드 우회 값을 주입할 수 없습니다.

Fleet 안의 배포는 전용 runtime ID와 저장 plan에 결합된 경로로만 준비·적용합니다. 기존 단일 배포 prepare·일반 task·rollout API에 Fleet 세대를 직접 넘겨 전용 상태와 예약 검사를 우회할 수 없습니다. 한 배포의 실패는 그 runtime에 `failure_phase`와 폐쇄형 `failure_code`를 남기고 다른 배포의 작업을 자동 취소하지 않습니다.

실패 시 Dure는 다음 동작을 자동으로 수행하지 않습니다.

- 다른 노드나 모델로 재스케줄링
- 실행 중이거나 일부 적용된 컨테이너 중지
- 과거 세대로 롤백
- Fleet 노드·GPU 예약 해제

따라서 `PARTIAL_FAILED`나 `FAILED`를 확인하면 `fleet status`의 배포별 준비·operation ID와 실패 코드를 기준으로 실제 호스트 상태를 조사해야 합니다. Fleet 예약은 조사와 명시적 복구 동안 유지되며, 현재 구현에는 Fleet 전체 취소·예약 해제 명령이 없습니다.

현재 노드 프로필 자체에는 운영자가 지정한 네트워크 영역 값이 없습니다. 내부 평가 호출자가 서버가 신뢰하는 명시적 `node UUID → network zone` 매핑을 제공하면, 모든 rank가 같은 영역인 후보의 비용은 0이고 여러 영역을 가로지르는 후보는 서로 다른 영역 수에 따라 비용을 받습니다. 매핑이 없거나 후보의 일부 노드에만 있으면 추측하지 않고 중립값을 사용합니다. 어느 경우에도 exact NCCL 증적은 필수입니다. 전용 Fleet runtime은 기존에 제출된 exact 증적을 재검사해 소비할 뿐, 다중 노드 네트워크·NCCL qualification 시험을 새로 자동 실행하지 않습니다.

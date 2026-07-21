# Fleet 후보 생성과 결정론적 스케줄러

> 상태: **읽기 전용 평가와 불변 추천 저장 구현**. 네 모델의 ACTIVE 자동 배치 프로필과 현재 유효한 exact qualification 증적을 GPU 풀에 투영하고, 서로 겹치지 않는 여러 배포 단위를 결정론적으로 조합합니다. 관리자 API·CLI로 결과를 영속 저장·조회할 수 있으며, 원자적 수락·예약과 실제 적용은 후속 단계입니다.

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
```

대응 API는 다음 두 개이며 모두 관리자 인증을 요구합니다.

- `POST /v1/admin/fleet-recommendations`
- `GET /v1/admin/fleet-recommendations/{recommendation-id}`

저장 행은 요청 정책, 전체 인벤토리·GPU 풀, 모든 후보와 투영 탈락 사유, 선택 배포, 미배정 노드, 증적·카탈로그·스케줄러 버전과 탐색 완료 상태를 포함합니다. ID는 생성 시각을 제외한 전체 정규 스냅샷의 SHA-256입니다. 같은 입력과 관측 상태를 반복하면 같은 ID의 기존 행을 반환하며, `recorded_at`은 ID 바깥의 DB 메타데이터입니다.

`show`는 저장 당시 검토 기록을 보여 줄 뿐 현재 유효성을 다시 증명하지 않습니다. API는 계산 한도나 임의 network zone을 클라이언트 입력으로 받지 않습니다. Fleet 수락·예약 엔드포인트는 아직 없으므로 추천 생성·조회만으로 배포 세대나 예약이 생기지 않습니다.

## 무변경 경계와 현재 제한

현재 Fleet 평가는 DB의 모델·증적·인벤토리를 읽고 메모리에서 후보와 조합을 계산할 뿐입니다. 추천 행, 배포 세대, task, 예약을 만들지 않고 모델 다운로드·이미지 pull·컨테이너 실행·중지를 수행하지 않습니다.

현재 노드 프로필 자체에는 운영자가 지정한 네트워크 영역 값이 없습니다. 내부 평가 호출자가 서버가 신뢰하는 명시적 `node UUID → network zone` 매핑을 제공하면, 모든 rank가 같은 영역인 후보의 비용은 0이고 여러 영역을 가로지르는 후보는 서로 다른 영역 수에 따라 비용을 받습니다. 매핑이 없거나 후보의 일부 노드에만 있으면 추측하지 않고 중립값을 사용합니다. 어느 경우에도 exact NCCL 증적은 필수입니다. 영속 추천은 구현됐지만 수락·예약·적용 경계는 후속 단계입니다.

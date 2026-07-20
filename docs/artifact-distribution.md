# 모델 아티팩트 매니페스트와 배포 계약

> 상태: **`FULL_SNAPSHOT`과 rank별 `STAGE` 준비·배포 경로 구현**. 중앙 제어면의 불변 정규 매니페스트 레지스트리, 노드 콘텐츠 주소 캐시, 명시적 deployment 준비 API·CLI와 배포 소비 게이트를 제공합니다. 0.3.19는 운영자가 정확한 `VALIDATED` variant digest를 지정하면 2·3노드 `VLLM_RAY_PP_V1`이 각 노드의 서로 다른 rank 캐시를 vLLM `sharded_state`로 소비합니다. 추천의 자동 variant 선택과 중앙 캐시 수명 주기 조정은 아직 제공하지 않습니다.

## 목적

모델 이름과 저장소 리비전만으로는 어떤 파일 바이트를 어느 노드에 준비해야 하는지 검증할 수 없습니다. Dure는 모델 아티팩트에 다음 정보를 결합한 정규화 매니페스트를 저장합니다.

- 전체 매니페스트의 SHA-256 다이제스트와 총 바이트 수
- 상대 경로별 일반 파일 크기와 파일 SHA-256
- 콘텐츠 주소로 사용할 청크 SHA-256과 청크 크기
- 각 파일을 구성하는 청크의 순서, 파일 내 오프셋과 길이
- 파일 수와 파일-청크 연결 수. 고유 청크 레코드는 다이제스트로 중복 제거됩니다.

이 레지스트리는 준비 작업이 필요한 바이트를 정확히 식별하고, 여러 파일이나 모델에서 같은 청크를 재사용할 수 있게 하는 기반입니다. `0.3.16`의 중앙 제어면은 추천을 수락해 만든 세대에 대해 준비 계획을 먼저 저장하고, 운영자가 명시적으로 적용한 뒤에만 이 매니페스트로 각 노드의 로컬 캐시와 OCI 이미지를 준비합니다.

## 현재 구현 범위

중앙 제어면은 관리자 인증을 거친 매니페스트를 하나의 트랜잭션으로 등록하고 조회합니다. 논리 구조는 다음 네 부분으로 정규화됩니다.

| 구분 | 저장하는 내용 |
|---|---|
| 매니페스트 | 연결된 모델 아티팩트, 정규 다이제스트, 전체 크기, 파일 수와 청크 연결 수 |
| 파일 | 정규화된 상대 경로, 파일 크기와 파일 SHA-256 |
| 청크 | 청크 SHA-256과 크기. 같은 다이제스트의 청크는 공유 |
| 파일-청크 연결 | 파일별 순번, 오프셋, 길이와 참조 청크 |

외래 키, 고유 제약, 범위 검사와 인덱스로 부분 등록이나 모순된 연결을 막습니다. 등록 실패 시 관련 매니페스트·파일·청크 연결은 함께 롤백됩니다. 동일한 논리 매니페스트를 다시 등록하면 기존 결과를 반환하고, 같은 다이제스트에 다른 내용을 연결하려 하면 충돌로 거부합니다.

기존 모델 아티팩트의 `manifest_digest`는 새 정규 매니페스트의 다이제스트와 정확히 같아야 합니다. 이전 버전에서 만들어진 아티팩트에는 검증된 매니페스트를 추측해 생성하지 않습니다. 해당 레코드는 명시적으로 **미등록** 상태로 남으며, 매니페스트 등록 여부와 모델 릴리스 상태는 서로 다른 사실입니다.

노드 쪽에는 다음 로컬 구성 요소가 추가되었습니다.

- `/var/lib/dure/model-store` 아래의 SHA-256 콘텐츠 주소 청크 저장소와 청크별 잠금
- `/var/lib/dure/models/.dure-staging` 아래의 매니페스트별 결정적 조립 영역
- 다운로드 중단 뒤 `Range`로 이어받는 제한 재시도와 전체 청크 SHA-256 재검증
- 파일별 결정적 부분 파일, 전체 파일 SHA-256 검사와 `fsync`
- `config.json`의 제한된 JSON 검사와 선언된 양자화 방식·캐시 식별자 일치 검사
- 검증 표식을 마지막에 기록한 뒤 `renameat2(RENAME_NOREPLACE)`로 활성화하는 `FULL_SNAPSHOT` 준비기
- 매니페스트별 시도 저널과 폐쇄형 상태·실패 코드

모델 준비기는 GPU, Docker나 vLLM을 요구하지 않는 파일 계층입니다. 단위 테스트는 가짜 전송 계층을 사용하지만 실제 `ArtifactChunkDownloader`는 HTTPS 네트워크 원본을 사용합니다. Agent는 검증된 `TrustedHTTPSOrigin` 객체를 각 노드의 root 전용 설정에서만 구성하고, 중앙 task payload의 URL·host·header·token은 받지 않습니다. 현재 전송기는 인증 token, cookie와 사용자 지정 header를 지원하지 않으므로 별도 자격 증명 없이 접근 가능한 신뢰 HTTPS origin이 필요합니다.

## 현재 `STAGE` 생성·등록·소비 범위

0.3.17의 신뢰된 오프라인 builder는 정규 매니페스트로 검증된 `FULL_SNAPSHOT`을 vLLM 0.9.0의 네이티브 `sharded_state`로 내보내고, pipeline rank별 디렉터리를 다시 같은 정규 파일·청크 형식의 매니페스트로 고정합니다. 지원 범위는 V0 executor, `Qwen2ForCausalLM` AWQ, `TP=1`입니다. remote code, LoRA·adapter, MoE, 멀티모달과 임의 아키텍처는 거부합니다.

vLLM의 sharded-state 파일명 rank는 pipeline rank가 아니라 tensor-parallel rank입니다. `TP=1`, `PP>1`에서 공용 출력 디렉터리를 사용하면 모든 PP worker가 `model-rank-0-*`를 써 충돌할 수 있으므로 각 worker 출력은 반드시 `stages/<pp-rank>`에 격리합니다. 기존 배치 계획의 `layer_start`·`layer_end`로 가중치 파일을 직접 자르지 않습니다.

하나의 stage variant는 다음 불변 입력과 출력에 결합됩니다.

- source `FULL_SNAPSHOT` 매니페스트 다이제스트
- digest 고정 runtime 이미지, vLLM 버전과 exporter build 다이제스트
- 아키텍처·양자화, TP·PP와 loader 형식
- `0..PP-1` 순서의 완전한 rank별 stage 매니페스트와 tensor 요약

등록은 모든 rank를 하나의 논리 단위로 처리합니다. 누락·중복·범위 밖 rank, source/runtime/topology 불일치와 같은 고정 입력에서 달라진 stage 출력은 거부합니다. 성공한 등록은 `DRAFT`이며 실제 GPU에서 export와 load를 모두 검증한 최신 `GPU_EXPORT_LOAD/PASSED` 증적만 `VALIDATED` 승격을 허용합니다. 전제 조건 부족을 나타내는 `NOT_RUN`, synthetic 검사와 `FAILED`는 승격 근거가 아닙니다. 새 validation run 증적은 `DRAFT`에서만 추가합니다. 이미 등록한 동일 run의 정확한 재전송은 상태 전환 뒤에도 기존 결과를 반환하지만, `VALIDATED`와 `REVOKED`에 새 run을 추가하는 요청은 거부합니다. 검증된 variant의 신뢰가 깨졌다면 운영자가 영향 범위를 검토해 명시적으로 `REVOKED`로 닫고, 수정된 계약은 새 `DRAFT` variant에서 검증합니다. builder harness는 `PP=1`, 별도 분산 runtime harness는 준비된 `PP=2/3`의 load·최소 추론을 검증합니다.

variant 등록·증적 기록·상태 전이는 중앙 DB만 바꾸며 Agent task, 모델 다운로드, 이미지 pull, 추천·배포 세대 생성과 기존 컨테이너 변경을 만들지 않습니다. 별도의 준비 preview·apply에서 `--stage-variant`를 명시해야 중앙이 서버 UUID와 pipeline rank를 정확한 stage 매니페스트에 결합합니다. strict runtime은 variant·contract·source·runtime·topology·rank·tensor-key 전체에서 파생한 cache identity와 시작 직전 전체 재해시를 요구합니다. 자세한 빌더·검증·실패 경계는 [stage artifact 문서](stage-artifacts.md)를 따릅니다.

vLLM·PyTorch·safetensors·CUDA 계열 heavy dependency는 기본 Debian 패키지에 포함하지 않습니다. 별도의 digest 고정 OCI builder 환경에서만 설치하고 운영 Agent나 중앙 서버를 변환 작업자로 사용하지 않습니다.

## 정규 형식과 다이제스트

서버는 입력 배열 순서와 JSON 표현 차이를 제거한 정규 구조를 만든 뒤 UTF-8 정규 JSON의 SHA-256을 계산합니다. 같은 파일·청크 관계를 표현한 입력은 제출 순서가 달라도 같은 매니페스트 다이제스트가 됩니다. 다이제스트 형식은 `sha256:<64자리 소문자 16진수>`입니다.

등록 입력은 다음 조건을 모두 만족해야 합니다.

- 파일은 루트 기준의 상대 경로로 표현한 일반 파일만 허용합니다.
- 절대 경로, 빈 경로, `.`·`..` 구간, 중복 구분자, 역슬래시와 정규화 후 중복되는 경로를 거부합니다.
- 심볼릭 링크, 디렉터리, 장치 파일과 소켓을 파일 항목으로 표현할 수 없습니다.
- 파일과 청크 크기, 오프셋, 길이와 순번은 음수가 아니며 허용 상한 안에 있어야 합니다.
- 한 파일의 연결은 순번대로 오프셋 0부터 시작해 틈이나 겹침 없이 파일 크기를 정확히 덮어야 합니다.
- 연결 길이와 참조 청크 크기가 일치해야 합니다. 파일·청크 연결 수와 전체 크기는 검증된 정규 구조에서 서버가 계산합니다.
- 알 수 없는 필드와 과도한 파일·청크·연결 수를 거부합니다.

청크 공유는 SHA-256 다이제스트와 크기가 모두 같은 경우에만 허용합니다. 다이제스트는 같지만 크기가 다르면 저장하지 않습니다.

## 관리자 등록과 조회 계약

관리자 등록 기능은 기존 모델 아티팩트를 지정하고 정규 매니페스트 문서를 제출받습니다. 현재 CLI와 API는 다음과 같습니다.

```bash
dure admin artifact-manifest register <artifact-id> --file manifest.json
dure admin artifact-manifest show <artifact-id>
```

```text
POST /v1/admin/model-artifacts/{artifact_id}/manifest
GET  /v1/admin/model-artifacts/{artifact_id}/manifest
```

등록 본문의 최상위 필드는 `schema_version`과 `files`뿐입니다. 각 파일은 `path`, `kind=REGULAR`, `size_bytes`, `sha256`, `chunks`를 가지며, 각 청크 연결은 `ordinal`, `offset_bytes`, `length_bytes`, `sha256`만 가집니다. 서버는 다음 순서로 처리합니다.

1. 관리자 인증과 요청 스키마를 검사합니다.
2. 대상 모델 아티팩트와 기존 `manifest_digest`를 확인합니다.
3. 경로·크기·청크 범위와 연결 관계를 모두 검증합니다.
4. 정규 JSON 다이제스트를 계산해 기존 다이제스트와 비교합니다.
5. 매니페스트, 파일, 공유 청크와 연결을 원자적으로 저장합니다.

조회 기능은 정규화된 파일·청크 순서와 계산된 요약을 반환합니다. 안정된 계약은 인증된 등록, 다이제스트별 멱등성, 불변 조회와 허용 필드 밖 입력 거부입니다.

등록 요청에는 다운로드 원본의 접근 토큰, 쿠키, 자격 증명, 임의 HTTP 헤더, 호스트 경로, 셸 명령, Docker 인자, 환경 변수나 마운트를 넣을 수 없습니다. 원본 저장소가 인증을 요구하더라도 토큰을 매니페스트나 중앙 DB에 보관하지 않습니다.

등록과 조회는 중앙 DB만 읽거나 변경합니다. 다음 동작은 발생하지 않습니다.

- Agent 작업 생성이나 임대
- 모델 파일 다운로드 또는 노드 간 전송
- OCI 이미지 내려받기
- 캐시 디렉터리 생성, 파일 조립 또는 기존 파일 삭제
- Docker·Ray·vLLM 실행과 기존 배포 변경
- 모델 추천, 릴리스 승격 또는 배포 세대 자동 생성

## 명시적 중앙 준비 계약

추천 수락으로 만들어진 배포 세대는 준비와 실행을 분리합니다. 같은 정규 UUID 요청 ID를 사용해 먼저 preview를 만들고, 응답을 검토한 다음에만 `--apply`를 추가합니다.

```bash
# DB에 불변 준비 계획과 노드 행만 저장하며 task는 만들지 않습니다.
dure admin deployment prepare <deployment-id> --request-id <request-uuid>

# 같은 요청 ID의 계획을 다시 검증한 뒤 모델 작업을 큐잉합니다.
dure admin deployment prepare <deployment-id> \
  --request-id <request-uuid> --apply

# 준비, 노드, 단계와 시도 상태를 조회합니다.
dure admin deployment preparation <preparation-id>
```

```text
POST /v1/admin/deployments/{deployment_id}/prepare
GET  /v1/admin/deployment-preparations/{preparation_id}
```

preview는 배포 세대, 정규 매니페스트, 런타임 이미지와 정확한 노드 UUID 집합을 하나의 준비 계획에 결합하고 task를 0개 만듭니다. preview와 최초 적용 시 서버는 전체 대상 노드를 정렬된 순서로 잠근 뒤 다음 조건을 다시 검사합니다.

- 추천을 수락해 만든 배포 세대이고 등록된 정규 매니페스트가 아티팩트 다이제스트와 정확히 같습니다.
- 모든 대상 노드가 승인되어 있고 최근 heartbeat와 신선한 인벤토리를 보유합니다.
- 인벤토리의 남은 디스크가 등록 매니페스트 전체 크기와 보수적 여유 공간을 충족합니다.
- 런타임 이미지가 정확한 OCI SHA-256 다이제스트로 고정돼 있습니다. Docker의 canonical `RepoDigest`와 모호하지 않게 비교할 수 있도록 형식은 `repository@sha256:...`만 허용하며 `repository:tag@sha256:...`는 거부합니다. registry 포트의 콜론은 허용됩니다.
- 준비 요청, 배포 세대, 모델·리비전·매니페스트, 런타임과 노드 집합이 preview 이후 바뀌지 않았습니다.

한 조건이라도 불확실하거나 달라지면 작업을 일부만 만들지 않고 실패 안전 방식으로 거부합니다. 같은 요청 ID와 같은 내용의 재전송은 기존 준비를 반환하고, 같은 요청 ID에 다른 내용을 연결할 수 없습니다.

실패 단계 재시도에서는 이미 받은 CAS 청크와 조립 중인 staging이 디스크를 사용하므로 중앙의 최초 최악 조건(`전체 청크 + 전체 조립본 + 여유 공간`)을 그대로 다시 적용하지 않습니다. 중앙은 승인·온라인·프로필 신선도·안정 하드웨어·활성 작업·매니페스트·런타임 결합을 다시 검사하고, Agent가 실제 CAS와 staging이 놓인 각 파일시스템에서 남은 바이트와 고정 여유 공간을 작업 시작 직전에 권위 있게 계산합니다. 부족하면 네트워크나 final 활성화 전에 `MODEL_STORE_DISK_INSUFFICIENT`로 실패합니다. 따라서 재시도 task가 큐잉됐다는 사실 자체는 중앙이 최신 디스크 용량을 보증했다는 뜻이 아닙니다.

적용 뒤 각 노드는 반드시 다음 순서로 진행합니다.

```text
PREPARE_MODEL
    ↓ 전체 청크·파일·marker와 정확한 final 경로 검증 성공
PREPARE_IMAGE
    ↓ 정확한 RepoDigest inspect 성공
노드 준비 완료
```

`PREPARE_MODEL`은 등록된 정규 매니페스트를 Agent 전용 인증 API로 읽고 노드 로컬 origin으로만 바이트를 받습니다. 기본 `FULL_SNAPSHOT`은 `/var/lib/dure/models/sha256-<manifesthex>`, 명시적 `STAGE`는 `/var/lib/dure/models/stages/sha256-<복합-cache-identity>`에 준비합니다. STAGE task마다 해당 노드 rank의 서로 다른 매니페스트를 사용하며 marker와 tensor-key 계약까지 검증합니다. task payload는 raw URL, 자격 증명, 임의 header, 호스트 경로, 셸 명령이나 Python 코드를 표현할 수 없습니다. `PREPARE_IMAGE`는 먼저 정확한 digest 참조를 `docker image inspect`하고, 없을 때만 같은 참조를 `docker pull`한 뒤 다시 inspect합니다. 이 작업은 컨테이너를 run·start·stop·remove하지 않습니다.

준비 전체 상태는 `PREPARED`, `QUEUED`, `RUNNING`, `SUCCEEDED`, `PARTIAL_FAILED`, `FAILED`의 폐쇄형 값입니다. 모델 단계가 성공한 노드에만 이미지 단계가 큐잉됩니다. 일부 노드만 완료되면 성공 증적을 보존한 채 `PARTIAL_FAILED`로 끝나며, 어떤 노드도 준비되지 못하면 `FAILED`입니다. 원인을 해결하고 같은 요청과 `--apply`를 다시 보내면 다음 규칙으로 재시도합니다.

- 모델 단계가 실패한 노드는 새 모델 `attempt_no`로 다시 시작하고 이미지 작업은 만들지 않습니다.
- 모델은 성공했지만 이미지 단계가 실패한 노드는 모델을 반복하지 않고 새 이미지 `attempt_no`만 큐잉합니다.
- 이미 두 단계를 성공한 노드는 다시 실행하지 않습니다.
- 완료·실패 보고는 preparation, 노드, 단계, task ID와 현재 시도 번호가 모두 일치할 때만 반영합니다. 임대 만료나 재시도 뒤 과거 작업이 늦게 보고해도 새 상태를 덮어쓰지 못합니다.

일반 task 생성 API로 `PREPARE_MODEL`이나 `PREPARE_IMAGE`를 만들 수 없습니다. 전용 준비 서비스가 고정한 식별자와 payload만 Agent에 전달하며, Agent의 로컬 완료 저널 재전송도 같은 결합을 다시 검증합니다.

모든 노드의 두 단계가 성공하면 준비 계획의 정확한 콘텐츠 주소 캐시 경로와 이미지 다이제스트가 해당 추천 세대의 실행 증거가 됩니다. 추천 세대의 일반 apply는 이 `SUCCEEDED` 증적이 없거나 배포·노드·매니페스트·경로·이미지 결합이 다르면 거부합니다. 기존 수동 deployment의 명시적 로컬 캐시 경로는 호환되지만, 추천 세대가 그 legacy 경로로 준비 게이트를 우회할 수는 없습니다.

롤백은 준비 서비스의 네트워크 복구 경로가 아닙니다. 추천으로 만들어진 롤백 대상에는 과거에 성공한 정확한 준비 증적이 있어야 하며, 기존 검증 캐시와 로컬 다이제스트 이미지만 사용합니다. 롤백 중 새 `PREPARE_MODEL`·`PREPARE_IMAGE`, 모델 다운로드나 이미지 pull은 만들지 않습니다. 준비 증적은 완료 당시의 사실이며 이후 수동 삭제를 실시간 보증하지 않으므로, 운영자는 롤백 전에 대상 캐시와 이미지를 별도로 확인해야 합니다. 사라진 아티팩트는 자동 복구되지 않고 대상 시작 단계가 실패할 수 있습니다.

## 노드 캐시 준비 계약

Agent 준비기는 노드 로컬 설정으로 생성한 검증된 `TrustedHTTPSOrigin` 객체에서만 청크를 받습니다. 최초 object URL은 이 객체와 매니페스트의 청크 SHA-256으로 만듭니다. redirect는 query·fragment·userinfo 없이 객체의 허용 host·port 안에 있어야 하지만, 그 host의 redirect path 자체는 신뢰 origin의 범위입니다. 매니페스트·중앙 DB·task·결과·로컬 저널에는 raw URL이나 자격 증명을 넣지 않습니다. 압축 전송, `Transfer-Encoding`, 모호한 `Content-Length`와 범위가 다른 `Content-Range`도 거부합니다.

처리 순서는 다음과 같습니다.

1. `FULL_SNAPSHOT`, 매니페스트 다이제스트, `config.json`, 예약 경로와 전체 경로 구조를 검사합니다.
2. Dure 전용 루트, 소유자, 쓰기 권한과 symlink 조상을 검사하고 매니페스트별 잠금을 얻습니다.
3. 검증된 기존 청크와 부분 다운로드의 실제 할당량, 조립에 남은 바이트와 기본 여유 공간을 반영해 디스크를 먼저 검사합니다.
4. 없는 청크만 내려받고, 이어받은 부분을 포함한 전체 바이트의 SHA-256을 검사한 뒤 기존 CAS 항목을 덮어쓰지 않고 게시합니다.
5. 매니페스트별 고정 staging 하나에서 파일별 부분 조립을 이어가며, 완성 파일은 크기와 전체 SHA-256을 다시 검사합니다.
6. `config.json`이 제한 크기의 JSON 객체인지 검사하고, 양자화 방식이 선언되어 있으면 캐시 식별자와 정확히 같은지 확인합니다.
7. 예상 파일만 존재하고 symlink·hardlink·special file·추가 파일이 없는지 검사합니다.
8. 모든 파일과 디렉터리를 `fsync`하고 v2 `.dure-model.json`을 마지막에 쓴 뒤, 기존 대상을 교체하지 않는 원자적 rename으로 최종 캐시를 활성화합니다.
9. 활성화 뒤 같은 전체 검사를 다시 수행하고 성공 저널을 기록합니다.

`FULL_SNAPSHOT` 최종 디렉터리는 매니페스트 다이제스트로 결정됩니다. `STAGE` 최종 디렉터리는 매니페스트 하나만이 아니라 source·variant·runtime·exporter·topology·rank·tensor-key를 포함한 복합 cache identity로 결정됩니다. 같은 캐시가 이미 있으면 전체 트리, marker, 정규 매니페스트 sidecar, 파일 해시와 loader 계약을 다시 검사한 뒤에만 멱등 재사용합니다. 기존 최종 경로가 비어 있더라도 기대 캐시와 다르면 덮어쓰거나 삭제하지 않습니다.

## 실패, 재시도와 운영자 복구

실패는 검증되지 않은 캐시를 활성 상태로 보이게 하지 않습니다.

- `MODEL_STORE_DOWNLOAD_TIMEOUT`과 응답 body를 읽던 중의 `MODEL_STORE_DOWNLOAD_INTERRUPTED`는 검증 가능한 `.part`를 보존하고 제한 횟수 안에서 같은 offset부터 이어받습니다.
- 응답 계약 위반, non-timeout DNS·TLS·connect 거부 또는 청크 digest 불일치는 `MODEL_STORE_DOWNLOAD_REJECTED`나 `MODEL_STORE_DIGEST_MISMATCH`로 분류하고 해당 청크 부분 파일을 정확히 0바이트로 되돌린 뒤 제한 재시도합니다. 최종 실패 코드에는 원격 본문·URL·예외 원문을 넣지 않습니다.
- 조립 중단은 매니페스트별 staging 하나와 파일별 결정적 부분 파일에 남습니다. 다음 호출은 기존 prefix를 CAS 바이트와 다시 비교한 뒤 이어 쓰므로 반복 실패마다 새 모델 크기만큼 디스크를 누적하지 않습니다.
- CAS 충돌, 오염된 staging, 추가 파일, symlink·hardlink·special file, 잘못된 final은 보존한 채 실패합니다. 자동 덮어쓰기·재귀 삭제·캐시 퇴출은 하지 않습니다.
- 디스크 부족은 가능하면 네트워크와 staging 생성 전에 거부합니다. 쓰기 도중 부족해지면 유효 marker나 final은 게시하지 않고, 파일·marker의 결정적 부분 파일만 남겨 다음 호출에서 검증한 뒤 재개합니다.
- marker가 있는 staging은 marker-last 규칙에 따라 전체 트리와 식별자가 정확할 때만 활성화를 다시 시도합니다.

디스크 사전 검사는 공간 예약이 아닙니다. 다른 프로세스나 동시 준비가 공간을 소비해 쓰기 중 `ENOSPC`가 발생할 수 있으며, 이 경우 marker와 final 없이 `MODEL_STORE_DISK_INSUFFICIENT`로 실패합니다. 공간을 확보한 뒤 같은 매니페스트 digest를 재시도합니다.

저장소와 저널 경계 자체가 정상이라면 시도 저널은 `/var/lib/dure/model-store/attempts/<manifesthex>/journal.json`에 로컬 마지막 상태를 남깁니다. 루트·권한·저널 I/O 자체가 실패하면 원래 작업의 실패 저널도 기록하지 못할 수 있습니다. 어느 경우든 중앙 operation 진행률, 노드 `READY` 증적 또는 감사 로그가 아닙니다.

### 실패 코드 분류와 재시도 기준

운영자는 준비 조회 결과에서 노드, `MODEL` 또는 `IMAGE` 단계, task ID, 시도 번호와 실패 코드를 함께 기록해야 합니다. 상태만 보고 재시도하거나 중앙 데이터베이스의 task·시도 행을 직접 고치면 현재 시도의 fencing을 우회할 수 있습니다. 대표 코드는 다음과 같이 분류합니다.

| 구간 | 대표 실패 코드 | 운영자 확인 | 복구와 재시도 조건 |
| --- | --- | --- | --- |
| 중앙 preview·최초 apply | `PREPARATION_RECOMMENDATION_REQUIRED`, `PREPARATION_RECOMMENDATION_MISSING`, `PREPARATION_RECOMMENDATION_INVALID`, `PREPARATION_RECOMMENDATION_STALE`, `PREPARATION_ASSIGNMENT_INVALID`, `PREPARATION_NODE_MISSING`, `PREPARATION_NODE_UNAPPROVED`, `PREPARATION_NODE_OFFLINE`, `PREPARATION_AGENT_UNSUPPORTED`, `PREPARATION_PROFILE_STALE`, `PREPARATION_PROFILE_INVALID`, `PREPARATION_INVENTORY_STALE`, `PREPARATION_WORKLOAD_ACTIVE`, `PREPARATION_RUNTIME_UNAVAILABLE`, `PREPARATION_NODE_BUSY`, `PREPARATION_ARTIFACT_STALE`, `PREPARATION_MANIFEST_REQUIRED`, `PREPARATION_MANIFEST_INVALID`, `PREPARATION_IMAGE_INVALID`, `PREPARATION_DISK_INSUFFICIENT` | 수락된 추천·배포 세대, 정확한 노드 UUID, 승인·heartbeat·Agent 버전·프로필과 인벤토리 시각, 활성 작업, 정규 매니페스트, `repository@sha256:...` 런타임과 모델 디스크를 확인합니다. | 일시적인 노드·프로필·작업·디스크 조건만 복구했고 불변 계획이 같으면 같은 request ID로 apply합니다. 추천, 노드 집합, 매니페스트나 런타임처럼 불변 입력을 바꿔야 하면 새 추천·배포 세대와 새 request ID로 preview부터 시작합니다. |
| 중앙 요청·동시성 결합 | `PREPARATION_REQUEST_INVALID`, `PREPARATION_REQUEST_CONFLICT`, `PREPARATION_PLAN_CONFLICT`, `PREPARATION_ATTEMPT_CONFLICT`, `DEPLOYMENT_NOT_FOUND`, `ARTIFACT_PREPARATION_NOT_FOUND` | 정규 UUID인지, request ID가 이미 다른 배포에 결합됐는지, 저장된 계획과 현재 배포가 같은지, 현재 task와 시도 번호가 일치하는지 확인합니다. | 기존 행이나 request ID를 재사용해 다른 계획을 덮어쓰지 않습니다. 조회 불일치나 시도 충돌은 자동 재시도를 멈추고 API·감사 이벤트·서버 버전과 데이터 결합을 조사합니다. 계획을 바꿔야 하면 새 배포 세대와 request ID를 사용합니다. |
| 배포 소비 게이트 | `DEPLOYMENT_ARTIFACTS_NOT_PREPARED`, `DEPLOYMENT_PREPARATION_INVALID`, `DEPLOYMENT_MODEL_RELEASE_REVOKED` | 모든 대상 노드의 정확한 모델·이미지 성공 증적, 저장된 세대·추천·매니페스트·런타임 결합과 모델 릴리스 상태를 확인합니다. | 준비 미완료는 같은 준비 요청의 실패 단계만 재시도합니다. 증적 결합이 손상됐거나 릴리스가 `REVOKED`이면 apply·restart·verify·rollback 시작을 우회하지 않습니다. `DEPRECATED`는 이미 성공한 정확한 준비 증적의 소비와 검증된 세대 rollback에는 허용하지만 새 준비와 재시도에는 허용하지 않습니다. 긴급 `STOP_DEPLOYMENT`와 rollback의 `STOP_SOURCE`는 계속 사용할 수 있습니다. |
| Agent payload·신원·설정 | `PREPARATION_PAYLOAD_REJECTED`, `PREPARATION_NODE_MISMATCH`, `PREPARATION_BINDING_MISMATCH`, `PREPARATION_HISTORY_INVALID`, `PREPARATION_ORIGIN_UNAVAILABLE`, `PREPARATION_MANIFEST_UNAVAILABLE` | task의 노드 UUID·단계·시도 결합, 중앙과 Agent 버전, root 전용 Agent 설정의 `artifact_origin`, 노드 승인·자격 증명과 매니페스트 조회 가능 여부를 확인합니다. 설정 파일 원문이나 자격 증명은 로그·지원 요청에 첨부하지 않습니다. | 스키마·신원·설정 원인을 고치기 전에는 반복하지 않습니다. 현재 시도를 수정해 보고하지 말고 원인을 고친 뒤 같은 준비 요청을 apply해 새 시도를 만듭니다. 불변 결합이 달라졌다면 새 준비 요청이 필요합니다. |
| 모델 전송 | `MODEL_STORE_DOWNLOAD_TIMEOUT`, `MODEL_STORE_DOWNLOAD_INTERRUPTED`, `MODEL_STORE_DOWNLOAD_REJECTED`, `MODEL_STORE_DIGEST_MISMATCH` | 신뢰 origin의 DNS·TLS·연결, 응답 길이·범위 계약과 해당 digest object가 정규 매니페스트의 바이트인지 확인합니다. | timeout·body 중단은 검증된 부분에서 재개할 수 있습니다. 응답 거부·digest 불일치는 잘못된 origin object나 매니페스트를 먼저 바로잡아야 하며, 해당 청크는 0바이트부터 다시 받습니다. |
| 모델 저장소 구조·무결성 | `MODEL_STORE_INVALID`, `MODEL_STORE_ROOT_UNSAFE`, `MODEL_STORE_PATH_COLLISION`, `MODEL_STORE_LOCK_BUSY`, `MODEL_STORE_JOURNAL_CORRUPT`, `MODEL_STORE_CHUNK_COLLISION`, `MODEL_STORE_CHUNK_CORRUPT`, `MODEL_STORE_MANIFEST_MISMATCH`, `MODEL_STORE_CACHE_KIND_UNSUPPORTED`, `MODEL_STORE_FILE_INTEGRITY_FAILED`, `MODEL_STORE_TARGET_COLLISION` | Dure 전용 경로의 소유권·권한·symlink, 동시 준비, 저널과 정확한 digest의 CAS·staging·final, `FULL_SNAPSHOT` 계약을 확인합니다. | 잠금 경쟁은 실행 중인 동일 준비가 끝난 뒤 재시도합니다. 충돌·오염·계약 불일치는 자동 삭제나 덮어쓰기를 하지 않고 원인과 참조를 조사한 뒤 정확한 단일 digest만 수동 격리합니다. |
| 모델 저장소 자원·호스트 기능 | `MODEL_STORE_DISK_INSUFFICIENT`, `MODEL_STORE_IO_FAILED`, `MODEL_STORE_ATOMIC_ACTIVATION_UNAVAILABLE` | CAS와 모델 staging이 실제로 놓인 각 파일시스템의 남은 공간·inode·마운트 상태·I/O 오류와 `renameat2(RENAME_NOREPLACE)` 지원을 확인합니다. | 참조되지 않은 데이터만 별도 절차로 정리하고 공간·파일시스템 문제를 해결한 뒤 같은 digest를 재시도합니다. 원자적 no-replace를 지원하지 않는 파일시스템에는 copy·교체 fallback이 없으므로 지원 파일시스템으로 옮기기 전에는 재시도하지 않습니다. |
| OCI 이미지 준비 | `PREPARATION_RUNTIME_UNAVAILABLE`, `PREPARATION_IMAGE_PULL_FAILED`, `PREPARATION_IMAGE_INSPECT_FAILED`, `PREPARATION_IMAGE_DIGEST_MISMATCH` | Docker daemon과 Agent 권한, Docker 데이터 루트의 공간, registry·TLS·네트워크·인증 경계, exact digest의 존재와 inspect 결과의 canonical `RepoDigests`를 확인합니다. | daemon·용량·registry 문제를 고친 뒤 같은 request ID로 apply하면 모델 성공 노드는 실패한 `IMAGE` 단계만 새 시도로 실행합니다. 가변 tag로 바꾸거나 digest 불일치를 허용하지 않으며, Dure는 NVIDIA host driver를 자동 설치·변경하지 않습니다. |
| 임대·취소·철회·결과 검증 | `PREPARATION_LEASE_EXPIRED`, `PREPARATION_TASK_CANCELED`, `PREPARATION_NODE_REVOKED`, `PREPARATION_RESULT_REJECTED`, `PREPARATION_EXECUTION_FAILED` | 현재 시도와 임대, Agent·네트워크 상태, 노드 승인·자격 증명, 결과 스키마와 등록 매니페스트의 크기·파일 수를 확인합니다. `PREPARATION_EXECUTION_FAILED`는 안전하게 축약된 예기치 않은 실행 실패이므로 Agent와 호스트 서비스 로그를 함께 조사합니다. | 과거 시도를 고쳐서 다시 보고하지 않습니다. 아래 fencing 조건을 만족하고 원인을 제거한 뒤 같은 준비 요청을 apply해 새 시도를 만듭니다. 노드를 철회했다면 신뢰와 자격 증명을 복구하고 중앙에서 다시 승인하기 전에는 재시도하지 않습니다. |

### 이미지 pull 용량의 제한

중앙의 디스크 사전 검사는 정규 모델 매니페스트를 기준으로 전체 청크, 조립본과 고정 여유 공간을 계산합니다. OCI 이미지의 압축 layer 크기, unpack 뒤 크기, Docker 데이터 루트가 모델 저장소와 같은 파일시스템인지, 기존 layer 공유량은 준비 계획에 포함되지 않습니다. 따라서 중앙 검사를 통과했거나 `PREPARATION_DISK_INSUFFICIENT`가 없다는 사실은 이미지 pull 공간을 보증하지 않습니다.

`PREPARATION_IMAGE_PULL_FAILED`이면 노드에서 read-only `docker system df`, Docker 데이터 루트와 exact digest의 registry 존재 여부를 확인합니다. pull 뒤 `PREPARATION_IMAGE_INSPECT_FAILED`이면 daemon·권한과 exact 참조의 inspect 가능 여부를, `PREPARATION_IMAGE_DIGEST_MISMATCH`이면 `RepoDigests`에 계획의 정확한 `repository@sha256:...`가 있는지를 확인합니다. task payload나 매니페스트에 registry credential·header를 넣지 않고, tag 재지정으로 digest 검사를 우회하지 않습니다. Dure는 이미지나 layer를 자동 prune하지 않으므로 수동 정리도 Dure와 다른 작업 부하가 참조하지 않는 정확한 대상을 증명한 뒤에만 수행합니다.

### lease, revoke와 늦은 결과의 fencing

- Agent는 완료 결과를 로컬 pending report에 먼저 보존하고 중앙 보고를 시도합니다. 완료 응답만 유실된 같은 현재 시도는 멱등 재전송할 수 있지만, 이것이 임대가 끝난 과거 시도를 되살리지는 않습니다.
- `PREPARATION_LEASE_EXPIRED`는 현재 실행 시도를 실패로 닫습니다. 이후 도착한 완료는 새 상태를 바꾸지 못합니다. 운영자는 해당 호스트 작업이 끝났거나 안전하게 중단됐고 Agent의 heartbeat·임대 갱신이 정상인지 확인한 뒤 같은 request ID로 apply해 새 시도를 만듭니다. 검증된 CAS는 다음 시도에서 재검사 후 재사용할 수 있습니다.
- 명시적 취소는 queued task를 `CANCELED`로 닫고, 만료된 running task는 임대 만료 실패로 닫습니다. 활성 임대의 running task를 취소로 강제 종료하지 않습니다. 호스트 작업 종료와 남은 staging을 확인한 뒤에만 재시도합니다.
- 노드 revoke 시 queued 준비는 취소되고 running 준비는 `PREPARATION_NODE_REVOKED`로 실패하며 기존 자격 증명으로 보내는 후속 보고는 권한을 갖지 않습니다. 침해·오등록 원인을 조사하고 자격 증명을 교체한 뒤 노드를 다시 승인하기 전에는 준비를 재개하지 않습니다.
- task ID, preparation·노드·단계 또는 현재 시도 번호가 다른 늦은 완료·실패는 HTTP `409` 충돌로 거부되며 최신 시도를 덮어쓰지 않습니다. 운영자가 과거 결과를 새 시도 결과로 복사해서는 안 됩니다.
- `PREPARATION_RESULT_REJECTED`는 결과가 폐쇄형 스키마나 등록 매니페스트 증적과 맞지 않는 경우이며, 중앙은 해당 task와 시도를 `FAILED`로 기록한 뒤 완료 요청에 HTTP `422`를 반환합니다. 같은 시도의 JSON을 고쳐 재전송하지 말고 Agent·controller 버전과 task 결합을 바로잡은 뒤 새 시도로 재시도합니다.

현재 버전에는 참조 검사, quarantine 또는 삭제 CLI가 없습니다. 반복 실패를 복구할 때는 먼저 같은 매니페스트의 준비 실행이 없음을 확인하고 저널의 폐쇄형 실패 코드와 정확한 digest를 기록합니다. staging이나 비활성 final도 어떤 배포·벤치마크·준비가 참조하지 않는다는 사실을 확인한 뒤 정확한 단일 digest 경로만 별도 보존 위치로 옮겨야 합니다. CAS 청크는 여러 매니페스트가 공유하므로 모든 등록 매니페스트와 진행 중 준비의 미참조를 증명할 수 없다면 옮기거나 삭제하면 안 됩니다. glob, 상위 모델 루트, 실행 중인 캐시를 대상으로 재귀 삭제하면 안 됩니다. 감사와 전역 참조 검사를 포함한 공식 quarantine 명령은 후속 범위입니다.

`/var/lib/dure`는 패키지 설치 시 `root:dure` 소유의 `0750` 경계로 유지하고, 중앙 서버가 쓸 수 있는 상태는 `/var/lib/dure/server`로 분리합니다. Agent 캐시의 부모가 다른 사용자에게 쓰기 가능하거나 symlink이면 준비를 시작하지 않습니다. 이 권한 검사는 NVIDIA host driver를 설치하거나 변경하지 않습니다.

## 알려진 제한

- Linux kernel이나 대상 파일시스템이 `renameat2(RENAME_NOREPLACE)`를 지원하지 않으면 `MODEL_STORE_ATOMIC_ACTIVATION_UNAVAILABLE`로 실패합니다. copy 또는 기존 대상 교체 fallback은 없습니다.
- 디스크 사전 검사는 예약이 아니며, 검사 뒤 다른 쓰기가 공간을 소비하면 조립 중 `ENOSPC`로 실패할 수 있습니다. 파일 또는 marker 부분 파일은 남을 수 있지만 유효 marker·final은 게시하지 않고 같은 digest 재시도에서 검증해 이어갑니다.
- 검증된 CAS 청크는 준비 성공 뒤에도 유지됩니다. 자동 eviction이 없으므로 `FULL_SNAPSHOT`과 함께 추가 디스크 공간을 계속 차지할 수 있습니다.
- attempt journal은 매니페스트별 append-only 감사 이력이 아니라 마지막 상태 한 건을 원자적으로 교체하는 로컬 진단값입니다.
- 현재 HTTPS 전송기는 인증 token, cookie 또는 사용자 지정 header를 지원하지 않습니다. 별도 인증 정보 없이 네트워크와 TLS 경계에서 접근 가능한 신뢰 origin이 필요합니다.
- 중앙 준비 상태와 노드별 시도 증적은 제공하지만 자동 경보, 전역 참조 검사와 quarantine는 아직 없습니다.
- `STAGE` variant는 준비 요청에서 digest를 명시해야 하며 추천기가 자동 선택하지 않습니다. rank별 노드 다운로드·원자적 활성화와 stage-local `sharded_state` 소비는 제공하지만 P2P 청크 전송은 없습니다.

## 무결성과 신뢰 경계

SHA-256 다이제스트는 나중에 받은 바이트가 등록된 기대값과 같은지를 검사하는 무결성 식별자입니다. 그러나 다이제스트 자체는 다음을 증명하지 않습니다.

- 매니페스트를 실제 모델 게시자가 만들었다는 사실
- 모델 리비전과 파일 내용이 신뢰할 수 있는 출처에서 왔다는 사실
- 등록 관리자가 악의적이거나 잘못된 매니페스트를 제출하지 않았다는 사실
- 라이선스, 악성 모델 코드 또는 안전성을 검토했다는 사실

공격자가 매니페스트와 파일을 함께 바꿀 수 있다면 새 SHA-256도 계산할 수 있습니다. 따라서 현재 레지스트리와 노드 준비기는 게시자 서명, 투명성 로그, 공급망 증명이나 신뢰할 수 있는 원본 출처를 대신하지 않습니다. 중앙의 `0.3.14` 등록 경로는 제출 당시 실제 파일을 읽지 않으며, `0.3.16` 준비기는 나중에 받은 바이트가 등록된 기대 해시와 같은지만 검증합니다.

운영자는 신뢰된 오프라인 작성 환경에서 매니페스트를 만들고, 모델 게시자와 리비전·라이선스를 별도로 확인해야 합니다. 게시자 서명과 provenance 검증이 추가되기 전에는 관리자 인증 경계 안에서만 등록 기능을 사용합니다.

## 다음 단계

정규 매니페스트 등록만으로 모델이 특정 노드에 준비됐거나 실행 가능하다고 판단해서는 안 됩니다. `0.3.16`에서는 전용 준비 적용을 완료해 두 단계의 정확한 노드 증적이 모두 성공해야 추천 세대 apply가 이를 소비할 수 있습니다. 추천·수락과 preview는 계속 다운로드나 호스트 변경을 만들지 않으며, `benchmark-runs/prepare`도 모델 바이트가 아니라 DB 실행 문맥만 준비합니다.

후속 버전은 추천·수락이 정확한 source/runtime/topology에 맞는 `VALIDATED` variant를 결정론적으로 선택하고, probe 기반 중앙 캐시 투영에 `READY`·`STALE`·`MISSING`·`CORRUPT`·`QUARANTINED` 상태와 감사 가능한 격리 절차를 추가할 계획입니다. 현재 rank별 준비가 구현됐더라도 stage 등록만을 분산 설치 완료로, 수동 캐시 이동을 공식 quarantine로 해석하지 않습니다.

그 밖의 후속 범위는 게시자·이미지 서명과 provenance, 전역 참조 검사·eviction, origin 인증 수단과 중앙 자동 경보입니다.

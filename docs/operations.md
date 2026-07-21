# 중앙 제어면 운영 절차

## 중앙 서버

중앙 호스트에는 중앙 제어면 추가 의존성을 설치합니다. APT 패키지는 이동 가능한 노드 CLI/에이전트용이며 서버 의존성이나 서버 systemd unit을 설치하지 않습니다.

```bash
python3 -m pip install -e '.[server]'
```

secret은 저장소 밖에 둡니다.

```dotenv
DURE_DATABASE_URL=postgresql+psycopg://dure:password@127.0.0.1/dure
DURE_ADMIN_TOKEN=<random-secret>
```

새 버전 시작 전 migration을 적용합니다.

```bash
set -a
source /etc/dure/server.env
set +a
dure-server --migrate
systemctl restart dure-server
```

패키지의 개발/LAN service는 `0.0.0.0:8081`에서 listen합니다. 운영 환경에서는 application을 loopback에 bind하고 TLS reverse proxy를 통해 HTTPS 443만 노출해야 합니다. PostgreSQL과 Ray 포트는 공개하지 않습니다.

```bash
curl -fsS http://127.0.0.1:8081/health
```

## 노드 등록과 승인

```bash
sudo apt install dure
sudo dure join
```

join은 profile을 수집하고 root 전용 `/etc/dure/agent.json`을 쓰며 `dure-agent`를 활성화합니다. 결과 node UUID는 pending 상태입니다. 중앙에서 확인·승인한 뒤 필요하면 profile을 갱신합니다.

```bash
dure admin nodes --pending
dure admin node show <node-id>
dure admin node approve <node-id>
dure admin probe --nodes <node-id>
```

hostname, GPU inventory, network 주소, 운영자 소유권을 검토한 뒤에만 승인합니다. pending 노드는 heartbeat는 가능하지만 task 생성과 claim 양쪽에서 거부됩니다.

## Codex 기반 용량 진단

Codex는 관리자 컴퓨터에만 설치·로그인합니다.

```bash
codex --version
codex login status
```

에이전트를 먼저 갱신·재시작해 `PROBE` 결과에 설치 모델과 LLM 작업 부하가 포함되게 한 뒤 진단합니다.

```bash
dure admin diagnose
dure admin diagnose --nodes <node-a> <node-b> --output diagnosis.json
```

기본값은 모든 승인된 온라인 노드에 `PROBE` 작업을 보내고 최대 180초 대기한 뒤, 인벤토리를 로컬 Codex에 전달하는 것입니다. `--no-refresh`, `--timeout`, `--codex-timeout`, `--model`, `--json`으로 동작을 조절할 수 있습니다.

이 보고서는 참고용입니다. 배포 구성을 만들거나 적용하지 않습니다.

- 오프라인 또는 오래된 프로필은 즉시 배포 가능하다고 취급하지 않습니다.
- 다중 노드 Ray 추천은 RTT/대역폭, 방화벽, NCCL 검증 뒤에만 적용합니다.
- 불완전한 모델 디렉터리는 재사용 가능한 아티팩트로 취급하지 않습니다.
- Dure 이외의 LLM 컨테이너는 이름·이미지·상태만 관찰하며 자동 중지하지 않습니다.
- CPU 전용 노드는 utility 역할만 추천합니다.

인벤토리에는 하드웨어, 네트워크 주소, 런타임, 모델 경로·이름, 컨테이너 이미지·상태 메타데이터가 포함될 수 있습니다. 관리자·노드 전달자 자격 증명, 컨테이너 환경 변수·명령, 모델 토큰, 프롬프트 데이터는 제외합니다.

신뢰할 수 없거나 분실된 노드는 credential을 폐기합니다.

```bash
dure admin credential revoke <node-id>
```

credential rotate는 새 secret을 반환하므로 해당 노드의 Agent 설정을 즉시 갱신해야 합니다.

## 현재 배포 구성 운영

다이제스트로 고정한 배포 구성을 만들고 노드별 작업을 보냅니다.

```bash
dure admin deployment create \
  --profile node-a.json --profile node-b.json --profile node-c.json \
  --model qwen2.5-72b-awq \
  --image registry.example/vllm@sha256:<digest> \
  --accept-model-download --pull

dure admin apply <deployment-id> --nodes <node-a> <node-b> <node-c>
dure admin tasks --watch
```

현재 vLLM API는 Ray head에서만 listen합니다. worker와 head 검증을 분리합니다.

```bash
# 모든 배정 노드의 GPU/Ray 검증
dure admin verify <deployment-id> --nodes <node-a> <node-b> <node-c>

# Ray head에서만 HTTP API 검증
dure admin verify <deployment-id> --nodes <ray-head-node-id> --api
```

`start`, `stop`, `restart`는 동일한 deployment ID와 명시적 node 목록을 요구합니다. bulk 요청은 노드마다 독립 task를 만들므로 부분 실패를 확인해야 하며 all-or-nothing으로 가정해서는 안 됩니다.

`apply`와 `verify` 요청은 배포 세대의 operation과 노드별 상태에 연결됩니다. operation은 `QUEUED → RUNNING → SUCCEEDED`로 진행하며 노드 일부 또는 전체가 실패하면 `PARTIAL_FAILED` 또는 `FAILED`가 됩니다. 노드 레코드는 `PENDING`, `QUEUED`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELED`를 사용하고 task 재시도는 증가하는 시도 번호에 결합됩니다. 현재 단계·노드·시도 번호와 다른 늦은 완료 보고는 상태를 변경하지 않습니다.

롤백 증거인 `verified_at`은 다음 조건을 모두 만족한 `VERIFY`에서만 기록됩니다.

- 요청 노드가 계획의 전체 배정 노드와 정확히 같습니다.
- 모든 노드의 검증 task가 성공합니다.
- 모든 대상 노드의 Agent가 0.3.12 이상입니다.

일부 노드만 검증하거나 다중 노드 배포에서 전체 배정 집합이 아닌 Ray head만 `--api` 검증한 결과는 상태 조회에는 남지만 `verified_at`을 만들지 않습니다. 기존 수동 복구 경로에서 발생한 구 Agent의 성공 결과도 롤백 증거로 승격하지 않습니다.

새 Ray·API 컨테이너에는 `dure.deployment`, `dure.generation`, `dure.node` 레이블이 모두 있어야 합니다. 시작·검증·중지 전에 실제 컨테이너의 세 레이블을 다시 확인합니다. 0.3.12 이전 컨테이너에 `dure.node`가 없는 경우에만 정확한 배포 ID와 세대가 모두 일치할 때 제한적으로 관리할 수 있습니다. 노드 레이블이 존재하면서 다르거나 배포·세대 레이블이 없거나 다르면 컨테이너를 중지·제거·재사용하지 않습니다.

## 모델 레지스트리 운영

관리자 API는 모델 아티팩트, 런타임 릴리스, 모델 릴리스와 배치 프로필을 별도로 관리합니다.

- `POST /v1/admin/model-artifacts`: 변경 불가능한 모델 리비전과 매니페스트 다이제스트 등록
- `POST /v1/admin/runtime-releases`: OCI 다이제스트로 고정한 vLLM 런타임 등록
- `POST /v1/admin/model-releases`: 아티팩트와 런타임 조합 생성
- `POST /v1/admin/model-releases/{id}/placements`: `DRAFT` 릴리스에 형식화된 배치·SLO 정책 추가
- `POST /v1/admin/model-releases/{id}/transition`: 허용된 상태 전이 수행
- `GET /v1/admin/model-releases`: 릴리스와 배치 프로필 조회
- `POST /v1/admin/deployment-recommendations`: 저장 인벤토리와 `ACTIVE` 릴리스 평가 및 불변 스냅샷 저장
- `GET /v1/admin/deployment-recommendations/{id}`: 추천·인벤토리 스냅샷과 수락된 세대 조회
- `POST /v1/admin/deployment-recommendations/{id}/accept`: 현재 유효성 재검사 후 적용 전 배포 세대 생성

모든 경로는 관리자 전달자 인증을 요구합니다. 모델 리비전, 매니페스트, 런타임 이미지가 고정되지 않으면 등록할 수 없고, 허용 목록 밖의 Docker 인자·환경 변수·마운트·호스트 경로는 요청 단계에서 거부됩니다. 레지스트리 등록이나 상태 전이만으로 에이전트 작업 또는 호스트 변경이 발생하지 않습니다.

## 로컬 모델 캐시 준비와 실패 복구

`0.3.15`는 정규 매니페스트를 받아 노드 내부의 검증된 `FULL_SNAPSHOT` 캐시를 만드는 라이브러리를 제공합니다. 현재 작업 열거형에는 `PREPARE_MODEL`과 `PREPARE_IMAGE`가 없고 공개·관리자 준비 API·CLI도 없습니다. 따라서 `recommend`, `accept`, `deployment apply`, rollback과 `benchmark-runs/prepare`는 이 준비기를 호출하지 않습니다. 벤치마크의 prepare는 모델 바이트가 아니라 DB 실행 문맥만 준비하며, 제어면의 노드 등록·추천·수락 자체는 모델 다운로드나 모델 캐시 디렉터리 변경을 일으키지 않습니다. 패키지 설치는 별도로 `/var/lib/dure`의 상태 디렉터리와 권한을 준비합니다. 중앙 연결은 다음 PR에서만 추가합니다.

준비기가 연결되면 사용할 고정 경계는 다음과 같습니다.

- 청크 CAS·잠금·시도 저널: `/var/lib/dure/model-store`
- 활성 모델과 숨은 조립 영역: `/var/lib/dure/models`
- 원본: 현재 내부 API가 직접 받는 검증된 `TrustedHTTPSOrigin` 객체. 노드 설정에서 만드는 production 연결은 다음 PR 범위
- 지원 cache kind: `FULL_SNAPSHOT`만 해당. `STAGE`는 명시적으로 거부

정상 실행은 디스크 사전 검사, 청크 다운로드·전체 digest 검사, 파일 조립·전체 파일 해시 검사, `config.json` 양자화 일치, exact-tree 검사, marker-last 기록과 no-replace 활성화 순서로 진행합니다. 같은 digest의 검증된 청크·완성 파일·final은 다시 검사한 뒤 재사용합니다. `MODEL_STORE_DOWNLOAD_TIMEOUT`과 응답 body 중단, 파일·marker 조립 중단은 결정적 부분 파일에서 이어가지만, non-timeout DNS·TLS·connect 거부와 응답 계약·digest 오류는 청크 `.part`를 0바이트로 되돌리고 재시도합니다.

저장소와 저널 경계가 정상이라면 실패 코드는 `/var/lib/dure/model-store/attempts/<manifesthex>/journal.json`의 폐쇄형 로컬 마지막 상태로 남고 URL, credential, 원격 오류 본문과 예외 원문은 남기지 않습니다. 루트·권한·저널 I/O 자체가 실패하면 실패 상태도 기록하지 못할 수 있습니다. 이 저널은 중앙 operation 진행률, `READY` 증적이나 자동 경보가 아닙니다. 운영자는 다음 원칙으로 대응합니다.

1. `MODEL_STORE_DOWNLOAD_TIMEOUT`이나 body read·조립 중단이면 제한 재시도가 끝났는지 확인하고 같은 요청을 재시도합니다. 새 staging이 계속 생기지 않고 같은 digest 영역을 사용해야 합니다. `MODEL_STORE_DOWNLOAD_REJECTED`나 digest 불일치는 청크 부분 파일을 보존 재개하지 않고 0바이트부터 다시 받습니다.
2. `MODEL_STORE_DISK_INSUFFICIENT`이면 모델 전체 조립본, 없는 고유 청크와 기본 여유 공간을 합친 용량을 확보합니다. 기존 검증 부분의 실제 할당량은 재시도 계산에 반영됩니다. 사전 검사는 공간 예약이 아니므로 외부·동시 소비로 쓰기 중 `ENOSPC`가 날 수 있습니다. 이때 유효 marker와 final은 게시하지 않고 결정적 부분 파일만 남기므로 공간 확보 뒤 같은 digest를 재시도합니다.
3. digest·파일 무결성 오류, path·target collision이면 자동 덮어쓰기나 삭제를 시도하지 않습니다. 정확한 매니페스트, origin object와 소유권을 먼저 조사합니다.
4. staging이나 비활성 final을 수동 격리해야 하면 같은 digest 준비가 실행 중이 아니고 어떤 배포·벤치마크도 참조하지 않는지 확인합니다. 계산된 단일 경로만 같은 파일시스템의 별도 보존 위치로 원자적으로 옮기고 원본 증거를 남깁니다. 공유 CAS 청크는 모든 매니페스트와 진행 중 준비의 미참조를 증명할 수 없으면 옮기거나 삭제하지 않습니다.
5. `/var/lib/dure/models`, `/var/lib/dure/model-store`나 wildcard를 대상으로 재귀 삭제하지 않습니다. 현재 공식 quarantine·eviction 명령은 없으며 후속 버전에서 감사·참조 검사를 포함해 추가합니다.

패키지 업그레이드는 `/var/lib/dure`를 `root:dure` `0750`으로 바로잡고 `/var/lib/dure/server`만 `dure` 계정에 쓰기를 허용합니다. 이 경로가 symlink이면 설치를 거부합니다. 중앙 서버의 로컬 SQLite 파일이나 기타 쓰기 상태를 상위 `/var/lib/dure`에 새로 만들지 말고 서버 전용 하위에 둡니다. PostgreSQL 운영에는 영향이 없습니다.

기존 중앙 세대 계획의 `/var/lib/dure/models/<model-id>--<revision>` 경로와 새 CAS final `/var/lib/dure/models/sha256-<manifesthex>`는 서로 다른 계약입니다. `0.3.15`는 새 경로를 기존 generation plan에 자동 주입하지 않으므로 기존 중앙 apply가 준비된 CAS를 소비한다고 해석하면 안 됩니다. 다음 중앙 준비 PR이 정확한 prepared path와 증적을 계획에 연결해야 합니다.

## 벤치마크 증적 등록과 승격

모델 릴리스의 배치 프로필을 모두 추가한 뒤 `VALIDATED`로 전환합니다. 이후 신뢰된 외부 도구의 구조화된 결과를 등록하거나, 승인된 단일 GPU 노드에 폐쇄형 벤치마크 작업을 요청하고 `ACTIVE` 승격을 요청할 수 있습니다.

- `POST /v1/admin/benchmark-context`: 현재 프로필 지문과 고정 릴리스 식별자 준비
- `POST /v1/admin/benchmark-runs/prepare`: 단일 노드 실행 문맥 준비
- `POST /v1/admin/benchmark-runs/{request_id}/apply`: 명시적 적용과 작업 생성
- `GET /v1/admin/benchmark-runs/{request_id}`: 실행·작업·증적 상태 조회
- `POST /v1/admin/benchmark-evidence`: 증적 등록
- `GET /v1/admin/benchmark-evidence?release_id=<id>`: 릴리스의 최근 증적 조회
- `POST /v1/admin/model-releases/{id}/promote`: 모든 배치 프로필의 최신 증적을 검사하고 승격

먼저 context API에 릴리스·배치 프로필과 측정 노드 UUID를 보내 현재 인벤토리 지문, 아티팩트 리비전·매니페스트와 OCI 런타임 이미지를 받습니다. 증적 등록 요청에는 이 값과 고정 suite·정책 버전, Dure 커밋, 작업 부하 크기와 수치 지표가 필요합니다. 서버는 등록 시 현재 저장된 노드 프로필 지문과 레지스트리 식별자를 다시 계산·비교하고 불일치를 거부합니다.

SLO 미달 결과는 등록 오류가 아니라 `FAILED` 증적으로 저장됩니다. 운영자는 실패 코드를 검토하고 새 측정을 등록해야 합니다. 배치 프로필별 최신 증적만 승격 판단에 사용하며, 통과 증적 뒤의 자동 실행이 큐에 있거나 실행 중이면 승격을 보류합니다. 그 실행이 런타임·아티팩트·실행 오류 또는 취소로 끝난 경우에도 이전 `PASSED` 결과로 우회할 수 없고 새 통과 증적을 등록한 뒤 다시 승격해야 합니다. 이때 수치가 과거 통과 결과와 완전히 같아도 실패 실행 뒤의 새 수동 등록에는 새 순번이 부여됩니다. 모든 배치 프로필이 통과하면 승격에 사용한 증적 집합이 릴리스에 고정됩니다.

자동 실행에는 정규 UUID인 요청 ID를 사용합니다. 준비 요청은 릴리스·배치 프로필·단일 노드 UUID, 허용된 작업 부하 ID와 40~64자리 Dure 빌드 커밋을 받습니다. 공식 Debian Agent의 값은 `/usr/share/dure/build-commit`에서 확인할 수 있으며 준비 요청과 정확히 같아야 합니다. 일반 wheel·editable 개발 설치는 신뢰된 환경에서 `DURE_BUILD_COMMIT`을 명시해야 하고, 값이 없거나 다르면 Agent가 프로필 조사 전에 작업을 거부합니다. 응답이 `PREPARED`인지 확인한 뒤에만 별도 적용 API에 정확히 `{"apply": true}`를 보냅니다. 준비는 작업이나 Docker 변경을 만들지 않으며, 적용은 준비 당시 문맥과 현재 인벤토리를 다시 비교한 뒤 작업 하나만 큐잉합니다.

노드에는 `/var/lib/dure/models` 아래에 완전히 펼쳐진 모델 디렉터리와 정확한 `.dure-model.json` metadata, 레지스트리에 고정된 OCI 이미지가 미리 있어야 하며, 이미지는 `dure-benchmark` 진입점을 제공해야 합니다. metadata 형식은 [벤치마크 문서](benchmarking.md)를 따릅니다. Hugging Face hub snapshot은 외부 blob 링크 때문에 자동 실행에 사용하지 않습니다. 자동 경로는 다운로드나 이미지 내려받기를 하지 않습니다. 다른 활성 LLM 작업 부하·Dure 벤치마크 컨테이너·선택 GPU compute process가 있거나 캐시·프로필 지문·이미지가 다르면 실패합니다. MIG process는 상위 GPU를 판별하지 않고 모두 거부합니다. 가장 큰 정상 GPU 한 장만 UUID로 할당하며, 그 GPU의 compute capability가 런타임의 `gpu_architectures` 폐쇄 목록과 일치해야 합니다. capability가 없거나 미지원이면 준비·적용·증적 등록·승격을 거부합니다.

컨테이너는 `restart=no`로 실행하며 RAM은 현재 전체·가용량 중 작은 값의 절반(최대 32GiB, 계산값이 8GiB 미만이면 거부), swap은 같은 상한, CPU는 논리 CPU의 절반(최대 8코어)으로 제한합니다. 실행 출력은 합계 64KiB로 제한합니다. 재시도 시 폐쇄형 payload와 정확한 대상 노드를 확인한 직후, 현재 Agent 빌드·캐시·프로필·가용 자원·이미지보다 먼저 같은 작업의 컨테이너를 조정합니다. Docker `StartedAt` 기준 900초 측정과 300초 정리 여유를 넘긴 경우에만 레이블을 재확인해 중지·제거하고, 그 뒤 프로필을 새로 조사합니다. inspect·stop·remove나 부재 확인이 불확실하면 작업을 실패로 닫지 않고 다음 임대까지 유예합니다. 같은 요청 ID는 기존 실행을 재사용하고, 새 측정은 새 요청 ID로 준비합니다.

노드 소실로 `QUEUED` 실행의 task가 `RUNNING`에 남으면 활성 임대 중에는 취소 API가 `409`를 반환합니다. 임대 만료를 확인한 뒤 `POST /v1/admin/tasks/{task-id}/cancel`을 명시적으로 호출하면 task를 `CANCELED`, 실행을 `BENCHMARK_CANCELED`로 닫을 수 있습니다. 그 뒤 남은 정확한 벤치마크 컨테이너를 별도로 확인·정리하고 새 요청 ID로 재측정해야 합니다. 통과 증적 뒤의 큐·실행 중 작업 또는 실패·취소 실행이 있는 동안에는 승격할 수 없습니다.

자동 작업은 단일 노드의 네 가지 고정 작업 부하만 지원합니다. 전용 관리자 CLI, 다중 노드 네트워크·NCCL 실행, 전체 작업 부하 조합과 24시간·복구 수용 검사는 후속 범위입니다. 다중 노드 증적은 현재도 신뢰된 외부 도구로 측정한 뒤 기존 context·증적 API를 통해 등록해야 합니다.

증적 본문에는 프롬프트, 자격 증명, 모델 접근 토큰, 로그, 명령, Docker 인자, 환경 변수, 마운트, 호스트 경로나 자유 형식 metadata를 넣을 수 없습니다. 원본 결과와 큰 로그는 접근 제어된 별도 저장소에 보관합니다.

수동 증적 등록과 승격은 deployment나 task를 만들지 않고 모델 다운로드, 이미지 내려받기, Docker 실행 또는 기존 컨테이너 중지를 수행하지 않습니다. 자동 벤치마크도 준비 단계에는 무변경이며, 명시적 적용 뒤에 격리된 벤치마크 컨테이너 하나만 실행합니다. 어느 경로도 기존 배포를 중지하지 않으며, 승격 후 실제 배포에는 별도의 명시적 생성과 apply가 필요합니다.

0003 마이그레이션은 과거 버전에서 증적 없이 `ACTIVE`가 된 릴리스를 `VALIDATED`로 되돌립니다. 0004는 준비된 벤치마크 실행과 작업·증적 연결을 추가하고, 새 지문에서는 가용 메모리·남은 디스크·현재 작업처럼 순간적인 값을 제외합니다. 0005는 불변 추천 스냅샷과 배포 세대 계보를 추가하고 기존 수동 배포는 `lineage_id=id`로 보정하되 과거 generation과 plan JSON은 변경하지 않습니다. 0006은 배포 operation, 노드별 단계·시도, task 연결과 `verified_at`을 추가합니다. 0003에서 저장한 전체 프로필 지문은 당시 저장 프로필이 그대로인 동안 승격 검사에서 계속 인정합니다. 업그레이드 뒤 기존 릴리스의 모든 배치 프로필에 현재 증적을 등록하고 명시적으로 다시 승격해야 합니다. 이 상태 보정과 스키마 추가는 실행 중인 컨테이너에는 손대지 않습니다.

## 추천 스냅샷 수락과 배포 세대

정책 기반 `recommend`, 벤치마크 승격 게이트, 추천 스냅샷 조회, 명시적 수락, 세대별 apply·verify 상태와 rollback은 구현되어 있습니다. 추천은 데이터베이스에 저장된 노드 프로필과 `ACTIVE` 모델 릴리스만 평가합니다.

```bash
# 필요할 때만 명시적으로 프로필 조사 작업을 요청하고 완료를 확인합니다.
dure admin probe --nodes <node-id> <node-id>
dure admin tasks

# 저장된 승인·온라인 노드를 모두 평가합니다.
dure admin deployment recommend --all-online

# 지정한 중앙 노드 UUID를 평가하고 대기·오프라인·오래된 상태의 탈락 사유도 확인합니다.
dure admin deployment recommend --nodes <node-id> <node-id> --objective quality-first

# 저장된 추천과 정규화 인벤토리, 수락된 세대를 검토합니다.
dure admin recommendation show <recommendation-id>

# 현재 상태가 저장 스냅샷과 같은 경우 적용 전 generation 1을 만듭니다.
dure admin recommendation accept <recommendation-id>

# 기존 계보의 최신 세대를 이어 갈 때만 명시합니다.
dure admin recommendation accept <recommendation-id> \
  --previous-generation <deployment-id>
```

`recommend` 자체는 `PROBE` 작업을 만들거나 인벤토리를 갱신하지 않습니다. 응답에는 콘텐츠 해시 추천 ID, 카탈로그·정책 버전, 인벤토리 지문, 후보별 중앙 릴리스·배치 ID와 탈락 사유가 포함됩니다. 서버는 같은 ID의 추천 결과와 지문 계산에 사용한 정규화 인벤토리를 한 행으로 멱등 저장합니다. 자격 증명, 프롬프트, 컨테이너 명령·환경 변수는 이 스냅샷에 포함하지 않습니다. 네트워크·NCCL 증적을 중앙 추천의 배치 가능성 판단에 연결하는 기능이 추가되기 전까지 다중 노드 후보는 실패 안전 방식으로 거부됩니다.

운영 순서는 다음과 같습니다.

1. 최신 `PROBE`로 인벤토리를 갱신합니다.
2. 추천의 후보, 탈락 사유, 모델 리비전, 이미지 다이제스트, 네트워크 사전 조건을 검토합니다.
3. `recommendation show`로 저장 스냅샷을 확인하고 명시적으로 수락합니다.
4. 서버가 현재 상태를 다시 평가해 완전히 같을 때만 `CREATED` 배포 세대를 만듭니다.
5. 별도의 명시적 apply와 verify를 거쳐서만 호스트를 변경합니다.
6. 전체 노드 검증으로 `verified_at`을 확보한 세대만 이후 최신 세대의 직접 rollback 대상으로 사용합니다.

수락 시 저장 스냅샷과 현재 추천의 콘텐츠 ID·카탈로그·정책·인벤토리 지문·정규화 인벤토리·선택 결과가 모두 같아야 합니다. 프로필이 오래됐거나 내용이 바뀌고, 노드 승인·연결 상태나 `ACTIVE` 릴리스가 달라졌다면 `409` 응답을 받고 새 `PROBE`와 추천부터 다시 시작합니다. 이전 세대를 지정했다면 해당 계보에서 generation이 가장 큰 최신 행이어야 하고, `ROLLED_BACK` 상태이거나 활성 operation·변경 task가 있으면 안 됩니다. 같은 추천과 같은 이전 세대를 다시 수락하면 기존 세대를 반환합니다.

성공한 롤백의 소스 세대는 `ROLLED_BACK`이 되고 기존 `verified_at`도 제거됩니다. 이 세대를 `--previous-generation`으로 다시 지정해 과거 계보를 연장할 수 없습니다. 롤백 뒤 새 배포를 만들 때는 현재 검증된 구성과 추천을 다시 확인하고 `--previous-generation`을 생략해 generation 1의 새 계보를 시작합니다. 이 제한은 롤백으로 폐기한 세대를 다음 복구 기준으로 자동 부활시키지 않기 위한 실패 안전 정책입니다.

추천과 수락은 자동 다운로드, 이미지 내려받기, 적용, 기존 컨테이너 중지를 의미하지 않습니다. 생성된 세대의 다운로드·pull 플래그는 거짓이며 task나 benchmark run도 만들지 않습니다. 중앙 세대는 `/var/lib/dure/models/<model-id>--<revision>` 경로를 사용하므로 적용 전에 정확한 리비전의 펼쳐진 Dure 캐시가 그 위치에 있어야 합니다. 실제 변경은 별도 apply에서만 발생합니다. 동일 GPU를 공유하는 파이프라인은 블루/그린 방식이 불가능할 수 있으므로, 실제 무중단 여부를 과장하지 않고 재생성과 복구 절차를 문서화해야 합니다.

`serve=true`인 중앙 apply는 전체 배정 노드 집합에만 허용됩니다. 첫 `APPLY` 단계는 모든 노드에 `serve=false`를 보내 Ray만 준비하고, 모든 노드가 성공한 뒤 Ray head 한 대에만 `START_API`, `VERIFY_API`를 차례로 큐잉합니다. 서로 다른 계보라도 같은 노드를 포함하는 활성 operation이나 배포 task가 있으면 새 작업을 거부하므로 한 GPU에서 두 전환이 교차 실행되지 않습니다.

전체 `VERIFY`가 롤백 증거가 되려면 각 노드가 `host-gpu`, `container-gpu`, `ray-cluster` 검사를 모두 보고해야 하며 API 검증을 요청한 head는 `vllm-api`도 보고해야 합니다. API 시작 단계는 별도의 `vllm-api-start`와 `vllm-api` 결과를 모두 요구합니다. 필수 검사 누락, 이름 중복, 잘못된 결과 모양은 성공 응답으로 취급하지 않고 task를 `TASK_RESULT_INVALID`로 실패시키며 기존 `verified_at`을 제거합니다.

## 세대 조회와 명시적 롤백

한 세대의 계획·상태·검증 시각·operation·노드별 task를 확인하고 같은 계보를 조회합니다.

```bash
dure admin deployment show <deployment-id>
dure admin deployment generations <deployment-id>
```

같은 정보는 관리자 API의 `GET /v1/admin/deployments/{deployment_id}`와 `GET /v1/admin/deployments/{deployment_id}/generations`에서 조회할 수 있습니다. 기존 상세 응답의 `id`, `generation`, `status`, `plan` 필드는 유지되며 계보, 검증과 operation 상세가 추가됩니다.

롤백은 기본적으로 준비만 합니다. API와 CLI 모두 클라이언트가 `node_ids`, `apply`, `serve` 이외의 대상 세대·계획·다운로드·pull 입력을 지정할 수 없습니다. 다음 첫 명령은 안전 조건을 검사하고 `PREPARED` operation만 저장하며 task를 만들지 않습니다. API를 복구하려면 준비와 적용 양쪽에 같은 `--serve` 선택을 사용합니다.

```bash
# 준비만 수행하며 task는 0개입니다.
dure admin deployment rollback <latest-deployment-id> \
  --nodes <node-a> <node-b> <node-c> --serve

# 같은 입력에 --apply를 추가해야 실제 변경을 시작합니다.
dure admin deployment rollback <latest-deployment-id> \
  --nodes <node-a> <node-b> <node-c> --serve --apply
```

API는 `POST /v1/admin/deployments/{source_id}/rollback`에 다음과 같은 닫힌 본문을 받습니다. `apply`와 `serve`는 엄격한 불리언이며 기본값은 `false`입니다. 응답은 operation 상세, 이번 호출로 만든 task 목록과 `changed` 여부를 포함합니다.

```json
{
  "node_ids": ["<node-uuid>"],
  "apply": false,
  "serve": false
}
```

서버는 실제 적용 전에 다음 조건을 모두 검사합니다.

- 소스는 해당 계보의 최신 세대입니다.
- 대상은 소스의 `previous_generation_id`가 직접 가리키는 세대이며 상태가 `VERIFIED`이고 `verified_at`이 있습니다.
- 소스와 대상의 전체 배정 노드와 토폴로지가 정확히 같습니다.
- 요청한 중복 없는 정규 UUID 목록이 전체 배정 노드 집합과 정확히 같습니다.
- 모든 노드가 승인 상태이고 최근 30초 안에 온라인으로 관측됐으며 Agent가 0.3.12 이상입니다.
- 소스와 대상 이미지가 OCI 다이제스트로 고정돼 있습니다.
- 같은 계보에 이미 적용 중인 다른 변경이 없습니다.

`--apply` 뒤의 순서는 고정돼 있습니다.

```text
STOP_SOURCE
    ↓ 모든 노드 성공
START_TARGET (serve=false)
    ↓ 모든 노드 성공
VERIFY_TARGET
    ↓ 모든 노드 성공
선택적 START_API (Ray head)
    ↓
선택적 VERIFY_API (Ray head)
```

한 단계에서 일부 노드가 실패하거나 취소되면 다음 단계로 넘어가지 않습니다. 실행 중 task가 남아 있는 동안에는 재시도를 거부합니다. 원인을 해결한 뒤 같은 입력으로 `--apply`를 다시 지정하면 현재 단계의 실패 노드만 새 시도 번호로 큐잉합니다. 이미 성공한 노드는 반복하지 않고 과거 시도의 늦은 claim·완료·실패 보고는 현재 시도와 맞지 않으므로 무시합니다.

롤백 task는 항상 `accept_model_download=false`, `pull_image=false`를 사용합니다. 대상 모델 캐시와 다이제스트 이미지가 모든 노드에 이미 있어야 하며 롤백이 다운로드나 이미지 내려받기를 대신하지 않습니다. 동일 GPU에서 소스 컨테이너를 중지하고 대상 컨테이너를 다시 생성하므로 중단 시간이 생길 수 있고 블루·그린 전환이 아닙니다. 이 흐름은 다중 노드 네트워크·NCCL 시험이나 24시간 복구 검증을 수행하지 않으므로 실제 GPU 환경의 별도 수용 검사를 계속 진행해야 합니다.

## 업그레이드와 복구

0.3.15 Agent 패키지에서는 캐시 부모 소유권 경계를 먼저 확인합니다. `/var/lib/dure`가 실제 디렉터리인지, `root:dure` `0750`인지, `/var/lib/dure/server`만 `dure` 소유인지 확인한 뒤 Agent를 재시작합니다. post-install script는 두 상태 경로 중 하나가 symlink이면 실패합니다. 기존 중앙 서버가 상위 경로에 직접 쓰는 로컬 파일을 사용했다면 서비스 중지와 백업 후 서버 전용 하위 경로로 명시적으로 이전하고 설정을 갱신해야 합니다. 스크립트가 알 수 없는 파일을 자동 이동하거나 삭제하지 않습니다.

0.3.12에서는 controller를 먼저 업그레이드합니다. PostgreSQL을 백업하고 controller 코드와 migration 0006을 적용한 뒤 server를 재시작하고 세대 조회 API를 확인합니다. 그 다음 Agent를 작은 batch로 업그레이드합니다. 0.3.12 미만 Agent는 세대 인식 롤백 안전 검사를 통과하지 못하며, 전체 노드가 업그레이드되기 전의 검증 성공은 `verified_at` 롤백 증거가 되지 않습니다.

```bash
sudo apt update
sudo apt install --only-upgrade dure
sudo systemctl daemon-reload
sudo systemctl restart dure-agent
```

Agent는 재시작 뒤에도 credential과 완료 task journal을 재사용합니다. 만료된 task lease는 재전달될 수 있으므로 handler는 멱등적이어야 합니다. 활성 deployment 중에는 `/var/lib/dure/agent-tasks.json`을 삭제하지 않습니다.

Agent heartbeat는 실행 중인 패키지 버전을 함께 보내며 controller는 이 값을 노드의 `agent_version`으로 갱신해 0.3.12 롤백 게이트에 사용합니다. operation task가 실행 중 노드 장애로 멈췄다면 lease가 실제로 만료된 뒤 `POST /v1/admin/tasks/{task_id}/cancel`을 호출할 수 있습니다. controller는 이를 일반 취소가 아니라 `TASK_LEASE_EXPIRED` 실패로 원자적으로 기록합니다. 같은 요청은 멱등이며, 노드를 복구한 뒤 동일한 rollback 본문과 `apply=true`로 실패한 현재 단계만 재시도합니다. 만료 전 실행 중 task를 강제로 취소하거나 실패한 롤백의 활성 계보를 자동 해제하지는 않습니다.

0006에서 0005로 데이터베이스를 내릴 때는 롤백 operation의 호스트 작업이 끝났는지만 보고 강제로 진행해서는 안 됩니다. 다음 상태가 하나라도 있으면 migration이 downgrade를 거부합니다.

- `active_lineage_id IS NOT NULL`인 operation이 있습니다.
- 상태가 `PREPARED`, `QUEUED` 또는 `RUNNING`인 operation이 있습니다.
- operation에 연결된 task가 `QUEUED` 또는 `RUNNING`입니다.

먼저 controller를 현재 버전으로 유지한 채 연결 task를 완료하거나 취소하고 상태를 다시 조회합니다. 롤백 실패나 task 취소만으로 활성 operation이 해제되지는 않으므로 원인을 해결하고 지원되는 재시도로 operation을 성공적으로 완료해야 합니다. 현재 관리 API에는 사용하지 않을 `PREPARED` operation을 폐기하는 기능이 없으므로 이런 레코드가 남아 있으면 0006을 유지하고 별도 복구 절차를 검토합니다. PostgreSQL 백업을 확인하지 않은 채 안전 검사를 우회하거나 새 테이블·task 연결을 수동으로 삭제하지 않습니다.

```bash
systemctl status dure-server dure-agent
journalctl -u dure-server -u dure-agent --since -1h
dure admin nodes --json
dure admin tasks
```

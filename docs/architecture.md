# Dure 아키텍처

## 시스템 개요

Dure는 세 개의 협력 계층으로 구성됩니다.

```text
운영자 CLI ── HTTPS ──> 중앙 제어면 ── PostgreSQL
                              ↑
                    외부 방향 하트비트·작업 폴링
                              │
                         dure-agent
                              │
                    Docker / Ray / vLLM
```

- 로컬 CLI는 중앙 관리 없이도 하드웨어를 조사하고 배포 계획을 만들고 적용·검증할 수 있습니다.
- 중앙 제어면은 노드 프로필, 관측·희망 상태, 추천·인벤토리 스냅샷, 배포 세대, 작업, 모델·stage variant 레지스트리와 검증 증적, 자격 증명과 감사 이벤트를 저장합니다.
- root 권한 에이전트는 중앙 제어면에 외부 방향 폴링으로 연결하고, 미리 정의된 Python 작업만 실행합니다.
- Ray는 신뢰된 포드 내부의 분산 실행 수단이며, 등록·인증·보안 경계가 아닙니다.

## 노드 수명 주기

패키지 설치 시 `/etc/dure/dure-client.env`에 중앙 제어면 주소가 제공됩니다. `sudo dure join`은 설치 ID와 `NodeProfile`을 `POST /v1/nodes/join`으로 보내며, 서버는 노드별 자격 증명을 발급하고 노드를 대기 상태로 기록합니다.

대기 상태 노드는 인증과 하트비트는 가능하지만 작업 요청 결과는 비어 있습니다. 운영자가 `dure admin node approve <node-id>`로 승인해야 작업을 받을 수 있습니다. 폐기는 노드를 비활성화하고 활성 자격 증명을 폐기합니다.

로컬 deployment 상태는 다음과 같습니다.

```text
DISCOVERED → PROBING → ELIGIBLE → PLANNED → DOWNLOADING
                                      ↓
                              STARTING → VERIFYING → READY
                                             └────→ WAITING_FOR_PEERS
차단 오류 발생 → FAILED
```

서버의 desired task 상태와 Agent가 보고하는 observed lifecycle 상태는 별도로 저장합니다.

## 현재 용량 진단

`dure admin diagnose`는 승인되고 온라인인 노드에 기존의 `PROBE` 작업만 요청합니다. 결과 프로필에는 하드웨어, Dure/Hugging Face 모델 캐시, Ollama 모델 이름, Dure 또는 일반 LLM 컨테이너의 메타데이터만 포함됩니다.

컨테이너 명령, 환경 변수, 마운트 내용, 자격 증명, 프롬프트는 수집하지 않습니다. 운영자 CLI는 `GET /v1/admin/inventory` 결과를 읽고, 운영자 컴퓨터의 `codex exec`를 빈 임시 디렉터리와 읽기 전용 샌드박스에서 실행합니다. 이 진단은 참고용이며 배포 구성 생성이나 새 에이전트 작업 유형 전송 권한이 없습니다.

CPU 전용 노드는 중앙 제어면, 게이트웨이, 아티팩트 캐시, 관측성, 대기열, 전처리 같은 보조 역할의 후보가 될 수 있습니다. 현재 Dure 런타임은 GPU 노드가 Ray head여야 하며 CPU 노드에는 모델 레이어를 배정하지 않습니다.

## 현재 로컬 모델 캐시와 중앙 준비 계층

`0.3.16`에는 정규 아티팩트 매니페스트를 로컬 `FULL_SNAPSHOT` 캐시로 materialize하는 노드 라이브러리와 이를 호출하는 중앙 준비 operation·관리자 CLI·Agent 작업이 있습니다. 추천과 수락은 여전히 호스트를 바꾸지 않으며, 운영자가 같은 준비 요청에 명시적으로 `--apply`를 지정한 뒤에만 `PREPARE_MODEL → PREPARE_IMAGE` 작업이 실행됩니다. GPU 노드 추가만으로 준비를 자동 시작하지 않습니다.

```text
정규 매니페스트 + 검증된 TrustedHTTPSOrigin 객체
                    ↓ 경로·크기·응답 계약 검사
        SHA-256 청크 CAS와 재개 가능한 .part
                    ↓ 청크별·매니페스트별 잠금
       digest별 고정 staging과 파일별 부분 조립
                    ↓ 전체 파일·config·트리 재검증
          v2 marker를 마지막에 기록·fsync
                    ↓ no-replace 원자적 rename
             검증된 FULL_SNAPSHOT 캐시
```

production 기본값에서 CAS와 시도 저널은 `/var/lib/dure/model-store`, 활성 캐시와 숨은 staging은 `/var/lib/dure/models` 아래의 고정 경로를 사용합니다. 매니페스트 다이제스트가 캐시와 staging 이름을 결정하고 원격 task 입력은 호스트 경로를 지정하지 않습니다. 내부 `ContentAddressedModelStore` 생성자는 테스트와 로컬 임베딩을 위한 명시적 루트 override를 허용하지만 이를 원격 payload와 연결하면 안 됩니다. Agent handler는 노드 로컬 `artifact_origin` 신뢰 설정에서만 `TrustedHTTPSOrigin`을 구성하고 작업 payload의 URL·host·header·token을 거부합니다. 같은 매니페스트의 실행은 하나의 artifact lock으로 직렬화하고, 여러 매니페스트가 공유하는 청크는 chunk lock과 전체 SHA-256 검사 뒤 재사용합니다.

로컬 상태는 `부분 다운로드 → 검증 CAS → assembling → marker-last 검증 staging → no-replace final` 순서로만 전진합니다. 다운로드·조립 중단은 결정적 부분 파일에서 재개합니다. 잘못된 CAS, 예상 밖 entry, symlink·hardlink·special file, marker·양자화 불일치와 기존 final 충돌은 보존한 채 실패하며 자동 재귀 삭제나 캐시 퇴출을 하지 않습니다. 반복 실패가 staging 디렉터리를 계속 늘리지 않도록 매니페스트마다 조립 영역을 하나만 사용합니다. 로컬 저널은 마지막 시도 상태일 뿐 중앙 진행률이나 `READY` 증적이 아닙니다. 특히 공유 CAS 청크는 전역 미참조를 증명할 수 없으면 수동으로 옮기거나 삭제해서는 안 되며, 감사 가능한 quarantine 명령은 후속 범위입니다.

패키지는 root Agent의 캐시 경계를 중앙 서버 계정과 분리합니다. `/var/lib/dure`는 `root:dure`의 비쓰기 부모이고 중앙 서버의 로컬 쓰기 상태는 `/var/lib/dure/server`에 둡니다. 준비기는 설정 루트의 가장 가까운 기존 조상부터 생성한 루트까지 소유권·쓰기 권한·symlink를 검사합니다. 인벤토리와 벤치마크는 `/var/lib/dure/models` 루트와 캐시 후보·`config.json`·marker를 현재 Agent 사용자 소유의 비쓰기 일반 항목으로 각각 검사하며, 상위 `/var/lib/dure`까지 같은 검사를 반복하지는 않습니다. Hugging Face inventory의 표준 snapshot→repository `blobs` 링크는 읽기 호환성을 유지하지만 자동 벤치마크의 검증 캐시가 되지는 않습니다.

## 현재 stage artifact 생성·등록 계층

0.3.17에는 검증된 `FULL_SNAPSHOT`을 pipeline rank별 vLLM `sharded_state`로 내보내는 신뢰된 오프라인 빌더와 중앙 variant 레지스트리가 있습니다. 이 빌더는 Agent task가 아니며 커뮤니티 GPU 노드에서 중앙 지시로 실행되지 않습니다.

```text
정규 매니페스트로 검증된 FULL_SNAPSHOT
        ↓ digest 고정 오프라인 빌더 런타임
vLLM 0.9.0 / V0 / Qwen2ForCausalLM / AWQ / TP=1
        ↓ worker 내부의 네이티브 sharded-state 저장
stages/<pp-rank>별 파일과 정규 매니페스트
        ↓ 전체 rank를 원자적으로 결합
DRAFT variant
        ↓ 실제 GPU_EXPORT_LOAD / PASSED
VALIDATED ── 운영자 철회 ──> REVOKED
```

vLLM 0.9.0의 sharded-state 파일명 rank는 PP rank가 아니라 TP rank입니다. 현재 `TP=1`에서는 모든 PP worker가 `model-rank-0-*`를 사용하므로 공용 출력 디렉터리는 충돌과 덮어쓰기 위험이 있습니다. 각 worker는 자신의 pipeline rank를 확인하고 `stages/<pp-rank>` 아래에만 저장합니다. Dure의 `layer_start`·`layer_end`로 원본 파일을 임의 분할하지 않습니다.

variant identity는 원본 매니페스트, 런타임 OCI 다이제스트, vLLM 버전, exporter 빌드 다이제스트, 아키텍처·양자화, TP·PP, loader 형식과 rank 순서의 stage 매니페스트를 결합합니다. 누락·중복·범위 밖 rank, topology 불일치와 같은 고정 입력에서 달라진 stage 출력은 거부합니다. remote code, LoRA·adapter, MoE, 멀티모달과 임의 아키텍처도 지원하지 않습니다.

등록은 실제 실행 가능성 증명이 아닙니다. synthetic 검사는 구조·tensor coverage·결정성을 검사하지만 승격할 수 없고, 정확한 variant를 실제 GPU에서 export하고 다시 load한 최신 `GPU_EXPORT_LOAD/PASSED` 증적만 `DRAFT → VALIDATED`를 허용합니다. 전제 조건 부족을 뜻하는 `NOT_RUN`과 실패는 DRAFT 승격을 차단합니다. canonical UUIDv4 validation run의 새 증적은 `DRAFT`에서만 추가합니다. 등록된 동일 run의 정확한 재전송은 상태 전환 뒤에도 멱등 반환하지만, `VALIDATED`와 `REVOKED`에는 새 run을 추가하지 못합니다. 검증 뒤 신뢰 문제가 발견되면 운영자가 영향 범위를 검토해 명시적으로 `REVOKED`로 닫고 수정된 계약은 새 `DRAFT` variant에서 검증합니다. 번들 acceptance harness의 native load 범위는 `PP=1`뿐이므로 PP>1은 모든 rank를 검증하는 별도 신뢰 증적이 없으면 `DRAFT`로 유지합니다.

현재 추천기, 중앙 준비 operation, Agent와 런타임은 stage variant를 아직 소비하지 않습니다. 노드에는 계속 전체 `FULL_SNAPSHOT`을 준비합니다. 빌드·등록·검증 실패는 deployment나 task를 만들지 않고 기존 컨테이너를 변경하지 않습니다. rank별 다운로드, `STAGE` 캐시의 원자적 활성화와 `sharded_state` 로더 결합은 다음 PR 범위이며, probe 기반 캐시 상태 투영과 격리 수명주기는 그 다음 PR 범위입니다. 상세 계약은 [stage artifact 문서](stage-artifacts.md)를 따릅니다.

빌더의 vLLM·PyTorch·safetensors·CUDA 계열 의존성은 기본 Debian 패키지에 포함하지 않습니다. 운영 Agent·중앙 서버와 분리한 digest 고정 OCI 환경에서만 heavy dependency를 설치합니다.

## 현재 폐쇄형 다중 노드 Ray pipeline 실행

0.3.18은 중앙 추천을 수락해 만드는 다중 노드 세대에 `VLLM_RAY_PP_V1` 실행 계약을 결합합니다. 이 backend는 기존 로컬 계획과 legacy Ray 실행을 대체하지 않습니다. `execution_backend`가 없는 기존 계획 JSON은 기존 필드와 동작을 유지하며, 엄격한 backend 전용 필드가 섞인 legacy 계획이나 알 수 없는 backend는 실행 전에 거부합니다.

```text
서버 UUID와 최신 NodeProfile
        ↓ 정상 GPU 정확히 한 장·고유 RFC1918 IPv4 검사
head를 rank 0으로 고정, worker를 IPv4 문자열로 정렬
        ↓ TP=1, PP=노드 수(2 또는 3)
VLLM_RAY_PP_V1 / vLLM 0.9.0 / V0 / Ray 계획
        ↓ 모든 노드의 검증된 FULL_SNAPSHOT과 digest 고정 이미지 확인
고정 Ray 포트·고정 컨테이너 identity로 실행
        ↓
source-pinned pipeline-rank-contract + API readiness
```

계획의 각 assignment에는 서버가 발급한 정규 노드 UUID, `runtime_address`, pipeline rank와 기대 runtime rank가 들어갑니다. head assignment는 항상 rank 0이고, 나머지 worker는 계획에 선택된 고유한 canonical RFC1918 IPv4 문자열의 오름차순입니다. 각 노드는 정상 GPU가 정확히 한 장이어야 합니다. probe는 전체 주소와 별도로 기본 interface에 실제 결합된 `default_interface_addresses`를 보고하며, 선택 주소가 이 목록에 정확히 하나 존재하고 모든 노드의 기본 interface 이름이 같아야 합니다. 주소가 없거나 여러 개라 모호하면 새 probe와 네트워크 설정 정리가 필요합니다. 모델 캐시는 `/var/lib/dure/models/sha256-<manifesthex>` 경로와 marker의 manifest digest가 정확히 같은 검증된 `FULL_SNAPSHOT` 하나여야 합니다. 현재 지원 범위는 `TP=1`, `PP=2` 또는 `PP=3`이고 `STAGE` 캐시는 실행 입력으로 허용하지 않습니다.

엄격한 Ray 컨테이너는 `--node-ip-address`와 `VLLM_HOST_IP`를 같은 계획 주소로 고정하고, 서버 UUID에서 계산한 `dure_node_<uuidhex>` Ray custom resource를 정확히 하나 게시합니다. GCS는 `6379`, worker 범위는 `20000-21000`이며 vLLM API는 head의 `127.0.0.1:8000`에만 바인딩합니다. 모든 컨테이너는 기존 `dure.deployment`·`dure.generation`·`dure.node` 외에 `dure.backend`, `dure.pipeline-rank`, `dure.runtime-rank`, `dure.component`, `dure.runtime-contract` 레이블을 가집니다. 마지막 값은 이미지·모델 mount·GPU·network·entrypoint·고정 환경·명령을 포함한 정규 계약의 SHA-256입니다. 시작·재사용·readiness는 이 digest까지 정확히 비교합니다. incident containment용 `STOP`은 준비 경로가 손상되거나 원본 계획과 effective path가 달라도 동작해야 하므로 기존 배포·세대·노드·backend·rank·component만 정확히 확인하고 runtime-contract 값은 재계산하지 않습니다.

`pipeline-rank-contract`는 직접 관측값과 계획의 의미를 구분합니다. Dure는 컨테이너 안의 vLLM 버전, 살아 있는 Ray 노드의 정확한 사설 주소 집합, 노드별 GPU 한 장과 주소별 Dure UUID custom resource를 확인하고, API 시작 뒤 검사에서는 vLLM Ray worker actor 토폴로지도 요구합니다. 그 결과와 vLLM 0.9.0 소스에 고정된 head 우선·worker 주소 정렬 규칙으로 정규 rank binding을 산출합니다. Ray 상태 API가 vLLM 내부 pipeline rank 번호를 공개 필드로 직접 반환하는 것은 아니므로 이 검사는 **소스 고정 계약의 간접 증적**입니다. controller는 각 노드 결과의 폐쇄형 정규 JSON이 전체 계획과 정확히 같은지 다시 검사하며 누락·중복·교환된 binding을 성공으로 인정하지 않습니다.

계획·프로필·캐시·이미지 identity와 고정 포트 **값**은 Docker 변경 전에 검사합니다. 다만 현재 구현은 host의 `6379`, `20000-21000`, `8000` 점유 여부를 별도 preflight하지 않습니다. 다른 프로세스나 이전 세대가 포트를 사용하면 컨테이너 시작 뒤 restart/readiness 실패로 발견합니다. 이 경우 operation을 실패 상태로 보존하고 정확한 deployment·generation·node·rank label의 컨테이너만 `STOP`으로 정리한 뒤 원인을 해결해 제한적으로 재시도하거나 검증된 직전 세대로 명시적으로 롤백합니다. 동일 GPU의 전환이 이미 시작됐다면 이전 컨테이너가 계속 실행된다고 보장할 수 없으므로 롤백을 생략해서는 안 됩니다. Dure는 host NVIDIA driver를 자동 설치·교체하지 않으며 driver·CUDA·하드웨어 문제는 운영자가 고정 런타임 호환표에 따라 해결해야 합니다.

실제 2·3노드 분산 load와 최소 추론은 `scripts/acceptance-vllm-ray-pp.py`로 별도 확인합니다. 기본 실행과 전제 조건 부족은 `NOT_RUN`·종료 코드 77이고 실제 시작 뒤 오류는 `FAILED`입니다. harness는 Ray custom resource로 Dure UUID와 주소를 대조하지만 설정 파일의 runtime image digest는 선언값이므로, 신뢰된 digest 고정 wrapper가 실제 실행 이미지를 별도로 대조하지 않으면 provenance 증명이 아닙니다. 이 harness 결과가 없는 단위 테스트만으로 GPU·driver·NCCL·실제 worker 노드 토폴로지를 검증했다고 간주하지 않으며, harness 성공도 controller의 노드별 증적을 대체하지 않습니다. harness 역시 worker 내부 adjusted rank를 직접 읽는 것이 아니라 고정 소스 규칙과 actor 순서·노드 배치를 결합합니다.

## 현재 모델 자격 검증 흐름

중앙 모델 레지스트리는 고정된 모델 아티팩트, OCI 런타임 이미지, 모델 릴리스와 배치 프로필을 저장합니다. `VALIDATED` 릴리스의 구조화된 벤치마크 증적은 수동 등록 또는 폐쇄형 Agent 작업으로 만들 수 있습니다.

```text
신뢰된 외부 측정 도구의 관리자 인증 등록
또는 준비 → 명시적 적용 → 단일 노드 BENCHMARK
        ↓ 구조화된 증적
릴리스·배치·정렬된 노드 UUID·현재 프로필 지문 결합
        ↓ 고정 아티팩트·런타임 식별자와 SLO 검사
모든 배치 프로필의 최신 증적이 PASSED
        ↓ 운영자의 명시적 승격 요청
ACTIVE 릴리스 (배포·호스트 변경 없음)
```

최신 실패 증적은 과거 통과 결과보다 우선합니다. 승격 시 사용한 증적 집합을 릴리스에 고정해 반복 요청이 같은 결과를 내게 합니다. 증적 판정과 승격 자체는 중앙 DB만 변경하며 deployment, task, 다운로드 또는 컨테이너 작업을 만들지 않습니다.

자동 경로의 준비 단계는 고정 실행 문맥만 DB에 저장하고 작업이나 호스트 변경을 만들지 않습니다. 운영자가 `{"apply": true}`를 보낸 뒤에만 단일 승인 노드에 `BENCHMARK` 작업 하나를 큐잉합니다. Agent는 현재 프로필 지문, 정확한 로컬 모델 캐시와 다이제스트 고정 로컬 이미지를 다시 확인하고, 고정된 컨테이너 명령만 실행합니다. 결과는 폐쇄형 수치 요약으로 검증해 증적으로 연결합니다.

이 경로는 단일 노드 GPU 작업 부하만 지원합니다. 다중 노드 네트워크·NCCL 시험, 전체 동시성 매트릭스와 24시간 장애 복구 검증은 중앙 제어면이 자동 수행하지 않습니다.

## 추천 스냅샷, 배포 세대와 롤백 흐름

GPU 추가에 따른 정책 기반 모델 추천, 불변 스냅샷 저장, 명시적 수락, 세대별 적용·검증 상태와 직전 검증 세대 롤백은 구현되었습니다. 추천과 수락은 계속 호스트 변경 권한과 분리됩니다.

```text
승인 또는 명시적 PROBE 완료
        ↓
신선한 인벤토리 스냅샷
        ↓
결정론적 계획기와 ACTIVE 모델 릴리스 평가
        ↓
추천·정규화 인벤토리 스냅샷 저장 (호스트 변경 없음)
        ↓
운영자 수락 전 현재 상태 재검사
        ↓
불변 배포 세대 생성 (현재 구현, 작업 없음)
        ↓
명시적 적용·검증 작업과 노드별 상태 추적
        ↓ 전체 배정 노드 성공과 backend별 최소 Agent 버전 충족
verified_at 롤백 증거 기록
        ↓ 준비 요청은 작업 없음
명시적 적용으로 직전 검증 세대 롤백
```

추천 API는 저장된 노드 프로필과 `ACTIVE` 릴리스·배치 프로필을 조회하고 콘텐츠 해시 ID가 있는 결과와 지문 계산용 정규화 인벤토리를 한 번만 저장합니다. 이 기록은 나중에 `recommendation show`로 조회할 수 있습니다. 추천 생성 중에는 deployment·task·benchmark run을 만들지 않고, 프로필 갱신은 운영자가 별도 `PROBE` 작업으로 명시해야 합니다.

수락 API는 저장된 원래 선택 모드로 현재 추천을 다시 계산합니다. PostgreSQL에서는 재평가와 세대 저장이 끝날 때까지 추천 입력 레지스트리·인벤토리 테이블에 공유 잠금을 걸어 새 후보나 노드가 중간에 끼어드는 팬텀 변경을 막습니다. 콘텐츠 ID, 카탈로그·정책 버전, 인벤토리 지문·스냅샷과 선택 릴리스·배치·노드가 모두 같을 때만 OCI 이미지와 모델 리비전이 고정된 `CREATED` 배포 세대 하나를 저장합니다. 선택적으로 지정한 이전 세대는 같은 계보의 최신 세대여야 합니다. 반복 수락은 기존 세대를 반환합니다. 수락은 `accept_model_download=false`, `pull_image=false`를 강제하고 Agent 작업, 다운로드, 이미지 내려받기, Docker 실행 또는 기존 컨테이너 중지를 수행하지 않습니다.

`APPLY_DEPLOYMENT`와 `VERIFY` 묶음은 배포 세대에 연결된 operation으로 기록됩니다. operation 상태는 `PREPARED`, `QUEUED`, `RUNNING`, `SUCCEEDED`, `PARTIAL_FAILED`, `FAILED`의 폐쇄 목록을 사용하고, 노드별 단계는 `PENDING`, `QUEUED`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELED`로 추적합니다. 각 노드 단계에는 시도 번호가 있으며 생성된 task는 해당 단계와 시도 번호에 결합됩니다. 현재 시도 번호·단계·작업 유형과 일치하지 않는 늦은 claim이나 완료 보고는 상태를 전진시키지 않습니다.

전체 배정 노드를 정확히 대상으로 한 `VERIFY`가 모두 성공하고 backend별 최소 Agent 버전을 충족할 때만 `verified_at`을 기록합니다. legacy 배포는 0.3.12 이상, `VLLM_RAY_PP_V1`은 0.3.18 이상이 필요합니다. 엄격한 backend에서는 각 노드의 정확한 `pipeline-rank-contract`와 head의 API 검증도 성공해야 합니다. 일부 노드 검증, API 검증을 생략한 엄격한 결과, 전체 배정 집합이 아닌 Ray head 전용 검증과 구 Agent 결과는 운영 상태 조회에는 남지만 롤백 증거가 되지 않습니다. `dure admin deployment show`는 한 세대와 연결된 작업을, `dure admin deployment generations`는 같은 계보의 모든 세대를 조회합니다.

롤백 요청에서 서버가 대상 계획을 선택하므로 클라이언트는 대상 세대, 계획, 다운로드 또는 이미지 내려받기 값을 지정할 수 없습니다. 소스는 계보의 최신 세대여야 하고 대상은 그 소스가 직접 가리키는 직전 세대여야 하며, 대상 상태가 `VERIFIED`이고 `verified_at`이 있어야 합니다. 두 세대는 전체 노드 배정과 토폴로지가 정확히 같아야 합니다. 요청한 정규 UUID 노드 집합도 전체 배정 집합과 정확히 같아야 하고, 모든 노드는 승인됨·온라인이며 legacy는 Agent 0.3.12 이상, `VLLM_RAY_PP_V1`은 0.3.18 이상이어야 합니다. 두 계획의 이미지가 OCI 다이제스트로 고정되지 않았으면 거부합니다.

기본 롤백 요청은 `PREPARED` operation만 저장하고 task를 만들지 않습니다. `apply=true`를 명시해야 계보의 활성 변경이 되며 다음 단계를 순서대로 실행합니다.

```text
STOP_SOURCE
    ↓ 모든 소스 노드 성공
START_TARGET (serve=false)
    ↓ 모든 대상 노드 성공
VERIFY_TARGET
    ↓ 모든 대상 노드 성공
선택적 START_API (Ray head 한 대)
    ↓
선택적 VERIFY_API (Ray head 한 대)
```

각 단계의 모든 노드가 성공해야 다음 단계를 한 트랜잭션에서 큐잉합니다. 실패하거나 취소된 단계는 다음 단계로 넘어가지 않으며, 운영자가 같은 입력에 `apply=true`를 다시 지정하면 현재 단계의 실패 노드만 증가한 시도 번호로 재시도합니다. 이미 성공한 노드의 과거 시도나 늦은 완료는 현재 상태를 덮어쓰지 않습니다. 준비 상태의 operation은 여러 개 만들 수 있지만 같은 계보에서 실제 적용 중인 변경은 하나만 허용합니다.

롤백 task는 `accept_model_download=false`, `pull_image=false`를 강제합니다. 대상 모델 캐시와 다이제스트 이미지는 모든 노드에 미리 있어야 합니다. 현재 구현은 같은 GPU에서 소스 컨테이너를 중지하고 대상 세대를 다시 만드는 방식이므로 중단 시간이 발생할 수 있고 블루·그린 배포가 아닙니다.

새 Ray·API 컨테이너에는 `dure.deployment`, `dure.generation`, `dure.node` 레이블을 모두 기록합니다. 시작·검증·중지는 예상 배포 ID, 세대와 노드 ID를 다시 검사하며 하나라도 다른 컨테이너는 조작하지 않습니다. 0.3.12 이전에 만든 컨테이너는 `dure.node` 레이블이 없을 수 있어, 이 경우에만 정확한 배포 ID와 세대가 모두 일치할 때 제한적으로 기존 컨테이너로 인정합니다. 이미 존재하는 노드 레이블이 다르거나 배포·세대 레이블이 없거나 다르면 항상 거부합니다.

추천기는 모델의 이름·크기만 비교하지 않고 GPU VRAM, 드라이버·연산 능력·런타임 지원 아키텍처, 디스크, 모델 아티팩트, 런타임 이미지, 컨텍스트·동시성, 네트워크/NCCL 조건을 평가합니다. 중앙 다중 노드 후보는 정확히 정렬된 노드 UUID 집합·릴리스·배치 프로필·런타임·현재 인벤토리 지문에 결합된 최신 `PASSED` 네트워크·NCCL 증적을 적격성 입력으로 조회합니다. 증적이 없거나 오래됐거나 다른 조합에 속하거나, 그 뒤에 실패·취소·진행 중 실행이 있으면 실패 안전 방식으로 거부합니다. 다만 Dure가 네트워크·NCCL 시험 자체를 다중 노드에서 자동 실행하는 기능은 아직 구현되지 않았습니다. 현재 계획 전송 형식으로 안전하게 표현할 수 없는 TP 계열 배치도 수락 단계에서 거부합니다. 세대별 상태와 롤백은 전체 작업 부하 매트릭스, 네트워크·NCCL 시험 또는 24시간 복구 수용 검사를 새로 수행하지 않으므로 이 제한을 해제하지 않습니다. 모델 레지스트리, 아티팩트 검증, 세대별 단계적 전환의 상세 정책은 [모델 선택 정책](model-selection.md)과 [벤치마크 문서](benchmarking.md)를 따릅니다.

Codex 진단은 이 결정론적 선택의 입력이 아닙니다. 사람이 해석할 수 있는 참고 보고서로 유지합니다.

## 작업 프로토콜

현재 지원하는 작업 유형은 다음으로 고정됩니다.

- `PROBE`
- `BENCHMARK`
- `PREPARE_MODEL`
- `PREPARE_IMAGE`
- `VERIFY`
- `APPLY_DEPLOYMENT`
- `START_DEPLOYMENT`
- `STOP_DEPLOYMENT`
- `RESTART_DEPLOYMENT`

`PREPARE_MODEL`과 `PREPARE_IMAGE`는 일반 작업 생성 API로 만들 수 없습니다. 추천을 수락해 만든 세대에 대해 관리자 전용 deployment 준비 API가 먼저 불변 preview를 저장하고, 같은 요청에 명시적으로 `apply=true`를 지정한 뒤에만 노드별 모델 작업을 만듭니다. 모델 해시와 marker 검증이 성공한 노드에만 다이제스트 고정 이미지 작업을 이어서 만들며, 실패 시에는 성공한 단계를 보존하고 실패한 현재 단계만 새 시도 번호로 재시도합니다. 모든 노드의 두 단계가 성공해야 콘텐츠 주소 final 경로를 세대 plan에 주입해 일반 apply가 소비할 수 있습니다. 롤백은 과거의 정확한 성공 증적과 로컬 캐시·이미지만 사용하고 새 준비 작업이나 네트워크 복구를 만들지 않습니다.

`POST /v1/admin/benchmark-runs/prepare`는 이 아티팩트 준비 API와 별개입니다. 벤치마크 실행 문맥만 고정하고 모델 바이트를 준비하지 않으며, 적용 시 대상 노드에 배포·준비·다른 벤치마크 작업이 있으면 실패 안전 방식으로 거부합니다.

에이전트는 HTTPS 폴링으로 한 번에 하나의 작업을 5분 임대로 요청하고 실행 중 임대를 갱신합니다. 완료한 작업 ID와 결과는 로컬에 보관하므로 재전달된 작업은 가능한 한 변경을 반복하지 않고 이전 결과를 보고합니다. PostgreSQL 노드 행 잠금은 같은 노드의 요청을 직렬화합니다.

배포 operation 생산자는 전체 노드 집합을 UUID 순서로 잠근 뒤 계보를 잠그며, 같은 노드를 공유하는 다른 계보의 활성 operation이나 배포 task를 거부합니다. 완료·취소 경로는 공유 operation과 정렬된 노드별 상태를 같은 순서로 잠가 다중 노드 동시 완료를 직렬화합니다. 만료되지 않은 실행 task의 heartbeat만 lease를 연장할 수 있고 현재 operation 시도와 맞지 않는 heartbeat는 거부합니다.

배포 task의 페이로드는 작업 종류별 폐쇄형 스키마입니다. 모든 배포 작업은 저장 계획과 세대를 요구하며, 문자열 불리언, 임의 명령, Docker 인자와 외부 task의 배포 ID 불일치는 probe나 호스트 변경 전에 거부합니다. controller도 verify와 단계별 성공 결과에서 필수 검사 이름, 중복, 엄격한 필드·불리언과 blocking 실패를 다시 검증합니다.

`BENCHMARK`는 일반 작업 생성 API에서 만들 수 없습니다. 관리자 준비 API가 릴리스·배치·노드·인벤토리·작업 부하를 고정한 뒤, 별도 적용 API가 정확히 한 작업을 만듭니다. 페이로드는 suite, 정책, 모델·이미지 식별자, 고정 작업 부하 수치와 `apply=true`만 허용하고 셸 명령, Docker 인자, 환경 변수, 마운트, 프롬프트 또는 호스트 경로를 받지 않습니다. 노드는 한 번에 하나의 임대 작업만 처리하는 기존 직렬화 규칙을 그대로 따릅니다.

stage builder는 이 중앙 작업 열거형에 속하지 않습니다. 신뢰된 오프라인 환경에서만 실행하며, variant 등록·증적 기록·상태 전이는 중앙 DB 작업일 뿐 Agent 작업이나 호스트 변경 권한이 아닙니다.

계획은 서버가 발급한 노드 UUID를 사용합니다. 레거시 호스트명 배정은 승인된 노드 하나로만 해석될 때 정규화할 수 있습니다. `VLLM_RAY_PP_V1`은 이 호환 정규화를 허용하지 않고 계획의 UUID 전체 집합을 서버 노드와 직접 대조하며, 비중지 작업은 전체 배정 노드와 0.3.18 이상 Agent를 요구합니다. 중앙 배포 이미지는 OCI 다이제스트로 고정돼야 합니다.

## 신뢰 경계

- 공개 관리 경계는 HTTPS이며 데이터베이스와 Ray 포트는 사설망에 남아야 합니다.
- 관리자 전달자 자격 증명과 노드 자격 증명은 다른 권한을 가집니다.
- 토큰 없는 등록은 대기 상태 하트비트 권한만 주며 실행 권한을 주지 않습니다.
- 에이전트는 Docker와 `/var/lib/dure`를 관리하므로 root로 실행됩니다. 중앙 제어면이 일반 원격 셸이 되지 않도록 작업 언어는 폐쇄형입니다.
- stage builder는 digest 고정 별도 환경에 격리하고 remote code·adapter·임의 아키텍처를 거부합니다. 기본 Agent 패키지에 빌더 heavy dependency를 설치하지 않습니다.
- `VLLM_RAY_PP_V1`은 RFC1918 주소와 고정 포트만 허용하지만 host firewall을 대신하지 않습니다. GCS 6379와 worker 20000-21000은 신뢰 포드 안에서만 접근 가능해야 하고 API 8000은 loopback에 남겨야 합니다.
- 엄격한 rank 결과는 직접 runtime rank 관측이 아니라 vLLM 0.9.0 소스 계약과 Ray topology의 결합입니다. 버전·주소·노드·actor가 하나라도 불확실하면 실패로 닫고 다른 정렬 규칙을 추측하지 않습니다.
- GPU 호스트 운영자는 로컬 작업 부하를 관찰할 수 있습니다. 더 강한 기밀 컴퓨팅 경계가 생기기 전에는 민감 프롬프트나 비밀값을 커뮤니티 노드에서 처리하면 안 됩니다.

운영 절차는 [operations.md](operations.md), 위협 모델과 보안 강화 작업 목록은 [security.md](security.md)를 참고합니다.

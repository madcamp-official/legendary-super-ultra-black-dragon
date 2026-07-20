# Dure

**유휴 GPU를 모아 신뢰 가능한 커뮤니티 LLM 인프라를 구축합니다.**

Dure는 Linux CLI, 노드 에이전트, 선택형 중앙 제어면으로 구성된 커뮤니티 LLM 인프라 MVP입니다. 노드 하드웨어를 조사하고, 자원에 맞는 배포 계획을 만들며, Docker·Ray·vLLM으로 모델을 준비하고, GPU·클러스터·API 준비 상태를 확인합니다.

이 저장소는 신뢰된 운영자와 사설 네트워크를 위한 실행 가능한 MVP입니다. 누구나 참여하는 공개 GPU 네트워크나 완성된 공개 추론 서비스는 아직 제공하지 않습니다.

## 현재 제공 기능

- Linux 호스트의 CPU, 메모리, 디스크, 가상화, 네트워크 정보 수집
- NVIDIA GPU, VRAM, 드라이버, compute capability 조사
- Docker/NVIDIA runtime/Ray 탐지 및 CPU 전용 utility 노드 분류
- Qwen2.5 AWQ 7B·14B·32B·72B의 로컬 계획 생성
- 버전이 있는 정적 모델 카탈로그와 입력 순서에 독립적인 결정론적 로컬 모델 선택
- 3대의 24GB GPU 노드를 이용한 Qwen2.5-72B-AWQ pipeline 계획과 `27/27/26` 레이어 분할
- 노드 수명 주기 상태의 원자적 저장
- 재개 가능한 Hugging Face 다운로드 준비 영역 및 Docker 기반 Ray/vLLM 실행
- 호스트 GPU, 컨테이너 CUDA, Ray 리소스, HTTP 상태 확인, 제공 모델 검증
- Dure·Hugging Face·Ollama 모델 캐시와 일반 LLM 컨테이너의 메타데이터 수집
- 대기 상태 승인, 외부 방향 폴링, 임대 기반 중앙 작업 관리
- 고정된 모델 아티팩트·런타임·배치 프로필을 관리하는 중앙 모델 레지스트리
- 상대 일반 파일과 SHA-256 청크 관계를 저장하는 불변 정규 아티팩트 매니페스트 레지스트리
- vLLM 0.9.0의 제한된 계약으로 pipeline rank별 파일을 생성하는 신뢰된 오프라인 stage builder와 `DRAFT`·`VALIDATED`·`REVOKED` variant 레지스트리
- vLLM 0.9.0 V0 Ray에 고정된 폐쇄형 다중 노드 실행 백엔드 `VLLM_RAY_PP_V1`과 결정론적 pipeline rank 결합
- 신뢰 HTTPS origin, 재개 다운로드, 청크 CAS, 결정적 staging과 원자적 marker-last 활성화를 사용하는 로컬 `FULL_SNAPSHOT` 모델 캐시 준비기
- 저장된 GPU 인벤토리와 `ACTIVE` 릴리스를 평가해 불변 스냅샷으로 보관하는 결정론적 추천
- 추천 유효성을 다시 검사해 적용 전 배포 세대만 만드는 명시적 추천 수락
- 추천 세대별 준비 계획을 먼저 저장하고 명시적 적용 뒤에만 모델·이미지를 노드별로 준비하는 중앙 아티팩트 작업
- 모델 해시 검증 뒤 다이제스트 고정 OCI 이미지를 준비하며, 실패한 현재 단계만 재시도하는 노드별 준비 증적
- 배포 세대별 `APPLY`·`VERIFY` 작업, 노드별 상태와 재시도 시도 번호 추적
- 조회 전용 세대 이력과 준비·명시적 적용을 분리한 직전 검증 세대 롤백
- 구조화된 벤치마크 증적 저장과 모든 배치 프로필을 검사하는 `ACTIVE` 승격 게이트
- 정확한 다중 노드 UUID 조합의 최신 네트워크·NCCL 증적을 다시 검사하는 중앙 추천 자격 게이트
- 준비와 명시적 적용을 분리한 단일 노드 `BENCHMARK` 작업과 고정 작업 부하 실행기
- Codex를 이용한 읽기 전용 용량 진단
- 기본 모의 실행과 명시적 변경 플래그

## 문서

- [문서 색인](docs/README.md)
- [아키텍처](docs/architecture.md)
- [운영 절차](docs/operations.md)
- [보안 모델](docs/security.md)
- [모델 선택 정책](docs/model-selection.md) — 부분 구현
- [벤치마크 및 모델 자격 검증](docs/benchmarking.md) — 부분 구현
- [모델 아티팩트 매니페스트와 배포 계약](docs/artifact-distribution.md) — `FULL_SNAPSHOT` 배포 경로 구현
- [vLLM 단계 아티팩트 생성과 검증](docs/stage-artifacts.md) — 생성·등록 구현, 노드 배포 소비는 계획됨
- [vLLM 다중 노드 rank 결합 결정 기록](docs/adr-vllm-multinode-rank-binding.md) — 고정 런타임과 간접 증적의 근거
- [개발·릴리스 절차](docs/development.md)
- [APT 배포](docs/apt-distribution.md)
- [개발 로드맵](docs/roadmap.md)
- [제품 제안서](docs/dure-proposal.md)

## 개발 환경 설치

```bash
cd /root/workspace/dure
python3 -m pip install -e '.[test]'
```

서명된 APT 패키지에는 의존성이 없는 노드 CLI와 Agent만 포함됩니다. 중앙 제어면은 별도로 설치합니다.

```bash
python3 -m pip install -e '.[server]'
```

기본 Debian 패키지에는 vLLM, PyTorch, safetensors나 CUDA 계열 stage builder 의존성을 넣지 않습니다. rank별 `STAGE` 생성은 운영 Agent나 중앙 서버가 아니라 별도의 다이제스트 고정 OCI 빌더 환경에서 수행합니다.

## APT 설치

서명된 저장소가 게시된 뒤에는 다음 한 번의 등록으로 설치할 수 있습니다.

```bash
curl -fsSL https://chek737.github.io/dure/install.sh | sudo sh
```

APT 서명 키 fingerprint는 다음과 같습니다.

```text
E1F952F8B23E7A1B884CB5A33EC5C8CAE53AFA01
```

이후 설치와 업그레이드는 일반 APT 명령을 사용합니다.

```bash
sudo apt install dure
sudo apt upgrade
```

## 노드 조사

```bash
dure doctor
dure doctor --json
dure doctor --output camp-9.json
```

## 배포 계획 만들기

각 노드에서 프로필을 내보냅니다.

```bash
dure doctor --output camp-7.json
dure doctor --output camp-8.json
dure doctor --output camp-9.json
```

공유할 계획을 만듭니다.

```bash
dure plan \
  --profile camp-7.json \
  --profile camp-8.json \
  --profile camp-9.json \
  --model qwen2.5-72b-awq \
  --image registry.example.com/vllm@sha256:<digest> \
  --network-interface ens3 \
  --output qwen72b-plan.json
```

로컬 `dure plan`이 생성하는 기존 JSON 형식과 실행 경로는 호환성을 유지합니다. 중앙 추천을 수락해 만드는 2·3노드 계획은 별도의 폐쇄형 `VLLM_RAY_PP_V1` 계약을 사용하며, 모든 노드가 동일한 불변 계획을 받아야 합니다. 이 엄격한 계획은 서버가 발급한 노드 UUID, 노드별 정상 GPU 정확히 한 장, 고유한 RFC1918 IPv4 주소, `TP=1`, `PP=2` 또는 `PP=3`을 요구합니다. head는 rank 0이고 worker는 IPv4 문자열 오름차순으로 rank가 고정됩니다.

## 노드 초기화와 검증

안전한 모의 실행:

```bash
dure init --plan qwen72b-plan.json
dure status
```

검토 후 실제 적용:

```bash
sudo dure init \
  --plan qwen72b-plan.json \
  --apply \
  --accept-model-download \
  --pull
```

여기서 `--accept-model-download`는 기존 로컬 Hugging Face 모델 흐름에 대한 명시적 동의입니다. 새 콘텐츠 주소 캐시를 중앙에서 준비하거나 추천 모델을 자동 설치하는 기능은 아닙니다.

API는 할당된 Ray head에서만 시작합니다. 모든 worker가 join한 뒤 Ray head에서 실행합니다.

```bash
sudo dure init \
  --plan qwen72b-plan.json \
  --apply \
  --serve
```

배포를 검증합니다.

```bash
dure verify --plan qwen72b-plan.json --api
```

## 안전 모델

Dure는 NVIDIA host driver를 설치하거나 변경하지 않습니다. driver가 없거나 호환되지 않으면 provisioning을 중단하며, 운영자가 직접 조치해야 합니다.

CLI는 OCI 다이제스트로 고정되지 않은 이미지를 기본적으로 거부합니다. 중앙 배포는 항상 다이제스트로 고정한 이미지를 요구합니다. 모델 다운로드는 `--accept-model-download`, 이미지 내려받기는 `--pull`, 중지된 컨테이너 교체는 `--replace`가 필요합니다.

새 콘텐츠 주소 캐시의 production 기본 경로는 Dure 전용 고정 루트이고 task가 경로나 raw URL을 지정할 수 없게 설계되었습니다. 신뢰 HTTPS origin은 각 노드의 root 전용 Agent 설정에서만 읽고 중앙 task·DB·결과에는 URL, header나 token을 넣지 않습니다. 현재 전송기는 인증 header나 token을 지원하지 않으므로 별도 자격 증명 없이 접근 가능한 신뢰 origin이 필요합니다. 패키지는 `/var/lib/dure`를 root 소유 경계로 두고 중앙 서버용 쓰기 경로를 `/var/lib/dure/server`로 분리합니다. 준비기는 설정된 저장소·모델 루트의 symlink 조상, 소유자와 쓰기 권한을 검사합니다. 인벤토리와 벤치마크는 별도로 `/var/lib/dure/models` 루트, 캐시 후보, `config.json`과 marker를 검사하지만 상위 `/var/lib/dure` 전체 경계를 매번 재검사하지는 않습니다.

stage builder는 정확히 vLLM 0.9.0, V0 executor, `Qwen2ForCausalLM` AWQ, `TP=1`, `sharded_state`만 지원합니다. digest로 고정한 빌더 런타임에서만 실행하고 remote code, LoRA, MoE, 멀티모달과 임의 아키텍처를 거부합니다. `STAGE` 등록만으로 다운로드·배포 또는 기존 컨테이너 변경이 발생하지 않으며, 실제 GPU `GPU_EXPORT_LOAD/PASSED` 증적만 `DRAFT`를 `VALIDATED`로 승격할 수 있습니다. 검증하지 못한 `NOT_RUN`은 성공으로 취급하지 않습니다. 번들 GPU acceptance harness는 현재 `PP=1`만 native load를 검증하므로, PP>1 variant는 모든 rank를 검증하는 별도 신뢰 증적 없이는 `DRAFT`로 유지합니다.

Ray GCS, 대시보드, 워커 포트는 신뢰된 LAN 또는 WireGuard 같은 사설 오버레이에만 노출해야 합니다. 공개 인터넷에 노출해서는 안 됩니다.

`VLLM_RAY_PP_V1`은 정확히 vLLM 0.9.0 V0 executor와 Ray를 사용합니다. GCS는 `6379`, Ray worker 범위는 `20000-21000`으로 고정하고 vLLM API는 head의 `127.0.0.1:8000`에만 바인딩합니다. 각 컨테이너는 배포·세대·노드 외에 backend, pipeline rank, runtime rank, component 레이블까지 정확히 일치해야 시작·검증·중지·재시작 대상이 됩니다. 현재 이 백엔드는 각 노드에 검증된 모델 전체가 있는 `FULL_SNAPSHOT`만 지원합니다.

준비 상태의 `pipeline-rank-contract` 검사는 계획에 고정된 vLLM 0.9.0의 정렬 규칙, 정확한 Ray 노드 집합, 노드별 GPU 한 장과 `dure_node_<uuidhex>` custom resource를 확인하고, API 시작 뒤에는 vLLM worker actor 토폴로지도 요구합니다. 컨테이너의 `dure.runtime-contract` SHA-256은 이미지·mount·GPU·network·entrypoint·고정 환경·명령 drift를 시작·재사용·readiness에서 차단합니다. Ray가 vLLM 내부 pipeline rank 숫자를 공개 필드로 직접 보고하는 것은 아니므로, 이 결과는 **소스에 고정된 rank 계약의 간접 증적**입니다. 실제 2·3노드 GPU 환경에서는 별도의 수용 검사로 분산 load와 최소 추론까지 확인해야 합니다.

실제 GPU 수용 검사는 `scripts/acceptance-vllm-ray-pp.py`를 사용합니다. 기본 실행, opt-in 누락, `/etc/dure/acceptance-vllm-ray-pp-v1.json` 또는 고정 런타임·모델 전제 조건 누락은 호스트 변경 없이 `NOT_RUN`과 종료 코드 `77`을 반환합니다. 검사에는 명령행 인자를 전달하지 않으며 `DURE_RUN_VLLM_RAY_PP_ACCEPTANCE=1`만 허용합니다. harness는 Ray custom resource로 Dure UUID와 주소를 다시 결합하지만 설정의 runtime image digest는 선언값이므로 신뢰된 digest 고정 wrapper와 운영 기록 안에서 실제 이미지를 대조해야 합니다. 실제 분산 load가 시작된 뒤의 오류는 `FAILED`와 0이 아닌 종료 코드로 남겨야 하며 `NOT_RUN`이나 성공으로 바꾸면 안 됩니다. 이 harness는 controller의 노드별 `pipeline-rank-contract` 증적을 대체하지 않습니다.

## 노드 수명 주기

```text
DISCOVERED → PROBING → ELIGIBLE → PLANNED
                        ↓
                  DOWNLOADING → STARTING → VERIFYING → READY
                                                └────→ WAITING_FOR_PEERS
차단 오류 발생 → FAILED
```

상태는 `$XDG_STATE_HOME/dure/state.json` 또는 기본 경로인 `~/.local/state/dure/state.json`에 저장됩니다.

## 현재 제한 사항

- 현재 `--model auto`는 버전이 있는 Qwen2.5 AWQ 정적 카탈로그와 결정론적 선택기를 사용합니다. 중앙 추천의 다중 노드 후보는 정확히 정렬된 노드 UUID 조합·릴리스·배치 프로필·런타임·현재 인벤토리 지문에 결합된 24시간 이내의 최신 `PASSED` 네트워크·NCCL 증적이 있어야 합니다. 로컬 `dure plan --model auto`는 기존 3×24GB 계획 호환성을 위해 이 중앙 증적 검사만 예외로 두고 계획에 검증 경고를 남깁니다. 중앙 모델 레지스트리, 벤치마크 증적 기반 `ACTIVE` 승격 게이트, 추천 스냅샷과 명시적 수락, 세대별 적용·검증 상태와 명시적 롤백도 제공합니다.
- 롤백은 최신 세대의 검증된 직전 세대로만 가능하며, 전체 배정 노드·동일 토폴로지·승인·온라인 상태와 backend별 최소 Agent 버전을 요구합니다. legacy는 0.3.12 이상, `VLLM_RAY_PP_V1`은 0.3.18 이상입니다. 추천으로 만든 대상에는 0.3.16 준비 흐름에서 기록한 성공 증적도 필요합니다. 롤백 중에는 모델 다운로드나 이미지 pull을 허용하지 않고 이미 검증된 정확한 캐시와 로컬 이미지만 사용합니다. 같은 GPU에서 이전 세대 컨테이너를 다시 만드는 절차이므로 서비스 중단이 생길 수 있으며 블루·그린 전환이 아닙니다.
- 중앙 제어면은 승인된 단일 노드에서 고정된 네 가지 작업 부하를 `BENCHMARK` 작업으로 실행하고 결과를 증적으로 수집할 수 있습니다. 자동 실행은 `/var/lib/dure/models`에 펼쳐지고 `.dure-model.json`으로 고정 식별자를 결합한 모델과 로컬 다이제스트 이미지를 요구하며, Hugging Face hub snapshot, 다중 노드 네트워크·NCCL 시험, 전체 동시성 매트릭스와 24시간 복구 검증은 아직 지원하지 않습니다.
- 중앙 아티팩트 매니페스트 등록·조회, 콘텐츠 주소 기반 `FULL_SNAPSHOT` 준비기와 명시적 중앙 준비 흐름은 구현되었습니다. 준비 preview는 DB 계획만 만들고, 별도 적용 뒤 각 노드에서 `PREPARE_MODEL → PREPARE_IMAGE`를 순서대로 수행합니다. 모델은 전체 청크·파일 해시와 marker를 검증하고 이미지는 다이제스트 고정 참조를 pull한 뒤 다시 inspect합니다. 추천이나 수락 자체는 여전히 다운로드를 시작하지 않으며, 성공한 정확한 준비 증적이 없는 추천 세대는 apply할 수 없습니다.
- 오염된 CAS·staging·final은 덮어쓰거나 자동 삭제하지 않고 실패합니다. 현재 공식 quarantine·캐시 퇴출 및 전역 참조 검사 명령이 없습니다. 특히 공유 CAS 청크는 모든 매니페스트와 진행 중 준비의 미참조를 증명할 수 없으면 옮기거나 지우면 안 됩니다.
- 신뢰된 오프라인 빌더와 중앙 레지스트리는 제한된 `STAGE` variant를 생성·등록하고 실제 GPU export/load 증적으로 검증할 수 있습니다. vLLM의 sharded-state 파일명이 TP rank를 사용하므로 `TP=1`, `PP>1`에서는 각 worker 출력을 반드시 `stages/<pp-rank>`로 격리합니다. 공용 디렉터리에 쓰면 모든 PP worker의 rank가 0이 되어 파일 충돌이나 덮어쓰기가 발생할 수 있습니다.
- 중앙 준비와 Agent 배포는 여전히 모델 전체를 각 대상 노드에 두는 `FULL_SNAPSHOT`만 지원합니다. `VLLM_RAY_PP_V1`도 이 전체 캐시만 소비합니다. `VALIDATED` stage variant의 rank별 다운로드·원자적 활성화와 실제 `sharded_state` 로더 결합은 다음 PR 범위이며, probe와 조정되는 `READY`·`STALE`·`MISSING`·`CORRUPT`·`QUARANTINED` 캐시 투영은 그 다음 PR 범위입니다. stage 빌드·등록·검증 실패는 새 배포를 시작하지 않고 실행 중인 이전 배포를 자동 중지하거나 교체하지 않습니다.
- 명시적 `deployment create` 긴급 복구 경로는 여전히 프로필 JSON 파일이 필요합니다. 중앙에 저장된 프로필만으로 계획을 만드는 경로는 유효한 추천을 명시적으로 수락할 때만 제공됩니다.
- 적용 모드는 Docker만 지원하며, 물리 노드당 GPU 한 장만 배정합니다.
- Dure가 직접 실행하는 다중 노드 네트워크 벤치마크와 NCCL 집합 연산 조사는 아직 구현되지 않았습니다.
- 정규화된 모델·stage 아티팩트 매니페스트 등록은 제공하지만 SHA-256과 exporter 다이제스트는 게시자 서명이나 신뢰 provenance가 아닙니다. 게시자·이미지 서명 검증은 계획 단계입니다.
- 게이트웨이, 최종 사용자 인증, 크레딧 원장, WireGuard 자동화, 공개 노드 샌드박스는 아직 제공하지 않습니다.

## 중앙 노드 관리

Dure에는 선택형 FastAPI/PostgreSQL 제어면과 외부 방향 폴링 노드 에이전트가 있습니다. `dure-server --migrate`를 실행하고 `DURE_DATABASE_URL`, `DURE_ADMIN_TOKEN`을 설정한 뒤 TLS 역방향 프록시 뒤에서 `dure-server`를 시작합니다.

패키지의 `/etc/dure/dure-client.env`에는 제어면 주소가 들어 있습니다. 새 머신은 별도 노드 토큰이나 서버 인자 없이 등록합니다.

```bash
sudo apt install dure
sudo dure join
```

등록한 노드는 대기 상태입니다. 하트비트는 보낼 수 있지만 승인 전에는 작업을 요청할 수 없습니다.

```bash
dure admin nodes --pending
dure admin node approve <node-id>
```

중앙 작업은 `PROBE`, `BENCHMARK`, `PREPARE_MODEL`, `PREPARE_IMAGE`, `VERIFY`, `APPLY_DEPLOYMENT`, `START_DEPLOYMENT`, `STOP_DEPLOYMENT`, `RESTART_DEPLOYMENT`로 고정됩니다. `BENCHMARK`와 두 준비 작업은 각각의 전용 준비·적용 API로만 만들 수 있고 일반 task 생성 API나 임의 remote shell 명령으로 만들 수 없습니다.

관리자는 승인된 온라인 노드의 metadata를 이용해 Codex 기반 용량 진단을 요청할 수 있습니다.

```bash
codex login status
dure admin diagnose
dure admin diagnose --nodes <node-id> <node-id> --json --output diagnosis.json
```

진단은 하드웨어, 네트워크, 설치 모델, 컨테이너 metadata를 configured Codex provider로 보냅니다. Dure credential, container 환경 변수·명령, mount 내용, prompt는 보내지 않으며 진단 자체가 배포를 만들거나 적용하지도 않습니다.

저장된 중앙 인벤토리만 사용해 `ACTIVE` 모델 릴리스의 배치 가능성을 조회할 수도 있습니다. 최신 조사가 필요하면 별도 `probe` 작업을 먼저 완료한 뒤 추천을 다시 요청합니다.

```bash
dure admin probe --nodes <node-id> <node-id>
dure admin deployment recommend --all-online
dure admin deployment recommend --nodes <node-id> <node-id> --objective quality-first
dure admin recommendation show <recommendation-id>
dure admin recommendation accept <recommendation-id>
# 기존 세대의 후속 세대로 연결할 때만 지정합니다.
dure admin recommendation accept <recommendation-id> \
  --previous-generation <deployment-id>

# 같은 request ID로 먼저 준비 계획만 저장한 뒤 명시적으로 적용합니다.
dure admin deployment prepare <deployment-id> --request-id <request-uuid>
dure admin deployment prepare <deployment-id> \
  --request-id <request-uuid> --apply
dure admin deployment preparation <preparation-id>

# 배포 세대와 같은 계보의 전체 세대를 조회합니다.
dure admin deployment show <deployment-id>
dure admin deployment generations <deployment-id>

# --apply가 없으면 안전성만 검사하고 작업을 만들지 않습니다.
dure admin deployment rollback <latest-deployment-id> \
  --nodes <node-a> <node-b> --serve

# 같은 선택 사항에 --apply를 추가해야 롤백을 시작합니다.
dure admin deployment rollback <latest-deployment-id> \
  --nodes <node-a> <node-b> --serve --apply
```

`recommend`는 추천과 정규화된 인벤토리 스냅샷 한 건만 멱등하게 저장합니다. `accept`는 저장 당시와 현재의 추천 ID·카탈로그·정책·인벤토리 지문·선택 결과가 모두 같을 때만 `CREATED` 배포 세대 한 건을 만들며, 같은 요청의 재시도는 기존 세대를 반환합니다. 이전 세대를 지정하면 해당 계보의 최신 세대에만 연결할 수 있습니다. 수락 뒤 준비 preview는 task를 0개 만들고, 같은 요청에 `--apply`를 명시해야 모델 준비를 큐잉합니다. 각 노드의 모델 준비가 성공한 뒤에만 그 노드의 이미지 준비를 시작하며, 실패 뒤 같은 적용 요청은 성공한 단계를 반복하지 않고 현재 실패 단계만 새 시도 번호로 재시도합니다.

중앙 추천이 선택한 다중 노드 세대는 `VLLM_RAY_PP_V1`로 고정됩니다. 저장과 작업 생성 시 전체 배정 노드 집합을 요구하고, hostname을 UUID로 추측해 보정하지 않으며, 모든 대상 Agent가 0.3.18 이상이어야 합니다. 사전 검사가 실패하면 컨테이너를 변경하지 않습니다. 적용 중 일부 노드가 실패하면 다음 단계로 진행하지 않으며, 전환이 이미 시작됐다면 이전 세대가 계속 실행된다고 가정하지 않고 상태를 확인해 명시적으로 복구합니다. 실패 노드는 `dure admin credential revoke <node-id>`로 작업 수신에서 격리할 수 있으며, 원인을 해결한 뒤 명시적으로 재시도하거나 검증된 직전 세대로 롤백합니다. Dure는 이 과정에서도 NVIDIA host driver를 자동 설치·변경하지 않습니다.

추천 생성과 수락은 `PROBE`나 배포 작업을 만들지 않고, 모델 다운로드, 이미지 내려받기, Docker 실행, 기존 컨테이너 중지를 유발하지 않습니다. 준비 적용은 모델 캐시와 로컬 이미지까지만 변경하고 컨테이너를 실행·중지하지 않습니다. 모든 노드의 정확한 준비 증적이 성공한 뒤에만 별도의 `dure admin apply`로 추천 세대를 실행할 수 있습니다. 전체 배정 노드의 `VERIFY`가 성공하고 legacy는 Agent 0.3.12 이상, `VLLM_RAY_PP_V1`은 0.3.18 이상이며 엄격한 rank·API 검증까지 통과할 때만 해당 세대의 `verified_at`이 롤백 증거로 기록됩니다. 추천 세대의 준비 작업 자체는 0.3.16 Agent를 요구합니다.

롤백 준비도 작업을 만들지 않습니다. `--apply`를 명시한 뒤에만 `STOP_SOURCE → START_TARGET → VERIFY_TARGET` 순서로 진행하며, `--serve`를 선택하면 Ray head에서 `START_API → VERIFY_API`를 이어서 실행합니다. 한 단계의 모든 노드가 성공해야 다음 단계로 넘어가고, 실패 뒤 같은 명령에 `--apply`를 다시 지정하면 현재 단계의 실패 노드만 새 시도 번호로 재시도합니다. 추천으로 만든 롤백 대상에는 과거에 성공한 정확한 준비 증적이 필요하며, 롤백은 새 준비 task를 만들거나 모델 다운로드·이미지 pull을 하지 않습니다.

일반 apply의 `--serve`도 전체 배정 노드가 Ray 준비를 마친 뒤 head 한 대에서만 API를 시작·검증합니다. 필수 검사가 누락되거나 중복된 verify 결과는 롤백 증거가 되지 않습니다. 성공한 롤백의 소스 세대는 다시 계보를 이어 갈 수 없으므로 이후 추천 수락은 이전 세대를 생략해 새 계보로 시작합니다. 만료된 operation task는 관리자 취소 API로 `TASK_LEASE_EXPIRED` 처리한 뒤 같은 롤백 요청을 재시도할 수 있습니다.

중앙 추천은 구조화된 네트워크·NCCL 증적을 정확한 정렬 노드 UUID 조합의 적격성 입력으로 조회합니다. 같은 조합의 최신 증적이 `PASSED`가 아니거나 24시간보다 오래됐거나 미래 시각이거나, 릴리스·배치 프로필·아티팩트·런타임·현재 인벤토리 지문이 다르면 후보를 실패 안전 방식으로 거부합니다. 증적보다 뒤에 시작된 실패 또는 진행 중 벤치마크 실행도 과거 통과 결과로 우회할 수 없습니다. 배치 프로필의 대역폭·RTT·패킷 손실·NCCL 기준을 모두 통과해야 하며, 추천 생성과 수락은 모델 다운로드·이미지 내려받기·작업 생성·호스트 변경을 수행하지 않습니다.

다중 노드 네트워크·NCCL 시험을 자동으로 실행하는 기능과 실제 GPU에서의 분산 stage 배포는 아직 구현되지 않았습니다. 현재 세대 검증과 롤백도 전체 작업 부하 매트릭스, 네트워크·NCCL 시험 또는 24시간 복구 검증을 대신하지 않습니다.

## 테스트

```bash
python3 -m unittest discover -v
```

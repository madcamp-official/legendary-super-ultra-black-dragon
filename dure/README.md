# Dure

**유휴 GPU를 모아 신뢰 가능한 커뮤니티 LLM 인프라를 구축합니다.**

Dure는 Linux CLI, 노드 에이전트, 선택형 중앙 제어면으로 구성된 커뮤니티 LLM 인프라 MVP입니다. 노드 하드웨어를 조사하고, 자원에 맞는 배포 계획을 만들며, Docker·Ray·vLLM으로 모델을 준비하고, GPU·클러스터·API 준비 상태를 확인합니다.

이 저장소는 신뢰된 운영자와 사설 네트워크를 위한 실행 가능한 MVP입니다. 누구나 참여하는 공개 GPU 네트워크나 완성된 공개 추론 서비스는 아직 제공하지 않습니다.

## 현재 제공 기능

- Linux 호스트의 CPU, 메모리, 디스크, 가상화, 네트워크 정보 수집
- NVIDIA GPU, VRAM, 드라이버, compute capability 조사
- Docker/NVIDIA runtime/Ray 탐지 및 CPU 전용 utility 노드 분류
- 기존 NVIDIA driver를 유지하면서 Docker Engine과 NVIDIA Container Toolkit을 준비하는 로컬 전용 bootstrap
- Qwen2.5 AWQ 7B·14B·32B·72B의 로컬 계획 생성
- 버전이 있는 정적 모델 카탈로그와 입력 순서에 독립적인 결정론적 로컬 모델 선택
- 노드 수 상한 없이 인벤토리를 정규화하고 노드마다 사용할 GPU 한 장의 index·UUID를 고정하는 읽기 전용 GPU 풀 스냅샷
- 3대의 24GB GPU 노드를 이용한 Qwen2.5-72B-AWQ pipeline 계획과 `27/27/26` 레이어 분할
- 노드 수명 주기 상태의 원자적 저장
- 재개 가능한 Hugging Face 다운로드 준비 영역 및 Docker 기반 Ray/vLLM 실행
- 호스트 GPU, 컨테이너 CUDA, Ray 리소스, HTTP 상태 확인, 제공 모델 검증
- Dure·Hugging Face·Ollama 모델 캐시와 일반 LLM 컨테이너의 메타데이터 수집
- 대기 상태 승인, 외부 방향 폴링, 임대 기반 중앙 작업 관리
- 고정된 모델 아티팩트·런타임·배치 프로필을 관리하는 중앙 모델 레지스트리
- 네 Qwen2.5 AWQ 모델만 대상으로 `TP=1` 자동 배치 프로필을 미리보기·원자 생성하는 버전 고정 생성기
- 자동 프로필을 `DRAFT → QUALIFYING → VALIDATED → 운영자 ACTIVE`로 전이시키는 8단계 폐쇄형 qualification 계약과 exact rank·노드·GPU UUID 증적 결합
- PRIMARY·SUPPLEMENTARY exact 증적을 GPU 풀에 투영해 서로 겹치지 않는 여러 배포 단위를 고르는 결정론적 Fleet set-packing 평가기
- 상대 일반 파일과 SHA-256 청크 관계를 저장하는 불변 정규 아티팩트 매니페스트 레지스트리
- vLLM 0.9.0의 제한된 계약으로 pipeline rank별 파일을 생성하는 신뢰된 오프라인 stage builder와 `DRAFT`·`VALIDATED`·`REVOKED` variant 레지스트리
- vLLM 0.9.0 V0 Ray에 고정된 폐쇄형 다중 노드 실행 백엔드 `VLLM_RAY_PP_V1`과 결정론적 pipeline rank 결합
- 신뢰 HTTPS origin, 재개 다운로드, 청크 CAS, 결정적 staging과 원자적 marker-last 활성화를 사용하는 로컬 `FULL_SNAPSHOT` 모델 캐시 준비기
- 추천기가 검증된 exact `STAGE`와 독립 `FULL_SNAPSHOT` 후보를 결정론적으로 평가하고, 선택한 pipeline rank별 매니페스트를 각 노드의 분리된 캐시에 준비해 vLLM `sharded_state`로 읽는 다중 노드 경로
- 저장된 GPU 인벤토리와 `ACTIVE` 릴리스를 평가해 불변 스냅샷으로 보관하는 결정론적 추천
- 추천 유효성을 다시 검사해 적용 전 배포 세대만 만드는 명시적 추천 수락
- 추천 세대별 준비 계획을 먼저 저장하고 명시적 적용 뒤에만 모델·이미지를 노드별로 준비하는 중앙 아티팩트 작업
- 모델 해시 검증 뒤 다이제스트 고정 OCI 이미지를 준비하며, 실패한 현재 단계만 재시도하는 노드별 준비 증적
- 현재 준비 성공을 `READY`로 원자 투영하고 probe·검증·variant 철회로 `STALE`·`MISSING`·`CORRUPT`를 추적하는 중앙 캐시 수명 주기
- 참조를 읽기 전용으로 검사한 뒤 exact 캐시 하나만 보존 위치로 원자 이동하는 명시적 `QUARANTINED` 처리
- 배포 세대별 `APPLY`·`VERIFY` 작업, 노드별 상태와 재시도 시도 번호 추적
- 조회 전용 세대 이력과 준비·명시적 적용을 분리한 직전 검증 세대 롤백
- 구조화된 벤치마크 증적 저장과 모든 배치 프로필을 검사하는 `ACTIVE` 승격 게이트
- 정확한 다중 노드 UUID 조합의 최신 네트워크·NCCL 증적을 다시 검사하는 중앙 추천 자격 게이트
- 준비와 명시적 적용을 분리한 단일 노드 `BENCHMARK` 작업과 고정 작업 부하 실행기
- Codex를 이용한 읽기 전용 용량 진단
- 기본 모의 실행과 명시적 변경 플래그
- 단일 GPU 릴리스의 등록·준비·벤치마크·승격·추천·배포·API 검증을 잇는 명시적 자동 활성화

## 문서

- [문서 색인](docs/README.md)
- [아키텍처](docs/architecture.md)
- [운영 절차](docs/operations.md)
- [단일 GPU 릴리스 자동 활성화](docs/activation.md)
- [보안 모델](docs/security.md)
- [모델 선택 정책](docs/model-selection.md) — 부분 구현
- [벤치마크 및 모델 자격 검증](docs/benchmarking.md) — 부분 구현
- [자동 배치 프로필 qualification](docs/profile-qualification.md) — 중앙 증적 계약 구현, 다중 노드 Agent 자동 실행기는 미구현
- [Fleet 후보 생성과 결정론적 스케줄러](docs/fleet-scheduler.md) — exact 증적 기반 다중 배포 조합, 불변 추천과 원자적 수락·노드/GPU 예약
- [모델 아티팩트 매니페스트와 배포 계약](docs/artifact-distribution.md) — `STAGE`·`FULL_SNAPSHOT` 선택, 중앙 캐시 수명 주기와 격리 구현
- [vLLM 단계 아티팩트 생성·검증·배포](docs/stage-artifacts.md) — rank별 준비와 `sharded_state` 소비 구현
- [vLLM 다중 노드 rank 결합 결정 기록](docs/adr-vllm-multinode-rank-binding.md) — 고정 런타임과 간접 증적의 근거
- [개발·릴리스 절차](docs/development.md)
- [APT 배포](docs/apt-distribution.md)
- [개발 로드맵](docs/roadmap.md)
- [제품 제안서](docs/dure-proposal.md)

## 개발 환경 설치

```bash
cd /root/workspace/dure/dure
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

이후 Dure만 설치하거나 업그레이드할 때는 다음처럼 범위를 제한합니다.

```bash
sudo apt install dure
sudo apt install --only-upgrade dure
```

호스트 전체 `apt upgrade`는 별도 유지보수 작업입니다. Docker CE 업그레이드가 Docker daemon을 재시작할 수 있으므로 bootstrap의 재시작 승인과 같은 영향 검토를 거쳐 실행합니다. 현재 공식 Dure APT 저장소는 `amd64` 패키지만 게시합니다. bootstrap 엔진은 Ubuntu 22.04·24.04의 `amd64`와 `arm64`를 지원하지만, `arm64`에서는 Dure CLI·Agent 실행 파일뿐 아니라 패키지와 동일한 `dure-agent.service`와 `/etc/dure/dure-client.env`를 별도 방식으로 먼저 설치해야 합니다.

## GPU 노드 런타임 준비

Dure 패키지는 CLI, Agent와 systemd unit을 함께 설치합니다. GPU 노드에는 정상 동작하는 NVIDIA host driver와 `nvidia-smi`가 먼저 있어야 하며, Dure는 driver를 설치·업그레이드·교체하지 않습니다. bootstrap은 아직 등록되지 않고 Agent가 비활성인 노드에서만 허용됩니다.

기본 명령은 읽기 전용 미리보기입니다.

```bash
sudo dure bootstrap
sudo dure bootstrap --json
```

계획을 검토한 뒤에만 Docker Engine, NVIDIA Container Toolkit과 Docker runtime 설정을 적용합니다.

```bash
sudo dure bootstrap --apply
sudo dure doctor
sudo dure join
```

Ubuntu 22.04·24.04와 `amd64`·`arm64`, Docker CLI와 Engine 20.10 이상만 지원합니다. 기존에 정상 동작하는 로컬 systemd Docker는 보존하며, 배포판 Docker 29가 `Platform.Name`을 비워도 공식 version 응답의 단일 `Engine` 구성요소가 서버 version·Linux OS·지원 architecture와 일치하면 Docker Engine으로 확인합니다. Docker CLI 없이 package·service·socket이 남아 미설치를 증명할 수 없거나 Docker 신규 설치가 필요한데 충돌 패키지가 있거나, 부분 또는 지원 버전과 다른 Toolkit 설치·원격 또는 rootless Docker·안전하지 않은 설정 경로가 있으면 자동 수정하지 않고 중단합니다. Toolkit 네 패키지는 `1.19.1-1`로 설치하고 `/etc/apt/preferences.d/dure-nvidia-container-toolkit`에서 같은 버전으로 고정합니다. NVIDIA runtime 등록에는 Docker 재시작이 필요합니다. 실행 중인 컨테이너가 있으면 미리보기에서 개수와 영향을 경고하며, `--apply`는 검토한 계획에 포함된 Docker 재시작 승인까지 의미합니다. 유지보수 시간을 확보한 뒤 적용합니다.

```bash
sudo dure bootstrap --apply
```

기존 `--allow-docker-restart`도 CLI 호환성을 위해 허용하지만 `--apply`와 동작이 같습니다. bootstrap은 모델 다운로드, OCI 이미지 pull, Docker 컨테이너 실행·중지, 배포 생성이나 Agent 등록을 수행하지 않습니다. credential이 있는 `/etc/dure/agent.json`이 생겼거나 Agent가 활성화된 뒤에는 적용을 거부합니다. `dure unjoin`이 credential을 제거하고 안전한 `install_id`만 남긴 비활성 노드는 다시 pre-join 경계로 인정합니다. bootstrap은 `dure join`·`dure unjoin`과 `/run/lock/dure-host-setup.lock`을 공유해 등록과 host 변경을 직렬화합니다. 사용자를 `docker` 그룹에 추가하지도 않으므로 준비 직후 검증은 `sudo dure doctor`로 실행합니다. Docker 설치가 host netfilter 정책에 미치는 영향과 방화벽 규칙은 운영자가 별도로 검토해야 합니다. CPU utility 노드는 이 절차를 건너뛰고 `sudo dure join`을 실행할 수 있습니다.

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

로컬 `dure plan`이 생성하는 기존 JSON 형식과 실행 경로는 호환성을 유지합니다. 중앙 추천을 수락해 만드는 2·3노드 계획은 별도의 폐쇄형 `VLLM_RAY_PP_V1` 계약을 사용하며, 모든 노드가 동일한 불변 계획을 받아야 합니다. 이 엄격한 계획은 서버가 발급한 노드 UUID, 노드마다 선택된 정상 GPU 한 장의 index·UUID, 고유한 RFC1918 IPv4 주소, `TP=1`, `PP=2` 또는 `PP=3`을 요구합니다. 노드에 정상 GPU가 더 있어도 계획에 선택되지 않은 GPU는 사용하지 않습니다. head는 rank 0이고 worker는 IPv4 문자열 오름차순으로 rank가 고정됩니다.

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

Dure는 NVIDIA host driver를 설치하거나 변경하지 않습니다. driver가 없거나 호환되지 않으면 provisioning을 중단하며, 운영자가 직접 조치해야 합니다. `dure bootstrap`도 중앙 task가 아닌 노드 로컬 root 명령이고, 기본은 읽기 전용입니다. 명시적 `--apply`에서도 고정된 공식 저장소·서명 key fingerprint·패키지 목록만 사용하며 임의 셸, Docker 인자, 환경 변수나 host 경로를 입력받지 않습니다.

CLI는 OCI 다이제스트로 고정되지 않은 이미지를 기본적으로 거부합니다. 중앙 배포는 항상 다이제스트로 고정한 이미지를 요구합니다. 모델 다운로드는 `--accept-model-download`, 이미지 내려받기는 `--pull`, 중지된 컨테이너 교체는 `--replace`가 필요합니다.

새 콘텐츠 주소 캐시의 production 기본 경로는 Dure 전용 고정 루트이고 task가 경로나 raw URL을 지정할 수 없게 설계되었습니다. 신뢰 HTTPS origin은 각 노드의 root 전용 Agent 설정에서만 읽고 중앙 task·DB·결과에는 URL, header나 token을 넣지 않습니다. 현재 전송기는 인증 header나 token을 지원하지 않으므로 별도 자격 증명 없이 접근 가능한 신뢰 origin이 필요합니다. 패키지는 `/var/lib/dure`를 root 소유 경계로 두고 중앙 서버용 쓰기 경로를 `/var/lib/dure/server`로 분리합니다. 준비기는 설정된 저장소·모델 루트의 symlink 조상, 소유자와 쓰기 권한을 검사합니다. 인벤토리와 벤치마크는 별도로 `/var/lib/dure/models` 루트, 캐시 후보, `config.json`과 marker를 검사하지만 상위 `/var/lib/dure` 전체 경계를 매번 재검사하지는 않습니다.

stage builder는 정확히 vLLM 0.9.0, V0 executor, `Qwen2ForCausalLM` AWQ, `TP=1`, `sharded_state`만 지원합니다. digest로 고정한 빌더 런타임에서만 실행하고 remote code, LoRA, MoE, 멀티모달과 임의 아키텍처를 거부합니다. `STAGE` 등록만으로 다운로드·배포 또는 기존 컨테이너 변경이 발생하지 않으며, 실제 GPU `GPU_EXPORT_LOAD/PASSED` 증적만 `DRAFT`를 `VALIDATED`로 승격할 수 있습니다. 검증하지 못한 `NOT_RUN`은 성공으로 취급하지 않습니다. `PP=2/3` 배포에는 모든 rank의 고정 캐시를 검증하고 실제 Ray/vLLM load와 최소 추론까지 통과한 별도 수용 증적이 필요합니다.

Ray GCS, 대시보드, 워커 포트는 신뢰된 LAN 또는 WireGuard 같은 사설 오버레이에만 노출해야 합니다. 공개 인터넷에 노출해서는 안 됩니다.

`VLLM_RAY_PP_V1`은 정확히 vLLM 0.9.0 V0 executor와 Ray를 사용합니다. GCS는 `6379`, Ray worker 범위는 `20000-21000`으로 고정하고 vLLM API는 head의 `127.0.0.1:8000`에만 바인딩합니다. 각 컨테이너는 배포·세대·노드 외에 backend, pipeline rank, runtime rank, component 레이블까지 정확히 일치해야 시작·검증·중지·재시작 대상이 됩니다. 추천기는 `FULL_SNAPSHOT`과 검증된 exact `STAGE`를 독립 후보로 평가하고 같은 품질이면 `STAGE`를 우선하지만, 수락한 뒤 둘 사이를 묵시적으로 바꾸지는 않습니다.

준비 상태의 `pipeline-rank-contract` 검사는 계획에 고정된 vLLM 0.9.0의 정렬 규칙, 정확한 Ray 노드 집합, 노드별 GPU 한 장과 `dure_node_<uuidhex>` custom resource를 확인하고, API 시작 뒤에는 vLLM worker actor 토폴로지도 요구합니다. 컨테이너의 `dure.runtime-contract` SHA-256은 이미지·mount·GPU·network·entrypoint·고정 환경·명령 drift를 시작·재사용·readiness에서 차단합니다. Ray가 vLLM 내부 pipeline rank 숫자를 공개 필드로 직접 보고하는 것은 아니므로, 이 결과는 **소스에 고정된 rank 계약의 간접 증적**입니다. 실제 2·3노드 GPU 환경에서는 별도의 수용 검사로 분산 load와 최소 추론까지 확인해야 합니다.

실제 GPU 수용 검사는 `scripts/acceptance-vllm-ray-pp.py`를 사용합니다. 기본 실행, opt-in 누락, `/etc/dure/acceptance-vllm-ray-pp-v1.json` 또는 고정 런타임·모델 전제 조건 누락은 호스트 변경 없이 `NOT_RUN`과 종료 코드 `77`을 반환합니다. 검사에는 명령행 인자를 전달하지 않으며 `DURE_RUN_VLLM_RAY_PP_ACCEPTANCE=1`만 허용합니다. harness는 Ray custom resource로 Dure UUID와 주소를 다시 결합하지만 설정의 runtime image digest는 선언값이므로 신뢰된 digest 고정 wrapper와 운영 기록 안에서 실제 이미지를 대조해야 합니다. 실제 분산 load가 시작된 뒤의 오류는 `FAILED`와 0이 아닌 종료 코드로 남겨야 하며 `NOT_RUN`이나 성공으로 바꾸면 안 됩니다. 이 harness는 controller의 노드별 `pipeline-rank-contract` 증적을 대체하지 않습니다.

rank별 `STAGE` 분산 load는 `scripts/acceptance-vllm-stage-ray-pp.py`로 별도 검증합니다. 이 검사도 기본값은 `NOT_RUN(77)`이며, 신뢰된 2·3노드 환경에서만 각 노드의 서로 다른 stage 캐시가 같은 컨테이너 경로 `/models/model`에 결합되는지, `--load-format sharded_state`로 실제 load되는지, 최소 추론이 성공하는지를 확인합니다.

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

- Fleet 추천은 PRIMARY·SUPPLEMENTARY exact 증적을 여러 독립 배포 후보로 조합하고 100개 노드 이상도 고정 노드 수 분기 없이 계산합니다. `dure admin fleet recommend/show/accept`와 관리자 API는 전체 평가를 콘텐츠 주소 불변 스냅샷으로 멱등 저장·조회하고, 수락 시 모든 generation 1 배포와 exact 노드·GPU 예약을 한 트랜잭션으로 생성합니다. 수락 결과는 `CREATED` 중앙 기록이며 모델 준비·Agent task·컨테이너 적용은 아직 일어나지 않습니다. 제품 노드 수 상한 대신 후보 512개와 탐색 상태 250,000개의 명시적 안전 한도를 사용하며, 탐색 예산이 소진되면 결정론적 유효 해와 `search_complete=false`를 반환합니다.
- 자동 배치 프로필 qualification은 정책·suite·작업 부하와 8단계 증적, exact rank·노드·GPU UUID 결합을 중앙에서 검증하는 계약입니다. preview, run 생성, 증적 등록과 운영자 활성화 자체는 Agent 작업·다운로드·컨테이너 변경을 만들지 않습니다. 실제 다중 노드 시험은 신뢰된 외부 executor가 수행해 결과를 제출해야 하며, Dure Agent가 여러 노드의 네트워크·NCCL·Ray/vLLM 시험을 자동 조정하는 실행기는 아직 없습니다. 단일·다중 노드 AUTO 추천은 이 exact 결합의 24시간 이내 통과 증적만 사용합니다.
- 현재 `--model auto`는 버전이 있는 Qwen2.5 AWQ 정적 카탈로그와 결정론적 선택기를 사용합니다. 중앙 추천의 다중 노드 후보는 정확히 정렬된 노드 UUID 조합·릴리스·배치 프로필·런타임·현재 인벤토리 지문에 결합된 24시간 이내의 최신 `PASSED` 네트워크·NCCL 증적이 있어야 합니다. 로컬 `dure plan --model auto`는 기존 3×24GB 계획 호환성을 위해 이 중앙 증적 검사만 예외로 두고 계획에 검증 경고를 남깁니다. 중앙 모델 레지스트리, 벤치마크 증적 기반 `ACTIVE` 승격 게이트, 추천 스냅샷과 명시적 수락, 세대별 적용·검증 상태와 명시적 롤백도 제공합니다.
- 롤백은 최신 세대의 검증된 직전 세대로만 가능하며, 전체 배정 노드와 실제 실행 토폴로지·승인·온라인 상태, backend별 최소 Agent 버전을 요구합니다. 엄격한 `VLLM_RAY_PP_V1`에서 동일해야 하는 실행 토폴로지는 노드·GPU·role·rank·expected runtime rank·runtime address와 backend·vLLM·TP/PP·Ray·network 결합입니다. 각 세대의 모델·revision·layer 범위·매니페스트·variant와 `FULL_SNAPSHOT`/`STAGE` cache identity는 독립적으로 유효하고 대상의 exact 준비 게이트를 통과하면 달라도 됩니다. legacy 계획은 layer 범위도 토폴로지로 계속 비교합니다. legacy는 Agent 0.3.12 이상, `VLLM_RAY_PP_V1`은 0.3.18 이상이고 `STAGE` 대상을 다시 시작할 때는 0.3.19 이상입니다. 추천으로 만든 대상에는 exact `READY`, 현재 모델 시도와 최신 이미지 digest 준비 증적이 필요합니다. 롤백 중에는 모델 다운로드나 이미지 pull을 허용하지 않고, 소스 중지 뒤 대상 시작 직전에 이 증적을 다시 검사합니다. 같은 GPU에서 이전 세대 컨테이너를 다시 만드는 절차이므로 서비스 중단이 생길 수 있으며 블루·그린 전환이 아닙니다.
- 중앙 제어면은 승인된 단일 노드에서 고정된 네 가지 작업 부하를 `BENCHMARK` 작업으로 실행하고 결과를 증적으로 수집할 수 있습니다. Agent 0.3.25 이상은 명시적 승인으로 신뢰 origin의 exact 모델과 digest 이미지를 먼저 준비할 수 있고, `dure admin activate`는 등록부터 API 검증까지 연결합니다. Hugging Face hub snapshot 직접 실행, 다중 노드 네트워크·NCCL 시험, 전체 동시성 매트릭스와 24시간 복구 검증은 아직 지원하지 않습니다.
- 중앙 아티팩트 매니페스트 등록·조회, 콘텐츠 주소 기반 `FULL_SNAPSHOT`·rank별 `STAGE` 준비기와 명시적 중앙 준비 흐름은 구현되었습니다. 준비 preview는 DB 계획만 만들고, 별도 적용 뒤 각 노드에서 `PREPARE_MODEL → PREPARE_IMAGE`를 순서대로 수행합니다. 모델은 전체 청크·파일 해시와 marker를 검증하고 이미지는 다이제스트 고정 참조를 pull한 뒤 다시 inspect합니다. 추천이나 수락 자체는 다운로드를 시작하지 않으며, 현재 성공 시도에 결합된 exact `READY` 캐시와 최신 이미지 digest 증적이 없는 추천 세대는 apply·start·restart·verify할 수 없습니다.
- 오염된 CAS·staging·final은 덮어쓰거나 자동 삭제하지 않고 실패합니다. `artifact-cache quarantine`은 preview에서 참조만 검사하고, `--apply` 뒤 exact final 캐시 디렉터리 하나만 `.dure-quarantine` 아래로 원자 이동해 보존합니다. 공유 CAS 청크, staging, 다른 캐시는 옮기지 않으며 자동 퇴출·삭제는 제공하지 않습니다.
- 신뢰된 오프라인 빌더와 중앙 레지스트리는 제한된 `STAGE` variant를 생성·등록하고 실제 GPU export/load 증적으로 검증할 수 있습니다. vLLM의 sharded-state 파일명이 TP rank를 사용하므로 `TP=1`, `PP>1`에서는 각 worker 출력을 반드시 `stages/<pp-rank>`로 격리합니다. 공용 디렉터리에 쓰면 모든 PP worker의 rank가 0이 되어 파일 충돌이나 덮어쓰기가 발생할 수 있습니다.
- 중앙 추천은 같은 품질에서 exact `VALIDATED` `STAGE`를 먼저 평가하고 별도의 `FULL_SNAPSHOT` 후보도 평가합니다. 선택한 캐시 종류·variant·rank·loader·증적은 수락한 세대에 고정되며 `--stage-variant`는 그 선택과 같은 digest인지 확인하는 선택적 assertion일 뿐 배달 방식을 바꾸지 않습니다. `STAGE` 준비가 실패해도 수락된 세대 안에서 `FULL_SNAPSHOT`으로 자동 fallback하거나 실행 중인 이전 배포를 자동 중지·교체·롤백하지 않습니다.
- 일반 probe·heartbeat는 수 GiB 파일을 매번 재해시하지 않고 최대 256개의 폐쇄형 marker metadata를 보고합니다. 중앙 상태를 악화시키는 조정은 `scan_complete=true`인 완전한 조사만 수행하며, legacy·불완전 조사는 승격도 강등도 하지 않습니다. `PRESENT` 관측 역시 `READY`를 만들거나 손상 상태를 치유하지 않습니다. 현재 `PREPARE_MODEL` 성공과 시작 직전 전체 runtime 재해시가 권위 있는 무결성 게이트입니다.
- 중앙 캐시는 `READY`·`STALE`·`MISSING`·`CORRUPT`·`QUARANTINED`로 추적합니다. stage variant 철회는 `STALE`, 완전한 probe의 부재는 `MISSING`, unsafe·무결성 이상과 중앙 배포 검증 실패는 `CORRUPT`로 닫습니다. 현재 준비 성공만 다시 `READY`를 만들 수 있습니다.
- 명시적 `deployment create` 긴급 복구 경로는 여전히 프로필 JSON 파일이 필요합니다. 중앙에 저장된 프로필만으로 계획을 만드는 경로는 유효한 추천을 명시적으로 수락할 때만 제공됩니다.
- 적용 모드는 Docker만 지원하며, 물리 노드당 GPU 한 장의 index·UUID만 배정합니다. 같은 노드의 나머지 GPU는 이 계획에서 사용하지 않습니다.
- 노드 간 P2P 청크 전송, 공유 파일시스템을 이용한 단일 모델 저장소, erasure coding은 지원하지 않습니다. 지원 목록 밖의 모델 family를 자동 추정하지 않고, 캐시 자동 삭제·퇴출이나 추천 직후 자동 준비·배포도 수행하지 않습니다.
- Dure가 직접 실행하는 다중 노드 네트워크 벤치마크와 NCCL 집합 연산 조사는 아직 구현되지 않았습니다.
- 정규화된 모델·stage 아티팩트 매니페스트 등록은 제공하지만 SHA-256과 exporter 다이제스트는 게시자 서명이나 신뢰 provenance가 아닙니다. 게시자·이미지 서명 검증은 계획 단계입니다.
- 게이트웨이, 최종 사용자 인증, 크레딧 원장, WireGuard 자동화, 공개 노드 샌드박스는 아직 제공하지 않습니다.

## 중앙 노드 관리

Dure에는 선택형 FastAPI/PostgreSQL 제어면과 외부 방향 폴링 노드 에이전트가 있습니다. `dure-server`는 현재 작업 디렉터리의 `dure/.env` 또는 `.env`에서 `DURE_DATABASE_URL`과 `DURE_ADMIN_TOKEN`을 한 쌍으로 읽으며 `--env-file`로 안전한 파일을 명시할 수도 있습니다. migration과 서버 시작 전에 실제 DB 연결을 검사하므로 잘못된 PostgreSQL credential로 API를 열지 않습니다. migration을 적용한 뒤 TLS 역방향 프록시 뒤에서 서버를 시작합니다.

```bash
cd ~/workspace/dure
chmod 600 dure/.env
dure-server --env-file dure/.env --migrate
dure-server --env-file dure/.env --host 0.0.0.0 --port 8081
```

관리자 CLI는 명령행 credential을 매번 적지 않아도 현재 작업 디렉터리의 `dure/.env` 또는 `.env`에서 `DURE_SERVER`와 `DURE_ADMIN_TOKEN`을 한 쌍으로 읽습니다. 파일은 현재 사용자 소유의 일반 파일이고 group·other 접근 권한이 없어야 합니다. 파일을 셸로 실행하거나 다른 환경 변수를 가져오지 않습니다.

```bash
cd ~/workspace/dure
chmod 600 dure/.env
dure admin nodes

# 다른 안전한 위치를 명시할 수도 있습니다.
dure admin --env-file /secure/path/dure-admin.env nodes
```

두 값은 같은 파일에 모두 있어야 하며 `--server`·`--token`이 파일보다 우선합니다. 파일이 없으면 기존 `DURE_SERVER`·`DURE_ADMIN_TOKEN` 환경변수와 패키지의 서버 설정을 사용합니다. `.env`는 Git ignore 대상이지만 token을 commit하거나 지원 요청에 첨부해서는 안 됩니다.

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

GPU 노드를 반납할 때는 노드에서 직접 등록을 해제하거나 중앙에서 안전한 해제 작업을 보냅니다. 직접 해제는 해당 노드 UUID·배포 세대 label과 정확히 일치하는 Dure 컨테이너만 중지하고 Agent를 비활성화한 뒤 credential을 폐기합니다. 중앙 작업은 승인된 GPU 노드만 대상으로 하며 완료될 때까지 task 상태를 확인해야 합니다.

```bash
# 작업 노드에서 직접
sudo dure unjoin

# 중앙 운영자 컴퓨터에서 한 노드 또는 모든 승인 GPU 노드
dure admin unjoin --node <node-id>
dure admin unjoin --all
dure admin tasks --watch
```

해제된 로컬 설정에는 재등록 시 같은 설치 identity를 사용할 `install_id`만 남고 credential은 제거됩니다. 다시 참여하려면 host 준비 상태를 확인한 뒤 `sudo dure join`을 실행합니다.

중앙 작업은 `PROBE`, `BENCHMARK`, `PREPARE_MODEL`, `PREPARE_IMAGE`, `QUARANTINE_ARTIFACT_CACHE`, `UNJOIN_NODE`, `VERIFY`, `APPLY_DEPLOYMENT`, `START_DEPLOYMENT`, `STOP_DEPLOYMENT`, `RESTART_DEPLOYMENT`로 고정됩니다. `BENCHMARK`, 두 준비 작업, 캐시 격리와 노드 등록 해제는 각각의 전용 관리 흐름으로만 만들 수 있고 임의 remote shell 명령으로 만들 수 없습니다.

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

# 추천이 STAGE를 선택한 경우 이 옵션은 선택된 digest가 맞는지 확인하는 assertion입니다.
# 생략해도 수락된 세대의 STAGE 선택은 유지되며 다른 variant나 FULL로 바뀌지 않습니다.
dure admin deployment prepare <deployment-id> \
  --request-id <stage-request-uuid> --stage-variant sha256:<64-hex>
dure admin deployment prepare <deployment-id> \
  --request-id <stage-request-uuid> --stage-variant sha256:<64-hex> --apply

# 중앙 캐시 상태와 격리 가능 참조를 읽기 전용으로 확인합니다.
dure admin artifact-cache list
dure admin artifact-cache show <cache-id>
dure admin artifact-cache verify <cache-id>

# 첫 명령은 task 0개의 preview이고, --apply만 exact 캐시 하나의 격리 task를 만듭니다.
dure admin artifact-cache quarantine <cache-id>
dure admin artifact-cache quarantine <cache-id> --apply

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

`recommend`는 추천과 정규화된 인벤토리 스냅샷 한 건만 멱등하게 저장합니다. 같은 품질에서는 exact `VALIDATED` `STAGE`를 먼저 평가하고 `FULL_SNAPSHOT`도 독립 후보로 평가하며, 가능한 후보가 없으면 더 작은 `ACTIVE` 모델이나 다른 노드 조합의 탈락 사유를 남깁니다. `accept`는 저장 당시와 현재의 추천 ID·카탈로그·정책·인벤토리 지문·선택 결과가 모두 같을 때만 `CREATED` 배포 세대 한 건을 만들며, 같은 요청의 재시도는 기존 세대를 반환합니다. 이전 세대를 지정하면 해당 계보의 최신 세대에만 연결할 수 있습니다. 수락 뒤 준비 preview는 task를 0개 만들고, 같은 요청에 `--apply`를 명시해야 모델 준비를 큐잉합니다. 각 노드의 모델 준비가 성공한 뒤에만 그 노드의 이미지 준비를 시작하며, 실패 뒤 같은 적용 요청은 성공한 단계를 반복하지 않고 현재 실패 단계만 새 시도 번호로 재시도합니다. 과거 시도의 늦은 완료는 현재 시도와 맞지 않으면 상태를 바꾸지 못합니다.

`deployment preparation` 조회는 전체와 노드별 `expected_bytes`·`verified_bytes`, `download_expected_bytes`·`downloaded_bytes`, 현재 모델·이미지 단계와 중앙 시도·재시도 횟수를 `progress`로 반환합니다. `verified_bytes`는 현재 모델 시도의 전체 매니페스트 검증 성공만 집계합니다. `downloaded_bytes`는 중복을 제거한 청크별 로컬 준비 위치의 단조 증가 high-water로, 부분 파일 쓰기뿐 아니라 검증된 CAS·staging·final 재사용과 성공 시 정규화를 포함합니다. 따라서 실제 네트워크 전송량·속도·현재 유효 바이트가 아니며 100%여도 `READY`를 뜻하지 않습니다. 정확한 의미는 `download_bytes_source`로 구분하며 이미지 pull의 바이트 진행률은 제공하지 않습니다.

중앙 추천이 선택한 다중 노드 세대는 `VLLM_RAY_PP_V1`로 고정됩니다. 저장과 작업 생성 시 전체 배정 노드 집합을 요구하고, hostname을 UUID로 추측해 보정하지 않으며, `FULL_SNAPSHOT` 실행은 모든 대상 Agent 0.3.18 이상, `STAGE` 준비·실행은 0.3.19 이상이어야 합니다. 추천기는 GPU 건강·VRAM·compute capability, driver 관측값, 런타임 GPU 아키텍처, Docker/NVIDIA runtime, 디스크와 네트워크 증적을 검사합니다. 사전 검사가 실패하면 컨테이너를 변경하지 않습니다. 적용 중 일부 노드가 실패하면 다음 단계로 진행하지 않으며, 전환이 이미 시작됐다면 이전 세대가 계속 실행된다고 가정하지 않고 상태를 확인해 명시적으로 복구합니다. 실패 노드는 `dure admin credential revoke <node-id>`로 작업 수신에서 격리하고, 새 probe·recommend로 다른 적격 노드나 더 작은 모델의 독립 `FULL_SNAPSHOT` 후보를 선택할 수 있습니다. 이미 수락한 세대 안에서 모델·캐시 형식을 자동 교체하지는 않습니다. Dure는 이 과정에서도 NVIDIA host driver를 자동 설치·변경하지 않습니다.

추천 생성과 수락은 `PROBE`나 배포 작업을 만들지 않고, 모델 다운로드, 이미지 내려받기, Docker 실행, 기존 컨테이너 중지를 유발하지 않습니다. 준비 적용은 모델 캐시와 로컬 이미지까지만 변경하고 컨테이너를 실행·중지하지 않습니다. 모든 노드에서 exact identity가 `READY`이고 현재 모델 준비 시도와 최신 OCI digest 이미지 시도가 모두 성공한 뒤에만 별도의 `dure admin apply`로 추천 세대를 실행할 수 있습니다. 이 게이트는 start·restart·verify에도 다시 적용됩니다. 중앙 `VERIFY`가 실패하면 해당 노드의 exact 캐시를 `CORRUPT`로 투영해 후속 소비를 차단합니다. 전체 배정 노드의 `VERIFY`가 성공하고 legacy는 Agent 0.3.12 이상, `VLLM_RAY_PP_V1`은 0.3.18 이상이며 엄격한 rank·API 검증까지 통과할 때만 해당 세대의 `verified_at`이 롤백 증거로 기록됩니다. 추천 세대의 `FULL_SNAPSHOT` 준비는 0.3.16 이상, `STAGE` 준비는 0.3.19 이상 Agent를 요구하고 캐시 격리는 Agent 0.3.20 이상을 요구합니다.

롤백 준비도 작업을 만들지 않습니다. `--apply`를 명시한 뒤에만 `STOP_SOURCE → START_TARGET → VERIFY_TARGET` 순서로 진행하며, `--serve`를 선택하면 Ray head에서 `START_API → VERIFY_API`를 이어서 실행합니다. 한 단계의 모든 노드가 성공해야 다음 단계로 넘어가고, 실패 뒤 같은 명령에 `--apply`를 다시 지정하면 현재 단계의 실패 노드만 새 시도 번호로 재시도합니다. 추천으로 만든 롤백 대상에는 과거에 성공한 정확한 준비 증적과 현재 `READY` 상태가 필요합니다. 소스를 모두 중지한 직후, `START_TARGET` task를 만들기 전에 exact 캐시와 최신 이미지 준비 증적을 다시 검사합니다. 이때 캐시가 사라지거나 손상됐으면 `ROLLBACK_TARGET_CACHE_NOT_READY`로 닫고 시작 task를 0개 유지합니다. 롤백은 새 준비 task, 모델 다운로드와 이미지 pull을 만들지 않으므로 원인을 별도 준비 절차로 복구한 뒤 명시적으로 재시도해야 하며, 이미 소스가 중지된 시점이면 서비스 중단이 계속될 수 있습니다.

일반 apply의 `--serve`도 전체 배정 노드가 Ray 준비를 마친 뒤 head 한 대에서만 API를 시작·검증합니다. 필수 검사가 누락되거나 중복된 verify 결과는 롤백 증거가 되지 않습니다. 성공한 롤백의 소스 세대는 다시 계보를 이어 갈 수 없으므로 이후 추천 수락은 이전 세대를 생략해 새 계보로 시작합니다. 만료된 operation task는 관리자 취소 API로 `TASK_LEASE_EXPIRED` 처리한 뒤 같은 롤백 요청을 재시도할 수 있습니다.

중앙 추천은 구조화된 네트워크·NCCL 증적을 정확한 정렬 노드 UUID 조합의 적격성 입력으로 조회합니다. 같은 조합의 최신 증적이 `PASSED`가 아니거나 24시간보다 오래됐거나 미래 시각이거나, 릴리스·배치 프로필·아티팩트·런타임·현재 인벤토리 지문이 다르면 후보를 실패 안전 방식으로 거부합니다. 증적보다 뒤에 시작된 실패 또는 진행 중 벤치마크 실행도 과거 통과 결과로 우회할 수 없습니다. 배치 프로필의 대역폭·RTT·패킷 손실·NCCL 기준을 모두 통과해야 하며, 추천 생성과 수락은 모델 다운로드·이미지 내려받기·작업 생성·호스트 변경을 수행하지 않습니다.

다중 노드 네트워크·NCCL 시험을 자동으로 실행하는 기능은 아직 구현되지 않았습니다. 분산 `STAGE` 준비·실행 경로와 opt-in 실제 GPU 수용 harness는 제공하지만 이 저장소의 기본 테스트는 실제 GPU 검사를 실행하지 않습니다. 현재 세대 검증과 롤백도 전체 작업 부하 매트릭스, 네트워크·NCCL 시험 또는 24시간 복구 검증을 대신하지 않습니다.

## 테스트

```bash
python3 -m unittest discover -v
```

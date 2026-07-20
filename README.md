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
- 저장된 GPU 인벤토리와 `ACTIVE` 릴리스를 평가하는 결정론적 읽기 전용 추천
- 구조화된 벤치마크 증적 저장과 모든 배치 프로필을 검사하는 `ACTIVE` 승격 게이트
- Codex를 이용한 읽기 전용 용량 진단
- 기본 모의 실행과 명시적 변경 플래그

## 문서

- [문서 색인](docs/README.md)
- [아키텍처](docs/architecture.md)
- [운영 절차](docs/operations.md)
- [보안 모델](docs/security.md)
- [모델 선택 정책](docs/model-selection.md) — 부분 구현
- [벤치마크 및 모델 자격 검증](docs/benchmarking.md) — 부분 구현
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

생성된 계획은 Ray 순위와 파이프라인 단계를 각 노드에 하나씩 할당합니다. 모든 노드는 동일한 계획 파일을 받아야 합니다.

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

Ray GCS, 대시보드, 워커 포트는 신뢰된 LAN 또는 WireGuard 같은 사설 오버레이에만 노출해야 합니다. 공개 인터넷에 노출해서는 안 됩니다.

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

- 현재 `--model auto`는 버전이 있는 Qwen2.5 AWQ 정적 카탈로그와 결정론적 선택기를 사용합니다. 중앙 모델 레지스트리, 벤치마크 증적 기반 `ACTIVE` 승격 게이트와 저장 인벤토리 기반 읽기 전용 추천도 제공하지만, 추천 승인과 세대별 전환은 [모델 선택 정책](docs/model-selection.md)에서 계속 구현 중입니다.
- 중앙 제어면은 관리자가 등록한 구조화된 벤치마크 증적을 판정할 뿐, GPU 노드에서 벤치마크를 실행하지 않습니다. 안전한 Agent 벤치마크 작업, 전체 작업 부하 매트릭스와 다중 노드 24시간 복구 검증은 후속 범위입니다.
- 중앙에서 등록 노드 프로필만으로 계획을 생성하는 기능은 아직 구현되지 않았고, 프로필 JSON 파일이 필요합니다.
- 적용 모드는 Docker만 지원하며, 물리 노드당 GPU 한 장만 배정합니다.
- 네트워크 벤치마크와 NCCL 집합 연산 조사는 아직 구현되지 않았습니다.
- 모델 아티팩트의 서명된 매니페스트와 이미지 서명 검증은 계획 단계입니다.
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

중앙 작업은 `PROBE`, `VERIFY`, `APPLY_DEPLOYMENT`, `START_DEPLOYMENT`, `STOP_DEPLOYMENT`, `RESTART_DEPLOYMENT`로 고정됩니다. 임의 remote shell 명령은 받지 않습니다.

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
```

추천은 배포나 작업을 저장하지 않고 모델 다운로드, 이미지 내려받기, Docker 실행을 유발하지 않습니다. 다중 노드 후보는 네트워크·NCCL 증적 기능이 추가되기 전까지 실패 안전 방식으로 거부됩니다.

## 테스트

```bash
python3 -m unittest discover -v
```

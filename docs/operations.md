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

## 모델 레지스트리 운영

관리자 API는 모델 아티팩트, 런타임 릴리스, 모델 릴리스와 배치 프로필을 별도로 관리합니다.

- `POST /v1/admin/model-artifacts`: 변경 불가능한 모델 리비전과 매니페스트 다이제스트 등록
- `POST /v1/admin/runtime-releases`: OCI 다이제스트로 고정한 vLLM 런타임 등록
- `POST /v1/admin/model-releases`: 아티팩트와 런타임 조합 생성
- `POST /v1/admin/model-releases/{id}/placements`: `DRAFT` 릴리스에 형식화된 배치·SLO 정책 추가
- `POST /v1/admin/model-releases/{id}/transition`: 허용된 상태 전이 수행
- `GET /v1/admin/model-releases`: 릴리스와 배치 프로필 조회
- `POST /v1/admin/deployment-recommendations`: 저장 인벤토리와 `ACTIVE` 릴리스의 읽기 전용 평가

모든 경로는 관리자 전달자 인증을 요구합니다. 모델 리비전, 매니페스트, 런타임 이미지가 고정되지 않으면 등록할 수 없고, 허용 목록 밖의 Docker 인자·환경 변수·마운트·호스트 경로는 요청 단계에서 거부됩니다. 레지스트리 등록이나 상태 전이만으로 에이전트 작업 또는 호스트 변경이 발생하지 않습니다.

## 벤치마크 증적 등록과 승격

모델 릴리스의 배치 프로필을 모두 추가한 뒤 `VALIDATED`로 전환합니다. 이후 신뢰된 외부 도구로 측정한 구조화된 결과를 등록하고 `ACTIVE` 승격을 요청할 수 있습니다.

- `POST /v1/admin/benchmark-context`: 현재 프로필 지문과 고정 릴리스 식별자 준비
- `POST /v1/admin/benchmark-evidence`: 증적 등록
- `GET /v1/admin/benchmark-evidence?release_id=<id>`: 릴리스의 최근 증적 조회
- `POST /v1/admin/model-releases/{id}/promote`: 모든 배치 프로필의 최신 증적을 검사하고 승격

먼저 context API에 릴리스·배치 프로필과 측정 노드 UUID를 보내 현재 인벤토리 지문, 아티팩트 리비전·매니페스트와 OCI 런타임 이미지를 받습니다. 증적 등록 요청에는 이 값과 고정 suite·정책 버전, Dure 커밋, 작업 부하 크기와 수치 지표가 필요합니다. 서버는 등록 시 현재 저장된 노드 프로필 지문과 레지스트리 식별자를 다시 계산·비교하고 불일치를 거부합니다.

SLO 미달 결과는 등록 오류가 아니라 `FAILED` 증적으로 저장됩니다. 운영자는 실패 코드를 검토하고 새 측정을 등록해야 합니다. 배치 프로필별 최신 증적만 승격 판단에 사용하므로 최신 실패 뒤에 남아 있는 과거 `PASSED` 결과로 우회할 수 없습니다. 모든 배치 프로필이 통과하면 승격에 사용한 증적 집합이 릴리스에 고정됩니다.

현재 전용 벤치마크 CLI와 Agent 실행 작업은 제공하지 않습니다. context·증적 API를 연동한 신뢰된 내부 도구로만 결과를 제출해야 합니다. 중앙 제어면은 제출된 요약을 판정할 뿐 GPU 명령, 네트워크 시험 또는 NCCL을 실행하지 않습니다. 전체 작업 부하 매트릭스와 다중 노드 24시간·복구 수용 검사는 후속 범위입니다.

증적 본문에는 프롬프트, 자격 증명, 모델 접근 토큰, 로그, 명령, Docker 인자, 환경 변수, 마운트, 호스트 경로나 자유 형식 metadata를 넣을 수 없습니다. 원본 결과와 큰 로그는 접근 제어된 별도 저장소에 보관합니다.

증적 등록과 승격은 deployment나 task를 만들지 않고 모델 다운로드, 이미지 내려받기, Docker 실행 또는 기존 컨테이너 중지를 수행하지 않습니다. 승격 후에도 실제 배포에는 별도의 명시적 생성과 apply가 필요합니다.

0003 마이그레이션은 과거 버전에서 증적 없이 `ACTIVE`가 된 릴리스를 `VALIDATED`로 되돌립니다. 업그레이드 뒤 해당 릴리스의 모든 배치 프로필에 현재 증적을 등록하고 명시적으로 다시 승격해야 합니다. 이 상태 보정은 레지스트리 행만 바꾸며 실행 중인 컨테이너에는 손대지 않습니다.

## 읽기 전용 모델 추천과 계획된 단계적 전환

정책 기반 `recommend`와 벤치마크 승격 게이트는 현재 구현되어 있으며 추천 승인과 세대별 단계적 전환은 아직 구현되지 않았습니다. 추천은 데이터베이스에 저장된 노드 프로필과 `ACTIVE` 모델 릴리스만 조회합니다.

```bash
# 필요할 때만 명시적으로 프로필 조사 작업을 요청하고 완료를 확인합니다.
dure admin probe --nodes <node-id> <node-id>
dure admin tasks

# 저장된 승인·온라인 노드를 모두 평가합니다.
dure admin deployment recommend --all-online

# 지정한 중앙 노드 UUID를 평가하고 대기·오프라인·오래된 상태의 탈락 사유도 확인합니다.
dure admin deployment recommend --nodes <node-id> <node-id> --objective quality-first
```

`recommend` 자체는 `PROBE` 작업을 만들거나 인벤토리를 갱신하지 않습니다. 응답에는 콘텐츠 해시 추천 ID, 카탈로그·정책 버전, 인벤토리 지문, 후보별 중앙 릴리스·배치 ID와 탈락 사유가 포함됩니다. 결과는 아직 영속 저장하지 않으므로 운영 기록이 필요하면 응답 JSON을 별도로 보관해야 합니다. 네트워크·NCCL 증적을 중앙 추천의 배치 가능성 판단에 연결하는 기능이 추가되기 전까지 다중 노드 후보는 실패 안전 방식으로 거부됩니다.

현재와 후속 구현의 운영 순서는 다음과 같습니다.

1. 최신 `PROBE`로 인벤토리를 갱신합니다.
2. 읽기 전용 추천의 후보, 탈락 사유, 모델 리비전, 이미지 다이제스트, 네트워크 사전 조건을 검토합니다.
3. **계획됨:** 운영자가 후보를 승인해 배포 세대를 만듭니다.
4. **계획됨:** 명시적 apply와 verify를 거쳐서만 활성화합니다.
5. **계획됨:** 실패 시 이전에 검증된 세대로 복구합니다.

자동 추천은 자동 다운로드, 이미지 내려받기, 적용, 기존 컨테이너 중지를 의미하지 않습니다. 동일 GPU를 공유하는 파이프라인은 블루/그린 방식이 불가능할 수 있으므로, 실제 무중단 여부를 과장하지 않고 재생성과 복구 절차를 문서화해야 합니다.

## 업그레이드와 복구

controller에서는 PostgreSQL을 백업하고, 패키지를 업그레이드하고, migration 뒤 server를 재시작합니다. Agent는 작은 batch로 업그레이드합니다.

```bash
sudo apt update
sudo apt install --only-upgrade dure
sudo systemctl daemon-reload
sudo systemctl restart dure-agent
```

Agent는 재시작 뒤에도 credential과 완료 task journal을 재사용합니다. 만료된 task lease는 재전달될 수 있으므로 handler는 멱등적이어야 합니다. 활성 deployment 중에는 `/var/lib/dure/agent-tasks.json`을 삭제하지 않습니다.

```bash
systemctl status dure-server dure-agent
journalctl -u dure-server -u dure-agent --since -1h
dure admin nodes --json
dure admin tasks
```

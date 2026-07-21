# 중앙 제어면 운영 절차

## 중앙 서버

중앙 호스트에는 중앙 제어면 추가 의존성을 설치합니다. APT 패키지는 이동 가능한 노드 CLI/에이전트용이며 서버 의존성이나 서버 systemd unit을 설치하지 않습니다.

```bash
python3 -m pip install -e '.[server]'
```

systemd 운영 secret은 저장소 밖에 둡니다. 저장소에 제공된 서버 unit template은 이 파일을 프로세스 환경으로 읽습니다.

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

개발 또는 수동 운영에서 저장소 최상위에서 실행하면 `dure-server`는 먼저 `dure/.env`, 그다음 `.env`를 확인합니다. Dure 프로젝트 디렉터리 안에서 실행하면 해당 디렉터리의 `.env`를 확인합니다. 다른 위치의 파일은 `--env-file`로 명시합니다. 선택된 파일에는 `DURE_DATABASE_URL`과 `DURE_ADMIN_TOKEN`이 모두 있어야 하며, 둘 중 하나만 있으면 서버는 시작하지 않습니다.

```dotenv
DURE_SERVER=http://127.0.0.1:8081
DURE_DATABASE_URL=postgresql+psycopg://dure:<database-password>@127.0.0.1:5432/dure
DURE_ADMIN_TOKEN=<random-secret>
```

```bash
cd ~/workspace/dure
chmod 600 dure/.env
dure-server --env-file dure/.env --migrate
dure-server --env-file dure/.env --host 0.0.0.0 --port 8081
```

`DURE_SERVER`는 같은 파일을 관리자 CLI와 공유하기 위한 클라이언트 주소이며 서버 자체는 사용하지 않습니다. 서버의 DB URL 우선순위는 `--database-url`, 선택된 dotenv, 프로세스의 `DURE_DATABASE_URL`, 내장 기본값 순서입니다. 관리자 token은 선택된 dotenv가 프로세스 환경보다 우선하며, migration 전용 실행이 아닌데 token이 없으면 서버는 시작하지 않습니다. 서버는 migration과 listen 전에 `SELECT 1` 연결 검사를 수행합니다. DB 비밀번호가 맞지 않으면 URL이나 token을 출력하지 않고 시작을 중단하므로 PostgreSQL role 암호와 `DURE_DATABASE_URL`을 먼저 일치시켜야 합니다.

제공된 개발/LAN service template은 `0.0.0.0:8081`에서 listen합니다. 운영 환경에서는 application을 loopback에 bind하고 TLS reverse proxy를 통해 HTTPS 443만 노출해야 합니다. PostgreSQL과 Ray 포트는 공개하지 않습니다.

```bash
curl -fsS http://127.0.0.1:8081/health
```

## 관리자 CLI credential

관리자 CLI는 `DURE_SERVER`와 `DURE_ADMIN_TOKEN`을 같은 dotenv 파일에서 읽을 수 있습니다. 저장소 최상위에서 실행하면 먼저 `dure/.env`, 그다음 `.env`를 확인합니다. Dure 프로젝트 디렉터리 안에서 실행하면 해당 디렉터리의 `.env`를 확인합니다. 중앙 서버와 파일을 공유할 때는 위 예시처럼 `DURE_DATABASE_URL`도 함께 둡니다.

```bash
cd ~/workspace/dure
install -m 600 /dev/null dure/.env
```

파일에는 두 값을 모두 설정합니다. 실제 token은 출력하거나 Git에 추가하지 않습니다.

```dotenv
DURE_SERVER=https://api.dure.example
DURE_ADMIN_TOKEN=<same-random-secret-as-the-server>
```

```bash
dure admin nodes
dure admin nodes --pending
```

다른 위치는 admin 하위 명령보다 앞에 `--env-file`을 지정합니다.

```bash
dure admin --env-file /secure/path/dure-admin.env nodes
```

CLI는 파일을 shell로 source하지 않고 빈 줄·주석, `KEY=VALUE`와 `export KEY=VALUE`만 파싱합니다. 그중 `DURE_SERVER`·`DURE_ADMIN_TOKEN`만 사용하며 shell expansion과 command substitution은 실행하지 않습니다. 파일 크기는 64KiB 이하이고 현재 사용자 소유 regular file이며 group·other 권한이 없어야 합니다. symlink, 중복·빈 값, 두 설정 중 하나만 있는 파일은 요청 전에 거부합니다.

연결 설정 우선순위는 `--server`·`--token`, 선택된 dotenv의 한 쌍, 프로세스의 `DURE_SERVER`·`DURE_ADMIN_TOKEN`, 패키지의 서버 주소 순서입니다. 따라서 안전한 dotenv가 발견되면 셸에 남아 있는 오래된 admin 환경변수와 섞지 않습니다. 401이 발생하면 서버 프로세스와 관리자 파일의 token이 같은지 확인하되 값을 터미널·로그에 출력하지 말고, 수정 뒤 파일 권한을 다시 확인합니다.

## GPU 노드 런타임 준비

GPU 노드에는 Dure 패키지와 정상 동작하는 NVIDIA host driver가 먼저 있어야 합니다. 현재 bootstrap 지원 범위는 Ubuntu 22.04·24.04의 `amd64`·`arm64`입니다. 다만 공식 Dure APT 저장소는 아직 `amd64`만 게시합니다. `arm64`에서는 CLI·Agent 실행 파일과 함께 패키지의 `dure-agent.service`, `/etc/dure/dure-client.env`를 별도로 설치해야 하며 unit이 없으면 bootstrap을 차단합니다. CPU utility 노드는 Docker/NVIDIA runtime 준비를 건너뛸 수 있습니다.

먼저 root 권한으로 읽기 전용 계획을 확인합니다. 이 단계는 APT 설정, Docker 설정과 서비스를 바꾸지 않습니다.

```bash
sudo dure bootstrap
sudo dure bootstrap --json
```

검사는 OS·architecture, 기존 driver, Docker daemon, Toolkit 패키지 전체, Docker의 exact `nvidia` runtime, Dure가 사용하는 저장소·설정 경로와 예정 작업을 평가합니다. report에는 각 검사 코드·설명과 폐쇄형 예정 작업을 기록합니다. 다음 조건은 자동 복구하지 않고 차단합니다.

- NVIDIA driver가 없거나 `nvidia-smi`가 GPU를 보고하지 않습니다.
- Docker가 없는데 `docker.io`, `containerd`, `runc`, `podman-docker` 같은 충돌 패키지가 있습니다.
- NVIDIA Toolkit이 일부 패키지나 실행 파일만 가진 부분 설치 상태이거나 네 패키지가 지원 고정 버전과 다릅니다.
- 기존 Docker가 로컬 `/var/run/docker.sock`의 systemd `docker.service`가 아니거나 daemon에 연결할 수 없거나, 재부팅 뒤 `docker.service` 또는 `docker.socket` 기동이 유지됨을 증명할 수 없습니다.
- Docker CLI가 없더라도 Docker CE 패키지 일부, `dockerd`, `docker.service`나 `/var/run/docker.sock`이 남아 있어 미설치를 증명할 수 없습니다.
- Docker CLI 또는 Engine 버전이 GPU runtime과 `--pull never` 계약에 필요한 20.10보다 낮거나 버전을 해석할 수 없습니다. 배포판 Docker 29의 빈 `Platform.Name`은 단일 `Engine` 구성요소의 version·Linux OS·지원 architecture가 서버 응답과 일치할 때만 허용합니다.
- Dure Agent systemd unit이 없거나 Agent가 활성 상태이거나 `/etc/dure/agent.json`에 credential을 포함한 등록 정보가 존재합니다. `dure unjoin`이 credential을 제거하고 안전한 `install_id`만 남긴 설정은 예외입니다.
- APT key/source, `/etc/docker/daemon.json`, Dure backup 또는 그 부모가 예상과 다르거나 symbolic link입니다. 현재 `daemon.json` 없이 과거 backup만 남은 경우도 자동 해석하지 않습니다.

검토 뒤 명시적으로 적용하고 다시 조사한 다음 노드를 등록합니다.

```bash
sudo dure bootstrap --apply
sudo dure doctor
sudo dure join
```

적용은 Docker 공식 Ubuntu stable 저장소와 NVIDIA 공식 stable 저장소의 고정 URL을 추가해 사용하고, bootstrap이 새로 사용하거나 Dure의 고정 경로에 이미 있는 keyring에 기대한 primary key 하나만 있는지 fingerprint로 검사합니다. key와 부모는 APT의 비권한 key reader가 읽고 통과할 수 있어야 합니다. 실행 명령은 고정된 system `PATH`와 C locale을 사용하고 호출 셸의 proxy·APT·GPG 환경 설정 및 curl 사용자 설정은 상속하지 않습니다. 폐쇄망이나 proxy 전용 환경은 신뢰된 시스템 APT 설정과 별도 오프라인 provisioning 절차를 먼저 준비해야 합니다. Docker 설치는 제거를 허용하지 않는 닫힌 패키지 목록을 사용하고, Toolkit 네 패키지는 `1.19.1-1`로 설치해 `/etc/apt/preferences.d/dure-nvidia-container-toolkit`에서 우선순위 1001로 고정합니다. pin 파일이 다르면 자동 덮어쓰지 않습니다. 기존 `daemon.json`은 NVIDIA 설정 전에 `/var/lib/dure/bootstrap/daemon.json.before-nvidia-ctk`에 root 전용으로 보존합니다. `nvidia-ctk` 설정이 실패하면 원래 내용·권한·소유자만 복원하고 Docker를 재시작하지 않습니다. Docker 재시작을 이미 시도한 뒤 실패하거나 runtime이 확인되지 않으면 설정을 복원하고 기존 설정으로 복구 재시작을 한 번 시도합니다.

NVIDIA runtime을 처음 등록하면 Docker 재시작이 필요합니다. bootstrap은 사전 검사와 재시작 직전에 실행 중 컨테이너를 다시 조사합니다. 미리보기는 workload 개수와 영향을 경고하지만 차단하지 않으며, `--apply`는 검토한 폐쇄형 계획에 필요한 Docker 재시작까지 승인합니다. workload를 직접 확인하고 유지보수 시간을 확보한 뒤 실행합니다.

```bash
sudo dure bootstrap --apply
```

이 승인은 특정 Dure 컨테이너가 아니라 해당 Docker daemon의 모든 실행 workload가 잠시 중단될 수 있음을 뜻합니다. 기존 `--allow-docker-restart`는 호환성을 위해 계속 받지만 추가 권한을 뜻하지 않습니다. bootstrap은 host firewall을 수정하지 않지만 Docker Engine 설치 자체가 netfilter 규칙과 forwarding 동작에 영향을 줄 수 있으므로 적용 전후 방화벽을 별도로 검증합니다. 사용자를 `docker` 그룹에 추가하지 않으므로 준비 직후 검증은 `sudo dure doctor`로 실행합니다. 이 명령은 NVIDIA driver, 모델, 이미지, 컨테이너, 배포와 Agent 자격 증명을 만들거나 바꾸지 않습니다. 미설정 패키지 Agent는 `/etc/dure/agent.json`이 없으면 시작되지 않고 `dure join`이 설정을 쓴 뒤 활성화합니다. bootstrap apply와 join은 `/run/lock/dure-host-setup.lock`을 공유하며, 등록이 먼저 시작됐거나 노드가 이미 등록·활성화된 경우 bootstrap은 별도의 drain 절차를 추측하지 않고 거부합니다.

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

### GPU 노드 등록 해제

노드 운영자가 직접 반납할 때는 해당 노드에서 다음 명령을 실행합니다.

```bash
sudo dure unjoin
```

이 명령은 현재 상태에 기록된 배포 UUID·세대와 서버가 발급한 노드 UUID label이 모두 일치하는 Dure 컨테이너만 중지하고, Agent를 비활성화한 뒤 중앙 credential을 폐기합니다. 로컬 `/etc/dure/agent.json`에는 재등록 identity인 `install_id`만 남으며 모델 캐시, Docker와 NVIDIA driver는 삭제하지 않습니다. 이 안전한 비활성 설정은 bootstrap의 pre-join 경계를 통과합니다. 중앙 연결이나 exact 컨테이너 정리가 실패하면 credential을 지우지 않고 중단합니다.

중앙 운영자는 승인된 GPU 노드 한 대 또는 전체에 폐쇄형 `UNJOIN_NODE` 작업을 보낼 수 있습니다.

```bash
dure admin unjoin --node <node-id>
dure admin unjoin --all
dure admin tasks --watch
```

대상이 온라인인 동안 모든 작업이 성공했는지 확인하고, 다중 노드 배포에서 빠진 GPU는 기존 세대에 임의로 대체하지 말고 새 추천·준비·배포 세대로 재구성합니다. 다시 참여시키려면 해당 노드에서 `sudo dure join`을 실행하고 pending 등록을 새로 검토·승인합니다.

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

## 단일 GPU 릴리스 자동 활성화

단일 GPU 릴리스는 레지스트리 등록부터 실제 API 검증까지 하나의 명시적 자동
활성화 흐름으로 실행할 수 있습니다. 기본 실행은 읽기 전용 미리보기이고
`--apply`가 모델 준비·이미지 pull·벤치마크·승격·배포 전체를 승인합니다.

```bash
dure admin activate --file activation.json --all-online
dure admin activate --file activation.json --all-online --apply
```

후보를 제한하려면 `--all-online` 대신 `--nodes <uuid> ...`를 사용합니다. 자동
benchmark 노드는 exact cache 보유 여부, 남은 디스크와 VRAM 순으로 결정되며 최종
배포 노드는 ACTIVE 릴리스 추천기가 다시 선택합니다. immutable 문서 형식, Agent
0.3.25 요구 사항, artifact origin과 재시도 절차는
[자동 활성화 문서](activation.md)를 따릅니다. 다중 노드 pipeline은 자동
네트워크/NCCL 실행기가 없으므로 이 명령이 실패 안전 방식으로 거부합니다.

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
- 모든 대상 노드가 backend별 최소 버전을 충족합니다. legacy는 Agent 0.3.12 이상,
  `VLLM_RAY_PP_V1`은 0.3.18 이상입니다.
- `VLLM_RAY_PP_V1`은 전체 노드의 정확한 `pipeline-rank-contract`와 head의 API
  검증까지 성공합니다.

일부 노드만 검증하거나 다중 노드 배포에서 전체 배정 집합이 아닌 Ray head만 `--api` 검증한 결과는 상태 조회에는 남지만 `verified_at`을 만들지 않습니다. 기존 수동 복구 경로에서 발생한 구 Agent의 성공 결과도 롤백 증거로 승격하지 않습니다.

새 Ray·API 컨테이너에는 `dure.deployment`, `dure.generation`, `dure.node` 레이블이 모두 있어야 합니다. 시작·검증·중지 전에 실제 컨테이너의 세 레이블을 다시 확인합니다. 0.3.12 이전 컨테이너에 `dure.node`가 없는 경우에만 정확한 배포 ID와 세대가 모두 일치할 때 제한적으로 관리할 수 있습니다. 노드 레이블이 존재하면서 다르거나 배포·세대 레이블이 없거나 다르면 컨테이너를 중지·제거·재사용하지 않습니다.

## `VLLM_RAY_PP_V1` 다중 노드 운영

중앙 추천을 수락해 만드는 2·3노드 pipeline 세대는 `VLLM_RAY_PP_V1` 폐쇄형 실행 계약을 사용합니다. 기존 로컬 `dure plan` JSON과 legacy backend에는 이 계약을 소급 적용하지 않습니다. 운영자가 backend 이름이나 rank를 CLI 인자로 직접 주입하는 명령은 없으며, 중앙 추천·수락이 저장 인벤토리와 등록된 런타임을 검증해 계획에 고정합니다.

적용 전에 다음 조건을 모두 확인합니다.

- 런타임은 정확히 vLLM 0.9.0 V0 executor를 포함한 OCI digest 고정 이미지입니다.
- `TP=1`, `PP=2` 또는 `PP=3`이며 각 물리 노드에는 정상 GPU가 정확히 한 장 있습니다.
- assignment는 hostname이 아니라 서버가 발급한 canonical UUID를 사용합니다.
- 각 노드는 서로 다른 canonical RFC1918 IPv4를 가지며 head가 rank 0, worker는 IPv4 문자열 오름차순입니다.
- 각 노드의 계획 주소가 최신 probe의 `default_interface_addresses`에 정확히 하나
  존재하고 모든 노드의 기본 network interface 이름이 같습니다.
- `FULL_SNAPSHOT`이면 모든 노드의 exact 전체 캐시가, `STAGE`이면 각 노드의 서로 다른 exact rank 캐시가 중앙에서 `READY`입니다. 이 상태는 현재 모델 준비 시도에 결합되고 최신 이미지 준비 시도도 계획의 OCI digest를 성공적으로 검사해야 합니다.
- 대상 노드가 모두 승인·온라인이고 `FULL_SNAPSHOT` 실행은 Agent 0.3.18 이상, `STAGE` 준비·실행은 0.3.19 이상입니다. 비중지 작업은 일부 노드만 골라 실행할 수 없습니다.

일반 추천·준비·적용 명령을 그대로 사용합니다.

```bash
dure admin probe --nodes <node-a> <node-b> [<node-c>]
dure admin deployment recommend --nodes <node-a> <node-b> [<node-c>] \
  --objective quality-first
dure admin recommendation show <recommendation-id>
dure admin recommendation accept <recommendation-id>

dure admin deployment prepare <deployment-id> --request-id <request-uuid>
dure admin deployment prepare <deployment-id> \
  --request-id <request-uuid> --apply
dure admin deployment preparation <preparation-id>

# 추천이 STAGE를 선택한 경우 이 옵션은 선택된 digest 일치 assertion입니다.
# 생략해도 STAGE 선택은 유지되며 다른 variant나 FULL로 바뀌지 않습니다.
dure admin deployment prepare <deployment-id> \
  --request-id <stage-request-uuid> --stage-variant sha256:<64-hex>
dure admin deployment prepare <deployment-id> \
  --request-id <stage-request-uuid> --stage-variant sha256:<64-hex> --apply

dure admin apply <deployment-id> --nodes <node-a> <node-b> [<node-c>] --serve
dure admin verify <deployment-id> \
  --nodes <node-a> <node-b> [<node-c>] --api
```

엄격한 backend에서는 API가 head에서만 실행되더라도 `verify --api` 요청에 전체 배정 노드를 넣습니다. worker는 API HTTP 검사를 생략하지만 자신의 Ray·rank 계약을 검증하고, head만 loopback API까지 검사합니다. 기존 legacy 운영 예시의 head 단독 `verify --api`를 `VLLM_RAY_PP_V1`에 사용하면 전체 노드 집합 게이트에서 거부됩니다.

엄격한 backend의 직접 `apply`, `start`, `restart` 요청은 실제 API readiness까지
증명하도록 `--serve`가 필요하고 직접 `verify`는 `--api`가 필요합니다. controller가
관리하는 단계형 apply·rollback은 내부적으로 먼저 모든 노드에 `serve=false`로 Ray를
준비한 뒤 head 전용 API 단계를 이어가므로 이 직접 요청 게이트와 구분합니다.
`VLLM_RAY_PP_V1` rollback 요청 자체에는 `--serve`가 필수이며, API actor 증적 없이
복구한 대상을 새 `verified_at` 세대로 승격할 수 없습니다.

추천 세대의 apply·start·restart·verify는 task 생성 직전에 전체 노드의 exact `READY`, 현재 모델 준비 시도와 최신 OCI digest 이미지 시도를 다시 검사합니다. `STALE`·`MISSING`·`CORRUPT`·`QUARANTINED`, 철회된 stage variant, 과거 성공 시도나 image digest drift가 있으면 컨테이너 변경 전에 거부합니다. 긴급 stop은 이 준비 게이트가 손상돼도 exact 배포 레이블로 계속 제한 실행할 수 있습니다.

엄격한 backend의 네트워크 값은 다음으로 고정됩니다.

| 용도 | 주소·포트 | 운영 경계 |
|---|---|---|
| Ray GCS | head의 RFC1918 IPv4, TCP `6379` | 신뢰 LAN·사설 오버레이에서만 허용 |
| Ray worker | 각 계획 IPv4, TCP `20000-21000` | 노드 집합 사이만 허용 |
| vLLM API | head의 `127.0.0.1:8000` | loopback 전용, 외부 제공은 인증된 역방향 프록시 사용 |

Ray 컨테이너는 계획의 `--node-ip-address`와 `VLLM_HOST_IP`를 사용하고 서버 UUID에서 계산한 `dure_node_<uuidhex>` custom resource를 게시합니다. `dure.runtime-contract` 레이블은 이미지·모델 mount·GPU·network·entrypoint·고정 환경·명령의 정규 SHA-256이며 시작·재사용·readiness에서 정확히 비교합니다. 긴급 `STOP`은 준비 경로 손상에도 작동하도록 다른 exact identity 레이블로 대상을 제한하되 이 digest를 재계산하지 않습니다. 임의 Ray 포트나 vLLM Docker 인자를 task payload로 전달할 수 없습니다. GCS·worker 범위를 공용 인터넷에 열지 말고 host firewall에서 정확한 노드 주소만 허용합니다.

각 노드의 `VERIFY` 결과에는 기존 `host-gpu`, `container-gpu` 검사와 함께 blocking `pipeline-rank-contract`가 있어야 합니다. 이 검사는 정확한 vLLM 0.9.0, 살아 있는 Ray 노드·GPU, 주소별 Dure UUID custom resource와 계획 binding을 대조하고, API 시작 뒤 검사에서는 worker actor 토폴로지도 요구합니다. 결과 JSON의 rank는 Ray가 공개한 내부 pipeline rank 필드가 아니라, 고정된 vLLM 0.9.0 소스 규칙에서 도출한 기대 binding입니다. 따라서 이를 “runtime rank 직접 관측”으로 보고하거나 다른 vLLM 버전에 재사용하지 않습니다.

### 실제 2·3노드 GPU 수용 검사

단위 테스트는 실제 GPU, driver, NCCL과 분산 model load를 요구하지 않습니다. 운영 배포 전 신뢰된 2대 또는 3대 노드에서 `scripts/acceptance-vllm-ray-pp.py`를 별도로 실행합니다. harness는 명령행 인자를 받지 않고 다음 고정 경로만 읽습니다.

```text
/etc/dure/acceptance-vllm-ray-pp-v1.json
```

설정은 root가 소유하고 group/world writable이 아니어야 합니다. 다음 폐쇄형 필드 외의 command, 환경 변수 묶음, Docker 인자, mount 또는 host path를 넣지 않습니다. 실제 값은 생성된 계획과 digest 고정 레지스트리 값에서 복사해 서로 대조합니다.

```json
{
  "schema_version": 1,
  "backend": "VLLM_RAY_PP_V1",
  "vllm_version": "0.9.0",
  "validation_run_id": "11111111-1111-4111-8111-111111111111",
  "deployment_id": "22222222-2222-4222-8222-222222222222",
  "generation": 1,
  "runtime_image": "registry.example/vllm@sha256:<64-hex>",
  "model_manifest_digest": "sha256:<64-hex>",
  "ordered_bindings": [
    {
      "node_id": "33333333-3333-4333-8333-333333333333",
      "runtime_address": "10.20.0.10",
      "pipeline_rank": 0,
      "runtime_rank": 0
    },
    {
      "node_id": "44444444-4444-4444-8444-444444444444",
      "runtime_address": "10.20.0.11",
      "pipeline_rank": 1,
      "runtime_rank": 1
    }
  ]
}
```

검사 환경은 `/models/model`에 같은 매니페스트의 검증된 `FULL_SNAPSHOT`을 읽기 전용으로 제공하고 vLLM 0.9.0과 Ray를 미리 설치해야 합니다. 다음 명령은 실제 이미지 digest와 고정 mount를 먼저 검증한 신뢰된 wrapper **안에서** 실행합니다. harness가 허용하는 opt-in은 하나뿐입니다.

```bash
DURE_RUN_VLLM_RAY_PP_ACCEPTANCE=1 \
  python3 scripts/acceptance-vllm-ray-pp.py
```

기본 실행, opt-in 누락, 설정·GPU·고정 runtime·모델 전제 조건 부족은 `NOT_RUN`과 종료 코드 `77`입니다. 이는 통과가 아닙니다. preflight 뒤 실제 Ray 연결·분산 load가 시작된 후의 오류는 `FAILED`와 종료 코드 `1`이며 `NOT_RUN`으로 낮추지 않습니다. `PASSED`는 실제 executor topology, worker 배치와 고정 최소 추론까지 성공한 실행에만 사용합니다.

설정의 `runtime_image` digest는 harness가 읽는 선언값입니다. 스크립트는 Ray node의 `dure_node_<uuidhex>` custom resource와 주소를 설정의 `node_id`에 다시 결합하지만, 현재 프로세스의 OCI manifest digest 자체를 독립적으로 읽을 수는 없습니다. 따라서 운영자는 이 스크립트를 신뢰된 digest 고정 wrapper 안에서 실행하고 wrapper가 실제 이미지 digest, 중앙 계획과 설정을 대조한 기록을 함께 보존해야 합니다. 성공 결과도 `runtime_image_declared`와 `runtime_image_attested=false`를 구분합니다. 현재 Dure는 이 attested wrapper를 자동 생성하거나 원격 실행하지 않습니다. wrapper 기록이 없는 `PASSED`는 공급망 증적이 아닙니다. harness는 컨테이너 생성·중지·교체나 driver 설치를 수행하는 배포 명령이 아니며, controller가 수집한 각 노드의 `pipeline-rank-contract`와 별도 운영 적용·검증을 대신하지 않습니다.

### 실행 실패 대응

| 실패 지점 | Dure의 기본 처리 | 운영자 조치 |
|---|---|---|
| UUID·주소·GPU 수·계획·Agent 버전 불일치 | 컨테이너 시작 전 차단 | 최신 probe와 계획을 다시 비교하고 임의로 rank나 JSON을 고치지 않습니다. |
| exact 캐시가 `STALE`·`MISSING`·`CORRUPT`·`QUARANTINED`이거나 현재 준비·이미지 증적 불일치 | apply·start·restart·verify와 rollback target 시작 차단 | `artifact-cache show/verify`와 preparation을 확인하고, 오염된 캐시는 덮어쓰지 말고 참조가 없을 때만 명시적 quarantine 뒤 현재 준비 성공으로 `READY`를 복구합니다. |
| Ray join·노드 수·GPU 수·actor topology 불일치 | `pipeline-rank-contract` 실패, 다음 단계 차단 | 방화벽·주소·중복 Ray process·노드 상태를 조사하고 실패 노드를 격리합니다. |
| vLLM load·최소 추론·API 실패 | operation 또는 GPU harness를 `FAILED`로 보존 | 고정 image와 모델 계약, 메모리, driver/CUDA 호환성을 조사한 뒤 명시적으로 재시도합니다. |
| 중앙 배포 `VERIFY` 실패 | operation 실패와 함께 해당 노드의 exact 캐시를 `CORRUPT`로 투영 | 런타임·파일 무결성 원인을 조사하고 현재 준비 성공으로 `READY`를 다시 만든 뒤 재시도합니다. 과거 성공 증적을 재사용하지 않습니다. |
| driver 또는 host CUDA 불일치 | 자동 수정하지 않고 실패 | 지원 조합으로 host를 수동 정비하거나 해당 노드를 후보에서 제외합니다. Dure가 driver를 변경하도록 권한을 넓히지 않습니다. |
| 고정 port가 다른 process에 점유됨 | 시작 뒤 container restart/readiness 실패로 차단 | `6379`, `20000-21000`, `8000` 점유자를 확인하고 정확한 Dure label의 새 세대만 중지한 뒤 재시도합니다. 현재 별도 port 점유 preflight는 없습니다. |
| 반복 실패·신뢰 상실 노드 | 자동 재배정하지 않음 | `dure admin credential revoke <node-id>`로 task 수신을 격리하고 조사 후 credential 회전·설정 갱신·재승인을 별도로 수행합니다. |

사전 검사 실패는 실행 중인 이전 세대에 손대지 않습니다. 전환이 이미 시작된 뒤의 부분 실패는 같은 GPU에서 이전 컨테이너가 계속 실행됨을 보장하지 않으므로 operation 상태와 실제 컨테이너를 먼저 확인합니다. 원인을 해결한 뒤 같은 전체 노드 입력으로 실패 단계를 명시적으로 재시도하거나, 조건을 충족한 검증된 직전 세대에 `deployment rollback ... --apply`를 수행합니다. 자동 failover·자동 rollback은 없으며 롤백도 새 다운로드나 image pull을 하지 않습니다.

환경 차이는 GPU 건강·VRAM·compute capability, driver 관측값, 런타임 GPU architecture, Docker/NVIDIA runtime, 디스크와 네트워크 증적의 폐쇄형 규칙으로 처리합니다. 호환 여부가 불확실한 노드는 후보에서 제외합니다. 새 probe·recommend에서 다른 적격 노드나 더 작은 `ACTIVE` 모델의 독립 `FULL_SNAPSHOT` 후보를 선택할 수 있지만, 이미 수락한 세대 안에서 모델·노드·`STAGE`↔`FULL_SNAPSHOT`을 자동 변경하지 않습니다. Dure는 NVIDIA host driver를 설치·업그레이드·다운그레이드하지 않습니다.

노드 격리는 credential revoke를 뜻하고, 캐시 격리는 별도 `artifact-cache quarantine`을 뜻합니다. 캐시 격리는 참조가 없음을 완전하게 증명한 exact final 하나만 보존 위치로 원자 이동하며 삭제하지 않습니다. 두 절차를 혼동하지 말고, 운영자가 상위 모델 경로나 공유 CAS를 임의 삭제해 중앙 증적과 실제 호스트 상태를 어긋나게 만들어서는 안 됩니다.

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

## stage artifact 생성·검증 운영

0.3.17은 신뢰된 오프라인 환경에서 pipeline rank별 `STAGE`를 만들고 중앙에 variant·rank 매니페스트·검증 증적을 기록합니다. 0.3.20의 추천기는 exact `VALIDATED` variant와 독립 `FULL_SNAPSHOT`을 결정론적으로 평가하고, 수락한 세대에 cache kind·variant·loader·UUID→rank·증적을 고정합니다. 별도의 명시적 준비 적용 뒤에만 각 Agent가 자신의 rank를 내려받아 활성화하고 실행합니다. builder 자체는 여전히 중앙 Agent 작업이 아니며 GPU 노드에서 임의 변환하지 않습니다.

운영자는 빌드 전에 다음 고정 계약을 모두 확인합니다.

- source가 정규 매니페스트로 검증된 로컬 `FULL_SNAPSHOT`입니다.
- builder로 사용하는 runtime 이미지가 정확한 OCI SHA-256 다이제스트로 고정돼 있습니다.
- vLLM은 정확히 0.9.0이고 V0 executor를 사용합니다.
- 모델은 `Qwen2ForCausalLM` AWQ이며 `TP=1`, loader는 `sharded_state`입니다.
- remote code, `auto_map`, LoRA·adapter, MoE, 멀티모달 파일이나 임의 Python 모델 코드를 포함하지 않습니다.

기본 순서는 다음과 같습니다.

```text
고정 source·builder·runtime 확인
        ↓
격리된 오프라인 builder에서 export
        ↓ 각 PP worker를 stages/<pp-rank>로 분리
rank 완전성·tensor coverage·파일·digest 검사
        ↓
모든 stage 매니페스트를 결합해 DRAFT 등록
        ↓
실제 GPU export/load 검증
        ↓
GPU_EXPORT_LOAD/PASSED이면 VALIDATED
FAILED·NOT_RUN이면 비승격, 신뢰 철회 시 REVOKED
```

vLLM 0.9.0의 sharded-state 파일명은 TP rank를 사용합니다. `TP=1`에서는 모든 PP worker가 `model-rank-0-*`를 쓰므로 공용 출력 디렉터리를 사용하지 않습니다. 각 worker는 자신의 pipeline rank 디렉터리인 `stages/<pp-rank>`에만 출력해야 합니다. `layer_start`·`layer_end`로 원본 파일을 직접 분할하지 않습니다.

variant 등록 시 source manifest, runtime digest, vLLM, exporter build digest, 아키텍처·양자화, TP·PP, loader와 rank 정렬 stage manifest가 같은지 검토합니다. rank는 `0..PP-1`이 정확히 한 번씩 있어야 합니다. 같은 고정 입력에서 출력 매니페스트가 달라지면 새 정상 variant로 간주하지 말고 비결정성·잘못된 빌더 또는 변조를 조사합니다.

synthetic fixture 검사는 빠진 rank, 잘못된 파일, tensor 누락과 digest 변조를 찾는 개발 게이트일 뿐 승격 증적이 아닙니다. 실제 GPU 검증의 전제 조건이 없으면 결과를 `NOT_RUN`으로 기록하며 성공으로 바꾸거나 생략하지 않습니다. 정확한 variant에 결합된 최신 `GPU_EXPORT_LOAD/PASSED`만 `DRAFT → VALIDATED`를 허용합니다. 새 validation run 증적은 `DRAFT`에서만 추가할 수 있습니다. 이미 등록한 동일 run의 정확한 네트워크 재전송은 `VALIDATED`나 `REVOKED` 뒤에도 기존 결과를 반환하지만, 두 상태에서 새 run을 추가하는 요청은 거부합니다. 검증 뒤 신뢰 문제가 발견되면 운영자가 영향 범위를 검토해 명시적으로 `REVOKED`로 전환하고 수정된 계약은 새 `DRAFT` variant에서 검증합니다. `REVOKED`는 되돌리지 않습니다.

번들 `scripts/acceptance-vllm-stage-builder.py`는 `PP=1` export·native load·최소 추론을 검증합니다. `scripts/acceptance-vllm-stage-ray-pp.py`는 신뢰된 `PP=2/3` 노드에서 이미 준비한 rank별 캐시를 실제 Ray/vLLM `sharded_state`로 load하고 최소 추론을 확인합니다. 두 harness 모두 opt-in·GPU·고정 runtime 등 전제 조건이 없으면 `NOT_RUN`과 종료 코드 77이며 성공으로 취급하지 않습니다. 기존 `FULL_SNAPSHOT` 수용 결과를 stage 증적으로 바꾸어 기록해서도 안 됩니다.

현재 중앙 운영은 다음 관리자 인증 API를 사용합니다. 전용 `dure admin` 하위 명령은 아직 없습니다.

```text
POST /v1/admin/stage-artifact-variants
GET  /v1/admin/stage-artifact-variants
GET  /v1/admin/stage-artifact-variants/{artifact_set_digest}
POST /v1/admin/stage-artifact-variants/{artifact_set_digest}/evidence
POST /v1/admin/stage-artifact-variants/{artifact_set_digest}/transition
```

등록과 증적 본문은 폐쇄형 필드만 받습니다. raw 로그, credential, 원본 URL, 명령, Docker 인자, 환경 변수, 마운트나 호스트 경로를 추가하지 않습니다. 증적에는 실제 실행마다 새 canonical UUIDv4 `validation_run_id`를 넣습니다. 같은 run ID와 같은 내용의 전송만 기존 결과를 반환하고, 같은 run ID를 다른 결과에 재사용하면 충돌합니다. 같은 contract identity에 다른 stage bytes를 연결하려는 요청도 충돌로 처리합니다.

### stage 실패 대응

| 상황 | 현재 처리 | 운영자 조치 |
|---|---|---|
| 지원하지 않는 모델·vLLM·토폴로지 | build 또는 등록 전 거부 | 허용 목록을 우회하지 않고 별도 검증 PR로 지원 계약을 추가합니다. |
| PP rank 누락·중복·범위 오류 | variant 등록 거부 | partial 결과를 수동 결합하지 말고 전체 출력을 같은 고정 입력으로 다시 만듭니다. |
| 예상 밖 파일·remote code·adapter 감지 | 출력 거부 | 원본과 명시적 metadata 허용 목록을 조사합니다. |
| 파일·tensor·매니페스트 digest 불일치 | 변조 가능성이 있는 실패 | digest를 다시 써서 맞추지 말고 source·builder 이미지·출력 저장소를 조사합니다. |
| 빌더 중단·디스크 부족 | staging을 최종 결과로 게시하지 않음 | 공간과 I/O 원인을 해결하고 새 격리 staging에서 다시 실행합니다. 기존 variant를 덮어쓰지 않습니다. |
| GPU·driver·고정 이미지 전제 부족 | `NOT_RUN` | 적합한 환경에서 새 검증을 실행합니다. `NOT_RUN`으로 승격하지 않습니다. |
| 실제 export/load 실패 | `FAILED`, 비승격 | driver/CUDA/runtime과 모델 계약을 조사하고 수정된 결과는 새 증적으로 검증합니다. |

stage build, 등록, 증적 기록과 상태 전이는 deployment, 준비 operation, Agent task, 다운로드, image pull이나 Docker 실행을 만들지 않습니다. 실패해도 실행 중인 이전 배포를 자동 중지·교체·롤백하지 않습니다. 새 stage variant를 사용할 수 없다는 사실과 기존 배포가 정상이라는 사실은 별개이므로 기존 세대의 health와 `VERIFY` 결과를 계속 관찰합니다. `REVOKED` variant는 연결된 중앙 캐시를 `STALE`로 내려 향후 apply·start·restart·verify와 rollback target 시작을 차단하지만, 이미 실행 중인 컨테이너를 자동 중지하지는 않습니다.

추천이 `VALIDATED` variant를 선택한 세대의 실제 준비는 다음처럼 별도 수행합니다. `--stage-variant`는 이미 고정된 digest와의 선택적 일치 assertion이며, 생략해도 `STAGE`가 `FULL_SNAPSHOT`으로 바뀌지 않습니다. 지정한 digest가 다르면 자동 fallback 없이 거부합니다.

```bash
dure admin deployment prepare <deployment-id> \
  --request-id <request-uuid> --stage-variant sha256:<64-hex>
dure admin deployment prepare <deployment-id> \
  --request-id <request-uuid> --stage-variant sha256:<64-hex> --apply
```

중앙은 source·runtime·vLLM·양자화·TP·PP와 모든 rank 증적을 다시 확인한 뒤 node UUID와 PP rank별 매니페스트를 task에 결합합니다. Agent는 task가 준 경로가 아니라 계약 전체에서 계산한 `/var/lib/dure/models/stages/sha256-<cache-identity>`만 사용합니다. 준비 성공 뒤에도 시작 직전에 전체 stage 파일과 marker를 다시 해시하며 불일치하면 Docker 호출 전에 실패합니다.

일반 probe는 `artifact_cache_observations`와 `artifact_cache_scan_complete`를 함께 보고합니다. 최대 256개의 marker metadata만 읽으며 `PRESENT`는 `READY` 승격이나 치유 근거가 아닙니다. `scan_complete=true`인 완전한 목록만 알려진 cache identity를 `MISSING`·`STALE`·`CORRUPT`로 악화시킬 수 있고, legacy·불완전 조사는 어떤 상태도 바꾸지 않습니다. 현재 모델 준비 성공과 배포 시작 전 runtime 전체 재해시가 권위 있는 무결성 근거입니다.

vLLM·PyTorch·safetensors·CUDA 계열 의존성은 기본 Debian 패키지에 설치하지 않습니다. 운영 Agent나 중앙 서버에서 임시로 설치하지 말고 digest 고정 별도 OCI builder를 사용합니다. 자세한 계약과 후속 배포 연결 범위는 [stage artifact 문서](stage-artifacts.md)를 참고합니다.

## 중앙 아티팩트 준비와 실패 복구

중앙은 추천이 선택하고 수락한 exact `FULL_SNAPSHOT` 또는 rank별 `STAGE` 캐시와 다이제스트 고정 OCI 이미지를 준비합니다. `recommend`, `accept`와 준비 preview는 다운로드나 task를 만들지 않습니다. 운영자가 같은 준비 요청에 `--apply`를 명시한 뒤에만 각 노드에서 `PREPARE_MODEL → PREPARE_IMAGE`를 순서대로 실행합니다. `benchmark-runs/prepare`는 이름이 비슷하지만 모델 바이트가 아니라 DB 실행 문맥만 준비하는 별도 기능입니다.

production 고정 경계는 다음과 같습니다.

- 청크 CAS·잠금·시도 저널: `/var/lib/dure/model-store`
- 활성 모델과 숨은 조립 영역: `/var/lib/dure/models`
- 원본: 각 노드의 root 전용 Agent 설정으로 만드는 검증된 `TrustedHTTPSOrigin`. 중앙 task나 DB가 raw URL·header·token을 전달하지 않음
- 지원 cache kind: 추천에 고정된 `FULL_SNAPSHOT` 또는 exact `VALIDATED` `STAGE`. 준비 요청으로 형식을 바꿀 수 없고 둘 사이 자동 fallback 없음

Agent 설정의 기존 secret과 node ID를 유지한 채 root 권한으로 `artifact_origin`을 추가합니다. 이 파일 전체를 로그나 지원 요청에 첨부하면 안 됩니다.

```json
{
  "artifact_origin": {
    "base_url": "https://models.internal.example/artifacts",
    "allowed_redirect_hosts": ["objects.internal.example"]
  }
}
```

각 청크 URL은 이 base URL과 등록된 SHA-256에서 Agent가 계산합니다. 현재 전송기는 token, cookie와 사용자 지정 인증 header를 지원하지 않으므로 별도 credential 없이 접근 가능한 신뢰 HTTPS origin을 사용해야 합니다. 자격 증명을 task payload, 매니페스트, 중앙 DB나 결과에 추가해 우회하면 안 됩니다.

추천을 수락한 뒤 준비를 다음 순서로 진행합니다.

```bash
# task를 만들지 않는 preview
dure admin deployment prepare <deployment-id> --request-id <request-uuid>

# 같은 요청을 명시적으로 적용
dure admin deployment prepare <deployment-id> \
  --request-id <request-uuid> --apply

# 준비 전체와 노드별 MODEL/IMAGE 시도 조회
dure admin deployment preparation <preparation-id>
```

API는 `POST /v1/admin/deployments/{deployment_id}/prepare`와 `GET /v1/admin/deployment-preparations/{preparation_id}`를 제공합니다. 요청 ID는 정규 UUID여야 하며 preview와 apply에 같은 값을 사용합니다. preview는 불변 계획과 노드 행만 저장하고 task를 0개 반환합니다. preview와 최초 apply 직전에는 등록 매니페스트와 다이제스트 이미지, 승인·최근 온라인·신선한 인벤토리·보수적인 디스크 여유, 배포와 노드 집합을 다시 검사합니다. 하나라도 달라지거나 불확실하면 어떤 노드의 작업도 새로 만들지 않습니다.

준비 조회의 `progress`는 전체와 노드별 `expected_bytes`, `verified_bytes`, `download_expected_bytes`, `downloaded_bytes`, 현재 `stage`, 모델·이미지 단계의 `current_attempt`·`retry_count`·상태를 함께 보여 줍니다. 기대 바이트와 검증 바이트는 각각 정규 매니페스트 파일 전체 크기와 현재 모델 시도의 완료 무결성 검증 합계입니다. 다운로드 기대 바이트는 중복 제거한 고유 청크의 합이고, 다운로드 바이트는 로컬 CAS·부분 파일·staging·final 재사용과 쓰기에서 관측된 모델 준비 high-water입니다. 실제 네트워크 전송량이나 속도가 아니며 digest 오류 뒤 부분 파일이 초기화돼도 내려가지 않으므로 100%를 `READY`로 해석해서는 안 됩니다. `download_bytes_source`는 `NOT_STARTED`, `MODEL_PREPARATION_HIGH_WATER`, `DERIVED_FROM_COMPLETED_MODEL_VERIFICATION`, `UNAVAILABLE`, 여러 노드가 섞인 `MIXED` 중 하나입니다. 이미지 pull에는 바이트 진행률이 없습니다.

downloader가 한 중앙 모델 시도 안에서 수행하는 제한 재시도와 prepare를 다시 적용해 새 중앙 `attempt_no`를 만드는 재시도는 다릅니다. CLI의 `retry_count`는 후자만 셉니다. 실제 성공은 `verified_bytes`, 모델 단계 성공과 exact `READY`로 판단합니다.

적용은 먼저 모든 대상 노드에 `PREPARE_MODEL`을 큐잉합니다. 한 노드의 모델이 전체 청크·파일 해시와 marker-last 검사를 통과하면 그 현재 시도와 exact identity를 같은 트랜잭션에서 중앙 `READY`로 투영하고, 그 뒤에만 같은 노드의 `PREPARE_IMAGE`를 만듭니다. 이미지 작업은 정확한 digest 참조를 inspect하고 없을 때만 pull한 뒤 다시 inspect하며, 컨테이너를 실행·중지·삭제하지 않습니다. 준비 상태는 `PREPARED`, `QUEUED`, `RUNNING`, `SUCCEEDED`, `PARTIAL_FAILED`, `FAILED` 중 하나입니다.

실패 뒤 같은 request ID와 `--apply`를 다시 사용하면 현재 실패 단계만 새 시도 번호로 재시도합니다. 모델이 실패한 노드는 모델부터 다시 시작하고 이미지 작업을 만들지 않으며, 모델 성공 뒤 이미지가 실패한 노드는 이미 검증된 모델을 반복하지 않습니다. 두 단계를 성공한 노드도 다시 실행하지 않습니다. task ID, preparation·노드·단계와 현재 시도 번호가 모두 일치하지 않는 늦은 진행률 heartbeat·완료·실패 보고는 준비 상태나 중앙 cache state를 바꾸지 못합니다. 과거 성공은 이후 `CORRUPT`·`QUARANTINED` 캐시를 되살리지 않으며 현재 준비 성공만 `READY`를 복구합니다.

재시도 시 중앙은 부분 CAS·staging의 실제 할당량을 알 수 없어 최초의 전체 크기 최악 조건 디스크 검사를 반복하지 않습니다. 대신 승인·온라인·프로필 신선도·안정 하드웨어·활성 작업과 불변 아티팩트·런타임 결합을 다시 확인하고, Agent가 작업 시작 전에 실제 파일시스템별 남은 바이트를 계산합니다. 이 검사에서 부족하면 다운로드나 final 활성화 전에 실패하므로, 재시도 task 생성은 디스크 충분 증적이 아닙니다.

정상 실행은 디스크 사전 검사, 청크 다운로드·전체 digest 검사, 파일 조립·전체 파일 해시 검사, `config.json` 양자화 일치, exact-tree 검사, marker-last 기록과 no-replace 활성화 순서로 진행합니다. 같은 digest의 검증된 청크·완성 파일·final은 다시 검사한 뒤 재사용합니다. `MODEL_STORE_DOWNLOAD_TIMEOUT`과 응답 body 중단, 파일·marker 조립 중단은 결정적 부분 파일에서 이어가지만, non-timeout DNS·TLS·connect 거부와 응답 계약·digest 오류는 청크 `.part`를 0바이트로 되돌리고 재시도합니다.

저장소와 저널 경계가 정상이라면 실패 코드는 `/var/lib/dure/model-store/attempts/<manifesthex>/journal.json`의 폐쇄형 로컬 마지막 상태로 남고 URL, credential, 원격 오류 본문과 예외 원문은 남기지 않습니다. 루트·권한·저널 I/O 자체가 실패하면 실패 상태도 기록하지 못할 수 있습니다. 이 저널은 중앙 operation 진행률, `READY` 증적이나 자동 경보가 아닙니다. 운영자는 다음 원칙으로 대응합니다.

1. `MODEL_STORE_DOWNLOAD_TIMEOUT`이나 body read·조립 중단이면 제한 재시도가 끝났는지 확인하고 같은 요청을 재시도합니다. 새 staging이 계속 생기지 않고 같은 digest 영역을 사용해야 합니다. `MODEL_STORE_DOWNLOAD_REJECTED`나 digest 불일치는 청크 부분 파일을 보존 재개하지 않고 0바이트부터 다시 받습니다.
2. `MODEL_STORE_DISK_INSUFFICIENT`이면 모델 전체 조립본, 없는 고유 청크와 기본 여유 공간을 합친 용량을 확보합니다. 기존 검증 부분의 실제 할당량은 재시도 계산에 반영됩니다. 사전 검사는 공간 예약이 아니므로 외부·동시 소비로 쓰기 중 `ENOSPC`가 날 수 있습니다. 이때 유효 marker와 final은 게시하지 않고 결정적 부분 파일만 남기므로 공간 확보 뒤 같은 digest를 재시도합니다.
3. digest·파일 무결성 오류, path·target collision이면 자동 덮어쓰기나 삭제를 시도하지 않습니다. 정확한 매니페스트, origin object와 소유권을 먼저 조사합니다.
4. 중앙에 등록된 canonical final은 `artifact-cache verify`로 참조 투영을 확인한 뒤 `artifact-cache quarantine <cache-id> --apply`로만 보존 격리합니다. staging은 공식 격리 대상이 아니며 수동 조치 전 같은 digest 준비와 모든 관련 작업이 끝났는지 별도 증명해야 합니다. 공유 CAS 청크는 모든 매니페스트와 진행 중 준비의 미참조를 증명할 수 없으면 옮기거나 삭제하지 않습니다.
5. `/var/lib/dure/models`, `/var/lib/dure/model-store`나 wildcard를 대상으로 재귀 삭제하지 않습니다. 공식 quarantine도 자동 eviction·삭제가 아니라 exact final 하나의 원자적 보존 이동입니다.

### 중앙 캐시 상태 확인

```bash
dure admin artifact-cache list
dure admin artifact-cache show <cache-id>
dure admin artifact-cache verify <cache-id>
```

세 명령은 모두 읽기 전용입니다. `verify`도 중앙 cache row와 참조 투영만 검사하며 노드에 task를 보내거나 파일 전체를 재해시하지 않습니다. 응답의 `complete=false`는 참조가 없다는 뜻이 아니라 참조 계산을 신뢰할 수 없다는 뜻이므로 격리를 금지합니다.

| 상태 | 운영 해석 | 다음 조치 |
| --- | --- | --- |
| `READY` | 현재 모델 준비 성공과 exact identity가 일치 | 최신 이미지 준비 증적까지 확인한 뒤 배포 소비 가능 |
| `STALE` | variant 철회, identity 불일치 또는 격리 요청 | 원인을 확인하고 현재 준비 성공으로 복구 |
| `MISSING` | 완전한 probe에서 알려진 캐시가 빠짐 | 노드 경로와 mount를 조사하고 별도 준비 수행 |
| `CORRUPT` | unsafe·marker 손상 관측 또는 중앙 배포 검증 실패 | 실행을 중단하고 참조 확인 뒤 격리·재준비 |
| `QUARANTINED` | exact final이 보존 위치로 원자 이동됨 | 자동 복구·삭제 없음. 필요하면 지원되는 준비로 새 source와 `READY` 생성 |

Agent probe는 최대 256개의 폐쇄형 marker metadata와 `scan_complete`를 함께 보고합니다. `scan_complete=true`인 완전한 조사만 중앙에 알려진 identity를 `STALE`·`MISSING`·`CORRUPT`로 악화시킬 수 있습니다. 256개 초과, 루트 조사 오류, legacy Agent 또는 `scan_complete=false`는 중앙 상태를 승격도 강등도 하지 않습니다. 완전 조사에서 `PRESENT`도 관측 시각만 갱신할 뿐 `READY`로 올리거나 `CORRUPT`를 치유하지 않습니다.

### exact 캐시 격리

먼저 preview를 실행합니다.

```bash
# 참조 계산만 하고 task는 0개입니다.
dure admin artifact-cache quarantine <cache-id>

# 참조를 잠금 상태에서 다시 계산하고 task 하나를 만듭니다.
dure admin artifact-cache quarantine <cache-id> --apply
```

같은 기능은 관리자 인증 API의 `GET /v1/admin/artifact-caches`, `GET /v1/admin/artifact-caches/{cache_id}`, `GET /v1/admin/artifact-caches/{cache_id}/verify`, `POST /v1/admin/artifact-caches/{cache_id}/quarantine`에서 제공합니다. POST 본문은 엄격한 `{"apply": false|true}`만 받습니다.

`--apply`는 승인·온라인 Agent 0.3.20 이상인지 확인하고 다음 blocker가 하나도 없을 때만 `QUARANTINE_ARTIFACT_CACHE` task 한 건을 만듭니다. 이미 `QUARANTINED`이면 멱등 무변경으로 반환하고, source가 없다고 투영된 `MISSING`은 격리할 수 없습니다.

격리 task가 queued/running인 동안에는 같은 노드의 새 수동 배포 저장·배포 작업·추천 수락·벤치마크·롤백·준비도 만들지 않습니다. 이 역방향 차단은 참조 검사가 끝난 직후 새 current generation이나 host 작업이 끼어드는 경쟁을 막습니다. 격리 task를 임의 취소하거나 DB에서 지워 우회하지 말고 성공 또는 폐쇄형 실패로 정상 종료한 뒤 다음 작업을 다시 요청합니다.

- 해당 노드의 queued/running 준비·배포·벤치마크 또는 다른 활성 task
- 해당 노드를 포함하는 닫히지 않은 deployment operation
- 각 계보의 현재 최신 세대가 exact 캐시를 사용함
- 현재 세대의 direct `VERIFIED` rollback predecessor가 exact 캐시를 사용함
- 수동·불완전 plan이나 준비 snapshot 때문에 exact 참조를 완전하게 판정할 수 없음

Agent는 Dure 배포 컨테이너의 mount를 읽기 전용으로 다시 확인합니다. Docker 조회가 불확실하거나 source와 같거나 상·하위인 활성 mount가 있으면 실패합니다. 안전하면 canonical source 하나를 같은 파일시스템의 `/var/lib/dure/models/.dure-quarantine/<task-uuid>-<cache-kind>-sha256-<identity>`로 `RENAME_NOREPLACE` 원자 이동하고 부모를 `fsync`합니다. target을 덮어쓰거나 source를 재귀 삭제하지 않습니다. 격리 항목은 보존되며 자동 eviction·만료·삭제·P2P 이동은 없습니다.

대표 격리 실패 코드는 다음처럼 대응합니다.

| 실패 코드 | 의미와 조치 |
| --- | --- |
| `ARTIFACT_CACHE_REFERENCES_UNKNOWN`, `ARTIFACT_CACHE_REFERENCED` | 중앙 참조가 불완전하거나 blocker가 있습니다. DB 행을 수정하지 말고 task·operation·세대 계보를 정상 종료한 뒤 preview부터 다시 실행합니다. |
| `ARTIFACT_CACHE_NODE_UNAVAILABLE`, `ARTIFACT_CACHE_AGENT_TOO_OLD` | 노드를 승인·온라인으로 복구하고 Agent를 0.3.20 이상으로 업그레이드합니다. |
| `CACHE_QUARANTINE_ACTIVITY_UNKNOWN`, `CACHE_QUARANTINE_CACHE_ACTIVE` | Docker 조회가 실패했거나 실행 컨테이너가 source를 mount합니다. daemon과 실제 배포 상태를 확인하고 서비스를 명시적으로 종료하기 전에는 재시도하지 않습니다. |
| `CACHE_QUARANTINE_SOURCE_MISSING`, `CACHE_QUARANTINE_SOURCE_UNSAFE`, `CACHE_QUARANTINE_ROOT_UNSAFE` | exact 경로의 부재·소유권·권한·symlink를 조사합니다. 경로를 임의 생성해 성공으로 가장하지 않습니다. |
| `CACHE_QUARANTINE_TARGET_EXISTS`, `CACHE_QUARANTINE_ATOMIC_RENAME_UNAVAILABLE`, `CACHE_QUARANTINE_IO_FAILED` | 기존 보존 대상, 파일시스템 원자 rename 지원과 I/O를 확인합니다. target 삭제·덮어쓰기나 copy fallback을 하지 않습니다. |

격리 task가 실패하면 요청 전의 더 심한 비정상 상태를 낮추지 않고, 그 외에는 `STALE`과 폐쇄형 실패 근거를 유지합니다. 같은 과거 성공을 재전송해 `READY`로 돌릴 수 없으며, 원인을 해결한 뒤 현재 준비 성공으로 복구하거나 새 preview·apply로 다시 격리합니다.

### 단계별 실패 대응 런북

먼저 `dure admin deployment preparation <preparation-id>`로 현재 노드, `MODEL` 또는 `IMAGE` 단계, task ID, 시도 번호와 실패 코드를 기록합니다. 로컬 시도 저널만 보거나 과거 task의 오류만 보고 판단하지 않습니다. 다음 표의 원인을 복구한 뒤, 불변 계획이 그대로인 경우에만 같은 명령과 request ID로 새 시도를 요청합니다.

```bash
dure admin deployment prepare <deployment-id> \
  --request-id <기존-request-uuid> --apply
```

| 실패 단계 | 코드 분류 | 확인할 사항 | 복구·재시도 조건 |
| --- | --- | --- | --- |
| 중앙 사전검사 | `PREPARATION_RECOMMENDATION_REQUIRED`, `PREPARATION_RECOMMENDATION_MISSING`, `PREPARATION_RECOMMENDATION_INVALID`, `PREPARATION_RECOMMENDATION_STALE`, `PREPARATION_ASSIGNMENT_INVALID`, `PREPARATION_NODE_MISSING`, `PREPARATION_NODE_UNAPPROVED`, `PREPARATION_NODE_OFFLINE`, `PREPARATION_AGENT_UNSUPPORTED`, `PREPARATION_PROFILE_STALE`, `PREPARATION_PROFILE_INVALID`, `PREPARATION_INVENTORY_STALE`, `PREPARATION_WORKLOAD_ACTIVE`, `PREPARATION_RUNTIME_UNAVAILABLE`, `PREPARATION_NODE_BUSY`, `PREPARATION_ARTIFACT_STALE`, `PREPARATION_MANIFEST_REQUIRED`, `PREPARATION_MANIFEST_INVALID`, `PREPARATION_IMAGE_INVALID`, `PREPARATION_DISK_INSUFFICIENT` | 수락 추천과 세대, 대상 UUID, 승인·온라인·Agent 버전·프로필·인벤토리, 활성 작업, 매니페스트·다이제스트 이미지와 모델 저장소 용량을 확인합니다. | 일시 상태만 복구했고 저장 계획이 같으면 기존 request ID로 apply합니다. 추천·노드·매니페스트·이미지를 바꿔야 하면 새 추천·배포 세대와 request ID로 preview부터 다시 시작합니다. |
| 중앙 요청·시도 결합 | `PREPARATION_REQUEST_INVALID`, `PREPARATION_REQUEST_CONFLICT`, `PREPARATION_PLAN_CONFLICT`, `PREPARATION_ATTEMPT_CONFLICT`, `DEPLOYMENT_NOT_FOUND`, `ARTIFACT_PREPARATION_NOT_FOUND` | request ID의 정규 UUID·기존 배포 결합, 저장 계획, 현재 task·시도 결합과 감사 이벤트를 확인합니다. | 데이터베이스 행을 직접 수정하거나 다른 계획에 기존 ID를 재사용하지 않습니다. 결합 충돌은 자동 반복하지 말고 서버 버전·트랜잭션 상태를 조사하며, 바뀐 계획에는 새 request ID를 사용합니다. |
| 배포 소비 게이트 | `DEPLOYMENT_ARTIFACTS_NOT_PREPARED`, `DEPLOYMENT_PREPARATION_INVALID`, `DEPLOYMENT_ARTIFACT_CACHE_NOT_READY`, `DEPLOYMENT_MODEL_RELEASE_REVOKED` | 대상 노드별 exact cache status, 현재 모델 시도·최신 이미지 digest 증적, 세대·추천·매니페스트·런타임의 결합과 모델 릴리스 상태를 확인합니다. | 미완료 단계는 기존 준비를 apply해 재시도합니다. `STALE`·`MISSING`·`CORRUPT`·`QUARANTINED`, 손상된 증적이나 `REVOKED` 릴리스를 우회해 apply·start·restart·verify·rollback target 시작을 하지 않습니다. 긴급 stop 경로는 유지됩니다. |
| Agent 계약·설정 | `PREPARATION_PAYLOAD_REJECTED`, `PREPARATION_NODE_MISMATCH`, `PREPARATION_BINDING_MISMATCH`, `PREPARATION_HISTORY_INVALID`, `PREPARATION_ORIGIN_UNAVAILABLE`, `PREPARATION_MANIFEST_UNAVAILABLE` | Agent·controller 버전, task의 노드·단계·시도 결합, `/etc/dure/agent.json`의 `artifact_origin`, 승인·자격 증명과 매니페스트 API 접근을 확인합니다. | secret을 로그에 남기지 말고 설정·버전·신원 원인을 해결합니다. 현재 결과를 수정 재전송하지 않고 기존 준비의 새 시도를 만듭니다. 불변 결합이 달라졌다면 새 준비 요청을 만듭니다. |
| 모델 다운로드 | `MODEL_STORE_DOWNLOAD_TIMEOUT`, `MODEL_STORE_DOWNLOAD_INTERRUPTED`, `MODEL_STORE_DOWNLOAD_REJECTED`, `MODEL_STORE_DIGEST_MISMATCH` | 신뢰 origin의 DNS·TLS·연결과 range·길이 응답 계약, digest object의 원본 바이트를 확인합니다. | timeout·body 중단은 같은 digest를 재시도해 검증된 부분부터 잇습니다. 응답 거부·digest 불일치는 origin object 또는 매니페스트를 바로잡은 뒤 0바이트부터 다시 받습니다. |
| 모델 로컬 저장 | `MODEL_STORE_INVALID`, `MODEL_STORE_ROOT_UNSAFE`, `MODEL_STORE_PATH_COLLISION`, `MODEL_STORE_LOCK_BUSY`, `MODEL_STORE_JOURNAL_CORRUPT`, `MODEL_STORE_CHUNK_COLLISION`, `MODEL_STORE_CHUNK_CORRUPT`, `MODEL_STORE_MANIFEST_MISMATCH`, `MODEL_STORE_CACHE_KIND_UNSUPPORTED`, `MODEL_STORE_FILE_INTEGRITY_FAILED`, `MODEL_STORE_TARGET_COLLISION`, `MODEL_STORE_DISK_INSUFFICIENT`, `MODEL_STORE_IO_FAILED`, `MODEL_STORE_ATOMIC_ACTIVATION_UNAVAILABLE` | 경로 소유권·symlink·동시 준비, exact digest의 CAS·staging·final, 파일시스템 공간·inode·I/O와 no-replace 지원을 확인합니다. | lock 경쟁은 기존 실행 종료 뒤 재시도합니다. 공간·호스트 문제를 복구하고, canonical final 충돌·오염은 중앙 참조를 조사해 공식 quarantine로 보존한 뒤 재시도합니다. 자동 덮어쓰기·재귀 삭제는 금지합니다. |
| Docker 이미지 | `PREPARATION_RUNTIME_UNAVAILABLE`, `PREPARATION_IMAGE_PULL_FAILED`, `PREPARATION_IMAGE_INSPECT_FAILED`, `PREPARATION_IMAGE_DIGEST_MISMATCH` | Docker daemon·Agent 권한, `docker system df`, Docker 데이터 루트, registry·TLS·네트워크·인증, exact digest와 canonical `RepoDigests`를 확인합니다. | daemon·용량·registry 원인을 고친 뒤 기존 request ID로 apply하면 성공한 모델은 재사용하고 실패한 `IMAGE`만 다시 실행합니다. tag·digest 우회, 자동 prune, NVIDIA host driver 자동 변경은 하지 않습니다. |
| 임대·취소·철회·결과 | `PREPARATION_LEASE_EXPIRED`, `PREPARATION_TASK_CANCELED`, `PREPARATION_NODE_REVOKED`, `PREPARATION_RESULT_REJECTED`, `PREPARATION_EXECUTION_FAILED` | 현재 임대와 Agent heartbeat, 호스트 작업의 잔존 여부, 노드 신뢰·자격 증명, Agent·controller 결과 스키마와 등록 매니페스트 증적을 확인합니다. | 아래 fencing 절차에 따라 과거 작업이 더 실행되지 않음을 확인하고 원인을 해결한 뒤 새 시도를 만듭니다. revoke 노드는 자격 증명 교체와 재승인 전에는 재시도하지 않습니다. |

중앙 모델 디스크 검사는 이미지 pull 용량을 보증하지 않습니다. 중앙은 모델 매니페스트의 청크, 전체 조립본과 고정 여유 공간만 계산하며 OCI layer의 압축·unpack 크기, Docker 데이터 루트의 별도 파일시스템과 기존 layer 공유량을 알지 못합니다. 따라서 모델 사전검사를 통과해도 `PREPARATION_IMAGE_PULL_FAILED`가 날 수 있습니다. 운영자는 read-only `docker system df`와 exact digest inspect로 용량과 존재 여부를 확인하고, 수동 정리가 필요하면 Dure 및 다른 컨테이너가 참조하지 않는 정확한 이미지·layer만 대상으로 해야 합니다. `docker system prune` 같은 전역 정리를 자동 복구 절차로 사용하지 않습니다.

### lease, revoke, 늦은 완료와 결과 거부

1. Agent는 완료 결과를 로컬 pending report에 먼저 기록하고, 다음 task를 claim하기 전에 중앙 보고를 다시 시도합니다. 중앙이 완료를 반영했지만 응답만 유실된 같은 현재 시도의 재전송은 멱등 처리됩니다.
2. 임대가 만료되면 중앙은 현재 시도를 `PREPARATION_LEASE_EXPIRED`로 실패 처리합니다. 이후 도착한 과거 완료는 새 상태를 변경하지 못합니다. Agent heartbeat·임대 갱신과 해당 host 작업의 종료를 확인한 뒤 같은 request ID로 apply해 새 시도를 만듭니다.
3. 명시적 취소는 queued task를 `CANCELED`로 닫고, running task는 임대가 만료된 경우에만 만료 실패로 닫습니다. 활성 임대의 host 작업을 중앙 취소가 강제 종료한다고 가정하지 않습니다.
4. 노드를 revoke하면 queued 준비는 취소되고 running 준비는 `PREPARATION_NODE_REVOKED`로 실패합니다. 기존 자격 증명과 늦은 보고는 권한을 회복하지 못합니다. 침해·오등록 원인을 조사하고 자격 증명을 교체한 뒤 중앙에서 다시 승인해야 합니다.
5. task ID, preparation·노드·단계 또는 현재 시도 번호가 다른 늦은 완료·실패는 HTTP `409` 충돌 응답을 받고 최신 상태를 덮어쓰지 않습니다. 과거 결과를 새 시도에 복사하거나 중앙 행을 직접 수정하지 않습니다.
6. `PREPARATION_RESULT_REJECTED`는 결과가 폐쇄형 스키마 또는 등록 매니페스트의 크기·파일 수와 맞지 않는 경우입니다. 중앙은 task·시도를 `FAILED`로 커밋하고 완료 요청에 HTTP `422`를 반환하므로 같은 시도의 JSON을 고쳐 재전송하지 않습니다. Agent·controller 버전, payload/result 결합과 매니페스트를 바로잡은 뒤 기존 준비를 apply해 새 시도로 재시도합니다.
7. `PREPARATION_EXECUTION_FAILED`는 허용 목록 밖의 예외를 안전한 코드로 축약한 값입니다. URL·credential·원격 응답 본문을 중앙 결과에 복사하지 말고 Agent와 Docker·파일시스템 서비스 로그를 접근 통제된 환경에서 조사합니다.

패키지 업그레이드는 `/var/lib/dure`를 `root:dure` `0750`으로 바로잡고 `/var/lib/dure/server`만 `dure` 계정에 쓰기를 허용합니다. 이 경로가 symlink이면 설치를 거부합니다. 중앙 서버의 로컬 SQLite 파일이나 기타 쓰기 상태를 상위 `/var/lib/dure`에 새로 만들지 말고 서버 전용 하위에 둡니다. PostgreSQL 운영에는 영향이 없습니다.

기존 수동 배포 계획의 `/var/lib/dure/models/<model-id>--<revision>` 경로와 새 CAS final `/var/lib/dure/models/sha256-<manifesthex>`는 서로 다른 계약입니다. 수동 legacy deployment의 명시적 경로 호환은 유지하지만, 추천으로 만든 세대는 exact 콘텐츠 주소 cache identity가 `READY`이고 현재 모델 시도·최신 이미지 digest 증적이 모든 노드에서 성공해야 apply·start·restart·verify할 수 있습니다. 상태, 준비 증적, 배포·노드·매니페스트·경로·이미지 결합 중 하나라도 다르면 실패 안전 방식으로 거부합니다.

추천 세대 롤백 대상도 과거에 성공한 같은 준비 증적과 현재 `READY`를 가져야 합니다. 롤백은 기존 검증 캐시와 로컬 다이제스트 이미지만 사용하고 `PREPARE_MODEL`, `PREPARE_IMAGE`, 모델 다운로드나 이미지 pull을 만들지 않습니다. 적용 전뿐 아니라 모든 소스를 중지한 직후 `START_TARGET` task 생성 전에 exact target cache와 최신 이미지 증적을 다시 검사합니다. 사라지거나 손상된 아티팩트는 `ROLLBACK_TARGET_CACHE_NOT_READY`로 실패하고 시작 task는 0개이며, 별도 준비 절차로 복구하기 전까지 소스가 이미 중지된 서비스는 중단될 수 있습니다.

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

기본 개별 BENCHMARK 적용은 `/var/lib/dure/models` 아래의 완전히 펼쳐진 모델과 정확한 `.dure-model.json` metadata, 레지스트리에 고정된 로컬 OCI 이미지를 요구합니다. Agent 0.3.25 이상에서 apply 본문에 `prepare_model=true`, `pull_image=true`를 명시하거나 `dure admin activate --apply`를 사용하면 신뢰 artifact origin에서 exact 모델을 준비하고 digest 이미지를 받은 뒤 실행합니다. 이미지는 `dure-benchmark` 진입점을 제공해야 합니다. Hugging Face hub snapshot의 외부 blob 링크를 직접 실행 경로로 사용하지 않습니다. 다른 활성 LLM 작업 부하·Dure 벤치마크 컨테이너·선택 GPU compute process가 있거나 캐시·프로필 지문·이미지가 다르면 실패합니다. MIG process는 상위 GPU를 판별하지 않고 모두 거부합니다. 가장 큰 정상 GPU 한 장만 UUID로 할당하며, 그 GPU의 compute capability가 런타임의 `gpu_architectures` 폐쇄 목록과 일치해야 합니다. capability가 없거나 미지원이면 준비·적용·증적 등록·승격을 거부합니다.

컨테이너는 `restart=no`로 실행하며 RAM은 현재 전체·가용량 중 작은 값의 절반(최대 32GiB, 계산값이 8GiB 미만이면 거부), swap은 같은 상한, CPU는 논리 CPU의 절반(최대 8코어)으로 제한합니다. 실행 출력은 합계 64KiB로 제한합니다. 재시도 시 폐쇄형 payload와 정확한 대상 노드를 확인한 직후, 현재 Agent 빌드·캐시·프로필·가용 자원·이미지보다 먼저 같은 작업의 컨테이너를 조정합니다. Docker `StartedAt` 기준 900초 측정과 300초 정리 여유를 넘긴 경우에만 레이블을 재확인해 중지·제거하고, 그 뒤 프로필을 새로 조사합니다. inspect·stop·remove나 부재 확인이 불확실하면 작업을 실패로 닫지 않고 다음 임대까지 유예합니다. 같은 요청 ID는 기존 실행을 재사용하고, 새 측정은 새 요청 ID로 준비합니다.

노드 소실로 `QUEUED` 실행의 task가 `RUNNING`에 남으면 활성 임대 중에는 취소 API가 `409`를 반환합니다. 임대 만료를 확인한 뒤 `POST /v1/admin/tasks/{task-id}/cancel`을 명시적으로 호출하면 task를 `CANCELED`, 실행을 `BENCHMARK_CANCELED`로 닫을 수 있습니다. 그 뒤 남은 정확한 벤치마크 컨테이너를 별도로 확인·정리하고 새 요청 ID로 재측정해야 합니다. 통과 증적 뒤의 큐·실행 중 작업 또는 실패·취소 실행이 있는 동안에는 승격할 수 없습니다.

자동 작업과 `dure admin activate`는 단일 노드의 네 가지 고정 작업 부하만 지원합니다. 다중 노드 네트워크·NCCL 실행, 전체 작업 부하 조합과 24시간·복구 수용 검사는 후속 범위입니다. 다중 노드 증적은 현재도 신뢰된 외부 도구로 측정한 뒤 기존 context·증적 API를 통해 등록해야 합니다.

증적 본문에는 프롬프트, 자격 증명, 모델 접근 토큰, 로그, 명령, Docker 인자, 환경 변수, 마운트, 호스트 경로나 자유 형식 metadata를 넣을 수 없습니다. 원본 결과와 큰 로그는 접근 제어된 별도 저장소에 보관합니다.

수동 증적 등록과 승격은 deployment나 task를 만들지 않고 모델 다운로드, 이미지 내려받기, Docker 실행 또는 기존 컨테이너 중지를 수행하지 않습니다. 자동 벤치마크도 준비 단계에는 무변경이며, 명시적 적용 뒤에 격리된 벤치마크 컨테이너 하나만 실행합니다. 어느 경로도 기존 배포를 중지하지 않으며, 승격 후 실제 배포에는 별도의 명시적 생성과 apply가 필요합니다.

0003 마이그레이션은 과거 버전에서 증적 없이 `ACTIVE`가 된 릴리스를 `VALIDATED`로 되돌립니다. 0004는 준비된 벤치마크 실행과 작업·증적 연결을 추가하고, 새 지문에서는 가용 메모리·남은 디스크·현재 작업처럼 순간적인 값을 제외합니다. 0005는 불변 추천 스냅샷과 배포 세대 계보를 추가하고 기존 수동 배포는 `lineage_id=id`로 보정하되 과거 generation과 plan JSON은 변경하지 않습니다. 0006은 배포 operation, 노드별 단계·시도, task 연결과 `verified_at`을 추가합니다. 0008은 배포별 아티팩트 준비, 노드 단계와 시도·task 결합을 추가합니다. 0009는 stage artifact variant, rank별 매니페스트와 검증 증적을 추가합니다. 0010은 노드별 exact cache state와 append-only 상태 event를 추가하며 과거 준비 성공을 추측해 `READY`로 backfill하지 않습니다. 0003에서 저장한 전체 프로필 지문은 당시 저장 프로필이 그대로인 동안 승격 검사에서 계속 인정합니다. 업그레이드 뒤 기존 릴리스의 모든 배치 프로필에 현재 증적을 등록하고 명시적으로 다시 승격해야 합니다. 이 상태 보정과 스키마 추가는 실행 중인 컨테이너에는 손대지 않습니다.

## 추천 스냅샷 수락과 배포 세대

정책 기반 `recommend`, 벤치마크 승격 게이트, exact `STAGE`·독립 `FULL_SNAPSHOT` 선택, 추천 스냅샷 조회, 명시적 수락, 세대별 apply·verify 상태와 rollback은 구현되어 있습니다. 추천은 데이터베이스에 저장된 노드 프로필과 `ACTIVE` 모델 릴리스만 평가합니다.

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

`recommend` 자체는 `PROBE` 작업을 만들거나 인벤토리를 갱신하지 않습니다. 응답에는 콘텐츠 해시 추천 ID, 카탈로그·정책 버전, 인벤토리 지문, 후보별 중앙 릴리스·배치 ID, `model_cache_kind`, exact stage variant·rank 증적과 탈락 사유가 포함됩니다. 서버는 같은 ID의 추천 결과와 지문 계산에 사용한 정규화 인벤토리를 한 행으로 멱등 저장합니다. 자격 증명, 프롬프트, 컨테이너 명령·환경 변수는 이 스냅샷에 포함하지 않습니다. 다중 노드 후보는 정확한 정렬 노드 UUID 조합과 현재 인벤토리에 결합된 최신 통과 네트워크·NCCL 증적이 없으면 실패 안전 방식으로 거부됩니다.

같은 품질에서는 검증된 exact `STAGE`를 먼저 평가하고 variant digest로 순서를 고정하지만, `FULL_SNAPSHOT`도 독립 후보입니다. STAGE는 네트워크 증적의 UUID→rank 결합별로 `2 × rank 전체 바이트 + 64MiB`, FULL은 노드마다 `2 × 전체 snapshot 바이트 + 64MiB`와 배치 프로필 최소 디스크 중 큰 값을 요구합니다. 상위 모델 후보가 GPU·driver·compute capability·runtime architecture·Docker/NVIDIA runtime·디스크·네트워크 조건을 충족하지 못하면 다음 품질의 더 작은 모델이나 다른 노드 조합만 선택할 수 있습니다.

운영 순서는 다음과 같습니다.

1. 최신 `PROBE`로 인벤토리를 갱신합니다.
2. 추천의 후보, 탈락 사유, 모델 리비전, 이미지 다이제스트, `model_cache_kind`, STAGE이면 exact variant·rank·loader·검증 증적, 네트워크 사전 조건을 검토합니다.
3. `recommendation show`로 저장 스냅샷을 확인하고 명시적으로 수락합니다.
4. 서버가 현재 상태와 exact delivery 선택을 다시 평가해 완전히 같을 때만 `CREATED` 배포 세대를 만듭니다.
5. 같은 request ID로 deployment 준비 preview를 검토하고 별도 `--apply`로 모든 노드의 모델·이미지 준비를 완료합니다.
6. `artifact-cache show/verify`로 각 exact identity가 현재 시도에 결합된 `READY`인지 확인한 뒤 별도의 배포 apply와 verify를 진행합니다.
7. 전체 노드 검증으로 `verified_at`을 확보한 세대만 이후 최신 세대의 직접 rollback 대상으로 사용합니다.

수락 시 저장 스냅샷과 현재 추천의 콘텐츠 ID·카탈로그·정책·인벤토리 지문·정규화 인벤토리·선택 결과가 모두 같아야 합니다. 프로필이 오래됐거나 내용이 바뀌고, 노드 승인·연결 상태나 `ACTIVE` 릴리스가 달라졌다면 `409` 응답을 받고 새 `PROBE`와 추천부터 다시 시작합니다. 이전 세대를 지정했다면 해당 계보에서 generation이 가장 큰 최신 행이어야 하고, `ROLLED_BACK` 상태이거나 활성 operation·변경 task가 있으면 안 됩니다. 같은 추천과 같은 이전 세대를 다시 수락하면 기존 세대를 반환합니다.

성공한 롤백의 소스 세대는 `ROLLED_BACK`이 되고 기존 `verified_at`도 제거됩니다. 이 세대를 `--previous-generation`으로 다시 지정해 과거 계보를 연장할 수 없습니다. 롤백 뒤 새 배포를 만들 때는 현재 검증된 구성과 추천을 다시 확인하고 `--previous-generation`을 생략해 generation 1의 새 계보를 시작합니다. 이 제한은 롤백으로 폐기한 세대를 다음 복구 기준으로 자동 부활시키지 않기 위한 실패 안전 정책입니다.

추천과 수락은 자동 다운로드, 이미지 내려받기, 적용, 기존 컨테이너 중지를 의미하지 않습니다. 생성된 세대의 다운로드·pull 플래그는 거짓이며 task나 benchmark run도 만들지 않습니다. 별도 준비 preview도 DB만 변경하고, 준비 적용만 선택된 `/var/lib/dure/models/sha256-<manifesthex>` 또는 rank별 `/var/lib/dure/models/stages/sha256-<cache-identity>`와 다이제스트 고정 로컬 이미지를 준비합니다. 컨테이너 실행과 기존 서비스 변경은 모든 노드의 exact `READY`와 현재 준비 증적을 소비하는 별도 배포 apply에서만 발생합니다. 동일 GPU를 공유하는 파이프라인은 블루/그린 방식이 불가능할 수 있으므로, 실제 무중단 여부를 과장하지 않고 재생성과 복구 절차를 문서화해야 합니다.

`serve=true`인 중앙 apply는 전체 배정 노드 집합에만 허용됩니다. 첫 `APPLY` 단계는 모든 노드에 `serve=false`를 보내 Ray만 준비하고, 모든 노드가 성공한 뒤 Ray head 한 대에만 `START_API`, `VERIFY_API`를 차례로 큐잉합니다. 서로 다른 계보라도 같은 노드를 포함하는 활성 operation이나 배포 task가 있으면 새 작업을 거부하므로 한 GPU에서 두 전환이 교차 실행되지 않습니다.

전체 `VERIFY`가 롤백 증거가 되려면 legacy backend의 각 노드는 `host-gpu`, `container-gpu`, `ray-cluster`를 보고해야 합니다. `VLLM_RAY_PP_V1`은 `ray-cluster` 대신 폐쇄형 `pipeline-rank-contract`를 보고하고 전체 노드가 Agent 0.3.18 이상이어야 하며, head는 `vllm-api`도 성공해야 합니다. API 시작 단계는 별도의 `vllm-api-start`와 `vllm-api` 결과를 모두 요구합니다. 엄격한 결과는 필요한 검사 이름 집합과 정확히 같아야 하므로 필수 검사 누락·중복·알 수 없는 추가 검사·과도한 detail·잘못된 정규 JSON을 성공으로 취급하지 않습니다. 이런 task는 `TASK_RESULT_INVALID`로 실패하며 기존 `verified_at`을 제거하고 해당 노드의 exact 캐시를 `CORRUPT`로 투영합니다.

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

아래 `serve=false` 예시는 legacy backend에만 적용됩니다. `VLLM_RAY_PP_V1`은 준비
요청부터 `serve=true`가 필수이고 `START_API → VERIFY_API`도 선택 단계가 아닙니다.

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
- 소스와 대상의 전체 배정 노드와 실제 실행 토폴로지가 정확히 같습니다. 엄격한 backend에서는 노드·GPU·role·rank·expected runtime rank·runtime address와 backend·vLLM·TP/PP·Ray·network 결합을 비교합니다. 모델·revision·layer 범위·매니페스트·variant와 `FULL_SNAPSHOT`/`STAGE` identity는 세대별 독립 검증과 대상 exact 준비 게이트를 통과하면 달라도 되며, legacy만 layer 범위를 계속 토폴로지로 비교합니다.
- 요청한 중복 없는 정규 UUID 목록이 전체 배정 노드 집합과 정확히 같습니다.
- 모든 노드가 승인 상태이고 최근 30초 안에 온라인으로 관측됐으며 legacy는 Agent
  0.3.12 이상, `VLLM_RAY_PP_V1`은 0.3.18 이상입니다.
- 소스와 대상 이미지가 OCI 다이제스트로 고정돼 있습니다.
- 추천으로 만든 대상 세대라면 모든 노드의 exact cache identity가 현재 `READY`이고 현재 모델 시도·최신 이미지 digest 준비 증적이 성공 상태입니다.
- 같은 계보에 이미 적용 중인 다른 변경이 없습니다.

`--apply` 뒤의 순서는 고정돼 있습니다.

```text
STOP_SOURCE
    ↓ 모든 노드 성공 뒤 target READY·current evidence 재검사
START_TARGET (serve=false)
    ↓ 모든 노드 성공
VERIFY_TARGET
    ↓ 모든 노드 성공
legacy에서 선택적, 엄격 backend에서 필수 START_API (Ray head)
    ↓
legacy에서 선택적, 엄격 backend에서 필수 VERIFY_API (Ray head)
```

한 단계에서 일부 노드가 실패하거나 취소되면 다음 단계로 넘어가지 않습니다. 실행 중 task가 남아 있는 동안에는 재시도를 거부합니다. 특히 모든 `STOP_SOURCE`가 성공한 직후 target exact cache와 최신 이미지 증적을 잠금 상태로 다시 검사합니다. 캐시가 probe·검증·격리로 `READY`가 아니게 됐거나 증적이 오래됐으면 operation과 `START_TARGET` 노드 상태를 `ROLLBACK_TARGET_CACHE_NOT_READY`로 닫고 시작 task를 0개 유지합니다. 원인을 롤백 밖의 지원되는 준비 절차로 복구한 뒤 같은 입력으로 `--apply`를 다시 지정하면 현재 실패 단계만 재시도합니다. 이미 성공한 노드는 반복하지 않고 과거 시도의 늦은 claim·완료·실패 보고는 현재 시도와 맞지 않으므로 무시합니다.

롤백 task는 항상 `accept_model_download=false`, `pull_image=false`를 사용합니다. 추천으로 만든 대상은 기존 exact `READY`와 현재 준비 증적을 다시 사용하며 새 준비 task를 만들지 않습니다. 대상 모델 캐시와 다이제스트 이미지가 모든 노드에 이미 있어야 하며 롤백이 다운로드나 이미지 내려받기를 대신하지 않습니다. 재검사 실패 시 소스가 이미 중지됐을 수 있으므로 복구할 때까지 서비스 중단이 계속될 수 있습니다. 동일 GPU에서 소스 컨테이너를 중지하고 대상 컨테이너를 다시 생성하므로 정상 성공도 블루·그린 전환이 아닙니다. 이 흐름은 다중 노드 네트워크·NCCL 시험이나 24시간 복구 검증을 수행하지 않으므로 실제 GPU 환경의 별도 수용 검사를 계속 진행해야 합니다.

## 업그레이드와 복구

0.3.20에서는 PostgreSQL을 백업하고 controller 코드와 migration 0010을 먼저 적용합니다. 이 migration은 `artifact_preparation_attempts.download_progress`, `node_artifact_caches`, append-only `artifact_cache_events`와 stage variant 복합 참조 제약을 추가하지만 과거 preparation 행을 추측해 진행률이나 `READY`로 backfill하지 않습니다. PostgreSQL 이벤트 테이블에는 `UPDATE`·`DELETE`·`TRUNCATE` 거부 트리거를 설치합니다. 저장소 단위 suite는 SQLite 트리거를 실제 실행하지만 PostgreSQL은 DDL mock/compile까지만 검증하므로, 운영 업그레이드에서는 별도 PostgreSQL staging DB에서 migration과 거부 쿼리를 실행해 확인합니다. controller 시작 뒤 새 모델 준비 성공으로 필요한 exact cache row가 생성되는지 확인합니다. 이어서 Agent를 작은 batch로 0.3.20 이상에 올리고 새 probe에서 `artifact_cache_observations`와 `artifact_cache_scan_complete`가 함께 보고되는지 확인합니다. legacy·불완전 조사는 상태를 강등하지 않습니다.

격리를 시작하기 전에 `artifact-cache quarantine <cache-id>` preview가 task 0개인지 확인하고, current generation·direct verified rollback predecessor·활성 준비/배포/벤치마크·operation blocker를 의도대로 표시하는지 검토합니다. `--apply`는 Agent 0.3.20 이상에서만 사용합니다. `/var/lib/dure/models/.dure-quarantine`은 보존 영역이며 업그레이드 스크립트나 Agent가 자동 삭제하지 않습니다.

0010 downgrade는 `node_artifact_caches` 또는 `artifact_cache_events`에 행이 하나라도 있거나 `artifact_preparation_attempts.download_progress`에 SQL `NULL`·JSON `null`이 아닌 진행 데이터가 하나라도 있으면 감사·상태 손실을 피하기 위해 거부합니다. PostgreSQL에서는 관련 세 테이블을 write 순서대로 잠근 뒤 검사합니다. downgrade를 위해 캐시 상태·event·진행 데이터를 수동 삭제하거나 외래 키를 우회하지 말고, 백업과 별도 이관 계획이 없다면 0010을 유지합니다.

0.3.19에는 데이터베이스 migration이 없습니다. 기존 0009 stage 레지스트리와 0008 준비 증적을 사용합니다. controller를 먼저 올린 뒤 대상 Agent를 작은 batch로 0.3.19 이상에 올리고, 새 heartbeat·probe에서 버전과 rank별 `STAGE` marker projection을 확인합니다. Agent가 0.3.18이면 기존 `FULL_SNAPSHOT` 엄격 실행은 유지할 수 있지만 `--stage-variant` 준비·시작·재시작·롤백 대상 시작은 거부됩니다. 첫 STAGE 적용 전에 preview가 task 0개인지, 각 rank 매니페스트가 기대 UUID에 결합됐는지, 실제 GPU harness 기본값이 `NOT_RUN(77)`인지 확인합니다.

0.3.18에는 데이터베이스 migration이 없습니다. controller를 먼저 업그레이드한 뒤 Agent를 작은 batch로 0.3.18 이상에 올리고 새 heartbeat의 `agent_version`, UUID와 사설 주소를 확인합니다. 기존 local/legacy 계획은 계속 사용할 수 있지만 `VLLM_RAY_PP_V1` 비중지 작업은 전체 배정 노드가 0.3.18 이상일 때만 만듭니다. 전체 노드 업그레이드와 `FULL_SNAPSHOT` 준비가 끝나기 전에 엄격한 다중 노드 apply를 시작하지 않습니다.

업그레이드 뒤 실제 2·3노드 수용 검사 설정을 계획에서 새로 만들고 기본 실행이 `NOT_RUN`·77인지 먼저 확인합니다. 실제 GPU opt-in 결과가 `PASSED`가 아니면 이를 CI 통과로 바꾸지 말고 기존 검증 세대를 유지합니다. Dure 패키지는 NVIDIA driver를 바꾸지 않으므로 각 노드의 driver와 digest 고정 vLLM 이미지 호환성은 운영자가 별도 점검합니다.

0.3.17에서는 PostgreSQL을 백업하고 controller 코드와 migration 0009를 먼저 적용합니다. stage variant 테이블과 기존 매니페스트·모델·런타임 외래 키를 확인한 뒤 서버를 시작합니다. 이 migration과 DRAFT 등록은 실행 중인 컨테이너나 노드 캐시를 변경하지 않습니다. 기본 Debian Agent에는 builder heavy dependency가 추가되지 않으므로 stage 생성이 필요하면 운영 환경과 분리한 digest 고정 OCI builder를 별도로 준비합니다. 실제 GPU 검증이 `NOT_RUN`이면 배포 가능으로 해석하지 말고 `DRAFT`를 유지합니다.

0009 downgrade는 variant, rank, 검증 증적 또는 증적 rank 테이블에 데이터가 하나라도 있으면 거부합니다. `REVOKED` 상태도 감사 데이터이므로 빈 것으로 취급하지 않습니다. downgrade를 위해 이 행을 수동 삭제하거나 외래 키를 우회하지 말고 백업을 확인한 뒤 0009를 유지하거나 별도 감사·이관 절차를 준비합니다.

0.3.16에서는 PostgreSQL을 백업하고 controller 코드와 migration 0008을 먼저 적용한 뒤 준비 API를 확인합니다. 이어서 각 GPU 노드의 `/etc/dure/agent.json`에 신뢰 origin을 root 권한으로 설정하고 Agent를 작은 batch로 업그레이드·재시작합니다. `PREPARE_MODEL`과 `PREPARE_IMAGE`는 0.3.16 Agent만 처리할 수 있으므로 모든 대상 노드의 버전과 새 heartbeat를 확인하기 전에는 준비 적용을 시작하지 않습니다. 설정 파일에는 기존 node credential이 있으므로 복사·출력하지 않습니다.

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

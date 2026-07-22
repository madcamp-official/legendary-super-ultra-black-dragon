# 설정 참조서

이 문서는 Dure의 설정 파일, `DURE_*` 환경 변수, systemd 주입, GPU acceptance·stage builder opt-in의
용도를 한 곳에 정리한다. 값의 실제 우선순위와 보안 경계는 [운영 절차](operations.md),
[Agent 설정과 credential 회전 운영 절차](agent-operations.md), [보안 모델](security.md)이 기준이며,
이 문서는 그 문서를 대체하지 않는다.

## 설정 영역 요약

| 영역 | 일반 위치 | 쓰는 주체 | 비밀 포함 여부 | 비고 |
| --- | --- | --- | --- | --- |
| Controller service | `/etc/dure/server.env` | `dure-server` systemd service | 예 | 저장소·Agent node·ticket에 복사 금지 |
| 관리자 CLI dotenv | `dure/.env`, `.env` 또는 `dure admin --env-file` | 운영자 workstation | 예 | 현재 사용자 소유, group/other 접근 금지 |
| Agent join 전 client 설정 | `/etc/dure/dure-client.env` | `dure join` | 아니오 | `DURE_SERVER`, `DURE_INSECURE`만 허용 |
| Agent identity | `/etc/dure/agent.json` | `dure-agent` | 예 | root 소유 `0600`, 직접 출력·복사 금지 |
| 개발·수동 server dotenv | 명시한 `--env-file` 또는 탐색된 `.env` | 수동 `dure-server` | 예 | production systemd는 작업 디렉터리 탐색에 의존하지 않음 |
| stage builder/acceptance | 신뢰된 격리 builder 또는 root 보호 acceptance 환경 | 수동 builder·harness | 일부 identity 값 | Agent task·일반 shell profile·CI secret으로 재사용 금지 |

`/etc/dure/agent.json`과 `/etc/dure/server.env`는 문서 예시·issue·PR·CI log에 넣지 않는다. 모델
access token, Docker command, prompt, private URL도 Dure 설정 참조에 추가하면 안 된다.

## Control Plane과 관리자 CLI

| 이름 | 소비자 | 민감도 | 기본값·필수 여부 | 우선순위 |
| --- | --- | --- | --- | --- |
| `DURE_DATABASE_URL` | `dure-server`, Alembic | 높음: DB credential 포함 가능 | server DB URL. migration 전용이 아니면 DB 연결 가능해야 함 | `--database-url` → 선택 dotenv → process env → 내장 기본값 |
| `DURE_ADMIN_TOKEN` | `dure-server`, `dure admin` | 높음 | server는 migration 전용 외 필수, admin CLI도 필수 | server: 선택 dotenv → process env. admin: `--token` → 선택 dotenv → process env |
| `DURE_SERVER` | `dure admin`, `dure join` | 중간: URL·network 정보 | admin과 join의 Control Plane 주소 | admin: `--server` → 선택 dotenv → process env → package 기본 주소. join: `--server` → process env → `/etc/dure/dure-client.env` |

### Controller service

`dure-server` systemd service는 `/etc/dure/server.env`를 service manager가 process environment로 주입한다.
이 파일은 root/서비스 관리자만 읽을 수 있게 관리하며, `DURE_DATABASE_URL`과 `DURE_ADMIN_TOKEN`을
같이 제공한다. production Controller는 loopback `127.0.0.1:8081`에 bind하고 TLS reverse proxy 뒤에
둔다. `8081`, PostgreSQL, Ray port를 공용 인터넷에 열지 않는다.

### 관리자 CLI dotenv

`dure admin`은 두 값을 함께 가진 owner-only dotenv를 읽는다. 저장소 최상위에서 실행하면 먼저
`dure/.env`, 그다음 `.env`를 탐색하며, Dure 디렉터리 안에서는 해당 `.env`를 탐색한다. 다른 파일은
다음처럼 명시한다.

```bash
dure admin --env-file /secure/path/dure-admin.env nodes --online
```

dotenv parser는 shell을 source하지 않고 `DURE_SERVER`와 `DURE_ADMIN_TOKEN`만 읽는다. shell expansion,
command substitution, 다른 변수는 실행·상속하지 않는다. 파일을 발견한 뒤 file 값과 process environment의
값을 섞지 않는다.

## Agent join과 runtime

| 이름·파일 | 소비자 | 민감도 | 값·기본 | 우선순위·주의 |
| --- | --- | --- | --- | --- |
| `DURE_SERVER` | `dure join` | 중간 | HTTPS Controller URL | `--server` → process env → `/etc/dure/dure-client.env` |
| `DURE_INSECURE` | `dure join` | 낮음, 보안상 위험 | 기본 `false`; `true`면 TLS 검증 해제 | `--insecure` → process env → client env. 개발 전용 |
| `/etc/dure/agent.json`의 `server`, `credential`, `verify_tls` | 실행 중 `dure-agent` | 높음 | join이 생성 | join 뒤 client env만 바꿔도 Agent runtime은 바뀌지 않음 |
| `DURE_BUILD_COMMIT` | editable/wheel Agent의 새 benchmark 실행 | 중간: build identity | 40~64자리 trusted commit 필요 | 공식 Debian package는 `/usr/share/dure/build-commit`을 사용. 일반 환경은 명시하지 않으면 새 benchmark 거부 |

`DURE_INSECURE=true`는 production Agent에서 사용하지 않는다. Controller URL·TLS를 바꾸려면 다른 node의
identity file을 복사하지 말고 `sudo dure unjoin` 뒤 새 설정으로 `sudo dure join`한다. credential 회전은
`agent.json`의 credential만 안전한 경로에서 바꾸고, root 소유 `0600`과 heartbeat를 다시 확인한다.

## stage builder와 GPU acceptance opt-in

다음 값은 일반 Dure daemon 설정이 아니다. 신뢰된 digest-pinned builder 또는 격리된 GPU acceptance
환경에서만 명시하며, 중앙 task payload·DB·Agent 설정에 넣지 않는다.

| 이름 | 용도 | 필수 조건 |
| --- | --- | --- |
| `DURE_STAGE_RUNTIME_IMAGE` | stage builder가 실제로 실행할 OCI image identity | `repository@sha256:...` 형식, variant contract와 정확히 일치 |
| `DURE_STAGE_EXPORTER_BUILD_DIGEST` | stage exporter build identity | contract와 정확히 일치 |
| `DURE_RUN_STAGE_GPU_ACCEPTANCE=1` | stage builder GPU acceptance opt-in | 추가 load opt-in과 root 보호 input/output 필요 |
| `DURE_STAGE_ACCEPTANCE_LOAD=1` | stage export 뒤 native load·최소 추론 opt-in | `DURE_RUN_STAGE_GPU_ACCEPTANCE=1`과 함께 사용 |
| `DURE_STAGE_ACCEPTANCE_SOURCE` 등 `DURE_STAGE_ACCEPTANCE_*` | stage acceptance의 local source·manifest·digest·output·PP | trust된 local 경로와 정규 manifest 필요, `PP=1` native acceptance만 지원 |
| `DURE_RUN_VLLM_RAY_PP_ACCEPTANCE=1` | `FULL_SNAPSHOT` 다중 노드 GPU harness opt-in | root 보호 acceptance config, digest-pinned wrapper, exact 2·3 node binding 필요 |
| `DURE_RUN_VLLM_STAGE_RAY_PP_ACCEPTANCE=1` | rank별 `STAGE` 다중 노드 GPU harness opt-in | exact stage rank cache·binding과 digest-pinned wrapper 필요 |
| `DURE_VLLM_RAY_PP_ACCEPTANCE_*` | `FULL_SNAPSHOT` harness가 감지하면 거부하는 예약 접두사 | opt-in 외의 환경 변수로 command·Docker 인자·mount·host path를 주입할 수 없음 |
| `DURE_VLLM_STAGE_RAY_PP_ACCEPTANCE_*` | `STAGE` harness가 감지하면 거부하는 예약 접두사 | opt-in 외의 환경 변수로 rank binding·command·mount를 바꿀 수 없음 |

acceptance harness는 기본적으로 `NOT_RUN`·종료 코드 `77`을 반환하며, 실제 실행 시작 뒤 오류는
`FAILED`다. 환경 변수로 image digest·node binding·모델 계약을 임의로 바꾸는 경로가 아니며, 실제
결과는 [릴리스 증적 기록](release-evidence/README.md)에 별도 기록한다. 상세 입력은
[vLLM 단계 아티팩트](stage-artifacts.md)와 [릴리스 수용 검증](release-validation.md)을 따른다.

코드 내부의 `DURE_MODEL_CACHE_ROOT`, `DURE_STAGE_ROOT`처럼 `DURE_`로 시작하는 상수는 환경 변수
설정 항목이 아니다. 현재 모델 store·cache 기본 경로는 `/var/lib/dure/model-store`,
`/var/lib/dure/models`로 코드에 고정되어 있으며, 임의 환경 변수로 재지정하지 않는다.

## 파일 권한과 변경 절차

| 대상 | 권장 권한 | 변경 방식 |
| --- | --- | --- |
| `/etc/dure/server.env` | root/서비스 관리자 전용, group/world 읽기 금지 | secrets manager 또는 `sudoedit`, 변경 뒤 Controller health 확인 |
| 관리자 dotenv | 현재 사용자 소유, `0600` | `--env-file` 우선, token을 terminal·shell history에 넣지 않음 |
| `/etc/dure/dure-client.env` | root 소유, 일반 사용자 쓰기 금지 | join 전 Controller URL·TLS만 설정 |
| `/etc/dure/agent.json` | root 소유 `0600` | join·안전한 credential rotation 외 수동 복제 금지 |
| acceptance 설정·builder input | root 소유, group/world writable 금지 | 격리 환경에서만 작성·실행 |

설정 변경은 deployment·benchmark·migration과 같은 유지보수 창에 묶어 추측 적용하지 않는다. Controller
주소, credential, runtime image, acceptance binding이 바뀌면 [버전 호환성과 롤링 업그레이드](compatibility-upgrades.md),
[Agent 설정과 credential 회전 운영 절차](agent-operations.md), [네트워크·방화벽 운영 절차](networking.md)의
재검사 조건을 따른다.

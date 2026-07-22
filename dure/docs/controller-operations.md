# Controller 운영

이 문서는 Control Plane, PostgreSQL, 관리자 credential, GPU node 등록·승인의 일상 절차를 다룹니다.
배포·Fleet 실행은 [배포·Fleet 운영](deployment-operations.md)을, Agent 설정은
[Agent 운영](agent-operations.md)을 따릅니다.

## Controller 시작 전

1. `/etc/dure/server.env`의 `DURE_DATABASE_URL`, `DURE_ADMIN_TOKEN`을 root/서비스 관리자만 읽게 관리합니다.
2. PostgreSQL backup과 migration 상태를 확인합니다.
3. production Controller는 loopback `127.0.0.1:8081`에 bind하고, TLS reverse proxy와 관리망 접근 제어를 사용합니다.
4. PostgreSQL, Ray GCS, worker, OpenAPI 문서 경로를 public Internet에 노출하지 않습니다.

```bash
sudo systemctl status dure-server --no-pager
curl -fsS http://127.0.0.1:8081/health
sudo journalctl -u dure-server -n 100 --no-pager
```

설정 우선순위와 권한은 [설정 참조서](configuration-reference.md), DB backup·복구는
[재해 복구](disaster-recovery.md)를 기준으로 합니다.

## 관리자 CLI

관리자 CLI는 `DURE_SERVER`와 `DURE_ADMIN_TOKEN`을 권한 제한 dotenv 또는 `--env-file`에서 읽습니다.
token을 terminal history, ticket, PR, 로그에 넣지 않습니다.

```bash
dure --env-file /secure/path/dure-admin.env admin nodes --online
dure --env-file /secure/path/dure-admin.env admin nodes --pending
```

정확한 명령 인자와 preview/apply 조건은 [CLI 명령 참조](cli-reference.md)를 사용합니다.

## 노드 등록과 승인

1. GPU host에서 `dure doctor --json`, `dure bootstrap`으로 전제 조건을 진단합니다.
2. server owner가 필요할 때만 `sudo dure bootstrap --apply`를 실행합니다. Controller나 Agent는 원격 설치를 하지 않습니다.
3. node에서 `dure join`을 실행하면 node는 `pending`으로 등록됩니다.
4. 운영자는 GPU·runtime·네트워크·인벤토리를 검토한 뒤 node를 명시적으로 승인합니다.
5. 승인 뒤 heartbeat와 task claim 상태를 확인합니다.

pending node는 heartbeat할 수 있지만 task를 claim할 수 없습니다. revoke·unjoin·credential 회전은
[Agent 운영](agent-operations.md)과 [노드 수명주기](node-lifecycle.md)를 따라 수행합니다.

## 운영 중단 기준

- Controller health 또는 PostgreSQL 연결이 실패하면 apply·rollback을 반복하지 않습니다.
- admin token 또는 node credential 노출이 의심되면 관리 API 접근을 제한하고 rotation 절차를 시작합니다.
- node가 offline이거나 pending이면 배포 대상으로 해석하지 않습니다.

상세 HTTP 오류·인증 경계는 [API 계약](api-contract.md), 상태 보존과 incident 대응은
[관측·장애 대응](observability.md)을 따릅니다.

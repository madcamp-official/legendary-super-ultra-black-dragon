# 외부 추론 API 경계

## 현재 상태

Dure는 현재 공개 inference gateway, 최종 사용자 계정·API key, quota, billing, rate limit, tenant
격리 기능을 제공하지 않는다. vLLM API는 배포 head host의 loopback `127.0.0.1:8000`에만 bind하는
내부 runtime endpoint이며, Dure Control Plane의 `/v1/admin/*` API는 운영자 관리용 bearer token
endpoint다. 두 API는 같은 공개 endpoint나 credential으로 제공해서는 안 된다.

이 문서는 외부 endpoint를 이미 제공한다는 사용 설명서가 아니다. 외부 inference를 검토할 때 금지된
노출 방식과 별도 gateway가 갖춰야 하는 최소 경계를 정의한다.

## 절대 공개하지 않는 endpoint

다음 port와 경로는 public Internet에 직접 노출하면 안 된다.

| 대상 | 금지 이유 |
| --- | --- |
| vLLM `127.0.0.1:8000` | 사용자 인증·quota·rate limit·tenant 경계가 없음 |
| Controller `8081`, `/v1/admin/*` | node 승인, artifact, deployment, task를 관리하는 privileged API |
| FastAPI `/docs`, `/redoc`, `/openapi.json` | 관리 API의 schema·운영 표면을 노출 |
| Ray GCS `6379`, worker `20000-21000`, dashboard | 내부 distributed runtime이며 사용자 API가 아님 |
| PostgreSQL | Control Plane 상태와 credential 경계 |

이 port를 공용 security group, host firewall 또는 reverse proxy route로 열어 문제를 해결하면 안 된다.
현재 포트·network 경계는 [네트워크·방화벽 운영 절차](networking.md)를 따른다.

## 외부 gateway에 필요한 최소 조건

외부 inference가 필요하면 Dure 밖의 별도 gateway 또는 service layer를 설계·승인해야 한다. 최소한 다음을
충족하기 전에는 `8000`을 proxy pass하거나 Control Plane token을 client에 배포하지 않는다.

1. **분리된 인증**: 사용자 identity·API key·service credential은 `DURE_ADMIN_TOKEN`, enrollment token,
   node credential과 독립적으로 발급·회전·폐기한다.
2. **권한·quota·rate limit**: tenant별 모델 허용 목록, 요청 크기·token·동시성 제한, 일·월 quota, abuse
   차단과 audit가 있어야 한다. Dure의 Fleet scheduler나 benchmark SLO가 이를 대신하지 않는다.
3. **안전한 routing**: gateway는 승인된 active deployment의 내부 endpoint에만 연결하고, unhealthy 또는
   변경 중인 generation을 사용자 traffic에 자동으로 붙이지 않는다. Dure의 recommendation·accept는
   traffic cutover 권한이 아니다.
4. **데이터 보호**: TLS, secret store, request/response body 로그 기본 비활성화, redaction, 보존·삭제
   정책, data classification을 구현한다. 상세 기준은 [개인정보·프롬프트 처리 정책](data-privacy.md)을
   따른다.
5. **network 분리**: public ingress는 gateway까지로 한정하고, gateway→vLLM은 사설 network·allowlist·
   최소 port로 제한한다. gateway가 Ray·Controller·PostgreSQL에 직접 접근할 필요가 없어야 한다.
6. **관측·incident 대응**: request ID, aggregate token/latency/error metric, rate-limit·auth failure를
   안전하게 관찰하고, credential·prompt·completion이 log에 남지 않도록 한다. 노출 시 ingress 차단과
   credential rotation 절차를 사전에 검증한다.

## 배포 전 승인 체크리스트

외부 endpoint는 다음 승인이 모두 끝난 뒤에만 검토한다.

- gateway threat model, data classification, model license와 사용자 약관 검토
- user auth·key rotation·revocation·quota·rate limit의 실제 구현과 abuse test
- public ingress, private backend, admin API, Ray, PostgreSQL의 firewall·security group 검증
- prompt·completion·Authorization header가 reverse proxy·application·observability log에 남지 않는지 검증
- 모델별 input/output·context·concurrency 제한과 overload·timeout·backpressure 정책 확인
- backend generation failure, model withdrawal, credential 노출, logging 오설정의 incident runbook 검증
- 실제 load·security test 결과와 변경 승인을 별도 evidence에 기록

모두 충족하지 못하면 gateway가 아니라 신뢰된 운영자망의 loopback 또는 private service-to-service
경로만 유지한다.

## 현재 Dure와의 통합 금지 사항

- client가 `dure admin` CLI, `DURE_ADMIN_TOKEN`, enrollment token, Agent credential을 사용하게 하지 않음
- gateway 요청에서 Docker option, environment, mount, host path, model revision을 임의 전달하지 않음
- client traffic을 근거로 profile을 자동 `ACTIVE`로 승격하거나 Fleet recommendation을 자동 수락하지 않음
- 배포 실패 시 다른 model·cache kind·node로 자동 fallback·rollback한다고 사용자에게 약속하지 않음
- API key·prompt·completion을 artifact manifest, benchmark evidence, task payload, deployment plan에 저장하지 않음

## 사건 대응

외부 endpoint가 실수로 열렸거나 prompt·credential 노출이 의심되면 gateway ingress와 public route를
우선 차단한다. 이어서 영향받은 secret을 회전하고, 관련 node·deployment·log sink의 범위를 조사한다.
Ray·vLLM·Controller port를 더 넓게 열어 복구하지 않으며, 실제 실행 상태와 검증된 generation을 확인한
뒤 명시적으로 복구한다. 자세한 prompt incident 절차는 [개인정보·프롬프트 처리 정책](data-privacy.md),
운영 신호는 [관측·장애 대응 운영 절차](observability.md)를 따른다.

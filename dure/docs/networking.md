# 네트워크·방화벽 운영 절차

이 문서는 신뢰된 사설망에서 Dure Control Plane, Agent, Ray/vLLM 배포를 운영할 때의
통신 경계와 검증 절차를 정리한다. Dure는 방화벽·보안 그룹·WireGuard를 자동으로 만들거나
수정하지 않는다. 아래 표의 허용 규칙은 운영자가 사용하는 클라우드 보안 그룹, 호스트 방화벽,
사설망 또는 오버레이에 명시적으로 반영해야 한다.

현재 다중 노드 실행 계약은 `VLLM_RAY_PP_V1`, vLLM 0.9.0 V0 Ray, `TP=1`, `PP=2/3`에
한정된다. 다중 노드 네트워크·NCCL qualification은 Agent가 자동으로 수행하지 않으며, 신뢰된
외부 executor가 측정한 증적을 등록해야 한다. 지원 범위는 [지원 매트릭스](support-matrix.md)를
기준으로 한다.

## 기본 원칙

- Controller는 Agent에 SSH나 임의 inbound 연결을 열지 않는다. 각 Agent가 Controller로 outbound HTTPS 연결을 시작한다.
- Ray, NCCL/Gloo, vLLM worker 통신은 정확히 선택된 배포 노드끼리의 신뢰 LAN 또는 사설 오버레이에서만 허용한다. 공인 인터넷에 직접 노출하면 안 된다.
- 운영자 UI·관리 API가 필요한 경우에도 인터넷에 Controller `8081`을 직접 열지 않는다. TLS 종료와 접근 제어를 담당하는 reverse proxy의 HTTPS `443`만 선택적으로 공개한다.
- 계획에 고정된 `network_interface`를 모든 rank에서 사용한다. runtime은 그 값을 `NCCL_SOCKET_IFNAME`과 `GLOO_SOCKET_IFNAME`에 동일하게 전달한다. 서로 다른 노드에서 다른 인터페이스를 추측하거나 자동 선택하지 않는다.
- Dure 컨테이너, Ray GCS/dashboard, worker 포트, PostgreSQL, vLLM API는 일반 사용자나 공용 네트워크에 공개하지 않는다.

## 포트와 허용 방향

| 목적 | 출발 → 도착 | 프로토콜·포트 | 허용 범위 | 공개 여부 |
| --- | --- | --- | --- | --- |
| 운영자 HTTPS (선택) | 승인된 운영자망 → reverse proxy | TCP `443` | VPN·관리망·허용된 IP만 | 제한적으로 가능 |
| reverse proxy → Controller | 동일 host 또는 사설망 | TCP `8081` | proxy만 | 공개 금지 |
| Agent 제어 통신 | 각 GPU/CPU Agent → Controller | HTTPS `443` 또는 사설 reverse-proxy 경로 | Controller origin만 | Agent outbound만 |
| Controller 상태 점검 | Controller host → Controller | TCP `127.0.0.1:8081` | loopback | 공개 금지 |
| Ray GCS | pipeline worker → head | TCP `6379` | 정확한 배포 노드 집합 | 공개 금지 |
| Ray worker | head·worker 상호 간 | TCP `20000-21000` | 정확한 배포 노드 집합 | 공개 금지 |
| vLLM API | head host 내부 | TCP `127.0.0.1:8000` | loopback | 공개 금지 |
| NCCL/Gloo | pipeline rank 상호 간 | 계획의 사설 인터페이스를 통한 runtime 통신 | 정확한 배포 노드 집합 | 공개 금지 |
| PostgreSQL | Controller → PostgreSQL | 배포 환경의 PostgreSQL 포트 | Controller와 복구 환경만 | 공개 금지 |

NCCL/Gloo의 실제 transport 포트와 연결 수는 NIC, NCCL 버전, 컨테이너 runtime, 네트워크 정책에 따라 달라질 수 있다. 그러므로 공용 망에 넓은 포트 범위를 여는 방식으로 해결해서는 안 된다. 선택된 노드만 들어 있는 사설 L2/VPC 구간 또는 WireGuard 같은 오버레이 안에서 통신을 허용하고, 정확한 노드 집합으로 수집한 최신 네트워크·NCCL 증적을 qualification에 사용한다.

## 권장 배치

1. Controller와 PostgreSQL은 관리용 사설 subnet에 둔다.
2. Agent 노드는 Controller의 HTTPS origin에 outbound로만 연결할 수 있게 한다.
3. 다중 노드 pipeline 후보는 같은 신뢰 네트워크 영역에 배치한다. 서로 다른 영역을 연결해야 하면 WireGuard 등으로 운영자가 관리하는 private overlay를 먼저 구성한다.
4. Controller 서비스는 production에서 loopback `127.0.0.1:8081`에 bind하고 reverse proxy가 인증·TLS·접근 제어를 담당하게 한다. 패키지의 `0.0.0.0:8081` service template은 개발/LAN 시작점일 뿐 public production 기본값이 아니다.
5. vLLM API는 head의 loopback `127.0.0.1:8000`에만 bind한다. 외부 inference endpoint가 필요하면 별도 인증·rate limit·감사 설계를 거친 proxy를 추가하며, Dure Control Plane API와 같은 공개 endpoint로 합치지 않는다. 현재 지원하지 않는 공개 gateway의 필수 조건은 [외부 추론 API 경계](external-inference-boundary.md)를 따른다.
6. reverse proxy가 Control Plane에 연결되더라도 FastAPI schema 경로 `/docs`, `/redoc`, `/openapi.json`은 인터넷에 공개하지 않는다. VPN·관리 IP로 제한하거나 외부 route에서 `403` 또는 `404`로 차단한다.

Controller bind를 바꾸는 예시는 다음과 같다. 실제 reverse proxy 설정·인증서·방화벽 설정은 환경별로 별도 검토한다.

```bash
sudo systemctl edit dure-server
```

```ini
[Service]
ExecStart=
ExecStart=/usr/bin/dure-server --host 127.0.0.1 --port 8081
```

```bash
sudo systemctl daemon-reload
sudo systemctl restart dure-server
curl -fsS http://127.0.0.1:8081/health
```

## 배포 전 점검

1. **주소와 인터페이스를 고정한다.** 각 노드의 Dure UUID, RFC1918 주소, 계획의 `network_interface`가 실제 사설 NIC와 일치하는지 확인한다. 공인 IP나 임시 인터페이스를 rank 주소로 사용하지 않는다.
2. **허용 범위를 최소화한다.** `6379`, `20000-21000`은 해당 deployment의 노드끼리만 허용한다. 다른 Dure deployment, 일반 LAN host, 인터넷 CIDR에는 열지 않는다.
3. **점유 상태를 확인한다.** 시작 전 각 노드에서 `ss -ltnp`로 `6379`, `20000-21000`, `8000`, `8081`의 기존 점유자를 조사한다. 다른 서비스가 사용 중이면 그 서비스를 임의로 중지하지 말고 포트 충돌과 배포 세대 상태를 해결한다.
4. **보안 그룹과 host firewall을 함께 확인한다.** 한쪽만 열어도 통신이 실패할 수 있다. Docker 설치·재시작은 netfilter 및 forwarding 동작에 영향을 줄 수 있으므로 bootstrap 전후에 규칙을 다시 확인한다.
5. **NCCL 증적을 만든다.** 정확한 노드 UUID 집합, GPU UUID, interface, runtime digest, driver/runtime 지문, RTT·대역폭과 NCCL 결과를 함께 기록한다. 다른 노드를 하나라도 바꾸면 기존 증적을 재사용하지 않는다.

WireGuard를 쓰는 경우에는 터널이 올라와 있는지 `wg show`로 확인하고, Dure 계획의 `network_interface`가 터널 또는 그 위에서 통신 가능한 사설 NIC와 의도대로 일치하는지 확인한다. WireGuard 자체의 키 배포·피어 승인·라우팅은 Dure의 범위가 아니다.

## 장애 분류와 대응

| 증상 | 먼저 확인할 항목 | 대응 원칙 |
| --- | --- | --- |
| Agent가 heartbeat하지 못함 | Controller HTTPS origin, DNS/인증서, outbound 정책, Agent credential | Controller inbound SSH를 열지 말고 Agent의 outbound 경로와 credential을 복구 |
| Ray worker가 head에 join하지 못함 | 정확한 head RFC1918 주소, `6379`, `20000-21000`, 중복 Ray process | 노드 집합 밖의 허용 규칙을 넓히지 말고 충돌 process·주소·세대 상태를 조사 |
| NCCL 초기화 실패 | 모든 rank의 동일 interface, private overlay 경로, driver/runtime 조합, 최신 exact 증적 | 임의 환경 변수나 공용 포트 개방으로 우회하지 말고 해당 조합을 qualification 실패로 기록 |
| API가 외부에서 접근됨 | `ss -ltnp`, reverse proxy route, cloud security group | `8000`, `8081`, Ray 포트를 즉시 공개 경로에서 제거하고 노출된 credential을 회전 |
| Docker 적용 뒤 통신 정책 변화 | host firewall, forwarding, 보안 그룹, 실행 중 컨테이너 영향 | bootstrap이 방화벽을 보정한다고 가정하지 말고 변경 전후 규칙을 비교·검증 |

관련 절차는 [운영 절차](operations.md), [보안 모델](security.md), [자동 배치 프로필 qualification](profile-qualification.md)을 함께 따른다.

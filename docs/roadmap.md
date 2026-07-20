# Dure 개발 로드맵

기준일: 2026-07-19
현재 개발 브랜치: `version/0.3.3`

## 방향과 원칙

Dure의 다음 목표는 기능 수를 빠르게 늘리는 것이 아니라, 현재의 trusted-LAN MVP를 실제
3-GPU 환경에서 반복 배포 가능한 운영 단위로 만드는 것이다. 작업 순서는 다음 원칙을 따른다.

1. 수동 파일 교환과 환경별 하드코딩을 먼저 제거한다.
2. 공개 API보다 노드·모델 배포의 재현성, 보안, 장애 복구를 먼저 검증한다.
3. Ray는 신뢰된 pod 내부에만 두고 등록·인증·스케줄링은 Control Plane이 담당한다.
4. 각 버전은 exit criteria를 모두 통과한 후 사용자의 명시적 요청으로만 `main`에 병합한다.
5. 공개 참여는 보안 gate와 trusted alpha 운영 데이터가 확보된 후에만 시작한다.

## 단계별 계획

### v0.3 — Trusted control plane 안정화

예상 기간: 1~2주
목표: 설치한 노드가 `dure join`으로 등록되고 중앙에서 계획·배포·검증되는 전체 경로를 완성한다.

우선순위 작업:

- `dure admin diagnose`로 등록 노드의 하드웨어, 설치 모델, LLM 컨테이너 상태를 모아 Codex 기반
  GPU/Ray 배치 및 CPU utility 역할 진단을 제공한다. 진단은 advisory이며 자동 적용하지 않는다.
- 중앙 DB의 최신 `NodeProfile`을 이용해 `dure admin deployment create --nodes ...`를 구현하고
  수동 profile JSON 전달을 제거한다.
- 패키지에 고정된 LAN IP/HTTP 설정을 릴리스 환경에서 생성되는 HTTPS controller 설정으로
  교체한다. 개발용 HTTP는 명시적인 별도 설정으로만 허용한다.
- PostgreSQL에서 join, 승인, heartbeat, 동시 task claim, lease 만료와 재전달을 통합 검증한다.
- 3×24GB GPU pod에서 join → approve → plan → apply → serve → verify → stop/restart를 반복한다.
- 일반 push/PR에서도 실행되는 CI를 추가하고 unit, wheel, Alembic, Debian smoke test를 수행한다.
- 서버 DB 백업·복구, Agent 재시작, credential revoke/rotate 운영 절차를 실제로 리허설한다.
- README의 오래된 수동 controller 제한 설명과 모든 CLI 예제를 현재 동작에 맞춘다.

Exit criteria:

- 새 노드가 패키지 설치 후 `sudo dure join`만으로 pending 목록에 나타난다.
- 승인되지 않은 노드는 task를 한 건도 받을 수 없다.
- 등록된 노드 UUID만으로 3노드 72B 배포 계획을 생성할 수 있다.
- 72B pod가 24시간 연속 동작하고 중앙 stop/restart 후 동일 generation으로 복구된다.
- PostgreSQL 백업에서 노드·deployment·task 이력을 복원할 수 있다.
- main 병합 전에 CI와 실제 3노드 acceptance checklist가 모두 통과한다.

### v0.4 — 노드 보안과 사설 네트워크

예상 기간: 2~3주
목표: bearer credential과 root Agent에 집중된 위험을 줄여 trusted contributor 운영 기준을 만든다.

우선순위 작업:

- join endpoint에 IP/설치 ID rate limit, pending quota, audit alert와 관리자 차단 기능을 추가한다.
- 노드별 device key 또는 mTLS 인증서를 발급·회전·폐기하고 bearer-only 인증을 단계적으로 종료한다.
- WireGuard 기반 pod overlay와 host firewall 검증을 자동화해 Ray 포트를 사설망에만 바인딩한다.
- 컨테이너 이미지 서명과 모델 manifest/hash를 검증하고 승인된 artifact만 실행한다.
- Agent 권한 분리 방안을 구현한다. 최소한 controller 통신 프로세스와 root host-action helper를
  분리하고 좁은 IPC 명령 집합만 허용한다.
- join flood, credential 오용, 반복 task 실패, heartbeat 손실에 대한 구조화 감사 로그와 경보를
  추가한다.

Exit criteria:

- 네트워크에서 controller와 Ray 포트 노출 검사를 자동화해 위반 시 배포를 차단한다.
- 인증서/credential 폐기 후 30초 이내 해당 노드의 heartbeat와 task claim이 모두 거부된다.
- 변조된 image digest, 서명 또는 model manifest가 실제 실행 전에 차단된다.
- 보안 위협 모델의 pre-public-alpha gate를 재검토하고 독립적인 Agent 권한 리뷰를 완료한다.

### v0.5 — OpenAI 호환 Gateway와 스케줄러

예상 기간: 3주
목표: 준비된 단일 GPU pool과 72B pod에 실제 추론 요청을 안전하게 라우팅한다.

우선순위 작업:

- Model Registry에 repository, revision, quantization, image digest, 라이선스와 readiness를 저장한다.
- OpenAI 호환 `GET /v1/models`, `POST /v1/chat/completions`와 streaming을 구현한다.
- 사용자/API key 인증, 모델별 입력·출력 제한, 동시성 제한과 기본 quota를 구현한다.
- Scheduler가 model readiness, GPU 여유, pod 상태, RTT/대역폭, 최근 오류율을 기준으로 실행 위치를
  선택하도록 한다.
- 작은 모델은 single-GPU/data-parallel pool, 72B는 안정적인 regional pipeline pod로 분리한다.
- 대화 세션을 같은 pod에 고정하고 streaming 전 실패만 안전하게 재시도한다.
- 프롬프트 본문을 기본 로그에서 제외하고 request ID, 토큰 수, latency와 오류만 기록한다.

Exit criteria:

- 최소 두 모델이 `/v1/models`에 readiness와 함께 노출된다.
- 정상 노드 기준 요청 성공률 98% 이상을 유지한다.
- 모델별 TTFT, tokens/s, queue time의 P50/P95를 측정할 수 있다.
- 장애 노드 감지 후 30초 이내 신규 요청 배정을 중단한다.
- Gateway, controller, Ray와 worker 로그에 프롬프트가 남지 않는지 자동 검증한다.

### v0.6 — 신뢰성, 관측성, 사용량 측정

예상 기간: 2~3주
목표: 제한된 사용자를 지속적으로 운영할 수 있는 복구·측정 체계를 갖춘다.

우선순위 작업:

- Prometheus metrics와 Grafana dashboard로 node/pod 상태, GPU 이용률, queue, TTFT, 처리량과 오류율을
  제공한다.
- 정상 pod 우회, streaming 전 요청 재시도, 실패 node 격리와 재벤치마크 workflow를 구현한다.
- deployment generation별 rollout/rollback과 controller DB migration rollback 절차를 확립한다.
- 완료 요청을 기준으로 GPU seconds, 입력/출력 토큰과 유효 작업량을 측정한다.
- 내부 credit ledger의 append-only 원장, 중복 방지 key와 운영자 조정 audit를 구현한다.
- controller 백업, 복원, 프로세스 장애와 네트워크 단절 game day를 정기화한다.

Exit criteria:

- 노드·pod·controller 장애 시나리오별 탐지 및 복구 시간이 dashboard와 audit에 남는다.
- 실패 요청이 중복 과금 또는 중복 credit으로 기록되지 않는다.
- 동일 deployment의 단계적 rollout과 이전 generation rollback을 운영 절차로 재현한다.
- 7일 연속 운영에서 데이터 유실 없이 node/task/usage 상태가 복구된다.

### v0.7 — Trusted alpha

예상 기간: 2주 이상
목표: 5~10명의 승인된 기여자와 제한된 API 사용자를 대상으로 운영 가설을 검증한다.

우선순위 작업:

- 기여자 onboarding, 지원 GPU/driver matrix, 개인정보 등급과 incident response 절차를 공개한다.
- 기여자 console 또는 최소 운영 UI에서 node 상태, 제공 시간, 작업량과 credit을 확인하게 한다.
- 무료 quota, 기여 credit, 관리자 후원 pool과 남용 방지 정책을 적용한다.
- 7B/14B 또는 32B single-GPU pool과 72B pipeline pod를 동시에 운영한다.
- 성능, 비용, 장애, 재참여율과 사용자 만족 데이터를 수집해 다음 투자 여부를 판단한다.

Exit criteria:

- 승인된 GPU 10개 이상, 최소 두 모델, 정상 요청 성공률 98% 이상을 달성한다.
- 7일/30일 node 재참여율과 유효 GPU 시간 비율을 측정한다.
- 중대한 credential, 프롬프트 로그 또는 public Ray exposure 사고가 없다.
- 아래 go/no-go 기준으로 v1.0 범위를 확정한다.

## 공통 검증 트랙

각 버전과 병행해 다음 실험을 반복한다.

- 네트워크 RTT/대역폭 변화에 따른 TTFT와 tokens/s 측정
- 생성 중 worker 종료와 pod 전체 재시작
- 이질적인 GPU/driver 조합의 병목 및 실패 분석
- 동시 요청과 continuous batching 처리량 비교
- 7B/14B/32B single-GPU와 72B pipeline의 비용·지연 비교
- 로그·metric·trace의 prompt/credential 유출 검사
- task 재전달, controller 재시작과 DB 복구 시 멱등성 검사

모든 기능 변경은 unit/integration test, wheel, Alembic, Debian smoke test를 통과해야 한다. GPU와
네트워크 관련 acceptance는 별도의 실제 환경 결과를 릴리스 기록에 첨부한다.

## Go/No-Go 기준

Trusted alpha 종료 시 다음 질문 중 세 개 이상이 부정적이면 public federation을 진행하지 않고
single-GPU data-parallel 또는 비동기 batch 서비스에 집중한다.

1. 기여자가 반복적으로 안정적인 GPU 시간을 제공하는가?
2. 72B pipeline pod가 단일 호스팅보다 의미 있는 비용 또는 접근성 이점이 있는가?
3. 노드와 네트워크 변동 속에서도 요청 성공률과 지연 목표를 유지하는가?
4. 프롬프트와 host를 보호할 현실적인 보안 경계를 운영할 수 있는가?
5. 무료 수요가 기여와 후원 공급 범위 안에서 지속 가능한가?

## 명시적 제외 범위

다음 항목은 trusted alpha 이전에는 추진하지 않는다.

- 익명·무허가 노드의 자동 승인
- 임의 인터넷 GPU를 실시간 tensor parallel로 결합
- 민감정보에 대한 confidential-computing 보장
- 모델 학습과 fine-tuning 자원 공유
- 암호화폐 또는 외부 거래 가능한 토큰 발행
- 다중 지역 active-active controller와 상용 SLA

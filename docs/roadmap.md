# Dure 개발 로드맵

기준일: 2026-07-21
현재 누적 개발 브랜치: `version/0.3.19` (rank별 `STAGE` 준비·원자적 활성화와 `sharded_state` 소비, 공식 병합 전 Draft)

## 방향과 원칙

Dure의 다음 목표는 기능 수를 빠르게 늘리는 것이 아니라, 현재의 trusted-LAN MVP를 실제
3-GPU 환경에서 반복 배포 가능한 운영 단위로 만드는 것이다. 작업 순서는 다음 원칙을 따른다.

1. 수동 파일 교환과 환경별 하드코딩을 먼저 제거한다.
2. 공개 API보다 노드·모델 배포의 재현성, 보안, 장애 복구를 먼저 검증한다.
3. Ray는 신뢰된 포드 내부에만 두고 등록·인증·스케줄링은 중앙 제어면이 담당한다.
4. 각 버전은 완료 기준을 모두 통과한 후 사용자의 명시적 요청으로만 `main`에 병합한다.
5. 공개 참여는 보안 통과 기준과 신뢰된 알파 운영 데이터가 확보된 후에만 시작한다.

## 현재 구현 진행 상태

v0.3 계열의 모델 선택 기능은 독립적인 버전 브랜치와 Draft PR로 나누어 진행합니다. 각 기능은 `main`에 병합되기 전까지 공식 릴리스가 아닙니다.

- `version/0.3.6`: 결정론적 정적 모델 선택기
- `version/0.3.7`: 중앙 모델 레지스트리와 배치 프로필 영속화
- `version/0.3.8`: 인벤토리 기반 읽기 전용 추천
- `version/0.3.9`: 구조화된 벤치마크 증적과 `ACTIVE` 승격 게이트
- `version/0.3.10`: 준비·명시적 적용을 분리한 폐쇄형 단일 노드 Agent 벤치마크
- `version/0.3.11`: 불변 추천 스냅샷, 유효성 재검사와 적용 전 배포 세대
- `version/0.3.12`: 세대별 적용·검증 operation, 전체 노드 검증 증거와 명시적 직전 세대 롤백
- `version/0.3.13`: 정확한 노드 조합의 최신 네트워크·NCCL 증적을 소비하는 중앙 추천 자격 게이트
- `version/0.3.14`: 불변 정규 아티팩트 매니페스트와 파일·청크 레지스트리
- `version/0.3.15`: 신뢰 HTTPS 다운로드, 청크 CAS, 재개 가능한 파일 조립과 원자적 `FULL_SNAPSHOT` 캐시 활성화
- `version/0.3.16`: 명시적 중앙 준비 operation·API·CLI, 폐쇄형 `PREPARE_MODEL`·`PREPARE_IMAGE`, digest-pinned OCI pull과 노드별 시도 증적
- `version/0.3.17`: 제한된 vLLM 0.9.0 계약의 rank별 stage 빌더, 불변 variant 레지스트리와 실제 GPU 검증 승격 게이트
- `version/0.3.18`: vLLM 0.9.0 V0 Ray에 고정된 `VLLM_RAY_PP_V1`, 서버 UUID·RFC1918 주소 기반 pipeline rank 결합, 엄격한 컨테이너 identity와 source-pinned 실행 증적
- `version/0.3.19`: exact `VALIDATED` variant의 rank별 준비·복합 cache identity·원자적 활성화, stage-local `sharded_state` 로더와 실제 GPU 수용 harness
- 다음 PR: probe 기반 중앙 캐시 투영, 결정론적 stage variant 선택과 준비·배포 소비 게이트, 명시적 `QUARANTINED` 수명주기

현재 누적 `version/0.3.19` 범위는 기존 추천·수락·준비·배포 경계와 legacy 계획 JSON 호환성을 유지하면서, `VLLM_RAY_PP_V1`에 명시적 rank별 `STAGE` 소비를 추가합니다. 런타임은 정확히 vLLM 0.9.0 V0 Ray, `TP=1`, 노드별 정상 GPU 한 장과 검증된 `FULL_SNAPSHOT` 또는 exact rank 캐시를 요구합니다. 서버 UUID를 identity로 사용하고 head를 rank 0으로 고정하며 worker는 고유 RFC1918 IPv4 문자열 순으로 결합합니다. GCS 6379, worker 20000-21000, loopback API 8000과 backend·rank·component·stage identity 컨테이너 레이블도 고정합니다.

`pipeline-rank-contract`는 vLLM 버전, Ray 노드·GPU, Dure UUID custom resource와 API 시작 뒤 worker actor topology를 확인하고 고정된 vLLM 0.9.0 소스 정렬 규칙에서 binding을 도출합니다. Ray가 vLLM 내부 rank를 공개 필드로 직접 보고한 증거는 아닙니다. 실제 2·3노드 harness는 기본적으로 `NOT_RUN`·77이며 명시적 opt-in과 고정 설정·GPU·모델·runtime 전제가 모두 있어야 분산 load와 최소 추론을 시작합니다. harness는 UUID resource를 대조하지만 설정의 이미지 digest는 선언값이므로 신뢰된 wrapper의 별도 이미지 대조가 필요합니다. 이 Draft 범위는 실제 GPU 수용 검사 결과가 첨부되기 전 장기 안정성이나 이기종 driver 호환성을 증명하지 않습니다.

아티팩트 매니페스트는 중앙 DB에 정규 파일·청크 관계를 저장하고, 노드 라이브러리는 노드 로컬 신뢰 origin에서 해당 청크를 받아 `FULL_SNAPSHOT` 또는 rank별 `STAGE` 캐시로 materialize합니다. 중단 다운로드와 조립을 재개하고, CAS·파일·전체 트리·marker·복합 식별자를 다시 검사한 뒤 marker-last와 no-replace rename으로 활성화합니다. 중앙 준비 API·CLI와 `PREPARE_MODEL`·`PREPARE_IMAGE` Agent 작업은 preview와 명시적 적용을 분리하며, 실패 노드의 현재 단계만 재시도합니다. 별도 오프라인 빌더는 source 매니페스트와 전체 파일 SHA-256을 export 직전에 다시 검증하고 `stages/<pp-rank>`로 격리한 vLLM-native sharded state를 만듭니다. 추천·수락·GPU 추가만으로 자동 준비를 시작하지 않으며, probe와 조정되는 독립 `READY` 수명주기와 자동 variant 선택은 아직 없습니다. 이 누적 브랜치는 공식 `main`에 병합되기 전까지 Draft 개발 상태입니다.

롤백은 전체 노드·동일 토폴로지·승인·온라인·다이제스트 이미지 조건을 강제하고, `STOP_SOURCE → START_TARGET(serve=false) → VERIFY_TARGET`과 선택적 `START_API → VERIFY_API`를 모든 노드 성공 게이트로 진행합니다. 실패 노드 재시도는 새 시도 번호로 펜싱합니다. 같은 GPU에서 컨테이너를 다시 만드는 방식이므로 중단 가능성이 있고 블루·그린 전환이 아닙니다. 네트워크·NCCL 자동 시험과 24시간 복구 검증은 여전히 후속 범위입니다.

## 단계별 계획

### v0.3 — 신뢰된 중앙 제어면 안정화

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

완료 기준:

- 새 노드가 패키지 설치 후 `sudo dure join`만으로 pending 목록에 나타난다.
- 승인되지 않은 노드는 task를 한 건도 받을 수 없다.
- 등록된 노드 UUID만으로 3노드 72B 배포 계획을 생성할 수 있다.
- 72B pod가 24시간 연속 동작하고 중앙 stop/restart 후 동일 generation으로 복구된다.
- PostgreSQL 백업에서 노드·deployment·task 이력을 복원할 수 있다.
- main 병합 전에 CI와 실제 3노드 acceptance checklist가 모두 통과한다.

### v0.3.5 이후 — 결정론적 모델 추천과 자격 검증 기반

목표: GPU가 추가되었을 때 프로필 입력 순서에 의존하지 않고 검증된 후보 중 SLO를 만족하는 최고 품질 모델을 추천하며, 운영자의 명시적 수락으로만 적용 전 세대를 만든다.

현재 상태: 결정론적 선택기, 중앙 모델 레지스트리, 구조화된 증적과 승격 게이트, 폐쇄형 단일 GPU 벤치마크, 추천 스냅샷과 명시적 수락, 세대별 적용·검증·롤백, `FULL_SNAPSHOT`과 명시적 `STAGE` 기반 2·3노드 `VLLM_RAY_PP_V1` rank 결합은 구현되었습니다. 실제 GPU harness의 `PASSED`, 네트워크·NCCL 자동 증적, 자동 stage variant 선택·중앙 캐시 수명주기, 전체 작업 부하 매트릭스와 24시간 복구 검증이 남아 있어 다중 노드 완료 기준은 아직 충족하지 않았습니다.

우선순위 작업:

- `quality-within-SLO` 정책과 카탈로그·배치 프로필을 도입한다.
- 승인됨·온라인·최신 노드 인벤토리를 UUID 기준으로 정렬해 결정론적으로 평가한다.
- VRAM, 디스크, Docker/NVIDIA 런타임, 드라이버·연산 능력, 모델 캐시, 네트워크/NCCL 사전 조건을 후보별로 설명한다.
- `dure admin deployment recommend`와 인벤토리 기반 미리보기를 추가하되 배포 구성·작업·Docker 변경을 만들지 않는다.
- 기존 프로필 JSON 방식과 명시적 `--model`, 기존 계획 JSON 호환성을 유지한다.
- Qwen3.5 등 신규 후보는 변경 불가능한 리비전, 런타임 이미지 다이제스트, 라이선스, 벤치마크 결과를 갖춘 뒤에만 활성 카탈로그에 넣는다.
- 구조화된 벤치마크 증적을 릴리스·배치 프로필·현재 노드 지문에 결합하고, 모든 배치 프로필의 최신 통과 결과로만 `ACTIVE` 승격을 허용한다.
- 폐쇄형 Agent 작업과 허용 목록 기반 실행기의 단일 노드 경로를 실제 GPU 환경에서 검증하고, 다중 노드 네트워크·NCCL 및 24시간 복구 검증으로 확장한다.
- 일반 push/PR CI에서 의존성 없는 핵심, 서버 추가 의존성, 선택기 순열, 마이그레이션, 휠, Debian 간이 검사를 검증한다.

완료 기준:

- 같은 인벤토리는 입력 순서와 무관하게 동일한 추천과 탈락 사유를 만든다.
- 대기 중·오프라인·오래된 노드, 3×22GB, 네트워크/NCCL 미검증 포드는 72B 후보를 통과하지 못한다.
- GPU 추가가 자동 다운로드·이미지 내려받기·적용·기존 배포 중지를 유발하지 않는다.
- 오래된 추천은 수락할 수 없고, 같은 추천 수락은 동일한 적용 전 세대를 반환한다.
- 이전 세대를 지정한 수락은 해당 계보의 최신 세대만 이어 간다.
- 실패한 최신 벤치마크 증적을 과거 통과 결과로 우회할 수 없고, 승격 반복 요청은 같은 증적 집합을 반환한다.
- 전체 배정 노드의 검증 성공만 롤백 증거가 되며 부분 노드·구 Agent 검증은 증거가 되지 않는다.
- 롤백 준비는 task를 만들지 않고, 명시적 적용은 검증된 직접 직전 세대와 동일 전체 토폴로지만 복구한다.
- 실패 노드 재시도와 늦은 task 완료가 시도 번호로 분리되고 같은 계보의 활성 변경은 하나뿐이다.
- 3×24GB의 정상 사설망에서는 기존 72B 기준선과 호환되는 추천을 만들 수 있다.
- 다중 노드 세대는 서버 UUID·고유 사설 IPv4와 정확한 vLLM 0.9.0 계약에 결합되고, rank·노드·actor 불일치 시 host 변경 또는 다음 단계를 차단한다.
- preflight 실패는 기존 세대를 변경하지 않고, 실행 중 실패는 자동 driver 변경·자동 failover 없이 노드 격리, 명시적 재시도 또는 직전 검증 세대 롤백으로 복구한다.
- 새 문서, 단위·통합 테스트, GPU 수용 검사 아티팩트가 정책과 일치한다.

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

완료 기준:

- 네트워크에서 controller와 Ray 포트 노출 검사를 자동화해 위반 시 배포를 차단한다.
- 인증서/credential 폐기 후 30초 이내 해당 노드의 heartbeat와 task claim이 모두 거부된다.
- 변조된 image digest, 서명 또는 model manifest가 실제 실행 전에 차단된다.
- 보안 위협 모델의 pre-public-alpha gate를 재검토하고 독립적인 Agent 권한 리뷰를 완료한다.

### v0.5 — OpenAI 호환 게이트웨이와 스케줄러

예상 기간: 3주
목표: 준비된 단일 GPU pool과 72B pod에 실제 추론 요청을 안전하게 라우팅한다.

우선순위 작업:

- 모델 레지스트리에 저장소, 리비전, 양자화, 이미지 다이제스트, 라이선스와 준비 상태를 저장한다.
- OpenAI 호환 `GET /v1/models`, `POST /v1/chat/completions`와 streaming을 구현한다.
- 사용자/API key 인증, 모델별 입력·출력 제한, 동시성 제한과 기본 quota를 구현한다.
- 스케줄러가 모델 준비 상태, GPU 여유, 포드 상태, RTT/대역폭, 최근 오류율을 기준으로 실행 위치를
  선택하도록 한다.
- 작은 모델은 single-GPU/data-parallel pool, 72B는 안정적인 regional pipeline pod로 분리한다.
- 대화 세션을 같은 pod에 고정하고 streaming 전 실패만 안전하게 재시도한다.
- 프롬프트 본문을 기본 로그에서 제외하고 request ID, 토큰 수, latency와 오류만 기록한다.

완료 기준:

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
- 구현된 deployment generation별 rollout/rollback을 실제 GPU 장애 훈련으로 강화하고 controller DB downgrade 절차를 운영 검증한다.
- 완료 요청을 기준으로 GPU seconds, 입력/출력 토큰과 유효 작업량을 측정한다.
- 내부 credit ledger의 append-only 원장, 중복 방지 key와 운영자 조정 audit를 구현한다.
- controller 백업, 복원, 프로세스 장애와 네트워크 단절 game day를 정기화한다.

완료 기준:

- 노드·pod·controller 장애 시나리오별 탐지 및 복구 시간이 dashboard와 audit에 남는다.
- 실패 요청이 중복 과금 또는 중복 credit으로 기록되지 않는다.
- 동일 deployment의 단계적 rollout과 이전 generation rollback을 운영 절차로 재현한다.
- 7일 연속 운영에서 데이터 유실 없이 node/task/usage 상태가 복구된다.

### v0.7 — 신뢰된 알파

예상 기간: 2주 이상
목표: 5~10명의 승인된 기여자와 제한된 API 사용자를 대상으로 운영 가설을 검증한다.

우선순위 작업:

- 기여자 onboarding, 지원 GPU/driver matrix, 개인정보 등급과 incident response 절차를 공개한다.
- 기여자 console 또는 최소 운영 UI에서 node 상태, 제공 시간, 작업량과 credit을 확인하게 한다.
- 무료 quota, 기여 credit, 관리자 후원 pool과 남용 방지 정책을 적용한다.
- 7B/14B 또는 32B single-GPU pool과 72B pipeline pod를 동시에 운영한다.
- 성능, 비용, 장애, 재참여율과 사용자 만족 데이터를 수집해 다음 투자 여부를 판단한다.

완료 기준:

- 승인된 GPU 10개 이상, 최소 두 모델, 정상 요청 성공률 98% 이상을 달성한다.
- 7일/30일 node 재참여율과 유효 GPU 시간 비율을 측정한다.
- 중대한 credential, 프롬프트 로그 또는 public Ray exposure 사고가 없다.
- 아래 go/no-go 기준으로 v1.0 범위를 확정한다.

## 공통 검증 트랙

각 버전과 병행해 다음 실험을 반복한다.

- 네트워크 RTT/대역폭 변화에 따른 TTFT와 tokens/s 측정
- 생성 중 worker 종료와 pod 전체 재시작
- 이질적인 GPU/driver 조합의 병목 및 실패 분석
- 동시 요청과 연속 배치 처리량 비교
- 7B/14B/32B 단일 GPU와 72B 파이프라인의 비용·지연 비교
- 로그·메트릭·추적 정보의 프롬프트·자격 증명 유출 검사
- 작업 재전달, 중앙 제어면 재시작과 DB 복구 시 멱등성 검사
- vLLM 0.9.0의 source-pinned rank 계약과 실제 2·3노드 worker 배치의 일치 검사

모든 기능 변경은 단위·통합 테스트, 휠, Alembic, Debian 간이 검사를 통과해야 한다. GPU와
네트워크 관련 수용 검사는 별도의 실제 환경 결과를 릴리스 기록에 첨부한다. 전제 조건이 없어 실행하지 못한 `NOT_RUN`·77은 통과 결과로 바꾸지 않는다.

## 진행·중단 기준

신뢰된 알파 종료 시 다음 질문 중 세 개 이상이 부정적이면 공개 연합을 진행하지 않고
단일 GPU 데이터 병렬 또는 비동기 배치 서비스에 집중한다.

1. 기여자가 반복적으로 안정적인 GPU 시간을 제공하는가?
2. 72B pipeline pod가 단일 호스팅보다 의미 있는 비용 또는 접근성 이점이 있는가?
3. 노드와 네트워크 변동 속에서도 요청 성공률과 지연 목표를 유지하는가?
4. 프롬프트와 host를 보호할 현실적인 보안 경계를 운영할 수 있는가?
5. 무료 수요가 기여와 후원 공급 범위 안에서 지속 가능한가?

## 명시적 제외 범위

다음 항목은 신뢰된 알파 이전에는 추진하지 않는다.

- 익명·무허가 노드의 자동 승인
- 임의 인터넷 GPU를 실시간 tensor parallel로 결합
- 민감정보에 대한 기밀 컴퓨팅 보장
- 모델 학습과 미세 조정 자원 공유
- 암호화폐 또는 외부 거래 가능한 토큰 발행
- 다중 지역 active-active controller와 상용 SLA

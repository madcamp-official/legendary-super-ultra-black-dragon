# 개인정보·프롬프트 처리 정책

이 문서는 현재 Dure가 처리·기록하는 데이터의 경계와, 신뢰된 사설 GPU 노드에서 허용할 수 있는
추론 입력의 기준을 정의한다. Dure는 완성된 공개 inference gateway나 최종 사용자 인증 서비스가
아니며, 이 정책은 그러한 기능이 이미 구현됐다는 뜻이 아니다.

## 현재 보안 경계

Dure Control Plane과 Agent task는 프롬프트·완성문·model access token·credential·컨테이너 command,
environment, mount 내용을 중앙 DB·task 결과·benchmark evidence에 받거나 저장하지 않도록 설계되어
있다. `dure admin diagnose`도 선택한 inventory를 외부 진단 제공자에게 보낼 수 있지만 프롬프트와
비밀값은 전송 대상에서 제외한다.

그러나 이 제한이 GPU host를 신뢰 실행 환경으로 바꾸지는 않는다. host 관리자 또는 host를 침해한
공격자는 container process·memory·network traffic·local log에 접근할 수 있을 수 있다. Dure는 TEE,
기밀 컴퓨팅, end-to-end prompt encryption, tenant 격리, 사용자 데이터 삭제 API를 제공하지 않는다.

## 데이터 등급과 허용 기준

| 등급 | 예시 | 현재 신뢰된 사설 노드에서의 기준 |
| --- | --- | --- |
| 공개·비민감 | 공개 문서, synthetic test prompt, 공개 benchmark 입력 | 허용 가능. 그래도 불필요한 원문 로그는 남기지 않음 |
| 내부 업무 데이터 | 공개 전 문서, 내부 코드 조각, 운영 메타데이터 | 데이터 소유자 승인과 신뢰된 전용 node·network가 있을 때만 허용 |
| 민감 개인정보·기밀 | 주민번호, 계정 credential, API key, 의료·금융·인사 정보, 고객 원문 | 현재 community/shared GPU 환경에서는 금지 |
| 규제·계약상 제한 데이터 | 법률·계약·지역 보관 조건이 있는 데이터 | 별도 법무·보안 검토와 전용 환경이 없는 한 금지 |

“신뢰된 전용 node”는 최소한 운영 주체·host root 접근자·network zone·log 저장소가 데이터 처리
승인 범위 안에 있는 node를 뜻한다. 단순히 Dure Agent가 승인되었거나 GPU가 online이라는 사실은
민감 데이터 처리 승인 근거가 아니다.

## 데이터 흐름별 원칙

| 흐름 | Dure 현재 동작 | 운영자 의무 |
| --- | --- | --- |
| Control Plane API | inventory, node·task·deployment·artifact identity와 폐쇄형 metric을 처리 | admin token을 보호하고 API·DB·reverse proxy log에서 request body를 기록하지 않음 |
| Agent polling·task | model/image digest와 폐쇄형 task payload를 처리 | credential·`agent.json`·host journal을 공개 ticket·chat에 넣지 않음 |
| 모델 artifact | manifest·chunk digest·license metadata를 처리 | 모델 원본·revision·license와 보관 지역을 [모델 반입·승인 정책](model-onboarding-policy.md)에 따라 승인 |
| vLLM inference | 기본적으로 head host의 loopback `127.0.0.1:8000`에만 bind | prompt·completion이 node 밖으로 나가는 proxy, client, log 저장소를 별도로 통제 |
| benchmark·evidence | 고정 workload 수치와 aggregate metric만 저장 | 실제 prompt, stdout/stderr 원문, model token을 evidence에 넣지 않음 |

## 로그·관측·지원 요청

1. Controller, Agent, reverse proxy, vLLM, Docker, system journal의 수집 규칙에서 request/response body,
   Authorization header, query token, `DURE_ADMIN_TOKEN`, enrollment token, node credential을 제외한다.
2. 장애를 재현할 때도 실제 고객 prompt를 issue·PR·CI log·benchmark evidence에 붙이지 않는다. synthetic
   입력, request ID, 시간, node UUID, 안전한 metric·failure code로 대체한다.
3. debug log가 payload를 기록할 수 있는 component를 임시로 켰다면, 승인된 격리 환경과 짧은 기간으로
   제한하고 [데이터 보존·격리·삭제 정책](data-retention.md)에 따라 접근·삭제를 기록한다.
4. 외부 observability·ticket·AI 진단 도구는 별도 데이터 처리자다. 전송 전 data classification과
   redaction을 확인하며, “운영용”이라는 이유만으로 민감 prompt를 보내지 않는다.

## 노드 운영자의 가시성과 배치

다중 노드 Ray/vLLM 실행은 선택된 node 사이의 사설 network와 host runtime을 사용한다. 노드 운영자는
자신이 관리하는 host의 process, container, GPU memory, filesystem 또는 network를 관찰할 수 있다는
전제로 운영한다. 따라서 서로 다른 조직의 community node에 민감 prompt를 분산 배치하면 안 된다.

민감도가 낮더라도 다음은 금지한다.

- `8000`, `8081`, Ray GCS·worker port를 공용 인터넷에 직접 노출
- Control Plane admin bearer token을 inference client credential로 재사용
- prompt 또는 completion을 deployment plan, task payload, artifact manifest, evidence metadata에 삽입
- 노드 교체를 위해 `/etc/dure/agent.json`이나 local journal을 다른 host에 복사

포트와 reverse proxy의 현재 경계는 [네트워크·방화벽 운영 절차](networking.md), 외부 endpoint의
필수 조건은 [외부 추론 API 경계](external-inference-boundary.md)를 따른다.

## prompt 노출 의심 incident

1. public ingress 또는 관련 proxy route를 차단하고, 노출 범위·시간·host·log sink를 식별한다.
2. prompt와 함께 credential 노출 가능성이 있으면 admin token·node credential·외부 proxy credential을
   별도 영향 분석 후 회전한다. prompt 자체를 중앙 evidence에 복사하지 않는다.
3. 접근 가능한 journal·proxy·observability storage를 격리하고, redaction된 시간·범위·대응만 incident
   record에 남긴다.
4. 영향을 받은 node는 필요 시 revoke·unjoin하고, cache·journal·backup의 보존 또는 삭제는
   [GPU 노드 폐기·교체 운영 절차](node-lifecycle.md)와 데이터 보존 정책을 따른다.
5. 재개 전에는 ingress, log 수집, data classification, 승인된 node·network 경계를 다시 검토한다.

이 절차는 법률상 통지·데이터 주체 요청·계약상 사고 대응을 자동으로 충족하지 않는다. 해당 의무는
조직의 법무·보안·개인정보 책임자가 별도로 판단한다.

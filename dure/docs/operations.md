# 운영 절차 허브

이 문서는 Dure의 일상 운영 순서와 역할별 runbook의 진입점입니다. 세부 계약과 역사적 절차는
[통합 운영 참조](operations-reference.md)에 보존되어 있지만, 새 운영 변경은 아래 역할별 문서를
기준으로 합니다.

## 운영 순서

```text
Controller 상태 확인
→ GPU 노드 bootstrap·join·승인
→ 인벤토리·증적·추천 검토
→ Fleet 또는 deployment 수락
→ 준비 preview와 명시적 apply
→ 배포 apply·verify
→ 상태 관찰·필요 시 명시적 rollback
```

각 단계는 이전 단계가 안전하게 완료됐다는 뜻이 아닙니다. 추천·수락·preview는 host 상태를 바꾸지
않으며, 준비·적용·rollback에는 해당 명령의 명시적 `--apply`와 현재 identity 재검사가 필요합니다.

## 역할별 runbook

| 역할·상황 | 기준 문서 | 핵심 경계 |
| --- | --- | --- |
| Controller 설치, DB, 관리자 credential, node 등록·승인 | [Controller 운영](controller-operations.md) | Controller는 관리망에 두고 node는 outbound Agent 연결만 사용 |
| 배포 generation, Fleet, qualification, benchmark, rollback | [배포·Fleet 운영](deployment-operations.md) | 추천·수락은 실행 권한이 아니며, rollback은 명시적이고 중단 가능 |
| 모델 manifest, `FULL_SNAPSHOT`, `STAGE`, 준비, cache 격리 | [아티팩트·캐시 운영](artifact-cache-operations.md) | exact identity·`READY` gate, 자동 삭제·P2P 전송 금지 |
| 과거 Agent/DB 경로의 업그레이드와 복구 | [레거시 업그레이드](legacy-upgrades.md) | migration 전 backup, 직접 DB 수정·downgrade 추측 금지 |
| Agent 설정·credential 회전 | [Agent 운영](agent-operations.md) | credential은 출력·복사하지 않고 heartbeat로 확인 |
| 포트, TLS, NCCL interface | [네트워크 운영](networking.md) | Ray·worker·vLLM API를 public Internet에 공개하지 않음 |
| day-2 상태·장애 대응 | [관측·장애 대응](observability.md) | 자동 failover·자동 rollback을 가정하지 않음 |

## 실제 GPU 수용 검증

수용 절차는 [릴리스 수용 검증](release-validation.md), 결과 기록은
[릴리스 증적](release-evidence/README.md)을 사용합니다. 전제 조건 부족으로 실행하지 않은 결과는
`NOT_RUN`이며 배포 성공이나 `VALIDATED` 증거가 아닙니다.

## 역사적 상세 참조

이전 통합 문서에 있던 명령 예시, failure code, 단계별 안전 게이트와 버전별 절차는
[통합 운영 참조](operations-reference.md)에 보존합니다. 새로운 정책·명령·상태 변경은 역할별 runbook과
지원 매트릭스에 먼저 반영한 뒤 참조 문서를 갱신합니다.

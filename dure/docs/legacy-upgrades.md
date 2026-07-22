# 레거시 업그레이드와 복구

이 문서는 현재 package 기준선으로 올리기 전의 legacy Agent·deployment·database 경로를 다룹니다.
일상 rollout은 [버전 호환성과 롤링 업그레이드](compatibility-upgrades.md), DB backup·restore는
[재해 복구](disaster-recovery.md)를 기준으로 합니다.

## 공통 원칙

1. migration 전 PostgreSQL backup과 복원 절차를 확인합니다.
2. Controller와 migration을 먼저 적용하고, Agent는 작은 batch로 올린 뒤 heartbeat version을 확인합니다.
3. 실행 중 task, preparation, deployment operation, active lineage가 있으면 downgrade를 추측하거나
   DB 행·Alembic revision을 직접 수정하지 않습니다.
4. 지원되지 않는 downgrade가 필요하면 마지막 검증 backup과 호환 package로 복원합니다.

## 최소 Agent 호환 경계

| 기능 | 최소 Agent |
| --- | --- |
| legacy generation rollback 증거 | 0.3.12 |
| `FULL_SNAPSHOT` 엄격 다중 노드 실행 | 0.3.18 |
| `STAGE` 준비·실행·rollback 대상 시작 | 0.3.19 |
| artifact cache quarantine | 0.3.20 |

혼합 버전 node가 있으면 해당 backend의 더 강한 계약을 적용하지 않습니다. 기존 deployment를 자동 변환하거나
새 backend로 묵시적으로 전환하지 않습니다.

## 역사적 절차

0.3.12~0.3.20의 migration·filesystem ownership·rank stage 도입 세부 절차는
[통합 운영 참조](operations-reference.md)에 보존합니다. 현재 지원 version의 rollout·rollback은
[호환성 문서](compatibility-upgrades.md)를 우선 사용합니다.

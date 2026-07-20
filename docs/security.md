# 보안 모델과 보안 강화 계획

## 보호 대상

- admin과 node bearer credential
- host root 권한과 Docker 제어권
- model artifact와 immutable image identity
- deployment topology, task 이력, node inventory
- vLLM이 처리하는 prompt와 생성 데이터

## 주요 위협과 현재 통제

| 위협 | 현재 통제 | 남은 과제 |
|---|---|---|
| 권한 없는 노드의 작업 수신 | 새 join은 운영자 승인 전 pending | join rate limit과 네트워크 제한 |
| 탈취된 node credential | 노드별 hash 저장과 개별 revoke | mTLS와 자동 rotation |
| 중앙 제어면의 원격 셸화 | 폐쇄형 작업 열거형과 검증된 페이로드 | 호스트 작업 권한 분리 |
| 이미지 치환 | 중앙 계획은 OCI 다이제스트 요구 | 이미지 서명과 출처 검증 |
| 모델 아티팩트 변경 | 리비전 고정 정책과 캐시 조사 | 서명된 매니페스트·파일 해시 검증 |
| 작업 재생 | 임대, 세대 검사, 로컬 완료 저널 | 서명된 봉투와 펜싱 토큰 |
| 동시 변경 | 노드 행 잠금과 한 개의 활성 임대 | PostgreSQL 동시성 부하 시험 |
| 컨테이너 오조작 | 정확한 배포 레이블 필터링 | 세대별 레이블 검증과 격리 검토 |
| 공개 Ray 노출 | 사설망 사용 문서화 | WireGuard와 firewall 검증 자동화 |
| join endpoint 남용 | pending 노드에는 작업 권한 없음 | rate limit, quota, audit alert |
| host 운영자의 prompt 관찰 | community workload는 비기밀로 선언 | confidential-computing 또는 private pool |

## 운영 요구 사항

- 개발 목적이 아닌 모든 에이전트 연결에는 HTTPS를 사용합니다.
- `DURE_ADMIN_TOKEN`, 데이터베이스 자격 증명, APT 서명 키, 모델 자격 증명을 Git 밖에 보관합니다.
- 가능하면 join과 중앙 제어면 종단점을 신뢰된 LAN 또는 사설 오버레이로 제한합니다.
- 노드를 승인하기 전 호스트명, GPU 인벤토리, 주소, 소유자를 검토합니다.
- 배포 이미지와 모델 리비전을 고정하고 Ray 포드 전체에서 같은 검증 런타임을 사용합니다.
- 프롬프트와 자격 증명을 기록하지 않고 메타데이터와 오류만 수집합니다.
- `dure admin diagnose`는 명시적 외부 처리입니다. 선택된 인벤토리가 운영자 컴퓨터의 Codex 제공자로 전송될 수 있지만 자격 증명, 컨테이너 환경 변수·명령, 프롬프트는 전송하지 않습니다.
- PostgreSQL 백업, 자격 증명 폐기, 복구 절차를 실제로 검증합니다.

## 모델 레지스트리와 읽기 전용 추천기의 경계

모델 레지스트리의 영속 스키마, 관리자 인증 API, 고정 리비전·다이제스트 검증·상태 전이와 정책 기반 읽기 전용 추천은 구현되었습니다. 추천은 호스트 변경 권한이 아닙니다. 저장된 인벤토리와 `ACTIVE` 모델 릴리스만 읽고, 승인·온라인·프로필 신선도·GPU 아키텍처를 통과한 노드만 선택합니다. 배포 구성 생성과 적용은 별도 운영자 행동입니다.

- 아티팩트는 변경 불가능한 리비전과 매니페스트 다이제스트를 가져야 합니다.
- 런타임 이미지는 정확한 OCI 다이제스트로 고정합니다.
- 라이선스와 사용 조건은 릴리스 승격 전에 검토합니다.
- 레지스트리 API는 허용 필드 외 입력을 거부하며 임의 셸, Docker 인자 목록, 호스트 경로, 마운트, 환경 변수를 저장하지 않습니다.
- 추천 API는 `refresh`, 임의 명령·Docker 인자·환경 변수·마운트와 네트워크 증적 우회 입력을 거부하며 배포·작업·감사 이벤트를 생성하지 않습니다.
- 추천 인벤토리 지문과 배정에는 에이전트가 보고한 이름이 아니라 서버가 발급한 노드 UUID를 사용합니다.
- 오래된 인벤토리, 폐기된 아티팩트, 고정되지 않은 이미지, 검증되지 않은 네트워크는 승격을 막아야 합니다.
- 모델·벤치마크 메타데이터에는 프롬프트나 자격 증명을 포함하지 않습니다.

## 공개 알파 전 통과 기준

신뢰된 운영자 그룹 밖의 노드를 받기 전 다음을 완료해야 합니다.

1. tokenless join의 rate limit, quota, abuse control을 추가합니다.
2. bearer-only Agent 인증을 mTLS 또는 서명된 device key로 대체합니다.
3. 사설 network overlay와 host firewall을 배포하고 검증합니다.
4. image signature, provenance, model manifest를 검증합니다.
5. root Agent와 container isolation을 독립 검토합니다.
6. join flood, heartbeat 손실, 반복 task 실패, credential 오용을 알리는 alert를 추가합니다.

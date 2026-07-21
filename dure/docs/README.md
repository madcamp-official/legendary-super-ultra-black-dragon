# Dure 문서 색인

이 디렉터리는 Dure의 현재 동작, 운영 절차, 보안 경계, 개발 규칙, 향후 계획을 설명합니다. 기능 문서에서 **현재 제공**과 **계획됨**을 구분하며, 계획 문서를 실제 운영 절차로 사용해서는 안 됩니다.

## 시작점

- [루트 README](../README.md): 설치, 로컬 CLI, 중앙 관리의 빠른 시작
- [아키텍처](architecture.md): 로컬 CLI·에이전트·중앙 제어면의 경계와 작업 프로토콜
- [운영 절차](operations.md): GPU host bootstrap, 서버 운영, 노드 승인, Fleet 준비·적용, 세대별 검증·롤백과 장애 복구
- [단일 GPU 자동 활성화](activation.md): 불변 릴리스 등록, 자동 준비·벤치마크·승격·추천·배포·검증
- [보안 모델](security.md): 현재 통제와 공개 전 보안 강화 과제
- [vLLM 다중 노드 rank 결합 결정 기록](adr-vllm-multinode-rank-binding.md): `VLLM_RAY_PP_V1`의 고정 소스 계약과 검증 한계

## 모델과 용량

- [모델 선택 정책](model-selection.md): GPU 인벤토리 기반 결정론적 선택기, 모델 레지스트리·승격 게이트, 결정론적 추천 스냅샷, 배포 세대 상태와 명시적 롤백 — 부분 구현
- [벤치마크 및 모델 자격 검증](benchmarking.md): 구조화된 증적, 단일 노드 폐쇄형 실행과 후보 모델의 품질·성능·안정성 승격 기준 — 부분 구현
- [자동 배치 프로필 qualification](profile-qualification.md): `DRAFT → QUALIFYING → VALIDATED → ACTIVE`, 8단계 폐쇄형 증적과 exact rank·노드·GPU 결합을 강제하는 중앙 계약
- [Fleet 후보 생성과 결정론적 스케줄러](fleet-scheduler.md): 여러 exact 증적을 비중첩 배포로 조합하고 불변 추천·원자적 수락·명시적 준비·적용·검증으로 연결하는 계약과 이기종·대규모 합성 수용 매트릭스
- [모델 아티팩트 매니페스트와 배포 계약](artifact-distribution.md): 불변 파일·청크 레지스트리, 결정론적 `STAGE`·`FULL_SNAPSHOT` 선택, 중앙 캐시 수명 주기, 명시적 준비·격리와 배포·롤백 소비 게이트
- [vLLM 단계 아티팩트 생성·검증·배포](stage-artifacts.md): 제한된 vLLM 0.9.0 stage builder, variant·rank 매니페스트, 추천에 고정된 rank별 준비와 `sharded_state` 소비
- [제품 제안서](dure-proposal.md): 장기 제품 비전과 MVP 가설

## 개발과 배포

- [개발·릴리스 절차](development.md): 단위 테스트, 마이그레이션, Git 훅, 릴리스 규칙과 실제 GPU·PostgreSQL 검증의 분리
- [APT 배포](apt-distribution.md): Debian 패키지와 서명된 APT 저장소 운영
- [개발 로드맵](roadmap.md): v0.3.6 이후 모델 선택·자격 검증·세대 롤백과 단계별 목표

## 문서 관리 원칙

- 실행 가능한 CLI/API만 현재형으로 기술합니다.
- 계획 중인 기능은 상태와 전제 조건을 표시합니다.
- 모델은 이름만이 아니라 리비전, 양자화, 런타임 이미지 다이제스트, 라이선스를 함께 관리합니다.
- 프롬프트, 자격 증명, 토큰, 실제 비밀값을 예시나 벤치마크 결과에 기록하지 않습니다.
- 모델 릴리스와 배치 프로필의 실제 상태는 중앙 레지스트리가 진실의 원천이며, 이 문서는 정책과 절차를 설명합니다.
- 배포 세대의 `verified_at`은 전체 배정 노드가 검증에 성공하고 backend별 최소 Agent 버전을 충족할 때만 롤백 증거로 사용합니다. legacy는 0.3.12 이상, `VLLM_RAY_PP_V1`은 0.3.18 이상, `STAGE`는 0.3.19 이상과 엄격한 rank·API 검증이 필요합니다.
- 추천 세대의 apply와 rollback은 배포·노드·매니페스트·exact cache identity·현재 준비 시도·OCI 다이제스트에 결합된 `READY` 증적을 요구하며, rollback은 네트워크 준비를 수행하지 않습니다.
- Fleet 추천·조회·수락은 호스트 변경 권한이 아닙니다. 전용 `fleet prepare`와 `fleet apply`만 Agent 작업을 만들며, 배포별 실패 뒤에도 자동 롤백·중지·예약 해제는 수행하지 않습니다.
- Fleet의 이기종·대규모 기본 수용 매트릭스는 합성 인벤토리와 SQLite를 사용합니다. 알고리즘 불변식을 검증할 뿐 실제 GPU·NCCL·vLLM 또는 PostgreSQL 부하 증적을 대신하지 않습니다.
- stage variant의 `VALIDATED`는 실제 GPU export/load 검증 상태이지 노드 설치나 배포 완료 상태가 아닙니다. 추천이 exact digest와 rank 결합을 선택한 뒤에도 별도 준비 적용과 모든 rank의 배포 검증을 완료해야 합니다.
- `VLLM_RAY_PP_V1`은 vLLM 0.9.0 V0 Ray, `TP=1`, 노드별 GPU 한 장, `PP=2/3`만 지원하는 현재 실행 계약입니다. 기존 로컬 계획 JSON과 legacy backend는 그대로 호환됩니다.
- `pipeline-rank-contract`는 고정된 vLLM 소스 규칙과 Ray 노드·actor 토폴로지를 결합한 간접 증적입니다. vLLM 내부 rank를 Ray가 직접 보고했다는 뜻으로 기술하지 않습니다.
- 추천기는 같은 품질에서 검증된 exact `STAGE`를 우선하고 독립 `FULL_SNAPSHOT` 후보도 결정론적으로 평가합니다. 수락 뒤에는 두 형식 사이를 묵시적으로 바꾸지 않습니다.
- 중앙 캐시는 `READY`·`STALE`·`MISSING`·`CORRUPT`·`QUARANTINED`로 투영합니다. 완전한 probe만 상태를 악화시킬 수 있고, `READY` 복구는 현재 준비 성공만 가능합니다.
- `artifact-cache` 조회·참조 검사는 읽기 전용입니다. 격리는 preview 뒤 명시적 적용으로 정확한 캐시 하나를 보존 위치로 원자 이동하며 자동 삭제·퇴출·P2P 전송은 하지 않습니다.

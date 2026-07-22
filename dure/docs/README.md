# Dure 문서 색인

이 디렉터리는 Dure의 현재 동작, 운영 절차, 보안 경계, 개발 규칙, 향후 계획을 설명합니다. 기능 문서에서 **현재 제공**과 **계획됨**을 구분하며, 계획 문서를 실제 운영 절차로 사용해서는 안 됩니다.

## 시작점

- [루트 README](../README.md): 설치, 로컬 CLI, 중앙 관리의 빠른 시작
- [아키텍처](architecture.md): 로컬 CLI·에이전트·중앙 제어면의 경계와 작업 프로토콜
- [운영 절차](operations.md): GPU host bootstrap, 서버 운영, 노드 승인, Fleet 준비·적용, 세대별 검증·롤백과 장애 복구
- [Controller 운영](controller-operations.md): Controller·PostgreSQL·관리자 credential, node 등록·승인과 중단 기준
- [배포·Fleet 운영](deployment-operations.md): qualification, recommendation, 준비·적용·검증과 명시적 rollback
- [아티팩트·캐시 운영](artifact-cache-operations.md): manifest, `FULL_SNAPSHOT`, `STAGE`, cache 상태와 격리
- [레거시 업그레이드와 복구](legacy-upgrades.md): 과거 Agent·deployment 경로의 backup, migration, 최소 호환 경계
- [통합 운영 참조](operations-reference.md): 이전 통합 문서의 상세 명령·failure code·역사적 절차 보존본
- [설정 참조서](configuration-reference.md): 환경 변수, 설정 파일, systemd 및 수용 검증 opt-in의 범위·우선순위·권한
- [CLI 명령 참조](cli-reference.md): 로컬·관리자 명령의 권한, 호스트 변경 여부, preview/apply 조건
- [Agent 설정과 credential 회전 운영 절차](agent-operations.md): `/etc/dure` 설정 경계, HTTPS/TLS 우선순위, 안전한 회전·재등록
- [단일 GPU 자동 활성화](activation.md): 불변 릴리스 등록, 자동 준비·벤치마크·승격·추천·배포·검증
- [릴리스 수용 검증](release-validation.md): v0.4.14 activation 순서 회귀와 실제 3×24GiB `PP=3` GPU 검증 절차
- [릴리스 증적 기록](release-evidence/README.md): runbook과 실제 GPU 수용 결과를 분리해 보관하는 형식
- [릴리스 실행 체크리스트](release-runbook.md): 승인자·중단 기준·재시도·APT 게시 뒤 확인을 연결하는 release record 절차
- [사용자용 변경 이력](../CHANGELOG.md): 기능 변화, 호환성 영향, 업그레이드 요구 사항과 알려진 제한
- [보안 모델](security.md): 현재 통제와 공개 전 보안 강화 과제
- [개인정보·프롬프트 처리 정책](data-privacy.md): 현재 데이터 등급, node 운영자의 가시성, logging·incident 대응 경계
- [네트워크·방화벽 운영 절차](networking.md): Controller·Agent·Ray/vLLM·NCCL 통신 경계, 포트와 사설망 검증
- [외부 추론 API 경계](external-inference-boundary.md): 현재 공개 gateway 미지원 범위와 별도 gateway의 필수 조건
- [PostgreSQL 백업·복구·재해 복구](disaster-recovery.md): backup, restore drill, migration 실패, credential 회전 절차
- [데이터 보존·격리·삭제 정책](data-retention.md): DB·audit·evidence·journal·model cache의 보존 기준과 수동 삭제 승인
- [관측·장애 대응 운영 절차](observability.md): systemd·heartbeat·task·DB 신호, 외부 알림 기준과 redaction
- [GPU 노드 폐기·교체 운영 절차](node-lifecycle.md): revoke·unjoin, cache 보존·격리, package 제거와 새 node 재등록
- [관리자·Agent API 계약](api-contract.md): 인증 주체, 재시도·오류 처리, 목록 한계와 민감 정보 규칙
- [vLLM 다중 노드 rank 결합 결정 기록](adr-vllm-multinode-rank-binding.md): `VLLM_RAY_PP_V1`의 고정 소스 계약과 검증 한계

## 모델과 용량

- [모델 선택 정책](model-selection.md): GPU 인벤토리 기반 결정론적 선택기, 모델 레지스트리·승격 게이트, 결정론적 추천 스냅샷, 배포 세대 상태와 명시적 롤백 — 부분 구현
- [벤치마크 및 모델 자격 검증](benchmarking.md): 구조화된 증적, 단일 노드 폐쇄형 실행과 후보 모델의 품질·성능·안정성 승격 기준 — 부분 구현
- [SLO·벤치마크 정책 운영 절차](slo-benchmark-policy.md): SLO 기준선, evidence 재검증, `ACTIVE` 승격 승인 절차
- [자동 배치 프로필 qualification](profile-qualification.md): `DRAFT → QUALIFYING → VALIDATED → ACTIVE`, 8단계 폐쇄형 증적과 exact rank·노드·GPU 결합을 강제하는 중앙 계약
- [Fleet 후보 생성과 결정론적 스케줄러](fleet-scheduler.md): 여러 exact 증적을 비중첩 배포로 조합하고 불변 추천·원자적 수락·명시적 준비·적용·검증으로 연결하는 계약과 이기종·대규모 합성 수용 매트릭스
- [모델 아티팩트 매니페스트와 배포 계약](artifact-distribution.md): 불변 파일·청크 레지스트리, 결정론적 `STAGE`·`FULL_SNAPSHOT` 선택, 중앙 캐시 수명 주기, 명시적 준비·격리와 배포·롤백 소비 게이트
- [모델 반입·승인 정책](model-onboarding-policy.md): allowlist, 원본 revision·라이선스·runtime 검토, 승격과 철회 책임
- [vLLM 단계 아티팩트 생성·검증·배포](stage-artifacts.md): 제한된 vLLM 0.9.0 stage builder, variant·rank 매니페스트, 추천에 고정된 rank별 준비와 `sharded_state` 소비
- [지원 매트릭스](support-matrix.md): OS·APT package·모델·GPU·TP/PP·runtime의 현재 지원 범위
- [버전 호환성과 롤링 업그레이드](compatibility-upgrades.md): Controller·Agent·migration·backend의 최소 조건과 안전한 rollout·복구 순서
- [용어집](glossary.md): 모델 배포와 운영에서 쓰는 핵심 용어
- [제품 제안서](dure-proposal.md): 장기 제품 비전과 MVP 가설

## 개발과 배포

- [개발·릴리스 절차](development.md): 단위 테스트, 마이그레이션, Git 훅, 릴리스 규칙과 실제 GPU·PostgreSQL 검증의 분리
- [APT 배포](apt-distribution.md): Debian 패키지와 서명된 APT 저장소 운영
- [릴리스 권한과 출처 관리](release-governance.md): source, tag, 서명 키, package, APT mirror의 신뢰 경계
- [개발 로드맵](roadmap.md): 현재 구현 상태와 공개 운영 전 단계별 목표

## 문서 관리 원칙

- 실행 가능한 CLI/API만 현재형으로 기술합니다.
- 계획 중인 기능은 상태와 전제 조건을 표시합니다.
- 절차(runbook)와 실제 실행 결과(evidence)를 같은 문서에서 혼합하지 않습니다. version별 실제 GPU·네트워크 수용 결과는 `release-evidence/`에 `PASSED`·`FAILED`·`NOT_RUN`으로 기록합니다.
- source checkout, Git tag, GitHub Release, APT package는 서로 다른 상태입니다. source-to-package provenance가 없는 경우에는 공식 승인 package라고 표현하지 않습니다.
- release tag·package 게시·실제 GPU 수용 결과의 실행 순서는 [릴리스 실행 체크리스트](release-runbook.md)를 따르며, `PUBLISHED`와 `ACCEPTED`를 혼동하지 않습니다.
- 네트워크 포트·방화벽·NCCL interface의 운영 기준은 [네트워크·방화벽 운영 절차](networking.md)를 단일 기준으로 사용합니다. Agent의 Controller 주소·TLS·credential 회전은 [Agent 설정과 credential 회전 운영 절차](agent-operations.md), PostgreSQL backup·restore와 credential 복구는 [PostgreSQL 백업·복구·재해 복구](disaster-recovery.md), 상태 확인과 외부 alert는 [관측·장애 대응 운영 절차](observability.md)를 따릅니다.
- 모델·OS·runtime 지원 범위는 [지원 매트릭스](support-matrix.md)를 기준으로 하며, 기능 문서에 같은 수치를 반복할 때는 이 문서와 함께 갱신합니다.
- 새 문서·이미지·링크는 `python3 scripts/check_docs.py`를 통과해야 합니다. 이미지에는 alt text와 갱신 가능한 원본 형식을 함께 보관합니다.
- 모델은 이름만이 아니라 리비전, 양자화, 런타임 이미지 다이제스트, 라이선스를 함께 관리합니다.
- 모델 반입의 출처·라이선스·보안·철회 검토는 [모델 반입·승인 정책](model-onboarding-policy.md)을 따르며, 레지스트리 등록이 그 검토를 자동화하지 않습니다.
- 프롬프트, 자격 증명, 토큰, 실제 비밀값을 예시나 벤치마크 결과에 기록하지 않습니다.
- 현재 허용 데이터 등급, host 운영자의 가시성, prompt 노출 대응은 [개인정보·프롬프트 처리 정책](data-privacy.md)을 따릅니다. 공개 inference endpoint는 현재 제공 범위가 아니며 [외부 추론 API 경계](external-inference-boundary.md)의 별도 gateway 조건을 충족하기 전에는 만들지 않습니다.
- 모델 릴리스와 배치 프로필의 실제 상태는 중앙 레지스트리가 진실의 원천이며, 이 문서는 정책과 절차를 설명합니다.
- 배포 세대의 `verified_at`은 전체 배정 노드가 검증에 성공하고 backend별 최소 Agent 버전을 충족할 때만 롤백 증거로 사용합니다. legacy는 0.3.12 이상, `VLLM_RAY_PP_V1`은 0.3.18 이상, `STAGE`는 0.3.19 이상과 엄격한 rank·API 검증이 필요합니다.
- 추천 세대의 apply와 rollback은 배포·노드·매니페스트·exact cache identity·현재 준비 시도·OCI 다이제스트에 결합된 `READY` 증적을 요구하며, rollback은 네트워크 준비를 수행하지 않습니다.
- 모델·profile의 SLO 기준선과 evidence 재검증·승격은 [SLO·벤치마크 정책 운영 절차](slo-benchmark-policy.md)를 따르며, `NOT_RUN`은 통과 증적이 아닙니다.
- Fleet 추천·조회·수락은 호스트 변경 권한이 아닙니다. 전용 `fleet prepare`와 `fleet apply`만 Agent 작업을 만들며, 배포별 실패 뒤에도 자동 롤백·중지·예약 해제는 수행하지 않습니다.
- Fleet의 이기종·대규모 기본 수용 매트릭스는 합성 인벤토리와 SQLite를 사용합니다. 알고리즘 불변식을 검증할 뿐 실제 GPU·NCCL·vLLM 또는 PostgreSQL 부하 증적을 대신하지 않습니다.
- stage variant의 `VALIDATED`는 실제 GPU export/load 검증 상태이지 노드 설치나 배포 완료 상태가 아닙니다. 추천이 exact digest와 rank 결합을 선택한 뒤에도 별도 준비 적용과 모든 rank의 배포 검증을 완료해야 합니다.
- `VLLM_RAY_PP_V1`은 vLLM 0.9.0 V0 Ray, `TP=1`, 노드별 GPU 한 장, `PP=2/3`만 지원하는 현재 실행 계약입니다. 기존 로컬 계획 JSON과 legacy backend는 그대로 호환됩니다.
- `pipeline-rank-contract`는 고정된 vLLM 소스 규칙과 Ray 노드·actor 토폴로지를 결합한 간접 증적입니다. vLLM 내부 rank를 Ray가 직접 보고했다는 뜻으로 기술하지 않습니다.
- 추천기는 같은 품질에서 검증된 exact `STAGE`를 우선하고 독립 `FULL_SNAPSHOT` 후보도 결정론적으로 평가합니다. 수락 뒤에는 두 형식 사이를 묵시적으로 바꾸지 않습니다.
- 중앙 캐시는 `READY`·`STALE`·`MISSING`·`CORRUPT`·`QUARANTINED`로 투영합니다. 완전한 probe만 상태를 악화시킬 수 있고, `READY` 복구는 현재 준비 성공만 가능합니다.
- `artifact-cache` 조회·참조 검사는 읽기 전용입니다. 격리는 preview 뒤 명시적 적용으로 정확한 캐시 하나를 보존 위치로 원자 이동하며 자동 삭제·퇴출·P2P 전송은 하지 않습니다.
- DB·evidence·journal·model cache의 보존·삭제는 [데이터 보존·격리·삭제 정책](data-retention.md)을 따릅니다. node를 교체하거나 폐기할 때 credential·cache·hardware는 [GPU 노드 폐기·교체 운영 절차](node-lifecycle.md)를 따릅니다.

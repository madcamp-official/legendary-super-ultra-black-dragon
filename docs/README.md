# Dure 문서 색인

이 디렉터리는 Dure의 현재 동작, 운영 절차, 보안 경계, 개발 규칙, 향후 계획을 설명합니다. 기능 문서에서 **현재 제공**과 **계획됨**을 구분하며, 계획 문서를 실제 운영 절차로 사용해서는 안 됩니다.

## 시작점

- [루트 README](../README.md): 설치, 로컬 CLI, 중앙 관리의 빠른 시작
- [아키텍처](architecture.md): 로컬 CLI·에이전트·중앙 제어면의 경계와 작업 프로토콜
- [운영 절차](operations.md): 서버 운영, 노드 승인, 세대별 적용·검증·롤백과 장애 복구
- [보안 모델](security.md): 현재 통제와 공개 전 보안 강화 과제
- [vLLM 다중 노드 rank 결합 결정 기록](adr-vllm-multinode-rank-binding.md): `VLLM_RAY_PP_V1`의 고정 소스 계약과 검증 한계

## 모델과 용량

- [모델 선택 정책](model-selection.md): GPU 인벤토리 기반 결정론적 선택기, 모델 레지스트리·승격 게이트, 결정론적 추천 스냅샷, 배포 세대 상태와 명시적 롤백 — 부분 구현
- [벤치마크 및 모델 자격 검증](benchmarking.md): 구조화된 증적, 단일 노드 폐쇄형 실행과 후보 모델의 품질·성능·안정성 승격 기준 — 부분 구현
- [모델 아티팩트 매니페스트와 배포 계약](artifact-distribution.md): 불변 파일·청크 레지스트리, 명시적 중앙 준비 preview·apply·재시도, 노드 `FULL_SNAPSHOT` 캐시와 OCI 이미지 증적, 배포·롤백 소비 게이트
- [vLLM 단계 아티팩트 생성과 검증](stage-artifacts.md): 제한된 vLLM 0.9.0 stage builder, variant·rank 매니페스트와 검증 상태 — 생성·등록 구현, 노드 배포 소비는 다음 PR로 계획됨
- [제품 제안서](dure-proposal.md): 장기 제품 비전과 MVP 가설

## 개발과 배포

- [개발·릴리스 절차](development.md): 테스트, 마이그레이션, Git 훅, 릴리스 규칙
- [APT 배포](apt-distribution.md): Debian 패키지와 서명된 APT 저장소 운영
- [개발 로드맵](roadmap.md): v0.3.6 이후 모델 선택·자격 검증·세대 롤백과 단계별 목표

## 문서 관리 원칙

- 실행 가능한 CLI/API만 현재형으로 기술합니다.
- 계획 중인 기능은 상태와 전제 조건을 표시합니다.
- 모델은 이름만이 아니라 리비전, 양자화, 런타임 이미지 다이제스트, 라이선스를 함께 관리합니다.
- 프롬프트, 자격 증명, 토큰, 실제 비밀값을 예시나 벤치마크 결과에 기록하지 않습니다.
- 모델 릴리스와 배치 프로필의 실제 상태는 중앙 레지스트리가 진실의 원천이며, 이 문서는 정책과 절차를 설명합니다.
- 배포 세대의 `verified_at`은 전체 배정 노드가 검증에 성공하고 backend별 최소 Agent 버전을 충족할 때만 롤백 증거로 사용합니다. legacy는 0.3.12 이상, `VLLM_RAY_PP_V1`은 0.3.18 이상과 엄격한 rank·API 검증이 필요합니다.
- 추천 세대의 apply와 rollback은 배포·노드·매니페스트·exact cache path·OCI 다이제스트에 결합된 성공한 준비 증적을 요구하며, rollback은 네트워크 준비를 수행하지 않습니다.
- stage variant의 `VALIDATED`는 실제 GPU export/load 검증 상태이지 노드 설치나 배포 완료 상태가 아닙니다. 현재 중앙 준비와 Agent는 계속 `FULL_SNAPSHOT`만 소비합니다.
- `VLLM_RAY_PP_V1`은 vLLM 0.9.0 V0 Ray, `TP=1`, 노드별 GPU 한 장, `PP=2/3`만 지원하는 현재 실행 계약입니다. 기존 로컬 계획 JSON과 legacy backend는 그대로 호환됩니다.
- `pipeline-rank-contract`는 고정된 vLLM 소스 규칙과 Ray 노드·actor 토폴로지를 결합한 간접 증적입니다. vLLM 내부 rank를 Ray가 직접 보고했다는 뜻으로 기술하지 않습니다.
- rank별 `STAGE` 로더·노드 활성화와 중앙 캐시 상태 투영은 각각 후속 PR의 계획이며 현재 기능으로 운영하지 않습니다.

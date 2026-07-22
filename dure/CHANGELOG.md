# 변경 이력

이 문서는 Dure 사용자가 알아야 하는 기능 변화, 호환성 영향, 업그레이드 요구 사항과 알려진 제한을 요약합니다. 패키지 관리용 Debian changelog와는 목적이 다릅니다.

`source version`, Git tag, `.deb` 패키지 게시, APT 저장소 게시, 실제 GPU/NCCL 수용 검증은 서로 다른 사실입니다. 이 문서에 적힌 항목만으로 특정 패키지의 출처나 GPU 실행 성공을 증명하지 않습니다. 패키지 출처는 [APT 배포와 신뢰 경로](docs/apt-distribution.md), 릴리스 승인 절차는 [릴리스 거버넌스](docs/release-governance.md), 실행 증적은 [릴리스 증적](docs/release-evidence/README.md)에서 확인합니다.

## Unreleased

### 문서와 운영 기준

- 환경 변수, 설정 파일, systemd 및 수용 검증 opt-in 변수를 한데 모은 [설정 참조서](docs/configuration-reference.md)를 추가했습니다.
- 로컬 CLI와 관리자 CLI의 권한, 호스트 변경 여부, preview/apply 조건을 정리한 [CLI 명령 참조](docs/cli-reference.md)를 추가했습니다.
- 역할 위임, 보안 triage, 릴리스·문서 승인, 행동강령 조치를 정리한 [거버넌스 정책](../GOVERNANCE.md)을 추가했습니다.

### 호환성 영향

- 위 항목은 문서·운영 정책 변화이며 API, 데이터베이스 스키마, Agent 프로토콜 또는 배포 런타임의 호환성 변경을 포함하지 않습니다.

## 0.4.21 — 소스 기준선

현재 저장소의 `0.4.21` 소스 버전을 기준으로 문서 체계를 정비했습니다. 과거 버전의 사용자용 변경 내역은 신뢰할 수 있는 릴리스 메모가 없으므로 소급해서 추정·작성하지 않습니다.

- 이 버전의 패키지 게시 여부와 설치 가능한 APT 경로는 별도로 확인해야 합니다.
- GPU, Docker, Ray/vLLM, NCCL 검증의 통과 여부는 버전별 증적에서 `PASSED`, `FAILED`, `NOT_RUN` 상태로 확인해야 합니다.
- 업그레이드 전에 [호환성·롤링 업그레이드](docs/compatibility-upgrades.md), [릴리스 실행 체크리스트](docs/release-runbook.md), [복구 절차](docs/disaster-recovery.md)를 확인합니다.

## 이후 릴리스 작성 기준

새 릴리스를 만들 때는 해당 버전에 다음 정보를 추가합니다.

1. 사용자에게 보이는 추가·변경·수정·폐기 항목
2. Controller, Agent, DB migration, 배포 계약의 호환성 영향과 최소 업그레이드 순서
3. 운영자가 수행해야 하는 설정·재시작·롤백 조치
4. 아직 지원하지 않거나 검증되지 않은 제한 사항
5. 대응하는 tag, 공식 release asset, 증적 기록의 링크

릴리스 노트에 비밀값, enrollment credential, 관리자 토큰, 노드 내부 주소 또는 민감한 장애 로그를 기록해서는 안 됩니다.

# 보안 정책

## 지원 범위

보안 수정은 canonical `main`과 현재 유지 중인 release branch를 우선 대상으로 검토합니다. 과거 tag,
폐기된 package, 개인·미러 인프라의 계정 보안은 영향을 함께 조사할 수 있지만 자동 지원을 보장하지
않습니다. 현재 package mirror authority와 source authority의 차이는
[릴리스 권한과 출처 관리](dure/docs/release-governance.md)를 따릅니다.

## 취약점 신고

credential 노출, public Ray/vLLM/Controller 노출, 권한 상승, 임의 task 실행, APT signing key 문제,
prompt·개인정보 유출 가능성은 public issue에 상세 내용이나 재현 payload를 올리지 마십시오.

1. GitHub 저장소의 **Security → Report a vulnerability**(Private Vulnerability Reporting)가 보이면
   그 경로로 신고합니다.
2. 이 기능이 보이지 않으면 public issue에는 상세를 쓰지 말고 `Security contact request`라는 최소한의
   제목만 등록해 비공개 연락 경로를 요청합니다. token, IP, URL, exploit, prompt, log를 넣지 않습니다.
3. maintainer가 비공개 경로를 제공한 뒤 영향 범위, 재현에 필요한 최소 단계, 버전·commit, 완화 방법을
   전달합니다.

신고자는 취약점 해결 전까지 공격 절차·비밀값·개인정보를 공개하지 않습니다. 프로젝트는 응답 시점이나
bug bounty를 보장하지 않지만, 영향 확인·완화·수정·공개 시점을 신고자와 조율하는 것을 목표로 합니다.

## 처리 원칙

- 먼저 public ingress·credential·node 영향 범위를 제한하고, 필요하면 credential revoke·rotation을
  수행합니다.
- prompt·secret을 evidence나 issue로 복사하지 않고, redaction된 시간·범위·failure code만 기록합니다.
- 영향 받는 package는 같은 version artifact를 덮어쓰지 않고 안전한 후속 version과 release record로
  처리합니다.
- 보안 수정은 failure-path test, 문서, 필요한 release evidence를 함께 검토합니다.

운영자 보안 경계는 [Dure 보안 모델](dure/docs/security.md), prompt 사고 대응은
[개인정보·프롬프트 처리 정책](dure/docs/data-privacy.md)을 따릅니다.

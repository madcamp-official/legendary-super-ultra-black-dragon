# 릴리스 권한과 출처 관리

이 문서는 source checkout, Git tag, CI build, 서명 key, GitHub Release, APT 저장소가 각각 무엇을
증명하는지 정리합니다. 같은 version 문자열만으로는 설치 package가 공식 승인 source에서 만들어졌다는
증명이 되지 않습니다.

## 현재 배포 경계

| 항목 | 현재 역할 | 이 사실만으로 증명되는 범위 |
| --- | --- | --- |
| `madcamp-official/legendary-super-ultra-black-dragon` | canonical source repository | 검토할 source code와 commit의 기준 |
| source의 `vX.Y.Z` tag | source version 표시 | tag가 가리키는 source commit |
| `chek737/dure` | 현재 Debian package build·archive signing·APT Pages mirror | mirror key가 서명한 package와 APT metadata의 무결성 |
| APT `InRelease` | archive metadata 서명 | 선택한 mirror key가 `Packages` metadata를 서명했다는 사실 |
| `.deb` SHA-256 | package 파일 식별 | 비교한 두 파일이 같은 bytes라는 사실 |

따라서 현재 APT key는 **미러 package**를 인증합니다. canonical source 조직이 그 package를
승인했다는 암호학적 증명은 아닙니다. 미러의 source claim과 package hash가 일치해도, 공식 조직의
보호된 tag·Release asset·attestation이 연결되기 전에는 이를 공식 release라고 표현하지 않습니다.

## Release authority와 provenance

- **Release authority**: 공식 tag 생성, package 서명, release 게시를 승인할 수 있는 주체입니다.
- **Provenance**: 특정 `.deb`가 어느 source commit과 CI run에서 만들어졌는지 따라갈 수 있는
  증거입니다.

안전한 설치 판단에는 둘 다 필요합니다. 다음 연결이 같은 release record에서 확인돼야 합니다.

```text
보호된 공식 tag와 commit
→ 공식 CI build
→ .deb SHA-256과 provenance manifest
→ GitHub Release asset
→ APT Packages SHA-256
→ InRelease archive-key 서명
```

source repository에 publish workflow 파일이 있거나 tag가 존재한다는 사실은 이 연결의 일부일 뿐입니다.
실제 workflow 성공, release asset 게시, archive key custody를 별도로 확인하기 전에는 authority를
추정하지 않습니다.

## 운영자 확인 절차

1. source commit과 tag를 canonical repository에서 검토합니다.
2. 해당 release의 package SHA-256과 provenance manifest 서명을 확인합니다.
3. APT key fingerprint로 `InRelease`와 `Packages` hash를 확인합니다.
4. 설치 후보와 설치된 binary version을 별도로 확인합니다.

```bash
apt-cache policy dure
dure --version
```

5. 실제 GPU 수용 결과는 [릴리스 증적 기록](release-evidence/README.md)에서 같은 source·image·model
   identity에 결합됐는지 확인합니다.

## 공식 authority로 전환할 때의 조건

공식 조직이 APT 배포를 직접 맡으려면 코드 변경 외에 다음 운영 변경이 필요합니다.

- `main` ruleset과 release tag 생성 권한을 공식 release manager 또는 GitHub App으로 제한합니다.
- APT archive private key를 공식 조직의 보호된 environment에만 보관하고, 미러와 개인 계정에
  private key를 두지 않습니다.
- 공식 CI가 `.deb`, `SHA256SUMS`, provenance manifest와 signature 또는 artifact attestation을
  공식 GitHub Release에 함께 게시하게 합니다.
- Pages 또는 별도 package domain의 배포 환경 정책이 tag release를 명시적으로 허용하는지 검증합니다.
- 미러를 계속 쓸 경우 공식 산출물을 hash·서명 검증 후 그대로 복제하는 배포 전용 경로로 제한합니다.

전환이 끝나기 전에는 README, installer, APT 문서에서 현재 mirror URL과 key의 authority를 숨기거나
공식 authority로 표현하지 않습니다. 상세 설치 절차는 [APT 배포](apt-distribution.md)를 따릅니다.

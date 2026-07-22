# 릴리스 실행 체크리스트

이 문서는 source 변경 검토, Debian package·APT 게시, 설치 확인, 실제 GPU 수용 증적 기록을 한
실행 순서로 연결한다. 신뢰 경계 자체는 [릴리스 권한과 출처 관리](release-governance.md), 설치와
미러 절차는 [APT 배포](apt-distribution.md)를 기준으로 한다.

현재 canonical source는 `madcamp-official/legendary-super-ultra-black-dragon`이고, Debian package
build·archive signing·APT Pages 배포는 `chek737/dure` 미러가 담당한다. 이 체크리스트를 통과해도
mirror package가 공식 조직의 암호학적 승인을 받았다는 뜻은 아니다. authority가 이전되기 전에는
모든 기록과 공지에서 **현재 Dure APT 미러**라고 표현한다.

## 역할과 권한

| 역할 | 책임 | 금지 사항 |
| --- | --- | --- |
| 릴리스 책임자 | source commit·변경 범위·Go/No-Go 결정 | key·credential 공유, authority 과장 |
| 패키지 게시자 | 승인된 immutable tag로 build·서명·APT 게시 | branch tip·fork·임의 artifact build |
| 검증 운영자 | 격리 환경 설치·GPU 수용 검증·evidence 작성 | `NOT_RUN`을 성공으로 변경, 운영 workload 실험 |
| key/비밀 관리자 | archive key와 release 권한 보호·회전 | private key·token을 log·asset·ticket에 기록 |

작은 팀에서 역할을 겸할 수 있어도, source 승인·package 게시·GPU 수용 결과는 release record에
구분해 남긴다. 이 문서는 권한을 코드로 부여하지 않으며 tag 생성·서명·배포 권한은 repository와
secrets 관리 정책에서 별도로 제한해야 한다.

## 상태와 중단 기준

| 상태 | 의미 | 다음 행동 |
| --- | --- | --- |
| `DRAFT` | 입력과 검증 계획 준비 중 | tag·게시·운영 적용을 하지 않음 |
| `BLOCKED` | 권한·입력·검증 결과 부족 또는 불일치 | 원인을 기록하고 중단 |
| `READY_TO_PUBLISH` | source와 package 입력 확인 완료 | 권한 있는 게시자만 게시 |
| `PUBLISHED` | mirror package와 APT metadata 게시 완료 | 설치·hash·provenance 확인 |
| `ACCEPTED` | 필요한 실제 수용 검증이 `PASSED` | 해당 지원 범위의 운영 승인 가능 |
| `FAILED` | 게시 또는 수용 검증 실패 | artifact를 덮어쓰지 않고 원인 조사 |

아래 중 하나라도 맞으면 `BLOCKED` 또는 `FAILED`로 닫는다.

- source tag, resolved commit, Debian version, package version 불일치
- CI·package build·APT integration test·checksum·서명 검증 실패
- source authority와 mirror authority를 동일하게 표시하려는 변경
- 같은 Debian version의 package나 APT metadata를 다른 내용으로 교체하려는 시도
- credential, token, private key, private model URL이 log·asset·evidence에 노출됨
- 실제 GPU 결과가 `FAILED` 또는 `NOT_RUN`인데 수용 성공이라고 표시함

## 1. 게시 전 확인

1. 대상 source commit, PR 검토 상태, 변경 요약, Debian version, 예정 tag `v<version>`을 release
   record에 기록한다. source branch, Git tag, 설치 package는 서로 다른 상태다.
2. source checkout에서 다음 검증을 수행한다.

   ```bash
   cd dure
   python3 scripts/check_version_sync.py
   python3 -m compileall -q src tests
   python3 -m unittest discover -v
   python3 scripts/check_docs.py
   git diff --check
   dure-server --database-url sqlite:////tmp/dure-migration-check.db --migrate
   python3 -m pip wheel . --no-deps --no-build-isolation -w /tmp/dure-wheel-check
   ```

   unit suite와 migration smoke 통과는 실제 GPU·Docker·NCCL 수용 증명이 아니다.
3. schema, Agent task, package entry point, runtime image, 모델·배치 profile이 바뀌면
   [버전 호환성과 롤링 업그레이드](compatibility-upgrades.md)를 검토한다.
4. source tag와 package 게시 권한이 다르면, 릴리스 책임자는 확인한 exact commit만 게시자에게
   전달한다. 이동 가능한 branch head를 전달하지 않는다.
5. GPU 수용 검증이 필요한 경우에는 격리 노드, model manifest digest, OCI image digest, evidence
   파일명을 미리 정한다. 실행하지 못하면 `NOT_RUN`으로 기록할 계획을 세운다.

## 2. package와 APT 미러 게시

이 단계는 현재 미러의 권한 있는 게시자만 수행한다. source repository에 workflow가 있거나 tag가
있다는 사실만으로 게시가 일어났다고 판단하지 않는다.

1. resolved source tag·immutable commit·Debian changelog version을 다시 대조한다.
2. package build, unit test, Debian build, disposable APT integration test를 수행한다.
3. `.deb` SHA-256, source repository·tag·commit, build/workflow 식별자, archive-key fingerprint를
   provenance record에 남긴다.
4. archive key로 `InRelease`와 provenance signature를 만들고, 동일한 package·metadata·keyring을
   mirror GitHub Release와 정적 APT tree에 게시한다.
5. 별도 깨끗한 APT root 또는 격리 host에서 `InRelease`, `Packages`, package SHA-256과 설치 version을
   확인한다.

```bash
apt-cache policy dure
dure --version
```

설치 경로와 fingerprint·provenance 검증 명령은 [APT 배포](apt-distribution.md)를 따른다. release
record에는 binary version뿐 아니라 선택 package의 SHA-256과 provenance 확인 결과를 남긴다.

## 3. 게시 후 수용 검증과 기록

1. package 설치 smoke test, `dure --version`, Controller migration smoke를 수행한다.
2. 지원 대상 변경이 있으면 [릴리스 수용 검증](release-validation.md)의 해당 runbook을 격리 검증
   환경에서 실행한다. 운영 generation을 실험 대상으로 쓰지 않는다.
3. 실제 결과는 절차 문서에 덮어쓰지 말고 `docs/release-evidence/v<version>.md`에 `PASSED`,
   `FAILED`, `NOT_RUN` 중 하나로 기록한다. source commit, package version, image·manifest digest,
   실행 시각, 비밀값을 제외한 structured result를 남긴다.
4. 실제 `PASSED` evidence가 필요한 지원 claim만 `ACCEPTED`로 표시한다. evidence가 없는 package는
   `PUBLISHED`이지만 해당 GPU profile에 수용 완료라고 홍보하지 않는다.
5. README, 지원 매트릭스, [변경 이력](../CHANGELOG.md)이 실제 지원 범위와 현재 APT mirror authority에 맞는지 검토한다.

## 실패·재시도·철회

- build·게시가 실패하면 원인을 고친 뒤 새 검증을 시작한다. 이미 게시한 version의 package를
  덮어쓰거나 같은 version으로 재서명하지 않는다.
- GPU 수용 검증이 실패하면 evidence와 generation 상태를 보존한다. 자동 rollback·node 재배정을
  가정하지 말고 [운영 절차](operations.md)의 명시적 복구 경로를 사용한다.
- package 철회는 APT metadata·공지·지원 문서에 영향 version을 명시하고, 안전한 후속 version으로
  수정한다. signing key 침해는 일반 package 재게시가 아니라 key rotation과 trust anchor 교체가
  필요한 incident다.
- 재검증은 현재 package·image·manifest·node binding으로 새 evidence를 만든다. 과거 `PASSED`를
  복사하지 않는다.

## release record 최소 양식

```text
version / source tag / source commit:
릴리스 책임자 / package 게시자 / 검증 운영자:
authority: 현재 Dure APT 미러 | 공식 authority 전환 후에만 공식 경로
검증: unit / migration / Debian·APT / GPU acceptance
package SHA-256 / InRelease fingerprint / provenance signature:
evidence 파일과 상태: PASSED | FAILED | NOT_RUN
Go / No-Go 결정과 시각(UTC):
```

이 기록에는 signing private key, credential, token, private URL, raw prompt를 넣지 않는다.

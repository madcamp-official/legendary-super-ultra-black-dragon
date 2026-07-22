# APT 배포

Dure는 서명된 Debian 패키지 저장소로 배포합니다. 현재 정적 host는 GitHub Pages이고, 생성된
`public/` 디렉터리는 S3, R2, nginx 또는 HTTPS를 제공하는 다른 정적 host에도 올릴 수 있습니다.

## 현재 trust boundary

- Canonical source repository는
  [`madcamp-official/legendary-super-ultra-black-dragon`](https://github.com/madcamp-official/legendary-super-ultra-black-dragon)입니다.
- 현재 Debian package의 build, archive-key 서명, GitHub Pages APT 배포와 release asset 관리는
  [`chek737/dure`](https://github.com/chek737/dure) 미러가 담당합니다.
- 따라서 APT key는 **미러가 게시한 package**의 무결성과 출처를 인증합니다. 이 key 또는 미러
  provenance record는 공식 조직이 package release를 승인했다는 암호학적 증명이 아닙니다.

사용자는 source 코드의 기준과 package 배포자의 권한을 구분해야 합니다. 미러 key·workflow·Pages가
침해되면 공격자는 새 package와 provenance record를 정상적으로 서명할 수 있습니다. 공식 조직의
보호된 tag, Release 또는 artifact attestation이 별도로 도입되기 전에는 이를 공식 승인의 증거로
표시해서는 안 됩니다.

저장소에 공식 publish workflow 정의가 존재하더라도, 그 사실만으로 해당 tag가 실제로 package를
게시했거나 현재 설치 주소의 authority가 공식 조직으로 이전됐다는 뜻은 아닙니다. tag·Release asset·
APT `InRelease`·서명 key의 연결을 release마다 확인해야 합니다. 현재 상태와 공식 authority 전환
조건은 [릴리스 권한과 출처 관리](release-governance.md)에 정리합니다.

## 사용자 설치

편의 installer는 network response를 shell로 바로 연결하지 않습니다. 내려받은 내용을 검토한 뒤
root로 실행합니다.

```bash
curl -fSLo /tmp/dure-install.sh https://chek737.github.io/dure/install.sh
sed -n '1,240p' /tmp/dure-install.sh
sudo sh /tmp/dure-install.sh
rm -f /tmp/dure-install.sh
```

Dure APT archive signing-key fingerprint는 다음과 같습니다.

```text
E1F952F8B23E7A1B884CB5A33EC5C8CAE53AFA01
```

APT 저장소 등록 installer는 공개 서명 키를 `/usr/share/keyrings`에 설치하고, deb822 source를
`/etc/apt/sources.list.d`에 만들며, `apt-get update` 뒤에 Dure를 설치합니다. 현재 저장소와
installer는 `amd64` 전용입니다. Docker, NVIDIA Container Toolkit이나 driver를 설치하지 않고
Agent를 등록하지도 않습니다.

```bash
sudo dure bootstrap
sudo dure bootstrap --apply
sudo dure doctor
sudo dure join
```

bootstrap 엔진 자체는 Ubuntu 22.04·24.04의 `amd64`·`arm64`를 지원하지만, APT에서 `arm64`
package를 게시하려면 저장소 생성·Release metadata·통합 테스트를 함께 확장해야 합니다. 그 전의
별도 설치는 CLI·Agent 실행 파일뿐 아니라 `dure-agent.service`와 `/etc/dure/dure-client.env`까지
제공해야 합니다.

등록 이후 Dure만 업그레이드할 때는 package 범위를 제한합니다.

```bash
sudo apt install dure
sudo apt update
sudo apt install --only-upgrade dure
```

호스트 전체 `apt upgrade`는 Docker CE를 함께 갱신하고 daemon을 재시작할 수 있는 별도 유지보수
작업입니다. 실행 workload와 방화벽 영향을 검토하고 유지보수 시간에 수행합니다.

### 수동 등록

installer를 실행하지 않으려면 key fingerprint를 확인한 뒤 수동으로 등록할 수 있습니다.

```bash
curl -fSLo /tmp/dure-archive-keyring.gpg \
  https://chek737.github.io/dure/dure-archive-keyring.gpg
gpg --show-keys --with-fingerprint /tmp/dure-archive-keyring.gpg
sudo install -m 0644 /tmp/dure-archive-keyring.gpg \
  /usr/share/keyrings/dure-archive-keyring.gpg
rm -f /tmp/dure-archive-keyring.gpg

sudo tee /etc/apt/sources.list.d/dure.sources >/dev/null <<'EOF'
Types: deb
URIs: https://chek737.github.io/dure
Suites: stable
Components: main
Architectures: amd64
Signed-By: /usr/share/keyrings/dure-archive-keyring.gpg
EOF

sudo apt update
sudo apt install dure
```

`gpg --show-keys` 출력의 fingerprint가 이 문서의 값과 정확히 같은지 확인합니다. APT는 이 keyring으로
`InRelease`를, `Packages`에 기록된 SHA-256으로 내려받은 `.deb`를 검증합니다.

## 미러 provenance record

미러 release가 다음 asset를 제공하는 경우, package를 설치하기 전에 배포 record를 추가로 검토할 수
있습니다.

- `dure_<version>_all.deb` — 설치 package
- `release-provenance.json` — canonical source tag·commit, mirror workflow run, package SHA-256과 key fingerprint
- `release-provenance.json.asc` — mirror archive key로 만든 detached signature
- `dure-archive-keyring.gpg` — provenance와 APT metadata를 검증할 public keyring

먼저 manifest가 미러 key로 서명되었는지 검증합니다.

```bash
gpgv --keyring dure-archive-keyring.gpg \
  release-provenance.json.asc release-provenance.json
sha256sum dure_<version>_all.deb
```

그 다음 JSON의 `artifact.sha256`이 package hash와 같은지, `source.repository`, `source.tag`,
`source.commit`이 검토한 canonical source와 같은지 확인합니다. 미러 repository checkout에서는
`tools/release_provenance.py verify`가 모든 claim과 package hash를 함께 검사합니다.

이 record가 보장하는 것은 “이 mirror key가 이 package와 명시된 source claim을 승인했다”는 사실입니다.
`source.commit`은 source-to-package 추적을 돕지만, source 조직의 approval·보호된 tag·release
attestation을 대신하지 않습니다. `InRelease` 검증도 계속 필요합니다.

## 미러 배포자 절차

1. `chek737/dure`의 `Publish signed Dure APT mirror` workflow에서 source tag를 입력하거나 같은
   이름의 `v<version>` mirror tag를 push합니다.
2. workflow는 canonical source repository에서 그 tag를 checkout하고 tag가 가리킨 immutable commit과
   Debian changelog version이 정확히 일치하는지 확인합니다.
3. checkout한 source 자체의 test, Debian build와 disposable APT integration test를 실행합니다.
4. mirror archive key로 APT `InRelease`와 provenance JSON detached signature를 만듭니다. manifest는
   source tag·commit, mirror workflow run URL, package SHA-256, mirror URL과 key fingerprint를 기록합니다.
5. 검증한 `public/` tree를 GitHub Pages에 배포하고 package, provenance JSON·signature·keyring을
   같은 mirror GitHub Release에 올립니다.

source URL, branch tip, fork artifact 또는 임의 package를 입력으로 받아 build하면 안 됩니다. workflow는
정확한 source tag와 resolved commit만 허용해야 하며, 같은 Debian version을 다른 package 내용에
재사용해서는 안 됩니다.

## 로컬 저장소 빌드

build dependency를 설치합니다.

```bash
sudo apt install apt-utils debhelper dh-python dpkg-dev gnupg python3-all python3-setuptools
```

package와 repository를 build합니다.

```bash
package=$(scripts/build-deb.sh)
scripts/build-apt-repo.sh "$package" public stable main <GPG_KEY_ID>
scripts/render-installer.sh https://packages.example.com/dure public/install.sh
```

임시 signing key를 사용한 APT end-to-end integration test:

```bash
scripts/test-apt-repo.sh "$package"
```

생성된 `public/`은 구성한 HTTPS URL root에 업로드합니다. signing private key를 source repository,
fork, 일반 CI 또는 검증 전용 cache mirror에 복사하지 않습니다.

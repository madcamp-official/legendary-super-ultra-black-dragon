# APT 배포

Dure는 서명된 Debian 패키지 저장소로 배포합니다. 기본 정적 host는 GitHub Pages이지만, 생성된 `public/` 디렉터리는 S3, R2, nginx 또는 HTTPS를 제공하는 다른 정적 host에도 그대로 올릴 수 있습니다.

## 사용자 설치

공식 release authority와 기본 APT origin은
`https://madcamp-official.github.io/legendary-super-ultra-black-dragon`입니다. 이 URL의
`InRelease`, package 및 release provenance는 공식 저장소
`madcamp-official/legendary-super-ultra-black-dragon`의 protected tag release에서만 생성됩니다.
개인 fork·예전 미러 URL은 새 공식 릴리스의 authority가 아닙니다.

편의 installer를 쓰려면 내용을 먼저 내려받아 검토한 뒤 root로 실행합니다. network response를
바로 shell로 연결하지 않습니다.

```bash
curl -fSLo /tmp/dure-install.sh \
  https://madcamp-official.github.io/legendary-super-ultra-black-dragon/install.sh
sed -n '1,240p' /tmp/dure-install.sh
sudo sh /tmp/dure-install.sh
rm -f /tmp/dure-install.sh
```

Dure APT 서명 키 fingerprint는 다음과 같습니다.

```text
E1F952F8B23E7A1B884CB5A33EC5C8CAE53AFA01
```

APT 저장소 등록 installer는 공개 서명 키를 `/usr/share/keyrings`에 설치하고, deb822 source를 `/etc/apt/sources.list.d`에 만들며, `apt-get update` 뒤에 Dure를 설치합니다. 현재 저장소와 installer는 `amd64` 전용입니다.

이 installer는 Docker, NVIDIA Container Toolkit이나 driver를 설치하지 않고 Agent를 등록하지도 않습니다. GPU 노드는 Dure 설치 뒤 별도 로컬 preview와 명시적 apply를 수행합니다.

```bash
sudo dure bootstrap
sudo dure bootstrap --apply
sudo dure doctor
sudo dure join
```

bootstrap 엔진 자체는 Ubuntu 22.04·24.04의 `amd64`·`arm64`를 지원하지만, 공식 APT에서 `arm64` Dure 패키지를 게시하려면 저장소 생성·Release metadata·통합 테스트를 함께 확장해야 합니다. 그 전의 별도 설치는 CLI·Agent 실행 파일만이 아니라 `dure-agent.service`와 `/etc/dure/dure-client.env`까지 제공해야 합니다.

등록 이후 Dure만 업그레이드할 때는 패키지 범위를 제한합니다.

```bash
sudo apt install dure
sudo apt update
sudo apt install --only-upgrade dure
```

호스트 전체 `apt upgrade`는 Docker CE를 함께 갱신하고 daemon을 재시작할 수 있는 별도 유지보수 작업입니다. 실행 workload와 방화벽 영향을 검토하고 유지보수 시간에 수행합니다. bootstrap이 설치한 NVIDIA Container Toolkit 네 패키지는 검증 버전 pin을 유지하며, 다른 버전이 필요하면 pin과 Dure 지원 범위를 먼저 갱신해 검증합니다.

APT 저장소 등록 installer를 실행하지 않으려면 저장소를 수동으로 등록할 수 있습니다.

```bash
curl -fSLo /tmp/dure-archive-keyring.gpg \
  https://madcamp-official.github.io/legendary-super-ultra-black-dragon/dure-archive-keyring.gpg
gpg --show-keys --with-fingerprint /tmp/dure-archive-keyring.gpg
sudo install -m 0644 /tmp/dure-archive-keyring.gpg \
  /usr/share/keyrings/dure-archive-keyring.gpg
rm -f /tmp/dure-archive-keyring.gpg

sudo tee /etc/apt/sources.list.d/dure.sources >/dev/null <<'EOF'
Types: deb
URIs: https://madcamp-official.github.io/legendary-super-ultra-black-dragon
Suites: stable
Components: main
Architectures: amd64
Signed-By: /usr/share/keyrings/dure-archive-keyring.gpg
EOF

sudo apt update
sudo apt install dure
```

`gpg --show-keys` 출력의 fingerprint가 이 문서의 fingerprint와 정확히 같은지 확인합니다. APT는
그 keyring으로 `InRelease`를 확인하고, `Packages`에 기록된 SHA-256으로 다운로드한 `.deb`를
확인합니다.

## 릴리스 provenance 확인

각 공식 release에는 다음 asset가 함께 게시됩니다.

- `dure_<version>_all.deb` — 설치 패키지
- `release-provenance.json` — tag, source commit, workflow run, package SHA-256, APT key fingerprint
- `release-provenance.json.asc` — archive key로 만든 detached signature
- `dure-archive-keyring.gpg` — provenance와 APT metadata를 검증할 public keyring

GitHub Actions가 만든 package라는 사실은 GitHub artifact attestation으로, APT key가 승인한
source-to-package binding은 provenance signature로 독립 검증합니다. 예를 들어 release asset을
다운로드한 디렉터리에서 다음을 실행합니다.

```bash
gpgv --keyring dure-archive-keyring.gpg \
  release-provenance.json.asc release-provenance.json

gh attestation verify dure_0.4.17_all.deb \
  --repo madcamp-official/legendary-super-ultra-black-dragon
```

그 뒤 `release-provenance.json`의 `artifact.sha256`이 `.deb`의 `sha256sum`과 같은지, `release.tag`,
`release.source.commit`, `build.run_url`이 검토한 공식 tag와 workflow run인지 확인합니다. 저장소
checkout에서는 아래 도구가 모든 JSON claim과 package hash를 함께 확인합니다.

```bash
python3 scripts/release_provenance.py verify \
  --manifest release-provenance.json \
  --package dure_0.4.17_all.deb \
  --version 0.4.17 \
  --tag v0.4.17 \
  --source-repository https://github.com/madcamp-official/legendary-super-ultra-black-dragon \
  --source-commit <40-character-commit> \
  --workflow-run-url https://github.com/madcamp-official/legendary-super-ultra-black-dragon/actions/runs/<run-id> \
  --signing-key-fingerprint E1F952F8B23E7A1B884CB5A33EC5C8CAE53AFA01
```

이 검증은 `InRelease` 검증을 대체하지 않습니다. 소비자는 APT 설치 때 둘 다 유지해야 합니다.

## 공식 authority와 미러 운영

릴리스 authority는 source, protected tag, build attestation, APT signing key, GitHub Release와
Pages deploy를 모두 가진 공식 조직입니다. 개인 미러는 package를 clone·build·재서명하거나 자체
key를 사용자에게 신뢰시키면 안 됩니다. 미러가 계속 필요하다면 다음의 *검증 후 정적 복제* 역할만
가질 수 있습니다.

1. 공식 GitHub Release의 `.deb`, provenance JSON·signature·keyring을 가져와 `gpgv`와 GitHub
   attestation을 확인합니다.
2. JSON의 package SHA-256과 source commit을 다시 확인하고, 공식 Pages의 `InRelease`와
   `Packages`가 같은 package digest를 참조하는지 확인합니다.
3. 검증한 Pages tree 전체를 새 staging directory에 복사하고, 검증 뒤 원자적으로 static host를
   교체합니다. 임의 URL, 브랜치 tip, fork artifact를 입력으로 받지 않습니다.
4. 미러에는 archive private key, release creation 권한, source build 권한을 주지 않습니다.

공식 origin으로 전환한 뒤에는 README, installer, release notes가 미러 URL을 공식 install URL로
표시하지 않는지 점검합니다. 미러 URL을 계속 제공해야 하면 "cache mirror"라고만 표시하고
official URL과 provenance 검증 방법을 함께 제공합니다.

## 배포자 준비

1. 공식 조직의 repository에서 Pages source를 GitHub Actions로 지정합니다.
2. `main` ruleset에서 `Required CI`, pull request, 최신 base, review conversation 해결을 요구하고,
   `v*` tag ruleset은 release manager 또는 전용 GitHub App만 생성·갱신하도록 제한합니다.
3. `github-pages` environment는 **Selected tags**의 `v*`만 허용하고, release approver를 요구하며,
   self-review와 administrator bypass를 끕니다. 기존 `main`만 허용하는 Pages rule은 tag release를
   job 시작 전에 막으므로 반드시 바꿉니다.
4. offline primary key와 release 전용 signing subkey를 분리합니다. ASCII-armored subkey는
   `github-pages` environment secret `APT_GPG_PRIVATE_KEY`에만 넣고 repository-level secret은
   제거합니다. private key를 미러·fork·일반 CI에 복사하지 않습니다.
5. 같은 environment variable `DURE_APT_GPG_FINGERPRINT`에 공백 없는 public fingerprint를
   등록합니다. workflow는 import한 private key가 이 값과 정확히 일치하지 않으면 중단합니다.
6. `DURE_APT_REPOSITORY_URL`은 공식 custom domain을 실제로 이전한 경우에만 같은 protected
   environment variable로 설정합니다. 비어 있으면 official GitHub Pages URL을 사용합니다.
7. GitHub Release의 immutable release policy를 켜고, Debian changelog version과 일치하는
   protected tag(예: `v0.4.17`)만 push합니다.

workflow의 `build-and-attest` job은 read-only이며 test, Debian build와 GitHub Sigstore attestation만
만듭니다. `publish` job은 environment 승인 뒤에만 package attestation을 source commit·workflow와
대조하고, signing key를 import해 APT repository와 detached provenance signature를 만듭니다. Pages
deploy 성공 전에는 GitHub Release를 draft로만 만들고, 모든 asset이 기존 draft와 byte-for-byte
일치할 때만 재시도합니다. Pages까지 성공한 뒤 draft를 publish하므로 release asset을 `--clobber`로
교체하지 않습니다.

개발용 키 생성 예시:

```bash
gpg --quick-generate-key "Dure APT Repository <packages@example.com>" rsa3072 sign 2y
gpg --armor --export-secret-keys <KEY_ID>
gpg --armor --export <KEY_ID>
```

CI는 키를 non-interactive하게 사용할 수 있어야 합니다. 운영 환경에서는 offline primary key를 분리하고, GitHub Secret에는 전용 signing subkey만 넣습니다. 저장소나 secret이 침해되면 키를 rotate·revoke합니다.

## 로컬 저장소 빌드

build dependency를 설치합니다.

```bash
sudo apt install apt-utils debhelper dh-python dpkg-dev gnupg python3-all python3-setuptools
```

패키지와 저장소를 build합니다.

```bash
package=$(scripts/build-deb.sh)
scripts/build-apt-repo.sh "$package" public stable main <GPG_KEY_ID>
scripts/render-installer.sh https://packages.example.com/dure public/install.sh
```

임시 서명 키를 사용한 APT end-to-end integration test:

```bash
scripts/test-apt-repo.sh "$package"
```

생성된 `public/`의 내용을 구성한 HTTPS URL의 root에 업로드합니다. 공식 workflow를 우회해
수동으로 다른 package를 넣거나, 이 tree를 새 key로 재서명해 미러에 올리면 provenance chain이
끊깁니다.

## 릴리스 절차

1. `pyproject.toml`, `setup.py`, `src/dure/__init__.py`, `debian/changelog`의 version을 일치시킵니다.
2. unit test와 package build를 로컬에서 실행합니다.
3. 의도한 범위만 commit하고 version branch를 push합니다.
4. 사용자 승인 후 `v<debian-version>` tag를 만듭니다.
5. tag를 `git push origin v<debian-version>`으로 push합니다.
6. protected environment 승인 뒤 GitHub Pages 배포, official Release asset, `InRelease`, provenance
   signature, GitHub attestation, `apt update`, 신규 설치, 이전 버전에서의 upgrade를 확인합니다.

같은 Debian version을 다른 package 내용에 재사용하면 안 됩니다. APT는 version 하나가 불변 artifact 하나를 식별한다고 가정합니다.

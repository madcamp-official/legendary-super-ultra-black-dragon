# APT 배포

Dure는 서명된 Debian 패키지 저장소로 배포합니다. 기본 정적 host는 GitHub Pages이지만, 생성된 `public/` 디렉터리는 S3, R2, nginx 또는 HTTPS를 제공하는 다른 정적 host에도 그대로 올릴 수 있습니다.

## 사용자 설치

저장소가 게시된 뒤에는 다음 명령으로 한 번에 등록·설치할 수 있습니다.

```bash
curl -fsSL https://chek737.github.io/dure/install.sh | sudo sh
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
curl -fsSL https://chek737.github.io/dure/dure-archive-keyring.gpg \
  | sudo tee /usr/share/keyrings/dure-archive-keyring.gpg >/dev/null

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

## 배포자 준비

1. 공개 GitHub 저장소를 만들고 프로젝트를 push합니다.
2. 저장소 설정에서 Pages source를 GitHub Actions로 지정합니다.
3. 전용 APT 서명 키를 만듭니다. 개인 identity 대신 release 전용 signing subkey를 권장합니다.
4. ASCII-armored private key를 Actions secret `APT_GPG_PRIVATE_KEY`로 등록합니다.
5. Debian changelog version과 일치하는 tag(예: `v0.3.5`)를 push합니다.

저장소 URL은 `https://OWNER.github.io/REPOSITORY`로 자동 계산됩니다. custom domain을 쓴다면 release 전에 `.github/workflows/publish-apt.yml`의 `repository_url` 계산 방식을 조정합니다.

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

생성된 `public/`의 내용을 구성한 HTTPS URL의 root에 업로드합니다.

## 릴리스 절차

1. `pyproject.toml`, `setup.py`, `src/dure/__init__.py`, `debian/changelog`의 version을 일치시킵니다.
2. unit test와 package build를 로컬에서 실행합니다.
3. 의도한 범위만 commit하고 version branch를 push합니다.
4. 사용자 승인 후 `v<debian-version>` tag를 만듭니다.
5. tag를 `git push origin v<debian-version>`으로 push합니다.
6. GitHub Pages 배포, Release asset, `apt update`, 신규 설치, 이전 버전에서의 upgrade를 확인합니다.

같은 Debian version을 다른 package 내용에 재사용하면 안 됩니다. APT는 version 하나가 불변 artifact 하나를 식별한다고 가정합니다.

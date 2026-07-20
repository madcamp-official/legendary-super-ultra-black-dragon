# APT distribution

Dure is distributed as a signed Debian package repository. GitHub Pages is the default static host, but the generated `public/` directory can be uploaded unchanged to S3, R2, nginx, or any HTTPS static host.

## User installation

After the repository is published, users can bootstrap it with:

```bash
curl -fsSL https://chek737.github.io/dure/install.sh | sudo sh
```

The Dure APT signing-key fingerprint is
`E1F952F8B23E7A1B884CB5A33EC5C8CAE53AFA01`.

The installer places the public signing key in `/usr/share/keyrings`, creates a deb822 source under `/etc/apt/sources.list.d`, runs `apt-get update`, and installs Dure.

After the one-time registration, normal APT commands work:

```bash
sudo apt install dure
sudo apt update
sudo apt upgrade
```

Users who do not want to run a bootstrap script can register the repository manually:

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

## Publisher setup

1. Create a public GitHub repository and push this project.
2. In repository settings, configure Pages to use GitHub Actions.
3. Generate a dedicated APT signing key. Use a release-only signing subkey rather than a personal identity key.
4. Add the ASCII-armored private key as the Actions secret `APT_GPG_PRIVATE_KEY`.
5. Push a tag matching the Debian changelog version, such as `v0.1.0`.

The repository URL is derived automatically as
`https://OWNER.github.io/REPOSITORY`. If you publish under a custom domain,
change `repository_url` in `.github/workflows/publish-apt.yml` before releasing.

Example key creation for development:

```bash
gpg --quick-generate-key "Dure APT Repository <packages@example.com>" rsa3072 sign 2y
gpg --armor --export-secret-keys <KEY_ID>
gpg --armor --export <KEY_ID>
```

The CI environment must be able to use the key non-interactively. For production, keep the offline primary key separate and upload only a dedicated signing subkey to GitHub Secrets. Rotate and revoke it if the repository or secret is compromised.

## Local repository build

Install build dependencies:

```bash
sudo apt install apt-utils debhelper dh-python dpkg-dev gnupg python3-all python3-setuptools
```

Build the package and repository:

```bash
package=$(scripts/build-deb.sh)
scripts/build-apt-repo.sh "$package" public stable main <GPG_KEY_ID>
scripts/render-installer.sh https://packages.example.com/dure public/install.sh
```

Run the end-to-end APT integration test with an ephemeral signing key:

```bash
scripts/test-apt-repo.sh "$package"
```

Upload the contents of `public/` to the root of the configured HTTPS URL.

## Release process

1. Update `pyproject.toml`, `setup.py`, `src/dure/__init__.py`, and `debian/changelog` to the same version.
2. Run the unit tests and build the package locally.
3. Commit and push the release.
4. Tag the commit, for example `git tag v0.1.1`.
5. Push the tag with `git push origin v0.1.1`.
6. Verify the GitHub Pages deployment, Release asset, `apt update`, fresh install, and upgrade from the previous version.

Do not reuse the same Debian version for different package contents. APT assumes a version uniquely identifies an immutable artifact.

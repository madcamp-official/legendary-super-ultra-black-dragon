#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <deb-path>" >&2
  exit 2
fi

project_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
deb_path=$(realpath "$1")
temporary_dir=$(mktemp -d /tmp/dure-apt-test.XXXXXX)

cleanup() {
  find "$temporary_dir" -depth -delete 2>/dev/null || true
}
trap cleanup EXIT INT TERM

chmod 0755 "$temporary_dir"
mkdir -m 0700 "$temporary_dir/gnupg"
export GNUPGHOME="$temporary_dir/gnupg"

gpg --batch --passphrase '' --quick-generate-key \
  'Dure APT Integration Test <apt-test@dure.invalid>' rsa2048 sign 1d
key_id=$(gpg --batch --with-colons --list-secret-keys | \
  awk -F: '$1 == "sec" && !found { print $5; found=1 }')
test -n "$key_id"

"$project_dir/scripts/build-apt-repo.sh" \
  "$deb_path" "$temporary_dir/repo" stable main "$key_id"
gpgv --keyring "$temporary_dir/repo/dure-archive-keyring.gpg" \
  "$temporary_dir/repo/dists/stable/InRelease"

mkdir -p \
  "$temporary_dir/apt/lists/partial" \
  "$temporary_dir/apt/cache/archives/partial" \
  "$temporary_dir/download"
touch "$temporary_dir/status"
printf 'deb [arch=amd64 signed-by=%s] file://%s stable main\n' \
  "$temporary_dir/repo/dure-archive-keyring.gpg" \
  "$temporary_dir/repo" > "$temporary_dir/sources.list"

apt_options=(
  -o "Dir::Etc::sourcelist=$temporary_dir/sources.list"
  -o 'Dir::Etc::sourceparts=-'
  -o "Dir::State::lists=$temporary_dir/apt/lists"
  -o "Dir::State::status=$temporary_dir/status"
  -o "Dir::Cache=$temporary_dir/apt/cache"
  -o 'APT::Get::List-Cleanup=0'
  -o 'APT::Sandbox::User=root'
)

apt-get "${apt_options[@]}" update
expected_version=$(dpkg-deb -f "$deb_path" Version)
candidate=$(apt-cache "${apt_options[@]}" policy dure | \
  awk '/Candidate:/ && !found { print $2; found=1 }')
if [[ "$candidate" != "$expected_version" ]]; then
  echo "Expected APT candidate $expected_version, found $candidate" >&2
  exit 1
fi

(
  cd "$temporary_dir/download"
  apt-get "${apt_options[@]}" download "dure=$expected_version"
)
downloaded_package=$(find "$temporary_dir/download" -maxdepth 1 \
  -type f -name 'dure_*.deb' -print -quit)
test -n "$downloaded_package"
cmp "$deb_path" "$downloaded_package"

echo "APT integration test passed for dure $expected_version"

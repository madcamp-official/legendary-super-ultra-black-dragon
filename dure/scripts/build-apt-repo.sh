#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 <deb-path> <output-dir> <suite> <component> <gpg-key-id>" >&2
}

if [[ $# -ne 5 ]]; then
  usage
  exit 2
fi

deb_path=$(realpath "$1")
output_dir=$2
suite=$3
component=$4
gpg_key_id=$5
architecture=${DURE_APT_ARCHITECTURE:-amd64}

for command in dpkg-deb dpkg-scanpackages apt-ftparchive gpg gzip; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "Missing required command: $command" >&2
    exit 1
  fi
done

if [[ ! -f "$deb_path" ]]; then
  echo "Package not found: $deb_path" >&2
  exit 1
fi

if [[ -e "$output_dir" ]] && find "$output_dir" -mindepth 1 -print -quit | grep -q .; then
  echo "Output directory must be empty: $output_dir" >&2
  exit 1
fi

package_name=$(dpkg-deb -f "$deb_path" Package)
package_version=$(dpkg-deb -f "$deb_path" Version)
package_arch=$(dpkg-deb -f "$deb_path" Architecture)
if [[ "$package_name" != "dure" ]]; then
  echo "Unexpected package name: $package_name" >&2
  exit 1
fi
if [[ "$package_arch" != "all" && "$package_arch" != "$architecture" ]]; then
  echo "Package architecture $package_arch is not compatible with $architecture" >&2
  exit 1
fi

pool_dir="$output_dir/pool/main/d/dure"
binary_dir="$output_dir/dists/$suite/$component/binary-$architecture"
mkdir -p "$pool_dir" "$binary_dir"
install -m 0644 "$deb_path" "$pool_dir/dure_${package_version}_${package_arch}.deb"

(
  cd "$output_dir"
  dpkg-scanpackages --multiversion pool /dev/null > \
    "dists/$suite/$component/binary-$architecture/Packages"
)
gzip -9n -c "$binary_dir/Packages" > "$binary_dir/Packages.gz"

release_dir="$output_dir/dists/$suite"
apt-ftparchive \
  -o APT::FTPArchive::Release::Origin="Dure" \
  -o APT::FTPArchive::Release::Label="Dure" \
  -o APT::FTPArchive::Release::Suite="$suite" \
  -o APT::FTPArchive::Release::Codename="$suite" \
  -o APT::FTPArchive::Release::Architectures="$architecture" \
  -o APT::FTPArchive::Release::Components="$component" \
  -o APT::FTPArchive::Release::Description="Dure community LLM packages" \
  release "$release_dir" > "$release_dir/Release"

gpg --batch --yes --local-user "$gpg_key_id" \
  --output "$release_dir/InRelease" --clearsign "$release_dir/Release"
gpg --batch --yes --local-user "$gpg_key_id" \
  --output "$release_dir/Release.gpg" --armor --detach-sign "$release_dir/Release"
gpg --batch --yes --output "$output_dir/dure-archive-keyring.gpg" \
  --export "$gpg_key_id"

echo "Built signed APT repository for $package_name $package_version in $output_dir"

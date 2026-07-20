#!/usr/bin/env bash
set -euo pipefail

project_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$project_dir"

dpkg-buildpackage -us -uc -b >&2

version=$(dpkg-parsechangelog -S Version)
package="$project_dir/../dure_${version}_all.deb"
if [[ ! -f "$package" ]]; then
  echo "Expected package was not produced: $package" >&2
  exit 1
fi

echo "$package"

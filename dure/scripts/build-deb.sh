#!/usr/bin/env bash
set -euo pipefail

project_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$project_dir"

if [[ -z "${DURE_BUILD_COMMIT:-}" ]]; then
  DURE_BUILD_COMMIT=$(git rev-parse --verify HEAD)
fi
if [[ ! "$DURE_BUILD_COMMIT" =~ ^[0-9a-f]{40,64}$ ]]; then
  echo "DURE_BUILD_COMMIT must be an immutable 40-64 character commit hash" >&2
  exit 1
fi
export DURE_BUILD_COMMIT

dpkg-buildpackage -us -uc -b >&2

version=$(dpkg-parsechangelog -S Version)
package="$project_dir/../dure_${version}_all.deb"
if [[ ! -f "$package" ]]; then
  echo "Expected package was not produced: $package" >&2
  exit 1
fi

echo "$package"

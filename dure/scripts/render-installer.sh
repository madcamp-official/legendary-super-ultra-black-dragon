#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <repository-url> <output-file>" >&2
  exit 2
fi

project_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
repository_url=${1%/}
output_file=$2

escaped_url=${repository_url//|/\\|}
sed "s|@DURE_REPOSITORY_URL@|$escaped_url|g" \
  "$project_dir/packaging/install.sh.in" > "$output_file"
chmod 0755 "$output_file"


#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
kani_command=${OZZY_KANI_BIN:-cargo-kani}
if ! command -v "$kani_command" > /dev/null; then
    echo "Missing cargo-kani. Install Kani or set OZZY_KANI_BIN to its path." >&2
    exit 1
fi
cd "$repo_dir"
exec "$kani_command" kani -p ozzy-journal -p ozzy-core --lib --output-format terse

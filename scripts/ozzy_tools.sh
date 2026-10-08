#!/usr/bin/env bash
# Artifact paths shared by the test suite and Cargo launcher.
ozzy_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ozzy_checkout_key="$(printf '%s' "$ozzy_repo_root" | sha256sum)"
ozzy_artifact_root="${OZZY_ARTIFACT_ROOT:-${TMPDIR:-/tmp}}"
if [[ "$ozzy_artifact_root" != /* ]]; then
    ozzy_artifact_root="$ozzy_repo_root/$ozzy_artifact_root"
fi
export OZZY_ARTIFACT_ROOT="$ozzy_artifact_root"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$ozzy_artifact_root/cargo-target/checkouts/${ozzy_checkout_key%% *}}"
export TMPDIR="${TMPDIR:-$ozzy_artifact_root}"
ozzy_tool_dir="$CARGO_TARGET_DIR/ozzy-tools"
mkdir -p "$ozzy_artifact_root" "$ozzy_tool_dir"

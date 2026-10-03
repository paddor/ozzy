#!/usr/bin/env bash
# SSD artifact paths shared by the test suite and Cargo launcher.
ozzy_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ozzy_checkout_key="$(printf '%s' "$ozzy_repo_root" | sha256sum)"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/mnt/ssd/tmp/cargo-target/checkouts/${ozzy_checkout_key%% *}}"
export TMPDIR="${TMPDIR:-/mnt/ssd/tmp}"
ozzy_tool_dir="$CARGO_TARGET_DIR/ozzy-tools"
mkdir -p "$ozzy_tool_dir"

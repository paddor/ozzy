#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/ozzy_tools.sh"
cd "$ozzy_repo_root"
if [[ "${OZZY_TEST_ALL_REEXEC:-}" != 1 ]]; then
    ln -sfn "$(command -v bash)" "$ozzy_tool_dir/ozzy_test_all"
    export OZZY_TEST_ALL_REEXEC=1
    exec "$ozzy_tool_dir/ozzy_test_all" "$ozzy_repo_root/scripts/test-all.sh" "$@"
fi

phase=${1:-all}
if [[ $# -gt 1 || ! "$phase" =~ ^(all|lint|tests)$ ]]; then
    printf 'Usage: %s [all|lint|tests]\n' "$0" >&2
    exit 2
fi

check_phase() {
    local phase=$1
    shift
    local TIMEFORMAT="check=$phase elapsed=%3Rs user=%3Us system=%3Ss"
    time "$@"
}

if [[ "$phase" != tests ]]; then
    check_phase format cargo fmt --all -- --check
    check_phase lint cargo clippy --workspace --all-targets -- -D warnings
fi
if [[ "$phase" == lint ]]; then
    exit 0
fi

check_phase tests cargo nextest run --workspace --test-threads 8 \
    --status-level fail --final-status-level fail
check_phase doctests cargo test --workspace --doc
RUSTDOCFLAGS='-D warnings' check_phase docs cargo doc \
    -p ozzy \
    -p ozzy-runtime \
    -p ozzy-proto \
    --no-deps

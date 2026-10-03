#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
tlc_jar=${OZZY_TLC_JAR:-"$repo_dir/target/verification/tla2tools-1.7.2.jar"}
if [[ ! -f "$tlc_jar" ]]; then
    echo "Missing TLC jar. Set OZZY_TLC_JAR; see verification/README.md." >&2
    exit 1
fi
mkdir -p "$repo_dir/target/verification"
run_dir=$(mktemp -d "$repo_dir/target/verification/run-XXXXXX")

check_model() {
    local model=$1 config=$2 expected=${3:-} status=0
    local log_file="$run_dir/$config.log"
    java -Xmx512m -XX:+UseParallelGC -cp "$tlc_jar" tlc2.TLC -workers 1 -seed 1 -fp 0 \
        -metadir "$run_dir/$config.states" \
        -config "$repo_dir/verification/$config.cfg" \
        "$repo_dir/verification/$model.tla" > "$log_file" 2>&1 || status=$?
    cat "$log_file"
    if [[ -z "$expected" ]]; then
        [[ $status -eq 0 ]] || return "$status"
    else
        [[ $status -ne 0 ]] && rg -F -q "$expected" "$log_file"
    fi
}

check_model LocalCommit LocalCommit
check_model LocalCommit LocalCommit.broken 'Error: Invariant PrefixOrder is violated.'
check_model PrimaryRestart PrimaryRestart
check_model PrimaryRestart PrimaryRestart.broken 'Error: Invariant SlotIdentity is violated.'
check_model PrepareReplacement PrepareReplacement
check_model PrepareReplacement PrepareReplacement.broken 'Error: Deadlock reached.'
check_model ExitView ExitView
check_model ExitView ExitView.broken 'Error: Temporal properties were violated.'
check_model WriteLane WriteLane
check_model WriteLane WriteLane.broken 'Error: Deadlock reached.'
echo "All corrected models passed; all negative controls failed as expected."
echo "TLC logs: $run_dir"

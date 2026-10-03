#!/usr/bin/env bash
# Focused development checks. test-all.sh remains the full validation gate.
set -euo pipefail
source "$(dirname "$0")/ozzy_tools.sh"
cd "$ozzy_repo_root"

scope=${1:-unit}
if (($#)); then shift; fi
case "$scope" in
    unit) args=(--workspace --lib) ;;
    core) args=(-p ozzy-core -p ozzy-proto -p ozzy-journal -p ozzy-replication) ;;
    writer) args=(-p ozzy-runtime --lib 'replicated::writer::') ;;
    local) args=(-p ozzy-broker --test ozzy_broker_config) ;;
    inproc) args=(-p ozzy-broker --test ozzy_broker_config -E
        'test(inproc_memory_) or test(journals::serving::simulated::tests::)') ;;
    storage) args=(-p ozzy-journal-segment --features lz4 --lib) ;;
    io) args=(-p ozzy-io -p ozzy-io-pool -p ozzy-io-aio --features ozzy-io/simulation) ;;
    replication) args=(-p ozzy-runtime --features lz4-storage,simulation
        --test ozzy_replica_transport --test ozzy_replication) ;;
    simulation) args=(-p ozzy-sim -p ozzy-io --features ozzy-io/simulation) ;;
    loom)
        export RUSTFLAGS="${RUSTFLAGS:--C target-cpu=native} --cfg ozzy_loom"
        exec cargo test -p ozzy-runtime --test ozzy_loom -- "$@" ;;
    -h|--help)
        echo 'Usage: scripts/test.sh [unit|core|writer|local|inproc|storage|io|replication|simulation|loom] [test filters]'
        echo 'Default: unit. Example: writer.'
        exit 0 ;;
    *) echo "Unknown test scope: $scope" >&2; exit 2 ;;
esac

TIMEFORMAT="test scope=$scope elapsed=%3Rs user=%3Us system=%3Ss"
time cargo nextest run --test-threads 8 --status-level fail \
    --final-status-level fail "${args[@]}" -- "$@"

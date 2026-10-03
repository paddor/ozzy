#!/usr/bin/env bash
# Cargo's facade unit-test executable inherits the public crate name `ozzy`.
# Keep that API name while making its process identifiable alongside other tests.
set -euo pipefail
case "${1##*/}" in
    ozzy-[0-9a-f][0-9a-f]*)
        executable=$1
        shift
        alias_path="${executable%/*}/ozzy_unit-${executable##*/ozzy-}"
        ln -sfn "${executable##*/}" "$alias_path"
        exec "$alias_path" "$@"
        ;;
    *) exec "$@" ;;
esac

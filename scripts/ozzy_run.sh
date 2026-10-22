#!/usr/bin/env bash
# Cargo's facade unit-test executable inherits the public crate name `ozzy`.
# Keep that API name while making its process identifiable alongside other tests.
set -euo pipefail
case "${1##*/}" in
    ozzy-[0-9a-f][0-9a-f]*)
        executable=$1
        shift
        alias_path="${executable%/*}/ozzy_unit-${executable##*/ozzy-}"
        # Nextest lists each binary twice at once, and multicall coreutils
        # replace links non-atomically. The link always names the same file,
        # so one another call made first is correct.
        [[ -L $alias_path ]] || ln -s "${executable##*/}" "$alias_path" 2> /dev/null ||
            [[ -L $alias_path ]]
        exec "$alias_path" "$@"
        ;;
    *) exec "$@" ;;
esac

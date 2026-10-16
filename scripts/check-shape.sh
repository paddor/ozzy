#!/usr/bin/env bash
# Fails on Rust files over the line limits: 600 for product code, 1000 for
# tests and simulation support. Files in check-shape.allow are exempt until
# they fit; a listed file that fits fails too, so the list only shrinks.
set -euo pipefail

cd "$(dirname "$0")/.."
allow=scripts/check-shape.allow

files=()
while IFS= read -r -d '' path; do
    [[ -f "$path" ]] && files+=("$path")
done < <(git ls-files -z --cached --others --exclude-standard -- '*.rs')

awk -v allow="$allow" '
    function limit(path) {
        if (path ~ /^ozzy-sim\// || path ~ /\/tests\// ||
            path ~ /(\/|_)tests\.rs$/ || path ~ /\/test_[^\/]*\.rs$/ ||
            path ~ /\/simulation(\.rs|\/)/)
            return 1000
        return 600
    }
    FILENAME == allow {
        if ($0 !~ /^(#|[ \t]*$)/)
            allowed[$1] = 1
        next
    }
    { lines[FILENAME] = FNR }
    END {
        for (path in lines) {
            if (lines[path] > limit(path) && !(path in allowed)) {
                printf "%s: %d lines, limit %d\n", path, lines[path], limit(path)
                failed = 1
            }
        }
        for (path in allowed) {
            if (lines[path] <= limit(path)) {
                printf "%s: fits %d lines; remove it from %s\n", path,
                    limit(path), allow
                failed = 1
            }
        }
        exit failed
    }
' "$allow" "${files[@]}" | sort

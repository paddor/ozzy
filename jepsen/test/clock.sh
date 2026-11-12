#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
fixture=$(mktemp -d "${OZZY_ARTIFACT_ROOT:?}/clock-test.XXXXXXXX")
mkdir -p "$fixture/bin" "$fixture/run"
printf 'OZZY_NODE=n0\nOZZY_TEST_IF=traffic\n' > "$fixture/lab.env"
export OZZY_CLOCK_TEST_ROOT="$fixture" OZZY_CLOCK_TEST_STATE="$fixture/clock-ns"
cat > "$fixture/bin/date" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
ns=$(cat "$OZZY_CLOCK_TEST_STATE")
case "$1" in
    +%s%N) echo "$ns" ;;
    +%s) echo "$((ns/1000000000))" ;;
    --iso-8601=ns) echo "$ns" ;;
    -s)
        value=${2#@}
        seconds=${value%%.*}
        fraction=0
        if [[ $value == *.* ]]; then fraction=${value#*.}; fi
        fraction="${fraction}000000000"
        fraction=${fraction:0:9}
        printf '%s\n' "$((seconds*1000000000 + 10#$fraction))" > "$OZZY_CLOCK_TEST_STATE" ;;
    *) exit 2 ;;
esac
EOF
chmod +x "$fixture/bin/date"
for command in timedatectl dmsetup iptables tc; do
    ln -s /usr/bin/true "$fixture/bin/$command"
done
# Execute the actual node commands with a private working directory and clock.
cd() { builtin cd "$OZZY_CLOCK_TEST_ROOT"; }
export -f cd
export PATH="$fixture/bin:$PATH"
for fraction in 123456789 999999999; do
    for offset in 10 -10 3600 -3600; do
        initial=$((1794040000*1000000000 + fraction))
        printf '%s\n' "$initial" > "$OZZY_CLOCK_TEST_STATE"
        bash "$root/jepsen/node" clock "$offset"
        test "$(cat "$OZZY_CLOCK_TEST_STATE")" = "$((initial + offset*1000000000))"
        elapsed=2876543210
        printf '%s\n' "$((initial + offset*1000000000 + elapsed))" > "$OZZY_CLOCK_TEST_STATE"
        bash "$root/jepsen/node" heal
        test "$(cat "$OZZY_CLOCK_TEST_STATE")" = "$((initial + elapsed))"
        test ! -e "$fixture/run/clock-offset"
    done
done
baseline=1794050000987654321
bash "$root/jepsen/node" clock-set "$baseline"
test "$(cat "$OZZY_CLOCK_TEST_STATE")" = "$baseline"
echo 'Clock faults preserve fractional seconds and elapsed time through healing.'

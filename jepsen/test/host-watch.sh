#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
fixture=$(mktemp -d "${OZZY_ARTIFACT_ROOT:?}/host-watch-test.XXXXXXXX")
mkdir -p "$fixture/bin" "$fixture/lab"
touch "$fixture/lab/owned"
printf '%s\n' owned-controller > "$fixture/lab/controller.uuid"
printf '%s\n' old-n0 > "$fixture/lab/n0.uuid"
export OZZY_HOST_WATCH_TEST_LOG="$fixture/destroyed"
cat > "$fixture/bin/virsh" <<'EOF'
#!/usr/bin/env bash
set -eu
shift 2
case "$1" in
    list)
        if [[ ${OZZY_HOST_WATCH_TEST_UNRELATED:-0} == 1 ]]; then
            echo unrelated-vm
        else printf '%s\n' ozzy-jepsen-controller ozzy-jepsen-n0; fi ;;
    domuuid)
        if [[ $2 == ozzy-jepsen-controller ]]; then echo owned-controller; else echo new-n0; fi ;;
    destroy) printf '%s\n' "$2" >> "$OZZY_HOST_WATCH_TEST_LOG" ;;
    *) exit 2 ;;
esac
EOF
chmod +x "$fixture/bin/virsh"
if PATH="$fixture/bin:$PATH" bash "$root/jepsen/host-watch" "$fixture/lab" qemu:///system 999999999; then
    echo 'Low-memory guard unexpectedly succeeded' >&2
    exit 1
else test "$?" -eq 1; fi
test "$(cat "$fixture/destroyed")" = ozzy-jepsen-controller
cat > "$fixture/bin/cat" <<'EOF'
#!/usr/bin/env bash
if [[ $1 == /sys/fs/cgroup/machine.slice/memory.swap.current ]]; then
    echo 4096
else exec /usr/bin/cat "$@"; fi
EOF
chmod +x "$fixture/bin/cat"
if PATH="$fixture/bin:$PATH" bash "$root/jepsen/host-watch" "$fixture/lab" qemu:///system 0; then
    echo 'Swap guard unexpectedly succeeded' >&2
    exit 1
else test "$?" -eq 1; fi
test "$(wc -l < "$fixture/destroyed")" -eq 2
PATH="$fixture/bin:$PATH" OZZY_HOST_WATCH_TEST_UNRELATED=1 \
    bash "$root/jepsen/host-watch" "$fixture/lab" qemu:///system 999999999
test "$(wc -l < "$fixture/destroyed")" -eq 2
echo 'RAM and swap guards stopped only matching owned VMs; unrelated and replaced VMs were untouched.'

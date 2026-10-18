# Shared by the smoke scripts: builds s3dal, serves a temporary fs root (or the backend that
# S3DAL_BACKEND names, as under scripts/with-rustfs.sh), and records each step.
# Source it after setting SMOKE_NAME. There is deliberately no `set -e`: every step runs, its
# exit code is recorded, and `smoke_summary` reports all of them.

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT_DIR/target}"
RUN_DIR="$TARGET_DIR/smoke/$SMOKE_NAME-$(date +%Y%m%d-%H%M%S)"
LOG_DIR="$RUN_DIR/logs"
DATA_ROOT="$RUN_DIR/root" # the fs backend root
WORK="$RUN_DIR/work"      # client-side files, mounted into the client containers at /work
ACCESS_KEY="smoke-access-key"
SECRET_KEY="smoke-secret-key"
SERVER_PID=""
ENDPOINT=""
STEP_NAMES=()
STEP_CODES=()

mkdir -p "$LOG_DIR" "$DATA_ROOT" "$WORK"

# Containers must reach a server that listens on the host's loopback only, so the test keys
# are never exposed to the network.
# - macOS (Docker Desktop, OrbStack): containers run inside a VM, where 127.0.0.1 is the
#   container itself. host.docker.internal is forwarded to the host's loopback, so the server
#   can stay on 127.0.0.1.
# - Linux: --network host shares the host's loopback directly.
if [[ "$(uname -s)" == Darwin ]]; then
    DOCKER_NET=()
    CLIENT_HOST="host.docker.internal"
else
    DOCKER_NET=(--network host)
    CLIENT_HOST="127.0.0.1"
fi

smoke_cleanup() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -INT "$SERVER_PID" 2>/dev/null
        wait "$SERVER_PID" 2>/dev/null
    fi
    if [[ "${SMOKE_KEEP:-0}" != 1 ]]; then
        rm -rf "$DATA_ROOT" "$WORK"
    fi
}
trap smoke_cleanup EXIT
trap 'exit 130' INT TERM

# step NAME CMD...: runs CMD with its output in logs/NN-NAME.log and records the exit code.
step() {
    local name=$1
    shift
    local log
    log=$(printf '%s/%02d-%s.log' "$LOG_DIR" $((${#STEP_NAMES[@]} + 1)) "$name")
    "$@" >"$log" 2>&1
    local code=$?
    STEP_NAMES+=("$name")
    STEP_CODES+=("$code")
    if ((code == 0)); then
        printf 'PASS  %s\n' "$name"
    else
        printf 'FAIL  %s (exit %d, %s)\n' "$name" "$code" "$log"
        tail -n 3 "$log" | sed 's/^/        /'
    fi
    return "$code"
}

# Prints every recorded step and fails if any of them failed.
smoke_summary() {
    local i failed=0 status
    printf '\n== %s summary (logs: %s) ==\n' "$SMOKE_NAME" "$LOG_DIR"
    for i in "${!STEP_NAMES[@]}"; do
        if ((STEP_CODES[i] == 0)); then
            status="PASS"
        else
            status="FAIL (exit ${STEP_CODES[i]})"
            failed=$((failed + 1))
        fi
        printf '%-26s %s\n' "${STEP_NAMES[i]}" "$status"
    done
    printf '%d passed, %d failed\n' $((${#STEP_NAMES[@]} - failed)) "$failed"
    ((failed == 0))
}

# Ends the run when a setup step failed: client steps cannot mean anything without a server.
smoke_abort() {
    echo "setup failed; client steps were not run" >&2
    smoke_summary
    exit 1
}

build_server() {
    cargo build --manifest-path "$ROOT_DIR/Cargo.toml" --bin s3dal
}

start_server() {
    # The temporary fs root, unless S3DAL_BACKEND names another backend for s3dal to read
    # from the environment (scripts/with-rustfs.sh sets it).
    local backend=()
    if [[ -z "${S3DAL_BACKEND:-}" ]]; then
        backend=(--backend fs --backend-opt "root=$DATA_ROOT")
    fi
    # Port 0 lets the OS pick a free port; the server logs the address it bound.
    # s3s::ops at debug logs the operation each request resolved to, which shows what a
    # client actually called when a step fails.
    NO_COLOR=1 RUST_LOG="${RUST_LOG:-info,s3s::ops=debug}" "$TARGET_DIR/debug/s3dal" serve \
        "${backend[@]}" --address 127.0.0.1:0 \
        --access-key "$ACCESS_KEY" --secret-key "$SECRET_KEY" \
        >"$LOG_DIR/server.log" 2>&1 &
    SERVER_PID=$!
    local re='listening on http://127\.0\.0\.1:([0-9]+)' _
    for _ in $(seq 1 100); do
        if ! kill -0 "$SERVER_PID" 2>/dev/null; then
            echo "s3dal exited before listening"
            cat "$LOG_DIR/server.log"
            return 1
        fi
        if [[ "$(<"$LOG_DIR/server.log")" =~ $re ]]; then
            ENDPOINT="http://$CLIENT_HOST:${BASH_REMATCH[1]}"
            echo "s3dal is listening; clients use $ENDPOINT"
            return 0
        fi
        sleep 0.1
    done
    echo "timed out waiting for s3dal to listen"
    cat "$LOG_DIR/server.log"
    return 1
}

ensure_image() {
    docker image inspect "$1" >/dev/null 2>&1 || docker pull "$1"
}

# Succeeds if a line of $1 is $2 or ends with " $2"; listings print the name last.
has_entry() {
    local line
    while IFS= read -r line; do
        [[ "$line" == "$2" || "$line" == *" $2" ]] && return 0
    done <<<"$1"
    return 1
}

# listed NAME CMD...: runs CMD and requires NAME in its output.
listed() {
    local name=$1 out code
    shift
    out=$("$@")
    code=$?
    printf '%s\n' "$out"
    ((code == 0)) || return "$code"
    has_entry "$out" "$name" || {
        echo "!! expected '$name' in the output above"
        return 1
    }
}

# unlisted NAME CMD...: runs CMD and requires NAME to be absent from its output.
unlisted() {
    local name=$1 out code
    shift
    out=$("$@")
    code=$?
    printf '%s\n' "$out"
    ((code == 0)) || return "$code"
    if has_entry "$out" "$name"; then
        echo "!! '$name' is still listed"
        return 1
    fi
}

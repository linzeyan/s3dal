#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2023-2026 The s3s Authors
# SPDX-License-Identifier: Apache-2.0
#
# Adapted from s3s v0.17.0 scripts/e2e-s3tests.sh, with its s3tests.env and
# source-s3tests-ref.sh folded in: the pinned ref, the shallow fetch, the venv, an s3tests.conf
# that gives every user the same key pair, and one pytest run over the whole suite.

# Runs ceph/s3-tests against a freshly started s3dal and records every test's outcome in
# compat/s3-tests/<label>.json, the input of scripts/s3tests-report.py.
#
#   scripts/s3tests.sh [LABEL]
#       fs on a fresh temporary root; LABEL defaults to fs
#   S3DAL_BACKEND=s3 S3DAL_BACKEND_OPTS=bucket=b,endpoint=http://127.0.0.1:9000,... \
#       scripts/s3tests.sh s3-rustfs
#       any backend s3dal serves, configured through s3dal's own environment variables
#   scripts/s3tests.sh LABEL -- PYTEST_ARGS...
#       a debugging run of the tests PYTEST_ARGS select (`-k multipart`, a node id, ...). It
#       never writes compat/: a subset would silently replace the full record.
#
# Environment: S3TESTS_TIMEOUT (seconds per test, default 120), S3TESTS_CARGO_FLAGS (extra
# `cargo build` flags, such as "--features services-s3"), RUST_LOG (the server's log filter),
# S3TESTS_KEEP=1 (keep the fs root after the run).
#
# The s3-tests checkout and its venv are cached under target/s3tests/ and rebuilt only when the
# ref or the dependency snapshot changes, so only the first run needs network access. Each run
# replaces target/s3tests/runs/<label>/, which keeps the junit XML, the server log, the
# s3tests.conf and one log per step (logs/*-pytest.log is the pytest output). Failing tests do
# not fail the script, they are the result; it exits non-zero only if a step fails.

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT_DIR/target}"
CACHE_DIR="$TARGET_DIR/s3tests"

S3TESTS_REPO=https://github.com/ceph/s3-tests.git
# The ref s3s v0.17.0 pins (scripts/s3tests.env); results stay comparable with s3s's own runs.
S3TESTS_REF=fb8b73092bb1dd8db829f1205a9e52e73bf9a232
S3TESTS_DIR="$CACHE_DIR/s3-tests"
VENV="$CACHE_DIR/venv"
PY_VERSION=3.12
# requirements.txt is unpinned, and botocore's behavior (default checksums, retries) changes
# between releases. Resolving it as of the ref's commit date gives every machine, and every
# backend's run, the same client, so results differ only by backend.
PY_DEPS_DATE=2026-04-11T00:00:00Z
# Not in requirements.txt; s3s sets no per-test limit. Its default signal method fails only the
# running test, so the run goes on and a timed-out test counts as failed.
PY_EXTRA=(pytest-timeout)
# Twice botocore's 60 s read timeout: a hung request is cut during its first retry. The slowest
# tests, test_object_checksum_{sha256,crc64nvme}, take about 70 s on any server: they reuse a
# body that is already read, so botocore's retry sends more bytes than its Content-Length and
# then waits out its read timeout once. Everything else takes under 10 s on a debug build.
TIMEOUT="${S3TESTS_TIMEOUT:-120}"

LABEL="${1:-${S3DAL_BACKEND:-fs}}"
(($#)) && shift
PYTEST_ARGS=()
if [[ "${1:-}" == "--" ]]; then
    shift
    PYTEST_ARGS=("$@")
elif [[ $# -gt 0 ]]; then
    echo "usage: scripts/s3tests.sh [LABEL] [-- PYTEST_ARGS...]" >&2
    exit 2
fi
if [[ ! "$LABEL" =~ ^[A-Za-z0-9._-]+$ ]]; then
    echo "label must match [A-Za-z0-9._-]+: $LABEL" >&2
    exit 2
fi

RUN_DIR="$CACHE_DIR/runs/$LABEL"
LOG_DIR="$RUN_DIR/logs"
DATA_ROOT="$RUN_DIR/root" # the fs root when S3DAL_BACKEND is unset
RESULT="$ROOT_DIR/compat/s3-tests/$LABEL.json"
ACCESS_KEY="s3tests-access-key"
SECRET_KEY="s3tests-secret-key"
SERVER_PID=""
PORT=""
STEP_NAMES=()
STEP_CODES=()

rm -rf "$RUN_DIR"
mkdir -p "$LOG_DIR"

cleanup() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -INT "$SERVER_PID" 2>/dev/null
        wait "$SERVER_PID" 2>/dev/null
    fi
    if [[ "${S3TESTS_KEEP:-0}" != 1 ]]; then
        rm -rf "$DATA_ROOT"
    fi
}
trap cleanup EXIT
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
        tail -n 5 "$log" | sed 's/^/        /'
    fi
    return "$code"
}

summary() {
    local i failed=0 status
    printf '\n== s3-tests %s summary (logs: %s) ==\n' "$LABEL" "$RUN_DIR"
    for i in "${!STEP_NAMES[@]}"; do
        if ((STEP_CODES[i] == 0)); then
            status="PASS"
        else
            status="FAIL (exit ${STEP_CODES[i]})"
            failed=$((failed + 1))
        fi
        printf '%-14s %s\n' "${STEP_NAMES[i]}" "$status"
    done
    printf '%d steps passed, %d failed\n' $((${#STEP_NAMES[@]} - failed)) "$failed"
    ((failed == 0))
}

abort() {
    echo "a setup step failed; later steps were not run" >&2
    summary
    exit 1
}

# The source the server is built from, as the git tree id of the working tree without compat/.
# A commit id would name nothing once history is rewritten, and "-dirty" would not say which
# changes ran. compat/ is left out because this script writes it: a rerun before the previous
# result is committed is still a run of the same source. For a commit, the same id is
#   git ls-tree COMMIT | awk -F'\t' '$2 != "compat"' | git mktree
s3dal_source() {
    local dir tree
    dir=$(mktemp -d) || return 1
    # A scratch index, so the real one and what it has staged stay as they are.
    tree=$(export GIT_INDEX_FILE="$dir/index" &&
        git -C "$ROOT_DIR" read-tree HEAD &&
        git -C "$ROOT_DIR" add -A -- . ':!compat' &&
        git -C "$ROOT_DIR" rm -r -q --cached --ignore-unmatch -- compat &&
        git -C "$ROOT_DIR" write-tree)
    local code=$?
    rm -rf "$dir"
    ((code == 0)) || return 1
    git -C "$ROOT_DIR" rev-parse --short "$tree"
}

fetch_s3tests() {
    if [[ -d "$S3TESTS_DIR/.git" && "$(git -C "$S3TESTS_DIR" rev-parse HEAD)" == "$S3TESTS_REF" ]]; then
        echo "using $S3TESTS_DIR at $S3TESTS_REF"
        return 0
    fi
    rm -rf "$S3TESTS_DIR"
    git init -q "$S3TESTS_DIR" &&
        git -C "$S3TESTS_DIR" remote add origin "$S3TESTS_REPO" &&
        git -C "$S3TESTS_DIR" fetch -q --depth 1 origin "$S3TESTS_REF" &&
        git -C "$S3TESTS_DIR" checkout -q --detach FETCH_HEAD
}

setup_venv() {
    local stamp="$S3TESTS_REF $PY_VERSION $PY_DEPS_DATE ${PY_EXTRA[*]}"
    if [[ -x "$VENV/bin/python" && "$(cat "$VENV/.s3dal-stamp" 2>/dev/null)" == "$stamp" ]]; then
        echo "using $VENV"
        return 0
    fi
    rm -rf "$VENV"
    uv venv --python "$PY_VERSION" "$VENV" &&
        uv pip install --python "$VENV/bin/python" --exclude-newer "$PY_DEPS_DATE" \
            -r "$S3TESTS_DIR/requirements.txt" "${PY_EXTRA[@]}" &&
        printf '%s\n' "$stamp" >"$VENV/.s3dal-stamp"
}

build_server() {
    # shellcheck disable=SC2086 # S3TESTS_CARGO_FLAGS is a list of flags
    cargo build --manifest-path "$ROOT_DIR/Cargo.toml" --bin s3dal $S3TESTS_CARGO_FLAGS
}

start_server() {
    local backend=()
    # With S3DAL_BACKEND set, s3dal reads the backend and its options from the environment.
    if [[ -z "${S3DAL_BACKEND:-}" ]]; then
        mkdir -p "$DATA_ROOT"
        backend=(--backend fs --backend-opt "root=$DATA_ROOT")
    fi
    # Port 0 lets the OS pick a free port; the server logs the address it bound. s3s at debug
    # logs each request's operation and the error document it got, which is what explains a
    # failing test.
    NO_COLOR=1 RUST_LOG="${RUST_LOG:-info,s3s=debug}" "$TARGET_DIR/debug/s3dal" serve \
        "${backend[@]}" --address 127.0.0.1:0 \
        --access-key "$ACCESS_KEY" --secret-key "$SECRET_KEY" \
        >"$RUN_DIR/server.log" 2>&1 &
    SERVER_PID=$!
    local re='listening on http://127\.0\.0\.1:([0-9]+)' _
    # The startup probe writes to the backend, which takes a while on a remote one.
    for _ in $(seq 1 300); do
        if ! kill -0 "$SERVER_PID" 2>/dev/null; then
            echo "s3dal exited before listening"
            cat "$RUN_DIR/server.log"
            return 1
        fi
        if [[ "$(<"$RUN_DIR/server.log")" =~ $re ]]; then
            PORT=${BASH_REMATCH[1]}
            echo "s3dal is listening on 127.0.0.1:$PORT"
            return 0
        fi
        sleep 0.1
    done
    echo "timed out waiting for s3dal to listen"
    cat "$RUN_DIR/server.log"
    return 1
}

# s3dal has one tenant: every key pair reaches the same buckets as the same owner. So every
# user section carries the one pair, as s3s's conf does, and "alt", "tenant" and the IAM users
# are the main user under other names: a test where alt must be denied fails, and a test where
# alt succeeds after a grant proves nothing about the grant. ACL, policy, IAM and STS results
# do not measure isolation.
#
# 127.0.0.1 rather than s3s's localhost: botocore addresses an IP endpoint path-style, so no
# wildcard DNS (bucket.localhost) and no --domain are needed.
write_conf() {
    local section
    {
        printf '[DEFAULT]\nhost = 127.0.0.1\nport = %s\nis_secure = False\nssl_verify = False\n\n' "$PORT"
        printf '[fixtures]\nbucket prefix = s3dal-{random}-\n'
        for section in "s3 main" "s3 alt" "s3 tenant" "iam" "iam root" "iam alt root"; do
            printf '\n[%s]\n' "$section"
            printf 'display_name = s3dal %s\nuser_id = s3dal-%s\nemail = s3dal-%s@example.com\n' \
                "$section" "${section// /-}" "${section// /-}"
            printf 'access_key = %s\nsecret_key = %s\n' "$ACCESS_KEY" "$SECRET_KEY"
            if [[ "$section" == "s3 tenant" ]]; then
                printf 'tenant = s3dal\n'
            fi
        done
    } >"$RUN_DIR/s3tests.conf"
    cat "$RUN_DIR/s3tests.conf"
}

# Test selection mirrors s3s: the whole `s3tests` package, no marker filter. ceph's own runs
# deselect by marker (fails_on_rgw, ...), but here every backend gets the same denominator, and
# features s3dal refuses (versioning, ACLs, SSE, lifecycle, ...) are counted as failing in their
# category instead of disappearing from it.
run_pytest() {
    local args=("${PYTEST_ARGS[@]}")
    ((${#args[@]})) || args=(s3tests)
    (
        cd "$S3TESTS_DIR" &&
            S3TEST_CONF="$RUN_DIR/s3tests.conf" \
                "$VENV/bin/python" -m pytest -p no:cacheprovider \
                --timeout "$TIMEOUT" -v --tb=short --durations=20 \
                --junitxml "$RUN_DIR/junit.xml" "${args[@]}"
    )
    local code=$?
    # 1 means some tests failed: that is a result, not a broken run.
    ((code == 0 || code == 1))
}

# A server that died mid-run fails every later test for no reason of its own.
server_alive() {
    kill -0 "$SERVER_PID" 2>/dev/null || {
        echo "s3dal exited during the run; see $RUN_DIR/server.log"
        tail -n 20 "$RUN_DIR/server.log"
        return 1
    }
}

# Reduces the junit XML to what is worth diffing: one outcome per test id, sorted, with no
# times, hosts or tracebacks.
write_result() {
    mkdir -p "$(dirname -- "$RESULT")"
    python3 -I - "$RUN_DIR/junit.xml" "$RESULT" "$S3TESTS_REF" "$SOURCE" <<'EOF'
import json
import sys
from collections import Counter
from xml.etree import ElementTree

junit, out, ref, source = sys.argv[1:]
tests = {}
for case in ElementTree.parse(junit).getroot().iter("testcase"):
    # The classname is the module's dotted path: s3-tests defines no test classes.
    node = case.get("classname").replace(".", "/") + ".py::" + case.get("name")
    # A failure in the test body wins over an error in its teardown, as in s3s's report.
    if case.find("failure") is not None:
        outcome = "failed"
    elif case.find("error") is not None:
        outcome = "error"
    elif case.find("skipped") is not None:
        outcome = "skipped"
    else:
        outcome = "passed"
    if node in tests:
        sys.exit(f"duplicate test id in {junit}: {node}")
    tests[node] = outcome
if not tests:
    sys.exit(f"no test cases in {junit}")
with open(out, "w", encoding="utf-8") as f:
    json.dump({"s3dal_source": source, "s3tests_ref": ref, "tests": tests}, f, indent=1, sort_keys=True)
    f.write("\n")
counts = Counter(tests.values())
print(f"{out}: {len(tests)} tests, " + ", ".join(f"{counts[k]} {k}" for k in ("passed", "failed", "error", "skipped")))
EOF
}

SOURCE=$(s3dal_source) || {
    echo "cannot read the s3dal source tree" >&2
    exit 1
}
echo "s3dal source $SOURCE, s3-tests $S3TESTS_REF, label $LABEL"

step fetch-s3tests fetch_s3tests || abort
step venv setup_venv || abort
step build build_server || abort
step server start_server || abort
step conf write_conf || abort
step pytest run_pytest
step server-alive server_alive
if ((${#PYTEST_ARGS[@]})); then
    echo "debugging run: $RESULT not written"
elif summary >/dev/null; then
    step result write_result && tail -n 1 "$LOG_DIR"/*-result.log
else
    echo "a step failed: $RESULT not written"
fi

summary

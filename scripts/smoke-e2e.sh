#!/usr/bin/env bash
# Smoke test: s3s's own end-to-end suite (s3s-e2e), built from the s3s sources of the version
# s3dal depends on, against s3dal on a temporary fs root.
#
#   scripts/smoke-e2e.sh
#
# Runs the Basic suite, plus the Advanced fixtures for features s3dal implements: multipart,
# list pagination, presigned URLs. Advanced/Tagging and Advanced/STS are not run, because s3dal
# refuses object tags and has no STS endpoint.
#
# S3S_SRC names an s3s checkout at tag v0.17.0; without it, one is cloned (shallow) under
# target/. The e2e client runs on the host, so no Docker is needed. Exits non-zero if any step
# fails. Per-step logs, the JSON reports and the server log stay under target/smoke/; set
# SMOKE_KEEP=1 to keep the backend root as well.

SMOKE_NAME=e2e
. "$(dirname -- "${BASH_SOURCE[0]}")/smoke-lib.sh"

S3S_VERSION=0.17.0
S3S_SRC="${S3S_SRC:-$TARGET_DIR/s3s-src-v$S3S_VERSION}"
# The client runs on the host, where the server's loopback address is reachable directly.
CLIENT_HOST=127.0.0.1

fetch_s3s() {
    if [[ -d "$S3S_SRC/crates/s3s-e2e" ]]; then
        echo "using $S3S_SRC"
        return 0
    fi
    git clone --depth 1 --branch "v$S3S_VERSION" https://github.com/s3s-project/s3s "$S3S_SRC"
}

build_e2e() {
    # The s3s workspace builds into its own target directory; one shared with s3dal's would
    # mix two workspaces' artifacts.
    env -u CARGO_TARGET_DIR cargo build --manifest-path "$S3S_SRC/Cargo.toml" -p s3s-e2e
}

# e2e NAME FILTER: runs the cases whose suite/fixture/case path matches FILTER. The environment
# is cleared so the client sees only the test keys, never a profile from ~/.aws or a .env file.
e2e() {
    (
        cd "$WORK" &&
            env -i PATH="$PATH" HOME="$WORK" \
                AWS_ACCESS_KEY_ID="$ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
                AWS_REGION=us-east-1 AWS_ENDPOINT_URL="$ENDPOINT" \
                AWS_CONFIG_FILE=/dev/null AWS_SHARED_CREDENTIALS_FILE=/dev/null \
                RUST_LOG="${E2E_LOG:-s3s_test=info}" NO_COLOR=1 \
                "$S3S_SRC/target/debug/s3s-e2e" --filter "$2" --json "$LOG_DIR/$1.json"
    )
}

step fetch-s3s fetch_s3s || smoke_abort
step build build_server || smoke_abort
step build-e2e build_e2e || smoke_abort
step server start_server || smoke_abort

step basic e2e basic '^Basic/'
step advanced e2e advanced '^Advanced/(Multipart|ListPagination|PresignedUrl)/'

smoke_summary | tee "$RUN_DIR/summary.txt"
exit "${PIPESTATUS[0]}"

#!/usr/bin/env bash
# Smoke test: rclone, run from its Docker image, against s3dal on a temporary fs root.
#
#   scripts/smoke-rclone.sh
#
# Three flows, each valid on its own:
# - bucket lifecycle on an empty bucket: mkdir, lsd, rmdir, lsd.
# - a tree with nested directories and files of 0 B, 1 B and 1 MiB: copy, lsf -R, check
#   (sizes and hashes), delete, lsf -R, purge, lsd.
# - large files through multipart: sync, check, change the tree (one file rewritten, one
#   removed, one added), sync again, check, a server-side copy (multipart copy) downloaded and
#   compared, purge.
# Exits non-zero if any step fails. Per-step logs and the server log stay under
# target/smoke/; set SMOKE_KEEP=1 to keep the backend root and client files as well.
# RCLONE_IMAGE overrides the pinned client image.

SMOKE_NAME=rclone
. "$(dirname -- "${BASH_SOURCE[0]}")/smoke-lib.sh"

# The version s3s pins for its own rclone e2e run.
RCLONE_IMAGE="${RCLONE_IMAGE:-rclone/rclone:1.75.0}"
EMPTY_BUCKET="smoke-rclone-buckets"
TREE_BUCKET="smoke-rclone-objects"
LARGE_BUCKET="smoke-rclone-large"
MIB=1048576

rclone_() {
    # One attempt only: rclone retries 5xx answers, which would turn every
    # NotImplemented into minutes of retries and hide flaky behavior.
    # The cutoffs are far below rclone's defaults (200 MiB, 4.6 GiB), so files of tens of MiB
    # already upload, and copy server-side, in parts.
    docker run --rm "${DOCKER_NET[@]}" --user "$(id -u):$(id -g)" \
        -e RCLONE_CONFIG_S3DAL_TYPE=s3 -e RCLONE_CONFIG_S3DAL_PROVIDER=Other \
        -e RCLONE_CONFIG_S3DAL_ENDPOINT="$ENDPOINT" \
        -e RCLONE_CONFIG_S3DAL_ACCESS_KEY_ID="$ACCESS_KEY" \
        -e RCLONE_CONFIG_S3DAL_SECRET_ACCESS_KEY="$SECRET_KEY" \
        -e RCLONE_CONFIG_S3DAL_REGION=us-east-1 \
        -e RCLONE_CONFIG_S3DAL_FORCE_PATH_STYLE=true \
        -e RCLONE_CONFIG_S3DAL_UPLOAD_CUTOFF=16M -e RCLONE_CONFIG_S3DAL_CHUNK_SIZE=5M \
        -e RCLONE_CONFIG_S3DAL_COPY_CUTOFF=16M \
        -e RCLONE_RETRIES=1 -e RCLONE_LOW_LEVEL_RETRIES=1 -e HOME=/tmp \
        -v "$WORK:/work" \
        "$RCLONE_IMAGE" "$@"
}

prepare_tree() {
    local t="$WORK/tree" e="$WORK/expected-after-delete"
    mkdir -p "$t/nested/deeper" "$t/keep" &&
        : >"$t/zero.bin" &&
        printf 'x' >"$t/one.bin" &&
        head -c 1048576 /dev/urandom >"$t/nested/deeper/mib.bin" &&
        printf 'nested\n' >"$t/nested/note.txt" &&
        printf 'kept\n' >"$t/keep/kept.txt" &&
        # What the remote must look like after `delete` of tree/nested.
        cp -R "$t" "$e" &&
        rm -rf "$e/nested"
}

# 40 MiB is 8 parts of 5 MiB; 17 MiB + 1 leaves a 1-byte last part.
prepare_large() {
    local l="$WORK/large"
    mkdir -p "$l/sub" "$WORK/down" &&
        head -c $((40 * MIB)) /dev/urandom >"$l/a.bin" &&
        head -c $((17 * MIB + 1)) /dev/urandom >"$l/sub/b.bin" &&
        head -c $((20 * MIB)) /dev/urandom >"$l/c.bin"
}

# What sync must carry over: a rewritten file of the same size, a removed one, and a new one.
# The rewrite is a new file renamed over the old one. OrbStack serves the attributes of a file
# rewritten in place from a cache for a moment, so the next container saw the old mtime at the
# same size, and rclone skipped the file as unchanged; a new file has fresh attributes at once.
change_large() {
    local l="$WORK/large"
    head -c $((40 * MIB)) /dev/urandom >"$l/a.bin.new" && mv "$l/a.bin.new" "$l/a.bin" &&
        rm "$l/c.bin" &&
        head -c $((18 * MIB)) /dev/urandom >"$l/sub/d.bin"
}

# same_tree REMOTE LOCAL: `lsf -R` of both must match. rclone walks the remote one directory
# at a time, so this also checks the delimiter listing.
same_tree() {
    local want got
    # Captured before sorting, so a failing rclone is not masked by sort's exit code.
    want=$(rclone_ lsf -R "$2") || return 1
    got=$(rclone_ lsf -R "$1") || return 1
    want=$(LC_ALL=C sort <<<"$want")
    got=$(LC_ALL=C sort <<<"$got")
    printf 'want:\n%s\n\ngot:\n%s\n' "$want" "$got"
    [[ -n "$want" && "$want" == "$got" ]]
}

# rclone check falls back to comparing sizes when a hash is missing and still succeeds, so
# this also fails if any hash could not be compared.
check_hashes() {
    local out code
    out=$(rclone_ check "$@" 2>&1)
    code=$?
    printf '%s\n' "$out"
    ((code == 0)) || return "$code"
    if [[ "$out" == *"could not be checked"* ]]; then
        echo "!! some hashes were not compared"
        return 1
    fi
}

step build build_server || smoke_abort
step image ensure_image "$RCLONE_IMAGE" || smoke_abort
step files prepare_tree || smoke_abort
step large-files prepare_large || smoke_abort
step server start_server || smoke_abort

step mkdir rclone_ mkdir "s3dal:$EMPTY_BUCKET"
step lsd listed "$EMPTY_BUCKET" rclone_ lsd s3dal:
step rmdir rclone_ rmdir "s3dal:$EMPTY_BUCKET"
step lsd-after-rmdir unlisted "$EMPTY_BUCKET" rclone_ lsd s3dal:

step mkdir-tree-bucket rclone_ mkdir "s3dal:$TREE_BUCKET"
step copy rclone_ copy /work/tree "s3dal:$TREE_BUCKET/tree"
step lsf-R same_tree "s3dal:$TREE_BUCKET/tree" /work/tree
step check check_hashes /work/tree "s3dal:$TREE_BUCKET/tree"
step delete rclone_ delete "s3dal:$TREE_BUCKET/tree/nested"
step lsf-R-after-delete same_tree "s3dal:$TREE_BUCKET/tree" /work/expected-after-delete
step purge rclone_ purge "s3dal:$TREE_BUCKET"
step lsd-after-purge unlisted "$TREE_BUCKET" rclone_ lsd s3dal:

step mkdir-large-bucket rclone_ mkdir "s3dal:$LARGE_BUCKET"
step sync-large rclone_ sync /work/large "s3dal:$LARGE_BUCKET/large"
step check-large check_hashes /work/large "s3dal:$LARGE_BUCKET/large"
step change-large change_large
step resync-large rclone_ sync /work/large "s3dal:$LARGE_BUCKET/large"
step check-resync check_hashes /work/large "s3dal:$LARGE_BUCKET/large"
step lsf-R-resync same_tree "s3dal:$LARGE_BUCKET/large" /work/large
step server-side-copy rclone_ copyto "s3dal:$LARGE_BUCKET/large/a.bin" "s3dal:$LARGE_BUCKET/copy/a.bin"
step download-copy rclone_ copyto "s3dal:$LARGE_BUCKET/copy/a.bin" /work/down/a.bin
step cmp-copy cmp "$WORK/large/a.bin" "$WORK/down/a.bin"
step purge-large rclone_ purge "s3dal:$LARGE_BUCKET"

smoke_summary | tee "$RUN_DIR/summary.txt"
exit "${PIPESTATUS[0]}"

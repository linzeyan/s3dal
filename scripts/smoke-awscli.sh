#!/usr/bin/env bash
# Smoke test: the aws CLI, run from its Docker image, against s3dal on a temporary fs root.
#
#   scripts/smoke-awscli.sh
#
# Steps: mb, ls (buckets), cp up, cp down + cmp; a 64 MiB file through multipart: cp up, its
# ETag, cp down + cmp; an s3-to-s3 copy of it (multipart copy): its ETag, cp down + cmp;
# ls (bucket), ls (prefix), rm, rm --recursive, rb, ls (buckets).
# Exits non-zero if any step fails. Per-step logs and the server log stay under
# target/smoke/; set SMOKE_KEEP=1 to keep the backend root and client files as well.
# AWS_CLI_IMAGE overrides the pinned client image.

SMOKE_NAME=awscli
. "$(dirname -- "${BASH_SOURCE[0]}")/smoke-lib.sh"

AWS_CLI_IMAGE="${AWS_CLI_IMAGE:-amazon/aws-cli:2.38.1}"
BUCKET="smoke-awscli"
# Above 1 MiB to cross buffer boundaries, below the CLI's 8 MiB multipart threshold, so it is a
# single PUT.
PAYLOAD_SIZE=1048577
# Above the threshold: the CLI uploads, and copies, it in 8 parts of 8 MiB.
BIG_SIZE=$((64 * 1024 * 1024))
BIG_PARTS=8

aws_() {
    docker run --rm "${DOCKER_NET[@]}" --user "$(id -u):$(id -g)" \
        -e AWS_ACCESS_KEY_ID="$ACCESS_KEY" -e AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
        -e AWS_DEFAULT_REGION=us-east-1 -e AWS_CONFIG_FILE=/work/aws-config \
        -e AWS_MAX_ATTEMPTS=1 -e HOME=/tmp \
        -v "$WORK:/work" -w /work \
        "$AWS_CLI_IMAGE" --endpoint-url "$ENDPOINT" "$@"
}

# multipart_etag KEY: the object's ETag must be a multipart one with BIG_PARTS parts, which
# proves the CLI took the multipart path rather than a single request.
multipart_etag() {
    local etag
    etag=$(aws_ s3api head-object --bucket "$BUCKET" --key "$1" --query ETag --output text) ||
        return 1
    echo "$etag"
    [[ "$etag" == *"-$BIG_PARTS\"" ]]
}

prepare_files() {
    mkdir -p "$WORK/up" "$WORK/down" &&
        head -c "$PAYLOAD_SIZE" /dev/urandom >"$WORK/up/payload.bin" &&
        head -c "$BIG_SIZE" /dev/urandom >"$WORK/up/big.bin" &&
        # Virtual-hosted style would put the bucket into the hostname
        # (smoke-awscli.host.docker.internal), which does not resolve; s3dal answers path style
        # unless started with --domain.
        printf '[default]\ns3 =\n    addressing_style = path\n' >"$WORK/aws-config"
}

step build build_server || smoke_abort
step image ensure_image "$AWS_CLI_IMAGE" || smoke_abort
step files prepare_files || smoke_abort
step server start_server || smoke_abort

step mb aws_ s3 mb "s3://$BUCKET"
step ls-buckets listed "$BUCKET" aws_ s3 ls
step cp-up aws_ s3 cp --no-progress /work/up/payload.bin "s3://$BUCKET/dir/payload.bin"
step cp-down aws_ s3 cp --no-progress "s3://$BUCKET/dir/payload.bin" /work/down/payload.bin
step cmp cmp "$WORK/up/payload.bin" "$WORK/down/payload.bin"
step cp-up-multipart aws_ s3 cp --no-progress /work/up/big.bin "s3://$BUCKET/big.bin"
step etag-multipart multipart_etag big.bin
step cp-down-multipart aws_ s3 cp --no-progress "s3://$BUCKET/big.bin" /work/down/big.bin
step cmp-multipart cmp "$WORK/up/big.bin" "$WORK/down/big.bin"
step cp-s3-to-s3 aws_ s3 cp --no-progress "s3://$BUCKET/big.bin" "s3://$BUCKET/copy/big.bin"
step etag-copy multipart_etag copy/big.bin
step cp-down-copy aws_ s3 cp --no-progress "s3://$BUCKET/copy/big.bin" /work/down/copy.bin
step cmp-copy cmp "$WORK/up/big.bin" "$WORK/down/copy.bin"
step ls-bucket listed "PRE dir/" aws_ s3 ls "s3://$BUCKET/"
step ls-prefix listed "$PAYLOAD_SIZE payload.bin" aws_ s3 ls "s3://$BUCKET/dir/"
step rm aws_ s3 rm "s3://$BUCKET/dir/payload.bin"
step rm-recursive aws_ s3 rm --recursive "s3://$BUCKET/"
step rb aws_ s3 rb "s3://$BUCKET"
step ls-buckets-after-rb unlisted "$BUCKET" aws_ s3 ls

smoke_summary | tee "$RUN_DIR/summary.txt"
exit "${PIPESTATUS[0]}"

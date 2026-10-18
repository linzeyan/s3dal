#!/usr/bin/env bash
# Runs a command with an S3 upstream for s3dal: a throwaway RustFS container on the host's
# loopback, with one empty bucket.
#
#   scripts/with-rustfs.sh cargo test                   # the integration tests on s3
#   scripts/with-rustfs.sh scripts/s3tests.sh s3-rustfs # ceph s3-tests on s3
#   scripts/with-rustfs.sh scripts/smoke-rclone.sh      # a smoke test on s3
#
# The command sees the upstream twice, in the form each consumer reads:
# - S3DAL_TEST_BACKEND / S3DAL_TEST_BACKEND_OPTS for the integration test harness, which gives
#   every test server a root of its own inside the bucket;
# - S3DAL_BACKEND / S3DAL_BACKEND_OPTS for `s3dal serve` (the scripts that start a server).
# The container is removed on exit, whatever the command did, and the command's exit code is
# this script's. RUSTFS_IMAGE overrides the pinned image.

RUSTFS_IMAGE="${RUSTFS_IMAGE:-rustfs/rustfs:1.0.2}"
ACCESS_KEY="rustfs-test-access"
SECRET_KEY="rustfs-test-secret"
BUCKET="s3dal-upstream"
REGION="us-east-1"

if (($# == 0)); then
    echo "usage: $0 COMMAND [ARGS...]" >&2
    exit 2
fi

CONTAINER=""
cleanup() {
    if [[ -n "$CONTAINER" ]]; then
        docker rm -f "$CONTAINER" >/dev/null 2>&1
    fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

docker image inspect "$RUSTFS_IMAGE" >/dev/null 2>&1 || docker pull "$RUSTFS_IMAGE" || exit 1

# Published on the host's loopback only, at a port Docker picks, so parallel runs and other
# services on 9000 do not collide.
CONTAINER=$(docker run -d --rm -p 127.0.0.1::9000 \
    -e RUSTFS_ACCESS_KEY="$ACCESS_KEY" -e RUSTFS_SECRET_KEY="$SECRET_KEY" \
    -e RUSTFS_VOLUMES=/data -e RUSTFS_ADDRESS=:9000 \
    "$RUSTFS_IMAGE") || exit 1
PORT=$(docker port "$CONTAINER" 9000/tcp | head -n 1 | sed 's/.*://')
ENDPOINT="http://127.0.0.1:$PORT"

# curl signs with SigV4 itself, so creating the bucket needs no client image. Until the
# server listens, curl fails to connect; a refused request means it is up but not ready.
created=0
for _ in $(seq 1 100); do
    code=$(curl -s -o /dev/null -w '%{http_code}' -X PUT \
        --aws-sigv4 "aws:amz:$REGION:s3" --user "$ACCESS_KEY:$SECRET_KEY" \
        "$ENDPOINT/$BUCKET")
    if [[ "$code" == 200 ]]; then
        created=1
        break
    fi
    sleep 0.2
done
if ((created == 0)); then
    echo "RustFS at $ENDPOINT did not create bucket $BUCKET (last status $code)" >&2
    docker logs "$CONTAINER" 2>&1 | tail -n 20 >&2
    exit 1
fi
echo "RustFS $RUSTFS_IMAGE at $ENDPOINT, bucket $BUCKET" >&2

# disable_config_load: the upstream is exactly what these options say, never a profile or
# credentials file of whoever runs the script.
OPTS="endpoint=$ENDPOINT,bucket=$BUCKET,region=$REGION"
OPTS+=",access_key_id=$ACCESS_KEY,secret_access_key=$SECRET_KEY,disable_config_load=true"

S3DAL_TEST_BACKEND=s3 S3DAL_TEST_BACKEND_OPTS="$OPTS" \
    S3DAL_BACKEND=s3 S3DAL_BACKEND_OPTS="$OPTS" \
    "$@"

#!/usr/bin/env bash
# Runs a command with a Google Cloud Storage upstream for s3dal: a throwaway fake-gcs-server
# container on the host's loopback, with one empty bucket.
#
#   scripts/with-fake-gcs.sh cargo test                    # the integration tests on gcs
#   scripts/with-fake-gcs.sh scripts/s3tests.sh gcs-fake   # ceph s3-tests on gcs
#   scripts/with-fake-gcs.sh scripts/smoke-rclone.sh       # a smoke test on gcs
#
# The command sees the upstream twice, in the form each consumer reads:
# - S3DAL_TEST_BACKEND / S3DAL_TEST_BACKEND_OPTS for the integration test harness, which gives
#   every test server a root of its own inside the bucket;
# - S3DAL_BACKEND / S3DAL_BACKEND_OPTS for `s3dal serve` (the scripts that start a server).
# The container is removed on exit, whatever the command did, and the command's exit code is
# this script's. FAKE_GCS_IMAGE overrides the pinned image.
#
# s3dal reaches fake-gcs-server through scripts/fake-gcs-proxy.py, which fills in two things
# GCS does and fake-gcs-server does not: XML API multipart uploads, which OpenDAL uses for every
# write larger than 5 MiB, and the batch request form OpenDAL deletes with.

FAKE_GCS_IMAGE="${FAKE_GCS_IMAGE:-fsouza/fake-gcs-server:1.56.1}"
BUCKET="s3dal-upstream"

if (($# == 0)); then
    echo "usage: $0 COMMAND [ARGS...]" >&2
    exit 2
fi

CONTAINER=""
PROXY=""
PORT_FILE=$(mktemp) || exit 1
cleanup() {
    if [[ -n "$PROXY" ]]; then
        kill "$PROXY" 2>/dev/null
        wait "$PROXY" 2>/dev/null
    fi
    if [[ -n "$CONTAINER" ]]; then
        docker rm -f "$CONTAINER" >/dev/null 2>&1
    fi
    rm -f "$PORT_FILE"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

docker image inspect "$FAKE_GCS_IMAGE" >/dev/null 2>&1 || docker pull "$FAKE_GCS_IMAGE" || exit 1

# Published on the host's loopback only, at a port Docker picks, so parallel runs do not
# collide. Plain HTTP, in memory: nothing outlives the run.
CONTAINER=$(docker run -d --rm -p 127.0.0.1::4443 "$FAKE_GCS_IMAGE" \
    -scheme http -port 4443 -backend memory) || exit 1
PORT=$(docker port "$CONTAINER" 4443/tcp | head -n 1 | sed 's/.*://')
UPSTREAM="http://127.0.0.1:$PORT"

# fake-gcs-server takes unauthenticated JSON API requests, so curl creates the bucket. Until
# the server listens, curl fails to connect.
created=0
for _ in $(seq 1 100); do
    code=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
        -H 'Content-Type: application/json' -d "{\"name\":\"$BUCKET\"}" \
        "$UPSTREAM/storage/v1/b?project=s3dal")
    if [[ "$code" == 200 ]]; then
        created=1
        break
    fi
    sleep 0.2
done
if ((created == 0)); then
    echo "fake-gcs-server at $UPSTREAM did not create bucket $BUCKET (last status $code)" >&2
    docker logs "$CONTAINER" 2>&1 | tail -n 20 >&2
    exit 1
fi

python3 -I "$(dirname "$0")/fake-gcs-proxy.py" "$UPSTREAM" "$PORT_FILE" &
PROXY=$!
for _ in $(seq 1 100); do
    [[ -s "$PORT_FILE" ]] && break
    sleep 0.1
done
if [[ ! -s "$PORT_FILE" ]]; then
    echo "scripts/fake-gcs-proxy.py did not start" >&2
    exit 1
fi
ENDPOINT="http://127.0.0.1:$(head -n 1 "$PORT_FILE")"
echo "fake-gcs-server $FAKE_GCS_IMAGE at $UPSTREAM via $ENDPOINT, bucket $BUCKET" >&2

# skip_signature: fake-gcs-server checks no credentials. disable_config_load and
# disable_vm_metadata: the upstream is exactly what these options say, never credentials of
# whoever runs the script.
OPTS="endpoint=$ENDPOINT,bucket=$BUCKET"
OPTS+=",skip_signature=true,disable_config_load=true,disable_vm_metadata=true"

S3DAL_TEST_BACKEND=gcs S3DAL_TEST_BACKEND_OPTS="$OPTS" \
    S3DAL_BACKEND=gcs S3DAL_BACKEND_OPTS="$OPTS" \
    "$@"

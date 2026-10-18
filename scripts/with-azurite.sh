#!/usr/bin/env bash
# Runs a command with an Azure Blob Storage upstream for s3dal: a throwaway Azurite container on
# the host's loopback, with one empty blob container.
#
#   scripts/with-azurite.sh cargo test                         # the integration tests on azblob
#   scripts/with-azurite.sh scripts/s3tests.sh azblob-azurite  # ceph s3-tests on azblob
#   scripts/with-azurite.sh scripts/smoke-rclone.sh            # a smoke test on azblob
#
# The command sees the upstream twice, in the form each consumer reads:
# - S3DAL_TEST_BACKEND / S3DAL_TEST_BACKEND_OPTS for the integration test harness, which gives
#   every test server a root of its own inside the container;
# - S3DAL_BACKEND / S3DAL_BACKEND_OPTS for `s3dal serve` (the scripts that start a server).
# The container is removed on exit, whatever the command did, and the command's exit code is
# this script's. AZURITE_IMAGE overrides the pinned image.

AZURITE_IMAGE="${AZURITE_IMAGE:-mcr.microsoft.com/azure-storage/azurite:3.37.0}"
# Azurite's well-known development account, the only one it has unless told otherwise.
ACCOUNT="devstoreaccount1"
ACCOUNT_KEY="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
BLOB_CONTAINER="s3dal-upstream"

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

docker image inspect "$AZURITE_IMAGE" >/dev/null 2>&1 || docker pull "$AZURITE_IMAGE" || exit 1

# Published on the host's loopback only, at a port Docker picks, so parallel runs and other
# services on 10000 do not collide. Blob service only, in memory: nothing outlives the run.
CONTAINER=$(docker run -d --rm -p 127.0.0.1::10000 "$AZURITE_IMAGE" \
    azurite-blob --blobHost 0.0.0.0 --inMemoryPersistence --disableTelemetry) || exit 1
PORT=$(docker port "$CONTAINER" 10000/tcp | head -n 1 | sed 's/.*://')
ENDPOINT="http://127.0.0.1:$PORT/$ACCOUNT"

# Creating a container takes a Shared Key signature, which curl cannot make; Python's standard
# library can, so no client image is needed. Prints the HTTP status, 000 while Azurite is not
# listening yet.
create_container() {
    python3 -I - "$ENDPOINT" "$ACCOUNT" "$ACCOUNT_KEY" "$BLOB_CONTAINER" <<'PY'
import base64, email.utils, hashlib, hmac, sys, urllib.error, urllib.request

endpoint, account, key, container = sys.argv[1:]
date = email.utils.formatdate(usegmt=True)
version = "2021-08-06"
# Shared Key string to sign for a PUT without body or standard headers: the verb, eleven empty
# header lines, the x-ms-* headers, then the resource.
to_sign = (
    "PUT\n" + "\n" * 11
    + f"x-ms-date:{date}\nx-ms-version:{version}\n"
    + f"/{account}/{account}/{container}\nrestype:container"
)
signature = base64.b64encode(
    hmac.new(base64.b64decode(key), to_sign.encode(), hashlib.sha256).digest()
).decode()
request = urllib.request.Request(
    f"{endpoint}/{container}?restype=container",
    method="PUT",
    # No body argument: urllib would add a Content-Type that the signature does not cover.
    headers={
        "Content-Length": "0",
        "x-ms-date": date,
        "x-ms-version": version,
        "Authorization": f"SharedKey {account}:{signature}",
    },
)
try:
    with urllib.request.urlopen(request, timeout=5) as response:
        print(response.status)
except urllib.error.HTTPError as err:
    print(err.code)
except OSError:
    print("000")
PY
}

created=0
for _ in $(seq 1 100); do
    code=$(create_container)
    if [[ "$code" == 201 ]]; then
        created=1
        break
    fi
    sleep 0.2
done
if ((created == 0)); then
    echo "Azurite at $ENDPOINT did not create container $BLOB_CONTAINER (last status $code)" >&2
    docker logs "$CONTAINER" 2>&1 | tail -n 20 >&2
    exit 1
fi
echo "Azurite $AZURITE_IMAGE at $ENDPOINT, container $BLOB_CONTAINER" >&2

OPTS="endpoint=$ENDPOINT,container=$BLOB_CONTAINER"
OPTS+=",account_name=$ACCOUNT,account_key=$ACCOUNT_KEY"

S3DAL_TEST_BACKEND=azblob S3DAL_TEST_BACKEND_OPTS="$OPTS" \
    S3DAL_BACKEND=azblob S3DAL_BACKEND_OPTS="$OPTS" \
    "$@"

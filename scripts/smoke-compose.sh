#!/usr/bin/env bash
# Smoke test: docker-compose.yml as a user runs it, with the image built from this checkout.
#
#   scripts/smoke-compose.sh
#
# Steps: up --wait (the image's own health check must pass), the S3 port refuses an unsigned
# request, the console serves its page, the client subcommands in the container write and read
# an object with the key pair from the environment, the config file in its volume is s3dal's to
# write (the console saves to it), and the object outlives a restart.
# Runs as compose project s3dal-smoke on ports 19000 and 19001, and removes it and its volumes
# at the end; set SMOKE_KEEP=1 to leave it running. Per-step logs stay under
# target/smoke/compose/.

cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." || exit 1
OUT=target/smoke/compose
rm -rf "$OUT"
mkdir -p "$OUT"
export S3DAL_ACCESS_KEY=smoke-access S3DAL_SECRET_KEY=smoke-secret-key
export S3DAL_PORT=19000 S3DAL_CONSOLE_PORT=19001
compose() { docker compose -p s3dal-smoke "$@"; }

failed=0
step() {
    local name=$1
    shift
    if "$@" >"$OUT/$name.log" 2>&1; then
        echo "ok   $name"
    else
        echo "FAIL $name (see $OUT/$name.log)"
        failed=1
    fi
}

status_of() { # status_of URL EXPECTED
    local got
    got=$(curl -s -o /dev/null -w '%{http_code}' "$1")
    echo "$1: $got"
    test "$got" = "$2"
}

roundtrip() {
    compose exec -T s3dal s3dal mb s3://smoke &&
        printf 'hello from compose\n' | compose exec -T s3dal s3dal cp - s3://smoke/hello.txt &&
        read_back
}

read_back() {
    local got
    got=$(compose exec -T s3dal s3dal cat s3://smoke/hello.txt) || return 1
    echo "$got"
    test "$got" = "hello from compose"
}

# The image has no shell to ask; the archive `compose cp` streams carries the owner.
config_owned_by_s3dal() {
    compose cp s3dal:/etc/s3dal/s3dal.toml - | tar -tvf - | tee /dev/stderr | grep -q 65532
}

restart() { compose restart && compose up --wait --wait-timeout 60; }

step 01-up compose up --build --wait --wait-timeout 300
step 02-s3-unsigned status_of http://127.0.0.1:19000/ 403
step 03-console-page status_of http://127.0.0.1:19001/ 200
step 04-client-roundtrip roundtrip
step 05-config-writable config_owned_by_s3dal
step 06-restart restart
step 07-object-kept read_back

compose logs >"$OUT/server.log" 2>&1
if [ -z "$SMOKE_KEEP" ]; then
    compose down -v >"$OUT/down.log" 2>&1
fi
exit $failed

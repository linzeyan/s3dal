# Running s3dal

## Docker

The image runs as a non-root user (uid 65532) and serves the fs backend from the volume `/data`
on port 9000. It contains no credentials; pass them with `-e`. It is published for amd64 and
arm64 as `ghcr.io/linzeyan/s3dal:<version>`, `:<major>.<minor>` and `:latest`; `docker build
-t s3dal .` builds it from a checkout.

- `/data` is declared a volume, so objects never land in the container's own writable layer,
  which is removed with the container. Without `-v`, Docker creates an anonymous volume.
- The volume's filesystem must support user xattrs (see [Backends](backends.md)). A named
  volume lives on the Docker host's filesystem under `/var/lib/docker/volumes` (inside the
  Linux VM on Docker Desktop), usually ext4 or XFS, which support them. For a bind mount
  (`-v /srv/s3dal:/data`), the host directory's filesystem decides, and the directory must be
  writable by uid 65532 (`chown 65532:65532 /srv/s3dal`, or run with `--user` set to its
  owner). A tmpfs mount works from Linux 6.6. Network filesystems often lack user xattrs; NFS
  has them only from version 4.2 with Linux 5.9 on both ends.
- The image's defaults (fs on `/data`, address `0.0.0.0:9000`) are in the config file
  `/etc/s3dal/s3dal.toml`, so any `-e S3DAL_*` overrides them. To serve an S3 upstream
  instead: `-e S3DAL_BACKEND=s3 -e S3DAL_BACKEND_OPTS=bucket=...,endpoint=...`.
- To configure it with a file of your own, for several key pairs or HTTPS, mount it over the
  default, together with any certificate it names:
  `-v ./s3dal.toml:/etc/s3dal/s3dal.toml -v ./tls:/etc/s3dal/tls:ro`. The file replaces the
  defaults whole, so it sets `backend`, `address` and `backend-opt` itself. The files must be
  readable by uid 65532, and the config file writable by it if the console is to save
  settings to it. A file mounted on its own stays the file it was when the container started:
  an editor that saves by writing a new file and renaming it over the old one leaves the
  container reading the old one. To edit it on the host while s3dal runs, mount its directory
  instead (`-v ./config:/etc/s3dal`, with the file at `./config/s3dal.toml`).
- `-e S3DAL_CONSOLE_ADDRESS=0.0.0.0:9001 -p 9001:9001` turns on the web console. The image's
  config file is s3dal's own, so the console can save to it; mount a named volume on
  `/etc/s3dal` to keep what it saves when the container is replaced, as
  [docker-compose.yml](../docker-compose.yml) does.
- The image has no shell; `docker exec <container> s3dal ls s3://bucket` runs the client with
  the container's own key pair and address.
- On `docker stop`, s3dal finishes in-flight requests for up to 10 seconds before it exits.
  Docker's own stop timeout is also 10 seconds; give it a margin with `--stop-timeout 15`.

## Health checks

`GET /_s3dal/health` (or HEAD) on the S3 port answers `200` with an empty body, without a
signature, as long as the server runs. A signed request for that path is an ordinary S3 request, for the object `health`
in the bucket `_s3dal`, a name S3 does not allow.

`s3dal healthcheck` asks it and exits 0 or 1, for a container's health check where there is no
curl. Without `--url` it reads the same variables and config file as `serve` to find the
address, so in the image `["CMD", "/usr/local/bin/s3dal", "healthcheck"]` needs nothing else.
Over HTTPS it does not verify the certificate: it sends no secret, and only asks whether the
process answers.

## Logs

s3dal logs to stderr, one line per request when it ends, and its own warnings and errors:

```text
2026-11-03T12:05:53.465376Z  INFO request{id=5A0A3DC3AEDA4156}: s3dal::server: request method="PUT" path="/photos/hello%20%E4%B8%96%E7%95%8C.txt" status=200 code="-" key="alice" sent=0 ms=15 complete=true
```

- `id` is the request ID that the response carries as `x-amz-request-id`, and an error
  document as `<RequestId>`, so a client's error can be found in the log. AWS sends the same
  header; it reveals nothing but the ID.
- `path` leaves out the query string, which carries a presigned URL's signature.
- `code` is the S3 error code; `key` is the access key the request was signed with, even when
  the signature was wrong; `sent` is the bytes of the response body sent; `complete=false`
  means the client went away before the response ended.
- A 5xx other than 501 is logged at WARN, everything else at INFO.

`--log-format json` writes one JSON object per line, for log collectors. `RUST_LOG` sets the
levels (default `info,s3s=off`); `RUST_LOG=info,s3s=debug` adds s3s's own account of each
request's operation and error. The startup log shows the backend profile, which tells which
metadata the backend stores natively.

## HTTP/2

s3dal speaks HTTP/1.1 and HTTP/2 on the same port. Over HTTPS the client chooses with ALPN, as
browsers and SDKs do on their own. Without TLS, HTTP/2 needs a client that starts with it
("prior knowledge"), as `curl --http2-prior-knowledge` does; plain `curl` and most clients
speak HTTP/1.1 there, which is why `curl -I` shows `HTTP/1.1`.

## Connections and shutdown

A connection that sends nothing, or does not finish its TLS handshake, an HTTP/2 client's
connection preface, or a request's headers within 30 seconds is closed. So is a connection idle
for that long: a keep-alive HTTP/1.1 connection waiting for its next request, or an HTTP/2
connection with no request in flight, which gets a GOAWAY first; clients open a new one. A
request stays in flight, and its connection open, for as long as its upload or download takes,
but an upload whose body stops arriving for 30 seconds gets `400 RequestTimeout`, as on AWS.
A connection that carries nothing at all for 15 seconds is probed with TCP keepalives, so one
whose client vanished without closing it is dropped within a few minutes.
Closing a connection, s3dal reads and discards what the client still sends for up to 5 seconds,
so a client whose request is refused before its body is read gets the error rather than a reset
connection.

s3dal stops on SIGTERM or SIGINT (Ctrl-C): it stops listening at once, so new connections are
refused and clients can go elsewhere, and gives in-flight requests up to 10 seconds to finish.

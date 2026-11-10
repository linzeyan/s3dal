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

`--log-format json` writes one JSON object per line, for log collectors. The startup log shows
the backend profile, which tells which metadata the backend stores natively.

`--log-level` sets the levels: one level (`trace`, `debug`, `info`, `warn`, `error`, `off`),
then any `target=level`, comma-separated, as in `info,s3dal=debug`. The default is `info`.
s3s's own lines stay off unless a directive names `s3s`: `info,s3s=debug` adds its account of
each request's operation and error. A word that is not a level, alone or after `=`, is
refused: a typo such as `verbose` would otherwise turn the log off without a word. The level
changes without a restart, from the config file or the console.

The log goes to stderr, or with `--log-file` to a file, without colors. The file's missing
directories are created. Once a line would take
the file past `--log-max-size` (default `100MiB`), it is renamed to `<file>.1`, the older ones
move up to `<file>.<log-max-files>` (default 5), the oldest beyond that is removed, and a new
file is started; a restart appends to the file it finds. `--log-max-size 0` never rotates, for
logrotate's `copytruncate` or a platform that rotates the file itself. These three take a
restart. A file s3dal cannot open stops it at startup. In the Docker image, which runs as uid
65532, `/var/log/s3dal` is writable: `S3DAL_LOG_FILE=/var/log/s3dal/s3dal.log`, with a volume
on `/var/log/s3dal` to keep it, as [docker-compose.yml](../docker-compose.yml) shows. The
console's Server page shows where the log goes.

### Sending the log to a collector

`--log-forward` (`S3DAL_LOG_FORWARD`) sends every line to a collector as well, as JSON, while
stderr or the file keeps it too:

| URL | Sends to |
|---|---|
| `gelf+udp://graylog:12201` | A Graylog GELF UDP input; messages past one datagram go in GELF chunks |
| `gelf+tcp://graylog:12201` | A Graylog GELF TCP input, each message ended by a NUL byte |
| `gelf+http://graylog:12201/gelf`, `gelf+https://…` | A Graylog GELF HTTP input, one POST per message; the path defaults to `/gelf` |
| `elasticsearch+https://user:password@es:9200/index`, `elasticsearch+http://…` | The bulk API of Elasticsearch or OpenSearch, one request per batch. The index defaults to `logs-s3dal-default`, a data stream Elasticsearch's built-in `logs-*-*` template takes without setup. A query (`?pipeline=…`) is kept |
| `syslog+udp://rsyslog:514` | A syslog server, RFC 5424, one datagram per message |
| `syslog+tcp://rsyslog:514`, `syslog+tls://logs.example.com:6514` | A syslog server, RFC 5424, each message framed by its length (RFC 6587 octet counting); over TLS the server's certificate is checked as the system checks one |
| `otlp+http://otel-collector:4318`, `otlp+https://…` | An OpenTelemetry collector (the OpenTelemetry Collector, Grafana Alloy), or a backend that takes OTLP over HTTP itself, such as Loki 3 at `otlp+http://loki:3100/otlp/v1/logs`; OTLP/HTTP JSON, one request per batch. The path defaults to `/v1/logs` |
| `splunk+https://:TOKEN@splunk:8088`, `splunk+http://…` | Splunk's HTTP Event Collector, one request per batch; the token takes the password's place, with no user. The path defaults to `/services/collector/event` |
| `ndjson+https://vector:8080/s3dal`, `ndjson+http://…` | Any HTTP input that takes newline-delimited JSON, such as Vector's `http_server` source: the lines as they are, one request per batch |

- A GELF message carries the line's message as `short_message`, its level as a syslog level,
  the time it was logged, the container's hostname as `host`, and every other field with a
  leading `_`: `_status`, `_path`, `_span_id` (the request ID).
- A bulk document is the line's JSON with its time as `@timestamp`.
- A syslog message has the facility `daemon`, the line's level as its severity, the container's
  hostname, `s3dal` as the app name and the line's JSON as the message, for the server's JSON
  parser (rsyslog's `mmjsonparse` with `cookie=""`, syslog-ng's `json-parser`).
- An OTLP record has the line's level as its severity, its message as the body, and every
  other field as an attribute, the span's joined with a dot (`span.id`); the resource is
  `service.name=s3dal` with the hostname as `host.name`. Records the collector reports as
  rejected are not sent again.
- A Splunk event is the line's JSON with `sourcetype` `_json`, so Splunk extracts its fields.
- User and password go in the URL, percent-encoded, and are sent as HTTP Basic
  authentication; Splunk's token is sent as `Authorization: Splunk <token>`. A collector that
  wants another header (an API key) is reached through an OpenTelemetry collector or Vector in
  front of it. `check-config`, the log and the console never show the password or token; a URL
  saved from the console without one keeps the one it had.
- Sending never holds up a request: lines wait in a queue of 10,000, which a thread of their
  own empties. While the collector cannot be reached, s3dal retries with a growing pause, up to
  30 seconds; lines that find the queue full are dropped. Lines the collector refuses (a 4xx
  answer, or a document the index will not take) are not sent again. stderr, which is never
  forwarded, says when the collector stops answering and when it answers again, how many lines
  were dropped meanwhile, and each refusal that differs from the one before.
- On exit, s3dal logs that it stopped, and why if it was an error, then waits up to 3 seconds
  for the collector to take the lines still queued; stderr counts any it could not send in
  that time.
- A URL with a mistake stops s3dal at startup, as a bad `--log-level` does. A collector that
  is down does not: it is retried. The setting takes a restart.

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
The log says `stopping` when the signal comes and `stopped` at the end; with a log collector,
s3dal then gives it up to 3 more seconds to take the last lines.

A restart from the console stops the same way, the log saying `restarting` and the settings it
applies, then runs the same command line again in the same process (`exec`), with the same
environment. Between the two, while the new start probes the backend, connections are refused.

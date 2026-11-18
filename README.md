<p align="center"><img src="src/console/assets/icon.svg" alt="" width="96"></p>

<h1 align="center">s3dal</h1>

s3dal serves the Amazon S3 API on top of an [Apache OpenDAL](https://opendal.apache.org/)
backend: a local directory, an S3-compatible service, Azure Blob Storage, or Google Cloud
Storage. It is a single Rust binary, and a library: `s3dal::S3Dal` implements the `s3s::S3`
trait, so it can be embedded in another service. [s3s](https://github.com/s3s-project/s3s)
handles the protocol (routing, SigV4, XML); OpenDAL handles storage.

- **A gateway, not a storage system.** A bucket is a directory of the backend and an object a
  file under it, readable by other tools; metadata the backend cannot store natively goes into
  its user metadata (xattrs on fs). No sidecar files, no database. Durability is the backend's.
- **The S3 API that clients use**: buckets, objects, multipart uploads, listings with any
  delimiter, checksums, conditional requests, presigned URLs and POST forms, HTTPS with HTTP/2.
  What s3dal does not have, it refuses with `NotImplemented` rather than pretending.
- **A web console** to browse, upload and share objects, and manage key pairs and settings.
- **Image thumbnails** on the S3 port: `?x-s3dal-thumbnail=256` on a GET, presigned URLs
  included, made once and kept in the backend.
- **Settings that apply while it runs**: edit the config file, or save in the console, and key
  pairs and most settings change without a restart.
- **A client in the same binary**: `s3dal ls`, `cp`, `rm`, ... against any S3 endpoint.
- **Small to run**: one binary of about 25 MB; a Docker image for amd64 and arm64 of about
  22 MiB to download.

## Compared with MinIO

| | s3dal | MinIO |
|---|---|---|
| Stores objects on | A directory, or an S3, Azure Blob or GCS bucket you already have | Drives it manages itself |
| Durability | The backend's | Erasure coding across drives and nodes |
| Users, IAM policies | Key pairs limited to buckets or folders, read-only, write-only or read-write, with an optional expiry; no policies | Yes |
| Versioning | Yes, on every backend, in the console too | Yes |
| Lifecycle | Expiration, noncurrent version expiration and multipart abort rules by prefix and size, applied daily; transitions into cold tiers; no tag filters | Yes |
| Event notifications | To webhooks, signed with HMAC if asked, retried for a while, then dropped; no queue on disk | Webhooks, queues, databases, with a queue on disk |
| Object lock, encryption | No | Yes |
| Web console | Objects, key pairs, settings, a dashboard of the last hour | Yes |
| Metrics | Prometheus, at `/_s3dal/metrics` | Prometheus |
| License | Apache-2.0 | AGPL-3.0 |

s3dal fits when the storage is already there and what is missing is the S3 API in front of
it: a disk on one server, a bucket of another cloud, a dev or CI environment. It is no fit for
multi-tenant storage. [Differences from Amazon S3](guide/differences.md) lists what clients
may notice.

## Quick start

### Docker Compose

```sh
curl -LO https://raw.githubusercontent.com/linzeyan/s3dal/main/docker-compose.yml
printf 'S3DAL_ACCESS_KEY=admin\nS3DAL_SECRET_KEY=%s\n' "$(openssl rand -hex 20)" >.env
docker compose up -d
```

The S3 API is on `http://localhost:9000`; the console on `http://localhost:9001`, where the
key pair in `.env` signs in. Objects are kept in the volume `data`, the config file in the
volume `config`. `S3DAL_PORT` and `S3DAL_CONSOLE_PORT` in `.env` move the host ports. The log
goes to `docker compose logs`; to keep it in a file as well, or send it to Graylog or
Elasticsearch, see [Logs](guide/operations.md#logs).

### Docker

```sh
docker run -d --name s3dal -p 9000:9000 -v s3dal-data:/data \
    -e S3DAL_ACCESS_KEY=my-access-key -e S3DAL_SECRET_KEY=my-secret-key \
    ghcr.io/linzeyan/s3dal:latest
```

Add `-e S3DAL_CONSOLE_ADDRESS=0.0.0.0:9001 -p 9001:9001` for the console. Volumes, other
backends and your own config file: [Running s3dal](guide/operations.md#docker).

### Binary

```sh
cargo install --locked s3dal          # needs Rust 1.96 or later
export S3DAL_ACCESS_KEY=my-access-key S3DAL_SECRET_KEY=my-secret-key
s3dal serve --backend fs --backend-opt root=/srv/s3dal --console-address 127.0.0.1:9001
```

s3dal listens on `127.0.0.1:9000` (`--address 0.0.0.0:9000` to accept remote clients).
Buckets become directories under `/srv/s3dal`, which must be on a filesystem with user xattrs.
To serve an S3, Azure Blob or GCS bucket instead, see [Backends](guide/backends.md).

Then point any S3 client at it, path style, region `us-east-1`; [Clients](guide/clients.md)
has the aws CLI, rclone and SDK settings. Or use s3dal itself:

```sh
s3dal mb s3://photos
s3dal cp ./holiday.jpg s3://photos/2026/
s3dal ls -r s3://photos
```

## Configuration

`s3dal serve` takes its settings as flags, as environment variables, or from a config file. A
flag wins over its variable, and a variable over the file. Prefer the variables or the file
for secrets, which then stay out of shell history and process listings.

| Flag | Environment variable | Default | Meaning |
|---|---|---|---|
| `--config` | `S3DAL_CONFIG` | | TOML config file; see [Config file](#config-file) |
| `--backend` | `S3DAL_BACKEND` | (required) | OpenDAL service: `fs`, `s3`, `azblob` or `gcs` |
| `--backend-opt key=value` | `S3DAL_BACKEND_OPTS` | | A [backend option](guide/backends.md#backend-options); repeatable, or comma-separated, so a value cannot contain a comma (in the config file it can) |
| `--address` | `S3DAL_ADDRESS` | `127.0.0.1:9000` | Address and port of the S3 API |
| `--console-address` | `S3DAL_CONSOLE_ADDRESS` | | Address and port of the [web console](#web-console); without it there is none |
| `--access-key` | `S3DAL_ACCESS_KEY` | | Access key ID clients sign with; see [Key pairs](#key-pairs) |
| `--secret-key` | `S3DAL_SECRET_KEY` | | Its secret access key |
| `--region` | `S3DAL_REGION` | `us-east-1` | Region s3dal reports for every bucket, and that clients sign for |
| `--domain` | `S3DAL_DOMAIN` | | Base domain for virtual-hosted-style requests (`<bucket>.<domain>`); repeatable, or comma-separated |
| `--etag-buffer-limit` | `S3DAL_ETAG_BUFFER_LIMIT` | `8MiB` | Largest single PUT body held in memory to compute an MD5 ETag on backends without native ETags (see [ETags](guide/differences.md#etags)). Accepts `B`, `KiB`, `MiB`, `GiB` |
| `--stale-upload-age` | `S3DAL_STALE_UPLOAD_AGE` | `24h` | Multipart uploads neither completed nor aborted within this time are removed, as are, on fs, the temporary files of writes a crash or shutdown cut off; checked at startup and then hourly (more often for a shorter age); with several instances on one backend, one of them sweeps (see [Background jobs](guide/operations.md#background-jobs)). Accepts `s`, `m`, `h`, `d`; `0` keeps them forever. An upload a [lifecycle](#lifecycle-rules) abort rule covers goes by the rule instead |
| `--usage-scan-interval` | `S3DAL_USAGE_SCAN_INTERVAL` | `5m` | How often what each bucket holds is counted, for [quotas](#bucket-quotas) and the bucket metrics; with several instances on one backend, one of them counts. Accepts `s`, `m`, `h`, `d`; `0` stops counting |
| `--cache-size` | `S3DAL_CACHE_SIZE` | `0` | Memory for a read cache of objects up to 1 MiB on a remote backend (`s3`, `azblob`, `gcs`): a GET of an object cached since it last changed is answered without reading it from the backend; see [Read cache](guide/operations.md#read-cache). Accepts `B`, `KiB`, `MiB`, `GiB`; `0` has no cache. Refused on `fs` |
| `--tls-cert` | `S3DAL_TLS_CERT` | | PEM certificate chain; with `--tls-key`, s3dal serves HTTPS only, the console included |
| `--tls-key` | `S3DAL_TLS_KEY` | | PEM private key of the certificate |
| `--log-format` | `S3DAL_LOG_FORMAT` | `text` | `text`, or `json` for one object per line; see [Logs](guide/operations.md#logs) |
| `--log-level` | `S3DAL_LOG_LEVEL` | `info` | A level, then any `target=level`, comma-separated, as in `info,s3dal=debug` |
| `--log-file` | `S3DAL_LOG_FILE` | | File the log is written to, its missing directories created; without it, stderr |
| `--log-max-size` | `S3DAL_LOG_MAX_SIZE` | `100MiB` | Size past which the log file is renamed to `<file>.1` and a new one started; `0` never rotates |
| `--log-max-files` | `S3DAL_LOG_MAX_FILES` | `5` | Rotated log files kept |
| `--log-forward` | `S3DAL_LOG_FORWARD` | | Also send the log to Graylog (`gelf+udp://host:12201`, `gelf+tcp://…`, `gelf+https://…`), Elasticsearch/OpenSearch (`elasticsearch+https://user:password@host:9200/index`), syslog (`syslog+udp://host:514`, `syslog+tcp://…`, `syslog+tls://…`), OpenTelemetry (`otlp+https://host:4318`), Splunk HEC (`splunk+https://:TOKEN@host:8088`) or any HTTP input (`ndjson+https://host/path`); see [Logs](guide/operations.md#sending-the-log-to-a-collector) |
| `--metrics` | `S3DAL_METRICS` | `token` | Who may read the Prometheus metrics at `/_s3dal/metrics`: `token`, a scraper sending `Authorization: Bearer` with `--metrics-token` (no token set, no one); `public`, anyone; `off`. See [Metrics](guide/operations.md#metrics) |
| `--metrics-token` | `S3DAL_METRICS_TOKEN` | | The bearer token `--metrics token` asks for; at least 16 printable characters |

`s3dal check-config` takes the same flags, variables and file as `serve`, and prints the
settings `serve` would run with, secrets redacted, or what is wrong with them.

### Config file

The keys of the file are the long flag names. `backend-opt` is a table, and key pairs are
`[[credentials]]` tables:

```toml
backend = "s3"
address = "0.0.0.0:9000"
console-address = "0.0.0.0:9001"
domain = ["s3.example.com"]
tls-cert = "/etc/s3dal/tls/cert.pem"
tls-key = "/etc/s3dal/tls/key.pem"

[backend-opt]
bucket = "upstream-bucket"
endpoint = "https://s3.example.net"

[[credentials]]
access-key = "app"
secret-key = "..."

[[credentials]]
access-key = "backup"
secret-key = "..."
allow = ["photos", "logs/app/"]
access = "read-only"

[[notify]]
id = "thumbnails"
url = "https://hooks.example.com/s3"
hmac-secret = "..."
```

`domain` or `backend-opt` given by flag or variable replaces the file's whole rather than
adding to it. An unknown key is an error, so a misspelling is caught at startup. Relative
paths are relative to the working directory.

### Key pairs

There is no anonymous mode: s3dal does not start without a key pair, and every request must be
signed (SigV4, in headers, as a presigned URL, or as a POST policy). The one exception is a
download from a [public bucket](#public-buckets).

A pair reaches every bucket unless its `[[credentials]]` table limits it:

- `allow` lists the buckets, or folders as `bucket/folder/`, it reaches. A listing must stay
  inside one, so a pair allowed `photos/2024/` cannot list `photos/` and see the other years.
- `access = "read-only"` lets it read and list, not write or delete; the default is
  `"read-write"`.
- `access = "write-only"` makes a drop box: it may PUT objects, upload with a form
  (PostObject), and create, upload parts of, list the parts of, complete and abort multipart
  uploads. Everything else is refused: GET and HEAD, every listing (ListBuckets and HeadBucket
  included), deletes of any kind, and copies, since a copy reads its source. Clients that look
  at an object after uploading it fail with such a pair; see [clients](guide/clients.md).
- A pair with any of these limits cannot create or delete buckets, nor use the console's
  server and settings pages: those reach past any one bucket. ListBuckets names only the
  buckets it reaches. A batch delete or a copy that names one key outside its folders is
  refused whole, and a copy's source must be one the pair may read.
- `expires = "2026-12-31T00:00:00Z"` (RFC 3339, any offset) ends a pair at that instant: from
  then on every request signed with it, presigned URLs included, is answered as an unknown key
  (`InvalidAccessKeyId`), without a reload. A pair already expired at startup or reload is
  logged as a warning, not an error, so the file still loads.

The pair given by `--access-key` and `--secret-key` (or their variables) comes first, has no
limits, and the file's `[[credentials]]` are added to it. That pair cannot be removed or
changed from the console, so the operator always keeps a way in. `s3dal keygen` prints a new
random pair in the file's form.

### Public buckets

A public bucket serves its objects to anyone, without a signature: `GET` and `HEAD` of an
object, so `http://host:9000/site/logo.png` works in a browser or an `<img>` tag. Nothing else
is open: listing the bucket, uploading, deleting, and an object's ACL and tags still need a key
pair, and so does every other bucket. The console's Buckets page switches a bucket between
public and private, and its "Copy link" offers a link that does not expire for an object in a
public bucket. From a shell, `s3dal public s3://site on` (or `off`) does the same through the
console's address:

```sh
s3dal public s3://site on --endpoint http://127.0.0.1:9001
```

Both need a key pair without limits. The setting belongs to the bucket: it is kept in the
backend, in `.s3dal/buckets/site.json`, so every s3dal instance sharing the backend serves it,
within 5 seconds of the change, and it goes with the bucket, so a bucket deleted and created
again under the same name starts private.

### Bucket quotas

A bucket may have a quota: a largest size in bytes, and optionally a most number of objects,
every version of every object counted (a delete marker or folder marker counts as nothing).
A write that would take the bucket past either is refused with `403 QuotaExceeded`, before its
body is read: PUT and form uploads by their `Content-Length` (one without it is refused with
`MissingContentLength`), a copy by its source's size, CompleteMultipartUpload by the assembled
object's. Reaching the limit exactly is allowed. In an unversioned bucket an overwrite counts
only what it adds over the object it replaces, so a full bucket still takes an overwrite of the
same size or smaller. A multipart upload cannot start in a bucket already past its quota; its
parts are not counted until it completes. The console's bucket page sets the quota and shows
what the bucket holds against it; from a shell, through the console's address:

```sh
s3dal quota s3://data --max-bytes 500GiB --max-objects 1000000 --endpoint http://127.0.0.1:9001
s3dal quota s3://data --endpoint http://127.0.0.1:9001          # data: 1048576 bytes of 500GiB, 3 of 1000000 objects
s3dal quota s3://data --clear --endpoint http://127.0.0.1:9001
```

Setting one needs a key pair without limits. The quota is kept with the bucket, as its public
setting is. What the bucket holds is the last count of it, made every `usage-scan-interval`
(5 minutes) by one instance of those sharing the backend, plus the writes this instance made
since; the bucket is counted at once when it gets a quota. So one instance never lets its own
writes pass the quota, but writes through another instance count from the next count: with
several instances, a bucket can go past its quota by what the others write until the next
count reaches this one: up to the interval plus about 25 seconds. Two writes racing on one
instance can also both pass. See
[Bucket usage](guide/operations.md#bucket-usage).

### Lifecycle rules

A bucket's lifecycle configuration (PutBucketLifecycleConfiguration, or the console's bucket
page) can expire objects after a number of days or from a date, remove noncurrent versions
some days after they were replaced (keeping the newest few if asked), remove delete markers
left with nothing under them, and abort multipart uploads left incomplete, each rule limited
to a prefix and object sizes. In a versioned bucket an expired object gets a delete marker, as
on AWS. One instance of those sharing the backend applies the rules once a day per bucket, and
within the hour after they change; an expired object goes on the day its rule names (the first
midnight UTC after it is that many days old). With [cold tiers](guide/backends.md#cold-tiers),
rules can also move objects and noncurrent versions into a tier's storage class, at the same
times; without them, transitions are refused with `NotImplemented`. Tag filters are refused with
`NotImplemented`. A bucket without an abort rule still loses incomplete uploads after
`--stale-upload-age`. See [Differences](guide/differences.md#other-differences).

### Event notifications

s3dal can tell a webhook when objects are written or removed. The operator names each webhook
in a `[[notify]]` table of the config file (`id`, `url`, and optionally `hmac-secret`,
`auth-token`, `queue-size`, `retry-for`); a bucket's notification configuration
(PutBucketNotificationConfiguration, or the console's bucket page) then sends it the events a
rule matches, by the target's ARN, `arn:s3dal:sqs:<region>:<id>:webhook`:

```sh
aws s3api put-bucket-notification-configuration --bucket photos --endpoint-url http://127.0.0.1:9000 \
  --notification-configuration '{"QueueConfigurations": [{"Id": "jpegs",
    "QueueArn": "arn:s3dal:sqs:us-east-1:thumbnails:webhook", "Events": ["s3:ObjectCreated:*"],
    "Filter": {"Key": {"FilterRules": [{"Name": "suffix", "Value": ".jpg"}]}}}]}'
```

The events are AWS's: `s3:ObjectCreated:Put`, `Post`, `Copy` and `CompleteMultipartUpload`,
`s3:ObjectRemoved:Delete` and `DeleteMarkerCreated`, `s3:LifecycleExpiration:Delete` and
`DeleteMarkerCreated`, and the `:*` of each kind, and `s3:LifecycleTransition` with
[cold tiers](guide/backends.md#cold-tiers); a rule may limit them to keys with a prefix
and a suffix. Each event is POSTed as AWS's event message, with `X-S3dal-Signature` when the
target has an `hmac-secret`. Delivery never holds up a request: each target has a queue in
memory, retried for `retry-for` (an hour) while the webhook fails, and an event that finds the
queue full or runs out of time is dropped and counted. Events are not kept on disk, so a
restart loses those still queued, and changes made to the backend by other programs send none.
Setting a configuration needs a key pair without limits. See
[Event notifications](guide/operations.md#event-notifications) for the message, the signature
and the metrics, and [Differences](guide/differences.md#other-differences).

### Thumbnails

A GET of an image with `x-s3dal-thumbnail=128`, `256`, `512` or `1024` in its query answers
with the image scaled down to fit that square, its proportions kept and never scaled up: a
JPEG, or a PNG when the image has transparent pixels. JPEG, PNG, GIF (its first frame) and WebP
are read; an EXIF rotation is applied.

```sh
curl --aws-sigv4 aws:amz:us-east-1:s3 --user "$S3DAL_ACCESS_KEY:$S3DAL_SECRET_KEY" \
  -o holiday-256.jpg 'http://127.0.0.1:9000/photos/2026/holiday.jpg?x-s3dal-thumbnail=256'
```

It is GetObject with one more parameter, so it takes what a download takes: a signature from a
key pair that may read the object (a write-only pair gets none), or none in a
[public bucket](#public-buckets); in a presigned URL the size is signed with the rest, so the
link cannot be changed into another size or into the original. `versionId` names a version, as
on a GET. A thumbnail is made on the first request and kept in the backend under
`.s3dal/thumbs/`, named by the object's ETag, so every instance sharing the backend serves the
one made and an overwritten object is never shown as it was. A background job removes the
thumbnails of objects deleted or overwritten since, daily. The console's object browser has a
grid view of them, and an image's details show its thumbnail. Objects over 64 MiB or 50
megapixels, other formats, and images that do not decode are refused with an error (see
[Thumbnails](guide/operations.md#thumbnails)). The Cargo feature `thumbnail`, on by default,
builds it in; without it the parameter is refused with `NotImplemented`.

### Changes while it runs

s3dal watches the config file, and the certificate and key it names, and applies a saved
change within about two seconds; `kill -HUP` applies it at once. Requests in flight are not
cut.

- Applied at once: key pairs, `region`, `domain`, `etag-buffer-limit`, `stale-upload-age`,
  `usage-scan-interval`, `log-level`, `metrics`, `metrics-token`, `[[notify]]` targets, and a
  renewed certificate and key.
- `backend`, `backend-opt`, `address`, `console-address`, `cache-size`, `log-format`,
  `log-file`, `log-max-size`, `log-max-files`, `log-forward`, `[[tier]]` tables (see
  [Cold tiers](guide/backends.md#cold-tiers)), and turning TLS on or off take a restart.
  Until then the log, and the console, say a restart is pending; the console can restart the
  server, as below.
- A file with a mistake changes nothing: the log says what is wrong, and s3dal keeps running
  with the settings it had. `s3dal check-config` shows the same error before you save.

## Web console

`--console-address` (`S3DAL_CONSOLE_ADDRESS`) turns on a web console on a port of its own:

- buckets and objects: browse, filter, upload files and folders by drag and drop (large ones
  in parallel parts, each with a SHA-512 checksum the server verifies and keeps), list or a
  grid of [thumbnails](#thumbnails), preview images (by their thumbnail) and text, download,
  delete, copy a link that expires. An upload asks before it
  replaces an object of the same name: replace, skip, or keep both under a new name (turned
  off in Settings, per browser); make a bucket public or private; set a bucket's
  [quota](#bucket-quotas) and see what it holds against it; set the bucket's
  [event notifications](#event-notifications), and see each webhook's last delivery; with
  [cold tiers](guide/backends.md#cold-tiers), see each object's storage class, upload to a
  tier, change an object's class, and set lifecycle rules that move objects;
- key pairs: add, remove, and set their limits and expiry; the list says when each expires;
- settings: change those that apply at once, and save the rest, the backend included, for
  the next restart; a new backend is probed first, so the restart does not find it broken.
  A setting with a fixed set of values (backend, log level, log format, log collector) is a
  menu;
- restart: "Save and restart", or "Restart now" while saved settings wait for one. s3dal
  first checks that it would start again with them (the config file reads, the certificate
  loads, the log file opens, a new address is free, the backend and every tier answer) and
  refuses with the reason if not. Then it stops as on SIGTERM and runs its own command line
  again in the same process, so its PID stays and a container goes on running; the page waits
  for it and reloads. Unix only;
- server: version, uptime, backend and what it stores natively, when it was probed, where the
  log goes and whether the collector takes it, who may read the metrics, and the cold tiers
  with what the last `tier-sweep` counted in each;
- dashboard: the last hour at 10-second resolution, kept in the server's memory: requests per
  second by kind of operation, error rate, p50 and p99 latency, throughput and memory, each
  chart with a table view; and each bucket's usage, with a bar against its quota, its cold
  part shaded apart.

You sign in with any key pair, and see what it may do: a pair with limits sees only its
buckets and folders, and no write controls if it is read-only. The console signs each request
in the browser with it, as an
S3 client does; the secret is kept in the tab's session storage and is sent nowhere. The
console's page and its settings API answer only on the console port, and settings are saved
to the config file, comments and all. Without a config file, or with one s3dal cannot write,
the settings are shown read-only. A setting given by flag or variable is shown locked: the
file could not override it.

Only signatures, each bound to its request, cross the network, never the secret; but over plain
HTTP the objects and settings do, so put the console behind TLS (`--tls-cert`) or on a private
network.

## Command-line client

The `s3dal` binary is also a client for any S3 endpoint:

| Command | Does |
|---|---|
| `s3dal ls [s3://bucket[/prefix]] [-r]` | List buckets, or objects and prefixes |
| `s3dal mb s3://bucket` / `s3dal rb s3://bucket [--force]` | Create, or remove (with its objects) a bucket |
| `s3dal cp SRC DST [-r]` | Copy between local paths and `s3://`, either way or s3 to s3; `-` is stdin or stdout. Large files go up in parallel parts |
| `s3dal mv SRC DST [-r]` | Copy, then delete the source |
| `s3dal rm s3://bucket/key [-r]` | Delete an object, or everything under a prefix |
| `s3dal cat s3://bucket/key` | Write an object to stdout |
| `s3dal stat s3://bucket/key` | Size, ETag, content type, modification time and user metadata |
| `s3dal presign s3://bucket/key [--expires 1h] [--put]` | A URL to download, or upload, without credentials |
| `s3dal public s3://bucket [on\|off]` | Show or switch whether a bucket is [public](#public-buckets); s3dal only, with the console's address as `--endpoint` |
| `s3dal quota s3://bucket [--max-bytes 10GiB [--max-objects N] \| --clear]` | Show, set or remove a bucket's [quota](#bucket-quotas); s3dal only, as `public` |

The connection comes from `--endpoint` (default `http://127.0.0.1:9000`), `--access-key`,
`--secret-key` and `--region`, or the server's own variables `S3DAL_ENDPOINT`,
`S3DAL_ACCESS_KEY`, `S3DAL_SECRET_KEY` and `S3DAL_REGION`. So inside the container,
`docker exec s3dal s3dal ls` needs no arguments. `--insecure` accepts a self-signed
certificate.

## More

- [Backends](guide/backends.md): fs, S3, Azure Blob and GCS, and their options.
- [Clients](guide/clients.md): aws CLI, rclone and SDK settings.
- [Running s3dal](guide/operations.md): Docker, health checks, metrics, logs, HTTP/2, timeouts
  and shutdown.
- [Differences from Amazon S3](guide/differences.md).
- [S3 API coverage and compatibility](guide/compatibility.md): the operations, the tests, and
  the ceph s3-tests results.
- [CHANGELOG](CHANGELOG.md), and [CONTRIBUTING](CONTRIBUTING.md) to build and test s3dal.

## License

Apache License 2.0; see [LICENSE](LICENSE). s3dal includes code adapted from s3s, listed in
[NOTICE](NOTICE). The Docker image carries the license texts of every crate compiled into the
binary in `/usr/share/doc/s3dal/THIRD-PARTY-LICENSES`.

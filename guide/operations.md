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

## Metrics

`GET /_s3dal/metrics` answers in the Prometheus text format, on the S3 port and on the
console's. Who may read it is the `metrics` setting's to say, which a reload or the console's
Settings page changes without a restart:

- `token`, the default: a scraper sends `Authorization: Bearer <metrics-token>`. A missing or
  wrong token is answered `401`. With no `metrics-token` set, every scrape is answered `403`
  rather than opened to anyone; set one, or choose `public`.
- `public`: anyone who reaches the port, for a scraper on a private network.
- `off`: answered `404`.

```yaml
# prometheus.yml
scrape_configs:
  - job_name: s3dal
    authorization:
      credentials_file: /etc/prometheus/s3dal-token
    static_configs:
      - targets: ["s3dal:9000"]
    metrics_path: /_s3dal/metrics
```

A request signed for S3 with that path is an ordinary S3 request, as for the health check.
Scrapes, health checks and the console's pages are not counted as requests; the console's
settings and dashboard calls are, as `ConsoleAdmin`.

| Series | Type | Labels | Meaning |
|---|---|---|---|
| `s3dal_requests_total` | counter | `operation`, `status_class` | S3 requests answered. `operation` is the S3 operation (`GetObject`, `PutObject`, ...), `ConsoleAdmin` for the console's settings API, `Other` for an operation s3dal does not implement (answered 501), `Unknown` for a request refused before its operation was known (a bad signature, a malformed request). `status_class` is `1xx` to `5xx` |
| `s3dal_request_duration_seconds` | histogram | `operation` | From a request's headers to the end of its response body, so a download counts its whole transfer. Buckets from 0.5 ms to 60 s |
| `s3dal_received_bytes_total` | counter | | Request body bytes read |
| `s3dal_sent_bytes_total` | counter | | Response body bytes sent, counted as they go, so a long download shows while it runs |
| `s3dal_requests_in_flight` | gauge | | Requests whose response body has not ended |
| `s3dal_connections_open` | gauge | `protocol` | Client connections open, `http1` or `http2`, counted from their first request |
| `s3dal_connections_total` | counter | `protocol` | Client connections that sent a request |
| `s3dal_backend_operation_duration_seconds` | histogram | `operation` | Backend operations, by OpenDAL's name for them (`read`, `write`, `stat`, `list`, `delete`, `copy`, `create_dir`, ...); a read or write lasts until it is closed |
| `s3dal_backend_errors_total` | counter | `operation`, `kind` | Backend operations that failed, by OpenDAL's error kind (`NotFound`, `PermissionDenied`, `RateLimited`, `ConditionNotMatch`, ...). A `stat` that finds nothing is a `NotFound` here, as every HEAD of a missing key is. OpenDAL's `Unexpected` is left out: it gives that kind both to a failure it cannot classify (a backend 500, a reset connection) and to an operation dropped before its end, which every download from an object store is, as is any request whose client went away. Such a failure fails its request, so it shows as a 5xx in `s3dal_requests_total` |
| `s3dal_multipart_uploads_open` | gauge | | Multipart uploads neither completed nor aborted. The stale-upload sweep counts them on the backend, at startup and then hourly, which takes in other instances on the same backend; this server's own creates, completes and aborts move it in between. The sweep runs on one instance of those sharing a backend (see [Background jobs](#background-jobs)), so only that one shows the backend's count; with `stale-upload-age = 0`, or on the others, it counts only this server's changes since its start |
| `s3dal_job_duration_seconds` | summary | `job`, `outcome` | Background job runs on this instance (`_count`) and how long they took together (`_sum`). `job` is `stale-uploads`, `usage-scan`, `lifecycle`, `thumbnail-sweep` or `tier-sweep`; `outcome` is `ok` or `error` |
| `s3dal_job_lease_held` | gauge | `job` | 1 if this instance holds the job's lease, and so runs it; 0 if another instance does |
| `s3dal_bucket_objects` | gauge | `bucket` | Objects in the bucket, every version counted, delete markers and folder markers not: the last [usage count](#bucket-usage) plus, in a bucket with a quota, this instance's writes since. The instance that counts shows every bucket; the others, the buckets they enforced a quota on or were asked about |
| `s3dal_bucket_bytes` | gauge | `bucket` | Bytes in the bucket, counted as `s3dal_bucket_objects` is |
| `s3dal_bucket_quota_bytes` | gauge | `bucket` | The bucket's quota in bytes, for a bucket with one |
| `s3dal_bucket_quota_objects` | gauge | `bucket` | The bucket's quota in objects, for a bucket whose quota limits them |
| `s3dal_events_dropped_total` | counter | | Object events (one per object written or removed) that a consumer of them fell too far behind to receive; the queue holds 4,096, and a slow consumer never holds up a request |
| `s3dal_lifecycle_expired_total` | counter | `kind` | What this instance's lifecycle runs removed: `current` (objects expired; in a versioned bucket, given a delete marker), `noncurrent` (noncurrent versions), `delete_marker` (noncurrent and lone delete markers), `upload` (incomplete multipart uploads aborted by a rule) |
| `s3dal_cache_hits_total` | counter | | GETs the [read cache](#read-cache) answered: of an object it takes, whose content it held as of the ETag the GET's stat found |
| `s3dal_cache_misses_total` | counter | | GETs of an object the read cache takes whose content it did not hold, or held as of an older ETag; the object is read whole from the backend and kept. GETs of objects it does not take (over 1 MiB, or empty) count as neither |
| `s3dal_cache_evictions_total` | counter | | Objects the read cache dropped, least recently used first, to stay within `cache-size` |
| `s3dal_cache_bytes` | gauge | | Bytes of object content the read cache holds |
| `s3dal_notify_deliveries_total` | counter | `target`, `outcome` | [Event notification](#event-notifications) requests to the webhook `target` (its `id`): `ok` for a 2xx answer, `failed` for any other answer or no answer, each retry counted |
| `s3dal_notify_dropped_total` | counter | `target`, `reason` | Event notifications for `target` given up: `overflow` (its queue was full), `retries_exhausted` (still not delivered after its `retry-for`), `refused` (a 4xx answer other than 429) |
| `s3dal_notify_queue_depth` | gauge | `target` | Event notifications waiting for `target`, besides the one being sent |
| `s3dal_tier_reads_total` | counter | `tier` | Reads of cold objects from the [cold tier](#cold-tiers): GETs, copy sources, thumbnails, cache fills. The `s3dal_tier_*` series exist only on a server with tiers |
| `s3dal_tier_read_bytes_total` | counter | `tier` | Bytes read from the tier |
| `s3dal_tier_writes_total` | counter | `tier`, `origin` | Objects written into the tier: `request` (a write naming its class) or `lifecycle` (a transition) |
| `s3dal_tier_written_bytes_total` | counter | `tier`, `origin` | Bytes written into the tier, by origin |
| `s3dal_tier_errors_total` | counter | `tier`, `operation`, `kind` | Tier operations that failed, by OpenDAL's error kind; `missing` is a read of a tier object a stub names that is not there |
| `s3dal_tier_transition_skipped_total` | counter | `reason` | Lifecycle transitions given up on: `key-too-long` (the key does not fit the tier's paths), `changed` (the object changed while it moved; tried again on the next run), `error` |
| `s3dal_tier_orphans_removed_total` | counter | `tier` | Tier objects no stub named that `tier-sweep` removed |
| `s3dal_tier_objects` | gauge | `tier` | Cold objects (every version) naming the tier, as of the last `tier-sweep` on this instance |
| `s3dal_tier_bytes` | gauge | `tier` | Bytes of those cold objects |
| `s3dal_tier_dangling` | gauge | `tier` | Stubs naming a tier object that is not in the tier, as of the last sweep; anything above 0 is lost data to look into |
| `s3dal_log_forward_dropped_total` | counter | | Log lines not sent to the collector because its queue was full |
| `s3dal_build_info` | gauge | `version` | Always 1 |
| `process_resident_memory_bytes` | gauge | | Resident memory (Linux and macOS) |
| `process_cpu_seconds_total` | counter | | User and system CPU time |
| `process_open_fds` | gauge | | Open file descriptors (Linux and macOS) |
| `process_max_fds` | gauge | | The limit on open file descriptors |
| `process_start_time_seconds` | gauge | | When the server started, in Unix seconds |

A labeled series appears once it has counted something, so a scrape lists the operations in
use rather than every one at zero. The counters start at zero with each start, a restart from
the console included.

The console's Dashboard shows the last hour of the same numbers at 10-second resolution:
requests per second by kind of operation, the error rate, p50 and p99 latency, throughput and
memory. The server keeps that hour in memory (360 samples), so it starts empty after a
restart; for longer history, scrape the endpoint. Below the charts it lists every bucket's
[usage](#bucket-usage), with a bar against its quota for a bucket that has one.

## Background jobs

s3dal runs some work in the background: the stale-upload sweep (`stale-upload-age`), the
usage count (`usage-scan-interval`, see [Bucket usage](#bucket-usage)), `lifecycle`, which
looks in hourly and applies each bucket's [lifecycle rules](../README.md#lifecycle-rules) once
a day (UTC), and again when they changed, listing the bucket whole, its versions included, and
`thumbnail-sweep`, which removes stale [thumbnails](#thumbnails) daily, and, with
[cold tiers](#cold-tiers), `tier-sweep`, which checks every tier against the stubs daily. With
several instances sharing one backend, each job runs on one of them, the holder of its lease,
`.s3dal/leases/<job>.json` in the backend (`{"node": ..., "until": ...}`). The holder renews it
every 20 seconds, for 60 from then; an instance that stops (it died, or the job was turned off
there) is replaced by another within about 80 seconds, and the new holder runs the job at once,
catching up. Each process start is a new holder, with a random id. On fs, which one process
serves, the lease holds within the process. The lease guards against wasted work, not against
harm: the jobs are safe to run twice, which instances whose clocks are more than 20 seconds
apart could do.

## Bucket usage

[Quotas](../README.md#bucket-quotas), the `s3dal_bucket_*` metrics and the Dashboard's bucket
usage come from one count. The job `usage-scan` lists every bucket whole, its versions
included, every `usage-scan-interval` (default 5 minutes), and records what it found in
`.s3dal/usage/<bucket>.json`, `{"objects", "bytes", "at", "cold_objects", "cold_bytes"}`, `at`
being when the listing started; the cold figures are the part of the totals whose bytes are in a
[cold tier](backends.md#cold-tiers), counted at their size like any other object.
It sits beside the bucket's state rather than in it: the state is read by every anonymous
download and written only when an operator changes a setting, and the count is rewritten every
few minutes. Every instance reads the newest count at most every 5 seconds and adds what its
own writes changed since `at`, which each write's object event carries; a write made while the listing ran may be counted twice until the
next count, which errs towards refusing.

What this means for an operator:

- A bucket gets counted as soon as it is given a quota; until then its writes are not counted
  as they happen, only by the next count.
- One instance is exact for its own writes: it never lets them take a bucket past its quota.
  Two writes in flight at once on one instance can both pass, by at most one write each.
- Another instance's writes reach this one with the next count: the count runs on the lease's
  holder at most one runner tick (20 seconds) after it is due, and the others take it up within
  5 seconds. Measured with a 1-second interval, two instances on one fs root and on one RustFS
  bucket: the instance that did not write refused 4.8 and 5.0 seconds after the other filled
  the bucket, the time it trusts the count it read. With the default,
  a bucket shared by several instances can go past its quota by what the others write in up to
  about 5 minutes and 25 seconds.
- Objects written behind s3dal's back are counted by the next count.
- A count lists the whole bucket: on S3 one request per 1,000 objects and versions, on fs a
  walk of the directory tree. Raise `usage-scan-interval` for buckets of many millions of
  objects; `0` stops counting, after which quotas see only the last count plus each instance's
  own writes since.

## Cold tiers

With [cold tiers](backends.md#cold-tiers) configured, the primary backend holds only an empty
stub for each cold object, and the bytes are in the tier:

- **Back up every tier with the primary**, at the same time. A backup of the primary alone
  restores stubs without their bytes, whose GETs then fail with `500 InternalError`; a backup of
  a tier alone holds bytes no key names. A tier object's own metadata holds what was known
  before its body arrived (headers, user metadata, a checksum the client sent), not its ETag.
- **Never edit a tier's `.s3dal/` by hand.** A tier object that disappears behind s3dal's back
  leaves a stub naming nothing: its GET answers `500 InternalError` and logs an error naming
  the key and the tier path, never `404`, which a sync tool would take for a delete. A tier
  object no stub names, left by a write that crashed between the tier and the stub, by a
  removal that failed after the key changed, or by two instances moving one object at once,
  only takes space.
- **The `tier-sweep` job** lists each tier and every bucket's stubs, its versions included,
  once a day. It removes a tier object no stub names once it is more than a day old (a younger
  one may belong to a write or a move that has not put its stub in place yet), including those
  of buckets that no longer exist, and counts them in `s3dal_tier_orphans_removed_total`. A stub
  whose tier object is gone is logged as an error and counted in `s3dal_tier_dangling`, never
  changed. `s3dal_tier_objects` and `s3dal_tier_bytes` are the size of each tier as the sweep
  found it.
- **A tier that cannot be reached stops startup**, as the primary does: started without it,
  every read of its objects would fail one by one, and their deletes would leave the bytes
  behind.
- **Retiring a tier**: copy its objects back first, then remove the `[[tier]]` table and
  restart. A CopyObject of an object onto itself with `x-amz-storage-class: STANDARD`, or with
  another tier's class (see [Cold tiers](backends.md#cold-tiers)), moves it there and removes
  its bytes from the tier. A stub naming a tier the server no longer has answers
  `500 InternalError` on GET.

## Event notifications

A bucket's notification configuration (see
[Event notifications](../README.md#event-notifications)) sends each matching object event to a
webhook named by a `[[notify]]` table:

```toml
[[notify]]
id = "thumbnails"                  # the ARN is arn:s3dal:sqs:<region>:thumbnails:webhook
url = "https://hooks.example.com/s3"
hmac-secret = "…"                  # optional: sign each body
auth-token = "…"                   # optional: send Authorization: Bearer <token>
queue-size = 10000                 # optional, the default
retry-for = "1h"                   # optional, the default
```

- Each event is one `POST` with `Content-Type: application/json` and a body `{"Records":
  [record]}`, one record per request, in AWS's format (`eventVersion` `2.1`, `2.3` for a
  lifecycle expiry or transition): `eventSource` `aws:s3`, `awsRegion`, `eventTime` (UTC, milliseconds),
  `eventName` (`ObjectCreated:Put`, `ObjectRemoved:DeleteMarkerCreated`, ...),
  `userIdentity.principalId`, `requestParameters.sourceIPAddress`,
  `responseElements.x-amz-request-id`, and `s3` with `s3SchemaVersion` `1.0`,
  `configurationId` (the rule's id), `bucket` (`name`, `ownerIdentity.principalId`, `arn`) and
  `object` (`key` URL-encoded as AWS encodes it, `size`, `eTag`, `versionId` in a versioned
  bucket, `sequencer`). Where these differ from AWS's is in
  [Differences](differences.md#other-differences).
- With `hmac-secret`, the request carries `X-S3dal-Signature: sha256=<hex>`: the HMAC-SHA256
  of the request body exactly as sent, keyed with the secret's UTF-8 bytes, in lowercase hex.
  A receiver computes the same over the raw body before parsing it, and compares in constant
  time. The signature covers the body only; a receiver that must refuse replays checks
  `eventTime` or remembers `sequencer`s.
- Sending never holds up a request, nor one target another: each target has a queue of
  `queue-size` events, emptied by a task of its own, one request at a time, so a target gets
  its events in the order they happened. An event that finds the queue full is dropped.
- A request that gets no answer within 10 seconds, cannot connect, or is answered `429` or
  `5xx` is retried after 1 second, then 2, 4, ... up to 30 seconds between tries, until
  `retry-for` after the event was queued; then it is dropped. An event that waited in the queue
  past its `retry-for` is dropped without being sent. `retry-for = "0s"` tries each event once.
  Any other answer (`2xx` delivers it; another `4xx` refuses it) ends its tries. The log says
  when a target stops answering, and when it answers again.
- Events queued are lost when s3dal stops, restarts or crashes; there is no queue on disk.
  Delivery is at most once per rule: an event lost is not sent later.
- `[[notify]]` changes apply on reload, from the config file or the console, without a
  restart. A target whose settings did not change keeps its queue; a changed one gets a new
  queue, while the old one is still emptied. A configuration naming a target removed since
  sends nothing to it.
- `check-config`, the log and the console never show `hmac-secret`, `auth-token`, or a
  target's `url` beyond its scheme, host and path: the user, password, query and fragment are
  left out, since a receiver may take a token in the query. The console's bucket page lists the targets, with each one's last
  delivery and its error, to key pairs that may manage the server.

## Thumbnails

`GET /<bucket>/<key>?x-s3dal-thumbnail=<size>` (see [Thumbnails](../README.md#thumbnails))
is GetObject: the same access rules, conditional headers (`If-None-Match` with the object's
ETag answers `304`), `versionId`, and `response-*` overrides. The answer carries the
object's ETag and Last-Modified, which change exactly when the thumbnail does, and
`Content-Type: image/jpeg` or `image/png`; Range and `partNumber` are ignored, the thumbnail
comes whole.

- **Sizes**: 128, 256, 512 or 1024, the square the image is fitted into. Any other value, or
  the parameter twice, is `400 InvalidArgument`.
- **Cache**: `.s3dal/thumbs/<bucket>/<E(key)>!!/<etag>-<size>.jpg` (or `.png`) in the backend,
  `E` being the escape of the versions tree; an ETag that is not safe in a file name (GCS's
  base64 ones) is replaced by its MD5 in hex. Made on the first request, while it waits, and
  read back by every instance sharing the backend after that. A thumbnail that cannot be kept
  (a key too long for the backend's names) is still served, and made again next time. An
  overwrite changes the ETag, so the old thumbnail is never served again; the background job
  `thumbnail-sweep` removes, daily, every thumbnail whose object is gone or has another ETag
  now, including those of versions other than the current one. It stats one object per key
  with thumbnails, a request each on a remote backend. Deleting a bucket removes its
  thumbnails.
- **Formats**: JPEG, PNG, GIF (the first frame) and WebP, told by their content, not by the
  key's extension; an EXIF rotation is applied. Out: JPEG at quality 80, or PNG when a pixel
  of the image is transparent.
- **Bounds**: each thumbnail decodes its whole source in memory, so:

  | Refused when | Answer |
  |---|---|
  | The object is larger than 64 MiB (checked before any of it is read) | `400 EntityTooLarge` |
  | The image has more than 50 megapixels (checked from its header, before decoding), or would take a decoder more than 400 MB | `400 EntityTooLarge` |
  | The content is not one of the four formats | `501 NotImplemented` |
  | The content claims one of them but does not decode | `400 InvalidRequest` |
  | The thumbnail is not made within 20 seconds of the request reaching the decoders, the wait for a decoder included | `503 ServiceUnavailable`, which SDKs retry |

  No thumbnail is kept for a refused one. Half the CPU cores decode at once; the other
  requests wait for one of them, so a grid of photos never takes every core from the requests
  for other objects. A decode that runs past the 20 seconds keeps its decoder until it ends,
  since it cannot be stopped midway; the request has its answer already.
- **Memory**: a decode holds the whole image, 3 bytes a pixel (4 with alpha), so a 50
  megapixel one takes about 150 MB, and the peak is about that times the decoders. Measured on
  a release build on 18 cores (9 decoders): 16 concurrent thumbnails of different 7071 × 7071
  JPEGs raised the server's resident memory from 14 MiB to a peak of 1362 MiB, and all 16
  answered within 0.3 s. Size the memory for the cores, or keep large originals out of the
  grid. (The peak is `/usr/bin/time -l`'s maximum resident set size of the server, confirmed
  by sampling `ps` every 20 ms; each request was a cache miss of size 256.)
- **Build**: the Cargo feature `thumbnail` (default on) brings the `image` crate with these four
  codecs only, all pure Rust, and adds about 1 MB (3.5 %) to the release binary (29,984,928
  bytes against 28,957,792 without it, on aarch64-apple-darwin). A build without it answers
  the parameter with `501 NotImplemented`.

## Read cache

`cache-size` (`--cache-size`, `S3DAL_CACHE_SIZE`; default `0`, off) gives GetObject a cache in
memory on a remote backend (`s3`, `azblob`, `gcs`), for objects that are read again and again:
the images of a web page, small configuration and manifest files.

- **What it keeps**: the whole content of objects up to 1 MiB, once a GET has read one. A hit
  saves the read from the backend, its round trip to the first byte, which is most of what a
  small GET costs; for larger objects the transfer takes over, and one of them would hold the
  room of many small ones. Empty objects are not kept: their GET reads nothing anyway.
- **Never stale**: every GET still stats the object, as it does without a cache, and an entry
  answers only when the backend ETag that stat returns is the one the entry was read with. The
  read that fills an entry is pinned to that ETag (`If-Match`), so the bytes kept are the
  version stat'ed. An object overwritten or deleted by another instance sharing the backend, or
  by any other tool, is read afresh at once; an overwrite replaces the entry. Headers (ETag,
  Content-Type, metadata, version id, checksums) always come from the stat, never from the
  cache.
- **Ranges and parts**: a Range or `partNumber` GET of an object the cache takes reads the
  whole object on a miss, at most 1 MiB, and is cut from it in memory, then and on every hit;
  the bytes and headers are those of an uncached read.
- **Versions**: each version is an object of its own in the backend, and is cached as one; a
  GET by `versionId` gets that version.
- **Budget**: `cache-size` bounds the content held; entries go least recently used first. Each
  entry's bookkeeping, about a hundred bytes and its path, comes on top. A change, in the config
  file or the console's Settings, takes a restart (the log, and the console, say one is
  pending).
- **Not on fs**: refused at startup and by `check-config`. The OS page cache already keeps
  recently read files in memory, and on fs a GET of an object up to 2 MiB already reads it in
  the same trip as its stat.
- **Metrics**: `s3dal_cache_hits_total`, `s3dal_cache_misses_total`,
  `s3dal_cache_evictions_total`, `s3dal_cache_bytes` (see [Metrics](#metrics)).

What it saves is the read, not the stat: a hit still costs one backend round trip, where a miss
or an uncached GET costs two. Measured on a Mac (release build, both servers over one RustFS
1.0.2 container on the loopback, 100 hot objects per size, one client, three interleaved rounds
of 10 s each, load average 4.5–6.3 throughout; median over the rounds):

| Object size | p50 with cache | p50 without | p99 with cache | p99 without |
|---|---|---|---|---|
| 1 KiB | 1.04 ms | 1.29 ms | 2.76 ms | 3.46 ms |
| 64 KiB | 1.39 ms | 1.73 ms | 4.47 ms | 3.90 ms |
| 1 MiB | 2.16 ms | 4.23 ms | 4.17 ms | 6.92 ms |

Over a network, where each round trip costs milliseconds rather than a fraction of one, the
saving per hit is that round trip.

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

# Changelog

All notable changes to s3dal are listed here, in the format of
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.4.0] - 2026-11-18

### Added

- S3 versioning on every backend: PutBucketVersioning/GetBucketVersioning, version ids on
  writes, delete markers, reads, copies and deletes by version, ListObjectVersions, Suspended
  buckets and the `null` version. Old versions are kept under `.s3dal/versions/`: hard links on
  fs, server-side copies on S3 upstreams, streamed copies on Azure Blob and GCS. Several
  instances sharing an S3 upstream with conditional writes lose no version to concurrent
  overwrites; a write that loses such a race answers `503 SlowDown`, which SDKs retry.
- Key pairs: `access = "write-only"`, a drop box that uploads and nothing else (no GET, HEAD,
  listing, delete or copy), and `expires = "<RFC 3339>"`, after which the pair is refused as
  unknown (`InvalidAccessKeyId`). The console's key pair editor sets both.
- Console: versioning. Buckets show and switch their versioning state; "Show versions" lists
  each object's versions and delete markers; an object's details download, restore or delete
  any version, and undelete a deleted object; deleting a bucket that still holds anything offers
  to empty it first, versions included.
- Prometheus metrics at `GET /_s3dal/metrics`, on the S3 and console ports: requests by
  operation and status class, their duration, bytes in and out, requests in flight, connections
  by protocol, backend operation latency and errors, open multipart uploads, dropped log lines,
  and the process's memory, CPU and file descriptors. `--metrics token|public|off` (default
  `token`, with `--metrics-token`) says who may read them, and changes without a restart; with
  no token set, the default refuses every scrape. See
  [Metrics](guide/operations.md#metrics).
- Console: a Dashboard of the last hour at 10-second resolution (requests per second by kind of
  operation, error rate, p50 and p99 latency, throughput, memory), each chart with a crosshair
  and a table view. The Server page adds when the backend was probed, whether the log collector
  takes the log and how many lines it dropped, and who may read the metrics; Settings edits
  `metrics` and `metrics-token`.
- `s3dal public s3://bucket [on|off]` shows or switches whether a bucket is public, through the
  console's address (`--endpoint`).
- Background jobs run on one instance of those sharing a backend, the holder of the job's
  lease (`.s3dal/leases/<job>.json`), which another instance takes over within about 80 seconds
  of its holder stopping. The stale-upload sweep is the first such job. Metrics
  `s3dal_job_duration_seconds` (runs by job and outcome) and `s3dal_job_lease_held`. See
  [Background jobs](guide/operations.md#background-jobs).
- Object events, for embedders (`S3Dal::subscribe`) and for the features built on them: one per
  change to a key that took effect (PUT, POST, CopyObject, CompleteMultipartUpload,
  DeleteObject, each key DeleteObjects removed), with the bucket, key, size, ETag, version,
  request id and access key. A consumer that falls behind loses events, counted in
  `s3dal_events_dropped_total`; requests never wait for one.
- Bucket quotas: a largest size and optionally a most number of objects per bucket, every
  version counted. A PUT, form upload, copy or CompleteMultipartUpload that would take the
  bucket past it gets `403 QuotaExceeded` before its body is read; CreateMultipartUpload is
  refused in a bucket already past it. Set on the console's bucket page, which shows a usage
  bar, through the settings API (`quota` of `/_s3dal/admin/buckets/<bucket>`), or with
  `s3dal quota s3://bucket --max-bytes 10GiB [--max-objects N] | --clear`. What a bucket holds
  is counted by a new background job, `usage-scan`, every `--usage-scan-interval` (default
  `5m`), plus each instance's own writes since. Metrics `s3dal_bucket_objects`,
  `s3dal_bucket_bytes`, `s3dal_bucket_quota_bytes` and `s3dal_bucket_quota_objects`; the
  Dashboard lists each bucket's usage against its quota. See
  [Bucket quotas](README.md#bucket-quotas).
- Bucket lifecycle rules: PutBucketLifecycleConfiguration, GetBucketLifecycleConfiguration and
  DeleteBucketLifecycle, with Expiration (`Days`, `Date`, `ExpiredObjectDeleteMarker`),
  NoncurrentVersionExpiration (`NoncurrentDays`, `NewerNoncurrentVersions`) and
  AbortIncompleteMultipartUpload, filtered by prefix and object size. Tag filters are refused
  with `NotImplemented`, as are transitions on a server without cold tiers (with tiers, see
  Cold tiers below). A new background job, `lifecycle`, applies each
  bucket's rules once a day and within the hour of a change, at AWS's times (the first
  midnight UTC after the days are up); in a versioned bucket an expired object gets a delete
  marker. Each expiry is an object event (`s3:LifecycleExpiration:Delete`,
  `s3:LifecycleExpiration:DeleteMarkerCreated`), so quotas follow. PUT, GET and HEAD send
  `x-amz-expiration`. An abort rule replaces `--stale-upload-age` for the uploads it covers.
  The console's bucket page lists, adds, edits and removes rules. Metric
  `s3dal_lifecycle_expired_total{kind}`. See [Lifecycle rules](README.md#lifecycle-rules).
- Bucket event notifications to webhooks: PutBucketNotificationConfiguration and
  GetBucketNotificationConfiguration, with AWS's event names (`s3:ObjectCreated:*`,
  `s3:ObjectRemoved:*`, `s3:LifecycleExpiration:*` and each specific one) and prefix and suffix
  filters. The operator configures the webhooks as `[[notify]]` tables (`id`, `url`, optional
  `hmac-secret`, `auth-token`, `queue-size`, `retry-for`), applied without a restart; a
  configuration names one by `arn:s3dal:sqs:<region>:<id>:webhook`, and an unknown one is
  refused. Each event is POSTed as an AWS S3 event message (`eventVersion` 2.1), signed with
  `X-S3dal-Signature: sha256=<hex HMAC-SHA256 of the body>` when the target has a secret. Each
  target has a queue in memory of its own and retries a failing webhook with a growing pause,
  then drops the event: nothing is kept on disk. Metrics
  `s3dal_notify_deliveries_total{target,outcome}`, `s3dal_notify_dropped_total{target,reason}`
  and `s3dal_notify_queue_depth{target}`. The console's bucket page lists, adds, edits and
  removes the rules, and shows each target's last delivery. See
  [Event notifications](README.md#event-notifications).
- Image thumbnails on the S3 port: `GET /<bucket>/<key>?x-s3dal-thumbnail=128|256|512|1024`
  answers with the image fitted into that square, as JPEG, or PNG when it has transparent
  pixels, from JPEG, PNG, GIF (first frame) and WebP. It is GetObject, so a presigned URL signs
  the size, and a write-only pair or an unsigned request outside a public bucket gets none.
  Made on the first request and kept in the backend under `.s3dal/thumbs/`, named by the
  object's ETag, so instances share them and an overwrite is never shown as before; a new
  background job, `thumbnail-sweep`, removes stale ones daily. Sources over 64 MiB or 50
  megapixels get `400 EntityTooLarge`, other formats `501 NotImplemented`, undecodable ones
  `400 InvalidRequest`, and a thumbnail not made within 20 seconds `503 ServiceUnavailable`;
  half the cores decode at once. Cargo feature `thumbnail`, on by default. The console's object
  browser has a grid view of thumbnails, and an image's details preview its thumbnail instead
  of the original. See [Thumbnails](README.md#thumbnails).
- A read cache in memory for remote backends: `--cache-size` (`S3DAL_CACHE_SIZE`, config key
  `cache-size`, default `0`, off) gives it a budget, and GetObject then keeps objects up to
  1 MiB, evicting the least recently used. An entry answers only a GET whose stat finds the ETag
  it was read with, so an object changed by another instance or another tool is never served
  stale; Range and partNumber are cut from the cached object. Refused on fs, whose files the OS
  page cache already holds; takes a restart to change, and the console's Settings save it for
  the next restart as they do the backend. Metrics `s3dal_cache_hits_total`,
  `s3dal_cache_misses_total`, `s3dal_cache_evictions_total` and `s3dal_cache_bytes`. See
  [Read cache](guide/operations.md#read-cache).
- Cold tiers: `[[tier]]` tables in the config file (`name`, `storage-class`, `backend`,
  `[tier.backend-opt]`) add backends that hold only the bytes of the objects written to them. A
  PutObject or form upload naming a tier's storage class streams its body there and leaves an
  empty stub at the key in the primary, whose metadata names the tier object
  (`.s3dal/tier/<bucket>/<E(key)>!!/<id>` in the tier). GET (ranges, `partNumber`, conditions),
  copies, UploadPartCopy and thumbnails read cold objects through their tier; GET, HEAD,
  GetObjectAttributes and listings report the tier's class; quotas and usage count their size,
  and the usage count records the cold part (`cold_objects`, `cold_bytes`). Deletes and
  overwrites remove the tier copy in unversioned buckets; in versioned ones it goes with its
  version. A tier that cannot be reached, or that another s3dal's primary claimed
  (`.s3dal/tier/owner.json`, against the primary's new `.s3dal/id`), stops startup; a changed
  `[[tier]]` takes a restart. A stub whose tier object is gone answers `500 InternalError`,
  never `404`; a GET racing an overwrite of a cold object answers `503 SlowDown`. A class no
  tier has stays advisory, as before. Other tools reading the primary see cold objects as empty
  files, so back up every tier with the primary. CopyObject and multipart uploads
  (CreateMultipartUpload's class, recorded and reported by ListParts and ListMultipartUploads)
  write into a tier too. A CopyObject of an object onto itself with another class moves it,
  bytes, headers and metadata kept: into a tier, between tiers, or back to the primary with
  `STANDARD` (the class a copy naming none gets), which removes the tier copy; before tiers such
  a self-copy was `InvalidRequest` without `REPLACE`, and it still is where the class it would
  get is the one it has. Lifecycle rules take `Transition` (`Days` or `Date`) and
  `NoncurrentVersionTransition` (`NoncurrentDays`, `NewerNoncurrentVersions`) to a tier's
  class, and a class no tier has is refused with `InvalidStorageClass`. The `lifecycle` job
  moves an object in place, at AWS's times: its ETag, Last-Modified, version id, headers and
  metadata stay, only its bytes go to the tier (or on to the next tier, the transition due last
  winning); an expiration due the same day wins, and size filters and expiry see a cold
  object's own size. An object changed while it moves is left as the client wrote it and tried
  again on the next run. Each move is an `s3:LifecycleTransition` event. A new background job,
  `tier-sweep`, runs daily with tiers: it removes tier objects no stub names once they are a
  day old, and reports stubs whose tier object is gone. Metrics `s3dal_tier_reads_total`,
  `s3dal_tier_read_bytes_total`, `s3dal_tier_writes_total{origin}`,
  `s3dal_tier_written_bytes_total{origin}`, `s3dal_tier_errors_total`,
  `s3dal_tier_transition_skipped_total{reason}`, `s3dal_tier_orphans_removed_total`,
  `s3dal_tier_objects`, `s3dal_tier_bytes` and `s3dal_tier_dangling`. The console marks
  objects of another class than `STANDARD` in the browser, shows an object's class in its
  details with "Change storage class" (the self-copy above), uploads to a tier's class when
  tiers exist, edits the transitions of lifecycle rules, offers `s3:LifecycleTransition` among
  the event kinds, lists the tiers on the Server page with what the last sweep counted, and
  splits each bucket's usage hot and cold; its restart first checks that every tier answers and
  is still this server's. See [Cold tiers](guide/backends.md#cold-tiers).

### Changed

- **Breaking:** whether a bucket is public is now a setting of the bucket, kept in the backend
  (`.s3dal/buckets/<bucket>.json`) with its versioning state, so every instance sharing the
  backend serves it (within 5 seconds), and a bucket deleted and created again starts private.
  `--public-bucket`, `S3DAL_PUBLIC_BUCKETS` and the config key `public-bucket` are removed: a
  config file that still has the key, or a command line with the flag, is refused at startup
  (`unknown key`); the variable is no longer read, so a bucket it named is private until set
  again. To migrate, remove the setting and make each bucket public again, on the console's
  Buckets page or with `s3dal public s3://<bucket> on --endpoint <console address>`.

## [0.3.2] - 2026-11-10

### Added

- `--log-forward` sends to more collectors: syslog (RFC 5424 over UDP, TCP or TLS), OTLP over
  HTTP for OpenTelemetry collectors (`otlp+https://…`), Splunk's HTTP Event Collector
  (`splunk+https://:TOKEN@…`), and any HTTP input that takes newline-delimited JSON
  (`ndjson+https://…`).
- Console: settings that take a restart have "Save" and "Save and restart", and the "Restart to
  apply" banner a "Restart now" button. s3dal first checks that it would start again with the
  saved settings and refuses with the reason if not; it then stops as on SIGTERM and runs the
  same command line again in the same process, keeping its PID. The page waits for it, or says
  where the console has moved to. Unix only.

## [0.3.1] - 2026-11-10

### Added

- `--log-forward`: the log also goes to Graylog (GELF over UDP, TCP or HTTP) or to
  Elasticsearch and OpenSearch (bulk API, Basic authentication), from a queue that never holds
  a request up. At exit, s3dal waits up to 3 seconds for the queue to empty.
- The log says when s3dal is stopping and when it has stopped, and the error it stopped on.
- Public buckets: `--public-bucket` (`public-bucket = [...]`) lets anyone download a bucket's
  objects without a key pair; listing and writing still take one. It changes without a
  restart, and the console's Buckets page switches it, with a link that does not expire.
- `-v` prints the version, as `-V` and `--version` do.
- Console: the Server page shows where the log goes, and settings with a fixed set of values
  (backend, log level, log format, log collector) are menus.
- The Docker image has `/var/log/s3dal`, writable by its user, for `S3DAL_LOG_FILE`.

### Fixed

- `--log-file` creates the directories it is in, at startup and after a rotation, instead of
  refusing to start.
- The README's icon no longer sits on the title's rule.

## [0.3.0] - 2026-11-04

### Added

- `--log-file`, with size-based rotation: `--log-max-size` (default `100MiB`, `0` never
  rotates) and `--log-max-files` (default 5) rotated files kept.
- `--log-level`, which a config reload or the console changes without a restart.
- Key pair limits: `allow` (buckets, or `bucket/folder/`) and `access = "read-only"` in a
  `[[credentials]]` table. A limited pair lists only the buckets it reaches, cannot list
  above its folders, and cannot manage buckets or the console's settings.
- Console: uploads send a SHA-512 checksum, which the object's details show in hex; an upload
  asks before replacing an existing object (replace, skip, keep both), an option per browser.
- Console: the upload panel minimizes, keeps its scroll position, lists every file of a large
  upload in order with a total, and stays open once scrolled.
- Console: the settings that take a restart are editable, the backend and its options
  included; a new backend is built and probed before it is saved.
- The project icon, in the console and the README.

### Changed

- `--log-level` (`S3DAL_LOG_LEVEL`) replaces `RUST_LOG`, which s3dal no longer reads. s3s's
  lines stay off unless a directive names `s3s`, and a word that is not a level, alone or
  after `=`, is refused rather than read as a target at TRACE.
- `--help` lists each command with one line, and wraps at the terminal's width.

## [0.2.0] - 2026-11-03

### Added

- A web console, on a port of its own with `--console-address`: browse, upload, preview,
  download and delete objects, share presigned links, manage key pairs, and change settings.
  Requests are signed in the browser with the key pair that signed in; changed settings are
  saved to the config file, its comments kept.
- Settings that change while s3dal runs: an edit of the config file, or a renewed TLS
  certificate and key, applies within seconds, or at once on SIGHUP. Key pairs, region,
  domains, `etag-buffer-limit` and `stale-upload-age` change without a restart; the rest is
  reported as waiting for one. A file with a mistake leaves the running settings alone.
- Client subcommands for any S3 endpoint: `ls`, `mb`, `rb`, `cp`, `mv`, `rm`, `cat`, `stat`,
  `presign`, in the `client` feature, on by default.
- `s3dal check-config`, `s3dal healthcheck`, `s3dal keygen`, and the unsigned health endpoint
  `GET /_s3dal/health`.
- An access log line for every request, with its request ID, status, error code, access key,
  bytes sent and time taken; `--log-format json` for one JSON object per line.
- `docker-compose.yml`, with the console on, and the image's health check.

### Changed

- The key pair given by `--access-key` and `--secret-key` (or `S3DAL_ACCESS_KEY` and
  `S3DAL_SECRET_KEY`) is added to the config file's `[[credentials]]` instead of replacing
  them. A deployment that relied on the variables to shut out the file's pairs must remove
  them from the file.
- s3s's own log lines are off by default (`RUST_LOG` default `info,s3s=off`): it logged every
  refused request, an unsigned one included, at ERROR. The access log reports them at INFO.
- The image's config file belongs to the user s3dal runs as, so the console can save to it,
  and the image exposes the console's port 9001 too.
- README covers features and settings; backends, clients, operations, compatibility and
  differences from S3 moved to `guide/`.

## [0.1.1] - 2026-10-21

### Added

- Docker images for amd64 and arm64 on `ghcr.io/linzeyan/s3dal`, published with each release
  as the version, `X.Y` and `latest`.

### Changed

- On fs, listings read their keys' metadata in batches rather than one call per key, and a
  delimiter listing remembers the file that proved each common prefix, so the next listing
  checks that file instead of reading the directory again.
- On fs, GET and HEAD look the object up before the bucket, and a GET of up to 2 MiB is one
  stat and read of the same file.
- On fs, DeleteObjects removes each directory it emptied once, after the batch.
- On fs, the parts of a multipart upload are written into one file at their place in the
  object, and Complete renames that file into place when the listed parts fill it exactly: a
  64 MiB upload in 16 MiB parts writes 64 MiB to an ext4 disk instead of 128. Otherwise
  Complete copies the parts file to file (`copy_file_range` on Linux). Uploads begun with 0.1.0
  can still be completed, also with parts added after the upgrade.
- On Linux with glibc, malloc is bounded to two arenas per core (`MALLOC_ARENA_MAX`, when set,
  decides instead). glibc's eight per core held on to the buffers of concurrent large uploads:
  in a benchmark with 8 concurrent 64 MiB uploads the server peaked at 1,175 MiB, now at
  468–598 MiB. Listings are 6–11% slower for it.

## [0.1.0] - 2026-10-18

The first version.

### Added

- `s3dal serve`: the S3 API over the OpenDAL `fs` backend and, with the default features
  `services-s3`, `services-azblob` and `services-gcs`, over an S3-compatible upstream, an Azure
  Blob Storage container, or a Google Cloud Storage bucket. Settings by flag, `S3DAL_*`
  environment variable, or a TOML config file (`--config`).
- On Azure, metadata names are stored encoded as the C# identifiers it requires, and read back
  as sent.
- SigV4 authentication with one or more key pairs, all acting for one owner; no anonymous
  access. Presigned URLs, and browser form uploads (PostObject) with SigV4 POST policies.
- HTTPS with `--tls-cert` and `--tls-key` (HTTP/2 and HTTP/1.1).
- A request ID on every response and in every error document, also carried by the log lines of
  the request.
- A startup probe that measures what the backend stores natively, and refuses a backend that
  cannot keep user metadata, such as a filesystem without user xattrs. A backend's ETag is
  used only if it is the MD5 of the content, and its Content-Encoding only if gzip content
  reads back as stored.
- Buckets: create, head, list, delete, location, versioning status (always unversioned), and
  owner-only ACLs. The configurations s3dal does not have (policy, CORS, lifecycle, ...) read
  as unset, as S3 answers for a bucket without them.
- Objects: put, get (with Range and `partNumber`), head, delete, batch delete, copy,
  GetObjectAttributes, and owner-only ACLs. Uploads are verified against `Content-MD5` and
  `x-amz-checksum-*`; standard headers, user metadata, and checksums are stored with the
  object.
- ETags that are the backend's own, or an MD5 stored with the object, or, when neither is
  possible, a synthesized value that is never mistaken for an MD5.
- Conditional reads and writes, including size and time conditions on deletes, and
  virtual-hosted-style requests. Where the backend checks write conditions itself (s3, azblob,
  gcs), conditional writes hold across s3dal processes sharing it.
- ListObjectsV2 and ListObjects (v1) with any delimiter, and ListObjectVersions, which lists
  every object as version `null`.
- Multipart uploads, including UploadPartCopy, COMPOSITE and FULL_OBJECT checksums, and retried
  CompleteMultipartUpload requests answered with the original result. On an S3 upstream, parts
  are copied server-side and the ETag matches the one the client computes; on gcs they are
  composed server-side. Uploads older than
  `--stale-upload-age` (24 hours by default) are removed, as are, on fs, the temporary files of
  writes a crash or shutdown cut off.
- Requests for features s3dal does not have (SSE, object lock, tags, ACLs that grant access)
  are refused with `NotImplemented` instead of being ignored.
- Graceful shutdown on SIGTERM and Ctrl-C, which refuses new connections at once; connections
  that stall for 30 seconds are closed, and a request body that stalls as long gets
  `RequestTimeout`.
- Calls to a remote backend that fail transiently are retried with backoff before a request
  fails.
- The ceph s3-tests compatibility report (`compat/s3-tests.md`) for fs, an S3 upstream
  (RustFS), Azure Blob Storage (Azurite), and Google Cloud Storage (fake-gcs-server).
- `scripts/with-azurite.sh` and `scripts/with-fake-gcs.sh`, which run the tests, smoke tests
  and s3-tests against a throwaway emulator, as `scripts/with-rustfs.sh` does for S3.
- A Docker image, GitHub Actions workflows, README, and CONTRIBUTING.

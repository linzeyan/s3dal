# Changelog

All notable changes to s3dal are listed here, in the format of
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

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

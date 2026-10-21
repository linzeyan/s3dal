# Changelog

All notable changes to s3dal are listed here, in the format of
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

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

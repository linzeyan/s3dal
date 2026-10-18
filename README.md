# s3dal

s3dal serves the Amazon S3 API on top of an [Apache OpenDAL](https://opendal.apache.org/)
backend: a local directory, an S3-compatible service, Azure Blob Storage, or Google Cloud
Storage. It is a single Rust binary, and a library: `s3dal::S3Dal` implements the `s3s::S3`
trait, so it can be embedded in another service. [s3s](https://github.com/s3s-project/s3s)
handles the protocol (routing, SigV4, XML); OpenDAL handles storage.

## What it is and is not

s3dal is a gateway. A bucket is a top-level directory of the backend, and an object is a file
(or an object) under it. Object metadata that the backend cannot store natively, such as the
ETag and Content-Type on a filesystem, is kept in the backend's user metadata (extended
attributes on fs), written together with the object. There are no sidecar files and no local
database.

It is not a storage system. The backend owns durability: there is no erasure coding,
replication, or healing. It also has none of these:

- users, IAM, bucket policies, or ACLs that grant access: every configured key pair has full
  access to every bucket;
- versioning, object lock, server-side encryption, object tags, lifecycle rules.

A request that relies on one of these features is refused with `NotImplemented` rather than
answered as if it had been applied, so a client never believes an object is encrypted, locked,
or tagged when it is not.

## Status

Version 0.1.0. It is tested on four backends: `fs`; `s3` against
[RustFS](https://github.com/rustfs/rustfs) 1.0.2 as the upstream; `azblob` against
[Azurite](https://github.com/Azure/Azurite) 3.37.0; and `gcs` against
[fake-gcs-server](https://github.com/fsouza/fake-gcs-server) 1.56.1. The last two are
emulators, not Azure and Google Cloud themselves (see [Compatibility](#compatibility)). Other
OpenDAL services are not compiled in.

Implemented operations:

- Buckets: CreateBucket, HeadBucket, ListBuckets, DeleteBucket, GetBucketLocation,
  GetBucketVersioning (always unversioned), GetBucketAcl and PutBucketAcl (owner only). The
  GETs of configurations s3dal does not have (policy, policy status, CORS, lifecycle, tagging,
  encryption, object lock, website, replication, ownership controls, public access block,
  logging, notifications, transfer acceleration, request payment) answer as S3 does for a
  bucket that has none, so tools that read them all, such as Terraform, see them unset.
- Objects: PutObject, GetObject (with Range and `partNumber`), HeadObject, DeleteObject,
  DeleteObjects, CopyObject, GetObjectAttributes, GetObjectTagging (always the empty tag
  set), GetObjectAcl and PutObjectAcl (owner only).
- Browser form uploads (PostObject) with a SigV4 POST policy.
- Listing: ListObjectsV2 and ListObjects (v1) with any delimiter, and ListObjectVersions
  (every object as its one version, `null`).
- Multipart: CreateMultipartUpload, UploadPart, UploadPartCopy, CompleteMultipartUpload
  (a retry gets the original result), AbortMultipartUpload, ListParts, ListMultipartUploads.
  On an S3 upstream the parts are copied server-side, and on gcs composed there, so their data
  does not pass through s3dal again.
- Checksums: `Content-MD5` and `x-amz-checksum-*`, including aws-chunked trailers, are
  verified on upload. Multipart uploads get COMPOSITE or FULL_OBJECT checksums derived from
  their parts.
- Conditional requests: `If-Match`, `If-None-Match`, `If-Modified-Since` and
  `If-Unmodified-Since` on GET and HEAD; `If-Match` and `If-None-Match: *` on PUT, copy, and
  CompleteMultipartUpload; `If-Match`, size and last-modified-time conditions on DELETE and
  on each DeleteObjects entry.
- Presigned URLs, several key pairs, HTTPS, and a request ID on every response.

Every other operation answers `NotImplemented`.

## Quick start

### Build from source

s3dal needs Rust 1.96 or later.

```sh
cargo install --locked s3dal   # the released version from crates.io, into ~/.cargo/bin
cargo build --release          # target/release/s3dal
cargo install --path .         # or install it into ~/.cargo/bin
```

The features `services-s3`, `services-azblob` and `services-gcs`, all on by default, add the
`s3`, `azblob` and `gcs` backends and the HTTP client they need.
`cargo build --release --no-default-features` builds an fs-only binary, and
`--no-default-features --features services-s3` one with fs and s3 only.

### Serve a local directory

```sh
export S3DAL_ACCESS_KEY=my-access-key
export S3DAL_SECRET_KEY=my-secret-key
s3dal serve --backend fs --backend-opt root=/srv/s3dal
```

s3dal listens on `127.0.0.1:9000` by default (`--address 0.0.0.0:9000` to accept remote
clients). Buckets become directories under `/srv/s3dal`, and s3dal keeps its own data
(multipart uploads in progress, temporary files) under `/srv/s3dal/.s3dal/`.

The filesystem must support user extended attributes (`user.*` xattrs). ext4, XFS, btrfs and
APFS do; tmpfs does from Linux 6.6. At startup s3dal writes a probe object and checks that its
metadata comes back; if it does not, s3dal exits with an error instead of serving objects
without their metadata.

### Serve an S3-compatible upstream

```sh
export S3DAL_ACCESS_KEY=my-access-key
export S3DAL_SECRET_KEY=my-secret-key
export S3DAL_BACKEND=s3
export S3DAL_BACKEND_OPTS=bucket=upstream-bucket,endpoint=https://s3.example.com,region=us-east-1,root=/s3dal
s3dal serve
```

All of s3dal's buckets live inside the one upstream bucket: object `k` in bucket `b` is stored
at `<root>/b/k` upstream, and s3dal's own data under `<root>/.s3dal/`. The upstream
credentials come from the standard AWS sources (`AWS_ACCESS_KEY_ID`, `~/.aws/credentials`,
instance metadata, ...) unless given as backend options; see
[Backend options](#backend-options).

### Serve Azure Blob Storage or Google Cloud Storage

```sh
export S3DAL_BACKEND=azblob
export S3DAL_BACKEND_OPTS=container=upstream,endpoint=https://myaccount.blob.core.windows.net,root=/s3dal
# or
export S3DAL_BACKEND=gcs
export S3DAL_BACKEND_OPTS=bucket=upstream-bucket,root=/s3dal
```

As with s3, everything lives inside the one container or bucket, under `root`. Credentials
come from the options or from the standard sources of each cloud: for Azure the
`AZURE_STORAGE_ACCOUNT_KEY` and `AZURE_STORAGE_SAS_TOKEN` variables, workload identity, or
managed identity; for Google Cloud `GOOGLE_APPLICATION_CREDENTIALS` or the VM's service
account.

### Docker

The Dockerfile builds a minimal image that runs as a non-root user (uid 65532) and serves the
fs backend from the volume `/data` on port 9000. The image contains no credentials; pass them
with `-e`.

```sh
docker build -t s3dal .
docker volume create s3dal-data
docker run -d --name s3dal -p 9000:9000 \
    -e S3DAL_ACCESS_KEY=my-access-key -e S3DAL_SECRET_KEY=my-secret-key \
    -v s3dal-data:/data s3dal
```

- `/data` is declared a volume, so objects never land in the container's own writable layer,
  which is removed with the container. Without `-v`, Docker creates an anonymous volume.
- The volume's filesystem must support user xattrs, as above. A named volume lives on the
  Docker host's filesystem under `/var/lib/docker/volumes` (inside the Linux VM on Docker
  Desktop), usually ext4 or XFS, which support them. For a bind mount (`-v /srv/s3dal:/data`),
  the host directory's filesystem decides, and the directory must be writable by uid 65532
  (`chown 65532:65532 /srv/s3dal`, or run with `--user` set to its owner). A tmpfs mount
  works from Linux 6.6. Network filesystems often lack user xattrs; NFS has them only from
  version 4.2 with Linux 5.9 on both ends.
- The image's defaults (fs on `/data`, address `0.0.0.0:9000`) are in the config file
  `/etc/s3dal/s3dal.toml`, so any `-e S3DAL_*` overrides them. To serve an S3 upstream
  instead: `-e S3DAL_BACKEND=s3 -e S3DAL_BACKEND_OPTS=bucket=...,endpoint=...`.
- To configure it with a file of your own, for several key pairs or HTTPS, mount it over the
  default, together with any certificate it names:
  `-v ./s3dal.toml:/etc/s3dal/s3dal.toml:ro -v ./tls:/etc/s3dal/tls:ro`. The file replaces
  the defaults whole, so it sets `backend`, `address` and `backend-opt` itself. The files must
  be readable by uid 65532.
- On `docker stop`, s3dal finishes in-flight requests for up to 10 seconds before it exits. Docker's own stop timeout is also 10 seconds; give it a margin
  with `--stop-timeout 15`.

## Configuration

`s3dal serve` takes these options, as flags, as environment variables, or from a config file.
A flag wins over its variable, and a variable over the file. Prefer the variables or the file
for secrets, which then stay out of shell history and process listings.

| Flag | Environment variable | Default | Meaning |
|---|---|---|---|
| `--config` | `S3DAL_CONFIG` | | TOML config file; see [Config file](#config-file) |
| `--backend` | `S3DAL_BACKEND` | (required) | OpenDAL service: `fs`, or `s3`, `azblob`, `gcs` with the feature of that name |
| `--backend-opt key=value` | `S3DAL_BACKEND_OPTS` | | A backend option; repeatable, or comma-separated, so a value cannot contain a comma (in the config file it can) |
| `--address` | `S3DAL_ADDRESS` | `127.0.0.1:9000` | Address and port to listen on |
| `--access-key` | `S3DAL_ACCESS_KEY` | (required) | Access key ID clients sign with; together with `--secret-key`, it replaces the config file's key pairs |
| `--secret-key` | `S3DAL_SECRET_KEY` | (required) | Secret access key |
| `--region` | `S3DAL_REGION` | `us-east-1` | Region s3dal reports for every bucket (GetBucketLocation, HeadBucket, ListBuckets) |
| `--domain` | `S3DAL_DOMAIN` | | Base domain for virtual-hosted-style requests (`<bucket>.<domain>`); repeatable, or comma-separated |
| `--etag-buffer-limit` | `S3DAL_ETAG_BUFFER_LIMIT` | `8MiB` | Largest single PUT body held in memory to compute an MD5 ETag on backends without native ETags (see [ETags](#etags)). Accepts `B`, `KiB`, `MiB`, `GiB` |
| `--stale-upload-age` | `S3DAL_STALE_UPLOAD_AGE` | `24h` | Multipart uploads neither completed nor aborted within this time are removed, as are, on fs, the temporary files of writes a crash or shutdown cut off; checked at startup and then hourly (more often for a shorter age). Accepts `s`, `m`, `h`, `d`; `0` keeps them forever |
| `--tls-cert` | `S3DAL_TLS_CERT` | | PEM certificate chain; with `--tls-key`, s3dal serves HTTPS only (HTTP/2 and HTTP/1.1) |
| `--tls-key` | `S3DAL_TLS_KEY` | | PEM private key of the certificate |

There is no anonymous mode: the server does not start without a key pair, and every request
must be signed (SigV4, in headers, as a presigned URL, or as a POST policy). All key pairs
reach the same buckets; there is one tenant. The certificate and key are read once at startup,
so replacing them takes a restart.

### Config file

The keys of the file are the long flag names. `backend-opt` is a table, and key pairs are
`[[credentials]]` tables:

```toml
backend = "s3"
address = "0.0.0.0:9000"
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
```

A list setting (`domain`, `backend-opt`, the key pairs) given by flag or variable replaces the
file's list whole rather than adding to it. An unknown key is an error, so a misspelling is
caught at startup. Relative paths are relative to the working directory.

Logging goes to stderr and is controlled by `RUST_LOG` (default `info`). The startup log shows
the backend profile, which tells which metadata the backend stores natively.
`RUST_LOG=info,s3s=debug` logs every request's operation and any error it got. Every response
carries an `x-amz-request-id`, error documents carry it as `<RequestId>`, and the log lines
written while serving a request carry the same id, so a client's error can be found in the
log.

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

### Backend options

Backend options are handed to OpenDAL's service configuration unchanged: each `key=value` sets
the field of that name in the service's config struct, [`FsConfig`][fs-config],
[`S3Config`][s3-config], [`AzblobConfig`][azblob-config] or [`GcsConfig`][gcs-config]
(OpenDAL 0.59.4). OpenDAL ignores keys it does not know, so a misspelled option has no effect
rather than causing an error.

`fs`:

| Option | Meaning |
|---|---|
| `root` | Directory that holds the buckets; required |
| `atomic_write_dir` | Where uploads are written before being renamed into place. s3dal sets it to `<root>/.s3dal/tmp`, so that partial uploads are never visible, and removes the files there last written longer ago than `--stale-upload-age`; if you set it, keep it on the same filesystem as `root`, and know that s3dal sweeps only its own directory |

`s3` (the most common options; every `S3Config` field is accepted):

| Option | Meaning |
|---|---|
| `bucket` | Upstream bucket; required |
| `endpoint` | Upstream URL, for example `https://s3.eu-west-1.amazonaws.com` or `http://127.0.0.1:9000`; defaults to `https://s3.amazonaws.com` |
| `region` | Signing region of the upstream |
| `root` | Prefix inside the upstream bucket under which s3dal stores everything; defaults to `/` |
| `access_key_id`, `secret_access_key`, `session_token` | Upstream credentials. Without them, OpenDAL reads the AWS environment variables, profile files, and instance metadata |
| `disable_config_load` | `true` to use only the options given here, never the AWS environment or profile files |
| `enable_virtual_host_style` | `true` to address the upstream bucket virtual-hosted style; path style by default |

`azblob` (the most common options; every `AzblobConfig` field is accepted):

| Option | Meaning |
|---|---|
| `container` | Upstream container; required |
| `endpoint` | Blob service URL, for example `https://myaccount.blob.core.windows.net`, or `http://127.0.0.1:10000/devstoreaccount1` for Azurite; required |
| `root` | Prefix inside the container under which s3dal stores everything; defaults to `/` |
| `account_name`, `account_key` | Shared Key credentials. The account name defaults to the one in `endpoint` |
| `sas_token` | A SAS token, instead of the account key |

`gcs` (the most common options; every `GcsConfig` field is accepted):

| Option | Meaning |
|---|---|
| `bucket` | Upstream bucket; required |
| `endpoint` | Defaults to `https://storage.googleapis.com` |
| `root` | Prefix inside the bucket under which s3dal stores everything; defaults to `/` |
| `credential_path`, `credential` | A service account key file, or its content base64-encoded. Without them, OpenDAL reads `GOOGLE_APPLICATION_CREDENTIALS` and the VM metadata server |
| `disable_config_load`, `disable_vm_metadata` | `true` to skip `GOOGLE_APPLICATION_CREDENTIALS` or the VM metadata server |

Options are never logged, since they may carry credentials.

On s3, azblob and gcs, a backend call that fails transiently (throttling, a 5xx answer, a
dropped connection) is retried up to three times, after about 1, 2 and 4 seconds, before the
request fails. Each retry is logged as a warning. fs calls are not retried.

[fs-config]: https://docs.rs/opendal-service-fs/0.59.4/opendal_service_fs/struct.FsConfig.html
[s3-config]: https://docs.rs/opendal-service-s3/0.59.4/opendal_service_s3/struct.S3Config.html
[azblob-config]: https://docs.rs/opendal-service-azblob/0.59.4/opendal_service_azblob/struct.AzblobConfig.html
[gcs-config]: https://docs.rs/opendal-service-gcs/0.59.4/opendal_service_gcs/struct.GcsConfig.html

## Using s3dal from clients

- **Addressing.** Path-style requests (`http://host:9000/bucket/key`) always work, so configure
  clients for path style. Virtual-hosted style (`http://bucket.host:9000/key`) works only with
  `--domain host`, and only if `<bucket>.host` resolves to s3dal.
- **Region.** Configure clients with the region s3dal runs with (`us-east-1` by default).

aws CLI (`~/.aws/config`):

```ini
[profile s3dal]
endpoint_url = http://127.0.0.1:9000
region = us-east-1
s3 =
    addressing_style = path
```

```sh
export AWS_PROFILE=s3dal AWS_ACCESS_KEY_ID=my-access-key AWS_SECRET_ACCESS_KEY=my-secret-key
aws s3 mb s3://photos
aws s3 cp ./holiday.jpg s3://photos/2026/holiday.jpg
aws s3 ls s3://photos/ --recursive
```

rclone (`rclone.conf`):

```ini
[s3dal]
type = s3
provider = Other
endpoint = http://127.0.0.1:9000
region = us-east-1
force_path_style = true
access_key_id = my-access-key
secret_access_key = my-secret-key
```

SDKs: set the endpoint, the region, and path style. With the Rust SDK:

```rust
let config = aws_sdk_s3::Config::builder()
    .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
    .endpoint_url("http://127.0.0.1:9000")
    .region(aws_sdk_s3::config::Region::new("us-east-1"))
    .credentials_provider(aws_sdk_s3::config::Credentials::new(
        "my-access-key", "my-secret-key", None, None, "static",
    ))
    .force_path_style(true)
    .build();
let client = aws_sdk_s3::Client::from_conf(config);
```

With boto3, pass `config=botocore.config.Config(s3={"addressing_style": "path"})` to
`boto3.client("s3", endpoint_url=..., region_name=...)`.

## Compatibility

s3dal is tested with:

- the integration tests (`cargo test`), which drive an s3dal server through the AWS Rust SDK,
  on fs and, each through a script that starts a throwaway container, on s3 against RustFS
  1.0.2, on azblob against Azurite 3.37.0, and on gcs against fake-gcs-server 1.56.1;
- smoke tests with the aws CLI 2.38.1, rclone 1.75.0, and the s3s end-to-end suite, on all
  four.

The [ceph/s3-tests](https://github.com/ceph/s3-tests) suite measures how much of the whole S3
API s3dal covers. The full results, per test and per category, are in
[compat/s3-tests.md](compat/s3-tests.md):

| Backend | Passed / total | Pass rate |
|---|--:|--:|
| fs | 281 / 1042 | 26.9% |
| s3 on RustFS | 283 / 1042 | 27.1% |
| azblob on Azurite | 283 / 1042 | 27.1% |
| gcs on fake-gcs-server | 283 / 1042 | 27.1% |

What the rate counts: every test in the suite, including the 158 the suite itself skips and
the tests of features s3dal does not have (ACLs, IAM, STS, versioning, SSE, object lock,
lifecycle, logging, ...), which make up most of the failures. It is a measure of API coverage,
not of how correct the implemented operations are; the per-category table in the report shows
the latter better (range reads 100%, objects 65.7%, listing 89.1% on fs and 91.5% on the
others). s3, azblob and gcs pass and fail the same tests; fs differs from them only in the v1
and v2 forms of one listing test, which stores a key that is also a prefix (see below).

Not tested as upstreams: AWS S3, Azure Blob Storage and Google Cloud Storage themselves, and
S3-compatible services other than RustFS. The emulators differ from the services where it
matters to s3dal:

- fake-gcs-server lacks the XML API multipart upload that OpenDAL uses for every gcs write
  larger than 5 MiB, and cannot parse the batch requests OpenDAL deletes with.
  `scripts/with-fake-gcs.sh` puts `scripts/fake-gcs-proxy.py` in front of it to fill in both,
  so the tests exercise what GCS would receive.
- fake-gcs-server answers a ranged read of gzip content with the content as stored, where GCS
  serves it decompressed, so the probe finds Content-Encoding native there and would not on
  GCS.

## Differences from Amazon S3

### Keys and folders

- A key must be non-empty UTF-8 of at most 1024 bytes, and must not start with `/`, contain an
  empty segment (`a//b`) or a `.` or `..` segment, end in whitespace, or contain a control
  character other than tab, line feed and carriage return. Other keys are refused with
  `InvalidArgument`: OpenDAL would store them under another key (it trims whitespace off the
  ends of a path, so `k ` would overwrite `k`), they could leave the bucket, or XML could not
  carry them. Files that another program stored under such a name are not listed, and a
  listing whose prefix, delimiter or marker holds such a control character gets
  `InvalidArgument` unless it asks for `encoding-type=url`.
- On fs, each segment of a key is a file name, which most filesystems limit to 255 bytes; the
  last segment can have 246, since a write goes through a temporary file named after it. The
  whole path, the root's included, is limited too: to 1024 bytes on macOS, so a key near S3's
  1024 does not fit there (Linux takes 4096). A longer one gets `KeyTooLongError` on write and
  `NoSuchKey` on read.
- On fs, a key cannot be both an object and a prefix: with `a/b` stored, writing `a` fails, and
  the other way round. The request gets `InvalidRequest` with a message naming the conflict.
  On the other backends both can exist.
- An empty object whose key ends in `/` (a "folder marker", as consoles create) becomes a
  directory. It is not listed as a key, and reading it gives `NoSuchKey`. Such a key with a
  non-empty body is refused.
- Listings take any delimiter, but on a backend without ordered listing (fs, azblob) a
  delimiter other than `/` may have to read every key under the prefix for one page.
- On azblob, which OpenDAL cannot list from a given key, every listing page reads in full the
  directory its prefix names and each directory it descends into, 5,000 entries per request:
  one page in a folder of 100,000 objects costs 20 requests to Azure.
- On azblob and gcs, as on fs, the ETag of a listed object is in its metadata, which a listing
  does not return, so a page also reads the metadata of every object it lists: up to 1,000
  requests, 16 at a time.

### ETags

On an s3 upstream whose ETag of a single PUT is the MD5 of the content, as on AWS and RustFS,
the ETag is the upstream's; the startup probe checks this. A single PUT or copy larger than
`--etag-buffer-limit` (at least 5 MiB) reaches the upstream as a multipart upload, so its ETag
has the multipart form (`…-N`) rather than being the MD5 of the content.

On every other backend, fs, azblob and gcs included, the backend's ETag is no content MD5 (fs
has none; Azure's and Google's are opaque version tags), so s3dal stores one with the object:

- the MD5 of the content, when it is known before the write: the client sent `Content-MD5`, or
  the body is at most `--etag-buffer-limit` and is hashed in memory first;
- for a multipart upload, the usual `<md5 of the part MD5s>-<part count>`;
- otherwise (a large PUT without `Content-MD5`, or an object written to the backend by another
  program) a synthesized value derived from the size and modification time, which always ends
  in `-0`. It is not a content hash, and changes when the object changes. The `-` keeps
  clients such as rclone from mistaking it for an MD5 and reporting corruption.

### Metadata as other tools see it

S3 clients get the same headers and metadata back on every backend. A tool that reads the
backend directly sees how s3dal stored them:

- What the backend has no place for is kept in the object's user metadata, under names
  starting with `s3dal-`: checksums, Content-Language and Expires on every backend, the ETag
  where the backend's is no content MD5, and each standard header the backend cannot store and
  return as sent. The startup probe measures which those are, and the startup log shows the
  result as the backend profile: on fs all of them; on azblob Content-Disposition and
  Content-Encoding, which OpenDAL cannot set there; on gcs Content-Disposition, and
  Content-Encoding as well where gzip content is served decompressed, as GCS does.
- Azure accepts only C# identifiers as metadata names, so on azblob every name is stored
  encoded: lowercase letters, and digits but a leading one, stay; every other byte becomes `_`
  and two hex digits. `x-amz-meta-project-id` is stored as `project_2did`, and `s3dal-etag` as
  `s3dal_2detag`.
- A metadata value that is not printable ASCII is stored RFC 2047-encoded, as S3 carries it in
  a header.

### Other differences

- There is no versioning: `versionId=null` is accepted, any other version ID gets
  `NoSuchVersion`, and ListObjectVersions lists every object as version `null`.
- ACLs: every bucket and object has one owner, `s3dal`, with full control, and nothing else
  can be granted. The canned ACLs `private`, `bucket-owner-read` and
  `bucket-owner-full-control` are accepted on objects, `private` on buckets; any ACL that grants
  more, on create or through PutBucketAcl and PutObjectAcl, is refused.
- Browser form uploads (POST) must be signed with SigV4; SigV2 forms are refused. The form
  fields `acl` and `tagging` are ignored (s3s reads only `x-amz-acl` and `x-amz-tagging`), so
  `acl=public-read` stores a private object. A failed policy condition gets
  `400 InvalidPolicyDocument` rather than `403 AccessDenied`. A form must come with a
  `Content-Length`, which browsers and curl always send; one without gets
  `411 MissingContentLength`, as a PUT without one does.
- A size or time condition on DELETE is accepted on every bucket; AWS takes them only on
  directory buckets.
- Conditional writes (`If-Match`, `If-None-Match: *`) are checked under a lock inside the s3dal
  process. Where the backend checks write conditions itself, s3dal hands it the condition as
  well, so it also holds against other s3dal processes and other programs writing to the same
  root: the startup probe measures this, and the startup log shows it as `conditional_writes`
  in the backend profile (on s3, azblob and gcs; not on fs). The backend is handed the object
  as it was when the write began, so if another writer replaces it while the body is still
  arriving, the write fails with `412` even when the new object would meet the condition too;
  S3 judges the condition when the upload ends. And as with S3 itself, a write retried after
  its response was lost can get `412` for its own earlier success.
- Conditions on DELETE are checked inside the s3dal process only, on every backend: S3 takes
  `If-Match` on DeleteObject only on directory buckets, so no S3 upstream could be handed one.
- A `Range` header with several ranges, or a malformed one, gets `400 InvalidArgument`; S3
  ignores it and returns the whole object.
- HEAD ignores `Range`, and answers `partNumber` with `200` rather than `206`.
- Multipart uploads neither completed nor aborted within `--stale-upload-age` (24 hours by
  default) are removed; AWS keeps them until a lifecycle rule removes them. GET or HEAD of one
  part (`partNumber`) of a multipart object returns no checksum.
- A single PUT or copy is not limited to 5 GiB.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache License 2.0; see [LICENSE](LICENSE). s3dal includes code adapted from s3s, listed in
[NOTICE](NOTICE). The Docker image carries the license texts of every crate compiled into the
binary in `/usr/share/doc/s3dal/THIRD-PARTY-LICENSES`.

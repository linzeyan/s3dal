# S3 API coverage and compatibility

## Implemented operations

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

Every other operation answers `NotImplemented`. Where s3dal answers differently from S3, it is
listed in [Differences from Amazon S3](differences.md).

## What it is tested with

- the integration tests (`cargo test`), which drive an s3dal server through the AWS Rust SDK,
  on fs and, each through a script that starts a throwaway container, on s3 against
  [RustFS](https://github.com/rustfs/rustfs) 1.0.2, on azblob against
  [Azurite](https://github.com/Azure/Azurite) 3.37.0, and on gcs against
  [fake-gcs-server](https://github.com/fsouza/fake-gcs-server) 1.56.1;
- smoke tests with the aws CLI 2.38.1, rclone 1.75.0, and the s3s end-to-end suite, on all
  four.

The [ceph/s3-tests](https://github.com/ceph/s3-tests) suite measures how much of the whole S3
API s3dal covers. The full results, per test and per category, are in
[compat/s3-tests.md](../compat/s3-tests.md):

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
and v2 forms of one listing test, which stores a key that is also a prefix (see
[Keys and folders](differences.md#keys-and-folders)).

## Upstreams not tested

AWS S3, Azure Blob Storage and Google Cloud Storage themselves, and S3-compatible services
other than RustFS. The emulators differ from the services where it matters to s3dal:

- fake-gcs-server lacks the XML API multipart upload that OpenDAL uses for every gcs write
  larger than 5 MiB, and cannot parse the batch requests OpenDAL deletes with.
  `scripts/with-fake-gcs.sh` puts `scripts/fake-gcs-proxy.py` in front of it to fill in both,
  so the tests exercise what GCS would receive.
- fake-gcs-server answers a ranged read of gzip content with the content as stored, where GCS
  serves it decompressed, so the probe finds Content-Encoding native there and would not on
  GCS.

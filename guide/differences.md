# Differences from Amazon S3

## A gateway, not a storage system

s3dal is a gateway. A bucket is a top-level directory of the backend, and an object is a file
(or an object) under it. Object metadata that the backend cannot store natively, such as the
ETag and Content-Type on a filesystem, is kept in the backend's user metadata (extended
attributes on fs), written together with the object. There are no sidecar files and no local
database.

It is not a storage system. The backend owns durability: there is no erasure coding,
replication, or healing. It also has none of these:

- users, IAM, bucket policies, or ACLs that grant access: a key pair reaches every bucket, or
  the buckets and folders its `allow` lists, read-only, write-only or read-write, until its
  optional expiry (see [Key pairs](../README.md#key-pairs)), and is refused with `AccessDenied`
  elsewhere. An expired pair gets `InvalidAccessKeyId`, as an unknown one does;
- object lock, server-side encryption, object tags;
- lifecycle filters on tags, and lifecycle transitions (`Transition`,
  `NoncurrentVersionTransition`) on a server without [cold tiers](backends.md#cold-tiers). s3dal
  applies lifecycle expiration rules (see [Other differences](#other-differences)), and
  transitions into its tiers, not these.

A request that relies on one of these features is refused with `NotImplemented` rather than
answered as if it had been applied, so a client never believes an object is encrypted, locked,
tagged, or moved to another storage class when it is not. A lifecycle configuration with any
rule that has a tag filter, or a transition on a server without tiers, is refused whole, and
nothing of it is stored; with tiers, a transition to a class no tier has is refused the same
way, with `InvalidStorageClass`.

## Keys and folders

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
  non-empty body is refused. On fs, an empty directory is not listed as a common prefix
  either, so a folder shows in listings only once it holds an object.
- Listings take any delimiter, but on a backend without ordered listing (fs, azblob) a
  delimiter other than `/` may have to read every key under the prefix for one page.
- On azblob, which OpenDAL cannot list from a given key, every listing page reads in full the
  directory its prefix names and each directory it descends into, 5,000 entries per request:
  one page in a folder of 100,000 objects costs 20 requests to Azure.
- On azblob and gcs, as on fs, the ETag of a listed object is in its metadata, which a listing
  does not return, so a page also reads the metadata of every object it lists: up to 1,000
  requests, 16 at a time.

## ETags

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

## Metadata as other tools see it

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
- A cold object, one written to a [cold tier](backends.md#cold-tiers), is an empty file (or
  object) in the primary backend: another tool reading the primary sees zero bytes, and its
  bytes are in the tier, at the place its `s3dal-tier` and `s3dal-tier-id` metadata names. A
  backup of the primary alone holds none of them (see
  [Cold tiers](operations.md#cold-tiers)). On an S3 primary the stub's metadata takes about 140
  bytes of the upstream's limit on user metadata (2 KB on AWS), so an object whose own metadata
  nearly fills it cannot be written to a tier there; the write gets the upstream's error.

## Other differences

- Versioning: a bucket that never had it keeps one version per key, `null`, and any other
  version ID gets `NoSuchVersion`. Once enabled, replaced versions and delete markers are kept
  under `.s3dal/versions/` in the backend, where other tools see them as ordinary files. On S3,
  Azure Blob and GCS backends, a version that becomes current again because the newer one was
  deleted reports the time of that change as its Last-Modified. MFA delete is refused. A write
  that collides with one through another
  s3dal instance sharing the backend may answer `503 SlowDown`, which SDKs retry.
- Bucket settings S3 keeps per bucket, versioning and whether the bucket is public (an s3dal
  setting, not an ACL or a policy), are kept in `.s3dal/buckets/<bucket>.json` in the backend.
  Each instance reads a bucket's file at most every 5 seconds, so a change made through another
  instance sharing the backend applies here within that time. Two instances changing settings
  of one bucket at once keep both changes where the backend checks write conditions; elsewhere
  (fs, which one process serves) the process keeps them apart.
- Bucket quotas are an s3dal setting; S3 has none. A write that would take a bucket past its
  [quota](../README.md#bucket-quotas) gets `403 QuotaExceeded`, a code S3 never sends; clients
  treat it as any other access error and do not retry it. In a bucket with a quota, a PUT or form upload without a
  `Content-Length` gets `411 MissingContentLength`, as its size must be known before its body is
  read. Writes through other instances sharing the backend count from the next usage count, so
  such a bucket can go past its quota for a while (see
  [Bucket usage](operations.md#bucket-usage)).
- Lifecycle rules: Expiration (`Days`, `Date`, `ExpiredObjectDeleteMarker`),
  NoncurrentVersionExpiration (`NoncurrentDays`, `NewerNoncurrentVersions`) and
  AbortIncompleteMultipartUpload, filtered by prefix and object size. Times are AWS's: a rule of
  N days applies from the first midnight UTC after creation plus N days; NoncurrentDays count
  from when a version was replaced or deleted. One instance of those sharing the backend (the
  `lifecycle` [background job](operations.md#background-jobs)) applies each bucket's rules
  once a day, soon after midnight UTC, and again within the hour of a change to them, so an
  object goes during the day its rule names rather than at midnight exactly; AWS also takes up
  to a day or two. Each pass lists the whole bucket. `x-amz-expiration` is sent on PUT, GET and
  HEAD of a current version, not on CopyObject or CompleteMultipartUpload.
- Event notifications go to webhooks the operator configures (`[[notify]]`), never to SQS, SNS
  or Lambda: a configuration's ARN must be `arn:s3dal:sqs:<region>:<id>:webhook`, in any of the
  queue, topic and Lambda lists, and an ARN naming no configured target is refused with
  `InvalidArgument`, as AWS refuses a destination it cannot validate
  (`x-amz-skip-destination-validation` does not change that). EventBridge, and the events s3dal
  never sends (restore, replication, tagging, ACL, Intelligent-Tiering, reduced redundancy),
  are refused with `NotImplemented`. Overlapping rules are not refused:
  each one sends. There is no `s3:TestEvent` message. Only changes made through s3dal are
  announced: objects written or removed by other tools on the backend, or by an instance of an
  older s3dal sharing it, produce no events. Delivery is at most once per rule and may lose
  events: on overflow of a target's queue, after its `retry-for`, on a 4xx answer, and on a
  crash or restart, which loses what was queued (see
  [Event notifications](operations.md#event-notifications)). In the message,
  `userIdentity.principalId` is the access key that signed the request, and `s3dal` for a
  lifecycle expiry or transition, whose `sourceIPAddress` is `s3dal` too; `responseElements` has no
  `x-amz-id-2`; the `sequencer` orders the events of one s3dal process, not across instances.
- Thumbnails are s3dal's own: a GET with `x-s3dal-thumbnail=<size>` answers with a scaled
  image instead of the object, under the object's ETag (see
  [Thumbnails](operations.md#thumbnails)); S3 has no such parameter. Thumbnails are kept under
  `.s3dal/thumbs/` in the backend, where other tools see them as ordinary files.
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
  default) are removed, unless a lifecycle `AbortIncompleteMultipartUpload` rule covers them,
  which then decides alone, sooner or later; AWS keeps uploads no rule covers. GET or HEAD of one
  part (`partNumber`) of a multipart object returns no checksum.
- A single PUT or copy is not limited to 5 GiB.
- Storage classes: a class names a [cold tier](backends.md#cold-tiers) when one is configured
  for it, and is otherwise accepted and ignored, the object reported `STANDARD`. There is no
  RestoreObject and no `InvalidObjectState`: an object of any class reads at once. An empty
  object is never cold and reports `STANDARD`, whatever class its PUT named. A GET of a cold
  object whose key is overwritten while the GET starts reading can answer `503 SlowDown`, which
  SDKs retry.
- An unsigned request for an operation s3dal does not have, `HEAD /` (`curl -I`) among them,
  gets `501 NotImplemented` rather than `403`: s3s decides the operation before it checks the
  signature. To check that s3dal is up, ask its [health endpoint](operations.md#health-checks).

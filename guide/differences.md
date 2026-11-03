# Differences from Amazon S3

## A gateway, not a storage system

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

## Other differences

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
- An unsigned request for an operation s3dal does not have, `HEAD /` (`curl -I`) among them,
  gets `501 NotImplemented` rather than `403`: s3s decides the operation before it checks the
  signature. To check that s3dal is up, ask its [health endpoint](operations.md#health-checks).

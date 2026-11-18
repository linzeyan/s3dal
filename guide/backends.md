# Backends

s3dal stores buckets and objects through an [Apache OpenDAL](https://opendal.apache.org/)
service: `fs`, and with the default features `services-s3`, `services-azblob` and
`services-gcs`, `s3`, `azblob` and `gcs`. Other OpenDAL services are not compiled in.

## A local directory (`fs`)

```sh
export S3DAL_ACCESS_KEY=my-access-key
export S3DAL_SECRET_KEY=my-secret-key
s3dal serve --backend fs --backend-opt root=/srv/s3dal
```

Buckets become directories under `/srv/s3dal`, and s3dal keeps its own data (multipart uploads
in progress, temporary files, each bucket's settings) under `/srv/s3dal/.s3dal/`.

The filesystem must support user extended attributes (`user.*` xattrs). ext4, XFS, btrfs and
APFS do; tmpfs does from Linux 6.6. At startup s3dal writes a probe object and checks that its
metadata comes back; if it does not, s3dal exits with an error instead of serving objects
without their metadata.

## An S3-compatible upstream (`s3`)

```sh
export S3DAL_BACKEND=s3
export S3DAL_BACKEND_OPTS=bucket=upstream-bucket,endpoint=https://s3.example.com,region=us-east-1,root=/s3dal
s3dal serve
```

All of s3dal's buckets live inside the one upstream bucket: object `k` in bucket `b` is stored
at `<root>/b/k` upstream, and s3dal's own data under `<root>/.s3dal/`. The upstream
credentials come from the standard AWS sources (`AWS_ACCESS_KEY_ID`, `~/.aws/credentials`,
instance metadata, ...) unless given as backend options.

## Azure Blob Storage (`azblob`) or Google Cloud Storage (`gcs`)

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

## Backend options

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

## Cold tiers

A cold tier is a second backend that holds only the bytes of the objects written to it. The
primary backend keeps the whole namespace, so listings, HEAD and deletes never wait on the tier,
and the tier can be cheaper and slower storage: a large disk next to a fast one, or a cloud
bucket behind a local directory. Tiers are configured in the config file, one `[[tier]]` table
each:

```toml
[[tier]]
name = "hdd"                       # 1 to 32 of a-z, 0-9 and -, not starting with -
storage-class = "STANDARD_IA"      # the class a write names to go there
backend = "fs"                     # any backend this build has
[tier.backend-opt]
root = "/mnt/hdd/s3dal-tier"
```

- **Choosing the tier**: a PutObject, form upload, CopyObject or multipart upload whose
  `x-amz-storage-class` is a tier's class writes the object into that tier, and leaves at the
  key an empty object, the stub, whose metadata names where the bytes are. A multipart upload
  names the class at CreateMultipartUpload; its parts stay in the primary until Complete
  streams them into the tier, and ListParts and ListMultipartUploads report the class. Each tier
  has its own class: `STANDARD_IA`, `ONEZONE_IA`, `INTELLIGENT_TIERING`, `GLACIER_IR`, `GLACIER`
  or `DEEP_ARCHIVE` (`STANDARD` and `REDUCED_REDUNDANCY` name the primary). A class no tier has
  is accepted and ignored, as without tiers: the object is stored in the primary and reported
  `STANDARD`. An empty object has nothing to move and stays in the primary too, as does a PUT
  body sent without a `Content-Length`.
- **Changing an object's class**: a CopyObject of an object onto itself with another class
  moves it, keeping its bytes, headers and metadata, as on AWS:
  `aws s3 cp s3://b/k s3://b/k --storage-class STANDARD_IA` sends it to that class's tier, and
  `--storage-class STANDARD` brings it back to the primary and removes its bytes from the tier
  (a CopyObject names `STANDARD` when it names no class). Another tier's class moves it between
  tiers. In a versioned bucket the copy is a new version, as every copy is. A self-copy that
  names the class the object already has, without `x-amz-metadata-directive: REPLACE`, is
  refused with `InvalidRequest`, as one that changes nothing. An object written into a tier by a
  PUT or a copy has the MD5 of its content as its ETag, whatever its size, and one completed
  there the multipart ETag; a copy back to the primary follows the
  [ETag rules](differences.md#etags), so an MD5 ETag stays the same.
- **In the console**: the object browser marks objects of another class than `STANDARD`, an
  object's details show its class and "Change storage class" (the self-copy above), a menu
  beside Upload picks the class new uploads go to, the bucket page's lifecycle rules move
  objects, and the Server page lists the tiers with what the last `tier-sweep` counted in each.
  The Dashboard shades each bucket's cold bytes apart in its usage bar.
- **Lifecycle transitions**: a bucket's [lifecycle rules](../README.md#lifecycle-rules) can
  move objects into a tier after some days (`Transition` with `Days`) or from a date (`Date`),
  and noncurrent versions some days after they were replaced (`NoncurrentVersionTransition`,
  keeping the newest few in place with `NewerNoncurrentVersions`). The class must be a tier's:
  a rule naming one no tier has is refused with `InvalidStorageClass`, since its objects would
  never move. The `lifecycle` job moves each object in place, on the day AWS would: its ETag,
  Last-Modified, version id, headers and metadata stay, and only its bytes go. A cold object
  moves on to another tier when a later transition is due, and an object for which several are
  due goes straight to the last one. An expiration due the same day wins, so nothing is copied
  to be removed. An object a client overwrites while it moves keeps the client's version and is
  considered again on the next run; one whose key is too long for the tier's paths stays where
  it is (see the `s3dal_tier_transition_skipped_total` metric). Each move is an
  `s3:LifecycleTransition` [event](../README.md#event-notifications).
- **Reading**: GET, ranges, `partNumber`, conditional headers, copies (CopyObject,
  UploadPartCopy) and thumbnails read a cold object as a hot one, its bytes from the tier. GET,
  HEAD, GetObjectAttributes and listings report the tier's class. Nothing is restored first: a
  `GLACIER` or `DEEP_ARCHIVE` object reads at once, like any other. The aws CLI skips objects of
  those two classes on downloads unless given `--force-glacier-transfer`, so prefer
  `GLACIER_IR` or `STANDARD_IA` for a tier that is read through; startup warns about the others.
- **Deleting and overwriting**: in a bucket without versioning, a delete or an overwrite of a
  cold object removes its bytes from the tier after the key changed. In a versioned bucket the
  bytes stay with the version, read by its `versionId`, until that version is deleted.
- **Usage and quotas** count a cold object's size, wherever its bytes are. The usage count
  records the cold part as `cold_objects` and `cold_bytes` (see
  [Bucket usage](operations.md#bucket-usage)).
- **Startup**: each tier is reached and probed as the primary is, and a tier that cannot be
  reached stops startup. The first start makes the primary an id, `.s3dal/id`, and records it in
  each tier as `.s3dal/tier/owner.json`; a tier that another s3dal's primary owns stops startup,
  since two primaries sharing one tier would each take the other's objects for leftovers. A
  change to `[[tier]]` takes a restart (the log, and the console, say one is pending); the
  console's restart first checks that every tier answers and is still this primary's, so a new
  backend `root` that would not own the tiers is refused rather than left unstarted.
  `check-config` shows each tier, with the values of options named like secrets hidden.
- **Layout**: in the tier, the bytes of key `k` of bucket `b` are at
  `.s3dal/tier/b/<E(k)>!!/<id>`, `E` being the escape of the versions tree, one file per write,
  written once and never changed. The stub at `b/k` in the primary is an empty file (or object)
  whose metadata (xattrs on fs) holds `s3dal-tier` (the tier's name), `s3dal-tier-id` (the
  `<id>`), `s3dal-size`, and the ETag and Last-Modified clients see, beside the usual headers and
  user metadata. To find a key's bytes by hand, read those two values and look in that tier.

[fs-config]: https://docs.rs/opendal-service-fs/0.59.4/opendal_service_fs/struct.FsConfig.html
[s3-config]: https://docs.rs/opendal-service-s3/0.59.4/opendal_service_s3/struct.S3Config.html
[azblob-config]: https://docs.rs/opendal-service-azblob/0.59.4/opendal_service_azblob/struct.AzblobConfig.html
[gcs-config]: https://docs.rs/opendal-service-gcs/0.59.4/opendal_service_gcs/struct.GcsConfig.html

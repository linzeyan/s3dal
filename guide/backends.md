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
in progress, temporary files) under `/srv/s3dal/.s3dal/`.

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

[fs-config]: https://docs.rs/opendal-service-fs/0.59.4/opendal_service_fs/struct.FsConfig.html
[s3-config]: https://docs.rs/opendal-service-s3/0.59.4/opendal_service_s3/struct.S3Config.html
[azblob-config]: https://docs.rs/opendal-service-azblob/0.59.4/opendal_service_azblob/struct.AzblobConfig.html
[gcs-config]: https://docs.rs/opendal-service-gcs/0.59.4/opendal_service_gcs/struct.GcsConfig.html

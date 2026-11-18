# Using s3dal from clients

- **Addressing.** Path-style requests (`http://host:9000/bucket/key`) always work, so configure
  clients for path style. Virtual-hosted style (`http://bucket.host:9000/key`) works only with
  `--domain host`, and only if `<bucket>.host` resolves to s3dal.
- **Region.** Configure clients with the region s3dal runs with (`us-east-1` by default).

The `s3dal` binary is a client as well; see [the command-line client](../README.md#command-line-client).

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

With a write-only key pair (a drop box), rclone must not look at what it uploads or at the
destination, none of which such a pair may do: add `--s3-no-head` (no HEAD after an upload),
`--s3-no-check-bucket` (no bucket check or creation) and `--no-check-dest` (no listing of the
destination first), for example `rclone copy --s3-no-head --s3-no-check-bucket --no-check-dest
./outbox s3dal:inbox/`. `aws s3 cp` of a local file needs nothing extra; `aws s3 sync` lists
the destination and fails.

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

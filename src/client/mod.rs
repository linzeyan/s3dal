//! `s3dal ls`, `cp`, ...: an aws-CLI-like client for any S3 endpoint, an s3dal server first of
//! all, so a deployment can be checked and fed from the binary it already has.

mod api;
mod transfer;

use std::fmt;
use std::io::{self, Write};
use std::time::Duration;

use anyhow::{Context, bail};
use clap::{Args, Subcommand};
use futures::{StreamExt, TryStreamExt};
use http::Method;

use api::S3;
use transfer::{Location, PartSizes};

/// Deletes in flight for `rm --recursive` and `rb --force`.
const DELETES_IN_FLIGHT: usize = 16;

/// SigV4 caps a presigned URL's lifetime at a week.
const MAX_PRESIGN: Duration = Duration::from_secs(7 * 24 * 60 * 60);

#[derive(Debug, Subcommand)]
pub enum ClientCommand {
    /// List buckets, or the objects and prefixes under s3://bucket[/prefix].
    Ls {
        /// What to list; without it, the buckets.
        #[arg(value_parser = S3Url::parse)]
        target: Option<S3Url>,
        /// Every object under the prefix, rather than one level of it.
        #[arg(short, long)]
        recursive: bool,
        #[command(flatten)]
        conn: Connection,
    },
    /// Create a bucket.
    Mb {
        #[arg(value_parser = S3Url::parse)]
        bucket: S3Url,
        #[command(flatten)]
        conn: Connection,
    },
    /// Remove a bucket.
    Rb {
        #[arg(value_parser = S3Url::parse)]
        bucket: S3Url,
        /// Delete every object in the bucket first.
        #[arg(long)]
        force: bool,
        #[command(flatten)]
        conn: Connection,
    },
    /// Copy a file or an object to a local path or s3://bucket/key; `-` is stdin or stdout.
    Cp(CopyArgs),
    /// Copy, then delete the source.
    Mv(CopyArgs),
    /// Delete an object, or with --recursive every object under a prefix.
    Rm {
        #[arg(value_parser = S3Url::parse)]
        target: S3Url,
        #[arg(short, long)]
        recursive: bool,
        #[command(flatten)]
        conn: Connection,
    },
    /// Write an object to stdout.
    Cat {
        #[arg(value_parser = S3Url::parse)]
        object: S3Url,
        #[command(flatten)]
        conn: Connection,
    },
    /// Show an object's size, ETag, content type, modification time and user metadata.
    Stat {
        #[arg(value_parser = S3Url::parse)]
        object: S3Url,
        #[command(flatten)]
        conn: Connection,
    },
    /// Print a URL that grants access to an object without credentials, until it expires.
    Presign {
        #[arg(value_parser = S3Url::parse)]
        object: S3Url,
        /// Lifetime: seconds, or a number with an `s` / `m` / `h` / `d` suffix; at most 7d.
        #[arg(long, default_value = "1h", value_parser = parse_expiry)]
        expires: Duration,
        /// A URL to upload the object with, instead of to download it.
        #[arg(long)]
        put: bool,
        #[command(flatten)]
        conn: Connection,
    },
}

#[derive(Debug, Args)]
pub struct CopyArgs {
    #[arg(value_parser = Location::parse)]
    source: Location,
    #[arg(value_parser = Location::parse)]
    destination: Location,
    /// Copy a directory or every object under a prefix.
    #[arg(short, long)]
    recursive: bool,
    // Hidden: they exist so tests reach multipart uploads with small files. S3 refuses parts
    // under 5 MiB but the last.
    /// Size from which uploads are multipart.
    #[arg(long, hide = true, default_value_t = 64 << 20)]
    multipart_threshold: u64,
    #[arg(long, hide = true, default_value_t = 16 << 20)]
    part_size: u64,
    #[command(flatten)]
    conn: Connection,
}

/// Where the server is and how to sign for it.
// The variables are the server's own, so `docker exec <container> s3dal ls` works with the
// container's environment as it is.
#[derive(Debug, Args)]
#[command(next_help_heading = "Connection")]
pub struct Connection {
    /// S3 endpoint, addressed path-style.
    #[arg(long, env = "S3DAL_ENDPOINT", default_value = "http://127.0.0.1:9000")]
    endpoint: String,

    #[arg(long, env = "S3DAL_ACCESS_KEY")]
    access_key: Option<String>,

    #[arg(long, env = "S3DAL_SECRET_KEY", hide_env_values = true)]
    secret_key: Option<String>,

    #[arg(long, env = "S3DAL_REGION", default_value = "us-east-1")]
    region: String,

    /// Accept any TLS certificate, as a local server's self-signed one.
    #[arg(long)]
    insecure: bool,
}

impl Connection {
    fn connect(self) -> anyhow::Result<S3> {
        let (Some(access_key), Some(secret_key)) = (self.access_key, self.secret_key) else {
            bail!(
                "no credentials: give --access-key and --secret-key, or set S3DAL_ACCESS_KEY and S3DAL_SECRET_KEY"
            );
        };
        S3::new(
            &self.endpoint,
            &self.region,
            access_key,
            secret_key,
            self.insecure,
        )
    }
}

/// `s3://bucket/key`; the key may be empty, or a prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Url {
    bucket: String,
    key: String,
}

impl S3Url {
    fn parse(s: &str) -> Result<Self, String> {
        let rest = s
            .strip_prefix("s3://")
            .ok_or_else(|| format!("expected s3://bucket[/key], found {s:?}"))?;
        let (bucket, key) = rest.split_once('/').unwrap_or((rest, ""));
        if bucket.is_empty() {
            return Err(format!("no bucket in {s:?}"));
        }
        Ok(Self {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
        })
    }

    fn with_key(&self, key: String) -> Self {
        Self {
            bucket: self.bucket.clone(),
            key,
        }
    }

    /// The key as a directory: everything under `a` is under `a/`, not `ab`. A prefix that is
    /// not a directory would let `rm -r s3://b/logs` delete `logs-old/` too.
    fn dir_prefix(&self) -> String {
        if self.key.is_empty() || self.key.ends_with('/') {
            self.key.clone()
        } else {
            format!("{}/", self.key)
        }
    }

    fn bucket_only(&self) -> anyhow::Result<&str> {
        if !self.key.is_empty() {
            bail!("expected s3://bucket, found {self}");
        }
        Ok(&self.bucket)
    }

    fn object(&self) -> anyhow::Result<(&str, &str)> {
        if self.key.is_empty() || self.key.ends_with('/') {
            bail!("{self} names no object");
        }
        Ok((&self.bucket, &self.key))
    }
}

impl fmt::Display for S3Url {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "s3://{}/{}", self.bucket, self.key)
    }
}

fn parse_expiry(s: &str) -> Result<Duration, String> {
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let n: u64 = num.parse().map_err(|_| format!("invalid duration {s:?}"))?;
    // A bare number is seconds, as for the aws CLI's `presign --expires-in`.
    let mult = match unit {
        "" | "s" => 1,
        "m" => 60,
        "h" => 60 * 60,
        "d" => 24 * 60 * 60,
        other => return Err(format!("unknown duration unit {other:?}; use s, m, h or d")),
    };
    let d = Duration::from_secs(n.saturating_mul(mult));
    if d.is_zero() || d > MAX_PRESIGN {
        return Err(format!("{s:?} is not between 1s and 7d"));
    }
    Ok(d)
}

/// Runs one client command on a runtime of its own.
pub fn run(cmd: ClientCommand) -> anyhow::Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("cannot start the async runtime")?;
    match rt.block_on(exec(cmd)) {
        // `s3dal cat ... | head` closes the pipe early on purpose; that is not a failure.
        Err(err)
            if err
                .chain()
                .filter_map(|e| e.downcast_ref::<io::Error>())
                .any(|e| e.kind() == io::ErrorKind::BrokenPipe) =>
        {
            Ok(())
        }
        result => result,
    }
}

async fn exec(cmd: ClientCommand) -> anyhow::Result<()> {
    match cmd {
        ClientCommand::Ls {
            target,
            recursive,
            conn,
        } => ls(&conn.connect()?, target, recursive).await,
        ClientCommand::Mb { bucket, conn } => {
            let name = bucket.bucket_only()?;
            conn.connect()?
                .create_bucket(name)
                .await
                .with_context(|| format!("cannot create bucket {name}"))?;
            say(format_args!("make_bucket: {name}"))
        }
        ClientCommand::Rb {
            bucket,
            force,
            conn,
        } => {
            let s3 = conn.connect()?;
            let name = bucket.bucket_only()?;
            if force {
                delete_all(&s3, &bucket).await?;
            }
            s3.delete_bucket(name)
                .await
                .with_context(|| format!("cannot remove bucket {name}"))?;
            say(format_args!("remove_bucket: {name}"))
        }
        ClientCommand::Cp(args) => copy(args, false).await,
        ClientCommand::Mv(args) => copy(args, true).await,
        ClientCommand::Rm {
            target,
            recursive,
            conn,
        } => {
            let s3 = conn.connect()?;
            if recursive {
                delete_all(&s3, &target).await
            } else {
                let (bucket, key) = target.object()?;
                s3.delete_object(bucket, key)
                    .await
                    .with_context(|| format!("cannot delete {target}"))?;
                say(format_args!("delete: {target}"))
            }
        }
        ClientCommand::Cat { object, conn } => {
            let s3 = conn.connect()?;
            transfer::download(&s3, &object, &Location::Stdio)
                .await
                .with_context(|| format!("cannot read {object}"))
        }
        ClientCommand::Stat { object, conn } => stat(&conn.connect()?, &object).await,
        ClientCommand::Presign {
            object,
            expires,
            put,
            conn,
        } => {
            let s3 = conn.connect()?;
            let (bucket, key) = object.object()?;
            let method = if put { Method::PUT } else { Method::GET };
            let url = s3.presign(method, bucket, key, expires).await?;
            say(format_args!("{url}"))
        }
    }
}

/// One line on stdout, with a write error returned rather than panicked on as `println!` does.
fn say(line: fmt::Arguments<'_>) -> anyhow::Result<()> {
    writeln!(io::stdout().lock(), "{line}")?;
    Ok(())
}

/// The aws CLI's `YYYY-MM-DD HH:MM:SS` from S3's ISO 8601 timestamps. Kept in UTC, which is
/// what the server sent, so the output does not depend on the local time zone.
fn date(iso: &str) -> String {
    iso.get(..19).unwrap_or(iso).replacen('T', " ", 1)
}

async fn ls(s3: &S3, target: Option<S3Url>, recursive: bool) -> anyhow::Result<()> {
    let mut out = io::stdout().lock();
    let Some(target) = target else {
        let buckets = s3.list_buckets().await.context("cannot list buckets")?;
        for b in buckets {
            writeln!(out, "{} {}", date(&b.creation_date), b.name)?;
        }
        return Ok(());
    };
    // As the aws CLI shows them: one level relative to the prefix's directory, or full keys.
    let base = match target.key.rfind('/') {
        Some(i) if !recursive => i + 1,
        _ => 0,
    };
    let delimiter = (!recursive).then_some("/");
    let mut token = None;
    loop {
        let (page, next) = s3
            .list_page(&target.bucket, &target.key, delimiter, token.as_deref())
            .await
            .with_context(|| format!("cannot list {target}"))?;
        for p in &page.common_prefixes {
            let name = p.prefix.get(base..).unwrap_or(&p.prefix);
            writeln!(out, "{:>30} {name}", "PRE")?;
        }
        for o in &page.contents {
            let name = o.key.get(base..).unwrap_or(&o.key);
            writeln!(out, "{} {:>10} {name}", date(&o.last_modified), o.size)?;
        }
        match next {
            Some(t) => token = Some(t),
            None => return Ok(()),
        }
    }
}

async fn delete_all(s3: &S3, target: &S3Url) -> anyhow::Result<()> {
    let prefix = target.dir_prefix();
    let objects = s3
        .list_all(&target.bucket, &prefix)
        .await
        .with_context(|| format!("cannot list {target}"))?;
    futures::stream::iter(objects)
        .map(|o| async move {
            let url = target.with_key(o.key);
            s3.delete_object(&url.bucket, &url.key)
                .await
                .with_context(|| format!("cannot delete {url}"))?;
            say(format_args!("delete: {url}"))
        })
        .buffer_unordered(DELETES_IN_FLIGHT)
        .try_collect::<()>()
        .await
}

async fn stat(s3: &S3, object: &S3Url) -> anyhow::Result<()> {
    let (bucket, key) = object.object()?;
    let headers = s3
        .head_object(bucket, key)
        .await
        .with_context(|| format!("cannot stat {object}"))?;
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
    };
    let mut out = io::stdout().lock();
    writeln!(out, "Name          : {object}")?;
    writeln!(out, "Size          : {}", header("content-length"))?;
    writeln!(out, "ETag          : {}", header("etag"))?;
    writeln!(out, "Content-Type  : {}", header("content-type"))?;
    writeln!(out, "Last-Modified : {}", header("last-modified"))?;
    let mut metadata: Vec<(&str, String)> = headers
        .iter()
        .filter_map(|(name, value)| {
            let name = name.as_str().strip_prefix("x-amz-meta-")?;
            let value = value.to_str().ok()?;
            // Non-ASCII values travel RFC 2047-encoded; shown as they were set.
            let value = s3s_rfc2047::decode(value).map_or_else(|_| value.into(), |v| v.into());
            Some((name, value))
        })
        .collect();
    if !metadata.is_empty() {
        metadata.sort();
        writeln!(out, "Metadata      :")?;
        for (name, value) in metadata {
            writeln!(out, "  {name}: {value}")?;
        }
    }
    Ok(())
}

async fn copy(args: CopyArgs, remove_source: bool) -> anyhow::Result<()> {
    let s3 = args.conn.connect()?;
    let sizes = PartSizes {
        threshold: args.multipart_threshold,
        part: args.part_size,
    };
    let job = transfer::Job {
        s3: &s3,
        sizes,
        remove_source,
    };
    if args.recursive {
        job.copy_tree(args.source, args.destination).await
    } else {
        job.copy_one_to(args.source, args.destination).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_urls_split_at_the_first_slash() {
        // Keys may contain any number of slashes; only the first one ends the bucket.
        let url = S3Url::parse("s3://b/dir/sub/key").unwrap();
        assert_eq!(
            (url.bucket.as_str(), url.key.as_str()),
            ("b", "dir/sub/key")
        );
        assert_eq!(S3Url::parse("s3://b").unwrap().key, "");
        assert!(S3Url::parse("s3://").is_err());
        assert!(S3Url::parse("/local/path").is_err());
    }

    #[test]
    fn recursive_prefixes_stop_at_a_directory_boundary() {
        // `rm -r s3://b/logs` must not delete `logs-old/`, which shares the prefix `logs`.
        assert_eq!(S3Url::parse("s3://b/logs").unwrap().dir_prefix(), "logs/");
        assert_eq!(S3Url::parse("s3://b/logs/").unwrap().dir_prefix(), "logs/");
        assert_eq!(S3Url::parse("s3://b").unwrap().dir_prefix(), "");
    }

    #[test]
    fn presign_lifetimes_stay_within_what_sigv4_signs() {
        // S3 rejects a presigned URL valid for more than a week only when it is used, so the
        // mistake is caught when the URL is made instead.
        assert_eq!(parse_expiry("1h"), Ok(Duration::from_secs(3600)));
        assert_eq!(parse_expiry("900"), Ok(Duration::from_secs(900)));
        assert_eq!(parse_expiry("7d"), Ok(MAX_PRESIGN));
        assert!(parse_expiry("8d").is_err());
        assert!(parse_expiry("0").is_err());
        assert!(parse_expiry("1w").is_err());
    }

    #[test]
    fn listing_dates_keep_the_servers_utc_time() {
        assert_eq!(date("2026-11-03T12:00:00.000Z"), "2026-11-03 12:00:00");
    }
}

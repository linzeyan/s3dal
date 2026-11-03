//! `cp` and `mv`: uploads, downloads and server-side copies, of one object or of a tree.

use std::fmt;
use std::io::Cursor;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, anyhow, bail};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

use super::api::S3;
use super::{S3Url, say};

/// Parts in flight per multipart upload: with 16 MiB parts, at most 64 MiB of a file is held
/// in memory at a time.
const PARTS_IN_FLIGHT: usize = 4;

/// Objects in flight for a recursive copy.
const FILES_IN_FLIGHT: usize = 8;

/// The most parts S3 accepts in one upload.
const MAX_PARTS: u64 = 10_000;

/// One side of a copy.
#[derive(Debug, Clone)]
pub enum Location {
    Local(PathBuf),
    /// `-`: stdin as a source, stdout as a destination.
    Stdio,
    S3(S3Url),
}

impl Location {
    pub fn parse(s: &str) -> Result<Self, String> {
        Ok(if s == "-" {
            Self::Stdio
        } else if s.starts_with("s3://") {
            Self::S3(S3Url::parse(s)?)
        } else {
            Self::Local(PathBuf::from(s))
        })
    }
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local(path) => write!(f, "{}", path.display()),
            Self::Stdio => write!(f, "-"),
            Self::S3(url) => write!(f, "{url}"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PartSizes {
    /// Uploads of this size and larger are multipart.
    pub threshold: u64,
    pub part: u64,
}

pub struct Job<'a> {
    pub s3: &'a S3,
    pub sizes: PartSizes,
    /// `mv`: delete each source once it is copied.
    pub remove_source: bool,
}

impl Job<'_> {
    /// Copies one file or object. As with the aws CLI, a destination that is a directory, a
    /// bucket, or ends in `/` receives the source under its own name.
    pub async fn copy_one_to(&self, src: Location, dst: Location) -> anyhow::Result<()> {
        let name = match &src {
            Location::Local(path) => {
                if path.is_dir() {
                    bail!(
                        "{} is a directory; copy it with --recursive",
                        path.display()
                    );
                }
                path.file_name().and_then(|n| n.to_str()).map(str::to_owned)
            }
            Location::S3(url) => url.object()?.1.rsplit('/').next().map(str::to_owned),
            Location::Stdio => None,
        };
        let dst = match dst {
            Location::S3(url) if url.key.is_empty() || url.key.ends_with('/') => {
                let name = name.with_context(|| format!("{url} names no object to write to"))?;
                Location::S3(url.with_key(format!("{}{name}", url.key)))
            }
            Location::Local(path)
                if path.is_dir() || path.to_string_lossy().ends_with(std::path::is_separator) =>
            {
                let name = name.context("a local destination needs an s3:// source")?;
                Location::Local(path.join(name))
            }
            dst => dst,
        };
        self.copy_one(&src, &dst).await
    }

    /// Copies a directory or every object under a prefix, keeping their relative paths.
    pub async fn copy_tree(&self, src: Location, dst: Location) -> anyhow::Result<()> {
        let pairs: Vec<(Location, Location)> = match (&src, &dst) {
            (Location::Local(dir), Location::S3(to)) => {
                let base = to.dir_prefix();
                local_files(dir)?
                    .into_iter()
                    .map(|(path, rel)| {
                        let key = format!("{base}{rel}");
                        (Location::Local(path), Location::S3(to.with_key(key)))
                    })
                    .collect()
            }
            (Location::S3(from), Location::Local(_) | Location::S3(_)) => {
                let prefix = from.dir_prefix();
                let objects = self
                    .s3
                    .list_all(&from.bucket, &prefix)
                    .await
                    .with_context(|| format!("cannot list {from}"))?;
                objects
                    .into_iter()
                    // Folder markers, as consoles create them, hold no data to copy.
                    .filter(|o| !o.key.ends_with('/'))
                    .map(|o| {
                        let rel = o.key.strip_prefix(&prefix).unwrap_or(&o.key);
                        let to = match &dst {
                            Location::Local(dir) => Location::Local(local_path(dir, rel)?),
                            Location::S3(to) => {
                                Location::S3(to.with_key(format!("{}{rel}", to.dir_prefix())))
                            }
                            Location::Stdio => unreachable!("matched above"),
                        };
                        Ok((Location::S3(from.with_key(o.key.clone())), to))
                    })
                    .collect::<anyhow::Result<_>>()?
            }
            _ => bail!(
                "--recursive copies a directory to s3://, or a prefix to a directory or to \
                 another prefix"
            ),
        };
        futures::stream::iter(pairs)
            .map(|(src, dst)| async move { self.copy_one(&src, &dst).await })
            .buffer_unordered(FILES_IN_FLIGHT)
            .try_collect()
            .await
    }

    async fn copy_one(&self, src: &Location, dst: &Location) -> anyhow::Result<()> {
        let s3 = self.s3;
        if self.remove_source && matches!((src, dst), (Location::Stdio, _) | (_, Location::Stdio)) {
            bail!("mv moves files and objects; use cp for stdin and stdout");
        }
        let copied = match (src, dst) {
            (Location::Local(path), Location::S3(url)) => {
                upload_file(s3, path, url, self.sizes).await
            }
            (Location::Stdio, Location::S3(url)) => upload_stdin(s3, url, self.sizes).await,
            (Location::S3(url), Location::Local(_) | Location::Stdio) => {
                download(s3, url, dst).await
            }
            (Location::S3(from), Location::S3(to)) => {
                // Moving an object onto itself would copy it, then delete the only copy.
                if from == to {
                    bail!("{from} cannot be copied onto itself");
                }
                let (src_bucket, src_key) = from.object()?;
                let (bucket, key) = to.object()?;
                s3.copy_object(src_bucket, src_key, bucket, key).await
            }
            _ => bail!("one side of a copy must be s3://"),
        };
        copied.with_context(|| format!("cannot copy {src} to {dst}"))?;
        if self.remove_source {
            match src {
                Location::Local(path) => tokio::fs::remove_file(path)
                    .await
                    .with_context(|| format!("cannot remove {}", path.display()))?,
                Location::S3(url) => s3
                    .delete_object(&url.bucket, &url.key)
                    .await
                    .with_context(|| format!("cannot delete {url}"))?,
                Location::Stdio => unreachable!("refused above"),
            }
        }
        // Nothing is said when stdout carries the data or stdin had no name.
        let verb = match (src, dst) {
            (Location::Stdio, _) | (_, Location::Stdio) => return Ok(()),
            _ if self.remove_source => "move",
            (Location::Local(_), _) => "upload",
            (_, Location::Local(_)) => "download",
            _ => "copy",
        };
        say(format_args!("{verb}: {src} to {dst}"))
    }
}

async fn upload_file(s3: &S3, path: &Path, url: &S3Url, sizes: PartSizes) -> anyhow::Result<()> {
    let (bucket, key) = url.object()?;
    let file = File::open(path)
        .await
        .with_context(|| format!("cannot open {}", path.display()))?;
    let len = file.metadata().await?.len();
    if len < sizes.threshold {
        return s3.put_object(bucket, key, file.into(), len).await;
    }
    multipart(s3, bucket, key, file, Some(len), sizes).await
}

async fn upload_stdin(s3: &S3, url: &S3Url, sizes: PartSizes) -> anyhow::Result<()> {
    let (bucket, key) = url.object()?;
    let mut stdin = tokio::io::stdin();
    // A stream's size is known only at its end, so what ends below the threshold goes up in
    // one PUT, as a file of that size would.
    let head = read_chunk(&mut stdin, sizes.threshold).await?;
    if (head.len() as u64) < sizes.threshold {
        let len = head.len() as u64;
        return s3.put_object(bucket, key, head.into(), len).await;
    }
    multipart(s3, bucket, key, Cursor::new(head).chain(stdin), None, sizes).await
}

/// Reads `reader` in parts, uploading a few at a time while the next is read. A failed upload
/// is aborted, so its parts do not linger on the server.
async fn multipart(
    s3: &S3,
    bucket: &str,
    key: &str,
    reader: impl AsyncRead + Unpin,
    len: Option<u64>,
    sizes: PartSizes,
) -> anyhow::Result<()> {
    // Larger parts for a file that would need more than S3's limit, as the aws CLI does.
    let part_size = match len {
        Some(len) => sizes.part.max(len.div_ceil(MAX_PARTS)),
        None => sizes.part,
    };
    let upload_id = s3.create_multipart_upload(bucket, key).await?;
    let chunks = futures::stream::try_unfold(
        (reader, 0u32, false),
        move |(mut reader, number, last)| async move {
            if last {
                return Ok(None);
            }
            let chunk = read_chunk(&mut reader, part_size).await?;
            // An empty source still gets its one part: an upload cannot complete without.
            if chunk.is_empty() && number > 0 {
                return Ok(None);
            }
            let number = number + 1;
            if u64::from(number) > MAX_PARTS {
                bail!("the stream needs more than {MAX_PARTS} parts of {part_size} bytes");
            }
            let last = (chunk.len() as u64) < part_size;
            anyhow::Ok(Some(((number, chunk), (reader, number, last))))
        },
    );
    let uploaded = chunks
        .map_ok(|(number, chunk)| s3.upload_part(bucket, key, &upload_id, number, chunk))
        .try_buffer_unordered(PARTS_IN_FLIGHT)
        .try_collect::<Vec<_>>()
        .await;
    let completed = match uploaded {
        Ok(mut parts) => {
            parts.sort_unstable_by_key(|(number, _)| *number);
            s3.complete_multipart_upload(bucket, key, &upload_id, &parts)
                .await
        }
        Err(err) => Err(err),
    };
    if completed.is_err() {
        // Best effort: the error worth reporting is the one that stopped the upload.
        let _ = s3.abort_multipart_upload(bucket, key, &upload_id).await;
    }
    completed
}

async fn read_chunk(reader: &mut (impl AsyncRead + Unpin), size: u64) -> std::io::Result<Bytes> {
    let mut buf = Vec::with_capacity(usize::try_from(size).unwrap_or(usize::MAX));
    reader.take(size).read_to_end(&mut buf).await?;
    Ok(buf.into())
}

/// Streams an object to a local file or stdout.
pub async fn download(s3: &S3, url: &S3Url, dst: &Location) -> anyhow::Result<()> {
    let (bucket, key) = url.object()?;
    let resp = s3.get_object(bucket, key).await?;
    let mut body = resp.bytes_stream();
    let path = match dst {
        Location::Local(path) => path,
        Location::Stdio => {
            let mut out = tokio::io::stdout();
            while let Some(chunk) = body.next().await {
                out.write_all(&chunk?).await?;
            }
            out.flush().await?;
            return Ok(());
        }
        Location::S3(_) => unreachable!("downloads go to a local file or stdout"),
    };
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        tokio::fs::create_dir_all(dir)
            .await
            .with_context(|| format!("cannot create {}", dir.display()))?;
    }
    // Written beside the destination and renamed over it once complete, so a failed download
    // never leaves a truncated file where a good one was.
    let name = path.file_name().context("no file name to download to")?;
    let tmp = path.with_file_name(format!(
        ".{}.{}.s3dal-download",
        name.to_string_lossy(),
        uuid::Uuid::new_v4().simple()
    ));
    let written = async {
        let mut file = File::create(&tmp)
            .await
            .with_context(|| format!("cannot create {}", tmp.display()))?;
        while let Some(chunk) = body.next().await {
            file.write_all(&chunk?).await?;
        }
        // tokio writes in the background; the last write must land before the rename.
        file.flush().await?;
        tokio::fs::rename(&tmp, path).await?;
        anyhow::Ok(())
    }
    .await;
    if written.is_err() {
        let _ = tokio::fs::remove_file(&tmp).await;
    }
    written
}

/// Every file under `root`, with its path relative to `root` in `/`-separated form, sorted.
fn local_files(root: &Path) -> anyhow::Result<Vec<(PathBuf, String)>> {
    let mut files = Vec::new();
    let mut dirs = vec![(root.to_path_buf(), String::new())];
    while let Some((dir, rel)) = dirs.pop() {
        let entries =
            std::fs::read_dir(&dir).with_context(|| format!("cannot read {}", dir.display()))?;
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| anyhow!("{} is not UTF-8, as keys must be", path.display()))?;
            let rel = format!("{rel}{name}");
            // Links to files are followed, as `cp` does; links to directories are not, as
            // they can form a cycle.
            if entry.file_type()?.is_dir() {
                dirs.push((path, format!("{rel}/")));
            } else if std::fs::metadata(&path).is_ok_and(|m| m.is_file()) {
                files.push((path, rel));
            }
        }
    }
    files.sort_by(|a, b| a.1.cmp(&b.1));
    Ok(files)
}

/// The path under `dir` for an object at relative key `rel`. A key is any string; one such as
/// `../../.bashrc` must not write outside the directory being copied to.
fn local_path(dir: &Path, rel: &str) -> anyhow::Result<PathBuf> {
    let mut path = dir.to_path_buf();
    for segment in rel.split('/') {
        let mut parts = Path::new(segment).components();
        match (parts.next(), parts.next()) {
            (Some(Component::Normal(name)), None) => path.push(name),
            _ => bail!("{rel:?} does not name a file under {}", dir.display()),
        }
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_cannot_escape_the_download_directory() {
        // Keys are chosen by whoever wrote the bucket; a recursive download must not let one
        // of them overwrite a file outside the destination.
        let dir = Path::new("/dl");
        assert_eq!(
            local_path(dir, "a/b.txt").unwrap(),
            Path::new("/dl/a/b.txt")
        );
        for key in ["../x", "a/../../x", "./x", "a//b", "/x", ""] {
            assert!(local_path(dir, key).is_err(), "{key}");
        }
    }
}

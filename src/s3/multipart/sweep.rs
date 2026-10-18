//! The stale-upload sweep. Uploads that are never completed or aborted would otherwise hold their
//! parts forever, out of sight of every listing but ListMultipartUploads; so would the records of
//! completed uploads, the leftovers of the races in design.md "Known deviations", and the uploads
//! of a bucket that is gone.
//!
//! Every decision rests on times that object stores provide too: the times `upload.json`
//! records, and file mtimes. Directory mtimes exist only on fs, so they matter only for a
//! directory that holds no file at all, which object stores cannot have.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use opendal::ErrorKind;
use uuid::Uuid;

use super::{parse_upload_id, record, remove_dir, upload_dir, uploads_dir};
use crate::S3Dal;
use crate::backend::RESERVED_ROOT;
use crate::s3::naming::{bucket_dir, validate_bucket_name};

/// Removes every stale upload directory and returns how many it removed. One that cannot be
/// read or removed is logged and left for the next sweep.
pub(in crate::s3) async fn sweep(dal: &S3Dal, age: Duration) -> anyhow::Result<usize> {
    let root = format!("{RESERVED_ROOT}uploads/");
    let buckets = match dal.op.list(&root).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(0),
        Err(err) => return Err(err).context("cannot list multipart uploads"),
    };
    let mut removed = 0;
    for entry in buckets {
        // s3dal names this directory only after a bucket it has checked, so anything else here
        // is not its own and stays.
        let Some(bucket) = entry.name().strip_suffix('/') else {
            continue;
        };
        if entry.path() == root || validate_bucket_name(bucket).is_err() {
            continue;
        }
        match sweep_bucket(dal, bucket, age).await {
            Ok(n) => removed += n,
            Err(err) => tracing::warn!(bucket, error = %err, "cannot sweep multipart uploads"),
        }
    }
    Ok(removed)
}

async fn sweep_bucket(dal: &S3Dal, bucket: &str, age: Duration) -> anyhow::Result<usize> {
    let exists = match dal.op.stat(&bucket_dir(bucket)).await {
        Ok(m) => m.is_dir(),
        Err(err) if err.kind() == ErrorKind::NotFound => false,
        Err(err) => return Err(err).context("cannot look up the bucket"),
    };
    let now = SystemTime::now();
    // The uploads of a bucket that is gone are stale at once, but only those that predate this
    // check: a newer one belongs to a bucket created again under the same name since.
    let cutoff = if exists {
        now.checked_sub(age).unwrap_or(UNIX_EPOCH)
    } else {
        now
    };
    let dir = uploads_dir(bucket);
    let entries = match dal.op.list(&dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(0),
        Err(err) => return Err(err).context("cannot list the bucket's uploads"),
    };
    let mut removed = 0;
    for entry in entries {
        let Some(id) = entry
            .name()
            .strip_suffix('/')
            .and_then(|name| parse_upload_id(name).ok())
        else {
            continue;
        };
        match sweep_upload(dal, bucket, id, cutoff).await {
            Ok(true) => removed += 1,
            Ok(false) => {}
            Err(err) => tracing::warn!(bucket, %id, error = %err, "cannot sweep multipart upload"),
        }
    }
    if !exists {
        // On fs the emptied directory would stay; one that still holds a newer upload is not
        // empty, and the non-recursive delete then fails and leaves it.
        let _ = dal.op.delete(&dir).await;
    }
    Ok(removed)
}

/// Removes the upload if it was last used before `cutoff`: initiated, for an active upload,
/// whatever happened since, as AWS lifecycle rules count (`DaysAfterInitiation`); completed, for
/// a completed one; and for a directory without `upload.json`, when anything in it was last
/// written.
async fn sweep_upload(
    dal: &S3Dal,
    bucket: &str,
    id: Uuid,
    cutoff: SystemTime,
) -> anyhow::Result<bool> {
    let dir = upload_dir(bucket, id);
    // A Complete of this upload in this process finishes first; its record then counts as
    // fresh. Elsewhere the sweep can race one, which then either fails or commits a whole
    // object, because it aborts its write when part data is missing.
    let _lock = dal.upload_locks.lock(&dir).await;
    let last_used = match record::load_upload_at(dal, &dir).await {
        Ok(Some(upload)) => Some(upload.completed.map_or(upload.initiated, |done| done.at)),
        Ok(None) => last_written(dal, &dir).await?,
        Err(err) => anyhow::bail!("cannot read upload.json: {err}"),
    };
    if !last_used.is_some_and(|t| t < cutoff) {
        return Ok(false);
    }
    remove_dir(dal, &dir)
        .await
        .map_err(|err| anyhow::anyhow!("cannot remove: {err}"))?;
    Ok(true)
}

/// When anything in `dir` was last written; `None` when nothing tells, which keeps the
/// directory. CreateMultipartUpload has such a directory without `upload.json` for a moment,
/// and its fresh time keeps the sweep from taking it for a leftover.
async fn last_written(dal: &S3Dal, dir: &str) -> anyhow::Result<Option<SystemTime>> {
    let mut newest = None;
    let mut files = 0;
    for entry in dal.op.list(dir).await.context("cannot list")? {
        if !entry.metadata().is_file() {
            continue;
        }
        let listed = entry.metadata().last_modified();
        let t = match listed {
            Some(t) => Some(t),
            None => match dal.op.stat(entry.path()).await {
                Ok(m) => m.last_modified(),
                // Removed since the listing, so it tells nothing either way.
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => return Err(err).context("cannot stat"),
            },
        };
        let Some(t) = t else {
            return Ok(None);
        };
        files += 1;
        newest = newest.max(Some(SystemTime::from(t)));
    }
    if files == 0 {
        // Only fs has empty directories, and only fs gives them an mtime.
        return match dal.op.stat(dir).await {
            Ok(m) => Ok(m.last_modified().map(SystemTime::from)),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err).context("cannot stat"),
        };
    }
    Ok(newest)
}

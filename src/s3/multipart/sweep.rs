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
use crate::s3::bucket_state;
use crate::s3::lifecycle::{self, Lifecycle};
use crate::s3::naming::{bucket_dir, validate_bucket_name};

/// Removes every stale upload directory and returns how many it removed. One that cannot be
/// read or removed is logged and left for the next sweep. The active uploads it keeps become
/// the metrics' count, which the requests of this process only move up and down from there:
/// the sweep is what sees those of other processes on the same backend, and those from before
/// the start.
pub(in crate::s3) async fn sweep(dal: &S3Dal, age: Duration) -> anyhow::Result<usize> {
    let root = format!("{RESERVED_ROOT}uploads/");
    let buckets = match dal.op.list(&root).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => {
            dal.metrics().set_multipart_open(0);
            return Ok(0);
        }
        Err(err) => return Err(err).context("cannot list multipart uploads"),
    };
    let (mut removed, mut active) = (0, 0);
    for entry in buckets {
        // s3dal names this directory only after a bucket it has checked, so anything else here
        // is not its own and stays.
        let Some(bucket) = entry.name().strip_suffix('/') else {
            continue;
        };
        if entry.path() == root || validate_bucket_name(bucket).is_err() {
            continue;
        }
        match sweep_bucket(dal, bucket, Some(age), SystemTime::now()).await {
            Ok((r, a)) => (removed, active) = (removed + r, active + a),
            Err(err) => tracing::warn!(bucket, error = %err, "cannot sweep multipart uploads"),
        }
    }
    dal.metrics().set_multipart_open(active);
    Ok(removed)
}

/// Aborts the bucket's incomplete uploads that its lifecycle rules (`AbortIncompleteMultipartUpload`)
/// name, as of `now`, and nothing else; for the lifecycle job, so the rules apply also where the
/// stale-upload sweep is off. Returns how many it aborted.
pub(in crate::s3) async fn abort_by_lifecycle(
    dal: &S3Dal,
    bucket: &str,
    now: SystemTime,
) -> anyhow::Result<usize> {
    sweep_bucket(dal, bucket, None, now)
        .await
        .map(|(removed, _)| removed)
}

/// How many uploads it removed, and how many active ones it kept. An active upload that one of
/// the bucket's abort rules covers goes by the rule; every other upload once it was last used
/// `age` ago, and with no `age` never.
async fn sweep_bucket(
    dal: &S3Dal,
    bucket: &str,
    age: Option<Duration>,
    now: SystemTime,
) -> anyhow::Result<(usize, usize)> {
    let exists = match dal.op.stat(&bucket_dir(bucket)).await {
        Ok(m) => m.is_dir(),
        Err(err) if err.kind() == ErrorKind::NotFound => false,
        Err(err) => return Err(err).context("cannot look up the bucket"),
    };
    // The uploads of a bucket that is gone are stale at once, but only those that predate this
    // check: a newer one belongs to a bucket created again under the same name since. Its rules,
    // if any were left behind, no longer apply.
    let (cutoff, lifecycle) = if exists {
        let state = bucket_state::get(dal, bucket)
            .await
            .map_err(|err| anyhow::anyhow!("cannot read the bucket's state: {err}"))?;
        let cutoff = age.map(|age| now.checked_sub(age).unwrap_or(UNIX_EPOCH));
        (cutoff, state.lifecycle.clone())
    } else {
        (Some(now), None)
    };
    let rules = Rules {
        cutoff,
        lifecycle: lifecycle.as_ref(),
        now,
    };
    let dir = uploads_dir(bucket);
    let entries = match dal.op.list(&dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok((0, 0)),
        Err(err) => return Err(err).context("cannot list the bucket's uploads"),
    };
    let (mut removed, mut active) = (0, 0);
    for entry in entries {
        let Some(id) = entry
            .name()
            .strip_suffix('/')
            .and_then(|name| parse_upload_id(name).ok())
        else {
            continue;
        };
        match sweep_upload(dal, bucket, id, &rules).await {
            Ok(Swept::Removed) => removed += 1,
            Ok(Swept::Active) => active += 1,
            Ok(Swept::Kept) => {}
            Err(err) => tracing::warn!(bucket, %id, error = %err, "cannot sweep multipart upload"),
        }
    }
    if !exists {
        // On fs the emptied directory would stay; one that still holds a newer upload is not
        // empty, and the non-recursive delete then fails and leaves it.
        let _ = dal.op.delete(&dir).await;
    }
    Ok((removed, active))
}

/// When an upload of one bucket is stale.
struct Rules<'a> {
    /// Last used before this; `None`: only the lifecycle rules remove anything.
    cutoff: Option<SystemTime>,
    lifecycle: Option<&'a Lifecycle>,
    now: SystemTime,
}

enum Swept {
    Removed,
    /// Kept, and neither completed nor aborted.
    Active,
    Kept,
}

/// Removes the upload if it was last used before the cutoff: initiated, for an active upload,
/// whatever happened since, as AWS lifecycle rules count (`DaysAfterInitiation`); completed, for
/// a completed one; and for a directory without `upload.json`, when anything in it was last
/// written. An active upload one of the bucket's abort rules covers goes by that rule instead.
async fn sweep_upload(
    dal: &S3Dal,
    bucket: &str,
    id: Uuid,
    rules: &Rules<'_>,
) -> anyhow::Result<Swept> {
    let dir = upload_dir(bucket, id);
    // A Complete of this upload in this process finishes first; its record then counts as
    // fresh. Elsewhere the sweep can race one, which then either fails or commits a whole
    // object, because it aborts its write when part data is missing.
    let _lock = dal.upload_locks.lock(&dir).await;
    let (last_used, active, deadline) = match record::load_upload_at(dal, &dir).await {
        Ok(Some(upload)) => {
            let active = upload.completed.is_none();
            let deadline = active
                .then(|| lifecycle::abort_due(rules.lifecycle, &upload.key, upload.initiated))
                .flatten();
            let last_used = upload
                .completed
                .as_ref()
                .map_or(upload.initiated, |done| done.at);
            (Some(last_used), active, deadline)
        }
        Ok(None) => (last_written(dal, &dir).await?, false, None),
        Err(err) => anyhow::bail!("cannot read upload.json: {err}"),
    };
    let stale = match deadline {
        Some(due) => rules.now >= due,
        None => rules
            .cutoff
            .is_some_and(|cutoff| last_used.is_some_and(|t| t < cutoff)),
    };
    if !stale {
        return Ok(if active { Swept::Active } else { Swept::Kept });
    }
    remove_dir(dal, &dir)
        .await
        .map_err(|err| anyhow::anyhow!("cannot remove: {err}"))?;
    Ok(Swept::Removed)
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

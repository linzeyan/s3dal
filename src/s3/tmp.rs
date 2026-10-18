//! The temporary files of fs writes. OpenDAL's fs writer writes each object to a file of its own
//! in `.s3dal/tmp/`, which `backend::build_operator` makes its `atomic_write_dir`, and renames it
//! into place once the write completes. A failed write removes its file, but one cut off by a
//! shutdown or a crash leaves it there, out of sight of every listing, for good.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use opendal::ErrorKind;

use crate::S3Dal;
use crate::backend::RESERVED_ROOT;

/// Removes every regular file in `.s3dal/tmp/` last written before `age` ago and returns how
/// many it removed. A file that cannot be removed is logged and left for the next sweep.
pub(super) async fn sweep(dal: &S3Dal, age: Duration) -> anyhow::Result<usize> {
    // Only the fs writer writes through the directory; elsewhere it stays empty.
    if dal.op.info().scheme() != "fs" {
        return Ok(0);
    }
    let dir = format!("{RESERVED_ROOT}tmp/");
    let entries = match dal.op.list(&dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(0),
        Err(err) => return Err(err).context("cannot list temporary files"),
    };
    // Every write moves its file's mtime, so the file of a live write, in this process or in
    // another one on the same root, is never this old.
    let cutoff = SystemTime::now().checked_sub(age).unwrap_or(UNIX_EPOCH);
    let mut removed = 0;
    for entry in entries {
        let meta = entry.metadata();
        let stale = meta
            .last_modified()
            .is_some_and(|t| SystemTime::from(t) < cutoff);
        if !meta.is_file() || !stale {
            continue;
        }
        match dal.op.delete(entry.path()).await {
            Ok(()) => removed += 1,
            Err(err) => {
                tracing::warn!(path = entry.path(), error = %err, "cannot remove temporary file");
            }
        }
    }
    Ok(removed)
}

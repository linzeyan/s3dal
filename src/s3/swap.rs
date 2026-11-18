//! Read-modify-write of a small JSON file of s3dal's own under `.s3dal/`, such as a bucket's
//! state or a job's lease, which several instances sharing the backend may change at once.
//!
//! Where the backend checks write conditions (`Profile::conditional_writes`), the write carries
//! the version it read, and one that another instance replaced in between is read and changed
//! again. Elsewhere a lock held for the whole read-modify-write keeps the changes apart within
//! the process, which is as far as they reach there: fs serves one process per root.

use std::sync::LazyLock;

use opendal::options::WriteOptions;
use opendal::{Error, ErrorKind, Metadata};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::S3Dal;
use super::store::KeyLocks;
use crate::backend::without_stalling;

/// How often a change is read and applied again after another instance's landed first. These
/// files change when an operator changes a setting or a lease is renewed, so two writers meet
/// rarely, and a third round almost never.
const ATTEMPTS: usize = 10;

/// Process-wide rather than per [`S3Dal`]: every handle of this process on the backend is one
/// writer to it.
static LOCKS: LazyLock<KeyLocks> = LazyLock::new(KeyLocks::default);

/// Reads the JSON file at `path` (`None` when there is none), passes it to `change`, and writes
/// what `change` returns, unless it returns `None`, which leaves the file as it is. Returns what
/// was written, or `None` when `change` left the file. `change` may run more than once, each time
/// on the file as it then is.
pub(crate) async fn update<T>(
    dal: &S3Dal,
    path: &str,
    mut change: impl FnMut(Option<T>) -> Option<T>,
) -> Result<Option<T>, Error>
where
    T: Serialize + DeserializeOwned,
{
    let checked = dal.profile.conditional_writes;
    let _guard = if checked {
        None
    } else {
        let info = dal.op.info();
        let key = format!("{}:{}:{}{path}", info.scheme(), info.name(), info.root());
        Some(LOCKS.lock(&key).await)
    };
    for _ in 0..ATTEMPTS {
        let (current, found) = read(dal, path).await?;
        let Some(next) = change(current) else {
            return Ok(None);
        };
        let json = serde_json::to_vec(&next).map_err(|e| invalid(path, e))?;
        let mut opts = WriteOptions::default();
        if checked {
            match found {
                Some(m) => opts.if_not_changed = Some(m),
                None => opts.if_not_exists = true,
            }
        }
        match without_stalling(&dal.op, dal.op.write_options(path, json, opts)).await {
            Ok(_) => return Ok(Some(next)),
            Err(err) if err.kind() == ErrorKind::ConditionNotMatch => {}
            Err(err) => return Err(err),
        }
    }
    Err(Error::new(
        ErrorKind::ConditionNotMatch,
        format!("{path} kept changing while it was being written"),
    ))
}

/// The file's content and the version it was read at. The version comes first: a change that
/// lands between the two then fails the conditional write that follows, since the backend no
/// longer holds that version, rather than going unnoticed.
async fn read<T: DeserializeOwned>(
    dal: &S3Dal,
    path: &str,
) -> Result<(Option<T>, Option<Metadata>), Error> {
    let found = match dal.op.stat(path).await {
        Ok(m) => m,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok((None, None)),
        Err(err) => return Err(err),
    };
    let buf = match dal.op.read(path).await {
        Ok(buf) => buf,
        // Removed in between: there is nothing, and the write may only create the file.
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok((None, None)),
        Err(err) => return Err(err),
    };
    let value = serde_json::from_slice(&buf.to_bytes()).map_err(|e| invalid(path, e))?;
    Ok((Some(value), Some(found)))
}

/// Reads the JSON file at `path`, `None` when there is none.
pub(crate) async fn load<T: DeserializeOwned>(dal: &S3Dal, path: &str) -> Result<Option<T>, Error> {
    match dal.op.read(path).await {
        Ok(buf) => serde_json::from_slice(&buf.to_bytes())
            .map(Some)
            .map_err(|e| invalid(path, e)),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

fn invalid(path: &str, err: serde_json::Error) -> Error {
    Error::new(ErrorKind::Unexpected, format!("{path} is not valid: {err}"))
}

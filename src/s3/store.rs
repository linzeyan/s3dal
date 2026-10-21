//! Changing a key: the steps PutObject, CopyObject, CompleteMultipartUpload, DeleteObject and
//! DeleteObjects share. Every commit to a key happens under [`KeyLocks`], so a conditional
//! write or delete is checked against exactly the state it replaces (design.md "Conditional
//! writes"). Where the backend checks write conditions itself (`Profile::conditional_writes`),
//! a conditional write also carries its condition there, which holds against other processes.

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::io;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use opendal::options::{ComposeOptions, WriteOptions};
use opendal::{Buffer, Metadata, Operator, Writer};
use s3s::dto::{ETag, ETagCondition, Timestamp};
use s3s::{S3Error, S3Result, s3_error};
use tokio::sync::{Mutex, MutexGuard};

use super::upload::{BodyCheck, Verified};
use super::{S3Dal, naming, precondition};
use crate::backend::without_stalling;
use crate::error::{Ctx, io_kind, map};
use crate::meta::{self, ObjectMeta};

/// Serializes commits to the same key within this process.
///
/// Keys hash onto a fixed set of stripes, so there is nothing to clean up when a waiting request
/// is cancelled. Two keys may share a stripe and briefly wait for each other; no operation ever
/// holds two stripes of one set at once, so that cannot deadlock. Multipart uploads have a set
/// of their own (`S3Dal::upload_locks`), always taken before a key's and never while holding
/// one.
pub(super) struct KeyLocks {
    stripes: Box<[Mutex<()>]>,
    hasher: RandomState,
}

impl Default for KeyLocks {
    fn default() -> Self {
        Self {
            stripes: (0..1024).map(|_| Mutex::new(())).collect(),
            hasher: RandomState::new(),
        }
    }
}

impl KeyLocks {
    pub(super) async fn lock(&self, path: &str) -> MutexGuard<'_, ()> {
        let i = self.hasher.hash_one(path) % self.stripes.len() as u64;
        self.stripes[i as usize].lock().await
    }
}

/// Conditions on the object a write or delete replaces.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct Precondition<'a> {
    pub if_match: Option<&'a ETagCondition>,
    pub if_none_match: Option<&'a ETagCondition>,
}

impl Precondition<'_> {
    /// Refuses what S3 does not support, before any body is read: on writes, `If-None-Match`
    /// takes only `*`.
    pub(super) fn validate(&self) -> S3Result<()> {
        if let Some(ETagCondition::ETag(_)) = self.if_none_match {
            return Err(s3_error!(
                NotImplemented,
                "If-None-Match on a write supports only '*'"
            ));
        }
        Ok(())
    }

    pub(super) fn is_empty(&self) -> bool {
        self.if_match.is_none() && self.if_none_match.is_none()
    }

    /// `current` is the ETag of the object the key holds now, `None` when it holds none.
    fn check(&self, current: Option<&str>) -> S3Result<()> {
        if let Some(cond) = self.if_match {
            // S3 answers If-Match on a missing key with 404, not 412.
            let Some(etag) = current else {
                return Err(s3_error!(NoSuchKey));
            };
            let matched = match cond {
                ETagCondition::Any => true,
                ETagCondition::ETag(tag) => ETag::Strong(etag.to_owned()).strong_cmp(tag),
            };
            if !matched {
                return Err(precondition_failed());
            }
        }
        if self.if_none_match.is_some() && current.is_some() {
            return Err(precondition_failed());
        }
        Ok(())
    }
}

/// Conditions on the object a delete removes. AWS takes the size and time conditions only on
/// directory buckets; here they hold on every bucket and are checked like `If-Match`.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct DeleteCondition<'a> {
    pub if_match: Option<&'a ETagCondition>,
    pub last_modified: Option<&'a Timestamp>,
    pub size: Option<i64>,
}

impl DeleteCondition<'_> {
    fn is_empty(&self) -> bool {
        self.if_match.is_none() && self.last_modified.is_none() && self.size.is_none()
    }

    /// `current` is the object the key holds now, `None` when it holds none.
    fn check(&self, current: Option<&ObjectMeta>) -> S3Result<()> {
        let etag = Precondition {
            if_match: self.if_match,
            if_none_match: None,
        };
        etag.check(current.map(|obj| obj.etag.as_str()))?;
        // AWS answers a size or time condition on a missing key with 204, unlike If-Match.
        let Some(obj) = current else {
            return Ok(());
        };
        let size_holds = self
            .size
            .is_none_or(|n| u64::try_from(n).is_ok_and(|n| n == obj.content_length));
        // An object without a known mtime cannot be shown to match, so it is kept.
        let time_holds = self
            .last_modified
            .is_none_or(|t| obj.last_modified.is_some_and(|m| same_second(m, t)));
        if size_holds && time_holds {
            Ok(())
        } else {
            Err(precondition_failed())
        }
    }
}

/// S3 reports Last-Modified in whole seconds, so a time read back from it, or from a listing
/// with milliseconds, names the second the mtime falls in.
fn same_second(mtime: SystemTime, given: &Timestamp) -> bool {
    let start = precondition::whole_seconds(mtime);
    let end = start + Duration::from_secs(1);
    (Timestamp::from(start)..Timestamp::from(end)).contains(given)
}

fn precondition_failed() -> S3Error {
    s3_error!(
        PreconditionFailed,
        "At least one of the pre-conditions you specified did not hold"
    )
}

/// The ETag of the object at `path`, `None` when there is none. A directory is not an object.
async fn current_etag(dal: &S3Dal, path: &str) -> S3Result<Option<String>> {
    Ok(stat_existing(dal, path)
        .await?
        .filter(Metadata::is_file)
        .map(|m| meta::resolve(&dal.profile, &m).etag))
}

/// What a conditional write requires of its key where it commits, from the state its condition
/// was checked against when the write began.
pub(super) enum Expected {
    /// The key held nothing: the write may only create it.
    Absent,
    /// The key held this object: the write may only replace exactly it.
    Unchanged(Metadata),
}

impl Expected {
    fn apply(self, opts: &mut WriteOptions) {
        match self {
            Self::Absent => opts.if_not_exists = true,
            Self::Unchanged(m) => opts.if_not_changed = Some(m),
        }
    }

    pub(super) fn apply_to_compose(self, opts: &mut ComposeOptions) {
        match self {
            Self::Absent => opts.if_not_exists = true,
            Self::Unchanged(m) => opts.if_not_changed = Some(m),
        }
    }
}

/// What `path` holds, `None` when nothing a key could name is there.
async fn stat_existing(dal: &S3Dal, path: &str) -> S3Result<Option<Metadata>> {
    match dal.op.stat(path).await {
        Ok(m) => Ok(Some(m)),
        Err(err) => {
            let mapped = map(err, Ctx::ObjectRead);
            if *mapped.code() == s3s::S3ErrorCode::NoSuchKey {
                Ok(None)
            } else {
                Err(mapped)
            }
        }
    }
}

/// What a writer hands the backend at a time where the backend sets no minimum part size.
/// OpenDAL then passes on every `write` as it comes, and azblob makes each one a block: a
/// streamed body became a request per network read, and failed past Azure's 50,000 blocks per
/// blob. fs declares the same capabilities, and only writes in larger batches.
pub(super) const UNSIZED_WRITE_CHUNK: usize = 8 * 1024 * 1024;

/// `opts` with [`UNSIZED_WRITE_CHUNK`] where the backend needs it. Elsewhere the chunking
/// stays as given, the default included, which `copy_parts` relies on.
pub(super) fn chunked(op: &Operator, mut opts: WriteOptions) -> WriteOptions {
    if opts.chunk.is_none() && op.info().capability().write_multi_min_size.is_none() {
        opts.chunk = Some(UNSIZED_WRITE_CHUNK);
    }
    opts
}

/// Azure's limit of blocks in one blob.
const MAX_BLOCKS: u64 = 50_000;

/// The chunk for a write of `total` bytes where the backend sets no minimum part size: at least
/// [`UNSIZED_WRITE_CHUNK`], and large enough for the write to fit in Azure's blocks. At 8 MiB
/// a Complete failed past 390 GiB, where S3 takes objects up to 5 TiB, and so did a PUT or a
/// copy, which s3dal does not cap at S3's 5 GiB. `None` elsewhere, which keeps the default
/// chunking `copy_parts` needs there.
pub(super) fn chunk_for(op: &Operator, total: u64) -> Option<usize> {
    const MIB: u64 = 1024 * 1024;
    if op.info().capability().write_multi_min_size.is_some() {
        return None;
    }
    let fitting = total.div_ceil(MAX_BLOCKS).div_ceil(MIB) * MIB;
    Some(usize::try_from(fitting).map_or(usize::MAX, |n| n.max(UNSIZED_WRITE_CHUNK)))
}

/// A key being written. Every writer from [`Dest::open`] ends in exactly one of
/// [`Dest::commit`] or [`Dest::abandon`].
pub(super) struct Dest<'a> {
    pub dal: &'a S3Dal,
    pub bucket: &'a str,
    pub key: &'a str,
    pub path: String,
}

impl<'a> Dest<'a> {
    /// The bucket must already be known to exist and the key to be valid.
    pub(super) fn new(dal: &'a S3Dal, bucket: &'a str, key: &'a str) -> Self {
        Self {
            dal,
            bucket,
            key,
            path: naming::object_path(bucket, key),
        }
    }

    /// With `native_etag` the backend computes the ETag, and S3 computes an MD5 only for a body
    /// sent in one request, while OpenDAL's writer turns a body into a multipart upload once it
    /// outgrows the chunk size (5 MiB on S3 by default). Raising the chunk to
    /// `etag_buffer_limit` gives every backend the same rule: a body up to that size gets an
    /// MD5 ETag, held in memory to get it. Only for single-request writes: Complete relies on
    /// the default chunking (see `copy_parts`).
    pub(super) fn send_whole_up_to_limit(&self, opts: &mut WriteOptions) {
        if self.dal.profile.native_etag {
            opts.chunk =
                Some(usize::try_from(self.dal.settings.etag_buffer_limit).unwrap_or(usize::MAX));
        }
    }

    /// Opens a writer for the key. Where the backend checks conditions, `cond` is checked now
    /// and the writer carries it (see [`Self::expected`]); [`Self::commit`] checks it either way.
    pub(super) async fn open(
        &self,
        mut opts: WriteOptions,
        cond: Precondition<'_>,
    ) -> S3Result<Writer> {
        if let Some(expected) = self.expected(cond).await? {
            expected.apply(&mut opts);
        }
        self.dal
            .op
            .writer_options(&self.path, chunked(&self.dal.op, opts))
            .await
            .map_err(write_err)
    }

    /// Whether the write carries `cond` to the backend, which checks it where the write lands.
    fn backend_checks(&self, cond: Precondition<'_>) -> bool {
        !cond.is_empty() && self.dal.profile.conditional_writes
    }

    /// Where the backend checks conditions, checks `cond` against what the key holds now and
    /// returns what the backend must then find when the write commits: another s3dal process
    /// sharing the backend takes no lock of this one, and only the backend sees its writes
    /// land. The condition is thus held to the state found here, not at the commit, so a key
    /// rewritten meanwhile fails the write with 412 even if the new object would meet the
    /// condition too, where S3 would let it through.
    pub(super) async fn expected(&self, cond: Precondition<'_>) -> S3Result<Option<Expected>> {
        if !self.backend_checks(cond) {
            return Ok(None);
        }
        let found = stat_existing(self.dal, &self.path)
            .await?
            .filter(Metadata::is_file);
        let etag = found
            .as_ref()
            .map(|m| meta::resolve(&self.dal.profile, m).etag);
        cond.check(etag.as_deref())?;
        Ok(Some(match found {
            Some(m) => Expected::Unchanged(m),
            None => Expected::Absent,
        }))
    }

    /// Evaluates `cond` against what the key holds now. Outside [`Self::commit`] the answer is
    /// only a forecast, since the key can change before the commit checks again under the lock.
    /// Copy and CompleteMultipartUpload ask before writing anything, so a request that is
    /// bound to fail does not move the data first, and Complete still gets to answer with a
    /// status code instead of an error document inside its 200.
    pub(super) async fn check(&self, cond: Precondition<'_>) -> S3Result<()> {
        if cond.is_empty() {
            return Ok(());
        }
        let current = current_etag(self.dal, &self.path).await?;
        cond.check(current.as_deref())
    }

    /// Makes the write visible. The condition is checked and the writer closed under the key's
    /// lock, so no other commit to the key in this process can land in between. Where the
    /// backend checks conditions, the writer carries the condition as well (see
    /// [`Self::open`]); the check here still holds where the backend's is only measured on a
    /// single request, not on the multipart upload a large write becomes. On failure the write
    /// is abandoned.
    pub(super) async fn commit(&self, mut w: Writer, cond: Precondition<'_>) -> S3Result<Metadata> {
        let result = {
            let _guard = self.dal.locks.lock(&self.path).await;
            match self.check(cond).await {
                Ok(()) => without_stalling(&self.dal.op, w.close())
                    .await
                    .map_err(write_err),
                Err(err) => Err(err),
            }
        };
        if result.is_err() {
            self.abandon(&mut w).await;
        }
        result
    }

    /// Writes a body that is already verified.
    pub(super) async fn write_all(
        &self,
        opts: WriteOptions,
        data: Buffer,
        cond: Precondition<'_>,
    ) -> S3Result<Metadata> {
        let mut w = self.open(opts, cond).await?;
        if let Err(err) = w.write(data).await {
            self.abandon(&mut w).await;
            return Err(write_err(err));
        }
        self.commit(w, cond).await
    }

    /// Streams `body` into the key and verifies it with `check` before committing, so a body
    /// that fails any check is aborted instead of becoming visible. `trailer` looks up
    /// aws-chunked trailers once the body has ended.
    pub(super) async fn write_stream<S>(
        &self,
        opts: WriteOptions,
        mut body: S,
        check: &mut BodyCheck,
        trailer: impl Fn(&str) -> Option<String>,
        cond: Precondition<'_>,
    ) -> S3Result<(Verified, Metadata)>
    where
        S: Stream<Item = S3Result<Bytes>> + Unpin,
    {
        let mut w = self.open(opts, cond).await?;
        let streamed = async {
            while let Some(chunk) = body.next().await {
                let chunk = chunk?;
                check.update(&chunk)?;
                w.write(chunk).await.map_err(write_err)?;
            }
            check.finish(trailer)
        }
        .await;
        match streamed {
            Ok(verified) => Ok((verified, self.commit(w, cond).await?)),
            Err(err) => {
                self.abandon(&mut w).await;
                Err(err)
            }
        }
    }

    /// Discards a write that failed or did not verify, so nothing of it becomes visible.
    /// `abort` (never `close`) removes the temporary file; creating the writer may already have
    /// made the key's parent directories, which are pruned like after a delete.
    pub(super) async fn abandon(&self, w: &mut Writer) {
        if let Err(err) = w.abort().await {
            tracing::warn!(
                path = self.path,
                error = %err,
                "cannot abort upload; temporary data may remain"
            );
        }
        prune_empty_parents(self.dal, self.bucket, self.key).await;
    }

    /// The ETag a following HEAD reports for the object just written. Close metadata describes
    /// exactly this write, so it is preferred; a `stat` could already see a newer one.
    pub(super) async fn written_etag(&self, closed: Metadata, written: u64) -> S3Result<String> {
        let profile = &self.dal.profile;
        let complete = if profile.native_etag {
            closed.etag().is_some()
        } else {
            closed.last_modified().is_some() && closed.content_length() == written
        };
        let m = if complete {
            closed
        } else {
            self.dal
                .op
                .stat(&self.path)
                .await
                .map_err(|e| map(e, Ctx::ObjectRead))?
        };
        Ok(meta::resolve(profile, &m).etag)
    }
}

/// Deletes what `key` names, as DeleteObject does: a key that names nothing is deleted
/// successfully. The bucket must already be known to exist.
pub(super) async fn delete_key(
    dal: &S3Dal,
    bucket: &str,
    key: &str,
    cond: DeleteCondition<'_>,
) -> S3Result<()> {
    if delete_unpruned(dal, bucket, key, cond).await? {
        prune_empty_parents(dal, bucket, key).await;
    }
    Ok(())
}

/// [`delete_key`] without pruning the parents: whether anything was deleted, so a caller
/// deleting many keys prunes each directory once (see [`prune_empty_parents`]).
pub(super) async fn delete_unpruned(
    dal: &S3Dal,
    bucket: &str,
    key: &str,
    cond: DeleteCondition<'_>,
) -> S3Result<bool> {
    naming::validate_key(key)?;
    let path = naming::object_path(bucket, key);
    let deleted = {
        let _guard = dal.locks.lock(&path).await;
        // OpenDAL's fs delete removes whatever the path names: deleting `a/` would remove the
        // object `a`, and deleting `a` would remove an empty directory `a/`. Only delete what
        // the key really names; anything else is a missing key.
        let found = stat_existing(dal, &path).await?;
        if !cond.is_empty() {
            let current = found
                .as_ref()
                .filter(|m| m.is_file())
                .map(|m| meta::resolve(&dal.profile, m));
            cond.check(current.as_ref())?;
            // Only an object can meet a condition. A folder marker is none, as for GET, so a
            // condition that held because no object is there must not remove the marker.
            if current.is_none() {
                return Ok(false);
            }
        }
        match found {
            Some(m) if key.ends_with('/') => m.is_dir() && remove_empty_dir(dal, &path).await?,
            Some(m) if m.is_file() => {
                dal.op.delete(&path).await.map_err(|e| map(e, Ctx::Other))?;
                true
            }
            _ => false,
        }
    };
    Ok(deleted)
}

/// Removes a folder marker. S3 deletes only the marker object, never the keys under it; here
/// the marker is the directory, so it stays while it holds anything.
async fn remove_empty_dir(dal: &S3Dal, dir: &str) -> S3Result<bool> {
    match dal.op.delete(dir).await {
        Ok(()) => Ok(true),
        Err(err) if io_kind(&err) == Some(io::ErrorKind::DirectoryNotEmpty) => Ok(false),
        Err(err) => Err(map(err, Ctx::Other)),
    }
}

/// On backends with real directories, a directory left empty by a delete would still list as
/// a CommonPrefix. Walks up from `key` removing empty parents, never the bucket itself.
/// Best effort: the object is already gone, so a failure here must not fail the request.
pub(super) async fn prune_empty_parents(dal: &S3Dal, bucket: &str, key: &str) {
    if !dal.profile.real_dirs {
        return;
    }
    let mut rest = key.trim_end_matches('/');
    while let Some((parent, _)) = rest.rsplit_once('/') {
        let dir = format!("{}/", naming::object_path(bucket, parent));
        // stat of `x/` succeeds only for a directory. Without this check a concurrent writer
        // that replaced the directory with an object `x` would lose it to the delete below.
        if !dal.op.stat(&dir).await.is_ok_and(|m| m.is_dir()) {
            return;
        }
        match dal.op.delete(&dir).await {
            Ok(()) => rest = parent,
            // Another prune removed it between the stat and here: that of a sibling directory
            // emptied by the same DeleteObjects batch, or of a concurrent request. It is gone,
            // as wanted, so this is no failure to warn about.
            Err(err) if err.kind() == opendal::ErrorKind::NotFound => rest = parent,
            Err(err) if io_kind(&err) == Some(io::ErrorKind::DirectoryNotEmpty) => return,
            Err(err) => {
                tracing::warn!(%dir, error = %err, "cannot remove empty parent directory");
                return;
            }
        }
    }
}

pub(super) fn write_err(err: opendal::Error) -> S3Error {
    map(err, Ctx::ObjectWrite)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn tag(value: &str) -> ETagCondition {
        ETagCondition::ETag(ETag::Strong(value.to_owned()))
    }

    fn outcome(cond: Precondition<'_>, current: Option<&str>) -> Option<s3s::S3ErrorCode> {
        cond.check(current).err().map(|e| e.code().clone())
    }

    #[tokio::test]
    async fn a_key_is_locked_until_its_guard_drops() {
        // A conditional write is only correct if no other commit to its key can run between
        // its check and its close.
        let locks = KeyLocks::default();
        let held = locks.lock("b/k").await;
        let waiting = tokio::time::timeout(Duration::from_millis(20), locks.lock("b/k")).await;
        assert!(
            waiting.is_err(),
            "a second commit got in while the key was held"
        );
        drop(held);
        let _next = tokio::time::timeout(Duration::from_secs(1), locks.lock("b/k"))
            .await
            .expect("the key stays locked after its guard dropped");
    }

    #[test]
    fn a_complete_fits_in_the_blocks_of_one_blob() {
        // Each chunk becomes an Azure block, and a blob takes 50,000: an object S3 accepts, up
        // to 5 TiB, must fit, or its Complete fails after every part was uploaded.
        let memory = crate::backend::build_operator("memory", &[]).unwrap();
        const MIB: u64 = 1024 * 1024;
        let max_object = 5 * 1024 * 1024 * MIB;
        let chunk = chunk_for(&memory, max_object).unwrap() as u64;
        assert!(chunk * MAX_BLOCKS >= max_object, "{chunk}");
        assert_eq!(chunk % MIB, 0, "{chunk}");
        assert_eq!(chunk_for(&memory, 0), Some(UNSIZED_WRITE_CHUNK));
        assert_eq!(
            chunk_for(&memory, MAX_BLOCKS * UNSIZED_WRITE_CHUNK as u64),
            Some(UNSIZED_WRITE_CHUNK)
        );
        assert!(
            chunk_for(&memory, MAX_BLOCKS * UNSIZED_WRITE_CHUNK as u64 + 1).unwrap()
                > UNSIZED_WRITE_CHUNK
        );
        // A backend with a minimum part size keeps OpenDAL's own chunking (see `copy_parts`).
        let gcs = crate::backend::build_operator(
            "gcs",
            &[
                ("bucket".to_owned(), "b".to_owned()),
                ("endpoint".to_owned(), "http://127.0.0.1:1".to_owned()),
                ("skip_signature".to_owned(), "true".to_owned()),
            ],
        )
        .unwrap();
        assert_eq!(chunk_for(&gcs, max_object), None);
    }

    #[test]
    fn if_none_match_star_refuses_any_existing_object() {
        // Two writers creating the same lock file: only the first may win.
        let any = ETagCondition::Any;
        let create = Precondition {
            if_none_match: Some(&any),
            ..Default::default()
        };
        assert_eq!(outcome(create, None), None);
        assert_eq!(
            outcome(create, Some("abc")),
            Some(s3s::S3ErrorCode::PreconditionFailed)
        );
    }

    #[test]
    fn if_match_replaces_only_the_expected_version() {
        let expected = tag("abc");
        let replace = Precondition {
            if_match: Some(&expected),
            ..Default::default()
        };
        assert_eq!(outcome(replace, Some("abc")), None);
        assert_eq!(
            outcome(replace, Some("def")),
            Some(s3s::S3ErrorCode::PreconditionFailed)
        );
        // S3 answers a missing key with 404, which tells the client the object is gone rather
        // than changed.
        assert_eq!(outcome(replace, None), Some(s3s::S3ErrorCode::NoSuchKey));
    }

    fn object(len: u64, mtime: Option<SystemTime>) -> ObjectMeta {
        ObjectMeta {
            etag: "abc".to_owned(),
            content_length: len,
            last_modified: mtime,
            attrs: meta::Attrs::default(),
            checksum: s3s::dto::Checksum::default(),
            parts: None,
        }
    }

    fn deleted(
        cond: DeleteCondition<'_>,
        current: Option<&ObjectMeta>,
    ) -> Option<s3s::S3ErrorCode> {
        cond.check(current).err().map(|e| e.code().clone())
    }

    #[test]
    fn size_and_time_conditions_delete_only_the_object_they_describe() {
        // A cleanup job that read an object's size and time must not delete a replacement.
        let at = |secs, nanos| std::time::UNIX_EPOCH + Duration::new(secs, nanos);
        let current = object(5, Some(at(20, 500_000_000)));
        let size = |n| DeleteCondition {
            size: Some(n),
            ..Default::default()
        };
        assert_eq!(deleted(size(5), Some(&current)), None);
        assert_eq!(
            deleted(size(6), Some(&current)),
            Some(s3s::S3ErrorCode::PreconditionFailed)
        );
        assert_eq!(
            deleted(size(-1), Some(&current)),
            Some(s3s::S3ErrorCode::PreconditionFailed)
        );

        // S3 reports whole seconds, and listings here milliseconds; either names the mtime.
        for (given, holds) in [
            (at(20, 0), true),
            (at(20, 123_000_000), true),
            (at(19, 999_000_000), false),
            (at(21, 0), false),
        ] {
            let given = Timestamp::from(given);
            let cond = DeleteCondition {
                last_modified: Some(&given),
                ..Default::default()
            };
            let expected = (!holds).then_some(s3s::S3ErrorCode::PreconditionFailed);
            assert_eq!(deleted(cond, Some(&current)), expected, "{given:?}");
            // Without a known mtime nothing can be shown to match, so the object is kept.
            assert_eq!(
                deleted(cond, Some(&object(5, None))),
                Some(s3s::S3ErrorCode::PreconditionFailed)
            );
        }
    }

    #[test]
    fn a_missing_key_meets_size_and_time_but_not_if_match() {
        // AWS: size and time conditions on a missing key succeed with 204; If-Match reports
        // the object is gone with 404.
        let given = Timestamp::from(std::time::UNIX_EPOCH);
        let expected = tag("abc");
        let size_and_time = DeleteCondition {
            size: Some(5),
            last_modified: Some(&given),
            ..Default::default()
        };
        assert_eq!(deleted(size_and_time, None), None);
        let with_etag = DeleteCondition {
            if_match: Some(&expected),
            ..size_and_time
        };
        assert_eq!(deleted(with_etag, None), Some(s3s::S3ErrorCode::NoSuchKey));
        // Every condition given must hold.
        let current = object(5, Some(std::time::UNIX_EPOCH));
        assert_eq!(deleted(with_etag, Some(&current)), None);
        assert_eq!(
            deleted(with_etag, Some(&object(6, Some(std::time::UNIX_EPOCH)))),
            Some(s3s::S3ErrorCode::PreconditionFailed)
        );
    }

    #[tokio::test]
    async fn a_conditional_write_fails_when_another_process_changes_the_key_before_its_commit() {
        // s3dal processes sharing a backend share no lock, and only the backend sees another
        // one's write land between this write's check and its commit. Unless the write carries
        // its condition there, two processes creating the same lock file both succeed.
        let op = Operator::via_iter("memory", Vec::<(String, String)>::new())
            .unwrap()
            .layer(intruding::Intruding);
        let profile = crate::Profile {
            native_etag: false,
            native_content_type: false,
            native_cache_control: false,
            native_content_disposition: false,
            native_content_encoding: false,
            encoded_metadata_names: false,
            ordered_list: false,
            real_dirs: false,
            conditional_writes: true,
        };
        let dal = S3Dal::new(op.clone(), profile, crate::Settings::default());
        let create = Precondition {
            if_none_match: Some(&ETagCondition::Any),
            ..Default::default()
        };
        let err = Dest::new(&dal, "b", "k")
            .write_all(WriteOptions::default(), Buffer::from("mine"), create)
            .await
            .unwrap_err();
        assert_eq!(*err.code(), s3s::S3ErrorCode::PreconditionFailed);
        assert_eq!(op.read("b/k").await.unwrap().to_vec(), b"intruder");
    }

    /// Another process writing a key between a write's check and its commit: each writer's
    /// close first writes `intruder` to its path through the backend beneath.
    mod intruding {
        use std::sync::Arc;

        use opendal::raw::*;
        use opendal::{Buffer, Capability, Metadata, OperationContext, Result};

        #[derive(Debug)]
        pub struct Intruding;

        impl Layer for Intruding {
            fn apply_service(&self, inner: Servicer) -> Servicer {
                Arc::new(IntrudingService { inner })
            }
        }

        #[derive(Debug)]
        struct IntrudingService {
            inner: Servicer,
        }

        impl Service for IntrudingService {
            type Reader = oio::Reader;
            type Writer = IntrudingWriter;
            type Lister = oio::Lister;
            type Deleter = oio::Deleter;
            type Copier = oio::Copier;
            type Composer = oio::Composer;

            fn info(&self) -> ServiceInfo {
                self.inner.info()
            }

            fn capability(&self) -> Capability {
                self.inner.capability()
            }

            async fn create_dir(
                &self,
                ctx: &OperationContext,
                path: &str,
                args: OpCreateDir,
            ) -> Result<RpCreateDir> {
                self.inner.create_dir(ctx, path, args).await
            }

            async fn stat(
                &self,
                ctx: &OperationContext,
                path: &str,
                args: OpStat,
            ) -> Result<RpStat> {
                self.inner.stat(ctx, path, args).await
            }

            fn read(
                &self,
                ctx: &OperationContext,
                path: &str,
                args: OpRead,
            ) -> Result<oio::Reader> {
                self.inner.read(ctx, path, args)
            }

            fn write(
                &self,
                ctx: &OperationContext,
                path: &str,
                args: OpWrite,
            ) -> Result<IntrudingWriter> {
                Ok(IntrudingWriter {
                    inner: self.inner.write(ctx, path, args)?,
                    service: self.inner.clone(),
                    ctx: ctx.clone(),
                    path: path.to_owned(),
                })
            }

            fn delete(&self, ctx: &OperationContext) -> Result<oio::Deleter> {
                self.inner.delete(ctx)
            }

            fn list(
                &self,
                ctx: &OperationContext,
                path: &str,
                args: OpList,
            ) -> Result<oio::Lister> {
                self.inner.list(ctx, path, args)
            }

            fn copy(
                &self,
                ctx: &OperationContext,
                from: &str,
                to: &str,
                args: OpCopy,
            ) -> Result<oio::Copier> {
                self.inner.copy(ctx, from, to, args)
            }

            async fn rename(
                &self,
                ctx: &OperationContext,
                from: &str,
                to: &str,
                args: OpRename,
            ) -> Result<RpRename> {
                self.inner.rename(ctx, from, to, args).await
            }

            async fn presign(
                &self,
                ctx: &OperationContext,
                path: &str,
                args: OpPresign,
            ) -> Result<RpPresign> {
                self.inner.presign(ctx, path, args).await
            }
        }

        pub struct IntrudingWriter {
            inner: oio::Writer,
            service: Servicer,
            ctx: OperationContext,
            path: String,
        }

        impl oio::Write for IntrudingWriter {
            async fn write(&mut self, bs: Buffer) -> Result<()> {
                self.inner.write(bs).await
            }

            async fn close(&mut self) -> Result<Metadata> {
                let mut other = self.service.write(&self.ctx, &self.path, OpWrite::new())?;
                other.write(Buffer::from("intruder")).await?;
                other.close().await?;
                self.inner.close().await
            }

            async fn abort(&mut self) -> Result<()> {
                self.inner.abort().await
            }
        }
    }

    #[test]
    fn if_none_match_with_an_etag_is_not_implemented_on_writes() {
        // S3 supports only `*` here; accepting an ETag would promise a check that never runs.
        let other = tag("abc");
        let cond = Precondition {
            if_none_match: Some(&other),
            ..Default::default()
        };
        assert_eq!(
            *cond.validate().unwrap_err().code(),
            s3s::S3ErrorCode::NotImplemented
        );
        assert!(
            Precondition {
                if_none_match: Some(&ETagCondition::Any),
                ..Default::default()
            }
            .validate()
            .is_ok()
        );
    }
}

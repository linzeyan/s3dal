//! Changing a key: the steps PutObject, CopyObject, CompleteMultipartUpload, DeleteObject and
//! DeleteObjects share. Every commit to a key happens under [`KeyLocks`], so a conditional
//! write or delete is checked against exactly the state it replaces (design.md "Conditional
//! writes"). Where the backend checks write conditions itself (`Profile::conditional_writes`),
//! a conditional write also carries its condition there, which holds against other processes.

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::io;
use std::sync::{Arc, PoisonError};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use opendal::options::{ComposeOptions, WriteOptions};
use opendal::{Buffer, Metadata, Operator, Writer};
use s3s::dto::{ETag, ETagCondition, Timestamp};
use s3s::{S3Error, S3Result, s3_error};
use tokio::sync::{Mutex, MutexGuard};

use super::tier::{self, Removals};
use super::upload::{BodyCheck, Verified};
use super::version::{self, Versioning};
use super::{S3Dal, bucket_state, naming, precondition};
use crate::backend::without_stalling;
use crate::error::{Ctx, io_kind, map};
use crate::events::Usage;
use crate::meta::{self, Attrs, Computed, ObjectMeta, Stub};
use crate::metrics::{TierStats, WriteOrigin};
use crate::tier::Tier;

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
    pub(super) fn is_empty(&self) -> bool {
        self.if_match.is_none() && self.last_modified.is_none() && self.size.is_none()
    }

    /// `current` is the object the key holds now, `None` when it holds none.
    pub(super) fn check(&self, current: Option<&ObjectMeta>) -> S3Result<()> {
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
pub(super) async fn stat_existing(dal: &S3Dal, path: &str) -> S3Result<Option<Metadata>> {
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
    /// The bucket's state when the write began, which decides both the new version's id and
    /// what happens to the version it replaces.
    pub versioning: Versioning,
    /// The new version's id, chosen before the write opens and stored with it (C1). `None` for
    /// the `null` version.
    pub version_id: Option<String>,
    /// The bucket's usage is counted (it has a quota), so the commit finds out what it replaces.
    pub tracked: bool,
    /// What the commit replaced, found under the key's lock (see [`Self::archive`]).
    replaced: std::sync::Mutex<Usage>,
    /// The tier objects of the cold objects the commit replaced, removed once it committed.
    removals: Removals,
}

impl<'a> Dest<'a> {
    /// The bucket must already be known to exist and the key to be valid.
    pub(super) fn versioned(
        dal: &'a S3Dal,
        bucket: &'a str,
        key: &'a str,
        versioning: Versioning,
        version_id: Option<String>,
        tracked: bool,
    ) -> Self {
        Self {
            dal,
            bucket,
            key,
            path: naming::object_path(bucket, key),
            versioning,
            version_id,
            tracked,
            replaced: std::sync::Mutex::default(),
            removals: Removals::default(),
        }
    }

    /// The destination of a write, with the bucket's versioning state read and, where it is
    /// enabled, the new version's id chosen. Folder markers have no versions.
    pub(super) async fn for_write(dal: &'a S3Dal, bucket: &'a str, key: &'a str) -> S3Result<Self> {
        let versioning = version::state(dal, bucket).await?;
        let tracked = bucket_state::tracked(dal, bucket).await?;
        if versioning == Versioning::Unversioned || key.ends_with('/') {
            return Ok(Self::versioned(dal, bucket, key, versioning, None, tracked));
        }
        version::check_archivable(dal, bucket, key)?;
        let version_id = (versioning == Versioning::Enabled).then(version::new_id);
        Ok(Self::versioned(
            dal, bucket, key, versioning, version_id, tracked,
        ))
    }

    /// What the committed write of `size` bytes changed in the bucket's usage, where it is
    /// counted: a new object, less whatever the commit replaced. A folder marker holds nothing.
    pub(super) fn usage(&self, size: u64) -> Option<Usage> {
        if !self.tracked {
            return None;
        }
        if self.key.ends_with('/') {
            return Some(Usage::default());
        }
        let replaced = *self.replaced.lock().unwrap_or_else(PoisonError::into_inner);
        Some(Usage::object(size).minus(replaced))
    }

    /// The version the write made, as an event names it: `null` in a suspended bucket, none in
    /// one that never had versioning.
    pub(super) fn event_version(&self) -> Option<String> {
        match self.versioning {
            Versioning::Unversioned => None,
            _ => Some(
                self.version_id
                    .clone()
                    .unwrap_or_else(|| version::NULL.to_owned()),
            ),
        }
    }

    /// Stores the new version's id with what `opts` writes.
    pub(super) fn stamp(&self, opts: &mut WriteOptions) {
        if let Some(id) = &self.version_id {
            meta::set_version_id(&self.dal.profile, opts, id);
        }
    }

    /// `x-amz-version-id` for the response: only a new version's id. A write in a Suspended
    /// bucket replaces the `null` version and, as on AWS (ceph's
    /// `test_versioning_bucket_atomic_upload_return_version_id`), names no version.
    pub(super) fn response_version_id(&self) -> Option<String> {
        self.version_id.clone()
    }

    /// Keeps the version the write is about to replace (see [`version::archive`]). Called under
    /// the key's lock, right before the commit. In a counted bucket it also notes what the write
    /// replaces: in an unversioned one the current object, which takes a stat here, the only
    /// place where the answer cannot change before the commit. With tiers configured that stat
    /// is taken in every bucket, since a replaced cold object leaves a tier object to remove; a
    /// server without tiers pays nothing more.
    pub(super) async fn archive(&self) -> S3Result<()> {
        // A commit that failed after an earlier archive must not remove what it then kept.
        self.removals.clear();
        let replaced = if self.versioning != Versioning::Unversioned {
            version::archive(
                self.dal,
                self.bucket,
                self.key,
                &self.path,
                self.versioning,
                self.tracked,
                &self.removals,
            )
            .await?
        } else if (self.tracked || self.dal.has_tiers()) && !self.key.ends_with('/') {
            match stat_existing(self.dal, &self.path)
                .await?
                .filter(Metadata::is_file)
            {
                Some(m) => {
                    let obj = meta::resolve(&self.dal.profile, &m);
                    self.removals.note(self.bucket, self.key, &obj);
                    Usage::object(obj.content_length)
                }
                None => Usage::default(),
            }
        } else {
            Usage::default()
        };
        *self.replaced.lock().unwrap_or_else(PoisonError::into_inner) = replaced;
        Ok(())
    }

    /// Removes the tier objects of the cold objects the committed write replaced: after the
    /// commit, so a crash in between leaves an orphan, never a stub naming nothing. Commits that
    /// do not go through [`Self::commit`] call it once they succeeded.
    pub(super) async fn remove_replaced(&self) {
        self.removals.run(self.dal).await;
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
                Some(usize::try_from(self.dal.settings().etag_buffer_limit).unwrap_or(usize::MAX));
        }
    }

    /// Opens a writer for the key. Where the backend checks conditions, `cond` is checked now
    /// and the writer carries it (see [`Self::expected`]); [`Self::commit`] checks it either way.
    pub(super) async fn open(
        &self,
        mut opts: WriteOptions,
        cond: Precondition<'_>,
    ) -> S3Result<Writer> {
        self.stamp(&mut opts);
        if let Some(expected) = self.expected(cond).await? {
            expected.apply(&mut opts);
        }
        self.dal
            .op
            .writer_options(&self.path, chunked(&self.dal.op, opts))
            .await
            .map_err(write_err)
    }

    /// Whether the write carries a condition to the backend, which checks it where the write
    /// lands: the client's, and for a version always one, so that a version another instance
    /// commits in between is not replaced before it was archived (versioning.md "V2").
    fn backend_checks(&self, cond: Precondition<'_>) -> bool {
        let versioned = self.versioning != Versioning::Unversioned && !self.key.ends_with('/');
        (!cond.is_empty() || versioned) && self.dal.profile.conditional_writes
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
            // A close that fails after the archive leaves the version both current and
            // archived; readers drop such a duplicate, and the next archive finds it there.
            match async {
                self.check(cond).await?;
                self.archive().await
            }
            .await
            {
                Ok(()) => without_stalling(&self.dal.op, w.close())
                    .await
                    .map_err(|err| commit_err(err, cond)),
                Err(err) => Err(err),
            }
        };
        if result.is_err() {
            self.abandon(&mut w).await;
        } else {
            self.remove_replaced().await;
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

    /// Streams `body` into a new object in `tier` and commits a stub naming it at the key
    /// (cold-tier.md "Writes into a tier"), verified with `check`, which must hash the MD5: the
    /// stub stores it as the ETag. The tier object is closed before the key's lock is taken, so
    /// a large body never holds it; the stub is the commit. A body that fails a check, a refused
    /// condition or a failed stub removes the tier object again and leaves the key as it was.
    /// Returns what was verified, the ETag and the Last-Modified the stub stores.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn write_cold<S>(
        &self,
        tier: &Tier,
        attrs: Attrs,
        mut body: S,
        check: &mut BodyCheck,
        trailer: impl Fn(&str) -> Option<String>,
        cond: Precondition<'_>,
        size: Option<u64>,
    ) -> S3Result<(Verified, String, SystemTime)>
    where
        S: Stream<Item = S3Result<Bytes>> + Unpin,
    {
        // Held to the state found now, as a hot write's condition is from its open.
        let expected = self.expected(cond).await?;
        // What is known before the body: the tier object describes itself as far as C1 lets it.
        let known = Computed {
            md5_hex: check.content_md5().map(str::to_owned),
            checksum: check.declared_checksum(),
            parts: None,
        };
        let opts = meta::write_options(&tier.profile, attrs.clone(), known);
        let mut w = self.open_cold(tier, opts, size).await?;
        let streamed = async {
            while let Some(chunk) = body.next().await {
                let chunk = chunk?;
                check.update(&chunk)?;
                w.write(chunk).await?;
            }
            check.finish(trailer)
        }
        .await;
        let verified = match streamed {
            Ok(verified) => verified,
            Err(err) => {
                w.abort().await;
                return Err(err);
            }
        };
        let closed = w.close().await?;

        let etag = verified.md5_hex.clone().unwrap_or_default();
        let computed = Computed {
            md5_hex: Some(etag.clone()),
            checksum: verified.checksum.clone(),
            parts: None,
        };
        let size = check.received();
        let modified = self
            .commit_cold(closed, attrs, computed, size, &etag, expected, cond)
            .await?;
        Ok((verified, etag, modified))
    }

    /// Opens a writer for a new object in `tier` that will hold the key's bytes, `opts`
    /// describing it under the tier's Profile. Every write gets a new id, so a tier object is
    /// written once and never changes.
    pub(super) async fn open_cold<'t>(
        &self,
        tier: &'t Tier,
        mut opts: WriteOptions,
        size: Option<u64>,
    ) -> S3Result<ColdWriter<'t>> {
        let holder = tier::holder(self.bucket, self.key);
        version::check_fits(&tier.op, &holder, "to keep in its tier")?;
        let id = tier::new_id();
        let path = format!("{holder}{id}");
        if let Some(version) = &self.version_id {
            meta::set_version_id(&tier.profile, &mut opts, version);
        }
        opts.chunk = size.and_then(|n| chunk_for(&tier.op, n));
        let stats = self.dal.metrics().tier(&tier.name);
        let w = match tier.op.writer_options(&path, chunked(&tier.op, opts)).await {
            Ok(w) => w,
            Err(err) => {
                stats.failed("write", err.kind().into_static());
                return Err(write_err(err));
            }
        };
        Ok(ColdWriter {
            object: ColdObject { tier, id, path },
            w,
            stats,
        })
    }

    /// Commits a stub naming `closed` at the key, as [`Self::commit`] commits a write: the
    /// object of `size` bytes, with `attrs` and `computed`, under `etag`. A stub that does not
    /// commit removes the tier object again, so the key stays as it was; one whose failure is
    /// ambiguous keeps it (see [`remove_unless_named`]). Returns the Last-Modified the stub
    /// stores.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn commit_cold(
        &self,
        closed: ColdObject<'_>,
        attrs: Attrs,
        computed: Computed,
        size: u64,
        etag: &str,
        expected: Option<Expected>,
        cond: Precondition<'_>,
    ) -> S3Result<SystemTime> {
        let ColdObject { tier, id, path } = closed;
        let modified = SystemTime::now();
        let mut opts = meta::write_options(&self.dal.profile, attrs, computed);
        meta::set_stub(
            &self.dal.profile,
            &mut opts,
            &Stub {
                tier: &tier.name,
                id: &id,
                size,
                etag,
                last_modified: modified,
            },
        );
        self.stamp(&mut opts);
        if let Err(err) = self.commit_stub(opts, expected, cond).await {
            remove_unless_named(self.dal, &self.path, tier, &id, &path).await;
            return Err(err);
        }
        self.dal
            .metrics()
            .tier(&tier.name)
            .wrote(WriteOrigin::Request, size);
        Ok(modified)
    }

    /// Writes the stub `opts` describes at the key, as [`Self::commit`] commits a write.
    async fn commit_stub(
        &self,
        mut opts: WriteOptions,
        expected: Option<Expected>,
        cond: Precondition<'_>,
    ) -> S3Result<()> {
        #[cfg(unix)]
        if super::fs::is_fs(&self.dal.op) {
            return self.commit_stub_on_fs(&opts, cond).await;
        }
        if let Some(expected) = expected {
            expected.apply(&mut opts);
        }
        let w = self
            .dal
            .op
            .writer_options(&self.path, opts)
            .await
            .map_err(write_err)?;
        self.commit(w, cond).await.map(drop)
    }

    /// The stub on fs: an empty file given its xattrs and an fsync in `.s3dal/tmp/`, renamed
    /// into place under the key's lock, as Complete commits on fs. OpenDAL's fs writer renames
    /// first and sets the xattrs after, and a GET in between would answer an empty 200.
    #[cfg(unix)]
    async fn commit_stub_on_fs(&self, opts: &WriteOptions, cond: Precondition<'_>) -> S3Result<()> {
        use super::fs;

        let (tmp, target) = stub_paths(self.dal, &self.path);
        let made = make_stub_file(&tmp, &target, opts).await;
        let committed = match made {
            Ok(()) => {
                let _guard = self.dal.locks.lock(&self.path).await;
                match async {
                    self.check(cond).await?;
                    self.archive().await
                }
                .await
                {
                    Ok(()) => {
                        let tmp = tmp.clone();
                        fs::blocking(move || fs::rename_into_place(&tmp, &target))
                            .await
                            .map_err(write_err)
                    }
                    Err(err) => Err(err),
                }
            }
            Err(err) => Err(err),
        };
        if let Err(err) = committed {
            let _ = fs::blocking(move || Ok(std::fs::remove_file(&tmp))).await;
            prune_empty_parents(self.dal, self.bucket, self.key).await;
            return Err(err);
        }
        self.remove_replaced().await;
        Ok(())
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
/// successfully. The bucket must already be known to exist. What was removed, `None` when
/// nothing was (see [`delete_unpruned`]).
pub(super) async fn delete_key(
    dal: &S3Dal,
    bucket: &str,
    key: &str,
    cond: DeleteCondition<'_>,
) -> S3Result<Option<Usage>> {
    let removals = Removals::default();
    let removed = delete_unpruned(dal, bucket, key, cond, &removals).await?;
    if removed.is_some() {
        removals.run(dal).await;
        prune_empty_parents(dal, bucket, key).await;
    }
    Ok(removed)
}

/// [`delete_key`] without pruning the parents, so a caller deleting many keys prunes each
/// directory once (see [`prune_empty_parents`]). What was deleted: an object, or a folder
/// marker, which holds nothing; `None` when nothing was. A cold object's tier object is noted
/// in `removals`, for the caller to remove once the stub is gone.
pub(super) async fn delete_unpruned(
    dal: &S3Dal,
    bucket: &str,
    key: &str,
    cond: DeleteCondition<'_>,
    removals: &Removals,
) -> S3Result<Option<Usage>> {
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
                return Ok(None);
            }
        }
        match found {
            Some(m) if key.ends_with('/') => {
                (m.is_dir() && remove_empty_dir(dal, &path).await?).then(Usage::default)
            }
            Some(m) if m.is_file() => {
                dal.op.delete(&path).await.map_err(|e| map(e, Ctx::Other))?;
                let obj = meta::resolve(&dal.profile, &m);
                removals.note(bucket, key, &obj);
                Some(Usage::object(obj.content_length))
            }
            _ => None,
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

/// Where a stub for `path` is made on fs, a fresh file in `.s3dal/tmp/`, and where it is renamed
/// to.
#[cfg(unix)]
pub(super) fn stub_paths(dal: &S3Dal, path: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    let root = std::path::PathBuf::from(dal.op.info().root());
    let tmp = root
        .join(crate::backend::RESERVED_ROOT)
        .join("tmp")
        .join(uuid::Uuid::new_v4().to_string());
    (tmp, root.join(path))
}

/// Makes a stub on fs at `tmp`: an empty file given the xattrs `opts` describes and an fsync, so
/// that renamed to `target`, whose parents this makes, it never shows there without them.
#[cfg(unix)]
pub(super) async fn make_stub_file(
    tmp: &std::path::Path,
    target: &std::path::Path,
    opts: &WriteOptions,
) -> S3Result<()> {
    use super::fs;

    let user_metadata = opts.user_metadata.clone().unwrap_or_default();
    let (tmp, target) = (tmp.to_owned(), target.to_owned());
    fs::blocking(move || {
        fs::make_parents(&tmp)?;
        fs::make_parents(&target)?;
        fs::assemble(&tmp, &[], &user_metadata).map_err(|err| match err {
            fs::AssembleError::Source(_, err) | fs::AssembleError::Write(err) => err,
        })
    })
    .await
    .map(drop)
    .map_err(write_err)
}

/// A new tier object being written; see [`Dest::open_cold`]. Ends in [`Self::close`] or
/// [`Self::abort`].
pub(super) struct ColdWriter<'t> {
    object: ColdObject<'t>,
    w: Writer,
    stats: Arc<TierStats>,
}

/// A closed tier object that no stub names yet; [`Dest::commit_cold`] makes one name it.
pub(super) struct ColdObject<'t> {
    pub tier: &'t Tier,
    pub id: String,
    pub path: String,
}

impl<'t> ColdWriter<'t> {
    pub(super) async fn write(&mut self, chunk: impl Into<Buffer>) -> S3Result<()> {
        self.w.write(chunk).await.map_err(write_err)
    }

    /// Discards a write that failed or did not verify.
    pub(super) async fn abort(mut self) {
        if let Err(err) = self.w.abort().await {
            tracing::warn!(
                tier = self.object.tier.name,
                path = self.object.path,
                error = %err,
                "cannot abort a tier write; the sweep removes what remains"
            );
        }
    }

    /// Closes the tier object, outside any key lock: a large body never holds one. A close that
    /// fails removes what it may have left.
    pub(super) async fn close(mut self) -> S3Result<ColdObject<'t>> {
        let tier = self.object.tier;
        if let Err(err) = without_stalling(&tier.op, self.w.close()).await {
            self.stats.failed("write", err.kind().into_static());
            remove_tier_object(tier, &self.object.path).await;
            return Err(write_err(err));
        }
        Ok(self.object)
    }
}

/// Whether the object at `stub` is a stub naming the tier object `id` of `tier`.
pub(super) async fn stub_names(dal: &S3Dal, stub: &str, tier: &Tier, id: &str) -> S3Result<bool> {
    Ok(stat_existing(dal, stub)
        .await?
        .filter(Metadata::is_file)
        .is_some_and(|m| {
            meta::resolve(&dal.profile, &m)
                .tier
                .is_some_and(|t| t.name == tier.name && t.id.as_deref() == Some(id))
        }))
}

/// After a stub write at `stub` that failed, removes the tier object `id` at `path` unless the
/// stub landed after all. The error does not say: a close that timed out or a reply lost on the
/// way may follow a write the backend kept, and removing the tier object then would leave the
/// key naming bytes that are gone, the object it replaced included. A stat that fails cannot
/// tell either, so the tier object stays; if no stub names it, the sweep removes it.
pub(super) async fn remove_unless_named(
    dal: &S3Dal,
    stub: &str,
    tier: &Tier,
    id: &str,
    path: &str,
) {
    match stub_names(dal, stub, tier, id).await {
        Ok(false) => remove_tier_object(tier, path).await,
        Ok(true) => tracing::warn!(
            stub,
            tier = tier.name,
            path,
            "a stub write reported a failure but landed; its tier object stays"
        ),
        Err(err) => tracing::warn!(
            stub,
            tier = tier.name,
            path,
            error = %err,
            "cannot tell whether a failed stub write landed; its tier object stays for the sweep"
        ),
    }
}

/// Removes a tier object no stub names, after a write into a tier failed. Best effort: what
/// remains is an orphan, which the sweep removes.
pub(super) async fn remove_tier_object(tier: &Tier, path: &str) {
    if let Err(err) = tier.op.delete(path).await {
        tracing::warn!(
            tier = tier.name,
            path,
            error = %err,
            "cannot remove a tier object no stub names; the sweep will"
        );
    }
}

pub(super) fn write_err(err: opendal::Error) -> S3Error {
    map(err, Ctx::ObjectWrite)
}

/// The error of a commit the backend refused. A refused condition the client did not send is
/// the one a versioned write carries (see `Dest::backend_checks`): another instance replaced
/// the key meanwhile. Nothing was lost and a retry will archive that version first, so it is
/// answered as a retryable conflict, which SDKs repeat on their own, instead of a 412 the
/// client could not explain.
pub(super) fn commit_err(err: opendal::Error, cond: Precondition<'_>) -> S3Error {
    if err.kind() == opendal::ErrorKind::ConditionNotMatch && cond.is_empty() {
        conflict()
    } else {
        write_err(err)
    }
}

/// Another instance sharing the backend changed the key while this one was writing it.
pub(super) fn conflict() -> S3Error {
    s3_error!(
        SlowDown,
        "Another write to this key landed at the same time; please retry"
    )
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
            version_id: None,
            tier: None,
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
        let err = Dest::versioned(&dal, "b", "k", Versioning::Unversioned, None, false)
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

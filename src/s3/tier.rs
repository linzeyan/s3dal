//! What requests do with cold objects (cold-tier.md): find a stub's tier object and read it,
//! and remove tier objects once no stub names them. Writes into a tier are `store::Dest`'s.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Mutex, PoisonError};

use bytes::Bytes;
use futures::{Stream, StreamExt, stream};
use opendal::options::WriteOptions;
use opendal::{ErrorKind, Metadata, Operator};
use s3s::{S3Error, S3Result, s3_error};

use super::copy::Source;
use super::store::{self, Dest};
use super::upload::BodyCheck;
use super::version::{self, Versioning};
use super::{S3Dal, naming};
use crate::backend::{RESERVED_ROOT, without_stalling};
use crate::error::{Ctx, map};
use crate::meta::{self, Computed, ObjectMeta, TierRef};
use crate::metrics::WriteOrigin;
use crate::tier::Tier;

/// Where a tier keeps the objects of `bucket`.
pub(super) fn bucket_dir(bucket: &str) -> String {
    format!("{RESERVED_ROOT}tier/{bucket}/")
}

/// The directory that holds `key`'s tier objects, one per write into a tier.
pub(super) fn holder(bucket: &str, key: &str) -> String {
    format!("{}{}!!/", bucket_dir(bucket), version::escape(key))
}

/// A fresh tier object id. Every write into a tier gets one, so a tier object is written once
/// and never changes: reads need no pinning, and two stubs never share one.
pub(super) fn new_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

impl S3Dal {
    pub(crate) fn has_tiers(&self) -> bool {
        !self.tiers.is_empty()
    }

    pub(crate) fn tiers(&self) -> &[Tier] {
        &self.tiers
    }

    pub(super) fn tier_named(&self, name: &str) -> Option<&Tier> {
        self.tiers.iter().find(|t| t.name == name)
    }

    /// The tier a write naming `class` goes to. Any other class stays advisory: stored hot,
    /// reported STANDARD (cold-tier.md, open question 4).
    pub(super) fn tier_for_class(&self, class: Option<&str>) -> Option<&Tier> {
        let class = class?;
        self.tiers.iter().find(|t| t.storage_class == class)
    }

    /// The storage class S3 reports for `obj`: its tier's for a stub, `None` for STANDARD, which
    /// AWS leaves out of GET and HEAD.
    pub(super) fn storage_class(&self, obj: &ObjectMeta) -> Option<&str> {
        let tier = self.tier_named(&obj.tier.as_ref()?.name)?;
        Some(tier.storage_class.as_str())
    }
}

/// A cold object's bytes: its tier object, and the stub that named it.
#[derive(Clone)]
pub(super) struct Cold {
    pub tier: Tier,
    /// The tier object's path in the tier.
    pub path: String,
    pub id: String,
    /// The backend path of the stub that was stat'ed: the key's, or an archived version's.
    pub stub: String,
}

/// Where `obj`, read from `stub` for `key`, keeps its bytes; `None` for a hot object. A stub
/// that cannot be followed, damaged or naming a tier this server does not have, fails the read
/// with a 500: an empty 200 would hand out a wrong object, and `NoSuchKey` would make sync
/// tools delete the key elsewhere.
pub(super) fn locate(
    dal: &S3Dal,
    bucket: &str,
    key: &str,
    stub: &str,
    obj: &ObjectMeta,
) -> S3Result<Option<Cold>> {
    let Some(TierRef { name, id }) = &obj.tier else {
        return Ok(None);
    };
    let Some(id) = id else {
        tracing::error!(
            path = stub,
            "a cold object's stub is damaged: its tier object id or size is missing or malformed"
        );
        return Err(s3_error!(InternalError));
    };
    let Some(tier) = dal.tier_named(name) else {
        tracing::error!(
            path = stub,
            tier = name,
            "a cold object's stub names a tier this server does not have"
        );
        return Err(s3_error!(InternalError));
    };
    Ok(Some(Cold {
        tier: tier.clone(),
        path: format!("{}{id}", holder(bucket, key)),
        id: id.clone(),
        stub: stub.to_owned(),
    }))
}

impl Cold {
    /// The bytes of `range`, streamed from the tier, the first chunk read before this returns:
    /// a tier object that is not there must fail the request with a status, not cut the body of
    /// a 200 short.
    pub(super) async fn stream(
        &self,
        dal: &S3Dal,
        range: Range<u64>,
    ) -> S3Result<impl Stream<Item = std::io::Result<Bytes>> + Send + 'static + use<>> {
        let opened = async {
            let reader = self.tier.op.reader(&self.path).await?;
            reader
                .into_bytes_stream(range)
                .await
                .map_err(std::io::Error::from)
                .map_err(opendal_error)
        };
        let mut chunks = match opened.await {
            Ok(chunks) => chunks,
            Err(err) => return Err(self.failed(dal, err).await),
        };
        let first = match chunks.next().await {
            Some(Err(err)) => return Err(self.failed(dal, opendal_error(err)).await),
            first => first,
        };
        let stats = dal.metrics().tier(&self.tier.name);
        stats.read();
        Ok(stream::iter(first).chain(chunks).inspect(move |chunk| {
            if let Ok(bytes) = chunk {
                stats.read_bytes(bytes.len() as u64);
            }
        }))
    }

    /// The bytes of `range`, whole.
    pub(super) async fn read(&self, dal: &S3Dal, range: Range<u64>) -> S3Result<Bytes> {
        match self.tier.op.read_with(&self.path).range(range).await {
            Ok(buf) => {
                let stats = dal.metrics().tier(&self.tier.name);
                stats.read();
                stats.read_bytes(buf.len() as u64);
                Ok(buf.to_bytes())
            }
            Err(err) => Err(self.failed(dal, err).await),
        }
    }

    /// The error for a tier read that failed. A tier object that is not there was either
    /// removed by a write that replaced the stub since it was stat'ed, which a retry gets past
    /// (503, as a pinned read of a replaced object fails today), or lost behind s3dal's back
    /// while the stub still names it: a 500, logged, never `NoSuchKey`.
    async fn failed(&self, dal: &S3Dal, err: opendal::Error) -> S3Error {
        let stats = dal.metrics().tier(&self.tier.name);
        if err.kind() != ErrorKind::NotFound {
            stats.failed("read", err.kind().into_static());
            return map(err, Ctx::ObjectRead);
        }
        let still = match store::stub_names(dal, &self.stub, &self.tier, &self.id).await {
            Ok(still) => still,
            Err(err) => return err,
        };
        if !still {
            return store::conflict();
        }
        tracing::error!(
            path = self.stub,
            tier = self.tier.name,
            tier_path = self.path,
            "a cold object's tier object is missing"
        );
        stats.failed("read", "missing");
        s3_error!(InternalError)
    }
}

/// The OpenDAL error inside an io error from a reader's byte stream, which wraps it.
pub(super) fn opendal_error(err: std::io::Error) -> opendal::Error {
    let kind = if err.kind() == std::io::ErrorKind::NotFound {
        ErrorKind::NotFound
    } else {
        ErrorKind::Unexpected
    };
    match err
        .into_inner()
        .map(|inner| inner.downcast::<opendal::Error>())
    {
        Some(Ok(err)) => *err,
        Some(Err(other)) => opendal::Error::new(kind, other.to_string()),
        None => opendal::Error::new(kind, "tier read failed"),
    }
}

/// Tier objects whose stubs are gone, removed once the change that dropped them committed:
/// stub first, tier object second, so no crash leaves a stub naming nothing. A tier object
/// whose removal fails stays as an orphan, which the sweep removes.
#[derive(Default)]
pub(super) struct Removals {
    /// By tier name: the tier objects' paths.
    paths: Mutex<HashMap<String, Vec<String>>>,
}

impl Removals {
    /// Notes the tier object of `obj`, read from `key`, if it is a cold object.
    pub(super) fn note(&self, bucket: &str, key: &str, obj: &ObjectMeta) {
        if let Some(TierRef { name, id: Some(id) }) = &obj.tier {
            self.paths
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(name.clone())
                .or_default()
                .push(format!("{}{id}", holder(bucket, key)));
        }
    }

    /// Forgets what was noted, for a change that did not commit.
    pub(super) fn clear(&self) {
        self.paths
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }

    /// Takes over what `other` noted, for a change that committed as part of a batch.
    pub(super) fn absorb(&self, other: Self) {
        let other = other
            .paths
            .into_inner()
            .unwrap_or_else(PoisonError::into_inner);
        let mut paths = self.paths.lock().unwrap_or_else(PoisonError::into_inner);
        for (tier, noted) in other {
            paths.entry(tier).or_default().extend(noted);
        }
    }

    /// Removes what was noted, in one batch per tier. Best effort: the change is committed, so a
    /// failure here only leaves an orphan.
    pub(super) async fn run(&self, dal: &S3Dal) {
        let noted = std::mem::take(&mut *self.paths.lock().unwrap_or_else(PoisonError::into_inner));
        for (name, mut paths) in noted {
            let Some(tier) = dal.tier_named(&name) else {
                tracing::warn!(
                    tier = name,
                    ?paths,
                    "cannot remove tier objects of a tier this server does not have"
                );
                continue;
            };
            paths.sort_unstable();
            paths.dedup();
            if let Err(err) = tier.op.delete_iter(paths.clone()).await {
                tracing::warn!(tier = name, error = %err, "cannot remove tier objects; the sweep will");
                dal.metrics()
                    .tier(&name)
                    .failed("delete", err.kind().into_static());
                continue;
            }
            if tier.profile.real_dirs {
                for path in &paths {
                    prune(&tier.op, path).await;
                }
            }
        }
    }
}

/// Removes the empty directories above a removed tier object, up to the tier's `.s3dal/tier/`:
/// a key's holder holds nothing once its last tier object goes.
pub(super) async fn prune(op: &Operator, path: &str) {
    let top = format!("{RESERVED_ROOT}tier/");
    let mut dir = path;
    while let Some(i) = dir.trim_end_matches('/').rfind('/') {
        dir = &dir[..=i];
        if dir.len() <= top.len() || op.delete(dir).await.is_err() {
            return;
        }
    }
}

/// Why a lifecycle transition was given up on, as `s3dal_tier_transition_skipped_total` labels it.
/// Each leaves the object where it was, for the next day's run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SkipReason {
    /// The key's tier object path does not fit the target tier's limits.
    KeyTooLong,
    /// The object changed while it was being moved.
    Changed,
    /// Reading it, or writing into the tier, failed.
    Error,
}

impl SkipReason {
    pub(crate) const ALL: [Self; 3] = [Self::KeyTooLong, Self::Changed, Self::Error];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::KeyTooLong => "key-too-long",
            Self::Changed => "changed",
            Self::Error => "error",
        }
    }
}

/// What a lifecycle move did.
pub(super) enum Moved {
    /// Moved: the object as it was, and still is but for where its bytes are.
    Done(Box<ObjectMeta>),
    /// Nothing to move: the object is gone, empty, or in the target tier already.
    Nothing,
    Skipped(SkipReason),
}

/// Moves the object at `path`, `key`'s or an archived version's entry, into `target`
/// (cold-tier.md "Moving an object"): its bytes are streamed into a new tier object and verified
/// as a copy source is, then a stub naming it replaces the object in place, keeping its ETag,
/// Last-Modified, headers, checksums and version id. A transition is not a new version, so
/// nothing is archived, and its usage does not change. From another tier, the old tier object
/// is removed once the stub names the new one.
pub(super) async fn move_object(
    dal: &S3Dal,
    bucket: &str,
    key: &str,
    path: &str,
    target: &Tier,
) -> Moved {
    match try_move(dal, bucket, key, path, target).await {
        Ok(moved) => moved,
        Err(err) => {
            tracing::warn!(bucket, key, path, tier = target.name, error = %err, "cannot move an object into its tier");
            Moved::Skipped(SkipReason::Error)
        }
    }
}

async fn try_move(
    dal: &S3Dal,
    bucket: &str,
    key: &str,
    path: &str,
    target: &Tier,
) -> S3Result<Moved> {
    let Some(m) = store::stat_existing(dal, path)
        .await?
        .filter(Metadata::is_file)
    else {
        return Ok(Moved::Nothing);
    };
    let obj = meta::resolve(&dal.profile, &m);
    let there = obj.tier.as_ref().is_some_and(|t| t.name == target.name);
    // A damaged stub resolves as empty too: it has no bytes to move.
    if obj.content_length == 0 || there {
        return Ok(Moved::Nothing);
    }
    if obj.last_modified.is_none() {
        // The stub stores it, and one without it would read as an empty hot object.
        return Err(s3_error!(InternalError, "the object has no Last-Modified"));
    }
    if version::check_fits(&target.op, &holder(bucket, key), "to keep in its tier").is_err() {
        return Ok(Moved::Skipped(SkipReason::KeyTooLong));
    }
    let source = Source::stated(dal, bucket, key, path, &m)?;
    let len = obj.content_length;
    let mut check = BodyCheck::new(&source.expected(0..len, None)?, false)?;
    // The tier object describes itself, the version id included (C1): every value is known.
    let dest = Dest::versioned(
        dal,
        bucket,
        key,
        Versioning::Unversioned,
        obj.version_id.clone(),
        false,
    );
    let computed = Computed {
        md5_hex: Some(obj.etag.clone()),
        checksum: obj.checksum.clone(),
        parts: obj.parts,
    };
    let opts = meta::write_options(&target.profile, obj.attrs.clone(), computed);
    let mut w = dest.open_cold(target, opts, Some(len)).await?;
    let streamed = async {
        let mut body = source.read(dal, 0..len).await?;
        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            check.update(&chunk)?;
            w.write(chunk).await?;
        }
        check.finish(|_| None)
    }
    .await;
    if let Err(err) = streamed {
        w.abort().await;
        if unchanged(dal, path, &obj).await? {
            return Err(err);
        }
        return Ok(Moved::Skipped(SkipReason::Changed));
    }
    let closed = w.close().await?;

    let stub = ObjectMeta {
        tier: Some(TierRef {
            name: target.name.clone(),
            id: Some(closed.id.clone()),
        }),
        ..obj.clone()
    };
    let opts = meta::version_copy_options(&dal.profile, &stub, true);
    match commit_move(dal, bucket, key, path, &obj, opts).await {
        Ok(true) => {}
        // Refused under the lock: no stub names the tier object.
        Ok(false) => {
            store::remove_tier_object(target, &closed.path).await;
            return Ok(Moved::Skipped(SkipReason::Changed));
        }
        // The stub may have landed over the hot bytes; then the tier object holds the only copy.
        Err(err) => {
            store::remove_unless_named(dal, path, target, &closed.id, &closed.path).await;
            return Err(err);
        }
    }
    let removals = Removals::default();
    removals.note(bucket, key, &obj);
    removals.run(dal).await;
    dal.metrics()
        .tier(&target.name)
        .wrote(WriteOrigin::Lifecycle, len);
    Ok(Moved::Done(Box::new(obj)))
}

/// Whether `path` still holds `was`, exactly as it was resolved.
async fn unchanged(dal: &S3Dal, path: &str, was: &ObjectMeta) -> S3Result<bool> {
    Ok(store::stat_existing(dal, path)
        .await?
        .filter(Metadata::is_file)
        .is_some_and(|m| meta::resolve(&dal.profile, &m) == *was))
}

/// Writes the stub `opts` describes over `path` under the key's lock, provided the object there
/// is still `was`; whether it was. Where the backend checks conditions the write also carries
/// the object stat'ed under the lock, which holds against another instance moving or writing the
/// key at the same time: of two moves only the first stub lands. On fs the stub is made beside
/// and renamed into place, as every stub there is.
async fn commit_move(
    dal: &S3Dal,
    bucket: &str,
    key: &str,
    path: &str,
    was: &ObjectMeta,
    mut opts: WriteOptions,
) -> S3Result<bool> {
    #[cfg(unix)]
    let on_fs = if super::fs::is_fs(&dal.op) {
        let (tmp, target) = store::stub_paths(dal, path);
        store::make_stub_file(&tmp, &target, &opts).await?;
        Some((tmp, target))
    } else {
        None
    };
    let committed = {
        let _guard = dal.locks.lock(&naming::object_path(bucket, key)).await;
        async {
            let found = store::stat_existing(dal, path)
                .await?
                .filter(Metadata::is_file);
            let Some(found) = found.filter(|m| meta::resolve(&dal.profile, m) == *was) else {
                return Ok(false);
            };
            #[cfg(unix)]
            if let Some((tmp, target)) = &on_fs {
                let (tmp, target) = (tmp.clone(), target.clone());
                super::fs::blocking(move || super::fs::rename_into_place(&tmp, &target))
                    .await
                    .map_err(store::write_err)?;
                return Ok(true);
            }
            if dal.profile.conditional_writes {
                opts.if_not_changed = Some(found);
            }
            let mut w = dal
                .op
                .writer_options(path, opts)
                .await
                .map_err(store::write_err)?;
            match without_stalling(&dal.op, w.close()).await {
                Ok(_) => Ok(true),
                Err(err) if err.kind() == ErrorKind::ConditionNotMatch => Ok(false),
                Err(err) => Err(store::write_err(err)),
            }
        }
        .await
    };
    #[cfg(unix)]
    if let Some((tmp, _)) = on_fs
        && !matches!(committed, Ok(true))
    {
        let _ = super::fs::blocking(move || Ok(std::fs::remove_file(&tmp))).await;
    }
    committed
}

/// Removes what every tier keeps for `bucket`, for a bucket that is gone or new: no stub can
/// name any of it.
pub(super) async fn forget(dal: &S3Dal, bucket: &str) -> S3Result<()> {
    for tier in dal.tiers.iter() {
        match tier
            .op
            .delete_with(&bucket_dir(bucket))
            .recursive(true)
            .await
        {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => return Err(map(err, Ctx::Other)),
        }
    }
    Ok(())
}

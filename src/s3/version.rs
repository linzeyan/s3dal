//! S3 versioning, plan A of versioning.md: the current version of `b/k` stays where it always
//! was, and the versions it replaced are kept under `.s3dal/versions/b/<E(k)>!!/`, one file per
//! version or delete marker, named so that name order is newest first. A bucket that never had
//! versioning takes none of these paths.
//!
//! Every change to a key's versions runs under its key lock (`store::KeyLocks`), so within one
//! process archiving a version, committing its successor and writing a marker are one step to
//! every other request on the key. fs archives by hard link and promotes by rename; object
//! stores copy, server-side where the backend can (versioning.md "V2").

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use hyper::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use opendal::options::{ReadOptions, WriteOptions};
use opendal::raw::new_std_io_error;
use opendal::{ErrorKind, Metadata, Operator};
use s3s::dto::*;
use s3s::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};

use super::bucket_state;
use super::store::{self, DeleteCondition};
use super::tier::{self, Removals};
use super::{S3Dal, fs, naming, usage};
use crate::backend::{RESERVED_ROOT, without_stalling};
use crate::error::{Ctx, map};
use crate::events::{EventKind, Usage};
use crate::meta;

/// The id of the version written while a bucket had no versioning, or had it suspended.
pub(crate) const NULL: &str = "null";

/// What `.s3dal/versions/` holds per entry beyond its key: `<seq>-<id>`, 16 + 1 + 32 bytes,
/// and `.dm` for a marker.
const ENTRY_NAME_MAX: usize = 16 + 1 + 32 + 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Versioning {
    /// Never versioned: everything behaves as it did before versioning existed.
    Unversioned,
    Enabled,
    Suspended,
}

/// Per process: the clock archive entries are named by.
#[derive(Default)]
pub(crate) struct Sequence {
    /// Nanoseconds since the epoch of the last entry named; see [`Sequence::next_seq`].
    last_seq: AtomicU64,
}

impl Sequence {
    /// 16 hex digits of `u64::MAX` minus the time, so that ascending name order is newest
    /// first, the order ListObjectVersions reports a key's versions in. Strictly decreasing
    /// within the process, even when the clock is not.
    fn next_seq(&self) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX - 1));
        let last = self
            .last_seq
            .update(Ordering::Relaxed, Ordering::Relaxed, |last| {
                now.max(last.saturating_add(1))
            });
        let taken = now.max(last.saturating_add(1));
        format!("{:016x}", u64::MAX - taken)
    }
}

/// The bucket's versioning state, from its cached state (`bucket_state`): every write and
/// delete asks.
pub(super) async fn state(dal: &S3Dal, bucket: &str) -> S3Result<Versioning> {
    Ok(match bucket_state::get(dal, bucket).await?.versioning {
        None => Versioning::Unversioned,
        Some(bucket_state::Versioning::Enabled) => Versioning::Enabled,
        Some(bucket_state::Versioning::Suspended) => Versioning::Suspended,
    })
}

/// Removes the state, the usage count, the versions and the thumbnails a bucket of this name
/// left behind, so a bucket created under the name starts unversioned, private, without a quota
/// and empty. For CreateBucket of a new bucket, which finds what a crashed DeleteBucket left, and
/// DeleteBucket once the bucket holds no versions.
pub(super) async fn forget(dal: &S3Dal, bucket: &str) -> S3Result<()> {
    remove_tree(dal, &versions_dir(bucket)).await?;
    remove_tree(dal, &super::thumbnail::dir(bucket)).await?;
    tier::forget(dal, bucket).await?;
    usage::forget(dal, bucket).await?;
    bucket_state::remove(dal, bucket).await
}

async fn remove_tree(dal: &S3Dal, dir: &str) -> S3Result<()> {
    match dal.op.delete_with(dir).recursive(true).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(map(err, Ctx::Other)),
    }
}

/// Whether the bucket holds any archived version or delete marker; DeleteBucket refuses one
/// that does, as AWS does.
pub(super) async fn has_versions(dal: &S3Dal, bucket: &str) -> S3Result<bool> {
    let mut lister = match dal
        .op
        .lister_with(&versions_dir(bucket))
        .recursive(true)
        .await
    {
        Ok(lister) => lister,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(map(err, Ctx::Bucket)),
    };
    use futures::TryStreamExt;
    while let Some(entry) = lister.try_next().await.map_err(|e| map(e, Ctx::Bucket))? {
        if entry.metadata().is_file() {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) async fn get_bucket_versioning(
    dal: &S3Dal,
    req: S3Request<GetBucketVersioningInput>,
) -> S3Result<S3Response<GetBucketVersioningOutput>> {
    let bucket = &req.input.bucket;
    naming::require_bucket(dal, bucket).await?;
    // A bucket that never had versioning answers with no Status, as on AWS. Clients such as
    // rclone ask before deleting.
    let status = match state(dal, bucket).await? {
        Versioning::Unversioned => None,
        Versioning::Enabled => Some(BucketVersioningStatus::ENABLED),
        Versioning::Suspended => Some(BucketVersioningStatus::SUSPENDED),
    };
    Ok(S3Response::new(GetBucketVersioningOutput {
        status: status.map(BucketVersioningStatus::from_static),
        ..Default::default()
    }))
}

pub(super) async fn put_bucket_versioning(
    dal: &S3Dal,
    req: S3Request<PutBucketVersioningInput>,
) -> S3Result<S3Response<PutBucketVersioningOutput>> {
    let input = req.input;
    naming::require_bucket(dal, &input.bucket).await?;
    let conf = input.versioning_configuration;
    // A protection: accepted and not enforced, deletes would go through that should not.
    if conf
        .mfa_delete
        .as_ref()
        .is_some_and(|m| m.as_str() == MFADelete::ENABLED)
    {
        return Err(s3_error!(NotImplemented, "MFA delete is not supported"));
    }
    let wanted = match conf.status.as_ref().map(BucketVersioningStatus::as_str) {
        Some(BucketVersioningStatus::ENABLED) => bucket_state::Versioning::Enabled,
        Some(BucketVersioningStatus::SUSPENDED) => bucket_state::Versioning::Suspended,
        // Only `MfaDelete` `Disabled`, which changes nothing.
        None => return Ok(S3Response::new(PutBucketVersioningOutput::default())),
        Some(_) => {
            return Err(s3_error!(
                MalformedXML,
                "The XML you provided was not well-formed or did not validate against our published schema"
            ));
        }
    };
    if fs::is_fs(&dal.op) && !hard_links_work(dal).await? {
        return Err(s3_error!(
            NotImplemented,
            "versioning needs a filesystem with hard links, which this backend's root lacks"
        ));
    }
    bucket_state::update(dal, &input.bucket, |s| s.versioning = Some(wanted)).await?;
    Ok(S3Response::new(PutBucketVersioningOutput::default()))
}

/// On fs, archiving links the current version's file into the versions tree; a filesystem without
/// hard links (FAT, exFAT) would need a copy under the key lock, which versioning.md Open
/// question 4 refused.
async fn hard_links_work(dal: &S3Dal) -> S3Result<bool> {
    let root = PathBuf::from(dal.op.info().root());
    let dir = root.join(RESERVED_ROOT).join("tmp");
    let name = uuid::Uuid::new_v4().simple().to_string();
    let worked = fs::blocking(move || {
        std::fs::create_dir_all(&dir).map_err(new_std_io_error)?;
        let (file, link) = (
            dir.join(format!("{name}.link")),
            dir.join(format!("{name}.linked")),
        );
        std::fs::write(&file, b"").map_err(new_std_io_error)?;
        let linked = std::fs::hard_link(&file, &link);
        let _ = std::fs::remove_file(&link);
        let _ = std::fs::remove_file(&file);
        Ok(linked.is_ok())
    })
    .await
    .map_err(|err| map(err, Ctx::Other))?;
    Ok(worked)
}

/// A fresh version id: 32 lowercase hex digits, as long as AWS's. Not time-ordered: the order of
/// versions is the order in which they stopped being current (see [`Sequence::next_seq`]).
pub(crate) fn new_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

fn is_id(id: &str) -> bool {
    id == NULL || (id.len() == 32 && id.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')))
}

/// A version id a client sent. One s3dal can never have issued is refused as AWS refuses one it
/// cannot parse, rather than looked for.
pub(super) fn check_id(id: &str) -> S3Result<()> {
    if is_id(id) {
        Ok(())
    } else {
        Err(s3_error!(InvalidArgument, "Invalid version id specified"))
    }
}

/// The order-preserving escape of versioning.md "The versions tree": each tab, LF, CR, space
/// and `!` becomes `!` and the byte plus 0x40, every other byte stays. Every escape starts with
/// `!` (0x21), below every kept byte, so byte order and prefixes are kept, and `!!` never occurs.
pub(super) fn escape(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for c in key.chars() {
        match c {
            '\t' | '\n' | '\r' | ' ' | '!' => {
                out.push('!');
                out.push(char::from(c as u8 + 0x40));
            }
            c => out.push(c),
        }
    }
    out
}

/// The key [`escape`] turned into `escaped`; `None` for a name it cannot have written.
pub(super) fn unescape(escaped: &str) -> Option<String> {
    let mut out = String::with_capacity(escaped.len());
    let mut chars = escaped.chars();
    while let Some(c) = chars.next() {
        if c == '!' {
            match chars.next()? {
                c @ ('I' | 'J' | 'M' | '`' | 'a') => out.push(char::from(c as u8 - 0x40)),
                _ => return None,
            }
        } else {
            out.push(c);
        }
    }
    Some(out)
}

pub(super) fn versions_dir(bucket: &str) -> String {
    format!("{RESERVED_ROOT}versions/{bucket}/")
}

/// The directory that holds the archived versions and markers of `key`.
fn holder(bucket: &str, key: &str) -> String {
    format!("{}{}!!/", versions_dir(bucket), escape(key))
}

/// The paths larger than the key that versioning adds must fit where the key fits (versioning.md
/// Open question 5): a write whose versions could not be archived later is refused now, rather
/// than accepted and then failing the overwrite that would archive it.
pub(super) fn check_archivable(dal: &S3Dal, bucket: &str, key: &str) -> S3Result<()> {
    check_fits(&dal.op, &holder(bucket, key), "to keep its versions")
}

/// Refuses a key whose holder directory `holder` in `op`, with an entry name of up to
/// [`ENTRY_NAME_MAX`] bytes in it, would not fit the backend's path limits: the versions tree's,
/// or a tier's, whose object names are shorter.
pub(super) fn check_fits(op: &Operator, holder: &str, purpose: &str) -> S3Result<()> {
    const NAME_MAX: usize = 255;
    #[cfg(target_os = "macos")]
    const PATH_MAX: usize = 1024;
    #[cfg(not(target_os = "macos"))]
    const PATH_MAX: usize = 4096;
    // S3 and GCS take object names up to 1024 bytes, Azure blob names up to 1024 characters,
    // of which the backend's root prefix is part.
    const OBJECT_NAME_MAX: usize = 1024;
    let root = op.info().root();
    let too_long = if fs::is_fs(op) {
        let longest = holder
            .trim_end_matches('/')
            .split('/')
            .map(str::len)
            .max()
            .unwrap_or(0);
        longest > NAME_MAX || root.len() + 1 + holder.len() + ENTRY_NAME_MAX >= PATH_MAX
    } else {
        let prefix = root.trim_matches('/');
        let prefix = if prefix.is_empty() {
            0
        } else {
            prefix.len() + 1
        };
        prefix + holder.len() + ENTRY_NAME_MAX > OBJECT_NAME_MAX
    };
    if too_long {
        return Err(S3Error::with_message(
            S3ErrorCode::KeyTooLongError,
            format!("the key is too long {purpose} on this backend"),
        ));
    }
    Ok(())
}

/// One archived version or delete marker of a key.
#[derive(Debug, Clone)]
pub(super) struct Entry {
    /// The backend path.
    pub path: String,
    /// The version id, or [`NULL`].
    pub id: String,
    pub marker: bool,
}

impl Entry {
    /// The entry a file named `name` in `holder` stands for; `None` for any other file.
    pub(super) fn parse(holder: &str, name: &str) -> Option<Self> {
        let (stem, marker) = match name.strip_suffix(".dm") {
            Some(stem) => (stem, true),
            None => (name, false),
        };
        let (seq, id) = stem.split_once('-')?;
        let seq_ok = seq.len() == 16 && seq.bytes().all(|b| b.is_ascii_hexdigit());
        (seq_ok && is_id(id)).then(|| Self {
            path: format!("{holder}{name}"),
            id: id.to_owned(),
            marker,
        })
    }

    /// When the entry was made, from the `seq` in its name (see [`Sequence::next_seq`]): when its
    /// version stopped being current, or its marker was written.
    pub(super) fn made(&self) -> SystemTime {
        let name = self.path.rsplit('/').next().unwrap_or_default();
        let seq = name
            .get(..16)
            .and_then(|s| u64::from_str_radix(s, 16).ok())
            .unwrap_or(u64::MAX);
        UNIX_EPOCH + std::time::Duration::from_nanos(u64::MAX - seq)
    }
}

/// The archived versions and markers of `key`, newest first.
pub(super) async fn entries(dal: &S3Dal, bucket: &str, key: &str) -> S3Result<Vec<Entry>> {
    let holder = holder(bucket, key);
    let listed = match dal.op.list(&holder).await {
        Ok(listed) => listed,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(map(err, Ctx::Other)),
    };
    let mut entries: Vec<Entry> = listed
        .iter()
        .filter(|e| e.metadata().is_file())
        .filter_map(|e| Entry::parse(&holder, e.name()))
        .collect();
    entries.sort_unstable_by(|a, b| a.path.cmp(&b.path));
    Ok(entries)
}

/// The id of the version a current object is.
pub(super) fn id_of(dal: &S3Dal, m: &Metadata) -> String {
    meta::resolve(&dal.profile, m)
        .version_id
        .unwrap_or_else(|| NULL.to_owned())
}

/// Before a write or a delete replaces the current version of `key` (at `path`) in a bucket in
/// `state`: keeps that version as an archived entry (versioning.md "Writes", "DeleteObject").
/// Runs under the key's lock. A version found archived already, left by a commit that failed
/// after its archive, is not archived twice.
///
/// Returns the versions that go for good, which only a suspended bucket's `null` version does:
/// the current one, and with `tracked` an archived one, whose size takes a stat to learn.
/// Every other version is kept and still counts. The tier objects of the cold versions that go
/// are noted in `removals`; a server with tiers therefore stats an archived `null` version even
/// where usage is not counted.
pub(super) async fn archive(
    dal: &S3Dal,
    bucket: &str,
    key: &str,
    path: &str,
    state: Versioning,
    tracked: bool,
    removals: &Removals,
) -> S3Result<Usage> {
    let mut gone = Usage::default();
    if state == Versioning::Unversioned {
        return Ok(gone);
    }
    let current = store::stat_existing(dal, path)
        .await?
        .filter(Metadata::is_file);
    let entries = entries(dal, bucket, key).await?;
    if let Some(m) = &current {
        let obj = meta::resolve(&dal.profile, m);
        let id = obj.version_id.clone().unwrap_or_else(|| NULL.to_owned());
        // In a suspended bucket a write or delete replaces the `null` version: it is gone.
        let kept = !(state == Versioning::Suspended && id == NULL);
        if kept && !entries.iter().any(|e| !e.marker && e.id == id) {
            let to = format!("{}{}-{id}", holder(bucket, key), dal.versions.next_seq());
            keep(dal, path, &to, m, &obj).await?;
        }
        if !kept {
            removals.note(bucket, key, &obj);
            gone = gone.plus(Usage::object(obj.content_length));
        }
    }
    if state == Versioning::Suspended {
        // What the write or marker that follows becomes is the `null` version, the only one.
        for e in entries.iter().filter(|e| e.id == NULL) {
            if (tracked || dal.has_tiers()) && !e.marker {
                gone = gone.plus(entry_usage(dal, bucket, key, e, removals).await?);
            }
            remove_entry(dal, e).await?;
        }
    }
    Ok(gone)
}

/// The archived version `entry` of `key` as usage, by its logical size; one removed meanwhile
/// counts as nothing. A cold one's tier object is noted in `removals`, as the entry is about
/// to go.
async fn entry_usage(
    dal: &S3Dal,
    bucket: &str,
    key: &str,
    entry: &Entry,
    removals: &Removals,
) -> S3Result<Usage> {
    let Some(m) = store::stat_existing(dal, &entry.path)
        .await?
        .filter(Metadata::is_file)
    else {
        return Ok(Usage::default());
    };
    let obj = meta::resolve(&dal.profile, &m);
    removals.note(bucket, key, &obj);
    Ok(Usage::object(obj.content_length))
}

/// Archives the version `obj` at `from` as `to`. On fs a hard link: the version stays the same
/// inode, so its content, metadata (stored ETag, checksums, part layout) and mtime, and with it
/// a synthesized ETag, are its own, and no data moves. Elsewhere a copy that stores what the
/// copy would change (see [`meta::version_copy_options`]), and that reads only the object `m`
/// stat found: another instance sharing the backend may replace it in between, and its content
/// must not be kept under this version's id.
async fn keep(
    dal: &S3Dal,
    from: &str,
    to: &str,
    m: &Metadata,
    obj: &meta::ObjectMeta,
) -> S3Result<()> {
    if !fs::is_fs(&dal.op) {
        let opts = meta::version_copy_options(&dal.profile, obj, true);
        let if_match = m
            .etag()
            .filter(|_| dal.op.info().capability().read_with_if_match)
            .map(str::to_owned);
        // The stored length, not the object's: a cold version's stub is empty, and a copy of
        // its logical size would fail or read past it.
        let total = m.content_length();
        return copy_version(dal, from, to, obj, total, opts, true, if_match).await;
    }
    let root = PathBuf::from(dal.op.info().root());
    let (from, to) = (root.join(from), root.join(to));
    fs::blocking(move || {
        if let Some(parent) = to.parent() {
            std::fs::create_dir_all(parent).map_err(new_std_io_error)?;
        }
        std::fs::hard_link(&from, &to).map_err(new_std_io_error)
    })
    .await
    .map_err(|err| map(err, Ctx::ObjectWrite))
}

/// The most one `Writer::copy_from` call takes, S3's limit for one UploadPartCopy.
const COPY_CALL_MAX: u64 = 5 * 1024 * 1024 * 1024;

/// Copies the version `obj` at `from` to `to` on an object store, written with `opts`. Where
/// the backend copies natively (S3's UploadPartCopy) no data passes through s3dal; elsewhere it
/// streams. A copy back to the key (`archived` false) on a backend with native ETags is shaped
/// so that the backend computes the version's ETag again, as Complete and PutObject shape their
/// writes (design.md "Native ETags"): part by part along a stored layout, else in one request
/// up to `--etag-buffer-limit`. `total` is the length stored at `from`; a cold version's stub
/// stores none of its bytes, and is copied as the empty stub it is.
#[allow(clippy::too_many_arguments)]
async fn copy_version(
    dal: &S3Dal,
    from: &str,
    to: &str,
    obj: &meta::ObjectMeta,
    total: u64,
    mut opts: WriteOptions,
    archived: bool,
    if_match: Option<String>,
) -> S3Result<()> {
    let layout = obj.parts.filter(|_| obj.tier.is_none()).and_then(|p| {
        (1..=p.count)
            .map(|n| p.range(n, total))
            .collect::<Option<Vec<_>>>()
    });
    let ranges = match layout {
        Some(parts) if !archived && dal.profile.native_etag => parts,
        _ => {
            if !archived && dal.profile.native_etag {
                opts.chunk =
                    Some(usize::try_from(dal.settings().etag_buffer_limit).unwrap_or(usize::MAX));
            }
            (0..total.div_ceil(COPY_CALL_MAX))
                .map(|i| i * COPY_CALL_MAX..total.min((i + 1) * COPY_CALL_MAX))
                .collect()
        }
    };
    if opts.chunk.is_none() {
        opts.chunk = store::chunk_for(&dal.op, total);
    }
    let mut w = dal
        .op
        .writer_options(to, opts)
        .await
        .map_err(store::write_err)?;
    for range in ranges {
        let read = ReadOptions {
            range: range.into(),
            if_match: if_match.clone(),
            ..Default::default()
        };
        if let Err(err) = w.copy_from_options(from, read).await {
            if let Err(err) = w.abort().await {
                tracing::warn!(path = to, error = %err, "cannot abort a version copy");
            }
            return Err(if err.kind() == ErrorKind::ConditionNotMatch {
                store::conflict()
            } else {
                map(err, Ctx::ObjectRead)
            });
        }
    }
    w.close().await.map(drop).map_err(store::write_err)
}

async fn remove_entry(dal: &S3Dal, entry: &Entry) -> S3Result<()> {
    match dal.op.delete(&entry.path).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(map(err, Ctx::Other)),
    }
}

/// Removes the key's holder directory and the empty directories above it in the versions tree,
/// once its last entry is gone. Best effort, as `store::prune_empty_parents`: an empty holder
/// lists nothing.
async fn prune_holder(dal: &S3Dal, bucket: &str, key: &str) {
    prune_up_to(dal, &versions_dir(bucket), holder(bucket, key)).await;
}

/// Removes `dir` and the empty directories above it, up to but not including `top`; the
/// thumbnail cache's holders go the same way.
pub(super) async fn prune_up_to(dal: &S3Dal, top: &str, mut dir: String) {
    if !dal.profile.real_dirs {
        return;
    }
    while dir.len() > top.len() {
        if dal.op.delete(&dir).await.is_err() {
            return;
        }
        let trimmed = dir.trim_end_matches('/');
        match trimmed.rfind('/') {
            Some(i) => dir.truncate(i + 1),
            None => return,
        }
    }
}

async fn write_marker(dal: &S3Dal, bucket: &str, key: &str, id: &str) -> S3Result<()> {
    let path = format!("{}{}-{id}.dm", holder(bucket, key), dal.versions.next_seq());
    without_stalling(&dal.op, dal.op.write(&path, Vec::<u8>::new()))
        .await
        .map(drop)
        .map_err(|err| map(err, Ctx::ObjectWrite))
}

/// After the current version at `path` was removed: makes the newest archived entry current
/// again (versioning.md "DeleteObject"), unless that entry is a marker, which is then the
/// latest. Entries that duplicate `removed`, left by a commit that failed after archiving it, go
/// with it. Whether a version was promoted.
async fn promote(
    dal: &S3Dal,
    bucket: &str,
    key: &str,
    path: &str,
    removed: Option<&str>,
) -> S3Result<bool> {
    let mut entries = entries(dal, bucket, key).await?;
    if let Some(removed) = removed {
        for e in entries.iter().filter(|e| !e.marker && e.id == removed) {
            remove_entry(dal, e).await?;
        }
        entries.retain(|e| e.marker || e.id != removed);
    }
    let Some(newest) = entries.first() else {
        prune_holder(dal, bucket, key).await;
        return Ok(false);
    };
    if newest.marker {
        return Ok(false);
    }
    if !fs::is_fs(&dal.op) {
        // The version keeps its id, ETag, content and headers; its Last-Modified becomes now,
        // since the key's object is a new one (versioning.md "V2").
        let m = dal
            .op
            .stat(&newest.path)
            .await
            .map_err(|e| map(e, Ctx::ObjectRead))?;
        let obj = meta::resolve(&dal.profile, &m);
        let opts = meta::version_copy_options(&dal.profile, &obj, false);
        let total = m.content_length();
        copy_version(dal, &newest.path, path, &obj, total, opts, false, None).await?;
        remove_entry(dal, newest).await?;
        if entries.len() == 1 {
            prune_holder(dal, bucket, key).await;
        }
        return Ok(true);
    }
    let root = PathBuf::from(dal.op.info().root());
    let (from, to) = (root.join(&newest.path), root.join(path));
    // A rename keeps the version's inode, so it comes back exactly as it was. A key written
    // under it since (`k/x` after `k` was deleted) leaves it archived: the conflict error.
    fs::blocking(move || {
        if let Some(parent) = to.parent() {
            std::fs::create_dir_all(parent).map_err(new_std_io_error)?;
        }
        std::fs::rename(&from, &to).map_err(new_std_io_error)
    })
    .await
    .map_err(|err| map(err, Ctx::ObjectWrite))?;
    if entries.len() == 1 {
        prune_holder(dal, bucket, key).await;
    }
    Ok(true)
}

/// What a versioned DeleteObject did.
pub(super) struct Deleted {
    /// The marker written, or the version removed.
    pub version_id: Option<String>,
    /// A marker was written, or the version removed was one.
    pub delete_marker: bool,
    /// The change as an event names it, `None` when nothing changed, and what it removed from
    /// the bucket's usage (see [`archive`]).
    pub change: Option<(EventKind, Usage)>,
}

/// DeleteObject in a bucket in `state`, which is not [`Versioning::Unversioned`]
/// (versioning.md "DeleteObject"). Without `version_id` the key gets a delete marker, and the
/// version it held is archived (or, a `null` one in a suspended bucket, removed). With one, that
/// version or marker is removed for good, and removing the latest makes the one below it current.
/// With `tracked`, an archived version removed is stat'ed for its size. The tier objects of the
/// cold versions removed for good are noted in `removals`, for the caller to remove.
#[allow(clippy::too_many_arguments)]
pub(super) async fn delete(
    dal: &S3Dal,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    cond: DeleteCondition<'_>,
    state: Versioning,
    tracked: bool,
    removals: &Removals,
) -> S3Result<Deleted> {
    naming::validate_key(key)?;
    if let Some(id) = version_id {
        check_id(id)?;
    }
    // Folder markers are directories here and have no versions (versioning.md "Scope").
    if key.ends_with('/') {
        naming::require_null_version(version_id)?;
        let removed = store::delete_key(dal, bucket, key, cond).await?;
        return Ok(Deleted {
            version_id: Some(NULL.to_owned()),
            delete_marker: false,
            change: removed.map(|r| (EventKind::Delete, r)),
        });
    }
    let path = naming::object_path(bucket, key);
    let (deleted, prune) = {
        let _guard = dal.locks.lock(&path).await;
        let current = store::stat_existing(dal, &path)
            .await?
            .filter(Metadata::is_file);
        match version_id {
            None => {
                delete_latest(
                    dal, bucket, key, &path, current, cond, state, tracked, removals,
                )
                .await?
            }
            Some(id) => {
                delete_version(
                    dal, bucket, key, &path, current, id, cond, tracked, removals,
                )
                .await?
            }
        }
    };
    if prune {
        store::prune_empty_parents(dal, bucket, key).await;
    }
    Ok(deleted)
}

/// The delete without a version id: a marker on top. Whether the key's directory may need
/// pruning comes second.
#[allow(clippy::too_many_arguments)]
async fn delete_latest(
    dal: &S3Dal,
    bucket: &str,
    key: &str,
    path: &str,
    current: Option<Metadata>,
    cond: DeleteCondition<'_>,
    state: Versioning,
    tracked: bool,
    removals: &Removals,
) -> S3Result<(Deleted, bool)> {
    if !cond.is_empty() {
        let resolved = current.as_ref().map(|m| meta::resolve(&dal.profile, m));
        cond.check(resolved.as_ref())?;
        // Only an object meets a condition, as in an unversioned bucket.
        if resolved.is_none() {
            return Ok((
                Deleted {
                    version_id: None,
                    delete_marker: false,
                    change: None,
                },
                false,
            ));
        }
    }
    let gone = archive(dal, bucket, key, path, state, tracked, removals).await?;
    let removed = current.is_some();
    if removed {
        dal.op.delete(path).await.map_err(|e| map(e, Ctx::Other))?;
    }
    // After the removal, so no crash leaves a marker on top of a current object; a crash in
    // between leaves the key deleted without one, with every version kept.
    let id = match state {
        Versioning::Enabled => new_id(),
        _ => NULL.to_owned(),
    };
    write_marker(dal, bucket, key, &id).await?;
    Ok((
        Deleted {
            version_id: Some(id),
            delete_marker: true,
            change: Some((EventKind::DeleteMarker, gone)),
        },
        removed,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn delete_version(
    dal: &S3Dal,
    bucket: &str,
    key: &str,
    path: &str,
    current: Option<Metadata>,
    id: &str,
    cond: DeleteCondition<'_>,
    tracked: bool,
    removals: &Removals,
) -> S3Result<(Deleted, bool)> {
    let removed_version = |marker, gone: Option<Usage>| Deleted {
        version_id: Some(id.to_owned()),
        delete_marker: marker,
        change: gone.map(|gone| (EventKind::Delete, gone)),
    };
    if let Some(m) = current.as_ref().filter(|m| id_of(dal, m) == id) {
        let obj = meta::resolve(&dal.profile, m);
        cond.check(Some(&obj))?;
        dal.op.delete(path).await.map_err(|e| map(e, Ctx::Other))?;
        removals.note(bucket, key, &obj);
        let promoted = promote(dal, bucket, key, path, Some(id)).await?;
        let gone = Usage::object(obj.content_length);
        return Ok((removed_version(false, Some(gone)), !promoted));
    }
    let entries = entries(dal, bucket, key).await?;
    let Some(entry) = entries.iter().find(|e| e.id == id).cloned() else {
        // AWS answers a version that does not exist with 204, conditions or not (ceph's
        // `test_delete_object_version_if_match`): a retried delete of a version must not fail,
        // unlike If-Match on a key that holds nothing.
        return Ok((removed_version(false, None), false));
    };
    if !cond.is_empty() {
        let found = if entry.marker {
            None
        } else {
            let m = dal
                .op
                .stat(&entry.path)
                .await
                .map_err(|e| map(e, Ctx::ObjectRead))?;
            Some(meta::resolve(&dal.profile, &m))
        };
        cond.check(found.as_ref())?;
    }
    // Before the removal, while there is a file to stat, and after the condition held: a cold
    // version's tier object is noted only once it is sure to go.
    let gone = if (tracked || dal.has_tiers()) && !entry.marker {
        entry_usage(dal, bucket, key, &entry, removals).await?
    } else {
        Usage::default()
    };
    // Two instances that archived the same version each left an entry of it; all of them go.
    for e in entries
        .iter()
        .filter(|e| e.id == id && e.marker == entry.marker)
    {
        remove_entry(dal, e).await?;
    }
    // The latest was this marker: the version below it becomes current ("undelete").
    let was_latest = current.is_none() && entries.first().is_some_and(|e| e.path == entry.path);
    if was_latest {
        promote(dal, bucket, key, path, None).await?;
    } else if entries
        .iter()
        .all(|e| e.id == id && e.marker == entry.marker)
    {
        prune_holder(dal, bucket, key).await;
    }
    Ok((removed_version(entry.marker, Some(gone)), false))
}

/// Where the version a read names lives.
pub(super) struct Target {
    pub path: String,
    /// The bucket has or had versioning, so responses name the version, `null` included.
    pub versioned: bool,
    /// The request named a version.
    pub named: bool,
}

/// Resolves the version a GET, HEAD, attributes, ACL or copy-source request names. In a bucket
/// that never had versioning only `null` exists, as before. A version that is a delete marker
/// answers 405 with the marker's headers, as on AWS.
pub(super) async fn read_target(
    dal: &S3Dal,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> S3Result<Target> {
    let path = naming::object_path(bucket, key);
    let versioned = state(dal, bucket).await? != Versioning::Unversioned;
    let Some(id) = version_id.filter(|_| versioned) else {
        if !versioned {
            naming::require_null_version(version_id)?;
        }
        return Ok(Target {
            path,
            versioned,
            named: false,
        });
    };
    check_id(id)?;
    let current = store::stat_existing(dal, &path)
        .await?
        .filter(Metadata::is_file);
    if current.is_some_and(|m| id_of(dal, &m) == id) {
        return Ok(Target {
            path,
            versioned,
            named: true,
        });
    }
    match entries(dal, bucket, key)
        .await?
        .into_iter()
        .find(|e| e.id == id)
    {
        Some(e) if e.marker => Err(marker_error(S3ErrorCode::MethodNotAllowed, &e.id)),
        Some(e) => Ok(Target {
            path: e.path,
            versioned,
            named: true,
        }),
        None => Err(s3_error!(
            NoSuchVersion,
            "The specified version does not exist."
        )),
    }
}

/// The error for a read of a key that has no current version: with the latest marker's headers
/// when a marker is why, as AWS answers, so clients tell a deleted key from one never written.
pub(super) async fn missing(dal: &S3Dal, bucket: &str, key: &str, err: S3Error) -> S3Error {
    if *err.code() != S3ErrorCode::NoSuchKey {
        return err;
    }
    match entries(dal, bucket, key).await {
        Ok(entries) => match entries.first() {
            Some(e) if e.marker => marker_error(S3ErrorCode::NoSuchKey, &e.id),
            _ => err,
        },
        Err(_) => err,
    }
}

/// `code` with `x-amz-delete-marker: true` and the marker's version id. s3s replaces the
/// response's headers with an error's own, so the XML body's type is restated.
fn marker_error(code: S3ErrorCode, id: &str) -> S3Error {
    let mut err = S3Error::new(code);
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/xml"));
    headers.insert(
        HeaderName::from_static("x-amz-delete-marker"),
        HeaderValue::from_static("true"),
    );
    if let Ok(id) = HeaderValue::from_str(id) {
        headers.insert(HeaderName::from_static("x-amz-version-id"), id);
    }
    err.set_headers(headers);
    err
}

/// The version id a response names for `obj` read from a [`Target`].
pub(super) fn response_id(target: &Target, obj: &meta::ObjectMeta) -> Option<String> {
    target
        .versioned
        .then(|| obj.version_id.clone().unwrap_or_else(|| NULL.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_escape_keeps_key_order_and_prefixes() {
        // ListObjectVersions lists keys in byte order straight off the versions tree, and a
        // key's holder `E(k)!!/` must sort right after the key and before any longer key.
        let keys = [
            "a", "a\tb", "a\nb", "a\rb", "a b", "a!", "a!b", "a\"", "a-", "a.txt", "a/", "a/b",
            "a0", "ab", "b", "é", "ü/x",
        ];
        let mut sorted = keys.to_vec();
        sorted.sort_unstable();
        let holders: Vec<String> = sorted.iter().map(|k| format!("{}!!/", escape(k))).collect();
        let mut by_holder = holders.clone();
        by_holder.sort_unstable();
        assert_eq!(holders, by_holder);
        for k in keys {
            let e = escape(k);
            assert!(!e.contains("!!"), "{k:?} -> {e:?}");
            assert!(!e.contains(char::is_whitespace), "{k:?} -> {e:?}");
            assert_eq!(unescape(&e).as_deref(), Some(k));
        }
        assert!(escape("a b/c").starts_with(&escape("a b/")));
    }

    #[test]
    fn escape_order_matches_byte_order_on_random_keys() {
        // Brute force over a small alphabet holding every escaped byte and its neighbours.
        let alphabet = ['\t', '\n', '\r', ' ', '!', '"', '-', '.', '/', '0', 'a'];
        let mut keys = vec![String::new()];
        for _ in 0..3 {
            let longer: Vec<String> = keys
                .iter()
                .flat_map(|k| alphabet.iter().map(move |c| format!("{k}{c}")))
                .collect();
            keys.extend(longer);
        }
        keys.retain(|k| !k.is_empty());
        keys.sort_unstable();
        keys.dedup();
        for pair in keys.windows(2) {
            let (a, b) = (
                format!("{}!!/", escape(&pair[0])),
                format!("{}!!/", escape(&pair[1])),
            );
            assert!(a < b, "{:?} < {:?} but {a:?} >= {b:?}", pair[0], pair[1]);
        }
    }

    #[test]
    fn names_order_newest_first_and_parse_back() {
        let states = Sequence::default();
        let first = states.next_seq();
        let second = states.next_seq();
        assert!(second < first, "a later entry must sort first");
        let id = new_id();
        let e = Entry::parse("h/", &format!("{second}-{id}.dm")).unwrap();
        assert!(e.marker && e.id == id && e.path == format!("h/{second}-{id}.dm"));
        let e = Entry::parse("h/", &format!("{first}-null")).unwrap();
        assert!(!e.marker && e.id == NULL);
        // Lifecycle counts how long a version has been noncurrent from the name's time.
        let age = SystemTime::now().duration_since(e.made()).unwrap();
        assert!(age < std::time::Duration::from_secs(5), "{age:?}");
        for bad in [
            "x",
            "0000-null",
            &format!("{first}-XYZ"),
            &format!("{first}-null.tmp"),
        ] {
            assert!(Entry::parse("h/", bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn only_ids_s3dal_issues_are_looked_for() {
        assert!(check_id(NULL).is_ok());
        assert!(check_id(&new_id()).is_ok());
        for bad in [
            "",
            "NULL",
            "3HL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY",
            &"A".repeat(32),
            "../x",
        ] {
            assert_eq!(
                *check_id(bad).unwrap_err().code(),
                S3ErrorCode::InvalidArgument,
                "{bad}"
            );
        }
    }
}

//! A bucket's usage, which its quota is enforced against and the metrics report: the last scan
//! of the bucket, plus the changes this process made since (features-2026-11.md "Quota").
//!
//! The scan (the F2 job `usage-scan`, on one instance of those sharing a backend) lists each
//! bucket whole, every version of every object included, and records `{objects, bytes, at}` in
//! `.s3dal/usage/<bucket>.json`, beside the bucket's state rather than in it: the state is read
//! on every anonymous download and every write, and rewritten only when an operator changes a
//! setting, so its readers never pay for, and its writers never race with, a file the scan
//! rewrites every few minutes. Between scans each process adds what its own writes changed (the
//! usage of their events, see `crate::events`); another instance's writes reach it with the next
//! scan. Writes made while a scan lists the bucket may be counted twice, by the listing and as a
//! change after its `at`, until the next scan: an error towards refusing, not towards letting
//! the bucket grow past its quota.

use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::TryStreamExt;
use opendal::ErrorKind;
use s3s::{S3Error, S3ErrorCode, S3Result};
use serde::{Deserialize, Serialize};

use super::bucket_state::{self, Quota};
use super::version::{self, Versioning};
use super::{S3Dal, fs, naming, store, swap};
use crate::backend::{RESERVED_ROOT, without_stalling};
use crate::error::{Ctx, map};
use crate::events::Usage;
use crate::meta;
use crate::metrics::BucketGauges;

/// How long a process trusts the scan it read: a newer one, of another instance, is taken up
/// within this time, as a bucket's state is.
const SCAN_TTL: Duration = Duration::from_secs(5);

/// The scan file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Scan {
    /// Every object, cold ones included, which count their logical size: quota and usage are
    /// about what the bucket holds, wherever its bytes are (cold-tier.md, open question 1).
    pub objects: u64,
    pub bytes: u64,
    /// When the listing started: changes from then on are not in it, or not for sure.
    pub at: SystemTime,
    /// Of `objects` and `bytes`, those of cold objects. Absent in a scan recorded before tiers
    /// existed.
    #[serde(default)]
    pub cold_objects: u64,
    #[serde(default)]
    pub cold_bytes: u64,
}

/// A bucket's usage as this process knows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BucketUsage {
    pub objects: u64,
    pub bytes: u64,
    /// Of `bytes`, those of cold objects as the last scan counted them: the changes since carry
    /// no split, and a transition changes no usage, so the split follows at the next scan.
    pub cold_bytes: u64,
    /// When the scan it starts from began; `None` before the bucket's first scan, when only this
    /// process's changes since its start are counted.
    pub scanned: Option<SystemTime>,
}

/// Per process: each bucket's last scan read and the changes made since.
#[derive(Default)]
pub(crate) struct Tracker {
    buckets: Mutex<HashMap<String, Known>>,
}

/// Changes are summed per slot of this many milliseconds.
const SLOT_MS: u128 = 100;

/// Slots kept per bucket before the oldest half is summed into one. Only an instance that reads
/// no newer scan, because the scan is off or failing, gets there.
const MAX_SLOTS: usize = 4096;

#[derive(Default)]
struct Known {
    scan: Option<Scan>,
    /// When the scan file was last read; `None`: never.
    read: Option<Instant>,
    /// This process's changes, summed per slot (see [`slot`]), oldest first.
    changes: VecDeque<(u64, Usage)>,
    /// The bucket's quota as last seen, for the metrics.
    quota: Option<Quota>,
}

fn slot(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).map_or(0, |d| {
        u64::try_from(d.as_millis() / SLOT_MS).unwrap_or(u64::MAX)
    })
}

impl Known {
    /// The scan plus the changes kept, which [`Self::take`] trimmed to those it does not cover.
    fn total(&self) -> BucketUsage {
        let base = self.scan.map_or(Usage::default(), |s| Usage {
            objects: i64::try_from(s.objects).unwrap_or(i64::MAX),
            bytes: i64::try_from(s.bytes).unwrap_or(i64::MAX),
        });
        let total = self
            .changes
            .iter()
            .fold(base, |sum, (_, change)| sum.plus(*change));
        let bytes = u64::try_from(total.bytes).unwrap_or(0);
        BucketUsage {
            objects: u64::try_from(total.objects).unwrap_or(0),
            bytes,
            // Deletes since the scan may have taken cold bytes away.
            cold_bytes: self.scan.map_or(0, |s| s.cold_bytes).min(bytes),
            scanned: self.scan.map(|s| s.at),
        }
    }

    /// Takes `scan` if it is newer than the one known, and forgets the changes it covers: those
    /// of the slots before the one its listing started in. A change in that slot may be in the
    /// listing already, and counting it twice errs towards the quota.
    fn take(&mut self, scan: Option<Scan>) {
        if let Some(scan) = scan
            && self.scan.is_none_or(|known| known.at < scan.at)
        {
            self.scan = Some(scan);
        }
        if let Some(scan) = self.scan {
            let since = slot(scan.at);
            while self.changes.front().is_some_and(|(at, _)| *at < since) {
                self.changes.pop_front();
            }
        }
    }

    fn add(&mut self, at: SystemTime, change: Usage) {
        let at = slot(at);
        match self.changes.back_mut() {
            // Commits finish out of order across requests: one a little older than the last
            // goes into the last slot, later than it was, which a scan can only count twice.
            Some((last, sum)) if *last >= at => *sum = sum.plus(change),
            _ => self.changes.push_back((at, change)),
        }
        if self.changes.len() > MAX_SLOTS {
            // Stamped with the newest of them, so a scan forgets the sum only once it covers
            // all of it: at worst the older ones count twice.
            let merged = self
                .changes
                .drain(..MAX_SLOTS / 2)
                .fold((0, Usage::default()), |(_, sum), (at, change)| {
                    (at, sum.plus(change))
                });
            self.changes.push_front(merged);
        }
    }

    fn gauges(&self) -> BucketGauges {
        let total = self.total();
        BucketGauges {
            objects: total.objects,
            bytes: total.bytes,
            max_bytes: self.quota.as_ref().map(|q| q.max_bytes),
            max_objects: self.quota.as_ref().and_then(|q| q.max_objects),
        }
    }
}

impl Tracker {
    fn with<T>(&self, bucket: &str, f: impl FnOnce(&mut Known) -> T) -> T {
        let mut buckets = self.buckets.lock().unwrap_or_else(PoisonError::into_inner);
        f(buckets.entry(bucket.to_owned()).or_default())
    }

    /// Adds a change this process committed at `at`.
    pub(crate) fn apply(&self, dal: &S3Dal, bucket: &str, at: SystemTime, change: Usage) {
        let gauges = self.with(bucket, |known| {
            known.add(at, change);
            known.gauges()
        });
        dal.metrics().set_bucket(bucket, Some(gauges));
    }

    /// Forgets the bucket, which was deleted.
    pub(crate) fn forget(&self, dal: &S3Dal, bucket: &str) {
        self.buckets
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(bucket);
        dal.metrics().set_bucket(bucket, None);
    }

    fn take(&self, dal: &S3Dal, bucket: &str, scan: Option<Scan>, quota: Option<Quota>) {
        let gauges = self.with(bucket, |known| {
            known.take(scan);
            known.read = Some(Instant::now());
            known.quota = quota;
            known.gauges()
        });
        dal.metrics().set_bucket(bucket, Some(gauges));
    }

    /// Has the next [`current`] read the scan and the quota again.
    pub(crate) fn stale(&self, bucket: &str) {
        self.with(bucket, |known| known.read = None);
    }

    fn fresh(&self, bucket: &str) -> Option<BucketUsage> {
        self.with(bucket, |known| {
            known
                .read
                .is_some_and(|read| read.elapsed() < SCAN_TTL)
                .then(|| known.total())
        })
    }
}

fn path(bucket: &str) -> String {
    format!("{RESERVED_ROOT}usage/{bucket}.json")
}

/// The bucket's usage: the newest scan any instance recorded, read at most once per
/// [`SCAN_TTL`], plus this process's changes since. The bucket must exist.
pub(crate) async fn current(dal: &S3Dal, bucket: &str) -> S3Result<BucketUsage> {
    if let Some(usage) = dal.usage.fresh(bucket) {
        return Ok(usage);
    }
    let scan = swap::load::<Scan>(dal, &path(bucket))
        .await
        .map_err(|err| map(err, Ctx::Other))?;
    let quota = bucket_state::get(dal, bucket).await?.quota.clone();
    dal.usage.take(dal, bucket, scan, quota);
    Ok(dal.usage.with(bucket, |known| known.total()))
}

/// Removes what a deleted bucket's name had recorded, with its state (see `version::forget`).
pub(super) async fn forget(dal: &S3Dal, bucket: &str) -> S3Result<()> {
    match dal.op.delete(&path(bucket)).await {
        Ok(()) => {}
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => return Err(map(err, Ctx::Other)),
    }
    dal.usage.forget(dal, bucket);
    Ok(())
}

/// Counts every bucket and records each count; the job `usage-scan`. A bucket that cannot be
/// counted is logged and left with its previous count. Returns how many were counted.
pub(crate) async fn scan_all(dal: &S3Dal) -> anyhow::Result<usize> {
    let buckets = bucket_names(dal).await?;
    let mut counted = 0;
    let mut failed = None;
    for bucket in &buckets {
        match scan(dal, bucket).await {
            Ok(_) => counted += 1,
            Err(err) => {
                tracing::warn!(bucket, error = %err, "cannot count the bucket's usage");
                failed = Some(err);
            }
        }
    }
    match failed {
        Some(err) if counted == 0 => Err(anyhow::anyhow!("cannot count any bucket: {err}")),
        _ => Ok(counted),
    }
}

/// Every bucket, as ListBuckets finds them.
pub(super) async fn bucket_names(dal: &S3Dal) -> opendal::Result<Vec<String>> {
    let entries = dal.op.list("/").await?;
    Ok(entries
        .iter()
        .filter(|e| e.metadata().is_dir())
        .filter_map(|e| e.path().strip_suffix('/'))
        // Valid names never start with `.`, so this also skips `.s3dal/`.
        .filter(|name| naming::validate_bucket_name(name).is_ok())
        .map(str::to_owned)
        .collect())
}

/// Every bucket's usage and quota, by name.
pub(crate) async fn all(dal: &S3Dal) -> S3Result<Vec<(String, BucketUsage, Option<Quota>)>> {
    let names = bucket_names(dal)
        .await
        .map_err(|err| map(err, Ctx::Other))?;
    let mut out = Vec::with_capacity(names.len());
    for name in names {
        let used = current(dal, &name).await?;
        let quota = bucket_state::get(dal, &name).await?.quota.clone();
        out.push((name, used, quota));
    }
    Ok(out)
}

/// Lists the bucket whole, every version of every object included (delete markers and folder
/// markers hold nothing), and records the count.
pub(crate) async fn scan(dal: &S3Dal, bucket: &str) -> S3Result<Scan> {
    let at = SystemTime::now();
    let current = count(dal, &naming::bucket_dir(bucket)).await?;
    let archived = count(dal, &version::versions_dir(bucket)).await?;
    let scan = Scan {
        objects: current.objects + archived.objects,
        bytes: current.bytes + archived.bytes,
        at,
        cold_objects: current.cold_objects + archived.cold_objects,
        cold_bytes: current.cold_bytes + archived.cold_bytes,
    };
    let json = serde_json::to_vec(&scan).map_err(S3Error::internal_error)?;
    without_stalling(&dal.op, dal.op.write(&path(bucket), json))
        .await
        .map_err(|err| map(err, Ctx::Other))?;
    let quota = bucket_state::get(dal, bucket).await?.quota.clone();
    dal.usage.take(dal, bucket, Some(scan), quota);
    Ok(scan)
}

/// What [`count`] found.
#[derive(Default)]
struct Counted {
    objects: u64,
    bytes: u64,
    cold_objects: u64,
    cold_bytes: u64,
}

impl Counted {
    /// Adds an object of `len` bytes, or a cold one of `cold` logical bytes.
    fn add(&mut self, len: u64, cold: Option<u64>) {
        self.objects += 1;
        match cold {
            Some(size) => {
                self.bytes += size;
                self.cold_objects += 1;
                self.cold_bytes += size;
            }
            None => self.bytes += len,
        }
    }
}

/// What the files under `dir` hold; a delete marker (`.dm`) is no object. A missing directory
/// holds none. With tiers configured an empty file may be a stub, which is stat'ed (or, on fs,
/// has its xattrs read) for the size it stands for; a server without tiers pays nothing more.
async fn count(dal: &S3Dal, dir: &str) -> S3Result<Counted> {
    let tiered = dal.has_tiers();
    #[cfg(unix)]
    if fs::is_fs(&dal.op) {
        let root = std::path::PathBuf::from(dal.op.info().root()).join(dir);
        return fs::blocking(move || walk(&root, tiered).map_err(opendal::raw::new_std_io_error))
            .await
            .map_err(|err| map(err, Ctx::Other));
    }
    let mut lister = match dal.op.lister_with(dir).recursive(true).await {
        Ok(lister) => lister,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Counted::default()),
        Err(err) => return Err(map(err, Ctx::Other)),
    };
    let mut counted = Counted::default();
    while let Some(entry) = lister.try_next().await.map_err(|e| map(e, Ctx::Other))? {
        let m = entry.metadata();
        if !m.is_file() || entry.path().ends_with(".dm") {
            continue;
        }
        let cold = if tiered && m.content_length() == 0 {
            match store::stat_existing(dal, entry.path()).await? {
                Some(m) if m.is_file() => {
                    let obj = meta::resolve(&dal.profile, &m);
                    obj.tier.is_some().then_some(obj.content_length)
                }
                // Removed since it was listed.
                _ => continue,
            }
        } else {
            None
        };
        counted.add(m.content_length(), cold);
    }
    Ok(counted)
}

/// [`count`] on fs, straight from the filesystem: OpenDAL's fs lister does not stat what it
/// lists, so it knows no sizes, and a stat through it each would be a trip through the blocking
/// pool per object. Links are followed as OpenDAL's stat follows them, but not into directories.
#[cfg(unix)]
fn walk(root: &std::path::Path, tiered: bool) -> std::io::Result<Counted> {
    let mut counted = Counted::default();
    let mut dirs = vec![root.to_owned()];
    while let Some(dir) = dirs.pop() {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err),
        };
        for entry in entries {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                dirs.push(entry.path());
                continue;
            }
            if entry.file_name().to_string_lossy().ends_with(".dm") {
                continue;
            }
            match std::fs::metadata(entry.path()) {
                Ok(m) if m.is_file() => {
                    let cold = if tiered && m.len() == 0 {
                        stub_size(&entry.path())
                    } else {
                        None
                    };
                    counted.add(m.len(), cold);
                }
                Ok(_) => {}
                // Removed since it was listed.
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(err),
            }
        }
    }
    Ok(counted)
}

/// The size an empty file on fs stands for, when it is a stub: its `s3dal-tier` and
/// `s3dal-size` xattrs, as `meta::resolve` reads them. A damaged stub counts as empty.
#[cfg(unix)]
fn stub_size(path: &std::path::Path) -> Option<u64> {
    let read = |name: &str| xattr::get(path, format!("user.{name}")).ok().flatten();
    read(meta::META_TIER)?;
    std::str::from_utf8(&read(meta::META_SIZE)?)
        .ok()?
        .parse()
        .ok()
}

/// Refuses a write of `size` bytes (`None`: not known before the body is read) to `key` that
/// would take the bucket past its quota. In an unversioned bucket the object the key holds is
/// replaced, so it is taken off first, which takes a stat; elsewhere every version stays.
/// A bucket without a quota costs nothing more than its cached state.
pub(super) async fn check_write(
    dal: &S3Dal,
    bucket: &str,
    key: &str,
    size: Option<u64>,
) -> S3Result<()> {
    let Some(quota) = bucket_state::get(dal, bucket).await?.quota.clone() else {
        return Ok(());
    };
    let Some(size) = size else {
        return Err(S3Error::with_message(
            S3ErrorCode::MissingContentLength,
            "This bucket has a quota, so a write must give its Content-Length.",
        ));
    };
    let adding = if key.ends_with('/') {
        Usage::default()
    } else if version::state(dal, bucket).await? == Versioning::Unversioned {
        let replaced = store::stat_existing(dal, &naming::object_path(bucket, key))
            .await?
            .filter(opendal::Metadata::is_file)
            .map_or(Usage::default(), |m| {
                Usage::object(meta::size(&dal.profile, &m))
            });
        Usage::object(size).minus(replaced)
    } else {
        Usage::object(size)
    };
    let used = current(dal, bucket).await?;
    enforce(&quota, used, adding)
}

/// Refuses to start a multipart upload in a bucket already over its quota, whose Complete would
/// be refused after every part was sent. One that is exactly full may still replace an object.
pub(super) async fn check_room(dal: &S3Dal, bucket: &str) -> S3Result<()> {
    let Some(quota) = bucket_state::get(dal, bucket).await?.quota.clone() else {
        return Ok(());
    };
    let used = current(dal, bucket).await?;
    let over_bytes = used.bytes > quota.max_bytes;
    let over_objects = quota.max_objects.is_some_and(|max| used.objects > max);
    refuse_if(&quota, used, over_bytes, over_objects)
}

/// Whether `used` plus `adding` stays within `quota`. Exactly at the limit is within it, and a
/// write is refused only for what it adds: one that takes no more bytes, such as an overwrite with
/// a smaller object, goes through in a bucket over its quota, so a lowered quota can be met by
/// shrinking what is stored, not only by deleting.
fn enforce(quota: &Quota, used: BucketUsage, adding: Usage) -> S3Result<()> {
    let objects = i128::from(used.objects) + i128::from(adding.objects);
    let bytes = i128::from(used.bytes) + i128::from(adding.bytes);
    let over_bytes = adding.bytes > 0 && bytes > i128::from(quota.max_bytes);
    let over_objects = adding.objects > 0
        && quota
            .max_objects
            .is_some_and(|max| objects > i128::from(max));
    refuse_if(quota, used, over_bytes, over_objects)
}

fn refuse_if(
    quota: &Quota,
    used: BucketUsage,
    over_bytes: bool,
    over_objects: bool,
) -> S3Result<()> {
    if !over_bytes && !over_objects {
        return Ok(());
    }
    let what = if over_bytes {
        format!("it holds {} of its {} bytes", used.bytes, quota.max_bytes)
    } else {
        format!(
            "it holds {} of its {} objects",
            used.objects,
            quota.max_objects.unwrap_or_default()
        )
    };
    let mut err = S3Error::with_message(
        S3ErrorCode::Custom("QuotaExceeded".into()),
        format!("The bucket's quota leaves no room for this write: {what}."),
    );
    err.set_status_code(hyper::StatusCode::FORBIDDEN);
    Err(err)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quota(max_bytes: u64, max_objects: Option<u64>) -> Quota {
        Quota {
            max_bytes,
            max_objects,
            unknown: Default::default(),
        }
    }

    fn used(objects: u64, bytes: u64) -> BucketUsage {
        BucketUsage {
            objects,
            bytes,
            cold_bytes: 0,
            scanned: None,
        }
    }

    #[test]
    fn a_write_may_fill_the_quota_exactly_but_not_pass_it() {
        let q = quota(100, Some(3));
        assert!(enforce(&q, used(2, 90), Usage::object(10)).is_ok());
        let err = enforce(&q, used(2, 90), Usage::object(11)).unwrap_err();
        assert_eq!(err.code().as_str(), "QuotaExceeded");
        assert_eq!(err.status_code(), Some(hyper::StatusCode::FORBIDDEN));
        assert!(enforce(&q, used(3, 10), Usage::object(1)).is_err());
        // An overwrite that adds no object fits a bucket full of objects.
        assert!(enforce(&q, used(3, 10), Usage::object(5).minus(Usage::object(4))).is_ok());
        // Without an object limit, only bytes count.
        assert!(enforce(&quota(100, None), used(1_000, 0), Usage::object(100)).is_ok());
        // Over a lowered quota, a write that frees bytes still goes through; one that adds
        // any does not.
        let shrink = Usage::object(10).minus(Usage::object(50));
        assert!(enforce(&q, used(3, 200), shrink).is_ok());
        assert!(enforce(&q, used(3, 200), Usage::object(1).minus(Usage::object(0))).is_err());
    }

    #[test]
    fn changes_count_from_the_slot_their_scan_started_in() {
        // A newer scan covers the changes before it; the ones after it still count, and an
        // older scan arriving late changes nothing.
        let t0 = UNIX_EPOCH + Duration::from_secs(1_000);
        let ms = Duration::from_millis;
        let mut known = Known::default();
        known.add(t0 - ms(100), Usage::object(5));
        // In the scan's slot, after its start: possibly listed, counted again.
        known.add(t0 + ms(50), Usage::object(7));
        known.add(t0 + ms(100), Usage::object(11));
        known.take(Some(Scan {
            objects: 10,
            bytes: 100,
            at: t0,
            cold_objects: 0,
            cold_bytes: 0,
        }));
        assert_eq!((known.total().objects, known.total().bytes), (12, 118));
        known.take(Some(Scan {
            objects: 1,
            bytes: 1,
            at: t0 - Duration::from_secs(60),
            cold_objects: 0,
            cold_bytes: 0,
        }));
        assert_eq!(known.total().bytes, 118);
        assert_eq!(known.changes.len(), 2);
    }

    #[test]
    fn slots_past_the_limit_merge_without_undercounting() {
        // Without a newer scan the changes would pile up; merged, they are still all counted
        // until a scan covers the newest of them.
        let t0 = UNIX_EPOCH + Duration::from_secs(1_000);
        let mut known = Known::default();
        for i in 0..=MAX_SLOTS as u64 {
            known.add(t0 + Duration::from_millis(100 * i), Usage::object(1));
        }
        assert!(known.changes.len() <= MAX_SLOTS);
        assert_eq!(known.total().objects, MAX_SLOTS as u64 + 1);
        // A scan from the middle of the merged ones keeps the merged sum: counted twice
        // rather than lost.
        known.take(Some(Scan {
            objects: 0,
            bytes: 0,
            at: t0 + Duration::from_millis(100),
            cold_objects: 0,
            cold_bytes: 0,
        }));
        assert_eq!(known.total().objects, MAX_SLOTS as u64 + 1);
    }
}

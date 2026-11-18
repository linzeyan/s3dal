//! The F2 job `tier-sweep` (cold-tier.md "Sweep"): removes the tier objects no stub names, which
//! a crash between a tier write and its stub, or a failed removal after a stub went, leaves
//! behind, and reports the stubs whose tier object is gone. It changes no object, so it emits no
//! events.
//!
//! Every write puts its tier object first and its stub second, so a tier object nobody names may
//! be one whose stub is about to be written. Only one older than [`TIER_GRACE`] is a candidate,
//! and it is checked again against its key, the current object and the key's versions, right
//! before it goes.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, SystemTime};

use futures::TryStreamExt;
use opendal::{ErrorKind, Metadata};

use super::tier::{self, prune};
use super::{S3Dal, naming, store, usage, version};
use crate::backend::RESERVED_ROOT;
use crate::meta::{self, ObjectMeta, TierRef};
use crate::tier::Tier;

/// How old a tier object no stub names must be before the sweep removes it: far longer than any
/// write takes from closing its tier object to committing its stub.
pub const TIER_GRACE: Duration = Duration::from_secs(24 * 60 * 60);

/// Dangling stubs logged per run, so a tier emptied behind s3dal's back does not flood the log.
const DANGLING_LOGGED: usize = 100;

/// What a sweep did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TierSweep {
    /// Tier objects no stub named, older than [`TIER_GRACE`], removed.
    pub orphans_removed: u64,
    /// Tier objects no stub named yet, younger than [`TIER_GRACE`], kept.
    pub young_kept: u64,
    /// Stubs naming a tier object that is not in their tier.
    pub dangling: u64,
}

/// A stub found by the mark.
struct Named {
    /// The stub's backend path.
    stub: String,
    size: u64,
}

/// Sweeps every tier as of `now`.
pub(crate) async fn sweep(dal: &S3Dal, now: SystemTime) -> anyhow::Result<TierSweep> {
    let mut done = TierSweep::default();
    if !dal.has_tiers() {
        return Ok(done);
    }
    let named = mark(dal).await?;
    let mut by_tier: BTreeMap<&str, HashMap<&str, &Named>> = BTreeMap::new();
    for ((tier, id), stub) in &named {
        by_tier
            .entry(tier.as_str())
            .or_default()
            .insert(id.as_str(), stub);
    }
    let mut logged = 0;
    for tier in dal.tiers.iter() {
        let mut unseen = by_tier.remove(tier.name.as_str()).unwrap_or_default();
        let objects = unseen.len() as u64;
        let bytes = unseen.values().map(|n| n.size).sum();
        let swept = sweep_tier(dal, tier, now, &mut unseen).await?;
        done.orphans_removed += swept.orphans_removed;
        done.young_kept += swept.young_kept;
        let mut dangling = 0;
        for (id, stub) in unseen {
            if !is_dangling(dal, tier, id, stub).await {
                continue;
            }
            dangling += 1;
            if logged < DANGLING_LOGGED {
                logged += 1;
                tracing::error!(
                    path = stub.stub,
                    tier = tier.name,
                    id,
                    "a cold object's tier object is missing; its reads fail until it is restored \
                     or the object is deleted"
                );
            }
        }
        done.dangling += dangling;
        let stats = dal.metrics().tier(&tier.name);
        stats.orphans_removed(swept.orphans_removed);
        stats.swept(objects, bytes, dangling);
    }
    Ok(done)
}

/// Every stub of every bucket, current or archived, by its tier and tier object id.
async fn mark(dal: &S3Dal) -> anyhow::Result<HashMap<(String, String), Named>> {
    let mut named = HashMap::new();
    for bucket in usage::bucket_names(dal).await? {
        for dir in [naming::bucket_dir(&bucket), version::versions_dir(&bucket)] {
            for path in maybe_stubs(dal, &dir).await? {
                let Some(m) = store::stat_existing(dal, &path)
                    .await?
                    .filter(Metadata::is_file)
                else {
                    continue;
                };
                let obj = meta::resolve(&dal.profile, &m);
                // A damaged stub names no tier object it could be matched with.
                if let Some(TierRef { name, id: Some(id) }) = obj.tier {
                    let size = obj.content_length;
                    named.insert((name, id), Named { stub: path, size });
                }
            }
        }
    }
    Ok(named)
}

/// The files under `dir` that may be stubs: empty ones, and on fs only those carrying
/// `s3dal-tier`, read straight from the filesystem as the usage scan does.
async fn maybe_stubs(dal: &S3Dal, dir: &str) -> anyhow::Result<Vec<String>> {
    #[cfg(unix)]
    if super::fs::is_fs(&dal.op) {
        let root = std::path::PathBuf::from(dal.op.info().root());
        let dir = dir.to_owned();
        let found = super::fs::blocking(move || {
            tagged_empty_files(&root, &dir).map_err(opendal::raw::new_std_io_error)
        })
        .await?;
        return Ok(found);
    }
    let mut lister = match dal.op.lister_with(dir).recursive(true).await {
        Ok(lister) => lister,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err.into()),
    };
    let mut found = Vec::new();
    while let Some(entry) = lister.try_next().await? {
        let m = entry.metadata();
        if m.is_file() && m.content_length() == 0 && !entry.path().ends_with(".dm") {
            found.push(entry.path().to_owned());
        }
    }
    Ok(found)
}

/// The paths, relative to `root`, of the empty files under `dir` that carry `s3dal-tier`.
#[cfg(unix)]
fn tagged_empty_files(root: &std::path::Path, dir: &str) -> std::io::Result<Vec<String>> {
    let mut found = Vec::new();
    let mut dirs = vec![root.join(dir)];
    while let Some(at) = dirs.pop() {
        let entries = match std::fs::read_dir(&at) {
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
            let path = entry.path();
            let empty = std::fs::metadata(&path).is_ok_and(|m| m.is_file() && m.len() == 0);
            let tagged = || {
                xattr::get(&path, format!("user.{}", meta::META_TIER))
                    .ok()
                    .flatten()
                    .is_some()
            };
            if empty
                && tagged()
                && let Ok(rel) = path.strip_prefix(root)
            {
                found.push(rel.to_string_lossy().into_owned());
            }
        }
    }
    Ok(found)
}

/// Lists `tier`'s objects, keeps those `unseen` names (and takes them out of it, so what is left
/// there was not found), and removes the others older than the grace that their key still does
/// not name.
async fn sweep_tier(
    dal: &S3Dal,
    tier: &Tier,
    now: SystemTime,
    unseen: &mut HashMap<&str, &Named>,
) -> anyhow::Result<TierSweep> {
    let mut done = TierSweep::default();
    let top = format!("{RESERVED_ROOT}tier/");
    let mut lister = match tier.op.lister_with(&top).recursive(true).await {
        Ok(lister) => lister,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(done),
        Err(err) => return Err(err.into()),
    };
    let mut candidates = Vec::new();
    while let Some(entry) = lister.try_next().await? {
        if !entry.metadata().is_file() {
            continue;
        }
        // `<bucket>/<E(key)>!!/<id>`; anything else there (the claim) is not a tier object.
        let Some((bucket, key, id)) = entry
            .path()
            .strip_prefix(&top)
            .and_then(|rel| rel.split_once('/'))
            .and_then(|(bucket, rest)| {
                let (escaped, id) = rest.rsplit_once("!!/")?;
                Some((bucket, version::unescape(escaped)?, id))
            })
            .filter(|(_, _, id)| meta::is_tier_id(id))
        else {
            continue;
        };
        if unseen.remove(id).is_some() {
            continue;
        }
        let modified = match entry.metadata().last_modified() {
            Some(t) => Some(SystemTime::from(t)),
            // fs lists names only.
            None => match tier.op.stat(entry.path()).await {
                Ok(m) => m.last_modified().map(SystemTime::from),
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => return Err(err.into()),
            },
        };
        let young =
            modified.is_none_or(|t| now.duration_since(t).unwrap_or_default() <= TIER_GRACE);
        if young {
            done.young_kept += 1;
            continue;
        }
        candidates.push((
            entry.path().to_owned(),
            bucket.to_owned(),
            key,
            id.to_owned(),
        ));
    }
    for (path, bucket, key, id) in candidates {
        let wanted = TierRef {
            name: tier.name.clone(),
            id: Some(id),
        };
        match still_named(dal, &bucket, &key, &wanted).await {
            Ok(false) => {}
            Ok(true) => continue,
            Err(err) => {
                tracing::warn!(tier = tier.name, path, error = %err, "cannot check a tier object against its key");
                continue;
            }
        }
        match tier.op.delete(&path).await {
            Ok(()) => {
                done.orphans_removed += 1;
                if tier.profile.real_dirs {
                    prune(&tier.op, &path).await;
                }
            }
            Err(err) => {
                tracing::warn!(tier = tier.name, path, error = %err, "cannot remove a tier object no stub names");
            }
        }
    }
    if done.orphans_removed > 0 {
        tracing::info!(
            tier = tier.name,
            removed = done.orphans_removed,
            "removed tier objects no stub named"
        );
    }
    Ok(done)
}

/// Whether `key`'s current object or one of its archived versions names `wanted`: a stub
/// written after the mark, or one the mark could not see. A bucket that is gone names nothing.
async fn still_named(
    dal: &S3Dal,
    bucket: &str,
    key: &str,
    wanted: &TierRef,
) -> anyhow::Result<bool> {
    let names = |m: Option<Metadata>| {
        m.filter(Metadata::is_file).is_some_and(|m| {
            let obj: ObjectMeta = meta::resolve(&dal.profile, &m);
            obj.tier.as_ref() == Some(wanted)
        })
    };
    if names(store::stat_existing(dal, &naming::object_path(bucket, key)).await?) {
        return Ok(true);
    }
    for e in version::entries(dal, bucket, key).await? {
        if !e.marker && names(store::stat_existing(dal, &e.path).await?) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Whether the tier object `id` the mark found `stub` naming is really gone: the listing may
/// have missed it only because a write replaced the stub and removed it since the mark.
async fn is_dangling(dal: &S3Dal, tier: &Tier, id: &str, stub: &Named) -> bool {
    let Some(holder) = holder_of(&stub.stub) else {
        return false;
    };
    match tier.op.stat(&format!("{holder}{id}")).await {
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        _ => return false,
    }
    let wanted = TierRef {
        name: tier.name.clone(),
        id: Some(id.to_owned()),
    };
    store::stat_existing(dal, &stub.stub)
        .await
        .ok()
        .flatten()
        .filter(Metadata::is_file)
        .is_some_and(|m| meta::resolve(&dal.profile, &m).tier.as_ref() == Some(&wanted))
}

/// The holder in a tier of the key whose stub is at `stub`, a current object's or an archived
/// version's path.
fn holder_of(stub: &str) -> Option<String> {
    let versions = format!("{RESERVED_ROOT}versions/");
    let (bucket, key) = match stub.strip_prefix(&versions) {
        Some(rel) => {
            let (bucket, rest) = rel.split_once('/')?;
            let (escaped, _) = rest.rsplit_once("!!/")?;
            (bucket.to_owned(), version::unescape(escaped)?)
        }
        None => {
            let (bucket, key) = stub.split_once('/')?;
            (bucket.to_owned(), key.to_owned())
        }
    };
    Some(tier::holder(&bucket, &key))
}

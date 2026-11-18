//! The hot read cache (`--cache-size`): the content of small objects, in memory, for GET.
//!
//! An entry is used only for the backend ETag GetObject's stat just returned, so an object that
//! changed since it was cached, through this instance, another one sharing the backend, or a tool
//! writing to the backend directly, is never served from it: a different ETag misses, and the
//! read that fills the entry is pinned to the stat'ed ETag. OpenDAL's foyer layer, measured
//! first, keys by path alone and served an overwrite by another instance stale. The stat stays
//! on every GET; what a hit saves is the read, the backend round trip that dominates a small
//! object's GET.

use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use lru::LruCache;
use opendal::Operator;
use s3s::S3Result;

use super::S3Dal;
use super::tier::Cold;
use crate::error::{Ctx, map};
use crate::metrics::Metrics;

/// Objects larger than this are never cached. A hit saves the backend's time to first byte,
/// which dominates a GET of a small object and matters less the longer the transfer; one entry
/// of a large object would hold the room of many small ones; and a Range miss reads the whole
/// object to fill its entry, which this keeps to at most 1 MiB.
pub(super) const MAX_OBJECT: u64 = 1 << 20;

pub(crate) struct Cache {
    /// Bytes of content the entries may hold together.
    budget: u64,
    entries: Mutex<Entries>,
    metrics: Arc<Metrics>,
}

struct Entries {
    /// One entry per backend path: a newer version of an object replaces the older one rather
    /// than leaving it to age out.
    lru: LruCache<String, Entry>,
    bytes: u64,
}

struct Entry {
    etag: String,
    content: Bytes,
}

impl Cache {
    /// `None`, no cache, for a `budget` of 0, and on a backend that cannot pin a read to an
    /// ETag: there the bytes read could belong to a newer version than the one stat'ed, and be
    /// served under the older one's ETag later. fs is one (the setting is refused there anyway).
    pub(crate) fn new(budget: u64, op: &Operator, metrics: Arc<Metrics>) -> Option<Self> {
        if budget == 0 || !op.info().capability().read_with_if_match {
            return None;
        }
        Some(Self {
            budget,
            entries: Mutex::new(Entries {
                lru: LruCache::unbounded(),
                bytes: 0,
            }),
            metrics,
        })
    }

    /// Whether an object of `len` bytes is cached: not empty (a GET of one reads nothing) and
    /// within both [`MAX_OBJECT`] and the budget.
    pub(crate) fn admits(&self, len: u64) -> bool {
        len > 0 && len <= MAX_OBJECT && len <= self.budget
    }

    /// The content of the object at `path` as of backend ETag `etag`, counted as a hit or a miss.
    pub(crate) fn get(&self, path: &str, etag: &str) -> Option<Bytes> {
        let mut entries = self.lock();
        match entries.lru.get(path) {
            Some(entry) if entry.etag == etag => {
                self.metrics.cache_hit();
                Some(entry.content.clone())
            }
            _ => {
                self.metrics.cache_miss();
                None
            }
        }
    }

    /// Keeps `content`, which [`Self::admits`], as the object at `path` with backend ETag
    /// `etag`, evicting the least recently used entries until the budget holds them all.
    pub(crate) fn insert(&self, path: &str, etag: &str, content: Bytes) {
        let len = content.len() as u64;
        let mut entries = self.lock();
        let entry = Entry {
            etag: etag.to_owned(),
            content,
        };
        // Unbounded by count, so `push` returns only the entry it replaced at this path.
        if let Some((_, old)) = entries.lru.push(path.to_owned(), entry) {
            entries.bytes -= old.content.len() as u64;
        }
        entries.bytes += len;
        let mut evicted = 0;
        // The entry just pushed is the most recently used, and fits the budget alone, so it is
        // never the one evicted.
        while entries.bytes > self.budget {
            let Some((_, old)) = entries.lru.pop_lru() else {
                break;
            };
            entries.bytes -= old.content.len() as u64;
            evicted += 1;
        }
        self.metrics.cache_stored(entries.bytes, evicted);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Entries> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The whole content of the object GetObject stat'ed at `path`, `len` bytes with backend ETag
/// `etag`: from the cache, or read and kept there. `None` when the cache does not take it, and
/// GetObject streams the object from the backend as without a cache. `len` is the object's
/// logical size; a cold object's content is its tier object's.
pub(super) async fn content(
    dal: &S3Dal,
    path: &str,
    etag: Option<&str>,
    cold: Option<&Cold>,
    len: u64,
) -> S3Result<Option<Bytes>> {
    let Some(cache) = dal.cache.as_deref() else {
        return Ok(None);
    };
    if !cache.admits(len) {
        return Ok(None);
    }
    if let Some(cold) = cold {
        // Keyed by the tier object's id: every stub has the same backend ETag where that is
        // the content's MD5 (the empty one), so an overwrite by another cold object would hit.
        // The bytes behind an id never change, so the read needs no pinning.
        let id = format!("tier:{}", cold.id);
        if let Some(content) = cache.get(path, &id) {
            return Ok(Some(content));
        }
        let content = cold.read(dal, 0..len).await?;
        if content.len() as u64 != len {
            return Ok(None);
        }
        cache.insert(path, &id, content.clone());
        return Ok(Some(content));
    }
    let Some(etag) = etag else {
        return Ok(None);
    };
    if let Some(content) = cache.get(path, etag) {
        return Ok(Some(content));
    }
    // Whole even for a Range or a part: the next request may want any other part of it.
    let content = dal
        .op
        .read_with(path)
        .if_match(etag)
        .await
        .map_err(|err| map(err, Ctx::ObjectRead))?
        .to_bytes();
    // Pinned to `etag`, so this is the object stat'ed. Were the length to disagree anyway, the
    // ranges computed from the stat would not fit it; the streaming read answers as uncached.
    if content.len() as u64 != len {
        return Ok(None);
    }
    cache.insert(path, etag, content.clone());
    Ok(Some(content))
}

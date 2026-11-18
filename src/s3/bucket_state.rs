//! A bucket's own settings, F1 of features-2026-11.md: `.s3dal/buckets/<bucket>.json`, one file
//! per bucket, in the backend, so every instance sharing it agrees. Each process reads a bucket's
//! file at most once per [`TTL`]; its own changes apply at once.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime};

use opendal::ErrorKind;
use s3s::S3Result;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::lifecycle::Lifecycle;
use super::notification::Notification;
use super::{S3Dal, swap};
use crate::backend::RESERVED_ROOT;
use crate::error::{Ctx, map};

/// How long a process trusts a bucket's state it read (versioning.md Open question 2). A change
/// another instance made is followed within this time.
const TTL: Duration = Duration::from_secs(5);

/// Past this many cached buckets, the expired ones are dropped. Anonymous downloads look up the
/// bucket they name, so names nobody created must not pile up.
const CACHE_PRUNE_AT: usize = 4096;

/// The file. Every field is optional with a default, so a missing file, and a field written by
/// a newer s3dal, read as "not set".
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct BucketState {
    /// Absent: never versioned. Once set, it can be suspended, never unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub versioning: Option<Versioning>,
    /// Anyone may download the bucket's objects (GET and HEAD), without a key pair.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub public: bool,
    /// How much the bucket may hold; absent: no limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota: Option<Quota>,
    /// The bucket's lifecycle rules (PutBucketLifecycleConfiguration); absent: none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<Lifecycle>,
    /// Where the bucket's object events are sent (PutBucketNotificationConfiguration); absent:
    /// nowhere.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notification: Option<Notification>,
    /// When the file was last written, for whoever reads it by hand.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub changed: Option<SystemTime>,
    /// Fields of a newer s3dal sharing the backend. Kept as they are when this one rewrites the
    /// file, so changing one setting here never drops a setting made there.
    #[serde(flatten)]
    pub unknown: Map<String, Value>,
}

/// A bucket's quota: its objects' bytes, every version's, and optionally how many objects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Quota {
    pub max_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_objects: Option<u64>,
    /// Limits of a newer s3dal, kept as [`BucketState::unknown`] is.
    #[serde(flatten)]
    pub unknown: Map<String, Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Versioning {
    Enabled,
    Suspended,
}

/// Per process: the bucket states read lately.
#[derive(Default)]
pub(crate) struct Cache {
    entries: Mutex<HashMap<String, (Instant, Arc<BucketState>)>>,
}

impl Cache {
    fn cached(&self, bucket: &str) -> Option<Arc<BucketState>> {
        let entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        let (at, state) = entries.get(bucket)?;
        (at.elapsed() < TTL).then(|| state.clone())
    }

    fn remember(&self, bucket: &str, state: Arc<BucketState>) {
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        if entries.len() >= CACHE_PRUNE_AT {
            entries.retain(|_, (at, _)| at.elapsed() < TTL);
        }
        entries.insert(bucket.to_owned(), (Instant::now(), state));
    }
}

fn path(bucket: &str) -> String {
    format!("{RESERVED_ROOT}buckets/{bucket}.json")
}

/// The bucket's state, read at most once per [`TTL`]. A bucket without a file has the default
/// state; whether the bucket exists is not checked.
pub(crate) async fn get(dal: &S3Dal, bucket: &str) -> S3Result<Arc<BucketState>> {
    if let Some(state) = dal.bucket_states.cached(bucket) {
        return Ok(state);
    }
    let state = swap::load::<BucketState>(dal, &path(bucket))
        .await
        .map_err(|err| {
            if err.kind() == ErrorKind::Unexpected {
                tracing::error!(bucket, error = %err, "corrupt bucket state");
            }
            map(err, Ctx::Other)
        })?
        .unwrap_or_default();
    let state = Arc::new(state);
    dal.bucket_states.remember(bucket, state.clone());
    Ok(state)
}

/// Whether the bucket's usage is counted as it changes: only a bucket with a quota needs it,
/// and finding out what an overwrite replaces costs it a backend round trip.
pub(crate) async fn tracked(dal: &S3Dal, bucket: &str) -> S3Result<bool> {
    Ok(get(dal, bucket).await?.quota.is_some())
}

/// Changes the bucket's state as `change` does, keeping what other instances changed meanwhile
/// (see [`swap::update`]); the new state applies in this process at once.
pub(crate) async fn update(
    dal: &S3Dal,
    bucket: &str,
    change: impl Fn(&mut BucketState),
) -> S3Result<()> {
    let written = swap::update(dal, &path(bucket), |current: Option<BucketState>| {
        let mut state = current.unwrap_or_default();
        change(&mut state);
        state.changed = Some(SystemTime::now());
        Some(state)
    })
    .await
    .map_err(|err| map(err, Ctx::Other))?
    .expect("the change always writes");
    dal.bucket_states.remember(bucket, Arc::new(written));
    Ok(())
}

/// Removes the bucket's state, for DeleteBucket, and for CreateBucket of a name a deleted bucket
/// may have left it under.
pub(crate) async fn remove(dal: &S3Dal, bucket: &str) -> S3Result<()> {
    match dal.op.delete(&path(bucket)).await {
        Ok(()) => {}
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => return Err(map(err, Ctx::Other)),
    }
    dal.bucket_states
        .remember(bucket, Arc::new(BucketState::default()));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fields_of_a_newer_version_survive_a_rewrite() {
        // A newer instance sharing the backend may store a setting this one does not know;
        // changing `public` here must neither fail on it nor drop it.
        let newer = r#"{"versioning":"Enabled","thumbnails":{"sizes":[]},"quota":{"max-bytes":10,"max-parts":3},"changed":{"secs_since_epoch":1,"nanos_since_epoch":0}}"#;
        let mut state: BucketState = serde_json::from_str(newer).unwrap();
        assert_eq!(state.versioning, Some(Versioning::Enabled));
        assert_eq!(state.quota.as_ref().map(|q| q.max_bytes), Some(10));
        state.public = true;
        let written: Value = serde_json::to_value(&state).unwrap();
        assert_eq!(
            written["thumbnails"]["sizes"],
            serde_json::json!([]),
            "{written}"
        );
        assert_eq!(written["quota"]["max-parts"], 3, "{written}");
        assert_eq!(written["quota"]["max-bytes"], 10, "{written}");
        assert_eq!(written["public"], true, "{written}");
        assert_eq!(written["versioning"], "Enabled", "{written}");
    }

    #[test]
    fn a_default_state_writes_no_settings() {
        // Unset settings stay out of the file, so an older reader never meets a value it would
        // have to interpret.
        let written = serde_json::to_string(&BucketState::default()).unwrap();
        assert_eq!(written, "{}");
        let read: BucketState = serde_json::from_str("{}").unwrap();
        assert_eq!(read, BucketState::default());
    }
}

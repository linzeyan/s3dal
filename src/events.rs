//! Object events, F3 of features-2026-11.md: every change to a key that took effect (PUT, POST,
//! CopyObject, CompleteMultipartUpload, DeleteObject, each entry of DeleteObjects, each lifecycle
//! expiry and transition) is announced once, after it committed. Consumers subscribe to a bounded broadcast; one that falls behind
//! loses the oldest events, counted in `s3dal_events_dropped_total`, and never slows a request.
//!
//! The quota's own bookkeeping does not ride the broadcast: it is applied before the request
//! answers (see `S3Dal::emit`), so the next write already sees it, and nothing can drop it.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::SystemTime;

use tokio::sync::broadcast;

use crate::metrics::Metrics;

/// Events held for the slowest subscriber. Each is a few hundred bytes, so a full queue is about
/// a megabyte, and a subscriber that falls this far behind is not keeping up anyway.
const CAPACITY: usize = 4096;

/// What happened, named after the S3 event it becomes for notifications. Lifecycle expiry emits
/// through `S3Dal::emit` as the requests do, so the quota's usage follows it too.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    /// `s3:ObjectCreated:Put`.
    Put,
    /// `s3:ObjectCreated:Post`, a form upload.
    Post,
    /// `s3:ObjectCreated:Copy`.
    Copy,
    /// `s3:ObjectCreated:CompleteMultipartUpload`.
    Complete,
    /// `s3:ObjectRemoved:Delete`: an object, a version or a delete marker removed for good.
    Delete,
    /// `s3:ObjectRemoved:DeleteMarkerCreated`: a delete in a versioned bucket, which keeps the
    /// version it hides.
    DeleteMarker,
    /// `s3:LifecycleExpiration:Delete`: a lifecycle rule removed an object, a version or a
    /// delete marker for good.
    LifecycleDelete,
    /// `s3:LifecycleExpiration:DeleteMarkerCreated`: a lifecycle rule expired the current
    /// version in a versioned bucket, which keeps it behind a new delete marker.
    LifecycleDeleteMarker,
    /// `s3:LifecycleTransition`: a lifecycle rule moved an object or a version into a cold tier,
    /// keeping its bytes, ETag and version id. Its usage does not change.
    LifecycleTransition,
}

/// A change to what a bucket stores: objects (every version that is not a delete marker; folder
/// markers hold nothing and are not counted) and their bytes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Usage {
    pub objects: i64,
    pub bytes: i64,
}

impl Usage {
    /// One object of `size` bytes.
    pub fn object(size: u64) -> Self {
        Self {
            objects: 1,
            bytes: i64::try_from(size).unwrap_or(i64::MAX),
        }
    }

    pub fn plus(self, other: Self) -> Self {
        Self {
            objects: self.objects.saturating_add(other.objects),
            bytes: self.bytes.saturating_add(other.bytes),
        }
    }

    pub fn minus(self, other: Self) -> Self {
        Self {
            objects: self.objects.saturating_sub(other.objects),
            bytes: self.bytes.saturating_sub(other.bytes),
        }
    }
}

/// One change to a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectEvent {
    pub kind: EventKind,
    pub bucket: String,
    pub key: String,
    /// The object written; `None` for a removal.
    pub size: Option<u64>,
    /// The object written; `None` for a removal.
    pub etag: Option<String>,
    /// The version written, removed, or the delete marker created; `None` in a bucket that
    /// never had versioning.
    pub version: Option<String>,
    /// The request's `x-amz-request-id`; `None` for a lifecycle expiry, which no request made.
    pub request_id: Option<String>,
    /// The key pair that signed the request; `None` for a lifecycle expiry.
    pub access_key: Option<String>,
    /// The address the request came from; `None` for a lifecycle expiry.
    pub source_ip: Option<IpAddr>,
    /// What the change did to the bucket's usage: an overwrite in an unversioned bucket replaces
    /// the old object's bytes, one in a versioned bucket keeps them, a delete marker frees
    /// nothing. Only known in a bucket with a quota, where the write looks up what it replaces;
    /// elsewhere that lookup would cost every overwrite a backend round trip. `None` there.
    pub usage: Option<Usage>,
    pub at: SystemTime,
}

impl ObjectEvent {
    /// An event of `kind` on `bucket/key` from `origin`, happening now, with nothing else known.
    pub(crate) fn new(kind: EventKind, bucket: &str, key: &str, origin: Origin) -> Self {
        Self {
            kind,
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            size: None,
            etag: None,
            version: None,
            request_id: origin.request_id,
            access_key: origin.access_key,
            source_ip: origin.source_ip,
            usage: None,
            at: SystemTime::now(),
        }
    }
}

/// The request a change came from, for its event.
#[derive(Debug, Clone, Default)]
pub(crate) struct Origin {
    pub request_id: Option<String>,
    pub access_key: Option<String>,
    pub source_ip: Option<IpAddr>,
}

impl Origin {
    pub(crate) fn of<T>(req: &s3s::S3Request<T>) -> Self {
        Self {
            request_id: req
                .extensions
                .get::<crate::server::RequestId>()
                .map(|id| id.as_str().to_owned()),
            access_key: req.credentials.as_ref().map(|c| c.access_key.clone()),
            source_ip: req.extensions.get::<crate::server::Peer>().map(|p| p.0),
        }
    }
}

/// The broadcast every process has one of.
pub(crate) struct Hub {
    sender: broadcast::Sender<ObjectEvent>,
}

impl Hub {
    pub(crate) fn new() -> Self {
        Self::with_capacity(CAPACITY)
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            sender: broadcast::channel(capacity).0,
        }
    }

    /// Whether anyone listens; without subscribers an event is not even built.
    pub(crate) fn heard(&self) -> bool {
        self.sender.receiver_count() > 0
    }

    /// Never waits: a full queue drops its oldest event for whoever has not read it.
    pub(crate) fn send(&self, event: ObjectEvent) {
        let _ = self.sender.send(event);
    }

    pub(crate) fn subscribe(&self, metrics: Arc<Metrics>) -> Subscriber {
        Subscriber {
            receiver: self.sender.subscribe(),
            metrics,
        }
    }
}

/// One consumer's view of the events, from when it subscribed.
pub struct Subscriber {
    receiver: broadcast::Receiver<ObjectEvent>,
    metrics: Arc<Metrics>,
}

impl Subscriber {
    /// The next event, waiting for one. Events this subscriber was too slow for are skipped
    /// and counted; `None` once the server is gone.
    pub async fn recv(&mut self) -> Option<ObjectEvent> {
        loop {
            match self.receiver.recv().await {
                Ok(event) => return Some(event),
                Err(broadcast::error::RecvError::Lagged(lost)) => {
                    self.metrics.events_dropped(lost);
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }

    /// The next event if one is waiting, without waiting.
    pub fn try_recv(&mut self) -> Option<ObjectEvent> {
        loop {
            match self.receiver.try_recv() {
                Ok(event) => return Some(event),
                Err(broadcast::error::TryRecvError::Lagged(lost)) => {
                    self.metrics.events_dropped(lost);
                }
                Err(_) => return None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(key: &str) -> ObjectEvent {
        ObjectEvent {
            kind: EventKind::Put,
            bucket: "b".into(),
            key: key.into(),
            size: Some(1),
            etag: None,
            version: None,
            request_id: None,
            access_key: None,
            source_ip: None,
            usage: None,
            at: SystemTime::now(),
        }
    }

    #[test]
    fn a_subscriber_that_falls_behind_loses_the_oldest_events_and_counts_them() {
        // A notification target that stalls must neither hold up requests nor make the queue
        // grow without bound; what it missed must show in the metrics.
        let metrics = Arc::new(Metrics::new());
        let hub = Hub::with_capacity(4);
        let mut slow = hub.subscribe(metrics.clone());
        for i in 0..10 {
            hub.send(event(&i.to_string()));
        }
        let kept: Vec<String> = std::iter::from_fn(|| slow.try_recv())
            .map(|e| e.key)
            .collect();
        assert_eq!(kept, ["6", "7", "8", "9"]);
        assert!(
            metrics.render().contains("s3dal_events_dropped_total 6\n"),
            "{}",
            metrics.render()
        );
    }

    #[test]
    fn without_subscribers_nothing_is_heard() {
        let hub = Hub::new();
        assert!(!hub.heard());
        hub.send(event("k"));
        let _sub = hub.subscribe(Arc::new(Metrics::new()));
        assert!(hub.heard());
    }
}

//! The numbers behind `GET /_s3dal/metrics`, in Prometheus's text format, and behind the
//! console's dashboard. Every request touches them, so they are atomics in fixed arrays: an
//! operation is an index into a static list rather than a label string built per request, and
//! only a scrape and the 10-second sampler take a lock.

use std::cell::Cell;
use std::collections::{BTreeMap, VecDeque};
use std::fmt::{self, Write as _};
use std::future::Future;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicU64};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use opendal::ErrorKind;
use opendal_layer_observe_metrics_common::{MetricLabels, MetricValue, MetricsIntercept};
use serde_json::{Map, Value, json};

use crate::config::MetricsMode;
use crate::s3::{ExpiredKind, SkipReason};

/// The S3 operations s3dal answers, sorted for a binary search, and `ConsoleAdmin` for the
/// console's settings API. Bounded on purpose: a label per name a client may send would let any
/// client grow the scrape without limit.
const OPERATIONS: [&str; 49] = [
    "AbortMultipartUpload",
    "CompleteMultipartUpload",
    "ConsoleAdmin",
    "CopyObject",
    "CreateBucket",
    "CreateMultipartUpload",
    "DeleteBucket",
    "DeleteBucketLifecycle",
    "DeleteObject",
    "DeleteObjects",
    "GetBucketAccelerateConfiguration",
    "GetBucketAcl",
    "GetBucketCors",
    "GetBucketEncryption",
    "GetBucketLifecycleConfiguration",
    "GetBucketLocation",
    "GetBucketLogging",
    "GetBucketNotificationConfiguration",
    "GetBucketOwnershipControls",
    "GetBucketPolicy",
    "GetBucketPolicyStatus",
    "GetBucketReplication",
    "GetBucketRequestPayment",
    "GetBucketTagging",
    "GetBucketVersioning",
    "GetBucketWebsite",
    "GetObject",
    "GetObjectAcl",
    "GetObjectAttributes",
    "GetObjectLockConfiguration",
    "GetObjectTagging",
    "GetPublicAccessBlock",
    "HeadBucket",
    "HeadObject",
    "ListBuckets",
    "ListMultipartUploads",
    "ListObjectVersions",
    "ListObjects",
    "ListObjectsV2",
    "ListParts",
    "PostObject",
    "PutBucketAcl",
    "PutBucketLifecycleConfiguration",
    "PutBucketNotificationConfiguration",
    "PutBucketVersioning",
    "PutObject",
    "PutObjectAcl",
    "UploadPart",
    "UploadPartCopy",
];
/// An operation s3 knows but s3dal does not implement, answered 501.
const OTHER: usize = OPERATIONS.len();
/// Refused before an operation was known: a bad signature, a malformed request.
const UNKNOWN: usize = OTHER + 1;
const OP_SLOTS: usize = UNKNOWN + 1;

fn operation_label(i: usize) -> &'static str {
    match i {
        OTHER => "Other",
        UNKNOWN => "Unknown",
        i => OPERATIONS[i],
    }
}

const STATUS_CLASSES: [&str; 5] = ["1xx", "2xx", "3xx", "4xx", "5xx"];
const PROTOCOLS: [&str; 2] = ["http1", "http2"];

/// From half a millisecond, a small object on a local disk, to a minute, a large transfer.
const BOUNDS: [f64; 16] = [
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
    60.0,
];
const BOUND_LABELS: [&str; 16] = [
    "0.0005", "0.001", "0.0025", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1",
    "2.5", "5", "10", "30", "60",
];
/// The bounds in microseconds, which is what an observation is compared in.
const BOUNDS_US: [u64; 16] = [
    500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000,
    2_500_000, 5_000_000, 10_000_000, 30_000_000, 60_000_000,
];
/// One per bound, and `+Inf`.
const BUCKETS: usize = BOUNDS.len() + 1;

/// OpenDAL's operation names, as its metrics layer labels them.
const BACKEND_OPS: [&str; 12] = [
    "info",
    "create_dir",
    "read",
    "write",
    "copy",
    "compose",
    "rename",
    "restore",
    "stat",
    "delete",
    "list",
    "presign",
];
/// OpenDAL's error kinds but `Unexpected` (see [`BackendMetrics`]); the list is open-ended, so a
/// kind added later is `Other`.
const ERROR_KINDS: [&str; 13] = [
    "Unsupported",
    "ConfigInvalid",
    "NotFound",
    "PermissionDenied",
    "IsADirectory",
    "NotADirectory",
    "AlreadyExists",
    "RateLimited",
    "IsSameFile",
    "ConditionNotMatch",
    "RangeNotSatisfied",
    "Conflict",
    "Other",
];

/// The dashboard's resolution and reach: an hour, at ten seconds.
pub const SAMPLE_INTERVAL: Duration = Duration::from_secs(10);
const SAMPLES: usize = 360;

tokio::task_local! {
    /// The operation of the request this task serves, set where s3s names it.
    static OPERATION: Cell<usize>;
}

/// Records which operation the current request is, from where s3s resolves it. Outside a
/// request (a test calling the service directly) it does nothing.
pub(crate) fn note_operation(name: &str) {
    let i = OPERATIONS.binary_search(&name).unwrap_or(OTHER);
    let _ = OPERATION.try_with(|op| op.set(i));
}

/// Runs a request's service call, and returns with its output the operation it turned out to
/// be. A task-local rather than a request extension: s3s hands the access check the request's
/// extensions, but the response it builds carries none of them back.
pub(crate) async fn with_operation<F: Future>(call: F) -> (F::Output, usize) {
    OPERATION
        .scope(Cell::new(UNKNOWN), async {
            let out = call.await;
            (out, OPERATION.with(Cell::get))
        })
        .await
}

#[derive(Default)]
struct Histogram {
    /// Not cumulative: a scrape adds them up, so an observation touches one bucket.
    buckets: [AtomicU64; BUCKETS],
    sum_us: AtomicU64,
}

impl Histogram {
    fn observe(&self, d: Duration) {
        let us = u64::try_from(d.as_micros()).unwrap_or(u64::MAX);
        // Prometheus buckets are inclusive: a value equal to a bound counts in it.
        let i = BOUNDS_US.partition_point(|&b| b < us);
        self.buckets[i].fetch_add(1, Relaxed);
        self.sum_us.fetch_add(us, Relaxed);
    }

    fn load(&self) -> [u64; BUCKETS] {
        std::array::from_fn(|i| self.buckets[i].load(Relaxed))
    }
}

/// A job's name and the outcome of its runs, `ok` or `error`.
type JobOutcome = (&'static str, &'static str);

/// What a bucket holds and its quota, as the bucket gauges show them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BucketGauges {
    pub objects: u64,
    pub bytes: u64,
    pub max_bytes: Option<u64>,
    pub max_objects: Option<u64>,
}

/// Where a write into a tier came from, as `s3dal_tier_writes_total` labels it.
#[derive(Debug, Clone, Copy)]
pub(crate) enum WriteOrigin {
    /// A PUT, form upload, copy or Complete naming the tier's class.
    Request,
    /// A lifecycle transition.
    Lifecycle,
}

const WRITE_ORIGINS: [&str; 2] = ["request", "lifecycle"];

/// One tier's traffic, and the gauges its last sweep set.
#[derive(Default)]
pub(crate) struct TierStats {
    reads: AtomicU64,
    read_bytes: AtomicU64,
    writes: [AtomicU64; WRITE_ORIGINS.len()],
    written_bytes: [AtomicU64; WRITE_ORIGINS.len()],
    orphans_removed: AtomicU64,
    /// Failed operations by operation and error kind. They are rare, so a lock costs nothing.
    errors: Mutex<BTreeMap<(&'static str, &'static str), u64>>,
    /// `(objects, bytes, dangling)` from the last sweep; none before one ran here.
    swept: Mutex<Option<(u64, u64, u64)>>,
}

impl TierStats {
    /// A read of a cold object started.
    pub(crate) fn read(&self) {
        self.reads.fetch_add(1, Relaxed);
    }

    pub(crate) fn read_bytes(&self, n: u64) {
        self.read_bytes.fetch_add(n, Relaxed);
    }

    /// A tier object of `bytes` became a cold object.
    pub(crate) fn wrote(&self, origin: WriteOrigin, bytes: u64) {
        self.writes[origin as usize].fetch_add(1, Relaxed);
        self.written_bytes[origin as usize].fetch_add(bytes, Relaxed);
    }

    /// `kind` is an OpenDAL error kind's name, or `missing` for a tier object a stub names that
    /// is not there.
    pub(crate) fn failed(&self, operation: &'static str, kind: &'static str) {
        *self
            .errors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry((operation, kind))
            .or_default() += 1;
    }

    pub(crate) fn orphans_removed(&self, n: u64) {
        self.orphans_removed.fetch_add(n, Relaxed);
    }

    /// What the sweep found: the cold objects naming the tier, their bytes, and the tier objects
    /// stubs name that are not there.
    pub(crate) fn swept(&self, objects: u64, bytes: u64, dangling: u64) {
        *self.swept.lock().unwrap_or_else(PoisonError::into_inner) =
            Some((objects, bytes, dangling));
    }

    /// `(objects, bytes, dangling)` from the last sweep this process ran; `None` before one.
    pub(crate) fn last_sweep(&self) -> Option<(u64, u64, u64)> {
        *self.swept.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Why a notification was given up on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DropReason {
    /// The target's queue was full when the event came.
    Overflow,
    /// Still failing when the target's `retry-for` ran out.
    RetriesExhausted,
    /// The target answered with a 4xx, which it would answer again.
    Refused,
}

impl DropReason {
    pub(crate) const ALL: [Self; 3] = [Self::Overflow, Self::RetriesExhausted, Self::Refused];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Overflow => "overflow",
            Self::RetriesExhausted => "retries_exhausted",
            Self::Refused => "refused",
        }
    }
}

/// One notification target's deliveries since the server started, and its last attempt, for
/// the metrics and the console.
#[derive(Default)]
pub(crate) struct NotifyStats {
    pub(crate) delivered: AtomicU64,
    pub(crate) failed: AtomicU64,
    pub(crate) dropped: [AtomicU64; DropReason::ALL.len()],
    pub(crate) queued: AtomicI64,
    last: Mutex<Option<Attempt>>,
}

/// The outcome of a delivery attempt.
#[derive(Debug, Clone)]
pub(crate) struct Attempt {
    pub(crate) at: SystemTime,
    /// `None` for an attempt that was accepted.
    pub(crate) error: Option<String>,
}

impl NotifyStats {
    pub(crate) fn dropped(&self, reason: DropReason) {
        self.dropped[reason as usize].fetch_add(1, Relaxed);
    }

    pub(crate) fn attempted(&self, error: Option<String>) {
        let counter = if error.is_none() {
            &self.delivered
        } else {
            &self.failed
        };
        counter.fetch_add(1, Relaxed);
        *self.last.lock().unwrap_or_else(PoisonError::into_inner) = Some(Attempt {
            at: SystemTime::now(),
            error,
        });
    }

    pub(crate) fn last(&self) -> Option<Attempt> {
        self.last
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The counters and the last attempt as the console shows them.
    pub(crate) fn json(&self) -> Value {
        let dropped: serde_json::Map<String, Value> = DropReason::ALL
            .iter()
            .map(|r| {
                (
                    r.label().to_owned(),
                    json!(self.dropped[*r as usize].load(Relaxed)),
                )
            })
            .collect();
        json!({
            "delivered": self.delivered.load(Relaxed),
            "failed": self.failed.load(Relaxed),
            "dropped": dropped,
            "queued": self.queued.load(Relaxed).max(0),
            "last": self.last().map(|a| json!({ "at": unix_secs(a.at), "error": a.error })),
        })
    }
}

/// Every counter of one server, from its start.
pub struct Metrics {
    started: SystemTime,
    requests: [[AtomicU64; 5]; OP_SLOTS],
    durations: [Histogram; OP_SLOTS],
    received: AtomicU64,
    sent: AtomicU64,
    in_flight: AtomicI64,
    connections_open: [AtomicI64; 2],
    connections: [AtomicU64; 2],
    backend_durations: [Histogram; BACKEND_OPS.len()],
    backend_errors: [[AtomicU64; ERROR_KINDS.len()]; BACKEND_OPS.len()],
    multipart_open: AtomicI64,
    /// Object events a subscriber fell too far behind to read.
    events_dropped: AtomicU64,
    /// What lifecycle rules removed, by [`ExpiredKind`].
    lifecycle_expired: [AtomicU64; ExpiredKind::ALL.len()],
    /// The read cache (`--cache-size`): lookups answered and not, entries evicted for room, and
    /// the bytes of content it holds.
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    cache_evictions: AtomicU64,
    cache_bytes: AtomicU64,
    /// Background job runs by job and outcome: how many, and how long they took together. A job
    /// runs a few times an hour, so a lock costs nothing here.
    jobs: Mutex<BTreeMap<JobOutcome, (u64, Duration)>>,
    /// Whether this process holds each job's lease, and so is the one running it.
    leases: Mutex<BTreeMap<&'static str, bool>>,
    /// What each bucket this process counted holds, and its quota.
    buckets: Mutex<BTreeMap<String, BucketGauges>>,
    /// Each notification target's deliveries, by its id.
    notify: Mutex<BTreeMap<String, Arc<NotifyStats>>>,
    /// Each cold tier's traffic and what its last sweep found, by its name; empty without tiers,
    /// which then render none of their series.
    tiers: Mutex<BTreeMap<String, Arc<TierStats>>>,
    /// Lifecycle transitions given up on, by [`SkipReason`].
    transitions_skipped: [AtomicU64; SkipReason::ALL.len()],
    probe: Mutex<Option<Probe>>,
    ring: Mutex<Ring>,
    sampling: AtomicBool,
}

impl fmt::Debug for Metrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Metrics").finish_non_exhaustive()
    }
}

/// The backend check a server ran at startup.
#[derive(Clone, Copy)]
struct Probe {
    at: SystemTime,
    took: Duration,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            started: SystemTime::now(),
            requests: std::array::from_fn(|_| Default::default()),
            durations: std::array::from_fn(|_| Histogram::default()),
            received: AtomicU64::new(0),
            sent: AtomicU64::new(0),
            in_flight: AtomicI64::new(0),
            connections_open: Default::default(),
            connections: Default::default(),
            backend_durations: std::array::from_fn(|_| Histogram::default()),
            backend_errors: std::array::from_fn(|_| Default::default()),
            multipart_open: AtomicI64::new(0),
            events_dropped: AtomicU64::new(0),
            lifecycle_expired: Default::default(),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            cache_evictions: AtomicU64::new(0),
            cache_bytes: AtomicU64::new(0),
            jobs: Mutex::default(),
            leases: Mutex::default(),
            buckets: Mutex::default(),
            notify: Mutex::default(),
            tiers: Mutex::default(),
            transitions_skipped: Default::default(),
            probe: Mutex::new(None),
            // Its first snapshot is all zeroes, as the counters are.
            ring: Mutex::new(Ring::default()),
            sampling: AtomicBool::new(false),
        }
    }

    /// The interceptor for OpenDAL's metrics layer, which times every backend operation.
    pub(crate) fn backend(self: &Arc<Self>) -> BackendMetrics {
        BackendMetrics(self.clone())
    }

    /// Counts a request as in flight until the returned value is dropped, which then counts it
    /// as answered.
    pub(crate) fn request(self: &Arc<Self>) -> RequestMetrics {
        self.in_flight.fetch_add(1, Relaxed);
        RequestMetrics {
            metrics: self.clone(),
            start: Instant::now(),
            operation: UNKNOWN,
            // What a request the service fails without an answer counts as.
            status: 500,
        }
    }

    /// Counts a connection as open, once it is known which protocol it speaks, until the
    /// returned value is dropped.
    pub(crate) fn connection(self: &Arc<Self>) -> ConnectionMetrics {
        ConnectionMetrics {
            metrics: self.clone(),
            protocol: AtomicU8::new(0),
        }
    }

    pub(crate) fn received(&self, n: usize) {
        self.received.fetch_add(n as u64, Relaxed);
    }

    pub(crate) fn multipart_opened(&self) {
        self.multipart_open.fetch_add(1, Relaxed);
    }

    pub(crate) fn multipart_closed(&self) {
        self.multipart_open.fetch_sub(1, Relaxed);
    }

    /// What a sweep counted on the backend, which also covers uploads other instances on the
    /// same backend started or finished.
    pub(crate) fn set_multipart_open(&self, n: usize) {
        self.multipart_open
            .store(i64::try_from(n).unwrap_or(i64::MAX), Relaxed);
    }

    pub(crate) fn events_dropped(&self, n: u64) {
        self.events_dropped.fetch_add(n, Relaxed);
    }

    pub(crate) fn lifecycle_expired(&self, kind: ExpiredKind, n: usize) {
        self.lifecycle_expired[kind as usize].fetch_add(n as u64, Relaxed);
    }

    pub(crate) fn cache_hit(&self) {
        self.cache_hits.fetch_add(1, Relaxed);
    }

    pub(crate) fn cache_miss(&self) {
        self.cache_misses.fetch_add(1, Relaxed);
    }

    /// After an entry was stored: what the cache holds now, and how many entries made room.
    pub(crate) fn cache_stored(&self, bytes: u64, evicted: u64) {
        self.cache_bytes.store(bytes, Relaxed);
        self.cache_evictions.fetch_add(evicted, Relaxed);
    }

    pub(crate) fn job_ran(&self, job: &'static str, ok: bool, took: Duration) {
        let outcome = if ok { "ok" } else { "error" };
        let mut jobs = self.jobs.lock().unwrap_or_else(PoisonError::into_inner);
        let (runs, total) = jobs.entry((job, outcome)).or_default();
        *runs += 1;
        *total += took;
    }

    pub(crate) fn set_lease_held(&self, job: &'static str, held: bool) {
        self.leases
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(job, held);
    }

    /// What the bucket holds and its quota; `None` for a bucket deleted.
    pub(crate) fn set_bucket(&self, bucket: &str, gauges: Option<BucketGauges>) {
        let mut buckets = self.buckets.lock().unwrap_or_else(PoisonError::into_inner);
        match gauges {
            Some(gauges) => buckets.insert(bucket.to_owned(), gauges),
            None => buckets.remove(bucket),
        };
    }

    /// The counters of notification target `id`, the same ones for as long as a target of that
    /// id is configured, so a target edited by a reload keeps counting where it was.
    pub(crate) fn notify_target(&self, id: &str) -> Arc<NotifyStats> {
        self.notify
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(id.to_owned())
            .or_default()
            .clone()
    }

    /// The counters of the tier named `name`. Tiers are fixed at startup, so the series of every
    /// configured tier exist from then on, at zero.
    pub(crate) fn tier(&self, name: &str) -> Arc<TierStats> {
        self.tiers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(name.to_owned())
            .or_default()
            .clone()
    }

    pub(crate) fn transition_skipped(&self, reason: SkipReason) {
        self.transitions_skipped[reason as usize].fetch_add(1, Relaxed);
    }

    /// Forgets the targets no longer configured, whose series would otherwise stay forever.
    pub(crate) fn keep_notify_targets(&self, ids: &[&str]) {
        self.notify
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|id, _| ids.contains(&id.as_str()));
    }

    pub fn record_probe(&self, took: Duration) {
        *self.probe.lock().unwrap_or_else(PoisonError::into_inner) = Some(Probe {
            at: SystemTime::now(),
            took,
        });
    }

    /// The startup probe, for the console's server page.
    pub(crate) fn probe_json(&self) -> Value {
        let probe = *self.probe.lock().unwrap_or_else(PoisonError::into_inner);
        probe.map_or(
            Value::Null,
            |p| json!({ "at": unix_secs(p.at), "ms": p.took.as_millis() as u64 }),
        )
    }

    /// Samples every [`SAMPLE_INTERVAL`] for the dashboard, from the first call on, for as long
    /// as the metrics are in use.
    pub(crate) fn start_sampling(self: &Arc<Self>) {
        if self.sampling.swap(true, Relaxed) {
            return;
        }
        let weak = Arc::downgrade(self);
        tokio::spawn(sample_every(weak));
    }

    /// The text a scrape is answered with.
    pub fn render(&self) -> String {
        let mut out = String::with_capacity(16 * 1024);
        let o = &mut out;

        family(
            o,
            "s3dal_build_info",
            "gauge",
            "Always 1; the version is in the label.",
        );
        line(
            o,
            "s3dal_build_info",
            &format!("version=\"{}\"", env!("CARGO_PKG_VERSION")),
            1,
        );

        family(
            o,
            "s3dal_requests_total",
            "counter",
            "S3 requests answered, by operation and status class.",
        );
        for (op, classes) in self.requests.iter().enumerate() {
            for (class, n) in classes.iter().enumerate() {
                let n = n.load(Relaxed);
                if n > 0 {
                    let labels = format!(
                        "operation=\"{}\",status_class=\"{}\"",
                        operation_label(op),
                        STATUS_CLASSES[class]
                    );
                    line(o, "s3dal_requests_total", &labels, n);
                }
            }
        }

        let name = "s3dal_request_duration_seconds";
        family(
            o,
            name,
            "histogram",
            "From a request's headers to the end of its response body, by operation.",
        );
        for (op, h) in self.durations.iter().enumerate() {
            let labels = format!("operation=\"{}\"", operation_label(op));
            histogram(o, name, &labels, h);
        }

        family(
            o,
            "s3dal_received_bytes_total",
            "counter",
            "Request body bytes read.",
        );
        line(
            o,
            "s3dal_received_bytes_total",
            "",
            self.received.load(Relaxed),
        );
        family(
            o,
            "s3dal_sent_bytes_total",
            "counter",
            "Response body bytes sent.",
        );
        line(o, "s3dal_sent_bytes_total", "", self.sent.load(Relaxed));
        family(
            o,
            "s3dal_requests_in_flight",
            "gauge",
            "S3 requests whose response body has not ended.",
        );
        line(
            o,
            "s3dal_requests_in_flight",
            "",
            self.in_flight.load(Relaxed).max(0),
        );

        family(
            o,
            "s3dal_connections_open",
            "gauge",
            "Client connections open, by protocol.",
        );
        for (p, n) in self.connections_open.iter().enumerate() {
            if self.connections[p].load(Relaxed) > 0 {
                let labels = format!("protocol=\"{}\"", PROTOCOLS[p]);
                line(o, "s3dal_connections_open", &labels, n.load(Relaxed).max(0));
            }
        }
        family(
            o,
            "s3dal_connections_total",
            "counter",
            "Client connections accepted, by protocol.",
        );
        for (p, n) in self.connections.iter().enumerate() {
            let n = n.load(Relaxed);
            if n > 0 {
                let labels = format!("protocol=\"{}\"", PROTOCOLS[p]);
                line(o, "s3dal_connections_total", &labels, n);
            }
        }

        let name = "s3dal_backend_operation_duration_seconds";
        family(
            o,
            name,
            "histogram",
            "Backend (OpenDAL) operations, by operation.",
        );
        for (op, h) in self.backend_durations.iter().enumerate() {
            let labels = format!("operation=\"{}\"", BACKEND_OPS[op]);
            histogram(o, name, &labels, h);
        }
        family(
            o,
            "s3dal_backend_errors_total",
            "counter",
            "Backend (OpenDAL) operations that failed, by operation and error kind.",
        );
        for (op, kinds) in self.backend_errors.iter().enumerate() {
            for (kind, n) in kinds.iter().enumerate() {
                let n = n.load(Relaxed);
                if n > 0 {
                    let labels = format!(
                        "operation=\"{}\",kind=\"{}\"",
                        BACKEND_OPS[op], ERROR_KINDS[kind]
                    );
                    line(o, "s3dal_backend_errors_total", &labels, n);
                }
            }
        }

        family(
            o,
            "s3dal_multipart_uploads_open",
            "gauge",
            "Multipart uploads started and neither completed nor aborted.",
        );
        line(
            o,
            "s3dal_multipart_uploads_open",
            "",
            self.multipart_open.load(Relaxed).max(0),
        );
        family(
            o,
            "s3dal_job_duration_seconds",
            "summary",
            "Background job runs on this instance, by job and outcome: their count and how long \
             they took.",
        );
        for ((job, outcome), (runs, took)) in self
            .jobs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
        {
            let labels = format!("job=\"{job}\",outcome=\"{outcome}\"");
            line(
                o,
                "s3dal_job_duration_seconds_sum",
                &labels,
                took.as_secs_f64(),
            );
            line(o, "s3dal_job_duration_seconds_count", &labels, runs);
        }
        family(
            o,
            "s3dal_job_lease_held",
            "gauge",
            "1 if this instance holds the job's lease, and so runs it; 0 if another does.",
        );
        for (job, held) in self
            .leases
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
        {
            line(
                o,
                "s3dal_job_lease_held",
                &format!("job=\"{job}\""),
                u8::from(*held),
            );
        }
        let buckets = self
            .buckets
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        type Gauge = fn(&BucketGauges) -> Option<u64>;
        let bucket_families: [(&str, &str, Gauge); 4] = [
            (
                "s3dal_bucket_objects",
                "Objects in the bucket, every version counted: the last usage scan plus this \
                 instance's changes since, in a bucket with a quota.",
                |g| Some(g.objects),
            ),
            (
                "s3dal_bucket_bytes",
                "Bytes in the bucket, every version counted, as s3dal_bucket_objects counts them.",
                |g| Some(g.bytes),
            ),
            (
                "s3dal_bucket_quota_bytes",
                "The bucket's quota in bytes, for a bucket with one.",
                |g| g.max_bytes,
            ),
            (
                "s3dal_bucket_quota_objects",
                "The bucket's quota in objects, for a bucket with one that limits them.",
                |g| g.max_objects,
            ),
        ];
        for (name, help, value) in bucket_families {
            family(o, name, "gauge", help);
            for (bucket, gauges) in &buckets {
                if let Some(n) = value(gauges) {
                    line(o, name, &format!("bucket=\"{bucket}\""), n);
                }
            }
        }
        family(
            o,
            "s3dal_events_dropped_total",
            "counter",
            "Object events a subscriber missed because it fell too far behind.",
        );
        line(
            o,
            "s3dal_events_dropped_total",
            "",
            self.events_dropped.load(Relaxed),
        );
        let targets = self
            .notify
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        family(
            o,
            "s3dal_notify_deliveries_total",
            "counter",
            "Event notifications sent to each target: ok, accepted with a 2xx; failed, an attempt \
             that failed and was retried or dropped.",
        );
        for (id, stats) in &targets {
            for (outcome, n) in [("ok", &stats.delivered), ("failed", &stats.failed)] {
                line(
                    o,
                    "s3dal_notify_deliveries_total",
                    &format!("target=\"{id}\",outcome=\"{outcome}\""),
                    n.load(Relaxed),
                );
            }
        }
        family(
            o,
            "s3dal_notify_dropped_total",
            "counter",
            "Event notifications given up on, by target and reason: overflow, the target's queue \
             was full; retries_exhausted, still failing at the end of retry-for; refused, a 4xx \
             answer.",
        );
        for (id, stats) in &targets {
            for reason in DropReason::ALL {
                line(
                    o,
                    "s3dal_notify_dropped_total",
                    &format!("target=\"{id}\",reason=\"{}\"", reason.label()),
                    stats.dropped[reason as usize].load(Relaxed),
                );
            }
        }
        family(
            o,
            "s3dal_notify_queue_depth",
            "gauge",
            "Event notifications waiting in each target's queue.",
        );
        for (id, stats) in &targets {
            line(
                o,
                "s3dal_notify_queue_depth",
                &format!("target=\"{id}\""),
                stats.queued.load(Relaxed).max(0),
            );
        }
        family(
            o,
            "s3dal_lifecycle_expired_total",
            "counter",
            "What lifecycle rules removed on this instance, by kind: current versions expired, \
             noncurrent versions and delete markers removed, incomplete multipart uploads aborted.",
        );
        for kind in ExpiredKind::ALL {
            line(
                o,
                "s3dal_lifecycle_expired_total",
                &format!("kind=\"{}\"", kind.label()),
                self.lifecycle_expired[kind as usize].load(Relaxed),
            );
        }
        self.render_tiers(o);
        for (name, kind, help, value) in [
            (
                "s3dal_cache_hits_total",
                "counter",
                "GETs of objects the read cache takes whose content it held.",
                &self.cache_hits,
            ),
            (
                "s3dal_cache_misses_total",
                "counter",
                "GETs of objects the read cache takes whose content it did not hold, or held as of \
                 an older ETag, and read from the backend.",
                &self.cache_misses,
            ),
            (
                "s3dal_cache_evictions_total",
                "counter",
                "Objects the read cache dropped, least recently used first, to stay within \
                 cache-size.",
                &self.cache_evictions,
            ),
            (
                "s3dal_cache_bytes",
                "gauge",
                "Bytes of object content the read cache holds.",
                &self.cache_bytes,
            ),
        ] {
            family(o, name, kind, help);
            line(o, name, "", value.load(Relaxed));
        }
        family(
            o,
            "s3dal_log_forward_dropped_total",
            "counter",
            "Log lines not sent to the collector because its queue was full.",
        );
        line(
            o,
            "s3dal_log_forward_dropped_total",
            "",
            crate::logging::forward::state().dropped,
        );

        let process = process::sample();
        if let Some(rss) = process.rss {
            family(
                o,
                "process_resident_memory_bytes",
                "gauge",
                "Resident memory size in bytes.",
            );
            line(o, "process_resident_memory_bytes", "", rss);
        }
        if let Some(cpu) = process.cpu_secs {
            family(
                o,
                "process_cpu_seconds_total",
                "counter",
                "User and system CPU time spent in seconds.",
            );
            let _ = writeln!(o, "process_cpu_seconds_total {cpu}");
        }
        if let Some(fds) = process.open_fds {
            family(
                o,
                "process_open_fds",
                "gauge",
                "Number of open file descriptors.",
            );
            line(o, "process_open_fds", "", fds);
        }
        if let Some(fds) = process.max_fds {
            family(
                o,
                "process_max_fds",
                "gauge",
                "Maximum number of open file descriptors.",
            );
            line(o, "process_max_fds", "", fds);
        }
        family(
            o,
            "process_start_time_seconds",
            "gauge",
            "Start time of the server since unix epoch in seconds.",
        );
        line(o, "process_start_time_seconds", "", unix_secs(self.started));
        out
    }

    /// The `s3dal_tier_*` series, for a server with tiers only: one without has no such series
    /// at all, as before tiers existed.
    fn render_tiers(&self, o: &mut String) {
        let tiers = self
            .tiers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if tiers.is_empty() {
            return;
        }
        type Counter = fn(&TierStats) -> &AtomicU64;
        let per_tier: [(&str, &str, Counter); 3] = [
            (
                "s3dal_tier_reads_total",
                "Reads of cold objects from each tier: GETs, copy sources, thumbnails, cache fills.",
                |t| &t.reads,
            ),
            (
                "s3dal_tier_read_bytes_total",
                "Bytes read from each tier.",
                |t| &t.read_bytes,
            ),
            (
                "s3dal_tier_orphans_removed_total",
                "Tier objects no stub named that the tier sweep removed.",
                |t| &t.orphans_removed,
            ),
        ];
        for (name, help, value) in per_tier {
            family(o, name, "counter", help);
            for (tier, stats) in &tiers {
                line(
                    o,
                    name,
                    &format!("tier=\"{tier}\""),
                    value(stats).load(Relaxed),
                );
            }
        }
        type ByOrigin = fn(&TierStats) -> &[AtomicU64; WRITE_ORIGINS.len()];
        let by_origin: [(&str, &str, ByOrigin); 2] = [
            (
                "s3dal_tier_writes_total",
                "Objects written into each tier, by origin: request (a write naming the tier's \
                 class) or lifecycle (a transition).",
                |t| &t.writes,
            ),
            (
                "s3dal_tier_written_bytes_total",
                "Bytes written into each tier, by origin.",
                |t| &t.written_bytes,
            ),
        ];
        for (name, help, value) in by_origin {
            family(o, name, "counter", help);
            for (tier, stats) in &tiers {
                for (origin, n) in WRITE_ORIGINS.iter().zip(value(stats)) {
                    let labels = format!("tier=\"{tier}\",origin=\"{origin}\"");
                    line(o, name, &labels, n.load(Relaxed));
                }
            }
        }
        family(
            o,
            "s3dal_tier_transition_skipped_total",
            "counter",
            "Lifecycle transitions given up on, by reason: key-too-long (the key does not fit the \
             tier's paths), changed (the object changed while it was moved), error.",
        );
        for reason in SkipReason::ALL {
            line(
                o,
                "s3dal_tier_transition_skipped_total",
                &format!("reason=\"{}\"", reason.label()),
                self.transitions_skipped[reason as usize].load(Relaxed),
            );
        }
        family(
            o,
            "s3dal_tier_errors_total",
            "counter",
            "Tier operations that failed, by tier, operation and kind; kind missing is a read of a \
             tier object a stub names that is not there.",
        );
        for (tier, stats) in &tiers {
            let errors = stats
                .errors
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone();
            for ((operation, kind), n) in errors {
                let labels = format!("tier=\"{tier}\",operation=\"{operation}\",kind=\"{kind}\"");
                line(o, "s3dal_tier_errors_total", &labels, n);
            }
        }
        type Swept = fn((u64, u64, u64)) -> u64;
        let swept: [(&str, &str, Swept); 3] = [
            (
                "s3dal_tier_objects",
                "Cold objects (every version) naming each tier, as of its last sweep here.",
                |s| s.0,
            ),
            (
                "s3dal_tier_bytes",
                "Bytes of the cold objects naming each tier, as of its last sweep here.",
                |s| s.1,
            ),
            (
                "s3dal_tier_dangling",
                "Stubs naming a tier object that is not in the tier, as of its last sweep here.",
                |s| s.2,
            ),
        ];
        for (name, help, value) in swept {
            family(o, name, "gauge", help);
            for (tier, stats) in &tiers {
                if let Some(s) = stats.last_sweep() {
                    line(o, name, &format!("tier=\"{tier}\""), value(s));
                }
            }
        }
    }

    /// The last hour at [`SAMPLE_INTERVAL`], oldest first, and last the part of the current
    /// interval that has passed, so a request shows at once.
    pub(crate) fn stats(&self) -> Value {
        let now = self.snapshot();
        let ring = self.ring.lock().unwrap_or_else(PoisonError::into_inner);
        let current = Sample::between(&ring.last, &now, process::sample().rss);
        let samples: Vec<Value> = ring
            .samples
            .iter()
            .chain(std::iter::once(&current))
            .map(Sample::json)
            .collect();
        json!({
            "interval_secs": SAMPLE_INTERVAL.as_secs(),
            "now": unix_secs(now.at),
            "started": unix_secs(self.started),
            "samples": samples,
        })
    }

    fn sample(&self) {
        let now = self.snapshot();
        let rss = process::sample().rss;
        let mut ring = self.ring.lock().unwrap_or_else(PoisonError::into_inner);
        let sample = Sample::between(&ring.last, &now, rss);
        if ring.samples.len() == SAMPLES {
            ring.samples.pop_front();
        }
        ring.samples.push_back(sample);
        ring.last = now;
    }

    fn snapshot(&self) -> Snapshot {
        let mut classes = [0; 5];
        let mut buckets = [0; BUCKETS];
        let requests = std::array::from_fn(|op| {
            let mut total = 0;
            for (class, n) in self.requests[op].iter().enumerate() {
                let n = n.load(Relaxed);
                classes[class] += n;
                total += n;
            }
            for (b, n) in self.durations[op].load().iter().enumerate() {
                buckets[b] += n;
            }
            total
        });
        Snapshot {
            at: SystemTime::now(),
            mono: Instant::now(),
            requests,
            classes,
            buckets,
            received: self.received.load(Relaxed),
            sent: self.sent.load(Relaxed),
        }
    }
}

async fn sample_every(metrics: Weak<Metrics>) {
    let mut tick = tokio::time::interval(SAMPLE_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The first tick is immediate; the first sample covers a whole interval.
    tick.tick().await;
    loop {
        tick.tick().await;
        let Some(metrics) = metrics.upgrade() else {
            return;
        };
        metrics.sample();
    }
}

fn family(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} {kind}");
}

fn line(out: &mut String, name: &str, labels: &str, value: impl fmt::Display) {
    if labels.is_empty() {
        let _ = writeln!(out, "{name} {value}");
    } else {
        let _ = writeln!(out, "{name}{{{labels}}} {value}");
    }
}

/// Written only once it has counted something, so a scrape lists the operations in use rather
/// than all of them.
fn histogram(out: &mut String, name: &str, labels: &str, h: &Histogram) {
    let buckets = h.load();
    let count: u64 = buckets.iter().sum();
    if count == 0 {
        return;
    }
    let mut cumulative = 0;
    for (i, n) in buckets.iter().enumerate() {
        cumulative += n;
        let le = BOUND_LABELS.get(i).copied().unwrap_or("+Inf");
        let _ = writeln!(out, "{name}_bucket{{{labels},le=\"{le}\"}} {cumulative}");
    }
    let sum = h.sum_us.load(Relaxed) as f64 / 1e6;
    let _ = writeln!(out, "{name}_sum{{{labels}}} {sum}");
    let _ = writeln!(out, "{name}_count{{{labels}}} {count}");
}

fn unix_secs(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

/// One request, from its headers to the end of its response body.
pub(crate) struct RequestMetrics {
    metrics: Arc<Metrics>,
    start: Instant,
    pub(crate) operation: usize,
    pub(crate) status: u16,
}

impl RequestMetrics {
    pub(crate) fn sent(&self, n: usize) {
        self.metrics.sent.fetch_add(n as u64, Relaxed);
    }
}

impl Drop for RequestMetrics {
    fn drop(&mut self) {
        let m = &self.metrics;
        m.in_flight.fetch_sub(1, Relaxed);
        let class = usize::from(self.status / 100).clamp(1, 5) - 1;
        m.requests[self.operation][class].fetch_add(1, Relaxed);
        m.durations[self.operation].observe(self.start.elapsed());
    }
}

/// A connection, counted from its first request, which is when hyper has settled which protocol
/// it speaks. One that never sends a request, a port scan, is not counted.
pub(crate) struct ConnectionMetrics {
    metrics: Arc<Metrics>,
    /// 0 until the first request, then 1 + the index into [`PROTOCOLS`].
    protocol: AtomicU8,
}

impl ConnectionMetrics {
    /// Called for every request; only the first counts. HTTP/2 requests of one connection may
    /// arrive at once.
    pub(crate) fn speaks(&self, http2: bool) {
        if self.protocol.load(Relaxed) != 0 {
            return;
        }
        let p = usize::from(http2);
        let tag = 1 + u8::from(http2);
        if self
            .protocol
            .compare_exchange(0, tag, Relaxed, Relaxed)
            .is_ok()
        {
            self.metrics.connections_open[p].fetch_add(1, Relaxed);
            self.metrics.connections[p].fetch_add(1, Relaxed);
        }
    }
}

impl Drop for ConnectionMetrics {
    fn drop(&mut self) {
        if let Some(p) = usize::from(*self.protocol.get_mut()).checked_sub(1) {
            self.metrics.connections_open[p].fetch_sub(1, Relaxed);
        }
    }
}

/// Keeps the two values of OpenDAL's metrics layer that the scrape shows, the duration and the
/// errors of each operation, and ignores the rest.
#[derive(Clone, Debug)]
pub(crate) struct BackendMetrics(Arc<Metrics>);

impl MetricsIntercept for BackendMetrics {
    fn observe(&self, labels: MetricLabels, value: MetricValue) {
        let Some(op) = BACKEND_OPS.iter().position(|o| *o == labels.operation) else {
            return;
        };
        match value {
            MetricValue::OperationDurationSeconds(d) => self.0.backend_durations[op].observe(d),
            // The layer reports an operation dropped before its end as `Unexpected`: a future
            // cancelled when its client went away, and a reader that stops at the length it
            // asked for, which is every download from an object store. It gives a failure it
            // cannot classify the same kind, and nothing in what it reports tells the two
            // apart; counted, every GET would look like a backend error. Such a failure still
            // fails its request, which s3dal_requests_total counts as a 5xx.
            MetricValue::OperationErrorsTotal if labels.error == Some(ErrorKind::Unexpected) => {}
            MetricValue::OperationErrorsTotal => {
                let kind = labels.error.map_or("Other", ErrorKind::into_static);
                let kind = ERROR_KINDS
                    .iter()
                    .position(|k| *k == kind)
                    .unwrap_or(ERROR_KINDS.len() - 1);
                self.0.backend_errors[op][kind].fetch_add(1, Relaxed);
            }
            _ => {}
        }
    }
}

/// How a scrape is answered.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Scrape {
    Off,
    /// `token` mode with no token configured: no one may scrape.
    NoToken,
    Unauthorized,
    Allowed,
}

/// `authorization` is the request's header, if any.
pub(crate) fn authorize(
    mode: MetricsMode,
    token: Option<&str>,
    authorization: Option<&[u8]>,
) -> Scrape {
    match mode {
        MetricsMode::Off => Scrape::Off,
        MetricsMode::Public => Scrape::Allowed,
        MetricsMode::Token => {
            let Some(token) = token else {
                return Scrape::NoToken;
            };
            let given = authorization.and_then(|h| {
                let (scheme, rest) = h.split_at_checked(7)?;
                scheme
                    .eq_ignore_ascii_case(b"bearer ")
                    .then(|| rest.trim_ascii())
            });
            match given {
                Some(given) if same(given, token.as_bytes()) => Scrape::Allowed,
                _ => Scrape::Unauthorized,
            }
        }
    }
}

/// Takes as long whichever byte differs, so the time of a refusal does not tell how much of a
/// guess was right. The length is not hidden; it says little about a random token.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[derive(Default)]
struct Ring {
    last: Snapshot,
    samples: VecDeque<Sample>,
}

/// The counters at one instant; samples are the differences between two.
struct Snapshot {
    at: SystemTime,
    mono: Instant,
    requests: [u64; OP_SLOTS],
    classes: [u64; 5],
    buckets: [u64; BUCKETS],
    received: u64,
    sent: u64,
}

impl Default for Snapshot {
    fn default() -> Self {
        Self {
            at: SystemTime::now(),
            mono: Instant::now(),
            requests: [0; OP_SLOTS],
            classes: [0; 5],
            buckets: [0; BUCKETS],
            received: 0,
            sent: 0,
        }
    }
}

struct Sample {
    /// When it ends.
    at: SystemTime,
    secs: f64,
    /// Only the operations that had requests.
    requests: Vec<(usize, u64)>,
    classes: [u64; 5],
    p50: Option<f64>,
    p99: Option<f64>,
    received: u64,
    sent: u64,
    rss: Option<u64>,
}

impl Sample {
    fn between(a: &Snapshot, b: &Snapshot, rss: Option<u64>) -> Self {
        let buckets: [u64; BUCKETS] =
            std::array::from_fn(|i| b.buckets[i].saturating_sub(a.buckets[i]));
        Self {
            at: b.at,
            secs: b.mono.duration_since(a.mono).as_secs_f64(),
            requests: (0..OP_SLOTS)
                .map(|op| (op, b.requests[op].saturating_sub(a.requests[op])))
                .filter(|(_, n)| *n > 0)
                .collect(),
            classes: std::array::from_fn(|i| b.classes[i].saturating_sub(a.classes[i])),
            p50: quantile(&buckets, 0.5),
            p99: quantile(&buckets, 0.99),
            received: b.received.saturating_sub(a.received),
            sent: b.sent.saturating_sub(a.sent),
            rss,
        }
    }

    fn json(&self) -> Value {
        let requests: Map<String, Value> = self
            .requests
            .iter()
            .map(|(op, n)| (operation_label(*op).to_owned(), json!(n)))
            .collect();
        let status: Map<String, Value> = STATUS_CLASSES
            .iter()
            .zip(self.classes)
            .map(|(c, n)| ((*c).to_owned(), json!(n)))
            .collect();
        let ms = |s: Option<f64>| s.map(|s| (s * 1e6).round() / 1e3);
        json!({
            "t": unix_secs(self.at),
            "secs": (self.secs * 1e3).round() / 1e3,
            "requests": requests,
            "status": status,
            "p50_ms": ms(self.p50),
            "p99_ms": ms(self.p99),
            "bytes_in": self.received,
            "bytes_out": self.sent,
            "rss_bytes": self.rss,
        })
    }
}

/// The `q` quantile of a histogram's counts, in seconds, assuming the values in a bucket are
/// spread evenly across it, as Prometheus's `histogram_quantile` does. Past the last bound only
/// that bound is known.
fn quantile(buckets: &[u64; BUCKETS], q: f64) -> Option<f64> {
    let total: u64 = buckets.iter().sum();
    if total == 0 {
        return None;
    }
    let rank = q * total as f64;
    let mut seen = 0;
    for (i, &n) in buckets.iter().enumerate() {
        if n > 0 && (seen + n) as f64 >= rank {
            let lo = if i == 0 { 0.0 } else { BOUNDS[i - 1] };
            let Some(&hi) = BOUNDS.get(i) else {
                return Some(lo);
            };
            return Some(lo + (hi - lo) * (rank - seen as f64) / n as f64);
        }
        seen += n;
    }
    BOUNDS.last().copied()
}

/// The process's own resources, read when asked; `None` where the system does not tell.
mod process {
    #[derive(Default)]
    pub(super) struct Process {
        pub(super) rss: Option<u64>,
        pub(super) cpu_secs: Option<f64>,
        pub(super) open_fds: Option<u64>,
        pub(super) max_fds: Option<u64>,
    }

    #[cfg(unix)]
    pub(super) fn sample() -> Process {
        Process {
            rss: rss(),
            cpu_secs: cpu_secs(),
            open_fds: open_fds(),
            max_fds: max_fds(),
        }
    }

    #[cfg(not(unix))]
    pub(super) fn sample() -> Process {
        Process::default()
    }

    #[cfg(unix)]
    fn cpu_secs() -> Option<f64> {
        // SAFETY: rusage is plain integers, for which all zeroes is a value.
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        // SAFETY: the pointer is to a whole rusage, which getrusage only writes.
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, &raw mut usage) } != 0 {
            return None;
        }
        let secs = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
        Some(secs(usage.ru_utime) + secs(usage.ru_stime))
    }

    #[cfg(unix)]
    fn max_fds() -> Option<u64> {
        // SAFETY: as for rusage.
        let mut limit: libc::rlimit = unsafe { std::mem::zeroed() };
        // SAFETY: as for getrusage.
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &raw mut limit) } != 0 {
            return None;
        }
        (limit.rlim_cur != libc::RLIM_INFINITY).then_some(limit.rlim_cur)
    }

    /// Counted in the directory the system lists them in, less the one the listing holds.
    #[cfg(unix)]
    fn open_fds() -> Option<u64> {
        let dir = if cfg!(target_os = "linux") {
            "/proc/self/fd"
        } else {
            "/dev/fd"
        };
        let n = std::fs::read_dir(dir).ok()?.count();
        Some(n.saturating_sub(1) as u64)
    }

    #[cfg(target_os = "linux")]
    fn rss() -> Option<u64> {
        // The second field is the resident set, in pages.
        let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
        let pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
        // SAFETY: sysconf reads a constant of the system.
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        Some(pages * u64::try_from(page).ok()?)
    }

    #[cfg(target_os = "macos")]
    fn rss() -> Option<u64> {
        // SAFETY: as for rusage.
        let mut info: libc::proc_taskinfo = unsafe { std::mem::zeroed() };
        let size = libc::c_int::try_from(size_of::<libc::proc_taskinfo>()).ok()?;
        // SAFETY: the buffer is a whole proc_taskinfo, of the size passed, which the call only
        // writes; it answers how much it wrote.
        let written = unsafe {
            libc::proc_pidinfo(
                libc::getpid(),
                libc::PROC_PIDTASKINFO,
                0,
                (&raw mut info).cast(),
                size,
            )
        };
        (written == size).then_some(info.pti_resident_size)
    }

    #[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
    fn rss() -> Option<u64> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operations_are_sorted_so_the_search_finds_each() {
        // An unsorted list would file real operations under Other, silently.
        assert!(OPERATIONS.windows(2).all(|w| w[0] < w[1]));
        for (i, name) in OPERATIONS.iter().enumerate() {
            assert_eq!(OPERATIONS.binary_search(name), Ok(i));
        }
        assert_eq!(
            OPERATIONS
                .binary_search(&"PutBucketPolicy")
                .unwrap_or(OTHER),
            OTHER
        );
    }

    #[test]
    fn a_backend_error_counts_by_kind_but_a_dropped_operation_does_not() {
        // Every download from an object store ends with its reader dropped, which OpenDAL's
        // layer reports as Unexpected; counted, a healthy backend would show an error per GET.
        let metrics = Arc::new(Metrics::new());
        let backend = metrics.backend();
        let error = |kind| MetricLabels {
            operation: "read",
            error: Some(kind),
            ..Default::default()
        };
        backend.observe(
            error(ErrorKind::Unexpected),
            MetricValue::OperationErrorsTotal,
        );
        backend.observe(
            error(ErrorKind::PermissionDenied),
            MetricValue::OperationErrorsTotal,
        );
        let text = metrics.render();
        assert!(
            text.contains(
                "s3dal_backend_errors_total{operation=\"read\",kind=\"PermissionDenied\"} 1\n"
            ),
            "{text}"
        );
        assert!(!text.contains("Unexpected"), "{text}");
    }

    #[test]
    fn bounds_agree_in_every_unit() {
        for i in 0..BOUNDS.len() {
            assert_eq!(BOUNDS_US[i] as f64 / 1e6, BOUNDS[i]);
            assert_eq!(BOUND_LABELS[i].parse::<f64>().unwrap(), BOUNDS[i]);
        }
    }

    #[test]
    fn a_value_on_a_bound_counts_in_that_bucket() {
        // Prometheus's `le` is "less than or equal": a request of exactly 1 ms is in le="0.001".
        let h = Histogram::default();
        h.observe(Duration::from_millis(1));
        h.observe(Duration::from_secs(3600));
        let b = h.load();
        assert_eq!(b[1], 1);
        assert_eq!(b[BUCKETS - 1], 1);
    }

    #[test]
    fn quantiles_interpolate_within_a_bucket() {
        // The dashboard's p50 and p99 come from these, so they must land where the requests are.
        let mut b = [0; BUCKETS];
        assert_eq!(quantile(&b, 0.5), None);
        // 100 requests between 10 and 25 ms.
        b[5] = 100;
        let p50 = quantile(&b, 0.5).unwrap();
        assert!((p50 - 0.0175).abs() < 1e-9, "{p50}");
        // One slow request among them moves p99 only to the edge of the slow one's bucket.
        b[5] = 99;
        b[12] = 1;
        let p99 = quantile(&b, 0.99).unwrap();
        assert!((p99 - 0.025).abs() < 1e-9, "{p99}");
        // Past the last bound, the last bound.
        let mut slow = [0; BUCKETS];
        slow[BUCKETS - 1] = 5;
        assert_eq!(quantile(&slow, 0.99), Some(60.0));
    }

    #[test]
    fn the_token_must_match_whole_and_only_in_token_mode() {
        let t = Some("s3cret-token");
        let ok = Some(b"Bearer s3cret-token".as_slice());
        assert_eq!(authorize(MetricsMode::Token, t, ok), Scrape::Allowed);
        assert_eq!(
            authorize(MetricsMode::Token, t, Some(b"bearer  s3cret-token ")),
            Scrape::Allowed
        );
        for bad in [
            None,
            Some(b"Bearer s3cret".as_slice()),
            Some(b"Bearer s3cret-token2"),
            Some(b"Basic s3cret-token"),
            Some(b"Bearer"),
        ] {
            assert_eq!(authorize(MetricsMode::Token, t, bad), Scrape::Unauthorized);
        }
        // No token configured: closed to everyone, rather than open to anyone.
        assert_eq!(authorize(MetricsMode::Token, None, ok), Scrape::NoToken);
        assert_eq!(authorize(MetricsMode::Public, t, None), Scrape::Allowed);
        assert_eq!(authorize(MetricsMode::Off, t, ok), Scrape::Off);
    }

    #[test]
    fn the_scrape_lists_what_was_counted_in_prometheus_syntax() {
        let m = Arc::new(Metrics::new());
        {
            let mut r = m.request();
            r.operation = OPERATIONS.binary_search(&"GetObject").unwrap();
            r.status = 206;
            r.sent(10);
        }
        let conn = m.connection();
        conn.speaks(true);
        conn.speaks(true);
        let text = m.render();
        assert!(
            text.contains("s3dal_requests_total{operation=\"GetObject\",status_class=\"2xx\"} 1\n")
        );
        assert!(text.contains(
            "s3dal_request_duration_seconds_bucket{operation=\"GetObject\",le=\"+Inf\"} 1\n"
        ));
        assert!(text.contains("s3dal_request_duration_seconds_count{operation=\"GetObject\"} 1\n"));
        assert!(text.contains("s3dal_sent_bytes_total 10\n"));
        assert!(text.contains("s3dal_requests_in_flight 0\n"));
        // Counted once, whatever number of requests it carries.
        assert!(text.contains("s3dal_connections_open{protocol=\"http2\"} 1\n"));
        assert!(text.contains("s3dal_connections_total{protocol=\"http2\"} 1\n"));
        // Operations with nothing counted are left out, not listed as zeros.
        assert!(!text.contains("operation=\"PutObject\""));
        assert!(!text.contains("protocol=\"http1\""));
        drop(conn);
        assert!(
            m.render()
                .contains("s3dal_connections_open{protocol=\"http2\"} 0\n")
        );
        // Every sample line is a name, optional labels and a number.
        for l in text.lines().filter(|l| !l.starts_with('#')) {
            let value = l.rsplit_once(' ').unwrap().1;
            assert!(value.parse::<f64>().is_ok(), "{l}");
        }
    }

    #[test]
    fn stats_end_with_the_interval_in_progress() {
        // A request shows on the dashboard at once, not at the next tick.
        let m = Arc::new(Metrics::new());
        m.sample();
        drop(m.request());
        let stats = m.stats();
        let samples = stats["samples"].as_array().unwrap();
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[1]["requests"]["Unknown"], 1);
        assert_eq!(samples[1]["status"]["5xx"], 1);
        assert!(samples[1]["p50_ms"].is_number());
        assert!(samples[0]["requests"].as_object().unwrap().is_empty());
    }
}

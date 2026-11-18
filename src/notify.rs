//! Bucket event notifications to webhooks, features-2026-11.md "5. Event notifications": the
//! operator configures targets (`[[notify]]` in the config file); a bucket's notification
//! configuration (PutBucketNotificationConfiguration, see `s3::notification`) names them by ARN;
//! each object event (F3) a rule matches is POSTed to its target as an AWS S3 event message.
//!
//! Loss is allowed (decided): each target has a bounded queue in memory, and an event is dropped,
//! and counted, when the queue is full, when the target refuses it, or when it still fails after
//! `retry-for`. Nothing is written down, so a crash loses what was queued. Every target has a task
//! of its own on the server's runtime, so a slow or dead target holds up neither a request (the
//! request only puts the event into the F3 broadcast) nor another target (each queue is its own).
//!
//! The log forwarder (`logging::forward`) sends from a thread because tracing hands it lines
//! synchronously; events come from an async broadcast, and finding a bucket's rules reads the
//! backend, so here the queues are tokio channels and the senders tokio tasks. What is shared is
//! the HTTP client (reqwest, as the log forwarder's HTTP collectors use) and its answer to failure:
//! a connection error, a 429 or a 5xx is retried with a pause that doubles up to 30 seconds; any
//! other 4xx is not.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::{Duration, Instant};

use serde::Deserialize;
use tokio::sync::mpsc;

use crate::metrics::{DropReason, Metrics, NotifyStats};

/// Events held for a target that is slow or down, by default: a few MiB, as each message is a
/// kilobyte or two.
pub const DEFAULT_QUEUE: usize = 10_000;
/// How long an event may wait and be retried before it is dropped, by default: a target
/// restarted, or redeployed, within the hour loses nothing.
pub const DEFAULT_RETRY_FOR: Duration = Duration::from_secs(60 * 60);
/// Per attempt, connecting included, as the log forwarder's HTTP collectors get.
const TIMEOUT: Duration = Duration::from_secs(10);
const FIRST_PAUSE: Duration = Duration::from_secs(1);
const MAX_PAUSE: Duration = Duration::from_secs(30);

/// One webhook, as a `[[notify]]` table of the config file names it.
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(try_from = "TargetFile")]
pub struct Target {
    /// What its ARN names it by, `arn:s3dal:sqs:<region>:<id>:webhook`, and its metrics.
    pub id: String,
    /// `http://` or `https://`; every event is POSTed to it.
    pub url: String,
    /// Sent as `Authorization: Bearer <auth-token>`.
    pub auth_token: Option<String>,
    /// Signs every message: `X-S3dal-Signature: sha256=<hex>`, see [`signature`].
    pub hmac_secret: Option<String>,
    /// Events waiting for the target at most; more are dropped.
    pub queue_size: usize,
    /// How long after it was queued an event is still tried.
    pub retry_for: Duration,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct TargetFile {
    id: String,
    url: String,
    #[serde(default)]
    auth_token: Option<String>,
    #[serde(default)]
    hmac_secret: Option<String>,
    #[serde(default)]
    queue_size: Option<usize>,
    #[serde(default)]
    retry_for: Option<String>,
}

impl TryFrom<TargetFile> for Target {
    type Error = String;

    fn try_from(f: TargetFile) -> Result<Self, String> {
        let at = |msg: String| format!("[[notify]] {:?}: {msg}", f.id);
        let retry_for = match &f.retry_for {
            Some(s) => {
                crate::config::parse_duration(s).map_err(|e| at(format!("retry-for: {e}")))?
            }
            None => DEFAULT_RETRY_FOR,
        };
        let target = Self {
            id: f.id.clone(),
            url: f.url.trim().to_owned(),
            auth_token: f.auth_token,
            hmac_secret: f.hmac_secret,
            queue_size: f.queue_size.unwrap_or(DEFAULT_QUEUE),
            retry_for,
        };
        target.check().map_err(at)?;
        Ok(target)
    }
}

// Written by hand so that printing the settings never prints a secret.
impl fmt::Debug for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Target")
            .field("id", &self.id)
            .field("url", &shown_url(&self.url))
            .field(
                "auth_token",
                &self.auth_token.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "hmac_secret",
                &self.hmac_secret.as_ref().map(|_| "<redacted>"),
            )
            .field("queue_size", &self.queue_size)
            .field("retry_for", &self.retry_for)
            .finish()
    }
}

impl Target {
    /// A target with the defaults, for embedders and tests.
    pub fn new(id: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            url: url.into(),
            auth_token: None,
            hmac_secret: None,
            queue_size: DEFAULT_QUEUE,
            retry_for: DEFAULT_RETRY_FOR,
        }
    }

    /// The id goes into an ARN, between colons, and into metric labels, so it is kept to
    /// letters, digits, `-` and `_`. The URL is checked now rather than at the first event, when
    /// nobody would read why nothing arrives.
    fn check(&self) -> Result<(), String> {
        if self.id.is_empty()
            || !self
                .id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err("`id` must be letters, digits, `-` and `_`".into());
        }
        let uri: hyper::Uri = self
            .url
            .parse()
            .map_err(|e| format!("`url` {:?}: {e}", shown_url(&self.url)))?;
        if !matches!(uri.scheme_str(), Some("http" | "https")) || uri.host().is_none() {
            return Err(format!(
                "`url` {:?}: expected http:// or https:// and a host",
                shown_url(&self.url)
            ));
        }
        if self.queue_size == 0 {
            return Err("`queue-size` must be at least 1".into());
        }
        Ok(())
    }

    /// The ARN a bucket's notification configuration names this target by.
    pub fn arn(&self, region: &str) -> String {
        format!("arn:s3dal:sqs:{region}:{}:webhook", self.id)
    }
}

/// Targets as the config file lists them: each checked, and no id twice, which would leave
/// which of them an ARN means to the order of the file.
pub(crate) fn check_targets(targets: &[Target]) -> Result<(), String> {
    let mut seen = HashSet::new();
    for t in targets {
        t.check()
            .map_err(|e| format!("[[notify]] {:?}: {e}", t.id))?;
        if !seen.insert(t.id.as_str()) {
            return Err(format!(
                "[[notify]] id {:?} is configured more than once",
                t.id
            ));
        }
    }
    Ok(())
}

/// The region and target id of an ARN of the form `arn:s3dal:sqs:<region>:<id>:webhook`; `None`
/// for any other ARN.
pub(crate) fn target_of(arn: &str) -> Option<(&str, &str)> {
    let rest = arn
        .strip_prefix("arn:s3dal:sqs:")?
        .strip_suffix(":webhook")?;
    let (region, id) = rest.split_once(':')?;
    (!id.is_empty() && !id.contains(':')).then_some((region, id))
}

/// The URL without what may be secret, for the log, `check-config` and the console: no user,
/// password or query. Not a URL parser's job, so a URL it would refuse is shown the same way.
pub fn shown_url(url: &str) -> String {
    let Some(start) = url.find("://").map(|i| i + 3) else {
        return url.split(['?', '#']).next().unwrap_or_default().to_owned();
    };
    let end = url[start..]
        .find(['/', '?', '#'])
        .map_or(url.len(), |i| start + i);
    let host = url[start..end].rsplit('@').next().unwrap_or_default();
    let path = url[end..].split(['?', '#']).next().unwrap_or_default();
    format!("{}{host}{path}", &url[..start])
}

/// `X-S3dal-Signature`: `sha256=` and the lowercase hex HMAC-SHA256 of the request body, keyed
/// with the target's `hmac-secret` as UTF-8 bytes, as GitHub signs its webhooks, so receivers
/// can reuse their verification code.
#[cfg(feature = "notify")]
pub fn signature(secret: &str, body: &[u8]) -> String {
    use hmac::{Hmac, KeyInit, Mac};
    let mut mac =
        Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC takes any key size");
    mac.update(body);
    let digest = mac.finalize().into_bytes();
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    format!("sha256={hex}")
}

/// One event message for one target.
struct Message {
    body: Vec<u8>,
    queued: Instant,
}

/// A target's queue, for whoever matches events to it.
#[derive(Clone)]
pub(crate) struct Queue {
    sender: mpsc::Sender<Message>,
    stats: Arc<NotifyStats>,
}

impl Queue {
    /// Never waits: with the queue full the message is dropped and counted.
    pub(crate) fn send(&self, body: Vec<u8>) {
        // Counted before it is sent, so the depth never goes below zero when the worker takes
        // the message at once; taken back if the queue refuses it.
        self.stats
            .queued
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let message = Message {
            body,
            queued: Instant::now(),
        };
        if self.sender.try_send(message).is_err() {
            self.stats
                .queued
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            self.stats.dropped(DropReason::Overflow);
        }
    }
}

/// A running target: its settings and its queue.
struct Running {
    target: Target,
    queue: Queue,
}

/// The targets of a server, which a config reload replaces.
#[derive(Default)]
pub(crate) struct Notifier {
    running: RwLock<HashMap<String, Running>>,
}

impl Notifier {
    /// The queue of target `id`, `None` if no such target is configured (any more).
    pub(crate) fn queue(&self, id: &str) -> Option<Queue> {
        self.running
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(id)
            .map(|r| r.queue.clone())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.running
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
    }

    /// Runs `targets` from now on. A target whose settings did not change keeps its queue and
    /// its task; one that changed gets a new pair, and the old task sends what it already holds
    /// as the old settings said, then ends, as does that of a target removed. Spawns on the
    /// current Tokio runtime.
    pub(crate) fn configure(&self, targets: &[Target], metrics: &Arc<Metrics>) {
        let mut running = self.running.write().unwrap_or_else(PoisonError::into_inner);
        running.retain(|id, r| targets.iter().any(|t| t.id == *id && *t == r.target));
        for target in targets {
            if running.contains_key(&target.id) {
                continue;
            }
            let stats = metrics.notify_target(&target.id);
            let (sender, receiver) = mpsc::channel(target.queue_size);
            tokio::spawn(deliver_all(target.clone(), receiver, stats.clone()));
            let queue = Queue { sender, stats };
            running.insert(
                target.id.clone(),
                Running {
                    target: target.clone(),
                    queue,
                },
            );
        }
        let ids: Vec<&str> = targets.iter().map(|t| t.id.as_str()).collect();
        metrics.keep_notify_targets(&ids);
    }
}

/// Sends what the queue holds, in order, until every sender of it is gone.
async fn deliver_all(target: Target, mut queue: mpsc::Receiver<Message>, stats: Arc<NotifyStats>) {
    let client = Client::new();
    let shown = shown_url(&target.url);
    let mut failing = false;
    while let Some(message) = queue.recv().await {
        stats
            .queued
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        let dropped = deliver(&client, &target, &stats, &message, &mut failing, &shown).await;
        if let Some(reason) = dropped {
            stats.dropped(reason);
        }
    }
}

enum Failure {
    /// Worth sending again: the target was unreachable, busy or failing.
    // Only an HTTP attempt can fail this way, and a build without `notify` makes none.
    #[cfg_attr(not(feature = "notify"), allow(dead_code))]
    Retry(String),
    /// Not worth it: the target answered and refused, which it would do again.
    Refused(String),
}

/// One message, tried until it is accepted, refused, or its time is up. `None` once delivered,
/// otherwise why it was dropped. The log says when the target starts failing and when it answers
/// again, not at every attempt, which a dead target would make thousands of.
async fn deliver(
    client: &Client,
    target: &Target,
    stats: &NotifyStats,
    message: &Message,
    failing: &mut bool,
    shown: &str,
) -> Option<DropReason> {
    let deadline = message.queued + target.retry_for;
    // Waited its whole time behind other messages: a target that has been down for that long
    // would only make it wait again, and everything behind it with it.
    if !target.retry_for.is_zero() && Instant::now() >= deadline {
        return Some(DropReason::RetriesExhausted);
    }
    let mut pause = FIRST_PAUSE;
    loop {
        match client.post(target, &message.body).await {
            Ok(()) => {
                stats.attempted(None);
                if std::mem::take(failing) {
                    tracing::info!(
                        target_id = target.id,
                        url = shown,
                        "notification target answers again"
                    );
                }
                return None;
            }
            Err(Failure::Refused(e)) => {
                stats.attempted(Some(e.clone()));
                tracing::warn!(
                    target_id = target.id,
                    url = shown,
                    error = e,
                    "notification refused; not sent again"
                );
                return Some(DropReason::Refused);
            }
            Err(Failure::Retry(e)) => {
                stats.attempted(Some(e.clone()));
                if !std::mem::replace(failing, true) {
                    tracing::warn!(
                        target_id = target.id,
                        url = shown,
                        error = e,
                        "cannot send notifications; retrying"
                    );
                }
                if Instant::now() + pause > deadline {
                    return Some(DropReason::RetriesExhausted);
                }
                tokio::time::sleep(pause).await;
                pause = (pause * 2).min(MAX_PAUSE);
            }
        }
    }
}

/// The HTTP client of one target, whose connections it keeps open between events.
struct Client {
    #[cfg(feature = "notify")]
    http: Option<reqwest::Client>,
}

impl Client {
    #[cfg(feature = "notify")]
    fn new() -> Self {
        let http = reqwest::Client::builder().timeout(TIMEOUT).build();
        if let Err(e) = &http {
            tracing::error!(error = %e, "cannot build the notification HTTP client");
        }
        Self { http: http.ok() }
    }

    #[cfg(not(feature = "notify"))]
    fn new() -> Self {
        let _ = TIMEOUT;
        Self {}
    }

    #[cfg(feature = "notify")]
    async fn post(&self, target: &Target, body: &[u8]) -> Result<(), Failure> {
        let Some(http) = &self.http else {
            return Err(Failure::Refused("no HTTP client".into()));
        };
        let mut req = http
            .post(&target.url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body.to_vec());
        if let Some(token) = &target.auth_token {
            req = req.bearer_auth(token);
        }
        if let Some(secret) = &target.hmac_secret {
            req = req.header("x-s3dal-signature", signature(secret, body));
        }
        let res = req.send().await.map_err(|e| {
            // reqwest's message names the URL, query included, which may hold a token.
            Failure::Retry(e.without_url().to_string())
        })?;
        let status = res.status();
        if status.is_success() {
            return Ok(());
        }
        let text = res.text().await.unwrap_or_default();
        let msg = format!("{status}: {}", text.chars().take(300).collect::<String>());
        if status.as_u16() == 429 || status.is_server_error() {
            Err(Failure::Retry(msg))
        } else {
            Err(Failure::Refused(msg))
        }
    }

    /// The config refuses targets in such a build; an embedder that sets some anyway gets
    /// every message refused.
    #[cfg(not(feature = "notify"))]
    async fn post(&self, _: &Target, _: &[u8]) -> Result<(), Failure> {
        Err(Failure::Refused(
            "this build of s3dal has no event notifications (the `notify` feature)".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_an_arn_of_the_s3dal_webhook_form_names_a_target() {
        // An AWS queue or MinIO target of the same name is somewhere else entirely; sending
        // there events meant for it would hand them to the wrong receiver.
        assert_eq!(
            target_of("arn:s3dal:sqs:us-east-1:hooks:webhook"),
            Some(("us-east-1", "hooks"))
        );
        for other in [
            "arn:aws:sqs:us-east-1:123456789012:queue",
            "arn:minio:sqs:us-east-1:hooks:webhook",
            "arn:s3dal:sqs:us-east-1:hooks:kafka",
            "arn:s3dal:sqs:us-east-1::webhook",
            "arn:s3dal:sqs:us-east-1:a:b:webhook",
        ] {
            assert_eq!(target_of(other), None, "{other}");
        }
        assert_eq!(
            Target::new("hooks", "http://h/").arn("us-east-1"),
            "arn:s3dal:sqs:us-east-1:hooks:webhook"
        );
    }

    #[test]
    fn a_shown_url_keeps_no_credential() {
        // The console and check-config show where events go; a password in the URL or a token
        // in its query must not travel with it.
        assert_eq!(
            shown_url("https://user:pw@hooks.example.com:8443/in?token=abc#x"),
            "https://hooks.example.com:8443/in"
        );
        assert_eq!(shown_url("http://h"), "http://h");
    }

    #[test]
    fn targets_with_a_bad_id_url_or_queue_are_refused() {
        let ok = Target::new("a-b_1", "https://h/x");
        assert!(check_targets(std::slice::from_ref(&ok)).is_ok());
        for bad in [
            Target::new("a:b", "https://h/x"),
            Target::new("", "https://h/x"),
            Target::new("a", "ftp://h/x"),
            Target::new("a", "h/x"),
            Target {
                queue_size: 0,
                ..ok.clone()
            },
        ] {
            assert!(
                check_targets(std::slice::from_ref(&bad)).is_err(),
                "{bad:?}"
            );
        }
        assert!(check_targets(&[ok.clone(), ok]).is_err());
    }

    #[cfg(feature = "notify")]
    #[test]
    fn the_signature_is_the_hmac_sha256_of_the_body_in_hex() {
        // RFC 4231 test case 2, so a receiver's own HMAC library agrees with it.
        assert_eq!(
            signature("Jefe", b"what do ya want for nothing?"),
            "sha256=5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }
}

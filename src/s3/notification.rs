//! A bucket's notification configuration, features-2026-11.md "5. Event notifications":
//! Put/GetBucketNotificationConfiguration, stored with the bucket's state (F1), and the task that
//! matches each object event (F3) against it and hands an AWS S3 event message to every target a
//! rule names (see `crate::notify`).
//!
//! Queue, topic and Lambda configurations are all taken, each naming an operator-configured
//! target by ARN, `arn:s3dal:sqs:<region>:<id>:webhook`, MinIO's form; the list a rule was put
//! in is kept so it reads back as it was put. An ARN that names no configured target is refused
//! when the configuration is put, as AWS refuses a destination it cannot validate.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use s3s::dto::*;
use s3s::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use time::OffsetDateTime;

use super::{S3Dal, acl, bucket_state, naming};
use crate::events::{EventKind, ObjectEvent, Subscriber};
use crate::notify;

/// The event names s3dal sends, as a configuration names them.
const EMITTED: [&str; 9] = [
    "s3:ObjectCreated:Put",
    "s3:ObjectCreated:Post",
    "s3:ObjectCreated:Copy",
    "s3:ObjectCreated:CompleteMultipartUpload",
    "s3:ObjectRemoved:Delete",
    "s3:ObjectRemoved:DeleteMarkerCreated",
    "s3:LifecycleExpiration:Delete",
    "s3:LifecycleExpiration:DeleteMarkerCreated",
    "s3:LifecycleTransition",
];

/// The wildcards over [`EMITTED`]: a name ending in `*` matches every event it starts.
const WILDCARDS: [&str; 3] = [
    "s3:ObjectCreated:*",
    "s3:ObjectRemoved:*",
    "s3:LifecycleExpiration:*",
];

/// Event names AWS sends and s3dal never will: there is no restore, replication, Intelligent-Tiering
/// or tagging here. A rule for them is refused with `NotImplemented`, not stored to never fire.
const NOT_EMITTED: [&str; 15] = [
    "s3:ReducedRedundancyLostObject",
    "s3:ObjectRestore:*",
    "s3:ObjectRestore:Post",
    "s3:ObjectRestore:Completed",
    "s3:ObjectRestore:Delete",
    "s3:Replication:*",
    "s3:Replication:OperationFailedReplication",
    "s3:Replication:OperationNotTracked",
    "s3:Replication:OperationMissedThreshold",
    "s3:Replication:OperationReplicatedAfterThreshold",
    "s3:IntelligentTiering",
    "s3:ObjectTagging:*",
    "s3:ObjectTagging:Put",
    "s3:ObjectTagging:Delete",
    "s3:ObjectAcl:Put",
];

/// A bucket's rules as stored in its state file. Every level keeps the fields a newer s3dal
/// sharing the backend may write, as the lifecycle rules do.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct Notification {
    pub rules: Vec<Rule>,
    #[serde(flatten)]
    pub unknown: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct Rule {
    pub id: String,
    /// The list it was put in.
    pub list: List,
    pub arn: String,
    /// As put: specific names and wildcards.
    pub events: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suffix: Option<String>,
    #[serde(flatten)]
    pub unknown: Map<String, Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum List {
    Queue,
    Topic,
    Lambda,
}

/// The name a configuration gives the event of `kind`.
fn event_name(kind: EventKind) -> &'static str {
    match kind {
        EventKind::Put => EMITTED[0],
        EventKind::Post => EMITTED[1],
        EventKind::Copy => EMITTED[2],
        EventKind::Complete => EMITTED[3],
        EventKind::Delete => EMITTED[4],
        EventKind::DeleteMarker => EMITTED[5],
        EventKind::LifecycleDelete => EMITTED[6],
        EventKind::LifecycleDeleteMarker => EMITTED[7],
        EventKind::LifecycleTransition => EMITTED[8],
    }
}

impl Rule {
    /// Whether this process sends for the rule: made only of parts it knows. A part a newer
    /// s3dal added could be a condition, and without it the rule would send more than asked.
    fn active(&self) -> bool {
        self.unknown.is_empty()
    }

    fn matches(&self, kind: EventKind, key: &str) -> bool {
        let name = event_name(kind);
        let named = self.events.iter().any(|e| match e.strip_suffix('*') {
            Some(start) => name.starts_with(start),
            None => e == name,
        });
        named
            && self.prefix.as_deref().is_none_or(|p| key.starts_with(p))
            && self.suffix.as_deref().is_none_or(|s| key.ends_with(s))
    }
}

// ----- The S3 API -----

fn invalid(message: impl Into<String>) -> S3Error {
    S3Error::with_message(S3ErrorCode::InvalidArgument, message.into())
}

/// One configuration of any of the three lists.
struct Put {
    list: List,
    id: Option<String>,
    arn: String,
    events: Vec<Event>,
    filter: Option<NotificationConfigurationFilter>,
}

/// The prefix and suffix of a filter. AWS takes the rule names in any case, at most one of
/// each; an empty value filters nothing.
fn key_filter(
    filter: Option<NotificationConfigurationFilter>,
) -> S3Result<(Option<String>, Option<String>)> {
    let (mut prefix, mut suffix) = (None, None);
    let rules = filter
        .and_then(|f| f.key)
        .and_then(|k| k.filter_rules)
        .unwrap_or_default();
    for rule in rules {
        let name = rule
            .name
            .as_ref()
            .map(|n| n.as_str().to_ascii_lowercase())
            .unwrap_or_default();
        let slot = match name.as_str() {
            FilterRuleName::PREFIX => &mut prefix,
            FilterRuleName::SUFFIX => &mut suffix,
            _ => {
                return Err(invalid("filter rule name must be either prefix or suffix"));
            }
        };
        if slot.is_some() {
            return Err(invalid(format!(
                "Cannot specify more than one {name} rule in a filter."
            )));
        }
        *slot = Some(rule.value.unwrap_or_default());
    }
    let some = |v: Option<String>| v.filter(|v| !v.is_empty());
    Ok((some(prefix), some(suffix)))
}

/// The rules of a PutBucketNotificationConfiguration, checked as AWS checks them: an unknown
/// event name or a bad filter answers 400, an ARN that names no target of this server 400 as a
/// destination AWS cannot validate, and only a valid configuration that asks for events s3dal
/// never sends `NotImplemented`. `None` for a configuration without rules, which removes it.
fn parse(
    conf: NotificationConfiguration,
    targets: &[notify::Target],
    region: &str,
) -> S3Result<Option<Notification>> {
    if conf.event_bridge_configuration.is_some() {
        return Err(S3Error::with_message(
            S3ErrorCode::NotImplemented,
            "EventBridge notifications are not supported",
        ));
    }
    let queues = conf
        .queue_configurations
        .into_iter()
        .flatten()
        .map(|c| Put {
            list: List::Queue,
            id: c.id,
            arn: c.queue_arn,
            events: c.events,
            filter: c.filter,
        });
    let topics = conf
        .topic_configurations
        .into_iter()
        .flatten()
        .map(|c| Put {
            list: List::Topic,
            id: c.id,
            arn: c.topic_arn,
            events: c.events,
            filter: c.filter,
        });
    let lambdas = conf
        .lambda_function_configurations
        .into_iter()
        .flatten()
        .map(|c| Put {
            list: List::Lambda,
            id: c.id,
            arn: c.lambda_function_arn,
            events: c.events,
            filter: c.filter,
        });
    let mut rules = Vec::new();
    let mut ids = BTreeSet::new();
    let mut unknown_arns = Vec::new();
    let mut not_emitted = None;
    for put in queues.chain(topics).chain(lambdas) {
        if put.events.is_empty() {
            return Err(invalid(
                "A notification configuration needs at least one event",
            ));
        }
        let mut events = Vec::with_capacity(put.events.len());
        for event in put.events {
            let name: String = event.as_ref().to_owned();
            if NOT_EMITTED.contains(&name.as_str()) {
                not_emitted.get_or_insert(name.clone());
            } else if !EMITTED.contains(&name.as_str()) && !WILDCARDS.contains(&name.as_str()) {
                return Err(invalid(format!(
                    "The event is not supported for notifications: {name}"
                )));
            }
            events.push(name);
        }
        let (prefix, suffix) = key_filter(put.filter)?;
        let known = notify::target_of(&put.arn)
            .is_some_and(|(r, id)| r == region && targets.iter().any(|t| t.id == id));
        if !known {
            unknown_arns.push(put.arn.clone());
        }
        // AWS names a configuration put without an ID; any unique name will do.
        let id = put
            .id
            .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());
        if !ids.insert(id.clone()) {
            return Err(invalid(format!(
                "Configuration IDs must be unique; {id:?} is used twice"
            )));
        }
        rules.push(Rule {
            id,
            list: put.list,
            arn: put.arn,
            events,
            prefix,
            suffix,
            unknown: Map::new(),
        });
    }
    if !unknown_arns.is_empty() {
        return Err(invalid(format!(
            "Unable to validate the following destination configurations: {} (no such \
             notification target; this server's are arn:s3dal:sqs:{region}:<id>:webhook for each \
             [[notify]] id)",
            unknown_arns.join(", ")
        )));
    }
    if let Some(name) = not_emitted {
        return Err(S3Error::with_message(
            S3ErrorCode::NotImplemented,
            format!("s3dal never sends {name} events"),
        ));
    }
    Ok((!rules.is_empty()).then(|| Notification {
        rules,
        unknown: Map::new(),
    }))
}

fn filter_dto(rule: &Rule) -> Option<NotificationConfigurationFilter> {
    let rules: Vec<FilterRule> = [
        (FilterRuleName::PREFIX, &rule.prefix),
        (FilterRuleName::SUFFIX, &rule.suffix),
    ]
    .into_iter()
    .filter_map(|(name, value)| {
        value.as_ref().map(|v| FilterRule {
            name: Some(FilterRuleName::from_static(name)),
            value: Some(v.clone()),
        })
    })
    .collect();
    (!rules.is_empty()).then_some(NotificationConfigurationFilter {
        key: Some(S3KeyFilter {
            filter_rules: Some(rules),
        }),
    })
}

pub(super) async fn put_bucket_notification_configuration(
    dal: &S3Dal,
    req: S3Request<PutBucketNotificationConfigurationInput>,
) -> S3Result<S3Response<PutBucketNotificationConfigurationOutput>> {
    let input = req.input;
    naming::require_bucket(dal, &input.bucket).await?;
    let settings = dal.settings();
    let notification = parse(
        input.notification_configuration,
        &settings.notify,
        &settings.region,
    )?;
    // An empty configuration is how S3 removes one; without one there is nothing to write.
    if notification.is_some()
        || bucket_state::get(dal, &input.bucket)
            .await?
            .notification
            .is_some()
    {
        bucket_state::update(dal, &input.bucket, |s| {
            s.notification = notification.clone();
        })
        .await?;
    }
    Ok(S3Response::new(
        PutBucketNotificationConfigurationOutput::default(),
    ))
}

/// Answers with an empty configuration for a bucket without one, as AWS does.
pub(super) async fn get_bucket_notification_configuration(
    dal: &S3Dal,
    req: S3Request<GetBucketNotificationConfigurationInput>,
) -> S3Result<S3Response<GetBucketNotificationConfigurationOutput>> {
    let bucket = &req.input.bucket;
    naming::require_bucket(dal, bucket).await?;
    let state = bucket_state::get(dal, bucket).await?;
    let mut out = GetBucketNotificationConfigurationOutput::default();
    for rule in state.notification.iter().flat_map(|n| &n.rules) {
        let events: Vec<Event> = rule.events.iter().cloned().map(Event::from).collect();
        let (id, filter) = (Some(rule.id.clone()), filter_dto(rule));
        match rule.list {
            List::Queue => {
                out.queue_configurations
                    .get_or_insert_default()
                    .push(QueueConfiguration {
                        events,
                        filter,
                        id,
                        queue_arn: rule.arn.clone(),
                    })
            }
            List::Topic => {
                out.topic_configurations
                    .get_or_insert_default()
                    .push(TopicConfiguration {
                        events,
                        filter,
                        id,
                        topic_arn: rule.arn.clone(),
                    })
            }
            List::Lambda => out
                .lambda_function_configurations
                .get_or_insert_default()
                .push(LambdaFunctionConfiguration {
                    events,
                    filter,
                    id,
                    lambda_function_arn: rule.arn.clone(),
                }),
        }
    }
    Ok(S3Response::new(out))
}

// ----- Sending -----

/// Hands each object event to the targets of the rules it matches, until the server is gone.
/// Only queues: the sending is each target's own task, so a slow target never holds this up.
pub(super) async fn dispatch(dal: S3Dal, mut events: Subscriber) {
    while let Some(event) = events.recv().await {
        let state = match bucket_state::get(&dal, &event.bucket).await {
            Ok(state) => state,
            Err(err) => {
                tracing::warn!(bucket = event.bucket, key = event.key, error = %err, "cannot read the bucket's notification rules; event not sent");
                continue;
            }
        };
        let Some(notification) = &state.notification else {
            continue;
        };
        let rules: Vec<&Rule> = notification
            .rules
            .iter()
            .filter(|r| r.active() && r.matches(event.kind, &event.key))
            .collect();
        if rules.is_empty() {
            continue;
        }
        let region = dal.settings().region.clone();
        let sequencer = sequencer(event.at);
        for rule in rules {
            let Some(queue) =
                notify::target_of(&rule.arn).and_then(|(_, id)| dal.notifier.queue(id))
            else {
                tracing::debug!(arn = rule.arn, "notification target no longer configured");
                continue;
            };
            let message = json!({ "Records": [record(&event, rule, &region, &sequencer)] });
            queue.send(serde_json::to_vec(&message).expect("JSON values serialize"));
        }
    }
}

/// Orders the events of this process, as AWS's `sequencer` orders those of a key: later events
/// get larger values, compared as hex strings of equal length. From the event's time in
/// nanoseconds, moved on by one when two events share it.
fn sequencer(at: SystemTime) -> String {
    static LAST: AtomicU64 = AtomicU64::new(0);
    let nanos = at
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX));
    let previous = LAST.update(Ordering::Relaxed, Ordering::Relaxed, |last| {
        nanos.max(last.saturating_add(1))
    });
    format!("{:016X}", nanos.max(previous.saturating_add(1)))
}

/// `1970-01-01T00:00:00.000Z`, the form AWS gives `eventTime`.
fn event_time(at: SystemTime) -> String {
    let t = OffsetDateTime::from(at);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        t.year(),
        u8::from(t.month()),
        t.day(),
        t.hour(),
        t.minute(),
        t.second(),
        t.millisecond()
    )
}

/// The key as AWS puts it in an event: URL-encoded, a space as `+`, so a consumer decodes it
/// with `unquote_plus` or `decodeURIComponent(k.replace(/\+/g, " "))`. Slashes stay.
fn encode_key(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for b in key.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(char::from(b));
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// One record of the AWS S3 event message, field by field as AWS documents it
/// ("Event message structure"). `eventVersion` is 2.1, and 2.3 for lifecycle events, as AWS
/// sends them. Where s3dal has no such thing, a field says so rather than inventing an AWS
/// value: the principal is the access key (no IAM), a lifecycle event's principal and source
/// address are `s3dal`, and there is no `x-amz-id-2`.
fn record(event: &ObjectEvent, rule: &Rule, region: &str, sequencer: &str) -> Value {
    let lifecycle = matches!(
        event.kind,
        EventKind::LifecycleDelete
            | EventKind::LifecycleDeleteMarker
            | EventKind::LifecycleTransition
    );
    let mut object = Map::new();
    object.insert("key".into(), json!(encode_key(&event.key)));
    if let Some(size) = event.size {
        object.insert("size".into(), json!(size));
    }
    if let Some(etag) = &event.etag {
        object.insert("eTag".into(), json!(etag));
    }
    if let Some(version) = &event.version {
        object.insert("versionId".into(), json!(version));
    }
    object.insert("sequencer".into(), json!(sequencer));
    let mut response = Map::new();
    if let Some(id) = &event.request_id {
        response.insert("x-amz-request-id".into(), json!(id));
    }
    let name = event_name(event.kind);
    json!({
        "eventVersion": if lifecycle { "2.3" } else { "2.1" },
        "eventSource": "aws:s3",
        "awsRegion": region,
        "eventTime": event_time(event.at),
        "eventName": name.strip_prefix("s3:").unwrap_or(name),
        "userIdentity": { "principalId": event.access_key.as_deref().unwrap_or("s3dal") },
        "requestParameters": {
            "sourceIPAddress": event.source_ip.map_or_else(|| "s3dal".to_owned(), |ip| ip.to_string()),
        },
        "responseElements": response,
        "s3": {
            "s3SchemaVersion": "1.0",
            "configurationId": rule.id,
            "bucket": {
                "name": event.bucket,
                "ownerIdentity": { "principalId": acl::OWNER_ID },
                "arn": format!("arn:aws:s3:::{}", event.bucket),
            },
            "object": object,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(events: &[&str], prefix: Option<&str>, suffix: Option<&str>) -> Rule {
        Rule {
            id: "r".into(),
            list: List::Queue,
            arn: "arn:s3dal:sqs:us-east-1:t:webhook".into(),
            events: events.iter().map(|e| (*e).to_owned()).collect(),
            prefix: prefix.map(str::to_owned),
            suffix: suffix.map(str::to_owned),
            unknown: Map::new(),
        }
    }

    #[test]
    fn a_wildcard_covers_its_family_only() {
        // `s3:ObjectRemoved:*` is how most configurations ask for deletes; it must not also send
        // lifecycle expirations, which AWS keeps apart, nor creations.
        let removed = rule(&["s3:ObjectRemoved:*"], None, None);
        assert!(removed.matches(EventKind::Delete, "k"));
        assert!(removed.matches(EventKind::DeleteMarker, "k"));
        assert!(!removed.matches(EventKind::LifecycleDelete, "k"));
        assert!(!removed.matches(EventKind::Put, "k"));
        let put = rule(&["s3:ObjectCreated:Put"], None, None);
        assert!(put.matches(EventKind::Put, "k"));
        assert!(!put.matches(EventKind::Copy, "k"));
        let expiry = rule(&["s3:LifecycleExpiration:*"], None, None);
        assert!(expiry.matches(EventKind::LifecycleDeleteMarker, "k"));
        assert!(!expiry.matches(EventKind::DeleteMarker, "k"));
    }

    #[test]
    fn prefix_and_suffix_must_both_hold() {
        let r = rule(&["s3:ObjectCreated:*"], Some("images/"), Some(".jpg"));
        assert!(r.matches(EventKind::Put, "images/a.jpg"));
        assert!(!r.matches(EventKind::Put, "images/a.png"));
        assert!(!r.matches(EventKind::Put, "docs/a.jpg"));
    }

    #[test]
    fn a_rule_with_parts_this_version_does_not_know_sends_nothing() {
        let mut r = rule(&["s3:ObjectCreated:*"], None, None);
        r.unknown.insert("tags".into(), json!(["x"]));
        assert!(!r.active());
    }

    #[test]
    fn keys_are_encoded_as_aws_encodes_them() {
        // Consumers decode with unquote_plus: a literal plus must not come back as a space.
        assert_eq!(encode_key("a b+c/d é.txt"), "a+b%2Bc/d+%C3%A9.txt");
    }

    #[test]
    fn sequencers_grow_even_for_events_at_the_same_time() {
        let at = SystemTime::now();
        let (a, b) = (sequencer(at), sequencer(at));
        assert_eq!(a.len(), b.len());
        assert!(b > a, "{a} {b}");
    }

    #[test]
    fn event_times_have_milliseconds_and_a_z() {
        let at = UNIX_EPOCH + std::time::Duration::from_millis(1_700_000_000_123);
        assert_eq!(event_time(at), "2023-11-14T22:13:20.123Z");
    }
}

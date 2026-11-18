//! Bucket lifecycle, features-2026-11.md "1. Lifecycle": Put/Get/DeleteBucketLifecycleConfiguration,
//! stored with the bucket's state (F1), applied by the background job `lifecycle` (F2), and
//! announced through the object events (F3).
//!
//! A subset of AWS's rules: `Expiration` (`Days`, `Date`, `ExpiredObjectDeleteMarker`),
//! `NoncurrentVersionExpiration` (`NoncurrentDays`, `NewerNoncurrentVersions`),
//! `AbortIncompleteMultipartUpload`, and with cold tiers configured `Transition` and
//! `NoncurrentVersionTransition` into a tier's class (cold-tier.md "Lifecycle transitions"),
//! filtered by prefix and object size. Anything else (tag filters, transitions on a server without
//! tiers) is refused when the configuration is put, never stored and ignored: a rule that would
//! run without its condition would remove more than it was written for.
//!
//! Times follow AWS: a rule of N days applies from the first midnight UTC after the object's
//! creation (or the version's becoming noncurrent, or the upload's start) plus N days. Each bucket
//! is applied once a day, so an object goes during the day its rule names, not at its midnight.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::TryStreamExt;
use opendal::{ErrorKind, Metadata};
use s3s::dto::*;
use s3s::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::store::{self, DeleteCondition};
use super::tier::{self, Moved, Removals};
use super::version::{self, Versioning};
use super::{S3Dal, bucket_state, multipart, naming, usage};
use crate::error::{Ctx, map};
use crate::events::{EventKind, ObjectEvent, Origin, Usage};
use crate::meta;

/// AWS's limits: rules per configuration, and characters per rule ID.
const MAX_RULES: usize = 1000;
const MAX_ID_LEN: usize = 255;
/// AWS keeps at most this many newer noncurrent versions.
const MAX_NEWER_VERSIONS: i32 = 100;

const DAY: u64 = 24 * 60 * 60;

/// A bucket's rules as stored in its state file. Every level keeps the fields a newer s3dal
/// sharing the backend may write, as the state file does (see `bucket_state`), so changing
/// another setting here never drops them.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct Lifecycle {
    pub rules: Vec<Rule>,
    #[serde(flatten)]
    pub unknown: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct Rule {
    pub id: String,
    pub enabled: bool,
    /// The rule's own `Prefix`, the older form of a filter. Kept apart from `filter` so that the
    /// configuration reads back in the form it was put, which clients such as Terraform compare.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Filter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiration: Option<Expiration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub noncurrent_version_expiration: Option<NoncurrentExpiration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub abort_incomplete_multipart_upload: Option<AbortUpload>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transitions: Vec<Transition>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub noncurrent_version_transitions: Vec<NoncurrentTransition>,
    #[serde(flatten)]
    pub unknown: Map<String, Value>,
}

/// A `Transition`: the current version moves into the tier of `storage_class` after `days`
/// (counted as `Expiration` counts them) or from `date`, one of the two.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct Transition {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub days: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub date: Option<Midnight>,
    pub storage_class: String,
    #[serde(flatten)]
    pub unknown: Map<String, Value>,
}

/// A `NoncurrentVersionTransition`, counted as `NoncurrentVersionExpiration` is.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct NoncurrentTransition {
    pub noncurrent_days: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub newer_noncurrent_versions: Option<u32>,
    pub storage_class: String,
    #[serde(flatten)]
    pub unknown: Map<String, Value>,
}

/// A `Filter`. Its conditions all hold for a key it matches, whether they were put side by
/// side or under `And`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct Filter {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_size_greater_than: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_size_less_than: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub and: Option<Box<Filter>>,
    #[serde(flatten)]
    pub unknown: Map<String, Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct Expiration {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub days: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub date: Option<Midnight>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expired_object_delete_marker: Option<bool>,
    #[serde(flatten)]
    pub unknown: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct NoncurrentExpiration {
    pub noncurrent_days: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub newer_noncurrent_versions: Option<u32>,
    #[serde(flatten)]
    pub unknown: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct AbortUpload {
    pub days_after_initiation: u32,
    #[serde(flatten)]
    pub unknown: Map<String, Value>,
}

/// A midnight UTC, the only time AWS takes as an `Expiration` `Date`. Stored in RFC 3339, so a
/// person reading the state file can tell it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Midnight(OffsetDateTime);

impl Midnight {
    fn of(t: Timestamp) -> Option<Self> {
        let t = OffsetDateTime::from(t).to_offset(time::UtcOffset::UTC);
        (t.time() == time::Time::MIDNIGHT).then_some(Self(t))
    }

    fn time(self) -> SystemTime {
        self.0.into()
    }
}

impl Serialize for Midnight {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let text = self.0.format(&Rfc3339).map_err(serde::ser::Error::custom)?;
        s.serialize_str(&text)
    }
}

impl<'de> Deserialize<'de> for Midnight {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let text = String::deserialize(d)?;
        OffsetDateTime::parse(&text, &Rfc3339)
            .map(|t| Self(t.to_offset(time::UtcOffset::UTC)))
            .map_err(serde::de::Error::custom)
    }
}

/// AWS's rounding: `days` days after `t`, then on to the next midnight UTC. A time that falls on
/// a midnight goes on to the following one, as MinIO counts, so a rule never applies earlier
/// than a full `days` days.
pub(crate) fn due(t: SystemTime, days: u32) -> SystemTime {
    let secs = t.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
    let at = secs.saturating_add(u64::from(days) * DAY);
    UNIX_EPOCH + Duration::from_secs((at / DAY + 1) * DAY)
}

impl Filter {
    /// The conditions of this filter and of its `And`.
    fn levels(&self) -> impl Iterator<Item = &Filter> {
        std::iter::once(self).chain(self.and.as_deref())
    }

    fn understood(&self) -> bool {
        self.levels().all(|f| f.unknown.is_empty())
    }

    fn has_size(&self) -> bool {
        self.levels()
            .any(|f| f.object_size_greater_than.is_some() || f.object_size_less_than.is_some())
    }
}

impl Rule {
    /// Whether this process applies the rule: enabled, and made only of parts it knows. A part a
    /// newer s3dal added could be a condition, and a rule run without it would remove too much.
    fn active(&self) -> bool {
        self.enabled
            && self.unknown.is_empty()
            && self.filter.as_ref().is_none_or(Filter::understood)
            && self
                .expiration
                .as_ref()
                .is_none_or(|e| e.unknown.is_empty())
            && self
                .noncurrent_version_expiration
                .as_ref()
                .is_none_or(|n| n.unknown.is_empty())
            && self
                .abort_incomplete_multipart_upload
                .as_ref()
                .is_none_or(|a| a.unknown.is_empty())
            && self.transitions.iter().all(|t| t.unknown.is_empty())
            && self
                .noncurrent_version_transitions
                .iter()
                .all(|t| t.unknown.is_empty())
    }

    fn prefixes(&self) -> impl Iterator<Item = &str> {
        let filters = self.filter.iter().flat_map(Filter::levels);
        self.prefix
            .as_deref()
            .into_iter()
            .chain(filters.filter_map(|f| f.prefix.as_deref()))
    }

    /// Whether a key may match, whatever its size.
    fn covers(&self, key: &str) -> bool {
        self.prefixes().all(|p| key.starts_with(p))
    }

    /// Whether the rule applies to an object of `size` bytes at `key`; a delete marker has size 0.
    /// Sizes compare strictly, as AWS's `ObjectSizeGreaterThan` and `ObjectSizeLessThan` do.
    fn matches(&self, key: &str, size: u64) -> bool {
        self.covers(key)
            && self.filter.iter().flat_map(Filter::levels).all(|f| {
                f.object_size_greater_than.is_none_or(|n| size > n)
                    && f.object_size_less_than.is_none_or(|n| size < n)
            })
    }

    /// When a current version created at `created` expires by this rule; `None` for a rule that
    /// does not expire current versions.
    fn expiry(&self, created: SystemTime) -> Option<SystemTime> {
        let e = self.expiration.as_ref()?;
        match (e.days, e.date) {
            (Some(days), _) => Some(due(created, days)),
            (None, Some(date)) => Some(date.time()),
            (None, None) => None,
        }
    }

    fn expires_current(&self) -> bool {
        self.expiration
            .as_ref()
            .is_some_and(|e| e.days.is_some() || e.date.is_some())
    }

    /// Whether the rule removes archived versions or delete markers.
    fn expires_versions(&self) -> bool {
        self.noncurrent_version_expiration.is_some()
            || self.expires_current()
            || self.removes_lone_markers()
    }

    fn removes_lone_markers(&self) -> bool {
        self.expiration
            .as_ref()
            .is_some_and(|e| e.expired_object_delete_marker == Some(true))
    }

    fn moves_current(&self) -> bool {
        !self.transitions.is_empty()
    }

    fn moves_versions(&self) -> bool {
        !self.noncurrent_version_transitions.is_empty()
    }
}

impl Transition {
    /// When a current version created at `created` moves by this transition.
    fn at(&self, created: SystemTime) -> Option<SystemTime> {
        match (self.days, self.date) {
            (Some(days), _) => Some(due(created, days)),
            (None, Some(date)) => Some(date.time()),
            (None, None) => None,
        }
    }
}

impl Lifecycle {
    fn active(&self) -> impl Iterator<Item = &Rule> {
        self.rules.iter().filter(|r| r.active())
    }
}

// ----- The S3 API -----

fn malformed() -> S3Error {
    s3_error!(
        MalformedXML,
        "The XML you provided was not well-formed or did not validate against our published schema"
    )
}

fn invalid(message: &str) -> S3Error {
    S3Error::with_message(S3ErrorCode::InvalidArgument, message.to_owned())
}

fn positive(n: i32, what: &str) -> S3Result<u32> {
    u32::try_from(n)
        .ok()
        .filter(|n| *n > 0)
        .ok_or_else(|| invalid(&format!("'{what}' must be a positive integer")))
}

fn midnight(date: &Timestamp) -> S3Result<Midnight> {
    Midnight::of(date.clone()).ok_or_else(|| invalid("'Date' must be at midnight GMT"))
}

fn size(n: i64) -> S3Result<u64> {
    u64::try_from(n).map_err(|_| invalid("object size filters must not be negative"))
}

/// One level of a filter, or its `And`. Tags are refused once the rest of the rule is known to
/// be valid, so that a malformed rule is answered as such wherever it uses tags.
fn filter_level(
    prefix: Option<String>,
    greater: Option<i64>,
    less: Option<i64>,
    tagged: bool,
    unsupported: &mut Vec<&'static str>,
) -> S3Result<Filter> {
    let level = Filter {
        prefix,
        object_size_greater_than: greater.map(size).transpose()?,
        object_size_less_than: less.map(size).transpose()?,
        and: None,
        unknown: Map::new(),
    };
    if let (Some(gt), Some(lt)) = (level.object_size_greater_than, level.object_size_less_than)
        && gt >= lt
    {
        return Err(invalid(
            "'ObjectSizeGreaterThan' must be less than 'ObjectSizeLessThan'",
        ));
    }
    if tagged {
        unsupported.push("tag filters");
    }
    Ok(level)
}

/// The rules of a PutBucketLifecycleConfiguration, checked as AWS checks them, s3-tests' lifecycle
/// tests being the reference: invalid values answer 400, and only a valid configuration that uses
/// a part s3dal does not apply answers `NotImplemented`. `classes` are the configured tiers'
/// storage classes, the only ones a transition may name.
fn parse(conf: BucketLifecycleConfiguration, classes: &[&str]) -> S3Result<Lifecycle> {
    if conf.rules.is_empty() {
        return Err(malformed());
    }
    if conf.rules.len() > MAX_RULES {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            format!("a lifecycle configuration has at most {MAX_RULES} rules"),
        ));
    }
    let mut rules = Vec::with_capacity(conf.rules.len());
    let mut ids = BTreeSet::new();
    for rule in conf.rules {
        let rule = parse_rule(rule, classes)?;
        if !ids.insert(rule.id.clone()) {
            return Err(invalid(
                "Rule ID must be unique. Found same ID for more than one rule",
            ));
        }
        rules.push(rule);
    }
    Ok(Lifecycle {
        rules,
        unknown: Map::new(),
    })
}

fn parse_rule(rule: LifecycleRule, classes: &[&str]) -> S3Result<Rule> {
    let enabled = match rule.status.as_str() {
        ExpirationStatus::ENABLED => true,
        ExpirationStatus::DISABLED => false,
        _ => return Err(malformed()),
    };
    let id = match rule.id {
        Some(id) if id.chars().count() > MAX_ID_LEN => {
            return Err(invalid("ID length should not exceed allowed limit of 255"));
        }
        Some(id) => id,
        // AWS names a rule put without an ID; any unique name will do.
        None => uuid::Uuid::new_v4().simple().to_string(),
    };
    if rule.prefix.is_some() && rule.filter.is_some() {
        return Err(malformed());
    }
    let mut unsupported = Vec::new();
    let filter = match rule.filter {
        None => None,
        Some(f) => {
            let mut top = filter_level(
                f.prefix,
                f.object_size_greater_than,
                f.object_size_less_than,
                f.tag.is_some(),
                &mut unsupported,
            )?;
            if let Some(and) = f.and {
                let tagged = and.tags.as_ref().is_some_and(|t| !t.is_empty());
                top.and = Some(Box::new(filter_level(
                    and.prefix,
                    and.object_size_greater_than,
                    and.object_size_less_than,
                    tagged,
                    &mut unsupported,
                )?));
            }
            Some(top)
        }
    };
    let expiration = match rule.expiration {
        None => None,
        Some(e) => {
            let days = e.days.map(|d| positive(d, "Days")).transpose()?;
            let date = e.date.as_ref().map(midnight).transpose()?;
            let marker = e.expired_object_delete_marker == Some(true);
            let given =
                usize::from(days.is_some()) + usize::from(date.is_some()) + usize::from(marker);
            if given > 1 || (given == 0 && e.expired_object_delete_marker.is_none()) {
                return Err(malformed());
            }
            Some(Expiration {
                days,
                date,
                expired_object_delete_marker: e.expired_object_delete_marker,
                unknown: Map::new(),
            })
        }
    };
    let noncurrent = match rule.noncurrent_version_expiration {
        None => None,
        Some(n) => {
            let days = n.noncurrent_days.ok_or_else(malformed)?;
            let newer = match n.newer_noncurrent_versions {
                Some(v) if !(1..=MAX_NEWER_VERSIONS).contains(&v) => {
                    return Err(invalid(&format!(
                        "'NewerNoncurrentVersions' must be between 1 and {MAX_NEWER_VERSIONS}"
                    )));
                }
                v => v.map(|v| v.unsigned_abs()),
            };
            Some(NoncurrentExpiration {
                noncurrent_days: positive(days, "NoncurrentDays")?,
                newer_noncurrent_versions: newer,
                unknown: Map::new(),
            })
        }
    };
    let abort = match rule.abort_incomplete_multipart_upload {
        None => None,
        Some(a) => {
            let days = a.days_after_initiation.ok_or_else(malformed)?;
            Some(AbortUpload {
                days_after_initiation: positive(days, "DaysAfterInitiation")?,
                unknown: Map::new(),
            })
        }
    };
    // An upload has no size until it completes.
    if abort.is_some() && filter.as_ref().is_some_and(Filter::has_size) {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            "AbortIncompleteMultipartUpload cannot be combined with object size filters",
        ));
    }
    let transitions = rule
        .transitions
        .unwrap_or_default()
        .into_iter()
        .map(parse_transition)
        .collect::<S3Result<Vec<_>>>()?;
    let noncurrent_transitions = rule
        .noncurrent_version_transitions
        .unwrap_or_default()
        .into_iter()
        .map(parse_noncurrent_transition)
        .collect::<S3Result<Vec<_>>>()?;
    check_transitions(&transitions, &noncurrent_transitions)?;
    let moves = !transitions.is_empty() || !noncurrent_transitions.is_empty();
    // Without a tier nothing can move, as before tiers existed.
    if moves && classes.is_empty() {
        unsupported.push("transitions");
    }
    if let Some(what) = unsupported.first() {
        return Err(S3Error::with_message(
            S3ErrorCode::NotImplemented,
            format!("lifecycle {what} are not supported"),
        ));
    }
    // A class no tier has would leave a client believing its objects move while none ever does.
    if let Some(class) = transitions
        .iter()
        .map(|t| &t.storage_class)
        .chain(noncurrent_transitions.iter().map(|t| &t.storage_class))
        .find(|class| !classes.contains(&class.as_str()))
    {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidStorageClass,
            format!("no tier of this server has the storage class {class}"),
        ));
    }
    if expiration.is_none() && noncurrent.is_none() && abort.is_none() && !moves {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            "At least one action needs to be specified in a rule",
        ));
    }
    Ok(Rule {
        id,
        enabled,
        prefix: rule.prefix,
        filter,
        expiration,
        noncurrent_version_expiration: noncurrent,
        abort_incomplete_multipart_upload: abort,
        transitions,
        noncurrent_version_transitions: noncurrent_transitions,
        unknown: Map::new(),
    })
}

fn parse_transition(t: s3s::dto::Transition) -> S3Result<Transition> {
    let days = t
        .days
        .map(|d| {
            u32::try_from(d)
                .map_err(|_| invalid("'Days' for Transition action must be a nonnegative integer"))
        })
        .transpose()?;
    let date = t.date.as_ref().map(midnight).transpose()?;
    if days.is_some() == date.is_some() {
        return Err(malformed());
    }
    Ok(Transition {
        days,
        date,
        storage_class: t.storage_class.ok_or_else(malformed)?.as_str().to_owned(),
        unknown: Map::new(),
    })
}

fn parse_noncurrent_transition(t: NoncurrentVersionTransition) -> S3Result<NoncurrentTransition> {
    let days = t.noncurrent_days.ok_or_else(malformed)?;
    let days = u32::try_from(days).map_err(|_| {
        invalid(
            "'NoncurrentDays' for NoncurrentVersionTransition action must be a nonnegative integer",
        )
    })?;
    let newer = match t.newer_noncurrent_versions {
        Some(v) if !(1..=MAX_NEWER_VERSIONS).contains(&v) => {
            return Err(invalid(&format!(
                "'NewerNoncurrentVersions' must be between 1 and {MAX_NEWER_VERSIONS}"
            )));
        }
        v => v.map(|v| v.unsigned_abs()),
    };
    Ok(NoncurrentTransition {
        noncurrent_days: days,
        newer_noncurrent_versions: newer,
        storage_class: t.storage_class.ok_or_else(malformed)?.as_str().to_owned(),
        unknown: Map::new(),
    })
}

/// AWS's rules across a rule's transitions: each names another class, and they all count days or
/// all name dates, so their order is the same for every object.
fn check_transitions(current: &[Transition], noncurrent: &[NoncurrentTransition]) -> S3Result<()> {
    let distinct =
        |classes: Vec<&str>| classes.iter().collect::<BTreeSet<_>>().len() == classes.len();
    if !distinct(current.iter().map(|t| t.storage_class.as_str()).collect())
        || !distinct(
            noncurrent
                .iter()
                .map(|t| t.storage_class.as_str())
                .collect(),
        )
    {
        return Err(invalid(
            "'StorageClass' must be different for 'Transition' actions in same 'Rule'",
        ));
    }
    if current.iter().any(|t| t.days.is_some()) && current.iter().any(|t| t.date.is_some()) {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            "Found mixed 'Date' and 'Days' based Transition actions in lifecycle rule",
        ));
    }
    Ok(())
}

fn to_i64(n: u64) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

fn to_i32(n: u32) -> i32 {
    i32::try_from(n).unwrap_or(i32::MAX)
}

/// The rule as GetBucketLifecycleConfiguration answers it: as it was put.
fn to_dto(rule: &Rule) -> LifecycleRule {
    let filter = rule.filter.as_ref().map(|f| {
        let and = f.and.as_deref().map(|a| LifecycleRuleAndOperator {
            prefix: a.prefix.clone(),
            object_size_greater_than: a.object_size_greater_than.map(to_i64),
            object_size_less_than: a.object_size_less_than.map(to_i64),
            tags: None,
        });
        LifecycleRuleFilter {
            and,
            object_size_greater_than: f.object_size_greater_than.map(to_i64),
            object_size_less_than: f.object_size_less_than.map(to_i64),
            prefix: f.prefix.clone(),
            tag: None,
        }
    });
    let status = if rule.enabled {
        ExpirationStatus::ENABLED
    } else {
        ExpirationStatus::DISABLED
    };
    LifecycleRule {
        abort_incomplete_multipart_upload: rule.abort_incomplete_multipart_upload.as_ref().map(
            |a| AbortIncompleteMultipartUpload {
                days_after_initiation: Some(to_i32(a.days_after_initiation)),
            },
        ),
        expiration: rule.expiration.as_ref().map(|e| LifecycleExpiration {
            date: e.date.map(|d| Timestamp::from(d.0)),
            days: e.days.map(to_i32),
            expired_object_delete_marker: e.expired_object_delete_marker,
        }),
        filter,
        id: Some(rule.id.clone()),
        noncurrent_version_expiration: rule.noncurrent_version_expiration.as_ref().map(|n| {
            NoncurrentVersionExpiration {
                newer_noncurrent_versions: n.newer_noncurrent_versions.map(to_i32),
                noncurrent_days: Some(to_i32(n.noncurrent_days)),
            }
        }),
        noncurrent_version_transitions: (!rule.noncurrent_version_transitions.is_empty()).then(
            || {
                rule.noncurrent_version_transitions
                    .iter()
                    .map(|t| NoncurrentVersionTransition {
                        newer_noncurrent_versions: t.newer_noncurrent_versions.map(to_i32),
                        noncurrent_days: Some(to_i32(t.noncurrent_days)),
                        storage_class: Some(TransitionStorageClass::from(t.storage_class.clone())),
                    })
                    .collect()
            },
        ),
        prefix: rule.prefix.clone(),
        status: ExpirationStatus::from_static(status),
        transitions: (!rule.transitions.is_empty()).then(|| {
            rule.transitions
                .iter()
                .map(|t| s3s::dto::Transition {
                    date: t.date.map(|d| Timestamp::from(d.0)),
                    days: t.days.map(to_i32),
                    storage_class: Some(TransitionStorageClass::from(t.storage_class.clone())),
                })
                .collect()
        }),
    }
}

pub(super) async fn put_bucket_lifecycle_configuration(
    dal: &S3Dal,
    req: S3Request<PutBucketLifecycleConfigurationInput>,
) -> S3Result<S3Response<PutBucketLifecycleConfigurationOutput>> {
    let input = req.input;
    naming::require_bucket(dal, &input.bucket).await?;
    let classes: Vec<&str> = dal.tiers.iter().map(|t| t.storage_class.as_str()).collect();
    let lifecycle = parse(
        input.lifecycle_configuration.ok_or_else(malformed)?,
        &classes,
    )?;
    bucket_state::update(dal, &input.bucket, |s| {
        s.lifecycle = Some(lifecycle.clone());
    })
    .await?;
    Ok(S3Response::new(
        PutBucketLifecycleConfigurationOutput::default(),
    ))
}

pub(super) async fn get_bucket_lifecycle_configuration(
    dal: &S3Dal,
    req: S3Request<GetBucketLifecycleConfigurationInput>,
) -> S3Result<S3Response<GetBucketLifecycleConfigurationOutput>> {
    let bucket = &req.input.bucket;
    naming::require_bucket(dal, bucket).await?;
    let state = bucket_state::get(dal, bucket).await?;
    let Some(lifecycle) = &state.lifecycle else {
        let mut err = S3Error::new(S3ErrorCode::NoSuchLifecycleConfiguration);
        err.set_status_code(hyper::StatusCode::NOT_FOUND);
        return Err(err);
    };
    Ok(S3Response::new(GetBucketLifecycleConfigurationOutput {
        rules: Some(lifecycle.rules.iter().map(to_dto).collect()),
        ..Default::default()
    }))
}

/// Answers 204 whether or not there was a configuration, as AWS does.
pub(super) async fn delete_bucket_lifecycle(
    dal: &S3Dal,
    req: S3Request<DeleteBucketLifecycleInput>,
) -> S3Result<S3Response<DeleteBucketLifecycleOutput>> {
    let bucket = &req.input.bucket;
    naming::require_bucket(dal, bucket).await?;
    if bucket_state::get(dal, bucket).await?.lifecycle.is_some() {
        bucket_state::update(dal, bucket, |s| s.lifecycle = None).await?;
    }
    Ok(S3Response::new(DeleteBucketLifecycleOutput::default()))
}

// ----- x-amz-expiration -----

/// The `x-amz-expiration` header of the current version of `key`, `size` bytes, created at
/// `created`: when the earliest rule that applies to it will expire it, and that rule's ID,
/// URL-encoded as AWS sends it. The rules come with the bucket's cached state, which the request
/// has already read, so this costs no backend request.
pub(super) async fn expiration_header(
    dal: &S3Dal,
    bucket: &str,
    key: &str,
    size: u64,
    created: Option<SystemTime>,
) -> S3Result<Option<String>> {
    // A folder marker is a directory here, which no rule expires.
    let Some(created) = created.filter(|_| !key.ends_with('/')) else {
        return Ok(None);
    };
    let state = bucket_state::get(dal, bucket).await?;
    Ok(state
        .lifecycle
        .as_ref()
        .and_then(|l| header(l, key, size, created)))
}

fn header(lifecycle: &Lifecycle, key: &str, size: u64, created: SystemTime) -> Option<String> {
    let (at, rule) = lifecycle
        .active()
        .filter(|r| r.matches(key, size))
        .filter_map(|r| Some((r.expiry(created)?, r)))
        .min_by_key(|(at, _)| *at)?;
    let mut date = Vec::new();
    Timestamp::from(at)
        .format(TimestampFormat::HttpDate, &mut date)
        .ok()?;
    Some(format!(
        "expiry-date=\"{}\", rule-id=\"{}\"",
        String::from_utf8_lossy(&date),
        url_encode(&rule.id)
    ))
}

/// Percent-encodes every byte but the unreserved ones (RFC 3986), so any rule ID fits a header
/// and cannot end the quoted value early.
fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

// ----- Applying the rules -----

/// What a lifecycle run removed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Expired {
    /// Current versions expired: removed in an unversioned bucket, hidden behind a delete marker
    /// in a versioned one.
    pub current: u64,
    /// Noncurrent versions removed for good.
    pub noncurrent: u64,
    /// Delete markers removed: noncurrent ones, and expired object delete markers.
    pub delete_markers: u64,
    /// Incomplete multipart uploads aborted.
    pub uploads: u64,
}

impl Expired {
    fn plus(self, o: Self) -> Self {
        Self {
            current: self.current + o.current,
            noncurrent: self.noncurrent + o.noncurrent,
            delete_markers: self.delete_markers + o.delete_markers,
            uploads: self.uploads + o.uploads,
        }
    }
}

/// The kinds of [`Expired`], as the metrics label them.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ExpiredKind {
    Current,
    Noncurrent,
    DeleteMarker,
    Upload,
}

impl ExpiredKind {
    pub(crate) const ALL: [Self; 4] = [
        Self::Current,
        Self::Noncurrent,
        Self::DeleteMarker,
        Self::Upload,
    ];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::Noncurrent => "noncurrent",
            Self::DeleteMarker => "delete_marker",
            Self::Upload => "upload",
        }
    }
}

/// Per process: the day (UTC, days since the epoch) each bucket's rules were last applied in
/// full, and the rules as they were then. The job looks in hourly and applies a bucket again
/// once the day has turned or its rules have changed, so a bucket is listed once a day, soon
/// after midnight, and a rule just put takes effect within the hour rather than the next day.
#[derive(Default)]
pub(crate) struct Runs {
    done: Mutex<HashMap<String, (u64, Lifecycle)>>,
}

impl Runs {
    fn due(&self, bucket: &str, day: u64, lifecycle: &Lifecycle) -> bool {
        let done = self.done.lock().unwrap_or_else(PoisonError::into_inner);
        done.get(bucket)
            .is_none_or(|(d, l)| *d != day || l != lifecycle)
    }

    fn record(&self, bucket: &str, day: u64, lifecycle: Lifecycle) {
        let mut done = self.done.lock().unwrap_or_else(PoisonError::into_inner);
        // Buckets deleted since would otherwise stay here for the life of the process.
        done.retain(|_, (d, _)| *d == day);
        done.insert(bucket.to_owned(), (day, lifecycle));
    }
}

fn day_of(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() / DAY)
}

/// Applies the rules of every bucket that has any, as of `now`; with `only_due`, only those of
/// buckets not yet applied today with the rules they have now (see [`Runs`]). A bucket that
/// fails is logged and tried again at the next run; the error is for a run where every bucket
/// failed, or the buckets could not be listed.
pub(crate) async fn apply(dal: &S3Dal, now: SystemTime, only_due: bool) -> anyhow::Result<Expired> {
    let day = day_of(now);
    let mut total = Expired::default();
    let (mut applied, mut failed) = (0, None);
    for bucket in usage::bucket_names(dal).await? {
        let state = match bucket_state::get(dal, &bucket).await {
            Ok(state) => state,
            Err(err) => {
                tracing::warn!(bucket, error = %err, "cannot read the bucket's lifecycle rules");
                failed = Some(err);
                continue;
            }
        };
        let Some(lifecycle) = state
            .lifecycle
            .as_ref()
            .filter(|l| l.active().next().is_some())
        else {
            continue;
        };
        if only_due && !dal.lifecycle_runs.due(&bucket, day, lifecycle) {
            continue;
        }
        match apply_bucket(dal, &bucket, lifecycle, now).await {
            Ok((expired, moved)) => {
                applied += 1;
                total = total.plus(expired);
                if expired != Expired::default() || moved > 0 {
                    tracing::info!(
                        bucket,
                        ?expired,
                        moved,
                        "applied the bucket's lifecycle rules"
                    );
                }
                dal.lifecycle_runs.record(&bucket, day, lifecycle.clone());
            }
            Err(err) => {
                tracing::warn!(bucket, error = %err, "cannot apply the bucket's lifecycle rules");
                failed = Some(err);
            }
        }
    }
    match failed {
        Some(err) if applied == 0 => Err(anyhow::anyhow!(
            "cannot apply any bucket's lifecycle rules: {err}"
        )),
        _ => Ok(total),
    }
}

/// Lists the bucket whole, its versions included, and applies its rules as of `now`: what it
/// removed, and how many objects and versions it moved into a tier.
async fn apply_bucket(
    dal: &S3Dal,
    bucket: &str,
    lifecycle: &Lifecycle,
    now: SystemTime,
) -> S3Result<(Expired, u64)> {
    let rules: Vec<&Rule> = lifecycle.active().collect();
    // Transitions stored while tiers were configured do nothing without them; listing for them
    // would cost a server without tiers what it never paid before.
    let tiered = dal.has_tiers();
    let state = version::state(dal, bucket).await?;
    let tracked = bucket_state::tracked(dal, bucket).await?;
    let run = Run {
        dal,
        bucket,
        rules: &rules,
        state,
        tracked,
        now,
    };
    let mut expired = Expired::default();
    let mut moved = 0;
    // Current versions first: one expired in a versioned bucket becomes noncurrent now, and its
    // NoncurrentDays count from here, as on AWS.
    if rules
        .iter()
        .any(|r| r.expires_current() || (tiered && r.moves_current()))
    {
        let (current, transitioned) = run.expire_current().await?;
        expired.current = current;
        moved += transitioned;
    }
    if state != Versioning::Unversioned
        && rules
            .iter()
            .any(|r| r.expires_versions() || (tiered && r.moves_versions()))
    {
        let (noncurrent, markers, transitioned) = run.expire_versions().await?;
        expired.noncurrent = noncurrent;
        expired.delete_markers = markers;
        moved += transitioned;
    }
    if rules
        .iter()
        .any(|r| r.abort_incomplete_multipart_upload.is_some())
    {
        let aborted = multipart::abort_by_lifecycle(dal, bucket, now)
            .await
            .map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, err.to_string()))?;
        dal.metrics()
            .lifecycle_expired(ExpiredKind::Upload, aborted);
        expired.uploads = aborted as u64;
    }
    Ok((expired, moved))
}

/// The deadline of an incomplete upload of `key` started at `initiated` under the bucket's abort
/// rules, the earliest of those that apply; `None` when none applies, which leaves the upload
/// to `--stale-upload-age`.
pub(super) fn abort_due(
    lifecycle: Option<&Lifecycle>,
    key: &str,
    initiated: SystemTime,
) -> Option<SystemTime> {
    lifecycle?
        .active()
        .filter(|r| r.covers(key))
        .filter_map(|r| {
            let abort = r.abort_incomplete_multipart_upload.as_ref()?;
            Some(due(initiated, abort.days_after_initiation))
        })
        .min()
}

/// One application of a bucket's rules.
struct Run<'a> {
    dal: &'a S3Dal,
    bucket: &'a str,
    rules: &'a [&'a Rule],
    state: Versioning,
    tracked: bool,
    now: SystemTime,
}

/// A current object the listing found expired.
struct Candidate {
    key: String,
    size: u64,
    created: SystemTime,
}

impl Run<'_> {
    fn current_expired(&self, key: &str, size: u64, created: SystemTime) -> bool {
        self.rules
            .iter()
            .any(|r| r.matches(key, size) && r.expiry(created).is_some_and(|at| self.now >= at))
    }

    /// The class the transitions of the rules that apply to an object of `size` bytes at `key`,
    /// created at `created`, have moved it to by now: that of the transition due last, as on AWS,
    /// where an object passes each class in turn. An empty object has nothing to move.
    fn current_target(&self, key: &str, size: u64, created: SystemTime) -> Option<&str> {
        if size == 0 {
            return None;
        }
        self.rules
            .iter()
            .filter(|r| r.matches(key, size))
            .flat_map(|r| r.transitions.iter())
            .filter_map(|t| Some((t.at(created)?, t.storage_class.as_str())))
            .filter(|(at, _)| self.now >= *at)
            .max_by_key(|(at, _)| *at)
            .map(|(_, class)| class)
    }

    /// [`Self::current_target`] for a version noncurrent since `since`, with `newer` noncurrent
    /// versions above it.
    fn noncurrent_target(
        &self,
        key: &str,
        size: u64,
        newer: usize,
        since: SystemTime,
    ) -> Option<&str> {
        if size == 0 {
            return None;
        }
        self.rules
            .iter()
            .filter(|r| r.matches(key, size))
            .flat_map(|r| r.noncurrent_version_transitions.iter())
            .filter(|t| newer >= t.newer_noncurrent_versions.unwrap_or(0) as usize)
            .map(|t| (due(since, t.noncurrent_days), t.storage_class.as_str()))
            .filter(|(at, _)| self.now >= *at)
            .max_by_key(|(at, _)| *at)
            .map(|(_, class)| class)
    }

    /// Expires the current versions the rules name, and moves those their transitions name into
    /// a tier; how many of each. Expiration wins over a transition due the same day, as on AWS.
    /// The listing is read whole before anything is removed: removing a key can prune
    /// directories that the listing has yet to enter.
    async fn expire_current(&self) -> S3Result<(u64, u64)> {
        let tiered = self.dal.has_tiers();
        let dir = naming::bucket_dir(self.bucket);
        let mut lister = match self.dal.op.lister_with(&dir).recursive(true).await {
            Ok(lister) => lister,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok((0, 0)),
            Err(err) => return Err(map(err, Ctx::Bucket)),
        };
        let (mut found, mut moves) = (Vec::new(), Vec::new());
        while let Some(entry) = lister.try_next().await.map_err(|e| map(e, Ctx::Bucket))? {
            if !entry.metadata().is_file() {
                continue;
            }
            let Some(key) = entry.path().strip_prefix(&dir) else {
                continue;
            };
            if !self
                .rules
                .iter()
                .any(|r| (r.expires_current() || (tiered && r.moves_current())) && r.covers(key))
            {
                continue;
            }
            // fs lists names only; the object stores list sizes and times too, but those of a
            // stub are an empty object's: its size, time and class are in its metadata.
            let listed = entry.metadata();
            let stub_like = tiered && listed.content_length() == 0;
            let (size, created, class) = match listed.last_modified() {
                Some(t) if !stub_like => (listed.content_length(), SystemTime::from(t), None),
                _ => match store::stat_existing(self.dal, entry.path()).await? {
                    Some(m) if m.is_file() => {
                        let obj = meta::resolve(&self.dal.profile, &m);
                        let class = self.dal.storage_class(&obj).map(str::to_owned);
                        match obj.last_modified {
                            Some(t) => (obj.content_length, t, class),
                            None => continue,
                        }
                    }
                    _ => continue,
                },
            };
            if self.current_expired(key, size, created) {
                found.push(Candidate {
                    key: key.to_owned(),
                    size,
                    created,
                });
            } else if let Some(target) = self.current_target(key, size, created).filter(|_| tiered)
                && class.as_deref() != Some(target)
            {
                moves.push((key.to_owned(), target.to_owned()));
            }
        }
        let mut expired = 0;
        for c in found {
            if self.expire_one(&c).await? {
                expired += 1;
            }
        }
        let mut moved = 0;
        for (key, class) in moves {
            let path = naming::object_path(self.bucket, &key);
            if self.transition(&key, &path, &class, false).await {
                moved += 1;
            }
        }
        Ok((expired, moved))
    }

    /// Moves the object at `path`, `key`'s current version or (`noncurrent`) an archived one,
    /// into the tier of `class`, and announces it. Whether it moved. A failure is counted and
    /// logged and leaves the object for the next day's run: one object a tier cannot take must
    /// not hold up the rest of the bucket.
    async fn transition(&self, key: &str, path: &str, class: &str, noncurrent: bool) -> bool {
        let Some(tier) = self.dal.tier_for_class(Some(class)) else {
            tracing::warn!(
                bucket = self.bucket,
                key,
                class,
                "a lifecycle transition names a class no tier of this server has"
            );
            return false;
        };
        match tier::move_object(self.dal, self.bucket, key, path, tier).await {
            Moved::Done(obj) => {
                let obj = *obj;
                let version = (noncurrent || self.state != Versioning::Unversioned)
                    .then(|| obj.version_id.unwrap_or_else(|| version::NULL.to_owned()));
                self.dal.emit(ObjectEvent {
                    size: Some(obj.content_length),
                    etag: Some(obj.etag),
                    version,
                    ..ObjectEvent::new(
                        EventKind::LifecycleTransition,
                        self.bucket,
                        key,
                        Origin::default(),
                    )
                });
                true
            }
            Moved::Nothing => false,
            Moved::Skipped(reason) => {
                self.dal.metrics().transition_skipped(reason);
                false
            }
        }
    }

    /// Removes the object, or in a versioned bucket puts a delete marker on it, provided it is
    /// still the one listed: a client may have replaced it since, with an object its rules give
    /// a later date. Whether it was expired.
    async fn expire_one(&self, c: &Candidate) -> S3Result<bool> {
        let created = Timestamp::from(c.created);
        let cond = DeleteCondition {
            if_match: None,
            last_modified: Some(&created),
            size: i64::try_from(c.size).ok(),
        };
        let done = if self.state == Versioning::Unversioned {
            store::delete_key(self.dal, self.bucket, &c.key, cond)
                .await
                .map(|removed| removed.map(|r| (EventKind::LifecycleDelete, r, None)))
        } else {
            let removals = Removals::default();
            let deleted = version::delete(
                self.dal,
                self.bucket,
                &c.key,
                None,
                cond,
                self.state,
                self.tracked,
                &removals,
            )
            .await;
            if deleted.is_ok() {
                removals.run(self.dal).await;
            }
            deleted.map(|d| {
                d.change
                    .map(|(_, r)| (EventKind::LifecycleDeleteMarker, r, d.version_id))
            })
        };
        match done {
            Ok(Some((kind, removed, version))) => {
                self.announce(kind, &c.key, version, removed);
                self.dal
                    .metrics()
                    .lifecycle_expired(ExpiredKind::Current, 1);
                Ok(true)
            }
            Ok(None) => Ok(false),
            Err(err) if *err.code() == S3ErrorCode::PreconditionFailed => Ok(false),
            Err(err) => Err(err),
        }
    }

    fn announce(&self, kind: EventKind, key: &str, version: Option<String>, removed: Usage) {
        self.dal.emit(ObjectEvent {
            version,
            usage: self.tracked.then(|| Usage::default().minus(removed)),
            ..ObjectEvent::new(kind, self.bucket, key, Origin::default())
        });
    }

    /// Removes the noncurrent versions and delete markers the rules name, and moves the versions
    /// their noncurrent transitions name into a tier. Returns how many versions and how many
    /// markers were removed, and how many versions moved.
    async fn expire_versions(&self) -> S3Result<(u64, u64, u64)> {
        let tiered = self.dal.has_tiers();
        let top = version::versions_dir(self.bucket);
        let mut lister = match self.dal.op.lister_with(&top).recursive(true).await {
            Ok(lister) => lister,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok((0, 0, 0)),
            Err(err) => return Err(map(err, Ctx::Bucket)),
        };
        // The keys first, for the reason `expire_current` gives.
        let mut keys = BTreeSet::new();
        while let Some(entry) = lister.try_next().await.map_err(|e| map(e, Ctx::Bucket))? {
            if !entry.metadata().is_file() {
                continue;
            }
            let key = entry
                .path()
                .strip_prefix(&top)
                .and_then(|rel| rel.rsplit_once('/'))
                .and_then(|(holder, _)| holder.strip_suffix("!!"))
                .and_then(version::unescape);
            if let Some(key) = key
                && self.rules.iter().any(|r| {
                    (r.expires_versions() || (tiered && r.moves_versions())) && r.covers(&key)
                })
            {
                keys.insert(key);
            }
        }
        let (mut versions, mut markers, mut moved) = (0, 0, 0);
        for key in keys {
            let (v, m, t) = self.expire_key_versions(&key).await?;
            versions += v;
            markers += m;
            moved += t;
        }
        Ok((versions, markers, moved))
    }

    /// One key's versions: those noncurrent long enough go, then a delete marker left alone
    /// with no version under it ("expired object delete marker"). When a version became
    /// noncurrent is the time in its entry's name: the entry is made under the key's lock right
    /// before its successor commits, so that is when it stopped being current, the instant AWS
    /// counts NoncurrentDays from. Its Last-Modified would be when it was written instead.
    async fn expire_key_versions(&self, key: &str) -> S3Result<(u64, u64, u64)> {
        let path = naming::object_path(self.bucket, key);
        let current = store::stat_existing(self.dal, &path)
            .await?
            .filter(Metadata::is_file);
        let current_id = current.as_ref().map(|m| version::id_of(self.dal, m));
        // An archived copy of the current version, left by a commit that failed after archiving
        // it, is no noncurrent version.
        let mut entries: Vec<version::Entry> = version::entries(self.dal, self.bucket, key)
            .await?
            .into_iter()
            .filter(|e| e.marker || current_id.as_deref() != Some(e.id.as_str()))
            .collect();
        // Without a current object the newest entry is the latest version, marker or not.
        let first_noncurrent = usize::from(current.is_none());
        let sized = self.rules.iter().any(|r| {
            r.noncurrent_version_expiration.is_some()
                && r.covers(key)
                && r.filter.as_ref().is_some_and(Filter::has_size)
        });
        let tiered = self.dal.has_tiers();
        let (mut versions, mut markers, mut moved) = (0, 0, 0);
        let mut removed = Vec::new();
        for (newer, e) in entries.iter().enumerate().skip(first_noncurrent) {
            let newer = newer - first_noncurrent;
            let since = e.made();
            // A version a transition may be due for is stat'ed whatever the filters: its class,
            // and a stub's size, are only in its metadata.
            let movable = tiered
                && !e.marker
                && self.rules.iter().any(|r| {
                    r.covers(key)
                        && r.noncurrent_version_transitions.iter().any(|t| {
                            newer >= t.newer_noncurrent_versions.unwrap_or(0) as usize
                                && self.now >= due(since, t.noncurrent_days)
                        })
                });
            let found = if (sized || movable) && !e.marker {
                match store::stat_existing(self.dal, &e.path).await? {
                    Some(m) => Some(meta::resolve(&self.dal.profile, &m)),
                    None => continue,
                }
            } else {
                None
            };
            // The logical size: a cold version's stub is empty.
            let size = found.as_ref().map_or(0, |obj| obj.content_length);
            let expired = self.rules.iter().any(|r| {
                r.noncurrent_version_expiration.as_ref().is_some_and(|n| {
                    r.matches(key, size)
                        && newer >= n.newer_noncurrent_versions.unwrap_or(0) as usize
                        && self.now >= due(since, n.noncurrent_days)
                })
            });
            if expired {
                if self.remove(key, e).await? {
                    removed.push(e.path.clone());
                    if e.marker {
                        markers += 1;
                    } else {
                        versions += 1;
                    }
                }
                continue;
            }
            // In place: the version keeps its entry, and with it its id and its place in order.
            if let Some(obj) = found.as_ref().filter(|_| movable)
                && let Some(target) = self.noncurrent_target(key, size, newer, since)
                && self.dal.storage_class(obj) != Some(target)
                && self.transition(key, &e.path, target, true).await
            {
                moved += 1;
            }
        }
        entries.retain(|e| !removed.contains(&e.path));
        if let [marker] = entries.as_slice()
            && current.is_none()
            && marker.marker
        {
            let made = marker.made();
            let expired = self.rules.iter().any(|r| {
                r.matches(key, 0)
                    && (r.removes_lone_markers() || r.expiry(made).is_some_and(|at| self.now >= at))
            });
            if expired && self.remove(key, marker).await? {
                markers += 1;
            }
        }
        Ok((versions, markers, moved))
    }

    /// Removes one archived version or marker for good. Whether it was there to remove.
    async fn remove(&self, key: &str, entry: &version::Entry) -> S3Result<bool> {
        let removals = Removals::default();
        let deleted = version::delete(
            self.dal,
            self.bucket,
            key,
            Some(&entry.id),
            DeleteCondition::default(),
            self.state,
            self.tracked,
            &removals,
        )
        .await?;
        removals.run(self.dal).await;
        let Some((_, removed)) = deleted.change else {
            return Ok(false);
        };
        self.announce(
            EventKind::LifecycleDelete,
            key,
            Some(entry.id.clone()),
            removed,
        );
        let kind = if entry.marker {
            ExpiredKind::DeleteMarker
        } else {
            ExpiredKind::Noncurrent
        };
        self.dal.metrics().lifecycle_expired(kind, 1);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> SystemTime {
        OffsetDateTime::parse(s, &Rfc3339).unwrap().into()
    }

    #[test]
    fn days_count_to_the_next_midnight_utc() {
        // AWS's own example: created 2014-01-15 10:30 UTC, 3 days, gone from 2014-01-19 00:00.
        assert_eq!(
            due(at("2014-01-15T10:30:00Z"), 3),
            at("2014-01-19T00:00:00Z")
        );
        // A second before midnight still waits for that midnight; one at midnight waits a
        // whole day more, so no object goes before its full number of days.
        assert_eq!(
            due(at("2014-01-15T23:59:59Z"), 1),
            at("2014-01-17T00:00:00Z")
        );
        assert_eq!(
            due(at("2014-01-15T00:00:00Z"), 1),
            at("2014-01-17T00:00:00Z")
        );
        // An offset is just another way to write the same instant.
        assert_eq!(
            due(at("2014-01-15T23:30:00-02:00"), 1),
            at("2014-01-18T00:00:00Z")
        );
    }

    fn rule(json: &str) -> Rule {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn a_filter_matches_on_every_condition_it_has() {
        let r = rule(
            r#"{"id":"r","enabled":true,"filter":{"prefix":"logs/","and":{"object-size-greater-than":10,"object-size-less-than":20}},"expiration":{"days":1}}"#,
        );
        assert!(r.matches("logs/a", 15));
        assert!(!r.matches("logs/a", 10), "sizes compare strictly");
        assert!(!r.matches("logs/a", 20), "sizes compare strictly");
        assert!(!r.matches("data/a", 15));
        let legacy = rule(r#"{"id":"r","enabled":true,"prefix":"a/","expiration":{"days":1}}"#);
        assert!(legacy.matches("a/x", 0) && !legacy.matches("b/x", 0));
        let everything = rule(r#"{"id":"r","enabled":true,"filter":{},"expiration":{"days":1}}"#);
        assert!(everything.matches("any", u64::MAX));
    }

    #[test]
    fn a_rule_with_a_part_this_server_does_not_know_does_not_run() {
        // A newer s3dal sharing the backend may store a condition this one cannot evaluate;
        // run without it, the rule would remove objects the condition keeps.
        let known = rule(r#"{"id":"r","enabled":true,"expiration":{"days":1}}"#);
        assert!(known.active());
        for newer in [
            r#"{"id":"r","enabled":true,"expiration":{"days":1},"tags":[{"k":"v"}]}"#,
            r#"{"id":"r","enabled":true,"filter":{"tag":{"k":"v"}},"expiration":{"days":1}}"#,
            r#"{"id":"r","enabled":true,"filter":{"and":{"tags":[]}},"expiration":{"days":1}}"#,
            r#"{"id":"r","enabled":true,"expiration":{"days":1,"all-versions":true}}"#,
        ] {
            let r = rule(newer);
            assert!(!r.active(), "{newer}");
            // And it survives a rewrite of the state file.
            let back = serde_json::to_string(&r).unwrap();
            assert_eq!(rule(&back), r, "{back}");
        }
        let disabled = rule(r#"{"id":"r","enabled":false,"expiration":{"days":1}}"#);
        assert!(!disabled.active());
    }

    #[test]
    fn the_header_names_the_earliest_rule_that_applies() {
        let lifecycle: Lifecycle = serde_json::from_str(
            r#"{"rules":[
                {"id":"late","enabled":true,"expiration":{"days":30}},
                {"id":"soon \"quoted\"","enabled":true,"prefix":"tmp/","expiration":{"days":2}},
                {"id":"off","enabled":false,"expiration":{"days":1}},
                {"id":"markers","enabled":true,"expiration":{"expired-object-delete-marker":true}},
                {"id":"dated","enabled":true,"filter":{"object-size-greater-than":100},"expiration":{"date":"2014-01-16T00:00:00Z"}}
            ]}"#,
        )
        .unwrap();
        let created = at("2014-01-15T10:30:00Z");
        assert_eq!(
            header(&lifecycle, "tmp/a", 1, created).as_deref(),
            Some(r#"expiry-date="Sat, 18 Jan 2014 00:00:00 GMT", rule-id="soon%20%22quoted%22""#)
        );
        assert_eq!(
            header(&lifecycle, "big", 101, created).as_deref(),
            Some(r#"expiry-date="Thu, 16 Jan 2014 00:00:00 GMT", rule-id="dated""#)
        );
        assert_eq!(
            header(&lifecycle, "other", 1, created).as_deref(),
            Some(r#"expiry-date="Sat, 15 Feb 2014 00:00:00 GMT", rule-id="late""#)
        );
        let none: Lifecycle = serde_json::from_str(
            r#"{"rules":[{"id":"m","enabled":true,"expiration":{"expired-object-delete-marker":true}}]}"#,
        )
        .unwrap();
        assert_eq!(header(&none, "k", 1, created), None);
    }

    #[test]
    fn an_abort_rule_sets_the_deadline_of_the_uploads_it_covers() {
        let lifecycle: Lifecycle = serde_json::from_str(
            r#"{"rules":[
                {"id":"a","enabled":true,"prefix":"big/","abort-incomplete-multipart-upload":{"days-after-initiation":7}},
                {"id":"b","enabled":true,"abort-incomplete-multipart-upload":{"days-after-initiation":3}},
                {"id":"c","enabled":false,"prefix":"x/","abort-incomplete-multipart-upload":{"days-after-initiation":1}}
            ]}"#,
        )
        .unwrap();
        let t = at("2014-01-15T10:30:00Z");
        // The earliest of the rules that cover the key.
        assert_eq!(
            abort_due(Some(&lifecycle), "big/a", t),
            Some(at("2014-01-19T00:00:00Z"))
        );
        assert_eq!(
            abort_due(Some(&lifecycle), "x/a", t),
            Some(at("2014-01-19T00:00:00Z"))
        );
        assert_eq!(abort_due(None, "x/a", t), None);
    }

    #[test]
    fn a_bucket_is_applied_again_on_a_new_day_or_with_new_rules() {
        let runs = Runs::default();
        let one: Lifecycle = serde_json::from_str(
            r#"{"rules":[{"id":"r","enabled":true,"expiration":{"days":1}}]}"#,
        )
        .unwrap();
        let two: Lifecycle = serde_json::from_str(
            r#"{"rules":[{"id":"r","enabled":true,"expiration":{"days":2}}]}"#,
        )
        .unwrap();
        assert!(runs.due("b", 10, &one));
        runs.record("b", 10, one.clone());
        assert!(!runs.due("b", 10, &one), "twice on one day");
        assert!(runs.due("b", 10, &two), "the rules changed");
        assert!(runs.due("b", 11, &one), "the day turned");
        assert!(runs.due("c", 10, &one), "another bucket");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn an_object_replaced_since_the_listing_is_not_expired() {
        // The listing and the removal are apart; an object a client wrote in between is not the
        // one the rule expired, and its own days have only begun.
        let root = tempfile::tempdir().unwrap();
        let fs_root = root.path().to_string_lossy().into_owned();
        let op = crate::backend::build_operator("fs", &[("root".to_owned(), fs_root)]).unwrap();
        let profile = crate::backend::probe(&op).await.unwrap();
        let dal = S3Dal::new(op.clone(), profile, crate::Settings::default());
        op.write("bkt/k", b"new".to_vec()).await.unwrap();
        let written = store::stat_existing(&dal, "bkt/k").await.unwrap().unwrap();
        let written = SystemTime::from(written.last_modified().unwrap());
        let r = rule(r#"{"id":"r","enabled":true,"expiration":{"days":1}}"#);
        let run = Run {
            dal: &dal,
            bucket: "bkt",
            rules: &[&r],
            state: Versioning::Unversioned,
            tracked: false,
            now: written + Duration::from_secs(3 * DAY),
        };
        let listed = |size, created| Candidate {
            key: "k".to_owned(),
            size,
            created,
        };
        let older = written - Duration::from_secs(2 * DAY);
        assert!(!run.expire_one(&listed(3, older)).await.unwrap());
        assert!(!run.expire_one(&listed(9, written)).await.unwrap());
        assert!(op.exists("bkt/k").await.unwrap());
        assert!(run.expire_one(&listed(3, written)).await.unwrap());
        assert!(!op.exists("bkt/k").await.unwrap());
    }
}

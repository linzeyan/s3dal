//! Conditional reads (GET, HEAD, and a copy's source), evaluated in the order RFC 9110 §13.2.2
//! prescribes, which is also what AWS documents for S3.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use s3s::dto::{ETag, ETagCondition, Timestamp};
use s3s::{S3Error, S3ErrorCode, S3Result, s3_error};

pub(super) struct Conditions<'a> {
    pub if_match: Option<&'a ETagCondition>,
    pub if_none_match: Option<&'a ETagCondition>,
    pub if_modified_since: Option<&'a Timestamp>,
    pub if_unmodified_since: Option<&'a Timestamp>,
}

/// `etag` is unquoted, as in [`crate::meta::ObjectMeta`].
pub(super) fn check(
    c: &Conditions<'_>,
    etag: &str,
    last_modified: Option<SystemTime>,
) -> S3Result<()> {
    let current = ETag::Strong(etag.to_owned());
    // HTTP dates carry whole seconds. Comparing the full-precision mtime would make a client
    // that echoes Last-Modified back always see the object as modified since then.
    let mtime = last_modified.map(|t| Timestamp::from(whole_seconds(t)));

    // A date condition only applies when the matching ETag condition is absent.
    if let Some(cond) = c.if_match {
        let matched = match cond {
            ETagCondition::Any => true,
            ETagCondition::ETag(tag) => listed(tag).iter().any(|t| current.strong_cmp(t)),
        };
        if !matched {
            return Err(precondition_failed());
        }
    } else if let (Some(since), Some(mtime)) = (c.if_unmodified_since, &mtime)
        && mtime > since
    {
        return Err(precondition_failed());
    }

    if let Some(cond) = c.if_none_match {
        let matched = match cond {
            ETagCondition::Any => true,
            ETagCondition::ETag(tag) => listed(tag).iter().any(|t| current.weak_cmp(t)),
        };
        if matched {
            return Err(not_modified(&current));
        }
    } else if let (Some(since), Some(mtime)) = (c.if_modified_since, &mtime)
        && mtime <= since
    {
        return Err(not_modified(&current));
    }

    Ok(())
}

/// The entity tags of an If-Match or If-None-Match header, which RFC 9110 §13.1.1 defines as a
/// list (`"a", W/"b"`). s3s parses the header as one tag, from its first quote to its last, so a
/// list arrives as one tag with quotes inside, which no entity tag may contain (§8.8.3). Such a
/// tag is split back into the list; one that is no valid list stays as s3s parsed it.
fn listed(tag: &ETag) -> Vec<ETag> {
    let (value, header) = match tag {
        ETag::Strong(v) => (v, format!("\"{v}\"")),
        ETag::Weak(v) => (v, format!("W/\"{v}\"")),
    };
    if !value.contains('"') {
        return vec![tag.clone()];
    }
    split_list(&header).unwrap_or_else(|| vec![tag.clone()])
}

fn split_list(header: &str) -> Option<Vec<ETag>> {
    let mut tags = Vec::new();
    let mut rest = header;
    loop {
        // Empty list elements are allowed (§5.6.1): `"a", , "b"`.
        rest = rest.trim_start_matches([' ', '\t', ',']);
        if rest.is_empty() {
            return (!tags.is_empty()).then_some(tags);
        }
        let weak = rest.starts_with("W/");
        if weak {
            rest = &rest[2..];
        }
        rest = rest.strip_prefix('"')?;
        let end = rest.find('"')?;
        let value = rest[..end].to_owned();
        tags.push(if weak {
            ETag::Weak(value)
        } else {
            ETag::Strong(value)
        });
        rest = rest[end + 1..].trim_start_matches([' ', '\t']);
        if !(rest.is_empty() || rest.starts_with(',')) {
            return None;
        }
    }
}

pub(super) fn whole_seconds(t: SystemTime) -> SystemTime {
    let secs = t.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
    UNIX_EPOCH + Duration::from_secs(secs)
}

fn precondition_failed() -> S3Error {
    s3_error!(
        PreconditionFailed,
        "At least one of the pre-conditions you specified did not hold"
    )
}

/// RFC 9110 §15.4.5: a 304 must still carry the ETag a 200 would have, so caches can keep
/// validating with it.
fn not_modified(etag: &ETag) -> S3Error {
    let mut err = S3Error::new(S3ErrorCode::NotModified);
    let mut headers = err.headers().cloned().unwrap_or_default();
    if let Ok(value) = etag.to_http_header() {
        headers.insert("etag", value);
    }
    err.set_headers(headers);
    err
}

#[cfg(test)]
mod tests {
    use super::*;

    const ETAG: &str = "abc";

    fn at(secs: u64, nanos: u32) -> SystemTime {
        UNIX_EPOCH + Duration::new(secs, nanos)
    }

    fn date(secs: u64) -> Timestamp {
        Timestamp::from(at(secs, 0))
    }

    fn none() -> Conditions<'static> {
        Conditions {
            if_match: None,
            if_none_match: None,
            if_modified_since: None,
            if_unmodified_since: None,
        }
    }

    fn outcome(c: &Conditions<'_>, mtime: SystemTime) -> Option<S3ErrorCode> {
        check(c, ETAG, Some(mtime)).err().map(|e| e.code().clone())
    }

    fn tag(value: &str) -> ETagCondition {
        ETagCondition::ETag(ETag::Strong(value.to_owned()))
    }

    #[test]
    fn if_match_overrides_if_unmodified_since() {
        // AWS: If-Match true and If-Unmodified-Since false returns 200; a client that pins
        // an ETag must not be refused because of a clock-based condition.
        let matching = tag(ETAG);
        let past = date(10);
        let c = Conditions {
            if_match: Some(&matching),
            if_unmodified_since: Some(&past),
            ..none()
        };
        assert_eq!(outcome(&c, at(20, 0)), None);
    }

    #[test]
    fn if_none_match_overrides_if_modified_since() {
        // A changed ETag means the client's copy is stale, whatever the dates say.
        let other = tag("old");
        let future = date(30);
        let c = Conditions {
            if_none_match: Some(&other),
            if_modified_since: Some(&future),
            ..none()
        };
        assert_eq!(outcome(&c, at(20, 0)), None);
    }

    #[test]
    fn precondition_failure_wins_over_not_modified() {
        // 412 is evaluated first: a write guard must never be answered with "use your cache".
        let other = tag("old");
        let c = Conditions {
            if_match: Some(&other),
            if_none_match: Some(&ETagCondition::Any),
            ..none()
        };
        assert_eq!(
            outcome(&c, at(20, 0)),
            Some(S3ErrorCode::PreconditionFailed)
        );
    }

    #[test]
    fn echoed_last_modified_is_not_modified_despite_subsecond_mtime() {
        // Last-Modified is sent in whole seconds; the mtime on disk has nanoseconds.
        let echoed = date(20);
        let c = Conditions {
            if_modified_since: Some(&echoed),
            ..none()
        };
        assert_eq!(
            outcome(&c, at(20, 999_999_999)),
            Some(S3ErrorCode::NotModified)
        );
        assert_eq!(outcome(&c, at(21, 0)), None);
    }

    #[test]
    fn if_unmodified_since_fails_only_after_a_later_write() {
        let since = date(20);
        let c = Conditions {
            if_unmodified_since: Some(&since),
            ..none()
        };
        assert_eq!(outcome(&c, at(20, 500)), None);
        assert_eq!(
            outcome(&c, at(21, 0)),
            Some(S3ErrorCode::PreconditionFailed)
        );
    }

    #[test]
    fn if_none_match_uses_weak_comparison() {
        // RFC 9110: If-None-Match matches W/"abc" against "abc".
        let weak = ETagCondition::ETag(ETag::Weak(ETAG.to_owned()));
        let c = Conditions {
            if_none_match: Some(&weak),
            ..none()
        };
        assert_eq!(outcome(&c, at(20, 0)), Some(S3ErrorCode::NotModified));
    }

    #[test]
    fn an_etag_list_matches_any_of_its_tags() {
        // RFC 9110 §13.1.1: both headers take a list, which s3s hands over as one tag. A cache
        // holding several versions revalidates with all of them at once.
        let listed = tag(&format!("old\", \"{ETAG}"));
        let c = Conditions {
            if_match: Some(&listed),
            ..none()
        };
        assert_eq!(outcome(&c, at(20, 0)), None);
        let c = Conditions {
            if_none_match: Some(&listed),
            ..none()
        };
        assert_eq!(outcome(&c, at(20, 0)), Some(S3ErrorCode::NotModified));

        let others = tag("old\", \"older");
        let c = Conditions {
            if_match: Some(&others),
            ..none()
        };
        assert_eq!(
            outcome(&c, at(20, 0)),
            Some(S3ErrorCode::PreconditionFailed)
        );
    }

    #[test]
    fn a_weak_tag_in_a_list_keeps_its_comparison() {
        // If-Match compares strongly, so W/"abc" in its list must not match; If-None-Match
        // compares weakly, so it does.
        let listed = tag(&format!("old\", W/\"{ETAG}"));
        let c = Conditions {
            if_match: Some(&listed),
            ..none()
        };
        assert_eq!(
            outcome(&c, at(20, 0)),
            Some(S3ErrorCode::PreconditionFailed)
        );
        let c = Conditions {
            if_none_match: Some(&listed),
            ..none()
        };
        assert_eq!(outcome(&c, at(20, 0)), Some(S3ErrorCode::NotModified));
    }

    #[test]
    fn only_a_well_formed_list_is_split() {
        // An entity tag may hold a comma, so a tag without quotes inside is one tag; text between
        // quoted tags that is no list separator leaves the header as s3s parsed it.
        let ETagCondition::ETag(comma) = tag("a,b") else {
            unreachable!()
        };
        assert_eq!(listed(&comma), [ETag::Strong("a,b".to_owned())]);
        let ETagCondition::ETag(junk) = tag("a\" junk \"b") else {
            unreachable!()
        };
        assert_eq!(listed(&junk), std::slice::from_ref(&junk));
        let ETagCondition::ETag(spaced) = tag("a\" , ,W/\"b") else {
            unreachable!()
        };
        assert_eq!(
            listed(&spaced),
            [ETag::Strong("a".to_owned()), ETag::Weak("b".to_owned())]
        );
    }

    #[test]
    fn not_modified_carries_the_etag() {
        let c = Conditions {
            if_none_match: Some(&ETagCondition::Any),
            ..none()
        };
        let err = check(&c, ETAG, None).unwrap_err();
        let etag = err.headers().and_then(|h| h.get("etag"));
        assert_eq!(etag.map(|v| v.as_bytes()), Some(&b"\"abc\""[..]));
    }
}

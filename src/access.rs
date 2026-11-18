//! What a key pair may reach. A pair names the buckets, or folders of them, it is limited to,
//! and whether it may read there, write there, or both; a pair that names neither reaches every
//! bucket and the settings, as every pair did before limits existed. This is an allowlist, not
//! IAM: there are no policies, deny rules or conditions, so what a pair reaches can be read off
//! its keys. A pair may also have an expiry, after which it signs nothing.

use std::fmt;
use std::time::SystemTime;

use serde::{Deserialize, Deserializer};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Access {
    #[default]
    ReadWrite,
    ReadOnly,
    /// A drop box: objects go in, and nothing about them comes back out.
    WriteOnly,
}

impl Access {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReadWrite => "read-write",
            Self::ReadOnly => "read-only",
            Self::WriteOnly => "write-only",
        }
    }
}

/// Operations that change nothing, by s3s's name for them.
pub fn reads(op: &str) -> bool {
    ["Get", "Head", "List"].iter().any(|p| op.starts_with(p)) || op == "SelectObjectContent"
}

/// All a write-only pair may do: put an object in, by any of S3's ways, and carry a multipart
/// upload through to its end, which takes listing the upload's own parts to resume it and
/// aborting it to clean up. Named rather than derived from `reads`, so an operation s3s adds
/// later stays closed to a drop box until it is placed here. Deletes are left out on purpose: a
/// drop box that could empty itself would not keep what was dropped.
const DROPS: [&str; 9] = [
    "PutObject",
    "PostObject",
    "CopyObject",
    "UploadPartCopy",
    "CreateMultipartUpload",
    "UploadPart",
    "CompleteMultipartUpload",
    "AbortMultipartUpload",
    "ListParts",
];

/// The instant a key pair stops signing, in UTC. Written in RFC 3339, the form TOML and the
/// console's date field both produce, with any offset; kept in UTC so it prints the same
/// instant back whatever offset it was written with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Expiry(OffsetDateTime);

impl Expiry {
    pub fn parse(s: &str) -> Result<Self, String> {
        OffsetDateTime::parse(s.trim(), &Rfc3339)
            .map(|t| Self(t.to_offset(time::UtcOffset::UTC)))
            .map_err(|e| {
                format!(
                    "{s:?}: expected an RFC 3339 date and time, such as 2026-12-31T00:00:00Z ({e})"
                )
            })
    }

    /// From that instant on, not only after it, so `expires` names the first moment the pair
    /// is refused, as an operator reading "expires at midnight" expects.
    pub fn passed(self, now: SystemTime) -> bool {
        OffsetDateTime::from(now) >= self.0
    }
}

impl fmt::Display for Expiry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0.format(&Rfc3339).map_err(|_| fmt::Error)?)
    }
}

impl<'de> Deserialize<'de> for Expiry {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl serde::de::Visitor<'_> for Visitor {
            type Value = Expiry;
            // TOML's unquoted date-times reach here as strings too, and parse the same way.
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an RFC 3339 date and time, such as \"2026-12-31T00:00:00Z\"")
            }
            fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<Expiry, E> {
                Expiry::parse(s).map_err(E::custom)
            }
        }
        d.deserialize_str(Visitor)
    }
}

/// A bucket, or a folder in one: `photos`, or `photos/2024/`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    pub bucket: String,
    /// Empty for the whole bucket; otherwise ends in `/`.
    pub prefix: String,
}

impl Scope {
    pub fn parse(s: &str) -> Result<Self, String> {
        let s = s.trim();
        let (bucket, rest) = s.split_once('/').unwrap_or((s, ""));
        if bucket.is_empty() {
            return Err(format!("{s:?}: expected a bucket, or bucket/folder/"));
        }
        // Always a folder, so `photos/2024` does not also reach `photos/2024-old/`.
        let prefix = if rest.is_empty() || rest.ends_with('/') {
            rest.to_owned()
        } else {
            format!("{rest}/")
        };
        Ok(Self {
            bucket: bucket.to_owned(),
            prefix,
        })
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.prefix.is_empty() {
            f.write_str(&self.bucket)
        } else {
            write!(f, "{}/{}", self.bucket, self.prefix)
        }
    }
}

/// The limits of a key pair that has any. Such a pair never manages buckets or the settings:
/// those reach past any one bucket or folder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limit {
    /// `None` for every bucket, as a read-only pair without `allow` has.
    scopes: Option<Vec<Scope>>,
    access: Access,
}

impl Limit {
    /// `None` for a pair without limits.
    pub fn of(allow: Option<&[String]>, access: Access) -> Result<Option<Self>, String> {
        if allow.is_none() && access == Access::ReadWrite {
            return Ok(None);
        }
        let scopes = match allow {
            None => None,
            // Reaching nothing is never what was meant; more likely a list left half-edited.
            Some([]) => return Err("`allow` is empty; leave it out to allow every bucket".into()),
            Some(list) => Some(
                list.iter()
                    .map(|s| Scope::parse(s))
                    .collect::<Result<_, _>>()?,
            ),
        };
        Ok(Some(Self { scopes, access }))
    }

    pub fn may_write(&self) -> bool {
        self.access != Access::ReadOnly
    }

    pub fn may_read(&self) -> bool {
        self.access != Access::WriteOnly
    }

    /// Whether the pair's access allows the operation at all, by s3s's name for it; where the
    /// operation acts is checked apart, against the scopes.
    pub fn permits(&self, op: &str) -> bool {
        match self.access {
            Access::ReadWrite => true,
            Access::ReadOnly => reads(op),
            Access::WriteOnly => DROPS.contains(&op),
        }
    }

    /// Whether the pair may read the object, as a copy's source must be: a copy hands the
    /// source's content to a key the pair chose, which is a read of it by other means.
    pub fn reads_key(&self, bucket: &str, key: &str) -> bool {
        self.may_read() && self.reaches_key(bucket, key)
    }

    pub fn scopes(&self) -> Option<&[Scope]> {
        self.scopes.as_deref()
    }

    fn any(&self, f: impl Fn(&Scope) -> bool) -> bool {
        self.scopes.as_ref().is_none_or(|s| s.iter().any(f))
    }

    /// Whether the bucket is listed and its name may be seen, for a scope anywhere in it.
    pub fn reaches_bucket(&self, bucket: &str) -> bool {
        self.any(|s| s.bucket == bucket)
    }

    pub fn reaches_key(&self, bucket: &str, key: &str) -> bool {
        self.any(|s| s.bucket == bucket && key.starts_with(&s.prefix))
    }

    /// A listing shows keys under its prefix, and the names of folders there, so the whole
    /// prefix must lie inside a scope: listing `photos/` for a pair limited to `photos/2024/`
    /// would show the other years' folders.
    pub fn reaches_listing(&self, bucket: &str, prefix: &str) -> bool {
        self.any(|s| s.bucket == bucket && prefix.starts_with(&s.prefix))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limit(allow: &[&str], access: Access) -> Limit {
        let allow: Vec<String> = allow.iter().map(|s| (*s).to_owned()).collect();
        Limit::of(Some(&allow), access).unwrap().unwrap()
    }

    #[test]
    fn a_pair_without_limits_has_none() {
        // Pairs configured before limits existed must keep reaching everything.
        assert_eq!(Limit::of(None, Access::ReadWrite), Ok(None));
        let read_only = Limit::of(None, Access::ReadOnly).unwrap().unwrap();
        assert!(read_only.reaches_key("any", "thing") && !read_only.may_write());
    }

    #[test]
    fn a_folder_scope_reaches_its_keys_only() {
        let l = limit(&["photos/2024"], Access::ReadWrite);
        assert!(l.reaches_key("photos", "2024/a.jpg"));
        assert!(l.reaches_key("photos", "2024/"));
        // Not the sibling that merely shares the name's start, nor another bucket.
        assert!(!l.reaches_key("photos", "2024-old/a.jpg"));
        assert!(!l.reaches_key("photos", "2023/a.jpg"));
        assert!(!l.reaches_key("videos", "2024/a.jpg"));
        assert!(l.reaches_bucket("photos") && !l.reaches_bucket("videos"));
    }

    #[test]
    fn listings_must_stay_inside_a_scope() {
        // A listing of the parent would reveal the names of folders the pair cannot open.
        let l = limit(&["photos/2024/", "logs"], Access::ReadOnly);
        assert!(l.reaches_listing("photos", "2024/"));
        assert!(l.reaches_listing("photos", "2024/may/"));
        assert!(!l.reaches_listing("photos", ""));
        assert!(!l.reaches_listing("photos", "20"));
        assert!(l.reaches_listing("logs", ""));
    }

    #[test]
    fn an_empty_allow_list_is_refused() {
        assert!(Limit::of(Some(&[]), Access::ReadWrite).is_err());
        assert!(Limit::of(Some(&["/x".to_owned()]), Access::ReadWrite).is_err());
    }

    /// Every operation that reads an object, a listing or a setting, which a drop box must not
    /// answer: the pair that drops files must learn nothing about what else, or what, is there.
    const READS: [&str; 23] = [
        "GetObject",
        "HeadObject",
        "HeadBucket",
        "ListBuckets",
        "ListObjects",
        "ListObjectsV2",
        "ListObjectVersions",
        "ListMultipartUploads",
        "GetBucketLocation",
        "GetBucketVersioning",
        "GetBucketAcl",
        "GetBucketCors",
        "GetBucketTagging",
        "GetBucketPolicy",
        "GetBucketLifecycleConfiguration",
        "GetBucketNotificationConfiguration",
        "GetObjectAcl",
        "GetObjectAttributes",
        "GetObjectTagging",
        "GetObjectRetention",
        "GetObjectLegalHold",
        "GetObjectTorrent",
        "SelectObjectContent",
    ];

    /// Every operation that removes an object, a version, a tag or a bucket, or moves an object
    /// away: a drop box keeps what was dropped.
    const DELETES: [&str; 5] = [
        "DeleteObject",
        "DeleteObjects",
        "DeleteObjectTagging",
        "DeleteBucket",
        "RenameObject",
    ];

    #[test]
    fn a_write_only_pair_puts_objects_and_finishes_its_uploads() {
        // A drop box that could not finish or resume a multipart upload would take small files
        // only, and one that could not abort would leave the parts of a failed upload behind.
        let l = limit(&["inbox"], Access::WriteOnly);
        for op in [
            "PutObject",
            "PostObject",
            "CopyObject",
            "CreateMultipartUpload",
            "UploadPart",
            "CompleteMultipartUpload",
            "AbortMultipartUpload",
            "ListParts",
        ] {
            assert!(l.permits(op), "{op}");
        }
        assert!(l.may_write() && !l.may_read());
    }

    #[test]
    fn a_write_only_pair_refuses_every_read() {
        let l = limit(&["inbox"], Access::WriteOnly);
        for op in READS {
            assert!(!l.permits(op), "{op}");
        }
    }

    #[test]
    fn a_write_only_pair_refuses_every_delete() {
        // Decided: no deletes, so whatever was dropped stays for whoever empties the box.
        let l = limit(&["inbox"], Access::WriteOnly);
        for op in DELETES {
            assert!(!l.permits(op), "{op}");
        }
        // Nor anything that changes an object already there other than by replacing it, or
        // manages the bucket.
        for op in [
            "PutObjectTagging",
            "PutObjectAcl",
            "PutObjectRetention",
            "PutBucketVersioning",
            "PutBucketAcl",
            "CreateBucket",
            "RestoreObject",
        ] {
            assert!(!l.permits(op), "{op}");
        }
    }

    #[test]
    fn a_write_only_pair_without_allow_is_still_limited() {
        // Leaving out `allow` means every bucket, not every operation: dropping the limit here
        // would turn a drop box into a full-access pair.
        let l = Limit::of(None, Access::WriteOnly).unwrap().unwrap();
        assert!(l.reaches_key("any", "thing"));
        assert!(!l.permits("GetObject") && !l.permits("DeleteObject"));
    }

    #[test]
    fn a_copy_source_must_be_readable_by_the_pair() {
        // A copy reads its source into a key the pair names, so a pair that may not read the
        // source must not copy it, inside its own scope or not.
        let writer = limit(&["inbox"], Access::WriteOnly);
        assert!(!writer.reads_key("inbox", "dropped.txt"));
        assert!(!writer.reads_key("other", "secret.txt"));
        let reader = limit(&["inbox"], Access::ReadOnly);
        assert!(reader.reads_key("inbox", "dropped.txt"));
        assert!(!reader.reads_key("other", "secret.txt"));
        let both = limit(&["inbox"], Access::ReadWrite);
        assert!(both.reads_key("inbox", "dropped.txt"));
    }

    #[test]
    fn read_only_and_read_write_keep_their_operations() {
        let reader = limit(&["inbox"], Access::ReadOnly);
        for op in READS {
            assert!(reader.permits(op), "{op}");
        }
        assert!(!reader.permits("PutObject") && !reader.permits("DeleteObject"));
        let both = limit(&["inbox"], Access::ReadWrite);
        assert!(both.permits("GetObject") && both.permits("DeleteObject"));
    }

    fn at(s: &str) -> SystemTime {
        OffsetDateTime::parse(s, &Rfc3339).unwrap().into()
    }

    #[test]
    fn a_pair_expires_at_the_instant_named() {
        // The pair must stop at the instant written, not a second later: "expires at midnight"
        // means a request at midnight is refused.
        let e = Expiry::parse("2026-12-31T00:00:00Z").unwrap();
        assert!(!e.passed(at("2026-12-30T23:59:59.999999999Z")));
        assert!(e.passed(at("2026-12-31T00:00:00Z")));
        assert!(e.passed(at("2027-01-01T00:00:00Z")));
    }

    #[test]
    fn an_expiry_with_an_offset_is_the_same_instant_in_utc() {
        // Written in local time, it must expire at that local time, and print back the instant
        // it names rather than the wall-clock digits with a Z appended.
        let e = Expiry::parse("2026-12-31T08:00:00+08:00").unwrap();
        assert_eq!(e.to_string(), "2026-12-31T00:00:00Z");
        assert!(e.passed(at("2026-12-31T00:00:00Z")));
        assert!(!e.passed(at("2026-12-30T23:59:59Z")));
        // The console's date field sends milliseconds.
        assert_eq!(
            Expiry::parse("2026-12-31T00:00:00.000Z").unwrap(),
            Expiry::parse("2026-12-31T00:00:00Z").unwrap()
        );
    }

    #[test]
    fn an_expiry_without_a_time_or_offset_is_refused() {
        // A date alone, or a time without an offset, names no single instant; guessing the
        // server's time zone would end the pair hours early or late.
        assert!(Expiry::parse("2026-12-31").is_err());
        assert!(Expiry::parse("2026-12-31T00:00:00").is_err());
    }
}

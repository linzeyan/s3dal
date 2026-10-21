//! Mapping between S3 names and backend paths, and the validation that keeps every request
//! inside its bucket.

use s3s::{S3Result, s3_error};

use super::S3Dal;
use crate::error::{Ctx, map};

/// S3 bucket naming rules, which also guarantee that a bucket can never address the reserved
/// `.s3dal/` root or escape the namespace. Builds on the check s3s routing applies, so a
/// bucket ListBuckets shows is always one that requests can reach. s3s lets a dot-separated
/// label start or end with `-` (`a-.b`), which S3 refuses: such a name is no DNS name, so it
/// cannot be addressed virtual-hosted style.
pub(crate) fn validate_bucket_name(name: &str) -> S3Result<()> {
    let dns_labels = name
        .split('.')
        .all(|label| !label.starts_with('-') && !label.ends_with('-'));
    if s3s::path::check_bucket_name(name) && dns_labels {
        Ok(())
    } else {
        Err(s3_error!(
            InvalidBucketName,
            "The specified bucket is not valid."
        ))
    }
}

/// Keys that backends cannot store faithfully are rejected rather than normalized, because
/// OpenDAL would silently map `a//b` and `a/./b` onto `a/b`, and `..` could escape the bucket.
pub(crate) fn validate_key(key: &str) -> S3Result<()> {
    if key.is_empty() {
        return Err(s3_error!(InvalidArgument, "object key must not be empty"));
    }
    if key.len() > 1024 {
        return Err(s3_error!(KeyTooLongError));
    }
    // s3s answers 500 to a response that would carry one: such a key would break every
    // listing of its bucket for clients that do not ask for URL encoding.
    if !xml_safe(key) {
        return Err(s3_error!(
            InvalidArgument,
            "object keys with control characters other than tab, line feed and carriage return are not supported"
        ));
    }
    // OpenDAL trims whitespace off both ends of every path, so `k ` would read, overwrite and
    // delete `k`. Leading whitespace is safe: the bucket comes first in the path.
    if key.ends_with(char::is_whitespace) {
        return Err(s3_error!(
            InvalidArgument,
            "object keys ending in whitespace are not supported"
        ));
    }
    let segments: Vec<&str> = key.split('/').collect();
    let last = segments.len() - 1;
    for (i, seg) in segments.iter().enumerate() {
        // A trailing `/` (folder marker) leaves one empty final segment, which is allowed.
        let trailing_slash = i == last && i > 0;
        if (seg.is_empty() && !trailing_slash) || *seg == "." || *seg == ".." {
            return Err(s3_error!(
                InvalidArgument,
                "object keys with a leading '/', empty segments, or '.'/'..' segments are not supported"
            ));
        }
    }
    Ok(())
}

/// Whether XML 1.0 can carry `s`: no control character but tab, line feed and carriage return.
pub(crate) fn xml_safe(s: &str) -> bool {
    xml_safe_prefix(s).len() == s.len()
}

/// The longest start of `s` that XML 1.0 can carry: `s` up to its first control character
/// other than tab, line feed and carriage return.
pub(crate) fn xml_safe_prefix(s: &str) -> &str {
    // Such a character is ASCII, so it starts a character.
    let end = s
        .bytes()
        .position(|b| b < b' ' && !matches!(b, b'\t' | b'\n' | b'\r'))
        .unwrap_or(s.len());
    &s[..end]
}

/// Listings echo their parameters, so one that XML cannot carry would make s3s answer 500;
/// URL-encoded, it is fine.
pub(crate) fn check_listing_params<'a>(
    url_encoded: bool,
    params: impl IntoIterator<Item = Option<&'a str>>,
) -> S3Result<()> {
    if url_encoded || params.into_iter().flatten().all(xml_safe) {
        return Ok(());
    }
    Err(s3_error!(
        InvalidArgument,
        "listing parameters with control characters require encoding-type=url"
    ))
}

pub(crate) fn bucket_dir(bucket: &str) -> String {
    format!("{bucket}/")
}

pub(crate) fn object_path(bucket: &str, key: &str) -> String {
    format!("{bucket}/{key}")
}

/// s3dal has no versioning, so every object has exactly one version, `null`, which is what S3
/// reports for objects in an unversioned bucket. Any other id names a version that does not
/// exist, which S3 answers with `NoSuchVersion`; serving or deleting the current object instead
/// would act on data the client did not name. (S3 answers `InvalidArgument` instead when it
/// cannot parse the id, but that depends on S3's private id format.)
pub(crate) fn require_null_version(version_id: Option<&str>) -> S3Result<()> {
    match version_id {
        None | Some("null") => Ok(()),
        Some(_) => Err(s3_error!(
            NoSuchVersion,
            "The specified version does not exist."
        )),
    }
}

/// Every object operation answers `NoSuchBucket` before touching the key. A name that cannot
/// be a bucket cannot exist, so it is `NoSuchBucket` too, never a backend lookup.
pub(crate) async fn require_bucket(dal: &S3Dal, bucket: &str) -> S3Result<()> {
    if validate_bucket_name(bucket).is_err() {
        return Err(s3_error!(NoSuchBucket));
    }
    match dal.op.stat(&bucket_dir(bucket)).await {
        Ok(m) if m.is_dir() => Ok(()),
        Ok(_) => Err(s3_error!(NoSuchBucket)),
        Err(err) => Err(map(err, Ctx::Bucket)),
    }
}

/// `lookup` under [`require_bucket`]'s rule, with the bucket checked only when needed. Where
/// directories are real, a key's file lies inside its bucket's directory, so finding it proves
/// the bucket; only a failure needs the bucket checked, and the bucket's error then wins, as
/// checking it first would have made it. GET and HEAD save a backend call each that way: on fs
/// a blocking-pool round trip, a third of a small GET's.
pub(crate) async fn with_bucket<T>(
    dal: &S3Dal,
    bucket: &str,
    lookup: impl Future<Output = S3Result<T>>,
) -> S3Result<T> {
    if !dal.profile.real_dirs {
        require_bucket(dal, bucket).await?;
        return lookup.await;
    }
    // The name is checked before `lookup` builds a path from it.
    if validate_bucket_name(bucket).is_err() {
        return Err(s3_error!(NoSuchBucket));
    }
    match lookup.await {
        Ok(found) => Ok(found),
        Err(err) => {
            require_bucket(dal, bucket).await?;
            Err(err)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_names_follow_s3_rules() {
        for ok in ["abc", "my-bucket.v2", "0bucket", &"a".repeat(63)] {
            assert!(validate_bucket_name(ok).is_ok(), "{ok}");
        }
        for bad in [
            "ab",
            "Abc",
            "-abc",
            "abc-",
            "a..b",
            "a-.b",
            "a.-b",
            "a_b",
            "192.168.0.1",
            // s3s routing refuses it, so a directory with this name would be listed yet
            // unreachable.
            "xn--example",
            &"a".repeat(64),
        ] {
            assert!(validate_bucket_name(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn reserved_root_is_never_a_bucket() {
        // `.s3dal/` holds uploads, probes and temp files; a client must not reach it.
        assert!(validate_bucket_name(".s3dal").is_err());
    }

    #[test]
    fn only_the_null_version_exists() {
        assert!(require_null_version(None).is_ok());
        assert!(require_null_version(Some("null")).is_ok());
        assert_eq!(
            *require_null_version(Some("3HL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY"))
                .unwrap_err()
                .code(),
            s3s::S3ErrorCode::NoSuchVersion
        );
    }

    #[test]
    fn keys_that_would_be_normalized_or_escape_are_rejected() {
        for bad in [
            "",
            "/a",
            "a//b",
            "a/./b",
            "../x",
            "a/..",
            "x\0y",
            "ctl\u{1}char",
            "x\u{1f}",
            " ",
            "ws ",
            "a/ ",
            "tab\t",
            "nl\n",
            "nbsp\u{a0}",
            "ideographic\u{3000}",
        ] {
            assert!(validate_key(bad).is_err(), "{bad:?}");
        }
        assert!(validate_key(&"k".repeat(1025)).is_err());
    }

    #[test]
    fn ordinary_keys_and_folder_markers_are_accepted() {
        for ok in [
            "a",
            "a/b/c.txt",
            "dir/",
            "a.b",
            "..a",
            "a..",
            "ünï/cødé",
            " leading",
            "a /b",
            "in\tside",
            "line\nbreak",
            "a b",
            &"k".repeat(1024),
        ] {
            assert!(validate_key(ok).is_ok(), "{ok:?}");
        }
    }
}

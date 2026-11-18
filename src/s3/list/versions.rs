//! ListObjectVersions of a bucket that has or had versioning (versioning.md
//! "ListObjectVersions"): the bucket's current objects and the versions tree, each walked in key
//! order, merged key by key. Within a key the current version comes first, then its archived
//! versions and markers in name order, which is newest first. Delimiter folding applies to the
//! merged keys, so a CommonPrefix holding only old versions or markers is listed. `max-keys`
//! counts versions, markers and CommonPrefixes, as AWS does.
//!
//! A bucket that never had versioning keeps the single-source listing in `list_object_versions`.
//! This walk reads through CommonPrefix groups rather than seeking past them; versioned buckets
//! pay that, unversioned ones do not.

use futures::{StreamExt, TryStreamExt};
use opendal::{Metadata, Operator};
use s3s::dto::*;
use s3s::{S3Response, S3Result, s3_error};

use super::source::{prefix_dir, sorted_children};
use super::{MAX_KEYS, STAT_CONCURRENCY, echoed_delimiter, non_empty, url_encode};
use crate::error::{Ctx, map};
use crate::meta;
use crate::s3::version::{self, Entry, NULL};
use crate::s3::{S3Dal, acl, naming};

/// The files below `root` whose paths, relative to it, start with `prefix` and are not below
/// `lower`, in byte order. Each directory is sorted with its subdirectories named `name/`, which
/// gives the global byte order of the paths, as the bucket walk relies on.
struct Walk<'a> {
    op: &'a Operator,
    root: String,
    prefix: String,
    lower: String,
    stack: Vec<std::vec::IntoIter<(String, Metadata)>>,
}

impl<'a> Walk<'a> {
    async fn new(
        op: &'a Operator,
        root: String,
        start: &str,
        prefix: String,
        lower: String,
    ) -> S3Result<Self> {
        let first = sorted_children(op, &root, start).await?;
        Ok(Self {
            op,
            root,
            prefix,
            lower,
            stack: vec![first.into_iter()],
        })
    }

    async fn next(&mut self) -> S3Result<Option<String>> {
        while let Some(children) = self.stack.last_mut() {
            let Some((path, meta)) = children.next() else {
                self.stack.pop();
                continue;
            };
            if meta.is_dir() {
                if self.may_hold(&path) {
                    let below = sorted_children(self.op, &self.root, &path).await?;
                    self.stack.push(below.into_iter());
                }
            } else if path.starts_with(&self.prefix) && path >= self.lower {
                return Ok(Some(path));
            }
        }
        Ok(None)
    }

    /// Whether a path below `dir` can start with the prefix and lie at or after `lower`.
    fn may_hold(&self, dir: &str) -> bool {
        let prefixed = dir.starts_with(&self.prefix) || self.prefix.starts_with(dir);
        // Every path below `dir` starts with it, so if `lower` is greater and does not, it is
        // greater than all of them.
        let before = self.lower.as_str() > dir && !self.lower.starts_with(dir);
        prefixed && !before
    }
}

/// The next current object: its key.
async fn next_current(walk: &mut Walk<'_>) -> S3Result<Option<String>> {
    while let Some(key) = walk.next().await? {
        // As the bucket listing does: a name another tool stored that s3dal refuses as a key is
        // left out rather than read as another key.
        if naming::validate_key(&key).is_ok() {
            return Ok(Some(key));
        }
    }
    Ok(None)
}

/// The next archived entry: its key and the entry.
async fn next_archived(walk: &mut Walk<'_>) -> S3Result<Option<(String, Entry)>> {
    while let Some(rel) = walk.next().await? {
        // `!!` never occurs inside an escaped key, so the first one ends it.
        let Some(end) = rel.find("!!/") else {
            continue;
        };
        let (escaped, name) = (&rel[..end], &rel[end + 3..]);
        if name.contains('/') {
            continue;
        }
        let holder = format!("{}{}", walk.root, &rel[..end + 3]);
        if let (Some(key), Some(entry)) = (version::unescape(escaped), Entry::parse(&holder, name))
        {
            return Ok(Some((key, entry)));
        }
    }
    Ok(None)
}

/// One line of the page, before its stat.
enum Line {
    Version {
        key: String,
        path: String,
        id: String,
        latest: bool,
        /// The current object, already stat'ed for its id.
        stat: Option<Metadata>,
    },
    Marker {
        key: String,
        path: String,
        id: String,
        latest: bool,
    },
    Prefix(String),
}

impl Line {
    /// The key and version id a client resumes after.
    fn marker(&self) -> (String, String) {
        match self {
            Self::Version { key, id, .. } | Self::Marker { key, id, .. } => {
                (key.clone(), id.clone())
            }
            // As the unversioned listing answers after a CommonPrefix: `null` resumes past the
            // group, as the key marker alone does.
            Self::Prefix(prefix) => (prefix.clone(), NULL.to_owned()),
        }
    }
}

pub(super) async fn list_versioned(
    dal: &S3Dal,
    input: ListObjectVersionsInput,
) -> S3Result<S3Response<ListObjectVersionsOutput>> {
    let delimiter = input
        .delimiter
        .as_deref()
        .filter(|d| !d.is_empty())
        .map(str::to_owned);
    let max_keys = match input.max_keys {
        None => MAX_KEYS,
        Some(n) => usize::try_from(n)
            .map_err(|_| s3_error!(InvalidArgument, "max-keys must not be negative"))?
            .min(MAX_KEYS),
    };
    let url_encoded = match input.encoding_type.as_ref() {
        None => false,
        Some(t) if t.as_str().eq_ignore_ascii_case(EncodingType::URL) => true,
        Some(_) => {
            return Err(s3_error!(
                InvalidArgument,
                "Invalid Encoding Method specified in Request"
            ));
        }
    };
    let prefix = input.prefix.clone().unwrap_or_default();
    let key_marker = input.key_marker.clone().unwrap_or_default();
    let version_marker = input.version_id_marker.clone().filter(|m| !m.is_empty());
    if let Some(m) = &version_marker {
        if key_marker.is_empty() {
            return Err(s3_error!(
                InvalidArgument,
                "A version-id marker cannot be specified without a key marker."
            ));
        }
        version::check_id(m)?;
    }
    naming::check_listing_params(
        url_encoded,
        [
            Some(prefix.as_str()),
            delimiter.as_deref(),
            Some(key_marker.as_str()),
        ],
    )?;

    let mut lines = Vec::new();
    let mut truncated = false;
    if max_keys > 0
        && let Some(start) = prefix_dir(&prefix)
    {
        truncated = collect(
            dal,
            &input.bucket,
            Query {
                prefix: &prefix,
                start,
                delimiter: delimiter.as_deref(),
                key_marker: &key_marker,
                version_marker: version_marker.as_deref(),
                max_keys,
            },
            &mut lines,
        )
        .await?;
    }
    let next = truncated.then(|| lines.last().map(Line::marker)).flatten();

    let encode = |s: String| if url_encoded { url_encode(&s) } else { s };
    let stated: Vec<Option<Stated>> = futures::stream::iter(lines)
        .map(|line| stat(dal, line))
        .buffered(STAT_CONCURRENCY)
        .try_collect()
        .await?;
    let mut versions = Vec::new();
    let mut delete_markers = Vec::new();
    let mut common_prefixes = Vec::new();
    for line in stated.into_iter().flatten() {
        match line {
            Stated::Version(v) => versions.push(ObjectVersion {
                key: v.key.map(encode),
                ..v
            }),
            Stated::Marker(m) => delete_markers.push(DeleteMarkerEntry {
                key: m.key.map(encode),
                ..m
            }),
            Stated::Prefix(p) => common_prefixes.push(CommonPrefix {
                prefix: Some(encode(p)),
            }),
        }
    }
    let (next_key_marker, next_version_id_marker) = match next {
        Some((key, id)) => (Some(encode(key)), Some(id)),
        None => (None, None),
    };
    Ok(S3Response::new(ListObjectVersionsOutput {
        name: Some(input.bucket),
        prefix: Some(encode(prefix)),
        key_marker: Some(encode(key_marker)),
        version_id_marker: Some(input.version_id_marker.unwrap_or_default()),
        delimiter: echoed_delimiter(input.delimiter).map(encode),
        encoding_type: input.encoding_type,
        max_keys: Some(i32::try_from(max_keys).unwrap_or(i32::MAX)),
        is_truncated: Some(truncated),
        next_key_marker,
        next_version_id_marker,
        versions: non_empty(versions),
        delete_markers: non_empty(delete_markers),
        common_prefixes: non_empty(common_prefixes),
        ..Default::default()
    }))
}

struct Query<'a> {
    prefix: &'a str,
    /// The directory part of the prefix, where both walks start.
    start: &'a str,
    delimiter: Option<&'a str>,
    key_marker: &'a str,
    version_marker: Option<&'a str>,
    max_keys: usize,
}

/// Fills `lines` with up to `max_keys` lines; whether more follow.
async fn collect(dal: &S3Dal, bucket: &str, q: Query<'_>, lines: &mut Vec<Line>) -> S3Result<bool> {
    let mut current = Walk::new(
        &dal.op,
        naming::bucket_dir(bucket),
        q.start,
        q.prefix.to_owned(),
        q.key_marker.to_owned(),
    )
    .await?;
    let mut archived = Walk::new(
        &dal.op,
        version::versions_dir(bucket),
        &version::escape(q.start),
        version::escape(q.prefix),
        version::escape(q.key_marker),
    )
    .await?;
    let mut next_cur = next_current(&mut current).await?;
    let mut next_arc = next_archived(&mut archived).await?;
    let mut last_prefix: Option<String> = None;
    loop {
        let key = match (&next_cur, &next_arc) {
            (None, None) => return Ok(false),
            (Some(c), None) => c.clone(),
            (None, Some((a, _))) => a.clone(),
            (Some(c), Some((a, _))) => c.min(a).clone(),
        };
        let has_current = next_cur.as_ref() == Some(&key);
        if has_current {
            next_cur = next_current(&mut current).await?;
        }
        let mut entries = Vec::new();
        while let Some((k, _)) = &next_arc
            && *k == key
        {
            let (_, entry) = next_arc.take().expect("checked above");
            entries.push(entry);
            next_arc = next_archived(&mut archived).await?;
        }

        if let Some(group) = common_prefix(q.prefix, q.delimiter, &key) {
            // A group the marker lies in or after was listed already.
            let listed = last_prefix.as_deref() == Some(group)
                || (!q.key_marker.is_empty() && group <= q.key_marker);
            if !listed {
                if lines.len() == q.max_keys {
                    return Ok(true);
                }
                lines.push(Line::Prefix(group.to_owned()));
                last_prefix = Some(group.to_owned());
            }
            continue;
        }

        let mut key_lines = key_versions(dal, bucket, &key, has_current, entries).await?;
        if key == q.key_marker {
            // The key marker alone resumes after the key; with a version marker, after that
            // version of it. One that is gone by now resumes after the key.
            let after = q.version_marker.and_then(|m| {
                key_lines.iter().position(
                    |l| matches!(l, Line::Version { id, .. } | Line::Marker { id, .. } if id == m),
                )
            });
            key_lines = match after {
                Some(i) => key_lines.split_off(i + 1),
                None => Vec::new(),
            };
        }
        for line in key_lines {
            if lines.len() == q.max_keys {
                return Ok(true);
            }
            lines.push(line);
        }
    }
}

/// The CommonPrefix `key` folds into: the prefix and the key up to and including the first
/// delimiter after it.
fn common_prefix<'k>(prefix: &str, delimiter: Option<&str>, key: &'k str) -> Option<&'k str> {
    let delimiter = delimiter?;
    let rest = key.get(prefix.len()..)?;
    let at = rest.find(delimiter)?;
    Some(&key[..prefix.len() + at + delimiter.len()])
}

/// A key's lines: the current version, then the archived ones and markers, newest first. An
/// archived copy of the current version, left by a commit that failed after archiving it, is
/// not listed twice.
async fn key_versions(
    dal: &S3Dal,
    bucket: &str,
    key: &str,
    has_current: bool,
    mut entries: Vec<Entry>,
) -> S3Result<Vec<Line>> {
    let mut lines = Vec::new();
    if has_current {
        let path = naming::object_path(bucket, key);
        // Gone or turned into a directory since the walk: listed without it, as the bucket
        // listing leaves such a key out.
        if let Ok(m) = dal.op.stat(&path).await
            && m.is_file()
        {
            let id = meta::resolve(&dal.profile, &m)
                .version_id
                .unwrap_or_else(|| NULL.to_owned());
            entries.retain(|e| e.marker || e.id != id);
            lines.push(Line::Version {
                key: key.to_owned(),
                path,
                id,
                latest: true,
                stat: Some(m),
            });
        }
    }
    // Two instances archiving one version each leave an entry of it, the same content.
    let mut seen = std::collections::HashSet::new();
    entries.retain(|e| seen.insert((e.marker, e.id.clone())));
    let none_current = lines.is_empty();
    for (i, e) in entries.into_iter().enumerate() {
        let latest = none_current && i == 0;
        lines.push(if e.marker {
            Line::Marker {
                key: key.to_owned(),
                path: e.path,
                id: e.id,
                latest,
            }
        } else {
            Line::Version {
                key: key.to_owned(),
                path: e.path,
                id: e.id,
                latest,
                stat: None,
            }
        });
    }
    Ok(lines)
}

enum Stated {
    Version(ObjectVersion),
    Marker(DeleteMarkerEntry),
    Prefix(String),
}

/// A line with what its stat tells: ETag, size and time, as HEAD of that version reports them.
/// An entry removed since the walk is left out.
async fn stat(dal: &S3Dal, line: Line) -> S3Result<Option<Stated>> {
    async fn found(dal: &S3Dal, path: &str) -> S3Result<Option<Metadata>> {
        match dal.op.stat(path).await {
            Ok(m) => Ok(Some(m)),
            Err(err) if err.kind() == opendal::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(map(err, Ctx::Other)),
        }
    }
    Ok(match line {
        Line::Prefix(p) => Some(Stated::Prefix(p)),
        Line::Marker {
            key,
            path,
            id,
            latest,
        } => found(dal, &path).await?.map(|m| {
            Stated::Marker(DeleteMarkerEntry {
                key: Some(key),
                version_id: Some(id),
                is_latest: Some(latest),
                last_modified: m
                    .last_modified()
                    .map(|t| Timestamp::from(std::time::SystemTime::from(t))),
                owner: Some(acl::owner()),
            })
        }),
        Line::Version {
            key,
            path,
            id,
            latest,
            stat,
        } => {
            let m = match stat {
                Some(m) => Some(m),
                None => found(dal, &path).await?,
            };
            m.map(|m| {
                let obj = meta::resolve(&dal.profile, &m);
                let storage_class = dal
                    .storage_class(&obj)
                    .unwrap_or(ObjectVersionStorageClass::STANDARD)
                    .to_owned();
                Stated::Version(ObjectVersion {
                    key: Some(key),
                    version_id: Some(id),
                    is_latest: Some(latest),
                    e_tag: Some(ETag::Strong(obj.etag)),
                    size: Some(i64::try_from(obj.content_length).unwrap_or(i64::MAX)),
                    last_modified: obj.last_modified.map(Timestamp::from),
                    owner: Some(acl::owner()),
                    storage_class: Some(ObjectVersionStorageClass::from(storage_class)),
                    ..Default::default()
                })
            })
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_fold_at_the_first_delimiter_after_the_prefix() {
        assert_eq!(common_prefix("", Some("/"), "a/b/c"), Some("a/"));
        assert_eq!(common_prefix("a/", Some("/"), "a/b/c"), Some("a/b/"));
        assert_eq!(common_prefix("a/", Some("/"), "a/b"), None);
        assert_eq!(common_prefix("", None, "a/b"), None);
        assert_eq!(common_prefix("", Some("b"), "ab"), Some("ab"));
    }
}

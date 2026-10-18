//! ListObjectsV2, ListObjects (v1) and ListObjectVersions.
//!
//! [`page`] decides what a page holds from keys in key order; [`source`] produces those keys
//! from the backend without reading more than one page needs. This file validates the request
//! and turns the page into each response. The two versions differ only in how a client
//! resumes: v2 with an opaque token, v1 with the raw last name. ListObjectVersions resumes like
//! v1, from its key marker, and reports each object as its one version.

mod page;
mod source;

use std::fmt::Write as _;

use futures::{StreamExt, TryStreamExt};
use opendal::Metadata;
use s3s::dto::*;
use s3s::{S3Request, S3Response, S3Result, s3_error};

use super::{S3Dal, acl, naming};
use crate::backend::Profile;
use crate::error::{Ctx, map};
use crate::meta;
use page::{Item, Query};

/// AWS's default and its cap: asking for more still returns at most this many entries.
const MAX_KEYS: usize = 1000;

/// Stats in flight for one page. A page holds up to 1000 objects, and stating them one by one
/// would make a full page cost 1000 backend round trips in sequence.
const STAT_CONCURRENCY: usize = 16;

pub(super) async fn list_objects_v2(
    dal: &S3Dal,
    req: S3Request<ListObjectsV2Input>,
) -> S3Result<S3Response<ListObjectsV2Output>> {
    let input = req.input;
    naming::require_bucket(dal, &input.bucket).await?;
    // AWS lists from the start for an empty token, as if none was sent.
    let token = input
        .continuation_token
        .as_deref()
        .filter(|t| !t.is_empty())
        .map(decode_token)
        .transpose()?;

    let listing = list(
        dal,
        Params {
            bucket: &input.bucket,
            prefix: input.prefix.as_deref(),
            delimiter: input.delimiter.as_deref(),
            max_keys: input.max_keys,
            encoding_type: input.encoding_type.as_ref(),
            // A token always lies after the start-after it was issued under. Taking the later
            // of the two also keeps a client that changes start-after mid-listing from seeing
            // keys again.
            start_after: token.into_iter().chain(input.start_after.clone()).max(),
            echoed: input.start_after.as_deref(),
        },
    )
    .await?;

    // As on AWS, v2 names the owner only when asked; v1 always does.
    let fetch_owner = input.fetch_owner == Some(true);
    let next_continuation_token = listing.next_marker.as_deref().map(encode_token);
    let delimiter = echoed_delimiter(input.delimiter).map(|d| listing.encode(d));
    let start_after = input.start_after.map(|s| listing.encode(s));
    let prefix = listing.encode(listing.prefix.clone());
    let key_count = listing.contents.len() + listing.common_prefixes.len();
    let output = ListObjectsV2Output {
        name: Some(input.bucket),
        prefix: Some(prefix),
        delimiter,
        start_after,
        encoding_type: input.encoding_type,
        max_keys: Some(listing.max_keys),
        key_count: Some(i32::try_from(key_count).unwrap_or(i32::MAX)),
        is_truncated: Some(next_continuation_token.is_some()),
        continuation_token: input.continuation_token,
        next_continuation_token,
        contents: non_empty(if fetch_owner {
            with_owner(listing.contents)
        } else {
            listing.contents
        }),
        common_prefixes: non_empty(listing.common_prefixes),
        ..Default::default()
    };
    Ok(S3Response::new(output))
}

/// rclone (with `provider = Other`) and many older SDK paths still list with v1, so leaving it
/// unimplemented would break them with their default settings.
pub(super) async fn list_objects(
    dal: &S3Dal,
    req: S3Request<ListObjectsInput>,
) -> S3Result<S3Response<ListObjectsOutput>> {
    let input = req.input;
    naming::require_bucket(dal, &input.bucket).await?;

    let listing = list(
        dal,
        Params {
            bucket: &input.bucket,
            prefix: input.prefix.as_deref(),
            delimiter: input.delimiter.as_deref(),
            max_keys: input.max_keys,
            encoding_type: input.encoding_type.as_ref(),
            start_after: input.marker.clone().filter(|m| !m.is_empty()),
            echoed: None,
        },
    )
    .await?;

    // A v1 client sends NextMarker back as `marker`, a plain name, so it must be the raw last
    // name and never a v2 token. AWS returns it only with a delimiter; without one, clients
    // resume from the last Key, which is the same name.
    let next_marker = listing
        .next_marker
        .clone()
        .filter(|_| listing.delimited)
        .map(|m| listing.encode(m));
    let delimiter = echoed_delimiter(input.delimiter).map(|d| listing.encode(d));
    let marker = listing.encode(input.marker.unwrap_or_default());
    let output = ListObjectsOutput {
        name: Some(input.bucket),
        // Raw even with encoding-type=url, as on AWS. botocore asks for url encoding on every v1
        // listing and decodes every field but this one, so an encoded Prefix would reach
        // applications still encoded.
        prefix: Some(listing.prefix),
        marker: Some(marker),
        delimiter,
        encoding_type: input.encoding_type,
        max_keys: Some(listing.max_keys),
        is_truncated: Some(listing.next_marker.is_some()),
        next_marker,
        contents: non_empty(with_owner(listing.contents)),
        common_prefixes: non_empty(listing.common_prefixes),
        ..Default::default()
    };
    Ok(S3Response::new(output))
}

/// The id S3 gives the one version of an object in an unversioned bucket, which is what every
/// object here is.
const NULL_VERSION: &str = "null";

/// s3dal keeps no versions, so every object is listed as its one version, `null`, the latest,
/// and there are no delete markers. Clients ask anyway: ceph s3-tests empty every bucket with
/// it, `rclone --s3-versions` and `aws s3api list-object-versions` list with it, and backup
/// tools probe it.
pub(super) async fn list_object_versions(
    dal: &S3Dal,
    req: S3Request<ListObjectVersionsInput>,
) -> S3Result<S3Response<ListObjectVersionsOutput>> {
    let input = req.input;
    naming::require_bucket(dal, &input.bucket).await?;
    let key_marker = input.key_marker.clone().filter(|m| !m.is_empty());
    match input.version_id_marker.as_deref().filter(|m| !m.is_empty()) {
        None => {}
        // As on AWS: a version id marker names a version of the key marker's key.
        Some(_) if key_marker.is_none() => {
            return Err(s3_error!(
                InvalidArgument,
                "A version-id marker cannot be specified without a key marker."
            ));
        }
        // The key's only version, so the listing goes on after the key, as for the key marker
        // alone.
        Some(NULL_VERSION) => {}
        // No other version exists, so s3dal never issued this marker; resuming after the key
        // anyway would guess where the client meant to be. AWS answers a version id it cannot
        // parse this way.
        Some(_) => {
            return Err(s3_error!(InvalidArgument, "Invalid version id specified"));
        }
    }

    let listing = list(
        dal,
        Params {
            bucket: &input.bucket,
            prefix: input.prefix.as_deref(),
            delimiter: input.delimiter.as_deref(),
            max_keys: input.max_keys,
            encoding_type: input.encoding_type.as_ref(),
            start_after: key_marker,
            echoed: None,
        },
    )
    .await?;

    // Clients send both back, and s3-tests' cleanup passes NextVersionIdMarker on without
    // checking it is there, so every truncated page carries one. AWS does not document what
    // follows a CommonPrefix; there `null` resumes past the group, as the key marker alone
    // does.
    let next_version_id_marker = listing
        .next_marker
        .as_ref()
        .map(|_| NULL_VERSION.to_owned());
    let next_key_marker = listing.next_marker.clone().map(|m| listing.encode(m));
    let delimiter = echoed_delimiter(input.delimiter).map(|d| listing.encode(d));
    let key_marker = listing.encode(input.key_marker.unwrap_or_default());
    // Encoded with the rest, unlike v1's: AWS lists Prefix among the encoded names here, and
    // botocore decodes it.
    let prefix = listing.encode(listing.prefix.clone());
    let versions = listing
        .contents
        .into_iter()
        .map(|object| ObjectVersion {
            key: object.key,
            version_id: Some(NULL_VERSION.to_owned()),
            is_latest: Some(true),
            e_tag: object.e_tag,
            size: object.size,
            last_modified: object.last_modified,
            owner: Some(acl::owner()),
            storage_class: Some(ObjectVersionStorageClass::from_static(
                ObjectVersionStorageClass::STANDARD,
            )),
            ..Default::default()
        })
        .collect();
    let output = ListObjectVersionsOutput {
        name: Some(input.bucket),
        prefix: Some(prefix),
        key_marker: Some(key_marker),
        version_id_marker: Some(input.version_id_marker.unwrap_or_default()),
        delimiter,
        encoding_type: input.encoding_type,
        max_keys: Some(listing.max_keys),
        is_truncated: Some(next_key_marker.is_some()),
        next_key_marker,
        next_version_id_marker,
        versions: non_empty(versions),
        common_prefixes: non_empty(listing.common_prefixes),
        ..Default::default()
    };
    Ok(S3Response::new(output))
}

/// The request fields every listing shares; only where to resume differs.
struct Params<'a> {
    bucket: &'a str,
    prefix: Option<&'a str>,
    delimiter: Option<&'a str>,
    max_keys: Option<i32>,
    encoding_type: Option<&'a EncodingType>,
    start_after: Option<String>,
    /// Echoed in the response without deciding where the listing resumes: v2's start-after
    /// behind a later continuation token. Unchecked, it made s3s answer 500.
    echoed: Option<&'a str>,
}

/// One page in the response's encoding, except `prefix` and `next_marker`: they stay raw
/// because the two versions report them differently.
struct Listing {
    url_encoded: bool,
    delimited: bool,
    prefix: String,
    max_keys: i32,
    contents: Vec<Object>,
    common_prefixes: Vec<CommonPrefix>,
    next_marker: Option<String>,
}

impl Listing {
    fn encode(&self, s: String) -> String {
        if self.url_encoded { url_encode(&s) } else { s }
    }
}

/// The bucket must already be known to exist.
async fn list(dal: &S3Dal, params: Params<'_>) -> S3Result<Listing> {
    // Any string groups, as on AWS; an empty one is none.
    let delimiter = params
        .delimiter
        .filter(|d| !d.is_empty())
        .map(str::to_owned);
    let max_keys = match params.max_keys {
        None => MAX_KEYS,
        Some(n) => usize::try_from(n)
            .map_err(|_| s3_error!(InvalidArgument, "max-keys must not be negative"))?
            .min(MAX_KEYS),
    };
    let url_encoded = match params.encoding_type {
        None => false,
        Some(t) if t.as_str().eq_ignore_ascii_case(EncodingType::URL) => true,
        Some(_) => {
            return Err(s3_error!(
                InvalidArgument,
                "Invalid Encoding Method specified in Request"
            ));
        }
    };
    naming::check_listing_params(
        url_encoded,
        [
            params.prefix,
            params.delimiter,
            params.start_after.as_deref(),
            params.echoed,
        ],
    )?;

    let query = Query {
        prefix: params.prefix.unwrap_or_default().to_owned(),
        delimiter,
        start_after: params.start_after,
        max_keys,
    };
    let page = source::collect(dal, params.bucket, &query).await?;

    let encode = |s: String| if url_encoded { url_encode(&s) } else { s };
    let mut objects = Vec::new();
    let mut common_prefixes = Vec::new();
    for item in page.items {
        match item {
            Item::Object { key, entry } => objects.push((key, entry)),
            Item::CommonPrefix(prefix) => common_prefixes.push(CommonPrefix {
                prefix: Some(encode(prefix)),
            }),
        }
    }
    let contents: Vec<Object> = futures::stream::iter(objects)
        .map(|(key, entry)| object(dal, params.bucket, key, entry))
        .buffered(STAT_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .flatten()
        .map(|object| Object {
            key: object.key.map(encode),
            ..object
        })
        .collect();

    Ok(Listing {
        url_encoded,
        delimited: query.delimiter.is_some(),
        prefix: query.prefix,
        max_keys: i32::try_from(max_keys).unwrap_or(i32::MAX),
        contents,
        common_prefixes,
        next_marker: page.next_marker,
    })
}

/// An empty delimiter means none, and AWS leaves it out of the response as it does then.
fn echoed_delimiter(delimiter: Option<String>) -> Option<String> {
    delimiter.filter(|d| !d.is_empty())
}

/// The owner GetObjectAcl names, on each entry of a listing that reports it.
fn with_owner(contents: Vec<Object>) -> Vec<Object> {
    contents
        .into_iter()
        .map(|object| Object {
            owner: Some(acl::owner()),
            ..object
        })
        .collect()
}

fn non_empty<T>(v: Vec<T>) -> Option<Vec<T>> {
    (!v.is_empty()).then_some(v)
}

/// The ETag, size and mtime HEAD would report. An entry that vanished or turned into a
/// directory since it was listed is left out of the page.
async fn object(
    dal: &S3Dal,
    bucket: &str,
    key: String,
    listed: Metadata,
) -> S3Result<Option<Object>> {
    let stat = if listing_is_complete(&dal.profile, &listed) {
        listed
    } else {
        match dal.op.stat(&naming::object_path(bucket, &key)).await {
            Ok(m) if m.is_file() => m,
            Ok(_) => return Ok(None),
            // Read as a GET reads it: once `a/b` is deleted and `a` written, fs answers ENOTDIR,
            // and one key that changed under the listing failed the whole page.
            Err(err) => match map(err, Ctx::ObjectRead) {
                err if *err.code() == s3s::S3ErrorCode::NoSuchKey => return Ok(None),
                err => return Err(err),
            },
        }
    };
    let meta = meta::resolve(&dal.profile, &stat);
    Ok(Some(Object {
        key: Some(key),
        e_tag: Some(ETag::Strong(meta.etag)),
        size: Some(i64::try_from(meta.content_length).unwrap_or(i64::MAX)),
        last_modified: meta.last_modified.map(Timestamp::from),
        storage_class: Some(ObjectStorageClass::from_static(
            ObjectStorageClass::STANDARD,
        )),
        ..Default::default()
    }))
}

/// Object stores list native ETags, but s3dal's own ETag lives in user metadata, which no
/// listing carries. Without the stat, a listing would show a synthesized ETag where HEAD
/// shows the stored MD5, and clients that compare the two would see a changed object.
fn listing_is_complete(profile: &Profile, listed: &Metadata) -> bool {
    profile.native_etag && listed.etag().is_some() && listed.last_modified().is_some()
}

/// Opaque to clients, but it must carry any UTF-8 key, `/` and `+` included, through a query
/// string unchanged.
fn encode_token(marker: &str) -> String {
    base64_simd::URL_SAFE_NO_PAD.encode_to_string(marker)
}

fn decode_token(token: &str) -> S3Result<String> {
    base64_simd::URL_SAFE_NO_PAD
        .decode_to_vec(token)
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .ok_or_else(|| {
            s3_error!(
                InvalidArgument,
                "The continuation token provided is incorrect"
            )
        })
}

/// S3's `encoding-type=url` form. Some clients decode it as a form value (botocore uses
/// `unquote_plus`), others as a plain percent-encoded string (Go's `url.PathUnescape`, most
/// hand-written clients). A space is written `%20`, which both read as a space; `+` would reach
/// the second kind as a literal `+`. A literal `+` must then be `%2B`, or form decoding would
/// turn it into a space. `/` and `*` stay readable, as on AWS; every other byte outside
/// `[A-Za-z0-9._-]` is percent-encoded.
pub(super) fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'/' | b'*' => {
                out.push(char::from(b));
            }
            _ => {
                let _ = write!(out, "%{b:02X}");
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use opendal::MetadataBuilder;
    use opendal::raw::Timestamp as OpendalTimestamp;

    use super::*;

    #[tokio::test]
    async fn a_key_under_what_became_an_object_since_the_listing_is_left_out() {
        // Between the listing and its stats, another client may delete `a/b` and put `a`.
        let root = tempfile::tempdir().unwrap();
        let fs_root = root.path().to_string_lossy().into_owned();
        let op = crate::backend::build_operator("fs", &[("root".to_owned(), fs_root)]).unwrap();
        let profile = crate::backend::probe(&op).await.unwrap();
        let dal = S3Dal::new(op.clone(), profile, crate::Settings::default());
        op.write("b/a", b"x".to_vec()).await.unwrap();
        let listed = MetadataBuilder::file(1).build();
        let object = object(&dal, "b", "a/b".to_owned(), listed).await.unwrap();
        assert!(object.is_none(), "{object:?}");
    }

    #[test]
    fn continuation_tokens_round_trip_any_key() {
        // A token that loses a byte resumes at the wrong place: keys repeat or go missing.
        for key in ["a", "a/b/", "a+b c%2F", "ünï/😀", "x=y&z", "~"] {
            let token = encode_token(key);
            assert!(
                token
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
                "{token} needs escaping in a query string"
            );
            assert_eq!(decode_token(&token).unwrap(), key);
        }
    }

    #[test]
    fn tokens_s3dal_never_issues_are_rejected() {
        let invalid_utf8 = base64_simd::URL_SAFE_NO_PAD.encode_to_string([0xff, 0xfe]);
        for token in ["not base64!", invalid_utf8.as_str()] {
            let err = decode_token(token).unwrap_err();
            assert_eq!(*err.code(), s3s::S3ErrorCode::InvalidArgument, "{token:?}");
        }
    }

    #[test]
    fn url_encoding_follows_s3() {
        // Every name must come back unchanged through form decoding and plain percent decoding
        // alike: a space written `+` reaches a percent-decoding client as `+`, and an unescaped
        // `+` reaches a form-decoding one as a space.
        assert_eq!(
            url_encode("a b+c%d/ø*~_.-Z9"),
            "a%20b%2Bc%25d/%C3%B8*%7E_.-Z9"
        );
    }

    #[test]
    fn listing_metadata_is_trusted_only_for_native_etags() {
        let gateway = Profile {
            native_etag: false,
            native_content_type: false,
            native_cache_control: false,
            native_content_disposition: false,
            native_content_encoding: false,
            encoded_metadata_names: false,
            ordered_list: false,
            real_dirs: true,
            conditional_writes: false,
        };
        let native = Profile {
            native_etag: true,
            ..gateway
        };
        let entry = |etag: Option<&str>| {
            let mut m = MetadataBuilder::file(1);
            m.last_modified(OpendalTimestamp::from_second(1_700_000_000).unwrap());
            if let Some(etag) = etag {
                m.etag(etag);
            }
            m.build()
        };
        assert!(listing_is_complete(&native, &entry(Some("\"e\""))));
        assert!(!listing_is_complete(&native, &entry(None)));
        // A backend ETag is not the ETag s3dal reports on a gateway profile.
        assert!(!listing_is_complete(&gateway, &entry(Some("\"e\""))));
    }
}

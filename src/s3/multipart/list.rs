//! ListParts and ListMultipartUploads.

use std::time::SystemTime;

use futures::{StreamExt, TryStreamExt};
use s3s::dto::*;
use s3s::{S3Error, S3Request, S3Response, S3Result, s3_error};

use super::record::{self, PartManifest};
use super::{parse_upload_id, upload_dir, uploads_dir};
use crate::S3Dal;
use crate::error::{Ctx, map};
use crate::s3::list::url_encode;
use crate::s3::{acl, naming};

/// AWS's default and cap for max-parts and max-uploads.
const MAX_PAGE: usize = 1000;

/// Manifest and `upload.json` reads in flight; a page names up to 1000 of them.
const READ_CONCURRENCY: usize = 16;

pub(in crate::s3) async fn list_parts(
    dal: &S3Dal,
    req: S3Request<ListPartsInput>,
) -> S3Result<S3Response<ListPartsOutput>> {
    let input = req.input;
    naming::require_bucket(dal, &input.bucket).await?;
    let id = parse_upload_id(&input.upload_id)?;
    let upload = record::load_upload(dal, &input.bucket, id, &input.key).await?;
    let max_parts = page_size(input.max_parts, "max-parts")?;
    let after = i64::from(input.part_number_marker.unwrap_or(0));

    let dir = upload_dir(&input.bucket, id);
    let mut numbers: Vec<u32> = dal
        .op
        .list(&dir)
        .await
        .map_err(|err| map(err, Ctx::Other))?
        .iter()
        .filter_map(|entry| record::manifest_part(entry.name()))
        .filter(|&n| i64::from(n) > after)
        .collect();
    numbers.sort_unstable();
    // As for ListObjects, a page size of 0 asks for nothing and is never truncated.
    let is_truncated = max_parts > 0 && numbers.len() > max_parts;
    numbers.truncate(max_parts);
    let next_part_number_marker = numbers.last().map(|&n| n as i32);

    let dir = &dir;
    let parts: Vec<Part> = futures::stream::iter(numbers)
        .map(|n| async move {
            let manifest = record::load_manifest(dal, dir, n).await?;
            Ok::<_, S3Error>(manifest.map(|m| part(n, m)))
        })
        .buffered(READ_CONCURRENCY)
        // A manifest gone since the listing belongs to an upload just completed or aborted.
        .try_filter_map(|part| async move { Ok(part) })
        .try_collect()
        .await?;

    Ok(S3Response::new(ListPartsOutput {
        bucket: Some(input.bucket),
        key: Some(input.key),
        upload_id: Some(input.upload_id),
        part_number_marker: input.part_number_marker,
        next_part_number_marker,
        max_parts: Some(max_parts as i32),
        is_truncated: Some(is_truncated),
        parts: Some(parts),
        storage_class: Some(StorageClass::from_static(StorageClass::STANDARD)),
        owner: Some(acl::owner()),
        initiator: Some(acl::initiator()),
        checksum_algorithm: upload.checksum_algorithm.map(ChecksumAlgorithm::from),
        checksum_type: upload.checksum_type.map(ChecksumType::from),
        ..Default::default()
    }))
}

fn part(n: u32, m: PartManifest) -> Part {
    let checksum = record::checksum_of(&m.checksums);
    Part {
        part_number: Some(n as i32),
        e_tag: Some(ETag::Strong(m.md5)),
        size: Some(i64::try_from(m.size).unwrap_or(i64::MAX)),
        last_modified: Some(Timestamp::from(m.last_modified)),
        checksum_crc32: checksum.checksum_crc32,
        checksum_crc32c: checksum.checksum_crc32c,
        checksum_crc64nvme: checksum.checksum_crc64nvme,
        checksum_sha1: checksum.checksum_sha1,
        checksum_sha256: checksum.checksum_sha256,
        checksum_sha512: checksum.checksum_sha512,
        checksum_md5: checksum.checksum_md5,
        checksum_xxhash64: checksum.checksum_xxhash64,
        checksum_xxhash3: checksum.checksum_xxhash3,
        checksum_xxhash128: checksum.checksum_xxhash128,
    }
}

pub(in crate::s3) async fn list_multipart_uploads(
    dal: &S3Dal,
    req: S3Request<ListMultipartUploadsInput>,
) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
    let input = req.input;
    naming::require_bucket(dal, &input.bucket).await?;
    let max_uploads = page_size(input.max_uploads, "max-uploads")?;
    let url_encoded = match &input.encoding_type {
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
            input.prefix.as_deref(),
            input.delimiter.as_deref(),
            input.key_marker.as_deref(),
            input.upload_id_marker.as_deref(),
        ],
    )?;
    let encode = |s: String| if url_encoded { url_encode(&s) } else { s };

    let uploads = load_uploads(dal, &input.bucket).await?;
    let query = Query {
        prefix: input.prefix.as_deref().unwrap_or_default(),
        delimiter: input.delimiter.as_deref().filter(|d| !d.is_empty()),
        key_marker: input.key_marker.as_deref().filter(|m| !m.is_empty()),
        upload_id_marker: input.upload_id_marker.as_deref().filter(|m| !m.is_empty()),
        max: max_uploads,
    };
    let page = query.page(&uploads);

    let (next_key_marker, next_upload_id_marker) = match page.items.last() {
        Some(Item::Upload(u)) if page.is_truncated => (Some(u.key.clone()), Some(u.id.clone())),
        Some(Item::CommonPrefix(prefix)) if page.is_truncated => (Some(prefix.clone()), None),
        _ => (None, None),
    };
    let mut listed = Vec::new();
    let mut common_prefixes = Vec::new();
    for item in page.items {
        match item {
            Item::Upload(u) => listed.push(MultipartUpload {
                key: Some(encode(u.key.clone())),
                upload_id: Some(u.id.clone()),
                initiated: Some(Timestamp::from(u.initiated)),
                owner: Some(acl::owner()),
                initiator: Some(acl::initiator()),
                storage_class: Some(StorageClass::from_static(StorageClass::STANDARD)),
                checksum_algorithm: u.checksum_algorithm.clone().map(ChecksumAlgorithm::from),
                checksum_type: u.checksum_type.clone().map(ChecksumType::from),
            }),
            Item::CommonPrefix(prefix) => common_prefixes.push(CommonPrefix {
                prefix: Some(encode(prefix)),
            }),
        }
    }

    Ok(S3Response::new(ListMultipartUploadsOutput {
        bucket: Some(input.bucket),
        prefix: Some(encode(input.prefix.unwrap_or_default())),
        delimiter: input.delimiter.map(encode),
        key_marker: input.key_marker.map(encode),
        upload_id_marker: input.upload_id_marker,
        next_key_marker: next_key_marker.map(encode),
        next_upload_id_marker,
        max_uploads: Some(max_uploads as i32),
        is_truncated: Some(page.is_truncated),
        uploads: Some(listed),
        common_prefixes: (!common_prefixes.is_empty()).then_some(common_prefixes),
        encoding_type: input.encoding_type,
        ..Default::default()
    }))
}

/// max-parts and max-uploads: AWS's cap answers a larger request, as for max-keys.
pub(in crate::s3) fn page_size(requested: Option<i32>, name: &str) -> S3Result<usize> {
    match requested {
        None => Ok(MAX_PAGE),
        Some(n) => usize::try_from(n)
            .map(|n| n.min(MAX_PAGE))
            .map_err(|_| s3_error!(InvalidArgument, "{name} must not be negative")),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Listed {
    key: String,
    initiated: SystemTime,
    id: String,
    checksum_algorithm: Option<String>,
    checksum_type: Option<String>,
}

/// Every upload of the bucket in listing order: by key, then by initiation time. The id breaks
/// ties, so pages never shift between requests.
async fn load_uploads(dal: &S3Dal, bucket: &str) -> S3Result<Vec<Listed>> {
    let ids: Vec<_> = dal
        .op
        .list(&uploads_dir(bucket))
        .await
        .map_err(|err| map(err, Ctx::Other))?
        .iter()
        .filter_map(|entry| parse_upload_id(entry.name().strip_suffix('/')?).ok())
        .collect();
    let mut uploads: Vec<Listed> = futures::stream::iter(ids)
        .map(|id| async move {
            let upload = record::load_upload_at(dal, &upload_dir(bucket, id)).await?;
            // A completed upload is kept only to answer a retried Complete.
            let active = upload.filter(|u| u.completed.is_none());
            Ok::<_, S3Error>(active.map(|u| Listed {
                key: u.key,
                initiated: u.initiated,
                id: id.hyphenated().to_string(),
                checksum_algorithm: u.checksum_algorithm,
                checksum_type: u.checksum_type,
            }))
        })
        .buffered(READ_CONCURRENCY)
        .try_filter_map(|listed| async move { Ok(listed) })
        .try_collect()
        .await?;
    uploads.sort_by(|a, b| (&a.key, a.initiated, &a.id).cmp(&(&b.key, b.initiated, &b.id)));
    Ok(uploads)
}

/// A ListMultipartUploads request, as far as paging is concerned.
struct Query<'a> {
    prefix: &'a str,
    /// Non-empty; an empty delimiter is no delimiter, as for ListObjects.
    delimiter: Option<&'a str>,
    key_marker: Option<&'a str>,
    /// Only meaningful with `key_marker`, as on AWS.
    upload_id_marker: Option<&'a str>,
    max: usize,
}

#[derive(Debug, PartialEq, Eq)]
enum Item<'u> {
    Upload(&'u Listed),
    CommonPrefix(String),
}

struct Page<'u> {
    items: Vec<Item<'u>>,
    is_truncated: bool,
}

impl Query<'_> {
    /// Cuts one page from `uploads`, which must be in listing order. The rules follow
    /// ListObjects (design.md "Listing"): a CommonPrefix sorts as itself, so a key marker equal
    /// to one skips its whole group.
    fn page<'u>(&self, uploads: &'u [Listed]) -> Page<'u> {
        // The marker upload's position. If it has gone since the last page, every upload of the
        // marker key is listed again: repeating one is better than skipping one.
        let resume_after = self
            .key_marker
            .zip(self.upload_id_marker)
            .and_then(|(key, id)| uploads.iter().position(|u| u.key == key && u.id == id));

        let mut items: Vec<Item<'u>> = Vec::new();
        let mut is_truncated = false;
        for (i, upload) in uploads.iter().enumerate() {
            if self.max == 0 {
                break;
            }
            let Some(rest) = upload.key.strip_prefix(self.prefix) else {
                continue;
            };
            let group_end = self
                .delimiter
                .and_then(|d| Some(self.prefix.len() + rest.find(d)? + d.len()));
            let item = match group_end {
                Some(end) => {
                    let prefix = &upload.key[..end];
                    // Every key that starts with a CommonPrefix is in its group, whatever the
                    // delimiter (design.md "Listing"), so the keys of one group are contiguous
                    // and the group was just taken if at all.
                    if matches!(items.last(), Some(Item::CommonPrefix(p)) if p == prefix) {
                        continue;
                    }
                    Item::CommonPrefix(prefix.to_owned())
                }
                None => Item::Upload(upload),
            };
            if !self.is_after_marker(&item, i, resume_after) {
                continue;
            }
            if items.len() == self.max {
                is_truncated = true;
                break;
            }
            items.push(item);
        }
        Page {
            items,
            is_truncated,
        }
    }

    fn is_after_marker(&self, item: &Item<'_>, index: usize, resume_after: Option<usize>) -> bool {
        let Some(marker) = self.key_marker else {
            return true;
        };
        match item {
            Item::CommonPrefix(prefix) => prefix.as_str() > marker,
            Item::Upload(u) if u.key == marker => {
                self.upload_id_marker.is_some() && resume_after.is_none_or(|pos| index > pos)
            }
            Item::Upload(u) => u.key.as_str() > marker,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use super::*;

    fn upload(key: &str, secs: u64, id: &str) -> Listed {
        Listed {
            key: key.to_owned(),
            initiated: UNIX_EPOCH + Duration::from_secs(secs),
            id: id.to_owned(),
            checksum_algorithm: None,
            checksum_type: None,
        }
    }

    fn query<'a>(
        prefix: &'a str,
        delimiter: Option<&'a str>,
        key_marker: Option<&'a str>,
        upload_id_marker: Option<&'a str>,
        max: usize,
    ) -> Query<'a> {
        Query {
            prefix,
            delimiter,
            key_marker,
            upload_id_marker,
            max,
        }
    }

    /// Uploads as `key:id`, CommonPrefixes as `CP:<prefix>`.
    fn run(uploads: &[Listed], q: &Query<'_>) -> (Vec<String>, bool) {
        let page = q.page(uploads);
        let names = page
            .items
            .iter()
            .map(|item| match item {
                Item::Upload(u) => format!("{}:{}", u.key, u.id),
                Item::CommonPrefix(p) => format!("CP:{p}"),
            })
            .collect();
        (names, page.is_truncated)
    }

    fn sample() -> Vec<Listed> {
        // Already in listing order: key, then initiation time.
        vec![
            upload("a", 2, "z"),
            upload("a", 3, "b"),
            upload("b", 1, "c"),
            upload("d/1", 1, "d"),
            upload("d/2", 1, "e"),
            upload("e", 1, "f"),
        ]
    }

    #[test]
    fn pages_resume_after_the_marker_upload_of_a_key() {
        // Two uploads of one key can straddle a page boundary; the second page must start
        // with the second upload, not skip the rest of the key.
        let uploads = sample();
        let (first, truncated) = run(&uploads, &query("", None, None, None, 1));
        assert_eq!(first, ["a:z"]);
        assert!(truncated);
        let (second, _) = run(&uploads, &query("", None, Some("a"), Some("z"), 2));
        assert_eq!(second, ["a:b", "b:c"]);
        // A key marker alone moves past every upload of that key, as on AWS.
        let (by_key, _) = run(&uploads, &query("", None, Some("a"), None, 1));
        assert_eq!(by_key, ["b:c"]);
    }

    #[test]
    fn a_vanished_marker_upload_lists_its_key_again() {
        let uploads = sample();
        let (page, _) = run(&uploads, &query("", None, Some("a"), Some("gone"), 2));
        assert_eq!(page, ["a:z", "a:b"]);
    }

    #[test]
    fn delimiter_groups_keys_and_a_group_marker_skips_the_group() {
        let uploads = sample();
        let (page, truncated) = run(&uploads, &query("", Some("/"), Some("b"), None, 1));
        assert_eq!(page, ["CP:d/"]);
        assert!(truncated);
        // Resuming from the CommonPrefix must not list keys inside it again.
        let (next, truncated) = run(&uploads, &query("", Some("/"), Some("d/"), None, 10));
        assert_eq!(next, ["e:f"]);
        assert!(!truncated);
        let (inside, _) = run(&uploads, &query("d/", Some("/"), None, None, 10));
        assert_eq!(inside, ["d/1:d", "d/2:e"]);
    }

    #[test]
    fn any_delimiter_groups_up_to_its_first_occurrence_after_the_prefix() {
        // Clients list uploads with the delimiter they list objects with; a key's group must be
        // the CommonPrefix ListObjects would give it, or a cleanup tool walking both misses
        // uploads.
        let uploads = vec![
            upload("a-1", 1, "a"),
            upload("a-2-x", 1, "b"),
            upload("a/b", 1, "c"),
            upload("b--c--d", 1, "d"),
            upload("b--e", 1, "e"),
            upload("c", 1, "f"),
        ];
        let (dash, _) = run(&uploads, &query("", Some("-"), None, None, 10));
        assert_eq!(dash, ["CP:a-", "a/b:c", "CP:b-", "c:f"]);
        let (double, _) = run(&uploads, &query("", Some("--"), None, None, 10));
        assert_eq!(double, ["a-1:a", "a-2-x:b", "a/b:c", "CP:b--", "c:f"]);
        // The first occurrence after the prefix, not in it.
        let (after_prefix, _) = run(&uploads, &query("b--", Some("--"), None, None, 10));
        assert_eq!(after_prefix, ["CP:b--c--", "b--e:e"]);
        // A page boundary on a group resumes past the group.
        let (first, truncated) = run(&uploads, &query("", Some("-"), None, None, 1));
        assert_eq!(
            (first.as_slice(), truncated),
            (&["CP:a-".to_owned()][..], true)
        );
        let (next, _) = run(&uploads, &query("", Some("-"), Some("a-"), None, 10));
        assert_eq!(next, ["a/b:c", "CP:b-", "c:f"]);
    }

    #[test]
    fn a_full_last_page_is_not_truncated_and_zero_asks_for_nothing() {
        let uploads = sample();
        let (all, truncated) = run(&uploads, &query("", None, None, None, 6));
        assert_eq!(all.len(), 6);
        assert!(!truncated);
        let (none, truncated) = run(&uploads, &query("", None, None, None, 0));
        assert!(none.is_empty());
        assert!(!truncated);
    }

    #[test]
    fn negative_page_sizes_are_invalid_and_large_ones_are_capped() {
        assert_eq!(page_size(None, "max-uploads").unwrap(), MAX_PAGE);
        assert_eq!(page_size(Some(5000), "max-uploads").unwrap(), MAX_PAGE);
        assert_eq!(
            *page_size(Some(-1), "max-uploads").unwrap_err().code(),
            s3s::S3ErrorCode::InvalidArgument
        );
    }
}

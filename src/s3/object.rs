//! PutObject, GetObject, HeadObject, GetObjectAttributes and DeleteObject.

use bytes::Bytes;
use futures::{Stream, StreamExt};
use hyper::header::{
    HeaderMap, HeaderName, HeaderValue, IF_MATCH, IF_MODIFIED_SINCE, IF_NONE_MATCH,
    IF_UNMODIFIED_SINCE,
};
use opendal::Buffer;
use s3s::dto::*;
use s3s::stream::ByteStream;
use s3s::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, TrailingHeaders, s3_error};

use super::precondition::{self, Conditions};
use super::store::{self, DeleteCondition, Dest, Precondition, write_err};
use super::upload::{self, BodyCheck, BodyHeaders, body_error, trailer_lookup};
use super::{S3Dal, multipart, naming, unsupported};
use crate::checksum::{self, ALGORITHMS};
use crate::error::{Ctx, map};
use crate::meta::{self, Attrs, Computed, ObjectMeta};

pub(super) async fn put_object(
    dal: &S3Dal,
    req: S3Request<PutObjectInput>,
) -> S3Result<S3Response<PutObjectOutput>> {
    let trailers = req.trailing_headers;
    let mut input = req.input;
    input.content_md5 = upload::content_md5(input.content_md5.take(), &req.headers);
    naming::require_bucket(dal, &input.bucket).await?;
    naming::validate_key(&input.key)?;
    unsupported::refuse_new_object_features!(input)?;
    // An append at an offset; writing the body as the whole object would lose the rest.
    unsupported::refuse(&[(
        input.write_offset_bytes.is_some(),
        "x-amz-write-offset-bytes",
    )])?;
    let (if_match, if_none_match) = (input.if_match.take(), input.if_none_match.take());
    let cond = Precondition {
        if_match: if_match.as_ref(),
        if_none_match: if_none_match.as_ref(),
    };
    cond.validate()?;
    let user_metadata = input.metadata.take().unwrap_or_default();
    meta::check_user_metadata(&user_metadata)?;
    let attrs = Attrs {
        content_type: input.content_type.take(),
        cache_control: input.cache_control.take(),
        content_disposition: input.content_disposition.take(),
        content_encoding: stored_content_encoding(input.content_encoding.take()),
        content_language: input.content_language.take(),
        expires: input.expires.take(),
        user_metadata,
    };
    attrs.check_header_values()?;
    let Some(body) = input.body.take() else {
        return Err(s3_error!(IncompleteBody));
    };
    // s3s 0.17 leaves Content-Length out for an empty form upload (PostObject) file, though
    // the file stream knows its exact length; without it the empty object would get a
    // synthesized ETag instead of the MD5 an empty PUT gets.
    let declared = input
        .content_length
        .and_then(|n| u64::try_from(n).ok())
        .or_else(|| body.remaining_length().exact().map(|n| n as u64));
    let mut body = body.map(|chunk| chunk.map_err(body_error));

    // Decision C1: a computed MD5 or checksum can be stored only if it is known before the
    // write starts, so small bodies are taken in memory first, and large ones fall back to a
    // synthesized ETag. With native ETags the backend computes the ETag, and buffering is what
    // stores a trailer checksum; it costs no extra memory there, because OpenDAL holds a body of
    // that size anyway to send it in one request (`Dest::send_whole_up_to_limit`).
    let buffered = input.content_md5.is_none()
        && declared.is_some_and(|n| n <= dal.settings.etag_buffer_limit);
    let mut check = BodyCheck::new(&BodyHeaders::from_put(&input), buffered)?;
    let dest = Dest::new(dal, &input.bucket, &input.key);

    let trailers = trailers.as_ref();
    let (etag, checksum) = if input.key.ends_with('/') {
        if declared.is_some_and(|n| n > 0) {
            return Err(unexpected_content());
        }
        // A folder marker is a directory, created without a writer to commit through.
        if !cond.is_empty() {
            return Err(s3_error!(
                NotImplemented,
                "conditional writes of folder markers are not supported"
            ));
        }
        put_folder_marker(&dest, &mut body, &mut check, trailers).await?
    } else if buffered {
        let mut chunks = Vec::new();
        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            check.update(&chunk)?;
            chunks.push(chunk);
        }
        // Trailers have arrived with the whole body, so every checksum is known before the
        // write and can be stored with it.
        let verified = check.finish(trailer_lookup(trailers))?;
        let md5 = verified.md5_hex.clone().unwrap_or_default();
        let computed = Computed {
            md5_hex: Some(md5.clone()),
            checksum: verified.checksum.clone(),
            parts: None,
        };
        let opts = meta::write_options(&dal.profile, attrs, computed);
        let closed = dest.write_all(opts, Buffer::from(chunks), cond).await?;
        let etag = if dal.profile.native_etag {
            dest.written_etag(closed, check.received()).await?
        } else {
            md5
        };
        (etag, verified.checksum)
    } else {
        let stored_md5 = check.content_md5().map(str::to_owned);
        let computed = Computed {
            md5_hex: stored_md5.clone(),
            checksum: check.declared_checksum(),
            parts: None,
        };
        let mut opts = meta::write_options(&dal.profile, attrs, computed);
        opts.chunk = declared.and_then(|n| store::chunk_for(&dal.op, n));
        dest.send_whole_up_to_limit(&mut opts);
        let (verified, closed) = dest
            .write_stream(opts, &mut body, &mut check, trailer_lookup(trailers), cond)
            .await?;
        let etag = match stored_md5 {
            Some(md5) if !dal.profile.native_etag => md5,
            _ => dest.written_etag(closed, check.received()).await?,
        };
        (etag, verified.checksum)
    };

    Ok(S3Response::new(PutObjectOutput {
        e_tag: Some(ETag::Strong(etag)),
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
        ..Default::default()
    }))
}

/// A key ending in `/` becomes a directory, which is all a hierarchical backend can hold for it.
async fn put_folder_marker(
    dest: &Dest<'_>,
    body: &mut (impl Stream<Item = S3Result<Bytes>> + Unpin),
    check: &mut BodyCheck,
    trailers: Option<&TrailingHeaders>,
) -> S3Result<(String, Checksum)> {
    // The body is still read to the end: its length may be unknown up front, and an
    // aws-chunked trailer checksum only arrives after it.
    while let Some(chunk) = body.next().await {
        let chunk = chunk?;
        if !chunk.is_empty() {
            return Err(unexpected_content());
        }
        check.update(&chunk)?;
    }
    let verified = check.finish(trailer_lookup(trailers))?;
    dest.dal
        .op
        .create_dir(&dest.path)
        .await
        .map_err(write_err)?;
    Ok((meta::md5_hex(b""), verified.checksum))
}

fn unexpected_content() -> s3s::S3Error {
    s3_error!(
        UnexpectedContent,
        "This request does not support content: a key ending in '/' is a folder marker"
    )
}

/// `aws-chunked` names how this request's body was framed, not how the object is encoded, and
/// S3 does not store it. SDKs add it to every streamed upload that carries a trailing checksum;
/// keeping it would make GET announce an encoding the stored bytes do not have.
fn stored_content_encoding(header: Option<String>) -> Option<String> {
    let header = header?;
    let chunked = |coding: &&str| coding.trim().eq_ignore_ascii_case("aws-chunked");
    if !header.split(',').any(|c| chunked(&c)) {
        return Some(header);
    }
    let rest: Vec<&str> = header
        .split(',')
        .filter(|c| !chunked(c))
        .map(str::trim)
        .filter(|c| !c.is_empty())
        .collect();
    (!rest.is_empty()).then(|| rest.join(","))
}

pub(super) async fn get_object(
    dal: &S3Dal,
    req: S3Request<GetObjectInput>,
) -> S3Result<S3Response<GetObjectOutput>> {
    unsupported::refuse_sse_on_read(&req.headers)?;
    let input = req.input;
    naming::require_bucket(dal, &input.bucket).await?;
    naming::validate_key(&input.key)?;
    unsupported::refuse_sse_c(
        input.sse_customer_algorithm.as_ref(),
        input.sse_customer_key.as_ref(),
        input.sse_customer_key_md5.as_ref(),
    )?;
    naming::require_null_version(input.version_id.as_deref())?;
    let part = check_part_number(input.part_number, input.range.is_some())?;
    let (path, obj, version) = stat_object(dal, &input.bucket, &input.key).await?;
    precondition::check(
        &Conditions {
            if_match: input.if_match.as_ref(),
            if_none_match: input.if_none_match.as_ref(),
            if_modified_since: input.if_modified_since.as_ref(),
            if_unmodified_since: input.if_unmodified_since.as_ref(),
        },
        &obj.etag,
        obj.last_modified,
    )?;

    let len = obj.content_length;
    let served = match (part, &input.range) {
        (Some(n), _) => Served::part(&obj, n)?,
        (None, Some(range)) => Served::range(range, len)?,
        (None, None) => Served::whole(len),
    };
    let range = served.range.clone();
    let body = if range.is_empty() {
        StreamingBlob::from(Bytes::new())
    } else {
        // The conditions and headers describe the object as stat'ed, and a version written since
        // would be served under them: a ranged download, which SDK transfer managers make one
        // part at a time with If-Match, would splice two objects. Where the backend can, the
        // read is pinned to the stat'ed version, and a newer one fails the body instead, which
        // clients retry. fs cannot, but there the gap is a stat and an open.
        let reader = match version.filter(|_| dal.op.info().capability().read_with_if_match) {
            Some(version) => dal.op.reader_with(&path).if_match(&version).await,
            None => dal.op.reader(&path).await,
        }
        .map_err(read_err)?;
        let stream = reader
            .into_bytes_stream(range.clone())
            .await
            .map_err(read_err)?;
        StreamingBlob::wrap(stream)
    };

    let content_type = obj.content_type().to_owned();
    let checksum = returned_checksum(
        obj.checksum,
        input.checksum_mode.as_ref(),
        served.is_whole(len),
    );
    let attrs = obj.attrs;
    // `response-*` query parameters are applied by s3s after this returns: it replaces these
    // headers with the requested values.
    Ok(S3Response::new(GetObjectOutput {
        body: Some(body),
        content_length: Some(length(range.end - range.start)?),
        content_range: served.content_range,
        parts_count: served.parts_count,
        accept_ranges: Some("bytes".to_owned()),
        e_tag: Some(ETag::Strong(obj.etag)),
        content_type: Some(content_type),
        cache_control: attrs.cache_control,
        content_disposition: attrs.content_disposition,
        content_encoding: attrs.content_encoding,
        content_language: attrs.content_language,
        expires: attrs.expires,
        last_modified: obj.last_modified.map(Timestamp::from),
        metadata: Some(attrs.user_metadata),
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
        checksum_type: checksum.checksum_type,
        ..Default::default()
    }))
}

/// HEAD ignores `Range`: s3s 0.17 always answers HeadObject with 200, and a 200 carrying a
/// partial Content-Length would misstate the object's size.
///
/// `partNumber` is different: S3 answers it with the part's Content-Length and Content-Range,
/// and SDK transfer managers read a part's size and position from exactly those. Here they
/// come with 200 instead of S3's 206. The part is named in the URI, so no cache takes its
/// length for the object's.
pub(super) async fn head_object(
    dal: &S3Dal,
    req: S3Request<HeadObjectInput>,
) -> S3Result<S3Response<HeadObjectOutput>> {
    unsupported::refuse_sse_on_read(&req.headers)?;
    let mut input = req.input;
    // s3s applies `response-*` to GET only, before the call; AWS applies them to HEAD as well,
    // and a HEAD that checks a presigned download must show the headers its GET will carry.
    // They are checked first, as s3s checks GET's.
    let content_type = overridden(input.response_content_type.take())?;
    let cache_control = overridden(input.response_cache_control.take())?;
    let content_disposition = overridden(input.response_content_disposition.take())?;
    let content_encoding = overridden(input.response_content_encoding.take())?;
    let content_language = overridden(input.response_content_language.take())?;
    let expires = input.response_expires.take().map(http_date).transpose()?;
    naming::require_bucket(dal, &input.bucket).await?;
    naming::validate_key(&input.key)?;
    unsupported::refuse_sse_c(
        input.sse_customer_algorithm.as_ref(),
        input.sse_customer_key.as_ref(),
        input.sse_customer_key_md5.as_ref(),
    )?;
    naming::require_null_version(input.version_id.as_deref())?;
    let part = check_part_number(input.part_number, input.range.is_some())?;
    let (_, obj, _) = stat_object(dal, &input.bucket, &input.key).await?;
    precondition::check(
        &Conditions {
            if_match: input.if_match.as_ref(),
            if_none_match: input.if_none_match.as_ref(),
            if_modified_since: input.if_modified_since.as_ref(),
            if_unmodified_since: input.if_unmodified_since.as_ref(),
        },
        &obj.etag,
        obj.last_modified,
    )?;

    let len = obj.content_length;
    let served = match part {
        Some(n) => Served::part(&obj, n)?,
        None => Served::whole(len),
    };
    let stored_content_type = obj.content_type().to_owned();
    let checksum = returned_checksum(
        obj.checksum,
        input.checksum_mode.as_ref(),
        served.is_whole(len),
    );
    let attrs = obj.attrs;
    Ok(S3Response::new(HeadObjectOutput {
        content_length: Some(length(served.range.end - served.range.start)?),
        content_range: served.content_range,
        parts_count: served.parts_count,
        accept_ranges: Some("bytes".to_owned()),
        e_tag: Some(ETag::Strong(obj.etag)),
        content_type: content_type.or(Some(stored_content_type)),
        cache_control: cache_control.or(attrs.cache_control),
        content_disposition: content_disposition.or(attrs.content_disposition),
        content_encoding: content_encoding.or(attrs.content_encoding),
        content_language: content_language.or(attrs.content_language),
        expires: expires.or(attrs.expires),
        last_modified: obj.last_modified.map(Timestamp::from),
        metadata: Some(attrs.user_metadata),
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
        checksum_type: checksum.checksum_type,
        ..Default::default()
    }))
}

/// A `response-*` value of a HEAD. One no header can carry would fail the response with a 500
/// once the handler returns, so it is refused, as s3s refuses it on GET.
fn overridden(value: Option<String>) -> S3Result<Option<String>> {
    match value {
        Some(value) if HeaderValue::from_str(&value).is_err() => Err(S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            format!("invalid overridden header value: {value:?}"),
        )),
        value => Ok(value),
    }
}

/// `response-expires` as the Expires header carries it.
fn http_date(t: Timestamp) -> S3Result<String> {
    let mut buf = Vec::new();
    t.format(TimestampFormat::HttpDate, &mut buf)
        .map_err(|err| S3Error::with_source(S3ErrorCode::InvalidRequest, Box::new(err)))?;
    String::from_utf8(buf).map_err(S3Error::internal_error)
}

/// `partNumber` is checked before the object is looked up, so a malformed request fails the
/// same way whether or not the key exists.
fn check_part_number(part_number: Option<PartNumber>, has_range: bool) -> S3Result<Option<u32>> {
    let Some(n) = part_number else {
        return Ok(None);
    };
    if has_range {
        return Err(s3_error!(
            InvalidRequest,
            "Cannot specify both Range header and partNumber query parameter"
        ));
    }
    match u32::try_from(n) {
        Ok(n) if (1..=10_000).contains(&n) => Ok(Some(n)),
        _ => Err(s3_error!(
            InvalidArgument,
            "Part number must be an integer between 1 and 10000, inclusive"
        )),
    }
}

/// The bytes of an object that a GET returns, or that a HEAD describes.
struct Served {
    range: std::ops::Range<u64>,
    /// Set for a partial response; s3s then answers GET with 206.
    content_range: Option<String>,
    parts_count: Option<PartsCount>,
}

impl Served {
    fn whole(len: u64) -> Self {
        Self {
            range: 0..len,
            content_range: None,
            parts_count: None,
        }
    }

    fn range(range: &Range, len: u64) -> S3Result<Self> {
        let r = range.check(len)?;
        // Only a suffix range on an empty object resolves to nothing; Content-Range cannot
        // express an empty range, so the whole (empty) object is served instead.
        if r.is_empty() {
            return Ok(Self::whole(len));
        }
        Ok(Self::partial(r, len, None))
    }

    /// Part `n` of the object, per design.md "Part layout". `n` is already within 1..=10000.
    fn part(obj: &ObjectMeta, n: u32) -> S3Result<Self> {
        let len = obj.content_length;
        match obj.parts {
            // Not assembled from parts: the object is its own single part. S3 reports a part
            // count only for multipart objects.
            None if n == 1 => Ok(Self::partial(0..len, len, None)),
            None => Err(invalid_part_number()),
            Some(layout) if n > layout.count => Err(invalid_part_number()),
            Some(layout) => match layout.range(n, len) {
                Some(r) => Ok(Self::partial(r, len, Some(layout.count))),
                // Serving a guessed range would hand out the wrong bytes for the part.
                None => Err(unsupported::not_implemented(
                    "partNumber on an object whose part boundaries are not stored",
                )),
            },
        }
    }

    fn partial(range: std::ops::Range<u64>, len: u64, count: Option<u32>) -> Self {
        // An empty part (of an empty object) has no Content-Range form.
        let content_range =
            (!range.is_empty()).then(|| format!("bytes {}-{}/{len}", range.start, range.end - 1));
        Self {
            range,
            content_range,
            parts_count: count.and_then(|c| PartsCount::try_from(c).ok()),
        }
    }

    fn is_whole(&self, len: u64) -> bool {
        self.range == (0..len)
    }
}

/// S3 answers a part number beyond the object's part count with 416, like an unsatisfiable
/// Range. s3s 0.17 has no named code for it, so the status is borrowed from InvalidRange.
fn invalid_part_number() -> S3Error {
    let mut err = S3Error::with_message(
        S3ErrorCode::Custom("InvalidPartNumber".into()),
        "The requested partnumber is not satisfiable",
    );
    if let Some(status) = S3ErrorCode::InvalidRange.status_code() {
        err.set_status_code(status);
    }
    err
}

/// Stored checksums are returned only when the client asks (`x-amz-checksum-mode: ENABLED`,
/// which current SDKs send on every GET) and only with a response that covers the whole
/// object: SDKs validate the body they receive against them, so a full-object checksum on a
/// range or a part would fail a correct download.
fn returned_checksum(stored: Checksum, mode: Option<&ChecksumMode>, whole: bool) -> Checksum {
    let enabled = mode.is_some_and(|m| m.as_str() == ChecksumMode::ENABLED);
    if !enabled || !whole || stored == Checksum::default() {
        return Checksum::default();
    }
    // A composite checksum, of a multipart object, carries its part count; any other covers the
    // whole object, whether a single write's or combined from the parts' CRCs.
    let mut values = stored.clone();
    let composite = ALGORITHMS.into_iter().any(|(_, field, _)| {
        field(&mut values)
            .as_deref()
            .is_some_and(checksum::is_composite)
    });
    let checksum_type = if composite {
        ChecksumType::COMPOSITE
    } else {
        ChecksumType::FULL_OBJECT
    };
    Checksum {
        checksum_type: Some(ChecksumType::from_static(checksum_type)),
        ..stored
    }
}

pub(super) async fn delete_object(
    dal: &S3Dal,
    req: S3Request<DeleteObjectInput>,
) -> S3Result<S3Response<DeleteObjectOutput>> {
    let input = req.input;
    naming::require_bucket(dal, &input.bucket).await?;
    naming::require_null_version(input.version_id.as_deref())?;
    let cond = DeleteCondition {
        if_match: input.if_match.as_ref(),
        last_modified: input.if_match_last_modified_time.as_ref(),
        size: input.if_match_size,
    };
    store::delete_key(dal, &input.bucket, &input.key, cond).await?;
    Ok(S3Response::new(DeleteObjectOutput::default()))
}

/// Every request that would tag an object is refused (`unsupported.rs`), so no object has tags,
/// and its tag set is empty, which is also how S3 answers for an untagged object. The aws CLI
/// reads the source's tags before every multipart copy and fails the copy without an answer.
pub(super) async fn get_object_tagging(
    dal: &S3Dal,
    req: S3Request<GetObjectTaggingInput>,
) -> S3Result<S3Response<GetObjectTaggingOutput>> {
    let input = req.input;
    naming::require_bucket(dal, &input.bucket).await?;
    naming::validate_key(&input.key)?;
    naming::require_null_version(input.version_id.as_deref())?;
    stat_object(dal, &input.bucket, &input.key).await?;
    Ok(S3Response::new(GetObjectTaggingOutput::default()))
}

/// What HEAD reports, plus the part layout, in an XML body; only the requested attributes are
/// included. AWS evaluates the conditional headers as for GET, but s3s 0.17 does not parse them
/// for this operation, so they are read from the request here.
pub(super) async fn get_object_attributes(
    dal: &S3Dal,
    req: S3Request<GetObjectAttributesInput>,
) -> S3Result<S3Response<GetObjectAttributesOutput>> {
    let headers = &req.headers;
    let etag = |name| {
        condition_header(headers, name, |v| {
            ETagCondition::parse_http_header(v.as_bytes()).ok()
        })
    };
    let date = |name| {
        condition_header(headers, name, |v| {
            Timestamp::parse(TimestampFormat::HttpDate, v).ok()
        })
    };
    let (if_match, if_none_match) = (etag(&IF_MATCH)?, etag(&IF_NONE_MATCH)?);
    let if_modified_since = date(&IF_MODIFIED_SINCE)?;
    let if_unmodified_since = date(&IF_UNMODIFIED_SINCE)?;
    let input = req.input;
    naming::require_bucket(dal, &input.bucket).await?;
    naming::validate_key(&input.key)?;
    unsupported::refuse_sse_c(
        input.sse_customer_algorithm.as_ref(),
        input.sse_customer_key.as_ref(),
        input.sse_customer_key_md5.as_ref(),
    )?;
    naming::require_null_version(input.version_id.as_deref())?;
    let wanted = Wanted::parse(&input.object_attributes)?;
    let max_parts = multipart::page_size(input.max_parts, "max-parts")?;
    let (_, obj, _) = stat_object(dal, &input.bucket, &input.key).await?;
    precondition::check(
        &Conditions {
            if_match: if_match.as_ref(),
            if_none_match: if_none_match.as_ref(),
            if_modified_since: if_modified_since.as_ref(),
            if_unmodified_since: if_unmodified_since.as_ref(),
        },
        &obj.etag,
        obj.last_modified,
    )?;

    // Asking for the Checksum attribute is the opt-in that `x-amz-checksum-mode` is for GET, and
    // the attributes describe the whole object.
    let enabled = ChecksumMode::from_static(ChecksumMode::ENABLED);
    let checksum = returned_checksum(obj.checksum.clone(), Some(&enabled), true);
    let object_parts = if wanted.object_parts {
        object_parts(&obj, max_parts, input.part_number_marker)
    } else {
        None
    };
    let object_size = if wanted.object_size {
        Some(length(obj.content_length)?)
    } else {
        None
    };
    Ok(S3Response::new(GetObjectAttributesOutput {
        e_tag: wanted.etag.then(|| ETag::Strong(obj.etag.clone())),
        checksum: (wanted.checksum && checksum != Checksum::default()).then_some(checksum),
        object_parts,
        object_size,
        storage_class: wanted
            .storage_class
            .then(|| StorageClass::from_static(StorageClass::STANDARD)),
        last_modified: obj.last_modified.map(Timestamp::from),
        ..Default::default()
    }))
}

/// A conditional header, refused as s3s refuses a malformed or repeated one on GET: acting
/// on a condition other than the one sent could return what the client meant to exclude.
fn condition_header<T>(
    headers: &HeaderMap,
    name: &HeaderName,
    parse: impl FnOnce(&str) -> Option<T>,
) -> S3Result<Option<T>> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    value
        .to_str()
        .ok()
        .filter(|_| values.next().is_none())
        .and_then(parse)
        .map(Some)
        .ok_or_else(|| s3_error!(InvalidArgument, "invalid header: {}", name.as_str()))
}

/// The attributes a GetObjectAttributes request names.
#[derive(Default)]
struct Wanted {
    etag: bool,
    checksum: bool,
    object_parts: bool,
    storage_class: bool,
    object_size: bool,
}

impl Wanted {
    /// s3s takes each `x-amz-object-attributes` header line as one name, but SDKs send the
    /// whole list comma-separated in one line, so each line is split here.
    fn parse(list: &[ObjectAttributes]) -> S3Result<Self> {
        let mut wanted = Self::default();
        for name in list.iter().flat_map(|a| a.as_str().split(',')) {
            let flag = match name.trim() {
                ObjectAttributes::ETAG => &mut wanted.etag,
                ObjectAttributes::CHECKSUM => &mut wanted.checksum,
                ObjectAttributes::OBJECT_PARTS => &mut wanted.object_parts,
                ObjectAttributes::STORAGE_CLASS => &mut wanted.storage_class,
                ObjectAttributes::OBJECT_SIZE => &mut wanted.object_size,
                _ => {
                    return Err(s3_error!(
                        InvalidArgument,
                        "Invalid attribute name specified."
                    ));
                }
            };
            *flag = true;
        }
        Ok(wanted)
    }
}

/// Only an object assembled from parts has any (design.md "Part layout"), as on S3. Part
/// sizes are listed whenever the stored layout gives them, as ceph s3-tests expect; AWS lists
/// a general purpose bucket's parts only when they carry checksums. No part has a checksum
/// here, because none is stored per part. Paging follows ListParts.
fn object_parts(
    obj: &ObjectMeta,
    max_parts: usize,
    marker: Option<PartNumberMarker>,
) -> Option<GetObjectAttributesParts> {
    let layout = obj.parts?;
    let len = obj.content_length;
    let total_parts_count = PartsCount::try_from(layout.count).ok();
    // Whether boundaries are known does not depend on the part. Without them only the count is
    // reported; guessed sizes would misstate the parts.
    if layout.range(layout.count, len).is_none() {
        return Some(GetObjectAttributesParts {
            total_parts_count,
            ..Default::default()
        });
    }
    let after = i64::from(marker.unwrap_or(0));
    let mut numbers = (1..=layout.count).filter(|&n| i64::from(n) > after);
    let listed: Vec<u32> = numbers.by_ref().take(max_parts).collect();
    let is_truncated = max_parts > 0 && numbers.next().is_some();
    let parts = listed
        .iter()
        .filter_map(|&n| {
            let range = layout.range(n, len)?;
            Some(ObjectPart {
                part_number: Some(n as i32),
                size: i64::try_from(range.end - range.start).ok(),
                ..Default::default()
            })
        })
        .collect();
    Some(GetObjectAttributesParts {
        total_parts_count,
        part_number_marker: marker,
        next_part_number_marker: listed.last().map(|&n| n as i32),
        max_parts: Some(max_parts as i32),
        is_truncated: Some(is_truncated),
        parts: Some(parts),
    })
}

/// Resolves an object for GET and HEAD, once the bucket and key are known to be valid.
/// Directories (folder markers and prefixes) are not objects.
/// The object's backend path and metadata, and the backend's own ETag, which names the version
/// that was stat'ed.
pub(super) async fn stat_object(
    dal: &S3Dal,
    bucket: &str,
    key: &str,
) -> S3Result<(String, ObjectMeta, Option<String>)> {
    let path = naming::object_path(bucket, key);
    let m = dal.op.stat(&path).await.map_err(read_err)?;
    if !m.is_file() {
        return Err(s3_error!(NoSuchKey));
    }
    let version = m.etag().map(str::to_owned);
    Ok((path, meta::resolve(&dal.profile, &m), version))
}

fn read_err(err: opendal::Error) -> s3s::S3Error {
    map(err, Ctx::ObjectRead)
}

fn length(n: u64) -> S3Result<i64> {
    i64::try_from(n).map_err(|_| s3_error!(InternalError, "object size exceeds i64"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored(header: &str) -> Option<String> {
        stored_content_encoding(Some(header.to_owned()))
    }

    #[test]
    fn aws_chunked_framing_is_not_stored_as_content_encoding() {
        // Otherwise every streamed SDK upload would come back announcing an encoding that no
        // client can undo, and a real encoding sent alongside it would be garbled.
        assert_eq!(stored("aws-chunked"), None);
        assert_eq!(stored("aws-chunked,gzip").as_deref(), Some("gzip"));
        assert_eq!(stored("gzip, AWS-Chunked").as_deref(), Some("gzip"));
        assert_eq!(stored_content_encoding(None), None);
        // Anything else is kept verbatim, as S3 does.
        assert_eq!(stored("br, gzip").as_deref(), Some("br, gzip"));
    }
}

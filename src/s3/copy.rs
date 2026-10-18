//! CopyObject, and the source handling UploadPartCopy shares with it: resolving
//! `x-amz-copy-source`, evaluating `x-amz-copy-source-if-*`, and streaming the source bytes.
//!
//! A copy reads the source through s3dal and writes the destination like a PUT, rather than
//! using OpenDAL's `copy`, which cannot set metadata (design.md "Copy and batch delete").

use std::ops::Range;
use std::time::SystemTime;

use base64_simd::STANDARD;
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use opendal::Buffer;
use s3s::dto::*;
use s3s::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};

use super::precondition::{self, Conditions};
use super::store::{self, Dest, Precondition};
use super::upload::{BodyCheck, BodyHeaders};
use super::{S3Dal, naming, unsupported};
use crate::checksum::{self, ALGORITHMS};
use crate::error::{Ctx, map};
use crate::meta::{self, Attrs, Computed, ObjectMeta};

/// The object `x-amz-copy-source` names, as it was when its conditions were evaluated.
pub(super) struct Source {
    path: String,
    pub meta: ObjectMeta,
}

impl Source {
    /// Resolves the source and evaluates `cond` against it. For a copy, AWS answers every failed
    /// source condition with 412, including the ones a GET would answer with 304: there is no
    /// cached copy to keep using, only a write that did not happen.
    pub(super) async fn resolve(
        dal: &S3Dal,
        source: &CopySource,
        cond: &Conditions<'_>,
    ) -> S3Result<Self> {
        let CopySource::Bucket {
            bucket,
            key,
            version_id,
        } = source
        else {
            return Err(s3_error!(
                NotImplemented,
                "only a bucket/key copy source is supported"
            ));
        };
        naming::require_bucket(dal, bucket).await?;
        naming::require_null_version(version_id.as_deref())?;
        naming::validate_key(key)?;
        let path = naming::object_path(bucket, key);
        let m = dal.op.stat(&path).await.map_err(read_err)?;
        // Folder markers and prefixes are directories, not objects.
        if !m.is_file() {
            return Err(s3_error!(NoSuchKey));
        }
        let meta = meta::resolve(&dal.profile, &m);
        precondition::check(cond, &meta.etag, meta.last_modified).map_err(|err| {
            if *err.code() == S3ErrorCode::NotModified {
                s3_error!(
                    PreconditionFailed,
                    "At least one of the pre-conditions you specified did not hold"
                )
            } else {
                err
            }
        })?;
        Ok(Self { path, meta })
    }

    /// What the bytes [`Self::read`] returns for `range` must match, in the form an upload's
    /// body is checked against: the range's length and, for the whole object, its MD5 (when the
    /// ETag is one) and stored checksums. Nothing records a digest of a smaller range, so only
    /// its length can be checked. A composite checksum is a digest of part checksums, not of
    /// the bytes, so it cannot be checked either.
    pub(super) fn expected(
        &self,
        range: Range<u64>,
        algorithm: Option<ChecksumAlgorithm>,
    ) -> S3Result<BodyHeaders> {
        let whole = range == (0..self.meta.content_length);
        let len = i64::try_from(range.end - range.start)
            .map_err(|_| s3_error!(InternalError, "object size exceeds i64"))?;
        Ok(BodyHeaders {
            content_length: Some(len),
            content_md5: etag_md5(&self.meta.etag)
                .filter(|_| whole)
                .map(|digest| STANDARD.encode_to_string(digest)),
            checksum: if whole {
                let mut checksum = self.meta.checksum.clone();
                for (_, field, _) in ALGORITHMS {
                    let value = field(&mut checksum);
                    if value.as_deref().is_some_and(checksum::is_composite) {
                        *value = None;
                    }
                }
                checksum
            } else {
                Checksum::default()
            },
            algorithm,
        })
    }

    /// Streams `range` of the source. The bytes are read after the conditions were evaluated,
    /// so callers verify them against [`Self::expected`] to catch a source replaced in between;
    /// [`source_changed`] turns a mismatch into the right error.
    pub(super) async fn read(
        &self,
        dal: &S3Dal,
        range: Range<u64>,
    ) -> S3Result<BoxStream<'static, S3Result<Bytes>>> {
        // An empty range is never sent to the backend: OpenDAL reads an empty range as "to the
        // end".
        if range.is_empty() {
            return Ok(stream::empty().boxed());
        }
        let reader = dal.op.reader(&self.path).await.map_err(read_err)?;
        let chunks = reader.into_stream(range).await.map_err(read_err)?;
        Ok(chunks.map_ok(|b| b.to_bytes()).map_err(read_err).boxed())
    }
}

pub(super) async fn copy_object(
    dal: &S3Dal,
    req: S3Request<CopyObjectInput>,
) -> S3Result<S3Response<CopyObjectOutput>> {
    let mut input = req.input;
    unsupported::refuse_new_object_features!(input)?;
    unsupported::refuse_sse_c(
        input.copy_source_sse_customer_algorithm.as_ref(),
        input.copy_source_sse_customer_key.as_ref(),
        input.copy_source_sse_customer_key_md5.as_ref(),
    )?;
    let replace = replaces(
        input.metadata_directive.as_ref().map(|d| d.as_str()),
        "metadata",
    )?;
    // Tags are refused above, so either directive leaves the copy without tags, as the source
    // has none.
    replaces(
        input.tagging_directive.as_ref().map(|d| d.as_str()),
        "tagging",
    )?;

    naming::require_bucket(dal, &input.bucket).await?;
    naming::validate_key(&input.key)?;
    if input.key.ends_with('/') {
        // A key ending in '/' is a directory here, which cannot hold content or metadata.
        return Err(s3_error!(
            NotImplemented,
            "copying onto a folder marker key (ending in '/') is not supported"
        ));
    }
    let cond = Precondition {
        if_match: input.if_match.as_ref(),
        if_none_match: input.if_none_match.as_ref(),
    };
    cond.validate()?;
    let replaced = if replace {
        let user_metadata = input.metadata.take().unwrap_or_default();
        meta::check_user_metadata(&user_metadata)?;
        Some(Attrs {
            content_type: input.content_type.take(),
            cache_control: input.cache_control.take(),
            content_disposition: input.content_disposition.take(),
            content_encoding: input.content_encoding.take(),
            content_language: input.content_language.take(),
            expires: input.expires.take(),
            user_metadata,
        })
    } else {
        None
    };
    let onto_itself = matches!(
        &input.copy_source,
        CopySource::Bucket { bucket, key, .. } if **bucket == *input.bucket && **key == *input.key
    );
    if onto_itself && !replace {
        return Err(s3_error!(
            InvalidRequest,
            "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes."
        ));
    }

    let source_cond = Conditions {
        if_match: input.copy_source_if_match.as_ref(),
        if_none_match: input.copy_source_if_none_match.as_ref(),
        if_modified_since: input.copy_source_if_modified_since.as_ref(),
        if_unmodified_since: input.copy_source_if_unmodified_since.as_ref(),
    };
    let source = Source::resolve(dal, &input.copy_source, &source_cond).await?;
    let src = &source.meta;
    let attrs = replaced.unwrap_or_else(|| src.attrs.clone());
    let profile = &dal.profile;
    let len = src.content_length;

    // An MD5 ETag is known before the write (C1), so it is stored with the copy and the stream
    // is verified against it. Stored checksums are carried and verified the same way; the part
    // layout is not, because a copy is a single part.
    let headers = source.expected(0..len, input.checksum_algorithm.take())?;
    let buffered = !profile.native_etag
        && headers.content_md5.is_none()
        && len <= dal.settings.etag_buffer_limit;
    let mut check = BodyCheck::new(&headers, buffered)?;
    let dest = Dest::new(dal, &input.bucket, &input.key);
    dest.check(cond).await?;
    let mut body = source.read(dal, 0..len).await?;
    let no_trailer = |_: &str| None;

    let (etag, checksum, closed) = if buffered {
        let mut chunks = Vec::new();
        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            check.update(&chunk).map_err(source_changed)?;
            chunks.push(chunk);
        }
        let verified = check.finish(no_trailer).map_err(source_changed)?;
        let md5 = verified.md5_hex.clone().unwrap_or_default();
        let computed = Computed {
            md5_hex: Some(md5.clone()),
            checksum: verified.checksum.clone(),
            parts: None,
        };
        let opts = meta::write_options(profile, attrs, computed);
        let closed = dest.write_all(opts, Buffer::from(chunks), cond).await?;
        (Some(md5), verified.checksum, closed)
    } else {
        let stored_md5 = check.content_md5().map(str::to_owned);
        let computed = Computed {
            md5_hex: stored_md5.clone(),
            checksum: check.declared_checksum(),
            parts: None,
        };
        let mut opts = meta::write_options(profile, attrs, computed);
        opts.chunk = store::chunk_for(&dal.op, len);
        dest.send_whole_up_to_limit(&mut opts);
        let (verified, closed) = dest
            .write_stream(opts, &mut body, &mut check, no_trailer, cond)
            .await
            .map_err(source_changed)?;
        let etag = stored_md5.filter(|_| !profile.native_etag);
        (etag, verified.checksum, closed)
    };

    let last_modified = match closed.last_modified() {
        Some(t) => Some(SystemTime::from(t)),
        None => dal
            .op
            .stat(&dest.path)
            .await
            .map_err(read_err)?
            .last_modified()
            .map(SystemTime::from),
    };
    let etag = match etag {
        Some(etag) => etag,
        None => dest.written_etag(closed, len).await?,
    };

    Ok(S3Response::new(CopyObjectOutput {
        copy_object_result: Some(CopyObjectResult {
            e_tag: Some(ETag::Strong(etag)),
            last_modified: last_modified.map(Timestamp::from),
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
        }),
        ..Default::default()
    }))
}

/// Reads a `COPY` / `REPLACE` directive; `COPY` is the default.
fn replaces(directive: Option<&str>, name: &str) -> S3Result<bool> {
    match directive {
        None | Some(MetadataDirective::COPY) => Ok(false),
        Some(MetadataDirective::REPLACE) => Ok(true),
        Some(_) => Err(S3Error::with_message(
            S3ErrorCode::InvalidArgument,
            format!("Unknown {name} directive."),
        )),
    }
}

/// The digest behind a content-MD5 ETag (32 hex digits). Multipart and synthesized ETags
/// contain '-', so they never qualify.
fn etag_md5(etag: &str) -> Option<[u8; 16]> {
    if etag.len() != 32 || !etag.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let mut digest = [0; 16];
    for (i, byte) in digest.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&etag[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(digest)
}

/// The source's bytes did not match its length, MD5 or stored checksums. Either it was
/// replaced while being copied, or it was modified behind s3dal's back and its recorded digests
/// are stale. The copy was aborted. This is not the client's fault, and a retry succeeds in
/// the first case, so it is a retryable 500 rather than the 400 a client-sent digest gets.
pub(super) fn source_changed(err: S3Error) -> S3Error {
    match err.code() {
        S3ErrorCode::BadDigest | S3ErrorCode::IncompleteBody => {
            tracing::warn!(error = %err, "copy source changed or has stale digests; copy aborted");
            S3Error::with_message(
                S3ErrorCode::InternalError,
                "The source object does not match its recorded size, ETag or checksum; it may have changed during the copy.",
            )
        }
        _ => err,
    }
}

fn read_err(err: opendal::Error) -> S3Error {
    map(err, Ctx::ObjectRead)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_32_hex_etags_are_content_md5s() {
        // A multipart or synthesized ETag carried over as an MD5 would make every copy of it
        // fail verification; a real MD5 not recognized would lose the copy its MD5 ETag.
        let md5 = meta::md5_hex(b"x");
        assert_eq!(etag_md5(&md5).map(|d| meta::hex(&d)), Some(md5.clone()));
        assert_eq!(
            etag_md5(&md5.to_ascii_uppercase()).map(|d| meta::hex(&d)),
            Some(md5)
        );
        for not_md5 in [
            "d41d8cd98f00b204e9800998ecf8427e-2",
            "d41d8cd98f00b204e9800998ecf8427",
            "+41d8cd98f00b204e9800998ecf8427e",
            "z41d8cd98f00b204e9800998ecf8427e",
        ] {
            assert_eq!(etag_md5(not_md5), None, "{not_md5}");
        }
    }

    #[test]
    fn digest_mismatches_while_copying_become_retryable() {
        // A source replaced mid-copy is a transient server-side condition, not a bad request.
        let err = source_changed(s3_error!(BadDigest));
        assert_eq!(*err.code(), S3ErrorCode::InternalError);
        let err = source_changed(s3_error!(IncompleteBody));
        assert_eq!(*err.code(), S3ErrorCode::InternalError);
        // Destination conditions and write failures keep their own codes.
        let err = source_changed(s3_error!(PreconditionFailed));
        assert_eq!(*err.code(), S3ErrorCode::PreconditionFailed);
    }
}

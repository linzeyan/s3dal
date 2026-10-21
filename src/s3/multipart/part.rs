//! UploadPart and UploadPartCopy, which share the part writer.

use std::ops::Range;
use std::time::SystemTime;

use bytes::Bytes;
#[cfg(unix)]
use bytes::BytesMut;
use futures::{Stream, StreamExt};
use opendal::options::WriteOptions;
#[cfg(unix)]
use opendal::raw::new_std_io_error;
use s3s::dto::*;
use s3s::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use uuid::Uuid;

use super::record::{self, PartManifest};
#[cfg(unix)]
use super::slots;
use super::{parse_upload_id, upload_dir};
use crate::S3Dal;
use crate::backend::without_stalling;
use crate::checksum::ALGORITHMS;
use crate::error::{Ctx, map};
use crate::s3::copy::{self, Source};
#[cfg(unix)]
use crate::s3::fs;
use crate::s3::naming;
use crate::s3::precondition::Conditions;
use crate::s3::store;
use crate::s3::unsupported::refuse_sse_c;
use crate::s3::upload::{self, BodyCheck, BodyHeaders, Verified, body_error, trailer_lookup};

/// S3's limits for one part.
pub(super) const MAX_PART_NUMBER: u32 = 10_000;
const MAX_PART_SIZE: u64 = 5 * 1024 * 1024 * 1024;

pub(in crate::s3) async fn upload_part(
    dal: &S3Dal,
    req: S3Request<UploadPartInput>,
) -> S3Result<S3Response<UploadPartOutput>> {
    let trailers = req.trailing_headers;
    let mut input = req.input;
    naming::require_bucket(dal, &input.bucket).await?;
    let id = parse_upload_id(&input.upload_id)?;
    refuse_sse_c(
        input.sse_customer_algorithm.as_ref(),
        input.sse_customer_key.as_ref(),
        input.sse_customer_key_md5.as_ref(),
    )?;
    let part = part_number(input.part_number).ok_or_else(invalid_part_number)?;
    if input
        .content_length
        .is_some_and(|n| u64::try_from(n).is_ok_and(|n| n > MAX_PART_SIZE))
    {
        return Err(entity_too_large());
    }
    let upload = record::load_upload(dal, &input.bucket, id, &input.key).await?;

    let checksum = Checksum {
        checksum_crc32: input.checksum_crc32.take(),
        checksum_crc32c: input.checksum_crc32c.take(),
        checksum_crc64nvme: input.checksum_crc64nvme.take(),
        checksum_sha1: input.checksum_sha1.take(),
        checksum_sha256: input.checksum_sha256.take(),
        checksum_sha512: input.checksum_sha512.take(),
        checksum_md5: input.checksum_md5.take(),
        checksum_xxhash64: input.checksum_xxhash64.take(),
        checksum_xxhash3: input.checksum_xxhash3.take(),
        checksum_xxhash128: input.checksum_xxhash128.take(),
        ..Default::default()
    };
    let headers = BodyHeaders {
        content_length: input.content_length,
        content_md5: upload::content_md5(input.content_md5.take(), &req.headers),
        algorithm: part_algorithm(
            input.checksum_algorithm.take(),
            &checksum,
            upload.checksum_algorithm,
        )?,
        checksum,
    };
    let Some(body) = input.body.take() else {
        return Err(s3_error!(IncompleteBody));
    };
    let body = body.map(|chunk| chunk.map_err(body_error));
    let manifest = write_part(
        dal,
        &input.bucket,
        id,
        part,
        &headers,
        body,
        trailer_lookup(trailers.as_ref()),
    )
    .await?;

    let checksum = record::checksum_of(&manifest.checksums);
    Ok(S3Response::new(UploadPartOutput {
        e_tag: Some(ETag::Strong(manifest.md5)),
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

/// The algorithm a part is hashed with. In an upload created with one, every part must bring a
/// checksum of it, by name or by the value it `sent`, as AWS requires: Complete checks each
/// part's value as the client computed it and derives the object's checksum from them all, so a
/// client that sends none would only find out after its last part. A part of another algorithm
/// could not contribute to that checksum either. SDKs name the algorithm of a header value only
/// in `x-amz-sdk-checksum-algorithm`, which s3s does not read, so the value counts too.
fn part_algorithm(
    named: Option<ChecksumAlgorithm>,
    sent: &Checksum,
    upload: Option<String>,
) -> S3Result<Option<ChecksumAlgorithm>> {
    if let Some(upload) = &upload {
        let mut sent = sent.clone();
        let used: Vec<&str> = named
            .iter()
            .map(ChecksumAlgorithm::as_str)
            .chain(
                ALGORITHMS
                    .into_iter()
                    .filter_map(|(name, field, _)| field(&mut sent).is_some().then_some(name)),
            )
            .collect();
        let other = match used.iter().find(|used| *used != upload) {
            Some(other) => Some(other.to_ascii_lowercase()),
            None if used.is_empty() => Some("null".to_owned()),
            None => None,
        };
        if let Some(other) = other {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidRequest,
                format!(
                    "Checksum Type mismatch occurred, expected checksum Type: {}, actual checksum Type: {other}",
                    upload.to_ascii_lowercase(),
                ),
            ));
        }
    }
    Ok(named.or_else(|| upload.map(ChecksumAlgorithm::from)))
}

/// Copies an object, or a range of it, into a part. The bytes pass through s3dal like an
/// uploaded body, so the part gets the same manifest, MD5 ETag and checksums as UploadPart's.
pub(in crate::s3) async fn upload_part_copy(
    dal: &S3Dal,
    req: S3Request<UploadPartCopyInput>,
) -> S3Result<S3Response<UploadPartCopyOutput>> {
    let input = req.input;
    naming::require_bucket(dal, &input.bucket).await?;
    let id = parse_upload_id(&input.upload_id)?;
    refuse_sse_c(
        input.sse_customer_algorithm.as_ref(),
        input.sse_customer_key.as_ref(),
        input.sse_customer_key_md5.as_ref(),
    )?;
    refuse_sse_c(
        input.copy_source_sse_customer_algorithm.as_ref(),
        input.copy_source_sse_customer_key.as_ref(),
        input.copy_source_sse_customer_key_md5.as_ref(),
    )?;
    let part = part_number(input.part_number).ok_or_else(invalid_part_number)?;
    let upload = record::load_upload(dal, &input.bucket, id, &input.key).await?;

    let source_cond = Conditions {
        if_match: input.copy_source_if_match.as_ref(),
        if_none_match: input.copy_source_if_none_match.as_ref(),
        if_modified_since: input.copy_source_if_modified_since.as_ref(),
        if_unmodified_since: input.copy_source_if_unmodified_since.as_ref(),
    };
    let source = Source::resolve(dal, &input.copy_source, &source_cond).await?;
    let len = source.meta.content_length;
    let range = match &input.copy_source_range {
        Some(header) => copy_source_range(header, len)?,
        None => 0..len,
    };
    if range.end - range.start > MAX_PART_SIZE {
        return Err(entity_too_large());
    }
    // As for UploadPart, the part is hashed with the upload's algorithm so Complete can check
    // the value a client computed for it.
    let algorithm = upload.checksum_algorithm.map(ChecksumAlgorithm::from);
    let headers = source.expected(range.clone(), algorithm)?;
    let body = source.read(dal, range).await?;
    let manifest = write_part(dal, &input.bucket, id, part, &headers, body, |_| None)
        .await
        .map_err(copy::source_changed)?;

    let checksum = record::checksum_of(&manifest.checksums);
    Ok(S3Response::new(UploadPartCopyOutput {
        copy_part_result: Some(CopyPartResult {
            e_tag: Some(ETag::Strong(manifest.md5)),
            last_modified: Some(Timestamp::from(manifest.last_modified)),
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
        }),
        ..Default::default()
    }))
}

/// `x-amz-copy-source-range` of an object of `len` bytes. Unlike `Range`, S3 accepts only the
/// form `bytes=first-last`: anything else is `InvalidArgument`, and a range that does not lie
/// within the object is `InvalidRange`, as s3-tests expect.
fn copy_source_range(header: &str, len: u64) -> S3Result<Range<u64>> {
    let offset = |s: &str| {
        // `parse` alone would also take a sign, as in `+1`.
        (!s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
            .then(|| s.parse::<u64>().ok())
            .flatten()
    };
    let parsed = header
        .strip_prefix("bytes=")
        .and_then(|r| r.split_once('-'))
        .and_then(|(first, last)| Some((offset(first)?, offset(last)?)))
        .filter(|(first, last)| first <= last);
    let Some((first, last)) = parsed else {
        return Err(s3_error!(
            InvalidArgument,
            "The x-amz-copy-source-range value must be of the form bytes=first-last where first and last are the zero-based offsets of the first and last bytes to copy"
        ));
    };
    if last >= len {
        return Err(s3_error!(
            InvalidRange,
            "The requested range is not satisfiable"
        ));
    }
    Ok(first..last + 1)
}

/// `n` as a part number S3 accepts.
pub(super) fn part_number(n: i32) -> Option<u32> {
    u32::try_from(n)
        .ok()
        .filter(|n| (1..=MAX_PART_NUMBER).contains(n))
}

/// Streams `body` into part `part` of upload `id`, verifying it as `headers` describe, then
/// commits it by writing its manifest. A body that fails any check is aborted and its manifest
/// never written, so the part a previous upload committed stays in place. Any byte stream will
/// do, so UploadPartCopy can feed it a source object.
pub(super) async fn write_part<S>(
    dal: &S3Dal,
    bucket: &str,
    id: Uuid,
    part: u32,
    headers: &BodyHeaders,
    mut body: S,
    trailer: impl Fn(&str) -> Option<String>,
) -> S3Result<PartManifest>
where
    S: Stream<Item = S3Result<Bytes>> + Unpin,
{
    // The part's MD5 is its ETag, so it is always computed.
    let mut check = BodyCheck::new(headers, true)?;
    let dir = upload_dir(bucket, id);
    #[cfg(unix)]
    if let Some(slot) = open_slot(dal, &dir, part, headers).await {
        let (offset, claim) = (slot.offset, slot.claim.clone());
        let verified = match write_slot(slot, &mut check, &mut body, trailer).await {
            Ok(verified) => verified,
            Err(err) => {
                // No manifest names the slot: this upload claimed it, and no other writes it.
                // Given up, it takes a retry of the part, which would otherwise be written
                // apart and make Complete copy the whole object.
                let _ = fs::blocking(move || Ok(std::fs::remove_file(claim))).await;
                return Err(err);
            }
        };
        let data = slots::OBJECT_FILE.to_owned();
        return commit(dal, &dir, part, data, offset, check.received(), verified).await;
    }
    let data = format!("part-{part:05}-{}", Uuid::new_v4().hyphenated());
    let path = format!("{dir}{data}");
    let opts = store::chunked(&dal.op, WriteOptions::default());
    let mut w = dal.op.writer_options(&path, opts).await.map_err(internal)?;
    let streamed = async {
        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            check.update(&chunk)?;
            // A body without a declared length is only bounded here.
            if check.received() > MAX_PART_SIZE {
                return Err(entity_too_large());
            }
            w.write(chunk).await.map_err(internal)?;
        }
        let verified = check.finish(trailer)?;
        without_stalling(&dal.op, w.close())
            .await
            .map_err(internal)?;
        Ok(verified)
    }
    .await;
    let verified = match streamed {
        Ok(verified) => verified,
        Err(err) => {
            if let Err(abort) = w.abort().await {
                tracing::warn!(path, error = %abort, "cannot abort part upload; temporary data may remain");
            }
            return Err(err);
        }
    };
    commit(dal, &dir, part, data, 0, check.received(), verified).await
}

/// Commits a part whose `size` bytes lie from `offset` in `data`.
async fn commit(
    dal: &S3Dal,
    dir: &str,
    part: u32,
    data: String,
    offset: u64,
    size: u64,
    verified: Verified,
) -> S3Result<PartManifest> {
    let manifest = PartManifest {
        data,
        offset,
        size,
        md5: verified
            .md5_hex
            .expect("BodyCheck hashes MD5 when asked to"),
        checksums: record::checksum_map(verified.checksum),
        last_modified: SystemTime::now(),
    };
    record::store_manifest(dal, dir, part, &manifest).await?;
    Ok(manifest)
}

/// On fs, the part's slot in the upload's object file (see `slots`), for a body whose length
/// is declared; `None` sends the part to a file of its own.
#[cfg(unix)]
async fn open_slot(
    dal: &S3Dal,
    dir: &str,
    part: u32,
    headers: &BodyHeaders,
) -> Option<slots::Slot> {
    let len = u64::try_from(headers.content_length?).ok()?;
    if !fs::is_fs(&dal.op) {
        return None;
    }
    let path = std::path::PathBuf::from(dal.op.info().root()).join(dir);
    let opened = fs::blocking(move || slots::open(&path, part, len).map_err(new_std_io_error));
    match opened.await {
        Ok(slot) => slot,
        // A slot only spares Complete a copy. A filesystem that refuses what slots need (hard
        // links, locks) still takes the part as it took every part before.
        Err(err) => {
            tracing::warn!(dir, error = %err, "cannot open the part's slot; writing it apart");
            None
        }
    }
}

/// Streams `body` into `slot`, checked as `check` says, in the batches the fs writer would
/// write, then syncs it as that writer does on close: the manifest commits the part only once
/// its data is on disk. `check` refuses a byte past the declared length, so the part never
/// runs into the next slot.
#[cfg(unix)]
async fn write_slot<S>(
    slot: slots::Slot,
    check: &mut BodyCheck,
    body: &mut S,
    trailer: impl Fn(&str) -> Option<String>,
) -> S3Result<Verified>
where
    S: Stream<Item = S3Result<Bytes>> + Unpin,
{
    use std::os::unix::fs::FileExt;

    let file = std::sync::Arc::new(slot.file);
    let write = |batch: Bytes, at: u64, sync: bool| {
        let file = file.clone();
        fs::blocking(move || {
            let written = file.write_all_at(&batch, at);
            let synced = written.and_then(|()| if sync { file.sync_data() } else { Ok(()) });
            synced.map_err(new_std_io_error)
        })
    };
    let mut at = slot.offset;
    let mut batch = BytesMut::with_capacity(store::UNSIZED_WRITE_CHUNK);
    while let Some(chunk) = body.next().await {
        let chunk = chunk?;
        check.update(&chunk)?;
        // Written before the chunk would overflow it: a batch grown from empty, or past its
        // capacity, is copied on every doubling and ends up near twice the batch size.
        if !batch.is_empty() && batch.len() + chunk.len() > store::UNSIZED_WRITE_CHUNK {
            let next = BytesMut::with_capacity(store::UNSIZED_WRITE_CHUNK);
            let full = std::mem::replace(&mut batch, next).freeze();
            let len = full.len() as u64;
            write(full, at, false).await.map_err(internal)?;
            at += len;
        }
        batch.extend_from_slice(&chunk);
    }
    let verified = check.finish(trailer)?;
    write(batch.freeze(), at, true).await.map_err(internal)?;
    Ok(verified)
}

fn internal(err: opendal::Error) -> s3s::S3Error {
    map(err, Ctx::Other)
}

fn invalid_part_number() -> s3s::S3Error {
    s3_error!(
        InvalidArgument,
        "Part number must be an integer between 1 and 10000, inclusive"
    )
}

fn entity_too_large() -> s3s::S3Error {
    s3_error!(
        EntityTooLarge,
        "Your proposed upload exceeds the maximum allowed size"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn part_numbers_are_limited_to_the_s3_range() {
        // The number becomes part of a file name; one outside S3's range must never get there.
        assert_eq!(part_number(1), Some(1));
        assert_eq!(part_number(10_000), Some(10_000));
        for bad in [0, -1, 10_001, i32::MIN] {
            assert_eq!(part_number(bad), None, "{bad}");
        }
    }

    #[test]
    fn a_part_brings_a_checksum_of_the_upload_algorithm() {
        // Complete checks every part's checksum of the upload's algorithm as the client
        // computed it, so a part that brings none, or one of another algorithm, is refused when
        // it arrives, as AWS does, not after the client has sent every other part.
        let named = |a: &'static str| Some(ChecksumAlgorithm::from_static(a));
        let crc32_value = Checksum {
            checksum_crc32: Some("DUoRhQ==".to_owned()),
            ..Default::default()
        };
        let none = Checksum::default();
        let used = |named, sent: &Checksum, upload: Option<&str>| {
            part_algorithm(named, sent, upload.map(str::to_owned))
                .map(|a| a.map(|a| a.as_str().to_owned()))
        };
        let err = used(None, &none, Some("CRC32")).unwrap_err();
        assert_eq!(*err.code(), s3s::S3ErrorCode::InvalidRequest);
        assert!(
            err.message()
                .unwrap()
                .ends_with("actual checksum Type: null"),
            "{err:?}"
        );
        assert_eq!(
            used(named("CRC32"), &none, Some("CRC32"))
                .unwrap()
                .as_deref(),
            Some("CRC32")
        );
        assert_eq!(
            used(None, &crc32_value, Some("CRC32")).unwrap().as_deref(),
            Some("CRC32")
        );
        assert_eq!(
            used(named("SHA256"), &crc32_value, None)
                .unwrap()
                .as_deref(),
            Some("SHA256")
        );
        assert_eq!(used(None, &none, None).unwrap(), None);
        for (named, sent, upload) in [
            (named("SHA256"), &none, "CRC32"),
            (None, &crc32_value, "SHA256"),
        ] {
            let err = used(named, sent, Some(upload)).unwrap_err();
            assert_eq!(*err.code(), s3s::S3ErrorCode::InvalidRequest);
        }
    }

    #[test]
    fn copy_source_ranges_take_only_the_first_last_form() {
        // Guessing at another form (a suffix, an open end, several ranges) would copy bytes
        // the client did not ask for into the part.
        assert_eq!(copy_source_range("bytes=0-4", 5).unwrap(), 0..5);
        assert_eq!(copy_source_range("bytes=2-2", 5).unwrap(), 2..3);
        for malformed in [
            "0-2",
            "bytes=0",
            "bytes=hello-world",
            "bytes=0-bar",
            "bytes=hello-",
            "bytes=-2",
            "bytes=0-2,3-4",
            "bytes=+0-2",
            "bytes=3-2",
        ] {
            assert_eq!(
                *copy_source_range(malformed, 5).unwrap_err().code(),
                s3s::S3ErrorCode::InvalidArgument,
                "{malformed}"
            );
        }
        for beyond in ["bytes=0-5", "bytes=5-9"] {
            assert_eq!(
                *copy_source_range(beyond, 5).unwrap_err().code(),
                s3s::S3ErrorCode::InvalidRange,
                "{beyond}"
            );
        }
        // An empty object has no byte a range could name.
        assert_eq!(
            *copy_source_range("bytes=0-0", 0).unwrap_err().code(),
            s3s::S3ErrorCode::InvalidRange
        );
    }
}

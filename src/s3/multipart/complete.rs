//! CompleteMultipartUpload. Everything that can be decided from the manifests is checked
//! before the response starts; assembling the object then runs behind s3s's keep-alive body,
//! which sends 200 at once and whitespace until the result (or an error document) is ready.

use std::time::SystemTime;

use futures::{StreamExt, TryFutureExt, TryStreamExt};
use hyper::header::HOST;
use md5::{Digest, Md5};
use opendal::options::{ComposeOptions, WriteOptions};
#[cfg(unix)]
use opendal::raw::new_std_io_error;
use opendal::{Metadata, Writer};
use s3s::dto::*;
use s3s::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use tracing::Instrument;
use uuid::Uuid;

use super::part::part_number;
use super::record::{self, Completed, PartManifest, Upload};
#[cfg(unix)]
use super::slots;
use super::{parse_upload_id, upload_dir};
use crate::S3Dal;
use crate::checksum::{self, ALGORITHMS};
use crate::meta::{self, Computed, PartLayout};
use crate::s3::store::{self, Dest, Precondition, write_err};
use crate::s3::unsupported::refuse_sse_c;
use crate::s3::{fs, naming};
use crate::server::{RequestId, Scheme};

/// Every part except the last must be at least this large.
pub(super) const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

/// Manifest reads in flight. A Complete may list 10000 parts, and reading their manifests one
/// by one would keep the client waiting before the response even starts.
const READ_CONCURRENCY: usize = 16;

pub(in crate::s3) async fn complete_multipart_upload(
    dal: &S3Dal,
    req: S3Request<CompleteMultipartUploadInput>,
) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
    let location = object_url(&req);
    let mut input = req.input;
    naming::require_bucket(dal, &input.bucket).await?;
    naming::validate_key(&input.key)?;
    let id = parse_upload_id(&input.upload_id)?;
    refuse_sse_c(
        input.sse_customer_algorithm.as_ref(),
        input.sse_customer_key.as_ref(),
        input.sse_customer_key_md5.as_ref(),
    )?;
    let sent = sent_checksum(&input)?;
    let cond = Precondition {
        if_match: input.if_match.as_ref(),
        if_none_match: input.if_none_match.as_ref(),
    };
    cond.validate()?;
    let upload = record::load_record(dal, &input.bucket, id, &input.key).await?;
    check_sent(&upload, sent.as_ref(), input.checksum_type.as_ref())?;
    let requested = input
        .multipart_upload
        .take()
        .and_then(|m| m.parts)
        .filter(|parts| !parts.is_empty())
        .ok_or_else(malformed_xml)?;

    if let Some(done) = &upload.completed {
        // The request already took effect, so its conditions are not evaluated again: the key
        // may hold this very object by now, and If-None-Match would refuse the retry for that.
        check_object_size(input.mpu_object_size, done.size)?;
        let listed = requested
            .iter()
            .map(|part| part.e_tag.as_ref().and_then(ETag::as_strong))
            .collect::<Option<Vec<&str>>>()
            .and_then(multipart_etag);
        return retried(
            &upload,
            done,
            listed.as_deref(),
            sent.as_ref(),
            input.bucket,
            input.key,
            location,
        )
        .map(S3Response::new);
    }

    let dir = upload_dir(&input.bucket, id);
    let parts = validate(dal, &dir, &requested).await?;
    // Like the ETag below, the object's checksum is stored with it (C1), so it is derived before
    // the write starts. The client's value is compared first: s3-tests expect BadDigest for a
    // wrong one even when the list also lacks the part checksums.
    let checksum = object_checksum(&upload, &parts, &requested)?;
    verify_sent(sent.as_ref(), checksum.as_deref())?;
    require_part_checksums(&upload, &requested)?;
    let sizes: Vec<u64> = parts.iter().map(|p| p.size).collect();
    let total: u64 = sizes.iter().sum();
    check_object_size(input.mpu_object_size, total)?;
    Dest::new(dal, &input.bucket, &input.key)
        .check(cond)
        .await?;

    // Decision C1: both are stored with the object, so both are known before the write starts.
    let etag = multipart_etag(parts.iter().map(|p| p.md5.as_str())).ok_or_else(|| {
        tracing::error!(dir, "corrupt part manifest: an MD5 is not 32 hex digits");
        s3_error!(InternalError)
    })?;
    let assembly = Assembly {
        dal: dal.clone(),
        etag,
        checksum,
        sent,
        layout: PartLayout::new(&sizes),
        bucket: input.bucket,
        key: input.key,
        location,
        id,
        dir,
        parts,
        total,
        if_match: input.if_match,
        if_none_match: input.if_none_match,
    };
    // The assembly runs while the 200's body is sent, after the request's span has closed and
    // its error documents were stamped (src/server.rs), so it carries both itself.
    let request_id = req.extensions.get::<RequestId>().cloned();
    let run = assembly.run().map_err(move |mut err| {
        if let Some(id) = &request_id {
            err.set_request_id(id.as_str());
        }
        err
    });
    // AWS finishes a Complete once it has started. s3s polls this future from the response
    // body, so a client that went away would drop the assembly at whatever it awaited: in the
    // copy, leaving the whole object behind as a temporary file, or in the commit, leaving the
    // object without its metadata. Spawned, it runs to the end, and a retry finds its record.
    let assembling = tokio::spawn(run.instrument(tracing::Span::current()));
    let joined = async move { assembling.await.map_err(S3Error::internal_error)? };
    Ok(S3Response::new(CompleteMultipartUploadOutput {
        future: Some(Box::pin(joined)),
        ..Default::default()
    }))
}

fn check_object_size(expected: Option<MpuObjectSize>, total: u64) -> S3Result<()> {
    if expected.is_some_and(|expected| u64::try_from(expected).ok() != Some(total)) {
        return Err(s3_error!(
            InvalidRequest,
            "x-amz-mp-object-size does not match the total size of the parts"
        ));
    }
    Ok(())
}

/// The answer to a Complete of an upload that is already completed: the original result when
/// the part ETags give the multipart ETag it was completed with (`listed`), as on AWS, which
/// compares the parts' ETags and nothing else. Any other list asks to complete the upload a
/// second time, differently; AWS answers that as an upload that no longer exists. A checksum of
/// the object the retry sends is checked against the one recorded, as the original's was.
fn retried(
    upload: &Upload,
    done: &Completed,
    listed: Option<&str>,
    sent: Option<&Sent>,
    bucket: String,
    key: String,
    location: Option<String>,
) -> S3Result<CompleteMultipartUploadOutput> {
    if listed != Some(done.parts.as_str()) {
        return Err(record::no_such_upload());
    }
    verify_sent(sent, done.checksum.as_deref())?;
    Ok(output(
        bucket,
        key,
        location,
        done.etag.clone(),
        upload,
        done.checksum.as_deref(),
    ))
}

/// The new object's URL, which AWS returns as Location and SDK upload helpers hand on (the JS
/// SDK's `Upload`, Go's `s3manager`). A Complete is sent to the object's own URL, in whichever
/// addressing style the client uses, so it is this request's URL without the upload id.
fn object_url<T>(req: &S3Request<T>) -> Option<String> {
    let Scheme(scheme) = *req.extensions.get::<Scheme>()?;
    // HTTP/2 names the host in the URI (`:authority`), HTTP/1.1 in the Host header.
    let host = match req.uri.authority() {
        Some(authority) => authority.as_str(),
        None => req.headers.get(HOST)?.to_str().ok()?,
    };
    Some(format!("{scheme}://{host}{}", req.uri.path()))
}

fn output(
    bucket: String,
    key: String,
    location: Option<String>,
    etag: String,
    upload: &Upload,
    checksum: Option<&str>,
) -> CompleteMultipartUploadOutput {
    let checksum = stored_checksum(upload, checksum);
    CompleteMultipartUploadOutput {
        bucket: Some(bucket),
        key: Some(key),
        location,
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
        checksum_type: checksum.checksum_type,
        ..Default::default()
    }
}

/// The object's checksum `value`, of the upload's algorithm and type.
fn stored_checksum(upload: &Upload, value: Option<&str>) -> Checksum {
    let mut checksum = Checksum::default();
    let algorithm = upload
        .checksum_algorithm
        .as_deref()
        .and_then(checksum::algorithm);
    if let (Some(value), Some((_, field, _))) = (value, algorithm) {
        *field(&mut checksum) = Some(value.to_owned());
        checksum.checksum_type = upload.checksum_type.clone().map(ChecksumType::from);
    }
    checksum
}

/// A checksum of the whole object sent with Complete: `(algorithm, value)`.
type Sent = (&'static str, String);

fn sent_checksum(input: &CompleteMultipartUploadInput) -> S3Result<Option<Sent>> {
    let mut sent = Checksum {
        checksum_crc32: input.checksum_crc32.clone(),
        checksum_crc32c: input.checksum_crc32c.clone(),
        checksum_crc64nvme: input.checksum_crc64nvme.clone(),
        checksum_sha1: input.checksum_sha1.clone(),
        checksum_sha256: input.checksum_sha256.clone(),
        checksum_sha512: input.checksum_sha512.clone(),
        checksum_md5: input.checksum_md5.clone(),
        checksum_xxhash64: input.checksum_xxhash64.clone(),
        checksum_xxhash3: input.checksum_xxhash3.clone(),
        checksum_xxhash128: input.checksum_xxhash128.clone(),
        ..Default::default()
    };
    let mut values = ALGORITHMS
        .into_iter()
        .filter_map(|(name, field, _)| Some((name, field(&mut sent).take()?)));
    let first = values.next();
    // An object has one checksum, so a second value could only contradict the first.
    if values.next().is_some() {
        return Err(s3_error!(
            InvalidRequest,
            "Expecting a single x-amz-checksum- header. Multiple checksum Types are not allowed."
        ));
    }
    Ok(first)
}

/// What the request says about the object's checksum, against what the upload was created
/// with, before any part is read. AWS documents a mismatched `x-amz-checksum-type` as
/// BadDigest, the header being an integrity check. A value of another algorithm than the
/// upload's names a checksum the object will not have, so it cannot be checked.
fn check_sent(
    upload: &Upload,
    sent: Option<&Sent>,
    sent_type: Option<&ChecksumType>,
) -> S3Result<()> {
    if let Some(sent_type) = sent_type
        && upload.checksum_type.as_deref() != Some(sent_type.as_str())
    {
        return Err(S3Error::with_message(
            S3ErrorCode::BadDigest,
            format!(
                "The x-amz-checksum-type you specified ({}) does not match the upload's ({}).",
                sent_type.as_str(),
                upload.checksum_type.as_deref().unwrap_or("none")
            ),
        ));
    }
    if let Some((name, _)) = sent
        && upload.checksum_algorithm.as_deref() != Some(*name)
    {
        let created = match &upload.checksum_algorithm {
            Some(algorithm) => format!("using a {} checksum", algorithm.to_ascii_lowercase()),
            None => "without a checksum algorithm".to_owned(),
        };
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            format!(
                "The upload was created {created}, so the object has no {} checksum to check.",
                name.to_ascii_lowercase()
            ),
        ));
    }
    Ok(())
}

/// The object's checksum, derived from the parts' as the upload's type says; `None` for an
/// upload without an algorithm. Every part was hashed with the upload's algorithm (see
/// `part_algorithm`), so a part without that checksum is refused rather than skipped: skipping
/// it would give a checksum of something other than the object.
fn object_checksum(
    upload: &Upload,
    parts: &[PartManifest],
    requested: &[CompletedPart],
) -> S3Result<Option<String>> {
    let (Some(algorithm), Some(checksum_type)) =
        (&upload.checksum_algorithm, &upload.checksum_type)
    else {
        return Ok(None);
    };
    let mut values = Vec::with_capacity(parts.len());
    for (manifest, part) in parts.iter().zip(requested) {
        let Some(value) = manifest.checksums.get(algorithm) else {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidPart,
                format!(
                    "Part {} has no {} checksum; upload it again.",
                    part.part_number.unwrap_or_default(),
                    algorithm.to_ascii_lowercase()
                ),
            ));
        };
        values.push((value.as_str(), manifest.size));
    }
    let derived = if checksum_type == ChecksumType::FULL_OBJECT {
        checksum::combine(algorithm, values)
    } else {
        checksum::composite(algorithm, values.into_iter().map(|(value, _)| value))
    };
    derived.map(Some).ok_or_else(|| {
        tracing::error!(
            algorithm,
            "corrupt part manifest: a checksum does not decode"
        );
        s3_error!(InternalError)
    })
}

/// The client's checksum of the object against the one s3dal derived. A composite one may be
/// sent with or without its `-<part count>`; with it, the count must match too.
fn verify_sent(sent: Option<&Sent>, derived: Option<&str>) -> S3Result<()> {
    let Some((name, value)) = sent else {
        return Ok(());
    };
    let matches = derived.is_some_and(|derived| {
        derived == value
            || derived
                .split_once('-')
                .is_some_and(|(digest, _)| digest == value)
    });
    if !matches {
        return Err(S3Error::with_message(
            S3ErrorCode::BadDigest,
            format!("The {name} you specified did not match the calculated checksum."),
        ));
    }
    Ok(())
}

/// For a COMPOSITE upload AWS requires each part's checksum in the list, so the client confirms
/// every value the object's checksum is built from. A FULL_OBJECT checksum stands on its own.
fn require_part_checksums(upload: &Upload, requested: &[CompletedPart]) -> S3Result<()> {
    if upload.checksum_type.as_deref() != Some(ChecksumType::COMPOSITE) {
        return Ok(());
    }
    let Some((name, field, _)) = upload
        .checksum_algorithm
        .as_deref()
        .and_then(checksum::algorithm)
    else {
        return Ok(());
    };
    for part in requested {
        if field(&mut listed_checksums(part)).is_none() {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidRequest,
                format!(
                    "The upload was created using a {} checksum. The complete request must include the checksum for each part. It was missing for part {} in the request.",
                    name.to_ascii_lowercase(),
                    part.part_number.unwrap_or_default()
                ),
            ));
        }
    }
    Ok(())
}

/// The committed manifests of the requested parts, in order, once every rule S3 applies holds.
async fn validate(
    dal: &S3Dal,
    dir: &str,
    requested: &[CompletedPart],
) -> S3Result<Vec<PartManifest>> {
    let mut previous = 0;
    for part in requested {
        let n = part.part_number.ok_or_else(malformed_xml)?;
        if n <= previous {
            return Err(s3_error!(
                InvalidPartOrder,
                "The list of parts was not in ascending order. Parts must be ordered by part number."
            ));
        }
        previous = n;
    }

    // A number outside S3's range cannot have been uploaded.
    let numbers: Vec<Option<u32>> = requested
        .iter()
        .map(|part| part.part_number.and_then(part_number))
        .collect();
    let found: Vec<Option<PartManifest>> = futures::stream::iter(numbers)
        .map(|n| async move {
            match n {
                Some(n) => record::load_manifest(dal, dir, n).await,
                None => Ok(None),
            }
        })
        .buffered(READ_CONCURRENCY)
        .try_collect()
        .await?;

    let last = requested.len() - 1;
    let mut parts = Vec::with_capacity(requested.len());
    for (i, (part, manifest)) in requested.iter().zip(found).enumerate() {
        let manifest = manifest
            .filter(|m| {
                part.e_tag
                    .as_ref()
                    .and_then(ETag::as_strong)
                    .is_some_and(|etag| etag == m.md5)
            })
            .ok_or_else(invalid_part)?;
        check_part_checksums(part, &manifest)?;
        if i < last && manifest.size < MIN_PART_SIZE {
            return Err(s3_error!(
                EntityTooSmall,
                "Your proposed upload is smaller than the minimum allowed object size."
            ));
        }
        parts.push(manifest);
    }
    Ok(parts)
}

/// Each checksum the client lists for a part must equal the one computed when the part was
/// uploaded. One that was never computed cannot be confirmed, so it does not match either.
fn check_part_checksums(part: &CompletedPart, manifest: &PartManifest) -> S3Result<()> {
    let mut sent = listed_checksums(part);
    for (name, field, _) in ALGORITHMS {
        if let Some(value) = field(&mut sent)
            && manifest.checksums.get(name) != Some(value)
        {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidPart,
                format!(
                    "The {name} checksum you specified for part {} did not match the uploaded part.",
                    part.part_number.unwrap_or_default()
                ),
            ));
        }
    }
    Ok(())
}

fn listed_checksums(part: &CompletedPart) -> Checksum {
    Checksum {
        checksum_crc32: part.checksum_crc32.clone(),
        checksum_crc32c: part.checksum_crc32c.clone(),
        checksum_crc64nvme: part.checksum_crc64nvme.clone(),
        checksum_sha1: part.checksum_sha1.clone(),
        checksum_sha256: part.checksum_sha256.clone(),
        checksum_sha512: part.checksum_sha512.clone(),
        checksum_md5: part.checksum_md5.clone(),
        checksum_xxhash64: part.checksum_xxhash64.clone(),
        checksum_xxhash3: part.checksum_xxhash3.clone(),
        checksum_xxhash128: part.checksum_xxhash128.clone(),
        ..Default::default()
    }
}

/// S3's multipart ETag: the MD5 of the concatenated binary part MD5s, then `-<part count>`.
/// Clients such as rclone and the aws CLI recompute it to verify uploads. `None` when a part
/// MD5 is not 32 hex digits.
fn multipart_etag<'a>(part_md5s: impl IntoIterator<Item = &'a str>) -> Option<String> {
    let mut md5 = Md5::new();
    let mut count = 0;
    for hex in part_md5s {
        md5.update(unhex(hex).filter(|digest| digest.len() == 16)?);
        count += 1;
    }
    Some(format!("{}-{count}", meta::hex(&md5.finalize())))
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    // `from_str_radix` alone would also take a sign, as in `+1`.
    if !s.len().is_multiple_of(2) || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

/// The validated upload, owned, so assembling it can outlive the request.
struct Assembly {
    dal: S3Dal,
    bucket: String,
    key: String,
    location: Option<String>,
    id: Uuid,
    dir: String,
    parts: Vec<PartManifest>,
    etag: String,
    checksum: Option<String>,
    sent: Option<Sent>,
    layout: PartLayout,
    total: u64,
    if_match: Option<ETagCondition>,
    if_none_match: Option<ETagCondition>,
}

impl Assembly {
    /// Streams the parts into the key and commits under the key lock. A failure leaves the
    /// upload as it was, so the client can retry the Complete or abort.
    async fn run(self) -> S3Result<CompleteMultipartUploadOutput> {
        let Self {
            dal,
            bucket,
            key,
            location,
            id,
            dir,
            parts,
            etag,
            checksum,
            sent,
            layout,
            total,
            if_match,
            if_none_match,
        } = self;
        // One Complete of an upload at a time in this process. A retry that arrives while the
        // first one is still assembling waits here and then answers from the record the first
        // one left, rather than writing the object again over whatever the key holds by then.
        let _upload_lock = dal.upload_locks.lock(&dir).await;
        // Validated before the response started; since then an Abort or the stale-upload
        // sweep may have removed the upload, or another Complete may have completed it.
        let mut upload = record::load_record(&dal, &bucket, id, &key).await?;
        if let Some(done) = &upload.completed {
            return retried(
                &upload,
                done,
                Some(&etag),
                sent.as_ref(),
                bucket,
                key,
                location,
            );
        }

        let dest = Dest::new(&dal, &bucket, &key);
        let computed = Computed {
            md5_hex: Some(etag.clone()),
            checksum: stored_checksum(&upload, checksum.as_deref()),
            parts: Some(layout),
        };
        let opts = meta::write_options(&dal.profile, upload.attrs.clone(), computed);
        let cond = Precondition {
            if_match: if_match.as_ref(),
            if_none_match: if_none_match.as_ref(),
        };
        let closed = match compose_options(&dal, &opts) {
            Some(compose) => compose_parts(&dest, &dir, &parts, compose, cond).await?,
            #[cfg(unix)]
            None if fs::is_fs(&dal.op) => assemble_on_fs(&dest, &dir, &parts, &opts, cond).await?,
            None => {
                let opts = WriteOptions {
                    chunk: store::chunk_for(&dal.op, total),
                    ..opts
                };
                let mut w = dest.open(opts, cond).await?;
                if let Err(err) = copy_parts(&dir, &parts, &mut w).await {
                    dest.abandon(&mut w).await;
                    return Err(err);
                }
                dest.commit(w, cond).await?
            }
        };

        // As for PutObject: a backend with native ETags reports its own, and the response must
        // name the ETag a following HEAD sees.
        let written = if dal.profile.native_etag {
            dest.written_etag(closed, total).await?
        } else {
            etag.clone()
        };
        upload.completed = Some(Completed {
            parts: etag,
            etag: written.clone(),
            checksum: checksum.clone(),
            size: total,
            at: SystemTime::now(),
        });
        finish(&dal, &bucket, id, &dir, &upload).await;
        Ok(output(
            bucket,
            key,
            location,
            written,
            &upload,
            checksum.as_deref(),
        ))
    }
}

/// Records the completion, then removes the parts. The object is committed already, so a
/// failure here costs only space, which the stale-upload sweep reclaims; it is logged, not
/// returned.
async fn finish(dal: &S3Dal, bucket: &str, id: Uuid, dir: &str, completed: &record::Upload) {
    if let Err(err) = record::store_upload(dal, bucket, id, completed).await {
        // The parts stay: a retry then finds the upload still active and assembles the same
        // object again, where without them it would fail with InvalidPart.
        tracing::warn!(dir, error = %err, "cannot record the completed upload");
        return;
    }
    let removed = async {
        let entries = dal.op.list(dir).await?;
        let parts = entries
            .into_iter()
            .filter(|e| e.metadata().is_file() && e.name() != record::UPLOAD_FILE)
            .map(|e| e.path().to_owned());
        dal.op.delete_iter(parts).await
    };
    if let Err(err) = removed.await {
        tracing::warn!(dir, error = %err, "cannot remove the parts of a completed upload");
    }
}

/// Appends each part with `copy_from`. Where the backend copies natively (S3's UploadPartCopy),
/// no part data passes through s3dal, and each part of at least the backend's minimum part size
/// becomes exactly one part of the backend's own multipart upload. The backend's ETag is then
/// the one the client computes from its parts, which matters because with `native_etag` it is
/// the only ETag (design.md "ETag"). A last part below that size is streamed into the final
/// write. Elsewhere `copy_from` streams through, as a read-and-write loop would.
///
/// On a backend with a minimum part size the writer must keep OpenDAL's default chunking, as
/// `store::chunked` leaves it: an explicit chunk size makes `copy_from` split every part at
/// that size.
async fn copy_parts(dir: &str, parts: &[PartManifest], w: &mut Writer) -> S3Result<()> {
    for part in parts {
        let path = format!("{dir}{}", part.data);
        w.copy_from(&path, part.offset..part.offset + part.size)
            .await
            .map_err(|err| copy_part_err(&path, err))?;
    }
    Ok(())
}

/// Assembles the parts on fs and commits the object as [`Dest::commit`] commits a write: the
/// condition checked and the file renamed into place under the key's lock. fs checks no
/// condition itself (`Profile::conditional_writes`), so that check is the only one, as it is
/// for a writer there. The file is the upload's own when its parts fill it (see `slots`),
/// otherwise one the parts are copied into (see [`fs::assemble`]). On failure nothing reaches
/// the key and the upload stays as it was.
#[cfg(unix)]
async fn assemble_on_fs(
    dest: &Dest<'_>,
    dir: &str,
    parts: &[PartManifest],
    opts: &WriteOptions,
    cond: Precondition<'_>,
) -> S3Result<Metadata> {
    let dal = dest.dal;
    let root = std::path::PathBuf::from(dal.op.info().root());
    let target = root.join(&dest.path);
    let user_metadata = opts.user_metadata.clone().unwrap_or_default();
    if slots::fill(parts)
        && let Some(closed) =
            take_in_place(dest, &root.join(dir), parts, &user_metadata, cond).await?
    {
        return Ok(closed);
    }
    #[cfg(test)]
    tests::record_fs_complete("copied");
    // Where OpenDAL's fs writer keeps its files, so the tmp sweep removes one a crash left.
    let tmp = root
        .join(crate::backend::RESERVED_ROOT)
        .join("tmp")
        .join(Uuid::new_v4().to_string());
    let sources: Vec<_> = parts
        .iter()
        .map(|p| {
            let path = root.join(format!("{dir}{}", p.data));
            (path, p.offset..p.offset + p.size)
        })
        .collect();
    let assembled = {
        let (tmp, target) = (tmp.clone(), target.clone());
        fs::blocking(move || {
            // As the writer does for every write, so a removed tmp directory is made again.
            fs::make_parents(&tmp)?;
            fs::make_parents(&target)?;
            Ok(fs::assemble(&tmp, &sources, &user_metadata))
        })
        .await
        .map_err(write_err)?
    };
    let closed = match assembled {
        Ok(closed) => closed,
        Err(err) => {
            store::prune_empty_parents(dal, dest.bucket, dest.key).await;
            return Err(match err {
                fs::AssembleError::Source(n, err) => {
                    copy_part_err(&format!("{dir}{}", parts[n].data), err)
                }
                fs::AssembleError::Write(err) => write_err(err),
            });
        }
    };
    let committed = {
        let _guard = dal.locks.lock(&dest.path).await;
        match dest.check(cond).await {
            Ok(()) => {
                let tmp = tmp.clone();
                fs::blocking(move || fs::rename_into_place(&tmp, &target))
                    .await
                    .map_err(write_err)
            }
            Err(err) => Err(err),
        }
    };
    if let Err(err) = committed {
        let _ = fs::blocking(move || Ok(std::fs::remove_file(&tmp))).await;
        store::prune_empty_parents(dal, dest.bucket, dest.key).await;
        return Err(err);
    }
    Ok(closed)
}

/// Commits the upload's object file in `upload`, which `parts` fill, as the object: on ext4,
/// which shares no extents, a copy wrote every byte of it a second time. `None` when the file
/// cannot be taken (see [`slots::take`]); the parts are then copied. A failed commit leaves
/// the file where it was, for the next Complete to take.
#[cfg(unix)]
async fn take_in_place(
    dest: &Dest<'_>,
    upload: &std::path::Path,
    parts: &[PartManifest],
    user_metadata: &std::collections::HashMap<String, String>,
    cond: Precondition<'_>,
) -> S3Result<Option<Metadata>> {
    let dal = dest.dal;
    let total = parts.iter().map(|p| p.size).sum();
    let taken = {
        let (upload, user_metadata) = (upload.to_owned(), user_metadata.clone());
        fs::blocking(move || slots::take(&upload, total, &user_metadata).map_err(new_std_io_error))
            .await
    };
    let (file, closed) = match taken {
        Ok(Some(taken)) => taken,
        Ok(None) => return Ok(None),
        // Copying needs none of what taking it does (a lock, xattrs on the file as it is).
        Err(err) => {
            tracing::warn!(upload = %upload.display(), error = %err, "cannot take the upload's object file; copying the parts");
            return Ok(None);
        }
    };
    #[cfg(test)]
    tests::record_fs_complete("in place");
    let object = upload.join(slots::OBJECT_FILE);
    let target = std::path::PathBuf::from(dal.op.info().root()).join(&dest.path);
    let committed = {
        let _guard = dal.locks.lock(&dest.path).await;
        match dest.check(cond).await {
            Ok(()) => fs::blocking(move || fs::rename_into_place(&object, &target))
                .await
                .map_err(write_err),
            Err(err) => Err(err),
        }
    };
    // Its lock keeps parts out of the file until it is the object.
    drop(file);
    if let Err(err) = committed {
        store::prune_empty_parents(dal, dest.bucket, dest.key).await;
        return Err(err);
    }
    Ok(Some(closed))
}

/// `opts` for composing the parts on the backend, where it composes objects with every field
/// `opts` sets and with the write conditions s3dal hands it; `None` streams them through a
/// writer instead. Only where s3dal stores the ETag itself: a composed object's own ETag is not
/// the multipart ETag a native one must be.
fn compose_options(dal: &S3Dal, opts: &WriteOptions) -> Option<ComposeOptions> {
    let cap = dal.op.info().capability();
    let can = |set: bool, supported: bool| !set || supported;
    let conditions = cap.compose_with_if_not_exists
        && (cap.compose_with_if_match || cap.compose_with_if_version_match);
    let composable = cap.compose
        && !dal.profile.native_etag
        && can(dal.profile.conditional_writes, conditions)
        && can(opts.content_type.is_some(), cap.compose_with_content_type)
        && can(opts.cache_control.is_some(), cap.compose_with_cache_control)
        && can(
            opts.content_disposition.is_some(),
            cap.compose_with_content_disposition,
        )
        && can(
            opts.content_encoding.is_some(),
            cap.compose_with_content_encoding,
        )
        && can(opts.user_metadata.is_some(), cap.compose_with_user_metadata);
    composable.then(|| ComposeOptions {
        content_type: opts.content_type.clone(),
        cache_control: opts.cache_control.clone(),
        content_disposition: opts.content_disposition.clone(),
        content_encoding: opts.content_encoding.clone(),
        user_metadata: opts.user_metadata.clone(),
        ..Default::default()
    })
}

/// Assembles the parts on the backend (GCS compose): no part data passes through s3dal, and
/// the object is not bound by the backend's own multipart limits (on GCS 10,000 parts of
/// OpenDAL's 5 MiB chunks, about 48.8 GiB). Composing is the commit, so it runs under the key's
/// lock with the condition checked there, as a writer's close does in `Dest::commit`.
async fn compose_parts(
    dest: &Dest<'_>,
    dir: &str,
    parts: &[PartManifest],
    mut opts: ComposeOptions,
    cond: Precondition<'_>,
) -> S3Result<Metadata> {
    let sources: Vec<String> = parts.iter().map(|p| format!("{dir}{}", p.data)).collect();
    let _guard = dest.dal.locks.lock(&dest.path).await;
    match dest.expected(cond).await? {
        Some(expected) => expected.apply_to_compose(&mut opts),
        None => dest.check(cond).await?,
    }
    dest.dal
        .op
        .compose_options(sources, &dest.path, opts)
        .await
        .map_err(|err| match err.kind() {
            opendal::ErrorKind::NotFound => record::no_such_upload(),
            // A failed condition is 412; anything unexpected is logged by `write_err`.
            _ => write_err(err),
        })
}

/// Part data disappears only when the upload is aborted or completed by another request.
fn copy_part_err(path: &str, err: opendal::Error) -> S3Error {
    if err.kind() == opendal::ErrorKind::NotFound {
        record::no_such_upload()
    } else {
        tracing::error!(path, error = %err, "cannot append part data");
        write_err(err)
    }
}

fn invalid_part() -> S3Error {
    s3_error!(
        InvalidPart,
        "One or more of the specified parts could not be found. The part may not have been uploaded, or the specified entity tag may not match the part's entity tag."
    )
}

fn malformed_xml() -> S3Error {
    s3_error!(
        MalformedXML,
        "The XML you provided was not well-formed or did not validate against our published schema."
    )
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::time::SystemTime;

    use opendal::Operator;

    use super::*;

    thread_local! {
        static FS_COMPLETES: std::cell::RefCell<Vec<&'static str>> =
            const { std::cell::RefCell::new(Vec::new()) };
    }

    /// How each Complete on fs on this thread made the object (see `assemble_on_fs`).
    pub(super) fn record_fs_complete(how: &'static str) {
        FS_COMPLETES.with(|c| c.borrow_mut().push(how));
    }

    /// Those recorded since the last call.
    fn fs_completes() -> Vec<&'static str> {
        FS_COMPLETES.with(std::cell::RefCell::take)
    }

    fn request<T>(input: T) -> S3Request<T> {
        S3Request {
            input,
            method: hyper::Method::POST,
            uri: "/".parse().unwrap(),
            headers: hyper::HeaderMap::new(),
            extensions: hyper::http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    /// An upload to `bkt/<key>`, created with a Content-Type, on fs in a directory of its own.
    #[cfg(unix)]
    struct FsUpload {
        root: tempfile::TempDir,
        op: Operator,
        dal: S3Dal,
        key: String,
        id: String,
    }

    #[cfg(unix)]
    impl FsUpload {
        async fn new(key: &str) -> Self {
            use s3s::S3;

            let root = tempfile::tempdir().unwrap();
            let fs_root = root.path().to_string_lossy().into_owned();
            let op = crate::backend::build_operator("fs", &[("root".to_owned(), fs_root)]).unwrap();
            let profile = crate::backend::probe(&op).await.unwrap();
            let dal = S3Dal::new(op.clone(), profile, crate::Settings::default());
            op.create_dir("bkt/").await.unwrap();
            let created = CreateMultipartUploadInput::builder()
                .bucket("bkt".to_owned())
                .key(key.to_owned())
                .content_type(Some("text/plain".to_owned()))
                .build()
                .unwrap();
            let id = dal
                .create_multipart_upload(request(created))
                .await
                .unwrap()
                .output
                .upload_id
                .unwrap();
            fs_completes();
            Self {
                root,
                op,
                dal,
                key: key.to_owned(),
                id,
            }
        }

        async fn part(
            &self,
            n: i32,
            data: Vec<u8>,
            content_md5: Option<String>,
        ) -> S3Result<CompletedPart> {
            use s3s::S3;

            // In 64 KiB pieces, as a request body arrives, so a part spans write batches.
            let len = data.len();
            let data = bytes::Bytes::from(data);
            let pieces: Vec<Result<_, std::io::Error>> = (0..len)
                .step_by(64 * 1024)
                .map(|at| Ok(data.slice(at..len.min(at + 64 * 1024))))
                .collect();
            let part = UploadPartInput::builder()
                .bucket("bkt".to_owned())
                .key(self.key.clone())
                .upload_id(self.id.clone())
                .part_number(n)
                .content_length(Some(i64::try_from(len).unwrap()))
                .content_md5(content_md5)
                .body(Some(StreamingBlob::wrap(futures::stream::iter(pieces))))
                .build()
                .unwrap();
            let e_tag = self.dal.upload_part(request(part)).await?.output.e_tag;
            Ok(CompletedPart {
                part_number: Some(n),
                e_tag,
                ..Default::default()
            })
        }

        async fn complete(
            &self,
            parts: &[CompletedPart],
        ) -> S3Result<CompleteMultipartUploadOutput> {
            use s3s::S3;

            let complete = CompleteMultipartUploadInput::builder()
                .bucket("bkt".to_owned())
                .key(self.key.clone())
                .upload_id(self.id.clone())
                .multipart_upload(Some(CompletedMultipartUpload {
                    parts: Some(parts.to_vec()),
                }))
                .build()
                .unwrap();
            let out = self
                .dal
                .complete_multipart_upload(request(complete))
                .await?;
            out.output.future.unwrap().await
        }

        /// The upload's directory, or a file in it.
        fn upload(&self, name: &str) -> std::path::PathBuf {
            let dir = self.root.path().join(".s3dal/uploads/bkt").join(&self.id);
            dir.join(name)
        }

        /// The object as HEAD reports it, after its content was checked against `expected`.
        async fn object(&self, expected: &[u8]) -> meta::ObjectMeta {
            let path = format!("bkt/{}", self.key);
            // Not `assert_eq!`, which would print megabytes.
            assert!(self.op.read(&path).await.unwrap().to_vec() == expected);
            meta::resolve(&self.dal.profile, &self.op.stat(&path).await.unwrap())
        }
    }

    #[cfg(unix)]
    fn filled(byte: u8, len: u64) -> Vec<u8> {
        vec![byte; usize::try_from(len).unwrap()]
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn on_fs_parts_of_one_size_become_the_object_in_place() {
        // Clients upload parts of one size, the last one shorter. Copied, every byte of them
        // was written a second time, as ext4 shares no extents; the file they were written
        // into becomes the object instead. It must be the object a copy makes: the parts in
        // order, with the ETag and Content-Type HEAD reports, and nothing left of the upload
        // but its record.
        use std::os::unix::fs::MetadataExt;

        let up = FsUpload::new("a/k").await;
        let mut parts = Vec::new();
        let mut expected = Vec::new();
        for (n, data) in [
            (1, filled(1, MIN_PART_SIZE)),
            (2, filled(2, MIN_PART_SIZE)),
            (3, b"tail".to_vec()),
        ] {
            expected.extend_from_slice(&data);
            parts.push(up.part(n, data, None).await.unwrap());
        }
        let written = std::fs::metadata(up.upload(slots::OBJECT_FILE)).unwrap();

        let done = up.complete(&parts).await.unwrap();
        assert_eq!(fs_completes(), ["in place"]);
        let object = std::fs::metadata(up.root.path().join("bkt/a/k")).unwrap();
        assert_eq!(object.ino(), written.ino());
        let head = up.object(&expected).await;
        assert!(head.etag.ends_with("-3"), "{}", head.etag);
        assert_eq!(head.content_type(), "text/plain");
        assert_eq!(done.e_tag, Some(ETag::Strong(head.etag)));
        let left: Vec<_> = std::fs::read_dir(up.upload(""))
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(left, [record::UPLOAD_FILE]);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn on_fs_a_part_longer_than_a_write_batch_lands_whole_in_its_slot() {
        // A part is written to its slot in batches, each of which must land right after the
        // one before. Bytes that vary with their position show a batch landing anywhere else,
        // or not at all.
        let size = crate::s3::store::UNSIZED_WRITE_CHUNK as u64 * 2 + 12_345;
        let varied = |n: u8| -> Vec<u8> { (0..size).map(|i| (i % 251) as u8 ^ n).collect() };
        let up = FsUpload::new("a/k").await;
        let mut parts = Vec::new();
        let mut expected = Vec::new();
        for (n, data) in [(1, varied(1)), (2, varied(2)), (3, b"tail".to_vec())] {
            expected.extend_from_slice(&data);
            parts.push(up.part(n, data, None).await.unwrap());
        }

        up.complete(&parts).await.unwrap();
        assert_eq!(fs_completes(), ["in place"]);
        up.object(&expected).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn on_fs_parts_out_of_place_are_copied_file_to_file() {
        // A last part that arrives before any full one cannot know its slot, so it is written
        // apart; through OpenDAL's writer every byte of the parts was then read into s3dal and
        // written out again, where Complete copies file to file. The object must still be the
        // one the writer made, with nothing left in the writer's directory.
        let up = FsUpload::new("a/k").await;
        let tail = up.part(2, b"tail".to_vec(), None).await.unwrap();
        let first = up.part(1, filled(1, MIN_PART_SIZE), None).await.unwrap();

        let done = up.complete(&[first, tail]).await.unwrap();
        assert_eq!(fs_completes(), ["copied"]);
        let mut expected = filled(1, MIN_PART_SIZE);
        expected.extend_from_slice(b"tail");
        let head = up.object(&expected).await;
        assert!(head.etag.ends_with("-2"), "{}", head.etag);
        assert_eq!(head.content_type(), "text/plain");
        assert_eq!(done.e_tag, Some(ETag::Strong(head.etag)));
        let left = up.op.list(".s3dal/tmp/").await.unwrap();
        assert!(left.iter().all(|e| e.path() == ".s3dal/tmp/"), "{left:?}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn on_fs_a_file_holding_a_part_not_listed_is_copied_from() {
        // A Complete may leave out the last parts uploaded; taken as it is, the file would make
        // them part of the object.
        let up = FsUpload::new("a/k").await;
        let first = up.part(1, filled(1, MIN_PART_SIZE), None).await.unwrap();
        let second = up.part(2, filled(2, MIN_PART_SIZE), None).await.unwrap();
        up.part(3, b"tail".to_vec(), None).await.unwrap();

        up.complete(&[first, second]).await.unwrap();
        assert_eq!(fs_completes(), ["copied"]);
        let mut expected = filled(1, MIN_PART_SIZE);
        expected.extend_from_slice(&filled(2, MIN_PART_SIZE));
        up.object(&expected).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn on_fs_a_failed_upload_of_a_part_leaves_the_committed_one() {
        // A manifest is a part's commit point, so the data it names must not change. Another
        // upload of the part number is found to be bad only once its body has ended, after
        // its first batch was written; written into the slot, it would corrupt the object.
        let up = FsUpload::new("a/k").await;
        let size = 9 << 20;
        let first = up.part(1, filled(1, size), None).await.unwrap();
        let second = up.part(2, filled(2, size), None).await.unwrap();
        let other = base64_simd::STANDARD.encode_to_string(Md5::digest(b"other"));
        let err = up.part(2, filled(7, size), Some(other)).await.unwrap_err();
        assert_eq!(*err.code(), S3ErrorCode::BadDigest);

        up.complete(&[first, second]).await.unwrap();
        assert_eq!(fs_completes(), ["in place"]);
        let mut expected = filled(1, size);
        expected.extend_from_slice(&filled(2, size));
        up.object(&expected).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn on_fs_a_part_retried_after_it_failed_still_takes_its_slot() {
        // SDKs retry a part that failed. Written apart, the retry would make Complete copy the
        // whole object again, for one network error; written into the slot, it must replace
        // every byte the failed upload left there.
        let up = FsUpload::new("a/k").await;
        let size = 9 << 20;
        let first = up.part(1, filled(1, size), None).await.unwrap();
        let other = base64_simd::STANDARD.encode_to_string(Md5::digest(b"other"));
        up.part(2, filled(7, size), Some(other)).await.unwrap_err();
        let second = up.part(2, filled(2, size), None).await.unwrap();

        up.complete(&[first, second]).await.unwrap();
        assert_eq!(fs_completes(), ["in place"]);
        let mut expected = filled(1, size);
        expected.extend_from_slice(&filled(2, size));
        up.object(&expected).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn on_fs_a_failed_commit_leaves_the_file_for_the_next_complete() {
        // A Complete that fails is retried, or the client fixes what made it fail and sends
        // it again; until the file is in place nothing it needs may be gone.
        let up = FsUpload::new("a").await;
        let first = up.part(1, filled(1, MIN_PART_SIZE), None).await.unwrap();
        let tail = up.part(2, b"tail".to_vec(), None).await.unwrap();
        // A directory where the object would go.
        up.op.write("bkt/a/b", b"x".to_vec()).await.unwrap();
        up.complete(&[first.clone(), tail.clone()])
            .await
            .unwrap_err();
        assert_eq!(fs_completes(), ["in place"]);

        up.op.delete("bkt/a/b").await.unwrap();
        up.op.delete("bkt/a/").await.unwrap();
        let done = up.complete(&[first, tail]).await.unwrap();
        assert_eq!(fs_completes(), ["in place"]);
        let mut expected = filled(1, MIN_PART_SIZE);
        expected.extend_from_slice(b"tail");
        let head = up.object(&expected).await;
        assert_eq!(done.e_tag, Some(ETag::Strong(head.etag)));
    }

    fn manifest(data: &[u8]) -> PartManifest {
        PartManifest {
            data: String::new(),
            offset: 0,
            size: data.len() as u64,
            md5: meta::md5_hex(data),
            checksums: BTreeMap::new(),
            last_modified: SystemTime::now(),
        }
    }

    #[test]
    fn multipart_etag_hashes_the_binary_part_md5s() {
        // The value must be the one AWS computes, or clients that recompute it to verify an
        // upload (rclone, the aws CLI) report corruption.
        let parts = [manifest(b"hello "), manifest(b"world")];
        let mut concat = Md5::digest(b"hello ").to_vec();
        concat.extend_from_slice(&Md5::digest(b"world"));
        assert_eq!(
            multipart_etag(parts.iter().map(|p| p.md5.as_str())).unwrap(),
            format!("{}-2", meta::md5_hex(&concat))
        );
        // A retry is matched by this value computed from the ETags it lists; one that is not an
        // MD5 cannot name a part s3dal stored, so it matches nothing.
        assert_eq!(multipart_etag(["abc"]), None);
        assert_eq!(multipart_etag([&parts[0].md5[..30]]), None);
    }

    fn complete_request(uri: &str, host: Option<&str>, scheme: &'static str) -> S3Request<()> {
        let mut headers = hyper::HeaderMap::new();
        if let Some(host) = host {
            headers.insert(HOST, host.parse().unwrap());
        }
        let mut extensions = hyper::http::Extensions::new();
        extensions.insert(Scheme(scheme));
        S3Request {
            input: (),
            method: hyper::Method::POST,
            uri: uri.parse().unwrap(),
            headers,
            extensions,
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    #[test]
    fn location_is_the_url_the_complete_was_sent_to() {
        // A virtual-hosted request names the bucket in the host; naming it in the path as well
        // would point at the key `bkt/dir/a b` instead.
        let path_style =
            complete_request("/bkt/dir/a%20b?uploadId=x", Some("s3.test:9000"), "https");
        assert_eq!(
            object_url(&path_style).as_deref(),
            Some("https://s3.test:9000/bkt/dir/a%20b")
        );
        let virtual_host = complete_request("/dir/a%20b?uploadId=x", Some("bkt.s3.test"), "http");
        assert_eq!(
            object_url(&virtual_host).as_deref(),
            Some("http://bkt.s3.test/dir/a%20b")
        );
        // HTTP/2 names the host in the URI, and sends no Host header.
        let http2 = complete_request("https://s3.test/bkt/k?uploadId=x", None, "https");
        assert_eq!(object_url(&http2).as_deref(), Some("https://s3.test/bkt/k"));
    }

    #[test]
    fn hex_decoding_rejects_what_hex_never_writes() {
        assert_eq!(unhex("00ff"), Some(vec![0, 255]));
        for bad in ["0", "zz", "+1", "é0"] {
            assert_eq!(unhex(bad), None, "{bad}");
        }
    }

    #[test]
    fn a_part_checksum_must_match_the_one_stored_for_it() {
        // A client listing a checksum expects S3 to confirm it; one never computed cannot be.
        let mut stored = manifest(b"x");
        stored
            .checksums
            .insert("CRC32".to_owned(), "DUoRhQ==".to_owned());
        let part = |crc32: Option<&str>, sha1: Option<&str>| CompletedPart {
            part_number: Some(1),
            checksum_crc32: crc32.map(str::to_owned),
            checksum_sha1: sha1.map(str::to_owned),
            ..Default::default()
        };
        assert!(check_part_checksums(&part(Some("DUoRhQ=="), None), &stored).is_ok());
        assert!(check_part_checksums(&part(None, None), &stored).is_ok());
        for wrong in [part(Some("AAAAAA=="), None), part(None, Some("x"))] {
            let err = check_part_checksums(&wrong, &stored).unwrap_err();
            assert_eq!(*err.code(), S3ErrorCode::InvalidPart);
        }
    }

    #[test]
    fn parts_are_composed_only_where_s3dal_keeps_the_etag() {
        // A composed object gets the backend's ETag, not the multipart one clients recompute
        // to verify an upload; and where s3dal hands the backend its write conditions, the
        // compose must take them as a write would.
        let gcs = crate::backend::build_operator(
            "gcs",
            &[
                ("bucket".to_owned(), "b".to_owned()),
                ("endpoint".to_owned(), "http://127.0.0.1:1".to_owned()),
                ("skip_signature".to_owned(), "true".to_owned()),
            ],
        )
        .unwrap();
        let memory = crate::backend::build_operator("memory", &[]).unwrap();
        let profile = |native_etag| crate::Profile {
            native_etag,
            native_content_type: true,
            native_cache_control: true,
            native_content_disposition: true,
            native_content_encoding: true,
            encoded_metadata_names: false,
            ordered_list: true,
            real_dirs: false,
            conditional_writes: true,
        };
        let opts = WriteOptions {
            content_type: Some("text/plain".to_owned()),
            user_metadata: Some(HashMap::from([("k".to_owned(), "v".to_owned())])),
            ..Default::default()
        };
        let composes = |op: &Operator, native_etag| {
            let dal = S3Dal::new(op.clone(), profile(native_etag), crate::Settings::default());
            compose_options(&dal, &opts)
        };
        let composed = composes(&gcs, false).expect("gcs composes");
        assert_eq!(composed.content_type.as_deref(), Some("text/plain"));
        assert_eq!(composed.user_metadata, opts.user_metadata);
        assert!(composes(&gcs, true).is_none());
        assert!(composes(&memory, false).is_none());
    }
}

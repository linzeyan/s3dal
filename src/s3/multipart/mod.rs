//! Multipart uploads (design.md "Multipart (phase 2)").
//!
//! An upload lives in `.s3dal/uploads/<bucket>/<upload-id>/`: `upload.json` from Create, and for
//! each part its data committed by its `part-<nnnnn>.json` manifest: in a file of its own, or on
//! fs in the slot `slots` gives it in the file that becomes the object. Complete marks
//! `upload.json` completed and removes the parts; the record then answers only a retried
//! Complete. That directory is s3dal's own state, so it is written directly; only the object
//! Complete produces goes through the key lock.

mod complete;
mod list;
mod part;
mod record;
#[cfg(unix)]
mod slots;
mod sweep;

use std::time::SystemTime;

use opendal::options::DeleteOptions;
use s3s::dto::*;
use s3s::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use uuid::{Uuid, Version};

use super::{S3Dal, naming, unsupported};
use crate::backend::RESERVED_ROOT;
use crate::checksum::{self, ALGORITHMS};
use crate::error::{Ctx, map};
use crate::meta::{self, Attrs};

pub(super) use complete::complete_multipart_upload;
pub(super) use list::{list_multipart_uploads, list_parts, page_size};
pub(super) use part::{upload_part, upload_part_copy};
pub(super) use sweep::sweep;

pub(super) async fn create_multipart_upload(
    dal: &S3Dal,
    req: S3Request<CreateMultipartUploadInput>,
) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
    let mut input = req.input;
    naming::require_bucket(dal, &input.bucket).await?;
    naming::validate_key(&input.key)?;
    // A folder marker is a directory here, and a directory cannot be assembled from parts.
    if input.key.ends_with('/') {
        return Err(s3_error!(
            NotImplemented,
            "multipart uploads to a key ending in '/' (a folder marker) are not supported"
        ));
    }
    unsupported::refuse_new_object_features!(input)?;
    let algorithm = input.checksum_algorithm.take();
    if let Some(a) = &algorithm
        && !ALGORITHMS.iter().any(|(name, ..)| *name == a.as_str())
    {
        return Err(s3_error!(
            InvalidRequest,
            "Value for x-amz-checksum-algorithm header is invalid."
        ));
    }
    let checksum_type = checksum_type(algorithm.as_ref(), input.checksum_type.take())?;
    let user_metadata = input.metadata.take().unwrap_or_default();
    meta::check_user_metadata(&user_metadata)?;

    let id = Uuid::new_v4();
    let upload = record::Upload {
        key: input.key,
        attrs: Attrs {
            content_type: input.content_type,
            cache_control: input.cache_control,
            content_disposition: input.content_disposition,
            content_encoding: input.content_encoding,
            content_language: input.content_language,
            expires: input.expires,
            user_metadata,
        },
        checksum_algorithm: algorithm.as_ref().map(|a| a.as_str().to_owned()),
        checksum_type: checksum_type.as_ref().map(|t| t.as_str().to_owned()),
        initiated: SystemTime::now(),
        completed: None,
    };
    record::store_upload(dal, &input.bucket, id, &upload).await?;

    Ok(S3Response::new(CreateMultipartUploadOutput {
        bucket: Some(input.bucket),
        key: Some(upload.key),
        upload_id: Some(id.hyphenated().to_string()),
        checksum_algorithm: algorithm,
        checksum_type,
        ..Default::default()
    }))
}

/// How the object's checksum will derive from the parts': as named, or AWS's default for the
/// algorithm. A type without an algorithm, or one the algorithm cannot have, would leave the
/// client expecting a checksum that Complete cannot compute.
fn checksum_type(
    algorithm: Option<&ChecksumAlgorithm>,
    named: Option<ChecksumType>,
) -> S3Result<Option<ChecksumType>> {
    let Some(algorithm) = algorithm.map(ChecksumAlgorithm::as_str) else {
        return match named {
            Some(_) => Err(s3_error!(
                InvalidRequest,
                "The x-amz-checksum-type header can only be used with the x-amz-checksum-algorithm header."
            )),
            None => Ok(None),
        };
    };
    let named =
        named.unwrap_or_else(|| ChecksumType::from_static(checksum::default_type(algorithm)));
    let name = named.as_str();
    if ![ChecksumType::COMPOSITE, ChecksumType::FULL_OBJECT].contains(&name) {
        return Err(s3_error!(
            InvalidRequest,
            "Value for x-amz-checksum-type header is invalid."
        ));
    }
    if !checksum::supports(algorithm, name) {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            format!(
                "The {name} checksum type cannot be used with the {} checksum algorithm.",
                algorithm.to_ascii_lowercase()
            ),
        ));
    }
    Ok(Some(named))
}

pub(super) async fn abort_multipart_upload(
    dal: &S3Dal,
    req: S3Request<AbortMultipartUploadInput>,
) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
    let input = req.input;
    naming::require_bucket(dal, &input.bucket).await?;
    // A guard against aborting someone else's newer upload; ignoring it would abort anyway.
    if input.if_match_initiated_time.is_some() {
        return Err(s3_error!(
            NotImplemented,
            "x-amz-if-match-initiated-time is not supported"
        ));
    }
    let id = parse_upload_id(&input.upload_id)?;
    record::load_upload(dal, &input.bucket, id, &input.key).await?;
    remove_dir(dal, &upload_dir(&input.bucket, id)).await?;
    Ok(S3Response::new(AbortMultipartUploadOutput::default()))
}

/// Removes every upload of `bucket`, for DeleteBucket: a bucket created later under the same
/// name must not inherit them.
pub(super) async fn delete_bucket_uploads(dal: &S3Dal, bucket: &str) -> S3Result<()> {
    remove_dir(dal, &uploads_dir(bucket)).await
}

async fn remove_dir(dal: &S3Dal, dir: &str) -> S3Result<()> {
    let recursive = DeleteOptions {
        recursive: true,
        ..Default::default()
    };
    dal.op
        .delete_options(dir, recursive)
        .await
        .map_err(|err| map(err, Ctx::Other))
}

/// The bucket name must already be validated: valid names hold no `/` or `..`.
fn uploads_dir(bucket: &str) -> String {
    format!("{RESERVED_ROOT}uploads/{bucket}/")
}

/// Takes only a parsed id, so a string from a client never becomes part of a path.
fn upload_dir(bucket: &str, id: Uuid) -> String {
    format!("{}{}/", uploads_dir(bucket), id.hyphenated())
}

/// Accepts exactly the form s3dal issues: a lowercase, hyphenated UUID v4. Anything else cannot
/// name an upload. Other spellings of the same UUID are refused too, because on a
/// case-insensitive filesystem an uppercase id would still reach the upload's directory.
fn parse_upload_id(id: &str) -> S3Result<Uuid> {
    match Uuid::try_parse(id) {
        Ok(uuid)
            if uuid.get_version() == Some(Version::Random)
                && uuid.hyphenated().to_string() == id =>
        {
            Ok(uuid)
        }
        _ => Err(record::no_such_upload()),
    }
}

#[cfg(test)]
mod tests {
    use s3s::S3ErrorCode;

    use super::*;

    #[test]
    fn only_canonical_v4_ids_reach_a_path() {
        // The id becomes a directory name: `..` would escape the upload area, and another
        // spelling of a real id would still reach its directory on a case-insensitive fs.
        let id = Uuid::new_v4();
        let canonical = id.hyphenated().to_string();
        assert_eq!(parse_upload_id(&canonical).unwrap(), id);
        for bad in [
            "../x".to_owned(),
            String::new(),
            canonical.to_uppercase(),
            id.simple().to_string(),
            id.braced().to_string(),
            id.urn().to_string(),
            format!("{canonical}/"),
            // A UUID, but not one s3dal issues.
            "00000000-0000-1000-8000-000000000000".to_owned(),
        ] {
            let err = parse_upload_id(&bad).unwrap_err();
            assert_eq!(*err.code(), S3ErrorCode::NoSuchUpload, "{bad:?}");
        }
    }

    #[test]
    fn upload_paths_stay_in_the_reserved_area() {
        let id = Uuid::new_v4();
        assert_eq!(
            upload_dir("bkt", id),
            format!(".s3dal/uploads/bkt/{}/", id.hyphenated())
        );
    }
}

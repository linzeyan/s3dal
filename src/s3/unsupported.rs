//! Request features s3dal does not implement. What a client relies on as a protection or as a
//! stored value is refused with `NotImplemented`: answering 200 would let it believe an object
//! is encrypted, locked, tagged, or shared when it is not. Advisory features are accepted and
//! have no effect: any storage class S3 defines, request payer, and canned ACLs that grant
//! nothing beyond the owner.

use hyper::HeaderMap;
use s3s::dto::{
    ObjectCannedACL, SSECustomerAlgorithm, SSECustomerKey, SSECustomerKeyMD5, StorageClass,
};
use s3s::{S3Error, S3ErrorCode, S3Result, s3_error};

/// Refuses what a new object can ask for on PutObject, CopyObject and CreateMultipartUpload,
/// the same way on all three. A macro because the three inputs share these field names but no
/// trait.
macro_rules! refuse_new_object_features {
    ($input:expr) => {{
        let input = &$input;
        $crate::s3::unsupported::check_storage_class(input.storage_class.as_ref())?;
        $crate::s3::unsupported::refuse(&[
            (
                input.server_side_encryption.is_some()
                    || input.ssekms_key_id.is_some()
                    || input.ssekms_encryption_context.is_some()
                    || input.bucket_key_enabled == Some(true),
                "server-side encryption",
            ),
            (
                input.sse_customer_algorithm.is_some()
                    || input.sse_customer_key.is_some()
                    || input.sse_customer_key_md5.is_some(),
                "server-side encryption with customer-provided keys",
            ),
            (
                input.object_lock_mode.is_some()
                    || input.object_lock_retain_until_date.is_some()
                    || input.object_lock_legal_hold_status.is_some(),
                "object lock",
            ),
            (
                input.tagging.as_deref().is_some_and(|t| !t.is_empty()),
                "object tagging",
            ),
            (
                input.website_redirect_location.is_some(),
                "x-amz-website-redirect-location",
            ),
        ])
        .and_then(|()| {
            $crate::s3::unsupported::refuse_access_beyond_owner(
                input.acl.as_ref(),
                input.grant_full_control.is_some()
                    || input.grant_read.is_some()
                    || input.grant_read_acp.is_some()
                    || input.grant_write_acp.is_some(),
            )
        })
    }};
}
pub(super) use refuse_new_object_features;

/// Access for anyone but the owner, which s3dal can neither store nor enforce: a canned ACL
/// other than the owner-only ones, or any `x-amz-grant-*` header (`granted`). New objects and
/// PutBucketAcl/PutObjectAcl apply this one rule, so an ACL that one of them accepts is never
/// refused by another.
pub(super) fn refuse_access_beyond_owner(
    acl: Option<&ObjectCannedACL>,
    granted: bool,
) -> S3Result<()> {
    // A value no canned ACL has is a malformed request, which S3 answers with 400, not a
    // feature s3dal lacks.
    if let Some(acl) = acl.filter(|acl| !canned(acl)) {
        return Err(s3_error!(
            InvalidArgument,
            "unknown canned ACL `{}`",
            acl.as_str()
        ));
    }
    refuse(&[
        (
            acl.is_some_and(|acl| !owner_only(acl)),
            "a canned ACL that grants access beyond the owner",
        ),
        (granted, "x-amz-grant-*"),
    ])
}

/// Any storage class S3 defines is accepted and has no effect; a value it does not define is a
/// malformed request, which S3 answers with 400.
pub(super) fn check_storage_class(class: Option<&StorageClass>) -> S3Result<()> {
    let known = |class: &StorageClass| {
        matches!(
            class.as_str(),
            StorageClass::STANDARD
                | StorageClass::REDUCED_REDUNDANCY
                | StorageClass::STANDARD_IA
                | StorageClass::ONEZONE_IA
                | StorageClass::INTELLIGENT_TIERING
                | StorageClass::GLACIER
                | StorageClass::GLACIER_IR
                | StorageClass::DEEP_ARCHIVE
                | StorageClass::OUTPOSTS
                | StorageClass::SNOW
                | StorageClass::EXPRESS_ONEZONE
                | StorageClass::FSX_OPENZFS
                | StorageClass::FSX_ONTAP
                | StorageClass::AWS_BACKUP_WARM
                | StorageClass::AWS_BACKUP_LOW_COST_WARM
        )
    };
    match class.filter(|class| !known(class)) {
        Some(_) => Err(s3_error!(
            InvalidStorageClass,
            "The storage class you specified is not valid"
        )),
        None => Ok(()),
    }
}

/// An object's encryption is its own, not the reader's to choose, so S3 refuses
/// `x-amz-server-side-encryption` on GET and HEAD with 400. Ignoring it would let a client that
/// asked for an encrypted object believe it read one.
pub(super) fn refuse_sse_on_read(headers: &HeaderMap) -> S3Result<()> {
    if headers.contains_key("x-amz-server-side-encryption")
        || headers.contains_key("x-amz-server-side-encryption-aws-kms-key-id")
    {
        return Err(s3_error!(
            InvalidArgument,
            "x-amz-server-side-encryption header is not supported for this operation."
        ));
    }
    Ok(())
}

/// Fails with the first feature that is present.
pub(super) fn refuse(checks: &[(bool, &str)]) -> S3Result<()> {
    match checks.iter().find(|(present, _)| *present) {
        Some((_, feature)) => Err(not_implemented(feature)),
        None => Ok(()),
    }
}

/// Every canned ACL S3 defines, for objects or buckets (`log-delivery-write` is a bucket's).
fn canned(acl: &ObjectCannedACL) -> bool {
    matches!(
        acl.as_str(),
        ObjectCannedACL::PRIVATE
            | ObjectCannedACL::PUBLIC_READ
            | ObjectCannedACL::PUBLIC_READ_WRITE
            | ObjectCannedACL::AUTHENTICATED_READ
            | ObjectCannedACL::AWS_EXEC_READ
            | ObjectCannedACL::BUCKET_OWNER_READ
            | ObjectCannedACL::BUCKET_OWNER_FULL_CONTROL
            | "log-delivery-write"
    )
}

/// With a single tenant the object owner is the bucket owner, so these grant nothing the owner
/// does not already have. Clients such as rclone send `private` on every upload.
fn owner_only(acl: &ObjectCannedACL) -> bool {
    matches!(
        acl.as_str(),
        ObjectCannedACL::PRIVATE
            | ObjectCannedACL::BUCKET_OWNER_FULL_CONTROL
            | ObjectCannedACL::BUCKET_OWNER_READ
    )
}

/// SSE-C asks s3dal to encrypt or decrypt with the client's key. No object is ever encrypted,
/// so going ahead would store or serve the data in the clear behind the client's back.
pub(super) fn refuse_sse_c(
    algorithm: Option<&SSECustomerAlgorithm>,
    key: Option<&SSECustomerKey>,
    key_md5: Option<&SSECustomerKeyMD5>,
) -> S3Result<()> {
    refuse(&[(
        algorithm.is_some() || key.is_some() || key_md5.is_some(),
        "server-side encryption with customer-provided keys",
    )])
}

pub(super) fn not_implemented(feature: &str) -> S3Error {
    S3Error::with_message(
        S3ErrorCode::NotImplemented,
        format!("{feature} is not supported"),
    )
}

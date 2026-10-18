//! Bucket and object ACLs. A single tenant owns every bucket and object, and nothing can be
//! granted to anyone else (`unsupported.rs`), so every ACL is the owner's full control and an
//! ACL write can only restate it.

use s3s::dto::*;
use s3s::{S3Request, S3Response, S3Result, s3_error};

use super::{S3Dal, naming, object, unsupported};

/// The owner of everything s3dal serves. A constant rather than the caller's access key: every
/// key acts for the same tenant, so the owner must not change with the key that asks or when
/// keys rotate, and access key ids do not belong in responses. It has the 64-hex-digit form of
/// a canonical user ID (it is the SHA-256 of `s3dal`) for clients that expect one.
const OWNER_ID: &str = "f79ceb84c3cb7f5b57f5e928103eabda70d4351387693ea4c7be9feec207ab34";
const OWNER_NAME: &str = "s3dal";

pub(super) fn owner() -> Owner {
    Owner {
        id: Some(OWNER_ID.to_owned()),
        display_name: Some(OWNER_NAME.to_owned()),
    }
}

/// Every key acts for the owner, so the owner is also who initiated every upload.
pub(super) fn initiator() -> Initiator {
    Initiator {
        id: Some(OWNER_ID.to_owned()),
        display_name: Some(OWNER_NAME.to_owned()),
    }
}

/// The owner's full control, and nothing for anyone else.
fn grants() -> Grants {
    vec![Grant {
        grantee: Some(Grantee {
            id: Some(OWNER_ID.to_owned()),
            display_name: Some(OWNER_NAME.to_owned()),
            type_: Type::from_static(Type::CANONICAL_USER),
            email_address: None,
            uri: None,
        }),
        permission: Some(Permission::from_static(Permission::FULL_CONTROL)),
    }]
}

/// An ACL write may ask for exactly what a new object may, which leaves the ACL as it is. A
/// policy body is a grant list, and s3dal stores none, so it is refused even when it only
/// restates the owner's grant.
fn check_write(
    acl: Option<&ObjectCannedACL>,
    granted: bool,
    policy: Option<&AccessControlPolicy>,
) -> S3Result<()> {
    unsupported::refuse_access_beyond_owner(acl, granted)?;
    unsupported::refuse(&[(policy.is_some(), "an AccessControlPolicy request body")])?;
    // S3 requires one of the three; a request with none sets nothing.
    if acl.is_none() && !granted && policy.is_none() {
        return Err(s3_error!(
            MissingSecurityHeader,
            "Your request was missing a required header"
        ));
    }
    Ok(())
}

pub(super) async fn get_bucket_acl(
    dal: &S3Dal,
    req: S3Request<GetBucketAclInput>,
) -> S3Result<S3Response<GetBucketAclOutput>> {
    naming::require_bucket(dal, &req.input.bucket).await?;
    Ok(S3Response::new(GetBucketAclOutput {
        owner: Some(owner()),
        grants: Some(grants()),
    }))
}

pub(super) async fn put_bucket_acl(
    dal: &S3Dal,
    req: S3Request<PutBucketAclInput>,
) -> S3Result<S3Response<PutBucketAclOutput>> {
    let input = req.input;
    naming::require_bucket(dal, &input.bucket).await?;
    // `private` is the owner-only value bucket and object canned ACLs share; the others a
    // bucket can take all grant access to someone else.
    let acl = input
        .acl
        .map(|acl| ObjectCannedACL::from(acl.as_str().to_owned()));
    let granted = input.grant_full_control.is_some()
        || input.grant_read.is_some()
        || input.grant_read_acp.is_some()
        || input.grant_write.is_some()
        || input.grant_write_acp.is_some();
    check_write(acl.as_ref(), granted, input.access_control_policy.as_ref())?;
    Ok(S3Response::new(PutBucketAclOutput::default()))
}

pub(super) async fn get_object_acl(
    dal: &S3Dal,
    req: S3Request<GetObjectAclInput>,
) -> S3Result<S3Response<GetObjectAclOutput>> {
    let input = req.input;
    naming::require_bucket(dal, &input.bucket).await?;
    naming::validate_key(&input.key)?;
    naming::require_null_version(input.version_id.as_deref())?;
    object::stat_object(dal, &input.bucket, &input.key).await?;
    Ok(S3Response::new(GetObjectAclOutput {
        owner: Some(owner()),
        grants: Some(grants()),
        ..Default::default()
    }))
}

pub(super) async fn put_object_acl(
    dal: &S3Dal,
    req: S3Request<PutObjectAclInput>,
) -> S3Result<S3Response<PutObjectAclOutput>> {
    let input = req.input;
    naming::require_bucket(dal, &input.bucket).await?;
    naming::validate_key(&input.key)?;
    let granted = input.grant_full_control.is_some()
        || input.grant_read.is_some()
        || input.grant_read_acp.is_some()
        || input.grant_write.is_some()
        || input.grant_write_acp.is_some();
    check_write(
        input.acl.as_ref(),
        granted,
        input.access_control_policy.as_ref(),
    )?;
    naming::require_null_version(input.version_id.as_deref())?;
    object::stat_object(dal, &input.bucket, &input.key).await?;
    Ok(S3Response::new(PutObjectAclOutput::default()))
}

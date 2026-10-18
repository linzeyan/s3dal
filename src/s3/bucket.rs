//! Bucket operations. A bucket is a directory at the namespace root (design.md "Buckets" and
//! "Namespace mapping").

use std::time::SystemTime;

use futures::{StreamExt, TryStreamExt};
use hyper::StatusCode;
use s3s::dto::*;
use s3s::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};

use super::naming::{bucket_dir, require_bucket, validate_bucket_name};
use super::{S3Dal, acl, multipart, unsupported};
use crate::error::{Ctx, map};

/// S3 reports buckets in its original region with an empty LocationConstraint, and SDKs read
/// an empty value as this region.
const LEGACY_REGION: &str = "us-east-1";

/// The largest ListBuckets page AWS accepts.
const MAX_BUCKETS: i32 = 10_000;

/// Creation-date lookups in flight; see `list_buckets`.
const DATE_CONCURRENCY: usize = 16;

pub(super) async fn create_bucket(
    dal: &S3Dal,
    req: S3Request<CreateBucketInput>,
) -> S3Result<S3Response<CreateBucketOutput>> {
    let input = req.input;
    let bucket = input.bucket;
    validate_bucket_name(&bucket)?;
    // A bucket created public or with object lock would be believed to be so. This is the rule
    // PutBucketAcl applies, and `private` is the owner-only value both canned ACLs share.
    let granted = input.grant_full_control.is_some()
        || input.grant_read.is_some()
        || input.grant_read_acp.is_some()
        || input.grant_write.is_some()
        || input.grant_write_acp.is_some();
    let acl = input
        .acl
        .map(|acl| ObjectCannedACL::from(acl.as_str().to_owned()));
    unsupported::refuse_access_beyond_owner(acl.as_ref(), granted)?;
    unsupported::refuse(&[(
        input.object_lock_enabled_for_bucket == Some(true),
        "object lock",
    )])?;
    let dir = bucket_dir(&bucket);
    let created = CreateBucketOutput {
        location: Some(format!("/{bucket}")),
        ..Default::default()
    };
    // OpenDAL's create_dir succeeds on an existing directory, so existence is checked first.
    // Two concurrent creates may both succeed; with a single tenant, both callers end up with
    // the bucket they asked for.
    if dal.op.exists(&dir).await.map_err(|e| map(e, Ctx::Bucket))? {
        // AWS answers a re-create of a bucket the caller owns with 200 in us-east-1, for legacy
        // compatibility, and with BucketAlreadyOwnedByYou in every other region. The bucket
        // stays as it is either way.
        if dal.settings.region == LEGACY_REGION {
            return Ok(S3Response::new(created));
        }
        return Err(s3_error!(BucketAlreadyOwnedByYou));
    }
    dal.op
        .create_dir(&dir)
        .await
        .map_err(|e| map(e, Ctx::Bucket))?;
    Ok(S3Response::new(created))
}

pub(super) async fn delete_bucket(
    dal: &S3Dal,
    req: S3Request<DeleteBucketInput>,
) -> S3Result<S3Response<DeleteBucketOutput>> {
    let bucket = req.input.bucket;
    require_bucket(dal, &bucket).await?;
    let dir = bucket_dir(&bucket);
    let Some(mut dirs) = dirs_of_empty_bucket(dal, &dir).await? else {
        return Err(s3_error!(BucketNotEmpty));
    };
    // A bucket created later under this name must not inherit these uploads. They go before the
    // bucket, so a failure here leaves a bucket that a retry can still delete.
    multipart::delete_bucket_uploads(dal, &bucket).await?;
    // A child's path extends its parent's, so reverse byte order removes leaves first.
    dirs.sort_unstable_by(|a, b| b.cmp(a));
    for d in &dirs {
        // Non-recursive on purpose: an object written after the scan leaves its directory
        // non-empty, and removing the directory must then fail instead of taking the object
        // with it.
        if let Err(err) = dal.op.delete(d).await {
            if dirs_of_empty_bucket(dal, &dir).await?.is_none() {
                return Err(s3_error!(BucketNotEmpty));
            }
            return Err(map(err, Ctx::Bucket));
        }
    }
    Ok(S3Response::new(DeleteBucketOutput {}))
}

/// Every directory of the bucket at `dir`, itself included, or `None` if the bucket holds an
/// object. Only non-directories count: S3 has no directories, so ones left behind on a
/// hierarchical backend are not content, and a client that has deleted every listed object
/// must be able to delete the bucket. Entries of unknown type (symlinks, sockets) count as
/// content, because s3dal must not delete what it cannot classify.
async fn dirs_of_empty_bucket(dal: &S3Dal, dir: &str) -> S3Result<Option<Vec<String>>> {
    let mut lister = dal
        .op
        .lister_with(dir)
        .recursive(true)
        .await
        .map_err(|e| map(e, Ctx::Bucket))?;
    let mut dirs = vec![dir.to_owned()];
    while let Some(entry) = lister.try_next().await.map_err(|e| map(e, Ctx::Bucket))? {
        if !entry.metadata().is_dir() {
            return Ok(None);
        }
        if entry.path() != dir {
            dirs.push(entry.path().to_owned());
        }
    }
    Ok(Some(dirs))
}

pub(super) async fn head_bucket(
    dal: &S3Dal,
    req: S3Request<HeadBucketInput>,
) -> S3Result<S3Response<HeadBucketOutput>> {
    require_bucket(dal, &req.input.bucket).await?;
    // SDKs read x-amz-bucket-region to pick the signing region for the bucket.
    Ok(S3Response::new(HeadBucketOutput {
        bucket_region: Some(dal.settings.region.clone()),
        ..Default::default()
    }))
}

pub(super) async fn list_buckets(
    dal: &S3Dal,
    req: S3Request<ListBucketsInput>,
) -> S3Result<S3Response<ListBucketsOutput>> {
    let input = req.input;
    let max_buckets = match input.max_buckets {
        None => usize::MAX,
        Some(n) if (1..=MAX_BUCKETS).contains(&n) => n as usize,
        Some(_) => {
            return Err(s3_error!(
                InvalidArgument,
                "max-buckets must be between 1 and {MAX_BUCKETS}"
            ));
        }
    };
    let region = &dal.settings.region;

    let mut buckets = Vec::new();
    // Every bucket lives in the configured region, so a filter naming another region matches
    // none of them.
    if input.bucket_region.as_ref().is_none_or(|r| r == region) {
        let entries = dal.op.list("/").await.map_err(|e| map(e, Ctx::Other))?;
        for entry in entries {
            let meta = entry.metadata();
            let Some(name) = entry.path().strip_suffix('/') else {
                continue;
            };
            let has_prefix = input.prefix.as_deref().is_none_or(|p| name.starts_with(p));
            let after_token = input.continuation_token.as_deref().is_none_or(|t| name > t);
            // Valid names never start with `.`, so this also drops the reserved `.s3dal/`.
            if !meta.is_dir() || validate_bucket_name(name).is_err() || !has_prefix || !after_token
            {
                continue;
            }
            buckets.push(Bucket {
                name: Some(name.to_owned()),
                creation_date: meta
                    .last_modified()
                    .map(|t| Timestamp::from(SystemTime::from(t))),
                bucket_region: Some(region.clone()),
                bucket_arn: None,
            });
        }
    }
    // S3 lists buckets by name; readdir order is arbitrary, and pages rely on the order.
    buckets.sort_unstable_by(|a, b| a.name.cmp(&b.name));
    let continuation_token = if buckets.len() > max_buckets {
        buckets.truncate(max_buckets);
        buckets.last().and_then(|b| b.name.clone())
    } else {
        None
    };
    // Object stores list a bucket as a common prefix, which has no mtime. The aws CLI reads
    // CreationDate unconditionally, so it is looked up for the page being returned.
    let undated: Vec<(usize, String)> = buckets
        .iter()
        .enumerate()
        .filter(|(_, b)| b.creation_date.is_none())
        .filter_map(|(i, b)| Some((i, bucket_dir(b.name.as_deref()?))))
        .collect();
    let dates: Vec<(usize, Option<opendal::raw::Timestamp>)> = futures::stream::iter(undated)
        .map(|(i, dir)| async move { Ok::<_, s3s::S3Error>((i, marker_date(dal, &dir).await?)) })
        .buffer_unordered(DATE_CONCURRENCY)
        .try_collect()
        .await?;
    for (i, date) in dates {
        buckets[i].creation_date = date.map(|t| Timestamp::from(SystemTime::from(t)));
    }

    // AWS always names the owner here; scripts read their canonical ID from it
    // (`aws s3api list-buckets --query Owner.ID`).
    Ok(S3Response::new(ListBucketsOutput {
        buckets: Some(buckets),
        continuation_token,
        owner: Some(acl::owner()),
        prefix: input.prefix,
    }))
}

/// When the marker object CreateBucket wrote for `dir` was last modified. A listing of `dir`
/// holds the marker itself, as the first of its files, after any common prefixes of the same
/// page, so the search ends at the first file. `None` for a bucket that has no marker, such as
/// a prefix another tool wrote objects under.
async fn marker_date(dal: &S3Dal, dir: &str) -> S3Result<Option<opendal::raw::Timestamp>> {
    let mut lister = dal.op.lister(dir).await.map_err(|e| map(e, Ctx::Bucket))?;
    while let Some(entry) = lister.try_next().await.map_err(|e| map(e, Ctx::Bucket))? {
        if entry.path() == dir {
            return Ok(entry.metadata().last_modified());
        }
        if entry.metadata().is_file() {
            break;
        }
    }
    Ok(None)
}

pub(super) async fn get_bucket_location(
    dal: &S3Dal,
    req: S3Request<GetBucketLocationInput>,
) -> S3Result<S3Response<GetBucketLocationOutput>> {
    require_bucket(dal, &req.input.bucket).await?;
    let region = &dal.settings.region;
    Ok(S3Response::new(GetBucketLocationOutput {
        location_constraint: (region != LEGACY_REGION)
            .then(|| BucketLocationConstraint::from(region.clone())),
    }))
}

/// s3dal has no versioning, so every bucket is one that never had it enabled: AWS answers that
/// with no Status at all. Clients such as rclone ask before deleting and log an error on 501.
pub(super) async fn get_bucket_versioning(
    dal: &S3Dal,
    req: S3Request<GetBucketVersioningInput>,
) -> S3Result<S3Response<GetBucketVersioningOutput>> {
    require_bucket(dal, &req.input.bucket).await?;
    Ok(S3Response::new(GetBucketVersioningOutput::default()))
}

/// A GET of a bucket configuration s3dal never stores (policy, CORS, lifecycle, ...) answers as
/// S3 does for a bucket that has none: with the not-found error the operation defines. Clients
/// that read every configuration of a bucket, such as Terraform's `aws_s3_bucket`, take that as
/// "not set", where `NotImplemented` fails them. Setting one is still `NotImplemented`. Not
/// AWS's defaults for a new bucket: its default encryption (SSE-S3) would claim encryption
/// s3dal does not do.
pub(super) async fn unset_configuration<T>(
    dal: &S3Dal,
    bucket: &str,
    code: S3ErrorCode,
) -> S3Result<T> {
    require_bucket(dal, bucket).await?;
    // AWS answers every one of these with 404, ServerSideEncryptionConfigurationNotFoundError
    // too, which s3s lists as 400; and s3s sends a code it does not know
    // (NoSuchPublicAccessBlockConfiguration) as 500 unless it is given a status.
    let mut err = S3Error::new(code);
    err.set_status_code(StatusCode::NOT_FOUND);
    Err(err)
}

/// As [`unset_configuration`], for the configurations whose absence S3 reports as an empty
/// document (logging, notifications, transfer acceleration).
pub(super) async fn empty_configuration<T: Default>(
    dal: &S3Dal,
    bucket: &str,
) -> S3Result<S3Response<T>> {
    require_bucket(dal, bucket).await?;
    Ok(S3Response::new(T::default()))
}

/// The bucket owner pays, as for every bucket that never enabled Requester Pays.
pub(super) async fn get_bucket_request_payment(
    dal: &S3Dal,
    req: S3Request<GetBucketRequestPaymentInput>,
) -> S3Result<S3Response<GetBucketRequestPaymentOutput>> {
    require_bucket(dal, &req.input.bucket).await?;
    Ok(S3Response::new(GetBucketRequestPaymentOutput {
        payer: Some(Payer::from_static(Payer::BUCKET_OWNER)),
    }))
}

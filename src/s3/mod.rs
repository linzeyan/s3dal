//! The `s3s::S3` implementation. This file only delegates; each operation lives in the module
//! that owns its semantics, so parallel work on buckets, objects and listing does not collide.

mod acl;
mod bucket;
mod copy;
mod delete_objects;
mod fs;
mod list;
mod multipart;
mod naming;
mod object;
mod precondition;
mod store;
mod tmp;
mod unsupported;
mod upload;

use std::sync::Arc;
use std::time::Duration;

use opendal::Operator;
use s3s::dto::*;
use s3s::{S3, S3ErrorCode, S3Request, S3Response, S3Result};

use crate::backend::Profile;
use crate::config::Settings;

/// Cheap to clone: work that must outlive a request, such as a keep-alive
/// CompleteMultipartUpload, takes its own handle.
#[derive(Clone)]
pub struct S3Dal {
    pub(crate) op: Operator,
    pub(crate) profile: Profile,
    pub(crate) settings: Settings,
    locks: Arc<store::KeyLocks>,
    /// Keyed by upload directory: one Complete of an upload at a time.
    upload_locks: Arc<store::KeyLocks>,
    proofs: Arc<list::Proofs>,
}

impl S3Dal {
    /// `profile` must come from [`crate::backend::probe`] on the same operator.
    pub fn new(op: Operator, profile: Profile, settings: Settings) -> Self {
        Self {
            op,
            profile,
            settings,
            locks: Arc::default(),
            upload_locks: Arc::default(),
            proofs: Arc::default(),
        }
    }

    /// Removes the multipart uploads last used more than `age` ago: active uploads by when they
    /// were initiated, completed ones by when they were completed, leftovers without a record by
    /// their newest file, and every upload of a bucket that no longer exists. Returns how many it
    /// removed. An upload it cannot read or remove is logged and left for the next sweep; the
    /// error is for a backend that cannot even be listed.
    pub async fn sweep_stale_uploads(&self, age: Duration) -> anyhow::Result<usize> {
        multipart::sweep(self, age).await
    }

    /// Removes the temporary files of fs writes last written more than `age` ago: those of
    /// writes a shutdown or a crash cut off, which nothing else removes. Every write moves its
    /// file's mtime, so the files of live writes stay, also those of another process on the same
    /// root. Returns how many it removed; on other backends there are none.
    pub async fn sweep_stale_temp_files(&self, age: Duration) -> anyhow::Result<usize> {
        tmp::sweep(self, age).await
    }
}

#[async_trait::async_trait]
impl S3 for S3Dal {
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        bucket::create_bucket(self, req).await
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        bucket::delete_bucket(self, req).await
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        bucket::head_bucket(self, req).await
    }

    async fn list_buckets(
        &self,
        req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        bucket::list_buckets(self, req).await
    }

    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        bucket::get_bucket_location(self, req).await
    }

    async fn get_bucket_versioning(
        &self,
        req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        bucket::get_bucket_versioning(self, req).await
    }

    async fn get_bucket_policy(
        &self,
        req: S3Request<GetBucketPolicyInput>,
    ) -> S3Result<S3Response<GetBucketPolicyOutput>> {
        bucket::unset_configuration(self, &req.input.bucket, S3ErrorCode::NoSuchBucketPolicy).await
    }

    async fn get_bucket_policy_status(
        &self,
        req: S3Request<GetBucketPolicyStatusInput>,
    ) -> S3Result<S3Response<GetBucketPolicyStatusOutput>> {
        bucket::unset_configuration(self, &req.input.bucket, S3ErrorCode::NoSuchBucketPolicy).await
    }

    async fn get_bucket_cors(
        &self,
        req: S3Request<GetBucketCorsInput>,
    ) -> S3Result<S3Response<GetBucketCorsOutput>> {
        bucket::unset_configuration(
            self,
            &req.input.bucket,
            S3ErrorCode::NoSuchCORSConfiguration,
        )
        .await
    }

    async fn get_bucket_lifecycle_configuration(
        &self,
        req: S3Request<GetBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketLifecycleConfigurationOutput>> {
        bucket::unset_configuration(
            self,
            &req.input.bucket,
            S3ErrorCode::NoSuchLifecycleConfiguration,
        )
        .await
    }

    async fn get_bucket_tagging(
        &self,
        req: S3Request<GetBucketTaggingInput>,
    ) -> S3Result<S3Response<GetBucketTaggingOutput>> {
        bucket::unset_configuration(self, &req.input.bucket, S3ErrorCode::NoSuchTagSet).await
    }

    async fn get_bucket_encryption(
        &self,
        req: S3Request<GetBucketEncryptionInput>,
    ) -> S3Result<S3Response<GetBucketEncryptionOutput>> {
        bucket::unset_configuration(
            self,
            &req.input.bucket,
            S3ErrorCode::ServerSideEncryptionConfigurationNotFoundError,
        )
        .await
    }

    async fn get_object_lock_configuration(
        &self,
        req: S3Request<GetObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<GetObjectLockConfigurationOutput>> {
        bucket::unset_configuration(
            self,
            &req.input.bucket,
            S3ErrorCode::ObjectLockConfigurationNotFoundError,
        )
        .await
    }

    async fn get_bucket_website(
        &self,
        req: S3Request<GetBucketWebsiteInput>,
    ) -> S3Result<S3Response<GetBucketWebsiteOutput>> {
        bucket::unset_configuration(
            self,
            &req.input.bucket,
            S3ErrorCode::NoSuchWebsiteConfiguration,
        )
        .await
    }

    async fn get_bucket_replication(
        &self,
        req: S3Request<GetBucketReplicationInput>,
    ) -> S3Result<S3Response<GetBucketReplicationOutput>> {
        bucket::unset_configuration(
            self,
            &req.input.bucket,
            S3ErrorCode::ReplicationConfigurationNotFoundError,
        )
        .await
    }

    async fn get_bucket_ownership_controls(
        &self,
        req: S3Request<GetBucketOwnershipControlsInput>,
    ) -> S3Result<S3Response<GetBucketOwnershipControlsOutput>> {
        bucket::unset_configuration(
            self,
            &req.input.bucket,
            S3ErrorCode::OwnershipControlsNotFoundError,
        )
        .await
    }

    async fn get_public_access_block(
        &self,
        req: S3Request<GetPublicAccessBlockInput>,
    ) -> S3Result<S3Response<GetPublicAccessBlockOutput>> {
        // s3s has no variant for this code.
        let code = S3ErrorCode::Custom("NoSuchPublicAccessBlockConfiguration".into());
        bucket::unset_configuration(self, &req.input.bucket, code).await
    }

    async fn get_bucket_logging(
        &self,
        req: S3Request<GetBucketLoggingInput>,
    ) -> S3Result<S3Response<GetBucketLoggingOutput>> {
        bucket::empty_configuration(self, &req.input.bucket).await
    }

    async fn get_bucket_notification_configuration(
        &self,
        req: S3Request<GetBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketNotificationConfigurationOutput>> {
        bucket::empty_configuration(self, &req.input.bucket).await
    }

    async fn get_bucket_accelerate_configuration(
        &self,
        req: S3Request<GetBucketAccelerateConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketAccelerateConfigurationOutput>> {
        bucket::empty_configuration(self, &req.input.bucket).await
    }

    async fn get_bucket_request_payment(
        &self,
        req: S3Request<GetBucketRequestPaymentInput>,
    ) -> S3Result<S3Response<GetBucketRequestPaymentOutput>> {
        bucket::get_bucket_request_payment(self, req).await
    }

    async fn get_bucket_acl(
        &self,
        req: S3Request<GetBucketAclInput>,
    ) -> S3Result<S3Response<GetBucketAclOutput>> {
        acl::get_bucket_acl(self, req).await
    }

    async fn put_bucket_acl(
        &self,
        req: S3Request<PutBucketAclInput>,
    ) -> S3Result<S3Response<PutBucketAclOutput>> {
        acl::put_bucket_acl(self, req).await
    }

    async fn get_object_acl(
        &self,
        req: S3Request<GetObjectAclInput>,
    ) -> S3Result<S3Response<GetObjectAclOutput>> {
        acl::get_object_acl(self, req).await
    }

    async fn put_object_acl(
        &self,
        req: S3Request<PutObjectAclInput>,
    ) -> S3Result<S3Response<PutObjectAclOutput>> {
        acl::put_object_acl(self, req).await
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        object::put_object(self, req).await
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        object::get_object(self, req).await
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        object::head_object(self, req).await
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        object::delete_object(self, req).await
    }

    async fn get_object_tagging(
        &self,
        req: S3Request<GetObjectTaggingInput>,
    ) -> S3Result<S3Response<GetObjectTaggingOutput>> {
        object::get_object_tagging(self, req).await
    }

    async fn get_object_attributes(
        &self,
        req: S3Request<GetObjectAttributesInput>,
    ) -> S3Result<S3Response<GetObjectAttributesOutput>> {
        object::get_object_attributes(self, req).await
    }

    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        copy::copy_object(self, req).await
    }

    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        delete_objects::delete_objects(self, req).await
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        list::list_objects_v2(self, req).await
    }

    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        list::list_objects(self, req).await
    }

    async fn list_object_versions(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        list::list_object_versions(self, req).await
    }

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        multipart::create_multipart_upload(self, req).await
    }

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        multipart::upload_part(self, req).await
    }

    async fn upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        multipart::upload_part_copy(self, req).await
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        multipart::complete_multipart_upload(self, req).await
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        multipart::abort_multipart_upload(self, req).await
    }

    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        multipart::list_parts(self, req).await
    }

    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        multipart::list_multipart_uploads(self, req).await
    }
}

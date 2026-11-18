//! The `s3s::S3` implementation. This file only delegates; each operation lives in the module
//! that owns its semantics, so parallel work on buckets, objects and listing does not collide.

mod acl;
mod bucket;
mod bucket_state;
mod cache;
mod copy;
mod delete_objects;
mod fs;
mod lifecycle;
mod list;
mod multipart;
mod naming;
mod notification;
mod object;
mod precondition;
mod store;
pub(crate) mod swap;
mod thumbnail;
mod tier;
mod tier_sweep;
mod tmp;
mod unsupported;
mod upload;
mod usage;
mod version;

use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::{Duration, SystemTime};

use opendal::Operator;
use opendal_layer_observe_metrics_common::MetricsLayer;
use s3s::dto::*;
use s3s::{S3, S3ErrorCode, S3Request, S3Response, S3Result};

pub use bucket_state::Quota;
pub use lifecycle::Expired;
pub(crate) use lifecycle::ExpiredKind;
pub(crate) use tier::SkipReason;
pub use tier_sweep::TierSweep;
pub use usage::BucketUsage;

use crate::backend::Profile;
use crate::config::Settings;
use crate::events::{Hub, ObjectEvent, Subscriber};
use crate::metrics::Metrics;
use crate::notify::{self, Notifier};

/// Cheap to clone: work that must outlive a request, such as a keep-alive
/// CompleteMultipartUpload, takes its own handle.
#[derive(Clone)]
pub struct S3Dal {
    pub(crate) op: Operator,
    pub(crate) profile: Profile,
    /// Replaced whole when the config file changes; a request reads it once per decision, so a
    /// reload mid-request changes nothing it has already decided.
    settings: Arc<RwLock<Arc<Settings>>>,
    locks: Arc<store::KeyLocks>,
    /// Keyed by upload directory: one Complete of an upload at a time.
    upload_locks: Arc<store::KeyLocks>,
    proofs: Arc<list::Proofs>,
    versions: Arc<version::Sequence>,
    bucket_states: Arc<bucket_state::Cache>,
    events: Arc<Hub>,
    usage: Arc<usage::Tracker>,
    lifecycle_runs: Arc<lifecycle::Runs>,
    metrics: Arc<Metrics>,
    /// Sized once, at startup: `cache-size` takes a restart.
    cache: Option<Arc<cache::Cache>>,
    notifier: Arc<Notifier>,
    /// The task matching object events to notification rules, while any target is configured;
    /// without one nothing subscribes, so requests build no events for it.
    dispatcher: Arc<Mutex<Option<tokio::task::AbortHandle>>>,
    /// The cold tiers, fixed at startup (see [`Self::with_tiers`]); empty without `[[tier]]`.
    tiers: Arc<[crate::tier::Tier]>,
}

impl S3Dal {
    /// `profile` must come from [`crate::backend::probe`] on the same operator. With
    /// notification targets in `settings`, call it on a Tokio runtime, which sends to them.
    pub fn new(op: Operator, profile: Profile, settings: Settings) -> Self {
        let metrics = Arc::new(Metrics::new());
        let targets = settings.notify.clone();
        let cache = cache::Cache::new(settings.cache_size, &op, metrics.clone()).map(Arc::new);
        let dal = Self {
            op: op.layer(MetricsLayer::new(metrics.backend())),
            profile,
            settings: Arc::new(RwLock::new(Arc::new(settings))),
            locks: Arc::default(),
            upload_locks: Arc::default(),
            proofs: Arc::default(),
            versions: Arc::default(),
            bucket_states: Arc::default(),
            events: Arc::new(Hub::new()),
            usage: Arc::default(),
            lifecycle_runs: Arc::default(),
            metrics,
            cache,
            notifier: Arc::default(),
            dispatcher: Arc::default(),
            tiers: Arc::new([]),
        };
        dal.configure_notifications(&targets);
        dal
    }

    /// The server with `tiers`, from [`crate::tier::connect`] or [`crate::tier::Tier::open`] on
    /// this server's operator. A write naming a tier's storage class keeps its bytes there; a
    /// stub naming a tier not given here fails its reads.
    pub fn with_tiers(mut self, tiers: Vec<crate::tier::Tier>) -> Self {
        for tier in &tiers {
            self.metrics.tier(&tier.name);
        }
        self.tiers = tiers.into();
        self
    }

    /// Sends notifications to `targets` from now on, starting the dispatcher with the first
    /// target and stopping it with the last.
    fn configure_notifications(&self, targets: &[notify::Target]) {
        let mut dispatcher = self
            .dispatcher
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if targets.is_empty() && dispatcher.is_none() && self.notifier.is_empty() {
            return;
        }
        self.notifier.configure(targets, &self.metrics);
        match (targets.is_empty(), dispatcher.as_ref()) {
            (false, None) => {
                // Subscribed here, before the task first runs, so no event between this call
                // and its start is missed.
                let task = notification::dispatch(self.clone(), self.subscribe());
                *dispatcher = Some(tokio::spawn(task).abort_handle());
            }
            (true, Some(task)) => {
                task.abort();
                *dispatcher = None;
            }
            _ => {}
        }
    }

    /// The object events of this process from now on (F3): one per change to a key that took
    /// effect, written or removed, through any handle of this server.
    pub fn subscribe(&self) -> Subscriber {
        self.events.subscribe(self.metrics.clone())
    }

    /// Announces a change to a key once it has committed; a change that failed or was refused
    /// announces nothing. Its usage counts towards the bucket's quota before the request
    /// answers, so the next write sees it, whether anyone listens or not.
    pub(crate) fn emit(&self, event: ObjectEvent) {
        if let Some(change) = event.usage {
            self.usage.apply(self, &event.bucket, event.at, change);
        }
        if self.events.heard() {
            self.events.send(event);
        }
    }

    /// The bucket's quota, `None` without one.
    pub async fn bucket_quota(&self, bucket: &str) -> S3Result<Option<Quota>> {
        naming::require_bucket(self, bucket).await?;
        Ok(bucket_state::get(self, bucket).await?.quota.clone())
    }

    /// Sets the bucket's quota, or with `None` removes it. Stored with the bucket, as
    /// [`Self::set_bucket_public`] is. A bucket that had no quota is counted at once, rather
    /// than at the next usage scan: until a quota is set, its writes are not counted as they
    /// happen.
    pub async fn set_bucket_quota(&self, bucket: &str, quota: Option<Quota>) -> S3Result<()> {
        naming::require_bucket(self, bucket).await?;
        let had = bucket_state::get(self, bucket).await?.quota.is_some();
        bucket_state::update(self, bucket, |s| s.quota = quota.clone()).await?;
        if had || quota.is_none() {
            // Read again, so the metrics show the new limits at once.
            self.usage.stale(bucket);
            usage::current(self, bucket).await?;
            return Ok(());
        }
        usage::scan(self, bucket).await.map(drop)
    }

    /// What the bucket holds as far as this process knows: the last usage scan plus the
    /// changes this process made since, in a bucket with a quota (see `usage-scan-interval`).
    pub async fn bucket_usage(&self, bucket: &str) -> S3Result<BucketUsage> {
        naming::require_bucket(self, bucket).await?;
        usage::current(self, bucket).await
    }

    /// [`Self::bucket_usage`] and the quota of every bucket, by name.
    pub async fn buckets_usage(&self) -> S3Result<Vec<(String, BucketUsage, Option<Quota>)>> {
        usage::all(self).await
    }

    /// Counts what every bucket holds and records it for every instance sharing the backend;
    /// the background job `usage-scan`. Returns how many buckets it counted.
    pub async fn scan_usage(&self) -> anyhow::Result<usize> {
        usage::scan_all(self).await
    }

    /// Applies every bucket's lifecycle rules as of `now`: expires the objects, versions and
    /// delete markers they name and aborts the uploads, each announced as an object event.
    /// `now` is a parameter so tests can move the clock ahead; the background job `lifecycle`
    /// passes the time and applies each bucket once a day. Returns what was removed.
    pub async fn apply_lifecycle(&self, now: SystemTime) -> anyhow::Result<Expired> {
        lifecycle::apply(self, now, false).await
    }

    /// [`Self::apply_lifecycle`] for the buckets not yet applied today with the rules they have
    /// now, in this process; the background job `lifecycle`.
    pub async fn apply_due_lifecycle(&self, now: SystemTime) -> anyhow::Result<Expired> {
        lifecycle::apply(self, now, true).await
    }

    /// Whether anyone may download the bucket's objects, GET and HEAD, without a key pair.
    /// Listing it, writing to it and its other settings still take a signature.
    pub async fn bucket_public(&self, bucket: &str) -> S3Result<bool> {
        naming::require_bucket(self, bucket).await?;
        Ok(bucket_state::get(self, bucket).await?.public)
    }

    /// Opens the bucket's objects to downloads without a key pair, or closes them. Stored with
    /// the bucket in the backend: other instances sharing it follow within seconds, and a bucket
    /// deleted and created again under the name starts private.
    pub async fn set_bucket_public(&self, bucket: &str, public: bool) -> S3Result<()> {
        naming::require_bucket(self, bucket).await?;
        bucket_state::update(self, bucket, |s| s.public = public).await
    }

    /// [`Self::bucket_public`] for an unsigned request, without looking for the bucket: a
    /// bucket that does not exist has no state, so it is not public.
    pub(crate) async fn opens_downloads(&self, bucket: &str) -> S3Result<bool> {
        if naming::validate_bucket_name(bucket).is_err() {
            return Ok(false);
        }
        Ok(bucket_state::get(self, bucket).await?.public)
    }

    /// What the server counts, from requests to backend operations.
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    pub fn settings(&self) -> Arc<Settings> {
        // A panic while holding the lock leaves a whole value behind, since a write only swaps
        // the Arc, so a poisoned lock is still safe to read.
        self.settings
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Applies to requests that start from now on, and to the next stale-upload sweep;
    /// notification targets apply at once (see [`Notifier::configure`]).
    pub fn set_settings(&self, settings: Settings) {
        self.configure_notifications(&settings.notify);
        *self
            .settings
            .write()
            .unwrap_or_else(PoisonError::into_inner) = Arc::new(settings);
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

    /// Removes the tier objects no stub names that are older than `tier_sweep::TIER_GRACE` as of
    /// `now`, and counts and reports the stubs whose tier object is gone; the background job
    /// `tier-sweep`. `now` is a parameter so tests can move the clock past the grace.
    pub async fn sweep_tiers(&self, now: SystemTime) -> anyhow::Result<TierSweep> {
        tier_sweep::sweep(self, now).await
    }

    /// Removes the cached thumbnails whose object is gone or was overwritten since; the
    /// background job `thumbnail-sweep`. Returns how many it removed.
    #[cfg(feature = "thumbnail")]
    pub async fn sweep_thumbnails(&self) -> anyhow::Result<usize> {
        thumbnail::sweep(self).await
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
        version::get_bucket_versioning(self, req).await
    }

    async fn put_bucket_versioning(
        &self,
        req: S3Request<PutBucketVersioningInput>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        version::put_bucket_versioning(self, req).await
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
        lifecycle::get_bucket_lifecycle_configuration(self, req).await
    }

    async fn put_bucket_lifecycle_configuration(
        &self,
        req: S3Request<PutBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketLifecycleConfigurationOutput>> {
        lifecycle::put_bucket_lifecycle_configuration(self, req).await
    }

    async fn delete_bucket_lifecycle(
        &self,
        req: S3Request<DeleteBucketLifecycleInput>,
    ) -> S3Result<S3Response<DeleteBucketLifecycleOutput>> {
        lifecycle::delete_bucket_lifecycle(self, req).await
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
        notification::get_bucket_notification_configuration(self, req).await
    }

    async fn put_bucket_notification_configuration(
        &self,
        req: S3Request<PutBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketNotificationConfigurationOutput>> {
        notification::put_bucket_notification_configuration(self, req).await
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

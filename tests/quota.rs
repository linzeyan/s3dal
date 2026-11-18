//! Bucket quotas: a bucket with one refuses, with 403 QuotaExceeded, every write that would take
//! it past its bytes or its objects, every version counted. What it holds is the last usage scan
//! plus the changes this instance made since; another instance's writes count from the next scan.

mod common;

use std::time::{Duration, Instant};

use aws_sdk_s3::Client;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, VersioningConfiguration,
};
use common::{Backend, TestServer};
use s3dal::jobs::Runner;
use s3dal::{Quota, Settings};

const BUCKET: &str = "bkt";

async fn server() -> TestServer {
    let server = TestServer::start().await;
    server.create_bucket_dir(BUCKET).await;
    server
}

async fn limit(server: &TestServer, max_bytes: u64, max_objects: Option<u64>) {
    let quota = Quota {
        max_bytes,
        max_objects,
        unknown: Default::default(),
    };
    server
        .dal
        .set_bucket_quota(BUCKET, Some(quota))
        .await
        .unwrap();
}

/// `(objects, bytes)` as the server counts them.
async fn usage(server: &TestServer) -> (u64, u64) {
    let usage = server.dal.bucket_usage(BUCKET).await.unwrap();
    (usage.objects, usage.bytes)
}

fn assert_refused<T: std::fmt::Debug, E: ProvideErrorMetadata + std::fmt::Debug>(
    result: Result<T, SdkError<E>>,
) {
    let err = result.expect_err("the write went through");
    assert_eq!(err.code(), Some("QuotaExceeded"), "{err:?}");
    assert_eq!(err.raw_response().unwrap().status().as_u16(), 403);
}

// Returned as the SDK returns it, so tests inspect it the same way; its size does not
// matter in a test.
#[allow(clippy::result_large_err)]
async fn put(
    client: &Client,
    key: &str,
    len: usize,
) -> Result<
    aws_sdk_s3::operation::put_object::PutObjectOutput,
    SdkError<aws_sdk_s3::operation::put_object::PutObjectError>,
> {
    client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from(vec![b'x'; len]))
        .send()
        .await
}

async fn delete(client: &Client, key: &str) {
    client
        .delete_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .unwrap();
}

async fn exists(client: &Client, key: &str) -> bool {
    client
        .head_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .is_ok()
}

#[tokio::test]
async fn puts_may_fill_the_quota_exactly_but_not_one_byte_or_object_past_it() {
    // The limit is what an operator promised the bucket's owner: reaching it is allowed, going
    // past it by any amount is not, and a refused write leaves nothing behind.
    let server = server().await;
    let client = &server.client;
    limit(&server, 100, Some(3)).await;
    put(client, "a", 60).await.unwrap();
    put(client, "b", 40).await.unwrap();
    assert_eq!(usage(&server).await, (2, 100));
    assert_refused(put(client, "c", 1).await);
    assert!(!exists(client, "c").await);

    // An overwrite counts what it adds over what it replaces: up to the same size fits a full
    // bucket, one byte more does not, and a smaller one frees space.
    put(client, "a", 60).await.unwrap();
    assert_refused(put(client, "a", 61).await);
    put(client, "a", 10).await.unwrap();
    assert_eq!(usage(&server).await, (2, 50));
    put(client, "c", 50).await.unwrap();
    assert_eq!(usage(&server).await, (3, 100));

    // The object limit holds for an empty object too, which adds no bytes; a delete makes
    // room again.
    delete(client, "c").await;
    put(client, "empty", 0).await.unwrap();
    assert_eq!(usage(&server).await, (3, 50));
    assert_refused(put(client, "more", 0).await);
    delete(client, "a").await;
    put(client, "more", 0).await.unwrap();
    assert_eq!(usage(&server).await, (3, 40));
    server.assert_no_temp_files().await;
}

#[tokio::test]
async fn a_copy_counts_its_source_size() {
    // CopyObject writes without a body to measure; the source's size is what it adds.
    let server = server().await;
    let client = &server.client;
    limit(&server, 100, None).await;
    put(client, "src", 50).await.unwrap();
    let copy = |to: &'static str| {
        client
            .copy_object()
            .bucket(BUCKET)
            .key(to)
            .copy_source(format!("{BUCKET}/src"))
            .send()
    };
    copy("fits").await.unwrap();
    assert_eq!(usage(&server).await, (2, 100));
    assert_refused(copy("over").await);
    assert!(!exists(client, "over").await);
    // Onto a key of the same size, it replaces as much as it adds.
    copy("fits").await.unwrap();
    assert_eq!(usage(&server).await, (2, 100));
}

#[allow(clippy::result_large_err)]
async fn start_upload(
    client: &Client,
    key: &str,
) -> Result<
    String,
    SdkError<aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadError>,
> {
    let out = client
        .create_multipart_upload()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await?;
    Ok(out.upload_id().unwrap().to_owned())
}

/// Sends one part of `len` bytes and completes the upload.
#[allow(clippy::result_large_err)]
async fn finish_upload(
    client: &Client,
    key: &str,
    id: &str,
    len: usize,
) -> Result<
    aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput,
    SdkError<aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadError>,
> {
    let part = client
        .upload_part()
        .bucket(BUCKET)
        .key(key)
        .upload_id(id)
        .part_number(1)
        .body(ByteStream::from(vec![b'p'; len]))
        .send()
        .await
        .unwrap();
    client
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key(key)
        .upload_id(id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(part.e_tag().unwrap())
                        .build(),
                )
                .build(),
        )
        .send()
        .await
}

#[tokio::test]
async fn a_complete_counts_the_assembled_object_and_uploads_start_only_within_the_quota() {
    // Parts are not counted, as S3 does not bill them as objects: the assembled object is, when
    // the upload completes. An upload cannot start in a bucket already past its quota, where its
    // Complete could only be refused after every part was sent.
    let server = server().await;
    let client = &server.client;
    limit(&server, 100, None).await;
    put(client, "a", 40).await.unwrap();
    let fits = start_upload(client, "fits").await.unwrap();
    let over = start_upload(client, "over").await.unwrap();
    finish_upload(client, "fits", &fits, 60).await.unwrap();
    assert_eq!(usage(&server).await, (2, 100));
    assert_refused(finish_upload(client, "over", &over, 1).await);
    assert!(!exists(client, "over").await);

    // Exactly full, an upload may still start: it may replace an object.
    let replace = start_upload(client, "a").await.unwrap();
    finish_upload(client, "a", &replace, 40).await.unwrap();
    // Past a lowered quota, none starts.
    limit(&server, 99, None).await;
    assert_refused(start_upload(client, "late").await);
}

#[tokio::test]
async fn in_a_versioned_bucket_every_version_counts() {
    // An overwrite keeps the old version, which still takes space; only removing a version for
    // good frees it.
    let server = server().await;
    let client = &server.client;
    limit(&server, 100, None).await;
    client
        .put_bucket_versioning()
        .bucket(BUCKET)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let first = put(client, "k", 60).await.unwrap();
    put(client, "k", 40).await.unwrap();
    assert_eq!(usage(&server).await, (2, 100));
    // In an unversioned bucket this overwrite would replace 40 bytes; here it adds 40 more.
    assert_refused(put(client, "k", 40).await);
    // A delete marker frees nothing; deleting the version does.
    delete(client, "k").await;
    assert_refused(put(client, "k", 1).await);
    client
        .delete_object()
        .bucket(BUCKET)
        .key("k")
        .version_id(first.version_id().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(usage(&server).await, (1, 40));
    put(client, "k", 60).await.unwrap();

    // A scan finds the same: every version, no marker.
    tokio::time::sleep(Duration::from_millis(200)).await;
    server.dal.scan_usage().await.unwrap();
    assert_eq!(usage(&server).await, (2, 100));
}

#[tokio::test]
async fn usage_is_the_last_scan_plus_the_changes_made_since() {
    // Objects written behind s3dal's back, or by another instance, count from the next scan;
    // this instance's own writes count at once; a scan replaces what came before it.
    let server = server().await;
    let client = &server.client;
    server.plant(BUCKET, "planted", &[0; 30]).await;
    server.plant(BUCKET, "dir/planted", &[0; 20]).await;
    // Setting a quota counts the bucket at once.
    limit(&server, 1_000, None).await;
    let scanned = server.dal.bucket_usage(BUCKET).await.unwrap();
    assert_eq!((scanned.objects, scanned.bytes), (2, 50));
    assert!(scanned.scanned.is_some());

    put(client, "own", 7).await.unwrap();
    put(client, "folder/", 0).await.unwrap();
    assert_eq!(
        usage(&server).await,
        (3, 57),
        "a folder marker holds nothing"
    );
    server.plant(BUCKET, "late", &[0; 100]).await;
    assert_eq!(usage(&server).await, (3, 57));

    // The changes before the scan are in it, so they are not added again.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(server.dal.scan_usage().await.unwrap(), 1);
    assert_eq!(usage(&server).await, (4, 157));
    delete(client, "late").await;
    assert_eq!(usage(&server).await, (3, 57));

    // The metrics show the same, with the quota.
    let metrics = server.dal.metrics().render();
    for line in [
        "s3dal_bucket_objects{bucket=\"bkt\"} 3\n",
        "s3dal_bucket_bytes{bucket=\"bkt\"} 57\n",
        "s3dal_bucket_quota_bytes{bucket=\"bkt\"} 1000\n",
    ] {
        assert!(metrics.contains(line), "{line}{metrics}");
    }
    assert!(
        !metrics.contains("s3dal_bucket_quota_objects{"),
        "{metrics}"
    );
}

#[tokio::test]
async fn a_bucket_created_again_under_the_name_starts_without_a_quota() {
    // The quota and the count belong to the bucket, not to its name.
    let server = server().await;
    let client = &server.client;
    limit(&server, 10, None).await;
    put(client, "a", 10).await.unwrap();
    delete(client, "a").await;
    client.delete_bucket().bucket(BUCKET).send().await.unwrap();
    assert!(!server.exists(&format!(".s3dal/usage/{BUCKET}.json")).await);
    client.create_bucket().bucket(BUCKET).send().await.unwrap();
    assert_eq!(server.dal.bucket_quota(BUCKET).await.unwrap(), None);
    put(client, "big", 100).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn another_instance_refuses_once_a_scan_has_counted_the_writes() {
    // Two instances on one backend: B writes the bucket full, and A, which only learns of B's
    // writes from a scan, must refuse within the scan interval plus the time it trusts the scan
    // it read. The measured lag is printed for guide/operations.md.
    let settings = Settings {
        usage_scan_interval: Some(Duration::from_secs(1)),
        stale_upload_age: None,
        ..Settings::default()
    };
    let a = TestServer::start_with(settings.clone()).await;
    let b = TestServer::start_on(Backend::sharing(&a), settings).await;
    a.create_bucket_dir(BUCKET).await;
    limit(&a, 100, None).await;
    // B scans, as the holder of the lease; a 3 s lease ticks every second.
    let runner = tokio::spawn(
        Runner::new(b.dal.clone())
            .with_lease(Duration::from_secs(3))
            .run(),
    );
    put(&b.client, "full", 100).await.unwrap();
    assert_refused(put(&b.client, "b-more", 1).await);

    let wrote = Instant::now();
    let mut through = 0;
    let lag = loop {
        match put(&a.client, &format!("a-{through}"), 1).await {
            Ok(_) => through += 1,
            Err(err) => {
                assert_eq!(err.code(), Some("QuotaExceeded"), "{err:?}");
                break wrote.elapsed();
            }
        }
        assert!(
            wrote.elapsed() < Duration::from_secs(20),
            "A still takes writes"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    eprintln!("two-instance lag: {lag:?}, {through} one-byte writes let through on A");
    // Interval (1 s) + runner tick (1 s) + scan time + scan trusted for 5 s, with slack.
    assert!(lag < Duration::from_secs(10), "{lag:?}");
    runner.abort();
}

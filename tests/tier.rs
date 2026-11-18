//! Cold tiers (cold-tier.md): a PUT, a copy or a Complete naming a tier's storage class leaves an
//! empty stub at the key and the bytes in the tier; reads, listings, copies and deletes treat the
//! object as the one it stands for, and a copy onto itself moves it between classes.
//!
//! The suite's backend is the primary with an fs tier; the tests named `with_an_fs_primary_*`
//! swap them, with the suite's backend as the tier. The rules that differ per backend (the
//! listing stat on native ETags, the read cache, the stub's own ETag) are pinned by the runs
//! under scripts/with-rustfs.sh, with-azurite.sh and with-fake-gcs.sh.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_sdk_s3::Client;
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::primitives::{ByteStream, DateTime};
use aws_sdk_s3::types::{
    BucketLifecycleConfiguration, BucketVersioningStatus, ChecksumAlgorithm, ChecksumMode,
    CompletedMultipartUpload, CompletedPart, Delete, ExpirationStatus, LifecycleExpiration,
    LifecycleRule, LifecycleRuleFilter, NoncurrentVersionExpiration, NoncurrentVersionTransition,
    ObjectAttributes, ObjectIdentifier, StorageClass, Transition, TransitionStorageClass,
    VersioningConfiguration,
};
use common::{Backend, TestServer};
use opendal::{ErrorKind, Operator};
use s3dal::events::EventKind;
use s3dal::meta::{self, Computed, PartLayout, Stub};
use s3dal::{Quota, Settings};
use tokio::sync::Semaphore;

const BUCKET: &str = "cold";
const CLASS: &str = "STANDARD_IA";
const TIER: &str = "hdd";
const EMPTY_MD5: &str = "d41d8cd98f00b204e9800998ecf8427e";

/// A server with one tier, and the tier's backend, kept alive with it.
struct Setup {
    server: TestServer,
    tier: Operator,
    _tier_backend: Backend,
}

async fn setup_on(primary: Backend, tier: Backend, tier_op: Operator, settings: Settings) -> Setup {
    let server =
        TestServer::start_tiered(primary, settings, &[(TIER, CLASS, tier_op.clone())]).await;
    server
        .client
        .create_bucket()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    Setup {
        server,
        tier: tier_op,
        _tier_backend: tier,
    }
}

/// The suite's backend as the primary, fs as the tier.
async fn setup() -> Setup {
    setup_with(Settings::default()).await
}

async fn setup_with(settings: Settings) -> Setup {
    let tier = Backend::fs().await;
    let op = tier.op.clone();
    setup_on(Backend::from_env().await, tier, op, settings).await
}

/// fs as the primary, the suite's backend as the tier.
async fn fs_primary() -> Setup {
    let tier = Backend::from_env().await;
    let op = tier.op.clone();
    setup_on(Backend::fs().await, tier, op, Settings::default()).await
}

/// Content that differs at every offset, so a misplaced range shows.
fn content(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

async fn put_class(
    client: &Client,
    key: &str,
    body: Vec<u8>,
    class: Option<StorageClass>,
) -> (String, Option<String>) {
    let out = client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .set_storage_class(class)
        .content_type("application/x-cold")
        .metadata("origin", "tier-test")
        .body(ByteStream::from(body))
        .send()
        .await
        .unwrap();
    (
        out.e_tag().unwrap().trim_matches('"').to_owned(),
        out.version_id().map(str::to_owned),
    )
}

/// A PUT into the tier: its ETag and version id.
async fn put_cold(client: &Client, key: &str, body: Vec<u8>) -> (String, Option<String>) {
    put_class(client, key, body, Some(StorageClass::StandardIa)).await
}

async fn put_hot(client: &Client, key: &str, body: Vec<u8>) -> (String, Option<String>) {
    put_class(client, key, body, None).await
}

async fn get(client: &Client, key: &str, version: Option<&str>) -> Result<Vec<u8>, String> {
    let out = client
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .set_version_id(version.map(str::to_owned))
        .send()
        .await
        .map_err(|e| e.code().unwrap_or("none").to_owned())?;
    Ok(out.body.collect().await.unwrap().into_bytes().to_vec())
}

async fn get_range(client: &Client, key: &str, range: &str) -> (Vec<u8>, Option<String>) {
    let out = client
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .range(range)
        .send()
        .await
        .unwrap();
    let content_range = out.content_range().map(str::to_owned);
    (
        out.body.collect().await.unwrap().into_bytes().to_vec(),
        content_range,
    )
}

/// A client that does not retry, to see the status of a failure SDKs treat as transient.
fn no_retry(server: &TestServer) -> Client {
    Client::from_conf(
        server
            .client
            .config()
            .to_builder()
            .retry_config(RetryConfig::disabled())
            .build(),
    )
}

/// The tier objects of `key`, by name.
async fn tier_objects(tier: &Operator, key: &str) -> Vec<String> {
    let dir = format!(".s3dal/tier/{BUCKET}/{key}!!/");
    match tier.list(&dir).await {
        Ok(entries) => entries
            .iter()
            .filter(|e| e.metadata().is_file())
            .map(|e| e.name().to_owned())
            .collect(),
        Err(err) if err.kind() == ErrorKind::NotFound => Vec::new(),
        Err(err) => panic!("cannot list {dir}: {err}"),
    }
}

/// The length stored at `key` in the primary, what other tools reading it see.
async fn physical_len(server: &TestServer, path: &str) -> u64 {
    server.op.stat(path).await.unwrap().content_length()
}

/// Rewrites `key`'s stub as `stub` says, keeping the rest of what it stores, with `parts` as
/// its part layout: what a cold multipart object (T2) or a damaged stub would hold.
async fn rewrite_stub(
    server: &TestServer,
    key: &str,
    parts: Option<PartLayout>,
    stub: impl FnOnce(&meta::ObjectMeta) -> (String, String, u64),
) {
    let path = format!("{BUCKET}/{key}");
    let m = server.op.stat(&path).await.unwrap();
    let obj = meta::resolve(&server.profile, &m);
    let (tier, id, size) = stub(&obj);
    let computed = Computed {
        md5_hex: Some(obj.etag.clone()),
        checksum: obj.checksum.clone(),
        parts,
    };
    let mut opts = meta::write_options(&server.profile, obj.attrs.clone(), computed);
    meta::set_stub(
        &server.profile,
        &mut opts,
        &Stub {
            tier: &tier,
            id: &id,
            size,
            etag: &obj.etag,
            last_modified: obj.last_modified.unwrap(),
        },
    );
    server
        .op
        .write_options(&path, Vec::<u8>::new(), opts)
        .await
        .unwrap();
}

/// The stub's tier name, id and size as they are.
fn as_is(obj: &meta::ObjectMeta) -> (String, String, u64) {
    let tier = obj.tier.clone().expect("a stub");
    (
        tier.name,
        tier.id.expect("an intact stub"),
        obj.content_length,
    )
}

#[tokio::test]
async fn a_put_naming_a_tiers_class_leaves_an_empty_stub_and_the_bytes_in_the_tier() {
    // The point of a tier: the primary keeps the namespace, the tier the bytes. Read back, the
    // object is the one written, under the MD5 ETag a client checks its upload against.
    let s = setup().await;
    let body = content(3000, 1);
    let (etag, _) = put_cold(&s.server.client, "k", body.clone()).await;
    assert_eq!(etag, meta::md5_hex(&body));
    assert_eq!(physical_len(&s.server, &format!("{BUCKET}/k")).await, 0);
    let ids = tier_objects(&s.tier, "k").await;
    assert_eq!(ids.len(), 1, "{ids:?}");
    let held = s
        .tier
        .read(&format!(".s3dal/tier/{BUCKET}/k!!/{}", ids[0]))
        .await
        .unwrap()
        .to_vec();
    assert_eq!(held, body);
    assert_eq!(get(&s.server.client, "k", None).await.unwrap(), body);
    s.server.assert_no_temp_files().await;

    // HEAD answers from the stub alone: the stored ETag, not the empty object's MD5 the
    // backend computed for it (RustFS), the logical size, the tier's class and the headers.
    let head = s
        .server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(head.e_tag().unwrap().trim_matches('"'), etag);
    assert_ne!(head.e_tag().unwrap().trim_matches('"'), EMPTY_MD5);
    assert_eq!(head.content_length(), Some(3000));
    assert_eq!(head.storage_class(), Some(&StorageClass::StandardIa));
    assert_eq!(head.content_type(), Some("application/x-cold"));
    assert_eq!(
        head.metadata().unwrap().get("origin").map(String::as_str),
        Some("tier-test")
    );
    let got = s
        .server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(got.storage_class(), Some(&StorageClass::StandardIa));
    assert_eq!(got.e_tag().unwrap().trim_matches('"'), etag);
}

#[tokio::test]
async fn ranges_parts_and_conditions_read_a_cold_object_as_a_hot_one() {
    // Download managers fetch ranges and parts with If-Match, and browsers revalidate with
    // If-None-Match: all of it answers from the stub, and only the bytes come from the tier.
    let s = setup().await;
    let body = content(13, 7);
    let (etag, _) = put_cold(&s.server.client, "k", body.clone()).await;
    let client = &s.server.client;

    let (got, range) = get_range(client, "k", "bytes=3-9").await;
    assert_eq!(got, body[3..10]);
    assert_eq!(range.as_deref(), Some("bytes 3-9/13"));
    let (got, _) = get_range(client, "k", "bytes=-4").await;
    assert_eq!(got, body[9..]);

    let quoted = format!("\"{etag}\"");
    let err = client
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .if_none_match(&quoted)
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.raw_response().unwrap().status().as_u16(), 304);
    let err = client
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .if_match("\"0123\"")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.raw_response().unwrap().status().as_u16(), 412);

    // The layout of a multipart-sized object, as a Complete into a tier will store it (T2).
    rewrite_stub(&s.server, "k", Some(PartLayout::new(&[5, 5, 3])), as_is).await;
    for (n, want) in [(1, &body[..5]), (2, &body[5..10]), (3, &body[10..])] {
        let out = client
            .get_object()
            .bucket(BUCKET)
            .key("k")
            .part_number(n)
            .if_match(&quoted)
            .send()
            .await
            .unwrap();
        assert_eq!(out.parts_count(), Some(3));
        let got = out.body.collect().await.unwrap().into_bytes().to_vec();
        assert_eq!(got, want, "part {n}");
    }
}

#[tokio::test]
async fn a_real_empty_object_stays_hot_and_empty() {
    // An empty body has nothing to move; it stays an empty object, reported STANDARD, so it is
    // never taken for a stub and a GET of it is never a tier read.
    let s = setup().await;
    let (etag, _) = put_cold(&s.server.client, "empty", Vec::new()).await;
    assert_eq!(etag, EMPTY_MD5);
    assert!(tier_objects(&s.tier, "empty").await.is_empty());
    assert_eq!(get(&s.server.client, "empty", None).await.unwrap(), b"");
    let head = s
        .server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("empty")
        .send()
        .await
        .unwrap();
    assert_eq!(head.storage_class(), None);
    assert_eq!(head.content_length(), Some(0));

    // Nor does a copy of it, or an upload of no bytes, naming the tier's class.
    let copied = s
        .server
        .client
        .copy_object()
        .bucket(BUCKET)
        .key("copied")
        .copy_source(format!("{BUCKET}/empty"))
        .storage_class(StorageClass::StandardIa)
        .send()
        .await;
    assert!(copied.is_ok(), "{copied:?}");
    let client = &s.server.client;
    let (id, parts) = upload_parts(
        client,
        "uploaded",
        Some(StorageClass::StandardIa),
        &[vec![]],
    )
    .await;
    complete(client, "uploaded", &id, &parts).await;
    for key in ["copied", "uploaded"] {
        assert_eq!(self::head(client, key).await.storage_class(), None, "{key}");
        assert!(tier_objects(&s.tier, key).await.is_empty(), "{key}");
        assert_eq!(get(client, key, None).await.unwrap(), b"", "{key}");
    }
}

#[tokio::test]
async fn a_class_no_tier_has_stays_advisory() {
    // Clients send classes out of habit (`aws s3 cp --storage-class GLACIER`); without a tier
    // for it the object is stored hot, as before tiers, and reported STANDARD.
    let s = setup().await;
    let body = content(100, 3);
    put_class(
        &s.server.client,
        "k",
        body.clone(),
        Some(StorageClass::Glacier),
    )
    .await;
    assert_eq!(physical_len(&s.server, &format!("{BUCKET}/k")).await, 100);
    assert!(tier_objects(&s.tier, "k").await.is_empty());
    let head = s
        .server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(head.storage_class(), None);
    assert_eq!(get(&s.server.client, "k", None).await.unwrap(), body);
}

#[tokio::test]
async fn a_dangling_stub_answers_500_never_404() {
    // A tier object lost behind s3dal's back is a fault to fix, not a deleted key: NoSuchKey
    // would make a sync tool delete its copy elsewhere. HEAD still answers from the stub.
    let s = setup().await;
    put_cold(&s.server.client, "k", content(50, 2)).await;
    for id in tier_objects(&s.tier, "k").await {
        s.tier
            .delete(&format!(".s3dal/tier/{BUCKET}/k!!/{id}"))
            .await
            .unwrap();
    }
    let err = no_retry(&s.server)
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("InternalError"), "{err:?}");
    assert_eq!(err.raw_response().unwrap().status().as_u16(), 500);
    let head = s
        .server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(head.content_length(), Some(50));
}

#[tokio::test]
async fn a_damaged_stub_or_one_naming_an_unknown_tier_answers_500() {
    // Its bytes cannot be found: an empty 200 would hand out a wrong object, a 404 would make
    // sync tools delete it elsewhere.
    let s = setup().await;
    put_cold(&s.server.client, "k", content(20, 4)).await;
    let client = no_retry(&s.server);
    rewrite_stub(&s.server, "k", None, |obj| {
        let (tier, _, size) = as_is(obj);
        (tier, "not-a-tier-id".to_owned(), size)
    })
    .await;
    let err = client
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("InternalError"), "{err:?}");

    put_cold(&s.server.client, "k", content(20, 4)).await;
    rewrite_stub(&s.server, "k", None, |obj| {
        let (_, id, size) = as_is(obj);
        ("retired".to_owned(), id, size)
    })
    .await;
    let err = client
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("InternalError"), "{err:?}");
}

#[tokio::test]
async fn a_get_racing_an_overwrite_answers_503() {
    // The overwrite removes the tier object the GET stat'ed its stub for. The GET must fail
    // with a status clients retry, as a pinned read of a replaced object does, never with the
    // 500 of a lost tier object, nor a 404.
    let tier = Backend::fs().await;
    let gate = Arc::new(gate::Gate::default());
    let op = tier.op.clone().layer(gate::Gated::reads(gate.clone()));
    let s = setup_on(Backend::from_env().await, tier, op, Settings::default()).await;
    put_cold(&s.server.client, "k", content(64, 1)).await;

    gate.arm();
    let client = no_retry(&s.server);
    let racing = tokio::spawn(async move {
        client
            .get_object()
            .bucket(BUCKET)
            .key("k")
            .send()
            .await
            .map(drop)
            .map_err(|err| {
                (
                    err.code().map(str::to_owned),
                    err.raw_response().map(|r| r.status().as_u16()),
                )
            })
    });
    gate.reached().await;
    put_cold(&s.server.client, "k", content(64, 2)).await;
    gate.release();
    let (code, status) = racing.await.unwrap().unwrap_err();
    assert_eq!(code.as_deref(), Some("SlowDown"));
    assert_eq!(status, Some(503));
    assert_eq!(
        get(&s.server.client, "k", None).await.unwrap(),
        content(64, 2)
    );
}

#[tokio::test]
async fn deleting_or_overwriting_removes_the_tier_object_in_an_unversioned_bucket() {
    // Nothing names a replaced or deleted object's tier object any more; left there it would
    // take the tier's space until the sweep.
    let s = setup().await;
    let client = &s.server.client;
    put_cold(client, "k", content(40, 1)).await;
    assert_eq!(tier_objects(&s.tier, "k").await.len(), 1);
    // A cold overwrite of a cold object: a new tier object, the old one gone.
    put_cold(client, "k", content(40, 2)).await;
    assert_eq!(tier_objects(&s.tier, "k").await.len(), 1);
    assert_eq!(get(client, "k", None).await.unwrap(), content(40, 2));
    // A hot overwrite.
    put_hot(client, "k", content(40, 3)).await;
    assert!(tier_objects(&s.tier, "k").await.is_empty());

    put_cold(client, "k", content(40, 4)).await;
    client
        .delete_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    assert!(tier_objects(&s.tier, "k").await.is_empty());

    // DeleteObjects removes each, batched per tier.
    for key in ["a", "b"] {
        put_cold(client, key, content(40, 5)).await;
    }
    let delete = Delete::builder()
        .objects(ObjectIdentifier::builder().key("a").build().unwrap())
        .objects(ObjectIdentifier::builder().key("b").build().unwrap())
        .build()
        .unwrap();
    let out = client
        .delete_objects()
        .bucket(BUCKET)
        .delete(delete)
        .send()
        .await
        .unwrap();
    assert!(out.errors().is_empty(), "{:?}", out.errors());
    assert!(tier_objects(&s.tier, "a").await.is_empty());
    assert!(tier_objects(&s.tier, "b").await.is_empty());
}

#[tokio::test]
async fn a_failed_conditional_put_leaves_the_key_and_no_tier_object() {
    // The condition holds the stub, not the tier write; a refused one must not leave a tier
    // object nobody names, nor change the key. Where the backend checks conditions the write
    // is refused before the body moves.
    conditional_put_fails_cleanly(&setup().await).await;
}

#[tokio::test]
async fn with_an_fs_primary_a_failed_conditional_put_leaves_the_key_and_no_tier_object() {
    // fs checks the condition only at the commit, once the tier object is written.
    conditional_put_fails_cleanly(&fs_primary().await).await;
}

async fn conditional_put_fails_cleanly(s: &Setup) {
    let client = &s.server.client;
    put_cold(client, "k", content(30, 1)).await;
    let err = client
        .put_object()
        .bucket(BUCKET)
        .key("k")
        .storage_class(StorageClass::StandardIa)
        .if_none_match("*")
        .body(ByteStream::from(content(30, 2)))
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.raw_response().unwrap().status().as_u16(), 412);
    assert_eq!(tier_objects(&s.tier, "k").await.len(), 1);
    assert_eq!(get(client, "k", None).await.unwrap(), content(30, 1));
}

async fn enable_versioning(client: &Client) {
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
}

async fn delete_version(client: &Client, key: &str, version: &str) {
    client
        .delete_object()
        .bucket(BUCKET)
        .key(key)
        .version_id(version)
        .send()
        .await
        .unwrap();
}

/// What a versioned bucket must do with a cold version, on `s`.
async fn versions_keep_their_tier_objects(s: &Setup) {
    let client = &s.server.client;
    enable_versioning(client).await;
    let first = content(70, 1);
    let (_, v1) = put_cold(client, "k", first.clone()).await;
    let v1 = v1.unwrap();
    let (_, v2) = put_hot(client, "k", content(70, 2)).await;
    let v2 = v2.unwrap();
    // Archived as the empty stub it is: a copy of its logical size would read past the stub.
    let holder = format!(".s3dal/versions/{BUCKET}/k!!/");
    let entries = s.server.names(&holder).await;
    assert_eq!(entries.len(), 1, "{entries:?}");
    assert_eq!(
        physical_len(&s.server, &format!("{holder}{}", entries[0])).await,
        0
    );
    assert_eq!(tier_objects(&s.tier, "k").await.len(), 1);
    assert_eq!(get(client, "k", Some(&v1)).await.unwrap(), first);

    // A delete marker removes nothing.
    client
        .delete_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(tier_objects(&s.tier, "k").await.len(), 1);
    let markers = client
        .list_object_versions()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    let marker = markers.delete_markers()[0].version_id().unwrap().to_owned();
    delete_version(client, "k", &marker).await;
    delete_version(client, "k", &v2).await;
    // v1 is current again, still cold, its tier object untouched.
    assert_eq!(get(client, "k", None).await.unwrap(), first);
    assert_eq!(physical_len(&s.server, &format!("{BUCKET}/k")).await, 0);
    let head = client
        .head_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(head.storage_class(), Some(&StorageClass::StandardIa));
    assert_eq!(head.version_id(), Some(v1.as_str()));
    assert_eq!(tier_objects(&s.tier, "k").await.len(), 1);

    // Deleting the version for good removes its bytes.
    delete_version(client, "k", &v1).await;
    assert!(tier_objects(&s.tier, "k").await.is_empty());

    // So does deleting an archived cold version.
    let (_, v3) = put_cold(client, "k", content(70, 3)).await;
    put_hot(client, "k", content(70, 4)).await;
    assert_eq!(tier_objects(&s.tier, "k").await.len(), 1);
    delete_version(client, "k", &v3.unwrap()).await;
    assert!(tier_objects(&s.tier, "k").await.is_empty());
}

#[tokio::test]
async fn a_versioned_bucket_keeps_the_tier_object_until_its_version_is_deleted() {
    // An archived version is still an object a client can read by id; its bytes go only with
    // it.
    versions_keep_their_tier_objects(&setup().await).await;
}

#[tokio::test]
async fn quota_and_usage_count_a_cold_object_by_its_logical_size() {
    // A quota bounds what a bucket holds, wherever its bytes are (cold-tier.md, open question
    // 1): an empty stub must not let a bucket grow past it.
    quota_counts_logical_sizes(&setup().await).await;
}

#[tokio::test]
async fn with_an_fs_primary_quota_and_usage_count_a_cold_object_by_its_logical_size() {
    // The fs scan walks the tree itself and reads a stub's size from its xattrs.
    quota_counts_logical_sizes(&fs_primary().await).await;
}

async fn quota_counts_logical_sizes(s: &Setup) {
    s.server
        .dal
        .set_bucket_quota(
            BUCKET,
            Some(Quota {
                max_bytes: 1500,
                max_objects: None,
                unknown: Default::default(),
            }),
        )
        .await
        .unwrap();
    let client = &s.server.client;
    put_cold(client, "a", content(1000, 1)).await;
    let used = s.server.dal.bucket_usage(BUCKET).await.unwrap();
    assert_eq!((used.objects, used.bytes), (1, 1000));
    let err = client
        .put_object()
        .bucket(BUCKET)
        .key("b")
        .storage_class(StorageClass::StandardIa)
        .body(ByteStream::from(content(1000, 2)))
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("QuotaExceeded"));
    // An overwrite adds only what it grows by.
    put_cold(client, "a", content(1200, 3)).await;
    let used = s.server.dal.bucket_usage(BUCKET).await.unwrap();
    assert_eq!((used.objects, used.bytes), (1, 1200));

    // A scan finds the same, and tells the cold part apart.
    put_hot(client, "h", content(100, 4)).await;
    s.server.dal.scan_usage().await.unwrap();
    // The scan as recorded: the bucket's usage adds this instance's changes of the scan's
    // first moments again, on purpose.
    let scan: serde_json::Value =
        serde_json::from_slice(&s.server.read(&format!(".s3dal/usage/{BUCKET}.json")).await)
            .unwrap();
    assert_eq!(
        (&scan["objects"], &scan["bytes"]),
        (&2.into(), &1300.into())
    );
    assert_eq!(scan["cold_objects"], 1);
    assert_eq!(scan["cold_bytes"], 1200);
}

#[tokio::test]
async fn listings_report_a_cold_objects_size_etag_and_class() {
    // Sync tools compare listed sizes and ETags with their copies: a stub's empty length or the
    // empty MD5 would make them copy every cold object again. On native-ETag backends this
    // takes a stat of every empty entry, which the listing alone cannot tell from a stub.
    let s = setup().await;
    let client = &s.server.client;
    let body = content(500, 9);
    let (etag, _) = put_cold(client, "cold", body).await;
    put_hot(client, "hot", content(10, 1)).await;
    put_hot(client, "zero", Vec::new()).await;

    let listed = client
        .list_objects_v2()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    let rows: Vec<(String, i64, String, String)> = listed
        .contents()
        .iter()
        .map(|o| {
            (
                o.key().unwrap().to_owned(),
                o.size().unwrap(),
                o.e_tag().unwrap().trim_matches('"').to_owned(),
                o.storage_class().unwrap().as_str().to_owned(),
            )
        })
        .collect();
    assert_eq!(
        rows[0],
        ("cold".to_owned(), 500, etag.clone(), CLASS.to_owned())
    );
    assert_eq!((rows[1].1, rows[1].3.as_str()), (10, "STANDARD"));
    assert_eq!(
        (rows[2].1, rows[2].2.as_str(), rows[2].3.as_str()),
        (0, EMPTY_MD5, "STANDARD")
    );

    let versions = client
        .list_object_versions()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    let cold = &versions.versions()[0];
    assert_eq!(cold.key(), Some("cold"));
    assert_eq!(cold.size(), Some(500));
    assert_eq!(cold.storage_class().unwrap().as_str(), CLASS);

    // And in a versioned bucket, archived versions included.
    enable_versioning(client).await;
    put_hot(client, "cold", content(5, 1)).await;
    let versions = client
        .list_object_versions()
        .bucket(BUCKET)
        .prefix("cold")
        .send()
        .await
        .unwrap();
    let classes: Vec<(i64, &str)> = versions
        .versions()
        .iter()
        .map(|v| (v.size().unwrap(), v.storage_class().unwrap().as_str()))
        .collect();
    assert_eq!(classes, [(5, "STANDARD"), (500, CLASS)]);
}

#[tokio::test]
async fn get_object_attributes_reports_the_tiers_class_and_logical_size() {
    let s = setup().await;
    put_cold(&s.server.client, "k", content(77, 1)).await;
    let out = s
        .server
        .client
        .get_object_attributes()
        .bucket(BUCKET)
        .key("k")
        .object_attributes(ObjectAttributes::StorageClass)
        .object_attributes(ObjectAttributes::ObjectSize)
        .send()
        .await
        .unwrap();
    assert_eq!(out.storage_class(), Some(&StorageClass::StandardIa));
    assert_eq!(out.object_size(), Some(77));
}

#[tokio::test]
async fn a_cold_object_is_a_copy_source_for_copy_and_upload_part_copy() {
    // Copies read the source's bytes from its tier and verify them against the stub's MD5, as
    // they verify a hot source's.
    let s = setup().await;
    let client = &s.server.client;
    let body = content(4000, 6);
    put_cold(client, "src", body.clone()).await;
    let out = client
        .copy_object()
        .bucket(BUCKET)
        .key("copy")
        .copy_source(format!("{BUCKET}/src"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        out.copy_object_result()
            .unwrap()
            .e_tag()
            .unwrap()
            .trim_matches('"'),
        meta::md5_hex(&body)
    );
    assert_eq!(get(client, "copy", None).await.unwrap(), body);

    let upload = client
        .create_multipart_upload()
        .bucket(BUCKET)
        .key("mp")
        .send()
        .await
        .unwrap();
    let id = upload.upload_id().unwrap();
    let part = client
        .upload_part_copy()
        .bucket(BUCKET)
        .key("mp")
        .upload_id(id)
        .part_number(1)
        .copy_source(format!("{BUCKET}/src"))
        .copy_source_range("bytes=100-2099")
        .send()
        .await
        .unwrap();
    let etag = part.copy_part_result().unwrap().e_tag().unwrap().to_owned();
    client
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key("mp")
        .upload_id(id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(CompletedPart::builder().part_number(1).e_tag(etag).build())
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(get(client, "mp", None).await.unwrap(), body[100..2100]);
}

#[tokio::test]
async fn an_overwritten_cold_object_is_never_served_from_the_cache() {
    // Every stub of a key has the same backend ETag where that is its content's MD5 (the empty
    // one, RustFS): a cache keyed by it would answer a new cold object with the old bytes.
    let s = setup_with(Settings {
        cache_size: 64 << 20,
        ..Default::default()
    })
    .await;
    let client = &s.server.client;
    put_cold(client, "k", content(32, 1)).await;
    assert_eq!(get(client, "k", None).await.unwrap(), content(32, 1));
    assert_eq!(get(client, "k", None).await.unwrap(), content(32, 1));
    put_cold(client, "k", content(32, 2)).await;
    assert_eq!(get(client, "k", None).await.unwrap(), content(32, 2));
}

#[cfg(feature = "thumbnail")]
#[tokio::test]
async fn a_thumbnail_is_made_from_a_cold_images_bytes() {
    // A browser's grid of photos asks for thumbnails whatever class the photos are kept in.
    let s = setup().await;
    let img = image::RgbaImage::from_pixel(64, 32, image::Rgba([255, 0, 0, 255]));
    let mut png = std::io::Cursor::new(Vec::new());
    image::DynamicImage::ImageRgba8(img)
        .write_to(&mut png, image::ImageFormat::Png)
        .unwrap();
    put_cold(&s.server.client, "p.png", png.into_inner()).await;
    let out = s
        .server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("p.png")
        .customize()
        .mutate_request(|req| {
            let sep = if req.uri().contains('?') { '&' } else { '?' };
            let uri = format!("{}{sep}x-s3dal-thumbnail=128", req.uri());
            req.set_uri(uri).unwrap();
        })
        .send()
        .await
        .unwrap();
    let bytes = out.body.collect().await.unwrap().into_bytes();
    let thumb = image::load_from_memory(&bytes).unwrap();
    assert_eq!((thumb.width(), thumb.height()), (64, 32));
}

#[tokio::test]
async fn with_an_fs_primary_a_small_cold_object_reads_from_its_tier() {
    // On fs a small GET takes the file's content with its stat; a stub's file is empty, and
    // serving that would answer the cold object with no bytes.
    let s = fs_primary().await;
    let body = content(200, 5);
    put_cold(&s.server.client, "k", body.clone()).await;
    assert_eq!(physical_len(&s.server, &format!("{BUCKET}/k")).await, 0);
    assert_eq!(get(&s.server.client, "k", None).await.unwrap(), body);
    let (got, _) = get_range(&s.server.client, "k", "bytes=10-19").await;
    assert_eq!(got, body[10..20]);
    // The stub is renamed into place with its xattrs, through `.s3dal/tmp/`, left empty.
    s.server.assert_no_temp_files().await;
}

#[tokio::test]
async fn with_an_fs_primary_overwrites_and_deletes_remove_the_tier_object() {
    // fs commits a stub and a Complete by renaming a file into place, not through a writer;
    // those commits remove what they replaced too.
    let s = fs_primary().await;
    let client = &s.server.client;
    put_cold(client, "k", content(40, 1)).await;
    put_cold(client, "k", content(40, 2)).await;
    assert_eq!(tier_objects(&s.tier, "k").await.len(), 1);
    put_hot(client, "k", content(40, 3)).await;
    assert!(tier_objects(&s.tier, "k").await.is_empty());
    put_cold(client, "k", content(40, 4)).await;
    client
        .delete_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    assert!(tier_objects(&s.tier, "k").await.is_empty());

    put_cold(client, "k", content(40, 5)).await;
    let upload = client
        .create_multipart_upload()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    let id = upload.upload_id().unwrap();
    let part = client
        .upload_part()
        .bucket(BUCKET)
        .key("k")
        .upload_id(id)
        .part_number(1)
        .body(ByteStream::from(content(40, 6)))
        .send()
        .await
        .unwrap();
    client
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key("k")
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
        .unwrap();
    assert!(tier_objects(&s.tier, "k").await.is_empty());
    assert_eq!(get(client, "k", None).await.unwrap(), content(40, 6));
}

#[tokio::test]
async fn with_an_fs_primary_a_versioned_bucket_keeps_the_tier_object_until_its_version_is_deleted()
{
    // fs archives by hard link and promotes by rename: the stub stays the same file.
    versions_keep_their_tier_objects(&fs_primary().await).await;
}

/// CopyObject of `key` onto itself naming `class`: the copy's ETag, or the error code.
async fn self_copy(
    client: &Client,
    key: &str,
    class: Option<StorageClass>,
) -> Result<String, String> {
    let out = client
        .copy_object()
        .bucket(BUCKET)
        .key(key)
        .copy_source(format!("{BUCKET}/{key}"))
        .set_storage_class(class)
        .send()
        .await
        .map_err(|e| e.code().unwrap_or("none").to_owned())?;
    let result = out.copy_object_result().unwrap();
    Ok(result.e_tag().unwrap().trim_matches('"').to_owned())
}

async fn head(client: &Client, key: &str) -> HeadObjectOutput {
    client
        .head_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn hot_to_cold_to_hot_by_self_copy_keeps_the_bytes_and_etag_and_removes_the_tier_copy() {
    // `aws s3 cp s3://b/k s3://b/k --storage-class …` is how a client moves an object between
    // classes, and back to STANDARD the only way back from a tier. Sync tools compare ETags:
    // a move that changed one would make them copy the object again.
    self_copies_move_objects(&setup().await).await;
}

#[tokio::test]
async fn with_an_fs_primary_hot_to_cold_to_hot_by_self_copy_keeps_the_bytes_and_etag() {
    // fs renames the stub over the hot file, and a hot write over the stub.
    self_copies_move_objects(&fs_primary().await).await;
}

async fn self_copies_move_objects(s: &Setup) {
    let client = &s.server.client;
    let body = content(3000, 1);
    let (etag, _) = put_hot(client, "k", body.clone()).await;
    assert_eq!(etag, meta::md5_hex(&body));
    let path = format!("{BUCKET}/k");
    // A self-copy that changes nothing is refused, as on AWS; a class no tier has is stored hot
    // as STANDARD, so naming one changes nothing either.
    for class in [None, Some(StorageClass::Glacier)] {
        assert_eq!(
            self_copy(client, "k", class).await,
            Err("InvalidRequest".to_owned())
        );
    }

    let moved = self_copy(client, "k", Some(StorageClass::StandardIa)).await;
    assert_eq!(moved.as_deref(), Ok(etag.as_str()));
    assert_eq!(physical_len(&s.server, &path).await, 0);
    assert_eq!(tier_objects(&s.tier, "k").await.len(), 1);
    let cold = head(client, "k").await;
    assert_eq!(cold.storage_class(), Some(&StorageClass::StandardIa));
    assert_eq!(cold.e_tag().unwrap().trim_matches('"'), etag);
    assert_eq!(cold.content_type(), Some("application/x-cold"));
    assert_eq!(
        cold.metadata().unwrap().get("origin").map(String::as_str),
        Some("tier-test")
    );
    assert_eq!(get(client, "k", None).await.unwrap(), body);
    // Naming the class it already has changes nothing either.
    assert_eq!(
        self_copy(client, "k", Some(StorageClass::StandardIa)).await,
        Err("InvalidRequest".to_owned())
    );
    assert_eq!(tier_objects(&s.tier, "k").await.len(), 1);

    // No class is STANDARD: the object comes back, and the tier keeps nothing of it.
    let back = self_copy(client, "k", None).await;
    assert_eq!(back.as_deref(), Ok(etag.as_str()));
    assert_eq!(physical_len(&s.server, &path).await, 3000);
    assert!(tier_objects(&s.tier, "k").await.is_empty());
    let hot = head(client, "k").await;
    assert_eq!(hot.storage_class(), None);
    assert_eq!(hot.e_tag().unwrap().trim_matches('"'), etag);
    assert_eq!(hot.content_type(), Some("application/x-cold"));
    assert_eq!(get(client, "k", None).await.unwrap(), body);
    s.server.assert_no_temp_files().await;
}

#[tokio::test]
async fn a_self_copy_naming_another_tiers_class_moves_the_object_between_tiers() {
    // A tier being retired is emptied into another, and the old one must keep no copy.
    let (hdd, archive) = (Backend::fs().await, Backend::fs().await);
    let server = TestServer::start_tiered(
        Backend::from_env().await,
        Settings::default(),
        &[
            (TIER, CLASS, hdd.op.clone()),
            ("archive", "GLACIER_IR", archive.op.clone()),
        ],
    )
    .await;
    let client = &server.client;
    client.create_bucket().bucket(BUCKET).send().await.unwrap();
    let body = content(900, 2);
    let (etag, _) = put_cold(client, "k", body.clone()).await;

    let moved = self_copy(client, "k", Some(StorageClass::GlacierIr)).await;
    assert_eq!(moved.as_deref(), Ok(etag.as_str()));
    assert!(tier_objects(&hdd.op, "k").await.is_empty());
    assert_eq!(tier_objects(&archive.op, "k").await.len(), 1);
    assert_eq!(
        head(client, "k").await.storage_class(),
        Some(&StorageClass::GlacierIr)
    );
    assert_eq!(get(client, "k", None).await.unwrap(), body);
}

#[tokio::test]
async fn a_write_only_pair_cannot_copy_even_to_change_a_class_within_its_scope() {
    // A copy hands the source's bytes to a key the pair chose, a read by other means; a drop
    // box may not do it at all, even onto the same key (user decision, 2026-11-17).
    let s = setup().await;
    let body = content(100, 1);
    put_hot(&s.server.client, "k", body.clone()).await;
    let mut dropper = s3dal::config::KeyPair::new("dropper", "dropper-secret");
    dropper.allow = Some(vec![BUCKET.into()]);
    dropper.access = s3dal::access::Access::WriteOnly;
    let keys = [
        s3dal::config::KeyPair::new(common::ACCESS_KEY, common::SECRET_KEY),
        dropper,
    ];
    s.server.auth.replace(&keys, &[]).unwrap();
    let dropper = common::client(&s.server.endpoint, "dropper", "dropper-secret");
    let err = self_copy(&dropper, "k", Some(StorageClass::StandardIa)).await;
    assert_eq!(err, Err("AccessDenied".to_owned()));
    assert_eq!(physical_len(&s.server, &format!("{BUCKET}/k")).await, 100);
    assert!(tier_objects(&s.tier, "k").await.is_empty());
}

/// Creates an upload of `key` naming `class`, with CRC32 checksums, and uploads `parts`: the
/// upload id and the parts as Complete lists them.
async fn upload_parts(
    client: &Client,
    key: &str,
    class: Option<StorageClass>,
    parts: &[Vec<u8>],
) -> (String, Vec<CompletedPart>) {
    let upload = client
        .create_multipart_upload()
        .bucket(BUCKET)
        .key(key)
        .set_storage_class(class)
        .content_type("application/x-cold")
        .checksum_algorithm(ChecksumAlgorithm::Crc32)
        .send()
        .await
        .unwrap();
    let id = upload.upload_id().unwrap().to_owned();
    let mut completed = Vec::new();
    for (i, data) in parts.iter().enumerate() {
        let n = i32::try_from(i).unwrap() + 1;
        let out = client
            .upload_part()
            .bucket(BUCKET)
            .key(key)
            .upload_id(&id)
            .part_number(n)
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .body(ByteStream::from(data.clone()))
            .send()
            .await
            .unwrap();
        completed.push(
            CompletedPart::builder()
                .part_number(n)
                .e_tag(out.e_tag().unwrap())
                .set_checksum_crc32(out.checksum_crc32().map(str::to_owned))
                .build(),
        );
    }
    (id, completed)
}

async fn complete(
    client: &Client,
    key: &str,
    id: &str,
    parts: &[CompletedPart],
) -> CompleteMultipartUploadOutput {
    client
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key(key)
        .upload_id(id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(parts.to_vec()))
                .build(),
        )
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn a_multipart_upload_into_a_tier_serves_part_numbers_and_its_checksum() {
    // SDK transfer managers send large files as multipart uploads and download them part by
    // part, checking each response's checksum: a cold object must keep the layout and the
    // checksum its Complete derived, not only its bytes.
    multipart_into_a_tier(&setup().await).await;
}

#[tokio::test]
async fn with_an_fs_primary_a_multipart_upload_into_a_tier_serves_part_numbers_and_its_checksum() {
    // On fs a Complete takes the parts' file as the object; into a tier it streams them out of
    // that file instead.
    multipart_into_a_tier(&fs_primary().await).await;
}

async fn multipart_into_a_tier(s: &Setup) {
    let client = &s.server.client;
    let parts = [content(5 << 20, 1), content(1000, 2)];
    let (id, completed) = upload_parts(client, "mp", Some(StorageClass::StandardIa), &parts).await;
    let done = complete(client, "mp", &id, &completed).await;
    let etag = done.e_tag().unwrap().trim_matches('"').to_owned();
    assert!(etag.ends_with("-2"), "{etag}");
    let checksum = done.checksum_crc32().expect("a CRC32 of the object");
    assert_eq!(physical_len(&s.server, &format!("{BUCKET}/mp")).await, 0);
    assert_eq!(tier_objects(&s.tier, "mp").await.len(), 1);

    let total = (parts[0].len() + parts[1].len()) as i64;
    let cold = head(client, "mp").await;
    assert_eq!(cold.storage_class(), Some(&StorageClass::StandardIa));
    assert_eq!(cold.e_tag().unwrap().trim_matches('"'), etag);
    assert_eq!(cold.content_length(), Some(total));
    assert_eq!(cold.content_type(), Some("application/x-cold"));
    for (n, want) in [(1, &parts[0]), (2, &parts[1])] {
        let out = client
            .get_object()
            .bucket(BUCKET)
            .key("mp")
            .part_number(n)
            .send()
            .await
            .unwrap();
        assert_eq!(out.parts_count(), Some(2));
        let got = out.body.collect().await.unwrap().into_bytes();
        assert!(got.as_ref() == want.as_slice(), "part {n}");
    }
    let whole = client
        .get_object()
        .bucket(BUCKET)
        .key("mp")
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap();
    assert_eq!(whole.checksum_crc32(), Some(checksum));
    let got = whole.body.collect().await.unwrap().into_bytes();
    assert!(got.len() == total as usize && got[..parts[0].len()] == parts[0][..]);
    assert!(got[parts[0].len()..] == parts[1][..]);

    // A copy into a tier hashes what it writes: its ETag is the content's MD5, even from a
    // source whose multipart ETag gives none to check against.
    let copied = client
        .copy_object()
        .bucket(BUCKET)
        .key("copy")
        .copy_source(format!("{BUCKET}/mp"))
        .storage_class(StorageClass::StandardIa)
        .send()
        .await
        .unwrap();
    let mut all = parts[0].clone();
    all.extend_from_slice(&parts[1]);
    let result = copied.copy_object_result().unwrap();
    assert_eq!(
        result.e_tag().unwrap().trim_matches('"'),
        meta::md5_hex(&all)
    );
    assert_eq!(tier_objects(&s.tier, "copy").await.len(), 1);
}

#[tokio::test]
async fn an_upload_into_a_tier_lists_with_the_tiers_class() {
    // A client resuming an upload reads the class it started with from ListParts and
    // ListMultipartUploads; one no tier has is stored hot, so it lists as STANDARD.
    let s = setup().await;
    let client = &s.server.client;
    let (cold, _) = upload_parts(client, "a", Some(StorageClass::StandardIa), &[]).await;
    upload_parts(client, "b", Some(StorageClass::Glacier), &[]).await;
    upload_parts(client, "c", None, &[]).await;
    let parts = client
        .list_parts()
        .bucket(BUCKET)
        .key("a")
        .upload_id(&cold)
        .send()
        .await
        .unwrap();
    assert_eq!(parts.storage_class(), Some(&StorageClass::StandardIa));
    let uploads = client
        .list_multipart_uploads()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    let classes: Vec<(&str, &str)> = uploads
        .uploads()
        .iter()
        .map(|u| (u.key().unwrap(), u.storage_class().unwrap().as_str()))
        .collect();
    assert_eq!(
        classes,
        [("a", CLASS), ("b", "STANDARD"), ("c", "STANDARD")]
    );
}

#[tokio::test]
async fn a_retried_complete_into_a_tier_answers_from_the_record() {
    // SDKs send Complete again when its answer is lost. The retry must get the same answer and
    // must not write the object into the tier a second time, which would leave a copy no stub
    // names.
    let s = setup().await;
    let client = &s.server.client;
    let body = content(700, 3);
    let (id, parts) = upload_parts(
        client,
        "mp",
        Some(StorageClass::StandardIa),
        std::slice::from_ref(&body),
    )
    .await;
    let first = complete(client, "mp", &id, &parts).await;
    let ids = tier_objects(&s.tier, "mp").await;
    assert_eq!(ids.len(), 1);
    let again = complete(client, "mp", &id, &parts).await;
    assert_eq!(again.e_tag(), first.e_tag());
    assert_eq!(again.checksum_crc32(), first.checksum_crc32());
    assert_eq!(tier_objects(&s.tier, "mp").await, ids);
    assert_eq!(get(client, "mp", None).await.unwrap(), body);
}

#[tokio::test]
async fn a_crash_between_the_tier_close_and_the_stub_leaves_the_key_and_one_orphan() {
    // Bytes first, stub second: a process that dies in between leaves the key as it was and a
    // tier object nobody names, for the sweep; never a stub naming nothing. The writes are held
    // right after their tier object closed and never let go, as a dead process never goes on.
    let tier = Backend::fs().await;
    let gate = Arc::new(gate::Gate::default());
    let op = tier.op.clone().layer(gate::Gated::closes(gate.clone()));
    let s = setup_on(Backend::from_env().await, tier, op, Settings::default()).await;
    let client = &s.server.client;
    let body = content(500, 1);
    let (etag, _) = put_hot(client, "k", body.clone()).await;

    gate.arm();
    let copying = no_retry(&s.server);
    let _copy =
        tokio::spawn(async move { self_copy(&copying, "k", Some(StorageClass::StandardIa)).await });
    gate.reached().await;
    assert_eq!(tier_objects(&s.tier, "k").await.len(), 1);
    let hot = head(client, "k").await;
    assert_eq!(hot.storage_class(), None);
    assert_eq!(hot.e_tag().unwrap().trim_matches('"'), etag);
    assert_eq!(physical_len(&s.server, &format!("{BUCKET}/k")).await, 500);
    assert_eq!(get(client, "k", None).await.unwrap(), body);

    // A Complete leaves the key without an object, and the upload open for a retry or an abort.
    let (id, parts) = upload_parts(client, "mp", Some(StorageClass::StandardIa), &[body]).await;
    gate.arm();
    let completing = no_retry(&s.server);
    let _complete = tokio::spawn(async move {
        completing
            .complete_multipart_upload()
            .bucket(BUCKET)
            .key("mp")
            .upload_id(id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .send()
            .await
            .map(drop)
    });
    gate.reached().await;
    assert_eq!(tier_objects(&s.tier, "mp").await.len(), 1);
    assert_eq!(
        get(client, "mp", None).await.unwrap_err(),
        "NoSuchKey".to_owned()
    );
    let uploads = client
        .list_multipart_uploads()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    assert_eq!(uploads.uploads().len(), 1);
}

// ----- Lifecycle transitions and the sweep (T3) -----

const DAY: u64 = 24 * 60 * 60;

/// AWS's rounding, written out again so a change to the server's is caught: `days` days after
/// `t`, then on to the next midnight UTC.
fn due(t: SystemTime, days: u64) -> SystemTime {
    let secs = t.duration_since(UNIX_EPOCH).unwrap().as_secs() + days * DAY;
    UNIX_EPOCH + Duration::from_secs((secs / DAY + 1) * DAY)
}

/// When the key's current version was created, as HEAD says.
async fn created(client: &Client, key: &str) -> SystemTime {
    let t = head(client, key).await.last_modified().unwrap().secs();
    UNIX_EPOCH + Duration::from_secs(t as u64)
}

fn rule(id: &str, prefix: &str) -> aws_sdk_s3::types::builders::LifecycleRuleBuilder {
    LifecycleRule::builder()
        .id(id)
        .status(ExpirationStatus::Enabled)
        .filter(LifecycleRuleFilter::builder().prefix(prefix).build())
}

fn after(days: i32, class: TransitionStorageClass) -> Transition {
    Transition::builder()
        .days(days)
        .storage_class(class)
        .build()
}

async fn put_rules(client: &Client, rules: Vec<LifecycleRule>) -> Result<(), String> {
    client
        .put_bucket_lifecycle_configuration()
        .bucket(BUCKET)
        .lifecycle_configuration(
            BucketLifecycleConfiguration::builder()
                .set_rules(Some(rules))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .map(drop)
        .map_err(|e| e.code().unwrap_or("none").to_owned())
}

fn moves(events: &mut s3dal::events::Subscriber) -> Vec<(String, Option<u64>, Option<String>)> {
    std::iter::from_fn(|| events.try_recv())
        .filter(|e| e.kind == EventKind::LifecycleTransition)
        .map(|e| (e.key, e.size, e.version))
        .collect()
}

#[tokio::test]
async fn transition_rules_read_back_as_put_and_name_only_a_tiers_class() {
    // Terraform compares what it reads with what it put. A class no tier has would leave a client
    // believing its objects move while none ever does, so it is refused (cold-tier.md
    // "Lifecycle transitions"), as are the combinations AWS refuses.
    let s = setup().await;
    let c = &s.server.client;
    let rules = vec![
        rule("move", "logs/")
            .transitions(after(30, TransitionStorageClass::StandardIa))
            .noncurrent_version_transitions(
                NoncurrentVersionTransition::builder()
                    .noncurrent_days(7)
                    .newer_noncurrent_versions(2)
                    .storage_class(TransitionStorageClass::StandardIa)
                    .build(),
            )
            .expiration(LifecycleExpiration::builder().days(365).build())
            .build()
            .unwrap(),
        rule("dated", "")
            .transitions(
                Transition::builder()
                    .date(DateTime::from_secs(1_893_456_000)) // 2030-01-01
                    .storage_class(TransitionStorageClass::StandardIa)
                    .build(),
            )
            .build()
            .unwrap(),
    ];
    put_rules(c, rules.clone()).await.unwrap();
    let got = c
        .get_bucket_lifecycle_configuration()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    assert_eq!(got.rules(), rules.as_slice());

    let refused = [
        (
            vec![after(30, TransitionStorageClass::Glacier)],
            "InvalidStorageClass",
        ),
        (
            vec![
                after(30, TransitionStorageClass::StandardIa),
                after(60, TransitionStorageClass::StandardIa),
            ],
            "InvalidArgument",
        ),
        (
            vec![
                after(30, TransitionStorageClass::StandardIa),
                Transition::builder()
                    .date(DateTime::from_secs(1_893_456_000))
                    .storage_class(TransitionStorageClass::GlacierIr)
                    .build(),
            ],
            "InvalidRequest",
        ),
        (
            vec![
                Transition::builder()
                    .storage_class(TransitionStorageClass::StandardIa)
                    .build(),
            ],
            "MalformedXML",
        ),
        (
            vec![after(-1, TransitionStorageClass::StandardIa)],
            "InvalidArgument",
        ),
    ];
    for (transitions, code) in refused {
        let r = rule("bad", "")
            .set_transitions(Some(transitions))
            .build()
            .unwrap();
        assert_eq!(
            put_rules(c, vec![r.clone()]).await,
            Err(code.to_owned()),
            "{r:?}"
        );
    }
    let kept = c
        .get_bucket_lifecycle_configuration()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    assert_eq!(kept.rules(), rules.as_slice());
}

#[tokio::test]
async fn an_object_moves_into_its_tier_on_the_first_midnight_after_its_days() {
    // AWS's times, as for expiry. The object a client reads afterwards is the one it wrote:
    // bytes, ETag, Last-Modified and headers; only where its bytes are changed. A second run
    // finds it in its class and does nothing.
    let s = setup().await;
    let c = &s.server.client;
    let body = content(3000, 5);
    let (etag, _) = put_hot(c, "logs/a", body.clone()).await;
    put_hot(c, "other/a", content(3000, 6)).await;
    let before = head(c, "logs/a").await;
    put_rules(
        c,
        vec![
            rule("move", "logs/")
                .transitions(after(30, TransitionStorageClass::StandardIa))
                .build()
                .unwrap(),
        ],
    )
    .await
    .unwrap();
    let mut events = s.server.dal.subscribe();
    let at = due(created(c, "logs/a").await, 30);

    s.server
        .dal
        .apply_lifecycle(at - Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(head(c, "logs/a").await.storage_class(), None);
    assert!(tier_objects(&s.tier, "logs/a").await.is_empty());

    s.server.dal.apply_lifecycle(at).await.unwrap();
    assert_eq!(
        physical_len(&s.server, &format!("{BUCKET}/logs/a")).await,
        0
    );
    assert_eq!(tier_objects(&s.tier, "logs/a").await.len(), 1);
    let after_move = head(c, "logs/a").await;
    assert_eq!(after_move.storage_class(), Some(&StorageClass::StandardIa));
    assert_eq!(after_move.e_tag(), before.e_tag());
    assert_eq!(after_move.last_modified(), before.last_modified());
    assert_eq!(after_move.content_type(), Some("application/x-cold"));
    assert_eq!(after_move.metadata(), before.metadata());
    assert_eq!(get(c, "logs/a", None).await.unwrap(), body);
    // The prefix filter holds.
    assert_eq!(head(c, "other/a").await.storage_class(), None);
    assert_eq!(
        moves(&mut events),
        [("logs/a".to_owned(), Some(3000), None)]
    );
    let metrics = s.server.dal.metrics().render();
    assert!(
        metrics.contains("s3dal_tier_writes_total{tier=\"hdd\",origin=\"lifecycle\"} 1\n"),
        "{metrics}"
    );
    assert!(
        metrics
            .contains("s3dal_tier_written_bytes_total{tier=\"hdd\",origin=\"lifecycle\"} 3000\n"),
        "{metrics}"
    );

    s.server
        .dal
        .apply_lifecycle(at + Duration::from_secs(DAY))
        .await
        .unwrap();
    assert_eq!(tier_objects(&s.tier, "logs/a").await.len(), 1);
    assert!(moves(&mut events).is_empty());
    assert_eq!(get(c, "logs/a", None).await.unwrap(), body);
    assert_eq!(
        head(c, "logs/a").await.e_tag().unwrap().trim_matches('"'),
        etag
    );
}

#[tokio::test]
async fn a_later_transition_moves_an_object_on_to_the_next_tier() {
    // An object passes each class in turn and stays in the last one due: a cold object moves
    // between tiers, its old tier object going once the stub names the new one, and an object
    // two transitions are due for goes straight to the later one.
    let (hdd, archive) = (Backend::fs().await, Backend::fs().await);
    let server = TestServer::start_tiered(
        Backend::from_env().await,
        Settings::default(),
        &[
            (TIER, CLASS, hdd.op.clone()),
            ("archive", "GLACIER_IR", archive.op.clone()),
        ],
    )
    .await;
    let c = &server.client;
    c.create_bucket().bucket(BUCKET).send().await.unwrap();
    let a = content(2000, 1);
    put_hot(c, "a", a.clone()).await;
    put_rules(
        c,
        vec![
            rule("waterfall", "")
                .transitions(after(1, TransitionStorageClass::StandardIa))
                .transitions(after(2, TransitionStorageClass::GlacierIr))
                .build()
                .unwrap(),
        ],
    )
    .await
    .unwrap();
    let made = created(c, "a").await;
    server.dal.apply_lifecycle(due(made, 1)).await.unwrap();
    assert_eq!(tier_objects(&hdd.op, "a").await.len(), 1);
    assert_eq!(
        head(c, "a").await.storage_class(),
        Some(&StorageClass::StandardIa)
    );

    let b = content(2000, 2);
    put_hot(c, "b", b.clone()).await;
    let later = due(made, 2).max(due(created(c, "b").await, 2));
    server.dal.apply_lifecycle(later).await.unwrap();
    for (key, body) in [("a", &a), ("b", &b)] {
        assert!(tier_objects(&hdd.op, key).await.is_empty(), "{key}");
        assert_eq!(tier_objects(&archive.op, key).await.len(), 1, "{key}");
        assert_eq!(
            head(c, key).await.storage_class(),
            Some(&StorageClass::GlacierIr)
        );
        assert_eq!(&get(c, key, None).await.unwrap(), body);
    }
}

#[tokio::test]
async fn expiration_wins_over_a_transition_due_the_same_day() {
    // As on AWS: an object due to go is not first copied into a tier.
    let s = setup().await;
    let c = &s.server.client;
    put_hot(c, "k", content(100, 1)).await;
    put_rules(
        c,
        vec![
            rule("both", "")
                .transitions(after(1, TransitionStorageClass::StandardIa))
                .expiration(LifecycleExpiration::builder().days(1).build())
                .build()
                .unwrap(),
        ],
    )
    .await
    .unwrap();
    let mut events = s.server.dal.subscribe();
    s.server
        .dal
        .apply_lifecycle(due(created(c, "k").await, 1))
        .await
        .unwrap();
    assert_eq!(get(c, "k", None).await, Err("NoSuchKey".to_owned()));
    assert!(tier_objects(&s.tier, "k").await.is_empty());
    assert!(moves(&mut events).is_empty());
}

#[tokio::test]
async fn size_filters_see_a_cold_objects_size_and_a_cold_object_still_expires() {
    // A stub is empty and written at its own time; read as such, a size filter would never
    // match a cold object and its expiry condition would never hold.
    expires_cold_objects_by_their_size(&setup().await).await;
}

#[tokio::test]
async fn with_an_fs_primary_size_filters_see_a_cold_objects_size() {
    // fs lists names only, so every entry is stat'ed: the stat must be resolved too.
    expires_cold_objects_by_their_size(&fs_primary().await).await;
}

async fn expires_cold_objects_by_their_size(s: &Setup) {
    let c = &s.server.client;
    put_cold(c, "big", content(3000, 1)).await;
    put_cold(c, "small", content(100, 2)).await;
    put_rules(
        c,
        vec![
            rule("big", "")
                .filter(
                    LifecycleRuleFilter::builder()
                        .object_size_greater_than(1000)
                        .build(),
                )
                .expiration(LifecycleExpiration::builder().days(1).build())
                .build()
                .unwrap(),
        ],
    )
    .await
    .unwrap();
    let at = due(created(c, "big").await, 1).max(due(created(c, "small").await, 1));
    let expired = s.server.dal.apply_lifecycle(at).await.unwrap();
    assert_eq!(expired.current, 1);
    assert_eq!(get(c, "big", None).await, Err("NoSuchKey".to_owned()));
    assert!(tier_objects(&s.tier, "big").await.is_empty());
    assert_eq!(get(c, "small", None).await.unwrap(), content(100, 2));
}

#[tokio::test]
async fn a_noncurrent_version_moves_in_place_keeping_its_id() {
    // A transition is not a new version: the version keeps its id and its place, and only
    // versions with enough newer noncurrent ones above them move. A cold version's size is its
    // own for a noncurrent expiry's filter, and its bytes go with it.
    let s = setup().await;
    let c = &s.server.client;
    enable_versioning(c).await;
    let bodies: Vec<Vec<u8>> = (1..=3).map(|i| content(500, i)).collect();
    let mut ids = Vec::new();
    for body in &bodies {
        ids.push(put_hot(c, "k", body.clone()).await.1.unwrap());
    }
    put_rules(
        c,
        vec![
            rule("history", "")
                .noncurrent_version_transitions(
                    NoncurrentVersionTransition::builder()
                        .noncurrent_days(1)
                        .newer_noncurrent_versions(1)
                        .storage_class(TransitionStorageClass::StandardIa)
                        .build(),
                )
                .build()
                .unwrap(),
        ],
    )
    .await
    .unwrap();
    let mut events = s.server.dal.subscribe();
    let later = due(SystemTime::now(), 2);
    s.server.dal.apply_lifecycle(later).await.unwrap();

    let class = |key_version: &str| {
        let key_version = key_version.to_owned();
        async move {
            c.head_object()
                .bucket(BUCKET)
                .key("k")
                .version_id(key_version)
                .send()
                .await
                .unwrap()
                .storage_class()
                .cloned()
        }
    };
    assert_eq!(class(&ids[0]).await, Some(StorageClass::StandardIa));
    assert_eq!(class(&ids[1]).await, None);
    assert_eq!(class(&ids[2]).await, None);
    assert_eq!(tier_objects(&s.tier, "k").await.len(), 1);
    for (id, body) in ids.iter().zip(&bodies) {
        assert_eq!(&get(c, "k", Some(id)).await.unwrap(), body);
    }
    let listed = c
        .list_object_versions()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    let listed: Vec<&str> = listed
        .versions()
        .iter()
        .map(|v| v.version_id().unwrap())
        .collect();
    assert_eq!(listed, [&ids[2], &ids[1], &ids[0]]);
    assert_eq!(
        moves(&mut events),
        [("k".to_owned(), Some(500), Some(ids[0].clone()))]
    );

    put_rules(
        c,
        vec![
            rule("history", "")
                .filter(
                    LifecycleRuleFilter::builder()
                        .object_size_greater_than(100)
                        .build(),
                )
                .noncurrent_version_expiration(
                    NoncurrentVersionExpiration::builder()
                        .noncurrent_days(1)
                        .build(),
                )
                .build()
                .unwrap(),
        ],
    )
    .await
    .unwrap();
    let expired = s.server.dal.apply_lifecycle(later).await.unwrap();
    assert_eq!(expired.noncurrent, 2);
    assert!(tier_objects(&s.tier, "k").await.is_empty());
    assert_eq!(get(c, "k", None).await.unwrap(), bodies[2]);
}

#[tokio::test]
async fn in_a_versioned_bucket_a_moved_current_version_keeps_its_id() {
    // A current version's id is only in its metadata, so the stub must carry it: without it the
    // version reads as `null`, and the next write archives it under that id, taking the place of
    // a real `null` version and leaving the client's id naming nothing.
    let s = setup().await;
    let c = &s.server.client;
    enable_versioning(c).await;
    let (_, id) = put_hot(c, "k", content(700, 1)).await;
    let id = id.unwrap();
    put_rules(
        c,
        vec![
            rule("move", "")
                .transitions(after(1, TransitionStorageClass::StandardIa))
                .build()
                .unwrap(),
        ],
    )
    .await
    .unwrap();
    s.server
        .dal
        .apply_lifecycle(due(created(c, "k").await, 1))
        .await
        .unwrap();
    let moved = head(c, "k").await;
    assert_eq!(moved.storage_class(), Some(&StorageClass::StandardIa));
    assert_eq!(moved.version_id(), Some(id.as_str()));

    put_hot(c, "k", content(800, 2)).await;
    assert_eq!(get(c, "k", Some(&id)).await.unwrap(), content(700, 1));
}

#[tokio::test]
async fn a_key_too_long_for_its_tier_stays_hot_and_is_counted() {
    // A tier's paths are longer than the primary's (its prefix, `!!`, the id), so a key the
    // primary holds may not fit a tier on fs. It stays where it is, readable, and the skip shows.
    // A tier rooted deep enough makes any key too long, whatever the primary is.
    #[cfg(target_os = "macos")]
    const PATH_MAX: usize = 1024;
    #[cfg(not(target_os = "macos"))]
    const PATH_MAX: usize = 4096;
    let base = tempfile::tempdir().unwrap();
    let mut root = base.path().to_string_lossy().into_owned();
    while root.len() < PATH_MAX - 70 {
        let room = PATH_MAX - 70 - root.len() - 1;
        root = format!("{root}/{}", "d".repeat(room.clamp(1, 200)));
    }
    std::fs::create_dir_all(&root).unwrap();
    let op = s3dal::backend::build_operator("fs", &[("root".to_owned(), root)]).unwrap();
    let s = setup_on(
        Backend::from_env().await,
        Backend::fs().await,
        op,
        Settings::default(),
    )
    .await;
    let c = &s.server.client;
    let key = "k".to_owned();
    put_hot(c, &key, content(100, 1)).await;
    put_rules(
        c,
        vec![
            rule("move", "")
                .transitions(after(1, TransitionStorageClass::StandardIa))
                .build()
                .unwrap(),
        ],
    )
    .await
    .unwrap();
    s.server
        .dal
        .apply_lifecycle(due(created(c, &key).await, 1))
        .await
        .unwrap();
    assert_eq!(head(c, &key).await.storage_class(), None);
    assert_eq!(get(c, &key, None).await.unwrap(), content(100, 1));
    let metrics = s.server.dal.metrics().render();
    assert!(
        metrics.contains("s3dal_tier_transition_skipped_total{reason=\"key-too-long\"} 1\n"),
        "{metrics}"
    );
}

#[tokio::test]
async fn an_object_overwritten_while_it_moves_keeps_the_new_one_and_no_tier_object() {
    // The move reads the object, writes it into the tier, and only then replaces it with a stub.
    // A client's write landing in between must win: the stub would otherwise stand for bytes the
    // key no longer holds, and the client's object would be gone.
    let tier = Backend::fs().await;
    let gate = Arc::new(gate::Gate::default());
    let op = tier.op.clone().layer(gate::Gated::closes(gate.clone()));
    let s = setup_on(Backend::from_env().await, tier, op, Settings::default()).await;
    let c = &s.server.client;
    put_hot(c, "k", content(800, 1)).await;
    put_rules(
        c,
        vec![
            rule("move", "")
                .transitions(after(1, TransitionStorageClass::StandardIa))
                .build()
                .unwrap(),
        ],
    )
    .await
    .unwrap();
    let at = due(created(c, "k").await, 1);
    gate.arm();
    let dal = s.server.dal.clone();
    let applying = tokio::spawn(async move { dal.apply_lifecycle(at).await });
    gate.reached().await;
    let newer = content(900, 2);
    put_hot(c, "k", newer.clone()).await;
    gate.release();
    applying.await.unwrap().unwrap();

    assert_eq!(get(c, "k", None).await.unwrap(), newer);
    assert_eq!(physical_len(&s.server, &format!("{BUCKET}/k")).await, 900);
    assert!(tier_objects(&s.tier, "k").await.is_empty());
    let metrics = s.server.dal.metrics().render();
    assert!(
        metrics.contains("s3dal_tier_transition_skipped_total{reason=\"changed\"} 1\n"),
        "{metrics}"
    );
}

/// A server whose primary fails the first write of `key`'s stub after [`gate::Gate::arm`], once
/// the backend kept it; and whether it can. fs renames stubs into place past OpenDAL, so there
/// the write never fails and only the outcome is checked.
async fn stub_lost_reply(key: &str) -> (Setup, Arc<gate::Gate>, bool) {
    let gate = Arc::new(gate::Gate::default());
    let mut primary = Backend::from_env().await;
    let injected = primary.op.info().scheme() != "fs";
    let path = format!("{BUCKET}/{key}");
    primary.op = primary
        .op
        .clone()
        .layer(gate::Gated::fails_close_of(gate.clone(), &path));
    let tier = Backend::fs().await;
    let op = tier.op.clone();
    (
        setup_on(primary, tier, op, Settings::default()).await,
        gate,
        injected,
    )
}

#[tokio::test]
async fn a_move_whose_stub_write_fails_after_it_landed_keeps_the_tier_object() {
    // A stub write can fail after the backend kept it: a close that timed out, a reply lost. The
    // stub then stands over the hot bytes and the tier object is the only copy left; removing it,
    // as for a commit refused under the lock, would lose the object.
    let (s, gate, injected) = stub_lost_reply("k").await;
    let c = &s.server.client;
    put_hot(c, "k", content(1200, 1)).await;
    put_rules(
        c,
        vec![
            rule("move", "")
                .transitions(after(1, TransitionStorageClass::StandardIa))
                .build()
                .unwrap(),
        ],
    )
    .await
    .unwrap();
    gate.arm();
    s.server
        .dal
        .apply_lifecycle(due(created(c, "k").await, 1))
        .await
        .unwrap();
    assert_eq!(get(c, "k", None).await.unwrap(), content(1200, 1));
    assert_eq!(
        head(c, "k").await.storage_class(),
        Some(&StorageClass::StandardIa)
    );
    assert_eq!(tier_objects(&s.tier, "k").await.len(), 1);
    let metrics = s.server.dal.metrics().render();
    let errors = format!(
        "s3dal_tier_transition_skipped_total{{reason=\"error\"}} {}\n",
        u8::from(injected)
    );
    assert!(metrics.contains(&errors), "{metrics}");
}

#[tokio::test]
async fn a_put_into_a_tier_whose_stub_write_fails_after_it_landed_keeps_the_tier_object() {
    // As for a move: the client is told the PUT failed, but the key holds the stub that landed,
    // and a GET must serve its bytes, not a 500 for a tier object removed under it.
    let (s, gate, injected) = stub_lost_reply("k").await;
    let body = content(1300, 2);
    gate.arm();
    let put = no_retry(&s.server)
        .put_object()
        .bucket(BUCKET)
        .key("k")
        .storage_class(StorageClass::StandardIa)
        .body(ByteStream::from(body.clone()))
        .send()
        .await;
    assert_eq!(put.is_err(), injected, "{put:?}");
    let c = &s.server.client;
    assert_eq!(get(c, "k", None).await.unwrap(), body);
    assert_eq!(tier_objects(&s.tier, "k").await.len(), 1);
}

#[tokio::test]
async fn two_instances_moving_one_bucket_lose_nothing_and_leave_one_tier_object_per_key() {
    // A lease lost to a pause can let two instances run the job at once. Of two moves of one
    // object only one stub may land; where the backend checks conditions the other's tier
    // object goes at once, elsewhere (fs, which one process serves) the sweep takes it.
    let s = setup().await;
    let second = TestServer::start_tiered(
        Backend::sharing(&s.server),
        Settings::default(),
        &[(TIER, CLASS, s.tier.clone())],
    )
    .await;
    let c = &s.server.client;
    let keys: Vec<String> = (0..12).map(|i| format!("k{i:02}")).collect();
    for (i, key) in keys.iter().enumerate() {
        put_hot(c, key, content(1500, i as u8)).await;
    }
    put_rules(
        c,
        vec![
            rule("move", "")
                .transitions(after(1, TransitionStorageClass::StandardIa))
                .build()
                .unwrap(),
        ],
    )
    .await
    .unwrap();
    let at = due(SystemTime::now(), 1);
    let (one, two) = tokio::join!(
        s.server.dal.apply_lifecycle(at),
        second.dal.apply_lifecycle(at)
    );
    one.unwrap();
    two.unwrap();

    async fn named(server: &TestServer, key: &str) -> String {
        let m = server.op.stat(&format!("{BUCKET}/{key}")).await.unwrap();
        let obj = meta::resolve(&server.profile, &m);
        obj.tier.expect("a stub").id.expect("an intact stub")
    }
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(
            get(c, key, None).await.unwrap(),
            content(1500, i as u8),
            "{key}"
        );
        let ids = tier_objects(&s.tier, key).await;
        assert!(ids.contains(&named(&s.server, key).await), "{key}: {ids:?}");
        if s.server.profile.conditional_writes {
            assert_eq!(ids.len(), 1, "{key}: {ids:?}");
        }
    }
    // Once the orphans are old enough, the sweep leaves exactly the named ones.
    let swept = s
        .server
        .dal
        .sweep_tiers(SystemTime::now() + Duration::from_secs(2 * DAY))
        .await
        .unwrap();
    assert_eq!(swept.dangling, 0);
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(
            tier_objects(&s.tier, key).await,
            [named(&s.server, key).await],
            "{key}"
        );
        assert_eq!(
            get(c, key, None).await.unwrap(),
            content(1500, i as u8),
            "{key}"
        );
    }
}

#[tokio::test]
async fn the_sweep_removes_an_old_orphan_keeps_a_young_one_and_every_named_one_and_reports_a_dangling_stub()
 {
    // An orphan only takes space; a tier object a stub is about to name looks like one, so only
    // one older than a day goes, and a named one, current or archived, never. A stub whose tier
    // object is gone is a fault for an operator: reported, never changed.
    let s = setup().await;
    let c = &s.server.client;
    enable_versioning(c).await;
    put_cold(c, "kept", content(400, 1)).await;
    put_cold(c, "kept", content(500, 2)).await;
    put_cold(c, "lost", content(600, 3)).await;
    for id in tier_objects(&s.tier, "lost").await {
        s.tier
            .delete(&format!(".s3dal/tier/{BUCKET}/lost!!/{id}"))
            .await
            .unwrap();
    }
    let orphan = format!(".s3dal/tier/{BUCKET}/kept!!/{}", "0".repeat(32));
    let gone_bucket = format!(".s3dal/tier/gone/k!!/{}", "1".repeat(32));
    for path in [&orphan, &gone_bucket] {
        s.tier.write(path, b"orphan".to_vec()).await.unwrap();
    }
    let named = tier_objects(&s.tier, "kept").await;
    assert_eq!(named.len(), 3);

    let now = SystemTime::now();
    let young = s.server.dal.sweep_tiers(now).await.unwrap();
    assert_eq!(young.orphans_removed, 0);
    assert_eq!(young.young_kept, 2);
    assert_eq!(young.dangling, 1);
    assert!(s.tier.exists(&orphan).await.unwrap());

    let old = s
        .server
        .dal
        .sweep_tiers(now + Duration::from_secs(DAY + 3600))
        .await
        .unwrap();
    assert_eq!(old.orphans_removed, 2);
    assert_eq!(old.dangling, 1);
    assert!(!s.tier.exists(&orphan).await.unwrap());
    assert!(!s.tier.exists(&gone_bucket).await.unwrap());
    let mut left = tier_objects(&s.tier, "kept").await;
    left.retain(|id| *id != "0".repeat(32));
    assert_eq!(left.len(), 2);
    assert_eq!(get(c, "kept", None).await.unwrap(), content(500, 2));
    let metrics = s.server.dal.metrics().render();
    for want in [
        "s3dal_tier_orphans_removed_total{tier=\"hdd\"} 2\n",
        "s3dal_tier_dangling{tier=\"hdd\"} 1\n",
        "s3dal_tier_objects{tier=\"hdd\"} 3\n",
        "s3dal_tier_bytes{tier=\"hdd\"} 1500\n",
    ] {
        assert!(metrics.contains(want), "{want}{metrics}");
    }
}

/// A tier whose first read, or first writer close, after [`Gate::arm`] waits until the test lets
/// it go. Held at a read, a write can land between a GET's stat and its tier read; the read's
/// backend reader is made only then, so it sees what the write left. Held after a close, the
/// tier object is written and nothing after it happens, as when the process dies there.
mod gate {
    use std::sync::Arc;

    use opendal::raw::*;
    use opendal::{Buffer, BytesRange, Capability, Metadata, OperationContext, Result};

    use super::{AtomicBool, Ordering, Semaphore};

    pub struct Gate {
        armed: AtomicBool,
        reached: Semaphore,
        released: Semaphore,
    }

    impl Default for Gate {
        fn default() -> Self {
            Self {
                armed: AtomicBool::new(false),
                reached: Semaphore::new(0),
                released: Semaphore::new(0),
            }
        }
    }

    impl Gate {
        pub fn arm(&self) {
            self.armed.store(true, Ordering::SeqCst);
        }

        /// Resolves once a read is waiting at the gate.
        pub async fn reached(&self) {
            self.reached.acquire().await.unwrap().forget();
        }

        pub fn release(&self) {
            self.released.add_permits(1);
        }

        async fn pass(&self) {
            if self.armed.swap(false, Ordering::SeqCst) {
                self.reached.add_permits(1);
                self.released.acquire().await.unwrap().forget();
            }
        }
    }

    #[derive(Debug)]
    pub struct Gated {
        gate: Arc<Gate>,
        closes: bool,
        fails_close_of: Option<String>,
    }

    impl Gated {
        /// Holds the first read after the gate is armed.
        pub fn reads(gate: Arc<Gate>) -> Self {
            Self {
                gate,
                closes: false,
                fails_close_of: None,
            }
        }

        /// Holds the first writer close after the gate is armed, once the object is written.
        pub fn closes(gate: Arc<Gate>) -> Self {
            Self {
                gate,
                closes: true,
                fails_close_of: None,
            }
        }

        /// Fails the first close of a writer to `path` after the gate is armed, once the object
        /// is written: a reply lost after the backend kept the write.
        pub fn fails_close_of(gate: Arc<Gate>, path: &str) -> Self {
            Self {
                gate,
                closes: false,
                fails_close_of: Some(path.to_owned()),
            }
        }
    }

    impl std::fmt::Debug for Gate {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("Gate")
        }
    }

    impl Layer for Gated {
        fn apply_service(&self, inner: Servicer) -> Servicer {
            Arc::new(GatedService {
                inner,
                gate: self.gate.clone(),
                closes: self.closes,
                fails_close_of: self.fails_close_of.clone(),
            })
        }
    }

    #[derive(Debug)]
    struct GatedService {
        inner: Servicer,
        gate: Arc<Gate>,
        closes: bool,
        fails_close_of: Option<String>,
    }

    struct GatedWriter {
        inner: oio::Writer,
        gate: Option<Arc<Gate>>,
        fail: Option<Arc<Gate>>,
    }

    impl oio::Write for GatedWriter {
        async fn write(&mut self, bs: Buffer) -> Result<()> {
            self.inner.write(bs).await
        }

        async fn close(&mut self) -> Result<Metadata> {
            let closed = self.inner.close().await?;
            if let Some(gate) = &self.gate {
                gate.pass().await;
            }
            if let Some(gate) = &self.fail
                && gate.armed.swap(false, Ordering::SeqCst)
            {
                return Err(opendal::Error::new(
                    opendal::ErrorKind::Unexpected,
                    "injected: the reply was lost after the write",
                ));
            }
            Ok(closed)
        }

        async fn abort(&mut self) -> Result<()> {
            self.inner.abort().await
        }
    }

    struct GatedReader {
        gate: Option<Arc<Gate>>,
        service: Servicer,
        ctx: OperationContext,
        path: String,
        args: OpRead,
    }

    impl oio::Read for GatedReader {
        async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
            if let Some(gate) = &self.gate {
                gate.pass().await;
            }
            let reader = self
                .service
                .read(&self.ctx, &self.path, self.args.clone())?;
            oio::Read::open(&reader, range).await
        }

        async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
            if let Some(gate) = &self.gate {
                gate.pass().await;
            }
            let reader = self
                .service
                .read(&self.ctx, &self.path, self.args.clone())?;
            oio::Read::read(&reader, range).await
        }
    }

    impl Service for GatedService {
        type Reader = oio::Reader;
        type Writer = GatedWriter;
        type Lister = oio::Lister;
        type Deleter = oio::Deleter;
        type Copier = oio::Copier;
        type Composer = oio::Composer;

        fn info(&self) -> ServiceInfo {
            self.inner.info()
        }

        fn capability(&self) -> Capability {
            self.inner.capability()
        }

        async fn create_dir(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpCreateDir,
        ) -> Result<RpCreateDir> {
            self.inner.create_dir(ctx, path, args).await
        }

        async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
            self.inner.stat(ctx, path, args).await
        }

        fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<oio::Reader> {
            Ok(Box::new(GatedReader {
                gate: (!self.closes && self.fails_close_of.is_none()).then(|| self.gate.clone()),
                service: self.inner.clone(),
                ctx: ctx.clone(),
                path: path.to_owned(),
                args,
            }))
        }

        fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<GatedWriter> {
            Ok(GatedWriter {
                inner: self.inner.write(ctx, path, args)?,
                gate: self.closes.then(|| self.gate.clone()),
                fail: (self.fails_close_of.as_deref() == Some(path)).then(|| self.gate.clone()),
            })
        }

        fn delete(&self, ctx: &OperationContext) -> Result<oio::Deleter> {
            self.inner.delete(ctx)
        }

        fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<oio::Lister> {
            self.inner.list(ctx, path, args)
        }

        fn copy(
            &self,
            ctx: &OperationContext,
            from: &str,
            to: &str,
            args: OpCopy,
        ) -> Result<oio::Copier> {
            self.inner.copy(ctx, from, to, args)
        }

        async fn rename(
            &self,
            ctx: &OperationContext,
            from: &str,
            to: &str,
            args: OpRename,
        ) -> Result<RpRename> {
            self.inner.rename(ctx, from, to, args).await
        }

        async fn presign(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpPresign,
        ) -> Result<RpPresign> {
            self.inner.presign(ctx, path, args).await
        }
    }
}

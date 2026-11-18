//! Object events (F3): every change to a key that took effect is announced once, after it
//! committed, naming what it wrote or removed; and in a bucket with a quota, what it did to the
//! bucket's usage, which the quota is counted from.

mod common;

use std::time::Duration;

use aws_sdk_s3::Client;
use aws_sdk_s3::operation::RequestId;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier,
    VersioningConfiguration,
};
use common::{ACCESS_KEY, TestServer};
use s3dal::Quota;
use s3dal::events::{EventKind, ObjectEvent, Subscriber, Usage};

const BUCKET: &str = "bkt";

async fn server() -> TestServer {
    let server = TestServer::start().await;
    server.create_bucket_dir(BUCKET).await;
    server
}

/// Gives the bucket a quota far above what the tests write, so its usage is counted.
async fn counted(server: &TestServer) {
    let quota = Quota {
        max_bytes: 1 << 40,
        max_objects: None,
        unknown: Default::default(),
    };
    server
        .dal
        .set_bucket_quota(BUCKET, Some(quota))
        .await
        .unwrap();
}

async fn set_versioning(client: &Client, status: BucketVersioningStatus) {
    client
        .put_bucket_versioning()
        .bucket(BUCKET)
        .versioning_configuration(VersioningConfiguration::builder().status(status).build())
        .send()
        .await
        .unwrap();
}

/// The next event, which a committed change has already sent by the time its response arrived.
fn next(events: &mut Subscriber) -> ObjectEvent {
    events.try_recv().expect("no event")
}

fn none(events: &mut Subscriber) {
    if let Some(event) = events.try_recv() {
        panic!("unexpected event {event:?}");
    }
}

fn usage(objects: i64, bytes: i64) -> Option<Usage> {
    Some(Usage { objects, bytes })
}

/// `(version id, ETag, request id)` of the PUT.
async fn put(client: &Client, key: &str, data: &[u8]) -> (Option<String>, String, String) {
    let out = client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from(data.to_vec()))
        .send()
        .await
        .unwrap();
    (
        out.version_id().map(str::to_owned),
        out.e_tag().unwrap().trim_matches('"').to_owned(),
        out.request_id().unwrap().to_owned(),
    )
}

async fn delete(client: &Client, key: &str, version: Option<&str>) -> Option<String> {
    client
        .delete_object()
        .bucket(BUCKET)
        .key(key)
        .set_version_id(version.map(str::to_owned))
        .send()
        .await
        .unwrap()
        .version_id()
        .map(str::to_owned)
}

/// A multipart upload of `parts`, completed; its ETag.
async fn upload(client: &Client, key: &str, parts: &[Vec<u8>]) -> String {
    let id = client
        .create_multipart_upload()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .unwrap()
        .upload_id()
        .unwrap()
        .to_owned();
    let mut done = Vec::new();
    for (i, data) in parts.iter().enumerate() {
        let n = i as i32 + 1;
        let part = client
            .upload_part()
            .bucket(BUCKET)
            .key(key)
            .upload_id(&id)
            .part_number(n)
            .body(ByteStream::from(data.clone()))
            .send()
            .await
            .unwrap();
        done.push(
            CompletedPart::builder()
                .part_number(n)
                .e_tag(part.e_tag().unwrap())
                .build(),
        );
    }
    let out = client
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key(key)
        .upload_id(&id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(done))
                .build(),
        )
        .send()
        .await
        .unwrap();
    out.e_tag().unwrap().trim_matches('"').to_owned()
}

#[tokio::test]
async fn each_write_announces_once_what_it_wrote_and_who_asked() {
    // Notifications are built from these: one per change, carrying the object as the client's
    // response described it and the request and key pair behind it, so a consumer can match
    // the event to the request in the access log.
    let server = server().await;
    let client = &server.client;
    let mut events = server.dal.subscribe();

    let (_, etag, request) = put(client, "a", b"hello").await;
    let event = next(&mut events);
    assert_eq!(event.kind, EventKind::Put);
    assert_eq!((event.bucket.as_str(), event.key.as_str()), (BUCKET, "a"));
    assert_eq!(event.size, Some(5));
    assert_eq!(event.etag.as_deref(), Some(etag.as_str()));
    assert_eq!(event.version, None, "the bucket never had versioning");
    assert_eq!(event.request_id.as_deref(), Some(request.as_str()));
    assert_eq!(event.access_key.as_deref(), Some(ACCESS_KEY));
    assert_eq!(event.usage, None, "the bucket has no quota");
    none(&mut events);

    let copied = client
        .copy_object()
        .bucket(BUCKET)
        .key("b")
        .copy_source(format!("{BUCKET}/a"))
        .send()
        .await
        .unwrap();
    let event = next(&mut events);
    assert_eq!(event.kind, EventKind::Copy);
    assert_eq!((event.key.as_str(), event.size), ("b", Some(5)));
    let copy_etag = copied.copy_object_result().unwrap().e_tag().unwrap();
    assert_eq!(event.etag.as_deref(), Some(copy_etag.trim_matches('"')));
    assert_eq!(event.request_id.as_deref(), copied.request_id());
    none(&mut events);

    let parts = vec![vec![1u8; 5 << 20], b"tail".to_vec()];
    let etag = upload(client, "c", &parts).await;
    let event = next(&mut events);
    assert_eq!(event.kind, EventKind::Complete);
    assert_eq!(event.key, "c");
    assert_eq!(event.size, Some((5 << 20) + 4));
    assert_eq!(event.etag.as_deref(), Some(etag.as_str()));
    assert_eq!(event.access_key.as_deref(), Some(ACCESS_KEY));
    // Creating the upload and its parts change no key.
    none(&mut events);
}

#[tokio::test]
async fn each_removal_announces_once_and_a_delete_of_nothing_announces_nothing() {
    // DeleteObjects is one request but many changes: each key it removed is an event of its
    // own, and a key that held nothing changed nothing.
    let server = server().await;
    let client = &server.client;
    for key in ["a", "b", "c"] {
        put(client, key, b"x").await;
    }
    let mut events = server.dal.subscribe();

    delete(client, "a", None).await;
    let event = next(&mut events);
    assert_eq!((event.kind, event.key.as_str()), (EventKind::Delete, "a"));
    assert_eq!((event.size, event.etag.as_deref()), (None, None));
    assert_eq!(event.access_key.as_deref(), Some(ACCESS_KEY));
    delete(client, "missing", None).await;
    none(&mut events);

    let ids = ["b", "missing", "c"]
        .map(|k| ObjectIdentifier::builder().key(k).build().unwrap())
        .to_vec();
    let out = client
        .delete_objects()
        .bucket(BUCKET)
        .delete(Delete::builder().set_objects(Some(ids)).build().unwrap())
        .send()
        .await
        .unwrap();
    let mut keys: Vec<String> = std::iter::from_fn(|| events.try_recv())
        .map(|e| {
            assert_eq!(e.kind, EventKind::Delete);
            assert_eq!(e.request_id.as_deref(), out.request_id());
            e.key
        })
        .collect();
    keys.sort();
    assert_eq!(keys, ["b", "c"]);
}

#[tokio::test]
async fn a_write_that_fails_or_is_refused_announces_nothing() {
    // A consumer acting on an event (a thumbnail, a webhook) must never act on an object that
    // was not written.
    let server = server().await;
    let client = &server.client;
    put(client, "a", b"first").await;
    let mut events = server.dal.subscribe();

    let refused = client
        .put_object()
        .bucket(BUCKET)
        .key("a")
        .if_none_match("*")
        .body(ByteStream::from_static(b"second"))
        .send()
        .await;
    assert!(refused.is_err());
    let refused = client
        .copy_object()
        .bucket(BUCKET)
        .key("b")
        .copy_source(format!("{BUCKET}/missing"))
        .send()
        .await;
    assert!(refused.is_err());
    let refused = client
        .delete_object()
        .bucket(BUCKET)
        .key("a")
        .if_match("\"not-its-etag\"")
        .send()
        .await;
    assert!(refused.is_err());
    let refused = client
        .put_object()
        .bucket("no-such-bucket")
        .key("a")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await;
    assert!(refused.is_err());
    none(&mut events);
}

#[tokio::test]
async fn versioned_changes_name_the_version_or_marker() {
    let server = server().await;
    let client = &server.client;
    set_versioning(client, BucketVersioningStatus::Enabled).await;
    let mut events = server.dal.subscribe();

    let (first, _, _) = put(client, "k", b"one").await;
    assert_eq!(next(&mut events).version, first);
    let marker = delete(client, "k", None).await;
    let event = next(&mut events);
    assert_eq!(event.kind, EventKind::DeleteMarker);
    assert_eq!(event.version, marker);
    // Removing the marker for good brings the version back: a removal, of the marker.
    delete(client, "k", marker.as_deref()).await;
    let event = next(&mut events);
    assert_eq!((event.kind, event.version), (EventKind::Delete, marker));
    delete(client, "k", first.as_deref()).await;
    let event = next(&mut events);
    assert_eq!((event.kind, event.version), (EventKind::Delete, first));
    none(&mut events);
}

#[tokio::test]
async fn in_an_unversioned_bucket_an_overwrite_replaces_the_old_objects_bytes() {
    // The quota is counted from these: an overwrite that added the new size without taking
    // the old one away would fill the bucket with bytes it does not hold.
    let server = server().await;
    let client = &server.client;
    counted(&server).await;
    let mut events = server.dal.subscribe();

    put(client, "k", &[0; 10]).await;
    assert_eq!(next(&mut events).usage, usage(1, 10));
    put(client, "k", &[0; 4]).await;
    assert_eq!(next(&mut events).usage, usage(0, -6));
    client
        .copy_object()
        .bucket(BUCKET)
        .key("k")
        .copy_source(format!("{BUCKET}/k"))
        .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
        .send()
        .await
        .unwrap();
    assert_eq!(next(&mut events).usage, usage(0, 0));
    upload(client, "k", &[vec![1; 5 << 20], vec![2; 6]]).await;
    assert_eq!(next(&mut events).usage, usage(0, (5 << 20) + 6 - 4));
    delete(client, "k", None).await;
    assert_eq!(next(&mut events).usage, usage(-1, -((5 << 20) + 6)));

    // A folder marker holds nothing.
    put(client, "dir/", b"").await;
    assert_eq!(next(&mut events).usage, usage(0, 0));
    none(&mut events);
}

#[tokio::test]
async fn in_a_versioned_bucket_an_overwrite_or_a_marker_keeps_every_version() {
    // Every version takes space until it is deleted for good, as AWS bills it: an overwrite
    // adds, a delete marker frees nothing, only removing a version does.
    let server = server().await;
    let client = &server.client;
    counted(&server).await;
    set_versioning(client, BucketVersioningStatus::Enabled).await;
    let mut events = server.dal.subscribe();

    let (first, _, _) = put(client, "k", &[0; 10]).await;
    assert_eq!(next(&mut events).usage, usage(1, 10));
    let (second, _, _) = put(client, "k", &[0; 4]).await;
    assert_eq!(next(&mut events).usage, usage(1, 4));
    let marker = delete(client, "k", None).await;
    assert_eq!(next(&mut events).usage, usage(0, 0));
    // An archived version, then the marker, then the version the marker hid.
    delete(client, "k", first.as_deref()).await;
    assert_eq!(next(&mut events).usage, usage(-1, -10));
    delete(client, "k", marker.as_deref()).await;
    assert_eq!(next(&mut events).usage, usage(0, 0));
    delete(client, "k", second.as_deref()).await;
    assert_eq!(next(&mut events).usage, usage(-1, -4));

    // Suspended, a write replaces the `null` version, wherever it is, and keeps the rest.
    put(client, "s", &[0; 3]).await;
    assert_eq!(next(&mut events).usage, usage(1, 3));
    set_versioning(client, BucketVersioningStatus::Suspended).await;
    put(client, "s", &[0; 5]).await;
    assert_eq!(
        next(&mut events).usage,
        usage(1, 5),
        "the old version stays"
    );
    put(client, "s", &[0; 2]).await;
    assert_eq!(
        next(&mut events).usage,
        usage(0, -3),
        "the null version goes"
    );
    // A marker that replaces the null version frees it.
    delete(client, "s", None).await;
    assert_eq!(next(&mut events).usage, usage(-1, -2));
    put(client, "s", &[0; 7]).await;
    assert_eq!(
        next(&mut events).usage,
        usage(1, 7),
        "the null marker went, which held nothing"
    );
    // A null version archived while versioning was on goes as well when suspended again.
    set_versioning(client, BucketVersioningStatus::Enabled).await;
    put(client, "s", &[0; 9]).await;
    assert_eq!(next(&mut events).usage, usage(1, 9));
    set_versioning(client, BucketVersioningStatus::Suspended).await;
    put(client, "s", &[0; 1]).await;
    assert_eq!(
        next(&mut events).usage,
        usage(0, 1 - 7),
        "the archived null version goes"
    );
    none(&mut events);
}

#[tokio::test]
async fn deletes_in_a_batch_count_each_removal() {
    let server = server().await;
    let client = &server.client;
    counted(&server).await;
    put(client, "a", &[0; 3]).await;
    put(client, "b", &[0; 5]).await;
    let mut events = server.dal.subscribe();
    let ids = ["a", "b", "missing"]
        .map(|k| ObjectIdentifier::builder().key(k).build().unwrap())
        .to_vec();
    client
        .delete_objects()
        .bucket(BUCKET)
        .delete(Delete::builder().set_objects(Some(ids)).build().unwrap())
        .send()
        .await
        .unwrap();
    let mut changes: Vec<(String, Option<Usage>)> = std::iter::from_fn(|| events.try_recv())
        .map(|e| (e.key, e.usage))
        .collect();
    changes.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        changes,
        [
            ("a".to_owned(), usage(-1, -3)),
            ("b".to_owned(), usage(-1, -5))
        ]
    );
    tokio::time::sleep(Duration::from_millis(10)).await;
    none(&mut events);
}

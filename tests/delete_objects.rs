mod common;

use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::delete_objects::{DeleteObjectsError, DeleteObjectsOutput};
use aws_sdk_s3::primitives::{ByteStream, DateTime};
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use common::TestServer;

const BUCKET: &str = "bkt";

async fn server() -> TestServer {
    let server = TestServer::start().await;
    server.create_bucket_dir(BUCKET).await;
    server
}

async fn put(server: &TestServer, key: &str, data: &[u8]) -> String {
    let out = server
        .client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from(data.to_vec()))
        .send()
        .await
        .unwrap();
    out.e_tag().unwrap().to_owned()
}

fn id(key: &str) -> ObjectIdentifier {
    ObjectIdentifier::builder().key(key).build().unwrap()
}

fn ids(keys: &[&str]) -> Vec<ObjectIdentifier> {
    keys.iter().map(|k| id(k)).collect()
}

// Returned as the SDK returns it, so tests inspect it the same way; its size does not
// matter in a test.
#[allow(clippy::result_large_err)]
async fn delete(
    server: &TestServer,
    objects: Vec<ObjectIdentifier>,
    quiet: bool,
) -> Result<DeleteObjectsOutput, SdkError<DeleteObjectsError, HttpResponse>> {
    server
        .client
        .delete_objects()
        .bucket(BUCKET)
        .delete(
            Delete::builder()
                .set_objects(Some(objects))
                .quiet(quiet)
                .build()
                .unwrap(),
        )
        .send()
        .await
}

fn deleted_keys(out: &DeleteObjectsOutput) -> Vec<&str> {
    out.deleted().iter().map(|d| d.key().unwrap()).collect()
}

/// `(key, code)` of each per-key error.
fn errors(out: &DeleteObjectsOutput) -> Vec<(&str, &str)> {
    out.errors()
        .iter()
        .map(|e| (e.key().unwrap(), e.code().unwrap()))
        .collect()
}

async fn exists(server: &TestServer, key: &str) -> bool {
    server.exists(&format!("{BUCKET}/{key}")).await
}

#[tokio::test]
async fn each_key_succeeds_or_fails_on_its_own() {
    // Tools delete whole listings in batches; one bad key must not stop the others, and a
    // key that is already gone counts as deleted, exactly as DeleteObject reports it.
    let server = server().await;
    put(&server, "a", b"1").await;
    put(&server, "dir/b", b"2").await;

    let out = delete(&server, ids(&["a", "missing", "x//y", "dir/b"]), false)
        .await
        .unwrap();
    assert_eq!(deleted_keys(&out), ["a", "missing", "dir/b"]);
    assert_eq!(errors(&out), [("x//y", "InvalidArgument")]);
    assert!(out.errors()[0].message().is_some_and(|m| !m.is_empty()));

    assert!(!exists(&server, "a").await);
    // Deleting the last object under `dir/` prunes the directory, as DeleteObject does, so it
    // does not linger as a CommonPrefix.
    assert!(!exists(&server, "dir/").await);
}

#[tokio::test]
async fn a_batch_prunes_every_directory_it_empties() {
    // A batch prunes each emptied directory once, after its deletes, instead of after every
    // key. It must leave the same state: no directory it emptied, up the whole chain, lingers
    // as a CommonPrefix, and none that still holds an object goes.
    let server = server().await;
    let mut keys = Vec::new();
    for dir in ["p/q", "p/r", "s/t/u"] {
        for i in 0..20 {
            keys.push(format!("{dir}/{i}"));
        }
    }
    for key in &keys {
        put(&server, key, b"x").await;
    }
    put(&server, "p/keep", b"x").await;

    let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    let out = delete(&server, ids(&refs), true).await.unwrap();
    assert!(out.errors().is_empty(), "{:?}", out.errors());

    for gone in ["p/q/", "p/r/", "s/t/u/", "s/t/", "s/"] {
        assert!(!exists(&server, gone).await, "{gone} lingers");
    }
    assert!(exists(&server, "p/keep").await);
}

#[tokio::test]
async fn a_key_the_response_cannot_carry_refuses_the_request_before_any_delete() {
    // The response names every key, and XML cannot carry a control character. Found only once
    // the response is written, it failed the request with a 500 after the other keys were
    // gone; a client syncing a listing from another store would retry into the same thing.
    let server = server().await;
    put(&server, "ok", b"1").await;
    for objects in [
        ids(&["ok", "a\u{1}b"]),
        vec![
            id("ok"),
            ObjectIdentifier::builder()
                .key("k")
                .version_id("v\u{1}")
                .build()
                .unwrap(),
        ],
    ] {
        let err = delete(&server, objects, false).await.unwrap_err();
        assert_eq!(err.code(), Some("InvalidArgument"), "{err:?}");
        assert!(exists(&server, "ok").await);
    }
}

#[tokio::test]
async fn quiet_mode_reports_only_errors() {
    // Quiet keeps the response small for large batches; clients then read only the failures.
    let server = server().await;
    put(&server, "a", b"1").await;

    let out = delete(&server, ids(&["a", "x//y"]), true).await.unwrap();
    assert!(out.deleted().is_empty(), "{:?}", out.deleted());
    assert_eq!(errors(&out), [("x//y", "InvalidArgument")]);
    assert!(!exists(&server, "a").await);
}

#[tokio::test]
async fn etags_guard_each_key() {
    // ObjectIdentifier.ETag is If-Match for that key: a key that changed since the client
    // listed it must survive the batch.
    let server = server().await;
    let go = put(&server, "go", b"1").await;
    put(&server, "keep", b"2").await;

    let objects = vec![
        ObjectIdentifier::builder()
            .key("go")
            .e_tag(&go)
            .build()
            .unwrap(),
        ObjectIdentifier::builder()
            .key("keep")
            .e_tag("\"00000000000000000000000000000000\"")
            .build()
            .unwrap(),
        ObjectIdentifier::builder()
            .key("gone")
            .e_tag(&go)
            .build()
            .unwrap(),
    ];
    let out = delete(&server, objects, false).await.unwrap();
    assert_eq!(deleted_keys(&out), ["go"]);
    assert_eq!(
        errors(&out),
        [("keep", "PreconditionFailed"), ("gone", "NoSuchKey")]
    );
    assert!(!exists(&server, "go").await);
    assert!(exists(&server, "keep").await);
}

#[tokio::test]
async fn at_most_1000_keys_and_at_least_one() {
    // AWS rejects an oversized or empty batch as a whole, so a client that splits batches
    // wrongly finds out instead of losing part of the request.
    let server = server().await;
    put(&server, "k0", b"1").await;
    let keys: Vec<String> = (0..1001).map(|i| format!("k{i}")).collect();
    let objects = |n: usize| keys[..n].iter().map(|k| id(k)).collect();

    let err = delete(&server, objects(1001), false).await.unwrap_err();
    assert_eq!(err.code(), Some("MalformedXML"), "{err:?}");
    assert!(exists(&server, "k0").await);

    let err = delete(&server, Vec::new(), false).await.unwrap_err();
    assert_eq!(err.code(), Some("MalformedXML"), "{err:?}");

    let out = delete(&server, objects(1000), false).await.unwrap();
    assert_eq!(out.deleted().len(), 1000);
    assert!(out.errors().is_empty());
    assert!(!exists(&server, "k0").await);
}

#[tokio::test]
async fn an_etag_of_star_requires_only_that_the_key_exists() {
    // AWS documents `*` here as If-Match: * does on DeleteObject: delete whatever object the
    // key holds, and report a key with none as missing instead of deleted.
    let server = server().await;
    put(&server, "a", b"1").await;
    let objects = vec![
        ObjectIdentifier::builder()
            .key("a")
            .e_tag("*")
            .build()
            .unwrap(),
        ObjectIdentifier::builder()
            .key("gone")
            .e_tag("*")
            .build()
            .unwrap(),
    ];
    let out = delete(&server, objects, false).await.unwrap();
    assert_eq!(deleted_keys(&out), ["a"]);
    assert_eq!(errors(&out), [("gone", "NoSuchKey")]);
    assert!(!exists(&server, "a").await);
}

#[tokio::test]
async fn sizes_and_times_guard_each_key() {
    // A batch built from a listing must skip keys replaced since: each key is deleted only if
    // its Size and LastModified still match the listing, whose times carry milliseconds where
    // S3 compares seconds. A key already gone counts as deleted, as AWS answers it with 204.
    let server = server().await;
    put(&server, "a", b"1").await;
    put(&server, "b", b"22").await;
    put(&server, "c", b"333").await;
    let listed = server
        .client
        .list_objects_v2()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    let mtime = |key: &str| {
        *listed
            .contents()
            .iter()
            .find(|o| o.key() == Some(key))
            .and_then(|o| o.last_modified())
            .unwrap()
    };

    let objects = vec![
        ObjectIdentifier::builder()
            .key("a")
            .size(1)
            .last_modified_time(mtime("a"))
            .build()
            .unwrap(),
        ObjectIdentifier::builder()
            .key("b")
            .size(1)
            .build()
            .unwrap(),
        ObjectIdentifier::builder()
            .key("c")
            .last_modified_time(DateTime::from_secs(mtime("c").secs() - 1))
            .build()
            .unwrap(),
        ObjectIdentifier::builder()
            .key("gone")
            .size(1)
            .build()
            .unwrap(),
    ];
    let out = delete(&server, objects, false).await.unwrap();
    assert_eq!(deleted_keys(&out), ["a", "gone"]);
    assert_eq!(
        errors(&out),
        [("b", "PreconditionFailed"), ("c", "PreconditionFailed")]
    );
    assert!(!exists(&server, "a").await);
    assert!(exists(&server, "b").await);
    assert!(exists(&server, "c").await);
}

#[tokio::test]
async fn only_the_null_version_can_be_deleted() {
    // Unversioned objects have the version id "null". Deleting another version must not
    // delete the current object in its place.
    let server = server().await;
    put(&server, "a", b"1").await;
    put(&server, "b", b"2").await;

    let objects = vec![
        ObjectIdentifier::builder()
            .key("a")
            .version_id("null")
            .build()
            .unwrap(),
        ObjectIdentifier::builder()
            .key("b")
            .version_id("3HL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY")
            .build()
            .unwrap(),
    ];
    let out = delete(&server, objects, false).await.unwrap();
    assert_eq!(deleted_keys(&out), ["a"]);
    assert_eq!(out.deleted()[0].version_id(), Some("null"));
    assert_eq!(errors(&out), [("b", "NoSuchVersion")]);
    assert!(!exists(&server, "a").await);
    assert!(exists(&server, "b").await);
}

#[tokio::test]
async fn missing_bucket_fails_the_whole_request() {
    // Reporting every key as deleted from a bucket that does not exist would hide a typo in
    // the bucket name.
    let server = TestServer::start().await;
    let err = server
        .client
        .delete_objects()
        .bucket("nosuchbucket")
        .delete(
            Delete::builder()
                .set_objects(Some(ids(&["a"])))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("NoSuchBucket"), "{err:?}");
}

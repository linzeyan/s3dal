//! S3 versioning (versioning.md). Each test is named after the rule it holds, and runs on the
//! suite's backend: fs archives by hard link, object stores by copy.

mod common;

use std::collections::BTreeMap;

use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, Delete, MfaDelete,
    ObjectIdentifier, VersioningConfiguration,
};
use common::TestServer;

const BUCKET: &str = "bkt";

async fn server() -> TestServer {
    let server = TestServer::start().await;
    server.create_bucket_dir(BUCKET).await;
    server
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

async fn versioned() -> TestServer {
    let server = server().await;
    set_versioning(&server.client, BucketVersioningStatus::Enabled).await;
    server
}

/// The new version's id.
async fn put(client: &Client, key: &str, data: &str) -> Option<String> {
    client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from(data.as_bytes().to_vec()))
        .send()
        .await
        .unwrap()
        .version_id()
        .map(str::to_owned)
}

async fn get(client: &Client, key: &str, version: Option<&str>) -> (String, Option<String>) {
    let out = client
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .set_version_id(version.map(str::to_owned))
        .send()
        .await
        .unwrap();
    let id = out.version_id().map(str::to_owned);
    let body = out.body.collect().await.unwrap().into_bytes();
    (String::from_utf8(body.to_vec()).unwrap(), id)
}

/// `(code, status, x-amz-delete-marker, x-amz-version-id)` of a GET that fails.
async fn get_err(
    client: &Client,
    key: &str,
    version: Option<&str>,
) -> (String, u16, Option<String>, Option<String>) {
    let err = client
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .set_version_id(version.map(str::to_owned))
        .send()
        .await
        .unwrap_err();
    let raw = err.raw_response().unwrap();
    let header = |name| raw.headers().get(name).map(str::to_owned);
    let (marker, id) = (header("x-amz-delete-marker"), header("x-amz-version-id"));
    let status = raw.status().as_u16();
    (
        err.code().unwrap_or_default().to_owned(),
        status,
        marker,
        id,
    )
}

/// The marker's id, or the removed version's.
async fn delete(client: &Client, key: &str, version: Option<&str>) -> (Option<String>, bool) {
    let out = client
        .delete_object()
        .bucket(BUCKET)
        .key(key)
        .set_version_id(version.map(str::to_owned))
        .send()
        .await
        .unwrap();
    (
        out.version_id().map(str::to_owned),
        out.delete_marker() == Some(true),
    )
}

/// One entry of ListObjectVersions. The SDK splits a page into versions and markers, which
/// loses how they interleave, and Last-Modified cannot restore it: some backends keep whole
/// seconds. So a key's entries are compared as the latest, then its versions in listed order,
/// then its markers in listed order.
#[derive(Debug, PartialEq, Eq, Clone)]
struct Listed {
    key: String,
    id: String,
    marker: bool,
    latest: bool,
}

async fn list_all(client: &Client, prefix: Option<&str>, page: i32) -> Vec<Listed> {
    let mut all = Vec::new();
    let (mut key_marker, mut version_marker) = (None::<String>, None::<String>);
    loop {
        let out = client
            .list_object_versions()
            .bucket(BUCKET)
            .set_prefix(prefix.map(str::to_owned))
            .max_keys(page)
            .set_key_marker(key_marker.clone())
            .set_version_id_marker(version_marker.clone())
            .send()
            .await
            .unwrap();
        let mut found: Vec<Listed> = Vec::new();
        for v in out.versions() {
            found.push(Listed {
                key: v.key().unwrap().to_owned(),
                id: v.version_id().unwrap().to_owned(),
                marker: false,
                latest: v.is_latest() == Some(true),
            });
        }
        for m in out.delete_markers() {
            found.push(Listed {
                key: m.key().unwrap().to_owned(),
                id: m.version_id().unwrap().to_owned(),
                marker: true,
                latest: m.is_latest() == Some(true),
            });
        }
        assert!(
            found.len() <= usize::try_from(page).unwrap(),
            "a page holds at most max-keys entries"
        );
        all.extend(found);
        if out.is_truncated() != Some(true) {
            // Stable: within a kind, the listed order stays.
            all.sort_by(|a, b| {
                a.key
                    .cmp(&b.key)
                    .then(b.latest.cmp(&a.latest))
                    .then(a.marker.cmp(&b.marker))
            });
            return all;
        }
        key_marker = out.next_key_marker().map(str::to_owned);
        version_marker = out.next_version_id_marker().map(str::to_owned);
        assert!(
            key_marker.is_some(),
            "a truncated page names where to resume"
        );
    }
}

#[tokio::test]
async fn a_bucket_reports_its_versioning_state_and_never_returns_to_unversioned() {
    let server = server().await;
    let status = || async {
        server
            .client
            .get_bucket_versioning()
            .bucket(BUCKET)
            .send()
            .await
            .unwrap()
            .status()
            .cloned()
    };
    // A bucket that never had versioning answers with no Status, as on AWS.
    assert_eq!(status().await, None);
    set_versioning(&server.client, BucketVersioningStatus::Suspended).await;
    assert_eq!(status().await, Some(BucketVersioningStatus::Suspended));
    set_versioning(&server.client, BucketVersioningStatus::Enabled).await;
    assert_eq!(status().await, Some(BucketVersioningStatus::Enabled));
    set_versioning(&server.client, BucketVersioningStatus::Suspended).await;
    assert_eq!(status().await, Some(BucketVersioningStatus::Suspended));

    // MFA delete is a protection: refused, not accepted and ignored.
    let err = server
        .client
        .put_bucket_versioning()
        .bucket(BUCKET)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .mfa_delete(MfaDelete::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("NotImplemented"), "{err:?}");
}

/// Uploads `parts` as one multipart object; its version id and ETag.
async fn multipart(client: &Client, key: &str, parts: &[Vec<u8>]) -> (Option<String>, String) {
    let upload = client
        .create_multipart_upload()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .unwrap();
    let id = upload.upload_id().unwrap();
    let mut completed = CompletedMultipartUpload::builder();
    for (n, data) in (1..).zip(parts) {
        let part = client
            .upload_part()
            .bucket(BUCKET)
            .key(key)
            .upload_id(id)
            .part_number(n)
            .body(ByteStream::from(data.clone()))
            .send()
            .await
            .unwrap();
        completed = completed.parts(
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
        .upload_id(id)
        .multipart_upload(completed.build())
        .send()
        .await
        .unwrap();
    (
        out.version_id().map(str::to_owned),
        out.e_tag().unwrap().to_owned(),
    )
}

/// `(ETag, Last-Modified)` of the version HEAD finds.
async fn head(client: &Client, key: &str, version: Option<&str>) -> (String, i64) {
    let out = client
        .head_object()
        .bucket(BUCKET)
        .key(key)
        .set_version_id(version.map(str::to_owned))
        .send()
        .await
        .unwrap();
    (
        out.e_tag().unwrap().to_owned(),
        out.last_modified().unwrap().secs(),
    )
}

#[tokio::test]
async fn an_archived_version_keeps_its_etag_and_last_modified() {
    // On an object store the archive is a copy, whose own ETag (S3's for what it assembled) and
    // mtime are the copy's. Clients compare a version's ETag with what they uploaded, and
    // restore a version by its time, so both must stay the version's own.
    let server = versioned().await;
    let client = &server.client;
    let body = [vec![b'x'; 5 * 1024 * 1024], b"tail".to_vec()];
    let (v1, etag) = multipart(client, "k", &body).await;
    let v1 = v1.unwrap();
    let before = head(client, "k", None).await;
    assert_eq!(before.0, etag);
    // HEAD reports whole seconds.
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let v2 = put(client, "k", "small").await.unwrap();
    assert_eq!(head(client, "k", Some(&v1)).await, before);
    let listed = client
        .list_object_versions()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    let old = listed
        .versions()
        .iter()
        .find(|v| v.version_id() == Some(v1.as_str()))
        .unwrap();
    assert_eq!(old.e_tag(), Some(etag.as_str()));
    assert_eq!(old.last_modified().unwrap().secs(), before.1);

    // Deleting the latest makes v1 current again, with its ETag, also where a listing reads the
    // backend's own ETag instead of a HEAD (S3 upstreams).
    delete(client, "k", Some(&v2)).await;
    assert_eq!(head(client, "k", None).await.0, etag);
    let objects = client
        .list_objects_v2()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    assert_eq!(objects.contents()[0].e_tag(), Some(etag.as_str()));
    let (data, id) = get(client, "k", None).await;
    assert_eq!(id, Some(v1));
    assert_eq!(data.len(), 5 * 1024 * 1024 + 4);
}

#[tokio::test]
async fn every_overwrite_keeps_the_version_it_replaced() {
    let server = versioned().await;
    let client = &server.client;
    let mut ids = Vec::new();
    for body in ["one", "two", "three"] {
        ids.push(put(client, "k", body).await.expect("a version id"));
    }
    assert_eq!(
        ids.iter().collect::<std::collections::HashSet<_>>().len(),
        3,
        "{ids:?}"
    );
    assert_eq!(
        get(client, "k", None).await,
        ("three".to_owned(), Some(ids[2].clone()))
    );
    for (id, body) in ids.iter().zip(["one", "two", "three"]) {
        assert_eq!(
            get(client, "k", Some(id)).await,
            (body.to_owned(), Some(id.clone()))
        );
    }
    let head = client
        .head_object()
        .bucket(BUCKET)
        .key("k")
        .version_id(&ids[0])
        .send()
        .await
        .unwrap();
    assert_eq!(head.content_length(), Some(3));
    assert_eq!(head.version_id(), Some(ids[0].as_str()));

    let listed = list_all(client, None, 1000).await;
    let expected: Vec<Listed> = ids
        .iter()
        .rev()
        .enumerate()
        .map(|(i, id)| Listed {
            key: "k".into(),
            id: id.clone(),
            marker: false,
            latest: i == 0,
        })
        .collect();
    assert_eq!(listed, expected);

    // An id s3dal never issued is refused; one it could have issued but did not is missing.
    let (code, status, ..) = get_err(client, "k", Some("not-an-id")).await;
    assert_eq!((code.as_str(), status), ("InvalidArgument", 400));
    let (code, status, ..) = get_err(client, "k", Some(&"0".repeat(32))).await;
    assert_eq!((code.as_str(), status), ("NoSuchVersion", 404));
}

#[tokio::test]
async fn a_delete_without_a_version_adds_a_marker_and_loses_nothing() {
    let server = versioned().await;
    let client = &server.client;
    let v1 = put(client, "k", "data").await.unwrap();
    let (marker, is_marker) = delete(client, "k", None).await;
    let marker = marker.expect("the marker's id");
    assert!(is_marker);
    assert_ne!(marker, v1);

    // Clients tell a deleted key from one never written by the marker's headers.
    let (code, status, dm, id) = get_err(client, "k", None).await;
    assert_eq!((code.as_str(), status), ("NoSuchKey", 404));
    assert_eq!(dm.as_deref(), Some("true"));
    assert_eq!(id.as_deref(), Some(marker.as_str()));
    let (code, status, dm, _) = get_err(client, "k", Some(&marker)).await;
    assert_eq!((code.as_str(), status), ("MethodNotAllowed", 405));
    assert_eq!(dm.as_deref(), Some("true"));
    assert_eq!(get(client, "k", Some(&v1)).await.0, "data");
    // A plain listing no longer shows the key.
    let objects = client
        .list_objects_v2()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    assert!(objects.contents().is_empty());

    assert_eq!(
        list_all(client, None, 1000).await,
        [
            Listed {
                key: "k".into(),
                id: marker.clone(),
                marker: true,
                latest: true
            },
            Listed {
                key: "k".into(),
                id: v1.clone(),
                marker: false,
                latest: false
            },
        ]
    );

    // Removing the marker undeletes the key.
    let (removed, was_marker) = delete(client, "k", Some(&marker)).await;
    assert_eq!(removed.as_deref(), Some(marker.as_str()));
    assert!(was_marker);
    assert_eq!(get(client, "k", None).await, ("data".to_owned(), Some(v1)));
}

#[tokio::test]
async fn deleting_the_current_version_makes_the_one_before_it_current() {
    let server = versioned().await;
    let client = &server.client;
    let v1 = put(client, "a/k", "one").await.unwrap();
    let v2 = put(client, "a/k", "two").await.unwrap();
    let (removed, is_marker) = delete(client, "a/k", Some(&v2)).await;
    assert_eq!((removed.as_deref(), is_marker), (Some(v2.as_str()), false));
    assert_eq!(
        get(client, "a/k", None).await,
        ("one".to_owned(), Some(v1.clone()))
    );
    // A version that is gone is answered as missing, and deleting it again succeeds, as on AWS,
    // even with If-Match: a client retrying a conditional delete must not see it fail.
    let (code, ..) = get_err(client, "a/k", Some(&v2)).await;
    assert_eq!(code, "NoSuchVersion");
    delete(client, "a/k", Some(&v2)).await;
    client
        .delete_object()
        .bucket(BUCKET)
        .key("a/k")
        .version_id(&v2)
        .if_match("*")
        .send()
        .await
        .unwrap();

    // The last version gone, nothing is left of the key or its folder.
    delete(client, "a/k", Some(&v1)).await;
    assert!(list_all(client, None, 1000).await.is_empty());
    assert!(
        !server.exists("bkt/a/").await,
        "an emptied folder is pruned"
    );
    let objects = client
        .list_objects_v2()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    assert!(objects.contents().is_empty() && objects.common_prefixes().is_empty());
}

#[tokio::test]
async fn objects_from_before_versioning_are_the_null_version() {
    let server = server().await;
    let client = &server.client;
    assert_eq!(
        put(client, "k", "old").await,
        None,
        "no version id without versioning"
    );
    set_versioning(client, BucketVersioningStatus::Enabled).await;
    assert_eq!(
        get(client, "k", None).await,
        ("old".to_owned(), Some("null".to_owned()))
    );
    let v1 = put(client, "k", "new").await.unwrap();
    assert_eq!(get(client, "k", Some("null")).await.0, "old");
    assert_eq!(get(client, "k", Some(&v1)).await.0, "new");
    let ids: Vec<String> = list_all(client, None, 1000)
        .await
        .into_iter()
        .map(|l| l.id)
        .collect();
    assert_eq!(ids, [v1, "null".to_owned()]);
}

#[tokio::test]
async fn in_a_suspended_bucket_writes_and_deletes_replace_the_null_version() {
    let server = versioned().await;
    let client = &server.client;
    let v1 = put(client, "k", "kept").await.unwrap();
    set_versioning(client, BucketVersioningStatus::Suspended).await;
    // Such a write names no version, as on AWS: it replaces `null` rather than adding one.
    assert_eq!(put(client, "k", "first null").await, None);
    assert_eq!(put(client, "k", "second null").await, None);
    assert_eq!(get(client, "k", None).await.1.as_deref(), Some("null"));
    // One null version only; the real one stays.
    let listed = list_all(client, None, 1000).await;
    let ids: Vec<&str> = listed.iter().map(|l| l.id.as_str()).collect();
    assert_eq!(ids, ["null", v1.as_str()]);
    assert_eq!(get(client, "k", None).await.0, "second null");

    // A delete writes a null marker, replacing the null version too.
    let (marker, is_marker) = delete(client, "k", None).await;
    assert_eq!((marker.as_deref(), is_marker), (Some("null"), true));
    let listed = list_all(client, None, 1000).await;
    assert_eq!(
        listed,
        [
            Listed {
                key: "k".into(),
                id: "null".into(),
                marker: true,
                latest: true
            },
            Listed {
                key: "k".into(),
                id: v1.clone(),
                marker: false,
                latest: false
            },
        ]
    );
    assert_eq!(get(client, "k", Some(&v1)).await.0, "kept");
}

#[tokio::test]
async fn a_multipart_upload_and_a_copy_each_create_a_version() {
    let server = versioned().await;
    let client = &server.client;
    let v1 = put(client, "k", "small").await.unwrap();
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
        .body(ByteStream::from(b"multipart".to_vec()))
        .send()
        .await
        .unwrap();
    let completed = client
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
    let v2 = completed
        .version_id()
        .expect("Complete names the version")
        .to_owned();
    assert_ne!(v1, v2);
    assert_eq!(
        get(client, "k", None).await,
        ("multipart".to_owned(), Some(v2.clone()))
    );
    assert_eq!(get(client, "k", Some(&v1)).await.0, "small");

    // Copying the old version onto its key restores it as a new latest version.
    let copied = client
        .copy_object()
        .bucket(BUCKET)
        .key("k")
        .copy_source(format!("{BUCKET}/k?versionId={v1}"))
        .send()
        .await
        .unwrap();
    assert_eq!(copied.copy_source_version_id(), Some(v1.as_str()));
    let v3 = copied.version_id().unwrap().to_owned();
    assert_eq!(
        get(client, "k", None).await,
        ("small".to_owned(), Some(v3.clone()))
    );
    let ids: Vec<String> = list_all(client, None, 1000)
        .await
        .into_iter()
        .map(|l| l.id)
        .collect();
    assert_eq!(ids, [v3, v2, v1]);
}

#[tokio::test]
async fn a_delete_marker_cannot_be_a_copy_source() {
    let server = versioned().await;
    let client = &server.client;
    put(client, "k", "x").await;
    let (marker, _) = delete(client, "k", None).await;
    let err = client
        .copy_object()
        .bucket(BUCKET)
        .key("other")
        .copy_source(format!("{BUCKET}/k?versionId={}", marker.unwrap()))
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("InvalidRequest"), "{err:?}");
}

#[tokio::test]
async fn delete_objects_writes_markers_and_removes_named_versions() {
    let server = versioned().await;
    let client = &server.client;
    let a1 = put(client, "a", "a").await.unwrap();
    let b1 = put(client, "b", "b").await.unwrap();
    let out = client
        .delete_objects()
        .bucket(BUCKET)
        .delete(
            Delete::builder()
                .objects(ObjectIdentifier::builder().key("a").build().unwrap())
                .objects(
                    ObjectIdentifier::builder()
                        .key("b")
                        .version_id(&b1)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert!(out.errors().is_empty(), "{:?}", out.errors());
    let deleted: BTreeMap<&str, _> = out
        .deleted()
        .iter()
        .map(|d| (d.key().unwrap(), d))
        .collect();
    let a = deleted["a"];
    assert_eq!(a.delete_marker(), Some(true));
    let marker = a.delete_marker_version_id().unwrap().to_owned();
    let b = deleted["b"];
    assert_eq!(b.version_id(), Some(b1.as_str()));
    assert_ne!(b.delete_marker(), Some(true));

    let listed = list_all(client, None, 1000).await;
    assert_eq!(
        listed,
        [
            Listed {
                key: "a".into(),
                id: marker,
                marker: true,
                latest: true
            },
            Listed {
                key: "a".into(),
                id: a1,
                marker: false,
                latest: false
            },
        ]
    );
}

#[tokio::test]
async fn a_bucket_with_versions_left_is_not_empty() {
    let server = versioned().await;
    let client = &server.client;
    let v1 = put(client, "k", "x").await.unwrap();
    let (marker, _) = delete(client, "k", None).await;
    let refused = client
        .delete_bucket()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Some("BucketNotEmpty"));
    delete(client, "k", Some(&v1)).await;
    delete(client, "k", marker.as_deref()).await;
    client.delete_bucket().bucket(BUCKET).send().await.unwrap();

    // A new bucket of the name starts unversioned, with nothing of the old one.
    client.create_bucket().bucket(BUCKET).send().await.unwrap();
    let status = client
        .get_bucket_versioning()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    assert_eq!(status.status(), None);
    assert_eq!(put(client, "k", "y").await, None);
}

#[tokio::test]
async fn a_new_bucket_does_not_inherit_what_a_deleted_one_left() {
    // DeleteBucket that crashed after removing the bucket left its state and versions.
    let server = versioned().await;
    put(&server.client, "k", "x").await;
    put(&server.client, "k", "y").await;
    server.op.delete("bkt/k").await.unwrap();
    server.op.delete("bkt/").await.unwrap();
    server
        .client
        .create_bucket()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    assert!(list_all(&server.client, None, 1000).await.is_empty());
    assert!(!server.exists(".s3dal/versions/bkt/").await);
    assert!(!server.exists(".s3dal/buckets/bkt.json").await);
}

#[tokio::test]
async fn duplicate_archives_of_a_version_are_listed_once_and_deleted_together() {
    // A close that failed after the archive leaves the current version archived too, and two
    // instances archiving one version each leave an entry of it. Neither may be listed twice,
    // archived again, or survive a delete of the version.
    let server = versioned().await;
    let client = &server.client;
    let holder = ".s3dal/versions/bkt/k!!/";
    let v1 = put(client, "k", "x").await.unwrap();
    server
        .op
        .copy("bkt/k", &format!("{holder}{:016x}-{v1}", u64::MAX - 1))
        .await
        .unwrap();
    assert_eq!(list_all(client, None, 1000).await.len(), 1);
    let v2 = put(client, "k", "y").await.unwrap();
    let entry = format!("{holder}{}", server.names(holder).await[0]);
    server
        .op
        .copy(&entry, &format!("{holder}{:016x}-{v1}", 1))
        .await
        .unwrap();
    let ids: Vec<String> = list_all(client, None, 1000)
        .await
        .into_iter()
        .map(|l| l.id)
        .collect();
    assert_eq!(ids, [v2.clone(), v1.clone()]);
    delete(client, "k", Some(&v1)).await;
    let (code, ..) = get_err(client, "k", Some(&v1)).await;
    assert_eq!(code, "NoSuchVersion");
    assert!(server.names(holder).await.is_empty());
}

#[tokio::test]
async fn keys_too_long_to_keep_their_versions_are_refused_up_front() {
    // `!` is escaped to two bytes in the versions tree, so a name that fits as a key does not
    // fit there. Accepted, its first overwrite would fail instead.
    let server = server().await;
    // fs takes 255 bytes per name; object stores 1024 per object name, and some (RustFS) keep
    // each name segment as a file, so the key is split into segments.
    let key = if server.op.info().scheme() == "fs" {
        "!".repeat(130)
    } else {
        vec!["!".repeat(100); 5].join("/")
    };
    assert_eq!(put(&server.client, &key, "x").await, None);
    set_versioning(&server.client, BucketVersioningStatus::Enabled).await;
    let err = server
        .client
        .put_object()
        .bucket(BUCKET)
        .key(&key)
        .body(ByteStream::from_static(b"y"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("KeyTooLongError"), "{err:?}");
}

#[tokio::test]
async fn version_listings_page_through_every_version_exactly_once() {
    // Keys chosen around the bytes the versions tree escapes and the folder boundary, with
    // versions, markers, keys whose only entries are archived, and a CommonPrefix of markers.
    let server = versioned().await;
    let client = &server.client;
    let keys = ["a.txt", "a b", "a!", "a-", "a/x", "a/y", "b", "c/d/e"];
    for (n, key) in keys.iter().enumerate() {
        for i in 0..=n % 3 {
            put(client, key, &format!("{key}{i}")).await;
        }
        if n % 2 == 1 {
            delete(client, key, None).await;
        }
    }
    delete(client, "c/d/e", None).await;
    let full = list_all(client, None, 1000).await;
    let listed_keys: Vec<&str> = full.iter().map(|l| l.key.as_str()).collect();
    let mut sorted = listed_keys.clone();
    sorted.sort();
    assert_eq!(listed_keys, sorted, "keys in byte order");
    let expected: usize = keys
        .iter()
        .enumerate()
        .map(|(n, key)| n % 3 + 1 + n % 2 + usize::from(*key == "c/d/e"))
        .sum();
    assert_eq!(full.len(), expected);
    // Exactly one latest per key.
    for key in keys {
        assert_eq!(
            full.iter().filter(|l| l.key == key && l.latest).count(),
            1,
            "{key}"
        );
    }
    for page in [1, 2, 3, 5] {
        assert_eq!(list_all(client, None, page).await, full, "page size {page}");
    }
    let under_a: Vec<Listed> = full
        .iter()
        .filter(|l| l.key.starts_with("a/"))
        .cloned()
        .collect();
    assert_eq!(list_all(client, Some("a/"), 1).await, under_a);

    // With `/`, every folder is one CommonPrefix, also `c/`, which holds only markers and
    // old versions.
    let out = client
        .list_object_versions()
        .bucket(BUCKET)
        .delimiter("/")
        .send()
        .await
        .unwrap();
    let prefixes: Vec<&str> = out
        .common_prefixes()
        .iter()
        .map(|p| p.prefix().unwrap())
        .collect();
    assert_eq!(prefixes, ["a/", "c/"]);
    assert!(
        out.versions()
            .iter()
            .all(|v| !v.key().unwrap().contains('/'))
    );
}

#[tokio::test]
async fn unversioned_buckets_answer_as_before() {
    // No version ids in responses, `null` the only version, and no state written.
    let server = server().await;
    let client = &server.client;
    assert_eq!(put(client, "k", "x").await, None);
    assert_eq!(get(client, "k", None).await, ("x".to_owned(), None));
    assert_eq!(get(client, "k", Some("null")).await.0, "x");
    let (code, ..) = get_err(client, "k", Some(&"0".repeat(32))).await;
    assert_eq!(code, "NoSuchVersion");
    assert_eq!(delete(client, "k", None).await, (None, false));
    assert!(!server.exists(".s3dal/buckets/").await);
    assert!(!server.exists(".s3dal/versions/").await);
}

#[tokio::test]
async fn two_instances_overwriting_one_key_lose_no_version() {
    // Instances sharing a backend take no lock of each other's: only the backend's write
    // conditions keep one from replacing a version the other committed before it was archived.
    // fs checks no conditions and is one process per root (design.md), so there is nothing to
    // hold there.
    let a = versioned().await;
    if !a.profile.conditional_writes {
        eprintln!("not applicable: this backend checks no write conditions");
        return;
    }
    let b = TestServer::start_on(common::Backend::sharing(&a), s3dal::Settings::default()).await;
    let writes = |server: &TestServer, name: &'static str| {
        let client = server.client.clone();
        async move {
            let mut written = Vec::new();
            for i in 0..10 {
                let body = format!("{name}-{i}");
                // A write that lost its race answers a retryable conflict, which the SDK repeats
                // a few times; one that still fails is not acknowledged, so nothing of it is
                // owed.
                if let Ok(out) = client
                    .put_object()
                    .bucket(BUCKET)
                    .key("k")
                    .body(ByteStream::from(body.clone().into_bytes()))
                    .send()
                    .await
                {
                    written.push((out.version_id().unwrap().to_owned(), body));
                }
            }
            written
        }
    };
    let (from_a, from_b) = tokio::join!(writes(&a, "a"), writes(&b, "b"));
    let written: Vec<(String, String)> = from_a.into_iter().chain(from_b).collect();
    assert!(
        written.len() >= 10,
        "most writes succeed: {}",
        written.len()
    );
    let listed = list_all(&a.client, None, 1000).await;
    let mut listed_ids: Vec<&str> = listed.iter().map(|l| l.id.as_str()).collect();
    let mut written_ids: Vec<&str> = written.iter().map(|(id, _)| id.as_str()).collect();
    listed_ids.sort_unstable();
    written_ids.sort_unstable();
    assert_eq!(
        listed_ids, written_ids,
        "every acknowledged version, and only those"
    );
    // Each version holds what was written as it, not what replaced it during its archive.
    for (id, body) in &written {
        assert_eq!(&get(&a.client, "k", Some(id)).await.0, body);
    }
}

//! Bucket lifecycle: the configuration API, the rules applied as of an injected time (no test
//! waits a day), the events and metrics an expiry produces, and `x-amz-expiration`.

mod common;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::{ByteStream, DateTime};
use aws_sdk_s3::types::{
    AbortIncompleteMultipartUpload, BucketLifecycleConfiguration, BucketVersioningStatus,
    ExpirationStatus, LifecycleExpiration, LifecycleRule, LifecycleRuleAndOperator,
    LifecycleRuleFilter, NoncurrentVersionExpiration, Tag, Transition, TransitionStorageClass,
    VersioningConfiguration,
};
use common::TestServer;
use s3dal::events::{EventKind, Subscriber};
use s3dal::{Expired, Quota};

const BUCKET: &str = "bkt";
const DAY: u64 = 24 * 60 * 60;

async fn server() -> TestServer {
    let server = TestServer::start().await;
    server.create_bucket_dir(BUCKET).await;
    server
}

/// AWS's rounding, written out again so a change to the server's is caught: `days` days after
/// `t`, then on to the next midnight UTC.
fn due(t: SystemTime, days: u64) -> SystemTime {
    let secs = t.duration_since(UNIX_EPOCH).unwrap().as_secs() + days * DAY;
    UNIX_EPOCH + Duration::from_secs((secs / DAY + 1) * DAY)
}

fn rule(id: &str) -> aws_sdk_s3::types::builders::LifecycleRuleBuilder {
    LifecycleRule::builder()
        .id(id)
        .status(ExpirationStatus::Enabled)
}

fn prefix(p: &str) -> LifecycleRuleFilter {
    LifecycleRuleFilter::builder().prefix(p).build()
}

fn expire_after(days: i32) -> LifecycleExpiration {
    LifecycleExpiration::builder().days(days).build()
}

fn noncurrent_after(days: i32, newer: Option<i32>) -> NoncurrentVersionExpiration {
    NoncurrentVersionExpiration::builder()
        .noncurrent_days(days)
        .set_newer_noncurrent_versions(newer)
        .build()
}

// Returned as the SDK returns it, so tests inspect it the same way; its size does not
// matter in a test.
#[allow(clippy::result_large_err)]
async fn put_rules(
    client: &Client,
    rules: Vec<LifecycleRule>,
) -> Result<(), aws_sdk_s3::error::SdkError<impl ProvideErrorMetadata + std::fmt::Debug>> {
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
}

async fn set_rules(client: &Client, rules: Vec<LifecycleRule>) {
    put_rules(client, rules).await.unwrap();
}

async fn put(client: &Client, key: &str, len: usize) -> Option<String> {
    client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from(vec![b'x'; len]))
        .send()
        .await
        .unwrap()
        .version_id()
        .map(str::to_owned)
}

/// When the key's current version was created, as HEAD says.
async fn created(client: &Client, key: &str) -> SystemTime {
    let head = client
        .head_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .unwrap();
    let t = head.last_modified().unwrap();
    UNIX_EPOCH + Duration::from_secs(t.secs() as u64)
}

async fn keys(client: &Client) -> Vec<String> {
    let out = client
        .list_objects_v2()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    out.contents()
        .iter()
        .map(|o| o.key().unwrap().to_owned())
        .collect()
}

/// `(versions, delete markers)` as ListObjectVersions lists them, each as `key` or `key*` for
/// the latest.
async fn versions(client: &Client) -> (Vec<String>, Vec<String>) {
    let out = client
        .list_object_versions()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    let name = |key: Option<&str>, latest: Option<bool>| {
        format!(
            "{}{}",
            key.unwrap(),
            if latest == Some(true) { "*" } else { "" }
        )
    };
    (
        out.versions()
            .iter()
            .map(|v| name(v.key(), v.is_latest()))
            .collect(),
        out.delete_markers()
            .iter()
            .map(|m| name(m.key(), m.is_latest()))
            .collect(),
    )
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

fn drain(events: &mut Subscriber) -> Vec<(EventKind, String)> {
    std::iter::from_fn(|| events.try_recv())
        .map(|e| (e.kind, e.key))
        .collect()
}

fn expired(current: u64, noncurrent: u64, delete_markers: u64, uploads: u64) -> Expired {
    Expired {
        current,
        noncurrent,
        delete_markers,
        uploads,
    }
}

#[tokio::test]
async fn a_configuration_reads_back_as_it_was_put_and_can_be_removed() {
    // Terraform and the console compare what they read with what they put; a rule that came
    // back changed would be put again on every refresh.
    let server = server().await;
    let c = &server.client;
    let rules = vec![
        rule("logs")
            .filter(prefix("logs/"))
            .expiration(expire_after(30))
            .build()
            .unwrap(),
        rule("big")
            .filter(
                LifecycleRuleFilter::builder()
                    .and(
                        LifecycleRuleAndOperator::builder()
                            .prefix("media/")
                            .object_size_greater_than(1000)
                            .object_size_less_than(5000)
                            .build(),
                    )
                    .build(),
            )
            .expiration(
                LifecycleExpiration::builder()
                    .date(DateTime::from_secs(1_893_456_000)) // 2030-01-01
                    .build(),
            )
            .build()
            .unwrap(),
        rule("history")
            .filter(LifecycleRuleFilter::builder().build())
            .noncurrent_version_expiration(noncurrent_after(7, Some(3)))
            .expiration(
                LifecycleExpiration::builder()
                    .expired_object_delete_marker(true)
                    .build(),
            )
            .abort_incomplete_multipart_upload(
                AbortIncompleteMultipartUpload::builder()
                    .days_after_initiation(2)
                    .build(),
            )
            .status(ExpirationStatus::Disabled)
            .build()
            .unwrap(),
    ];
    set_rules(c, rules.clone()).await;
    let got = c
        .get_bucket_lifecycle_configuration()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    assert_eq!(got.rules(), rules.as_slice());

    // Removing it twice is fine, as on AWS.
    for _ in 0..2 {
        c.delete_bucket_lifecycle()
            .bucket(BUCKET)
            .send()
            .await
            .unwrap();
    }
    let err = c
        .get_bucket_lifecycle_configuration()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("NoSuchLifecycleConfiguration"));
}

#[tokio::test]
async fn invalid_rules_are_refused_with_aws_errors() {
    // s3-tests' lifecycle tests are the reference; a value AWS refuses must not be stored
    // and then applied in some guessed way.
    let server = server().await;
    let c = &server.client;
    let days = |d| rule("r").filter(prefix("")).expiration(expire_after(d));
    let cases: Vec<(Vec<LifecycleRule>, &str)> = vec![
        (vec![days(0).build().unwrap()], "InvalidArgument"),
        (
            vec![
                rule(&"a".repeat(256))
                    .filter(prefix(""))
                    .expiration(expire_after(1))
                    .build()
                    .unwrap(),
            ],
            "InvalidArgument",
        ),
        (
            vec![days(1).build().unwrap(), days(2).build().unwrap()],
            "InvalidArgument",
        ),
        (
            vec![
                rule("r")
                    .filter(prefix(""))
                    .expiration(
                        LifecycleExpiration::builder()
                            .date(DateTime::from_secs(1_893_456_000 + 3600))
                            .build(),
                    )
                    .build()
                    .unwrap(),
            ],
            "InvalidArgument",
        ),
        (
            vec![
                rule("r")
                    .filter(prefix(""))
                    .expiration(
                        LifecycleExpiration::builder()
                            .days(1)
                            .expired_object_delete_marker(true)
                            .build(),
                    )
                    .build()
                    .unwrap(),
            ],
            "MalformedXML",
        ),
        (
            vec![
                rule("r")
                    .filter(prefix(""))
                    .noncurrent_version_expiration(noncurrent_after(0, None))
                    .build()
                    .unwrap(),
            ],
            "InvalidArgument",
        ),
        (
            vec![
                rule("r")
                    .filter(
                        LifecycleRuleFilter::builder()
                            .object_size_greater_than(10)
                            .object_size_less_than(10)
                            .build(),
                    )
                    .expiration(expire_after(1))
                    .build()
                    .unwrap(),
            ],
            "InvalidArgument",
        ),
        (
            vec![
                rule("r")
                    .filter(
                        LifecycleRuleFilter::builder()
                            .object_size_greater_than(10)
                            .build(),
                    )
                    .abort_incomplete_multipart_upload(
                        AbortIncompleteMultipartUpload::builder()
                            .days_after_initiation(1)
                            .build(),
                    )
                    .build()
                    .unwrap(),
            ],
            "InvalidRequest",
        ),
        (
            vec![
                rule("r")
                    .filter(prefix(""))
                    .status(ExpirationStatus::from("enabled"))
                    .expiration(expire_after(1))
                    .build()
                    .unwrap(),
            ],
            "MalformedXML",
        ),
    ];
    for (rules, code) in cases {
        let err = put_rules(c, rules.clone()).await.unwrap_err();
        assert_eq!(err.code(), Some(code), "{rules:?}");
    }
    let err = c
        .get_bucket_lifecycle_configuration()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("NoSuchLifecycleConfiguration"));
}

#[tokio::test]
async fn rules_s3dal_does_not_apply_are_refused_and_never_stored() {
    // A tag filter ignored would expire objects the tag was meant to spare; a transition
    // ignored would leave a client believing its data moved.
    let server = server().await;
    let c = &server.client;
    set_rules(
        c,
        vec![
            rule("kept")
                .filter(prefix("a/"))
                .expiration(expire_after(1))
                .build()
                .unwrap(),
        ],
    )
    .await;
    let tag = Tag::builder().key("k").value("v").build().unwrap();
    let unsupported = [
        rule("tags")
            .filter(LifecycleRuleFilter::builder().tag(tag.clone()).build())
            .expiration(expire_after(1))
            .build()
            .unwrap(),
        rule("and-tags")
            .filter(
                LifecycleRuleFilter::builder()
                    .and(
                        LifecycleRuleAndOperator::builder()
                            .prefix("a/")
                            .tags(tag)
                            .build(),
                    )
                    .build(),
            )
            .expiration(expire_after(1))
            .build()
            .unwrap(),
        rule("transition")
            .filter(prefix(""))
            .transitions(
                Transition::builder()
                    .days(30)
                    .storage_class(TransitionStorageClass::Glacier)
                    .build(),
            )
            .build()
            .unwrap(),
    ];
    for r in unsupported {
        let err = put_rules(c, vec![r.clone()]).await.unwrap_err();
        assert_eq!(err.code(), Some("NotImplemented"), "{r:?}");
    }
    let got = c
        .get_bucket_lifecycle_configuration()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    assert_eq!(got.rules().len(), 1);
    assert_eq!(got.rules()[0].id(), Some("kept"));
}

#[tokio::test]
async fn an_object_expires_on_the_first_midnight_after_its_days() {
    // AWS rounds up to midnight UTC, so a day's objects go together, and never before a
    // full day has passed.
    let server = server().await;
    let c = &server.client;
    put(c, "tmp/a", 3).await;
    put(c, "keep/a", 3).await;
    set_rules(
        c,
        vec![
            rule("tmp")
                .filter(prefix("tmp/"))
                .expiration(expire_after(1))
                .build()
                .unwrap(),
        ],
    )
    .await;
    let mut events = server.dal.subscribe();
    let at = due(created(c, "tmp/a").await, 1);

    let before = server
        .dal
        .apply_lifecycle(at - Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(before, Expired::default());
    assert_eq!(keys(c).await, ["keep/a", "tmp/a"]);

    let on = server.dal.apply_lifecycle(at).await.unwrap();
    assert_eq!(on, expired(1, 0, 0, 0));
    assert_eq!(keys(c).await, ["keep/a"]);
    assert_eq!(
        drain(&mut events),
        [(EventKind::LifecycleDelete, "tmp/a".to_owned())]
    );
    let metrics = server.dal.metrics().render();
    assert!(
        metrics.contains("s3dal_lifecycle_expired_total{kind=\"current\"} 1\n"),
        "{metrics}"
    );
}

#[tokio::test]
async fn a_date_rule_expires_what_it_covers_from_that_day() {
    let server = server().await;
    let c = &server.client;
    put(c, "past/a", 1).await;
    put(c, "future/a", 1).await;
    let date = |secs| {
        LifecycleExpiration::builder()
            .date(DateTime::from_secs(secs))
            .build()
    };
    set_rules(
        c,
        vec![
            rule("past")
                .filter(prefix("past/"))
                .expiration(date(1_420_070_400)) // 2015-01-01
                .build()
                .unwrap(),
            rule("future")
                .filter(prefix("future/"))
                .expiration(date(1_893_456_000)) // 2030-01-01
                .build()
                .unwrap(),
        ],
    )
    .await;
    let now = SystemTime::now();
    assert_eq!(
        server.dal.apply_lifecycle(now).await.unwrap(),
        expired(1, 0, 0, 0)
    );
    assert_eq!(keys(c).await, ["future/a"]);
}

#[tokio::test]
async fn size_filters_choose_what_expires() {
    // ObjectSizeGreaterThan and ObjectSizeLessThan compare strictly, as on AWS.
    let server = server().await;
    let c = &server.client;
    for (key, len) in [("s/10", 10), ("s/11", 11), ("s/19", 19), ("s/20", 20)] {
        put(c, key, len).await;
    }
    set_rules(
        c,
        vec![
            rule("mid")
                .filter(
                    LifecycleRuleFilter::builder()
                        .and(
                            LifecycleRuleAndOperator::builder()
                                .prefix("s/")
                                .object_size_greater_than(10)
                                .object_size_less_than(20)
                                .build(),
                        )
                        .build(),
                )
                .expiration(expire_after(1))
                .build()
                .unwrap(),
        ],
    )
    .await;
    let later = SystemTime::now() + Duration::from_secs(3 * DAY);
    assert_eq!(
        server.dal.apply_lifecycle(later).await.unwrap(),
        expired(2, 0, 0, 0)
    );
    assert_eq!(keys(c).await, ["s/10", "s/20"]);
}

#[tokio::test]
async fn expiry_in_a_versioned_bucket_keeps_the_version_behind_a_marker() {
    // As on AWS: an expired current version is only hidden, so it can be restored, and it is
    // removed for good only by a noncurrent rule.
    let server = server().await;
    let c = &server.client;
    enable_versioning(c).await;
    put(c, "k", 4).await;
    set_rules(
        c,
        vec![
            rule("r")
                .filter(prefix(""))
                .expiration(expire_after(1))
                .build()
                .unwrap(),
        ],
    )
    .await;
    let mut events = server.dal.subscribe();
    let later = SystemTime::now() + Duration::from_secs(3 * DAY);
    assert_eq!(
        server.dal.apply_lifecycle(later).await.unwrap(),
        expired(1, 0, 0, 0)
    );
    assert_eq!(
        versions(c).await,
        (vec!["k".to_owned()], vec!["k*".to_owned()])
    );
    assert_eq!(
        drain(&mut events),
        [(EventKind::LifecycleDeleteMarker, "k".to_owned())]
    );
    // The marker it put is fresh: neither it nor the version under it goes by Days again.
    assert_eq!(
        server.dal.apply_lifecycle(later).await.unwrap(),
        Expired::default()
    );
}

#[tokio::test]
async fn noncurrent_versions_go_their_days_after_they_were_replaced() {
    // NoncurrentDays count from when a version stopped being current, not from when it was
    // written: a version written long ago and replaced today must stay its full days. The old
    // write time is set on the file, so this runs on fs.
    let server = TestServer::start_fs().await;
    server.create_bucket_dir(BUCKET).await;
    let c = &server.client;
    enable_versioning(c).await;
    put(c, "k", 1).await;
    let long_ago = SystemTime::now() - Duration::from_secs(30 * DAY);
    std::fs::File::options()
        .write(true)
        .open(server.fs_path(&format!("{BUCKET}/k")))
        .unwrap()
        .set_modified(long_ago)
        .unwrap();
    put(c, "k", 2).await;
    put(c, "k", 3).await;
    put(c, "k", 4).await;
    let replaced = SystemTime::now();
    set_rules(
        c,
        vec![
            rule("r")
                .filter(prefix(""))
                .noncurrent_version_expiration(noncurrent_after(2, Some(1)))
                .build()
                .unwrap(),
        ],
    )
    .await;
    let mut events = server.dal.subscribe();
    let at = due(replaced, 2);
    assert_eq!(
        server
            .dal
            .apply_lifecycle(at - Duration::from_secs(1))
            .await
            .unwrap(),
        Expired::default(),
        "the oldest version was written 30 days ago but replaced just now"
    );
    // The newest noncurrent version stays, as NewerNoncurrentVersions asks; the two below it go.
    assert_eq!(
        server.dal.apply_lifecycle(at).await.unwrap(),
        expired(0, 2, 0, 0)
    );
    assert_eq!(versions(c).await.0, ["k*", "k"]);
    assert_eq!(
        drain(&mut events),
        [
            (EventKind::LifecycleDelete, "k".to_owned()),
            (EventKind::LifecycleDelete, "k".to_owned())
        ]
    );
}

#[tokio::test]
async fn a_lone_delete_marker_goes_once_nothing_is_under_it() {
    // A marker over no version hides nothing and only slows listings: AWS's "expired object
    // delete marker". One with a version under it still hides that version and stays.
    let server = server().await;
    let c = &server.client;
    enable_versioning(c).await;
    for key in ["gone/a", "kept/a"] {
        put(c, key, 1).await;
        c.delete_object()
            .bucket(BUCKET)
            .key(key)
            .send()
            .await
            .unwrap();
    }
    set_rules(
        c,
        vec![
            rule("markers")
                .filter(prefix(""))
                .expiration(
                    LifecycleExpiration::builder()
                        .expired_object_delete_marker(true)
                        .build(),
                )
                .build()
                .unwrap(),
            rule("history")
                .filter(prefix("gone/"))
                .noncurrent_version_expiration(noncurrent_after(1, None))
                .build()
                .unwrap(),
        ],
    )
    .await;
    let later = SystemTime::now() + Duration::from_secs(3 * DAY);
    // The version under gone/a goes, which leaves its marker alone, which then goes too.
    assert_eq!(
        server.dal.apply_lifecycle(later).await.unwrap(),
        expired(0, 1, 1, 0)
    );
    assert_eq!(
        versions(c).await,
        (vec!["kept/a".to_owned()], vec!["kept/a*".to_owned()])
    );
}

#[tokio::test]
async fn expiration_days_also_remove_a_lone_marker_once_it_is_that_old() {
    // s3-tests' test_lifecycle_deletemarker_expiration_with_days_tag: with Days, a marker
    // left alone goes once it is as old as the rule says, not at once.
    let server = server().await;
    let c = &server.client;
    enable_versioning(c).await;
    put(c, "k", 1).await;
    c.delete_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    let deleted = SystemTime::now();
    set_rules(
        c,
        vec![
            rule("r")
                .filter(prefix(""))
                .expiration(expire_after(5))
                .noncurrent_version_expiration(noncurrent_after(1, None))
                .build()
                .unwrap(),
        ],
    )
    .await;
    let after_two_days = deleted + Duration::from_secs(2 * DAY + 1);
    assert_eq!(
        server.dal.apply_lifecycle(after_two_days).await.unwrap(),
        expired(0, 1, 0, 0)
    );
    assert_eq!(versions(c).await, (vec![], vec!["k*".to_owned()]));
    assert_eq!(
        server.dal.apply_lifecycle(due(deleted, 5)).await.unwrap(),
        expired(0, 0, 1, 0)
    );
    assert_eq!(versions(c).await, (vec![], vec![]));
}

#[tokio::test]
async fn an_abort_rule_replaces_the_default_age_for_the_uploads_it_covers() {
    // --stale-upload-age stays the default for uploads no rule covers; one a rule covers lives
    // as long as the rule says, longer or shorter.
    let server = server().await;
    let c = &server.client;
    let start = |key: &'static str| async move {
        c.create_multipart_upload()
            .bucket(BUCKET)
            .key(key)
            .send()
            .await
            .unwrap();
    };
    start("ruled/a").await;
    start("other/a").await;
    let started = SystemTime::now();
    set_rules(
        c,
        vec![
            rule("abort")
                .filter(prefix("ruled/"))
                .abort_incomplete_multipart_upload(
                    AbortIncompleteMultipartUpload::builder()
                        .days_after_initiation(2)
                        .build(),
                )
                .build()
                .unwrap(),
        ],
    )
    .await;
    let uploads = || async {
        let out = c
            .list_multipart_uploads()
            .bucket(BUCKET)
            .send()
            .await
            .unwrap();
        out.uploads()
            .iter()
            .map(|u| u.key().unwrap().to_owned())
            .collect::<Vec<_>>()
    };
    // A sweep with any age leaves the ruled upload to its rule.
    server
        .dal
        .sweep_stale_uploads(Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(uploads().await, ["ruled/a"]);
    assert_eq!(
        server
            .dal
            .apply_lifecycle(due(started, 2) - Duration::from_secs(1))
            .await
            .unwrap(),
        Expired::default()
    );
    assert_eq!(
        server.dal.apply_lifecycle(due(started, 2)).await.unwrap(),
        expired(0, 0, 0, 1)
    );
    assert!(uploads().await.is_empty());
}

#[tokio::test]
async fn the_expiration_header_names_the_rule_and_the_day() {
    // Clients show it as "expires on"; it must agree with when the job removes the object.
    let server = server().await;
    let c = &server.client;
    enable_versioning(c).await;
    set_rules(
        c,
        vec![
            rule("tmp \"rule\"")
                .filter(prefix("tmp/"))
                .expiration(expire_after(3))
                .build()
                .unwrap(),
        ],
    )
    .await;
    let first = put(c, "tmp/a", 2).await.unwrap();
    let put_out = c
        .put_object()
        .bucket(BUCKET)
        .key("tmp/a")
        .body(ByteStream::from_static(b"new"))
        .send()
        .await
        .unwrap();
    let at = due(created(c, "tmp/a").await, 3);
    let date = DateTime::from(at)
        .fmt(aws_sdk_s3::primitives::DateTimeFormat::HttpDate)
        .unwrap();
    let want = format!("expiry-date=\"{date}\", rule-id=\"tmp%20%22rule%22\"");
    assert_eq!(put_out.expiration(), Some(want.as_str()));
    let head = c
        .head_object()
        .bucket(BUCKET)
        .key("tmp/a")
        .send()
        .await
        .unwrap();
    assert_eq!(head.expiration(), Some(want.as_str()));
    let get = c
        .get_object()
        .bucket(BUCKET)
        .key("tmp/a")
        .send()
        .await
        .unwrap();
    assert_eq!(get.expiration(), Some(want.as_str()));
    // A noncurrent version is not what Expiration expires, and a key no rule covers has none.
    let old = c
        .head_object()
        .bucket(BUCKET)
        .key("tmp/a")
        .version_id(first)
        .send()
        .await
        .unwrap();
    assert_eq!(old.expiration(), None);
    put(c, "other", 1).await;
    let other = c
        .head_object()
        .bucket(BUCKET)
        .key("other")
        .send()
        .await
        .unwrap();
    assert_eq!(other.expiration(), None);
}

#[tokio::test]
async fn expiry_frees_quota_usage() {
    // The quota is counted from the events; an expiry that did not announce what it removed
    // would leave the bucket full of objects that are gone.
    let server = server().await;
    let c = &server.client;
    let quota = Quota {
        max_bytes: 1 << 30,
        max_objects: None,
        unknown: Default::default(),
    };
    server
        .dal
        .set_bucket_quota(BUCKET, Some(quota))
        .await
        .unwrap();
    put(c, "a", 100).await;
    put(c, "b", 50).await;
    set_rules(
        c,
        vec![
            rule("a")
                .filter(prefix("a"))
                .expiration(expire_after(1))
                .build()
                .unwrap(),
        ],
    )
    .await;
    let used = server.dal.bucket_usage(BUCKET).await.unwrap();
    assert_eq!((used.objects, used.bytes), (2, 150));
    let later = SystemTime::now() + Duration::from_secs(3 * DAY);
    server.dal.apply_lifecycle(later).await.unwrap();
    let used = server.dal.bucket_usage(BUCKET).await.unwrap();
    assert_eq!((used.objects, used.bytes), (1, 50));
}

#[tokio::test]
async fn the_job_applies_a_bucket_once_a_day_unless_its_rules_change() {
    // A full listing of every bucket on each hourly look would cost far more than a day's
    // granularity needs; a rule just put must still not wait for the next day.
    let server = server().await;
    let c = &server.client;
    let rules = |days| {
        vec![
            rule("r")
                .filter(prefix(""))
                .expiration(expire_after(days))
                .build()
                .unwrap(),
        ]
    };
    set_rules(c, rules(1)).await;
    put(c, "a", 1).await;
    let day3 = SystemTime::now() + Duration::from_secs(3 * DAY);
    assert_eq!(
        server.dal.apply_due_lifecycle(day3).await.unwrap(),
        expired(1, 0, 0, 0)
    );
    put(c, "b", 1).await;
    assert_eq!(
        server.dal.apply_due_lifecycle(day3).await.unwrap(),
        Expired::default(),
        "applied already today"
    );
    set_rules(c, rules(2)).await;
    assert_eq!(
        server.dal.apply_due_lifecycle(day3).await.unwrap(),
        expired(1, 0, 0, 0),
        "the rules changed"
    );
    put(c, "c", 1).await;
    assert_eq!(
        server
            .dal
            .apply_due_lifecycle(day3 + Duration::from_secs(DAY))
            .await
            .unwrap(),
        expired(1, 0, 0, 0),
        "the next day"
    );
}

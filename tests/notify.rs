//! Bucket event notifications: a bucket's notification configuration names operator-configured
//! webhooks, and every object event a rule matches reaches its webhook as an AWS S3 event
//! message, with retries, bounded queues and counted losses. A local HTTP receiver (`Hook`)
//! stands for the webhook.

mod common;

use std::time::{Duration, Instant, SystemTime};

use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::operation::RequestId;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketLifecycleConfiguration, BucketVersioningStatus, CompletedMultipartUpload, CompletedPart,
    Event, ExpirationStatus, FilterRule, FilterRuleName, LambdaFunctionConfiguration,
    LifecycleExpiration, LifecycleRule, LifecycleRuleFilter, NotificationConfiguration,
    NotificationConfigurationFilter, QueueConfiguration, S3KeyFilter, TopicConfiguration,
    VersioningConfiguration,
};
use common::{ACCESS_KEY, Answer, Hook, TestServer};
use hmac::{Hmac, KeyInit, Mac};
use s3dal::Settings;
use s3dal::notify::Target;
use serde_json::Value;
use sha2::Sha256;

const BUCKET: &str = "bkt";

fn arn(id: &str) -> String {
    format!("arn:s3dal:sqs:us-east-1:{id}:webhook")
}

/// A server whose targets are `targets`, with bucket [`BUCKET`].
async fn server(targets: Vec<Target>) -> TestServer {
    let settings = Settings {
        notify: targets,
        ..Settings::default()
    };
    let server = TestServer::start_with(settings).await;
    server.create_bucket_dir(BUCKET).await;
    server
}

fn queue(id: &str, target: &str, events: &[&str]) -> QueueConfiguration {
    QueueConfiguration::builder()
        .id(id)
        .queue_arn(arn(target))
        .set_events(Some(events.iter().map(|e| Event::from(*e)).collect()))
        .build()
        .unwrap()
}

fn key_filter(prefix: Option<&str>, suffix: Option<&str>) -> NotificationConfigurationFilter {
    let rules = [
        (FilterRuleName::Prefix, prefix),
        (FilterRuleName::Suffix, suffix),
    ]
    .into_iter()
    .filter_map(|(name, value)| value.map(|v| FilterRule::builder().name(name).value(v).build()))
    .collect();
    NotificationConfigurationFilter::builder()
        .key(S3KeyFilter::builder().set_filter_rules(Some(rules)).build())
        .build()
}

async fn put_config(
    client: &Client,
    bucket: &str,
    config: NotificationConfiguration,
) -> Result<(), (String, String)> {
    client
        .put_bucket_notification_configuration()
        .bucket(bucket)
        .notification_configuration(config)
        .send()
        .await
        .map(drop)
        .map_err(|e| {
            let e = e.into_service_error();
            (
                e.code().unwrap_or_default().to_owned(),
                e.message().unwrap_or_default().to_owned(),
            )
        })
}

async fn notify_all(client: &Client, bucket: &str, target: &str) {
    let all = [
        "s3:ObjectCreated:*",
        "s3:ObjectRemoved:*",
        "s3:LifecycleExpiration:*",
    ];
    let config = NotificationConfiguration::builder()
        .queue_configurations(queue("all", target, &all))
        .build();
    put_config(client, bucket, config).await.unwrap();
}

async fn put(client: &Client, bucket: &str, key: &str, data: &[u8]) -> (String, String) {
    let out = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(data.to_vec()))
        .send()
        .await
        .unwrap();
    (
        out.e_tag().unwrap().trim_matches('"').to_owned(),
        out.request_id().unwrap().to_owned(),
    )
}

/// Waits until the server's metrics show `line`, a whole sample line.
async fn metric(server: &TestServer, line: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let text = server.dal.metrics().render();
        if text.lines().any(|l| l == line) {
            return;
        }
        assert!(Instant::now() < deadline, "no {line:?} in:\n{text}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn name_and_key(record: &Value) -> (String, String) {
    (
        record["eventName"].as_str().unwrap().to_owned(),
        record["s3"]["object"]["key"].as_str().unwrap().to_owned(),
    )
}

#[tokio::test]
async fn a_configuration_reads_back_as_put_and_names_only_configured_targets() {
    // Tools such as Terraform compare what they read back with what they put; and a rule
    // naming a target this server does not have would send to nowhere without a word, so it is
    // refused when put, as AWS refuses a destination it cannot validate.
    let hook = Hook::ok().await;
    let server = server(vec![Target::new("hooks", &hook.url)]).await;
    let c = &server.client;
    let config = NotificationConfiguration::builder()
        .queue_configurations(
            QueueConfiguration::builder()
                .id("images")
                .queue_arn(arn("hooks"))
                .events(Event::from("s3:ObjectCreated:Put"))
                .filter(key_filter(Some("images/"), Some(".jpg")))
                .build()
                .unwrap(),
        )
        .topic_configurations(
            TopicConfiguration::builder()
                .id("removed")
                .topic_arn(arn("hooks"))
                .events(Event::from("s3:ObjectRemoved:*"))
                .build()
                .unwrap(),
        )
        .lambda_function_configurations(
            LambdaFunctionConfiguration::builder()
                .id("expired")
                .lambda_function_arn(arn("hooks"))
                .events(Event::from("s3:LifecycleExpiration:*"))
                .build()
                .unwrap(),
        )
        .build();
    put_config(c, BUCKET, config).await.unwrap();
    let got = c
        .get_bucket_notification_configuration()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    let q = &got.queue_configurations()[0];
    assert_eq!(q.id(), Some("images"));
    assert_eq!(q.queue_arn(), arn("hooks"));
    assert_eq!(q.events(), [Event::from("s3:ObjectCreated:Put")]);
    let rules = q.filter().unwrap().key().unwrap().filter_rules();
    let rules: Vec<(String, String)> = rules
        .iter()
        .map(|r| {
            (
                r.name().unwrap().as_str().to_owned(),
                r.value().unwrap().to_owned(),
            )
        })
        .collect();
    assert_eq!(
        rules,
        [
            ("prefix".to_owned(), "images/".to_owned()),
            ("suffix".to_owned(), ".jpg".to_owned())
        ]
    );
    assert_eq!(got.topic_configurations()[0].id(), Some("removed"));
    assert_eq!(got.topic_configurations()[0].topic_arn(), arn("hooks"));
    assert_eq!(
        got.lambda_function_configurations()[0].id(),
        Some("expired")
    );

    let refused = |events: &[&str], target: &str| {
        NotificationConfiguration::builder()
            .queue_configurations(queue("x", target, events))
            .build()
    };
    let created = ["s3:ObjectCreated:*"];
    for (config, code) in [
        (refused(&created, "missing"), "InvalidArgument"),
        (
            refused(&["s3:ObjectCreated:Nope"], "hooks"),
            "InvalidArgument",
        ),
        (refused(&["s3:ObjectRestore:*"], "hooks"), "NotImplemented"),
    ] {
        let (got, message) = put_config(c, BUCKET, config).await.unwrap_err();
        assert_eq!(got, code, "{message}");
    }
    // Another region's ARN, or an AWS queue, names nothing here.
    for other in [
        "arn:s3dal:sqs:eu-west-1:hooks:webhook",
        "arn:aws:sqs:us-east-1:123456789012:hooks",
    ] {
        let config = NotificationConfiguration::builder()
            .queue_configurations(
                QueueConfiguration::builder()
                    .queue_arn(other)
                    .events(Event::from("s3:ObjectCreated:*"))
                    .build()
                    .unwrap(),
            )
            .build();
        let (code, message) = put_config(c, BUCKET, config).await.unwrap_err();
        assert_eq!(code, "InvalidArgument", "{other}");
        assert!(message.contains(other), "{message}");
    }
    let two_prefixes = NotificationConfiguration::builder()
        .queue_configurations(
            QueueConfiguration::builder()
                .queue_arn(arn("hooks"))
                .events(Event::from("s3:ObjectCreated:*"))
                .filter(
                    NotificationConfigurationFilter::builder()
                        .key(
                            S3KeyFilter::builder()
                                .filter_rules(
                                    FilterRule::builder()
                                        .name(FilterRuleName::Prefix)
                                        .value("a")
                                        .build(),
                                )
                                .filter_rules(
                                    FilterRule::builder()
                                        .name(FilterRuleName::Prefix)
                                        .value("b")
                                        .build(),
                                )
                                .build(),
                        )
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .build();
    let (code, _) = put_config(c, BUCKET, two_prefixes).await.unwrap_err();
    assert_eq!(code, "InvalidArgument");
    // A refused configuration changed nothing.
    let still = c
        .get_bucket_notification_configuration()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    assert_eq!(still.queue_configurations()[0].id(), Some("images"));

    // An empty configuration is how S3 removes one.
    put_config(c, BUCKET, NotificationConfiguration::builder().build())
        .await
        .unwrap();
    let gone = c
        .get_bucket_notification_configuration()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    assert!(gone.queue_configurations().is_empty(), "{gone:?}");
    assert!(gone.topic_configurations().is_empty(), "{gone:?}");
}

#[tokio::test]
async fn each_event_kind_reaches_the_target_once_with_the_fields_aws_documents() {
    // Consumers written for AWS (Lambda handlers, SQS workers) read these fields by name; the
    // key comes URL-encoded as AWS sends it, and the sequencer orders a key's events.
    let mut hook = Hook::ok().await;
    let server = server(vec![Target::new("t", &hook.url)]).await;
    let c = &server.client;
    notify_all(c, BUCKET, "t").await;

    let key = "dir/a b+c.txt";
    let (etag, request) = put(c, BUCKET, key, b"hello").await;
    let d = hook.next().await;
    assert_eq!(d.header("content-type"), Some("application/json"));
    assert_eq!(d.header("x-s3dal-signature"), None, "no hmac-secret");
    let r = d.record();
    assert_eq!(r["eventVersion"], "2.1");
    assert_eq!(r["eventSource"], "aws:s3");
    assert_eq!(r["awsRegion"], "us-east-1");
    assert_eq!(r["eventName"], "ObjectCreated:Put");
    let time = r["eventTime"].as_str().unwrap();
    assert!(
        time.len() == 24 && time.ends_with('Z') && time.as_bytes()[19] == b'.',
        "{time}"
    );
    assert_eq!(r["userIdentity"]["principalId"], ACCESS_KEY);
    assert_eq!(r["requestParameters"]["sourceIPAddress"], "127.0.0.1");
    assert_eq!(r["responseElements"]["x-amz-request-id"], request.as_str());
    let s3 = &r["s3"];
    assert_eq!(s3["s3SchemaVersion"], "1.0");
    assert_eq!(s3["configurationId"], "all");
    assert_eq!(s3["bucket"]["name"], BUCKET);
    assert_eq!(s3["bucket"]["arn"], format!("arn:aws:s3:::{BUCKET}"));
    assert!(s3["bucket"]["ownerIdentity"]["principalId"].is_string());
    assert_eq!(s3["object"]["key"], "dir/a+b%2Bc.txt");
    assert_eq!(s3["object"]["size"], 5);
    assert_eq!(s3["object"]["eTag"], etag.as_str());
    assert!(s3["object"].get("versionId").is_none(), "{r}");
    let mut sequencers = vec![s3["object"]["sequencer"].as_str().unwrap().to_owned()];

    c.copy_object()
        .bucket(BUCKET)
        .key("copy")
        .copy_source(format!("{BUCKET}/dir/a%20b%2Bc.txt"))
        .send()
        .await
        .unwrap();
    let id = c
        .create_multipart_upload()
        .bucket(BUCKET)
        .key("mp")
        .send()
        .await
        .unwrap()
        .upload_id()
        .unwrap()
        .to_owned();
    let part = c
        .upload_part()
        .bucket(BUCKET)
        .key("mp")
        .upload_id(&id)
        .part_number(1)
        .body(ByteStream::from_static(b"parts"))
        .send()
        .await
        .unwrap();
    c.complete_multipart_upload()
        .bucket(BUCKET)
        .key("mp")
        .upload_id(&id)
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
    c.delete_object()
        .bucket(BUCKET)
        .key("copy")
        .send()
        .await
        .unwrap();
    let mut seen = Vec::new();
    for _ in 0..3 {
        let r = hook.next().await.record();
        sequencers.push(r["s3"]["object"]["sequencer"].as_str().unwrap().to_owned());
        seen.push((name_and_key(&r), r["s3"]["object"].clone()));
    }
    assert_eq!(
        seen.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>(),
        [
            ("ObjectCreated:Copy".to_owned(), "copy".to_owned()),
            (
                "ObjectCreated:CompleteMultipartUpload".to_owned(),
                "mp".to_owned()
            ),
            ("ObjectRemoved:Delete".to_owned(), "copy".to_owned()),
        ]
    );
    assert_eq!(seen[1].1["size"], 5);
    // A removal carries no size and no ETag, as on AWS.
    assert!(seen[2].1.get("size").is_none() && seen[2].1.get("eTag").is_none());
    assert!(sequencers.windows(2).all(|w| w[0] < w[1]), "{sequencers:?}");

    // A versioned bucket: the version written, and the delete marker a delete creates.
    let versioned = "versioned";
    server.create_bucket_dir(versioned).await;
    c.put_bucket_versioning()
        .bucket(versioned)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    notify_all(c, versioned, "t").await;
    let written = c
        .put_object()
        .bucket(versioned)
        .key("v")
        .body(ByteStream::from_static(b"v1"))
        .send()
        .await
        .unwrap();
    let r = hook.next().await.record();
    assert_eq!(r["s3"]["bucket"]["name"], versioned);
    assert_eq!(
        r["s3"]["object"]["versionId"].as_str(),
        written.version_id()
    );
    let marker = c
        .delete_object()
        .bucket(versioned)
        .key("v")
        .send()
        .await
        .unwrap();
    let r = hook.next().await.record();
    assert_eq!(
        name_and_key(&r),
        (
            "ObjectRemoved:DeleteMarkerCreated".to_owned(),
            "v".to_owned()
        )
    );
    assert_eq!(r["s3"]["object"]["versionId"].as_str(), marker.version_id());
    put(c, versioned, "w", b"w").await;
    hook.next().await;

    // Lifecycle expiry: no request, so no key pair and no address; eventVersion 2.3 as AWS
    // sends lifecycle events.
    for bucket in [BUCKET, versioned] {
        c.put_bucket_lifecycle_configuration()
            .bucket(bucket)
            .lifecycle_configuration(
                BucketLifecycleConfiguration::builder()
                    .rules(
                        LifecycleRule::builder()
                            .id("expire")
                            .status(ExpirationStatus::Enabled)
                            .filter(LifecycleRuleFilter::builder().prefix("").build())
                            .expiration(LifecycleExpiration::builder().days(1).build())
                            .build()
                            .unwrap(),
                    )
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();
    }
    let later = SystemTime::now() + Duration::from_secs(3 * 24 * 60 * 60);
    server.dal.apply_lifecycle(later).await.unwrap();
    let mut expired = Vec::new();
    for _ in 0..3 {
        let r = hook.next().await.record();
        assert_eq!(r["eventVersion"], "2.3", "{r}");
        assert_eq!(r["userIdentity"]["principalId"], "s3dal");
        assert!(r["responseElements"].get("x-amz-request-id").is_none());
        let (name, key) = name_and_key(&r);
        expired.push((
            r["s3"]["bucket"]["name"].as_str().unwrap().to_owned(),
            name,
            key,
        ));
    }
    expired.sort();
    let expect = |b: &str, n: &str, k: &str| (b.to_owned(), n.to_owned(), k.to_owned());
    assert_eq!(
        expired,
        [
            expect(BUCKET, "LifecycleExpiration:Delete", "dir/a+b%2Bc.txt"),
            expect(BUCKET, "LifecycleExpiration:Delete", "mp"),
            expect(versioned, "LifecycleExpiration:DeleteMarkerCreated", "w"),
        ]
    );
    hook.none_within(Duration::from_millis(300)).await;
}

#[tokio::test]
async fn rules_select_events_by_name_prefix_and_suffix() {
    // A consumer subscribed to new JPEGs under images/ must not be woken by every other write,
    // and one subscribed to deletes not by writes.
    let mut images = Hook::ok().await;
    let mut deletes = Hook::ok().await;
    let server = server(vec![
        Target::new("images", &images.url),
        Target::new("deletes", &deletes.url),
    ])
    .await;
    let c = &server.client;
    let config = NotificationConfiguration::builder()
        .queue_configurations(
            QueueConfiguration::builder()
                .queue_arn(arn("images"))
                .events(Event::from("s3:ObjectCreated:Put"))
                .filter(key_filter(Some("images/"), Some(".jpg")))
                .build()
                .unwrap(),
        )
        .queue_configurations(queue("del", "deletes", &["s3:ObjectRemoved:*"]))
        .build();
    put_config(c, BUCKET, config).await.unwrap();

    for key in ["images/a.jpg", "images/a.png", "docs/a.jpg", "images.jpg"] {
        put(c, BUCKET, key, b"x").await;
    }
    // A copy is ObjectCreated:Copy, not Put.
    c.copy_object()
        .bucket(BUCKET)
        .key("images/copy.jpg")
        .copy_source(format!("{BUCKET}/images/a.jpg"))
        .send()
        .await
        .unwrap();
    c.delete_object()
        .bucket(BUCKET)
        .key("docs/a.jpg")
        .send()
        .await
        .unwrap();
    put(c, BUCKET, "images/z.jpg", b"x").await;

    // Each target's deliveries come in order, so the next after the first is the last write:
    // nothing between them was sent.
    for want in ["images/a.jpg", "images/z.jpg"] {
        let r = images.next().await.record();
        assert_eq!(
            name_and_key(&r),
            ("ObjectCreated:Put".to_owned(), want.to_owned())
        );
    }
    let r = deletes.next().await.record();
    assert_eq!(
        name_and_key(&r),
        ("ObjectRemoved:Delete".to_owned(), "docs/a.jpg".to_owned())
    );
    images.none_within(Duration::from_millis(300)).await;
    deletes.none_within(Duration::from_millis(1)).await;
}

#[tokio::test]
async fn a_signed_message_verifies_with_the_hmac_secret_and_carries_the_token() {
    // A receiver on a network others reach must be able to tell s3dal's messages from forged
    // ones: the signature is HMAC-SHA256 of the exact body, hex, as documented.
    let mut hook = Hook::ok().await;
    let target = Target {
        auth_token: Some("hook-token".into()),
        hmac_secret: Some("hook-secret".into()),
        ..Target::new("t", &hook.url)
    };
    let server = server(vec![target]).await;
    notify_all(&server.client, BUCKET, "t").await;
    put(&server.client, BUCKET, "k", b"x").await;
    let d = hook.next().await;
    assert_eq!(d.header("authorization"), Some("Bearer hook-token"));
    let mut mac = Hmac::<Sha256>::new_from_slice(b"hook-secret").unwrap();
    mac.update(&d.body);
    let hex: String = mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(
        d.header("x-s3dal-signature"),
        Some(format!("sha256={hex}").as_str())
    );
}

#[tokio::test]
async fn a_failing_target_is_retried_until_it_takes_the_message() {
    // A webhook restarting or overloaded answers 503 for a moment; the event must still arrive,
    // once, the same message.
    let mut hook = Hook::start(|n| Answer::Status(if n == 0 { 503 } else { 200 })).await;
    let server = server(vec![Target::new("t", &hook.url)]).await;
    notify_all(&server.client, BUCKET, "t").await;
    put(&server.client, BUCKET, "k", b"x").await;
    let first = hook.next().await;
    let started = Instant::now();
    let second = hook.next().await;
    assert_eq!(first.body, second.body);
    // The first pause is a second.
    assert!(
        started.elapsed() >= Duration::from_millis(900),
        "{:?}",
        started.elapsed()
    );
    metric(
        &server,
        "s3dal_notify_deliveries_total{target=\"t\",outcome=\"ok\"} 1",
    )
    .await;
    metric(
        &server,
        "s3dal_notify_deliveries_total{target=\"t\",outcome=\"failed\"} 1",
    )
    .await;
    hook.none_within(Duration::from_millis(1500)).await;
    let text = server.dal.metrics().render();
    for reason in ["overflow", "retries_exhausted", "refused"] {
        let line = format!("s3dal_notify_dropped_total{{target=\"t\",reason=\"{reason}\"}} 0");
        assert!(text.lines().any(|l| l == line), "{line} in:\n{text}");
    }
}

#[tokio::test]
async fn a_message_still_failing_after_retry_for_is_dropped_and_counted() {
    // Loss is allowed, but never silent: an operator must see which target lost what, and why.
    let mut failing = Hook::start(|_| Answer::Status(500)).await;
    let mut refusing = Hook::start(|_| Answer::Status(400)).await;
    let server = server(vec![
        Target {
            retry_for: Duration::from_millis(2500),
            ..Target::new("failing", &failing.url)
        },
        Target::new("refusing", &refusing.url),
    ])
    .await;
    let config = NotificationConfiguration::builder()
        .queue_configurations(queue("a", "failing", &["s3:ObjectCreated:*"]))
        .queue_configurations(queue("b", "refusing", &["s3:ObjectCreated:*"]))
        .build();
    put_config(&server.client, BUCKET, config).await.unwrap();
    put(&server.client, BUCKET, "k", b"x").await;
    // Tried at once and a second later; the next pause, two seconds, would end past retry-for.
    failing.next().await;
    failing.next().await;
    metric(
        &server,
        "s3dal_notify_dropped_total{target=\"failing\",reason=\"retries_exhausted\"} 1",
    )
    .await;
    metric(
        &server,
        "s3dal_notify_deliveries_total{target=\"failing\",outcome=\"failed\"} 2",
    )
    .await;
    // A 4xx would be answered again: tried once, dropped as refused.
    refusing.next().await;
    metric(
        &server,
        "s3dal_notify_dropped_total{target=\"refusing\",reason=\"refused\"} 1",
    )
    .await;
    failing.none_within(Duration::from_millis(2500)).await;
    refusing.none_within(Duration::from_millis(1)).await;
}

#[tokio::test]
async fn messages_that_waited_past_retry_for_are_dropped_untried() {
    // Behind a message the target holds, the others age in the queue. Trying each once more
    // would hold the queue for another timeout apiece, all to send what is past its time.
    let mut hook = Hook::start(|_| Answer::Stall).await;
    let target = Target {
        retry_for: Duration::from_secs(2),
        ..Target::new("t", &hook.url)
    };
    let server = server(vec![target]).await;
    notify_all(&server.client, BUCKET, "t").await;
    for i in 0..3 {
        put(&server.client, BUCKET, &format!("k{i}"), b"x").await;
    }
    hook.next().await;
    // The first try times out after 10 seconds; the two behind it are dropped right after.
    tokio::time::sleep(Duration::from_secs(9)).await;
    metric(
        &server,
        "s3dal_notify_dropped_total{target=\"t\",reason=\"retries_exhausted\"} 3",
    )
    .await;
    hook.none_within(Duration::from_millis(500)).await;
}

#[tokio::test]
async fn a_full_queue_drops_and_counts_what_does_not_fit() {
    // Memory stays bounded however long a target hangs; what did not fit is counted.
    let mut hook = Hook::start(|n| {
        if n == 0 {
            Answer::Stall
        } else {
            Answer::Status(200)
        }
    })
    .await;
    let target = Target {
        queue_size: 2,
        ..Target::new("t", &hook.url)
    };
    let server = server(vec![target]).await;
    notify_all(&server.client, BUCKET, "t").await;
    put(&server.client, BUCKET, "k0", b"x").await;
    // The target holds the first message, so the queue keeps the next two and drops the rest.
    hook.next().await;
    for i in 1..10 {
        put(&server.client, BUCKET, &format!("k{i}"), b"x").await;
    }
    metric(
        &server,
        "s3dal_notify_dropped_total{target=\"t\",reason=\"overflow\"} 7",
    )
    .await;
    metric(&server, "s3dal_notify_queue_depth{target=\"t\"} 2").await;
}

#[tokio::test]
async fn a_dead_target_slows_neither_requests_nor_other_targets() {
    // A webhook that accepts connections and never answers is the worst case: requests must not
    // wait for it, and another target must get its messages as if it were not there.
    let dead = Hook::start(|_| Answer::Stall).await;
    let mut live = Hook::ok().await;
    let server = server(vec![
        Target::new("dead", &dead.url),
        Target::new("live", &live.url),
    ])
    .await;
    let config = NotificationConfiguration::builder()
        .queue_configurations(queue("a", "dead", &["s3:ObjectCreated:*"]))
        .queue_configurations(queue("b", "live", &["s3:ObjectCreated:*"]))
        .build();
    put_config(&server.client, BUCKET, config).await.unwrap();
    let started = Instant::now();
    let mut slowest = Duration::ZERO;
    for i in 0..20 {
        let t = Instant::now();
        put(&server.client, BUCKET, &format!("k{i:02}"), b"x").await;
        slowest = slowest.max(t.elapsed());
    }
    let total = started.elapsed();
    for i in 0..20 {
        let r = live.next().await.record();
        assert_eq!(r["s3"]["object"]["key"], format!("k{i:02}"));
    }
    let delivered = started.elapsed();
    eprintln!(
        "20 PUTs with a dead target: {total:?}, slowest {slowest:?}; live target had all 20 after {delivered:?}"
    );
    // The dead target holds its first message for 10 seconds; nothing here waited for it.
    assert!(slowest < Duration::from_secs(2), "slowest PUT {slowest:?}");
    assert!(
        delivered < Duration::from_secs(8),
        "live target done after {delivered:?}"
    );
    metric(&server, "s3dal_notify_queue_depth{target=\"dead\"} 19").await;
}

#[tokio::test]
async fn targets_follow_a_reload() {
    // Targets are edited in the config file like key pairs, and apply without a restart: a new
    // one can be named at once, and one removed stops receiving.
    let mut hook = Hook::ok().await;
    let server = server(Vec::new()).await;
    let c = &server.client;
    let config = || {
        NotificationConfiguration::builder()
            .queue_configurations(queue("a", "t", &["s3:ObjectCreated:*"]))
            .build()
    };
    let (code, _) = put_config(c, BUCKET, config()).await.unwrap_err();
    assert_eq!(code, "InvalidArgument");

    server.dal.set_settings(Settings {
        notify: vec![Target::new("t", &hook.url)],
        ..Settings::default()
    });
    put_config(c, BUCKET, config()).await.unwrap();
    put(c, BUCKET, "after-add", b"x").await;
    assert_eq!(name_and_key(&hook.next().await.record()).1, "after-add");

    server.dal.set_settings(Settings::default());
    put(c, BUCKET, "after-remove", b"x").await;
    hook.none_within(Duration::from_millis(500)).await;
    let text = server.dal.metrics().render();
    assert!(!text.contains("target=\"t\""), "{text}");
    // The rule stays with the bucket; it sends again once the target is back.
    server.dal.set_settings(Settings {
        notify: vec![Target::new("t", &hook.url)],
        ..Settings::default()
    });
    put(c, BUCKET, "back", b"x").await;
    assert_eq!(name_and_key(&hook.next().await.record()).1, "back");

    // A target edited under the same id sends where it now says.
    let mut moved = Hook::ok().await;
    server.dal.set_settings(Settings {
        notify: vec![Target::new("t", &moved.url)],
        ..Settings::default()
    });
    put(c, BUCKET, "moved", b"x").await;
    assert_eq!(name_and_key(&moved.next().await.record()).1, "moved");
    hook.none_within(Duration::from_millis(500)).await;
}

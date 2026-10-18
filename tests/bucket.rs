//! Bucket operations (design.md "Buckets"), driven over HTTP by the AWS SDK.

mod common;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_sdk_s3::Client;
use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::list_buckets::ListBucketsOutput;
use aws_sdk_s3::types::{
    AccessControlPolicy, BucketCannedAcl, BucketLocationConstraint, Grant, Grantee, Owner, Payer,
    Permission, Type,
};
use common::{ACCESS_KEY, Backend, SECRET_KEY, TestServer, client};
use s3dal::{S3Dal, Settings, server};
use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use s3s::validation::NameValidation;
use tokio::net::TcpListener;

fn names(out: &ListBucketsOutput) -> Vec<&str> {
    out.buckets().iter().filter_map(|b| b.name()).collect()
}

/// HEAD responses carry no error body, so the status is all a client gets.
fn status<E>(err: &SdkError<E, HttpResponse>) -> u16 {
    err.raw_response()
        .expect("the request reached the server")
        .status()
        .as_u16()
}

async fn list(client: &Client) -> ListBucketsOutput {
    client.list_buckets().send().await.unwrap()
}

/// Routing that accepts any bucket name, as an embedder may configure through
/// `S3ServiceBuilder::set_validation`. s3dal's own checks are then the only guard.
struct AnyBucketName;

impl NameValidation for AnyBucketName {
    fn validate_bucket_name(&self, name: &str) -> bool {
        !name.is_empty()
    }
}

/// A server whose router lets every bucket name through to s3dal. The default router already
/// answers InvalidBucketName itself, which would hide whether s3dal validates names at all.
async fn start_with_permissive_routing() -> (Backend, Client) {
    let backend = Backend::from_env().await;
    let dal = S3Dal::new(backend.op.clone(), backend.profile, Settings::default());
    let mut builder = S3ServiceBuilder::new(dal);
    builder.set_auth(SimpleAuth::from_single(ACCESS_KEY, SECRET_KEY));
    builder.set_validation(AnyBucketName);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(server::serve(
        listener,
        builder.build(),
        None,
        std::future::pending(),
    ));
    (backend, client(&endpoint, ACCESS_KEY, SECRET_KEY))
}

#[tokio::test]
async fn created_bucket_answers_head_and_is_listed() {
    // SDK waiters probe a new bucket with HeadBucket; `aws s3 ls` and `rclone lsd` find it
    // through ListBuckets.
    let server = TestServer::start().await;
    let c = &server.client;
    c.create_bucket().bucket("alpha").send().await.unwrap();

    assert!(server.exists("alpha/").await);
    let head = c.head_bucket().bucket("alpha").send().await.unwrap();
    // SDKs pick the signing region for a bucket from this header.
    assert_eq!(head.bucket_region(), Some("us-east-1"));
    let out = list(c).await;
    assert_eq!(names(&out), ["alpha"]);
    assert!(out.buckets()[0].creation_date().is_some());
}

#[tokio::test]
async fn list_buckets_names_the_owner_the_acls_name() {
    // AWS always names the owner here, and scripts take their canonical ID from it
    // (`aws s3api list-buckets --query Owner.ID`) to write grants; it must be the ACLs' owner.
    let server = TestServer::start().await;
    let c = &server.client;
    c.create_bucket().bucket("owned").send().await.unwrap();
    let listed = list(c).await;
    let acl = c.get_bucket_acl().bucket("owned").send().await.unwrap();
    assert!(listed.owner().and_then(Owner::id).is_some(), "{listed:?}");
    assert_eq!(listed.owner(), acl.owner());
}

#[tokio::test]
async fn recreating_a_bucket_in_us_east_1_succeeds_and_keeps_it() {
    // AWS answers 200 here for legacy compatibility, and scripts written against it create
    // their bucket unconditionally on every run; the existing contents must survive.
    let server = TestServer::start().await;
    let c = &server.client;
    c.create_bucket().bucket("dup").send().await.unwrap();
    server.plant("dup", "keep/me", b"data").await;

    c.create_bucket().bucket("dup").send().await.unwrap();
    assert_eq!(server.read("dup/keep/me").await, b"data");
}

#[tokio::test]
async fn recreating_a_bucket_elsewhere_is_bucket_already_owned_by_you() {
    // Outside us-east-1 AWS reports the bucket as already the caller's, and clients such as
    // rclone carry on on this code; the existing contents must survive the second create.
    let server = TestServer::start_with(Settings {
        region: "eu-central-1".to_owned(),
        ..Settings::default()
    })
    .await;
    let c = &server.client;
    c.create_bucket().bucket("dup").send().await.unwrap();
    server.plant("dup", "keep/me", b"data").await;

    let err = c.create_bucket().bucket("dup").send().await.unwrap_err();
    assert_eq!(err.code(), Some("BucketAlreadyOwnedByYou"), "{err:?}");
    assert_eq!(server.read("dup/keep/me").await, b"data");
}

#[tokio::test]
async fn buckets_are_created_only_as_private_and_unlocked() {
    // A client that asked for a public or object-locked bucket and got a 200 would rely on a
    // sharing or retention s3dal never applies. Nothing is created when the request is refused.
    let server = TestServer::start().await;
    let c = &server.client;
    let public = c
        .create_bucket()
        .bucket("public")
        .acl(BucketCannedAcl::PublicRead)
        .send()
        .await
        .unwrap_err();
    assert_eq!(public.code(), Some("NotImplemented"), "{public:?}");
    let granted = c
        .create_bucket()
        .bucket("granted")
        .grant_read("uri=\"http://acs.amazonaws.com/groups/global/AllUsers\"")
        .send()
        .await
        .unwrap_err();
    assert_eq!(granted.code(), Some("NotImplemented"), "{granted:?}");
    let locked = c
        .create_bucket()
        .bucket("locked")
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap_err();
    assert_eq!(locked.code(), Some("NotImplemented"), "{locked:?}");
    assert!(names(&list(c).await).is_empty());

    c.create_bucket()
        .bucket("private")
        .acl(BucketCannedAcl::Private)
        .send()
        .await
        .unwrap();
    assert_eq!(names(&list(c).await), ["private"]);
}

#[tokio::test]
async fn invalid_bucket_name_is_rejected_by_s3dal_itself() {
    // An embedder may relax s3s's routing rules; a name that is not a valid bucket must
    // still never become a directory in the namespace root.
    let (backend, c) = start_with_permissive_routing().await;
    for bad in ["Upper_Case", "-dash-first"] {
        let err = c.create_bucket().bucket(bad).send().await.unwrap_err();
        assert_eq!(err.code(), Some("InvalidBucketName"), "{bad}: {err:?}");
        let created = backend.op.exists(&format!("{bad}/")).await.unwrap();
        assert!(!created, "{bad} was created");
    }
}

#[tokio::test]
async fn reserved_root_is_not_a_bucket_even_if_routing_allows_it() {
    // `.s3dal/` holds probe and temp data. If DeleteBucket could reach it, a client could
    // wipe in-flight uploads; HeadBucket must not reveal it either.
    let (backend, c) = start_with_permissive_routing().await;
    backend
        .op
        .write(".s3dal/keep", b"x".to_vec())
        .await
        .unwrap();

    let err = c.delete_bucket().bucket(".s3dal").send().await.unwrap_err();
    assert_eq!(err.code(), Some("NoSuchBucket"), "{err:?}");
    assert!(backend.op.exists(".s3dal/keep").await.unwrap());

    let err = c.head_bucket().bucket(".s3dal").send().await.unwrap_err();
    assert_eq!(status(&err), 404, "{err:?}");
    let err = c
        .get_bucket_location()
        .bucket(".s3dal")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("NoSuchBucket"), "{err:?}");
}

#[tokio::test]
async fn head_of_missing_bucket_is_not_found() {
    // HEAD carries no error body, so clients only see the 404 that NoSuchBucket maps to.
    let server = TestServer::start().await;
    let err = server
        .client
        .head_bucket()
        .bucket("missing")
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&err), 404, "{err:?}");
}

#[tokio::test]
async fn delete_of_missing_bucket_is_no_such_bucket() {
    let server = TestServer::start().await;
    let err = server
        .client
        .delete_bucket()
        .bucket("missing")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("NoSuchBucket"), "{err:?}");
}

#[tokio::test]
async fn bucket_with_a_nested_object_is_not_deleted() {
    // DeleteBucket must never destroy data. The only object sits two directories deep, so a
    // check of the top level alone would miss it.
    let server = TestServer::start().await;
    server.create_bucket_dir("full").await;
    server.plant("full", "a/b/object", b"data").await;

    let err = server
        .client
        .delete_bucket()
        .bucket("full")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("BucketNotEmpty"), "{err:?}");
    assert_eq!(server.read("full/a/b/object").await, b"data");
}

#[tokio::test]
async fn empty_directories_do_not_keep_a_bucket_alive() {
    // S3 has no directories. Ones left behind on fs after their objects were deleted, or
    // folder-marker objects on an object store, are invisible to listing, so a client that
    // emptied the bucket must be able to delete it.
    let server = TestServer::start().await;
    let c = &server.client;
    c.create_bucket().bucket("hollow").send().await.unwrap();
    server.op.create_dir("hollow/x/y/").await.unwrap();
    server.op.create_dir("hollow/z/").await.unwrap();

    c.delete_bucket().bucket("hollow").send().await.unwrap();

    assert!(!server.exists("hollow/").await);
    let err = c.head_bucket().bucket("hollow").send().await.unwrap_err();
    assert_eq!(status(&err), 404, "{err:?}");
    assert!(names(&list(c).await).is_empty());
}

#[tokio::test]
async fn bucket_planted_on_disk_is_listed() {
    // A directory created by other tools sharing the fs root is a bucket, as design.md's
    // namespace mapping promises.
    let server = TestServer::start().await;
    server.create_bucket_dir("planted").await;
    assert_eq!(names(&list(&server.client).await), ["planted"]);
    server
        .client
        .head_bucket()
        .bucket("planted")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn only_directories_with_valid_bucket_names_are_listed() {
    // The reserved root and stray entries must never show up as buckets: clients would try
    // to use them, and every request to them fails.
    let server = TestServer::start().await;
    server.op.write(".s3dal/keep", b"x".to_vec()).await.unwrap();
    for dir in ["ok-bucket", "Upper", "under_score", "ab"] {
        server.op.create_dir(&format!("{dir}/")).await.unwrap();
    }
    server.op.write("loose-file", b"x".to_vec()).await.unwrap();

    let out = list(&server.client).await;
    assert!(!names(&out).contains(&".s3dal"));
    assert_eq!(names(&out), ["ok-bucket"]);
}

#[tokio::test]
async fn buckets_are_listed_by_name() {
    // S3 returns buckets in name order, and pagination depends on it; readdir order is
    // arbitrary.
    let server = TestServer::start().await;
    for b in ["zeta", "alpha", "mid", "beta"] {
        server.create_bucket_dir(b).await;
    }
    assert_eq!(
        names(&list(&server.client).await),
        ["alpha", "beta", "mid", "zeta"]
    );
}

#[tokio::test]
async fn creation_date_is_the_directory_mtime() {
    // There is no bucket metadata store, so the directory itself is the only record of when
    // the bucket appeared. Only fs lets a test set a directory's mtime.
    let server = TestServer::start_fs().await;
    server.create_bucket_dir("dated").await;
    let mtime = UNIX_EPOCH + Duration::from_secs(1_000_000_000);
    std::fs::File::open(server.fs_path("dated"))
        .unwrap()
        .set_modified(mtime)
        .unwrap();

    let out = list(&server.client).await;
    let date = out.buckets()[0].creation_date().unwrap();
    assert_eq!(SystemTime::try_from(*date).unwrap(), mtime);
}

#[tokio::test]
async fn list_buckets_honors_prefix() {
    // Ignoring a filter the client asked for would silently return the wrong buckets.
    let server = TestServer::start().await;
    for b in ["app-one", "app-two", "web"] {
        server.create_bucket_dir(b).await;
    }
    let out = server
        .client
        .list_buckets()
        .prefix("app-")
        .send()
        .await
        .unwrap();
    assert_eq!(names(&out), ["app-one", "app-two"]);
    assert_eq!(out.prefix(), Some("app-"));
}

#[tokio::test]
async fn list_buckets_pages_with_max_buckets() {
    // SDK paginators follow the continuation token until it is absent; every bucket must
    // appear exactly once across pages.
    let server = TestServer::start().await;
    for b in ["bkt-1", "bkt-2", "bkt-3"] {
        server.create_bucket_dir(b).await;
    }
    let c = &server.client;
    let first = c.list_buckets().max_buckets(2).send().await.unwrap();
    assert_eq!(names(&first), ["bkt-1", "bkt-2"]);
    let token = first.continuation_token().expect("more buckets remain");

    let second = c
        .list_buckets()
        .max_buckets(2)
        .continuation_token(token)
        .send()
        .await
        .unwrap();
    assert_eq!(names(&second), ["bkt-3"]);
    assert_eq!(second.continuation_token(), None);
}

#[tokio::test]
async fn max_buckets_outside_aws_range_is_invalid_argument() {
    let server = TestServer::start().await;
    for n in [0, 10_001] {
        let err = server
            .client
            .list_buckets()
            .max_buckets(n)
            .send()
            .await
            .unwrap_err();
        assert_eq!(err.code(), Some("InvalidArgument"), "{n}: {err:?}");
    }
}

#[tokio::test]
async fn list_buckets_filters_by_region() {
    // All buckets live in the configured region; a filter for another region matches none.
    let server = TestServer::start().await;
    server.create_bucket_dir("here").await;
    let c = &server.client;
    let other = c
        .list_buckets()
        .bucket_region("eu-west-1")
        .send()
        .await
        .unwrap();
    assert!(names(&other).is_empty());
    let own = c
        .list_buckets()
        .bucket_region("us-east-1")
        .send()
        .await
        .unwrap();
    assert_eq!(names(&own), ["here"]);
    assert_eq!(own.buckets()[0].bucket_region(), Some("us-east-1"));
}

#[tokio::test]
async fn location_of_us_east_1_bucket_is_empty() {
    // S3 reports its original region as an empty LocationConstraint; SDKs and the aws CLI
    // map that back to us-east-1, and some reject a literal "us-east-1".
    let server = TestServer::start().await;
    server.create_bucket_dir("east").await;
    let out = server
        .client
        .get_bucket_location()
        .bucket("east")
        .send()
        .await
        .unwrap();
    // The element is present but empty, as from S3; this SDK parses that as "" rather than None.
    assert_eq!(out.location_constraint().map_or("", |c| c.as_str()), "");
}

#[tokio::test]
async fn location_is_the_configured_region() {
    // Clients use the location to choose the signing region for later requests.
    let server = TestServer::start_with(Settings {
        region: "eu-central-1".to_owned(),
        ..Settings::default()
    })
    .await;
    server.create_bucket_dir("frankfurt").await;
    let c = &server.client;
    let out = c
        .get_bucket_location()
        .bucket("frankfurt")
        .send()
        .await
        .unwrap();
    assert_eq!(
        out.location_constraint(),
        Some(&BucketLocationConstraint::EuCentral1)
    );
    let head = c.head_bucket().bucket("frankfurt").send().await.unwrap();
    assert_eq!(head.bucket_region(), Some("eu-central-1"));
}

#[tokio::test]
async fn location_of_missing_bucket_is_no_such_bucket() {
    let server = TestServer::start().await;
    let err = server
        .client
        .get_bucket_location()
        .bucket("missing")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("NoSuchBucket"), "{err:?}");
}

#[tokio::test]
async fn versioning_reads_as_never_enabled() {
    // rclone asks before purging; "never enabled" (no Status) tells it there are no old
    // versions to clean up, where a 501 makes it log an error.
    let server = TestServer::start().await;
    server.create_bucket_dir("bkt").await;
    let out = server
        .client
        .get_bucket_versioning()
        .bucket("bkt")
        .send()
        .await
        .unwrap();
    assert_eq!(out.status(), None);
    assert_eq!(out.mfa_delete(), None);
}

#[tokio::test]
async fn versioning_of_missing_bucket_is_no_such_bucket() {
    let server = TestServer::start().await;
    let err = server
        .client
        .get_bucket_versioning()
        .bucket("missing")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("NoSuchBucket"), "{err:?}");
}

/// The status and code of a configuration GET that must find nothing.
fn absent<T: std::fmt::Debug, E: ProvideErrorMetadata + std::fmt::Debug>(
    res: Result<T, SdkError<E, HttpResponse>>,
) -> (u16, String) {
    let err = res.expect_err("answered with a configuration s3dal does not have");
    (status(&err), err.code().unwrap_or_default().to_owned())
}

#[tokio::test]
async fn unset_bucket_configurations_read_as_absent() {
    // Terraform's aws_s3_bucket reads every one of these on each refresh; S3's answer for a
    // bucket without the configuration reads as "not set", where NotImplemented fails the
    // refresh. A 200 with a configuration would claim one s3dal does not apply.
    let server = TestServer::start().await;
    let c = &server.client;
    server.create_bucket_dir("cfg").await;
    let b = "cfg";
    let found = [
        absent(c.get_bucket_policy().bucket(b).send().await),
        absent(c.get_bucket_policy_status().bucket(b).send().await),
        absent(c.get_bucket_cors().bucket(b).send().await),
        absent(
            c.get_bucket_lifecycle_configuration()
                .bucket(b)
                .send()
                .await,
        ),
        absent(c.get_bucket_tagging().bucket(b).send().await),
        absent(c.get_bucket_encryption().bucket(b).send().await),
        absent(c.get_object_lock_configuration().bucket(b).send().await),
        absent(c.get_bucket_website().bucket(b).send().await),
        absent(c.get_bucket_replication().bucket(b).send().await),
        absent(c.get_bucket_ownership_controls().bucket(b).send().await),
        absent(c.get_public_access_block().bucket(b).send().await),
    ];
    let expected = [
        "NoSuchBucketPolicy",
        "NoSuchBucketPolicy",
        "NoSuchCORSConfiguration",
        "NoSuchLifecycleConfiguration",
        "NoSuchTagSet",
        "ServerSideEncryptionConfigurationNotFoundError",
        "ObjectLockConfigurationNotFoundError",
        "NoSuchWebsiteConfiguration",
        "ReplicationConfigurationNotFoundError",
        "OwnershipControlsNotFoundError",
        "NoSuchPublicAccessBlockConfiguration",
    ];
    assert_eq!(found, expected.map(|code| (404, code.to_owned())));

    // These S3 answers with an empty configuration rather than an error.
    let logging = c.get_bucket_logging().bucket(b).send().await.unwrap();
    assert_eq!(logging.logging_enabled(), None);
    let notification = c
        .get_bucket_notification_configuration()
        .bucket(b)
        .send()
        .await
        .unwrap();
    assert!(
        notification.topic_configurations().is_empty(),
        "{notification:?}"
    );
    assert!(
        notification.queue_configurations().is_empty(),
        "{notification:?}"
    );
    assert!(
        notification.lambda_function_configurations().is_empty(),
        "{notification:?}"
    );
    assert_eq!(notification.event_bridge_configuration(), None);
    let accelerate = c
        .get_bucket_accelerate_configuration()
        .bucket(b)
        .send()
        .await
        .unwrap();
    assert_eq!(accelerate.status(), None);
    let payment = c
        .get_bucket_request_payment()
        .bucket(b)
        .send()
        .await
        .unwrap();
    assert_eq!(payment.payer(), Some(&Payer::BucketOwner));
}

#[tokio::test]
async fn configurations_of_a_missing_bucket_are_no_such_bucket() {
    // "No policy" must not be said of a bucket that is not there.
    let server = TestServer::start().await;
    let c = &server.client;
    let policy = c.get_bucket_policy().bucket("missing").send().await;
    let logging = c.get_bucket_logging().bucket("missing").send().await;
    assert_eq!(absent(policy), (404, "NoSuchBucket".to_owned()));
    assert_eq!(absent(logging), (404, "NoSuchBucket".to_owned()));
}

#[tokio::test]
async fn bucket_acl_is_the_owners_full_control_alone() {
    // Tools read a bucket's ACL to show who can reach it, and some (s3cmd, Cyberduck) before
    // they work with it. Any other grant would claim access s3dal never gives.
    let server = TestServer::start().await;
    server.create_bucket_dir("bkt").await;
    let acl = server
        .client
        .get_bucket_acl()
        .bucket("bkt")
        .send()
        .await
        .unwrap();
    let owner = acl.owner().and_then(|o| o.id()).unwrap();
    assert!(!owner.is_empty());
    let grants = acl.grants();
    assert_eq!(grants.len(), 1, "{grants:?}");
    let grantee = grants[0].grantee().unwrap();
    assert_eq!(grantee.r#type(), &Type::CanonicalUser);
    assert_eq!(grantee.id(), Some(owner));
    assert_eq!(grants[0].permission(), Some(&Permission::FullControl));

    let missing = server
        .client
        .get_bucket_acl()
        .bucket("missing")
        .send()
        .await
        .unwrap_err();
    assert_eq!(missing.code(), Some("NoSuchBucket"), "{missing:?}");
}

#[tokio::test]
async fn bucket_acl_writes_succeed_only_when_they_grant_nobody_else() {
    // A write that would share the bucket must fail, since nothing here could enforce it; one
    // that restates the owner-only ACL succeeds, so tools that set `private` as a matter of
    // course keep working. The rule is the one PutObject applies to its ACL headers.
    let server = TestServer::start().await;
    server.create_bucket_dir("bkt").await;
    let put = || server.client.put_bucket_acl().bucket("bkt");
    put().acl(BucketCannedAcl::Private).send().await.unwrap();

    let owner_grant = Grant::builder()
        .grantee(
            Grantee::builder()
                .r#type(Type::CanonicalUser)
                .id("someone")
                .build()
                .unwrap(),
        )
        .permission(Permission::FullControl)
        .build();
    let policy = AccessControlPolicy::builder()
        .grants(owner_grant)
        .owner(Owner::builder().id("someone").build())
        .build();
    let refused = [
        (
            "public",
            put().acl(BucketCannedAcl::PublicRead).send().await,
        ),
        (
            "grant",
            put()
                .grant_read("uri=http://acs.amazonaws.com/groups/global/AllUsers")
                .send()
                .await,
        ),
        ("policy", put().access_control_policy(policy).send().await),
    ];
    for (name, result) in refused {
        let err = result.unwrap_err();
        assert_eq!(status(&err), 501, "{name}: {err:?}");
        assert_eq!(err.code(), Some("NotImplemented"), "{name}: {err:?}");
    }
    // A value that is no canned ACL at all is a malformed request, as on S3, not a feature
    // s3dal lacks; a client must not read it as one it could switch on elsewhere.
    let unknown = put()
        .acl(BucketCannedAcl::from("public-ready"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&unknown), 400, "{unknown:?}");
    assert_eq!(unknown.code(), Some("InvalidArgument"), "{unknown:?}");

    // S3 requires an ACL in some form; an empty write sets nothing.
    let empty = put().send().await.unwrap_err();
    assert_eq!(status(&empty), 400, "{empty:?}");
    assert_eq!(empty.code(), Some("MissingSecurityHeader"), "{empty:?}");
    let missing = server
        .client
        .put_bucket_acl()
        .bucket("missing")
        .acl(BucketCannedAcl::Private)
        .send()
        .await
        .unwrap_err();
    assert_eq!(missing.code(), Some("NoSuchBucket"), "{missing:?}");
}

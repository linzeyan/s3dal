mod common;

use std::collections::HashMap;

use aws_sdk_s3::Client;
use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::copy_object::builders::CopyObjectFluentBuilder;
use aws_sdk_s3::operation::upload_part_copy::builders::UploadPartCopyFluentBuilder;
use aws_sdk_s3::primitives::{ByteStream, DateTime};
use aws_sdk_s3::types::{
    ChecksumAlgorithm, CompletedMultipartUpload, CompletedPart, MetadataDirective, ObjectCannedAcl,
    ObjectLockMode, ServerSideEncryption, StorageClass, TaggingDirective,
};
use base64_simd::STANDARD;
use common::{Backend, TestServer};
use md5::{Digest, Md5};
use s3dal::Settings;
use s3dal::meta::{self, Attrs, Computed, ObjectMeta, PartLayout, md5_hex};

const BUCKET: &str = "bkt";
const OTHER: &str = "other";

async fn server() -> TestServer {
    server_with(Settings::default()).await
}

/// A server that buffers at most `limit` bytes for an MD5 ETag, so the large-copy path can be
/// exercised with small objects.
async fn server_with_buffer_limit(limit: u64) -> TestServer {
    server_with(Settings {
        etag_buffer_limit: limit,
        ..Default::default()
    })
    .await
}

async fn server_with(settings: Settings) -> TestServer {
    with_buckets(TestServer::start_with(settings).await).await
}

/// A server on fs whatever the suite's backend, for the gateway ETag path, which is only taken
/// without native ETags: synthesized ETags, and stored digests that can go stale.
async fn fs_server_with(settings: Settings) -> TestServer {
    with_buckets(TestServer::start_on(Backend::fs().await, settings).await).await
}

async fn with_buckets(server: TestServer) -> TestServer {
    server.create_bucket_dir(BUCKET).await;
    server.create_bucket_dir(OTHER).await;
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

async fn get(server: &TestServer, bucket: &str, key: &str) -> Vec<u8> {
    let out = server
        .client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    out.body.collect().await.unwrap().into_bytes().to_vec()
}

async fn head_etag(server: &TestServer, bucket: &str, key: &str) -> String {
    let out = server
        .client
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    out.e_tag().unwrap().to_owned()
}

/// A copy of `BUCKET/src_key` to `bucket/key`.
fn copy(server: &TestServer, src_key: &str, bucket: &str, key: &str) -> CopyObjectFluentBuilder {
    server
        .client
        .copy_object()
        .copy_source(format!("{BUCKET}/{src_key}"))
        .bucket(bucket)
        .key(key)
}

/// What s3dal has stored for `bucket/key`.
async fn stored(server: &TestServer, bucket: &str, key: &str) -> ObjectMeta {
    let m = server.op.stat(&format!("{bucket}/{key}")).await.unwrap();
    meta::resolve(&server.profile, &m)
}

/// Writes `data` with the metadata s3dal would store for `computed`, without verifying that
/// `computed` describes `data`. This stands in for an object whose recorded digests went stale,
/// or for one written by CompleteMultipartUpload.
async fn plant_with(server: &TestServer, key: &str, data: &[u8], computed: Computed) {
    let opts = meta::write_options(&server.profile, Attrs::default(), computed);
    server
        .op
        .write_options(&format!("{BUCKET}/{key}"), data.to_vec(), opts)
        .await
        .unwrap();
}

/// A client that does not retry, for failures the SDK treats as transient.
fn no_retry_client(server: &TestServer) -> Client {
    Client::from_conf(
        server
            .client
            .config()
            .to_builder()
            .retry_config(RetryConfig::disabled())
            .build(),
    )
}

fn status<E>(err: &SdkError<E, HttpResponse>) -> u16 {
    err.raw_response().unwrap().status().as_u16()
}

fn quoted(etag: &str) -> String {
    format!("\"{etag}\"")
}

fn patterned(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

async fn exists(server: &TestServer, bucket: &str, key: &str) -> bool {
    server.exists(&format!("{bucket}/{key}")).await
}

#[tokio::test]
async fn copy_within_and_across_buckets_is_byte_for_byte() {
    // The copy is a new write fed from a read of the source, so every byte passes through
    // s3dal; 3 MiB spans several of OpenDAL's read chunks.
    let server = server().await;
    let data = patterned(3 * 1024 * 1024 + 7);
    let etag = put(&server, "src", &data).await;

    for (bucket, key) in [(BUCKET, "copy"), (OTHER, "nested/copy")] {
        let out = copy(&server, "src", bucket, key).send().await.unwrap();
        let result = out.copy_object_result().unwrap();
        assert_eq!(result.e_tag(), Some(etag.as_str()));
        assert_eq!(get(&server, bucket, key).await, data);

        // Clients record the copy's ETag and Last-Modified from this response; they must match
        // what the object reports afterwards.
        let head = server
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(head.e_tag(), Some(etag.as_str()));
        assert_eq!(
            result.last_modified().unwrap().secs(),
            head.last_modified().unwrap().secs()
        );
    }
    assert_eq!(get(&server, BUCKET, "src").await, data);
}

#[tokio::test]
async fn md5_source_keeps_its_etag_even_above_the_buffer_limit() {
    // A content-MD5 ETag is known before the copy starts, so it is stored without buffering;
    // tools that compare ETags with local MD5s then accept the copy.
    let server = server_with_buffer_limit(16).await;
    let data = patterned(64);
    let out = server
        .client
        .put_object()
        .bucket(BUCKET)
        .key("src")
        .content_md5(STANDARD.encode_to_string(Md5::digest(&data)))
        .body(ByteStream::from(data.clone()))
        .send()
        .await
        .unwrap();
    let etag = out.e_tag().unwrap().to_owned();
    assert_eq!(etag, quoted(&md5_hex(&data)));

    let out = copy(&server, "src", OTHER, "copy").send().await.unwrap();
    assert_eq!(
        out.copy_object_result().unwrap().e_tag(),
        Some(etag.as_str())
    );
    assert_eq!(head_etag(&server, OTHER, "copy").await, etag);
}

#[tokio::test]
async fn small_source_without_an_md5_gets_one() {
    // Objects written by other tools have synthesized ETags. A small copy is buffered and
    // hashed, so the copy gets a real MD5 like a small PUT does.
    let server = fs_server_with(Settings::default()).await;
    let data = patterned(100);
    server.plant(BUCKET, "planted", &data).await;
    let source_etag = head_etag(&server, BUCKET, "planted").await;
    assert!(source_etag.contains('-'), "{source_etag}");

    let out = copy(&server, "planted", BUCKET, "copy")
        .send()
        .await
        .unwrap();
    let md5 = quoted(&md5_hex(&data));
    assert_eq!(
        out.copy_object_result().unwrap().e_tag(),
        Some(md5.as_str())
    );
    assert_eq!(head_etag(&server, BUCKET, "copy").await, md5);
}

#[tokio::test]
async fn large_copy_without_an_md5_gets_a_synthesized_etag() {
    // Above the limit the content cannot be hashed before the write starts, so the ETag is
    // synthesized. It contains '-' so no client mistakes it for an MD5, and the copy response
    // must report exactly what later reads report.
    let server = fs_server_with(Settings {
        etag_buffer_limit: 1024,
        ..Default::default()
    })
    .await;
    let data = patterned(3 * 1024 * 1024 + 7);
    server.plant(BUCKET, "planted", &data).await;

    let out = copy(&server, "planted", OTHER, "copy")
        .send()
        .await
        .unwrap();
    let etag = out
        .copy_object_result()
        .unwrap()
        .e_tag()
        .unwrap()
        .to_owned();
    assert!(etag.contains('-'), "{etag}");
    assert_eq!(head_etag(&server, OTHER, "copy").await, etag);
    assert_eq!(get(&server, OTHER, "copy").await, data);
}

#[tokio::test]
async fn multipart_source_copies_as_a_single_part() {
    // A copy is one part: carrying the source's `-N` ETag or part layout over would make
    // partNumber reads of the copy serve ranges it was never assembled from.
    let server = server().await;
    let data = patterned(12);
    plant_with(
        &server,
        "mpu",
        &data,
        Computed {
            md5_hex: Some(format!("{}-2", md5_hex(b"parts"))),
            parts: Some(PartLayout::new(&[6, 6])),
            ..Default::default()
        },
    )
    .await;
    assert!(stored(&server, BUCKET, "mpu").await.parts.is_some());

    let out = copy(&server, "mpu", BUCKET, "copy").send().await.unwrap();
    let md5 = quoted(&md5_hex(&data));
    assert_eq!(
        out.copy_object_result().unwrap().e_tag(),
        Some(md5.as_str())
    );
    let copied = stored(&server, BUCKET, "copy").await;
    assert_eq!(copied.parts, None);
    assert_eq!(get(&server, BUCKET, "copy").await, data);
}

#[tokio::test]
async fn stored_checksums_are_copied() {
    // SDKs validate GET responses against stored checksums; a copy that dropped them would
    // silently lose that protection.
    let server = server().await;
    let data = patterned(64);
    put(&server, "src", &data).await;
    let source = stored(&server, BUCKET, "src").await;
    assert!(
        source.checksum.checksum_crc32.is_some(),
        "the SDK's default CRC32 should have been stored with a buffered PUT"
    );

    // The source has an MD5 ETag, so the copy streams: the checksum is carried over from the
    // source's metadata, and verified on the way.
    let out = copy(&server, "src", OTHER, "copy").send().await.unwrap();
    let copied = stored(&server, OTHER, "copy").await;
    assert!(copied.checksum == source.checksum);
    assert_eq!(
        out.copy_object_result().unwrap().checksum_crc32(),
        source.checksum.checksum_crc32.as_deref()
    );
}

#[tokio::test]
async fn a_copy_that_asks_for_an_algorithm_keeps_only_that_checksum() {
    // AWS gives the copy the checksum it asks for in place of the source's. The source's is
    // still verified on the way, but a copy that kept both could never be copied again: a copy
    // presents its source's stored checksums as its body's, and a body may have only one.
    let server = server().await;
    let data = patterned(64);
    let digest = |sha256: bool| {
        let mut hasher = s3s::checksum::ChecksumHasher::default();
        if sha256 {
            hasher.sha256 = Some(Default::default());
        } else {
            hasher.crc32 = Some(Default::default());
        }
        hasher.update(&data);
        hasher.finalize()
    };
    let (crc32, sha256) = (digest(false), digest(true));
    // Without an MD5 ETag the copy is buffered and stores every checksum it computes, where
    // the backend keeps s3dal's ETag.
    plant_with(
        &server,
        "src",
        &data,
        Computed {
            checksum: crc32.clone(),
            ..Default::default()
        },
    )
    .await;
    for (from, to) in [("src", "sha"), ("sha", "again")] {
        let out = copy(&server, from, BUCKET, to)
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .send()
            .await
            .unwrap();
        let result = out.copy_object_result().unwrap();
        assert_eq!(result.checksum_crc32(), None, "{to}");
        assert_eq!(
            result.checksum_sha256(),
            sha256.checksum_sha256.as_deref(),
            "{to}"
        );
        let kept = stored(&server, BUCKET, to).await.checksum;
        assert_eq!(kept.checksum_crc32, None, "{to}");
        if !server.profile.native_etag {
            assert_eq!(kept.checksum_sha256, sha256.checksum_sha256, "{to}");
        }
    }
    copy(&server, "again", BUCKET, "plain")
        .send()
        .await
        .unwrap();
    assert_eq!(get(&server, BUCKET, "plain").await, data);
}

#[tokio::test]
async fn copy_is_aborted_when_the_source_does_not_match_its_md5() {
    // The source is hashed while it streams. A source that no longer matches its ETag, because
    // it was replaced mid-copy or modified behind s3dal's back, must not produce a copy that
    // claims the old MD5.
    let server = fs_server_with(Settings::default()).await;
    plant_with(
        &server,
        "stale",
        &patterned(64),
        Computed {
            md5_hex: Some(md5_hex(b"what it used to be")),
            ..Default::default()
        },
    )
    .await;

    let err = no_retry_client(&server)
        .copy_object()
        .copy_source(format!("{BUCKET}/stale"))
        .bucket(OTHER)
        .key("dir/copy")
        .send()
        .await
        .unwrap_err();
    // Retryable: the next attempt reads the source as it is then.
    assert_eq!(status(&err), 500, "{err:?}");
    assert!(!exists(&server, OTHER, "dir/").await);
    server.assert_no_temp_files().await;
}

#[tokio::test]
async fn copy_is_aborted_when_the_source_does_not_match_its_checksum() {
    // The same protection for stored checksums, on the buffered path.
    let server = server().await;
    plant_with(
        &server,
        "stale",
        &patterned(64),
        Computed {
            checksum: s3s::dto::Checksum {
                checksum_crc32: Some("AAAAAA==".to_owned()),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await;

    let err = no_retry_client(&server)
        .copy_object()
        .copy_source(format!("{BUCKET}/stale"))
        .bucket(BUCKET)
        .key("copy")
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&err), 500, "{err:?}");
    assert!(!exists(&server, BUCKET, "copy").await);
    server.assert_no_temp_files().await;
}

async fn put_with_attrs(server: &TestServer, key: &str) {
    server
        .client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .content_type("text/plain")
        .cache_control("max-age=60")
        .content_disposition("attachment")
        .content_encoding("identity")
        .content_language("zh-TW")
        .metadata("color", "red")
        .body(ByteStream::from_static(b"hello"))
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn metadata_directive_copy_keeps_the_source_attrs() {
    // COPY is the default: a copy must carry Content-Type and the other stored headers, or a
    // copied web asset would be served as binary. Headers sent with COPY are ignored, as S3
    // ignores them.
    let server = server().await;
    put_with_attrs(&server, "src").await;
    copy(&server, "src", OTHER, "copy")
        .content_type("image/png")
        .metadata("color", "blue")
        .send()
        .await
        .unwrap();

    let source = stored(&server, BUCKET, "src").await;
    assert_eq!(source.attrs.cache_control.as_deref(), Some("max-age=60"));
    assert_eq!(stored(&server, OTHER, "copy").await.attrs, source.attrs);

    let got = server
        .client
        .get_object()
        .bucket(OTHER)
        .key("copy")
        .send()
        .await
        .unwrap();
    assert_eq!(got.content_type(), Some("text/plain"));
    assert_eq!(
        got.metadata().unwrap().get("color").map(String::as_str),
        Some("red")
    );
}

#[tokio::test]
async fn metadata_directive_replace_takes_attrs_from_the_request_only() {
    // REPLACE is how clients change an object's headers; nothing of the source's may linger.
    let server = server().await;
    put_with_attrs(&server, "src").await;
    copy(&server, "src", OTHER, "copy")
        .metadata_directive(MetadataDirective::Replace)
        .content_type("image/png")
        .metadata("shape", "round")
        .send()
        .await
        .unwrap();

    assert_eq!(
        stored(&server, OTHER, "copy").await.attrs,
        Attrs {
            content_type: Some("image/png".to_owned()),
            user_metadata: HashMap::from([("shape".to_owned(), "round".to_owned())]),
            ..Default::default()
        }
    );
}

#[tokio::test]
async fn metadata_directive_replace_rejects_reserved_metadata() {
    // Otherwise a client could forge the ETag or checksums s3dal reports for the copy.
    let server = server().await;
    put(&server, "src", b"x").await;
    let err = copy(&server, "src", BUCKET, "copy")
        .metadata_directive(MetadataDirective::Replace)
        .metadata("s3dal-etag", "forged")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("InvalidArgument"), "{err:?}");
    assert!(!exists(&server, BUCKET, "copy").await);
}

#[tokio::test]
async fn copying_onto_itself_needs_replace() {
    // S3 refuses a self-copy that changes nothing; with REPLACE it is how metadata is edited in
    // place, and the content must survive being read and rewritten at the same path.
    let server = server().await;
    let data = patterned(5000);
    let etag = put(&server, "obj", &data).await;

    let err = copy(&server, "obj", BUCKET, "obj")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("InvalidRequest"), "{err:?}");

    let out = copy(&server, "obj", BUCKET, "obj")
        .metadata_directive(MetadataDirective::Replace)
        .content_type("text/csv")
        .send()
        .await
        .unwrap();
    assert_eq!(
        out.copy_object_result().unwrap().e_tag(),
        Some(etag.as_str())
    );
    assert_eq!(get(&server, BUCKET, "obj").await, data);
    assert_eq!(
        stored(&server, BUCKET, "obj")
            .await
            .attrs
            .content_type
            .as_deref(),
        Some("text/csv")
    );
}

#[tokio::test]
async fn failed_source_conditions_are_412_never_304() {
    // For a copy, a condition that would make GET answer 304 Not Modified still means the
    // copy did not happen; AWS answers 412 for every failed x-amz-copy-source-if-* header.
    let server = server().await;
    let etag = put(&server, "src", b"data").await;
    let past = DateTime::from_secs(1_000_000_000);
    let future = DateTime::from_secs(4_000_000_000);

    let failing: Vec<(&str, CopyObjectFluentBuilder)> = vec![
        (
            "if-match other",
            copy(&server, "src", BUCKET, "copy").copy_source_if_match("\"0123\""),
        ),
        (
            "if-none-match current",
            copy(&server, "src", BUCKET, "copy").copy_source_if_none_match(&etag),
        ),
        (
            "if-modified-since future",
            copy(&server, "src", BUCKET, "copy").copy_source_if_modified_since(future),
        ),
        (
            "if-unmodified-since past",
            copy(&server, "src", BUCKET, "copy").copy_source_if_unmodified_since(past),
        ),
    ];
    for (name, request) in failing {
        let err = request.send().await.unwrap_err();
        assert_eq!(status(&err), 412, "{name}: {err:?}");
        assert_eq!(err.code(), Some("PreconditionFailed"), "{name}");
        assert!(!exists(&server, BUCKET, "copy").await, "{name}");
    }

    // If-Match that holds overrides If-Unmodified-Since, as for GET.
    copy(&server, "src", BUCKET, "copy")
        .copy_source_if_match(&etag)
        .copy_source_if_unmodified_since(past)
        .send()
        .await
        .unwrap();
    assert_eq!(get(&server, BUCKET, "copy").await, b"data");
}

#[tokio::test]
async fn destination_conditions_guard_the_key_being_replaced() {
    // If-None-Match and If-Match apply to the destination, as for PutObject: a copy used to
    // create a lock file, or to replace a known version, must not clobber anything else.
    let server = server().await;
    put(&server, "src", b"new").await;

    copy(&server, "src", BUCKET, "dst")
        .if_none_match("*")
        .send()
        .await
        .unwrap();
    let err = copy(&server, "src", BUCKET, "dst")
        .if_none_match("*")
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&err), 412, "{err:?}");

    let current = put(&server, "dst", b"old").await;
    let err = copy(&server, "src", BUCKET, "dst")
        .if_match("\"0123\"")
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&err), 412, "{err:?}");
    assert_eq!(get(&server, BUCKET, "dst").await, b"old");
    copy(&server, "src", BUCKET, "dst")
        .if_match(&current)
        .send()
        .await
        .unwrap();
    assert_eq!(get(&server, BUCKET, "dst").await, b"new");

    // S3 answers If-Match on a missing key with 404, which tells the client the object is gone.
    let err = copy(&server, "src", BUCKET, "absent")
        .if_match(&current)
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("NoSuchKey"), "{err:?}");
    assert!(!exists(&server, BUCKET, "absent").await);
}

#[tokio::test]
async fn missing_source_bucket_key_or_destination_bucket_is_404() {
    // Clients tell a typo in the bucket from a missing object by these codes.
    let server = server().await;
    put(&server, "src", b"x").await;
    server.plant(BUCKET, "dir/inner", b"x").await;

    let cases = [
        ("nosuchbucket/src", BUCKET, "NoSuchBucket"),
        ("bkt/absent", BUCKET, "NoSuchKey"),
        // A prefix is a directory on fs, never an object.
        ("bkt/dir", BUCKET, "NoSuchKey"),
        ("bkt/src", "nosuchbucket", "NoSuchBucket"),
    ];
    for (source, bucket, code) in cases {
        let err = server
            .client
            .copy_object()
            .copy_source(source)
            .bucket(bucket)
            .key("copy")
            .send()
            .await
            .unwrap_err();
        assert_eq!(status(&err), 404, "{source} -> {bucket}: {err:?}");
        assert_eq!(err.code(), Some(code), "{source} -> {bucket}");
    }
}

#[tokio::test]
async fn only_the_null_source_version_exists() {
    // Unversioned objects have the version id "null"; copying any other version must not copy
    // the current object as if it were that version.
    let server = server().await;
    put(&server, "src", b"x").await;
    let copy_version = |version: &str| {
        server
            .client
            .copy_object()
            .copy_source(format!("{BUCKET}/src?versionId={version}"))
            .bucket(BUCKET)
            .key("copy")
    };

    copy_version("null").send().await.unwrap();
    let err = copy_version("3HL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY")
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&err), 404, "{err:?}");
    assert_eq!(err.code(), Some("NoSuchVersion"));
}

#[tokio::test]
async fn protections_and_stored_values_s3dal_lacks_are_refused() {
    // A client asking for encryption, retention, tags, a public ACL or an owner check relies on
    // it; a copy that silently went ahead without it would mislead the client.
    let server = server().await;
    put(&server, "src", b"x").await;
    let refused: Vec<(&str, CopyObjectFluentBuilder)> = vec![
        (
            "sse",
            copy(&server, "src", BUCKET, "copy")
                .server_side_encryption(ServerSideEncryption::Aes256),
        ),
        (
            "sse-c",
            copy(&server, "src", BUCKET, "copy")
                .sse_customer_algorithm("AES256")
                .sse_customer_key(STANDARD.encode_to_string([7u8; 32]))
                .sse_customer_key_md5(STANDARD.encode_to_string(Md5::digest([7u8; 32]))),
        ),
        (
            "object lock",
            copy(&server, "src", BUCKET, "copy")
                .object_lock_mode(ObjectLockMode::Governance)
                .object_lock_retain_until_date(DateTime::from_secs(4_000_000_000)),
        ),
        (
            "tagging",
            copy(&server, "src", BUCKET, "copy")
                .tagging_directive(TaggingDirective::Replace)
                .tagging("team=a"),
        ),
        (
            "public acl",
            copy(&server, "src", BUCKET, "copy").acl(ObjectCannedAcl::PublicRead),
        ),
        (
            "website redirect",
            copy(&server, "src", BUCKET, "copy").website_redirect_location("/elsewhere"),
        ),
        (
            "expected owner",
            copy(&server, "src", BUCKET, "copy").expected_bucket_owner("123456789012"),
        ),
    ];
    for (name, request) in refused {
        let err = request.send().await.unwrap_err();
        assert_eq!(err.code(), Some("NotImplemented"), "{name}: {err:?}");
        assert!(!exists(&server, BUCKET, "copy").await, "{name}");
    }

    // Advisory headers change nothing a client can observe here, so they are accepted.
    copy(&server, "src", BUCKET, "copy")
        .acl(ObjectCannedAcl::Private)
        .storage_class(StorageClass::StandardIa)
        .tagging_directive(TaggingDirective::Replace)
        .send()
        .await
        .unwrap();
    assert_eq!(get(&server, BUCKET, "copy").await, b"x");
}

#[tokio::test]
async fn access_point_copy_sources_are_not_implemented() {
    // s3dal has no access points; resolving the ARN's key in some bucket would be a guess.
    let server = server().await;
    put(&server, "src", b"x").await;
    let err = server
        .client
        .copy_object()
        .copy_source("arn:aws:s3:us-east-1:123456789012:accesspoint/myap/object/src")
        .bucket(BUCKET)
        .key("copy")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("NotImplemented"), "{err:?}");
}

#[tokio::test]
async fn unknown_directives_are_invalid_arguments() {
    // Guessing COPY or REPLACE for a misspelled directive would keep or drop the source's
    // metadata against the client's intent.
    let server = server().await;
    put(&server, "src", b"x").await;
    let err = copy(&server, "src", BUCKET, "copy")
        .metadata_directive(MetadataDirective::from("REPLACED"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("InvalidArgument"), "{err:?}");
    let err = copy(&server, "src", BUCKET, "copy")
        .tagging_directive(TaggingDirective::from("KEEP"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("InvalidArgument"), "{err:?}");
    assert!(!exists(&server, BUCKET, "copy").await);
}

#[tokio::test]
async fn copying_onto_a_folder_marker_key_is_not_implemented() {
    // A key ending in '/' is a directory on fs, which can hold neither the source's bytes nor
    // its metadata; writing it anyway would fail with a misleading key/prefix conflict.
    let server = server().await;
    put(&server, "src", b"").await;
    let err = copy(&server, "src", BUCKET, "dir/")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("NotImplemented"), "{err:?}");
    assert!(!exists(&server, BUCKET, "dir/").await);
}

/// An upload of `BUCKET/key`.
async fn create_upload(server: &TestServer, key: &str) -> String {
    let out = server
        .client
        .create_multipart_upload()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .unwrap();
    out.upload_id().unwrap().to_owned()
}

/// Copies `BUCKET/src_key` into part `n` of upload `id` of `BUCKET/key`.
fn part_copy(
    client: &Client,
    src_key: &str,
    key: &str,
    id: &str,
    n: i32,
) -> UploadPartCopyFluentBuilder {
    client
        .upload_part_copy()
        .copy_source(format!("{BUCKET}/{src_key}"))
        .bucket(BUCKET)
        .key(key)
        .upload_id(id)
        .part_number(n)
}

/// The part numbers and sizes ListParts reports for the upload.
async fn listed_parts(server: &TestServer, key: &str, id: &str) -> Vec<(i32, i64)> {
    let out = server
        .client
        .list_parts()
        .bucket(BUCKET)
        .key(key)
        .upload_id(id)
        .send()
        .await
        .unwrap();
    out.parts()
        .iter()
        .map(|p| (p.part_number().unwrap(), p.size().unwrap()))
        .collect()
}

async fn complete_upload(server: &TestServer, key: &str, id: &str, etags: &[&str]) -> String {
    let parts = etags
        .iter()
        .zip(1..)
        .map(|(etag, n)| CompletedPart::builder().part_number(n).e_tag(*etag).build())
        .collect();
    let out = server
        .client
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key(key)
        .upload_id(id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(parts))
                .build(),
        )
        .send()
        .await
        .unwrap();
    out.e_tag().unwrap().to_owned()
}

#[tokio::test]
async fn upload_part_copy_assembles_an_object_from_ranges_of_another() {
    // Copy tools build large server-side copies from ranges of the source. Every byte must land
    // in its part, and each part's ETag must be the MD5 of its range, or the object's multipart
    // ETag differs from the one S3 computes and clients that recompute it report corruption.
    let server = server().await;
    let split = 5 * 1024 * 1024;
    let data = patterned(split + 1000);
    put(&server, "src", &data).await;
    let id = create_upload(&server, "big").await;

    let first = part_copy(&server.client, "src", "big", &id, 1)
        .copy_source_range(format!("bytes=0-{}", split - 1))
        .send()
        .await
        .unwrap();
    let second = part_copy(&server.client, "src", "big", &id, 2)
        .copy_source_range(format!("bytes={split}-{}", data.len() - 1))
        .send()
        .await
        .unwrap();
    let e1 = first
        .copy_part_result()
        .unwrap()
        .e_tag()
        .unwrap()
        .to_owned();
    let e2 = second
        .copy_part_result()
        .unwrap()
        .e_tag()
        .unwrap()
        .to_owned();
    assert_eq!(e1, quoted(&md5_hex(&data[..split])));
    assert_eq!(e2, quoted(&md5_hex(&data[split..])));
    assert!(second.copy_part_result().unwrap().last_modified().is_some());
    assert_eq!(
        listed_parts(&server, "big", &id).await,
        [(1, split as i64), (2, 1000)]
    );

    let etag = complete_upload(&server, "big", &id, &[&e1, &e2]).await;
    let mut concat = Md5::digest(&data[..split]).to_vec();
    concat.extend_from_slice(&Md5::digest(&data[split..]));
    assert_eq!(etag, quoted(&format!("{}-2", md5_hex(&concat))));
    assert_eq!(get(&server, BUCKET, "big").await, data);
    server.assert_no_temp_files().await;
}

#[tokio::test]
async fn upload_part_copy_without_a_range_copies_the_whole_source() {
    // The whole source is verified against its MD5 and stored checksums while it streams, and
    // the part reports them like an uploaded part.
    let server = server().await;
    put(&server, "src", b"whole source").await;
    let crc32 = stored(&server, BUCKET, "src").await.checksum.checksum_crc32;
    assert!(
        crc32.is_some(),
        "the SDK sends CRC32 with an in-memory body"
    );
    let id = create_upload(&server, "k").await;
    let out = part_copy(&server.client, "src", "k", &id, 1)
        .send()
        .await
        .unwrap();
    let result = out.copy_part_result().unwrap();
    let etag = result.e_tag().unwrap().to_owned();
    assert_eq!(etag, quoted(&md5_hex(b"whole source")));
    assert_eq!(result.checksum_crc32(), crc32.as_deref());

    complete_upload(&server, "k", &id, &[&etag]).await;
    assert_eq!(get(&server, BUCKET, "k").await, b"whole source");
}

#[tokio::test]
async fn upload_part_copy_is_aborted_when_the_source_does_not_match_its_md5() {
    // As for CopyObject: a source that no longer matches its ETag must not become a part, and
    // the failure is a retryable 500.
    let server = fs_server_with(Settings::default()).await;
    plant_with(
        &server,
        "stale",
        &patterned(64),
        Computed {
            md5_hex: Some(md5_hex(b"what it used to be")),
            ..Default::default()
        },
    )
    .await;
    let id = create_upload(&server, "k").await;
    let err = part_copy(&no_retry_client(&server), "stale", "k", &id, 1)
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&err), 500, "{err:?}");
    assert_eq!(listed_parts(&server, "k", &id).await, []);
}

#[tokio::test]
async fn upload_part_copy_refuses_what_it_cannot_copy_and_commits_nothing() {
    let server = server().await;
    let etag = put(&server, "src", b"12345").await;
    let id = create_upload(&server, "k").await;
    let key = [7u8; 32];
    let client = &server.client;
    let cases: Vec<(&str, UploadPartCopyFluentBuilder, u16, &str)> = vec![
        (
            "open-ended range",
            part_copy(client, "src", "k", &id, 1).copy_source_range("bytes=0-"),
            400,
            "InvalidArgument",
        ),
        (
            "range beyond the source",
            part_copy(client, "src", "k", &id, 1).copy_source_range("bytes=0-21"),
            416,
            "InvalidRange",
        ),
        // Never 304: the part was not copied, so there is nothing cached to keep using.
        (
            "failed source condition",
            part_copy(client, "src", "k", &id, 1).copy_source_if_none_match(&etag),
            412,
            "PreconditionFailed",
        ),
        (
            "missing source",
            part_copy(client, "absent", "k", &id, 1),
            404,
            "NoSuchKey",
        ),
        (
            "another source version",
            part_copy(client, "src", "k", &id, 1)
                .copy_source(format!("{BUCKET}/src?versionId=3HL4kqtJlcpXroDTDmJ")),
            404,
            "NoSuchVersion",
        ),
        (
            "customer key",
            part_copy(client, "src", "k", &id, 1)
                .sse_customer_algorithm("AES256")
                .sse_customer_key(STANDARD.encode_to_string(key))
                .sse_customer_key_md5(STANDARD.encode_to_string(Md5::digest(key))),
            501,
            "NotImplemented",
        ),
        (
            "unknown upload",
            part_copy(
                client,
                "src",
                "k",
                "3f0e1c52-7a4b-4c1d-9e2f-0a1b2c3d4e5f",
                1,
            ),
            404,
            "NoSuchUpload",
        ),
        (
            "part number out of range",
            part_copy(client, "src", "k", &id, 10_001),
            400,
            "InvalidArgument",
        ),
    ];
    for (name, request, expected_status, code) in cases {
        let err = request.send().await.unwrap_err();
        assert_eq!(status(&err), expected_status, "{name}: {err:?}");
        assert_eq!(err.code(), Some(code), "{name}: {err:?}");
    }
    assert_eq!(listed_parts(&server, "k", &id).await, []);
    server.assert_no_temp_files().await;
}

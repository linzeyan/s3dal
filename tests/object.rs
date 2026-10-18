mod common;

use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::get_object_attributes::GetObjectAttributesOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::operation::put_object::builders::PutObjectFluentBuilder;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::{ByteStream, DateTime};
use aws_sdk_s3::types::{
    ChecksumMode, ChecksumType, ObjectAttributes, ObjectCannedAcl, ObjectLockLegalHoldStatus,
    ObjectLockMode, Permission, RequestPayer, ServerSideEncryption, StorageClass, Tag, Tagging,
};
use base64_simd::STANDARD;
use common::TestServer;
use md5::{Digest, Md5};
use s3dal::meta::{Attrs, Computed, PartLayout, md5_hex};
use s3dal::{Settings, meta};
use s3s::checksum::ChecksumHasher;
use s3s::dto::Checksum;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const BUCKET: &str = "bkt";

async fn server() -> TestServer {
    let server = TestServer::start().await;
    server.create_bucket_dir(BUCKET).await;
    server
}

/// A server on fs whatever the suite's backend, for what only fs has: files planted by other
/// tools without s3dal's metadata, and directories that conflict with keys.
async fn fs_server() -> TestServer {
    let server = TestServer::start_fs().await;
    server.create_bucket_dir(BUCKET).await;
    server
}

/// A server that buffers at most `limit` bytes for an MD5 ETag, so the large-PUT path can be
/// exercised with tiny bodies.
async fn server_with_buffer_limit(limit: u64) -> TestServer {
    let server = TestServer::start_with(Settings {
        etag_buffer_limit: limit,
        ..Default::default()
    })
    .await;
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

async fn get(server: &TestServer, key: &str) -> Vec<u8> {
    let out = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .unwrap();
    out.body.collect().await.unwrap().into_bytes().to_vec()
}

fn status<E>(err: &SdkError<E, HttpResponse>) -> u16 {
    err.raw_response().unwrap().status().as_u16()
}

fn quoted(etag: &str) -> String {
    format!("\"{etag}\"")
}

fn content_md5(data: &[u8]) -> String {
    STANDARD.encode_to_string(Md5::digest(data))
}

/// A body past the 16-byte buffer limit of `server_with_buffer_limit(16)`, and past OpenDAL's
/// 5 MiB minimum part size, so a backend with native ETags receives it in parts as well: with
/// one request it would compute an MD5 ETag, and the streaming path would not show.
const STREAMED_LEN: usize = 5 * 1024 * 1024 + 1;

fn patterned(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// A body the SDK cannot hold in memory, as for a file upload. The SDK then sends its CRC32
/// as an aws-chunked trailer after the data; for in-memory bodies it sends a header instead.
/// `dir` must outlive the request, which reads the file while sending.
async fn streamed_body(dir: &tempfile::TempDir, data: &[u8]) -> ByteStream {
    let path = dir.path().join("body");
    std::fs::write(&path, data).unwrap();
    ByteStream::from_path(&path).await.unwrap()
}

/// Writes an object through OpenDAL with s3dal's metadata, as CompleteMultipartUpload does,
/// so stored part layouts and checksums can be tested on their own.
async fn plant_with(server: &TestServer, key: &str, data: &[u8], computed: Computed) {
    let opts = meta::write_options(&server.profile, Attrs::default(), computed);
    server
        .op
        .write_options(&format!("{BUCKET}/{key}"), data.to_vec(), opts)
        .await
        .unwrap();
}

fn parts(sizes: &[u64]) -> Computed {
    Computed {
        parts: Some(PartLayout::new(sizes)),
        ..Default::default()
    }
}

fn crc32(data: &[u8]) -> String {
    let mut hasher = ChecksumHasher {
        crc32: Some(Default::default()),
        ..Default::default()
    };
    hasher.update(data);
    hasher.finalize().checksum_crc32.unwrap()
}

/// HEAD in checksum mode. The SDK asks for checksums on every GET, but not on HEAD.
async fn head_with_checksums(server: &TestServer, key: &str) -> HeadObjectOutput {
    server
        .client
        .head_object()
        .bucket(BUCKET)
        .key(key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn small_and_large_objects_round_trip_byte_for_byte() {
    // Both write paths (buffered for an MD5 ETag, and streamed) must store exactly what was
    // sent. The large body also spans several of OpenDAL's 2 MiB read chunks.
    let server = server_with_buffer_limit(1024).await;
    let small = patterned(512);
    let large = patterned(3 * 1024 * 1024 + 7);
    put(&server, "small", &small).await;
    put(&server, "dir/large", &large).await;
    assert_eq!(get(&server, "small").await, small);
    assert_eq!(get(&server, "dir/large").await, large);
}

#[tokio::test]
async fn small_put_without_content_md5_gets_its_md5_as_etag() {
    // aws CLI and rclone compare a 32-hex ETag with the content MD5; uploads below the
    // buffer limit must therefore get the real MD5 even though the SDK sends no Content-MD5.
    let server = server().await;
    let data = b"hello s3dal";
    let etag = put(&server, "k", data).await;
    assert_eq!(etag, quoted(&md5_hex(data)));
    let head = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(head.e_tag(), Some(etag.as_str()));
}

#[tokio::test]
async fn content_md5_becomes_the_etag_even_above_the_buffer_limit() {
    // A client-supplied MD5 is known before the write starts, so it can be stored with the
    // object without buffering it.
    let server = server_with_buffer_limit(16).await;
    let data = patterned(64);
    let out = server
        .client
        .put_object()
        .bucket(BUCKET)
        .key("k")
        .content_md5(content_md5(&data))
        .body(ByteStream::from(data.clone()))
        .send()
        .await
        .unwrap();
    assert_eq!(out.e_tag(), Some(quoted(&md5_hex(&data)).as_str()));
    let head = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(head.e_tag(), out.e_tag());
}

#[tokio::test]
async fn large_put_gets_a_synthesized_etag_that_put_head_and_get_agree_on() {
    // Above the limit nothing could store an MD5, so the ETag is synthesized. It contains '-'
    // so no client mistakes it for a content MD5, and PUT must report exactly what later reads
    // report, or clients that verify uploads by ETag would see a mismatch.
    let server = server_with_buffer_limit(16).await;
    let data = patterned(STREAMED_LEN);
    let etag = put(&server, "big", &data).await;
    assert!(etag.contains('-'), "{etag}");
    assert_ne!(etag, quoted(&md5_hex(&data)));

    let head = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("big")
        .send()
        .await
        .unwrap();
    assert_eq!(head.e_tag(), Some(etag.as_str()));
    let got = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("big")
        .send()
        .await
        .unwrap();
    assert_eq!(got.e_tag(), Some(etag.as_str()));
}

#[tokio::test]
async fn content_md5_mismatch_is_bad_digest_and_leaves_nothing_behind() {
    // A corrupted upload must never become visible, not even as an empty parent directory.
    let server = server_with_buffer_limit(16).await;
    let err = server
        .client
        .put_object()
        .bucket(BUCKET)
        .key("x/y/obj")
        .content_md5(content_md5(b"something else"))
        .body(ByteStream::from(patterned(64)))
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("BadDigest"), "{err:?}");
    assert_eq!(server.names(BUCKET).await, Vec::<String>::new());
    server.assert_no_temp_files().await;
}

#[tokio::test]
async fn an_empty_content_md5_is_invalid_digest() {
    // The client asked for a check and sent nothing to check against; AWS refuses that rather
    // than store the body unverified. The SDK leaves empty headers out, so it is added before
    // signing.
    let server = server().await;
    let err = server
        .client
        .put_object()
        .bucket(BUCKET)
        .key("obj")
        .body(ByteStream::from_static(b"hello"))
        .customize()
        .mutate_request(|req| {
            req.headers_mut().insert("content-md5", "");
        })
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("InvalidDigest"), "{err:?}");
    assert_eq!(server.names(BUCKET).await, Vec::<String>::new());
}

#[tokio::test]
async fn checksum_mismatch_is_bad_digest_and_leaves_nothing_behind() {
    let server = server().await;
    let err = server
        .client
        .put_object()
        .bucket(BUCKET)
        .key("obj")
        .checksum_crc32("AAAAAA==")
        .body(ByteStream::from_static(b"hello world"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("BadDigest"), "{err:?}");
    assert_eq!(server.names(BUCKET).await, Vec::<String>::new());
    server.assert_no_temp_files().await;
}

#[tokio::test]
async fn put_echoes_the_checksum_the_sdk_sent() {
    // The SDK sends CRC32 by default (as a header for in-memory bodies, as an aws-chunked
    // trailer for streamed ones) and S3 echoes it back; the value here can only come from
    // hashing the body it was declared for.
    let server = server().await;
    let out = server
        .client
        .put_object()
        .bucket(BUCKET)
        .key("obj")
        .body(ByteStream::from_static(b"hello world"))
        .send()
        .await
        .unwrap();
    assert_eq!(out.checksum_crc32(), Some("DUoRhQ=="));
}

#[tokio::test]
async fn single_byte_ranges_return_partial_content() {
    // Resumable downloads and parallel range readers (aws CLI, rclone, Spark) depend on exact
    // Content-Range bookkeeping, including open-ended and suffix forms.
    let server = server().await;
    put(&server, "digits", b"0123456789").await;
    for (range, body, content_range) in [
        ("bytes=0-4", "01234", "bytes 0-4/10"),
        ("bytes=7-", "789", "bytes 7-9/10"),
        ("bytes=-3", "789", "bytes 7-9/10"),
        ("bytes=-30", "0123456789", "bytes 0-9/10"),
        ("bytes=5-100", "56789", "bytes 5-9/10"),
    ] {
        let out = server
            .client
            .get_object()
            .bucket(BUCKET)
            .key("digits")
            .range(range)
            .send()
            .await
            .unwrap();
        assert_eq!(out.content_range(), Some(content_range), "{range}");
        assert_eq!(out.content_length(), Some(body.len() as i64), "{range}");
        let got = out.body.collect().await.unwrap().into_bytes();
        assert_eq!(&got[..], body.as_bytes(), "{range}");
    }
}

#[tokio::test]
async fn range_beyond_the_object_is_invalid_range() {
    let server = server().await;
    put(&server, "digits", b"0123456789").await;
    let err = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("digits")
        .range("bytes=10-")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("InvalidRange"), "{err:?}");
    assert_eq!(status(&err), 416);
}

#[tokio::test]
async fn multiple_ranges_are_refused_before_reaching_s3dal() {
    // Documented deviation: AWS ignores a multi-range header and returns the whole object;
    // s3s 0.17 rejects it while parsing, before s3dal sees the request. If s3s changes, this
    // test fails and the deviation note must be revisited.
    let server = server().await;
    put(&server, "digits", b"0123456789").await;
    let err = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("digits")
        .range("bytes=0-1,3-4")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("InvalidArgument"), "{err:?}");
}

#[tokio::test]
async fn if_match_mismatch_is_precondition_failed_on_get_and_head() {
    // Ignoring If-Match would let a reader mix bytes from two versions of an object.
    let server = server().await;
    put(&server, "k", b"v1").await;
    let get = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .if_match("\"not-the-etag\"")
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&get), 412);
    assert_eq!(get.code(), Some("PreconditionFailed"));
    let head = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("k")
        .if_match("\"not-the-etag\"")
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&head), 412);
}

#[tokio::test]
async fn matching_if_match_returns_the_object() {
    let server = server().await;
    let etag = put(&server, "k", b"v1").await;
    let out = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .if_match(&etag)
        .send()
        .await
        .unwrap();
    assert_eq!(&out.body.collect().await.unwrap().into_bytes()[..], b"v1");
}

#[tokio::test]
async fn matching_if_none_match_is_not_modified_and_keeps_the_etag() {
    // Caches revalidate with If-None-Match; a 304 must still name the current ETag.
    let server = server().await;
    let etag = put(&server, "k", b"v1").await;
    let get = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .if_none_match(&etag)
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&get), 304);
    assert_eq!(
        get.raw_response().unwrap().headers().get("etag"),
        Some(etag.as_str())
    );
    let head = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("k")
        .if_none_match(&etag)
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&head), 304);
}

#[tokio::test]
async fn etag_lists_match_any_of_their_tags() {
    // If-Match and If-None-Match take a list of entity tags (RFC 9110 §13.1.1): a cache that
    // holds several versions revalidates with all of them at once.
    let server = server().await;
    let etag = put(&server, "k", b"v1").await;
    let listed = format!("\"other\", {etag}");
    let out = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .if_match(&listed)
        .send()
        .await
        .unwrap();
    assert_eq!(&out.body.collect().await.unwrap().into_bytes()[..], b"v1");
    let head = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("k")
        .if_none_match(&listed)
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&head), 304);
    let get = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .if_match("\"other\", \"another\"")
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&get), 412);
}

#[tokio::test]
async fn echoed_last_modified_is_not_modified() {
    // Clients send back the Last-Modified they received, which has whole-second precision
    // while the file's mtime does not.
    let server = server().await;
    put(&server, "k", b"v1").await;
    let head = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    let last_modified = *head.last_modified().unwrap();
    let err = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .if_modified_since(last_modified)
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&err), 304);
}

#[tokio::test]
async fn write_after_if_unmodified_since_is_precondition_failed() {
    let server = server().await;
    put(&server, "k", b"v1").await;
    let err = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .if_unmodified_since(DateTime::from_secs(1_000_000_000))
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&err), 412);
}

#[tokio::test]
async fn missing_content_type_reads_back_as_binary_octet_stream() {
    // That is what AWS S3 reports, and some clients branch on it. The Rust SDK fills in
    // application/octet-stream itself, so the header is removed before signing to act like
    // clients (curl, some S3 libraries) that send none.
    let server = server().await;
    server
        .client
        .put_object()
        .bucket(BUCKET)
        .key("k")
        .body(ByteStream::from_static(b"x"))
        .customize()
        .mutate_request(|req| {
            req.headers_mut().remove("content-type");
        })
        .send()
        .await
        .unwrap();
    let head = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(head.content_type(), Some("binary/octet-stream"));
}

#[tokio::test]
async fn content_type_round_trips() {
    let server = server().await;
    let ct = "text/plain; charset=utf-8";
    server
        .client
        .put_object()
        .bucket(BUCKET)
        .key("k")
        .content_type(ct)
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap();
    let head = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(head.content_type(), Some(ct));
    let got = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(got.content_type(), Some(ct));
}

#[tokio::test]
async fn user_metadata_round_trips_without_reserved_keys() {
    // Clients see exactly their own x-amz-meta-*, never s3dal's bookkeeping next to it.
    let server = server().await;
    server
        .client
        .put_object()
        .bucket(BUCKET)
        .key("k")
        .metadata("color", "red")
        .metadata("shape", "round")
        .content_type("text/plain")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap();
    let head = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    let mut meta: Vec<_> = head.metadata().unwrap().iter().collect();
    meta.sort();
    assert_eq!(
        meta,
        [
            (&"color".to_owned(), &"red".to_owned()),
            (&"shape".to_owned(), &"round".to_owned())
        ]
    );
}

#[tokio::test]
async fn every_metadata_name_s3_allows_round_trips() {
    // Backends differ in the names they keep: Azure takes only C# identifiers, so a name with
    // `-`, `.` or a leading digit failed the whole upload there. `s3dal_2detag` is how such a
    // backend stores s3dal's own `s3dal-etag`; a client's name of that form must stay the
    // client's and never set the ETag.
    let server = server().await;
    let names = [
        "with-dash",
        "with_underscore",
        "0digit-first",
        "MiXeD-Case",
        "token!#$%&'*+.^`|~chars",
        "s3dal_2detag",
    ];
    let mut put = server
        .client
        .put_object()
        .bucket(BUCKET)
        .key("k")
        .body(ByteStream::from_static(b"x"));
    for (i, name) in names.iter().enumerate() {
        put = put.metadata(*name, format!("v{i}"));
    }
    let etag = put.send().await.unwrap().e_tag.unwrap();
    let head = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    let mut got: Vec<(String, String)> = head
        .metadata()
        .unwrap()
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    got.sort();
    // Header names are case-insensitive, and reach s3dal lowercased.
    let mut expected: Vec<(String, String)> = names
        .iter()
        .enumerate()
        .map(|(i, name)| (name.to_ascii_lowercase(), format!("v{i}")))
        .collect();
    expected.sort();
    assert_eq!(got, expected);
    assert_eq!(head.e_tag.unwrap(), etag);
}

#[tokio::test]
async fn metadata_values_lose_only_the_edge_spaces_a_header_cannot_carry() {
    // Leading and trailing spaces reach s3dal only RFC 2047-encoded, and no header can return
    // them. Stored as they came, they failed the whole upload on Azure, which trims them
    // before checking a signature made over the untrimmed value.
    let server = server().await;
    server
        .client
        .put_object()
        .bucket(BUCKET)
        .key("k")
        .metadata("padded", "=?UTF-8?B?ICBwYWRkZWQgIA==?=")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap();
    let head = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(head.metadata().unwrap()["padded"], "padded");
}

#[tokio::test]
async fn non_ascii_user_metadata_round_trips_rfc_2047_encoded() {
    // HTTP headers carry only ASCII, so S3 returns such a value RFC 2047-encoded, and an S3
    // upstream must be sent it that way too. A copy reads and writes it once more.
    let server = server().await;
    let value = "你好，世界";
    server
        .client
        .put_object()
        .bucket(BUCKET)
        .key("k")
        .metadata("greeting", value)
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap();
    server
        .client
        .copy_object()
        .bucket(BUCKET)
        .key("copy")
        .copy_source(format!("{BUCKET}/k"))
        .send()
        .await
        .unwrap();
    for key in ["k", "copy"] {
        let head = server
            .client
            .head_object()
            .bucket(BUCKET)
            .key(key)
            .send()
            .await
            .unwrap();
        let sent = head.metadata().unwrap().get("greeting").unwrap();
        assert_eq!(s3s_rfc2047::decode(sent).unwrap(), value, "{key}: {sent}");
    }
}

#[tokio::test]
async fn reserved_metadata_names_are_rejected_and_nothing_is_written() {
    // Otherwise a client could forge the ETag or Content-Type s3dal reports.
    let server = server().await;
    let err = server
        .client
        .put_object()
        .bucket(BUCKET)
        .key("k")
        .metadata("s3dal-etag", "forged")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("InvalidArgument"), "{err:?}");
    assert_eq!(server.names(BUCKET).await, Vec::<String>::new());
}

#[tokio::test]
async fn planted_file_reads_with_a_synthesized_etag_and_default_content_type() {
    // Files written into the fs root by other tools carry no s3dal metadata but must still be
    // served, with an ETag no client mistakes for a content MD5.
    let server = fs_server().await;
    server
        .plant(BUCKET, "planted/file.txt", b"from elsewhere")
        .await;
    let got = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("planted/file.txt")
        .send()
        .await
        .unwrap();
    let etag = got.e_tag().unwrap().to_owned();
    assert!(etag.ends_with("-0\""), "{etag}");
    assert_eq!(got.content_type(), Some("binary/octet-stream"));
    assert_eq!(
        &got.body.collect().await.unwrap().into_bytes()[..],
        b"from elsewhere"
    );
}

#[tokio::test]
async fn delete_is_idempotent() {
    // S3 answers 204 for a key that is already gone; retries and sync tools depend on it.
    let server = server().await;
    put(&server, "k", b"x").await;
    for _ in 0..2 {
        server
            .client
            .delete_object()
            .bucket(BUCKET)
            .key("k")
            .send()
            .await
            .unwrap();
    }
    let err = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("NoSuchKey"));
}

#[tokio::test]
async fn delete_prunes_empty_parents_but_never_the_bucket() {
    // Empty directories left behind would list as CommonPrefixes of nothing.
    let server = server().await;
    put(&server, "p/q/r", b"x").await;
    server
        .client
        .delete_object()
        .bucket(BUCKET)
        .key("p/q/r")
        .send()
        .await
        .unwrap();
    assert!(server.exists(&format!("{BUCKET}/")).await);
    assert_eq!(server.names(BUCKET).await, Vec::<String>::new());
}

#[tokio::test]
async fn delete_keeps_parents_that_still_hold_objects() {
    let server = server().await;
    put(&server, "p/q/r", b"x").await;
    put(&server, "p/s", b"y").await;
    server
        .client
        .delete_object()
        .bucket(BUCKET)
        .key("p/q/r")
        .send()
        .await
        .unwrap();
    assert_eq!(server.names("bkt/p").await, ["s"]);
    assert_eq!(get(&server, "p/s").await, b"y");
}

#[tokio::test]
async fn deleting_a_key_never_touches_the_same_name_with_a_slash() {
    // On fs `a` and `a/` share one path; a delete of either must not remove the other.
    let server = server().await;
    put(&server, "obj", b"keep me").await;
    server
        .client
        .delete_object()
        .bucket(BUCKET)
        .key("obj/")
        .send()
        .await
        .unwrap();
    assert_eq!(get(&server, "obj").await, b"keep me");

    put(&server, "marker/", b"").await;
    server
        .client
        .delete_object()
        .bucket(BUCKET)
        .key("marker")
        .send()
        .await
        .unwrap();
    assert!(server.exists("bkt/marker/").await);
}

#[tokio::test]
async fn deleting_a_folder_marker_keeps_the_objects_under_it() {
    // S3 deletes only the marker object, never the keys that share its prefix.
    let server = server().await;
    put(&server, "d/", b"").await;
    put(&server, "d/x", b"x").await;
    server
        .client
        .delete_object()
        .bucket(BUCKET)
        .key("d/")
        .send()
        .await
        .unwrap();
    assert_eq!(get(&server, "d/x").await, b"x");
}

#[tokio::test]
async fn folder_marker_put_creates_a_directory_with_the_empty_md5_etag() {
    // Consoles and sync tools create "folders" this way and expect S3's ETag for empty content.
    let server = server().await;
    let etag = put(&server, "folder/", b"").await;
    assert_eq!(etag, quoted(&md5_hex(b"")));
    assert!(server.exists("bkt/folder/").await);
    // Design: a directory reads as a missing key.
    let err = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("folder/")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("NoSuchKey"));
}

#[tokio::test]
async fn folder_marker_with_content_is_unexpected_content() {
    // A directory cannot hold bytes; silently dropping them would lose data.
    let server = server().await;
    let err = server
        .client
        .put_object()
        .bucket(BUCKET)
        .key("folder/")
        .body(ByteStream::from_static(b"data"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("UnexpectedContent"), "{err:?}");
    assert!(!server.exists("bkt/folder/").await);
}

#[tokio::test]
async fn object_over_an_existing_prefix_is_invalid_request() {
    // Documented deviation: on fs `a` cannot be an object while `a/b` exists.
    let server = fs_server().await;
    put(&server, "a/b", b"child").await;
    let err = server
        .client
        .put_object()
        .bucket(BUCKET)
        .key("a")
        .body(ByteStream::from_static(b"parent"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("InvalidRequest"), "{err:?}");
    assert_eq!(get(&server, "a/b").await, b"child");
    server.assert_no_temp_files().await;
}

#[tokio::test]
async fn object_under_an_existing_object_is_invalid_request() {
    // The other direction, at one and two levels below, and as a folder marker.
    let server = fs_server().await;
    put(&server, "f", b"file").await;
    for key in ["f/g", "f/g/h", "f/"] {
        let err = server
            .client
            .put_object()
            .bucket(BUCKET)
            .key(key)
            .body(ByteStream::from_static(b""))
            .send()
            .await
            .unwrap_err();
        assert_eq!(err.code(), Some("InvalidRequest"), "{key}: {err:?}");
    }
    assert_eq!(get(&server, "f").await, b"file");
    server.assert_no_temp_files().await;
}

#[tokio::test]
async fn reading_below_an_object_is_no_such_key() {
    // fs answers ENOTDIR here; clients must see an ordinary missing key, not a server error.
    let server = server().await;
    put(&server, "f", b"file").await;
    let err = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("f/g")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("NoSuchKey"), "{err:?}");
    server
        .client
        .delete_object()
        .bucket(BUCKET)
        .key("f/g")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn missing_bucket_is_no_such_bucket_for_every_operation() {
    let server = server().await;
    let client = &server.client;
    let put = client
        .put_object()
        .bucket("nope")
        .key("k")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(put.code(), Some("NoSuchBucket"));
    let get = client
        .get_object()
        .bucket("nope")
        .key("k")
        .send()
        .await
        .unwrap_err();
    assert_eq!(get.code(), Some("NoSuchBucket"));
    let delete = client
        .delete_object()
        .bucket("nope")
        .key("k")
        .send()
        .await
        .unwrap_err();
    assert_eq!(delete.code(), Some("NoSuchBucket"));
    let head = client
        .head_object()
        .bucket("nope")
        .key("k")
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&head), 404);
    // A PUT into a missing bucket must not create it as a side effect.
    assert!(!server.exists("nope/").await);
}

#[tokio::test]
async fn missing_key_is_no_such_key() {
    let server = server().await;
    let get = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("absent")
        .send()
        .await
        .unwrap_err();
    assert_eq!(get.code(), Some("NoSuchKey"));
    let head = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("absent")
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&head), 404);
}

#[tokio::test]
async fn keys_that_fs_would_normalize_or_escape_are_rejected() {
    // OpenDAL would store `a//b` as `a/b`, and `..` could leave the bucket.
    let server = server().await;
    let before = server.names("").await;
    for key in ["a//b", "../x"] {
        let err = server
            .client
            .put_object()
            .bucket(BUCKET)
            .key(key)
            .body(ByteStream::from_static(b"x"))
            .send()
            .await
            .unwrap_err();
        assert_eq!(err.code(), Some("InvalidArgument"), "{key}: {err:?}");
    }
    assert_eq!(server.names(BUCKET).await, Vec::<String>::new());
    assert_eq!(server.names("").await, before);
}

#[tokio::test]
async fn a_key_ending_in_whitespace_is_refused_rather_than_aliased() {
    // OpenDAL trims whitespace off the ends of every path, so `ws ` would overwrite, read and
    // delete `ws`: another object, without a word.
    let server = server().await;
    put(&server, "ws", b"first").await;
    // The SDK itself refuses a key of whitespace only, such as " ".
    for key in ["ws ", "ws\t", "ws\u{a0}", "ws/ "] {
        let err = server
            .client
            .put_object()
            .bucket(BUCKET)
            .key(key)
            .body(ByteStream::from_static(b"second"))
            .send()
            .await
            .unwrap_err();
        assert_eq!(err.code(), Some("InvalidArgument"), "{key:?}: {err:?}");
        let err = server
            .client
            .delete_object()
            .bucket(BUCKET)
            .key(key)
            .send()
            .await
            .unwrap_err();
        assert_eq!(err.code(), Some("InvalidArgument"), "{key:?}: {err:?}");
    }
    assert_eq!(get(&server, "ws").await, b"first");
}

#[tokio::test]
async fn a_key_with_a_control_character_is_refused() {
    // Every response that names a key is XML, which cannot carry the character, and s3s answers
    // 500 rather than send it: one such key would break every listing of its bucket for
    // clients that do not ask for URL-encoded keys.
    let server = server().await;
    let err = server
        .client
        .put_object()
        .bucket(BUCKET)
        .key("ctl\u{1}char")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("InvalidArgument"), "{err:?}");
    assert_eq!(server.names(BUCKET).await, Vec::<String>::new());
}

#[tokio::test]
async fn on_fs_files_no_key_can_name_are_left_out_of_listings() {
    // Other tools write files s3dal would refuse as keys. Listed, a control character would
    // fail the whole page, and a name ending in a space would read another object.
    let server = fs_server().await;
    for name in ["ctl\u{1}char", "ws ", "ok"] {
        std::fs::write(server.fs_path(&format!("{BUCKET}/{name}")), b"x").unwrap();
    }
    let v2 = server
        .client
        .list_objects_v2()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = v2.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(keys, ["ok"]);
    let v1 = server
        .client
        .list_objects()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = v1.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(keys, ["ok"]);
}

#[tokio::test]
async fn on_fs_a_segment_too_long_for_a_file_name_is_key_too_long() {
    // A file name holds at most 255 bytes, and s3dal's temporary file adds 9 to the last
    // segment; S3 limits only the whole key. A client can act on a 400; a 500 it would retry.
    let server = fs_server().await;
    for key in [
        "s".repeat(250),
        format!("dir/{}", "s".repeat(300)),
        format!("{}/x", "d".repeat(300)),
    ] {
        let err = server
            .client
            .put_object()
            .bucket(BUCKET)
            .key(&key)
            .body(ByteStream::from_static(b"x"))
            .send()
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            Some("KeyTooLongError"),
            "{}: {err:?}",
            key.len()
        );
        let err = server
            .client
            .get_object()
            .bucket(BUCKET)
            .key(&key)
            .send()
            .await
            .unwrap_err();
        assert_eq!(err.code(), Some("NoSuchKey"), "{}: {err:?}", key.len());
    }
    assert_eq!(server.names(BUCKET).await, Vec::<String>::new());
    server.assert_no_temp_files().await;
}

#[tokio::test]
async fn on_fs_deleting_a_key_beside_others_answers_at_once() {
    // Removing the key's now-empty folder fails while other keys remain in it, as it should.
    // Retried as if the disk had hiccuped, that failure held every such DELETE for 8 seconds,
    // and a 1000-key DeleteObjects for 12 minutes.
    let server = fs_server().await;
    put(&server, "d/a", b"a").await;
    put(&server, "d/b", b"b").await;
    let started = Instant::now();
    server
        .client
        .delete_object()
        .bucket(BUCKET)
        .key("d/a")
        .send()
        .await
        .unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(get(&server, "d/b").await, b"b");
}

#[tokio::test]
async fn two_checksums_on_one_put_are_refused() {
    // An object keeps one checksum, and S3 does not pick one of several for the client.
    let server = server().await;
    let err = server
        .client
        .put_object()
        .bucket(BUCKET)
        .key("k")
        .checksum_crc32("DUoRhQ==")
        .body(ByteStream::from_static(b"hello"))
        .customize()
        .mutate_request(|req| {
            req.headers_mut().insert(
                "x-amz-checksum-sha256",
                "LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=",
            );
        })
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("InvalidRequest"), "{err:?}");
    assert_eq!(server.names(BUCKET).await, Vec::<String>::new());
}

#[tokio::test]
async fn user_metadata_over_2_kb_is_metadata_too_large() {
    // S3's limit, counted over the bytes of every name and value. An S3 upstream would refuse
    // it too, but only after the whole body had been sent.
    let server = server().await;
    let put_with = |value: String| {
        server
            .client
            .put_object()
            .bucket(BUCKET)
            .key("k")
            .metadata("m", value)
            .body(ByteStream::from_static(b"x"))
            .send()
    };
    let err = put_with("v".repeat(2048)).await.unwrap_err();
    assert_eq!(err.code(), Some("MetadataTooLarge"), "{err:?}");
    assert_eq!(server.names(BUCKET).await, Vec::<String>::new());
    put_with("v".repeat(2047)).await.unwrap();
}

#[tokio::test]
async fn an_unknown_storage_class_is_invalid() {
    // A known class is accepted and has no effect; an unknown one is a typo S3 reports.
    let server = server().await;
    let put_in = |class: StorageClass| {
        server
            .client
            .put_object()
            .bucket(BUCKET)
            .key("k")
            .storage_class(class)
            .body(ByteStream::from_static(b"x"))
            .send()
    };
    let err = put_in(StorageClass::from("BOGUS")).await.unwrap_err();
    assert_eq!(err.code(), Some("InvalidStorageClass"), "{err:?}");
    put_in(StorageClass::StandardIa).await.unwrap();
}

#[tokio::test]
async fn reads_that_ask_for_server_side_encryption_are_invalid() {
    // Encryption belongs to the object, and nothing is stored encrypted: a client that asks to
    // read with it must not believe it got an encrypted object.
    let server = server().await;
    put(&server, "k", b"v").await;
    let sse = |req: &mut aws_sdk_s3::config::http::HttpRequest| {
        req.headers_mut()
            .insert("x-amz-server-side-encryption", "aws:kms");
    };
    let err = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .customize()
        .mutate_request(sse)
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("InvalidArgument"), "{err:?}");
    let err = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("k")
        .customize()
        .mutate_request(sse)
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&err), 400);
}

// Stored object headers and `response-*` overrides.

#[tokio::test]
async fn object_headers_round_trip_through_head_and_get_but_not_as_user_metadata() {
    // Browsers and CDNs act on these headers (caching, download names, decompression). S3
    // returns them exactly as stored, and never as x-amz-meta-* entries a client would mistake
    // for its own metadata.
    let server = server().await;
    server
        .client
        .put_object()
        .bucket(BUCKET)
        .key("k")
        .cache_control("max-age=60")
        .content_disposition("attachment; filename=\"a.txt\"")
        .content_encoding("gzip")
        .content_language("zh-TW")
        .expires(DateTime::from_secs(1_445_412_480))
        .metadata("color", "red")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap();
    let expires = "Wed, 21 Oct 2015 07:28:00 GMT";
    let only_color = |m: Option<&std::collections::HashMap<String, String>>| {
        let keys: Vec<&String> = m.unwrap().keys().collect();
        assert_eq!(keys, ["color"]);
    };

    let head = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(head.cache_control(), Some("max-age=60"));
    assert_eq!(
        head.content_disposition(),
        Some("attachment; filename=\"a.txt\"")
    );
    assert_eq!(head.content_encoding(), Some("gzip"));
    assert_eq!(head.content_language(), Some("zh-TW"));
    assert_eq!(head.expires_string(), Some(expires));
    only_color(head.metadata());

    let got = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(got.cache_control(), Some("max-age=60"));
    assert_eq!(
        got.content_disposition(),
        Some("attachment; filename=\"a.txt\"")
    );
    assert_eq!(got.content_encoding(), Some("gzip"));
    assert_eq!(got.content_language(), Some("zh-TW"));
    assert_eq!(got.expires_string(), Some(expires));
    only_color(got.metadata());
}

#[tokio::test]
async fn streamed_upload_does_not_report_aws_chunked_as_content_encoding() {
    // The SDK frames a streamed body as aws-chunked and says so in Content-Encoding. That
    // describes the request, not the object: a GET repeating it would tell clients to undo an
    // encoding the stored bytes never had.
    let server = server().await;
    let dir = tempfile::tempdir().unwrap();
    server
        .client
        .put_object()
        .bucket(BUCKET)
        .key("k")
        .body(streamed_body(&dir, b"hello").await)
        .send()
        .await
        .unwrap();
    let head = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(head.content_encoding(), None);
    let got = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(got.content_encoding(), None);
}

#[tokio::test]
async fn content_encoding_on_a_streamed_sdk_upload_is_stored_without_aws_chunked() {
    // The Rust SDK sends a streamed body's Content-Encoding and its own aws-chunked as two
    // header lines, which s3s alone refuses as a duplicate header. Uploading a pre-compressed
    // file is common, so the object must keep its encoding, and only it.
    let server = server().await;
    let dir = tempfile::tempdir().unwrap();
    server
        .client
        .put_object()
        .bucket(BUCKET)
        .key("k")
        .content_encoding("gzip")
        .body(streamed_body(&dir, b"hello").await)
        .send()
        .await
        .unwrap();
    let head = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(head.content_encoding(), Some("gzip"));
    assert_eq!(get(&server, "k").await, b"hello");
}

#[tokio::test]
async fn response_overrides_replace_stored_headers_on_get_and_head() {
    // Presigned download links use response-* to make a browser save the object under a
    // chosen name and type, whatever was stored with it. A HEAD of such a link must show what
    // its GET will carry.
    let server = server().await;
    server
        .client
        .put_object()
        .bucket(BUCKET)
        .key("k")
        .content_type("text/plain")
        .cache_control("no-cache")
        .content_disposition("inline")
        .content_language("en")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap();
    let got = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .response_content_type("text/csv")
        .response_cache_control("max-age=5")
        .response_content_disposition("attachment; filename=\"report.csv\"")
        .response_content_encoding("identity")
        .response_content_language("zh-TW")
        .response_expires(DateTime::from_secs(1_445_412_480))
        .send()
        .await
        .unwrap();
    assert_eq!(got.content_type(), Some("text/csv"));
    assert_eq!(got.cache_control(), Some("max-age=5"));
    assert_eq!(
        got.content_disposition(),
        Some("attachment; filename=\"report.csv\"")
    );
    assert_eq!(got.content_encoding(), Some("identity"));
    assert_eq!(got.content_language(), Some("zh-TW"));
    assert_eq!(got.expires_string(), Some("Wed, 21 Oct 2015 07:28:00 GMT"));

    let head = || {
        server
            .client
            .head_object()
            .bucket(BUCKET)
            .key("k")
            .response_cache_control("max-age=5")
    };
    let headed = head()
        .response_content_type("text/csv")
        .response_content_disposition("attachment; filename=\"report.csv\"")
        .response_content_encoding("identity")
        .response_content_language("zh-TW")
        .response_expires(DateTime::from_secs(1_445_412_480))
        .send()
        .await
        .unwrap();
    assert_eq!(headed.content_type(), Some("text/csv"));
    assert_eq!(headed.cache_control(), Some("max-age=5"));
    assert_eq!(
        headed.content_disposition(),
        Some("attachment; filename=\"report.csv\"")
    );
    assert_eq!(headed.content_encoding(), Some("identity"));
    assert_eq!(headed.content_language(), Some("zh-TW"));
    assert_eq!(
        headed.expires_string(),
        Some("Wed, 21 Oct 2015 07:28:00 GMT")
    );
    // What is not overridden is what was stored.
    let headed = head().send().await.unwrap();
    assert_eq!(headed.content_type(), Some("text/plain"));
    assert_eq!(headed.content_disposition(), Some("inline"));
    // A value no header can carry is the client's error, not a 500.
    let err = head()
        .response_content_type("text/plain\nX-Injected: 1")
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&err), 400, "{err:?}");
}

// Stored checksums.

#[tokio::test]
async fn stored_checksum_is_returned_only_in_checksum_mode() {
    // S3 returns checksums only when asked. SDKs ask on every GET and validate the body against
    // the answer, which gives downloads end-to-end integrity.
    let server = server().await;
    put(&server, "k", b"hello world").await;

    let plain = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(plain.checksum_crc32(), None);
    assert_eq!(plain.checksum_type(), None);

    let head = head_with_checksums(&server, "k").await;
    assert_eq!(head.checksum_crc32(), Some("DUoRhQ=="));
    // A single write's checksum covers the whole body; there are no parts to combine.
    assert_eq!(head.checksum_type(), Some(&ChecksumType::FullObject));

    let got = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(got.checksum_crc32(), Some("DUoRhQ=="));
    assert_eq!(
        &got.body.collect().await.unwrap().into_bytes()[..],
        b"hello world"
    );
}

#[tokio::test]
async fn sdk_rejects_a_body_that_does_not_match_the_returned_checksum() {
    // Proves the SDK really validates what s3dal returns, so every test here that reads a body
    // successfully also proves the checksum returned with it is right for that body.
    let server = server().await;
    let wrong = Computed {
        checksum: Checksum {
            checksum_crc32: Some(crc32(b"something else")),
            ..Default::default()
        },
        ..Default::default()
    };
    plant_with(&server, "k", b"hello world", wrong).await;
    let got = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    let err = got.body.collect().await.unwrap_err();
    assert!(format!("{err:?}").contains("hecksum"), "{err:?}");
}

#[tokio::test]
async fn ranged_and_part_reads_carry_no_full_object_checksum() {
    // The SDK validates whatever checksum comes back against the bytes it received. The whole
    // object's checksum on a range or a part would fail every correct partial download.
    let server = server().await;
    put(&server, "k", b"hello world").await;
    let ranged = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .range("bytes=0-4")
        .send()
        .await
        .unwrap();
    assert_eq!(ranged.checksum_crc32(), None);
    assert_eq!(
        &ranged.body.collect().await.unwrap().into_bytes()[..],
        b"hello"
    );

    let data = b"aaaaabbbbbcc";
    let with_checksum = Computed {
        checksum: Checksum {
            checksum_crc32: Some(crc32(data)),
            ..Default::default()
        },
        ..parts(&[5, 5, 2])
    };
    plant_with(&server, "mp", data, with_checksum).await;
    let part = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("mp")
        .part_number(1)
        .send()
        .await
        .unwrap();
    assert_eq!(part.checksum_crc32(), None);
    assert_eq!(
        &part.body.collect().await.unwrap().into_bytes()[..],
        b"aaaaa"
    );
}

#[tokio::test]
async fn header_checksum_on_a_streamed_body_is_stored_and_returned() {
    // A checksum sent as a header is known before the write opens, so even a body too large to
    // buffer has it stored (C1), and later downloads are validated against it.
    let server = server_with_buffer_limit(16).await;
    let data = patterned(STREAMED_LEN);
    let out = server
        .client
        .put_object()
        .bucket(BUCKET)
        .key("k")
        .body(ByteStream::from(data.clone()))
        .send()
        .await
        .unwrap();
    // A synthesized ETag shows the body took the streaming path.
    assert!(out.e_tag().unwrap().contains('-'), "{out:?}");
    assert_eq!(out.checksum_crc32(), Some(crc32(&data).as_str()));

    let head = head_with_checksums(&server, "k").await;
    assert_eq!(head.checksum_crc32(), out.checksum_crc32());
    let got = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(got.checksum_crc32(), out.checksum_crc32());
    assert_eq!(
        got.body.collect().await.unwrap().into_bytes().to_vec(),
        data
    );
}

#[tokio::test]
async fn trailer_checksum_on_a_buffered_body_is_stored() {
    // File uploads send their checksum as a trailer. A body small enough to buffer has its
    // trailer in hand before the write opens, so the checksum is stored like a header value.
    let server = server().await;
    let dir = tempfile::tempdir().unwrap();
    let out = server
        .client
        .put_object()
        .bucket(BUCKET)
        .key("k")
        .body(streamed_body(&dir, b"hello world").await)
        .send()
        .await
        .unwrap();
    assert_eq!(out.checksum_crc32(), Some("DUoRhQ=="));
    let head = head_with_checksums(&server, "k").await;
    assert_eq!(head.checksum_crc32(), Some("DUoRhQ=="));
}

#[tokio::test]
async fn trailer_checksum_on_a_streamed_body_is_verified_but_not_stored() {
    // C1: a trailer arrives after the body, when the writer's metadata is already fixed. The
    // upload is still verified, and the PUT echoes the checksum it checked; nothing is stored,
    // so a later GET carries no checksum rather than a wrong one.
    let server = server_with_buffer_limit(16).await;
    let dir = tempfile::tempdir().unwrap();
    let data = patterned(STREAMED_LEN);
    let out = server
        .client
        .put_object()
        .bucket(BUCKET)
        .key("k")
        .body(streamed_body(&dir, &data).await)
        .send()
        .await
        .unwrap();
    assert!(out.e_tag().unwrap().contains('-'), "{out:?}");
    assert_eq!(out.checksum_crc32(), Some(crc32(&data).as_str()));

    let head = head_with_checksums(&server, "k").await;
    assert_eq!(head.checksum_crc32(), None);
    assert_eq!(get(&server, "k").await, data);
}

#[tokio::test]
async fn a_streamed_body_whose_trailer_checksum_does_not_match_is_refused() {
    // With an unsigned payload the trailer is all that guards the data, and s3s leaves the
    // comparison to the server. The SDK always sends the right value, so the request is built by
    // hand; the right value on the same request shows that only the value decides.
    let server = server_with_buffer_limit(16).await;
    let data = patterned(4096);
    let put = |key: &'static str, crc32: String| {
        let (body, headers) = common::aws_chunked(&data, &format!("x-amz-checksum-crc32:{crc32}"));
        let endpoint = server.endpoint.clone();
        async move {
            let headers: Vec<(&str, &str)> =
                headers.iter().map(|(k, v)| (*k, v.as_str())).collect();
            common::signed_request(
                &endpoint,
                "PUT",
                &format!("/{BUCKET}/{key}"),
                "",
                &headers,
                "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
                &body,
            )
            .await
        }
    };
    let (code, body) = put("bad", "AAAAAA==".to_owned()).await;
    let body = String::from_utf8_lossy(&body);
    assert_eq!(code, 400, "{body}");
    assert!(body.contains("BadDigest"), "{body}");
    let err = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("bad")
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&err), 404);
    server.assert_no_temp_files().await;

    let (code, body) = put("good", crc32(&data)).await;
    assert_eq!(code, 200, "{}", String::from_utf8_lossy(&body));
    assert_eq!(get(&server, "good").await, data);
}

// partNumber on GET and HEAD.

#[tokio::test]
async fn part_number_serves_exactly_the_bytes_of_each_part() {
    // SDK transfer managers download a multipart object part by part and reassemble it; each
    // request must return exactly the bytes uploaded as that part, and the part count.
    let server = server().await;
    plant_with(&server, "mp", b"aaaaabbbbbcc", parts(&[5, 5, 2])).await;
    for (n, body, content_range) in [
        (1, "aaaaa", "bytes 0-4/12"),
        (2, "bbbbb", "bytes 5-9/12"),
        (3, "cc", "bytes 10-11/12"),
    ] {
        let out = server
            .client
            .get_object()
            .bucket(BUCKET)
            .key("mp")
            .part_number(n)
            .send()
            .await
            .unwrap();
        assert_eq!(out.parts_count(), Some(3), "{n}");
        assert_eq!(out.content_range(), Some(content_range), "{n}");
        assert_eq!(out.content_length(), Some(body.len() as i64), "{n}");
        let got = out.body.collect().await.unwrap().into_bytes();
        assert_eq!(&got[..], body.as_bytes(), "{n}");
    }
}

#[tokio::test]
async fn head_with_part_number_describes_that_part() {
    // Transfer managers (the Java v1 SDK's, for one) HEAD with partNumber to learn the part
    // count and each part's size and position before downloading.
    let server = server().await;
    plant_with(&server, "mp", b"aaaaabbbbbcc", parts(&[5, 5, 2])).await;
    for (n, len, content_range) in [(2, 5, "bytes 5-9/12"), (3, 2, "bytes 10-11/12")] {
        let head = server
            .client
            .head_object()
            .bucket(BUCKET)
            .key("mp")
            .part_number(n)
            .send()
            .await
            .unwrap();
        assert_eq!(head.parts_count(), Some(3), "{n}");
        assert_eq!(head.content_length(), Some(len), "{n}");
        assert_eq!(head.content_range(), Some(content_range), "{n}");
    }
    // Without partNumber, HEAD describes the whole object and no parts.
    let head = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("mp")
        .send()
        .await
        .unwrap();
    assert_eq!(head.content_length(), Some(12));
    assert_eq!(head.parts_count(), None);
    assert_eq!(head.content_range(), None);
}

#[tokio::test]
async fn part_number_beyond_the_part_count_is_invalid_part_number() {
    // Downloaders stop at this error or at the reported count. A 200 would hand them bytes
    // that are not a part; a 500 would make them retry.
    let server = server().await;
    plant_with(&server, "mp", b"aaaaabbbbbcc", parts(&[5, 5, 2])).await;
    put(&server, "single", b"hello").await;
    for (key, n) in [("mp", 4), ("single", 2)] {
        let err = server
            .client
            .get_object()
            .bucket(BUCKET)
            .key(key)
            .part_number(n)
            .send()
            .await
            .unwrap_err();
        assert_eq!(status(&err), 416, "{key}");
        assert_eq!(err.code(), Some("InvalidPartNumber"), "{key}: {err:?}");
    }
    let head = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("mp")
        .part_number(4)
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&head), 416);
}

#[tokio::test]
async fn part_number_with_a_range_is_invalid_request() {
    // AWS refuses the combination instead of picking one; serving either would return bytes
    // the client may not have meant.
    let server = server().await;
    put(&server, "k", b"hello").await;
    let err = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .part_number(1)
        .range("bytes=0-1")
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&err), 400);
    assert_eq!(err.code(), Some("InvalidRequest"), "{err:?}");
    let head = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("k")
        .part_number(1)
        .range("bytes=0-1")
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&head), 400);
}

#[tokio::test]
async fn part_number_outside_1_to_10000_is_invalid_argument() {
    // No upload can have such a part; the request is malformed, not unsatisfiable.
    let server = server().await;
    put(&server, "k", b"hello").await;
    for n in [0, -1, 10_001] {
        let err = server
            .client
            .get_object()
            .bucket(BUCKET)
            .key("k")
            .part_number(n)
            .send()
            .await
            .unwrap_err();
        assert_eq!(err.code(), Some("InvalidArgument"), "{n}: {err:?}");
    }
}

#[tokio::test]
async fn part_number_on_irregular_parts_is_not_implemented() {
    // Only uniform part sizes are stored. Guessing the boundaries would hand out wrong bytes
    // under a successful status.
    let server = server().await;
    plant_with(&server, "mp", b"aaaaabbbbbbcc", parts(&[5, 6, 2])).await;
    let err = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("mp")
        .part_number(1)
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("NotImplemented"), "{err:?}");
    // The object itself is still readable.
    assert_eq!(get(&server, "mp").await, b"aaaaabbbbbbcc");
}

#[tokio::test]
async fn part_number_1_of_a_single_part_object_is_the_whole_object() {
    // Downloaders ask for part 1 before they know how an object was uploaded. An object from a
    // single PUT must come back whole and without a part count, so they stop there; its
    // checksum still applies, because the response is the whole object.
    let server = server().await;
    put(&server, "k", b"hello world").await;
    let got = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .part_number(1)
        .send()
        .await
        .unwrap();
    assert_eq!(got.parts_count(), None);
    assert_eq!(got.content_range(), Some("bytes 0-10/11"));
    assert_eq!(got.checksum_crc32(), Some("DUoRhQ=="));
    assert_eq!(
        &got.body.collect().await.unwrap().into_bytes()[..],
        b"hello world"
    );
    let head = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("k")
        .part_number(1)
        .send()
        .await
        .unwrap();
    assert_eq!(head.content_length(), Some(11));
    assert_eq!(head.parts_count(), None);
}

// GetObjectAttributes.

const ALL_ATTRIBUTES: [ObjectAttributes; 5] = [
    ObjectAttributes::Etag,
    ObjectAttributes::Checksum,
    ObjectAttributes::ObjectParts,
    ObjectAttributes::StorageClass,
    ObjectAttributes::ObjectSize,
];

async fn attributes(
    server: &TestServer,
    key: &str,
    wanted: &[ObjectAttributes],
) -> GetObjectAttributesOutput {
    server
        .client
        .get_object_attributes()
        .bucket(BUCKET)
        .key(key)
        .set_object_attributes(Some(wanted.to_vec()))
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn object_attributes_describe_a_single_put_as_head_does() {
    // Sync and backup tools ask for size, ETag and checksum without downloading, and compare
    // them with what they hold; any value that disagrees with HEAD makes them copy again or
    // report corruption. Only the attributes asked for come back.
    let server = server().await;
    let etag = put(&server, "k", b"hello world").await;
    let head = head_with_checksums(&server, "k").await;

    let all = attributes(&server, "k", &ALL_ATTRIBUTES).await;
    // The XML element carries the ETag without quotes, as on S3.
    assert_eq!(all.e_tag().map(quoted), Some(etag));
    assert_eq!(all.object_size(), Some(11));
    assert_eq!(all.storage_class(), Some(&StorageClass::Standard));
    let checksum = all.checksum().unwrap();
    assert_eq!(checksum.checksum_crc32(), Some("DUoRhQ=="));
    assert_eq!(checksum.checksum_type(), Some(&ChecksumType::FullObject));
    assert_eq!(checksum.checksum_crc32(), head.checksum_crc32());
    // An object from a single PUT has no parts, as on S3.
    assert!(all.object_parts().is_none(), "{all:?}");
    assert_eq!(all.last_modified(), head.last_modified());
    assert_eq!(all.version_id(), None);

    let size_only = attributes(&server, "k", &[ObjectAttributes::ObjectSize]).await;
    assert_eq!(size_only.object_size(), Some(11));
    assert_eq!(size_only.e_tag(), None);
    assert!(size_only.checksum().is_none());
    assert_eq!(size_only.storage_class(), None);

    // The Rust SDK sends one header line per attribute; botocore and others send one
    // comma-separated line, which must name the same attributes.
    let joined = server
        .client
        .get_object_attributes()
        .bucket(BUCKET)
        .key("k")
        .object_attributes(ObjectAttributes::Etag)
        .customize()
        .mutate_request(|req| {
            req.headers_mut()
                .insert("x-amz-object-attributes", "ETag, ObjectSize");
        })
        .send()
        .await
        .unwrap();
    assert_eq!(joined.object_size(), Some(11));
    assert!(joined.e_tag().is_some());
    assert!(joined.checksum().is_none());
}

#[tokio::test]
async fn object_attributes_list_part_sizes_page_by_page() {
    // Parallel downloaders and s3-tests read each part's size from here to plan ranged reads;
    // a size or page that disagrees with partNumber reads would split the object wrongly.
    let server = server().await;
    plant_with(&server, "mp", b"aaaaabbbbbcc", parts(&[5, 5, 2])).await;
    let sizes = |parts: &[aws_sdk_s3::types::ObjectPart]| -> Vec<(i32, i64)> {
        parts
            .iter()
            .map(|p| (p.part_number().unwrap(), p.size().unwrap()))
            .collect()
    };

    let all = attributes(&server, "mp", &ALL_ATTRIBUTES).await;
    let parts_of = all.object_parts().unwrap();
    assert_eq!(parts_of.total_parts_count(), Some(3));
    assert_eq!(sizes(parts_of.parts()), [(1, 5), (2, 5), (3, 2)]);
    assert_eq!(parts_of.is_truncated(), Some(false));
    // No checksum is stored per part, so none is reported.
    assert!(
        parts_of
            .parts()
            .iter()
            .all(|p| p.checksum_crc32().is_none())
    );
    assert_eq!(all.object_size(), Some(12));

    let page = server
        .client
        .get_object_attributes()
        .bucket(BUCKET)
        .key("mp")
        .object_attributes(ObjectAttributes::ObjectParts)
        .max_parts(1)
        .part_number_marker("1")
        .send()
        .await
        .unwrap();
    let page = page.object_parts().unwrap();
    assert_eq!(sizes(page.parts()), [(2, 5)]);
    assert_eq!(page.is_truncated(), Some(true));
    assert_eq!(page.next_part_number_marker(), Some("2"));
    assert_eq!(page.part_number_marker(), Some("1"));
    assert_eq!(page.max_parts(), Some(1));
    assert_eq!(page.total_parts_count(), Some(3));

    // Boundaries of irregular parts are not stored; the count is still known.
    plant_with(&server, "irregular", b"aaaaabbbbbbcc", parts(&[5, 6, 2])).await;
    let irregular = attributes(&server, "irregular", &[ObjectAttributes::ObjectParts]).await;
    let irregular = irregular.object_parts().unwrap();
    assert_eq!(irregular.total_parts_count(), Some(3));
    assert!(irregular.parts().is_empty(), "{irregular:?}");
}

#[tokio::test]
async fn object_attributes_follow_the_rules_of_a_read() {
    // GetObjectAttributes is a read of the object: what GET refuses or answers conditionally,
    // it must too, or a client could learn about a version, a key or a state it was told is
    // not there.
    let server = server().await;
    let etag = put(&server, "k", b"v1").await;
    let request = |key: &str| {
        server
            .client
            .get_object_attributes()
            .bucket(BUCKET)
            .key(key)
            .object_attributes(ObjectAttributes::ObjectSize)
    };

    let missing = request("absent").send().await.unwrap_err();
    assert_eq!(status(&missing), 404);
    assert_eq!(missing.code(), Some("NoSuchKey"), "{missing:?}");
    let version = request("k")
        .version_id("3sL4kqtJlcpXroDTDmJ-rmSpXd3dIbrHY")
        .send()
        .await
        .unwrap_err();
    assert_eq!(version.code(), Some("NoSuchVersion"), "{version:?}");
    let null = request("k").version_id("null").send().await.unwrap();
    assert_eq!(null.object_size(), Some(2));
    let bucket = server
        .client
        .get_object_attributes()
        .bucket("nosuchbucket")
        .key("k")
        .object_attributes(ObjectAttributes::ObjectSize)
        .send()
        .await
        .unwrap_err();
    assert_eq!(bucket.code(), Some("NoSuchBucket"), "{bucket:?}");

    let invalid = request("k")
        .object_attributes(ObjectAttributes::from("Bogus"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&invalid), 400);
    assert_eq!(invalid.code(), Some("InvalidArgument"), "{invalid:?}");

    let (key, md5) = sse_c_key();
    let sse_c = request("k")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(key)
        .sse_customer_key_md5(md5)
        .send()
        .await
        .unwrap_err();
    assert_eq!(sse_c.code(), Some("NotImplemented"), "{sse_c:?}");

    // The SDK has no setters for the conditional headers; S3 honors them as on GET.
    let with_header = |name: &'static str, value: String| {
        request("k").customize().mutate_request(move |req| {
            req.headers_mut().insert(name, value.clone());
        })
    };
    let stale = with_header("if-match", quoted("0123"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&stale), 412, "{stale:?}");
    let cached = with_header("if-none-match", etag.clone())
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&cached), 304, "{cached:?}");
    let current = with_header("if-match", etag).send().await.unwrap();
    assert_eq!(current.object_size(), Some(2));
}

// Conditional writes.

// Returned as the SDK returns it, so tests inspect it the same way; its size does not
// matter in a test.
#[allow(clippy::result_large_err)]
async fn put_if(
    server: &TestServer,
    key: &str,
    data: &'static [u8],
    if_match: Option<&str>,
    if_none_match: Option<&str>,
) -> Result<String, SdkError<aws_sdk_s3::operation::put_object::PutObjectError, HttpResponse>> {
    let out = server
        .client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .set_if_match(if_match.map(str::to_owned))
        .set_if_none_match(if_none_match.map(str::to_owned))
        .body(ByteStream::from_static(data))
        .send()
        .await?;
    Ok(out.e_tag().unwrap().to_owned())
}

#[tokio::test]
async fn if_none_match_star_creates_only_once() {
    // Lock files and idempotent producers write with If-None-Match: * so that an existing
    // object is never overwritten.
    let server = server().await;
    put_if(&server, "k", b"first", None, Some("*"))
        .await
        .unwrap();
    let err = put_if(&server, "k", b"second", None, Some("*"))
        .await
        .unwrap_err();
    assert_eq!(status(&err), 412);
    assert_eq!(err.code(), Some("PreconditionFailed"), "{err:?}");
    assert_eq!(get(&server, "k").await, b"first");
}

#[tokio::test]
async fn concurrent_if_none_match_puts_have_exactly_one_winner() {
    // The check and the commit must be atomic: if two creators could both pass the check, both
    // would believe they own the key. The bodies stream (above the buffer limit), so the writes
    // overlap and only their commits are serialized.
    let server = server_with_buffer_limit(16).await;
    let bodies: Vec<Vec<u8>> = (0..16u8).map(|i| vec![i; 256 * 1024]).collect();
    let tasks: Vec<_> = bodies
        .iter()
        .map(|body| {
            let request = server
                .client
                .put_object()
                .bucket(BUCKET)
                .key("lock")
                .if_none_match("*")
                .body(ByteStream::from(body.clone()));
            tokio::spawn(request.send())
        })
        .collect();
    let mut winners = Vec::new();
    for (i, task) in tasks.into_iter().enumerate() {
        match task.await.unwrap() {
            Ok(_) => winners.push(i),
            Err(err) => assert_eq!(err.code(), Some("PreconditionFailed"), "{i}: {err:?}"),
        }
    }
    assert_eq!(winners.len(), 1, "{winners:?}");
    assert_eq!(get(&server, "lock").await, bodies[winners[0]]);
    server.assert_no_temp_files().await;
}

#[tokio::test]
async fn a_create_fails_when_another_process_writes_the_key_during_its_upload() {
    // s3dal's lock covers one process. Another gateway on the same backend, or any other
    // writer, may create the key while a create's body is still arriving; the create must then
    // fail instead of replacing that object. Whether that write lands before or after s3dal
    // opens its writer decides which check catches it, never the outcome. The body streams
    // (above the buffer limit), so the writer is open while it arrives.
    let server = server_with_buffer_limit(16).await;
    let put = server
        .client
        .put_object()
        .bucket(BUCKET)
        .key("lock")
        .if_none_match("*")
        .presigned(PresigningConfig::expires_in(Duration::from_secs(300)).unwrap())
        .await
        .unwrap();
    let body = patterned(128 * 1024);
    let (first, rest) = body.split_at(64 * 1024);

    // By hand rather than through the SDK, to pause between the two halves of the body.
    let host = server.endpoint.trim_start_matches("http://");
    let mut stream = TcpStream::connect(host).await.unwrap();
    let mut head = format!(
        "PUT {} HTTP/1.1\r\nhost: {host}\r\ncontent-length: {}\r\nconnection: close\r\n",
        put.uri().strip_prefix(&server.endpoint).unwrap(),
        body.len()
    );
    for (name, value) in put.headers() {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(first).await.unwrap();
    server.plant(BUCKET, "lock", b"theirs").await;
    stream.write_all(rest).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();

    let response = String::from_utf8_lossy(&response);
    assert!(response.starts_with("HTTP/1.1 412"), "{response}");
    assert!(
        response.contains("<Code>PreconditionFailed</Code>"),
        "{response}"
    );
    assert_eq!(server.read(&format!("{BUCKET}/lock")).await, b"theirs");
    server.assert_no_temp_files().await;
}

#[tokio::test]
async fn if_match_put_replaces_only_the_expected_etag() {
    // Optimistic concurrency: a read-modify-write must fail when someone else wrote in
    // between, instead of silently discarding their write.
    let server = server().await;
    let v1 = put(&server, "k", b"v1").await;
    let v2 = put_if(&server, "k", b"v2", Some(&v1), None).await.unwrap();
    assert_eq!(v2, quoted(&md5_hex(b"v2")));
    let err = put_if(&server, "k", b"v3", Some(&v1), None)
        .await
        .unwrap_err();
    assert_eq!(status(&err), 412);
    assert_eq!(err.code(), Some("PreconditionFailed"), "{err:?}");
    assert_eq!(get(&server, "k").await, b"v2");
}

#[tokio::test]
async fn if_match_put_on_a_missing_key_is_no_such_key() {
    // S3 answers 404, which tells the client the object is gone rather than changed; and the
    // write must not create the key it was only allowed to replace.
    let server = server().await;
    let err = put_if(&server, "dir/k", b"v1", Some("\"abc\""), None)
        .await
        .unwrap_err();
    assert_eq!(status(&err), 404);
    assert_eq!(err.code(), Some("NoSuchKey"), "{err:?}");
    assert_eq!(server.names(BUCKET).await, Vec::<String>::new());
    server.assert_no_temp_files().await;
}

#[tokio::test]
async fn if_none_match_put_with_an_etag_is_not_implemented() {
    // S3 accepts only `*` on writes; taking an ETag would promise a check that never runs.
    let server = server().await;
    let err = put_if(&server, "k", b"v1", None, Some("\"abc\""))
        .await
        .unwrap_err();
    assert_eq!(status(&err), 501);
    assert_eq!(err.code(), Some("NotImplemented"), "{err:?}");
    assert_eq!(server.names(BUCKET).await, Vec::<String>::new());
}

#[tokio::test]
async fn conditional_folder_marker_put_is_not_implemented() {
    // A folder marker is a directory, created without a commit the condition could guard.
    let server = server().await;
    let err = put_if(&server, "dir/", b"", None, Some("*"))
        .await
        .unwrap_err();
    assert_eq!(status(&err), 501);
    assert_eq!(err.code(), Some("NotImplemented"), "{err:?}");
    assert!(!server.exists("bkt/dir/").await);
}

#[tokio::test]
async fn delete_with_if_match_deletes_only_the_expected_etag() {
    // A cleanup job must not delete an object that was replaced after it decided to.
    let server = server().await;
    let etag = put(&server, "k", b"v1").await;
    let err = server
        .client
        .delete_object()
        .bucket(BUCKET)
        .key("k")
        .if_match("\"not-the-etag\"")
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&err), 412);
    assert_eq!(err.code(), Some("PreconditionFailed"), "{err:?}");
    assert_eq!(get(&server, "k").await, b"v1");

    server
        .client
        .delete_object()
        .bucket(BUCKET)
        .key("k")
        .if_match(&etag)
        .send()
        .await
        .unwrap();
    let err = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("NoSuchKey"));
}

#[tokio::test]
async fn delete_with_if_match_on_a_missing_key_is_no_such_key() {
    // Unlike a plain delete, a conditional one reports that its object is gone, as S3 does.
    let server = server().await;
    let err = server
        .client
        .delete_object()
        .bucket(BUCKET)
        .key("absent")
        .if_match("\"abc\"")
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&err), 404);
    assert_eq!(err.code(), Some("NoSuchKey"), "{err:?}");
}

#[tokio::test]
async fn delete_with_a_size_or_mtime_condition_deletes_only_a_matching_object() {
    // A cleanup job that recorded an object's size and Last-Modified must not delete one that
    // replaced it since. A key that is already gone succeeds, as AWS answers it with 204, so a
    // retried cleanup does not fail.
    let server = server().await;
    put(&server, "k", b"v1").await;
    let head = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    let mtime = *head.last_modified().unwrap();
    let delete = || server.client.delete_object().bucket(BUCKET).key("k");

    let by_size = delete().if_match_size(999).send().await.unwrap_err();
    assert_eq!(status(&by_size), 412);
    assert_eq!(by_size.code(), Some("PreconditionFailed"), "{by_size:?}");
    let earlier = DateTime::from_secs(mtime.secs() - 1);
    let by_mtime = delete()
        .if_match_last_modified_time(earlier)
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&by_mtime), 412);
    // Every condition must hold, not just one of them.
    let mixed = delete()
        .if_match_size(2)
        .if_match_last_modified_time(earlier)
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&mixed), 412);
    assert_eq!(get(&server, "k").await, b"v1");

    delete()
        .if_match_size(2)
        .if_match_last_modified_time(mtime)
        .send()
        .await
        .unwrap();
    assert!(!server.exists("bkt/k").await);
    delete().if_match_size(999).send().await.unwrap();
    delete()
        .if_match_last_modified_time(earlier)
        .send()
        .await
        .unwrap();

    // A folder marker is not an object to GET, nor to a condition: one that holds because no
    // object is there must not remove the marker.
    put(&server, "dir/", b"").await;
    server
        .client
        .delete_object()
        .bucket(BUCKET)
        .key("dir/")
        .if_match_size(0)
        .send()
        .await
        .unwrap();
    assert!(server.exists("bkt/dir/").await);
}

// Request features s3dal cannot honor.

type PutCase = (
    &'static str,
    fn(PutObjectFluentBuilder) -> PutObjectFluentBuilder,
);

/// A 32-byte SSE-C key and its MD5, both base64, as S3 expects them.
fn sse_c_key() -> (String, String) {
    let key = [7u8; 32];
    (
        STANDARD.encode_to_string(key),
        STANDARD.encode_to_string(Md5::digest(key)),
    )
}

#[tokio::test]
async fn put_refuses_headers_it_cannot_honor_and_writes_nothing() {
    // A client that asked for encryption, a retention lock, tags, access for others, a
    // redirect or an append must not get a 200 for an object that has none of them.
    let server = server().await;
    let cases: [PutCase; 11] = [
        ("SSE-S3", |b| {
            b.server_side_encryption(ServerSideEncryption::Aes256)
        }),
        ("SSE-KMS", |b| {
            b.server_side_encryption(ServerSideEncryption::AwsKms)
                .ssekms_key_id("key")
        }),
        ("bucket key", |b| b.bucket_key_enabled(true)),
        ("SSE-C", |b| {
            let (key, md5) = sse_c_key();
            b.sse_customer_algorithm("AES256")
                .sse_customer_key(key)
                .sse_customer_key_md5(md5)
        }),
        ("retention", |b| {
            b.object_lock_mode(ObjectLockMode::Governance)
                .object_lock_retain_until_date(DateTime::from_secs(4_000_000_000))
        }),
        ("legal hold", |b| {
            b.object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
        }),
        ("tagging", |b| b.tagging("team=a")),
        ("public ACL", |b| b.acl(ObjectCannedAcl::PublicRead)),
        ("grant", |b| b.grant_read("id=someone-else")),
        ("redirect", |b| b.website_redirect_location("/elsewhere")),
        ("append", |b| b.write_offset_bytes(0)),
    ];
    for (name, apply) in cases {
        let request = server
            .client
            .put_object()
            .bucket(BUCKET)
            .key("k")
            .body(ByteStream::from_static(b"x"));
        let err = apply(request).send().await.unwrap_err();
        assert_eq!(status(&err), 501, "{name}: {err:?}");
        assert_eq!(err.code(), Some("NotImplemented"), "{name}: {err:?}");
    }
    assert_eq!(server.names(BUCKET).await, Vec::<String>::new());
}

#[tokio::test]
async fn object_tags_read_as_empty_and_cannot_be_set() {
    // Tags are refused wherever they would be stored, so every object's tag set is empty. Tools
    // read it before copying (the aws CLI before every multipart copy) and fail without it.
    let server = server().await;
    put(&server, "k", b"x").await;
    let tags = server
        .client
        .get_object_tagging()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    assert!(tags.tag_set().is_empty(), "{tags:?}");

    let missing = server
        .client
        .get_object_tagging()
        .bucket(BUCKET)
        .key("absent")
        .send()
        .await
        .unwrap_err();
    assert_eq!(missing.code(), Some("NoSuchKey"), "{missing:?}");

    let tag = Tag::builder().key("team").value("a").build().unwrap();
    let set = server
        .client
        .put_object_tagging()
        .bucket(BUCKET)
        .key("k")
        .tagging(Tagging::builder().tag_set(tag).build().unwrap())
        .send()
        .await
        .unwrap_err();
    assert_eq!(set.code(), Some("NotImplemented"), "{set:?}");
}

#[tokio::test]
async fn object_acl_is_the_owners_full_control_and_writes_may_only_restate_it() {
    // Copy tools read an object's ACL to carry it over, and some set `private` or
    // `bucket-owner-full-control` after every upload. Reads must name no one but the owner,
    // writes that would share the object must fail, and writes that restate the owner-only
    // ACL succeed, by the rule PutObject applies to its ACL headers.
    let server = server().await;
    put(&server, "k", b"x").await;
    let acl = server
        .client
        .get_object_acl()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    let bucket_acl = server
        .client
        .get_bucket_acl()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    // One tenant owns the bucket and everything in it.
    assert_eq!(acl.owner(), bucket_acl.owner());
    assert_eq!(acl.grants(), bucket_acl.grants());
    assert_eq!(acl.grants().len(), 1);
    assert_eq!(acl.grants()[0].permission(), Some(&Permission::FullControl));

    let put_acl = |key: &str| server.client.put_object_acl().bucket(BUCKET).key(key);
    for canned in [
        ObjectCannedAcl::Private,
        ObjectCannedAcl::BucketOwnerFullControl,
    ] {
        put_acl("k").acl(canned).send().await.unwrap();
    }
    let public = put_acl("k")
        .acl(ObjectCannedAcl::PublicRead)
        .send()
        .await
        .unwrap_err();
    assert_eq!(public.code(), Some("NotImplemented"), "{public:?}");
    let unknown = put_acl("k")
        .acl(ObjectCannedAcl::from("public-ready"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(unknown.code(), Some("InvalidArgument"), "{unknown:?}");
    let grant = put_acl("k")
        .grant_read("uri=http://acs.amazonaws.com/groups/global/AllUsers")
        .send()
        .await
        .unwrap_err();
    assert_eq!(grant.code(), Some("NotImplemented"), "{grant:?}");

    let missing = put_acl("absent")
        .acl(ObjectCannedAcl::Private)
        .send()
        .await
        .unwrap_err();
    assert_eq!(missing.code(), Some("NoSuchKey"), "{missing:?}");
    let read_missing = server
        .client
        .get_object_acl()
        .bucket(BUCKET)
        .key("absent")
        .send()
        .await
        .unwrap_err();
    assert_eq!(read_missing.code(), Some("NoSuchKey"), "{read_missing:?}");
    let version = server
        .client
        .get_object_acl()
        .bucket(BUCKET)
        .key("k")
        .version_id("3sL4kqtJlcpXroDTDmJ-rmSpXd3dIbrHY")
        .send()
        .await
        .unwrap_err();
    assert_eq!(version.code(), Some("NoSuchVersion"), "{version:?}");
}

#[tokio::test]
async fn put_accepts_advisory_headers() {
    // Upload tools commonly send these. On a single-tenant gateway they change nothing a client
    // relies on, so refusing them would break ordinary uploads for no protection.
    let server = server().await;
    let cases: [PutCase; 5] = [
        ("private", |b| b.acl(ObjectCannedAcl::Private)),
        ("owner full control", |b| {
            b.acl(ObjectCannedAcl::BucketOwnerFullControl)
        }),
        ("owner read", |b| b.acl(ObjectCannedAcl::BucketOwnerRead)),
        ("storage class", |b| b.storage_class(StorageClass::Glacier)),
        ("request payer", |b| {
            b.request_payer(RequestPayer::Requester)
        }),
    ];
    for (name, apply) in cases {
        let request = server
            .client
            .put_object()
            .bucket(BUCKET)
            .key("k")
            .body(ByteStream::from_static(b"x"));
        apply(request)
            .send()
            .await
            .unwrap_or_else(|err| panic!("{name}: {err:?}"));
    }
    assert_eq!(get(&server, "k").await, b"x");
}

#[tokio::test]
async fn versions_other_than_null_do_not_exist() {
    // Without versioning, serving or deleting the current object for another version id would
    // act on data the client did not name; S3 answers a version it does not have with
    // NoSuchVersion. `null` is the id S3 gives the one version a key in an unversioned bucket
    // has, so it still works.
    let server = server().await;
    put(&server, "k", b"v1").await;
    let version = "3sL4kqtJlcpXroDTDmJ-rmSpXd3dIbrHY";
    let get_err = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .version_id(version)
        .send()
        .await
        .unwrap_err();
    assert_eq!(get_err.code(), Some("NoSuchVersion"), "{get_err:?}");
    let head_err = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("k")
        .version_id(version)
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&head_err), 404);
    let delete_err = server
        .client
        .delete_object()
        .bucket(BUCKET)
        .key("k")
        .version_id(version)
        .send()
        .await
        .unwrap_err();
    assert_eq!(delete_err.code(), Some("NoSuchVersion"), "{delete_err:?}");
    assert_eq!(get(&server, "k").await, b"v1");

    let null = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .version_id("null")
        .send()
        .await
        .unwrap();
    assert_eq!(&null.body.collect().await.unwrap().into_bytes()[..], b"v1");
    server
        .client
        .delete_object()
        .bucket(BUCKET)
        .key("k")
        .version_id("null")
        .send()
        .await
        .unwrap();
    assert_eq!(server.names(BUCKET).await, Vec::<String>::new());
}

#[tokio::test]
async fn reads_with_a_customer_encryption_key_are_not_implemented() {
    // Nothing is stored encrypted. A client that sends its key expects the object to be, and
    // must not be handed plaintext as if it were.
    let server = server().await;
    put(&server, "k", b"v1").await;
    let (key, md5) = sse_c_key();
    let get_err = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("k")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key)
        .sse_customer_key_md5(&md5)
        .send()
        .await
        .unwrap_err();
    assert_eq!(get_err.code(), Some("NotImplemented"), "{get_err:?}");
    let head_err = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("k")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(key)
        .sse_customer_key_md5(md5)
        .send()
        .await
        .unwrap_err();
    assert_eq!(status(&head_err), 501);
}

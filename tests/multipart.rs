mod common;

use std::time::{Duration, Instant, SystemTime};

use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::complete_multipart_upload::builders::CompleteMultipartUploadFluentBuilder;
use aws_sdk_s3::operation::complete_multipart_upload::{
    CompleteMultipartUploadError, CompleteMultipartUploadOutput,
};
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumMode, ChecksumType, CompletedMultipartUpload, CompletedPart,
    ObjectCannedAcl, ObjectLockMode, ServerSideEncryption, StorageClass,
};
use common::TestServer;
use md5::{Digest, Md5};
use s3dal::meta::md5_hex;

const BUCKET: &str = "bkt";
const MIB: usize = 1024 * 1024;

async fn server() -> TestServer {
    let server = TestServer::start().await;
    server.create_bucket_dir(BUCKET).await;
    server
}

async fn create(server: &TestServer, key: &str) -> String {
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

/// Uploads part `n` and returns its ETag as the SDK reports it (quoted).
async fn upload(server: &TestServer, key: &str, id: &str, n: i32, data: &[u8]) -> String {
    let out = server
        .client
        .upload_part()
        .bucket(BUCKET)
        .key(key)
        .upload_id(id)
        .part_number(n)
        .body(ByteStream::from(data.to_vec()))
        .send()
        .await
        .unwrap();
    out.e_tag().unwrap().to_owned()
}

fn parts<S: AsRef<str>>(list: &[(i32, S)]) -> CompletedMultipartUpload {
    CompletedMultipartUpload::builder()
        .set_parts(Some(
            list.iter()
                .map(|(n, etag)| {
                    CompletedPart::builder()
                        .part_number(*n)
                        .e_tag(etag.as_ref())
                        .build()
                })
                .collect(),
        ))
        .build()
}

type CompleteResult =
    Result<CompleteMultipartUploadOutput, SdkError<CompleteMultipartUploadError, HttpResponse>>;

// Returned as the SDK returns it, so tests inspect it the same way; its size does not
// matter in a test.
#[allow(clippy::result_large_err)]
async fn complete<S: AsRef<str>>(
    server: &TestServer,
    key: &str,
    id: &str,
    list: &[(i32, S)],
) -> CompleteResult {
    server
        .client
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key(key)
        .upload_id(id)
        .multipart_upload(parts(list))
        .send()
        .await
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

/// The part numbers and ETags ListParts reports for the upload.
async fn listed_parts(server: &TestServer, key: &str, id: &str) -> Vec<(i32, String)> {
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
        .map(|p| (p.part_number().unwrap(), p.e_tag().unwrap().to_owned()))
        .collect()
}

fn status<E>(err: &SdkError<E, HttpResponse>) -> u16 {
    err.raw_response().unwrap().status().as_u16()
}

fn quoted(etag: &str) -> String {
    format!("\"{etag}\"")
}

/// S3's multipart ETag computed independently of s3dal.
fn multipart_etag(parts: &[&[u8]]) -> String {
    let concat: Vec<u8> = parts.iter().flat_map(|p| Md5::digest(p).to_vec()).collect();
    quoted(&format!("{}-{}", md5_hex(&concat), parts.len()))
}

fn patterned(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

#[tokio::test]
async fn a_part_whose_trailer_checksum_does_not_match_is_refused() {
    // The part's checksum goes into its manifest and its response, and the client hands it back
    // at Complete, so a corrupted part taken here would complete as an object with a valid
    // checksum. The SDK always sends the right value, so the request is built by hand.
    let server = server().await;
    let id = create(&server, "k").await;
    let data = patterned(4096);
    let send = |crc32: &str| {
        let (body, headers) = common::aws_chunked(&data, &format!("x-amz-checksum-crc32:{crc32}"));
        let endpoint = server.endpoint.clone();
        let query = format!("partNumber=1&uploadId={id}");
        async move {
            let headers: Vec<(&str, &str)> =
                headers.iter().map(|(k, v)| (*k, v.as_str())).collect();
            common::signed_request(
                &endpoint,
                "PUT",
                &format!("/{BUCKET}/k"),
                &query,
                &headers,
                "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
                &body,
            )
            .await
        }
    };
    let (code, body) = send("AAAAAA==").await;
    let body = String::from_utf8_lossy(&body);
    assert_eq!(code, 400, "{body}");
    assert!(body.contains("BadDigest"), "{body}");
    assert_eq!(listed_parts(&server, "k", &id).await, []);

    let crc32 = {
        let mut hasher = s3s::checksum::ChecksumHasher {
            crc32: Some(Default::default()),
            ..Default::default()
        };
        hasher.update(&data);
        hasher.finalize().checksum_crc32.unwrap()
    };
    let (code, body) = send(&crc32).await;
    assert_eq!(code, 200, "{}", String::from_utf8_lossy(&body));
    assert_eq!(
        listed_parts(&server, "k", &id).await,
        [(1, quoted(&md5_hex(&data)))]
    );
}

#[tokio::test]
async fn two_part_upload_round_trips_with_the_multipart_etag_and_create_attrs() {
    // The object must be exactly the parts in order, carry the headers given at Create (not at
    // Complete), and report S3's multipart ETag, which aws CLI and rclone recompute to verify
    // the upload. Complete and HEAD must agree on it.
    let server = server().await;
    let first = patterned(5 * MIB);
    let second = b"tail of the object".to_vec();
    let id = server
        .client
        .create_multipart_upload()
        .bucket(BUCKET)
        .key("dir/big")
        .content_type("text/plain")
        .cache_control("max-age=60")
        .content_disposition("attachment; filename=\"big.txt\"")
        .content_encoding("gzip")
        .content_language("zh-TW")
        .expires(aws_sdk_s3::primitives::DateTime::from_secs(1_445_412_480))
        .metadata("color", "red")
        .send()
        .await
        .unwrap()
        .upload_id()
        .unwrap()
        .to_owned();
    let e1 = upload(&server, "dir/big", &id, 1, &first).await;
    let e2 = upload(&server, "dir/big", &id, 2, &second).await;
    assert_eq!(e1, quoted(&md5_hex(&first)));

    let out = complete(&server, "dir/big", &id, &[(1, &e1), (2, &e2)])
        .await
        .unwrap();
    let expected = multipart_etag(&[&first, &second]);
    assert_eq!(out.e_tag(), Some(expected.as_str()));
    assert_eq!(out.key(), Some("dir/big"));

    let head = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("dir/big")
        .send()
        .await
        .unwrap();
    assert_eq!(head.e_tag(), Some(expected.as_str()));
    assert_eq!(head.content_type(), Some("text/plain"));
    assert_eq!(head.cache_control(), Some("max-age=60"));
    assert_eq!(
        head.content_disposition(),
        Some("attachment; filename=\"big.txt\"")
    );
    assert_eq!(head.content_encoding(), Some("gzip"));
    assert_eq!(head.content_language(), Some("zh-TW"));
    assert_eq!(head.expires_string(), Some("Wed, 21 Oct 2015 07:28:00 GMT"));
    assert_eq!(
        head.metadata().unwrap().get("color").map(String::as_str),
        Some("red")
    );
    assert_eq!(get(&server, "dir/big").await, [first, second].concat());

    // The part data is gone; only the record that answers a retried Complete stays.
    assert_eq!(
        server.names(&format!(".s3dal/uploads/{BUCKET}/{id}")).await,
        ["upload.json"]
    );
    server.assert_no_temp_files().await;
}

#[tokio::test]
async fn complete_names_the_object_url_as_location() {
    // SDK upload helpers hand Location on as the uploaded object's URL (the JS SDK's `Upload`,
    // Go's `s3manager`), and applications store it, so it must reach the object, also for a key
    // that needs percent-encoding, and a retried Complete must name the same URL.
    let server = server().await;
    let key = "dir/a b+ü";
    let id = create(&server, key).await;
    let e1 = upload(&server, key, &id, 1, b"one part").await;
    let done = complete(&server, key, &id, &[(1, &e1)]).await.unwrap();
    let location = done.location().expect("a Location").to_owned();
    let path = location
        .strip_prefix(&server.endpoint)
        .unwrap_or_else(|| panic!("{location} is not on {}", server.endpoint));
    let (code, body) = common::signed_request(
        &server.endpoint,
        "GET",
        path,
        "",
        &[],
        "UNSIGNED-PAYLOAD",
        b"",
    )
    .await;
    assert_eq!(code, 200, "{}", String::from_utf8_lossy(&body));
    assert_eq!(body, b"one part");

    let again = complete(&server, key, &id, &[(1, &e1)]).await.unwrap();
    assert_eq!(again.location(), Some(location.as_str()));
}

#[tokio::test]
async fn a_retried_complete_answers_with_the_original_result() {
    // SDKs retry Complete when its response is lost. Answering the retry with NoSuchUpload
    // would report a failed upload that in fact succeeded, so AWS answers it with the original
    // result, and so must s3dal, without touching the object again.
    let server = server().await;
    let first = patterned(5 * MIB);
    let id = create(&server, "k").await;
    let e1 = upload(&server, "k", &id, 1, &first).await;
    let e2 = upload(&server, "k", &id, 2, b"tail").await;
    let list = [(1, e1.clone()), (2, e2.clone())];
    let done = complete(&server, "k", &id, &list).await.unwrap();
    let etag = done.e_tag().unwrap().to_owned();

    let again = complete(&server, "k", &id, &list).await.unwrap();
    assert_eq!(again.e_tag(), Some(etag.as_str()));
    assert_eq!(again.key(), Some("k"));
    assert_eq!(again.bucket(), Some(BUCKET));
    // AWS matches a retry by its part ETags alone.
    let renumbered = complete(&server, "k", &id, &[(3, &e1), (7, &e2)])
        .await
        .unwrap();
    assert_eq!(renumbered.e_tag(), Some(etag.as_str()));

    let head = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(head.e_tag(), Some(etag.as_str()));
    assert_eq!(get(&server, "k").await, [first, b"tail".to_vec()].concat());

    // A different list cannot complete the upload a second time.
    for other in [
        vec![(1, e1.clone())],
        vec![(1, e2.clone()), (2, e1.clone())],
    ] {
        let err = complete(&server, "k", &id, &other).await.unwrap_err();
        assert_eq!(err.code(), Some("NoSuchUpload"), "{other:?}: {err:?}");
        assert_eq!(status(&err), 404);
    }
    // Nor can the id complete another key.
    let err = complete(&server, "other", &id, &list).await.unwrap_err();
    assert_eq!(err.code(), Some("NoSuchUpload"), "{err:?}");
}

#[tokio::test]
async fn concurrent_completes_of_one_upload_both_answer_with_one_result() {
    // A client that times out while a large object is assembled retries while the first
    // Complete is still running. The retry must not fail on parts the first one removes, nor
    // write the object a second time; it waits and answers with the first one's result. With
    // If-None-Match a second write would fail on the object the first one created, so a retry
    // that assembled again would turn a successful upload into a 412.
    let server = server().await;
    let data = [patterned(6 * MIB), patterned(5 * MIB), b"tail".to_vec()];
    let id = create(&server, "k").await;
    let mut list = Vec::new();
    for (n, part) in (1..).zip(&data) {
        list.push((n, upload(&server, "k", &id, n, part).await));
    }
    let create_only = || {
        server
            .client
            .complete_multipart_upload()
            .bucket(BUCKET)
            .key("k")
            .upload_id(&id)
            .if_none_match("*")
            .multipart_upload(parts(&list))
            .send()
    };
    let (a, b) = tokio::join!(create_only(), create_only());
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_eq!(a.e_tag(), b.e_tag());
    assert_eq!(
        a.e_tag(),
        Some(multipart_etag(&[&data[0], &data[1], &data[2]]).as_str())
    );
    assert_eq!(get(&server, "k").await, data.concat());
    server.assert_no_temp_files().await;
}

#[tokio::test]
async fn a_completed_upload_is_gone_for_every_other_operation() {
    // Only a retried Complete may still see a completed upload. A part uploaded into it would
    // be stored for nothing, listing it would invite clients to resume it, and Abort must not
    // report removing an upload whose object exists.
    let server = server().await;
    let client = &server.client;
    let id = create(&server, "k").await;
    let etag = upload(&server, "k", &id, 1, b"data").await;
    let done = complete(&server, "k", &id, &[(1, &etag)]).await.unwrap();
    client
        .put_object()
        .bucket(BUCKET)
        .key("src")
        .body(ByteStream::from_static(b"source"))
        .send()
        .await
        .unwrap();

    let part = client
        .upload_part()
        .bucket(BUCKET)
        .key("k")
        .upload_id(&id)
        .part_number(2)
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(part.code(), Some("NoSuchUpload"), "{part:?}");
    let copy = client
        .upload_part_copy()
        .bucket(BUCKET)
        .key("k")
        .upload_id(&id)
        .part_number(2)
        .copy_source(format!("{BUCKET}/src"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(copy.code(), Some("NoSuchUpload"), "{copy:?}");
    let list = client
        .list_parts()
        .bucket(BUCKET)
        .key("k")
        .upload_id(&id)
        .send()
        .await
        .unwrap_err();
    assert_eq!(list.code(), Some("NoSuchUpload"), "{list:?}");
    let abort = client
        .abort_multipart_upload()
        .bucket(BUCKET)
        .key("k")
        .upload_id(&id)
        .send()
        .await
        .unwrap_err();
    assert_eq!(abort.code(), Some("NoSuchUpload"), "{abort:?}");
    let uploads = client
        .list_multipart_uploads()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    assert!(uploads.uploads().is_empty(), "{uploads:?}");

    // None of them stored anything, and the record still answers the retry.
    assert_eq!(
        server.names(&format!(".s3dal/uploads/{BUCKET}/{id}")).await,
        ["upload.json"]
    );
    let again = complete(&server, "k", &id, &[(1, &etag)]).await.unwrap();
    assert_eq!(again.e_tag(), done.e_tag());
    assert_eq!(get(&server, "k").await, b"data");
}

#[tokio::test]
async fn upload_listings_name_the_owner_as_owner_and_initiator() {
    // Every key acts for the one owner, so it owns and initiated every upload. Cleanup tools
    // pick the uploads to abort by initiator, and an upload without one would be skipped.
    let server = server().await;
    let id = create(&server, "k").await;
    let c = &server.client;
    let acl = c.get_bucket_acl().bucket(BUCKET).send().await.unwrap();
    let owner = acl.owner().unwrap();
    let as_initiator = (owner.id(), owner.display_name());

    let parts = c
        .list_parts()
        .bucket(BUCKET)
        .key("k")
        .upload_id(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(parts.owner(), Some(owner));
    let initiator = parts.initiator().unwrap();
    assert_eq!((initiator.id(), initiator.display_name()), as_initiator);

    let uploads = c
        .list_multipart_uploads()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    let upload = &uploads.uploads()[0];
    assert_eq!(upload.owner(), Some(owner));
    let initiator = upload.initiator().unwrap();
    assert_eq!((initiator.id(), initiator.display_name()), as_initiator);
}

#[tokio::test]
async fn reuploading_a_part_replaces_it() {
    // Clients retry failed parts under the same number; the retry must be what Complete uses,
    // and the ETag of the replaced data must no longer be accepted.
    let server = server().await;
    let id = create(&server, "k").await;
    let old = upload(&server, "k", &id, 1, b"first try").await;
    let new = upload(&server, "k", &id, 1, b"second try").await;
    assert_eq!(listed_parts(&server, "k", &id).await, [(1, new.clone())]);

    let err = complete(&server, "k", &id, &[(1, &old)]).await.unwrap_err();
    assert_eq!(err.code(), Some("InvalidPart"), "{err:?}");
    complete(&server, "k", &id, &[(1, &new)]).await.unwrap();
    assert_eq!(get(&server, "k").await, b"second try");
}

#[tokio::test]
async fn a_part_that_fails_its_checksum_is_bad_digest_and_keeps_the_previous_part() {
    // A corrupted retry must not replace a good part: the manifest is written only after the
    // data verified.
    let server = server().await;
    let id = create(&server, "k").await;
    let good = upload(&server, "k", &id, 1, b"good").await;
    let err = server
        .client
        .upload_part()
        .bucket(BUCKET)
        .key("k")
        .upload_id(&id)
        .part_number(1)
        .checksum_crc32("AAAAAA==")
        .body(ByteStream::from_static(b"bad"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("BadDigest"), "{err:?}");
    assert_eq!(listed_parts(&server, "k", &id).await, [(1, good)]);
    server.assert_no_temp_files().await;
}

#[tokio::test]
async fn a_part_with_an_empty_content_md5_is_invalid_digest() {
    // As for PutObject: an empty Content-MD5 is a failed check, not a skipped one.
    let server = server().await;
    let id = create(&server, "k").await;
    let err = server
        .client
        .upload_part()
        .bucket(BUCKET)
        .key("k")
        .upload_id(&id)
        .part_number(1)
        .body(ByteStream::from_static(b"part"))
        .customize()
        .mutate_request(|req| {
            req.headers_mut().insert("content-md5", "");
        })
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("InvalidDigest"), "{err:?}");
    assert_eq!(
        listed_parts(&server, "k", &id).await,
        Vec::<(i32, String)>::new()
    );
}

#[tokio::test]
async fn part_numbers_outside_1_to_10000_are_invalid_argument() {
    let server = server().await;
    let id = create(&server, "k").await;
    for n in [0, 10_001] {
        let err = server
            .client
            .upload_part()
            .bucket(BUCKET)
            .key("k")
            .upload_id(&id)
            .part_number(n)
            .body(ByteStream::from_static(b"x"))
            .send()
            .await
            .unwrap_err();
        assert_eq!(err.code(), Some("InvalidArgument"), "{n}: {err:?}");
    }
    assert_eq!(listed_parts(&server, "k", &id).await, Vec::new());
}

#[tokio::test]
async fn complete_errors_leave_the_upload_ready_for_a_corrected_complete() {
    // Each rule S3 enforces at Complete, checked before the response starts so the client gets
    // an ordinary error status. None of them may consume the upload: the client fixes its list
    // and completes again.
    let server = server().await;
    let id = create(&server, "k").await;
    let big = patterned(5 * MIB);
    let small = b"small".to_vec();
    let e1 = upload(&server, "k", &id, 1, &big).await;
    let e2 = upload(&server, "k", &id, 2, &small).await;
    let e3 = upload(&server, "k", &id, 3, &small).await;

    let cases: [(&[(i32, &String)], &str); 6] = [
        (&[(2, &e2), (1, &e1)], "InvalidPartOrder"),
        (&[(1, &e1), (1, &e1)], "InvalidPartOrder"),
        (&[(1, &e2), (2, &e2)], "InvalidPart"),
        (&[(1, &e1), (4, &e2)], "InvalidPart"),
        // Part 2 is under 5 MiB and not last.
        (&[(1, &e1), (2, &e2), (3, &e3)], "EntityTooSmall"),
        (&[], "MalformedXML"),
    ];
    for (list, code) in cases {
        let err = complete(&server, "k", &id, list).await.unwrap_err();
        assert_eq!(err.code(), Some(code), "{list:?}: {err:?}");
        assert_eq!(status(&err), 400, "{list:?}");
    }

    // A part checksum the client lists must match the one computed at upload.
    let wrong_checksum = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(&e1)
                .checksum_crc32("AAAAAA==")
                .build(),
        )
        .build();
    let err = server
        .client
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key("k")
        .upload_id(&id)
        .multipart_upload(wrong_checksum)
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("InvalidPart"), "{err:?}");

    // An upload id is bound to its key.
    let err = complete(&server, "other", &id, &[(1, &e1)])
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("NoSuchUpload"), "{err:?}");

    // Parts may skip numbers, and a correct checksum is accepted.
    let crc32 = server
        .client
        .upload_part()
        .bucket(BUCKET)
        .key("k")
        .upload_id(&id)
        .part_number(5)
        .body(ByteStream::from(small.clone()))
        .send()
        .await
        .unwrap()
        .checksum_crc32()
        .unwrap()
        .to_owned();
    let e5 = listed_parts(&server, "k", &id)
        .await
        .last()
        .unwrap()
        .1
        .clone();
    let good = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().part_number(1).e_tag(&e1).build())
        .parts(
            CompletedPart::builder()
                .part_number(5)
                .e_tag(&e5)
                .checksum_crc32(crc32)
                .build(),
        )
        .build();
    server
        .client
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key("k")
        .upload_id(&id)
        .multipart_upload(good)
        .send()
        .await
        .unwrap();
    assert_eq!(get(&server, "k").await, [big, small].concat());
}

#[tokio::test]
async fn complete_of_an_unknown_upload_is_no_such_upload() {
    let server = server().await;
    let err = complete(
        &server,
        "k",
        "0b5e6f4c-59f5-4b55-9d57-5a0f2d0a7a51",
        &[(1, "\"d41d8cd98f00b204e9800998ecf8427e\"")],
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), Some("NoSuchUpload"), "{err:?}");
    assert_eq!(status(&err), 404);
}

#[tokio::test]
async fn a_failed_final_write_is_an_error_document_in_the_200_and_keeps_the_upload() {
    // Once validation passes, the 200 and keep-alive whitespace go out before the object is
    // written, so a failure after that can only be an error document in the body. SDKs must
    // read it as an error, never as a completed upload. Here the write fails because `a` is a
    // prefix on fs (a documented deviation), which other backends do not have.
    let server = TestServer::start_fs().await;
    server.create_bucket_dir(BUCKET).await;
    server.plant(BUCKET, "a/b", b"child").await;
    let id = create(&server, "a").await;
    let etag = upload(&server, "a", &id, 1, b"parent").await;
    let err = complete(&server, "a", &id, &[(1, &etag)])
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("InvalidRequest"), "{err:?}");
    assert_eq!(status(&err), 200);

    assert_eq!(get(&server, "a/b").await, b"child");
    assert_eq!(listed_parts(&server, "a", &id).await, [(1, etag)]);
    server.assert_no_temp_files().await;
}

#[tokio::test]
async fn complete_with_if_none_match_star_never_replaces_an_existing_object() {
    // Two writers racing to create the same key: only the first may win, also when the second
    // assembles its object from parts.
    let server = server().await;
    server
        .client
        .put_object()
        .bucket(BUCKET)
        .key("k")
        .body(ByteStream::from_static(b"first writer"))
        .send()
        .await
        .unwrap();
    let id = create(&server, "k").await;
    let etag = upload(&server, "k", &id, 1, b"second writer").await;
    let err = server
        .client
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key("k")
        .upload_id(&id)
        .if_none_match("*")
        .multipart_upload(parts(&[(1, &etag)]))
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("PreconditionFailed"), "{err:?}");
    // Known before assembly starts, so it is a real 412 rather than an error inside a 200.
    assert_eq!(status(&err), 412);
    assert_eq!(get(&server, "k").await, b"first writer");
    server.assert_no_temp_files().await;

    let id = create(&server, "fresh").await;
    let etag = upload(&server, "fresh", &id, 1, b"only writer").await;
    server
        .client
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key("fresh")
        .upload_id(&id)
        .if_none_match("*")
        .multipart_upload(parts(&[(1, &etag)]))
        .send()
        .await
        .unwrap();
    assert_eq!(get(&server, "fresh").await, b"only writer");
}

#[tokio::test]
async fn complete_with_if_match_replaces_only_the_expected_object() {
    // A read-modify-write through a multipart upload must not overwrite a version it never
    // read. A failed condition leaves the upload in place, so the client can retry.
    let server = server().await;
    let put = |body: &'static [u8]| {
        server
            .client
            .put_object()
            .bucket(BUCKET)
            .key("k")
            .body(ByteStream::from_static(body))
            .send()
    };
    let read = put(b"read").await.unwrap().e_tag().unwrap().to_owned();
    let id = create(&server, "k").await;
    let etag = upload(&server, "k", &id, 1, b"modified").await;
    let complete_if = |expected: &str| {
        server
            .client
            .complete_multipart_upload()
            .bucket(BUCKET)
            .key("k")
            .upload_id(&id)
            .if_match(expected)
            .multipart_upload(parts(&[(1, &etag)]))
            .send()
    };

    put(b"newer").await.unwrap();
    let err = complete_if(&read).await.unwrap_err();
    assert_eq!(err.code(), Some("PreconditionFailed"), "{err:?}");
    assert_eq!(status(&err), 412);
    assert_eq!(get(&server, "k").await, b"newer");

    // S3 answers If-Match on a missing key with 404: the object is gone, not changed.
    server
        .client
        .delete_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    let err = complete_if(&read).await.unwrap_err();
    assert_eq!(err.code(), Some("NoSuchKey"), "{err:?}");
    assert_eq!(status(&err), 404);

    let current = put(b"current").await.unwrap().e_tag().unwrap().to_owned();
    complete_if(&current).await.unwrap();
    assert_eq!(get(&server, "k").await, b"modified");
    server.assert_no_temp_files().await;
}

#[tokio::test]
async fn abort_removes_the_upload_and_every_later_call_is_no_such_upload() {
    // Abort is how clients reclaim space; the data must really go, and the id must stop
    // working rather than revive a half-deleted upload.
    let server = server().await;
    let id = create(&server, "k").await;
    let etag = upload(&server, "k", &id, 1, b"data").await;
    server
        .client
        .abort_multipart_upload()
        .bucket(BUCKET)
        .key("k")
        .upload_id(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        server.names(&format!(".s3dal/uploads/{BUCKET}")).await,
        Vec::<String>::new()
    );

    let part = server
        .client
        .upload_part()
        .bucket(BUCKET)
        .key("k")
        .upload_id(&id)
        .part_number(2)
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(part.code(), Some("NoSuchUpload"), "{part:?}");
    let list = server
        .client
        .list_parts()
        .bucket(BUCKET)
        .key("k")
        .upload_id(&id)
        .send()
        .await
        .unwrap_err();
    assert_eq!(list.code(), Some("NoSuchUpload"), "{list:?}");
    let done = complete(&server, "k", &id, &[(1, &etag)])
        .await
        .unwrap_err();
    assert_eq!(done.code(), Some("NoSuchUpload"), "{done:?}");
    let again = server
        .client
        .abort_multipart_upload()
        .bucket(BUCKET)
        .key("k")
        .upload_id(&id)
        .send()
        .await
        .unwrap_err();
    assert_eq!(again.code(), Some("NoSuchUpload"), "{again:?}");
    // The upload directory was not recreated by the refused part.
    assert_eq!(
        server.names(&format!(".s3dal/uploads/{BUCKET}")).await,
        Vec::<String>::new()
    );
}

#[tokio::test]
async fn upload_ids_that_are_not_canonical_never_reach_a_path() {
    // The id names a directory. `..` would escape the upload area, and on a case-insensitive
    // filesystem an uppercase spelling of a real id would reach that upload's directory: an
    // abort with it would delete another client's upload.
    let server = server().await;
    let id = create(&server, "k").await;
    let etag = upload(&server, "k", &id, 1, b"data").await;
    for bad in ["../x".to_owned(), "../../bkt".to_owned(), id.to_uppercase()] {
        let abort = server
            .client
            .abort_multipart_upload()
            .bucket(BUCKET)
            .key("k")
            .upload_id(&bad)
            .send()
            .await
            .unwrap_err();
        assert_eq!(abort.code(), Some("NoSuchUpload"), "{bad}: {abort:?}");
        let part = server
            .client
            .upload_part()
            .bucket(BUCKET)
            .key("k")
            .upload_id(&bad)
            .part_number(1)
            .body(ByteStream::from_static(b"x"))
            .send()
            .await
            .unwrap_err();
        assert_eq!(part.code(), Some("NoSuchUpload"), "{bad}: {part:?}");
        let list = server
            .client
            .list_parts()
            .bucket(BUCKET)
            .key("k")
            .upload_id(&bad)
            .send()
            .await
            .unwrap_err();
        assert_eq!(list.code(), Some("NoSuchUpload"), "{bad}: {list:?}");
        let done = complete(&server, "k", &bad, &[(1, &etag)])
            .await
            .unwrap_err();
        assert_eq!(done.code(), Some("NoSuchUpload"), "{bad}: {done:?}");
    }
    assert_eq!(listed_parts(&server, "k", &id).await, [(1, etag)]);
    assert_eq!(server.names("").await, [".s3dal", BUCKET]);
    assert_eq!(server.names(".s3dal/uploads").await, [BUCKET]);
    assert_eq!(
        server.names(&format!(".s3dal/uploads/{BUCKET}")).await,
        [id]
    );
    assert!(!server.exists(".s3dal/bkt/").await);
    assert_eq!(server.names(BUCKET).await, Vec::<String>::new());
}

#[tokio::test]
async fn the_multipart_etag_holds_for_parts_of_any_size() {
    // With native ETags the backend's multipart ETag is the only one, so every client part
    // must become exactly one backend part. Parts that are neither a multiple of OpenDAL's 5 MiB
    // write chunk nor all the same size would come out cut differently if Complete streamed
    // them into the object; rclone then reports the upload as corrupted.
    let server = server().await;
    let data = [patterned(7 * MIB), patterned(5 * MIB + 3), b"tail".to_vec()];
    let id = create(&server, "k").await;
    let mut listed = Vec::new();
    for (n, part) in (1..).zip(&data) {
        listed.push((n, upload(&server, "k", &id, n, part).await));
    }

    let out = complete(&server, "k", &id, &listed).await.unwrap();
    let expected = multipart_etag(&[&data[0], &data[1], &data[2]]);
    assert_eq!(out.e_tag(), Some(expected.as_str()));
    let head = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(head.e_tag(), Some(expected.as_str()));
    assert_eq!(get(&server, "k").await, data.concat());
}

#[tokio::test]
async fn list_parts_pages_by_part_number_marker() {
    // SDK paginators and resuming clients walk parts with part-number-marker; a part repeated
    // or skipped between pages corrupts the Complete list they build from it.
    let server = server().await;
    let id = create(&server, "k").await;
    let mut etags = Vec::new();
    for n in [3, 1, 2] {
        etags.push((
            n,
            upload(&server, "k", &id, n, format!("part {n}").as_bytes()).await,
        ));
    }
    etags.sort();

    let first = server
        .client
        .list_parts()
        .bucket(BUCKET)
        .key("k")
        .upload_id(&id)
        .max_parts(2)
        .send()
        .await
        .unwrap();
    let numbers: Vec<i32> = first
        .parts()
        .iter()
        .map(|p| p.part_number().unwrap())
        .collect();
    assert_eq!(numbers, [1, 2]);
    assert_eq!(first.is_truncated(), Some(true));
    assert_eq!(first.next_part_number_marker(), Some("2"));
    assert_eq!(first.parts()[0].e_tag(), Some(etags[0].1.as_str()));
    assert_eq!(first.parts()[0].size(), Some(6));

    let second = server
        .client
        .list_parts()
        .bucket(BUCKET)
        .key("k")
        .upload_id(&id)
        .max_parts(2)
        .part_number_marker("2")
        .send()
        .await
        .unwrap();
    let numbers: Vec<i32> = second
        .parts()
        .iter()
        .map(|p| p.part_number().unwrap())
        .collect();
    assert_eq!(numbers, [3]);
    assert_eq!(second.is_truncated(), Some(false));
}

#[tokio::test]
async fn list_multipart_uploads_pages_by_key_then_initiation_time() {
    // Cleanup tools page through uploads with key-marker and upload-id-marker; uploads of one
    // key may straddle a page boundary and must still each be listed exactly once.
    let server = server().await;
    let b = create(&server, "b").await;
    let a1 = create(&server, "a").await;
    let a2 = create(&server, "a").await;
    let cx = create(&server, "c/x").await;
    let cy = create(&server, "c/y").await;
    let expected = [
        ("a", a1.as_str()),
        ("a", a2.as_str()),
        ("b", b.as_str()),
        ("c/x", cx.as_str()),
        ("c/y", cy.as_str()),
    ];

    let mut seen = Vec::new();
    let (mut key_marker, mut id_marker) = (None::<String>, None::<String>);
    loop {
        let page = server
            .client
            .list_multipart_uploads()
            .bucket(BUCKET)
            .max_uploads(2)
            .set_key_marker(key_marker.clone())
            .set_upload_id_marker(id_marker.clone())
            .send()
            .await
            .unwrap();
        for u in page.uploads() {
            seen.push((
                u.key().unwrap().to_owned(),
                u.upload_id().unwrap().to_owned(),
            ));
        }
        if page.is_truncated() != Some(true) {
            break;
        }
        key_marker = page.next_key_marker().map(str::to_owned);
        id_marker = page.next_upload_id_marker().map(str::to_owned);
    }
    let seen: Vec<(&str, &str)> = seen.iter().map(|(k, i)| (k.as_str(), i.as_str())).collect();
    assert_eq!(seen, expected);

    let grouped = server
        .client
        .list_multipart_uploads()
        .bucket(BUCKET)
        .delimiter("/")
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = grouped.uploads().iter().map(|u| u.key().unwrap()).collect();
    assert_eq!(keys, ["a", "a", "b"]);
    let prefixes: Vec<&str> = grouped
        .common_prefixes()
        .iter()
        .map(|p| p.prefix().unwrap())
        .collect();
    assert_eq!(prefixes, ["c/"]);

    let below = server
        .client
        .list_multipart_uploads()
        .bucket(BUCKET)
        .prefix("c/")
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = below.uploads().iter().map(|u| u.key().unwrap()).collect();
    assert_eq!(keys, ["c/x", "c/y"]);

    // Any delimiter groups, as in ListObjects.
    let by_x = server
        .client
        .list_multipart_uploads()
        .bucket(BUCKET)
        .delimiter("x")
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = by_x.uploads().iter().map(|u| u.key().unwrap()).collect();
    assert_eq!(keys, ["a", "a", "b", "c/y"]);
    let prefixes: Vec<&str> = by_x
        .common_prefixes()
        .iter()
        .map(|p| p.prefix().unwrap())
        .collect();
    assert_eq!(prefixes, ["c/x"]);
}

#[tokio::test]
async fn delete_bucket_removes_its_uploads() {
    // A bucket created later under the same name is a new bucket; it must not inherit uploads
    // (and their storage) from the one deleted before.
    let server = TestServer::start().await;
    let client = &server.client;
    client.create_bucket().bucket(BUCKET).send().await.unwrap();
    let id = create(&server, "k").await;
    upload(&server, "k", &id, 1, b"data").await;

    client.delete_bucket().bucket(BUCKET).send().await.unwrap();
    assert!(!server.exists(&format!(".s3dal/uploads/{BUCKET}/")).await);

    client.create_bucket().bucket(BUCKET).send().await.unwrap();
    let uploads = client
        .list_multipart_uploads()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    assert!(uploads.uploads().is_empty(), "{uploads:?}");
    let err = client
        .list_parts()
        .bucket(BUCKET)
        .key("k")
        .upload_id(&id)
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("NoSuchUpload"), "{err:?}");
}

#[tokio::test]
async fn the_stale_upload_sweep_removes_exactly_what_went_stale() {
    // s3dal has no lifecycle rules, so without the sweep an abandoned upload would hold its
    // parts forever. It must take every kind of stale entry, judged by times every backend has,
    // and nothing a client may still use: an upload in progress, or the record a retried
    // Complete needs. What exists before the wait below is stale and what comes after it is
    // not: each sweep's age reaches back to the end of the wait and a margin further, however
    // long the requests since took. Backend mtimes have second precision and may come from
    // another clock, so the margin is seconds, and the wait three of them.
    const MARGIN: Duration = Duration::from_secs(2);
    let server = server().await;
    let client = &server.client;
    let uploads = format!(".s3dal/uploads/{BUCKET}");
    let op = &server.op;
    let plant = |id: &str| {
        let path = format!("{uploads}/{id}/part-00001.json");
        async move { op.write(&path, b"{}".to_vec()).await.unwrap() }
    };

    let stale = create(&server, "stale").await;
    let done = create(&server, "done").await;
    let done_etag = upload(&server, "done", &done, 1, b"done").await;
    complete(&server, "done", &done, &[(1, &done_etag)])
        .await
        .unwrap();
    // Initiated now, completed after the wait.
    let late = create(&server, "late").await;
    let late_etag = upload(&server, "late", &late, 1, b"late").await;
    // What an UploadPart racing an Abort leaves: parts without `upload.json`.
    let leftover = uuid::Uuid::new_v4().to_string();
    plant(&leftover).await;
    // Not an upload id, so not s3dal's to remove, however old.
    plant("not-an-upload").await;

    tokio::time::sleep(3 * MARGIN).await;
    let waited = Instant::now();
    let age = || waited.elapsed() + MARGIN;

    let late_done = complete(&server, "late", &late, &[(1, &late_etag)])
        .await
        .unwrap();
    // As with AWS lifecycle rules (DaysAfterInitiation), an upload's age counts from its
    // initiation, so a recent part does not keep it.
    upload(&server, "stale", &stale, 1, b"part").await;
    let fresh = create(&server, "fresh").await;
    let fresh_etag = upload(&server, "fresh", &fresh, 1, b"fresh").await;
    // A bucket removed behind s3dal's back leaves uploads nobody can reach, however recent.
    server.create_bucket_dir("gone").await;
    let orphan = client
        .create_multipart_upload()
        .bucket("gone")
        .key("k")
        .send()
        .await
        .unwrap();
    client
        .upload_part()
        .bucket("gone")
        .key("k")
        .upload_id(orphan.upload_id().unwrap())
        .part_number(1)
        .body(ByteStream::from_static(b"orphan"))
        .send()
        .await
        .unwrap();
    server.op.delete("gone/").await.unwrap();
    let recent = uuid::Uuid::new_v4().to_string();
    plant(&recent).await;

    assert_eq!(server.dal.sweep_stale_uploads(age()).await.unwrap(), 4);
    let mut kept = vec![
        fresh.clone(),
        late.clone(),
        recent,
        "not-an-upload".to_owned(),
    ];
    kept.sort();
    assert_eq!(server.names(&uploads).await, kept);
    assert_eq!(server.names(".s3dal/uploads").await, [BUCKET]);

    let err = client
        .list_parts()
        .bucket(BUCKET)
        .key("stale")
        .upload_id(&stale)
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("NoSuchUpload"), "{err:?}");
    // The record of an upload completed long ago goes; its object stays.
    let err = complete(&server, "done", &done, &[(1, &done_etag)])
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("NoSuchUpload"), "{err:?}");
    assert_eq!(get(&server, "done").await, b"done");
    // A record's age counts from its completion: it still answers a retry.
    let again = complete(&server, "late", &late, &[(1, &late_etag)])
        .await
        .unwrap();
    assert_eq!(again.e_tag(), late_done.e_tag());
    complete(&server, "fresh", &fresh, &[(1, &fresh_etag)])
        .await
        .unwrap();
    assert_eq!(get(&server, "fresh").await, b"fresh");

    assert_eq!(server.dal.sweep_stale_uploads(age()).await.unwrap(), 0);
}

#[tokio::test]
async fn the_sweep_ages_an_empty_upload_directory_by_its_own_mtime_on_fs() {
    // Only fs has empty directories: Create leaves one behind when writing `upload.json` into it
    // fails. No file in it can tell its age, so its own mtime must, or such directories would
    // pile up forever; a recent one may be a Create still writing.
    let server = TestServer::start_fs().await;
    server.create_bucket_dir(BUCKET).await;
    let uploads = format!(".s3dal/uploads/{BUCKET}");
    let (old, new) = (uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
    for id in [old, new] {
        server
            .op
            .create_dir(&format!("{uploads}/{id}/"))
            .await
            .unwrap();
    }
    let day_ago = SystemTime::now() - Duration::from_secs(24 * 60 * 60);
    std::fs::File::open(server.fs_path(&format!("{uploads}/{old}")))
        .unwrap()
        .set_modified(day_ago)
        .unwrap();

    let removed = server
        .dal
        .sweep_stale_uploads(Duration::from_secs(60 * 60))
        .await
        .unwrap();
    assert_eq!(removed, 1);
    assert_eq!(server.names(&uploads).await, [new.to_string()]);
}

#[tokio::test]
async fn uploads_into_a_missing_bucket_are_no_such_bucket() {
    // Every multipart operation checks the bucket first, and none creates it as a side effect.
    let server = server().await;
    let client = &server.client;
    let id = "0b5e6f4c-59f5-4b55-9d57-5a0f2d0a7a51";
    let create = client
        .create_multipart_upload()
        .bucket("nope")
        .key("k")
        .send()
        .await
        .unwrap_err();
    assert_eq!(create.code(), Some("NoSuchBucket"), "{create:?}");
    let part = client
        .upload_part()
        .bucket("nope")
        .key("k")
        .upload_id(id)
        .part_number(1)
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(part.code(), Some("NoSuchBucket"), "{part:?}");
    let done = client
        .complete_multipart_upload()
        .bucket("nope")
        .key("k")
        .upload_id(id)
        .multipart_upload(parts(&[(1, "\"d41d8cd98f00b204e9800998ecf8427e\"")]))
        .send()
        .await
        .unwrap_err();
    assert_eq!(done.code(), Some("NoSuchBucket"), "{done:?}");
    let abort = client
        .abort_multipart_upload()
        .bucket("nope")
        .key("k")
        .upload_id(id)
        .send()
        .await
        .unwrap_err();
    assert_eq!(abort.code(), Some("NoSuchBucket"), "{abort:?}");
    let list = client
        .list_parts()
        .bucket("nope")
        .key("k")
        .upload_id(id)
        .send()
        .await
        .unwrap_err();
    assert_eq!(list.code(), Some("NoSuchBucket"), "{list:?}");
    let uploads = client
        .list_multipart_uploads()
        .bucket("nope")
        .send()
        .await
        .unwrap_err();
    assert_eq!(uploads.code(), Some("NoSuchBucket"), "{uploads:?}");
    assert!(!server.exists("nope/").await);
    assert!(!server.exists(".s3dal/uploads/nope/").await);
}

#[tokio::test]
async fn create_refuses_what_it_cannot_store_or_enforce() {
    // Encryption, object lock, tags, ACL grants and redirects are protections or stored values
    // a client relies on; accepting them and dropping them would fail silently. Purely advisory
    // headers are accepted.
    let server = server().await;
    let client = &server.client;
    let base = || client.create_multipart_upload().bucket(BUCKET).key("k");
    let refused = [
        base().server_side_encryption(ServerSideEncryption::Aes256),
        base().sse_customer_algorithm("AES256"),
        base().tagging("team=a"),
        base()
            .object_lock_mode(ObjectLockMode::Governance)
            .object_lock_retain_until_date(aws_sdk_s3::primitives::DateTime::from_secs(
                4_000_000_000,
            )),
        base().acl(ObjectCannedAcl::PublicRead),
        base().grant_read("id=someone"),
        base().website_redirect_location("/elsewhere"),
    ];
    for request in refused {
        let err = request.send().await.unwrap_err();
        assert_eq!(err.code(), Some("NotImplemented"), "{err:?}");
    }
    // A folder marker is a directory here and cannot be assembled from parts.
    let marker = client
        .create_multipart_upload()
        .bucket(BUCKET)
        .key("dir/")
        .send()
        .await
        .unwrap_err();
    assert_eq!(marker.code(), Some("NotImplemented"), "{marker:?}");
    // Otherwise a client could forge the ETag the object will report.
    let reserved = base()
        .metadata("s3dal-etag", "forged")
        .send()
        .await
        .unwrap_err();
    assert_eq!(reserved.code(), Some("InvalidArgument"), "{reserved:?}");
    let uploads = client
        .list_multipart_uploads()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    assert!(uploads.uploads().is_empty(), "{uploads:?}");

    let id = base()
        .acl(ObjectCannedAcl::Private)
        .storage_class(StorageClass::StandardIa)
        .send()
        .await
        .unwrap()
        .upload_id()
        .unwrap()
        .to_owned();
    // SSE-C on a part would ask for encryption the upload cannot give.
    let part = client
        .upload_part()
        .bucket(BUCKET)
        .key("k")
        .upload_id(&id)
        .part_number(1)
        .sse_customer_algorithm("AES256")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(part.code(), Some("NotImplemented"), "{part:?}");
}

// Checksums of multipart objects.

/// The binary digest of `data` under `algorithm`, computed independently of s3dal.
fn digest(algorithm: &ChecksumAlgorithm, data: &[u8]) -> Vec<u8> {
    use s3s::crypto::{Checksum as _, Crc32, Crc32c, Crc64Nvme, Sha1, Sha256};
    match algorithm {
        ChecksumAlgorithm::Crc32 => Crc32::checksum(data).to_vec(),
        ChecksumAlgorithm::Crc32C => Crc32c::checksum(data).to_vec(),
        ChecksumAlgorithm::Crc64Nvme => Crc64Nvme::checksum(data).to_vec(),
        ChecksumAlgorithm::Sha1 => Sha1::checksum(data).to_vec(),
        ChecksumAlgorithm::Sha256 => Sha256::checksum(data).to_vec(),
        other => panic!("no test digest for {other}"),
    }
}

fn base64(bytes: &[u8]) -> String {
    base64_simd::STANDARD.encode_to_string(bytes)
}

/// AWS's composite checksum of `parts`, computed independently of s3dal.
fn composite(algorithm: &ChecksumAlgorithm, parts: &[Vec<u8>]) -> String {
    let concat: Vec<u8> = parts.iter().flat_map(|p| digest(algorithm, p)).collect();
    format!("{}-{}", base64(&digest(algorithm, &concat)), parts.len())
}

/// The value for `algorithm` among an output's CRC32, CRC32C, CRC64NVME, SHA1 and SHA256
/// checksums, in that order.
fn pick<'a>(algorithm: &ChecksumAlgorithm, values: [Option<&'a str>; 5]) -> Option<&'a str> {
    let i = match algorithm {
        ChecksumAlgorithm::Crc32 => 0,
        ChecksumAlgorithm::Crc32C => 1,
        ChecksumAlgorithm::Crc64Nvme => 2,
        ChecksumAlgorithm::Sha1 => 3,
        ChecksumAlgorithm::Sha256 => 4,
        other => panic!("no test field for {other}"),
    };
    values[i]
}

/// Part `n` as Complete lists it, with its checksum under `algorithm`.
fn checksummed(n: i32, etag: &str, algorithm: &ChecksumAlgorithm, value: &str) -> CompletedPart {
    let part = CompletedPart::builder().part_number(n).e_tag(etag);
    match algorithm {
        ChecksumAlgorithm::Crc32 => part.checksum_crc32(value),
        ChecksumAlgorithm::Crc32C => part.checksum_crc32_c(value),
        ChecksumAlgorithm::Crc64Nvme => part.checksum_crc64_nvme(value),
        ChecksumAlgorithm::Sha1 => part.checksum_sha1(value),
        ChecksumAlgorithm::Sha256 => part.checksum_sha256(value),
        other => panic!("no test field for {other}"),
    }
    .build()
}

/// A Complete of `parts` that sends `value` as the object's checksum under `algorithm`.
fn complete_checked(
    server: &TestServer,
    key: &str,
    id: &str,
    parts: &[CompletedPart],
    algorithm: &ChecksumAlgorithm,
    value: &str,
) -> CompleteMultipartUploadFluentBuilder {
    let request = server
        .client
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key(key)
        .upload_id(id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(parts.to_vec()))
                .build(),
        );
    match algorithm {
        ChecksumAlgorithm::Crc32 => request.checksum_crc32(value),
        ChecksumAlgorithm::Crc32C => request.checksum_crc32_c(value),
        ChecksumAlgorithm::Crc64Nvme => request.checksum_crc64_nvme(value),
        ChecksumAlgorithm::Sha1 => request.checksum_sha1(value),
        ChecksumAlgorithm::Sha256 => request.checksum_sha256(value),
        other => panic!("no test field for {other}"),
    }
}

/// Creates an upload of `key` with `algorithm` (and `checksum_type`, if given), uploads `data`
/// as its parts with that algorithm, and returns the id and the parts as Complete lists them.
/// Each part's checksum must be the one computed independently.
async fn upload_checksummed(
    server: &TestServer,
    key: &str,
    algorithm: &ChecksumAlgorithm,
    checksum_type: Option<ChecksumType>,
    data: &[Vec<u8>],
) -> (String, Vec<CompletedPart>) {
    let client = &server.client;
    let created = client
        .create_multipart_upload()
        .bucket(BUCKET)
        .key(key)
        .checksum_algorithm(algorithm.clone())
        .set_checksum_type(checksum_type)
        .send()
        .await
        .unwrap();
    assert_eq!(created.checksum_algorithm(), Some(algorithm));
    let id = created.upload_id().unwrap().to_owned();
    let mut listed = Vec::new();
    for (n, part) in (1..).zip(data) {
        let out = client
            .upload_part()
            .bucket(BUCKET)
            .key(key)
            .upload_id(&id)
            .part_number(n)
            .checksum_algorithm(algorithm.clone())
            .body(ByteStream::from(part.clone()))
            .send()
            .await
            .unwrap();
        let value = pick(
            algorithm,
            [
                out.checksum_crc32(),
                out.checksum_crc32_c(),
                out.checksum_crc64_nvme(),
                out.checksum_sha1(),
                out.checksum_sha256(),
            ],
        );
        assert_eq!(value, Some(base64(&digest(algorithm, part)).as_str()));
        listed.push(checksummed(
            n,
            out.e_tag().unwrap(),
            algorithm,
            value.unwrap(),
        ));
    }
    (id, listed)
}

async fn head_checksummed(server: &TestServer, key: &str) -> HeadObjectOutput {
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
async fn a_composite_checksum_is_derived_from_the_parts_and_returned_with_the_object() {
    // A client that creates an upload with an algorithm gets the object's checksum from
    // Complete, GET and HEAD, and compares it with the one it computed (s3-tests and the aws
    // CLI do). It must be AWS's composite value, labeled COMPOSITE: SDKs see the `-<count>`
    // and know not to validate the body against it, so no download fails for it.
    let server = server().await;
    let client = &server.client;
    let data = [patterned(5 * MIB), b"tail".to_vec()];
    // CRC32 is composite unless the upload asks for FULL_OBJECT, as on AWS.
    for algorithm in [ChecksumAlgorithm::Sha256, ChecksumAlgorithm::Crc32] {
        let key = algorithm.as_str();
        let (id, listed) = upload_checksummed(&server, key, &algorithm, None, &data).await;
        let in_progress = client
            .list_parts()
            .bucket(BUCKET)
            .key(key)
            .upload_id(&id)
            .send()
            .await
            .unwrap();
        assert_eq!(in_progress.checksum_algorithm(), Some(&algorithm));
        assert_eq!(in_progress.checksum_type(), Some(&ChecksumType::Composite));
        let uploads = client
            .list_multipart_uploads()
            .bucket(BUCKET)
            .prefix(key)
            .send()
            .await
            .unwrap();
        assert_eq!(
            uploads.uploads()[0].checksum_type(),
            Some(&ChecksumType::Composite)
        );

        let expected = composite(&algorithm, &data);
        // The client may send the value with its part count, or without.
        let sent = match algorithm {
            ChecksumAlgorithm::Sha256 => expected.clone(),
            _ => expected.split_once('-').unwrap().0.to_owned(),
        };
        let done = complete_checked(&server, key, &id, &listed, &algorithm, &sent)
            .send()
            .await
            .unwrap();
        assert_eq!(
            pick(
                &algorithm,
                [
                    done.checksum_crc32(),
                    done.checksum_crc32_c(),
                    done.checksum_crc64_nvme(),
                    done.checksum_sha1(),
                    done.checksum_sha256()
                ]
            ),
            Some(expected.as_str())
        );
        assert_eq!(done.checksum_type(), Some(&ChecksumType::Composite));

        let head = head_checksummed(&server, key).await;
        assert_eq!(
            pick(
                &algorithm,
                [
                    head.checksum_crc32(),
                    head.checksum_crc32_c(),
                    head.checksum_crc64_nvme(),
                    head.checksum_sha1(),
                    head.checksum_sha256()
                ]
            ),
            Some(expected.as_str())
        );
        assert_eq!(head.checksum_type(), Some(&ChecksumType::Composite));
        let got = client
            .get_object()
            .bucket(BUCKET)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(
            pick(
                &algorithm,
                [
                    got.checksum_crc32(),
                    got.checksum_crc32_c(),
                    got.checksum_crc64_nvme(),
                    got.checksum_sha1(),
                    got.checksum_sha256()
                ]
            ),
            Some(expected.as_str())
        );
        assert_eq!(got.checksum_type(), Some(&ChecksumType::Composite));
        let body = got.body.collect().await.unwrap().into_bytes();
        assert_eq!(&body[..], &data.concat()[..]);

        // A part is not the object: the object's checksum would not match its bytes.
        let part = client
            .get_object()
            .bucket(BUCKET)
            .key(key)
            .part_number(1)
            .send()
            .await
            .unwrap();
        assert_eq!(part.checksum_type(), None);
        assert_eq!(part.checksum_sha256(), None);
        assert_eq!(part.checksum_crc32(), None);

        // A retried Complete answers with the same checksum.
        let again = complete_checked(&server, key, &id, &listed, &algorithm, &sent)
            .send()
            .await
            .unwrap();
        assert_eq!(again.checksum_type(), Some(&ChecksumType::Composite));
        assert_eq!(
            pick(
                &algorithm,
                [
                    again.checksum_crc32(),
                    again.checksum_crc32_c(),
                    again.checksum_crc64_nvme(),
                    again.checksum_sha1(),
                    again.checksum_sha256()
                ]
            ),
            Some(expected.as_str())
        );
    }
}

#[tokio::test]
async fn a_complete_that_contradicts_the_upload_checksums_is_refused_and_keeps_the_upload() {
    // The object's checksum and the part checksums are integrity checks the client asked for;
    // a contradiction means corruption or a confused client, and committing the object anyway
    // would defeat the check. Like every Complete error, none consumes the upload.
    let server = server().await;
    let client = &server.client;
    let sha256 = ChecksumAlgorithm::Sha256;
    let data = [patterned(5 * MIB), b"tail".to_vec()];
    let (id, listed) = upload_checksummed(&server, "k", &sha256, None, &data).await;
    let expected = composite(&sha256, &data);
    let other = composite(&sha256, &[b"other".to_vec(), b"data".to_vec()]);
    let bare: Vec<CompletedPart> = listed
        .iter()
        .map(|p| {
            CompletedPart::builder()
                .part_number(p.part_number().unwrap())
                .e_tag(p.e_tag().unwrap())
                .build()
        })
        .collect();
    let wrong_count = format!("{}-3", expected.split_once('-').unwrap().0);

    let cases = [
        (
            complete_checked(&server, "k", &id, &listed, &sha256, &other),
            "BadDigest",
        ),
        // The count is part of the value.
        (
            complete_checked(&server, "k", &id, &listed, &sha256, &wrong_count),
            "BadDigest",
        ),
        // A wrong object checksum is BadDigest even without part checksums (s3-tests).
        (
            complete_checked(&server, "k", &id, &bare, &sha256, &other),
            "BadDigest",
        ),
        // AWS requires every part's checksum for a composite one.
        (
            complete_checked(&server, "k", &id, &bare, &sha256, &expected),
            "InvalidRequest",
        ),
        (
            complete_checked(&server, "k", &id, &listed, &sha256, &expected)
                .checksum_type(ChecksumType::FullObject),
            "BadDigest",
        ),
        // The object will have no CRC32 checksum to check.
        (
            complete_checked(
                &server,
                "k",
                &id,
                &listed,
                &ChecksumAlgorithm::Crc32,
                "AAAAAA==",
            ),
            "InvalidRequest",
        ),
        (
            complete_checked(&server, "k", &id, &listed, &sha256, &expected)
                .checksum_crc32("AAAAAA=="),
            "InvalidRequest",
        ),
    ];
    for (request, code) in cases {
        let err = request.send().await.unwrap_err();
        assert_eq!(err.code(), Some(code), "{err:?}");
        assert_eq!(status(&err), 400, "{err:?}");
    }
    assert!(!server.exists(&format!("{BUCKET}/k")).await);

    // A part of another algorithm could not contribute to the object's checksum.
    let crc32 = client
        .upload_part()
        .bucket(BUCKET)
        .key("k")
        .upload_id(&id)
        .part_number(3)
        .checksum_algorithm(ChecksumAlgorithm::Crc32)
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(crc32.code(), Some("InvalidRequest"), "{crc32:?}");
    // Nor could a part without one, which AWS refuses on arrival rather than at Complete.
    let bare = aws_sdk_s3::Client::from_conf(
        common::client_config(&server.endpoint, common::ACCESS_KEY, common::SECRET_KEY)
            .request_checksum_calculation(
                aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired,
            )
            .build(),
    );
    let none = bare
        .upload_part()
        .bucket(BUCKET)
        .key("k")
        .upload_id(&id)
        .part_number(3)
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(none.code(), Some("InvalidRequest"), "{none:?}");
    assert!(
        none.message()
            .is_some_and(|m| m.ends_with("actual checksum Type: null")),
        "{none:?}"
    );

    complete_checked(&server, "k", &id, &listed, &sha256, &expected)
        .checksum_type(ChecksumType::Composite)
        .send()
        .await
        .unwrap();
    assert_eq!(get(&server, "k").await, data.concat());
    // A retry is held to the checksum the original was.
    let err = complete_checked(&server, "k", &id, &listed, &sha256, &other)
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("BadDigest"), "{err:?}");
}

#[tokio::test]
async fn full_object_crcs_combine_into_the_checksum_of_the_whole_object() {
    // A FULL_OBJECT checksum is the one a client computes over the object it downloads, so
    // SDKs validate GET bodies against it: a wrong combination of the part CRCs would fail every
    // download, and this test's GETs would fail with it.
    let server = server().await;
    let mut second = patterned(5 * MIB + 3);
    second.reverse();
    let data = [patterned(5 * MIB), second, b"tail".to_vec()];
    let whole = data.concat();
    // CRC64NVME is FULL_OBJECT without being asked, as on AWS.
    for (algorithm, checksum_type) in [
        (ChecksumAlgorithm::Crc32, Some(ChecksumType::FullObject)),
        (ChecksumAlgorithm::Crc32C, Some(ChecksumType::FullObject)),
        (ChecksumAlgorithm::Crc64Nvme, None),
    ] {
        let key = algorithm.as_str();
        let (id, listed) = upload_checksummed(&server, key, &algorithm, checksum_type, &data).await;
        let expected = base64(&digest(&algorithm, &whole));

        let wrong = base64(&digest(&algorithm, b"other"));
        let err = complete_checked(&server, key, &id, &listed, &algorithm, &wrong)
            .send()
            .await
            .unwrap_err();
        assert_eq!(err.code(), Some("BadDigest"), "{algorithm}: {err:?}");

        // Part checksums are optional in the list for a full-object checksum.
        let bare: Vec<CompletedPart> = listed
            .iter()
            .map(|p| {
                CompletedPart::builder()
                    .part_number(p.part_number().unwrap())
                    .e_tag(p.e_tag().unwrap())
                    .build()
            })
            .collect();
        let done = complete_checked(&server, key, &id, &bare, &algorithm, &expected)
            .send()
            .await
            .unwrap();
        assert_eq!(done.checksum_type(), Some(&ChecksumType::FullObject));
        assert_eq!(
            pick(
                &algorithm,
                [
                    done.checksum_crc32(),
                    done.checksum_crc32_c(),
                    done.checksum_crc64_nvme(),
                    done.checksum_sha1(),
                    done.checksum_sha256()
                ]
            ),
            Some(expected.as_str()),
            "{algorithm}"
        );

        let head = head_checksummed(&server, key).await;
        assert_eq!(head.checksum_type(), Some(&ChecksumType::FullObject));
        let got = server
            .client
            .get_object()
            .bucket(BUCKET)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(
            pick(
                &algorithm,
                [
                    got.checksum_crc32(),
                    got.checksum_crc32_c(),
                    got.checksum_crc64_nvme(),
                    got.checksum_sha1(),
                    got.checksum_sha256()
                ]
            ),
            Some(expected.as_str()),
            "{algorithm}"
        );
        // The SDK validated the body against the checksum while collecting it.
        let body = got.body.collect().await.unwrap().into_bytes();
        assert_eq!(&body[..], &whole[..], "{algorithm}");
    }
}

#[tokio::test]
async fn create_refuses_checksum_types_its_algorithm_cannot_have() {
    // A type without an algorithm, or one the algorithm cannot have, promises the client a
    // checksum Complete could not compute; AWS refuses both up front. Without a type, the
    // upload gets AWS's default for its algorithm.
    let server = server().await;
    let client = &server.client;
    let base = || client.create_multipart_upload().bucket(BUCKET).key("k");
    for request in [
        base().checksum_type(ChecksumType::Composite),
        base()
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .checksum_type(ChecksumType::FullObject),
        base()
            .checksum_algorithm(ChecksumAlgorithm::Crc64Nvme)
            .checksum_type(ChecksumType::Composite),
    ] {
        let err = request.send().await.unwrap_err();
        assert_eq!(err.code(), Some("InvalidRequest"), "{err:?}");
    }
    for (algorithm, named, expected) in [
        (ChecksumAlgorithm::Crc32, None, ChecksumType::Composite),
        (ChecksumAlgorithm::Sha1, None, ChecksumType::Composite),
        (ChecksumAlgorithm::Crc64Nvme, None, ChecksumType::FullObject),
        (
            ChecksumAlgorithm::Crc32C,
            Some(ChecksumType::FullObject),
            ChecksumType::FullObject,
        ),
    ] {
        let created = base()
            .checksum_algorithm(algorithm.clone())
            .set_checksum_type(named)
            .send()
            .await
            .unwrap();
        assert_eq!(created.checksum_type(), Some(&expected), "{algorithm}");
    }

    // An upload without an algorithm gives the object no checksum to check one against.
    let id = create(&server, "plain").await;
    let etag = upload(&server, "plain", &id, 1, b"x").await;
    let err = complete_checked(
        &server,
        "plain",
        &id,
        &[CompletedPart::builder().part_number(1).e_tag(&etag).build()],
        &ChecksumAlgorithm::Crc32,
        &base64(&digest(&ChecksumAlgorithm::Crc32, b"x")),
    )
    .send()
    .await
    .unwrap_err();
    assert_eq!(err.code(), Some("InvalidRequest"), "{err:?}");
    let err = server
        .client
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key("plain")
        .upload_id(&id)
        .checksum_type(ChecksumType::FullObject)
        .multipart_upload(parts(&[(1, &etag)]))
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("BadDigest"), "{err:?}");
    let done = complete(&server, "plain", &id, &[(1, &etag)])
        .await
        .unwrap();
    assert_eq!(done.checksum_type(), None);
    assert_eq!(done.checksum_crc32(), None);
}

#[tokio::test]
async fn an_object_with_a_composite_checksum_can_be_copied() {
    // A copy verifies the bytes it reads against the source's stored checksums. A composite
    // one is a digest of part checksums, not of the bytes, so checking against it would fail
    // every copy of such an object with a 500.
    let server = server().await;
    let client = &server.client;
    let sha256 = ChecksumAlgorithm::Sha256;
    let data = [patterned(5 * MIB), b"tail".to_vec()];
    let (id, listed) = upload_checksummed(&server, "src", &sha256, None, &data).await;
    // A client need not send the object's checksum for the object to get one.
    client
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key("src")
        .upload_id(&id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(listed))
                .build(),
        )
        .send()
        .await
        .unwrap();
    let source = head_checksummed(&server, "src").await;
    assert_eq!(
        source.checksum_sha256(),
        Some(composite(&sha256, &data).as_str())
    );

    client
        .copy_object()
        .bucket(BUCKET)
        .key("copy")
        .copy_source(format!("{BUCKET}/src"))
        .send()
        .await
        .unwrap();
    assert_eq!(get(&server, "copy").await, data.concat());

    let target = create(&server, "assembled").await;
    let copied = client
        .upload_part_copy()
        .bucket(BUCKET)
        .key("assembled")
        .upload_id(&target)
        .part_number(1)
        .copy_source(format!("{BUCKET}/src"))
        .send()
        .await
        .unwrap();
    let etag = copied
        .copy_part_result()
        .unwrap()
        .e_tag()
        .unwrap()
        .to_owned();
    complete(&server, "assembled", &target, &[(1, &etag)])
        .await
        .unwrap();
    assert_eq!(get(&server, "assembled").await, data.concat());
}

#[tokio::test]
async fn a_complete_runs_to_the_end_when_its_client_goes_away() {
    // AWS finishes a Complete once it has started. A client that times out mid-assembly retries
    // and expects the object; dropped with the connection, the assembly left the object
    // unwritten and its data behind as a temporary file. Writes of the object are slowed so the
    // client is surely gone before the assembly ends.
    let mut backend = common::Backend::fs().await;
    backend.op = backend.op.layer(slow_writes::SlowWrites);
    let server = TestServer::start_on(backend, s3dal::Settings::default()).await;
    server.create_bucket_dir(BUCKET).await;
    let id = create(&server, "k").await;
    let data = [patterned(5 * MIB), patterned(5 * MIB), patterned(5 * MIB)];
    let mut listed = String::new();
    for (n, part) in (1..).zip(&data) {
        let etag = upload(&server, "k", &id, n, part).await;
        listed.push_str(&format!(
            "<Part><PartNumber>{n}</PartNumber><ETag>{etag}</ETag></Part>"
        ));
    }
    let body = format!("<CompleteMultipartUpload>{listed}</CompleteMultipartUpload>");
    let payload: String = sha2::Sha256::digest(body.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let query = format!("uploadId={id}");
    let headers = common::signed_headers(
        &server.endpoint,
        "POST",
        &format!("/{BUCKET}/k"),
        &query,
        &[],
        &payload,
    );
    let host = server.endpoint.trim_start_matches("http://");
    let mut stream = tokio::net::TcpStream::connect(host).await.unwrap();
    let mut head = format!(
        "POST /{BUCKET}/k?{query} HTTP/1.1\r\nhost: {host}\r\ncontent-length: {}\r\n",
        body.len()
    );
    for (name, value) in &headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(body.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    while !response.windows(4).any(|w| w == b"\r\n\r\n") {
        let mut buf = [0; 1024];
        let n = stream.read(&mut buf).await.unwrap();
        assert!(n > 0, "connection closed before the response");
        response.extend_from_slice(&buf[..n]);
    }
    assert!(
        response.starts_with(b"HTTP/1.1 200"),
        "{}",
        String::from_utf8_lossy(&response)
    );
    drop(stream);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let head = loop {
        match server
            .client
            .head_object()
            .bucket(BUCKET)
            .key("k")
            .send()
            .await
        {
            Ok(head) => break head,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(err) => panic!("the object never appeared: {err:?}"),
        }
    };
    let parts: Vec<&[u8]> = data.iter().map(Vec::as_slice).collect();
    assert_eq!(head.e_tag(), Some(multipart_etag(&parts).as_str()));
    assert_eq!(get(&server, "k").await, data.concat());
    server.assert_no_temp_files().await;
}

/// Writes of objects take a second each, as on a slow backend; s3dal's own state, under
/// `.s3dal/`, is written at full speed.
mod slow_writes {
    use std::sync::Arc;
    use std::time::Duration;

    use opendal::raw::*;
    use opendal::{Buffer, Capability, Metadata, OperationContext, Result};

    #[derive(Debug)]
    pub struct SlowWrites;

    impl Layer for SlowWrites {
        fn apply_service(&self, inner: Servicer) -> Servicer {
            Arc::new(SlowService { inner })
        }
    }

    #[derive(Debug)]
    struct SlowService {
        inner: Servicer,
    }

    impl Service for SlowService {
        type Reader = oio::Reader;
        type Writer = SlowWriter;
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
            self.inner.read(ctx, path, args)
        }

        fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<SlowWriter> {
            Ok(SlowWriter {
                inner: self.inner.write(ctx, path, args)?,
                slow: !path.starts_with(".s3dal/"),
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

    pub struct SlowWriter {
        inner: oio::Writer,
        slow: bool,
    }

    impl oio::Write for SlowWriter {
        async fn write(&mut self, bs: Buffer) -> Result<()> {
            if self.slow {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            self.inner.write(bs).await
        }

        async fn close(&mut self) -> Result<Metadata> {
            self.inner.close().await
        }

        async fn abort(&mut self) -> Result<()> {
            self.inner.abort().await
        }
    }
}

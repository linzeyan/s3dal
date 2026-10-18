//! Browser form uploads (PostObject): an app's backend signs a POST policy with SigV4 and hands
//! the browser the form fields; the browser posts them with the file straight to s3dal. These
//! tests build that form by hand, as the app would, and send it outside the SDK, which has no
//! PostObject.

mod common;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_sdk_s3::primitives::{DateTime, DateTimeFormat};
use aws_sdk_s3::types::ChecksumMode;
use base64_simd::STANDARD;
use common::{ACCESS_KEY, SECRET_KEY, TestServer, raw_response};
use hmac::{Hmac, KeyInit, Mac};
use md5::{Digest, Md5};
use s3dal::meta::md5_hex;
use serde_json::{Value, json};
use sha2::Sha256;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const BUCKET: &str = "bkt";
const REGION: &str = "us-east-1";

async fn server() -> TestServer {
    let server = TestServer::start().await;
    server.create_bucket_dir(BUCKET).await;
    server
}

fn hmac(key: &[u8], data: &str) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(data.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

fn iso8601(secs: i64) -> String {
    DateTime::from_secs(secs)
        .fmt(DateTimeFormat::DateTime)
        .unwrap()
}

/// The fields an app's backend hands a browser for one upload: the base64 policy and its SigV4
/// signature with the credential, date and algorithm it was made with. `conditions` are the
/// app's own; the ones SigV4 requires for those three fields are added here.
fn sign(mut conditions: Vec<Value>, secret: &str, expires_in: i64) -> Vec<(String, String)> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let amz_date = iso8601(now).replace(['-', ':'], "");
    let date = &amz_date[..8];
    let credential = format!("{ACCESS_KEY}/{date}/{REGION}/s3/aws4_request");
    conditions.extend([
        json!({"x-amz-algorithm": "AWS4-HMAC-SHA256"}),
        json!({"x-amz-credential": credential}),
        json!({"x-amz-date": amz_date}),
    ]);
    let policy = json!({"expiration": iso8601(now + expires_in), "conditions": conditions});
    let policy = STANDARD.encode_to_string(policy.to_string());
    let signing_key = [date, REGION, "s3", "aws4_request"]
        .iter()
        .fold(format!("AWS4{secret}").into_bytes(), |key, part| {
            hmac(&key, part)
        });
    let signature: String = hmac(&signing_key, &policy)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    fields(&[
        ("policy", &policy),
        ("x-amz-algorithm", "AWS4-HMAC-SHA256"),
        ("x-amz-credential", &credential),
        ("x-amz-date", &amz_date),
        ("x-amz-signature", &signature),
    ])
}

/// A policy valid for ten minutes, signed with the server's key.
fn signed(conditions: Vec<Value>) -> Vec<(String, String)> {
    sign(conditions, SECRET_KEY, 600)
}

fn fields(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect()
}

struct Posted {
    status: u16,
    head: String,
    body: String,
}

/// Posts `form` and then the file, which a form upload must send last, as
/// `multipart/form-data` the way browsers encode it.
async fn post(
    server: &TestServer,
    form: &[(String, String)],
    filename: &str,
    file: &[u8],
) -> Posted {
    let (content_type, body) = form_data(form, filename, file);
    post_body(server, &[("content-type", &content_type)], &body).await
}

async fn post_body(server: &TestServer, headers: &[(&str, &str)], body: &[u8]) -> Posted {
    let (status, head, body) = raw_response(
        &server.endpoint,
        "POST",
        &format!("/{BUCKET}"),
        headers,
        body,
    )
    .await;
    Posted {
        status,
        head,
        body: String::from_utf8_lossy(&body).into_owned(),
    }
}

/// The content type and body of the request [`post`] sends.
fn form_data(form: &[(String, String)], filename: &str, file: &[u8]) -> (String, Vec<u8>) {
    let boundary = "----s3dalFormBoundary7MA4YWxkTrZu0gW";
    let mut body = Vec::new();
    for (name, value) in form {
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
            )
            .as_bytes(),
        );
    }
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; \
             filename=\"{filename}\"\r\nContent-Type: application/octet-stream\r\n\r\n"
        )
        .as_bytes(),
    );
    body.extend_from_slice(file);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    (format!("multipart/form-data; boundary={boundary}"), body)
}

/// The value of response header `name` (lowercase) in a header section.
fn header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.lines().find_map(|line| {
        let (n, v) = line.split_once(':')?;
        n.eq_ignore_ascii_case(name).then(|| v.trim())
    })
}

async fn assert_nothing_written(server: &TestServer) {
    assert_eq!(server.names(BUCKET).await, Vec::<String>::new());
    server.assert_no_temp_files().await;
}

#[tokio::test]
async fn a_signed_form_uploads_the_file_with_the_attributes_it_names() {
    // Browser uploads go straight to storage with a form the app signed. The object must come
    // out as the form described it, or the app serves it with the wrong type or loses its
    // metadata; `${filename}` is how a form keeps the name the user picked.
    let server = server().await;
    let mut form = signed(vec![
        json!({"bucket": BUCKET}),
        json!(["starts-with", "$key", "uploads/"]),
        json!(["starts-with", "$Content-Type", "image/"]),
        json!({"x-amz-meta-owner": "alice"}),
        json!(["content-length-range", 1, 1024]),
    ]);
    form.extend(fields(&[
        ("key", "uploads/${filename}"),
        ("Content-Type", "image/png"),
        ("x-amz-meta-owner", "alice"),
    ]));
    let data = b"not really a png";
    let posted = post(&server, &form, "photo.png", data).await;
    assert_eq!(posted.status, 204, "{}", posted.body);

    let got = server
        .client
        .get_object()
        .bucket(BUCKET)
        .key("uploads/photo.png")
        .send()
        .await
        .unwrap();
    assert_eq!(got.content_type(), Some("image/png"));
    let owner = got.metadata().and_then(|m| m.get("owner"));
    assert_eq!(owner.map(String::as_str), Some("alice"));
    assert_eq!(got.e_tag(), Some(format!("\"{}\"", md5_hex(data)).as_str()));
    assert_eq!(&got.body.collect().await.unwrap().into_bytes()[..], data);
}

#[tokio::test]
async fn success_action_status_and_redirect_shape_the_answer() {
    // A plain HTML form navigates to whatever the upload answers: the redirect brings the browser
    // back to the app with the key and ETag, and 201 hands scripts the key that was stored.
    let server = server().await;
    let any = |field: &str| json!(["starts-with", format!("${field}"), ""]);
    let form = |key: &str, extra: &[(&str, &str)]| {
        let mut form = signed(vec![
            any("key"),
            any("success_action_status"),
            any("success_action_redirect"),
        ]);
        form.extend(fields(&[("key", key)]));
        form.extend(fields(extra));
        form
    };
    let etag = md5_hex(b"hello");

    let ok = post(
        &server,
        &form("s200", &[("success_action_status", "200")]),
        "f",
        b"hello",
    )
    .await;
    assert_eq!(ok.status, 200, "{}", ok.body);
    assert!(ok.body.is_empty(), "{}", ok.body);

    let created = post(
        &server,
        &form("s201", &[("success_action_status", "201")]),
        "f",
        b"hello",
    )
    .await;
    assert_eq!(created.status, 201, "{}", created.body);
    // s3s 0.17 writes the ETag unquoted and Location as a path; AWS quotes the one and gives
    // a URL for the other. Only what both carry is checked.
    for element in ["<Bucket>bkt</Bucket>", "<Key>s201</Key>", &etag] {
        assert!(
            created.body.contains(element),
            "{element}: {}",
            created.body
        );
    }

    // AWS answers anything other than 200 and 201 with 204.
    for (key, status) in [("s204", "204"), ("s404", "404")] {
        let posted = post(
            &server,
            &form(key, &[("success_action_status", status)]),
            "f",
            b"hello",
        )
        .await;
        assert_eq!(posted.status, 204, "{status}: {}", posted.body);
    }

    let redirect = "https://app.example/uploaded";
    let redirected = post(
        &server,
        &form("redirected", &[("success_action_redirect", redirect)]),
        "f",
        b"hello",
    )
    .await;
    assert_eq!(redirected.status, 303, "{}", redirected.body);
    let location = header(&redirected.head, "location").unwrap();
    // AWS sends the ETag quoted (`%22...%22`); s3s 0.17 sends it bare.
    let expected = format!("{redirect}?bucket={BUCKET}&key=redirected&etag=");
    assert!(location.starts_with(&expected), "{location}");
    assert!(location.contains(&etag), "{location}");

    for key in ["s200", "s201", "s204", "s404", "redirected"] {
        assert!(server.exists(&format!("{BUCKET}/{key}")).await, "{key}");
    }
}

/// `(case, form, file, status, code)`.
type RefusedForm = (
    &'static str,
    Vec<(String, String)>,
    &'static [u8],
    u16,
    &'static str,
);

#[tokio::test]
async fn forms_outside_their_policy_are_refused_and_write_nothing() {
    // The policy is the app's only say over what a browser uploads with its signature: a key,
    // type or size outside it, or a field it does not name, must not reach storage.
    let server = server().await;
    let conditions = || {
        vec![
            json!(["starts-with", "$key", "uploads/"]),
            json!(["starts-with", "$Content-Type", "image/"]),
            json!(["content-length-range", 1, 8]),
        ]
    };
    let form = |key: &str, content_type: &str, extra: &[(&str, &str)]| {
        let mut form = signed(conditions());
        form.extend(fields(&[("key", key), ("Content-Type", content_type)]));
        form.extend(fields(extra));
        form
    };
    // AWS answers a failed condition with 403 AccessDenied; s3s 0.17 with 400
    // InvalidPolicyDocument. Either way nothing is written, which is what matters here.
    let cases: [RefusedForm; 5] = [
        (
            "key outside the prefix",
            form("elsewhere/x", "image/png", &[]),
            b"small",
            400,
            "InvalidPolicyDocument",
        ),
        (
            "content type outside the prefix",
            form("uploads/x", "text/html", &[]),
            b"small",
            400,
            "InvalidPolicyDocument",
        ),
        (
            "file above the range",
            form("uploads/x", "image/png", &[]),
            b"too large",
            400,
            "EntityTooLarge",
        ),
        (
            "file below the range",
            form("uploads/x", "image/png", &[]),
            b"",
            400,
            "EntityTooSmall",
        ),
        (
            "field the policy does not name",
            form("uploads/x", "image/png", &[("x-amz-meta-extra", "1")]),
            b"small",
            403,
            "AccessDenied",
        ),
    ];
    for (name, form, file, status, code) in cases {
        let posted = post(&server, &form, "f", file).await;
        assert_eq!(posted.status, status, "{name}: {}", posted.body);
        assert!(
            posted.body.contains(&format!("<Code>{code}</Code>")),
            "{name}: {}",
            posted.body
        );
    }
    assert_nothing_written(&server).await;
}

#[tokio::test]
async fn forms_without_a_valid_signature_are_refused() {
    // A signed form is a bearer credential like a presigned URL: a forged, expired or unsigned
    // one must not upload anything. SigV2 forms are refused too, since s3s disables SigV2.
    let server = server().await;
    let key_only = || vec![json!(["starts-with", "$key", ""])];
    let with_key = |mut form: Vec<(String, String)>| {
        form.extend(fields(&[("key", "k")]));
        form
    };
    let sig_v2 = fields(&[
        ("key", "k"),
        ("AWSAccessKeyId", ACCESS_KEY),
        ("policy", &STANDARD.encode_to_string(b"{}")),
        ("signature", "c2lnbmF0dXJl"),
    ]);
    let cases = [
        (
            "wrong secret",
            with_key(sign(key_only(), "not-the-secret", 600)),
            "SignatureDoesNotMatch",
        ),
        (
            "expired policy",
            with_key(sign(key_only(), SECRET_KEY, -60)),
            "AccessDenied",
        ),
        ("unsigned", fields(&[("key", "k")]), "AccessDenied"),
        ("SigV2", sig_v2, "AccessDenied"),
    ];
    for (name, form, code) in cases {
        let posted = post(&server, &form, "f", b"data").await;
        assert_eq!(posted.status, 403, "{name}: {}", posted.body);
        assert!(
            posted.body.contains(&format!("<Code>{code}</Code>")),
            "{name}: {}",
            posted.body
        );
    }
    assert_nothing_written(&server).await;
}

#[tokio::test]
async fn an_unsigned_form_is_refused_before_its_file_is_read() {
    // s3s checks access only once it knows the file's length. Given a Content-Length, it derives
    // that from the header and refuses an unsigned form at once; without one it would first read
    // the whole file into memory, up to 5 GiB, for a client with no right to upload anything.
    // Such a form is refused unread instead, as a PUT without the header is. The files here never
    // end, so an answer that waits for the whole file never comes.
    let server = server().await;
    let (content_type, form) = form_data(&fields(&[("key", "k")]), "f", b"");
    // The form up to the end of the file part's headers, then the file's first bytes.
    let file_start = form.windows(4).rposition(|w| w == b"\r\n\r\n").unwrap() + 4;
    let start = [&form[..file_start], b"the file goes on"].concat();
    let chunk = [format!("{:x}\r\n", start.len()).as_bytes(), &start, b"\r\n"].concat();
    let host = server.endpoint.trim_start_matches("http://");
    for (framing, body, status, code) in [
        ("content-length: 1000000000", &start, 403, "AccessDenied"),
        (
            "transfer-encoding: chunked",
            &chunk,
            411,
            "MissingContentLength",
        ),
    ] {
        let mut stream = TcpStream::connect(host).await.unwrap();
        let head = format!(
            "POST /{BUCKET} HTTP/1.1\r\nhost: {host}\r\ncontent-type: {content_type}\r\n{framing}\r\n\r\n"
        );
        stream.write_all(head.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
        let mut response = Vec::new();
        let read = tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut response));
        read.await
            .unwrap_or_else(|_| panic!("{framing}: no answer before the file ends"))
            .unwrap();
        let response = String::from_utf8_lossy(&response);
        assert!(
            response.starts_with(&format!("HTTP/1.1 {status}")),
            "{framing}: {response}"
        );
        assert!(
            response.contains(&format!("<Code>{code}</Code>")),
            "{framing}: {response}"
        );
    }
    assert_nothing_written(&server).await;
}

#[tokio::test]
async fn checksums_and_content_md5_in_a_form_are_verified() {
    // A form can carry a digest the browser computed. A file damaged on the way must be refused,
    // not stored, and a verified checksum is kept for later downloads to check against.
    let server = server().await;
    let form = |key: &str, extra: &[(&str, &str)]| {
        let mut form = signed(vec![
            json!(["starts-with", "$key", ""]),
            json!(["starts-with", "$Content-MD5", ""]),
            json!(["starts-with", "$x-amz-checksum-crc32", ""]),
        ]);
        form.extend(fields(&[("key", key)]));
        form.extend(fields(extra));
        form
    };
    let md5 = STANDARD.encode_to_string(Md5::digest(b"hello world"));

    let good_md5 = post(
        &server,
        &form("md5", &[("Content-MD5", &md5)]),
        "f",
        b"hello world",
    )
    .await;
    assert_eq!(good_md5.status, 204, "{}", good_md5.body);
    let good_crc = post(
        &server,
        &form("crc", &[("x-amz-checksum-crc32", "DUoRhQ==")]),
        "f",
        b"hello world",
    )
    .await;
    assert_eq!(good_crc.status, 204, "{}", good_crc.body);
    let head = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("crc")
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap();
    assert_eq!(head.checksum_crc32(), Some("DUoRhQ=="));

    for (name, extra) in [
        ("Content-MD5", ("Content-MD5", md5.as_str())),
        ("CRC32", ("x-amz-checksum-crc32", "DUoRhQ==")),
    ] {
        let posted = post(&server, &form("bad", &[extra]), "f", b"hello there").await;
        assert_eq!(posted.status, 400, "{name}: {}", posted.body);
        assert!(
            posted.body.contains("<Code>BadDigest</Code>"),
            "{name}: {}",
            posted.body
        );
    }
    assert!(!server.exists(&format!("{BUCKET}/bad")).await);
    server.assert_no_temp_files().await;

    // A Content-MD5 header on the request covers the whole form, not the file, so it must not
    // be checked against the file.
    let (content_type, body) = form_data(&form("whole", &[]), "f", b"hello world");
    let whole_md5 = STANDARD.encode_to_string(Md5::digest(&body));
    let headers = [
        ("content-type", content_type.as_str()),
        ("content-md5", whole_md5.as_str()),
    ];
    let posted = post_body(&server, &headers, &body).await;
    assert_eq!(posted.status, 204, "{}", posted.body);
    assert!(server.exists(&format!("{BUCKET}/whole")).await);
}

#[tokio::test]
async fn form_fields_s3dal_cannot_honor_are_refused() {
    // As for PUT: a form asking for public access, tags, encryption or an owner check must not
    // get a success for an object that has none of them. s3s 0.17 reads these under their
    // header names only; AWS's own `acl` and `tagging` form fields never reach s3dal.
    let server = server().await;
    for (field, value) in [
        ("x-amz-acl", "public-read"),
        (
            "x-amz-tagging",
            "<Tagging><TagSet><Tag><Key>a</Key><Value>b</Value></Tag></TagSet></Tagging>",
        ),
        ("x-amz-server-side-encryption", "AES256"),
        ("x-amz-expected-bucket-owner", "123456789012"),
    ] {
        let mut form = signed(vec![
            json!(["starts-with", "$key", ""]),
            json!({ field: value }),
        ]);
        form.extend(fields(&[("key", "k"), (field, value)]));
        let posted = post(&server, &form, "f", b"data").await;
        assert_eq!(posted.status, 501, "{field}: {}", posted.body);
        assert!(
            posted.body.contains("<Code>NotImplemented</Code>"),
            "{field}: {}",
            posted.body
        );
    }
    assert_nothing_written(&server).await;

    // The canned ACLs PUT accepts are accepted here too.
    let mut form = signed(vec![
        json!(["starts-with", "$key", ""]),
        json!({"x-amz-acl": "private"}),
    ]);
    form.extend(fields(&[("key", "k"), ("x-amz-acl", "private")]));
    let posted = post(&server, &form, "f", b"data").await;
    assert_eq!(posted.status, 204, "{}", posted.body);
}

#[tokio::test]
async fn form_fields_no_header_can_carry_are_refused() {
    // A form may name any field, while a header name or value cannot hold just anything. Stored,
    // such a field would fail every GET and HEAD of the object with a 500.
    let server = server().await;
    for (field, value) in [
        ("x-amz-meta-original name", "photo.png"),
        ("Content-Type", "image/png\nX-Injected: 1"),
        ("Cache-Control", "no-cache\u{1}"),
    ] {
        let policy_field = field.to_ascii_lowercase();
        let mut form = signed(vec![
            json!(["starts-with", "$key", ""]),
            json!(["starts-with", format!("${policy_field}"), ""]),
        ]);
        form.extend(fields(&[("key", "k"), (field, value)]));
        let posted = post(&server, &form, "f", b"data").await;
        assert_eq!(posted.status, 400, "{field}: {}", posted.body);
        assert!(
            posted.body.contains("<Code>InvalidArgument</Code>"),
            "{field}: {}",
            posted.body
        );
    }
    assert_nothing_written(&server).await;
}

#[tokio::test]
async fn an_empty_file_is_stored_as_an_empty_put_would_be() {
    // Forms are often submitted without a file chosen; the empty object must get the same ETag
    // an empty PUT gets, or clients comparing ETags see two different empty objects.
    let server = server().await;
    let mut form = signed(vec![json!(["starts-with", "$key", ""])]);
    form.extend(fields(&[("key", "empty")]));
    let posted = post(&server, &form, "f", b"").await;
    assert_eq!(posted.status, 204, "{}", posted.body);
    let head = server
        .client
        .head_object()
        .bucket(BUCKET)
        .key("empty")
        .send()
        .await
        .unwrap();
    assert_eq!(head.content_length(), Some(0));
    assert_eq!(head.e_tag(), Some(format!("\"{}\"", md5_hex(b"")).as_str()));
}

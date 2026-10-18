mod common;

use std::future::poll_fn;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use aws_sdk_s3::Client;
use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::RequestId;
use aws_sdk_s3::presigning::{PresignedRequest, PresigningConfig};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_smithy_http_client::tls::rustls_provider::CryptoMode;
use aws_smithy_http_client::tls::{Provider, TlsContext, TrustStore};
use bytes::Bytes;
use common::{ACCESS_KEY, Backend, SECRET_KEY, TestServer, client, client_config, raw_request};
use h2::client::SendRequest;
use hyper::{Request, Version};
use opendal::ErrorKind;
use opendal::options::WriteOptions;
use s3dal::config::KeyPair;
use s3dal::{Profile, S3Dal, Settings, server};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_rustls::rustls::crypto::aws_lc_rs;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::{TlsAcceptor, TlsConnector};

#[tokio::test]
async fn requests_signed_with_a_wrong_secret_are_rejected() {
    // There is no anonymous mode; a request that fails SigV4 must never reach the backend.
    let server = TestServer::start().await;
    let intruder = client(&server.endpoint, ACCESS_KEY, "wrong-secret");
    let err = intruder.list_buckets().send().await.unwrap_err();
    assert_eq!(err.code(), Some("SignatureDoesNotMatch"), "{err:?}");
}

#[tokio::test]
async fn requests_with_an_unknown_access_key_are_rejected() {
    let server = TestServer::start().await;
    let stranger = client(&server.endpoint, "unknown-access", "whatever");
    let err = stranger.list_buckets().send().await.unwrap_err();
    assert_eq!(err.code(), Some("InvalidAccessKeyId"), "{err:?}");
}

#[tokio::test]
async fn unsigned_requests_are_rejected() {
    // s3dal replaces s3s's default access check; the replacement must still refuse a request
    // that carries no signature at all.
    let server = TestServer::start().await;
    server.create_bucket_dir("bkt").await;
    server.plant("bkt", "k", b"secret").await;
    for (method, path) in [("GET", "/"), ("GET", "/bkt?list-type=2"), ("GET", "/bkt/k")] {
        let (status, body) = raw_request(&server.endpoint, method, path, &[], b"").await;
        assert_eq!(status, 403, "{method} {path}");
        let body = String::from_utf8_lossy(&body);
        assert!(body.contains("<Code>AccessDenied</Code>"), "{body}");
    }
    let (status, _) = raw_request(&server.endpoint, "PUT", "/bkt/new", &[], b"data").await;
    assert_eq!(status, 403);
    assert!(!server.exists("bkt/new").await);
}

/// Sends a presigned request as a browser or `curl` would: only the URL and the headers the
/// signature covers, no SDK.
async fn send_presigned(
    server: &TestServer,
    presigned: &PresignedRequest,
    path_and_query: Option<&str>,
    body: &[u8],
) -> (u16, Vec<u8>) {
    let signed = presigned.uri().strip_prefix(&server.endpoint).unwrap();
    let headers: Vec<(&str, &str)> = presigned.headers().collect();
    raw_request(
        &server.endpoint,
        presigned.method(),
        path_and_query.unwrap_or(signed),
        &headers,
        body,
    )
    .await
}

fn presign_for(secs: u64) -> PresigningConfig {
    PresigningConfig::expires_in(Duration::from_secs(secs)).unwrap()
}

#[tokio::test]
async fn presigned_urls_download_and_upload_without_credentials() {
    // Presigned URLs are how apps hand a browser one download or upload without sharing keys.
    let server = TestServer::start().await;
    server.create_bucket_dir("bkt").await;
    server.plant("bkt", "report.csv", b"a,b\n1,2\n").await;

    let get = server
        .client
        .get_object()
        .bucket("bkt")
        .key("report.csv")
        .presigned(presign_for(300))
        .await
        .unwrap();
    let (status, body) = send_presigned(&server, &get, None, b"").await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    assert_eq!(body, b"a,b\n1,2\n");

    let put = server
        .client
        .put_object()
        .bucket("bkt")
        .key("upload.bin")
        .presigned(presign_for(300))
        .await
        .unwrap();
    let (status, body) = send_presigned(&server, &put, None, b"from a browser").await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let got = server
        .client
        .get_object()
        .bucket("bkt")
        .key("upload.bin")
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.e_tag(),
        Some(format!("\"{}\"", s3dal::meta::md5_hex(b"from a browser")).as_str())
    );
    assert_eq!(
        &got.body.collect().await.unwrap().into_bytes()[..],
        b"from a browser"
    );
}

#[tokio::test]
async fn presigned_urls_grant_only_what_was_signed_and_only_until_they_expire() {
    // A presigned URL is a bearer credential; changing what it names or using it late must not
    // reach the object.
    let server = TestServer::start().await;
    server.create_bucket_dir("bkt").await;
    server.plant("bkt", "public", b"shared").await;
    server.plant("bkt", "private", b"not shared").await;

    let get = server
        .client
        .get_object()
        .bucket("bkt")
        .key("public")
        .presigned(presign_for(300))
        .await
        .unwrap();
    let signed = get.uri().strip_prefix(&server.endpoint).unwrap();
    let other_key = signed.replacen("/bkt/public", "/bkt/private", 1);
    let (status, body) = send_presigned(&server, &get, Some(&other_key), b"").await;
    assert_eq!(status, 403);
    let body = String::from_utf8_lossy(&body);
    assert!(
        body.contains("<Code>SignatureDoesNotMatch</Code>"),
        "{body}"
    );

    let expired = PresigningConfig::builder()
        .start_time(SystemTime::now() - Duration::from_secs(3600))
        .expires_in(Duration::from_secs(60))
        .build()
        .unwrap();
    let late = server
        .client
        .get_object()
        .bucket("bkt")
        .key("public")
        .presigned(expired)
        .await
        .unwrap();
    let (status, body) = send_presigned(&server, &late, None, b"").await;
    assert_eq!(status, 403);
    let body = String::from_utf8_lossy(&body);
    assert!(body.contains("<Code>AccessDenied</Code>"), "{body}");
}

#[tokio::test]
async fn expected_bucket_owner_checks_are_not_implemented() {
    // The header guards against acting on a bucket another account owns. Buckets here have no
    // owning account id, so the check cannot be made, and ignoring it would act anyway. It is
    // refused for every operation, before any of them runs.
    let server = TestServer::start().await;
    server.create_bucket_dir("bkt").await;
    let owner = "123456789012";
    let list = server
        .client
        .list_objects_v2()
        .bucket("bkt")
        .expected_bucket_owner(owner)
        .send()
        .await
        .unwrap_err();
    assert_eq!(list.code(), Some("NotImplemented"), "{list:?}");
    let put = server
        .client
        .put_object()
        .bucket("bkt")
        .key("k")
        .expected_bucket_owner(owner)
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(put.code(), Some("NotImplemented"), "{put:?}");
    assert!(!server.exists("bkt/k").await);
    let delete = server
        .client
        .delete_bucket()
        .bucket("bkt")
        .expected_bucket_owner(owner)
        .send()
        .await
        .unwrap_err();
    assert_eq!(delete.code(), Some("NotImplemented"), "{delete:?}");
    assert!(server.exists("bkt/").await);
}

#[tokio::test]
async fn each_backend_is_probed_to_the_profile_it_is_known_to_have() {
    // Tests that differ by backend take their expectations from the probed profile, so a probe
    // that misjudged a backend would pass them while serving other ETags, listings or
    // directories than intended. Pinning the profile per scheme catches that.
    let server = TestServer::start().await;
    let scheme = std::env::var("S3DAL_TEST_BACKEND").unwrap_or_else(|_| "fs".to_owned());
    let gateway = Profile {
        native_etag: false,
        native_content_type: false,
        native_cache_control: false,
        native_content_disposition: false,
        native_content_encoding: false,
        encoded_metadata_names: false,
        ordered_list: false,
        real_dirs: true,
        conditional_writes: false,
    };
    let expected = match scheme.as_str() {
        "fs" => gateway,
        "s3" => Profile {
            native_etag: true,
            native_content_type: true,
            native_cache_control: true,
            native_content_disposition: true,
            native_content_encoding: true,
            encoded_metadata_names: false,
            ordered_list: true,
            real_dirs: false,
            conditional_writes: true,
        },
        // Azure's ETag is an opaque version tag, OpenDAL sets neither its Content-Disposition
        // nor its Content-Encoding, its metadata names must be C# identifiers, and OpenDAL
        // cannot list it after a key.
        "azblob" => Profile {
            native_etag: false,
            native_content_type: true,
            native_cache_control: true,
            native_content_disposition: false,
            native_content_encoding: false,
            encoded_metadata_names: true,
            ordered_list: false,
            real_dirs: false,
            conditional_writes: true,
        },
        // GCS's ETag is opaque too (fake-gcs-server's is a base64 MD5), and OpenDAL cannot set
        // its Content-Disposition. GCS serves gzip content decompressed even to a ranged read,
        // so it probes to a gateway Content-Encoding; fake-gcs-server serves a ranged read as
        // stored, so here it is native.
        "gcs" => Profile {
            native_etag: false,
            native_content_type: true,
            native_cache_control: true,
            native_content_disposition: false,
            native_content_encoding: true,
            encoded_metadata_names: false,
            ordered_list: true,
            real_dirs: false,
            conditional_writes: true,
        },
        other => panic!("no known profile for {other}; add the one it should have"),
    };
    assert_eq!(server.profile, expected);
}

#[tokio::test]
async fn a_backend_trusted_with_write_conditions_checks_them_on_multipart_writes_too() {
    // The probe measures conditions on writes of one request. A write past one chunk reaches
    // the backend as a multipart upload, whose completion must check them as well: otherwise a
    // large conditional PUT would hold only inside one s3dal process.
    let backend = Backend::from_env().await;
    let op = &backend.op;
    op.write("k", b"old".to_vec()).await.unwrap();
    let old = op.stat("k").await.unwrap();
    op.write("k", b"new".to_vec()).await.unwrap();
    let chunk = 5 * 1024 * 1024;
    let write_large = |opts: WriteOptions| async move {
        let opts = WriteOptions {
            chunk: Some(chunk),
            ..opts
        };
        let mut w = op.writer_options("k", opts).await?;
        w.write(vec![7u8; chunk + 1]).await?;
        w.close().await
    };

    let create = write_large(WriteOptions {
        if_not_exists: true,
        ..Default::default()
    })
    .await;
    assert_eq!(
        create.map(|_| ()).unwrap_err().kind(),
        ErrorKind::ConditionNotMatch
    );
    // fs cannot check a version at all, and s3dal does not ask it to.
    if backend.profile.conditional_writes {
        let replace = write_large(WriteOptions {
            if_not_changed: Some(old),
            ..Default::default()
        })
        .await;
        assert_eq!(
            replace.map(|_| ()).unwrap_err().kind(),
            ErrorKind::ConditionNotMatch
        );
    }
    assert_eq!(op.read("k").await.unwrap().to_vec(), b"new");
}

fn pair(access_key: &str, secret_key: &str) -> KeyPair {
    KeyPair {
        access_key: access_key.to_owned(),
        secret_key: secret_key.to_owned(),
    }
}

#[tokio::test]
async fn every_configured_key_pair_signs_requests() {
    // Separate key pairs let each client or team hold, and later lose, a secret of its own.
    let keys = [pair("alice", "alice-secret"), pair("bob", "bob-secret")];
    let server =
        TestServer::start_serving(Backend::from_env().await, Settings::default(), &keys, None)
            .await;
    let alice = client(&server.endpoint, "alice", "alice-secret");
    let bob = client(&server.endpoint, "bob", "bob-secret");
    alice.create_bucket().bucket("bkt").send().await.unwrap();
    alice
        .put_object()
        .bucket("bkt")
        .key("k")
        .body(ByteStream::from_static(b"shared"))
        .send()
        .await
        .unwrap();
    // One tenant: every pair reaches the same buckets.
    let got = bob
        .get_object()
        .bucket("bkt")
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(
        &got.body.collect().await.unwrap().into_bytes()[..],
        b"shared"
    );

    // A secret signs only for its own access key.
    let mixed = client(&server.endpoint, "bob", "alice-secret");
    let err = mixed.list_buckets().send().await.unwrap_err();
    assert_eq!(err.code(), Some("SignatureDoesNotMatch"), "{err:?}");
    let stranger = client(&server.endpoint, "carol", "alice-secret");
    let err = stranger.list_buckets().send().await.unwrap_err();
    assert_eq!(err.code(), Some("InvalidAccessKeyId"), "{err:?}");
}

#[tokio::test]
async fn build_service_refuses_missing_or_duplicate_key_pairs() {
    // Embedders call build_service without the CLI's checks. It must neither serve without
    // keys nor leave which of two secrets for one access key applies to lookup order.
    let backend = Backend::fs().await;
    let dal = || S3Dal::new(backend.op.clone(), backend.profile, Settings::default());
    assert!(server::build_service(dal(), &[], &[]).is_err());
    let twice = [pair("a", "one"), pair("a", "two")];
    assert!(server::build_service(dal(), &twice, &[]).is_err());
}

/// A self-signed certificate for 127.0.0.1 and its key, as the PEM files `--tls-cert` and
/// `--tls-key` name.
struct TestCert {
    dir: tempfile::TempDir,
    pem: String,
    der: CertificateDer<'static>,
}

impl TestCert {
    fn new() -> Self {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_owned()]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("cert.pem"), cert.pem()).unwrap();
        std::fs::write(dir.path().join("key.pem"), signing_key.serialize_pem()).unwrap();
        Self {
            dir,
            pem: cert.pem(),
            der: cert.der().clone(),
        }
    }

    fn cert(&self) -> PathBuf {
        self.dir.path().join("cert.pem")
    }

    fn key(&self) -> PathBuf {
        self.dir.path().join("key.pem")
    }

    fn acceptor(&self) -> TlsAcceptor {
        server::tls_acceptor(&self.cert(), &self.key()).unwrap()
    }
}

async fn start_https(cert: &TestCert) -> TestServer {
    let keys = [pair(ACCESS_KEY, SECRET_KEY)];
    let backend = Backend::from_env().await;
    TestServer::start_serving(backend, Settings::default(), &keys, Some(cert.acceptor())).await
}

/// An SDK client that trusts `cert` and nothing else. It offers HTTP/2 and HTTP/1.1 by ALPN,
/// as the SDK's default client does.
fn https_client(server: &TestServer, cert: &TestCert) -> Client {
    let trust = TrustStore::empty().with_pem_certificate(cert.pem.as_bytes());
    let http = aws_smithy_http_client::Builder::new()
        .tls_provider(Provider::Rustls(CryptoMode::AwsLc))
        .tls_context(
            TlsContext::builder()
                .with_trust_store(trust)
                .build()
                .unwrap(),
        )
        .build_https();
    let config = client_config(&server.endpoint, ACCESS_KEY, SECRET_KEY).http_client(http);
    Client::from_conf(config.build())
}

#[tokio::test]
async fn https_serves_clients_that_trust_the_certificate() {
    // TLS keeps credentials and data private between client and gateway without a reverse
    // proxy in front.
    let cert = TestCert::new();
    let server = start_https(&cert).await;
    assert!(
        server.endpoint.starts_with("https://"),
        "{}",
        server.endpoint
    );
    let client = https_client(&server, &cert);
    client.create_bucket().bucket("bkt").send().await.unwrap();
    client
        .put_object()
        .bucket("bkt")
        .key("k")
        .body(ByteStream::from_static(b"over tls"))
        .send()
        .await
        .unwrap();
    let got = client
        .get_object()
        .bucket("bkt")
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(
        &got.body.collect().await.unwrap().into_bytes()[..],
        b"over tls"
    );
}

#[tokio::test]
async fn plaintext_requests_to_an_https_port_get_no_http_response() {
    // A client set up with http:// by mistake must fail rather than be served in the clear.
    let cert = TestCert::new();
    let server = start_https(&cert).await;
    let addr = server.endpoint.trim_start_matches("https://");
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nhost: 127.0.0.1\r\nconnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    // The server may close with a TLS alert or reset the connection; either way it ends.
    let read = tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut response));
    let _ = read.await.expect("the server closes the connection");
    assert!(
        !response.starts_with(b"HTTP/"),
        "{}",
        String::from_utf8_lossy(&response)
    );
    // The failed handshake ends only that connection.
    https_client(&server, &cert)
        .list_buckets()
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn https_offers_http2_and_http1_by_alpn() {
    // Clients choose HTTP/2 during the TLS handshake; one that speaks only HTTP/1.1 must still
    // be served, as on plaintext.
    let cert = TestCert::new();
    let server = start_https(&cert).await;
    let addr = server.endpoint.trim_start_matches("https://");

    let h2 = tls_connect(addr, &cert, &[b"h2", b"http/1.1"]).await;
    assert_eq!(h2.get_ref().1.alpn_protocol(), Some(&b"h2"[..]));

    // A client that offers no protocol at all speaks HTTP/1.1, which HTTP/2 over TLS rules out.
    for offer in [&[&b"http/1.1"[..]][..], &[]] {
        let mut h1 = tls_connect(addr, &cert, offer).await;
        assert_eq!(h1.get_ref().1.alpn_protocol(), offer.first().copied());
        h1.write_all(b"GET / HTTP/1.1\r\nhost: 127.0.0.1\r\nconnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        let _ = h1.read_to_end(&mut response).await;
        // Unsigned, so refused, but answered over HTTP/1.1.
        assert!(
            response.starts_with(b"HTTP/1.1 403"),
            "{offer:?}: {}",
            String::from_utf8_lossy(&response)
        );
    }
}

/// Completes a TLS handshake with the server at `addr`, offering the given ALPN protocols.
async fn tls_connect(
    addr: &str,
    cert: &TestCert,
    offer: &[&[u8]],
) -> tokio_rustls::client::TlsStream<TcpStream> {
    let mut roots = RootCertStore::empty();
    roots.add(cert.der.clone()).unwrap();
    let mut config = ClientConfig::builder_with_provider(Arc::new(aws_lc_rs::default_provider()))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = offer.iter().map(|p| p.to_vec()).collect();
    let tcp = TcpStream::connect(addr).await.unwrap();
    let name = ServerName::try_from("127.0.0.1").unwrap();
    TlsConnector::from(Arc::new(config))
        .connect(name, tcp)
        .await
        .unwrap()
}

#[tokio::test]
async fn stalled_connections_are_closed() {
    // A client that connects and stalls must not hold its socket for good: enough of them
    // exhaust the server's file descriptors.
    let keys = [pair(ACCESS_KEY, SECRET_KEY)];
    let cert = TestCert::new();
    let http = TestServer::start_fs().await;
    let https = TestServer::start_serving(
        Backend::fs().await,
        Settings::default(),
        &keys,
        Some(cert.acceptor()),
    )
    .await;
    let addr = |server: &TestServer| {
        let endpoint = &server.endpoint;
        endpoint[endpoint.find("://").unwrap() + 3..].to_owned()
    };
    let silent = TcpStream::connect(addr(&http)).await.unwrap();
    let mut partial = TcpStream::connect(addr(&http)).await.unwrap();
    partial
        .write_all(b"GET / HTTP/1.1\r\nhost: 127.0.0.1\r\n")
        .await
        .unwrap();
    let mut preface = TcpStream::connect(addr(&http)).await.unwrap();
    preface.write_all(b"PRI * HTTP/2.0\r\n").await.unwrap();
    let silent_tls = TcpStream::connect(addr(&https)).await.unwrap();
    let silent_after_tls = tls_connect(&addr(&https), &cert, &[b"http/1.1"]).await;
    let silent_after_h2 = tls_connect(&addr(&https), &cert, &[b"h2"]).await;
    // A request answered on a later connection means each server has accepted the stalled ones
    // before it (the listen queue is first in, first out) and started their timers.
    let (status, _) = raw_request(&http.endpoint, "GET", "/", &[], b"").await;
    assert_eq!(status, 403);
    https_client(&https, &cert)
        .list_buckets()
        .send()
        .await
        .unwrap();

    // Jump past the server's 30 seconds at once, then wait in real time. Left paused, the clock
    // would also jump to the test's own deadline before the runtime saw a close, which loopback
    // delivers only after the timer that caused it has run.
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(31)).await;
    tokio::time::resume();
    // With whether the server speaks first: over HTTP/2 it sends its settings before it reads
    // the client's preface.
    let stalled: [(&str, bool, Box<dyn AsyncRead + Unpin>); 6] = [
        ("silent", false, Box::new(silent)),
        ("partial headers", false, Box::new(partial)),
        ("partial HTTP/2 preface", false, Box::new(preface)),
        ("silent over TLS", false, Box::new(silent_tls)),
        ("silent after TLS", false, Box::new(silent_after_tls)),
        ("silent after TLS, h2", true, Box::new(silent_after_h2)),
    ];
    for (what, speaks_first, mut stream) in stalled {
        let mut rest = Vec::new();
        let read = tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut rest));
        // Closed or reset, either way ended, and with no response.
        let _ = read.await.unwrap_or_else(|_| panic!("{what}: still open"));
        assert!(
            speaks_first || rest.is_empty(),
            "{what}: {}",
            String::from_utf8_lossy(&rest)
        );
    }
}

/// Opens an HTTP/2 connection with prior knowledge, as clients do on plaintext, and drives it on
/// a task of its own, which ends with the connection.
async fn h2_connect(addr: &str) -> (SendRequest<Bytes>, JoinHandle<Result<(), h2::Error>>) {
    let tcp = TcpStream::connect(addr).await.unwrap();
    let (requests, connection) = h2::client::handshake(tcp).await.unwrap();
    (requests, tokio::spawn(connection))
}

/// Sends an unsigned `GET /`, which is refused, and reads the whole answer.
async fn h2_refused_get(requests: &mut SendRequest<Bytes>, endpoint: &str) {
    poll_fn(|cx| requests.poll_ready(cx)).await.unwrap();
    let get = Request::get(format!("{endpoint}/"))
        .version(Version::HTTP_2)
        .body(())
        .unwrap();
    let (response, _) = requests.send_request(get, true).unwrap();
    let response = response.await.unwrap();
    assert_eq!(response.status(), 403);
    let mut body = response.into_body();
    while let Some(chunk) = body.data().await {
        chunk.unwrap();
    }
}

#[tokio::test]
async fn idle_http2_connections_are_closed_but_busy_ones_are_not() {
    // An HTTP/2 client that keeps its connection open and sends nothing must not hold the socket
    // for good. A connection with a request still running must stay, or a long download would be
    // cut short. (An upload that stalls as long ends by its own timeout; see
    // a_request_body_that_stalls_ends_with_request_timeout.)
    let server = TestServer::start_fs().await;
    server.create_bucket_dir("bkt").await;
    // Far more than the server can hand over before the client reads. hyper gives the stream
    // each chunk the backend reads, up to 2 MiB on fs, whatever the client's window, and waits
    // only for the next one; a body handed over whole is no longer in flight.
    let content = vec![7; 8 << 20];
    server.plant("bkt", "k", &content).await;
    let addr = server.endpoint.trim_start_matches("http://");

    let (mut idle, idle_connection) = h2_connect(addr).await;
    h2_refused_get(&mut idle, &server.endpoint).await;

    // Presigned, so no SigV4 headers need computing here.
    let get = server
        .client
        .get_object()
        .bucket("bkt")
        .key("k")
        .presigned(presign_for(300))
        .await
        .unwrap();
    let mut request = Request::get(get.uri()).version(Version::HTTP_2);
    for (name, value) in get.headers() {
        request = request.header(name, value);
    }
    let (mut busy, _busy_connection) = h2_connect(addr).await;
    poll_fn(|cx| busy.poll_ready(cx)).await.unwrap();
    let (downloaded, _) = busy.send_request(request.body(()).unwrap(), true).unwrap();
    // The headers have come; the rest of the body waits for the client to read.
    let downloaded = downloaded.await.unwrap();
    assert_eq!(downloaded.status(), 200);

    // As in stalled_connections_are_closed: past the server's 30 seconds at once, then real time.
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(31)).await;
    tokio::time::resume();

    tokio::time::timeout(Duration::from_secs(10), idle_connection)
        .await
        .expect("the idle connection is still open")
        .unwrap()
        .unwrap();
    // It was told to go away, rather than cut off: a client then opens a new connection.
    let Err(err) = idle.ready().await else {
        panic!("the idle connection still takes requests");
    };
    assert!(
        err.is_go_away() && err.is_remote() && err.reason() == Some(h2::Reason::NO_ERROR),
        "{err:?}"
    );

    let mut body = downloaded.into_body();
    let mut got = Vec::new();
    while let Some(chunk) = body.data().await {
        let chunk = chunk.unwrap();
        body.flow_control().release_capacity(chunk.len()).unwrap();
        got.extend_from_slice(&chunk);
    }
    assert!(got == content, "{} of {} bytes", got.len(), content.len());
    // Nor was it told to go away later: it still takes requests.
    h2_refused_get(&mut busy, &server.endpoint).await;
}

/// The head of a presigned PUT of `bkt/k` with an 8-byte body.
async fn presigned_put_head(server: &TestServer) -> String {
    let put = server
        .client
        .put_object()
        .bucket("bkt")
        .key("k")
        .presigned(presign_for(300))
        .await
        .unwrap();
    let host = server.endpoint.trim_start_matches("http://");
    let path = put.uri().strip_prefix(&server.endpoint).unwrap();
    let mut head = format!("PUT {path} HTTP/1.1\r\nhost: {host}\r\ncontent-length: 8\r\n");
    for (name, value) in put.headers() {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head
}

/// Sends the request head `head` with `Expect: 100-continue` and returns the connection once
/// the server has answered 100 Continue, which it does as it starts to read the body.
async fn until_continue(host: &str, head: &str) -> TcpStream {
    let mut stream = TcpStream::connect(host).await.unwrap();
    let head = format!("{head}expect: 100-continue\r\n\r\n");
    stream.write_all(head.as_bytes()).await.unwrap();
    let mut interim = Vec::new();
    while !interim.ends_with(b"\r\n\r\n") {
        interim.push(stream.read_u8().await.unwrap());
    }
    assert!(
        interim.starts_with(b"HTTP/1.1 100 "),
        "{}",
        String::from_utf8_lossy(&interim)
    );
    stream
}

/// What the server sends on `stream` until it closes it, which must be within ten seconds.
async fn rest_of(mut stream: TcpStream) -> String {
    let mut rest = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut rest));
    read.await.expect("the server answers and closes").unwrap();
    String::from_utf8_lossy(&rest).into_owned()
}

#[tokio::test]
async fn a_request_body_that_stalls_ends_with_request_timeout() {
    // A client that stops halfway through a body must not hold its request, socket and task for
    // good, nor keep an HTTP/2 connection from being closed as idle. Unsigned form uploads count
    // too: s3s reads their fields before it checks access. AWS answers 400 RequestTimeout.
    let server = TestServer::start_fs().await;
    server.create_bucket_dir("bkt").await;
    let host = server.endpoint.trim_start_matches("http://");
    let form = format!(
        "POST /bkt HTTP/1.1\r\nhost: {host}\r\ncontent-type: multipart/form-data; boundary=b\r\n\
         content-length: 1000\r\n"
    );
    let mut stalled = Vec::new();
    for head in [presigned_put_head(&server).await, form] {
        stalled.push(until_continue(host, &head).await);
    }

    // As in stalled_connections_are_closed: past the server's 30 seconds at once, then real time.
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(31)).await;
    tokio::time::resume();
    for stream in stalled {
        let response = rest_of(stream).await;
        assert!(response.starts_with("HTTP/1.1 400"), "{response}");
        assert!(
            response.contains("<Code>RequestTimeout</Code>"),
            "{response}"
        );
    }
    assert!(!server.exists("bkt/k").await);
    server.assert_no_temp_files().await;
}

#[tokio::test]
async fn new_connections_are_refused_while_shutdown_drains() {
    // While the requests in flight finish, a new client must be refused at once, so it goes to
    // another instance. Still listening, the server would have the kernel queue it and reset it
    // when the drain ends, up to ten seconds later and after it may have sent its request.
    let mut server = TestServer::start_fs().await;
    server.create_bucket_dir("bkt").await;
    let host = server.endpoint.trim_start_matches("http://").to_owned();
    let mut upload = until_continue(&host, &presigned_put_head(&server).await).await;
    upload.write_all(b"half").await.unwrap();

    server.shut_down();
    // A connect that meets the listener closing is reset rather than refused (macOS); either
    // way the client learns at once.
    let refused = async {
        loop {
            match TcpStream::connect(&host).await {
                Err(err)
                    if matches!(
                        err.kind(),
                        std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::ConnectionReset
                    ) =>
                {
                    return;
                }
                Err(err) => panic!("{err}"),
                Ok(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), refused)
        .await
        .expect("new connections are still taken while the server drains");

    // The upload in flight still completes.
    upload.write_all(b"rest").await.unwrap();
    let response = rest_of(upload).await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert_eq!(server.read("bkt/k").await, b"halfrest");
}

#[tokio::test]
async fn an_idle_http2_client_that_ignores_the_goaway_is_dropped() {
    // Shutting an idle HTTP/2 connection down gracefully ends only once the client has answered
    // the server's ping. A client that never answers must not keep its socket that way.
    let server = TestServer::start_fs().await;
    let addr = server.endpoint.trim_start_matches("http://");
    let mut silent = TcpStream::connect(addr).await.unwrap();
    // The preface and an empty SETTINGS frame, then nothing, not even an acknowledgement.
    silent
        .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\0\0\0\x04\0\0\0\0\0")
        .await
        .unwrap();
    // As in stalled_connections_are_closed: answered later, so the server has taken the preface.
    let (status, _) = raw_request(&server.endpoint, "GET", "/", &[], b"").await;
    assert_eq!(status, 403);

    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(31)).await;
    tokio::time::resume();
    // Frames up to the GOAWAY: its 9-byte header holds the payload length and the type, 7.
    let mut header = [0; 9];
    while header[3] != 7 {
        silent.read_exact(&mut header).await.unwrap();
        let len = u32::from_be_bytes([0, header[0], header[1], header[2]]);
        let mut payload = vec![0; len as usize];
        silent.read_exact(&mut payload).await.unwrap();
    }

    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(6)).await;
    tokio::time::resume();
    let mut rest = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(10), silent.read_to_end(&mut rest));
    // Closed or reset, either way ended.
    let _ = read.await.expect("the connection is still open");
}

#[tokio::test]
async fn an_error_answered_before_the_body_is_read_reaches_the_client() {
    // A request refused before its body is read, here for its bucket name, is answered while the
    // client is still sending the body. Closing a socket on unread data makes the kernel reset
    // the connection, and the client's next write fails: curl reports "Send failure: Broken
    // pipe" and never shows the error. The server must read the rest and discard it.
    let server = TestServer::start_fs().await;
    let host = server.endpoint.trim_start_matches("http://").to_owned();
    let (mut reader, mut writer) = TcpStream::connect(&host).await.unwrap().into_split();
    // More than the socket buffers of both ends hold together, so it is all sent only if the
    // server reads it.
    let body = vec![0; 16 << 20];
    let upload = tokio::spawn(async move {
        let head = format!(
            "PUT /b/k HTTP/1.1\r\nhost: {host}\r\ncontent-length: {}\r\n\r\n",
            body.len()
        );
        writer.write_all(head.as_bytes()).await?;
        writer.write_all(&body).await
    });
    let mut response = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(10), reader.read_to_end(&mut response));
    let _ = read.await.expect("the server closes the connection");
    let sent = upload.await.unwrap();
    let response = String::from_utf8_lossy(&response);
    assert!(sent.is_ok(), "{sent:?}, response: {response}");
    assert!(response.starts_with("HTTP/1.1 400"), "{response}");
    assert!(
        response.contains("<Code>InvalidBucketName</Code>"),
        "{response}"
    );
}

/// What the binary logs after the lines [`start_binary`] returns.
type Log = tokio::io::Lines<tokio::io::BufReader<tokio::process::ChildStderr>>;

/// Starts the `s3dal` binary on fs at `root`, with its log on a pipe and, when given, at most
/// `max_files` file descriptors. Returns it with the log lines up to the one that says it
/// listens, and the rest of the log.
async fn start_binary(
    root: &std::path::Path,
    max_files: Option<u32>,
) -> (tokio::process::Child, Vec<String>, Log) {
    use tokio::io::AsyncBufReadExt;
    let bin = env!("CARGO_BIN_EXE_s3dal");
    let mut command = match max_files {
        None => tokio::process::Command::new(bin),
        // A shell builtin; exec keeps the limit for s3dal.
        Some(n) => {
            let mut sh = tokio::process::Command::new("/bin/sh");
            sh.arg("-c")
                .arg(format!("ulimit -n {n} && exec \"$0\" \"$@\""))
                .arg(bin);
            sh
        }
    };
    let mut child = command
        .args(["serve", "--backend", "fs", "--address", "127.0.0.1:0"])
        .arg("--backend-opt")
        .arg(format!("root={}", root.display()))
        .env_clear()
        .env("S3DAL_ACCESS_KEY", ACCESS_KEY)
        .env("S3DAL_SECRET_KEY", SECRET_KEY)
        .env("RUST_LOG", "info")
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut log = tokio::io::BufReader::new(child.stderr.take().unwrap()).lines();
    let listening = async {
        let mut lines = Vec::new();
        while let Some(line) = log.next_line().await.unwrap() {
            let done = line.contains("listening on");
            lines.push(line);
            if done {
                return lines;
            }
        }
        panic!("s3dal exited before listening: {lines:#?}");
    };
    let lines = tokio::time::timeout(Duration::from_secs(30), listening)
        .await
        .expect("s3dal listens");
    (child, lines, log)
}

#[cfg(unix)]
#[tokio::test]
async fn the_binary_stops_gracefully_on_sigterm() {
    // Docker, systemd and Kubernetes stop services with SIGTERM. Killed by it, s3dal would cut
    // requests off mid-upload, and as PID 1 in a container it would not stop at all.
    let root = tempfile::tempdir().unwrap();
    let (mut child, _, _) = start_binary(root.path(), None).await;
    let pid = child.id().unwrap().to_string();
    let kill = std::process::Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .unwrap();
    assert!(kill.success());
    let status = tokio::time::timeout(Duration::from_secs(15), child.wait())
        .await
        .expect("s3dal exits on SIGTERM")
        .unwrap();
    assert!(status.success(), "{status}");
}

#[tokio::test]
async fn logs_written_to_a_pipe_carry_no_color_codes() {
    // `docker logs`, journald and log files are not terminals; escape codes there garble every
    // line and break searching the log for a request id.
    let root = tempfile::tempdir().unwrap();
    let (_child, lines, _) = start_binary(root.path(), None).await;
    for line in &lines {
        assert!(!line.contains('\x1b'), "{line:?}");
    }
    assert!(lines.iter().any(|l| l.contains("backend=fs")), "{lines:#?}");
}

#[cfg(unix)]
#[tokio::test]
async fn a_server_out_of_file_descriptors_waits_before_accepting_again() {
    // Out of file descriptors, accept fails. Linux leaves the connection queued and the listener
    // ready, so retried at once, accept fails again in a loop that pins a core and floods the
    // log; macOS resets the connection instead, so retried at once, it resets every queued
    // client. The server must wait instead, and serve again once connections have closed.
    let root = tempfile::tempdir().unwrap();
    // About a dozen go to the runtime, the listener and stdio; the connections below take the
    // rest, and the server holds each until its first bytes are due.
    let (_child, lines, mut log) = start_binary(root.path(), Some(32)).await;
    let endpoint = lines
        .last()
        .and_then(|line| line.split("listening on ").nth(1))
        .unwrap()
        .to_owned();
    let addr = endpoint.trim_start_matches("http://");
    let mut held = Vec::new();
    for _ in 0..48 {
        held.push(TcpStream::connect(addr).await.unwrap());
    }
    let mut failed = 0;
    let count = async {
        while let Some(line) = log.next_line().await.unwrap() {
            if line.contains("cannot accept connection") {
                failed += 1;
            }
        }
    };
    let _ = tokio::time::timeout(Duration::from_secs(3), count).await;
    assert!(failed > 0, "the server never ran out of file descriptors");
    // One attempt a second, and the one that failed first.
    assert!(failed <= 5, "{failed} failed accepts in 3 seconds");

    drop(held);
    // Read on, so the server never blocks writing its log.
    tokio::spawn(async move { while let Ok(Some(_)) = log.next_line().await {} });
    // The server frees its descriptors only as it notices the held connections closed, and
    // until then macOS resets what it fails to accept, so a client may have to try again.
    let served = async {
        loop {
            match status_of_get(addr).await {
                Ok(status) => return status,
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    };
    let status = tokio::time::timeout(Duration::from_secs(10), served)
        .await
        .expect("the server takes connections again");
    assert_eq!(status, 403);
}

/// The status of an unsigned `GET /`, or the error that ended the connection first.
async fn status_of_get(addr: &str) -> std::io::Result<u16> {
    let mut stream = TcpStream::connect(addr).await?;
    let head = format!("GET / HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\n\r\n");
    stream.write_all(head.as_bytes()).await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    response
        .get(9..12)
        .and_then(|code| std::str::from_utf8(code).ok()?.parse().ok())
        .ok_or_else(|| std::io::Error::other("no status line"))
}

#[tokio::test]
async fn temporary_files_of_cut_off_writes_are_removed_at_startup() {
    // An fs write goes to a temporary file first, which a shutdown or a crash mid-write leaves
    // behind with nothing to remove it, so restarts would fill the disk. A file still being
    // written, by another instance on the same root, has a recent mtime and must stay.
    let root = tempfile::tempdir().unwrap();
    let tmp = root.path().join(".s3dal").join("tmp");
    std::fs::create_dir_all(&tmp).unwrap();
    let (cut_off, live) = (tmp.join("object.cut-off"), tmp.join("object.live"));
    for path in [&cut_off, &live] {
        std::fs::write(path, b"partial").unwrap();
    }
    // Older than the default stale-upload age of a day.
    let written = SystemTime::now() - Duration::from_secs(25 * 60 * 60);
    std::fs::File::options()
        .write(true)
        .open(&cut_off)
        .unwrap()
        .set_modified(written)
        .unwrap();

    let (_child, mut lines, mut log) = start_binary(root.path(), None).await;
    let swept = async {
        loop {
            if let Some(line) = lines.iter().find(|l| l.contains("stale temporary files")) {
                return line.clone();
            }
            lines.push(log.next_line().await.unwrap().expect("s3dal runs"));
        }
    };
    let line = tokio::time::timeout(Duration::from_secs(10), swept)
        .await
        .expect("the startup sweep removes the stale file");
    assert!(line.contains("removed=1"), "{line}");
    assert!(!cut_off.exists());
    assert!(live.exists());
}

#[test]
fn help_does_not_show_secrets_from_the_environment() {
    // `--help` shows each flag's environment variable with its current value, and help output
    // ends up in terminals, CI logs and bug reports. Backend options carry upstream credentials,
    // as the secret key is s3dal's own.
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_s3dal"))
        .args(["serve", "--help"])
        .env_clear()
        .env(
            "S3DAL_BACKEND_OPTS",
            "root=/srv,secret_access_key=upstream-secret",
        )
        .env("S3DAL_SECRET_KEY", "gateway-secret")
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let help = String::from_utf8(output.stdout).unwrap();
    // Still named, so operators know the variables exist.
    for name in ["S3DAL_BACKEND_OPTS", "S3DAL_SECRET_KEY"] {
        assert!(help.contains(name), "{help}");
    }
    for secret in ["upstream-secret", "gateway-secret"] {
        assert!(!help.contains(secret), "{help}");
    }
}

#[tokio::test]
async fn an_unusable_certificate_or_key_fails_at_startup() {
    // Found at startup, the error names the file. Found at the first handshake, every client
    // fails with an error that names nothing on the server.
    let one = TestCert::new();
    let other = TestCert::new();
    let Err(err) = server::tls_acceptor(&one.cert(), &other.key()) else {
        panic!("a key of another certificate was accepted");
    };
    assert!(
        format!("{err:#}").contains(&other.key().display().to_string()),
        "{err:#}"
    );
    let missing = one.dir.path().join("missing.pem");
    let Err(err) = server::tls_acceptor(&missing, &one.key()) else {
        panic!("a missing certificate was accepted");
    };
    assert!(format!("{err:#}").contains("missing.pem"), "{err:#}");
}

fn is_request_id(id: &str) -> bool {
    id.len() == 16 && id.bytes().all(|b| matches!(b, b'0'..=b'9' | b'A'..=b'F'))
}

/// The request id of an error response: from its `x-amz-request-id` header, and from the
/// `RequestId` of its document.
fn request_ids<E>(err: &SdkError<E, HttpResponse>) -> (String, String) {
    let raw = err.raw_response().expect("an HTTP response");
    let header = raw
        .headers()
        .get("x-amz-request-id")
        .expect("x-amz-request-id");
    let body = String::from_utf8_lossy(raw.body().bytes().expect("a buffered error body"));
    let doc = body
        .split_once("<RequestId>")
        .and_then(|(_, rest)| rest.split_once("</RequestId>"))
        .unwrap_or_else(|| panic!("no RequestId in {body}"))
        .0;
    (header.to_owned(), doc.to_owned())
}

#[tokio::test]
async fn every_response_carries_its_own_request_id() {
    // Clients log the id of each request so that a failure can be found on the server; an id
    // shared by two requests would point at the wrong one.
    let server = TestServer::start().await;
    let first = server.client.list_buckets().send().await.unwrap();
    let second = server.client.list_buckets().send().await.unwrap();
    let (first, second) = (first.request_id().unwrap(), second.request_id().unwrap());
    assert!(is_request_id(first), "{first}");
    assert!(is_request_id(second), "{second}");
    assert_ne!(first, second);
    // A HEAD error has no document; the header is all the client gets.
    let err = server
        .client
        .head_bucket()
        .bucket("missing")
        .send()
        .await
        .unwrap_err();
    let id = err
        .raw_response()
        .unwrap()
        .headers()
        .get("x-amz-request-id");
    assert!(id.is_some_and(is_request_id), "{err:?}");
}

#[tokio::test]
async fn error_documents_carry_the_request_id_of_their_response() {
    // A client may report the id from either place, and both must lead to the same request
    // (ceph s3-tests `test_object_requestid_matches_header_on_error`). Errors come from s3dal
    // and from s3s itself, before s3dal sees the request; both must carry it.
    let server = TestServer::start().await;
    server.create_bucket_dir("bkt").await;
    let from_s3dal = server
        .client
        .get_object()
        .bucket("bkt")
        .key("missing")
        .send()
        .await
        .unwrap_err();
    assert_eq!(from_s3dal.code(), Some("NoSuchKey"), "{from_s3dal:?}");
    let stranger = client(&server.endpoint, "unknown-access", "whatever");
    let from_auth = stranger.list_buckets().send().await.unwrap_err();
    assert_eq!(
        from_auth.code(),
        Some("InvalidAccessKeyId"),
        "{from_auth:?}"
    );
    let from_routing = server
        .client
        .list_objects_v2()
        .bucket("Not_A_Bucket")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        from_routing.code(),
        Some("InvalidBucketName"),
        "{from_routing:?}"
    );

    let ids = [
        request_ids(&from_s3dal),
        request_ids(&from_auth),
        request_ids(&from_routing),
    ];
    for (header, doc) in &ids {
        assert!(is_request_id(header), "{header}");
        assert_eq!(header, doc);
    }
    assert_ne!(ids[0].0, ids[1].0);
}

#[tokio::test]
async fn an_error_document_inside_a_complete_200_carries_the_request_id() {
    // CompleteMultipartUpload reports a failure while assembling in the body of a 200 that has
    // already been sent. Its id must still match the header, or the failure cannot be traced.
    // The failure is a key/prefix conflict on fs, as in tests/multipart.rs.
    let server = TestServer::start_fs().await;
    server.create_bucket_dir("bkt").await;
    server.plant("bkt", "a/b", b"child").await;
    let upload = server
        .client
        .create_multipart_upload()
        .bucket("bkt")
        .key("a")
        .send()
        .await
        .unwrap();
    let id = upload.upload_id().unwrap();
    let part = server
        .client
        .upload_part()
        .bucket("bkt")
        .key("a")
        .upload_id(id)
        .part_number(1)
        .body(ByteStream::from_static(b"parent"))
        .send()
        .await
        .unwrap();
    let parts = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(part.e_tag().unwrap())
                .build(),
        )
        .build();
    let err = server
        .client
        .complete_multipart_upload()
        .bucket("bkt")
        .key("a")
        .upload_id(id)
        .multipart_upload(parts)
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.raw_response().unwrap().status().as_u16(), 200);
    let (header, doc) = request_ids(&err);
    assert!(is_request_id(&header), "{header}");
    assert_eq!(header, doc);
}

/// What a tracing subscriber writes.
#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Logs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn error_logs_carry_the_request_id() {
    // The id a client reports is useful only if the server log shows it next to the error.
    // Whatever is logged while a request runs is in its span; s3s logs a refused signature at
    // the default level. The subscriber is this thread's, and the test runtime runs the server
    // on this thread.
    let logs = Logs::default();
    let sink = logs.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .with_writer(move || sink.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    // tracing caches, for all threads, whether a call site's events are wanted. While only one
    // subscriber exists it asks the thread that reaches the site first, which may be another
    // test's, with no subscriber. With a second one it asks each event's own thread instead.
    let _second = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());

    let server = TestServer::start().await;
    let stranger = client(&server.endpoint, "unknown-access", "whatever");
    let err = stranger.list_buckets().send().await.unwrap_err();
    let (id, _) = request_ids(&err);
    let text = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    let line = text
        .lines()
        .find(|line| line.contains(&id))
        .unwrap_or_else(|| panic!("{id} is not in the log:\n{text}"));
    assert!(line.contains("InvalidAccessKeyId"), "{line}");
}

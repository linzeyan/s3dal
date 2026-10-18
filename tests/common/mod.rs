//! Shared harness: a real s3dal server on 127.0.0.1, driven by the AWS SDK, so tests cover the
//! same HTTP, SigV4 and checksum paths that real clients take.
//!
//! The backend is fs on a temporary root unless `S3DAL_TEST_BACKEND` names another OpenDAL
//! scheme, configured by `S3DAL_TEST_BACKEND_OPTS` in the server's `key=value,...` form
//! (`scripts/with-rustfs.sh` runs the suite against an S3 upstream this way). Every server gets a
//! fresh root, so tests stay independent on a shared upstream as they are on fs.

// Each test binary uses a different subset of the harness.
#![allow(dead_code)]

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use aws_sdk_s3::Client;
use aws_sdk_s3::config::{self, BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::{DateTime, DateTimeFormat};
use hmac::{Hmac, KeyInit, Mac};
use opendal::{ErrorKind, HttpTransporter, Operator};
use opendal_http_transport_reqwest::ReqwestTransport;
use s3dal::config::KeyPair;
use s3dal::{Profile, S3Dal, Settings, backend, server};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_rustls::TlsAcceptor;

pub const ACCESS_KEY: &str = "s3dal-test-access";
pub const SECRET_KEY: &str = "s3dal-test-secret";

/// A probed backend with a root of its own.
pub struct Backend {
    pub op: Operator,
    pub profile: Profile,
    /// Kept so an fs root lives as long as the backend.
    fs_root: Option<tempfile::TempDir>,
}

impl Backend {
    /// The backend the suite runs against; see the module docs.
    pub async fn from_env() -> Self {
        let scheme = std::env::var("S3DAL_TEST_BACKEND").unwrap_or_else(|_| "fs".to_owned());
        if scheme == "fs" {
            return Self::fs().await;
        }
        let mut opts: Vec<(String, String)> = std::env::var("S3DAL_TEST_BACKEND_OPTS")
            .unwrap_or_default()
            .split(',')
            .filter(|kv| !kv.is_empty())
            .map(|kv| {
                let (k, v) = kv.split_once('=').unwrap_or_else(|| {
                    panic!("S3DAL_TEST_BACKEND_OPTS: expected key=value, found {kv:?}")
                });
                (k.to_owned(), v.to_owned())
            })
            .collect();
        let base = opts
            .iter()
            .position(|(k, _)| k == "root")
            .map(|i| opts.remove(i).1)
            .unwrap_or_default();
        let root = format!(
            "{}/s3dal-test-{}/",
            base.trim_end_matches('/'),
            uuid::Uuid::new_v4()
        );
        opts.push(("root".to_owned(), root));
        // See the reqwest dev-dependency in Cargo.toml.
        let transport = HttpTransporter::new(ReqwestTransport::new(reqwest::Client::new()));
        let op = backend::build_operator(&scheme, &opts).unwrap();
        let op = op
            .clone()
            .with_context(op.base_context().with_http_transport(transport));
        let profile = backend::probe(&op).await.unwrap();
        Self {
            op,
            profile,
            fs_root: None,
        }
    }

    /// fs on a fresh temporary root, whatever the suite's backend is, for tests about fs
    /// itself: files planted or inspected behind s3dal's back, directory mtimes.
    pub async fn fs() -> Self {
        let root = tempfile::tempdir().unwrap();
        let op = backend::build_operator(
            "fs",
            &[(
                "root".to_owned(),
                root.path().to_string_lossy().into_owned(),
            )],
        )
        .unwrap();
        let profile = backend::probe(&op).await.unwrap();
        Self {
            op,
            profile,
            fs_root: Some(root),
        }
    }
}

pub struct TestServer {
    pub endpoint: String,
    pub client: Client,
    /// The backend behind s3dal, for tests that inspect or plant objects behind its back.
    pub op: Operator,
    pub profile: Profile,
    /// The server's own handle, for work it does outside requests, such as the stale-upload
    /// sweep, which tests run once instead of waiting for a background schedule.
    pub dal: S3Dal,
    fs_root: Option<tempfile::TempDir>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl TestServer {
    pub async fn start() -> Self {
        Self::start_with(Settings::default()).await
    }

    pub async fn start_with(settings: Settings) -> Self {
        Self::start_on(Backend::from_env().await, settings).await
    }

    /// A server on fs regardless of the suite's backend; see [`Backend::fs`].
    pub async fn start_fs() -> Self {
        Self::start_on(Backend::fs().await, Settings::default()).await
    }

    pub async fn start_on(backend: Backend, settings: Settings) -> Self {
        let keys = [KeyPair {
            access_key: ACCESS_KEY.to_owned(),
            secret_key: SECRET_KEY.to_owned(),
        }];
        Self::start_serving(backend, settings, &keys, None).await
    }

    /// A server that accepts `keys`, over HTTPS when `tls` is given. Its `client` signs with
    /// the first pair; over HTTPS it does not trust the certificate, so tests bring their own.
    pub async fn start_serving(
        backend: Backend,
        settings: Settings,
        keys: &[KeyPair],
        tls: Option<TlsAcceptor>,
    ) -> Self {
        let Backend {
            op,
            profile,
            fs_root,
        } = backend;
        let dal = S3Dal::new(op.clone(), profile, settings);
        let service = server::build_service(dal.clone(), keys, &[]).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let scheme = if tls.is_some() { "https" } else { "http" };
        let endpoint = format!("{scheme}://{}", listener.local_addr().unwrap());
        let (tx, rx) = oneshot::channel::<()>();
        tokio::spawn(server::serve(listener, service, tls, async move {
            let _ = rx.await;
        }));

        let client = client(&endpoint, &keys[0].access_key, &keys[0].secret_key);
        Self {
            endpoint,
            client,
            op,
            profile,
            dal,
            fs_root,
            shutdown: Some(tx),
        }
    }

    /// Path of `rel` inside the fs root. Only for servers from [`Self::start_fs`].
    pub fn fs_path(&self, rel: &str) -> PathBuf {
        self.fs_root
            .as_ref()
            .expect("this test inspects fs itself; start it with TestServer::start_fs")
            .path()
            .join(rel)
    }

    /// Creates a bucket behind s3dal's back, so object and listing tests do not depend on
    /// CreateBucket.
    pub async fn create_bucket_dir(&self, bucket: &str) {
        self.op.create_dir(&format!("{bucket}/")).await.unwrap();
    }

    /// Writes an object behind s3dal's back, as other tools sharing the backend would. Such
    /// objects carry no s3dal metadata.
    pub async fn plant(&self, bucket: &str, key: &str, data: &[u8]) {
        self.op
            .write(&format!("{bucket}/{key}"), data.to_vec())
            .await
            .unwrap();
    }

    /// The content of the object at backend path `path`.
    pub async fn read(&self, path: &str) -> Vec<u8> {
        self.op.read(path).await.unwrap().to_vec()
    }

    /// Whether anything is at `path`, a file, or a directory when `path` ends in `/`.
    pub async fn exists(&self, path: &str) -> bool {
        self.op.exists(path).await.unwrap()
    }

    /// The names directly inside directory `dir` (`""` for the root), sorted, with
    /// subdirectories named without their trailing `/`. A missing directory has none.
    pub async fn names(&self, dir: &str) -> Vec<String> {
        let dir = if dir.is_empty() {
            String::new()
        } else {
            format!("{}/", dir.trim_end_matches('/'))
        };
        let entries = match self.op.list(&dir).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == ErrorKind::NotFound => return Vec::new(),
            Err(err) => panic!("cannot list {dir:?}: {err}"),
        };
        let mut names: Vec<String> = entries
            .iter()
            .filter_map(|e| e.path().strip_prefix(dir.as_str()))
            .map(|name| name.trim_end_matches('/').to_owned())
            .filter(|name| !name.is_empty() && name != "/")
            .collect();
        names.sort();
        names
    }

    /// Failed writes must not leave temporary files behind in s3dal's own area. Only fs
    /// writes through `.s3dal/tmp`; elsewhere it stays empty by construction.
    pub async fn assert_no_temp_files(&self) {
        assert_eq!(self.names(".s3dal/tmp").await, Vec::<String>::new());
    }

    /// Starts the server's shutdown, as SIGTERM does the binary's, while the backend stays.
    pub fn shut_down(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.shut_down();
    }
}

/// Sends one HTTP/1.1 request outside the SDK, for what the SDK will not send itself: unsigned
/// requests and presigned URLs. Returns the status and the body; a chunked body comes back with
/// its framing, which is fine for the substring checks tests make on error documents.
pub async fn raw_request(
    endpoint: &str,
    method: &str,
    path_and_query: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> (u16, Vec<u8>) {
    let (status, _, body) = raw_response(endpoint, method, path_and_query, headers, body).await;
    (status, body)
}

/// [`raw_request`] that also returns the response's header section as text, for responses
/// whose headers are the point, such as a redirect.
pub async fn raw_response(
    endpoint: &str,
    method: &str,
    path_and_query: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> (u16, String, Vec<u8>) {
    let host = endpoint.trim_start_matches("http://");
    let mut stream = TcpStream::connect(host).await.unwrap();
    let mut head = format!(
        "{method} {path_and_query} HTTP/1.1\r\nhost: {host}\r\ncontent-length: {}\r\nconnection: close\r\n",
        body.len()
    );
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let split = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response has a header section");
    let status = std::str::from_utf8(&response[9..12])
        .unwrap()
        .parse()
        .unwrap();
    let head = String::from_utf8_lossy(&response[..split]).into_owned();
    (status, head, response[split + 4..].to_vec())
}

/// Sends one request signed with SigV4 in its headers, as SDKs sign, for a body the SDK would
/// never send, such as an aws-chunked body whose trailer does not match its data. `query` is
/// the canonical query string (sorted, encoded); `payload` is the `x-amz-content-sha256` value.
pub async fn signed_request(
    endpoint: &str,
    method: &str,
    path: &str,
    query: &str,
    headers: &[(&str, &str)],
    payload: &str,
    body: &[u8],
) -> (u16, Vec<u8>) {
    let sent = signed_headers(endpoint, method, path, query, headers, payload);
    let sent: Vec<(&str, &str)> = sent.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let target = if query.is_empty() {
        path.to_owned()
    } else {
        format!("{path}?{query}")
    };
    raw_request(endpoint, method, &target, &sent, body).await
}

/// The headers of a [`signed_request`] but `host`, which [`raw_request`] sends itself, for
/// tests that drive the connection by hand.
pub fn signed_headers(
    endpoint: &str,
    method: &str,
    path: &str,
    query: &str,
    headers: &[(&str, &str)],
    payload: &str,
) -> Vec<(String, String)> {
    let hmac = |key: &[u8], data: &str| {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
        mac.update(data.as_bytes());
        mac.finalize().into_bytes().to_vec()
    };
    let hex = |bytes: &[u8]| -> String { bytes.iter().map(|b| format!("{b:02x}")).collect() };
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let amz_date = DateTime::from_secs(now.as_secs() as i64)
        .fmt(DateTimeFormat::DateTime)
        .unwrap()
        .replace(['-', ':'], "");
    let date = &amz_date[..8];
    let mut signed: Vec<(String, String)> = headers
        .iter()
        .map(|(name, value)| (name.to_ascii_lowercase(), (*value).to_owned()))
        .chain([
            (
                "host".to_owned(),
                endpoint.trim_start_matches("http://").to_owned(),
            ),
            ("x-amz-content-sha256".to_owned(), payload.to_owned()),
            ("x-amz-date".to_owned(), amz_date.clone()),
        ])
        .collect();
    signed.sort();
    let canonical_headers: String = signed.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
    let names = signed
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical = format!("{method}\n{path}\n{query}\n{canonical_headers}\n{names}\n{payload}");
    let scope = format!("{date}/us-east-1/s3/aws4_request");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex(&Sha256::digest(canonical))
    );
    let key = [date, "us-east-1", "s3", "aws4_request"]
        .iter()
        .fold(format!("AWS4{SECRET_KEY}").into_bytes(), |key, part| {
            hmac(&key, part)
        });
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={ACCESS_KEY}/{scope}, SignedHeaders={names}, Signature={}",
        hex(&hmac(&key, &to_sign))
    );
    signed.retain(|(k, _)| k != "host");
    signed.push(("authorization".to_owned(), authorization));
    signed
}

/// `data` framed as aws-chunked in one chunk, followed by the trailer `name:value`, as SDKs send
/// a file upload with `STREAMING-UNSIGNED-PAYLOAD-TRAILER`. Returns the body and the headers
/// that announce it.
pub fn aws_chunked(data: &[u8], trailer: &str) -> (Vec<u8>, Vec<(&'static str, String)>) {
    let mut body = format!("{:x}\r\n", data.len()).into_bytes();
    body.extend_from_slice(data);
    body.extend_from_slice(format!("\r\n0\r\n{trailer}\r\n\r\n").as_bytes());
    let name = trailer.split_once(':').unwrap().0.to_owned();
    let headers = vec![
        ("content-encoding", "aws-chunked".to_owned()),
        ("x-amz-decoded-content-length", data.len().to_string()),
        ("x-amz-trailer", name),
    ];
    (body, headers)
}

/// A client with the SDK's default behavior, including CRC32 request checksums, because that
/// is what current AWS SDKs send in the wild.
pub fn client(endpoint: &str, access_key: &str, secret_key: &str) -> Client {
    Client::from_conf(client_config(endpoint, access_key, secret_key).build())
}

/// The configuration [`client`] builds, for tests that change part of it.
pub fn client_config(endpoint: &str, access_key: &str, secret_key: &str) -> config::Builder {
    aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .credentials_provider(Credentials::new(
            access_key,
            secret_key,
            None,
            None,
            "s3dal-tests",
        ))
        .force_path_style(true)
}

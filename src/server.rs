use std::collections::HashMap;
use std::future::{Future, poll_fn};
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{self, Poll, ready};
use std::time::Duration;

use anyhow::Context;
use bytes::Bytes;
use hyper::body::{Frame, Incoming, SizeHint};
use hyper::header::{CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, HeaderMap, HeaderValue};
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use s3s::access::{S3Access, S3AccessContext};
use s3s::auth::{S3Auth, SecretKey};
use s3s::dto::PostObjectInput;
use s3s::header::X_AMZ_REQUEST_ID;
use s3s::host::MultiDomain;
use s3s::service::{S3Service, S3ServiceBuilder};
use s3s::{Body, HttpError, HttpResponse, S3Error, S3Request, S3Result, StdError, s3_error};
use socket2::{SockRef, TcpKeepalive};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Handle;
use tokio::sync::{oneshot, watch};
use tokio::time::{Instant, Sleep};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::crypto::aws_lc_rs;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tracing::Instrument;

use crate::S3Dal;
use crate::config::{KeyPair, check_key_pairs};

pub fn build_service(
    dal: S3Dal,
    key_pairs: &[KeyPair],
    domains: &[String],
) -> anyhow::Result<S3Service> {
    let mut b = S3ServiceBuilder::new(dal);
    b.set_auth(KeyAuth::new(key_pairs)?);
    b.set_access(Access);
    if !domains.is_empty() {
        b.set_host(
            MultiDomain::new(domains)
                .context("invalid domain for virtual-hosted-style requests")?,
        );
    }
    Ok(b.build())
}

/// s3s's `SimpleAuth` holds one key pair and answers unknown keys with `NotSignedUp`. AWS S3,
/// and the `S3Auth` contract itself, use `InvalidAccessKeyId`, which clients and s3-tests
/// expect.
struct KeyAuth {
    secrets: HashMap<String, SecretKey>,
}

impl KeyAuth {
    fn new(key_pairs: &[KeyPair]) -> anyhow::Result<Self> {
        check_key_pairs(key_pairs).map_err(anyhow::Error::msg)?;
        let secrets = key_pairs
            .iter()
            .map(|p| (p.access_key.clone(), SecretKey::from(p.secret_key.as_str())))
            .collect();
        Ok(Self { secrets })
    }
}

#[async_trait::async_trait]
impl S3Auth for KeyAuth {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        self.secrets.get(access_key).cloned().ok_or_else(|| {
            s3_error!(
                InvalidAccessKeyId,
                "The access key ID you provided does not exist in our records."
            )
        })
    }
}

/// Runs before any operation, so what applies to every operation is decided once.
struct Access;

#[async_trait::async_trait]
impl S3Access for Access {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        // What s3s's default check does; replacing the check must not open anonymous access.
        if cx.credentials().is_none() {
            return Err(s3_error!(AccessDenied, "Signature is required"));
        }
        // A guard against acting on a bucket another account owns. Buckets here have no owning
        // account id, so the comparison cannot be made, and skipping it would act anyway.
        let expected_owner = [
            "x-amz-expected-bucket-owner",
            "x-amz-source-expected-bucket-owner",
        ];
        if expected_owner.iter().any(|h| cx.headers().contains_key(*h)) {
            return Err(expected_owner_not_implemented());
        }
        Ok(())
    }

    /// A form upload names the expected owner in a form field, which `check` does not see.
    async fn post_object(&self, req: &mut S3Request<PostObjectInput>) -> S3Result<()> {
        if req.input.expected_bucket_owner.is_some() {
            return Err(expected_owner_not_implemented());
        }
        Ok(())
    }
}

fn expected_owner_not_implemented() -> S3Error {
    s3_error!(
        NotImplemented,
        "expected bucket owner checks are not supported"
    )
}

/// The Rust SDK sends a streamed upload's Content-Encoding and its own `aws-chunked` as two
/// header lines, which s3s refuses as a duplicate header. Content-Encoding is a list, so HTTP
/// defines the two lines as equal to one comma-joined line (RFC 9110, section 5.3), and SigV4
/// signs repeated lines as their trimmed values joined by commas, so the signature still holds.
fn join_content_encoding(headers: &mut HeaderMap) {
    let lines = headers.get_all(CONTENT_ENCODING);
    if lines.iter().nth(1).is_none() {
        return;
    }
    // A line that is not text could not have been signed either; s3s refuses it unchanged.
    let Some(values) = lines
        .iter()
        .map(|v| v.to_str().ok().map(str::trim))
        .collect::<Option<Vec<_>>>()
    else {
        return;
    };
    if let Ok(joined) = HeaderValue::from_str(&values.join(",")) {
        headers.insert(CONTENT_ENCODING, joined);
    }
}

/// Identifies one request to the client (`x-amz-request-id`, and `RequestId` in an error
/// document) and in the server log (the request's span), so a client's report of a failure can
/// be found in the log. 16 uppercase hex digits, as AWS formats it; random rather than counted,
/// so ids stay unique across restarts and across instances.
#[derive(Clone)]
pub(crate) struct RequestId(String);

impl RequestId {
    fn new() -> Self {
        Self(format!("{:016X}", uuid::Uuid::new_v4().as_u64_pair().0))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    /// s3s writes error documents itself, also for errors that never reach s3dal (signatures,
    /// malformed requests), and has no hook for a request id, so the id goes into the finished
    /// document. s3s builds every error document in memory; a streamed body is left alone.
    fn stamp(&self, res: &mut HttpResponse) {
        let value = HeaderValue::from_str(&self.0).expect("hex digits are a valid header value");
        res.headers_mut().insert(X_AMZ_REQUEST_ID, value);
        // An object's content may end like an error document; only errors are rewritten.
        if res.status().is_success() {
            return;
        }
        let Some(doc) = res.body().bytes() else {
            return;
        };
        let Some(head) = doc.strip_suffix(b"</Error>") else {
            return;
        };
        let mut stamped = Vec::with_capacity(doc.len() + 40);
        stamped.extend_from_slice(head);
        stamped.extend_from_slice(b"<RequestId>");
        stamped.extend_from_slice(self.0.as_bytes());
        stamped.extend_from_slice(b"</RequestId></Error>");
        *res.body_mut() = Body::from(stamped);
        // hyper derives the length from the new body.
        res.headers_mut().remove(CONTENT_LENGTH);
    }
}

/// The scheme of the connection a request came on, for answers that name a URL
/// (CompleteMultipartUpload's Location). An HTTP/1.1 request line carries no scheme.
#[derive(Clone, Copy)]
pub(crate) struct Scheme(pub(crate) &'static str);

/// HTTPS with a PEM certificate chain and its private key. Both are read and matched here, so a
/// wrong path or a key that does not belong to the certificate stops the server at startup
/// rather than failing every handshake.
pub fn tls_acceptor(cert: &Path, key: &Path) -> anyhow::Result<TlsAcceptor> {
    let chain = CertificateDer::pem_file_iter(cert)
        .and_then(|certs| certs.collect::<Result<Vec<_>, _>>())
        .with_context(|| format!("cannot read TLS certificate {}", cert.display()))?;
    anyhow::ensure!(!chain.is_empty(), "no certificate in {}", cert.display());
    let key_der = PrivateKeyDer::from_pem_file(key)
        .with_context(|| format!("cannot read TLS private key {}", key.display()))?;
    // An explicit provider: the process default is ambiguous, and `builder()` panics, once
    // another dependency enables rustls's ring backend as well.
    let mut config = ServerConfig::builder_with_provider(Arc::new(aws_lc_rs::default_provider()))
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(chain, key_der)
        .with_context(|| {
            format!(
                "cannot use TLS certificate {} with private key {}",
                cert.display(),
                key.display()
            )
        })?;
    // The auto builder serves either; offering h2 first gives HTTP/2 clients what they get on
    // plaintext too.
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// How long a client may stay silent after connecting, take over the TLS handshake, take to
/// send a request's headers, or pause in a request's body. Without a bound, a client that
/// connects and sends nothing, or stops halfway through a body, holds a socket and a task until
/// it leaves, and enough of them exhaust the file descriptors. 30 seconds is hyper's default for
/// the headers; a healthy upload pauses for a few retransmission timeouts (1, 2, 4, 8 seconds)
/// at most, even over a lossy link.
const STALL_TIMEOUT: Duration = Duration::from_secs(30);

/// How long the accept loop waits after the listener fails other than for one client's
/// connection. Out of file descriptors (EMFILE, ENFILE), accept fails without taking the
/// connection. Linux leaves it queued and the listener ready, so retrying at once spins a core
/// and floods the log until a descriptor frees up; macOS resets it instead, so retrying at once
/// resets every queued client. Waiting gives connections time to finish and free descriptors.
/// One second, as hyper 0.14 and axum wait.
const ACCEPT_BACKOFF: Duration = Duration::from_secs(1);

/// How long a connection may carry nothing at all before the kernel probes the peer, and the
/// interval between probes; after the system's count of unanswered ones (9 on Linux, 8 on
/// macOS) it drops the connection. Reads from a client have their own bounds here, and the
/// kernel gives up on writes a vanished client never acknowledges, but an HTTP/2 response that
/// the client's flow-control window holds back neither reads nor writes: a client gone without
/// closing (power loss, a dropped mobile link, a NAT that forgot the mapping) would hold the
/// connection, its task and its backend reader for good. A probe is a few bytes on a connection
/// that is idle anyway; 15 seconds is Go's default for servers.
const KEEPALIVE: Duration = Duration::from_secs(15);

/// How long a client gets to finish closing a connection the server is done with: to answer the
/// ping that completes an HTTP/2 GOAWAY, and to stop sending once the server has closed its
/// side. A client does either within a round trip, which stays well under a second even on slow
/// mobile or satellite links; one that has not after 5 seconds is not going to, and the bound
/// keeps it from holding the socket.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Serves HTTP/1.1 and HTTP/2, over TLS when `tls` is given, until `shutdown` resolves, then
/// gives in-flight requests ten seconds to finish so a restart does not cut uploads mid-stream
/// without warning.
pub async fn serve(
    listener: TcpListener,
    service: S3Service,
    tls: Option<TlsAcceptor>,
    shutdown: impl Future<Output = ()>,
) {
    let mut http = ConnBuilder::new(TokioExecutor::new());
    // hyper enforces the header timeout only with a timer. It also closes a keep-alive
    // connection idle for that long, which clients reconnect from.
    http.http1()
        .timer(TokioTimer::new())
        .header_read_timeout(STALL_TIMEOUT);
    // Not hyper-util's GracefulShutdown: it takes over the connection, and an idle HTTP/2
    // connection is also shut down on its own. Each connection holds a receiver until it ends.
    let graceful = watch::Sender::new(());
    let mut shutdown = std::pin::pin!(shutdown);

    loop {
        let socket = tokio::select! {
            res = listener.accept() => match res {
                Ok((socket, _)) => socket,
                Err(err) => {
                    tracing::error!(error = %err, "cannot accept connection");
                    // A connection reset before it was taken fails only itself; waiting would
                    // let any client stall everyone's accepts.
                    if is_connection_error(&err) {
                        continue;
                    }
                    tokio::select! {
                        () = tokio::time::sleep(ACCEPT_BACKOFF) => continue,
                        () = shutdown.as_mut() => break,
                    }
                }
            },
            () = shutdown.as_mut() => break,
        };
        enable_keepalive(&socket);
        let (http, s3, watcher, tls) = (
            http.clone(),
            service.clone(),
            graceful.subscribe(),
            tls.clone(),
        );
        // The handshake runs on the connection's own task, so a slow or silent client cannot
        // hold up the accept loop.
        tokio::spawn(async move {
            match tls {
                None => serve_connection(http, socket, s3, watcher, Scheme("http")).await,
                Some(tls) => match tokio::time::timeout(STALL_TIMEOUT, tls.accept(socket)).await {
                    Ok(Ok(stream)) => {
                        // ALPN has already chosen the protocol, so the first bytes need not be
                        // read to tell. HTTP/2 over TLS requires ALPN.
                        let http = match stream.get_ref().1.alpn_protocol() {
                            Some(b"h2") => http.http2_only(),
                            _ => http.http1_only(),
                        };
                        serve_connection(http, stream, s3, watcher, Scheme("https")).await
                    }
                    // Plaintext requests to the HTTPS port end here, as do port scanners.
                    Ok(Err(err)) => tracing::debug!(error = %err, "TLS handshake failed"),
                    Err(_) => tracing::debug!("TLS handshake timed out"),
                },
            }
        });
    }
    // New clients are refused from now on and can go elsewhere at once. Left open, the listener
    // would have the kernel take them into its queue, to reset them only when the drain below
    // ends, up to ten seconds later and after they may have sent a request.
    drop(listener);

    // Fails only when no connection is left to tell.
    let _ = graceful.send(());
    tokio::select! {
        () = graceful.closed() => {}
        () = tokio::time::sleep(Duration::from_secs(10)) => {
            tracing::warn!("in-flight requests did not finish within 10 seconds");
        }
    }
}

/// What fails accept for one client's connection only, as opposed to the listener or the
/// process, such as running out of file descriptors.
fn is_connection_error(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionRefused
    )
}

/// Has the kernel probe the peer of a connection idle for [`KEEPALIVE`].
fn enable_keepalive(socket: &TcpStream) {
    let probes = TcpKeepalive::new()
        .with_time(KEEPALIVE)
        .with_interval(KEEPALIVE);
    if let Err(err) = SockRef::from(socket).set_tcp_keepalive(&probes) {
        tracing::debug!(error = %err, "cannot enable TCP keepalive");
    }
}

async fn serve_connection<I>(
    http: ConnBuilder<TokioExecutor>,
    io: I,
    s3: S3Service,
    mut watcher: watch::Receiver<()>,
    scheme: Scheme,
) where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // Held for the whole connection: with every sender gone, `changed` returns at once and
    // `idle_for` would spin.
    let requests = watch::Sender::new(0);
    let mut idle = requests.subscribe();
    let gateway = {
        let requests = requests.clone();
        service_fn(move |req| handle(s3.clone(), req, scheme, InFlight::new(&requests)))
    };
    let (io, http2) = FirstBytesDeadline::new(LingeringClose(Some(io)));
    let conn = http.serve_connection(TokioIo::new(io), gateway);
    let mut conn = std::pin::pin!(conn);
    tokio::select! {
        _ = conn.as_mut() => {}
        _ = watcher.changed() => {
            conn.as_mut().graceful_shutdown();
            let _ = conn.await;
        }
        () = idle_http2(http2, &mut idle) => {
            tracing::debug!("closing an idle HTTP/2 connection");
            conn.as_mut().graceful_shutdown();
            // hyper finishes the GOAWAY only once the client has answered its ping, so a client
            // that never answers would keep the connection. A request that crossed the GOAWAY is
            // still served; only a connection with nothing in flight is dropped.
            tokio::select! {
                _ = conn.as_mut() => {}
                () = idle_for(&mut idle, CLOSE_TIMEOUT) => {
                    tracing::debug!("dropping an idle HTTP/2 connection that did not close");
                }
            }
        }
    }
}

/// Resolves once an HTTP/2 connection has had no request in flight for [`STALL_TIMEOUT`]. hyper
/// has no idle timeout for HTTP/2, and keep-alive pings find only peers that are gone, since
/// clients answer them; a client that keeps a connection open would hold its socket for good.
/// Never for HTTP/1.1: hyper's header timeout already closes a connection that waits that long
/// for its next request, counted from when the last response was written out rather than from
/// when its body ended, so a second rule could only close one sooner, while a slow client is
/// still reading the end of a response.
async fn idle_http2(http2: oneshot::Receiver<()>, requests: &mut watch::Receiver<usize>) {
    if http2.await.is_err() {
        return std::future::pending().await;
    }
    idle_for(requests, STALL_TIMEOUT).await;
}

/// Resolves once no request on the connection has been in flight for `period`. Every change
/// restarts the wait, so a request that starts and ends between two polls counts as well.
async fn idle_for(requests: &mut watch::Receiver<usize>, period: Duration) {
    loop {
        if *requests.borrow_and_update() > 0 {
            let _ = requests.changed().await;
        } else if tokio::time::timeout(period, requests.changed())
            .await
            .is_err()
        {
            return;
        }
    }
}

/// Counts a request as in flight on its connection from the service call until its response
/// body has ended or been dropped, so a request whose body is still arriving, a download that
/// streams for minutes, or CompleteMultipartUpload's keep-alive body keeps the connection open.
struct InFlight(watch::Sender<usize>);

impl InFlight {
    fn new(requests: &watch::Sender<usize>) -> Self {
        requests.send_modify(|n| *n += 1);
        Self(requests.clone())
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.send_modify(|n| *n -= 1);
    }
}

/// A response body that holds its request's [`InFlight`]. hyper drops it once it has taken the
/// last frame, or when it gives up on the response.
struct ResponseBody {
    body: Body,
    _in_flight: InFlight,
}

impl hyper::body::Body for ResponseBody {
    type Data = Bytes;
    type Error = <Body as hyper::body::Body>::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        Pin::new(&mut self.body).poll_frame(cx)
    }

    // Forwarded so hyper still derives Content-Length and skips empty bodies.
    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

/// A request body that fails once the client has sent nothing for [`STALL_TIMEOUT`] while the
/// server waits for it, and then marks its request as timed out. Without it, a client that stops
/// halfway through a body holds its request, task and socket until it leaves, and over HTTP/2
/// keeps the whole connection from being closed as idle. The wait counts only while the server
/// reads: the time it spends writing what it read to the backend is not the client's, and over
/// HTTP/2 the client may send no more until the server has read on.
struct RequestBody<B> {
    body: B,
    /// Set for the read in progress when `waiting`.
    stall: Pin<Box<Sleep>>,
    waiting: bool,
    timed_out: Arc<AtomicBool>,
}

impl<B> RequestBody<B> {
    fn new(body: B, timed_out: Arc<AtomicBool>) -> Self {
        Self {
            body,
            stall: Box::pin(tokio::time::sleep(STALL_TIMEOUT)),
            waiting: false,
            timed_out,
        }
    }
}

impl<B> hyper::body::Body for RequestBody<B>
where
    B: hyper::body::Body<Data = Bytes> + Unpin,
    B::Error: Into<StdError>,
{
    type Data = Bytes;
    type Error = StdError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let this = &mut *self;
        // Once failed, a late byte must not make the body look whole.
        if this.timed_out.load(Ordering::Relaxed) {
            return Poll::Ready(Some(Err(body_stalled())));
        }
        if let Poll::Ready(frame) = Pin::new(&mut this.body).poll_frame(cx) {
            this.waiting = false;
            return Poll::Ready(frame.map(|frame| frame.map_err(Into::into)));
        }
        if !this.waiting {
            this.waiting = true;
            this.stall.as_mut().reset(Instant::now() + STALL_TIMEOUT);
        }
        ready!(this.stall.as_mut().poll(cx));
        tracing::debug!("a request body stalled");
        this.timed_out.store(true, Ordering::Relaxed);
        Poll::Ready(Some(Err(body_stalled())))
    }

    // Forwarded: s3s takes a body's length from its size hint where no header gives one.
    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

fn body_stalled() -> StdError {
    io::Error::new(
        io::ErrorKind::TimedOut,
        "the client stopped sending the body",
    )
    .into()
}

/// A form upload (PostObject) without Content-Length. s3s checks access only once it knows the
/// file's length, which it derives from that header; without one it reads the whole file into
/// memory first, up to 5 GiB, also for a client with no right to upload anything. With the
/// header it streams the file, and holds no more than the form fields before the check, which
/// its form limits bound. Browsers and curl send the header with every form.
fn is_form_of_unknown_length(req: &Request<Incoming>) -> bool {
    let is_form = req
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("multipart/form-data"));
    req.method() == Method::POST && is_form && !req.headers().contains_key(CONTENT_LENGTH)
}

/// The bytes that must arrive within [`STALL_TIMEOUT`] of the connection's start, or of its TLS
/// handshake: the HTTP/2 connection preface. Nothing else bounds reading it. hyper-util's auto
/// builder reads that many bytes, with no time limit, to tell HTTP/2 from HTTP/1.1, and an
/// HTTP/2 handshake waits for the preface as long as it takes; hyper's header timeout covers
/// HTTP/1.1 only, and starts after both. Every HTTP/1.1 request is longer.
const FIRST_BYTES: usize = H2_PREFACE.len();

/// hyper-util's auto builder serves a connection over HTTP/2 when its first bytes are exactly
/// these.
const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// Fails reads with `TimedOut`, which ends the connection, once [`STALL_TIMEOUT`] has passed
/// before the first [`FIRST_BYTES`] bytes arrived. Also tells from them whether the connection
/// speaks HTTP/2; hyper-util does not say which protocol it chose.
struct FirstBytesDeadline<I> {
    io: I,
    /// Cleared once the bytes have arrived, or the client has closed its side.
    deadline: Option<Pin<Box<Sleep>>>,
    awaited: usize,
    /// Sent once the bytes have all arrived and are [`H2_PREFACE`]; dropped as soon as they are
    /// not, or with the connection if they never arrive.
    http2: Option<oneshot::Sender<()>>,
}

impl<I> FirstBytesDeadline<I> {
    fn new(io: I) -> (Self, oneshot::Receiver<()>) {
        let (http2, speaks_http2) = oneshot::channel();
        let io = Self {
            io,
            deadline: Some(Box::pin(tokio::time::sleep(STALL_TIMEOUT))),
            awaited: FIRST_BYTES,
            http2: Some(http2),
        };
        (io, speaks_http2)
    }
}

impl<I: AsyncRead + Unpin> AsyncRead for FirstBytesDeadline<I> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = &mut *self;
        let filled = buf.filled().len();
        let res = Pin::new(&mut this.io).poll_read(cx, buf);
        if let Some(deadline) = &mut this.deadline {
            match res {
                Poll::Ready(Ok(())) => {
                    let read = &buf.filled()[filled..];
                    let seen = FIRST_BYTES - this.awaited;
                    let first = &read[..read.len().min(this.awaited)];
                    if *first != H2_PREFACE[seen..seen + first.len()] {
                        this.http2 = None;
                    }
                    this.awaited -= first.len();
                    if this.awaited == 0
                        && let Some(http2) = this.http2.take()
                    {
                        let _ = http2.send(());
                    }
                    if this.awaited == 0 || read.is_empty() {
                        this.deadline = None;
                    }
                }
                Poll::Pending if deadline.as_mut().poll(cx).is_ready() => {
                    tracing::debug!("closing a connection that sent too little in time");
                    return Poll::Ready(Err(io::ErrorKind::TimedOut.into()));
                }
                _ => {}
            }
        }
        res
    }
}

impl<I: AsyncWrite + Unpin> AsyncWrite for FirstBytesDeadline<I> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, buf)
    }

    // Forwarded so hyper keeps writing headers and body in one call where the socket can.
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

/// Closes a connection in stages once hyper is done with it, as RFC 9112, section 9.6 asks of a
/// server. hyper closes the socket at once, also right after answering a request whose body it
/// has not read (an invalid bucket name, a refused signature). Closing a socket on unread data
/// makes the kernel reset the connection, and a client still sending its body then fails with a
/// broken pipe before it has read the answer.
struct LingeringClose<I: AsyncRead + AsyncWrite + Unpin + Send + 'static>(Option<I>);

impl<I: AsyncRead + AsyncWrite + Unpin + Send + 'static> LingeringClose<I> {
    fn io(&mut self) -> Pin<&mut I> {
        Pin::new(self.0.as_mut().expect("taken only when dropped"))
    }
}

impl<I: AsyncRead + AsyncWrite + Unpin + Send + 'static> Drop for LingeringClose<I> {
    fn drop(&mut self) {
        // On a task of its own, so nothing waits for it, a server shutdown included. With no
        // runtime the process is ending, and there is no one left to linger for.
        if let (Some(io), Ok(runtime)) = (self.0.take(), Handle::try_current()) {
            runtime.spawn(linger(io));
        }
    }
}

/// Shuts the write side, so the client sees the end at once (a FIN, after TLS's close_notify),
/// then reads and discards what it still sends until it closes its side or [`CLOSE_TIMEOUT`] has
/// passed, all that a client that never stops sending or never closes costs.
async fn linger<I: AsyncRead + AsyncWrite + Unpin>(mut io: I) {
    let drain = async {
        let _ = poll_fn(|cx| Pin::new(&mut io).poll_shutdown(cx)).await;
        let mut buf = [0; 8192];
        loop {
            let mut discarded = ReadBuf::new(&mut buf);
            let read = poll_fn(|cx| Pin::new(&mut io).poll_read(cx, &mut discarded)).await;
            if read.is_err() || discarded.filled().is_empty() {
                return;
            }
        }
    };
    let _ = tokio::time::timeout(CLOSE_TIMEOUT, drain).await;
}

impl<I: AsyncRead + AsyncWrite + Unpin + Send + 'static> AsyncRead for LingeringClose<I> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.io().poll_read(cx, buf)
    }
}

impl<I: AsyncRead + AsyncWrite + Unpin + Send + 'static> AsyncWrite for LingeringClose<I> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.io().poll_write(cx, buf)
    }

    // Forwarded for the same reason as in FirstBytesDeadline.
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.io().poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.0.as_ref().is_some_and(I::is_write_vectored)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<io::Result<()>> {
        self.io().poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<io::Result<()>> {
        self.io().poll_shutdown(cx)
    }
}

async fn handle(
    s3: S3Service,
    mut req: Request<Incoming>,
    scheme: Scheme,
    in_flight: InFlight,
) -> Result<Response<ResponseBody>, HttpError> {
    join_content_encoding(req.headers_mut());
    let id = RequestId::new();
    // For work that outlives this call: CompleteMultipartUpload finishes in the response body.
    req.extensions_mut().insert(id.clone());
    req.extensions_mut().insert(scheme);
    let span = tracing::info_span!("request", id = %id.as_str());
    let mut res = if is_form_of_unknown_length(&req) {
        // As s3s refuses a PUT without the header.
        error_response(s3_error!(
            MissingContentLength,
            "You must provide the Content-Length HTTP header."
        ))
    } else {
        let timed_out = Arc::new(AtomicBool::new(false));
        let req = req.map(|body| Body::http_body(RequestBody::new(body, timed_out.clone())));
        let res = S3Service::call(&s3, req).instrument(span).await?;
        // The failed read reaches s3s or s3dal as one of many errors, each mapped by its own
        // reader (IncompleteBody, MalformedPOSTRequest, InternalError); the answer is decided
        // here once, as AWS gives it.
        if timed_out.load(Ordering::Relaxed) {
            error_response(s3_error!(
                RequestTimeout,
                "Your socket connection to the server was not read from or written to within \
                 the timeout period. Idle connections will be closed."
            ))
        } else {
            res
        }
    };
    id.stamp(&mut res);
    Ok(res.map(|body| ResponseBody {
        body,
        _in_flight: in_flight,
    }))
}

fn error_response(err: S3Error) -> HttpResponse {
    err.to_http_response()
        .expect("a code and a message serialize")
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use hyper::StatusCode;
    use tokio::sync::mpsc;

    use super::*;

    fn response(status: StatusCode, body: &'static str) -> HttpResponse {
        let mut res = HttpResponse::new(Body::from(body.to_owned()));
        *res.status_mut() = status;
        res
    }

    #[test]
    fn only_error_documents_get_the_request_id_in_their_body() {
        // An object's content must reach the client byte for byte, even when it happens to end
        // like an error document; every response still gets the header.
        let id = RequestId("0123456789ABCDEF".to_owned());
        let mut err = response(
            StatusCode::NOT_FOUND,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>NoSuchKey</Code></Error>",
        );
        id.stamp(&mut err);
        assert_eq!(
            err.body().bytes().unwrap(),
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>NoSuchKey</Code>\
             <RequestId>0123456789ABCDEF</RequestId></Error>"
        );
        assert_eq!(err.headers()[X_AMZ_REQUEST_ID], "0123456789ABCDEF");

        for status in [StatusCode::OK, StatusCode::PARTIAL_CONTENT] {
            let mut object = response(status, "<Error>stored content</Error>");
            id.stamp(&mut object);
            assert_eq!(
                object.body().bytes().unwrap(),
                "<Error>stored content</Error>"
            );
            assert_eq!(object.headers()[X_AMZ_REQUEST_ID], "0123456789ABCDEF");
        }
    }

    /// A request body whose bytes arrive when the test sends them.
    struct Arriving(mpsc::UnboundedReceiver<&'static str>);

    impl hyper::body::Body for Arriving {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            cx: &mut task::Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
            self.0
                .poll_recv(cx)
                .map(|sent| sent.map(|s| Ok(Frame::data(Bytes::from_static(s.as_bytes())))))
        }
    }

    async fn next(body: &mut RequestBody<Arriving>) -> Result<Bytes, StdError> {
        let frame = poll_fn(|cx| hyper::body::Body::poll_frame(Pin::new(&mut *body), cx)).await;
        Ok(frame
            .expect("the test never ends the body")?
            .into_data()
            .unwrap())
    }

    /// Sends `bytes` after `delay`, as a client would.
    fn send_after(
        client: &mpsc::UnboundedSender<&'static str>,
        delay: Duration,
        bytes: &'static str,
    ) {
        let client = client.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            client.send(bytes).unwrap();
        });
    }

    #[tokio::test(start_paused = true)]
    async fn a_request_body_fails_only_once_the_client_has_paused_for_the_stall_timeout() {
        // A slow client that keeps sending must not be cut off, nor one that waits for a server
        // busy writing to the backend; a client that stops must not hold its request for good.
        let (client, sent) = mpsc::unbounded_channel();
        let timed_out = Arc::new(AtomicBool::new(false));
        let mut body = RequestBody::new(Arriving(sent), timed_out.clone());
        let pause = STALL_TIMEOUT - Duration::from_secs(1);

        // Slower than the timeout in all, but never silent for as long.
        let start = Instant::now();
        for _ in 0..3 {
            send_after(&client, pause, "slow");
            assert_eq!(next(&mut body).await.unwrap(), "slow");
        }
        assert!(start.elapsed() > 2 * STALL_TIMEOUT);

        // The server reads on only after twice the timeout, and the client, held back by HTTP/2
        // flow control, sends only then.
        tokio::time::sleep(2 * STALL_TIMEOUT).await;
        send_after(&client, pause, "held back");
        assert_eq!(next(&mut body).await.unwrap(), "held back");
        assert!(!timed_out.load(Ordering::Relaxed));

        let silent = Instant::now();
        let err = next(&mut body).await.unwrap_err();
        assert_eq!(silent.elapsed(), STALL_TIMEOUT);
        let err = err.downcast_ref::<io::Error>().expect("an io::Error");
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        // So that handle answers RequestTimeout, whatever error the reader made of it.
        assert!(timed_out.load(Ordering::Relaxed));
        // Bytes that arrive too late do not revive it.
        client.send("late").unwrap();
        assert!(next(&mut body).await.is_err());
    }

    #[tokio::test]
    async fn accepted_connections_have_the_kernel_probe_for_a_vanished_peer() {
        // Without probes, an HTTP/2 response held back by a client that vanished without closing
        // would hold its connection and backend reader for good; no read or write fails then.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let _client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        enable_keepalive(&socket);
        let socket = SockRef::from(&socket);
        assert!(socket.keepalive().unwrap());
        assert_eq!(socket.tcp_keepalive_time().unwrap(), KEEPALIVE);
        assert_eq!(socket.tcp_keepalive_interval().unwrap(), KEEPALIVE);
    }
}

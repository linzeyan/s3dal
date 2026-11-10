//! Sends the log to a collector as well as to stderr or the log file: Graylog over GELF (UDP,
//! TCP or HTTP), Elasticsearch and OpenSearch over their bulk API, a syslog server (RFC 5424
//! over UDP, TCP or TLS), an OpenTelemetry collector (OTLP over HTTP), Splunk's HTTP Event
//! Collector, or any HTTP input that takes newline-delimited JSON.
//!
//! tracing writes each line on the thread that logs it, a request's among them, and a collector
//! that is slow or down must not hold a request up. So a line only goes into a bounded queue
//! there; a thread of its own sends it. While the collector cannot be reached, that thread
//! retries with a growing pause and the queue fills; lines that find it full are dropped and
//! counted. Both are reported on stderr, which is not forwarded, so the report never loops back
//! into the queue.

use std::io::{self, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs, UdpSocket};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value};
use tracing_subscriber::fmt::MakeWriter;

/// Lines held while the collector is unreachable: a few MiB, minutes of a busy server's log.
const QUEUE: usize = 10_000;
/// Lines per HTTP request, and the most sent before the dropped count is looked at again.
const BATCH: usize = 500;
const TIMEOUT: Duration = Duration::from_secs(10);
const MAX_PAUSE: Duration = Duration::from_secs(30);
/// GELF over UDP: a datagram is at most 8192 bytes, a message at most 128 of them.
const UDP_DATAGRAM: usize = 8192;
const UDP_CHUNK_HEADER: usize = 12;
const UDP_MAX_CHUNKS: usize = 128;
/// Syslog over UDP has no chunks: a message is one datagram, which IPv4 bounds here.
const UDP_MAX: usize = 65_507;
/// RFC 5424's facility for system daemons, which s3dal is.
const SYSLOG_DAEMON: u8 = 3;

const SCHEMES: [&str; 15] = [
    "gelf+udp",
    "gelf+tcp",
    "gelf+http",
    "gelf+https",
    "elasticsearch+http",
    "elasticsearch+https",
    "syslog+udp",
    "syslog+tcp",
    "syslog+tls",
    "otlp+http",
    "otlp+https",
    "splunk+http",
    "splunk+https",
    "ndjson+http",
    "ndjson+https",
];

/// Where the log is sent, and in what form, as `--log-forward` names them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub format: Format,
    pub transport: Transport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// GELF 1.1: over UDP one datagram per message, chunked past one; over TCP each message
    /// ended by a NUL byte; over HTTP one POST per message.
    Gelf,
    /// Elasticsearch's bulk API, one POST per batch.
    Bulk,
    /// RFC 5424 with the line's JSON as the message; over TCP and TLS each message framed by
    /// its length (RFC 6587's octet counting).
    Syslog,
    /// OTLP's JSON encoding, one POST per batch.
    Otlp,
    /// Splunk HEC events, one POST per batch.
    Splunk,
    /// The lines as tracing wrote them, one per line, one POST per batch.
    Ndjson,
}

/// Addresses are kept as text and resolved on every connect: the collector's name may resolve
/// only once its container is up, and to another address after it is replaced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Transport {
    Udp(String),
    Tcp(String),
    Tls(String),
    Http(Http),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Http {
    /// The request URL, without the user and password.
    pub url: String,
    pub user: Option<String>,
    pub password: Option<String>,
}

impl Target {
    /// Errors name the URL with its password redacted, since they reach the log and the console.
    pub fn parse(s: &str) -> Result<Self, String> {
        let s = s.trim();
        Self::parse_parts(s).map_err(|e| format!("{:?}: {e}", redact(s, "<redacted>")))
    }

    fn parse_parts(s: &str) -> Result<Self, String> {
        let (scheme, rest) = s
            .split_once("://")
            .ok_or("expected a URL such as gelf+udp://graylog:12201")?;
        let (format, transport) = scheme.split_once('+').unwrap_or((scheme, ""));
        let (format, transport) = match (format, transport) {
            ("gelf", "udp") => (Format::Gelf, Transport::Udp(host_port(rest)?)),
            ("gelf", "tcp") => (Format::Gelf, Transport::Tcp(host_port(rest)?)),
            ("gelf", "http" | "https") => (
                Format::Gelf,
                Transport::Http(http(transport, rest, "/gelf")?),
            ),
            ("elasticsearch", "http" | "https") => {
                // A data stream name that Elasticsearch's built-in `logs-*-*` template matches,
                // so the index needs no setup there.
                let mut http = http(transport, rest, "/logs-s3dal-default")?;
                http.url = bulk_url(&http.url);
                (Format::Bulk, Transport::Http(http))
            }
            ("syslog", "udp") => (Format::Syslog, Transport::Udp(host_port(rest)?)),
            ("syslog", "tcp") => (Format::Syslog, Transport::Tcp(host_port(rest)?)),
            ("syslog", "tls") => (Format::Syslog, tls(host_port(rest)?)?),
            // The path every OTLP/HTTP receiver takes logs on.
            ("otlp", "http" | "https") => (
                Format::Otlp,
                Transport::Http(http(transport, rest, "/v1/logs")?),
            ),
            ("splunk", "http" | "https") => {
                let http = http(transport, rest, "/services/collector/event")?;
                // Without it every request is refused; found now, the typo is fixed in a minute.
                if http.password.as_deref().is_none_or(str::is_empty) {
                    return Err("expected the HEC token in place of a password, as \
                                splunk+https://:TOKEN@splunk:8088"
                        .into());
                }
                (Format::Splunk, Transport::Http(http))
            }
            // An HTTP input's path is its own to choose; `/` is left as it is.
            ("ndjson", "http" | "https") => {
                (Format::Ndjson, Transport::Http(http(transport, rest, "/")?))
            }
            _ => {
                return Err(format!(
                    "unknown collector {scheme:?}; use {}",
                    SCHEMES.join(", ")
                ));
            }
        };
        Ok(Self { format, transport })
    }
}

fn host_port(rest: &str) -> Result<String, String> {
    let addr = rest.strip_suffix('/').unwrap_or(rest);
    let valid = addr
        .rsplit_once(':')
        .is_some_and(|(host, port)| !host.is_empty() && port.parse::<u16>().is_ok_and(|p| p > 0));
    if !valid || addr.contains(['/', '@', '?', '#']) {
        return Err("expected host:port after ://".into());
    }
    Ok(addr.to_owned())
}

/// The host of `host:port`, as a certificate names it: an IPv6 address without its brackets.
#[cfg(feature = "log-forward")]
fn host_of(addr: &str) -> &str {
    addr.rsplit_once(':')
        .map_or(addr, |(host, _)| host)
        .trim_matches(['[', ']'])
}

/// `.../index` to `.../index/_bulk`, with any query (`?pipeline=...`) kept after it.
fn bulk_url(url: &str) -> String {
    let (base, query) = match url.split_once('?') {
        Some((base, query)) => (base, Some(query)),
        None => (url, None),
    };
    let mut out = format!("{}/_bulk", base.trim_end_matches('/'));
    if let Some(query) = query {
        out.push('?');
        out.push_str(query);
    }
    out
}

#[cfg(feature = "log-forward")]
fn http(transport: &str, rest: &str, default_path: &str) -> Result<Http, String> {
    use percent_encoding::percent_decode_str;
    let mut url =
        reqwest::Url::parse(&format!("{transport}://{rest}")).map_err(|e| e.to_string())?;
    if url.host_str().is_none_or(str::is_empty) {
        return Err("expected a host after ://".into());
    }
    let decode = |s: &str| percent_decode_str(s).decode_utf8_lossy().into_owned();
    let user = Some(url.username()).filter(|u| !u.is_empty()).map(decode);
    let password = url.password().map(decode);
    let _ = url.set_username("");
    let _ = url.set_password(None);
    if url.path() == "/" {
        url.set_path(default_path);
    }
    Ok(Http {
        url: url.to_string(),
        user,
        password,
    })
}

#[cfg(not(feature = "log-forward"))]
fn http(_: &str, _: &str, _: &str) -> Result<Http, String> {
    Err("this build of s3dal has no HTTP log forwarding (the `log-forward` feature)".into())
}

/// A name the certificate can be checked against, refused now rather than at every connect.
#[cfg(feature = "log-forward")]
fn tls(addr: String) -> Result<Transport, String> {
    tokio_rustls::rustls::pki_types::ServerName::try_from(host_of(&addr)).map_err(|e| {
        format!(
            "{:?} cannot be checked against a certificate: {e}",
            host_of(&addr)
        )
    })?;
    Ok(Transport::Tls(addr))
}

#[cfg(not(feature = "log-forward"))]
fn tls(_: String) -> Result<Transport, String> {
    Err("this build of s3dal has no TLS log forwarding (the `log-forward` feature)".into())
}

/// The parts of a URL around its password, by hand rather than with a URL parser: `redact`
/// must work in every build, and the password must go even from a URL a parser refuses.
struct Parts<'a> {
    /// Up to and including `://`.
    scheme: &'a str,
    /// Empty when there is none, as in Splunk's `:TOKEN@host`.
    user: &'a str,
    password: Option<&'a str>,
    /// The host on, after the `@` when there is one.
    rest: &'a str,
}

impl Parts<'_> {
    fn join(&self, password: Option<&str>) -> String {
        let Self {
            scheme, user, rest, ..
        } = self;
        match password {
            Some(password) => format!("{scheme}{user}:{password}@{rest}"),
            None if user.is_empty() => format!("{scheme}{rest}"),
            None => format!("{scheme}{user}@{rest}"),
        }
    }

    fn host(&self) -> &str {
        self.rest
            .find(['/', '?', '#'])
            .map_or(self.rest, |i| &self.rest[..i])
    }
}

fn parts(url: &str) -> Option<Parts<'_>> {
    let start = url.find("://")? + 3;
    let authority_end = url[start..]
        .find(['/', '?', '#'])
        .map_or(url.len(), |i| start + i);
    let (userinfo, rest) = match url[start..authority_end].rfind('@') {
        Some(at) => (&url[start..start + at], &url[start + at + 1..]),
        None => ("", &url[start..]),
    };
    let (user, password) = match userinfo.split_once(':') {
        Some((user, password)) => (user, Some(password)),
        None => (userinfo, None),
    };
    Some(Parts {
        scheme: &url[..start],
        user,
        password,
        rest,
    })
}

/// The URL with its password replaced by `mask`, or taken out with an empty one: for
/// `check-config`, the log and the console, none of which should show it.
pub fn redact(url: &str, mask: &str) -> String {
    match parts(url) {
        Some(p) if p.password.is_some() => p.join((!mask.is_empty()).then_some(mask)),
        _ => url.to_owned(),
    }
}

/// Whether the URL carries a password.
pub fn has_password(url: &str) -> bool {
    parts(url).is_some_and(|p| p.password.is_some())
}

/// The console is never sent the password, so a URL it sends back with the same user and host
/// but none keeps the password the current one has, as a backend option left empty does.
pub fn keep_password(new: &str, current: Option<&str>) -> String {
    let (Some(n), Some(c)) = (parts(new), current.and_then(parts)) else {
        return new.to_owned();
    };
    match (n.password, c.password) {
        (None, Some(password))
            if n.scheme == c.scheme && n.user == c.user && n.host() == c.host() =>
        {
            n.join(Some(password))
        }
        _ => new.to_owned(),
    }
}

/// The writer the log's JSON layer writes to: hands each line to the sending thread.
pub struct Forwarder {
    queue: SyncSender<Line>,
    dropped: Arc<AtomicU64>,
    progress: Arc<Progress>,
}

/// Lines queued, and lines the sending thread is done with, sent or refused, so the process can
/// wait for the queue to empty before it exits.
#[derive(Default)]
struct Progress {
    queued: AtomicU64,
    done: Mutex<u64>,
    changed: Condvar,
}

/// What `main` keeps to wait for the queue at exit: the lines that say why and when the server
/// stopped are the last ones logged, and without the wait they would be the ones lost.
#[derive(Clone)]
pub struct Flush {
    progress: Arc<Progress>,
    shown: String,
}

impl Flush {
    /// Waits until every line queued so far has been sent, or refused, or `timeout` has passed,
    /// which is all an unreachable collector gets: exiting must not wait on it. Lines still
    /// unsent then are counted on stderr. Whether every line went.
    pub fn wait(&self, timeout: Duration) -> bool {
        let target = self.progress.queued.load(Ordering::SeqCst);
        let done = self
            .progress
            .done
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let (done, _) = self
            .progress
            .changed
            .wait_timeout_while(done, timeout, |done| *done < target)
            .unwrap_or_else(PoisonError::into_inner);
        if *done < target {
            eprintln!(
                "s3dal: {} log lines were not sent to {} before exit",
                target - *done,
                self.shown
            );
            return false;
        }
        true
    }
}

/// One line as tracing wrote it, JSON, and when: a line sent minutes late, after the collector
/// came back, keeps the time it happened.
struct Line {
    at: SystemTime,
    json: Vec<u8>,
}

/// Starts the sending thread. Fails only on what cannot change by waiting, so the server does
/// not start without the forwarding it was told to do; an unreachable collector is retried.
pub fn start(url: &str) -> Result<(Forwarder, Flush), String> {
    let target = Target::parse(url)?;
    let collector = Collector::new(target)?;
    let (queue, lines) = mpsc::sync_channel(QUEUE);
    let dropped = Arc::new(AtomicU64::new(0));
    let progress = Arc::new(Progress::default());
    let shown = redact(url, "");
    let flush = Flush {
        progress: progress.clone(),
        shown: shown.clone(),
    };
    let (counter, sent) = (dropped.clone(), progress.clone());
    std::thread::Builder::new()
        .name("s3dal-log-forward".into())
        .spawn(move || run(collector, &lines, &counter, &sent, &shown))
        .map_err(|e| format!("cannot start the log forwarding thread: {e}"))?;
    let forwarder = Forwarder {
        queue,
        dropped,
        progress,
    };
    Ok((forwarder, flush))
}

pub struct ForwardWriter<'a>(&'a Forwarder);

impl Write for ForwardWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let line = Line {
            at: SystemTime::now(),
            json: buf.to_vec(),
        };
        // Counted before it is sent, so `Flush::wait` never misses a line the thread already
        // took; one the full queue refuses is taken back.
        self.0.progress.queued.fetch_add(1, Ordering::SeqCst);
        if self.0.queue.try_send(line).is_err() {
            self.0.progress.queued.fetch_sub(1, Ordering::SeqCst);
            self.0.dropped.fetch_add(1, Ordering::Relaxed);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for Forwarder {
    type Writer = ForwardWriter<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        ForwardWriter(self)
    }
}

fn run(
    mut collector: Collector,
    lines: &Receiver<Line>,
    dropped: &AtomicU64,
    progress: &Progress,
    shown: &str,
) {
    let host = hostname();
    let mut failing = false;
    let mut last_refusal = String::new();
    // Ends when the queue's sender is gone, which is when the process is.
    while let Ok(first) = lines.recv() {
        let mut batch = vec![first];
        batch.extend(lines.try_iter().take(BATCH - 1));
        let payloads = collector.encode(&batch, &host);
        let mut pause = Duration::from_secs(1);
        let mut i = 0;
        while i < payloads.len() {
            match collector.deliver(&payloads[i]) {
                Ok(()) => {
                    if failing {
                        eprintln!("s3dal: sending the log to {shown} again");
                        failing = false;
                    }
                    i += 1;
                }
                Err(Failure::Retry(e)) => {
                    if !failing {
                        eprintln!("s3dal: cannot send the log to {shown}: {e}; retrying");
                        failing = true;
                    }
                    std::thread::sleep(pause);
                    pause = (pause * 2).min(MAX_PAUSE);
                }
                // Sending it again would get the same answer.
                Err(Failure::Refused(e)) => {
                    if e != last_refusal {
                        eprintln!("s3dal: {shown} refused log lines: {e}");
                        last_refusal = e;
                    }
                    i += 1;
                }
            }
        }
        *progress.done.lock().unwrap_or_else(PoisonError::into_inner) += batch.len() as u64;
        progress.changed.notify_all();
        let n = dropped.swap(0, Ordering::Relaxed);
        if n > 0 {
            eprintln!(
                "s3dal: {n} log lines were not sent to {shown}: more came than it took in time"
            );
        }
    }
}

enum Failure {
    /// Worth sending again: the collector was unreachable, busy or failing.
    Retry(String),
    /// Not worth it: the collector answered and refused, which it would do again.
    Refused(String),
}

struct Collector {
    format: Format,
    sink: Sink,
}

enum Sink {
    Udp {
        addr: String,
        socket: Option<(UdpSocket, SocketAddr)>,
    },
    /// TCP, or TLS over it, on a connection kept open.
    Stream {
        addr: String,
        #[cfg(feature = "log-forward")]
        tls: Option<Arc<tokio_rustls::rustls::ClientConfig>>,
        conn: Option<Conn>,
    },
    #[cfg(feature = "log-forward")]
    Http {
        runtime: tokio::runtime::Runtime,
        client: reqwest::Client,
        http: Http,
    },
}

enum Conn {
    Plain(TcpStream),
    #[cfg(feature = "log-forward")]
    Tls(Box<tokio_rustls::rustls::StreamOwned<tokio_rustls::rustls::ClientConnection, TcpStream>>),
}

impl Write for Conn {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plain(s) => s.write(buf),
            #[cfg(feature = "log-forward")]
            Self::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(s) => s.flush(),
            #[cfg(feature = "log-forward")]
            Self::Tls(s) => s.flush(),
        }
    }
}

impl Collector {
    fn new(target: Target) -> Result<Self, String> {
        let sink = match target.transport {
            Transport::Udp(addr) => Sink::Udp { addr, socket: None },
            Transport::Tcp(addr) => Sink::Stream {
                addr,
                #[cfg(feature = "log-forward")]
                tls: None,
                conn: None,
            },
            #[cfg(feature = "log-forward")]
            Transport::Tls(addr) => Sink::Stream {
                addr,
                tls: Some(tls_config()?),
                conn: None,
            },
            #[cfg(feature = "log-forward")]
            Transport::Http(http) => Sink::Http {
                runtime: tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| format!("cannot start the log forwarding runtime: {e}"))?,
                client: reqwest::Client::builder()
                    .timeout(TIMEOUT)
                    .build()
                    .map_err(|e| format!("cannot build the log forwarding client: {e}"))?,
                http,
            },
            #[cfg(not(feature = "log-forward"))]
            Transport::Tls(_) | Transport::Http(_) => unreachable!("refused by parse"),
        };
        Ok(Self {
            format: target.format,
            sink,
        })
    }

    /// What `deliver` sends for a batch: one message per line, or one request body for all.
    fn encode(&self, batch: &[Line], host: &str) -> Vec<Vec<u8>> {
        match self.format {
            Format::Gelf => batch.iter().map(|line| gelf(line, host)).collect(),
            Format::Syslog => batch.iter().map(|line| syslog(line, host)).collect(),
            #[cfg(feature = "log-forward")]
            Format::Bulk => vec![bulk(batch)],
            #[cfg(feature = "log-forward")]
            Format::Otlp => vec![otlp(batch, host)],
            #[cfg(feature = "log-forward")]
            Format::Splunk => vec![splunk(batch, host)],
            #[cfg(feature = "log-forward")]
            Format::Ndjson => vec![ndjson(batch)],
            #[cfg(not(feature = "log-forward"))]
            Format::Bulk | Format::Otlp | Format::Splunk | Format::Ndjson => {
                unreachable!("refused by parse")
            }
        }
    }

    fn deliver(&mut self, payload: &[u8]) -> Result<(), Failure> {
        let format = self.format;
        match &mut self.sink {
            Sink::Udp { addr, socket } => {
                if socket.is_none() {
                    let to = resolve(addr)?;
                    let local: SocketAddr = if to.is_ipv4() {
                        "0.0.0.0:0".parse().expect("valid address")
                    } else {
                        "[::]:0".parse().expect("valid address")
                    };
                    let s = UdpSocket::bind(local).map_err(|e| Failure::Retry(e.to_string()))?;
                    *socket = Some((s, to));
                }
                let (s, to) = socket.as_ref().expect("bound above");
                let datagrams = match format {
                    Format::Gelf => udp_datagrams(payload),
                    _ => (payload.len() <= UDP_MAX).then(|| vec![payload.to_vec()]),
                };
                let Some(datagrams) = datagrams else {
                    let tcp = if format == Format::Gelf {
                        "gelf+tcp"
                    } else {
                        "syslog+tcp"
                    };
                    return Err(Failure::Refused(format!(
                        "a message of {} bytes is longer than UDP can carry; use {tcp}",
                        payload.len()
                    )));
                };
                for d in datagrams {
                    if let Err(e) = s.send_to(&d, to) {
                        // Resolved again next time, in case the collector moved.
                        *socket = None;
                        return Err(Failure::Retry(e.to_string()));
                    }
                }
                Ok(())
            }
            Sink::Stream {
                addr,
                #[cfg(feature = "log-forward")]
                tls,
                conn,
            } => {
                if conn.is_none() {
                    #[cfg(feature = "log-forward")]
                    let tls = tls.as_ref();
                    #[cfg(not(feature = "log-forward"))]
                    let tls = None::<()>;
                    *conn = Some(connect(addr, tls)?);
                }
                let c = conn.as_mut().expect("connected above");
                // One write per message: over TLS each write is a record of its own.
                let mut framed = Vec::with_capacity(payload.len() + 8);
                if format == Format::Syslog {
                    framed.extend_from_slice(format!("{} ", payload.len()).as_bytes());
                }
                framed.extend_from_slice(payload);
                if format == Format::Gelf {
                    framed.push(0);
                }
                c.write_all(&framed).and_then(|()| c.flush()).map_err(|e| {
                    *conn = None;
                    Failure::Retry(e.to_string())
                })
            }
            #[cfg(feature = "log-forward")]
            Sink::Http {
                runtime,
                client,
                http,
            } => runtime.block_on(post(client, http, format, payload.to_vec())),
        }
    }
}

fn resolve(addr: &str) -> Result<SocketAddr, Failure> {
    addr.to_socket_addrs()
        .map_err(|e| Failure::Retry(format!("{addr}: {e}")))?
        .next()
        .ok_or_else(|| Failure::Retry(format!("{addr}: no address")))
}

#[cfg(feature = "log-forward")]
type TlsConfig<'a> = Option<&'a Arc<tokio_rustls::rustls::ClientConfig>>;
#[cfg(not(feature = "log-forward"))]
type TlsConfig<'a> = Option<()>;

fn connect(addr: &str, tls: TlsConfig<'_>) -> Result<Conn, Failure> {
    let retry = |e: &dyn std::fmt::Display| Failure::Retry(e.to_string());
    let to = resolve(addr)?;
    let s = TcpStream::connect_timeout(&to, TIMEOUT).map_err(|e| retry(&e))?;
    let _ = s.set_write_timeout(Some(TIMEOUT));
    // The TLS handshake reads; a collector that stops answering it must not hang the thread.
    let _ = s.set_read_timeout(Some(TIMEOUT));
    let Some(config) = tls else {
        return Ok(Conn::Plain(s));
    };
    #[cfg(feature = "log-forward")]
    {
        use tokio_rustls::rustls::pki_types::ServerName;
        use tokio_rustls::rustls::{ClientConnection, StreamOwned};
        let name = ServerName::try_from(host_of(addr).to_owned()).map_err(|e| retry(&e))?;
        let conn = ClientConnection::new(config.clone(), name).map_err(|e| retry(&e))?;
        let mut stream = StreamOwned::new(conn, s);
        // Now rather than on the first write, so a certificate the collector's name does not
        // match is what stderr reports, not a failed write.
        while stream.conn.is_handshaking() {
            stream
                .conn
                .complete_io(&mut stream.sock)
                .map_err(|e| retry(&e))?;
        }
        Ok(Conn::Tls(Box::new(stream)))
    }
    #[cfg(not(feature = "log-forward"))]
    {
        let () = config;
        unreachable!("refused by parse")
    }
}

/// Certificates checked as the system checks them, as reqwest does for the HTTPS collectors.
#[cfg(feature = "log-forward")]
fn tls_config() -> Result<Arc<tokio_rustls::rustls::ClientConfig>, String> {
    use rustls_platform_verifier::BuilderVerifierExt;
    use tokio_rustls::rustls::ClientConfig;
    use tokio_rustls::rustls::crypto::aws_lc_rs;
    let config = ClientConfig::builder_with_provider(Arc::new(aws_lc_rs::default_provider()))
        .with_safe_default_protocol_versions()
        .and_then(BuilderVerifierExt::with_platform_verifier)
        .map_err(|e| format!("cannot set up TLS for the log collector: {e}"))?
        .with_no_client_auth();
    Ok(Arc::new(config))
}

#[cfg(feature = "log-forward")]
async fn post(
    client: &reqwest::Client,
    http: &Http,
    format: Format,
    body: Vec<u8>,
) -> Result<(), Failure> {
    let content_type = match format {
        Format::Bulk | Format::Ndjson => "application/x-ndjson",
        _ => "application/json",
    };
    let mut req = client
        .post(&http.url)
        .header(reqwest::header::CONTENT_TYPE, content_type)
        .body(body);
    req = match (format, &http.user, &http.password) {
        // HEC takes its token in a header of its own rather than as Basic authentication.
        (Format::Splunk, _, Some(token)) => {
            req.header(reqwest::header::AUTHORIZATION, format!("Splunk {token}"))
        }
        (_, None, None) => req,
        (_, user, password) => {
            req.basic_auth(user.as_deref().unwrap_or_default(), password.as_ref())
        }
    };
    let res = req
        .send()
        .await
        .map_err(|e| Failure::Retry(e.to_string()))?;
    let status = res.status();
    let text = res.text().await.unwrap_or_default();
    if status.is_success() {
        // A request may succeed as a whole and still have refused some lines: a field whose
        // type the index maps differently, say.
        let refused = match format {
            Format::Bulk => bulk_error(&text),
            Format::Otlp => otlp_error(&text),
            _ => None,
        };
        return refused.map_or(Ok(()), |e| Err(Failure::Refused(e)));
    }
    let msg = format!("{status}: {}", text.chars().take(300).collect::<String>());
    if status.as_u16() == 429 || status.is_server_error() {
        Err(Failure::Retry(msg))
    } else {
        Err(Failure::Refused(msg))
    }
}

/// The first refusal in a bulk response, if it has any.
#[cfg(feature = "log-forward")]
fn bulk_error(text: &str) -> Option<String> {
    let doc: Value = serde_json::from_str(text).ok()?;
    if doc.get("errors") != Some(&Value::Bool(true)) {
        return None;
    }
    let first = doc["items"]
        .as_array()?
        .iter()
        .filter_map(|item| item.as_object()?.values().next()?.get("error"))
        .next()
        .map_or_else(|| "some documents".to_owned(), Value::to_string);
    Some(first)
}

/// OTLP's partial success: the records it rejected, with its reason.
#[cfg(feature = "log-forward")]
fn otlp_error(text: &str) -> Option<String> {
    let doc: Value = serde_json::from_str(text).ok()?;
    let partial = doc.get("partialSuccess")?;
    // An int64, which JSON may carry as a string or a number.
    let rejected = match partial.get("rejectedLogRecords")? {
        Value::String(s) => s.parse().ok()?,
        n => n.as_u64()?,
    };
    if rejected == 0 {
        return None;
    }
    let reason = partial
        .get("errorMessage")
        .and_then(Value::as_str)
        .unwrap_or_default();
    Some(format!("{rejected} records rejected: {reason}"))
}

/// A line as tracing's JSON layer wrote it, or, should it not parse, its text as the message.
fn object(json: &[u8]) -> Map<String, Value> {
    match serde_json::from_slice(json) {
        Ok(Value::Object(map)) => map,
        _ => {
            let text = String::from_utf8_lossy(json).trim_end().to_owned();
            Map::from_iter([("message".to_owned(), Value::String(text))])
        }
    }
}

/// The line's message as text, `-` when it has none: GELF requires one.
fn message(fields: &mut Map<String, Value>) -> String {
    match fields.remove("message") {
        Some(Value::String(s)) if !s.is_empty() => s,
        Some(other) if !other.is_null() => other.to_string(),
        _ => "-".to_owned(),
    }
}

/// A tracing level as a syslog severity, which GELF uses too.
fn severity(level: Option<&Value>) -> u8 {
    match level.and_then(Value::as_str) {
        Some("ERROR") => 3,
        Some("WARN") => 4,
        Some("INFO") => 6,
        _ => 7,
    }
}

/// Seconds since the epoch with milliseconds, as GELF and Splunk take the time.
fn seconds(at: SystemTime) -> Value {
    let at = at.duration_since(UNIX_EPOCH).unwrap_or_default();
    let millis = u64::try_from(at.as_millis()).unwrap_or(u64::MAX);
    serde_json::Number::from_f64(millis as f64 / 1000.0).map_or(Value::Null, Value::Number)
}

/// A bulk request body: each line's fields as they are, its time as `@timestamp`, the field
/// Elasticsearch's log templates and Kibana sort by.
#[cfg(feature = "log-forward")]
fn bulk(batch: &[Line]) -> Vec<u8> {
    let mut body = Vec::new();
    for line in batch {
        let mut doc = object(&line.json);
        if let Some(ts) = doc.remove("timestamp") {
            doc.insert("@timestamp".into(), ts);
        }
        body.extend_from_slice(b"{\"create\":{}}\n");
        body.extend_from_slice(&serde_json::to_vec(&doc).expect("a JSON map serializes"));
        body.push(b'\n');
    }
    body
}

/// A GELF 1.1 message. Its own fields are fixed; every other one is an additional field, named
/// with a leading `_`, nested fields (the request's span) joined with `_`, as `_span_id`.
fn gelf(line: &Line, host: &str) -> Vec<u8> {
    let mut fields = object(&line.json);
    fields.remove("timestamp");
    let level = severity(fields.remove("level").as_ref());
    let message = message(&mut fields);
    let mut out = Map::new();
    out.insert("version".into(), "1.1".into());
    out.insert("host".into(), host.into());
    out.insert("short_message".into(), message.into());
    out.insert("timestamp".into(), seconds(line.at));
    out.insert("level".into(), level.into());
    for (key, value) in fields {
        additional(&mut out, "", &key, value);
    }
    serde_json::to_vec(&out).expect("a JSON map serializes")
}

fn additional(out: &mut Map<String, Value>, prefix: &str, key: &str, value: Value) {
    let clean: String = key
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let mut name = format!("{prefix}_{clean}");
    // GELF reserves `_id`.
    if name == "_id" {
        name.push('_');
    }
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                additional(out, &name, &k, v);
            }
        }
        Value::Null => {}
        // GELF's additional fields are strings or numbers.
        Value::Bool(b) => {
            out.insert(name, b.to_string().into());
        }
        Value::Array(_) => {
            out.insert(name, value.to_string().into());
        }
        value => {
            out.insert(name, value);
        }
    }
}

/// An RFC 5424 message: the line's level as the severity, the time tracing wrote, this host,
/// the process id, and the line's JSON as the message, for the collector to parse as JSON.
fn syslog(line: &Line, host: &str) -> Vec<u8> {
    let json = String::from_utf8_lossy(&line.json);
    let json = json.trim_end();
    let fields = object(json.as_bytes());
    let pri = SYSLOG_DAEMON * 8 + severity(fields.get("level"));
    // RFC 3339 with microseconds, which RFC 5424 takes as it is.
    let time = fields
        .get("timestamp")
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty() && !t.contains(' '))
        .unwrap_or("-");
    // Printable ASCII without spaces, or the field would end early.
    let host = if !host.is_empty() && host.bytes().all(|b| b.is_ascii_graphic()) {
        host
    } else {
        "-"
    };
    let pid = std::process::id();
    format!("<{pri}>1 {time} {host} s3dal {pid} - - {json}").into_bytes()
}

/// One OTLP request for the batch: s3dal and this host as the resource, each line a log record
/// with its level as the severity, its message as the body, and every other field an
/// attribute, nested ones (the request's span) joined with `.`, as `span.id`.
#[cfg(feature = "log-forward")]
fn otlp(batch: &[Line], host: &str) -> Vec<u8> {
    use serde_json::json;
    let records: Vec<Value> = batch
        .iter()
        .map(|line| {
            let mut fields = object(&line.json);
            fields.remove("timestamp");
            let level = fields.remove("level");
            let level = level.as_ref().and_then(Value::as_str).unwrap_or_default();
            let number = match level {
                "TRACE" => 1,
                "DEBUG" => 5,
                "INFO" => 9,
                "WARN" => 13,
                "ERROR" => 17,
                _ => 0,
            };
            let body = message(&mut fields);
            let mut attributes = Vec::new();
            for (key, value) in fields {
                otlp_attributes(&mut attributes, "", key, value);
            }
            let at = line.at.duration_since(UNIX_EPOCH).unwrap_or_default();
            json!({
                "timeUnixNano": at.as_nanos().to_string(),
                "severityNumber": number,
                "severityText": level,
                "body": { "stringValue": body },
                "attributes": attributes,
            })
        })
        .collect();
    let attribute =
        |key: &str, value: &str| json!({ "key": key, "value": { "stringValue": value } });
    let body = json!({ "resourceLogs": [{
        "resource": { "attributes": [
            attribute("service.name", "s3dal"),
            attribute("service.version", env!("CARGO_PKG_VERSION")),
            attribute("host.name", host),
        ] },
        "scopeLogs": [{ "scope": { "name": "s3dal" }, "logRecords": records }],
    }] });
    serde_json::to_vec(&body).expect("a JSON value serializes")
}

#[cfg(feature = "log-forward")]
fn otlp_attributes(out: &mut Vec<Value>, prefix: &str, key: String, value: Value) {
    let key = if prefix.is_empty() {
        key
    } else {
        format!("{prefix}.{key}")
    };
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                otlp_attributes(out, &key, k, v);
            }
        }
        value => {
            if let Some(value) = any_value(value) {
                out.push(serde_json::json!({ "key": key, "value": value }));
            }
        }
    }
}

/// OTLP's AnyValue, with an int64 as a string, as protobuf's JSON mapping writes it.
#[cfg(feature = "log-forward")]
fn any_value(value: Value) -> Option<Value> {
    use serde_json::json;
    Some(match value {
        Value::Null => return None,
        Value::Bool(b) => json!({ "boolValue": b }),
        Value::Number(n) => match n.as_i64() {
            Some(i) => json!({ "intValue": i.to_string() }),
            None => json!({ "doubleValue": n.as_f64() }),
        },
        Value::String(s) => json!({ "stringValue": s }),
        Value::Array(items) => {
            let values: Vec<Value> = items.into_iter().filter_map(any_value).collect();
            json!({ "arrayValue": { "values": values } })
        }
        Value::Object(map) => {
            let values: Vec<Value> = map
                .into_iter()
                .filter_map(|(k, v)| any_value(v).map(|v| json!({ "key": k, "value": v })))
                .collect();
            json!({ "kvlistValue": { "values": values } })
        }
    })
}

/// HEC events, one per line: its time, this host, and the line's fields as the event, typed
/// `_json` so Splunk extracts them.
#[cfg(feature = "log-forward")]
fn splunk(batch: &[Line], host: &str) -> Vec<u8> {
    let mut body = Vec::new();
    for line in batch {
        let mut event = object(&line.json);
        event.remove("timestamp");
        let doc = serde_json::json!({
            "time": seconds(line.at),
            "host": host,
            "source": "s3dal",
            "sourcetype": "_json",
            "event": event,
        });
        body.extend_from_slice(&serde_json::to_vec(&doc).expect("a JSON value serializes"));
        body.push(b'\n');
    }
    body
}

/// The lines as tracing wrote them, each on a line of its own.
#[cfg(feature = "log-forward")]
fn ndjson(batch: &[Line]) -> Vec<u8> {
    let mut body = Vec::new();
    for line in batch {
        body.extend_from_slice(line.json.trim_ascii_end());
        body.push(b'\n');
    }
    body
}

/// The message as datagrams: itself, or GELF chunks of it, each with the magic bytes, a message
/// id, its number and the count. `None` when it takes more chunks than GELF allows.
fn udp_datagrams(message: &[u8]) -> Option<Vec<Vec<u8>>> {
    if message.len() <= UDP_DATAGRAM {
        return Some(vec![message.to_vec()]);
    }
    let chunks: Vec<&[u8]> = message.chunks(UDP_DATAGRAM - UDP_CHUNK_HEADER).collect();
    if chunks.len() > UDP_MAX_CHUNKS {
        return None;
    }
    let id = &uuid::Uuid::new_v4().into_bytes()[..8];
    let count = u8::try_from(chunks.len()).ok()?;
    Some(
        chunks
            .iter()
            .zip(0u8..)
            .map(|(chunk, n)| {
                let mut d = Vec::with_capacity(UDP_CHUNK_HEADER + chunk.len());
                d.extend_from_slice(&[0x1e, 0x0f]);
                d.extend_from_slice(id);
                d.extend_from_slice(&[n, count]);
                d.extend_from_slice(chunk);
                d
            })
            .collect(),
    )
}

/// The host a collector files the lines under: in a container, its hostname, which Docker sets
/// to the container id, as `docker ps` shows it.
fn hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .or_else(|| std::fs::read_to_string("/proc/sys/kernel/hostname").ok())
        .or_else(|| std::fs::read_to_string("/etc/hostname").ok())
        .map(|h| h.trim().to_owned())
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "s3dal".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collector_urls_are_checked_when_read() {
        // A typo found at startup is fixed in a minute; one found when the logs are needed is
        // found too late.
        let target = |format, transport| Ok(Target { format, transport });
        assert_eq!(
            Target::parse("gelf+udp://graylog:12201"),
            target(Format::Gelf, Transport::Udp("graylog:12201".into()))
        );
        assert_eq!(
            Target::parse("gelf+tcp://[::1]:12201/"),
            target(Format::Gelf, Transport::Tcp("[::1]:12201".into()))
        );
        assert_eq!(
            Target::parse("syslog+udp://rsyslog:514"),
            target(Format::Syslog, Transport::Udp("rsyslog:514".into()))
        );
        assert_eq!(
            Target::parse("syslog+tcp://rsyslog:514"),
            target(Format::Syslog, Transport::Tcp("rsyslog:514".into()))
        );
        for bad in [
            "graylog:12201",
            "gelf://graylog:12201",
            "gelf+udp://graylog",
            "gelf+udp://graylog:0",
            "gelf+tcp://graylog:12201/path",
            "syslog://host:514",
            "syslog+http://host:514",
            "otlp+udp://collector:4318",
        ] {
            assert!(Target::parse(bad).is_err(), "{bad} accepted");
        }
    }

    #[cfg(feature = "log-forward")]
    #[test]
    fn http_collectors_get_their_paths_and_credentials() {
        let http = |url| match Target::parse(url) {
            Ok(Target {
                format,
                transport: Transport::Http(http),
            }) => (format, http),
            other => panic!("{url}: {other:?}"),
        };
        let (format, es) = http("elasticsearch+https://elastic:p%40ss@es:9200");
        // The credentials go in a header, decoded, never in the URL that errors may print.
        assert_eq!(format, Format::Bulk);
        assert_eq!(es.url, "https://es:9200/logs-s3dal-default/_bulk");
        assert_eq!(es.user.as_deref(), Some("elastic"));
        assert_eq!(es.password.as_deref(), Some("p@ss"));
        let (_, es) = http("elasticsearch+http://es:9200/app-logs?pipeline=s3dal");
        assert_eq!(es.url, "http://es:9200/app-logs/_bulk?pipeline=s3dal");
        assert_eq!(
            http("gelf+http://graylog:12201").1.url,
            "http://graylog:12201/gelf"
        );
        // OTLP receivers take logs at one path; an HTTP input's path is its own.
        let (format, otlp) = http("otlp+http://otel-collector:4318");
        assert_eq!(
            (format, otlp.url.as_str()),
            (Format::Otlp, "http://otel-collector:4318/v1/logs")
        );
        assert_eq!(
            http("ndjson+http://vector:8080").1.url,
            "http://vector:8080/"
        );
        assert_eq!(
            http("ndjson+https://logs.example/in").1.url,
            "https://logs.example/in"
        );
        let (format, hec) = http("splunk+https://:hec-token@splunk:8088");
        assert_eq!(format, Format::Splunk);
        assert_eq!(hec.url, "https://splunk:8088/services/collector/event");
        assert_eq!(
            (hec.user, hec.password.as_deref()),
            (None, Some("hec-token"))
        );
        for bad in [
            "elasticsearch+https://",
            // Every request would be refused.
            "splunk+https://splunk:8088",
        ] {
            assert!(Target::parse(bad).is_err(), "{bad} accepted");
        }
        assert_eq!(
            Target::parse("syslog+tls://logs.example.com:6514"),
            Ok(Target {
                format: Format::Syslog,
                transport: Transport::Tls("logs.example.com:6514".into())
            })
        );
    }

    #[test]
    fn a_password_is_never_shown_and_survives_a_console_edit() {
        // check-config's output is pasted into issues, and the console page holds only the
        // secrets it was given; the collector's password is in neither.
        let url = "elasticsearch+https://elastic:hunter2@es:9200/logs";
        assert_eq!(
            redact(url, "<redacted>"),
            "elasticsearch+https://elastic:<redacted>@es:9200/logs"
        );
        assert_eq!(
            redact(url, ""),
            "elasticsearch+https://elastic@es:9200/logs"
        );
        assert!(has_password(url) && !has_password("gelf+udp://g:1"));
        // An error message must not carry it either, even for a URL that does not parse.
        let err = Target::parse("elasticsearch+https://elastic:hunter2@").unwrap_err();
        assert!(!err.contains("hunter2"), "{err}");
        // Sent back without it, by a page that never had it: the password stays, as long as
        // the user and host are the ones it belongs to.
        let edited = "elasticsearch+https://elastic@es:9200/other-index";
        assert_eq!(
            keep_password(edited, Some(url)),
            "elasticsearch+https://elastic:hunter2@es:9200/other-index"
        );
        for elsewhere in [
            "elasticsearch+https://elastic@evil:9200/logs",
            "elasticsearch+https://admin@es:9200/logs",
            "elasticsearch+http://elastic@es:9200/logs",
        ] {
            assert_eq!(keep_password(elsewhere, Some(url)), elsewhere);
        }
    }

    #[test]
    fn a_token_without_a_user_is_hidden_and_kept_the_same_way() {
        // Splunk's token has no user before it; shown without it, the URL must not keep a
        // dangling `@`, and sent back so, it must get its token back.
        let url = "splunk+https://:hec-token@splunk:8088";
        assert_eq!(redact(url, ""), "splunk+https://splunk:8088");
        assert_eq!(
            redact(url, "<redacted>"),
            "splunk+https://:<redacted>@splunk:8088"
        );
        assert_eq!(keep_password("splunk+https://splunk:8088", Some(url)), url);
        assert_eq!(
            keep_password("splunk+https://splunk.evil:8088", Some(url)),
            "splunk+https://splunk.evil:8088"
        );
        // Nothing to keep from a URL that never had a password.
        assert_eq!(
            keep_password("gelf+tcp://g:1", Some("gelf+udp://g:1")),
            "gelf+tcp://g:1"
        );
    }

    fn line(json: &str) -> Line {
        Line {
            at: UNIX_EPOCH + Duration::from_millis(1_762_000_000_123),
            json: json.as_bytes().to_vec(),
        }
    }

    #[test]
    fn a_log_line_becomes_a_gelf_message() {
        // Graylog drops a message without `short_message` or with a field named `_id`, and
        // takes only strings and numbers in additional fields.
        let msg: Value = serde_json::from_slice(&gelf(
            &line(
                r#"{"timestamp":"x","level":"WARN","message":"request","status":503,
                "complete":false,"id":"7","span":{"id":"5A0A","name":"request"}}"#,
            ),
            "box",
        ))
        .unwrap();
        assert_eq!(msg["version"], "1.1");
        assert_eq!(msg["host"], "box");
        assert_eq!(msg["short_message"], "request");
        assert_eq!(msg["level"], 4);
        assert_eq!(msg["timestamp"], 1_762_000_000.123);
        assert_eq!(msg["_status"], 503);
        assert_eq!(msg["_complete"], "false");
        assert_eq!(msg["_id_"], "7");
        assert_eq!(msg["_span_id"], "5A0A");
        assert!(msg.get("_timestamp").is_none() && msg.get("_level").is_none());
    }

    #[test]
    fn a_log_line_becomes_a_syslog_message() {
        // rsyslog and syslog-ng read the header by position: a space in the wrong place shifts
        // every field after it. The message is the JSON, for their JSON parsers.
        let json =
            r#"{"timestamp":"2026-11-10T08:00:00.123456Z","level":"ERROR","message":"boom"}"#;
        let msg = String::from_utf8(syslog(&line(&format!("{json}\n")), "box")).unwrap();
        let pid = std::process::id();
        // daemon (3) * 8 + err (3)
        assert_eq!(
            msg,
            format!("<27>1 2026-11-10T08:00:00.123456Z box s3dal {pid} - - {json}")
        );
        let msg = String::from_utf8(syslog(&line("not json"), "a host")).unwrap();
        assert!(msg.starts_with("<31>1 - - s3dal "), "{msg}");
    }

    #[cfg(feature = "log-forward")]
    #[test]
    fn a_batch_becomes_a_bulk_request() {
        let body = bulk(&[line(
            r#"{"timestamp":"2026-11-10T00:00:00Z","level":"INFO"}"#,
        )]);
        let body = String::from_utf8(body).unwrap();
        let mut lines = body.lines();
        assert_eq!(lines.next(), Some(r#"{"create":{}}"#));
        let doc: Value = serde_json::from_str(lines.next().unwrap()).unwrap();
        assert_eq!(doc["@timestamp"], "2026-11-10T00:00:00Z");
        assert!(doc.get("timestamp").is_none());
    }

    #[cfg(feature = "log-forward")]
    #[test]
    fn a_batch_becomes_one_otlp_request() {
        // Collectors drop records whose attribute values are not AnyValues, and Grafana and
        // the others filter on severity and service.name.
        let body: Value = serde_json::from_slice(&otlp(
            &[
                line(r#"{"level":"WARN","message":"request","status":503,"span":{"id":"5A0A"}}"#),
                line(r#"{"level":"INFO","message":"ok","complete":true,"ratio":0.5}"#),
            ],
            "box",
        ))
        .unwrap();
        let resource = &body["resourceLogs"][0];
        assert_eq!(
            resource["resource"]["attributes"][0],
            serde_json::json!({"key":"service.name","value":{"stringValue":"s3dal"}})
        );
        let records = resource["scopeLogs"][0]["logRecords"].as_array().unwrap();
        assert_eq!(records.len(), 2);
        let first = &records[0];
        assert_eq!(first["timeUnixNano"], "1762000000123000000");
        assert_eq!(
            (
                first["severityNumber"].as_u64(),
                first["severityText"].as_str()
            ),
            (Some(13), Some("WARN"))
        );
        assert_eq!(first["body"]["stringValue"], "request");
        let attrs = first["attributes"].as_array().unwrap();
        assert!(
            attrs.contains(&serde_json::json!({"key":"status","value":{"intValue":"503"}})),
            "{attrs:?}"
        );
        assert!(
            attrs.contains(&serde_json::json!({"key":"span.id","value":{"stringValue":"5A0A"}})),
            "{attrs:?}"
        );
        let attrs = records[1]["attributes"].as_array().unwrap();
        assert!(attrs.contains(&serde_json::json!({"key":"complete","value":{"boolValue":true}})));
        assert!(attrs.contains(&serde_json::json!({"key":"ratio","value":{"doubleValue":0.5}})));
        // A partial success names what it rejected; a whole one is none.
        assert_eq!(
            otlp_error(r#"{"partialSuccess":{"rejectedLogRecords":"2","errorMessage":"too old"}}"#),
            Some("2 records rejected: too old".into())
        );
        assert_eq!(otlp_error(r#"{"partialSuccess":{}}"#), None);
        assert_eq!(otlp_error("{}"), None);
    }

    #[cfg(feature = "log-forward")]
    #[test]
    fn a_batch_becomes_splunk_events_and_plain_lines() {
        let body = splunk(
            &[line(r#"{"timestamp":"x","level":"INFO","message":"hi"}"#)],
            "box",
        );
        let event: Value = serde_json::from_slice(body.strip_suffix(b"\n").unwrap()).unwrap();
        assert_eq!(event["time"], 1_762_000_000.123);
        assert_eq!(
            (event["host"].as_str(), event["sourcetype"].as_str()),
            (Some("box"), Some("_json"))
        );
        assert_eq!(
            event["event"],
            serde_json::json!({"level":"INFO","message":"hi"})
        );
        // As tracing wrote them, one per line, however each ended.
        let body = ndjson(&[line("{\"a\":1}\n"), line("{\"b\":2}")]);
        assert_eq!(body, b"{\"a\":1}\n{\"b\":2}\n");
    }

    /// Accepts one HTTP request on `listener`, answers it with `answer`, and returns its head,
    /// lowercased, and its body.
    #[cfg(feature = "log-forward")]
    fn take_request(listener: &std::net::TcpListener, answer: &str) -> (Vec<String>, String) {
        use std::io::{BufRead, BufReader, Read};
        let (conn, _) = listener.accept().unwrap();
        conn.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut reader = BufReader::new(conn.try_clone().unwrap());
        let mut head = Vec::new();
        loop {
            let mut l = String::new();
            reader.read_line(&mut l).unwrap();
            if l == "\r\n" {
                break;
            }
            head.push(l.trim_end().to_ascii_lowercase());
        }
        let len: usize = head
            .iter()
            .find_map(|h| h.strip_prefix("content-length: "))
            .unwrap()
            .parse()
            .unwrap();
        let mut body = vec![0; len];
        reader.read_exact(&mut body).unwrap();
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n{answer}",
            answer.len()
        );
        (&conn).write_all(response.as_bytes()).unwrap();
        (head, String::from_utf8(body).unwrap())
    }

    #[cfg(feature = "log-forward")]
    #[test]
    fn lines_reach_elasticsearch_in_a_bulk_request_with_the_password_in_a_header() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (forwarder, _) =
            start(&format!("elasticsearch+http://elastic:p%40ss@{addr}/app")).unwrap();
        forwarder
            .make_writer()
            .write_all(br#"{"timestamp":"2026-11-10T00:00:00Z","level":"INFO","message":"hi"}"#)
            .unwrap();
        let (head, body) = take_request(&listener, r#"{"errors":false}"#);
        assert_eq!(head[0], "post /app/_bulk http/1.1");
        // "elastic:p@ss", decoded from the URL before it is encoded for the header.
        assert!(
            head.contains(&"authorization: basic zwxhc3rpyzpwqhnz".to_owned()),
            "{head:?}"
        );
        let mut lines = body.lines();
        assert_eq!(lines.next(), Some(r#"{"create":{}}"#));
        let doc: Value = serde_json::from_str(lines.next().unwrap()).unwrap();
        assert_eq!(doc["message"], "hi");
    }

    #[cfg(feature = "log-forward")]
    #[test]
    fn splunk_gets_its_token_in_its_own_header() {
        // HEC refuses Basic authentication; the token goes as `Splunk <token>`.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (forwarder, _) = start(&format!("splunk+http://:hec-token@{addr}")).unwrap();
        forwarder
            .make_writer()
            .write_all(br#"{"level":"INFO","message":"hi"}"#)
            .unwrap();
        let (head, body) = take_request(&listener, r#"{"text":"Success","code":0}"#);
        assert_eq!(head[0], "post /services/collector/event http/1.1");
        assert!(
            head.contains(&"authorization: splunk hec-token".to_owned()),
            "{head:?}"
        );
        let event: Value = serde_json::from_str(body.trim_end()).unwrap();
        assert_eq!(event["event"]["message"], "hi");
    }

    #[test]
    fn exit_waits_for_the_queue_but_not_for_a_collector_that_is_down() {
        use std::io::Read;
        // The last lines, which say why the server stopped, are the ones a collector most
        // needs, and the ones still queued when it exits.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let reader = std::thread::spawn(move || {
            let (mut conn, _) = listener.accept().unwrap();
            let mut all = Vec::new();
            conn.read_to_end(&mut all).unwrap();
            all.iter().filter(|b| **b == 0).count()
        });
        let (forwarder, flush) = start(&format!("gelf+tcp://{addr}")).unwrap();
        for n in 0..2_000 {
            let line = format!(r#"{{"message":"line {n}"}}"#);
            forwarder.make_writer().write_all(line.as_bytes()).unwrap();
        }
        assert!(flush.wait(Duration::from_secs(10)));
        // Closing the connection, as exiting does, ends the reader with everything sent.
        drop(forwarder);
        assert_eq!(reader.join().unwrap(), 2_000);

        let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = closed.local_addr().unwrap();
        drop(closed);
        let (forwarder, flush) = start(&format!("gelf+tcp://{addr}")).unwrap();
        forwarder.make_writer().write_all(b"{}").unwrap();
        let begun = std::time::Instant::now();
        assert!(!flush.wait(Duration::from_millis(300)));
        assert!(begun.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn syslog_over_tcp_frames_each_message_by_its_length() {
        use std::io::Read;
        // Octet counting needs no delimiter that a message could hold, and it is the framing
        // rsyslog and syslog-ng take on a TCP input without setup.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (forwarder, flush) = start(&format!("syslog+tcp://{addr}")).unwrap();
        for message in ["one", "two\nlines"] {
            let line = serde_json::json!({ "level": "INFO", "message": message }).to_string();
            forwarder.make_writer().write_all(line.as_bytes()).unwrap();
        }
        let (mut conn, _) = listener.accept().unwrap();
        assert!(flush.wait(Duration::from_secs(10)));
        drop(forwarder);
        let mut all = String::new();
        conn.read_to_string(&mut all).unwrap();
        let mut rest = all.as_str();
        let mut messages = Vec::new();
        while !rest.is_empty() {
            let (len, after) = rest.split_once(' ').unwrap();
            let len: usize = len.parse().unwrap();
            messages.push(&after[..len]);
            rest = &after[len..];
        }
        assert_eq!(messages.len(), 2, "{all:?}");
        assert!(messages[0].starts_with("<30>1 "), "{}", messages[0]);
        assert!(
            messages[1].ends_with(r#""message":"two\nlines"}"#),
            "{}",
            messages[1]
        );
    }

    #[test]
    fn long_udp_messages_are_chunked() {
        // Past one datagram, an unchunked message is cut by the network or refused by Graylog.
        let message = vec![b'x'; UDP_DATAGRAM * 2];
        let datagrams = udp_datagrams(&message).unwrap();
        assert_eq!(datagrams.len(), 3);
        assert!(datagrams.iter().all(|d| d.len() <= UDP_DATAGRAM));
        assert_eq!(&datagrams[2][..2], &[0x1e, 0x0f]);
        assert_eq!(datagrams[2][10..12], [2, 3]);
        // All chunks of one message share its id.
        assert_eq!(datagrams[0][2..10], datagrams[2][2..10]);
        let joined: usize = datagrams.iter().map(|d| d.len() - UDP_CHUNK_HEADER).sum();
        assert_eq!(joined, message.len());
        assert!(udp_datagrams(&vec![b'x'; UDP_DATAGRAM * 129]).is_none());
    }

    #[test]
    fn lines_reach_a_tcp_collector_and_one_down_holds_up_nobody() {
        use std::io::Read;
        // The writer runs on a request's thread; it must return at once, collector or not.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (forwarder, _) = start(&format!("gelf+tcp://{addr}")).unwrap();
        forwarder
            .make_writer()
            .write_all(br#"{"level":"INFO","message":"hello"}"#)
            .unwrap();
        let (mut conn, _) = listener.accept().unwrap();
        conn.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut got = Vec::new();
        let mut byte = [0];
        while conn.read(&mut byte).unwrap() == 1 && byte[0] != 0 {
            got.push(byte[0]);
        }
        let msg: Value = serde_json::from_slice(&got).unwrap();
        assert_eq!(msg["short_message"], "hello");

        // Nobody listens here: lines past the queue are dropped, never waited for.
        let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = closed.local_addr().unwrap();
        drop(closed);
        let (forwarder, _) = start(&format!("gelf+tcp://{addr}")).unwrap();
        let begun = std::time::Instant::now();
        for _ in 0..QUEUE + 2 * BATCH {
            forwarder.make_writer().write_all(b"{}").unwrap();
        }
        assert!(begun.elapsed() < Duration::from_secs(5));
        // The sending thread holds at most one batch out of the queue while it retries.
        assert!(forwarder.dropped.load(Ordering::Relaxed) >= BATCH as u64);
    }
}

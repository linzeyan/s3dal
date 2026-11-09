//! Sends the log to a collector as well as to stderr or the log file: Graylog over GELF (UDP,
//! TCP or HTTP), or Elasticsearch and OpenSearch over their bulk API.
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
/// Lines per bulk request, and the most sent before the dropped count is looked at again.
const BATCH: usize = 500;
const TIMEOUT: Duration = Duration::from_secs(10);
const MAX_PAUSE: Duration = Duration::from_secs(30);
/// GELF over UDP: a datagram is at most 8192 bytes, a message at most 128 of them.
const UDP_DATAGRAM: usize = 8192;
const UDP_CHUNK_HEADER: usize = 12;
const UDP_MAX_CHUNKS: usize = 128;

/// Where the log is sent, as `--log-forward` names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// GELF, one datagram per message, split into chunks past one datagram.
    GelfUdp(String),
    /// GELF, each message ended by a NUL byte, on a connection kept open.
    GelfTcp(String),
    /// GELF, one POST per message, the way Graylog's HTTP input takes them.
    GelfHttp(Http),
    /// The bulk API, one POST per batch.
    Elasticsearch(Http),
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
        match scheme {
            "gelf+udp" => host_port(rest).map(Self::GelfUdp),
            "gelf+tcp" => host_port(rest).map(Self::GelfTcp),
            "gelf+http" | "gelf+https" => http(scheme, rest, "/gelf").map(Self::GelfHttp),
            "elasticsearch+http" | "elasticsearch+https" => {
                // A data stream name that Elasticsearch's built-in `logs-*-*` template matches,
                // so the index needs no setup there.
                let mut http = http(scheme, rest, "/logs-s3dal-default")?;
                http.url = bulk_url(&http.url);
                Ok(Self::Elasticsearch(http))
            }
            other => Err(format!(
                "unknown collector {other:?}; use gelf+udp, gelf+tcp, gelf+http, gelf+https, \
                 elasticsearch+http or elasticsearch+https"
            )),
        }
    }
}

/// Kept as text and resolved on every connect: the collector's name may resolve only once its
/// container is up, and to another address after it is replaced.
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
fn http(scheme: &str, rest: &str, default_path: &str) -> Result<Http, String> {
    use percent_encoding::percent_decode_str;
    let transport = scheme.split_once('+').map_or(scheme, |(_, t)| t);
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

/// The parts of a URL around its password, by hand rather than with a URL parser: `redact`
/// must work in every build, and the password must go even from a URL a parser refuses.
struct Parts<'a> {
    /// Up to and including `://`, and the user.
    head: &'a str,
    password: Option<&'a str>,
    /// From the `@` on.
    tail: &'a str,
}

fn parts(url: &str) -> Option<Parts<'_>> {
    let start = url.find("://")? + 3;
    let authority_end = url[start..]
        .find(['/', '?', '#'])
        .map_or(url.len(), |i| start + i);
    let at = start + url[start..authority_end].rfind('@')?;
    let (head, password) = match url[start..at].find(':') {
        Some(colon) => (&url[..start + colon], Some(&url[start + colon + 1..at])),
        None => (&url[..at], None),
    };
    Some(Parts {
        head,
        password,
        tail: &url[at..],
    })
}

/// The URL with its password replaced by `mask`, or taken out with an empty one: for
/// `check-config`, the log and the console, none of which should show it.
pub fn redact(url: &str, mask: &str) -> String {
    match parts(url) {
        Some(Parts {
            head,
            password: Some(_),
            tail,
        }) if mask.is_empty() => format!("{head}{tail}"),
        Some(Parts {
            head,
            password: Some(_),
            tail,
        }) => format!("{head}:{mask}{tail}"),
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
    let host = |tail: &str| {
        tail.find(['/', '?', '#'])
            .map_or(tail.to_owned(), |i| tail[..i].to_owned())
    };
    match (n.password, c.password) {
        (None, Some(password)) if n.head == c.head && host(n.tail) == host(c.tail) => {
            format!("{}:{password}{}", n.head, n.tail)
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
    let sink = Sink::new(&target)?;
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
        .spawn(move || run(sink, &lines, &counter, &sent, &shown))
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
    mut sink: Sink,
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
        let payloads = sink.encode(&batch, &host);
        let mut pause = Duration::from_secs(1);
        let mut i = 0;
        while i < payloads.len() {
            match sink.deliver(&payloads[i]) {
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

enum Sink {
    Udp {
        addr: String,
        socket: Option<(UdpSocket, SocketAddr)>,
    },
    Tcp {
        addr: String,
        stream: Option<TcpStream>,
    },
    #[cfg(feature = "log-forward")]
    Http {
        runtime: tokio::runtime::Runtime,
        client: reqwest::Client,
        http: Http,
        bulk: bool,
    },
}

impl Sink {
    fn new(target: &Target) -> Result<Self, String> {
        Ok(match target {
            Target::GelfUdp(addr) => Self::Udp {
                addr: addr.clone(),
                socket: None,
            },
            Target::GelfTcp(addr) => Self::Tcp {
                addr: addr.clone(),
                stream: None,
            },
            #[cfg(feature = "log-forward")]
            Target::GelfHttp(http) | Target::Elasticsearch(http) => Self::Http {
                runtime: tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| format!("cannot start the log forwarding runtime: {e}"))?,
                client: reqwest::Client::builder()
                    .timeout(TIMEOUT)
                    .build()
                    .map_err(|e| format!("cannot build the log forwarding client: {e}"))?,
                http: http.clone(),
                bulk: matches!(target, Target::Elasticsearch(_)),
            },
            #[cfg(not(feature = "log-forward"))]
            Target::GelfHttp(_) | Target::Elasticsearch(_) => unreachable!("refused by parse"),
        })
    }

    /// What `deliver` sends for a batch: one GELF message per line, or one bulk request body.
    fn encode(&self, batch: &[Line], host: &str) -> Vec<Vec<u8>> {
        #[cfg(feature = "log-forward")]
        if let Self::Http { bulk: true, .. } = self {
            let mut body = Vec::new();
            for line in batch {
                body.extend_from_slice(b"{\"create\":{}}\n");
                body.extend_from_slice(&bulk_doc(&line.json));
                body.push(b'\n');
            }
            return vec![body];
        }
        batch.iter().map(|line| gelf(line, host)).collect()
    }

    fn deliver(&mut self, payload: &[u8]) -> Result<(), Failure> {
        match self {
            Self::Udp { addr, socket } => {
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
                let Some(datagrams) = udp_datagrams(payload) else {
                    return Err(Failure::Refused(format!(
                        "a message of {} bytes is longer than GELF over UDP can carry; use \
                         gelf+tcp",
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
            Self::Tcp { addr, stream } => {
                if stream.is_none() {
                    let to = resolve(addr)?;
                    let s = TcpStream::connect_timeout(&to, TIMEOUT)
                        .map_err(|e| Failure::Retry(e.to_string()))?;
                    let _ = s.set_write_timeout(Some(TIMEOUT));
                    *stream = Some(s);
                }
                let s = stream.as_mut().expect("connected above");
                let sent = s.write_all(payload).and_then(|()| s.write_all(&[0]));
                sent.map_err(|e| {
                    *stream = None;
                    Failure::Retry(e.to_string())
                })
            }
            #[cfg(feature = "log-forward")]
            Self::Http {
                runtime,
                client,
                http,
                bulk,
            } => runtime.block_on(post(client, http, *bulk, payload.to_vec())),
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
async fn post(
    client: &reqwest::Client,
    http: &Http,
    bulk: bool,
    body: Vec<u8>,
) -> Result<(), Failure> {
    let content_type = if bulk {
        "application/x-ndjson"
    } else {
        "application/json"
    };
    let mut req = client
        .post(&http.url)
        .header(reqwest::header::CONTENT_TYPE, content_type)
        .body(body);
    if let Some(user) = &http.user {
        req = req.basic_auth(user, http.password.as_ref());
    }
    let res = req
        .send()
        .await
        .map_err(|e| Failure::Retry(e.to_string()))?;
    let status = res.status();
    let text = res.text().await.unwrap_or_default();
    if status.is_success() {
        // A bulk request succeeds as a whole and may still have refused some documents, a
        // field whose type the index maps differently, say.
        return match bulk.then(|| bulk_error(&text)).flatten() {
            Some(e) => Err(Failure::Refused(e)),
            None => Ok(()),
        };
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

/// A bulk document: the line's fields as they are, its time as `@timestamp`, the field
/// Elasticsearch's log templates and Kibana sort by.
#[cfg(feature = "log-forward")]
fn bulk_doc(json: &[u8]) -> Vec<u8> {
    let mut doc = object(json);
    if let Some(ts) = doc.remove("timestamp") {
        doc.insert("@timestamp".into(), ts);
    }
    serde_json::to_vec(&doc).expect("a JSON map serializes")
}

/// A GELF 1.1 message. Its own fields are fixed; every other one is an additional field, named
/// with a leading `_`, nested fields (the request's span) joined with `_`, as `_span_id`.
fn gelf(line: &Line, host: &str) -> Vec<u8> {
    let mut fields = object(&line.json);
    fields.remove("timestamp");
    let level = match fields.remove("level").as_ref().and_then(Value::as_str) {
        Some("ERROR") => 3,
        Some("WARN") => 4,
        Some("INFO") => 6,
        _ => 7,
    };
    let message = match fields.remove("message") {
        Some(Value::String(s)) if !s.is_empty() => s,
        Some(other) if !other.is_null() => other.to_string(),
        _ => "-".to_owned(),
    };
    let at = line.at.duration_since(UNIX_EPOCH).unwrap_or_default();
    let mut out = Map::new();
    out.insert("version".into(), "1.1".into());
    out.insert("host".into(), host.into());
    out.insert("short_message".into(), message.into());
    // Seconds with milliseconds, as GELF takes it.
    let millis = u64::try_from(at.as_millis()).unwrap_or(u64::MAX);
    out.insert(
        "timestamp".into(),
        serde_json::Number::from_f64(millis as f64 / 1000.0).map_or(Value::Null, Value::Number),
    );
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

/// The `host` GELF requires: in a container, its hostname, which Docker sets to the container
/// id, as `docker ps` shows it.
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
        assert_eq!(
            Target::parse("gelf+udp://graylog:12201"),
            Ok(Target::GelfUdp("graylog:12201".into()))
        );
        assert_eq!(
            Target::parse("gelf+tcp://[::1]:12201/"),
            Ok(Target::GelfTcp("[::1]:12201".into()))
        );
        for bad in [
            "graylog:12201",
            "gelf://graylog:12201",
            "gelf+udp://graylog",
            "gelf+udp://graylog:0",
            "gelf+tcp://graylog:12201/path",
            "syslog://host:514",
        ] {
            assert!(Target::parse(bad).is_err(), "{bad} accepted");
        }
    }

    #[cfg(feature = "log-forward")]
    #[test]
    fn http_collectors_get_their_paths_and_credentials() {
        let Ok(Target::Elasticsearch(es)) =
            Target::parse("elasticsearch+https://elastic:p%40ss@es:9200")
        else {
            panic!("not parsed as Elasticsearch")
        };
        // The credentials go in a header, decoded, never in the URL that errors may print.
        assert_eq!(es.url, "https://es:9200/logs-s3dal-default/_bulk");
        assert_eq!(es.user.as_deref(), Some("elastic"));
        assert_eq!(es.password.as_deref(), Some("p@ss"));
        let Ok(Target::Elasticsearch(es)) =
            Target::parse("elasticsearch+http://es:9200/app-logs?pipeline=s3dal")
        else {
            panic!("not parsed as Elasticsearch")
        };
        assert_eq!(es.url, "http://es:9200/app-logs/_bulk?pipeline=s3dal");
        let Ok(Target::GelfHttp(gelf)) = Target::parse("gelf+http://graylog:12201") else {
            panic!("not parsed as GELF over HTTP")
        };
        assert_eq!(gelf.url, "http://graylog:12201/gelf");
        assert!(Target::parse("elasticsearch+https://").is_err());
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

    #[cfg(feature = "log-forward")]
    #[test]
    fn a_log_line_becomes_a_bulk_document() {
        let doc: Value = serde_json::from_slice(&bulk_doc(
            br#"{"timestamp":"2026-11-10T00:00:00Z","level":"INFO"}"#,
        ))
        .unwrap();
        assert_eq!(doc["@timestamp"], "2026-11-10T00:00:00Z");
        assert!(doc.get("timestamp").is_none());
    }

    #[cfg(feature = "log-forward")]
    #[test]
    fn lines_reach_elasticsearch_in_a_bulk_request_with_the_password_in_a_header() {
        use std::io::{BufRead, BufReader, Read};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (forwarder, _) =
            start(&format!("elasticsearch+http://elastic:p%40ss@{addr}/app")).unwrap();
        forwarder
            .make_writer()
            .write_all(br#"{"timestamp":"2026-11-10T00:00:00Z","level":"INFO","message":"hi"}"#)
            .unwrap();
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
        (&conn)
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 16\r\n\r\n{\"errors\":false}")
            .unwrap();
        assert_eq!(head[0], "post /app/_bulk http/1.1");
        // "elastic:p@ss", decoded from the URL before it is encoded for the header.
        assert!(
            head.contains(&"authorization: basic zwxhc3rpyzpwqhnz".to_owned()),
            "{head:?}"
        );
        let body = String::from_utf8(body).unwrap();
        let mut lines = body.lines();
        assert_eq!(lines.next(), Some(r#"{"create":{}}"#));
        let doc: Value = serde_json::from_str(lines.next().unwrap()).unwrap();
        assert_eq!(doc["@timestamp"], "2026-11-10T00:00:00Z");
        assert_eq!(doc["message"], "hi");
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

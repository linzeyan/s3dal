//! The console's settings API: what the server runs with, and edits of the config file, which
//! reach the server through the same reload as an edit by hand.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use hyper::header::{CONTENT_TYPE, HeaderValue};
use hyper::http::Extensions;
use hyper::{HeaderMap, Method, Uri};
use s3s::route::S3Route;
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use serde::Deserialize;
use serde_json::{Value, json};

use super::ConsoleRequest;
use crate::access::{Access, Expiry};
use crate::backend;
use crate::config::{ServeArgs, format_duration, format_size, is_secretish};
use crate::live::Live;
use crate::logging::forward;
use crate::s3::{BucketUsage, Quota};

const PREFIX: &str = "/_s3dal/admin/";

/// Under [`PREFIX`]: `buckets/<bucket>`, a bucket's own settings.
const BUCKETS: &str = "buckets/";

/// The settings a reload applies at once.
const LIVE: [&str; 8] = [
    "region",
    "domain",
    "etag-buffer-limit",
    "stale-upload-age",
    "usage-scan-interval",
    "log-level",
    "metrics",
    "metrics-token",
];

/// The settings that take a restart: an edit is written to the file, and the server and
/// settings pages say what waits for the restart, which they offer to do.
const RESTART: [&str; 11] = [
    "backend",
    "cache-size",
    "address",
    "console-address",
    "tls-cert",
    "tls-key",
    "log-format",
    "log-file",
    "log-max-size",
    "log-max-files",
    "log-forward",
];

/// A new backend is built and probed before it is written, as at startup: one that fails
/// would otherwise be found only by the restart, which it would keep from starting. The probe
/// writes and reads a few small objects; a remote backend that has not answered by then is
/// as good as broken.
const PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// A settings edit is a few hundred bytes; the bound keeps a signed client from making the
/// server hold an arbitrary body.
const MAX_EDIT: usize = 64 * 1024;

pub(crate) struct AdminRoute {
    live: Arc<Live>,
    started: Instant,
}

impl AdminRoute {
    pub(crate) fn new(live: Arc<Live>) -> Self {
        Self {
            live,
            started: Instant::now(),
        }
    }
}

#[async_trait::async_trait]
impl S3Route for AdminRoute {
    fn is_match(&self, _: &Method, uri: &Uri, _: &HeaderMap, extensions: &mut Extensions) -> bool {
        let ours = extensions.get::<ConsoleRequest>().is_some() && uri.path().starts_with(PREFIX);
        if ours {
            crate::metrics::note_operation("ConsoleAdmin");
        }
        ours
    }

    /// Any valid signature may ask who it is, which is how the console signs in, and read the
    /// settings of a bucket it reaches, so the console can show whether the bucket is public;
    /// the rest is for key pairs without limits, since the settings reach past any one bucket.
    async fn check_access(&self, req: &mut S3Request<Body>) -> S3Result<()> {
        if req.credentials.is_none() {
            return Err(s3_error!(AccessDenied, "Signature is required"));
        }
        let Some(limit) = self.live.auth().limit(req.credentials.as_ref()) else {
            return Ok(());
        };
        let name = req.uri.path().strip_prefix(PREFIX);
        let open = name == Some("whoami")
            || (req.method == Method::GET
                && name
                    .and_then(|n| n.strip_prefix(BUCKETS))
                    .is_some_and(|b| limit.reaches_bucket(b)));
        if !open {
            return Err(s3_error!(
                AccessDenied,
                "this key pair has limits; the server's settings need one without"
            ));
        }
        Ok(())
    }

    async fn call(&self, mut req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        let name = req.uri.path().strip_prefix(PREFIX).unwrap_or_default();
        let doc = match (&req.method, name) {
            (method, name) if name.starts_with(BUCKETS) => {
                let admin = self.live.auth().limit(req.credentials.as_ref()).is_none();
                self.bucket(method, &mut req.input, &name[BUCKETS.len()..], admin)
                    .await?
            }
            (&Method::GET, "whoami") => self.whoami(&req),
            (&Method::GET, "info") => self.info(),
            (&Method::GET, "stats") => self.live.dal().metrics().stats(),
            (&Method::GET, "usage") => self.usage().await?,
            (&Method::GET, "config") => config_view(&self.live),
            (&Method::PUT, "config") => {
                let body =
                    req.input.store_all_limited(MAX_EDIT).await.map_err(|_| {
                        s3_error!(EntityTooLarge, "a settings edit is at most 64 KiB")
                    })?;
                let edit: Edit =
                    serde_json::from_slice(&body).map_err(|e| s3_error!(InvalidArgument, "{e}"))?;
                let signer = req.credentials.as_ref().map(|c| c.access_key.as_str());
                self.apply(edit, signer).await?;
                config_view(&self.live)
            }
            (&Method::POST, "restart") => {
                self.restart().await?;
                json!({ "restarting": true })
            }
            _ => return Err(s3_error!(MethodNotAllowed, "no such settings request")),
        };
        let mut res = S3Response::new(Body::from(doc.to_string()));
        res.headers
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Ok(res)
    }
}

impl AdminRoute {
    /// What the signing pair reaches, so the console shows only what it can do.
    fn whoami(&self, req: &S3Request<Body>) -> Value {
        let limit = self.live.auth().limit(req.credentials.as_ref());
        json!({
            "access_key": req.credentials.as_ref().map(|c| c.access_key.as_str()),
            "admin": limit.is_none(),
            "write": limit.as_ref().is_none_or(|l| l.may_write()),
            "allow": limit.as_ref().and_then(|l| l.scopes()).map(|s| {
                s.iter().map(|s| json!({ "bucket": s.bucket, "prefix": s.prefix })).collect::<Vec<_>>()
            }),
        })
    }

    /// A bucket's settings that S3 has no request for, kept in the bucket's state (F1), so
    /// every instance sharing the backend serves them: `{"public": bool, "quota": {"max_bytes",
    /// "max_objects"} or null}`, and what the bucket holds, `usage`. A PUT changes the fields it
    /// lists; `"quota": null` removes the quota. For a pair without limits, `notify` lists the
    /// server's notification targets, which the bucket's notification rules name, with how
    /// their deliveries went; a pair with limits gets `null`, as the targets are the server's.
    /// `tiers` lists the cold tiers, `{"name", "storage_class"}`, for every reader: they are the
    /// storage classes a write into the bucket can name.
    async fn bucket(
        &self,
        method: &Method,
        input: &mut Body,
        bucket: &str,
        admin: bool,
    ) -> S3Result<Value> {
        let dal = self.live.dal();
        if method == Method::PUT {
            let body = input
                .store_all_limited(MAX_EDIT)
                .await
                .map_err(|_| s3_error!(EntityTooLarge, "a settings edit is at most 64 KiB"))?;
            let edit: BucketEdit =
                serde_json::from_slice(&body).map_err(|e| s3_error!(InvalidArgument, "{e}"))?;
            if let Some(public) = edit.public {
                dal.set_bucket_public(bucket, public).await?;
                tracing::info!(bucket, public, "bucket access changed");
            }
            if let Some(edit) = edit.quota {
                // Fields a newer server wrote into the quota are kept through the edit.
                let unknown = dal
                    .bucket_quota(bucket)
                    .await?
                    .map(|q| q.unknown)
                    .unwrap_or_default();
                let quota = edit.map(|q| Quota {
                    max_bytes: q.max_bytes,
                    max_objects: q.max_objects,
                    unknown,
                });
                dal.set_bucket_quota(bucket, quota.clone()).await?;
                tracing::info!(
                    bucket,
                    max_bytes = quota.as_ref().map(|q| q.max_bytes),
                    max_objects = quota.as_ref().and_then(|q| q.max_objects),
                    "bucket quota changed"
                );
            }
        } else if method != Method::GET {
            return Err(s3_error!(MethodNotAllowed, "no such settings request"));
        }
        Ok(json!({
            "public": dal.bucket_public(bucket).await?,
            "quota": quota_json(dal.bucket_quota(bucket).await?.as_ref()),
            "usage": usage_json(dal.bucket_usage(bucket).await?),
            "notify": admin.then(|| notify_json(dal)),
            "tiers": dal
                .tiers()
                .iter()
                .map(|t| json!({ "name": t.name(), "storage_class": t.storage_class() }))
                .collect::<Vec<_>>(),
        }))
    }

    /// What every bucket holds and its quota, for the dashboard.
    async fn usage(&self) -> S3Result<Value> {
        let dal = self.live.dal();
        let buckets: Vec<Value> = dal
            .buckets_usage()
            .await?
            .into_iter()
            .map(|(name, usage, quota)| {
                json!({
                    "name": name,
                    "usage": usage_json(usage),
                    "quota": quota_json(quota.as_ref()),
                })
            })
            .collect();
        Ok(json!({
            "buckets": buckets,
            "scan_interval_secs": dal.settings().usage_scan_interval.map(|d| d.as_secs()),
        }))
    }

    fn info(&self) -> Value {
        let started = self.live.started();
        json!({
            "version": env!("CARGO_PKG_VERSION"),
            "backend": started.backend,
            "profile": self.live.dal().profile,
            "uptime_secs": self.started.elapsed().as_secs(),
            "address": started.address.to_string(),
            "console_address": started.console_address.map(|a| a.to_string()),
            "tls": started.tls_cert.is_some(),
            "pending_restart": self.live.pending_restart(),
            // Where the running server's log goes, which a page cannot otherwise tell: without
            // a file it is stderr, which in a container is `docker logs`.
            "log_file": started.log_file.as_ref().map(|p| p.display().to_string()),
            "log_forward": started.log_forward.as_deref().map(|u| forward::redact(u, "")),
            "log_forward_state": started.log_forward.as_ref().map(|_| {
                let state = forward::state();
                json!({ "failing": state.failing, "dropped": state.dropped })
            }),
            "probe": self.live.dal().metrics().probe_json(),
            "metrics": self.live.dal().settings().metrics.as_str(),
            "tiers": tiers_json(self.live.dal()),
        })
    }

    /// Writes the edit into the config file and reloads. A file the reload refuses is put back
    /// as it was, so the file never holds settings the server does not run with.
    async fn apply(&self, edit: Edit, signer: Option<&str>) -> S3Result<()> {
        let current = self.live.current();
        let path = current
            .config
            .as_deref()
            .ok_or_else(|| s3_error!(InvalidRequest, "the server runs without a config file"))?;
        let old = std::fs::read_to_string(path)
            .map_err(|e| s3_error!(InternalError, "cannot read {}: {e}", path.display()))?;
        let mut doc: toml_edit::DocumentMut = old
            .parse()
            .map_err(|e| s3_error!(InvalidRequest, "the config file does not parse: {e}"))?;
        let locked = |name: &str| {
            if current.locked.iter().any(|l| l == name) {
                Err(s3_error!(
                    InvalidArgument,
                    "`{name}` is set by a flag or environment variable, which outranks the file"
                ))
            } else {
                Ok(())
            }
        };

        let new_backend = match edit.settings.get("backend") {
            Some(Value::String(s)) => Some(s.trim().to_owned()),
            Some(_) => return Err(s3_error!(InvalidArgument, "`backend` cannot be empty")),
            None => None,
        };
        let new_opts = match &edit.backend_opts {
            Some(opts) => {
                locked("backend-opt")?;
                Some(backend_opts(&current, opts)?)
            }
            None => None,
        };
        if new_backend.is_some() || new_opts.is_some() {
            let scheme = new_backend.as_deref().unwrap_or(&current.backend);
            let opts = new_opts.as_ref().unwrap_or(&current.backend_opts);
            check_backend(scheme, opts).await?;
        }
        if let Some(opts) = new_opts {
            doc.remove("backend-opt");
            if !opts.is_empty() {
                let mut table = toml_edit::Table::new();
                for (name, value) in opts {
                    table[name.as_str()] = toml_edit::value(value);
                }
                doc.insert("backend-opt", toml_edit::Item::Table(table));
            }
        }

        for (name, value) in edit.settings {
            if !LIVE.contains(&name.as_str()) && !RESTART.contains(&name.as_str()) {
                return Err(s3_error!(
                    InvalidArgument,
                    "`{name}` cannot be changed here"
                ));
            }
            locked(&name)?;
            match value {
                Value::Null => {
                    doc.remove(&name);
                }
                Value::String(s) if name == "log-forward" => {
                    let url = forward::keep_password(&s, current.log_forward.as_deref());
                    doc[name.as_str()] = toml_edit::value(url);
                }
                Value::String(s) => doc[name.as_str()] = toml_edit::value(s),
                Value::Array(items) => {
                    let mut array = toml_edit::Array::new();
                    for item in items {
                        let Value::String(s) = item else {
                            return Err(s3_error!(InvalidArgument, "`{name}`: expected strings"));
                        };
                        array.push(s);
                    }
                    doc[name.as_str()] = toml_edit::value(array);
                }
                _ => return Err(s3_error!(InvalidArgument, "`{name}`: expected a string")),
            }
        }
        if let Some(pairs) = edit.credentials {
            doc.remove("credentials");
            let tables = key_pairs(&current, pairs, signer)?;
            if !tables.is_empty() {
                doc.insert("credentials", toml_edit::Item::ArrayOfTables(tables));
            }
        }

        let new = doc.to_string();
        write(path, &new)?;
        if let Err(err) = self.live.reload() {
            write(path, &old)?;
            return Err(s3_error!(InvalidArgument, "{err:#}"));
        }
        Ok(())
    }

    /// Asks the server to start again, once it is clear that it would: a start that fails
    /// leaves nothing serving, this console included, to put the settings right from.
    async fn restart(&self) -> S3Result<()> {
        if !cfg!(unix) {
            return Err(s3_error!(
                NotImplemented,
                "restarting from the console needs a Unix system"
            ));
        }
        let args = self
            .live
            .read()
            .map_err(|e| s3_error!(InvalidArgument, "the settings do not read: {e:#}"))?;
        preflight(&args, self.live.started()).await?;
        self.live.request_restart();
        Ok(())
    }
}

/// Tries what a start does that can fail, while the server still runs. The addresses it holds
/// are free again once it stops; any other must be free now.
async fn preflight(args: &ServeArgs, started: &ServeArgs) -> S3Result<()> {
    let refuse =
        |what: String| s3_error!(InvalidArgument, "the server would not start again: {what}");
    crate::server::LiveAuth::new(&args.credentials, &args.domain)
        .map_err(|e| refuse(format!("{e:#}")))?;
    crate::logging::filter(&args.log_level).map_err(refuse)?;
    if let (Some(cert), Some(key)) = (&args.tls_cert, &args.tls_key) {
        crate::server::tls_acceptor(cert, key).map_err(|e| refuse(format!("{e:#}")))?;
    }
    if let Some(path) = &args.log_file {
        crate::logging::RotatingFile::open(path, args.log_max_size, args.log_max_files)
            .map_err(|e| refuse(format!("cannot open the log file {}: {e}", path.display())))?;
    }
    let held = |port: u16| {
        port == started.address.port() || started.console_address.is_some_and(|a| a.port() == port)
    };
    for addr in std::iter::once(args.address).chain(args.console_address) {
        if !held(addr.port()) {
            tokio::net::TcpListener::bind(addr)
                .await
                .map_err(|e| refuse(format!("cannot listen on {addr}: {e}")))?;
        }
    }
    let (op, profile) = check_backend(&args.backend, &args.backend_opts).await?;
    if args.tiers.is_empty() {
        return Ok(());
    }
    // As startup does: a tier that cannot be reached, or one another primary claimed (a new
    // backend root with the old tiers), would keep the server from starting. Claiming a tier
    // that has no owner yet is what the restart would do first anyway.
    match tokio::time::timeout(
        PROBE_TIMEOUT,
        crate::tier::connect(&op, &profile, &args.tiers),
    )
    .await
    {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(refuse(format!("{e:#}"))),
        Err(_) => Err(refuse(format!(
            "the tiers did not answer within {}s",
            PROBE_TIMEOUT.as_secs()
        ))),
    }
}

/// The file's key pairs as the edit lists them. A pair listed without a secret keeps the one it
/// has, so the page never needs to hold secrets it did not create.
fn key_pairs(
    current: &ServeArgs,
    pairs: Vec<KeyEdit>,
    signer: Option<&str>,
) -> S3Result<toml_edit::ArrayOfTables> {
    let secrets: HashMap<&str, &str> = current
        .credentials
        .iter()
        .map(|p| (p.access_key.as_str(), p.secret_key.as_str()))
        .collect();
    let expiries: HashMap<&str, Option<Expiry>> = current
        .credentials
        .iter()
        .map(|p| (p.access_key.as_str(), p.expires))
        .collect();
    let fixed = current.fixed_access_key.as_deref();
    let mut tables = toml_edit::ArrayOfTables::new();
    for pair in &pairs {
        let key = pair.access_key.as_str();
        if Some(key) == fixed {
            // Not the file's to list: it would become a duplicate of the environment's pair.
            continue;
        }
        if key.is_empty() || key.contains(|c: char| c.is_whitespace() || c == '/' || c == '=') {
            return Err(s3_error!(
                InvalidArgument,
                "access key {key:?}: use letters, digits and punctuation other than / and ="
            ));
        }
        let secret = match &pair.secret_key {
            Some(secret) if secret.chars().count() < 8 => {
                return Err(s3_error!(
                    InvalidArgument,
                    "the secret key of {key:?} must have at least 8 characters"
                ));
            }
            Some(secret) => secret.as_str(),
            None => secrets.get(key).copied().ok_or_else(|| {
                s3_error!(
                    InvalidArgument,
                    "access key {key:?} is new and needs a secret key"
                )
            })?,
        };
        let mut table = toml_edit::Table::new();
        table["access-key"] = toml_edit::value(key);
        table["secret-key"] = toml_edit::value(secret);
        // Limiting the pair the page signs with would take the settings from this session in
        // the middle of editing them, as removing it would; so would setting or moving its
        // expiry. An expiry it already has, set in the file, is sent back as it is and kept.
        if Some(key) == signer
            && (pair.allow.is_some()
                || pair.access != Access::ReadWrite
                || expiries.get(key).copied().flatten() != pair.expires)
        {
            return Err(s3_error!(
                InvalidArgument,
                "the console is signed in with {key:?}; sign in with another key to limit it"
            ));
        }
        if let Some(allow) = &pair.allow {
            let list: toml_edit::Array = allow
                .iter()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect();
            table["allow"] = toml_edit::value(list);
        }
        if pair.access != Access::ReadWrite {
            table["access"] = toml_edit::value(pair.access.as_str());
        }
        if let Some(expires) = pair.expires {
            table["expires"] = toml_edit::value(expires.to_string());
        }
        tables.push(table);
    }
    // Removing the pair the page signs with would end the session mid-edit, with no way back
    // in from the console; another session, or the file, can remove it.
    if let Some(signer) = signer
        && Some(signer) != fixed
        && !pairs.iter().any(|p| p.access_key == signer)
    {
        return Err(s3_error!(
            InvalidArgument,
            "the console is signed in with {signer:?}; sign in with another key to remove it"
        ));
    }
    Ok(tables)
}

/// The backend options as the edit lists them. An option listed without a value keeps the one
/// it has: the page is never sent the values that may be credentials, so it cannot send them
/// back.
fn backend_opts(current: &ServeArgs, opts: &[OptEdit]) -> S3Result<Vec<(String, String)>> {
    let mut out = Vec::new();
    for opt in opts {
        let name = opt.name.trim();
        if name.is_empty() || name.contains(|c: char| c.is_whitespace() || c == '=') {
            return Err(s3_error!(
                InvalidArgument,
                "backend option {name:?}: expected a name without spaces or ="
            ));
        }
        if out.iter().any(|(n, _): &(String, String)| n == name) {
            return Err(s3_error!(
                InvalidArgument,
                "backend option {name:?} is listed twice"
            ));
        }
        let value = match &opt.value {
            Some(v) => v.clone(),
            None => current
                .backend_opts
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, v)| v.clone())
                .ok_or_else(|| {
                    s3_error!(InvalidArgument, "backend option {name:?} needs a value")
                })?,
        };
        out.push((name.to_owned(), value));
    }
    Ok(out)
}

async fn check_backend(
    scheme: &str,
    opts: &[(String, String)],
) -> S3Result<(opendal::Operator, backend::Profile)> {
    let op = backend::build_operator(scheme, opts)
        .map_err(|e| s3_error!(InvalidArgument, "backend {scheme:?}: {e:#}"))?;
    match tokio::time::timeout(PROBE_TIMEOUT, backend::probe(&op)).await {
        Ok(Ok(profile)) => Ok((op, profile)),
        Ok(Err(e)) => Err(s3_error!(
            InvalidArgument,
            "backend {scheme:?} does not work: {e:#}"
        )),
        Err(_) => Err(s3_error!(
            InvalidArgument,
            "backend {scheme:?} did not answer within {}s",
            PROBE_TIMEOUT.as_secs()
        )),
    }
}

/// In place rather than by renaming a new file over it: a config file mounted into a container
/// on its own cannot be replaced, and the owner and mode stay as the operator set them.
fn write(path: &Path, text: &str) -> S3Result<()> {
    std::fs::write(path, text)
        .map_err(|e| s3_error!(InternalError, "cannot write {}: {e}", path.display()))
}

fn config_view(live: &Live) -> Value {
    let args = &live.current();
    let editable = match &args.config {
        None => Err("the server runs without a config file (--config)".to_owned()),
        Some(path) => std::fs::OpenOptions::new()
            .append(true)
            .open(path)
            .map(|_| ())
            .map_err(|e| format!("{} is not writable: {e}", path.display())),
    };
    let setting = |name: &str, value: Value| {
        let live = LIVE.contains(&name);
        let locked = args.locked.iter().any(|l| l == name);
        json!({
            "name": name,
            "value": value,
            "live": live,
            "locked": locked,
            "editable": !locked && editable.is_ok(),
        })
    };
    let path_value = |p: &Option<std::path::PathBuf>| {
        p.as_ref()
            .map_or(Value::Null, |p| Value::String(p.display().to_string()))
    };
    let settings = vec![
        setting("region", json!(args.region)),
        setting("domain", json!(args.domain)),
        setting(
            "etag-buffer-limit",
            json!(format_size(args.etag_buffer_limit)),
        ),
        setting(
            "stale-upload-age",
            json!(format_duration(args.stale_upload_age)),
        ),
        setting(
            "usage-scan-interval",
            json!(format_duration(args.usage_scan_interval)),
        ),
        setting("log-level", json!(args.log_level)),
        setting("metrics", json!(args.metrics.as_str())),
        // Never sent, as a secret key is not: the page says whether one is set, and an empty
        // field keeps it.
        {
            let mut s = setting("metrics-token", Value::Null);
            s["secret"] = json!(args.metrics_token.is_some());
            s
        },
        setting("backend", json!(args.backend)),
        setting("cache-size", json!(format_size(args.cache_size))),
        setting(
            "log-format",
            json!(match args.log_format {
                crate::config::LogFormat::Text => "text",
                crate::config::LogFormat::Json => "json",
            }),
        ),
        setting("address", json!(args.address.to_string())),
        setting(
            "console-address",
            args.console_address
                .map_or(Value::Null, |a| Value::String(a.to_string())),
        ),
        setting("tls-cert", path_value(&args.tls_cert)),
        setting("tls-key", path_value(&args.tls_key)),
        setting("log-file", path_value(&args.log_file)),
        setting("log-max-size", json!(format_size(args.log_max_size))),
        setting("log-max-files", json!(args.log_max_files)),
        // Without its password, which the page never needs: one sent back without it keeps it.
        {
            let url = args.log_forward.as_deref();
            let mut s = setting("log-forward", json!(url.map(|u| forward::redact(u, ""))));
            s["secret"] = json!(url.is_some_and(forward::has_password));
            s
        },
    ];
    let credentials: Vec<Value> = args
        .credentials
        .iter()
        .map(|p| {
            let locked = args.fixed_access_key.as_deref() == Some(p.access_key.as_str());
            json!({
                "access_key": p.access_key,
                "locked": locked,
                "allow": p.allow,
                "access": p.access.as_str(),
                "expires": p.expires.map(|e| e.to_string()),
            })
        })
        .collect();
    // Values that may be credentials are not sent: the page holds the signed-in secret, and
    // has no use for the upstream's.
    let backend_opts: Vec<Value> = args
        .backend_opts
        .iter()
        .map(|(name, value)| {
            let secret = is_secretish(name);
            json!({ "name": name, "value": (!secret).then_some(value), "secret": secret })
        })
        .collect();
    json!({
        "file": path_value(&args.config),
        "editable": editable.is_ok(),
        "reason": editable.err(),
        "settings": settings,
        "backend_opts": backend_opts,
        "backend_opts_locked": args.locked.iter().any(|l| l == "backend-opt"),
        "backends": backend::schemes(),
        "pending_restart": live.pending_restart(),
        "credentials": credentials,
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Edit {
    /// By long name; `null` removes the setting from the file, back to its default.
    #[serde(default)]
    settings: BTreeMap<String, Value>,
    /// The backend options, whole; absent leaves them as they are.
    backend_opts: Option<Vec<OptEdit>>,
    /// The file's key pairs, whole; absent leaves them as they are.
    credentials: Option<Vec<KeyEdit>>,
}

/// Refuses what it does not know, so a client that sends a setting this server lacks learns it
/// was not applied.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BucketEdit {
    public: Option<bool>,
    /// Absent leaves the quota as it is; `null` removes it.
    #[serde(default, deserialize_with = "present")]
    quota: Option<Option<QuotaEdit>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QuotaEdit {
    max_bytes: u64,
    max_objects: Option<u64>,
}

/// A field that is present, `null` included, as `Some`; with `#[serde(default)]` an absent one
/// is `None`.
fn present<'de, D, T>(d: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(d).map(Some)
}

/// The notification targets, without their secrets: whether one is set, and the URL without
/// user, password or query.
fn notify_json(dal: &crate::S3Dal) -> Value {
    let settings = dal.settings();
    let targets: Vec<Value> = settings
        .notify
        .iter()
        .map(|t| {
            let mut v = dal.metrics().notify_target(&t.id).json();
            v["id"] = json!(t.id);
            v["arn"] = json!(t.arn(&settings.region));
            v["url"] = json!(crate::notify::shown_url(&t.url));
            v["auth_token"] = json!(t.auth_token.is_some());
            v["hmac"] = json!(t.hmac_secret.is_some());
            v
        })
        .collect();
    json!(targets)
}

fn quota_json(quota: Option<&Quota>) -> Value {
    quota.map_or(
        Value::Null,
        |q| json!({ "max_bytes": q.max_bytes, "max_objects": q.max_objects }),
    )
}

fn usage_json(usage: BucketUsage) -> Value {
    let scanned = usage
        .scanned
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs());
    json!({
        "objects": usage.objects,
        "bytes": usage.bytes,
        "cold_bytes": usage.cold_bytes,
        "scanned": scanned,
    })
}

/// The cold tiers for the server page: what each is, and what its last sweep here found (`null`
/// until one ran: the daily sweep runs on the instance holding its lease).
fn tiers_json(dal: &crate::S3Dal) -> Value {
    dal.tiers()
        .iter()
        .map(|t| {
            let swept = dal.metrics().tier(t.name()).last_sweep();
            json!({
                "name": t.name(),
                "storage_class": t.storage_class(),
                "backend": t.scheme(),
                "profile": t.profile(),
                "swept": swept.map(|(objects, bytes, dangling)| {
                    json!({ "objects": objects, "bytes": bytes, "dangling": dangling })
                }),
            })
        })
        .collect()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OptEdit {
    name: String,
    /// `None` keeps the option's current value.
    value: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyEdit {
    access_key: String,
    secret_key: Option<String>,
    /// Required, with `allow`, so a client that lists the pairs without their limits fails
    /// rather than lifting them.
    access: Access,
    allow: Option<Vec<String>>,
    /// Required too, `null` for none: a client that lists the pairs without their expiries
    /// would otherwise make them last forever. A deserializer of its own is what makes serde
    /// require an `Option` field.
    #[serde(deserialize_with = "Option::deserialize")]
    expires: Option<Expiry>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn current(opts: &[(&str, &str)]) -> ServeArgs {
        let mut args = crate::config::Cli::serve_from([
            "s3dal",
            "serve",
            "--backend=fs",
            "--access-key=a",
            "--secret-key=bbbbbbbb",
        ])
        .unwrap();
        args.backend_opts = opts
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        args
    }

    fn opt(name: &str, value: Option<&str>) -> OptEdit {
        OptEdit {
            name: name.into(),
            value: value.map(Into::into),
        }
    }

    #[test]
    fn an_option_listed_without_a_value_keeps_its_secret() {
        // The page is never sent upstream credentials, so saving the other options must not
        // blank them.
        let args = current(&[("bucket", "up"), ("secret_access_key", "UPSTREAM")]);
        let out = backend_opts(
            &args,
            &[opt("bucket", Some("down")), opt("secret_access_key", None)],
        )
        .unwrap();
        assert_eq!(
            out,
            [
                ("bucket".to_owned(), "down".to_owned()),
                ("secret_access_key".to_owned(), "UPSTREAM".to_owned())
            ]
        );
        // A new option has no value to keep, and one name twice is ambiguous.
        assert!(backend_opts(&args, &[opt("endpoint", None)]).is_err());
        assert!(backend_opts(&args, &[opt("root", Some("/a")), opt("root", Some("/b"))]).is_err());
    }

    #[tokio::test]
    async fn a_backend_is_probed_before_it_is_saved() {
        // A backend that cannot start would be found only by the restart, which it would then
        // keep from starting.
        assert!(check_backend("no-such-service", &[]).await.is_err());
        let root = tempfile::tempdir().unwrap();
        let opts = [("root".to_owned(), root.path().display().to_string())];
        check_backend("fs", &opts).await.unwrap();
    }

    #[tokio::test]
    async fn a_restart_that_a_tier_would_stop_is_refused() {
        // A new backend root with the old tier: the tier belongs to the first primary, so the
        // start would fail and leave nothing serving, this console included, to set it right.
        let (first, second, tier) = (
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
        );
        let root = |d: &tempfile::TempDir| d.path().display().to_string();
        let mut args = current(&[("root", &root(&first))]);
        args.tiers = vec![crate::config::TierConfig {
            name: "hdd".into(),
            storage_class: "STANDARD_IA".into(),
            backend: "fs".into(),
            backend_opts: vec![("root".into(), root(&tier))],
        }];
        preflight(&args, &args).await.unwrap();
        args.backend_opts = vec![("root".into(), root(&second))];
        let err = preflight(&args, &args).await.unwrap_err();
        assert!(
            format!("{err:?}").contains("belongs to another s3dal"),
            "{err:?}"
        );
    }
}

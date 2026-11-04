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
use crate::access::Access;
use crate::backend;
use crate::config::{ServeArgs, format_duration, format_size, is_secretish};
use crate::live::Live;

const PREFIX: &str = "/_s3dal/admin/";

/// The settings a reload applies at once.
const LIVE: [&str; 5] = [
    "region",
    "domain",
    "etag-buffer-limit",
    "stale-upload-age",
    "log-level",
];

/// The settings that take a restart, which the console cannot do: an edit is written to the
/// file, and the server and settings pages say what waits for the restart.
const RESTART: [&str; 9] = [
    "backend",
    "address",
    "console-address",
    "tls-cert",
    "tls-key",
    "log-format",
    "log-file",
    "log-max-size",
    "log-max-files",
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
        extensions.get::<ConsoleRequest>().is_some() && uri.path().starts_with(PREFIX)
    }

    /// Any valid signature may ask who it is, which is how the console signs in; the rest is
    /// for key pairs without limits, since the settings reach past any one bucket.
    async fn check_access(&self, req: &mut S3Request<Body>) -> S3Result<()> {
        if req.credentials.is_none() {
            return Err(s3_error!(AccessDenied, "Signature is required"));
        }
        let whoami = req.uri.path().strip_prefix(PREFIX) == Some("whoami");
        if !whoami && self.live.auth().limit(req.credentials.as_ref()).is_some() {
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
            (&Method::GET, "whoami") => self.whoami(&req),
            (&Method::GET, "info") => self.info(),
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
        // the middle of editing them, as removing it would.
        if Some(key) == signer && (pair.allow.is_some() || pair.access != Access::ReadWrite) {
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

async fn check_backend(scheme: &str, opts: &[(String, String)]) -> S3Result<()> {
    let op = backend::build_operator(scheme, opts)
        .map_err(|e| s3_error!(InvalidArgument, "backend {scheme:?}: {e:#}"))?;
    match tokio::time::timeout(PROBE_TIMEOUT, backend::probe(&op)).await {
        Ok(Ok(_)) => Ok(()),
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
        setting("log-level", json!(args.log_level)),
        setting("backend", json!(args.backend)),
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
}

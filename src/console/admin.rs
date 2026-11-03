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
use crate::config::{ServeArgs, format_duration, format_size};
use crate::live::Live;

const PREFIX: &str = "/_s3dal/admin/";

/// The settings a reload applies, and so the only ones the console edits. The rest take a
/// restart, which the console cannot do, and are shown read-only.
const LIVE: [&str; 4] = ["region", "domain", "etag-buffer-limit", "stale-upload-age"];

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

    // The default `check_access` requires a valid signature, which is all there is to check:
    // every key pair has full access.

    async fn call(&self, mut req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        let name = req.uri.path().strip_prefix(PREFIX).unwrap_or_default();
        let doc = match (&req.method, name) {
            (&Method::GET, "info") => self.info(),
            (&Method::GET, "config") => config_view(&self.live.current()),
            (&Method::PUT, "config") => {
                let body =
                    req.input.store_all_limited(MAX_EDIT).await.map_err(|_| {
                        s3_error!(EntityTooLarge, "a settings edit is at most 64 KiB")
                    })?;
                let edit: Edit =
                    serde_json::from_slice(&body).map_err(|e| s3_error!(InvalidArgument, "{e}"))?;
                let signer = req.credentials.as_ref().map(|c| c.access_key.as_str());
                self.apply(edit, signer)?;
                config_view(&self.live.current())
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
    fn apply(&self, edit: Edit, signer: Option<&str>) -> S3Result<()> {
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

        for (name, value) in edit.settings {
            if !LIVE.contains(&name.as_str()) {
                return Err(s3_error!(
                    InvalidArgument,
                    "`{name}` cannot be changed here"
                ));
            }
            if current.locked.contains(&name) {
                return Err(s3_error!(
                    InvalidArgument,
                    "`{name}` is set by a flag or environment variable, which outranks the file"
                ));
            }
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

/// In place rather than by renaming a new file over it: a config file mounted into a container
/// on its own cannot be replaced, and the owner and mode stay as the operator set them.
fn write(path: &Path, text: &str) -> S3Result<()> {
    std::fs::write(path, text)
        .map_err(|e| s3_error!(InternalError, "cannot write {}: {e}", path.display()))
}

fn config_view(args: &ServeArgs) -> Value {
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
            "editable": live && !locked && editable.is_ok(),
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
        setting("backend", json!(args.backend)),
        setting("address", json!(args.address.to_string())),
        setting(
            "console-address",
            args.console_address
                .map_or(Value::Null, |a| Value::String(a.to_string())),
        ),
        setting("tls-cert", path_value(&args.tls_cert)),
        setting("tls-key", path_value(&args.tls_key)),
    ];
    let credentials: Vec<Value> = args
        .credentials
        .iter()
        .map(|p| {
            let locked = args.fixed_access_key.as_deref() == Some(p.access_key.as_str());
            json!({ "access_key": p.access_key, "locked": locked })
        })
        .collect();
    json!({
        "file": path_value(&args.config),
        "editable": editable.is_ok(),
        "reason": editable.err(),
        "settings": settings,
        "credentials": credentials,
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Edit {
    /// By long name; `null` removes the setting from the file, back to its default.
    #[serde(default)]
    settings: BTreeMap<String, Value>,
    /// The file's key pairs, whole; absent leaves them as they are.
    credentials: Option<Vec<KeyEdit>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyEdit {
    access_key: String,
    secret_key: Option<String>,
}

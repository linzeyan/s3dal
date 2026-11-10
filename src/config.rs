use std::collections::HashSet;
use std::ffi::OsString;
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::error::ErrorKind;
use clap::parser::ValueSource;
use clap::{ArgAction, Args, CommandFactory, FromArgMatches, Parser, Subcommand};
use serde::Deserialize;

use crate::access::{Access, Limit};
use crate::logging::forward;

#[derive(Debug, Parser)]
#[command(
    name = "s3dal",
    version,
    about = "Expose any Apache OpenDAL backend as an S3-compatible endpoint",
    disable_version_flag = true
)]
pub struct Cli {
    // clap's own flag is `-V` alone; `-v` is what most tools take, and typing it got an error.
    /// Print version
    #[arg(short = 'v', short_alias = 'V', long, action = ArgAction::Version)]
    version: Option<bool>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Serve the S3 API on top of an OpenDAL backend.
    Serve(ServeArgs),
    // The first paragraph of each comment is the one line `--help` lists the command with; the
    // rest shows only in the command's own help.
    /// Check `serve`'s settings and print them with secrets redacted.
    ///
    /// Reads the same flags, variables and config file as `serve`.
    CheckConfig(ServeArgs),
    /// Exit 0 if the server answers its health check, 1 if not.
    ///
    /// Without `--url`, asks the server that `serve` would start with the same variables and
    /// config file, so a container's health check needs no arguments.
    Healthcheck(HealthcheckArgs),
    /// Print a new random key pair, in the config file's `[[credentials]]` form.
    Keygen,
    #[cfg(feature = "client")]
    #[command(flatten)]
    Client(crate::client::ClientCommand),
}

/// The subcommands that take `serve`'s settings, config file included.
const SERVE_LIKE: [&str; 2] = ["serve", "check-config"];

#[derive(Debug, Clone, Args)]
pub struct HealthcheckArgs {
    /// Base URL of the server, for example `https://127.0.0.1:9000`. Its certificate is not
    /// verified: the check asks whether the process answers, and sends no secret.
    #[arg(long)]
    pub url: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum LogFormat {
    Text,
    /// One JSON object per line, for log collectors.
    Json,
}

#[derive(Debug, Clone, Args)]
pub struct ServeArgs {
    /// TOML file with `serve` settings. Its keys are the long flag names below; `backend-opt`
    /// is a table, and key pairs are `[[credentials]]` tables with `access-key` and
    /// `secret-key`. Flags and environment variables override it.
    #[arg(long, env = "S3DAL_CONFIG")]
    pub config: Option<PathBuf>,

    /// OpenDAL service scheme, for example `fs`.
    #[arg(long, env = "S3DAL_BACKEND")]
    pub backend: String,

    /// OpenDAL service option as `key=value`; repeatable. Both the flag and the variable split
    /// on commas, so a value with a comma can only come from the config file's table.
    // Hidden from `--help` as the secret key is: the options carry upstream credentials.
    #[arg(long = "backend-opt", env = "S3DAL_BACKEND_OPTS", hide_env_values = true, value_delimiter = ',', value_parser = parse_key_value)]
    pub backend_opts: Vec<(String, String)>,

    #[arg(long, env = "S3DAL_ADDRESS", default_value = "127.0.0.1:9000")]
    pub address: SocketAddr,

    /// One key pair, in addition to the config file's `[[credentials]]`.
    // Keys are mandatory: an open-source gateway must not default to anonymous access.
    #[arg(long, env = "S3DAL_ACCESS_KEY")]
    access_key: Option<String>,

    #[arg(long, env = "S3DAL_SECRET_KEY", hide_env_values = true)]
    secret_key: Option<String>,

    /// Every key pair that may sign requests: the flags' or variables' first, then the config
    /// file's.
    #[arg(skip)]
    pub credentials: Vec<KeyPair>,

    /// The access key of the pair from the flags or variables, which editing the config file
    /// cannot change or remove.
    #[arg(skip)]
    pub fixed_access_key: Option<String>,

    #[arg(long, env = "S3DAL_REGION", default_value = "us-east-1")]
    pub region: String,

    /// Base domain for virtual-hosted-style requests; repeatable.
    #[arg(long, env = "S3DAL_DOMAIN", value_delimiter = ',')]
    pub domain: Vec<String>,

    /// Largest single PUT body buffered in memory to compute an MD5 ETag on backends
    /// without native ETags. Accepts a byte count or a `KiB` / `MiB` / `GiB` suffix.
    #[arg(long, env = "S3DAL_ETAG_BUFFER_LIMIT", default_value = "8MiB", value_parser = parse_size)]
    pub etag_buffer_limit: u64,

    /// Remove multipart uploads initiated, and records of uploads completed, longer ago than
    /// this, checked periodically in the background. A number with an `s` / `m` / `h` / `d`
    /// suffix; `0` disables the sweep.
    #[arg(long, env = "S3DAL_STALE_UPLOAD_AGE", default_value = "24h", value_parser = parse_duration)]
    pub stale_upload_age: Duration,

    /// PEM certificate chain. With `--tls-key`, the server speaks HTTPS only.
    #[arg(long, env = "S3DAL_TLS_CERT")]
    pub tls_cert: Option<PathBuf>,

    /// PEM private key of `--tls-cert`.
    #[arg(long, env = "S3DAL_TLS_KEY")]
    pub tls_key: Option<PathBuf>,

    /// Log line format.
    #[arg(long, env = "S3DAL_LOG_FORMAT", value_enum, default_value = "text")]
    pub log_format: LogFormat,

    /// Log levels: a level (`trace`, `debug`, `info`, `warn`, `error`, `off`), then any
    /// `target=level`, comma-separated, as in `info,s3dal=debug`. s3s's own lines stay off unless
    /// a directive names `s3s`.
    #[arg(long, env = "S3DAL_LOG_LEVEL", default_value = "info", value_parser = parse_log_level)]
    pub log_level: String,

    /// File the log is written to, instead of stderr.
    #[arg(long, env = "S3DAL_LOG_FILE")]
    pub log_file: Option<PathBuf>,

    /// Size past which `--log-file` is renamed to `<file>.1` and a new one started. Accepts a
    /// byte count or a `KiB` / `MiB` / `GiB` suffix; `0` never rotates.
    #[arg(long, env = "S3DAL_LOG_MAX_SIZE", default_value = "100MiB", value_parser = parse_size)]
    pub log_max_size: u64,

    /// Rotated log files kept, `<file>.1` the newest; older ones are removed.
    #[arg(long, env = "S3DAL_LOG_MAX_FILES", default_value = "5")]
    pub log_max_files: u32,

    /// Also send the log to a collector: `gelf+udp://host:12201`, `gelf+tcp://...` or
    /// `gelf+https://host/gelf` for Graylog; `elasticsearch+https://user:password@host:9200/index`
    /// for Elasticsearch and OpenSearch; `syslog+udp://host:514`, `syslog+tcp://...` or
    /// `syslog+tls://host:6514`; `otlp+https://host:4318` for OpenTelemetry;
    /// `splunk+https://:TOKEN@host:8088` for Splunk HEC; `ndjson+https://host/path` for any HTTP
    /// input.
    // Hidden from `--help` as the secret key is: the URL may carry a password.
    #[arg(long, env = "S3DAL_LOG_FORWARD", hide_env_values = true, value_parser = parse_log_forward)]
    pub log_forward: Option<String>,

    /// Buckets whose objects anyone may download without a key pair; repeatable, or
    /// comma-separated. Listing, uploading and deleting still need one.
    #[arg(long, env = "S3DAL_PUBLIC_BUCKETS", value_delimiter = ',')]
    pub public_bucket: Vec<String>,

    /// Address and port of the web console; without it there is none. It speaks HTTPS when the
    /// S3 port does, with the same certificate.
    #[arg(long, env = "S3DAL_CONSOLE_ADDRESS")]
    pub console_address: Option<SocketAddr>,

    /// The long names of the settings given by a flag or a variable, which outrank the config
    /// file, so editing the file cannot change them.
    #[arg(skip)]
    pub locked: Vec<String>,
}

impl ServeArgs {
    pub fn settings(&self) -> Settings {
        Settings {
            region: self.region.clone(),
            etag_buffer_limit: self.etag_buffer_limit,
            stale_upload_age: Some(self.stale_upload_age).filter(|age| !age.is_zero()),
        }
    }

    /// The settings in the config file's syntax, with secret keys and every backend option
    /// that may be a credential redacted, so the output can be pasted into an issue.
    pub fn describe(&self) -> String {
        const REDACTED: &str = "\"<redacted>\"";
        let path =
            |p: &Option<PathBuf>| p.as_ref().map(|p| format!("{:?}", p.display().to_string()));
        let mut out = vec![
            format!("backend = {:?}", self.backend),
            format!("address = \"{}\"", self.address),
            format!("region = {:?}", self.region),
            format!("domain = {:?}", self.domain),
            format!("etag-buffer-limit = {}", self.etag_buffer_limit),
            format!(
                "stale-upload-age = \"{}s\"",
                self.stale_upload_age.as_secs()
            ),
        ];
        out.extend(path(&self.tls_cert).map(|p| format!("tls-cert = {p}")));
        out.extend(path(&self.tls_key).map(|p| format!("tls-key = {p}")));
        let format = match self.log_format {
            LogFormat::Text => "text",
            LogFormat::Json => "json",
        };
        out.push(format!("log-format = \"{format}\""));
        out.push(format!("log-level = {:?}", self.log_level));
        out.extend(path(&self.log_file).map(|p| format!("log-file = {p}")));
        out.push(format!(
            "log-max-size = \"{}\"",
            format_size(self.log_max_size)
        ));
        out.push(format!("log-max-files = {}", self.log_max_files));
        out.extend(
            self.log_forward
                .as_deref()
                .map(|url| format!("log-forward = {:?}", forward::redact(url, "<redacted>"))),
        );
        if !self.public_bucket.is_empty() {
            out.push(format!("public-bucket = {:?}", self.public_bucket));
        }
        out.extend(
            self.console_address
                .map(|a| format!("console-address = \"{a}\"")),
        );
        out.push("\n[backend-opt]".into());
        for (name, value) in &self.backend_opts {
            if is_secretish(name) {
                out.push(format!("{name} = {REDACTED}"));
            } else {
                out.push(format!("{name} = {value:?}"));
            }
        }
        for pair in &self.credentials {
            out.push(format!(
                "\n[[credentials]]\naccess-key = {:?}\nsecret-key = {REDACTED}",
                pair.access_key
            ));
            if let Some(allow) = &pair.allow {
                out.push(format!("allow = {allow:?}"));
            }
            if pair.access != Access::ReadWrite {
                out.push(format!("access = \"{}\"", pair.access.as_str()));
            }
        }
        out.join("\n") + "\n"
    }

    /// Takes what the file holds in shapes no flag has, then applies the rules that span
    /// several settings.
    fn finish(&mut self, file: FileOnly) -> Result<(), String> {
        // A flag or variable replaces the file's options whole, as for every list setting:
        // merging key by key would make an option's source hard to tell.
        if self.backend_opts.is_empty() {
            self.backend_opts = file.backend_opts;
        }
        // Added to the file's pairs rather than replacing them: a deployment keeps one pair in
        // its environment, which always works, and manages the others in the file, from the
        // console or by hand, without a restart.
        self.credentials = match (self.access_key.take(), self.secret_key.take()) {
            (Some(access_key), Some(secret_key)) => {
                self.fixed_access_key = Some(access_key.clone());
                // Without limits: it is the pair that always works, whatever the file says.
                vec![KeyPair::new(access_key, secret_key)]
            }
            (None, None) => Vec::new(),
            _ => return Err("--access-key and --secret-key must be given together".into()),
        };
        self.credentials.extend(file.credentials);
        check_key_pairs(&self.credentials)?;
        if self.tls_cert.is_some() != self.tls_key.is_some() {
            return Err("--tls-cert and --tls-key must be given together".into());
        }
        // Whole buckets only: a folder made public would need its own rule for listings and
        // for keys that merely start like it, which is what `allow` already does for key pairs.
        for name in &self.public_bucket {
            if name.is_empty() || name.contains('/') {
                return Err(format!(
                    "public bucket {name:?}: expected a bucket name, without a folder"
                ));
            }
        }
        Ok(())
    }
}

/// One SigV4 credential. Each pair signs with its own secret, and reaches every bucket unless
/// `allow` or `access` limits it (see [`crate::access`]).
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct KeyPair {
    pub access_key: String,
    pub secret_key: String,
    /// Buckets, or folders as `bucket/folder/`, the pair is limited to.
    #[serde(default)]
    pub allow: Option<Vec<String>>,
    #[serde(default)]
    pub access: Access,
}

impl KeyPair {
    /// A pair without limits.
    pub fn new(access_key: impl Into<String>, secret_key: impl Into<String>) -> Self {
        Self {
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            allow: None,
            access: Access::ReadWrite,
        }
    }

    pub fn limit(&self) -> Result<Option<Limit>, String> {
        Limit::of(self.allow.as_deref(), self.access)
            .map_err(|e| format!("access key {:?}: {e}", self.access_key))
    }
}

// Written by hand so that printing a configuration never prints a secret.
impl fmt::Debug for KeyPair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyPair")
            .field("access_key", &self.access_key)
            .field("secret_key", &"<redacted>")
            .field("allow", &self.allow)
            .field("access", &self.access)
            .finish()
    }
}

/// Backend option names that may hold a credential, whose values are never shown.
pub(crate) fn is_secretish(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    ["key", "secret", "token", "password", "credential", "sas"]
        .iter()
        .any(|s| lower.contains(s))
}

/// There is no anonymous mode. Two secrets for one access key would leave which of them
/// verifies a signature to lookup order, so that is refused rather than resolved.
pub(crate) fn check_key_pairs(pairs: &[KeyPair]) -> Result<(), String> {
    for pair in pairs {
        pair.limit()?;
    }
    if pairs.is_empty() {
        return Err(
            "no key pair: give --access-key and --secret-key, or [[credentials]] in the config \
             file (there is no anonymous access)"
                .into(),
        );
    }
    let mut seen = HashSet::new();
    for pair in pairs {
        if !seen.insert(pair.access_key.as_str()) {
            return Err(format!(
                "access key {:?} is configured more than once",
                pair.access_key
            ));
        }
    }
    Ok(())
}

impl Cli {
    /// Parses the command line as [`Parser::parse`] does, with the `serve --config` file read
    /// in underneath flags and environment variables. Exits on bad input.
    pub fn load() -> Self {
        Self::load_from(Self::command(), std::env::args_os()).unwrap_or_else(|err| err.exit())
    }

    /// `cmd` is [`Cli::command`]; tests pass one that ignores the environment.
    fn load_from(
        mut cmd: clap::Command,
        args: impl IntoIterator<Item = impl Into<OsString>>,
    ) -> Result<Self, clap::Error> {
        let args: Vec<OsString> = args.into_iter().map(Into::into).collect();
        // A forgiving first pass only looks for the file: the settings in it may be what makes
        // the command line complete (`--backend` is required).
        let first = cmd
            .clone()
            .ignore_errors(true)
            .try_get_matches_from(&args)?;
        let sub = first
            .subcommand_name()
            .filter(|name| SERVE_LIKE.contains(name))
            .unwrap_or("serve")
            .to_owned();
        let path = first
            .subcommand_matches(&sub)
            .and_then(|m| m.get_one::<PathBuf>("config"))
            .cloned();
        let file = match path {
            Some(path) => {
                let serve = cmd.find_subcommand(&sub).expect("defined above").clone();
                let (serve, file) = read_file(serve, &path)
                    .map_err(|(kind, msg)| serve_error(&cmd, &sub, kind, msg))?;
                cmd = cmd.mut_subcommand(&sub, |_| serve);
                file
            }
            None => FileOnly::default(),
        };
        let matches = cmd.clone().try_get_matches_from(&args)?;
        let mut cli = Self::from_arg_matches(&matches)?;
        if let Command::Serve(serve) | Command::CheckConfig(serve) = &mut cli.command {
            // File values are defaults to clap, so a value from anywhere else outranks the file.
            if let (Some(m), Some(def)) =
                (matches.subcommand_matches(&sub), cmd.find_subcommand(&sub))
            {
                serve.locked = def
                    .get_arguments()
                    .filter(|arg| {
                        matches!(
                            m.value_source(arg.get_id().as_str()),
                            Some(ValueSource::CommandLine | ValueSource::EnvVariable)
                        )
                    })
                    .filter_map(|arg| arg.get_long().map(str::to_owned))
                    .collect();
            }
            serve
                .finish(file)
                .map_err(|msg| serve_error(&cmd, &sub, ErrorKind::ArgumentConflict, msg))?;
        }
        Ok(cli)
    }

    /// `serve`'s settings from the environment and its config file alone, as a server started
    /// without flags would have them.
    pub fn serve_from_env() -> Result<ServeArgs, clap::Error> {
        Self::serve_from(["s3dal", "serve"])
    }

    /// `serve`'s settings from a command line, read again as at startup: a config reload
    /// passes the process's own arguments, so flags, variables and the file keep their order
    /// of precedence.
    pub fn serve_from(
        args: impl IntoIterator<Item = impl Into<OsString>>,
    ) -> Result<ServeArgs, clap::Error> {
        let cli = Self::load_from(Self::command(), args)?;
        let Command::Serve(serve) = cli.command else {
            return Err(clap::Error::raw(
                ErrorKind::InvalidSubcommand,
                "not `serve`",
            ));
        };
        Ok(serve)
    }
}

/// An error shown with the subcommand's usage line, like the errors clap reports itself.
fn serve_error(cmd: &clap::Command, sub: &str, kind: ErrorKind, msg: String) -> clap::Error {
    let mut cmd = cmd.clone();
    cmd.build();
    let serve = cmd.find_subcommand_mut(sub).expect("defined above");
    serve.error(kind, msg)
}

/// File settings that are not a flag's value: the backend options as a table, so a value may
/// contain a comma, and any number of key pairs.
#[derive(Default)]
struct FileOnly {
    backend_opts: Vec<(String, String)>,
    credentials: Vec<KeyPair>,
}

/// Every other key becomes the default of the flag with the same long name. clap then parses
/// it exactly as the flag (sizes, addresses, repeatable values), a flag or environment variable
/// still wins, and a flag added to `serve` gets its file key with no further code.
fn read_file(
    mut serve: clap::Command,
    path: &Path,
) -> Result<(clap::Command, FileOnly), (ErrorKind, String)> {
    let at = |msg: String| format!("config file {}: {}", path.display(), msg.trim_end());
    let text = std::fs::read_to_string(path).map_err(|e| (ErrorKind::Io, at(e.to_string())))?;
    let table: toml::Table = text
        .parse()
        .map_err(|e: toml::de::Error| (ErrorKind::InvalidValue, at(e.to_string())))?;
    let invalid = |key: &str, msg: String| (ErrorKind::InvalidValue, at(format!("`{key}`: {msg}")));
    let mut file = FileOnly::default();
    for (key, value) in table {
        match key.as_str() {
            "credentials" => {
                file.credentials = value.try_into().map_err(|e: toml::de::Error| {
                    invalid(&key, format!("expected [[credentials]] tables: {e}"))
                })?;
            }
            "backend-opt" => {
                let toml::Value::Table(opts) = value else {
                    return Err(invalid(&key, "expected a table".into()));
                };
                for (name, value) in opts {
                    let value = flag_value(value).map_err(|msg| invalid(&key, msg))?;
                    file.backend_opts.push((name, value));
                }
            }
            "access-key" | "secret-key" => {
                return Err((
                    ErrorKind::UnknownArgument,
                    at(format!(
                        "unknown key `{key}`; key pairs go in [[credentials]]"
                    )),
                ));
            }
            _ => {
                let arg = serve
                    .get_arguments()
                    .find(|arg| arg.get_long() == Some(key.as_str()) && arg.get_id() != "config")
                    .ok_or_else(|| {
                        (
                            ErrorKind::UnknownArgument,
                            at(format!("unknown key `{key}`")),
                        )
                    })?;
                let id = arg.get_id().clone();
                let values = match value {
                    // A repeatable flag takes an array, as if it were given once per item.
                    toml::Value::Array(items) if matches!(arg.get_action(), ArgAction::Append) => {
                        items.into_iter().map(flag_value).collect()
                    }
                    value => flag_value(value).map(|v| vec![v]),
                }
                .map_err(|msg| invalid(&key, msg))?;
                // Parsed now, by the flag's own parser, because a bad default fails later with
                // an error that names only the flag, as if it had been typed after it.
                let alone = clap::Command::new("s3dal").no_binary_name(true);
                let alone = alone.arg(arg.clone().required(false));
                for value in &values {
                    alone
                        .clone()
                        .try_get_matches_from([format!("--{key}"), value.clone()])
                        .map_err(|err| {
                            let msg = err.to_string();
                            let first = msg.lines().next().unwrap_or_default();
                            (
                                err.kind(),
                                at(format!("`{key}`: {}", first.trim_start_matches("error: "))),
                            )
                        })?;
                }
                serve = serve.mut_arg(id, |arg| arg.default_values(values).required(false));
            }
        }
    }
    Ok((serve, file))
}

/// A value as it would be typed after the flag.
fn flag_value(value: toml::Value) -> Result<String, String> {
    match value {
        toml::Value::String(s) => Ok(s),
        toml::Value::Integer(n) => Ok(n.to_string()),
        toml::Value::Float(n) => Ok(n.to_string()),
        toml::Value::Boolean(b) => Ok(b.to_string()),
        other => Err(format!(
            "expected a string, number or boolean, found {}",
            other.type_str()
        )),
    }
}

/// Runtime behavior knobs, separate from the CLI so embedders and tests need no clap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    pub region: String,
    pub etag_buffer_limit: u64,
    /// The age the binary passes to [`crate::S3Dal::sweep_stale_uploads`]; `None` never sweeps.
    pub stale_upload_age: Option<Duration>,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            region: "us-east-1".to_owned(),
            // Matches the aws CLI multipart threshold, so CLI uploads always get an MD5 ETag.
            etag_buffer_limit: 8 * 1024 * 1024,
            // MinIO's default. AWS keeps uploads until a lifecycle rule aborts them, but s3dal
            // has no lifecycle rules, so without a default abandoned parts would fill the backend
            // for good; a day outlasts any upload a client still means to finish, and keeps a
            // completed record long enough to answer any retry of its Complete.
            stale_upload_age: Some(Duration::from_secs(24 * 60 * 60)),
        }
    }
}

fn parse_key_value(s: &str) -> Result<(String, String), String> {
    let (k, v) = s
        .split_once('=')
        .ok_or_else(|| format!("expected key=value, found {s:?}"))?;
    if k.is_empty() {
        return Err(format!("empty key in {s:?}"));
    }
    Ok((k.to_owned(), v.to_owned()))
}

/// A size as `--etag-buffer-limit` takes it, in the largest unit that divides it.
pub fn format_size(n: u64) -> String {
    for (unit, size) in [("GiB", 1 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)] {
        if n != 0 && n.is_multiple_of(size) {
            return format!("{}{unit}", n / size);
        }
    }
    n.to_string()
}

/// A duration as `--stale-upload-age` takes it, in the largest unit that divides it.
pub fn format_duration(d: Duration) -> String {
    let s = d.as_secs();
    if s == 0 {
        return "0".into();
    }
    for (unit, secs) in [("d", 86_400), ("h", 3_600), ("m", 60)] {
        if s.is_multiple_of(secs) {
            return format!("{}{unit}", s / secs);
        }
    }
    format!("{s}s")
}

fn parse_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let n: u64 = num.parse().map_err(|_| format!("invalid size {s:?}"))?;
    let mult = match unit.trim() {
        "" | "B" => 1,
        "KiB" => 1 << 10,
        "MiB" => 1 << 20,
        "GiB" => 1 << 30,
        other => {
            return Err(format!(
                "unknown size unit {other:?}; use B, KiB, MiB or GiB"
            ));
        }
    };
    n.checked_mul(mult)
        .ok_or_else(|| format!("size {s:?} overflows"))
}

/// Checked when the settings are read, at startup and on every reload, so a bad level fails
/// there with the file named rather than when it is applied.
fn parse_log_level(s: &str) -> Result<String, String> {
    crate::logging::filter(s).map(|_| s.to_owned())
}

/// Checked when read, as the level is, so a URL with a typo stops the server at startup or
/// fails the reload, rather than leaving the log unsent.
fn parse_log_forward(s: &str) -> Result<String, String> {
    forward::Target::parse(s).map(|_| s.to_owned())
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let n: u64 = num.parse().map_err(|_| format!("invalid duration {s:?}"))?;
    let mult = match unit.trim() {
        "s" => 1,
        "m" => 60,
        "h" => 60 * 60,
        "d" => 24 * 60 * 60,
        // A bare number has no unit to get wrong only when it is zero.
        "" if n == 0 => 1,
        other => {
            return Err(format!("unknown duration unit {other:?}; use s, m, h or d"));
        }
    };
    n.checked_mul(mult)
        .map(Duration::from_secs)
        .ok_or_else(|| format!("duration {s:?} overflows"))
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    /// `s3dal serve` with `args`, blind to the environment: `scripts/with-rustfs.sh` exports
    /// `S3DAL_BACKEND` and `S3DAL_BACKEND_OPTS` for the whole test run.
    fn serve(args: &[&str]) -> Result<ServeArgs, clap::Error> {
        let cmd = Cli::command().mut_subcommand("serve", |serve| {
            serve.mut_args(|arg| arg.env(None::<&'static str>))
        });
        let argv = ["s3dal", "serve"].iter().chain(args).copied();
        let Command::Serve(serve) = Cli::load_from(cmd, argv)?.command else {
            unreachable!("parsed as serve")
        };
        Ok(serve)
    }

    fn config_file(text: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(text.as_bytes()).unwrap();
        file
    }

    fn pair(access_key: &str, secret_key: &str) -> KeyPair {
        KeyPair::new(access_key, secret_key)
    }

    #[test]
    fn key_pair_limits_come_from_the_file_and_bad_ones_refuse_to_start() {
        // A limit misread would hand a pair more than the operator meant; refusing the file
        // is the only safe answer to one that cannot be read.
        let file = config_file(
            "backend = \"fs\"\n\
             [[credentials]]\naccess-key = \"viewer\"\nsecret-key = \"viewer-secret\"\n\
             allow = [\"photos\", \"logs/app/\"]\naccess = \"read-only\"\n",
        );
        let args = serve(&["--config", file.path().to_str().unwrap()]).unwrap();
        let viewer = &args.credentials[0];
        assert_eq!(viewer.access, Access::ReadOnly);
        let limit = viewer.limit().unwrap().unwrap();
        assert!(limit.reaches_key("logs", "app/x.log") && !limit.reaches_key("logs", "db/x.log"));
        assert!(!limit.may_write());
        assert!(args.describe().contains("access = \"read-only\""));

        for bad in ["allow = []", "access = \"write-only\"", "allow = [\"/x\"]"] {
            let file = config_file(&format!(
                "backend = \"fs\"\n[[credentials]]\naccess-key = \"a\"\nsecret-key = \"b\"\n{bad}\n"
            ));
            assert!(
                serve(&["--config", file.path().to_str().unwrap()]).is_err(),
                "{bad}"
            );
        }
    }

    const FULL: &str = r#"
        backend = "fs"
        address = "0.0.0.0:9100"
        region = "eu-west-1"
        domain = ["s3.example.com", "example.org"]
        etag-buffer-limit = "16MiB"
        stale-upload-age = "7d"
        tls-cert = "/etc/s3dal/cert.pem"
        tls-key = "/etc/s3dal/key.pem"

        [backend-opt]
        root = "/srv/data"
        endpoint = "http://upstream:9000/?a=1,b=2"

        [[credentials]]
        access-key = "alice"
        secret-key = "alice-secret"

        [[credentials]]
        access-key = "bob"
        secret-key = "bob-secret"
    "#;

    #[test]
    fn the_config_file_sets_every_serve_setting() {
        // A deployment keeps its whole configuration, secrets included, in one file that only
        // the service can read, instead of on a command line every user can list.
        let file = config_file(FULL);
        let args = serve(&["--config", file.path().to_str().unwrap()]).unwrap();
        assert_eq!(args.backend, "fs");
        assert_eq!(args.address, "0.0.0.0:9100".parse().unwrap());
        assert_eq!(args.region, "eu-west-1");
        assert_eq!(args.domain, ["s3.example.com", "example.org"]);
        assert_eq!(args.etag_buffer_limit, 16 * 1024 * 1024);
        assert_eq!(args.stale_upload_age, Duration::from_secs(7 * 24 * 60 * 60));
        assert_eq!(args.tls_cert, Some("/etc/s3dal/cert.pem".into()));
        assert_eq!(args.tls_key, Some("/etc/s3dal/key.pem".into()));
        // A table, so an option value may contain the commas its flag form cannot.
        let mut opts = args.backend_opts.clone();
        opts.sort();
        assert_eq!(
            opts,
            [
                (
                    "endpoint".to_owned(),
                    "http://upstream:9000/?a=1,b=2".to_owned()
                ),
                ("root".to_owned(), "/srv/data".to_owned()),
            ]
        );
        assert_eq!(
            args.credentials,
            [pair("alice", "alice-secret"), pair("bob", "bob-secret")]
        );
    }

    #[test]
    fn flags_override_the_config_file() {
        // A shared file must not keep an operator from changing one setting for one run, and a
        // flag that silently lost to the file would leave them debugging the wrong value.
        let file = config_file(FULL);
        let args = serve(&[
            "--config",
            file.path().to_str().unwrap(),
            "--region",
            "us-west-2",
            "--backend-opt",
            "root=/elsewhere",
            "--access-key",
            "carol",
            "--secret-key",
            "carol-secret",
        ])
        .unwrap();
        assert_eq!(args.region, "us-west-2");
        // Lists are replaced whole rather than merged, so each setting has one source.
        assert_eq!(
            args.backend_opts,
            [("root".to_owned(), "/elsewhere".to_owned())]
        );
        // Key pairs are the exception: the flag's pair is added, so the file's pairs, which the
        // console manages, keep working next to the one the environment fixes.
        assert_eq!(
            args.credentials,
            [
                pair("carol", "carol-secret"),
                pair("alice", "alice-secret"),
                pair("bob", "bob-secret")
            ]
        );
        assert_eq!(args.fixed_access_key.as_deref(), Some("carol"));
        // What no flag names still comes from the file.
        assert_eq!(args.address, "0.0.0.0:9100".parse().unwrap());
        assert_eq!(args.etag_buffer_limit, 16 * 1024 * 1024);
        assert_eq!(args.domain, ["s3.example.com", "example.org"]);
    }

    #[test]
    fn settings_in_neither_place_keep_their_defaults() {
        let file = config_file(
            "backend = \"fs\"\n[[credentials]]\naccess-key = \"a\"\nsecret-key = \"b\"\n",
        );
        let args = serve(&["--config", file.path().to_str().unwrap()]).unwrap();
        assert_eq!(args.address, "127.0.0.1:9000".parse().unwrap());
        assert_eq!(args.settings(), Settings::default());
        assert_eq!(args.tls_cert, None);
    }

    #[test]
    fn unknown_config_keys_are_rejected() {
        // A misspelled key ignored would leave its setting at the default without a word, and
        // for a key such as `tls-cert` that means serving plaintext.
        for (text, key) in [
            (
                "backend = \"fs\"\ntls-certificate = \"c.pem\"\n",
                "tls-certificate",
            ),
            (
                "backend = \"fs\"\n[[credentials]]\naccess-key = \"a\"\nsecret = \"b\"\n",
                "secret",
            ),
            // `--config` names the file; it cannot name another one from inside it.
            ("backend = \"fs\"\nconfig = \"other.toml\"\n", "config"),
            // One place for key pairs in the file, not two that would have to be merged.
            (
                "backend = \"fs\"\naccess-key = \"a\"\nsecret-key = \"b\"\n",
                "access-key",
            ),
        ] {
            let file = config_file(text);
            let err = serve(&["--config", file.path().to_str().unwrap()]).unwrap_err();
            assert!(
                err.to_string().contains(&format!("`{key}`")),
                "{key}: {err}"
            );
        }
    }

    #[test]
    fn config_values_are_parsed_as_their_flags_are() {
        // The file and the flag take the same syntax; a value the flag refuses must not be
        // accepted from the file, or be read differently there.
        let file = config_file("backend = \"fs\"\netag-buffer-limit = \"8MB\"\n");
        let err = serve(&["--config", file.path().to_str().unwrap()]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ValueValidation, "{err}");
        // Named as a value of the file, so nobody looks for it on the command line.
        let msg = err.to_string();
        assert!(
            msg.contains("config file") && msg.contains("`etag-buffer-limit`"),
            "{msg}"
        );
        assert!(msg.contains("8MB"), "{msg}");
        let file = config_file("backend = \"fs\"\nregion = [\"a\", \"b\"]\n");
        let err = serve(&["--config", file.path().to_str().unwrap()]).unwrap_err();
        assert!(err.to_string().contains("`region`"), "{err}");
    }

    #[test]
    fn duplicate_access_keys_are_rejected() {
        // Two secrets for one access key would leave which of them verifies a signature to
        // lookup order.
        let file = config_file(
            "backend = \"fs\"\n\
             [[credentials]]\naccess-key = \"a\"\nsecret-key = \"one\"\n\
             [[credentials]]\naccess-key = \"a\"\nsecret-key = \"two\"\n",
        );
        let err = serve(&["--config", file.path().to_str().unwrap()]).unwrap_err();
        assert!(err.to_string().contains("\"a\""), "{err}");
    }

    #[test]
    fn a_key_pair_is_required() {
        // There is no anonymous mode: a server without keys must not start.
        let err = serve(&["--backend", "fs"]).unwrap_err();
        assert!(err.to_string().contains("no key pair"), "{err}");
        // Half a pair is a mistake, not a request for the file's pairs.
        let err = serve(&["--backend", "fs", "--access-key", "a"]).unwrap_err();
        assert!(err.to_string().contains("--secret-key"), "{err}");
    }

    #[test]
    fn tls_needs_both_a_certificate_and_a_key() {
        // Either one alone is a mistake. Serving plaintext instead would send credentials and
        // data in the clear to clients that expect HTTPS.
        let keys = ["--backend", "fs", "--access-key", "a", "--secret-key", "b"];
        for tls in [&["--tls-cert", "c.pem"][..], &["--tls-key", "k.pem"]] {
            let err = serve(&[&keys[..], tls].concat()).unwrap_err();
            assert!(
                err.to_string().contains("--tls-cert and --tls-key"),
                "{err}"
            );
        }
        let args = serve(&[&keys[..], &["--tls-cert", "c.pem", "--tls-key", "k.pem"]].concat());
        assert_eq!(args.unwrap().tls_key, Some("k.pem".into()));
        // The two may come from different places.
        let file = config_file("tls-cert = \"c.pem\"\n");
        let path = file.path().to_str().unwrap();
        let args = serve(&[&keys[..], &["--config", path, "--tls-key", "k.pem"]].concat());
        assert_eq!(args.unwrap().tls_cert, Some("c.pem".into()));
    }

    #[test]
    fn sizes_use_binary_units() {
        assert_eq!(parse_size("8MiB"), Ok(8 * 1024 * 1024));
        assert_eq!(parse_size("512KiB"), Ok(512 * 1024));
        assert_eq!(parse_size("1024"), Ok(1024));
        // Decimal units are rejected rather than guessed, so 8MB never silently means 8MiB.
        assert!(parse_size("8MB").is_err());
    }

    #[test]
    fn durations_need_a_unit_unless_zero() {
        assert_eq!(parse_duration("24h"), Ok(Duration::from_secs(86_400)));
        assert_eq!(parse_duration("90m"), Ok(Duration::from_secs(5_400)));
        assert_eq!(parse_duration("7d"), Ok(Duration::from_secs(604_800)));
        assert_eq!(parse_duration("30s"), Ok(Duration::from_secs(30)));
        // `0` is how the sweep is disabled, so it must parse; any other bare number is refused
        // rather than read as seconds, so `24` never sweeps uploads after 24 seconds.
        assert_eq!(parse_duration("0"), Ok(Duration::ZERO));
        assert!(parse_duration("24").is_err());
        assert!(parse_duration("1w").is_err());
    }

    #[test]
    fn a_zero_stale_upload_age_disables_the_sweep() {
        // The binary sweeps only when the setting holds an age; a zero age reaching it would
        // remove every upload, including those still being written.
        let args = |age: &str| {
            let cli = Cli::try_parse_from([
                "s3dal",
                "serve",
                "--backend=fs",
                "--access-key=a",
                "--secret-key=b",
                &format!("--stale-upload-age={age}"),
            ])
            .unwrap();
            let Command::Serve(args) = cli.command else {
                unreachable!("parsed as serve")
            };
            args.settings().stale_upload_age
        };
        assert_eq!(args("0"), None);
        assert_eq!(args("0h"), None);
        assert_eq!(args("2h"), Some(Duration::from_secs(7_200)));
    }

    #[test]
    fn check_config_prints_no_secret() {
        // Its output is what users paste into an issue or a chat to ask why the server does
        // not do what they expect; a secret key or upstream credential must not travel with it.
        let file = config_file(
            "backend = \"s3\"\n\
             [backend-opt]\nbucket = \"up\"\nsecret_access_key = \"UPSTREAM-SECRET\"\n\
             session_token = \"UPSTREAM-TOKEN\"\n\
             [[credentials]]\naccess-key = \"alice\"\nsecret-key = \"ALICE-SECRET\"\n",
        );
        let args = serve(&["--config", file.path().to_str().unwrap()]).unwrap();
        let text = args.describe();
        for secret in ["UPSTREAM-SECRET", "UPSTREAM-TOKEN", "ALICE-SECRET"] {
            assert!(!text.contains(secret), "{secret} in:\n{text}");
        }
        // What is not secret is shown, or the output would not help.
        for shown in [
            "bucket = \"up\"",
            "access-key = \"alice\"",
            "backend = \"s3\"",
        ] {
            assert!(text.contains(shown), "{shown} missing from:\n{text}");
        }
    }

    #[test]
    fn check_config_reads_the_config_file_as_serve_does() {
        // A check that read the file differently from `serve` would pass a file the server
        // then refuses.
        let file = config_file("backend = \"fs\"\nregion = \"eu-west-1\"\n");
        let path = file.path().to_str().unwrap();
        let cmd = Cli::command().mut_subcommand("check-config", |sub| {
            sub.mut_args(|arg| arg.env(None::<&'static str>))
        });
        let argv = [
            "s3dal",
            "check-config",
            "--config",
            path,
            "--access-key=a",
            "--secret-key=b",
        ];
        let Command::CheckConfig(args) = Cli::load_from(cmd.clone(), argv).unwrap().command else {
            panic!("parsed as another command")
        };
        assert_eq!(args.region, "eu-west-1");
        let bad = config_file("backend = \"fs\"\nregion = 1\nunknown = 2\n");
        let argv = [
            "s3dal",
            "check-config",
            "--config",
            bad.path().to_str().unwrap(),
        ];
        let err = Cli::load_from(cmd, argv).unwrap_err();
        assert!(err.to_string().contains("`unknown`"), "{err}");
    }

    #[test]
    fn a_log_level_that_does_nothing_is_refused_at_startup() {
        // `verbose` is valid filter syntax that enables nothing, so a server that took it would
        // run with less logging than the operator asked for and never say so.
        let file = config_file("backend = \"fs\"\nlog-level = \"verbose\"\n");
        let err = serve(&["--config", file.path().to_str().unwrap()]).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("`log-level`") && msg.contains("verbose"),
            "{msg}"
        );
        let keys = ["--backend", "fs", "--access-key", "a", "--secret-key", "b"];
        assert!(serve(&[&keys[..], &["--log-level", "info,s3dal="]].concat()).is_err());
        let args = serve(&[&keys[..], &["--log-level", "warn,s3dal=debug"]].concat()).unwrap();
        assert_eq!(args.log_level, "warn,s3dal=debug");
        assert_eq!(args.log_max_size, 100 * 1024 * 1024);
        assert_eq!(args.log_max_files, 5);
        assert_eq!(args.log_file, None);
    }

    #[test]
    fn public_buckets_and_the_log_collector_are_checked_when_read() {
        // A folder named as a public bucket would open nothing, or more than meant, depending
        // on how it was read; a collector URL with a typo would leave the log unsent. Both are
        // refused at startup instead.
        let keys = ["--backend", "fs", "--access-key", "a", "--secret-key", "b"];
        let args = serve(&[&keys[..], &["--public-bucket", "site,assets"]].concat()).unwrap();
        assert_eq!(args.public_bucket, ["site", "assets"]);
        assert!(serve(&[&keys[..], &["--public-bucket", "site/img"]].concat()).is_err());
        assert!(serve(&[&keys[..], &["--log-forward", "graylog:12201"]].concat()).is_err());

        let file = config_file(
            "backend = \"fs\"\npublic-bucket = [\"site\"]\n\
             log-forward = \"gelf+tcp://graylog:12201\"\n\
             [[credentials]]\naccess-key = \"a\"\nsecret-key = \"b\"\n",
        );
        let args = serve(&["--config", file.path().to_str().unwrap()]).unwrap();
        assert_eq!(args.public_bucket, ["site"]);
        assert_eq!(
            args.log_forward.as_deref(),
            Some("gelf+tcp://graylog:12201")
        );
    }

    #[test]
    fn check_config_hides_the_collector_password() {
        let keys = ["--backend", "fs", "--access-key", "a", "--secret-key", "b"];
        let url = "elasticsearch+https://elastic:COLLECTOR-SECRET@es:9200/logs";
        let args = serve(&[&keys[..], &["--log-forward", url]].concat()).unwrap();
        let text = args.describe();
        assert!(!text.contains("COLLECTOR-SECRET"), "{text}");
        assert!(text.contains("elastic:<redacted>@es:9200"), "{text}");
    }

    #[test]
    fn version_answers_to_both_v_and_capital_v() {
        // `-v` is what most tools take; refusing it sent people to `--help` to find `-V`.
        for flag in ["-v", "-V", "--version"] {
            let err = Cli::try_parse_from(["s3dal", flag]).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::DisplayVersion, "{flag}");
        }
    }

    #[test]
    fn backend_options_split_on_first_equals() {
        // Option values such as endpoints or signed URLs may contain '='.
        assert_eq!(
            parse_key_value("endpoint=http://h/?a=b"),
            Ok(("endpoint".into(), "http://h/?a=b".into()))
        );
        assert!(parse_key_value("=x").is_err());
        assert!(parse_key_value("root").is_err());
    }
}

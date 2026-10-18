use std::collections::HashSet;
use std::ffi::OsString;
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::error::ErrorKind;
use clap::{ArgAction, Args, CommandFactory, FromArgMatches, Parser, Subcommand};
use serde::Deserialize;

#[derive(Debug, Parser)]
#[command(
    name = "s3dal",
    version,
    about = "Expose any Apache OpenDAL backend as an S3-compatible endpoint"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Serve the S3 API on top of an OpenDAL backend.
    Serve(ServeArgs),
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

    /// One key pair; the config file can list several. These two replace the file's.
    // Keys are mandatory: an open-source gateway must not default to anonymous access.
    #[arg(long, env = "S3DAL_ACCESS_KEY")]
    access_key: Option<String>,

    #[arg(long, env = "S3DAL_SECRET_KEY", hide_env_values = true)]
    secret_key: Option<String>,

    /// Every key pair that may sign requests, from the flags or else from the config file.
    #[arg(skip)]
    pub credentials: Vec<KeyPair>,

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
}

impl ServeArgs {
    pub fn settings(&self) -> Settings {
        Settings {
            region: self.region.clone(),
            etag_buffer_limit: self.etag_buffer_limit,
            stale_upload_age: Some(self.stale_upload_age).filter(|age| !age.is_zero()),
        }
    }

    /// Takes what the file holds in shapes no flag has, then applies the rules that span
    /// several settings.
    fn finish(&mut self, file: FileOnly) -> Result<(), String> {
        // A flag or variable replaces the file's options whole, as for every list setting:
        // merging key by key would make an option's source hard to tell.
        if self.backend_opts.is_empty() {
            self.backend_opts = file.backend_opts;
        }
        self.credentials = match (self.access_key.take(), self.secret_key.take()) {
            (Some(access_key), Some(secret_key)) => vec![KeyPair {
                access_key,
                secret_key,
            }],
            (None, None) => file.credentials,
            _ => return Err("--access-key and --secret-key must be given together".into()),
        };
        check_key_pairs(&self.credentials)?;
        if self.tls_cert.is_some() != self.tls_key.is_some() {
            return Err("--tls-cert and --tls-key must be given together".into());
        }
        Ok(())
    }
}

/// One SigV4 credential. Each pair signs with its own secret; all of them reach the same
/// buckets, since there is one tenant.
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct KeyPair {
    pub access_key: String,
    pub secret_key: String,
}

// Written by hand so that printing a configuration never prints a secret.
impl fmt::Debug for KeyPair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyPair")
            .field("access_key", &self.access_key)
            .field("secret_key", &"<redacted>")
            .finish()
    }
}

/// There is no anonymous mode. Two secrets for one access key would leave which of them
/// verifies a signature to lookup order, so that is refused rather than resolved.
pub(crate) fn check_key_pairs(pairs: &[KeyPair]) -> Result<(), String> {
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
        let path = first
            .subcommand_matches("serve")
            .and_then(|m| m.get_one::<PathBuf>("config"))
            .cloned();
        let file = match path {
            Some(path) => {
                let serve = cmd.find_subcommand("serve").expect("defined above").clone();
                let (serve, file) =
                    read_file(serve, &path).map_err(|(kind, msg)| serve_error(&cmd, kind, msg))?;
                cmd = cmd.mut_subcommand("serve", |_| serve);
                file
            }
            None => FileOnly::default(),
        };
        let mut cli = Self::from_arg_matches(&cmd.clone().try_get_matches_from(&args)?)?;
        let Command::Serve(serve) = &mut cli.command;
        serve
            .finish(file)
            .map_err(|msg| serve_error(&cmd, ErrorKind::ArgumentConflict, msg))?;
        Ok(cli)
    }
}

/// An error shown with `serve`'s usage line, like the errors clap reports itself.
fn serve_error(cmd: &clap::Command, kind: ErrorKind, msg: String) -> clap::Error {
    let mut cmd = cmd.clone();
    cmd.build();
    let serve = cmd.find_subcommand_mut("serve").expect("defined above");
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
        let Command::Serve(serve) = Cli::load_from(cmd, argv)?.command;
        Ok(serve)
    }

    fn config_file(text: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(text.as_bytes()).unwrap();
        file
    }

    fn pair(access_key: &str, secret_key: &str) -> KeyPair {
        KeyPair {
            access_key: access_key.into(),
            secret_key: secret_key.into(),
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
        assert_eq!(args.credentials, [pair("carol", "carol-secret")]);
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
            let Command::Serve(args) = cli.command;
            args.settings().stale_upload_age
        };
        assert_eq!(args("0"), None);
        assert_eq!(args("0h"), None);
        assert_eq!(args("2h"), Some(Duration::from_secs(7_200)));
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

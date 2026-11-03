//! Settings that change while the server runs: the config file, and the certificate files it
//! names, are read again when they change or on SIGHUP, and what can change without a restart
//! is applied at once.

use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use crate::S3Dal;
use crate::config::{Cli, ServeArgs};
use crate::server::{LiveAuth, LiveCert};

/// How often the watched files are compared with what was last read. Reading a few small files
/// every two seconds costs nothing, and unlike file system events it also sees a Kubernetes
/// ConfigMap update, which swaps a symlink.
const POLL: Duration = Duration::from_secs(2);

/// How long a changed file must stay the same before it is read for good, so a file an editor
/// or `cp` is still writing is not mistaken for a broken one.
const SETTLE: Duration = Duration::from_millis(300);

pub struct Live {
    /// The process's own command line, read again on every reload.
    argv: Vec<OsString>,
    started: ServeArgs,
    current: Mutex<ServeArgs>,
    dal: S3Dal,
    auth: LiveAuth,
    cert: Option<LiveCert>,
}

impl Live {
    pub fn new(
        argv: Vec<OsString>,
        started: ServeArgs,
        dal: S3Dal,
        auth: LiveAuth,
        cert: Option<LiveCert>,
    ) -> Self {
        Self {
            argv,
            current: Mutex::new(started.clone()),
            started,
            dal,
            auth,
            cert,
        }
    }

    /// The settings as last read, which may include changes that wait for a restart; see
    /// [`Self::pending_restart`].
    pub fn current(&self) -> ServeArgs {
        self.lock().clone()
    }

    pub fn started(&self) -> &ServeArgs {
        &self.started
    }

    pub(crate) fn dal(&self) -> &S3Dal {
        &self.dal
    }

    /// The settings whose values changed since startup but take a restart to apply.
    pub fn pending_restart(&self) -> Vec<&'static str> {
        restart_only_changes(&self.started, &self.lock())
    }

    /// Reads the settings again, as at startup, and applies those that can change while
    /// serving. Everything is checked before anything is applied, so a file with a mistake
    /// leaves the server as it was.
    pub fn reload(&self) -> anyhow::Result<()> {
        let mut current = self.lock();
        let args = Cli::serve_from(&self.argv).map_err(|err| anyhow::anyhow!("{err}"))?;
        // A certificate is replaced only on a server that started with one: turning TLS on or
        // off changes what the listener speaks, which takes a restart.
        let certified = match (&self.cert, &args.tls_cert, &args.tls_key) {
            (Some(_), Some(cert), Some(key)) => Some(LiveCert::load(cert, key)?),
            _ => None,
        };
        self.auth.replace(&args.credentials, &args.domain)?;
        if let (Some(live), Some(certified)) = (&self.cert, certified) {
            live.set(certified);
        }
        self.dal.set_settings(args.settings());
        *current = args;
        Ok(())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ServeArgs> {
        self.current.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The files whose content decides the settings: the config file and the certificate and
    /// key, so a renewed certificate is picked up even when the config file stays the same.
    fn watched(&self) -> Vec<PathBuf> {
        let current = self.lock();
        [&current.config, &current.tls_cert, &current.tls_key]
            .into_iter()
            .flatten()
            .cloned()
            .collect()
    }
}

fn restart_only_changes(started: &ServeArgs, now: &ServeArgs) -> Vec<&'static str> {
    let mut changed = Vec::new();
    if started.backend != now.backend {
        changed.push("backend");
    }
    if started.backend_opts != now.backend_opts {
        changed.push("backend-opt");
    }
    if started.address != now.address {
        changed.push("address");
    }
    if started.log_format != now.log_format {
        changed.push("log-format");
    }
    if started.console_address != now.console_address {
        changed.push("console-address");
    }
    if started.tls_cert.is_some() != now.tls_cert.is_some() {
        changed.push("tls-cert");
    }
    changed
}

/// Reloads whenever a watched file's content changes or the process gets SIGHUP, until the
/// process ends.
pub async fn watch(live: std::sync::Arc<Live>) -> anyhow::Result<()> {
    #[cfg(unix)]
    let mut hangup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;
    let mut seen = snapshot(&live.watched());
    loop {
        #[cfg(unix)]
        let hup = tokio::select! {
            () = tokio::time::sleep(POLL) => false,
            _ = hangup.recv() => true,
        };
        #[cfg(not(unix))]
        let hup = {
            tokio::time::sleep(POLL).await;
            false
        };
        let paths = live.watched();
        let mut now = snapshot(&paths);
        if !hup && now == seen {
            continue;
        }
        // Read until two looks agree, so a write in progress is not taken for the file.
        loop {
            tokio::time::sleep(SETTLE).await;
            let again = snapshot(&paths);
            if again == now {
                break;
            }
            now = again;
        }
        match live.reload() {
            Ok(()) => {
                tracing::info!(signal = hup, "configuration reloaded");
                let pending = live.pending_restart();
                if !pending.is_empty() {
                    tracing::warn!(settings = ?pending, "changed settings take effect on restart");
                }
            }
            Err(err) => tracing::error!(
                error = format!("{err:#}"),
                "configuration not reloaded; the server keeps the settings it runs with"
            ),
        }
        // A reload may name other certificate files; watch those from now on.
        seen = snapshot(&live.watched());
    }
}

/// The content of each file, or `None` for one that cannot be read. The files are small, and
/// comparing content also catches an edit within the same second as the last one, which a
/// modification time might not show.
fn snapshot(paths: &[PathBuf]) -> Vec<Option<Vec<u8>>> {
    paths.iter().map(|p| std::fs::read(p).ok()).collect()
}

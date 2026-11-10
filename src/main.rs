use std::io::IsTerminal;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::time::Duration;

use anyhow::Context;
use s3dal::config::{Cli, Command, HealthcheckArgs, LogFormat, ServeArgs};
use std::sync::Arc;

use futures::FutureExt;
use s3dal::console::Console;
use s3dal::live::{self, Live};
use s3dal::server::LiveAuth;
use s3dal::{S3Dal, backend, logging, server};
use tokio::net::TcpListener;
use tracing_subscriber::fmt::writer::BoxMakeWriter;
use tracing_subscriber::prelude::*;
use tracing_subscriber::reload;

fn main() -> anyhow::Result<()> {
    limit_malloc_arenas();
    let cli = Cli::load();
    match cli.command {
        Command::Serve(args) => {
            let flush = init_logging(&args)?;
            let served = serve(args);
            // Into the log as well as onto stderr, where `main` prints it: the file and the
            // collector would otherwise end without saying why.
            if let Err(err) = &served {
                tracing::error!(error = format!("{err:#}"), "s3dal stopped on an error");
            }
            // The file and stderr are written as each line comes; only the collector's queue
            // can still hold the last ones.
            if let Some(flush) = flush {
                flush.wait(FLUSH_TIMEOUT);
            }
            match served? {
                Stop::Asked => Ok(()),
                Stop::Restart => Err(restart()),
            }
        }
        Command::CheckConfig(args) => {
            print!("{}", args.describe());
            Ok(())
        }
        Command::Healthcheck(args) => healthcheck(args),
        Command::Keygen => keygen(),
        #[cfg(feature = "client")]
        Command::Client(cmd) => s3dal::client::run(cmd),
    }
}

/// How long exiting waits for the collector to take the lines still queued. Requests get 10
/// seconds to finish on SIGTERM, and Docker's and the compose file's stop grace leave a few
/// more before SIGKILL; a collector that is down must not use them all.
const FLUSH_TIMEOUT: Duration = Duration::from_secs(3);

/// Behind a reload layer, so a config reload can change `--log-level` without a restart;
/// `logging::filter` says why s3s is off by default. Returns what waits for the collector at
/// exit, when there is one.
fn init_logging(args: &ServeArgs) -> anyhow::Result<Option<logging::forward::Flush>> {
    // `--log-level` was checked when the settings were read.
    let filter = logging::filter(&args.log_level).map_err(|e| anyhow::anyhow!(e))?;
    let (filter, handle) = reload::Layer::new(filter);
    logging::set_reload_handle(handle);
    // stderr by default, with clap's errors: logs are diagnostics, not the output of a command.
    let (writer, terminal) = match &args.log_file {
        Some(path) => {
            let file = logging::RotatingFile::open(path, args.log_max_size, args.log_max_files)
                .with_context(|| format!("cannot open the log file {}", path.display()))?;
            (BoxMakeWriter::new(file), false)
        }
        None => (
            BoxMakeWriter::new(std::io::stderr),
            std::io::stderr().is_terminal(),
        ),
    };
    let logs = tracing_subscriber::fmt::layer().with_writer(writer);
    let logs = match args.log_format {
        LogFormat::Json => logs.json().boxed(),
        // Colors only on a terminal: tracing-subscriber colors by default, which leaves escape
        // codes in `docker logs`, journald and log files.
        LogFormat::Text => logs
            .with_ansi(terminal && std::env::var_os("NO_COLOR").is_none_or(|v| v.is_empty()))
            .boxed(),
    };
    // In addition to stderr or the file, not instead: `docker logs` keeps working, and so does
    // the log while the collector is down. JSON whatever `--log-format` says, flat, with the
    // request's span, so each field becomes one field of the collector's.
    let (forward, flush) = match &args.log_forward {
        Some(url) => {
            let (writer, flush) = logging::forward::start(url).map_err(|e| anyhow::anyhow!(e))?;
            let layer = tracing_subscriber::fmt::layer()
                .json()
                .flatten_event(true)
                .with_current_span(true)
                .with_span_list(false)
                .with_writer(writer);
            (Some(layer), Some(flush))
        }
        None => (None, None),
    };
    tracing_subscriber::registry()
        .with(filter)
        .with(logs)
        .with(forward)
        .init();
    if let Some(url) = &args.log_forward {
        let to = logging::forward::redact(url, "");
        tracing::info!(%to, "the log is also sent to a collector");
    }
    Ok(flush)
}

#[tokio::main(flavor = "current_thread")]
async fn healthcheck(args: HealthcheckArgs) -> anyhow::Result<()> {
    let url = match args.url {
        Some(url) => url,
        None => {
            let serve = Cli::serve_from_env().map_err(|err| anyhow::anyhow!("{err}"))?;
            // A server listening on every interface answers on the loopback one.
            let mut address = serve.address;
            if address.ip().is_unspecified() {
                address.set_ip(match address.ip() {
                    IpAddr::V4(_) => Ipv4Addr::LOCALHOST.into(),
                    IpAddr::V6(_) => Ipv6Addr::LOCALHOST.into(),
                });
            }
            let scheme = if serve.tls_cert.is_some() {
                "https"
            } else {
                "http"
            };
            format!("{scheme}://{address}")
        }
    };
    server::check_health(&url)
        .await
        .with_context(|| format!("{url} is not healthy"))
}

/// An access key of 20 characters and a secret key of 40, as AWS shapes them, from the OS
/// generator. The secret holds 240 random bits.
fn keygen() -> anyhow::Result<()> {
    const UPPER_ALNUM: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut bytes = [0u8; 50];
    getrandom::fill(&mut bytes).map_err(|err| anyhow::anyhow!("no randomness: {err}"))?;
    // 32 symbols, so each byte's low five bits pick one without bias.
    let access_key: String = bytes[..20]
        .iter()
        .map(|b| char::from(UPPER_ALNUM[usize::from(b & 31)]))
        .collect();
    let secret_key = base64_simd::URL_SAFE_NO_PAD.encode_to_string(&bytes[20..]);
    println!("[[credentials]]\naccess-key = \"{access_key}\"\nsecret-key = \"{secret_key}\"");
    Ok(())
}

/// Two malloc arenas per core, where glibc allows eight. A thread that finds the arenas busy
/// moves to another, and an arena keeps what is freed into it rather than returning it, so
/// the buffers of large uploads, 8 at a time, come to be held once per arena. In the benchmark
/// on a 4-core guest the server peaked at 728 MiB with eight per core and 520 MiB with two,
/// at the same throughput but for listings, 6–11% slower. One per core gave 467 MiB, with
/// listings 14–20% slower: they allocate on every thread at once. MALLOC_ARENA_MAX, when set,
/// still decides.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn limit_malloc_arenas() {
    if std::env::var_os("MALLOC_ARENA_MAX").is_none() {
        let cores = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        let arenas = libc::c_int::try_from(cores * 2).unwrap_or(libc::c_int::MAX);
        // SAFETY: mallopt only bounds the arenas created from now on; no other thread runs yet.
        unsafe { libc::mallopt(libc::M_ARENA_MAX, arenas) };
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn limit_malloc_arenas() {}

/// Why the server stopped serving.
#[derive(Clone, Copy)]
enum Stop {
    /// A signal: the process exits.
    Asked,
    /// The console asked for a restart.
    Restart,
}

/// The same command line again, in this process: its PID stays, so a container whose PID 1 it
/// is runs on, and a supervisor sees no exit to act on. The environment, which may hold
/// settings, stays too. Returns only if the program could not be run.
#[cfg(unix)]
fn restart() -> anyhow::Error {
    use std::os::unix::process::CommandExt;
    let mut argv = std::env::args_os();
    let program = argv.next().unwrap_or_else(|| "s3dal".into());
    let err = std::process::Command::new(&program).args(argv).exec();
    anyhow::anyhow!("cannot restart as {}: {err}", program.to_string_lossy())
}

/// The console refuses to restart here, so this is never reached.
#[cfg(not(unix))]
fn restart() -> anyhow::Error {
    anyhow::anyhow!("restarting needs a Unix system")
}

#[tokio::main]
async fn serve(args: ServeArgs) -> anyhow::Result<Stop> {
    // Before the probe, which can take a while on a remote backend, so a bad certificate fails
    // at once.
    let (tls, cert) = match (&args.tls_cert, &args.tls_key) {
        (Some(cert), Some(key)) => {
            let (acceptor, live) = server::tls_acceptor(cert, key)?;
            (Some(acceptor), Some(live))
        }
        _ => (None, None),
    };
    // Backend options are not logged: they may carry upstream credentials.
    let op = backend::build_operator(&args.backend, &args.backend_opts)?;
    let profile = backend::probe(&op).await?;
    tracing::info!(backend = %args.backend, ?profile, "backend profile");

    let dal = S3Dal::new(op, profile, args.settings());
    tokio::spawn(sweep_stale(dal.clone()));
    let auth = LiveAuth::new(&args.credentials, &args.domain)?;
    auth.set_public(&args.public_bucket);
    let (address, console_address) = (args.address, args.console_address);
    let live = Arc::new(Live::new(
        std::env::args_os().collect(),
        args,
        dal.clone(),
        auth.clone(),
        cert,
    ));
    let service = server::build_service(dal, &auth, Some(live.clone()));
    let scheme = if tls.is_some() { "https" } else { "http" };
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("cannot listen on {address}"))?;
    tracing::info!("listening on {scheme}://{}", listener.local_addr()?);
    let console = match console_address {
        Some(address) => {
            let listener = TcpListener::bind(address)
                .await
                .with_context(|| format!("cannot listen on {address} for the console"))?;
            tracing::info!("console on {scheme}://{}", listener.local_addr()?);
            Some(listener)
        }
        None => None,
    };

    let watcher = tokio::spawn(live::watch(live.clone()));
    // One signal, or a restart, stops both listeners, each draining its own requests.
    let signal = shutdown_signal()?;
    let restart = {
        let live = live.clone();
        async move {
            live.restart_requested().await;
            let settings = live.pending_restart();
            tracing::info!(
                ?settings,
                "restarting: no new connections; requests in flight get 10 seconds"
            );
        }
    };
    let stop = async move {
        tokio::select! {
            () = signal => Stop::Asked,
            () = restart => Stop::Restart,
        }
    }
    .shared();
    let shutdown = || stop.clone().map(|_| ());
    let s3 = server::serve(listener, service.clone(), tls.clone(), shutdown());
    match console {
        Some(listener) => {
            let console = Arc::new(Console::new(live));
            let pages = server::serve_console(listener, service, console, tls, shutdown());
            tokio::join!(s3, pages);
        }
        None => s3.await,
    }
    watcher.abort();
    tracing::info!("stopped");
    // Resolved already: the listeners stopped on it.
    Ok(stop.await)
}

/// Ctrl-C, or SIGTERM: what Docker, systemd and Kubernetes stop a service with. Without a
/// handler, SIGTERM would end s3dal mid-request, or, as PID 1 in a container, not at all.
fn shutdown_signal() -> anyhow::Result<impl Future<Output = ()>> {
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("cannot handle SIGTERM")?;
    Ok(async move {
        #[cfg(unix)]
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
        // So the log tells a stop that was asked for from a crash.
        tracing::info!("stopping: no new connections; requests in flight get 10 seconds");
    })
}

/// Sweeps stale multipart uploads, and on fs the temporary files of cut-off writes, at startup,
/// which catches up on what went stale while s3dal was down or was left by a crash, and then
/// hourly, or more often for an age under an hour, so nothing outlives its age by much. The age
/// is read again each time, since a config reload may change it, or turn the sweep on or off.
async fn sweep_stale(dal: S3Dal) {
    const HOUR: Duration = Duration::from_secs(60 * 60);
    loop {
        let Some(age) = dal.settings().stale_upload_age else {
            // Looks again in a minute, so a reload that turns the sweep on is not missed for long.
            tokio::time::sleep(Duration::from_secs(60)).await;
            continue;
        };
        let next = tokio::time::Instant::now() + age.min(HOUR);
        match dal.sweep_stale_uploads(age).await {
            Ok(0) => {}
            Ok(removed) => tracing::info!(removed, "removed stale multipart uploads"),
            Err(err) => tracing::warn!(error = %err, "cannot sweep stale multipart uploads"),
        }
        match dal.sweep_stale_temp_files(age).await {
            Ok(0) => {}
            Ok(removed) => tracing::info!(removed, "removed stale temporary files"),
            Err(err) => tracing::warn!(error = %err, "cannot sweep stale temporary files"),
        }
        tokio::time::sleep_until(next).await;
    }
}

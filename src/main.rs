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
use s3dal::{S3Dal, backend, server};
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

fn main() -> anyhow::Result<()> {
    limit_malloc_arenas();
    let cli = Cli::load();
    match cli.command {
        Command::Serve(args) => {
            init_logging(args.log_format);
            serve(args)
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

fn init_logging(format: LogFormat) {
    // s3s logs every error answer, a client's unsigned request included, at ERROR, in two or
    // three lines; the access log says the same in one line at the right level. s3dal logs the
    // failures that are its own. `RUST_LOG=info,s3s=debug` brings s3s's lines back.
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,s3s=off"));
    // stderr, with clap's errors: logs are diagnostics, not the output of a command.
    let logs = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(filter);
    match format {
        LogFormat::Json => logs.json().init(),
        LogFormat::Text => {
            // Colors only on a terminal: tracing-subscriber colors by default, which leaves
            // escape codes in `docker logs`, journald and log files.
            let ansi = std::io::stderr().is_terminal()
                && std::env::var_os("NO_COLOR").is_none_or(|v| v.is_empty());
            logs.with_ansi(ansi).init();
        }
    }
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

#[tokio::main]
async fn serve(args: ServeArgs) -> anyhow::Result<()> {
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
    // One signal stops both listeners, each draining its own requests.
    let shutdown = shutdown_signal()?.shared();
    let s3 = server::serve(listener, service.clone(), tls.clone(), shutdown.clone());
    match console {
        Some(listener) => {
            let console = Arc::new(Console::new(live));
            let pages = server::serve_console(listener, service, console, tls, shutdown);
            tokio::join!(s3, pages);
        }
        None => s3.await,
    }
    watcher.abort();
    Ok(())
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

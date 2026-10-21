use std::io::IsTerminal;
use std::time::Duration;

use anyhow::Context;
use s3dal::config::{Cli, Command, ServeArgs};
use s3dal::{S3Dal, backend, server};
use tokio::net::TcpListener;
use tokio::time::MissedTickBehavior;
use tracing_subscriber::EnvFilter;

fn main() -> anyhow::Result<()> {
    limit_malloc_arenas();
    let cli = Cli::load();
    // stderr, with clap's errors: logs are diagnostics, not the output of a command. Colors
    // only on a terminal: tracing-subscriber colors by default, which leaves escape codes in
    // `docker logs`, journald and log files.
    let ansi = std::io::stderr().is_terminal()
        && std::env::var_os("NO_COLOR").is_none_or(|v| v.is_empty());
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(ansi)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    match cli.command {
        Command::Serve(args) => serve(args),
    }
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
    let tls = match (&args.tls_cert, &args.tls_key) {
        (Some(cert), Some(key)) => Some(server::tls_acceptor(cert, key)?),
        _ => None,
    };
    // Backend options are not logged: they may carry upstream credentials.
    let op = backend::build_operator(&args.backend, &args.backend_opts)?;
    let profile = backend::probe(&op).await?;
    tracing::info!(backend = %args.backend, ?profile, "backend profile");

    let settings = args.settings();
    let stale_upload_age = settings.stale_upload_age;
    let dal = S3Dal::new(op, profile, settings);
    if let Some(age) = stale_upload_age {
        tokio::spawn(sweep_stale(dal.clone(), age));
    }
    let service = server::build_service(dal, &args.credentials, &args.domain)?;
    let listener = TcpListener::bind(args.address)
        .await
        .with_context(|| format!("cannot listen on {}", args.address))?;
    let scheme = if tls.is_some() { "https" } else { "http" };
    tracing::info!("listening on {scheme}://{}", listener.local_addr()?);

    server::serve(listener, service, tls, shutdown_signal()?).await;
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
/// hourly, or more often for an age under an hour, so nothing outlives its age by much.
async fn sweep_stale(dal: S3Dal, age: Duration) {
    let mut ticks = tokio::time::interval(age.min(Duration::from_secs(60 * 60)));
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        ticks.tick().await;
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
    }
}

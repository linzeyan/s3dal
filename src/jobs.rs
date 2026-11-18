//! Background jobs, F2 of features-2026-11.md: one runner per process, and with several
//! instances sharing a backend a job runs on the holder of its lease only. The lease is
//! `.s3dal/leases/<job>.json`, `{node, until}`, taken and renewed with a conditional write where
//! the backend checks them (see `s3::swap`); elsewhere, as on fs, which one process serves, it
//! holds within the process.
//!
//! A holder renews its lease every third of the lease's length, also while a job runs, so a
//! long run is not taken over. A holder that stops renewing, because it died or turned the job
//! off, is replaced once `until` passes: within one lease length plus one renewal period. The
//! times are wall clocks, compared across instances, so clocks more than a third of the lease
//! apart could let two instances hold a lease at once; NTP keeps them far closer, and the jobs
//! are safe to run twice, only wasteful.

use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant, SystemTime};

use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::S3Dal;
use crate::backend::RESERVED_ROOT;
use crate::config::Settings;
use crate::s3::swap;

/// How long a lease lasts from its last renewal.
pub const LEASE: Duration = Duration::from_secs(60);

/// A job: its name (also its lease's and its metrics' label), how often it runs with the
/// server's settings as they are (`None`: off in this process), and the run itself.
struct Job {
    name: &'static str,
    every: fn(&S3Dal) -> Option<Duration>,
    run: fn(S3Dal) -> Run,
}

type Run = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;

const JOBS: &[Job] = &[
    Job {
        name: STALE_UPLOADS,
        every: |dal| stale_uploads_every(&dal.settings()),
        run: sweep_stale,
    },
    Job {
        name: USAGE_SCAN,
        every: |dal| dal.settings().usage_scan_interval,
        run: scan_usage,
    },
    Job {
        name: LIFECYCLE,
        every: |_| Some(LIFECYCLE_EVERY),
        run: apply_lifecycle,
    },
    #[cfg(feature = "thumbnail")]
    Job {
        name: THUMBNAIL_SWEEP,
        every: |_| Some(THUMBNAIL_SWEEP_EVERY),
        run: sweep_thumbnails,
    },
    Job {
        name: TIER_SWEEP,
        every: |dal| dal.has_tiers().then_some(TIER_SWEEP_EVERY),
        run: sweep_tiers,
    },
];

/// The stale-upload sweep.
pub const STALE_UPLOADS: &str = "stale-uploads";

/// The count of what each bucket holds, which quotas are enforced against.
pub const USAGE_SCAN: &str = "usage-scan";

/// The buckets' lifecycle rules.
pub const LIFECYCLE: &str = "lifecycle";

/// How often the lifecycle job looks in. A bucket is applied only once a day (see
/// `S3Dal::apply_due_lifecycle`), so a run that finds nothing due lists the bucket names and
/// reads their cached states; hourly, a day's expiry starts within the hour after midnight UTC,
/// and rules just put take effect within the hour.
const LIFECYCLE_EVERY: Duration = Duration::from_secs(60 * 60);

/// The removal of cached thumbnails whose object is gone or was overwritten.
#[cfg(feature = "thumbnail")]
pub const THUMBNAIL_SWEEP: &str = "thumbnail-sweep";

/// Daily: a stale thumbnail is never served (its name holds the ETag it was made of), it only
/// takes space, and a sweep stats one object per key with thumbnails, which on a remote backend
/// is a billed request each.
#[cfg(feature = "thumbnail")]
const THUMBNAIL_SWEEP_EVERY: Duration = Duration::from_secs(24 * 60 * 60);

/// The removal of tier objects no stub names, with cold tiers configured.
pub const TIER_SWEEP: &str = "tier-sweep";

/// Daily: an orphan only takes space, it goes only once a day old anyway, and the sweep lists
/// every bucket and every tier whole.
const TIER_SWEEP_EVERY: Duration = Duration::from_secs(24 * 60 * 60);

/// Hourly, or more often for an age under an hour, so nothing outlives its age by much; off with
/// `--stale-upload-age 0`.
fn stale_uploads_every(settings: &Settings) -> Option<Duration> {
    const HOUR: Duration = Duration::from_secs(60 * 60);
    settings.stale_upload_age.map(|age| age.min(HOUR))
}

/// Removes stale multipart uploads, and on fs the temporary files of cut-off writes; the second
/// runs whether the first failed or not. The age is read at each run, since a config reload may
/// change it.
fn sweep_stale(dal: S3Dal) -> Run {
    Box::pin(async move {
        let Some(age) = dal.settings().stale_upload_age else {
            return Ok(());
        };
        let uploads = dal.sweep_stale_uploads(age).await;
        if let Ok(removed @ 1..) = uploads {
            tracing::info!(removed, "removed stale multipart uploads");
        }
        let temp = dal.sweep_stale_temp_files(age).await;
        if let Ok(removed @ 1..) = temp {
            tracing::info!(removed, "removed stale temporary files");
        }
        uploads
            .context("cannot sweep stale multipart uploads")
            .and(temp.context("cannot sweep stale temporary files"))
            .map(drop)
    })
}

fn scan_usage(dal: S3Dal) -> Run {
    Box::pin(async move {
        let counted = dal
            .scan_usage()
            .await
            .context("cannot count the buckets' usage")?;
        tracing::debug!(buckets = counted, "counted the buckets' usage");
        Ok(())
    })
}

fn apply_lifecycle(dal: S3Dal) -> Run {
    Box::pin(async move {
        let expired = dal
            .apply_due_lifecycle(SystemTime::now())
            .await
            .context("cannot apply the lifecycle rules")?;
        tracing::debug!(?expired, "applied the lifecycle rules");
        Ok(())
    })
}

#[cfg(feature = "thumbnail")]
fn sweep_thumbnails(dal: S3Dal) -> Run {
    Box::pin(async move {
        let removed = dal
            .sweep_thumbnails()
            .await
            .context("cannot sweep the thumbnail cache")?;
        if removed > 0 {
            tracing::info!(removed, "removed stale thumbnails");
        }
        Ok(())
    })
}

fn sweep_tiers(dal: S3Dal) -> Run {
    Box::pin(async move {
        let swept = dal
            .sweep_tiers(SystemTime::now())
            .await
            .context("cannot sweep the tiers")?;
        tracing::debug!(?swept, "swept the tiers");
        Ok(())
    })
}

/// The lease file.
#[derive(Serialize, Deserialize)]
struct Lease {
    node: String,
    /// RFC 3339, so a person reading the file can tell it.
    until: String,
}

impl Lease {
    /// A lease whose time cannot be read is as good as expired: nothing renews it into shape.
    fn expired(&self, now: SystemTime) -> bool {
        OffsetDateTime::parse(&self.until, &Rfc3339)
            .map_or(true, |until| SystemTime::from(until) <= now)
    }
}

/// Runs the jobs of one process.
pub struct Runner {
    dal: S3Dal,
    node: String,
    lease: Duration,
}

impl Runner {
    /// A runner with a node id of its own, random, so a restarted process is a new holder and
    /// never mistakes the lease of its previous run for its own.
    pub fn new(dal: S3Dal) -> Self {
        Self {
            dal,
            node: uuid::Uuid::new_v4().simple().to_string(),
            lease: LEASE,
        }
    }

    /// A lease of another length, for tests that watch a takeover.
    pub fn with_lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    pub fn node(&self) -> &str {
        &self.node
    }

    /// Takes the job's lease, or renews it, as of `now`: whether this runner holds it now, until
    /// `now` plus the lease's length. A lease another runner holds and has not let expire is
    /// left to it.
    pub async fn hold(&self, job: &str, now: SystemTime) -> anyhow::Result<bool> {
        let until = OffsetDateTime::from(now + self.lease)
            .format(&Rfc3339)
            .map_err(|e| anyhow::anyhow!("cannot write the lease's time: {e}"))?;
        let path = format!("{RESERVED_ROOT}leases/{job}.json");
        let taken = swap::update(&self.dal, &path, |current: Option<Lease>| {
            let free = current
                .as_ref()
                .is_none_or(|l| l.node == self.node || l.expired(now));
            free.then(|| Lease {
                node: self.node.clone(),
                until: until.clone(),
            })
        })
        .await
        .map_err(|e| anyhow::anyhow!("cannot take the lease of job {job}: {e}"))?;
        Ok(taken.is_some())
    }

    /// Runs every job that is on, when due and while this runner holds its lease, until the
    /// process ends. A job is due at once when the runner starts or takes its lease over, which
    /// catches up on what was missed while no one ran it.
    pub async fn run(self) {
        let tick = self.lease / 3;
        let mut due: Vec<Instant> = vec![Instant::now(); JOBS.len()];
        loop {
            for (job, due) in JOBS.iter().zip(due.iter_mut()) {
                // Off here: the lease is left to expire, for an instance where the job is on.
                let Some(every) = (job.every)(&self.dal) else {
                    self.dal.metrics().set_lease_held(job.name, false);
                    continue;
                };
                let held = match self.hold(job.name, SystemTime::now()).await {
                    Ok(held) => held,
                    Err(err) => {
                        tracing::warn!(job = job.name, error = %err, "cannot take the job's lease");
                        false
                    }
                };
                self.dal.metrics().set_lease_held(job.name, held);
                if !held {
                    *due = Instant::now();
                    continue;
                }
                if Instant::now() >= *due {
                    self.run_job(job, tick).await;
                    *due = Instant::now() + every;
                }
            }
            tokio::time::sleep(tick).await;
        }
    }

    /// Runs `job`, renewing its lease every `tick` until it is done.
    async fn run_job(&self, job: &Job, tick: Duration) {
        let started = Instant::now();
        let run = (job.run)(self.dal.clone());
        tokio::pin!(run);
        let outcome = loop {
            tokio::select! {
                outcome = &mut run => break outcome,
                () = tokio::time::sleep(tick) => {
                    if let Err(err) = self.hold(job.name, SystemTime::now()).await {
                        tracing::warn!(job = job.name, error = %err, "cannot renew the job's lease");
                    }
                }
            }
        };
        let took = started.elapsed();
        self.dal.metrics().job_ran(job.name, outcome.is_ok(), took);
        match outcome {
            Ok(()) => tracing::debug!(job = job.name, ?took, "job done"),
            Err(err) => tracing::warn!(job = job.name, error = %err, "job failed"),
        }
    }
}

//! Background jobs and their lease (F2): with several instances on one backend, a job runs on
//! one of them, and another takes over when that one stops renewing its lease.

mod common;

use std::time::{Duration, SystemTime};

use common::{Backend, TestServer};
use s3dal::Settings;
use s3dal::jobs::{LEASE, Runner, STALE_UPLOADS};

#[tokio::test]
async fn a_lease_is_held_by_one_instance_at_a_time() {
    // Two instances asking at the same instant must not both run a job that is meant to run
    // once per backend.
    let a = TestServer::start().await;
    let b = TestServer::start_on(Backend::sharing(&a), Settings::default()).await;
    let (ra, rb) = (Runner::new(a.dal.clone()), Runner::new(b.dal.clone()));
    let now = SystemTime::now();
    let (held_a, held_b) = tokio::join!(ra.hold("test-job", now), rb.hold("test-job", now));
    let (held_a, held_b) = (held_a.unwrap(), held_b.unwrap());
    assert!(held_a != held_b, "a: {held_a}, b: {held_b}");
    let (holder, other) = if held_a { (&ra, &rb) } else { (&rb, &ra) };

    // The holder renews; the other keeps out for as long as the lease lasts.
    let later = now + LEASE / 2;
    assert!(holder.hold("test-job", later).await.unwrap());
    assert!(!other.hold("test-job", later).await.unwrap());
    // Another job's lease is another file.
    assert!(other.hold("other-job", later).await.unwrap());
    let lease: serde_json::Value =
        serde_json::from_slice(&a.read(".s3dal/leases/test-job.json").await).unwrap();
    assert_eq!(lease["node"], holder.node(), "{lease}");
}

#[tokio::test]
async fn a_lease_its_holder_stops_renewing_is_taken_over_once_it_expires() {
    // A holder that died renews nothing; its job must not stop for good, nor be taken over
    // while the lease still runs. The clock is passed in, so no test waits a lease out.
    let a = TestServer::start().await;
    let b = TestServer::start_on(Backend::sharing(&a), Settings::default()).await;
    let (ra, rb) = (Runner::new(a.dal.clone()), Runner::new(b.dal.clone()));
    let t0 = SystemTime::now();
    assert!(ra.hold("test-job", t0).await.unwrap());
    assert!(
        !rb.hold("test-job", t0 + LEASE - Duration::from_secs(1))
            .await
            .unwrap()
    );
    let expired = t0 + LEASE + Duration::from_secs(1);
    assert!(rb.hold("test-job", expired).await.unwrap());
    // The old holder, back, finds the lease taken and leaves it.
    assert!(!ra.hold("test-job", expired).await.unwrap());
    let lease: serde_json::Value =
        serde_json::from_slice(&a.read(".s3dal/leases/test-job.json").await).unwrap();
    assert_eq!(lease["node"], rb.node(), "{lease}");

    // A runner with a short lease, for a test that does wait, gets the same treatment.
    let short = Runner::new(a.dal.clone()).with_lease(Duration::from_millis(200));
    assert!(short.hold("short-job", SystemTime::now()).await.unwrap());
    assert!(!rb.hold("short-job", SystemTime::now()).await.unwrap());
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(rb.hold("short-job", SystemTime::now()).await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_stale_upload_sweep_runs_on_the_lease_holder_only() {
    // Every instance runs a runner; the sweep must still remove what went stale, once, and the
    // metrics must say which instance ran it.
    let settings = Settings {
        stale_upload_age: Some(Duration::from_secs(1)),
        ..Settings::default()
    };
    let a = TestServer::start_with(settings.clone()).await;
    let b = TestServer::start_on(Backend::sharing(&a), settings).await;
    a.create_bucket_dir("bkt").await;
    a.client
        .create_multipart_upload()
        .bucket("bkt")
        .key("abandoned")
        .send()
        .await
        .unwrap();
    // Older than the age once the runners start.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let runners = [
        tokio::spawn(Runner::new(a.dal.clone()).run()),
        tokio::spawn(Runner::new(b.dal.clone()).run()),
    ];

    let started = std::time::Instant::now();
    loop {
        let uploads = a
            .client
            .list_multipart_uploads()
            .bucket("bkt")
            .send()
            .await
            .unwrap();
        if uploads.uploads().is_empty() {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the stale upload is still there"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // The run is counted once it is done, a moment after its last removal, and the other
    // instance's lease state once it has asked.
    let ran =
        format!("s3dal_job_duration_seconds_count{{job=\"{STALE_UPLOADS}\",outcome=\"ok\"}} 1");
    let held = |n: u8| format!("s3dal_job_lease_held{{job=\"{STALE_UPLOADS}\"}} {n}");
    let (runner, other) = loop {
        let [sa, sb] = [a.dal.metrics().render(), b.dal.metrics().render()];
        let (runner, other) = if sa.contains(&ran) {
            (sa, sb)
        } else {
            (sb, sa)
        };
        if runner.contains(&ran) && other.contains(&held(0)) {
            break (runner, other);
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "no run counted:\n{runner}\n{other}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(runner.contains(&held(1)), "{runner}");
    // The other jobs' leases may have gone either way.
    assert!(
        !other.contains(&format!(
            "s3dal_job_duration_seconds_count{{job=\"{STALE_UPLOADS}\""
        )),
        "{other}"
    );
    for runner in runners {
        runner.abort();
    }
}

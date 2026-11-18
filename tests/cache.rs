//! The hot read cache, `--cache-size` (features-2026-11.md item 2a).
//!
//! The cache runs on remote backends only; on fs, which cannot pin a read to an ETag, it is
//! never built, and the tests check there that nothing is counted. Their point is the runs under
//! scripts/with-rustfs.sh, with-azurite.sh and with-fake-gcs.sh.

mod common;

use std::collections::HashMap;

use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::{ByteStream, DateTime};
use aws_sdk_s3::types::{
    BucketVersioningStatus, ChecksumMode, CompletedMultipartUpload, CompletedPart,
    VersioningConfiguration,
};
use common::{Backend, TestServer};
use s3dal::Settings;

const BUCKET: &str = "hot";

/// The per-object limit, `MAX_OBJECT` in src/s3/cache.rs.
const MAX_OBJECT: usize = 1 << 20;

fn with_cache(bytes: u64) -> Settings {
    Settings {
        cache_size: bytes,
        ..Default::default()
    }
}

/// Whether `server` caches at all: everywhere but on fs, where the page cache already does.
fn caches(server: &TestServer) -> bool {
    server.op.info().scheme() != "fs"
}

/// A series of `/_s3dal/metrics` without labels.
fn metric(server: &TestServer, name: &str) -> u64 {
    let text = server.dal.metrics().render();
    text.lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(' '))
        .unwrap_or_else(|| panic!("{name} missing from:\n{text}"))
        .parse()
        .unwrap()
}

/// Hits and misses so far, and what is held.
fn counts(server: &TestServer) -> (u64, u64, u64) {
    (
        metric(server, "s3dal_cache_hits_total"),
        metric(server, "s3dal_cache_misses_total"),
        metric(server, "s3dal_cache_bytes"),
    )
}

/// `expected` where the server caches, nothing at all where it does not.
fn expect(server: &TestServer, expected: (u64, u64, u64)) {
    let expected = if caches(server) { expected } else { (0, 0, 0) };
    assert_eq!(counts(server), expected, "(hits, misses, bytes)");
}

async fn start(settings: Settings) -> TestServer {
    let server = TestServer::start_with(settings).await;
    server
        .client
        .create_bucket()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    server
}

/// Content that differs at every offset a test reads, so a misplaced slice shows.
fn content(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

async fn put(client: &Client, key: &str, body: Vec<u8>) -> String {
    client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .content_type("application/x-test")
        .metadata("origin", "cache-test")
        .body(ByteStream::from(body))
        .send()
        .await
        .unwrap()
        .version_id()
        .unwrap_or_default()
        .to_owned()
}

/// What a client can observe of a GET.
#[derive(Debug, PartialEq)]
struct Got {
    body: Vec<u8>,
    content_length: Option<i64>,
    content_range: Option<String>,
    parts_count: Option<i32>,
    e_tag: Option<String>,
    checksum_crc32: Option<String>,
    checksum_type: Option<String>,
    content_type: Option<String>,
    last_modified: Option<DateTime>,
    metadata: Option<HashMap<String, String>>,
    version_id: Option<String>,
}

#[derive(Clone, Copy, Default)]
struct Ask<'a> {
    range: Option<&'a str>,
    part: Option<i32>,
    version: Option<&'a str>,
}

/// A GET as current SDKs send it, with checksums asked for; the error code on failure.
async fn get(client: &Client, key: &str, ask: Ask<'_>) -> Result<Got, String> {
    let out = client
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .set_range(ask.range.map(str::to_owned))
        .set_part_number(ask.part)
        .set_version_id(ask.version.map(str::to_owned))
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .map_err(|e| e.code().unwrap_or("none").to_owned())?;
    Ok(Got {
        content_length: out.content_length(),
        content_range: out.content_range().map(str::to_owned),
        parts_count: out.parts_count(),
        e_tag: out.e_tag().map(str::to_owned),
        checksum_crc32: out.checksum_crc32().map(str::to_owned),
        checksum_type: out.checksum_type().map(|t| t.as_str().to_owned()),
        content_type: out.content_type().map(str::to_owned),
        last_modified: out.last_modified().copied(),
        metadata: out.metadata().cloned(),
        version_id: out.version_id().map(str::to_owned),
        body: out.body.collect().await.unwrap().into_bytes().to_vec(),
    })
}

async fn body(client: &Client, key: &str) -> Result<Vec<u8>, String> {
    get(client, key, Ask::default()).await.map(|got| got.body)
}

#[tokio::test]
async fn an_overwrite_by_another_instance_is_never_served_stale() {
    // Several instances may share one backend (design.md), and other tools may write to it:
    // a cache that answered from what it held would hand out an object's old content after
    // another instance replaced it, for as long as the entry lived. The stat every GET makes
    // names the current ETag, and only an entry of that ETag may answer.
    let a = start(with_cache(64 << 20)).await;
    let b = TestServer::start_on(Backend::sharing(&a), Settings::default()).await;
    // The same length, so nothing but the content tells the two apart.
    let first = b"first-version-AAAA".to_vec();
    let second = b"other-version-BBBB".to_vec();
    put(&a.client, "k", first.clone()).await;
    assert_eq!(body(&a.client, "k").await.unwrap(), first);
    assert_eq!(body(&a.client, "k").await.unwrap(), first);
    expect(&a, (1, 1, first.len() as u64));

    put(&b.client, "k", second.clone()).await;
    assert_eq!(body(&a.client, "k").await.unwrap(), second);
    assert_eq!(body(&a.client, "k").await.unwrap(), second);
    // The new version replaced the old one's entry rather than adding to it.
    expect(&a, (2, 2, second.len() as u64));

    // Written behind every instance's back, as another tool would.
    a.plant(BUCKET, "k", &first).await;
    assert_eq!(body(&a.client, "k").await.unwrap(), first);

    b.client
        .delete_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(body(&a.client, "k").await.unwrap_err(), "NoSuchKey");
}

#[tokio::test]
async fn ranges_and_parts_from_the_cache_equal_an_uncached_read() {
    // A cached object is sliced in memory, where an uncached one is read by range from the
    // backend; SDK transfer managers and media players resume and seek by Range and partNumber,
    // so a slice off by one, or a header that differs, corrupts what they assemble.
    let cached = start(with_cache(64 << 20)).await;
    let plain = TestServer::start_on(Backend::sharing(&cached), Settings::default()).await;
    let data = content(1000, 7);
    put(&cached.client, "single", data.clone()).await;
    // A multipart object of one part (any other part would have to be at least 5 MiB, past
    // the per-object limit): its part is addressed by number with a part count.
    let upload = cached
        .client
        .create_multipart_upload()
        .bucket(BUCKET)
        .key("multi")
        .send()
        .await
        .unwrap();
    let id = upload.upload_id().unwrap();
    let part = cached
        .client
        .upload_part()
        .bucket(BUCKET)
        .key("multi")
        .upload_id(id)
        .part_number(1)
        .body(ByteStream::from(content(777, 3)))
        .send()
        .await
        .unwrap();
    cached
        .client
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key("multi")
        .upload_id(id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(part.e_tag().unwrap())
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    let ranges = [
        "bytes=0-0",
        "bytes=10-99",
        "bytes=-17",
        "bytes=500-",
        "bytes=990-5000",
        "bytes=0-999",
        "bytes=999-",
        "bytes=5000-",
    ];
    let mut asks: Vec<(&str, Ask)> = Vec::new();
    for key in ["single", "multi"] {
        asks.push((key, Ask::default()));
        for part in [1, 2] {
            let part = Some(part);
            asks.push((
                key,
                Ask {
                    part,
                    ..Ask::default()
                },
            ));
        }
        for range in ranges {
            let range = Some(range);
            asks.push((
                key,
                Ask {
                    range,
                    ..Ask::default()
                },
            ));
        }
    }
    // Each twice: the first fills the entry (whatever it asks for, the whole object is read),
    // the second is answered from it.
    let mut lookups = 0;
    for round in 0..2 {
        for (key, ask) in &asks {
            let want = get(&plain.client, key, *ask).await;
            let got = get(&cached.client, key, *ask).await;
            assert_eq!(
                got, want,
                "{key} range {:?} part {:?}, round {round}",
                ask.range, ask.part
            );
            // A partNumber past the count and a Range past the end fail before the cache is
            // asked.
            lookups += u64::from(got.is_ok());
        }
    }
    // Those refused in both rounds: part 2 and `bytes=5000-` of each, and of the 777 bytes
    // also `bytes=990-5000` and `bytes=999-`. A wrong slice bound would refuse or serve
    // another set, which the comparison above shows first.
    assert_eq!(lookups, 2 * (asks.len() as u64 - 6));
    // One miss per object, the first ask of it.
    expect(&cached, (lookups - 2, 2, 1000 + 777));
}

#[tokio::test]
async fn objects_over_the_per_object_limit_are_not_cached() {
    // One large object would hold the room of many small ones, and its GET gains least: its
    // time goes to the transfer, not to the backend's first byte. A Range miss reads the whole
    // object to fill the entry, which the limit keeps small.
    let server = start(with_cache(64 << 20)).await;
    put(&server.client, "at-limit", content(MAX_OBJECT, 1)).await;
    put(&server.client, "over-limit", content(MAX_OBJECT + 1, 2)).await;
    // The fill reads the whole object before answering, so its backend read is counted by the
    // time the response is in.
    assert_eq!(
        body(&server.client, "at-limit").await.unwrap(),
        content(MAX_OBJECT, 1)
    );
    let before = backend_reads(&server);
    assert_eq!(
        body(&server.client, "at-limit").await.unwrap(),
        content(MAX_OBJECT, 1)
    );
    // A hit is what saves the backend read.
    assert_eq!(backend_reads(&server), before);
    for _ in 0..2 {
        let over = body(&server.client, "over-limit").await.unwrap();
        assert_eq!(over, content(MAX_OBJECT + 1, 2));
    }
    // The object over the limit is neither looked up nor held.
    expect(&server, (1, 1, MAX_OBJECT as u64));
    if caches(&server) {
        // Streamed from the backend each time; a streamed read is counted when the stream is
        // dropped, which may come just after the client has the last byte.
        let mut reads = backend_reads(&server) - before;
        for _ in 0..50 {
            if reads >= 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            reads = backend_reads(&server) - before;
        }
        assert!(reads >= 2, "{reads} backend reads for two GETs");
    }
}

/// Backend reads the server made so far (OpenDAL's, so fs's own small reads are not among them).
/// A labeled series appears once it has counted something, so none is none yet.
fn backend_reads(server: &TestServer) -> u64 {
    let name = "s3dal_backend_operation_duration_seconds_count{operation=\"read\"} ";
    let text = server.dal.metrics().render();
    text.lines()
        .find_map(|line| line.strip_prefix(name))
        .map_or(0, |n| n.parse().unwrap())
}

#[tokio::test]
async fn the_budget_holds_under_eviction_least_recently_used_first() {
    // `cache-size` is memory the operator gave; holding more would let a busy server grow
    // until the OS kills it. What goes is what was read longest ago, so the objects in use
    // stay.
    let budget = 10_000;
    let server = start(with_cache(budget)).await;
    for i in 0..5 {
        put(&server.client, &format!("k{i}"), content(3000, i)).await;
    }
    put(&server.client, "too-big", content(12_000, 9)).await;
    async fn read(server: &TestServer, i: u8, budget: u64) {
        let key = format!("k{i}");
        assert_eq!(body(&server.client, &key).await.unwrap(), content(3000, i));
        assert!(metric(server, "s3dal_cache_bytes") <= budget);
    }
    for i in 0..5 {
        read(&server, i, budget).await;
    }
    // Three fit: k2, k3 and k4, in the order they were used.
    expect(&server, (0, 5, 9000));
    // An object larger than the whole budget is not cached, and evicts nothing.
    assert_eq!(
        body(&server.client, "too-big").await.unwrap(),
        content(12_000, 9)
    );
    expect(&server, (0, 5, 9000));
    read(&server, 2, budget).await; // hit; k3 is now the least recently used
    read(&server, 0, budget).await; // miss; evicts k3
    read(&server, 4, budget).await; // hit
    read(&server, 2, budget).await; // hit
    read(&server, 3, budget).await; // miss; evicts k0
    expect(&server, (3, 7, 9000));
    let evictions = metric(&server, "s3dal_cache_evictions_total");
    assert_eq!(evictions, if caches(&server) { 4 } else { 0 });
}

#[tokio::test]
async fn a_version_read_by_id_with_the_cache_on_is_that_version() {
    // Each version of a key is an object of its own in the backend, and two versions with the
    // same content have the same ETag: a cache keyed by the ETag alone, or by the key, would
    // answer a version id with another version's bytes or headers.
    let server = start(with_cache(64 << 20)).await;
    server
        .client
        .put_bucket_versioning()
        .bucket(BUCKET)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let alpha = b"alpha".to_vec();
    let beta = b"beta-content".to_vec();
    let v1 = put(&server.client, "k", alpha.clone()).await;
    let v2 = put(&server.client, "k", beta.clone()).await;
    let v3 = put(&server.client, "k", alpha.clone()).await;
    for _ in 0..2 {
        check_version(&server, None, &alpha, &v3).await;
        check_version(&server, Some(&v1), &alpha, &v1).await;
        check_version(&server, Some(&v2), &beta, &v2).await;
        check_version(&server, Some(&v3), &alpha, &v3).await;
    }
    if caches(&server) {
        assert!(metric(&server, "s3dal_cache_hits_total") >= 4);
    }
    // A delete marker hides the key; the versions stay readable by id.
    server
        .client
        .delete_object()
        .bucket(BUCKET)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(body(&server.client, "k").await.unwrap_err(), "NoSuchKey");
    check_version(&server, Some(&v3), &alpha, &v3).await;
    check_version(&server, Some(&v2), &beta, &v2).await;
}

/// GET `k`, of version `version` or the latest, answers `want` as version `id`.
async fn check_version(server: &TestServer, version: Option<&str>, want: &[u8], id: &str) {
    let ask = Ask {
        version,
        ..Default::default()
    };
    let got = get(&server.client, "k", ask).await.unwrap();
    assert_eq!(
        (got.body.as_slice(), got.version_id.as_deref()),
        (want, Some(id)),
        "{version:?}"
    );
}

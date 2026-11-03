//! The client subcommands (`s3dal ls`, `cp`, ...), run as the built binary against a real
//! server, as an operator runs them.

mod common;

use std::path::Path;
use std::process::{Output, Stdio};

use aws_sdk_s3::primitives::ByteStream;
use common::{ACCESS_KEY, SECRET_KEY, TestServer};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

/// The smallest part S3 accepts, used as both threshold and part size so a multipart upload
/// takes a file of a few MiB rather than the default 64.
const PART: usize = 5 * 1024 * 1024;

/// `s3dal <args>` against `server`, with `stdin` as its input. The environment's `S3DAL_*`
/// variables are dropped: `scripts/with-rustfs.sh` exports some for the whole test run, and
/// the client reads the server's variables.
async fn run_with_stdin(server: &TestServer, args: &[&str], stdin: &[u8]) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_s3dal"));
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("S3DAL_") {
            cmd.env_remove(name);
        }
    }
    cmd.args(args)
        .args(["--endpoint", &server.endpoint])
        .args(["--access-key", ACCESS_KEY, "--secret-key", SECRET_KEY])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn().unwrap();
    let mut input = child.stdin.take().unwrap();
    input.write_all(stdin).await.unwrap();
    drop(input);
    child.wait_with_output().await.unwrap()
}

async fn run(server: &TestServer, args: &[&str]) -> Output {
    run_with_stdin(server, args, b"").await
}

/// The stdout of a run that must succeed.
async fn ok(server: &TestServer, args: &[&str]) -> String {
    let out = run(server, args).await;
    assert!(
        out.status.success(),
        "s3dal {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

/// The stderr of a run that must fail.
async fn fails(server: &TestServer, args: &[&str]) -> String {
    let out = run(server, args).await;
    assert!(!out.status.success(), "s3dal {args:?} succeeded");
    String::from_utf8(out.stderr).unwrap()
}

/// Content that differs at every offset, so a misplaced part or a dropped chunk shows.
fn content(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 31 % 251) as u8).collect()
}

async fn put(server: &TestServer, bucket: &str, key: &str, data: &[u8]) {
    server
        .client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(data.to_vec()))
        .send()
        .await
        .unwrap();
}

async fn get(server: &TestServer, bucket: &str, key: &str) -> Vec<u8> {
    let out = server
        .client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    out.body.collect().await.unwrap().to_vec()
}

async fn keys(server: &TestServer, bucket: &str) -> Vec<String> {
    let out = server
        .client
        .list_objects_v2()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
    out.contents()
        .iter()
        .filter_map(|o| o.key().map(str::to_owned))
        .collect()
}

fn path_arg(path: &Path) -> &str {
    path.to_str().unwrap()
}

#[tokio::test]
async fn buckets_are_made_listed_and_removed() {
    // The first thing an operator does with a fresh deployment is make a bucket and see it.
    let server = TestServer::start().await;
    assert_eq!(
        ok(&server, &["mb", "s3://alpha"]).await,
        "make_bucket: alpha\n"
    );
    let listing = ok(&server, &["ls"]).await;
    assert!(listing.lines().any(|l| l.ends_with(" alpha")), "{listing}");

    put(&server, "alpha", "dir/a", b"a").await;
    // A bucket with objects stays unless --force says to empty it first.
    let err = fails(&server, &["rb", "s3://alpha"]).await;
    assert!(err.contains("BucketNotEmpty"), "{err}");
    let removed = ok(&server, &["rb", "s3://alpha", "--force"]).await;
    assert_eq!(removed, "delete: s3://alpha/dir/a\nremove_bucket: alpha\n");
    assert!(!ok(&server, &["ls"]).await.contains("alpha"));
}

#[tokio::test]
async fn ls_shows_one_level_unless_recursive_and_follows_continuation_tokens() {
    // Scripts parse this output as they parse the aws CLI's, and a listing that stopped at the
    // first page would silently hide every object after the thousandth.
    let server = TestServer::start().await;
    server.create_bucket_dir("bkt").await;
    put(&server, "bkt", "top.txt", b"12345").await;
    put(&server, "bkt", "dir/inner.txt", b"1").await;

    let listing = ok(&server, &["ls", "s3://bkt"]).await;
    let lines: Vec<&str> = listing.lines().collect();
    assert_eq!(lines.len(), 2, "{listing}");
    assert_eq!(lines[0], format!("{:>30} dir/", "PRE"));
    // `YYYY-MM-DD HH:MM:SS`, the size right-aligned in 10 columns, the key.
    let (date, rest) = lines[1].split_at(19);
    assert_eq!(date.len(), "2026-11-03 12:00:00".len());
    assert_eq!(rest, format!(" {:>10} top.txt", 5));

    // Within a prefix, names are relative to it.
    let inner = ok(&server, &["ls", "s3://bkt/dir/"]).await;
    assert!(inner.ends_with(" inner.txt\n"), "{inner}");

    let many = 1001;
    let planted = &server;
    futures::future::join_all((0..many).map(|i| async move {
        planted.plant("bkt", &format!("many/{i:04}"), b"x").await;
    }))
    .await;
    let recursive = ok(&server, &["ls", "-r", "s3://bkt/many/"]).await;
    assert_eq!(recursive.lines().count(), many, "{recursive}");
    assert!(recursive.lines().all(|l| l.contains(" many/")));
}

#[tokio::test]
async fn uploads_and_downloads_round_trip_including_multipart() {
    // The point of `cp` is that what comes back is what went up, and large files must go up
    // in parts: a single PUT is capped at 5 GiB, and a failure would restart it from zero.
    let server = TestServer::start().await;
    server.create_bucket_dir("bkt").await;
    let dir = tempfile::tempdir().unwrap();
    let small = dir.path().join("small.bin");
    let large = dir.path().join("large.bin");
    std::fs::write(&small, content(1000)).unwrap();
    std::fs::write(&large, content(2 * PART + 12345)).unwrap();
    let threshold = PART.to_string();
    let sizes = [
        "--multipart-threshold",
        &threshold,
        "--part-size",
        &threshold,
    ];

    // A bucket or a trailing `/` as the destination keeps the file's name.
    let up = ok(
        &server,
        &[&["cp", path_arg(&small), "s3://bkt/"][..], &sizes].concat(),
    )
    .await;
    assert_eq!(
        up,
        format!("upload: {} to s3://bkt/small.bin\n", small.display())
    );
    ok(
        &server,
        &[
            &["cp", path_arg(&large), "s3://bkt/big/large.bin"][..],
            &sizes,
        ]
        .concat(),
    )
    .await;
    assert_eq!(get(&server, "bkt", "small.bin").await, content(1000));
    assert_eq!(
        get(&server, "bkt", "big/large.bin").await,
        content(2 * PART + 12345)
    );
    let head = server
        .client
        .head_object()
        .bucket("bkt")
        .key("big/large.bin")
        .send()
        .await
        .unwrap();
    // A multipart ETag ends in the number of parts.
    assert!(head.e_tag().unwrap().ends_with("-3\""), "{head:?}");

    let down = tempfile::tempdir().unwrap();
    let target = down.path().join("nested/copy.bin");
    ok(
        &server,
        &["cp", "s3://bkt/big/large.bin", path_arg(&target)],
    )
    .await;
    assert_eq!(std::fs::read(&target).unwrap(), content(2 * PART + 12345));
    // An existing directory as the destination keeps the object's name.
    ok(
        &server,
        &["cp", "s3://bkt/small.bin", path_arg(down.path())],
    )
    .await;
    assert_eq!(
        std::fs::read(down.path().join("small.bin")).unwrap(),
        content(1000)
    );
    // Nothing but the two files: no temporary file is left behind.
    let mut names: Vec<_> = std::fs::read_dir(down.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    assert_eq!(names, ["nested", "small.bin"]);
}

#[tokio::test]
async fn stdin_and_stdout_stream_through_cp_and_cat() {
    // Pipelines (`tar c . | s3dal cp - s3://bkt/backup.tar`) have no file to name, and a
    // stream larger than the threshold must still go up whole.
    let server = TestServer::start().await;
    server.create_bucket_dir("bkt").await;
    let threshold = PART.to_string();
    let data = content(PART + 777);
    let args = [
        "cp",
        "-",
        "s3://bkt/piped",
        "--multipart-threshold",
        &threshold,
        "--part-size",
        &threshold,
    ];
    let out = run_with_stdin(&server, &args, &data).await;
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    // Nothing is said on stdout, which may be part of the pipeline.
    assert!(out.stdout.is_empty());
    assert_eq!(get(&server, "bkt", "piped").await, data);

    let small = run_with_stdin(&server, &["cp", "-", "s3://bkt/small"], b"hello").await;
    assert!(small.status.success());
    assert_eq!(get(&server, "bkt", "small").await, b"hello");

    let cat = run(&server, &["cat", "s3://bkt/piped"]).await;
    assert!(cat.status.success());
    assert_eq!(cat.stdout, data);
    let to_stdout = run(&server, &["cp", "s3://bkt/small", "-"]).await;
    assert_eq!(to_stdout.stdout, b"hello");
}

#[tokio::test]
async fn recursive_copies_keep_relative_paths_both_ways() {
    // A directory tree must arrive with its structure intact, in either direction.
    let server = TestServer::start().await;
    server.create_bucket_dir("bkt").await;
    let src = tempfile::tempdir().unwrap();
    let files = [
        ("a.txt", "a"),
        ("sub/b.txt", "bb"),
        ("sub/deeper/c.txt", "ccc"),
    ];
    for (rel, data) in files {
        let path = src.path().join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, data).unwrap();
    }

    ok(
        &server,
        &["cp", "-r", path_arg(src.path()), "s3://bkt/tree"],
    )
    .await;
    assert_eq!(
        keys(&server, "bkt").await,
        ["tree/a.txt", "tree/sub/b.txt", "tree/sub/deeper/c.txt"]
    );

    let dst = tempfile::tempdir().unwrap();
    ok(
        &server,
        &["cp", "-r", "s3://bkt/tree/", path_arg(dst.path())],
    )
    .await;
    for (rel, data) in files {
        assert_eq!(std::fs::read_to_string(dst.path().join(rel)).unwrap(), data);
    }

    ok(&server, &["cp", "-r", "s3://bkt/tree", "s3://bkt/copy"]).await;
    assert_eq!(get(&server, "bkt", "copy/sub/deeper/c.txt").await, b"ccc");
}

#[tokio::test]
async fn recursive_rm_deletes_only_under_the_prefix() {
    // `rm -r s3://bkt/logs` must not take `logs-old/` with it, though the keys share `logs`.
    let server = TestServer::start().await;
    server.create_bucket_dir("bkt").await;
    for key in ["logs/1", "logs/sub/2", "logs-old/1"] {
        put(&server, "bkt", key, b"x").await;
    }
    let out = ok(&server, &["rm", "-r", "s3://bkt/logs"]).await;
    assert_eq!(out.lines().count(), 2, "{out}");
    assert_eq!(keys(&server, "bkt").await, ["logs-old/1"]);

    assert_eq!(
        ok(&server, &["rm", "s3://bkt/logs-old/1"]).await,
        "delete: s3://bkt/logs-old/1\n"
    );
    assert!(keys(&server, "bkt").await.is_empty());
}

#[tokio::test]
async fn mv_removes_the_source_once_copied() {
    // A move that left the source would double the data; one that removed it first could
    // lose it.
    let server = TestServer::start().await;
    server.create_bucket_dir("bkt").await;
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("f.txt");
    std::fs::write(&file, "moved").unwrap();

    ok(&server, &["mv", path_arg(&file), "s3://bkt/f.txt"]).await;
    assert!(!file.exists());
    ok(&server, &["mv", "s3://bkt/f.txt", "s3://bkt/g.txt"]).await;
    assert_eq!(keys(&server, "bkt").await, ["g.txt"]);
    assert_eq!(get(&server, "bkt", "g.txt").await, b"moved");
    // Onto itself, a move would delete the only copy.
    let err = fails(&server, &["mv", "s3://bkt/g.txt", "s3://bkt/g.txt"]).await;
    assert!(err.contains("onto itself"), "{err}");
    assert_eq!(get(&server, "bkt", "g.txt").await, b"moved");
}

#[tokio::test]
async fn stat_shows_what_head_object_returns() {
    // Checking an object's size, type and metadata should not need another tool.
    let server = TestServer::start().await;
    server.create_bucket_dir("bkt").await;
    server
        .client
        .put_object()
        .bucket("bkt")
        .key("doc.txt")
        .content_type("text/plain")
        .metadata("owner", "ops")
        .metadata("note", "café")
        .body(ByteStream::from_static(b"hello world"))
        .send()
        .await
        .unwrap();
    let out = ok(&server, &["stat", "s3://bkt/doc.txt"]).await;
    assert!(out.contains("Size          : 11\n"), "{out}");
    assert!(out.contains("Content-Type  : text/plain\n"), "{out}");
    assert!(out.contains("ETag          : \""), "{out}");
    assert!(out.contains("  owner: ops\n"), "{out}");
    // Shown as set, not in the RFC 2047 form non-ASCII values travel in.
    assert!(out.contains("  note: café\n"), "{out}");
}

#[tokio::test]
async fn presigned_urls_grant_access_without_credentials() {
    // A presigned URL is handed to someone without keys: curl or a browser must be able to use
    // it as it is.
    let server = TestServer::start().await;
    server.create_bucket_dir("bkt").await;
    put(&server, "bkt", "shared.bin", &content(4096)).await;
    let http = reqwest::Client::new();

    let url = ok(
        &server,
        &["presign", "s3://bkt/shared.bin", "--expires", "10m"],
    )
    .await;
    let resp = http.get(url.trim()).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.bytes().await.unwrap(), content(4096));

    let url = ok(&server, &["presign", "--put", "s3://bkt/upload.txt"]).await;
    let resp = http.put(url.trim()).body("via url").send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(get(&server, "bkt", "upload.txt").await, b"via url");
}

#[tokio::test]
async fn errors_exit_non_zero_with_the_s3_error_code() {
    // Scripts stop on the exit status, and the operator needs S3's own code to know what went
    // wrong.
    let server = TestServer::start().await;
    server.create_bucket_dir("bkt").await;
    let err = fails(&server, &["cat", "s3://bkt/missing"]).await;
    assert!(err.contains("NoSuchKey"), "{err}");
    let err = fails(&server, &["ls", "s3://nonexistent"]).await;
    assert!(err.contains("NoSuchBucket"), "{err}");
    assert!(err.contains("request id"), "{err}");
    // HEAD responses have no body, so the status is all there is to show.
    let err = fails(&server, &["stat", "s3://bkt/missing"]).await;
    assert!(err.contains("404"), "{err}");
    let dir = tempfile::tempdir().unwrap();
    let err = fails(&server, &["cp", "s3://bkt/missing", path_arg(dir.path())]).await;
    assert!(err.contains("NoSuchKey"), "{err}");
    // A failed download leaves nothing behind.
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);

    // Wrong keys are refused by the server, not mistaken for an empty listing.
    let out = Command::new(env!("CARGO_BIN_EXE_s3dal"))
        .args(["ls", "--endpoint", &server.endpoint])
        .args(["--access-key", ACCESS_KEY, "--secret-key", "wrong"])
        .env_remove("S3DAL_ACCESS_KEY")
        .env_remove("S3DAL_SECRET_KEY")
        .output()
        .await
        .unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("SignatureDoesNotMatch"), "{err}");
}

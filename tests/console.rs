mod common;

use std::sync::Arc;

use aws_sdk_s3::error::ProvideErrorMetadata;
use common::{
    ACCESS_KEY, Backend, SECRET_KEY, client, raw_request, raw_response, signed_headers,
    signed_headers_as,
};
use s3dal::config::Cli;
use s3dal::console::Console;
use s3dal::live::Live;
use s3dal::{S3Dal, server};
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

/// A server with the console on, its settings from a config file it can edit.
struct ConsoleServer {
    s3: String,
    console: String,
    file: tempfile::NamedTempFile,
    live: Arc<Live>,
    dal: S3Dal,
    _backend: Backend,
    _tier: Option<Backend>,
    _stop: oneshot::Sender<()>,
}

const CONFIG: &str = "# Written by hand; the console must keep this comment.\n\
                      backend = \"fs\"\n\n\
                      [[credentials]]\naccess-key = \"s3dal-test-access\"\n\
                      secret-key = \"s3dal-test-secret\"\n";

impl ConsoleServer {
    async fn start(text: &str, extra_args: &[&str]) -> Self {
        Self::launch(text, extra_args, false).await
    }

    /// With one cold tier on fs, `hdd`, for STANDARD_IA.
    async fn start_tiered(text: &str) -> Self {
        Self::launch(text, &[], true).await
    }

    async fn launch(text: &str, extra_args: &[&str], tiered: bool) -> Self {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), text).unwrap();
        // `--backend` as a flag: scripts/with-rustfs.sh exports S3DAL_BACKEND for the whole run.
        let mut argv: Vec<std::ffi::OsString> = vec![
            "s3dal".into(),
            "serve".into(),
            "--backend=fs".into(),
            "--config".into(),
            file.path().into(),
        ];
        argv.extend(extra_args.iter().map(Into::into));
        let args = Cli::serve_from(&argv).unwrap();
        let backend = Backend::fs().await;
        let tier = if tiered {
            Some(Backend::fs().await)
        } else {
            None
        };
        let mut tiers = Vec::new();
        if let Some(t) = &tier {
            tiers.push(
                s3dal::Tier::open(
                    "hdd",
                    "STANDARD_IA",
                    t.op.clone(),
                    &backend.op,
                    &backend.profile,
                )
                .await
                .unwrap(),
            );
        }
        let dal =
            S3Dal::new(backend.op.clone(), backend.profile, args.settings()).with_tiers(tiers);
        let auth = server::LiveAuth::new(&args.credentials, &args.domain).unwrap();
        let live = Arc::new(Live::new(argv, args, dal.clone(), auth.clone(), None));
        let service = server::build_service(dal.clone(), &auth, Some(live.clone()));

        let s3 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pages = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (s3_addr, console_addr) = (s3.local_addr().unwrap(), pages.local_addr().unwrap());
        let (stop, stopped) = oneshot::channel::<()>();
        let stopped = futures::FutureExt::shared(async move {
            let _ = stopped.await;
        });
        tokio::spawn(server::serve(s3, service.clone(), None, stopped.clone()));
        let console = Arc::new(Console::new(live.clone()));
        tokio::spawn(server::serve_console(
            pages, service, console, None, stopped,
        ));
        Self {
            s3: format!("http://{s3_addr}"),
            console: format!("http://{console_addr}"),
            file,
            live,
            dal,
            _backend: backend,
            _tier: tier,
            _stop: stop,
        }
    }

    async fn api(&self, method: &str, path: &str, body: &str) -> (u16, String) {
        console_api(&self.console, method, path, body).await
    }

    fn file_text(&self) -> String {
        std::fs::read_to_string(self.file.path()).unwrap()
    }
}

/// A request signed as the browser signs it: for the path the S3 service sees, sent under the
/// console's `/s3` prefix.
async fn console_api(console: &str, method: &str, path: &str, body: &str) -> (u16, String) {
    let payload: String = Sha256::digest(body.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let headers = signed_headers(console, method, path, "", &[], &payload);
    let headers: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let (status, body) = raw_request(
        console,
        method,
        &format!("/s3{path}"),
        &headers,
        body.as_bytes(),
    )
    .await;
    (status, String::from_utf8_lossy(&body).into_owned())
}

#[tokio::test]
async fn console_pages_allow_no_script_from_elsewhere() {
    // The page holds the secret key in memory; a script injected through an object name or
    // loaded from another origin could read it.
    let server = ConsoleServer::start(CONFIG, &[]).await;
    for path in ["/", "/app.js", "/b/some/deep/link"] {
        let (status, head, _) = raw_response(&server.console, "GET", path, &[], b"").await;
        assert_eq!(status, 200, "{path}");
        let head = head.to_ascii_lowercase();
        assert!(
            head.contains("content-security-policy: default-src 'none'; script-src 'self'"),
            "{path}: {head}"
        );
        assert!(head.contains("x-content-type-options: nosniff"), "{path}");
    }
    // Any other path is the page, which routes in the browser and is told the region to sign
    // for.
    let (_, body) = raw_request(&server.console, "GET", "/b/x", &[], b"").await;
    let body = String::from_utf8_lossy(&body);
    assert!(
        body.contains("<meta name=\"s3dal-region\" content=\"us-east-1\""),
        "{body}"
    );
}

#[tokio::test]
async fn objects_served_on_the_console_origin_run_no_script() {
    // An uploaded HTML file opened from the console must not run on the origin that holds the
    // secret key.
    let server = ConsoleServer::start(CONFIG, &[]).await;
    let s3 = client(&server.s3, ACCESS_KEY, SECRET_KEY);
    s3.create_bucket().bucket("bkt").send().await.unwrap();
    s3.put_object()
        .bucket("bkt")
        .key("page.html")
        .content_type("text/html")
        .body(b"<script>alert(1)</script>".to_vec().into())
        .send()
        .await
        .unwrap();
    let headers = signed_headers(
        &server.console,
        "GET",
        "/bkt/page.html",
        "",
        &[],
        "UNSIGNED-PAYLOAD",
    );
    let headers: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let (status, head, _) =
        raw_response(&server.console, "GET", "/s3/bkt/page.html", &headers, b"").await;
    assert_eq!(status, 200, "{head}");
    assert!(
        head.to_ascii_lowercase()
            .contains("content-security-policy: sandbox"),
        "{head}"
    );
}

#[tokio::test]
async fn the_settings_api_answers_only_through_the_console() {
    // The S3 port is the plain S3 API, as it was before the console existed: a settings API
    // there would widen what every exposed S3 endpoint offers.
    let server = ConsoleServer::start(CONFIG, &[]).await;
    let (status, body) = server.api("GET", "/_s3dal/admin/info", "").await;
    assert_eq!(status, 200, "{body}");
    assert!(
        body.contains(&format!("\"version\":\"{}\"", env!("CARGO_PKG_VERSION"))),
        "{body}"
    );

    let headers = signed_headers(
        &server.s3,
        "GET",
        "/_s3dal/admin/info",
        "",
        &[],
        "UNSIGNED-PAYLOAD",
    );
    let headers: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let (status, body) = raw_request(&server.s3, "GET", "/_s3dal/admin/info", &headers, b"").await;
    assert_ne!(status, 200, "{}", String::from_utf8_lossy(&body));

    // And it takes a signature like any S3 request.
    let (status, _) = raw_request(&server.console, "GET", "/s3/_s3dal/admin/info", &[], b"").await;
    assert_eq!(status, 403);
}

#[tokio::test]
async fn the_console_is_told_the_tiers_and_the_cold_bytes() {
    // The pages offer a tier's class to uploads, lifecycle rules and "Change storage class",
    // which any pair reaching the bucket may use, so the bucket's settings list the tiers; the
    // server page shows what each tier is and what its sweep found, and the dashboard splits a
    // bucket's bytes into hot and cold.
    let server = ConsoleServer::start_tiered(CONFIG).await;
    let s3 = client(&server.s3, ACCESS_KEY, SECRET_KEY);
    s3.create_bucket().bucket("data").send().await.unwrap();
    s3.put_object()
        .bucket("data")
        .key("hot")
        .body(vec![1; 10].into())
        .send()
        .await
        .unwrap();
    s3.put_object()
        .bucket("data")
        .key("cold")
        .storage_class(aws_sdk_s3::types::StorageClass::StandardIa)
        .body(vec![2; 20].into())
        .send()
        .await
        .unwrap();

    let (status, body) = server.api("GET", "/_s3dal/admin/buckets/data", "").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        json(&body)["tiers"],
        serde_json::json!([{ "name": "hdd", "storage_class": "STANDARD_IA" }]),
        "{body}"
    );

    server.dal.scan_usage().await.unwrap();
    let (_, body) = server.api("GET", "/_s3dal/admin/usage", "").await;
    let usage = &json(&body)["buckets"][0]["usage"];
    assert_eq!(
        (&usage["bytes"], &usage["cold_bytes"]),
        (&30.into(), &20.into()),
        "{body}"
    );

    let (_, body) = server.api("GET", "/_s3dal/admin/info", "").await;
    let tier = &json(&body)["tiers"][0];
    assert_eq!(
        (&tier["name"], &tier["backend"]),
        (&"hdd".into(), &"fs".into()),
        "{body}"
    );
    assert!(tier["profile"]["ordered_list"].is_boolean(), "{body}");
    // Before a sweep there is nothing to show, which is not the same as nothing cold.
    assert!(tier["swept"].is_null(), "{body}");
    server
        .dal
        .sweep_tiers(std::time::SystemTime::now())
        .await
        .unwrap();
    let (_, body) = server.api("GET", "/_s3dal/admin/info", "").await;
    assert_eq!(
        json(&body)["tiers"][0]["swept"],
        serde_json::json!({ "objects": 1, "bytes": 20, "dangling": 0 }),
        "{body}"
    );

    // A server without tiers offers none.
    let plain = ConsoleServer::start(CONFIG, &[]).await;
    client(&plain.s3, ACCESS_KEY, SECRET_KEY)
        .create_bucket()
        .bucket("data")
        .send()
        .await
        .unwrap();
    let (_, body) = plain.api("GET", "/_s3dal/admin/buckets/data", "").await;
    assert_eq!(json(&body)["tiers"], serde_json::json!([]), "{body}");
}

#[tokio::test]
async fn a_console_edit_is_written_to_the_file_and_applied_at_once() {
    // Adding a key pair from the console must work at once, survive a restart (it is in the
    // file), and leave what the operator wrote by hand in the file as it was.
    let server = ConsoleServer::start(CONFIG, &[]).await;
    let edit = r#"{"settings":{"stale-upload-age":"12h"},
        "credentials":[{"access_key":"s3dal-test-access","access":"read-write","expires":null},
                       {"access_key":"bob","secret_key":"bob-secret-key","access":"read-write","expires":null}]}"#;
    let (status, body) = server.api("PUT", "/_s3dal/admin/config", edit).await;
    assert_eq!(status, 200, "{body}");

    let text = server.file_text();
    assert!(
        text.starts_with("# Written by hand; the console must keep this comment."),
        "{text}"
    );
    assert!(text.contains("stale-upload-age = \"12h\""), "{text}");
    // The pair listed without a secret kept the one it had.
    assert!(
        text.contains("secret-key = \"s3dal-test-secret\""),
        "{text}"
    );
    let bob = client(&server.s3, "bob", "bob-secret-key");
    bob.list_buckets().send().await.unwrap();
    assert_eq!(server.live.current().stale_upload_age.as_secs(), 12 * 3600);
}

#[tokio::test]
async fn a_bucket_made_public_in_the_console_opens_at_once() {
    // The switch on the Buckets page saves the bucket's state; a public link copied right after
    // must work, on the S3 port and on the console's own origin, where the console builds it.
    // The config file is not where it lives: instances sharing the backend read the bucket's.
    let server = ConsoleServer::start(CONFIG, &[]).await;
    let s3 = client(&server.s3, ACCESS_KEY, SECRET_KEY);
    s3.create_bucket().bucket("site").send().await.unwrap();
    s3.put_object()
        .bucket("site")
        .key("a.txt")
        .body(b"hello".to_vec().into())
        .send()
        .await
        .unwrap();
    let (status, _) = raw_request(&server.s3, "GET", "/site/a.txt", &[], b"").await;
    assert_eq!(status, 403);
    let (status, body) = server.api("GET", "/_s3dal/admin/buckets/site", "").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(json(&body)["public"], false, "{body}");

    let (status, body) = server
        .api("PUT", "/_s3dal/admin/buckets/site", r#"{"public":true}"#)
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(json(&body)["public"], true, "{body}");
    assert_eq!(server.file_text(), CONFIG);
    for (endpoint, path) in [
        (&server.s3, "/site/a.txt"),
        (&server.console, "/s3/site/a.txt"),
    ] {
        let (status, body) = raw_request(endpoint, "GET", path, &[], b"").await;
        assert_eq!((status, body.as_slice()), (200, &b"hello"[..]), "{path}");
    }

    // A bucket that does not exist cannot be made public ahead of its creation, and the old
    // setting is no longer one the settings take.
    let (status, body) = server
        .api("PUT", "/_s3dal/admin/buckets/later", r#"{"public":true}"#)
        .await;
    assert_eq!(status, 404, "{body}");
    let (status, body) = server
        .api(
            "PUT",
            "/_s3dal/admin/config",
            r#"{"settings":{"public-bucket":["site"]}}"#,
        )
        .await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(server.file_text(), CONFIG);
}

#[tokio::test]
async fn the_client_switches_a_bucket_public_without_a_browser() {
    // Where no browser reaches the console, as in a container, `s3dal public` makes the same
    // change, signed as the console's pages sign; without it, a deployment that set
    // `public-bucket` in its config would have no way to set the buckets again.
    let server = ConsoleServer::start(CONFIG, &[]).await;
    let s3 = client(&server.s3, ACCESS_KEY, SECRET_KEY);
    s3.create_bucket().bucket("site").send().await.unwrap();
    let run = |args: &'static [&'static str], endpoint: String| async move {
        let mut cmd = tokio::process::Command::new(env!("CARGO_BIN_EXE_s3dal"));
        // scripts/with-rustfs.sh exports S3DAL_* for the whole run, and the client reads them.
        for (name, _) in std::env::vars_os() {
            if name.to_string_lossy().starts_with("S3DAL_") {
                cmd.env_remove(name);
            }
        }
        let out = cmd
            .args(args)
            .args(["--endpoint", &endpoint])
            .args(["--access-key", ACCESS_KEY, "--secret-key", SECRET_KEY])
            .output()
            .await
            .unwrap();
        (
            out.status.success(),
            String::from_utf8_lossy(&out.stdout).into_owned(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    };
    let anonymous = |path: &'static str| raw_request(&server.s3, "GET", path, &[], b"");

    let (ok, out, err) = run(&["public", "s3://site", "on"], server.console.clone()).await;
    assert!(ok, "{err}");
    assert_eq!(out, "site: public\n");
    // Public: the missing object is reported, where a private bucket refuses the question.
    assert_eq!(anonymous("/site/missing").await.0, 404);
    let (ok, out, err) = run(&["public", "s3://site"], server.console.clone()).await;
    assert!(ok, "{err}");
    assert_eq!(out, "site: public\n");
    let (ok, out, err) = run(&["public", "s3://site", "off"], server.console.clone()).await;
    assert!(ok, "{err}");
    assert_eq!(out, "site: private\n");
    assert_eq!(anonymous("/site/missing").await.0, 403);

    // The S3 port has no settings API; the error says where it is.
    let (ok, _, err) = run(&["public", "s3://site", "on"], server.s3.clone()).await;
    assert!(!ok);
    assert!(err.contains("console"), "{err}");
    assert_eq!(anonymous("/site/missing").await.0, 403);
}

#[tokio::test]
async fn the_client_and_the_console_set_and_remove_a_bucket_quota() {
    // `s3dal quota` and the bucket page make the same change through the settings API; the
    // quota holds at once on the S3 port, and the answer shows what the bucket holds against it.
    let server = ConsoleServer::start(CONFIG, &[]).await;
    let s3 = client(&server.s3, ACCESS_KEY, SECRET_KEY);
    s3.create_bucket().bucket("data").send().await.unwrap();
    let put = |key: &'static str, len: usize| {
        s3.put_object()
            .bucket("data")
            .key(key)
            .body(vec![0; len].into())
            .send()
    };
    put("a", 1000).await.unwrap();
    let run = |args: &'static [&'static str]| {
        let endpoint = server.console.clone();
        async move {
            let mut cmd = tokio::process::Command::new(env!("CARGO_BIN_EXE_s3dal"));
            for (name, _) in std::env::vars_os() {
                if name.to_string_lossy().starts_with("S3DAL_") {
                    cmd.env_remove(name);
                }
            }
            let out = cmd
                .args(args)
                .args(["--endpoint", &endpoint])
                .args(["--access-key", ACCESS_KEY, "--secret-key", SECRET_KEY])
                .output()
                .await
                .unwrap();
            (
                out.status.success(),
                String::from_utf8_lossy(&out.stdout).into_owned(),
                String::from_utf8_lossy(&out.stderr).into_owned(),
            )
        }
    };

    let (ok, out, err) = run(&[
        "quota",
        "s3://data",
        "--max-bytes",
        "1KiB",
        "--max-objects",
        "5",
    ])
    .await;
    assert!(ok, "{err}");
    assert_eq!(out, "data: 1000 bytes of 1KiB, 1 of 5 objects\n");
    put("fits", 24).await.unwrap();
    let refused = put("over", 1).await.unwrap_err();
    assert_eq!(refused.code(), Some("QuotaExceeded"), "{refused:?}");
    let (ok, out, err) = run(&["quota", "s3://data"]).await;
    assert!(ok, "{err}");
    assert_eq!(out, "data: 1024 bytes of 1KiB, 2 of 5 objects\n");
    // The quota is the bucket's, kept in the backend, not in the config file.
    assert_eq!(server.file_text(), CONFIG);

    let (status, body) = server.api("GET", "/_s3dal/admin/usage", "").await;
    assert_eq!(status, 200, "{body}");
    let usage: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(usage["buckets"][0]["name"], "data", "{body}");
    assert_eq!(usage["buckets"][0]["usage"]["bytes"], 1024, "{body}");
    assert_eq!(usage["buckets"][0]["quota"]["max_bytes"], 1024, "{body}");

    // A quota the server cannot read whole is refused rather than half applied.
    let (status, body) = server
        .api(
            "PUT",
            "/_s3dal/admin/buckets/data",
            r#"{"quota":{"max_bytes":1,"max_parts":2}}"#,
        )
        .await;
    assert_eq!(status, 400, "{body}");
    let (ok, out, err) = run(&["quota", "s3://data", "--clear"]).await;
    assert!(ok, "{err}");
    assert_eq!(out, "data: 1024 bytes, 2 objects, no quota\n");
    put("over", 1).await.unwrap();
    // A limit on objects alone is not a quota s3dal keeps.
    let (ok, _, err) = run(&["quota", "s3://data", "--max-objects", "5"]).await;
    assert!(!ok);
    assert!(err.contains("--max-bytes"), "{err}");
}

#[tokio::test]
async fn a_key_pair_with_limits_reads_but_cannot_switch_the_public_state_of_its_buckets() {
    // The console shows such a pair whether a bucket it reaches is public, which is how it
    // offers a plain link; switching it, and other buckets' state, is for a pair without limits.
    let config = format!(
        "{CONFIG}\n[[credentials]]\naccess-key = \"viewer\"\nsecret-key = \"viewer-secret\"\n\
         allow = [\"photos\"]\naccess = \"read-only\"\n"
    );
    let server = ConsoleServer::start(&config, &[]).await;
    let s3 = client(&server.s3, ACCESS_KEY, SECRET_KEY);
    for bucket in ["photos", "other"] {
        s3.create_bucket().bucket(bucket).send().await.unwrap();
    }
    let (status, body) = server
        .api("PUT", "/_s3dal/admin/buckets/photos", r#"{"public":true}"#)
        .await;
    assert_eq!(status, 200, "{body}");
    let as_viewer = |method: &'static str, path: &'static str, body: &'static str| {
        let payload: String = Sha256::digest(body.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let headers = signed_headers_as(
            &server.console,
            method,
            path,
            "",
            &[],
            &payload,
            "viewer",
            "viewer-secret",
        );
        let console = server.console.clone();
        async move {
            let h: Vec<(&str, &str)> = headers
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            let (status, body) =
                raw_request(&console, method, &format!("/s3{path}"), &h, body.as_bytes()).await;
            (status, String::from_utf8_lossy(&body).into_owned())
        }
    };
    let (status, body) = as_viewer("GET", "/_s3dal/admin/buckets/photos", "").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(json(&body)["public"], true, "{body}");
    // The notification targets' URLs and delivery errors are the operator's, not a tenant's.
    assert_eq!(json(&body)["notify"], serde_json::Value::Null, "{body}");
    let (status, body) = as_viewer("GET", "/_s3dal/admin/buckets/other", "").await;
    assert_eq!(status, 403, "{body}");
    let (status, body) =
        as_viewer("PUT", "/_s3dal/admin/buckets/photos", r#"{"public":false}"#).await;
    assert_eq!(status, 403, "{body}");
    let (_, body) = server.api("GET", "/_s3dal/admin/buckets/photos", "").await;
    assert_eq!(json(&body)["public"], true, "{body}");
    // Nor can it set the bucket's quota.
    let (status, body) = as_viewer(
        "PUT",
        "/_s3dal/admin/buckets/photos",
        r#"{"quota":{"max_bytes":1}}"#,
    )
    .await;
    assert_eq!(status, 403, "{body}");
}

#[tokio::test]
async fn metrics_settings_from_the_console_apply_at_once_on_both_ports() {
    // A fresh server has a token mode and no token: no one scrapes until the operator sets
    // one, and the token, once set, never travels back to the page.
    let server = ConsoleServer::start(CONFIG, &[]).await;
    let ports = [&server.s3, &server.console];
    let status = |endpoint: &str, auth: Option<&str>| {
        let (endpoint, auth) = (endpoint.to_owned(), auth.map(str::to_owned));
        async move {
            let headers: Vec<(&str, &str)> = auth
                .as_deref()
                .map(|a| ("authorization", a))
                .into_iter()
                .collect();
            raw_request(&endpoint, "GET", "/_s3dal/metrics", &headers, b"")
                .await
                .0
        }
    };
    for port in ports {
        assert_eq!(status(port, None).await, 403);
    }

    let token = "console-set-token-0123";
    let edit = format!(r#"{{"settings":{{"metrics-token":"{token}"}}}}"#);
    let (code, body) = server.api("PUT", "/_s3dal/admin/config", &edit).await;
    assert_eq!(code, 200, "{body}");
    assert!(!body.contains(token), "{body}");
    let view: serde_json::Value = serde_json::from_str(&body).unwrap();
    let setting = view["settings"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == "metrics-token")
        .unwrap();
    assert_eq!(setting["secret"], true, "{setting}");
    assert!(setting["value"].is_null(), "{setting}");
    assert!(server.file_text().contains(token));
    let bearer = format!("Bearer {token}");
    for port in ports {
        assert_eq!(status(port, None).await, 401);
        assert_eq!(status(port, Some(&bearer)).await, 200);
    }
    let (_, info) = server.api("GET", "/_s3dal/admin/info", "").await;
    assert!(!info.contains(token), "{info}");

    let (code, body) = server
        .api(
            "PUT",
            "/_s3dal/admin/config",
            r#"{"settings":{"metrics":"off"}}"#,
        )
        .await;
    assert_eq!(code, 200, "{body}");
    for port in ports {
        assert_eq!(status(port, Some(&bearer)).await, 404);
    }
    let (code, body) = server
        .api(
            "PUT",
            "/_s3dal/admin/config",
            r#"{"settings":{"metrics":"public"}}"#,
        )
        .await;
    assert_eq!(code, 200, "{body}");
    for port in ports {
        assert_eq!(status(port, None).await, 200);
    }
    assert!(server.file_text().contains("metrics = \"public\""));
}

#[tokio::test]
async fn the_dashboard_stats_count_the_requests_of_the_last_interval() {
    // The dashboard draws these; a request made a moment ago must already be in them, and the
    // console's own calls are told apart from the S3 traffic.
    let server = ConsoleServer::start(CONFIG, &[]).await;
    let s3 = client(&server.s3, ACCESS_KEY, SECRET_KEY);
    s3.create_bucket().bucket("dash").send().await.unwrap();
    for key in ["a", "b"] {
        s3.put_object()
            .bucket("dash")
            .key(key)
            .body(vec![7; 1000].into())
            .send()
            .await
            .unwrap();
    }
    let (_, info) = server.api("GET", "/_s3dal/admin/info", "").await;
    assert!(info.contains("\"metrics\":\"token\""), "{info}");

    let sum = |stats: &serde_json::Value, path: &[&str]| -> u64 {
        stats["samples"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| path.iter().fold(s, |v, k| &v[*k]).as_u64().unwrap_or(0))
            .sum()
    };
    let mut stats = serde_json::Value::Null;
    for _ in 0..100 {
        let (code, body) = server.api("GET", "/_s3dal/admin/stats", "").await;
        assert_eq!(code, 200, "{body}");
        stats = serde_json::from_str(&body).unwrap();
        if sum(&stats, &["requests", "PutObject"]) == 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(stats["interval_secs"], 10);
    assert_eq!(sum(&stats, &["requests", "PutObject"]), 2, "{stats}");
    assert_eq!(sum(&stats, &["requests", "CreateBucket"]), 1, "{stats}");
    // The info call, and the stats calls before this one.
    assert!(sum(&stats, &["requests", "ConsoleAdmin"]) >= 1, "{stats}");
    assert!(sum(&stats, &["bytes_in"]) >= 2000, "{stats}");
    assert!(sum(&stats, &["status", "2xx"]) >= 3, "{stats}");
    let last = stats["samples"].as_array().unwrap().last().unwrap().clone();
    assert!(last["p99_ms"].is_number(), "{last}");
    if cfg!(any(target_os = "linux", target_os = "macos")) {
        assert!(last["rss_bytes"].as_u64().unwrap() > 0, "{last}");
    }

    // Only the console's listener answers it, as for the other settings calls.
    let headers = signed_headers(
        &server.s3,
        "GET",
        "/_s3dal/admin/stats",
        "",
        &[],
        "UNSIGNED-PAYLOAD",
    );
    let headers: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let (code, _) = raw_request(&server.s3, "GET", "/_s3dal/admin/stats", &headers, b"").await;
    assert_ne!(code, 200);
}

#[tokio::test]
async fn the_collector_password_never_reaches_the_page_and_survives_its_edits() {
    // As with a backend's credentials: the page is never sent the password, so a URL it sends
    // back without one keeps it rather than blanking it.
    let config = "backend = \"fs\"\n\
                  log-forward = \"elasticsearch+https://elastic:COLLECTOR-SECRET@es:9200/logs\"\n\
                  [[credentials]]\naccess-key = \"s3dal-test-access\"\n\
                  secret-key = \"s3dal-test-secret\"\n";
    let server = ConsoleServer::start(config, &[]).await;
    for path in ["/_s3dal/admin/config", "/_s3dal/admin/info"] {
        let (status, body) = server.api("GET", path, "").await;
        assert_eq!(status, 200, "{body}");
        assert!(!body.contains("COLLECTOR-SECRET"), "{path}: {body}");
        assert!(body.contains("elastic@es:9200/logs"), "{path}: {body}");
    }
    let edit = r#"{"settings":{"log-forward":"elasticsearch+https://elastic@es:9200/other"}}"#;
    let (status, body) = server.api("PUT", "/_s3dal/admin/config", edit).await;
    assert_eq!(status, 200, "{body}");
    assert!(
        server
            .file_text()
            .contains("elasticsearch+https://elastic:COLLECTOR-SECRET@es:9200/other"),
        "{}",
        server.file_text()
    );
    assert!(server.live.pending_restart().contains(&"log-forward"));
}

#[tokio::test]
async fn a_console_edit_the_server_refuses_leaves_the_file_as_it_was() {
    // The file must never hold settings the server does not run with, or the next restart
    // would fail, or start with settings nobody saw take effect.
    let server = ConsoleServer::start(CONFIG, &[]).await;
    for edit in [
        r#"{"settings":{"etag-buffer-limit":"8MB"}}"#,
        r#"{"settings":{"backend":"s3"}}"#,
        r#"{"credentials":[{"access_key":"s3dal-test-access","access":"read-write","expires":null},{"access_key":"new","access":"read-write","expires":null}]}"#,
        r#"{"credentials":[{"access_key":"s3dal-test-access","access":"read-write","expires":null},{"access_key":"weak","secret_key":"short","access":"read-write","expires":null}]}"#,
        // A limit the server cannot read must not reach the file either.
        r#"{"credentials":[{"access_key":"s3dal-test-access","access":"read-write","expires":null},{"access_key":"bob","secret_key":"bob-secret-key","access":"read-write","allow":[],"expires":null}]}"#,
        r#"{"credentials":[{"access_key":"s3dal-test-access","access":"read-write","expires":null},{"access_key":"bob","secret_key":"bob-secret-key","access":"read-write","expires":"2026-12-31"}]}"#,
        // Listing pairs without their limits would lift them; the fields are required.
        r#"{"credentials":[{"access_key":"s3dal-test-access","expires":null}]}"#,
        r#"{"credentials":[{"access_key":"s3dal-test-access","access":"read-write"}]}"#,
        r#"{"settings":{"log-max-files":"many"}}"#,
        r#"{"settings":{"log-forward":"graylog:12201"}}"#,
    ] {
        let (status, body) = server.api("PUT", "/_s3dal/admin/config", edit).await;
        assert_eq!(status, 400, "{edit}: {body}");
        assert_eq!(server.file_text(), CONFIG, "{edit}");
    }
}

#[tokio::test]
async fn a_key_pair_with_limits_signs_in_but_cannot_reach_the_settings() {
    // The settings hold every key pair and the backend, which reach past any one folder; the
    // console still needs to learn what such a pair may do, to show only that.
    let config = format!(
        "{CONFIG}\n[[credentials]]\naccess-key = \"viewer\"\nsecret-key = \"viewer-secret\"\n\
         allow = [\"photos/2024/\"]\naccess = \"read-only\"\n"
    );
    let server = ConsoleServer::start(&config, &[]).await;
    let as_viewer = |method: &'static str, path: &'static str| {
        let headers = signed_headers_as(
            &server.console,
            method,
            path,
            "",
            &[],
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "viewer",
            "viewer-secret",
        );
        let console = server.console.clone();
        async move {
            let h: Vec<(&str, &str)> = headers
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            let (status, body) =
                raw_request(&console, method, &format!("/s3{path}"), &h, b"").await;
            (status, String::from_utf8_lossy(&body).into_owned())
        }
    };
    let (status, body) = as_viewer("GET", "/_s3dal/admin/whoami").await;
    assert_eq!(status, 200, "{body}");
    let me: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(me["admin"], false);
    assert_eq!(me["write"], false);
    assert_eq!(me["allow"][0]["bucket"], "photos");
    assert_eq!(me["allow"][0]["prefix"], "2024/");
    for path in ["/_s3dal/admin/config", "/_s3dal/admin/info"] {
        let (status, body) = as_viewer("GET", path).await;
        assert_eq!(status, 403, "{path}: {body}");
    }
    // A pair without limits is told so.
    let (_, body) = server.api("GET", "/_s3dal/admin/whoami", "").await;
    assert!(body.contains("\"admin\":true"), "{body}");
}

#[tokio::test]
async fn the_console_cannot_lock_out_its_own_session_or_override_the_environment() {
    // Removing the key the console signs with would end the session with no way back in from
    // the console; a setting given by a flag outranks the file, so editing it there would
    // change nothing while appearing to.
    let server = ConsoleServer::start(CONFIG, &["--region=eu-west-1"]).await;
    let (status, body) = server
        .api("PUT", "/_s3dal/admin/config", r#"{"credentials":[]}"#)
        .await;
    assert_eq!(status, 400, "{body}");
    assert!(body.contains("signed in with"), "{body}");
    let (status, body) = server
        .api(
            "PUT",
            "/_s3dal/admin/config",
            r#"{"settings":{"region":"us-west-2"}}"#,
        )
        .await;
    assert_eq!(status, 400, "{body}");
    let (_, body) = server.api("GET", "/_s3dal/admin/config", "").await;
    assert!(
        body.contains(
            r#"{"editable":false,"live":true,"locked":true,"name":"region","value":"eu-west-1"}"#
        ),
        "{body}"
    );
    assert_eq!(server.file_text(), CONFIG);
}

#[tokio::test]
async fn a_console_edit_sets_write_only_access_and_an_expiry() {
    // Both reach the file, where the next start finds them, and apply at once: an expiry
    // already past refuses the pair from the next request on.
    let config = format!(
        "{CONFIG}\n[[credentials]]\naccess-key = \"drop\"\nsecret-key = \"drop-secret-key\"\n"
    );
    let server = ConsoleServer::start(&config, &[]).await;
    let s3 = client(&server.s3, ACCESS_KEY, SECRET_KEY);
    s3.create_bucket().bucket("inbox").send().await.unwrap();
    let drop = client(&server.s3, "drop", "drop-secret-key");
    drop.list_buckets().send().await.unwrap();

    let edit = r#"{"credentials":[{"access_key":"s3dal-test-access","access":"read-write","expires":null},
        {"access_key":"drop","access":"write-only","allow":["inbox"],"expires":"2031-01-01T08:00:00+08:00"}]}"#;
    let (status, body) = server.api("PUT", "/_s3dal/admin/config", edit).await;
    assert_eq!(status, 200, "{body}");
    let text = server.file_text();
    assert!(text.contains("access = \"write-only\""), "{text}");
    // Kept in UTC, as the page is sent it back.
    assert!(
        text.contains("expires = \"2031-01-01T00:00:00Z\""),
        "{text}"
    );
    assert!(
        body.contains(r#""expires":"2031-01-01T00:00:00Z""#),
        "{body}"
    );
    drop.put_object()
        .bucket("inbox")
        .key("a.txt")
        .body(b"dropped".to_vec().into())
        .send()
        .await
        .unwrap();
    let err = drop.list_buckets().send().await.unwrap_err();
    assert_eq!(err.code(), Some("AccessDenied"), "{err:?}");

    let edit = r#"{"credentials":[{"access_key":"s3dal-test-access","access":"read-write","expires":null},
        {"access_key":"drop","access":"write-only","allow":["inbox"],"expires":"2001-01-01T00:00:00Z"}]}"#;
    let (status, body) = server.api("PUT", "/_s3dal/admin/config", edit).await;
    assert_eq!(status, 200, "{body}");
    let err = drop
        .put_object()
        .bucket("inbox")
        .key("b.txt")
        .body(b"late".to_vec().into())
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("InvalidAccessKeyId"), "{err:?}");
}

#[tokio::test]
async fn the_console_cannot_set_an_expiry_on_its_own_session_but_keeps_one_set_by_hand() {
    // An expiry on the signed-in pair would end the session at that instant, which is editing
    // away its own access. One the operator wrote into the file must survive the console's
    // edits of other pairs, which send the signed-in pair back as it is.
    let server = ConsoleServer::start(CONFIG, &[]).await;
    let edit = r#"{"credentials":[{"access_key":"s3dal-test-access","access":"read-write","expires":"2999-01-01T00:00:00Z"}]}"#;
    let (status, body) = server.api("PUT", "/_s3dal/admin/config", edit).await;
    assert_eq!(status, 400, "{body}");
    assert!(body.contains("signed in with"), "{body}");
    assert_eq!(server.file_text(), CONFIG);

    let by_hand = format!("{CONFIG}expires = \"2999-01-01T00:00:00Z\"\n");
    let server = ConsoleServer::start(&by_hand, &[]).await;
    let edit = r#"{"credentials":[{"access_key":"s3dal-test-access","access":"read-write","expires":"2999-01-01T00:00:00Z"},
        {"access_key":"bob","secret_key":"bob-secret-key","access":"read-write","expires":null}]}"#;
    let (status, body) = server.api("PUT", "/_s3dal/admin/config", edit).await;
    assert_eq!(status, 200, "{body}");
    assert!(
        server
            .file_text()
            .contains("expires = \"2999-01-01T00:00:00Z\""),
        "{}",
        server.file_text()
    );
    let edit = r#"{"credentials":[{"access_key":"s3dal-test-access","access":"read-write","expires":null}]}"#;
    let (status, body) = server.api("PUT", "/_s3dal/admin/config", edit).await;
    assert_eq!(status, 400, "lifting its own expiry: {body}");
}

/// Whether the server was asked to restart, looking for a moment.
async fn restart_asked(live: &Live) -> bool {
    tokio::time::timeout(
        std::time::Duration::from_millis(300),
        live.restart_requested(),
    )
    .await
    .is_ok()
}

#[tokio::test]
async fn a_restart_is_refused_while_the_server_would_not_start_again() {
    // A server that stops and then fails to start leaves nothing to fix the settings from,
    // the console included; what would fail must be found while it still runs.
    // The root as a flag, as `--backend`: scripts/with-rustfs.sh exports S3DAL_BACKEND_OPTS for
    // the whole run, which outranks the file.
    let root = tempfile::tempdir().unwrap();
    let root_flag = format!("--backend-opt=root={}", root.path().display());
    let server = ConsoleServer::start(CONFIG, &[&root_flag]).await;
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    for (file, says) in [
        (
            format!("address = \"{}\"\n{CONFIG}", taken.local_addr().unwrap()),
            "cannot listen on",
        ),
        ("this is not toml".to_owned(), "do not read"),
    ] {
        std::fs::write(server.file.path(), &file).unwrap();
        let (status, body) = server.api("POST", "/_s3dal/admin/restart", "").await;
        assert_eq!(status, 400, "{file}: {body}");
        assert!(body.contains(says), "{file}: {body}");
        assert!(!restart_asked(&server.live).await, "{file}");
    }

    // Once it would, the restart is asked for.
    std::fs::write(server.file.path(), CONFIG).unwrap();
    let (status, body) = server.api("POST", "/_s3dal/admin/restart", "").await;
    assert_eq!(status, 200, "{body}");
    assert!(restart_asked(&server.live).await);

    // A backend that does not work, which a start would stop on.
    let not_a_dir = tempfile::NamedTempFile::new().unwrap();
    let bad_root = format!(
        "--backend-opt=root={}",
        not_a_dir.path().join("x").display()
    );
    let server = ConsoleServer::start(CONFIG, &[&bad_root]).await;
    let (status, body) = server.api("POST", "/_s3dal/admin/restart", "").await;
    assert_eq!(status, 400, "{body}");
    assert!(body.contains("backend"), "{body}");
    assert!(!restart_asked(&server.live).await);
}

#[cfg(unix)]
#[tokio::test]
async fn a_restart_runs_the_same_process_again_with_the_saved_settings() {
    // A restart by exiting would leave a server started by hand, or by Docker without a
    // restart policy, stopped for good; in the same process the PID stays, as a container's
    // PID 1 must.
    use tokio::io::AsyncBufReadExt;
    let root = tempfile::tempdir().unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        file.path(),
        format!(
            "backend = \"fs\"\naddress = \"127.0.0.1:0\"\nconsole-address = \"127.0.0.1:0\"\n\
             [backend-opt]\nroot = {:?}\n",
            root.path()
        ),
    )
    .unwrap();
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_s3dal"))
        .arg("serve")
        .arg("--config")
        .arg(file.path())
        .env_clear()
        .env("S3DAL_ACCESS_KEY", ACCESS_KEY)
        .env("S3DAL_SECRET_KEY", SECRET_KEY)
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let pid = child.id();
    let mut log = tokio::io::BufReader::new(child.stderr.take().unwrap()).lines();
    // The console's URL from the line that says where it listens, and that line.
    let mut console_line = async move || {
        let wait = async {
            while let Some(line) = log.next_line().await.unwrap() {
                if let Some(at) = line.find("console on ") {
                    let url = line[at + "console on ".len()..]
                        .split(['"', ' '])
                        .next()
                        .unwrap()
                        .to_owned();
                    return (url, line);
                }
            }
            panic!("s3dal exited");
        };
        tokio::time::timeout(std::time::Duration::from_secs(30), wait)
            .await
            .expect("s3dal said where the console is")
    };
    let (console, first) = console_line().await;
    assert!(!first.starts_with('{'), "{first}");

    let edit = r#"{"settings":{"log-format":"json"}}"#;
    let (status, body) = console_api(&console, "PUT", "/_s3dal/admin/config", edit).await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = console_api(&console, "POST", "/_s3dal/admin/restart", "").await;
    assert_eq!(status, 200, "{body}");

    // The log now comes as JSON: the settings the first start did not have.
    let (console, line) = console_line().await;
    assert!(line.starts_with('{'), "{line}");
    assert_eq!(child.id(), pid);
    assert!(child.try_wait().unwrap().is_none());
    let (status, body) = console_api(&console, "GET", "/_s3dal/admin/info", "").await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("\"pending_restart\":[]"), "{body}");
}

fn json(body: &str) -> serde_json::Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("{e}: {body}"))
}

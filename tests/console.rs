mod common;

use std::sync::Arc;

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
    _backend: Backend,
    _stop: oneshot::Sender<()>,
}

const CONFIG: &str = "# Written by hand; the console must keep this comment.\n\
                      backend = \"fs\"\n\n\
                      [[credentials]]\naccess-key = \"s3dal-test-access\"\n\
                      secret-key = \"s3dal-test-secret\"\n";

impl ConsoleServer {
    async fn start(text: &str, extra_args: &[&str]) -> Self {
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
        let dal = S3Dal::new(backend.op.clone(), backend.profile, args.settings());
        let auth = server::LiveAuth::new(&args.credentials, &args.domain).unwrap();
        auth.set_public(&args.public_bucket);
        let live = Arc::new(Live::new(argv, args, dal.clone(), auth.clone(), None));
        let service = server::build_service(dal, &auth, Some(live.clone()));

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
            _backend: backend,
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
async fn a_console_edit_is_written_to_the_file_and_applied_at_once() {
    // Adding a key pair from the console must work at once, survive a restart (it is in the
    // file), and leave what the operator wrote by hand in the file as it was.
    let server = ConsoleServer::start(CONFIG, &[]).await;
    let edit = r#"{"settings":{"stale-upload-age":"12h"},
        "credentials":[{"access_key":"s3dal-test-access","access":"read-write"},
                       {"access_key":"bob","secret_key":"bob-secret-key","access":"read-write"}]}"#;
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
    // The switch on the Buckets page saves this setting; a public link copied right after must
    // work, on the S3 port and on the console's own origin, where the console builds it.
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

    let edit = r#"{"settings":{"public-bucket":["site"]}}"#;
    let (status, body) = server.api("PUT", "/_s3dal/admin/config", edit).await;
    assert_eq!(status, 200, "{body}");
    assert!(server.file_text().contains("public-bucket = [\"site\"]"));
    for (endpoint, path) in [
        (&server.s3, "/site/a.txt"),
        (&server.console, "/s3/site/a.txt"),
    ] {
        let (status, body) = raw_request(endpoint, "GET", path, &[], b"").await;
        assert_eq!((status, body.as_slice()), (200, &b"hello"[..]), "{path}");
    }
    let (_, body) = server.api("GET", "/_s3dal/admin/whoami", "").await;
    assert!(body.contains("\"public\":[\"site\"]"), "{body}");
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
        r#"{"credentials":[{"access_key":"s3dal-test-access","access":"read-write"},{"access_key":"new","access":"read-write"}]}"#,
        r#"{"credentials":[{"access_key":"s3dal-test-access","access":"read-write"},{"access_key":"weak","secret_key":"short","access":"read-write"}]}"#,
        // A limit the server cannot read must not reach the file either.
        r#"{"credentials":[{"access_key":"s3dal-test-access","access":"read-write"},{"access_key":"bob","secret_key":"bob-secret-key","access":"read-write","allow":[]}]}"#,
        // Listing pairs without their limits would lift them; the field is required.
        r#"{"credentials":[{"access_key":"s3dal-test-access"}]}"#,
        r#"{"settings":{"log-max-files":"many"}}"#,
        r#"{"settings":{"log-forward":"graylog:12201"}}"#,
        r#"{"settings":{"public-bucket":["site/img"]}}"#,
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

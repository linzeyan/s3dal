mod common;

use std::time::Duration;

use common::{TestServer, raw_request, raw_response};
use s3dal::Settings;
use s3dal::config::MetricsMode;

const TOKEN: &str = "scrape-token-0123456789";

async fn server(metrics: MetricsMode, token: Option<&str>) -> TestServer {
    TestServer::start_with(Settings {
        metrics,
        metrics_token: token.map(str::to_owned),
        ..Default::default()
    })
    .await
}

/// The value of the sample line that starts with `series` (name and labels), if listed.
fn value(text: &str, series: &str) -> Option<f64> {
    text.lines()
        .find_map(|l| l.strip_prefix(series)?.strip_prefix(' ')?.parse().ok())
}

async fn scrape(endpoint: &str) -> String {
    let (status, body) = raw_request(endpoint, "GET", "/_s3dal/metrics", &[], b"").await;
    let body = String::from_utf8(body).unwrap();
    assert_eq!(status, 200, "{body}");
    body
}

#[tokio::test]
async fn a_scrape_counts_each_request_by_operation_and_status() {
    // An operator alerts on these: a 5xx rate, a slow operation, uploads left open. A request
    // counted under the wrong operation or class, or not at all, would hide exactly that.
    let server = server(MetricsMode::Public, None).await;
    let c = &server.client;
    c.create_bucket().bucket("metered").send().await.unwrap();
    for key in ["a", "b", "c"] {
        c.put_object()
            .bucket("metered")
            .key(key)
            .body(b"hello".to_vec().into())
            .send()
            .await
            .unwrap();
    }
    for _ in 0..2 {
        let got = c
            .get_object()
            .bucket("metered")
            .key("a")
            .send()
            .await
            .unwrap();
        assert_eq!(
            got.body.collect().await.unwrap().into_bytes().as_ref(),
            b"hello"
        );
    }
    assert!(
        c.get_object()
            .bucket("metered")
            .key("none")
            .send()
            .await
            .is_err()
    );
    c.head_object()
        .bucket("metered")
        .key("b")
        .send()
        .await
        .unwrap();
    c.list_objects_v2().bucket("metered").send().await.unwrap();
    let mut ids = Vec::new();
    for _ in 0..2 {
        let up = c
            .create_multipart_upload()
            .bucket("metered")
            .key("big")
            .send()
            .await
            .unwrap();
        ids.push(up.upload_id.unwrap());
    }
    c.abort_multipart_upload()
        .bucket("metered")
        .key("big")
        .upload_id(&ids[0])
        .send()
        .await
        .unwrap();
    // Unsigned: refused once s3s knows the operation, so it counts as a GetObject.
    let (status, _) = raw_request(&server.endpoint, "GET", "/metered/a", &[], b"").await;
    assert_eq!(status, 403);
    // A key s3dal does not know: refused before any operation is known.
    let bad = "AWS4-HMAC-SHA256 Credential=nobody/20260101/us-east-1/s3/aws4_request, \
               SignedHeaders=host, Signature=00";
    let (status, _) = raw_request(
        &server.endpoint,
        "GET",
        "/metered/a",
        &[("authorization", bad)],
        b"",
    )
    .await;
    assert_eq!(status, 403);
    // Neither the health check nor the scrape is an S3 request.
    let (status, _) = raw_request(&server.endpoint, "GET", "/_s3dal/health", &[], b"").await;
    assert_eq!(status, 200);

    let expected = [
        (
            "s3dal_requests_total{operation=\"CreateBucket\",status_class=\"2xx\"}",
            1.0,
        ),
        (
            "s3dal_requests_total{operation=\"PutObject\",status_class=\"2xx\"}",
            3.0,
        ),
        (
            "s3dal_requests_total{operation=\"GetObject\",status_class=\"2xx\"}",
            2.0,
        ),
        (
            "s3dal_requests_total{operation=\"GetObject\",status_class=\"4xx\"}",
            2.0,
        ),
        (
            "s3dal_requests_total{operation=\"HeadObject\",status_class=\"2xx\"}",
            1.0,
        ),
        (
            "s3dal_requests_total{operation=\"ListObjectsV2\",status_class=\"2xx\"}",
            1.0,
        ),
        (
            "s3dal_requests_total{operation=\"CreateMultipartUpload\",status_class=\"2xx\"}",
            2.0,
        ),
        (
            "s3dal_requests_total{operation=\"AbortMultipartUpload\",status_class=\"2xx\"}",
            1.0,
        ),
        (
            "s3dal_requests_total{operation=\"Unknown\",status_class=\"4xx\"}",
            1.0,
        ),
        (
            "s3dal_request_duration_seconds_count{operation=\"PutObject\"}",
            3.0,
        ),
        (
            "s3dal_request_duration_seconds_bucket{operation=\"PutObject\",le=\"+Inf\"}",
            3.0,
        ),
        ("s3dal_requests_in_flight", 0.0),
        ("s3dal_multipart_uploads_open", 1.0),
    ];
    // A response is counted when hyper drops its body, which may come a moment after the client
    // has read the last byte.
    let mut text = String::new();
    for _ in 0..100 {
        text = scrape(&server.endpoint).await;
        if expected.iter().all(|(s, v)| value(&text, s) == Some(*v)) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for (series, v) in expected {
        assert_eq!(value(&text, series), Some(v), "{series} in:\n{text}");
    }
    // 15 bytes up and 10 down, with the error documents and listings on top.
    assert!(
        value(&text, "s3dal_received_bytes_total").unwrap() >= 15.0,
        "{text}"
    );
    assert!(
        value(&text, "s3dal_sent_bytes_total").unwrap() >= 10.0,
        "{text}"
    );
    assert!(value(&text, "s3dal_connections_total{protocol=\"http1\"}").unwrap() >= 1.0);
    // The backend is timed, and clean reads and writes are no errors.
    assert!(
        value(
            &text,
            "s3dal_backend_operation_duration_seconds_count{operation=\"write\"}"
        )
        .unwrap()
            >= 3.0,
        "{text}"
    );
    for op in ["read", "write"] {
        let prefix = format!("s3dal_backend_errors_total{{operation=\"{op}\"");
        assert!(!text.contains(&prefix), "{prefix} in:\n{text}");
    }
    // Nothing else was counted: the health check and the scrapes are not S3 requests.
    let counted = expected
        .iter()
        .filter(|(s, _)| s.starts_with("s3dal_requests_total{"))
        .count();
    assert_eq!(
        text.lines()
            .filter(|l| l.starts_with("s3dal_requests_total{"))
            .count(),
        counted,
        "{text}"
    );
    assert!(text.contains("s3dal_build_info{version=\""));
}

#[tokio::test]
async fn metrics_answer_as_the_access_mode_says() {
    // Off must not even say they exist; token must refuse a missing or wrong token; a token
    // mode with no token set refuses everyone rather than opening to anyone.
    let bearer = format!("Bearer {TOKEN}");
    let get = |endpoint: String, auth: Option<String>| async move {
        let headers: Vec<(&str, &str)> = auth
            .as_deref()
            .map(|a| ("authorization", a))
            .into_iter()
            .collect();
        raw_response(&endpoint, "GET", "/_s3dal/metrics", &headers, b"").await
    };

    let off = server(MetricsMode::Off, Some(TOKEN)).await;
    assert_eq!(get(off.endpoint.clone(), Some(bearer.clone())).await.0, 404);

    let token = server(MetricsMode::Token, Some(TOKEN)).await;
    for auth in [
        None,
        Some(format!("Bearer {TOKEN}x")),
        Some("Bearer wrong".to_owned()),
    ] {
        let (status, head, _) = get(token.endpoint.clone(), auth.clone()).await;
        assert_eq!(status, 401, "{auth:?}");
        assert!(
            head.to_ascii_lowercase()
                .contains("www-authenticate: bearer"),
            "{head}"
        );
    }
    let (status, head, body) = get(token.endpoint.clone(), Some(bearer.clone())).await;
    assert_eq!(status, 200);
    assert!(head.contains("text/plain; version=0.0.4"), "{head}");
    assert!(String::from_utf8_lossy(&body).contains("s3dal_build_info"));
    // HEAD is checked the same way.
    let (status, _) = raw_request(&token.endpoint, "HEAD", "/_s3dal/metrics", &[], b"").await;
    assert_eq!(status, 401);

    let no_token = server(MetricsMode::Token, None).await;
    let (status, _, body) = get(no_token.endpoint.clone(), Some(bearer.clone())).await;
    assert_eq!(status, 403);
    assert!(String::from_utf8_lossy(&body).contains("metrics-token"));

    let public = server(MetricsMode::Public, None).await;
    assert_eq!(get(public.endpoint.clone(), None).await.0, 200);
}

#[tokio::test]
async fn a_request_signed_for_s3_is_never_taken_for_a_scrape() {
    // The path is also a key in a virtual-hosted bucket; a signed request for it is S3's,
    // whatever the metrics setting.
    let server = server(MetricsMode::Public, None).await;
    let headers = common::signed_headers(
        &server.endpoint,
        "GET",
        "/_s3dal/metrics",
        "",
        &[],
        "UNSIGNED-PAYLOAD",
    );
    let headers: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let (status, body) =
        raw_request(&server.endpoint, "GET", "/_s3dal/metrics", &headers, b"").await;
    let body = String::from_utf8_lossy(&body);
    assert_ne!(status, 200, "{body}");
    assert!(!body.contains("s3dal_build_info"), "{body}");
}

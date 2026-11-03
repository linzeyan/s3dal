//! The web console, on a listener of its own. Its pages are static and sign S3 requests in the
//! browser with the key pair the user logs in with, so the console can do exactly what that
//! key can, and the server has no session, cookie or second login to keep safe. The pages reach
//! the S3 API under `/s3/` on the same origin, and the settings API under
//! `/s3/_s3dal/admin/`, which only the console's listener answers.

mod admin;

use std::sync::Arc;

use hyper::header::{CACHE_CONTROL, CONTENT_TYPE, HeaderValue};
use hyper::{Method, StatusCode, Uri};
use s3s::{Body, HttpResponse};

pub(crate) use admin::AdminRoute;

use crate::live::Live;

/// Marks a request that came through the console's listener. Only such requests reach the
/// settings API: the S3 port stays the plain S3 API it was.
#[derive(Clone, Copy)]
pub(crate) struct ConsoleRequest;

/// Where the pages send S3 requests; every other path is a page. A prefix keeps the two apart
/// without reserving a bucket name (a bucket `s3` is `/s3/s3/`). The browser signs the path as
/// the S3 service sees it, without the prefix.
const API_PREFIX: &str = "/s3";

/// The pages themselves allow nothing from elsewhere: a script injected through an object name
/// could otherwise read the secret key the page holds.
const PAGE_POLICY: &str = "default-src 'none'; script-src 'self'; style-src 'self'; \
    img-src 'self' blob: data:; media-src 'self'; connect-src 'self'; frame-ancestors 'none'; \
    base-uri 'none'; form-action 'none'";

/// Objects served on the console's origin run nothing: an uploaded HTML file opened from the
/// console would otherwise run on the origin that holds the secret key.
pub(crate) const OBJECT_POLICY: &str = "sandbox; default-src 'none'; img-src 'self' data:; \
    media-src 'self'; style-src 'unsafe-inline'";

const ASSETS: &[(&str, &str, &str)] = &[
    (
        "/app.js",
        "text/javascript; charset=utf-8",
        include_str!("assets/app.js"),
    ),
    (
        "/s3.js",
        "text/javascript; charset=utf-8",
        include_str!("assets/s3.js"),
    ),
    (
        "/sigv4.js",
        "text/javascript; charset=utf-8",
        include_str!("assets/sigv4.js"),
    ),
    (
        "/ui.js",
        "text/javascript; charset=utf-8",
        include_str!("assets/ui.js"),
    ),
    (
        "/style.css",
        "text/css; charset=utf-8",
        include_str!("assets/style.css"),
    ),
    (
        "/icon.svg",
        "image/svg+xml",
        include_str!("assets/icon.svg"),
    ),
];

const INDEX: &str = include_str!("assets/index.html");

pub struct Console {
    live: Arc<Live>,
}

impl Console {
    pub fn new(live: Arc<Live>) -> Self {
        Self { live }
    }

    /// A page or asset; any other path is the single page, which routes in the browser.
    pub(crate) fn page(&self, method: &Method, path: &str) -> HttpResponse {
        if method != Method::GET && method != Method::HEAD {
            let mut res = HttpResponse::new(Body::empty());
            *res.status_mut() = StatusCode::METHOD_NOT_ALLOWED;
            return res;
        }
        let (content_type, body) = match ASSETS.iter().find(|(p, _, _)| *p == path) {
            Some((_, content_type, text)) => (*content_type, (*text).to_owned()),
            None => ("text/html; charset=utf-8", self.index()),
        };
        let mut res = HttpResponse::new(Body::from(body));
        let headers = res.headers_mut();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
        // Asked again on every load, so a new version or region shows at once; the files are
        // small and come from memory.
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
        headers.insert(
            "content-security-policy",
            HeaderValue::from_static(PAGE_POLICY),
        );
        headers.insert(
            "x-content-type-options",
            HeaderValue::from_static("nosniff"),
        );
        headers.insert("referrer-policy", HeaderValue::from_static("no-referrer"));
        res
    }

    /// The page, told the region to sign for: a signature names the region, and a reload may
    /// change it.
    fn index(&self) -> String {
        let region = self.live.dal().settings().region.clone();
        INDEX
            .replace("{{REGION}}", &escape_html(&region))
            .replace("{{VERSION}}", env!("CARGO_PKG_VERSION"))
    }
}

/// The request's URI as the S3 service must see it, for a request to the S3 API under
/// [`API_PREFIX`]; `None` for a page.
pub(crate) fn s3_uri(uri: &Uri) -> Option<Uri> {
    let rest = uri.path().strip_prefix(API_PREFIX)?;
    if !(rest.is_empty() || rest.starts_with('/')) {
        return None;
    }
    let path = if rest.is_empty() { "/" } else { rest };
    let path_and_query = match uri.query() {
        Some(query) => format!("{path}?{query}"),
        None => path.to_owned(),
    };
    Uri::builder().path_and_query(path_and_query).build().ok()
}

fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_paths_under_the_api_prefix_reach_the_s3_api() {
        // A page path that reached the S3 API would be read as a bucket; an S3 path that
        // reached the pages would get HTML where a client expects XML.
        let s3 = |u: &str| s3_uri(&u.parse().unwrap()).map(|u| u.to_string());
        assert_eq!(s3("/s3").as_deref(), Some("/"));
        assert_eq!(s3("/s3/").as_deref(), Some("/"));
        assert_eq!(
            s3("/s3/bkt/a%20b?list-type=2").as_deref(),
            Some("/bkt/a%20b?list-type=2")
        );
        assert_eq!(s3("/s3bucket/k"), None);
        assert_eq!(s3("/app.js"), None);
        assert_eq!(s3("/"), None);
    }
}

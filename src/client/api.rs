//! The S3 requests the client commands make: SigV4-signed, path-style, over reqwest.

use std::fmt;
use std::time::Duration;

use anyhow::Context as _;
use bytes::Bytes;
use http::header::{CONTENT_LENGTH, HeaderMap, HeaderName, HeaderValue};
use http::{Method, StatusCode};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use reqsign_aws_v4::{Credential, RequestSigner};
use reqsign_core::{Context, SignRequest};
use serde::Deserialize;

/// Everything but RFC 3986's unreserved characters is escaped: the form SigV4 canonicalizes
/// to, so what is signed is byte for byte what is sent.
const QUERY_VALUE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');
const KEY: &AsciiSet = &QUERY_VALUE.remove(b'/');

/// An S3 error response, as the server reported it.
#[derive(Debug)]
pub struct S3Error {
    pub status: StatusCode,
    /// `None` for a response without an error document, such as any HEAD response.
    pub code: Option<String>,
    pub message: Option<String>,
    pub request_id: Option<String>,
}

impl fmt::Display for S3Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut details = Vec::new();
        match &self.code {
            Some(code) => {
                write!(f, "{code}")?;
                if let Some(message) = &self.message {
                    write!(f, ": {message}")?;
                }
                details.push(format!("HTTP {}", self.status.as_u16()));
            }
            None => write!(f, "HTTP {}", self.status)?,
        }
        if let Some(id) = &self.request_id {
            details.push(format!("request id {id}"));
        }
        if !details.is_empty() {
            write!(f, " ({})", details.join(", "))?;
        }
        Ok(())
    }
}

impl std::error::Error for S3Error {}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ErrorDocument {
    code: String,
    message: Option<String>,
    request_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListAllMyBucketsResult {
    #[serde(default)]
    buckets: Buckets,
    continuation_token: Option<String>,
}

#[derive(Default, Deserialize)]
struct Buckets {
    #[serde(default, rename = "Bucket")]
    bucket: Vec<Bucket>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Bucket {
    pub name: String,
    pub creation_date: String,
}

/// One ListObjectsV2 page.
#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListPage {
    #[serde(default)]
    pub contents: Vec<Object>,
    #[serde(default)]
    pub common_prefixes: Vec<CommonPrefix>,
    #[serde(default)]
    is_truncated: bool,
    next_continuation_token: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Object {
    pub key: String,
    pub last_modified: String,
    pub size: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CommonPrefix {
    pub prefix: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct InitiateMultipartUploadResult {
    upload_id: String,
}

pub struct S3 {
    http: reqwest::Client,
    /// `scheme://authority[/path]`, without a trailing slash.
    endpoint: String,
    region: String,
    signer: RequestSigner,
    credential: Credential,
    ctx: Context,
}

impl S3 {
    pub fn new(
        endpoint: &str,
        region: &str,
        access_key: String,
        secret_key: String,
        insecure: bool,
    ) -> anyhow::Result<Self> {
        let endpoint = endpoint.trim_end_matches('/').to_owned();
        let uri: http::Uri = endpoint
            .parse()
            .with_context(|| format!("invalid endpoint {endpoint:?}"))?;
        if uri.scheme().is_none() || uri.authority().is_none() {
            anyhow::bail!("invalid endpoint {endpoint:?}: expected http://host:port or https://");
        }
        let http = reqwest::Client::builder()
            .tls_danger_accept_invalid_certs(insecure)
            .build()
            .context("cannot set up the HTTP client")?;
        Ok(Self {
            http,
            endpoint,
            region: region.to_owned(),
            signer: RequestSigner::new("s3", region),
            credential: Credential {
                access_key_id: access_key,
                secret_access_key: secret_key,
                ..Default::default()
            },
            ctx: Context::new(),
        })
    }

    /// Path-style always: an endpoint given as an IP address or a bare host name has no
    /// virtual-host DNS behind it.
    fn url(&self, bucket: &str, key: &str, query: &[(&str, &str)]) -> String {
        let mut url = self.endpoint.clone();
        if !bucket.is_empty() {
            url.push('/');
            url.push_str(bucket);
        }
        url.push('/');
        url.extend(utf8_percent_encode(key, KEY));
        for (i, (name, value)) in query.iter().enumerate() {
            url.push(if i == 0 { '?' } else { '&' });
            url.push_str(name);
            if !value.is_empty() {
                url.push('=');
                url.extend(utf8_percent_encode(value, QUERY_VALUE));
            }
        }
        url
    }

    async fn sign(
        &self,
        method: Method,
        url: &str,
        headers: HeaderMap,
        expires: Option<Duration>,
    ) -> anyhow::Result<http::request::Parts> {
        let (mut parts, ()) = http::Request::builder()
            .method(method)
            .uri(url)
            .body(())
            .with_context(|| format!("invalid request URL {url:?}"))?
            .into_parts();
        parts.headers = headers;
        self.signer
            .sign_request(&self.ctx, &mut parts, Some(&self.credential), expires)
            .await
            .context("cannot sign the request")?;
        Ok(parts)
    }

    /// Sends a signed request and turns any non-2xx response into an [`S3Error`].
    async fn send(
        &self,
        method: Method,
        bucket: &str,
        key: &str,
        query: &[(&str, &str)],
        headers: HeaderMap,
        body: Option<reqwest::Body>,
    ) -> anyhow::Result<reqwest::Response> {
        let url = self.url(bucket, key, query);
        let parts = self.sign(method, &url, headers, None).await?;
        let mut req = self
            .http
            .request(parts.method, parts.uri.to_string())
            .headers(parts.headers);
        if let Some(body) = body {
            req = req.body(body);
        }
        let resp = req
            .send()
            .await
            .with_context(|| format!("cannot reach {}", self.endpoint))?;
        if resp.status().is_success() {
            return Ok(resp);
        }
        let status = resp.status();
        let request_id = request_id(resp.headers());
        let text = resp.text().await.unwrap_or_default();
        Err(error_from(status, request_id, &text).into())
    }

    /// The body of a response that S3 may send with status 200 and still fail: CopyObject and
    /// CompleteMultipartUpload report errors met after the headers went out in the body.
    async fn checked_body(resp: reqwest::Response) -> anyhow::Result<String> {
        let status = resp.status();
        let request_id = request_id(resp.headers());
        let text = resp.text().await.context("cannot read the response")?;
        // An element name cannot appear as text inside another element: `<` is escaped there.
        if text.contains("<Error>") {
            return Err(error_from(status, request_id, &text).into());
        }
        Ok(text)
    }

    pub async fn list_buckets(&self) -> anyhow::Result<Vec<Bucket>> {
        let mut buckets = Vec::new();
        let mut token = None;
        loop {
            let query: Vec<(&str, &str)> = token
                .as_deref()
                .map(|t| ("continuation-token", t))
                .into_iter()
                .collect();
            let resp = self
                .send(Method::GET, "", "", &query, HeaderMap::new(), None)
                .await?;
            let page: ListAllMyBucketsResult = parse(&resp.text().await?)?;
            buckets.extend(page.buckets.bucket);
            match page.continuation_token {
                Some(t) if !t.is_empty() => token = Some(t),
                _ => return Ok(buckets),
            }
        }
    }

    /// One page of ListObjectsV2 and the token of the next, if any.
    pub async fn list_page(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        token: Option<&str>,
    ) -> anyhow::Result<(ListPage, Option<String>)> {
        let mut query = vec![("list-type", "2"), ("prefix", prefix)];
        query.extend(delimiter.map(|d| ("delimiter", d)));
        query.extend(token.map(|t| ("continuation-token", t)));
        let resp = self
            .send(Method::GET, bucket, "", &query, HeaderMap::new(), None)
            .await?;
        let page: ListPage = parse(&resp.text().await?)?;
        let next = match &page.next_continuation_token {
            Some(t) if page.is_truncated => Some(t.clone()),
            _ => None,
        };
        Ok((page, next))
    }

    /// Every object under `prefix`, across pages.
    pub async fn list_all(&self, bucket: &str, prefix: &str) -> anyhow::Result<Vec<Object>> {
        let mut objects = Vec::new();
        let mut token = None;
        loop {
            let (page, next) = self
                .list_page(bucket, prefix, None, token.as_deref())
                .await?;
            objects.extend(page.contents);
            match next {
                Some(t) => token = Some(t),
                None => return Ok(objects),
            }
        }
    }

    pub async fn create_bucket(&self, bucket: &str) -> anyhow::Result<()> {
        // As the aws CLI does: AWS rejects a CreateBucket without a location outside the
        // region buckets defaulted to before regions existed.
        let body = (self.region != "us-east-1").then(|| {
            format!(
                "<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                 <LocationConstraint>{}</LocationConstraint></CreateBucketConfiguration>",
                self.region
            )
        });
        self.send(
            Method::PUT,
            bucket,
            "",
            &[],
            HeaderMap::new(),
            body.map(Into::into),
        )
        .await?;
        Ok(())
    }

    pub async fn delete_bucket(&self, bucket: &str) -> anyhow::Result<()> {
        self.send(Method::DELETE, bucket, "", &[], HeaderMap::new(), None)
            .await?;
        Ok(())
    }

    /// A single PUT; `len` is the body's length, which S3 requires up front.
    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: reqwest::Body,
        len: u64,
    ) -> anyhow::Result<()> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_LENGTH, HeaderValue::from(len));
        self.send(Method::PUT, bucket, key, &[], headers, Some(body))
            .await?;
        Ok(())
    }

    pub async fn get_object(&self, bucket: &str, key: &str) -> anyhow::Result<reqwest::Response> {
        self.send(Method::GET, bucket, key, &[], HeaderMap::new(), None)
            .await
    }

    pub async fn head_object(&self, bucket: &str, key: &str) -> anyhow::Result<HeaderMap> {
        let resp = self
            .send(Method::HEAD, bucket, key, &[], HeaderMap::new(), None)
            .await?;
        Ok(resp.headers().clone())
    }

    pub async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        bucket: &str,
        key: &str,
    ) -> anyhow::Result<()> {
        let source = format!("/{src_bucket}/{}", utf8_percent_encode(src_key, KEY));
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-amz-copy-source"),
            HeaderValue::from_str(&source)?,
        );
        let resp = self
            .send(Method::PUT, bucket, key, &[], headers, None)
            .await?;
        Self::checked_body(resp).await?;
        Ok(())
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> anyhow::Result<()> {
        self.send(Method::DELETE, bucket, key, &[], HeaderMap::new(), None)
            .await?;
        Ok(())
    }

    pub async fn create_multipart_upload(&self, bucket: &str, key: &str) -> anyhow::Result<String> {
        let resp = self
            .send(
                Method::POST,
                bucket,
                key,
                &[("uploads", "")],
                HeaderMap::new(),
                None,
            )
            .await?;
        let result: InitiateMultipartUploadResult = parse(&resp.text().await?)?;
        Ok(result.upload_id)
    }

    /// Uploads one part and returns its ETag, which completing the upload names it by.
    pub async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        number: u32,
        data: Bytes,
    ) -> anyhow::Result<(u32, String)> {
        let number_text = number.to_string();
        let query = [
            ("partNumber", number_text.as_str()),
            ("uploadId", upload_id),
        ];
        let resp = self
            .send(
                Method::PUT,
                bucket,
                key,
                &query,
                HeaderMap::new(),
                Some(data.into()),
            )
            .await?;
        let etag = resp
            .headers()
            .get(http::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .with_context(|| format!("part {number}: the response has no ETag"))?;
        Ok((number, etag.to_owned()))
    }

    pub async fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[(u32, String)],
    ) -> anyhow::Result<()> {
        let mut body = String::from("<CompleteMultipartUpload>");
        for (number, etag) in parts {
            // ETags are quoted hex, with nothing to escape in XML.
            body.push_str(&format!(
                "<Part><PartNumber>{number}</PartNumber><ETag>{etag}</ETag></Part>"
            ));
        }
        body.push_str("</CompleteMultipartUpload>");
        let resp = self
            .send(
                Method::POST,
                bucket,
                key,
                &[("uploadId", upload_id)],
                HeaderMap::new(),
                Some(body.into()),
            )
            .await?;
        Self::checked_body(resp).await?;
        Ok(())
    }

    pub async fn abort_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> anyhow::Result<()> {
        self.send(
            Method::DELETE,
            bucket,
            key,
            &[("uploadId", upload_id)],
            HeaderMap::new(),
            None,
        )
        .await?;
        Ok(())
    }

    /// A URL that grants `method` on the object to whoever holds it, until `expires` passes.
    pub async fn presign(
        &self,
        method: Method,
        bucket: &str,
        key: &str,
        expires: Duration,
    ) -> anyhow::Result<String> {
        let url = self.url(bucket, key, &[]);
        let parts = self
            .sign(method, &url, HeaderMap::new(), Some(expires))
            .await?;
        Ok(parts.uri.to_string())
    }
}

fn request_id(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-amz-request-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

fn error_from(status: StatusCode, request_id: Option<String>, body: &str) -> S3Error {
    let doc = quick_xml::de::from_str::<ErrorDocument>(body).ok();
    S3Error {
        status,
        code: doc.as_ref().map(|d| d.code.clone()),
        message: doc.as_ref().and_then(|d| d.message.clone()),
        // The header is what AWS support asks for; the document repeats it, when there is one.
        request_id: request_id.or_else(|| doc.and_then(|d| d.request_id)),
    }
}

fn parse<T: for<'de> Deserialize<'de>>(text: &str) -> anyhow::Result<T> {
    quick_xml::de::from_str(text).context("cannot parse the response")
}

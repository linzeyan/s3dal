// AWS Signature Version 4 with WebCrypto, for the requests the console sends and the URLs it
// hands out. The secret key never leaves the browser; the server checks each signature as it
// checks any S3 client's.

const encoder = new TextEncoder();

function hex(buffer) {
  return Array.from(new Uint8Array(buffer), (b) => b.toString(16).padStart(2, "0")).join("");
}

export async function sha256Hex(data) {
  const bytes = typeof data === "string" ? encoder.encode(data) : data;
  return hex(await crypto.subtle.digest("SHA-256", bytes));
}

async function hmac(key, message) {
  const raw = typeof key === "string" ? encoder.encode(key) : key;
  const k = await crypto.subtle.importKey("raw", raw, { name: "HMAC", hash: "SHA-256" }, false, [
    "sign",
  ]);
  return crypto.subtle.sign("HMAC", k, encoder.encode(message));
}

// S3's UriEncode: RFC 3986 unreserved characters stay, everything else is percent-encoded.
// encodeURIComponent leaves !'()* alone, which S3 signs encoded.
export function uriEncode(s) {
  return encodeURIComponent(s).replace(
    /[!'()*]/g,
    (c) => "%" + c.charCodeAt(0).toString(16).toUpperCase(),
  );
}

// A key as a path: each segment encoded, the slashes kept.
export function encodePath(key) {
  return key.split("/").map(uriEncode).join("/");
}

export function canonicalQuery(query) {
  return query
    .map(([k, v]) => [uriEncode(k), uriEncode(v ?? "")])
    .sort(([a, x], [b, y]) => (a < b ? -1 : a > b ? 1 : x < y ? -1 : x > y ? 1 : 0))
    .map(([k, v]) => `${k}=${v}`)
    .join("&");
}

function amzDate(now) {
  return now.toISOString().replace(/[-:]/g, "").replace(/\.\d{3}/, "");
}

async function signingKey(secretKey, day, region) {
  const kDate = await hmac("AWS4" + secretKey, day);
  const kRegion = await hmac(kDate, region);
  const kService = await hmac(kRegion, "s3");
  return hmac(kService, "aws4_request");
}

async function signature(creds, now, canonicalRequest) {
  const stamp = amzDate(now);
  const day = stamp.slice(0, 8);
  const scope = `${day}/${creds.region}/s3/aws4_request`;
  const toSign = [
    "AWS4-HMAC-SHA256",
    stamp,
    scope,
    await sha256Hex(canonicalRequest),
  ].join("\n");
  const key = await signingKey(creds.secretKey, day, creds.region);
  return { stamp, scope, signature: hex(await hmac(key, toSign)) };
}

// Headers for a request signed in its Authorization header. `headers` are lowercase names the
// request sends and the signature covers; `host` is the page's own, which the browser sends.
export async function signHeaders(creds, { method, path, query, headers, payloadHash, now }) {
  const stamp = amzDate(now);
  const all = {
    ...headers,
    host: location.host,
    "x-amz-content-sha256": payloadHash,
    "x-amz-date": stamp,
  };
  const names = Object.keys(all).sort();
  const canonicalHeaders = names.map((n) => `${n}:${String(all[n]).trim()}\n`).join("");
  const signedHeaders = names.join(";");
  const canonicalRequest = [
    method,
    path,
    canonicalQuery(query),
    canonicalHeaders,
    signedHeaders,
    payloadHash,
  ].join("\n");
  const sig = await signature(creds, now, canonicalRequest);
  const out = { ...all };
  delete out.host;
  out.authorization =
    `AWS4-HMAC-SHA256 Credential=${creds.accessKey}/${sig.scope}, ` +
    `SignedHeaders=${signedHeaders}, Signature=${sig.signature}`;
  return out;
}

// A presigned URL's query: anyone holding the URL can make this one request until it expires,
// without the secret key.
export async function presignQuery(creds, { method, path, query, expires, now }) {
  const stamp = amzDate(now);
  const day = stamp.slice(0, 8);
  const full = [
    ...query,
    ["X-Amz-Algorithm", "AWS4-HMAC-SHA256"],
    ["X-Amz-Credential", `${creds.accessKey}/${day}/${creds.region}/s3/aws4_request`],
    ["X-Amz-Date", stamp],
    ["X-Amz-Expires", String(expires)],
    ["X-Amz-SignedHeaders", "host"],
  ];
  const canonicalRequest = [
    method,
    path,
    canonicalQuery(full),
    `host:${location.host}\n`,
    "host",
    "UNSIGNED-PAYLOAD",
  ].join("\n");
  const sig = await signature(creds, now, canonicalRequest);
  return canonicalQuery([...full, ["X-Amz-Signature", sig.signature]]);
}

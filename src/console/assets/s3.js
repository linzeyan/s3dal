// The S3 API as the console uses it, on the console's own origin under /s3. Every request is
// signed here with the key pair the user signed in with.

import { encodePath, canonicalQuery, presignQuery, sha256Hex, signHeaders } from "./sigv4.js";

const API = "/s3";
const EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

// Above this a file goes up in parts, so a dropped connection costs one part, not the file,
// and several parts travel at once.
const MULTIPART_THRESHOLD = 32 * 1024 * 1024;
const PART_SIZE = 16 * 1024 * 1024;
const PART_CONCURRENCY = 4;

export class S3Error extends Error {
  constructor(status, code, message, requestId) {
    super(message || code || `HTTP ${status}`);
    this.status = status;
    this.code = code;
    this.requestId = requestId;
  }
}

function xml(text) {
  return new DOMParser().parseFromString(text, "application/xml");
}

function text(node, name) {
  return node.getElementsByTagName(name)[0]?.textContent ?? "";
}

function children(node, name) {
  return Array.from(node.getElementsByTagName(name));
}

// The SHA-512 of a file or part, as S3 sends checksums: base64. Read into memory only while it
// is hashed; the upload sends the Blob again, which the browser streams from disk, so a part's
// bytes are not held for as long as it travels.
async function sha512(blob) {
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-512", await blob.arrayBuffer()));
  return btoa(String.fromCharCode(...digest));
}

const CHECKSUMS = ["crc32", "crc32c", "crc64nvme", "sha1", "sha256", "sha512", "md5"];

async function errorFrom(res) {
  const body = res.status === 204 || !res.body ? "" : await res.text();
  const requestId = res.headers.get("x-amz-request-id") ?? "";
  if (body.includes("<Error>")) {
    const doc = xml(body);
    return new S3Error(res.status, text(doc, "Code"), text(doc, "Message"), requestId);
  }
  const code = { 403: "AccessDenied", 404: "NotFound" }[res.status] ?? "";
  return new S3Error(res.status, code, "", requestId);
}

export function objectPath(bucket, key = "") {
  return key ? `/${bucket}/${encodePath(key)}` : `/${bucket}`;
}

export class S3 {
  constructor({ accessKey, secretKey, region }) {
    this.creds = { accessKey, secretKey, region };
    // Corrected from the server's clock when it refuses ours: a laptop's clock a quarter of an
    // hour off would otherwise fail every request with RequestTimeTooSkewed.
    this.clockOffset = 0;
  }

  now() {
    return new Date(Date.now() + this.clockOffset);
  }

  async signed(method, path, { query = [], headers = {}, payloadHash = EMPTY_SHA256 } = {}) {
    const signed = await signHeaders(this.creds, {
      method,
      path,
      query,
      headers,
      payloadHash,
      now: this.now(),
    });
    const qs = canonicalQuery(query);
    return { url: API + path + (qs ? "?" + qs : ""), headers: signed };
  }

  async request(method, path, { query = [], headers = {}, body, signal, retried } = {}) {
    let payloadHash = EMPTY_SHA256;
    if (body !== undefined) {
      payloadHash = await sha256Hex(typeof body === "string" ? body : new Uint8Array(body));
    }
    const req = await this.signed(method, path, { query, headers, payloadHash });
    const res = await fetch(req.url, { method, headers: req.headers, body, signal });
    if (res.ok) return res;
    const err = await errorFrom(res);
    if (err.code === "RequestTimeTooSkewed" && !retried) {
      const server = Date.parse(res.headers.get("date") ?? "");
      if (!Number.isNaN(server)) {
        this.clockOffset = server - Date.now();
        return this.request(method, path, { query, headers, body, signal, retried: true });
      }
    }
    throw err;
  }

  // Buckets

  async listBuckets() {
    const doc = xml(await (await this.request("GET", "/")).text());
    return children(doc, "Bucket").map((b) => ({
      name: text(b, "Name"),
      created: new Date(text(b, "CreationDate")),
    }));
  }

  createBucket(name) {
    return this.request("PUT", objectPath(name));
  }

  deleteBucket(name) {
    return this.request("DELETE", objectPath(name));
  }

  // Objects

  // One page of the level under `prefix`: the folders and the objects in it.
  async list(bucket, prefix, token, { delimiter = "/" } = {}) {
    const query = [
      ["list-type", "2"],
      ["encoding-type", "url"],
      ["prefix", prefix],
    ];
    if (delimiter) query.push(["delimiter", delimiter]);
    if (token) query.push(["continuation-token", token]);
    const doc = xml(await (await this.request("GET", objectPath(bucket), { query })).text());
    const decode = (s) => decodeURIComponent(s);
    return {
      folders: children(doc, "CommonPrefixes").map((p) => decode(text(p, "Prefix"))),
      objects: children(doc, "Contents").map((c) => ({
        key: decode(text(c, "Key")),
        size: Number(text(c, "Size")),
        modified: new Date(text(c, "LastModified")),
        etag: text(c, "ETag"),
      })),
      next: text(doc, "IsTruncated") === "true" ? text(doc, "NextContinuationToken") : null,
    };
  }

  // Every key under `prefix`, for deleting a folder.
  async *allKeys(bucket, prefix) {
    let token = null;
    do {
      const page = await this.list(bucket, prefix, token, { delimiter: "" });
      for (const o of page.objects) yield o.key;
      token = page.next;
    } while (token);
  }

  async head(bucket, key) {
    const res = await this.request("HEAD", objectPath(bucket, key), {
      headers: { "x-amz-checksum-mode": "ENABLED" },
    });
    const metadata = [];
    res.headers.forEach((value, name) => {
      if (name.startsWith("x-amz-meta-")) metadata.push([name.slice(11), value]);
    });
    const checksums = CHECKSUMS.flatMap((alg) => {
      const value = res.headers.get(`x-amz-checksum-${alg}`);
      return value ? [{ algorithm: alg, value, type: res.headers.get("x-amz-checksum-type") ?? "" }] : [];
    });
    return {
      checksums,
      size: Number(res.headers.get("content-length")),
      contentType: res.headers.get("content-type") ?? "",
      etag: res.headers.get("etag") ?? "",
      modified: new Date(res.headers.get("last-modified") ?? ""),
      cacheControl: res.headers.get("cache-control") ?? "",
      contentEncoding: res.headers.get("content-encoding") ?? "",
      metadata,
    };
  }

  async exists(bucket, key) {
    try {
      await this.request("HEAD", objectPath(bucket, key));
      return true;
    } catch (err) {
      if (err.status === 404) return false;
      throw err;
    }
  }

  // The first bytes of an object, for a text preview.
  async readStart(bucket, key, bytes) {
    const res = await this.request("GET", objectPath(bucket, key), {
      headers: { range: `bytes=0-${bytes - 1}` },
    });
    return res.arrayBuffer();
  }

  deleteObject(bucket, key) {
    return this.request("DELETE", objectPath(bucket, key));
  }

  // A folder as consoles make one: an empty object whose key ends in "/".
  createFolder(bucket, prefix) {
    return this.request("PUT", objectPath(bucket, prefix), { body: "" });
  }

  async presign(bucket, key, { expires = 3600, download = false } = {}) {
    const path = objectPath(bucket, key);
    const query = [];
    if (download) {
      const name = key.split("/").pop();
      query.push(["response-content-disposition", `attachment; filename*=UTF-8''${encodeURIComponent(name)}`]);
    }
    const qs = await presignQuery(this.creds, {
      method: "GET",
      path,
      query,
      expires,
      now: this.now(),
    });
    return `${location.origin}${API}${path}?${qs}`;
  }

  // Uploads `file`, reporting progress in bytes. A single PUT up to the threshold, parts above
  // it. Each body carries its SHA-512, which the server checks and keeps: a whole file's for a
  // PUT, each part's for a multipart upload (S3 has no whole-object SHA checksum for those, only
  // the composite of the parts'). `ifNoneMatch` fails the upload with PreconditionFailed if the
  // key exists by the time it would be written. `signal` aborts it; an aborted multipart upload
  // is removed from the server.
  async upload(bucket, key, file, { onProgress = () => {}, signal, ifNoneMatch = false } = {}) {
    const contentType = file.type || "application/octet-stream";
    const condition = ifNoneMatch ? { "if-none-match": "*" } : {};
    const path = objectPath(bucket, key);
    if (file.size <= MULTIPART_THRESHOLD) {
      const headers = { "content-type": contentType, "x-amz-checksum-sha512": await sha512(file), ...condition };
      await this.send("PUT", path, [], file, headers, onProgress, signal);
      return;
    }
    const created = await this.request("POST", path, {
      query: [["uploads", ""]],
      headers: { "content-type": contentType, "x-amz-checksum-algorithm": "SHA512" },
    });
    const uploadId = text(xml(await created.text()), "UploadId");
    const loaded = new Map();
    const report = () => onProgress([...loaded.values()].reduce((a, b) => a + b, 0));
    try {
      const count = Math.ceil(file.size / PART_SIZE);
      const parts = new Array(count);
      let next = 0;
      const worker = async () => {
        while (next < count) {
          const n = next++;
          const part = file.slice(n * PART_SIZE, Math.min(file.size, (n + 1) * PART_SIZE));
          const query = [
            ["partNumber", String(n + 1)],
            ["uploadId", uploadId],
          ];
          const checksum = await sha512(part);
          const etag = await this.send("PUT", path, query, part, { "x-amz-checksum-sha512": checksum }, (b) => {
            loaded.set(n, b);
            report();
          }, signal);
          parts[n] = { etag, checksum };
        }
      };
      await Promise.all(Array.from({ length: Math.min(PART_CONCURRENCY, count) }, worker));
      const body =
        "<CompleteMultipartUpload>" +
        parts.map((p, i) => `<Part><PartNumber>${i + 1}</PartNumber><ETag>${p.etag}</ETag><ChecksumSHA512>${p.checksum}</ChecksumSHA512></Part>`).join("") +
        "</CompleteMultipartUpload>";
      const done = await this.request("POST", path, { query: [["uploadId", uploadId]], body, headers: condition });
      // Complete answers 200 and may still carry an error, sent after the parts were joined.
      const answer = await done.text();
      if (answer.includes("<Error>")) {
        const doc = xml(answer);
        throw new S3Error(200, text(doc, "Code"), text(doc, "Message"), "");
      }
    } catch (err) {
      this.request("DELETE", path, { query: [["uploadId", uploadId]] }).catch(() => {});
      throw err;
    }
  }

  // One body upload with XMLHttpRequest, which unlike fetch reports upload progress. The body is
  // not hashed (UNSIGNED-PAYLOAD): hashing a large file first would read it twice. Returns the
  // ETag.
  async send(method, path, query, body, headers, onProgress, signal) {
    const req = await this.signed(method, path, {
      query,
      headers,
      payloadHash: "UNSIGNED-PAYLOAD",
    });
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open(method, req.url);
      for (const [k, v] of Object.entries(req.headers)) xhr.setRequestHeader(k, v);
      xhr.upload.onprogress = (e) => onProgress(e.loaded);
      xhr.onload = () => {
        if (xhr.status >= 200 && xhr.status < 300) {
          onProgress(body.size);
          resolve(xhr.getResponseHeader("etag") ?? "");
        } else {
          const doc = xml(xhr.responseText || "<x/>");
          reject(
            new S3Error(
              xhr.status,
              text(doc, "Code"),
              text(doc, "Message"),
              xhr.getResponseHeader("x-amz-request-id") ?? "",
            ),
          );
        }
      };
      xhr.onerror = () => reject(new S3Error(0, "NetworkError", "The connection failed"));
      xhr.onabort = () => reject(new DOMException("Upload cancelled", "AbortError"));
      signal?.addEventListener("abort", () => xhr.abort(), { once: true });
      xhr.send(body);
    });
  }

  // The server's own settings API, answered on the console's listener only.

  // What the signed-in key pair may reach; the one request a pair with limits may make here.
  async whoami() {
    return (await this.request("GET", "/_s3dal/admin/whoami")).json();
  }

  async info() {
    return (await this.request("GET", "/_s3dal/admin/info")).json();
  }

  async config() {
    return (await this.request("GET", "/_s3dal/admin/config")).json();
  }

  async saveConfig(edit) {
    const body = JSON.stringify(edit);
    const res = await this.request("PUT", "/_s3dal/admin/config", {
      body,
      headers: { "content-type": "application/json" },
    });
    return res.json();
  }
}

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

function escapeXml(s) {
  return s.replace(/[&<>"']/g, (c) => `&#${c.charCodeAt(0)};`);
}

// Within a key: the latest first, then newest to oldest. S3 sends versions and delete markers
// as two lists, which lose the order between them; the sort is stable, so entries of one kind
// that share a time keep the server's order.
function byKeyThenNewest(a, b) {
  if (a.key !== b.key) return a.key < b.key ? -1 : 1;
  if (a.latest !== b.latest) return a.latest ? -1 : 1;
  return b.modified - a.modified;
}

const versionQuery = (versionId) => (versionId ? [["versionId", versionId]] : []);

// A lifecycle <Rule> as S3.lifecycle describes it. A rule put elsewhere with the older
// rule-level <Prefix> reads the same, and is written back in the <Filter> form.
function parseRule(r) {
  const first = (parent, name) => parent?.getElementsByTagName(name)[0] ?? null;
  const num = (parent, name) => {
    const n = first(parent, name);
    return n ? Number(n.textContent) : null;
  };
  const expiration = first(r, "Expiration");
  const noncurrent = first(r, "NoncurrentVersionExpiration");
  const date = first(expiration, "Date")?.textContent;
  // Kept whole, every one of them: the page edits the first of each kind, and a rule saved
  // without the rest would lose them.
  const transitions = children(r, "Transition").map((t) => ({
    days: num(t, "Days"),
    date: first(t, "Date")?.textContent.slice(0, 10) ?? null,
    storageClass: text(t, "StorageClass"),
  }));
  const noncurrentTransitions = children(r, "NoncurrentVersionTransition").map((t) => ({
    days: num(t, "NoncurrentDays"),
    keepNewer: num(t, "NewerNoncurrentVersions"),
    storageClass: text(t, "StorageClass"),
  }));
  return {
    id: text(r, "ID"),
    enabled: text(r, "Status") === "Enabled",
    prefix: text(r, "Prefix"),
    larger: num(r, "ObjectSizeGreaterThan"),
    smaller: num(r, "ObjectSizeLessThan"),
    expireDays: num(expiration, "Days"),
    // Always a midnight UTC, so its first ten characters are the day.
    expireDate: date ? date.slice(0, 10) : null,
    deleteMarkers: first(expiration, "ExpiredObjectDeleteMarker")?.textContent === "true",
    noncurrentDays: num(noncurrent, "NoncurrentDays"),
    keepNewer: num(noncurrent, "NewerNoncurrentVersions"),
    abortDays: num(first(r, "AbortIncompleteMultipartUpload"), "DaysAfterInitiation"),
    transitions,
    noncurrentTransitions,
  };
}

function ruleXml(r) {
  const el = (name, value) => (value == null || value === "" ? "" : `<${name}>${escapeXml(String(value))}</${name}>`);
  const conditions = [el("Prefix", r.prefix), el("ObjectSizeGreaterThan", r.larger), el("ObjectSizeLessThan", r.smaller)].filter(Boolean);
  // S3 takes several conditions only inside <And>.
  const filter = conditions.length > 1 ? `<And>${conditions.join("")}</And>` : conditions.join("");
  const expiration = el("Days", r.expireDays) || el("Date", r.expireDate && `${r.expireDate}T00:00:00Z`) ||
    (r.deleteMarkers ? el("ExpiredObjectDeleteMarker", "true") : "");
  const noncurrent = r.noncurrentDays == null ? "" : el("NoncurrentDays", r.noncurrentDays) + el("NewerNoncurrentVersions", r.keepNewer);
  const transitions = (r.transitions ?? []).map((t) =>
    `<Transition>${el("Days", t.days) || el("Date", t.date && `${t.date}T00:00:00Z`)}${el("StorageClass", t.storageClass)}</Transition>`).join("");
  const noncurrentTransitions = (r.noncurrentTransitions ?? []).map((t) =>
    `<NoncurrentVersionTransition>${el("NoncurrentDays", t.days)}${el("NewerNoncurrentVersions", t.keepNewer)}${el("StorageClass", t.storageClass)}</NoncurrentVersionTransition>`).join("");
  return `<Rule>${el("ID", r.id)}<Filter>${filter}</Filter><Status>${r.enabled ? "Enabled" : "Disabled"}</Status>` +
    transitions + noncurrentTransitions +
    (expiration && `<Expiration>${expiration}</Expiration>`) +
    (noncurrent && `<NoncurrentVersionExpiration>${noncurrent}</NoncurrentVersionExpiration>`) +
    (r.abortDays == null ? "" : `<AbortIncompleteMultipartUpload>${el("DaysAfterInitiation", r.abortDays)}</AbortIncompleteMultipartUpload>`) +
    "</Rule>";
}

// The three lists of a notification configuration, by their XML element and the element of
// their ARN; s3dal sends every one to a webhook alike.
const NOTIFICATION_LISTS = [
  ["queue", "QueueConfiguration", "Queue"],
  ["topic", "TopicConfiguration", "Topic"],
  ["lambda", "CloudFunctionConfiguration", "CloudFunction"],
];

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

  // "Enabled", "Suspended", or "" for a bucket that never had versioning.
  async versioning(bucket) {
    const res = await this.request("GET", objectPath(bucket), { query: [["versioning", ""]] });
    return text(xml(await res.text()), "Status");
  }

  setVersioning(bucket, status) {
    const body = `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>${status}</Status></VersioningConfiguration>`;
    return this.request("PUT", objectPath(bucket), { query: [["versioning", ""]], body });
  }

  // The bucket's lifecycle rules, [] for a bucket without any, as plain objects:
  // { id, enabled, prefix, larger, smaller, expireDays, expireDate, deleteMarkers,
  //   noncurrentDays, keepNewer, abortDays, transitions, noncurrentTransitions }, a part the
  // rule lacks being null. `transitions` are { days, date, storageClass },
  // `noncurrentTransitions` { days, keepNewer, storageClass }; both [] when it has none.
  async lifecycle(bucket) {
    let res;
    try {
      res = await this.request("GET", objectPath(bucket), { query: [["lifecycle", ""]] });
    } catch (err) {
      if (err.code === "NoSuchLifecycleConfiguration") return [];
      throw err;
    }
    return children(xml(await res.text()), "Rule").map(parseRule);
  }

  // Replaces the bucket's rules; none removes the configuration, as S3 takes no empty one.
  setLifecycle(bucket, rules) {
    const query = [["lifecycle", ""]];
    if (!rules.length) return this.request("DELETE", objectPath(bucket), { query });
    const body = `<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">${rules.map(ruleXml).join("")}</LifecycleConfiguration>`;
    return this.request("PUT", objectPath(bucket), { query, body });
  }

  // The bucket's event notification rules, [] for a bucket without any, as plain objects:
  // { id, list, arn, events, prefix, suffix }. `list` is the configuration list the rule came in
  // ("queue", "topic" or "lambda"), so a rule put by another tool is written back where it was.
  async notification(bucket) {
    const res = await this.request("GET", objectPath(bucket), { query: [["notification", ""]] });
    const doc = xml(await res.text());
    return NOTIFICATION_LISTS.flatMap(([list, element, arnElement]) =>
      children(doc, element).map((c) => {
        const filter = (name) => children(c, "FilterRule").find((f) => text(f, "Name").toLowerCase() === name);
        return {
          id: text(c, "Id"),
          list,
          arn: text(c, arnElement),
          events: children(c, "Event").map((e) => e.textContent),
          prefix: filter("prefix") ? text(filter("prefix"), "Value") : "",
          suffix: filter("suffix") ? text(filter("suffix"), "Value") : "",
        };
      }));
  }

  // Replaces the bucket's rules; none removes them, as S3 does with an empty configuration.
  setNotification(bucket, rules) {
    const el = (name, value) => `<${name}>${escapeXml(value)}</${name}>`;
    const body = '<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
      rules.map((r) => {
        const [, element, arnElement] = NOTIFICATION_LISTS.find(([list]) => list === (r.list ?? "queue"));
        const filters = [["prefix", r.prefix], ["suffix", r.suffix]].filter(([, v]) => v)
          .map(([name, value]) => `<FilterRule>${el("Name", name)}${el("Value", value)}</FilterRule>`).join("");
        return `<${element}>${r.id ? el("Id", r.id) : ""}${el(arnElement, r.arn)}` +
          r.events.map((e) => el("Event", e)).join("") +
          (filters && `<Filter><S3Key>${filters}</S3Key></Filter>`) + `</${element}>`;
      }).join("") +
      "</NotificationConfiguration>";
    return this.request("PUT", objectPath(bucket), { query: [["notification", ""]], body });
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
        storageClass: text(c, "StorageClass") || "STANDARD",
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

  // One page of the versions and delete markers under `prefix`, with the folders of the level
  // when `delimiter` is set. `next` continues where the previous page stopped.
  async listVersions(bucket, prefix, { delimiter = "/", next = null, maxKeys } = {}) {
    const query = [
      ["versions", ""],
      ["encoding-type", "url"],
      ["prefix", prefix],
    ];
    if (delimiter) query.push(["delimiter", delimiter]);
    if (maxKeys) query.push(["max-keys", String(maxKeys)]);
    if (next) query.push(["key-marker", next.key], ["version-id-marker", next.versionId]);
    const doc = xml(await (await this.request("GET", objectPath(bucket), { query })).text());
    const decode = (s) => decodeURIComponent(s);
    const entry = (node, marker) => ({
      key: decode(text(node, "Key")),
      versionId: text(node, "VersionId"),
      latest: text(node, "IsLatest") === "true",
      modified: new Date(text(node, "LastModified")),
      size: marker ? 0 : Number(text(node, "Size")),
      etag: marker ? "" : text(node, "ETag"),
      storageClass: marker ? "" : text(node, "StorageClass") || "STANDARD",
      marker,
    });
    const versions = [...children(doc, "Version").map((v) => entry(v, false)), ...children(doc, "DeleteMarker").map((m) => entry(m, true))];
    return {
      folders: children(doc, "CommonPrefixes").map((p) => decode(text(p, "Prefix"))),
      versions: versions.sort(byKeyThenNewest),
      next: text(doc, "IsTruncated") === "true"
        ? { key: decode(text(doc, "NextKeyMarker")), versionId: text(doc, "NextVersionIdMarker") }
        : null,
    };
  }

  // Every version and delete marker of one key, the latest first.
  async keyVersions(bucket, key) {
    const out = [];
    let next = null;
    do {
      const page = await this.listVersions(bucket, key, { delimiter: "", next });
      const mine = page.versions.filter((v) => v.key === key);
      out.push(...mine);
      // Longer keys sharing the prefix sort after this one's versions: once one shows, the
      // key's versions are all read.
      next = mine.length === page.versions.length ? page.next : null;
    } while (next);
    return out.sort(byKeyThenNewest);
  }

  // Deletes versions and markers, up to 1000, in one request. Answers with those it could not.
  async deleteVersions(bucket, entries) {
    const body = "<Delete><Quiet>true</Quiet>" +
      entries.map((e) => `<Object><Key>${escapeXml(e.key)}</Key><VersionId>${escapeXml(e.versionId)}</VersionId></Object>`).join("") +
      "</Delete>";
    const res = await this.request("POST", objectPath(bucket), { query: [["delete", ""]], body });
    return children(xml(await res.text()), "Error").map((e) => ({ key: text(e, "Key"), code: text(e, "Code"), message: text(e, "Message") }));
  }

  // Copies a version of `key` onto the key: a new latest version with its content, the S3 way
  // of restoring, so the versions in between are kept.
  async restoreVersion(bucket, key, versionId) {
    const res = await this.request("PUT", objectPath(bucket, key), {
      headers: { "x-amz-copy-source": `${objectPath(bucket, key)}?versionId=${encodeURIComponent(versionId)}` },
    });
    // Like Complete, a copy answers 200 and may still carry an error.
    const answer = await res.text();
    if (answer.includes("<Error>")) {
      const doc = xml(answer);
      throw new S3Error(200, text(doc, "Code"), text(doc, "Message"), "");
    }
  }

  async head(bucket, key, versionId) {
    const res = await this.request("HEAD", objectPath(bucket, key), {
      query: versionQuery(versionId),
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
      versionId: res.headers.get("x-amz-version-id") ?? "",
      size: Number(res.headers.get("content-length")),
      contentType: res.headers.get("content-type") ?? "",
      etag: res.headers.get("etag") ?? "",
      modified: new Date(res.headers.get("last-modified") ?? ""),
      cacheControl: res.headers.get("cache-control") ?? "",
      contentEncoding: res.headers.get("content-encoding") ?? "",
      // S3 leaves STANDARD out.
      storageClass: res.headers.get("x-amz-storage-class") ?? "STANDARD",
      metadata,
    };
  }

  // Copies the object onto itself in `storageClass`, as S3 changes a class: into a cold tier or
  // back to STANDARD. A new write: Last-Modified moves, and a versioned bucket gets a version.
  async changeStorageClass(bucket, key, storageClass) {
    const res = await this.request("PUT", objectPath(bucket, key), {
      headers: { "x-amz-copy-source": objectPath(bucket, key), "x-amz-storage-class": storageClass },
    });
    // Like Complete, a copy answers 200 and may still carry an error.
    const answer = await res.text();
    if (answer.includes("<Error>")) {
      const doc = xml(answer);
      throw new S3Error(200, text(doc, "Code"), text(doc, "Message"), "");
    }
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

  // Without `versionId`, in a versioned bucket, this adds a delete marker; with it, it removes
  // that version for good.
  deleteObject(bucket, key, versionId) {
    return this.request("DELETE", objectPath(bucket, key), { query: versionQuery(versionId) });
  }

  // A folder as consoles make one: an empty object whose key ends in "/".
  createFolder(bucket, prefix) {
    return this.request("PUT", objectPath(bucket, prefix), { body: "" });
  }

  // `thumbnail` (128, 256, 512 or 1024) asks for the image scaled down to fit that square, made
  // and cached by the server; the size is signed with the rest of the URL.
  async presign(bucket, key, { expires = 3600, download = false, versionId, thumbnail } = {}) {
    const path = objectPath(bucket, key);
    const query = versionQuery(versionId);
    if (thumbnail) query.push(["x-s3dal-thumbnail", String(thumbnail)]);
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
  // key exists by the time it would be written. `storageClass`, other than STANDARD, writes it
  // straight into that class's cold tier. `signal` aborts it; an aborted multipart upload is
  // removed from the server.
  async upload(bucket, key, file, { onProgress = () => {}, signal, ifNoneMatch = false, storageClass = "STANDARD" } = {}) {
    const contentType = file.type || "application/octet-stream";
    const condition = ifNoneMatch ? { "if-none-match": "*" } : {};
    const classHeader = storageClass === "STANDARD" ? {} : { "x-amz-storage-class": storageClass };
    const path = objectPath(bucket, key);
    if (file.size <= MULTIPART_THRESHOLD) {
      const headers = { "content-type": contentType, "x-amz-checksum-sha512": await sha512(file), ...condition, ...classHeader };
      await this.send("PUT", path, [], file, headers, onProgress, signal);
      return;
    }
    const created = await this.request("POST", path, {
      query: [["uploads", ""]],
      headers: { "content-type": contentType, "x-amz-checksum-algorithm": "SHA512", ...classHeader },
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

  async stats() {
    return (await this.request("GET", "/_s3dal/admin/stats")).json();
  }

  async config() {
    return (await this.request("GET", "/_s3dal/admin/config")).json();
  }

  // What every bucket holds and its quota: { buckets: [{ name, usage, quota }], scan_interval_secs }.
  async usage() {
    return (await this.request("GET", "/_s3dal/admin/usage")).json();
  }

  // A bucket's settings that S3 has no request for, and what it holds:
  // { public, quota, usage, notify, tiers }. `tiers` are the cold tiers, { name, storage_class },
  // whose classes a write may name. `quota` is { max_bytes, max_objects } or null. A pair with
  // limits may read those of a bucket it reaches. `notify` lists the server's webhooks,
  // { id, arn, url, auth_token, hmac, delivered, failed, dropped, queued, last }, `last` being
  // { at, error } or null; it is null for a pair with limits.
  async bucketSettings(bucket) {
    return (await this.request("GET", `/_s3dal/admin/buckets/${encodeURIComponent(bucket)}`)).json();
  }

  async saveBucketSettings(bucket, edit) {
    const res = await this.request("PUT", `/_s3dal/admin/buckets/${encodeURIComponent(bucket)}`, {
      body: JSON.stringify(edit),
      headers: { "content-type": "application/json" },
    });
    return res.json();
  }

  async saveConfig(edit) {
    const body = JSON.stringify(edit);
    const res = await this.request("PUT", "/_s3dal/admin/config", {
      body,
      headers: { "content-type": "application/json" },
    });
    return res.json();
  }

  // Answers once the server has checked that it would start again with the saved settings;
  // it then stops taking requests and starts again.
  async restart() {
    return (await this.request("POST", "/_s3dal/admin/restart")).json();
  }
}

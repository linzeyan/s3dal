// The console's single page. Routes live in the URL hash, so the browser's back and forward
// buttons and bookmarks work, and the server needs only one page.

import { dashboard } from "./dashboard.js";
import { S3, objectPath } from "./s3.js";
import {
  confirm,
  copyText,
  describeError,
  dialog,
  formatBytes,
  formatDate,
  formatUptime,
  h,
  icon,
  meter,
  percent,
  quotaState,
  toast,
} from "./ui.js";

const REGION = document.querySelector('meta[name="s3dal-region"]').content;
const VERSION = document.querySelector('meta[name="s3dal-version"]').content;
// sessionStorage: the key pair lasts as long as the tab, survives a reload, and is never sent
// anywhere, nor shared with other tabs.
const SESSION = "s3dal-session";

let s3 = null;
let signedInAs = "";
// What the signed-in pair may reach, from the server: { admin, write, allow }. The page shows
// only what it can do; the server checks every request regardless.
let me = null;

const isAdmin = () => me?.admin === true;
const canWrite = () => me?.write === true;

// The folders of `bucket` the pair is limited to, or null when it reaches the whole bucket.
function scopesIn(bucket) {
  if (!me?.allow) return null;
  const prefixes = me.allow.filter((a) => a.bucket === bucket).map((a) => a.prefix);
  return prefixes.includes("") ? null : prefixes;
}

function fail(err) {
  const d = describeError(err);
  if (d) toast(d.message, { kind: "error", detail: d.detail });
}

// Asked again on every load rather than kept: the pair's limits may have changed since.
async function restore() {
  try {
    const { accessKey, secretKey } = JSON.parse(sessionStorage.getItem(SESSION) ?? "null") ?? {};
    if (!accessKey || !secretKey) return;
    const candidate = new S3({ accessKey, secretKey, region: REGION });
    me = await candidate.whoami();
    s3 = candidate;
    signedInAs = accessKey;
  } catch {
    sessionStorage.removeItem(SESSION);
  }
}

function signOut() {
  sessionStorage.removeItem(SESSION);
  s3 = null;
  me = null;
  signedInAs = "";
  history.replaceState(null, "", "#/");
  render();
}

// ----- Generic pieces -----

function logo(cls = "logo") {
  return h("img", { class: cls, src: "/icon.svg", alt: "" });
}

function field(label, input, hint) {
  return h("label", { class: "field" }, h("span", { class: "field-label" }, label), input, hint && h("span", { class: "field-hint" }, hint));
}

function pageHeader(title, actions = [], crumbs) {
  return h("header", { class: "page-header" },
    h("div", { class: "page-title" }, crumbs ?? h("h1", {}, title)),
    h("div", { class: "page-actions" }, actions),
  );
}

function loadingRows(n = 6) {
  return h("div", { class: "skeleton", "aria-busy": "true", "aria-label": "Loading" },
    Array.from({ length: n }, () => h("div", { class: "skeleton-row" })));
}

function emptyState(iconName, title, text, action) {
  return h("div", { class: "empty" }, icon(iconName, "empty-icon"), h("h3", {}, title), h("p", {}, text), action);
}

function errorState(err, retry) {
  const d = describeError(err) ?? { message: "", detail: "" };
  return h("div", { class: "empty" }, icon("alert", "empty-icon danger-text"), h("h3", {}, "Could not load"),
    h("p", {}, d.message), d.detail && h("p", { class: "mono muted small" }, d.detail),
    h("button", { class: "button", onclick: retry }, icon("refresh"), "Try again"));
}

async function pool(items, size, fn) {
  let i = 0;
  const worker = async () => {
    while (i < items.length) {
      const item = items[i++];
      await fn(item);
    }
  };
  await Promise.all(Array.from({ length: Math.min(size, items.length) }, worker));
}

function randomKey(length, alphabet) {
  const bytes = crypto.getRandomValues(new Uint8Array(length));
  return Array.from(bytes, (b) => alphabet[b % alphabet.length]).join("");
}

// 32 and 64 symbols: a byte's value modulo either is unbiased.
function generateKeyPair() {
  return {
    accessKey: randomKey(20, "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"),
    secretKey: randomKey(40, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"),
  };
}

// ----- Sign in -----

function loginView() {
  const error = h("p", { class: "form-error", role: "alert" });
  const access = h("input", { class: "input mono", name: "username", autocomplete: "username", required: true, spellcheck: "false", autocapitalize: "off" });
  const secret = h("input", { class: "input mono", name: "password", type: "password", autocomplete: "current-password", required: true });
  const submit = h("button", { class: "button primary wide", type: "submit" }, "Sign in");
  const form = h("form", {
    class: "login-card",
    onsubmit: async (e) => {
      e.preventDefault();
      submit.disabled = true;
      submit.textContent = "Signing in…";
      error.textContent = "";
      const candidate = new S3({ accessKey: access.value.trim(), secretKey: secret.value, region: REGION });
      try {
        // Any signed request proves the pair; this one also says what it may reach.
        me = await candidate.whoami();
        sessionStorage.setItem(SESSION, JSON.stringify({ accessKey: candidate.creds.accessKey, secretKey: secret.value }));
        s3 = candidate;
        signedInAs = candidate.creds.accessKey;
        render();
      } catch (err) {
        error.textContent = describeError(err)?.message ?? "";
        secret.select();
      } finally {
        submit.disabled = false;
        submit.textContent = "Sign in";
      }
    },
  },
    h("div", { class: "brand brand-large" }, logo(), "s3dal"),
    h("p", { class: "muted" }, "Sign in with a key pair configured on this server."),
    field("Access key", access),
    field("Secret key", secret),
    error,
    submit,
    h("p", { class: "fine-print" }, icon("lock", "icon small"),
      "Requests are signed in this tab; the secret key is never sent. It is forgotten when the tab closes."),
  );
  queueMicrotask(() => access.focus());
  return h("main", { class: "login" }, form);
}

// ----- Shell -----

function shell(active, ...content) {
  const link = (href, iconName, label, name) =>
    h("a", { class: "nav-link", href, "aria-current": active === name ? "page" : null }, icon(iconName), label);
  return h("div", { class: "shell" },
    h("nav", { class: "sidebar", "aria-label": "Main" },
      h("a", { class: "brand", href: "#/" }, logo(), "s3dal"),
      h("div", { class: "nav" },
        link("#/", "bucket", "Buckets", "buckets"),
        isAdmin() && link("#/dashboard", "chart", "Dashboard", "dashboard"),
        isAdmin() && link("#/server", "server", "Server", "server"),
        isAdmin() && link("#/settings", "settings", "Settings", "settings"),
      ),
      h("div", { class: "sidebar-foot" },
        h("div", { class: "whoami", title: `Signed in as ${signedInAs}` }, icon("key"), h("span", { class: "mono truncate" }, signedInAs)),
        h("button", { class: "nav-link", onclick: signOut }, icon("logout"), "Sign out"),
        h("div", { class: "version" }, `s3dal ${VERSION}`),
      ),
    ),
    h("main", { class: "content", id: "content" }, content),
  );
}

// ----- Buckets -----

function bucketsView() {
  const body = h("section", { class: "panel" }, loadingRows());
  const load = async () => {
    try {
      const buckets = (await s3.listBuckets()).sort((a, b) => a.name.localeCompare(b.name));
      if (!buckets.length) {
        body.replaceChildren(isAdmin()
          ? emptyState("bucket", "No buckets yet", "A bucket holds objects. Create one to start uploading.",
            h("button", { class: "button primary", onclick: createBucket }, icon("plus"), "Create bucket"))
          : emptyState("bucket", "No buckets", "This key pair reaches no existing bucket."));
        return;
      }
      // Requests per bucket, so the table shows first and each state fills in its own cell,
      // whose fixed width keeps the row from moving.
      const states = new Map(buckets.map((b) => [b.name, h("td", { class: "col-versioning" })]));
      const access = new Map(buckets.map((b) => [b.name, h("td", { class: "col-access" })]));
      const paintState = (name, status) => states.get(name).replaceChildren(versioningPill(name, status, load, true));
      const unknown = () => h("span", { class: "muted" }, "—");
      pool(buckets, 6, async (b) => {
        try {
          const status = await s3.versioning(b.name);
          versioningOf.set(b.name, status);
          paintState(b.name, status);
        } catch {
          states.get(b.name).replaceChildren(unknown());
        }
        try {
          const settings = await s3.bucketSettings(b.name);
          access.get(b.name).replaceChildren(accessCell(b.name, settings.public === true, load));
        } catch {
          access.get(b.name).replaceChildren(unknown());
        }
      });
      body.replaceChildren(h("table", { class: "table" },
        h("thead", {}, h("tr", {}, h("th", {}, "Name"), h("th", { class: "col-access" }, "Access"), h("th", { class: "col-versioning" }, "Versioning"), h("th", { class: "col-date" }, "Created"), h("th", { class: "col-actions" }))),
        h("tbody", {}, buckets.map((b) => h("tr", {},
          h("td", {}, h("a", { class: "name-cell", href: bucketHref(b.name, "") }, icon("bucket"), h("span", { class: "truncate" }, b.name))),
          access.get(b.name),
          states.get(b.name),
          h("td", { class: "col-date muted" }, formatDate(b.created)),
          h("td", { class: "col-actions" }, isAdmin() &&
            h("button", { class: "icon-button", title: `Delete ${b.name}`, "aria-label": `Delete ${b.name}`, onclick: () => deleteBucket(b.name, load) }, icon("trash"))),
        ))),
      ));
    } catch (err) {
      body.replaceChildren(errorState(err, load));
    }
  };
  load();
  // Managing buckets reaches past any one bucket, which a pair with limits never may.
  return shell("buckets",
    pageHeader("Buckets", [isAdmin() && h("button", { class: "button primary", onclick: createBucket }, icon("plus"), "Create bucket")]),
    body);
}

function bucketHref(bucket, prefix) {
  return `#/b/${encodeURIComponent(bucket)}/${encodeURIComponent(prefix)}`;
}

async function createBucket() {
  const input = h("input", { class: "input mono", name: "bucket", required: true, minLength: 3, maxLength: 63, pattern: "[a-z0-9][a-z0-9.\\-]*[a-z0-9]", spellcheck: "false", autocapitalize: "off", placeholder: "my-bucket" });
  const value = await dialog({
    title: "Create bucket",
    body: field("Name", input, "3–63 lowercase letters, digits, dots and hyphens."),
    actions: [{ label: "Cancel", value: "", kind: "ghost" }, { label: "Create", value: "ok", kind: "primary" }],
    onOpen: () => input.focus(),
  });
  if (value !== "ok") return;
  const name = input.value.trim();
  try {
    await s3.createBucket(name);
    toast(`Created ${name}`);
    location.hash = bucketHref(name, "");
  } catch (err) {
    fail(err);
  }
}

// A switch for a pair without limits, which may change it; the others see the state only.
function accessCell(name, on, reload) {
  if (!isAdmin()) return on ? h("span", { class: "pill accent" }, "Public") : h("span", { class: "muted" }, "Private");
  const input = h("input", {
    type: "checkbox", role: "switch", checked: on,
    "aria-label": `Public: ${name}`,
    onchange: () => setPublic(name, input.checked, input, reload),
  });
  return h("label", { class: "switch" }, input, on ? "Public" : "Private");
}

// Public means anyone may download the bucket's objects without a key pair; listing, uploading
// and deleting still take one. Made private without asking: that only closes.
async function setPublic(name, on, input, reload) {
  if (on && !(await confirm({
    title: `Make ${name} public?`,
    message: "Anyone who has the address of an object in it can download it, without signing in. Listing the bucket, uploading and deleting still need a key pair.",
    action: "Make public",
  }))) {
    input.checked = false;
    return;
  }
  try {
    await s3.saveBucketSettings(name, { public: on });
    toast(on ? `${name} is public` : `${name} is private`);
  } catch (err) {
    input.checked = !on;
    fail(err);
  }
  reload();
}

// S3 deletes only an empty bucket, and a versioned one is empty only once its older versions
// and delete markers are gone too, which the object browser does not reach one by one. So a
// bucket that holds anything is offered emptied first.
async function deleteBucket(name, reload) {
  let empty;
  try {
    empty = !(await s3.listVersions(name, "", { delimiter: "", maxKeys: 1 })).versions.length;
  } catch (err) {
    fail(err);
    return;
  }
  const ok = empty
    ? await confirm({ title: `Delete ${name}?`, message: "The bucket is empty. This cannot be undone.", action: "Delete bucket" })
    : await confirmEmpty(name);
  if (!ok) return;
  try {
    if (!empty) await emptyBucket(name);
    await s3.deleteBucket(name);
    toast(`Deleted ${name}`);
  } catch (err) {
    fail(err);
    reload();
    return;
  }
  versioningOf.delete(name);
  reload();
}

// Everything in it goes for good, every older version too: the name typed in is what keeps a
// slip of the pointer from doing that.
async function confirmEmpty(name) {
  const input = h("input", { class: "input mono", spellcheck: "false", autocomplete: "off", autocapitalize: "off" });
  const value = await dialog({
    title: `Empty and delete ${name}?`,
    body: [
      h("p", {}, versioningOf.get(name)
        ? "The bucket still holds objects, older versions or delete markers. Emptying it deletes every one of them for good, and then the bucket. This cannot be undone."
        : "The bucket still holds objects. Emptying it deletes every one of them for good, and then the bucket. This cannot be undone."),
      field(h("span", {}, "Type ", h("span", { class: "mono" }, name), " to confirm"), input),
    ],
    actions: [{ label: "Cancel", value: "", kind: "ghost" }, { label: "Empty and delete", value: "ok", kind: "danger" }],
    onOpen: (el) => {
      const go = el.querySelector(".button.danger");
      go.disabled = true;
      input.addEventListener("input", () => { go.disabled = input.value !== name; });
      input.focus();
    },
  });
  return value === "ok" && input.value === name;
}

// Removes every version and delete marker, a page at a time. Each page is read from the start:
// the entry the previous page ended on is gone, so there is nothing to continue after.
async function emptyBucket(name) {
  const waiting = busyDialog(`Emptying ${name}`);
  let removed = 0;
  try {
    for (;;) {
      waiting.say(removed ? `${removed.toLocaleString()} removed…` : "Listing what the bucket holds…");
      const page = await s3.listVersions(name, "", { delimiter: "" });
      if (!page.versions.length) return;
      const failed = await s3.deleteVersions(name, page.versions);
      // A page of which nothing could be removed would be read again forever.
      if (failed.length === page.versions.length) {
        const err = new Error(failed[0].message || `${failed.length} versions could not be deleted`);
        err.code = failed[0].code;
        throw err;
      }
      removed += page.versions.length - failed.length;
    }
  } finally {
    waiting.close();
  }
}

// ----- Versioning -----

// Each bucket's versioning state as last read: "Enabled", "Suspended", or "" for one that never
// had it. The upload question reads it, and uploads outlive the page they started on.
const versioningOf = new Map();

// Whether the object browser lists versions. Kept for the tab while the user moves between
// folders: it is how they are looking, not a property of one folder.
let showVersions = false;

const VERSIONING_SHORT = { "": "Off", Enabled: "Enabled", Suspended: "Suspended" };
const VERSIONING_LONG = { "": "Versioning off", Enabled: "Versioning enabled", Suspended: "Versioning suspended" };

// The state, as a button that changes it for an admin. `short` is the bucket list's wording.
function versioningPill(bucket, status, onChange, short) {
  const label = (short ? VERSIONING_SHORT : VERSIONING_LONG)[status];
  const cls = `pill ${{ Enabled: "accent", Suspended: "warn" }[status] ?? ""}`;
  if (!isAdmin()) return status ? h("span", { class: cls }, label) : short ? h("span", { class: "muted" }, label) : "";
  return h("button", {
    class: `${cls} pill-button`, type: "button", title: "Change versioning",
    "aria-label": `Versioning of ${bucket}: ${VERSIONING_SHORT[status]}`,
    onclick: () => changeVersioning(bucket, status, onChange),
  }, label, icon("chevron", "icon small"));
}

async function changeVersioning(bucket, status, onChange) {
  const enable = status !== "Enabled";
  const value = await dialog({
    title: enable ? `Enable versioning for ${bucket}?` : `Suspend versioning for ${bucket}?`,
    body: [
      h("p", {}, enable
        ? "Every upload, copy and delete then keeps the version it replaces. Older versions can be downloaded, restored, or deleted for good from an object's details, and take space until they are."
        : "Uploads and deletes then stop adding versions: each replaces the object's unversioned copy, as without versioning. The versions kept so far stay, and versioning can be enabled again."),
      // Said before the step that cannot be taken back, and when the user may expect "off".
      status !== "Suspended" && h("p", { class: "muted small" }, "A bucket that has had versioning cannot go back to unversioned: versioning can only be suspended."),
    ],
    actions: [
      { label: "Cancel", value: "", kind: "ghost" },
      { label: enable ? "Enable versioning" : "Suspend versioning", value: "ok", kind: "primary", autofocus: true },
    ],
  });
  if (value !== "ok") return;
  const next = enable ? "Enabled" : "Suspended";
  try {
    await s3.setVersioning(bucket, next);
    versioningOf.set(bucket, next);
    toast(enable ? `Versioning enabled for ${bucket}` : `Versioning suspended for ${bucket}`);
    onChange();
  } catch (err) {
    fail(err);
  }
}

// ----- Quota -----

// The bucket page's line about its quota: a bar of what the bucket holds against it, and for an
// admin the way to set it. Empty for a bucket without one, unless the reader may set one.
function quotaStrip(bucket, settings, onChange) {
  const { quota, usage } = settings;
  const edit = isAdmin() && h("button", {
    class: "button ghost small", type: "button",
    onclick: () => editQuota(bucket, quota, onChange),
  }, quota ? "Edit quota" : "Set quota");
  if (!quota) return edit ? [h("span", { class: "muted" }, "No quota"), edit] : [];
  const state = quotaState(usage, quota);
  const bytes = `${formatBytes(usage.bytes)} of ${formatBytes(quota.max_bytes)}`;
  const objects = quota.max_objects != null
    ? `${usage.objects.toLocaleString()} of ${quota.max_objects.toLocaleString()} objects`
    : `${usage.objects.toLocaleString()} object${usage.objects === 1 ? "" : "s"}`;
  const counted = usage.scanned ? `Counted at ${formatDate(new Date(usage.scanned * 1000))}, plus this server's writes since. ` : "";
  return [
    h("span", { class: "muted" }, "Quota"),
    meter(usage.bytes, quota.max_bytes, state.level, `Bytes used of the quota of ${bucket}`, usage.cold_bytes ?? 0),
    h("span", { class: "quota-figure", title: `${counted}Every version counts, cold ones too.` },
      `${bytes} (${percent(usage.bytes / Math.max(quota.max_bytes, 1))}) · ${objects}${usage.cold_bytes ? ` · ${formatBytes(usage.cold_bytes)} cold` : ""}`),
    state.word && h("span", { class: `pill ${state.level === "over" ? "danger" : "warn"}` }, icon("alert", "icon small"), state.word),
    edit,
  ];
}

const SIZE_UNITS = [["MB", 1024 ** 2], ["GB", 1024 ** 3], ["TB", 1024 ** 4]];

async function editQuota(bucket, quota, onChange) {
  // Shown in the largest unit that holds it whole, as the CLI prints it.
  const [unit, mult] = [...SIZE_UNITS].reverse().find(([, m]) => quota && quota.max_bytes % m === 0) ?? ["GB", 1024 ** 3];
  const size = h("input", {
    class: "input", type: "number", min: "0", step: "any", required: true, "aria-label": "Largest size",
    value: quota ? String(quota.max_bytes / mult) : "",
  });
  const units = h("select", { class: "input unit-select", "aria-label": "Unit" },
    SIZE_UNITS.map(([u]) => h("option", { value: u, selected: u === unit }, u)));
  const objects = h("input", {
    class: "input", type: "number", min: "0", step: "1", placeholder: "No limit", "aria-label": "Most objects",
    value: quota?.max_objects != null ? String(quota.max_objects) : "",
  });
  const value = await dialog({
    title: `Quota of ${bucket}`,
    body: [
      h("p", {}, "Writes that would take the bucket past its quota are refused with QuotaExceeded. Every version of every object counts."),
      field("Largest size", h("div", { class: "input-group" }, size, units)),
      field("Most objects", objects, "Optional; leave empty to limit the size only."),
      h("p", { class: "muted small" }, "With several servers on one backend, writes through the others count from the next usage scan."),
    ],
    actions: [
      { label: "Cancel", value: "", kind: "ghost" },
      quota && { label: "Remove quota", value: "remove", kind: "danger" },
      { label: "Save", value: "save", kind: "primary", autofocus: true },
    ].filter(Boolean),
  });
  if (!value) return;
  let edit;
  if (value === "remove") {
    edit = { quota: null };
  } else {
    const m = SIZE_UNITS.find(([u]) => u === units.value)[1];
    const maxBytes = Math.round(Number(size.value) * m);
    const maxObjects = objects.value === "" ? null : Number(objects.value);
    if (!Number.isSafeInteger(maxBytes) || maxBytes < 0 || (maxObjects !== null && !Number.isSafeInteger(maxObjects))) {
      fail(new Error("The quota needs a size, and whole numbers."));
      return;
    }
    edit = { quota: { max_bytes: maxBytes, max_objects: maxObjects } };
  }
  try {
    const settings = await s3.saveBucketSettings(bucket, edit);
    toast(settings.quota ? `Quota of ${bucket} set to ${formatBytes(settings.quota.max_bytes)}` : `Quota of ${bucket} removed`);
    onChange(settings);
  } catch (err) {
    fail(err);
  }
}

// ----- Lifecycle -----

const days = (n) => `${n} day${n === 1 ? "" : "s"}`;

// A rule in a line: what it covers, then what it does.
function describeRule(r) {
  const scope = [
    r.prefix || "whole bucket",
    r.larger != null && `over ${formatBytes(r.larger)}`,
    r.smaller != null && `under ${formatBytes(r.smaller)}`,
  ].filter(Boolean).join(", ");
  const does = [
    ...r.transitions.map((t) => `move to ${t.storageClass} ${t.date ? `on ${t.date}` : `after ${days(t.days)}`}`),
    ...r.noncurrentTransitions.map((t) => `old versions to ${t.storageClass} after ${days(t.days)}${t.keepNewer ? ` (newest ${t.keepNewer} kept)` : ""}`),
    r.expireDays != null && `expire after ${days(r.expireDays)}`,
    r.expireDate && `expire on ${r.expireDate}`,
    r.deleteMarkers && "remove lone delete markers",
    r.noncurrentDays != null && `old versions after ${days(r.noncurrentDays)}${r.keepNewer ? ` (newest ${r.keepNewer} kept)` : ""}`,
    r.abortDays != null && `abort uploads after ${days(r.abortDays)}`,
  ].filter(Boolean).join(" · ");
  return `${scope}: ${does}`;
}

// The bucket page's line of lifecycle rules, each a pill an admin opens to edit. Empty for a
// bucket without rules, unless the reader may add one. `tiers()` gives the server's cold tiers,
// as the bucket's settings last said, when a rule is opened.
function lifecycleStrip(bucket, rules, onChange, tiers) {
  const admin = isAdmin();
  const add = admin && h("button", {
    class: "button ghost small", type: "button", onclick: () => editRule(bucket, rules, null, onChange, tiers()),
  }, rules.length ? "Add rule" : "Add lifecycle rule");
  if (!rules.length) return add ? [h("span", { class: "muted" }, "No lifecycle rules"), add] : [];
  return [
    h("span", { class: "muted" }, "Lifecycle"),
    ...rules.map((r, i) => {
      const label = [h("strong", {}, r.id), " ", describeRule(r), !r.enabled && " (off)"];
      const cls = `pill rule-pill${r.enabled ? "" : " off"}`;
      return admin
        ? h("button", { class: cls, type: "button", title: "Edit rule", onclick: () => editRule(bucket, rules, i, onChange, tiers()) }, label)
        : h("span", { class: cls }, label);
    }),
    add,
  ];
}

// Adds a rule (`index` null) or edits or removes rules[index]; the whole configuration is put
// again, as S3 has no request for one rule. The first transition of each kind is edited here;
// further ones, which other tools can put, are kept as they are.
async function editRule(bucket, rules, index, onChange, tiers) {
  const r = index === null ? { enabled: true, transitions: [], noncurrentTransitions: [] } : rules[index];
  const count = (label, value, extra = {}) => h("input", {
    class: "input", type: "number", min: "1", step: "1", "aria-label": label, value: value ?? "", ...extra,
  });
  const id = h("input", { class: "input mono", maxlength: "255", value: r.id ?? "", placeholder: "Generated if empty", spellcheck: "false" });
  const enabled = h("input", { type: "checkbox", role: "switch", checked: r.enabled });
  const prefix = h("input", { class: "input mono", value: r.prefix ?? "", placeholder: "Whole bucket", spellcheck: "false" });
  const larger = count("Larger than", r.larger, { min: "0", placeholder: "Any" });
  const smaller = count("Smaller than", r.smaller, { placeholder: "Any" });
  const expireDays = count("Expire after days", r.expireDays);
  const expireDate = h("input", { class: "input", type: "date", "aria-label": "Expire on", value: r.expireDate ?? "" });
  const markers = h("input", { type: "checkbox", checked: r.deleteMarkers === true });
  const noncurrentDays = count("Noncurrent days", r.noncurrentDays, { placeholder: "Keep" });
  const keepNewer = count("Keep newest", r.keepNewer, { max: "100", placeholder: "None" });
  const abortDays = count("Abort after days", r.abortDays, { placeholder: "Server default" });
  const when = h("select", { class: "input", "aria-label": "Current versions" },
    [["keep", "Keep"], ["days", "Expire after days"], ["date", "Expire on a date"]].map(([value, name]) => h("option", { value }, name)));
  when.value = r.expireDays != null ? "days" : r.expireDate ? "date" : "keep";
  const sync = () => {
    expireDays.hidden = !(expireDays.required = when.value === "days");
    expireDate.hidden = !(expireDate.required = when.value === "date");
    // S3 takes one or the other; expiring current versions removes lone markers anyway.
    markers.disabled = when.value !== "keep";
    if (markers.disabled) markers.checked = false;
  };
  when.addEventListener("change", sync);
  sync();

  // Moves to a cold tier, offered where the server has one, or where the rule already names a
  // class (of a tier since removed), so it can be seen and taken out.
  const [move, oldMove] = [r.transitions[0], r.noncurrentTransitions[0]];
  const classes = [...new Set([...tiers.map((t) => t.storage_class), move?.storageClass, oldMove?.storageClass].filter(Boolean))];
  const classMenu = (label, value) => {
    const el = h("select", { class: "input", "aria-label": label }, classes.map((c) => h("option", { value: c }, classLabel(c, tiers))));
    el.value = value ?? classes[0];
    return el;
  };
  // Labels that the expiry fields' do not contain, so each is found by its own.
  const moveWhen = h("select", { class: "input", "aria-label": "Move objects" },
    [["keep", "Keep here"], ["days", "Move after days"], ["date", "Move on a date"]].map(([value, name]) => h("option", { value }, name)));
  moveWhen.value = move?.days != null ? "days" : move?.date ? "date" : "keep";
  const moveDays = count("Move after days", move?.days);
  const moveDate = h("input", { class: "input", type: "date", "aria-label": "Move on", value: move?.date ?? "" });
  const moveClass = classMenu("Move objects to", move?.storageClass);
  const oldMoveDays = count("Move old versions after days", oldMove?.days, { placeholder: "Keep here" });
  const oldMoveClass = classMenu("Move old versions to", oldMove?.storageClass);
  // The tier gets a row of its own: beside the day count, the dialog's width cut both menus short.
  const moveTo = field("To", moveClass, "A moved object reads as before, from its tier; its storage class says where it is.");
  const syncMove = () => {
    moveDays.hidden = !(moveDays.required = moveWhen.value === "days");
    moveDate.hidden = !(moveDate.required = moveWhen.value === "date");
    moveTo.hidden = moveWhen.value === "keep";
  };
  moveWhen.addEventListener("change", syncMove);
  syncMove();
  const moves = classes.length > 0 && [
    field("Move to a cold tier", h("div", { class: "input-group" }, moveWhen, moveDays, moveDate)),
    moveTo,
    h("div", { class: "field-pair" },
      field("Old versions: move after (days)", oldMoveDays),
      field("To", oldMoveClass)),
  ];

  const value = await dialog({
    title: index === null ? `New lifecycle rule for ${bucket}` : `Lifecycle rule ${r.id}`,
    body: [
      h("div", { class: "field-pair" }, field("Rule ID", id), h("label", { class: "switch rule-switch" }, enabled, "Enabled")),
      field("Prefix", prefix, "Only keys that start with it."),
      h("div", { class: "field-pair" }, field("Larger than (bytes)", larger), field("Smaller than (bytes)", smaller)),
      field("Current versions", h("div", { class: "input-group" }, when, expireDays, expireDate),
        "In a versioned bucket an expired object gets a delete marker, and its data stays as a noncurrent version."),
      h("label", { class: "check-field" }, markers, "Remove delete markers left with no version under them"),
      h("div", { class: "field-pair" },
        field("Noncurrent versions: remove after (days)", noncurrentDays),
        field("Keep the newest", keepNewer)),
      moves,
      field("Incomplete uploads: abort after (days)", abortDays, "Empty: after the server's stale-upload age."),
      h("p", { class: "muted small" }, "Rules run once a day: an object goes on the first midnight (UTC) after it is that many days old."),
    ],
    actions: [
      { label: "Cancel", value: "", kind: "ghost" },
      index !== null && { label: "Delete rule", value: "delete", kind: "danger" },
      { label: "Save", value: "save", kind: "primary", autofocus: true },
    ].filter(Boolean),
  });
  if (!value) return;
  let next;
  if (value === "delete") {
    next = rules.filter((_, i) => i !== index);
  } else {
    const n = (input) => (input.value === "" ? null : Number(input.value));
    const rule = {
      id: id.value.trim(),
      enabled: enabled.checked,
      prefix: prefix.value,
      larger: n(larger),
      smaller: n(smaller),
      expireDays: when.value === "days" ? n(expireDays) : null,
      expireDate: when.value === "date" ? expireDate.value || null : null,
      deleteMarkers: markers.checked,
      noncurrentDays: n(noncurrentDays),
      keepNewer: n(noncurrentDays) == null ? null : n(keepNewer),
      abortDays: n(abortDays),
      // Keep here drops the rule's moves of that kind; otherwise the first is the one edited.
      transitions: !classes.length ? r.transitions : moveWhen.value === "keep" ? [] : [{
        days: moveWhen.value === "days" ? n(moveDays) : null,
        date: moveWhen.value === "date" ? moveDate.value || null : null,
        storageClass: moveClass.value,
      }, ...r.transitions.slice(1)],
      noncurrentTransitions: !classes.length ? r.noncurrentTransitions : n(oldMoveDays) == null ? [] : [{
        days: n(oldMoveDays),
        keepNewer: oldMove?.keepNewer ?? null,
        storageClass: oldMoveClass.value,
      }, ...r.noncurrentTransitions.slice(1)],
    };
    if (rule.expireDays == null && !rule.expireDate && !rule.deleteMarkers && rule.noncurrentDays == null && rule.abortDays == null &&
        !rule.transitions.length && !rule.noncurrentTransitions.length) {
      fail(new Error("A rule needs something to do: expire or move objects, remove versions or markers, or abort uploads."));
      return;
    }
    next = index === null ? [...rules, rule] : rules.map((x, i) => (i === index ? rule : x));
  }
  try {
    await s3.setLifecycle(bucket, next);
    toast(value === "delete" ? `Lifecycle rule ${r.id} removed` : `Lifecycle rules of ${bucket} saved`);
    onChange();
  } catch (err) {
    fail(err);
  }
}

// ----- Event notifications -----

// The events a rule can name: each kind's wildcard, then its specific events.
const EVENT_KINDS = [
  ["Created", "s3:ObjectCreated:*", ["Put", "Post", "Copy", "CompleteMultipartUpload"]],
  ["Removed", "s3:ObjectRemoved:*", ["Delete", "DeleteMarkerCreated"]],
  ["Expired by lifecycle", "s3:LifecycleExpiration:*", ["Delete", "DeleteMarkerCreated"]],
  // One event, with no specific ones under it: an object or version moved to a cold tier.
  ["Moved by lifecycle", "s3:LifecycleTransition", []],
];

// A target's id from its ARN, arn:s3dal:sqs:<region>:<id>:webhook; the ARN itself otherwise.
const targetName = (arn) => /^arn:s3dal:sqs:[^:]*:([^:]+):webhook$/.exec(arn)?.[1] ?? arn;

function describeEventRule(r) {
  const events = r.events.map((e) => e.replace(/^s3:/, "")).join(", ");
  const keys = [r.prefix && `starting ${r.prefix}`, r.suffix && `ending ${r.suffix}`].filter(Boolean).join(", ");
  return `${events}${keys ? ` (${keys})` : ""} → ${targetName(r.arn)}`;
}

// A webhook's last delivery, as a pill: whether it answers, and when it last did or failed.
function targetPill(t) {
  const dropped = Object.values(t.dropped).reduce((a, b) => a + b, 0);
  const title = [
    t.url,
    `${t.delivered.toLocaleString()} delivered, ${t.failed.toLocaleString()} failed attempts, ${dropped.toLocaleString()} dropped, ${t.queued.toLocaleString()} queued`,
    t.last?.error && `Last error: ${t.last.error}`,
  ].filter(Boolean).join("\n");
  const when = t.last && formatDate(new Date(t.last.at * 1000));
  const [cls, status] = !t.last ? ["", "no delivery yet"]
    : t.last.error ? ["warn", `failing since ${when}`]
    : ["", `delivered ${when}`];
  return h("span", { class: `pill target-pill ${cls}`, title }, h("strong", {}, t.id), " ", status, dropped ? ` · ${dropped.toLocaleString()} dropped` : "");
}

// The bucket's line of notification rules, each a pill an admin opens to edit, then for an admin
// the server's webhooks with their last delivery. `targets` is null for a pair with limits,
// which sees the rules but not where the webhooks are. Empty for a bucket without rules, unless
// the reader may add one.
function eventsStrip(bucket, rules, targets, onChange) {
  const admin = isAdmin() && targets !== null;
  const add = admin && targets.length > 0 && h("button", {
    class: "button ghost small", type: "button", onclick: () => editEventRule(bucket, rules, targets, null, onChange),
  }, rules.length ? "Add rule" : "Add event rule");
  const hooks = admin && targets.length > 0 && [h("span", { class: "muted" }, "Webhooks"), ...targets.map(targetPill)];
  if (!rules.length) {
    if (!admin) return [];
    if (!targets.length) return [h("span", { class: "muted", title: "Add [[notify]] tables to the config file to send events to webhooks." }, "No event notifications: no webhooks configured")];
    return [h("span", { class: "muted" }, "No event notifications"), add, ...hooks];
  }
  return [
    h("span", { class: "muted" }, "Events"),
    ...rules.map((r, i) => {
      const label = [h("strong", {}, r.id), " ", describeEventRule(r)];
      return admin
        ? h("button", { class: "pill rule-pill", type: "button", title: "Edit rule", onclick: () => editEventRule(bucket, rules, targets, i, onChange) }, label)
        : h("span", { class: "pill rule-pill" }, label);
    }),
    add,
    ...(hooks || []),
  ];
}

// Adds a rule (`index` null) or edits or removes rules[index]; the whole configuration is put
// again, as S3 has no request for one rule.
async function editEventRule(bucket, rules, targets, index, onChange) {
  const r = index === null ? { events: ["s3:ObjectCreated:*"], arn: targets[0].arn, prefix: "", suffix: "" } : rules[index];
  const id = h("input", { class: "input mono", maxlength: "255", value: r.id ?? "", placeholder: "Generated if empty", spellcheck: "false" });
  const target = h("select", { class: "input", "aria-label": "Webhook" },
    targets.map((t) => h("option", { value: t.arn }, `${t.id} (${t.url})`)));
  // A rule naming a webhook since removed keeps it, so saving other rules does not fail on it.
  if (!targets.some((t) => t.arn === r.arn)) target.append(h("option", { value: r.arn }, `${targetName(r.arn)} (not configured)`));
  target.value = r.arn;
  const boxes = [];
  const kinds = EVENT_KINDS.map(([name, wildcard, specifics]) => {
    const all = h("input", { type: "checkbox", value: wildcard, checked: r.events.includes(wildcard) });
    const each = specifics.map((s) => {
      const event = wildcard.replace("*", s);
      return h("input", { type: "checkbox", value: event, checked: r.events.includes(event) });
    });
    // The wildcard takes in its specific events, which S3 would send twice for nothing.
    const sync = () => each.forEach((b) => { b.disabled = all.checked; if (all.checked) b.checked = false; });
    all.addEventListener("change", sync);
    sync();
    boxes.push(all, ...each);
    return h("fieldset", { class: "event-kind" },
      h("legend", {}, name),
      h("label", { class: "check-field" }, all, "All"),
      each.map((b, i) => h("label", { class: "check-field" }, b, specifics[i])));
  });
  const prefix = h("input", { class: "input mono", value: r.prefix, placeholder: "Any key", spellcheck: "false" });
  const suffix = h("input", { class: "input mono", value: r.suffix, placeholder: "Any key", spellcheck: "false" });

  const value = await dialog({
    title: index === null ? `New event rule for ${bucket}` : `Event rule ${r.id}`,
    body: [
      h("div", { class: "field-pair" }, field("Rule ID", id), field("Webhook", target)),
      h("div", { class: "event-kinds" }, kinds),
      h("div", { class: "field-pair" }, field("Key starts with", prefix), field("Key ends with", suffix)),
      h("p", { class: "muted small" }, "Events are sent after the change, at most once; a webhook down for longer than its retry time, or a full queue, loses them."),
    ],
    actions: [
      { label: "Cancel", value: "", kind: "ghost" },
      index !== null && { label: "Delete rule", value: "delete", kind: "danger" },
      { label: "Save", value: "save", kind: "primary", autofocus: true },
    ].filter(Boolean),
  });
  if (!value) return;
  let next;
  if (value === "delete") {
    next = rules.filter((_, i) => i !== index);
  } else {
    const events = boxes.filter((b) => b.checked).map((b) => b.value);
    if (!events.length) {
      fail(new Error("A rule needs at least one event."));
      return;
    }
    const rule = { id: id.value.trim(), list: r.list, arn: target.value, events, prefix: prefix.value, suffix: suffix.value };
    next = index === null ? [...rules, rule] : rules.map((x, i) => (i === index ? rule : x));
  }
  try {
    await s3.setNotification(bucket, next);
    toast(value === "delete" ? `Event rule ${r.id} removed` : `Event rules of ${bucket} saved`);
    onChange();
  } catch (err) {
    fail(err);
  }
}

// ----- Objects -----

function displayName(key, prefix) {
  return key.slice(prefix.length);
}

// A versions listing as the browser's rows: one per key, drawn as its latest version, with the
// key's versions and markers under it.
function byKey(versions) {
  const rows = [];
  for (const v of versions) {
    const last = rows[rows.length - 1];
    if (last?.key === v.key) last.versions.push(v);
    else rows.push({ key: v.key, folder: false, size: v.size, modified: v.modified, storageClass: v.storageClass, deleted: v.marker, versions: [v] });
  }
  return rows;
}

// "Standard", or a tier's class with the tier's name, as the menus offer them.
function classLabel(storageClass, tiers) {
  if (storageClass === "STANDARD") return "Standard";
  const tier = tiers.find((t) => t.storage_class === storageClass);
  return tier ? `${storageClass} (${tier.name})` : storageClass;
}

// A cold object's class beside its name; nothing for STANDARD, which most objects are.
function classPill(storageClass) {
  return storageClass && storageClass !== "STANDARD" && h("span", { class: "pill class-pill", title: "Storage class: kept in a cold tier, read through as usual" }, storageClass);
}

function isImage(key) {
  return /\.(png|jpe?g|gif|webp|avif|svg|bmp|ico)$/i.test(key);
}

// The images the server makes thumbnails of; the others are previewed whole, as before.
function hasThumbnail(key) {
  return /\.(png|jpe?g|gif|webp)$/i.test(key);
}

// The thumbnail sizes a screen of this density needs for a box `css` pixels wide.
const thumbnailSize = (css) => [128, 256, 512, 1024].find((s) => s >= css * (window.devicePixelRatio || 1)) ?? 1024;

// List or grid, per browser, for every bucket: a choice about how to look, not about the data.
const VIEW = "s3dal-view";

function isText(key, contentType) {
  return contentType.startsWith("text/") || /json|xml|yaml|javascript|csv/.test(contentType) ||
    /\.(txt|md|json|ya?ml|toml|csv|log|xml|html?|css|js|ts|rs|go|py|sh|ini|conf)$/i.test(key);
}

function browserView(bucket, prefix) {
  // `versions` holds the listing while versions are shown, `objects` otherwise. `versioning` is
  // the bucket's state, null until read.
  // `tiers` are the server's cold tiers, from the bucket's settings: [] until read, and on a
  // server without any.
  const state = { folders: [], objects: [], versions: [], next: null, selected: new Set(), filter: "", active: null, versioning: versioningOf.get(bucket) ?? null, tiers: [] };
  const listingVersions = () => showVersions && Boolean(state.versioning);
  // A pair limited to folders of this bucket cannot list the levels above them, which would
  // show the names of the others; there the page shows the way down to its folders instead.
  const scopes = scopesIn(bucket);
  const inScope = !scopes || scopes.some((p) => prefix.startsWith(p));
  const writable = canWrite() && inScope;
  const tbody = h("tbody");
  const table = h("table", { class: "table selectable" });
  const body = h("section", { class: "panel" }, loadingRows());
  const more = h("button", { class: "button ghost wide", onclick: () => load(false) }, "Load more");
  const footer = h("div", { class: "panel-footer" });
  const count = h("span", { class: "muted small" });
  const drawer = h("aside", { class: "drawer", "aria-label": "Object details", hidden: true });

  const deleteButton = h("button", { class: "button danger", hidden: true, onclick: () => deleteSelected() }, icon("trash"), "Delete");
  const filter = h("input", {
    class: "input filter", type: "search", placeholder: "Filter this folder", "aria-label": "Filter",
    oninput: () => { state.filter = filter.value.toLowerCase(); paint(); },
  });
  const fileInput = h("input", { type: "file", multiple: true, hidden: true, onchange: () => { enqueue([...fileInput.files].map((f) => [f.name, f])); fileInput.value = ""; } });
  const folderInput = h("input", { type: "file", hidden: true, webkitdirectory: true, onchange: () => { enqueue([...folderInput.files].map((f) => [f.webkitRelativePath || f.name, f])); folderInput.value = ""; } });
  const versionsInput = h("input", {
    type: "checkbox", role: "switch", checked: showVersions,
    onchange: () => { showVersions = versionsInput.checked; load(); },
  });
  // Hidden for a bucket that never had versioning, where every object has one version.
  const versionsSwitch = h("label", { class: "switch toolbar-switch", hidden: true }, versionsInput, "Show versions");
  let grid = localStorage.getItem(VIEW) === "grid";
  const tiles = h("ul", { class: "tiles", "aria-label": "Objects" });
  const viewButton = (name, label) => h("button", { class: "icon-button", type: "button", title: label, "aria-label": label, onclick: () => setView(name === "grid") }, icon(name));
  const listButton = viewButton("list", "List view");
  const gridButton = viewButton("grid", "Grid view");
  const viewToggle = h("div", { class: "view-toggle", role: "group", "aria-label": "View" }, listButton, gridButton);
  const paintToggle = () => {
    listButton.setAttribute("aria-pressed", String(!grid));
    gridButton.setAttribute("aria-pressed", String(grid));
  };
  paintToggle();
  function setView(on) {
    grid = on;
    localStorage.setItem(VIEW, on ? "grid" : "list");
    paintToggle();
    paint();
  }
  // A presigned thumbnail URL per object and ETag, kept while the page is open: a paint, which
  // every click on a checkbox makes, then asks for the same URL, which the browser has cached.
  // Signed for an hour and taken again after 50 minutes, before a tile could get one expired.
  const thumbUrls = new Map();
  const noThumbnail = new Set();
  function thumbnailUrl(key, etag, size) {
    const id = `${size}\n${key}\n${etag}`;
    const kept = thumbUrls.get(id);
    if (kept && Date.now() - kept.at < 50 * 60_000) return kept.url;
    const url = s3.presign(bucket, key, { thumbnail: size, expires: 3600 });
    thumbUrls.set(id, { url, at: Date.now() });
    return url;
  }
  const versioningSlot = h("span", { class: "crumb-state" });
  const quotaSlot = h("div", { class: "quota-strip", hidden: true });
  const paintQuota = (settings) => {
    const parts = quotaStrip(bucket, settings, paintQuota);
    quotaSlot.replaceChildren(...parts.filter(Boolean));
    quotaSlot.hidden = parts.length === 0;
  };
  // Where the tiers' classes are offered: the uploads' class, for a pair that may write here.
  // Hidden on a server without tiers, whose uploads all go to STANDARD.
  const uploadClass = h("select", { class: "input class-select", "aria-label": "Storage class of uploads", hidden: true });
  const setTiers = (tiers) => {
    const kept = uploadClass.value;
    state.tiers = tiers;
    uploadClass.replaceChildren(...["STANDARD", ...tiers.map((t) => t.storage_class)]
      .map((c) => h("option", { value: c }, `Upload as ${classLabel(c, tiers)}`)));
    uploadClass.value = tiers.some((t) => t.storage_class === kept) ? kept : "STANDARD";
    uploadClass.hidden = !writable || !tiers.length;
  };
  // Read with each listing, as what the bucket holds changes with every write. A server too old
  // to answer, or a pair that may not ask, shows no quota line.
  const loadQuota = () => s3.bucketSettings(bucket).then((s) => {
    setTiers(s.tiers ?? []);
    paintQuota(s);
  }, () => { quotaSlot.hidden = true; });
  const lifecycleSlot = h("div", { class: "lifecycle-strip", hidden: true });
  // Read with each listing too: another client may have changed the rules. A pair that may not
  // read them sees no line.
  const loadLifecycle = () => s3.lifecycle(bucket).then((rules) => {
    const parts = lifecycleStrip(bucket, rules, loadLifecycle, () => state.tiers);
    lifecycleSlot.replaceChildren(...parts.filter(Boolean));
    lifecycleSlot.hidden = parts.length === 0;
  }, () => { lifecycleSlot.hidden = true; });
  const eventsSlot = h("div", { class: "events-strip", hidden: true });
  // Read with each listing, as the lifecycle line is, and so is each webhook's last delivery.
  const loadEvents = () => Promise.all([
    s3.notification(bucket),
    isAdmin() ? s3.bucketSettings(bucket).then((s) => s.notify ?? null) : null,
  ]).then(([rules, targets]) => {
    const parts = eventsStrip(bucket, rules, targets, loadEvents);
    eventsSlot.replaceChildren(...parts.filter(Boolean));
    eventsSlot.hidden = parts.length === 0;
  }, () => { eventsSlot.hidden = true; });

  const crumbs = h("nav", { class: "crumbs", "aria-label": "Path" },
    h("a", { href: "#/" }, "Buckets"),
    icon("chevron", "icon crumb-sep"),
    prefix ? h("a", { href: bucketHref(bucket, "") }, bucket) : h("span", { "aria-current": "page" }, bucket),
    prefix.split("/").filter(Boolean).map((part, i, parts) => {
      const to = parts.slice(0, i + 1).join("/") + "/";
      const last = i === parts.length - 1;
      return [icon("chevron", "icon crumb-sep"), last ? h("span", { "aria-current": "page" }, part) : h("a", { href: bucketHref(bucket, to) }, part)];
    }),
    versioningSlot,
  );

  const selectAll = h("input", {
    type: "checkbox", "aria-label": "Select all",
    onchange: () => {
      const keys = visible().filter((e) => !e.deleted).map((e) => e.key);
      if (selectAll.checked) keys.forEach((k) => state.selected.add(k));
      else state.selected.clear();
      paint();
    },
  });
  table.append(h("thead", {}, h("tr", {},
    h("th", { class: "col-check" }, selectAll),
    h("th", {}, "Name"),
    h("th", { class: "col-size" }, "Size"),
    h("th", { class: "col-date" }, "Last modified"),
    h("th", { class: "col-actions" }),
  )), tbody);

  function visible() {
    const listed = new Set(state.folders);
    const made = [...madeFolders]
      .filter((p) => p.startsWith(`${bucket}/${prefix}`))
      .map((p) => p.slice(bucket.length + 1))
      .filter((key) => key !== prefix && !listed.has(key) && !key.slice(prefix.length, -1).includes("/"))
      .map((key) => ({ key, folder: true, empty: true }));
    const folders = [...state.folders.map((key) => ({ key, folder: true })), ...made].sort((a, b) => a.key.localeCompare(b.key));
    const objects = listingVersions()
      ? byKey(state.versions).filter((o) => o.key !== prefix)
      : state.objects.filter((o) => o.key !== prefix).map((o) => ({ ...o, folder: false }));
    return [...folders, ...objects].filter((e) => !state.filter || displayName(e.key, prefix).toLowerCase().includes(state.filter));
  }

  function paint() {
    const rows = visible();
    // A key whose latest version is a delete marker is not selectable: deleting it again would
    // only stack another marker.
    const selectable = rows.filter((r) => !r.deleted);
    // The grid shows each key once, as the latest version; its versions are in its details.
    if (grid) tiles.replaceChildren(...rows.map(tile));
    else tbody.replaceChildren(...rows.flatMap((e) => [row(e), ...(e.versions ?? []).map((v) => versionRow(e, v))]));
    const listing = grid ? tiles : table;
    const n = state.selected.size;
    deleteButton.hidden = n === 0 || !writable;
    deleteButton.lastChild.textContent = n ? `Delete ${n}` : "Delete";
    selectAll.checked = selectable.length > 0 && selectable.every((r) => state.selected.has(r.key));
    selectAll.indeterminate = n > 0 && !selectAll.checked;
    versionsSwitch.hidden = !state.versioning;
    const keys = listingVersions() ? new Set(state.versions.map((v) => v.key)).size : state.objects.length;
    const total = state.folders.length + keys;
    const versions = listingVersions() ? ` · ${state.versions.length.toLocaleString()} version${state.versions.length === 1 ? "" : "s"}` : "";
    count.textContent = `${total.toLocaleString()} item${total === 1 ? "" : "s"}${versions}${state.next ? " loaded" : ""}`;
    footer.replaceChildren(count, state.next ? more : "");
    if (!rows.length && !state.filter) {
      body.replaceChildren(writable
        ? emptyState("folder", prefix ? "This folder is empty" : "This bucket is empty",
          "Drop files anywhere on this page, or use Upload.",
          h("button", { class: "button primary", onclick: () => fileInput.click() }, icon("upload"), "Upload files"))
        : emptyState("folder", prefix ? "This folder is empty" : "This bucket is empty", "This key pair can read here, not write."));
    } else if (!body.contains(listing)) {
      body.replaceChildren(listing, footer);
    }
  }

  // A tile of the grid: the thumbnail of an image, or an icon, over the name and size.
  function tile(e) {
    const name = displayName(e.key, prefix).replace(/\/$/, "");
    const checked = state.selected.has(e.key);
    const check = !e.deleted && h("input", {
      type: "checkbox", class: "tile-check", checked, "aria-label": `Select ${name}`,
      onchange: () => { check.checked ? state.selected.add(e.key) : state.selected.delete(e.key); paint(); },
    });
    const etag = e.etag ?? e.versions?.[0]?.etag ?? "";
    const art = h("div", { class: "tile-art" },
      e.folder ? icon("folder", "icon tile-icon folder-icon")
        : !e.deleted && hasThumbnail(e.key) && !noThumbnail.has(e.key) ? tileImage(e.key, etag)
        : icon(isImage(e.key) ? "image" : "file", "icon tile-icon"));
    const label = [art,
      h("span", { class: "tile-name truncate", title: name }, name),
      h("span", { class: "tile-meta muted small" }, e.empty ? "Empty" : e.folder ? "Folder" : e.deleted ? "Deleted" : formatBytes(e.size), !e.folder && !e.deleted && classPill(e.storageClass))];
    const main = e.folder
      ? h("a", { class: "tile-main", href: bucketHref(bucket, e.key) }, label)
      : h("button", { class: "tile-main link-button", type: "button", onclick: () => openDetails(e) }, label);
    return h("li", { class: ["tile", checked && "selected", state.active === e.key && "active", e.deleted && "deleted"].filter(Boolean).join(" ") }, main, check);
  }

  // The name is the tile's text right under the picture, so the picture's alt is empty rather
  // than the name again: a screen reader would read every name twice.
  function tileImage(key, etag) {
    const img = h("img", {
      alt: "", loading: "lazy", decoding: "async",
      // Not an image after all, too large, or an older server: the tile shows the icon, and
      // later paints do not ask again.
      onerror: () => {
        noThumbnail.add(key);
        img.replaceWith(icon("image", "icon tile-icon"));
      },
    });
    thumbnailUrl(key, etag, thumbnailSize(160)).then((url) => { img.src = url; }, () => img.dispatchEvent(new Event("error")));
    return img;
  }

  function row(e) {
    const name = displayName(e.key, prefix);
    const checked = state.selected.has(e.key);
    const check = !e.deleted && h("input", {
      type: "checkbox", checked, "aria-label": `Select ${name}`,
      onchange: () => { check.checked ? state.selected.add(e.key) : state.selected.delete(e.key); paint(); },
    });
    const nameCell = e.folder
      ? h("a", { class: "name-cell", href: bucketHref(bucket, e.key) }, icon("folder", "icon folder-icon"), h("span", { class: "truncate" }, name.replace(/\/$/, "")))
      : h("button", { class: `name-cell link-button${e.deleted ? " deleted" : ""}`, onclick: () => openDetails(e) },
        icon(isImage(e.key) ? "image" : "file"), h("span", { class: "truncate", title: name }, name),
        e.deleted ? h("span", { class: "pill warn" }, "Deleted") : classPill(e.storageClass));
    return h("tr", { class: [checked && "selected", state.active === e.key && "active", e.versions && "has-versions"].filter(Boolean).join(" ") },
      h("td", { class: "col-check" }, check),
      h("td", {}, nameCell),
      h("td", { class: "col-size num muted" }, e.empty ? "Empty" : e.folder || e.deleted ? "—" : formatBytes(e.size)),
      h("td", { class: "col-date muted" }, e.folder ? "" : formatDate(e.modified)),
      h("td", { class: "col-actions" }, e.folder || e.deleted ? "" :
        h("button", { class: "icon-button", title: "Download", "aria-label": `Download ${name}`, onclick: () => download(e.key) }, icon("download"))),
    );
  }

  // A version or delete marker of the key above it, while versions are shown.
  function versionRow(e, v) {
    const name = displayName(e.key, prefix);
    const what = v.marker ? "Delete marker" : "Version";
    return h("tr", { class: `version-row${v.marker ? " marker" : ""}` },
      h("td", { class: "col-check" }),
      h("td", {}, h("button", { class: "version-cell link-button", title: `${what} ${v.versionId}`, "aria-label": `${what} ${v.versionId} of ${name}`, onclick: () => openDetails(e) },
        icon(v.marker ? "trash" : "history", "icon small"),
        h("span", { class: "mono truncate" }, v.versionId),
        v.latest && h("span", { class: "pill accent" }, "Latest"),
        v.marker ? h("span", { class: "pill" }, "Delete marker") : classPill(v.storageClass))),
      h("td", { class: "col-size num muted" }, v.marker ? "—" : formatBytes(v.size)),
      h("td", { class: "col-date muted" }, formatDate(v.modified)),
      h("td", { class: "col-actions" }, !v.marker &&
        h("button", { class: "icon-button small", title: "Download this version", "aria-label": `Download version ${v.versionId} of ${name}`, onclick: () => download(e.key, v.versionId) }, icon("download"))),
    );
  }

  async function load(reset = true) {
    if (!inScope) {
      const next = new Set(scopes.filter((p) => p.startsWith(prefix)).map((p) => prefix + p.slice(prefix.length).split("/")[0] + "/"));
      Object.assign(state, { folders: [...next].sort(), objects: [], next: null });
      paint();
      return;
    }
    const listing = (next) => listingVersions() ? s3.listVersions(bucket, prefix, { next }) : s3.list(bucket, prefix, next);
    try {
      if (reset) {
        state.next = null;
        loadQuota();
        loadLifecycle();
        loadEvents();
        // Read on every load, as the listing is: another client may have changed it. Only the
        // first load with versions shown waits for it, to know which listing to ask for.
        const status = s3.versioning(bucket).catch(() => state.versioning ?? "");
        if (showVersions && state.versioning === null) state.versioning = await status;
        const [page, versioning] = await Promise.all([listing(null), status]);
        state.versioning = versioning;
        versioningOf.set(bucket, versioning);
        paintVersioning();
        Object.assign(state, { folders: page.folders, objects: page.objects ?? [], versions: page.versions ?? [], next: page.next });
        const live = new Set([...state.objects, ...state.versions.filter((v) => v.latest && !v.marker)].map((o) => o.key));
        for (const k of [...state.selected]) {
          if (!state.folders.includes(k) && !live.has(k)) state.selected.delete(k);
        }
      } else {
        more.disabled = true;
        const page = await listing(state.next);
        state.folders.push(...page.folders);
        state.objects.push(...(page.objects ?? []));
        // Pages come in listing order and each is sorted, so appending keeps a key's versions
        // newest first across the page boundary.
        state.versions.push(...(page.versions ?? []));
        state.next = page.next;
        more.disabled = false;
      }
      paint();
    } catch (err) {
      if (reset) body.replaceChildren(errorState(err, () => load()));
      else fail(err);
    }
  }

  function paintVersioning() {
    versioningSlot.replaceChildren(versioningPill(bucket, state.versioning, () => {
      state.versioning = versioningOf.get(bucket);
      load();
    }));
  }

  async function download(key, versionId) {
    try {
      const a = h("a", { href: await s3.presign(bucket, key, { download: true, expires: 300, versionId }), download: "" });
      document.body.append(a);
      a.click();
      a.remove();
    } catch (err) {
      fail(err);
    }
  }

  function closeDetails() {
    drawer.hidden = true;
    state.active = null;
    paint();
  }

  // `refresh` paints the drawer again after its key's versions changed, keeping what it shows
  // until the new state is read rather than flashing the skeleton.
  async function openDetails(entry, { refresh = false } = {}) {
    state.active = entry.key;
    paint();
    const name = displayName(entry.key, prefix);
    const props = h("dl", { class: "props" }, loadingRows(4));
    const preview = h("div", { class: "preview" });
    const header = h("header", { class: "drawer-header" },
      h("h2", { class: "truncate", title: name }, name),
      h("button", { class: "icon-button", "aria-label": "Close details", onclick: closeDetails }, icon("close")));
    const actions = h("div", { class: "drawer-actions" },
      h("button", { class: "button primary", onclick: () => download(entry.key) }, icon("download"), "Download"),
      h("button", { class: "button", onclick: () => shareLink(entry.key) }, icon("link"), "Copy link"),
      writable && h("button", { class: "icon-button danger-text", title: "Delete", "aria-label": `Delete ${name}`, onclick: () => deleteKeys([entry.key]) }, icon("trash")));
    if (!refresh) {
      // A key the listing shows as deleted has nothing to download: no buttons to take back.
      drawer.replaceChildren(header, ...(entry.deleted ? [] : [preview, actions]), props);
      drawer.hidden = false;
    }
    try {
      // In a versioned bucket a key may have versions and no current object, when its latest
      // is a delete marker; HEAD answers that with 404.
      const [meta, versions] = await Promise.all([
        s3.head(bucket, entry.key).catch((err) => {
          if (state.versioning && err.status === 404) return null;
          throw err;
        }),
        state.versioning ? s3.keyVersions(bucket, entry.key) : null,
      ]);
      if (state.active !== entry.key) return;
      if (!meta && !versions.length) {
        closeDetails();
        return;
      }
      const history = versions ? versionList(entry.key, name, versions) : "";
      if (!meta) {
        drawer.replaceChildren(header, deletedBanner(entry.key, name, versions), history);
        drawer.hidden = false;
        return;
      }
      drawer.replaceChildren(header, preview, actions, props, history);
      drawer.hidden = false;
      const prop = (k, v, mono) => v ? [h("dt", {}, k), h("dd", { class: mono ? "mono" : "" }, v)] : [];
      // Shown where it can be other than STANDARD. An empty object is never cold, so it has no
      // class to change.
      const classProp = state.tiers.length || meta.storageClass !== "STANDARD"
        ? [h("dt", {}, "Storage class"), h("dd", { class: "class-prop" }, h("span", {}, classLabel(meta.storageClass, state.tiers)),
          writable && state.tiers.length > 0 && meta.size > 0 &&
            h("button", { class: "button small", type: "button", onclick: () => changeClass(entry.key, name, meta.storageClass) }, "Change storage class"))]
        : [];
      props.replaceChildren(
        ...prop("Key", entry.key, true),
        ...(state.versioning ? prop("Version ID", meta.versionId, true) : []),
        ...prop("Size", `${formatBytes(meta.size)} (${meta.size.toLocaleString()} bytes)`),
        ...classProp,
        ...prop("Last modified", formatDate(meta.modified)),
        ...prop("Content type", meta.contentType, true),
        ...prop("ETag", meta.etag, true),
        ...meta.checksums.flatMap((c) => prop(`Checksum (${CHECKSUM_NAMES[c.algorithm] ?? c.algorithm})`, checksumText(c), true)),
        ...prop("Cache control", meta.cacheControl, true),
        ...prop("Content encoding", meta.contentEncoding, true),
        ...meta.metadata.flatMap(([k, v]) => prop(`x-amz-meta-${k}`, v, true)),
      );
      if (hasThumbnail(entry.key)) {
        // The server's thumbnail rather than the original: a 20 MB photo would take seconds to
        // show at the drawer's width.
        const img = h("img", {
          alt: name,
          onerror: () => preview.replaceChildren(h("p", { class: "preview-note muted small" }, "No preview: the server makes none of this object.")),
        });
        img.src = await thumbnailUrl(entry.key, meta.etag, thumbnailSize(360));
        preview.replaceChildren(img);
      } else if (isImage(entry.key)) {
        const img = h("img", { alt: name, src: await s3.presign(bucket, entry.key, { expires: 300 }) });
        preview.replaceChildren(img);
      } else if (isText(entry.key, meta.contentType) && meta.size > 0) {
        const bytes = await s3.readStart(bucket, entry.key, 64 * 1024);
        const text = new TextDecoder("utf-8", { fatal: false }).decode(bytes);
        preview.replaceChildren(h("pre", { class: "text-preview" }, text, meta.size > bytes.byteLength ? "\n…" : ""));
      }
    } catch (err) {
      fail(err);
    }
  }

  // After a change to a key's versions, both the drawer and the listing say something else.
  function refreshKey(key) {
    return Promise.all([openDetails({ key }, { refresh: true }), load()]);
  }

  function versionList(key, name, versions) {
    return h("section", { class: "versions", "aria-label": "Versions" },
      h("h3", { class: "versions-title" }, "Versions", h("span", { class: "muted" }, versions.length.toLocaleString())),
      h("ol", { class: "version-list" }, versions.map((v, i) => h("li", { class: `version-item${v.latest ? " latest" : ""}${v.marker ? " marker" : ""}` },
        h("div", { class: "version-head" },
          h("span", { class: "version-when" }, formatDate(v.modified)),
          v.latest && h("span", { class: "pill accent" }, "Latest"),
          v.marker && h("span", { class: "pill" }, "Delete marker")),
        h("div", { class: "mono small muted truncate", title: v.versionId }, v.versionId),
        !v.marker && h("div", { class: "version-meta small muted" }, formatBytes(v.size), " · ETag ", h("span", { class: "mono" }, v.etag),
          v.storageClass !== "STANDARD" && ` · ${v.storageClass}`),
        h("div", { class: "version-actions" },
          !v.marker && h("button", { class: "button small", "aria-label": `Download version ${v.versionId}`, onclick: () => download(key, v.versionId) }, icon("download", "icon small"), "Download"),
          writable && !v.marker && !v.latest && h("button", { class: "button small", onclick: () => restoreVersion(key, v) }, icon("restore", "icon small"), "Restore"),
          writable && h("button", {
            class: "icon-button small danger-text",
            title: v.marker ? "Remove delete marker" : "Delete version permanently",
            "aria-label": v.marker ? "Remove delete marker" : "Delete version permanently",
            onclick: () => deleteVersion(key, name, v, versions[i + 1]),
          }, icon("trash"))),
      ))));
  }

  function deletedBanner(key, name, versions) {
    // Every marker above the newest version: with markers stacked, removing only the top one
    // would leave the object deleted, which is not what Undelete promises.
    const markers = [];
    for (const v of versions) {
      if (!v.marker) break;
      markers.push(v);
    }
    const kept = versions.length - markers.length;
    const text = !markers.length ? "There is no current version. Restore one below to make it the latest again."
      : kept ? `The latest version is a delete marker. ${kept === 1 ? "The version under it is" : `The ${kept} versions under it are`} kept: Undelete makes the newest the latest again.`
      : "Only delete markers are left: there is no version to bring back.";
    // The button goes under the text: beside it, the drawer's width leaves the text a sliver.
    return h("div", { class: "banner warn drawer-banner" }, icon("trash"),
      h("div", { class: "banner-text" }, h("strong", {}, "Deleted"), h("div", { class: "muted small" }, text),
        writable && markers.length > 0 && kept > 0 && h("button", { class: "button small", onclick: () => undelete(key, name, markers) }, icon("restore", "icon small"), "Undelete")));
  }

  // Not asked first: removing markers loses nothing, and deleting again puts one back.
  async function undelete(key, name, markers) {
    try {
      for (const m of markers) await s3.deleteObject(bucket, key, m.versionId);
      toast(`Undeleted ${name}`);
    } catch (err) {
      fail(err);
    }
    await refreshKey(key);
  }

  // A copy onto itself, as S3 changes a class (`aws s3 cp s3://b/k s3://b/k --storage-class`):
  // the bytes go through the server into the tier, or back to the primary.
  async function changeClass(key, name, current) {
    const select = h("select", { class: "input" },
      ["STANDARD", ...state.tiers.map((t) => t.storage_class)].map((c) => h("option", { value: c }, classLabel(c, state.tiers))));
    select.value = current;
    const note = {
      Enabled: "Versioning is on: this makes a new version, and the current one stays as it is, in its class.",
      Suspended: "Versioning is suspended: this replaces the current version, unless it was written while versioning was on.",
    }[state.versioning] ?? "The object is copied onto itself through the server; its Last-Modified becomes now.";
    const value = await dialog({
      title: `Storage class of ${name}`,
      body: [field("Storage class", select, "Standard keeps it with the other objects; a tier's class moves it there. It reads the same either way."),
        h("p", { class: "muted small" }, note)],
      actions: [{ label: "Cancel", value: "", kind: "ghost" }, { label: "Change", value: "ok", kind: "primary", autofocus: true }],
    });
    if (value !== "ok" || select.value === current) return;
    try {
      await s3.changeStorageClass(bucket, key, select.value);
      toast(`${name} is now ${classLabel(select.value, state.tiers)}`);
    } catch (err) {
      fail(err);
    }
    await refreshKey(key);
  }

  // A copy, as S3 restores: the restored content becomes a new latest version and the ones in
  // between stay, so a restore can itself be undone.
  async function restoreVersion(key, v) {
    try {
      await s3.restoreVersion(bucket, key, v.versionId);
      toast(`Restored the version of ${formatDate(v.modified)}`);
    } catch (err) {
      fail(err);
    }
    await refreshKey(key);
  }

  async function deleteVersion(key, name, v, next) {
    const id = h("span", { class: "mono" }, v.versionId);
    const ok = v.marker
      ? await confirm({
        title: "Remove this delete marker?",
        message: !v.latest ? "It is not the latest, so the object does not change."
          : !next ? "Nothing is under it: the key leaves the listing."
          : next.marker ? "Another delete marker is under it, so the object stays deleted."
          : "The version under it becomes the latest again: the object is back.",
        action: "Remove marker",
        danger: false,
      })
      : await confirm({
        title: "Delete this version permanently?",
        message: ["Version ", id, ` of ${name} is removed for good. `,
          !v.latest ? "The other versions are kept."
          : !next ? "It is the only one, so the object is gone."
          : next.marker ? "The delete marker under it becomes the latest, so the object reads as deleted."
          : "The version before it becomes the latest."],
        action: "Delete permanently",
      });
    if (!ok) return;
    try {
      await s3.deleteObject(bucket, key, v.versionId);
      toast(v.marker ? "Delete marker removed" : "Version deleted for good");
    } catch (err) {
      fail(err);
    }
    await refreshKey(key);
  }

  async function shareLink(key) {
    // Asked now rather than kept: the bucket may have been switched since, here or elsewhere.
    // Unknown, only signed links are offered, which work either way.
    const open = await s3.bucketSettings(bucket).then((s) => s.public === true, () => false);
    // A public bucket's objects need no signature, so their plain address never expires; it
    // stops working when the bucket is made private, which a signed link outlives.
    const expiries = [...(open ? [["Never, while the bucket is public", 0]] : []), ["1 hour", 3600], ["1 day", 86400], ["7 days", 604800]];
    const select = h("select", { class: "input" }, expiries.map(([label, secs]) => h("option", { value: String(secs) }, label)));
    const value = await dialog({
      title: "Copy a link",
      body: [h("p", { class: "muted" }, open
        ? "This bucket is public: anyone with the link can download this object without signing in."
        : "Anyone with the link can download this object until it expires, without signing in."),
      field("Expires after", select)],
      actions: [{ label: "Cancel", value: "", kind: "ghost" }, { label: "Copy link", value: "ok", kind: "primary", autofocus: true }],
    });
    if (value !== "ok") return;
    try {
      const secs = Number(select.value);
      const link = secs ? await s3.presign(bucket, key, { expires: secs }) : `${location.origin}/s3${objectPath(bucket, key)}`;
      await copyText(link, "Link copied");
    } catch (err) {
      fail(err);
    }
  }

  async function newFolder() {
    const input = h("input", { class: "input", required: true, placeholder: "photos", spellcheck: "false" });
    const value = await dialog({
      title: "New folder",
      body: field("Name", input, `In ${bucket}/${prefix}`),
      actions: [{ label: "Cancel", value: "", kind: "ghost" }, { label: "Create", value: "ok", kind: "primary" }],
      onOpen: () => input.focus(),
    });
    const name = input.value.trim().replace(/^\/+|\/+$/g, "");
    if (value !== "ok" || !name) return;
    try {
      await s3.createFolder(bucket, `${prefix}${name}/`);
      madeFolders.add(`${bucket}/${prefix}${name}/`);
      location.hash = bucketHref(bucket, `${prefix}${name}/`);
    } catch (err) {
      fail(err);
    }
  }

  async function deleteSelected() {
    await deleteKeys([...state.selected]);
  }

  async function deleteKeys(keys) {
    const folders = keys.filter((k) => k.endsWith("/"));
    const files = keys.filter((k) => !k.endsWith("/"));
    const what = [files.length && `${files.length} object${files.length === 1 ? "" : "s"}`,
      folders.length && `${folders.length} folder${folders.length === 1 ? "" : "s"} and everything in ${folders.length === 1 ? "it" : "them"}`]
      .filter(Boolean).join(" and ");
    // In a versioned bucket a delete takes nothing away, which is worth saying: the question
    // otherwise reads as final.
    const consequence = {
      Enabled: "Versioning is on: each object gets a delete marker and keeps its versions, so Show versions can undelete it.",
      Suspended: "Versioning is suspended: each object gets a delete marker, and a version written while suspended is removed for good. Older versions are kept.",
    }[state.versioning] ?? "It cannot be undone.";
    const ok = await confirm({ title: "Delete?", message: `This deletes ${what}. ${consequence}`, action: "Delete" });
    if (!ok) return;
    try {
      const all = [...files];
      for (const folder of folders) {
        for await (const key of s3.allKeys(bucket, folder)) all.push(key);
      }
      let failed = 0;
      await pool(all, 8, async (key) => {
        try {
          await s3.deleteObject(bucket, key);
        } catch {
          failed++;
        }
      });
      // The folder's own marker, if one was created; a folder that only held objects is gone.
      await pool(folders, 4, (f) => s3.deleteObject(bucket, f).catch(() => {}));
      folders.forEach((f) => madeFolders.delete(`${bucket}/${f}`));
      if (failed) toast(`${failed} of ${all.length} could not be deleted`, { kind: "error" });
      else toast(`Deleted ${all.length + (folders.length && !all.length ? folders.length : 0)} item${all.length === 1 ? "" : "s"}`);
      state.selected.clear();
      // A versioned object's drawer stays, now offering Undelete right where it was deleted.
      if (keys.includes(state.active)) {
        if (state.versioning && !state.active.endsWith("/")) openDetails({ key: state.active }, { refresh: true });
        else closeDetails();
      }
      load();
    } catch (err) {
      fail(err);
    }
  }

  function enqueue(files) {
    uploads.add(bucket, prefix, files, () => {
      if (currentRoute === routeKey) load();
    }, uploadClass.hidden ? "STANDARD" : uploadClass.value);
  }

  // Folders dropped from the desktop arrive as entries to walk, not as files.
  async function filesFromDrop(items) {
    const out = [];
    const walk = async (entry, path) => {
      if (entry.isFile) {
        const file = await new Promise((res, rej) => entry.file(res, rej));
        out.push([path + file.name, file]);
      } else if (entry.isDirectory) {
        const reader = entry.createReader();
        let batch;
        do {
          batch = await new Promise((res, rej) => reader.readEntries(res, rej));
          for (const child of batch) await walk(child, `${path}${entry.name}/`);
        } while (batch.length);
      }
    };
    await Promise.all([...items].map((item) => item.webkitGetAsEntry?.()).filter(Boolean).map((e) => walk(e, "")));
    return out;
  }

  const overlay = h("div", { class: "drop-overlay", hidden: true },
    h("div", { class: "drop-message" }, icon("upload", "icon large"), h("div", {}, "Drop to upload to"), h("div", { class: "mono" }, `${bucket}/${prefix}`)));
  let dragDepth = 0;
  const view = shell("buckets",
    pageHeader("", [
      deleteButton,
      h("button", { class: "icon-button", title: "Refresh", "aria-label": "Refresh", onclick: () => load() }, icon("refresh")),
      writable && [
        h("button", { class: "button", onclick: newFolder }, icon("plus"), "New folder"),
        h("button", { class: "button", onclick: () => folderInput.click() }, icon("folder"), "Upload folder"),
        h("button", { class: "button primary", onclick: () => fileInput.click() }, icon("upload"), "Upload"),
      ],
    ], crumbs),
    h("div", { class: "toolbar" }, filter, h("div", { class: "toolbar-end" }, uploadClass, versionsSwitch, viewToggle)),
    quotaSlot,
    lifecycleSlot,
    eventsSlot,
    body, drawer, overlay, fileInput, folderInput,
  );
  view.addEventListener("dragenter", (e) => {
    if (!writable || ![...e.dataTransfer.types].includes("Files")) return;
    dragDepth++;
    overlay.hidden = false;
  });
  view.addEventListener("dragover", (e) => {
    if (writable && [...e.dataTransfer.types].includes("Files")) e.preventDefault();
  });
  view.addEventListener("dragleave", () => {
    if (--dragDepth <= 0) {
      dragDepth = 0;
      overlay.hidden = true;
    }
  });
  view.addEventListener("drop", async (e) => {
    e.preventDefault();
    dragDepth = 0;
    overlay.hidden = true;
    if (!writable) return;
    const files = await filesFromDrop(e.dataTransfer.items);
    if (files.length) enqueue(files);
  });
  keyHandler = (e) => {
    const typing = /^(INPUT|TEXTAREA|SELECT)$/.test(document.activeElement?.tagName) || document.querySelector("dialog[open]");
    if (e.key === "/" && !typing) {
      e.preventDefault();
      filter.focus();
    } else if (e.key === "Escape" && !drawer.hidden && !document.querySelector("dialog[open]")) {
      closeDetails();
    } else if ((e.key === "Delete" || (e.key === "Backspace" && e.metaKey)) && !typing && state.selected.size && writable) {
      deleteSelected();
    }
  };
  const routeKey = `${bucket}/${prefix}`;
  load();
  return view;
}

const CHECKSUM_NAMES = {
  crc32: "CRC32", crc32c: "CRC32C", crc64nvme: "CRC64NVME", sha1: "SHA-1", sha256: "SHA-256", sha512: "SHA-512", md5: "MD5",
};

// In hex, as `shasum -a 512` prints it, so it can be compared with a local file's. A multipart
// upload's is the checksum of its parts' checksums ("…-3" from S3), which no local tool computes.
function checksumText({ value, type }) {
  const [, b64, parts] = /^([^-]*)(?:-(\d+))?$/.exec(value) ?? [];
  let hex;
  try {
    hex = Array.from(atob(b64), (c) => c.charCodeAt(0).toString(16).padStart(2, "0")).join("");
  } catch {
    return value;
  }
  return parts || type === "COMPOSITE" ? `${hex} (of ${parts ?? "its"} parts)` : hex;
}

// Folders made in this tab, as "bucket/prefix/". s3dal lists a prefix only once it holds an
// object, so a new folder would vanish from its parent until something is uploaded into it.
const madeFolders = new Set();

// ----- Uploads -----

// Whether an upload asks before it replaces an object of the same name. Kept per browser: it is
// how this person works, not a setting of the server.
const ASK_REPLACE = "s3dal-ask-replace";
const askBeforeReplacing = () => localStorage.getItem(ASK_REPLACE) !== "0";

const FINISHED = new Set(["done", "skipped", "cancelled", "failed"]);

// "a/b (1).txt", "a/b (2).txt", ...: the first name under which nothing exists yet.
async function freeKey(bucket, key) {
  const slash = key.lastIndexOf("/") + 1;
  const name = key.slice(slash);
  const dot = name.lastIndexOf(".");
  const [base, ext] = dot > 0 ? [name.slice(0, dot), name.slice(dot)] : [name, ""];
  for (let n = 1; ; n++) {
    const candidate = `${key.slice(0, slash)}${base} (${n})${ext}`;
    if (!(await s3.exists(bucket, candidate))) return candidate;
  }
}

// The panel is built once and its rows are updated in place. Rebuilt on every change, as it
// was, it lost its scroll position many times a second, and showing only the newest 200 rows
// hid the files being uploaded from a folder of more.
const uploads = {
  items: [],
  panel: null,
  collapsed: false,
  // Keeps the file being uploaded in view until the user scrolls the list themselves.
  follow: true,
  asking: Promise.resolve(),
  scheduled: false,
  autoClear: null,

  add(bucket, prefix, files, onDone, storageClass = "STANDARD") {
    const batch = { decision: null };
    const items = files.map(([path, file]) => ({
      key: prefix + path,
      prefix,
      bucket,
      storageClass,
      file,
      size: file.size,
      loaded: 0,
      status: "queued",
      controller: new AbortController(),
      batch,
    }));
    // An earlier batch's pending auto-clear would look at this one's items when it fires, and
    // take its panel away as soon as it finished, before anyone saw how it went.
    clearTimeout(this.autoClear);
    this.items.push(...items);
    this.build();
    this.follow = true;
    this.list.append(...items.map((item) => this.row(item)));
    this.update();
    pool(items, 3, (item) => this.run(item)).then(() => {
      onDone();
      // A finished batch gets out of the way; one with failures stays until dismissed, so
      // the user sees which files did not make it, and so does one the user has scrolled
      // through, which would otherwise vanish under the pointer.
      clearTimeout(this.autoClear);
      this.autoClear = setTimeout(() => {
        if (this.follow && this.items.every((i) => FINISHED.has(i.status) && i.status !== "failed")) this.clear();
      }, 4000);
    });
  },

  async run(item) {
    if (item.status === "cancelled") return;
    item.status = "uploading";
    this.paintItem(item);
    if (this.follow) item.li.scrollIntoView({ block: "nearest" });
    try {
      for (;;) {
        // Asked first, so a large file is not sent only to be refused; the write is still
        // conditional, so a file that appears meanwhile is caught rather than replaced.
        let ifNoneMatch = false;
        if (askBeforeReplacing()) {
          ifNoneMatch = true;
          if (await s3.exists(item.bucket, item.key)) {
            const choice = await this.ask(item);
            if (item.status === "cancelled") return;
            if (choice === "skip") {
              item.status = "skipped";
              return;
            }
            if (choice === "keep") item.key = await freeKey(item.bucket, item.key);
            else ifNoneMatch = false;
            this.paintItem(item);
          }
        }
        try {
          await s3.upload(item.bucket, item.key, item.file, {
            signal: item.controller.signal,
            ifNoneMatch,
            storageClass: item.storageClass,
            onProgress: (b) => {
              item.loaded = b;
              this.paintItem(item);
            },
          });
          break;
        } catch (err) {
          if (!(ifNoneMatch && err?.code === "PreconditionFailed")) throw err;
          item.loaded = 0;
        }
      }
      item.status = "done";
    } catch (err) {
      item.status = err?.name === "AbortError" ? "cancelled" : "failed";
      item.error = describeError(err)?.message;
    } finally {
      // The File is a handle to the user's disk; holding it after the upload is what kept a
      // large dropped folder's entries alive for as long as the panel stayed open.
      item.file = null;
      item.controller = null;
      this.paintItem(item);
    }
  },

  // One question at a time, however many uploads run: the answer may apply to the rest.
  ask(item) {
    const answer = this.asking.then(async () => {
      if (item.batch.decision) return item.batch.decision;
      if (item.status === "cancelled") return "skip";
      const all = h("input", { type: "checkbox" });
      // Replacing loses nothing in a versioned bucket; the question stays, saying so
      // (versioning.md, Open question 6).
      const kept = {
        Enabled: "Versioning is on: the current version is kept, and can be restored from the object's versions.",
        Suspended: "Versioning is suspended: the current version is kept only if it was written while versioning was on.",
      }[versioningOf.get(item.bucket)];
      const value = await dialog({
        title: "Replace existing object?",
        body: [
          h("p", {}, h("span", { class: "mono" }, `${item.bucket}/${item.key}`), " already exists."),
          kept && h("p", { class: "muted" }, kept),
          h("label", { class: "check-field" }, all, "Do the same for the other files of this upload that exist"),
        ],
        actions: [
          { label: "Skip", value: "skip", kind: "ghost" },
          { label: "Keep both", value: "keep" },
          { label: "Replace", value: "replace", kind: "primary", autofocus: true },
        ],
      });
      // Dismissing the dialog replaces nothing.
      const choice = value ?? "skip";
      if (all.checked) item.batch.decision = choice;
      return choice;
    });
    this.asking = answer;
    return answer;
  },

  build() {
    if (this.panel) return;
    this.title = h("strong", {});
    this.summary = h("span", { class: "muted small" });
    this.bar = h("div", { class: "progress" });
    this.toggle = h("button", {
      class: "icon-button small upload-toggle", "aria-label": "Minimize uploads", "aria-expanded": "true",
      onclick: () => {
        this.collapsed = !this.collapsed;
        this.update();
      },
    }, icon("chevron"));
    this.close = h("button", { class: "icon-button small", "aria-label": "Clear uploads", onclick: () => this.clear() }, icon("close"));
    this.list = h("ul", { class: "upload-list" });
    // Only the user's own scrolling stops the list from following the uploads.
    const stop = () => { this.follow = false; };
    for (const e of ["wheel", "touchmove", "keydown", "pointerdown"]) this.list.addEventListener(e, stop, { passive: true });
    this.panel = h("section", { class: "uploads", "aria-label": "Uploads" },
      h("header", {},
        h("div", { class: "upload-heading" }, this.title, this.summary),
        h("div", { class: "upload-actions" }, this.toggle, this.close)),
      this.bar,
      this.list);
    document.body.append(this.panel);
  },

  clear() {
    clearTimeout(this.autoClear);
    for (const item of this.items) item.controller?.abort();
    this.items = [];
    this.panel?.remove();
    this.panel = null;
  },

  row(item) {
    item.label = h("span", { class: "truncate" });
    item.state = h("span", { class: "upload-status" });
    item.cancel = h("button", {
      class: "icon-button small", "aria-label": "Cancel upload",
      onclick: () => {
        item.status = "cancelled";
        item.controller?.abort();
        this.paintItem(item);
      },
    }, icon("close"));
    item.bar = h("div", { class: "progress" });
    item.meta = h("span", { class: "muted small" });
    item.li = h("li", { class: "upload-item" }, h("div", { class: "upload-row" }, item.label, item.state, item.cancel), item.bar, item.meta);
    this.paintItem(item);
    return item.li;
  },

  paintItem(item) {
    if (!item.li) return;
    // The path inside the upload, not only the file name: a folder holds many index.html.
    item.label.textContent = item.key.slice(item.prefix.length);
    item.label.title = `${item.bucket}/${item.key}`;
    const status = item.status;
    item.li.className = `upload-item ${status}`;
    item.cancel.hidden = FINISHED.has(status);
    item.state.className = `upload-status ${status}`;
    item.state.replaceChildren(status === "done" ? icon("check", "icon small")
      : { queued: "Waiting", skipped: "Skipped", cancelled: "Cancelled", failed: "Failed" }[status] ?? "");
    item.bar.className = `progress ${status}`;
    const pct = item.size ? Math.min(100, (item.loaded / item.size) * 100) : 100;
    item.bar.style.setProperty("--progress", `${status === "uploading" ? pct : 0}%`);
    item.meta.textContent = status === "uploading" ? `${formatBytes(item.loaded)} of ${formatBytes(item.size)}`
      : status === "failed" ? item.error ?? "" : "";
    item.meta.hidden = !item.meta.textContent;
    this.schedule();
  },

  // The header sums every row, so it is redrawn once a frame however many progress events
  // arrive.
  schedule() {
    if (this.scheduled) return;
    this.scheduled = true;
    requestAnimationFrame(() => {
      this.scheduled = false;
      this.update();
    });
  },

  update() {
    if (!this.panel) return;
    let active = 0, done = 0, failed = 0, sent = 0, total = 0;
    for (const i of this.items) {
      if (i.status === "queued" || i.status === "uploading") active++;
      else if (i.status === "done") done++;
      else if (i.status === "failed") failed++;
      if (i.status === "skipped" || i.status === "cancelled") continue;
      total += i.size;
      sent += i.status === "done" ? i.size : i.loaded;
    }
    const n = this.items.length;
    this.title.textContent = active ? `Uploading ${active.toLocaleString()} file${active === 1 ? "" : "s"}`
      : failed ? `${failed.toLocaleString()} upload${failed === 1 ? "" : "s"} failed` : "Uploads complete";
    this.summary.textContent = `${done.toLocaleString()} of ${n.toLocaleString()} done · ${formatBytes(sent)} of ${formatBytes(total)}`;
    this.bar.className = `progress upload-total ${active ? "" : failed ? "failed" : "done"}`;
    this.bar.style.setProperty("--progress", `${total ? Math.min(100, (sent / total) * 100) : 100}%`);
    this.close.hidden = active > 0;
    this.panel.classList.toggle("collapsed", this.collapsed);
    this.toggle.setAttribute("aria-expanded", String(!this.collapsed));
    this.toggle.setAttribute("aria-label", this.collapsed ? "Restore uploads" : "Minimize uploads");
  },
};

// ----- Dashboard -----

function dashboardView() {
  const view = dashboard(() => s3.stats(), () => s3.usage(), { errorState, loadingRows });
  leavePage = view.stop;
  return shell("dashboard", pageHeader("Dashboard", [view.status]), view.el);
}

// ----- Server -----

const PROFILE = [
  ["native_etag", "ETags come from the backend", "s3dal keeps an MD5 ETag in each object's metadata"],
  ["native_content_type", "Content-Type is stored by the backend", "s3dal keeps Content-Type in metadata"],
  ["native_cache_control", "Cache-Control is stored by the backend", "s3dal keeps Cache-Control in metadata"],
  ["native_content_disposition", "Content-Disposition is stored by the backend", "s3dal keeps Content-Disposition in metadata"],
  ["native_content_encoding", "Content-Encoding is stored by the backend", "s3dal keeps Content-Encoding in metadata"],
  ["ordered_list", "The backend lists keys in order", "Listings sort keys themselves"],
  ["conditional_writes", "The backend enforces If-Match on writes", "Write conditions hold within this process only"],
  ["real_dirs", "The backend has real directories", "Folders exist only as key prefixes"],
];

function serverView() {
  const body = h("div", { class: "stack" }, loadingRows(5));
  const load = async () => {
    try {
      const info = await s3.info();
      const stat = (label, value, mono, hint) => h("div", { class: "stat" }, h("div", { class: "stat-label" }, label),
        h("div", { class: `stat-value ${mono ? "mono" : ""}` }, value), hint && h("div", { class: "muted small" }, hint));
      // Whether the collector takes the log now, and what it has missed since the start.
      const forward = info.log_forward_state;
      const forwardHint = !info.log_forward
        ? "Graylog, Elasticsearch, syslog, OpenTelemetry, Splunk or any HTTP input; set in Settings."
        : h("span", { class: forward?.failing ? "warn-text" : "" },
          forward?.failing ? "Unreachable; retrying. " : "Sending. ",
          forward?.dropped ? `${forward.dropped.toLocaleString()} lines dropped since the start.` : "No lines dropped.");
      const probe = info.probe
        ? ` Last probed ${formatDate(new Date(info.probe.at * 1000))}, in ${info.probe.ms.toLocaleString()} ms.`
        : "";
      body.replaceChildren(
        pendingBanner(info.pending_restart, "These settings changed in the config file but take effect when the server restarts."),
        h("section", { class: "stats" },
          stat("Version", info.version),
          stat("Backend", info.backend, true),
          stat("Uptime", formatUptime(info.uptime_secs)),
          stat("S3 endpoint", `${info.tls ? "https" : "http"}://${info.address}`, true),
          stat("Console", info.console_address ?? "—", true),
          stat("Region", REGION, true),
          stat("Log", info.log_file ?? "stderr", true, info.log_file ? "Rotated by size; set in Settings." : "In a container, docker logs shows it. Set a file in Settings."),
          stat("Log forwarding", info.log_forward ?? "Off", true, forwardHint),
          stat("Metrics", METRICS_MODES[info.metrics] ?? info.metrics, false,
            h("span", {}, h("span", { class: "mono" }, "/_s3dal/metrics"), " on both ports, for Prometheus. Charts are on the Dashboard.")),
        ),
        h("section", { class: "panel" },
          h("header", { class: "panel-header" }, h("h2", {}, "Backend profile"),
            h("p", { class: "muted small" }, `Measured at startup by writing and reading a probe object.${probe}`)),
          h("ul", { class: "profile" }, PROFILE.map(([key, yes, no]) => {
            const on = Boolean(info.profile[key]);
            return h("li", {}, icon(on ? "check" : "minus", `icon ${on ? "ok-text" : "muted"}`), h("span", {}, on ? yes : no));
          })),
        ),
        tiersPanel(info.tiers ?? []),
      );
    } catch (err) {
      body.replaceChildren(errorState(err, load));
    }
  };
  load();
  return shell("server", pageHeader("Server"), body);
}

// What a tier's probe found, in short: only the parts that are on.
const TIER_PROFILE = [["native_etag", "native ETags"], ["conditional_writes", "conditional writes"], ["ordered_list", "ordered listing"], ["real_dirs", "real directories"]];

// The cold tiers, from the config file's [[tier]] tables, with what the last sweep counted. The
// sweep runs daily on one instance, so another shows none until it holds the job's lease.
function tiersPanel(tiers) {
  if (!tiers.length) return "";
  const n = (v) => h("td", { class: "num num-col" }, v == null ? "—" : v.toLocaleString());
  return h("section", { class: "panel" },
    h("header", { class: "panel-header" }, h("h2", {}, "Cold tiers"),
      h("p", { class: "muted small" }, "Objects written or moved with a tier's storage class keep their bytes there and an empty stub here. Counted by the daily tier sweep; — until it has run on this server.")),
    h("table", { class: "table tiers-table" },
      h("thead", {}, h("tr", {},
        h("th", {}, "Tier"), h("th", {}, "Storage class"), h("th", {}, "Backend"), h("th", {}, "Profile"),
        h("th", { class: "num num-col" }, "Objects"), h("th", { class: "num num-col" }, "Bytes"), h("th", { class: "num num-col" }, "Missing"))),
      h("tbody", {}, tiers.map((t) => h("tr", {},
        h("td", { class: "mono" }, t.name),
        h("td", {}, h("span", { class: "pill class-pill" }, t.storage_class)),
        h("td", { class: "mono" }, t.backend),
        h("td", { class: "muted small" }, TIER_PROFILE.filter(([k]) => t.profile[k]).map(([, label]) => label).join(", ") || "—"),
        n(t.swept?.objects),
        h("td", { class: "num num-col", title: t.swept ? `${t.swept.bytes.toLocaleString()} bytes` : "" }, t.swept ? formatBytes(t.swept.bytes) : "—"),
        h("td", { class: `num num-col${t.swept?.dangling ? " warn-text" : ""}`, title: "Stubs whose tier object is gone; their reads fail. The server log names them." }, t.swept ? t.swept.dangling.toLocaleString() : "—"))))));
}

// ----- Restart -----

// How long the page waits for the server to answer again: it probes the backend before it
// listens, which a remote one may take a while to answer.
const RESTART_WAIT_MS = 120_000;

// What waits for a restart, with the button that does it.
function pendingBanner(pending, detail) {
  if (!pending.length) return "";
  return h("div", { class: "banner warn" }, icon("alert"),
    h("div", { class: "banner-text" }, h("strong", {}, "Restart to apply: "), h("span", { class: "mono" }, pending.join(", ")),
      detail && h("div", { class: "muted small" }, detail)),
    h("button", { class: "button", type: "button", onclick: () => restartServer() }, icon("refresh"), "Restart now"));
}

// While the server restarts the page has nothing to talk to; a modal says so, and keeps clicks
// from failing one by one meanwhile. Emptying a bucket uses it too, so the page is not left
// halfway through.
function busyDialog(title) {
  const status = h("span", {});
  const el = h("dialog", { class: "dialog", "aria-busy": "true" },
    h("h2", {}, title),
    h("div", { class: "dialog-body" }, h("p", { class: "restart-status", role: "status" }, icon("refresh", "icon spin"), status)));
  // Nothing to do but wait: Escape would only hide that the page cannot work meanwhile.
  el.addEventListener("cancel", (e) => e.preventDefault());
  document.body.append(el);
  el.showModal();
  return { say: (text) => { status.textContent = text; }, close: () => el.remove() };
}

// Whether the restarted server answers within the wait. The old process answers until it
// stops, so an answer counts only from a process up no longer than the wait has been.
async function cameBack(since) {
  while (Date.now() - since < RESTART_WAIT_MS) {
    await new Promise((resolve) => setTimeout(resolve, 1000));
    try {
      const info = await s3.info();
      if (info.uptime_secs * 1000 <= Date.now() - since + 1000) return true;
    } catch {
      // Not listening yet.
    }
  }
  return false;
}

// Where the console is after the restart, when that is not this page: a new port, HTTPS turned
// on or off, or no console at all. The page cannot follow it there.
function consoleMoves(config, info) {
  const pending = config.pending_restart;
  if (!pending.includes("console-address") && !pending.includes("tls-cert")) return null;
  const value = (name) => config.settings.find((s) => s.name === name)?.value ?? null;
  const address = value("console-address");
  if (!address) return "The console is off after the restart; turn it on again in the config file.";
  const portOf = (a) => a?.slice(a.lastIndexOf(":") + 1) ?? "";
  // The port the browser sees may be mapped, by Docker say; it changes only if the setting's does.
  const port = portOf(address) === portOf(info.console_address) ? location.port : portOf(address);
  const scheme = value("tls-cert") ? "https:" : "http:";
  if (port === location.port && scheme === location.protocol) return null;
  const url = `${scheme}//${location.hostname}${port ? `:${port}` : ""}/`;
  return h("span", {}, "The console moves to ", h("a", { href: url }, url), ": this page cannot follow it there.");
}

// Restarts the server once confirmed, after it has checked that it would start, and waits for
// it to answer again.
async function restartServer() {
  let config, info;
  try {
    [config, info] = await Promise.all([s3.config(), s3.info()]);
  } catch (err) {
    fail(err);
    return;
  }
  if (!config.pending_restart.length) {
    render();
    toast("Nothing waits for a restart.");
    return;
  }
  const moves = consoleMoves(config, info);
  const ok = await confirm({
    title: "Restart the server?",
    message: [
      "s3dal stops taking requests, gives those in flight 10 seconds, and starts again with the saved ",
      h("span", { class: "mono" }, config.pending_restart.join(", ")),
      ". Clients are refused until it is back: a few seconds, or as long as the backend takes to answer.",
      moves && h("span", { class: "restart-note" }, moves),
    ],
    action: "Restart",
    danger: false,
  });
  if (!ok) return;
  const waiting = busyDialog("Restarting the server");
  waiting.say("Checking that the server would start with these settings…");
  try {
    await s3.restart();
  } catch (err) {
    waiting.close();
    fail(err);
    return;
  }
  const since = Date.now();
  if (moves) {
    waiting.close();
    await dialog({ title: "Restarting", body: h("p", {}, moves), actions: [{ label: "OK", value: "ok", kind: "primary", autofocus: true }] });
    return;
  }
  waiting.say("Waiting for the server to start again…");
  const back = await cameBack(since);
  waiting.close();
  // Before the toast: painting the page replaces everything in the body, toasts included.
  render();
  if (back) toast("Restarted with the saved settings");
  else toast("The server has not answered for 2 minutes. Its log says why.", { kind: "error" });
}

// ----- Settings -----

const DESCRIPTIONS = {
  region: "Reported for every bucket. Clients, and this console, sign requests for it.",
  domain: "Base domains for virtual-hosted-style requests (bucket.domain). One per line.",
  "etag-buffer-limit": "Largest single upload hashed in memory for an MD5 ETag. B, KiB, MiB or GiB.",
  "stale-upload-age": "Unfinished multipart uploads older than this are removed. s, m, h or d; 0 keeps them.",
  "usage-scan-interval": "How often what each bucket holds is counted, for quotas: other servers' writes count from the next count. s, m, h or d; 0 stops counting.",
  backend: "OpenDAL service the objects are stored in. Its options are below.",
  "cache-size": "Memory for caching objects up to 1 MiB that GET reads from a remote backend; 0 turns it off. B, KiB, MiB or GiB. Not on fs, which the OS page cache covers.",
  "log-format": "How each line of the log is written.",
  address: "Where the S3 API listens.",
  "console-address": "Where this console listens.",
  "tls-cert": "Certificate chain; replaced without a restart when the file changes.",
  "tls-key": "Private key of the certificate.",
  "log-level": "The least severe lines logged. Custom takes target=level after it, comma-separated, as info,s3dal=debug.",
  "log-file": "File the log is written to; missing folders are created. Empty, the log goes to stderr, which docker logs and journald show.",
  "log-max-size": "Size past which the log file is renamed to <file>.1 and a new one started; 0 never rotates.",
  "log-max-files": "Rotated log files kept; older ones are removed.",
  "log-forward": "Also send the log to a collector, as well as to stderr or the file. Lines it cannot take in time are dropped, not waited for.",
  metrics: "Who may read the Prometheus metrics at /_s3dal/metrics, on the S3 and console ports.",
  "metrics-token": "What a scraper sends as Authorization: Bearer <token>; at least 16 characters. Empty keeps the one set.",
};

const METRICS_MODES = { token: "Bearer token", public: "Public", off: "Off" };

const LEVELS = ["trace", "debug", "info", "warn", "error", "off"];

const BACKENDS = { fs: "fs: a local directory", s3: "s3: an S3-compatible service", azblob: "azblob: Azure Blob Storage", gcs: "gcs: Google Cloud Storage" };

const COLLECTORS = [
  ["", "Off"],
  ["gelf+udp", "Graylog: GELF over UDP"],
  ["gelf+tcp", "Graylog: GELF over TCP"],
  ["gelf+http", "Graylog: GELF over HTTP"],
  ["gelf+https", "Graylog: GELF over HTTPS"],
  ["elasticsearch+http", "Elasticsearch, OpenSearch: HTTP"],
  ["elasticsearch+https", "Elasticsearch, OpenSearch: HTTPS"],
  ["syslog+udp", "Syslog: RFC 5424 over UDP"],
  ["syslog+tcp", "Syslog: RFC 5424 over TCP"],
  ["syslog+tls", "Syslog: RFC 5424 over TLS"],
  ["otlp+http", "OpenTelemetry: OTLP over HTTP"],
  ["otlp+https", "OpenTelemetry: OTLP over HTTPS"],
  ["splunk+http", "Splunk: HTTP Event Collector over HTTP"],
  ["splunk+https", "Splunk: HTTP Event Collector over HTTPS"],
  ["ndjson+http", "Any HTTP input: NDJSON over HTTP"],
  ["ndjson+https", "Any HTTP input: NDJSON over HTTPS"],
];

const COLLECTOR_ADDRESSES = {
  "gelf+udp": "graylog:12201",
  "gelf+tcp": "graylog:12201",
  "gelf+http": "graylog:12201/gelf",
  "gelf+https": "graylog.example.com/gelf",
  "elasticsearch+http": "elastic@es:9200/logs-s3dal-default",
  "elasticsearch+https": "elastic@es:9200/logs-s3dal-default",
  "syslog+udp": "rsyslog:514",
  "syslog+tcp": "rsyslog:514",
  "syslog+tls": "logs.example.com:6514",
  "otlp+http": "otel-collector:4318",
  "otlp+https": "otel.example.com",
  "splunk+http": "splunk:8088",
  "splunk+https": "splunk.example.com:8088",
  "ndjson+http": "vector:8080/s3dal",
  "ndjson+https": "logs.example.com/s3dal",
};

const PLACEHOLDERS = { "log-file": "stderr", "console-address": "Off", "tls-cert": "None, plain HTTP", "tls-key": "None" };

// The control a setting is edited with: a menu where its values are a fixed set, a text field
// where they are not. `read()` gives what would be saved, null for the default; it throws on
// input it cannot turn into a value, with a message for the page.
function settingControl(s, config) {
  const disabled = !s.editable;
  const text = (props = {}) => h("input", { class: "input mono", disabled, spellcheck: "false", autocomplete: "off", ...props });
  const menu = (options, value) => {
    const el = h("select", { class: "input", disabled }, options.map(([v, label]) => h("option", { value: v }, label)));
    el.value = value;
    return el;
  };
  switch (s.name) {
    case "log-format": {
      const el = menu([["text", "Text, for people"], ["json", "JSON, one object per line"]], s.value);
      return { el, read: () => el.value };
    }
    case "backend": {
      // A scheme this page does not list, set in the file by hand, stays selectable.
      const names = config.backends.includes(s.value) ? config.backends : [...config.backends, s.value];
      const el = menu(names.map((n) => [n, BACKENDS[n] ?? n]), s.value);
      return { el, read: () => el.value };
    }
    case "metrics": {
      const el = menu([
        ["token", "Bearer token: scrapers that send metrics-token"],
        ["public", "Public: anyone who reaches the port"],
        ["off", "Off: not served"],
      ], s.value);
      return { el, read: () => el.value };
    }
    case "metrics-token":
      return tokenControl(s, text);
    case "log-level":
      return levelControl(s.value, text, menu);
    case "log-forward":
      return collectorControl(s, text, menu);
    case "domain": {
      const el = h("textarea", { class: "input mono", rows: 2, disabled, spellcheck: "false" });
      el.value = (s.value ?? []).join("\n");
      return {
        el,
        read: () => {
          const list = el.value.split(/[\s,]+/).filter(Boolean);
          return list.length ? list : null;
        },
      };
    }
    default: {
      const el = text({ placeholder: PLACEHOLDERS[s.name] ?? "" });
      el.value = s.value === null || s.value === undefined ? "" : String(s.value);
      return { el, read: () => el.value.trim() || null };
    }
  }
}

// The page is never sent the token, so the field starts empty and saves only what is typed or
// generated. A generated one is shown, to be copied into the scraper's configuration.
function tokenControl(s, text) {
  const el = text({ type: "password", placeholder: s.secret ? "Set; type to replace" : "None: no one can scrape", "aria-label": "Metrics token" });
  const generate = h("button", {
    class: "icon-button", type: "button", "aria-label": "Generate a token", title: "Generate a token", disabled: el.disabled,
    onclick: () => {
      el.type = "text";
      el.value = randomKey(32, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_");
      el.dispatchEvent(new Event("input", { bubbles: true }));
      copyText(el.value, "Token copied");
    },
  }, icon("key"));
  return { el: h("div", { class: "input-group" }, el, generate), read: () => el.value.trim() || null };
}

// A level from the menu; Custom opens a field for directives such as info,s3dal=debug.
function levelControl(value, text, menu) {
  const level = value.toLowerCase();
  const custom = !LEVELS.includes(level);
  const select = menu([...LEVELS.map((l) => [l, l === "info" ? "info (default)" : l]), ["custom", "Custom…"]], custom ? "custom" : level);
  const directives = text({ placeholder: "info,s3dal=debug", hidden: !custom, "aria-label": "Log directives" });
  directives.value = custom ? value : "";
  select.addEventListener("change", () => {
    directives.hidden = select.value !== "custom";
    if (!directives.hidden) {
      directives.value ||= value;
      directives.focus();
    }
  });
  return {
    el: h("div", { class: "control-stack" }, select, directives),
    read: () => (select.value === "custom" ? directives.value.trim() || null : select.value),
  };
}

// The collector as a menu of protocols and an address. The page is never sent the password; a
// URL saved without one keeps the one the server has, so the field is filled only to change it.
function collectorControl(s, text, menu) {
  const at = s.value ? s.value.indexOf("://") : -1;
  const scheme = at < 0 ? "" : s.value.slice(0, at);
  const known = COLLECTORS.some(([v]) => v === scheme);
  const select = menu(known ? COLLECTORS : [...COLLECTORS, [scheme, scheme]], scheme);
  const address = text({ "aria-label": "Collector address" });
  address.value = at < 0 ? "" : s.value.slice(at + 3);
  const password = text({ type: "password", "aria-label": "Collector password" });
  // Splunk's HEC takes a token, with no user before it.
  const isToken = () => select.value.startsWith("splunk+");
  const paint = () => {
    const on = select.value !== "";
    address.hidden = !on;
    address.placeholder = COLLECTOR_ADDRESSES[select.value] ?? "host:port";
    password.hidden = !on || !select.value.includes("http");
    password.setAttribute("aria-label", isToken() ? "Collector token" : "Collector password");
    password.placeholder = s.secret && select.value === scheme
      ? (isToken() ? "Token unchanged" : "Password unchanged")
      : (isToken() ? "HEC token" : "Password, if it takes one");
  };
  select.addEventListener("change", () => {
    paint();
    if (!address.hidden) address.focus();
  });
  paint();
  return {
    el: h("div", { class: "control-stack" }, select, address, password),
    read: () => {
      if (!select.value) return null;
      let rest = address.value.trim();
      const secret = password.hidden ? "" : password.value;
      if (secret) {
        const end = rest.search(/[/?#]/);
        const authority = end < 0 ? rest : rest.slice(0, end);
        const user = authority.lastIndexOf("@");
        if (user >= 0) rest = `${authority.slice(0, user).split(":")[0]}:${encodeURIComponent(secret)}${rest.slice(user)}`;
        else if (isToken()) rest = `:${encodeURIComponent(secret)}@${rest}`;
        else throw new Error("Put the user before the host, as elastic@es:9200, to send a password.");
      }
      return `${select.value}://${rest}`;
    },
  };
}

// One row of a settings form, and whether its value differs from the one it opened with.
function settingRow(s, config) {
  const control = settingControl(s, config);
  const initial = JSON.stringify(control.read());
  return {
    control,
    changed: () => {
      try {
        return JSON.stringify(control.read()) !== initial;
      } catch {
        return true;
      }
    },
    row: h("div", { class: "setting" },
      h("div", { class: "setting-text" },
        h("div", { class: "setting-name mono" }, s.name, s.locked && h("span", { class: "pill", title: "Set by a flag or environment variable, which outranks the file" }, icon("lock", "icon small"), "Locked")),
        h("div", { class: "muted small" }, DESCRIPTIONS[s.name])),
      control.el),
  };
}

// The changed settings of a form, as the server takes them; null if one cannot be read, which
// has been reported.
function changedSettings(rows) {
  const settings = {};
  try {
    for (const [name, row] of Object.entries(rows)) {
      if (row.changed()) settings[name] = row.control.read();
    }
  } catch (err) {
    toast(err.message, { kind: "error" });
    return null;
  }
  return settings;
}

const ACCESS_NAMES = { "read-write": "Read and write", "read-only": "Read only", "write-only": "Write only" };

// A datetime-local field's value (local time, to the minute) for an RFC 3339 instant.
function localDateTime(iso) {
  const d = new Date(iso);
  return new Date(d.getTime() - d.getTimezoneOffset() * 60000).toISOString().slice(0, 16);
}

// The limits and expiry of a key pair, as the add and edit dialogs ask for them.
function limitFields({ access = "read-write", allow = null, expires = null } = {}) {
  const mode = h("select", { class: "input" },
    Object.entries(ACCESS_NAMES).map(([value, name]) => h("option", { value }, name)));
  mode.value = access;
  const scope = h("textarea", { class: "input mono", rows: 3, spellcheck: "false", placeholder: "photos\nlogs/app/" });
  scope.value = (allow ?? []).join("\n");
  const until = h("input", { class: "input", type: "datetime-local" });
  const shown = expires ? localDateTime(expires) : "";
  until.value = shown;
  return {
    fields: [
      field("Permission", mode, "Write only takes uploads and lists nothing, downloads nothing and deletes nothing: a drop box."),
      field("Limit to", scope, "One bucket, or bucket/folder/, per line; empty for every bucket. A key pair with any limit cannot open Server or Settings, nor create or delete buckets."),
      field("Expires", until, "Empty for never. In this browser's time zone; from then on the key is refused as unknown."),
    ],
    read: () => {
      const list = scope.value.split("\n").map((s) => s.trim()).filter(Boolean);
      // An expiry left as it was is sent back exactly, seconds included, which the field
      // cannot show.
      const end = until.value === shown ? expires : until.value ? new Date(until.value).toISOString() : null;
      return { access: mode.value, allow: list.length ? list : null, expires: end };
    },
  };
}

const relative = new Intl.RelativeTimeFormat("en", { numeric: "auto" });

// "expires in 3 days", or "Expired" once past, with the exact time on hover.
function describeExpiry(expires) {
  if (!expires) return null;
  const at = new Date(expires);
  const ms = at.getTime() - Date.now();
  if (ms <= 0) return h("span", { class: "pill warn", title: formatDate(at) }, "Expired");
  const [n, unit] = ms >= 86400000 ? [Math.round(ms / 86400000), "day"]
    : ms >= 3600000 ? [Math.round(ms / 3600000), "hour"]
    : [Math.max(1, Math.round(ms / 60000)), "minute"];
  return [" · ", h("span", { class: "expiry", title: formatDate(at) }, `expires ${relative.format(n, unit)}`)];
}

function describeLimits(c) {
  const limits = !c.allow && c.access === "read-write" ? "Full access"
    : h("span", {}, ACCESS_NAMES[c.access], " · ", h("span", { class: "mono" }, c.allow ? c.allow.join(", ") : "every bucket"));
  return h("span", {}, limits, describeExpiry(c.expires));
}

// The settings that take a restart: written to the file now, applied when the server starts
// again. A backend is probed before it is saved, so the restart does not find it broken.
function restartForm(config, save) {
  const inputs = {};
  const rows = config.settings.filter((s) => !s.live).map((s) => {
    inputs[s.name] = settingRow(s, config);
    return inputs[s.name].row;
  });

  const optsEditable = config.editable && !config.backend_opts_locked;
  let optsChanged = false;
  const opts = [];
  const list = h("div", { class: "opt-list" });
  const addOpt = ({ name = "", value = "", secret = false } = {}) => {
    const opt = {
      secret,
      name: h("input", { class: "input mono", value: name, placeholder: "root", "aria-label": "Option name", disabled: !optsEditable, spellcheck: "false" }),
      // A credential's value is never sent to the page; left empty, it keeps the one it has.
      value: h("input", {
        class: "input mono", value: value ?? "", type: secret ? "password" : "text", "aria-label": `Value of ${name || "the option"}`,
        placeholder: secret ? "Unchanged" : "/srv/data", disabled: !optsEditable, spellcheck: "false", autocomplete: "off",
      }),
    };
    opt.row = h("div", { class: "opt-row" }, opt.name, opt.value,
      h("button", {
        type: "button", class: "icon-button", "aria-label": `Remove ${name || "option"}`, disabled: !optsEditable,
        onclick: () => {
          opts.splice(opts.indexOf(opt), 1);
          opt.row.remove();
          optsChanged = true;
          changed();
        },
      }, icon("trash")));
    opts.push(opt);
    list.append(opt.row);
  };
  config.backend_opts.forEach(addOpt);

  // Save first in the footer: Enter in a field submits with the first button, and must not
  // restart the server.
  const saveButton = h("button", { class: "button", type: "submit", disabled: true }, "Save");
  const restartButton = h("button", { class: "button primary", type: "submit", disabled: true }, "Save and restart");
  const changed = () => {
    saveButton.disabled = restartButton.disabled = !optsChanged && !Object.values(inputs).some((row) => row.changed());
  };
  return h("form", {
    class: "panel",
    oninput: (e) => {
      if (list.contains(e.target)) optsChanged = true;
      changed();
    },
    // A menu's choice that shows or hides a field fires change, not input, in some browsers.
    onchange: changed,
    onsubmit: async (e) => {
      e.preventDefault();
      const settings = changedSettings(inputs);
      if (!settings) return;
      const edit = { settings };
      if (optsChanged) {
        edit.backend_opts = opts.filter((o) => o.name.value.trim())
          .map((o) => ({ name: o.name.value.trim(), value: o.secret && !o.value.value ? null : o.value.value }));
      }
      const restart = e.submitter === restartButton;
      const busy = restart ? restartButton : saveButton;
      const label = busy.textContent;
      saveButton.disabled = restartButton.disabled = true;
      const probing = "backend" in settings || edit.backend_opts;
      busy.textContent = probing ? "Checking the backend…" : "Saving…";
      if (!(await save(edit, restart ? "Saved" : "Saved. Restart the server to apply."))) {
        busy.textContent = label;
        changed();
        return;
      }
      if (restart) await restartServer();
    },
  },
    h("header", { class: "panel-header" }, h("h2", {}, "Restart required"),
      h("p", { class: "muted small" }, "Saved to the config file now, applied when the server restarts. An empty field returns to the default.")),
    rows,
    h("div", { class: "setting opt-setting" },
      h("div", { class: "setting-text" },
        h("div", { class: "setting-name mono" }, "backend-opt", config.backend_opts_locked && h("span", { class: "pill", title: "Set by a flag or environment variable, which outranks the file" }, icon("lock", "icon small"), "Locked")),
        h("div", { class: "muted small" }, "Options of the OpenDAL service, such as root, bucket or endpoint. Values that may be credentials are not shown.")),
      h("div", { class: "opt-editor" }, list,
        h("button", { type: "button", class: "button ghost", disabled: !optsEditable, onclick: () => { addOpt(); optsChanged = true; changed(); opts.at(-1).name.focus(); } }, icon("plus"), "Add option"))),
    h("div", { class: "panel-footer end" }, saveButton, restartButton),
  );
}

function settingsView() {
  // A fieldset, so a save can disable every control at once: the server's answer repaints the
  // page, and an edit made while it is awaited (a backend probe takes seconds) would be lost.
  const body = h("fieldset", { class: "stack" }, loadingRows(6));
  let config;
  const load = async () => {
    try {
      config = await s3.config();
      paint();
    } catch (err) {
      body.replaceChildren(errorState(err, load));
    }
  };

  const save = async (edit, message) => {
    body.disabled = true;
    try {
      config = await s3.saveConfig(edit);
      toast(message);
      paint();
      return true;
    } catch (err) {
      fail(err);
      return false;
    } finally {
      body.disabled = false;
    }
  };

  // Each pair with its limits and expiry: the server refuses a list without them, which would
  // lift them.
  const fileKeys = () => config.credentials.filter((c) => !c.locked)
    .map((c) => ({ access_key: c.access_key, access: c.access, allow: c.allow, expires: c.expires }));

  async function addKey() {
    const pair = generateKeyPair();
    const access = h("input", { class: "input mono", value: pair.accessKey, required: true, spellcheck: "false" });
    const secret = h("input", { class: "input mono", value: pair.secretKey, required: true, minLength: 8, spellcheck: "false" });
    const limits = limitFields();
    const value = await dialog({
      title: "Add key pair",
      body: [
        field("Access key", access),
        field("Secret key", h("div", { class: "input-group" }, secret,
          h("button", { type: "button", class: "icon-button", "aria-label": "Copy secret key", onclick: () => copyText(secret.value, "Secret key copied") }, icon("copy"))),
        "Copy it now: the console cannot show it again."),
        limits.fields,
      ],
      actions: [{ label: "Cancel", value: "", kind: "ghost" }, { label: "Add key pair", value: "ok", kind: "primary" }],
    });
    if (value !== "ok") return;
    await save({ credentials: [...fileKeys(), { access_key: access.value.trim(), secret_key: secret.value, ...limits.read() }] },
      `Added ${access.value.trim()}`);
  }

  async function editKey(c) {
    const limits = limitFields(c);
    const value = await dialog({
      title: `Limits of ${c.access_key}`,
      body: limits.fields,
      actions: [{ label: "Cancel", value: "", kind: "ghost" }, { label: "Save", value: "ok", kind: "primary" }],
    });
    if (value !== "ok") return;
    await save({ credentials: fileKeys().map((k) => (k.access_key === c.access_key ? { ...k, ...limits.read() } : k)) },
      `Saved the limits of ${c.access_key}`);
  }

  async function removeKey(key) {
    const ok = await confirm({ title: `Remove ${key}?`, message: "Clients signing with this key are refused from now on.", action: "Remove key" });
    if (!ok) return;
    await save({ credentials: fileKeys().filter((c) => c.access_key !== key) }, `Removed ${key}`);
  }

  function paint() {
    const editable = config.editable;
    const keys = h("section", { class: "panel" },
      h("header", { class: "panel-header row" },
        h("div", {}, h("h2", {}, "Key pairs"), h("p", { class: "muted small" }, "A key pair reaches every bucket unless it is limited to some.")),
        h("button", { class: "button primary", disabled: !editable, onclick: addKey }, icon("plus"), "Add key pair")),
      h("table", { class: "table" },
        h("thead", {}, h("tr", {}, h("th", {}, "Access key"), h("th", {}, "Access"), h("th", {}, "Source"), h("th", { class: "col-actions wide" }))),
        h("tbody", {}, config.credentials.map((c) => {
          const self = c.access_key === signedInAs;
          return h("tr", {},
            h("td", {}, h("span", { class: "name-cell" }, icon("key"), h("span", { class: "mono truncate" }, c.access_key), self && h("span", { class: "pill accent" }, "You"))),
            h("td", { class: "muted" }, describeLimits(c)),
            h("td", { class: "muted" }, c.locked ? h("span", { class: "pill" }, icon("lock", "icon small"), "Environment") : "Config file"),
            h("td", { class: "col-actions wide" },
              h("button", {
                class: "icon-button", "aria-label": `Edit the limits of ${c.access_key}`,
                title: c.locked ? "Set by a flag or environment variable" : self ? "You are signed in with this key" : `Edit the limits of ${c.access_key}`,
                disabled: !editable || c.locked || self,
                onclick: () => editKey(c),
              }, icon("edit")),
              h("button", {
                class: "icon-button", "aria-label": `Remove ${c.access_key}`,
                title: c.locked ? "Set by a flag or environment variable" : self ? "You are signed in with this key" : `Remove ${c.access_key}`,
                disabled: !editable || c.locked || self,
                onclick: () => removeKey(c.access_key),
              }, icon("trash"))));
        })),
      ),
    );

    const inputs = {};
    const liveRows = config.settings.filter((s) => s.live).map((s) => {
      inputs[s.name] = settingRow(s, config);
      return inputs[s.name].row;
    });
    const saveButton = h("button", { class: "button primary", type: "submit", disabled: true }, "Save changes");
    const changed = () => { saveButton.disabled = !Object.values(inputs).some((row) => row.changed()); };
    const form = h("form", {
      class: "panel",
      oninput: changed,
      onchange: changed,
      onsubmit: async (e) => {
        e.preventDefault();
        const settings = changedSettings(inputs);
        if (!settings) return;
        saveButton.disabled = true;
        const regionChanged = "region" in settings;
        if (await save({ settings }, "Settings saved and applied") && regionChanged) {
          // The page signs for the region it was served with.
          location.reload();
        }
      },
    },
      h("header", { class: "panel-header" }, h("h2", {}, "Server settings"),
        h("p", { class: "muted small" }, "Applied at once, without a restart. An empty field returns to the default.")),
      liveRows,
      h("div", { class: "panel-footer end" }, saveButton),
    );

    const fixed = restartForm(config, save);

    body.replaceChildren(
      editable
        ? h("div", { class: "banner" }, icon("settings"), h("div", {}, "Changes are written to ", h("span", { class: "mono" }, config.file), ". Edits made to the file by hand are picked up within seconds."))
        : h("div", { class: "banner warn" }, icon("lock"), h("div", {}, h("strong", {}, "Read only. "), config.reason ?? "")),
      pendingBanner(config.pending_restart),
      keys, form, fixed,
    );
  }

  // Outside `body`, which the server's answer replaces: this one needs no server.
  const ask = h("input", {
    type: "checkbox", role: "switch", checked: askBeforeReplacing(),
    onchange: () => localStorage.setItem(ASK_REPLACE, ask.checked ? "1" : "0"),
  });
  const browser = h("section", { class: "panel" },
    h("header", { class: "panel-header" }, h("h2", {}, "This browser"),
      h("p", { class: "muted small" }, "Kept in this browser, for every server it signs in to.")),
    h("label", { class: "setting check-setting" },
      h("div", { class: "setting-text" },
        h("div", { class: "setting-name" }, "Ask before replacing"),
        h("div", { class: "muted small" }, "An upload stops to ask when an object of the same name exists: replace it, skip the file, or keep both under a new name. Off, uploads replace without asking, as S3 clients do.")),
      ask));

  load();
  return shell("settings", pageHeader("Settings"), h("div", { class: "stack" }, body, browser));
}

// ----- Router -----

let keyHandler = null;
let currentRoute = "";
// Ends what the page left keeps running, the dashboard's refreshes.
let leavePage = null;

document.addEventListener("keydown", (e) => keyHandler?.(e));

function route() {
  keyHandler = null;
  leavePage?.();
  leavePage = null;
  const hash = location.hash.slice(1) || "/";
  currentRoute = "";
  if (hash === "/dashboard" && isAdmin()) return dashboardView();
  if (hash === "/server" && isAdmin()) return serverView();
  if (hash === "/settings" && isAdmin()) return settingsView();
  if (hash.startsWith("/b/")) {
    const rest = hash.slice(3);
    const i = rest.indexOf("/");
    const bucket = decodeURIComponent(i < 0 ? rest : rest.slice(0, i));
    const prefix = i < 0 ? "" : decodeURIComponent(rest.slice(i + 1));
    currentRoute = `${bucket}/${prefix}`;
    return browserView(bucket, prefix);
  }
  return bucketsView();
}

function render() {
  // Taken off the page with everything else and put back, which loses the list's position.
  const scrolled = uploads.list?.scrollTop ?? 0;
  document.body.replaceChildren(s3 ? route() : loginView());
  if (uploads.panel) {
    document.body.append(uploads.panel);
    uploads.list.scrollTop = scrolled;
  }
}

window.addEventListener("hashchange", render);
restore().finally(render);

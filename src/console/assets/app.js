// The console's single page. Routes live in the URL hash, so the browser's back and forward
// buttons and bookmarks work, and the server needs only one page.

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
  toast,
} from "./ui.js";

const REGION = document.querySelector('meta[name="s3dal-region"]').content;
const VERSION = document.querySelector('meta[name="s3dal-version"]').content;
// sessionStorage: the key pair lasts as long as the tab, survives a reload, and is never sent
// anywhere, nor shared with other tabs.
const SESSION = "s3dal-session";

let s3 = null;
let signedInAs = "";
// What the signed-in pair may reach, from the server: { admin, write, allow, public }. The page
// shows only what it can do; the server checks every request regardless.
let me = null;

const isAdmin = () => me?.admin === true;
const canWrite = () => me?.write === true;
const isPublic = (bucket) => me?.public?.includes(bucket) === true;

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
      // The settings tell whether the switch can save: the file may be read only, or the
      // setting fixed by a flag or variable.
      const [list, config] = await Promise.all([s3.listBuckets(), isAdmin() ? s3.config() : null]);
      const buckets = list.sort((a, b) => a.name.localeCompare(b.name));
      if (!buckets.length) {
        body.replaceChildren(isAdmin()
          ? emptyState("bucket", "No buckets yet", "A bucket holds objects. Create one to start uploading.",
            h("button", { class: "button primary", onclick: createBucket }, icon("plus"), "Create bucket"))
          : emptyState("bucket", "No buckets", "This key pair reaches no existing bucket."));
        return;
      }
      const setting = config?.settings.find((s) => s.name === "public-bucket");
      if (setting) me.public = setting.value;
      const access = (b) => {
        if (!setting) return isPublic(b.name) ? h("span", { class: "pill accent" }, "Public") : h("span", { class: "muted" }, "Private");
        const on = isPublic(b.name);
        const input = h("input", {
          type: "checkbox", role: "switch", checked: on, disabled: !setting.editable,
          "aria-label": `Public: ${b.name}`,
          onchange: () => setPublic(b.name, input.checked, input, load),
        });
        const why = setting.locked ? "Set by a flag or environment variable, which outranks the file" : config.reason;
        return h("label", { class: "switch", title: setting.editable ? "" : why ?? "" }, input, on ? "Public" : "Private");
      };
      body.replaceChildren(h("table", { class: "table" },
        h("thead", {}, h("tr", {}, h("th", {}, "Name"), h("th", { class: "col-access" }, "Access"), h("th", { class: "col-date" }, "Created"), h("th", { class: "col-actions" }))),
        h("tbody", {}, buckets.map((b) => h("tr", {},
          h("td", {}, h("a", { class: "name-cell", href: bucketHref(b.name, "") }, icon("bucket"), h("span", { class: "truncate" }, b.name))),
          h("td", { class: "col-access" }, access(b)),
          h("td", { class: "col-date muted" }, formatDate(b.created)),
          h("td", { class: "col-actions" }, isAdmin() &&
            h("button", { class: "icon-button", title: `Delete ${b.name}`, "aria-label": `Delete ${b.name}`, onclick: () => deleteBucket(b.name, load, setting) }, icon("trash"))),
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
  const next = on ? [...(me.public ?? []), name] : (me.public ?? []).filter((b) => b !== name);
  try {
    await s3.saveConfig({ settings: { "public-bucket": next.length ? next : null } });
    me.public = next;
    toast(on ? `${name} is public` : `${name} is private`);
  } catch (err) {
    input.checked = !on;
    fail(err);
  }
  reload();
}

async function deleteBucket(name, reload, publicSetting) {
  const ok = await confirm({ title: `Delete ${name}?`, message: "Only an empty bucket can be deleted. This cannot be undone.", action: "Delete bucket" });
  if (!ok) return;
  try {
    await s3.deleteBucket(name);
    toast(`Deleted ${name}`);
  } catch (err) {
    fail(err);
    return;
  }
  // Otherwise a bucket created later under the same name would start out public.
  if (publicSetting?.editable && isPublic(name)) {
    const next = me.public.filter((b) => b !== name);
    try {
      await s3.saveConfig({ settings: { "public-bucket": next.length ? next : null } });
      me.public = next;
    } catch (err) {
      fail(err);
    }
  }
  reload();
}

// ----- Objects -----

function displayName(key, prefix) {
  return key.slice(prefix.length);
}

function isImage(key) {
  return /\.(png|jpe?g|gif|webp|avif|svg|bmp|ico)$/i.test(key);
}

function isText(key, contentType) {
  return contentType.startsWith("text/") || /json|xml|yaml|javascript|csv/.test(contentType) ||
    /\.(txt|md|json|ya?ml|toml|csv|log|xml|html?|css|js|ts|rs|go|py|sh|ini|conf)$/i.test(key);
}

function browserView(bucket, prefix) {
  const state = { folders: [], objects: [], next: null, selected: new Set(), filter: "", active: null };
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

  const crumbs = h("nav", { class: "crumbs", "aria-label": "Path" },
    h("a", { href: "#/" }, "Buckets"),
    icon("chevron", "icon crumb-sep"),
    prefix ? h("a", { href: bucketHref(bucket, "") }, bucket) : h("span", { "aria-current": "page" }, bucket),
    prefix.split("/").filter(Boolean).map((part, i, parts) => {
      const to = parts.slice(0, i + 1).join("/") + "/";
      const last = i === parts.length - 1;
      return [icon("chevron", "icon crumb-sep"), last ? h("span", { "aria-current": "page" }, part) : h("a", { href: bucketHref(bucket, to) }, part)];
    }),
  );

  const selectAll = h("input", {
    type: "checkbox", "aria-label": "Select all",
    onchange: () => {
      const keys = visible().map((e) => e.key);
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
    const objects = state.objects.filter((o) => o.key !== prefix).map((o) => ({ ...o, folder: false }));
    return [...folders, ...objects].filter((e) => !state.filter || displayName(e.key, prefix).toLowerCase().includes(state.filter));
  }

  function paint() {
    const rows = visible();
    tbody.replaceChildren(...rows.map(row));
    const n = state.selected.size;
    deleteButton.hidden = n === 0 || !writable;
    deleteButton.lastChild.textContent = n ? `Delete ${n}` : "Delete";
    selectAll.checked = rows.length > 0 && rows.every((r) => state.selected.has(r.key));
    selectAll.indeterminate = n > 0 && !selectAll.checked;
    const total = state.folders.length + state.objects.length;
    count.textContent = `${total.toLocaleString()} item${total === 1 ? "" : "s"}${state.next ? " loaded" : ""}`;
    footer.replaceChildren(count, state.next ? more : "");
    if (!rows.length && !state.filter) {
      body.replaceChildren(writable
        ? emptyState("folder", prefix ? "This folder is empty" : "This bucket is empty",
          "Drop files anywhere on this page, or use Upload.",
          h("button", { class: "button primary", onclick: () => fileInput.click() }, icon("upload"), "Upload files"))
        : emptyState("folder", prefix ? "This folder is empty" : "This bucket is empty", "This key pair can read here, not write."));
    } else if (!body.contains(table)) {
      body.replaceChildren(table, footer);
    }
  }

  function row(e) {
    const name = displayName(e.key, prefix);
    const checked = state.selected.has(e.key);
    const check = h("input", {
      type: "checkbox", checked, "aria-label": `Select ${name}`,
      onchange: () => { check.checked ? state.selected.add(e.key) : state.selected.delete(e.key); paint(); },
    });
    const nameCell = e.folder
      ? h("a", { class: "name-cell", href: bucketHref(bucket, e.key) }, icon("folder", "icon folder-icon"), h("span", { class: "truncate" }, name.replace(/\/$/, "")))
      : h("button", { class: "name-cell link-button", onclick: () => openDetails(e) }, icon(isImage(e.key) ? "image" : "file"), h("span", { class: "truncate", title: name }, name));
    return h("tr", { class: [checked && "selected", state.active === e.key && "active"].filter(Boolean).join(" ") },
      h("td", { class: "col-check" }, check),
      h("td", {}, nameCell),
      h("td", { class: "col-size num muted" }, e.empty ? "Empty" : e.folder ? "—" : formatBytes(e.size)),
      h("td", { class: "col-date muted" }, e.folder ? "" : formatDate(e.modified)),
      h("td", { class: "col-actions" }, e.folder ? "" :
        h("button", { class: "icon-button", title: "Download", "aria-label": `Download ${name}`, onclick: () => download(e.key) }, icon("download"))),
    );
  }

  async function load(reset = true) {
    if (!inScope) {
      const next = new Set(scopes.filter((p) => p.startsWith(prefix)).map((p) => prefix + p.slice(prefix.length).split("/")[0] + "/"));
      Object.assign(state, { folders: [...next].sort(), objects: [], next: null });
      paint();
      return;
    }
    try {
      if (reset) {
        state.next = null;
        const page = await s3.list(bucket, prefix, null);
        Object.assign(state, { folders: page.folders, objects: page.objects, next: page.next });
        for (const k of [...state.selected]) {
          if (!state.folders.includes(k) && !state.objects.some((o) => o.key === k)) state.selected.delete(k);
        }
      } else {
        more.disabled = true;
        const page = await s3.list(bucket, prefix, state.next);
        state.folders.push(...page.folders);
        state.objects.push(...page.objects);
        state.next = page.next;
        more.disabled = false;
      }
      paint();
    } catch (err) {
      if (reset) body.replaceChildren(errorState(err, () => load()));
      else fail(err);
    }
  }

  async function download(key) {
    try {
      const a = h("a", { href: await s3.presign(bucket, key, { download: true, expires: 300 }), download: "" });
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

  async function openDetails(entry) {
    state.active = entry.key;
    paint();
    const name = displayName(entry.key, prefix);
    const props = h("dl", { class: "props" }, loadingRows(4));
    const preview = h("div", { class: "preview" });
    drawer.replaceChildren(
      h("header", { class: "drawer-header" },
        h("h2", { class: "truncate", title: name }, name),
        h("button", { class: "icon-button", "aria-label": "Close details", onclick: closeDetails }, icon("close"))),
      preview,
      h("div", { class: "drawer-actions" },
        h("button", { class: "button primary", onclick: () => download(entry.key) }, icon("download"), "Download"),
        h("button", { class: "button", onclick: () => shareLink(entry.key) }, icon("link"), "Copy link"),
        writable && h("button", { class: "icon-button danger-text", title: "Delete", "aria-label": `Delete ${name}`, onclick: () => deleteKeys([entry.key]) }, icon("trash"))),
      props,
    );
    drawer.hidden = false;
    try {
      const meta = await s3.head(bucket, entry.key);
      if (state.active !== entry.key) return;
      const prop = (k, v, mono) => v ? [h("dt", {}, k), h("dd", { class: mono ? "mono" : "" }, v)] : [];
      props.replaceChildren(
        ...prop("Key", entry.key, true),
        ...prop("Size", `${formatBytes(meta.size)} (${meta.size.toLocaleString()} bytes)`),
        ...prop("Last modified", formatDate(meta.modified)),
        ...prop("Content type", meta.contentType, true),
        ...prop("ETag", meta.etag, true),
        ...meta.checksums.flatMap((c) => prop(`Checksum (${CHECKSUM_NAMES[c.algorithm] ?? c.algorithm})`, checksumText(c), true)),
        ...prop("Cache control", meta.cacheControl, true),
        ...prop("Content encoding", meta.contentEncoding, true),
        ...meta.metadata.flatMap(([k, v]) => prop(`x-amz-meta-${k}`, v, true)),
      );
      if (isImage(entry.key)) {
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

  async function shareLink(key) {
    const open = isPublic(bucket);
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
    const ok = await confirm({ title: "Delete?", message: `This deletes ${what}. It cannot be undone.`, action: "Delete" });
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
      if (keys.includes(state.active)) closeDetails();
      load();
    } catch (err) {
      fail(err);
    }
  }

  function enqueue(files) {
    uploads.add(bucket, prefix, files, () => {
      if (currentRoute === routeKey) load();
    });
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
    h("div", { class: "toolbar" }, filter),
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

  add(bucket, prefix, files, onDone) {
    const batch = { decision: null };
    const items = files.map(([path, file]) => ({
      key: prefix + path,
      prefix,
      bucket,
      file,
      size: file.size,
      loaded: 0,
      status: "queued",
      controller: new AbortController(),
      batch,
    }));
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
      setTimeout(() => {
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
      const value = await dialog({
        title: "Replace existing object?",
        body: [
          h("p", {}, h("span", { class: "mono" }, `${item.bucket}/${item.key}`), " already exists."),
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
      body.replaceChildren(
        info.pending_restart.length ? h("div", { class: "banner warn" }, icon("alert"),
          h("div", {}, h("strong", {}, "Restart to apply: "), info.pending_restart.join(", "),
            h("div", { class: "muted small" }, "These settings changed in the config file but take effect when the server restarts."))) : "",
        h("section", { class: "stats" },
          stat("Version", info.version),
          stat("Backend", info.backend, true),
          stat("Uptime", formatUptime(info.uptime_secs)),
          stat("S3 endpoint", `${info.tls ? "https" : "http"}://${info.address}`, true),
          stat("Console", info.console_address ?? "—", true),
          stat("Region", REGION, true),
          stat("Log", info.log_file ?? "stderr", true, info.log_file ? "Rotated by size; set in Settings." : "In a container, docker logs shows it. Set a file in Settings."),
          stat("Log forwarding", info.log_forward ?? "Off", true, info.log_forward ? "" : "Graylog or Elasticsearch, set in Settings."),
        ),
        h("section", { class: "panel" },
          h("header", { class: "panel-header" }, h("h2", {}, "Backend profile"),
            h("p", { class: "muted small" }, "Measured at startup by writing and reading a probe object.")),
          h("ul", { class: "profile" }, PROFILE.map(([key, yes, no]) => {
            const on = Boolean(info.profile[key]);
            return h("li", {}, icon(on ? "check" : "minus", `icon ${on ? "ok-text" : "muted"}`), h("span", {}, on ? yes : no));
          })),
        ),
      );
    } catch (err) {
      body.replaceChildren(errorState(err, load));
    }
  };
  load();
  return shell("server", pageHeader("Server"), body);
}

// ----- Settings -----

const DESCRIPTIONS = {
  region: "Reported for every bucket. Clients, and this console, sign requests for it.",
  domain: "Base domains for virtual-hosted-style requests (bucket.domain). One per line.",
  "etag-buffer-limit": "Largest single upload hashed in memory for an MD5 ETag. B, KiB, MiB or GiB.",
  "stale-upload-age": "Unfinished multipart uploads older than this are removed. s, m, h or d; 0 keeps them.",
  backend: "OpenDAL service the objects are stored in. Its options are below.",
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
};

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
];

const COLLECTOR_ADDRESSES = {
  "gelf+udp": "graylog:12201",
  "gelf+tcp": "graylog:12201",
  "gelf+http": "graylog:12201/gelf",
  "gelf+https": "graylog.example.com/gelf",
  "elasticsearch+http": "elastic@es:9200/logs-s3dal-default",
  "elasticsearch+https": "elastic@es:9200/logs-s3dal-default",
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
  const paint = () => {
    const on = select.value !== "";
    address.hidden = !on;
    address.placeholder = COLLECTOR_ADDRESSES[select.value] ?? "host:port";
    password.hidden = !on || !select.value.includes("http");
    password.placeholder = s.secret && select.value === scheme ? "Password unchanged" : "Password, if it takes one";
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
        if (user < 0) throw new Error("Put the user before the host, as elastic@es:9200, to send a password.");
        rest = `${authority.slice(0, user).split(":")[0]}:${encodeURIComponent(secret)}${rest.slice(user)}`;
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

// The two limits of a key pair, as the add and edit dialogs ask for them.
function limitFields({ access = "read-write", allow = null } = {}) {
  const mode = h("select", { class: "input" },
    h("option", { value: "read-write" }, "Read and write"),
    h("option", { value: "read-only" }, "Read only"));
  mode.value = access;
  const scope = h("textarea", { class: "input mono", rows: 3, spellcheck: "false", placeholder: "photos\nlogs/app/" });
  scope.value = (allow ?? []).join("\n");
  return {
    fields: [
      field("Permission", mode),
      field("Limit to", scope, "One bucket, or bucket/folder/, per line; empty for every bucket. A key pair with any limit cannot open Server or Settings, nor create or delete buckets."),
    ],
    read: () => {
      const list = scope.value.split("\n").map((s) => s.trim()).filter(Boolean);
      return { access: mode.value, allow: list.length ? list : null };
    },
  };
}

function describeLimits(c) {
  if (!c.allow && c.access !== "read-only") return "Full access";
  const mode = c.access === "read-only" ? "Read only" : "Read and write";
  return h("span", {}, mode, " · ", h("span", { class: "mono" }, c.allow ? c.allow.join(", ") : "every bucket"));
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

  const saveButton = h("button", { class: "button primary", type: "submit", disabled: true }, "Save for restart");
  const changed = () => {
    saveButton.disabled = !optsChanged && !Object.values(inputs).some((row) => row.changed());
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
      saveButton.disabled = true;
      const probing = "backend" in settings || edit.backend_opts;
      saveButton.textContent = probing ? "Checking the backend…" : "Saving…";
      if (!(await save(edit, "Saved. Restart the server to apply."))) {
        saveButton.textContent = "Save for restart";
        changed();
      }
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
    h("div", { class: "panel-footer end" }, saveButton),
  );
}

function settingsView() {
  const body = h("div", { class: "stack" }, loadingRows(6));
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
    try {
      config = await s3.saveConfig(edit);
      toast(message);
      paint();
      return true;
    } catch (err) {
      fail(err);
      return false;
    }
  };

  // Each pair with its limits: the server refuses a list without them, which would lift them.
  const fileKeys = () => config.credentials.filter((c) => !c.locked)
    .map((c) => ({ access_key: c.access_key, access: c.access, allow: c.allow }));

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
    // Public buckets are switched on the Buckets page, next to the buckets themselves.
    const liveRows = config.settings.filter((s) => s.live && s.name !== "public-bucket").map((s) => {
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
      config.pending_restart.length ? h("div", { class: "banner warn" }, icon("alert"),
        h("div", {}, h("strong", {}, "Restart to apply: "), h("span", { class: "mono" }, config.pending_restart.join(", ")))) : "",
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

document.addEventListener("keydown", (e) => keyHandler?.(e));

function route() {
  keyHandler = null;
  const hash = location.hash.slice(1) || "/";
  currentRoute = "";
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

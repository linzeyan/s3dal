// The console's single page. Routes live in the URL hash, so the browser's back and forward
// buttons and bookmarks work, and the server needs only one page.

import { S3 } from "./s3.js";
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

function fail(err) {
  const d = describeError(err);
  if (d) toast(d.message, { kind: "error", detail: d.detail });
}

function restore() {
  try {
    const { accessKey, secretKey } = JSON.parse(sessionStorage.getItem(SESSION) ?? "null") ?? {};
    if (!accessKey || !secretKey) return;
    s3 = new S3({ accessKey, secretKey, region: REGION });
    signedInAs = accessKey;
  } catch {
    sessionStorage.removeItem(SESSION);
  }
}

function signOut() {
  sessionStorage.removeItem(SESSION);
  s3 = null;
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
        // Any signed request proves the pair; this one also tells the console it may use the
        // settings API.
        await candidate.info();
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
        link("#/server", "server", "Server", "server"),
        link("#/settings", "settings", "Settings", "settings"),
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
        body.replaceChildren(emptyState("bucket", "No buckets yet", "A bucket holds objects. Create one to start uploading.",
          h("button", { class: "button primary", onclick: createBucket }, icon("plus"), "Create bucket")));
        return;
      }
      body.replaceChildren(h("table", { class: "table" },
        h("thead", {}, h("tr", {}, h("th", {}, "Name"), h("th", { class: "col-date" }, "Created"), h("th", { class: "col-actions" }))),
        h("tbody", {}, buckets.map((b) => h("tr", {},
          h("td", {}, h("a", { class: "name-cell", href: bucketHref(b.name, "") }, icon("bucket"), h("span", { class: "truncate" }, b.name))),
          h("td", { class: "col-date muted" }, formatDate(b.created)),
          h("td", { class: "col-actions" },
            h("button", { class: "icon-button", title: `Delete ${b.name}`, "aria-label": `Delete ${b.name}`, onclick: () => deleteBucket(b.name, load) }, icon("trash"))),
        ))),
      ));
    } catch (err) {
      body.replaceChildren(errorState(err, load));
    }
  };
  load();
  return shell("buckets",
    pageHeader("Buckets", [h("button", { class: "button primary", onclick: createBucket }, icon("plus"), "Create bucket")]),
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

async function deleteBucket(name, reload) {
  const ok = await confirm({ title: `Delete ${name}?`, message: "Only an empty bucket can be deleted. This cannot be undone.", action: "Delete bucket" });
  if (!ok) return;
  try {
    await s3.deleteBucket(name);
    toast(`Deleted ${name}`);
    reload();
  } catch (err) {
    fail(err);
  }
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
    deleteButton.hidden = n === 0;
    deleteButton.lastChild.textContent = n ? `Delete ${n}` : "Delete";
    selectAll.checked = rows.length > 0 && rows.every((r) => state.selected.has(r.key));
    selectAll.indeterminate = n > 0 && !selectAll.checked;
    const total = state.folders.length + state.objects.length;
    count.textContent = `${total.toLocaleString()} item${total === 1 ? "" : "s"}${state.next ? " loaded" : ""}`;
    footer.replaceChildren(count, state.next ? more : "");
    if (!rows.length && !state.filter) {
      body.replaceChildren(emptyState("folder", prefix ? "This folder is empty" : "This bucket is empty",
        "Drop files anywhere on this page, or use Upload.",
        h("button", { class: "button primary", onclick: () => fileInput.click() }, icon("upload"), "Upload files")));
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
        h("button", { class: "icon-button danger-text", title: "Delete", "aria-label": `Delete ${name}`, onclick: () => deleteKeys([entry.key]) }, icon("trash"))),
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
    const expiries = [["1 hour", 3600], ["1 day", 86400], ["7 days", 604800]];
    const select = h("select", { class: "input" }, expiries.map(([label, secs]) => h("option", { value: String(secs) }, label)));
    const value = await dialog({
      title: "Copy a link",
      body: [h("p", { class: "muted" }, "Anyone with the link can download this object until it expires, without signing in."),
        field("Expires after", select)],
      actions: [{ label: "Cancel", value: "", kind: "ghost" }, { label: "Copy link", value: "ok", kind: "primary", autofocus: true }],
    });
    if (value !== "ok") return;
    try {
      await copyText(await s3.presign(bucket, key, { expires: Number(select.value) }), "Link copied");
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
      h("button", { class: "button", onclick: newFolder }, icon("plus"), "New folder"),
      h("button", { class: "button", onclick: () => folderInput.click() }, icon("folder"), "Upload folder"),
      h("button", { class: "button primary", onclick: () => fileInput.click() }, icon("upload"), "Upload"),
    ], crumbs),
    h("div", { class: "toolbar" }, filter),
    body, drawer, overlay, fileInput, folderInput,
  );
  view.addEventListener("dragenter", (e) => {
    if (![...e.dataTransfer.types].includes("Files")) return;
    dragDepth++;
    overlay.hidden = false;
  });
  view.addEventListener("dragover", (e) => {
    if ([...e.dataTransfer.types].includes("Files")) e.preventDefault();
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
    } else if ((e.key === "Delete" || (e.key === "Backspace" && e.metaKey)) && !typing && state.selected.size) {
      deleteSelected();
    }
  };
  const routeKey = `${bucket}/${prefix}`;
  load();
  return view;
}

// Folders made in this tab, as "bucket/prefix/". s3dal lists a prefix only once it holds an
// object, so a new folder would vanish from its parent until something is uploaded into it.
const madeFolders = new Set();

// ----- Uploads -----

const uploads = {
  items: [],
  panel: null,

  add(bucket, prefix, files, onDone) {
    const batch = files.map(([path, file]) => ({
      key: prefix + path,
      bucket,
      file,
      loaded: 0,
      status: "queued",
      controller: new AbortController(),
    }));
    this.items.push(...batch);
    this.paint();
    pool(batch, 3, async (item) => {
      if (item.status === "cancelled") return;
      item.status = "uploading";
      this.paint();
      try {
        await s3.upload(item.bucket, item.key, item.file, {
          signal: item.controller.signal,
          onProgress: (b) => {
            item.loaded = b;
            this.paintItem(item);
          },
        });
        item.status = "done";
      } catch (err) {
        item.status = err?.name === "AbortError" ? "cancelled" : "failed";
        item.error = describeError(err)?.message;
      }
      this.paint();
    }).then(() => {
      onDone();
      // A finished batch gets out of the way; one with failures stays until dismissed, so
      // the user sees which files did not make it.
      setTimeout(() => {
        const idle = this.items.every((i) => i.status === "done" || i.status === "cancelled");
        if (idle) {
          this.items = [];
          this.paint();
        }
      }, 4000);
    });
  },

  paintItem(item) {
    const pct = item.file.size ? Math.min(100, (item.loaded / item.file.size) * 100) : 100;
    item.bar?.style.setProperty("--progress", `${pct}%`);
    if (item.meta) item.meta.textContent = `${formatBytes(item.loaded)} of ${formatBytes(item.file.size)}`;
  },

  paint() {
    if (!this.items.length) {
      this.panel?.remove();
      this.panel = null;
      return;
    }
    const active = this.items.filter((i) => i.status === "queued" || i.status === "uploading").length;
    const failed = this.items.filter((i) => i.status === "failed").length;
    const title = active ? `Uploading ${active} file${active === 1 ? "" : "s"}` : failed ? `${failed} upload${failed === 1 ? "" : "s"} failed` : "Uploads complete";
    const list = h("ul", { class: "upload-list" }, this.items.slice(-200).reverse().map((item) => {
      item.bar = h("div", { class: `progress ${item.status}` });
      item.meta = h("span", { class: "muted small" });
      const status = {
        queued: "Waiting",
        done: "Done",
        cancelled: "Cancelled",
        failed: item.error ?? "Failed",
      }[item.status];
      const li = h("li", { class: "upload-item" },
        h("div", { class: "upload-row" },
          h("span", { class: "truncate", title: `${item.bucket}/${item.key}` }, item.key.split("/").pop()),
          item.status === "uploading" || item.status === "queued"
            ? h("button", { class: "icon-button small", "aria-label": "Cancel upload", onclick: () => { item.status = "cancelled"; item.controller.abort(); this.paint(); } }, icon("close"))
            : h("span", { class: `upload-status ${item.status}` }, item.status === "done" ? icon("check", "icon small") : status)),
        item.bar,
        item.status === "uploading" ? item.meta : status && item.status !== "done" ? h("span", { class: "muted small" }, status) : "",
      );
      this.paintItem(item);
      return li;
    }));
    const panel = h("section", { class: "uploads", "aria-label": "Uploads" },
      h("header", {}, h("strong", {}, title),
        active ? "" : h("button", { class: "icon-button small", "aria-label": "Clear uploads", onclick: () => { this.items = []; this.paint(); } }, icon("close"))),
      list);
    if (this.panel) this.panel.replaceWith(panel);
    else document.body.append(panel);
    this.panel = panel;
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
      const stat = (label, value, mono) => h("div", { class: "stat" }, h("div", { class: "stat-label" }, label), h("div", { class: `stat-value ${mono ? "mono" : ""}` }, value));
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
  backend: "OpenDAL service the objects are stored in.",
  address: "Where the S3 API listens.",
  "console-address": "Where this console listens.",
  "tls-cert": "Certificate chain; replaced without a restart when the file changes.",
  "tls-key": "Private key of the certificate.",
};

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

  const fileKeys = () => config.credentials.filter((c) => !c.locked).map((c) => ({ access_key: c.access_key }));

  async function addKey() {
    const pair = generateKeyPair();
    const access = h("input", { class: "input mono", value: pair.accessKey, required: true, spellcheck: "false" });
    const secret = h("input", { class: "input mono", value: pair.secretKey, required: true, minLength: 8, spellcheck: "false" });
    const value = await dialog({
      title: "Add key pair",
      body: [
        field("Access key", access),
        field("Secret key", h("div", { class: "input-group" }, secret,
          h("button", { type: "button", class: "icon-button", "aria-label": "Copy secret key", onclick: () => copyText(secret.value, "Secret key copied") }, icon("copy"))),
        "Copy it now: the console cannot show it again."),
      ],
      actions: [{ label: "Cancel", value: "", kind: "ghost" }, { label: "Add key pair", value: "ok", kind: "primary" }],
    });
    if (value !== "ok") return;
    await save({ credentials: [...fileKeys(), { access_key: access.value.trim(), secret_key: secret.value }] },
      `Added ${access.value.trim()}`);
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
        h("div", {}, h("h2", {}, "Key pairs"), h("p", { class: "muted small" }, "Every key pair has full access to every bucket.")),
        h("button", { class: "button primary", disabled: !editable, onclick: addKey }, icon("plus"), "Add key pair")),
      h("table", { class: "table" },
        h("thead", {}, h("tr", {}, h("th", {}, "Access key"), h("th", {}, "Source"), h("th", { class: "col-actions" }))),
        h("tbody", {}, config.credentials.map((c) => {
          const self = c.access_key === signedInAs;
          return h("tr", {},
            h("td", {}, h("span", { class: "name-cell" }, icon("key"), h("span", { class: "mono truncate" }, c.access_key), self && h("span", { class: "pill accent" }, "You"))),
            h("td", { class: "muted" }, c.locked ? h("span", { class: "pill" }, icon("lock", "icon small"), "Environment") : "Config file"),
            h("td", { class: "col-actions" },
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
      const value = Array.isArray(s.value) ? s.value.join("\n") : s.value ?? "";
      const input = s.name === "domain"
        ? h("textarea", { class: "input mono", rows: 2, value, disabled: !s.editable, spellcheck: "false" })
        : h("input", { class: "input mono", value, disabled: !s.editable, spellcheck: "false" });
      input.value = value;
      inputs[s.name] = { input, initial: value };
      return h("div", { class: "setting" },
        h("div", { class: "setting-text" },
          h("div", { class: "setting-name mono" }, s.name, s.locked && h("span", { class: "pill", title: "Set by a flag or environment variable, which outranks the file" }, icon("lock", "icon small"), "Locked")),
          h("div", { class: "muted small" }, DESCRIPTIONS[s.name])),
        input);
    });
    const saveButton = h("button", { class: "button primary", type: "submit", disabled: true }, "Save changes");
    const form = h("form", {
      class: "panel",
      oninput: () => { saveButton.disabled = !Object.values(inputs).some(({ input, initial }) => input.value !== initial); },
      onsubmit: async (e) => {
        e.preventDefault();
        const settings = {};
        for (const [name, { input, initial }] of Object.entries(inputs)) {
          if (input.value === initial) continue;
          const v = input.value.trim();
          if (name === "domain") {
            const list = v.split(/[\s,]+/).filter(Boolean);
            settings[name] = list.length ? list : null;
          } else {
            settings[name] = v || null;
          }
        }
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

    const fixed = h("section", { class: "panel" },
      h("header", { class: "panel-header" }, h("h2", {}, "Restart required"),
        h("p", { class: "muted small" }, "Changed in the config file, these take effect when the server restarts.")),
      h("dl", { class: "props wide" }, config.settings.filter((s) => !s.live).flatMap((s) => [
        h("dt", { class: "mono" }, s.name),
        h("dd", {}, h("span", { class: "mono" }, s.value ?? "—"), h("div", { class: "muted small" }, DESCRIPTIONS[s.name])),
      ])));

    body.replaceChildren(
      editable
        ? h("div", { class: "banner" }, icon("settings"), h("div", {}, "Changes are written to ", h("span", { class: "mono" }, config.file), " and applied at once. Edits made to the file by hand are picked up within seconds."))
        : h("div", { class: "banner warn" }, icon("lock"), h("div", {}, h("strong", {}, "Read only. "), config.reason ?? "")),
      keys, form, fixed,
    );
  }

  load();
  return shell("settings", pageHeader("Settings"), body);
}

// ----- Router -----

let keyHandler = null;
let currentRoute = "";

document.addEventListener("keydown", (e) => keyHandler?.(e));

function route() {
  keyHandler = null;
  const hash = location.hash.slice(1) || "/";
  currentRoute = "";
  if (hash === "/server") return serverView();
  if (hash === "/settings") return settingsView();
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
  document.body.replaceChildren(s3 ? route() : loginView());
  if (uploads.panel) document.body.append(uploads.panel);
}

window.addEventListener("hashchange", render);
restore();
render();

// DOM helpers. Every element is built with createElement and every string set as text, never
// as HTML: object names and metadata are whatever a client stored, and markup in one must not
// run on the page that holds the secret key.

export function h(tag, props = {}, ...kids) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(props ?? {})) {
    if (v === undefined || v === null || v === false) continue;
    if (k === "class") el.className = v;
    else if (k === "dataset") Object.assign(el.dataset, v);
    else if (k.startsWith("on")) el.addEventListener(k.slice(2), v);
    else if (k in el && typeof v !== "string") el[k] = v;
    else el.setAttribute(k, v === true ? "" : v);
  }
  append(el, kids);
  return el;
}

function append(el, kids) {
  for (const kid of kids.flat(Infinity)) {
    if (kid === null || kid === undefined || kid === false) continue;
    el.append(kid instanceof Node ? kid : document.createTextNode(String(kid)));
  }
}

// Stroke icons on a 24-unit grid, 1.75 wide, drawn from path data only.
const ICONS = {
  bucket: ["M4 7h16", "M5 7l1.5 12.2a2 2 0 0 0 2 1.8h7a2 2 0 0 0 2-1.8L19 7", "M8 7V5.5A1.5 1.5 0 0 1 9.5 4h5A1.5 1.5 0 0 1 16 5.5V7"],
  folder: ["M3.5 7.5A1.5 1.5 0 0 1 5 6h4l2 2h8a1.5 1.5 0 0 1 1.5 1.5v8A1.5 1.5 0 0 1 19 19H5a1.5 1.5 0 0 1-1.5-1.5z"],
  file: ["M14 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8z", "M14 3v5h5"],
  image: ["M5 4h14a1 1 0 0 1 1 1v14a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V5a1 1 0 0 1 1-1z", "M4 16l4.5-4.5a1.5 1.5 0 0 1 2 0L16 17", "M14 14l1.5-1.5a1.5 1.5 0 0 1 2 0L20 15", "M15 8.5h.01"],
  upload: ["M12 16V4", "M7 9l5-5 5 5", "M5 20h14"],
  download: ["M12 4v12", "M7 11l5 5 5-5", "M5 20h14"],
  plus: ["M12 5v14", "M5 12h14"],
  trash: ["M4 7h16", "M10 11v6", "M14 11v6", "M6 7l1 12a2 2 0 0 0 2 2h6a2 2 0 0 0 2-2l1-12", "M9 7V4h6v3"],
  refresh: ["M20 11a8 8 0 1 0-2.3 5.7", "M20 4v7h-7"],
  link: ["M10 14a4 4 0 0 0 5.66 0l3-3a4 4 0 0 0-5.66-5.66l-1 1", "M14 10a4 4 0 0 0-5.66 0l-3 3a4 4 0 0 0 5.66 5.66l1-1"],
  copy: ["M9 9h10a1 1 0 0 1 1 1v10a1 1 0 0 1-1 1H9a1 1 0 0 1-1-1V10a1 1 0 0 1 1-1z", "M5 15H4a1 1 0 0 1-1-1V4a1 1 0 0 1 1-1h10a1 1 0 0 1 1 1v1"],
  close: ["M6 6l12 12", "M18 6L6 18"],
  server: ["M4 5h16v6H4z", "M4 13h16v6H4z", "M8 8h.01", "M8 16h.01"],
  settings: ["M12 15a3 3 0 1 0 0-6 3 3 0 0 0 0 6z", "M19.4 15a1.7 1.7 0 0 0 .3 1.8l.1.1a2 2 0 1 1-2.8 2.8l-.1-.1a1.7 1.7 0 0 0-1.8-.3 1.7 1.7 0 0 0-1 1.5V21a2 2 0 1 1-4 0v-.1a1.7 1.7 0 0 0-1.1-1.5 1.7 1.7 0 0 0-1.8.3l-.1.1a2 2 0 1 1-2.8-2.8l.1-.1a1.7 1.7 0 0 0 .3-1.8 1.7 1.7 0 0 0-1.5-1H3a2 2 0 1 1 0-4h.1a1.7 1.7 0 0 0 1.5-1.1 1.7 1.7 0 0 0-.3-1.8l-.1-.1a2 2 0 1 1 2.8-2.8l.1.1a1.7 1.7 0 0 0 1.8.3H9a1.7 1.7 0 0 0 1-1.5V3a2 2 0 1 1 4 0v.1a1.7 1.7 0 0 0 1 1.5 1.7 1.7 0 0 0 1.8-.3l.1-.1a2 2 0 1 1 2.8 2.8l-.1.1a1.7 1.7 0 0 0-.3 1.8V9a1.7 1.7 0 0 0 1.5 1H21a2 2 0 1 1 0 4h-.1a1.7 1.7 0 0 0-1.5 1z"],
  key: ["M15.5 7.5a3 3 0 1 1-6 0 3 3 0 0 1 6 0z", "M12.5 10.5L5 18v2h3v-2h2v-2h2l1.2-1.2"],
  lock: ["M6 11h12v9H6z", "M8.5 11V8a3.5 3.5 0 0 1 7 0v3"],
  logout: ["M15 4h3a2 2 0 0 1 2 2v12a2 2 0 0 1-2 2h-3", "M10 17l5-5-5-5", "M15 12H4"],
  check: ["M5 12.5l4.5 4.5L19 7.5"],
  minus: ["M6 12h12"],
  chevron: ["M9 6l6 6-6 6"],
  alert: ["M12 9v4", "M12 17h.01", "M10.3 3.9L2.4 17.5A2 2 0 0 0 4.1 20.5h15.8a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0z"],
};

export function icon(name, cls = "icon") {
  const ns = "http://www.w3.org/2000/svg";
  const svg = document.createElementNS(ns, "svg");
  svg.setAttribute("viewBox", "0 0 24 24");
  svg.setAttribute("class", cls);
  svg.setAttribute("aria-hidden", "true");
  for (const d of ICONS[name] ?? []) {
    const p = document.createElementNS(ns, "path");
    p.setAttribute("d", d);
    svg.append(p);
  }
  return svg;
}

const UNITS = ["B", "KB", "MB", "GB", "TB", "PB"];

export function formatBytes(n) {
  if (!Number.isFinite(n)) return "";
  let i = 0;
  while (n >= 1024 && i < UNITS.length - 1) {
    n /= 1024;
    i++;
  }
  const digits = i === 0 || n >= 100 ? 0 : 1;
  return `${n.toFixed(digits)} ${UNITS[i]}`;
}

const dateFormat = new Intl.DateTimeFormat(undefined, {
  year: "numeric",
  month: "short",
  day: "numeric",
  hour: "2-digit",
  minute: "2-digit",
});

export function formatDate(d) {
  return d instanceof Date && !Number.isNaN(d.getTime()) ? dateFormat.format(d) : "";
}

export function formatUptime(secs) {
  const d = Math.floor(secs / 86400);
  const hrs = Math.floor((secs % 86400) / 3600);
  const m = Math.floor((secs % 3600) / 60);
  if (d) return `${d}d ${hrs}h`;
  if (hrs) return `${hrs}h ${m}m`;
  return `${m}m`;
}

// Toasts: errors stay until dismissed, since their request id is what a bug report needs.
export function toast(message, { kind = "info", detail } = {}) {
  let stack = document.querySelector(".toasts");
  if (!stack) {
    stack = h("div", { class: "toasts", role: "status", "aria-live": "polite" });
    document.body.append(stack);
  }
  const el = h(
    "div",
    { class: `toast toast-${kind}` },
    kind === "error" ? icon("alert") : icon("check"),
    h("div", { class: "toast-body" }, h("div", {}, message), detail && h("div", { class: "toast-detail" }, detail)),
    h("button", { class: "icon-button", "aria-label": "Dismiss", onclick: () => el.remove() }, icon("close")),
  );
  stack.append(el);
  if (kind !== "error") setTimeout(() => el.remove(), 4000);
}

// A modal dialog on <dialog>, which traps focus and closes on Escape by itself. Resolves to the
// value of the button pressed, or null when dismissed.
export function dialog({ title, body, actions, onOpen }) {
  return new Promise((resolve) => {
    const el = h(
      "dialog",
      { class: "dialog" },
      h("form", { method: "dialog" },
        h("h2", {}, title),
        h("div", { class: "dialog-body" }, body),
        h("div", { class: "dialog-actions" },
          actions.map((a) =>
            h("button", { class: `button ${a.kind ?? ""}`, value: a.value, autofocus: a.autofocus, formNoValidate: !a.value }, a.label),
          ),
        ),
      ),
    );
    el.addEventListener("close", () => {
      el.remove();
      resolve(el.returnValue || null);
    });
    document.body.append(el);
    el.showModal();
    onOpen?.(el);
  });
}

export async function confirm({ title, message, action, danger = true }) {
  const value = await dialog({
    title,
    body: h("p", {}, message),
    actions: [
      { label: "Cancel", value: "", kind: "ghost" },
      { label: action, value: "ok", kind: danger ? "danger" : "primary", autofocus: true },
    ],
  });
  return value === "ok";
}

export async function copyText(text, what = "Copied") {
  try {
    await navigator.clipboard.writeText(text);
    toast(what);
  } catch {
    toast("The browser refused clipboard access", { kind: "error" });
  }
}

export function describeError(err) {
  if (err?.name === "AbortError") return null;
  const friendly = {
    InvalidAccessKeyId: "This access key is not configured on the server.",
    SignatureDoesNotMatch: "The secret key does not match the access key.",
    AccessDenied: "Access denied.",
    NoSuchBucket: "The bucket does not exist.",
    NoSuchKey: "The object does not exist.",
    BucketNotEmpty: "The bucket is not empty. Delete its objects first.",
    BucketAlreadyOwnedByYou: "A bucket with this name already exists.",
    BucketAlreadyExists: "A bucket with this name already exists.",
    InvalidBucketName:
      "Bucket names have 3–63 lowercase letters, digits, dots and hyphens, and start and end with a letter or digit.",
    NetworkError: "The server could not be reached.",
  }[err?.code];
  const message = friendly ?? err?.message ?? String(err);
  const detail = [err?.code, err?.requestId && `request ${err.requestId}`].filter(Boolean).join(" · ");
  return { message, detail };
}

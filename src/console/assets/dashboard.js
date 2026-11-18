// The Dashboard page: the server's last hour, from the samples it keeps every 10 seconds
// (GET /_s3dal/admin/stats). Charts are drawn here as SVG. Their colors are CSS classes rather
// than attributes: the page's policy allows no inline style, and the palette has a light and a
// dark step for every series.

import { formatBytes, h, meter, percent, quotaState } from "./ui.js";

const REFRESH_MS = 10_000;
const NS = "http://www.w3.org/2000/svg";
const HEIGHT = 200;
const MARGIN = { top: 12, right: 14, bottom: 24, left: 56 };

// The requests chart folds the operations into families, in the palette's order; anything else
// is Other, in gray. Five lines stay legible where forty would not; the table lists every one.
const FAMILIES = [
  ["Reads", ["GetObject", "HeadObject", "GetObjectAttributes", "GetObjectTagging", "GetObjectAcl"]],
  ["Writes", ["PutObject", "PostObject", "CopyObject", "UploadPart", "UploadPartCopy", "CreateMultipartUpload", "CompleteMultipartUpload"]],
  ["Lists", ["ListBuckets", "ListObjects", "ListObjectsV2", "ListObjectVersions", "ListParts", "ListMultipartUploads"]],
  ["Deletes", ["DeleteObject", "DeleteObjects", "DeleteBucket", "AbortMultipartUpload"]],
];
const OTHER = "Other";
const family = (op) => FAMILIES.find(([, ops]) => ops.includes(op))?.[0] ?? OTHER;
const seriesClass = (i, name) => (name === OTHER ? "s-other" : `s${i + 1}`);

// ----- Formats -----

const decimals = (v) => (v !== 0 && Math.abs(v) < 10 ? 1 : 0);
const num = (v, d = decimals(v)) => v.toLocaleString(undefined, { minimumFractionDigits: d, maximumFractionDigits: d });
const FORMATS = {
  rate: (v) => `${num(v)}/s`,
  percent: (v) => `${num(v)}%`,
  ms: (v) => (v >= 1000 ? `${num(v / 1000)} s` : `${num(v)} ms`),
  bytesRate: (v) => `${formatBytes(v)}/s`,
  bytes: (v) => formatBytes(v),
};
const show = (format, v) => (v === null || v === undefined || !Number.isFinite(v) ? "—" : FORMATS[format](v));

const clock = (t, seconds) =>
  new Date(t * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: seconds ? "2-digit" : undefined });

// Axis ticks on round steps: 1, 2 or 5 of a power of ten, of the largest binary unit for bytes,
// so an axis reads 0 / 512 KB / 1.0 MB rather than 0 / 476.8 KB.
function scale(max, binary) {
  const top = max > 0 ? max : 1;
  const unit = binary ? 1024 ** Math.max(0, Math.floor(Math.log(top) / Math.log(1024))) : 1;
  const rough = top / unit / 4;
  const exp = 10 ** Math.floor(Math.log10(rough));
  const step = [1, 2, 5, 10].map((m) => m * exp).find((s) => s >= rough) * unit;
  const end = Math.ceil(top / step - 1e-9) * step;
  const ticks = Array.from({ length: Math.round(end / step) + 1 }, (_, i) => i * step);
  const tick = (v) => {
    if (binary) return formatBytes(v);
    const d = Math.max(0, -Math.floor(Math.log10(step) + 1e-9));
    return num(v, d);
  };
  return { end, ticks, tick };
}

function svg(tag, attrs = {}) {
  const el = document.createElementNS(NS, tag);
  for (const [k, v] of Object.entries(attrs)) el.setAttribute(k, v);
  return el;
}

// ----- Data -----

// The samples as the charts plot them: per second, per request, at the end of each interval.
// The interval in progress joins once it is a second long.
function derive(stats) {
  const points = stats.samples.filter((s) => s.secs >= 1);
  const t = points.map((s) => s.t);
  const total = (s) => Object.values(s.status).reduce((a, b) => a + b, 0);
  const share = (s, cls) => (total(s) ? (100 * s.status[cls]) / total(s) : null);
  const familyRate = (name) => points.map((s) =>
    Object.entries(s.requests).filter(([op]) => family(op) === name).reduce((a, [, n]) => a + n, 0) / s.secs);
  const names = [...FAMILIES.map(([name]) => name), OTHER];
  return {
    points,
    t,
    requests: names.map((name, i) => ({ name, cls: seriesClass(i, name), values: familyRate(name) })),
    errors: [
      { name: "4xx, client errors", cls: "s1", values: points.map((s) => share(s, "4xx")) },
      { name: "5xx, server errors", cls: "s2", values: points.map((s) => share(s, "5xx")) },
    ],
    latency: [
      { name: "p50", cls: "s1", values: points.map((s) => s.p50_ms) },
      { name: "p99", cls: "s2", values: points.map((s) => s.p99_ms) },
    ],
    throughput: [
      { name: "Received", cls: "s1", values: points.map((s) => s.bytes_in / s.secs) },
      { name: "Sent", cls: "s2", values: points.map((s) => s.bytes_out / s.secs) },
    ],
    memory: [{ name: "Resident memory", cls: "s1", values: points.map((s) => s.rss_bytes) }],
  };
}

// The figures of the last minute.
function headline(points) {
  const end = points.at(-1)?.t ?? 0;
  const recent = points.filter((s) => s.t > end - 60);
  const secs = recent.reduce((a, s) => a + s.secs, 0);
  const sum = (f) => recent.reduce((a, s) => a + f(s), 0);
  const requests = sum((s) => Object.values(s.requests).reduce((a, b) => a + b, 0));
  const answered = sum((s) => Object.values(s.status).reduce((a, b) => a + b, 0));
  const p = (key) => {
    const values = recent.map((s) => s[key]).filter((v) => v !== null);
    return values.length ? Math.max(...values) : null;
  };
  return {
    rate: secs ? requests / secs : null,
    errors5: answered ? (100 * sum((s) => s.status["5xx"])) / answered : null,
    errors4: answered ? (100 * sum((s) => s.status["4xx"])) / answered : null,
    p99: p("p99_ms"),
    p50: p("p50_ms"),
    in: secs ? sum((s) => s.bytes_in) / secs : null,
    out: secs ? sum((s) => s.bytes_out) / secs : null,
    rss: points.at(-1)?.rss_bytes ?? null,
  };
}

// ----- Chart -----

// A line chart over time with a crosshair: pointer or arrow keys pick an interval, and the
// tooltip lists every series at it. `update` redraws it in place, so a refresh keeps the
// interval the reader is looking at.
function lineChart({ label, format, binary = false, area = false }) {
  const plot = svg("svg", { role: "img", tabindex: "0", "aria-label": `${label}. Arrow keys move through the hour.` });
  const tip = h("div", { class: "chart-tooltip", hidden: true });
  const el = h("div", { class: "chart" }, plot, tip);
  let width = 0;
  let data = null;
  let focus = null; // the time of the interval picked, which survives a refresh

  const draw = () => {
    if (!data || !width) return;
    const { t, series } = data;
    plot.replaceChildren();
    plot.setAttribute("viewBox", `0 0 ${width} ${HEIGHT}`);
    plot.setAttribute("width", width);
    plot.setAttribute("height", HEIGHT);
    const right = width - MARGIN.right;
    const bottom = HEIGHT - MARGIN.bottom;
    // An hour at most; at least five minutes, so a server just started is not stretched across.
    const t1 = t.at(-1) ?? Date.now() / 1000;
    const t0 = Math.min(Math.max(t[0] - 10, t1 - 3600), t1 - 300);
    const x = (v) => MARGIN.left + ((v - t0) / (t1 - t0)) * (right - MARGIN.left);
    const values = series.flatMap((s) => s.values).filter((v) => v !== null && Number.isFinite(v));
    const ys = scale(Math.max(0, ...values), binary);
    const y = (v) => bottom - (v / ys.end) * (bottom - MARGIN.top);

    for (const v of ys.ticks) {
      plot.append(svg("line", { class: v === 0 ? "chart-axis" : "chart-grid", x1: MARGIN.left, x2: right, y1: y(v), y2: y(v) }));
      const text = svg("text", { class: "chart-tick", x: MARGIN.left - 8, y: y(v), "text-anchor": "end", "dominant-baseline": "middle" });
      text.textContent = ys.tick(v);
      plot.append(text);
    }
    const span = t1 - t0;
    const every = span > 1800 ? 600 : span > 600 ? 300 : 60;
    for (let v = Math.ceil(t0 / every) * every; v <= t1; v += every) {
      const text = svg("text", { class: "chart-tick", x: x(v), y: HEIGHT - 6, "text-anchor": "middle" });
      text.textContent = clock(v, false);
      plot.append(text);
    }

    for (const s of series) {
      let d = "";
      let open = false;
      s.values.forEach((v, i) => {
        if (v === null || !Number.isFinite(v)) {
          open = false;
          return;
        }
        d += `${open ? "L" : "M"}${x(t[i]).toFixed(1)},${y(v).toFixed(1)}`;
        open = true;
      });
      if (!d) continue;
      if (area && series.length === 1) {
        const first = s.values.findIndex((v) => v !== null);
        const last = s.values.findLastIndex((v) => v !== null);
        plot.append(svg("path", { class: `chart-area ${s.cls}`, d: `${d}L${x(t[last]).toFixed(1)},${bottom}L${x(t[first]).toFixed(1)},${bottom}Z` }));
      }
      plot.append(svg("path", { class: `chart-line ${s.cls}`, d }));
    }
    // End markers, so the latest value is found at a glance.
    for (const s of series) {
      const last = s.values.findLastIndex((v) => v !== null && Number.isFinite(v));
      if (last >= 0) plot.append(svg("circle", { class: `chart-dot ${s.cls}`, cx: x(t[last]), cy: y(s.values[last]), r: 4 }));
    }

    const i = focus === null ? -1 : t.findIndex((v) => v === focus);
    if (i < 0) {
      tip.hidden = true;
      return;
    }
    const cx = x(t[i]);
    plot.append(svg("line", { class: "chart-cursor", x1: cx, x2: cx, y1: MARGIN.top, y2: bottom }));
    for (const s of series) {
      const v = s.values[i];
      if (v !== null && Number.isFinite(v)) plot.append(svg("circle", { class: `chart-dot ${s.cls}`, cx, cy: y(v), r: 4 }));
    }
    tip.replaceChildren(
      h("div", { class: "tooltip-time" }, clock(t[i], true)),
      ...series.map((s) => h("div", { class: "tooltip-row" }, h("span", { class: `swatch ${s.cls}` }),
        h("strong", {}, show(format, s.values[i])), h("span", { class: "muted" }, s.name))),
    );
    tip.hidden = false;
    // Beside the cursor, on whichever side has room.
    const flip = cx > width / 2;
    tip.style.left = flip ? "" : `${cx + 12}px`;
    tip.style.right = flip ? `${width - cx + 12}px` : "";
  };

  const pick = (clientX) => {
    if (!data?.t.length) return;
    const box = plot.getBoundingClientRect();
    const px = clientX - box.left;
    const { t } = data;
    const t1 = t.at(-1);
    const t0 = Math.min(Math.max(t[0] - 10, t1 - 3600), t1 - 300);
    const at = t0 + ((px - MARGIN.left) / (width - MARGIN.right - MARGIN.left)) * (t1 - t0);
    let best = 0;
    t.forEach((v, i) => {
      if (Math.abs(v - at) < Math.abs(t[best] - at)) best = i;
    });
    focus = t[best];
    draw();
  };
  plot.addEventListener("pointermove", (e) => pick(e.clientX));
  plot.addEventListener("pointerleave", () => {
    if (document.activeElement !== plot) {
      focus = null;
      draw();
    }
  });
  plot.addEventListener("blur", () => {
    focus = null;
    draw();
  });
  plot.addEventListener("keydown", (e) => {
    if (!data?.t.length) return;
    const { t } = data;
    const i = focus === null ? t.length : t.indexOf(focus);
    if (e.key === "ArrowLeft") focus = t[Math.max(0, i - 1)];
    else if (e.key === "ArrowRight") focus = t[Math.min(t.length - 1, i < 0 ? 0 : i + 1)];
    else if (e.key === "Home") focus = t[0];
    else if (e.key === "End") focus = t.at(-1);
    else if (e.key === "Escape") focus = null;
    else return;
    e.preventDefault();
    draw();
  });
  new ResizeObserver(([entry]) => {
    const w = Math.round(entry.contentRect.width);
    if (w && w !== width) {
      width = w;
      draw();
    }
  }).observe(el);

  return {
    el,
    update(t, series) {
      data = { t, series };
      draw();
    },
  };
}

// The same numbers as a table, a minute to a row, newest first: what the chart shows, for
// anyone who reads values rather than lines, and for the light colors that are hard to see.
function minuteTable(t, series, format, rollup) {
  const rows = new Map();
  t.forEach((v, i) => {
    const minute = Math.floor((v - 1) / 60) * 60;
    if (!rows.has(minute)) rows.set(minute, series.map(() => []));
    series.forEach((s, j) => {
      const x = s.values[i];
      if (x !== null && Number.isFinite(x)) rows.get(minute)[j].push(x);
    });
  });
  const combine = (xs) => (!xs.length ? null : rollup === "max" ? Math.max(...xs) : xs.reduce((a, b) => a + b, 0) / xs.length);
  return h("table", { class: "table" },
    h("thead", {}, h("tr", {}, h("th", {}, "Minute"), series.map((s) => h("th", { class: "num-col" }, s.name)))),
    h("tbody", {}, [...rows.entries()].reverse().map(([minute, cols]) =>
      h("tr", {}, h("td", { class: "num" }, clock(minute, false)),
        cols.map((xs) => h("td", { class: "num num-col" }, show(format, combine(xs))))))));
}

// The operations behind the requests chart, each with its count over the hour.
function operationsTable(points) {
  const totals = new Map();
  for (const s of points) for (const [op, n] of Object.entries(s.requests)) totals.set(op, (totals.get(op) ?? 0) + n);
  const secs = points.reduce((a, s) => a + s.secs, 0);
  const names = [...FAMILIES.map(([name]) => name), OTHER];
  const rows = [...totals.entries()].sort((a, b) => b[1] - a[1]);
  return h("table", { class: "table ops-table" },
    h("thead", {}, h("tr", {}, h("th", {}, "Operation"), h("th", {}, "Family"), h("th", { class: "num-col" }, "Requests"), h("th", { class: "num-col" }, "Average"))),
    h("tbody", {}, rows.length
      ? rows.map(([op, n]) => {
        const name = family(op);
        return h("tr", {},
          h("td", { class: "mono" }, op),
          h("td", {}, h("span", { class: "legend-item" }, h("span", { class: `swatch ${seriesClass(names.indexOf(name), name)}` }), name)),
          h("td", { class: "num num-col" }, n.toLocaleString()),
          h("td", { class: "num num-col" }, show("rate", secs ? n / secs : null)));
      })
      : h("tr", {}, h("td", { class: "muted", colspan: 4 }, "No requests in the last hour."))));
}

function legend(series) {
  if (series.length < 2) return null;
  return h("div", { class: "chart-legend" },
    series.map((s) => h("span", { class: "legend-item" }, h("span", { class: `swatch ${s.cls}` }), s.name)));
}

// A chart in its panel, with a switch to its table.
function chartPanel({ title, note, format, binary, area, rollup = "mean", table }) {
  const chart = lineChart({ label: title, format, binary, area });
  const tableBox = h("div", { class: "chart-table", hidden: true });
  const legendBox = h("div", {});
  const toggle = h("button", {
    class: "button ghost small", type: "button", "aria-pressed": "false",
    onclick: () => {
      const on = tableBox.hidden;
      tableBox.hidden = !on;
      chart.el.hidden = on;
      toggle.setAttribute("aria-pressed", String(on));
      toggle.textContent = on ? "Chart" : "Table";
    },
  }, "Table");
  const el = h("section", { class: "panel chart-panel" },
    h("header", { class: "panel-header row" },
      h("div", { class: "chart-heading" }, h("h2", {}, title), note && h("p", { class: "muted small" }, note), legendBox),
      toggle),
    chart.el, tableBox);
  return {
    el,
    update(t, series, points) {
      chart.update(t, series);
      legendBox.replaceChildren(legend(series) ?? "");
      tableBox.replaceChildren(table ? table(points) : minuteTable(t, series, format, rollup));
    },
  };
}

// ----- Bucket usage -----

const plural = (n, word) => `${n.toLocaleString()} ${word}${n === 1 ? "" : "s"}`;

// One row per bucket: its name, a meter per limit of its quota, and the figures. Buckets with a
// quota come first, fullest first; the rest by size. A meter list rather than one bar chart: each
// bucket is read against its own limit, not against the others.
function usageRow(b) {
  const { usage, quota } = b;
  const state = quotaState(usage, quota);
  // Bytes in cold tiers, as the last count found them; quota counts them like any others.
  const cold = usage.cold_bytes ?? 0;
  const meters = quota
    ? [
      meter(usage.bytes, quota.max_bytes, state.level, `${b.name}: bytes used of its quota`, cold),
      quota.max_objects != null && meter(usage.objects, quota.max_objects, state.level, `${b.name}: objects of its quota`),
    ]
    // Without a quota a bar says only how much of the bucket is cold.
    : cold > 0 ? [meter(usage.bytes, usage.bytes, "ok", `${b.name}: bytes, hot then cold`, cold)]
    : [h("span", { class: "muted small" }, "No quota")];
  const coldFigure = cold > 0 ? [" · ", h("span", { class: "cold-figure" }, `${formatBytes(cold)} cold`)] : [];
  const figure = quota
    ? [
      `${formatBytes(usage.bytes)} of ${formatBytes(quota.max_bytes)} (${percent(usage.bytes / Math.max(quota.max_bytes, 1))})`,
      " · ",
      quota.max_objects != null ? `${usage.objects.toLocaleString()} of ${plural(quota.max_objects, "object")}` : plural(usage.objects, "object"),
      ...coldFigure,
    ]
    : [formatBytes(usage.bytes), " · ", plural(usage.objects, "object"), ...coldFigure];
  const exact = `${usage.bytes.toLocaleString()} bytes${cold > 0 ? ` (${cold.toLocaleString()} in cold tiers)` : ""}, ${plural(usage.objects, "object")}` +
    (usage.scanned ? `; counted at ${clock(usage.scanned, true)}, plus this server's writes since` : "; not counted yet");
  return h("li", { class: "usage-row", title: exact },
    h("span", { class: "truncate" }, b.name),
    h("div", { class: "meters" }, meters),
    h("div", { class: "quota-figure" }, figure,
      state?.word && h("span", { class: `pill ${state.level === "over" ? "danger" : "warn"}` }, state.word)));
}

function usageTable(buckets) {
  const cell = (v) => h("td", { class: "num num-col" }, v == null ? "—" : v.toLocaleString());
  return h("table", { class: "table" },
    h("thead", {}, h("tr", {},
      h("th", {}, "Bucket"),
      h("th", { class: "num num-col" }, "Bytes"),
      h("th", { class: "num num-col" }, "Cold bytes"),
      h("th", { class: "num num-col" }, "Quota bytes"),
      h("th", { class: "num num-col" }, "Objects"),
      h("th", { class: "num num-col" }, "Quota objects"))),
    h("tbody", {}, buckets.map((b) => h("tr", {},
      h("td", { class: "mono" }, b.name),
      cell(b.usage.bytes), cell(b.usage.cold_bytes), cell(b.quota?.max_bytes), cell(b.usage.objects), cell(b.quota?.max_objects)))));
}

function usagePanel() {
  const list = h("ul", { class: "usage-list" });
  const tableBox = h("div", { class: "chart-table", hidden: true });
  const note = h("p", { class: "muted small" });
  const toggle = h("button", {
    class: "button ghost small", type: "button", "aria-pressed": "false",
    onclick: () => {
      const on = tableBox.hidden;
      tableBox.hidden = !on;
      list.hidden = on;
      toggle.setAttribute("aria-pressed", String(on));
      toggle.textContent = on ? "Bars" : "Table";
    },
  }, "Table");
  const el = h("section", { class: "panel" },
    h("header", { class: "panel-header row" },
      h("div", { class: "chart-heading" }, h("h2", {}, "Bucket usage"), note),
      toggle),
    list, tableBox);
  return {
    el,
    update({ buckets, scan_interval_secs: every }) {
      const fullness = (b) => quotaState(b.usage, b.quota)?.ratio ?? -1;
      const sorted = [...buckets].sort((a, b) => fullness(b) - fullness(a) || b.usage.bytes - a.usage.bytes);
      note.textContent = "Every version counts. " + (every
        ? `Counted every ${every >= 60 && every % 60 === 0 ? `${every / 60} min` : `${every} s`}, plus this server's writes since.`
        : "Counting is off (usage-scan-interval = 0): only this server's writes since it started count.");
      list.replaceChildren(...(sorted.length ? sorted.map(usageRow) : [h("li", { class: "muted usage-row" }, "No buckets.")]));
      tableBox.replaceChildren(usageTable(sorted));
    },
  };
}

function tile(label) {
  const value = h("div", { class: "stat-value" }, "—");
  const hint = h("div", { class: "muted small" });
  return { el: h("div", { class: "stat" }, h("div", { class: "stat-label" }, label), value, hint), value, hint };
}

// ----- Page -----

// The page's content, the status line for its header, and `stop`, which ends the refreshes
// when the page is left.
export function dashboard(fetchStats, fetchUsage, { errorState, loadingRows }) {
  const status = h("span", { class: "muted small dash-status", role: "status" });
  const body = h("div", { class: "stack" }, loadingRows(6));
  const tiles = {
    rate: tile("Requests"),
    errors: tile("Server errors"),
    p99: tile("p99 latency"),
    throughput: tile("Throughput"),
    rss: tile("Memory"),
  };
  const panels = {
    requests: chartPanel({ title: "Requests per second", note: "By kind of operation; the table lists each one.", format: "rate", table: operationsTable }),
    errors: chartPanel({ title: "Error rate", note: "Share of the requests answered with an error.", format: "percent" }),
    latency: chartPanel({ title: "Latency", note: "From a request's headers to the end of its response.", format: "ms", rollup: "max" }),
    throughput: chartPanel({ title: "Throughput", note: "Request and response bodies.", format: "bytesRate", binary: true }),
    memory: chartPanel({ title: "Resident memory", format: "bytes", binary: true, area: true, rollup: "max" }),
  };
  const usage = usagePanel();
  const content = h("div", { class: "stack" },
    h("section", { class: "stats kpis" }, Object.values(tiles).map((t) => t.el)),
    h("div", { class: "charts" }, Object.values(panels).map((p) => p.el)),
    usage.el);
  let shown = false;
  let busy = false;

  const paint = (stats) => {
    const d = derive(stats);
    const k = headline(d.points);
    tiles.rate.value.textContent = show("rate", k.rate);
    tiles.rate.hint.textContent = "Last minute";
    tiles.errors.value.textContent = show("percent", k.errors5);
    tiles.errors.hint.textContent = `5xx; 4xx ${show("percent", k.errors4)}`;
    tiles.p99.value.textContent = show("ms", k.p99);
    tiles.p99.hint.textContent = `Slowest 10 s of the last minute; p50 ${show("ms", k.p50)}`;
    tiles.throughput.value.textContent = show("bytesRate", k.in === null ? null : k.in + k.out);
    tiles.throughput.hint.textContent = `${show("bytesRate", k.in)} in, ${show("bytesRate", k.out)} out`;
    tiles.rss.value.textContent = show("bytes", k.rss);
    tiles.rss.hint.textContent = "Resident set of the process";
    for (const [name, panel] of Object.entries(panels)) panel.update(d.t, d[name], d.points);
    status.textContent = `Last hour, every 10 seconds · updated ${clock(stats.now, true)}`;
    status.classList.remove("warn-text");
  };

  const load = async () => {
    if (busy) return;
    busy = true;
    content.classList.add("is-refreshing");
    try {
      const [stats, buckets] = await Promise.all([fetchStats(), fetchUsage()]);
      if (!shown) {
        body.replaceChildren(content);
        shown = true;
      }
      paint(stats);
      usage.update(buckets);
    } catch (err) {
      // The last picture stays; the status says it is not current.
      if (shown) {
        status.textContent = `Not updated: ${err?.message ?? err}`;
        status.classList.add("warn-text");
      } else {
        body.replaceChildren(errorState(err, load));
      }
    } finally {
      busy = false;
      content.classList.remove("is-refreshing");
    }
  };

  load();
  const timer = setInterval(() => {
    if (document.visibilityState === "visible") load();
  }, REFRESH_MS);
  return { el: body, status, stop: () => clearInterval(timer) };
}

// Data Medallion UI: upload a CSV and follow it through every checkpoint of the pipeline.
// No build step: plain ES module. All values are escaped by the h`` template tag.

// --- tiny helpers ---------------------------------------------------------------------
class Raw { constructor(s) { this.s = s; } toString() { return this.s; } }
const raw = (s) => new Raw(s);
const ESC = { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" };
const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ESC[c]);
const val = (v) => (v == null || v === false ? "" : v instanceof Raw ? v.s : Array.isArray(v) ? v.map(val).join("") : esc(v));
const h = (strings, ...vals) => raw(strings.reduce((out, s, i) => out + s + (i < vals.length ? val(vals[i]) : ""), ""));
const $ = (sel, root = document) => root.querySelector(sel);

const num = (x) => (x == null ? "–" : Number(x).toLocaleString());
const pct = (x, d = 1) => (x == null ? "–" : `${(x * 100).toFixed(d)}%`);
const money = (x) => (x == null ? "–" : Number(x) > 0 && Number(x) < 0.01 ? "<$0.01" : `$${Number(x).toFixed(2)}`);
const dur = (ms) => (ms == null ? "–" : ms < 1000 ? `${ms} ms` : ms < 120000 ? `${(ms / 1000).toFixed(1)} s` : `${Math.round(ms / 60000)} min`);
const when = (iso) => (iso ? new Date(iso.replace(" ", "T")).toLocaleString() : "–");
const short = (id) => String(id).slice(0, 8);
const plural = (n, word) => `${num(n)} ${word}${n === 1 ? "" : "s"}`;

async function api(path, opts = {}) {
  const res = await fetch(`/api${path}`, {
    headers: opts.body && !(opts.body instanceof FormData) ? { "Content-Type": "application/json" } : {},
    ...opts,
    body: opts.body && !(opts.body instanceof FormData) ? JSON.stringify(opts.body) : opts.body,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(typeof data.detail === "string" ? data.detail : `${res.status} ${res.statusText}`);
    err.status = res.status;
    throw err;
  }
  return data;
}

function toast(message, bad = false) {
  const el = document.createElement("div");
  el.className = `toast${bad ? " bad" : ""}`;
  el.textContent = message;
  document.body.append(el);
  setTimeout(() => el.remove(), 4500);
}

// A raw cell value: shows empty / missing and highlights spaces at the edges.
function rv(v) {
  if (v === null || v === undefined) return h`<span class="empty-v">missing</span>`;
  const s = String(v);
  if (s === "") return h`<span class="empty-v">empty</span>`;
  if (!s.trim()) return h`<span class="ws" title="only spaces">${"␣".repeat(Math.min(s.length, 6))}</span>`;
  const m = s.match(/^(\s*)(.*?)(\s*)$/s);
  const shown = m[2].length > 80 ? m[2].slice(0, 80) + "…" : m[2];
  return h`<span title="${s}">${m[1] ? h`<span class="ws" title="leading spaces">${"␣".repeat(m[1].length)}</span>` : ""}${shown}${m[3] ? h`<span class="ws" title="trailing spaces">${"␣".repeat(m[3].length)}</span>` : ""}</span>`;
}

const kpi = (value, label, tone = "") => h`<div class="kpi ${tone}"><div class="v">${value}${tone === "unreliable" ? h`<span class="badge-unreliable" title="A data-quality finding says this number is not meaningful for this file">unreliable</span>` : ""}</div><div class="l">${label}</div></div>`;
const bar = (parts) => h`<div class="bar">${parts.filter((p) => p[0] > 0).map(([share, tone, title]) => h`<span class="${tone}" style="width:${Math.max(0, Math.min(100, share * 100)).toFixed(2)}%" title="${title || ""}"></span>`)}</div>`;
const loading = (text = "Loading…") => h`<div class="loading"><span class="spinner"></span>${text}</div>`;
const sevPill = (s) => h`<span class="pill ${s}">${s}</span>`;
const byPill = (g) => ({ agent: h`<span class="pill agent">AI</span>`, human: h`<span class="pill human">person</span>`, default: h`<span class="pill">default</span>` }[g] || "");

// --- copy: what each checkpoint means, in business terms --------------------------------
const STEP_COPY = {
  upload: ["Your file arrived. We fingerprint its exact bytes (SHA-256), so uploading the same file again never creates a second copy.",
    "This is the starting point of the audit trail: every later value can be traced back to a line of this file."],
  bronze: ["Bronze is an exact, untouched copy of every record in your file. Nothing is trimmed, fixed or removed. Even broken rows are kept and labelled.",
    "Because nothing is lost here, any cleaning decision can be reviewed or undone later, and every number can be traced back to what you sent."],
  profile: ["Before changing anything, the pipeline measures every column: how much is missing, which formats appear, which values are placeholders, and which spellings differ.",
    "These are facts computed by code over every row, not AI guesses. They are the evidence the cleaning rules have to answer to."],
  context: ["The AI works out what the data is for and which columns matter most, from the column names, types and a few masked sample rows. The AI reviewer uses this to decide which problems hurt the business.",
    "Nothing to do here: the AI decides it on its own. If you ever want to correct it, a description you write replaces the AI's."],
  card: ["The AI reviewer never reads your whole file. It gets this compact briefing built from the health check, and then asks targeted questions.",
    "This keeps the AI cheap and repeatable, and columns about people (names, contacts) are shown to it only as shapes, never as values."],
  agent: ["An AI reviewer studies the briefing, runs checks against the data, and proposes one cleaning rule per column, each with a business reason.",
    "It can only propose. Every proposal is checked automatically: rules must be valid, are dry-run on 2,000 sample rows, and must cover every high and medium issue."],
  approval: ["Nothing changes in your data until a person has reviewed every rule. Each rule shows what it does, why, which issues it fixes, its effect on your rows, and every setting it has.",
    "Approve or reject each rule on its own. When all are decided, finish the review to use them for the cleaned data."],
  silver: ["Silver is your data after the approved rules are applied and stored as a table: one date format, placeholders emptied, spellings merged, numbers made numeric. It is built automatically when you finish the review.",
    "Every value keeps a link to its original, and every fix is labelled, so you can filter on what was changed instead of trusting it blindly."],
  gold: ["Gold turns the cleaned tickets into business tables: resolution time and SLA, volume and backlog, and repair cost. It is built automatically after the cleaned table.",
    "Every number shows how much of the data it is based on, and the findings at the top say what limits the numbers and what to do about it."],
};

// --- routing ------------------------------------------------------------------------------
const app = $("#app");
let state = { runId: null, step: null, ov: null, poll: null, sig: "" };

window.addEventListener("hashchange", route);
document.addEventListener("click", onClick);
document.addEventListener("change", onChange);
document.addEventListener("keydown", (e) => { if (e.key === "Escape") closeDrawer(); });
api("/health").then((hl) => { $("#mode").textContent = `AI mode: ${hl.llm_mode}`; }).catch(() => {});
route();

function route() {
  const parts = location.hash.replace(/^#\/?/, "").split("/");
  if (parts[0] === "run" && parts[1]) {
    const [runId, step] = [parts[1], parts[2] || null];
    if (state.runId === runId && state.ov) {
      state.step = step || pickStep(state.ov);
      renderStepper();
      loadPanel();
    } else {
      openRun(runId, step);
    }
  } else {
    stopPoll();
    state = { runId: null, step: null, ov: null, poll: null, sig: "" };
    renderHome();
  }
}

// --- home ---------------------------------------------------------------------------------
async function renderHome() {
  app.innerHTML = h`
    <section class="hero">
      <div class="stack">
        <h1>See exactly what happens to your data</h1>
        <p class="lead">Upload a CSV. You can follow every record from the original file, through the raw copy and the health check, to the cleaning rules an AI proposes and a person approves, down to the cleaned result, value by value.</p>
        <div class="journey">${["File", "Bronze: raw copy", "Health check", "Business context", "AI review", "Your approval", "Silver: cleaned", "Gold: business tables"].map((j, i, a) => h`<span class="j">${j}</span>${i < a.length - 1 ? h`<span class="arrow">→</span>` : ""}`)}</div>
      </div>
      <label class="drop" id="drop">
        <input type="file" id="file" accept=".csv,.tsv,.txt" hidden>
        <div class="big">Drop a CSV file here</div>
        <div class="muted">or click to choose one · any columns, any size up to 200 MB</div>
        <div id="upload-status" class="small muted"></div>
      </label>
    </section>
    <section class="stack" style="margin-top:28px">
      <h2>Uploaded files</h2>
      <div id="runs">${loading()}</div>
    </section>`.s;

  const drop = $("#drop");
  $("#file").addEventListener("change", (e) => e.target.files[0] && upload(e.target.files[0]));
  drop.addEventListener("dragover", (e) => { e.preventDefault(); drop.classList.add("over"); });
  drop.addEventListener("dragleave", () => drop.classList.remove("over"));
  drop.addEventListener("drop", (e) => { e.preventDefault(); drop.classList.remove("over"); e.dataTransfer.files[0] && upload(e.dataTransfer.files[0]); });

  try {
    const runs = await api("/runs");
    $("#runs").innerHTML = runs.length ? h`
      <div class="table-wrap"><table>
        <thead><tr><th>File</th><th>Uploaded</th><th class="num">Records</th><th class="num">Issues</th><th>Where it is now</th><th></th></tr></thead>
        <tbody>${runs.map((r) => h`
          <tr class="clickable" data-act="go" data-href="#/run/${r.run_id}">
            <td><b>${r.file}</b><div class="faint small mono">${short(r.run_id)}</div></td>
            <td class="nowrap">${when(r.started_at)}</td>
            <td class="num">${num(r.records_parsed)}</td>
            <td class="num">${r.profiled ? h`${num(r.issues)} ${r.high_issues ? h`<span class="pill high">${r.high_issues} high</span>` : ""}` : "–"}</td>
            <td>${runWhere(r)}</td>
            <td><a href="#/run/${r.run_id}">Open →</a></td>
          </tr>`)}</tbody></table></div>` : h`<div class="callout">No files yet. Upload one above to start.</div>`;
  } catch (e) {
    $("#runs").innerHTML = h`<div class="callout bad">Could not load runs: ${e.message}</div>`;
  }
}

function runWhere(r) {
  if (r.job && r.job.state === "running") return h`<span class="pill running">working: ${r.job.step}</span>`;
  if (r.status === "failed") return h`<span class="pill error">load failed</span>`;
  const s = { awaiting_approval: ["action", "waiting for your approval"], approved: ["ok", "rules approved"], rejected: ["error", "rules rejected"], failed: ["error", "AI review failed"] }[r.agent_status];
  if (s) return h`<span class="pill ${s[0]}">${s[1]}</span>`;
  return r.profiled ? h`<span class="pill">health checked</span>` : h`<span class="pill">raw data stored</span>`;
}

async function upload(file) {
  const status = $("#upload-status");
  status.innerHTML = h`<span class="row" style="justify-content:center"><span class="spinner"></span>Uploading ${file.name} and storing the raw copy…</span>`.s;
  const form = new FormData();
  form.append("file", file);
  try {
    const res = await api("/upload", { method: "POST", body: form });
    if (res.duplicate) toast("This exact file was uploaded before; showing that upload.");
    if (res.error) toast(`Loading failed: ${res.error}`, true);
    location.hash = `#/run/${res.run_id}`;
  } catch (e) {
    status.innerHTML = h`<span style="color:var(--bad)">${e.message}</span>`.s;
  }
}

// --- run page -------------------------------------------------------------------------------
async function openRun(runId, step) {
  stopPoll();
  state = { runId, step, ov: null, poll: null, sig: "" };
  app.innerHTML = loading("Loading this upload…").s;
  try {
    state.ov = await api(`/runs/${runId}`);
  } catch (e) {
    app.innerHTML = h`<div class="callout bad">${e.status === 404 ? "This upload does not exist." : e.message} <a href="#/">Back to uploads</a></div>`.s;
    return;
  }
  state.step = step || pickStep(state.ov);
  state.sig = signature(state.ov);
  const ov = state.ov;
  app.innerHTML = h`
    <div class="run-head">
      <div class="stack-sm">
        <a href="#/" class="small">← All uploads</a>
        <h1>${ov.file}</h1>
        <div class="muted small">Uploaded ${when(ov.started_at)} · run <span class="mono">${ov.run_id}</span></div>
      </div>
    </div>
    <div class="layout">
      <nav class="card stepper" id="stepper"></nav>
      <section class="stack">
        <div id="jobbar"></div>
        <div id="panel" class="card stack"></div>
      </section>
    </div>`.s;
  renderStepper();
  renderJobbar();
  loadPanel();
  ensurePoll();
}

function pickStep(ov) {
  const st = ov.stages;
  const find = (s) => st.find((x) => x.state === s);
  return (find("action") || find("running") || find("error") || st.find((x) => x.key === "silver" && x.state !== "waiting") ||
    [...st].reverse().find((x) => x.state === "done") || st[0]).key;
}

const ICON = { done: "✓", action: "!", error: "×", running: "", waiting: "", preview: "◐", later: "·" };

function renderStepper() {
  const el = $("#stepper");
  if (!el || !state.ov) return;
  el.innerHTML = state.ov.stages.map((s, i) => h`
    <div class="step ${s.state} ${s.key === state.step ? "sel" : ""}" data-act="step" data-step="${s.key}" title="${s.summary}">
      <div class="ic">${ICON[s.state] || i + 1}</div>
      <div><div class="t">${s.title}</div><div class="s">${s.summary}</div>${s.state === "action" ? h`<div class="needs">Needs you</div>` : ""}</div>
    </div>`).map(val).join("");
}

function renderJobbar() {
  const el = $("#jobbar");
  const job = state.ov && state.ov.job;
  if (!el) return;
  if (!job || (job.state === "done" && Date.now() - new Date(job.finished_at) > 30000)) { el.innerHTML = ""; return; }
  const events = job.events.slice(-6);
  const tone = job.state === "error" ? "bad" : job.state === "running" ? "info" : "ok";
  el.innerHTML = h`<div class="callout ${tone}"><div class="jobbar">
    ${job.state === "running" ? h`<span class="spinner"></span>` : h`<b>${job.state === "error" ? "×" : "✓"}</b>`}
    <div><b>${job.state === "running" ? "Working…" : job.state === "error" ? "Something went wrong" : "Done"}</b>
      ${job.error ? h`<div>${job.error}</div>` : ""}
      <ul class="events">${events.map((e) => h`<li class="${e.state}">${e.state === "running" ? "…" : e.state === "error" ? "!" : "✓"} ${e.message}</li>`)}</ul>
    </div></div></div>`.s;
}

function signature(ov) {
  return JSON.stringify([ov.stages.map((s) => [s.state, s.summary]), ov.job && [ov.job.state, ov.job.events.length]]);
}

const isBusy = (ov) => ov && ((ov.job && ov.job.state === "running") || ov.stages.some((s) => s.state === "running"));

function ensurePoll() {
  stopPoll();
  state.poll = setInterval(tick, isBusy(state.ov) ? 2000 : 15000);
}
function stopPoll() { if (state.poll) clearInterval(state.poll); state.poll = null; }

async function tick() {
  if (!state.runId) return;
  const wasBusy = isBusy(state.ov);
  try { state.ov = await api(`/runs/${state.runId}`); } catch { return; }
  const sig = signature(state.ov);
  const busy = isBusy(state.ov);
  if (sig !== state.sig) {
    state.sig = sig;
    renderStepper();
    renderJobbar();
    if (!busy || state.step === "agent") loadPanel(true);
  } else if (busy && state.step === "agent") {
    loadPanel(true); // live view of the agent's checks
  }
  if (wasBusy !== busy) ensurePoll();
}

async function refreshNow() {
  state.ov = await api(`/runs/${state.runId}`);
  state.sig = signature(state.ov);
  renderStepper();
  renderJobbar();
  ensurePoll();
}

async function loadPanel(quiet = false) {
  const el = $("#panel");
  if (!el) return;
  const stage = state.ov.stages.find((s) => s.key === state.step) || state.ov.stages[0];
  const [what, why] = STEP_COPY[stage.key];
  const n = state.ov.stages.indexOf(stage) + 1;
  const head = h`
    <div class="panel-head">
      <div class="stack-sm"><div class="eyebrow">Step ${n} of ${state.ov.stages.length}</div><h2>${stage.title}</h2></div>
      <span class="pill ${stage.state}">${{ done: "done", action: "needs you", error: "problem", running: "in progress", waiting: "not yet", preview: "preview", later: "later" }[stage.state]}</span>
    </div>
    <div class="stack-sm"><p class="what">${what}</p><p class="why small">${why}</p></div>`;
  const key = `${state.runId}:${stage.key}`;
  if (!quiet) el.innerHTML = h`${head}${loading(stage.key === "silver" ? "Applying the rules to every row (first time takes a few seconds)…" : "Loading…")}`.s;
  el.dataset.key = key;
  let body;
  try {
    body = await PANELS[stage.key](state.ov, stage);
  } catch (e) {
    body = h`<div class="callout ${e.status === 404 ? "" : "bad"}">${e.status === 404 ? notYet(stage) : e.message}</div>`;
  }
  if (el.dataset.key !== key) return; // user moved on
  const scroll = window.scrollY;
  el.innerHTML = h`${head}${body}`.s;
  if (quiet) window.scrollTo(0, scroll);
  afterRender[stage.key]?.();
}

function notYet(stage) {
  return { profile: "The health check has not run yet.", card: "The AI briefing has not been prepared yet.",
    approval: "There are no proposed rules yet. They appear after the AI review.", silver: "The cleaned preview appears once rules have been proposed." }[stage.key] || "Not available yet.";
}

// --- panels -----------------------------------------------------------------------------------
const PANELS = {};
const afterRender = {};

PANELS.upload = async (ov) => {
  const b = await api(`/runs/${ov.run_id}/bronze`);
  const r = b.run;
  return h`
    <div class="grid">${kpi(`${num(Math.round((r.file_size_bytes || 0) / 1024))} KB`, "file size")}${kpi(r.encoding || "–", "text encoding detected")}${kpi(num(r.physical_lines), "lines in the file")}${kpi((r.header || []).length, "columns")}</div>
    <div class="stack-sm"><h4>Columns found in the header</h4><div class="chips">${(r.header || []).map((c) => h`<span class="chip code">${c || "(blank)"}</span>`)}</div></div>
    <details><summary>Technical details</summary><div class="stack-sm small">
      <div>File name: <span class="mono">${b.run.file}</span></div>
      <div>Stored at: <span class="mono">${r.source_file}</span></div>
      <div>Fingerprint (SHA-256): <span class="mono">${r.file_sha256}</span></div>
      <div>Received: ${when(r.started_at)} · processed in ${dur(r.duration_ms)}</div>
    </div></details>`;
};

PANELS.bronze = async (ov) => {
  const b = await api(`/runs/${ov.run_id}/bronze`);
  const r = b.run;
  if (r.status === "failed") return h`<div class="callout bad"><b>Loading failed.</b> ${r.error_message}</div>`;
  const parsed = r.records_parsed || 1;
  const reasons = Object.entries(r.malformed_by_reason || {});
  return h`
    <div class="callout ${b.reconciled ? "ok" : "bad"}">${b.reconciled
      ? h`<b>✓ Every record is accounted for.</b> ${num(r.records_parsed)} records were read, and ${num(r.rows_loaded_ok)} stored + ${num(r.rows_loaded_malformed)} stored as broken + ${num(r.load_failed)} refused = ${num(b.accounted)}.`
      : h`<b>Counts do not add up.</b> ${num(b.accounted)} of ${num(r.records_parsed)} records accounted for.`}</div>
    <div class="stack-sm"><h4>From file to Bronze</h4><div class="funnel">
      ${funnelRow("Lines in the file", r.physical_lines, r.physical_lines, "muted", "includes the header line and line breaks inside quoted values")}
      ${funnelRow("Records read", r.records_parsed, r.physical_lines, "accent")}
      ${funnelRow("Stored as-is", r.rows_loaded_ok, parsed, "ok")}
      ${funnelRow("Stored, marked broken", r.rows_loaded_malformed, parsed, "warn", "wrong number of fields etc.; kept so nothing is lost")}
      ${funnelRow("Refused by the database", r.load_failed, parsed, "bad")}
    </div></div>
    ${reasons.length ? h`<div class="stack-sm"><h4>Why some rows are marked broken</h4><div class="chips">${reasons.map(([k, v]) => h`<span class="chip">${k}: ${num(v)}</span>`)}</div></div>` : ""}
    ${b.duplicate_groups.length ? h`<div class="callout warn">${plural(r.duplicate_hash_groups, "group")} of identical lines. They are kept here; removing duplicates is a cleaning decision made later.</div>` : ""}
    <div class="stack-sm">
      <div class="row between"><h4>Data at this checkpoint: the raw copy</h4><span class="small muted">Click a row to follow it through the pipeline</span></div>
      <div class="toolbar">
        <div class="seg" data-group="bstatus">${[["", "All"], ["ok", "Stored as-is"], ["malformed", "Broken"]].map(([v, l]) => h`<button data-act="bfilter" data-v="${v}" class="${v === bronzeTable.status ? "on" : ""}">${l}</button>`)}</div>
        <input type="search" id="bsearch" placeholder="Search the raw text…" value="${bronzeTable.q}">
      </div>
      <div id="btable">${loading()}</div>
    </div>
    <details><summary>Load batches (${b.batches.length})</summary>
      <div class="table-wrap"><table><thead><tr><th class="num">Batch</th><th class="num">Lines</th><th class="num">Records</th><th class="num">Broken</th><th class="num">Refused</th><th>Method</th><th class="num">Time</th></tr></thead>
      <tbody>${b.batches.map((x) => h`<tr><td class="num">${x.batch_no}</td><td class="num">${x.first_line}–${x.last_line}</td><td class="num">${num(x.records)}</td><td class="num">${num(x.malformed)}</td><td class="num">${num(x.load_failed)}</td><td>${x.method === "copy" ? "bulk copy" : "row by row (fallback)"}</td><td class="num">${dur(x.duration_ms)}</td></tr>`)}</tbody></table></div>
    </details>`;
};

function funnelRow(label, n, of, tone, title) {
  return h`<div class="f" title="${title || ""}"><div class="small">${label}</div>${bar([[(n || 0) / (of || 1), tone]])}<div class="n">${num(n || 0)}</div></div>`;
}

const bronzeTable = { status: "", q: "", offset: 0, limit: 50 };
afterRender.bronze = () => {
  loadBronzeRows();
  const s = $("#bsearch");
  let t;
  s && s.addEventListener("input", () => { clearTimeout(t); t = setTimeout(() => { bronzeTable.q = s.value; bronzeTable.offset = 0; loadBronzeRows(); }, 300); });
};

async function loadBronzeRows() {
  const el = $("#btable");
  if (!el) return;
  const p = new URLSearchParams({ offset: bronzeTable.offset, limit: bronzeTable.limit });
  if (bronzeTable.status) p.set("status", bronzeTable.status);
  if (bronzeTable.q) p.set("q", bronzeTable.q);
  const d = await api(`/runs/${state.runId}/bronze/rows?${p}`);
  const cols = d.header.length ? d.header : Object.keys(d.rows[0]?.payload || {});
  el.innerHTML = h`
    <div class="table-wrap"><table>
      <thead><tr><th class="num">Line</th><th>Status</th>${cols.map((c) => h`<th>${c}</th>`)}</tr></thead>
      <tbody>${d.rows.map((r) => h`<tr class="clickable" data-act="trace" data-id="${r.bronze_id}">
        <td class="num mono">${r.source_line}</td>
        <td>${r.parse_status === "ok" ? h`<span class="pill ok">ok</span>` : h`<span class="pill warn" title="${r.parse_error}">broken</span>`}</td>
        ${cols.map((c) => h`<td class="cell">${rv(r.payload[c])}</td>`)}</tr>`)}
      </tbody></table></div>
    ${pager(d.total, bronzeTable, "bpage")}`.s;
}

function pager(total, t, act) {
  const from = total ? t.offset + 1 : 0;
  const to = Math.min(t.offset + t.limit, total);
  return h`<div class="row between small muted"><span>Showing ${num(from)}–${num(to)} of ${num(total)}</span>
    <span class="row"><button class="btn small" data-act="${act}" data-d="-1" ${t.offset <= 0 ? raw("disabled") : ""}>← Previous</button>
    <button class="btn small" data-act="${act}" data-d="1" ${to >= total ? raw("disabled") : ""}>Next →</button></span></div>`;
}

const profileState = { sev: "" };
PANELS.profile = async (ov) => {
  const p = await api(`/runs/${ov.run_id}/profile`);
  const s = p.summary;
  const counts = { high: 0, medium: 0, low: 0 };
  p.issues.forEach((i) => counts[i.severity]++);
  const issues = p.issues.filter((i) => !profileState.sev || i.severity === profileState.sev);
  return h`
    <div class="grid">
      ${kpi(num(s.rows_ok), "rows checked")}${kpi(num(s.columns), "columns")}
      ${kpi(counts.high, "high-severity issues", counts.high ? "bad" : "ok")}${kpi(counts.medium, "medium issues", counts.medium ? "warn" : "ok")}
      ${kpi(pct(s.overall_empty_or_null_like_share), "of all cells empty or placeholder")}
      ${kpi(num(s.exact_duplicate_rows), "exact duplicate rows")}
    </div>
    <div class="stack-sm">
      <div class="row between"><h4>Columns</h4><span class="small muted">Click a column for its full health check</span></div>
      <div class="table-wrap"><table>
        <thead><tr><th>Column</th><th>Looks like</th><th>Empty or placeholder</th><th class="num">Distinct values</th><th>Most common values</th><th>Issues</th></tr></thead>
        <tbody>${p.columns.map((c) => h`<tr class="clickable" data-act="colprofile" data-col="${c.column}">
          <td><b>${c.column}</b></td>
          <td class="nowrap">${typeLabel(c.type)} <span class="faint small">${pct(c.confidence, 0)}</span></td>
          <td style="min-width:150px"><div class="row" style="gap:8px;flex-wrap:nowrap">${bar([[c.empty_share, c.empty_share > 0.2 ? "warn" : "muted"]])}<span class="small nowrap">${pct(c.empty_share)}</span></div></td>
          <td class="num">${num(c.distinct)}</td>
          <td class="cell small">${c.top_values.slice(0, 3).map(([v, n]) => h`<span class="chip">${v === "" ? "(empty)" : v} ×${num(n)}</span> `)}</td>
          <td>${c.issues ? h`${sevPill(c.worst)} <span class="small muted">${c.issues}</span>` : h`<span class="pill ok">clean</span>`}</td>
        </tr>`)}</tbody></table></div>
    </div>
    <div class="stack-sm">
      <div class="row between"><h4>Issues found (${p.issues.length})</h4>
        <div class="seg">${[["", "All"], ["high", `High ${counts.high}`], ["medium", `Medium ${counts.medium}`], ["low", `Low ${counts.low}`]].map(([v, l]) => h`<button data-act="sev" data-v="${v}" class="${v === profileState.sev ? "on" : ""}">${l}</button>`)}</div>
      </div>
      <div class="small muted">${Object.entries(p.severity_help).map(([k, v]) => h`<span><b>${k}</b>: ${v} </span>`)}</div>
      <div>${issues.map(issueRow)}</div>
    </div>`;
};

const typeLabel = (t) => ({ text: "text", integer: "whole numbers", float: "numbers", date: "dates", datetime: "dates and times", mixed: "mixed", boolean: "yes/no", empty: "empty", email: "emails", phone: "phone numbers", url: "links" }[t] || t);

function issueRow(i) {
  return h`<div class="issue">
    <div>${sevPill(i.severity)}</div>
    <div class="stack-sm">
      <div><b>${i.title}</b> <span class="chip code">${i.column || "whole dataset"}</span></div>
      <div>${i.plain}</div>
      ${i.why ? h`<div class="small muted">Why it matters: ${i.why}</div>` : ""}
      <details><summary class="small">Evidence (${i.id})</summary><pre>${JSON.stringify(i.evidence, null, 2)}</pre></details>
    </div></div>`;
}

PANELS.context = async (ov) => {
  const c = await api(`/runs/${ov.run_id}/context`);
  const used = c.used;
  const d = used && used.draft;
  return h`
    ${!used ? h`<div class="callout ${ov.stages.find((s) => s.key === "context").state === "error" ? "warn" : ""}">${ov.llm_mode === "replay" ? "The AI is in replay mode and has no recorded answer for this file, so it could not write the business context. " : "The AI has not written the business context yet. "}The AI review tries again when it starts, and runs without it if it still cannot.
      ${ov.llm_mode !== "replay" ? h`<div class="row"><button class="btn" data-act="ctxretry">Try again now</button></div>` : ""}</div>` : ""}
    ${used ? h`<div class="stack">
      <div class="row"><span class="pill ${used.source === "human" ? "human" : "agent"}">${used.source === "human" ? "written by a person" : "decided by AI"}</span>
        ${used.model ? h`<span class="small muted">${used.model} · ${num(used.input_tokens)} in / ${num(used.output_tokens)} out tokens · ${money(used.cost_usd)}${used.cache_hit ? " (replayed, $0)" : ""}</span>` : ""}</div>
      ${d && d.domain ? h`<h3>${d.domain}</h3>` : ""}
      <p class="what">${d ? d.summary : used.text}</p>
      ${d && d.likely_metrics && d.likely_metrics.length ? h`<div class="stack-sm"><h4>Metrics this data feeds</h4><div class="chips">${d.likely_metrics.map((m) => h`<span class="chip">${m}</span>`)}</div></div>` : ""}
      ${d && d.critical_columns && d.critical_columns.length ? h`<div class="stack-sm"><h4>Critical columns</h4><div class="chips">${d.critical_columns.map((m) => h`<span class="chip code">${m}</span>`)}</div></div>` : ""}
      ${d && d.column_tags && Object.keys(d.column_tags).length ? h`<div class="stack-sm"><h4>What each column is</h4><div class="table-wrap"><table><thead><tr><th>Column</th><th>Kind</th><th>Shown to the AI as</th></tr></thead><tbody>
        ${Object.entries(d.column_tags).map(([col, tag]) => h`<tr><td class="mono">${col}</td><td>${tag.replace("_", " ")}</td><td class="small">${["person", "contact_pii"].includes(tag) ? h`<span class="pill warn">shape only (private)</span>` : ["free_text", "id"].includes(tag) ? h`<span class="pill">shape only</span>` : h`<span class="pill ok">values</span>`}</td></tr>`)}
      </tbody></table></div></div>` : ""}
    </div>` : ""}
    ${c.human && c.inferred ? h`<details><summary>Compare with the AI's draft</summary><div class="grid-2">
      <div class="stack-sm"><h4>Person</h4><p>${c.human.text}</p></div><div class="stack-sm"><h4>AI draft</h4><p>${c.inferred.text}</p></div></div></details>` : ""}
    <details><summary>Optional: correct it yourself</summary>
      <div class="stack-sm">
        <p class="small muted">Plain text. Optional lines: <span class="mono">Domain:</span>, <span class="mono">Critical columns: a, b</span> and <span class="mono">Tags: column=kind, …</span> (kinds: id, timestamp, category, status, priority, money, quantity, duration, free_text, person, contact_pii, location, other). Columns tagged person or contact_pii are never shown to the AI.</p>
        <textarea id="ctxtext">${contextTemplate(used, ov)}</textarea>
        <div class="row"><button class="btn" data-act="ctxsave">Save my description (replaces the AI's)</button></div>
      </div>
    </details>`;
};

function contextTemplate(used) {
  if (used && used.source === "human") return used.text;
  const d = used && used.draft;
  if (!d) return "Domain: \nCritical columns: \nTags: \n\nWhat is one row? Who uses this data, and for which decisions?";
  const tags = Object.entries(d.column_tags || {}).map(([c, t]) => `${c}=${t}`).join(", ");
  return `Domain: ${d.domain || ""}\nCritical columns: ${(d.critical_columns || []).join(", ")}\nTags: ${tags}\n\n${d.summary || ""}`;
}

PANELS.card = async (ov) => {
  const c = await api(`/runs/${ov.run_id}/card`);
  const tokens = c.exact_tokens || c.est_tokens;
  return h`
    <div class="grid">
      ${kpi(num(tokens), `tokens ${c.exact_tokens ? "(exact)" : "(estimated)"} of ${num(c.budget_tokens)} allowed`, c.over_budget ? "bad" : "ok")}
      ${kpi(`${c.issues_shown} / ${c.issues_total}`, "issues included")}
      ${kpi(`${c.columns_flagged} / ${c.columns_total}`, "columns with issues")}
      ${kpi(num(c.chars), "characters")}
    </div>
    <div class="callout info small">The briefing is the same every time for the same data (byte for byte), so it can be cached and the review can be replayed exactly. Anything that does not fit is counted with a pointer the AI can follow, so no issue is silently left out.</div>
    <div class="stack-sm"><h4>Exactly what the AI reads</h4><pre>${c.card_text}</pre></div>`;
};

PANELS.agent = async (ov, stage) => {
  const a = await api(`/runs/${ov.run_id}/agent`);
  const snap = a.snapshot;
  const m = a.metrics || {};
  const running = stage.state === "running";
  const modeNote = ov.llm_mode === "replay"
    ? h`<div class="callout warn small">AI mode is <b>replay</b>: only reviews recorded earlier can be replayed (free, identical). A new file needs <span class="mono">LLM_MODE=live</span> and an API key in <span class="mono">.env</span>.</div>`
    : h`<div class="callout small">This calls <b>${a.model}</b>. A review of a 13-column file took about 2 minutes and cost about $0.70.</div>`;
  if (!snap && !running) {
    return h`${stage.state === "error" ? h`<div class="callout bad"><b>The review could not start.</b> ${ov.job && ov.job.error}${a.llm && a.llm.last_error ? h`<div class="small">${a.llm.last_error}</div>` : ""}</div>` : ""}
      <div class="callout row between"><span>The AI reviewer has not looked at this data yet.</span><button class="btn primary" data-act="agentstart">Start the AI review</button></div>${modeNote}`;
  }
  const v = a.validation;
  return h`
    ${running ? h`<div class="callout info"><div class="jobbar"><span class="spinner"></span><div><b>The AI is investigating.</b> Each check it runs appears below as it happens.</div></div></div>` : ""}
    ${snap && snap.interrupted && !running ? h`<div class="callout warn row between"><span>The review stopped before it finished (for example, the server restarted).</span><button class="btn primary" data-act="agentresume">Resume</button></div>` : ""}
    ${snap && snap.status === "failed" ? h`<div class="callout bad">The AI could not produce rules that pass the automatic checks after 3 attempts.</div>` : ""}
    <div class="grid">
      ${kpi(num(m.turns ?? snap?.turns), "reasoning steps")}${kpi(num(m.tool_calls ?? snap?.tool_calls), "checks run on the data")}
      ${kpi(money(m.cost_usd), "AI cost")}${kpi(dur(m.duration_ms), "time taken")}
      ${kpi(num(m.validation_attempts ?? snap?.attempts), "proposals sent back by the checks")}
      ${m.issues_total != null ? kpi(`${num(m.issues_addressed)} / ${num(m.issues_total)}`, "required issues fixed by a rule") : ""}
    </div>
    ${m.first_validation_errors && m.first_validation_errors.length ? h`<div class="callout warn small"><b>The first proposal was sent back by the automatic checks:</b><ul>${m.first_validation_errors.slice(0, 6).map((e) => h`<li>${e}</li>`)}</ul>The AI fixed these and resubmitted.</div>` : ""}
    ${v ? h`<div class="callout ${v.ok ? "ok" : "bad"} small"><b>${v.ok ? "✓ Final proposal passed every automatic check" : `Blocked at the ${v.layer} check`}</b>: format, meaning, a dry run on 2,000 sample rows, and coverage of every high and medium issue.
      ${v.errors && v.errors.length ? h`<ul>${v.errors.map((e) => h`<li>${e}</li>`)}</ul>` : ""}</div>` : ""}
    ${running && a.recent_tool_calls.length ? h`<div class="stack-sm"><h4>Latest checks</h4><div>${a.recent_tool_calls.slice(0, 8).map((c) => h`<div class="call"><span class="pill ${c.ok ? "" : "error"}">${c.tool}</span> ${c.plain} <span class="faint small">${dur(c.latency_ms)}</span></div>`)}</div></div>` : ""}
    <div class="stack-sm"><h4>What the AI did, step by step</h4>
      ${a.transcript.length ? h`<div>${a.transcript.map(turnRow)}</div>` : h`<p class="muted">No steps recorded yet.</p>`}
    </div>
    <div class="row">${!running ? h`<button class="btn" data-act="agentfresh">Run a fresh review</button>` : ""}<span class="small muted">The AI only sees masked values for private and free-text columns.</span></div>
    ${modeNote}`;
};

function turnRow(t) {
  return h`<div class="turn"><div class="n">${t.turn}</div><div class="stack-sm">
    ${t.said.map((s) => h`<p>${s}</p>`)}
    ${t.calls.map((c) => h`<div class="call">
      <div>${c.tool === "submit_rules" ? h`<span class="pill agent">submit</span>` : h`<span class="chip code">${c.tool}</span>`} ${c.plain}${c.result && c.result.error ? h` <span class="pill error">returned an error</span>` : ""}</div>
      <details class="res"><summary class="small">What it asked and what came back</summary><pre>${JSON.stringify(c.args, null, 2)}</pre>${c.result ? h`<pre style="margin-top:6px">${c.result.text}</pre>` : ""}</details>
    </div>`)}
    ${!t.said.length && !t.calls.length ? h`<p class="muted small">Thinking (not shown)</p>` : ""}
  </div></div>`;
}

const reviewState = { filter: "all", rejecting: null, items: [], editable: false, leftovers: {} };

PANELS.approval = async (ov) => {
  const r = await api(`/runs/${ov.run_id}/rules`);
  if (r.invalid) return h`<div class="callout bad">The rules file <span class="mono">${r.path}</span> is not valid. Fix it or run a fresh review.</div>`;
  const awaiting = r.agent_status === "awaiting_approval" && r.state !== "approved";
  const cov = { rule: 0, ignored: 0, open: 0 };
  r.coverage.forEach((c) => cov[c.resolution === "rule" ? "rule" : c.resolution === "ignored" ? "ignored" : "open"]++);
  const warnings = (r.validation && r.validation.warnings) || [];
  const rv_ = r.review;
  const items = [
    ...r.columns.map((c) => ({ kind: "column", rule: c })),
    ...r.row_checks.map((c) => ({ kind: "row_check", rule: c })),
    ...(r.dedup ? [{ kind: "dedup", rule: r.dedup }] : []),
  ];
  reviewState.items = items;
  reviewState.editable = awaiting;
  const shown = items.map((it, i) => [it, i]).filter(([it]) => {
    const d = it.rule.review.decision;
    return reviewState.filter === "all" || (reviewState.filter === "todo" ? d === "pending" || d === "stale" : d === reviewState.filter);
  });
  const n = rv_.total || 1;
  return h`
    ${awaiting ? h`<div class="callout warn stack-sm">
      <div><b>Review each rule below.</b> Approve it, or reject it with a reason. A rejected column rule falls back to the safe default (spaces and invisible characters only); a rejected check is dropped. Nothing is applied until you press <b>Finish review</b>.</div>
      <div class="row" style="gap:10px">${bar([[rv_.approved / n, "ok", "approved"], [rv_.rejected / n, "bad", "rejected"]])}
        <span class="small nowrap"><b>${rv_.approved + rv_.rejected} of ${rv_.total}</b> reviewed · ${rv_.approved} approved · ${rv_.rejected} rejected${rv_.stale ? ` · ${rv_.stale} changed since your decision` : ""}</span></div>
      <div class="row">
        <button class="btn primary" data-act="finishreview" ${rv_.can_finish ? "" : raw("disabled")}>Finish review</button>
        <span class="small muted">${rv_.can_finish ? `Uses ${rv_.approved} approved rule(s); ${rv_.rejected} rejected fall back to defaults or are dropped.` : `${rv_.pending + rv_.stale} rule(s) still need a decision.`}</span>
        <span class="spacer" style="flex:1"></span><button class="btn small" data-act="editrules">Edit the YAML directly</button>
      </div></div>` : ""}
    ${r.state === "approved" ? h`<div class="callout ok row between"><span><b>✓ Review finished.</b> ${rv_.approved} rule(s) approved, ${rv_.rejected} rejected. Saved to <span class="mono">${r.path}</span>; these rules produce the cleaned data.</span>
      <button class="btn" data-act="reopen">Reopen review to make changes</button></div>` : ""}
    ${r.agent_status === "rejected" ? h`<div class="callout bad"><b>This proposal was rejected.</b> Run a fresh AI review to get a new one.</div>` : ""}
    <div class="grid">
      ${kpi(items.length, "rules to review")}${kpi(r.columns.filter((c) => c.generated_by === "agent").length, "column rules written by the AI")}
      ${kpi(r.columns.filter((c) => c.generated_by === "default").length, "safe defaults (clean columns)")}
      ${kpi(`${cov.rule} / ${r.coverage.length}`, "issues handled by a rule", cov.open ? "warn" : "ok")}
    </div>
    ${r.needs_human.length ? h`<div class="stack-sm"><h4>Questions the AI wants you to answer</h4><p class="small muted">Answer them through your decisions below (or by editing the rule).</p>${r.needs_human.map((q) => h`<div class="callout warn stack-sm"><b>${q.question}</b><div class="chips">${q.options.map((o) => h`<span class="chip">${o}</span>`)}</div><div class="small">AI recommends: <b>${q.agent_recommendation}</b></div><div class="small muted">About: ${q.issue_ids.join(", ")}</div></div>`)}</div>` : ""}
    ${warnings.length ? h`<div class="stack-sm"><h4>Points to look at</h4><div class="callout warn small"><ul style="margin:0;padding-left:18px">${warnings.map((w) => h`<li>${w}</li>`)}</ul></div></div>` : ""}
    <div class="stack-sm">
      <div class="row between"><h4>Rules (${items.length})</h4>
        <div class="seg">${[["all", "All"], ["todo", `To review ${rv_.pending + rv_.stale}`], ["approved", `Approved ${rv_.approved}`], ["rejected", `Rejected ${rv_.rejected}`]].map(([v, l]) => h`<button data-act="rfilter" data-v="${v}" class="${v === reviewState.filter ? "on" : ""}">${l}</button>`)}</div></div>
      ${shown.length ? shown.map(([it, i]) => ruleCard(it, i, awaiting, r.field_help)) : h`<p class="muted">No rules in this view.</p>`}
    </div>
    <div class="stack-sm"><h4>Every issue and how it is handled</h4>
      <div class="table-wrap"><table><thead><tr><th>Severity</th><th>Issue</th><th>Handled by</th></tr></thead><tbody>
      ${r.coverage.map((c) => h`<tr><td>${sevPill(c.severity)}</td><td><b>${c.title}</b><div class="small muted">${c.plain}</div><div class="faint small mono">${c.id}</div></td>
        <td>${c.resolution === "rule" ? h`<span class="pill ok">a rule</span>` : c.resolution === "ignored" ? h`<span class="pill">left as is</span><div class="small muted">${c.reason}</div>` : h`<span class="pill ${c.severity === "low" ? "low" : "warn"}">${c.severity === "low" ? "defaults only" : "not handled"}</span>`}</td></tr>`)}
      </tbody></table></div></div>`;
};
afterRender.approval = () => loadRuleEffects();

const DECISION_PILL = {
  pending: h`<span class="pill">not reviewed</span>`, approved: h`<span class="pill ok">✓ approved</span>`,
  rejected: h`<span class="pill bad">rejected</span>`, stale: h`<span class="pill warn">changed since your decision</span>`,
};

function fieldValue(k, v, ctx = {}) {
  if (v === null || v === undefined || (Array.isArray(v) && !v.length)) return h`<span class="faint">—</span>`;
  if (typeof v === "boolean") return v ? "yes" : "no";
  if (k === "mapping") return h`<table><tbody>${Object.entries(v).map(([t, vs]) => h`<tr><td class="nowrap"><b>${t}</b></td><td class="small">${vs.map((x) => h`<span class="chip">${x}${ctx.editable ? h` <button class="btn link small" title="Stop merging '${x}' into ${t}" data-act="rmvalue" data-col="${ctx.column}" data-target="${t}" data-value="${x}">×</button>` : ""}</span> `)}</td></tr>`)}</tbody></table>`;
  if (k === "range") return [v.min != null ? `min ${v.min}` : "", v.max != null ? `max ${v.max}` : ""].filter(Boolean).join(" · ") || h`<span class="faint">—</span>`;
  if (k === "params") return Object.entries(v).map(([a, b]) => `${a}: ${Array.isArray(b) ? b.join(", ") : b}`).join(" · ");
  if (k === "confidence") return pct(v, 0);
  if (Array.isArray(v)) return h`<div class="chips">${v.map((x) => h`<span class="chip ${k === "addresses" || k === "parse_formats" ? "code" : ""}">${x}</span>`)}</div>`;
  return String(v);
}

const unset = (v) => v === null || v === undefined || (Array.isArray(v) && !v.length);

function fieldsTable(fields, help, ctx = {}) {
  const entries = Object.entries(fields);
  const empty = entries.filter(([, v]) => unset(v));
  return h`<table><tbody>${entries.filter(([, v]) => !unset(v)).map(([k, v]) => h`<tr>
    <td class="nowrap" style="width:1%"><b class="small">${(help[k] || [k])[0]}</b><div class="faint small mono">${k}</div></td>
    <td>${fieldValue(k, v, ctx)}</td>
    <td class="small muted">${(help[k] || ["", ""])[1]}</td></tr>`)}
    ${empty.length ? h`<tr><td class="nowrap"><b class="small">Not used</b></td><td colspan="2" class="small">${empty.map(([k]) => h`<span class="chip" title="${(help[k] || ["", ""])[1]}">${(help[k] || [k])[0]} <span class="faint mono">${k}</span></span> `)}</td></tr>` : ""}
  </tbody></table>`;
}

function ruleCard(it, i, editable, help) {
  const c = it.rule;
  const d = c.review;
  const title = it.kind === "column" ? h`${c.name}${c.source !== c.name ? h` <span class="faint small">from ${c.source}</span>` : ""}`
    : it.kind === "row_check" ? h`Check: ${c.plain}` : h`Duplicates: ${c.plain}`;
  const who = it.kind === "column" ? (c.edited ? h`<span class="pill human">edited by you</span>` : byPill(c.generated_by)) : "";
  return h`<div class="rule" style="${d.decision === "approved" ? "border-color:var(--ok)" : d.decision === "rejected" ? "border-color:var(--bad)" : ""}">
    <div class="row between"><h3>${title}</h3>
      <span class="row" style="gap:6px">${who}${c.confidence != null ? h`<span class="conf" title="how sure the author is">${bar([[c.confidence, c.confidence >= 0.8 ? "ok" : c.confidence >= 0.6 ? "warn" : "bad"]])}${pct(c.confidence, 0)}</span>` : ""}${DECISION_PILL[d.decision]}</span></div>
    ${it.kind === "column" ? h`<div class="stack-sm"><h4>What it does</h4><ol>${c.plain.map((p) => h`<li>${p}</li>`)}</ol></div>` : ""}
    <div class="small"><b>Why:</b> ${c.why}</div>
    ${c.issues.length ? h`<div class="stack-sm"><h4>Fixes these issues</h4>${c.issues.map((x) => h`<div class="small">${x.severity ? sevPill(x.severity) : ""} <b>${x.title || x.id}</b> ${x.plain || ""} <span class="faint mono">${x.id}</span></div>`)}</div>` : ""}
    ${c.ai_mapped && c.ai_mapped.length ? aiMappedTable(c, i, editable) : ""}
    <div class="stack-sm"><h4>Effect on your data</h4><div class="small" data-effect="${i}">${loading("Applying it to every row…")}</div>
      ${c.stats ? h`<div class="small faint">On the 2,000 validation sample rows: empty ${pct(c.stats.null_before)} → ${pct(c.stats.null_after)}${c.stats.invalid ? `, unreadable ${pct(c.stats.invalid)}` : ""}${c.stats.mapped ? `, ${num(c.stats.mapped)} spellings standardised` : ""}${c.stats.unmapped ? `, ${num(c.stats.unmapped)} not in the standard list` : ""}</div>` : ""}</div>
    <details open><summary>All settings</summary>${fieldsTable(c.all_fields, help, { editable: editable && it.kind === "column", column: c.name })}</details>
    <details><summary>As written in the file</summary><pre>${c.yaml}</pre></details>
    ${d.decision === "rejected" ? h`<div class="callout bad small stack-sm"><div><b>Rejected</b> ${when(d.at)}: ${d.reason}</div>
      <div>${it.kind === "column" ? "The safe default rule is used instead (shown above)." : "This check is no longer applied."} Its issues are recorded as left as is, with your reason.</div>
      ${d.original ? h`<details><summary>What the AI proposed</summary>${fieldsTable(d.original, help)}</details>` : ""}</div>` : ""}
    ${d.decision === "approved" ? h`<div class="small" style="color:var(--ok)">✓ Approved ${when(d.at)}</div>` : ""}
    ${d.decision === "stale" ? h`<div class="callout warn small">You ${d.was} this rule ${when(d.at)}, but it has been edited since. Please decide again.</div>` : ""}
    ${editable ? (d.decision === "pending" || d.decision === "stale" ? (reviewState.rejecting === i ? h`
      <div class="row"><input type="text" id="rej-${i}" placeholder="Why reject it? (at least 10 characters)" style="flex:1;min-width:220px">
        <button class="btn danger" data-act="rulereject" data-i="${i}">Reject rule</button><button class="btn" data-act="rulerejectcancel">Cancel</button></div>` : h`
      <div class="row"><button class="btn primary" data-act="ruleapprove" data-i="${i}">Approve</button><button class="btn danger" data-act="rulerejectopen" data-i="${i}">Reject…</button></div>`)
      : h`<div class="row"><button class="btn small" data-act="ruleundo" data-i="${i}">Undo decision</button></div>`) : ""}
  </div>`;
}

async function loadRuleEffects() {
  if (!document.querySelector("[data-effect]")) return;
  let s;
  try { s = await api(`/runs/${state.runId}/silver`); } catch (e) {
    document.querySelectorAll("[data-effect]").forEach((el) => { el.innerHTML = h`<span class="muted">${e.message}</span>`.s; });
    return;
  }
  const n = s.rows_in;
  const cols = Object.fromEntries(s.columns.map((c) => [c.name, c]));
  const errs = Object.fromEntries(s.errors.map((e) => [e.error, e]));
  document.querySelectorAll("[data-effect]").forEach((el) => {
    const it = reviewState.items[Number(el.dataset.effect)];
    if (!it) return;
    let html;
    if (it.kind === "column") {
      const c = cols[it.rule.name];
      html = c ? h`On all ${num(n)} rows: <b>${num(c.fixed)}</b> values fixed, ${num(c.reformatted)} reformatted, empty ${num(c.empty_before)} → <b>${num(c.empty_after)}</b>.
        ${c.flags.length ? h`<div class="chips" style="margin-top:4px">${c.flags.map((f) => h`<span class="chip">${f.label} ×${num(f.count)}</span>`)}</div>` : ""}
        ${c.examples.length ? h`<div style="margin-top:4px">${c.examples.slice(0, 4).map((e) => h`<div class="cell">${rv(e.before)} → <b>${e.after ?? h`<span class="empty-v">empty</span>`}</b> <span class="faint">×${num(e.count)}</span></div>`)}</div>` : ""}
        <button class="btn link small" data-act="silvercol" data-col="${it.rule.name}">See every value and what it becomes →</button>
        ${c.leftovers.length ? leftoverChooser(it, Number(el.dataset.effect), c.leftovers) : ""}` : h`<span class="muted">Not in the preview.</span>`;
      reviewState.leftovers[it.rule.name] = c ? c.leftovers : [];
    } else if (it.kind === "row_check" && it.rule.kind === "swapped_columns") {
      html = h`<b>${num(s.row_fixes?.swapped_columns || 0)}</b> of ${num(n)} rows have their two columns swapped back.`;
    } else if (it.kind === "row_check" && it.rule.kind === "declared_duplicate") {
      html = h`<b>${num(s.row_fixes?.declared_duplicate || 0)}</b> of ${num(n)} tickets say they duplicate another; they are kept but not counted.`;
    } else if (it.kind === "row_check") {
      const e = errs[it.rule.error_key];
      html = h`<b>${num(e ? e.count : 0)}</b> of ${num(n)} rows fail this check (${pct((e ? e.count : 0) / n)}).`;
    } else {
      html = h`<b>${num(s.status.duplicate || 0)}</b> duplicate rows removed.`;
    }
    el.innerHTML = val(html);
  });
}

const squash = (s) => String(s).toLowerCase().replace(/[^a-z0-9]/g, "");

// A leftover that starts a standard value ("hi" -> high, "crit" -> critical) is suggested.
function suggestTarget(value, targets) {
  const v = squash(value);
  if (v.length < 2) return null;
  const hits = targets.filter((t) => squash(t).startsWith(v) || (v.startsWith(squash(t)) && squash(t).length >= 3));
  return hits.length === 1 ? hits[0] : null;
}

const nowLabel = (n) => ({ merge: h`→ <b>${n.target}</b>`, new: h`new standard value <b>${n.target}</b>`,
  empty: h`treated as <b>empty</b>`, keep: h`kept as a <b>separate value</b>` }[n.action]);

function aiMappedTable(c, i, editable) {
  const rows = c.ai_mapped.reduce((n, d) => n + d.count, 0);
  const changed = c.ai_mapped.filter((d) => d.changed).length;
  const targets = Object.keys(c.mapping || {});
  return h`<div class="stack-sm"><h4>The AI decided ${c.ai_mapped.length} value(s) outside the standard list for you (${num(rows)} rows)</h4>
    <p class="small muted">Check them; change only what is wrong.${changed ? ` You changed ${changed}.` : ""}</p>
    <div class="table-wrap" style="max-height:420px"><table><thead><tr><th>Value in your data</th><th class="num">Rows</th><th>Now</th><th>AI's reason</th>${editable ? h`<th>Change to</th>` : ""}</tr></thead><tbody>
    ${c.ai_mapped.map((d) => h`<tr>
      <td><b>${d.value}</b>${d.spellings.length > 1 ? h`<div class="faint small">also ${d.spellings.slice(1).join(", ")}</div>` : ""}</td>
      <td class="num">${num(d.count)}</td>
      <td class="nowrap">${nowLabel(d.now)}${d.changed ? h` <span class="pill human">changed by you</span>` : ""}</td>
      <td class="small muted">${d.reason}</td>
      ${editable ? h`<td><select data-aimap="${i}" data-value="${d.value}" data-current="${d.now.target || ""}" data-spellings="${JSON.stringify(d.spellings)}">
        <option value="">(no change)</option>
        ${targets.filter((t) => !(d.now.action === "merge" && t === d.now.target)).map((t) => h`<option value="merge:${t}">Merge into “${t}”</option>`)}
        ${d.now.action !== "new" ? h`<option value="new">Make it a new standard value</option>` : ""}
        ${d.now.action !== "empty" ? h`<option value="empty">Treat as empty</option>` : ""}
        ${d.now.action !== "keep" ? h`<option value="keep">Keep as a separate value</option>` : ""}
      </select></td>` : ""}</tr>`)}
    </tbody></table></div>
    ${editable ? h`<div class="row"><button class="btn small" data-act="aimapapply" data-i="${i}">Apply changes</button></div>` : ""}
  </div>`;
}

async function applyAiMapChanges(i) {
  const it = reviewState.items[i];
  const choices = [...document.querySelectorAll(`select[data-aimap="${i}"]`)].filter((s) => s.value).map((sel) => {
    const [action, ...rest] = sel.value.split(":");
    const spellings = JSON.parse(sel.dataset.spellings || "[]");
    if (action === "keep") return { value: sel.dataset.value, action: "remove", target: sel.dataset.current || null, spellings };
    return { value: sel.dataset.value, action, target: rest.length ? rest.join(":") : null, spellings };
  });
  if (!choices.length) { toast("Nothing selected to change."); return; }
  try {
    await api(`/runs/${state.runId}/rules/values`, { method: "POST", body: { column: it.rule.name, choices } });
    toast(`${it.rule.name}: ${choices.length} value(s) changed`);
    await refreshNow();
    loadPanel(true);
  } catch (e) { toast(e.message, true); }
}

function leftoverChooser(it, i, leftovers) {
  const targets = Object.keys(it.rule.all_fields.mapping || {});
  const rows = leftovers.reduce((n, l) => n + l.count, 0);
  const editable = reviewState.editable;
  return h`<div class="callout warn stack-sm" style="margin-top:8px">
    <div><b>${leftovers.length} value(s) in your data are not in the standard list</b> (${num(rows)} rows).${leftovers.some((l) => l.value.length > 40 || l.value.split(/\s+/).length > 4) ? " Long text values (sentences that ended up in this column) are never sent to the AI; decide those yourself." : ""} They stay exactly as they are and count as separate ${it.rule.name} values unless you choose otherwise.${editable ? " Likely matches are pre-selected; check them, then apply." : ""}</div>
    <table><thead><tr><th>Value in your data</th><th class="num">Rows</th>${editable ? h`<th>What should it become?</th>` : ""}</tr></thead><tbody>
    ${leftovers.map((l, k) => {
      const guess = suggestTarget(l.value, targets);
      return h`<tr><td><b>${l.value}</b>${l.spellings.length > 1 ? h`<div class="faint small">also written ${l.spellings.slice(1).join(", ")}</div>` : ""}</td><td class="num">${num(l.count)}</td>
        ${editable ? h`<td><select data-leftover="${i}" data-value="${l.value}" data-spellings="${JSON.stringify(l.spellings)}" data-k="${k}">
          <option value="keep">Leave as is (separate value)</option>
          ${targets.map((t) => h`<option value="merge:${t}" ${t === guess ? raw("selected") : ""}>Merge into “${t}”${t === guess ? " (suggested)" : ""}</option>`)}
          <option value="new">Make it a new standard value</option>
          <option value="empty">Treat as empty</option>
        </select></td>` : ""}</tr>`;
    })}</tbody></table>
    ${editable ? h`<div class="row"><button class="btn primary small" data-act="automap">Let the AI decide these</button><button class="btn small" data-act="applyvalues" data-i="${i}">Apply my choices</button><span class="small muted">The rule is re-checked; if you had approved it, decide again.</span></div>` : ""}
  </div>`;
}

async function applyValues(i) {
  const it = reviewState.items[i];
  const choices = [...document.querySelectorAll(`select[data-leftover="${i}"]`)].map((sel) => {
    const [action, ...rest] = sel.value.split(":");
    return { value: sel.dataset.value, action, target: rest.length ? rest.join(":") : null, spellings: JSON.parse(sel.dataset.spellings || "[]") };
  }).filter((c) => c.action !== "keep");
  if (!choices.length) { toast("Nothing to change: every value is set to “Leave as is”."); return; }
  try {
    await api(`/runs/${state.runId}/rules/values`, { method: "POST", body: { column: it.rule.name, choices } });
    toast(`${it.rule.name}: ${choices.length} value(s) updated`);
    await refreshNow();
    loadPanel(true);
  } catch (e) { toast(e.message, true); }
}

async function ruleDecision(i, decision, reason) {
  const it = reviewState.items[i];
  if (!it) return;
  try {
    await api(`/runs/${state.runId}/rules/review`, { method: "POST", body: { key: it.rule.key, decision, reason } });
    reviewState.rejecting = null;
    await refreshNow();
    loadPanel(true);
  } catch (e) { toast(e.message, true); }
}

const silverTable = { show: "changed", column: "", offset: 0, limit: 50 };
PANELS.silver = async (ov) => {
  const s = await api(`/runs/${ov.run_id}/silver`);
  const n = s.rows_in || 1;
  const st = s.status;
  return h`
    ${s.build ? h`<div class="callout ok small row between"><span><b>✓ Stored in <span class="mono">${s.build.target_table}</span></b>, built ${when(s.build.started_at)} in ${dur(s.build.duration_ms)} from the approved rules. Every one of the ${num(s.rows_in)} raw rows is accounted for. Usable rows (clean + with a warning) are in <span class="mono">${s.build.target_table}_valid</span>; this page reads the stored table.</span>
        <button class="btn small" data-act="buildsilver">Rebuild</button></div>`
    : s.state === "approved" ? h`<div class="callout warn small row between"><span><b>Rules approved, but the cleaned table is not built yet.</b> What you see is computed live from the approved rules.</span>
        <button class="btn primary small" data-act="buildsilver">Build the cleaned table</button></div>`
    : h`<div class="callout info small"><b>Preview.</b> These are the <i>proposed</i> rules, not yet approved. Computed live over all ${num(s.rows_in)} rows, read-only. The cleaned table <span class="mono">${s.target_table}</span> is built when you finish the review.</div>`}
    <div class="grid">
      ${kpi(num(s.rows_in), "rows in")}${kpi(num(st.kept || 0), "clean rows", "ok")}${kpi(num(st.flagged || 0), "kept, with a warning", st.flagged ? "warn" : "")}
      ${kpi(num(st.rejected || 0), "rejected", st.rejected ? "bad" : "")}${kpi(num(st.duplicate || 0), "duplicates removed")}
    </div>
    <div class="stack-sm">${bar([[(st.kept || 0) / n, "ok", "clean"], [(st.flagged || 0) / n, "warn", "with a warning"], [(st.rejected || 0) / n, "bad", "rejected"], [(st.duplicate || 0) / n, "muted", "duplicates"]])}
      <div class="small muted">Green: clean · amber: kept with a warning · red: rejected · grey: duplicates removed</div></div>
    ${s.errors.length ? h`<div class="stack-sm"><h4>Why rows got a warning or were rejected</h4><div class="vbars">${s.errors.map((e) => h`<div class="vb"><span>${e.label}</span>${bar([[e.count / n, e.label.includes("missing but required") ? "bad" : "warn"]])}<span class="small num">${num(e.count)}</span></div>`)}</div></div>` : ""}
    <div class="stack-sm"><div class="row between"><h4>What changed in each column</h4><span class="small muted">Click a column to see every value and what it became</span></div>
      <div class="table-wrap"><table><thead><tr><th>Column</th><th class="num">Values fixed</th><th class="num">Reformatted</th><th>Empty before → after</th><th>Biggest changes</th></tr></thead><tbody>
      ${s.columns.map((c) => h`<tr class="clickable" data-act="silvercol" data-col="${c.name}">
        <td><b>${c.name}</b> ${byPill(c.generated_by)}<div class="small faint">${c.type}</div></td>
        <td class="num">${num(c.fixed)}</td><td class="num">${num(c.reformatted)}</td>
        <td class="nowrap small">${num(c.empty_before)} → <b>${num(c.empty_after)}</b></td>
        <td class="small">${c.examples.slice(0, 3).map((e) => h`<div class="cell">${rv(e.before)} → <b>${e.after ?? h`<span class="empty-v">empty</span>`}</b> <span class="faint">×${num(e.count)}</span></div>`)}</td></tr>`)}
      </tbody></table></div></div>
    <div class="stack-sm">
      <div class="row between"><h4>Data at this checkpoint: before and after</h4><span class="small muted">Click a row to follow it from the file</span></div>
      <div class="toolbar">
        <div class="seg">${[["changed", "Changed rows"], ["all", "All rows"], ["flagged", "With a warning"], ["rejected", "Rejected"]].map(([v, l]) => h`<button data-act="sshow" data-v="${v}" class="${v === silverTable.show ? "on" : ""}">${l}</button>`)}</div>
        <select id="scol"><option value="">All columns</option>${s.columns.map((c) => h`<option value="${c.name}" ${c.name === silverTable.column ? raw("selected") : ""}>${c.name}</option>`)}</select>
      </div>
      <div id="stable">${loading()}</div>
    </div>`;
};
afterRender.silver = () => loadSilverRows();

async function loadSilverRows() {
  const el = $("#stable");
  if (!el) return;
  const p = new URLSearchParams({ show: silverTable.show, offset: silverTable.offset, limit: silverTable.limit });
  if (silverTable.column) p.set("column", silverTable.column);
  const d = await api(`/runs/${state.runId}/silver/rows?${p}`);
  el.innerHTML = h`
    <div class="table-wrap"><table>
      <thead><tr><th class="num">Line</th><th>Outcome</th>${d.columns.map((c) => h`<th>${c.name}</th>`)}</tr></thead>
      <tbody>${d.rows.map((r) => h`<tr class="clickable" data-act="trace" data-id="${r.bronze_id}">
        <td class="num mono">${r.source_line}</td>
        <td>${outcomePill(r.status)}${r.error_labels.length ? h`<div class="small muted" style="max-width:220px">${r.error_labels.join("; ")}</div>` : ""}</td>
        ${d.columns.map((c) => silverCell(r.cells[c.name]))}</tr>`)}
      </tbody></table></div>
    ${d.rows.length ? "" : h`<p class="muted">No rows match.</p>`}
    ${pager(d.total, silverTable, "spage")}`.s;
}

const outcomePill = (s) => ({ kept: h`<span class="pill ok">clean</span>`, flagged: h`<span class="pill warn">warning</span>`, rejected: h`<span class="pill bad">rejected</span>`, duplicate: h`<span class="pill">duplicate</span>` }[s]);

function silverCell(c) {
  if (!c) return h`<td></td>`;
  const after = c.after === null ? h`<span class="empty-v">empty</span>` : c.after;
  if (c.change === "same") return h`<td class="cell" title="${c.flags.join(", ")}">${c.after === null ? rv(c.before) : after}${c.flags.includes("unmapped") ? h` <span class="pill warn" title="not in the standard list">?</span>` : ""}</td>`;
  return h`<td class="cell ${c.change === "fixed" ? "changed" : ""}" title="${c.flags.join(", ") || "format unified"}"><span class="before">${c.before === null || c.before === "" ? "(empty)" : c.before}</span><span class="after ${c.change}">${after}</span></td>`;
}

const hrs = (h) => (h == null ? "–" : Math.abs(h) < 48 ? `${Number(h).toFixed(1)} h` : `${(h / 24).toFixed(0)} days`);
const rate = (n, d) => (d ? pct(n / d) : "–");

PANELS.gold = async (ov, stage) => {
  let g;
  try { g = await api(`/runs/${ov.run_id}/gold`); } catch (e) {
    if (e.status !== 404) throw e;
    return h`<div class="callout row between"><span>The business tables are not built yet. They are built from the stored cleaned table.</span>
      ${stage.state === "action" ? h`<button class="btn primary" data-act="buildgold">Build the business tables</button>` : ""}</div>`;
  }
  const b = g.build;
  const checks = Object.entries(b.checks || {});
  const head = h`<div class="callout ${g.current ? "ok" : b.status === "failed" ? "bad" : "warn"} small row between">
    <span>${g.current ? h`<b>✓ Built ${when(b.started_at)}</b> in ${dur(b.duration_ms)} from the current cleaned table.`
      : b.status === "failed" ? h`<b>The last build failed:</b> ${b.error_message}. Showing the previous build.`
      : h`<b>The cleaned table changed since these tables were built.</b> Rebuild to update them.`}
      ${checks.length ? h` Reconciliation: ${checks.filter(([, c]) => c.ok).length}/${checks.length} checks passed.` : ""}</span>
    <button class="btn small ${g.current ? "" : "primary"}" data-act="buildgold">${g.current ? "Rebuild" : "Build now"}</button></div>`;
  if (!g.kpis) return h`${head}${g.note ? h`<p class="muted">${g.note}</p>` : ""}`;
  const k = g.kpis;
  const datesBad = g.findings.find((f) => f.finding === "resolution_dates_unreliable");
  const sevTone = { high: "bad", medium: "warn", low: "" };
  const monthly = g.monthly.filter((m) => m.month);
  const noMonth = g.monthly.find((m) => !m.month);
  return h`${head}
    ${g.findings.length ? h`<div class="stack-sm"><h4>What limits these numbers (business case)</h4>
      ${g.findings.map((f) => h`<div class="callout ${sevTone[f.severity] || ""} stack-sm">
        <div class="row between"><b>${f.title}</b><span class="row" style="gap:6px">${sevPill(f.severity)}<span class="small">${num(f.rows_affected)} of ${num(f.denominator)} (${f.share_pct}%)</span></span></div>
        <div class="small">${f.impact}</div>
        <div class="small"><b>Affects:</b> ${f.metric_affected} · <b>Recommendation:</b> ${f.recommendation}</div></div>`)}</div>` : ""}
    <div class="grid">
      ${kpi(num(k.tickets), "tickets")}
      ${kpi(num(k.resolved), `resolved (${rate(k.resolved, k.tickets)})`)}
      ${kpi(hrs(k.median_hours), `median time to resolve · based on ${num(k.usable)} of ${num(k.resolved)} resolved (${rate(k.usable, k.resolved)})`, datesBad ? "unreliable" : "")}
      ${kpi(rate(k.sla_met, k.sla_measured), `SLA met · ${num(k.sla_measured)} tickets measurable`, datesBad ? "unreliable" : "")}
      ${g.escalation ? kpi(rate(k.escalated, k.tickets), "currently escalated") : ""}
      ${kpi(`$${num(Math.round(k.total_cost || 0))}`, `repair cost · ${num(k.with_cost)} of ${num(k.tickets)} tickets have a cost (${rate(k.with_cost, k.tickets)})`)}
    </div>
    ${datesBad ? h`<div class="callout bad small"><b>Time metrics are shown for completeness but are not meaningful for this file:</b> ${datesBad.impact}</div>` : ""}
    <div class="stack-sm"><h4>Resolution time and SLA by priority</h4>
      <div class="table-wrap"><table><thead><tr><th>Priority</th><th class="num">Tickets</th><th class="num">Resolved</th><th class="num">Used for time</th><th class="num">Excluded</th><th>Coverage</th><th class="num ${datesBad ? "unreliable" : ""}">Median</th><th class="num ${datesBad ? "unreliable" : ""}">90th pct</th><th class="num ${datesBad ? "unreliable" : ""}">SLA met${datesBad ? h`<span class="badge-unreliable">unreliable</span>` : ""}</th></tr></thead><tbody>
      ${g.by_priority.map((p) => h`<tr><td><b>${p.priority}</b></td><td class="num">${num(p.tickets)}</td><td class="num">${num(p.resolved)}</td><td class="num">${num(p.usable)}</td>
        <td class="num">${num(p.excluded)}</td><td style="min-width:120px"><div class="row" style="gap:6px;flex-wrap:nowrap">${bar([[p.resolved ? p.usable / p.resolved : 0, "ok"]])}<span class="small">${rate(p.usable, p.resolved)}</span></div></td>
        <td class="num ${datesBad ? "unreliable" : ""}">${hrs(p.median_hours)}</td><td class="num ${datesBad ? "unreliable" : ""}">${hrs(p.p90_hours)}</td><td class="num ${datesBad ? "unreliable" : ""}">${rate(p.sla_met, p.sla_measured)} <span class="faint small">of ${num(p.sla_measured)}</span></td></tr>`)}
      </tbody></table></div>
      <p class="small muted">Time metrics use only resolved tickets with both dates in the right order ("used for time"); excluded = resolved tickets with a missing or impossible date. Rows with warnings are counted as tickets.</p></div>
    <div class="stack-sm"><h4>Volume and backlog by month</h4>${volumeChart(monthly)}
      <p class="small muted">Bars: opened / validly closed per month. Line: open at month end. ${noMonth ? `${num(noMonth.opened)} tickets have no readable creation date and are not on the chart.` : ""} Closures with an impossible date are not counted as closed in any month (${num(g.monthly.reduce((n, m) => n + Number(m.closed_unknown || 0), 0))} tickets).</p></div>
    <div class="stack-sm"><h4>Repair cost by category</h4>
      <div class="table-wrap"><table><thead><tr><th>Category</th><th class="num">Tickets</th><th>Cost known</th><th class="num">Total</th><th class="num">Average</th></tr></thead><tbody>
      ${g.by_category.map((c) => h`<tr><td><b>${c.category}</b></td><td class="num">${num(c.tickets)}</td>
        <td style="min-width:140px"><div class="row" style="gap:6px;flex-wrap:nowrap">${bar([[c.tickets ? c.with_cost / c.tickets : 0, "accent"]])}<span class="small">${rate(c.with_cost, c.tickets)}</span></div></td>
        <td class="num">$${num(Math.round(c.total_cost || 0))}</td><td class="num">${c.avg_cost == null ? "–" : `$${num(Math.round(c.avg_cost))}`}</td></tr>`)}
      </tbody></table></div>
      <p class="small muted">Totals and averages use tickets with a known cost only. Category source: ${g.category_source.map((c) => `${c.category_source} ${num(c.tickets)}`).join(" · ")} (ai = recovered from the ticket text).</p></div>
    <details><summary>Technical details</summary><div class="stack-sm small">
      <div>Tables: ${Object.entries(b.row_counts || {}).filter(([, v]) => typeof v === "number").map(([t, v]) => `${b.target_schema}.${t} (${num(v)})`).join(", ")}</div>
      <div>Columns used: ${Object.entries(b.roles || {}).map(([r, c]) => `${r} = ${c}`).join(", ")}</div>
      <ul style="margin:0;padding-left:18px">${checks.map(([n, c]) => h`<li>${c.ok ? "✓" : "✗"} ${n}: ${c.actual}${c.ok ? "" : ` (expected ${c.expected})`}</li>`)}</ul>
      <div>Every metric's definition (numerator, denominator, exclusions) is in <span class="mono">docs/metrics.md</span> and on each column in the database.</div>
    </div></details>`;
};

function volumeChart(months) {
  if (!months.length) return h`<p class="muted">No dated tickets.</p>`;
  // Two scales: monthly flows (bars, left axis) are ~10x smaller than the cumulative backlog (line, right axis).
  const W = 760, H = 240, padL = 44, padR = 52, padT = 18, padB = 30, n = months.length, bw = (W - padL - padR) / n;
  const nice = (v) => { const p = 10 ** Math.floor(Math.log10(Math.max(v, 1))); return Math.ceil(v / p) * p; };
  const maxFlow = nice(Math.max(1, ...months.map((m) => Math.max(Number(m.opened), Number(m.closed)))));
  const maxBack = nice(Math.max(1, ...months.map((m) => Number(m.backlog || 0))));
  const yF = (v) => H - padB - (Number(v) / maxFlow) * (H - padT - padB);
  const yB = (v) => H - padB - (Number(v) / maxBack) * (H - padT - padB);
  const ticks = [0, 0.5, 1];
  const line = months.map((m, i) => `${padL + bw * i + bw / 2},${yB(m.backlog || 0)}`).join(" ");
  return h`<div style="overflow-x:auto"><svg viewBox="0 0 ${W} ${H}" style="width:100%;min-width:520px;height:auto" role="img" aria-label="Tickets opened and closed per month (left axis) and open backlog at month end (right axis)">
    ${ticks.map((t) => h`<line x1="${padL}" x2="${W - padR}" y1="${yF(maxFlow * t)}" y2="${yF(maxFlow * t)}" style="stroke:var(--border)"/>
      <text x="${padL - 6}" y="${yF(maxFlow * t) + 4}" text-anchor="end" style="fill:var(--accent);font-size:11px">${num(maxFlow * t)}</text>
      <text x="${W - padR + 6}" y="${yB(maxBack * t) + 4}" style="fill:var(--warn);font-size:11px">${num(maxBack * t)}</text>`)}
    ${months.map((m, i) => h`<g><title>${String(m.month).slice(0, 7)}: opened ${num(m.opened)}, closed ${num(m.closed)}, open at end ${num(m.backlog)}</title>
      <rect x="${padL + bw * i + bw * 0.1}" y="${yF(m.opened)}" width="${bw * 0.38}" height="${H - padB - yF(m.opened)}" style="fill:var(--accent)"/>
      <rect x="${padL + bw * i + bw * 0.52}" y="${yF(m.closed)}" width="${bw * 0.38}" height="${H - padB - yF(m.closed)}" style="fill:var(--ok)"/>
      ${i % Math.ceil(n / 8) === 0 ? h`<text x="${padL + bw * i + bw / 2}" y="${H - padB + 16}" text-anchor="middle" style="fill:var(--muted);font-size:11px">${String(m.month).slice(0, 7)}</text>` : ""}</g>`)}
    <polyline points="${line}" style="fill:none;stroke:var(--warn);stroke-width:2"/>
  </svg></div>
  <div class="small row"><span style="color:var(--accent)">■ opened</span><span style="color:var(--ok)">■ closed</span><span class="muted">(left axis, per month)</span>
    <span style="color:var(--warn)">— open at month end</span><span class="muted">(right axis)</span></div>`;
}

// --- drawers ------------------------------------------------------------------------------------
function openDrawer(title, contentPromise) {
  closeDrawer(true);
  const scrim = document.createElement("div");
  scrim.className = "scrim";
  scrim.dataset.act = "closedrawer";
  const dr = document.createElement("aside");
  dr.className = "drawer";
  dr.innerHTML = h`<header><h2>${title}</h2><button class="btn small" data-act="closedrawer">Close</button></header><div class="body">${loading()}</div>`.s;
  document.body.append(scrim, dr);
  Promise.resolve(contentPromise).then((c) => { $(".body", dr).innerHTML = val(c); })
    .catch((e) => { $(".body", dr).innerHTML = h`<div class="callout bad">${e.message}</div>`.s; });
}
function closeDrawer(force = false) {
  if (!force && $("#rulesyaml") && editor.dirty && !confirm("You have unsaved edits to the rules. Close without saving?")) return;
  editor.dirty = false;
  document.querySelectorAll(".scrim, .drawer").forEach((e) => e.remove());
}

async function traceView(bronzeId) {
  const t = await api(`/runs/${state.runId}/trace/${bronzeId}`);
  const b = t.bronze;
  const s = t.silver;
  return h`<div class="journey-v">
    <div class="hop file stack-sm"><h4>1 · In your file</h4><div class="small muted">Line ${t.file.line} of ${t.file.name} (${t.file.encoding})</div><pre>${t.file.raw}</pre></div>
    <div class="hop bronze stack-sm"><h4>2 · Stored in Bronze, unchanged</h4>
      <div class="small muted">Batch ${b.batch_no} · ${b.parse_status === "ok" ? "parsed cleanly" : `marked broken: ${b.parse_error}`} · fingerprint <span class="mono">${String(b.row_hash).slice(0, 12)}…</span></div>
      <table><tbody>${Object.entries(b.payload).map(([k, v]) => h`<tr><td class="mono small">${k}</td><td>${rv(v)}</td></tr>`)}</tbody></table></div>
    <div class="hop silver stack-sm"><h4>3 · In Silver ${s && s.state !== "approved" ? "(preview of proposed rules)" : ""}</h4>
      ${!s ? h`<p class="muted">${b.parse_status === "ok" ? "Appears here once cleaning rules have been proposed." : "Broken rows are not cleaned automatically; they stay in Bronze for review."}</p>` : h`
        <div class="row">${outcomePill(s.status)}${s.error_labels.map((e) => h`<span class="chip">${e}</span>`)}</div>
        <table><thead><tr><th>Column</th><th>Original</th><th>Result</th><th>What happened</th></tr></thead><tbody>
        ${s.cells.map((c) => h`<tr${c.change !== "same" ? raw(' class="changed"') : ""}><td class="mono small">${c.name}</td><td>${rv(c.before)}</td><td>${c.after === null ? h`<span class="empty-v">empty</span>` : h`<b>${c.after}</b>`}</td>
          <td class="small">${c.flag_labels.length ? c.flag_labels.join("; ") : c.change === "reformatted" ? "Format unified" : h`<span class="faint">unchanged</span>`}
            <details><summary class="small">Rule</summary><ol style="margin:0;padding-left:18px">${c.plain.map((p) => h`<li>${p}</li>`)}</ol></details></td></tr>`)}
        </tbody></table>`}
    </div></div>`;
}

async function columnProfileView(col) {
  const p = await api(`/runs/${state.runId}/profile/${encodeURIComponent(col)}`);
  const c = p.completeness || {};
  const t = p.type_inference || {};
  const top = (p.frequency && p.frequency.top_values) || [];
  const max = Math.max(1, ...top.map((x) => x[1]));
  const nums = p.numeric;
  const dates = p.dates;
  return h`<div class="stack">
    <div class="grid">${kpi(num(c.total), "values")}${kpi(pct(c.empty_or_null_like_share), "empty or placeholder")}${kpi(num(p.cardinality?.distinct), "distinct")}${kpi(typeLabel(t.dominant_type), `type (${pct(t.confidence, 0)} sure)`)}</div>
    ${c.null_like && Object.keys(c.null_like).length ? h`<div class="stack-sm"><h4>Placeholder words found</h4><div class="chips">${Object.entries(c.null_like).map(([k, v]) => h`<span class="chip">'${k}' ×${num(v)}</span>`)}</div></div>` : ""}
    <div class="stack-sm"><h4>Most common values</h4><div class="vbars">${top.map(([v, n]) => h`<div class="vb"><span class="cell">${rv(v)}</span>${bar([[n / max, "accent"]])}<span class="num small">${num(n)}</span></div>`)}</div></div>
    ${p.patterns ? h`<div class="stack-sm"><h4>Shapes of the values</h4><p class="small muted">Letters shown as A, digits as 9.</p><div class="vbars">${p.patterns.top_masks.map(([m, sh]) => h`<div class="vb"><span class="mono cell">${m}</span>${bar([[sh, "info"]])}<span class="num small">${pct(sh)}</span></div>`)}</div></div>` : ""}
    ${t.shares ? h`<div class="stack-sm"><h4>What the values look like</h4><div class="vbars">${Object.entries(t.shares).filter(([, v]) => v > 0).map(([k, v]) => h`<div class="vb"><span>${k}</span>${bar([[v, "ok"]])}<span class="num small">${pct(v)}</span></div>`)}</div></div>` : ""}
    ${nums ? h`<div class="stack-sm"><h4>Numbers</h4><div class="grid">${kpi(num(nums.min), "lowest")}${kpi(num(nums.median), "median")}${kpi(num(Math.round(nums.mean)), "average")}${kpi(num(nums.max), "highest")}</div>
      ${nums.without_sentinels ? h`<div class="small muted">Without placeholder values: average ${num(Math.round(nums.without_sentinels.mean))}, lowest ${num(nums.without_sentinels.min)}.</div>` : ""}</div>` : ""}
    ${dates && dates.formats ? h`<div class="stack-sm"><h4>Date formats</h4><div class="vbars">${dates.formats.map((f) => h`<div class="vb"><span class="mono cell" title="${f.example}">${f.format}</span>${bar([[f.share, "info"]])}<span class="num small">${pct(f.share)}</span></div>`)}</div></div>` : ""}
    ${p.sentinels && p.sentinels.length ? h`<div class="stack-sm"><h4>Placeholder values</h4><div class="chips">${p.sentinels.map((s) => h`<span class="chip">'${s.value}' ×${num(s.count)}</span>`)}</div></div>` : ""}
    ${p.samples ? h`<div class="stack-sm"><h4>Random examples</h4><div class="chips">${p.samples.map((s) => h`<span class="chip">${rv(s)}</span>`)}</div></div>` : ""}
    <details><summary>Full health-check record</summary><pre>${JSON.stringify(p, null, 2)}</pre></details>
  </div>`;
}

const silverColState = { changedOnly: true };
async function silverColumnView(col) {
  const d = await api(`/runs/${state.runId}/silver/columns/${encodeURIComponent(col)}`);
  const rows = d.values.filter((v) => !silverColState.changedOnly || v.change !== "same");
  return h`<div class="stack">
    <div class="grid">${kpi(num(d.distinct_before), "different values before")}${kpi(num(d.distinct_after), "different values after", "ok")}</div>
    <div class="stack-sm"><h4>The rule</h4><ol style="margin:0;padding-left:20px">${d.plain.map((p) => h`<li>${p}</li>`)}</ol><p class="small muted">Why: ${d.why}</p></div>
    <div class="row"><label class="small"><input type="checkbox" data-act="scolchanged" data-col="${col}" ${silverColState.changedOnly ? raw("checked") : ""}> Only values that changed</label></div>
    <div class="table-wrap"><table><thead><tr><th>Original value</th><th>Becomes</th><th>What happened</th><th class="num">Rows</th></tr></thead><tbody>
      ${rows.map((v) => h`<tr><td>${rv(v.before)}</td><td>${v.after === null ? h`<span class="empty-v">empty</span>` : h`<b>${v.after}</b>`}</td><td class="small">${v.flags.length ? v.flags.join("; ") : v.change === "reformatted" ? "format unified" : h`<span class="faint">unchanged</span>`}</td><td class="num">${num(v.count)}</td></tr>`)}
    </tbody></table></div>
    ${d.values.length >= 300 ? h`<p class="small muted">Showing the 300 most common combinations.</p>` : ""}
  </div>`;
}

// --- rules editor -------------------------------------------------------------------------------
const LAYER_HELP = {
  yaml: "The text is not valid YAML (check indentation and quotes).",
  schema: "A field has a wrong name or an allowed value is misspelt.",
  semantic: "A rule points at something that does not exist or does not fit the data.",
  dry_run: "The rules failed when they were tried on sample rows.",
  coverage: "A high or medium issue is no longer handled by any rule (or ignored with a reason).",
};

const FIELD_GUIDE = [
  ["steps", "Tidying, applied in order: trim, collapse_spaces, remove_hidden_chars, lower, upper, title, strip_currency, strip_thousands, strip_percent, strip_units, european_decimal"],
  ["null_tokens", "Words that mean \"empty\", e.g. ['n/a', 'tbd'] (case does not matter)"],
  ["sentinels", "Fake placeholder values to empty and flag, e.g. ['999', '-1']"],
  ["mapping", "Standard value → list of spellings to merge into it, e.g. high: [hi, HIGH]"],
  ["unmapped_action", "keep (default), null or invalid: what happens to values outside the mapping"],
  ["type", "text, integer, numeric, boolean, date or timestamp"],
  ["parse_formats / day_month_order", "Date formats to read (ids as in the health check), and mdy or dmy"],
  ["range", "min and/or max; values outside become empty"],
  ["required / invalid_action", "required: true + invalid_action: reject drops rows with a missing value"],
  ["confidence / why / addresses", "How sure you are (0–1), the business reason (20+ characters), and the issue ids it fixes"],
];

const editor = { dirty: false, saved: false };

function openRulesEditor() {
  editor.dirty = false;
  editor.saved = false;
  openDrawer("Edit the proposed rules", api(`/runs/${state.runId}/rules/yaml`).then((d) => h`<div class="stack">
    ${d.editable ? "" : h`<div class="callout warn">These rules are not waiting for approval any more, so they can no longer be edited.</div>`}
    <p class="small muted">This is the proposal file itself. Change what you need, press <b>Check</b> to see whether it is valid, then <b>Save</b>. Nothing is applied until you press <b>Approve</b> on the approval page.</p>
    <textarea id="rulesyaml" spellcheck="false" style="min-height:52vh" ${d.editable ? "" : raw("readonly")}>${d.text}</textarea>
    <div class="row">
      <button class="btn" data-act="rcheck">Check</button>
      <button class="btn primary" data-act="rsave" ${d.editable ? "" : raw("disabled")}>Save</button>
      ${d.ai_text && d.ai_text !== d.text ? h`<button class="btn danger" data-act="rreset" ${d.editable ? "" : raw("disabled")}>Undo all my edits</button>` : ""}
    </div>
    <div id="rulesresult"></div>
    <details><summary>Field guide</summary><table><tbody>${FIELD_GUIDE.map(([k, v]) => h`<tr><td class="mono small nowrap">${k}</td><td class="small">${v}</td></tr>`)}</tbody></table></details>
  </div>`));
}

async function rulesAction(kind) {
  const ta = $("#rulesyaml");
  const out = $("#rulesresult");
  if (!ta || !out) return;
  out.innerHTML = loading(kind === "check" ? "Checking: format, meaning, a trial on 2,000 sample rows, and issue coverage…" : "Checking and saving…").s;
  try {
    const path = `/runs/${state.runId}/rules/${kind === "check" ? "check" : kind === "save" ? "yaml" : "reset"}`;
    const method = kind === "save" ? "PUT" : "POST";
    const report = await api(path, { method, body: kind === "reset" ? undefined : { text: ta.value } });
    out.innerHTML = reportView(report, kind).s;
    if (report.ok && kind !== "check") {
      editor.dirty = false;
      editor.saved = true;
      if (kind === "reset") ta.value = (await api(`/runs/${state.runId}/rules/yaml`)).text;
      loadPanel(true);
    }
  } catch (e) {
    out.innerHTML = h`<div class="callout bad">${e.message}</div>`.s;
  }
}

function reportView(r, kind) {
  if (!r.ok) {
    return h`<div class="callout bad stack-sm"><b>${kind === "check" ? "Not valid yet" : "Not saved: the rules are not valid"}.</b>
      <div class="small">${LAYER_HELP[r.layer] || ""}</div>
      <ul style="margin:0;padding-left:18px" class="small mono">${r.errors.map((e) => h`<li>${e}</li>`)}</ul></div>`;
  }
  const rows = r.stats && r.stats.rows;
  return h`<div class="callout ok stack-sm"><b>${kind === "check" ? "✓ Valid. Press Save to keep it." : kind === "reset" ? "✓ Back to the AI's proposal." : "✓ Saved. Press Approve on the approval page to use it."}</b>
    ${rows ? h`<div class="small">On ${num(rows.sampled)} sample rows: ${pct(rows.with_errors)} would get a warning, ${num(rows.rejected)} would be rejected.</div>` : ""}
    ${r.warnings.length ? h`<div class="small"><b>Worth a look:</b><ul style="margin:0;padding-left:18px">${r.warnings.map((w) => h`<li>${w}</li>`)}</ul></div>` : ""}</div>`;
}

document.addEventListener("input", (e) => { if (e.target.id === "rulesyaml") editor.dirty = true; });
document.addEventListener("keydown", (e) => {
  if (e.target.id === "rulesyaml" && e.key === "Tab") { // YAML is indented with spaces
    e.preventDefault();
    e.target.setRangeText("  ", e.target.selectionStart, e.target.selectionEnd, "end");
    editor.dirty = true;
  }
});

// --- actions ------------------------------------------------------------------------------------
async function act(fn, okMessage) {
  try {
    await fn();
    if (okMessage) toast(okMessage);
    await refreshNow();
    loadPanel(true);
  } catch (e) {
    toast(e.message, true);
  }
}

const ACTIONS = {
  go: (el) => { location.hash = el.dataset.href; },
  step: (el) => { location.hash = `#/run/${state.runId}/${el.dataset.step}`; },
  closedrawer: () => closeDrawer(),
  trace: (el) => openDrawer("Journey of one record", traceView(el.dataset.id)),
  colprofile: (el) => openDrawer(`Health check: ${el.dataset.col}`, columnProfileView(el.dataset.col)),
  silvercol: (el) => openDrawer(`${el.dataset.col}: before and after`, silverColumnView(el.dataset.col)),
  bfilter: (el) => { bronzeTable.status = el.dataset.v; bronzeTable.offset = 0; el.parentElement.querySelectorAll("button").forEach((b) => b.classList.toggle("on", b === el)); loadBronzeRows(); },
  bpage: (el) => { bronzeTable.offset = Math.max(0, bronzeTable.offset + Number(el.dataset.d) * bronzeTable.limit); loadBronzeRows(); },
  sshow: (el) => { silverTable.show = el.dataset.v; silverTable.offset = 0; el.parentElement.querySelectorAll("button").forEach((b) => b.classList.toggle("on", b === el)); loadSilverRows(); },
  spage: (el) => { silverTable.offset = Math.max(0, silverTable.offset + Number(el.dataset.d) * silverTable.limit); loadSilverRows(); },
  sev: (el) => { profileState.sev = el.dataset.v; loadPanel(true); },
  ctxretry: () => act(() => api(`/runs/${state.runId}/context/retry`, { method: "POST" }), "Asking the AI again"),
  ctxsave: () => act(() => api(`/runs/${state.runId}/context`, { method: "POST", body: { text: $("#ctxtext").value } }), "Description saved"),
  agentstart: () => act(() => api(`/runs/${state.runId}/agent/start`, { method: "POST", body: { fresh: false } }), "AI review started"),
  agentresume: () => act(() => api(`/runs/${state.runId}/agent/start`, { method: "POST", body: { fresh: false } }), "Resuming the AI review"),
  agentfresh: () => {
    if (!confirm("Start a new AI review from scratch? In live mode this calls the AI again and costs money. The current proposal stays on record.")) return;
    act(() => api(`/runs/${state.runId}/agent/start`, { method: "POST", body: { fresh: true } }), "New AI review started");
  },
  editrules: () => openRulesEditor(),
  rcheck: () => rulesAction("check"),
  rsave: () => rulesAction("save"),
  rreset: () => {
    if (!confirm("Throw away all edits and go back to the AI's original proposal?")) return;
    rulesAction("reset");
  },
  rfilter: (el) => { reviewState.filter = el.dataset.v; loadPanel(true); },
  ruleapprove: (el) => {
    const it = reviewState.items[Number(el.dataset.i)];
    const left = (it && it.kind === "column" && reviewState.leftovers[it.rule.name]) || [];
    if (left.length) {
      const rows = left.reduce((n, l) => n + l.count, 0);
      const list = left.slice(0, 6).map((l) => `“${l.value}” (${num(l.count)})`).join(", ");
      if (!confirm(`${num(rows)} rows keep values outside the standard list of ${it.rule.name}: ${list}${left.length > 6 ? ", …" : ""}.\n\nThey will be counted as separate values. Approve anyway?`)) return;
    }
    ruleDecision(Number(el.dataset.i), "approve");
  },
  applyvalues: (el) => applyValues(Number(el.dataset.i)),
  aimapapply: (el) => applyAiMapChanges(Number(el.dataset.i)),
  buildgold: () => act(() => api(`/runs/${state.runId}/gold/build`, { method: "POST" }), "Building the business tables…"),
  buildsilver: () => act(() => api(`/runs/${state.runId}/silver/build`, { method: "POST" }), "Building the cleaned table…"),
  automap: () => act(() => api(`/runs/${state.runId}/rules/automap`, { method: "POST" }), "The AI is deciding the leftover values…"),
  rmvalue: async (el) => {
    if (!confirm(`Stop merging “${el.dataset.value}” into “${el.dataset.target}”? It will stay as a separate value (you can choose again below).`)) return;
    try {
      await api(`/runs/${state.runId}/rules/values`, { method: "POST", body: { column: el.dataset.col, choices: [{ value: el.dataset.value, action: "remove", target: el.dataset.target }] } });
      await refreshNow();
      loadPanel(true);
    } catch (e) { toast(e.message, true); }
  },
  reopen: () => {
    if (!confirm("Reopen the review? The approval is withdrawn (a copy is kept) until you finish the review again.")) return;
    act(() => api(`/runs/${state.runId}/rules/reopen`, { method: "POST" }), "Review reopened");
  },
  rulerejectopen: (el) => { reviewState.rejecting = Number(el.dataset.i); loadPanel(true).then(() => $(`#rej-${el.dataset.i}`)?.focus()); },
  rulerejectcancel: () => { reviewState.rejecting = null; loadPanel(true); },
  rulereject: (el) => {
    const reason = ($(`#rej-${el.dataset.i}`)?.value || "").trim();
    if (reason.length < 10) { toast("Please say why you reject it (at least 10 characters).", true); return; }
    ruleDecision(Number(el.dataset.i), "reject", reason);
  },
  ruleundo: (el) => ruleDecision(Number(el.dataset.i), "undo"),
  finishreview: () => {
    if (!confirm("Finish the review? The approved rules (and safe defaults for rejected ones) are saved and used to build the cleaned data.")) return;
    act(() => api(`/runs/${state.runId}/rules/finish`, { method: "POST" }), "Finishing the review…");
  },
};

function onClick(e) {
  const el = e.target.closest("[data-act]");
  if (!el || el.type === "checkbox" || el.tagName === "SELECT") return;
  const fn = ACTIONS[el.dataset.act];
  if (!fn) return;
  if (el.tagName === "A" && el.getAttribute("href")) return; // plain links navigate themselves
  e.preventDefault();
  fn(el, e);
}

function onChange(e) {
  if (e.target.id === "scol") { silverTable.column = e.target.value; silverTable.offset = 0; loadSilverRows(); }
  if (e.target.dataset.act === "scolchanged") {
    silverColState.changedOnly = e.target.checked;
    const col = e.target.dataset.col;
    silverColumnView(col).then((c) => { const body = $(".drawer .body"); if (body) body.innerHTML = val(c); });
  }
}

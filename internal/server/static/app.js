// Furcate UI: hash-routed views over the JSON API in internal/server. Plain JS,
// no dependencies, no requests beyond this origin.
'use strict';

const app = document.getElementById('app');
// A snapshot (furcate snapshot) is this UI plus the API's answers as files, for a
// static host. Its index.html says so, and api() reads the files instead.
const STATIC = document.querySelector('meta[name="furcate-static"]') !== null;
let seq = 0; // bumped on every navigation; async work for an older view drops its result
let info = { fork_enabled: false, max_n: 100 };
const payloads = new Map(); // sha256 -> Promise<string>; payloads are immutable
const prefs = { since: '7d', filter: '', trackedOnly: false };

// ---------- helpers ----------

// h builds an element. Props: class, text, on<event>, anything else is an attribute.
function h(tag, props, ...kids) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(props || {})) {
    if (v == null || v === false) continue;
    if (k === 'class') el.className = v;
    else if (k === 'text') el.textContent = v;
    else if (k.startsWith('on')) el.addEventListener(k.slice(2), v);
    else el.setAttribute(k, v === true ? '' : String(v));
  }
  for (const c of kids.flat()) {
    if (c != null && c !== false) el.append(c instanceof Node ? c : String(c));
  }
  return el;
}

async function api(path, opts) {
  if (STATIC) {
    if (path.startsWith('/api/compare?')) return staticCompare(new URLSearchParams(path.split('?')[1]));
    path = staticPath(path);
  }
  const res = await fetch(path, opts);
  const json = (res.headers.get('Content-Type') || '').includes('application/json');
  const body = json ? await res.json() : await res.text();
  if (!res.ok) throw new Error((json && body.error) || `${res.status} ${res.statusText}`);
  return body;
}

// staticPath maps an API path to its file in a snapshot. Relative, so a snapshot
// works under any base path.
function staticPath(path) {
  const p = path.split('?')[0].replace(/^\//, '');
  if (p === 'api/runs') return 'api/runs.json';
  if (p.startsWith('api/payloads/')) return `${p}.txt`;
  return `${p}.json`;
}

// staticCompare answers GET /api/compare in a snapshot, which can't hold every
// pair: both runs' rows, and their end-of-run manifests diffed as the server does.
async function staticCompare(q) {
  const load = async id => {
    const d = await api(`/api/runs/${enc(id)}`);
    if (!d.run) throw new Error(`run ${id} not found in the run store`);
    return [d.run, await api(`/api/files/${enc(id)}`)];
  };
  const [[a, fa], [b, fb]] = await Promise.all([load(q.get('a')), load(q.get('b'))]);
  const files = { available: fa != null && fb != null, a: fa ? Object.keys(fa).length : 0, b: fb ? Object.keys(fb).length : 0,
    same: 0, changed: [], only_a: [], only_b: [] };
  if (files.available) {
    for (const p of Object.keys(fa).sort()) {
      if (!(p in fb)) files.only_a.push(p);
      else if (fb[p] !== fa[p]) files.changed.push(p);
      else files.same++;
    }
    for (const p of Object.keys(fb).sort()) if (!(p in fa)) files.only_b.push(p);
  }
  return { a, b, files };
}

const enc = encodeURIComponent;
const safeDecode = s => { try { return decodeURIComponent(s); } catch { return ''; } };
const sleep = ms => new Promise(r => setTimeout(r, ms));
const live = my => my === seq;
const short = id => (id && id.length > 12 ? id.slice(0, 12) : id || '–');
const shortCp = id => (id.length > 11 ? id.slice(0, 11) + '…' : id);
const kindCls = k => (['llm', 'tool', 'agent'].includes(k) ? k : 'other');

const fmt = {
  dur(ms) {
    if (ms == null) return '–';
    if (ms < 1) return `${Math.round(ms * 1000)}µs`;
    if (ms < 1000) return `${Math.round(ms)}ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(2)}s`;
    return `${Math.floor(ms / 60000)}m${String(Math.round((ms % 60000) / 1000)).padStart(2, '0')}s`;
  },
  cost: usd => (usd ? `$${usd.toFixed(usd < 1 ? 4 : 2)}` : '–'),
  num: n => (n == null ? '–' : Number(n).toLocaleString()),
  pct: x => `${Math.round(x * 100)}%`,
  bytes: n => (n < 1024 ? `${n} B` : n < 1 << 20 ? `${(n / 1024).toFixed(1)} KB` : `${(n / (1 << 20)).toFixed(1)} MB`),
  time: iso => new Date(iso).toLocaleString(undefined, {
    month: 'short', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
  }),
};

function pretty(text) {
  try { return JSON.stringify(JSON.parse(text), null, 2); } catch { return text; }
}

function badge(text, cls) { return h('span', { class: `badge ${cls}`, text }); }

// resultBadge works for list rows, run rows, branch rows and job branches.
function resultBadge(r) {
  if (r.outcome) return badge(r.outcome, r.outcome === 'pass' ? 'pass' : 'fail');
  if (r.status === 'error') return badge('error', 'error');
  if (r.status === 'running') return badge('running', 'running');
  if (r.status) return badge(r.status, 'neutral');
  if (r.span_status === 'error') return badge('error', 'error');
  return badge(r.span_status || 'unknown', 'neutral');
}

function errorBox(err) {
  return h('div', { class: 'error-box', role: 'alert' }, h('strong', {}, 'Error: '), err.message || String(err));
}

function render(my, ...nodes) {
  if (!live(my)) return false;
  app.replaceChildren(...nodes);
  return true;
}

// ---------- router ----------

function route() {
  const my = ++seq;
  const [view, a, b] = location.hash.replace(/^#\/?/, '').split('/').map(safeDecode);
  for (const l of document.querySelectorAll('[data-nav]')) {
    if (l.dataset.nav === (view || 'runs')) l.setAttribute('aria-current', 'page');
    else l.removeAttribute('aria-current');
  }
  let p;
  if (!view || view === 'runs') p = viewRuns(my);
  else if (view === 'run' && a) p = viewRun(my, a);
  else if (view === 'compare' && a && b) p = viewCompare(my, a, b);
  else p = Promise.resolve(render(my, h('p', {}, 'No such page. ', h('a', { href: '#/runs' }, 'Back to runs'))));
  p.catch(err => render(my, errorBox(err)));
}

// ---------- runs list ----------

async function viewRuns(my) {
  document.title = 'Runs · Furcate';
  const tbody = h('tbody');
  const count = h('span', { class: 'muted' });
  let runs = [];

  const matches = r => {
    if (prefs.trackedOnly && !r.tracked) return false;
    const q = prefs.filter.trim().toLowerCase();
    return !q || [r.id, r.name, r.service, r.parent_run_id].some(s => s && s.toLowerCase().includes(q));
  };
  const fill = () => {
    const shown = runs.filter(matches);
    count.textContent = `${shown.length} of ${runs.length}`;
    tbody.replaceChildren(...(shown.length ? shown.map(runRow)
      : [h('tr', {}, h('td', { colspan: 9, class: 'muted' }, runs.length ? 'No runs match the filter.' : 'No finished runs in this window.'))]));
  };
  const load = async () => {
    tbody.replaceChildren(h('tr', {}, h('td', { colspan: 9, class: 'muted' }, 'Loading…')));
    try {
      const data = await api(`/api/runs?since=${enc(prefs.since)}&limit=200`);
      if (!live(my)) return;
      runs = data.runs;
      fill();
    } catch (err) {
      if (live(my)) tbody.replaceChildren(h('tr', {}, h('td', { colspan: 9 }, errorBox(err))));
    }
  };

  const windows = [['1h', 'Last hour'], ['24h', 'Last 24 hours'], ['7d', 'Last 7 days'], ['30d', 'Last 30 days']];
  const cols = [['Started'], ['Run'], ['Name'], ['Duration', 'num'], ['Spans', 'num opt'], ['Tokens', 'num opt'],
    ['Cost', 'num'], ['Result'], ['Branches']];
  render(my, h('section', { class: 'panel' },
    h('div', { class: 'toolbar' },
      h('h1', {}, 'Runs'), count, h('span', { class: 'spacer' }),
      h('input', { type: 'search', placeholder: 'Filter by id, name, parent…', 'aria-label': 'Filter runs', value: prefs.filter,
        oninput: e => { prefs.filter = e.target.value; fill(); } }),
      STATIC ? null : h('select', { 'aria-label': 'Time window', onchange: e => { prefs.since = e.target.value; load(); } },
        windows.map(([v, t]) => h('option', { value: v, selected: v === prefs.since }, t))),
      h('label', { class: 'check', title: 'Hide traces with no run-store row, such as loadgen runs' },
        h('input', { type: 'checkbox', checked: prefs.trackedOnly, onchange: e => { prefs.trackedOnly = e.target.checked; fill(); } }),
        'Agent runs only'),
      STATIC ? null : h('button', { type: 'button', onclick: load }, 'Refresh')),
    h('div', { class: 'table-wrap' }, h('table', { class: 'runs' },
      h('thead', {}, h('tr', {}, cols.map(([t, cls]) => h('th', { class: cls, scope: 'col' }, t)))),
      tbody))));
  await load();
}

function runRow(r) {
  const href = `#/run/${enc(r.id)}`;
  const markers = h('span', { class: 'markers' },
    r.parent_run_id && h('a', { class: 'marker', href: `#/run/${enc(r.parent_run_id)}`,
      title: `Branch of ${r.parent_run_id} at step ${r.parent_step}: ${r.change_label || ''}` },
    `↳ ${short(r.parent_run_id)} @${r.parent_step}`),
    r.branches > 0 && h('span', { class: 'marker fork', title: 'Runs forked from this one' },
      `${r.branches} branch${r.branches === 1 ? '' : 'es'}`),
    r.eval && h('span', { class: 'marker', title: `eval case ${r.eval.case} ${r.eval.variant || ''}` }, `eval ${r.eval.case}`));
  return h('tr', { class: 'clickable', onclick: e => { if (!e.target.closest('a')) location.hash = href; } },
    h('td', { class: 'nowrap' }, fmt.time(r.start)),
    h('td', {}, h('a', { href, class: 'mono', title: r.id }, short(r.id))),
    h('td', { class: 'name', title: r.name }, r.name),
    h('td', { class: 'num' }, fmt.dur(r.duration_ms)),
    h('td', { class: 'num opt' }, fmt.num(r.spans)),
    h('td', { class: 'num opt' }, fmt.num(r.tokens)),
    h('td', { class: 'num' }, fmt.cost(r.cost_usd)),
    h('td', { class: 'nowrap' }, resultBadge(r),
      r.errors > 0 ? h('span', { class: 'muted small' }, ` ${r.errors} err span${r.errors === 1 ? '' : 's'}`) : null),
    h('td', {}, markers));
}

// ---------- run view ----------

async function viewRun(my, id) {
  document.title = `Run ${short(id)} · Furcate`;
  render(my, h('p', { class: 'muted' }, 'Loading run…'));
  const d = await api(`/api/runs/${enc(id)}`);
  if (!live(my)) return;
  const branches = h('section', { class: 'panel', 'aria-label': 'Branches' });
  const reload = () => loadBranches(my, d, branches);
  render(my, runHeader(d), d.run ? branches : null, tracePanel(d, reload));
  if (d.run) reload();
}

function runHeader(d) {
  const r = d.run, t = d.trace;
  const root = t && t.roots[0];
  const facts = h('dl', { class: 'facts' });
  const add = (k, v) => facts.append(h('div', { class: 'fact' }, h('dt', {}, k), h('dd', {}, v)));
  add('Run', h('span', { class: 'mono' }, d.id));
  if (r) { add('Task', r.task_id || '–'); add('Driver', r.driver || '–'); }
  add('Started', fmt.time(r ? r.started : t.start));
  add('Duration', fmt.dur(r && r.duration_ms != null ? r.duration_ms : t && t.duration_ms));
  if (t) add('Spans', `${t.spans} · ${t.totals.llm_calls} LLM · ${t.totals.tool_calls} tool`);
  if (r) {
    const u = r.usage;
    add('LLM calls', fmt.num(u.llm_calls));
    add('Tokens', `${fmt.num(u.input_tokens)} in · ${fmt.num(u.cache_read_tokens)} cache read · ${fmt.num(u.output_tokens)} out`);
    add('Cost', fmt.cost(u.cost_usd));
    if (r.final_files != null) add('Final files', fmt.num(r.final_files));
  } else if (t) {
    add('Tokens', `${fmt.num(t.totals.tokens_in)} in · ${fmt.num(t.totals.tokens_out)} out`);
    add('Cost', fmt.cost(t.totals.cost_usd));
  }
  const title = (root && root.name) || (r && r.task_id) || 'Run';
  return h('section', { class: 'panel head' },
    h('div', { class: 'title' }, h('h1', {}, title), r ? resultBadge(r) : resultBadge({ span_status: root && root.status }),
      r && r.eval ? h('span', { class: 'marker' }, `eval ${r.eval.case} ${r.eval.variant || ''}`) : null),
    r && r.parent_run_id ? h('p', {},
      'Branch of ', h('a', { href: `#/run/${enc(r.parent_run_id)}`, class: 'mono' }, short(r.parent_run_id)),
      ` at step ${r.parent_step} · change `, h('code', {}, r.change_label || 'none'), ' · ',
      h('a', { href: `#/compare/${enc(r.parent_run_id)}/${enc(d.id)}` }, 'compare with parent')) : null,
    r ? null : h('p', { class: 'muted' }, 'Trace only: this run has no row in the run store, so it has no checkpoints to fork.'),
    r && r.error ? h('p', { class: 'error-msg' }, r.error) : null,
    facts,
    r && r.task ? h('details', { class: 'task' }, h('summary', {}, 'Task spec'),
      h('pre', { class: 'code' }, JSON.stringify(r.task, null, 2))) : null);
}

function tracePanel(d, onForked) {
  const head = h('div', { class: 'panel-head trace-head' }, h('h2', {}, 'Trace'), h('span', { class: 'spacer' }),
    h('span', { class: 'legend', 'aria-hidden': 'true' },
      ['llm', 'tool', 'agent'].map(k => h('span', {}, h('i', { class: k }), k)), h('span', {}, h('i', { class: 'err' }), 'error')));
  if (!d.trace) {
    return h('section', { class: 'panel' }, head, h('p', { class: 'empty' }, STATIC ? 'No spans were recorded for this run.'
      : 'No spans in the trace store yet. They reach ClickHouse through the collector a few seconds after each step.'));
  }
  const total = d.trace.duration_ms || 1;
  const steps = new Map(d.steps.map(s => [s.span_id, s]));
  const ticks = [0, 0.25, 0.5, 0.75, 1].map(f => {
    const t = h('span', { class: 'tick' }, fmt.dur(total * f));
    t.style.left = `${f * 100}%`;
    return t;
  });
  const rows = [h('div', { class: 'trow thead' },
    h('div', {}, 'Span'), h('div', { class: 'c-step' }, 'Step'), h('div', { class: 'c-cp' }, 'Checkpoint'),
    h('div', {}), h('div', { class: 'c-dur' }, 'Duration'), h('div', { class: 'c-wf axis' }, ticks))];
  const walk = (nodes, depth) => {
    for (const n of nodes) {
      rows.push(...spanRow(n, depth, total, d, steps.get(n.span_id), onForked));
      walk(n.children || [], depth + 1);
    }
  };
  walk(d.trace.roots, 0);
  const t = d.trace.totals;
  head.insertBefore(h('span', { class: 'muted' },
    `${d.trace.spans} spans · ${fmt.dur(d.trace.duration_ms)}${t.errors ? ` · ${t.errors} with error status` : ''}`), head.children[1]);
  return h('section', { class: 'panel', 'aria-label': 'Trace' }, head, h('div', {}, rows));
}

// spanRow returns the row plus a slot under it for the span's detail or fork form.
function spanRow(n, depth, total, d, step, onForked) {
  const err = n.status === 'error';
  const slot = h('div', { class: 'slot' });
  let open = null; // 'detail' | 'fork' | null
  const row = h('div', { class: 'trow' });
  const toggle = what => {
    open = open === what ? null : what;
    slot.replaceChildren();
    if (open === 'detail') slot.append(spanDetail(n, step));
    if (open === 'fork') slot.append(forkForm(n, d, onForked));
    row.classList.toggle('selected', open !== null);
    nameBtn.setAttribute('aria-expanded', String(open === 'detail'));
  };

  const nameBtn = h('button', { type: 'button', class: 'span-name', title: n.name, 'aria-expanded': 'false', onclick: () => toggle('detail') },
    h('span', { class: `kind ${kindCls(n.kind)}` }, n.kind),
    h('span', { class: 'nm' }, n.name),
    n.model && !n.name.includes(n.model) ? h('span', { class: 'model' }, n.model) : null,
    err ? h('span', { class: 'errmark', title: n.status_message || 'error', 'aria-label': 'error' }, '!') : null);
  const label = h('div', { class: 'c-label' }, nameBtn);
  label.style.paddingLeft = `${6 + depth * 14}px`;

  const fork = h('div', { class: 'c-fork' });
  if (n.forkable) {
    fork.append(h('button', {
      type: 'button', class: 'btn small', disabled: !info.fork_enabled, onclick: () => toggle('fork'),
      'aria-label': `Fork from here (step ${n.step})`,
      title: info.fork_enabled ? null : STATIC ? 'A snapshot of recorded runs: forking needs furcate serve with a sandbox driver'
        : 'Read-only: start furcate serve with -driver to fork',
    }, 'Fork from here'));
  } else if (step && step.kind === 'llm' && step.checkpoint && step.checkpoint.deleted) {
    fork.append(h('span', { class: 'muted small', title: 'This checkpoint was garbage-collected' }, 'checkpoint gone'));
  }

  const bar = h('div', { class: `bar ${kindCls(n.kind)}${err ? ' err' : ''}`,
    title: `${n.name}: starts +${fmt.dur(n.offset_ms)}, takes ${fmt.dur(n.duration_ms)}${err ? ', error' : ''}` });
  const left = Math.min(Math.max((n.offset_ms / total) * 100, 0), 100);
  bar.style.left = `${left}%`;
  bar.style.width = `${Math.min(Math.max((n.duration_ms / total) * 100, 0.15), 100 - left)}%`;

  row.append(label,
    h('div', { class: 'c-step num' }, n.step ? String(n.step) : ''),
    h('div', { class: 'c-cp mono', title: n.checkpoint_id || '' }, n.checkpoint_id ? shortCp(n.checkpoint_id) : ''),
    fork,
    h('div', { class: 'c-dur num' }, fmt.dur(n.duration_ms)),
    h('div', { class: 'c-wf' }, bar));
  row.addEventListener('click', e => { if (!e.target.closest('button, a, input, textarea, select')) toggle('detail'); });
  return [row, slot];
}

const PAYLOAD_LABELS = {
  'furcate.llm.request': 'Request', 'furcate.llm.response': 'Response',
  'gen_ai.tool.call.arguments': 'Tool args', 'gen_ai.tool.call.result': 'Tool result',
  'gen_ai.input.messages': 'Input messages', 'gen_ai.output.messages': 'Output messages',
  'gen_ai.system_instructions': 'System', 'gen_ai.tool.definitions': 'Tool definitions',
  'gen_ai.prompt': 'Prompt', 'gen_ai.completion': 'Completion',
};
const payloadOrder = k => { const i = Object.keys(PAYLOAD_LABELS).indexOf(k); return i < 0 ? 99 : i; };

function spanDetail(n, step) {
  const facts = h('dl', { class: 'facts compact' });
  const add = (k, v) => { if (v != null && v !== '') facts.append(h('div', { class: 'fact' }, h('dt', {}, k), h('dd', {}, v))); };
  add('Span', h('span', { class: 'mono' }, n.span_id));
  add('Kind', n.kind);
  add('Model', n.model);
  add('Start', `+${fmt.dur(n.offset_ms)}`);
  add('Duration', fmt.dur(n.duration_ms));
  if (n.tokens_in || n.tokens_out) add('Tokens', `${fmt.num(n.tokens_in)} in · ${fmt.num(n.tokens_out)} out`);
  if (n.cost_usd) add('Cost', fmt.cost(n.cost_usd));
  add('Status', n.status);
  if (n.step) add('Step', String(n.step));
  if (n.checkpoint_id) {
    const cp = step && step.checkpoint;
    add('Checkpoint', h('span', {}, h('span', { class: 'mono' }, n.checkpoint_id),
      cp ? ` · ${cp.driver} level ${cp.level}${cp.size_bytes ? ` · ${fmt.bytes(cp.size_bytes)}` : ''}` +
        `${cp.pinned ? ' · pinned' : ''}${cp.deleted ? ' · garbage-collected' : ''}` : ''));
  }
  const keys = Object.keys(n.payloads || {}).sort((a, b) => payloadOrder(a) - payloadOrder(b) || a.localeCompare(b));
  const attrs = Object.entries(n.attributes || {}).sort(([a], [b]) => a.localeCompare(b));
  return h('div', { class: 'detail' }, facts,
    n.status_message ? h('p', { class: 'error-msg' }, n.status_message) : null,
    n.orphan ? h('p', { class: 'muted' }, 'This span’s parent never arrived.') : null,
    keys.length ? payloadViewer(n.payloads, keys) : h('p', { class: 'muted small' }, 'No payloads recorded on this span.'),
    attrs.length ? h('details', { class: 'attrs' }, h('summary', {}, `Attributes (${attrs.length})`),
      h('table', { class: 'kv' }, h('tbody', {}, attrs.map(([k, v]) => h('tr', {}, h('th', { class: 'mono' }, k), h('td', { class: 'mono' }, v)))))) : null);
}

function loadPayload(hash) {
  if (!payloads.has(hash)) {
    payloads.set(hash, api(`/api/payloads/${enc(hash)}`).catch(err => { payloads.delete(hash); throw err; }));
  }
  return payloads.get(hash);
}

function payloadViewer(hashes, keys) {
  const tabs = h('div', { class: 'tabs', role: 'tablist', 'aria-label': 'Payloads' });
  const meta = h('span', { class: 'muted small' });
  const pre = h('pre', { class: 'code', tabindex: '0', role: 'tabpanel' });
  let current = null;
  const show = async key => {
    current = key;
    for (const b of tabs.children) b.setAttribute('aria-selected', String(b.dataset.key === key));
    const hash = hashes[key];
    meta.textContent = `sha256 ${hash.slice(0, 12)}…`;
    pre.textContent = 'Loading…';
    try {
      const text = await loadPayload(hash);
      if (current !== key) return;
      pre.textContent = pretty(text);
      meta.textContent = `${fmt.bytes(new Blob([text]).size)} · sha256 ${hash.slice(0, 12)}…`;
    } catch (err) {
      if (current === key) pre.textContent = `Could not load this payload: ${err.message}`;
    }
  };
  for (const k of keys) {
    tabs.append(h('button', { type: 'button', role: 'tab', class: 'tab', 'data-key': k, title: k, onclick: () => show(k) },
      PAYLOAD_LABELS[k] || k));
  }
  const copy = h('button', { type: 'button', class: 'btn small', onclick: async () => {
    try { await navigator.clipboard.writeText(pre.textContent); copy.textContent = 'Copied'; } catch { copy.textContent = 'Copy failed'; }
    setTimeout(() => { copy.textContent = 'Copy'; }, 1200);
  } }, 'Copy');
  show(keys[0]);
  return h('div', { class: 'payloads' }, h('div', { class: 'payload-bar' }, tabs, h('span', { class: 'spacer' }), meta, copy), pre);
}

// ---------- fork ----------

function forkForm(n, d, onForked) {
  const count = h('input', { type: 'number', min: 1, max: info.max_n, value: 5, required: true });
  const model = h('input', { type: 'text', spellcheck: 'false', placeholder: `unchanged (${n.model || 'parent model'})` });
  const sys = h('textarea', { rows: 2, placeholder: 'e.g. Never delete test fixtures.' });
  const override = h('input', { type: 'checkbox' });
  const toolOut = h('textarea', { rows: 2, disabled: true, placeholder: 'Replaces the output of the last tool call before this step' });
  override.addEventListener('change', () => { toolOut.disabled = !override.checked; if (override.checked) toolOut.focus(); });
  const status = h('div', { class: 'fork-status', role: 'status', 'aria-live': 'polite' });
  const submit = h('button', { type: 'submit', class: 'btn primary' }, 'Start fork');

  const onSubmit = async e => {
    e.preventDefault();
    const change = {};
    if (model.value.trim()) change.model = model.value.trim();
    if (sys.value.trim()) change.append_system = sys.value.trim();
    if (override.checked) change.tool_output = toolOut.value;
    submit.disabled = true;
    status.replaceChildren('Starting…');
    try {
      const job = await api(`/api/runs/${enc(d.id)}/fork`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ step: n.step, n: Number(count.value), change }),
      });
      await pollJob(job, status, onForked);
    } catch (err) {
      status.replaceChildren(h('span', { class: 'error-msg' }, err.message));
    }
    submit.disabled = false;
  };

  return h('form', { class: 'fork-form', onsubmit: onSubmit },
    h('h3', {}, `Fork from step ${n.step}`, h('span', { class: 'muted' }, ` · restores ${n.checkpoint_id} and resends this request`)),
    h('label', {}, 'Continuations (N)', count),
    h('label', {}, 'Model', model),
    h('label', { class: 'wide' }, 'Append system instruction', sys),
    h('div', { class: 'wide' }, h('label', { class: 'check' }, override, 'Override the last tool output'), toolOut),
    h('div', { class: 'wide row' }, submit, h('span', { class: 'muted small' }, 'Each branch is a new run; the result is a pass rate with a 95% CI.')),
    status);
}

async function pollJob(job, status, onForked) {
  const my = seq;
  for (;;) {
    status.replaceChildren(jobSummary(job));
    if (job.status !== 'running' || !live(my)) break;
    await sleep(1500);
    job = await api(`/api/jobs/${enc(job.id)}`);
  }
  if (job.status === 'done' && live(my)) onForked();
}

function jobSummary(j) {
  if (j.status === 'running') {
    return h('span', {}, h('span', { class: 'spinner', 'aria-hidden': 'true' }),
      ` ${j.n} branch${j.n === 1 ? '' : 'es'} from step ${j.step} running · ${fmt.dur(j.elapsed_ms)} · job ${j.id}`);
  }
  if (j.status === 'error') return h('span', { class: 'error-msg' }, `Fork failed: ${j.error}`);
  const r = j.result;
  return h('div', {},
    h('div', {}, h('strong', {}, `Passed ${r.passes} of ${r.done}`),
      r.done ? ` · ${fmt.pct(r.rate)} · 95% CI ${fmt.pct(r.lo)}–${fmt.pct(r.hi)}` : '',
      ` · ${fmt.cost(r.usage.cost_usd)} · ${fmt.dur(j.elapsed_ms)}`, r.errors ? ` · ${r.errors} errored` : ''),
    h('ul', { class: 'branch-list' }, r.branches.map(b => h('li', {},
      b.run_id ? h('a', { href: `#/run/${enc(b.run_id)}`, class: 'mono' }, short(b.run_id)) : '–', ' ', resultBadge(b),
      b.error ? h('span', { class: 'error-msg small' }, ` ${b.error}`) : null))));
}

// ---------- branches ----------

async function loadBranches(my, d, panel) {
  let data;
  try {
    data = await api(`/api/runs/${enc(d.id)}/branches`);
  } catch (err) {
    if (live(my)) panel.replaceChildren(errorBox(err));
    return;
  }
  if (!live(my)) return;
  const picked = new Set();
  const compare = h('button', { type: 'button', class: 'btn small', disabled: true,
    onclick: () => { const [a, b] = picked; location.hash = `#/compare/${enc(a)}/${enc(b)}`; } }, 'Compare 2 selected');
  const head = h('div', { class: 'panel-head' }, h('h2', {}, 'Branches'),
    h('span', { class: 'muted' }, data.total ? `${data.total} run${data.total === 1 ? '' : 's'} forked from this one, by step and change` : ''),
    h('span', { class: 'spacer' }), data.total > 1 ? compare : null);
  if (!data.total) {
    panel.replaceChildren(head, h('p', { class: 'empty' }, info.fork_enabled
      ? 'No branches yet. Use “Fork from here” on an LLM step to run N continuations with a change.'
      : 'No branches yet. Fork with furcate fork, or start furcate serve with -driver to fork from here.'));
    return;
  }
  const pick = (box, id) => {
    if (box.checked && picked.size >= 2) { box.checked = false; return; }
    if (box.checked) picked.add(id); else picked.delete(id);
    compare.disabled = picked.size !== 2;
  };
  panel.replaceChildren(head, h('div', { class: 'groups' }, data.groups.map(g => branchGroup(g, d.id, pick))));
}

function branchGroup(g, parentId, pick) {
  const bar = h('span', { class: 'cibar', role: 'img',
    'aria-label': g.done ? `pass rate ${fmt.pct(g.rate)}, 95% CI ${fmt.pct(g.lo)} to ${fmt.pct(g.hi)}` : 'no finished branches' });
  const ci = h('span', { class: 'ci' });
  ci.style.left = `${g.lo * 100}%`;
  ci.style.width = `${(g.hi - g.lo) * 100}%`;
  bar.append(ci);
  if (g.done) {
    const mark = h('span', { class: 'rate' });
    mark.style.left = `${g.rate * 100}%`;
    bar.append(mark);
  }
  const parts = [`${g.passes}/${g.done} passed`];
  if (g.done) parts.push(fmt.pct(g.rate), `CI ${fmt.pct(g.lo)}–${fmt.pct(g.hi)}`);
  if (g.errors) parts.push(`${g.errors} error${g.errors === 1 ? '' : 's'}`);
  if (g.running) parts.push(`${g.running} running`);
  // Eval variants replace the whole system prompt, so their labels start alike: name the variant.
  const variants = new Set(g.runs.map(r => (r.eval && r.eval.variant) || ''));
  const variant = variants.size === 1 ? [...variants][0] : '';
  return h('details', { class: 'bgroup' },
    h('summary', {}, h('span', { class: 'bstep' }, `step ${g.step}`),
      h('code', { class: 'blabel', title: JSON.stringify(g.change) }, variant ? `eval ${variant}: ${g.label}` : g.label), bar,
      h('span', { class: 'bstats' }, parts.join(' · '))),
    h('ul', { class: 'branch-runs' }, g.runs.map(r => {
      const box = h('input', { type: 'checkbox', 'aria-label': `Select ${r.id} to compare` });
      box.addEventListener('change', () => pick(box, r.id));
      return h('li', {}, box,
        h('a', { href: `#/run/${enc(r.id)}`, class: 'mono', title: r.id }, short(r.id)),
        h('span', {}, resultBadge(r)),
        h('span', { class: 'num muted' }, fmt.dur(r.duration_ms)),
        h('span', { class: 'num muted' }, fmt.cost(r.cost_usd)),
        h('a', { class: 'small', href: `#/compare/${enc(parentId)}/${enc(r.id)}` }, 'vs parent'),
        r.error ? h('span', { class: 'error-msg small', title: r.error }, r.error) : h('span', {}));
    })));
}

// ---------- compare ----------

async function viewCompare(my, a, b) {
  document.title = 'Compare · Furcate';
  render(my, h('p', { class: 'muted' }, 'Loading comparison…'));
  const d = await api(`/api/compare?a=${enc(a)}&b=${enc(b)}`);
  if (!live(my)) return;
  const A = d.a, B = d.b, f = d.files;
  const origin = r => (r.parent_run_id
    ? h('span', {}, 'branch of ', h('a', { href: `#/run/${enc(r.parent_run_id)}`, class: 'mono' }, short(r.parent_run_id)),
      ` at step ${r.parent_step}: `, h('code', {}, r.change_label || 'none'))
    : 'original');
  const rows = [
    ['Origin', origin(A), origin(B), false],
    ['Result', resultBadge(A), resultBadge(B), (A.outcome || A.status) !== (B.outcome || B.status)],
  ];
  const val = (label, get, show = fmt.num) => rows.push([label, show(get(A)), show(get(B)), get(A) !== get(B)]);
  val('LLM calls', r => r.usage.llm_calls);
  val('Input tokens (uncached)', r => r.usage.input_tokens);
  val('Cache read tokens', r => r.usage.cache_read_tokens);
  val('Cache write tokens', r => r.usage.cache_write_tokens);
  val('Output tokens', r => r.usage.output_tokens);
  val('Cost', r => r.usage.cost_usd, fmt.cost);
  val('Duration', r => r.duration_ms, fmt.dur);
  val('Files at the end', r => r.final_files);

  const fileItems = [
    ...f.changed.map(p => h('li', { class: 'changed' }, h('span', { class: 'mk', title: 'differs' }, '~'), p)),
    ...f.only_a.map(p => h('li', { class: 'only-a' }, h('span', { class: 'mk', title: 'only in A' }, '-'), p, h('span', { class: 'muted' }, '  only in A'))),
    ...f.only_b.map(p => h('li', { class: 'only-b' }, h('span', { class: 'mk', title: 'only in B' }, '+'), p, h('span', { class: 'muted' }, '  only in B'))),
  ];
  const runHead = (tag, r) => h('th', { scope: 'col' }, `${tag} `, h('a', { href: `#/run/${enc(r.id)}`, class: 'mono', title: r.id }, short(r.id)));
  render(my,
    h('section', { class: 'panel' },
      h('div', { class: 'toolbar' }, h('h1', {}, 'Compare runs'), h('span', { class: 'spacer' }),
        h('a', { href: `#/compare/${enc(b)}/${enc(a)}` }, 'Swap A and B')),
      h('div', { class: 'table-wrap' }, h('table', { class: 'cmp' },
        h('thead', {}, h('tr', {}, h('th', {}), runHead('A', A), runHead('B', B))),
        h('tbody', {}, rows.map(([k, va, vb, differs]) => h('tr', {}, h('th', { scope: 'row' }, k),
          h('td', { class: differs ? 'differs' : null }, va), h('td', { class: differs ? 'differs' : null }, vb))))))),
    h('section', { class: 'panel' },
      h('div', { class: 'panel-head' }, h('h2', {}, 'Files at the end of the run'), f.available
        ? h('span', { class: 'muted' }, `${f.a} in A, ${f.b} in B · ${f.changed.length} differ · ${f.only_a.length} only in A · ${f.only_b.length} only in B · ${f.same} identical`)
        : null),
      !f.available ? h('p', { class: 'empty' }, 'One of the runs has no final file manifest (it errored or predates manifests).')
        : fileItems.length ? h('ul', { class: 'files' }, fileItems)
          : h('p', { class: 'empty' }, 'Both runs ended with identical files.')));
}

// snapshotNotice says what a snapshot is and where to start.
function snapshotNotice(i) {
  return h('div', { class: 'notice', role: 'note' },
    i.note ? h('span', {}, `${i.note} `) : null,
    'Every trace, prompt and tool call is browsable; forking needs ',
    i.home ? h('a', { href: i.home }, 'Furcate') : 'Furcate', ' running with a sandbox driver.',
    i.featured ? h('span', {}, ' Start with ', h('a', { href: `#/run/${enc(i.featured)}` }, 'a failed run and its forks'), '.') : null);
}

// ---------- start ----------

window.addEventListener('hashchange', route);
api('/api/info')
  .then(i => {
    info = i;
    const chip = document.getElementById('mode');
    chip.textContent = i.fork_enabled ? 'forking enabled' : i.static ? 'snapshot' : 'read-only';
    chip.title = i.fork_enabled ? 'Forks run on this server’s sandbox driver'
      : i.static ? 'Recorded runs as static files: browsing only, no forks' : 'Started without -driver: browsing only, no forks';
    chip.hidden = false;
    if (i.static) app.before(snapshotNotice(i));
  })
  .catch(() => {})
  .finally(route);

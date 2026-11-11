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
const runs = new Map(); // run id -> Promise<run detail>
let shownRun = null; // the run on screen, so switching its tabs doesn't refetch it
const prefs = { since: '7d', filter: '', show: 'runs' };

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
const clamp = (x, lo, hi) => Math.min(Math.max(x, lo), hi);
const kindCls = k => (['llm', 'tool', 'agent'].includes(k) ? k : 'other');
const kindLabel = k => ({ llm: 'LLM', tool: 'Tool', agent: 'Agent' }[k] || k || 'span');
const parseJSON = text => { try { return JSON.parse(text); } catch { return undefined; } };
const plural = (n, word) => `${fmt.num(n)} ${word}${n === 1 ? '' : 's'}`;

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

// A run's state as [class, word], for list rows, run rows, branch rows and job branches.
function stateOf(r) {
  if (r.outcome) return [r.outcome === 'pass' ? 'pass' : 'fail', r.outcome];
  if (r.status === 'error') return ['error', 'error'];
  if (r.status === 'running') return ['running', 'running'];
  if (r.status) return ['', r.status];
  if (r.span_status === 'error') return ['error', 'error'];
  return ['', r.span_status || 'unknown'];
}
const status = r => { const [c, t] = stateOf(r); return h('span', { class: `status ${c}` }, h('i'), t); };
const pill = r => { const [c, t] = stateOf(r); return h('span', { class: `pill ${c}` }, t); };

function errorBox(err) {
  return h('div', { class: 'error-box', role: 'alert' }, h('strong', {}, 'Error: '), err.message || String(err));
}

function render(my, ...nodes) {
  if (!live(my)) return false;
  app.replaceChildren(...nodes);
  fitSplit();
  return true;
}

const page = (...kids) => h('div', { class: 'page' }, ...kids);

function fold(title, body, aside, open) {
  return h('details', { class: 'fold', open: !!open },
    h('summary', {}, title, aside ? h('span', { class: 'muted small' }, aside) : null),
    h('div', { class: 'fold-body' }, body));
}

function seg(label, options, current, onpick) {
  const el = h('div', { class: 'seg', role: 'group', 'aria-label': label });
  for (const [value, text] of options) {
    el.append(h('button', { type: 'button', 'aria-pressed': String(value === current), onclick: e => {
      for (const b of el.children) b.setAttribute('aria-pressed', String(b === e.currentTarget));
      onpick(value);
    } }, text));
  }
  return el;
}

// The trace's two panes fill the window below the run header on wide screens.
function fitSplit() {
  const split = app.querySelector('.split');
  if (!split) return;
  if (window.innerWidth <= 900) { split.style.height = ''; return; }
  const top = split.getBoundingClientRect().top + window.scrollY;
  split.style.height = `${Math.max(480, window.innerHeight - top - 24)}px`;
}

// ---------- router ----------

function route() {
  const my = ++seq;
  const [view, a, b, c] = location.hash.replace(/^#\/?/, '').split('/').map(safeDecode);
  for (const l of document.querySelectorAll('[data-nav]')) {
    if (['', 'runs', 'run', 'compare'].includes(view || '')) l.setAttribute('aria-current', 'page');
    else l.removeAttribute('aria-current');
  }
  let p;
  if (!view || view === 'runs') p = viewRuns(my);
  else if (view === 'run' && a) p = viewRun(my, a, b || 'trace', c);
  else if (view === 'compare' && a && b) p = viewCompare(my, a, b);
  else p = Promise.resolve(render(my, page(h('p', {}, 'No such page. ', h('a', { href: '#/runs' }, 'Back to runs')))));
  p.catch(err => render(my, page(errorBox(err))));
}

// ---------- runs list ----------

async function viewRuns(my) {
  document.title = 'Runs · Furcate';
  shownRun = null;
  const cols = [['Status'], ['Run'], ['Task'], ['Driver', 'opt'], ['Started', 'opt'], ['Duration', 'num'],
    ['Tokens', 'num opt'], ['Cost', 'num'], ['Branches', 'num opt']];
  const tbody = h('tbody');
  const count = h('span', { class: 'count' });
  let list = [];

  const matches = r => {
    if (prefs.show === 'runs' && r.parent_run_id) return false;
    const q = prefs.filter.trim().toLowerCase();
    return !q || [r.id, r.name, r.task_id, r.driver, r.parent_run_id].some(s => s && s.toLowerCase().includes(q));
  };
  const empty = text => h('tr', {}, h('td', { colspan: cols.length, class: 'empty' }, text));
  const fill = () => {
    const shown = list.filter(matches);
    count.textContent = shown.length === list.length ? fmt.num(list.length) : `${fmt.num(shown.length)} of ${fmt.num(list.length)}`;
    tbody.replaceChildren(...(shown.length ? shown.map(runRow)
      : [empty(list.length ? 'No runs match.' : STATIC ? 'No runs in this snapshot.' : 'No finished runs in this window.')]));
  };
  const load = async () => {
    tbody.replaceChildren(empty('Loading…'));
    try {
      const data = await api(`/api/runs?since=${enc(prefs.since)}&limit=200`);
      if (!live(my)) return;
      list = data.runs;
      fill();
    } catch (err) {
      if (live(my)) tbody.replaceChildren(h('tr', {}, h('td', { colspan: cols.length }, errorBox(err))));
    }
  };

  const windows = [['1h', 'Last hour'], ['24h', 'Last 24 hours'], ['7d', 'Last 7 days'], ['30d', 'Last 30 days']];
  render(my, page(
    h('div', { class: 'page-head' },
      h('h1', {}, 'Runs'), count, h('span', { class: 'spacer' }),
      h('input', { type: 'search', placeholder: 'Filter by id, task, driver…', 'aria-label': 'Filter runs', value: prefs.filter,
        oninput: e => { prefs.filter = e.target.value; fill(); } }),
      seg('Which runs', [['runs', 'Runs'], ['all', 'With branches']], prefs.show, v => { prefs.show = v; fill(); }),
      STATIC ? null : h('select', { 'aria-label': 'Time window', onchange: e => { prefs.since = e.target.value; load(); } },
        windows.map(([v, t]) => h('option', { value: v, selected: v === prefs.since }, t))),
      STATIC ? null : h('button', { type: 'button', class: 'btn', onclick: load }, 'Refresh')),
    h('div', { class: 'table-wrap' }, h('table', { class: 'grid' },
      h('thead', {}, h('tr', {}, cols.map(([t, cls]) => h('th', { class: cls, scope: 'col' }, t)))),
      tbody))));
  await load();
}

function runRow(r) {
  const href = `#/run/${enc(r.id)}`;
  let sub = null;
  if (r.parent_run_id) {
    sub = h('span', { class: 'sub' }, 'branch of ', h('a', { href: `#/run/${enc(r.parent_run_id)}`, class: 'mono' }, short(r.parent_run_id)),
      ` at step ${r.parent_step}`);
  } else if (r.eval) {
    sub = h('span', { class: 'sub' }, `eval ${r.eval.case}${r.eval.variant ? ` · ${r.eval.variant}` : ''}`);
  }
  return h('tr', { class: 'clickable', onclick: e => { if (!e.target.closest('a')) location.hash = href; } },
    h('td', {}, status(r)),
    h('td', {}, h('a', { href, class: 'mono', title: r.id }, short(r.id))),
    h('td', {}, r.task_id || r.name, sub),
    h('td', { class: 'opt' }, r.driver || '–'),
    h('td', { class: 'opt nowrap' }, fmt.time(r.start)),
    h('td', { class: 'num' }, fmt.dur(r.duration_ms)),
    h('td', { class: 'num opt' }, r.tokens ? fmt.num(r.tokens) : '–'),
    h('td', { class: 'num' }, fmt.cost(r.cost_usd)),
    h('td', { class: 'num opt' }, r.branches ? fmt.num(r.branches) : '–'));
}

// ---------- run view ----------

function getRun(id, fresh) {
  if (fresh || !runs.has(id)) {
    runs.set(id, api(`/api/runs/${enc(id)}`).catch(err => { runs.delete(id); throw err; }));
  }
  return runs.get(id);
}

async function viewRun(my, id, tab, spanId) {
  document.title = `Run ${short(id)} · Furcate`;
  if (shownRun !== id) render(my, page(h('p', { class: 'muted' }, 'Loading run…')));
  const d = await getRun(id, !STATIC && shownRun !== id);
  if (!live(my)) return;
  shownRun = id;
  const r = d.run;
  const tabs = [['trace', 'Trace', d.trace ? d.trace.spans : null]];
  if (r) tabs.push(['branches', 'Branches', d.branches]);
  if (r && r.task) tabs.push(['task', 'Task', null]);
  if (!tabs.some(([k]) => k === tab)) tab = 'trace';
  const body = tab === 'branches' ? branchesView(my, d) : tab === 'task' ? taskView(r.task) : traceView(d, spanId);
  render(my, page(runHeader(d),
    h('nav', { class: 'tabs', 'aria-label': 'Run' }, tabs.map(([k, t, n]) =>
      h('a', { href: `#/run/${enc(id)}${k === 'trace' ? '' : `/${k}`}`, 'aria-current': k === tab ? 'page' : null },
        t, n ? h('span', { class: 'n' }, fmt.num(n)) : null))),
    body));
}

function runHeader(d) {
  const r = d.run, t = d.trace;
  const root = t && t.roots[0];
  const title = (r && r.task_id) || (root && root.name) || 'Run';
  const meta = h('div', { class: 'meta' });
  const add = (...kids) => meta.append(h('span', {}, ...kids));
  if (r && r.driver) add(r.driver);
  add(fmt.time(r ? r.started : t.start));
  add(fmt.dur(r && r.duration_ms != null ? r.duration_ms : t && t.duration_ms));
  if (r) {
    const u = r.usage;
    add(plural(u.llm_calls, 'LLM call'));
    add(h('span', { title: `${fmt.num(u.input_tokens)} uncached input · ${fmt.num(u.cache_read_tokens)} cache read · ` +
      `${fmt.num(u.cache_write_tokens)} cache write · ${fmt.num(u.output_tokens)} output` },
      `${fmt.num(u.input_tokens + u.cache_read_tokens + u.cache_write_tokens)} in · ${fmt.num(u.output_tokens)} out tokens`));
    add(fmt.cost(u.cost_usd));
    if (r.final_files != null) add(plural(r.final_files, 'file'));
  } else if (t) {
    add(`${fmt.num(t.totals.tokens_in)} in · ${fmt.num(t.totals.tokens_out)} out tokens`);
    add(fmt.cost(t.totals.cost_usd));
  }
  return h('div', {},
    h('div', { class: 'crumbs' }, h('a', { href: '#/runs' }, 'Runs'), ' / ', h('span', { class: 'mono' }, d.id)),
    h('div', { class: 'run-head' },
      h('h1', {}, title), r ? pill(r) : pill({ span_status: root && root.status }),
      r && r.eval ? h('span', { class: 'tag' }, `eval ${r.eval.case}${r.eval.variant ? ` · ${r.eval.variant}` : ''}`) : null,
      h('span', { class: 'spacer' }),
      r && r.parent_run_id ? h('a', { class: 'btn sm', href: `#/compare/${enc(r.parent_run_id)}/${enc(d.id)}` }, 'Compare with parent') : null),
    meta,
    r && r.parent_run_id ? h('div', { class: 'lineage' }, 'Branch of ',
      h('a', { href: `#/run/${enc(r.parent_run_id)}`, class: 'mono' }, short(r.parent_run_id)),
      ` from step ${r.parent_step}, with `, h('code', {}, r.change_label || 'no change')) : null,
    r ? null : h('div', { class: 'lineage muted' }, 'Trace only: this run has no row in the run store, so it has no checkpoints to fork.'),
    r && r.error ? h('div', { class: 'callout' }, r.error) : null);
}

// ---------- trace ----------

function traceView(d, spanId) {
  if (!d.trace) {
    return h('p', { class: 'empty' }, STATIC ? 'No spans were recorded for this run.'
      : 'No spans in the trace store yet. They reach ClickHouse through the collector a few seconds after each step.');
  }
  const total = d.trace.duration_ms || 1;
  const steps = new Map(d.steps.map(s => [s.span_id, s]));
  const nodes = [];
  const walk = (ns, depth) => { for (const n of ns) { nodes.push([n, depth]); walk(n.children || [], depth + 1); } };
  walk(d.trace.roots, 0);

  const detail = h('div', { class: 'pane', 'aria-live': 'polite' });
  const rows = [];
  let current = -1;
  const select = (i, how) => {
    if (i < 0 || i >= nodes.length || i === current) return;
    if (current >= 0) { rows[current].classList.remove('sel'); rows[current].setAttribute('aria-selected', 'false'); }
    current = i;
    rows[i].classList.add('sel');
    rows[i].setAttribute('aria-selected', 'true');
    const [n] = nodes[i];
    detail.replaceChildren(spanDetail(n, d, steps.get(n.span_id)));
    detail.scrollTop = 0;
    if (how === 'key') rows[i].scrollIntoView({ block: 'nearest' });
    if (how !== 'init') history.replaceState(null, '', `#/run/${enc(d.id)}/trace/${enc(n.span_id)}`);
  };
  nodes.forEach(([n, depth], i) => {
    const err = n.status === 'error';
    const left = clamp((n.offset_ms / total) * 100, 0, 100);
    const bar = h('span', { class: `bar ${kindCls(n.kind)}${err ? ' err' : ''}` });
    bar.style.left = `${left}%`;
    bar.style.width = `${clamp((n.duration_ms / total) * 100, 0.4, 100 - left)}%`;
    const name = h('div', { class: 'tname' },
      h('span', { class: `kdot ${kindCls(n.kind)}`, title: kindLabel(n.kind) }),
      h('span', { class: 'nm', title: n.name }, n.name),
      n.step ? h('span', { class: 'stepn', title: `step ${n.step}` }, `#${n.step}`) : null,
      n.checkpoint_id ? h('span', { class: 'cpmark', title: `checkpoint ${n.checkpoint_id}` }, '◆') : null,
      err ? h('span', { class: 'errmark', title: n.status_message || 'error' }, '!') : null);
    name.style.paddingLeft = `${depth * 16}px`;
    rows.push(h('div', { class: 'trow', role: 'option', 'aria-selected': 'false', onclick: () => select(i, 'click') },
      name,
      h('div', { class: 'tl', title: `starts at +${fmt.dur(n.offset_ms)}, takes ${fmt.dur(n.duration_ms)}` }, bar),
      h('div', { class: 'tdur' }, fmt.dur(n.duration_ms))));
  });
  const tree = h('div', { class: 'pane tree', role: 'listbox', tabindex: '0', 'aria-label': 'Spans (arrow keys move)',
    onkeydown: e => {
      if (e.key === 'ArrowDown' || e.key === 'j') { select(current + 1, 'key'); e.preventDefault(); }
      if (e.key === 'ArrowUp' || e.key === 'k') { select(current - 1, 'key'); e.preventDefault(); }
    } },
    h('div', { class: 'tree-head' }, h('span', {}, plural(d.trace.spans, 'span')), h('span', {}, 'Timeline'),
      h('span', { class: 'num' }, fmt.dur(d.trace.duration_ms))),
    rows);
  const at = nodes.findIndex(([n]) => n.span_id === spanId);
  select(at >= 0 ? at : 0, 'init');
  if (at > 0) requestAnimationFrame(() => rows[at].scrollIntoView({ block: 'nearest' }));
  return h('div', { class: 'split' }, tree, detail);
}

const PAYLOAD_LABELS = {
  'furcate.llm.request': 'Request', 'furcate.llm.response': 'Response',
  'gen_ai.tool.call.arguments': 'Tool input', 'gen_ai.tool.call.result': 'Tool output',
  'gen_ai.input.messages': 'Input messages', 'gen_ai.output.messages': 'Output messages',
  'gen_ai.system_instructions': 'System', 'gen_ai.tool.definitions': 'Tool definitions',
  'gen_ai.prompt': 'Prompt', 'gen_ai.completion': 'Completion',
};
const payloadOrder = k => { const i = Object.keys(PAYLOAD_LABELS).indexOf(k); return i < 0 ? 99 : i; };

function spanDetail(n, d, step) {
  const kind = kindCls(n.kind);
  const keys = Object.keys(n.payloads || {}).sort((a, b) => payloadOrder(a) - payloadOrder(b) || a.localeCompare(b));
  const formatted = formattedView(n);

  const forkSlot = h('div', {});
  let action = null;
  if (n.forkable) {
    action = h('button', {
      type: 'button', class: 'btn sm primary', disabled: !info.fork_enabled,
      title: info.fork_enabled ? `Restore ${n.checkpoint_id} and run continuations from here`
        : STATIC ? 'This is a snapshot of recorded runs: forking needs furcate serve with a sandbox driver'
          : 'Read-only: start furcate serve with -driver to fork',
      onclick: () => forkSlot.replaceChildren(...(forkSlot.firstChild ? [] : [forkForm(n, d)])),
    }, `Fork from step ${n.step}`);
  } else if (step && step.kind === 'llm' && step.checkpoint && step.checkpoint.deleted) {
    action = h('span', { class: 'muted small' }, 'checkpoint garbage-collected');
  }

  const meta = h('div', { class: 'meta' });
  const add = (...kids) => meta.append(h('span', {}, ...kids));
  add(fmt.dur(n.duration_ms));
  add(`at +${fmt.dur(n.offset_ms)}`);
  if (n.step) add(`step ${n.step}`);
  if (n.tokens_in || n.tokens_out) add(`${fmt.num(n.tokens_in)} in · ${fmt.num(n.tokens_out)} out tokens`);
  if (n.cost_usd) add(fmt.cost(n.cost_usd));
  if (n.checkpoint_id) {
    const cp = step && step.checkpoint;
    add(h('span', { title: cp ? `${cp.driver} checkpoint, level ${cp.level}${cp.size_bytes ? `, ${fmt.bytes(cp.size_bytes)}` : ''}` +
      `${cp.pinned ? ', pinned' : ''}${cp.deleted ? ', garbage-collected' : ''}` : 'checkpoint' },
    h('span', { class: 'cpmark' }, '◆ '), h('span', { class: 'mono' }, n.checkpoint_id)));
  }

  const body = h('div', {});
  const showRaw = () => body.replaceChildren(keys.length ? payloadViewer(n.payloads, keys) : h('p', { class: 'muted small' }, 'No payloads recorded on this span.'));
  const kids = [
    h('div', { class: 'd-head' },
      h('h2', {}, h('span', { class: `kind ${kind}` }, kindLabel(n.kind)), n.name),
      h('span', { class: 'spacer' }), action),
    meta,
    n.status === 'error' && n.status_message ? h('div', { class: 'callout' }, n.status_message) : null,
    n.orphan ? h('p', { class: 'muted small' }, 'This span’s parent never arrived.') : null,
    forkSlot,
  ];
  if (d.run && d.trace.roots[0] === n) kids.push(runSummary(d));
  if (formatted && keys.length) {
    kids.push(h('div', { class: 'd-tools' }, seg('Payload view', [['formatted', 'Formatted'], ['raw', 'Raw']], 'formatted',
      v => (v === 'raw' ? showRaw() : body.replaceChildren(formatted)))));
    body.append(formatted);
  } else if (keys.length) {
    kids.push(h('div', { class: 'sec' }, h('div', { class: 'sec-title' }, 'Payloads'), body));
    showRaw();
  }
  if (formatted) kids.push(body);

  const attrs = Object.entries(n.attributes || {}).sort(([a], [b]) => a.localeCompare(b));
  kids.push(h('div', { class: 'sec' }, fold(`Attributes (${attrs.length + 1})`,
    h('table', { class: 'attrs' }, h('tbody', {}, h('tr', {}, h('th', {}, 'span_id'), h('td', {}, n.span_id)),
      attrs.map(([k, v]) => h('tr', {}, h('th', {}, k), h('td', {}, v))))))));
  return h('div', { class: 'detail' }, kids);
}

function runSummary(d) {
  const r = d.run, t = d.trace.totals;
  return h('div', { class: 'sec' }, h('div', { class: 'sec-title' }, 'Run'),
    h('dl', { class: 'kv' },
      h('dt', {}, 'Outcome'), h('dd', {}, status(r)),
      h('dt', {}, 'Steps'), h('dd', {}, `${plural(t.llm_calls, 'LLM call')}, ${plural(t.tool_calls, 'tool call')}`),
      h('dt', {}, 'Branches'), h('dd', {}, d.branches ? h('a', { href: `#/run/${enc(d.id)}/branches` }, plural(d.branches, 'run')) : 'none'),
      h('dt', {}, 'Task'), h('dd', {}, r.task ? h('a', { href: `#/run/${enc(d.id)}/task` }, r.task_id) : r.task_id)),
    h('p', { class: 'muted small' }, 'Select a step on the left (or use the arrow keys) to see its prompt, response or tool output.'));
}

function loadPayload(hash) {
  if (!payloads.has(hash)) {
    payloads.set(hash, api(`/api/payloads/${enc(hash)}`).catch(err => { payloads.delete(hash); throw err; }));
  }
  return payloads.get(hash);
}

// formattedView shows an LLM call as a conversation and a tool call as a command
// and its output, or returns null for spans it doesn't know how to read.
function formattedView(n) {
  const p = n.payloads || {};
  if (p['furcate.llm.request'] || p['furcate.llm.response']) {
    return sections([['Input', p['furcate.llm.request'], requestView],
      ['Output', p['furcate.llm.response'], responseView, text => parseJSON(text) ?? sseMessage(text)]]);
  }
  if (p['gen_ai.tool.call.arguments'] || p['gen_ai.tool.call.result']) {
    const name = (n.attributes && n.attributes['gen_ai.tool.name']) || n.name.replace(/^execute_tool /, '');
    return sections([
      ['Input', p['gen_ai.tool.call.arguments'], v => callView(name, v)],
      ['Output', p['gen_ai.tool.call.result'], v => resultView(typeof v === 'string' ? v : JSON.stringify(v, null, 2), n.status === 'error')],
    ], true);
  }
  return null;
}

// sseMessage rebuilds the final message from a streamed Messages response (the
// drop-in proxy records the stream as it went by), or returns undefined.
function sseMessage(text) {
  if (!/^data:/m.test(text)) return undefined;
  let msg = null;
  const blocks = [];
  for (const line of text.split('\n')) {
    if (!line.startsWith('data:')) continue;
    const ev = parseJSON(line.slice(5).trim());
    if (!ev) continue;
    if (ev.type === 'message_start') msg = { ...ev.message, content: [] };
    else if (ev.type === 'content_block_start') blocks[ev.index] = { ...ev.content_block, partial: '' };
    else if (ev.type === 'content_block_delta' && blocks[ev.index]) {
      const b = blocks[ev.index], d = ev.delta || {};
      if (d.type === 'text_delta') b.text = (b.text || '') + d.text;
      else if (d.type === 'thinking_delta') b.thinking = (b.thinking || '') + d.thinking;
      else if (d.type === 'input_json_delta') b.partial += d.partial_json;
    } else if (ev.type === 'message_delta' && msg) {
      Object.assign(msg, ev.delta || {});
      msg.usage = { ...(msg.usage || {}), ...(ev.usage || {}) };
    }
  }
  if (!msg) return undefined;
  msg.content = blocks.filter(Boolean).map(({ partial, ...b }) => (partial ? { ...b, input: parseJSON(partial) ?? partial } : b));
  return msg;
}

// sections loads each payload and renders it; anything it can't read shows as text.
function sections(parts, keepText) {
  return h('div', {}, parts.map(([title, hash, view, read = parseJSON]) => {
    const el = h('div', {}, h('p', { class: 'muted small' }, hash ? 'Loading…' : 'Not recorded.'));
    if (hash) {
      loadPayload(hash).then(text => {
        const v = read(text);
        let node = null;
        try { node = v !== undefined && (typeof v === 'object' || keepText) ? view(v) : keepText ? view(text) : null; } catch { node = null; }
        el.replaceChildren(node || h('pre', { class: 'code' }, pretty(text)));
      }, err => el.replaceChildren(h('p', { class: 'error-msg' }, `Could not load this payload: ${err.message}`)));
    }
    return h('div', { class: 'sec' }, h('div', { class: 'sec-title' }, title), el);
  }));
}

function requestView(req) {
  if (!req || !Array.isArray(req.messages)) return null;
  const out = [];
  const facts = [req.model, req.max_tokens && `max ${fmt.num(req.max_tokens)} tokens`,
    req.output_config && req.output_config.effort && `effort ${req.output_config.effort}`,
    Array.isArray(req.tools) && req.tools.length && `tools: ${req.tools.map(t => t.name).join(', ')}`].filter(Boolean);
  if (facts.length) out.push(h('div', { class: 'usage' }, facts.join(' · ')));
  const sys = typeof req.system === 'string' ? req.system
    : Array.isArray(req.system) ? req.system.map(b => b.text || '').join('\n\n') : '';
  if (sys) out.push(fold('System prompt', h('div', { class: 'text' }, sys), `${fmt.num(sys.length)} characters`));
  const msgs = req.messages;
  const keep = Math.min(msgs.length, 1); // the newest turn is what this call adds; the step before shows the rest
  const early = msgs.slice(0, msgs.length - keep);
  if (early.length) out.push(fold(`${plural(early.length, 'earlier message')}`, early.map(messageView)));
  out.push(...msgs.slice(msgs.length - keep).map(messageView));
  return h('div', {}, out);
}

function responseView(resp) {
  if (!resp || !Array.isArray(resp.content)) return null;
  const u = resp.usage || {};
  const usage = [resp.stop_reason && `stop: ${resp.stop_reason}`, u.input_tokens != null && `${fmt.num(u.input_tokens)} input`,
    u.cache_read_input_tokens && `${fmt.num(u.cache_read_input_tokens)} cache read`,
    u.cache_creation_input_tokens && `${fmt.num(u.cache_creation_input_tokens)} cache write`,
    u.output_tokens != null && `${fmt.num(u.output_tokens)} output tokens`].filter(Boolean).join(' · ');
  return h('div', {},
    h('div', { class: 'msg assistant' }, h('div', { class: 'msg-head' }, 'assistant', resp.model ? h('span', { class: 'muted' }, resp.model) : null),
      h('div', { class: 'msg-body' }, blocksView(resp.content))),
    usage ? h('div', { class: 'usage' }, usage) : null);
}

function messageView(m) {
  const blocks = typeof m.content === 'string' ? [{ type: 'text', text: m.content }] : Array.isArray(m.content) ? m.content : [];
  // A user turn that only carries tool results reads better labelled as such.
  const label = m.role === 'user' && blocks.length && blocks.every(b => b.type === 'tool_result') ? 'tool result' : m.role;
  return h('div', { class: `msg ${m.role}` }, h('div', { class: 'msg-head' }, label), h('div', { class: 'msg-body' }, blocksView(blocks)));
}

// blocksView renders content blocks, folding a run of thinking blocks without
// text (the API returned only their signatures) into one line.
function blocksView(blocks) {
  const out = [];
  let quiet = 0;
  const flush = () => {
    if (quiet) out.push(h('div', { class: 'thinking' }, `Thinking${quiet > 1 ? ` ×${quiet}` : ''} (no text recorded)`));
    quiet = 0;
  };
  for (const b of blocks) {
    if (b.type === 'thinking' && !b.thinking) { quiet++; continue; }
    flush();
    out.push(blockView(b));
  }
  flush();
  return out;
}

function blockView(b) {
  switch (b.type) {
    case 'text': return h('div', { class: 'text' }, b.text || '');
    case 'thinking': return b.thinking ? fold('Thinking', h('div', { class: 'text' }, b.thinking)) : h('div', { class: 'thinking' }, 'Thinking (no text recorded)');
    case 'redacted_thinking': return h('div', { class: 'thinking' }, 'Redacted thinking');
    case 'tool_use': return callView(b.name, b.input);
    case 'tool_result': return resultView(b.content, b.is_error, 8);
    default: return h('pre', { class: 'code' }, JSON.stringify(b, null, 2));
  }
}

// clipped shows the first lines of a long text, with a button for the rest, so a
// file dump doesn't push the rest of a conversation off screen.
function clipped(text, lines = 14) {
  const all = (text || '').split('\n');
  if (all.length <= lines + 4) return h('pre', {}, text || '');
  const pre = h('pre', {}, all.slice(0, lines).join('\n'));
  const more = h('button', { type: 'button', class: 'more', onclick: () => { pre.textContent = text; more.remove(); } },
    `Show ${fmt.num(all.length - lines)} more lines`);
  return h('div', {}, pre, more);
}

function callView(name, input) {
  let body;
  if (input && typeof input.command === 'string' && Object.keys(input).length === 1) {
    body = clipped(`$ ${input.command}`);
  } else if (input && typeof input.path === 'string' && typeof input.content === 'string') {
    body = h('div', {}, h('div', { class: 'call-sub mono' }, input.path), clipped(input.content));
  } else {
    body = clipped(typeof input === 'string' ? input : JSON.stringify(input, null, 2));
  }
  return h('div', { class: 'call' }, h('div', { class: 'call-head' }, '→', h('b', {}, name || 'tool')), body);
}

function resultView(content, isError, lines) {
  const text = typeof content === 'string' ? content
    : Array.isArray(content) ? content.map(c => (c.type === 'text' ? c.text : `[${c.type}]`)).join('\n') : JSON.stringify(content, null, 2);
  const exit = /^exit code (\d+)/.exec(text || '');
  const failed = isError || (exit && exit[1] !== '0');
  return h('div', { class: `result${failed ? ' err' : ''}` },
    h('div', { class: 'result-head' }, '← result', exit ? h('span', {}, `exit ${exit[1]}`) : null, isError ? h('span', {}, 'error') : null),
    clipped(text, lines));
}

function payloadViewer(hashes, keys) {
  const meta = h('span', { class: 'muted small' });
  const pre = h('pre', { class: 'code', tabindex: '0' });
  let current = null;
  const tabs = h('div', { class: 'seg', role: 'tablist', 'aria-label': 'Payloads' });
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
    tabs.append(h('button', { type: 'button', role: 'tab', 'data-key': k, 'aria-selected': 'false', title: k, onclick: () => show(k) },
      PAYLOAD_LABELS[k] || k));
  }
  const copy = h('button', { type: 'button', class: 'btn sm', onclick: async () => {
    try { await navigator.clipboard.writeText(pre.textContent); copy.textContent = 'Copied'; } catch { copy.textContent = 'Copy failed'; }
    setTimeout(() => { copy.textContent = 'Copy'; }, 1200);
  } }, 'Copy');
  show(keys[0]);
  return h('div', {}, h('div', { class: 'payload-bar' }, tabs, h('span', { class: 'spacer' }), meta, copy), pre);
}

// ---------- fork ----------

function forkForm(n, d) {
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
      await pollJob(job, status, d.id);
    } catch (err) {
      status.replaceChildren(h('span', { class: 'error-msg' }, err.message));
    }
    submit.disabled = false;
  };

  return h('form', { class: 'fork-form', onsubmit: onSubmit },
    h('h3', {}, `Fork from step ${n.step}`, h('span', { class: 'muted small' }, ` · restores ${n.checkpoint_id} and resends this request`)),
    h('label', {}, 'Continuations (N)', count),
    h('label', {}, 'Model', model),
    h('label', { class: 'wide' }, 'Append a system instruction', sys),
    h('div', { class: 'wide' }, h('label', { class: 'check' }, override, 'Override the last tool output'), toolOut),
    h('div', { class: 'wide row' }, submit, h('span', { class: 'muted small' }, 'Each branch is a new run; the result is a pass rate with a 95% CI.')),
    status);
}

async function pollJob(job, status, runId) {
  const my = seq;
  for (;;) {
    status.replaceChildren(jobSummary(job, runId));
    if (job.status !== 'running' || !live(my)) break;
    await sleep(1500);
    job = await api(`/api/jobs/${enc(job.id)}`);
  }
  if (job.status === 'done') runs.delete(runId); // its branch count changed
}

function jobSummary(j, runId) {
  if (j.status === 'running') {
    return h('span', {}, h('span', { class: 'spinner', 'aria-hidden': 'true' }),
      ` ${plural(j.n, 'branch')} from step ${j.step} running · ${fmt.dur(j.elapsed_ms)} · job ${j.id}`);
  }
  if (j.status === 'error') return h('span', { class: 'error-msg' }, `Fork failed: ${j.error}`);
  const r = j.result;
  return h('div', {},
    h('div', {}, h('strong', {}, `Passed ${r.passes} of ${r.done}`),
      r.done ? ` · ${fmt.pct(r.rate)} · 95% CI ${fmt.pct(r.lo)}–${fmt.pct(r.hi)}` : '',
      ` · ${fmt.cost(r.usage.cost_usd)} · ${fmt.dur(j.elapsed_ms)}`, r.errors ? ` · ${r.errors} errored` : '',
      ' · ', h('a', { href: `#/run/${enc(runId)}/branches` }, 'see all branches')),
    h('ul', {}, r.branches.map(b => h('li', {},
      b.run_id ? h('a', { href: `#/run/${enc(b.run_id)}`, class: 'mono' }, short(b.run_id)) : '–', ' ', status(b),
      b.error ? h('span', { class: 'error-msg small' }, ` ${b.error}`) : null))));
}

// ---------- branches ----------

function branchesView(my, d) {
  const panel = h('div', {}, h('p', { class: 'muted' }, 'Loading branches…'));
  loadBranches(my, d, panel);
  return panel;
}

async function loadBranches(my, d, panel) {
  let data;
  try {
    data = await api(`/api/runs/${enc(d.id)}/branches`);
  } catch (err) {
    if (live(my)) panel.replaceChildren(errorBox(err));
    return;
  }
  if (!live(my)) return;
  if (!data.total) {
    panel.replaceChildren(h('p', { class: 'empty' }, info.fork_enabled
      ? 'No branches yet. Select an LLM step in the trace and fork from it.'
      : 'No branches yet. Fork with furcate fork or furcate bisect, or start furcate serve with -driver to fork from the trace.'));
    return;
  }
  const picked = new Set();
  const compare = h('button', { type: 'button', class: 'btn sm', disabled: true,
    onclick: () => { const [a, b] = picked; location.hash = `#/compare/${enc(a)}/${enc(b)}`; } }, 'Compare 2 selected');
  const pick = (box, id) => {
    if (box.checked && picked.size >= 2) { box.checked = false; return; }
    if (box.checked) picked.add(id); else picked.delete(id);
    compare.disabled = picked.size !== 2;
  };
  panel.replaceChildren(
    h('div', { class: 'page-head' },
      h('span', { class: 'muted' }, `${plural(data.total, 'run')} forked from this one, grouped by step and change. Open a group to see its runs.`),
      h('span', { class: 'spacer' }), data.total > 1 ? compare : null),
    h('div', { class: 'bgroups' },
      h('div', { class: 'bhead' }, h('span', {}, 'Step'), h('span', {}, 'Change'), h('span', {}, 'Pass rate · 95% CI'),
        h('span', {}, 'Passed'), h('span', { class: 'num' }, 'Cost')),
      data.groups.map(g => branchGroup(g, d.id, pick))));
}

function cibar(g) {
  const bar = h('span', { class: 'cibar', role: 'img',
    'aria-label': g.done ? `pass rate ${fmt.pct(g.rate)}, 95% CI ${fmt.pct(g.lo)} to ${fmt.pct(g.hi)}` : 'no finished branches' });
  const ci = h('span', { class: 'ci' });
  ci.style.left = `${g.lo * 100}%`;
  ci.style.width = `${Math.max((g.hi - g.lo) * 100, 1)}%`;
  bar.append(ci);
  if (g.done) {
    const mark = h('span', { class: 'rate' });
    mark.style.left = `${g.rate * 100}%`;
    bar.append(mark);
  }
  return bar;
}

function branchGroup(g, parentId, pick) {
  // Eval variants replace the whole system prompt, so their labels start alike: name the variant.
  const variants = new Set(g.runs.map(r => (r.eval && r.eval.variant) || ''));
  const variant = variants.size === 1 ? [...variants][0] : '';
  const cost = g.runs.reduce((s, r) => s + (r.cost_usd || 0), 0);
  const stats = h('span', { class: 'bstats' }, h('b', {}, `${g.passes}/${g.done}`),
    g.done ? ` · ${fmt.pct(g.rate)} · ${fmt.pct(g.lo)}–${fmt.pct(g.hi)}` : '',
    g.errors ? ` · ${g.errors} err` : '', g.running ? ` · ${g.running} running` : '');
  return h('details', { class: 'bgroup' },
    h('summary', { class: 'brow' },
      h('span', { class: 'mono' }, `step ${g.step}`),
      h('span', { class: 'blabel', title: JSON.stringify(g.change) }, variant ? `eval ${variant}: ${g.label}` : g.label),
      cibar(g), stats, h('span', { class: 'num' }, cost ? `$${cost.toFixed(2)}` : '–')),
    h('ul', { class: 'bruns' }, g.runs.map(r => {
      const box = h('input', { type: 'checkbox', 'aria-label': `Select ${r.id} to compare` });
      box.addEventListener('change', () => pick(box, r.id));
      return h('li', {}, box,
        h('a', { href: `#/run/${enc(r.id)}`, class: 'mono', title: r.id }, short(r.id)),
        status(r),
        h('span', { class: 'num muted' }, fmt.dur(r.duration_ms)),
        h('span', { class: 'num muted' }, fmt.cost(r.cost_usd)),
        h('a', { class: 'small', href: `#/compare/${enc(parentId)}/${enc(r.id)}` }, 'vs parent'),
        r.error ? h('span', { class: 'error-msg small', title: r.error }, r.error) : h('span', {}));
    })));
}

// ---------- task ----------

const shellQuote = s => (/^[\w@%+=:,./-]+$/.test(s) ? s : `'${s.replace(/'/g, `'\\''`)}'`);

function taskView(task) {
  if (!task || typeof task !== 'object' || Array.isArray(task)) return h('pre', { class: 'code' }, JSON.stringify(task, null, 2));
  const order = ['id', 'model', 'effort', 'prompt', 'system', 'template', 'workdir', 'setup', 'check', 'max_steps', 'tool_timeout'];
  const keys = [...order.filter(k => k in task), ...Object.keys(task).filter(k => !order.includes(k))];
  const isCmd = c => Array.isArray(c) && c.length > 0 && c.every(s => typeof s === 'string');
  const value = v => {
    if (typeof v === 'string') return v.length > 80 || v.includes('\n') ? h('div', { class: 'text' }, v) : h('span', {}, v);
    if (typeof v === 'number' || typeof v === 'boolean') return h('span', { class: 'mono' }, String(v));
    if (isCmd(v)) return h('pre', { class: 'code' }, `$ ${v.map(shellQuote).join(' ')}`);
    if (Array.isArray(v) && v.every(isCmd)) return h('pre', { class: 'code' }, v.map(c => `$ ${c.map(shellQuote).join(' ')}`).join('\n'));
    return h('pre', { class: 'code' }, JSON.stringify(v, null, 2));
  };
  return h('div', { class: 'table-wrap task' },
    h('dl', { class: 'kv' }, keys.flatMap(k => [h('dt', {}, k), h('dd', {}, value(task[k]))])),
    fold('As JSON', h('pre', { class: 'code' }, JSON.stringify(task, null, 2))));
}

// ---------- compare ----------

async function viewCompare(my, a, b) {
  document.title = 'Compare · Furcate';
  shownRun = null;
  render(my, page(h('p', { class: 'muted' }, 'Loading comparison…')));
  const d = await api(`/api/compare?a=${enc(a)}&b=${enc(b)}`);
  if (!live(my)) return;
  const A = d.a, B = d.b, f = d.files;
  const origin = r => (r.parent_run_id
    ? h('span', {}, 'branch of ', h('a', { href: `#/run/${enc(r.parent_run_id)}`, class: 'mono' }, short(r.parent_run_id)),
      ` at step ${r.parent_step}: `, h('code', {}, r.change_label || 'no change'))
    : 'original');
  const rows = [
    ['Origin', origin(A), origin(B), false],
    ['Result', status(A), status(B), (A.outcome || A.status) !== (B.outcome || B.status)],
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
  render(my, page(
    h('div', { class: 'crumbs' }, h('a', { href: '#/runs' }, 'Runs'), ' / compare'),
    h('div', { class: 'page-head' }, h('h1', {}, 'Compare runs'), h('span', { class: 'spacer' }),
      h('a', { class: 'btn sm', href: `#/compare/${enc(b)}/${enc(a)}` }, 'Swap A and B')),
    h('div', { class: 'table-wrap' }, h('table', { class: 'grid cmp' },
      h('thead', {}, h('tr', {}, h('th', {}), runHead('A', A), runHead('B', B))),
      h('tbody', {}, rows.map(([k, va, vb, differs]) => h('tr', { class: differs ? null : 'same' },
        h('th', { scope: 'row', class: 'muted' }, k), h('td', {}, va), h('td', {}, vb)))))),
    h('div', { class: 'sec' },
      h('div', { class: 'sec-title' }, 'Files at the end of the run', f.available
        ? h('span', { class: 'muted' }, `${f.a} in A, ${f.b} in B · ${f.changed.length} differ · ${f.only_a.length} only in A · ${f.only_b.length} only in B · ${f.same} identical`)
        : null),
      h('div', { class: 'table-wrap' }, !f.available ? h('p', { class: 'empty' }, 'One of the runs has no final file manifest (it errored or predates manifests).')
        : fileItems.length ? h('ul', { class: 'files' }, fileItems)
          : h('p', { class: 'empty' }, 'Both runs ended with identical files.')))));
}

// ---------- start ----------

// snapshotNotice says what a snapshot is and where to start.
function snapshotNotice(i) {
  return h('div', { class: 'notice', role: 'note' },
    h('span', {}, h('strong', {}, 'Snapshot'), ` · ${i.note || 'Recorded runs, read-only.'}`),
    h('span', { class: 'muted' }, 'Forking needs ', i.home ? h('a', { href: i.home }, 'Furcate') : 'Furcate', ' running locally.'),
    i.featured ? h('a', { href: `#/run/${enc(i.featured)}` }, 'Start with a failed run and its branches →') : null);
}

window.addEventListener('hashchange', route);
window.addEventListener('resize', fitSplit);
api('/api/info')
  .then(i => {
    info = i;
    const chip = document.getElementById('mode');
    chip.textContent = i.fork_enabled ? 'forking enabled' : i.static ? 'snapshot' : 'read-only';
    chip.title = i.fork_enabled ? 'Forks run on this server’s sandbox driver'
      : i.static ? 'Recorded runs as static files: browsing only, no forks' : 'Started without -driver: browsing only, no forks';
    chip.hidden = false;
    if (i.static) document.querySelector('header.top').after(snapshotNotice(i));
  })
  .catch(() => {})
  .finally(route);

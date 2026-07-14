// Shared helpers + navigation for the FileTransfer master UI.
const api = (m, p, b) => fetch('/api' + p, {
  method: m,
  headers: { 'Content-Type': 'application/json' },
  body: b ? JSON.stringify(b) : undefined,
}).then(async r => { const t = await r.text(); return t ? JSON.parse(t) : null; });

const esc = s => String(s ?? '').replace(/[&<>]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]));
const bytes = n => { n = +n || 0; const u = ['B', 'KB', 'MB', 'GB', 'TB']; let i = 0; while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; } return n.toFixed(i ? 1 : 0) + ' ' + u[i]; };
const when = s => { if (!s) return '—'; const d = new Date(s); return isNaN(d) ? '—' : d.toLocaleString(); };
const latency = ms => { const v = +ms || 0; if (!v) return '—'; if (v < 1000) return v + ' ms'; const s = v / 1000; return s.toFixed(s >= 10 ? 0 : 1) + ' s'; };
const pct = t => t.status === 'completed' ? 100 : (t.status === 'in_progress' ? 60 : (t.status === 'assigned' ? 25 : 0));

// One row of the transfers table (shared by Transfers + Worker pages).
// opts.showFlow adds a flow-identifier column; opts.showRequestedBy a requester column.
function transferRow(t, opts) {
  opts = opts || {};
  return `<tr>
    <td class="mono" title="${esc(t.id)}">${esc(t.id)}</td>
    ${opts.showFlow ? `<td class="mono">${esc(t.flow_id || '—')}</td>` : ''}
    <td><div>${esc(t.source)}</div><div class="muted">→ ${esc(t.target)}</div></td>
    <td>${bytes(t.size_bytes)}</td>
    <td style="min-width:110px;"><div class="bar"><span style="width:${pct(t)}%;"></span></div></td>
    <td><span class="pill s-${esc(t.status)}">${esc(t.status)}</span>${t.error ? `<div class="muted" style="font-size:.68rem;">${esc(t.error)}</div>` : ''}</td>
    ${opts.showRequestedBy ? `<td class="muted">${esc(t.requested_by || '—')}</td>` : ''}
    <td class="muted">${when(t.updated_at)}</td></tr>`;
}

// browse lists entries under a flow endpoint sandbox (flow id + role) at a relative path.
const browse = (flow, role, path) => api('GET', '/flows/browse?flow=' + encodeURIComponent(flow) +
  '&role=' + encodeURIComponent(role || 'sender') + '&path=' + encodeURIComponent(path || ''));

// mountBrowser renders a navigable file browser for a flow endpoint sandbox into `el`.
// opts.onPick(relPath) is called when a file is chosen (omit for read-only browsing).
function mountBrowser(el, flow, role, opts) {
  opts = opts || {};
  let cur = '';
  async function render() {
    let data;
    try { data = await browse(flow, role, cur); }
    catch (e) { el.innerHTML = `<div class="muted" style="padding:10px;">${esc('' + e)}</div>`; return; }
    const entries = (data && data.entries) || [];
    const up = cur ? `<div class="fb-item fb-up" data-up="1">⬆ ..</div>` : '';
    const rows = entries.map(en => en.is_dir
      ? `<div class="fb-item fb-dir" data-dir="${esc(en.name)}">📁 ${esc(en.name)}</div>`
      : `<div class="fb-item fb-file" data-file="${esc(en.name)}">📄 ${esc(en.name)}<span class="muted" style="float:right;">${bytes(en.size)}</span></div>`
    ).join('');
    el.innerHTML = `<div class="fb-path muted mono">/${esc(cur)}</div>${up}${rows || '<div class="muted" style="padding:10px;">(empty)</div>'}`;
    el.querySelectorAll('[data-dir]').forEach(n => n.onclick = () => { cur = (cur ? cur + '/' : '') + n.getAttribute('data-dir'); render(); });
    el.querySelectorAll('[data-up]').forEach(n => n.onclick = () => { cur = cur.split('/').slice(0, -1).join('/'); render(); });
    if (opts.onPick) el.querySelectorAll('[data-file]').forEach(n => n.onclick = () => opts.onPick((cur ? cur + '/' : '') + n.getAttribute('data-file')));
  }
  render();
  return { refresh: render };
}

// Render the top nav, marking `active` (a data-nav key on <body>).
function renderNav(active) {
  const links = [
    ['overview', '/', 'Overview'],
    ['requests', '/requests.html', 'Requests'],
    ['transfers', '/transfers.html', 'File Transfers'],
    ['flows', '/flows.html', 'Flows'],
    ['initiate', '/initiate.html', 'Initiate Transfer'],
  ];
  const el = document.getElementById('nav');
  if (!el) return;
  el.innerHTML = links.map(([k, href, label]) =>
    `<a href="${href}" class="${k === active ? 'active' : ''}">${label}</a>`).join('');
}

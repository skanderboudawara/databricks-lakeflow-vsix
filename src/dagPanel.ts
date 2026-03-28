import * as vscode from 'vscode';
import * as crypto from 'crypto';
import { JobDefinition } from './jobParser';

/**
 * Manages the webview panel that renders the job DAG.
 * One panel per job — reopening the same job re-uses the existing panel.
 */
export class DagPanel {
  private static panels = new Map<string, DagPanel>();

  private readonly panel: vscode.WebviewPanel;
  private disposables: vscode.Disposable[] = [];

  static createOrShow(extensionUri: vscode.Uri, job: JobDefinition): void {
    const column = vscode.window.activeTextEditor?.viewColumn ?? vscode.ViewColumn.One;

    if (DagPanel.panels.has(job.name)) {
      DagPanel.panels.get(job.name)!.panel.reveal(column);
      return;
    }

    const panel = vscode.window.createWebviewPanel(
      'databricksJobDag',
      job.name,
      column,
      {
        enableScripts: true,
        retainContextWhenHidden: true,
        localResourceRoots: [extensionUri],
      }
    );

    new DagPanel(panel, job);
  }

  private constructor(panel: vscode.WebviewPanel, job: JobDefinition) {
    this.panel = panel;
    DagPanel.panels.set(job.name, this);

    this.panel.webview.html = this.buildHtml(job);

    this.panel.onDidDispose(
      () => {
        DagPanel.panels.delete(job.name);
        this.dispose();
      },
      null,
      this.disposables
    );

    this.panel.webview.onDidReceiveMessage(
      async (message: { command: string; filePath?: string; notebookPath?: string }) => {
        if (message.command === 'openFile' && message.filePath) {
          const doc = await vscode.workspace.openTextDocument(message.filePath);
          vscode.window.showTextDocument(doc);
        }

        if (message.command === 'openNotebook' && message.notebookPath) {
          const folders = vscode.workspace.workspaceFolders;
          if (!folders) { return; }

          for (const ext of ['.py', '.ipynb', '']) {
            const pattern = new vscode.RelativePattern(
              folders[0],
              message.notebookPath + ext
            );
            const found = await vscode.workspace.findFiles(pattern, null, 1);
            if (found.length > 0) {
              const doc = await vscode.workspace.openTextDocument(found[0]);
              vscode.window.showTextDocument(doc);
              return;
            }
          }
          vscode.window.showWarningMessage(
            `Notebook not found: ${message.notebookPath}`
          );
        }
      },
      null,
      this.disposables
    );
  }

  private dispose(): void {
    this.panel.dispose();
    for (const d of this.disposables) {
      d.dispose();
    }
    this.disposables = [];
  }

  // ─── HTML ────────────────────────────────────────────────────────────────────

  private buildHtml(job: JobDefinition): string {
    const nonce = crypto.randomBytes(16).toString('hex');
    const jobJson = JSON.stringify(job).replace(/</g, '\\u003c');

    return /* html */ `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta http-equiv="Content-Security-Policy"
    content="default-src 'none'; style-src 'unsafe-inline'; script-src 'nonce-${nonce}';">
  <title>${escapeHtml(job.name)}</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    :root {
      --bg:      var(--vscode-editor-background,   #1e1e2e);
      --surface: var(--vscode-sideBar-background,  #252535);
      --border:  var(--vscode-panel-border,        #3a3a4e);
      --fg:      var(--vscode-editor-foreground,   #d4d4d4);
      --fg-dim:  var(--vscode-descriptionForeground, #888);
      --link:    var(--vscode-textLink-foreground, #4fc1ff);
      --hover:   var(--vscode-list-hoverBackground, #2a2a3e);
      --badge-bg:var(--vscode-badge-background,   #3a3a4e);
      --mono:    var(--vscode-editor-font-family,  'Consolas', monospace);
      --blue:    #4a9eff;
      --orange:  #ffb74d;
      --green:   #4caf50;
      --red:     #f44336;
      --purple:  #ce93d8;
      --teal:    #4dd0e1;
    }

    body {
      background: var(--bg);
      color: var(--fg);
      font-family: var(--vscode-font-family, 'Segoe UI', system-ui, sans-serif);
      font-size: 12px;
      height: 100vh;
      display: flex;
      flex-direction: column;
      overflow: hidden;
      user-select: none;
    }

    /* ── Top header bar ──────────────────────────────────────────────────── */
    .topbar {
      background: var(--surface);
      border-bottom: 1px solid var(--border);
      padding: 0 16px;
      display: flex;
      align-items: center;
      gap: 10px;
      height: 44px;
      flex-shrink: 0;
    }
    .topbar-icon {
      font-size: 16px;
      opacity: 0.7;
      flex-shrink: 0;
    }
    .topbar-name {
      font-size: 14px;
      font-weight: 600;
      color: var(--fg);
      flex: 1;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .pill {
      background: var(--badge-bg);
      color: var(--fg-dim);
      border-radius: 10px;
      padding: 2px 9px;
      font-size: 11px;
      white-space: nowrap;
      flex-shrink: 0;
    }
    .pill-trigger { background: #163016; color: #6fcf97; }
    .pill-cond    { background: #2e1e00; color: #f9a825; }
    .topbar-btn {
      background: transparent;
      border: 1px solid var(--border);
      color: var(--fg);
      border-radius: 4px;
      padding: 3px 10px;
      font-size: 11px;
      cursor: pointer;
      flex-shrink: 0;
      transition: background 0.12s;
    }
    .topbar-btn:hover { background: var(--hover); }

    /* ── Body: canvas + sidebar ──────────────────────────────────────────── */
    .body {
      display: flex;
      flex: 1;
      overflow: hidden;
    }

    /* ── DAG canvas ────────────────────────────────────────────────────────── */
    .canvas-wrap {
      flex: 1;
      overflow: hidden;
      position: relative;
    }
    svg#dag {
      width: 100%; height: 100%;
      display: block;
      cursor: grab;
    }
    svg#dag.panning { cursor: grabbing; }

    /* Zoom controls */
    .zoom-bar {
      position: absolute;
      bottom: 14px; right: 14px;
      display: flex; flex-direction: column;
      gap: 3px; z-index: 5;
    }
    .zoom-btn {
      width: 26px; height: 26px;
      background: var(--surface);
      border: 1px solid var(--border);
      color: var(--fg);
      border-radius: 4px;
      font-size: 15px;
      cursor: pointer;
      display: flex; align-items: center; justify-content: center;
      transition: background 0.12s;
    }
    .zoom-btn:hover { background: var(--hover); }

    /* Legend */
    .legend {
      position: absolute;
      bottom: 14px; left: 14px;
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 8px 10px;
      font-size: 11px;
      z-index: 5;
      display: flex; flex-direction: column; gap: 5px;
    }
    .legend-row { display: flex; align-items: center; gap: 6px; }
    .legend-dot { width: 10px; height: 10px; border-radius: 2px; flex-shrink: 0; }

    /* ── Right sidebar ─────────────────────────────────────────────────────── */
    .sidebar {
      width: 290px;
      flex-shrink: 0;
      border-left: 1px solid var(--border);
      background: var(--surface);
      display: flex;
      flex-direction: column;
      overflow: hidden;
      transition: width 0.18s ease;
    }
    .sidebar.collapsed { width: 0; }

    .sidebar-inner {
      width: 290px;
      height: 100%;
      overflow-y: auto;
      overflow-x: hidden;
      display: flex;
      flex-direction: column;
    }

    /* Sidebar top strip: breadcrumb + collapse button */
    .sidebar-strip {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 8px 12px;
      border-bottom: 1px solid var(--border);
      flex-shrink: 0;
    }
    .breadcrumb {
      font-size: 10px;
      color: var(--fg-dim);
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }
    .breadcrumb-link {
      color: var(--link);
      cursor: pointer;
      text-decoration: underline;
    }
    .breadcrumb-link:hover { opacity: 0.8; }
    .collapse-btn {
      background: none;
      border: none;
      color: var(--fg-dim);
      cursor: pointer;
      font-size: 14px;
      padding: 0 2px;
      line-height: 1;
    }
    .collapse-btn:hover { color: var(--fg); }

    /* Panel header area */
    .panel-header {
      padding: 12px 14px 10px;
      border-bottom: 1px solid var(--border);
      flex-shrink: 0;
    }
    .panel-title {
      font-size: 14px;
      font-weight: 700;
      color: var(--fg);
      word-break: break-all;
      line-height: 1.3;
    }
    .panel-subtitle {
      font-size: 11px;
      color: var(--fg-dim);
      margin-top: 3px;
    }
    .type-badge {
      display: inline-block;
      margin-top: 6px;
      padding: 2px 7px;
      border-radius: 3px;
      font-size: 10px;
      font-weight: 600;
      letter-spacing: 0.05em;
    }

    /* ── Accordion sections ────────────────────────────────────────────────── */
    details.section {
      border-top: 1px solid var(--border);
    }
    details.section > summary {
      padding: 9px 14px;
      cursor: pointer;
      list-style: none;
      display: flex;
      justify-content: space-between;
      align-items: center;
      font-size: 11px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: var(--fg-dim);
      user-select: none;
      transition: background 0.12s;
    }
    details.section > summary::-webkit-details-marker { display: none; }
    details.section > summary .sec-title { flex: 1; }
    details.section > summary .sec-count {
      font-size: 10px; font-weight: 400;
      background: var(--badge-bg);
      border-radius: 8px;
      padding: 1px 6px;
      margin-right: 6px;
    }
    details.section > summary::after {
      content: '›';
      font-size: 16px;
      font-weight: 300;
      transition: transform 0.15s;
      opacity: 0.6;
    }
    details.section[open] > summary::after { transform: rotate(90deg); }
    details.section > summary:hover { background: var(--hover); }

    .section-body {
      padding: 6px 14px 12px;
    }

    /* ── Section content helpers ───────────────────────────────────────────── */
    .prop { margin-bottom: 9px; }
    .prop-label {
      font-size: 10px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--fg-dim);
      margin-bottom: 2px;
    }
    .prop-value {
      font-size: 11px;
      color: var(--fg);
      word-break: break-all;
      line-height: 1.4;
    }
    .prop-value.mono {
      font-family: var(--mono);
      font-size: 10px;
      background: var(--bg);
      border-radius: 3px;
      padding: 3px 6px;
      display: block;
    }

    .dep-item {
      display: flex;
      align-items: center;
      gap: 6px;
      padding: 4px 0;
      border-bottom: 1px solid var(--border);
      font-size: 11px;
    }
    .dep-item:last-child { border-bottom: none; }
    .dep-arrow { color: var(--fg-dim); flex-shrink: 0; }
    .dep-key { flex: 1; word-break: break-all; }
    .outcome-badge {
      font-size: 9px;
      font-weight: 600;
      padding: 1px 5px;
      border-radius: 3px;
      text-transform: uppercase;
      flex-shrink: 0;
    }
    .outcome-true  { background: #1a3a1a; color: var(--green); }
    .outcome-false { background: #3a1a1a; color: var(--red); }

    .cluster-grid {
      display: grid;
      grid-template-columns: auto 1fr;
      gap: 4px 10px;
      font-size: 11px;
    }
    .cluster-key { color: var(--fg-dim); }
    .cluster-val { word-break: break-all; }

    .lib-item, .param-item, .table-item, .perm-item {
      padding: 3px 0;
      border-bottom: 1px solid var(--border);
      font-size: 11px;
    }
    .lib-item:last-child, .param-item:last-child,
    .table-item:last-child, .perm-item:last-child { border-bottom: none; }

    .param-name { color: var(--fg-dim); font-size: 10px; margin-bottom: 1px; }
    .param-val  { font-family: var(--mono); font-size: 10px; word-break: break-all; }

    .health-rule {
      display: flex; align-items: center; gap: 6px;
      padding: 4px 0;
      font-size: 11px;
      border-bottom: 1px solid var(--border);
    }
    .health-rule:last-child { border-bottom: none; }
    .health-metric { flex: 1; word-break: break-all; }
    .health-op  { color: var(--orange); font-weight: 600; font-family: var(--mono); font-size: 10px; }
    .health-val { color: var(--blue); font-family: var(--mono); font-size: 11px; }

    .empty-note { color: var(--fg-dim); font-size: 11px; font-style: italic; }

    .run-if-note {
      margin-top: 6px;
      padding: 4px 8px;
      background: #2e2800;
      border-left: 3px solid var(--orange);
      font-size: 10px;
      color: #f9a825;
      border-radius: 0 3px 3px 0;
    }

    /* ── Resize handle ─────────────────────────────────────────────────────── */
    .resize-handle {
      width: 4px;
      flex-shrink: 0;
      cursor: col-resize;
      background: transparent;
      transition: background 0.15s;
      position: relative;
      z-index: 10;
    }
    .resize-handle:hover,
    .resize-handle.dragging {
      background: var(--vscode-focusBorder, #007acc);
    }

    /* Sidebar toggle button (on the left edge when collapsed) */
    .sidebar-toggle-fab {
      position: absolute;
      right: 0;
      top: 50%;
      transform: translateY(-50%);
      width: 18px;
      height: 36px;
      background: var(--surface);
      border: 1px solid var(--border);
      border-right: none;
      border-radius: 4px 0 0 4px;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 11px;
      color: var(--fg-dim);
      z-index: 6;
      transition: background 0.12s;
    }
    .sidebar-toggle-fab:hover { background: var(--hover); color: var(--fg); }
  </style>
</head>
<body>

<!-- Top bar -->
<div class="topbar">
  <span class="topbar-icon">⬡</span>
  <span class="topbar-name" id="hdr-name"></span>
  <span class="pill" id="hdr-tasks"></span>
  <span class="pill pill-trigger" id="hdr-trigger" style="display:none"></span>
  <span class="pill pill-cond"    id="hdr-cond"    style="display:none">Conditions</span>
  <button class="topbar-btn" id="btn-open-file">Open YAML ↗</button>
</div>

<div class="body">

  <!-- DAG canvas -->
  <div class="canvas-wrap" id="canvas-wrap">
    <svg id="dag">
      <defs>
        <marker id="arr-d" markerWidth="7" markerHeight="5" refX="7" refY="2.5" orient="auto" markerUnits="strokeWidth">
          <path d="M0,0 L7,2.5 L0,5 Z" fill="#546e7a"/>
        </marker>
        <marker id="arr-t" markerWidth="7" markerHeight="5" refX="7" refY="2.5" orient="auto" markerUnits="strokeWidth">
          <path d="M0,0 L7,2.5 L0,5 Z" fill="#4caf50"/>
        </marker>
        <marker id="arr-f" markerWidth="7" markerHeight="5" refX="7" refY="2.5" orient="auto" markerUnits="strokeWidth">
          <path d="M0,0 L7,2.5 L0,5 Z" fill="#f44336"/>
        </marker>
      </defs>
      <g id="zg"></g>
    </svg>

    <div class="zoom-bar">
      <button class="zoom-btn" id="z-in"  title="Zoom in">+</button>
      <button class="zoom-btn" id="z-fit" title="Fit">⊡</button>
      <button class="zoom-btn" id="z-out" title="Zoom out">−</button>
    </div>

    <div class="legend">
      <div class="legend-row"><div class="legend-dot" style="background:#1a3a6e;border:1.5px solid #4a9eff"></div>Notebook</div>
      <div class="legend-row"><div class="legend-dot" style="background:#2a1e00;border:1.5px solid #ffb74d;transform:rotate(45deg)"></div>Condition</div>
      <div class="legend-row"><div class="legend-dot" style="background:#1a3a1a;border:1.5px solid #66bb6a"></div>Python / Spark</div>
      <div class="legend-row"><div class="legend-dot" style="background:#1e1e1e;border:1.5px solid #78909c"></div>Other</div>
    </div>
  </div>

  <!-- Resize handle -->
  <div class="resize-handle" id="resize-handle"></div>

  <!-- Right sidebar -->
  <div class="sidebar" id="sidebar">
    <div class="sidebar-inner" id="sidebar-inner">

      <!-- Strip: breadcrumb + collapse -->
      <div class="sidebar-strip">
        <span class="breadcrumb" id="breadcrumb">Job</span>
        <button class="collapse-btn" id="btn-collapse" title="Collapse panel">✕</button>
      </div>

      <!-- Dynamic panel content -->
      <div id="panel-content"></div>

    </div>
  </div>

</div><!-- .body -->

<script nonce="${nonce}">
(function () {
  'use strict';

  const job = ${jobJson};
  const vscodeApi = acquireVsCodeApi();

  // ── Layout constants ────────────────────────────────────────────────────────
  const NW = 190, NH = 62, CS = 44, HG = 130, VG = 28;

  // ── DOM refs ────────────────────────────────────────────────────────────────
  const dagSvg    = document.getElementById('dag');
  const zg        = document.getElementById('zg');
  const sidebar   = document.getElementById('sidebar');
  const breadcrumb = document.getElementById('breadcrumb');
  const panelContent = document.getElementById('panel-content');

  // ── State ───────────────────────────────────────────────────────────────────
  let tx = 40, ty = 0, sc = 1;
  let selectedKey = null;
  let positions = {};
  let sidebarOpen = true;
  let _bbox = null;

  // ── Top bar ─────────────────────────────────────────────────────────────────
  document.getElementById('hdr-name').textContent = job.name;
  document.getElementById('hdr-tasks').textContent =
    job.tasks.length + ' task' + (job.tasks.length !== 1 ? 's' : '');

  if (job.trigger) {
    const lbl = { table_update:'Table trigger', periodic:'Scheduled', file_arrival:'File arrival', manual:'Manual' };
    const el = document.getElementById('hdr-trigger');
    el.textContent = lbl[job.trigger.type] || job.trigger.type;
    el.style.display = '';
  }
  if (job.hasConditions) document.getElementById('hdr-cond').style.display = '';

  document.getElementById('btn-open-file').addEventListener('click', () =>
    vscodeApi.postMessage({ command: 'openFile', filePath: job.filePath })
  );

  document.getElementById('btn-collapse').addEventListener('click', () => toggleSidebar(false));

  // ── Sidebar toggle ──────────────────────────────────────────────────────────
  function toggleSidebar(open) {
    sidebarOpen = open;
    sidebar.classList.toggle('collapsed', !open);
    if (open && _bbox) setTimeout(() => fitView(_bbox), 200);
  }

  // FAB to re-open sidebar
  const fab = document.createElement('button');
  fab.className = 'sidebar-toggle-fab';
  fab.title = 'Show panel';
  fab.textContent = '‹';
  fab.style.display = 'none';
  document.querySelector('.canvas-wrap').appendChild(fab);
  fab.addEventListener('click', () => { toggleSidebar(true); fab.style.display = 'none'; });

  document.getElementById('btn-collapse').addEventListener('click', () => {
    toggleSidebar(false);
    fab.style.display = '';
  });

  // ── Layout ──────────────────────────────────────────────────────────────────
  function computeLayout(tasks) {
    const tmap = {}, ch = {}, ind = {};
    for (const t of tasks) { tmap[t.task_key] = t; ch[t.task_key] = []; ind[t.task_key] = 0; }
    for (const t of tasks)
      for (const d of t.depends_on) {
        ind[t.task_key]++;
        if (ch[d.task_key]) ch[d.task_key].push(t.task_key);
      }

    const q = [], lv = {};
    for (const [k, d] of Object.entries(ind))
      if (d === 0) { q.push(k); lv[k] = 0; }

    let qi = 0;
    while (qi < q.length) {
      const k = q[qi++];
      for (const c of ch[k]) {
        lv[c] = Math.max(lv[c] || 0, lv[k] + 1);
        ind[c]--;
        if (ind[c] === 0) q.push(c);
      }
    }
    for (const t of tasks) if (lv[t.task_key] === undefined) lv[t.task_key] = 0;

    const byLv = {}; let maxLv = 0;
    for (const [k, l] of Object.entries(lv)) {
      maxLv = Math.max(maxLv, l);
      if (!byLv[l]) byLv[l] = [];
      byLv[l].push(k);
    }
    for (const l in byLv) byLv[l].sort((a, b) => a.localeCompare(b));

    const pos = {};
    for (let l = 0; l <= maxLv; l++) {
      const keys = byLv[l] || [];
      const colH = keys.reduce((s, k) => s + (tmap[k].type === 'condition' ? CS*2 : NH), 0)
                 + Math.max(0, keys.length - 1) * VG;
      let y = -colH / 2;
      for (const k of keys) {
        const t = tmap[k], h = t.type === 'condition' ? CS*2 : NH;
        pos[k] = { x: l*(NW+HG), y, w: t.type === 'condition' ? CS*2 : NW, h, level: l };
        y += h + VG;
      }
    }
    return pos;
  }

  // ── Node colours ─────────────────────────────────────────────────────────────
  function nodeColors(type) {
    return ({
      notebook:     { fill:'#1a3a6e', stroke:'#4a9eff' },
      condition:    { fill:'#2a1e00', stroke:'#ffb74d' },
      spark_python: { fill:'#1a3a1a', stroke:'#66bb6a' },
      python_wheel: { fill:'#1a3a1a', stroke:'#66bb6a' },
      sql:          { fill:'#1a1a3a', stroke:'#ce93d8' },
      run_job:      { fill:'#1a2a2a', stroke:'#4dd0e1' },
      unknown:      { fill:'#1e1e1e', stroke:'#78909c' },
    })[type] || { fill:'#1e1e1e', stroke:'#78909c' };
  }

  // ── SVG helpers ──────────────────────────────────────────────────────────────
  const NS = 'http://www.w3.org/2000/svg';
  function el(tag, a={}) {
    const e = document.createElementNS(NS, tag);
    for (const [k,v] of Object.entries(a)) e.setAttribute(k, v);
    return e;
  }
  function txt(s, a={}) { const e = el('text',a); e.textContent = s; return e; }
  function trunc(s, n) { return s && s.length > n ? s.slice(0,n-1)+'…' : (s||''); }

  function edgePath(sp, st, dp) {
    const x1 = st.type==='condition' ? sp.x+CS*2 : sp.x+NW;
    const y1 = sp.y + sp.h/2;
    const x2 = dp.x, y2 = dp.y + dp.h/2;
    const cx = (x1+x2)/2;
    return \`M\${x1},\${y1} C\${cx},\${y1} \${cx},\${y2} \${x2},\${y2}\`;
  }

  // ── Draw ─────────────────────────────────────────────────────────────────────
  function draw(tasks, pos) {
    zg.innerHTML = '';
    const tmap = {};
    for (const t of tasks) tmap[t.task_key] = t;

    // edges
    const eg = el('g');
    for (const t of tasks) {
      for (const dep of t.depends_on) {
        const sp = pos[dep.task_key], dp = pos[t.task_key];
        if (!sp || !dp) continue;
        const iT = dep.outcome==='true', iF = dep.outcome==='false';
        const color = iT ? '#4caf50' : iF ? '#f44336' : '#546e7a';
        const marker = iT ? 'url(#arr-t)' : iF ? 'url(#arr-f)' : 'url(#arr-d)';
        eg.appendChild(el('path', {
          d: edgePath(sp, tmap[dep.task_key], dp),
          stroke: color, 'stroke-width':'1.5', fill:'none', 'marker-end': marker,
        }));
        if (dep.outcome) {
          const mx = (sp.x+sp.w/2+dp.x)/2, my = (sp.y+sp.h/2+dp.y+dp.h/2)/2 - 7;
          eg.appendChild(txt(dep.outcome, {
            x: mx, y: my, fill: color,
            'font-size':'10', 'text-anchor':'middle', 'font-family':'monospace',
          }));
        }
      }
    }
    zg.appendChild(eg);

    // nodes
    const ng = el('g');
    for (const t of tasks) {
      const p = pos[t.task_key];
      if (!p) continue;
      const c = nodeColors(t.type);
      const g = el('g', { 'data-key': t.task_key, cursor:'pointer' });

      if (t.type === 'condition') {
        const cx=p.x+CS, cy=p.y+CS, s=CS-3;
        g.appendChild(el('polygon', {
          points: \`\${cx},\${cy-s} \${cx+s},\${cy} \${cx},\${cy+s} \${cx-s},\${cy}\`,
          fill:c.fill, stroke:c.stroke, 'stroke-width':'1.5',
        }));
        const opMap = {EQUAL_TO:'==', NOT_EQUAL_TO:'!=', GREATER_THAN:'>', LESS_THAN:'<'};
        g.appendChild(txt(opMap[t.condition?.op]||'?', {
          x:cx, y:cy+4, fill:c.stroke, 'font-size':'13', 'font-weight':'bold',
          'text-anchor':'middle', 'pointer-events':'none',
        }));
        g.appendChild(txt(trunc(t.task_key,22), {
          x:cx, y:p.y+CS*2+14, fill:'#aaa', 'font-size':'10',
          'text-anchor':'middle', 'pointer-events':'none',
        }));
      } else {
        g.appendChild(el('rect', {
          x:p.x, y:p.y, width:NW, height:NH, rx:'5',
          fill:c.fill, stroke:c.stroke, 'stroke-width':'1.5',
        }));
        const typeLbl = {notebook:'NOTEBOOK',spark_python:'SPARK PYTHON',python_wheel:'PYTHON WHEEL',sql:'SQL',run_job:'RUN JOB'};
        g.appendChild(txt(typeLbl[t.type]||'', {
          x:p.x+8, y:p.y+14, fill:c.stroke, 'font-size':'9', 'font-weight':'600',
          'letter-spacing':'0.05em', 'pointer-events':'none',
        }));
        g.appendChild(txt(trunc(t.task_key,26), {
          x:p.x+8, y:p.y+32, fill:'#e0e0e0', 'font-size':'11.5', 'font-weight':'500',
          'pointer-events':'none',
        }));
        if (t.notebook_path) {
          const parts = t.notebook_path.split('/');
          g.appendChild(txt(trunc(parts.slice(-2).join('/'),30), {
            x:p.x+8, y:p.y+49, fill:'#666', 'font-size':'9.5', 'pointer-events':'none',
          }));
        }
        if (t.run_if) {
          const s = t.run_if==='AT_LEAST_ONE_SUCCESS' ? '≥1 ok' : t.run_if.replace(/_/g,' ').toLowerCase();
          g.appendChild(txt(s, {
            x:p.x+NW-6, y:p.y+13, fill:'#666', 'font-size':'9',
            'text-anchor':'end', 'pointer-events':'none',
          }));
        }
      }

      g.addEventListener('click', (e) => { e.stopPropagation(); selectTask(t.task_key); });

      if (t.type === 'notebook' && t.notebook_path) {
        // Double-click opens the file
        g.addEventListener('dblclick', (e) => {
          e.stopPropagation();
          vscodeApi.postMessage({ command: 'openNotebook', notebookPath: t.notebook_path });
        });
        // Small open-file indicator in top-right corner
        const p = pos[t.task_key];
        ng.appendChild(txt('↗', {
          x: p.x + NW - 7, y: p.y + 13,
          fill: '#4a9eff', 'font-size': '10', 'text-anchor': 'middle',
          'pointer-events': 'none', opacity: '0.5',
        }));
        // Native SVG tooltip
        const title = document.createElementNS(NS, 'title');
        title.textContent = 'Double-click to open file';
        g.prepend(title);
      }

      ng.appendChild(g);
    }
    zg.appendChild(ng);

    let minX=Infinity,minY=Infinity,maxX=-Infinity,maxY=-Infinity;
    for (const p of Object.values(pos)) {
      minX=Math.min(minX,p.x); minY=Math.min(minY,p.y);
      maxX=Math.max(maxX,p.x+p.w); maxY=Math.max(maxY,p.y+p.h);
    }
    return {minX,minY,maxX,maxY};
  }

  function applyT() { zg.setAttribute('transform', \`translate(\${tx},\${ty}) scale(\${sc})\`); }

  function fitView(bbox) {
    const w = document.getElementById('canvas-wrap');
    const W = w.clientWidth - 80, H = w.clientHeight - 80;
    const bw = bbox.maxX - bbox.minX || 1, bh = bbox.maxY - bbox.minY || 1;
    sc = Math.min(W/bw, H/bh, 1.4);
    tx = (W - bw*sc)/2 + 40 - bbox.minX*sc;
    ty = (H - bh*sc)/2 + 40 - bbox.minY*sc;
    applyT();
  }

  // ── SVG background click → job panel ────────────────────────────────────────
  dagSvg.addEventListener('click', (e) => {
    if (e.target === dagSvg || e.target === zg) {
      deselect();
    }
  });

  // ── Selection ────────────────────────────────────────────────────────────────
  function deselect() {
    if (selectedKey) {
      const n = zg.querySelector(\`[data-key="\${selectedKey}"]\`);
      if (n) (n.querySelector('rect')||n.querySelector('polygon'))?.setAttribute('stroke-width','1.5');
    }
    selectedKey = null;
    showJobPanel();
  }

  function selectTask(key) {
    if (selectedKey === key) { deselect(); return; }
    if (selectedKey) {
      const n = zg.querySelector(\`[data-key="\${selectedKey}"]\`);
      if (n) (n.querySelector('rect')||n.querySelector('polygon'))?.setAttribute('stroke-width','1.5');
    }
    selectedKey = key;
    const n = zg.querySelector(\`[data-key="\${key}"]\`);
    if (n) (n.querySelector('rect')||n.querySelector('polygon'))?.setAttribute('stroke-width','3');
    const task = job.tasks.find(t => t.task_key === key);
    if (task) showTaskPanel(task);
    if (!sidebarOpen) { toggleSidebar(true); fab.style.display = 'none'; }
  }

  // ── HTML helpers ─────────────────────────────────────────────────────────────
  function esc(s) {
    return String(s??'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  function section(title, bodyHtml, open=false, count=null) {
    const countBadge = count !== null ? \`<span class="sec-count">\${count}</span>\` : '';
    return \`<details class="section"\${open?' open':''}>
      <summary><span class="sec-title">\${esc(title)}</span>\${countBadge}</summary>
      <div class="section-body">\${bodyHtml}</div>
    </details>\`;
  }

  function prop(label, valueHtml) {
    return \`<div class="prop"><div class="prop-label">\${esc(label)}</div><div class="prop-value">\${valueHtml}</div></div>\`;
  }

  // ── Job panel ─────────────────────────────────────────────────────────────────
  function showJobPanel() {
    breadcrumb.innerHTML = \`<span>Job</span>\`;
    const parts = [];

    // --- Header ---
    const taskLabel = job.tasks.length + ' task' + (job.tasks.length!==1?'s':'');
    const timeoutLabel = job.timeoutSeconds ? (job.timeoutSeconds/60|0) + ' min timeout' : '';
    parts.push(\`<div class="panel-header">
      <div class="panel-title">\${esc(job.name)}</div>
      <div class="panel-subtitle">\${esc([taskLabel, timeoutLabel].filter(Boolean).join('  ·  '))}</div>
    </div>\`);

    // --- Trigger ---
    let triggerBody = '<span class="empty-note">No trigger (manual)</span>';
    if (job.trigger && job.trigger.type !== 'manual') {
      const t = job.trigger;
      let html = '';
      if (t.pauseStatus) html += prop('Status', \`<span style="color:\${t.pauseStatus==='UNPAUSED'?'#4caf50':'#f44336'}">\${esc(t.pauseStatus)}</span>\`);
      if (t.type === 'table_update') {
        html += prop('Type', 'Continuous (table update)');
        if (t.tableCondition) html += prop('Condition', esc(t.tableCondition));
        if (t.tableNames?.length) {
          const rows = t.tableNames.map(n => \`<div class="table-item"><span class="prop-value mono">\${esc(n)}</span></div>\`).join('');
          html += prop('Tables', rows);
        }
      } else if (t.type === 'periodic') {
        html += prop('Type', 'Scheduled');
        if (t.cronExpression) html += prop('Cron', \`<code class="prop-value mono">\${esc(t.cronExpression)}</code>\`);
        if (t.timezone) html += prop('Timezone', esc(t.timezone));
      } else if (t.type === 'file_arrival') {
        html += prop('Type', 'File arrival');
      }
      triggerBody = html;
    }
    parts.push(section('Trigger', triggerBody, true));

    // --- Health ---
    let healthBody = '<span class="empty-note">No health rules</span>';
    if (job.health.length > 0) {
      healthBody = job.health.map(r => {
        const opLabel = {GREATER_THAN:'>',LESS_THAN:'<',EQUAL_TO:'==',NOT_EQUAL:'\u2260'}[r.op]||r.op;
        const valLabel = r.metric==='RUN_DURATION_SECONDS' ? (r.value/60|0)+' min' : String(r.value);
        return \`<div class="health-rule">
          <span class="health-metric">\${esc(r.metric.replace(/_/g,' ').toLowerCase())}</span>
          <span class="health-op">\${esc(opLabel)}</span>
          <span class="health-val">\${esc(valLabel)}</span>
        </div>\`;
      }).join('');
    }
    parts.push(section('Health', healthBody, true));

    // --- Notifications ---
    const notifBody = job.emailNotifications
      ? prop('Email', \`<span class="prop-value mono">\${esc(job.emailNotifications)}</span>\`)
      : '<span class="empty-note">No notifications configured</span>';
    parts.push(section('Notifications', notifBody));

    // --- Job parameters ---
    let paramBody = '<span class="empty-note">No parameters</span>';
    if (job.parameters.length > 0) {
      paramBody = job.parameters.map(p =>
        \`<div class="param-item">
          <div class="param-name">\${esc(p.name)}</div>
          <div class="param-val">\${esc(p.default||'(empty)')}</div>
        </div>\`
      ).join('');
    }
    parts.push(section('Job Parameters', paramBody, false, job.parameters.length||null));

    // --- Queue & concurrency ---
    const queueBody = prop('Queue enabled', job.queueEnabled ? '✓ Yes' : '✗ No')
      + prop('Max concurrent runs', String(job.maxConcurrentRuns ?? 1))
      + (job.timeoutSeconds ? prop('Timeout', (job.timeoutSeconds/60|0) + ' min (' + job.timeoutSeconds + 's)') : '');
    parts.push(section('Queue & Limits', queueBody));

    // --- Permissions ---
    let permBody = '<span class="empty-note">No permissions listed</span>';
    if (job.permissions.length > 0) {
      permBody = job.permissions.map(p =>
        \`<div class="perm-item">
          <div class="param-name">\${esc(p.level.replace(/_/g,' '))}</div>
          <div class="prop-value mono" style="font-size:10px">\${esc(p.principal||p.principalType)}</div>
        </div>\`
      ).join('');
    }
    parts.push(section('Permissions', permBody, false, job.permissions.length||null));

    // --- Clusters ---
    const clusterKeys = Object.keys(job.jobClusters);
    if (clusterKeys.length > 0) {
      const cBody = clusterKeys.map(k => {
        const c = job.jobClusters[k];
        let rows = \`<div class="prop-label" style="margin-bottom:6px">\${esc(k)}</div><div class="cluster-grid">\`;
        if (c.nodeTypeId) rows += \`<span class="cluster-key">Node type</span><span class="cluster-val">\${esc(c.nodeTypeId)}</span>\`;
        if (c.sparkVersion) rows += \`<span class="cluster-key">Runtime</span><span class="cluster-val">\${esc(c.sparkVersion)}</span>\`;
        if (c.minWorkers!=null||c.maxWorkers!=null) rows += \`<span class="cluster-key">Workers</span><span class="cluster-val">\${c.minWorkers}–\${c.maxWorkers}</span>\`;
        else if (c.numWorkers!=null) rows += \`<span class="cluster-key">Workers</span><span class="cluster-val">\${c.numWorkers} (fixed)</span>\`;
        if (c.dataSecurityMode) rows += \`<span class="cluster-key">Security</span><span class="cluster-val">\${esc(c.dataSecurityMode.replace(/_/g,' '))}</span>\`;
        if (c.runtimeEngine) rows += \`<span class="cluster-key">Engine</span><span class="cluster-val">\${esc(c.runtimeEngine)}</span>\`;
        rows += '</div>';
        if (c.initScripts?.length) rows += prop('Init scripts', c.initScripts.map(s=>\`<div class="prop-value mono">\${esc(s)}</div>\`).join(''));
        return rows;
      }).join('<hr style="border:none;border-top:1px solid var(--border);margin:10px 0">');
      parts.push(section('Clusters', cBody, false, clusterKeys.length));
    }

    // --- Serverless environments ---
    const envKeys = Object.keys(job.environments);
    if (envKeys.length > 0) {
      const eBody = envKeys.map(k => {
        const e = job.environments[k];
        let html = \`<div class="prop-label" style="margin-bottom:5px">\${esc(k)}</div>\`;
        if (e.version) html += prop('Version', \`<span class="prop-value mono">\${esc(e.version)}</span>\`);
        if (e.dependencies.length) {
          html += prop('Dependencies', e.dependencies.map(d=>\`<div class="lib-item">\${esc(d)}</div>\`).join(''));
        }
        return html;
      }).join('<hr style="border:none;border-top:1px solid var(--border);margin:10px 0">');
      parts.push(section('Environments', eBody, false, envKeys.length));
    }

    panelContent.innerHTML = parts.join('');
  }

  // ── Task panel ────────────────────────────────────────────────────────────────
  function showTaskPanel(task) {
    const c = nodeColors(task.type);
    const typeLbl = {notebook:'Notebook',condition:'Condition',spark_python:'Spark Python',
                     python_wheel:'Python Wheel',sql:'SQL',run_job:'Run Job',unknown:'Unknown'}[task.type]||'Unknown';

    breadcrumb.innerHTML =
      \`<span class="breadcrumb-link" id="bc-job">Job</span> › <span>\${esc(task.task_key)}</span>\`;
    document.getElementById('bc-job')?.addEventListener('click', deselect);

    const typeBadgeStyle = \`background:\${c.fill};color:\${c.stroke};border:1px solid \${c.stroke}\`;
    const parts = [];

    // Header
    parts.push(\`<div class="panel-header">
      <div class="panel-title">\${esc(task.task_key)}</div>
      <span class="type-badge" style="\${typeBadgeStyle}">\${esc(typeLbl.toUpperCase())}</span>
      \${task.notebook_path ? \`<div class="prop-value mono" style="font-size:10px;margin-top:8px">\${esc(task.notebook_path)}</div>\` : ''}
      \${task.run_if ? \`<div class="run-if-note">Run if: \${esc(task.run_if.replace(/_/g,' ').toLowerCase())}</div>\` : ''}
    </div>\`);

    // --- Dependent tasks (what this task depends on) ---
    let depsBody = '<span class="empty-note">No upstream dependencies (root task)</span>';
    if (task.depends_on.length > 0) {
      depsBody = task.depends_on.map(d => {
        const oc = d.outcome === 'true' ? 'true' : d.outcome === 'false' ? 'false' : null;
        const badge = oc ? \`<span class="outcome-badge outcome-\${oc}">\${oc}</span>\` : '';
        return \`<div class="dep-item">
          <span class="dep-arrow">→</span>
          <span class="dep-key" style="cursor:pointer;color:var(--link)" onclick="selectTaskByKey('\${esc(d.task_key)}')">\${esc(d.task_key)}</span>
          \${badge}
        </div>\`;
      }).join('');
    }
    parts.push(section('Upstream Dependencies', depsBody, true, task.depends_on.length||null));

    // --- Condition expression ---
    if (task.condition) {
      const opLabel = {EQUAL_TO:'==',NOT_EQUAL_TO:'!=',GREATER_THAN:'>',LESS_THAN:'<'}[task.condition.op]||task.condition.op;
      const condBody = \`<div class="cluster-grid">
        <span class="cluster-key">Left</span><span class="cluster-val mono prop-value">\${esc(task.condition.left)}</span>
        <span class="cluster-key">Op</span><span class="cluster-val" style="color:var(--orange);font-family:monospace">\${esc(opLabel)}</span>
        <span class="cluster-key">Right</span><span class="cluster-val mono prop-value">\${esc(task.condition.right)}</span>
      </div>\`;
      parts.push(section('Condition', condBody, true));
    }

    // --- Compute ---
    let computeBody = '';
    if (task.job_cluster_key) {
      const cl = job.jobClusters[task.job_cluster_key];
      computeBody += prop('Type', 'Job Cluster');
      computeBody += prop('Cluster', \`<span class="prop-value mono">\${esc(task.job_cluster_key)}</span>\`);
      if (cl) {
        computeBody += \`<div class="cluster-grid">\`;
        if (cl.nodeTypeId)  computeBody += \`<span class="cluster-key">Node type</span><span class="cluster-val">\${esc(cl.nodeTypeId)}</span>\`;
        if (cl.sparkVersion) computeBody += \`<span class="cluster-key">Runtime</span><span class="cluster-val">\${esc(cl.sparkVersion)}</span>\`;
        if (cl.minWorkers!=null||cl.maxWorkers!=null)
          computeBody += \`<span class="cluster-key">Workers</span><span class="cluster-val">\${cl.minWorkers}–\${cl.maxWorkers} (autoscale)</span>\`;
        else if (cl.numWorkers!=null)
          computeBody += \`<span class="cluster-key">Workers</span><span class="cluster-val">\${cl.numWorkers} (fixed)</span>\`;
        if (cl.dataSecurityMode)
          computeBody += \`<span class="cluster-key">Security</span><span class="cluster-val">\${esc(cl.dataSecurityMode.replace(/_/g,' '))}</span>\`;
        if (cl.runtimeEngine)
          computeBody += \`<span class="cluster-key">Engine</span><span class="cluster-val">\${esc(cl.runtimeEngine)}</span>\`;
        computeBody += '</div>';
        if (cl.initScripts?.length)
          computeBody += prop('Init scripts', cl.initScripts.map(s=>\`<div class="prop-value mono">\${esc(s)}</div>\`).join(''));
      }
    } else if (task.environment_key) {
      const env = job.environments[task.environment_key];
      computeBody += prop('Type', 'Serverless');
      computeBody += prop('Environment', \`<span class="prop-value mono">\${esc(task.environment_key)}</span>\`);
      if (env) {
        if (env.version) computeBody += prop('Version', \`<span class="prop-value mono">\${esc(env.version)}</span>\`);
        if (env.dependencies.length)
          computeBody += prop('Dependencies', env.dependencies.map(d=>\`<div class="lib-item">\${esc(d)}</div>\`).join(''));
      }
    } else {
      computeBody = '<span class="empty-note">No compute defined (inherits from job)</span>';
    }
    if (task.max_retries != null) {
      const retryMs = task.min_retry_interval_millis;
      const retryStr = retryMs ? \` (wait \${retryMs/1000}s)\` : '';
      computeBody += prop('Retries', \`\${task.max_retries} max\${retryStr}\`);
    }
    parts.push(section('Compute', computeBody, true));

    // --- Task parameters ---
    const paramKeys = Object.keys(task.base_parameters || {});
    let taskParamBody = '<span class="empty-note">No task-level parameters</span>';
    if (paramKeys.length > 0) {
      taskParamBody = paramKeys.map(k =>
        \`<div class="param-item">
          <div class="param-name">\${esc(k)}</div>
          <div class="param-val">\${esc(task.base_parameters[k])}</div>
        </div>\`
      ).join('');
    }
    parts.push(section('Task Parameters', taskParamBody, paramKeys.length > 0, paramKeys.length||null));

    // --- Libraries ---
    let libBody = '<span class="empty-note">No libraries</span>';
    if (task.libraries.length > 0) {
      libBody = task.libraries.map(l => \`<div class="lib-item prop-value mono">\${esc(l)}</div>\`).join('');
    }
    parts.push(section('Libraries', libBody, task.libraries.length > 0, task.libraries.length||null));

    panelContent.innerHTML = parts.join('');
  }

  // Global helper for dep click
  window.selectTaskByKey = function(key) { selectTask(key); };

  // ── Zoom & pan ────────────────────────────────────────────────────────────────
  dagSvg.addEventListener('wheel', (e) => {
    e.preventDefault();
    const r = dagSvg.getBoundingClientRect();
    const mx = e.clientX-r.left, my = e.clientY-r.top;
    const d = e.deltaY > 0 ? 0.85 : 1.18;
    tx = mx-(mx-tx)*d; ty = my-(my-ty)*d; sc *= d;
    applyT();
  }, { passive:false });

  let drag=false, dx=0, dy=0;
  dagSvg.addEventListener('mousedown', (e) => {
    const t = e.target;
    if (t===dagSvg || t===zg || t.tagName==='path' || t.tagName==='svg') {
      drag=true; dx=e.clientX; dy=e.clientY;
      dagSvg.classList.add('panning');
    }
  });
  window.addEventListener('mousemove', (e) => {
    if (!drag) return;
    tx+=e.clientX-dx; ty+=e.clientY-dy; dx=e.clientX; dy=e.clientY; applyT();
  });
  window.addEventListener('mouseup', () => { drag=false; dagSvg.classList.remove('panning'); });

  function zoomCenter(delta) {
    const w = document.getElementById('canvas-wrap');
    const cx=w.clientWidth/2, cy=w.clientHeight/2;
    tx=cx-(cx-tx)*delta; ty=cy-(cy-ty)*delta; sc*=delta; applyT();
  }
  document.getElementById('z-in').addEventListener('click', () => zoomCenter(1.25));
  document.getElementById('z-out').addEventListener('click', () => zoomCenter(0.8));
  document.getElementById('z-fit').addEventListener('click', () => { if(_bbox) fitView(_bbox); });

  // ── Sidebar resize ───────────────────────────────────────────────────────────
  const resizeHandle = document.getElementById('resize-handle');
  const sidebarInner = document.getElementById('sidebar-inner');
  let resizing = false, resizeStartX = 0, resizeStartW = 0;
  const MIN_W = 180, MAX_W = 600;

  resizeHandle.addEventListener('mousedown', (e) => {
    if (!sidebarOpen) return;
    resizing = true;
    resizeStartX = e.clientX;
    resizeStartW = sidebar.offsetWidth;
    resizeHandle.classList.add('dragging');
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
    e.preventDefault();
  });

  window.addEventListener('mousemove', (e) => {
    if (!resizing) return;
    const newW = Math.max(MIN_W, Math.min(MAX_W, resizeStartW - (e.clientX - resizeStartX)));
    sidebar.style.width = newW + 'px';
    sidebarInner.style.width = newW + 'px';
  });

  window.addEventListener('mouseup', () => {
    if (!resizing) return;
    resizing = false;
    resizeHandle.classList.remove('dragging');
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
    if (_bbox) fitView(_bbox);
  });

  // ── Init ──────────────────────────────────────────────────────────────────────
  positions = computeLayout(job.tasks);
  _bbox     = draw(job.tasks, positions);
  showJobPanel();
  requestAnimationFrame(() => fitView(_bbox));
  window.addEventListener('resize', () => { if(_bbox) fitView(_bbox); });

})();
</script>
</body>
</html>`;
  }
}

function escapeHtml(s: string): string {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

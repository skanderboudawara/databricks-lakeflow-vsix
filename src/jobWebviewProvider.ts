import * as vscode from 'vscode';
import * as crypto from 'crypto';
import { JobParser, JobDefinition } from './jobParser';

export class JobWebviewProvider implements vscode.WebviewViewProvider {
  public static readonly viewId = 'databricksJobsTree';

  private view?: vscode.WebviewView;
  private jobs: JobDefinition[] = [];

  constructor(
    private readonly parser: JobParser,
    private readonly extensionUri: vscode.Uri,
  ) {}

  resolveWebviewView(
    webviewView: vscode.WebviewView,
    _context: vscode.WebviewViewResolveContext,
    _token: vscode.CancellationToken,
  ): void {
    this.view = webviewView;
    webviewView.webview.options = {
      enableScripts: true,
      localResourceRoots: [this.extensionUri],
    };

    webviewView.webview.onDidReceiveMessage((msg) => {
      if (msg.command === 'openDag' && msg.job) {
        vscode.commands.executeCommand('databricksJobViewer.openDag', msg.job);
      }
      if (msg.command === 'openFile' && msg.filePath) {
        vscode.workspace.openTextDocument(msg.filePath).then((doc) => {
          vscode.window.showTextDocument(doc);
        });
      }
    });

    this.loadAndRender();
  }

  refresh(): void {
    this.loadAndRender();
  }

  private async loadAndRender(): Promise<void> {
    if (this.view) {
      this.view.webview.html = this.buildHtml(null); // show loading state
    }
    this.jobs = await this.parser.findAllJobs();
    if (this.view) {
      this.view.webview.html = this.buildHtml(this.jobs);
    }
  }

  private buildHtml(jobs: JobDefinition[] | null): string {
    const nonce = crypto.randomBytes(16).toString('hex');
    const jobsJson = jobs ? JSON.stringify(jobs).replace(/</g, '\\u003c') : 'null';

    return /* html */ `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta http-equiv="Content-Security-Policy"
    content="default-src 'none'; style-src 'unsafe-inline'; script-src 'nonce-${nonce}';">
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      background: transparent;
      color: var(--vscode-foreground);
      font-family: var(--vscode-font-family, 'Segoe UI', sans-serif);
      font-size: var(--vscode-font-size, 13px);
      display: flex;
      flex-direction: column;
      height: 100vh;
      overflow: hidden;
    }

    /* ── Search bar ─────────────────────────────────────────────────────── */
    #search-wrap {
      flex-shrink: 0;
      padding: 6px 8px;
      position: relative;
      border-bottom: 1px solid var(--vscode-sideBarSectionHeader-border, transparent);
    }
    #search-icon {
      position: absolute;
      left: 16px;
      top: 50%;
      transform: translateY(-50%);
      font-size: 13px;
      opacity: 0.4;
      pointer-events: none;
    }
    #search {
      width: 100%;
      padding: 4px 22px 4px 26px;
      background: var(--vscode-input-background);
      color: var(--vscode-input-foreground);
      border: 1px solid var(--vscode-input-border, var(--vscode-widget-border, transparent));
      border-radius: 3px;
      font-family: inherit;
      font-size: 12px;
      outline: none;
    }
    #search:focus { border-color: var(--vscode-focusBorder); }
    #search::placeholder { color: var(--vscode-input-placeholderForeground); }
    #search-clear {
      position: absolute;
      right: 12px;
      top: 50%;
      transform: translateY(-50%);
      background: none;
      border: none;
      color: var(--vscode-descriptionForeground);
      cursor: pointer;
      font-size: 14px;
      line-height: 1;
      padding: 0;
      display: none;
    }
    #search-clear:hover { color: var(--vscode-foreground); }

    /* ── Scrollable list ────────────────────────────────────────────────── */
    #job-list {
      flex: 1;
      overflow-y: auto;
      overflow-x: hidden;
    }

    /* ── Category / subcategory headers ─────────────────────────────────── */
    .cat-header {
      display: flex;
      align-items: center;
      gap: 4px;
      padding: 4px 8px;
      cursor: pointer;
      user-select: none;
      font-size: 11px;
      font-weight: 600;
      letter-spacing: 0.05em;
      text-transform: uppercase;
      color: var(--vscode-sideBarSectionHeader-foreground, var(--vscode-foreground));
      background: var(--vscode-sideBarSectionHeader-background, rgba(128,128,128,0.1));
    }
    .cat-header:hover { background: var(--vscode-list-hoverBackground); }
    .subcat-header {
      display: flex;
      align-items: center;
      gap: 4px;
      padding: 3px 8px 3px 18px;
      cursor: pointer;
      user-select: none;
      font-size: 12px;
      color: var(--vscode-foreground);
    }
    .subcat-header:hover { background: var(--vscode-list-hoverBackground); }

    .arrow {
      width: 10px;
      flex-shrink: 0;
      font-size: 9px;
      display: inline-block;
      transition: transform 0.12s;
      opacity: 0.6;
    }
    .arrow.open { transform: rotate(90deg); }
    .hdr-count {
      margin-left: auto;
      font-size: 10px;
      font-weight: 400;
      color: var(--vscode-descriptionForeground);
    }

    /* ── Job items ──────────────────────────────────────────────────────── */
    .job-item {
      display: flex;
      align-items: center;
      gap: 5px;
      padding: 3px 8px 3px 32px;
      cursor: pointer;
      font-size: 12px;
      white-space: nowrap;
      position: relative;
    }
    .job-item:hover { background: var(--vscode-list-hoverBackground); }
    .job-item-icon { flex-shrink: 0; opacity: 0.65; font-size: 12px; }
    .job-item-name { flex: 1; overflow: hidden; text-overflow: ellipsis; }
    .job-item-count { font-size: 10px; color: var(--vscode-descriptionForeground); flex-shrink: 0; }
    .job-open-btn {
      opacity: 0;
      background: none;
      border: none;
      color: var(--vscode-descriptionForeground);
      cursor: pointer;
      font-size: 12px;
      padding: 0 2px;
      flex-shrink: 0;
      line-height: 1;
    }
    .job-item:hover .job-open-btn { opacity: 1; }
    .job-open-btn:hover { color: var(--vscode-foreground); }

    /* Search result items (flat mode) */
    .job-item.flat { padding-left: 8px; }
    .job-item.flat .job-item-sub {
      font-size: 10px;
      color: var(--vscode-descriptionForeground);
      overflow: hidden;
      text-overflow: ellipsis;
    }

    mark {
      background: transparent;
      color: var(--vscode-editorWarning-foreground, #e5c07b);
      font-weight: 700;
    }

    .info-msg {
      padding: 12px 10px;
      font-size: 12px;
      color: var(--vscode-descriptionForeground);
    }
  </style>
</head>
<body>
  <div id="search-wrap">
    <span id="search-icon">⌕</span>
    <input id="search" type="text" placeholder="Search jobs…" autocomplete="off" spellcheck="false">
    <button id="search-clear" title="Clear">✕</button>
  </div>
  <div id="job-list">
    <div class="info-msg">Loading…</div>
  </div>

  <script nonce="${nonce}">
  (function() {
    const vscode = acquireVsCodeApi();
    const jobs = ${jobsJson};

    const searchInput = document.getElementById('search');
    const searchClear = document.getElementById('search-clear');
    const jobList    = document.getElementById('job-list');

    if (!jobs) { return; } // still loading

    // Persist collapse state across view reloads
    let colState = {};
    try { colState = vscode.getState()?.col || {}; } catch(e) {}
    function saveState() { try { vscode.setState({ col: colState }); } catch(e) {} }

    function esc(s) {
      return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
    }

    function fuzzyMatch(q, target) {
      const ql = q.toLowerCase(), tl = target.toLowerCase();
      let qi = 0;
      for (let i = 0; i < tl.length && qi < ql.length; i++) {
        if (tl[i] === ql[qi]) { qi++; }
      }
      return qi === ql.length;
    }

    function fuzzyHighlight(q, text) {
      if (!q) { return esc(text); }
      const ql = q.toLowerCase();
      let out = '', qi = 0;
      for (let i = 0; i < text.length; i++) {
        const ch = text[i];
        if (qi < ql.length && ch.toLowerCase() === ql[qi]) {
          out += '<mark>' + esc(ch) + '</mark>';
          qi++;
        } else {
          out += esc(ch);
        }
      }
      return out;
    }

    // Group jobs by category → subcategory
    const grouped = new Map();
    for (const job of jobs) {
      if (!grouped.has(job.category)) { grouped.set(job.category, new Map()); }
      const byCat = grouped.get(job.category);
      if (!byCat.has(job.subCategory)) { byCat.set(job.subCategory, []); }
      byCat.get(job.subCategory).push(job);
    }
    const CAT_ORDER = ['app_jobs', 'etl_jobs'];

    function jobItemHtml(job, idx, flat, query) {
      const name = flat ? fuzzyHighlight(query, job.name) : esc(job.name);
      const subLine = flat
        ? '<div class="job-item-sub">' + esc(job.subCategory) + '</div>'
        : '';
      return '<div class="job-item' + (flat ? ' flat' : '') + '" data-job="' + idx + '">'
        + '<span class="job-item-icon">◈</span>'
        + '<div style="flex:1;min-width:0"><div class="job-item-name">' + name + '</div>' + subLine + '</div>'
        + '<span class="job-item-count">' + esc(String(job.tasks.length)) + '</span>'
        + '<button class="job-open-btn" data-open="' + esc(job.filePath) + '" title="Open YAML">↗</button>'
        + '</div>';
    }

    function renderNormal() {
      if (jobs.length === 0) {
        jobList.innerHTML = '<div class="info-msg">No jobs found in workspace.</div>';
        return;
      }
      let html = '';
      const cats = [...grouped.keys()].sort(function(a, b) {
        const ia = CAT_ORDER.indexOf(a), ib = CAT_ORDER.indexOf(b);
        if (ia >= 0 && ib >= 0) { return ia - ib; }
        if (ia >= 0) { return -1; } if (ib >= 0) { return 1; }
        return a.localeCompare(b);
      });
      for (const cat of cats) {
        const byCat  = grouped.get(cat);
        const label  = cat === 'app_jobs' ? 'App Jobs' : cat === 'etl_jobs' ? 'ETL Jobs' : cat;
        const catOpen = colState['c:' + cat] !== false;
        const total  = [...byCat.values()].reduce(function(n, arr) { return n + arr.length; }, 0);
        html += '<div data-cat="' + esc(cat) + '">'
          + '<div class="cat-header" data-tcl="c:' + esc(cat) + '" data-body="cb:' + esc(cat) + '">'
          + '<span class="arrow' + (catOpen ? ' open' : '') + '">▶</span>'
          + '<span>' + esc(label) + '</span>'
          + '<span class="hdr-count">' + total + '</span>'
          + '</div>'
          + '<div id="cb:' + esc(cat) + '" style="' + (catOpen ? '' : 'display:none') + '">';
        const subs = [...byCat.keys()].sort();
        for (const sub of subs) {
          const subjobs = byCat.get(sub);
          const subKey  = 's:' + cat + ':' + sub;
          const subOpen = colState[subKey] !== false;
          html += '<div>'
            + '<div class="subcat-header" data-tcl="' + esc(subKey) + '" data-body="' + esc(subKey) + 'b">'
            + '<span class="arrow' + (subOpen ? ' open' : '') + '">▶</span>'
            + '<span>' + esc(sub) + '</span>'
            + '<span class="hdr-count">' + subjobs.length + '</span>'
            + '</div>'
            + '<div id="' + esc(subKey) + 'b" style="' + (subOpen ? '' : 'display:none') + '">';
          for (const job of subjobs) {
            html += jobItemHtml(job, jobs.indexOf(job), false, '');
          }
          html += '</div></div>';
        }
        html += '</div></div>';
      }
      jobList.innerHTML = html;
    }

    function renderSearch(query) {
      const matches = jobs.filter(function(j) { return fuzzyMatch(query, j.name); });
      if (matches.length === 0) {
        jobList.innerHTML = '<div class="info-msg">No jobs match <strong>' + esc(query) + '</strong></div>';
        return;
      }
      jobList.innerHTML = matches.map(function(job) {
        return jobItemHtml(job, jobs.indexOf(job), true, query);
      }).join('');
    }

    function render() {
      const q = searchInput.value.trim();
      searchClear.style.display = q ? 'block' : 'none';
      if (q) { renderSearch(q); } else { renderNormal(); }
    }

    // Event delegation on the list
    jobList.addEventListener('click', function(e) {
      // Open file button
      const openBtn = e.target.closest('[data-open]');
      if (openBtn) {
        e.stopPropagation();
        vscode.postMessage({ command: 'openFile', filePath: openBtn.dataset.open });
        return;
      }
      // Toggle collapse
      const hdr = e.target.closest('[data-tcl]');
      if (hdr) {
        const key    = hdr.dataset.tcl;
        const bodyId = hdr.dataset.body;
        const body   = document.getElementById(bodyId);
        const arrow  = hdr.querySelector('.arrow');
        if (!body) { return; }
        const open = body.style.display !== 'none';
        body.style.display = open ? 'none' : '';
        if (arrow) { arrow.classList.toggle('open', !open); }
        colState[key] = !open;
        saveState();
        return;
      }
      // Open job DAG
      const item = e.target.closest('[data-job]');
      if (item) {
        const idx = parseInt(item.dataset.job, 10);
        vscode.postMessage({ command: 'openDag', job: jobs[idx], allJobNames: jobs.map(function(j) { return j.name; }) });
      }
    });

    searchInput.addEventListener('input', render);
    searchClear.addEventListener('click', function() {
      searchInput.value = '';
      render();
      searchInput.focus();
    });

    render();
  })();
  </script>
</body>
</html>`;
  }
}

import * as crypto from 'crypto';
import * as vscode from 'vscode';

import type { JobDefinition, JobUpdates } from './jobParser';
import { JobParser, saveJobUpdates } from './jobParser';

/**
 * Manages the webview panel that renders a job's DAG as an interactive SVG canvas.
 * One panel per job name — reopening the same job reveals the existing panel instead
 * of creating a duplicate.
 */
export class DagPanel {
  private static panels = new Map<string, DagPanel>();

  private readonly panel: vscode.WebviewPanel;
  private readonly extensionUri: vscode.Uri;
  private disposables: vscode.Disposable[] = [];

  /**
   * Opens the DAG panel for `job`, revealing the existing panel if one is already open
   * for that job name, otherwise creating a new webview panel in the active editor column.
   */
  static createOrShow(
    extensionUri: vscode.Uri,
    job: JobDefinition,
    allJobNames: string[] = [],
  ): void {
    const column = vscode.window.activeTextEditor?.viewColumn ?? vscode.ViewColumn.One;

    if (DagPanel.panels.has(job.name)) {
      DagPanel.panels.get(job.name)!.panel.reveal(column);
      return;
    }

    const panel = vscode.window.createWebviewPanel('databricksJobDag', job.name, column, {
      enableScripts: true,
      retainContextWhenHidden: true,
      localResourceRoots: [extensionUri],
    });

    new DagPanel(panel, extensionUri, job, allJobNames);
  }

  private constructor(
    panel: vscode.WebviewPanel,
    extensionUri: vscode.Uri,
    job: JobDefinition,
    allJobNames: string[] = [],
  ) {
    this.panel = panel;
    this.extensionUri = extensionUri;
    DagPanel.panels.set(job.name, this);

    this.panel.webview.html = this.buildHtml(this.panel.webview, job, allJobNames);

    this.panel.onDidDispose(
      () => {
        DagPanel.panels.delete(job.name);
        this.dispose();
      },
      null,
      this.disposables,
    );

    this.panel.webview.onDidReceiveMessage(
      async (message: {
        command: string;
        filePath?: string;
        notebookPath?: string;
        jobName?: string;
        updates?: JobUpdates;
      }) => {
        if (message.command === 'openFile' && message.filePath) {
          const doc = await vscode.workspace.openTextDocument(message.filePath);
          vscode.window.showTextDocument(doc);
        }

        if (message.command === 'openNotebook' && message.notebookPath) {
          const folders = vscode.workspace.workspaceFolders;
          if (!folders) {
            return;
          }
          for (const ext of ['.py', '.ipynb', '']) {
            const pattern = new vscode.RelativePattern(
              folders[0],
              message.notebookPath + ext,
            );
            const found = await vscode.workspace.findFiles(pattern, null, 1);
            if (found.length > 0) {
              const doc = await vscode.workspace.openTextDocument(found[0]);
              vscode.window.showTextDocument(doc);
              return;
            }
          }
          vscode.window.showWarningMessage(`Notebook not found: ${message.notebookPath}`);
        }

        if (
          message.command === 'saveJob' &&
          message.filePath &&
          message.jobName &&
          message.updates
        ) {
          try {
            saveJobUpdates(message.filePath, message.jobName, message.updates);
            const updatedJob = new JobParser().parseFile(message.filePath);
            vscode.window.showInformationMessage(`Job "${message.jobName}" saved successfully.`);
            this.panel.webview.postMessage({ type: 'saveDone', updatedJob });
          } catch (err) {
            const msg = err instanceof Error ? err.message : String(err);
            vscode.window.showErrorMessage(`Save failed: ${msg}`);
            this.panel.webview.postMessage({ type: 'saveError', error: msg });
          }
        }

        // Immediate task add/remove — writes to file but does NOT reset pending edits
        if (
          message.command === 'applyJobChange' &&
          message.filePath &&
          message.jobName &&
          message.updates
        ) {
          try {
            saveJobUpdates(message.filePath, message.jobName, message.updates);
            const updatedJob = new JobParser().parseFile(message.filePath);
            this.panel.webview.postMessage({ type: 'jobChanged', updatedJob });
          } catch (err) {
            const msg = err instanceof Error ? err.message : String(err);
            vscode.window.showErrorMessage(`Failed: ${msg}`);
          }
        }
      },
      null,
      this.disposables,
    );
  }

  /**
   * Disposes the underlying webview panel and all event-listener disposables
   * registered during construction.
   */
  private dispose(): void {
    this.panel.dispose();
    for (const d of this.disposables) {
      d.dispose();
    }
    this.disposables = [];
  }

  // ─── HTML ────────────────────────────────────────────────────────────────────

  /**
   * Builds the full HTML document for the DAG webview.
   * Generates a per-render nonce for the Content Security Policy, resolves
   * `media/dag-panel.css` and `media/dag-panel.js` to webview-accessible URIs,
   * and injects the job data via `window.__DAG_JOB__` and `window.__DAG_ALL_JOB_NAMES__`
   * so the external script can bootstrap without an inline data blob.
   */
  private buildHtml(
    webview: vscode.Webview,
    job: JobDefinition,
    allJobNames: string[] = [],
  ): string {
    const nonce = crypto.randomBytes(16).toString('hex');
    const jobJson = JSON.stringify(job).replace(/</g, '\\u003c');
    const allJobNamesJson = JSON.stringify(allJobNames).replace(/</g, '\\u003c');

    const cssUri = webview.asWebviewUri(
      vscode.Uri.joinPath(this.extensionUri, 'media', 'dag-panel.css'),
    );
    const jsUri = webview.asWebviewUri(
      vscode.Uri.joinPath(this.extensionUri, 'media', 'dag-panel.js'),
    );
    const csp = `default-src 'none'; style-src ${webview.cspSource}; script-src 'nonce-${nonce}' ${webview.cspSource};`;

    return /* html */ `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta http-equiv="Content-Security-Policy" content="${csp}">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>${job.name}</title>
  <link rel="stylesheet" href="${cssUri}">
</head>
<body>

<!-- Top bar -->
<div class="topbar">
  <span class="topbar-icon">⬡</span>
  <input type="text" class="topbar-name" id="hdr-name" title="Click to edit job name">
  <span class="pill" id="hdr-tasks"></span>
  <span class="pill pill-trigger" id="hdr-trigger" style="display:none"></span>
  <span class="pill pill-cond"    id="hdr-cond"    style="display:none">Conditions</span>
  <button class="topbar-btn" id="btn-save">Save</button>
  <button class="topbar-btn" id="btn-revert" style="display:none">Revert</button>
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

    <!-- Add-task FAB -->
    <button class="add-task-fab" id="btn-add-task" title="Add new task">+</button>

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
      <div class="sidebar-strip">
        <span class="breadcrumb" id="sidebar-breadcrumb">Job</span>
        <button class="collapse-btn" id="btn-collapse" title="Collapse panel">✕</button>
      </div>
      <div id="panel-content"></div>
    </div>
  </div>

</div><!-- .body -->

<!-- Bottom bar (task details, shown on task click) -->
<div class="bottom-bar" id="bottom-bar">
  <div class="bottom-bar-resize" id="bottom-bar-resize"></div>
  <div class="bottom-bar-inner" id="bottom-bar-inner">
    <div class="bottom-bar-strip">
      <span class="breadcrumb" id="task-breadcrumb"></span>
      <button class="collapse-btn" id="btn-close-bottom" title="Close">✕</button>
    </div>
    <div id="task-panel-content"></div>
  </div>
</div>

<!-- Library editor modal -->
<div class="lib-backdrop" id="lib-backdrop">
  <div class="lib-modal">
    <div class="lib-modal-header">
      <span class="lib-modal-title" id="lib-modal-title">Libraries</span>
      <button class="lib-modal-done" onclick="closeAndSaveModal()">Done</button>
    </div>
    <div class="lib-list" id="lib-list"></div>
    <div class="lib-add-form">
      <div class="lib-add-row" style="flex-wrap:wrap">
        <div style="flex:0 0 auto">
          <label>Type</label>
          <select class="prop-input" id="lib-add-type" onchange="updateLibForm()" style="width:160px">
            <option value="whl">whl — Python wheel</option>
            <option value="jar">jar — JAR file</option>
            <option value="pypi">pypi — PyPI package</option>
            <option value="maven">maven — Maven package</option>
            <option value="requirements">requirements — requirements.txt</option>
          </select>
        </div>
        <div id="lib-form-path" style="flex:1;min-width:180px">
          <label>Path</label>
          <input id="lib-path" class="prop-input" type="text" placeholder="/Volumes/…/lib.whl">
        </div>
        <div id="lib-form-pypi-pkg" style="flex:1;min-width:180px;display:none">
          <label>Package</label>
          <input id="lib-pypi-pkg" class="prop-input" type="text" placeholder="numpy==1.25.2">
        </div>
        <div id="lib-form-maven-coords" style="flex:1;min-width:180px;display:none">
          <label>Coordinates</label>
          <input id="lib-maven-coords" class="prop-input" type="text" placeholder="com.example:lib:1.0">
        </div>
        <div style="flex:0 0 auto;display:flex;align-items:flex-end">
          <button class="lib-add-btn" onclick="addLib()">+ Add</button>
        </div>
      </div>
      <div id="lib-form-opts" style="display:none;flex-direction:column;gap:6px">
        <div id="lib-form-repo" style="display:none">
          <label>Repo (optional)</label>
          <input id="lib-repo" class="prop-input" type="text" placeholder="https://pypi.org/simple/">
        </div>
        <div id="lib-form-excl" style="display:none">
          <label>Exclusions (optional — one per line)</label>
          <textarea id="lib-excl" class="prop-input" rows="2" style="resize:vertical;font-family:var(--mono);font-size:10px"></textarea>
        </div>
      </div>
    </div>
  </div>
</div>

<script nonce="${nonce}">
  window.__DAG_JOB__ = ${jobJson};
  window.__DAG_ALL_JOB_NAMES__ = ${allJobNamesJson};
</script>
<script nonce="${nonce}" src="${jsUri}"></script>
</body>
</html>`;
  }
}

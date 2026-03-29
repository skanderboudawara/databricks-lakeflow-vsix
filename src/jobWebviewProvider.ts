import * as crypto from 'crypto';
import * as vscode from 'vscode';

import type { JobDefinition , JobParser } from './jobParser';

/**
 * VS Code `WebviewViewProvider` that renders the collapsible job list in the
 * Databricks Jobs activity-bar sidebar panel.
 * The view shows all jobs discovered under `resources/{app_jobs,etl_jobs}`, grouped
 * by category and sub-category, with a fuzzy-search bar at the top.
 */
export class JobWebviewProvider implements vscode.WebviewViewProvider {
  /** The VS Code view ID that matches the `views` contribution in `package.json`. */
  public static readonly viewId = 'databricksJobsTree';

  private view?: vscode.WebviewView;
  private jobs: JobDefinition[] = [];

  constructor(
    private readonly parser: JobParser,
    private readonly extensionUri: vscode.Uri,
  ) {}

  /**
   * Called by VS Code when the sidebar panel is first made visible.
   * Configures webview options, registers the message listener for `openDag` and
   * `openFile` commands, and triggers the initial job discovery.
   */
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

  /**
   * Re-discovers all job YAML files and re-renders the sidebar with up-to-date data.
   * Called by the refresh command registered in `extension.ts`.
   */
  refresh(): void {
    this.loadAndRender();
  }

  /**
   * Shows a loading state immediately, then asynchronously fetches all jobs and
   * replaces the loading HTML with the populated job list.
   */
  private async loadAndRender(): Promise<void> {
    if (this.view) {
      this.view.webview.html = this.buildHtml(this.view.webview, null);
    }
    try {
      this.jobs = await this.parser.findAllJobs();
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (this.view) {
        this.view.webview.html = this.buildErrorHtml(this.view.webview, msg);
      }
      return;
    }
    if (this.view) {
      this.view.webview.html = this.buildHtml(this.view.webview, this.jobs);
    }
  }

  /**
   * Builds the full HTML document for the job-list webview.
   * Resolves `media/job-webview.css` and `media/job-webview.js` to webview-safe URIs,
   * enforces a strict Content Security Policy via a per-render nonce, and injects
   * `window.__JOB_LIST__` so the external script can render without inline data.
   * Pass `null` for `jobs` to render the initial loading state.
   */
  /**
   * Builds a minimal error HTML page shown when job discovery throws an unexpected
   * exception, so the sidebar never stays stuck on "Loading…".
   */
  private buildErrorHtml(webview: vscode.Webview, message: string): string {
    const nonce = crypto.randomBytes(16).toString('hex');
    const csp = `default-src 'none'; style-src 'nonce-${nonce}';`;
    const safe = message.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    return /* html */ `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta http-equiv="Content-Security-Policy" content="${csp}">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <style nonce="${nonce}">
    body { font-family: var(--vscode-font-family); font-size: 12px; padding: 12px; color: var(--vscode-errorForeground); }
    pre { white-space: pre-wrap; word-break: break-all; margin-top: 8px; font-size: 11px; color: var(--vscode-descriptionForeground); }
  </style>
</head>
<body>
  <strong>Failed to load jobs</strong>
  <pre>${safe}</pre>
</body>
</html>`;
  }

  /**
   * Builds the full HTML document for the job-list webview.
   * Resolves `media/job-webview.css` and `media/job-webview.js` to webview-safe URIs,
   * enforces a strict Content Security Policy via a per-render nonce, and injects
   * `window.__JOB_LIST__` so the external script can render without inline data.
   * Pass `null` for `jobs` to render the initial loading state.
   */
  private buildHtml(webview: vscode.Webview, jobs: JobDefinition[] | null): string {
    const nonce = crypto.randomBytes(16).toString('hex');
    const jobsJson = jobs ? JSON.stringify(jobs).replace(/</g, '\\u003c') : 'null';

    const cssUri = webview.asWebviewUri(
      vscode.Uri.joinPath(this.extensionUri, 'media', 'job-webview.css'),
    );
    const jsUri = webview.asWebviewUri(
      vscode.Uri.joinPath(this.extensionUri, 'media', 'job-webview.js'),
    );
    const csp = `default-src 'none'; style-src ${webview.cspSource}; script-src 'nonce-${nonce}' ${webview.cspSource};`;

    return /* html */ `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta http-equiv="Content-Security-Policy" content="${csp}">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Databricks Jobs</title>
  <link rel="stylesheet" href="${cssUri}">
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
    window.__JOB_LIST__ = ${jobsJson};
  </script>
  <script nonce="${nonce}" src="${jsUri}"></script>
</body>
</html>`;
  }
}

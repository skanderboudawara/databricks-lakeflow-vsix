import * as vscode from 'vscode';

import { DagPanel } from './dagPanel';
import { JobParser } from './jobParser';
import { JobWebviewProvider } from './jobWebviewProvider';

/**
 * VS Code extension lifecycle hook — called once when the extension first activates.
 * Registers the webview view provider for the jobs sidebar, the refresh command,
 * and the openDag command that opens a job's DAG panel.
 */
export function activate(context: vscode.ExtensionContext): void {
  const parser = new JobParser();
  const jobProvider = new JobWebviewProvider(parser, context.extensionUri);

  context.subscriptions.push(
    vscode.window.registerWebviewViewProvider(JobWebviewProvider.viewId, jobProvider),
  );

  context.subscriptions.push(
    vscode.commands.registerCommand('databricksJobViewer.refresh', () => {
      jobProvider.refresh();
    }),
  );

  context.subscriptions.push(
    vscode.commands.registerCommand('databricksJobViewer.openDag', (job, allJobNames) => {
      DagPanel.createOrShow(context.extensionUri, job, allJobNames || []);
    }),
  );
}

/**
 * VS Code extension lifecycle hook — called when the extension is deactivated.
 * No additional teardown is required beyond the automatic disposal of registered subscriptions.
 */
export function deactivate(): void {}

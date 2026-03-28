import * as vscode from 'vscode';
import { JobParser } from './jobParser';
import { JobWebviewProvider } from './jobWebviewProvider';
import { DagPanel } from './dagPanel';

export function activate(context: vscode.ExtensionContext): void {
  const parser = new JobParser();
  const jobProvider = new JobWebviewProvider(parser, context.extensionUri);

  context.subscriptions.push(
    vscode.window.registerWebviewViewProvider(JobWebviewProvider.viewId, jobProvider)
  );

  context.subscriptions.push(
    vscode.commands.registerCommand('databricksJobViewer.refresh', () => {
      jobProvider.refresh();
    })
  );

  context.subscriptions.push(
    vscode.commands.registerCommand('databricksJobViewer.openDag', (job, allJobNames) => {
      DagPanel.createOrShow(context.extensionUri, job, allJobNames || []);
    })
  );
}

export function deactivate(): void {}

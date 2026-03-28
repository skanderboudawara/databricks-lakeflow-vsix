import * as vscode from 'vscode';
import { JobParser } from './jobParser';
import { JobTreeProvider } from './jobTreeProvider';
import { DagPanel } from './dagPanel';

export function activate(context: vscode.ExtensionContext): void {
  const parser = new JobParser();
  const treeProvider = new JobTreeProvider(parser);

  vscode.window.registerTreeDataProvider('databricksJobsTree', treeProvider);

  context.subscriptions.push(
    vscode.commands.registerCommand('databricksJobViewer.refresh', () => {
      treeProvider.refresh();
    })
  );

  context.subscriptions.push(
    vscode.commands.registerCommand('databricksJobViewer.openDag', (job) => {
      DagPanel.createOrShow(context.extensionUri, job);
    })
  );

  context.subscriptions.push(
    vscode.commands.registerCommand('databricksJobViewer.openFile', (item) => {
      if (item?.job?.filePath) {
        vscode.workspace.openTextDocument(item.job.filePath).then((doc) => {
          vscode.window.showTextDocument(doc);
        });
      }
    })
  );
}

export function deactivate(): void {}

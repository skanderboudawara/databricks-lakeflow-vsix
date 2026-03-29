import * as path from 'path';
import * as vscode from 'vscode';

import type { JobParser, JobDefinition } from './jobParser';

/** Discriminated union of the three tree-item kinds used in the sidebar view. */
type TreeNode = CategoryNode | SubCategoryNode | JobNode;

/**
 * Tree item representing a top-level job category (e.g. `App Jobs`, `ETL Jobs`).
 * Starts expanded so users immediately see the sub-categories beneath it.
 */
class CategoryNode extends vscode.TreeItem {
  readonly kind = 'category';
  constructor(
    public readonly label: string,
    public readonly children: SubCategoryNode[],
  ) {
    super(label, vscode.TreeItemCollapsibleState.Expanded);
    this.iconPath = new vscode.ThemeIcon('folder');
    this.contextValue = 'category';
  }
}

/**
 * Tree item representing a sub-category folder within a category.
 * Corresponds to the directory immediately beneath `resources/{app_jobs,etl_jobs}/`.
 */
class SubCategoryNode extends vscode.TreeItem {
  readonly kind = 'subcategory';
  constructor(
    public readonly label: string,
    public readonly children: JobNode[],
  ) {
    super(label, vscode.TreeItemCollapsibleState.Collapsed);
    this.iconPath = new vscode.ThemeIcon('folder-opened');
    this.contextValue = 'subcategory';
    this.description = `${children.length} job${children.length !== 1 ? 's' : ''}`;
  }
}

/**
 * Leaf tree item representing a single Databricks job.
 * Clicking it triggers the `databricksJobViewer.openDag` command.
 */
class JobNode extends vscode.TreeItem {
  readonly kind = 'job';
  constructor(public readonly job: JobDefinition) {
    super(job.name, vscode.TreeItemCollapsibleState.None);
    this.iconPath = new vscode.ThemeIcon('circuit-board');
    this.contextValue = 'job';
    this.description = `${job.tasks.length} tasks`;
    this.tooltip = this.buildTooltip(job);
    this.command = {
      command: 'databricksJobViewer.openDag',
      title: 'Open Job DAG',
      arguments: [job],
    };
  }

  /**
   * Builds a Markdown tooltip summarising the job's task count, trigger type,
   * optional timeout, and the base file name of the YAML source file.
   */
  private buildTooltip(job: JobDefinition): vscode.MarkdownString {
    const lines = [`**${job.name}**`, `Tasks: ${job.tasks.length}`];

    if (job.trigger) {
      const triggerLabel = {
        table_update: `Table update (${job.trigger.tableNames?.length ?? 0} tables)`,
        periodic: `Scheduled: ${job.trigger.cronExpression ?? 'cron'}`,
        file_arrival: 'File arrival',
        manual: 'Manual',
      }[job.trigger.type];
      lines.push(`Trigger: ${triggerLabel}`);
    }

    if (job.timeoutSeconds) {
      lines.push(`Timeout: ${Math.round(job.timeoutSeconds / 60)} min`);
    }

    lines.push('', `*${path.basename(job.filePath)}*`);
    return new vscode.MarkdownString(lines.join('\n\n'));
  }
}

/**
 * Returns `true` if every character in `query` appears in `target` in order,
 * using a case-insensitive subsequence match (fuzzy search).
 */
function fuzzyMatch(query: string, target: string): boolean {
  const q = query.toLowerCase();
  const t = target.toLowerCase();
  let qi = 0;
  for (let i = 0; i < t.length && qi < q.length; i++) {
    if (t[i] === q[qi]) {
      qi++;
    }
  }
  return qi === q.length;
}

/**
 * Tree data provider for the Databricks Jobs sidebar view.
 * Organizes jobs by category (app_jobs / etl_jobs) and sub-category (folder name).
 */
export class JobTreeProvider implements vscode.TreeDataProvider<TreeNode> {
  private _onDidChangeTreeData = new vscode.EventEmitter<TreeNode | undefined | null>();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  private rootNodes: CategoryNode[] = [];
  private allJobs: JobDefinition[] = [];
  private filterQuery = '';
  private loading = false;

  constructor(private readonly parser: JobParser) {
    this.loadJobs();
  }

  /**
   * Re-loads all job files from disk and fires a tree-data-changed event
   * to refresh the entire sidebar view.
   */
  refresh(): void {
    this.loadJobs();
  }

  /**
   * Updates the fuzzy-search filter string and fires a tree-data-changed event
   * so the view immediately re-renders showing only matching jobs.
   */
  setFilter(query: string): void {
    this.filterQuery = query.trim();
    this._onDidChangeTreeData.fire(null);
  }

  /** Returns the current fuzzy-search filter string. */
  getFilter(): string {
    return this.filterQuery;
  }

  /**
   * Returns the tree item for display.
   * Elements are already `TreeItem` instances, so this is a direct pass-through.
   */
  getTreeItem(element: TreeNode): vscode.TreeItem {
    return element;
  }

  /**
   * Returns the children for a given tree node.
   * At the root level, returns all category nodes (or a flat filtered list when a
   * search query is active). Delegates to the node's own `children` array otherwise.
   */
  getChildren(element?: TreeNode): vscode.ProviderResult<TreeNode[]> {
    if (this.loading) {
      return [];
    }

    // Filtered mode: flat list of matching jobs (no category grouping)
    if (!element && this.filterQuery) {
      return this.allJobs
        .filter((job) => fuzzyMatch(this.filterQuery, job.name))
        .map((job) => new JobNode(job));
    }

    if (!element) {
      return this.rootNodes;
    }

    if (element instanceof CategoryNode) {
      return element.children;
    }

    if (element instanceof SubCategoryNode) {
      return element.children;
    }

    return [];
  }

  /**
   * Discovers all job YAML files via the parser, groups them into a
   * category → sub-category hierarchy, builds the `CategoryNode` tree,
   * and fires a refresh event when done.
   */
  private async loadJobs(): Promise<void> {
    this.loading = true;
    this._onDidChangeTreeData.fire(null);

    const jobs = await this.parser.findAllJobs();
    this.allJobs = jobs;

    // Group by category → subCategory
    const grouped = new Map<string, Map<string, JobDefinition[]>>();

    for (const job of jobs) {
      if (!grouped.has(job.category)) {
        grouped.set(job.category, new Map());
      }
      const byCategory = grouped.get(job.category)!;
      if (!byCategory.has(job.subCategory)) {
        byCategory.set(job.subCategory, []);
      }
      byCategory.get(job.subCategory)!.push(job);
    }

    // Build tree nodes, ordering app_jobs before etl_jobs
    const categoryOrder = ['app_jobs', 'etl_jobs'];
    const sortedCategories = [...grouped.keys()].sort((a, b) => {
      const ia = categoryOrder.indexOf(a);
      const ib = categoryOrder.indexOf(b);
      if (ia >= 0 && ib >= 0) {
        return ia - ib;
      }
      if (ia >= 0) {
        return -1;
      }
      if (ib >= 0) {
        return 1;
      }
      return a.localeCompare(b);
    });

    this.rootNodes = sortedCategories.map((cat) => {
      const byCategory = grouped.get(cat)!;
      const sortedSubs = [...byCategory.keys()].sort();

      const subNodes = sortedSubs.map((sub) => {
        const jobNodes = byCategory.get(sub)!.map((j) => new JobNode(j));
        return new SubCategoryNode(sub, jobNodes);
      });

      const label = cat === 'app_jobs' ? 'App Jobs' : cat === 'etl_jobs' ? 'ETL Jobs' : cat;
      return new CategoryNode(label, subNodes);
    });

    this.loading = false;
    this._onDidChangeTreeData.fire(null);
  }
}

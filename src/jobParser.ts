import * as vscode from 'vscode';
import * as yaml from 'js-yaml';
import * as path from 'path';
import * as fs from 'fs';

export type TaskType = 'notebook' | 'condition' | 'spark_python' | 'python_wheel' | 'sql' | 'run_job' | 'unknown';

export interface TaskDependency {
  task_key: string;
  outcome?: string;
}

export interface Task {
  task_key: string;
  type: TaskType;
  notebook_path?: string;
  depends_on: TaskDependency[];
  run_if?: string;
  job_cluster_key?: string;
  environment_key?: string;
  max_retries?: number;
  min_retry_interval_millis?: number;
  condition?: { op: string; left: string; right: string };
  base_parameters: Record<string, string>;
  libraries: string[];
}

export interface TriggerInfo {
  type: 'table_update' | 'periodic' | 'file_arrival' | 'manual';
  pauseStatus?: string;
  tableNames?: string[];
  tableCondition?: string;
  cronExpression?: string;
  timezone?: string;
}

export interface JobCluster {
  key: string;
  policyId?: string;
  nodeTypeId?: string;
  sparkVersion?: string;
  minWorkers?: number;
  maxWorkers?: number;
  numWorkers?: number;
  dataSecurityMode?: string;
  runtimeEngine?: string;
  initScripts?: string[];
}

export interface JobEnvironment {
  key: string;
  version?: string;
  dependencies: string[];
}

export interface JobParameter {
  name: string;
  default: string;
}

export interface HealthRule {
  metric: string;
  op: string;
  value: number;
}

export interface JobPermission {
  level: string;
  principal: string;
  principalType: 'user' | 'group' | 'service_principal';
}

export interface JobDefinition {
  name: string;
  filePath: string;
  category: string;
  subCategory: string;
  tasks: Task[];
  trigger?: TriggerInfo;
  timeoutSeconds?: number;
  maxConcurrentRuns?: number;
  hasConditions: boolean;
  jobClusters: Record<string, JobCluster>;
  environments: Record<string, JobEnvironment>;
  parameters: JobParameter[];
  health: HealthRule[];
  emailNotifications: string;
  queueEnabled: boolean;
  permissions: JobPermission[];
}

export class JobParser {
  /**
   * Parse a single YAML job file into a JobDefinition.
   * Returns null if the file does not contain a valid job definition.
   */
  parseFile(filePath: string): JobDefinition | null {
    try {
      const content = fs.readFileSync(filePath, 'utf8');
      const doc = yaml.load(content) as Record<string, unknown>;

      const resources = doc?.resources as Record<string, unknown>;
      const jobs = resources?.jobs as Record<string, unknown>;
      if (!jobs) {
        return null;
      }

      const jobKey = Object.keys(jobs)[0];
      const job = jobs[jobKey] as Record<string, unknown>;
      if (!job) {
        return null;
      }

      const rawTasks = (job.tasks as unknown[]) || [];
      const tasks: Task[] = rawTasks.map((t) => this.parseTask(t as Record<string, unknown>));

      const { category, subCategory } = this.extractCategory(filePath);

      return {
        name: (job.name as string) || jobKey,
        filePath,
        category,
        subCategory,
        tasks,
        trigger:
          this.parseTrigger(job.trigger as Record<string, unknown>) ??
          this.parseSchedule(job.schedule as Record<string, unknown>),
        timeoutSeconds: job.timeout_seconds as number | undefined,
        maxConcurrentRuns: job.max_concurrent_runs as number | undefined,
        hasConditions: tasks.some((t) => t.type === 'condition'),
        jobClusters: this.parseJobClusters(job.job_clusters as unknown[]),
        environments: this.parseEnvironments(job.environments as unknown[]),
        parameters: this.parseParameters(job.parameters as unknown[]),
        health: this.parseHealth(job.health as Record<string, unknown>),
        emailNotifications: this.parseEmailNotifications(job.email_notifications),
        queueEnabled: (job.queue as Record<string, unknown> | undefined)?.enabled === true,
        permissions: this.parsePermissions(job.permissions as unknown[]),
      };
    } catch (e) {
      console.error(`[JobParser] Failed to parse ${filePath}:`, e);
      return null;
    }
  }

  /** Find and parse all job YAML files under resources/{app_jobs,etl_jobs}. */
  async findAllJobs(): Promise<JobDefinition[]> {
    const workspaceFolders = vscode.workspace.workspaceFolders;
    if (!workspaceFolders) {
      return [];
    }

    const jobs: JobDefinition[] = [];

    for (const folder of workspaceFolders) {
      const pattern = new vscode.RelativePattern(folder, 'resources/{app_jobs,etl_jobs}/**/*.yml');
      const files = await vscode.workspace.findFiles(pattern);

      for (const file of files) {
        const job = this.parseFile(file.fsPath);
        if (job) {
          jobs.push(job);
        }
      }
    }

    return jobs.sort((a, b) => a.name.localeCompare(b.name));
  }

  // ─── Private parsers ────────────────────────────────────────────────────────

  private parseTask(t: Record<string, unknown>): Task {
    let type: TaskType = 'unknown';
    let notebook_path: string | undefined;
    let condition: Task['condition'] | undefined;
    let base_parameters: Record<string, string> = {};

    if (t.notebook_task) {
      type = 'notebook';
      const nt = t.notebook_task as Record<string, unknown>;
      notebook_path = ((nt.notebook_path as string) || '')
        .replace(/\$\{workspace\.file_path\}\//g, '')
        .replace(/\$\{[^}]+\}\//g, '');
      base_parameters = (nt.base_parameters as Record<string, string>) || {};
    } else if (t.condition_task) {
      type = 'condition';
      const ct = t.condition_task as Record<string, string>;
      condition = { op: ct.op, left: ct.left, right: ct.right };
    } else if (t.spark_python_task) {
      type = 'spark_python';
    } else if (t.python_wheel_task) {
      type = 'python_wheel';
    } else if (t.sql_task) {
      type = 'sql';
    } else if (t.run_job_task) {
      type = 'run_job';
    }

    const rawDeps = (t.depends_on as unknown[]) || [];
    const depends_on: TaskDependency[] = rawDeps.map((d) => {
      const dep = d as Record<string, string>;
      return { task_key: dep.task_key, outcome: dep.outcome };
    });

    const rawLibs = (t.libraries as unknown[]) || [];
    const libraries: string[] = rawLibs.map((lib) => {
      const l = lib as Record<string, unknown>;
      if (l.jar) return `jar: ${l.jar}`;
      if (l.pypi) {
        const p = l.pypi as Record<string, string>;
        return `pypi: ${p.package}`;
      }
      if (l.whl) return `whl: ${l.whl}`;
      if (l.maven) {
        const m = l.maven as Record<string, string>;
        return `maven: ${m.coordinates}`;
      }
      return JSON.stringify(lib);
    });

    return {
      task_key: t.task_key as string,
      type,
      notebook_path,
      depends_on,
      run_if: t.run_if as string | undefined,
      job_cluster_key: t.job_cluster_key as string | undefined,
      environment_key: t.environment_key as string | undefined,
      max_retries: t.max_retries as number | undefined,
      min_retry_interval_millis: t.min_retry_interval_millis as number | undefined,
      condition,
      base_parameters,
      libraries,
    };
  }

  private parseTrigger(trigger: Record<string, unknown> | undefined): TriggerInfo | undefined {
    if (!trigger) {
      return undefined;
    }

    const pauseStatus = trigger.pause_status as string | undefined;

    if (trigger.table_update) {
      const tu = trigger.table_update as Record<string, unknown>;
      const tableNames = ((tu.table_names as string[]) || []).map((n) =>
        n.replace(/\$\{[^}]+\}\./g, '')
      );
      return {
        type: 'table_update',
        pauseStatus,
        tableNames,
        tableCondition: tu.condition as string | undefined,
      };
    }

    if (trigger.periodic) {
      const p = trigger.periodic as Record<string, unknown>;
      const cron = p.cron as Record<string, string> | undefined;
      return {
        type: 'periodic',
        pauseStatus,
        cronExpression: cron?.quartz_cron_expression,
        timezone: cron?.timezone_id,
      };
    }

    if (trigger.file_arrival) {
      return { type: 'file_arrival', pauseStatus };
    }

    return { type: 'manual' };
  }

  /** Handles the top-level `schedule:` key (quartz cron, used by ETL jobs). */
  private parseSchedule(schedule: Record<string, unknown> | undefined): TriggerInfo | undefined {
    if (!schedule) {
      return undefined;
    }
    return {
      type: 'periodic',
      pauseStatus: schedule.pause_status as string | undefined,
      cronExpression: schedule.quartz_cron_expression as string | undefined,
      timezone: schedule.timezone_id as string | undefined,
    };
  }

  private parseJobClusters(raw: unknown[] | undefined): Record<string, JobCluster> {
    const result: Record<string, JobCluster> = {};
    if (!raw) {
      return result;
    }

    for (const c of raw) {
      const cluster = c as Record<string, unknown>;
      const key = cluster.job_cluster_key as string;
      if (!key) {
        continue;
      }

      const nc = (cluster.new_cluster as Record<string, unknown>) || {};
      const autoscale = nc.autoscale as Record<string, number> | undefined;

      const rawInits = (nc.init_scripts as unknown[]) || [];
      const initScripts: string[] = rawInits.map((is) => {
        const s = is as Record<string, unknown>;
        if (s.s3) {
          return ((s.s3 as Record<string, string>).destination || 's3://…').replace(
            /\$\{[^}]+\}\//g,
            ''
          );
        }
        if (s.workspace) {
          return (s.workspace as Record<string, string>).destination || 'workspace://…';
        }
        return JSON.stringify(s);
      });

      result[key] = {
        key,
        policyId: nc.policy_id as string | undefined,
        nodeTypeId: nc.node_type_id as string | undefined,
        sparkVersion: nc.spark_version as string | undefined,
        minWorkers: autoscale?.min_workers,
        maxWorkers: autoscale?.max_workers,
        numWorkers: nc.num_workers as number | undefined,
        dataSecurityMode: nc.data_security_mode as string | undefined,
        runtimeEngine: nc.runtime_engine as string | undefined,
        initScripts: initScripts.length > 0 ? initScripts : undefined,
      };
    }

    return result;
  }

  private parseEnvironments(raw: unknown[] | undefined): Record<string, JobEnvironment> {
    const result: Record<string, JobEnvironment> = {};
    if (!raw) {
      return result;
    }

    for (const e of raw) {
      const env = e as Record<string, unknown>;
      const key = env.environment_key as string;
      if (!key) {
        continue;
      }

      const spec = (env.spec as Record<string, unknown>) || {};
      result[key] = {
        key,
        version: spec.environment_version as string | undefined,
        dependencies: (spec.dependencies as string[]) || [],
      };
    }

    return result;
  }

  private parseParameters(raw: unknown[] | undefined): JobParameter[] {
    if (!raw) {
      return [];
    }
    return raw.map((p) => {
      const param = p as Record<string, string>;
      return { name: param.name || '', default: param.default || '' };
    });
  }

  private parseHealth(health: Record<string, unknown> | undefined): HealthRule[] {
    const rules = (health?.rules as unknown[]) || [];
    return rules.map((r) => {
      const rule = r as Record<string, unknown>;
      return {
        metric: rule.metric as string,
        op: rule.op as string,
        value: rule.value as number,
      };
    });
  }

  private parseEmailNotifications(raw: unknown): string {
    if (!raw) {
      return '';
    }
    if (typeof raw === 'string') {
      return raw;
    }
    // Object with on_failure / on_start / on_success arrays
    const obj = raw as Record<string, unknown>;
    const parts: string[] = [];
    for (const [event, recipients] of Object.entries(obj)) {
      if (Array.isArray(recipients) && recipients.length > 0) {
        parts.push(`${event}: ${recipients.join(', ')}`);
      } else if (typeof recipients === 'string') {
        parts.push(`${event}: ${recipients}`);
      }
    }
    return parts.join(' | ') || JSON.stringify(raw);
  }

  private parsePermissions(raw: unknown[] | undefined): JobPermission[] {
    if (!raw) {
      return [];
    }
    return raw.map((p) => {
      const perm = p as Record<string, string>;
      let principal = '';
      let principalType: JobPermission['principalType'] = 'user';

      if (perm.user_name) {
        principal = perm.user_name;
        principalType = 'user';
      } else if (perm.group_name) {
        principal = perm.group_name;
        principalType = 'group';
      } else if (perm.service_principal_name) {
        principal = perm.service_principal_name;
        principalType = 'service_principal';
      }

      return { level: perm.level, principal, principalType };
    });
  }

  private extractCategory(filePath: string): { category: string; subCategory: string } {
    const parts = filePath.split(path.sep).join('/').split('/');
    const resIdx = parts.findIndex((p) => p === 'resources');

    if (resIdx < 0) {
      return { category: 'other', subCategory: 'other' };
    }

    return {
      category: parts[resIdx + 1] || 'other',
      subCategory: parts[resIdx + 2] || 'other',
    };
  }
}

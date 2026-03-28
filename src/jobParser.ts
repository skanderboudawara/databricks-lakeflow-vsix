import * as vscode from 'vscode';
import * as yaml from 'js-yaml';
import * as path from 'path';
import * as fs from 'fs';

export type TaskType = 'notebook' | 'condition' | 'spark_python' | 'python_wheel' | 'sql' | 'run_job' | 'unknown';

export type LibraryType = 'whl' | 'jar' | 'pypi' | 'maven' | 'requirements';

export interface Library {
  type: LibraryType;
  path?: string;         // whl, jar, requirements
  package?: string;      // pypi
  repo?: string;         // pypi, maven
  coordinates?: string;  // maven
  exclusions?: string[]; // maven
}

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
  libraries: Library[];
}

export interface TriggerInfo {
  type: 'table_update' | 'periodic' | 'file_arrival' | 'manual';
  pauseStatus?: string;
  tableNames?: string[];
  tableCondition?: string;
  cronExpression?: string;
  timezone?: string;
  interval?: number;
  intervalUnit?: string;
  fileArrivalUrl?: string;
  minTimeBetweenTriggersSec?: number;
  waitAfterLastChangeSec?: number;
}

export interface JobCluster {
  key: string;
  policyId?: string;
  nodeTypeId?: string;
  driverNodeTypeId?: string;
  sparkVersion?: string;
  minWorkers?: number;
  maxWorkers?: number;
  numWorkers?: number;
  autoterminationMinutes?: number;
  dataSecurityMode?: string;
  runtimeEngine?: string;
  initScripts?: string[];
  sparkConf?: Record<string, string>;
  sparkEnvVars?: Record<string, string>;
  singleUserName?: string;
  instancePoolId?: string;
  enableElasticDisk?: boolean;
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
  description?: string;
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
        description: job.description as string | undefined,
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
    const libraries: Library[] = rawLibs.map((lib) => {
      const l = lib as Record<string, unknown>;
      if (l.jar) return { type: 'jar', path: String(l.jar) };
      if (l.whl) return { type: 'whl', path: String(l.whl) };
      if (l.requirements) return { type: 'requirements', path: String(l.requirements) };
      if (l.pypi) {
        const p = l.pypi as Record<string, string>;
        return { type: 'pypi', package: p.package, repo: p.repo };
      }
      if (l.maven) {
        const m = l.maven as Record<string, any>;
        return {
          type: 'maven',
          coordinates: m.coordinates,
          repo: m.repo,
          exclusions: Array.isArray(m.exclusions) ? m.exclusions.map(String) : undefined,
        };
      }
      return { type: 'whl', path: JSON.stringify(lib) };
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
      if (p.interval !== undefined) {
        return {
          type: 'periodic',
          pauseStatus,
          interval: p.interval as number,
          intervalUnit: p.unit as string | undefined,
        };
      }
      const cron = p.cron as Record<string, string> | undefined;
      return {
        type: 'periodic',
        pauseStatus,
        cronExpression: cron?.quartz_cron_expression,
        timezone: cron?.timezone_id,
      };
    }

    if (trigger.file_arrival) {
      const fa = trigger.file_arrival as Record<string, unknown>;
      return {
        type: 'file_arrival',
        pauseStatus,
        fileArrivalUrl: fa.url as string | undefined,
        minTimeBetweenTriggersSec: fa.min_time_between_triggers_seconds as number | undefined,
        waitAfterLastChangeSec: fa.wait_after_last_change_seconds as number | undefined,
      };
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
        driverNodeTypeId: nc.driver_node_type_id as string | undefined,
        sparkVersion: nc.spark_version as string | undefined,
        minWorkers: autoscale?.min_workers,
        maxWorkers: autoscale?.max_workers,
        numWorkers: nc.num_workers as number | undefined,
        autoterminationMinutes: nc.autotermination_minutes as number | undefined,
        dataSecurityMode: nc.data_security_mode as string | undefined,
        runtimeEngine: nc.runtime_engine as string | undefined,
        initScripts: initScripts.length > 0 ? initScripts : undefined,
        sparkConf: nc.spark_conf as Record<string, string> | undefined,
        sparkEnvVars: nc.spark_env_vars as Record<string, string> | undefined,
        singleUserName: nc.single_user_name as string | undefined,
        instancePoolId: nc.instance_pool_id as string | undefined,
        enableElasticDisk: nc.enable_elastic_disk as boolean | undefined,
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

// ─── Job Updates ─────────────────────────────────────────────────────────────

export interface JobUpdates {
  name?: string;
  description?: string;
  trigger?: {
    type?: string;
    pauseStatus?: string;
    cronExpression?: string;
    timezone?: string;
    tableCondition?: string;
    tableNames?: string[];
    fileArrivalUrl?: string;
    interval?: number;
    intervalUnit?: string;
  };
  timeoutSeconds?: number | null;
  maxConcurrentRuns?: number | null;
  queueEnabled?: boolean;
  health?: Array<{ value?: number | null }>;
  parameters?: Array<{ default?: string }>;
  jobClusters?: Record<string, {
    nodeTypeId?: string;
    driverNodeTypeId?: string;
    sparkVersion?: string;
    numWorkers?: number | null;
    minWorkers?: number | null;
    maxWorkers?: number | null;
    autoterminationMinutes?: number | null;
    policyId?: string;
    dataSecurityMode?: string;
    runtimeEngine?: string;
    sparkConf?: Record<string, string>;
    sparkEnvVars?: Record<string, string>;
    singleUserName?: string;
    instancePoolId?: string;
    enableElasticDisk?: boolean;
  }>;
  _newClusters?: Array<Record<string, any>>;
  _removedClusterKeys?: string[];
  environments?: Record<string, {
    version?: string;
    dependencies?: string[];
  }>;
  _newEnvironments?: Array<Record<string, any>>;
  _removedEnvironmentKeys?: string[];
  _newTasks?: Array<Record<string, any>>;
  _removedTaskKeys?: string[];
  _renamedTasks?: Record<string, string>;
  tasks?: Record<string, {
    _newKey?: string;
    notebook_path?: string;
    run_if?: string;
    max_retries?: number | null;
    min_retry_interval_millis?: number | null;
    job_cluster_key?: string;
    environment_key?: string;
    base_parameters?: Record<string, string>;
    libraries?: Library[];
    depends_on?: Array<{ task_key: string; outcome?: string }>;
  }>;
}

export function saveJobUpdates(filePath: string, jobName: string, updates: JobUpdates): void {
  const content = fs.readFileSync(filePath, 'utf8');
  const doc = yaml.load(content) as Record<string, any>;
  const jobNode = doc?.resources?.jobs?.[jobName];
  if (!jobNode) {
    throw new Error(`Job "${jobName}" not found in ${filePath}`);
  }
  applyUpdatesToJobNode(jobNode, updates);
  const newContent = yaml.dump(doc, { lineWidth: 120, noRefs: true, indent: 2 });
  fs.writeFileSync(filePath, newContent, 'utf8');
}

function applyUpdatesToJobNode(jobNode: Record<string, any>, updates: JobUpdates): void {
  // Job name
  if (updates.name !== undefined && updates.name) jobNode.name = updates.name;

  // Description
  if (updates.description !== undefined) jobNode.description = updates.description || undefined;

  // Trigger / schedule — handle type change first
  if (updates.trigger?.type !== undefined) {
    const newType = updates.trigger.type;
    // Always save pause_status then wipe both schedule and trigger before rebuilding
    const savedPauseStatus = (jobNode.trigger || jobNode.schedule || {}).pause_status;
    delete jobNode.trigger;
    delete jobNode.schedule;
    if (newType !== 'manual') {
      jobNode.trigger = {};
      if (savedPauseStatus) jobNode.trigger.pause_status = savedPauseStatus;
      if (newType === 'table_update') {
        jobNode.trigger.table_update = {};
      } else if (newType === 'periodic') {
        jobNode.trigger.periodic = { cron: { quartz_cron_expression: '0 0 * * * ?', timezone_id: 'UTC' } };
      } else if (newType === 'file_arrival') {
        jobNode.trigger.file_arrival = { url: '' };
      }
    }
  }

  // Trigger / schedule
  if (updates.trigger) {
    const t = updates.trigger;
    if (jobNode.schedule) {
      if (t.cronExpression !== undefined) jobNode.schedule.quartz_cron_expression = t.cronExpression || undefined;
      if (t.timezone !== undefined) jobNode.schedule.timezone_id = t.timezone || undefined;
      if (t.pauseStatus !== undefined) jobNode.schedule.pause_status = t.pauseStatus || undefined;
    }
    if (jobNode.trigger) {
      if (t.pauseStatus !== undefined) jobNode.trigger.pause_status = t.pauseStatus || undefined;
      if (jobNode.trigger.table_update) {
        if (t.tableCondition !== undefined) jobNode.trigger.table_update.condition = t.tableCondition || undefined;
        if (t.tableNames !== undefined) jobNode.trigger.table_update.table_names = t.tableNames.length > 0 ? t.tableNames : undefined;
      }
      if (jobNode.trigger.file_arrival) {
        if (t.fileArrivalUrl !== undefined) jobNode.trigger.file_arrival.url = t.fileArrivalUrl || undefined;
      }
      if (jobNode.trigger.periodic) {
        if (t.interval !== undefined) jobNode.trigger.periodic.interval = t.interval;
        if (t.intervalUnit !== undefined) jobNode.trigger.periodic.unit = t.intervalUnit || undefined;
      }
    }
  }

  if (updates.timeoutSeconds !== undefined) jobNode.timeout_seconds = updates.timeoutSeconds || undefined;
  if (updates.maxConcurrentRuns !== undefined) jobNode.max_concurrent_runs = updates.maxConcurrentRuns || undefined;
  if (updates.queueEnabled !== undefined) {
    if (!jobNode.queue) jobNode.queue = {};
    jobNode.queue.enabled = updates.queueEnabled;
  }

  if (updates.health && Array.isArray(jobNode.health?.rules)) {
    updates.health.forEach((ru, i) => {
      if (jobNode.health.rules[i] && ru.value !== undefined) {
        jobNode.health.rules[i].value = ru.value ?? undefined;
      }
    });
  }

  if (updates.parameters && Array.isArray(jobNode.parameters)) {
    updates.parameters.forEach((pu, i) => {
      if (jobNode.parameters[i] && pu.default !== undefined) {
        jobNode.parameters[i].default = pu.default;
      }
    });
  }

  // Remove clusters first, then add, then edit
  if (updates._removedClusterKeys && updates._removedClusterKeys.length > 0) {
    if (Array.isArray(jobNode.job_clusters)) {
      jobNode.job_clusters = jobNode.job_clusters.filter(
        (c: any) => !updates._removedClusterKeys!.includes(c.job_cluster_key)
      );
    }
  }
  if (updates._newClusters && updates._newClusters.length > 0) {
    if (!jobNode.job_clusters) jobNode.job_clusters = [];
    for (const nc of updates._newClusters) {
      jobNode.job_clusters.push(nc);
    }
  }
  if (updates.jobClusters) {
    const rawClusters: any[] = Array.isArray(jobNode.job_clusters) ? jobNode.job_clusters : [];
    for (const [key, cu] of Object.entries(updates.jobClusters)) {
      const rc = rawClusters.find((c: any) => c.job_cluster_key === key);
      if (!rc) continue;
      if (!rc.new_cluster) rc.new_cluster = {};
      const nc = rc.new_cluster;
      if (cu.nodeTypeId !== undefined) nc.node_type_id = cu.nodeTypeId || undefined;
      if (cu.driverNodeTypeId !== undefined) nc.driver_node_type_id = cu.driverNodeTypeId || undefined;
      if (cu.sparkVersion !== undefined) nc.spark_version = cu.sparkVersion || undefined;
      if (cu.policyId !== undefined) nc.policy_id = cu.policyId || undefined;
      if (cu.dataSecurityMode !== undefined) nc.data_security_mode = cu.dataSecurityMode || undefined;
      if (cu.runtimeEngine !== undefined) nc.runtime_engine = cu.runtimeEngine || undefined;
      if (cu.singleUserName !== undefined) nc.single_user_name = cu.singleUserName || undefined;
      if (cu.instancePoolId !== undefined) nc.instance_pool_id = cu.instancePoolId || undefined;
      if (cu.autoterminationMinutes !== undefined) nc.autotermination_minutes = cu.autoterminationMinutes ?? undefined;
      if (cu.enableElasticDisk !== undefined) nc.enable_elastic_disk = cu.enableElasticDisk;
      if (cu.numWorkers !== undefined) {
        nc.num_workers = cu.numWorkers ?? undefined;
        nc.autoscale = undefined;
      }
      if (cu.minWorkers !== undefined || cu.maxWorkers !== undefined) {
        if (!nc.autoscale) nc.autoscale = {};
        if (cu.minWorkers !== undefined) nc.autoscale.min_workers = cu.minWorkers ?? undefined;
        if (cu.maxWorkers !== undefined) nc.autoscale.max_workers = cu.maxWorkers ?? undefined;
        nc.num_workers = undefined;
      }
      if (cu.sparkConf !== undefined) nc.spark_conf = Object.keys(cu.sparkConf).length > 0 ? cu.sparkConf : undefined;
      if (cu.sparkEnvVars !== undefined) nc.spark_env_vars = Object.keys(cu.sparkEnvVars).length > 0 ? cu.sparkEnvVars : undefined;
    }
  }

  // Remove envs first, then add, then edit
  if (updates._removedEnvironmentKeys && updates._removedEnvironmentKeys.length > 0) {
    if (Array.isArray(jobNode.environments)) {
      jobNode.environments = jobNode.environments.filter(
        (e: any) => !updates._removedEnvironmentKeys!.includes(e.environment_key)
      );
    }
  }
  if (updates._newEnvironments && updates._newEnvironments.length > 0) {
    if (!jobNode.environments) jobNode.environments = [];
    for (const ne of updates._newEnvironments) {
      jobNode.environments.push(ne);
    }
  }
  if (updates.environments) {
    const envArr: any[] = Array.isArray(jobNode.environments) ? jobNode.environments : [];
    for (const [key, eu] of Object.entries(updates.environments)) {
      const re = envArr.find((e: any) => e.environment_key === key);
      if (!re) continue;
      if (!re.spec) re.spec = {};
      if (eu.version !== undefined) re.spec.environment_version = eu.version || undefined;
      if (eu.dependencies !== undefined) re.spec.dependencies = eu.dependencies.length > 0 ? eu.dependencies : undefined;
    }
  }

  if (updates.tasks) {
    const rawTasks: any[] = Array.isArray(jobNode.tasks) ? jobNode.tasks : [];
    for (const [taskKey, tu] of Object.entries(updates.tasks)) {
      const rt = rawTasks.find((t: any) => t.task_key === taskKey);
      if (!rt) continue;
      if (tu.depends_on !== undefined) {
        rt.depends_on = tu.depends_on.map(d => d.outcome
          ? { task_key: d.task_key, outcome: d.outcome }
          : { task_key: d.task_key }
        );
      }
      if (tu.notebook_path !== undefined && rt.notebook_task) rt.notebook_task.notebook_path = tu.notebook_path || undefined;
      if (tu.run_if !== undefined) rt.run_if = tu.run_if || undefined;
      if (tu.max_retries !== undefined) rt.max_retries = tu.max_retries ?? undefined;
      if (tu.min_retry_interval_millis !== undefined) rt.min_retry_interval_millis = tu.min_retry_interval_millis ?? undefined;
      if (tu.job_cluster_key !== undefined) rt.job_cluster_key = tu.job_cluster_key || undefined;
      if (tu.environment_key !== undefined) rt.environment_key = tu.environment_key || undefined;
      if (tu.base_parameters && rt.notebook_task) {
        if (!rt.notebook_task.base_parameters) rt.notebook_task.base_parameters = {};
        Object.assign(rt.notebook_task.base_parameters, tu.base_parameters);
      }
      if (tu._newKey !== undefined && tu._newKey && tu._newKey !== taskKey) {
        rt.task_key = tu._newKey;
        // Update depends_on references in all tasks
        for (const t2 of rawTasks) {
          if (Array.isArray(t2.depends_on)) {
            for (const dep of t2.depends_on) {
              if (dep.task_key === taskKey) dep.task_key = tu._newKey;
            }
          }
        }
      }
      if (tu.libraries !== undefined) {
        rt.libraries = tu.libraries.map(lib => {
          if (lib.type === 'jar') return { jar: lib.path };
          if (lib.type === 'whl') return { whl: lib.path };
          if (lib.type === 'requirements') return { requirements: lib.path };
          if (lib.type === 'pypi') {
            const p: Record<string, string> = { package: lib.package! };
            if (lib.repo) p.repo = lib.repo;
            return { pypi: p };
          }
          if (lib.type === 'maven') {
            const m: Record<string, any> = { coordinates: lib.coordinates! };
            if (lib.repo) m.repo = lib.repo;
            if (lib.exclusions?.length) m.exclusions = lib.exclusions;
            return { maven: m };
          }
          return {};
        }).filter((l: any) => Object.keys(l).length > 0);
      }
    }
  }

  // Remove tasks (and clean up their deps from other tasks)
  if (updates._removedTaskKeys && updates._removedTaskKeys.length > 0) {
    const removed = new Set(updates._removedTaskKeys);
    if (Array.isArray(jobNode.tasks)) {
      jobNode.tasks = jobNode.tasks.filter((t: any) => !removed.has(t.task_key));
      for (const t of jobNode.tasks) {
        if (Array.isArray(t.depends_on)) {
          t.depends_on = t.depends_on.filter((d: any) => !removed.has(d.task_key));
          if (t.depends_on.length === 0) { delete t.depends_on; }
        }
      }
    }
  }

  // New tasks appended to the job
  if (updates._newTasks && updates._newTasks.length > 0) {
    if (!jobNode.tasks) { jobNode.tasks = []; }
    for (const nt of updates._newTasks) {
      jobNode.tasks.push(nt);
    }
  }

  // Task renames
  if (updates._renamedTasks) {
    const rawTasks2: any[] = Array.isArray(jobNode.tasks) ? jobNode.tasks : [];
    for (const [oldKey, newKey] of Object.entries(updates._renamedTasks)) {
      if (!newKey || oldKey === newKey) continue;
      const rt = rawTasks2.find((t: any) => t.task_key === oldKey);
      if (rt) rt.task_key = newKey;
      for (const t2 of rawTasks2) {
        if (Array.isArray(t2.depends_on)) {
          for (const dep of t2.depends_on) {
            if (dep.task_key === oldKey) dep.task_key = newKey;
          }
        }
      }
    }
  }
}

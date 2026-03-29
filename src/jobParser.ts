import * as fs from 'fs';
import * as path from 'path';
import * as yaml from 'js-yaml';
import * as vscode from 'vscode';


/**
 * Generic YAML document node — a `Record<string, unknown>` used throughout the
 * parser and update helpers to represent raw YAML data before values are narrowed
 * to specific types via explicit casts.
 */
type YamlNode = Record<string, unknown>;

/**
 * Union of all supported Databricks task types.
 * `'unknown'` is used as a fallback when the task YAML contains an unrecognised sub-key.
 */
export type TaskType =
  | 'notebook'
  | 'condition'
  | 'spark_python'
  | 'python_wheel'
  | 'sql'
  | 'run_job'
  | 'unknown';

/** Union of all supported library attachment types for a Databricks task. */
export type LibraryType = 'whl' | 'jar' | 'pypi' | 'maven' | 'requirements';

/**
 * A library dependency attached to a Databricks task.
 * Which fields are populated depends on `type` — `path` for whl/jar/requirements,
 * `package` + optional `repo` for pypi, and `coordinates` + optional `repo` +
 * optional `exclusions` for maven.
 */
export interface Library {
  type: LibraryType;
  path?: string; // whl, jar, requirements
  package?: string; // pypi
  repo?: string; // pypi, maven
  coordinates?: string; // maven
  exclusions?: string[]; // maven
}

/**
 * An upstream task dependency edge.
 * `outcome` constrains the edge to a specific condition result (`'true'` / `'false'`)
 * and is only present on edges leaving a condition task.
 */
export interface TaskDependency {
  task_key: string;
  outcome?: string;
}

/** A parsed Databricks workflow task, normalised from raw YAML into a typed object. */
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

/**
 * Normalised trigger or schedule information, extracted from either the `trigger:`
 * or the `schedule:` YAML key. The `type` discriminant determines which optional
 * fields are populated.
 */
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

/** A job-level cluster definition, keyed by `job_cluster_key` in the YAML. */
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

/** A serverless environment definition, keyed by `environment_key` in the YAML. */
export interface JobEnvironment {
  key: string;
  version?: string;
  dependencies: string[];
}

/** A named job parameter with a default value. */
export interface JobParameter {
  name: string;
  default: string;
}

/** A single health metric rule, e.g. `num_runs_in_error > 3`. */
export interface HealthRule {
  metric: string;
  op: string;
  value: number;
}

/**
 * A permission entry granting `level` access to a user, group,
 * or service principal on the job.
 */
export interface JobPermission {
  level: string;
  principal: string;
  principalType: 'user' | 'group' | 'service_principal';
}

/**
 * A fully parsed Databricks job, combining metadata, tasks, trigger, clusters,
 * environments, parameters, health rules, and permissions into a single typed object.
 */
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

/**
 * Parses Databricks Asset Bundle YAML files into `JobDefinition` objects and
 * discovers all job files in the current VS Code workspace.
 */
export class JobParser {
  /**
   * Parses a single YAML job file into a `JobDefinition`.
   * Returns `null` if the file does not contain a valid `resources.jobs` block
   * or if parsing fails for any reason.
   */
  parseFile(filePath: string): JobDefinition | null {
    try {
      const content = fs.readFileSync(filePath, 'utf8');
      const doc = yaml.load(content) as YamlNode;

      const resources = doc?.resources as YamlNode | undefined;
      const jobs = resources?.jobs as YamlNode | undefined;
      if (!jobs) {
        return null;
      }

      const jobKey = Object.keys(jobs)[0];
      const job = jobs[jobKey] as YamlNode | undefined;
      if (!job) {
        return null;
      }

      const rawTasks = (job.tasks as unknown[]) || [];
      const tasks: Task[] = rawTasks.map((t) => this.parseTask(t as YamlNode));

      const { category, subCategory } = this.extractCategory(filePath);

      return {
        name: (job.name as string) || jobKey,
        description: job.description as string | undefined,
        filePath,
        category,
        subCategory,
        tasks,
        trigger:
          this.parseTrigger(job.trigger as YamlNode | undefined) ??
          this.parseSchedule(job.schedule as YamlNode | undefined),
        timeoutSeconds: job.timeout_seconds as number | undefined,
        maxConcurrentRuns: job.max_concurrent_runs as number | undefined,
        hasConditions: tasks.some((t) => t.type === 'condition'),
        jobClusters: this.parseJobClusters(job.job_clusters as unknown[] | undefined),
        environments: this.parseEnvironments(job.environments as unknown[] | undefined),
        parameters: this.parseParameters(job.parameters as unknown[] | undefined),
        health: this.parseHealth(job.health as YamlNode | undefined),
        emailNotifications: this.parseEmailNotifications(job.email_notifications),
        queueEnabled: (job.queue as YamlNode | undefined)?.enabled === true,
        permissions: this.parsePermissions(job.permissions as unknown[] | undefined),
      };
    } catch (e) {
      console.error(`[JobParser] Failed to parse ${filePath}:`, e);
      return null;
    }
  }

  /** Finds and parses all job YAML files under `resources/{app_jobs,etl_jobs}` in every workspace folder. */
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

  /**
   * Parses a raw YAML task node into a strongly-typed `Task`, inferring the task
   * type from whichever sub-key is present (`notebook_task`, `condition_task`, etc.).
   */
  private parseTask(t: YamlNode): Task {
    let type: TaskType = 'unknown';
    let notebook_path: string | undefined;
    let condition: Task['condition'] | undefined;
    let base_parameters: Record<string, string> = {};

    if (t.notebook_task) {
      type = 'notebook';
      const nt = t.notebook_task as YamlNode;
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
      const l = lib as YamlNode;
      if (l.jar) {
        return { type: 'jar', path: String(l.jar) };
      }
      if (l.whl) {
        return { type: 'whl', path: String(l.whl) };
      }
      if (l.requirements) {
        return { type: 'requirements', path: String(l.requirements) };
      }
      if (l.pypi) {
        const p = l.pypi as Record<string, string>;
        return { type: 'pypi', package: p.package, repo: p.repo };
      }
      if (l.maven) {
        /** Raw shape of a maven library entry as it appears in the YAML. */
        interface MavenLibRaw {
          coordinates: string;
          repo?: string;
          exclusions?: unknown[];
        }
        const m = l.maven as MavenLibRaw;
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

  /**
   * Parses the `trigger:` YAML key into a `TriggerInfo`, dispatching on the
   * trigger type sub-key (`table_update`, `periodic`, `file_arrival`).
   * Returns `undefined` if `trigger` is absent.
   */
  private parseTrigger(trigger: YamlNode | undefined): TriggerInfo | undefined {
    if (!trigger) {
      return undefined;
    }

    const pauseStatus = trigger.pause_status as string | undefined;

    if (trigger.table_update) {
      const tu = trigger.table_update as YamlNode;
      const tableNames = ((tu.table_names as string[]) || []).map((n) =>
        n.replace(/\$\{[^}]+\}\./g, ''),
      );
      return {
        type: 'table_update',
        pauseStatus,
        tableNames,
        tableCondition: tu.condition as string | undefined,
      };
    }

    if (trigger.periodic) {
      const p = trigger.periodic as YamlNode;
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
      const fa = trigger.file_arrival as YamlNode;
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
  private parseSchedule(schedule: YamlNode | undefined): TriggerInfo | undefined {
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

  /**
   * Parses the `job_clusters:` array into a `Record<key, JobCluster>` map,
   * extracting the `new_cluster` sub-object for each entry.
   */
  private parseJobClusters(raw: unknown[] | undefined): Record<string, JobCluster> {
    const result: Record<string, JobCluster> = {};
    if (!raw) {
      return result;
    }

    for (const c of raw) {
      const cluster = c as YamlNode;
      const key = cluster.job_cluster_key as string;
      if (!key) {
        continue;
      }

      const nc = (cluster.new_cluster as YamlNode) || {};
      const autoscale = nc.autoscale as Record<string, number> | undefined;

      const rawInits = (nc.init_scripts as unknown[]) || [];
      const initScripts: string[] = rawInits.map((is) => {
        const s = is as YamlNode;
        if (s.s3) {
          return ((s.s3 as Record<string, string>).destination || 's3://…').replace(
            /\$\{[^}]+\}\//g,
            '',
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

  /**
   * Parses the `environments:` array into a `Record<key, JobEnvironment>` map,
   * extracting the `spec` sub-object for each entry.
   */
  private parseEnvironments(raw: unknown[] | undefined): Record<string, JobEnvironment> {
    const result: Record<string, JobEnvironment> = {};
    if (!raw) {
      return result;
    }

    for (const e of raw) {
      const env = e as YamlNode;
      const key = env.environment_key as string;
      if (!key) {
        continue;
      }

      const spec = (env.spec as YamlNode) || {};
      result[key] = {
        key,
        version: spec.environment_version as string | undefined,
        dependencies: (spec.dependencies as string[]) || [],
      };
    }

    return result;
  }

  /** Parses the `parameters:` array into typed `JobParameter` objects. */
  private parseParameters(raw: unknown[] | undefined): JobParameter[] {
    if (!raw) {
      return [];
    }
    return raw.map((p) => {
      const param = p as Record<string, string>;
      return { name: param.name || '', default: param.default || '' };
    });
  }

  /** Parses the `health:` block into an array of `HealthRule` objects. */
  private parseHealth(health: YamlNode | undefined): HealthRule[] {
    const rules = (health?.rules as unknown[]) || [];
    return rules.map((r) => {
      const rule = r as YamlNode;
      return {
        metric: rule.metric as string,
        op: rule.op as string,
        value: rule.value as number,
      };
    });
  }

  /**
   * Normalises the `email_notifications:` value to a human-readable string,
   * handling both plain-string and structured-object forms.
   */
  private parseEmailNotifications(raw: unknown): string {
    if (!raw) {
      return '';
    }
    if (typeof raw === 'string') {
      return raw;
    }
    // Object with on_failure / on_start / on_success arrays
    const obj = raw as YamlNode;
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

  /** Parses the `permissions:` array into typed `JobPermission` objects. */
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

  /**
   * Derives `category` and `subCategory` from a file path by locating the
   * `resources/` path segment and reading the two directory levels that follow it.
   */
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

/**
 * Describes the set of changes to apply to a job definition.
 * Only provided fields are written back — omitted fields leave the YAML unchanged.
 * Prefixed keys (`_newTasks`, `_removedTaskKeys`, etc.) represent structural
 * operations that cannot be expressed as simple field edits.
 */
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
  jobClusters?: Record<
    string,
    {
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
    }
  >;
  _newClusters?: YamlNode[];
  _removedClusterKeys?: string[];
  environments?: Record<
    string,
    {
      version?: string;
      dependencies?: string[];
    }
  >;
  _newEnvironments?: YamlNode[];
  _removedEnvironmentKeys?: string[];
  _newParams?: Array<{ name: string; default: string }>;
  _removedParamNames?: string[];
  _newTasks?: YamlNode[];
  _removedTaskKeys?: string[];
  _renamedTasks?: Record<string, string>;
  tasks?: Record<
    string,
    {
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
    }
  >;
}

/**
 * Reads `filePath`, applies `updates` to the named job node in-place using
 * `applyUpdatesToJobNode`, then writes the modified YAML back to disk.
 * Throws if the job name is not found in the file.
 */
export function saveJobUpdates(filePath: string, jobName: string, updates: JobUpdates): void {
  const content = fs.readFileSync(filePath, 'utf8');
  const doc = yaml.load(content) as YamlNode;
  const resources = doc.resources as YamlNode | undefined;
  const jobs = resources?.jobs as YamlNode | undefined;
  const jobNode = jobs?.[jobName] as YamlNode | undefined;
  if (!jobNode) {
    throw new Error(`Job "${jobName}" not found in ${filePath}`);
  }
  applyUpdatesToJobNode(jobNode, updates);
  const newContent = yaml.dump(doc, { lineWidth: 120, noRefs: true, indent: 2 });
  fs.writeFileSync(filePath, newContent, 'utf8');
}

// ─── Apply helpers ────────────────────────────────────────────────────────────

/**
 * Applies top-level job metadata updates: name, description, timeout,
 * max concurrent runs, queue enabled flag, health rule values, and parameter defaults.
 */
function applyJobMetadata(jobNode: YamlNode, updates: JobUpdates): void {
  if (updates.name) {
    jobNode.name = updates.name;
  }
  if (updates.description !== undefined) {
    jobNode.description = updates.description || undefined;
  }
  if (updates.timeoutSeconds !== undefined) {
    jobNode.timeout_seconds = updates.timeoutSeconds || undefined;
  }
  if (updates.maxConcurrentRuns !== undefined) {
    jobNode.max_concurrent_runs = updates.maxConcurrentRuns || undefined;
  }
  if (updates.queueEnabled !== undefined) {
    if (!jobNode.queue) {
      jobNode.queue = {};
    }
    (jobNode.queue as YamlNode).enabled = updates.queueEnabled;
  }
  if (updates.health && Array.isArray((jobNode.health as YamlNode | undefined)?.rules)) {
    const rules = (jobNode.health as YamlNode).rules as YamlNode[];
    updates.health.forEach((ru, i) => {
      if (rules[i] && ru.value !== undefined) {
        rules[i].value = ru.value ?? undefined;
      }
    });
  }
  if (updates.parameters && Array.isArray(jobNode.parameters)) {
    const params = jobNode.parameters as YamlNode[];
    updates.parameters.forEach((pu, i) => {
      if (params[i] && pu.default !== undefined) {
        params[i].default = pu.default;
      }
    });
  }
  if (updates._removedParamNames && updates._removedParamNames.length > 0) {
    if (Array.isArray(jobNode.parameters)) {
      const removed = new Set(updates._removedParamNames);
      jobNode.parameters = (jobNode.parameters as YamlNode[]).filter(
        (p) => !removed.has(p.name as string),
      );
    }
  }
  if (updates._newParams && updates._newParams.length > 0) {
    if (!Array.isArray(jobNode.parameters)) {
      jobNode.parameters = [];
    }
    for (const np of updates._newParams) {
      (jobNode.parameters as YamlNode[]).push({ name: np.name, default: np.default });
    }
  }
}

/**
 * Applies trigger or schedule updates. When `updates.trigger.type` changes, the
 * existing `trigger` / `schedule` block is wiped and replaced with a new skeleton
 * of the target type before field-level edits are applied.
 */
function applyTriggerUpdates(jobNode: YamlNode, updates: JobUpdates): void {
  if (!updates.trigger) {
    return;
  }
  const t = updates.trigger;

  // Handle trigger type change first — wipe and rebuild
  if (t.type !== undefined) {
    const savedPauseStatus =
      ((jobNode.trigger as YamlNode) || (jobNode.schedule as YamlNode) || {}).pause_status;
    delete jobNode.trigger;
    delete jobNode.schedule;
    if (t.type !== 'manual') {
      jobNode.trigger = {};
      const trigger = jobNode.trigger as YamlNode;
      if (savedPauseStatus) {
        trigger.pause_status = savedPauseStatus;
      }
      if (t.type === 'table_update') {
        trigger.table_update = {};
      } else if (t.type === 'periodic') {
        trigger.periodic = { cron: { quartz_cron_expression: '0 0 * * * ?', timezone_id: 'UTC' } };
      } else if (t.type === 'file_arrival') {
        trigger.file_arrival = { url: '' };
      }
    }
  }

  // Apply field edits to existing schedule / trigger
  const schedule = jobNode.schedule as YamlNode | undefined;
  if (schedule) {
    if (t.cronExpression !== undefined) {
      schedule.quartz_cron_expression = t.cronExpression || undefined;
    }
    if (t.timezone !== undefined) {
      schedule.timezone_id = t.timezone || undefined;
    }
    if (t.pauseStatus !== undefined) {
      schedule.pause_status = t.pauseStatus || undefined;
    }
  }

  const trigger = jobNode.trigger as YamlNode | undefined;
  if (trigger) {
    if (t.pauseStatus !== undefined) {
      trigger.pause_status = t.pauseStatus || undefined;
    }
    const tableUpdate = trigger.table_update as YamlNode | undefined;
    if (tableUpdate) {
      if (t.tableCondition !== undefined) {
        tableUpdate.condition = t.tableCondition || undefined;
      }
      if (t.tableNames !== undefined) {
        tableUpdate.table_names = t.tableNames.length > 0 ? t.tableNames : undefined;
      }
    }
    const fileArrival = trigger.file_arrival as YamlNode | undefined;
    if (fileArrival && t.fileArrivalUrl !== undefined) {
      fileArrival.url = t.fileArrivalUrl || undefined;
    }
    const periodic = trigger.periodic as YamlNode | undefined;
    if (periodic) {
      if (t.interval !== undefined) {
        periodic.interval = t.interval;
      }
      if (t.intervalUnit !== undefined) {
        periodic.unit = t.intervalUnit || undefined;
      }
    }
  }
}

/**
 * Removes clusters listed in `_removedClusterKeys`, appends entries from
 * `_newClusters`, then applies field-level edits from `jobClusters`.
 */
function applyClusterUpdates(jobNode: YamlNode, updates: JobUpdates): void {
  if (updates._removedClusterKeys && updates._removedClusterKeys.length > 0) {
    if (Array.isArray(jobNode.job_clusters)) {
      const removed = new Set(updates._removedClusterKeys);
      jobNode.job_clusters = (jobNode.job_clusters as YamlNode[]).filter(
        (c) => !removed.has(c.job_cluster_key as string),
      );
    }
  }

  if (updates._newClusters && updates._newClusters.length > 0) {
    if (!jobNode.job_clusters) {
      jobNode.job_clusters = [];
    }
    for (const nc of updates._newClusters) {
      (jobNode.job_clusters as YamlNode[]).push(nc);
    }
  }

  if (updates.jobClusters) {
    const rawClusters: YamlNode[] = Array.isArray(jobNode.job_clusters)
      ? (jobNode.job_clusters as YamlNode[])
      : [];
    for (const [key, cu] of Object.entries(updates.jobClusters)) {
      const rc = rawClusters.find((c) => c.job_cluster_key === key);
      if (!rc) {
        continue;
      }
      if (!rc.new_cluster) {
        rc.new_cluster = {};
      }
      const nc = rc.new_cluster as YamlNode;
      if (cu.nodeTypeId !== undefined) {
        nc.node_type_id = cu.nodeTypeId || undefined;
      }
      if (cu.driverNodeTypeId !== undefined) {
        nc.driver_node_type_id = cu.driverNodeTypeId || undefined;
      }
      if (cu.sparkVersion !== undefined) {
        nc.spark_version = cu.sparkVersion || undefined;
      }
      if (cu.policyId !== undefined) {
        nc.policy_id = cu.policyId || undefined;
      }
      if (cu.dataSecurityMode !== undefined) {
        nc.data_security_mode = cu.dataSecurityMode || undefined;
      }
      if (cu.runtimeEngine !== undefined) {
        nc.runtime_engine = cu.runtimeEngine || undefined;
      }
      if (cu.singleUserName !== undefined) {
        nc.single_user_name = cu.singleUserName || undefined;
      }
      if (cu.instancePoolId !== undefined) {
        nc.instance_pool_id = cu.instancePoolId || undefined;
      }
      if (cu.autoterminationMinutes !== undefined) {
        nc.autotermination_minutes = cu.autoterminationMinutes ?? undefined;
      }
      if (cu.enableElasticDisk !== undefined) {
        nc.enable_elastic_disk = cu.enableElasticDisk;
      }
      if (cu.numWorkers !== undefined) {
        nc.num_workers = cu.numWorkers ?? undefined;
        nc.autoscale = undefined;
      }
      if (cu.minWorkers !== undefined || cu.maxWorkers !== undefined) {
        if (!nc.autoscale) {
          nc.autoscale = {};
        }
        const autoscale = nc.autoscale as YamlNode;
        if (cu.minWorkers !== undefined) {
          autoscale.min_workers = cu.minWorkers ?? undefined;
        }
        if (cu.maxWorkers !== undefined) {
          autoscale.max_workers = cu.maxWorkers ?? undefined;
        }
        nc.num_workers = undefined;
      }
      if (cu.sparkConf !== undefined) {
        nc.spark_conf = Object.keys(cu.sparkConf).length > 0 ? cu.sparkConf : undefined;
      }
      if (cu.sparkEnvVars !== undefined) {
        nc.spark_env_vars = Object.keys(cu.sparkEnvVars).length > 0 ? cu.sparkEnvVars : undefined;
      }
    }
  }
}

/**
 * Removes environments listed in `_removedEnvironmentKeys`, appends entries from
 * `_newEnvironments`, then applies field-level edits from `environments`.
 */
function applyEnvironmentUpdates(jobNode: YamlNode, updates: JobUpdates): void {
  if (updates._removedEnvironmentKeys && updates._removedEnvironmentKeys.length > 0) {
    if (Array.isArray(jobNode.environments)) {
      const removed = new Set(updates._removedEnvironmentKeys);
      jobNode.environments = (jobNode.environments as YamlNode[]).filter(
        (e) => !removed.has(e.environment_key as string),
      );
    }
  }

  if (updates._newEnvironments && updates._newEnvironments.length > 0) {
    if (!jobNode.environments) {
      jobNode.environments = [];
    }
    for (const ne of updates._newEnvironments) {
      (jobNode.environments as YamlNode[]).push(ne);
    }
  }

  if (updates.environments) {
    const envArr: YamlNode[] = Array.isArray(jobNode.environments)
      ? (jobNode.environments as YamlNode[])
      : [];
    for (const [key, eu] of Object.entries(updates.environments)) {
      const re = envArr.find((e) => e.environment_key === key);
      if (!re) {
        continue;
      }
      if (!re.spec) {
        re.spec = {};
      }
      const spec = re.spec as YamlNode;
      if (eu.version !== undefined) {
        spec.environment_version = eu.version || undefined;
      }
      if (eu.dependencies !== undefined) {
        spec.dependencies = eu.dependencies.length > 0 ? eu.dependencies : undefined;
      }
    }
  }
}

/**
 * Applies in-place edits to existing tasks: dependency list, notebook path,
 * run-if condition, retry config, cluster/environment keys, base parameters,
 * libraries, and task key rename (propagating the rename to all dependents).
 */
function applyTaskEdits(jobNode: YamlNode, updates: JobUpdates): void {
  if (!updates.tasks) {
    return;
  }
  const rawTasks: YamlNode[] = Array.isArray(jobNode.tasks)
    ? (jobNode.tasks as YamlNode[])
    : [];

  for (const [taskKey, tu] of Object.entries(updates.tasks)) {
    const rt = rawTasks.find((t) => t.task_key === taskKey);
    if (!rt) {
      continue;
    }

    if (tu.depends_on !== undefined) {
      rt.depends_on = tu.depends_on.map((d) =>
        d.outcome ? { task_key: d.task_key, outcome: d.outcome } : { task_key: d.task_key },
      );
    }
    if (tu.notebook_path !== undefined && rt.notebook_task) {
      (rt.notebook_task as YamlNode).notebook_path = tu.notebook_path || undefined;
    }
    if (tu.run_if !== undefined) {
      rt.run_if = tu.run_if || undefined;
    }
    if (tu.max_retries !== undefined) {
      rt.max_retries = tu.max_retries ?? undefined;
    }
    if (tu.min_retry_interval_millis !== undefined) {
      rt.min_retry_interval_millis = tu.min_retry_interval_millis ?? undefined;
    }
    if (tu.job_cluster_key !== undefined) {
      rt.job_cluster_key = tu.job_cluster_key || undefined;
    }
    if (tu.environment_key !== undefined) {
      rt.environment_key = tu.environment_key || undefined;
    }
    if (tu.base_parameters && rt.notebook_task) {
      const nt = rt.notebook_task as YamlNode;
      if (!nt.base_parameters) {
        nt.base_parameters = {};
      }
      Object.assign(nt.base_parameters as object, tu.base_parameters);
    }

    // Rename task key and update all references
    if (tu._newKey && tu._newKey !== taskKey) {
      rt.task_key = tu._newKey;
      for (const t2 of rawTasks) {
        if (Array.isArray(t2.depends_on)) {
          for (const dep of t2.depends_on as YamlNode[]) {
            if (dep.task_key === taskKey) {
              dep.task_key = tu._newKey;
            }
          }
        }
      }
    }

    if (tu.libraries !== undefined) {
      rt.libraries = tu.libraries
        .map((lib): YamlNode => {
          if (lib.type === 'jar') {
            return { jar: lib.path };
          }
          if (lib.type === 'whl') {
            return { whl: lib.path };
          }
          if (lib.type === 'requirements') {
            return { requirements: lib.path };
          }
          if (lib.type === 'pypi') {
            const p: Record<string, string> = { package: lib.package! };
            if (lib.repo) {
              p.repo = lib.repo;
            }
            return { pypi: p };
          }
          if (lib.type === 'maven') {
            const m: YamlNode = { coordinates: lib.coordinates! };
            if (lib.repo) {
              m.repo = lib.repo;
            }
            if (lib.exclusions?.length) {
              m.exclusions = lib.exclusions;
            }
            return { maven: m };
          }
          return {};
        })
        .filter((l) => Object.keys(l).length > 0);
    }
  }
}

/**
 * Handles structural task changes: removes tasks listed in `_removedTaskKeys`
 * (cleaning up dangling `depends_on` references), appends entries from `_newTasks`,
 * and renames tasks listed in `_renamedTasks` (propagating to all dependents).
 */
function applyTaskStructureChanges(jobNode: YamlNode, updates: JobUpdates): void {
  if (updates._removedTaskKeys && updates._removedTaskKeys.length > 0) {
    const removed = new Set(updates._removedTaskKeys);
    if (Array.isArray(jobNode.tasks)) {
      jobNode.tasks = (jobNode.tasks as YamlNode[]).filter(
        (t) => !removed.has(t.task_key as string),
      );
      for (const t of jobNode.tasks as YamlNode[]) {
        if (Array.isArray(t.depends_on)) {
          t.depends_on = (t.depends_on as YamlNode[]).filter(
            (d) => !removed.has(d.task_key as string),
          );
          if ((t.depends_on as YamlNode[]).length === 0) {
            delete t.depends_on;
          }
        }
      }
    }
  }

  if (updates._newTasks && updates._newTasks.length > 0) {
    if (!jobNode.tasks) {
      jobNode.tasks = [];
    }
    for (const nt of updates._newTasks) {
      (jobNode.tasks as YamlNode[]).push(nt);
    }
  }

  if (updates._renamedTasks) {
    const rawTasks: YamlNode[] = Array.isArray(jobNode.tasks)
      ? (jobNode.tasks as YamlNode[])
      : [];
    for (const [oldKey, newKey] of Object.entries(updates._renamedTasks)) {
      if (!newKey || oldKey === newKey) {
        continue;
      }
      const rt = rawTasks.find((t) => t.task_key === oldKey);
      if (rt) {
        rt.task_key = newKey;
      }
      for (const t2 of rawTasks) {
        if (Array.isArray(t2.depends_on)) {
          for (const dep of t2.depends_on as YamlNode[]) {
            if (dep.task_key === oldKey) {
              dep.task_key = newKey;
            }
          }
        }
      }
    }
  }
}

/**
 * Orchestrator that applies all update categories to `jobNode` in the correct order:
 * metadata → trigger → clusters → environments → task edits → task structure changes.
 * Call `saveJobUpdates` instead of this function directly to also handle file I/O.
 */
function applyUpdatesToJobNode(jobNode: YamlNode, updates: JobUpdates): void {
  applyJobMetadata(jobNode, updates);
  applyTriggerUpdates(jobNode, updates);
  applyClusterUpdates(jobNode, updates);
  applyEnvironmentUpdates(jobNode, updates);
  applyTaskEdits(jobNode, updates);
  applyTaskStructureChanges(jobNode, updates);
}

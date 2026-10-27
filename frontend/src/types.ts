export type Finding = {
  asset_key: string; limitation: string; rule: string; severity: string;
  evidence: string; location: string; recommendation: string;
};
export type Asset = {
  asset_key: string; asset_type: string; resource_id: string; name: string;
  compute_kind: string; readiness_score: number | null; eligible: boolean;
  assessment_coverage: string; migration_status: string; is_fixture: boolean;
  path?: string | null; owner?: string | null; language?: string;
  source_fingerprint?: string; task_count?: number; job_id?: string;
  findings?: Finding[]; configuration?: Record<string, unknown>;
  planning_supported?: boolean; planning_reason?: string; is_demo?: boolean;
};
export type Evidence = {
  run_key: string; status: string; checked_at: string;
  classic_rows?: number | null; serverless_rows?: number | null;
  schema_match?: boolean; left_only?: number | null; right_only?: number | null;
  classic_sales?: number | null; serverless_sales?: number | null;
  classic_table?: string; serverless_table?: string; comparison?: string;
};
export type Report = {
  scan_id: string; scan_time: string; scope: string; scope_root?: string;
  rule_version?: string; actor?: string; coverage: string; truncated?: boolean;
  assets: Asset[]; findings: Finding[];
  relationships: { from_key: string; to_key: string; relation: string; task_key?: string }[];
  errors: { operation: string; resource: string; error_type: string; message: string }[];
  validation_evidence?: Evidence[];
  cost?: { status: string; reason?: string }; limitations?: string;
  project_schema?: string;
};
export type Page = 'overview' | 'inventory' | 'compatibility' | 'plans' | 'validation' | 'activity';
export type Preferences = { theme: 'light' | 'dark' | 'system'; fixtures: boolean; compact: boolean; autoRefresh: boolean };

export type PlanningSession = {
  actor: string | null; role: string; can_generate: boolean; can_decide: boolean;
  execution_enabled: boolean; source_job_id: string; approval_policy: string;
  workspace_planning?: boolean;
  demo_execution_available?: boolean;
};
export type MigrationPlan = {
  execution_profile?: string;
  plan_id: string; plan_hash: string; planner_version: string; created_at: string; created_by: string;
  scan_id: string; asset_key: string; source_job_id: string; source_name: string; source_fingerprint: string;
  status: 'DRAFT' | 'BLOCKED' | 'APPROVED' | 'REJECTED' | 'STALE'; initial_status: string;
  freshness: 'CURRENT' | 'STALE' | 'NOT_CHECKED'; checked_at?: string | null;
  changes: { task_key: string; field: string; before: string; after: string; reason: string }[];
  tasks: { task_key: string; source_compute: string; target_compute: string; convert_compute: boolean; depends_on: string[]; source_notebook: string; target_notebook: string; language: string; run_if: string }[];
  target: { name: string; compute: string; environment_key: string; environment_version: string; candidate_root: string; automatic_triggers: boolean; max_concurrent_runs: number; timeout_seconds: number; notifications: string };
  findings: (Finding & { task_key: string })[];
  required_acknowledgements: { id: string; label: string }[];
  validation_checks: { id: string; title: string; status: string }[];
  decision: { decision: string; actor: string; role: string; decided_at: string; reason: string; acknowledgements: string[]; execution_enabled: boolean } | null;
  recovery: string; execution_enabled: boolean; approval_role: string;
  source_checks: { job_configuration: string; notebook_source: string; classic_configuration: string; cluster_evidence: { cluster_id: string; captured_at: string; configuration_fingerprint: string }[] };
};

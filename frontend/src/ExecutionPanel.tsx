import { useEffect, useRef, useState } from 'react';
import { ArrowUpRight, Check, CheckCircle2, Clock3, Loader2, Play, RefreshCw, ShieldCheck } from 'lucide-react';
import type { MigrationPlan, PlanningSession } from './types';
import { Badge, Dialog, SectionHead } from './components';
import './execution.css';

type Result = { classic_duration_seconds?: number; candidate_duration_seconds?: number; input_match?: boolean; status: string; failed_stage?: string; message?: string; code?: string; classic_rows?: number; candidate_rows?: number; schema_match?: boolean; left_only?: number; right_only?: number; classic_sales?: number; candidate_sales?: number; classic_table?: string; candidate_table?: string };
type ProgressTask = { task_key: string; state: string; result?: string | null; setup_seconds: number; execution_seconds: number };
type Execution = { execution_id: string; state: string; stages?: { preflight?: { status: string }; baseline?: unknown; candidate?: unknown; baseline_done?: unknown; candidate_done?: unknown; results?: Result; progress?: { phase: string; cluster_state?: string; elapsed_seconds?: number; updated_at?: string; tasks?: ProgressTask[] }; diagnostics?: { message?: string; tasks?: ProgressTask[] } }; promotion_enabled: boolean; links?: { candidate_job?: string; candidate_run?: string; baseline_run?: string } };
type Response = { execution: (Execution & { baseline_mode?: string }) | null; baseline_mode?: string };
type SourceRun = { run_id: string; start_time?: number; run_url?: string };
const terminal = ['PASS', 'FAILED', 'VALIDATION_FAILED', 'NEEDS_REVIEW'];
const acknowledgements = [
  { id: 'compute_cost', label: 'I authorize serverless runner/candidate costs and a fresh run on the dedicated classic demo cluster. It may start that cluster.' },
  { id: 'demo_outputs', label: 'I reviewed the synthetic demo scope. Copies and run-specific input/output tables stay in the project folder and schema.' },
  { id: 'no_cutover', label: 'This is a validation trial. It does not replace, schedule, or promote the original job; one execution is allowed per approved plan.' },
];
async function api<T = Response>(path: string, body?: unknown): Promise<T> {
  const response = await fetch(path, { cache: 'no-store', ...(body ? { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Migration-Request': '1' }, body: JSON.stringify(body) } : {}) });
  const value = await response.json();
  if (!response.ok) throw new Error(value.error?.message || 'Execution could not be confirmed. Refresh its status before retrying.');
  return value;
}

export default function ExecutionPanel({ plan, session }: { plan: MigrationPlan; session: PlanningSession | null }) {
  const experiment = plan.execution_profile === 'isolated-experiment-orders-v1';
  const ordersProof = plan.execution_profile === 'isolated-orders-batch-v1';
  const [execution, setExecution] = useState<Execution | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [confirm, setConfirm] = useState(false);
  const [starting, setStarting] = useState(false);
  const [checked, setChecked] = useState<Set<string>>(new Set());
  const [pollTick, setPollTick] = useState(0);
  const [baselineMode, setBaselineMode] = useState('legacy_copy');
  const [sourceRuns, setSourceRuns] = useState<SourceRun[]>([]);
  const [sourceRunId, setSourceRunId] = useState('');
  const reuseSource = baselineMode === 'existing_source_run';
  const runAcknowledgements = reuseSource ? [
    { id: 'compute_cost', label: 'I authorize serverless controller and candidate costs. The agent will not run or start the original classic job.' },
    { id: 'demo_outputs', label: 'I selected the original successful run. Its retained input and output will be checked and pinned to Delta versions; the candidate uses isolated project outputs.' },
    acknowledgements[2],
  ] : acknowledgements;
  const pending = useRef<string | null>(null);
  const alive = useRef(true);
  const sequence = useRef(0);
  async function refresh() {
    const seq = ++sequence.current;
    setError('');
    try {
      const response = await api(`/api/plans/${plan.plan_id}/execution`);
      const mode = response.execution?.baseline_mode || response.baseline_mode || 'legacy_copy';
      if (alive.current && seq === sequence.current) { setExecution(response.execution); setBaselineMode(mode); }
      if (mode === 'existing_source_run' && !response.execution) {
        const choices = await api<{ runs: SourceRun[] }>(`/api/plans/${plan.plan_id}/baseline-runs`);
        if (alive.current && seq === sequence.current) setSourceRuns(choices.runs);
      }
    } catch (cause) { if (alive.current && seq === sequence.current) setError(cause instanceof Error ? cause.message : 'Unable to load execution.'); }
    finally { if (alive.current && seq === sequence.current) { setLoading(false); setPollTick(current => current + 1); } }
  }
  useEffect(() => { alive.current = true; setExecution(null); setLoading(true); setConfirm(false); setChecked(new Set()); setSourceRuns([]); setSourceRunId(''); pending.current = null; refresh(); return () => { alive.current = false; sequence.current++; }; }, [plan.plan_id]);
  useEffect(() => {
    if (!execution || terminal.includes(execution.state)) return;
    const timer = setTimeout(refresh, error ? 10000 : 5000);
    return () => clearTimeout(timer);
  }, [execution, pollTick, error]);
  async function start() {
    setStarting(true); setError(''); pending.current ||= crypto.randomUUID();
    try {
      const response = await api('/api/executions', { plan_id: plan.plan_id, plan_hash: plan.plan_hash, source_fingerprint: plan.source_fingerprint, request_id: pending.current, acknowledgements: [...checked], ...(reuseSource ? { baseline_run_id: sourceRunId } : {}) });
      if (!alive.current) return;
      setExecution(response.execution); setConfirm(false); setChecked(new Set());
    } catch (cause) { if (alive.current) { setError(cause instanceof Error ? cause.message : 'Execution could not be confirmed.'); setConfirm(false); } }
    finally { if (alive.current) setStarting(false); }
  }
  const canStart = Boolean(session?.can_decide && plan.status === 'APPROVED' && plan.freshness === 'CURRENT' && !execution && !loading && !error && (!reuseSource || sourceRuns.some(run => run.run_id === sourceRunId)));
  const result = execution?.stages?.results;
  const progress = execution?.stages?.progress;
  const failed = Boolean(execution && terminal.includes(execution.state) && execution.state !== 'PASS');
  const stages = [
    { name: 'Live preflight', done: Boolean(execution?.stages?.preflight), active: Boolean(execution) },
    { name: reuseSource ? 'Original classic run snapshot' : 'Fresh classic baseline', done: Boolean(execution?.stages?.baseline_done), active: Boolean(execution?.stages?.baseline) || progress?.phase === 'CLASSIC_STARTUP' },
    { name: 'Serverless candidate', done: Boolean(execution?.stages?.candidate_done), active: Boolean(execution?.stages?.candidate) },
    { name: 'Cross-run output comparison', done: result?.status === 'PASS', active: Boolean(execution?.stages?.candidate_done) },
  ];
  return <section className="panel execution-panel"><SectionHead title="Validate the migration" description={reuseSource ? 'Compare one serverless candidate with a selected successful original classic run.' : 'Run a fresh classic baseline and serverless candidate, then compare their outputs.'} action={<Badge tone="blue">{experiment ? 'Classic library experiment' : ordersProof ? 'All-classic orders proof' : 'Synthetic demo profile'}</Badge>} />
    {experiment && <div className="notice notice-information">All three tasks migrate to serverless with their mapped pinned PyPI dependencies. Validation compares the retained original input and regional summary with isolated candidate tables, and checks that the notebook successfully uses its required libraries.</div>}
    {ordersProof && <div className="notice notice-information">All three baseline tasks use classic compute. All three candidate tasks use serverless. The profile checks six regional summaries and 1,080 completed orders from 1,200 deterministic synthetic inputs; no business data is used.</div>}
    <p className="execution-intro">{reuseSource ? 'The runner verifies the selected original run and retained input, pins its Delta output version, and creates only one serverless candidate job. It does not create a classic copy, rerun the original job, or start its cluster.' : 'The runner rechecks live sources and pinned demo code, creates isolated baseline and candidate jobs, and compares the classic output with its migrated serverless counterpart. Existing workloads and schedules stay unchanged.'}</p>
    {reuseSource && !execution && !loading && <div className="notice notice-information"><label htmlFor="source-baseline-run">Original successful classic run</label><select id="source-baseline-run" value={sourceRunId} onChange={event => setSourceRunId(event.target.value)} disabled={starting}><option value="">Select an original run</option>{sourceRuns.map(run => <option key={run.run_id} value={run.run_id}>{run.start_time ? new Date(run.start_time).toLocaleString() : 'Completed run'} · {run.run_id}</option>)}</select>{!sourceRuns.length && <p>No successful original run was found. Run your original job manually first; the agent will not launch it for you.</p>}</div>}
    {error && <div className="notice notice-warning" role="alert">{error}<button className="text-button" onClick={refresh}>Refresh execution status</button></div>}
    {loading ? <p className="execution-loading" role="status"><Loader2 size={16} className="spin" />Loading execution status…</p> : execution ? <>
      <div className="execution-state"><Badge tone={execution.state === 'PASS' ? 'green' : terminal.includes(execution.state) ? 'red' : 'blue'}>{execution.state.replaceAll('_', ' ')}</Badge><button className="text-button" onClick={refresh}><RefreshCw size={13} />Refresh execution</button></div>
      <ol className="execution-stages">{stages.map(stage => <li key={stage.name} className={stage.done ? 'stage-done' : ''}>{stage.done ? <CheckCircle2 size={18} /> : stage.active && !terminal.includes(execution.state) ? <Loader2 size={18} className="spin" /> : <Clock3 size={18} />}<span>{stage.name}</span><small>{stage.done ? 'Passed' : stage.active ? failed ? 'Failed · inspect result' : stage.name === 'Fresh classic baseline' && progress?.phase === 'CLASSIC_STARTUP' ? 'Starting compute' : 'In progress' : failed ? 'Not run' : 'Pending'}</small></li>)}</ol>
      {progress && !terminal.includes(execution.state) && <div className="execution-progress" role="status"><strong>{progress.phase === 'CLASSIC_STARTUP' ? 'Waiting for classic compute to become ready' : progress.phase.replaceAll('_', ' ')}</strong>{progress.cluster_state && <p>Cluster: {progress.cluster_state.toLowerCase()} · {Math.floor((progress.elapsed_seconds || 0) / 60)}m {(progress.elapsed_seconds || 0) % 60}s elapsed · separate 15-minute startup limit</p>}{progress.phase === 'CLASSIC_STARTUP' && <p>The notebook has not started. Its five-minute task limit begins after the cluster is ready.</p>}{progress.tasks?.map(task => <p key={task.task_key}>{task.task_key}: {(task.result || task.state).replaceAll('_', ' ')} · setup {task.setup_seconds}s · execution {task.execution_seconds}s</p>)}</div>}
      {execution.stages?.diagnostics?.message && <p className="execution-diagnostic">{execution.stages.diagnostics.message}</p>}
      {result?.status === 'PASS' && experiment && <div className="notice notice-information"><a className="button" href={`/api/plans/${plan.plan_id}/candidate-bundle`}>Export candidate Asset Bundle</a><p>The bundle contains the validated configuration. Bind its candidate resource to the existing candidate job before deploying.</p></div>}
      {result && <div className="execution-result"><strong>{result.status === 'PASS' ? 'Classic and migrated serverless outputs match.' : 'Execution did not validate. Promotion is blocked.'}</strong>{result.message && <p>{result.message}</p>}{result.classic_rows !== undefined && <dl><dt>Classic / candidate rows</dt><dd>{result.classic_rows} / {result.candidate_rows}</dd><dt>Schema</dt><dd>{result.schema_match ? 'Matches' : 'Does not match'}</dd><dt>Classic-only / candidate-only rows</dt><dd>{result.left_only ?? 'Unknown'} / {result.right_only ?? 'Unknown'}</dd><dt>Input data</dt><dd>{result.input_match === undefined ? 'Profile-specific checks' : result.input_match ? 'Matches original input' : 'Mismatch'}</dd>{result.classic_duration_seconds !== undefined && <><dt>Classic / candidate duration</dt><dd>{result.classic_duration_seconds.toFixed(1)}s / {result.candidate_duration_seconds?.toFixed(1)}s</dd></>}<dt>Synthetic sales</dt><dd>{result.classic_sales} / {result.candidate_sales}</dd></dl>}{result.classic_table && <details><summary>Isolated output references</summary><code>{result.classic_table}</code><code>{result.candidate_table}</code></details>}</div>}
      {execution.links?.candidate_job && <div className="execution-evidence-links"><a className="button button-small" href={execution.links.candidate_job} target="_blank" rel="noreferrer">Open serverless candidate<ArrowUpRight size={14} /></a>{execution.links.candidate_run && <a className="text-button" href={execution.links.candidate_run} target="_blank" rel="noreferrer">Serverless run<ArrowUpRight size={13} /></a>}{execution.links.baseline_run && <a className="text-button" href={execution.links.baseline_run} target="_blank" rel="noreferrer">Classic baseline run<ArrowUpRight size={13} /></a>}</div>}
      {execution.state === 'NEEDS_REVIEW'  && <p>The runner ended without a final validation record. Inspect its existing runs before creating a new plan; no automatic retry or promotion occurs.</p>}
    </> : <div className="execution-start"><span><ShieldCheck size={16} />{plan.status === 'APPROVED' ? 'Approval recorded. Separate run confirmation required.' : 'Approve a current demo plan to enable this trial.'}</span><button className="button button-primary" disabled={!canStart || starting} onClick={() => setConfirm(true)}><Play size={15} />Run migration validation</button></div>}
    <p className="execution-boundary">Scheduled cutover is unavailable. Other workspace workloads require their own output-isolation and validation profiles before execution is enabled.</p>
    {confirm && <Dialog title="Start migration validation?" onClose={() => { if (!starting) setConfirm(false); }}><p>{reuseSource ? 'This creates only one serverless candidate job. The selected original run is reused with its retained Delta input/output versions. The original job and classic cluster are not started. Serverless controller and candidate compute charges apply.' : 'This creates two isolated jobs and copied demo notebooks, runs a fresh classic baseline and a serverless candidate, and records their comparison. Compute charges apply. The classic demo cluster may start; its existing auto-termination policy remains in effect.'}</p><div className="execution-acknowledgements">{runAcknowledgements.map(item => <label key={item.id}><input type="checkbox" checked={checked.has(item.id)} disabled={starting} onChange={event => setChecked(current => { const next = new Set(current); event.target.checked ? next.add(item.id) : next.delete(item.id); return next; })} /><span>{item.label}</span></label>)}</div><div className="dialog-actions"><button className="button" disabled={starting} onClick={() => setConfirm(false)}>Cancel</button><button className="button button-primary" disabled={starting || checked.size !== runAcknowledgements.length || !canStart} onClick={start}>{starting ? <Loader2 size={15} className="spin" /> : <Check size={15} />}{starting ? 'Submitting execution…' : 'Confirm and run validation'}</button></div></Dialog>}
  </section>;
}

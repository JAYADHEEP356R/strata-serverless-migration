import { useCallback, useEffect, useRef, useState } from 'react';
import { ArrowUpRight, ClipboardCheck, Loader2, RefreshCw } from 'lucide-react';
import { Badge } from './components';
import { dateTime } from './lib';
import './fleet.css';

export const fleetLabels: Record<string, string> = { MIGRATED_UNVALIDATED: 'Migrated · not validated', NOT_PLANNED: 'Not planned', AWAITING_APPROVAL: 'Awaiting approval', APPROVED: 'Approved · not run', VALIDATING: 'Validating', VALIDATED: 'Validated on serverless', VALIDATION_FAILED: 'Validation failed', STALE: 'Source changed', BLOCKED: 'Blocked', NEEDS_REVIEW: 'Needs review', REJECTED: 'Rejected', NOT_TARGETED: 'Not targeted' };
const tones: Record<string, string> = { MIGRATED_UNVALIDATED: 'amber', VALIDATED: 'green', VALIDATING: 'blue', APPROVED: 'blue', AWAITING_APPROVAL: 'blue', BLOCKED: 'red', VALIDATION_FAILED: 'red', STALE: 'amber', NEEDS_REVIEW: 'amber' };
export type FleetTrial = { metrics?: Record<string, string | number | boolean>; execution_id: string; plan_id: string; state: string; validation_status: string; phase?: string; checked_at?: string; requested_at: string; live_status?: string; is_latest_plan?: boolean; matches_scan?: boolean; links: Record<string, string> };
export type FleetRow = { direct_candidate_created?: boolean; direct_candidate_job_id?: string | null; migration_completed?: boolean; name?: string; asset_key: string; job_id: string; status: string; readiness_score: number | null; blocker_count: number; source_freshness: string; plan_count: number; evidence_issues: string[]; execution_supported: boolean; latest_plan: { plan_id: string; status: string; created_at: string; decision: { decision: string; actor: string; role: string; decided_at: string } | null } | null; execution: FleetTrial | null; latest_validation: FleetTrial | null };
type Fleet = { scan_id: string; updated_at: string; workloads: FleetRow[]; summary: { planned_jobs: number; awaiting_approval: number; validating: number; validated_candidates: number; attention_jobs: number }; coverage: { inventory: string; records: string; plan_limit: number; plans_read: number; limit_reached: boolean; issues: { code: string }[]; plans_outside_snapshot: number }; cost: { status: string; reason?: string } };

export function useFleetProgress(scanId: string) {
  const [value, setValue] = useState<Fleet | null>(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(true);
  const [tick, setTick] = useState(0);
  const controller = useRef<AbortController | null>(null);
  const sequence = useRef(0);
  const refresh = useCallback(async (force = true) => {
    controller.current?.abort();
    const active = new AbortController(); controller.current = active;
    const seq = ++sequence.current;
    const timer = setTimeout(() => active.abort('timeout'), 45000);
    setLoading(true);
    try {
      const response = await fetch('/api/fleet' + (force ? '?refresh=true' : ''), { cache: 'no-store', signal: active.signal });
      const next = await response.json();
      if (!response.ok) throw new Error(next.error?.message || 'Migration progress is unavailable.');
      if (next.scan_id !== scanId) throw new Error('Progress refers to a different inventory snapshot. Use Refresh data to align the views.');
      if (!Array.isArray(next.workloads) || !next.summary || !next.coverage) throw new Error('Saved migration progress has an unsupported format.');
      if (seq === sequence.current) { setValue(next); setError(''); }
    } catch (cause) {
      if (seq === sequence.current && (!active.signal.aborted || active.signal.reason === 'timeout')) setError(active.signal.reason === 'timeout' ? 'Reading migration progress took too long. Retry to refresh the saved records.' : cause instanceof Error ? cause.message : 'Migration progress is unavailable.');
    } finally {
      clearTimeout(timer);
      if (seq === sequence.current) { setLoading(false); setTick(t => t + 1); }
    }
  }, [scanId]);
  useEffect(() => { setValue(null); setError(''); refresh(true); return () => { sequence.current++; controller.current?.abort(); }; }, [refresh]);
  useEffect(() => { const timer = setTimeout(() => { if (!document.hidden) refresh(false); else setTick(t => t + 1); }, value?.summary.validating ? 15000 : 60000); return () => clearTimeout(timer); }, [value, tick, refresh]);
  return { value, error, loading, refresh };
}

export function FleetSummary({ fleet }: { fleet: ReturnType<typeof useFleetProgress> }) {
  const { value, error, loading, refresh } = fleet;
  return <section className="panel fleet-summary" aria-label="Fleet migration progress"><div className="section-head"><div><h2>Migration progress</h2><p>Saved plans, approvals and validation evidence for the discovered jobs.</p></div><button className="text-button" disabled={loading} onClick={() => refresh()}><RefreshCw size={14} className={loading ? 'spin' : ''} />Refresh progress</button></div>
    {error && <div className="notice notice-warning" role="alert">{error}{value && ' Showing the last confirmed progress.'}</div>}
    {!value ? <p className="fleet-loading" role="status">{loading && <Loader2 size={15} className="spin" />}{loading ? 'Reading migration records…' : 'Progress unavailable. Inventory and selection remain available.'}</p> : <>
      <div className="fleet-metrics">{[{ label: 'Workloads with plans', value: value.summary.planned_jobs }, { label: 'Awaiting approval', value: value.summary.awaiting_approval }, { label: 'Validating', value: value.summary.validating }, { label: 'Latest plans validated', value: value.summary.validated_candidates }, { label: 'Need attention', value: value.summary.attention_jobs }].map(item => <div key={item.label}><span>{item.label}</span><strong>{item.value}</strong></div>)}</div>
      {(value.coverage.records === 'PARTIAL' || value.coverage.inventory === 'PARTIAL') && <div className="notice notice-warning"><strong>Partial coverage.</strong><span>{value.coverage.limit_reached ? `At most ${value.coverage.plan_limit} saved plans were read. ` : ''}{value.coverage.issues.length ? `${value.coverage.issues.length} record(s) could not be verified. ` : ''}Check inventory access and saved evidence before treating these counts as complete.</span></div>}
      <div className="fleet-caption"><span>Updated {dateTime(value.updated_at)} · inventory snapshot and project records</span><Badge tone="neutral">Cost: {value.cost.status === 'AVAILABLE' ? 'See billing evidence' : 'Unavailable'}</Badge></div>
      <p className="fleet-boundary">Counts follow each workload’s latest plan; older validation stays in its history. A validated candidate has passed its recorded A/B comparison. Production cutover is not performed. Source changes are checked against the published scan; live preflight still runs before execution.</p>
    </>}
  </section>;
}

export function WorkloadProgress({ row, loading, onOpenPlan }: { row?: FleetRow; loading: boolean; onOpenPlan: (id: string) => void }) {
  if (!row) return <div className="workload-progress"><span className="subtle">{loading ? 'Loading migration progress…' : 'Migration progress unavailable'}</span></div>;
  const plan = row.latest_plan;
  const validation = row.latest_validation;
  return <div className="workload-progress"><div className="fleet-row-header"><Badge tone={tones[row.status] || 'neutral'}>{fleetLabels[row.status] || 'Needs review'}</Badge><span>Readiness {row.readiness_score == null ? 'unknown' : `${row.readiness_score}/100`} · {row.blocker_count} blocking finding{row.blocker_count === 1 ? '' : 's'}</span>{plan && <button className="text-button" onClick={() => onOpenPlan(plan.plan_id)}><ClipboardCheck size={13} />Open saved plan</button>}</div>
    {plan ? <div className="fleet-row-details"><span>Plan <code>{plan.plan_id.slice(0, 8)}</code> · {plan.status.toLowerCase()} · {row.plan_count} saved version(s)</span><span>{plan.decision ? `${plan.decision.decision === 'APPROVED' ? 'Approved' : 'Rejected'} by ${plan.decision.actor} · ${dateTime(plan.decision.decided_at)}` : 'No approval decision recorded'}</span>{row.source_freshness === 'STALE' ? <strong className="fleet-stale">The latest scan differs from this plan. Review a new plan.</strong> : <span>{row.source_freshness === 'CURRENT' ? 'Plan matches published scan evidence' : 'Source freshness not checked against this snapshot'}</span>}</div> : <p className="fleet-row-note">No saved plan found within the loaded record coverage.</p>}
    {row.evidence_issues.length > 0 && <p className="fleet-row-note fleet-stale">Saved evidence needs review: {row.evidence_issues.join(', ')}.</p>}
    {row.execution && <p className="fleet-row-note">Execution: {row.execution.state.replaceAll('_', ' ').toLowerCase()}{row.execution.phase ? ` · ${row.execution.phase.replaceAll('_', ' ').toLowerCase()}` : ''}{row.execution.live_status === 'UNAVAILABLE' ? ' · live runner status unavailable' : ''}</p>}
    {validation && validation.validation_status !== 'NOT_RUN' && <div className="fleet-validation"><span>{validation.is_latest_plan ? 'Latest plan validation' : 'Historical validation · older plan'}: <strong>{validation.validation_status}</strong>{validation.checked_at ? ` · ${dateTime(validation.checked_at)}` : ''}</span>{validation.links.candidate_job && <a className="text-button" href={validation.links.candidate_job} target="_blank" rel="noreferrer">Serverless candidate<ArrowUpRight size={13} /></a>}{validation.links.candidate_run && <a className="text-button" href={validation.links.candidate_run} target="_blank" rel="noreferrer">Validation run<ArrowUpRight size={13} /></a>}</div>}
    {!row.execution_supported && row.status !== 'NOT_TARGETED' && <small className="fleet-execution-boundary">Planning and approval available where supported. Execution needs this workload’s own isolation and validation profile.</small>}
  </div>;
}

import { useCallback, useEffect, useRef, useState } from 'react';
import { ArrowRight, Layers3, Loader2, RefreshCw, Save } from 'lucide-react';
import type { Asset, PlanningSession, Report } from './types';
import { Badge, Dialog, Empty, SectionHead } from './components';
import { dateTime, shortName } from './lib';
import { WorkloadProgress, type FleetRow } from './FleetProgress';
import './waves.css';

type SavedWave = { wave_id: string; name: string; description: string; created_at: string; created_by: string; scan_id: string; member_count: number; wave_hash: string };
type Listing = { waves: SavedWave[]; coverage: string; limit: number; issues: { code: string }[] };
type Detail = { wave: SavedWave; status: string; summary: { total: number; with_plans: number; awaiting_approval: number; validating: number; validated: number; needs_attention: number; missing_from_snapshot: number }; members: { saved: { asset_key: string; name: string; job_id: string }; progress: FleetRow | null; status: string; source_changed_since_saved: boolean | null }[]; selection_scan_changed: boolean; updated_at: string; coverage: { records: string } };
type DirectMember = { name: string; asset_key: string; plan_id?: string; plan_hash?: string; source_fingerprint?: string; ready: boolean; reason: string; findings?: { severity: string; evidence: string; recommendation: string }[]; changes?: { task_key: string; field: string; before: string; after: string }[] };
type DirectPreview = { wave_id: string; wave_hash: string; wave_name: string; members: DirectMember[]; ready: boolean; scope: string };
type DirectStatus = { state: string; members: { name: string; state: string; message?: string; candidate_job_id?: string }[] };
const labels: Record<string, string> = { DRAFT: 'Planning group', IN_REVIEW: 'Plans in review', VALIDATING: 'Validation in progress', VALIDATED: 'Latest plans validated', NEEDS_REVIEW: 'Needs review' };
async function api<T>(path: string, body?: unknown): Promise<T> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 45000);
  try {
    const response = await fetch(path, { cache: 'no-store', signal: controller.signal, ...(body ? { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Migration-Request': '1' }, body: JSON.stringify(body) } : {}) });
    const value = await response.json();
    if (!response.ok) throw Object.assign(new Error(value.error?.message || 'The wave could not be confirmed. Refresh waves before retrying.'), { code: value.error?.code });
    return value;
  } catch (cause) { if (controller.signal.aborted) throw new Error('The wave request took too long. Refresh waves before retrying the same request.'); throw cause; }
  finally { clearTimeout(timer); }
}
type Props = { reloadInventory: () => Promise<void>; report: Report; selected: Asset[]; session: PlanningSession | null; createOpen: boolean; closeCreate: () => void; preparing: boolean; onPrepare: (keys: string[]) => Promise<void>; onOpenPlan: (id: string) => void; toast: (message: string) => void };

export default function WavesPanel({ reloadInventory, report, selected, session, createOpen, closeCreate, preparing, onPrepare, onOpenPlan, toast }: Props) {
  const [list, setList] = useState<Listing | null>(null);
  const [detail, setDetail] = useState<Detail | null>(null);
  const [error, setError] = useState('');
  const [directPreview, setDirectPreview] = useState<DirectPreview | null>(null);
  const [directStatus, setDirectStatus] = useState<DirectStatus | null>(null);
  const [directBusy, setDirectBusy] = useState(false);
  const [directAccept, setDirectAccept] = useState(false);
  const directRequest = useRef<string | null>(null);
  const [saveError, setSaveError] = useState('');
  const [staleInventory, setStaleInventory] = useState(false);
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [loading, setLoading] = useState(true);
  const [reading, setReading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [pollTick, setPollTick] = useState(0);
  const alive = useRef(true);
  const sequence = useRef(0);
  const pending = useRef<{ key: string; id: string } | null>(null);
  const load = useCallback(async () => {
    setLoading(true);
    try { const value = await api<Listing>('/api/waves'); if (alive.current) { setList(value); setError(''); } }
    catch (cause) { if (alive.current) setError(cause instanceof Error ? cause.message : 'Unable to read waves.'); }
    finally { if (alive.current) setLoading(false); }
  }, []);
  useEffect(() => { alive.current = true; load(); return () => { alive.current = false; sequence.current++; }; }, [load]);
  useEffect(() => { if (createOpen) { setName(''); setDescription(''); setSaveError(''); setStaleInventory(false); } }, [createOpen]);
  async function readDirect(waveId: string) {
    try { const value = await api<DirectStatus>(`/api/waves/${waveId}/direct-status`); if (alive.current) setDirectStatus(value); }
    catch (cause) { if (alive.current) setError(cause instanceof Error ? cause.message : 'Unable to read migration progress.'); }
  }
  async function open(waveId: string, refresh = true) {
    const seq = ++sequence.current;
    setReading(true); setError('');
    try { const value = await api<Detail>(`/api/waves/${waveId}${refresh ? '?refresh=true' : ''}`); if (alive.current && seq === sequence.current) { setDetail(value); await readDirect(waveId); } }
    catch (cause) { if (alive.current && seq === sequence.current) setError(cause instanceof Error ? cause.message : 'Unable to read this wave.'); }
    finally { if (alive.current && seq === sequence.current) setReading(false); setPollTick(t => t + 1); }
  }
  useEffect(() => { if (detail) open(detail.wave.wave_id); }, [report.scan_id]);
  useEffect(() => { if (!detail) return; const timer = setTimeout(() => { if (!document.hidden) open(detail.wave.wave_id, false); else setPollTick(t => t + 1); }, detail.summary.validating ? 15000 : 60000); return () => clearTimeout(timer); }, [detail, pollTick]);
  useEffect(() => { if (!detail || !directStatus || !['REQUESTED', 'RUNNING'].includes(directStatus.state)) return; const timer = setTimeout(() => readDirect(detail.wave.wave_id), 5000); return () => clearTimeout(timer); }, [detail, directStatus]);
  async function previewDirect() {
    if (!detail) return;
    setDirectBusy(true); setError(''); setDirectAccept(false);
    try { setDirectPreview(await api<DirectPreview>(`/api/waves/${detail.wave.wave_id}/direct-preview`)); }
    catch (cause) { setError(cause instanceof Error ? cause.message : 'Unable to prepare wave review.'); }
    finally { setDirectBusy(false); }
  }
  async function startDirect() {
    if (!directPreview || !directPreview.ready || !directAccept) return;
    directRequest.current ||= crypto.randomUUID(); setDirectBusy(true); setError('');
    const plans = directPreview.members.map(m => ({ plan_id: m.plan_id, plan_hash: m.plan_hash, source_fingerprint: m.source_fingerprint }));
    try { const value = await api<DirectStatus>(`/api/waves/${directPreview.wave_id}/direct-start`, { wave_hash: directPreview.wave_hash, plans, request_id: directRequest.current, acknowledged: true }); setDirectStatus(value); setDirectPreview(null); toast('Wave migration started. Candidates will not be run or validated.'); }
    catch (cause) { setError(cause instanceof Error ? cause.message : 'Unable to start wave migration.'); }
    finally { setDirectBusy(false); }
  }
  async function save() {
    const body = { name: name.trim(), description: description.trim(), asset_keys: selected.map(a => a.asset_key).sort(), scan_id: report.scan_id };
    const key = JSON.stringify(body);
    if (pending.current?.key !== key) pending.current = { key, id: crypto.randomUUID() };
    setSaving(true); setSaveError(''); setStaleInventory(false);
    try {
      const value = await api<SavedWave>('/api/waves', { ...body, request_id: pending.current.id });
      if (!alive.current) return;
      pending.current = null; closeCreate(); toast('Wave saved. Plan approvals and execution remain separate.');
      await load(); await open(value.wave_id, false);
    } catch (cause) { if (alive.current) { setSaveError(cause instanceof Error ? cause.message : 'The wave could not be confirmed.'); setStaleInventory((cause as { code?: string }).code === 'STALE_INVENTORY'); } }
    finally { if (alive.current) setSaving(false); }
  }
  const eligible = detail?.members.filter(m => m.status !== 'VALIDATED' && !m.progress?.migration_completed && report.assets.some(a => a.asset_key === m.saved.asset_key && a.planning_supported && ['CLASSIC', 'MIXED'].includes(a.compute_kind))).map(m => m.saved.asset_key) || [];
  async function prepare() {
    if (!detail) return;
    await onPrepare(eligible);
    if (alive.current) await open(detail.wave.wave_id, false);
  }
  return <section className="panel waves-panel" aria-label="Migration waves">
    <SectionHead title="Migration waves" description="Review a wave once, then create serverless candidates for its jobs." action={<button className="text-button" disabled={loading || saving} onClick={async () => { await load(); if (detail) await open(detail.wave.wave_id); }}><RefreshCw size={14} className={loading ? 'spin' : ''} />Refresh waves</button>} />
    {error && <div className="notice notice-warning" role="alert">{error}{list && ' Showing the last confirmed wave list.'}</div>}
    {list?.coverage === 'PARTIAL' && <div className="notice notice-warning">Wave listing is partial. Up to {list.limit} records are shown; unreadable records require review.</div>}
    {loading && !list ? <p className="fleet-loading"><Loader2 size={15} className="spin" />Loading saved waves…</p> : list && !list.waves.length ? <Empty title="Plan your first migration wave" description="Select classic workloads below, including blocked jobs you want to review, then choose Save as wave." icon={Layers3} /> : <div className="wave-list">{list?.waves.map(wave => <button className={`wave-card ${detail?.wave.wave_id === wave.wave_id ? 'selected' : ''}`} key={wave.wave_id} disabled={saving || preparing} onClick={() => open(wave.wave_id)}><span><Layers3 size={17} /><strong>{wave.name}</strong><ArrowRight size={14} /></span><small>{wave.member_count} workload{wave.member_count === 1 ? '' : 's'} · {dateTime(wave.created_at)}</small></button>)}</div>}
    {reading && <p className="fleet-loading" role="status"><Loader2 size={15} className="spin" />Updating wave evidence…</p>}
    {detail && <div className="wave-detail"><div className="wave-detail-title"><div><span className="eyebrow">SAVED REVIEW GROUP</span><h3>{detail.wave.name}</h3></div><Badge tone={detail.status === 'VALIDATED' ? 'green' : detail.status === 'NEEDS_REVIEW' ? 'amber' : 'blue'}>{labels[detail.status] || 'Needs review'}</Badge></div>{detail.wave.description && <p className="wave-description">{detail.wave.description}</p>}<p className="wave-description">Saved by {detail.wave.created_by} · {dateTime(detail.wave.created_at)}. Progress follows each member’s latest saved plan; older passing trials remain historical evidence.</p>
      <div className="wave-metrics">{[{ label: 'Members', value: detail.summary.total }, { label: 'With plans', value: detail.summary.with_plans }, { label: 'Awaiting approval', value: detail.summary.awaiting_approval }, { label: 'Validated', value: detail.summary.validated }, { label: 'Need attention', value: detail.summary.needs_attention }].map(item => <div key={item.label}><span>{item.label}</span><strong>{item.value}</strong></div>)}</div>
      {(detail.selection_scan_changed || detail.coverage.records === 'PARTIAL' || detail.summary.missing_from_snapshot > 0) && <div className="notice notice-warning">{detail.selection_scan_changed && 'Inventory has been refreshed since this wave was saved. '}{detail.coverage.records === 'PARTIAL' && 'Plan/evidence coverage is partial. '}{detail.summary.missing_from_snapshot > 0 && 'Some members are absent from the current snapshot; this does not mean they were deleted.'}</div>}
      <div className="wave-actions"><button className="button button-primary" disabled={!session?.can_generate || !eligible.length || preparing || reading || saving} onClick={prepare}>{preparing ? <Loader2 size={14} className="spin" /> : <Layers3 size={14} />}{preparing ? 'Preparing member plans…' : 'Prepare eligible wave plans'}</button><span>{eligible.length} currently supported member(s). Current matching plans are reused; new plans still need individual review.</span></div>
      <div className="wave-actions"><button className="button button-primary" disabled={!session?.can_generate || directBusy || !detail || (directStatus !== null && directStatus.state !== 'NOT_STARTED')} onClick={previewDirect}>Review and migrate wave</button><span>One approval; candidates are created without baseline or output validation.</span></div>
      {directStatus && directStatus.state !== 'NOT_STARTED' && <div className="notice notice-information" role="status"><strong>Wave migration: {directStatus.state.replaceAll('_', ' ')}</strong><div>{directStatus.members.map(member => <p key={member.name}>{member.name}: {member.state.replaceAll('_', ' ')}{member.message ? ` · ${member.message}` : ''}{member.state === 'CREATED' ? ' · Candidate created, not validated' : ''}</p>)}</div></div>}
      <div className="wave-members">{detail.members.map(member => <article className="wave-member" key={member.saved.asset_key}><h4>{member.saved.name}</h4><span className="wave-member-id">Job {member.saved.job_id}</span>{member.source_changed_since_saved && <p className="fleet-stale">Source fingerprint changed since this wave was saved. Review the current plan.</p>}{member.progress ? <WorkloadProgress row={member.progress} loading={false} onOpenPlan={onOpenPlan} /> : <p className="wave-description">Not in the current snapshot. Rescan or check visibility before planning this member.</p>}</article>)}</div>
      <p className="wave-boundary">Wave migration creates unscheduled candidates only. It does not run or validate the candidates or replace originals. Updated {dateTime(detail.updated_at)}.</p>
    </div>}
    {directPreview && <Dialog title={`Review migration · ${directPreview.wave_name}`} onClose={() => { if (!directBusy) setDirectPreview(null); }}><p>{directPreview.scope}</p>{directPreview.members.map(member => <div key={member.asset_key}><h3>{member.name} <Badge tone={member.ready ? 'green' : 'amber'}>{member.ready ? 'Ready' : 'Blocked'}</Badge></h3>{member.reason && <p>{member.reason}</p>}{member.findings?.map((finding, index) => <p key={index}><strong>{finding.severity}</strong> · {finding.evidence} — {finding.recommendation}</p>)}<details><summary>Proposed changes</summary>{member.changes?.map((change, index) => <p key={index}>{change.task_key}: {change.field} — {change.before} → {change.after}</p>)}</details></div>)}<label><input type="checkbox" checked={directAccept} onChange={event => setDirectAccept(event.target.checked)} /> I reviewed the changes and understand these candidates will not be validated or activated.</label><div className="dialog-actions"><button className="button" disabled={directBusy} onClick={() => setDirectPreview(null)}>Cancel</button><button className="button button-primary" disabled={!directPreview.ready || !directAccept || directBusy} onClick={startDirect}>{directBusy ? 'Starting…' : 'Approve and migrate wave'}</button></div></Dialog>}
    {createOpen && <Dialog title="Save a migration wave" onClose={() => { if (!saving) closeCreate(); }}><p>Save this selection as a named planning group. This creates a project record only. Blocked members stay visible for review.</p><label className="wave-field">Wave name<input maxLength={80} value={name} disabled={saving} onChange={event => setName(event.target.value)} placeholder="For example: Batch workloads — wave 1" /></label><label className="wave-field">Description (optional)<textarea rows={2} maxLength={500} value={description} disabled={saving} onChange={event => setDescription(event.target.value)} /></label><div className="wave-selection"><strong>{selected.length} selected workload(s)</strong>{selected.map(asset => <div key={asset.asset_key}><span>{shortName(asset)}</span><Badge tone={asset.planning_supported ? 'blue' : 'amber'}>{asset.planning_supported ? 'Planning supported' : 'Review required'}</Badge></div>)}</div>{saveError && <div className="notice notice-warning" role="alert">{saveError}<button className="text-button" onClick={staleInventory ? async () => { await reloadInventory(); closeCreate(); } : load}>{staleInventory ? 'Refresh inventory and review' : 'Reload saved waves'}</button></div>}<div className="dialog-actions"><button className="button" disabled={saving} onClick={closeCreate}>Cancel</button><button className="button button-primary" disabled={!session?.can_generate || saving || !name.trim() || !selected.length || selected.length > 50} onClick={save}>{saving ? <Loader2 size={14} className="spin" /> : <Save size={14} />}{saving ? 'Saving wave…' : 'Save wave'}</button></div></Dialog>}
  </section>;
}

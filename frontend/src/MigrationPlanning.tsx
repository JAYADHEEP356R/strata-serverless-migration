import { useEffect, useRef, useState } from 'react';
import { ArrowRight, FileSearch, Loader2, RefreshCw, Search, ShieldCheck } from 'lucide-react';
import type { Asset, MigrationPlan, Page, PlanningSession, Report } from './types';
import { Badge, Dialog, Empty } from './components';
import { dateTime, shortName } from './lib';
import './workloads.css';
import WavesPanel from './WavesPanel';
import { WorkloadProgress, fleetLabels, useFleetProgress } from './FleetProgress';

async function api<T>(path: string, body?: unknown): Promise<T> {
  const response = await fetch(path, { cache: 'no-store', ...(body ? { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Migration-Request': '1' }, body: JSON.stringify(body) } : {}) });
  const value = await response.json();
  if (!response.ok) throw new Error(value.error?.message || 'The workspace request failed. Please retry.');
  return value;
}
type Scan = { run_id: string; state: string; result: string | null };
type Props = { session: PlanningSession | null; onPrepared: () => Promise<void>; report: Report; assets: Asset[]; navigate: (page: Page) => void; onAsset: (asset: Asset) => void; reload: () => Promise<void>; toast: (message: string) => void; onOpenPlan: (id: string) => void };

export default function MigrationPlanning({ session, onPrepared, report, assets, navigate, onAsset, reload, toast, onOpenPlan }: Props) {
  const [query, setQuery] = useState('');
  const [migrationStatus, setMigrationStatus] = useState('ALL');
  const fleet = useFleetProgress(report.scan_id);
  const progress = new Map(fleet.value?.workloads.map(row => [row.asset_key, row]) || []);
  const [classicOnly, setClassicOnly] = useState(true);
  const [selection, setSelection] = useState<Set<string>>(new Set());
  const [confirmScan, setConfirmScan] = useState(false);
  const [createWave, setCreateWave] = useState(false);
  const [scan, setScan] = useState<Scan | null>(null);
  const [scanning, setScanning] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [error, setError] = useState('');
  const [results, setResults] = useState<{ name: string; status: string; reused?: boolean }[]>([]);
  const scanRequest = useRef<string | null>(null);
  const planRequests = useRef<Record<string, string>>({});
  const alive = useRef(true);
  useEffect(() => { alive.current = true; return () => { alive.current = false; }; }, []);
  useEffect(() => { setSelection(new Set()); setResults([]); planRequests.current = {}; }, [report.scan_id]);
  useEffect(() => {
    if (!scan || scan.result || ['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR'].includes(scan.state)) return;
    const timeout = setTimeout(async () => {
      try {
        const value = await api<Scan>('/api/discovery/scans/' + scan.run_id);
        if (!alive.current) return;
        setScan(value);
        if (value.result === 'SUCCESS') { scanRequest.current = null; await reload(); toast('Workspace scan published. Review the discovered workloads.'); }
        else if (value.result || ['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR'].includes(value.state)) setError('Discovery did not publish a successful scan. The previous snapshot remains available; inspect the project discovery run.');
      } catch (cause) { if (alive.current) { setError(cause instanceof Error ? cause.message : 'Unable to check discovery.'); setScan(null); } }
    }, 5000);
    return () => clearTimeout(timeout);
  }, [scan, reload, toast]);
  const jobs = assets.filter(asset => asset.asset_type === 'JOB' && !asset.is_fixture);
  const classic = jobs.filter(asset => ['CLASSIC', 'MIXED'].includes(asset.compute_kind));
  const shown = jobs.filter(asset => (!classicOnly || classic.includes(asset)) && `${asset.name} ${asset.owner || ''}`.toLowerCase().includes(query.toLowerCase()) && (migrationStatus === 'ALL' || progress.get(asset.asset_key)?.status === migrationStatus));
  const isWorkspace = report.scope === 'workspace';
  const running = scanning || Boolean(scan && !scan.result && !['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR'].includes(scan.state));
  const completed = (asset: Asset) => Boolean(progress.get(asset.asset_key)?.migration_completed || progress.get(asset.asset_key)?.direct_candidate_created);
  const selectable = (asset: Asset) => isWorkspace && !completed(asset) && Boolean(asset.planning_supported) && Boolean(session?.can_generate);
  const waveSelectable = (asset: Asset) => isWorkspace && !completed(asset) && ['CLASSIC', 'MIXED'].includes(asset.compute_kind) && Boolean(session?.can_generate);
  const validatedCount = jobs.filter(completed).length;
  const validatedPercent = jobs.length ? Math.round(validatedCount * 100 / jobs.length) : 0;
  const selectedAssets = jobs.filter(asset => selection.has(asset.asset_key) && waveSelectable(asset));
  const selectedPlanning = selectedAssets.filter(selectable);
  const visibleSelectable = shown.filter(waveSelectable);
  const allChecked = visibleSelectable.length > 0 && visibleSelectable.every(asset => selection.has(asset.asset_key));

  async function startScan() {
    setScanning(true); setError(''); scanRequest.current ||= crypto.randomUUID();
    try {
      const value = await api<Scan>('/api/discovery/scans', { request_id: scanRequest.current });
      if (!alive.current) return;
      setScan(value); setConfirmScan(false);
      if (value.result === 'SUCCESS') { scanRequest.current = null; await reload(); }
    } catch (cause) { if (alive.current) { setError(cause instanceof Error ? cause.message : 'Discovery could not be started.'); setConfirmScan(false); } }
    finally { if (alive.current) setScanning(false); }
  }
  async function preparePlans(keys?: string[]) {
    const requested = new Set(keys || [...selection]);
    setGenerating(true); setError(''); setResults([]);
    for (const asset of jobs.filter(job => requested.has(job.asset_key) && selectable(job))) {
      planRequests.current[asset.asset_key] ||= crypto.randomUUID();
      try {
        const current = progress.get(asset.asset_key);
        if (current?.latest_plan && current.source_freshness === 'CURRENT' && current.latest_plan.status !== 'REJECTED') {
          const saved = await api<MigrationPlan>('/api/plans/' + current.latest_plan.plan_id);
          if (saved.freshness === 'CURRENT' && ['DRAFT', 'BLOCKED', 'APPROVED'].includes(saved.status)) {
            if (alive.current) setResults(values => [...values, { name: shortName(asset), status: saved.status, reused: true }]);
            delete planRequests.current[asset.asset_key];
            continue;
          }
        }
        const plan = await api<MigrationPlan>('/api/plans', { asset_key: asset.asset_key, scan_id: report.scan_id, request_id: planRequests.current[asset.asset_key] });
        if (!alive.current) break;
        delete planRequests.current[asset.asset_key];
        setResults(values => [...values, { name: shortName(asset), status: plan.status }]);
      } catch (cause) { if (alive.current) setResults(values => [...values, { name: shortName(asset), status: cause instanceof Error ? cause.message : 'Plan generation failed' }]); }
    }
    if (alive.current) { setGenerating(false); await onPrepared(); await fleet.refresh(); }
  }
  return <>
    <div className="section-head"><div><h2>Choose jobs and organize waves</h2><p>Select the jobs to migrate, group them into waves, and prepare plans for review.</p></div><button className="button" onClick={() => setConfirmScan(true)} disabled={!session?.can_generate || running || generating}><RefreshCw size={15} className={running ? 'spin' : ''}/>{running ? 'Refreshing inventory...' : 'Scan for jobs'}</button></div>
    {error && <div className="notice notice-warning" role="alert">{error}</div>}
    {scan && <div className="notice notice-information" role="status">Discovery: {scan.result || scan.state}</div>}
    <section className="panel migration-progress-card" aria-label="Migration progress overview">
      <div className="migration-progress-heading"><div><h2>Migration progress</h2><p>Serverless candidates across the discovered job inventory</p></div>{fleet.value && <span className="migration-progress-percent">{validatedPercent}%</span>}</div>
      {fleet.error && <div className="notice notice-warning" role="alert">{fleet.error}{fleet.value && ' Showing the last confirmed progress.'}</div>}
      {!fleet.value ? <p role="status">{fleet.loading ? 'Loading migration progress…' : 'Migration progress is unavailable.'}</p> : <>
        <div className="migration-progress-totals"><strong>{validatedCount} <span>of {jobs.length} jobs migrated</span></strong><span>{jobs.length - validatedCount} remaining</span></div>
        <div className="migration-progress-track" role="progressbar" aria-label="Jobs with a serverless candidate" aria-valuenow={validatedCount} aria-valuemin={0} aria-valuemax={jobs.length}><div className="migration-progress-fill" style={{ width: `${validatedPercent}%` }} /></div>
        <div className="migration-progress-legend"><span><i className="migration-progress-dot done" />Migrated candidate</span><span><i className="migration-progress-dot remaining" />Remaining</span></div>
        {(fleet.value.coverage.records === 'PARTIAL' || fleet.value.coverage.inventory === 'PARTIAL') && <p className="migration-progress-note">Coverage is partial; these counts may be incomplete.</p>}
        <p className="migration-progress-note">Migrated counts serverless candidates created for the current job version. Direct candidates are not validated. Originals are retained; production cutover is not confirmed.</p>
      </>}
    </section>
    <WavesPanel reloadInventory={reload} report={report} selected={selectedAssets} session={session} createOpen={createWave} closeCreate={() => setCreateWave(false)} preparing={generating} onPrepare={preparePlans} onOpenPlan={onOpenPlan} toast={toast} />
    <section className="panel workload-panel"><div className="workload-toolbar"><label className="workload-search"><Search size={16} /><input aria-label="Search migration jobs" value={query} onChange={event => setQuery(event.target.value)} placeholder="Search jobs to migrate…" /></label><label className="workload-filter"><input type="checkbox" checked={classicOnly} onChange={event => setClassicOnly(event.target.checked)} />Classic jobs only</label><label className="fleet-status-filter">Migration status<select aria-label="Filter migration status" value={migrationStatus} onChange={event => setMigrationStatus(event.target.value)}><option value="ALL">All stages</option>{Object.entries(fleetLabels).map(([key, label]) => <option key={key} value={key}>{label}</option>)}</select></label><span className="subtle">Scanned {dateTime(report.scan_time)}</span></div>
    <div className="workload-batch"><label><input type="checkbox" aria-label="Select all visible supported workloads" disabled={!visibleSelectable.length || generating} checked={allChecked} onChange={() => setSelection(current => { const next = new Set(current); visibleSelectable.forEach(asset => allChecked ? next.delete(asset.asset_key) : next.add(asset.asset_key)); return next; })} />{selection.size} selected · {selectedPlanning.length} plan-ready</label><button className="button" disabled={!selectedAssets.length || selectedAssets.length > 50 || generating || running} onClick={() => setCreateWave(true)}>Save as wave</button><button className="button button-primary" onClick={() => preparePlans()} disabled={!selectedPlanning.length || generating || running}>{generating ? <Loader2 size={15} className="spin" /> : <FileSearch size={15} />}{generating ? 'Preparing plans…' : 'Prepare selected plans'}</button></div>
    {!shown.length ? <Empty title="No matching workloads" description={migrationStatus !== 'ALL' ? 'No workloads match this migration stage. Choose All stages to see the rest of the inventory.' : classicOnly ? 'No classic jobs match this view. Turn off the classic filter to inspect the other discovered jobs, or run a new scan.' : 'Try another search or run a workspace scan.'} icon={FileSearch} /> : <div className="workload-list">{shown.map(asset => <article className="workload-row" key={asset.asset_key}><input type="checkbox" aria-label={'Select ' + shortName(asset)} disabled={!waveSelectable(asset) || generating} checked={selection.has(asset.asset_key)} onChange={event => setSelection(current => { const next = new Set(current); event.target.checked ? next.add(asset.asset_key) : next.delete(asset.asset_key); return next; })} /><div className="workload-details"><button className="text-button" onClick={() => onAsset(asset)}>{shortName(asset)}<ArrowRight size={13} /></button><p>{asset.owner || 'Owner not reported'} · {asset.task_count ?? 'Unknown'} tasks</p><small>{asset.planning_reason || 'Run a workspace scan to inspect planning support.'}</small></div><div className="workload-badges"><Badge tone={['CLASSIC', 'MIXED'].includes(asset.compute_kind) ? 'orange' : 'neutral'}>{asset.compute_kind.replaceAll('_', ' ')}</Badge>{completed(asset) && <><Badge tone={progress.get(asset.asset_key)?.migration_completed ? "green" : "orange"}>Migrated</Badge><small>{progress.get(asset.asset_key)?.migration_completed ? "Serverless candidate validated" : "Serverless candidate created · Not validated"} · Original job retained</small></>}{asset.is_demo && <Badge tone="blue">Demo workload</Badge>}<Badge tone={asset.planning_supported ? 'green' : 'neutral'}>{asset.planning_supported ? 'Plan available' : 'Review only'}</Badge></div><WorkloadProgress row={progress.get(asset.asset_key)} loading={fleet.loading} onOpenPlan={onOpenPlan} /></article>)}</div>}</section>
    {results.length > 0 && <section className="panel workload-results" aria-live="polite"><h2>Selected workload results</h2>{results.map(result => <p key={result.name}><strong>{result.name}</strong><span>{result.status}{result.reused && <small> · existing plan reused</small>}</span></p>)}<button className="button" onClick={() => navigate('plans')}>Review prepared plans<ArrowRight size={15} /></button></section>}
    <div className="scope-note"><ShieldCheck size={16} /><p>Selection prepares plans only. Unsupported task types remain visible for review. Published scan evidence requires a fresh live preflight before any future execution, and candidate output isolation must be defined per workload.</p></div>
    {confirmScan && <Dialog title="Scan accessible workspace workloads?" onClose={() => { if (!scanning) setConfirmScan(false); }}><p>This runs our dedicated discovery job on serverless compute and may incur scanner compute costs. It reads accessible job definitions, cluster configuration, libraries, and notebook source; it saves metadata, findings, and fingerprints only in project storage.</p><p>It does not run, modify, or grant permissions on discovered workloads. Visibility is limited to the scanner operator’s existing access.</p><div className="dialog-actions"><button className="button" disabled={scanning} onClick={() => setConfirmScan(false)}>Cancel</button><button className="button button-primary" disabled={scanning} onClick={startScan}>{scanning ? 'Requesting scan…' : 'Start read-only scan'}</button></div></Dialog>}
  </>;
}


import { ArrowDownToLine, ArrowUpRight } from 'lucide-react';
import { Badge, SectionHead } from './components';
import { useFleetProgress } from './FleetProgress';
import { download } from './lib';

export default function LiveValidationSummary({ scanId }: { scanId: string }) {
  const fleet = useFleetProgress(scanId);
  const rows = fleet.value?.workloads.filter(row => row.latest_validation) || [];
  return <section className="panel" style={{padding: 24, marginBottom: 24}}>
    <SectionHead title="Migration execution results" description="Recorded candidate runs and comparisons for discovered workloads." action={<button className="button" disabled={fleet.loading} onClick={() => fleet.refresh()}>Refresh results</button>} />
    {fleet.error && <div className="notice notice-warning">{fleet.error}</div>}
    {!rows.length && <p>{fleet.loading ? 'Reading execution evidence...' : 'No migration execution has been recorded for the current inventory.'}</p>}
    {rows.map(row => { const trial = row.latest_validation!; const m = trial.metrics; const current = trial.is_latest_plan && trial.matches_scan; return <article key={row.asset_key} style={{padding: '18px 0',borderTop:'1px solid var(--border-soft)'}}>
      <div style={{display:'flex',gap:12,alignItems:'center',flexWrap:'wrap'}}><strong>{row.name || row.asset_key}</strong><Badge tone={current && trial.validation_status === 'PASS' ? 'green' : trial.validation_status === 'FAILED' ? 'red' : 'amber'}>{current ? trial.state.replaceAll('_',' ') : 'Historical evidence'}</Badge></div>
      {m && <p>Rows: {String(m.classic_rows ?? 'Unknown')} classic / {String(m.candidate_rows ?? 'Unknown')} serverless · Differences: {String(m.left_only ?? 'Unknown')} / {String(m.right_only ?? 'Unknown')} · Schema: {m.schema_match ? 'matches' : 'not confirmed'}{m.input_match !== undefined && ` · Input: ${m.input_match ? 'matches' : 'differs'}`}</p>}
      {m?.classic_duration_seconds !== undefined && <p>Classic: {String(m.classic_duration_seconds)}s · Serverless candidate: {String(m.candidate_duration_seconds)}s</p>}
      <div style={{display:'flex',gap:12,flexWrap:'wrap'}}>{trial.links.candidate_run && <a className="button" href={trial.links.candidate_run} target="_blank" rel="noreferrer">Candidate run<ArrowUpRight size={14}/></a>}{trial.links.baseline_run && <a className="button" href={trial.links.baseline_run} target="_blank" rel="noreferrer">Classic baseline<ArrowUpRight size={14}/></a>}<button className="button" onClick={() => download(`validation-${trial.execution_id}.json`, JSON.stringify({workload:row.name,source_freshness:row.source_freshness,...trial},null,2))}>Export evidence<ArrowDownToLine size={14}/></button></div>
    </article>; })}
  </section>;
}

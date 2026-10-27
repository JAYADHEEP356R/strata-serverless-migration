import { useEffect, useRef, type ReactNode } from 'react';
import { AlertCircle, ArrowUpRight, Box, Braces, Check, CheckCircle2, ChevronRight, Code2, Copy, Database, FileCode2, GitBranch, Layers3, SearchX, Server, ShieldCheck, X, type LucideIcon } from 'lucide-react';
import type { Asset, Finding, Report } from './types';
import { assetFindings, computeLabel, humanize, readiness, ruleName, shortName } from './lib';

export function Brand({ small = false }: { small?: boolean }) {
  return <div className={`brand ${small ? 'small' : ''}`}><span className="brand-symbol"><Layers3 size={23} strokeWidth={1.7} /></span><span>strata<span className="brand-period">.</span></span></div>;
}
export function Badge({ children, tone = 'neutral', dot = false }: { children: ReactNode; tone?: string; dot?: boolean }) {
  return <span className={`badge badge-${tone}`}>{dot && <i />}{children}</span>;
}
export function IconTile({ icon: Icon, tone = 'neutral', small = false }: { icon: LucideIcon; tone?: string; small?: boolean }) {
  return <span className={`icon-tile tile-${tone} ${small ? 'tile-small' : ''}`}><Icon size={small ? 16 : 20} strokeWidth={1.65} /></span>;
}
export const typeIcon = (type: string): LucideIcon => ({ JOB: GitBranch, TASK: Box, NOTEBOOK: FileCode2, CLUSTER: Server }[type] || Database);
export function AssetIcon({ asset, small = false }: { asset: Asset; small?: boolean }) { return <IconTile icon={typeIcon(asset.asset_type)} tone={asset.asset_type === 'NOTEBOOK' ? 'violet' : asset.asset_type === 'CLUSTER' ? 'blue' : asset.asset_type === 'JOB' ? 'orange' : 'neutral'} small={small} />; }
export function Empty({ title, description, action, icon: Icon = SearchX }: { title: string; description: string; action?: ReactNode; icon?: LucideIcon }) {
  return <div className="empty-state"><span className="empty-icon"><Icon size={27} strokeWidth={1.5} /></span><h3>{title}</h3><p>{description}</p>{action}</div>;
}
export function SectionHead({ title, description, action }: { title: string; description?: string; action?: ReactNode }) {
  return <div className="section-head"><div><h2>{title}</h2>{description && <p>{description}</p>}</div>{action}</div>;
}
export function Dialog({ title, children, onClose, className = '', label }: { title: string; children: ReactNode; onClose: () => void; className?: string; label?: string }) {
  const ref = useRef<HTMLDialogElement>(null);
  useEffect(() => {
    const dialog = ref.current!;
    const previous = document.activeElement as HTMLElement | null;
    dialog.showModal();
    return () => { dialog.close(); previous?.focus(); };
  }, []);
  return <dialog ref={ref} aria-label={label || title} className={`dialog ${className}`} onCancel={e => { e.preventDefault(); onClose(); }} onClick={e => { if (e.target === e.currentTarget) onClose(); }}><div className="dialog-body"><div className="dialog-title"><h2>{title}</h2><button className="icon-button" aria-label="Close dialog" onClick={onClose}><X size={20} /></button></div>{children}</div></dialog>;
}
export function Score({ value }: { value: number | null }) {
  return <span className="score-cell"><span className="score-track"><span style={{ width: `${value ?? 0}%` }} className={value == null ? 'unknown' : value >= 80 ? 'good' : value >= 50 ? 'medium' : 'low'} /></span><span>{value ?? '—'}{value != null && <small>/100</small>}</span></span>;
}
export function MigrationGraphic() {
  return <div className="migration-graphic" aria-hidden="true"><div className="orbit orbit-one" /><div className="orbit orbit-two" /><div className="graphic-node classic-node"><Server size={24} /><span>CLASSIC</span><div className="node-slots"><i /><i /><i /></div></div><div className="graphic-flow"><i /><i /><i /></div><div className="graphic-node serverless-node"><Layers3 size={29} /><span>SERVERLESS</span><div className="node-spark"><i /> Ready to assess</div></div><span className="graphic-spark spark-one">+</span><span className="graphic-spark spark-two">+</span></div>;
}
export function FindingCard({ finding, asset, onInspect }: { finding: Finding; asset?: Asset; onInspect?: () => void }) {
  return <article className={`finding-card finding-${finding.severity.toLowerCase()}`}><div className="finding-top"><Badge tone={finding.severity === 'BLOCKER' ? 'red' : finding.severity === 'REVIEW' ? 'amber' : 'blue'}>{humanize(finding.severity)}</Badge><span className="mono subtle">{finding.limitation}</span></div><h3>{ruleName(finding.rule)}</h3><p>{finding.evidence}</p><div className="finding-location"><Code2 size={14} /><span>{finding.location || 'Configuration'}</span></div><div className="recommendation"><ShieldCheck size={17} /><div><span>Recommended action</span><p>{finding.recommendation}</p></div></div>{asset && onInspect && <button className="finding-asset" onClick={onInspect}><FileCode2 size={15} /><span>{shortName(asset)}</span><ArrowUpRight size={15} /></button>}</article>;
}
export function AssetDrawer({ asset, report, onClose, onSelect, onPlan, toast }: { asset: Asset; report: Report; onClose: () => void; onSelect: (asset: Asset) => void; onPlan?: (asset: Asset) => void; toast: (message: string) => void }) {
  const own = assetFindings(asset, report.findings);
  const status = readiness(asset, report.findings);
  const dependencies = report.relationships.filter(r => r.from_key === asset.asset_key || r.to_key === asset.asset_key);
  const copy = async () => { try { await navigator.clipboard.writeText(asset.path || asset.resource_id); toast('Asset reference copied'); } catch { toast('Clipboard unavailable. You can select and copy the reference below.'); } };
  return <Dialog title="Asset details" className="drawer" onClose={onClose}><div className="asset-drawer-hero"><AssetIcon asset={asset} /><Badge tone={status.tone} dot>{status.label}</Badge><h2>{shortName(asset)}</h2><p>{humanize(asset.asset_type)}{asset.is_fixture ? ' · Assessment fixture' : ''}</p>{asset.asset_type === 'JOB' && !asset.is_fixture && onPlan && <button className="button button-primary drawer-plan-action" onClick={() => onPlan(asset)}><GitBranch size={15} />Prepare migration plan</button>}</div><div className="drawer-stats"><div><span>Static readiness</span><strong>{asset.readiness_score ?? '—'}<small>{asset.readiness_score != null ? '/100' : ''}</small></strong></div><div><span>Findings</span><strong>{own.length}</strong></div><div><span>Coverage</span><strong className="coverage-word">{humanize(asset.assessment_coverage)}</strong></div></div><div className="drawer-section"><h3>Configuration</h3><dl className="detail-list"><dt>Compute</dt><dd>{computeLabel(asset)}</dd><dt>Language</dt><dd>{asset.language || 'Not recorded'}</dd><dt>Owner</dt><dd>{asset.owner || 'Inherited from job or workspace'}</dd><dt>Migration status</dt><dd>{humanize(asset.migration_status)}</dd><dt>Resource ID</dt><dd className="mono">{asset.resource_id}</dd></dl>{asset.path && <div className="path-box"><span className="eyebrow">Workspace path</span><code>{asset.path}</code></div>}<button className="button button-small" onClick={copy}><Copy size={14} />Copy reference</button>{asset.source_fingerprint && <details className="fingerprint"><summary>Source fingerprint</summary><code>{asset.source_fingerprint}</code></details>}{asset.configuration && <details className="fingerprint"><summary>Compute configuration</summary><pre>{JSON.stringify(asset.configuration, null, 2)}</pre></details>}</div><div className="drawer-section"><h3>Compatibility findings <span className="count-pill">{own.length}</span></h3>{own.length ? own.map((f, i) => <FindingCard key={`${f.rule}-${i}`} finding={f} />) : <div className="inline-success"><CheckCircle2 size={18} /><span>No static findings. Execution validation is still required.</span></div>}</div><div className="drawer-section"><h3>Dependencies <span className="count-pill">{dependencies.length}</span></h3>{dependencies.length ? dependencies.map((r, i) => {
    const related = report.assets.find(a => a.asset_key === (r.from_key === asset.asset_key ? r.to_key : r.from_key));
    return related ? <button className="dependency-row" key={i} onClick={() => onSelect(related)}><AssetIcon asset={related} small /><div><strong>{shortName(related)}</strong><span>{humanize(r.relation)}</span></div><ChevronRight size={16} /></button> : null;
  }) : <p className="subtle small-text">No relationships recorded for this asset.</p>}</div><div className="drawer-footer"><ShieldCheck size={16} />Assessment only · This view does not change the asset.</div></Dialog>;
}
export function Skeleton() {
  return <div className="loading-layout" aria-label="Loading assessment" role="status"><div className="skeleton skeleton-heading" /><div className="skeleton skeleton-hero" /><div className="metric-grid">{[1,2,3,4].map(x => <div key={x} className="skeleton skeleton-metric" />)}</div><div className="skeleton skeleton-content" /></div>;
}
export function ErrorPanel({ retry, busy, message }: { retry: () => void; busy: boolean; message: string }) {
  return <div className="panel connection-error"><IconTile icon={AlertCircle} tone="orange" /><span className="eyebrow">Connection needs attention</span><h1>Your workspace view is temporarily unavailable.</h1><p>{message}</p><button className="button button-primary" disabled={busy} onClick={retry}>{busy ? 'Reconnecting…' : 'Try again'}</button><div className="error-note"><ShieldCheck size={16} />Your workspace assets have not been changed.</div></div>;
}
export function CoverageBadge({ report }: { report: Report }) {
  const partial = report.coverage === 'PARTIAL' || report.truncated || report.errors.length > 0;
  return <Badge tone={partial ? 'amber' : 'green'} dot>{partial ? 'Partial coverage' : 'Scope inspected'}</Badge>;
}

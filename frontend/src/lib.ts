import type { Asset, Evidence, Finding, Report } from './types';

export const humanize = (value: string) => value.toLowerCase().replaceAll('_', ' ').replaceAll('-', ' ').replace(/^./, s => s.toUpperCase());
export const ruleName = (value: string) => ({
  'source-reconciliation': 'Reconcile source before promotion',
  'notebook-language': 'Unsupported notebook language',
  'checkpoint': 'Checkpoint API needs a replacement',
  'global-temp-view': 'Global temporary view is unsupported',
  'mount-management': 'Mount management needs a replacement',
  'streaming-trigger': 'Review the streaming trigger',
  'ansi-cast-review': 'Review ANSI cast behavior',
}[value] || humanize(value));
export const dateTime = (value?: string) => {
  if (!value || Number.isNaN(new Date(value).getTime())) return 'Unknown time';
  return new Intl.DateTimeFormat(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }).format(new Date(value));
};
export const timeAgo = (value?: string) => {
  if (!value || Number.isNaN(new Date(value).getTime())) return 'Unknown';
  const minutes = Math.max(0, Math.floor((Date.now() - new Date(value).getTime()) / 60000));
  if (minutes < 1) return 'Just now';
  if (minutes < 60) return `${minutes}m ago`;
  if (minutes < 1440) return `${Math.floor(minutes / 60)}h ago`;
  return `${Math.floor(minutes / 1440)}d ago`;
};
export const computeLabel = (asset: Asset) => ({ CLASSIC: 'Classic', MIXED: 'Mixed compute', SERVERLESS_OR_OTHER: 'Serverless / other', DEPENDENT: 'Task dependent' }[asset.compute_kind] || humanize(asset.compute_kind || 'unknown'));
export const shortName = (asset: Asset) => asset.name.replace(/^\[dev [^\]]+\]\s*/, '');
export const assetFindings = (asset: Asset, findings: Finding[]) => findings.filter(f => f.asset_key === asset.asset_key);
export const evidencePassed = (e: Evidence) => e.status === 'PASS' && e.schema_match === true && e.classic_rows === 5 && e.serverless_rows === 5 && e.left_only === 0 && e.right_only === 0 && e.classic_sales === 1000 && e.serverless_sales === 1000;
export function readiness(asset: Asset, findings: Finding[]) {
  if (asset.assessment_coverage !== 'COMPLETE' || asset.readiness_score == null) return { label: 'Unknown', tone: 'neutral', order: 0 };
  const own = assetFindings(asset, findings);
  if (!asset.eligible || own.some(f => f.severity === 'BLOCKER' || f.severity === 'REVIEW')) return { label: 'Needs attention', tone: 'red', order: 1 };
  if (own.length) return { label: 'Review advised', tone: 'amber', order: 2 };
  return { label: 'Clear', tone: 'green', order: 3 };
}
export function visibleReport(report: Report, fixtures: boolean) {
  const assets = report.assets.filter(a => fixtures || !a.is_fixture);
  const keys = new Set(assets.map(a => a.asset_key));
  return { assets, findings: report.findings.filter(f => keys.has(f.asset_key)), relationships: report.relationships.filter(r => keys.has(r.from_key) && keys.has(r.to_key)) };
}
export function download(name: string, value: string, type = 'application/json') {
  const url = URL.createObjectURL(new Blob([value], { type }));
  const a = document.createElement('a'); a.href = url; a.download = name; a.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
export function csv(rows: Record<string, unknown>[]) {
  if (!rows.length) return '';
  const keys = Object.keys(rows[0]);
  const escape = (value: unknown) => {
    // Prevent spreadsheet formula execution when exported names begin with =,+,-,@.
    let text = String(value ?? '');
    if (/^[=+\-@\t\r]/.test(text)) text = "'" + text;
    return '"' + text.replaceAll('"', '""') + '"';
  };
  return [keys, ...rows.map(row => keys.map(key => row[key]))].map(row => row.map(escape).join(',')).join('\r\n');
}

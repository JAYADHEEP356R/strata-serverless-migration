import { ArrowRight, FileCode2, GitBranch, Layers3 } from 'lucide-react';

import type { Asset } from './types';

import type { AssetLineage } from './lineage';



export function InventoryLineage({ asset, lineage, onAsset }: { asset: Asset; lineage?: AssetLineage; onAsset: (asset: Asset) => void }) {

  if (!lineage || !['NOTEBOOK', 'TASK'].includes(asset.asset_type)) return null;

  const paths = asset.asset_type === 'NOTEBOOK' ? [...new Map(lineage.paths.map(p => [p.jobKey, p])).values()] : lineage.paths;
  const jobs = new Set(lineage.paths.map(p => p.jobKey).filter(Boolean));

  if (!lineage.paths.length) return <div className={`inventory-lineage lineage-empty ${lineage.uncertain ? 'lineage-unknown' : ''}`}><strong><FileCode2 size={13} />{lineage.uncertain ? 'Association unknown' : 'Workspace-only'}</strong><small>{lineage.uncertain ? 'Scan coverage cannot establish job associations.' : 'No job association found in visible workspace jobs.'}</small></div>;

  return <div className="inventory-lineage"><div className="lineage-heading"><GitBranch size={13} /><strong>{asset.asset_type === 'TASK' ? 'Source job' : `${jobs.size} associated job${jobs.size === 1 ? '' : 's'}`}</strong>{jobs.size > 1 && <span className="lineage-shared">Shared notebook</span>}</div><div className="lineage-paths">{paths.map(p => <div className="lineage-path" key={`${p.jobKey}:${p.taskKey}:${p.notebook?.asset_key || ''}`}>

    {p.job ? <button onClick={() => onAsset(p.job!)} title={`Inspect job ${p.job.name} - ${p.job.resource_id}`}><Layers3 size={13} /><span>{p.job.name}<small>Job {p.job.resource_id}</small></span></button> : <span className="lineage-unresolved">{p.jobKey || 'Parent job unknown'}</span>}

    <ArrowRight size={12} aria-hidden="true" />

    {asset.asset_type === 'NOTEBOOK' && p.notebook ? <button onClick={() => onAsset(p.notebook!)} title={p.notebook.path || p.notebook.name}><FileCode2 size={13} /><span>{p.notebook.name}<small>Notebook</small></span></button> : p.task ? <button onClick={() => onAsset(p.task!)} title={`Inspect task ${p.task.name}`}><GitBranch size={12} /><span>{p.task.name}<small>Task</small></span></button> : <span className="lineage-unresolved">Task details unavailable</span>}

  </div>)}</div>{lineage.uncertain && <small className="lineage-coverage">Known associations shown; scan coverage may omit others.</small>}</div>;

}


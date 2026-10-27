import type { Asset, Report } from './types';

export type LineagePath = { job?: Asset; task?: Asset; notebook?: Asset; jobKey: string; taskKey: string };
export type AssetLineage = { paths: LineagePath[]; uncertain: boolean; search: string };

// Relationships refer to stable asset keys, never notebook basenames (which can collide).
export function inventoryLineage(report: Report): Map<string, AssetLineage> {
  const assets = new Map(report.assets.map(a => [a.asset_key, a]));
  const parents = new Map<string, Set<string>>();
  const notebooks = new Map<string, Set<string>>();
  const tasksByNotebook = new Map<string, Set<string>>();
  let dangling = false;
  for (const edge of report.relationships || []) {
    if (!['HAS_TASK', 'RUNS_NOTEBOOK'].includes(edge.relation)) continue;
    const source = assets.get(edge.from_key), target = assets.get(edge.to_key);
    if (!source || !target) dangling = true;
    const map = edge.relation === 'HAS_TASK' ? parents : tasksByNotebook;
    if (!map.has(edge.to_key)) map.set(edge.to_key, new Set());
    map.get(edge.to_key)!.add(edge.from_key);
    if (edge.relation === 'RUNS_NOTEBOOK') {
      if (!notebooks.has(edge.from_key)) notebooks.set(edge.from_key, new Set());
      notebooks.get(edge.from_key)!.add(edge.to_key);
    }
  }
  for (const asset of report.assets) {
    if (asset.asset_type === 'TASK' && asset.job_id) {
      if (!parents.has(asset.asset_key)) parents.set(asset.asset_key, new Set());
      parents.get(asset.asset_key)!.add(`JOB:${asset.job_id}`);
    }
  }
  const incomplete = report.scope !== 'workspace' || report.coverage !== 'COMPLETE_WITHIN_SELECTED_VISIBILITY' || !!report.truncated || !!report.errors?.length || dangling || report.assets.some(a => ['JOB', 'TASK'].includes(a.asset_type) && a.assessment_coverage !== 'COMPLETE');
  return new Map(report.assets.map(asset => {
    const paths: LineagePath[] = [];
    const taskKeys = asset.asset_type === 'TASK' ? [asset.asset_key] : [...(tasksByNotebook.get(asset.asset_key) || [])];
    for (const taskKey of taskKeys) {
      const task = assets.get(taskKey);
      const jobKeys = [...(parents.get(taskKey) || [])];
      const notebookKeys = asset.asset_type === 'NOTEBOOK' ? [asset.asset_key] : [...(notebooks.get(taskKey) || [])];
      for (const jobKey of jobKeys.length ? jobKeys : ['']) {
        for (const notebookKey of notebookKeys.length ? notebookKeys : ['']) {
          paths.push({ jobKey, taskKey, job: assets.get(jobKey), task, notebook: assets.get(notebookKey) });
        }
      }
    }
    paths.sort((a,b) => `${a.job?.name || a.jobKey}/${a.task?.name || a.taskKey}`.localeCompare(`${b.job?.name || b.jobKey}/${b.task?.name || b.taskKey}`));
    const uncertain = incomplete || paths.some(p => !p.job || !p.task);
    return [asset.asset_key, { paths, uncertain, search: paths.map(p => `${p.job?.name || ''} ${p.job?.resource_id || ''} ${p.task?.name || ''} ${p.notebook?.path || ''}`).join(' ') }];
  }));
}

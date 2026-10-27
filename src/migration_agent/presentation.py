"""App visibility for retired smoke demonstrations; persistent audit records remain intact."""
import copy
import re

def legacy_name(name):
    name=re.sub(r"^\[[^\]]+\]\s*", "", name or "")
    return name in {"migration-agent-smoke-test","migration-proof-orders-all-classic"} or bool(re.fullmatch(r"migration-baseline-[a-f0-9]{12}",name))

def visible_report(report):
    value=copy.deepcopy(report)
    assets={a['asset_key']:a for a in value['assets']};edges=value.get('relationships',[])
    hidden={k for k,a in assets.items() if a.get('asset_type')=='JOB' and legacy_name(a.get('name'))}
    # Retired candidate copies have the old smoke/orders task names, not experiment task names.
    for k,a in assets.items():
        if a.get('asset_type')=='JOB' and re.fullmatch(r"migration-candidate-[a-f0-9]{12}",a.get('name','')):
            tasks=[assets.get(e['to_key'],{}) for e in edges if e['from_key']==k and e['relation']=='HAS_TASK']
            if any(t.get('name') in {'prepare','classic','serverless','validate','ingest_orders','aggregate_orders','verify_orders'} for t in tasks):hidden.add(k)
    for relation in ('HAS_TASK','RUNS_NOTEBOOK'):
        hidden.update(e['to_key'] for e in edges if e['from_key'] in hidden and e['relation']==relation)
    root=report.get('scope_root','')
    old_notebooks={'prepare','workload','validate','orders_batch_prepare','orders_batch_aggregate','orders_batch_verify'}
    for k,a in assets.items():
        path=a.get('path') or ''
        if root and path.rsplit('/',1)[0]==root+'/notebooks' and path.rsplit('/',1)[-1].removesuffix('.py') in old_notebooks:hidden.add(k)
    visible_tasks={k for k,a in assets.items() if a.get('asset_type')=='TASK' and k not in hidden}
    shared={e['to_key'] for e in edges if e['from_key'] in visible_tasks and e['relation']=='RUNS_NOTEBOOK'}
    hidden.difference_update(shared)
    value['assets']=[a for a in value['assets'] if a['asset_key'] not in hidden]
    value['findings']=[f for f in value['findings'] if f['asset_key'] not in hidden]
    value['relationships']=[e for e in edges if e['from_key'] not in hidden and e['to_key'] not in hidden]
    hidden_ids={str(assets[k].get('resource_id','')) for k in hidden if k in assets}
    value['planning_sources']={k:v for k,v in value.get('planning_sources',{}).items() if str(k) not in hidden_ids}
    value['errors']=[e for e in value.get('errors',[]) if str(e.get('resource','')) not in hidden_ids]
    value['validation_evidence']=[]
    value['presentation_scope']='Retired smoke demonstrations excluded from the App'
    return value

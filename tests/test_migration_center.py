import base64
import hashlib
import threading
import uuid
from types import SimpleNamespace as NS

import pytest
from migration_agent.assessment import fingerprint
from migration_agent.migration_center import MigrationCenter, execute_saved_plan, inspect_member
from migration_agent.plan_store import LocalPlanStore
from migration_agent.workflow import WorkflowError

OWNER = 'operator@example.com'
ROOT = '/Workspace/Users/operator@example.com/.bundle/workspace-migration-agent-jr/agent/files'
CODE = '# Databricks notebook source\nprint(1)\n'


def report():
    sources = {}
    assets = []
    for number in ('11', '12'):
        path = '/Workspace/samples/job-' + number
        tasks = [{'task_key':'main','source_notebook':path,'source_hash':hashlib.sha256(CODE.strip().encode()).hexdigest(),'language':'PYTHON','depends_on':[],'run_if':'ALL_SUCCESS','source_compute':'CLASSIC','serverless_dependencies':['packaging==24.2']}]
        sources[number] = {'job_id':number,'name':'Job '+number,'source_fingerprint':number[0]*64,'tasks':tasks,'findings':[], 'source_checks':{'cluster_evidence':[]},'settings':{'tasks':[{'task_key':'main','existing_cluster_id':'classic','notebook_task':{'notebook_path':path,'base_parameters':{}},'libraries':[{'pypi':{'package':'packaging==24.2'}}]}]}}
        assets.append({'asset_key':'JOB:'+number,'asset_type':'JOB','resource_id':number,'name':'Job '+number,'compute_kind':'CLASSIC','readiness_score':97})
    assets.append({'asset_key':'NOTEBOOK:99','asset_type':'NOTEBOOK','resource_id':'99','name':'standalone','path':'/Workspace/standalone','compute_kind':'DEPENDENT','language':'PYTHON','assessment_coverage':'COMPLETE','source_fingerprint':fingerprint({},CODE),'findings':[]})
    return {'scan_id':'scan1','scope':'workspace','scope_root':ROOT,'actor':OWNER,'assets':assets,'planning_sources':sources}


class Jobs:
    def __init__(self): self.launches=[];self.created=[];self.lock=threading.Lock()
    def run_now(self, **kwargs): self.launches.append(kwargs);return NS(response=NS(run_id=88))
    def get_run(self, run_id): return NS(job_id=7,state=NS(result_state=None))
    def get(self, job_id):
        settings = None
        if int(job_id) >= 101 and int(job_id)-101 < len(self.created):
            created=self.created[int(job_id)-101]
            settings=NS(tasks=created['tasks'],environments=created['environments'])
        return NS(job_id=job_id,creator_user_name=OWNER,settings=settings)
    def list(self, **kwargs): return []
    def create(self, **kwargs):
        with self.lock:
            self.created.append(kwargs)
            return NS(job_id=100+len(self.created))


def setup(tmp_path):
    data=report(); jobs=Jobs(); imports=[]
    workspace=NS(jobs=jobs,current_user=NS(me=lambda:NS(user_name=OWNER)),workspace=NS(export=lambda *a,**k:NS(content=base64.b64encode(CODE.encode()).decode()),get_status=lambda p:NS(language=NS(value='PYTHON')),mkdirs=lambda p:None,import_=lambda *a,**k:imports.append((a,k))))
    planner=NS(store=LocalPlanStore(tmp_path),workspace=workspace,policy=lambda actor:{'can_generate':actor==OWNER},report_loader=lambda:data)
    return MigrationCenter(planner,7),data,jobs,imports


def save(center, keys, mode='WAVE', request_id=None):
    inspected=center.inspect(OWNER,keys,'scan1')
    return center.save(OWNER,keys,'scan1',inspected['inspection_hash'],mode,'Sample wave',request_id or str(uuid.uuid4()))['saved']


def test_individual_saves_are_persistent_and_retry_does_not_duplicate(tmp_path):
    center,_,_,_=setup(tmp_path); request=str(uuid.uuid4())
    first=save(center,['JOB:11','JOB:12'],'INDIVIDUAL',request)
    again=save(center,['JOB:11','JOB:12'],'INDIVIDUAL',request)
    assert len(center.list()['items'])==2
    assert [p['definition_id'] for p in first]==[p['definition_id'] for p in again]
    assert all(len(p['members'])==1 for p in first)


def test_viewer_cannot_save_or_launch_and_scan_changes_block_selection(tmp_path):
    center,data,jobs,_=setup(tmp_path)
    with pytest.raises(WorkflowError): center.inspect('viewer@example.com',['JOB:11'],'scan1')
    with pytest.raises(WorkflowError): center.inspect(OWNER,['JOB:11'],'old-scan')
    plan=save(center,['JOB:11'])[0]
    data['planning_sources']['11']['source_fingerprint']='c'*64
    with pytest.raises(WorkflowError): center.start(OWNER,plan['definition_id'],plan['definition_hash'],True)
    assert jobs.launches==[]


def test_blocked_members_can_be_saved_but_cannot_be_run(tmp_path):
    center,data,jobs,_=setup(tmp_path)
    data['planning_sources']['11']['findings']=[{'severity':'BLOCKER','rule':'jar','evidence':'Unsupported JAR','recommendation':'Replace it','limitation':'L5'}]
    plan=save(center,['JOB:11'])[0]
    assert not plan['members'][0]['supported']
    with pytest.raises(WorkflowError): center.start(OWNER,plan['definition_id'],plan['definition_hash'],True)
    assert not jobs.launches


def test_mixed_workload_kinds_and_conflicting_notebook_parameters_are_blocked(tmp_path):
    center,data,_,_=setup(tmp_path)
    with pytest.raises(WorkflowError): center.inspect(OWNER,['JOB:11','NOTEBOOK:99'],'scan1')
    notebook=data['assets'][-1];notebook['path']='/Workspace/shared'
    for index,source in enumerate(data['planning_sources'].values()):
        source['tasks'][0]['source_notebook']=notebook['path']
        source['settings']['tasks'][0]['notebook_task']['base_parameters']={'mode':str(index)}
    member=inspect_member(data,'NOTEBOOK:99')
    assert not member['supported']
    assert any('different notebook parameters' in b for b in member['blockers'])


def test_one_approval_launches_once_and_parallel_wave_records_each_result(tmp_path,monkeypatch):
    center,data,jobs,imports=setup(tmp_path)
    monkeypatch.setattr('migration_agent.migration_center.guarded_runner',lambda *a:None)
    monkeypatch.setattr('migration_agent.migration_center.read_source',lambda w,j,*a,**k:data['planning_sources'][j])
    plan=save(center,['JOB:11','JOB:12'])[0]
    center.start(OWNER,plan['definition_id'],plan['definition_hash'],True)
    center.start(OWNER,plan['definition_id'],plan['definition_hash'],True)
    assert len(jobs.launches)==1
    result=execute_saved_plan(center.workspace,center.store,plan['definition_id'],7)
    assert result['state']=='SUCCEEDED'
    assert len(result['members'])==2 and len(jobs.created)==2 and len(imports)==2
    for created in jobs.created:
        assert 'schedule' not in created and 'trigger' not in created
        assert created['environments'][0].spec.dependencies==['packaging==24.2']
        task=created['tasks'][0].as_dict()
        assert 'existing_cluster_id' not in task and 'libraries' not in task
        assert task['environment_key']=='migration_serverless'
    assert len(jobs.launches)==1  # Only the runner was launched, never either workload.
    assert center.get(plan['definition_id'])['run']['state']=='SUCCEEDED'
    with pytest.raises(WorkflowError): execute_saved_plan(center.workspace,center.store,plan['definition_id'],7)


def test_notebook_source_drift_stops_copy_before_candidate_creation(tmp_path,monkeypatch):
    center,_,jobs,imports=setup(tmp_path)
    monkeypatch.setattr('migration_agent.migration_center.guarded_runner',lambda *a:None)
    plan=save(center,['NOTEBOOK:99'],'INDIVIDUAL')[0]
    center.start(OWNER,plan['definition_id'],plan['definition_hash'],True)
    center.workspace.workspace.export=lambda *a,**k:NS(content=base64.b64encode(b'changed source').decode())
    result=execute_saved_plan(center.workspace,center.store,plan['definition_id'],7)
    assert result['state']=='FAILED' and result['members'][0]['state']=='FAILED'
    assert not jobs.created and not imports

def test_dependency_details_do_not_expose_package_repository_credentials(tmp_path):
    center,data,_,_=setup(tmp_path)
    data['planning_sources']['11']['settings']['tasks'][0]['libraries'][0]['pypi']['repo']='https://user:secret@private.example.com/simple'
    details=center.inspect(OWNER,['JOB:11'],'scan1')
    import json
    assert 'secret' not in json.dumps(details)
    assert details['members'][0]['task_libraries']==[{'kind':'PyPI','name':'packaging==24.2'}]


def test_authenticated_api_can_save_and_read_without_launching_compute(tmp_path,monkeypatch):
    import sys
    sys.path.insert(0,'app')
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse
    from fastapi.testclient import TestClient
    from migration_center_api import register_migration_center
    center,data,jobs,_=setup(tmp_path)
    monkeypatch.setenv('MIGRATION_EXECUTION_JOB','7')
    monkeypatch.setenv('MIGRATION_AUTH_MODE','local')
    app=FastAPI()
    @app.exception_handler(WorkflowError)
    async def error_handler(request,exc): return JSONResponse({'error':{'message':exc.message}},status_code=exc.status)
    register_migration_center(app,lambda:center.planner,identity=lambda request:OWNER)
    headers={'Origin':'http://testserver','X-Migration-Request':'1'}
    with TestClient(app) as client:
        denied=client.post('/api/migration-center/inspect',json={'asset_keys':['JOB:11'],'scan_id':'scan1'})
        assert denied.status_code==403
        inspected=client.post('/api/migration-center/inspect',headers=headers,json={'asset_keys':['JOB:11'],'scan_id':'scan1'}).json()
        saved=client.post('/api/migration-center/plans',headers=headers,json={'asset_keys':['JOB:11'],'scan_id':'scan1','inspection_hash':inspected['inspection_hash'],'mode':'INDIVIDUAL','name':'','request_id':str(uuid.uuid4())})
        assert saved.status_code==201
        reference=saved.json()['saved'][0]['definition_id']
        assert client.get('/api/migration-center/plans/'+reference).json()['name']=='Job 11'
        assert len(client.get('/api/migration-center/plans').json()['items'])==1
        assert jobs.launches==[] and jobs.created==[]


def test_repeated_source_versions_reuse_candidate_but_detect_changed_compute(tmp_path,monkeypatch):
    center,data,jobs,_=setup(tmp_path)
    monkeypatch.setattr('migration_agent.migration_center.guarded_runner',lambda *a:None)
    monkeypatch.setattr('migration_agent.migration_center.read_source',lambda w,j,*a,**k:data['planning_sources'][j])
    for index in range(2):
        plan=save(center,['JOB:11'],'INDIVIDUAL')[0]
        center.start(OWNER,plan['definition_id'],plan['definition_hash'],True)
        result=execute_saved_plan(center.workspace,center.store,plan['definition_id'],7)
        assert result['state']=='SUCCEEDED'
    assert len(jobs.created)==1
    assert result['members'][0]['reused'] is True
    jobs.created[0]['tasks'][0].existing_cluster_id='changed-to-classic'
    plan=save(center,['JOB:11'],'INDIVIDUAL')[0]
    center.start(OWNER,plan['definition_id'],plan['definition_hash'],True)
    result=execute_saved_plan(center.workspace,center.store,plan['definition_id'],7)
    assert result['state']=='FAILED' and len(jobs.created)==1


def test_standalone_notebook_creates_one_unscheduled_serverless_wrapper(tmp_path,monkeypatch):
    center,_,jobs,imports=setup(tmp_path)
    monkeypatch.setattr('migration_agent.migration_center.guarded_runner',lambda *a:None)
    plan=save(center,['NOTEBOOK:99'],'INDIVIDUAL')[0]
    center.start(OWNER,plan['definition_id'],plan['definition_hash'],True)
    result=execute_saved_plan(center.workspace,center.store,plan['definition_id'],7)
    assert result['state']=='SUCCEEDED'
    assert len(jobs.created)==1 and len(imports)==1
    assert jobs.created[0]['tasks'][0].notebook_task.notebook_path.startswith(ROOT+'/migration-candidates/')
    assert jobs.created[0]['environments'][0].spec.dependencies==[]

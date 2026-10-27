import uuid
from types import SimpleNamespace as NS
import pytest
import test_migration_center as base
from migration_agent.post_validation import ValidationService, execute_validation
from migration_agent.activity_log import activity
from migration_agent.workflow import WorkflowError


def completed(tmp_path,monkeypatch):
    center,data,jobs,imports=base.setup(tmp_path)
    monkeypatch.setattr('migration_agent.migration_center.guarded_runner',lambda *a:None)
    monkeypatch.setattr('migration_agent.post_validation.guarded_runner',lambda *a:None)
    monkeypatch.setattr('migration_agent.migration_center.read_source',lambda w,j,*a,**k:data['planning_sources'][j])
    monkeypatch.setattr('migration_agent.post_validation.read_source',lambda w,j,*a,**k:data['planning_sources'][j])
    saved=base.save(center,['JOB:11'],'INDIVIDUAL')[0]
    center.start(base.OWNER,saved['definition_id'],saved['definition_hash'],True)
    from migration_agent.migration_center import execute_saved_plan
    execute_saved_plan(center.workspace,center.store,saved['definition_id'],7)
    def get(job_id):
        created=jobs.created[int(job_id)-101]
        settings={'tasks':[t.as_dict() for t in created['tasks']],'environments':[e.as_dict() for e in created['environments']]}
        return NS(job_id=int(job_id),creator_user_name=base.OWNER,settings=NS(as_dict=lambda:settings))
    jobs.get=get
    data.update(scan_time='2026-10-27T10:00:00+00:00',findings=[],errors=[])
    center.workspace.config=NS(host='https://example.azuredatabricks.net')
    return center,data,jobs,saved,ValidationService(center.planner,7)


def test_successful_migration_is_eligible_but_does_not_auto_validate(tmp_path,monkeypatch):
    center,data,jobs,saved,validator=completed(tmp_path,monkeypatch)
    listing=validator.list()
    assert len(listing['items'])==1
    assert listing['items'][0]['validation'] is None
    assert listing['history']==[]
    assert len(jobs.launches)==1
    assert center.store.list_record_ids('validation_requests')==[]
    data['assets']=[]
    assert validator.list()['items']==[]  # Removed sources are not fresh validation targets.


def test_configuration_validation_never_executes_candidate_or_claims_output_parity(tmp_path,monkeypatch):
    center,_,jobs,saved,validator=completed(tmp_path,monkeypatch)
    token=str(uuid.uuid4())
    requested=validator.start(base.OWNER,saved['definition_id'],'JOB:11','CONFIGURATION',None,token,True)
    validator.start(base.OWNER,saved['definition_id'],'JOB:11','CONFIGURATION',None,token,True)
    assert len(jobs.launches)==2  # migration controller plus one validation controller
    result=execute_validation(center.workspace,center.store,None,requested['validation_id'],7)
    assert result['state']=='PASSED' and result['mode']=='CONFIGURATION'
    assert result['output_compared'] is False and result['metrics'] is None
    assert result['candidate_run_id'] is None
    assert len(jobs.launches)==2
    assert all(check['passed'] for check in result['checks'])
    with pytest.raises(WorkflowError):execute_validation(center.workspace,center.store,None,requested['validation_id'],7)


def test_validation_requires_success_and_rejects_unconfigured_output_comparison(tmp_path,monkeypatch):
    center,_,jobs,saved,validator=completed(tmp_path,monkeypatch)
    with pytest.raises(WorkflowError):validator.start(base.OWNER,saved['definition_id'],'JOB:12','CONFIGURATION',None,str(uuid.uuid4()),True)
    with pytest.raises(WorkflowError):validator.start('viewer@example.com',saved['definition_id'],'JOB:11','CONFIGURATION',None,str(uuid.uuid4()),True)
    with pytest.raises(WorkflowError):validator.start(base.OWNER,saved['definition_id'],'JOB:11','OUTPUT','123',str(uuid.uuid4()),True)
    assert len(jobs.launches)==1


def test_changed_candidate_fails_configuration_without_running_workload(tmp_path,monkeypatch):
    center,_,jobs,saved,validator=completed(tmp_path,monkeypatch)
    requested=validator.start(base.OWNER,saved['definition_id'],'JOB:11','CONFIGURATION',None,str(uuid.uuid4()),True)
    jobs.created[0]['tasks'][0].existing_cluster_id='classic'
    result=execute_validation(center.workspace,center.store,None,requested['validation_id'],7)
    assert result['state']=='FAILED'
    assert any(c['id']=='serverless' and not c['passed'] for c in result['checks'])
    assert len(jobs.launches)==2


def test_activity_records_request_and_result_only_after_explicit_validation(tmp_path,monkeypatch):
    center,data,jobs,saved,validator=completed(tmp_path,monkeypatch)
    before=activity(center.store,data)['events']
    assert not any(e['category']=='VALIDATION' for e in before)
    assert any(e['category']=='MIGRATION' for e in before)
    requested=validator.start(base.OWNER,saved['definition_id'],'JOB:11','CONFIGURATION',None,str(uuid.uuid4()),True)
    execute_validation(center.workspace,center.store,None,requested['validation_id'],7)
    events=activity(center.store,data)['events']
    validation=[e for e in events if e['category']=='VALIDATION']
    assert len(validation)==2
    assert any(e['title']=='Configuration checks passed' for e in validation)
    assert not any(e['title']=='Output validation passed' for e in validation)

def test_output_comparison_failure_keeps_difference_metrics(tmp_path,monkeypatch):
    center,data,jobs,saved,validator=completed(tmp_path,monkeypatch)
    monkeypatch.setattr('migration_agent.post_validation.source_supported',lambda source:True)
    monkeypatch.setattr('migration_agent.post_validation.parameters',lambda source:{'catalog':'test','schema':'migration_agent_jr_exp_test','groups':'2','libraries':'packaging','scenario':'orders'})
    import migration_agent.experiment_runner as runner
    monkeypatch.setattr(runner,'source_run',lambda *a:{'input_table':'`test`.`migration_agent_jr_exp_test`.`input_1`','summary_table':'`test`.`migration_agent_jr_exp_test`.`output_1`'})
    monkeypatch.setattr(runner,'compare',lambda *a:{'status':'VALIDATION_FAILED','classic_rows':2,'candidate_rows':3,'left_only':0,'right_only':1,'schema_match':True,'input_match':True})
    center.workspace.schemas=NS(get=lambda name:NS(properties={'experiment':'classic-libraries-10-20261026'}))
    original_run=jobs.run_now
    def run_now(**kwargs):
        if kwargs['job_id']==101:
            jobs.launches.append(kwargs)
            return NS(response=NS(run_id=99))
        return original_run(**kwargs)
    jobs.run_now=run_now
    jobs.get_run=lambda rid:NS(job_id=101,state=NS(result_state=NS(value='SUCCESS')),tasks=[NS(task_key=k,run_id=88,state=NS(result_state=NS(value='SUCCESS'))) for k in ('ingest','aggregate','verify')]) if int(rid)==99 else NS(job_id=7,state=NS(result_state=None))
    jobs.get_run_output=lambda rid:NS(notebook_output=NS(result='{"library_checks":{"packaging":"ok"}}'))
    requested=validator.start(base.OWNER,saved['definition_id'],'JOB:11','OUTPUT','123',str(uuid.uuid4()),True)
    spark=NS(sql=lambda query:NS(first=lambda:{'version':1}))
    result=execute_validation(center.workspace,center.store,spark,requested['validation_id'],7)
    assert result['state']=='FAILED' and result['output_compared'] is True
    assert result['metrics']['right_only']==1
    assert any(c['id']=='output' and not c['passed'] for c in result['checks'])
    assert len([launch for launch in jobs.launches if launch['job_id']==101])==1
    assert not any(launch['job_id']==11 for launch in jobs.launches)

def test_validation_and_activity_api_preserve_explicit_start_boundary(tmp_path,monkeypatch):
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse
    from fastapi.testclient import TestClient
    from post_migration_api import register_post_migration
    center,data,jobs,saved,validator=completed(tmp_path,monkeypatch)
    monkeypatch.setenv('MIGRATION_EXECUTION_JOB','7');monkeypatch.setenv('MIGRATION_AUTH_MODE','local')
    app=FastAPI()
    @app.exception_handler(WorkflowError)
    async def handler(request,exc):return JSONResponse({'error':{'message':exc.message}},status_code=exc.status)
    register_post_migration(app,lambda:center.planner,identity=lambda request:base.OWNER)
    with TestClient(app) as client:
        available=client.get('/api/post-validation/items')
        assert available.status_code==200 and len(available.json()['items'])==1
        assert len(jobs.launches)==1
        body={'definition_id':saved['definition_id'],'asset_key':'JOB:11','mode':'CONFIGURATION','baseline_run_id':None,'request_id':str(uuid.uuid4()),'acknowledged':True}
        assert client.post('/api/post-validation',json=body).status_code==403
        response=client.post('/api/post-validation',headers={'Origin':'http://testserver','X-Migration-Request':'1'},json=body)
        assert response.status_code==202
        assert client.get('/api/post-validation/'+response.json()['validation_id']).json()['mode']=='CONFIGURATION'
        assert any(e['category']=='VALIDATION' for e in client.get('/api/activity').json()['events'])
        assert len(jobs.launches)==2

def test_archived_sample_plans_leave_active_workspace_and_cannot_run(tmp_path,monkeypatch):
    center,data,jobs,saved,validator=completed(tmp_path,monkeypatch)
    center.store.write_once('migration_archives',saved['definition_id'],{'definition_id':saved['definition_id'],'archived_at':'2026-10-27T11:00:00+00:00','actor':base.OWNER,'reason':'Fresh-start cleanup'})
    assert center.list()['items']==[]
    assert validator.list()['items']==[]
    with pytest.raises(WorkflowError):center.start(base.OWNER,saved['definition_id'],saved['definition_hash'],True)
    with pytest.raises(WorkflowError):validator.start(base.OWNER,saved['definition_id'],'JOB:11','CONFIGURATION',None,str(uuid.uuid4()),True)
    assert any(e['title']=='Sample migration plan archived' for e in activity(center.store,data)['events'])

"""Execute the pinned experiment profile using an existing original baseline."""
import base64
import hashlib
import json
import re
import time
from databricks.sdk.service.jobs import Task, JobEnvironment
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, Language
from .experiment_profile import PROFILE, GRAPH, HASHES, source_supported, plan_matches, parameters
from .workflow import WorkflowError, read_source, verified_plan, timestamp, digest


def source_run(workspace, plan, run_id):
    if plan.get("execution_profile") != PROFILE or not plan_matches(plan) or not re.fullmatch(r"[0-9]+",str(run_id)):
        raise WorkflowError("BASELINE_PROFILE_REQUIRED","Select a successful run for the reviewed experiment profile.")
    run=workspace.jobs.get_run(int(run_id)).as_dict()
    if str(run.get("job_id"))!=plan["source_job_id"] or run.get("state",{}).get("result_state")!="SUCCESS":
        raise WorkflowError("INVALID_SOURCE_RUN","The baseline must be a successful original run of this job.")
    tasks=run.get("tasks",[]); expected={t["task_key"]:t for t in plan["tasks"]}; config=plan["experiment"]
    if len(tasks)!=3 or {t.get("task_key") for t in tasks}!=set(GRAPH):raise WorkflowError("INVALID_SOURCE_RUN","Incomplete experiment baseline task graph.")
    outputs={}
    snapshots=[]
    for t in tasks:
        k=t["task_key"];n=t.get("notebook_task",{})
        if t.get("state",{}).get("result_state")!="SUCCESS" or n.get("notebook_path")!=expected[k]["source_notebook"] or not t.get("existing_cluster_id") or t.get("environment_key") or [d["task_key"] for d in t.get("depends_on",[])]!=GRAPH[k]:
            raise WorkflowError("INVALID_SOURCE_RUN","Original task code reference, graph or classic compute differs.")
        params=n.get("base_parameters",{})
        if any(str(params.get(key))!=str(value) for key,value in config.items()):raise WorkflowError("INVALID_SOURCE_RUN","Baseline parameters differ from the approved experiment.")
        try:outputs[k]=json.loads(workspace.jobs.get_run_output(t["run_id"]).notebook_output.result)
        except (ValueError,TypeError,AttributeError):raise WorkflowError("BASELINE_OUTPUT_UNAVAILABLE","Original task result is no longer available.") from None
        snapshots.append({k:t.get(k) for k in ("task_key","run_id","notebook_task","depends_on","existing_cluster_id")})
    ns=f"`{config['catalog']}`.`{config['schema']}`"
    left=ns+f".`input_{run_id}`";right=ns+f".`output_{run_id}`"
    checks=set(config["libraries"].split(","))-{ "none" }
    if outputs["ingest"].get("input_table")!=left or outputs["ingest"].get("rows")!=1200 or set(outputs["ingest"].get("library_checks",{}))!=checks or outputs["aggregate"].get("output_table")!=right or outputs["verify"].get("output_table")!=right or outputs["verify"].get("status")!="PASS" or outputs["verify"].get("completed_orders")!=1080 or outputs["verify"].get("schema_match") is not True:
        raise WorkflowError("BASELINE_OUTPUT_MISMATCH","Original output assertions do not match the reviewed experiment.")
    return {"job_id":plan["source_job_id"],"run_id":str(run_id),"input_table":left,"summary_table":right,"duration_seconds":max(0,run.get("end_time",0)-run.get("start_time",0))/1000,"mode":"existing_source_run","run_snapshot_hash":digest({"tasks":sorted(snapshots,key=lambda t:t['task_key']),"outputs":outputs})}


def checked_plan(workspace,store,execution_id):
    from .execution import RUN_ACKS, require_demo
    request=store.read("executions",execution_id)
    if not request or request.get("profile")!=PROFILE or set(request.get("acknowledgements",[]))!=RUN_ACKS or request.get("baseline_mode")!="existing_source_run":raise WorkflowError("EXECUTION_REQUEST_REQUIRED","A confirmed experiment request with an original baseline is required.")
    plan=verified_plan(store.read("plans",request["plan_id"]))
    require_demo(plan,"")
    decision=store.read("decisions",plan["plan_id"]) or {}
    actor=request["actor"]
    if plan["initial_status"]!="DRAFT" or decision.get("decision")!="APPROVED" or decision.get("role")!="PROJECT_OPERATOR" or decision.get("actor","").casefold()!=actor.casefold() or (workspace.current_user.me().user_name or "").casefold()!=actor.casefold() or plan.get("project_owner","").casefold()!=actor.casefold():raise WorkflowError("APPROVED_PLAN_REQUIRED","Operator approval and runner identity must match.")
    for key in ("plan_hash","source_fingerprint"):
        if decision.get(key)!=plan[key] or request.get(key)!=plan[key]:raise WorkflowError("PLAN_CHANGED","The exact approved plan must match the execution.")
    if set(decision.get("acknowledgements",[]))!={a['id'] for a in plan["required_acknowledgements"]}:raise WorkflowError("ACKNOWLEDGEMENTS_REQUIRED","Approval conditions differ.")
    source=read_source(workspace,plan["source_job_id"],plan["scope_root"],actor,workspace_scope=True)
    job=workspace.jobs.get(int(plan["source_job_id"]))
    if (job.creator_user_name or "").casefold()!=actor.casefold() or source['source_fingerprint']!=plan['source_fingerprint'] or not source_supported(source) or parameters(source)!=plan["experiment"]:raise WorkflowError("SOURCE_DRIFT","Experiment source or dependencies changed; rescan and review a new plan.")
    if source_run(workspace,plan,request["source_baseline"]["run_id"])!=request["source_baseline"]:raise WorkflowError("BASELINE_CHANGED","The selected original run evidence changed.")
    notebooks={}
    for t in source['tasks']:
        p=t['source_notebook'];text=base64.b64decode(workspace.workspace.export(p,format=ExportFormat.SOURCE).content).decode('utf-8')
        if hashlib.sha256(text.replace('\r\n','\n').strip().encode()).hexdigest()!=HASHES[t['task_key']]:raise WorkflowError("SOURCE_DRIFT","Experiment notebook differs from its reviewed code.")
        notebooks[p]=text
    return request,plan,source,notebooks


def compare(spark,baseline,candidate_id,config):
    ns=f"`{config['catalog']}`.`{config['schema']}`"
    left=spark.read.option('versionAsOf',baseline['summary_version']).table(baseline['summary_table'])
    right_name=ns+f".`output_{candidate_id}`";right=spark.table(right_name)
    shape=lambda f:[(x.name,x.dataType.simpleString()) for x in f.schema]
    same=shape(left)==shape(right);lc,rc=left.count(),right.count()
    lo,ro=(left.exceptAll(right).count(),right.exceptAll(left).count()) if same else (None,None)
    li=spark.read.option('versionAsOf',baseline['input_version']).table(baseline['input_table'])
    ri=spark.table(ns+f".`input_{candidate_id}`")
    input_match=shape(li)==shape(ri) and li.exceptAll(ri).count()==ri.exceptAll(li).count()==0
    ls=int(left.selectExpr('sum(sale_count)').first()[0]);rs=int(right.selectExpr('sum(sale_count)').first()[0])
    passed=same and lo==ro==0 and lc==rc and ls==rs==1080 and input_match
    return {'status':'PASS' if passed else 'VALIDATION_FAILED','classic_table':baseline['summary_table'],'candidate_table':right_name,'schema_match':same,'classic_rows':lc,'candidate_rows':rc,'left_only':lo,'right_only':ro,'classic_sales':ls,'candidate_sales':rs,'input_match':input_match,'baseline_snapshot':baseline,'comparison':'Pinned original Delta output versus isolated serverless result; bidirectional EXCEPT ALL and input equality','checked_at':timestamp(),'promotion_enabled':False,'validation_profile':PROFILE}


def execute(workspace,store,spark,execution_id,sleeper=time.sleep,clock=time.monotonic):
    from .demo_runner import publish_progress,task_progress,verify_definition
    if not store.write_once('execution_started',execution_id,{'started_at':timestamp()}):raise WorkflowError('EXECUTION_ALREADY_STARTED','Inspect the existing execution; it cannot be launched twice.')
    run_id=None;stage='preflight'
    try:
        request,plan,source,notebooks=checked_plan(workspace,store,execution_id)
        config=plan['experiment'];baseline=dict(request['source_baseline'])
        schema=workspace.schemas.get(config['catalog']+'.'+config['schema'])
        if (schema.properties or {}).get('experiment')!='classic-libraries-10-20261026':raise WorkflowError('INVALID_OUTPUT_SCOPE','Experiment schema marker is missing.')
        for key in ('input','summary'):
            row=spark.sql('DESCRIBE HISTORY '+baseline[key+'_table']+' LIMIT 1').first()
            baseline[key+'_version']=int(row['version'])
        # Prove the classic input still matches the deterministic reviewed dataset.
        actual=spark.read.option('versionAsOf',baseline['input_version']).table(baseline['input_table'])
        expected=spark.sql(f"SELECT id AS order_id,id % {int(config['groups'])} AS region_id,CAST((id*37)%10000 AS BIGINT) AS amount_cents,id % 10=0 AS cancelled FROM range(1200)")
        if actual.exceptAll(expected).count() or expected.exceptAll(actual).count():raise WorkflowError('BASELINE_INPUT_MISMATCH','Retained baseline input is not the reviewed synthetic input.')
        store.write_once('execution_baseline',execution_id,baseline)
        store.write_once('execution_baseline_done',execution_id,{'status':'PASS','run_id':baseline['run_id'],'reused_original':True})
        store.write_once('execution_preflight',execution_id,{'status':'PASS','checked_at':timestamp(),'checks':['approved_plan','live_sources','dependencies','pinned_notebooks','input_snapshot','isolated_outputs']})
        paths={};candidate_root=plan['scope_root']+'/candidates/'+plan['plan_id']
        if plan['target']['candidate_root']!=candidate_root:raise WorkflowError('INVALID_TARGET','Invalid candidate storage root.')
        for original,content in notebooks.items():
            target=next(t['target_notebook'] for t in plan['tasks'] if t['source_notebook']==original)
            if not target.startswith(candidate_root+'/notebooks/') or '/..' in target:raise WorkflowError('INVALID_TARGET','Invalid candidate notebook destination.')
            workspace.workspace.mkdirs(target.rsplit('/',1)[0])
            workspace.workspace.import_(target,content=base64.b64encode(content.encode()).decode(),format=ImportFormat.SOURCE,language=Language.PYTHON,overwrite=False)
            paths[original]=target
        env=plan['target']['environment_key'];deps=sorted({p for t in source['tasks'] for p in t['serverless_dependencies']})
        if deps!=plan['target']['dependencies']:raise WorkflowError('PLAN_CHANGED','Candidate dependency mapping differs.')
        tasks=[{'task_key':t['task_key'],'notebook_task':{'notebook_path':paths[t['source_notebook']],'source':'WORKSPACE','base_parameters':{**config,'run_key':'{{job.run_id}}'}},'environment_key':env,'depends_on':[{'task_key':d} for d in t['depends_on']],'run_if':'ALL_SUCCESS','timeout_seconds':900,'max_retries':0} for t in source['tasks']]
        definition={'name':plan['target']['name'],'description':'Experiment serverless candidate; validated against retained original classic run.','tasks':tasks,'environments':[{'environment_key':env,'spec':{'environment_version':'4','dependencies':deps}}],'max_concurrent_runs':1,'timeout_seconds':2700,'tags':{'Project':'serverless-migration-challenge-jr','Purpose':'demo-execution-candidate','Execution':execution_id,'Plan':plan['plan_id']}}
        stage='candidate';publish_progress(store,execution_id,'CREATING_CANDIDATE')
        job=workspace.jobs.create(**{**definition,'tasks':[Task.from_dict(t) for t in tasks],'environments':[JobEnvironment.from_dict(e) for e in definition['environments']]})
        binding={'job_id':str(job.job_id),'definition_hash':digest(definition)}
        store.write_once('execution_candidate_job',execution_id,binding);store.write_once('execution_jobs',execution_id,{'candidate':binding})
        checked_plan(workspace,store,execution_id)
        live=workspace.jobs.get(job.job_id);verify_definition(live,definition,request['actor'])
        for t in live.settings.tasks:
            copied=base64.b64decode(workspace.workspace.export(t.notebook_task.notebook_path,format=ExportFormat.SOURCE).content).decode()
            if hashlib.sha256(copied.replace('\r\n','\n').strip().encode()).hexdigest()!=HASHES[t.task_key]:raise WorkflowError('CANDIDATE_CHANGED','Copied notebook code changed.')
        run=workspace.jobs.run_now(job_id=job.job_id,idempotency_token=execution_id+'-candidate').response;run_id=run.run_id
        store.write_once('execution_candidate',execution_id,{'job_id':str(job.job_id),'run_id':str(run_id),'started_at':timestamp()})
        deadline=clock()+2700
        while clock()<deadline:
            state=workspace.jobs.get_run(run_id)
            if state.job_id!=job.job_id:raise WorkflowError('INVALID_CHILD_RUN','Candidate run belongs to another job.')
            publish_progress(store,execution_id,'CANDIDATE_RUNNING',tasks=task_progress(state),run_state=state.state.life_cycle_state.value)
            if state.state.result_state:
                if state.state.result_state.value!='SUCCESS':raise WorkflowError('CHILD_RUN_FAILED','Serverless candidate task failed; inspect its run output.')
                if len(state.tasks or [])!=3 or any(t.state.result_state.value!='SUCCESS' for t in state.tasks):raise WorkflowError('CHILD_RUN_FAILED','Not all candidate tasks passed.')
                evidence={t.task_key:workspace.jobs.get_run_output(t.run_id).as_dict() for t in state.tasks}
                checks=json.loads(evidence['ingest']['notebook_output']['result']).get('library_checks',{})
                if set(checks)!=(set(config['libraries'].split(',')) - {'none'}):raise WorkflowError('LIBRARY_VALIDATION_FAILED','Candidate did not exercise all required libraries.')
                store.write_once('execution_diagnostics',execution_id,{'task_outputs':evidence,'library_checks':checks,'duration_ms':(state.end_time or 0)-(state.start_time or 0),'tasks':task_progress(state)})
                store.write_once('execution_candidate_done',execution_id,{'status':'PASS','run_id':str(run_id),'completed_at':timestamp()})
                break
            sleeper(10)
        else:raise WorkflowError('EXECUTION_TIMEOUT','Candidate exceeded the validation time budget.')
        stage='comparison';publish_progress(store,execution_id,'COMPARING_OUTPUTS')
        result=compare(spark,baseline,run_id,config)
        result.update(classic_duration_seconds=baseline.get('duration_seconds'),candidate_duration_seconds=max(0,(state.end_time or 0)-(state.start_time or 0))/1000,execution_id=execution_id,plan_id=plan['plan_id'],plan_hash=plan['plan_hash'],source_fingerprint=plan['source_fingerprint'],baseline_run_id=baseline['run_id'],candidate_run_id=str(run_id))
        if result['status']=='PASS':
            save_candidate_bundle(store,execution_id,plan,definition,str(job.job_id),result)
        store.write_once('execution_results',execution_id,result);publish_progress(store,execution_id,'FINISHED',status=result['status'])
        return result
    except Exception as exc:
        if run_id:
            try:
                if not workspace.jobs.get_run(run_id).state.result_state:workspace.jobs.cancel_run(run_id)
            except Exception:pass
        result={'status':'FAILED','failed_stage':stage,'code':exc.code if isinstance(exc,WorkflowError) else 'EXECUTION_ERROR','message':exc.message if isinstance(exc,WorkflowError) else 'Experiment execution failed; inspect runner logs.','error_type':type(exc).__name__,'checked_at':timestamp(),'promotion_enabled':False}
        if result['status']=='PASS':
            save_candidate_bundle(store,execution_id,plan,definition,str(job.job_id),result)
        store.write_once('execution_results',execution_id,result);publish_progress(store,execution_id,'FINISHED',status='FAILED',failed_stage=stage)
        return result


def save_candidate_bundle(store, execution_id, plan, definition, job_id, result):
    if result.get('status')!='PASS' or result.get('plan_hash')!=plan['plan_hash'] or result.get('execution_id')!=execution_id:
        raise WorkflowError('VALIDATION_REQUIRED','A matching passing validation is required for bundle export.')
    config={'bundle':{'name':'migration-validated-'+plan['plan_id'][:12]},'resources':{'jobs':{'candidate':definition}},'targets':{'validated':{'default':True,'mode':'production'}}}
    store.write_once('execution_candidate_bundle',execution_id,{'status':'PREPARED_NOT_BOUND','plan_id':plan['plan_id'],'plan_hash':plan['plan_hash'],'candidate_job_id':job_id,'definition_hash':digest(definition),'config':config,'instructions':'Bind resource jobs.candidate to candidate_job_id before deploying; the original classic job is unchanged.'})

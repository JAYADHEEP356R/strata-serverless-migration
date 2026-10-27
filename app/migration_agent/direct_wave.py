"""Wave approval and controlled creation of unvalidated serverless candidates."""
import base64
import hashlib
import re
import uuid
from databricks.sdk.service.jobs import JobEnvironment, Task
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, Language
from .execution import guarded_runner
from .experiment_profile import PROFILE, HASHES, source_supported
from .plan_store import ID
from .waves import verified_wave
from .workflow import WorkflowError, digest, read_source, timestamp, verified_plan


def claim_key(plan):
    return hashlib.sha256((str(plan['source_job_id']) + ':' + plan['source_fingerprint']).encode()).hexdigest()[:32]


class DirectWaveService:
    def __init__(self, planner, fleet, runner_id):
        self.planner, self.fleet, self.store = planner, fleet, planner.store
        self.runner_id = str(runner_id)

    def preview(self, actor, wave_id):
        if not ID.fullmatch(wave_id): raise WorkflowError('INVALID_WAVE_ID', 'Invalid wave reference.', 400)
        wave = verified_wave(self.store.read('waves', wave_id), wave_id)
        if not self.planner.policy(actor)['can_decide']: raise WorkflowError('NOT_AUTHORIZED', 'Only the project operator can approve a wave.', 403)
        detail = self.fleet.get(refresh=True)
        rows = {r['asset_key']: r for r in detail['workloads']}
        members = []
        for member in wave['members']:
            row = rows.get(member['asset_key'])
            latest = row.get('latest_plan') if row else None
            if not latest:
                members.append({'name': member['name'], 'asset_key': member['asset_key'], 'ready': False, 'reason': 'Prepare a plan for this job first.'})
                continue
            plan = self.planner.get(latest['plan_id'])
            reason = ''
            if plan['freshness'] != 'CURRENT': reason = 'The plan is stale; rescan and prepare a new plan.'
            elif plan['status'] not in {'DRAFT', 'APPROVED'}: reason = 'Resolve blockers or a rejected plan before migration.'
            elif plan.get('execution_profile') != PROFILE: reason = 'Direct migration is not yet supported for this job type.'
            elif row.get('migration_completed'): reason = 'A validated serverless candidate already exists.'
            elif self.store.read('direct_candidate_results', claim_key(plan)): reason = 'A serverless candidate already exists for this source version.'
            members.append({'name': member['name'], 'asset_key': member['asset_key'], 'plan_id': plan['plan_id'], 'plan_hash': plan['plan_hash'], 'source_fingerprint': plan['source_fingerprint'], 'ready': not reason, 'reason': reason, 'findings': plan['findings'], 'changes': plan['changes']})
        return {'wave_id':wave_id, 'wave_name':wave['name'], 'wave_hash':wave['wave_hash'], 'members':members, 'ready': bool(members) and all(m['ready'] for m in members), 'scope':'Creates unscheduled serverless candidates without running or validating jobs. Originals are retained.'}

    def start(self, actor, wave_id, wave_hash, plans, request_id, acknowledged):
        if not acknowledged: raise WorkflowError('ACKNOWLEDGEMENT_REQUIRED', 'Review the proposed changes and accept that candidates are not validated.', 400)
        try: request_id = str(uuid.UUID(request_id))
        except (ValueError, AttributeError): raise WorkflowError('INVALID_REQUEST_ID', 'Invalid request.', 400) from None
        preview = self.preview(actor, wave_id)
        if not preview['ready']: raise WorkflowError('WAVE_BLOCKED', 'Resolve all listed blockers before starting this wave.')
        expected = [{'plan_id': m['plan_id'], 'plan_hash':m['plan_hash'], 'source_fingerprint':m['source_fingerprint']} for m in preview['members']]
        if wave_hash != preview['wave_hash'] or sorted(plans,key=lambda x:x['plan_id']) != sorted(expected,key=lambda x:x['plan_id']): raise WorkflowError('PLAN_CHANGED', 'The wave or plans changed. Review them again.')
        wave=verified_wave(self.store.read('waves',wave_id),wave_id)
        guarded_runner(self.planner.workspace, self.runner_id, wave['scope_root'], actor)
        run_id = uuid.uuid5(uuid.NAMESPACE_URL, 'direct-wave:'+wave_id+':'+request_id).hex
        request={'run_id':run_id,'wave_id':wave_id,'wave_hash':wave_hash,'actor':actor.casefold(),'requested_at':timestamp(),'plans':expected,'runner_job_id':self.runner_id,'acknowledged':True}
        if not self.store.write_once('direct_wave_requests', wave_id, request):
            previous=self.store.read('direct_wave_requests', wave_id)
            if any(previous.get(k) != request[k] for k in ('run_id','wave_id','wave_hash','actor','plans','runner_job_id','acknowledged')): raise WorkflowError('WAVE_ALREADY_REQUESTED','This wave already has a direct migration request. Review its progress.')
        if not self.store.read('direct_wave_launches',wave_id):
            run=self.planner.workspace.jobs.run_now(job_id=int(self.runner_id),job_parameters={'execution_id':'wave-'+wave_id},idempotency_token='wave-'+run_id).response
            self.store.write_once('direct_wave_launches',wave_id,{'runner_run_id':str(run.run_id)})
        return self.status(actor,wave_id)

    def status(self, actor, wave_id):
        if not ID.fullmatch(wave_id): raise WorkflowError('INVALID_WAVE_ID','Invalid wave reference.',400)
        verified_wave(self.store.read('waves',wave_id),wave_id)
        request=self.store.read('direct_wave_requests',wave_id)
        if not request: return {'state':'NOT_STARTED','members':[]}
        result=self.store.read('direct_wave_results',wave_id)
        progress=self.store.read('direct_wave_progress',wave_id)
        launch=self.store.read('direct_wave_launches',wave_id)
        if result: return result
        state='REQUESTED'
        if launch:
            run=self.planner.workspace.jobs.get_run(int(launch['runner_run_id']))
            if str(run.job_id)!=self.runner_id: raise WorkflowError('INVALID_EXECUTOR','The runner binding is invalid.',403)
            state='NEEDS_REVIEW' if run.state and run.state.result_state else 'RUNNING'
        return {'state':state,'members':(progress or {}).get('members', [{'name':member['name'],'state':'QUEUED'} for member in verified_wave(self.store.read('waves',wave_id),wave_id)['members']]),'updated_at':(progress or {}).get('updated_at')}


def run_wave(workspace, store, wave_id, runner_id):
    request=store.read('direct_wave_requests',wave_id)
    wave=verified_wave(store.read('waves',wave_id),wave_id)
    if not request or request.get('wave_hash')!=wave['wave_hash'] or request.get('runner_job_id')!=str(runner_id) or not request.get('acknowledged') or request['actor'] != wave['project_owner'].casefold():
        raise WorkflowError('WAVE_REQUEST_INVALID','The wave approval does not match the operator or runner.',403)
    if (workspace.current_user.me().user_name or '').casefold()!=request['actor']:
        raise WorkflowError('NOT_AUTHORIZED','The runner must use the approving operator identity.',403)
    if not store.write_once('direct_wave_started',wave_id,{'started_at':timestamp()}):
        raise WorkflowError('WAVE_ALREADY_STARTED','This request has already started. Review its progress.')
    plans=[verified_plan(store.read('plans',ref['plan_id'])) for ref in request['plans']]
    if {p['asset_key'] for p in plans} != {m['asset_key'] for m in wave['members']} or len(plans) != len(wave['members']):
        raise WorkflowError('WAVE_REQUEST_INVALID','The approved plans do not match the wave members.',403)
    results=[{'name':p['source_name'],'plan_id':p['plan_id'],'state':'QUEUED'} for p in plans]
    def publish(): store.write_pointer('direct_wave_progress',wave_id,{'state':'RUNNING','updated_at':timestamp(),'members':results})
    publish()
    for ref,plan,entry in zip(request['plans'],plans,results):
        entry['state']='IN_PROGRESS';publish()
        try:
            if any(plan.get(k)!=ref[k] for k in ('plan_hash','source_fingerprint')) or plan['initial_status']!='DRAFT' or plan.get('execution_profile')!=PROFILE:
                raise WorkflowError('PLAN_CHANGED','The plan is blocked, changed, or unsupported.')
            decision = store.read('decisions', plan['plan_id'])
            if decision and decision.get('decision') == 'REJECTED': raise WorkflowError('PLAN_REJECTED','The plan was rejected after wave approval.')
            if plan.get('project_owner','').casefold() != request['actor'] or plan.get('scope_root') != wave.get('scope_root'): raise WorkflowError('INVALID_SCOPE','The plan owner or destination does not match the approved wave.',403)
            current=read_source(workspace,plan['source_job_id'],plan['scope_root'],request['actor'],workspace_scope=True)
            if current['source_fingerprint']!=plan['source_fingerprint'] or not source_supported(current):
                raise WorkflowError('SOURCE_DRIFT','The job, libraries, or notebooks changed since planning.')
            original=workspace.jobs.get(int(plan['source_job_id']))
            if (original.creator_user_name or '').casefold()!=request['actor']: raise WorkflowError('NOT_AUTHORIZED','The operator must own the source job.',403)
            key=claim_key(plan); claim={'wave_id':wave_id,'source_job_id':plan['source_job_id'],'source_fingerprint':plan['source_fingerprint']}
            if not store.write_once('direct_candidate_claims',key,claim) and store.read('direct_candidate_claims',key)!=claim:
                raise WorkflowError('CANDIDATE_EXISTS','A different wave already requested a candidate for this source version.')
            previous=store.read('direct_candidate_results',key)
            if previous:
                entry.update(state='CREATED',candidate_job_id=previous['candidate_job_id']);publish();continue
            paths={}
            for task in plan['tasks']:
                original_path=task['source_notebook']; target=task['target_notebook']
                if not target.startswith(plan['scope_root']+'/candidates/'+plan['plan_id']+'/notebooks/') or '/..' in target: raise WorkflowError('INVALID_TARGET','Candidate path is invalid.')
                content=base64.b64decode(workspace.workspace.export(original_path,format=ExportFormat.SOURCE).content).decode('utf-8')
                if hashlib.sha256(content.replace('\r\n','\n').strip().encode()).hexdigest()!=HASHES[task['task_key']]: raise WorkflowError('SOURCE_DRIFT','A notebook changed after review.')
                workspace.workspace.mkdirs(target.rsplit('/',1)[0])
                workspace.workspace.import_(target,content=base64.b64encode(content.encode()).decode(),format=ImportFormat.SOURCE,language=Language.PYTHON,overwrite=True)
                paths[original_path]=target
            configs={t['task_key']:t for t in current['settings']['tasks']}
            tasks=[]
            for task in plan['tasks']:
                original_task=configs[task['task_key']]
                notebook=dict(original_task['notebook_task']); notebook['notebook_path']=paths[task['source_notebook']]; notebook['source']='WORKSPACE'
                tasks.append(Task.from_dict({'task_key':task['task_key'],'notebook_task':notebook,'environment_key':plan['target']['environment_key'],'depends_on':[{'task_key':v} for v in task['depends_on']],'run_if':task['run_if'],'timeout_seconds':original_task.get('timeout_seconds',900),'max_retries':0}))
            deps=sorted({p for task in current['tasks'] for p in task['serverless_dependencies']})
            if deps!=plan['target']['dependencies']: raise WorkflowError('PLAN_CHANGED','The dependency mapping changed.')
            name='migration-direct-'+key
            matches=[j for j in workspace.jobs.list(name=name,expand_tasks=False) if (j.settings.tags or {}).get('DirectSource')==key]
            if len(matches)>1: raise WorkflowError('DUPLICATE_CANDIDATE','More than one candidate exists; review manually.')
            if matches: candidate_id=matches[0].job_id
            else:
                env=JobEnvironment.from_dict({'environment_key':plan['target']['environment_key'],'spec':{'environment_version':str(plan['target']['environment_version']),'dependencies':deps}})
                candidate_id=workspace.jobs.create(name=name,description='Unvalidated serverless candidate; original job retained. No schedule.',tasks=tasks,environments=[env],max_concurrent_runs=1,timeout_seconds=2700,tags={'Project':'serverless-migration-challenge-jr','Purpose':'direct-wave-candidate','DirectSource':key,'DirectWave':wave_id}).job_id
            record={'source_job_id':plan['source_job_id'],'source_fingerprint':plan['source_fingerprint'],'candidate_job_id':str(candidate_id),'plan_id':plan['plan_id'],'wave_id':wave_id,'validation':'NOT_RUN','created_at':timestamp()}
            store.write_once('direct_candidate_results',key,record)
            entry.update(state='CREATED',candidate_job_id=str(candidate_id),validation='NOT_RUN')
        except Exception as exc:
            entry.update(state='FAILED',message=str(exc) if isinstance(exc,WorkflowError) else 'Candidate creation failed. Review the runner logs.')
        publish()
    result={'state':'COMPLETED' if all(x['state']=='CREATED' for x in results) else 'NEEDS_REVIEW','members':results,'updated_at':timestamp()}
    store.write_once('direct_wave_results',wave_id,result)
    return result

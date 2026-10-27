"""Read a successful original orders run; never submit or modify that job."""
import json
import re
from .proof_profile import BATCH_PROFILE_ID, BATCH_DEPENDENCIES, batch_plan_matches
from .workflow import WorkflowError, digest

EXISTING_BASELINE = 'existing_source_run'


def source_run(workspace, plan, run_id, catalog, schema):
    if plan.get('execution_profile') != BATCH_PROFILE_ID or not batch_plan_matches(plan):
        raise WorkflowError('BASELINE_PROFILE_REQUIRED', 'Reusing original output requires a reviewed output-validation profile.')
    if not re.fullmatch(r'[0-9]+', str(run_id)):
        raise WorkflowError('SOURCE_RUN_REQUIRED', 'Select a successful run of the original classic job.', 400)
    raw = workspace.jobs.get_run(int(run_id)).as_dict()
    if str(raw.get('job_id')) != plan['source_job_id'] or raw.get('state', {}).get('result_state') != 'SUCCESS':
        raise WorkflowError('INVALID_SOURCE_RUN', 'The selected run must be a successful run of this original job.')
    tasks = raw.get('tasks', [])
    expected = {t['task_key']: t for t in plan['tasks']}
    if len(tasks) != 3 or {t.get('task_key') for t in tasks} != set(expected):
        raise WorkflowError('INVALID_SOURCE_RUN', 'The original run has an incomplete task graph.')
    snapshots, outputs = [], {}
    for task in tasks:
        key = task['task_key']
        notebook = task.get('notebook_task') or {}
        if task.get('state', {}).get('result_state') != 'SUCCESS' or notebook.get('notebook_path') != expected[key]['source_notebook'] or not (task.get('existing_cluster_id') or task.get('cluster_instance', {}).get('cluster_id')) or task.get('environment_key') or [d['task_key'] for d in task.get('depends_on', [])] != BATCH_DEPENDENCIES[key]:
            raise WorkflowError('INVALID_SOURCE_RUN', 'Original task source, classic compute, dependencies or success state differs from the reviewed job.')
        output = workspace.jobs.get_run_output(int(task['run_id']))
        try:
            outputs[key] = json.loads(output.notebook_output.result)
        except (TypeError, ValueError, AttributeError):
            raise WorkflowError('BASELINE_OUTPUT_UNAVAILABLE', 'Original run output is unavailable; select a retained successful run.') from None
        snapshots.append({k: task.get(k) for k in ('task_key', 'run_id', 'notebook_task', 'depends_on', 'existing_cluster_id', 'cluster_instance', 'start_time', 'end_time')})
    namespace = f'`{catalog}`.`{schema}`'
    input_table = namespace + f'.`proof_orders_input_{run_id}`'
    summary_table = namespace + f'.`proof_orders_summary_{run_id}`'
    ingest, aggregate, verify = [outputs[key] for key in ('ingest_orders', 'aggregate_orders', 'verify_orders')]
    if ingest.get('input_table') != input_table or ingest.get('orders') != 1200 or aggregate.get('summary_table') != summary_table or aggregate.get('regions') != 6 or verify.get('summary_table') != summary_table or verify.get('status') != 'PASS' or verify.get('schema_match') is not True or verify.get('regions') != 6 or verify.get('completed_orders') != 1080:
        raise WorkflowError('BASELINE_OUTPUT_MISMATCH', 'Original run output does not match the reviewed project tables and input assertions.')
    return {'job_id': plan['source_job_id'], 'run_id': str(run_id), 'mode': EXISTING_BASELINE,
            'input_table': input_table, 'summary_table': summary_table,
            'run_snapshot_hash': digest({'job_id': raw['job_id'], 'run_id': str(run_id), 'tasks': sorted(snapshots, key=lambda t: t['task_key']), 'outputs': outputs})}


def freeze_tables(spark, baseline):
    """Pin retained Delta versions and validate the deterministic source input."""
    versions = {}
    for key in ('input', 'summary'):
        name = baseline[key + '_table']
        history = spark.sql('DESCRIBE HISTORY ' + name + ' LIMIT 1').first()
        if history is None:
            raise WorkflowError('BASELINE_OUTPUT_UNAVAILABLE', 'The selected original output has no retained Delta version.')
        versions[key + '_version'] = int(history['version'])
    actual = spark.read.option('versionAsOf', versions['input_version']).table(baseline['input_table'])
    expected = spark.sql('SELECT id AS order_id, id % 6 AS region_id, CAST((id * 37) % 10000 AS BIGINT) AS amount_cents, id % 10 = 0 AS cancelled FROM range(1200)')
    shape = lambda frame: [(f.name, f.dataType.simpleString()) for f in frame.schema]
    if shape(actual) != shape(expected) or actual.exceptAll(expected).count() or expected.exceptAll(actual).count():
        raise WorkflowError('BASELINE_INPUT_MISMATCH', 'Original input snapshot differs from the deterministic input used by this candidate profile.')
    return {**baseline, **versions}

"""Execute only the pinned synthetic demo, with fresh classic/serverless evidence.

Runs as the existing project operator, never with broad App permissions. Original
jobs, notebooks, triggers and cluster ACLs are never edited. No promotion exists.
"""
import base64
import copy
import hashlib
import json
import logging
import re
import time

from databricks.sdk.service.jobs import JobEnvironment, Task
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, Language
from .execution import RUN_ACKS, require_demo
from .plan_store import ID
from .proof_profile import BATCH_PROFILE_ID, BATCH_PURPOSE, BATCH_NOTEBOOKS, BATCH_DEPENDENCIES, BATCH_HASHES, batch_source_supported
from .source_baseline import EXISTING_BASELINE, source_run, freeze_tables
from .workflow import PROJECT, WorkflowError, capture_cluster_evidence, digest, read_source, timestamp, verified_plan

DEMO_HASHES = {
    "prepare": "4188339a9328a1e241d5b5adbd22572734102b32267713481bfd75745def48ae",
    "workload": "5acfd5202d3298e5d0a1ffe8209fcbcdebd7de950da0102260083a959211310d",
    "validate": "1efc49288a31c0937b4c8439507bbdb095051c8907c753c308d916696ed9dc19",
}
DEPENDENCIES = {"prepare": [], "classic": ["prepare"], "serverless": ["prepare"], "validate": ["classic", "serverless"]}
CLASSIC_STARTUP_SECONDS = 900


def publish_progress(store, execution_id, phase, **details):
    try:
        store.write_progress(execution_id, {"phase": phase, "updated_at": timestamp(), **details})
    except Exception as exc:
        logging.getLogger("migration.execution").warning("progress_write_failed error_type=%s", type(exc).__name__)


def wait_for_classic(workspace, store, execution_id, cluster_id, deadline, sleeper=time.sleep, clock=time.monotonic):
    """Keep cluster provisioning outside the bounded notebook-task timeout."""
    started = clock()
    expires = min(deadline, started + CLASSIC_STARTUP_SECONDS)
    requested = False
    while clock() < expires:
        cluster = workspace.clusters.get(cluster_id)
        state = cluster.state.value
        publish_progress(store, execution_id, "CLASSIC_STARTUP", cluster_state=state, elapsed_seconds=int(clock() - started), startup_limit_seconds=CLASSIC_STARTUP_SECONDS)
        if state == "RUNNING":
            return
        if state == "TERMINATED" and not requested:
            workspace.clusters.start(cluster_id)
            requested = True
        elif state in {"ERROR", "UNKNOWN"} or state == "TERMINATED" and requested:
            raise WorkflowError("CLASSIC_STARTUP_FAILED", "The dedicated classic cluster could not start. Inspect its compute events; no baseline task was launched.")
        sleeper(10)
    raise WorkflowError("CLASSIC_STARTUP_TIMEOUT", "Classic compute did not become ready within the separate 15-minute startup budget. No baseline notebook task was launched.")


def task_progress(run):
    return [{"task_key": task.task_key, "state": task.state.life_cycle_state.value, "result": task.state.result_state.value if task.state.result_state else None,
             "setup_seconds": int((task.setup_duration or 0) / 1000), "execution_seconds": int((task.execution_duration or 0) / 1000)} for task in getattr(run, "tasks", None) or []]


def normalized_hash(source):
    return hashlib.sha256(source.replace("\r\n", "\n").strip().encode()).hexdigest()


def preflight(workspace, store, execution_id, source_job_id, cluster_id, catalog, schema):
    if not ID.fullmatch(execution_id):
        raise WorkflowError("INVALID_EXECUTION_ID", "Invalid execution reference.", 400)
    request = store.read("executions", execution_id)
    if not request or request.get("profile") not in {"isolated-demo-v1", BATCH_PROFILE_ID} or set(request.get("acknowledgements", [])) != RUN_ACKS:
        raise WorkflowError("EXECUTION_REQUEST_REQUIRED", "A confirmed demo execution request is required.")
    plan = verified_plan(store.read("plans", request["plan_id"]))
    decision = store.read("decisions", plan["plan_id"])
    actor = request["actor"]
    if not decision or decision.get("decision") != "APPROVED" or decision.get("plan_hash") != plan["plan_hash"] or decision.get("source_fingerprint") != plan["source_fingerprint"] or decision.get("role") != "PROJECT_OPERATOR" or (decision.get("actor") or "").casefold() != actor.casefold() or set(decision.get("acknowledgements", [])) != {a["id"] for a in plan["required_acknowledgements"]}:
        raise WorkflowError("APPROVED_PLAN_REQUIRED", "The exact plan must have an operator approval before execution.")
    if request["plan_hash"] != plan["plan_hash"] or request["source_fingerprint"] != plan["source_fingerprint"] or plan["initial_status"] != "DRAFT":
        raise WorkflowError("PLAN_CHANGED", "The execution request and approved plan differ.")
    if (workspace.current_user.me().user_name or "").casefold() != actor.casefold() or (plan.get("project_owner") or "").casefold() != actor.casefold():
        raise WorkflowError("RUNNER_IDENTITY", "The runner must use the existing approved project operator identity.", 403)
    root = require_demo(plan, source_job_id)
    batch = plan.get("execution_profile") == BATCH_PROFILE_ID
    if request["profile"] != (BATCH_PROFILE_ID if batch else "isolated-demo-v1"):
        raise WorkflowError("EXECUTION_PROFILE_CHANGED", "The request and approved validation profile differ.")
    effective_job_id = plan["source_job_id"] if batch else source_job_id
    if plan["target"]["candidate_root"] != root + "/candidates/" + plan["plan_id"]:
        raise WorkflowError("INVALID_TARGET", "The candidate folder is outside the approved plan.", 403)
    if not re.fullmatch(r"[A-Za-z0-9_]+", catalog) or not re.fullmatch(r"migration_agent_jr_[A-Za-z0-9_]+", schema):
        raise WorkflowError("INVALID_OUTPUT_SCOPE", "Demo output requires the dedicated project schema.", 403)
    schema_info = workspace.schemas.get(catalog + "." + schema)
    if (schema_info.properties or {}).get("migration_agent_project") != PROJECT:
        raise WorkflowError("INVALID_OUTPUT_SCOPE", "The output schema project marker does not match.", 403)
    source = read_source(workspace, effective_job_id, root, actor, workspace_scope=True)
    if source["source_fingerprint"] != plan["source_fingerprint"]:
        raise WorkflowError("SOURCE_DRIFT", "Live job, notebook or classic dependencies changed. Rescan and approve a new plan.")
    settings = source["settings"]
    tags = settings.get("tags", {})
    if tags.get("Project") != PROJECT or tags.get("Purpose") != (BATCH_PURPOSE if batch else "isolated-smoke-test") or any(settings.get(k) for k in ("schedule", "trigger", "continuous")) or batch and not batch_source_supported(source, root):
        raise WorkflowError("DEMO_ONLY", "Execution supports only the manually triggered project demo.", 403)
    creator = workspace.jobs.get(int(effective_job_id)).creator_user_name or ""
    if creator.casefold() != actor.casefold():
        raise WorkflowError("OWNER_MISMATCH", "The demo job owner changed.", 403)
    assignments = [t for t in settings["tasks"] if t.get("existing_cluster_id") or t.get("new_cluster") or t.get("job_cluster_key")]
    reuse_source = request.get("baseline_mode") == EXISTING_BASELINE
    if reuse_source:
        configured_clusters = {t.get("existing_cluster_id") for t in assignments}
        if not batch or len(configured_clusters) != 1 or None in configured_clusters:
            raise WorkflowError("BASELINE_PROFILE_REQUIRED", "This original-run profile requires one existing classic cluster for its reviewed tasks.")
        cluster_id = next(iter(configured_clusters))
        baseline = source_run(workspace, plan, (request.get("source_baseline") or {}).get("run_id"), catalog, schema)
        if baseline != request.get("source_baseline"):
            raise WorkflowError("BASELINE_CHANGED", "The selected original run evidence changed after confirmation.")
    if (batch and (len(assignments) != 3 or any(t.get("existing_cluster_id") != cluster_id or t.get("new_cluster") or t.get("job_cluster_key") for t in assignments))) or (not batch and (len(assignments) != 1 or assignments[0].get("task_key") != "classic" or assignments[0].get("existing_cluster_id") != cluster_id)):
        raise WorkflowError("DEMO_COMPUTE_CHANGED", "The demo must reference only its dedicated classic baseline.")
    capture_cluster_evidence(workspace, cluster_id, actor, effective_job_id, workspace_scope=reuse_source)
    expected_graph = BATCH_DEPENDENCIES if batch else DEPENDENCIES
    expected_hashes = BATCH_HASHES if batch else DEMO_HASHES
    notebooks = {}
    for task in source["tasks"]:
        if task["depends_on"] != expected_graph[task["task_key"]] or task["run_if"] != "ALL_SUCCESS" or task["language"] != "PYTHON":
            raise WorkflowError("DEMO_GRAPH_CHANGED", "The demo task graph or language changed.")
        path = task["source_notebook"]
        if path not in notebooks:
            result = workspace.workspace.export(path, format=ExportFormat.SOURCE)
            content = base64.b64decode(result.content or "", validate=True).decode("utf-8")
            name = path.rsplit("/", 1)[-1].removesuffix(".py")
            if normalized_hash(content) != expected_hashes.get(name):
                raise WorkflowError("DEMO_CODE_CHANGED", "This executor accepts only the reviewed synthetic demo notebook versions.")
            notebooks[path] = content
    return request, plan, source, notebooks


def candidate_definition(plan, source, paths, role, execution_id, catalog, schema, cluster_id):
    tasks = []
    batch = plan.get("execution_profile") == BATCH_PROFILE_ID
    environment = plan["target"]["environment_key"]
    for task in source["tasks"]:
        key = task["task_key"]
        params = {"catalog": catalog, "schema": schema, "run_key": "{{job.run_id}}"}
        if key in {"classic", "serverless"}:
            params.update(variant=key, inject_mismatch="false")
        definition = {"task_key": key, "notebook_task": {"notebook_path": paths[task["source_notebook"]], "source": "WORKSPACE", "base_parameters": params}, "depends_on": [{"task_key": d} for d in task["depends_on"]], "run_if": "ALL_SUCCESS", "timeout_seconds": 300, "max_retries": 0}
        if role == "baseline" and (batch or key == "classic"):
            definition["existing_cluster_id"] = cluster_id
        else:
            definition["environment_key"] = environment
        tasks.append(definition)
    if batch and role == "baseline":
        # Databricks does not need a serverless environment for an all-classic job.
        environments = []
    else:
        environments = [{"environment_key": environment, "spec": {"environment_version": "4"}}]
    return {"name": "migration-" + role + "-" + execution_id[:12], "description": "Isolated synthetic migration validation; original workload unchanged.", "tasks": tasks,
            "environments": environments,
            "max_concurrent_runs": 1, "timeout_seconds": 900,
            "tags": {"Project": PROJECT, "Purpose": "demo-execution-" + role, "Execution": execution_id, "Plan": plan["plan_id"]}}


def compare_demo(spark, catalog, schema, baseline_run_id, candidate_run_id, batch=False, baseline_snapshot=None):
    if not all(re.fullmatch(r"[0-9]+", str(value)) for value in (baseline_run_id, candidate_run_id)):
        raise WorkflowError("INVALID_OUTPUT_REFERENCE", "Unexpected demo output reference.")
    namespace = f"`{catalog}`.`{schema}`"
    prefix = "proof_orders_summary" if batch else "classic"
    left_name = namespace + f".`{prefix}_{baseline_run_id}`"
    right_name = namespace + f".`{prefix}_{candidate_run_id}`"
    left = spark.read.option("versionAsOf", baseline_snapshot["summary_version"]).table(left_name) if baseline_snapshot else spark.table(left_name)
    right = spark.table(right_name)
    same_schema = [(f.name, f.dataType.simpleString()) for f in left.schema] == [(f.name, f.dataType.simpleString()) for f in right.schema]
    left_count, right_count = left.count(), right.count()
    left_only = left.exceptAll(right).count() if same_schema else None
    right_only = right.exceptAll(left).count() if same_schema else None
    left_sales = int(left.selectExpr("sum(sale_count)").first()[0])
    right_sales = int(right.selectExpr("sum(sale_count)").first()[0])
    expected_rows, expected_sales = (6, 1080) if batch else (5, 1000)
    passed = same_schema and left_count == right_count == expected_rows and left_only == right_only == 0 and left_sales == right_sales == expected_sales
    return {"status": "PASS" if passed else "VALIDATION_FAILED", "classic_table": left_name, "candidate_table": right_name, "schema_match": same_schema, "classic_rows": left_count, "candidate_rows": right_count, "left_only": left_only, "right_only": right_only, "classic_sales": left_sales, "candidate_sales": right_sales, "comparison": "Selected original run versus serverless candidate; pinned Delta output version and bidirectional EXCEPT ALL" if baseline_snapshot else "Fresh classic baseline versus migrated serverless task; bidirectional EXCEPT ALL", "baseline_snapshot": baseline_snapshot, "checked_at": timestamp(), "promotion_enabled": False}


def verify_definition(job, expected, actor):
    """Check execution-affecting fields, including task parameters and compute."""
    settings = job.settings.as_dict()
    if (job.creator_user_name or "").casefold() != actor.casefold() or any(settings.get(k) for k in ("schedule", "trigger", "triggers", "continuous", "git_source", "job_clusters", "parameters", "environment_variables", "email_notifications", "webhook_notifications")):
        raise WorkflowError("CANDIDATE_CHANGED", "The isolated job owner or execution configuration changed.")
    for key in ("name", "tags", "environments", "max_concurrent_runs", "timeout_seconds"):
        if settings.get(key, [] if key == "environments" else None) != expected[key]:
            raise WorkflowError("CANDIDATE_CHANGED", "The isolated job configuration no longer matches its recorded definition.")
    actual_tasks = settings.get("tasks", [])
    if len(actual_tasks) != len(expected["tasks"]):
        raise WorkflowError("CANDIDATE_CHANGED", "The candidate task count changed.")
    safe_optional = {"description", "email_notifications", "webhook_notifications", "notification_settings", "health", "retry_on_timeout", "min_retry_interval_millis", "disable_auto_optimization"}
    indexed = {task["task_key"]: task for task in actual_tasks}
    for task in expected["tasks"]:
        actual = indexed.get(task["task_key"], {})
        if actual.get("email_notifications") or actual.get("webhook_notifications"):
            raise WorkflowError("CANDIDATE_CHANGED", "Candidate notifications must remain disabled.")
        if any(value for key, value in actual.items() if key not in set(task) | safe_optional):
            raise WorkflowError("CANDIDATE_CHANGED", "Unexpected candidate task execution settings were added.")
        for key, value in task.items():
            default = [] if key == "depends_on" else 0 if key == "max_retries" else None
            observed = actual.get(key, default)
            if key == "notebook_task":
                observed = {"source": "WORKSPACE", **(observed or {})}
            if observed != value:
                raise WorkflowError("CANDIDATE_CHANGED", "Candidate task source, parameters, compute or dependencies changed.")
    run_as = settings.get("run_as", {})
    if run_as and run_as.get("user_name", "").casefold() != actor.casefold():
        raise WorkflowError("CANDIDATE_CHANGED", "The execution identity changed.")


def execute_demo(workspace, store, spark, execution_id, source_job_id, cluster_id, catalog, schema, sleeper=time.sleep, clock=time.monotonic):
    request = store.read("executions", execution_id) or {}
    if request.get("profile") == "isolated-experiment-orders-v1":
        from .experiment_runner import execute
        return execute(workspace, store, spark, execution_id, sleeper, clock)
    # Job retries are disabled. An interrupted execution requires review rather
    # than silently recreating resources or charging for another baseline.
    if not store.write_once("execution_started", execution_id, {"started_at": timestamp()}):
        raise WorkflowError("EXECUTION_ALREADY_STARTED", "This execution has already started; inspect its existing runs.")
    child_runs = []
    deadline = clock() + 2100
    active_stage = "preflight"
    try:
        request, plan, source, notebooks = preflight(workspace, store, execution_id, source_job_id, cluster_id, catalog, schema)
        batch = plan.get("execution_profile") == BATCH_PROFILE_ID
        reuse_source = request.get("baseline_mode") == EXISTING_BASELINE
        baseline_snapshot = None
        if reuse_source:
            baseline_snapshot = freeze_tables(spark, request["source_baseline"])
            store.write_once("execution_baseline", execution_id, baseline_snapshot)
            store.write_once("execution_baseline_done", execution_id, {"status": "PASS", "run_id": baseline_snapshot["run_id"], "reused_original": True, "completed_at": timestamp()})
        store.write_once("execution_preflight", execution_id, {"status": "PASS", "checked_at": timestamp(), "source_fingerprint": source["source_fingerprint"], "checks": ["approved_plan", "live_sources", "classic_dependencies", "pinned_demo_code", "project_schema", "isolated_outputs"]})
        bindings, definitions = {}, {}
        for role in (("candidate",) if reuse_source else ("baseline", "candidate")):
            paths = {}
            for original, content in notebooks.items():
                if role == "candidate":
                    target = next(t["target_notebook"] for t in plan["tasks"] if t["source_notebook"] == original)
                else:
                    target = plan["target"]["candidate_root"] + "/baseline/notebooks/" + original.rsplit("/", 1)[-1].removesuffix(".py")
                if not target.startswith(plan["target"]["candidate_root"] + "/") or "/../" in target:
                    raise WorkflowError("INVALID_TARGET", "A copied notebook would leave the approved candidate folder.", 403)
                workspace.workspace.mkdirs(target.rsplit("/", 1)[0])
                workspace.workspace.import_(target, content=base64.b64encode(content.encode()).decode(), format=ImportFormat.SOURCE, language=Language.PYTHON, overwrite=False)
                paths[original] = target
            definition = candidate_definition(plan, source, paths, role, execution_id, catalog, schema, cluster_id)
            definitions[role] = definition
            # No retries around create: an ambiguous creation failure is surfaced
            # for review, so a duplicate saved job is never created automatically.
            created = workspace.jobs.create(**{**definition, "tasks": [Task.from_dict(t) for t in definition["tasks"]], "environments": [JobEnvironment.from_dict(e) for e in definition["environments"]]})
            bindings[role] = {"job_id": str(created.job_id), "definition_hash": digest(definition)}
            store.write_once("execution_" + role + "_job", execution_id, bindings[role])
        store.write_once("execution_jobs", execution_id, bindings)

        def run_and_wait(role):
            # Fresh check at each child launch, including after the baseline run.
            preflight(workspace, store, execution_id, source_job_id, cluster_id, catalog, schema)
            job_id = int(bindings[role]["job_id"])
            live = workspace.jobs.get(job_id)
            verify_definition(live, definitions[role], request["actor"])
            # Verify copied code again before any child run.
            for task in live.settings.tasks or []:
                path = task.notebook_task.notebook_path
                if not path.startswith(plan["target"]["candidate_root"] + "/"):
                    raise WorkflowError("CANDIDATE_CHANGED", "An execution notebook left the candidate folder.")
                copied = base64.b64decode(workspace.workspace.export(path, format=ExportFormat.SOURCE).content).decode()
                original_name = BATCH_NOTEBOOKS[task.task_key] if batch else "workload" if task.task_key in {"classic", "serverless"} else task.task_key
                if normalized_hash(copied) != (BATCH_HASHES if batch else DEMO_HASHES)[original_name]:
                    raise WorkflowError("CANDIDATE_CHANGED", "Copied demo code changed before execution.")
            if role == "baseline":
                wait_for_classic(workspace, store, execution_id, cluster_id, deadline, sleeper, clock)
                # Starting compute must not hide a config or library change.
                preflight(workspace, store, execution_id, source_job_id, cluster_id, catalog, schema)
            run = workspace.jobs.run_now(job_id=job_id, idempotency_token=execution_id + "-" + role).response
            child_runs.append((job_id, run.run_id))
            store.write_once("execution_" + role, execution_id, {"job_id": str(job_id), "run_id": str(run.run_id), "started_at": timestamp()})
            while clock() < deadline:
                state = workspace.jobs.get_run(run.run_id)
                if state.job_id != job_id:
                    raise WorkflowError("INVALID_CHILD_RUN", "A child run does not match the isolated execution job.")
                publish_progress(store, execution_id, role.upper() + "_RUNNING", tasks=task_progress(state), run_state=state.state.life_cycle_state.value)
                if state.state.result_state:
                    if state.state.result_state.value != "SUCCESS":
                        store.write_once("execution_diagnostics", execution_id, {"failed_stage": role, "tasks": task_progress(state), "checked_at": timestamp()})
                        raise WorkflowError("CHILD_RUN_FAILED", "The isolated " + role + " run failed. Review its task output; promotion remains blocked.")
                    store.write_once("execution_" + role + "_done", execution_id, {"status": "PASS", "run_id": str(run.run_id), "completed_at": timestamp()})
                    return run.run_id
                sleeper(10)
            raise WorkflowError("EXECUTION_TIMEOUT", "The demo execution exceeded its bounded waiting period.")

        active_stage = "baseline"
        baseline_id = int(baseline_snapshot["run_id"]) if baseline_snapshot else run_and_wait("baseline")
        active_stage = "candidate"
        candidate_id = run_and_wait("candidate")
        active_stage = "comparison"
        publish_progress(store, execution_id, "COMPARING_OUTPUTS")
        result = compare_demo(spark, catalog, schema, baseline_id, candidate_id, batch=batch, baseline_snapshot=baseline_snapshot)
        result["validation_profile"] = request["profile"]
        result.update(execution_id=execution_id, plan_id=plan["plan_id"], baseline_run_id=str(baseline_id), candidate_run_id=str(candidate_id), plan_hash=plan["plan_hash"], source_fingerprint=plan["source_fingerprint"])
        store.write_once("execution_results", execution_id, result)
        publish_progress(store, execution_id, "FINISHED", status=result["status"])
        return result
    except Exception as exc:
        for job_id, run_id in child_runs:
            try:
                run = workspace.jobs.get_run(run_id)
                if run.job_id == job_id and not run.state.result_state:
                    workspace.jobs.cancel_run(run_id)
            except Exception:
                pass
        result = {"status": "FAILED", "failed_stage": active_stage, "code": exc.code if isinstance(exc, WorkflowError) else "EXECUTION_ERROR", "message": exc.message if isinstance(exc, WorkflowError) else "Execution failed. Inspect the dedicated runner logs; no promotion occurred.", "error_type": type(exc).__name__, "checked_at": timestamp(), "promotion_enabled": False}
        store.write_once("execution_results", execution_id, result)
        publish_progress(store, execution_id, "FINISHED", status="FAILED", failed_stage=active_stage)
        return result

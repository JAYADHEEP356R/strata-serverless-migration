"""Verify the exact validated demo before Asset Bundle reconciliation.

This module reads workspace assets and validation records. It does not manage an
active-job preference, promote workloads, or launch compute.
"""
import base64
from databricks.sdk.service.workspace import ExportFormat
from .demo_runner import DEMO_HASHES, candidate_definition, normalized_hash, verify_definition
from .execution import RUN_ACKS, require_demo
from .workflow import PROJECT, WorkflowError, digest, read_source, verified_plan


def inspected_context(workspace, store, execution_id, source_job_id, actor, inspect_candidate=True):
    request = store.read("executions", execution_id)
    if not request or request.get("profile") != "isolated-demo-v1" or set(request.get("acknowledgements", [])) != RUN_ACKS or (request.get("actor") or "").casefold() != actor.casefold():
        raise WorkflowError("INVALID_EXECUTION", "Reconciliation requires this operator's validated demo execution.", 403)
    plan = verified_plan(store.read("plans", request["plan_id"]))
    if request.get("plan_hash") != plan["plan_hash"] or request.get("source_fingerprint") != plan["source_fingerprint"]:
        raise WorkflowError("INVALID_EXECUTION", "The execution request does not match the reviewed plan.")
    root = require_demo(plan, source_job_id)
    decision = store.read("decisions", plan["plan_id"])
    if not decision or decision.get("decision") != "APPROVED" or decision.get("role") != "PROJECT_OPERATOR" or set(decision.get("acknowledgements", [])) != {a["id"] for a in plan["required_acknowledgements"]} or decision.get("plan_hash") != plan["plan_hash"] or decision.get("source_fingerprint") != plan["source_fingerprint"] or (decision.get("actor") or "").casefold() != actor.casefold() or (workspace.current_user.me().user_name or "").casefold() != actor.casefold():
        raise WorkflowError("APPROVED_PLAN_REQUIRED", "The exact demo plan and operator identity must be verified.")
    source = read_source(workspace, source_job_id, root, actor, workspace_scope=True)
    original = workspace.jobs.get(int(source_job_id))
    if (original.creator_user_name or "").casefold() != actor.casefold() or source["settings"].get("tags", {}).get("Project") != PROJECT or source["settings"].get("tags", {}).get("Purpose") != "isolated-smoke-test":
        raise WorkflowError("DEMO_ONLY", "The original workload is outside the dedicated demo ownership scope.", 403)
    if source["source_fingerprint"] != plan["source_fingerprint"]:
        raise WorkflowError("SOURCE_DRIFT", "The original source changed. Review it before reconciling the candidate.")
    if any(source["settings"].get(k) for k in ("schedule", "trigger", "triggers", "continuous")):
        raise WorkflowError("SCHEDULED_CUTOVER_UNSUPPORTED", "This controller supports only the manually triggered demo. Scheduled cutover requires additional controls.")
    evidence = store.read("execution_results", execution_id)
    bindings = store.read("execution_jobs", execution_id)
    baseline = store.read("execution_baseline", execution_id)
    candidate = store.read("execution_candidate", execution_id)
    if not evidence or evidence.get("status") != "PASS" or evidence.get("execution_id") != execution_id or evidence.get("plan_hash") != plan["plan_hash"] or evidence.get("source_fingerprint") != plan["source_fingerprint"] or evidence.get("schema_match") is not True or any(evidence.get(k) != value for k, value in {"classic_rows": 5, "candidate_rows": 5, "classic_sales": 1000, "candidate_sales": 1000, "left_only": 0, "right_only": 0}.items()):
        raise WorkflowError("VALIDATION_REQUIRED", "Successful complete cross-run validation is required before reconciliation.")
    if not bindings or not candidate or not baseline or evidence.get("candidate_run_id") != candidate["run_id"] or evidence.get("baseline_run_id") != baseline["run_id"] or candidate["job_id"] != bindings["candidate"]["job_id"] or baseline["job_id"] != bindings["baseline"]["job_id"]:
        raise WorkflowError("INVALID_VALIDATION_BINDING", "The validation does not match the recorded baseline and candidate runs.")
    expected = candidate_definition(plan, source, {t["source_notebook"]: t["target_notebook"] for t in plan["tasks"]}, "candidate", execution_id, *output_scope(store), "unused")
    if digest(expected) != bindings["candidate"]["definition_hash"]:
        raise WorkflowError("CANDIDATE_CHANGED", "The candidate definition does not match the validated profile.")
    if inspect_candidate:
        for kind, binding in (("baseline", baseline), ("candidate", candidate)):
            run = workspace.jobs.get_run(int(binding["run_id"]))
            if run.job_id != int(binding["job_id"]) or not run.state.result_state or run.state.result_state.value != "SUCCESS":
                raise WorkflowError("RUN_NOT_SUCCESSFUL", "Both recorded child runs must have ended successfully.")
        launch = store.read("execution_launches", execution_id)
        parent = workspace.jobs.get_run(int(launch["run_id"])) if launch else None
        if not parent or parent.job_id != int(request["runner_job_id"]) or not parent.state.result_state or parent.state.result_state.value != "SUCCESS":
            raise WorkflowError("EXECUTION_NOT_COMPLETE", "Wait for the dedicated execution runner to finish successfully.")
        job = workspace.jobs.get(int(candidate["job_id"]))
        verify_definition(job, expected, actor)
        if list(workspace.jobs.list_runs(job_id=int(candidate["job_id"]), active_only=True, limit=1)):
            raise WorkflowError("CANDIDATE_BUSY", "Wait for the candidate's active run to finish before reconciliation.")
        for task in job.settings.tasks or []:
            source_text = base64.b64decode(workspace.workspace.export(task.notebook_task.notebook_path, format=ExportFormat.SOURCE).content).decode()
            name = "workload" if task.task_key in {"classic", "serverless"} else task.task_key
            if normalized_hash(source_text) != DEMO_HASHES[name]:
                raise WorkflowError("CANDIDATE_CHANGED", "A candidate notebook changed after validation.")
    return {"request": request, "plan": plan, "source": source, "evidence": evidence, "evidence_hash": digest(evidence), "definition": expected, "candidate": candidate}


def output_scope(store):
    # The production store is restricted to /Volumes/catalog/project_schema/plans.
    # Local tests explicitly supply the corresponding two identifiers.
    if hasattr(store, "volume"):
        parts = store.volume.strip("/").split("/")
        return parts[1], parts[2]
    return store.output_scope



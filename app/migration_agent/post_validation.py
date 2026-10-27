"""Explicit validation after candidate creation; configuration and output parity differ."""
import base64
import hashlib
import json
import time
import uuid
from databricks.sdk.errors import NotFound

from databricks.sdk.service.workspace import ExportFormat

from .assessment import fingerprint
from .execution import guarded_runner
from .experiment_profile import PROFILE, parameters, source_supported
from .migration_center import checked_id, verified_definition
from .workflow import WorkflowError, digest, read_source, timestamp


def migrated_member(store, definition_id, asset_key):
    definition = verified_definition(store.read("migration_definitions", checked_id(definition_id)), definition_id)
    if store.read("migration_archives", definition_id):
        raise WorkflowError("PLAN_ARCHIVED", "This migration was archived during cleanup. Create a new migration before validating.")
    member = next((m for m in definition["members"] if m["asset_key"] == asset_key), None)
    result = store.read("migration_results", definition_id)
    migrated = next((m for m in (result or {}).get("members", []) if m["asset_key"] == asset_key and m.get("state") == "SUCCEEDED"), None)
    if not member or not migrated or not str(migrated.get("candidate_job_id", "")).isdigit():
        raise WorkflowError("MIGRATION_REQUIRED", "This item needs a successful migration before validation can start.")
    return definition, member, migrated


class ValidationService:
    def __init__(self, planner, runner_id):
        self.planner, self.store, self.workspace = planner, planner.store, planner.workspace
        self.runner_id = str(runner_id)

    def output_support(self, member, report=None):
        source = (report or self.planner.report_loader()).get("planning_sources", {}).get(member["resource_id"], {})
        return member["kind"] == "JOB" and source.get("source_fingerprint") == member["fingerprint"] and source_supported(source)

    def status(self, validation_id):
        checked_id(validation_id)
        request = self.store.read("validation_requests", validation_id)
        if not request:
            raise WorkflowError("VALIDATION_NOT_FOUND", "The validation request was not found.", 404)
        result = self.store.read("validation_results", validation_id)
        progress = self.store.read("validation_progress", validation_id)
        status = result or progress or {"state": "QUEUED", "requested_at": request["requested_at"]}
        launch = self.store.read("validation_launches", validation_id)
        if not result and launch:
            try:
                run = self.workspace.jobs.get_run(int(launch["runner_run_id"]))
                if str(run.job_id) != self.runner_id:
                    raise WorkflowError("INVALID_RUN", "Validation runner binding differs.")
                if run.state and run.state.result_state:
                    status = {**status, "state": "NEEDS_REVIEW", "message": "The runner stopped without recording final validation evidence."}
            except WorkflowError:
                raise
            except Exception:
                status = {**status, "connection_warning": "Live runner status is unavailable. Showing recorded progress."}
        return {**request, **status, "runner_run_id": (launch or {}).get("runner_run_id")}

    def list(self):
        report = self.planner.report_loader()
        current_keys = {a["asset_key"] for a in report["assets"]}
        ids = self.store.list_record_ids("validation_requests", limit=201)
        validations = []
        partial = len(ids) > 200
        for reference in ids[:200]:
            try:
                validations.append(self.status(reference))
            except Exception:
                partial = True
        latest = {}
        for validation in sorted(validations, key=lambda v: v["requested_at"], reverse=True):
            latest.setdefault((validation["definition_id"], validation["asset_key"]), validation)
        items = []
        for reference in self.store.list_record_ids("migration_definitions", limit=201)[:200]:
            try:
                if self.store.read("migration_archives", reference):
                    continue
                definition = verified_definition(self.store.read("migration_definitions", reference), reference)
                result = self.store.read("migration_results", reference)
                for migrated in (result or {}).get("members", []):
                    if migrated.get("state") != "SUCCEEDED" or migrated["asset_key"] not in current_keys:
                        continue
                    member = next(m for m in definition["members"] if m["asset_key"] == migrated["asset_key"])
                    items.append({"definition_id": reference, "plan_name": definition["name"], "asset_key": member["asset_key"],
                        "name": member["name"], "kind": member["kind"], "source_job_id": member["resource_id"] if member["kind"] == "JOB" else None,
                        "candidate_job_id": migrated["candidate_job_id"], "migrated_at": migrated.get("finished_at") or result.get("finished_at"),
                        "output_supported": self.output_support(member, report), "validation": latest.get((reference, member["asset_key"]))})
            except Exception:
                partial = True
        return {"items": sorted(items, key=lambda i: i.get("migrated_at") or "", reverse=True),
            "history": sorted(validations, key=lambda v: v["requested_at"], reverse=True), "partial": partial}

    def start(self, actor, definition_id, asset_key, mode, baseline_run_id, request_id, acknowledged):
        if not self.planner.policy(actor)["can_generate"]:
            raise WorkflowError("NOT_AUTHORIZED", "Only the project operator can start validation.", 403)
        definition, member, migrated = migrated_member(self.store, definition_id, asset_key)
        if definition["owner"] != actor.casefold():
            raise WorkflowError("NOT_AUTHORIZED", "Validate with the operator who owns the migration.", 403)
        if not acknowledged or mode not in {"CONFIGURATION", "OUTPUT"}:
            raise WorkflowError("VALIDATION_APPROVAL", "Choose and approve the validation operation.", 400)
        try:
            request_id = str(uuid.UUID(request_id))
        except (ValueError, AttributeError):
            raise WorkflowError("INVALID_REQUEST", "A valid validation request is required.", 400) from None
        if mode == "OUTPUT" and (not self.output_support(member) or not str(baseline_run_id or "").isdigit()):
            raise WorkflowError("OUTPUT_CONTRACT_REQUIRED", "Output comparison needs a supported workload profile and a retained successful original run.")
        validation_id = uuid.uuid5(uuid.NAMESPACE_URL, definition_id + ":" + asset_key + ":" + request_id).hex
        request = {"validation_id": validation_id, "definition_id": definition_id, "definition_hash": definition["definition_hash"],
            "asset_key": asset_key, "name": member["name"], "source_fingerprint": member["fingerprint"], "candidate_job_id": migrated["candidate_job_id"],
            "mode": mode, "baseline_run_id": str(baseline_run_id) if mode == "OUTPUT" else None, "actor": actor.casefold(), "runner_job_id": self.runner_id,
            "requested_at": timestamp(), "acknowledged": True}
        self.store.write_once("validation_requests", validation_id, request)
        actual = self.store.read("validation_requests", validation_id)
        if any(actual.get(k) != request[k] for k in ("definition_hash", "asset_key", "source_fingerprint", "candidate_job_id", "mode", "baseline_run_id", "actor", "runner_job_id")):
            raise WorkflowError("REQUEST_CONFLICT", "This request already started a different validation.")
        if not self.store.read("validation_launches", validation_id):
            guarded_runner(self.workspace, self.runner_id, definition["scope_root"], actor)
            run = self.workspace.jobs.run_now(job_id=int(self.runner_id), job_parameters={"execution_id": "validate-" + validation_id}, idempotency_token="validate-" + validation_id).response
            self.store.write_once("validation_launches", validation_id, {"runner_run_id": str(run.run_id)})
        return self.status(validation_id)


def configuration_checks(workspace, definition, member, candidate_id, actor):
    checks = []
    current = None
    notebook_parameters = {}
    root = definition["scope_root"]
    if member["kind"] == "JOB":
        current = read_source(workspace, member["resource_id"], root, actor, workspace_scope=True)
        source_matches = current["source_fingerprint"] == member["fingerprint"]
    else:
        source = base64.b64decode(workspace.workspace.export(member["path"], format=ExportFormat.SOURCE).content or "", validate=True).decode("utf-8")
        source_matches = fingerprint({}, source) == member["fingerprint"]
        for context in member["contexts"]:
            related = read_source(workspace, context["job_id"], root, actor, workspace_scope=True)
            source_matches = source_matches and related["source_fingerprint"] == context["fingerprint"]
            source_task = next(t for t in related["settings"]["tasks"] if t["task_key"] == context["task_key"])
            notebook_parameters = source_task.get("notebook_task", {}).get("base_parameters", {})
        info = workspace.workspace.get_status(member["path"])
        source_matches = source_matches and bool(info.language) and info.language.value == member["tasks"][0]["language"]
    checks.append({"id": "source", "title": "Original source matches the migration", "passed": source_matches})
    candidate = workspace.jobs.get(int(candidate_id))
    settings = candidate.settings.as_dict()
    if (candidate.creator_user_name or "").casefold() != actor:
        raise WorkflowError("CANDIDATE_OWNER", "The candidate is not owned by the migration operator.", 403)
    tasks = settings.get("tasks", [])
    expected = {t["task_key"]: t for t in member["tasks"]}
    graph_matches = len(tasks) == len(expected) and {t["task_key"] for t in tasks} == set(expected)
    serverless = bool(tasks) and all(not any(t.get(k) for k in ("existing_cluster_id", "new_cluster", "job_cluster_key")) and t.get("environment_key") for t in tasks)
    copied_sources = graph_matches
    for task in tasks:
        original = expected.get(task["task_key"])
        notebook = task.get("notebook_task", {})
        path = notebook.get("notebook_path", "")
        if not original or not (path.startswith(root + "/candidates/") or path.startswith(root + "/migration-candidates/")):
            copied_sources = False
            continue
        graph_matches = graph_matches and sorted(d["task_key"] for d in task.get("depends_on", [])) == sorted(original.get("depends_on", [])) and task.get("run_if", "ALL_SUCCESS") == original.get("run_if", "ALL_SUCCESS")
        source = base64.b64decode(workspace.workspace.export(path, format=ExportFormat.SOURCE).content or "", validate=True).decode("utf-8")
        info = workspace.workspace.get_status(path)
        copied_sources = copied_sources and bool(info.language) and info.language.value == original["language"]
        if member["kind"] == "JOB":
            copied_sources = copied_sources and hashlib.sha256(source.replace("\r\n", "\n").strip().encode()).hexdigest() == original.get("source_hash")
            source_task = next(t for t in current["settings"]["tasks"] if t["task_key"] == task["task_key"])
            graph_matches = graph_matches and notebook.get("base_parameters", {}) == source_task.get("notebook_task", {}).get("base_parameters", {})
        else:
            copied_sources = copied_sources and fingerprint({}, source) == member["fingerprint"]
            graph_matches = graph_matches and notebook.get("base_parameters", {}) == notebook_parameters
    environments = {e["environment_key"]: e.get("spec", {}) for e in settings.get("environments", [])}
    mapped = sorted({p for spec in environments.values() for p in spec.get("dependencies", [])})
    library_matches = mapped == member["dependencies"] and all(t.get("environment_key") in environments and str(environments[t["environment_key"]].get("environment_version")) == "4" for t in tasks)
    checks += [
        {"id": "serverless", "title": "Candidate tasks use serverless compute", "passed": serverless},
        {"id": "graph", "title": "Task graph and notebook parameters are preserved", "passed": graph_matches},
        {"id": "notebooks", "title": "Copied notebooks match the inspected source", "passed": copied_sources},
        {"id": "libraries", "title": "Pinned library mappings match the serverless environment", "passed": library_matches},
        {"id": "triggers", "title": "Candidate automatic triggers are disabled", "passed": not any(settings.get(k) for k in ("schedule", "trigger", "continuous"))},
    ]
    snapshot = digest({k: settings.get(k) for k in ("tasks", "environments", "schedule", "trigger", "continuous")})
    return checks, current, snapshot


def execute_validation(workspace, store, spark, validation_id, runner_id, sleeper=time.sleep, clock=time.monotonic):
    checked_id(validation_id)
    request = store.read("validation_requests", validation_id)
    if not request or request.get("runner_job_id") != str(runner_id) or not request.get("acknowledged"):
        raise WorkflowError("VALIDATION_REQUEST", "A recorded validation approval is required.", 403)
    actor = (workspace.current_user.me().user_name or "").casefold()
    definition, member, migrated = migrated_member(store, request["definition_id"], request["asset_key"])
    if actor != request["actor"] or actor != definition["owner"] or request["definition_hash"] != definition["definition_hash"] or request["candidate_job_id"] != migrated["candidate_job_id"] or request["source_fingerprint"] != member["fingerprint"]:
        raise WorkflowError("VALIDATION_BINDING", "The validation approval does not match the migration and operator.", 403)
    if not store.write_once("validation_started", validation_id, {"started_at": timestamp()}):
        raise WorkflowError("VALIDATION_ALREADY_STARTED", "This validation request already started. Inspect its result.")
    started = timestamp()
    candidate_run_id = None
    checks = []
    metrics = None
    def progress(state, **details):
        store.write_pointer("validation_progress", validation_id, {"state": state, "started_at": started, "updated_at": timestamp(), "checks": checks, **details})
    try:
        progress("CHECKING_CONFIGURATION")
        checks, source, candidate_hash = configuration_checks(workspace, definition, member, request["candidate_job_id"], actor)
        if not all(c["passed"] for c in checks):
            raise WorkflowError("CONFIGURATION_MISMATCH", "One or more configuration checks failed. Workload execution was not started.")
        if request["mode"] == "OUTPUT":
            from .experiment_runner import source_run, compare
            if not source or not source_supported(source):
                raise WorkflowError("OUTPUT_PROFILE_REQUIRED", "This workload has no supported output-comparison profile.")
            config = parameters(source)
            schema = workspace.schemas.get(config["catalog"] + "." + config["schema"])
            if (schema.properties or {}).get("experiment") != "classic-libraries-10-20261026":
                raise WorkflowError("OUTPUT_SCOPE", "The synthetic validation schema marker is missing.")
            progress("CHECKING_BASELINE")
            plan = {"source_job_id": member["resource_id"], "execution_profile": PROFILE, "tasks": source["tasks"], "experiment": config}
            baseline = source_run(workspace, plan, request["baseline_run_id"])
            for key in ("input", "summary"):
                baseline[key + "_version"] = int(spark.sql("DESCRIBE HISTORY " + baseline[key + "_table"] + " LIMIT 1").first()["version"])
            # Recheck the candidate configuration immediately before submitting compute.
            refreshed, _, refreshed_hash = configuration_checks(workspace, definition, member, request["candidate_job_id"], actor)
            if refreshed_hash != candidate_hash or not all(c["passed"] for c in refreshed):
                raise WorkflowError("CANDIDATE_CHANGED", "Candidate configuration changed before execution.")
            run = workspace.jobs.run_now(job_id=int(request["candidate_job_id"]), idempotency_token="validation-child-" + validation_id).response
            candidate_run_id = str(run.run_id)
            store.write_once("validation_candidate_run", validation_id, {"candidate_job_id": request["candidate_job_id"], "candidate_run_id": candidate_run_id})
            deadline = clock() + 1800
            while clock() < deadline:
                live = workspace.jobs.get_run(int(candidate_run_id))
                if str(live.job_id) != request["candidate_job_id"]:
                    raise WorkflowError("INVALID_CANDIDATE_RUN", "The candidate run belongs to another job.")
                progress("RUNNING_CANDIDATE", candidate_run_id=candidate_run_id)
                if live.state and live.state.result_state:
                    if live.state.result_state.value != "SUCCESS" or len(live.tasks or []) != 3 or any(not t.state or not t.state.result_state or t.state.result_state.value != "SUCCESS" for t in live.tasks):
                        raise WorkflowError("CANDIDATE_RUN_FAILED", "A serverless candidate task did not succeed.")
                    ingest = next(t for t in live.tasks if t.task_key == "ingest")
                    output = workspace.jobs.get_run_output(ingest.run_id)
                    library_checks = json.loads(output.notebook_output.result).get("library_checks", {})
                    if set(library_checks) != set(config["libraries"].split(",")) - {"none"}:
                        raise WorkflowError("LIBRARY_CHECK_FAILED", "The candidate did not exercise all expected libraries.")
                    break
                sleeper(10)
            else:
                raise WorkflowError("VALIDATION_TIMEOUT", "The candidate exceeded the validation time budget.")
            progress("COMPARING_OUTPUTS", candidate_run_id=candidate_run_id)
            final_checks, _, final_hash = configuration_checks(workspace, definition, member, request["candidate_job_id"], actor)
            if final_hash != candidate_hash or not all(c["passed"] for c in final_checks):
                raise WorkflowError("CANDIDATE_CHANGED", "The source or candidate configuration changed during validation.")
            metrics = compare(spark, baseline, candidate_run_id, config)
            checks += [{"id": "candidate_run", "title": "All serverless candidate tasks succeeded", "passed": True},
                {"id": "library_execution", "title": "Required libraries executed successfully", "passed": True},
                {"id": "output", "title": "Input, schema, and duplicate-sensitive output comparisons match", "passed": metrics["status"] == "PASS"}]
            if metrics["status"] != "PASS":
                raise WorkflowError("OUTPUT_MISMATCH", "The original and serverless outputs differ.")
        result = {"state": "PASSED", "mode": request["mode"], "checks": checks, "metrics": metrics, "started_at": started, "finished_at": timestamp(),
            "candidate_run_id": candidate_run_id, "baseline_run_id": request.get("baseline_run_id"), "output_compared": request["mode"] == "OUTPUT"}
    except Exception as exc:
        if candidate_run_id:
            try:
                if not workspace.jobs.get_run(int(candidate_run_id)).state.result_state:
                    workspace.jobs.cancel_run(int(candidate_run_id))
            except Exception:
                pass
        if isinstance(exc, NotFound):
            exc = WorkflowError("RESOURCE_REMOVED", "The original or candidate resource was removed. Refresh inventory and create a new migration.")
        result = {"state": "FAILED", "mode": request["mode"], "checks": checks, "started_at": started, "finished_at": timestamp(), "candidate_run_id": candidate_run_id,
            "metrics": metrics, "output_compared": metrics is not None, "code": exc.code if isinstance(exc, WorkflowError) else "VALIDATION_ERROR", "message": exc.message if isinstance(exc, WorkflowError) else "Validation failed. Inspect the recorded runner logs."}
    store.write_once("validation_results", validation_id, result)
    return result

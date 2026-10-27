"""Saved job/notebook migration plans and observable candidate creation runs.

Migration runs create unscheduled serverless job definitions and notebook copies.
They never execute source or candidate workloads, change triggers, or delete sources.
"""
import base64
import hashlib
import posixpath
import re
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

from databricks.sdk.service.jobs import JobEnvironment, Task
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, Language

from .assessment import fingerprint
from .execution import guarded_runner
from .experiment_profile import dependencies
from .plan_store import ID
from .workflow import WorkflowError, digest, read_source, timestamp

LIMIT = 50
RUNNING = {"QUEUED", "INSPECTING", "COPYING", "CREATING"}


def checked_id(value):
    if not isinstance(value, str) or not ID.fullmatch(value):
        raise WorkflowError("INVALID_REFERENCE", "Invalid migration reference.", 400)
    return value


def candidate_key(member):
    return digest({"asset": member["asset_key"], "source": member["fingerprint"]})[:32]


def verified_definition(value, identity=None):
    if not value or value.get("definition_hash") != digest({k: v for k, v in value.items() if k != "definition_hash"}):
        raise WorkflowError("PLAN_INTEGRITY", "The saved migration plan could not be verified.")
    checked_id(value.get("definition_id"))
    if identity and value["definition_id"] != identity:
        raise WorkflowError("PLAN_INTEGRITY", "The saved migration plan has a different identity.")
    return value


def unique_findings(findings):
    result = {}
    for item in findings:
        safe = {k: item.get(k, "") for k in ("severity", "rule", "evidence", "recommendation", "limitation")}
        result[digest(safe)] = safe
    return list(result.values())


def safe_libraries(items):
    output = []
    for item in items or []:
        if "pypi" in item:
            output.append({"kind": "PyPI", "name": item["pypi"].get("package", "Unspecified package")})
        else:
            kind = next(iter(item), "unknown")
            output.append({"kind": kind.upper(), "name": "Configured " + kind.upper() + " dependency"})
    return output


def inspect_member(report, asset_key):
    asset = next((a for a in report["assets"] if a["asset_key"] == asset_key and not a.get("is_fixture")), None)
    if not asset or asset.get("asset_type") not in {"JOB", "NOTEBOOK"}:
        raise WorkflowError("OUTSIDE_INVENTORY", "Select a job or notebook in the current inventory.", 403)
    member = {"asset_key": asset_key, "kind": asset["asset_type"], "resource_id": str(asset["resource_id"]),
              "name": asset["name"], "path": asset.get("path"), "compute": asset.get("compute_kind", "UNKNOWN"),
              "readiness": asset.get("readiness_score"), "tasks": [], "dependencies": [], "clusters": [],
              "contexts": [], "task_libraries": [], "findings": [], "blockers": [], "supported": False}
    sources = report.get("planning_sources", {})
    if member["kind"] == "JOB":
        source = sources.get(member["resource_id"])
        if not source:
            member["fingerprint"] = asset.get("source_fingerprint", "")
            member["blockers"] = [asset.get("planning_reason") or "The job has no complete, supported inspection. Rescan or resolve its compatibility findings."]
            return member
        member["fingerprint"] = source["source_fingerprint"]
        member["tasks"] = [{k: t.get(k) for k in ("task_key", "source_notebook", "source_hash", "language", "depends_on", "run_if", "source_compute")} for t in source["tasks"]]
        member["dependencies"] = sorted({p for t in source["tasks"] for p in t.get("serverless_dependencies", [])})
        member["task_libraries"] = safe_libraries([lib for task in source["settings"].get("tasks", []) for lib in task.get("libraries", [])])
        for cluster in source.get("source_checks", {}).get("cluster_evidence", []):
            member["clusters"].append({"cluster_id": cluster["cluster_id"], "captured_at": cluster.get("captured_at"),
                "libraries": safe_libraries(cluster.get("libraries")) if "libraries" in cluster else None, "library_inspection": "KNOWN" if "libraries" in cluster else "RESCAN_REQUIRED"})
        member["findings"] = unique_findings(source.get("findings", []))
        if asset.get("compute_kind") not in {"CLASSIC", "MIXED"}:
            member["blockers"].append("This job has no classic compute to migrate.")
    else:
        member["fingerprint"] = asset.get("source_fingerprint", "")
        member["tasks"] = [{"task_key": "notebook", "source_notebook": asset.get("path"), "language": asset.get("language"), "depends_on": []}]
        member["findings"] = unique_findings(asset.get("findings", []))
        path = asset.get("path") or ""
        if not path.startswith("/") or posixpath.normpath(path) != path:
            member["blockers"].append("The notebook has no valid workspace source path.")
        scope = report.get("scope_root", "")
        if scope and (path.startswith(scope + "/candidates/") or path.startswith(scope + "/migration-candidates/") or path.removesuffix(".py") in {scope + "/notebooks/discover_assess", scope + "/notebooks/execute_demo"}):
            member["blockers"].append("This notebook is an agent control resource or an existing migration candidate.")
        parameter_shapes = set()
        all_requirements = []
        for job_id, source in sources.items():
            settings = {t["task_key"]: t for t in source.get("settings", {}).get("tasks", [])}
            for task in source.get("tasks", []):
                if task.get("source_notebook") != asset.get("path"):
                    continue
                member["contexts"].append({"job_id": job_id, "name": source["name"], "fingerprint": source["source_fingerprint"], "task_key": task["task_key"]})
                parameter_shapes.add(digest(settings.get(task["task_key"], {}).get("notebook_task", {}).get("base_parameters", {})))
                all_requirements.extend({"pypi": {"package": p}} for p in task.get("serverless_dependencies", []))
                member["findings"].extend(source.get("findings", []))
                for cluster in source.get("source_checks", {}).get("cluster_evidence", []):
                    member["clusters"].append({"cluster_id": cluster["cluster_id"], "libraries": safe_libraries(cluster.get("libraries")) if "libraries" in cluster else None, "library_inspection": "KNOWN" if "libraries" in cluster else "RESCAN_REQUIRED"})
        mapped = dependencies(all_requirements)
        if mapped is None:
            member["blockers"].append("Associated jobs use conflicting dependency versions. Choose a single job context before migrating this notebook.")
        else:
            member["dependencies"] = mapped
        if len(parameter_shapes) > 1:
            member["blockers"].append("Associated jobs pass different notebook parameters. Migrate the relevant job instead.")
        if asset.get("assessment_coverage") != "COMPLETE":
            member["blockers"].append("Notebook inspection is incomplete. Rescan with source access.")
        if asset.get("language") not in {"PYTHON", "SQL"}:
            member["blockers"].append("Only Python and SQL workspace notebooks are supported.")
        if not member["contexts"]:
            member["findings"].append({"severity": "WARNING", "rule": "standalone-environment", "evidence": "No inspected job context supplies this notebook's libraries or parameters.", "recommendation": "Review its runtime dependencies and parameters before executing the candidate.", "limitation": "L5"})
    member["findings"] = unique_findings(member["findings"])
    member["blockers"].extend(f["evidence"] for f in member["findings"] if f["severity"] in {"BLOCKER", "REVIEW"})
    member["blockers"] = list(dict.fromkeys(member["blockers"]))
    member["clusters"] = list({c["cluster_id"]: c for c in member["clusters"]}.values())
    member["supported"] = bool(member["fingerprint"]) and not member["blockers"]
    return member


class MigrationCenter:
    def __init__(self, planner, runner_id):
        self.planner, self.store, self.workspace = planner, planner.store, planner.workspace
        self.runner_id = str(runner_id)

    def authorize(self, actor):
        if not self.planner.policy(actor)["can_generate"]:
            raise WorkflowError("NOT_AUTHORIZED", "Only the signed-in project operator can save or run migrations.", 403)

    def inspect(self, actor, asset_keys, scan_id):
        self.authorize(actor)
        if not 1 <= len(asset_keys) <= LIMIT or len(set(asset_keys)) != len(asset_keys):
            raise WorkflowError("INVALID_SELECTION", "Select 1–50 distinct items.", 400)
        report = self.planner.report_loader()
        if report.get("scope") != "workspace" or report["scan_id"] != scan_id:
            raise WorkflowError("STALE_INVENTORY", "The inventory changed. Refresh and review your selection.")
        members = [inspect_member(report, key) for key in asset_keys]
        if len({m["kind"] for m in members}) != 1:
            raise WorkflowError("MIXED_SELECTION", "Choose jobs or notebooks for one migration plan.", 400)
        return {"scan_id": scan_id, "members": members, "inspection_hash": digest(members)}

    def save(self, actor, asset_keys, scan_id, inspection_hash, mode, name, request_id):
        try:
            request_id = str(uuid.UUID(request_id))
        except (ValueError, AttributeError):
            raise WorkflowError("INVALID_REQUEST", "A valid save request is required.", 400) from None
        inspection = self.inspect(actor, asset_keys, scan_id)
        if inspection["inspection_hash"] != inspection_hash:
            raise WorkflowError("INSPECTION_CHANGED", "The inspected configuration changed. Review the selection again.")
        if mode not in {"WAVE", "INDIVIDUAL"}:
            raise WorkflowError("INVALID_MODE", "Choose a wave or individual migration plans.", 400)
        name = " ".join(name.split())
        if mode == "WAVE" and not 1 <= len(name) <= 80:
            raise WorkflowError("INVALID_NAME", "Give the wave a name of 1–80 characters.", 400)
        report = self.planner.report_loader()
        groups = [inspection["members"]] if mode == "WAVE" else [[m] for m in inspection["members"]]
        saved = []
        for index, members in enumerate(groups):
            reference = uuid.uuid5(uuid.NAMESPACE_URL, actor.casefold() + ":migration-center:" + request_id + ":" + str(index)).hex
            body = {"mode": mode, "kind": members[0]["kind"], "name": name if mode == "WAVE" else members[0]["name"], "members": members,
                    "scan_id": scan_id, "scope_root": report["scope_root"], "owner": actor.casefold()}
            request_hash = digest(body)
            value = {"definition_id": reference, "created_at": timestamp(), "request_hash": request_hash, **body}
            value["definition_hash"] = digest(value)
            self.store.write_once("migration_definitions", reference, value)
            actual = verified_definition(self.store.read("migration_definitions", reference), reference)
            if actual["request_hash"] != request_hash:
                raise WorkflowError("SAVE_CONFLICT", "This save request already contains different items.")
            saved.append(self.get(reference))
        return {"saved": saved}

    def get(self, reference):
        checked_id(reference)
        value = verified_definition(self.store.read("migration_definitions", reference), reference)
        request = self.store.read("migration_requests", reference)
        run = None
        if request:
            run = self.store.read("migration_results", reference) or self.store.read("migration_progress", reference)
            if not run:
                run = {"state": "QUEUED", "started_at": request["requested_at"], "members": [{"asset_key": m["asset_key"], "name": m["name"], "state": "QUEUED"} for m in value["members"]]}
            launch = self.store.read("migration_launches", reference)
            if launch:
                run = {**run, "runner_run_id": launch["runner_run_id"]}
                if run["state"] in {"RUNNING", "QUEUED"}:
                    try:
                        live = self.workspace.jobs.get_run(int(launch["runner_run_id"]))
                        if str(live.job_id) != self.runner_id:
                            raise WorkflowError("INVALID_RUN", "Migration runner does not match.")
                        if live.state and live.state.result_state:
                            # A terminated runner without final records is never a success.
                            run = {**run, "state": "NEEDS_REVIEW", "message": "The runner stopped before a final result was recorded."}
                    except WorkflowError:
                        raise
                    except Exception:
                        run = {**run, "connection_warning": "Live runner status is unavailable. Showing saved progress."}
        host = (getattr(getattr(self.workspace, "config", None), "host", "") or "").rstrip("/")
        if not re.fullmatch(r"https://[A-Za-z0-9.-]+\.(?:databricks\.com|azuredatabricks\.net)", host):
            host = None
        return {**value, "run": run, "workspace_host": host, "archived": bool(self.store.read("migration_archives", reference))}

    def list(self):
        ids = self.store.list_record_ids("migration_definitions", limit=201)
        items, issues = [], []
        def read(reference):
            try:
                return self.get(reference), None
            except Exception:
                return None, reference
        with ThreadPoolExecutor(max_workers=6) as pool:
            for item, issue in pool.map(read, ids[:200]):
                if item and not item.get("archived"):
                    items.append(item)
                if issue:
                    issues.append(issue)
        return {"items": sorted(items, key=lambda p: p["created_at"], reverse=True), "partial": len(ids) > 200 or bool(issues)}

    def start(self, actor, reference, definition_hash, acknowledged):
        self.authorize(actor)
        value = self.get(reference)
        if value.get("archived"):
            raise WorkflowError("PLAN_ARCHIVED", "This sample migration plan was archived during cleanup. Create a fresh plan.")
        if value["owner"] != actor.casefold() or value["definition_hash"] != definition_hash:
            raise WorkflowError("PLAN_CHANGED", "Review the saved plan with the owning operator.", 403)
        if not acknowledged:
            raise WorkflowError("APPROVAL_REQUIRED", "Approve the reviewed migration plan before starting.", 400)
        existing = self.store.read("migration_requests", reference)
        if not existing:
            report = self.planner.report_loader()
            for saved in value["members"]:
                current = inspect_member(report, saved["asset_key"])
                if not current["supported"] or current["fingerprint"] != saved["fingerprint"] or current["dependencies"] != saved["dependencies"] or current["contexts"] != saved["contexts"]:
                    raise WorkflowError("PLAN_BLOCKED", "An item is blocked or changed. Create a new plan from the current inventory.")
            guarded_runner(self.workspace, self.runner_id, value["scope_root"], actor)
            self.store.write_once("migration_requests", reference, {"definition_id": reference, "definition_hash": definition_hash,
                "actor": actor.casefold(), "requested_at": timestamp(), "runner_job_id": self.runner_id, "acknowledged": True})
        else:
            if existing["definition_hash"] != definition_hash or existing["actor"] != actor.casefold():
                raise WorkflowError("REQUEST_CONFLICT", "This plan already has a different migration request.")
        if not self.store.read("migration_launches", reference):
            run = self.workspace.jobs.run_now(job_id=int(self.runner_id), job_parameters={"execution_id": "plan-" + reference}, idempotency_token="plan-" + reference).response
            self.store.write_once("migration_launches", reference, {"runner_run_id": str(run.run_id)})
        return self.get(reference)


def execute_saved_plan(workspace, store, reference, runner_id):
    value = verified_definition(store.read("migration_definitions", checked_id(reference)), reference)
    request = store.read("migration_requests", reference)
    actor = (workspace.current_user.me().user_name or "").casefold()
    if not request or not request.get("acknowledged") or request.get("definition_hash") != value["definition_hash"] or request.get("runner_job_id") != str(runner_id) or request.get("actor") != actor or value["owner"] != actor:
        raise WorkflowError("INVALID_APPROVAL", "The migration approval, operator, and runner must match.", 403)
    if not store.write_once("migration_started", reference, {"started_at": timestamp()}):
        raise WorkflowError("ALREADY_STARTED", "This saved migration already started. Inspect its recorded run.")
    root = value["scope_root"]
    if not root.startswith("/Workspace/Users/") or "/.bundle/workspace-migration-agent-jr/" not in root or not root.endswith("/files") or "/.." in root:
        raise WorkflowError("INVALID_SCOPE", "Candidate storage is outside the dedicated project folder.")
    started_at = timestamp()
    rows = [{"asset_key": m["asset_key"], "name": m["name"], "state": "QUEUED", "tasks": m["tasks"]} for m in value["members"]]
    lock = threading.RLock()

    def update(index, state, **fields):
        with lock:
            rows[index].update(state=state, **fields)
            store.write_pointer("migration_progress", reference, {"state": "RUNNING", "started_at": started_at, "updated_at": timestamp(), "members": rows})

    def migrate(index):
        member = value["members"][index]
        try:
            if not member["supported"]:
                raise WorkflowError("BLOCKED", "Resolve this item's saved blockers before migration.")
            update(index, "INSPECTING", started_at=timestamp(), message="Checking current sources and dependency versions")
            if member["kind"] == "JOB":
                current = read_source(workspace, member["resource_id"], root, actor, workspace_scope=True)
                if current["source_fingerprint"] != member["fingerprint"]:
                    raise WorkflowError("SOURCE_CHANGED", "Job, notebook, or compute dependencies changed after this plan was saved.")
                if any(f["severity"] in {"BLOCKER", "REVIEW"} for f in current["findings"]):
                    raise WorkflowError("BLOCKED", "Current compatibility findings prevent migration.")
                if (workspace.jobs.get(int(member["resource_id"])).creator_user_name or "").casefold() != actor:
                    raise WorkflowError("OWNER_REQUIRED", "The operator must own the source job.", 403)
                task_configs = current["settings"]["tasks"]
                mapped = sorted({p for t in current["tasks"] for p in t["serverless_dependencies"]})
            else:
                task_configs = [{"task_key": "notebook", "notebook_task": {"notebook_path": member["path"], "base_parameters": {}}, "timeout_seconds": 1200}]
                for context in member["contexts"]:
                    current = read_source(workspace, context["job_id"], root, actor, workspace_scope=True)
                    if current["source_fingerprint"] != context["fingerprint"]:
                        raise WorkflowError("SOURCE_CHANGED", "The notebook's associated job context changed.")
                if member["contexts"]:
                    context = member["contexts"][0]
                    current = read_source(workspace, context["job_id"], root, actor, workspace_scope=True)
                    source_task = next(t for t in current["settings"]["tasks"] if t["task_key"] == context["task_key"])
                    task_configs[0]["notebook_task"]["base_parameters"] = source_task.get("notebook_task", {}).get("base_parameters", {})
                mapped = member["dependencies"]
            if mapped != member["dependencies"]:
                raise WorkflowError("DEPENDENCIES_CHANGED", "The serverless dependency mapping changed.")
            key = candidate_key(member)
            # Reuse an earlier direct-wave candidate for the same job version.
            legacy = None
            if member["kind"] == "JOB":
                legacy_key = hashlib.sha256((member["resource_id"] + ":" + member["fingerprint"]).encode()).hexdigest()[:32]
                legacy = store.read("direct_candidate_results", legacy_key)
            existing = store.read("migration_candidates", key) or legacy
            if existing:
                candidate = workspace.jobs.get(int(existing["candidate_job_id"]))
                settings = candidate.settings
                candidate_tasks = settings.tasks or []
                if len(candidate_tasks) != len(member["tasks"]) or any(t.existing_cluster_id or t.new_cluster or t.job_cluster_key or not t.environment_key or not t.notebook_task for t in candidate_tasks):
                    raise WorkflowError("CANDIDATE_CHANGED", "The existing candidate is no longer the expected serverless configuration.")
                pinned_dependencies = sorted({p for environment in settings.environments or [] for p in (environment.spec.dependencies or [])})
                if pinned_dependencies != mapped:
                    raise WorkflowError("CANDIDATE_CHANGED", "The existing candidate's dependencies changed. Review it before reuse.")
                for task in candidate_tasks:
                    path = task.notebook_task.notebook_path
                    if not path.startswith(root + "/candidates/") and not path.startswith(root + "/migration-candidates/"):
                        raise WorkflowError("CANDIDATE_CHANGED", "The existing candidate references a notebook outside candidate storage.")
                    content = base64.b64decode(workspace.workspace.export(path, format=ExportFormat.SOURCE).content or "", validate=True).decode("utf-8")
                    if member["kind"] == "NOTEBOOK":
                        intact = fingerprint({}, content) == member["fingerprint"]
                    else:
                        original_task = next((t for t in member["tasks"] if t["task_key"] == task.task_key), None)
                        intact = original_task and hashlib.sha256(content.replace("\r\n", "\n").strip().encode()).hexdigest() == original_task.get("source_hash")
                    if not intact:
                        raise WorkflowError("CANDIDATE_CHANGED", "The existing candidate notebook changed. Review it before reuse.")
                update(index, "SUCCEEDED", candidate_job_id=str(candidate.job_id), reused=True, finished_at=timestamp(), message="Existing serverless candidate reused", validation="NOT_RUN")
                return
            if not store.write_once("migration_source_claims", key, {"definition_id": reference, "fingerprint": member["fingerprint"]}):
                claim = store.read("migration_source_claims", key)
                if claim.get("definition_id") != reference:
                    raise WorkflowError("ALREADY_REQUESTED", "Another migration already claimed this source version. Inspect its run.")
            update(index, "COPYING", message="Copying notebooks to the serverless candidate folder")
            tasks = []
            for task in task_configs:
                if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", task["task_key"]):
                    raise WorkflowError("INVALID_TASK_KEY", "The task key cannot be used as a candidate destination.")
                path = task["notebook_task"]["notebook_path"]
                exported = workspace.workspace.export(path, format=ExportFormat.SOURCE)
                content = base64.b64decode(exported.content or "", validate=True).decode("utf-8")
                if member["kind"] == "NOTEBOOK" and fingerprint({}, content) != member["fingerprint"]:
                    raise WorkflowError("SOURCE_CHANGED", "The notebook source changed after inspection.")
                if member["kind"] == "JOB":
                    pinned = next(t for t in current["tasks"] if t["task_key"] == task["task_key"])
                    if hashlib.sha256(content.replace("\r\n", "\n").strip().encode()).hexdigest() != pinned["source_hash"]:
                        raise WorkflowError("SOURCE_CHANGED", "A notebook changed during migration.")
                info = workspace.workspace.get_status(path)
                if not info.language or info.language.value not in {"PYTHON", "SQL"}:
                    raise WorkflowError("UNSUPPORTED_LANGUAGE", "Only Python and SQL notebooks are supported.")
                if member["kind"] == "NOTEBOOK" and info.language.value != member["tasks"][0]["language"]:
                    raise WorkflowError("SOURCE_CHANGED", "The notebook language changed after inspection.")
                target = root + "/migration-candidates/" + key + "/" + task["task_key"]
                workspace.workspace.mkdirs(target.rsplit("/", 1)[0])
                workspace.workspace.import_(target, content=base64.b64encode(content.encode()).decode(), format=ImportFormat.SOURCE, language=Language(info.language.value), overwrite=True)
                safe = {k: task[k] for k in ("task_key", "depends_on", "run_if", "timeout_seconds", "max_retries", "min_retry_interval_millis", "retry_on_timeout") if k in task}
                safe.update(notebook_task={**task["notebook_task"], "notebook_path": target, "source": "WORKSPACE"}, environment_key="migration_serverless")
                tasks.append(Task.from_dict(safe))
            update(index, "CREATING", message="Creating serverless job and attaching mapped dependencies")
            name = "migration-" + re.sub(r"[^A-Za-z0-9_-]", "-", member["name"])[:60] + "-" + key[:8]
            matches = [j for j in workspace.jobs.list(name=name) if (j.settings.tags or {}).get("MigrationSource") == key]
            if len(matches) > 1:
                raise WorkflowError("DUPLICATE_CANDIDATES", "More than one matching candidate exists. Review them before continuing.")
            if matches:
                job_id = matches[0].job_id
            else:
                environment = JobEnvironment.from_dict({"environment_key": "migration_serverless", "spec": {"environment_version": "4", "dependencies": mapped}})
                job_id = workspace.jobs.create(name=name, tasks=tasks, environments=[environment], max_concurrent_runs=1, timeout_seconds=2700,
                    description="Serverless migration candidate. Workload execution and output validation have not been performed.",
                    tags={"Project": "serverless-migration-challenge-jr", "Purpose": "migration-center-candidate", "MigrationSource": key, "MigrationPlan": reference}).job_id
            record = {"candidate_job_id": str(job_id), "asset_key": member["asset_key"], "fingerprint": member["fingerprint"], "definition_id": reference, "created_at": timestamp(), "validation": "NOT_RUN"}
            store.write_once("migration_candidates", key, record)
            update(index, "SUCCEEDED", candidate_job_id=str(job_id), finished_at=timestamp(), message="Serverless candidate created", validation="NOT_RUN")
        except Exception as exc:
            update(index, "FAILED", finished_at=timestamp(), message=exc.message if isinstance(exc, WorkflowError) else "Candidate creation failed. Inspect the migration runner logs.")

    with ThreadPoolExecutor(max_workers=min(4, len(rows))) as pool:
        list(pool.map(migrate, range(len(rows))))
    result = {"state": "SUCCEEDED" if all(r["state"] == "SUCCEEDED" for r in rows) else "FAILED", "started_at": started_at, "finished_at": timestamp(), "members": rows}
    store.write_once("migration_results", reference, result)
    return result

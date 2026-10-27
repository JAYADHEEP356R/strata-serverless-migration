"""Read live project sources, propose an isolated candidate, and record decisions.

This module deliberately has no job create/update/run or workspace write methods.
Approval is a point-in-time decision; execution must recheck source and plan hashes.
"""
import base64
import hashlib
import hmac
import json
import posixpath
import re
import uuid
from dataclasses import asdict
from datetime import datetime, timezone

from databricks.sdk.service.workspace import ExportFormat
from .assessment import assess
from .plan_store import ID
from .experiment_profile import PROFILE as EXPERIMENT_PROFILE, source_supported as experiment_supported, dependencies as translate_dependencies, parameters as experiment_parameters
from .proof_profile import BATCH_PROFILE_ID, batch_source_supported

PROJECT = "serverless-migration-challenge-jr"
PLANNER_VERSION = "2026-10-26.3"
TARGET_ENVIRONMENT = "4"


class WorkflowError(Exception):
    def __init__(self, code, message, status=409):
        self.code, self.message, self.status = code, message, status
        super().__init__(message)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()).hexdigest()


def timestamp():
    return datetime.now(timezone.utc).isoformat()


def inside(path, root):
    return isinstance(path, str) and posixpath.normpath(path) == path and path.startswith(root.rstrip("/") + "/")


def capture_cluster_evidence(workspace, cluster_id, owner, job_id, workspace_scope=False):
    """Read with the operator's existing rights; persist facts, never credentials."""
    cluster = workspace.clusters.get(cluster_id).as_dict()
    if not workspace_scope and (cluster.get("custom_tags", {}).get("Project") != "serverless-migration-challenge" or cluster.get("cluster_name") != "serverless-migration-classic-baseline" or (cluster.get("creator_user_name") or "").casefold() != owner.casefold()):
        raise WorkflowError("OUTSIDE_PROJECT", "The source references a cluster outside the dedicated baseline.", 403)
    proof_keys = ("spark_version", "node_type_id", "num_workers", "runtime_engine", "data_security_mode", "spark_conf", "aws_attributes", "init_scripts", "cluster_log_conf", "docker_image", "ssh_public_keys", "policy_id", "single_user_name")
    config = {k: cluster[k] for k in proof_keys if k in cluster}
    libraries = workspace.libraries.cluster_status(cluster_id)
    libraries = getattr(libraries, "library_statuses", libraries) or []
    config["libraries"] = [item.library.as_dict() for item in libraries if item.library]
    safe_keys = ("spark_version", "node_type_id", "num_workers", "runtime_engine", "data_security_mode")
    return {"schema_version": 1, "project": PROJECT, "job_id": str(job_id), "cluster_id": cluster_id,
            "captured_by": owner, "captured_at": timestamp(), "configuration_fingerprint": digest(config),
            "safe_configuration": {k: config[k] for k in safe_keys if k in config},
            "findings": [asdict(f) for f in assess(config, "").findings], "has_libraries": bool(config["libraries"]),
            "libraries": config["libraries"], "extra_spark_configuration": bool(set(config.get("spark_conf", {})) - {"spark.master", "spark.databricks.cluster.profile"})}


def read_source(workspace, job_id, project_root, owner, cluster_evidence=None, workspace_scope=False):
    if not re.fullmatch(r"[0-9]+", str(job_id)):
        raise WorkflowError("INVALID_JOB", "A project job ID is required.", 400)
    if not project_root.startswith("/Workspace/Users/") or "/.bundle/workspace-migration-agent-jr/" not in project_root or not project_root.endswith("/files"):
        raise WorkflowError("INVALID_SCOPE", "The configured project source root is invalid.", 403)
    job = workspace.jobs.get(int(job_id))
    settings = job.settings.as_dict()
    if not workspace_scope and (settings.get("tags", {}).get("Project") != PROJECT or settings.get("tags", {}).get("Purpose") != "isolated-smoke-test"):
        raise WorkflowError("OUTSIDE_PROJECT", "Planning is restricted to the project demo workload.", 403)
    if not workspace_scope and (job.creator_user_name or "").casefold() != owner.casefold():
        raise WorkflowError("OWNER_MISMATCH", "The project owner does not match the source job creator.", 403)
    if not workspace_scope and any(settings.get(k) for k in ("schedule", "trigger", "continuous")):
        raise WorkflowError("AUTOMATIC_TRIGGER", "This milestone supports manually triggered project jobs only.")
    tasks = settings.get("tasks", [])
    if not tasks or len(tasks) > 100:
        raise WorkflowError("TASK_LIMIT", "The source task list is empty or exceeds this planner's inspection limit.")
    notebooks, clusters = {}, {}
    findings, task_views, proof_tasks = [], [], []
    classic_count = 0
    for index, task in enumerate(tasks):
        key = task.get("task_key", "")
        notebook_task = task.get("notebook_task")
        if not notebook_task or notebook_task.get("source", "WORKSPACE") != "WORKSPACE":
            raise WorkflowError("UNSUPPORTED_TASK", "Only workspace notebook tasks are supported by this planning milestone.")
        path = notebook_task.get("notebook_path")
        valid_workspace_path = isinstance(path, str) and path.startswith("/") and posixpath.normpath(path) == path
        if (not valid_workspace_path if workspace_scope else not inside(path, project_root)) or "/assessment_fixtures/" in path:
            raise WorkflowError("OUTSIDE_PROJECT", "A notebook is outside the approved project scope or is a test fixture.", 403)
        if path not in notebooks:
            info = workspace.workspace.get_status(path)
            language = info.language.value if info.language else "UNKNOWN"
            exported = workspace.workspace.export(path, format=ExportFormat.SOURCE)
            source = base64.b64decode(exported.content or "", validate=True).decode("utf-8")
            if not source.strip():
                raise WorkflowError("EMPTY_SOURCE", "A notebook has no readable source; readiness is unknown.")
            notebooks[path] = {"path": path, "language": language, "sha256": hashlib.sha256(source.encode()).hexdigest(), "source": source}
        notebook = notebooks[path]
        classic = bool(task.get("existing_cluster_id") or task.get("new_cluster") or task.get("job_cluster_key"))
        classic_count += int(classic)
        config = {"source_managed": bool(settings.get("git_source") or settings.get("deployment")), "target_environment_version": int(TARGET_ENVIRONMENT)}
        if task.get("existing_cluster_id"):
            cluster_id = task["existing_cluster_id"]
            if cluster_id not in clusters:
                evidence = cluster_evidence(cluster_id) if cluster_evidence else capture_cluster_evidence(workspace, cluster_id, owner, job_id, workspace_scope=workspace_scope)
                if evidence.get("project") != PROJECT or evidence.get("job_id") != str(job_id) or evidence.get("cluster_id") != cluster_id or (evidence.get("captured_by") or "").casefold() != owner.casefold():
                    raise WorkflowError("INVALID_CLUSTER_EVIDENCE", "The classic configuration evidence does not match this project.", 403)
                if not re.fullmatch(r"[a-f0-9]{64}", evidence.get("configuration_fingerprint", "")):
                    raise WorkflowError("INVALID_CLUSTER_EVIDENCE", "Classic configuration evidence has no valid fingerprint.")
                clusters[cluster_id] = evidence
            evidence = clusters[cluster_id]
            config.update(evidence["safe_configuration"])
            config["libraries"] = evidence.get("libraries", [])
            for finding in evidence.get("findings", []):
                findings.append({"task_key": key, **finding})
            if evidence.get("has_libraries") and "libraries" not in evidence:
                config["classic_libraries_present"] = True
            if evidence.get("extra_spark_configuration"):
                config["extra_classic_configuration"] = True
        elif task.get("new_cluster"):
            config.update(task["new_cluster"])
        elif task.get("job_cluster_key"):
            defined = next((c.get("new_cluster") for c in settings.get("job_clusters", []) if c.get("job_cluster_key") == task["job_cluster_key"]), None)
            if not defined:
                raise WorkflowError("MISSING_CLUSTER", "A referenced job cluster cannot be resolved.")
            config.update(defined)
        config["libraries"] = config.get("libraries", []) + task.get("libraries", [])
        result = assess(config, notebook["source"], notebook["language"])
        for finding in result.findings:
            findings.append({"task_key": key, **asdict(finding)})
        mapped = translate_dependencies(config.get("libraries", []))
        if classic and (config.get("libraries") or config.get("classic_libraries_present")):
            resolved = mapped is not None and not config.get("classic_libraries_present")
            findings.append({"task_key": key, "limitation": "L5", "rule": "dependency-translation", "severity": "WARNING" if resolved else "REVIEW", "location": "configuration", "evidence": "Pinned PyPI dependencies mapped: " + ", ".join(mapped) if resolved else "Classic libraries require an explicit serverless environment mapping.", "recommendation": "Validate mapped dependencies on serverless before promotion." if resolved else "Review and translate dependencies before approving this plan."})
        extra_conf = set(config.get("spark_conf", {})) - {"spark.master", "spark.databricks.cluster.profile"}
        if classic and (extra_conf or config.get("extra_classic_configuration")):
            findings.append({"task_key": key, "limitation": "L4", "rule": "spark-configuration-review", "severity": "REVIEW", "location": "configuration", "evidence": "Additional classic Spark configuration is present.", "recommendation": "Review session configuration and compatibility before migration."})
        task_views.append({"task_key": key, "index": index, "serverless_dependencies": mapped if mapped is not None else [], "source_hash": hashlib.sha256(notebook["source"].replace("\r\n", "\n").strip().encode()).hexdigest(), "source_notebook": path, "language": notebook["language"], "source_compute": "CLASSIC" if classic else "SERVERLESS", "convert_compute": classic, "depends_on": [d["task_key"] for d in task.get("depends_on", [])], "run_if": task.get("run_if", "ALL_SUCCESS")})
        proof_tasks.append({"key": key, "source_hash": notebook["sha256"], "language": notebook["language"]})
    if classic_count < 1:
        raise WorkflowError("NO_CLASSIC_TASKS", "This job has no classic tasks to migrate.")
    if classic_count > 10:
        raise WorkflowError("PLANNER_TASK_LIMIT", "This planner supports up to ten classic tasks per plan.")
    keys = {t["task_key"] for t in task_views}
    if len(keys) != len(task_views) or any(d not in keys for t in task_views for d in t["depends_on"]):
        raise WorkflowError("INVALID_DEPENDENCIES", "Task dependencies are incomplete or ambiguous.")
    pending = {t["task_key"]: set(t["depends_on"]) for t in task_views}
    while pending:
        ready = [key for key, deps in pending.items() if not deps]
        if not ready:
            raise WorkflowError("CYCLIC_DEPENDENCIES", "The task graph contains a cycle.")
        for key in ready:
            pending.pop(key)
        for deps in pending.values():
            deps.difference_update(ready)
    proof_clusters = {key: {k: v for k, v in value.items() if k != "captured_at"} for key, value in clusters.items()}
    proof = {"planner_version": PLANNER_VERSION, "job_id": str(job_id), "settings": settings, "notebooks": proof_tasks, "clusters": proof_clusters}
    unique = {(f["task_key"], f["rule"], f["severity"]): f for f in findings}
    return {"job_id": str(job_id), "name": settings.get("name", "Project workload"), "source_fingerprint": digest(proof), "settings": settings, "tasks": task_views, "findings": list(unique.values()), "classic_task_count": classic_count,
            "source_checks": {"job_configuration": "LIVE", "notebook_source": "LIVE", "classic_configuration": "PUBLISHED_EVIDENCE" if cluster_evidence else "LIVE", "cluster_evidence": [{k: value[k] for k in ("cluster_id", "captured_at", "configuration_fingerprint", "libraries") if k in value} for value in clusters.values()]}}


def make_plan(source, report, actor, plan_id):
    blocked = any(f["severity"] in {"BLOCKER", "REVIEW"} for f in source["findings"])
    environment = "migration_target_v1"
    existing = {e["environment_key"] for e in source["settings"].get("environments", [])}
    while environment in existing:
        environment += "_next"
    candidate_root = report["scope_root"] + "/candidates/" + plan_id
    changes, tasks = [], []
    for task in source["tasks"]:
        candidate_path = candidate_root + "/notebooks/" + task["source_notebook"].rsplit("/", 1)[-1] + "_" + hashlib.sha256(task["source_notebook"].encode()).hexdigest()[:8]
        tasks.append({**task, "target_notebook": candidate_path, "target_compute": "SERVERLESS" if task["convert_compute"] else task["source_compute"]})
        changes.append({"task_key": task["task_key"], "field": "notebook_task.notebook_path", "before": task["source_notebook"], "after": candidate_path, "reason": "Copy the inspected source into a separate candidate folder before any execution."})
        if task["convert_compute"]:
            changes.append({"task_key": task["task_key"], "field": "compute", "before": "Classic compute", "after": f"Serverless environment {TARGET_ENVIRONMENT} ({environment})", "reason": "Remove only the candidate task's cluster assignment; preserve the original workload."})
    acknowledgements = [
        {"id": "candidate_isolation", "label": "I reviewed the candidate-only changes. The original job and notebooks must remain unchanged."},
        {"id": "validation_required", "label": "I understand approval records this plan only. It does not run compute, migrate, or promote anything."},
        {"id": "source_reconciliation", "label": "I reviewed source ownership. Asset Bundle reconciliation and output validation are required before promotion."},
    ]
    if source["source_checks"]["classic_configuration"] == "PUBLISHED_EVIDENCE":
        acknowledgements.append({"id": "classic_preflight", "label": "Classic configuration uses timestamped operator evidence. A fresh cluster and library check is mandatory before any future execution."})
    if source["source_checks"]["job_configuration"] == "PUBLISHED_EVIDENCE":
        acknowledgements.append({"id": "workspace_preflight", "label": "This plan uses the published workspace scan. Live job, notebook, permissions, dependencies, and output destinations must be rechecked before any execution."})
    document = {
        "schema_version": 1, "plan_id": plan_id, "planner_version": PLANNER_VERSION, "created_at": timestamp(), "created_by": actor,
        "scan_id": report["scan_id"], "asset_key": "JOB:" + source["job_id"], "source_job_id": source["job_id"], "source_name": source["name"],
        "source_fingerprint": source["source_fingerprint"], "scope_root": report["scope_root"], "project_owner": report["actor"],
        "initial_status": "BLOCKED" if blocked else "DRAFT", "findings": source["findings"], "changes": changes, "tasks": tasks,
        "target": {"name": "migration-candidate-" + plan_id[:12], "compute": "SERVERLESS", "environment_key": environment, "environment_version": TARGET_ENVIRONMENT, "candidate_root": candidate_root, "automatic_triggers": False, "max_concurrent_runs": 1, "timeout_seconds": 1200, "notifications": "DISABLED"},
        "required_acknowledgements": acknowledgements,
        "validation_checks": [
            {"id": "fingerprints", "title": "Recheck source and plan fingerprints immediately before execution", "status": "REQUIRED"},
            {"id": "classic_preflight", "title": "Inspect live classic configuration and libraries against the recorded baseline fingerprint before execution", "status": "REQUIRED"},
            {"id": "baseline", "title": "Establish a classic baseline tied to these exact sources", "status": "REQUIRED"},
            {"id": "schema", "title": "Compare output column types and order", "status": "REQUIRED"},
            {"id": "rows", "title": "Compare row counts and bidirectional duplicate-sensitive EXCEPT ALL", "status": "REQUIRED"},
            {"id": "assertions", "title": "Define workload-specific assertions, output destinations, tolerances, and side-effect isolation before running a candidate", "status": "REQUIRED"},
            {"id": "promotion", "title": "Require a separate promotion decision after validation and source reconciliation", "status": "REQUIRED"},
        ],
        "recovery": "Keep the original job, notebooks, triggers, and outputs unchanged. A future candidate failure must block promotion; data rollback is not implied.",
        "execution_enabled": False, "approval_role": "PROJECT_OPERATOR",
        "source_checks": source["source_checks"],
    }
    if batch_source_supported(source, report["scope_root"]):
        document["execution_profile"] = BATCH_PROFILE_ID
    if experiment_supported(source):
        document["execution_profile"] = EXPERIMENT_PROFILE
        document["experiment"] = experiment_parameters(source)
        document["target"]["dependencies"] = sorted({p for t in tasks for p in t.get("serverless_dependencies", [])})
        for task in tasks:
            changes.append({"task_key": task["task_key"], "field": "libraries", "before": "Classic cluster/task dependencies", "after": ", ".join(task.get("serverless_dependencies", [])) or "Runtime built-ins", "reason": "Install reviewed pinned PyPI requirements in the candidate serverless environment."})
    document["plan_hash"] = digest(document)
    return document


def verified_plan(document):
    if not isinstance(document, dict) or not ID.fullmatch(document.get("plan_id", "")):
        raise WorkflowError("CORRUPT_PLAN", "This plan record is invalid.")
    expected = document.get("plan_hash", "")
    actual = digest({k: v for k, v in document.items() if k != "plan_hash"})
    if not isinstance(expected, str) or not hmac.compare_digest(expected, actual):
        raise WorkflowError("CORRUPT_PLAN", "The stored plan content no longer matches its digest.")
    return document


class PlanningService:
    def __init__(self, workspace, store, report_loader, allowed_job_id, cluster_evidence=None, workspace_planning=False):
        self.workspace, self.store, self.report_loader = workspace, store, report_loader
        self.allowed_job_id = str(allowed_job_id)
        self.cluster_evidence = cluster_evidence
        self.workspace_planning = workspace_planning

    def policy(self, actor):
        report = self.report_loader()
        owner = report.get("actor", "")
        permitted = bool(actor and owner and actor.casefold() == owner.casefold())
        return {"actor": actor or None, "role": "PROJECT_OPERATOR" if permitted else "VIEWER", "can_generate": permitted, "can_decide": permitted, "execution_enabled": False, "approval_policy": "The signed-in project operator may record a plan decision; this is not a workspace-admin certification."}

    def source(self, report, job_id=None):
        job_id = str(job_id or self.allowed_job_id)
        asset = next((a for a in report["assets"] if a["asset_key"] == "JOB:" + job_id and not a.get("is_fixture")), None)
        if not asset:
            raise WorkflowError("OUTSIDE_PROJECT", "The configured project job is not present in the published inventory.", 403)
        if self.workspace_planning and report.get("scope") == "workspace":
            root = report.get("scope_root", "")
            if not root.startswith("/Workspace/Users/") or "/.bundle/workspace-migration-agent-jr/" not in root or not root.endswith("/files") or posixpath.normpath(root) != root:
                raise WorkflowError("INVALID_SCOPE", "Candidate storage must stay inside the dedicated project folder.", 403)
            source = report.get("planning_sources", {}).get(job_id)
            if not source:
                raise WorkflowError("UNSUPPORTED_WORKLOAD", asset.get("planning_reason", "This workload needs a supported, complete source inspection before planning."))
            if source.get("job_id") != job_id or not re.fullmatch(r"[a-f0-9]{64}", source.get("source_fingerprint", "")) or source.get("source_checks", {}).get("job_configuration") != "PUBLISHED_EVIDENCE":
                raise WorkflowError("INVALID_SOURCE_EVIDENCE", "The published workload evidence is invalid.")
            return source
        if job_id != self.allowed_job_id:
            raise WorkflowError("OUTSIDE_PROJECT", "Select a workload in the published workspace scan.", 403)
        return read_source(self.workspace, job_id, report["scope_root"], report["actor"], self.cluster_evidence)

    def generate(self, actor, asset_key, scan_id, request_id):
        if not self.policy(actor)["can_generate"]:
            raise WorkflowError("NOT_AUTHORIZED", "Only the signed-in project operator can create plans.", 403)
        report = self.report_loader()
        if not isinstance(asset_key, str) or not re.fullmatch(r"JOB:[0-9]+", asset_key):
            raise WorkflowError("INVALID_JOB", "Select a discovered job.", 400)
        if not self.workspace_planning and asset_key != "JOB:" + self.allowed_job_id:
            raise WorkflowError("OUTSIDE_PROJECT", "Select the approved project demo job.", 403)
        if scan_id != report["scan_id"]:
            raise WorkflowError("STALE_INVENTORY", "The inventory changed. Refresh the assessment before generating a plan.")
        try:
            request_uuid = uuid.UUID(request_id)
        except (ValueError, AttributeError):
            raise WorkflowError("INVALID_REQUEST_ID", "A UUID idempotency key is required.", 400) from None
        plan_id = uuid.uuid5(uuid.NAMESPACE_URL, actor.casefold() + ":" + asset_key + ":" + str(request_uuid)).hex
        existing = self.store.read("plans", plan_id)
        if existing:
            return self.get(plan_id)
        source = self.source(report, asset_key.split(":", 1)[1])
        document = make_plan(source, report, actor, plan_id)
        self.store.write_once("plans", plan_id, document)
        return self.get(plan_id, source=source)

    def get(self, plan_id, check_source=True, source=None):
        if not ID.fullmatch(plan_id):
            raise WorkflowError("INVALID_PLAN_ID", "Invalid plan reference.", 400)
        document = self.store.read("plans", plan_id)
        if document is None:
            raise WorkflowError("PLAN_NOT_FOUND", "The plan was not found.", 404)
        document = verified_plan(document)
        if (not self.workspace_planning and document.get("source_job_id") != self.allowed_job_id) or not re.fullmatch(r"[0-9]+", str(document.get("source_job_id", ""))) or document.get("asset_key") != "JOB:" + document["source_job_id"]:
            raise WorkflowError("OUTSIDE_PROJECT", "The saved plan is outside the approved workload scope.", 403)
        decision = self.store.read("decisions", plan_id)
        if decision and (decision.get("plan_hash") != document["plan_hash"] or decision.get("source_fingerprint") != document["source_fingerprint"] or decision.get("decision") not in {"APPROVED", "REJECTED"}):
            raise WorkflowError("CORRUPT_DECISION", "The decision does not match this plan.")
        status = decision["decision"] if decision else document["initial_status"]
        freshness = "NOT_CHECKED"
        if check_source:
            report = self.report_loader()
            try:
                source = source or self.source(report, document["source_job_id"])
            except WorkflowError as exc:
                if not self.workspace_planning or exc.code not in {"OUTSIDE_PROJECT", "UNSUPPORTED_WORKLOAD"}:
                    raise
                source = None
            freshness = "CURRENT" if source and source["source_fingerprint"] == document["source_fingerprint"] and document["planner_version"] == PLANNER_VERSION and report.get("scope_root") == document["scope_root"] and report.get("actor") == document["project_owner"] else "STALE"
            if freshness == "STALE" and status != "REJECTED":
                status = "STALE"
        return {**document, "status": status, "freshness": freshness, "decision": decision, "checked_at": timestamp() if check_source else None}

    def list(self):
        plans = [self.get(plan_id, check_source=False) for plan_id in self.store.list_ids()]
        return sorted(plans, key=lambda p: p["created_at"], reverse=True)

    def decide(self, actor, plan_id, decision, expected_hash, expected_source, acknowledgements, reason=""):
        if not self.policy(actor)["can_decide"]:
            raise WorkflowError("NOT_AUTHORIZED", "Only the signed-in project operator can record this decision.", 403)
        if decision not in {"APPROVED", "REJECTED"}:
            raise WorkflowError("INVALID_DECISION", "Choose approve or reject.", 400)
        # Read freshly, without the snapshot/decision list cache, at the decision boundary.
        plan = self.get(plan_id, check_source=decision == "APPROVED")
        if expected_hash != plan["plan_hash"] or expected_source != plan["source_fingerprint"]:
            raise WorkflowError("PLAN_CHANGED", "The reviewed plan version does not match. Reopen the plan.")
        if plan["decision"]:
            raise WorkflowError("ALREADY_DECIDED", "A final decision already exists for this plan version.")
        if decision == "APPROVED":
            if plan["freshness"] != "CURRENT":
                raise WorkflowError("STALE_PLAN", "The source changed after planning. Generate and review a new plan.")
            if plan["initial_status"] != "DRAFT":
                raise WorkflowError("PLAN_BLOCKED", "Unresolved blockers or review findings prevent approval.")
            required = {item["id"] for item in plan["required_acknowledgements"]}
            if set(acknowledgements) != required:
                raise WorkflowError("ACKNOWLEDGEMENTS_REQUIRED", "Review and acknowledge every required condition.", 400)
        if decision == "REJECTED" and not 5 <= len(reason.strip()) <= 1000:
            raise WorkflowError("REASON_REQUIRED", "Provide a rejection reason of 5–1,000 characters.", 400)
        record = {"schema_version": 1, "plan_id": plan_id, "plan_hash": plan["plan_hash"], "source_fingerprint": plan["source_fingerprint"], "decision": decision, "actor": actor, "role": "PROJECT_OPERATOR", "decided_at": timestamp(), "acknowledgements": sorted(set(acknowledgements)), "reason": reason.strip(), "execution_enabled": False}
        if not self.store.write_once("decisions", plan_id, record):
            raise WorkflowError("ALREADY_DECIDED", "Another decision was recorded first. Refresh this plan.")
        return self.get(plan_id, check_source=False)

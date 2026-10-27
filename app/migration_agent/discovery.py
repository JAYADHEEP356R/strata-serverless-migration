"""Read-only inventory and assessment; no workspace mutation methods are used."""
import base64
import hashlib
from dataclasses import asdict
from datetime import datetime, timezone

from databricks.sdk.service.workspace import ExportFormat, ObjectType
from .assessment import assess

PROJECT = "serverless-migration-challenge-jr"
BASELINE_PROJECT = "serverless-migration-challenge"
RULE_VERSION = "2026-10-24.2"


def within(path, root):
    return path == root or path.startswith(root.rstrip("/") + "/")


def collect(w, project_root, scope="project", max_assets=2000):
    if scope not in {"project", "workspace"}:
        raise ValueError("Unsupported scope")
    if not project_root.startswith("/Workspace/Users/") or "/.bundle/workspace-migration-agent-jr/" not in project_root:
        raise PermissionError("Invalid dedicated project root")
    user = w.current_user.me().user_name
    assets, relationships, errors = {}, [], []
    clusters, notebooks = {}, {}
    discovered_paths = set()
    truncated = False

    def problem(kind, resource, exc):
        errors.append({"operation": kind, "resource": str(resource), "error_type": type(exc).__name__, "message": "Resource could not be inspected; assessment coverage is incomplete."})

    def put(kind, resource_id, name, scan_config=None, source=None, language="PYTHON", **metadata):
        nonlocal truncated
        key = f"{kind}:{resource_id}"
        if key not in assets and len(assets) >= max_assets:
            truncated = True
            return key
        assessment = assess(scan_config or {}, source or "", language)
        coverage = "COMPLETE" if source is not None or kind == "CLUSTER" else "METADATA_ONLY"
        assets[key] = {"asset_key": key, "asset_type": kind, "resource_id": str(resource_id), "name": name,
                       "readiness_score": assessment.readiness_score if coverage == "COMPLETE" else None,
                       "eligible": assessment.eligible and coverage == "COMPLETE", "assessment_coverage": coverage,
                       "findings": [asdict(f) for f in assessment.findings], "source_fingerprint": assessment.source_fingerprint,
                       "migration_status": "ASSESSED" if coverage == "COMPLETE" else "DISCOVERED", "language": language, **metadata}
        return key

    def cluster(cluster_id):
        if cluster_id in clusters:
            return clusters[cluster_id]
        try:
            info = w.clusters.get(cluster_id)
            raw = info.as_dict()
            config = {k: raw[k] for k in ("spark_version", "docker_image", "aws_attributes", "init_scripts", "cluster_log_conf", "ssh_public_keys") if k in raw}
            try:
                statuses = w.libraries.cluster_status(cluster_id)
                # Older SDKs return a response object; newer SDKs return an iterator.
                statuses = getattr(statuses, "library_statuses", statuses) or []
                config["libraries"] = [x.library.as_dict() for x in statuses if x.library]
            except Exception as exc:
                problem("cluster-libraries", cluster_id, exc)
                config["inspection_incomplete"] = True
            # Persist only safe configuration facts, not environment values or secret-bearing JSON.
            metadata = {k: raw.get(k) for k in ("spark_version", "node_type_id", "num_workers", "runtime_engine", "data_security_mode")}
            key = put("CLUSTER", cluster_id, info.cluster_name, config, owner=info.creator_user_name,
                      compute_kind="CLASSIC", configuration=metadata, is_fixture=False)
            if config.get("inspection_incomplete") and key in assets:
                assets[key].update(assessment_coverage="PARTIAL", eligible=False, readiness_score=None)
            clusters[cluster_id] = (key, config)
            return key, config
        except Exception as exc:
            problem("cluster", cluster_id, exc)
            return None, {"inspection_incomplete": True}

    def notebook(path, config=None):
        if path in notebooks:
            return notebooks[path]
        if scope == "project" and not within(path, project_root):
            problem("out-of-scope-notebook", path, PermissionError())
            return None
        try:
            info = w.workspace.get_status(path)
            export = w.workspace.export(path, format=ExportFormat.SOURCE)
            source = base64.b64decode(export.content or "").decode("utf-8")
            language = info.language.value if info.language else "PYTHON"
            key = put("NOTEBOOK", info.object_id or path, path.rsplit("/", 1)[-1], config, source, language,
                      path=path, compute_kind="DEPENDENT", is_fixture="/assessment_fixtures/" in path)
            notebooks[path] = key
            return key
        except Exception as exc:
            problem("notebook-export", path, exc)
            key = put("NOTEBOOK", path, path.rsplit("/", 1)[-1], path=path, language="UNKNOWN", compute_kind="UNKNOWN", is_fixture=False)
            notebooks[path] = key
            return key

    try:
        visible_jobs = w.jobs.list(expand_tasks=True)
        for visible in visible_jobs:
            if truncated:
                break
            settings = visible.settings
            tags = settings.tags or {} if settings else {}
            # The scanner itself is a control-plane resource, not a migration target.
            if tags.get("Purpose") in {"discovery-assessment", "demo-migration-execution", "demo-promotion", "validated-candidate-runs"}:
                continue
            if scope == "project" and tags.get("Project") != PROJECT:
                continue
            try:
                job = w.jobs.get(visible.job_id)
                settings = job.settings
                tasks = settings.tasks or []
                managed = bool(settings.git_source or settings.deployment)
                classic_count = sum(bool(t.existing_cluster_id or t.new_cluster or t.job_cluster_key) for t in tasks)
                compute = "MIXED" if 0 < classic_count < len(tasks) else "CLASSIC" if classic_count else "SERVERLESS_OR_OTHER"
                job_key = put("JOB", job.job_id, settings.name, {"source_managed": managed, "classic_task_count": classic_count}, "",
                              owner=job.creator_user_name, compute_kind=compute, task_count=len(tasks), is_fixture=False,
                              trigger_present=bool(settings.schedule or settings.trigger or settings.continuous))
                if job_key in assets:
                    assets[job_key]["is_demo"] = tags.get("Project") == PROJECT and tags.get("Purpose") == "isolated-smoke-test"
                job_clusters = {x.job_cluster_key: x.new_cluster.as_dict() for x in settings.job_clusters or []}
                for task in tasks:
                    task_config = {"source_managed": managed, "libraries": [x.as_dict() for x in task.libraries or []]}
                    if task.existing_cluster_id:
                        cluster_key, cluster_config = cluster(task.existing_cluster_id)
                        task_config.update(cluster_config)
                        if cluster_key:
                            relationships.append({"from_key": job_key, "to_key": cluster_key, "relation": "USES_CLUSTER", "task_key": task.task_key})
                    elif task.new_cluster:
                        task_config.update(task.new_cluster.as_dict())
                    elif task.job_cluster_key:
                        task_config.update(job_clusters.get(task.job_cluster_key, {}))
                    task_key = f"TASK:{job.job_id}/{task.task_key}"
                    source, language = None, "PYTHON"
                    notebook_key = None
                    if task.notebook_task:
                        path = task.notebook_task.notebook_path
                        notebook_key = notebook(path)
                        if notebook_key in assets and assets[notebook_key]["assessment_coverage"] == "COMPLETE":
                            # Source is used in memory only. Task context is assessed independently.
                            export = w.workspace.export(path, format=ExportFormat.SOURCE)
                            source = base64.b64decode(export.content or "").decode("utf-8")
                            language = assets[notebook_key]["language"]
                        discovered_paths.add(path)
                    task_type = next((name for name in task.as_dict() if name.endswith("_task")), "UNKNOWN")
                    put("TASK", f"{job.job_id}/{task.task_key}", task.task_key, task_config, source, language,
                        compute_kind="CLASSIC" if task.existing_cluster_id or task.new_cluster or task.job_cluster_key else "SERVERLESS_OR_OTHER",
                        job_id=str(job.job_id), task_type=task_type, is_fixture=False)
                    if task_config.get("inspection_incomplete") and task_key in assets:
                        assets[task_key].update(assessment_coverage="PARTIAL", eligible=False, readiness_score=None)
                    relationships.append({"from_key": job_key, "to_key": task_key, "relation": "HAS_TASK", "task_key": task.task_key})
                    if notebook_key:
                        relationships.append({"from_key": task_key, "to_key": notebook_key, "relation": "RUNS_NOTEBOOK", "task_key": task.task_key})
                task_assets = [assets.get(f"TASK:{job.job_id}/{t.task_key}") for t in tasks]
                task_assets = [x for x in task_assets if x]
                if task_assets and job_key in assets:
                    assets[job_key]["eligible"] = assets[job_key]["eligible"] and all(x["eligible"] for x in task_assets)
                    scores = [x["readiness_score"] for x in task_assets if x["readiness_score"] is not None]
                    if scores:
                        assets[job_key]["readiness_score"] = min([assets[job_key]["readiness_score"]] + scores)
                    if any(x["assessment_coverage"] != "COMPLETE" for x in task_assets):
                        assets[job_key]["assessment_coverage"] = "PARTIAL"
                        assets[job_key]["readiness_score"] = None
            except Exception as exc:
                problem("job", visible.job_id, exc)
                missing_key = f"JOB:{visible.job_id}"
                if missing_key not in assets:
                    put("JOB", visible.job_id, getattr(settings, "name", None) or "Unreadable job", compute_kind="UNKNOWN", is_fixture=False, planning_supported=False, planning_reason="Job details could not be inspected with the scanner's permissions.")
    except Exception as exc:
        problem("job-list", "visible jobs", exc)

    # Discover standalone notebooks as well as job-referenced ones, inside the chosen scope.
    pending = [project_root if scope == "project" else "/"]
    visited = set()
    while pending and not truncated:
        directory = pending.pop()
        if directory in visited:
            continue
        if len(visited) >= max_assets:
            truncated = True
            break
        visited.add(directory)
        try:
            for item in w.workspace.list(directory):
                if item.object_type == ObjectType.DIRECTORY:
                    if not any(x in item.path for x in ("/.git", "/.venv", "/__pycache__", "/state")):
                        pending.append(item.path)
                elif item.object_type == ObjectType.NOTEBOOK and item.path not in discovered_paths:
                    notebook(item.path)
        except Exception as exc:
            problem("workspace-list", directory, exc)
    try:
        for info in w.clusters.list():
            if scope == "workspace" or (info.custom_tags or {}).get("Project") in {PROJECT, BASELINE_PROJECT}:
                cluster(info.cluster_id)
    except Exception as exc:
        problem("cluster-list", "visible clusters", exc)

    values = list(assets.values())
    findings = [{"asset_key": a["asset_key"], **f} for a in values for f in a["findings"]]
    return {"schema_version": 1, "rule_version": RULE_VERSION, "scan_time": datetime.now(timezone.utc).isoformat(),
            "actor": user, "scope": scope, "scope_root": project_root, "coverage": "PARTIAL" if errors or truncated else "COMPLETE_WITHIN_SELECTED_VISIBILITY",
            "truncated": truncated, "max_assets": max_assets, "errors": errors,
            "assets": values, "relationships": [r for r in relationships if r["from_key"] in assets and r["to_key"] in assets], "findings": findings,
            "summary": {"assets": len(values), "jobs": sum(a["asset_type"] == "JOB" for a in values),
                        "notebooks": sum(a["asset_type"] == "NOTEBOOK" for a in values), "clusters": sum(a["asset_type"] == "CLUSTER" for a in values),
                        "findings": len(findings), "fixtures": sum(a["is_fixture"] for a in values),
                        "unknown_or_partial": sum(a["assessment_coverage"] != "COMPLETE" for a in values)},
            "cost": {"status": "UNAVAILABLE", "reason": "Billing schema access has not been granted; no zero-cost assumption is made."},
            "limitations": "Read-only assessment; static findings require review. No migration, promotion, approval decision, or rollback is executed."}

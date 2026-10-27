import os
from .presentation import legacy_name, visible_report
"""Read-only projection of inventory, versioned plans and execution evidence."""
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import threading
import time
from .execution import evidence_links
from .plan_store import ID
from .experiment_profile import PROFILE as EXPERIMENT_PROFILE, plan_matches as experiment_matches, source_supported as experiment_supported
from .proof_profile import BATCH_PROFILE_ID, batch_plan_matches, batch_source_supported
from .workflow import PLANNER_VERSION, WorkflowError, timestamp

PLAN_LIMIT = 100
ATTENTION = {"BLOCKED", "STALE", "VALIDATION_FAILED", "NEEDS_REVIEW", "REJECTED"}


def date_key(value):
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except (ValueError, AttributeError, TypeError):
        return 0


class FleetService:
    def __init__(self, planner, report_loader=None, runner_id=None, ttl=15):
        self.planner = planner
        self.store = planner.store
        self.report_loader = report_loader or planner.report_loader
        self.runner_id = str(runner_id or "")
        self.ttl = ttl
        self._cache = None
        self._loaded = 0
        self._lock = threading.Lock()

    def get(self, refresh=False):
        with self._lock:
            if not refresh and self._cache is not None and time.monotonic() - self._loaded < self.ttl:
                return self._cache
            result = self._build()
            self._cache, self._loaded = result, time.monotonic()
            return result

    def _plan(self, plan_id):
        try:
            plan = self.planner.get(plan_id, check_source=False)
        except Exception as exc:
            issue = {"plan_id": plan_id, "code": exc.code if isinstance(exc, WorkflowError) else "RECORD_UNAVAILABLE"}
            try:
                raw = self.store.read("plans", plan_id) or {}
                if isinstance(raw.get("asset_key"), str) and raw["asset_key"].startswith("JOB:") and raw["asset_key"][4:].isdigit():
                    issue["asset_key"] = raw["asset_key"]
            except Exception:
                pass
            return {"plan": None, "trial": None, "issue": issue}
        try:
            claim = self.store.read("execution_claims", plan_id)
            return {"plan": plan, "trial": self._trial(plan, claim) if claim else None, "issue": None}
        except Exception as exc:
            trial = {"plan_id": plan_id, "state": "NEEDS_REVIEW", "validation_status": "UNKNOWN", "requested_at": plan["created_at"], "execution_id": "", "links": {}, "live_status": "UNAVAILABLE"}
            return {"plan": plan, "trial": trial, "issue": {"plan_id": plan_id, "asset_key": plan["asset_key"], "code": exc.code if isinstance(exc, WorkflowError) else "RECORD_UNAVAILABLE"}}

    def _trial(self, plan, claim):
        execution_id = claim.get("execution_id", "")
        if not ID.fullmatch(execution_id) or claim.get("plan_id", plan["plan_id"]) != plan["plan_id"]:
            raise WorkflowError("INVALID_EXECUTION_BINDING", "Execution claim does not match its plan.")
        record = self.store.read("executions", execution_id)
        trial = {"execution_id": execution_id, "plan_id": plan["plan_id"], "requested_at": plan["created_at"], "state": "REQUESTING", "validation_status": "NOT_RUN", "phase": None, "links": {}, "live_status": "NOT_CHECKED"}
        if not record:
            return trial
        if any(record.get(key) != plan[key] for key in ("plan_id", "plan_hash", "source_fingerprint")) or not plan.get("decision") or plan["decision"]["decision"] != "APPROVED" or (record.get("actor") or "").casefold() != plan["decision"]["actor"].casefold():
            raise WorkflowError("INVALID_EXECUTION_BINDING", "Execution request does not match the approved plan.")
        trial["requested_at"] = record["requested_at"]
        result = self.store.read("execution_results", execution_id)
        baseline = self.store.read("execution_baseline", execution_id)
        candidate = self.store.read("execution_candidate", execution_id)
        trial["links"] = evidence_links(self.planner.workspace, {"baseline": baseline, "candidate": candidate})
        if result:
            legacy_failure = result.get("status") == "FAILED" and not any(key in result for key in ("execution_id", "plan_id", "plan_hash", "source_fingerprint"))
            if not legacy_failure and (result.get("execution_id") != execution_id or any(result.get(key) != plan[key] for key in ("plan_id", "plan_hash", "source_fingerprint"))):
                raise WorkflowError("INVALID_VALIDATION_BINDING", "Validation evidence does not match this execution and plan.")
            orders = plan.get("execution_profile") == BATCH_PROFILE_ID
            expected_metrics = {"classic_rows": 6 if orders else 5, "candidate_rows": 6 if orders else 5,
                                "classic_sales": 1080 if orders else 1000, "candidate_sales": 1080 if orders else 1000,
                                "left_only": 0, "right_only": 0}
            experiment = plan.get("execution_profile") == EXPERIMENT_PROFILE
            if experiment:
                expected_metrics = {"classic_sales": 1080, "candidate_sales": 1080, "left_only": 0, "right_only": 0, "input_match": True}
                if result.get("status") == "PASS" and (not experiment_matches(plan) or result.get("validation_profile") != EXPERIMENT_PROFILE or result.get("classic_rows") != result.get("candidate_rows") or not isinstance(result.get("classic_rows"), int) or not 1 <= result["classic_rows"] <= int(plan["experiment"]["groups"])):
                    raise WorkflowError("INVALID_VALIDATION_BINDING", "Experiment validation metrics do not match the reviewed profile.")
            invalid_orders_profile = orders and (not batch_plan_matches(plan) or result.get("validation_profile") != BATCH_PROFILE_ID)
            if result.get("status") == "PASS" and (invalid_orders_profile or not baseline or not candidate or result.get("baseline_run_id") != baseline["run_id"] or result.get("candidate_run_id") != candidate["run_id"] or result.get("schema_match") is not True or any(result.get(key) != expected for key, expected in expected_metrics.items())):
                raise WorkflowError("INVALID_VALIDATION_BINDING", "A passing demo result must match its actual A/B runs and metrics.")
            trial["metrics"] = {key: result[key] for key in ("classic_rows", "candidate_rows", "classic_sales", "candidate_sales", "left_only", "right_only", "schema_match", "input_match", "classic_duration_seconds", "candidate_duration_seconds", "classic_table", "candidate_table", "comparison") if key in result}
            status = result.get("status")
            trial.update(state=status if status in {"PASS", "FAILED", "VALIDATION_FAILED"} else "NEEDS_REVIEW", validation_status="PASS" if status == "PASS" else "FAILED" if status in {"FAILED", "VALIDATION_FAILED"} else "UNKNOWN", checked_at=result.get("checked_at"), failed_stage=result.get("failed_stage"))
            return trial
        progress = self.store.read("execution_progress", execution_id) or {}
        trial.update(state="REQUESTED", phase=progress.get("phase"), progress_updated_at=progress.get("updated_at"))
        launch = self.store.read("execution_launches", execution_id)
        if launch:
            if not self.runner_id or record.get("runner_job_id") != self.runner_id or not str(launch.get("run_id", "")).isdigit():
                raise WorkflowError("INVALID_EXECUTOR", "The execution is outside the configured project runner.")
            try:
                run = self.planner.workspace.jobs.get_run(int(launch["run_id"]))
                if str(run.job_id) != self.runner_id:
                    raise WorkflowError("INVALID_EXECUTOR", "The execution belongs to a different runner.")
                trial.update(state="NEEDS_REVIEW" if run.state.result_state else run.state.life_cycle_state.value, live_status="VERIFIED", runner_run_id=str(launch["run_id"]))
            except WorkflowError:
                raise
            except Exception:
                trial.update(state="NEEDS_REVIEW", live_status="UNAVAILABLE")
        return trial

    def _build(self):
        report = self.report_loader()
        if os.environ.get("MIGRATION_HIDE_LEGACY_DEMOS") == "true": report = visible_report(report)
        ids = self.store.list_ids(limit=PLAN_LIMIT + 1)
        limited = len(ids) > PLAN_LIMIT
        with ThreadPoolExecutor(max_workers=6) as pool:
            records = list(pool.map(self._plan, ids[:PLAN_LIMIT]))
        by_job = defaultdict(list)
        issues = []
        if os.environ.get("MIGRATION_HIDE_LEGACY_DEMOS") == "true":
            records = [r for r in records if not r.get("plan") or not legacy_name(r["plan"].get("source_name"))]
        for record in records:
            if record["issue"]:
                issues.append(record["issue"])
            if record["plan"]:
                by_job[record["plan"]["asset_key"]].append(record)
        edges = defaultdict(set)
        for edge in report.get("relationships", []):
            if edge.get("relation") in {"HAS_TASK", "RUNS_NOTEBOOK", "USES_CLUSTER"}:
                edges[edge["from_key"]].add(edge["to_key"])
        rows = []
        for asset in report["assets"]:
            if asset.get("asset_type") != "JOB" or asset.get("is_fixture") or os.environ.get("MIGRATION_HIDE_LEGACY_DEMOS") == "true" and legacy_name(asset.get("name")):
                continue
            keys, pending = set(), [asset["asset_key"]]
            while pending:
                key = pending.pop()
                if key not in keys:
                    keys.add(key)
                    pending.extend(edges[key] - keys)
            blockers = sum(f.get("severity") == "BLOCKER" and f.get("asset_key") in keys for f in report.get("findings", []))
            history = sorted(by_job.get(asset["asset_key"], []), key=lambda r: (date_key(r["plan"]["created_at"]), r["plan"]["plan_id"]), reverse=True)
            row = self._row(report, asset, blockers, history)
            row["evidence_issues"] = [issue["code"] for issue in issues if issue.get("asset_key") == asset["asset_key"]]
            if row["evidence_issues"]:
                row["status"] = "NEEDS_REVIEW"
            rows.append(row)
        counts = Counter(row["status"] for row in rows)
        job_keys = {row["asset_key"] for row in rows}
        return {"scan_id": report["scan_id"], "scan_time": report["scan_time"], "updated_at": timestamp(), "scope": report["scope"], "workloads": rows,
                "summary": {"total_jobs": len(rows), "classic_jobs": sum(row["compute_kind"] in {"CLASSIC", "MIXED"} for row in rows), "planned_jobs": sum(row["latest_plan"] is not None for row in rows), "awaiting_approval": counts["AWAITING_APPROVAL"], "validating": counts["VALIDATING"], "validated_candidates": counts["VALIDATED"], "attention_jobs": sum(counts[key] for key in ATTENTION), "by_status": dict(counts)},
                "coverage": {"inventory": report.get("coverage", "PARTIAL"), "records": "PARTIAL" if limited or issues else "COMPLETE_WITHIN_LIMIT", "plan_limit": PLAN_LIMIT, "plans_read": min(len(ids), PLAN_LIMIT), "limit_reached": limited, "issues": issues, "plans_outside_snapshot": sum(len(v) for k, v in by_job.items() if k not in job_keys)},
                "cost": report.get("cost") or {"status": "UNAVAILABLE", "reason": "Billing evidence has not been published."}, "cutover_tracked": False,
                "limitations": "Progress reflects published inventory and saved project records. A validated candidate is not a production cutover. Source freshness is checked against the published scan, not live workload configuration. No workloads are started or modified by this view."}

    def _row(self, report, asset, blockers, history):
        plan = history[0]["plan"] if history else None
        trials = [r["trial"] for r in history if r["trial"]]
        latest_trial = max(trials, key=lambda t: (date_key(t["requested_at"]), t["execution_id"])) if trials else None
        current_trial = history[0]["trial"] if history else None
        freshness = "NOT_CHECKED"
        if plan:
            source = report.get("planning_sources", {}).get(plan["source_job_id"])
            if source:
                freshness = "CURRENT" if source.get("source_fingerprint") == plan["source_fingerprint"] and plan["planner_version"] == PLANNER_VERSION and report.get("scope_root") == plan["scope_root"] and report.get("actor") == plan["project_owner"] else "STALE"
            elif report.get("scope") == "workspace":
                freshness = "STALE"
        status = "BLOCKED" if blockers else "NOT_PLANNED" if asset.get("planning_supported") else "NEEDS_REVIEW"
        if asset.get("compute_kind") not in {"CLASSIC", "MIXED"}:
            status = "NOT_TARGETED"
        if plan:
            status = {"DRAFT": "AWAITING_APPROVAL", "APPROVED": "APPROVED", "REJECTED": "REJECTED", "BLOCKED": "BLOCKED"}.get(plan["status"], "NEEDS_REVIEW")
            if current_trial:
                state = current_trial["state"]
                status = "VALIDATED" if state == "PASS" else "VALIDATION_FAILED" if state in {"FAILED", "VALIDATION_FAILED"} else "NEEDS_REVIEW" if state == "NEEDS_REVIEW" else "VALIDATING"
            if freshness == "STALE" and plan["status"] != "REJECTED":
                status = "STALE"
        completed = next((r["trial"] for r in history if r.get("trial") and r["trial"].get("validation_status") == "PASS" and r["plan"].get("source_fingerprint") == report.get("planning_sources", {}).get(str(asset.get("resource_id")), {}).get("source_fingerprint") and r["plan"].get("planner_version") == PLANNER_VERSION), None)
        direct = None
        if plan and plan.get("source_fingerprint") == report.get("planning_sources", {}).get(str(asset.get("resource_id")), {}).get("source_fingerprint"):
            from .direct_wave import claim_key
            direct = self.store.read("direct_candidate_results", claim_key(plan))
            if direct and direct.get("source_fingerprint") == plan["source_fingerprint"] and direct.get("source_job_id") == plan["source_job_id"] and str(direct.get("candidate_job_id", "")).isdigit():
                if not completed: status = "MIGRATED_UNVALIDATED"
            else: direct = None
        return {"direct_candidate_created": bool(direct), "direct_candidate_job_id": direct.get("candidate_job_id") if direct else None, "migration_completed": bool(completed), "asset_key": asset["asset_key"], "job_id": str(asset.get("resource_id", asset["asset_key"].split(":", 1)[1])), "name": asset["name"], "owner": asset.get("owner"), "compute_kind": asset.get("compute_kind", "UNKNOWN"), "readiness_score": asset.get("readiness_score"), "blocker_count": blockers,
                "assessment_coverage": asset.get("assessment_coverage", "METADATA_ONLY"), "published_source_fingerprint": report.get("planning_sources", {}).get(str(asset.get("resource_id", "")), {}).get("source_fingerprint"), "status": status, "source_freshness": freshness, "plan_count": len(history),
                "latest_plan": {"plan_id": plan["plan_id"], "status": plan["status"], "created_at": plan["created_at"], "decision": {key: plan["decision"][key] for key in ("decision", "actor", "role", "decided_at")} if plan.get("decision") else None} if plan else None,
                "execution": current_trial, "latest_validation": {**latest_trial, "is_latest_plan": latest_trial["plan_id"] == plan["plan_id"], "matches_scan": freshness == "CURRENT" and latest_trial["plan_id"] == plan["plan_id"]} if latest_trial else None,
                "execution_supported": experiment_supported(report.get("planning_sources", {}).get(str(asset.get("resource_id")), {})) or (bool(asset.get("is_demo")) and str(asset.get("resource_id")) == self.planner.allowed_job_id) or batch_source_supported(report.get("planning_sources", {}).get(str(asset.get("resource_id")), {}), report.get("scope_root", "")),
                "next_step": "Review saved plan and validation evidence." if plan else asset.get("planning_reason") or "Prepare a plan from the discovered workload."}

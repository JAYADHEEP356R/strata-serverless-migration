import os
from .presentation import legacy_name
"""Immutable named review groups; no approval, execution or workload mutation."""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import re
import uuid
from .fleet import date_key, ATTENTION
from .plan_store import ID
from .workflow import WorkflowError, digest, timestamp

WAVE_LIMIT = 50
MEMBER_LIMIT = 50


def verified_wave(value, expected_id=None):
    if not value or value.get("wave_hash") != digest({k: v for k, v in value.items() if k != "wave_hash"}):
        raise WorkflowError("WAVE_INTEGRITY", "The saved wave no longer matches its recorded fingerprint.")
    if (expected_id is not None and value.get("wave_id") != expected_id) or not ID.fullmatch(value.get("wave_id", "")) or not 1 <= len(value.get("members", [])) <= MEMBER_LIMIT or len({m["asset_key"] for m in value["members"]}) != len(value["members"]):
        raise WorkflowError("WAVE_INTEGRITY", "The saved wave membership is invalid.")
    return value


class WaveService:
    def __init__(self, planner, fleet):
        self.planner, self.fleet, self.store = planner, fleet, planner.store

    def create(self, actor, name, description, asset_keys, scan_id, request_id):
        if not self.planner.policy(actor)["can_generate"]:
            raise WorkflowError("NOT_AUTHORIZED", "Only the project operator may save migration waves.", 403)
        name, description = " ".join(name.split()), description.strip()
        if not 1 <= len(name) <= 80 or len(description) > 500 or any(ord(c) < 32 or ord(c) == 127 for c in name):
            raise WorkflowError("INVALID_WAVE_NAME", "Use a wave name of 1–80 characters and a description of up to 500 characters.", 400)
        if not isinstance(asset_keys, list) or not 1 <= len(asset_keys) <= MEMBER_LIMIT or len(set(asset_keys)) != len(asset_keys) or any(not isinstance(key, str) or not re.fullmatch(r"JOB:[0-9]+", key) for key in asset_keys):
            raise WorkflowError("INVALID_WAVE_MEMBERS", "Select 1–50 distinct discovered classic jobs.", 400)
        try:
            request_id = str(uuid.UUID(request_id))
        except (ValueError, AttributeError):
            raise WorkflowError("INVALID_REQUEST_ID", "A UUID wave request is required.", 400) from None
        wave_id = uuid.uuid5(uuid.NAMESPACE_URL, "migration-wave:" + actor.casefold() + ":" + request_id).hex
        body = {"name": name, "description": description, "asset_keys": sorted(asset_keys), "scan_id": scan_id, "created_by": actor.casefold()}
        request_hash = digest(body)
        existing = self.store.read("waves", wave_id)
        if existing:
            existing = verified_wave(existing, wave_id)
            if existing.get("request_hash") != request_hash:
                raise WorkflowError("WAVE_REQUEST_CONFLICT", "This request already saved a different wave. Reload the saved wave before retrying.")
            return existing
        report = self.planner.report_loader()
        if report.get("scope") != "workspace":
            raise WorkflowError("WORKSPACE_SCAN_REQUIRED", "Run workspace discovery before saving a migration wave.")
        if scan_id != report["scan_id"]:
            raise WorkflowError("STALE_INVENTORY", "The inventory changed. Refresh and review the selected workloads before saving the wave.")
        assets = {a["asset_key"]: a for a in report["assets"] if a.get("asset_type") == "JOB" and not a.get("is_fixture")}
        members = []
        for key in sorted(asset_keys):
            asset = assets.get(key)
            if not asset or asset.get("compute_kind") not in {"CLASSIC", "MIXED"}:
                raise WorkflowError("OUTSIDE_WAVE_SCOPE", "Every wave member must be a classic or mixed job in the published discovery snapshot.", 403)
            job_id = key.split(":", 1)[1]
            source = report.get("planning_sources", {}).get(job_id, {})
            members.append({"asset_key": key, "job_id": job_id, "name": asset["name"], "owner": asset.get("owner"), "compute_kind": asset["compute_kind"], "planning_supported": bool(asset.get("planning_supported")), "source_fingerprint": source.get("source_fingerprint")})
        value = {"schema_version": 1, "wave_id": wave_id, **body, "created_at": timestamp(), "scope_root": report.get("scope_root"), "project_owner": report.get("actor"), "members": members, "request_hash": request_hash, "execution_enabled": False}
        value["wave_hash"] = digest(value)
        self.store.write_once("waves", wave_id, value)
        saved = verified_wave(self.store.read("waves", wave_id), wave_id)
        if saved["request_hash"] != request_hash:
            raise WorkflowError("WAVE_REQUEST_CONFLICT", "Another wave was saved for this request first.")
        return saved

    def list(self):
        ids = self.store.list_record_ids("waves", limit=WAVE_LIMIT + 1)
        def load(wave_id):
            try:
                value = verified_wave(self.store.read("waves", wave_id), wave_id)
                if os.environ.get("MIGRATION_HIDE_LEGACY_DEMOS") == "true" and (value["name"] == "Demo migration review" or all(legacy_name(m.get("name")) for m in value["members"])):
                    return None, None
                return {"wave_id": value["wave_id"], "name": value["name"], "description": value["description"], "created_at": value["created_at"], "created_by": value["created_by"], "scan_id": value["scan_id"], "member_count": len(value["members"]), "wave_hash": value["wave_hash"]}, None
            except Exception as exc:
                return None, {"wave_id": wave_id, "code": exc.code if isinstance(exc, WorkflowError) else "WAVE_UNAVAILABLE"}
        with ThreadPoolExecutor(max_workers=4) as pool:
            rows = list(pool.map(load, ids[:WAVE_LIMIT]))
        return {"waves": sorted([value for value, error in rows if value], key=lambda w: (date_key(w["created_at"]), w["wave_id"]), reverse=True), "coverage": "PARTIAL" if len(ids) > WAVE_LIMIT or any(error for _, error in rows) else "COMPLETE_WITHIN_LIMIT", "limit": WAVE_LIMIT, "issues": [error for _, error in rows if error], "execution_enabled": False}

    def get(self, wave_id, refresh=False):
        if not ID.fullmatch(wave_id):
            raise WorkflowError("INVALID_WAVE_ID", "Invalid saved wave reference.", 400)
        value = self.store.read("waves", wave_id)
        if not value:
            raise WorkflowError("WAVE_NOT_FOUND", "The migration wave was not found.", 404)
        value = verified_wave(value, wave_id)
        if os.environ.get("MIGRATION_HIDE_LEGACY_DEMOS") == "true" and (value["name"] == "Demo migration review" or all(legacy_name(m.get("name")) for m in value["members"])):
            raise WorkflowError("WAVE_NOT_FOUND", "This retired demonstration is no longer shown in the App.", 404)
        fleet = self.fleet.get(refresh=refresh)
        rows = {r["asset_key"]: r for r in fleet["workloads"]}
        members = []
        for saved in value["members"]:
            current = rows.get(saved["asset_key"])
            members.append({"saved": saved, "progress": current, "status": current["status"] if current else "NOT_IN_SNAPSHOT", "source_changed_since_saved": saved["source_fingerprint"] != current["published_source_fingerprint"] if current and saved.get("source_fingerprint") and current.get("published_source_fingerprint") else None})
        counts = Counter(m["status"] for m in members)
        partial = fleet["coverage"]["records"] == "PARTIAL"
        attention = sum(counts[s] for s in ATTENTION | {"NOT_IN_SNAPSHOT"})
        status = "NEEDS_REVIEW" if partial or attention else "VALIDATED" if counts["VALIDATED"] == len(members) else "VALIDATING" if counts["VALIDATING"] else "IN_REVIEW" if any(m["progress"] and m["progress"]["latest_plan"] for m in members) else "DRAFT"
        return {"wave": value, "members": members, "status": status, "summary": {"total": len(members), "with_plans": sum(bool(m["progress"] and m["progress"]["latest_plan"]) for m in members), "awaiting_approval": counts["AWAITING_APPROVAL"], "validating": counts["VALIDATING"], "validated": counts["VALIDATED"], "needs_attention": attention, "missing_from_snapshot": counts["NOT_IN_SNAPSHOT"], "by_status": dict(counts)},
                "scan_id": fleet["scan_id"], "updated_at": fleet["updated_at"], "selection_scan_changed": fleet["scan_id"] != value["scan_id"], "coverage": fleet["coverage"], "execution_enabled": False,
                "scope": "Saved membership with each workload's latest project plan and evidence. Wave creation grants no approval and starts no workload. Missing from a partial snapshot does not mean a job was deleted."}

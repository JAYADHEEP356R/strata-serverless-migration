# Databricks notebook source
"""Persist a read-only discovery snapshot only inside the project-marked schema."""
import io
import json
import re
import sys
from datetime import datetime, timezone
from databricks.sdk import WorkspaceClient

for name, default in (("catalog", ""), ("schema", ""), ("project_root", ""), ("source_root", ""), ("scope", "project"), ("scan_id", "")):
    dbutils.widgets.text(name, default)
catalog, schema, scan_id = [dbutils.widgets.get(k) for k in ("catalog", "schema", "scan_id")]
for identifier in (catalog, schema, scan_id):
    if not re.fullmatch(r"[A-Za-z0-9_]+", identifier):
        raise ValueError("Invalid project identifier")
if not schema.startswith("migration_agent_jr_"):
    raise PermissionError("Refusing writes outside the project schema")
project_root = dbutils.widgets.get("project_root")
source_root = dbutils.widgets.get("source_root")
if source_root != project_root + "/src":
    raise PermissionError("Package source must be inside the bundle folder")
sys.path.insert(0, source_root)
from migration_agent.discovery import collect, PROJECT
from migration_agent.workspace_scan import enrich_planning

w = WorkspaceClient()
schema_info = w.schemas.get(f"{catalog}.{schema}")
if (schema_info.properties or {}).get("migration_agent_project") != PROJECT:
    raise PermissionError("Schema project marker does not match")
report = collect(w, project_root, scope=dbutils.widgets.get("scope"))
if report["scope"] == "workspace":
    enrich_planning(w, report)
report["scan_id"] = scan_id
report["project_schema"] = f"{catalog}.{schema}"
namespace = f"`{catalog}`.`{schema}`"

inventory_type = "scan_id string, asset_key string, asset_type string, resource_id string, name string, owner string, path string, compute_kind string, readiness_score long, eligible boolean, assessment_coverage string, migration_status string, is_fixture boolean, details_json string"
inventory = [(scan_id, a["asset_key"], a["asset_type"], a["resource_id"], a["name"], a.get("owner"), a.get("path"), a["compute_kind"], a["readiness_score"], a["eligible"], a["assessment_coverage"], a["migration_status"], a["is_fixture"], json.dumps(a)) for a in report["assets"]]
spark.createDataFrame(inventory, inventory_type).write.mode("append").saveAsTable(f"{namespace}.inventory_snapshots")
finding_type = "scan_id string, asset_key string, limitation string, rule string, severity string, location string, evidence string, recommendation string"
findings = [(scan_id, f["asset_key"], f["limitation"], f["rule"], f["severity"], f["location"], f["evidence"], f["recommendation"]) for f in report["findings"]]
spark.createDataFrame(findings, finding_type).write.mode("append").saveAsTable(f"{namespace}.assessment_findings")
relationship_type = "scan_id string, from_key string, to_key string, relation string, task_key string"
relationships = [(scan_id, r["from_key"], r["to_key"], r["relation"], r.get("task_key")) for r in report["relationships"]]
spark.createDataFrame(relationships, relationship_type).write.mode("append").saveAsTable(f"{namespace}.asset_relationships")
try:
    validations = spark.table(f"{namespace}.validation_results").orderBy("checked_at", ascending=False).limit(5).collect()
    report["validation_evidence"] = [json.loads(row.details_json) for row in validations]
except Exception:
    report["validation_evidence"] = []
    report["validation_evidence_status"] = "UNAVAILABLE"

scan_type = "scan_id string, scanned_at timestamp, scope string, coverage string, asset_count long, finding_count long, summary_json string, errors_json string"
scan_row = [(scan_id, datetime.now(timezone.utc), report["scope"], report["coverage"], len(report["assets"]), len(report["findings"]), json.dumps(report["summary"]), json.dumps(report["errors"]))]
spark.createDataFrame(scan_row, scan_type).write.mode("append").saveAsTable(f"{namespace}.scan_runs")
payload = json.dumps(report, sort_keys=True, indent=2).encode("utf-8")
report_root = f"/Volumes/{catalog}/{schema}/artifacts/discovery"
w.files.create_directory(report_root)
w.files.upload(f"{report_root}/scan_{scan_id}.json", io.BytesIO(payload), overwrite=False)
w.files.upload(f"{report_root}/latest.json", io.BytesIO(payload), overwrite=True)
# Append a small audit event; the published snapshot remains the fallback evidence.
try:
    import uuid
    from migration_agent.plan_store import VolumePlanStore
    audit = VolumePlanStore(w, f"/Volumes/{catalog}/{schema}/plans")
    reference = uuid.uuid5(uuid.NAMESPACE_URL, "discovery-event:" + scan_id).hex
    audit.write_once("activity_events", reference, {"event_id": "scan-" + scan_id, "time": report["scan_time"], "category": "DISCOVERY", "title": "Workspace inventory published", "name": "", "description": f"{len(report['assets'])} assets · {len(report['findings'])} findings", "actor": report.get("actor", ""), "status": "PARTIAL" if report["coverage"] == "PARTIAL" else "SUCCEEDED"})
except Exception as exc:
    print(json.dumps({"event": "activity_journal_unavailable", "error_type": type(exc).__name__}))

dbutils.notebook.exit(json.dumps({"scan_id": scan_id, "coverage": report["coverage"], "summary": report["summary"], "errors": report["errors"], "report_path": f"{report_root}/latest.json"}))

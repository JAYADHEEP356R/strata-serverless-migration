"""App activity derived from persistent records, including historical cleanup."""
from .migration_center import verified_definition


def activity(store, report):
    events = []
    partial = False
    def add(identity, when, category, title, name="", description="", actor="", status="RECORDED", **extra):
        if when:
            events.append({"event_id": identity, "time": when, "category": category, "title": title, "name": name,
                "description": description, "actor": actor, "status": status, **extra})
    def ids(kind, limit=201):
        nonlocal partial
        values = store.list_record_ids(kind, limit=limit)
        partial = partial or len(values) >= limit
        return values[:limit-1]
    add("scan-" + report["scan_id"], report["scan_time"], "DISCOVERY", "Workspace inventory published",
        description=f"{len(report['assets'])} assets · {len(report.get('findings', []))} findings · {len(report.get('errors', []))} inspection errors",
        actor=report.get("actor", ""), status="PARTIAL" if report.get("coverage") == "PARTIAL" else "SUCCEEDED")
    for reference in ids("activity_events"):
        try:
            record = store.read("activity_events", reference)
            if record:
                events.append(record)
        except Exception:
            partial = True
    for reference in ids("migration_definitions"):
        try:
            plan = verified_definition(store.read("migration_definitions", reference), reference)
            add("saved-"+reference, plan["created_at"], "PLAN", "Migration wave saved" if plan["mode"] == "WAVE" else "Individual migration saved",
                plan["name"], f"{len(plan['members'])} {plan['kind'].lower()} item(s)", plan["owner"], "SAVED", definition_id=reference)
            archived = store.read("migration_archives", reference)
            if archived:
                add("archive-"+reference, archived["archived_at"], "CLEANUP", "Sample migration plan archived", plan["name"], archived["reason"], archived["actor"], "SUCCEEDED", historical=True)
            request = store.read("migration_requests", reference)
            if request:
                add("approved-"+reference, request["requested_at"], "MIGRATION", "Migration approved and queued", plan["name"],
                    "Candidate creation was explicitly authorized.", request["actor"], "APPROVED", definition_id=reference)
            result = store.read("migration_results", reference)
            if result:
                for index, member in enumerate(result["members"]):
                    add(f"member-{reference}-{index}", member.get("finished_at") or result["finished_at"], "MIGRATION", "Serverless candidate ready" if member["state"] == "SUCCEEDED" else "Candidate creation failed",
                        member["name"], member.get("message", ""), plan["owner"], member["state"], definition_id=reference)
                add("result-"+reference, result["finished_at"], "MIGRATION", "Migration wave completed" if plan["mode"] == "WAVE" else "Migration completed",
                    plan["name"], f"{sum(m['state']=='SUCCEEDED' for m in result['members'])}/{len(result['members'])} candidates ready", plan["owner"], result["state"], definition_id=reference)
        except Exception:
            partial = True
    for reference in ids("validation_requests"):
        try:
            request = store.read("validation_requests", reference)
            add("validation-request-"+reference, request["requested_at"], "VALIDATION", "Output comparison requested" if request["mode"] == "OUTPUT" else "Configuration validation requested",
                request["name"], "Validation is a separate action after successful migration.", request["actor"], "QUEUED", validation_id=reference)
            result = store.read("validation_results", reference)
            if result:
                title = "Output validation passed" if request["mode"] == "OUTPUT" else "Configuration checks passed"
                add("validation-result-"+reference, result["finished_at"], "VALIDATION", title if result["state"] == "PASSED" else "Validation failed",
                    request["name"], result.get("message") or ("Original and serverless outputs match." if result.get("output_compared") else "Configuration checked; workload outputs were not compared."),
                    request["actor"], result["state"], validation_id=reference)
        except Exception:
            partial = True
    # Preserve the previously completed waves as historical audit events.
    for reference in ids("waves", 51):
        try:
            wave = store.read("waves", reference)
            add("legacy-wave-"+reference, wave["created_at"], "PLAN", "Historical wave saved", wave["name"], f"{len(wave['members'])} sample workloads", wave.get("created_by", ""), "SAVED", historical=True)
            request = store.read("direct_wave_requests", reference)
            result = store.read("direct_wave_results", reference)
            if request:
                add("legacy-approved-"+reference, request["requested_at"], "MIGRATION", "Historical wave approved", wave["name"], "Direct serverless candidate creation", request["actor"], "APPROVED", historical=True)
            if result:
                add("legacy-completed-"+reference, result["updated_at"], "MIGRATION", "Historical wave finished", wave["name"],
                    f"{sum(m['state']=='CREATED' for m in result['members'])} serverless candidates created; sample resources may since have been removed.",
                    wave.get("created_by", ""), "SUCCEEDED" if result["state"] == "COMPLETED" else "FAILED", historical=True)
        except Exception:
            partial = True
    unique = {e["event_id"]: e for e in events}
    return {"events": sorted(unique.values(), key=lambda e: e["time"], reverse=True)[:500], "partial": partial or len(unique)>500,
        "scope": "Recorded application activity, not a complete workspace audit log."}

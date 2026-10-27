"""A narrowly scoped, pinned all-classic orders proof; not arbitrary job execution."""
BATCH_PROFILE_ID = "isolated-orders-batch-v1"
BATCH_PURPOSE = "isolated-batch-proof"
BATCH_NOTEBOOKS = {
    "ingest_orders": "orders_batch_prepare",
    "aggregate_orders": "orders_batch_aggregate",
    "verify_orders": "orders_batch_verify",
}
BATCH_DEPENDENCIES = {"ingest_orders": [], "aggregate_orders": ["ingest_orders"], "verify_orders": ["aggregate_orders"]}
BATCH_HASHES = {
    "orders_batch_prepare": "1f7ede9139cdb96e4b79c965b10e500eba7aa30552d4252d46c51533bff90424",
    "orders_batch_aggregate": "92bbae4495a7675de5fd92d865060a82cb0e7f8cef1f2271a1d0e1c836fbe400",
    "orders_batch_verify": "f7e85ae63c0de5a0a2e0844245b8629e0d95d406102bba7c89c5cbe643740f09",
}


def batch_plan_matches(plan):
    tasks = plan.get("tasks", [])
    root = plan.get("scope_root", "")
    return len(tasks) == 3 and {t.get("task_key") for t in tasks} == set(BATCH_NOTEBOOKS) and all(
        t.get("source_notebook", "").removesuffix(".py") == root + "/notebooks/" + BATCH_NOTEBOOKS[t["task_key"]]
        and t.get("source_compute") == "CLASSIC"
        and t.get("depends_on") == BATCH_DEPENDENCIES[t["task_key"]]
        and t.get("run_if") == "ALL_SUCCESS" and t.get("language") == "PYTHON"
        for t in tasks
    )


def batch_source_supported(source, root):
    tags = source.get("settings", {}).get("tags", {})
    marked = source.get("execution_profile") == BATCH_PROFILE_ID or (tags.get("Project") == "serverless-migration-challenge-jr" and tags.get("Purpose") == BATCH_PURPOSE and tags.get("MigrationProfile") == BATCH_PROFILE_ID)
    return marked and batch_plan_matches({"tasks": source.get("tasks", []), "scope_root": root})

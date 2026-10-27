# Migration Plans workspace

The Migration Plans page now starts with **Create Migration Plan**:

1. Choose Jobs or Notebooks.
2. Select up to 50 discovered workloads.
3. Inspect compute context, cluster/task libraries, serverless dependency mappings and compatibility findings.
4. Save one named wave or save each selected workload as an individual migration plan.
5. Open a saved item, review its configuration, then approve and start the migration operation.

Saved configurations, approval requests, live progress and final results are persisted in the project plans volume. Waves create candidates for up to four members concurrently using the existing serverless migration executor. Individual plans use the same runner with one member. Each migration node records its own stage, timestamps and result. A terminated runner without a final record requires review. Retries reuse the same request and candidate claims prevent concurrent duplicate creation for the same source version.

Job migration supports inspected Python/SQL workspace notebook jobs within the existing planner limits. Pinned PyPI dependencies are mapped into environment version 4. Unsupported task types, untranslatable dependencies, unresolved blockers and changed sources prevent execution. Source jobs and notebooks are preserved. Existing intact candidates can be reused for the same source version.

Notebook migration creates a copied notebook and a serverless job wrapper. Associated job contexts supply inspected dependencies and parameters where consistent. Conflicting contexts block migration. Standalone notebooks show a warning that no job context supplies their runtime dependencies or parameters.

A successful migration operation means candidate configuration was created. It does not execute source or candidate workloads, compare outputs, activate schedules, perform production cutover, or delete originals. Validation lists successful saved-plan members and starts checks only after an explicit user request. Configuration checks verify serverless settings, notebook content and dependency mappings; they do not prove output equivalence. Output comparison is currently supported only for the pinned synthetic three-task experiment profile and requires an existing successful original run. Activity combines persisted discovery, plan, migration, validation and cleanup events with explicit historical labels. Archived sample plans retain their evidence but cannot run or appear as validation targets. Legacy records remain stored but the new Migration Plans page uses its own saved-plan collection.

Verification: focused local backend/API checks cover persistence, authorization, stale sources, blockers, dependency redaction, duplicate suppression, candidate integrity and notebook wrappers. Browser checks use isolated mock data and cover the creation wizard, both save modes, one-approval run initiation, progress polling, persistence, desktop/mobile layout and light/dark themes. These checks do not claim end-to-end execution of the new runtime in a fresh workspace.

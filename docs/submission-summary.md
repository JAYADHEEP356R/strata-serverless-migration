# Submission email template

Replace every placeholder and verify claims against the final rehearsal before sending.

Subject: Serverless Challenge - Submission - <your name>

Serverless Migration Challenge - Submission

Name(s) and team: <complete>
Repo link and final commit SHA (Option A) or zip file name (Option B): <complete>
Demo video: Attached separately to the submission email (five minutes or less).
Cloud tested on (AWS / Azure / GCP): Azure (deployment, discovery and candidate creation)

Limitations addressed (IDs): L1, L4, L5, L6, L7, L8, L10, L11, L13; partial detection for L2/L3/L9.
For each one, 1–2 lines on how:
- L1: Inventory, readiness/blockers, saved waves/individual plans, project-operator approval and live member progress. Admin-group integration remains a gap.
- L4: Detect unsupported runtime/compute settings and surface recommendations before migration.
- L5: Map supported pinned PyPI libraries to serverless environments; flag unsupported binary dependencies.
- L6/L7: Detect selected incompatible APIs and Scala/R; require review rather than claiming automatic rewrites.
- L8: Explicit configuration checks and sample-profile retained-original/candidate output diff. <Insert the actual final output-validation result; do not claim a passing cloud comparison before it succeeds.>
- L10: Create unscheduled candidates and verify their triggers remain disabled.
- L11/L13: Report selected classic-only attributes, streaming triggers and ANSI cast risks.
- L2/L3/L9: Detect large-job/init-script/source-management concerns; general remediation/write-back remains incomplete.

Architecture (max 5 lines):
Databricks App with React UI and FastAPI backend.
Lakeflow discovery job inspects visible workspace assets and compatibility.
Unity Catalog volumes persist snapshots, approvals, progress and evidence.
Serverless executor creates reviewed candidates with per-member progress.
Explicit validation requests produce configuration checks or supported output diffs.

Databricks components used: Apps, Lakeflow Jobs, Unity Catalog managed volumes, SDK, Asset Bundles, Delta tables for the optional sample comparison.
How to deploy: Follow README: authenticate, provision marked project storage, validate/deploy bundle, scan, publish App snapshot.
What is tested and how: 18 local backend/API checks; Azure deployment/discovery and candidate creation checked. <Add exact final rehearsal evidence and clean-workspace test status.>
Known gaps and what you would do next: See docs/challenge-coverage.md. Extend output contracts, admin roles, large-job migration, billing, Git reconciliation and controlled cutover.
Confirmation: no customer data, credentials or client workspace details are included (Yes / No): <confirm after final code/video review>

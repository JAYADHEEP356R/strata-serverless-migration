# Challenge coverage and known gaps

Coverage describes implementation, not a claim that every limitation is solved.

| ID | Implemented | Remaining boundary |
|---|---|---|
| L1 | Visible inventory, readiness/blockers, individual/wave plans, operator approval, per-member progress and separate validation | Not workspace-admin group integration; visibility is permission-bound; no billing comparison |
| L2 | Detects more than ten classic tasks | Migration planner also caps classic tasks at ten; no split/batch remediation |
| L3 | Flags init scripts, including S3/DBFS references | No automatic retrieval/translation |
| L4 | Flags custom images, ML/older runtime, instance profiles and other configuration concerns | Recommendations rather than universal automatic fixes |
| L5 | Maps supported pinned default-index PyPI libraries; flags eggs/JARs/Maven | Does not convert arbitrary binary/system dependencies |
| L6 | Static checks for selected cache/checkpoint/global-view/mount operations | No universal automatic rewrite or exhaustive code analysis |
| L7 | Classifies Scala/R and flags unsupported language | No automatic conversion |
| L8 | Explicit configuration validation; retained-original vs candidate output comparison for pinned sample profile | No universal workload output contract; latest end-to-end cloud output comparison still requires rehearsal |
| L9 | Source-managed warning and legacy bounded reconciliation tooling | New saved-wave flow has no generic Git/bundle write-back or continuous drift monitoring |
| L10 | Candidates are created without schedules/continuous triggers; validation rechecks this | Not general schedule cutover or automatic cleanup of unrelated duplicates |
| L11 | Reports selected classic-only logging/SSH attributes and mount concerns | Not exhaustive inventory or automatic replacements |
| L12 | UI avoids invented savings | Billing/DBU attribution and before/after cost comparison not implemented |
| L13 | Static missing/unsupported streaming-trigger and selected CAST-risk checks | Not exhaustive semantic verification or automatic rewrite |

Original jobs remain available. This preserves their configuration; it is not transactional
data rollback. Candidate failure does not automatically undo prior successful members of a wave.
No production schedules are activated and no originals are deleted by the migration flow.

Minimum submission materials: working workspace code, progress view, L1 plus at least two
other addressed limitations, README/setup/architecture/gaps, a video no longer than five minutes,
and no customer data, credentials or client workspace details. This package supplies code and
documentation; the video, repository access, final commit and organizer eligibility are separate.

> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Index Integrity Audit - 2026-06-25

## Run Metadata + Comparability Note

- scope: `index-integrity`

- recurring definition:
  `docs/audits/recurring/access/access-index-integrity.md`

- compared baseline report path:
  `docs/audits/reports/2026-05/2026-05-11/index-integrity.md`

- code snapshot identifier: `d389eec3bd` (`dirty` working tree)

- run timestamp: `2026-06-25T10:45:53+02:00`

- method tag/version: `Method V5`

- comparability status: `non-comparable`
  - Method V5 refreshes Method V4 for the current SQL DDL index publication
    surface
  - the stale standalone `write_boundary_guards` target no longer exists and
    was replaced with focused accepted-authority filters
  - future V5 runs should compare directly to this report

## Method Changes

- Added explicit `Method V5` identity to the recurring definition.

- Updated catalog mutation readiness for the current split between startup
  reconciliation and SQL DDL execution.

- Replaced the removed `write_boundary_guards` integration target with focused
  accepted-index, accepted-expression, accepted-relation, transition,
  reconciliation, and SQL DDL frontend authority filters.

- Added SQL DDL field-path, expression-index, unique-index, drop, generated-drop
  rejection, and cache-invalidation probes to the baseline.

- Added expression-index DDL rollback proof for post-insert validation failure.

- Added read-only run-mode and normalized verification statuses.

## Follow-Up Actions

- Keep future `index-integrity` runs on Method V5 so DDL-published field-path,
  expression, unique, drop, and rollback behavior remains comparable.

- Treat the removed `write_boundary_guards` integration target as retired; do
  not reintroduce it into reports unless a new target is actually added.

- Watch the dirty prepared/explain/cache work in the next run because accepted
  index fingerprints and shared plan cache identity are part of this audit.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/recurring/2026/06/25/index-integrity/01/report.md).

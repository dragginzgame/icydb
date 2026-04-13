# Structure / Module / Visibility Discipline Audit - 2026-04-13

## Report Preamble

- scope: `icydb-core` db subsystem structural boundaries and visibility discipline
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-24/module-structure.md`
- code snapshot identifier: `562f320cd`
- method tag/version: `Method V3`
- comparability status: `semi-comparable` (current run includes the post-0.77 SQL parser/lowering/execution split and uses direct inspected-file evidence for the SQL structural follow-through)

## Structural Findings

| Check | Evidence | Status | Risk |
| ---- | ---- | ---- | ---- |
| Upward imports / cross-layer policy re-derivations | `bash scripts/ci/check-layer-authority-invariants.sh` | PASS (`0` / `0`) | Low |
| Architecture text-scan invariant | `bash scripts/ci/check-architecture-text-scan-invariants.sh` | PASS | Low |
| Runtime compile stability | `cargo check -p icydb-core` | PASS | Medium |
| Recent SQL split preserved owner-local structure | inspected [`db/sql/parser/statement/mod.rs`](/home/adam/projects/icydb/crates/icydb-core/src/db/sql/parser/statement/mod.rs), [`db/session/sql/execute/mod.rs`](/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/execute/mod.rs), [`db/sql/lowering/select/mod.rs`](/home/adam/projects/icydb/crates/icydb-core/src/db/sql/lowering/select/mod.rs) | PASS | Low-Medium |

## Hub Import Pressure (Required Metric)

| Hub Module | Top Import Tokens | Unique Sibling Subsystems | Cross-Layer Count | Delta vs Previous |
| ---- | ---- | ----: | ----: | ---- |
| `executor/planning/route/planner/mod.rs` + `entrypoints.rs` | `executor`, `query`, `access` | 3 | 2 | stable |
| `sql/parser/statement/mod.rs` | `predicate`, `reduced_sql`, `sql::parser`, `sql::identifier` | 3 | 0 | changed shape, improved ownership concentration |
| `session/sql/execute/mod.rs` | `executor`, `query`, `session::sql`, `sql::parser` | 3 | 0 | improved after route/write/aggregate split |
| `sql/lowering/select/mod.rs` | `query`, `predicate`, `sql::parser`, `model` | 3 | 0 | changed shape, owner-local semantic concentration |

## Structural Interpretation

- The route-planner hub remains contained. [`planner/mod.rs`](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/planning/route/planner/mod.rs) is now only `23` lines and acts as a boundary/export root, while the heavier branching lives in [`entrypoints.rs`](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/planning/route/planner/entrypoints.rs) at `270` lines. That is still the main structural coordination point, but it is no longer a large root module.
- The recent SQL parser split improved ownership shape without pushing syntax logic into lowering. [`statement/mod.rs`](/home/adam/projects/icydb/crates/icydb-core/src/db/sql/parser/statement/mod.rs) is still a meaningful dispatcher at `415` lines because it owns top-level statement routing, clause-order diagnostics, `SHOW`, and `EXPLAIN` dispatch, but the statement-family parsing moved under `select.rs`, `insert.rs`, `update.rs`, and `delete.rs` rather than diffusing across parser root or lowering.
- SQL execution root pressure is lower and better contained. [`session/sql/execute/mod.rs`](/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/execute/mod.rs) is `127` lines and now reads as a route owner with submodules for aggregate, lowered, route, and write concerns. That is materially better than a monolithic execution root.
- SQL semantic concentration remains strongest in lowering, which is the correct owner. [`sql/lowering/select/mod.rs`](/home/adam/projects/icydb/crates/icydb-core/src/db/sql/lowering/select/mod.rs) is `299` lines after the split into `binding`, `projection`, `aggregate`, and `order`. That is moderate pressure, but it remains owner-local semantic work rather than a cross-layer leak.
- No new public-surface leak is evident from this run. The structural scripts are green, canonicalization entrypoints remain at `1`, and the SQL branch-ownership work did not widen facade or runtime visibility to compensate for the internal splits.

## Overall Structural Risk Index

**4/10**

## Follow-Up Actions

- Open one follow-up track only: route planner entrypoints discipline.
- Owner boundary: [`db/executor/planning/route/planner/entrypoints.rs`](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/planning/route/planner/entrypoints.rs).
- Action: keep route-planner entrypoints limited to route coordination, and reject new non-route imports or policy logic that belong in `execution`, `feasibility`, `intent`, or planner-local child modules.
- Target report date/run: next `module-structure` audit.

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS

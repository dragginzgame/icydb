# Crosscutting Flow Convergence Audit

## Purpose

Audit whether IcyDB query construction, planning, routing, execution, projection, pagination, and response shaping converge through canonical shared paths instead of drifting into parallel SQL/Fluent/runtime implementations.

This audit is concerned with duplicate flows, compatibility shims, redundant adapters, stale execution paths, and places where equivalent query semantics are implemented more than once.

## Audit Identity

Definition path:
`docs/audits/recurring/crosscutting/crosscutting-flow-convergence.md`

Report scope: `flow-convergence`

Current method tag: `Flow Convergence Method V2`

Use `docs/audits/reports/YYYY-MM/YYYY-MM-DD/flow-convergence.md` for the
first run of a day. Same-day reruns must use `flow-convergence-2.md`,
`flow-convergence-3.md`, and so on.

Method V2 refreshes the current split session-query, grouped-fold, shared
plan-cache, and executor-explain ownership checks from the original V1 report.

## Scope

Primary scope:

* SQL query path
* Fluent query path
* prepared query path
* grouped query path
* scalar load path
* projection/runtime shaping
* cursor/continuation handling
* aggregate execution
* response finalization

Code roots to inspect:

* `crates/icydb-core/src/db/session/`
* `crates/icydb-core/src/db/sql/`
* `crates/icydb-core/src/db/query/`
* `crates/icydb-core/src/db/executor/`
* `crates/icydb-core/src/db/cursor/`
* `crates/icydb/src/db/session/`

Current hotspot roots:

* `crates/icydb-core/src/db/session/query/*`
* `crates/icydb-core/src/db/session/sql/execute/*`
* `crates/icydb-core/src/db/executor/pipeline/entrypoints/*`
* `crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/*`
* `crates/icydb-core/src/db/executor/explain/*`
* `crates/icydb-core/src/db/query/plan/*`

## Core Questions

1. Do SQL and Fluent converge into the same canonical plan model?
2. Do prepared and non-prepared execution share the same runtime contracts?
3. Are scalar, grouped, aggregate, and projection flows using shared owners where semantics are equivalent?
4. Are there old compatibility surfaces, renamed shims, fallback DTOs, or wrapper-only modules that no longer carry real ownership?
5. Are there duplicate implementations of filtering, ordering, grouping, projection, pagination, continuation, or response shaping?
6. Are there repeated conversions between equivalent internal representations?
7. Are execution decisions derived once, or rediscovered in multiple downstream modules?

## Required Evidence Collection

Run and record:

```bash
rg "execute_query_result|execute_prepared|execute_delete_count" crates/icydb-core/src/db/session/query
rg "execute_compiled_sql_owned|execute_compiled_sql_with_cache_attribution|execute_select_compiled_sql_with_cache_attribution|execute_global_aggregate" crates/icydb-core/src/db/session/sql/execute
rg "execute_prepared_scalar|execute_prepared_grouped|execute_group_fold_stage|execute_global_distinct_grouped_fold_stage|execute_generic_grouped_fold_stage|execute_single_grouped_count_fold_stage" crates/icydb-core/src/db/executor
rg "finalize_structural_grouped_projection_result|grouped_next_cursor_boundary|group_key_matches_row_view|borrowed_group_probe_supported" crates/icydb-core/src/db
rg "compat|legacy|shim|fallback|adapter|wrapper" crates/icydb-core/src/db crates/icydb/src/db
rg "clone\\(|cloned\\(|to_vec\\(|to_string\\(" crates/icydb-core/src/db/session/query crates/icydb-core/src/db/session/sql/execute crates/icydb-core/src/db/query crates/icydb-core/src/db/executor
```

Also inspect module sizes:

```bash
find crates/icydb-core/src/db -name '*.rs' -print0 | xargs -0 wc -l | sort -nr | head -40
wc -l crates/icydb-core/src/db/session/query/mod.rs \
  crates/icydb-core/src/db/session/query/execution.rs \
  crates/icydb-core/src/db/session/query/fluent.rs \
  crates/icydb-core/src/db/session/query/cache.rs \
  crates/icydb-core/src/db/session/sql/execute/mod.rs \
  crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/mod.rs \
  crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/page_finalize.rs \
  crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/count/mod.rs \
  crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/distinct.rs \
  crates/icydb-core/src/db/executor/explain/mod.rs
```

## Classification Model

Classify each finding as one of:

* `DuplicateFlow`: same semantic operation implemented in multiple paths
* `LateConvergence`: SQL/Fluent/prepared paths converge later than necessary
* `PolicyRediscovery`: downstream module re-derives a decision already owned upstream
* `ShimResidue`: compatibility/wrapper/adapter remains after its purpose expired
* `ConversionChurn`: repeated equivalent representation conversion
* `HotPathBranching`: runtime branch exists because policy was not frozen earlier
* `OwnershipBlur`: module owns both orchestration and domain mechanics
* `LegitimateSeparation`: similar-looking paths are intentionally distinct

## Risk Scoring

Score each finding from 1 to 10.

Risk factors:

* hot-path impact
* semantic divergence risk
* number of affected query surfaces
* amount of duplicate code
* likelihood of future feature drift
* difficulty of safe removal

Suggested thresholds:

* 1–3: cosmetic or documentation-only
* 4–5: cleanup candidate
* 6–7: architectural follow-up needed
* 8–10: convergence defect or high-risk duplication

## Required Report Sections

Produce a report with:

1. Summary

   * overall convergence risk score
   * top three risks
   * whether SQL/Fluent convergence is acceptable

2. Query Flow Map

   * SQL path
   * Fluent path
   * prepared path
   * executor path
   * projection/finalization path

3. Duplicate Flow Findings

   * exact files/functions
   * duplicated responsibility
   * recommended owner

4. Shim and Compatibility Residue

   * wrappers/adapters/fallbacks found
   * whether each should remain, inline, or be deleted

5. Hot Path Branch/Conversion Findings

   * branch or conversion site
   * why it exists
   * whether policy can be resolved earlier

6. Legitimate Separations

   * similar code that should remain separate
   * reason it is not duplication

7. Recommended Patch Plan

   * safe mechanical extractions first
   * semantic convergence second
   * performance rewrites last

8. Validation Plan

   * targeted tests
   * invariant checks
   * grep checks
   * compile/clippy commands

Use normalized verification statuses:

* `PASS`
* `FAIL`
* `BLOCKED`

Use `PARTIAL` only in findings when evidence is mixed. Verification commands
must resolve to `PASS`, `FAIL`, or `BLOCKED`.

## Output Path

Save recurring definition as:

```text
docs/audits/recurring/crosscutting/crosscutting-flow-convergence.md
```

Save reports as:

```text
docs/audits/reports/YYYY-MM/YYYY-MM-DD/flow-convergence.md
```

Artifacts should go under:

```text
docs/audits/reports/YYYY-MM/YYYY-MM-DD/artifacts/flow-convergence/
```

## Read-Only Run Mode

When asked to run this audit read-only:

* do not modify product code or generated artifacts as a result of findings
* write only the requested audit definition/report updates and optional report
  artifacts
* do not start, stop, reset, or reconfigure external services
* prefer targeted convergence tests and invariant scripts over broad mutation-
  heavy validation
* record checks that require a non-read-only build, staged fixture, or external
  service as `BLOCKED`

## Guardrails

Do not recommend deleting a path unless its callers and semantic coverage are proven.

Do not merge SQL and Fluent source-level APIs. The audit target is internal convergence, not public API collapse.

Do not flatten legitimate specialization paths such as grouped COUNT, DISTINCT, cursor handling, or sparse projection unless they are demonstrably wrapper-only.

Prefer “move policy earlier” over “add runtime abstraction.”

Prefer canonical owner boundaries over generic shared helpers.

## Validation Commands

Read-only baseline:

```bash
make check-invariants
cargo test -p icydb-core --features sql execution_convergence -- --nocapture
cargo test -p icydb-core --features sql explain_cache_convergence -- --nocapture
cargo test -p icydb-core --features sql shared_query_plan_cache_is_reused_by_fluent_and_sql_select_surfaces -- --nocapture
git diff --check
```

After any follow-up product-code patch, add:

```bash
cargo fmt --all
cargo check -p icydb-core --all-targets --features sql
cargo clippy -p icydb-core --all-targets --features sql -- -D warnings
cargo test -p icydb-core --features sql grouped -- --nocapture
cargo test -p icydb-core --features sql sql -- --nocapture
```

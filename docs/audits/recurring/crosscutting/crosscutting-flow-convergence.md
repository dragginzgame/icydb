# Crosscutting Flow Convergence Audit

## Purpose

Audit whether IcyDB query construction, planning, routing, execution, projection, pagination, and response shaping converge through canonical shared paths instead of drifting into parallel SQL/Fluent/runtime implementations.

This audit is concerned with duplicate flows, compatibility shims, redundant adapters, stale execution paths, and places where equivalent query semantics are implemented more than once.

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
rg "SQL|Sql|sql" crates/icydb-core/src/db/session crates/icydb-core/src/db/executor crates/icydb-core/src/db/query
rg "Fluent|fluent" crates/icydb-core/src/db crates/icydb/src/db
rg "prepared|Prepared" crates/icydb-core/src/db/session crates/icydb-core/src/db/executor crates/icydb-core/src/db/query
rg "compat|legacy|shim|fallback|adapter|wrapper" crates/icydb-core/src crates/icydb/src
rg "clone\\(|to_vec\\(|to_string\\(" crates/icydb-core/src/db/session crates/icydb-core/src/db/query crates/icydb-core/src/db/executor
rg "execute_.*stage|finalize|project_.*projection|cursor|continuation" crates/icydb-core/src/db/executor
```

Also inspect module sizes:

```bash
find crates/icydb-core/src/db -name '*.rs' -print0 | xargs -0 wc -l | sort -nr | head -40
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

## Guardrails

Do not recommend deleting a path unless its callers and semantic coverage are proven.

Do not merge SQL and Fluent source-level APIs. The audit target is internal convergence, not public API collapse.

Do not flatten legitimate specialization paths such as grouped COUNT, DISTINCT, cursor handling, or sparse projection unless they are demonstrably wrapper-only.

Prefer “move policy earlier” over “add runtime abstraction.”

Prefer canonical owner boundaries over generic shared helpers.

## Validation Commands

At minimum, after any follow-up patch:

```bash
cargo fmt --all
cargo check -p icydb-core --all-targets
cargo clippy -p icydb-core --all-targets -- -D warnings
cargo test -p icydb-core grouped -- --nocapture
cargo test -p icydb-core sql -- --nocapture
make check-invariants
git diff --check
```

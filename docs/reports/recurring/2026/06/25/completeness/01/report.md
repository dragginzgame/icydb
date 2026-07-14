# Crosscutting Completeness Audit - 2026-06-25

## Report Preamble

- scope: current public single-entity SQL/query/mutation boundary, using
  `docs/contracts/SQL_SUBSET.md` as the boundary authority
- audit definition:
  `docs/audits/recurring/crosscutting/crosscutting-completeness.md`
- compared baseline report path:
  `docs/audits/reports/2026-05/2026-05-01/completeness.md`
- code snapshot identifier: `d389eec3bd` (`dirty` working tree)
- run timestamp: `2026-06-25T10:37:14+02:00`
- method tag/version: `Completeness Method V2`
- comparability status: `method refresh`
  - the May report used `Completeness Method V1`
  - the current SQL contract now includes broader DDL, public update,
    `RETURNING`, blob, expression-index, and introspection proof surfaces
  - feature-state movement in this report should be compared against future V2
    reports, not treated as a direct regression from V1
- run mode: read-only
  - product code, generated artifacts, release metadata, manifests, and lockfiles
    were not modified for this audit run
  - only the audit definition and this report were updated
  - no external service was started or stopped

The working tree was already dirty from active grouped-runtime, prepared/explain,
changelog, and design-status work. Those changes were inspected only as current
snapshot context and were not treated as stable feature-state evidence unless
covered by passing checks below.

## Executive Summary

Audit verdict: `PASS` for the current documented boundary.

The completeness read remains **bounded and coherent**. The public SQL contract
is materially broader than the May V1 report, but the broader surface still has
clear fail-closed boundaries and focused proof across read, write, DDL, blob,
prepared, cache, explain, and convergence paths.

No large in-scope feature family appears to be absent. The main classification
change is methodological: V2 splits newer contract families into explicit rows
instead of hiding them under broad mutation or query rows.

The highest drift risk is not a missing runtime path. It is keeping the expanded
SQL contract, DDL publication tests, generated-canister update policy, and
prepared/explain identity proof in sync as the active dirty work lands.

## System Boundary

Included:

- public SQL `SELECT`, `EXPLAIN`, introspection, and supported single-entity
  read shapes
- public SQL `INSERT`, `UPDATE`, `DELETE`, and narrow `RETURNING`
- SQL DDL for supported schema and index transitions, including expression
  indexes and supported `ALTER TABLE` forms
- blob literals and values where the SQL contract admits them
- typed/fluent surfaces where they confirm the same semantic boundary
- prepared SQL, compiled execution, cache identity, explain, and diagnostics
  where those paths are required for completeness

Excluded:

- multi-entity SQL, joins, subqueries, windows, and general relational SQL
- scalar SQL cursor pagination
- full SQL expression language beyond the admitted expression families
- unsupported blob ordering and unsupported widened blob operators
- unsupported index-order families such as SQL `DESC` index publication
- generated canister update widening beyond configured update policy
- live canister or network-dependent proof in this read-only run

## Feature Inventory

### Primary Feature Rows

| Feature Row | State | Readout |
| ---- | ---- | ---- |
| scalar `SELECT` | Complete | Strong admitted surface, lowering, planning, execution, explain, and proof within the single-entity boundary |
| grouped `SELECT` | Bounded | Strong grouped execution and explain inside the admitted family; unsupported grouped shapes remain fail-closed |
| predicates (`WHERE` / `HAVING`) | Bounded | Scalar and grouped predicate families are proven across convergence tests, but remain deliberately scoped |
| projection expressions | Bounded | Computed projections are real runtime behavior, but the expression language is intentionally narrower than full SQL |
| aggregates | Bounded | Grouped and global aggregate paths are strong inside the admitted aggregate family |
| `ORDER BY` | Bounded | Ordering is strong for admitted scalar/grouped values; blob ordering and other unsupported shapes are explicit rejections |
| `LIMIT` / `OFFSET` | Complete | Complete for the admitted scalar SQL windowing surface; cursor pagination is out of scope |
| `DISTINCT` | Bounded | Proven for supported query families, not a generalized SQL distinct framework |
| mutation and `RETURNING` | Bounded | Public write lane admits supported `INSERT`, `UPDATE`, `DELETE`, and narrow `RETURNING`; broader write shapes reject cleanly |
| SQL DDL | Bounded | DDL publication covers supported schema/index transitions, expression indexes, uniqueness, and ALTER forms, with unsupported forms rejected before publication |
| blob values | Bounded | Blob INSERT/UPDATE/SELECT/DELETE RETURNING, equality, and `OCTET_LENGTH` are supported; broader blob semantics stay outside the contract |
| `EXPLAIN` and introspection | Complete | Public diagnostics and introspection remain coherent for the supported SQL boundary |

### Supporting Rows

| Supporting Row | State | Readout |
| ---- | ---- | ---- |
| prepared SQL | Bounded | Compiled execution preserves supported read families; prepared widening beyond current lanes remains out of scope |
| semantic identity / canonicalization | Bounded | Strong for shipped scalar, grouped, predicate, and expression-index families, but not generalized |
| cache / reuse | Bounded | Cache and explain convergence pass for representative route-owned plans |
| expression-engine authority | Complete | Runtime scalar expression materialization continues to route through the compiled expression authority |
| fail-closed boundaries | Complete | Unsupported public SQL shapes generally reject before mutation or publication |
| proof surface | Bounded | Focused read-only proof is strong; live canister proof was outside this run |

## Pipeline Completeness

| Area | Surface | Lowering / Identity | Planning / Execution | Explain / Diagnostics | Proof |
| ---- | ---- | ---- | ---- | ---- | ---- |
| SQL reads | Strong | Strong | Strong | Strong | Strong |
| predicates and projections | Strong | Strong | Strong | Strong | Strong |
| grouped and aggregate reads | Bounded | Strong | Strong | Strong | Strong |
| public writes and `RETURNING` | Bounded | Strong | Strong | Strong | Strong |
| SQL DDL publication | Bounded | Strong | Strong | Strong | Strong |
| blob values | Bounded | Strong | Strong | Strong | Strong |
| prepared/cache paths | Bounded | Strong | Strong | Strong | Strong |
| live canister policy | Bounded | N/A for this run | N/A for this run | N/A for this run | BLOCKED |

## Delta Vs May V1

### 1. Boundary expanded, not product completeness regressed

The May report already found no large missing in-scope families. That remains
true, but V2 makes the expanded contract explicit:

- SQL DDL now has its own completeness row
- public SQL update and `RETURNING` are not hidden under a generic mutation row
- blob values have a bounded feature row
- expression indexes and ALTER TABLE metadata updates are direct proof targets
- introspection remains part of the public contract readout

### 2. The current state is still bounded and coherent

The expanded families are not broad SQL compatibility claims. They are
documented, bounded IcyDB contracts with focused rejections for unsupported
forms.

### 3. Dirty active work does not change the audit verdict

The dirty grouped, prepared/explain, and design-status work increases review
attention for future runs, but the focused checks passed on the current
snapshot. No contradiction was found between the documented contract and the
validated paths.

## Partial / Bounded Areas

### 1. SQL DDL is deliberately scoped

DDL supports the current schema/index publication contract, including expression
indexes and selected ALTER TABLE transitions. It does not claim general SQL DDL.

### 2. Mutation policy is split by surface

The public session/library write lane supports the documented update family.
Generated canister update behavior remains governed by configured update policy,
so generated canister widening should be audited separately before claiming
broader product completeness.

### 3. Blob support is useful but narrow

Blob values are supported for the documented literal, storage, returning,
equality, and byte-length cases. Ordering and broader blob operators are
correctly outside the current contract.

### 4. Expressions remain intentionally bounded

Projection, predicate, aggregate, searched CASE, and supported function families
are real runtime behavior, but they are not a promise of full SQL expression
coverage.

### 5. Prepared and cached execution are path-owned

Prepared and cached execution are coherent for shipped read families and route
identity, but the audit should continue to keep prepared widening separate from
regular read-surface completeness.

## Missing In-Scope Areas

No large feature family appears missing inside the current documented boundary.

No validation check produced a failing contradiction between the public SQL
contract and the current implementation proof.

## Out-Of-Scope Areas

- joins, subqueries, windows, and multi-entity SQL
- general SQL DDL beyond the documented schema/index transitions
- general SQL expression compatibility
- scalar SQL cursor pagination
- unsupported blob ordering and widened blob operators
- SQL `DESC` index-order publication
- generated canister update widening beyond configured policy
- live canister proof during this read-only run

## Architectural Seams

### 1. Contract breadth now depends more heavily on SQL_SUBSET discipline

Because the public boundary is broader, future completeness reports should start
from `docs/contracts/SQL_SUBSET.md` and then validate code/tests against it.
Implementation-only discovery is now too likely to confuse hidden capability
with public contract.

### 2. Generated canister update policy needs separate proof when widened

The split between public session/library update support and generated canister
update policy is coherent, but it is a recurring source of possible
misclassification.

### 3. Prepared/explain active work should be rechecked after landing

The current dirty tree includes prepared/explain edits. The focused convergence
checks pass, but the next V2 run should confirm whether those edits changed
cache, explain, or prepared completeness labels.

### 4. V2 taxonomy should be kept stable

DDL, blob, update, introspection, and prepared/cache rows should remain explicit
in future reports so headline movement reflects product changes rather than
taxonomy drift.

## Verification Readout

| Check | Status | Result |
| ---- | ---- | ---- |
| Audit definition self-review | PASS | Definition updated to V2 identity, read-only mode, verification statuses, and concrete baseline |
| SQL contract inspection | PASS | Current boundary inspected from `docs/contracts/SQL_SUBSET.md` |
| `make check-invariants` | PASS | All invariant scripts passed |
| `query_lowering` focused test filter | PASS | 11 tests passed |
| `predicate_convergence` focused test filter | PASS | 7 tests passed |
| `execution_convergence` focused test filter | PASS | 13 tests passed |
| `explain_cache_convergence` focused test filter | PASS | 2 tests passed |
| `sql_blob` focused test filter | PASS | 9 tests passed |
| public read-surface representative test | PASS | 1 test passed |
| compiled read-family representative test | PASS | 1 test passed |
| public update representative test | PASS | 1 test passed |
| expression-index DDL representative test | PASS | 1 test passed |
| expression-index rename metadata representative test | PASS | 1 test passed |
| `git diff --check` | PASS | No whitespace errors reported |
| live canister / network-dependent proof | BLOCKED | Not run under read-only constraints; no service was started or stopped |

## Recommended Next Steps

1. Keep future completeness reports on `Completeness Method V2` so the new DDL,
   blob, update, introspection, and prepared/cache rows remain comparable.
2. When the dirty prepared/explain work lands, rerun the V2 baseline and confirm
   cache/explain/prepared labels still hold.
3. If generated canister update policy is widened, add a dedicated proof row
   rather than folding it into the public session/library update row.

# IcyDB Feature Inventory and Completeness Audit

Historical note: this archive audit is pinned to the `0.56.1` checkout named
below. It does not describe the current SQL surface.

Later lines widened the admitted SQL language beyond what this audit reports,
including reduced SQL writes and alias support. For the current SQL contract,
see:

- `docs/contracts/SQL_SUBSET.md`
- `docs/architecture/sql-surface-mapping.md`

Date: 2026-03-16  
Scope: current checkout only (`workspace.version=0.56.1` in `Cargo.toml`)

## 1. Executive Summary

- IcyDB is in a high-capability phase where core data/runtime/query surfaces are already broad, and the dominant remaining work is closure of scoped SQL/optimizer subsets rather than foundational feature invention.
- Biggest shipped families:
  - typed entities/indexes + filtered indexes + expression-index foundations
  - fluent load/delete/grouped/aggregate execution
  - reduced SQL (query/projection/grouped/aggregate/explain)
  - introspection and canister SQL dispatch (`DESCRIBE`, `SHOW INDEXES`, `SHOW ENTITIES`, unified `SqlQueryResult`)
  - observability (`EXPLAIN`, trace, metrics)
  - survivability/recovery/fail-closed contracts
- Biggest remaining gaps:
  - SQL writes (`INSERT`, `UPDATE`) are explicitly out of scope
  - expression-index eligibility is intentionally narrow (`LOWER(field)` casefold Eq/In subset)
  - grouped SQL `HAVING` is constrained (`AND` clauses only; no broader boolean/expression families)
  - grouped fast-path optimization family remains deferred
- Maturity read: mostly hardening/closure mode, with a few meaningful product/runtime gaps for `0.57`.

## 2. Feature Taxonomy

### A. Core Data Model / Storage
- Entity modeling
- Index modeling (field, filtered, expression key-items)
- Persistence + recovery
- Schema validation + metadata

### B. Query Semantics
- Scalar load
- Delete / mutation
- Aggregate terminals (global + grouped)
- Grouped query semantics
- Projection
- Ordering/pagination/continuation
- Predicate families

### C. Planner / Access Path Capabilities
- Primary key and by-ids access
- Index prefix/range and multi-lookup
- Filtered-index implication gating
- Expression-index eligibility
- Order satisfaction / route contracts

### D. SQL Surface
- Reduced parser/lowering/session
- Projection/grouped/global aggregate SQL
- EXPLAIN SQL
- Introspection SQL (`DESCRIBE`, `SHOW INDEXES`, `SHOW ENTITIES`)
- Generated canister SQL dispatch

### E. Optimizations / Fast Paths
- `COUNT` fast paths
- `EXISTS` covering paths
- `BYTES` / `bytes_by`
- Covering/index-only projection paths
- ORDER BY + LIMIT seek and bounded windows
- Grouped fast-path/deferred expansions

### F. Observability / Diagnostics
- EXPLAIN descriptor/text/json/verbose
- Trace and execution metrics
- Schema/index introspection helpers
- SQL dispatch diagnostics

### G. Architecture / Hardening Lines
- semantic-authority boundaries
- continuation envelope stability
- load-pipeline containment
- invariant scripts/guards
- fail-closed parity and regression matrixing
- integration harness wiring

## 3. Implemented Feature Inventory

| Bucket | Feature | Status | User-visible? | Evidence | Basis |
|---|---|---|---|---|---|
| A | Typed entities + macro/index modeling | complete | yes | `README.md` (`typed entities and indexes`), `crates/icydb-core/src/model/index.rs` | Core public model is present and exercised across docs/tests. |
| A | Filtered/conditional indexes | complete | yes | `docs/changelog/0.54.md` (`0.54.0`) | Full metadata + planner implication + mutation/recovery parity landed. |
| A | Expression index foundations | partial | yes | `crates/icydb-core/src/model/index.rs` (`IndexExpression`, `supports_text_casefold_lookup`), `docs/changelog/0.55.md` (`0.55.2`, `0.55.3`) | Foundation is shipped but eligibility intentionally scoped. |
| A | Stable-memory persistence + guarded recovery | mostly complete | yes | `README.md` (`stable-memory persistence`), `CHANGELOG.md` (`0.53.x`) | Shipped and robust; ongoing hardening is mostly non-feature. |
| A | Schema metadata introspection payloads | complete | yes | `README.md` (`Describe/ShowIndexes/ShowEntities` payloads), `crates/icydb/src/db/sql/mod.rs` renderers | Typed schema/index/entity metadata surfaces are shipped. |
| B | Fluent scalar load/delete APIs | complete | yes | `README.md` fluent query examples and notes | Core day-to-day query semantics are shipped. |
| B | Grouped + aggregate fluent execution | mostly complete | yes | `README.md`, `docs/contracts/SQL_SUBSET.md` grouped/aggregate constraints | Broadly shipped with explicit grouped/global constraints. |
| B | Ordering/pagination/continuation contracts | complete | yes | `README.md` continuation notes, `docs/changelog/0.41.md` route/window work | Deterministic ordered paging and continuation behavior is established. |
| B | Predicate family support (strict subset + fail-closed) | mostly complete | yes | `docs/contracts/SQL_SUBSET.md`, planner tests/changelog references | Strong predicate support exists with explicit gating for out-of-scope shapes. |
| C | PK / by-ids / index prefix/range/multi-lookup | complete | internal/runtime | `docs/changelog/0.41.md` (`IndexMultiLookup`, `TopNSeekSpec`, range/order contracts) | Access-path families are shipped and contract-tested. |
| C | Filtered-index eligibility (`query => index predicate`) | complete | internal/runtime | `docs/changelog/0.54.md` implication gating | Planner/runtime eligibility is implemented end-to-end. |
| C | Expression-index planner/runtime eligibility | partial | internal/runtime | `docs/changelog/0.55.md` (`LOWER(field)` Eq/In subset, range-family excluded) | Implemented but intentionally narrow and fail-closed outside subset. |
| C | Order satisfaction and route execution contracts | mostly complete | internal/runtime | `docs/changelog/0.41.md`, `0.44.md` | Mature route contracts; remaining work is optimization breadth. |
| D | Reduced parser/lowering/session SQL surfaces | complete | yes | `README.md` reduced SQL section, `docs/contracts/SQL_SUBSET.md` | Production subset is explicit and shipped. |
| D | SQL projection/grouped/global aggregate lanes | mostly complete | yes | `docs/contracts/SQL_SUBSET.md` (executable + constrained) | Implemented with clear constrained families and dedicated entrypoints. |
| D | SQL introspection lanes (`DESCRIBE`, `SHOW INDEXES`, `SHOW ENTITIES`) | mostly complete | yes | `docs/changelog/0.56.md` (`0.56.0`-`0.56.3`), parser/lowering/session code | Dedicated lanes are shipped; breadth of show-style commands is still narrow. |
| D | Generated canister SQL dispatch (`SqlQueryResult`) | complete | yes | `docs/changelog/0.56.md` (`0.56.2`), `crates/icydb-build/src/db.rs`, `scripts/dev/sql.sh` | Unified dispatch/result envelope is in place and tested. |
| E | `COUNT` optimization family | mostly complete | user-visible perf | `docs/changelog/0.41.md`, `docs/changelog/0.44.md` | Strong coverage across safe shapes; edge expansions are mostly follow-up. |
| E | `EXISTS` covering/index-only fast paths | mostly complete | user-visible perf | `docs/changelog/0.44.md` (`0.44.1`) | Expanded substantially; still intentionally conservative on some shapes. |
| E | `BYTES` / `bytes_by` terminals + fast paths | partial | user-visible perf | `docs/changelog/0.43.md`, `docs/design/archive/0.44-optimisation-closure/0.44-status.md` | Shipped with explicit deferred shape families. |
| E | Grouped bounded fast paths | planned only | internal/runtime | `docs/design/archive/0.44-optimisation-closure/0.44-status.md` (`Deferred`) | Explicitly deferred, not absent by accident. |
| F | EXPLAIN (text/json/verbose) | complete | yes | `docs/changelog/0.42.md`, `README.md` | Core explain surfaces are stabilized and shipped. |
| F | Trace/metrics execution observability | complete | yes | `README.md` observability sections, `docs/changelog/0.41.md` | Public diagnostics surfaces exist and are documented. |
| F | SQL dispatch diagnostics and deterministic errors | mostly complete | yes | `docs/changelog/0.54.md`, `0.56.md` | Deterministic SQL entity/lane diagnostics are present. |
| G | Semantic-authority + DRY recurring audits | mostly complete | internal | `docs/audits/reports/2026-03/2026-03-15/summary.md` | Recurring governance is active with moderate residual risk only. |
| G | Continuation/pipeline containment work | mostly complete | internal | `docs/changelog/0.55.md` (`0.55.5`, `0.55.6`) | Ongoing containment/hardening line, mostly cleanup not feature gap. |

## 4. Missing / Partial Features

| Bucket | Feature | Current State | Why Not Complete | Evidence | Priority |
|---|---|---|---|---|---|
| D | SQL writes (`INSERT`, `UPDATE`) | missing | Explicitly out-of-scope in current reduced SQL contract | `README.md` out-of-scope list; `docs/contracts/SQL_SUBSET.md` out-of-scope list; parser unsupported keywords in `crates/icydb-core/src/db/sql/parser/mod.rs` | high |
| D | Broader show-style SQL introspection (`SHOW TABLES`, etc.) | missing | Parser intentionally supports only `SHOW INDEXES`/`SHOW ENTITIES` | `crates/icydb-core/src/db/sql/parser/mod.rs` (`SHOW commands beyond SHOW INDEXES/SHOW ENTITIES`) | medium |
| D/B | Rich `HAVING` boolean/expression family (`OR`, `NOT`, broader expressions) | partial | Grouped HAVING is intentionally constrained | `docs/contracts/SQL_SUBSET.md` (`AND` only; `OR`/`NOT` fail-closed) | medium |
| C | Expression-index range/starts-with support | partial | Current expression lookup eligibility intentionally limited | `docs/changelog/0.55.md` (`range-family excluded`), `docs/design/0.55-expression-indexes/0.55-status.md` | high |
| E | Grouped fast-path optimization family | planned only | Explicitly deferred to avoid route complexity drift | `docs/design/archive/0.44-optimisation-closure/0.44-status.md` (`Grouped ... Deferred`) | medium |
| E | Remaining BYTES/index-only optimization breadth | partial | Ordered/residual/distinct and related families intentionally fallback | `docs/design/archive/0.44-optimisation-closure/0.44-status.md` (BYTES deferred notes) | medium |
| D/F | EXPLAIN over introspection lanes (`EXPLAIN DESCRIBE`, `EXPLAIN SHOW INDEXES`) | missing by scope | Current contract explicitly rejects these | `docs/contracts/SQL_SUBSET.md` (`EXPLAIN DESCRIBE`/`EXPLAIN SHOW INDEXES` out of scope) | low |
| Tooling | PocketIC full run requires explicit `POCKET_IC_BIN` | partial | Intentional harness hardening; no implicit download path | `docs/changelog/0.55.md` (`0.55.4`), integration tests env guard | low |

## 5. Features vs Hardening

| Item | Class | Why |
|---|---|---|
| SQL `INSERT`/`UPDATE` reduced-surface support | feature | New user-visible capability, not cleanup. |
| Expression-index eligibility expansion beyond current subset | feature | New planner/runtime behavior family. |
| Additional introspection commands beyond current show/describe set | feature | New user-visible SQL introspection scope. |
| Grouped fast-path expansions | optimization | Performance/route breadth, grouped semantics already exist. |
| BYTES residual-shape expansion | optimization | Performance closure; terminal already exists. |
| Explain visibility for deferred projection families | optimization | Better diagnostics for existing behavior. |
| Continuation envelope consolidation/pipeline containment | hardening | Risk reduction and ownership clarity, not new product surface. |
| Recurring semantic-authority/DRY audits | hardening | Governance and regression prevention. |
| Legacy alias removals/module splits | cleanup | Internal clarity without capability gain. |

## 6. Release-Line Read (0.41 / 0.42 / 0.43 / 0.44 / 0.54 / 0.55)

- `0.41`: planner/access-route optimization and pre-EXPLAIN observability closure (`COUNT/EXISTS` fast paths, ordered seek/top-N route contracts, metrics/trace/schema helpers) in `docs/changelog/0.41.md`.
- `0.42`: EXPLAIN line (`text/json/verbose`) and descriptor stability hardening in `docs/changelog/0.42.md`.
- `0.43`: `bytes()` and `bytes_by(field)` terminals plus audit-cycle kickoff; later patches in this line are mostly boundary cleanup in `docs/changelog/0.43.md`.
- `0.44`: optimization closure for safe COUNT/BYTES/covering families with explicit deferred rows (grouped fast paths and some explain visibility) in `docs/changelog/0.44.md` and `docs/design/archive/0.44-optimisation-closure/0.44-status.md`.
- `0.54`: filtered/conditional indexes end-to-end (`0.54.0`) plus generated canister SQL dispatch baseline (`0.54.1`) in `docs/changelog/0.54.md`.
- `0.55`: expression-index foundations + hardening/composition/recovery parity, plus continuation/pipeline/semantic-authority containment work in `docs/changelog/0.55.md`.

## 7. What Is Actually Missing?

### User-facing gaps

1. Reduced SQL writes (`INSERT`, `UPDATE`) are still absent.
2. Broader introspection command family (beyond `DESCRIBE`, `SHOW INDEXES`, `SHOW ENTITIES`) is not present.
3. Grouped SQL `HAVING` remains intentionally constrained.

### Planner/runtime gaps

1. Expression-index eligibility breadth (range-family, starts-with, additional deterministic expression kinds).
2. Deferred grouped route optimization families.

### SQL/introspection gaps

1. No explainability for introspection lanes (`EXPLAIN DESCRIBE`, `EXPLAIN SHOW INDEXES`) by current contract.
2. Show-style coverage remains narrow.

### Optimization gaps

1. Deferred grouped fast paths.
2. Remaining BYTES/index-only coverage in deferred shape families.
3. Deferred projection-node explain visibility.

### Non-feature hardening backlog

1. Continue recurring crosscutting audits and ownership containment cycles (already active, moderate residual risk bands).

## 8. Recommended Next Steps (Aligned to Your Plan)

### `0.56` finish introspection (close this line)

1. Freeze introspection lane closure as shipped in `0.56.3`:
   - parser/lowering/session/generated dispatch all treat `SHOW ENTITIES` as first-class,
   - grouped `HAVING` remains constrained by explicit fail-closed contract.
2. Keep `0.56` release docs consistent:
   - root `CHANGELOG.md` summary bullets (`0.56.0`-`0.56.3`),
   - `docs/changelog/0.56.md` detailed breakdown + validation list,
   - `docs/contracts/SQL_SUBSET.md` scope matrix.

### `0.57` approach missing capabilities (priority order)

1. **Best small feature**: add one new show-style introspection command with explicit narrow contract (for example `SHOW TABLES`/equivalent entity listing alias) if desired.
2. **Best medium feature**: expand expression-index eligibility from current `LOWER(field)` Eq/In subset toward one additional safe family.
3. **Best finish-partial item**: broaden grouped `HAVING` shape support in controlled increments while preserving fail-closed boundaries.
4. **Best non-feature hardening item**: carry forward recurring semantic-authority and DRY audits while `0.57` features land.

---

## Evidence Index

- `README.md`
- `CHANGELOG.md`
- `docs/changelog/0.41.md`
- `docs/changelog/0.42.md`
- `docs/changelog/0.43.md`
- `docs/changelog/0.44.md`
- `docs/changelog/0.54.md`
- `docs/changelog/0.55.md`
- `docs/changelog/0.56.md`
- `docs/contracts/SQL_SUBSET.md`
- `docs/design/archive/0.44-optimisation-closure/0.44-status.md`
- `docs/design/0.55-expression-indexes/0.55-status.md`
- `docs/audits/reports/2026-03/2026-03-15/summary.md`
- `crates/icydb-core/src/db/sql/parser/mod.rs`
- `crates/icydb-core/src/model/index.rs`
- `crates/icydb-core/src/db/session/sql.rs`
- `crates/icydb/src/db/sql/mod.rs`
- `crates/icydb-build/src/db.rs`
- `scripts/dev/sql.sh`

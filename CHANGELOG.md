# IcyDB Changelog

All notable, and occasionally less notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [0.15.0] – 2026-02-19 - Ordered Key Stream Abstraction

### 📝 Summary

* Completed the `0.15` internal ordered-key stream milestone by moving key collection behind one shared stream interface.
* This is an internal cleanup release. Query results, cursors, pagination behavior, explain output, and metrics behavior are unchanged.

```rust
pub(crate) trait OrderedKeyStream {
    fn next_key(&mut self) -> Result<Option<DataKey>, InternalError>;
}
```

### 🔧 Changed

* Added internal `OrderedKeyStream` and `VecOrderedKeyStream` so load execution uses one key-stream model.
* Updated query key production to return stream producers (`produce_key_stream(...)`) instead of exposing direct key vectors.
* Updated load execution (normal path and fast paths) to read keys from streams through one shared row-loading path.
* Kept responsibilities in the same places as before: cursor resume/validation logic stays in cursor and range helpers, and filtering/sorting/paging still runs in post-access execution.

### 🧪 Testing

* Added focused stream abstraction unit coverage for deterministic key order and exhaustion behavior.
* Added explicit regression coverage that duplicate `by_ids(...)` input keys are still de-duplicated with stream-backed key production.

### 📚 Documentation

* Added `docs/status/0.15-status.md` and marked the `0.15` completion matrix as complete with verification notes.

---

## [0.14.2] – 2026-02-18 - Execution Phase Alignment

### 🧹 Cleanup

* Aligned the PK fast path with the same execution phase structure used by other load paths: ordered key iteration, row fetch, then shared row deserialization.
* Removed the PK-only inline decode loop so fast-path execution is easier to reason about and less likely to drift from the shared load pipeline.

### 🔧 Changed

* This is a structural cleanup only; query behavior and pagination semantics remain unchanged.

---

## [0.14.1] – 2026-02-18 - Validation Unification I

### ➕ Added

* Added a minimal, opt-in load execution trace surface in `icydb-core` for cursor-paged queries via `PagedLoadQuery::execute_with_trace()`.

### 🧹 Cleanup

* Cleaned up cursor checks so the same validation rules are applied in one place, with runtime failures treated as internal invariant issues.
* Kept plan-shape checks at planning time and reduced duplicate runtime re-checks to lightweight safety guards.
* Simplified store-side lookup checks so store code focuses on safe reads and decode integrity instead of user input rules.
* Simplified pushdown checks so they only decide optimization eligibility, not query validity.
* Standardized boundary-arity runtime failures as `InvariantViolation` instead of `Unsupported`.

### 🔧 Changed

* Trace output is debug-only and semantics-neutral: it reports access path variant, direction, pushdown and fast-path decisions, keys scanned, rows returned, and whether continuation was applied.

---

## [0.14.0] – 2026-02-18 - DESC Support Complete

### 📝 Summary

* Completed the `0.14` single-path `IndexRange` DESC milestone end-to-end, including planner direction derivation, reverse index-range traversal, directional continuation advancement, and cursor direction validation.
* Closed the `0.14` completion audit with passing workspace gate checks and a fully completed status matrix in `docs/status/0.14-status.md`.

```rust
let page1 = session
    .load::<PhaseEntity>()
    .order_by_desc("rank")
    .limit(20)
    .execute_paged()?;

if let Some(cursor) = page1.next_cursor.clone() {
    let page2 = session
        .load::<PhaseEntity>()
        .order_by_desc("rank")
        .limit(20)
        .cursor(cursor)
        .execute_paged()?;
}
```

### 🔧 Changed

* Wired executable direction selection from canonical `ORDER BY` direction across plan shapes, so execution now carries `Direction::Desc` whenever the leading ordered field is descending.
* Activated reverse store traversal for single-path `IndexRange` DESC execution by iterating raw-key ranges in reverse while preserving the same canonical bound envelope.
* Made continuation advancement checks direction-aware in index range scans (`candidate > anchor` for ASC, `candidate < anchor` for DESC).
* Removed ASC-only gating from secondary-order pushdown eligibility by accepting direction-uniform order specs (`Asc`-uniform or `Desc`-uniform) instead of only ascending specs.
* Enabled descending PK fast-path scans for PK-ordered load plans (`ORDER BY id DESC`) while preserving the same cursor-boundary semantics and fallback parity.
* Enabled descending `IndexRange` limit pushdown for direction-uniform DESC order specs (for example `ORDER BY tag DESC, id DESC`) while keeping mixed-direction shapes on safe fallback paths.

### 🧪 Testing

* Added explicit DESC `IndexRange` edge-case coverage for boundary resume behavior (upper-anchor continuation, lower-boundary exhaustion, and single-element range exhaustion).
* Added explicit multi-page DESC continuation coverage (`E,D` -> `C,B` -> `A`) with no-duplicate and no-omission assertions.
* Added full-result directional symmetry coverage asserting `reverse(ASC) == DESC` on a single-field `IndexRange` dataset.
* Added full-result directional symmetry coverage for composite and unique `IndexRange` paths, asserting `reverse(ASC) == DESC` on deterministic datasets.
* Added explicit DESC continuation coverage for duplicate tie-groups under mixed envelopes (`> lower`, `<= upper`) for both single-field and composite `IndexRange` paths.
* Confirmed duplicate-group DESC ordering keeps canonical PK tie-break stability within equal order values, and validated this alongside DESC continuation edge cases.
* Added descending secondary-order pushdown eligibility coverage for explicit PK-desc tie-break ordering.
* Added descending PK fast-path parity coverage against non-fast execution paths.
* Added descending `IndexRange` limit-pushdown trace coverage for direction-uniform DESC plans.

### 📚 Documentation

* Added `docs/status/0.14-status.md` with milestone-alignment progress, current risk points, and next implementation checkpoints.
* Clarified 0.14 design symmetry policy for duplicate groups: DESC preserves canonical PK tie-break order within equal-value groups, while strict `reverse(ASC) == DESC` assertions apply to deterministic non-duplicate datasets.

---

## [0.13.3] – 2026-02-18 - Audits & DESC Preparation

### 📝 Summary

* Prepared the query/execution stack for future DESC support without enabling DESC behavior, while preserving current ASC semantics.
* Completed an initial audit baseline pass and documented outcomes under `docs/audits` and `docs/audit-results`.

### 🔧 Changed

* Added execution-layer `Direction` plumbing (currently `Asc` only) so ordering direction is carried as data without expanding `AccessPath` variants.
* Centralized cursor continuation range rewrites into one helper (`resume_bounds`) and centralized raw-anchor envelope validation into one helper (`anchor_within_envelope`).
* Added a store traversal containment point (`index_range_stream(bounds, direction)`) and threaded direction through planner cursor validation, executor paging, and continuation token encoding.
* Cursor tokens now include direction (`Asc` for now) to keep wire format ready for future DESC execution support without changing current behavior.
* Reduced non-test `AccessPath`/`AccessPlan` branch fan-out by moving dispatch into enum impl methods for planner normalization, canonical ordering, projection, debug summaries, and executor access-plan execution.
* Standardized error-construction discipline so constructor helpers live on owning error types (`impl` associated functions) instead of free-floating `fn ... -> *Error` helpers.
* Moved plan, cursor, relation, index, and predicate error helper constructors into their owning error types while preserving variant payloads and classification semantics.

### 🩹 Fixed

* Kept encoded cursor-token validation strict for `IndexRange` resumes, but restored boundary-only resume support for executor-internal planned cursors so manual continuation boundaries continue to work.
* Restored stable executor invariant messages for PK cursor boundary failures (missing slot, type mismatch, and arity mismatch) after cursor-spine revalidation.
* Resolved the pagination regressions introduced by cursor-spine consolidation; `cargo test -p icydb-core --lib` now passes again.

### 🧪 Testing

* Re-ran targeted pagination regressions plus full library tests after cursor and dispatch containment changes.
* Baseline audit sweep completed across the current tracks:

```text
cursor ordering / boundary semantics
complexity
error taxonomy
invariant preservation
complexity accretion
dry consolidation
```

---

## [0.13.2] – 2026-02-18 - docs/audits

### 📝 Summary

* This release starts a first-pass audit sweep across each defined audit track to establish a consistent baseline before deeper follow-up passes.

### 🧹 Cleanup

* Restored generated companion type names to stable entity-prefixed forms (`TypeView`, `TypeCreate`, `TypeUpdate`) while keeping them in `<type>_views` submodules, avoiding frontend-facing numeric disambiguation names.

### 🥾 Governance

* Expanded the audit framework with additional audit definitions and a runnable guide (`docs/audits/AUDIT-HOWTO.md`) to make review and release checks more repeatable.

### 📚 Documentation

* Reorganized long-form docs into clearer `docs/contracts`, `docs/meta`, and `docs/archive` sections to make it easier to find normative contracts vs reference material.

---

## [0.13.1] – 2026-02-17

### 🧹 Cleanup

* Reduced duplication in `db/executor/tests/pagination.rs` by introducing shared local helpers for pushdown parity, page collection, limit matrices, and boundary resume checks.
* Simplified pagination test setup and repeated assertions with shared helpers (`setup_pagination_test`, ID extraction, cursor-boundary decode), which makes the file easier to extend for 0.14 DESC coverage.
* Split entity integrity decode from storage codec policy by moving key-validation decode logic into `db::entity_decode` (`decode_and_validate_entity_key`).
* Kept behavior and error classification stable while clarifying boundaries: `db::codec` now stays focused on storage decode policy, and executor/entity key checks live in a dedicated integrity layer.

### 🥾 Governance

* Added sample audit documents under `docs/` to make architecture and error-taxonomy review workflows easier to follow and repeat.

---

## [0.13.0] – 2026-02-17 - IndexRange LIMIT Pushdown

### 📝 Summary

* `0.13.0` focuses on `LIMIT` pushdown for `AccessPath::IndexRange` to stop scans earlier without changing query semantics.
* It keeps 0.12 cursor behavior intact while reducing unnecessary traversal for large range windows.
* Result ordering, continuation behavior, and plan semantics remain unchanged.

### 🔧 Changed

* Added a limited index-range resolver in `IndexStore` and kept the existing range resolver behavior by delegating through the same path.
* Added executor wiring to use limited range traversal for eligible `IndexRange` + `LIMIT` plans.
* Kept eligibility conservative (no residual predicate, compatible order shape) to avoid semantic drift while rollout is in progress.
* Eligible `IndexRange` plans with `limit=0` now short-circuit without scanning index entries.

### 🧪 Testing

* Added limit-matrix pagination tests for single-field and composite `IndexRange` paths.
* Covered `limit=0`, `limit=1`, bounded page sizes, and larger-than-result windows.
* Verified paginated collect-all results still match unbounded execution order and remain duplicate-free.
* Added exact-size and terminal-page assertions to confirm continuation cursors are suppressed when paging is complete.
* Added a trace assertion for eligible `IndexRange` + `LIMIT` plans to verify access-phase row scans are capped to `offset + limit + 1`.
* Added a trace assertion that `limit=0` eligible plans report zero access-phase rows scanned.
* Added explicit `limit=0` + non-zero `offset` coverage to verify the same zero-scan behavior.
* Isolated executor trace tests with thread-local event buffers so trace assertions stay deterministic under parallel test execution.

### 🧭 Migration Notes

* No public API migration required.
* Binary cursor envelope work is explicitly out of scope for `0.13` and deferred to a later milestone.

---

## [0.12.0] – 2026-02-17 - Cursor Pagination

### 📝 Summary

* `0.12.0` completes the `IndexRange` cursor hardening work.
* Continuation now resumes from exact raw index-key position, stays inside original range bounds, and keeps page traversal deterministic.
* This reduces duplicate/skip risk and makes pagination behavior more predictable for range-heavy queries.

### 🔧 Changed

* `IndexRange` cursors now carry a raw-key anchor (`last_raw_key`) and resume by rewriting only the lower bound to `Bound::Excluded(last_raw_key)`.
* Cursor validation now checks raw-key decode, index identity/namespace/arity, and envelope membership against the original `(prefix, lower, upper)` range.
* Planning and execution now share one canonical raw-bound builder to prevent drift between validation and store traversal.
* Load execution now passes planned cursor state (boundary + optional raw anchor) through to store-level range traversal.

### 🧪 Testing

* Added multi-page parity coverage that compares paginated results to unbounded execution, including byte-for-byte row parity checks.
* Added strict monotonic anchor-progression assertions across continuation pages.
* Added explicit unique-index continuation coverage for `IndexRange` (design Case F).

### 🧭 Migration Notes

* No public API migration required yet.
* Cursor format stability and binary cursor commitments are planned for a later milestone.

---

## [0.11.2] – 2026-02-17 - Rust Visibility Pass

### 📝 Summary

* This release completes a visibility pass across the DB internals based on `docs/VISIBILITY.md`. We tightened module boundaries, removed deep internal imports, and moved callers to clear subsystem root surfaces. This is better because internal refactors are safer, accidental API leakage is reduced, and privacy rules are now enforced by both module visibility and compile-fail tests.

### 🗑️ Removed

* Removed the hidden `__internal` module from `icydb`.

### 🧪 Testing

* Updated privacy tests for `db::data`, `db::index`, and `db::executor`.
* Updated sanitize/validate compile-fail tests to check private `visitor` internals.

---

## [0.11.1] – 2026-02-17

### 🔧 Changed

* Added a dedicated hidden macro wiring surface (`__macro`) in `icydb` so generated code no longer has to depend on long `__internal::core::...` DB paths.
* Updated actor/store codegen to use `::icydb::__macro::{Db, DataStore, IndexStore, StoreRegistry, EntityRuntimeHooks, ...}` for cleaner internal codegen boundaries.

### 🧹 Cleanup

* Tightened several internal module boundaries from `pub` to `pub(crate)` in `db` and `db::query` where external visibility was not needed.
* Removed dead predicate/query helpers and unused wrappers, including `IndexIdError`, unused text-op variants (`Eq`, `Contains`), unused typed visitor wrappers, and unused diagnostics helper constructors.
* Simplified infallible merge and conversion helpers by removing unnecessary return values and dead branches.
* Removed additional unused plan helpers while keeping current access-path behavior unchanged.

## [0.11.0] – 2026-02-16 - Range Pushdown

### 🔧 Changed

* Secondary index scans can now push down bounded range predicates (`>`, `>=`, `<`, `<=`) into index traversal instead of relying on full-scan filtering.
* Composite indexes now support prefix-equality plus one ranged component (for example `a = ?` with a range on `b`) with deterministic ordering through the primary-key tie-break.
* Range-compatible predicate pairs (`>=` + `<=`) now plan as one bounded `IndexRange`, which keeps pagination behavior aligned with fallback execution.
* Single-predicate one-sided ranges on single-field indexes (for example `tag > x` or `tag <= y`) now plan directly to `IndexRange` instead of falling back.

### 🧹 Cleanup

* Added explicit `AccessPath::IndexRange` guardrails for secondary-range planning prep, and wired explain/trace/hash/canonical/debug handling so new access variants cannot be silently skipped.

### 🧪 Testing

* Added planner tests for valid and invalid range extraction shapes, stricter-bound merging, and empty-range rejection.
* Added parity tests that compare range pushdown results against `by_ids` fallback for single-field and composite-prefix range queries.
* Added pagination boundary tests for range windows to verify no-duplicate resume behavior at lower and upper edges.
* Added edge-value tests around `0` and `u32::MAX` to verify inclusive and exclusive bound correctness.
* Added table-driven parity matrices across `>`, `>=`, `<`, `<=`, and `BETWEEN`-equivalent forms, including descending and no-match/all-match cases.
* Added composite duplicate-edge cursor boundary tests to verify strict resume behavior when lower/upper boundary groups contain multiple rows.

Example (shape only):

```rust
AccessPath::IndexRange {
    index,
    prefix: vec![Value::Uint(7)],
    lower: Bound::Included(Value::Uint(100)),
    upper: Bound::Excluded(Value::Uint(200)),
}
```

---

## [0.10.2] – 2026-02-16

### 🧹 Cleanup

* `AccessPlan` projection now uses one shared path for explain, trace, metrics, and hashing. This reduces repeated edits when access types change.
* Plan-shape rules now live in `query::policy`, and other layers wrap those errors instead of re-implementing them. This keeps behavior aligned.
* Save and delete commit-window setup now uses one shared helper. This keeps single, batch, and delete flows consistent.
* Explain and trace now share one pushdown mapping. New pushdown outcomes only need to be added in one place.
* Value type tags now come from one canonical source across normalization, ordering, and fingerprinting. This removes duplicated tag tables.
* Index-prefix compatibility checks and cursor primary-key decoding now use shared helpers. This lowers planner/executor drift risk.
* Reduced temporary allocations in cursor filtering and predicate sort-key encoding for cleaner hot-path behavior.
* `apply_post_access_with_cursor` is now split into clear phase helpers for filter, order, cursor, pagination, and delete limits. This makes future changes safer and easier to review.
* Pushdown validation tests now use table-driven cases for core and applicability scenarios. This removes repeated setup and keeps matrix coverage easier to extend.
* Changelog section headers are now normalized to one fixed emoji mapping for standard section types. This keeps release notes consistent across versions.

## [0.10.1] – 2026-02-16

* Macro codegen #[expect] changed to #[allow], oops.

## [0.10.0] – 2026-02-16 - Index Key Ordering

### 📝 Summary

* `0.10.0` begins IndexKey v2.  It was hashed before, now it's a canonical byte slice that can be ordered.
* Goal: keep index key bytes and key ordering stable across upgrades.
* Coming Next: all the cool stuff orderable indexes bring.

### 🔧 Changed

* Index keys now use a framed format with explicit lengths for each part.
* Index component encoding is now fully canonical and deterministic.
* User and system index keys are clearly separated by key kind.
* Startup recovery now rebuilds secondary indexes from saved rows, so stale index entries are corrected before normal reads and writes continue.
* Rebuild is fail-closed: if rebuild hits bad row bytes or hook wiring issues, recovery restores the previous index snapshot and returns a classified error.

Index key format (`v0.10`):

```text
[key_kind:u8][index_id:fixed][component_count:u8]
[component_len:u16be][component_bytes]...
[pk_len:u16be][pk_bytes]
```

### 🧪 Testing

* Added golden-byte tests that fail if key encoding changes.
* Added corruption tests for invalid lengths, truncation, and trailing bytes.
* Added ordering tests to ensure value order and byte order stay aligned.
* Added prefix-scan isolation tests for namespace and index boundaries.
* Added unique-index behavior tests for insert, update, and delete/reinsert flows.
* Added recovery tests to confirm index key bytes stay stable after replay.
* Added startup-rebuild tests that prove stale index entries are replaced by canonical entries rebuilt from row data.
* Added fail-closed rebuild tests that prove index state is rolled back if rebuild encounters corrupt rows.

### 🧹 Cleanup

* Replaced many `#[allow(...)]` attributes with `#[expect(...)]` where valid, and removed unfulfilled expects.

Example (simplified):

```rust
let key = IndexKey::new(&entity, index)?.expect("indexable");
let raw = key.to_raw();
let decoded = IndexKey::try_from_raw(&raw)?;
assert_eq!(decoded.to_raw().as_bytes(), raw.as_bytes());
```

---

## [0.9.0] – 2026-02-15 - Strengthening Release

### 📝 Summary

* `0.9.0` focuses on safer deletes, clearer batch-write behavior, and stronger query execution checks.
* Existing `0.8.x` user-facing behavior stays the same in key areas (cursor format, storage format, and default write semantics).

### 🔧 Changed

* Strong relation checks now block deletes that would leave broken references.
* Batch writes now have clear lanes: atomic (`*_many_atomic`) and non-atomic (`*_many_non_atomic`).
* Ordered pagination does less unnecessary work while keeping the same results.
* Planner and executor checks were tightened to catch invalid states earlier.

### 🩹 Fixed

* Recovery replay for interrupted writes is now more reliable and repeat-safe.
* Error categories are clearer (`Unsupported`, `Corruption`, `Internal`) across relation/index paths.
* Metrics and trace coverage improved for key read/write phases.
* Storage diagnostics now clearly separate user index data from system index data.

Example (simplified):

```rust
let saved = db.session().insert_many_atomic(users)?;
assert_eq!(saved.len(), users.len()); // all-or-nothing for this batch
```

---

## [0.8.5] – 2026-02-15 - Transaction Semantics Hardening

### 📝 Summary

* This release tightens and clarifies the batch write behavior introduced in `0.8.4`.
* `_many_atomic` is confirmed as all-or-nothing for one entity type.
* `_many_non_atomic` remains fail-fast with partial commits allowed.

### 🧪 Testing

* Added more conflict tests for atomic and non-atomic update/replace batch flows.
* Added tests that confirm invalid strong relations fail atomic batches without partial writes.
* Added empty-batch tests for both lanes.
* Added recovery tests for unknown entity paths and miswired hooks.
* Added tests for reserved index namespaces and storage corruption counters.
* Added tests to confirm delete `limit` is applied in the correct execution phase.

### 🔧 Changed

* Updated docs with simpler guidance on choosing atomic vs non-atomic batch writes.
* Improved ordered pagination performance for common first-page queries.
* Added a faster path for primary-key ordered scans, including key-range scans.

Example (simplified):

```rust
let result = db.session().update_many_non_atomic(batch);
if result.is_err() {
    // By design, earlier rows in this batch may already be committed.
}
```

---

## [0.8.4] – 2026-02-15 - Explicit Transaction Semantics Milestone

### 📝 Summary

* Added opt-in atomic batch APIs: `insert_many_atomic`, `update_many_atomic`, and `replace_many_atomic`.
* These are atomic only within one entity type.
* They are not full multi-entity transactions.
* Existing non-atomic batch APIs were kept as-is.

### 🔧 Changed

* Added an explicit all-or-nothing batch lane for single-entity writes.
* Updated docs to clearly explain atomic vs non-atomic behavior.

Example (single entity type only):

```rust
let users = vec![
    User { id: user_a, email: "a@example.com".into() },
    User { id: user_b, email: "b@example.com".into() },
];

let saved = db.session().insert_many_atomic(users)?;
assert_eq!(saved.len(), 2);
```

---

## [0.8.3] – 2026-02-15 - Strong RI Milestone

### 📝 Summary

* Completed the strong referential integrity milestone for the `0.9` plan.
* Deletes now better protect against broken strong references, and related replay/diagnostic paths are better covered by tests.

Example (simplified):

```rust
let err = db
    .session()
    .delete::<TargetEntity>()
    .by_id(target_id)
    .execute()
    .unwrap_err();
assert!(err.to_string().contains("strong relation"));
```

---

## [0.8.2] – 2026-02-15 - Reverse Index Integrity

### 🔧 Changed

* Strong-relation delete checks now use reverse indexes instead of full source scans.
* Reverse-index updates now follow the same commit/recovery path as row updates.
* Metrics now report reverse-index and relation-validation deltas more clearly.
* Storage snapshots now separate user index entries from system index entries.

### 🧹 Cleanup

* Simplified runtime dispatch by moving to one shared hook registry per entity.

### ⚠️ Breaking

* User index names in the reserved `~` namespace are now rejected at derive time.

Example (simplified):

```rust
// This now fails during schema derive/validation:
#[index(name = "~custom", fields = ["email"])]
```

---

## [0.8.1] – 2026-02-13 - Cursor Boundary Hardening

### 🧪 Testing

* Added stronger tests for invalid cursor tokens (empty, bad hex, odd length).
* Added live-state pagination tests for insert/delete changes between page requests.
* Added more cursor codec roundtrip and edge-case tests.

### 🩹 Fixed

* Schema validation now catches data/index memory ID collisions earlier.

### 🧹 Cleanup

* Broke index code into smaller modules and kept tests close to those modules.
* Simplified index fingerprint storage to one inline value next to each index entry.
* Removed no-longer-needed fingerprint memory config from schema metadata.

### 🔧 Changed

* Store access now goes through one shared registry handle.
* Index metrics now emit one delta event per commit apply.
* Added replay tests for mixed save/save/delete flows on shared index keys.

### ⚠️ Breaking

* Duplicate store path registration is now rejected instead of silently replaced.
* Store schema/runtime now uses a single combined store model instead of split data/index registries.
* Commit markers no longer store `kind`; mutation shape is derived from `before` and `after`.

Example (simplified):

```rust
let err = Query::<User>::new(ReadConsistency::MissingOk)
    .page()
    .cursor("not-hex")
    .limit(20)
    .plan()
    .unwrap_err();
```

---

## [0.8.0] – 2026-02-13 - Structural Correctness Baseline

### 📝 Summary

* `0.8.0` focuses on making core query and pagination behavior predictable.
* Goal: same input should reliably produce the same output.
* Strong delete-side relation checks were planned for later `0.8.x` updates.

### 🔧 Changed

* Pagination rules are now clearer and consistently enforced.
* Collection behavior is now clearly documented for `List`, `Set`, and `Map`.
* Added `icydb-primitives` to centralize scalar metadata.
* Updated docs and roadmap language to reduce ambiguity.

### ⚠️ Breaking

* Generated view/create/update payload types now live in entity-local modules.
* Call sites should use prelude aliases or explicit entity module paths.

### 🩹 Fixed

* Added wider regression coverage for cursor paging and uniqueness behavior.
* Improved planner/query error and lint hygiene without changing user-facing query behavior.

### 🧹 Cleanup

* Reduced duplicate internal logic in planning and mutation paths.
* Centralized canonical value ordering/tagging behavior in shared modules.
* Split `Unit` coercion behavior from `Bool` to make type handling clearer.

Example (simplified):

```rust
let page1 = query.order_by("created_at").limit(20).execute()?;
let page2 = query.cursor(page1.next_cursor.unwrap()).execute()?;
```

---

## [0.7.21] – 2026-02-11 - Cursor Pagination, Part I

### 🔧 Changed

* Cursor pagination now follows one clear execution order for filtering, ordering, cursor skip, and limits.
* Cursor payloads are now encoded and validated earlier in planning.
* Added typed pagination with `.page()`, which requires explicit order and limit.
* Documented expected pagination consistency when data changes between requests.

### 🩹 Fixed

* Schema validation cache now stays isolated per entity type.
* Singleton unit-key save/load behavior was tightened and covered with tests.
* `next_cursor` is now based on the last row returned, reducing cursor drift.
* Added stronger validation for malformed or mismatched cursor tokens.

### 🧹 Cleanup

* Removed unused query error layers and unused error variants.
* Removed dead missing-key patch error branches after no-op missing-key behavior.
* Reduced `QueryError::Plan` size while preserving diagnostics.

Example (simplified):

```rust
let page = Query::<User>::new(ReadConsistency::MissingOk)
    .page()
    .order_by("created_at")
    .limit(20)
    .execute()?;
```

## [0.7.20] – 2026-02-11 - Calm After the Storm

### 🔧 Changed

* Read paths now quickly check and replay pending commit markers before loading data.
* Write recovery now uses the same recovery path as reads for consistency.
* Saves now enforce that the declared primary key matches the entity identity.
* Facade query errors are grouped more clearly, including a dedicated unordered pagination error.
* Facade query exports were narrowed to safer boundary types.
* Map patch behavior now matches list/set behavior: missing-key remove/replace is a no-op.
* Removed disabled internal map-predicate branches.

### 🩹 Fixed

* Derive validation now rejects unsupported map value shapes earlier.
* Map value conversion avoids panic on invalid entries and reports issues safely.
* Fixed recursive map type inference issues in nested map-like value trees.
* Row decode errors now keep underlying deserialize details for easier debugging.
* Added more regression tests for map validation and incomplete marker replay.

Example (simplified):

```rust
let update = UserUpdate::default()
    .with_settings(MapPatch::remove("missing_key"));
db.session().patch_by_id(user_id, update)?; // remove on missing key is a no-op
```

## [0.7.19] – 2026-02-10

### 🔧 Changed

* `icydb-schema-derive` now treats field visibility as an entity/record responsibility: base `Field`/`FieldList` emission no longer hardcodes `pub(crate)`, entity fields are emitted as `pub(crate)`, and record fields are emitted as `pub`.

## [0.7.18] – 2026-02-10

### ➕ Added

* Icrc1::TokenAmount and Icrc1::Tokens provide a .units() -> u64 call

### 🔧 Changed

* `Timestamp` now supports signed and unsigned scalar arithmetic (`u64`/`i64`) via `+`, `-`, `+=`, and `-=`, using saturating behavior for underflow/overflow and negative deltas.
* `Duration` now supports the same signed and unsigned scalar arithmetic ergonomics (`u64`/`i64`) with saturating semantics.
* `Timestamp` arithmetic with `Duration` is now directly supported (`Timestamp +/- Duration` and assign variants), applying duration values in whole seconds.
* `Timestamp` and `Duration` now support direct scalar comparisons against `u64` and `i64` (`<`, `<=`, `>`, `>=`, `==`) in both directions.
* Scalar-left subtraction is now supported for both time types (`u64/i64 - Timestamp` and `u64/i64 - Duration`) so raw numeric timestamps and durations can be subtracted from wrapped values without manual conversion.

## [0.7.15] – 2026-02-09

### ➕ Added

* Any Id<E> can now be turned into a ledger subaccount with `.subaccount()`
* Added facade-level `UpdateView::merge` error promotion so patch failures are surfaced as `icydb::Error` with `ErrorKind::Update(UpdateErrorKind::Patch(...))`.

### 🔧 Changed

* Generated relation `*_ids()` accessors for `many` cardinality now return `impl Iterator<Item = Id<Relation>> + '_` instead of allocating a `Vec<Id<Relation>>`, while preserving key-to-`Id` projection behavior.

### ⚠️ Breaking

* `icydb::patch` no longer exports `MergePatch` or `MergePatchError`; callers should use `UpdateView::merge` and handle facade `icydb::Error`.

---

## [0.7.12] – 2026-02-09

### ➕ Added

* Added `UpdateView` trait generation for schema-derived list/set/map/newtype/record/tuple/enum/entity types so patch payload typing is explicit at the view boundary.
* Added `UpdateView` coverage for core container wrappers (`OrderedList`, `IdSet`) and structural containers (`Option`, `Vec`, `HashMap`, `HashSet`, `BTreeMap`, `BTreeSet`) using `ListPatch`/`SetPatch`/`MapPatch` payload shapes.

### 🔧 Changed

* Schema derives now route patch generation through `MergePatch` end-to-end (trait wiring, node dispatch, and emitted merge calls), while preserving existing `*Update` payload type names and patch shapes.
* Merge payload typing now resolves through `<T as UpdateView>::UpdateViewType`; `MergePatch` implementations no longer define or consume a separate `Patch` associated type.
* Atomic merge semantics now consistently use `traits::Atomic` in type modules, and the blanket `MergePatch` path applies full-replacement updates from `UpdateViewType = Self`.

---

## [0.7.10] – 2026-02-09 - Facade Error Kinds

### ➕ Added

* Added structured facade error categories in `icydb::error` via `ErrorKind`, `QueryErrorKind`, `UpdateErrorKind`, `PatchError`, and `StoreErrorKind` so callers can branch on stable semantic error kinds instead of parsing messages.
* Added explicit patch error lowering from `ViewPatchError` into facade `PatchError` variants, keeping patch failure handling user-facing and predictable.
* Added `DbSession::patch_by_id` in the facade to execute load-merge-save in one boundary-owned operation, mapping merge failures into `ErrorKind::Update(UpdateErrorKind::Patch)` without exposing core patch errors to callers.
* Added a dedicated `types::identity::GenerateKey` module trait so key generation capability is explicitly modeled at the identity layer.

### 🔧 Changed

* Query error mapping in the facade now classifies validation/planning/intent failures as `Query(Invalid)`, unsupported features as `Query(Unsupported)`, and response cardinality failures as `Query(NotFound|NotUnique)`.
* Internal execution failures continue to cross the facade as `ErrorKind::Internal` with preserved origin and message context.

### ⚠️ Breaking

* `icydb::Error` now exposes `kind` instead of the previous class-style taxonomy field, and the old facade `ErrorClass` surface is replaced by the new structured `ErrorKind` family.

---

## [0.7.9] – 2026-02-09 - Relation ID Accessors

### ➕ Added

* Added generated relation ID accessors on entity and record inherent impls for relation-backed fields, including `*_id()` for single/optional relations and `*_ids()` for many relations.
* Accessors now return typed IDs (`Id<Relation>`) derived from stored primitive relation keys, so relation fields can remain `pub(crate)` without losing ergonomic read access.

### 🔧 Changed

* Split inherent code generation into smaller focused modules (`entity`, `record`, `collection`, and relation accessor generation) to reduce coupling and make future schema macro changes easier to review.
* Split view/mutation traits into dedicated modules so behavior contracts are clearer: `AsView` stays in `traits::view`, `CreateView` moved to `traits::create`, and `UpdateView` + `ViewPatchError` now live in `traits::update`.

### ⚠️ Breaking

* `UpdateView::merge` now returns `ViewPatchError` directly instead of `InternalError`, and patch classification (`NotFound`/`Unsupported`) is now applied at the error boundary via explicit conversion.
* Removed `view` type aliases (`View<T>`, `Create<T>`, `Update<T>`); call sites now use associated types (`<T as AsView>::ViewType`, `<T as CreateView>::CreateViewType`, `<T as UpdateView>::UpdateViewType`).

---

## [0.7.7] – 2026-02-08 - Error Boundary and ID Naming

### 🔧 Changed

* `UpdateView::merge` now returns `Error` instead of `ViewPatchError`, with patch failures bubbled through `InternalError` via `ErrorDetail::ViewPatch` while preserving contextual path/leaf diagnostics.
* Standardized a broad set of accessor methods from `key()` to `id()` to align naming with typed identity usage across the public API.

---

## [0.7.4] – 2026-02-08

### ➕ Added

* Added contextual merge patch errors via `ViewPatchError::Context`, including `path()` and `leaf()` helpers, so callers can locate and classify update failures without depending on internal patch details.
* Added explicit executor-level phase-order tests covering optional-field equality, `IN`/`CONTAINS`, and text predicates.
* Added a structural post-access guard test plus `TracePhase::PostAccess` diagnostics so regressions in filter/order/pagination execution are detected at the executor boundary.

### 🔧 Changed

* Restored post-access query execution in load/delete paths so predicate evaluation, ordering, pagination, and delete limits are applied deterministically from the logical plan.

---

## [0.7.3] – 2026-02-08
* Added `EntityValue` back to the public prelude re-exports for easier trait access in downstream code.

---

## [0.7.2] – 2026-02-08 - Key Byte Contracts

### 📝 Summary

0.7.1 standardizes primary-key byte encoding through `EntityKeyBytes` and simplifies external identity projection to hash canonical key bytes directly.
This release also removes namespace-based projection metadata and makes key-byte encoding an explicit compile-time contract.

### ➕ Added

* Added `EntityKeyBytes` with explicit `BYTE_LEN` and `write_bytes` requirements for primary-key encoding.
* Added `Id<E>::KEY_BYTES` and `Id<E>::into_key()` for explicit key-size introspection and key extraction.

### 🔧 Changed

* `EntityKey::Key` now requires `EntityKeyBytes`, so key-encoding compatibility is checked at compile time.
* `Id<E>::project()` is now a direct projection path over canonical key bytes using the projection domain tag.
* Relaxed the docs so Codex stops faffing about the ID being a secret in a capability-first system

### 🗑️ Removed

* Removed `Subaccount::from_ulid` in favor of explicit subaccount byte construction paths.

---

## [0.7.0] – 2026-02-08 - Contract Freeze

### 📝 Summary

0.7.0 freezes the core engine contracts for identity, query behavior, atomicity, and referential integrity.

Identity is now explicitly typed (`Id<E>`), query intent/planning boundaries are formally locked, commit-marker discipline is specified as the atomicity source of truth, and RI remains explicit strong-only save-time validation with weak-by-default relations.

This release is the 0.7 baseline for deterministic behavior, compile-time schema rejection of illegal identity shapes, and bounded write-path enforcement without cascades.

### ➕ Added

* Added `strong`/`weak` relation flags in the schema DSL, with `weak` as the default.
* Added a `Display` derive in `icydb-derive` for tuple newtypes.
* Added collection types `OrderedList` and `IdSet` for explicit many-field semantics.
* Added `OrderedList::retain` plus `apply_patches` helpers on `OrderedList` and `IdSet` for explicit patch application.
* Added `docs/collections.md` as the contract reference for collection and patch semantics.
* Added `docs/IDENTITY_CONTRACT.md` as the normative identity and primary-key contract for `Id<E>`, explicit construction, and declared-type authority.
* Added `docs/QUERY_CONTRACT.md` as the intent/planning/execution boundary contract for query determinism and explicit missing-row policy.
* Added `docs/ATOMICITY.md` as the normative single-message commit and recovery contract for write safety.
* Added `docs/REF_INTEGRITY.md` as the normative RI contract for strong/weak relation behavior and bounded save-time validation.
* Added `saturating_add`/`saturating_sub` helpers to arithmetic newtypes for explicit saturating math.
* Added `Id<E>` as a typed primary-key value that preserves entity-kind correctness.
* Added parity coverage to keep keyability conversion paths aligned across `ScalarType::is_keyable`, `Value::as_storage_key`, and `StorageKey::try_from_value`.

### 🔧 Changed

* Save operations now enforce referential integrity for `RelationStrength::Strong` fields and fail if targets are missing.
* Write executors now perform a fast commit-marker check and replay recovery before mutations when needed; read recovery remains startup-only.
* Entity macros now allow primary keys to be relations for identity-borrowing singleton entities.
* Primary-key derivation now follows only the declared primary-key field type; relation metadata does not infer PK storage shape.
* Illegal or ambiguous identity/primary-key schema shapes are now treated as compile-time derive failures instead of runtime checks.
* ORDER BY and model key-range validation now use a shared canonical value comparator instead of `Value::partial_cmp`, keeping query ordering behavior consistent for all orderable key types.
* Documented that `Value::partial_cmp` is not the canonical database ordering path and should not be used for ORDER BY or key-range semantics.

### ⚠️ Breaking

* Entity and record fields with `many` cardinality now emit `OrderedList<T>` instead of `Vec<T>`.
* Relation fields with `many` cardinality now emit `IdSet<T>` instead of list types like `Vec<Id<T>>`.
* Entity primary-key fields now emit `Id<E>` instead of raw key values, and `EntityValue::set_id` wraps raw keys into `Id<E>` so call sites must pass the raw key type.
* Storage key admission is now registry-driven via `is_storage_key_encodable`; the encodable scalar set is unchanged, but the contract is now explicit and auditable.

---

## [0.6.20] – 2026-02-04

### ➕ Added

* Added `Blob::as_bytes()` and `Blob::as_mut_bytes()` for explicit byte access without deref.

### 🔧 Changed

* Relation/external field suffix bans now apply only to relation and external fields (not arbitrary primitives like `my_api_id`).

### 🩹 Fixed

* Made `Id<T>` `Sync + Send` to fix the `*const` variant.

---

## [0.6.17] – 2026-02-03 - Query Ergonomics

### ➕ Added

* Added `WriteResponse`/`WriteBatchResponse` helpers for write results, including key and view accessors.
* Added `Nat::to_i64`/`to_u64` and `Int::to_i64`/`to_u64` for explicit integer conversion without deref.
* Added `by_ref()` for query flow (later removed and replaced by `by_id()`/`by_ids()`).
* Added `many_refs()` for query flow (later removed and replaced by `by_ids()`).

### 🔧 Changed

* id_strict and key_strict to require_id and require_key to match other methods
* Clarified schema error messaging for banned suffixes on field names

### ⚠️ Breaking

* Schema field names ending in `_id`, `_ids`, `_ref`, `_refs`, `_key`, or `_keys` now fail at compile time; relation fields were renamed to base nouns.
* Singleton query `only()` no longer accepts an explicit ID and always uses the default singleton key.

---

## [0.6.11] – 2026-02-03 - Decimals, Collections and Stuff

### ➕ Added

* Added a `get()` accessor to map collection inherent traits for explicit lookup without deref.
* Added `Decimal::abs()` to expose absolute value math without deref.
* Added `Blob::to_vec()` for explicit byte cloning without deref.

### 🔧 Changed

* Planner access planning no longer re-validates predicates; validation is now owned by the intent/executor boundaries.
* Consolidated primary-key compatibility checks to the shared `FieldType::is_keyable` rule to avoid drift across planner/validator layers.
* Renamed primary_key() and similar methods in Response to key() for consistency

### ⚠️ Breaking

* `MapCollection::iter` now returns a GAT-backed iterator instead of a boxed trait object, so implementations and type annotations must update.
* `Collection::iter` now returns a GAT-backed iterator instead of a boxed trait object, so implementations and type annotations must update.
* `DbSession::insert`/`replace`/`update` now return `WriteResponse<E>` (and batch variants return `Vec<WriteResponse<E>>`).

---

## [0.6.6] – 2026-02-03 - Diagnostic Test Reenablement

### 📝 Summary

* Re-enabled query plan explain, fingerprint, and validation tests to guard planner determinism and invariants after the refactor.

### ➕ Added

* Added `ByKeys` determinism checks for `ExplainPlan` and `PlanFingerprint` to lock in set semantics for key batches.
* Added a typed-vs-model planning equivalence test to anchor `QueryModel`/`Query<E>` parity post-refactor.

---

## [0.6.5] – 2026-02-03 - Derive Consolidation & Explicit Collections

### 📝 Summary

* Introduced `QueryModel` to separate model-level intent, validation, and planning from typed `Query<E>` wrappers, reducing trait coupling in query logic.
* Added the `icydb-derive` proc-macro crate for arithmetic and ordering derives on schema-generated types.
* Relocated canister-centric tests to PocketIC-backed flows and removed canister builds from default `make test` runs.

### ➕ Added

* Added the `icydb-derive` proc-macro crate with `Add`, `AddAssign`, `Sub`, `SubAssign`, `Mul`, `MulAssign`, `Div`, `DivAssign`, and `Sum` derives for tuple newtypes.
* Added a `Rem` derive for tuple newtypes and re-exported the `Rem` trait from `traits`.
* Added a `PartialOrd` derive in `icydb-derive` and routed schema-generated types to it.
* Added `Decimal` helpers `is_sign_negative`, `scale`, and `mantissa` for explicit access without deref.
* Added `MulAssign` and `DivAssign` impls for `Decimal` to match arithmetic derives.
* Added `Blob::as_slice` for explicit byte access in validators.
* Added `Mul`/`Div` and assignment ops for `E8s` and `E18s` to satisfy fixed-point newtype arithmetic derives.
* Added `Mul`/`Div` and assignment ops for `Nat` and `Nat128` to support arithmetic newtype derives.
* Added `Mul`/`Div` and assignment ops for `Int` and `Int128` to support arithmetic newtype derives.
* Added `Collection` and wired list/set wrapper types to explicit iteration and length access without deref.
* Added `MapCollection` for explicit, read-only iteration over map wrapper types without deref.
* Added explicit mutation APIs on list/set/map wrapper types (`push`, `insert`, `remove`, `clear`) without implicit container access.
* Moved `PartialEq` derives to `icydb-derive` for schema-generated types.

### 🔧 Changed

* Newtype arithmetic derives now route through `icydb-derive` (including `Div`/`DivAssign`) instead of `derive_more`.
* `test_entity!` now requires an explicit `struct` block and derives `EntityKind::Id` from the primary key field’s Rust type, failing at compile time if the PK is missing from the struct or `fields {}`.
* `FieldValues` is now derived via `icydb-derive` and no longer implemented by schema-specific `imp` code.
* `DbSession::diagnose_query` now requires `EntityKind` only, keeping diagnostics schema-level.
* Public query builders now accept `EntityKind` for intent construction; execution continues to require `EntityValue`.
* Updated `canic` to `0.9.17`.
* `make test` no longer runs canister builds; `test-canisters` is now a no-op.

### 🗑️ Removed

* Removed schema-derive `imp` implementations for `Add`/`AddAssign`/`Sub`/`SubAssign` in favor of derives.
* Removed `Display` trait from schema-derive

### 🩹 Fixed

* Exported `Div`/`DivAssign` through `traits` so generated arithmetic derives resolve cleanly.
* Session write APIs and query execution now require `EntityValue`, aligning runtime execution with value-level access.
* `#[newtype]` now derives `Rem` only for primitives that support remainder, and `Int128`/`Nat128` implement `Rem` to match numeric newtype expectations.

---

## [0.6.4] – 2026-02-01 - Explicit Key Boundaries

### 🔧 Changed

* Removed `Into<...>` from `by_key` functions to keep primary key boundaries explicit (`by_key` was later replaced by `by_id`/`by_ids`).

---

## [0.6.3] – 2026-02-01 - Primary Key Guardrails

### 🩹 Fixed

* Entity macros now reject relation fields as primary keys, preventing relation identities from being used as primary key types.
* Primary key fields must have cardinality `One`; optional or many primary keys now fail at macro expansion time.
* Local schema invariants now fail fast during macro expansion, including field identifier rules, enum variant ordering, and redundant index prefix checks.
* Added compile-fail tests covering relation and non-One primary key shapes in the entity macro.

### 📝 Summary

* Locked primary key invariants at macro expansion time to avoid downstream RI violations.

---

## [0.6.1] – 2026-02-01 - Referential Integrity, Part II

### ➕ Added

* **Save-time referential integrity (RI v2)**: direct `Id<T>` and `Option<Id<T>>` relation fields are now validated pre-commit; saves fail if the referenced target row is missing.
* Added `docs/REF_INTEGRITY_v2.md`, defining the v2 RI contract, including:

  * strong vs weak reference shapes,
  * atomicity boundaries,
  * and explicit non-recursive enforcement rules.
* Added targeted RI tests covering:

  * strong reference failure on missing targets,
  * allowance of weak reference shapes,
  * and non-enforcement of references during delete operations.

### 🔧 Changed

* Nested and collection reference shapes (`Id<T>` inside records/enums, and `Vec`/`Set`/`Map<Id<T>>`) are now **explicitly treated as weak** at runtime and no longer trigger invariant violations during save.
* Clarified that schema-level relation validation is **advisory only** and does not imply runtime RI enforcement.
* Aligned runtime behavior, schema comments, and documentation with the RI v2 contract.

### 📝 Summary

* Introduced **minimal, explicit save-time referential integrity** for direct references only, while formally defining and locking the weak-reference contract for all other shapes.

---

## [0.6.0] – 2026-01-31 - Referential Integrity, Part I

### ⚠️ Breaking
* Index storage now splits data and index stores explicitly; index stores require separate entry and fingerprint memories.
* `IndexStore::init` now requires both entry and fingerprint memories; constructing an index store without fingerprint memory is no longer possible.

### ➕ Added
* Added dedicated index fingerprint storage to keep verification data independent from index routing entries.
* Added a cross-canister relation validation test with a dedicated relation canister to lock in the new schema invariant.

### 🩹 Fixed
* ORDER BY now preserves input order deterministically for incomparable values.
* Commit marker apply now rejects malformed index ops or unexpected delete payloads in release builds.
* Commit marker decoding now rejects unknown fields instead of silently ignoring them.
* Commit marker decoding now honors the marker size limit instead of the default row size cap.
* Oversized commit markers now surface invariant violations instead of corruption.

### 🔧 Changed
* Documented that `FieldRef` and `FilterExpr` use different coercion defaults for ordering; see `docs/QUERY_BUILDER.md`.
* Consolidated build-time schema validation behind `validate::validate_schema` so all passes run through a single entrypoint.

### 📝 Summary
* Logged the 0.6 atomicity audit results, including the read-path recovery mismatch, for follow-up.

---

## [0.5.25] – 2026-01-30

### ⚠️ Breaking
* Case-insensitive coercions are now rejected for non-text fields, including identifiers and numeric types.
* Text substring matching must use `TextContains`/`TextContainsCi`; `CompareOp::Contains` on text fields is invalid.
* ORDER BY now rejects unsupported or non-orderable fields instead of silently preserving input order.

### 🔧 Changed
* Executor ordering tests now sort only on orderable fields while preserving tie stability and secondary ordering guarantees.
* Conducted a DRY / legacy sweep across query session, executor, and plan layers to remove duplicated or misleading APIs.

---

## [0.5.24] – 2026-01-30

### 🩹 Fixed
- replaced FilterExpr helpers that were accidentally removed

---

## [0.5.23] – 2026-01-30

### 🩹 Fixed

* Insert now decodes existing rows and surfaces row-key mismatches as **corruption** instead of conflicts.
* `SaveExecutor` update/replace detects row-key mismatches as corruption, preventing index updates from amplifying bad rows.
* Unique index validation now treats stored entities missing indexed fields as **corruption**.
* Executors validate logical plan invariants at execution time to protect erased plans:

  * delete limits require ordering
  * delete plans cannot carry pagination
* Recovery validates commit marker kind semantics:

  * delete markers with payloads are rejected
  * save markers missing payloads are rejected
* Load execution performs recovery before reads when a commit marker exists, eliminating read-after-crash exposure to partial state.
* `NotIn` comparisons now return `false` for invalid inputs, matching the “unsupported comparisons are false” contract.
* **ORDER BY now permits opaque primary-key fields; incomparable values sort stably and preserve input order.**

### 🔧 Changed

* Recovery-guarded read access is now enforced via `Db::recovered_context`; raw store accessors are crate-private.
* `storage_report` now enforces recovery before collecting snapshots.
* `FilterExpr` now represents null / missing / empty checks explicitly, matching core predicate semantics.
* Dynamic filters now expose case-insensitive comparisons and text operators without embedding coercion flags in values.
* Map and membership predicates (`not_in`, map-contains variants) are now available via `FilterExpr`.

### 🗑️ Removed

* Dropped the unused projection surface (`ProjectionSpec` and related plan/query fields) to avoid false affordances.

### ⚠️ Breaking

* `obs::snapshot::storage_report` now returns `Result<StorageReport, InternalError>` instead of `StorageReport`.

---


## [0.5.22] - 2026-01-29

### 🩹 Fixed
* Unique index validation now treats index/data key mismatches as corruption, preventing hash-collision or conflict misclassification.
* Delete limits now treat empty sort expressions as missing ordering, avoiding nondeterministic delete ordering.

### 🔧 Changed
* Empty `many([])` / `ByKeys([])` is now a defined no-op that returns an empty result set.

### 🗑️ Removed
* Removed legacy index mutation helpers (`IndexStore::insert_index_entry`, `IndexStore::remove_index_entry`) and the unused `load_existing_index_entry` helper.

---

## [0.5.21] - 2026-01-29

### ➕ Added
* Added enum filter helpers (`EnumValue`, `Value::from_enum`, `Value::enum_strict`) and `FieldRef::eq_none` to make enum/null predicates ergonomic without changing planners or wire formats.
* Added ergonomic helpers to FilterExpr, ie. `FilterExpr::eq()`

---

## [0.5.15] - 2026-01-29

### 🩹 Fixed
* `only()` now works for singleton entities whose primary key is `()` or `types::Unit`, keeping unit keys explicit without leaking internal representations.

### ➕ Added
* Session load/delete queries now expose `Response` terminal helpers directly (for example `row`, `keys`, `primary_keys`, and `require_one`), so applications can avoid handling `Response` explicitly.

### 🔧 Changed
* Load query offsets now use `u32` across intent, planning, and session APIs.
* Also count is u32

---

## [0.5.13] - 2026-01-29

### ➕ Added
* Added dynamic query expressions (`FilterExpr`, `SortExpr`) that lower into validated predicates and order specs at the intent boundary.
* Session load/delete queries now expose `filter_expr` and `sort_expr` to attach dynamic filters and sorting safely.
* Re-exported expression types in the public query module for API endpoints that accept user-supplied filters or ordering.
* Facade versions of FilterExpr and SortExpr

---

## [0.5.11] - 2026-01-29

### 🔧 Changed
* View-to-entity conversions are now infallible; view values are treated as canonical state.
* Create/view-derived entity conversions now use `From` instead of `TryFrom`.
* Float view inputs now normalize `NaN`, infinities, and `-0.0` to `0.0` during conversion.
* Removed `ViewError` plumbing from view conversion and update merge paths.

### ⚠️ Breaking
* `View::from_view` and `UpdateView::merge` no longer return `Result`, and conversion errors are no longer surfaced at the view boundary.

---

## [0.5.10] - 2026-01-29

### ➕ Added
* Restored key-only query helpers: `only()` for singleton entities and `many()` for primary-key batch access.
* Added `text_contains` and `text_contains_ci` predicates for explicit substring searches on text fields.
* Session query execution now returns the facade `Response`, keeping core response types out of the public API.

### 🩹 Fixed
* Cardinality errors now surface as `NotFound`/`Conflict` instead of internal failures when interpreting query responses.

---

## [0.5.7] - 2026-01-28

### ➕ Added
* Generated entity field constants now use `FieldRef`, enabling predicate helpers like `Asset::ID.in_list(&ids)` without changing planner or executor behavior.
* Load and delete queries now support `many` for primary-key batch lookups, using key-based access instead of predicate scans.
* Singleton entities with unit primary keys can use `only()` on load/delete queries for key-only access.

### 🩹 Fixed
* The `icydb` load facade now exposes `count()` and `exists()` terminals.
* Delete queries now treat zero affected rows as a valid, idempotent outcome in the session facade.

---

## [0.5.6] - 2026-01-28

### ➕ Added
* Load queries now expose view terminals (`views`, `view`, `view_opt`) so callers can materialize read-only views directly.
* `Response` now provides view helpers (`views`, `view`, `view_opt`) to keep view materialization explicit at the terminal.
* Predicates now support `&` composition for building conjunctions inline.

### 🔧 Changed
* `key()` on load and delete session queries now accepts any type convertible into `Key`.

---

## [0.5.4] - 2026-01-28

### ➕ Added
* `key()` is now available on both session query types for consistent access to key-based lookups.

---

## [0.5.2] - 2026-01-28 - Public Facade Boundary

### 🩹 Fixed
* Public query methods now return `icydb::Error`, so low-level internal errors no longer leak into app code.
* You can no longer call executors or internal query execution paths from the public `icydb` API.
* Removed `core_db()` and similar test-only backdoors that skipped the public API entirely.
* Removed cross-canister query plumbing and erased-plan interfaces that exposed internal execution details.

### 🔧 Changed
* `db!()` now always returns the public `icydb` session wrapper, not the internal core session.
* Queries must be executed through the session’s load/delete helpers; executors are now core-only.
* Low-level executor corruption tests were removed from the public test suite.

### 🗑️ Removed
* Entity-based query dispatch (`EntityDispatch`, `dispatch_load/save/delete`) and canister-to-canister query handling.
* “Save query” abstractions — writes are now only done via explicit insert/replace/update APIs.
* Tests that depended on calling executors directly outside of `icydb-core`.
* Dropped `upsert` support and the related code paths (~800 lines).

---

## [0.5.1] - 2026-01-28 - Redesigned Query Builder

### 🩹 Fixed
* Executors now reject mismatched plan modes (load vs delete) with a typed `Unsupported` error instead of trapping.

### 🔧 Changed
* Query diagnostics now surface composite access shapes in trace access (union/intersection).
* Executor trace events include per-phase row counts (access, filter, order, page/delete limit).
* Fluent queries now start with explicit `DbSession::load`/`DbSession::delete` entry points (no implicit mode switching).
* Pagination and delete limits are expressed via `offset()`/`limit()` on mode-specific intents.

---

## [0.5.0] – 2026-01-24 – Query Engine v2 (Stabilization Release)

This release completes the **Query Engine v2 stabilization** effort. It introduces a typed, intent-driven query facade, seals executor boundaries, and formalizes correctness, atomicity, and testing contracts.

The focus is **correctness, determinism, and architectural hardening**, not new end-user features.

---

### ➕ Added

**Query Facade**
* Typed query intent (`Query<E>`), making it impossible to plan or execute a query against the wrong entity.
* Executable plan boundary: `ExecutablePlan<E>` is the sole executor input; executor-invalid plans are mechanically unrepresentable.
* Formal query facade contract defining responsibilities of intent construction, planning, and execution.

**Query Semantics**
* Intent-level pagination via `Page` and `Query::page(limit, offset)`.
* Explicit delete intent with `QueryMode::Delete` and `Query::delete_limit(max_rows)`.
* Explicit read consistency (`MissingOk` vs `Strict`) required for all queries.

**Testing & Guarantees**
* Compile-fail (trybuild) tests for facade invariants, preventing construction or execution of internal plan types by user code.
* Query facade testing guide for invariant-driven strategy and when to use compile-fail vs runtime tests.
* Write-unit rollback discipline enforcing “no fallible work after commit window” across mutation paths.

---

### 🩹 Fixed

**Planner / Executor Correctness**
* Missing-row behavior no longer varies based on index vs scan access paths.
* Planners no longer emit plans that executors cannot legally execute.
* Removed duplicated predicate and schema validation between builder, planner, and executor layers.
* Queries can no longer be planned against arbitrary schemas or entities.
* Replaced release assert!-based planner invariant checks with non-panicking error paths to avoid production traps.

**Storage & Indexing**
* Fixed full-scan lower-bound ordering for non-integer primary keys (e.g., Account PK), preventing empty result sets on scans and set operations.
* Eliminated executor panic on empty principals by aligning Key::Principal encoding with IC principal semantics (anonymous/empty principal).
* Index store now surfaces corruption when index entries diverge from entity keys, rather than silently reporting removal.
* Increased commit marker size cap to avoid rejecting valid commits with large index entries.

**Identity & Documentation**
* Removed panicking public `Id<T>` constructors in favor of fallible APIs; unchecked constructors are crate-private for generated models.
* Updated README and internal docs to reflect the actual query execution and atomicity model.

---

### 🔧 Changed

**API & Planning**
* Query API redesign: replaced untyped `QuerySpec` / v1-style DSL with a typed, intent-only `Query<E>` → `ExecutablePlan<E>` flow.
* Pagination is now an intent-level concern; response-level pagination helpers are removed to avoid ambiguity and post-hoc slicing.
* Executors now accept only `ExecutablePlan<E>` and no longer perform planner-style validation.
* `LogicalPlan` is sealed/internal and cannot be constructed or executed outside the planner.
* Planning is deterministic, entity-bound, and side-effect free; repeated planning of the same intent yields equivalent plans.

**Errors, Docs, Tooling**
* Clarified and enforced separation between `Unsupported`, `Corruption`, and `Internal` error classes.
* Improved index store error typing and auditing by preserving error class/origin for index resolution failures.
* Documented unique index NULL/Unsupported semantics: non-indexable values skip indexing and do not participate in uniqueness.
* Removed legacy integration docs and consolidated guidance into README and contract-level documents.
* Updated minimum supported Rust version to **1.93.1** (edition 2024).

---

### 🗑️ Removed

* v1 query DSL and legacy builder APIs.
* Public execution or construction of logical plans.
* Implicit read semantics.
* Executor-side validation and planning logic.
* Schema-parameterized planning APIs.
* Response-level pagination helpers (`Page`, `into_page`, `has_more`).
* Internal plan re-exports from the public facade.
* Plan cache, removed as a premature optimization; planning is deterministic and cheap.

---

### 🧭 Migration Notes

This release contains **intentional breaking changes**:

* All queries must be rewritten using `Query<E>` and explicitly planned before execution.
* Direct use of `LogicalPlan` or untyped query builders is no longer supported.
* Code relying on implicit missing-row behavior must now choose a consistency policy.
* Pagination must be expressed at intent time, not derived from execution results.

These changes are foundational. Future releases are expected to be **additive or performance-focused**, not corrective.

---

### 📝 Summary

0.5.0 marks the point where the query engine is considered *correct by construction*.
Subsequent releases should not re-litigate query correctness, atomicity, or executor safety.


---

## [0.4.7] - 2026-01-22
- 🔁 Renamed `ensure_exists_many` to `ensure_exists_all` for clarity.
- ✅ `ensure_exists_all` is now a true existence-only guard (no deserialization).
- 🧭 Insert no longer loads existing rows during index planning; missing rows are treated as expected.
- 🐛 Debug sessions now emit logs across load/exists, save, delete, and upsert executors.

---

## [0.4.6] - 2026-01-22
- 🧭 Existence checks now treat missing rows as normal and avoid false corruption on scans.
- 🧹 Deletes by primary key are idempotent; missing rows are skipped during pre-scan.
- 🧾 Store not-found is now typed (`StoreError::NotFound`) with `ErrorClass::NotFound`.

---

## [0.4.5] - 2026-01-21 - Atomicity, Part 1
- Moved `FromKey` into `db::traits` and relocated `FromKey` impls into `db/types/*` to keep core types DB-agnostic.
- Moved `Filterable` and `FilterView` into `db::traits` (still re-exported via `traits`).
- Moved index fingerprint hashing out of `Value` into `db::index::fingerprint`.
- Atomicity - commit markers and recovery gating

---

## [0.4.4] - 2026-01-20 - Localized CBOR safety checks and panic containment
- CBOR serialization is now internalized in `icydb-core`, with local decode bounds and structural validation.
- Deserialization rejects oversized payloads before decode and contains any decode panics as typed errors.
- Added targeted CBOR tests for oversized, truncated, and malformed inputs.
- Macro validation now reports invalid schema annotations as compile errors instead of panicking (including trait removal checks and item config validation).

---

## [0.4.3] - 2026-01-20 - Explicit, classified, and localized error propagation at the Disco!
- Storable encoding and decoding no longer panics
- Persisted rows and index entries now use raw, bounded value codecs (`RawRow`, `RawIndexEntry`); domain types no longer decode directly from stable memory.
- Added explicit size limits and corruption checks for row payloads and index entry key sets; invalid bytes surface as corruption instead of panics.
- Domain types no longer implement `Storable`; decoding uses explicit `try_from_bytes`/`TryFrom<&[u8]>` APIs.
- Added targeted raw codec tests for oversized payloads, truncated buffers, corrupted length fields, and duplicate index keys.
- Storage snapshots now count corrupted index entries via value decode checks.
- Fixed executor candidate scans to propagate decode errors from store range reads.

---

## [0.4.2] - 2026-01-19
- Increased `EntityName` and index field limits to 64 chars; `IndexName` length now uses a 2-byte prefix, widening `IndexKey` size.
- `DataKey` now reuses canonical `EntityName` decoding, and `IndexKey` rejects non-zero fingerprint padding beyond `len`.
- Standardized corruption error messages for strict decoders across keys and core types.

---

## [0.4.0] – 2026-01-18 – ⚠️ Very Breaky Things ⚠️

This release finalizes a major internal storage and planning refactor. It hardens corruption detection, fixes long-standing key-space ambiguities, and establishes strict invariants for ordered storage.

---

### ⚠️ Breaking

* **Entity identity is now name-based**
  Storage and index keys now use the per-canister `ENTITY_NAME` directly.
  This replaces the previous hashed `ENTITY_ID` representation.

  * Improves debuggability and introspection
  * Removes hash collision risk
  * Changes on-disk key layout

* **Key serialization invariants enforced**

  * `Key`, `DataKey`, and `IndexKey` are now *strictly fixed-size* and canonical
  * Variable-length encodings are no longer permitted for ordered keys
  * Any deviation is treated as corruption and surfaced immediately
  * `Account` encoding is now canonical (`None` ≠ `Some([0; 32])`)
  * `EntityName`/`IndexName` ordering now matches serialized bytes, with ASCII + padding validation on decode

---

### 🔧 Changed

* **Index executors decoupled from error/metrics plumbing**

  * Index stores no longer emit executor-level errors
  * Executors now:

    * Emit index metrics
    * Surface uniqueness conflicts explicitly

* **Strict read semantics expanded**

  * Missing or malformed rows are now treated as corruption
  * `delete`, `exists`, and `unique` paths use strict scans by default
  * Silent partial reads are no longer allowed

* **Unique index lookups re-validated**

  * Indexed field values are re-read and compared
  * Hash or value mismatches are surfaced as corruption
  * Prevents stale or inconsistent unique entries from going unnoticed

---

### 🔧 Changed

* **Planner is now side-effect free**

  * Planning no longer mutates state or emits metrics
  * All plan-kind metrics are emitted during execution only
  * Enables deterministic planning and easier reasoning about execution paths

---

### 🔧 Changed

* **IndexName sizing is now derived and validated**

  * Computed from:

    * Entity name (≤ 48 chars)
    * Up to 4 indexed field names (≤ 48 chars each)
  * Boundary checks enforced in:

    * Core storage
    * Schema validators
  * Prevents silent truncation and oversized index identifiers

---

### 🔧 Changed

* **Fixed-size key enforcement**

  * Ordered keys (`Key`, `DataKey`, `IndexKey`) now guarantee:

    * Deterministic byte layout
    * Total ordering equivalence between logical and serialized forms
  * Stable memory corruption is detected early and fails fast

* **Explicit size invariants**

  * All bounded `Storable` implementations now:

    * Enforce exact serialized size
    * Validate input on decode
    * Reject malformed or undersized buffers

---

### 🧭 Migration Notes

* Existing stable data **must be migrated**
* Custom storage code relying on:

  * Variable-length keys
  * Hashed entity identifiers
  * Lenient reads
    will need to be updated
* In return, the storage layer now has **database-grade guarantees** around ordering, identity, and corruption detection

---

This release lays the foundation for:

* Safer upgrades
* More aggressive validation
* Long-term storage stability

Future versions will build on these invariants rather than revisiting them.


## [0.3.3] - 2026-01-14
- fixed a CI issue where clippy errors broke things
- #mission70 is retarded

## [0.3.2] - 2026-01-14 - Metrics Decoupling
- Public `Error` now exposes `class` and `origin` alongside the message.
- Observability: unbundled metrics + query instrumentation via `obs::sink` dependency inversion, keeping executors/planner/storage metrics-agnostic while preserving global default and scoped overrides.
- Metrics: route report/reset through `obs::sink` helpers to keep metrics ingress sealed.
- Metrics: avoid double-counting plan kinds on pre-paginated loads.
- Docs: clarify metrics are update-only by design, instruction deltas are pressure indicators, and executor builders bypass session metrics overrides.
- updated canic to 0.8.4

## [0.3.1] - 2026-01-12
- fixed stupid bug

## [0.3.0] – 2026-01-12 – Public Facade Rewrite
### 🔧 Changed
- 🧱 Major layering refactor: icydb is now a strict public facade over icydb-core, with internal subsystems depending directly on core rather than facade modules.
- 🔌 Clear API boundaries: Engine internals (execution, queries, serialization, validation) are fully isolated in icydb-core; icydb exposes only intentional, stable entry points.
- 📦 Public query surface: icydb::db::query is now a supported public API and re-exports core query types for direct use.
- 🛠️ New facade utilities: Added top-level serialize, deserialize, sanitize, and validate helpers with normalized public errors.
- 🔒 Hardened macros & executors: Generated code now targets canonical core paths, preventing accidental API leakage.

### 📝 Summary
- ⚠️ Downstream crates using icydb-core internals may need import updates.
- 🚀 Future internal refactors should now cause far fewer breaking changes.

## [0.2.5] - 2026-01-11 - Error Upgrade
- Runtime errors are now unified under `RuntimeError` with class + origin metadata (internal taxonomy, not a stable API).
- Public `Error` values are produced only at API boundaries and now stringify with `origin:class:` prefixes.
- Added `REFACTOR.md` to document the maintainer-facing runtime contract and refactor baseline.

## [0.2.3] - 2026-01-04
- Added issue() and issue_at() for sanitizer and validators so you can pass Into<Issue>.  You couldn't before because
it's a dynamic trait.

## [0.2.2] - 2026-01-04
- Been working on Canic since Boxing Day, so pushing a new release with the latest [0.7.6] version

## [0.2.1] - 2025-12-26 - 📦 Boxing Day 📦
- Float32/Float64 deserialization rejects non-finite values; `from_view` now panics on non-finite inputs to enforce invariants.
- more tests!

## [0.2.0] - 2025-12-25 - 🎄 Christmas Cleanup 🎄
- 3 crates removed: icydb_error, icydb_paths, icydb_base.  Much simpler dependency graph.
- Goodbye 1100+ lines of code
- Refactored Sanitize/Validate so that creating Validators and Sanitizers cannot panic, but instead Validator::new() errors get added to the error tree
- Visitor method now uses a context instead of recursive trees
- Visitor method now has a generic return Error method via the VisitorCore / VisitorAdapter pattern
- Paths are now automatically ::icydb because we do an `extern crate self as icydb`
- Merry Christmas!

--------------------------------------------------------------------------------------------------------------------------

## [0.1.20] - 2025-12-24
- Metrics: add `rows_scanned`, `exists_calls`, and `plan_full_scan`; count scan rows during loads, exists, and deletes; report average rows scanned per load.
- Timestamp parsing rejects pre-epoch RFC3339 values; negative `from_i64` returns `None`.
- Date: `Date::new` returns epoch for out-of-range years; `Date` no longer exposes a public `i32` field.
- Numeric types: `Duration`/`E8s`/`E18s` reject negative inputs for `from_i64` and `from_f64`.
- E18s: `to_decimal` now returns `None` on overflow instead of wrapping; display shows `[overflow]`.
- Validators/sanitizers: numeric validators return errors for invalid configs instead of panicking; clamp sanitization no-ops on invalid configs.
- Tests: added coverage for timestamp/date edge cases, negative numeric inputs, E18s overflow, and metrics exists/scan counters.

## [0.1.19] - 2025-12-23
- Fix upsert to resolve unique-index matches using sanitized input values.
- Add upsert result helpers that report whether a unique-index upsert inserted or updated.
- Add upsert merge helpers to apply update logic inside the executor.
- Rename `UniqueIndexSpec` to `UniqueIndexHandle` to clarify the unique-index upsert API.
- Move `FromKey` into the core traits module (path change for callers).
- Add strict unique-index delete via `DeleteExecutor::by_unique_index` with corruption checks.
- Save sanitizes entities before primary key extraction to keep keys/indexes consistent.
- Query planning for `IN` on primary keys returns empty results for empty lists and dedups keys.
- Index-backed loads now return deterministic key order by sorting index candidates.
- `DeleteExecutor::by_unique_index` now emits delete metrics.
- Index planning now skips non-indexable equality values to avoid false negatives.
- PK `IN` filters now error when any element is not convertible to a storage key.
- PK `IN` filters now accept text keys for identifiers (Ulid/Principal/Account).
- `LoadExecutor::exists` now respects caller-provided offset/limit (limit=0 returns false).
- Remove the `db!(debug)` macro arm; use `db!().debug()` for verbose tracing.

## [0.1.18] - 2025-12-21
- added Row<E>, Page<T> and into_page to Response
- Fix `LoadExecutor::exists`/`exists_filter` to honor filters when index plans are used.
- Add unique-index upsert executor for insert-or-update without primary key lookup.
- removed unused ThisError variant arms

## [0.1.17] - 2025-12-20
- LoadQuery/DeleteQuery gained explicit many_by_field helpers and keep PK-based many as a convenience wrapper.

## [0.1.16] - 2025-12-20
- got rid of the unused generic on insert/create/replace_view
- added insert/create/replace_many to the SaveExecutor

## [0.1.15] - 2025-12-20
### ➕ Added
- Added cardinality guards to `Response`: `require_some` and `require_len`, complementing existing `require_one`.
- Added delete-side executor helpers `ensure_deleted_one` and `ensure_deleted_any` to express strict deletion invariants without leaking `Response` handling into call sites.

### 🔧 Changed
- Simplified delete call sites by replacing per-row delete loops and manual response checks with executor-level `ensure_deleted_*` helpers.

### 📝 Summary
- Happy birthday me!

## [0.1.14] - 2025-12-19
- started on the aggregation layer with group_count_by in LoadExecutor.  Not added to the Response because we need the
Executor to decide whether it's needs to deserialize rows or not (slow vs fast path)

## [0.1.13] - 2025-12-19
- added more existence checks to the ResponseExt helper

## [0.1.12] - 2025-12-19
- `Ulid::generate` and `Subaccount::random` now fall back to zeroed randomness when the RNG is unseeded, avoiding error surfaces.

## [0.1.11] - 2025-12-19
- You can now apply pre-built filters directly to queries, instead of wrapping them in awkward closures. This makes it easier to reuse filters and removes boilerplate in many call sites.
- Handling query results is now cleaner: you can interpret results (get entities, views, primary keys, counts, etc.) directly on the query call without extra mapping or intermediate ? operators.

## [0.1.10] - 2025-12-18
- introduced a ResponseExt helper to chain errors and make the call sites more ergonomic
- added .first(), .first_entity() to response
- put views() and other forgotten methods into ResponseExt
- added count() and pks() too

## [0.1.7] - 2025-12-18
- Improved how database queries return results so that “one item” vs “many items” is handled consistently and safely.
- Removed a number of convenience shortcuts that could silently return the wrong record when multiple matches existed.
- Simplified how queries that fetch a single record are written and interpreted.

## [0.1.5] - 2025-12-17
- added FilterExpr::method for all the clauses to improve idionomicy.  Before FilterExpr::eq(field, value) was falling
back to PartialEq
- fixed CI so it won't bug out on a new rust toolchain on CI but not locally
- Clippy, WHY?!?!  We were so close.  Fixing local to show clippy errors that CI errors on, so we don't get the github
email of shame.
- Fixed a bug where UpdateView<T> wasn't clearing the value when Some(None) was passed

## [0.1.1] - 2025-12-12
- removed msg_caller from Principal as it blurs system call boundaries
- pass through from_text to WrappedPrincipal

## [0.1.0] - 2025-12-12 - Somewhat Stable
- Update rust to 1.92.0
- Lots of changes because of the new canic crates (the location of utils and macros changed)
- clippy and cargo machete passes

## [0.0.20] - 2025-12-09
- Fix `DeleteExecutor` to honor `offset`/`limit` after filtering and stop scanning once the window is satisfied, preventing over-deletes and unnecessary allocations on ranged or indexed deletes.
- Extract shared query-plan scanning/deserialization helper used by load/delete executors to keep plan handling consistent while preserving existing filtering/pagination behaviour.

## [0.0.15] - 2025-12-08
- Added payload-aware enums: `ValueEnum` now carries payloads, hashing/equality include them, and enum FieldValue impls preserve payload data (fixes ICRC token amounts, etc.).
- Broadened FieldValue support to `Box<T>`/`Vec<T>` so nested/boxed schema values (e.g., ICRC-3 arrays of boxed values) serialize and index correctly.
- Added design/runtime tests to lock in enum payload persistence and boxed-value handling.
- Moved the `build!` macro into `icydb-build` and re-exported from the meta crate to keep runtime crates free of build-script deps.

## [0.0.14] - 2025-12-06
- removed dependency on canic, as the canic-core and canic-memory are now separate crates.  Will do further fixing/renaming soon.

## [0.0.13] - 2025-12-05
- Added unit tests for schema identifier validation, crate path resolution, metrics reporting, and FNV hashing; documented public macros for codegen/startup helpers.
- Renamed Icp/Icrc Payment and Amount because the Tokens struct name is confusing

## [0.0.11] - 2025-12-04
- updated the Timestamp type to have tests, from_seconds/millis/micros/nanos, and also have the chrono RFC3339 parsing

## [0.0.10] - 2025-12-04
- Removed the unauthenticated `icydb_query_*` canister endpoints; codegen now emits internal dispatch helpers so callers can enforce auth before invoking load/save/delete handlers.

## [0.0.9] - 2025-12-04
- upgrade to canic 0.4.8
- scan of public endpoints, either add documentation to them or change to pub(crate)

## [0.0.8] - 2025-12-03
- upgrading to new canic 0.4
- darling got yanked so cleaning that up (fixed shortly after)
- added rustdoc coverage for public APIs (value helpers, db queries/responses, core types) and tightened proc-macro helper visibilities

## [0.0.6] - 2025-11-27
- added finance types to icydb-base, Usd for now
- RoundDecimalPlaces sanitizer, defaults to Midpoint strategy
- quick trim with cargo machete
- fixed the mismatch with indirect (Box<T>) and the associated view type
- moved VERSION to main crate
- changed ValueEnum so that the path is optional, to allow strict and loose Enum matching
- made Enum matching case-insensitive, so "common" would match Rarity::Common

## [0.0.1] - IcyDB Reboot - 2025-11-26

```
   _________
  /        /|
 /  DATA  / |
/________/  |
|  COOL  |  /
|  ❄❄❄ | /
|________|/
 keep data cool
```

- New name, same mission: IcyDB takes over from Mimic with the public meta-crate exposed at `icydb`.
- Docs and guides refreshed to point at `icydb` tags, endpoints, and examples so newcomers can copy-paste.
- Path resolver now prefers `icydb::` for downstream users while keeping internal crates on direct deps to avoid cycles.
- Observability/query endpoints and codegen names align on the `icydb_*` prefix for a consistent surface.

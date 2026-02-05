# IcyDB Changelog

All notable, and occasionally less notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [0.7.0] – 2026-02-05 - Referential Integrity, Part III

### 🧁 Summary

0.7.0 finalizes the referential-integrity redesign introduced across late 0.6.x.

Relation intent is now explicit (strong vs weak), and save-time referential integrity is enforced only for relations explicitly marked strong. Enforcement is deterministic, schema-driven, and occurs during save preflight, with no load-time checks, inference, or cascading behavior.

This release locks in the new RI contract and stabilizes runtime behavior.
Write paths now refuse to proceed while a prior commit marker is present, ensuring no mutation
stacks on top of an incomplete commit.

### 🥨 Added

* Added `strong`/`weak` relation flags in the schema DSL, with `weak` as the default.
* Added relation strength metadata to `EntityFieldKind::Ref` for runtime consumers.
* Added `Response::reference`, `Response::try_reference`, and `Response::references` to return typed `Ref<E>` results from query responses.
* Added a `Display` derive in `icydb-derive` for tuple newtypes.
* Added collection types `OrderedList`, `UniqueList`, `KeyedList`, and `RefSet` for explicit many-field semantics.
* Added `OrderedList::retain` plus `apply_patches` helpers on `OrderedList` and `RefSet` for explicit patch application.
* Added `docs/collections.md` as the contract reference for collection and patch semantics.
* Added `saturating_add`/`saturating_sub` helpers to arithmetic newtypes for explicit saturating math.
* Added `Id<E>` as a typed primary-key wrapper for entity identities, distinct from `Ref<E>`.
* Added `Identifies<E>` as an internal capability trait for extracting entity keys from `Id<E>` or `Ref<E>`.

### 🪼 Changed

* Save operations now enforce referential integrity for `RelationStrength::Strong` fields and fail if targets are missing.
* Write executors now perform a fast commit-marker check and replay recovery before mutations when needed; read recovery remains startup-only.
* Many relation fields now emit `RefSet<T>` to enforce unique, key-ordered references and align update semantics with `SetPatch`.

### 🧢 Breaking

* `EntityFieldKind::Ref` now carries target entity/store metadata and relation strength, so downstream enum matches must update.
* Entity and record fields with `many` cardinality now emit `OrderedList<T>` instead of `Vec<T>`.
* Relation fields with `many` cardinality now emit `RefSet<T>` instead of list types like `Vec<Ref<T>>`.
* Entity primary-key fields now emit `Id<E>` instead of raw key values, so struct literals and accessors must wrap/unwrap explicitly.

---

## [0.6.20] – 2026-02-04

### 🪿 Added

* Added `Blob::as_bytes()` and `Blob::as_mut_bytes()` for explicit byte access without deref.

### 🧷 Changed

* Relation/external field suffix bans now apply only to relation and external fields (not arbitrary primitives like `my_api_id`).

### 🪁 Fixed

* Made `Ref<T>` `Sync + Send` to fix the `*const` variant.

---

## [0.6.17] – 2026-02-03 - Query Ergonomics

### 🦩 Added

* Added `WriteResponse`/`WriteBatchResponse` helpers for write results, including key and view accessors.
* Added `Nat::to_i64`/`to_u64` and `Int::to_i64`/`to_u64` for explicit integer conversion without deref.
* Added by_ref() for query flow
* Added many_refs() for query flow

### 🥪 Changed

* id_strict and key_strict to require_id and require_key to match other methods
* Clarified schema error messaging for banned suffixes on field names

### 🧯 Breaking

* Schema field names ending in `_id`, `_ids`, `_ref`, `_refs`, `_key`, or `_keys` now fail at compile time; relation fields were renamed to base nouns.
* Singleton query `only()` no longer accepts an explicit ID and always uses the default singleton key.

---

## [0.6.11] – 2026-02-03 - Decimals, Collections and Stuff

### 🦩 Added

* Added a `get()` accessor to map collection inherent traits for explicit lookup without deref.
* Added `Decimal::abs()` to expose absolute value math without deref.
* Added `Blob::to_vec()` for explicit byte cloning without deref.

### 🥪 Changed

* Planner access planning no longer re-validates predicates; validation is now owned by the intent/executor boundaries.
* Consolidated primary-key compatibility checks to the shared `FieldType::is_keyable` rule to avoid drift across planner/validator layers.
* Renamed primary_key() and similar methods in Response to key() for consistency

### 🪁 Breaking

* `MapCollection::iter` now returns a GAT-backed iterator instead of a boxed trait object, so implementations and type annotations must update.
* `Collection::iter` now returns a GAT-backed iterator instead of a boxed trait object, so implementations and type annotations must update.
* `DbSession::insert`/`replace`/`update` now return `WriteResponse<E>` (and batch variants return `Vec<WriteResponse<E>>`).

---

## [0.6.6] – 2026-02-03 - Diagnostic Test Reenablement

### 🧁 Summary

* Re-enabled query plan explain, fingerprint, and validation tests to guard planner determinism and invariants after the refactor.

### 🛰️ Added

* Added `ByKeys` determinism checks for `ExplainPlan` and `PlanFingerprint` to lock in set semantics for key batches.
* Added a typed-vs-model planning equivalence test to anchor `QueryModel`/`Query<E>` parity post-refactor.

---

## [0.6.5] – 2026-02-03 - Derive Consolidation & Explicit Collections

### 🧊 Summary

* Introduced `QueryModel` to separate model-level intent, validation, and planning from typed `Query<E>` wrappers, reducing trait coupling in query logic.
* Added the `icydb-derive` proc-macro crate for arithmetic and ordering derives on schema-generated types.
* Relocated canister-centric tests to PocketIC-backed flows and removed canister builds from default `make test` runs.

### 🍉 Added

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

### 🧭 Changed

* Newtype arithmetic derives now route through `icydb-derive` (including `Div`/`DivAssign`) instead of `derive_more`.
* `test_entity!` now requires an explicit `struct` block and derives `EntityKind::Id` from the primary key field’s Rust type, failing at compile time if the PK is missing from the struct or `fields {}`.
* `FieldValues` is now derived via `icydb-derive` and no longer implemented by schema-specific `imp` code.
* `DbSession::diagnose_query` now requires `EntityKind` only, keeping diagnostics schema-level.
* Public query builders now accept `EntityKind` for intent construction; execution continues to require `EntityValue`.
* Updated `canic` to `0.9.17`.
* `make test` no longer runs canister builds; `test-canisters` is now a no-op.

### 🪂 Removed

* Removed schema-derive `imp` implementations for `Add`/`AddAssign`/`Sub`/`SubAssign` in favor of derives.
* Removed `Display` trait from schema-derive

### 🧵 Fixed

* Exported `Div`/`DivAssign` through `traits` so generated arithmetic derives resolve cleanly.
* Session write APIs and query execution now require `EntityValue`, aligning runtime execution with value-level access.
* `#[newtype]` now derives `Rem` only for primitives that support remainder, and `Int128`/`Nat128` implement `Rem` to match numeric newtype expectations.

---

## [0.6.4] – 2026-02-01 - Explicit Key Boundaries

### 🧱 Changed

* Removed `Into<...>` from `by_key` functions to keep primary key boundaries explicit.

---

## [0.6.3] – 2026-02-01 - Primary Key Guardrails

### 🪱 Fixed

* Entity macros now reject relation fields as primary keys, preventing `Ref<T>` from being used as an identity type.
* Primary key fields must have cardinality `One`; optional or many primary keys now fail at macro expansion time.
* Local schema invariants now fail fast during macro expansion, including field identifier rules, enum variant ordering, and redundant index prefix checks.
* Added compile-fail tests covering relation and non-One primary key shapes in the entity macro.

### 🧪 Summary

* Locked primary key invariants at macro expansion time to avoid downstream RI violations.

---

## [0.6.1] – 2026-02-01 - Referential Integrity, Part II

### 🧉 Added

* **Save-time referential integrity (RI v2)**: direct `Ref<T>` and `Option<Ref<T>>` fields are now validated pre-commit; saves fail if the referenced target row is missing.
* Added `docs/REF_INTEGRITY_v2.md`, defining the v2 RI contract, including:

  * strong vs weak reference shapes,
  * atomicity boundaries,
  * and explicit non-recursive enforcement rules.
* Added targeted RI tests covering:

  * strong reference failure on missing targets,
  * allowance of weak reference shapes,
  * and non-enforcement of references during delete operations.

### 🧷 Changed

* Nested and collection reference shapes (`Ref<T>` inside records/enums, and `Vec`/`Set`/`Map<Ref<T>>`) are now **explicitly treated as weak** at runtime and no longer trigger invariant violations during save.
* Clarified that schema-level relation validation is **advisory only** and does not imply runtime RI enforcement.
* Aligned runtime behavior, schema comments, and documentation with the RI v2 contract.

### 🧻 Summary

* Introduced **minimal, explicit save-time referential integrity** for direct references only, while formally defining and locking the weak-reference contract for all other shapes.

---

## [0.6.0] – 2026-01-31 - Referential Integrity, Part I

### 🪐 Breaking
* Index storage now splits data and index stores explicitly; index stores require separate entry and fingerprint memories.
* `IndexStore::init` now requires both entry and fingerprint memories; constructing an index store without fingerprint memory is no longer possible.

### 🥖 Added
* Added dedicated index fingerprint storage to keep verification data independent from index routing entries.
* Added a cross-canister relation validation test with a dedicated relation canister to lock in the new schema invariant.

### 🪿 Fixed
* ORDER BY now preserves input order deterministically for incomparable values.
* Commit marker apply now rejects malformed index ops or unexpected delete payloads in release builds.
* Commit marker decoding now rejects unknown fields instead of silently ignoring them.
* Commit marker decoding now honors the marker size limit instead of the default row size cap.
* Oversized commit markers now surface invariant violations instead of corruption.

### 🎿 Changed
* Documented that `FieldRef` and `FilterExpr` use different coercion defaults for ordering; see `docs/QUERY_BUILDER.md`.
* Consolidated build-time schema validation behind `validate::validate_schema` so all passes run through a single entrypoint.

### 🧯 Summary
* Logged the 0.6 atomicity audit results, including the read-path recovery mismatch, for follow-up.

---

## [0.5.25] – 2026-01-30

### 🧲 Breaking
* Case-insensitive coercions are now rejected for non-text fields, including identifiers and numeric types.
* Text substring matching must use `TextContains`/`TextContainsCi`; `CompareOp::Contains` on text fields is invalid.
* ORDER BY now rejects unsupported or non-orderable fields instead of silently preserving input order.

### 🧻 Changed
* Executor ordering tests now sort only on orderable fields while preserving tie stability and secondary ordering guarantees.
* Conducted a DRY / legacy sweep across query session, executor, and plan layers to remove duplicated or misleading APIs.

---

## [0.5.24] – 2026-01-30

### 🪤 Fixed
- replaced FilterExpr helpers that were accidentally removed

---

## [0.5.23] – 2026-01-30

### 🪤 Fixed

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

### 🧵 Changed

* Recovery-guarded read access is now enforced via `Db::recovered_context`; raw store accessors are crate-private.
* `storage_report` now enforces recovery before collecting snapshots.
* `FilterExpr` now represents null / missing / empty checks explicitly, matching core predicate semantics.
* Dynamic filters now expose case-insensitive comparisons and text operators without embedding coercion flags in values.
* Map and membership predicates (`not_in`, map-contains variants) are now available via `FilterExpr`.

### 🥌 Removed

* Dropped the unused projection surface (`ProjectionSpec` and related plan/query fields) to avoid false affordances.

### 🪙 Breaking

* `obs::snapshot::storage_report` now returns `Result<StorageReport, InternalError>` instead of `StorageReport`.

---


## [0.5.22] - 2026-01-29

### 🧭 Fixed
* Unique index validation now treats index/data key mismatches as corruption, preventing hash-collision or conflict misclassification.
* Delete limits now treat empty sort expressions as missing ordering, avoiding nondeterministic delete ordering.

### 🦉 Changed
* Empty `many([])` / `ByKeys([])` is now a defined no-op that returns an empty result set.

### 🧵 Removed
* Removed legacy index mutation helpers (`IndexStore::insert_index_entry`, `IndexStore::remove_index_entry`) and the unused `load_existing_index_entry` helper.

---

## [0.5.21] - 2026-01-29

### 🪗 Added
* Added enum filter helpers (`EnumValue`, `Value::from_enum`, `Value::enum_strict`) and `FieldRef::eq_none` to make enum/null predicates ergonomic without changing planners or wire formats.
* Added ergonomic helpers to FilterExpr, ie. `FilterExpr::eq()`

---

## [0.5.15] - 2026-01-29

### 🦑 Fixed
* `only()` now works for singleton entities whose primary key is `()` or `types::Unit`, keeping unit keys explicit without leaking internal representations.

### 🪑 Added
* Session load/delete queries now expose `Response` terminal helpers directly (for example `row`, `keys`, `primary_keys`, and `require_one`), so applications can avoid handling `Response` explicitly.

### 🧻 Changed
* Load query offsets now use `u32` across intent, planning, and session APIs.
* Also count is u32

---

## [0.5.13] - 2026-01-29

### 🧃 Added
* Added dynamic query expressions (`FilterExpr`, `SortExpr`) that lower into validated predicates and order specs at the intent boundary.
* Session load/delete queries now expose `filter_expr` and `sort_expr` to attach dynamic filters and sorting safely.
* Re-exported expression types in the public query module for API endpoints that accept user-supplied filters or ordering.
* Facade versions of FilterExpr and SortExpr

---

## [0.5.11] - 2026-01-29

### 🧁 Changed
* View-to-entity conversions are now infallible; view values are treated as canonical state.
* Create/view-derived entity conversions now use `From` instead of `TryFrom`.
* Float view inputs now normalize `NaN`, infinities, and `-0.0` to `0.0` during conversion.
* Removed `ViewError` plumbing from view conversion and update merge paths.

### 🪁 Breaking
* `View::from_view` and `UpdateView::merge` no longer return `Result`, and conversion errors are no longer surfaced at the view boundary.

---

## [0.5.10] - 2026-01-29

### 🪐 Added
* Restored key-only query helpers: `only()` for singleton entities and `many()` for primary-key batch access.
* Added `text_contains` and `text_contains_ci` predicates for explicit substring searches on text fields.
* Session query execution now returns the facade `Response`, keeping core response types out of the public API.

### 🧩 Fixed
* Cardinality errors now surface as `NotFound`/`Conflict` instead of internal failures when interpreting query responses.

---

## [0.5.7] - 2026-01-28

### 🪁 Added
* Generated entity field constants now use `FieldRef`, enabling predicate helpers like `Asset::ID.in_list(&ids)` without changing planner or executor behavior.
* Load and delete queries now support `many` for primary-key batch lookups, using key-based access instead of predicate scans.
* Singleton entities with unit primary keys can use `only()` on load/delete queries for key-only access.

### 🥝 Fixed
* The `icydb` load facade now exposes `count()` and `exists()` terminals.
* Delete queries now treat zero affected rows as a valid, idempotent outcome in the session facade.

---

## [0.5.6] - 2026-01-28

### 🧲 Added
* Load queries now expose view terminals (`views`, `view`, `view_opt`) so callers can materialize read-only views directly.
* `Response` now provides view helpers (`views`, `view`, `view_opt`) to keep view materialization explicit at the terminal.
* Predicates now support `&` composition for building conjunctions inline.

### 🐚 Changed
* `key()` on load and delete session queries now accepts any type convertible into `Key`.

---

## [0.5.4] - 2026-01-28

### 🛴 Added
* `key()` is now available on both session query types for consistent access to key-based lookups.

---

## [0.5.2] - 2026-01-28 - Public Facade Boundary

### 🍕 Fixed
* Public query methods now return `icydb::Error`, so low-level internal errors no longer leak into app code.
* You can no longer call executors or internal query execution paths from the public `icydb` API.
* Removed `core_db()` and similar test-only backdoors that skipped the public API entirely.
* Removed cross-canister query plumbing and erased-plan interfaces that exposed internal execution details.

### 🦄 Changed
* `db!()` now always returns the public `icydb` session wrapper, not the internal core session.
* Queries must be executed through the session’s load/delete helpers; executors are now core-only.
* Low-level executor corruption tests were removed from the public test suite.

### 🤡 Removed
* Entity-based query dispatch (`EntityDispatch`, `dispatch_load/save/delete`) and canister-to-canister query handling.
* “Save query” abstractions — writes are now only done via explicit insert/replace/update APIs.
* Tests that depended on calling executors directly outside of `icydb-core`.
* Dropped `upsert` support and the related code paths (~800 lines).

---

## [0.5.1] - 2026-01-28 - Redesigned Query Builder

### 🦴 Fixed
* Executors now reject mismatched plan modes (load vs delete) with a typed `Unsupported` error instead of trapping.

### 🧃 Changed
* Query diagnostics now surface composite access shapes in trace access (union/intersection).
* Executor trace events include per-phase row counts (access, filter, order, page/delete limit).
* Fluent queries now start with explicit `DbSession::load`/`DbSession::delete` entry points (no implicit mode switching).
* Pagination and delete limits are expressed via `offset()`/`limit()` on mode-specific intents.

---

## [0.5.0] – 2026-01-24 – Query Engine v2 (Stabilization Release)

This release completes the **Query Engine v2 stabilization** effort. It introduces a typed, intent-driven query facade, seals executor boundaries, and formalizes correctness, atomicity, and testing contracts.

The focus is **correctness, determinism, and architectural hardening**, not new end-user features.

---

### 🧯 Added

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

### 🦴 Fixed

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
* Removed panicking public identity constructors in favor of fallible APIs; unchecked constructors are crate-private for generated models.
* Updated README and internal docs to reflect the actual query execution and atomicity model.

---

### 🧃 Changed

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
* Updated minimum supported Rust version to **1.93.0** (edition 2024).

---

### 🧦 Removed

* v1 query DSL and legacy builder APIs.
* Public execution or construction of logical plans.
* Implicit read semantics.
* Executor-side validation and planning logic.
* Schema-parameterized planning APIs.
* Response-level pagination helpers (`Page`, `into_page`, `has_more`).
* Internal plan re-exports from the public facade.
* Plan cache, removed as a premature optimization; planning is deterministic and cheap.

---

### ⚠️ Migration Notes

This release contains **intentional breaking changes**:

* All queries must be rewritten using `Query<E>` and explicitly planned before execution.
* Direct use of `LogicalPlan` or untyped query builders is no longer supported.
* Code relying on implicit missing-row behavior must now choose a consistency policy.
* Pagination must be expressed at intent time, not derived from execution results.

These changes are foundational. Future releases are expected to be **additive or performance-focused**, not corrective.

---

### 📌 Summary

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

### 🚨 Breaking Changes

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

### 🧱 Storage & Indexing

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

### 🧠 Planner & Execution Model

* **Planner is now side-effect free**

  * Planning no longer mutates state or emits metrics
  * All plan-kind metrics are emitted during execution only
  * Enables deterministic planning and easier reasoning about execution paths

---

### 🧩 Identity & Naming

* **IndexName sizing is now derived and validated**

  * Computed from:

    * Entity name (≤ 48 chars)
    * Up to 4 indexed field names (≤ 48 chars each)
  * Boundary checks enforced in:

    * Core storage
    * Schema validators
  * Prevents silent truncation and oversized index identifiers

---

### 🛡️ Data Integrity & Corruption Detection

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

### 🧪 Developer Impact

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
### Changed
- 🧱 Major layering refactor: icydb is now a strict public facade over icydb-core, with internal subsystems depending directly on core rather than facade modules.
- 🔌 Clear API boundaries: Engine internals (execution, queries, serialization, validation) are fully isolated in icydb-core; icydb exposes only intentional, stable entry points.
- 📦 Public query surface: icydb::db::query is now a supported public API and re-exports core query types for direct use.
- 🛠️ New facade utilities: Added top-level serialize, deserialize, sanitize, and validate helpers with normalized public errors.
- 🔒 Hardened macros & executors: Generated code now targets canonical core paths, preventing accidental API leakage.

### Impact
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
### Added
- Added cardinality guards to `Response`: `require_some` and `require_len`, complementing existing `require_one`.
- Added delete-side executor helpers `ensure_deleted_one` and `ensure_deleted_any` to express strict deletion invariants without leaking `Response` handling into call sites.

### Changed
- Simplified delete call sites by replacing per-row delete loops and manual response checks with executor-level `ensure_deleted_*` helpers.

### Other
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

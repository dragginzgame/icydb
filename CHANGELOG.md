# IcyDB Changelog

All notable, and occasionally less notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]
### Changed
- Public `Error` now exposes `class` and `origin` alongside the message.

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

--------------------------------------end icydb-----------------------------------------------
-------------------------------------start mimic----------------------------------------------

## [0.29] - Public Release Prep - 2025-11-19
- Rewrote the README with public-facing positioning, expanded quickstart steps, and detailed onboarding/contribution info for new users.
- Clarified repository status and support expectations as Mimic moves from a private to a public home.
- Updated documentation references to the latest tag (`v0.29.0`) so adopters can pin to the launch release.
- Fixed a bug where tuples only had the first pub
- Added the Merge functionality so any type can have .merge() on an UpdateView, and it works recursively.
- Added a new coercion module that handles filtering/search term conversion plus a robust test suite

## [0.28] - Filter Overhaul - 2025-11-14
- Overhauled the filter ergonomics across Mimic: every filter payload (ContainsFilter, EqualityFilter,
  RangeFilter, SetFilter, TextFilter) now ships with fluent, #[must_use]-annotated builders so schema-generated filter
  structs can be composed inline without hand-mutating options.

## [0.26] - 2025-11-01
- Filter types now contain helpful filter structs depending on the field type
- split and cleaned up the View traits in mimic_declare so there's a lot less mixed responsibility
- merged Create and Update to EditView
- removed EnumValue as it should really just be an Enum
- we arent deriving Default for enums as the serde code can get messy for multiple view types
- Selector and EntityId removed too, we're down to the basic ORM types

## [0.25] - 2025-10-22
- new View type - Create<Entity> which takes off the primary key and provides handy from() functionality
- added Update<Entity>
- moved the Views out of traits:: into view::
- added in merge() for CreateView
- added UpdateView for Record types
- added a Newtype check so that the provided inner Primitive type matches the inner Item
- Entity now has ::PRIMARY_KEY and primary_key(), so we can skip Value->Key if we want
- simplified queries, they all return Response<E> now, so its easy to do .keys(), .pks()
- added the Unit type as a possible Key, for entities with only one row
- added only() to go with one().  it calls one() with the unit type
- added Filter and Sort views
- direct validation rules now work on Map, Set and List (for instance len::Max)

## [0.24.35] - 2025-10-22
- it wasn't possible to create a newtype around a non-Primitive but that functionality is now back

## [0.24.29] - 2025-10-21
- removed the EntityLifecycle traits, and now we just have a simple CreatedAt/UpdatedAt
that mutates via the sanitizer that can be used multiple places

## [0.24.21] - 2025-10-19
- added a primitive Account type as we need to start using it in indexes
- also added Account as a potential Key type

## [0.24.19] - 2025-10-19
- added a primitive Date type (using i32 like chrono::NaiveDate)

## [0.24.16] - 2025-10-18
- removed todo functionality as it didn't feel like it was part of the data model
- added web::{Url, MimeType} sanitizers

## [0.24.13] - 2025-10-16
- changed create() to insert() to align naming with SQL, as potentially one day the query and storage
engines could be swapped out (plus makes more sense for the LLM)

## [0.24.7] - 2025-10-11
- added Alphanumeric and Numeric sanitizers

## [0.24.6] - 2025-10-10
- PartialEq and Ord for Decimal <-> WrappedDecimal

## [0.24.4] - 2025-10-08
- added a sort_by helper (field, SortDirection) to make writing queries easier

## [0.24.3] - 2025-10-08
- changed the db api to consume query to match other ORMs

## [0.24.2] - 2025-10-07
- redid the debug for db!  so now you either do db!() or db!().debug() and it passes the top level
debug boolean all the way through
- much better query debugging for load queries
- got rid of the sentinel values in Value and now use FullScan
- added back the Inner trait, hopefully for the last time

## [0.24.1] - 2025-10-06
- updated icu to canic 0.1!

## [0.24.0] - RIP Fixtures - 2025-10-03
- fixtures have been removed, if needed they should not be in the design/ crate
- renamed name to ident just to keep the code consistente

## [0.23.5] - 2025-09-30
- subaccount now has to/from Ulid

## [0.23.4] - 2025-09-30
- schema check for duplicate memory_ids in a Canister
- made the filter_nodes() generic easier to use

## [0.23.0] - 2025-09-29
- Raised the minimum supported Rust version to 1.90.0 and refreshed CI/docs references.
- Added newtype PartialEq/PartialOrd combinations that were missing
- Added a Duration primitive type to differentiate from Timestamp
- Added Subaccount::random()
- rewrote all the Arg/ArgNumber default/into traits, now they can also accept constants and paths
- removed the Constant macro as it's not really part of the data model

## [0.21.0] - Sanitizers Working

### Added
- Expanded the text case sanitizers with snake, kebab, title, and upper-camel converters so all common case transforms are reusable in schema definitions.
- Introduced design-test newtypes that exercise case validators and sanitizers inside lists, maps, and sets to verify the visitor implementations.

## [0.20.3] - Return of the Sanitizer
- serde / candid issue fixed
- moved validate traits to separate file and fixed a bug with Box, Vec, Option etc.
- changed numeric validators to use Clone instead of Copy

## [0.19.13] - 2025-09-15
- pinned serde to an older version

## [0.19.12] - 2025-09-14
- added more validation to the ISO validators and added 3166
- added the phonenumber crate for phone number validation, split iso into intl/iso

## [0.19.8] - 2025-09-09
- Decimals now serialize as strings
- added powu to Decimal plus the MathematicalOps trait
- added Sum to numeric types

## [0.19.1] - 2025-09-08

### Changed
- CI cleanup: removed separate canisters job; build and upload canister artifacts only in release.
- CI hygiene: single fmt check (no cargo-sort), reproducible installs (`--locked`), cache-lock cleanup, concurrency group, colored logs.

## [0.19.0] - 2025-09-08

### Added
- Git hooks: repository-tracked pre-commit at `.githooks/pre-commit` running `cargo fmt --check`, `cargo sort --check`, and `cargo sort-derives --check`.
- Tooling: `Makefile` `ensure-hooks` auto-configures `core.hooksPath` to `.githooks`; `install-dev` installs `cargo-sort` and `cargo-sort-derives`.
- Env: `scripts/env/update.sh` now sets local `core.hooksPath` to `.githooks`.

### Changed
- Value: unified collection/text helpers via small internal comparators (`contains_by`, `contains_any_by`, `contains_all_by`, `in_list_by`) and a `text_op` helper, reducing duplication while preserving behavior.
- Value (CI text ops): centralized case-insensitive equality via `eq_ci`; clarified Unicode folding note (temporary `to_lowercase`, future NFKC+casefold).
- Metrics docs: clarified `EventReport` comment to reflect event/counter focus; codegen `mimic_metrics` comment now references internal `since_ms` and reset.
- Schema Store docs: clarified that the type describes a stable IC BTreeMap store (schema node), not a runtime store.
- ULID docs: clarified why Serialize/Deserialize are implemented locally (crate features off by default to avoid `rand`).
- CI: removed `cargo sort` checks (enforced via pre-commit); keep a single `cargo fmt --check`.
- CI: clear stale Cargo package cache lock (`~/.cargo/.package-cache`) before running cargo.
- CI: add `security` job running `cargo audit --deny warnings` for PRs and non-tag pushes.
- CI: build `test_canister` to WASM in release; extract `.did` and upload artifacts.

### Removed
- Codex CLI config `codex.yaml`; guidance folded into `AGENTS.md` and README updated.

### Performance
- IndexStore (UNIQUE): skip redundant write when the key is already indexed; still records unique violations for conflicts.
- Value (CI membership): precompute folded strings once in `contains_ci`/`in_list_ci` to reduce allocations; generic helpers simplify `contains_any_ci`/`contains_all_ci`.
- Visitor: small pre-allocation in `ValidateVisitor::current_route`.

## [0.17.0] - 2025-09-03

### Added
- obs module consolidating observability:
  - obs::metrics: runtime counters/perf with `EventReport`, `EventState`, `EventOps`, `EventPerf`, `EntityCounters`, `EntitySummary`, `EventSelect` and helpers (`report()`, `reset_all()`).
  - obs::log: in-memory ring-buffer logs with `log_push`, `logs_snapshot`, `logs_reset`.
  - obs::snapshot: storage inventory with `StorageReport` (`DataStoreSnapshot`, `IndexStoreSnapshot`, `EntitySnapshot`) and `storage_report()`.
- Codegen endpoints:
  - `mimic_snapshot()` → `obs::snapshot::StorageReport`.
  - `mimic_metrics()` → `obs::metrics::EventReport`.
  - `mimic_metrics_reset()` resets runtime counters.
  - `mimic_logs()` returns current event logs.
- core/serialize counters: `serialize_call_count()`, `deserialize_call_count()`, `reset_serialize_counters()` using thread‑local `Cell` (no atomics).
- core/visit tests asserting dotted error paths for root, record+vec, nested record/tuple/map.

### Changed
- Visitor optimizations: `Event` is `Copy+Clone`; `perform_visit` inlined; avoid route build at root; Option wrappers do not push a path segment; path segments use `Field(&'static str)` and `Index(usize)`; faster `current_route` builder.
- Renamed report/selector types: `MetricsReport` → `EventReport`, `MetricsSelect` → `EventSelect`.
- Moved metrics/logs/snapshot under `obs/`; removed interface glue for metrics/snapshot (endpoints call modules directly). Query client helpers remain under `interface/query`.
- Executors/planner/store now record counters via `obs::metrics` (unique violation, plan kinds, index ops).

### Performance
- Reduced string allocations in visitor path building and validation routing.
- Single‑threaded serialize/deserialize counters remove atomic overhead.


## [0.16.2] - 2025-09-02

### Added
- `mimic_metrics(select)` query endpoint with `EventSelect` to choose data/index/counters/entities.
- Library helpers: `mimic::interface::metrics::{metrics_report, metrics_reset}` for endpoint delegation.
- Per-entity summaries (`EntitySummary`) including averages; global counters and perf totals remain.

### Changed
- Consolidated all “stats” naming under “metrics”; removed legacy `*Stats` type aliases.
- Codegen generates minimal glue and delegates to `interface::metrics` (cleaner, DRYer endpoints).
- Executors: atomic unique-index validation prevents partial index updates on failed saves; violation still counted.
- DRY refactor: shared `executor::plan_for::<E>(filter)` used by load/delete.

### Fixed
- Canister tests now clear both data and index stores between tests to avoid stale unique entries.

### Performance
- Minor allocation reductions and borrow-first tweaks in planner/index paths.

## [0.15.6] - 2025-09-01

### Added
- Codex CLI configuration (`codex.yaml`) with common commands and workflows.
- README "Using Codex CLI" section with sample invocations.
- `make test` now conditionally runs `scripts/app/test.sh` when `dfx` is available.

### Changed
- Float TypeView: `Float32::from_view` and `Float64::from_view` now preserve invariants
  (finite only; canonicalize -0.0 → 0.0) and fall back to `0.0` on NaN/Inf.
- ULID: added `Ulid::try_generate()` and made `Deserialize` return a serde error on invalid strings.
- API docs across core/types clarified constructor behavior and invariants
  (E8s/E18s scaling; Blob Display semantics; Principal parsing).
- Renamed `Float32::new_clamped` to `new_or_zero` for clarity.

### Removed
- `CONTRIBUTING.md` and its link from `RELEASE_GUIDE.md`.

### Performance
- Index store: avoid cloning `IndexEntry` in `remove_index_entry` by moving updated entry back.
- Pre-allocations: reserve capacity where sizes are known (query planner index values; range pagination; delete executor; index id buffer).

### Documentation
- Added a TypeView mapping overview in `core/types/mod.rs`.
- Expanded E8s/E18s docs to spell out constructors and behavior.

## [0.15.3] - 2025-09-91

### Added
- AGENTS.md contributor guide with structure, commands, and conventions.
- PR template to standardize submissions.
- GitHub Actions: CI (fmt, clippy, tests), Security Check, and Release workflows.
- Branch protection guide (.github/BRANCH_PROTECTION.md).
- Filter validation: comparator-to-value type checks in `FilterExpr` validation
  (e.g., ordering requires comparable RHS (numeric or text); CI text ops require text). New
  `QueryError` variants: `InvalidFilterValue`, `InvalidComparator`.
- Tests: negative cases for invalid comparator/value combos (e.g., `lt(name, 3)`,
  `starts_with(name, 1)`, `eq_ci(level, 1)`, CI membership with non-text lists,
  presence checks with non-Unit RHS).
 - Value helpers: `Value::is_numeric()` and `Value::is_text()` to centralize
   type guards used by validation and executors.

### Changed
- README: updated MSRV badge (1.89+), license link, concise workspace/commands, full crates list, badges.
- Replaced several unwraps with expect and clearer panic messages in codegen/schema paths and mutex/RwLock usage.
- Macro ergonomics: emit compile_error! for unsupported Arg types instead of unimplemented!.
- Removed redundant #[allow(unused)] in generated Default impl.
- Trimmed println! from tests; behavior unchanged.
- Fixed clippy lint name in Cargo.toml (unnecessary_cast).
- Index prefix scans: optimized `IndexStore::iter_with_hashed_prefix` to use a
  bounded BTree range based on the hashed prefix, avoiding full partition scans
  and per-entry `starts_with` checks. Behavior unchanged for non-indexable prefixes
  (empty scan).
- Relaxed `IN` validation to allow general field membership (scalar or list),
  matching existing execution semantics (e.g., `category IN ["A","C"]`).
- Ordering over Text is now allowed (validated as "comparable"), aligning with
  tests that perform lexicographic filters on names.

## [0.15.1] - 2025-08-31
- view enums now derive Copy, Eq, PartialEq, Ord, PartialOrd

## [0.15.0] - Mimic Stats!
- now you can use the mimic_stats endpoint on any canister to find out about all the stores, memory
size, number of entries, first and last key

## [0.14.13] - 2025-08-30
- fixed nasty bug with indexes

## [0.14.11] - 2025-08-26
- now entities have an associated constant for each field

## [0.14.9] - 2025-08-25
- fixed bug in ulid deserializing

## [0.14.8] - 2025-08-24
- added the FieldValue trait to List and Set so they can be used in queries

## [0.14.5] - 2025-08-22
- you can use an enum, ie. Rarity::Common in a filter query

## [0.14.4] - 2025-08-22
- proper Nat128 and Int128 support, had to wrap the types as minicbor doesn't support them
- latest ICU changes with the new CanisterPool code

## [0.14.2] - 2025-08-21
- fixed version.sh not to do double bump

## [0.14.1] - 2025-08-21
- added the _ci analogues of the comparison functions and lots more tests
- removed the Box<> from the Value::List to declutter

## [0.14.0] - Metadata Change
- Metadata has been added into every entity, and it's not stored separately any longer.  This means that
you can do queries on created_at / updated_at

## [0.13.12] - 2025-08-20
- made many queries easier to write

## [0.13.6] - 2025-08-19
- redid fixtures so the trait is easier to use

## [0.13.4] - Hallowe'en Edition
- all cleaned up, optimised, removed structs from DataRow and EntityRow

## [0.12.20] - 2025-08-17
- removed the Rc<> overhead on DATA_REGISTRY and INDEX_REGISTRY
- refactored the compiler changes to Store and Entity, passing Db now, so a lot less boilerplate code
- Option and Vec::default() for auto generated default statements

## [0.12.16] - 2025-08-16
- now a query on an entity on the wrong canister will be caught by the compiler!

## [0.12.6] - 2025-08-16
- made create/replace generic over Into<E> so we don't need create_from_view any more
- moved Plan from Context into the Executors so we can do .explain() and .plan()
- QueryPlan now only deals with Keys, the DataKey is now the domain of the Executor
- Query is now passed into the Executor by reference
- changed the query's sort functions so it follows filter (|s| s.asc ...)

## [0.12.4] - 2025-08-15
- added Eq trait to all nodes to hide that clippy message, actually much cleaner now

## [0.12.2] - 2025-08-15
- removed semver crate as we're going to do versions differently and its just bloat
- added pub const VERSION

## [0.12.0] - Query Executor Generic Change
- ::<Entity> has been moved to the root of the fluent builder, which means we can make it a lot more
ergonomic.  The compiler can infer <T, U> without <E, _, _>

## [0.11.0] - New Query Language
- replaced entire query builder with a much more easy to use / ergonomic way of constructing queries.
ie. .filter(|f| (f.eq("field", 3) | f.eq("field", 4"))

## [0.10.12] - 2025-08-11
- fixed a bug with inserting into an optional index
- removed Account as a primitive type

## [0.10.4] - 2025-08-11
- made ctor anonymous and now mimic users don't have to include it (done via macro attributes)
- moved Timestamp to a primitive type / Value
- redid E8s and E18s with better API

## [0.10.0] - Indexes for All Types
- changed Index equality to use hashed Values, implemented a canonical to_bytes for all Value types
- now any type can be used as an Index
- found lots of newtype bugs with things like Nat, Int, Principal and fixed them
- ordering bug with Decimal, so made sure all types can Order

## [0.9.22] - 2025-08-09
- added a way to detect invalid indexes at compile time
- added FieldKey to subaccount so it can be used as a database key/index

## [0.9.20] - 2025-08-08
- change extract_from_index so it skips any index that produces no keys (to fix a nasty bug)

## [0.9.18] - 2025-08-08
- actually using the filter on the DeleteExecutor
- added lots of tests for deleting, also fixed the fact Filterable was being saved to the wrong Store
- updated rust to 1.89
- moved shared Load and Delete code into Context
- added db!().create::<E>(e), replace and update

## [0.9.14] - 2025-08-07
- better error message on 0 rows found
- removed len and deref from LoadCollection, now its count -> u32

## [0.9.10] - 2025-08-07
- removed second generic arg on load and delete many()

## [0.9.8] - 2025-08-06
- fixed Selector so that it tries to use a familiar ident

## [0.9.6] - 2025-08-06
- added From<&String> to Value

## [0.9.5]
- added and_eq and or_eq to the filter builder

## [0.9.4] - Versioning & Security System
- **CRITICAL SECURITY**: Implemented tag immutability - once a version is tagged, the code at that version can NEVER change
- **NEW**: Comprehensive versioning system with automated tools (`make patch`, `make minor`, `make major`)
- **NEW**: Security validation scripts (`make security-check`, `make check-versioning`)
- **NEW**: Automated CI/CD pipeline with GitHub Actions
- **NEW**: Complete documentation suite (VERSIONING.md, INTEGRATION.md, RELEASE_GUIDE.md)
- **NEW**: Git dependency integration with immutable version pinning
- **IMPROVED**: Fixed all clippy warnings with `-D warnings`
- **IMPROVED**: Code quality optimizations (collapsible if statements, unnecessary map_err removal)
- **IMPROVED**: Enhanced Makefile with comprehensive command shortcuts
- **SECURITY**: Enterprise-grade tag immutability enforcement
- **SECURITY**: Supply chain attack prevention through immutable versions
- **SECURITY**: Automated security checks and validation

## [0.9.3] - Making Saving Great Again
- save queries now return the new entity, making it easier to return results without additional queries
- attached back the automated query endpoints
- made it a bit more user friendly.  Selector Variants can have a name, not an ident, and it
generates the ident for you.

## [0.9.2]
- query rewrite, LimitExpr is separate, and all parts of the query validate
- added tests to make sure that any query with an invalid field name on filter or sort fails
- combined default and impl into TraitStrategy, because sometimes we need both
- rewrote LoadExecutor to get a proper fast-track path for count() with a filter
- LoadQuery::all()
- count() alongside execute() uses the same logic but won't deserialize unless it has to

## [0.9.1]
- added a u8 version to the serialized data in the DataStore
- now converting metadata directly into bytes to avoid candid encode/decode - 10% less wasm instructions!
- standardised filter() on both Query and Executor types
- added validators for UpperCamel and UPPER_SNAKE
- moved text::Function to ident::{Function, Variant, Field}

## [0.9.0] - Codegen Rewrite
- rewrite of the macro codegen, much faster in vscode now
- added have view(), views() and try_view() to LoadCollection
- removed EntitySearchable and FieldSearchable as Value takes care of that
- removed EntityAccessor, FieldSortable as Value also takes care of that

## [0.8.3]
- cleaned up filter api with less generics
- added filter_eq to LoadExecutor and DeleteExecutor
- changed closures in FieldAccessor to static functions, smaller wasm and faster compiles

## [0.8.2] - Maintenance & Optimisation
- reduced delete executor to just one mutable borrow
- added delete_lots test just in case
- IndexTuple::HAS_INDEXES trait constant so you can easily skip indexes if needed
- db::Db is now passed to fixtures and not the SaveExecutor
- QueryShape::All removed because it's dangerous and doesn't add anything
- Delete Range working
- removed the Inner trait as to_view makes it obsolete
- merged the ValidatorBytes, ValidatorString into just Validator
- made index key a u64 and moved the fields into the IndexEntry

## [0.8.1] - Internal Changes
- added CanisterKind, StoreKind, IndexKind so that we can declare the static types
- Entity::Indexes are now type tuples (IndexA, IndexB)
- #[index] is used to declare an index
- completely rewrote the AsMacro, AsType, AsSchema code
- EntitySearchable and EntitySortable reduced dramatically in size using a shared helper

## [0.8.0] - Feature Complete?
- the query planner is now resolving indexes and hitting the IndexStore to get the keys
it needs, this means mimic is pretty much feature complete (for now).  Now comes a lot of testing!
- added count and count_all which take advantage of the new lazy evaluation on stable structures
BTreeMap

## [0.7.3]
- shortcut methods for create, update and replace
- In, AllIn and AnyIn now should work in queries
- mul and div for decimal
- e8s and e18s a lot more stable with more tests, from Decimal etc.

## [0.7.2] - Range and Filter
- we're now using a QueryPlanner that analyses the indexes
- changed View::default() so it's much leaner and works better
- every type now has an enforced default
- redid savequery and reduced a lot of code and bloat, save now returns the key

## [0.7.1] - Split Type and Relation
- now you can specify rel= and is/prim= at the item level, so the actual
type (Ulid/Principal, not Key) is used in the struct
- view now derives Default, good for endpoint code, but really hard to implement
as Principal doesn't derive Default
- added checking for redundant indexes

## [0.7.0] - Removed Entity Nesting
- Just wasn't working out, adding more complexity.  Much simplified now
and 10% faster
- Ulid storable now 16 bytes not 28
- Added tests for all storable types to make sure we never get panics from ic stable structures

## [0.6.0] - View Layer
- All types now implement TypeView where Type::View is a simplified type that
can be used in DTOs.
- removed lots of boilerplate code for Option and Vec<T> by using traits instead
(Visitable, EntitySearch etc.)
- Complete rewrite of mimic_declare (formerly mimic_design) with much more sensible traits, and
ToTokens derived for a specific purpose
- FieldList has come back to consolidate logic over multiple Fields
- Validator now has a FieldList so the types used to validate are in the schema
- optimised the mimic_declare macros as we don't need to pass through tokens any longer

## [0.5.5]
- IndexValue types (DataKey, IndexKey) are now Copy, so no Text keys
- Every EntityKind has a PrimaryKey type [IndexValue; N].  Single PrimaryKeys can be flattened to use as an IndexValue
- FixedE8 changed to E8s, and E18s as a bonus.  PrimitiveType -> Primitive as we don't have the macro node any more.
- Adding a suite of DTO helper functions in mimic::dto, to convert design types <-> DTO types

## [0.5.4]
- moved proc_macro2, darling, syn and quote out of mimic and just into the mimic_design crate and it saved
around 150k
- merged mimic_base and mimic, and added a seperate mimic_common trait.  Now the Schema types that are shared between
mimic and mimic_design are properly wrapped and kept DRY
- redid the crate layout so that people only have to include mimic
- removed hundreds of derives on types that didn't need them
- added a FixedE8 type to use instead of u64/Decimal for token values
- added MaxDecimalPlaces validator
- cleaned up the mimic_tests, design, canister a lot

## [0.5.3]
- full support for WHERE logic trees, with simplification and DeMorgan's laws (thanks GCSE Computing!)
- added type coercion so you can search for a u64 == 4u8
- removed .search() from the query as you can do that all within where now
- removed serde_json from the main mimic package, and did a cleanup
- renamed where to filter, and we have full nested logic tree searching now

## [0.5.2]
- changed the .to_string() usage of indexes and where statements to use a variant type
- sort key renamed to data key
- removed the dynamic loader db!().load_dyn, as it's not really needed
- removed Nat128 and Int128 which shouldn't have been there, they map to Nat and Int anyway

## [0.5.1]
- redid the query_load!() macro to be a single macro mimic_query!() that returns an Executor

## [0.5.0]
- Schema is now all static!  It doesn't need to be stored in memory at runtime
- SortKey and IndexKey changed to have a u64 id for 40x faster lookups
- data directory renamed db
- all the bits in the top mimic/ that were defined, generated entity specific are in def/
- added Index creation and deleting into the IndexStore
- replaced Sha256 with xxhash3
- benchmarks properly working
- 10x speed up in Storable for DataValue as we skip serializing the vec<u8>

## [0.4.6]
- Searchable trait now a blanket implementation for Display
- removed PrimitiveGroup in favour of is_numeric, is_displayable
- flattened the types::prim directory, so its types::ErrorTree, types::Ulid etc.
- Principal now handles From and PartialEq with its wrapped type much betterer
- added Subaccount as a primitive type, so Storable sticks to 32 bytes
- using the wrapped Subaccount and ic_principal from icu
- added Account to primitives

## [0.4.5]
- grouped data, store, response and executor under data/
- removed generics from queries, and put them onto the executor instead
- made IndexStore and DataStore newtypes
- added IndexValue that is a HashMap<String> to store index values
- Relation and CompositeKey have been merged to just Key

## [0.4.4]
- massive refactor!
- Primitive types now gone, they're just an item(prim = "PrimitiveType"), and all moved to the mimic crate
- removed id() and composite_key() and instead the logic will be inside the Resolver
- FormatSortKey now derived for all types, so we can get the indexed values alongside the keys
- Node traits (code generation) are now separate from Kind traits (schema types)
- removed lots of legacy code, like traits we weren't using

## [0.4.3]
- LoadQueryDyn is now generic, dropped the path variable, and include_children is available
- moved base out of mimic and added mimic_base.  mimic_common can now go back into mimic
- Sorted moved to a trait, which altered the logic so that traits can have attributes but no impl or derive
- moved build back into mimic
- changed the Errors so only the top level MimicError derives CandidType/Serialize/Deserialize
- removed Errors from Query as when we moved the executors out it made less sense

## [0.4.2]
- made Db into StoreRegistry and made it generic.  Now you can define multiple registries (one for DataStores, one
for IndexStores)

## [0.4.1]
- filter renamed search.  Filtering queries is now done using a closure like the rest of the rust ecosystem
- added search for Cardinality::Many, performance improvements
- removed FieldList as it wasn't adding anything
- rewrote EntitySort and EntitySearch to be way more performant
- removed the two different SaveQueries to simplify it in preparation for indexes
- moved all shared types between mimic and mimic_design into mimic_common, and re-exported it from mimic

## [0.4.0]
- breaking changes!  mimic_end is gone, moved a lot of things to ICU
- no more config.toml
- merged the preludes so there's just one and its top level
- renamed macro endpoints to have mimic_ prefix
- as_ref only derived for strings and ulids now
- refactored LoadMap to have a Relation as a key, and did From<SortKey> for Relation

## [0.3.8]
- Relation isn't Vec<Ulid> any more, instead it's Vec<String> to handle principals and other types
- IntoIterator auto derived for List, Set, Map
- Removed config file as it's just extra setup pain and isn't needed

## [0.3.7]
- standardising with the ICRC standardisations.  Nat, Int, Nat8 etc.
- lots more work on the RGBA types, thanks ChatGPT
- Relations are now a whole sort key not a ulid
- enforced _id, _ids, _key, s
- renamed the primitive type String to Text to match the Candid interface
- fixed a bug where RelationSet wasn't being used (a HashSet of Relations)
- fixed a bug where Opt/Many types with embedded validators wouldn't respect the cardinality
- is = Entity would work when it shouldnt
- added MimeType

## [0.3.6]
- completely redid the type validators, and now we have field-level validators too, so you can make a
field a U8 and validate it between 10 and 100
- went through the mimic_design/imp directory and forced everything into traits to be tidy
- moved type validators into the Item struct, so they can be used on basically anything
- isize and usize make no sense so have been removed

## [0.3.5]
- Map and List are back, and the Map logic has been taken out of Newtype, it's much cleaner this way.
- Newtype's Primitive is now not optional, as it's always Cardinality::One
- Fixed a bug with Filterable not working on Ulids
- Default has moved from Value, it's now on Newtype and Field
- moved a lot of From/Into logic to derive_more
- removed the map_derive() from the design logic, need a better way to do that
- Newtypes now have a brand new From<X> that goes down all the way to the bottom primitive
- BIG rewrite of trait derives/impls in the mimic_design package.  Now they're only calculated once and the functions
return Option<TokenStream> so it's easier to fall through to a simple derive
- Maps are now proper HashMap<K, V>
- Sets are back as a HashSet<T>, they'll only work for obvious Eq-supporting types

## [0.3.4]
- the Map type has been removed.  It was only a pseudo-map anyway as it was Vec<(K, V)> in the database.
Map logic can be handled in the DTO/game logic layers instead.
- map(key = "field") has been added to newtypes, validation is automatically carried out in validate_auto
- UlidSet replaces Vec<Ulid> so its easier to copy and also removes duplicate relations

## [0.3.3]
- removed the iterator from the load query, it's not really needed and makes the code super complex
- moved the build code into a new mimic_build crate
- made load, save, delete much cleaner and following exactly the same builder pattern
- standardised LoadResponse, DeleteResponse and removed the extra Result types
- added LoadMap and id to Entity trait to prepare for DTO hash maps

## [0.3.2]
- query::load<E> now replaced with Query::<E>::load() so I can have a map of string -> Type

## [0.3.1]
- added the 'todo' option for fields and types under the subcategory ty(), also TypeNode
- rust 1.84.1
- unit type is now there if you don't specify is or rel
- rewrote Item so it has a path and an optional entity
- switched ELoadBuilder and LoadBuilder to LoadBuilder and LoadBuilderDyn, better fluent api
- composite_key now uses unwrap_or_default()
- snafu has been replaced with thiserror
- removed strum as derive_more 2.0 does it all, also now properly exporting derive_more
- added mimic_get_store and StoreError

## [0.3.0]
- removing automated IC actor scripts [HUGE CHANGE]
- sanitise removed completely from the framework as it's bloated
and makes the code really complex, plus adds a lot of compile time and cycles
- redoing errors so they have specific names, ie. AuthError not auth::Error, and using context to bubble errors up
- Save, Load and Delete now come in dynamic and static/generic forms
- fixtures don't need to be a Vec<Box<dyn>> any more
- RowIterator renamed to LoadResult and cleaned up
- moved query out of the db/ directory

## [0.2.4]
- inter canister DB query calls working
- removed QueryRow because it's effectively the same thing as DataRow
- changed enum_value to use ArgNumber not i32

## [0.2.3]
- breaking change to database API.  Queries are now constructed separately, the fluid method
does not start with DB
- enum_value has returned, we do have a use for it in Dragginz

## [0.2.2]
- primary keys are no more, sort keys now include the key of the local entity
- we didn't need the OnCreate hook, so removed that complex code
- sort key now has more strict validation, demanding relations are used for keys
- cleaned up the mimic build code so that errors are properly displayed in text and not debug
- integrated new version of convert_case, added Sentence case
- fixed a bug and vastly improved the error messages in validator::string::len
- indexes can now be unique

## [0.2.1]
- split load and load_dyn on query in preparation for inter-canister queries

## [0.2.0]
- all changes that make mimic compatible with the Dragginz repo v0.2
- split EntityDynamic into EntityDyn and NodeDyn
- mimic is now just two crates, as everything else has been incorporated into the main crate
- guard_update and guard_query back
- constants added to schema

## [0.1.9]
- we need to store the Path of an entity inside the db, so we can tell what data it's
supposed to be
- changed Fixture to EntityFixture, and removed source from Entity.  Added new schema type EntityExtra which allows you to specify a list of data sources
- bumped rust to 1.82 and did a clippy pass
- added an 'indirect' directive to item so you can wrap things in Box<T>

## [0.1.8]
- wrapped Db in an Rc so that it's easier to pass through to await code
- added better fixture validation, it tells you the entity path now
- introduced a schema Builder that allows you to register hooks pre-build/validate

## [0.1.7]
- refactor of entire api crate with new error handling, and grouped submodules
- moved ic to lib_ic to keep the libraries consistent
- moved the five startup functions into a single StartupManager trait with defaults
- removed mimic_common and mimic_derive as they were only used for Storable, and we need a more
flexible implementation of Storable anyway
- removed lib_cbor and added from_binary and to_binary into the ic::structures module, accessible
to whatever apps use mimic
- removed lib_time and moved it to the public ic:: crate
- removed lib_rand and moved it to ic too
- added the semver crate so we can validate Version
- Validate renamed to ValidateManual, Sanitize to SanitizeManual to clean up how those traits work
- looped api::error back into it's child errors to handle ic::api::call

## [0.1.6]
- changed the concrete error types to a (Code, String) tuple, as dealing with two tiers of
candid error variants (mimic and app) was a nightmare

## [0.1.5]
- guides were a code smell, so they've been removed.  Instead we have EnumValue
which allows you to specify a unit enum with an additional value argument that
is stored in the schema JSON
- EnumValue now returns an Option type to handle Unspecified
- Removed the Constant schema node because there are better ways to handle it
- PrimaryKey implemented for String

## [0.1.4]
- fixed a bug in api::call that was affecting calls with more than one argument

## [0.1.3]
- Horrible circular dependency in mimic_base with the Ulid type fixed
- root canister now has a schema() command that's guarded at the controller level
- tests have been moved out of mimic_base so they don't interfere with the application tests
- removed application dependency on derive-more, num-traits and strum
- the user canister path is no longer hard coded and its pulled from the schema

## [0.1.2]

- Build scripts!  No longer do we have to rely on a suite of bash scripts piping data to random
files that may not get included properly when directories change.
- mimic_start!() takes in the config file as an argument, couldn't think of a better way to do it

## [0.1.1]

- Filter now has different arguments.  It's an enum where you can specify All(String), or Fields(Vec<(String, String)>) to either search every field if it contains a string (OR), or multiple fields with multiple different strings (AND)
- Unit enums now derive Display + Hash so they can be used as map keys
- Enums can now use the variant(unspecified) directive

## [0.1.0]

- Ok finally compiling and the Dragginz application is using mimic and working.  Back to
how things were before the split.
- removed Primitive::path from schema as it's just used in generated rust code
- Constant now uses PrimitiveType and ArgNumber (for consistency, and to display the types
and values in the schema).  It also strips the numeric annotation so you don't have to match the value to the type.
- Isize and Usize are now both in PrimitiveType and ArgNumber for consistency

## [0.0.3]

- vanity release because now apps compile if they have mimic as a dependency!  go me!

## [0.0.2]

- actorgen and schemagen combined into a single `mimicli` executable
- added mimic::prelude and mimic::prelude for external actor and design crates
- added a skip-validation flag to `mimicli`

## [0.0.1]

- Split off the Dragginz ORM into a separate repository (this one) and renamed it Mimic.  Everything in
this 0.0.1 release will just be getting the thing working.

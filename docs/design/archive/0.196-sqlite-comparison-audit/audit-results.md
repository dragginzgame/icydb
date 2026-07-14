# IcyDB SQLite-Comparison Audit

## Executive summary

This audit inspected the current IcyDB repository and compared its implementation discipline against SQLite reference principles. The comparison is about storage semantics, query planning, reliability, observability, and test depth, not feature parity. SQLite should not be copied wholesale: IcyDB targets deterministic Rust code running in Internet Computer canisters, with macro-generated typed APIs and stable-memory constraints.

### 10 most important findings

1. **Cursor pagination is still post-access for reads**, so some small-page queries can load, filter, or order far more rows than the page needs. Evidence: [docs/contracts/QUERY_CONTRACT.md](/home/adam/projects/icydb/docs/contracts/QUERY_CONTRACT.md:181) and [docs/contracts/CURSOR.md](/home/adam/projects/icydb/docs/contracts/CURSOR.md:20); performance artifacts show `LIMIT 1/3/10` ordered range cases reading 512 rows and costing about 22-23M instructions.
2. **Recursive value-storage decode still allocates runtime `Vec<Value>` structures**, which blocks SQLite-style serial-record discipline for projection and validation. Evidence: [crates/icydb-core/src/db/data/structural_field/value_storage/decode/value.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/data/structural_field/value_storage/decode/value.rs:484).
3. **There is no fair SQLite comparison benchmark harness yet.** Existing performance tooling is strong for IcyDB/PocketIC attribution but not comparative; `sqlite3` is not installed in this environment and no project `cargo bench` target exists.
4. **The planner has deterministic rule ranking but not yet enough statistics/explain surfacing from exact index cardinality metadata.** IcyDB has exact prefix-cardinality infrastructure; the next win is using it for admission, explain output, and operator diagnostics rather than copying SQLite's full cost-based planner.
5. **Bounded small-result ordered queries still materialize too much when the available access path cannot satisfy order.** This is the biggest confirmed performance risk in the current artifacts.
6. **Fast-path ownership still has documented unguarded areas.** Evidence: [docs/governance/fast-path-inventory.md](/home/adam/projects/icydb/docs/governance/fast-path-inventory.md:164).
7. **Schema mutation is catalog-native and fail-closed, but the durable migration runner/state machine is not yet productized for broader DDL.** This is the biggest medium-term correctness risk before widening migrations.
8. **Durability and recovery are stronger than earlier audits, but operator health does not yet expose marker/journal/fold-watermark state.** Storage and integrity reports exist, but not a SQLite-like operational recovery view.
9. **Persisted format policy is explicit, but a byte-level stable-memory/storage format spec is still missing.** Current inventory is a checklist, not a full file-format-style specification.
10. **Testing is broad, but the missing high-value categories are SQLite differential tests, persisted-format fuzzing, corruption/OOM/failure-injection tests, and benchmark regression gates with allocation/instruction counters.**

### Top 5 immediate wins

1. Add a cursor-boundary pushdown slice for index-compatible ordered pages.
2. Add streaming/borrowed visitors for recursive value-storage projection and validation.
3. Surface exact index cardinality and query-plan details in explain/diagnostics.
4. Add tripwire tests for the remaining unguarded fast-path precedence areas.
5. Add a committed benchmark harness skeleton that can run IcyDB and SQLite STRICT/WAL scenarios when `sqlite3` is available.

### Top 5 strategic wins

1. Introduce an explicit IC-native query-plan representation with deterministic exact statistics, not a general SQLite-style cost-based optimizer.
2. Move row storage toward offset-addressable field access to avoid full-row decode for projections, predicates, and covering paths.
3. Productize durable schema migration state before broader schema mutation support.
4. Add a byte-level stable-memory format spec, including marker, journal, row, index, and schema surfaces.
5. Build differential and fault-injection test harnesses that compare overlapping typed semantics with SQLite STRICT tables.

### Largest risks

- **Biggest correctness risk:** schema mutation/recovery edge cases once migration support expands beyond the currently fail-closed supported path.
- **Biggest performance risk:** bounded ordered reads that materialize/read many candidates before applying page boundaries.
- **Biggest cleanup/debt risk:** duplicated fast-path logic without complete ownership/tripwire tests.
- **Biggest missing test category:** differential plus fault-injection testing across query/index/recovery paths.
- **Current bottleneck assessment:** the repository evidence points more to algorithmic/materialization and row decode/storage-layout costs than to macro/API overhead. Stable-memory I/O may be important on canister paths, but this audit did not measure wasm/stable-memory counters directly.

## Scope

- **Repository:** `/home/adam/projects/icydb`
- **Repo commit:** `54ea26674652399a57b5282a0b7117886cffb7e1`
- **Date:** 2026-07-03
- **Rust:** `rustc 1.96.0 (ac68faa20 2026-05-25)`
- **Cargo:** `cargo 1.96.0 (30a34c682 2026-05-25)`
- **OS/CPU:** WSL2 Linux `6.6.87.2-microsoft-standard-WSL2`, AMD Ryzen Threadripper 7970X 32-Core Processor, 64 logical CPUs.
- **SQLite CLI:** unavailable in this environment: `sqlite3: command not found`.
- **IcyDB build profile used:** workspace test/dev validation with `--all-features`; existing performance artifacts are PocketIC/instruction reports generated by prior project harnesses.
- **Feature flags inspected:** workspace `--all-features`; key IcyDB features include `diagnostics`, `sql`, and `sql-explain`.
- **Dirty worktree at start:** `crates/icydb-core/src/db/session/tests/branch_set.rs` had pre-existing local modifications. It was not edited by this audit.

### What was inspected

- Repository layout, workspace packages, feature flags, CI-adjacent scripts, release/version files, changelogs, docs, prior audits, TODOs, and performance reports.
- Public API surface in `icydb`, `icydb-core`, `icydb-schema`, `icydb-schema-derive`, and `icydb-build`.
- Macro and generated-code paths for schema/entity derivation and generated stable-memory store wiring.
- Storage abstractions for data, indexes, journal tails, commit markers, schema snapshots, recovery, and diagnostics.
- Query/filter/index paths: plan ranking, index selection, covering plans, access contracts, cursor semantics, read admission, SQL performance harness, and existing performance artifacts.
- Serialization/deserialization paths, especially structural-field value storage.
- Observability and diagnostics: metrics state, storage report, integrity report, recovery path, and attribution artifacts.
- Test and benchmark structure: unit tests, integration tests, trybuild tests, ignored perf harnesses, and lack of committed `benches/`.

### What was not inspected or not run

- No production code was changed.
- `cargo bench` was not run because no project Cargo benchmark targets were found after excluding `.cache`, `target`, and `.git`.
- SQLite comparative benchmarks were not run because the `sqlite3` CLI is unavailable.
- Wasm binary size and stable-memory read/write counters were not freshly measured in this audit.
- No local ICP network was started or stopped.

### SQLite references used

- [Query planner](https://sqlite.org/queryplanner.html)
- [Optimizer overview](https://sqlite.org/optoverview.html)
- [WAL](https://sqlite.org/wal.html)
- [Atomic commit](https://sqlite.org/atomiccommit.html)
- [File format](https://sqlite.org/fileformat2.html)
- [Temporary files](https://sqlite.org/tempfiles.html)
- [PRAGMAs](https://sqlite.org/pragma.html)
- [Compile-time options](https://sqlite.org/compile.html)
- [Testing discipline](https://sqlite.org/testing.html)
- [Many small queries](https://sqlite.org/np1queryprob.html)
- [WITHOUT ROWID](https://sqlite.org/withoutrowid.html)
- [STRICT tables](https://sqlite.org/stricttables.html)
- [Release history](https://sqlite.org/changes.html)

## Prior context considered

### Existing audit reports found

- [docs/design/0.192-mega-audit-3/audit-results.md](/home/adam/projects/icydb/docs/design/0.192-mega-audit-3/audit-results.md:1)
- [docs/design/archive/0.189-mega-audit-2/audit-results.md](/home/adam/projects/icydb/docs/design/archive/0.189-mega-audit-2/audit-results.md:1)
- [docs/design/archive/0.184-query-engine-audit/findings.md](/home/adam/projects/icydb/docs/design/archive/0.184-query-engine-audit/findings.md:1)
- [docs/design/archive/0.184-query-engine-audit/status.md](/home/adam/projects/icydb/docs/design/archive/0.184-query-engine-audit/status.md:1)
- [sql-perf-audit report](/home/adam/projects/icydb/docs/reports/recurring/2026/04/30/sql-perf-audit/01/report.md:1)
- [2026-06-05 summary report](/home/adam/projects/icydb/docs/reports/recurring/2026/06/05/summary/01/report.md:1)

### Previously-fixed issues not reopened

- Query admission is no longer an open gap from the older audits. [READ_ADMISSION.md](/home/adam/projects/icydb/docs/contracts/READ_ADMISSION.md:123) documents bounded defaults, full-scan rejection, materialized-sort rejection, and generated SQL-admin separation.
- The 0.184 query-engine audit findings were marked closed after `0.184.50`; this audit does not reopen those exact items.
- Atomicity and recovery contracts now exist and are materially stronger than in early audit context. See [ATOMICITY.md](/home/adam/projects/icydb/docs/contracts/ATOMICITY.md:219) and [DURABILITY.md](/home/adam/projects/icydb/docs/contracts/DURABILITY.md:112).
- Accepted schema snapshots are explicitly the runtime authority, not generated model reconstruction. See [snapshot.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/schema/snapshot.rs:1).

### Remaining open themes from older audits

- Recovery size/proof limits are documented but not exhaustively stress-tested at production scale.
- Checksums, raw backup/import, and byte-level persisted-format documentation remain open design areas.
- Broader schema migration execution still needs durable state, phase tracking, and crash/resume tests before expanding beyond the current supported path.
- Performance methodology exists for IcyDB/PocketIC, but not for fair SQLite comparison or allocation/peak-memory accounting.

## Repository reconnaissance

### Layout

- `crates/icydb`: public facade crate and user-facing exports.
- `crates/icydb-core`: core database engine, query planning/execution, storage, schema, diagnostics, metrics, SQL support, commit/recovery, session APIs.
- `crates/icydb-schema` and `crates/icydb-schema-derive`: schema model and derive macros.
- `crates/icydb-derive`: user-facing derive support.
- `crates/icydb-build`: generated actor/store wiring and build-time codegen support.
- `crates/icydb-cli`: command-line tooling.
- `crates/icydb-config`, `icydb-primitives`, `icydb-utils`, `icydb-diagnostic-code`: support crates.
- `testing/`: integration tests, macro tests, wasm helpers, and performance matrix harnesses.
- `canisters/`: audit, demo, and test canisters.
- `docs/contracts`: durable contracts for query, read admission, atomicity, transactions, persistence, resource model, SQL subset, cursor behavior.
- `docs/design` and `docs/audits`: prior audits, design notes, reports, and performance artifacts.

### Workspace crates/packages

`cargo metadata --no-deps` found these main workspace packages:

- `icydb`
- `icydb-core`
- `icydb-diagnostic-code`
- `icydb-primitives`
- `icydb-utils`
- `icydb-derive`
- `icydb-schema`
- `icydb-schema-derive`
- `icydb-config`
- `icydb-build`
- `icydb-cli`
- Canister/test packages under `canisters/`, `schema/fixtures/`, and `testing/`.

### Core modules and responsibilities

Core engine modules under [crates/icydb-core/src/db](/home/adam/projects/icydb/crates/icydb-core/src/db/mod.rs:1):

- `data`: primary row storage.
- `index`: secondary index storage, key encoding, cardinality, uniqueness, and prefix metadata.
- `query`: typed query model, plan construction, predicate analysis, covering plans, SQL shared planning.
- `executor`: execution helpers, prefix cardinality/liveness, aggregation/filter/order support.
- `session`: session facade, query cache, SQL cache, branch set tests, read/write APIs.
- `commit`: marker, recovery, and commit orchestration.
- `journal`: append/fold/cleanup of durable mutation journal tails.
- `schema`: accepted snapshots, schema mutation/reconciliation, mutation runner scaffolding.
- `diagnostics`: storage and integrity reports.
- `metrics`: operation/event counters.
- `sql`: SQL front-end where enabled.

### Public API surface

[crates/icydb/src/lib.rs](/home/adam/projects/icydb/crates/icydb/src/lib.rs:1) provides the primary user-facing facade and re-exports core query APIs, diagnostics, schema builders, generated macros, transaction/session types, SQL feature APIs, and read-admission controls. [crates/icydb-core/src/lib.rs](/home/adam/projects/icydb/crates/icydb-core/src/lib.rs:1) and [crates/icydb-core/src/db/mod.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/mod.rs:1) expose the lower-level engine surface.

### Macro and generated-code paths

- [crates/icydb-schema-derive/src/lib.rs](/home/adam/projects/icydb/crates/icydb-schema-derive/src/lib.rs:1) defines schema/entity derive entry points.
- [crates/icydb-schema-derive/src/imp/entity.rs](/home/adam/projects/icydb/crates/icydb-schema-derive/src/imp/entity.rs:1) generates schema model, entity constants, and internal consistency tokens.
- [crates/icydb-build/src/lib.rs](/home/adam/projects/icydb/crates/icydb-build/src/lib.rs:1) controls build-time generation.
- [crates/icydb-build/src/db/store.rs](/home/adam/projects/icydb/crates/icydb-build/src/db/store.rs:1) emits generated stable-memory store wiring for heap and journaled stores.

### Storage abstractions

- [crates/icydb-core/src/db/data/store.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/data/store.rs:1) uses heap `BTreeMap` state and journaled canonical `StableBTreeMap` plus live/tombstone overlay.
- [crates/icydb-core/src/db/index/store.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/index/store.rs:1) mirrors the data store shape for secondary indexes, with live projections, tombstones, and prefix-cardinality metadata.
- [crates/icydb-core/src/db/journal/store.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/journal/store.rs:24) uses 64 KiB journal chunks, append IDs, fold watermark, and cleanup.
- [crates/icydb-core/src/db/commit/marker.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/commit/marker.rs:32) defines the commit marker invariant and bounded marker size.
- [crates/icydb-core/src/db/commit/recovery.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/commit/recovery.rs:175) runs recovery phases: marker, journal fold, live rebuild, index rebuild/fold, integrity validation, marker clear, ready.

### Query/filter/index abstractions

- [crates/icydb-core/src/db/query/plan/planner/index_select.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/planner/index_select.rs:1) builds index candidates and strips covered predicates where safe.
- [crates/icydb-core/src/db/query/plan/planner/ranking.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/planner/ranking.rs:117) ranks candidates deterministically by prefix, exactness, residual filter, range bound, and order compatibility.
- [crates/icydb-core/src/db/query/plan/covering/mod.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/covering/mod.rs:1) owns covering-plan eligibility and row-presence checks.
- [crates/icydb-core/src/db/executor/index_prefix_cardinality.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/index_prefix_cardinality.rs:145) expands exact prefix-cardinality/liveness data.
- [docs/contracts/QUERY_CONTRACT.md](/home/adam/projects/icydb/docs/contracts/QUERY_CONTRACT.md:181) documents that cursor continuation is post-access today.

### Serialization/deserialization path

The hot structural value-storage decode path is under [crates/icydb-core/src/db/data/structural_field/value_storage/decode/value.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/data/structural_field/value_storage/decode/value.rs:440). TODOs at lines 484 and 504 state that recursive list/map decode currently allocates runtime `Value` containers and should move toward walker/visitor-based projection and validation.

### Persistence and canister-specific storage path

Persistence is journaled and stable-memory-backed for durable stores. Generated store wiring lives in [crates/icydb-build/src/db/store.rs](/home/adam/projects/icydb/crates/icydb-build/src/db/store.rs:1). Durability and atomicity contracts define pre-commit fallibility, infallible apply, journal/marker recovery, and canister single-message semantics:

- [docs/contracts/DURABILITY.md](/home/adam/projects/icydb/docs/contracts/DURABILITY.md:1)
- [docs/contracts/ATOMICITY.md](/home/adam/projects/icydb/docs/contracts/ATOMICITY.md:1)
- [docs/contracts/TRANSACTION_SEMANTICS.md](/home/adam/projects/icydb/docs/contracts/TRANSACTION_SEMANTICS.md:1)

### Observability / metrics / tracing path

- [crates/icydb-core/src/db/diagnostics/storage_report.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/diagnostics/storage_report.rs:1) reports entity/data/index/schema/storage and corruption counters.
- [crates/icydb-core/src/db/diagnostics/integrity.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/diagnostics/integrity.rs:1) cross-checks data-to-index and index-to-data consistency.
- [crates/icydb-core/src/metrics/state.rs](/home/adam/projects/icydb/crates/icydb-core/src/metrics/state.rs:6) explicitly does not surface query-side instrumentation through normal metrics report paths because IC query calls should not mutate state.

### Error types and recovery behavior

The codebase uses typed internal errors, durable recovery errors, schema mutation errors, and diagnostic reports rather than panicking in normal production paths. `cargo clippy --all-features --all-targets -- -D warnings` passed, and broad `panic!/unwrap/expect/unsafe` searches did not reveal untriaged production red flags during this audit. Existing governance still correctly forbids panics/unwraps in production executor paths.

### Tests and benchmarks

- `cargo test --workspace --all-features` passed.
- Trybuild macro tests are present and pass.
- Proptest usage exists.
- Ignored PocketIC performance report tests exist:
  - `fluent_perf_audit_harness_reports_instruction_samples`
  - `sql_perf_audit_harness_reports_instruction_samples`
  - `sql_perf_generated_matrix_reports_hotspots`
- A native ignored wall-clock microbench exists at [crates/icydb-core/src/db/session/tests/execution_hot_path_bench.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/session/tests/execution_hot_path_bench.rs:25), but it explicitly does not sample allocation counts, instruction counters, or memory.
- No project `benches/` target was found after pruning `.cache`, `target`, and `.git`.

### CI configuration

This audit focused on local workspace validation. CI workflows were not deeply audited beyond repository file discovery and the already-required local commands.

### Feature flags

Key flags found by Cargo metadata and feature grep:

- `icydb`: `diagnostics`, `sql`, `sql-explain`.
- `icydb-core`: matching SQL/diagnostic features.
- Test canisters enable combinations for integration coverage.

### Release/versioning files

- [CHANGELOG.md](/home/adam/projects/icydb/CHANGELOG.md:1)
- [docs/governance/changelog.md](/home/adam/projects/icydb/docs/governance/changelog.md:1)
- [docs/1.0-TODO.md](/home/adam/projects/icydb/docs/1.0-TODO.md:1)
- [docs/ROADMAP.md](/home/adam/projects/icydb/docs/ROADMAP.md:1)

No changelog was edited because this is a docs/audit artifact and not a production behavior change.

### TODO/FIXME highlights

The most relevant production TODOs found are in recursive structural value decode:

- [value.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/data/structural_field/value_storage/decode/value.rs:484)
- [value.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/data/structural_field/value_storage/decode/value.rs:504)

Open roadmap/TODO themes also include cursor hardening, storage pushdown, streaming grouping, cardinality/row-count diagnostics, and CLI check/export/import work. See [docs/1.0-TODO.md](/home/adam/projects/icydb/docs/1.0-TODO.md:1) and [docs/ROADMAP.md](/home/adam/projects/icydb/docs/ROADMAP.md:153).

## SQLite comparison matrix

| Concern | SQLite reference subsystem / principle | IcyDB equivalent | Current IcyDB maturity | Evidence | Gaps | Potential win | Risk if ignored | Priority |
|---|---|---|---|---|---|---|---|---|
| A. Data model and typing | Dynamic typing with STRICT tables for opt-in rigid typing | Typed records, schema derive, schema snapshots, read admission | Strong for typed Rust boundary; good IC fit | `icydb` facade, schema derive, [STRICT SQLite docs](https://sqlite.org/stricttables.html), [snapshot.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/schema/snapshot.rs:1) | Need more decode/migration boundary tests for malformed persisted data and schema mismatch | Differential STRICT-table harness and persisted decode fuzzing | Silent mismatch at restore/migration boundary | P1 |
| B. Schema representation | `sqlite_schema`, schema cookie/versioning | Accepted schema snapshots, schema reconciliation, mutation deltas | Strong conceptual model; migration runner incomplete | [snapshot.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/schema/snapshot.rs:1), [reconcile.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/schema/reconcile.rs:1) | Durable migration phase/cookie semantics not productized broadly | Durable migration state with phase/watermark/report | Upgrade or migration stalls once DDL widens | P1 |
| C. Stable-memory layout | Page file, freelist, overflow pages, format header | StableBTreeMap-backed data/index/schema/journal stores | Practical and IC-native, but no byte-level spec | [PERSISTED_FORMAT_INVENTORY.md](/home/adam/projects/icydb/docs/contracts/PERSISTED_FORMAT_INVENTORY.md:10), [store.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/data/store.rs:1) | No full storage-format document, checksums, fragmentation/space model | Stable format spec and storage-health report | Harder repair/import/debug and upgrade review | P1 |
| D. Record encoding | Serial types and field-skip-friendly record format | Structural value storage and serde/Candid-related paths | Functional; projection decode still maturing | [value.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/data/structural_field/value_storage/decode/value.rs:484), [ROADMAP.md](/home/adam/projects/icydb/docs/ROADMAP.md:153) | Recursive list/map decode allocates; field-offset storage not complete | Streaming visitors, then offset-addressable row layout | Decode dominates projection/filter workloads | P0 |
| E. Primary-key path | Rowid and WITHOUT ROWID clustered keys | Primary row storage keyed in BTree structures | Mature enough; needs focused microbench counters | [data/store.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/data/store.rs:1), [withoutrowid docs](https://sqlite.org/withoutrowid.html) | Allocation/copy counts not measured; key-only decode opportunities unclear | Add PK get/update/delete microbench with allocation counters | Hidden regression in many-small-query path | P2 |
| F. Secondary indexes | B-tree indexes, covering/composite/partial/expression indexes | Index store, composite-ish key contracts, covering plan module | Strong recent work; covering and cardinality available | [index/store.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/index/store.rs:1), [covering/mod.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/covering/mod.rs:1) | Covering not yet enough for all projection/filter shapes; stale-index tests should expand | Covering-index expansion and invariant property tests | Extra row reads and stale-index risk | P1 |
| G. Query planner/execution | Rule and cost planner, WHERE analysis, sort avoidance, covering indexes, fast paths | Deterministic index selection/ranking, read admission, query cache | Good IC-native rule planner; missing stats surfacing and cursor pushdown | [ranking.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/planner/ranking.rs:117), [QUERY_CONTRACT.md](/home/adam/projects/icydb/docs/contracts/QUERY_CONTRACT.md:181) | No cursor pushdown; exact cardinality not fully used in explain/admission | Deterministic exact-stat planner improvements | Small-result queries remain expensive | P0 |
| H. Transactions/atomicity | Rollback journal/WAL atomic commit | IC single-message atomicity, pre-commit/apply split, marker | Strong and explicitly documented | [ATOMICITY.md](/home/adam/projects/icydb/docs/contracts/ATOMICITY.md:100), [recovery.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/commit/recovery.rs:175) | More failure-injection tests needed around marker/journal/live/index phases | Crash/failure injection harness | Rare recovery path bugs survive normal tests | P1 |
| I. WAL/journaling | Rollback journal/WAL, checkpointing | Journal tails, commit marker, fold watermark | Good IC-native analogue | [journal/store.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/journal/store.rs:24), [marker.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/commit/marker.rs:32) | Operator visibility into tail/fold/checkpoint health is limited | Durability health report | Operators cannot diagnose storage pressure/recovery state | P1 |
| J. Caching | Page cache, prepared statement reuse | Query plan cache and SQL command cache | Present and bounded | [session/query/cache.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/session/query/cache.rs:1), [bounded_cache.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/session/bounded_cache.rs:1) | Fixed FIFO size, limited metrics/config | Capacity config, hit/miss explain, consider LRU only with evidence | Churny workloads lose hot plans | P2 |
| K. Temporary structures | Temp B-trees/files for sorts/materialization | Vec/materialized candidates/order windows | Bounded by admission but still costly in admin/trusted paths | [QUERY_CONTRACT.md](/home/adam/projects/icydb/docs/contracts/QUERY_CONTRACT.md:181), perf matrix artifacts | Cursor/window/order boundaries not pushed low enough | Stream ordered ranges and limit early | Instruction spikes on small LIMIT | P0 |
| L. Many-small-query path | In-process queries make N+1 acceptable | Canister-local typed reads and caches | Conceptually aligned; benchmark not comparative | [np1queryprob docs](https://sqlite.org/np1queryprob.html), ignored microbench | No SQLite/IcyDB many-small benchmark; no allocation counters | Add benchmark scenario and result gates | Optimizing bulk path while point path regresses | P2 |
| M. Concurrency/reentrancy | Locking and WAL reader/writer model | IC single-message execution, no async transaction boundary | Good IC-specific model | [ATOMICITY.md](/home/adam/projects/icydb/docs/contracts/ATOMICITY.md:204), [RESOURCE_MODEL.md](/home/adam/projects/icydb/docs/contracts/RESOURCE_MODEL.md:1) | Observer/reentry/failure tests should stay explicit | Add negative tests for observer behavior and query-safe metrics | Accidental state mutation in query lane | P2 |
| N. Durability/recovery | Hot journal/WAL recovery | Commit marker, journal fold, live/index rebuild, integrity report | Strong but needs scale/fault testing | [recovery.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/commit/recovery.rs:175), [integrity.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/diagnostics/integrity.rs:1) | No checksums; recovery proof size limited | Corruption tests and optional checksum design | Corruption is detected late or unclearly | P1 |
| O. Observability | EXPLAIN QUERY PLAN, PRAGMAs, introspection | `sql-explain`, diagnostics, metrics state, perf attribution | Good foundation; operator surfaces incomplete | [metrics/state.rs](/home/adam/projects/icydb/crates/icydb-core/src/metrics/state.rs:6), [storage_report.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/diagnostics/storage_report.rs:1) | Query metrics cannot mutate in IC query calls; durability health not surfaced | Query-safe explain plus update-lane snapshots | Hard to identify slow plan/root cause in production | P1 |
| P. Configuration surface | PRAGMAs and compile options | Cargo features, read admission, build options | Sensible but sparse for performance knobs | [READ_ADMISSION.md](/home/adam/projects/icydb/docs/contracts/READ_ADMISSION.md:123), [compile options docs](https://sqlite.org/compile.html) | Cache size, strict checks, diagnostic verbosity not all configurable | Explicit configuration guide | Hidden trade-offs and inconsistent tuning | P2 |
| Q. Testing discipline | Huge matrix: fuzz, crash, OOM, malformed DB, regression | Unit/integration/trybuild/proptest/perf matrices | Strong for a young DB, still below SQLite safety bar | `cargo test` pass, [sql-fuzzer idea](/home/adam/projects/icydb/docs/design/ideas/sql-fuzzer.md:1) | No committed differential SQLite harness, cargo-fuzz, corruption/OOM matrix | Add focused harnesses by invariant | Bugs escape because normal tests are too well-formed | P0 |
| R. API ergonomics | Small C API plus large SQL surface | Typed Rust APIs, derives, SQL subset | Strong typed story; docs need performance guidance | [crates/icydb/src/lib.rs](/home/adam/projects/icydb/crates/icydb/src/lib.rs:1), [SQL_SUBSET.md](/home/adam/projects/icydb/docs/contracts/SQL_SUBSET.md:1) | Macro error comprehensibility and examples should remain tested | Compile-fail/example coverage expansion | Good types with confusing failure modes | P2 |
| S. Documentation | Detailed design docs and file-format docs | Good contracts; missing byte-level storage/perf guides | Above average, but not SQLite-level storage docs | [PERSISTED_FORMAT_INVENTORY.md](/home/adam/projects/icydb/docs/contracts/PERSISTED_FORMAT_INVENTORY.md:10), [ROADMAP.md](/home/adam/projects/icydb/docs/ROADMAP.md:153) | Storage format, benchmark methodology, canister upgrade safety need consolidation | Storage/perf/operator docs | Slower onboarding and riskier releases | P1 |
| T. Performance portability | Tuned for local embedded use | IC-focused instruction/stable-memory/wasm costs | Correct target philosophy | [RESOURCE_MODEL.md](/home/adam/projects/icydb/docs/contracts/RESOURCE_MODEL.md:1), [ROADMAP.md](/home/adam/projects/icydb/docs/ROADMAP.md:226) | Fresh wasm-size/stable I/O counters not captured here | Add wasm/stable counter gates | Native wins may not translate to canisters | P1 |

## Benchmark results

No new IcyDB-versus-SQLite benchmark result was produced because `sqlite3` is unavailable in this environment. The existing IcyDB performance artifacts are still useful for prioritization.

### Environment record

| Field | Value |
|---|---|
| Repo commit | `54ea26674652399a57b5282a0b7117886cffb7e1` |
| OS | WSL2 Linux `6.6.87.2-microsoft-standard-WSL2` |
| CPU | AMD Ryzen Threadripper 7970X 32-Core Processor, 64 logical CPUs |
| Rust | `rustc 1.96.0 (ac68faa20 2026-05-25)` |
| Cargo | `cargo 1.96.0 (30a34c682 2026-05-25)` |
| SQLite | unavailable, `sqlite3: command not found` |
| Build profile | workspace test/dev validation with `--all-features`; existing perf artifacts are PocketIC instruction reports |
| Feature flags | all workspace features for validation |

### Existing IcyDB performance artifacts inspected

| Artifact | Result | Confidence | Recommended action |
|---|---:|---|---|
| `sql_perf_195_1_liveness_full_matrix.md` | 1,756 scenarios generated, 1,675 executed, 81 expected/known failures; top ordered range `LIMIT 1/3/10` cases cost about 22.7-23.5M instructions with 512 row reads | Medium; artifact is generated and concrete but not freshly rerun here | Prioritize cursor/order/limit pushdown and index design guidance |
| `sql_perf_195_1_liveness_vs_195_0_summary.md` | Common successful scenarios 1,675; total +0.17%; no material regressions; sparse-in focused +0.25% | Medium | Treat recent perf optimization as stable; look for next hotspot rather than rehashing it |
| `sql_perf_195_0_vs_194_15_summary.md` | Total roughly flat; sparse-in page improved about -19.46% | Medium | Do not reopen sparse-in liveness as the next main item |
| `artifacts/perf-audit/sql_perf_deterministic_matrix_current.md` | Older top rows show scan/read/order-window dominating name range + age order + small limit | Medium | Confirms order/window materialization has been a recurring hotspot |
| `execution_hot_path_bench.rs` | Ignored wall-clock microbench only; no allocation/instruction sampling | Low for roadmap decisions | Replace or supplement with Criterion/iai/dhat/PocketIC counters |

### Runnable SQLite/IcyDB benchmark plan

The comparison should be a bottleneck-finding tool, not a winner-takes-all contest. SQLite runs on POSIX storage with decades of tuning; IcyDB targets deterministic canister execution and stable memory. Unsafe SQLite settings must be clearly labeled and never used as the main headline unless IcyDB is configured with equivalent durability semantics.

#### Common metadata to record

- OS, CPU, memory, kernel, virtualization.
- Rust and Cargo versions.
- SQLite version from `sqlite3 --version`.
- IcyDB commit and dirty-worktree state.
- Build profile and feature flags.
- Native versus wasm/PocketIC mode.
- Dataset size, row shape, key distribution, index definitions.
- Warm/cold cache status.
- Durability settings.
- Median, min, p95, max, allocation count, peak memory, and instruction count where available.

#### SQLite modes

- Default mode.
- WAL normal: `PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;`
- WAL full: `PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;`
- Unsafe speed reference, labeled unsafe: `PRAGMA synchronous=OFF;`

#### Logical schemas

Use STRICT SQLite tables for overlapping semantics:

- `users(id PRIMARY KEY, username UNIQUE, age, country, created_at, payload)`
- `posts(id PRIMARY KEY, user_id indexed, title, created_at)`
- `events(id PRIMARY KEY, kind indexed, ts indexed, payload)`

#### Scenarios

| Scenario | Measurement goal |
|---|---|
| Open/init | Empty/open existing DB, schema registration, macro setup |
| Insert | 1, 1k, 10k, 100k rows; individual and batched; no index, one index, multiple indexes; small/large payload |
| Primary-key read | Existing/missing/sequential/random/hot repeated; many small reads |
| Range scan | Full scan, primary range, secondary range, limits 1/10/100, projection |
| Filters | Indexed/non-indexed, AND, OR, inequality, prefix-like, case sensitivity where supported |
| Sort/order | ORDER BY primary, secondary, non-indexed, ORDER BY + LIMIT |
| Aggregates | count, filtered count, min/max, exists, first/last |
| Update | Non-indexed field, indexed field, primary key if supported, failed update, batch update |
| Delete | PK delete, indexed predicate delete, many deletes, delete/reinsert space reuse |
| Index maintenance | Build/rebuild/validate; insert/update/delete costs with index |
| Serialization | Encode, full decode, key-only decode, projected fields, payload sizes |
| Cache/plan cache | First versus repeated query, shape reuse with params, invalidation |
| Macro overhead | Compile time, expanded size, binary size, runtime overhead versus hand-written |
| Stable memory/wasm | Read/write counts, bytes, instruction count, upgrade save/restore |
| Reliability | Failed decode, interrupted batch, index inconsistency, restore, migration, corrupted bytes |

## Detailed findings

### Finding 1: Cursor boundaries are applied after access/materialization

Category:
- Performance / query planner / architecture

Severity:
- High

Priority:
- P0

Confidence:
- High

SQLite reference:
- SQLite's B-tree cursor and planner model seeks to an index range and stops as soon as enough ordered rows are found. The relevant principle is index/range cursor use and ORDER BY/LIMIT avoidance, not copying SQLite's full planner.

IcyDB evidence:
- [docs/contracts/QUERY_CONTRACT.md](/home/adam/projects/icydb/docs/contracts/QUERY_CONTRACT.md:181) says cursor continuation is post-access after access path, candidate materialization, filtering, ordering, and cursor/windowing.
- [docs/contracts/CURSOR.md](/home/adam/projects/icydb/docs/contracts/CURSOR.md:20) says pagination is applied post-access and no cursor pushdown is currently performed.
- [testing/integration/target/perf-hotspots/sql_perf_195_1_liveness_full_matrix.md](/home/adam/projects/icydb/testing/integration/target/perf-hotspots/sql_perf_195_1_liveness_full_matrix.md:13) shows top `user.name_range age_asc limit1/3/10` rows around 22.7-23.5M instructions and 512 data-store gets despite small limits.

Current behaviour:
- A query can choose an access path, read/materialize a candidate set, apply filters/order/cursor, then truncate to the requested page.

Why it matters:
- Canister reads are instruction-limited. A `LIMIT 1` or first page should not pay for hundreds of row reads when the index and order contract can produce a deterministic continuation point.

Likely root cause:
- Cursor tokens represent post-access windows; the lower-level index range/seek layer does not yet consume cursor boundary facts.

Recommended fix:
- Minimal safe change: implement pushdown for the narrow shape where the plan is index-backed, order-compatible, deterministic, and has a scalar cursor boundary for the same key/order. Seek directly to the continuation key and stop after `limit + 1`.
- Optional deeper change: make cursor boundary a first-class plan property with explain output showing whether it was pushed down.

Acceptance criteria:
- Tests for forward and reverse index pages with no duplicates/skips across pages.
- Tests with inserts/deletes between pages if the public contract permits or forbids them.
- Perf artifact showing row reads for matching `LIMIT 1/3/10` pages scale with page size rather than candidate-set size.
- Explain/debug output identifies `cursor_pushdown=true`.

Estimated effort:
- M

Risk:
- Pagination correctness regressions around equal keys, tombstones, reverse scans, or live/journaled overlays.

Follow-up patch prompt:
- "Implement cursor boundary pushdown for deterministic index-backed ordered reads. Limit the first slice to order-compatible single-index plans, add page-stability tests, and extend explain/perf attribution to show pushed versus post-access cursor handling."

### Finding 2: Recursive value-storage decode allocates projected containers

Category:
- Performance / serialization / storage

Severity:
- High

Priority:
- P0

Confidence:
- High

SQLite reference:
- SQLite's record format uses serial types and payload layout so the engine can reason about fields without always constructing full application values.

IcyDB evidence:
- [value.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/data/structural_field/value_storage/decode/value.rs:484) says recursive list decode currently allocates runtime `Vec<Value>`.
- [value.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/data/structural_field/value_storage/decode/value.rs:504) says recursive map decode allocates `Vec<(Value, Value)>`.
- [docs/ROADMAP.md](/home/adam/projects/icydb/docs/ROADMAP.md:153) already identifies row-storage optimization by replacing CBOR row-payload decode with offset-based row encoding/direct field access.

Current behaviour:
- Projection/validation paths that pass through recursive value decode may allocate full recursive `Value` structures even when a walker could validate or project without materializing.

Why it matters:
- This is on the hot path for filters/projections over structured data and directly affects instruction count, heap pressure, and wasm size/perf portability.

Likely root cause:
- The decode API returns owned runtime `Value` trees for recursive structures instead of exposing a streaming visitor/walker that can be consumed by projection and validation code.

Recommended fix:
- Minimal safe change: add a borrowed/streaming recursive visitor for list/map decode and migrate projection-only validation paths to it.
- Optional deeper change: pair this with row-offset storage so top-level field access can skip unrelated fields before invoking recursive decoders.

Acceptance criteria:
- Unit tests show identical decode errors and values for owned decode and streaming visitor paths.
- Projection tests prove unselected recursive fields are not materialized.
- Microbench records allocation reduction for list/map projection and validation.
- Persisted-format compatibility is documented if any encoded shape changes; otherwise assert no format change.

Estimated effort:
- M

Risk:
- Decode error position/context can regress if streaming code loses path metadata.

Follow-up patch prompt:
- "Add streaming visitors for recursive structural value-storage list/map decode, wire projection/validation paths to avoid owned `Value` allocation where possible, and add allocation-sensitive tests or benches."

### Finding 3: There is no runnable fair SQLite comparison harness

Category:
- Performance / testing

Severity:
- High

Priority:
- P0

Confidence:
- High

SQLite reference:
- SQLite's performance claims are tied to reproducible benchmarks and decades of regression testing. The relevant principle is methodology, not matching every SQLite scenario.

IcyDB evidence:
- `sqlite3 --version` failed with `command not found`.
- No project `benches/` files were found after pruning `.cache`, `target`, and `.git`.
- [testing/integration/tests/sql_perf_matrix_audit.rs](/home/adam/projects/icydb/testing/integration/tests/sql_perf_matrix_audit.rs:1) is an IcyDB SQL performance matrix, not a SQLite comparison harness.
- [execution_hot_path_bench.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/session/tests/execution_hot_path_bench.rs:35) says no allocation, instruction counter, or peak memory is sampled.

Current behaviour:
- IcyDB has useful PocketIC/perf attribution artifacts, but no committed fair comparison scaffold for SQLite STRICT/WAL modes and overlapping semantics.

Why it matters:
- Without comparable baselines, optimization choices can chase internal artifacts without knowing whether a bottleneck is algorithmic, serialization, stable-memory-specific, or API overhead.

Likely root cause:
- The existing perf system grew around IC instruction attribution, not cross-engine methodology.

Recommended fix:
- Add a benchmark harness under `testing/integration` or `docs/design/0.196-sqlite-comparison-audit/benchmarks` that can run IcyDB and SQLite when the SQLite CLI or rusqlite dependency is available.
- Keep it optional and non-blocking by default; record environment and fairness metadata.

Acceptance criteria:
- Harness documents SQLite modes and IcyDB durability semantics.
- At least insert, PK read, indexed range, non-index filter, ordered limit, count, update, delete scenarios run.
- Output includes median, min/max or p95, ratios, and fairness notes.
- CI can skip when SQLite is unavailable while local runs are reproducible.

Estimated effort:
- M

Risk:
- Bad benchmark framing could mislead engineering priorities; keep unsafe SQLite settings separated.

Follow-up patch prompt:
- "Create an optional SQLite/IcyDB comparison benchmark harness using STRICT SQLite schemas and IcyDB typed equivalents. Record environment metadata, WAL modes, median/p95 timings, and clear fairness notes; skip gracefully when SQLite is unavailable."

### Finding 4: Exact cardinality metadata is not yet a first-class planner/diagnostic signal

Category:
- Performance / observability / query planner

Severity:
- High

Priority:
- P1

Confidence:
- High

SQLite reference:
- SQLite uses table/index statistics, especially after `ANALYZE`, to improve plan choices. IcyDB should not copy a nondeterministic or opaque CBO, but it can use exact IC-native metadata.

IcyDB evidence:
- [index/cardinality.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/index/cardinality.rs:1) maintains exact prefix cardinality when synchronized with the row-store generation.
- [index_prefix_cardinality.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/index_prefix_cardinality.rs:226) exposes conservative/unknown liveness paths.
- [ranking.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/planner/ranking.rs:117) ranks by deterministic structural properties, not estimated row count.
- [docs/1.0-TODO.md](/home/adam/projects/icydb/docs/1.0-TODO.md:1) includes index cardinality, row count, median record size, and storage footprint as open items.

Current behaviour:
- Cardinality metadata exists for execution support, but plan explain/admission/operator reporting do not yet treat exact counts as a central signal.

Why it matters:
- Exact prefix counts could let IcyDB reject or explain expensive reads earlier, choose among deterministic candidate plans better, and provide operators with actionable index-design feedback.

Likely root cause:
- Cardinality work was added for liveness/optimization slices and has not been promoted to a public diagnostic/planner contract.

Recommended fix:
- Minimal safe change: surface exact per-index prefix counts in explain output and storage/index diagnostics when synchronized.
- Optional deeper change: add deterministic ranking tie-breakers or read-admission estimates based on exact counts with fail-closed stale-generation checks.

Acceptance criteria:
- Explain shows candidate cardinality and whether it is exact, stale, or unavailable.
- Storage report includes per-index entry count/cardinality summary without unbounded output.
- Planner tests cover two indexes where exact count changes the deterministic tie-breaker.
- Admission tests reject known-expensive shapes using exact counts where configured.

Estimated effort:
- M

Risk:
- Using stale cardinality would create wrong plans or misleading admission; generation checks must be strict.

Follow-up patch prompt:
- "Promote exact index cardinality metadata into query explain and bounded diagnostics, then add deterministic planner/admission tests that only use synchronized exact counts."

### Finding 5: Ordered small-limit paths still materialize excessive work

Category:
- Performance / query execution

Severity:
- High

Priority:
- P1

Confidence:
- Medium

SQLite reference:
- SQLite avoids sort/temp structures when an index can deliver the requested order, and it can stop early for ORDER BY + LIMIT when the plan supports it.

IcyDB evidence:
- [sql_perf_195_1_liveness_full_matrix.md](/home/adam/projects/icydb/testing/integration/target/perf-hotspots/sql_perf_195_1_liveness_full_matrix.md:13) shows ordered range + small-limit scenarios as top instruction consumers.
- [artifacts/perf-audit/sql_perf_deterministic_matrix_current.md](/home/adam/projects/icydb/artifacts/perf-audit/sql_perf_deterministic_matrix_current.md:213) shows historical attribution dominated by scan, row read, and order window for similar shapes.
- [READ_ADMISSION.md](/home/adam/projects/icydb/docs/contracts/READ_ADMISSION.md:123) protects public typed/fluent paths, so this is more about trusted/admin/perf and better plan selection than unbounded public exposure.

Current behaviour:
- Some queries with range predicate on one field and order by another field read many candidates before applying the small limit.

Why it matters:
- These are exactly the shapes users tend to write for feeds and dashboards: "recent/highest/lowest N matching X." They are common and expensive if the index shape is wrong or not exploited.

Likely root cause:
- The selected index supports filtering or ordering, but not both; the engine lacks enough composite/covering/order-aware pushdown or index-design diagnostics for these shapes.

Recommended fix:
- Minimal safe change: add explain hints when a query is bounded only after materialized ordering and recommend the composite index shape that would satisfy filter + order.
- Optional deeper change: expand composite ordered index support and planner ranking for filter+order+limit patterns.

Acceptance criteria:
- Explain output includes `order_materialized=true`, estimated candidate count, and suggested index fields where deterministic.
- Perf matrix row for at least one top `name_range age_asc limitN` scenario improves after a targeted composite/order-compatible path.
- Tests ensure public read admission still rejects unsafe materialized sorts by default.

Estimated effort:
- S for explain/index hints; M/L for broader plan support.

Risk:
- Over-eager index choice could make broad filters worse. Start with diagnostics and exact-count tie-breakers.

Follow-up patch prompt:
- "Add explain diagnostics for ORDER BY + LIMIT paths that materialize ordering, including exact candidate count when available and a suggested composite index shape; keep public admission behavior unchanged."

### Finding 6: Fast-path ownership still has documented unguarded areas

Category:
- Testing / cleanup / performance

Severity:
- Medium

Priority:
- P1

Confidence:
- High

SQLite reference:
- SQLite's testing discipline includes regression tests that lock down optimizer fast paths. IcyDB needs smaller IC-native tripwires for every dedicated fast path.

IcyDB evidence:
- [docs/governance/fast-path-inventory.md](/home/adam/projects/icydb/docs/governance/fast-path-inventory.md:164) lists remaining unguarded areas: stream fast-path precedence helpers, grouped dedicated fast-path ownership, and bytes-terminal derivation exception.

Current behaviour:
- Fast-path inventory is good, but some ownership boundaries are still documented as unguarded.

Why it matters:
- IcyDB has accumulated several fast paths. Without explicit tripwire tests, future cleanup can accidentally bypass a fast path or create duplicated precedence logic.

Likely root cause:
- Performance slices added behavior faster than the inventory/test harness could fully lock down ownership.

Recommended fix:
- Add focused tests for each listed unguarded area, preferably assertion-level tests that fail when the wrong path owns a query.

Acceptance criteria:
- Inventory is updated from "unguarded" to test references.
- Tests fail if stream/grouped/bytes terminal fast paths are bypassed or double-owned.
- No production behavior change required.

Estimated effort:
- S

Risk:
- Tests may need stable attribution labels; avoid brittle instruction counts.

Follow-up patch prompt:
- "Close the remaining unguarded entries in `docs/governance/fast-path-inventory.md` by adding ownership/tripwire tests for stream precedence, grouped fast paths, and bytes-terminal derivation."

### Finding 7: Durable schema migration execution is not yet ready for broad DDL

Category:
- Correctness / architecture / storage

Severity:
- High

Priority:
- P1

Confidence:
- High

SQLite reference:
- SQLite has explicit schema metadata/versioning and carefully managed schema changes. IcyDB's equivalent should be a durable schema mutation state machine, not runtime reconstruction from generated models.

IcyDB evidence:
- [reconcile.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/schema/reconcile.rs:1) supports exact match, append-only, metadata index rename, and AddFieldPathIndex recognition.
- [reconcile.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/schema/reconcile.rs:260) fails closed when mutation capabilities or runner support are missing.
- [mutation/execution.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/schema/mutation/execution.rs:1) describes physical execution steps as contracts for runner behavior.
- Prior audit context still calls out migration state-machine work.

Current behaviour:
- Catalog-native mutation planning exists and is intentionally conservative. Broad physical migration execution is not yet a general durable product surface.

Why it matters:
- Schema mutation is where type guarantees, persisted layout, index rebuild, and canister upgrade safety meet. Partial migration is a high-severity failure mode.

Likely root cause:
- IcyDB correctly prioritized fail-closed schema reconciliation before broad migration execution.

Recommended fix:
- Add durable migration ID/phase/watermark state, resumable steps, explicit publication point, and recovery tests before enabling more mutation kinds.

Acceptance criteria:
- Migration state survives simulated interruption at every phase.
- Old schema remains readable until publication, or failure mode is explicitly fail-closed with repair guidance.
- Index rebuild/mutation has validation before publish.
- Docs define migration semantics and non-goals.

Estimated effort:
- L

Risk:
- Migration framework can become too general. Keep first slice to one currently planned mutation kind.

Follow-up patch prompt:
- "Design and implement the first durable schema migration state slice for one supported mutation kind, with migration id, phase, watermark, publish point, recovery tests, and documentation."

### Finding 8: Durability health lacks marker/journal/fold diagnostics

Category:
- Observability / durability / operations

Severity:
- Medium

Priority:
- P1

Confidence:
- High

SQLite reference:
- SQLite exposes enough operational state around journal/WAL modes and recovery behavior for diagnosis. IcyDB needs an IC-native health surface, not POSIX file PRAGMAs.

IcyDB evidence:
- [journal/store.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/journal/store.rs:48) tracks fold watermarks.
- [marker.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/commit/marker.rs:32) defines commit marker invariants.
- [recovery.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/commit/recovery.rs:175) has explicit recovery phases.
- [storage_report.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/diagnostics/storage_report.rs:1) reports storage/corruption but not marker presence, tail ranges, chunk counts, fold watermark, or last recovery phase/result.

Current behaviour:
- Operators can inspect storage and integrity, but not journal/marker/fold health in one bounded report.

Why it matters:
- When recovery, journal growth, or fold lag is suspected, the current diagnostics do not provide the same direct operational footing as SQLite's journal/WAL introspection.

Likely root cause:
- Durable internals are implemented but the report model predates a full operator health view.

Recommended fix:
- Add a bounded `DurabilityHealth` diagnostic with marker state, journal tail sequence range, chunk count, fold watermark, live/canonical generation, recovery-ready state, and last recovery summary where available.

Acceptance criteria:
- Diagnostic does not scan unbounded row data.
- Tests cover clean state, marker-present state, journal-tail state, and post-recovery state.
- Output is available through the same diagnostics feature/API path as storage/integrity reports.

Estimated effort:
- S/M

Risk:
- Exposing too much internal detail can become API debt; mark low-level fields diagnostic/stability-limited if needed.

Follow-up patch prompt:
- "Add a bounded durability health diagnostic report that surfaces commit marker state, journal tail/fold-watermark state, recovery readiness, and tests for clean, dirty, and recovered stores."

### Finding 9: Persisted format policy is explicit but byte-level format docs are missing

Category:
- Documentation / storage / architecture

Severity:
- Medium

Priority:
- P1

Confidence:
- High

SQLite reference:
- SQLite has a public file-format document. IcyDB does not need SQLite pages, but it does need a stable-memory format specification for review and recovery.

IcyDB evidence:
- [PERSISTED_FORMAT_INVENTORY.md](/home/adam/projects/icydb/docs/contracts/PERSISTED_FORMAT_INVENTORY.md:10) says the inventory is a checklist, not a byte-level specification.
- [PERSISTED_FORMAT_POLICY.md](/home/adam/projects/icydb/docs/contracts/PERSISTED_FORMAT_POLICY.md:30) defines the pre-1.0 hard-cut policy and fail-closed posture.
- [DURABILITY.md](/home/adam/projects/icydb/docs/contracts/DURABILITY.md:138) states checksums are not currently exposed as a documented corruption-detection surface.

Current behaviour:
- Durable surfaces are inventoried and governed, but a reviewer cannot reconstruct all stable-memory byte/record/header invariants from a single spec.

Why it matters:
- Storage specs are essential for upgrade safety, corruption analysis, external tooling, and pre-1.0 hard cuts.

Likely root cause:
- The project has moved quickly through format changes and intentionally avoided premature compatibility guarantees.

Recommended fix:
- Create a byte-level stable storage spec covering schema snapshot keys, data rows, index keys, journal chunks, commit marker encoding, version headers, and fail-closed decode rules.

Acceptance criteria:
- Spec links each durable surface to code and tests.
- Spec states what is versioned, what is pre-1.0 hard-cut, and what decode failures mean.
- Any future persisted-format PR must update the spec.

Estimated effort:
- M

Risk:
- Spec can fossilize unfinished formats. Label pre-1.0 instability clearly.

Follow-up patch prompt:
- "Draft a byte-level persisted storage format spec that maps every durable stable-memory surface to code, tests, version/fail-closed rules, and pre-1.0 compatibility policy."

### Finding 10: Query-side metrics are intentionally not surfaced, leaving an observability gap

Category:
- Observability / API

Severity:
- Medium

Priority:
- P1

Confidence:
- High

SQLite reference:
- SQLite has `EXPLAIN QUERY PLAN` and introspection tools. IcyDB must respect IC query-call immutability, so it needs a different shape.

IcyDB evidence:
- [metrics/state.rs](/home/adam/projects/icydb/crates/icydb-core/src/metrics/state.rs:6) says query-side instrumentation may update an operation-local metrics state but is not surfaced through the normal `report` path.
- `EventOps` includes plan/cache/rows/index counters, showing the data is conceptually modeled.

Current behaviour:
- Query execution can collect operation-local attribution, but production query-call metrics cannot mutate persistent counters.

Why it matters:
- Operators need to know which plans are slow, whether caches hit, and whether read admission is preventing expensive access patterns.

Likely root cause:
- Correct IC semantics prevent simple global query counters from query calls.

Recommended fix:
- Add query-safe explain/analyze output that returns operation-local metrics to the caller, and optionally add update-lane sampling/snapshot APIs for trusted diagnostics.

Acceptance criteria:
- Query calls do not mutate canister state.
- Explain/analyze includes plan, index, candidate count, rows read, rows returned, cache hit/miss, and materialization flags.
- Tests prove metrics collection does not affect query results or state.

Estimated effort:
- M

Risk:
- Diagnostics can become expensive; make analyze explicit and bounded.

Follow-up patch prompt:
- "Add a query-safe explain/analyze result that returns operation-local read metrics without mutating canister state, with tests proving state invariance."

### Finding 11: Caches are bounded but fixed FIFO and weakly tunable

Category:
- Performance / configuration / cleanup

Severity:
- Medium

Priority:
- P2

Confidence:
- Medium

SQLite reference:
- SQLite exposes page-cache and prepared-statement reuse principles. IcyDB has plan caches, but cache policy should be visible and measured.

IcyDB evidence:
- [session/query/cache.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/session/query/cache.rs:1) defines a shared query plan cache with max entries.
- [session/sql/cache.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/cache.rs:1) defines a SQL compiled-command cache.
- [bounded_cache.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/session/bounded_cache.rs:1) implements a fixed-size `HashMap + VecDeque` bounded cache.

Current behaviour:
- Caches exist and are invalidated using schema identities/fingerprints, but capacity/policy are fixed and not obviously exposed as a configuration/perf-tuning surface.

Why it matters:
- Workloads with many query shapes may churn a FIFO cache and lose hot repeated shapes, especially if SQL strings differ only by literals.

Likely root cause:
- The current cache implementation favors simple deterministic bounded behavior.

Recommended fix:
- First surface hit/miss/eviction reason in explain/perf attribution and allow a bounded capacity option. Only switch to LRU if churn evidence supports it.

Acceptance criteria:
- Cache hit/miss/eviction counters are visible in explicit diagnostics.
- Capacity can be configured or documented as fixed.
- Benchmarks include repeated same-shape/different-parameter scenarios.

Estimated effort:
- S/M

Risk:
- More configuration surface can confuse users; default should remain conservative.

Follow-up patch prompt:
- "Expose bounded query/SQL cache capacity and hit/miss/eviction diagnostics, then add repeated-shape benchmark scenarios before considering an LRU policy."

### Finding 12: Differential testing against SQLite is only a design idea today

Category:
- Testing / correctness

Severity:
- High

Priority:
- P0

Confidence:
- High

SQLite reference:
- SQLite's testing discipline includes independent oracles and malformed input tests. A STRICT SQLite database can be a useful oracle for overlapping typed semantics.

IcyDB evidence:
- [docs/design/ideas/sql-fuzzer.md](/home/adam/projects/icydb/docs/design/ideas/sql-fuzzer.md:1) exists as an idea.
- No committed differential SQLite harness was found by searches for SQLite/sqlite/differential outside docs/artifacts.
- Existing tests are broad but engine-internal.

Current behaviour:
- IcyDB query/index correctness is tested internally, but not routinely compared against an external embedded database oracle for shared relational semantics.

Why it matters:
- Differential tests find planner/index/order/limit bugs that are easy to miss with hand-written examples.

Likely root cause:
- IcyDB semantics intentionally differ from SQLite in type system and IC constraints, so the oracle boundary needs careful design.

Recommended fix:
- Build a small random operation harness over STRICT schemas and classify mismatches as intended difference, IcyDB bug, model limitation, or SQLite-only feature.

Acceptance criteria:
- Random insert/update/delete/get/range/filter/count/sort/limit sequences run against both engines.
- The harness uses only overlapping semantics and explicit ordering.
- Failure output includes seed and minimized operation sequence where feasible.
- CI can run a small deterministic seed set and local audit can run larger sets.

Estimated effort:
- M

Risk:
- False positives if semantic mapping is too loose. Keep schema and operations narrow initially.

Follow-up patch prompt:
- "Implement a small SQLite STRICT differential test harness for users/posts/events with random insert/update/delete/query sequences and explicit mismatch classification."

### Finding 13: Fault-injection, corruption, and OOM-style tests remain below SQLite's bar

Category:
- Testing / durability / correctness

Severity:
- High

Priority:
- P1

Confidence:
- Medium

SQLite reference:
- SQLite heavily tests I/O errors, OOM, crashes, malformed databases, and recovery paths.

IcyDB evidence:
- [DURABILITY.md](/home/adam/projects/icydb/docs/contracts/DURABILITY.md:155) says recovery-size evidence is limited and only internally produced states are in scope.
- [PERSISTED_FORMAT_POLICY.md](/home/adam/projects/icydb/docs/contracts/PERSISTED_FORMAT_POLICY.md:123) defines fail-closed persisted decode rules.
- Existing `cargo test` coverage is strong, but no broad corruption/fault-injection harness was found.

Current behaviour:
- Recovery and integrity code exists and is tested, but not with SQLite-style systematic injected failures across every phase.

Why it matters:
- Durable code is rarely exercised by happy-path tests. The highest-impact bugs are usually in partial failure, corrupted bytes, stale markers, or decode failure paths.

Likely root cause:
- The project has focused on contracts and deterministic internal recovery before building adversarial harnesses.

Recommended fix:
- Add a phase-by-phase recovery fault harness that can inject failure before/after marker write, journal append, canonical fold, live rebuild, index rebuild, marker clear, and decode.

Acceptance criteria:
- Each injected phase either recovers to old state, recovers to new state, or fails closed with a typed diagnostic.
- Integrity report passes after successful recovery.
- Corrupted persisted bytes fail closed without mutating state.

Estimated effort:
- L

Risk:
- Test harness may need storage test hooks; keep hooks test-only and minimal.

Follow-up patch prompt:
- "Add a test-only recovery fault-injection harness that interrupts each commit/recovery phase and asserts fail-closed or fully recovered invariants."

### Finding 14: Public read admission is strong, but performance guidance needs to teach index design

Category:
- Documentation / API / performance

Severity:
- Medium

Priority:
- P2

Confidence:
- High

SQLite reference:
- SQLite documents planner behavior so users can shape indexes and queries. IcyDB should document its deterministic admission/planning rules.

IcyDB evidence:
- [READ_ADMISSION.md](/home/adam/projects/icydb/docs/contracts/READ_ADMISSION.md:123) documents default rejection rules.
- [QUERY_CONTRACT.md](/home/adam/projects/icydb/docs/contracts/QUERY_CONTRACT.md:181) documents post-access cursor semantics.
- Performance artifacts show top costs are specific query/index-shape combinations.

Current behaviour:
- Contracts define safe behavior, but there is not yet a practical "how to design indexes for bounded canister reads" guide.

Why it matters:
- Users can write safe but slow admin/trusted queries, or be surprised by public admission rejections, unless the planner's deterministic rules are explained with examples.

Likely root cause:
- Contract docs are written for correctness/governance more than user tuning.

Recommended fix:
- Add a query performance guide explaining equality-prefix indexes, range bounds, order compatibility, covering projections, cursor pagination, and admission rejection examples.

Acceptance criteria:
- Guide includes examples for good/bad index shapes and their explain output.
- Links to read admission, query contract, and benchmark methodology.
- Examples compile or are covered by doc tests where feasible.

Estimated effort:
- S

Risk:
- Docs can drift; link examples to tests/perf harness names.

Follow-up patch prompt:
- "Write a query performance and index-design guide for IcyDB's deterministic planner, including equality-prefix, range, ORDER BY + LIMIT, covering, cursor, and read-admission examples."

### Finding 15: `cargo bench`/allocation regression gates are absent

Category:
- Performance / testing

Severity:
- Medium

Priority:
- P2

Confidence:
- High

SQLite reference:
- SQLite combines correctness testing with performance regression discipline. IcyDB already has instruction matrices; it needs a smaller fast native regression layer too.

IcyDB evidence:
- `find` found no project `benches/` files after excluding third-party/cache directories.
- [execution_hot_path_bench.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/session/tests/execution_hot_path_bench.rs:35) says it avoids allocation, instruction, and memory sampling.
- Existing performance report tests are ignored/manual.

Current behaviour:
- Performance evidence comes mostly from ignored integration/perf report tests and artifacts, not from a standard cargo bench target.

Why it matters:
- Small allocation regressions in encode/decode/query caches can land without a local fast benchmark catching them.

Likely root cause:
- PocketIC instruction reports are the most target-relevant metric and have taken priority.

Recommended fix:
- Add a minimal Criterion or iai-callgrind-style benchmark suite for native hot paths, plus optional allocation counters for decode/projection/PK/index reads.

Acceptance criteria:
- `cargo bench` runs at least PK get, indexed range, covering projection, recursive decode, and plan-cache reuse.
- Bench outputs are documented and not required in normal fast CI unless configured.
- Allocation counts are captured for decode/projection scenarios.

Estimated effort:
- M

Risk:
- Native microbenchmarks can mislead for wasm/stable memory; label them as native hot-path screens.

Follow-up patch prompt:
- "Add a small native cargo benchmark target for IcyDB hot paths with allocation counters, explicitly labeled as a complement to PocketIC instruction benchmarks."

### Finding 16: Checksums/import/export remain explicit non-goals but need a decision record before 1.0

Category:
- Durability / docs / tooling

Severity:
- Medium

Priority:
- P2

Confidence:
- High

SQLite reference:
- SQLite's file format and recovery behavior are heavily documented, but it does not imply IcyDB should expose raw stable-memory backup/import without a contract.

IcyDB evidence:
- [DURABILITY.md](/home/adam/projects/icydb/docs/contracts/DURABILITY.md:33) says raw backup/import is out of scope today.
- [DURABILITY.md](/home/adam/projects/icydb/docs/contracts/DURABILITY.md:138) says checksums are not currently exposed.
- [PERSISTED_FORMAT_POLICY.md](/home/adam/projects/icydb/docs/contracts/PERSISTED_FORMAT_POLICY.md:95) classifies checksum adoption as a persisted-format decision.

Current behaviour:
- The project correctly avoids promising raw backup/import/checksum semantics before the format is stable.

Why it matters:
- Before 1.0, operators will need to know whether corruption detection, export/import, and repair tooling are supported or intentionally deferred.

Likely root cause:
- Checksums and import/export are format-stability commitments.

Recommended fix:
- Add a 1.0 decision record covering checksums, raw export/import, logical export/import, repair scope, and CLI health commands.

Acceptance criteria:
- Decision states "copy SQLite idea", "do IC-native variant", or "do not support".
- If checksums are deferred, detection alternatives are documented.
- CLI/tooling backlog is updated.

Estimated effort:
- S/M

Risk:
- Premature checksum format can create migration burden; decide only with threat model.

Follow-up patch prompt:
- "Write a 1.0 durability tooling decision record for checksums, raw/logical export-import, repair scope, and CLI health commands."

### Finding 17: Row-storage offset layout is the highest-leverage strategic performance bet

Category:
- Architecture / performance / serialization

Severity:
- High

Priority:
- P1

Confidence:
- Medium

SQLite reference:
- SQLite's serial record format and B-tree payload layout are designed for compactness and field-level access. IcyDB should pursue an IC-native row layout, not SQLite pages.

IcyDB evidence:
- [docs/ROADMAP.md](/home/adam/projects/icydb/docs/ROADMAP.md:153) explicitly proposes replacing CBOR row-payload decode with offset-based row encoding/direct field access.
- [value.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/data/structural_field/value_storage/decode/value.rs:484) shows current recursive decode allocation TODOs.
- Performance artifacts show row reads are a large component of top query costs.

Current behaviour:
- Row access can require decoding broader payloads than the query actually needs.

Why it matters:
- Projection pushdown, covering indexes, count/exists/min/max, and read admission all get more powerful when field-level access is cheap and predictable.

Likely root cause:
- Current encoding was optimized for correctness and flexibility before final field-access layout.

Recommended fix:
- First implement a side-by-side experimental row codec behind tests/benchmarks. Do not migrate persisted production format until evidence shows clear wins and fail-closed format policy is updated.

Acceptance criteria:
- Benchmarks compare old/new row codec for full decode, projected fields, nested fields, and malformed input.
- Format version policy is updated before persisted adoption.
- Fallback compatibility is not retained pre-1.0 unless explicitly required.

Estimated effort:
- L

Risk:
- Persisted format churn and migration complexity. Keep experiment non-persistent until validated.

Follow-up patch prompt:
- "Prototype an offset-addressable row codec in tests/benchmarks only, compare decode/projection allocation and instruction cost, then prepare a persisted-format policy update if it wins."

### Finding 18: IcyDB should not copy several SQLite mechanisms

Category:
- Architecture

Severity:
- Low

Priority:
- P3

Confidence:
- High

SQLite reference:
- SQLite has multi-process locking, POSIX fsync assumptions, temp files, a huge SQL surface, dynamic typing, and broad PRAGMA configurability.

IcyDB evidence:
- [RESOURCE_MODEL.md](/home/adam/projects/icydb/docs/contracts/RESOURCE_MODEL.md:1) defines IC constraints.
- [SQL_SUBSET.md](/home/adam/projects/icydb/docs/contracts/SQL_SUBSET.md:1) explicitly scopes SQL.
- [ROADMAP.md](/home/adam/projects/icydb/docs/ROADMAP.md:226) states non-goals and target identity.

Current behaviour:
- IcyDB already takes an IC-native typed approach.

Why it matters:
- The risk of a SQLite comparison audit is copying features that are actively wrong for canisters.

Likely root cause:
- SQLite is a useful reference but has a different operating environment.

Recommended fix:
- Keep a "do not copy from SQLite" section in architecture docs: POSIX fsync semantics, temp disk spill, general-purpose cost-based optimizer as primary plan contract, dynamic typing, arbitrary joins/window SQL, multi-process locking, unsafe speed PRAGMAs.

Acceptance criteria:
- Architecture docs explicitly classify SQLite ideas as copy, adapt, or reject.
- Future roadmap items reference IC-specific success metrics.

Estimated effort:
- XS

Risk:
- Over-documentation; keep it short and tied to roadmap.

Follow-up patch prompt:
- "Add a short architecture note classifying SQLite ideas IcyDB should copy, adapt IC-natively, or reject, with links to resource model and SQL subset docs."

## Quick wins

| Quick win | Why it is low-risk | Acceptance criteria |
|---|---|---|
| Add explain flag for post-access cursor handling | Diagnostic-only first slice | Explain distinguishes pushed/post-access cursor |
| Add fast-path tripwire tests | No production behavior change | `fast-path-inventory.md` links to tests |
| Add recursive decode allocation microbench | Benchmark-only | Allocation count captured for list/map decode |
| Add storage/durability health report skeleton | Bounded metadata only | Marker/journal/fold state visible |
| Add cache hit/miss attribution to explain | Uses existing cache metadata | Tests for hit/miss/miss reason |
| Add query performance/index-design doc | Docs-only | Examples for equality/range/order/limit/covering |
| Add benchmark harness skip-if-sqlite-missing | Optional local tooling | Records environment and skips cleanly |
| Add byte-format spec scaffold | Docs-only | Maps durable surfaces to code/tests |
| Add deterministic test for exact cardinality sync | Focused invariant test | Stale cardinality fails closed |
| Add mismatch classification enum for future differential harness | Test support only | Intended/IcyDB bug/model limitation/SQLite-only classes |

## Medium-term optimisation roadmap

### 1. Query/index planner

- **Problem:** deterministic planner is strong but not yet using exact cardinality and cursor boundaries enough.
- **Sequence:** explain cardinality -> cursor pushdown for narrow ordered index pages -> materialized order hints -> deterministic exact-count tie-breakers -> expanded covering/order-compatible plans.
- **Dependencies:** synchronized cardinality generation checks and stable explain labels.
- **Risk:** pagination/order correctness.
- **Expected benefit:** lower instruction count for small-page reads and clearer user index design.
- **Measure:** row reads, index entries visited, instruction count, explain flags for top matrix rows.

### 2. Storage/stable memory

- **Problem:** durable internals are strong but not fully visible/spec'd.
- **Sequence:** durability health report -> byte-format spec -> corruption/fault injection -> checksum/import/export decision.
- **Dependencies:** stable diagnostic DTO boundaries and persisted-format policy.
- **Risk:** exposing unstable internals as public API.
- **Expected benefit:** easier operations, safer upgrades, clearer 1.0 readiness.
- **Measure:** recovery/fault tests and diagnostic coverage.

### 3. Serialization

- **Problem:** recursive decode and row payload access allocate/read too much.
- **Sequence:** streaming recursive visitors -> projection-only decode tests -> offset row codec prototype -> persisted-format decision if benchmarks win.
- **Dependencies:** decode error-path parity tests.
- **Risk:** malformed data and path diagnostics.
- **Expected benefit:** fewer allocations and lower instruction count for projection/filter paths.
- **Measure:** allocation count, wasm instruction count, payload size.

### 4. Caching

- **Problem:** caches exist but are fixed FIFO and not sufficiently observable.
- **Sequence:** hit/miss/eviction attribution -> capacity option/doc -> repeated-shape benchmark -> LRU or shape-normalization only if evidence supports it.
- **Dependencies:** no query-call state mutation.
- **Risk:** tuning surface creep.
- **Expected benefit:** stable repeated-query performance.
- **Measure:** hit rate, eviction count, repeated query latency/instructions.

### 5. Macro/codegen

- **Problem:** no evidence of macro runtime bottleneck, but compile-time and expanded-size costs should be measured.
- **Sequence:** macro expansion size report -> compile-time benchmark -> generated-code hygiene tests.
- **Dependencies:** stable fixtures.
- **Risk:** premature optimization.
- **Expected benefit:** maintainable generated APIs and predictable wasm size.
- **Measure:** compile time, expanded line count, wasm size.

### 6. Observability

- **Problem:** query metrics cannot be normal mutable counters; durability health is missing.
- **Sequence:** query-safe explain/analyze -> update-lane sampled diagnostic snapshots -> durability health report -> CLI hooks.
- **Dependencies:** clear query/update semantic boundary.
- **Risk:** diagnostics themselves become expensive.
- **Expected benefit:** production debug without unsafe mutation.
- **Measure:** bounded report sizes and operator use cases covered.

### 7. Testing/fuzzing

- **Problem:** strong normal coverage, weaker adversarial coverage.
- **Sequence:** fast-path tripwires -> SQLite STRICT differential harness -> persisted decode fuzzing -> recovery fault injection -> OOM/resource simulation where feasible.
- **Dependencies:** minimal test hooks and deterministic seeds.
- **Risk:** flaky tests if resource/time limits are not bounded.
- **Expected benefit:** higher confidence in invariants.
- **Measure:** seeds/run count, minimized failure output, invariant coverage matrix.

### 8. Documentation

- **Problem:** contracts are strong but scattered for performance/storage users.
- **Sequence:** query performance guide -> storage byte-format spec -> migration semantics -> benchmark methodology -> operator troubleshooting.
- **Dependencies:** explain output and diagnostics names.
- **Risk:** docs drift.
- **Expected benefit:** faster implementation and review of roadmap slices.
- **Measure:** docs link to tests and code owners.

## Strategic architecture bets

### Explicit query plan representation

- **Why now:** explain, cursor pushdown, cardinality, and materialization diagnostics all need stable plan facts.
- **Why not now:** broad planner refactor could distract from narrow hotspot fixes.
- **Minimal first slice:** add plan facts for cursor handling, cardinality source, order materialization, and covering status.
- **Kill criteria:** if explain can be extended without a new representation and tests remain clear, defer a larger refactor.

### Covering index expansion

- **Why now:** row-read cost dominates several top scenarios.
- **Why not now:** covering semantics must preserve row-presence and live/tombstone correctness.
- **Minimal first slice:** one additional projection shape with tests comparing index path to scan path.
- **Kill criteria:** if row codec/projection work removes most row-read cost first, deprioritize broader covering support.

### Offset-addressable row storage

- **Why now:** current TODOs and roadmap identify decode allocation as a central bottleneck.
- **Why not now:** persisted format churn is high risk before measurement.
- **Minimal first slice:** non-persisted experimental codec benchmark.
- **Kill criteria:** if benchmarks show minor wins or complexity is high, stick to streaming visitors and covering indexes.

### Durable schema migration framework

- **Why now:** schema mutation is the largest correctness risk before 1.0.
- **Why not now:** broad migration support may be unnecessary for immediate users.
- **Minimal first slice:** durable state for one supported mutation kind.
- **Kill criteria:** if public migration scope remains intentionally narrow, document non-goals and defer broader runner.

### Differential testing harness

- **Why now:** it can catch planner/index/order bugs cheaply.
- **Why not now:** needs careful semantic mapping to avoid false positives.
- **Minimal first slice:** STRICT `users` table with insert/update/delete/get/filter/order/limit.
- **Kill criteria:** if SQLite dependency friction is too high, make it optional/local and keep deterministic seeds in CI.

## Test gap report

| Category | Current coverage | Missing scenario | SQLite-inspired rationale | Proposed test file | Example test | Property/fuzz/diff? |
|---|---|---|---|---|---|---|
| Cursor pushdown | Contract says post-access; tests cover pagination generally | Index-boundary seek pages | B-tree cursor stops early | `crates/icydb-core/src/db/query/tests/cursor_pushdown.rs` | `index_ordered_cursor_starts_after_boundary` | Property for no skip/dup |
| Recursive decode | Unit decode tests exist | Projection validates nested list/map without allocation | Serial-record field skipping | `crates/icydb-core/src/db/data/structural_field/tests/projection_decode.rs` | `nested_projection_uses_streaming_visitor` | Microbench/allocation |
| Index consistency | Integrity report exists | Random updates/deletes across scan/index equality | Index path equals scan path | `testing/integration/tests/index_diff.rs` | `random_index_path_matches_scan_path` | Property |
| SQLite differential | Design idea only | STRICT external oracle | Independent reference engine | `testing/integration/tests/sqlite_diff.rs` | `strict_users_random_sequence_matches_sqlite` | Differential |
| Recovery fault injection | Recovery tests exist | Interrupt every marker/journal/fold phase | Crash recovery testing | `crates/icydb-core/src/db/commit/tests/fault_injection.rs` | `interrupted_after_marker_recovers_or_fails_closed` | Fault injection |
| Corrupt persisted bytes | Fail-closed policy docs | Mutate row/index/journal bytes | Malformed DB tests | `crates/icydb-core/src/db/commit/tests/corruption.rs` | `corrupt_journal_chunk_fails_closed` | Fuzz |
| Cache invalidation | Cache code tests exist | Same shape/different params and schema change | Prepared statement reuse | `crates/icydb-core/src/db/session/tests/query_cache.rs` | `cache_hit_survives_param_change_but_not_schema_change` | Property optional |
| Fast-path ownership | Inventory exists | Remaining unguarded fast paths | Optimizer regression tests | Existing fast-path test modules | `grouped_fast_path_has_single_owner` | Unit/tripwire |
| Macro errors | Trybuild exists | Public examples and confusing schema failures | API stability | `testing/macro/tests/ui` | `duplicate_index_message_is_actionable` | Compile-fail |
| Wasm/stable counters | PocketIC perf exists | Stable read/write bytes per scenario | IC performance portability | `testing/integration/tests/stable_io_perf.rs` | `pk_get_stable_io_budget` | Benchmark |
| Migration state | Reconcile/runner tests exist | Interrupted migration resume | Schema cookie/recovery | `crates/icydb-core/src/db/schema/tests/migration_recovery.rs` | `add_index_migration_resumes_from_watermark` | Fault/property |
| Docs examples | Contracts exist | Query performance examples compile | SQLite planner docs style | doc tests or integration examples | `order_limit_requires_matching_index` | Doc test |

## Documentation gap report

| Missing/stale doc | Current state | Recommended doc |
|---|---|---|
| Storage byte format | Inventory is a checklist, not a byte-level spec | `docs/contracts/STABLE_STORAGE_FORMAT.md` |
| Query performance guide | Read admission and query contract are governance-focused | `docs/guides/query-performance.md` |
| Benchmark methodology | Performance artifacts exist, no unified methodology | `docs/guides/benchmarking.md` |
| Migration semantics | Mutation runner contracts exist, user-facing migration semantics incomplete | `docs/contracts/MIGRATION_SEMANTICS.md` |
| Durability health/operator guide | Durability contract exists, health fields not surfaced | `docs/operations/DURABILITY_HEALTH.md` |
| Feature flags/config | Cargo features scattered | `docs/guides/configuration.md` |
| Canister upgrade safety | Durability docs cover pieces | Consolidated upgrade guide |
| Observability guide | Metrics/diagnostics exist | Explain/analyze and diagnostic report guide |
| Differential testing guide | SQL fuzzer idea exists | `docs/testing/differential-sqlite.md` |

## Risk register

| Risk | Severity | Likelihood | Detection method | Mitigation | Owner suggestion |
|---|---|---:|---|---|---|
| Cursor pushdown returns duplicate/skipped rows | High | Medium | Pagination property tests | Limit first slice to deterministic index-compatible plans | Query planner owner |
| Streaming decode changes error semantics | Medium | Medium | Decode parity tests | Preserve path-aware errors | Storage/codec owner |
| Stale cardinality used for planning | High | Low/Medium | Generation mismatch tests | Fail closed on stale metadata | Index owner |
| Migration interrupted mid-rebuild | Critical | Medium after DDL widens | Fault-injection migration tests | Durable migration phase/watermark | Schema owner |
| Journal/fold growth unseen by operators | Medium | Medium | Durability health report | Add bounded diagnostic | Durability owner |
| Differential harness false positives | Medium | Medium | Mismatch classification | Start with STRICT narrow semantics | Testing owner |
| Native benchmarks mislead wasm decisions | Medium | Medium | Compare with PocketIC counters | Label native benches as hot-path screens | Perf owner |
| Byte-format docs fossilize pre-1.0 design | Low | Medium | Governance review | Mark hard-cut policy clearly | Docs/storage owner |
| Cache configurability increases API surface | Low | Medium | API review | Prefer diagnostics before knobs | API owner |
| Query metrics mutate state accidentally | High | Low | State-invariance tests | Return operation-local metrics only in query calls | Observability owner |

## Prioritised backlog

| Rank | Title | Category | Severity | Effort | Confidence | Expected impact | Files likely touched | Acceptance criteria | Suggested patch order |
|---:|---|---|---|---|---|---|---|---|---:|
| 1 | Cursor-boundary pushdown for index-backed ordered pages | Performance | High | M | High | Large instruction drop for small pages | `db/query`, `db/executor`, cursor tests | Row reads scale with limit; no page skips/dups | 1 |
| 2 | Streaming recursive value decode visitors | Serialization | High | M | High | Fewer allocations in projection/validation | `value_storage/decode`, projection tests | Allocation reduction and decode parity | 2 |
| 3 | SQLite STRICT differential harness skeleton | Testing | High | M | High | Finds query/index/order mismatches | `testing/integration` | Deterministic seed tests and mismatch classes | 3 |
| 4 | Fast-path tripwire tests | Testing | Medium | S | High | Prevents perf regressions | fast-path tests, inventory doc | Inventory unguarded entries closed | 4 |
| 5 | Query explain cardinality/materialization facts | Observability | High | M | High | Shows expensive plan causes | `db/query/plan`, explain tests | Explain includes exact/stale/unavailable counts | 5 |
| 6 | Durability health diagnostic | Observability | Medium | S/M | High | Better recovery/journal operations | `db/diagnostics`, `journal`, `commit` | Marker/journal/fold fields tested | 6 |
| 7 | Optional SQLite/IcyDB benchmark harness | Performance | High | M | High | Fair optimization evidence | `testing/integration` or audit bench dir | Skip-if-unavailable; emits med/p95/ratios | 7 |
| 8 | Query performance/index-design guide | Docs | Medium | S | High | Fewer slow/rejected queries | `docs/guides` | Examples for equality/range/order/limit | 8 |
| 9 | Stable storage byte-format spec scaffold | Docs/storage | Medium | M | High | Safer format changes | `docs/contracts` | Durable surfaces mapped to code/tests | 9 |
| 10 | Recovery fault-injection harness | Correctness | High | L | Medium | Higher durability confidence | `db/commit/tests` | Every phase recovers or fails closed | 10 |
| 11 | Materialized ORDER BY + LIMIT explain hints | Performance/docs | Medium | S | Medium | Immediate operator insight | `db/query/explain` | Suggested index shape appears | 11 |
| 12 | Cache hit/miss/eviction diagnostics | Performance | Medium | S/M | Medium | Better repeated-query tuning | `session/query/cache`, `session/sql/cache` | Metrics visible without state mutation in query | 12 |
| 13 | Native hot-path cargo bench target | Performance | Medium | M | High | Catches allocation regressions | `benches/` or crate bench target | PK/range/decode/cache benches run | 13 |
| 14 | Persisted decode fuzz target | Testing | High | M | Medium | Finds malformed decode bugs | fuzz/test harness | Corrupt bytes fail closed | 14 |
| 15 | Durable migration state first slice | Correctness | High | L | High | Enables safe DDL growth | `db/schema/mutation` | Phase/watermark/publish recovery tests | 15 |
| 16 | Offset row codec prototype | Architecture | High | L | Medium | Potential large projection win | codec/storage tests/benches | Bench old vs new without persisted adoption | 16 |
| 17 | Covering-index projection expansion | Performance | Medium | M | Medium | Reduces row reads | `query/plan/covering`, executor | Index path equals scan path | 17 |
| 18 | Read-admission exact-count option | Safety/perf | Medium | M | Medium | Earlier rejection/explain | read admission, cardinality | Exact synchronized counts drive bounded decisions | 18 |
| 19 | CLI durability check/export/import decision | Tooling/docs | Medium | S/M | High | Clear 1.0 operator story | docs, `icydb-cli` later | Decision record accepted | 19 |
| 20 | Macro compile-error polish audit | API | Low | S/M | Medium | Better developer UX | macro trybuild tests | More actionable compile-fail messages | 20 |
| 21 | Query-safe analyze API | Observability | Medium | M | High | Per-call plan metrics without mutation | query API/explain | State-invariance tests pass | 21 |
| 22 | Stable memory read/write counter scenarios | Performance | Medium | M | Medium | IC-specific bottleneck evidence | PocketIC perf harness | Bytes/calls recorded for key scenarios | 22 |
| 23 | Storage fragmentation/free-space report | Storage | Medium | M | Low/Medium | Detects durable bloat | storage diagnostics | Bounded estimate documented | 23 |
| 24 | Feature/config guide | Docs/API | Low | S | High | Clear tuning surface | docs | Feature flags and defaults documented | 24 |
| 25 | SQLite copy/adapt/reject architecture note | Docs/architecture | Low | XS | High | Prevents wrong SQLite cloning | docs/design | Short classification note | 25 |

## Final recommendation

### What should be fixed next

Start patching **cursor-boundary pushdown for deterministic index-backed ordered pages** and **streaming recursive value decode**. These are concrete, evidence-backed, and likely to reduce instruction/allocation cost without changing IcyDB's design identity.

### What should be benchmarked next

Benchmark the top ordered `LIMIT 1/3/10` perf-matrix rows before and after cursor pushdown. In parallel, add a small native decode/projection allocation benchmark and an optional SQLite STRICT comparison harness.

### What should be deferred

Defer broad cost-based planning, arbitrary SQL surface expansion, generalized migration support, raw backup/import, and persisted row-format changes until the smaller diagnostic, benchmark, and fail-closed groundwork is in place.

### What should never be copied from SQLite unchanged

Do not copy SQLite's POSIX fsync assumptions, temp disk spill, multi-process locking model, dynamic typing, unsafe speed PRAGMAs as headline baselines, arbitrary SQL feature breadth, or opaque general cost-based planning as the primary contract. Adapt the principles: deterministic index access, serial-record discipline, explainability, atomic recovery, and extreme testing depth.

## Commands run

All shell commands were run from `/home/adam/projects/icydb` unless noted. Web fetches of SQLite reference pages are listed separately above.

| Command | Status | Notes |
|---|---|---|
| `git status --short` | Pass | Found pre-existing dirty `crates/icydb-core/src/db/session/tests/branch_set.rs` |
| `pwd` | Pass | `/home/adam/projects/icydb` |
| `find . -maxdepth 3 -type f \| sort` | Pass | Repository file discovery |
| `rg --files -g 'AGENTS.md' -g 'docs/**' -g 'audits/**' -g 'reports/**' -g 'TODO*' -g 'CHANGELOG*'` | Pass | Existing docs/audits discovery |
| `git rev-parse HEAD` | Pass | `54ea26674652399a57b5282a0b7117886cffb7e1` |
| `git status --short` | Pass | Dirty file unchanged |
| `cargo metadata --format-version 1` | Pass | Output large/truncated by terminal |
| `cargo tree --workspace --all-features` | Pass | Output large/truncated by terminal |
| `rustc --version` | Pass | `rustc 1.96.0` |
| `cargo --version` | Pass | `cargo 1.96.0` |
| `sqlite3 --version` | Fail | `sqlite3: command not found` |
| `cargo metadata --format-version 1 --no-deps` | Pass | Workspace/package/feature inspection |
| `cargo tree --workspace --all-features --depth 1` | Pass | Dependency overview |
| `uname -a` | Pass | OS metadata |
| `lscpu` | Pass | CPU metadata |
| `rg --files -g 'Cargo.toml'` | Pass | Cargo manifest inventory |
| `rg '^\[features\]\|^[a-zA-Z0-9_-]+ = \[' Cargo.toml crates/*/Cargo.toml testing/*/Cargo.toml canisters/*/*/Cargo.toml schema/*/*/Cargo.toml` | Pass | Feature grep |
| `find crates -maxdepth 4 -type f \| sort` | Pass | Crate layout |
| `find canisters testing schema -maxdepth 4 -type f \| sort` | Pass | Canister/test/schema layout |
| `rg -n "TODO\|FIXME\|HACK\|XXX" crates testing canisters schema docs --glob '!target/**'` | Pass | TODO/FIXME sweep |
| `rg -n "\bunsafe\b\|panic!\(\|unwrap\(\|expect\(" crates testing canisters schema --glob '!target/**'` | Pass | Panic/unwrap/unsafe sweep |
| `rg -n "clone\(\|collect::<\|\.collect\(\)\|HashMap\|BTreeMap\|Box<\|dyn " crates/icydb-core/src crates/icydb/src crates/icydb-schema-derive/src crates/icydb-build/src --glob '!target/**'` | Pass | Allocation/static perf sweep |
| `rg -n "stable memory\|StableBTreeMap\|journal\|commit marker\|query\|index\|cache\|serialize\|deserialize\|candid\|serde\|macro\|observer\|metric\|plan-cache" crates docs testing --glob '!target/**'` | Pass | Storage/query keyword sweep |
| `find crates/icydb-core/src/db -maxdepth 3 -type d \| sort` | Pass | Core module map |
| `find crates/icydb-core/src/db -maxdepth 4 -type f \| sort` | Pass | Core file map |
| `rg --count-matches "\bunsafe\b\|panic!\(\|unwrap\(\|expect\(" crates/icydb-core/src crates/icydb/src crates/icydb-build/src crates/icydb-schema-derive/src --glob '!target/**'` | Pass | Counted risky constructs |
| `rg --count-matches "clone\(\|collect::<\|\.collect\(\)\|HashMap\|BTreeMap\|Box<\|dyn " crates/icydb-core/src crates/icydb/src crates/icydb-build/src crates/icydb-schema-derive/src --glob '!target/**'` | Pass | Counted allocation constructs |
| `rg --count-matches "TODO\|FIXME\|HACK\|XXX" crates testing canisters schema docs --glob '!target/**'` | Pass | Counted TODO/FIXME |
| `rg --count-matches "proptest\|trybuild\|fuzz\|corrupt\|migration\|upgrade\|roundtrip\|differential\|SQLite\|sqlite" crates testing canisters schema docs --glob '!target/**'` | Pass | Test/fuzz/diff sweep |
| `sed -n '1,260p' docs/design/0.192-mega-audit-3/audit-results.md` | Pass | Prior audit |
| `sed -n '1,260p' docs/design/archive/0.189-mega-audit-2/audit-results.md` | Pass | Prior audit |
| `sed -n '1,260p' docs/design/archive/0.184-query-engine-audit/findings.md` | Pass | Prior audit |
| `sed -n '1,240p' docs/design/archive/0.184-query-engine-audit/status.md` | Pass | Prior audit status |
| `sed -n '1,260p' docs/audits/reports/2026-04/2026-04-30/sql-perf-audit.md` | Pass | Prior perf report |
| `sed -n '1,220p' docs/audits/reports/2026-06/2026-06-05/summary.md` | Pass | Recent audit summary |
| `sed -n '1,260p' docs/contracts/DURABILITY.md` | Pass | Durability contract |
| `sed -n '1,260p' docs/contracts/ATOMICITY.md` | Pass | Atomicity contract |
| `sed -n '260,620p' docs/design/0.192-mega-audit-3/audit-results.md` | Pass | Prior audit remainder |
| `sed -n '1,260p' docs/contracts/QUERY_CONTRACT.md` | Pass | Query contract |
| `sed -n '1,280p' docs/contracts/SQL_SUBSET.md` | Pass | SQL contract |
| `sed -n '1,260p' docs/contracts/PERSISTED_FORMAT_POLICY.md` | Pass | Persisted policy |
| `sed -n '1,260p' docs/contracts/PERSISTED_FORMAT_INVENTORY.md` | Pass | Format inventory |
| `sed -n '1,260p' docs/1.0-TODO.md` | Pass | Roadmap/TODO |
| `sed -n '1,260p' docs/ROADMAP.md` | Pass | Roadmap |
| `sed -n '1,280p' docs/contracts/READ_ADMISSION.md` | Pass | Read admission |
| `sed -n '1,260p' docs/contracts/RESOURCE_MODEL.md` | Pass | Resource model |
| `sed -n '1,260p' docs/contracts/QUERY_PRACTICE.md` | Pass | Query practice |
| `sed -n '1,260p' docs/contracts/REF_INTEGRITY.md` | Pass | Reference integrity |
| `sed -n '1,260p' docs/contracts/TRANSACTION_SEMANTICS.md` | Pass | Transaction semantics |
| `sed -n '1,260p' docs/contracts/CURSOR.md` | Pass | Cursor contract |
| `sed -n '1,260p' docs/operations/DURABILITY_GUIDE.md` | Pass | Operator durability guide |
| `sed -n '1,260p' docs/governance/fast-path-inventory.md` | Pass | Fast-path inventory |
| `cargo fmt --check` | Pass | Formatting validation |
| `cargo test --workspace --all-features` | Pass | Full workspace tests |
| `cargo clippy --workspace --all-features --all-targets -- -D warnings` | Pass | Clippy validation |
| `find . -path '*/benches/*' -type f \| sort` | Pass | Mostly dependency/cache benches |
| `rg -n "\[\[bench\]\]\|criterion\|cargo bench\|bench_" Cargo.toml crates testing canisters schema docs Makefile` | Pass | No project cargo bench target found |
| `find . -path './.cache' -prune -o -path './target' -prune -o -path './.git' -prune -o -path '*/benches/*' -type f -print \| sort` | Pass | No project benches after pruning |
| `rg -n "execution_hot_path_benchmark_prints_runtime\|manual PocketIC perf report\|cargo bench\|\[\[bench\]\]\|criterion" crates testing canisters schema docs Makefile Cargo.toml` | Pass | Benchmark/test harness discovery |
| `nl -ba crates/icydb-core/src/db/data/structural_field/value_storage/decode/value.rs \| sed -n '440,520p'` | Pass | Decode TODO evidence |
| `nl -ba docs/contracts/CURSOR.md \| sed -n '1,180p'` | Pass | Cursor evidence |
| `nl -ba docs/contracts/QUERY_CONTRACT.md \| sed -n '1,220p'` | Pass | Query contract evidence |
| `nl -ba docs/governance/fast-path-inventory.md \| sed -n '1,240p'` | Pass | Fast-path evidence |
| `nl -ba crates/icydb-core/src/db/query/plan/covering/mod.rs \| sed -n '1,260p'` | Pass | Covering plan evidence |
| `nl -ba crates/icydb-core/src/db/query/plan/planner/index_select.rs \| sed -n '1,260p'` | Pass | Index selection evidence |
| `nl -ba crates/icydb-core/src/db/query/plan/planner/ranking.rs \| sed -n '1,260p'` | Pass | Ranking evidence |
| `nl -ba crates/icydb-core/src/db/executor/index_prefix_cardinality.rs \| sed -n '1,260p'` | Pass | Cardinality execution evidence |
| `nl -ba crates/icydb-core/src/db/index/store.rs \| sed -n '1,280p'` | Pass | Index store evidence |
| `nl -ba crates/icydb-core/src/db/data/store.rs \| sed -n '1,280p'` | Pass | Data store evidence |
| `nl -ba crates/icydb-core/src/db/session/query/cache.rs \| sed -n '1,260p'` | Pass | Query cache evidence |
| `nl -ba crates/icydb-core/src/db/session/sql/cache.rs \| sed -n '1,280p'` | Pass | SQL cache evidence |
| `nl -ba crates/icydb-core/src/db/session/bounded_cache.rs \| sed -n '1,220p'` | Pass | Cache policy evidence |
| `nl -ba crates/icydb-core/src/db/index/cardinality.rs \| sed -n '1,300p'` | Pass | Cardinality metadata evidence |
| `nl -ba docs/contracts/DURABILITY.md \| sed -n '1,240p'` | Pass | Durability lines |
| `nl -ba docs/contracts/ATOMICITY.md \| sed -n '1,220p'` | Pass | Atomicity lines |
| `nl -ba docs/contracts/ATOMICITY.md \| sed -n '219,360p'` | Pass | Atomicity invariants |
| `nl -ba docs/contracts/READ_ADMISSION.md \| sed -n '1,260p'` | Pass | Read admission lines |
| `nl -ba docs/contracts/PERSISTED_FORMAT_INVENTORY.md \| sed -n '1,220p'` | Pass | Format inventory lines |
| `nl -ba docs/contracts/PERSISTED_FORMAT_POLICY.md \| sed -n '1,220p'` | Pass | Format policy lines |
| `nl -ba crates/icydb-core/src/db/commit/recovery.rs \| sed -n '1,300p'` | Pass | Recovery evidence |
| `nl -ba crates/icydb-core/src/db/commit/marker.rs \| sed -n '1,260p'` | Pass | Commit marker evidence |
| `nl -ba crates/icydb-core/src/db/journal/store.rs \| sed -n '1,300p'` | Pass | Journal evidence |
| `nl -ba crates/icydb-core/src/db/diagnostics/storage_report.rs \| sed -n '1,260p'` | Pass | Storage report evidence |
| `nl -ba crates/icydb-core/src/db/diagnostics/storage_report.rs \| sed -n '260,620p'` | Pass | Storage report evidence |
| `nl -ba crates/icydb-core/src/db/diagnostics/integrity.rs \| sed -n '1,300p'` | Pass | Integrity evidence |
| `nl -ba crates/icydb-core/src/db/diagnostics/mod.rs \| sed -n '1,240p'` | Pass | Diagnostics surface |
| `nl -ba crates/icydb-core/src/metrics/state.rs \| sed -n '1,300p'` | Pass | Metrics evidence |
| `nl -ba crates/icydb-core/src/db/diagnostics/model.rs \| sed -n '1,320p'` | Pass | Diagnostic model |
| `nl -ba docs/1.0-TODO.md \| sed -n '1,260p'` | Pass | TODO evidence |
| `nl -ba docs/ROADMAP.md \| sed -n '1,260p'` | Pass | Roadmap evidence |
| `nl -ba docs/design/ideas/sql-fuzzer.md \| sed -n '1,260p'` | Pass | Differential/fuzzer idea |
| `nl -ba docs/audits/reports/2026-04/2026-04-30/sql-perf-audit.md \| sed -n '1,240p'` | Pass | Prior perf report |
| `nl -ba testing/integration/target/perf-hotspots/sql_perf_195_1_liveness_full_matrix.md \| sed -n '1,220p'` | Pass | Perf hotspot evidence |
| `nl -ba testing/integration/target/perf-hotspots/sql_perf_195_1_liveness_vs_195_0_full_matrix_summary.md \| sed -n '1,220p'` | Fail | File did not exist; actual summary path differs |
| `nl -ba artifacts/perf-audit/sql_perf_deterministic_matrix_current.md \| sed -n '1,220p'` | Pass | Older perf artifact |
| `find testing/integration/target/perf-hotspots -maxdepth 1 -type f \| sort` | Pass | Perf artifact inventory |
| `nl -ba testing/integration/target/perf-hotspots/sql_perf_195_1_liveness_vs_195_0_summary.md \| sed -n '1,220p'` | Pass | Correct summary artifact |
| `nl -ba testing/integration/target/perf-hotspots/sql_perf_195_0_vs_194_15_summary.md \| sed -n '1,220p'` | Pass | Correct summary artifact |
| `nl -ba testing/integration/tests/sql_perf_matrix_audit.rs \| sed -n '1,260p'` | Pass | Perf harness evidence |
| `nl -ba crates/icydb-core/src/db/session/tests/execution_hot_path_bench.rs \| sed -n '1,260p'` | Pass | Native ignored bench evidence |
| `nl -ba crates/icydb/src/lib.rs \| sed -n '1,260p'` | Pass | Public facade |
| `nl -ba crates/icydb-core/src/lib.rs \| sed -n '1,260p'` | Pass | Core facade |
| `nl -ba crates/icydb-core/src/db/mod.rs \| sed -n '1,260p'` | Pass | DB public surface |
| `nl -ba crates/icydb-core/src/db/query/mod.rs \| sed -n '1,260p'` | Pass | Query public surface |
| `nl -ba crates/icydb-schema-derive/src/lib.rs \| sed -n '1,240p'` | Pass | Macro surface |
| `nl -ba crates/icydb-schema-derive/src/imp/entity.rs \| sed -n '1,260p'` | Pass | Entity derive evidence |
| `nl -ba crates/icydb-build/src/lib.rs \| sed -n '1,240p'` | Pass | Build/codegen surface |
| `nl -ba crates/icydb-build/src/db/store.rs \| sed -n '1,260p'` | Pass | Generated store wiring |
| `find crates/icydb-core/src/db/schema -maxdepth 3 -type f \| sort` | Pass | Schema file map |
| `nl -ba crates/icydb-core/src/db/schema/snapshot.rs \| sed -n '1,260p'` | Pass | Schema snapshot evidence |
| `nl -ba crates/icydb-core/src/db/schema/mutation/mod.rs \| sed -n '1,260p'` | Pass | Schema mutation contract |
| `nl -ba crates/icydb-core/src/db/schema/reconcile.rs \| sed -n '1,260p'` | Pass | Schema reconcile evidence |
| `nl -ba crates/icydb-core/src/db/schema/reconcile.rs \| sed -n '260,560p'` | Pass | Schema reconcile evidence |
| `nl -ba crates/icydb-core/src/db/schema/mutation/execution.rs \| sed -n '1,300p'` | Pass | Mutation execution evidence |
| `nl -ba crates/icydb-core/src/db/schema/mutation/runner.rs \| sed -n '1,300p'` | Pass | Mutation runner evidence |
| `nl -ba crates/icydb-core/src/db/schema/mutation/delta.rs \| sed -n '1,260p'` | Pass | Mutation delta evidence |
| `jq empty docs/audits/icydb-sqlite-comparison-findings-2026-07-03.json` | Pass | Initial JSON validation before relocation |
| `git status --short` | Pass | Initial artifact status after creation |
| `git diff --stat` | Pass | No tracked diff because artifacts were untracked |
| `wc -l docs/audits/icydb-sqlite-comparison-audit-2026-07-03.md docs/audits/icydb-sqlite-comparison-findings-2026-07-03.json` | Pass | Initial artifact line counts |
| `git status --short --untracked-files=all` | Pass | Initial full untracked artifact status |
| `git diff --stat -- docs/audits/icydb-sqlite-comparison-audit-2026-07-03.md docs/audits/icydb-sqlite-comparison-findings-2026-07-03.json` | Pass | No tracked diff because artifacts were untracked |
| `git diff -- crates/icydb-core/src/db/session/tests/branch_set.rs` | Pass | Verified no diff remained visible for earlier dirty-file check |
| `git status --short --untracked-files=all` | Pass | Relocation pre-check |
| `find docs/design -maxdepth 2 -type f \| sort` | Pass | Design directory naming check |
| `ls docs/audits` | Pass | Source artifact location check |
| `mkdir -p docs/design/0.196-sqlite-comparison-audit` | Pass | Created target design directory |
| `mv docs/audits/icydb-sqlite-comparison-audit-2026-07-03.md docs/design/0.196-sqlite-comparison-audit/audit-results.md` | Pass | Moved markdown audit |
| `mv docs/audits/icydb-sqlite-comparison-findings-2026-07-03.json docs/design/0.196-sqlite-comparison-audit/findings.json` | Pass | Moved machine-readable findings |
| `tail -80 docs/design/0.196-sqlite-comparison-audit/audit-results.md` | Pass | Checked report tail before command-log update |
| `rg -n "docs/audits\|Docs/audit\|Validation and complexity\|Commands run\|jq empty\|git status --short --untracked\|wc -l" docs/design/0.196-sqlite-comparison-audit/audit-results.md` | Pass | Found relocation-sensitive report text |

## Validation and complexity delta

- `cargo fmt --check`: passed.
- `cargo test --workspace --all-features`: passed.
- `cargo clippy --workspace --all-features --all-targets -- -D warnings`: passed.
- `jq empty docs/audits/icydb-sqlite-comparison-findings-2026-07-03.json`: passed before relocation.
- `cargo bench`: skipped; no project Cargo bench target found.
- SQLite benchmark execution: skipped; `sqlite3` is not installed.
- Production code files touched: 0.
- Design audit files currently added: 2.
- Implementation complexity delta: neutral; this audit did not change engine behavior.
- New performance/wasm-size deltas from this audit: none measured.

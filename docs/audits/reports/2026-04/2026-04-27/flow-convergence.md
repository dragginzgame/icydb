# Flow Convergence Audit

Date: 2026-04-27

Recurring definition:
[/home/adam/projects/icydb/docs/audits/recurring/crosscutting/crosscutting-flow-convergence.md](/home/adam/projects/icydb/docs/audits/recurring/crosscutting/crosscutting-flow-convergence.md)

Evidence artifacts:
[/home/adam/projects/icydb/docs/audits/reports/2026-04/2026-04-27/artifacts/flow-convergence](/home/adam/projects/icydb/docs/audits/reports/2026-04/2026-04-27/artifacts/flow-convergence)

## Summary

Overall convergence risk score: 5/10.

SQL and Fluent convergence is acceptable. The live query paths converge through
shared prepared query plans, executor-owned scalar/grouped runtime stages, and
session-owned response finalization. The highest remaining risk is not a
semantic split between SQL and Fluent; it is smaller policy and adapter residue
inside prepared terminal dispatch, grouped generic ingest, and SQL aggregate
adapter shaping.

Top three risks:
- `HotPathBranching`: generic grouped bundle ingest still checks borrowed-probe
  support inside the per-row group-index resolution path.
- `OwnershipBlur`: fluent terminal session adapters carry several similar
  strategy-to-boundary-to-output match ladders.
- `ConversionChurn`: SQL grouped/global aggregate adapters still clone prepared
  logical plans and projection/query payloads at session handoff boundaries.

## Query Flow Map

SQL path:
SQL text enters the session SQL cache/compile boundary, lowers through
`db::sql::lowering`, then either executes as scalar/grouped structural query,
SQL global aggregate, delete, update, or explain. Scalar and grouped SQL now
reuse shared prepared query-plan cache identities where possible.

Fluent path:
Typed query builders produce `Query<E>` values, cache through
`DbSession::cached_prepared_query_plan_for_entity`, and execute via the same
prepared scalar/grouped executor boundaries used by adjacent structural paths.
Fluent terminal APIs keep public typed output shaping at the session/query
boundary.

Prepared path:
Prepared execution centers on `PreparedExecutionPlan` and
`SharedPreparedExecutionPlan`. SQL and typed surfaces share plan-cache storage,
with typed surfaces taking typed clones and SQL structural surfaces consuming
shared plan payloads.

Executor path:
Scalar execution converges through `executor::pipeline::entrypoints::scalar`.
Grouped execution converges through `executor::pipeline::entrypoints::grouped`
and `executor::aggregate::runtime::grouped_fold`. The recent grouped-fold split
makes the dedicated grouped `COUNT(*)`, generic grouped reducer, metrics,
dispatch, hashing, equality, and boundary owners explicit.

Projection/finalization path:
Executor projection owns runtime row shaping. Session response finalizers encode
cursor bytes and attach traces. SQL projection payload construction remains
session-owned because SQL owns labels, fixed scales, and statement envelope
formatting.

## Duplicate Flow Findings

### FC-1: Fluent terminal adapter ladders remain broad

Classification: `OwnershipBlur`

Risk: 5/10

Files:
- [/home/adam/projects/icydb/crates/icydb-core/src/db/session/query.rs:683](/home/adam/projects/icydb/crates/icydb-core/src/db/session/query.rs:683)
- [/home/adam/projects/icydb/crates/icydb-core/src/db/session/query.rs:709](/home/adam/projects/icydb/crates/icydb-core/src/db/session/query.rs:709)
- [/home/adam/projects/icydb/crates/icydb-core/src/db/session/query.rs:734](/home/adam/projects/icydb/crates/icydb-core/src/db/session/query.rs:734)
- [/home/adam/projects/icydb/crates/icydb-core/src/db/session/query.rs:783](/home/adam/projects/icydb/crates/icydb-core/src/db/session/query.rs:783)
- [/home/adam/projects/icydb/crates/icydb-core/src/db/session/query.rs:810](/home/adam/projects/icydb/crates/icydb-core/src/db/session/query.rs:810)

Responsibility duplicated:
Each terminal family repeats the same shape: convert prepared fluent strategy to
executor boundary request, execute the boundary, convert executor output into a
fluent output DTO, and map executor errors back into query errors.

Recommended owner:
Keep public fluent DTO shaping in `session/query`, but move the mechanical
strategy-to-boundary mappings onto the prepared strategy enums or a small
query-owned terminal adapter. This should be a mechanical cleanup only.

### FC-2: SQL grouped execution has normal and diagnostics envelopes

Classification: `LateConvergence`

Risk: 4/10

Files:
- [/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/execute/mod.rs:102](/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/execute/mod.rs:102)
- [/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/execute/mod.rs:127](/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/execute/mod.rs:127)

Responsibility duplicated:
Both helpers split projection metadata, clone the prepared logical plan, execute
the grouped path, and finalize the SQL statement result. The diagnostics variant
only needs the response-finalization timing split.

Recommended owner:
Keep diagnostics timing, but extract the common grouped SQL prepared-plan
handoff into one helper that accepts an optional response-finalization measurer.
Avoid changing cursor, projection, or metrics behavior.

### FC-3: SQL global aggregate still maps SQL strategy to executor terminal in session

Classification: `ConversionChurn`

Risk: 5/10

Files:
- [/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/execute/global_aggregate.rs:32](/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/execute/global_aggregate.rs:32)
- [/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/execute/global_aggregate.rs:84](/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/execute/global_aggregate.rs:84)

Responsibility duplicated:
The session adapter converts the lowered SQL aggregate strategy into executor
structural aggregate terminals and also shapes the SQL projection result. This
is much cleaner than the older terminal explosion, but the runtime-descriptor
match remains a session-layer bridge.

Recommended owner:
Let the lowered aggregate strategy construct or borrow an executor structural
terminal descriptor directly, leaving session to own only labels, fixed scales,
cache attribution, and SQL statement result construction.

### FC-4: Grouped next-cursor boundary shaping exists in two grouped finalizers

Classification: `DuplicateFlow`

Risk: 3/10

Files:
- [/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/count/window.rs:330](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/count/window.rs:330)
- [/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/page_finalize.rs:626](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/page_finalize.rs:626)

Responsibility duplicated:
Both grouped-count and generic grouped finalization derive a cursor boundary by
copying the last emitted grouped key before constructing the next grouped cursor.

Recommended owner:
If this grows, move the "last emitted grouped key to next cursor boundary" helper
into grouped finalization ownership. Do not merge grouped-count and generic
candidate selection; they are deliberately specialized.

## Shim and Compatibility Residue

### SR-1: Scalar and grouped response finalizers are similar but legitimate

Classification: `LegitimateSeparation`

Risk: 2/10

Files:
- [/home/adam/projects/icydb/crates/icydb-core/src/db/session/response/scalar.rs:19](/home/adam/projects/icydb/crates/icydb-core/src/db/session/response/scalar.rs:19)
- [/home/adam/projects/icydb/crates/icydb-core/src/db/session/response/grouped.rs:17](/home/adam/projects/icydb/crates/icydb-core/src/db/session/response/grouped.rs:17)

Why it should remain:
Both encode cursor payloads and attach traces, but they validate different
cursor families and produce different public response DTOs. A generic helper
would obscure the family-specific invariant.

### SR-2: Structural covering and aggregate wrappers still carry boundary value

Classification: `LegitimateSeparation`

Risk: 2/10

Evidence:
The shim signal scan still finds wrappers and adapters, but the high-signal
production ones are mostly structural transport boundaries between executor and
session/facade layers. These should not be deleted without caller and DTO
coverage proving they are wrapper-only.

## Hot Path Branch/Conversion Findings

### HP-1: Generic grouped bundle still branches on borrowed-probe support per row

Classification: `HotPathBranching`

Risk: 6/10

Files:
- [/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/bundle.rs:154](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/bundle.rs:154)
- [/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/bundle.rs:179](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/bundle.rs:179)
- [/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/bundle.rs:401](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/bundle.rs:401)

Why it exists:
`GroupedBundleIngestPolicy` freezes `borrowed_group_probe_supported`, but
`borrowed_group_hash` still checks that flag during each row's group-index
resolution.

Recommended owner:
Resolve generic grouped ingest mode before the loop, mirroring the recent
dedicated grouped-count cleanup. Prefer two concrete row-loop helpers or one
statically selected function path. Do not introduce trait objects or boxed
strategy dispatch in the row loop.

### HP-2: SQL global aggregate adapter clones query/projection payloads at handoff

Classification: `ConversionChurn`

Risk: 4/10

Files:
- [/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/execute/global_aggregate.rs:84](/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/execute/global_aggregate.rs:84)
- [/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/execute/global_aggregate.rs:92](/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/execute/global_aggregate.rs:92)
- [/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/execute/global_aggregate.rs:97](/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/execute/global_aggregate.rs:97)

Why it exists:
The command owns lowered SQL aggregate metadata, while the executor request
needs owned projection/query structures.

Recommended owner:
When touching this next, add borrow-first constructors or consume the lowered
command once at execution handoff. Keep SQL labels and statement output in the
session SQL layer.

## Legitimate Separations

### LS-1: Dedicated grouped `COUNT(*)` path vs generic grouped reducer

Files:
- [/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/count/mod.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/count/mod.rs)
- [/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/generic/runner.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/generic/runner.rs)

Reason:
Grouped count uses a single count map and specialized windowing. The generic
path owns aggregate-state bundles. They should stay separate for performance.

### LS-2: SQL and Fluent public APIs

Reason:
The audit target is internal convergence. SQL text compilation and typed fluent
builders should remain separate public construction APIs while converging at
prepared plan and executor boundaries.

### LS-3: Scalar vs grouped cursor validation

Reason:
Scalar and grouped cursors have different token families and invariants. The
response-layer helpers are similar but intentionally validate different cursor
families.

## Recommended Patch Plan

1. Mechanical cleanup first:
   - collapse common grouped SQL prepared-plan handoff between normal and
     diagnostics helpers
   - move SQL global aggregate strategy-to-terminal construction out of the
     session adapter if it can be done without extra clones
   - factor cursor-boundary construction only if another grouped finalizer needs
     the same behavior

2. Semantic convergence second:
   - reduce fluent terminal adapter ladders by moving pure request mappings onto
     prepared terminal strategy owners
   - keep output DTO shaping in `session/query`

3. Performance rewrites last:
   - split generic grouped borrowed vs owned ingest before the row loop
   - verify no new allocations, dynamic dispatch, or branch movement into hot
     row paths

## Validation Plan

Targeted tests:
- `cargo test -p icydb-core grouped -- --nocapture`
- `cargo test -p icydb-core sql -- --nocapture`
- `cargo test -p icydb-core execution_convergence -- --nocapture`

Invariant and lint checks:
- `cargo fmt --all`
- `cargo check -p icydb-core --all-targets`
- `cargo clippy -p icydb-core --all-targets -- -D warnings`
- `make check-invariants`
- `git diff --check`

Grep checks:
- `rg "dyn .*Probe|Box<.*Probe|GroupProbeStrategy" crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold`
- `rg "borrowed_group_probe_supported" crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold`
- `rg "execute_grouped_sql_statement_from_prepared_plan_with" crates/icydb-core/src/db/session/sql/execute`

## Evidence Summary

Captured artifacts:
- `sql-signals.txt`: SQL signal scan
- `fluent-signals.txt`: fluent signal scan
- `prepared-signals.txt`: prepared-plan signal scan
- `shim-signals.txt`: compatibility, fallback, adapter, and wrapper signal scan
- `conversion-signals.txt`: clone/to_vec/to_string signal scan
- `executor-flow-signals.txt`: executor stage, finalize, projection, cursor, and continuation scan
- `module-sizes-top40.txt`: largest db Rust modules
- `evidence-line-counts.txt`: artifact line counts

The evidence scan produced 16,572 lines across the text artifacts. The largest
production modules in the top-40 size scan are still executor/session/query
planning and projection owners rather than the newly split grouped fold module.

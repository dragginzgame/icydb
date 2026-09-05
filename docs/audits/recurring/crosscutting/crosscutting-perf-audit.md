# Audit: On-Canister Entity Cost

## Purpose

Identify which accepted entities are used most often and account for the most
local instruction work in deployed canisters. Endpoint-level narrowing belongs
to Canic.

This audit is intentionally small. It does not define an off-canister metrics
pipeline, retained event stream, SQL phase model, query-shape identity, cache
attribution format, or report artifact.

## Authoritative Surface

Apply [Authorization And Read-Only Work](../../README.md#authorization-and-read-only-work).
Inspection uses the existing metrics window and available endpoint evidence.
Resetting the window, generating measurement traffic, rebuilding, or deploying
requires that action to be within the requested measurement work. The presence
of a metrics endpoint alone does not authorize changing canister state.

Build the canister with the single `metrics` feature. Generated endpoints own
the complete observation surface:

- `icydb_metrics()` returns the current heap-only window;
- `icydb_metrics_reset()` starts a new window; and
- Canic supplies endpoint-level instruction evidence independently.

Each report row contains exactly:

- accepted entity path;
- observed execution-span count;
- total attributed local instructions; and
- maximum attributed local instructions for one span.

Rows are ordered by total instructions, then hits, then path. The report is
read-only and is not persisted across reinstall, upgrade heap replacement, or
explicit reset.

## Interpretation

Use `instructions_total` to find the entities consuming the most aggregate
work. Compare `hits` with `instructions_total` and `instructions_max` to
separate frequent inexpensive access from less frequent expensive access.
Use Canic to correlate an expensive entity with the endpoint that drove it.

The entity span deliberately avoids writes during replicated query execution,
because query heap mutations are discarded. Collect durable windows from
updates and other replicated execution. Treat query-only observations as
endpoint evidence in Canic.

## Focused Development Probes

The dedicated SQL audit canister may expose total-only instruction samples for
focused local regression checks. Those samples verify concrete operations in
one IC message; they are not a second metrics product and must not grow phase,
store-counter, cache, sink, event, baseline-matrix, or report-retention models.

When a code change needs a before/after cost check:

1. use the same canister, fixture state, method, SQL, and runtime version;
2. record the complete local-instruction delta only;
3. keep correctness and bounded-work assertions alongside the sample; and
4. discard or archive the result as review evidence rather than adding runtime
   state or a maintained off-canister format.

## Wasm And Complexity Gate

For instrumentation changes, report final raw non-gzipped Wasm bytes first;
gzip is secondary context. Also report files touched, approximate line delta,
and whether the implementation became simpler, stayed neutral, or became more
complex.

Reject additions that introduce another feature flag, report mode, persisted
state, execution route, attribution DTO, compatibility path, or independent
aggregation owner without a separately demonstrated product need.

## Safety Boundaries

Performance evidence never authorizes weakening accepted-schema authority,
execution budgets, deterministic ordering, typed failures, storage integrity,
or `EXPLAIN` fidelity. Optimize or delete duplicated observation machinery;
do not classify removed safety work as a performance improvement.

# 0.187 Status

Status: active audit.

## Focus

Second query-engine duplicate-authority audit after the 0.184 query-engine
cleanup, the 0.185 branch-aware routing revisit, and the 0.186 shared filter
authority work.

0.187 starts as an audit and source-map line. Runtime code should not change
until a suspected duplicate flow has evidence, a classification, and either a
parity test or a source invariant.

## Current Slice

- Guard duplicate-looking query-engine splits so they cannot drift onto
  separate semantic or execution authorities.
- Keep runtime behavior unchanged unless a guarded duplicate flow has a
  mechanical consolidation path.

## Input Readiness

| Required Input | Status | Notes |
| --- | --- | --- |
| 0.184 closeout status | Read | 0.184 is closed after 0.184.50 and should stay in guard mode unless this audit finds a concrete duplicate-flow deletion. |
| 0.185 branch-aware status | Read | 0.185 is closed after 0.185.22; branch families remain intentionally distinct unless a future route admits broader branch merging semantics. |
| 0.186 shared-filter decision | Read | `NormalizedFilter` is the pre-access authority baseline; downstream cache, route, EXPLAIN, and count/cardinality consumers are guarded from deriving frontend predicate facts directly. |
| SQL/fluent perf matrix reports | Needed before optimization claims | Existing focused perf evidence exists from earlier lines, but 0.187 should not make performance claims without a fresh targeted run. |
| EXPLAIN/diagnostics snapshots | Available as test coverage | No broad snapshot rerun has happened in this slice. |
| Source-invariant script results | Available as tests | 0.186 added the current filter-authority guards; broader 0.187 guards should be added only after a finding is classified. |
| Generated canister matrix results | Passed | SQL canister matrix passed on 2026-06-27: 76 tests, 0 failed. |

## Completed In Current Audit

- Built the active 0.187 source map and findings table.
- Added a source invariant proving the direct SQL COUNT prefix-cardinality path
  and prepared aggregate COUNT/EXISTS preflight both converge on the shared
  planner prefix-cardinality proof and metadata terminal execution helpers.
- Classified the direct SQL accepted-authority prefix builder as a deliberate
  pre-plan shortcut, not a separate store-cardinality execution authority.
- Added a source invariant proving SQL UPDATE/INSERT staged-row bounds stay on
  the shared `SqlWriteMutationExecution` boundary while SQL DELETE bounds stay
  on the delete projection/count post-access boundary before commit.
- Classified DELETE bound short-circuiting as a future post-access collector
  design item, because raw key-stream short-circuiting would be unsafe when
  residual filtering can still remove candidate rows.
- Added a source invariant proving materialized scalar pages, retained-slot
  pages, and aggregate kernel-row sinks all enter the shared
  `execute_prepared_scalar_kernel` / `ExecutionInputs` spine.
- Classified covering projection and aggregate projection fast paths as
  terminal payload specializations rather than duplicate scalar route
  preparation.
- Rechecked the global/grouped aggregate-family item against the 0.184 shared
  aggregate operator note and kept the first-class aggregate DTO deferred until
  it deletes a real duplicate consumer or becomes a shared runtime/EXPLAIN/cache
  handoff.
- Built a source-only lint-suppression inventory for query/executor/db
  surfaces. The production hits are mostly intentional Clippy shape/style
  fences; no obvious duplicate-authority cleanup was found. Treat stale
  suppression removal as a dedicated hygiene pass validated by Clippy, not a
  0.187.0 blocker.
- Ran the generated SQL canister matrix and validated the generated endpoint
  surface against the current 0.187 audit baseline: 76 passed, 0 failed.
- Removed several remaining panic-shaped runtime invariants from query
  expression preview/evaluation, predicate bridge conversion, grouped
  EXPLAIN/fingerprint projection, grouped strategy selection, resolved ORDER
  handling, and SQL write primary-key normalization.
- Replaced the finalized static-execution-planning contract `.expect(...)`
  accessor with optional and fallible accessors. Required projection and
  primary-key metadata now returns `InternalError`; fast-path eligibility and
  bytes/projection diagnostics fail closed when the finalized contract is
  absent. A source guard now locks this accessor shape.
- Made SQL UPDATE/DELETE policy validated-plan helpers recoverable. Generated
  exposure policies now stay on typed rejection paths, bounded write proof
  construction is fallible when a limit is absent, and a source guard prevents
  the removed `unreachable!` / `.expect(...)` traps from returning.
- Made covering aggregate terminal-value selection fail closed for non-FIRST
  and non-LAST aggregate kinds, with a source guard proving the helper no
  longer traps when validation drift reaches that boundary.

## Initial Queue

- Defer cost/selectivity optimization, cursor redesign, chunked durable
  commits, and aggregate operator DTO work unless the audit produces concrete
  evidence that one is blocking duplicate-authority deletion.

## Rules

- Start with source maps, not refactors.
- Classify every duplicate-looking flow as real duplicate, deliberate
  specialization, diagnostics-only projection, cache/fingerprint identity path,
  or deferred architecture item.
- For real duplicates, add or identify parity coverage before cleanup.
- For performance claims, gather attribution before optimization.
- For broad architecture changes, write a focused design note before code.

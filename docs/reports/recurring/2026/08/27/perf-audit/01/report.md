# 0.246 Query Execution Convergence Baseline

## 0. Run Metadata + Comparability Note

- scope: clean complete SQL P1, scale and P2 measurement plus prepared
  aggregate and scalar `DISTINCT` materialization inspection for 0.246 Patch 1
- definition path:
  `docs/audits/recurring/crosscutting/crosscutting-perf-audit.md`
- compared baseline report path:
  `docs/reports/recurring/2026/08/26/perf-audit/01/report.md`
- code snapshot identifier: tag `v0.245.1`, revision
  `630cc9c974294631ff77a77d69a2704581ccb2dd`, tree
  `4b1c4464679e836bfb22da61b72080db1133b7bd`
- method tag/version: `PERF-0.246-clean-matrix/v1`
- comparability status: exact clean current-source baseline; no historical
  instruction-delta verdict is made because the preceding recurring report was
  a dirty, focused debug run rather than the complete current profile
- auditor: Codex
- run timestamp (UTC): `2026-08-27T16:02:28Z`
- branch: `main`
- worktree: clean detached measurement checkout at the exact released tag
- execution environment: PocketIC 15.0.0, Rust 1.97.1, one shared
  `wasm-release` SQL performance canister
- entities in scope: all six checked-in SQL performance surfaces (`account`,
  `blob`, `heap_user`, `journaled_user`, `token`, and `user`)
- entry surfaces in scope: reduced SQL query execution with cold, proven-warm
  and scale attribution
- query shapes in scope: the complete 1,787-scenario P1 profile, 210 scale
  observations and 521 strict P2 confirmations, plus one focused clean-source
  global scalar `DISTINCT` control

The measurement subject was clean and exact. Its `Cargo.lock` SHA-256 was
`9d709b4c4a9ed51055ac35371f3c7db3aa83a534defa9c90c9c6ef0c96814d99`.
The raw Wasm contained 4,513,444 bytes and had SHA-256
`0070381c8ca1e0f5a94a7da80b2c9235f51595f42a8fbc79ff7764e9ca2c9f95`.
The build used `wasm32-unknown-unknown`, the `wasm-release` Cargo profile and
the `diagnostics`, `sql` and `test-admin-api` features. PocketIC's binary hash
was `29472ea4433b30a280676c4e22e369d79d5ba6ee1b4d48bab32ebe7d0ad2b4bb`.

All eight P1, eight scale and eight P2 receipts passed. P1 observed all 1,787
declared scenarios with zero failures and the expected scenario hash
`a6823a84aa768257b2dc27d166e79c20260c5629eb33c70f590d308c64a1f80b`.
Scale observed all 210 scenarios under hash
`bbd40c3f2633a1d2d38e5c9f4a24afae137aec10de49ec4e1627cc34b19d4c3c`.
P2 confirmed all 521 selected candidates under hash
`899f99560b2ef7e7c0d099d9e7aa4e325c588078831b021ebbebc42a3b3a8d72`.
The strict top-20-per-metric union itself contains 521 members, so the harness
ceiling was corrected from 512 to 544. Selection still fails rather than
truncating, and no profile member or fixture changed.

The complete raw bundle remains local at
`artifacts/perf-audit/0.246-patch1-clean/`; it is not checked in because the
merged reports are large generated measurement artifacts. Exact identities and
the decision-bearing rows are retained here.

## 1. Coverage Table

| Scenario family | Surfaces covered | Missing surfaces | Attribution depth | Risk |
| --- | --- | --- | --- | --- |
| Complete P1 discovery | Six SQL audit surfaces | Typed/fluent | Compile, planner, store, executor, physical work | Low |
| Scale profile | Six SQL audit surfaces at 16, 256 and 2,048 rows | Typed/fluent | 210 totals, 765 normalized costs, 140 slopes | Low |
| P2 confirmation | 521 selected SQL candidates | Typed/fluent | Five cold and five proven-warm samples per candidate | Low |
| Prepared global aggregate | `user` at 16, 256 and 2,048 rows | Other entity types use smaller fixtures | Base-row and reducer phases, physical gets | Low |
| Scalar global `DISTINCT` control | `user`, 2,048 rows | No scale-profile member for this exact expression shape | Total, compile/execute, candidates and physical gets | Medium |

Typed/fluent performance was not sampled. The 0.246 candidate is inside the
shared executor, but no cross-surface performance claim is made until a
candidate exists. Correctness equivalence across prepared and non-prepared
entrypoints remains an implementation gate, not evidence supplied by this
baseline.

## 2. Current Matrix

The table shows the decision-bearing rows rather than reproducing all 1,787
P1 samples.

| Scenario key | Entry surface | Count | Instructions | Notes |
| --- | --- | ---: | ---: | --- |
| `scale.user.not_paginated.aggregate_distinct_filter_all.rows16` | SQL scale | 1 | 1,917,011 | 16 rows and 16 data-store gets |
| `scale.user.not_paginated.aggregate_distinct_filter_all.rows256` | SQL scale | 1 | 14,188,153 | 256 rows and 256 data-store gets |
| `scale.user.not_paginated.aggregate_distinct_filter_all.rows2048` | SQL scale | 1 | 109,234,618 | 2,048 rows and 2,048 data-store gets |
| nullable-expression scalar `DISTINCT` | focused SQL | 1 | 118,738,995 | 2,048 candidates, five unique values, 2,048 gets |
| expression-ordered scalar `DISTINCT` | focused SQL | 1 | 109,375,905 | 2,048 candidates, five unique values, 2,048 gets |
| `journaled_user.select.full_entity.all.age_asc.limit10` | SQL P1 | 1 | 21,690,074 | current highest P1 total; 512 gets |
| `heap_user.select.full_entity.all.age_asc.limit10` | SQL P1 | 1 | 21,645,544 | heap mirror; 512 gets |
| `token.collection_id.sparse_in.page_only.limit50` | SQL P1 | 1 | 19,830,185 | planner and executor each exceed 7.2M |

The focused scalar `DISTINCT` control used the same clean released source and
PocketIC version but a focused debug audit canister, not the shared
`wasm-release` matrix subject. Its absolute value is therefore supporting
localization evidence and is not numerically compared with matrix rows.

## 3. Comparison Highlights

This run closes the preceding report's `PERF-001` gap: the current 210-scenario
scale profile now has one clean, complete P1/scale/P2 bundle. It does not
convert the bundle into a historical regression baseline or claim improvement
against the dirty 0.244 audit.

The important current-source observation is breadth. Prepared global aggregate
work scales almost linearly from 16 to 2,048 rows, and both rejected scalar
`DISTINCT` expression controls perform 2,048 row gets before their consumers
produce at most five unique values. These are two different semantic consumers
of the same retained-row materialization boundary, not two SQL spellings of one
narrow fast path.

## 4. Phase Attribution Read

| Scenario key | Compile | Planner | Store | Executor | Projection/finalize | Notes |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| aggregate distinct/filter, 16 rows | `PARTIAL` | `PARTIAL` | 495,110 | 757,865 base-row | 116,514 reducer | 16 ingested rows |
| aggregate distinct/filter, 256 rows | `PARTIAL` | `PARTIAL` | 8,492,809 | 11,484,387 base-row | 1,653,354 reducer | 256 ingested rows |
| aggregate distinct/filter, 2,048 rows | 628,292 | 288,370 | 71,852,668 | 95,061,223 base-row | 13,127,318 reducer | total 109,234,618; phase owners overlap |
| nullable-expression scalar `DISTINCT` | 506,393 | `PARTIAL` | `PARTIAL` | 118,232,602 execute | `PARTIAL` | 2,048 gets; focused debug method |
| expression-ordered scalar `DISTINCT` | 443,107 | `PARTIAL` | `PARTIAL` | 108,932,798 execute | `PARTIAL` | 2,048 gets; focused debug method |

The aggregate attribution establishes a theoretical upstream ceiling, not an
expected saving. Of 109,234,618 instructions, 13,127,318 belong to required
aggregate reduction and 628,292 to compilation. At most 95,479,008 instructions
remain outside those two buckets. The 71,852,668-instruction store phase cannot
simply disappear: values still have to be visited and decoded. The genuinely
avoidable work is the per-key key encoding/tree lookup and retention of all
2,048 `KernelRow` values before the already row-sink-shaped consumer is called.
The current diagnostics do not isolate that subset in instructions, so Patch 2
must prove its realistic saving rather than treating the ceiling as a forecast.

## 5. Ownership and Materialization Map

| Stage | Current owner | Required semantic work | Candidate-removable work |
| --- | --- | --- | --- |
| Access contract | prepared plan and route plan | selected index/store, residual policy, retry envelope | none; runtime must not reclassify |
| Key/row traversal | scalar execution kernel | missing-row, corruption, borrow and physical-work policy | per-key lookup when an existing borrowed primary visitor can supply the same row |
| Slot decode | scalar row runtime | bounded decode, `NULL`, coercion and expression errors | full-row/unused-slot payload only where retained-slot layout already proves it unnecessary |
| Post-access materialization | scalar page/kernel-row terminal | retry isolation, required ordering, cursor and page window | complete `Vec<KernelRow>` only when the consumer contract proves it does not need it |
| Aggregate projection | prepared aggregate terminal set | consumer-specific inputs and filters | transient row wrapper after values are delivered |
| Aggregate reduction | scalar aggregate reducer runtime | fold, `DISTINCT` set, `NULL`, budget and final type | none; remains aggregate-owned |
| Scalar `DISTINCT` projection | projection materializer | expression tuple, exact identity, ordering, window and output budgets | retained source `KernelRow`s once the canonical projected tuple/order key is owned |

The current aggregate “row sink” is buffered. The scalar streaming entrypoint
calls `materialize_kernel_rows_with_optional_residual_retry`, receives a
`KernelRowsExecutionAttempt` containing `Vec<KernelRow>`, and only then invokes
the aggregate closure over that vector. The projection `DISTINCT` path likewise
executes a retained-slot page before `project_distinct` evaluates and admits
canonical projected rows. Thus the common opportunity is delivery of a
consumer-projected value or tuple from an already authorized row visit. It is
not a common reducer.

The smallest acceptable Patch 2 seam is one executor-owned visitation boundary
parameterized by an existing consumer-owned projector. Aggregate consumers may
fold a transient projected value immediately. Scalar `DISTINCT` consumers may
retain their canonical projected tuple and order key, because global identity,
ordering and pagination require consumer state, but must not retain the source
`KernelRow` merely to obtain them later.

Residual retry is the hard boundary. Reducer or `DISTINCT` side effects from a
probe that is later discarded cannot survive into a widened retry. Direct
delivery is therefore admissible only when the prepared route proves retry is
impossible, or when the existing retry owner supplies an isolated attempt-local
consumer whose state is discarded atomically. The executor, not either
consumer, remains the sole owner of that decision.

Likewise, an authored sort, cursor, or page window may require materialization.
Patch 2 must preserve that existing flow unless it can retain only the complete
canonical projected tuple and order key under the same prepared contract. It
may not silently move ordering policy into a generic aggregate helper.

## 6. Patch 2 Decision

Patch 2 is eligible for one bounded implementation attempt.

The prerequisite is met structurally and across two semantic consumers:

- prepared aggregate: 109,234,618 instructions, 2,048 row gets, 2,048 retained
  rows delivered only after the vector is complete;
- scalar projection `DISTINCT`: 109,375,905–118,738,995 instructions, 2,048
  row gets and no more than five canonical distinct outputs; and
- both enter existing consumer-owned semantics after the common scalar
  retained-row boundary.

This does not pre-approve retention. Patch 2 must measure at least 25M and 25%
savings for each consumer family. Its implementation must share only projected
value delivery; aggregate folding and `DISTINCT` tuple identity, ordering,
pagination and budget charging remain separate. A result that helps only the
aggregate case or only one expression/type is a measured no-build.

## 7. Required Patch 2 Proof Matrix

The implementation handoff must prove all of the following against the frozen
clean subject:

- aggregate `COUNT(*)`, field count, `SUM`, `AVG`, `MIN`, `MAX`, aggregate
  `DISTINCT`, aggregate `FILTER`, expression inputs and mixed terminal sets;
- scalar single-expression and tuple `DISTINCT`, `NULL`, numeric coercion,
  aliases and expression errors;
- ASC/DESC ordering, offset, limit, cursor and deterministic tie behavior;
- residual retry required and impossible, including discarded probe state;
- missing row, orphan index entry, invalid slot, borrow conflict and typed
  corruption errors;
- logical work, retained-state, nested-value and output budget equality;
- prepared/non-prepared result bytes and error identity;
- existing exact aggregate and ordered group-seek routes unchanged;
- full P1, scale and P2 result/cursor signatures unchanged;
- physical work changes limited to the reviewed replacement of per-key gets or
  source-row retention, with no extra row/index visits; and
- raw production SQL Wasm, maintained lines, owners and state axes within the
  frozen design gates.

## 8. Overall Read

Patch 1 passes. It supplies the first clean complete current-profile bundle,
closes the prior coverage finding, and identifies one bounded cross-cutting
implementation seam. The candidate is materially different from another
shape-specific SQL fast path: it changes how already authorized prepared rows
reach existing semantic consumers.

The next action is Patch 2 only. No other future 0.246 candidate is retained.
No production runtime code changed during this audit.

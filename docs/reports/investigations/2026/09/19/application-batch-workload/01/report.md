# Application Batch Qualification — F60

## Outcome

PASS for the bounded fixture: 11 scenarios, 25 measured update calls, independent
readbacks before each scenario and after every measured message. This adds
qualification, not a database optimization.

At 128 items, bulk exact reads followed by one typed atomic write batch cost
236,855,733 cycles versus 414,975,697 for individual reads followed by the same
batch: 42.92% fewer cycles and 43.78% fewer measured instructions. Both use the
existing API and the same actor bytes. These are single samples per scenario,
not a universal guarantee or a newly introduced speedup.

Eight 16-item requests cost 326,895,613 cycles, 38.01% more than the single
128-item bulk request; measured instructions rise 15.08%. Each request is smaller,
but separate chunk commits are not equivalent to one atomic batch. Late domain
validation rejects with zero output rows in the single-request case; rejecting
the last chunk leaves 112 rows from seven previous committed requests.

No request-budget exhaustion occurred in this workload. This does not reproduce
the historical E273 incident, identify a maximum safe batch size, or establish a
missing coordinator. Keep the current APIs; a new coordinator/cache is not
justified here. The write stage is 59.66% of the 128-item bulk body's instructions,
but it includes construction, binding, validation, projection and commit, so this
does not attribute that cost to repeated layout construction.

## Exact inputs and ownership

Base HEAD: `5ccd45d3d24d33460afb2350f3082297da2dc2d0` (`v0.259.6`).
The initially clean worktree adds the F60 fixture, host test and notes. No engine,
schema, Cargo version or dependency changes. The final actor inputs are HEAD plus
the two changed actor files below, not HEAD alone.

SHA-256:

| Input | Hash |
| --- | --- |
| Cargo.lock | `8024973c209ab9244ac24687f3a8d42e772f0c3c4454d7a5f57aeaaa0c34c658` |
| canisters/test/sql/src/lib.rs | `295ac261ef85e5fa8df18a552be6b6c03280efa0ecbe757df63d0959cadfd5b8` |
| canisters/test/sql/src/batch_workload.rs | `ccd796a0f0c6a29a4d4cb96dd6972cb28ad3c5b01738cd5fbebb4a627366f80e` |
| schema/test/sql/src/sql.rs | `7821fe3b48034f04fcae7d9435309587fa38edfe02b7da034235fae2f2e0e681` |
| testing/integration/tests/sql_canister.rs | `c4bf6f438ed8090e4efa49a5e2c264128e5cbc4d869f89eefca2ba0e909e384c` |
| testing/integration/tests/batch_workload/mod.rs | `20e24be819dc4a6604319b0ea8178ad20420c22f0a13bcff163e266692038951` |

Rust 1.98.1 (`48a229cea`), PocketIC 16.0.0, current locked dependencies.
The existing retained Cargo/post-link owner builds `canister_test_sql` using
`wasm-release` (size optimization, fat LTO, one codegen unit, abort panics,
stripped symbols), LocalTest, SQL and Candid export enabled, defaults off.
Features: `candid-export,local-sql-query,test-admin-api`.
The retention handle remains alive while bytes are read, and every scenario
installs a clone of that same byte vector into a fresh disposable instance.

Final raw actor: **4,245,111 bytes**, SHA-256
`fe0d71e418da90205568d56bde00103692590eef521a8574c58c7c14fa6073c1`.
Retained artifact namespace:
`823dde03c7560a3664bd3b412f48ad81c265e1cc1d77fb12be6d3b9aad847199`;
entry `367873d896b92b9a346e059e75d9d5c09ae7f7343c09095a69eda5503522be2e`,
`outputs/0000.artifact`, under the existing canister artifact cache.
This size includes the test endpoints and SQL machinery, not a minimal typed
application. Incremental fixture Wasm size, gzip and defined-function deltas
were not measured; do not compare older differently versioned actors as a
matched before/after result.

## Workload and boundaries

The existing generated `SqlTestEnrollmentUser` has an authored ULID key,
bounded display name and managed timestamps. Seed IDs encode 1000 + position;
labels are `eligible-{position}`, except a rejected case's final `blocked`.
All seeds are committed before measurement. The application reads those exact
keys, checks returned identities and eligibility, then inserts output IDs
2000 + position with names derived from the loaded values. The host independently
expects `processed:eligible-{position}`, checking source and output IDs and names
through the maintained SQL read surface after each update. Timestamps are not
part of the cross-scenario comparison.

Each measured endpoint invocation owns one request-execution scope. Both read
strategies validate the complete input before staging the same mixed typed
builder's homogeneous output batch. A late domain rejection returns its position
and does not call the write API; it is not an engine rollback or budget failure.
Separate host invocations own chunking. No IcyDB continuation or retry protocol
is introduced, and retries/idempotency are not qualified here.

Read instructions include key-vector construction and generated row decoding.
Validation instructions cover the identity/eligibility loop. Write instructions
include derived input construction, builder pushes, execution and result disposal.
Total instructions include the request scope/session and remaining body work,
but exclude ingress decoding and reply encoding. Stage counters do not partition
all body overhead, and stage allocations can influence later-stage cost.

Cycles are the PocketIC canister balance difference around each update call.
Installation, seed calls, independent query reads and 64 ticks before each update
are outside these intervals. This drains already queued work without advancing
time; any work co-scheduled inside an update interval is not separately attributed.
Separately delivered journal convergence is excluded, so these are explicit-call
costs, not total application lifecycle costs. Only total-call cycles are observed;
stage-level cycles are not inferred from instructions.

## Scenario totals

| Items | Read strategy | Request chunk | Reject final item | Calls | Committed outputs | Body instructions | Explicit-call cycles |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | bulk | 1 | false | 1 | 1 | 6659656 | 14249284 |
| 1 | individual | 1 | false | 1 | 1 | 6661875 | 14250916 |
| 16 | bulk | 16 | false | 1 | 16 | 31599738 | 39350491 |
| 16 | individual | 16 | false | 1 | 16 | 52606265 | 60357770 |
| 64 | bulk | 64 | false | 1 | 64 | 115661947 | 123578279 |
| 64 | individual | 64 | false | 1 | 64 | 204387383 | 212298107 |
| 128 | bulk | 128 | false | 1 | 128 | 228695398 | 236855733 |
| 128 | individual | 128 | false | 1 | 128 | 406820922 | 414975697 |
| 128 | bulk | 128 | true | 1 | 0 | 91632982 | 99775967 |
| 128 | bulk | 16 | false | 8 | 128 | 263183332 | 326895613 |
| 128 | bulk | 16 | true | 8 | 112 | 243971754 | 308143357 |

## Per-message instruction attribution

Case key is items/chunk/read strategy/reject-final-item. Every row is one update.

| Case | Start position | Read | Validate | Write | Total body | Cycles |
| --- | --- | --- | --- | --- | --- | --- |
| 1/1/bulk/false | 0 | 2461292 | 963 | 3587626 | 6659656 | 14249284 |
| 1/1/individual/false | 0 | 2462386 | 963 | 3588720 | 6661875 | 14250916 |
| 16/16/bulk/false | 0 | 12612299 | 11163 | 18366807 | 31599738 | 39350491 |
| 16/16/individual/false | 0 | 33692576 | 11163 | 18293047 | 52606265 | 60357770 |
| 64/64/bulk/false | 0 | 46044129 | 43803 | 68964532 | 115661947 | 123578279 |
| 64/64/individual/false | 0 | 134886641 | 43803 | 68847465 | 204387383 | 212298107 |
| 128/128/bulk/false | 0 | 91559203 | 87323 | 136439161 | 228695398 | 236855733 |
| 128/128/individual/false | 0 | 269763994 | 87323 | 136359845 | 406820922 | 414975697 |
| 128/128/bulk/true | 0 | 90801236 | 87055 | 0 | 91632982 | 99775967 |
| 128/16/bulk/false | 0 | 12903877 | 11163 | 18995123 | 32519919 | 40669377 |
| 128/16/bulk/false | 16 | 12849774 | 11163 | 19061015 | 32691005 | 40765908 |
| 128/16/bulk/false | 32 | 13049038 | 11163 | 19111487 | 33021029 | 40852857 |
| 128/16/bulk/false | 48 | 13059770 | 11163 | 19187123 | 33026700 | 40945789 |
| 128/16/bulk/false | 64 | 13151293 | 11163 | 18983276 | 32755125 | 40751338 |
| 128/16/bulk/false | 80 | 13136295 | 11163 | 19298346 | 33135176 | 41049093 |
| 128/16/bulk/false | 96 | 13196137 | 11163 | 19064704 | 32881968 | 40873348 |
| 128/16/bulk/false | 112 | 13090290 | 11163 | 19201272 | 33152410 | 40987903 |
| 128/16/bulk/true | 0 | 12822022 | 11163 | 19011199 | 32533692 | 40683712 |
| 128/16/bulk/true | 16 | 13181387 | 11163 | 19079639 | 32880565 | 40792153 |
| 128/16/bulk/true | 32 | 13086590 | 11163 | 19232825 | 32940013 | 41013387 |
| 128/16/bulk/true | 48 | 13244994 | 11163 | 19243774 | 33109403 | 41021395 |
| 128/16/bulk/true | 64 | 13241316 | 11163 | 18922196 | 32783917 | 40698290 |
| 128/16/bulk/true | 80 | 13108967 | 11163 | 19404643 | 33133359 | 41203475 |
| 128/16/bulk/true | 96 | 13271726 | 11163 | 19145812 | 33038260 | 40956429 |
| 128/16/bulk/true | 112 | 12925725 | 10895 | 0 | 13552545 | 21774516 |

## Validation and complexity

The ordinary, non-ignored integration selection passes: **1 passed, 0 failed,
0 ignored, 98 filtered out**. Full `make clippy` and the focused final
actor-all-features/integration lint selection pass; formatting and whitespace
checks pass. Initial fixture compilation exposed ULID construction, host cycle
width and Candid service type visibility errors; all were corrected before the
successful measurement. Those failed builds provide no performance evidence.

Eight files, approximately 500 added lines including evidence and notes;
278 lines are test actor/host Rust and module wiring. Delivery domains:
Build/Canister and Integration Tests. Runtime structure is unchanged; test
coverage gains two gated fixture endpoints and one bounded scenario test.
No production API, mode, budget configuration, persisted state, parallel engine,
or generic runner is added.

Eleven disposable PocketIC instances were created and dropped through the
existing fixture owner; no application/local ICP network was changed. No
downstream edits, version changes, commits, pushes or publication. Full
repository/workspace tests remain user-owned.

Reproduce with the focused `sql_canister` integration selection
`application_batch_workload_preserves_results_and_reports_costs` and
`--nocapture`; it builds its own retained current actor. Working logs:
`/tmp/icydb-batch-workload-run3.log` and
`/tmp/icydb-batch-workload-clippy-final.log`. Material results are preserved here.

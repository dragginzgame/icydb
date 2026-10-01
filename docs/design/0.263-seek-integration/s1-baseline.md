# S1 — Current-path qualification

This is a current-source baseline, not an integrated seek comparison. The audit
actor reuses `PerfAuditStreamingRow`; no entity or production execution route
was added. Raw Wasm, instruction and cycle receipts are recorded after focused
validation below. Mainnet costs remain unmeasured.

## Frozen workloads

Every population contains IDs 0–159. Equality predicates select zero-valued
`lane_a`, `lane_b` and optionally `group_key`; unmatched fields contain one.
`sort_key = id % 2` supplies the unindexed residual control. Other rows have a
16-byte payload. Query identity includes ASC/DESC order, projection and the
authored total limit; limits of one and five are not page-size controls.

| Case | A matches | B matches | C matches | Two-way / three-way answers |
| --- | --- | --- | --- | --- |
| 0: spaced | 0–127 | 7, 15, …, 127 | 15, 31, …, 127 | 16 / 8 rows |
| 1: disjoint | 0–127 | 128–143 | 128–135 | empty / empty |
| 2: late | 0–127 | 112–127 | 120–127 | 16 / 8 rows |
| 3: dense | 0–127 | 0–127 | 0–127 | 128 / 128 rows |
| 4: wide late | 0–127 | 112–127 | 116–127 | 16 / 12 rows |
| 5: rotated late | 112–127 | 120–127 | 0–127 | 8 / 8 rows |
| 6: wide rotated | 112–127 | 116–127 | 0–127 | 12 / 12 rows |

Cases 4 and 6 put a 1-MiB payload on IDs 112–127 and project `id, payload`.
The engine executes the full projection inside the instruction window; the
audit endpoint discards payloads afterward and returns IDs, continuation and
page work. Whole-call cycles therefore include that compact reply, not the
wire cost of a full payload reply. These cases establish real budget-driven
pagination, not a small-row seek speedup. Load one four-row batch per update
and discharge wide-row journal debt between batches, outside measured windows.
Attempting all wide batches in one update hit the maintained 16-MiB convergence
byte boundary with E263; increasing that limit was unnecessary.

## Actual admission and route evidence

The fixture's fields are `Int32`. SQL's strict numeric predicates select two-
and three-child logical intersections. Dynamic `FieldRef` numeric filters do
not select that same access: the matched accepted-schema native fixture plans
a full scan, and the IC small-row live-page receipts visit all 160 rows.
SQL `EXPLAIN` is thus an admission control for the SQL statement only; it cannot
prove the dynamic page's route. Keep the two measurements separate.

The source boundary explains the difference: expression predicate extraction
chooses `NumericWiden` for signed integral atoms in
`query/plan/expr/predicate/compile.rs::compare_literal_coercion`; secondary
lookup admission accepts `Strict` or `TextCasefold` in
`query/plan/planner/compare.rs::coercion_supports_index_lookup`. SQL lowering
supplies its schema-canonical strict predicate. This is maintained conservative
admission, not evidence that a primary-key seek was attempted or failed.

Native coverage retains both the matched signed shape and the admitted
`Nat64` shape. The latter checks selected child arity, complete cursorless
prefix charges, empty-probe exhaustion without row reads, ordered page unions,
residuals, total limits, termination and every issued cursor's exact suffix.
Its resumed admitted intersections reach ordinary ordered polling; the direct
complete-prefix probe is excluded by a continuation boundary. Equal-cardinality
and oversized totals retain the conservative single-child decision.

The signed IC pages resume through their maintained primary scan. **An IC cost
baseline for resumed admitted intersections is not established by this actor.**
Do not label the payload-page measurements as that baseline or compare an
unsigned candidate against these signed scan controls as a seek-only delta.
S2 must respect this qualification limit when choosing integration or retirement.
Schema-bound signed predicate admission is a separate improvement to assess
before promising a dynamic-query intersection benefit; S1 does not change it.

## Measurement protocol and receipts

Use one frozen canonical `sql_perf` artifact: `wasm-release`, SQL enabled,
Candid export enabled, local test build. Install a fresh canister per population
on PocketIC 16.0.0. Discharge startup, fixture writes, journal folds and deferred
query fees outside each cycle-balance interval with the existing settling
helper. Sample IC instructions with `performance_counter(1)` and whole update
call charges using the canister cycle balance; no elapsed/native timing is a
performance metric.

SQL samples project IDs through the existing warmed SQL endpoint. Dynamic page
samples include request scope/session/page execution, excluding request
construction, ID extraction and Candid encoding from their instruction counter.
The charged-cycle interval includes the whole update. Repeated pages must agree
on exact IDs, continuation and work; each issued token is replayed to exhaustion.
All artifacts must be matched before any candidate delta is claimed.

Frozen module: **4,504,970 raw bytes**, SHA-256
`325db7585feb97e1ce1693d15081cd2fbebacf20c73058c6c88523ecc514c314`.
Compiler: Rust 1.98.1. Cargo package versions remain 0.262.2 under release tooling;
the implementation belongs to the active 0.263.0 notes. The frozen module is
available locally at `/tmp/icydb-0263-s1-baseline.wasm`; the committed
[CSV receipts](s1-baseline.csv) retain its 40 SQL samples and 38 main page steps,
each page measured twice. Token replay and residual/limit controls also passed.

| Warmed measurement | IC instructions | Charged cycles |
| --- | ---: | ---: |
| SQL, cases 0/1/2/5 | 4,573,407–9,852,509 | 17,576,083–22,744,398 |
| Dynamic scan pages, cases 0/1/2/5 | 20,099,518–21,361,298 | 28,822,465–29,847,309 |
| Dynamic payload pages, cases 4/6 | 50,295,922–152,302,150 | 59,597,097–160,402,843 |

Ranges use SQL repeat 1 and the repeated page sample. SQL's counter starts after
request setup and includes session/read/drop; the page counter includes request
setup and teardown. Endpoints and reply shapes differ. These ranges describe
current workloads and are not an isolated frontend or seek optimization delta.
No matched pre-S1 size or integrated-candidate cost delta was measured.

All 79 focused session tests passed, including 160 signed/unsigned workload
combinations. The manual PocketIC matrix passed all 28 initial shapes and real
continuation suffixes. Clippy recovery, focused core/actor all-feature and IC
Clippy gates, layer-authority, formatting and documentation checks passed.
Full repository suites were not run. Two disposable PocketIC servers were
started and stopped; the first run identified loader byte pressure, the final
run completed the matrix. No ICP network, commit, push or Cargo version changed.

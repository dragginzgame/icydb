# 0.257 final explain qualification

Historical qualification finding: the subsequently approved
[pagination repair](../02/report.md) resolves the functional failure and
executes the previously unreached membership case. The measurements and initial
failures below are preserved; they are not a fresh post-repair cost qualification.

2026-09-17 · published source `355bca1f72f17fb2af9c75ab5be8fb73b71604dc`
(0.257.21), plus the new native regression test. No production changes.

## Verdict

**Not ready for closeout or push.** The final functional matrix reproduces
duplicate rows during indexed-range continuation, and the unchanged diagnostic
instruction gate fails eight of nine samples. Do not reopen a general
optimization program or silently extend earlier cost acceptance to these artifacts.

The user accepted C1-N (complete shared normalization admission) and C1-M
(aggregate accepted-metadata preparation) deferrals. Existing safeguards and
their reopening triggers remain in the [closeout plan](../../../../../../../design/0.257-typed-query-explain/0.257-closeout.md).

## Functional qualification

Fresh focused executions on the published runtime:

| Selection | Passed | Coverage |
| --- | ---: | --- |
| Core explain, all features | 49 | Projection/render limits, detached DTO, unchanged identity after rejection, no-row diagnostics |
| Cardinality/planner lifecycle | 62 | Root/binding changes, unavailable-to-ready evidence, valid warm reuse, range and membership results |
| Session ordering/admission | 71 | Cache retention/publication, cumulative rejection, input and binding checks |
| Order-contract admission | 6 | Shared ordering facts, cumulative limits, rejected continuation construction |
| Composite schema catalog | 11 | Exact per-entity projection allowance, shared allowance across fields, semantic fallback |
| SQL-free public explain actor | 4 | Public consuming API, no generated row decode, cursor and input rejection |
| SQL-free core explain | 31 | Diagnostic owners without SQL |

These are 234 successful test executions, not 234 distinct tests: selections
overlap. Two native timing tests remain ignored by policy. No full suite ran.

One added test covers point lookup, an indexed text range, and a 1,024-value
primary-key membership input. It compares cold/two warm typed reports, unchanged
compilation counts, SQL canonical parity, zero result-row visits by explain,
and complete ordinary-read results. Its first failure was a harness mistake:
the native live-page fixture returns at most two rows, while SQL returns the
complete result. Corrected traversal exposes a real result-prefix failure:

```sql
SELECT * FROM PlannerRow
WHERE rare >= 'group-a' AND rare < 'group-b'
ORDER BY id LIMIT 20
```

The unchanged fixture contains IDs 0–11; IDs 0–5 match. Following the returned
live continuations produces `[0, 1]`, `[2, 3]`, then `[0, 1]`, instead of ending
with `[4, 5]`. The query, schema and rows do not change between pages. The
assertion compares actual row values, not diagnostic text. A fixed eight-page
guard first detected failure to terminate; the added prefix assertion identifies
the first wrong page. Cold/warm typed and SQL reports match and visit no rows
before this read. Point lookup passes; the later 1,024-value case is **not
reached**, so it remains unqualified, not implicitly passing.

The exact root cause is not established: do not assume the planner cache,
normalizer or `.explain()` itself is responsible. The smallest separately
approved repair outcome is to reproduce the range continuation independently
of diagnostic warm-up, correct its existing progress owner, and qualify the
remaining matrix. Retain this unignored failing test until repaired; do not
change query bounds or expected rows to obtain a passing run. No runtime fix
was attempted in this qualification handoff.

Existing evidence, **not rerun here**: the maintained multi-entity SQL actor's
six ordinary query families and whole-call cycle/instruction/heap measurements
in [catalog receipt 06](../../catalog-selection/06/report.md), and the
per-entity admission IC stress cases in
[schema receipt 03](../../../16/schema-preparation/03/report.md).
The current matched explain actors have one empty entity, not a multi-entity
or populated-query workload. Those historical receipts do not establish
arbitrary database-wide bounds or fresh measurements at every schema scale.

## Matched IC diagnostics

The unchanged maintained `typed_explain_measurement` gate ran once. All nine
typed/SQL/mixed report comparisons pass, and the mixed ordinary SQL read returns
zero rows. Only the final collected instruction assertion fails (exit 101).
Warm means later calls inside the same request, not cross-request heap reuse.

| Query | Call | Typed planning | Typed rendering | SQL total | +5% planning gate |
| --- | --- | ---: | ---: | ---: | --- |
| Primary-key equality | Cold | 2,402,995 | 230,404 | 2,250,906 | Fail |
| Primary-key equality | Warm 1 | 660,963 | 230,514 | 447,202 | Fail |
| Primary-key equality | Warm 2 | 660,963 | 230,514 | 447,202 | Fail |
| Scan/sort | Cold | 2,392,991 | 270,117 | 2,261,826 | Fail |
| Scan/sort | Warm 1 | 618,160 | 270,223 | 443,140 | Fail |
| Scan/sort | Warm 2 | 620,685 | 270,195 | 443,230 | Fail |
| Grouped COUNT | Cold | 2,368,178 | 321,314 | 2,326,149 | Pass |
| Grouped COUNT | Warm 1 | 630,331 | 321,437 | 510,748 | Fail |
| Grouped COUNT | Warm 2 | 629,571 | 321,247 | 512,410 | Fail |

Compared with the accepted published 0.257.5 receipt, typed cold planning falls
12.78–13.30%; the three-call planning-plus-rendering sum falls 6.79–7.16%.
This is a cumulative release comparison, with different locked dependencies,
not attribution to one change or a claim that every warm sample improved.
SQL also got cheaper: cold scan/sort now misses the original relative gate,
which previously had seven misses. That acceptance explicitly did not
preapprove future regressions. No threshold was changed.

### Whole endpoint instructions

The [temporary host probe](whole-call-probe.rs.txt) calls unchanged actors.
Each endpoint performs three explain/render calls (cold, warm, warm).
Use four epochs of thirteen distinct callers to avoid query-result-cache reuse,
with 60 zero-time ticks between epochs. Management statistics report 39 calls
from three completed epochs for each artifact/query pair. All 468 executed
calls return matching reports; 351 calls are represented in these statistics.

| Query | Typed instructions/endpoint | SQL explain | Mixed typed | Typed vs SQL |
| --- | ---: | ---: | ---: | ---: |
| Primary-key equality | 5,598,442 | 4,322,516 | 5,623,090 | +29.52% |
| Scan/sort | 5,642,184 | 4,339,479 | 5,666,894 | +30.02% |
| Grouped COUNT | 5,820,679 | 4,570,873 | 5,844,916 | +27.34% |

These counts include request setup and response work. Do not divide by three
and call the result the cost of an isolated cold or warm request. No new
charged-cycle comparison or whole-endpoint historical baseline was measured.
The local-counter gate and whole-endpoint comparison have different intervals.

## Wasm and provenance

Rust 1.98.1 (`48a229cea`, 2026-09-01); locked/offline dependencies;
Cargo.lock SHA-256 `2861ffbeb525446769bda64557a70e263e750ebfb547b2a6d4e3323f9d3d9562`.
Package `canister_audit_one_entity_typed_query`, no default features,
`wasm-release`, `wasm32-unknown-unknown`; Candid export/U256 off.
Binaryen 132: `-Oz --enable-bulk-memory --enable-sign-ext
--enable-nontrapping-float-to-int --one-caller-inline-max-function-size=0`.
Optimizer SHA-256 `1014958e6f20d412f1542320b43970214b0fb1ed780595e8f7c0d8761ed53725`.

| Subject / features | Compiler bytes | Raw post-link bytes | Defined functions |
| --- | ---: | ---: | ---: |
| Typed / `typed-explain-measurement` | 2,861,133 | 2,510,157 | 6,411 |
| SQL / `sql-explain-measurement` | 3,841,197 | 3,382,805 | 8,510 |
| Mixed / `sql,typed-explain-measurement` | 3,868,888 | 3,406,596 | 8,594 |

Typed saves 872,648 raw bytes (25.80%) and 2,099 functions versus matched SQL.
Non-explain and retained-SQL-only controls were not rebuilt; this is not a
fresh pass of every historical Wasm/control gate. No current runtime delta is
claimed: this handoff changes tests and records only.

Final raw hashes:

- Typed: `f8581ce124529dd63ec84e08febd66163c46f876b5e8f6cd6960de4795d02d03`
- SQL: `1cfa6bb258962530cc8d9c2ca95d6d9318616ba04e3cafba104d4c7a91fcd3e4`
- Mixed: `e36f99c18fd5eb5fe8cc90bb24c59fad2f1bec16cf5f675c19600f0884b6bd53`

Artifacts/build and test logs: `target/closeout-25721-qualification/`.
The host binary SHA-256 is
`5115a4a6353078fbfc8d54cce9230b3cbeed3994894f440ab9ea31c108fdac2d`.
Reproduce the gate with `ICYDB_EXPLAIN_WASM_DIR` pointing there and the
maintained opt-in comparison. For whole calls, temporarily wire the archived
probe as `closeout_whole_call_tmp` beside that test, using PocketIC 16.0.0.
Temporary wiring was removed; no actor instrumentation or production modes
were added. Twelve disposable PocketIC fixtures were used; shared networks
were untouched. No native timing benchmark ran.

## Handoff

Focused core Clippy and formatting/whitespace checks pass; 57 active-document
links/anchors resolve and both archived historical bodies match HEAD exactly.
The new functional
regression and the diagnostic instruction gate fail. Full release validation
is user-owned and should wait for the correctness repair. The historical
multi-entity/control receipts are evidence, not fresh passes of omitted cases.
Current charged cycles and the unreached large-membership case remain unmeasured
and unqualified respectively. No production performance delta is attributable
to this test/documentation-only handoff.

Complexity: ten files, approximately 567 net added lines including evidence
and archived measurement source; one native test adds 84 lines. No production Rust, runtime state,
authority, cache, mode or budget changes. Active planning is condensed, with
the original delivery history preserved rather than discarded. The next decision
is the bounded correctness repair; diagnostic cost acceptance remains separate.

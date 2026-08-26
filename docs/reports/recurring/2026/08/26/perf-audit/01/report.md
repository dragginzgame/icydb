# Query Instruction Footprint Audit

## 0. Run Metadata + Comparability Note

- scope: current 0.244 SQL query instruction footprint, with focused
  cross-version regression sampling around the 0.238, 0.240.1, 0.242 and
  0.244 optimized routes and one ordinary shared-floor sample
- definition path:
  `docs/audits/recurring/crosscutting/crosscutting-perf-audit.md`
- compared baseline report path:
  `docs/reports/recurring/2026/05/14/perf-audit/01/report.md`
- secondary evidence:
  `artifacts/perf-audit/0.237-patch3-expanded-clean/run-3/`
- code snapshot identifier: `7fa5cb5d35def70d1883a7c8e11f99718962d941`,
  tree `2201153fcf7933e5eea2da9a9d58cfcefa4c7323`, plus the current
  0.244.0 version surface, 0.244 closeout cleanup and documentation worktree
- method tag/version: `PERF-0.244-focused-pocketic-debug/v1`
- comparability status: `partial`
- auditor: `Codex`
- run timestamp (UTC): `2026-08-26T16:29:46Z`
- branch: `main`
- worktree: `dirty`
- execution environment: `PocketIC` 15.0.0, Rust 1.97.1, focused debug audit
  canister
- entities in scope: `PerfAuditUser`
- entry surfaces in scope: SQL query execution with instruction, cache,
  physical-work and selected phase attribution
- query shapes in scope: primary-key scalar reads, grouped count, ordered
  scalar `DISTINCT`, exact entity/distinct/range counts, exact indexed numeric
  aggregates, prepared aggregate controls, cold, true-warm and loop-local reuse
- normalized samples:
  `docs/reports/recurring/2026/08/26/perf-audit/01/artifacts/current-samples.json`

The 2026-05-14 report is not numerically comparable. It used an older harness
envelope whose representative SQL rows were tens of millions of instructions.
The 2026-06-13 matrix is also context only because it used PocketIC 14.0.0 and
0.182 source. Neither is used to call current regressions.

The last complete clean 0.237 P1/P2/scale evidence has the current 1,787-row P1
scenario-set hash, but its scale set contains 174 observations. The checked-in
profile now requires 210 observations under scale hash
`bbd40c3f2633a1d2d38e5c9f4a24afae137aec10de49ec4e1627cc34b19d4c3c`.
Local shards exist for that hash, but they were captured from a dirty 0.237.2
worktree and are not reviewed baseline authority. This run therefore compares
only the focused scenarios to their frozen candidate evidence. It does not
claim a complete current P1/P2/scale verdict.

Each focused test family used a fresh canister. Scenarios within a family
intentionally shared its documented fixture lifecycle, including empty,
2,048-row, all-unique, cache-warm and recovery transitions. The current debug
audit Wasm was 15,052,993 raw bytes with SHA-256
`13e750f3c3385ed9a09828055b2ba5e504945f25ddd559e8c5be2d67dbdddc98`.
That identity is recorded for repeatability and is not compared to the
production Wasm footprint.

## 1. Coverage Table

| Scenario Family | Surfaces Covered | Missing Surfaces | Attribution Depth | Risk |
| --- | --- | --- | --- | --- |
| Scalar load and projection | SQL primary-key covering/order plus finite membership shared-floor rows | typed/fluent current-source lane; complete scalar matrix | compile, planner, store, executor; projection partial | medium |
| Ordered scalar `DISTINCT` | SQL 0.238 admitted group seek and prepared rejection controls | complete range/window/type matrix in a current broad run | compile, aggregate execute and exact physical work | low |
| Exact global counts | entity, distinct and indexed range; cold/warm, zero, over-budget and recovery | current P2 repetition | total, compile, terminal executor, cache and physical work | low |
| Exact numeric aggregates | `SUM`, `AVG`, duplicate-heavy, all-unique, cold/warm, fallback and recovery | numeric types beyond the deliberately unsupported `Int32` cohort | total, cache and physical work; internal phase split partial | low |
| Prepared global aggregates | aggregate `FILTER`, all-unique `SUM` and rejected `DISTINCT` controls | broad current aggregate matrix | total and physical work; internal fold attribution partial | medium |
| Grouped queries | grouped count cold and ten-run loop-local reuse | grouped cursor, `HAVING`, multi-aggregate and scale rows | compile, planner, store and executor; finalize partial | medium |
| Cursor, explain and updates | covered by the user-owned full validation as behavior | no current instruction capture in this run | not sampled | medium |
| Complete P1/P2/scale profile | historical clean 0.237 evidence only | clean current-source 210-scale-scenario bundle | historically deep; current `N/A` | medium |

## 2. Current Matrix

All totals below are local instructions. `Count = 10` rows report both the
aggregate count and the per-call average required by the canonical row model.

| Scenario Key | Entry Surface | Count | Avg | Notes |
| --- | --- | ---: | ---: | --- |
| `user.pk.key_only.asc.limit1` | SQL | 1 | 750,329 | ordinary covering floor; 375,398 compile |
| `user.grouped.age_count.limit10` | SQL | 1 | 1,887,665 | 352,549 planner; 652,611 store; 426,656 executor |
| `repeat.user.grouped.age_count.limit10.runs10` | SQL | 10 | 1,435,703 | nine compiled/shared cache hits |
| `0.238.age_asc_limit_three.cold` | SQL | 1 | 2,088,036 | four index entries/range scans; zero row gets |
| `0.237.exact_entity_count.rows_2048.cold` | SQL | 1 | 817,481 | zero row/index traversal |
| `0.240.1.exact_distinct_count.cold` | SQL | 1 | 468,615 | zero row/index traversal |
| `0.240.1.exact_distinct_count.warm` | SQL | 1 | 301,158 | true warm |
| `0.242.range_count.cold` | SQL | 1 | 1,099,446 | zero row/index traversal |
| `0.242.range_count.warm` | SQL | 1 | 292,818 | true warm |
| `0.244.sum.cold` | SQL | 1 | 471,672 | zero row/index traversal |
| `0.244.sum.warm` | SQL | 1 | 301,275 | true warm |
| `0.244.avg.cold` | SQL | 1 | 471,461 | zero row/index traversal |
| `0.244.avg.warm` | SQL | 1 | 299,993 | true warm |
| `0.244.sum.all_unique.cold` | SQL | 1 | 1,358,076 | exact metadata fold |
| `0.244.sum.all_unique.prepared_control` | SQL | 1 | 122,556,045 | 2,048-row prepared scan |
| `0.240.aggregate_filter.cold` | SQL | 1 | 114,108,839 | maintained prepared control |
| `0.240.aggregate_filter.warm` | SQL | 1 | 113,768,277 | cache warmth cannot remove row work |

The current simple ordinary shared-floor rows are below two million
instructions. The greater-than-10M rows in this focused sample are prepared
2,048-row aggregate or rejected-shape controls, not the newly admitted exact
routes.

## 3. Comparison Highlights

The focused regression comparisons use the same named scenario and frozen
fixture contract. They are single observations protected by the existing hard
test gates, not replacements for five-sample P2 medians.

| Scenario | Documented Candidate | Current | Delta | Delta % |
| --- | ---: | ---: | ---: | ---: |
| 0.238 ordered `DISTINCT age ASC LIMIT 3` | 2,083,119 | 2,088,036 | +4,917 | +0.236% |
| 0.240.1 exact distinct count cold | 467,343 | 468,615 | +1,272 | +0.272% |
| 0.240.1 exact distinct count warm | 299,976 | 301,158 | +1,182 | +0.394% |
| 0.242 exact range count cold | 1,098,185 | 1,099,446 | +1,261 | +0.115% |
| 0.242 exact range count warm | 291,647 | 292,818 | +1,171 | +0.402% |
| 0.244 indexed `SUM` cold | 471,713 | 471,672 | -41 | -0.009% |
| 0.244 indexed `AVG` cold | 471,502 | 471,461 | -41 | -0.009% |
| 0.244 all-unique indexed `SUM` | 1,357,758 | 1,358,076 | +318 | +0.023% |
| ordinary aggregate `FILTER` cold | 114,058,947 | 114,108,839 | +49,892 | +0.044% |
| ordinary aggregate `FILTER` warm | 113,927,409 | 113,768,277 | -159,132 | -0.140% |

None reaches the checked-in absolute-plus-relative regression rule: each
positive delta is either below 10,000 instructions or below one percent, and
the focused route-specific ceilings also pass. The ordinary prepared control
is particularly important: 0.244 did not make the non-admitted aggregate path
materially worse.

Against their frozen predecessors, the current optimized rows retain the
intended large absolute reductions:

| Shape | Frozen Predecessor | Current | Absolute Saving | Improvement |
| --- | ---: | ---: | ---: | ---: |
| ordered `DISTINCT age ASC LIMIT 3` | 93,144,415 | 2,088,036 | 91,056,379 | 97.758% |
| exact entity `COUNT(*)`, 2,048 rows | 96,891,236 | 817,481 | 96,073,755 | 99.156% |
| exact `COUNT(DISTINCT age)` | 120,671,186 | 468,615 | 120,202,571 | 99.612% |
| exact indexed range `COUNT(*)` | 23,429,276 | 1,099,446 | 22,329,830 | 95.307% |
| exact indexed `SUM(age)` | 98,081,958 | 471,672 | 97,610,286 | 99.519% |
| exact indexed `AVG(age)` | 98,067,171 | 471,461 | 97,595,710 | 99.519% |

This is a focused no-whack-a-mole pass for the maintained optimized families.
It is not evidence that every one of the 1,787 P1 and 210 scale scenarios is
flat.

## 4. Phase Attribution Read

| Scenario Key | Compile | Planner | Store | Executor | Projection/Finalize | Notes |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `user.pk.key_only.asc.limit1` | 375,398 | 271,018 | 60,205 | 39,620 | 67,663 row assembly | complete outer split; nested values overlap their owner phase |
| `user.grouped.age_count.limit10` | 419,763 | 352,549 | 652,611 | 426,656 | `PARTIAL` | store is the largest measured phase |
| `repeat.user.grouped.age_count.limit10.runs10` | 288,462 | 38,060 | 651,997 | 420,837 | `PARTIAL` | compile/planner shrink; store work remains |
| `0.238.age_asc_limit_three.cold` | 437,874 | `PARTIAL` | `PARTIAL` | 1,650,162 aggregate execute | `PARTIAL` | exact physical proof: four entries, zero rows |
| `0.237.exact_entity_count.rows_2048.cold` | 356,422 | `PARTIAL` | zero physical reads | 457,055 terminal | `PARTIAL` | total 817,481 |
| `0.242.range_count.cold` | 600,543 | `PARTIAL` | zero physical reads | 20,430 terminal | `PARTIAL` | total 1,099,446; planner/envelope remains material |
| `0.244.sum.cold` | `N/A` | `N/A` | zero physical reads | `N/A` | `PARTIAL` | focused output records total and physical proof only |

The shared-floor path has useful compile/planner/store/executor separation.
The exact-route tests expose strong total and physical-work evidence but do not
render a uniform planner/metadata-fold breakdown. That limits localization of
small sub-million drift, but it does not weaken their absolute result or
zero-traversal gates.

## 5. Hotspot Localization

1. Prepared global aggregation remains the clearest current focused hotspot.
   The all-unique prepared `SUM` costs 122,556,045 instructions and the
   aggregate-`FILTER` control costs 114,108,839 cold. Both ingest 2,048 rows;
   cache warmth saves little because it does not remove store and reducer work.
   The relevant shared owners are
   `db/session/sql/execute/global_aggregate.rs`,
   `db/executor/aggregate/` and the ordinary row-loading pipeline.

2. Rejected scalar `DISTINCT` shapes remain similarly expensive. The nullable
   expression control measured 118,481,753 instructions with 2,048 row gets,
   while the admitted direct ordered shape used at most four index entries.
   This is a semantic admission boundary, not evidence that nullable or derived
   expressions should receive another narrow fast path.

3. Exact range execution is now dominated outside its terminal primitive. The
   current bounded range result totals 1,099,446 instructions, while the
   terminal executor attribution is only 20,430. Compile alone is 600,543.
   Any future reduction should first establish whether shared compilation or
   planner work is removable across multiple query families.

4. The small ordinary grouped floor remains store-led. Cold grouped count uses
   652,611 store instructions, and the ten-run cache path still averages
   651,997 store instructions. Compile and planner caching work as intended;
   the retained data work is not a cache miss disguised as execution cost.

These observations identify owners. They do not authorize another optimized
route, widen 0.244, or override the SQL performance-breadth governance rule.

## 6. Coverage Gaps

- `PERF-001` (`MEDIUM`): there is no clean current-source complete matrix for
  the 210-scenario scale profile. The next scheduled clean run must build one
  exact Wasm, execute all P1, scale and P2 shards, and compare only against
  reviewed compatible evidence.
- Current profile-hash scale shards under
  `artifacts/perf-audit/0.237-patch11-simple-query/` are useful historical
  discovery evidence but remain dirty-worktree evidence. They must not be
  promoted or cited as a clean 0.244 baseline.
- Typed/fluent execution was not sampled. No 0.244 code changed that path, but
  this run cannot make a cross-surface cost claim.
- Cursor, explain, update, mutation-job and rejection families were not
  captured as current instruction rows. Their behavior passed the user-owned
  complete validation; that is correctness evidence, not instruction evidence.
- Exact aggregate diagnostics do not yet render the same complete phase table
  as the ordinary shared-floor sampler. Attribution depth is `PARTIAL`, not
  zero.
- Peak heap, allocator traffic and stable-memory byte volume remain explicitly
  unmeasured. Zero is not substituted for unavailable evidence.

## 7. Overall Read

Verdict: `PASS WITH FINDINGS`.

The current 0.244 performance slice passes its focused audit. Its `SUM` and
`AVG` rows remain at roughly 0.47M cold and 0.30M true warm, the hostile
all-unique row remains 90 times cheaper than prepared execution, and the
ordinary aggregate control is flat. The earlier 0.238, 0.240.1 and 0.242
families remain within 0.402% of their documented candidate samples and retain
their zero-row/index-work or bounded-group-seek proofs. No focused
whack-a-mole regression was found.

The clearest expensive rows are still broad prepared row aggregation and
rejected expression-based `DISTINCT`, each around 114M-123M instructions at
2,048 rows. They should be reconsidered only through a broad, shared physical
capability. The current evidence does not justify another type- or
shape-specific fast path.

The one finding does not block 0.244. It limits the breadth of the conclusion:
matrix-wide regression freedom and the next hotspot ranking require a clean
current P1/P2/210-scale scheduled run. That run is the next best performance
measurement, after the report and all other relevant worktree files are
tracked so the ICYDB-011 release-integrity gate can establish a clean source
subject.

This audit changed documentation only. It made no production, fixture,
schema, Candid, export, stable-format or package behavior change.

# C24–C26 — Matched Wasm and IC cost measurements

Recorded 2026-09-22. The latest three optimisations reduce the measured actor
by **872 raw Wasm bytes (0.0195%)**. All 12 indexed-write cases use fewer
instructions and cycles. Bounded index reads are mixed: cycles change from
−1.573% to +0.920%, so this is not a blanket query-cost improvement.

## Scope and reproducibility

The baseline is the saved worktree immediately before C24–C26. C21 data-key
decoding, C22 standard comparisons and C23 recovery identity are present on
both sides. This isolates the three latest changes as a group; it does not
measure the full 0.261.5 patch against 0.261.4 or attribute gains to each change.

Both snapshots build in the same isolated source directory with the same
Cargo.lock, Rust 1.98.1, `wasm-release` profile (opt-level z, fat LTO, one codegen
unit), SQL/Candid enabled, maintained LocalTest feature policy and Binaryen 132
post-link pipeline. The subject is the broad `sql_perf` audit actor with existing
measurement endpoints, not a downstream production application. No actor
instrumentation, dependencies or Cargo versions were changed.

[Machine-readable evidence](c24-c26-measurements.json) retains artifact/source
hashes, all samples, the disposable host probe and the initial harness failure.
Frozen Wasms, host executable and logs remain in
`/tmp/icydb-c24-c26-measure`. The host binary is identical for both artifacts.

| Final deployable artifact | Raw non-gzipped bytes |
| --- | ---: |
| Before | 4,464,234 |
| After | 4,463,362 |
| Delta | −872 (−0.0195%) |

The emitted Candid interfaces match. Gzip size was not measured.

## Workloads and measurement windows

- PocketIC 16.0.0, fresh canister per fixture, normal startup delivery. Setup
  and 64 deterministic settling rounds precede each measured update. No
  auto-progress loop or manual time advancement is used.
- Indexed writes use the existing `indexed_big_integer_write_wasm_cost_matrix`
  and `measure_indexed_big_integer_write` endpoint: unsigned/negative signed
  values, 20/300/4092 decimal digits, one or 32 rows, two accepted indexes.
  All 12 writes succeed and post-write row counts match. Local instructions
  include request execution and mutation, excluding fixture-value construction
  and response encoding. Cycles are whole-update balance differences.
- Range reads seed 32 unsigned rows at 20 or 4092 digits, then select eight IDs
  with `unsigned > 0 ORDER BY unsigned, id LIMIT 8`, both ascending and
  descending. EXPLAIN confirms `perf_big_unsigned_idx`. Three successive updates
  per direction use `warm_user_query_with_perf`; instructions cover session
  acquisition and query execution, while cycles include the full update envelope.
  Repeats are reported individually; they are not interchangeable cold/warm
  samples.
- Before and after are each replayed twice (96 records total). Every instruction
  count, cycle charge and result hash is identical across replays for its
  artifact. Before/after range results and selected indexes match. This proves
  repeatability of these fixture envelopes, not attribution to an isolated
  allocator, encoder or traversal phase.

## Indexed writes

All one-row and 32-row cases improve: **0.0069–0.3330% fewer cycles** and
**0.0103–0.3972% fewer local instructions**. The larger batches are shown below;
all one-row records remain in the evidence JSON.

| Sign | Digits | 32-row cycles before → after | Cycle change | Instruction change |
| --- | ---: | ---: | ---: | ---: |
| Unsigned | 20 | 51,706,251 → 51,534,082 | -0.3330% | -0.3972% |
| Unsigned | 300 | 58,619,453 → 58,452,809 | -0.2843% | -0.3471% |
| Unsigned | 4092 | 231,371,421 → 231,149,235 | -0.0960% | -0.1821% |
| Negative signed | 20 | 51,876,278 → 51,716,793 | -0.3074% | -0.3641% |
| Negative signed | 300 | 60,984,409 → 60,819,644 | -0.2702% | -0.3145% |
| Negative signed | 4092 | 263,149,302 → 262,958,055 | -0.0727% | -0.1232% |

## Bounded index reads

The 20-digit reads all use fewer whole-update cycles. Wide-key reads have
both increases and decreases; the largest increase is **153,173 cycles
(+0.920%)**. Local instructions range from −13.448% to +6.175%. Retain these
regressions when assessing the cleanup; no causal explanation is established.

| Digits | Direction | Repeat | Cycles before → after | Cycle change | Instruction change |
| ---: | --- | ---: | ---: | ---: | ---: |
| 20 | ASC | 0 | 14,935,605 → 14,928,534 | -0.0473% | -0.1915% |
| 20 | ASC | 1 | 15,266,216 → 15,256,793 | -0.0617% | +5.9379% |
| 20 | ASC | 2 | 15,613,946 → 15,368,367 | -1.5728% | -13.4480% |
| 20 | DESC | 0 | 15,981,515 → 15,973,903 | -0.0476% | +4.7990% |
| 20 | DESC | 1 | 15,557,799 → 15,475,996 | -0.5258% | +2.8615% |
| 20 | DESC | 2 | 15,585,462 → 15,474,996 | -0.7088% | -0.3637% |
| 4092 | ASC | 0 | 16,480,890 → 16,322,742 | -0.9596% | +0.0302% |
| 4092 | ASC | 1 | 16,650,994 → 16,804,167 | +0.9199% | +6.1749% |
| 4092 | ASC | 2 | 16,914,307 → 16,989,351 | +0.4437% | -2.0516% |
| 4092 | DESC | 0 | 17,321,731 → 17,402,632 | +0.4670% | -7.3677% |
| 4092 | DESC | 1 | 16,897,361 → 16,897,608 | +0.0015% | -2.2410% |
| 4092 | DESC | 2 | 16,906,544 → 16,903,239 | -0.0195% | -4.4009% |

## Validation, limitations and follow-up

Both scoped host tests pass in all four matched runs. The existing 158 focused
native tests, strict lint and scoped invariants passed at the implementation
handoff; they were not repeated for this documentation-only measurement work.
Full release validation and remote CI remain user-owned.

The initial maintained ignored write test fails on the baseline because it
expects a 4094-digit unsigned value to exceed the index-component limit, while
the current encoder accepts it. The disposable host narrowed the write matrix
to known-valid widths on both artifacts. This is not a rejection-boundary
qualification. Repair the maintained boundary fixture separately; no production
change was made to suppress the failure.

Two disposable PocketIC servers were started and stopped (initial attempt and
matched replays). No persistent local network was restarted.

C24–C26 touch nine implementation/test/note files, with 44 fewer lines in
runtime source files and 53 net added test lines. Shared framing, borrowed
bounds and removal of unused bookkeeping simplify the implementation without
adding an independent behavior axis. This measurement adds only evidence and
note updates to the worktree.

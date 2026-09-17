# Warm-read attribution — whole-query and heap follow-up

Date: 2026-09-17. Same frozen baseline/candidate artifacts, fixture and toolchain
as [the simple-query qualification](../02/report.md). No production changes.

## Verdict

**A small real regression, with misleading partial-counter magnitudes.** Whole
query instructions rise 0.19–1.12% across these six warmed reads. The earlier
local interval's 5.4–6.0% increases and 4.4% range improvement do not describe
whole-query cost. Repeated updates executing the same reads also consume more
cycles. Do not call this a general query-performance improvement or dismiss the
increase as measurement noise.

The candidate allocates three additional Wasm heap pages during fixture setup;
the sampled reads do not grow that allocation. Allocation/layout effects are a
plausible explanation, not an established causal attribution. Exact live-object
retention, allocator high-water marks and touched-page addresses remain unmeasured.

## Complete query instructions

PocketIC query statistics, total reported instructions divided by 39 reported
calls per row. Each fixture executes 52 calls using the maintained four-epoch,
13-distinct-callers protocol; the last epoch has not yet entered the statistics.
The local interval is identical across all 52 calls and matches the earlier
warmed samples. Full results match the update warm-up and the other artifact.

| Query | Baseline per call | Candidate per call | Delta | Change |
| --- | ---: | ---: | ---: | ---: |
| Primary key | 8,438,285 | 8,525,120 | +86,835 | +1.029% |
| Indexed equality | 9,207,983 | 9,298,660 | +90,677 | +0.985% |
| Indexed range, limit 3 | 9,153,069 | 9,241,839 | +88,770 | +0.970% |
| Primary-key `IN` | 8,679,674 | 8,767,364 | +87,690 | +1.010% |
| Count | 8,673,857 | 8,770,941 | +97,084 | +1.119% |
| Grouped count | 9,125,830 | 9,143,476 | +17,646 | +0.193% |

[Whole-query totals](whole-query.csv) cover 624 executed queries, 468 reported
calls, on 12 fresh isolated fixtures. This measures complete query execution,
including wrapper work outside the actor's local counter. It is not a charged
query-cycle estimate, nor a large-data or production-application qualification.

## Phase and charged-cycle cross-check

The existing `warm_user_query_with_perf` endpoint already exposes counters at
entry, request-ready, session-ready, query-complete and request-complete. Its
last counter still excludes response encoding and final wrapper work. No actor
instrumentation was added. Each of six fresh fixture pairs replays the original
first/warmed sequence, then executes three matched updates. Cycle-balance
differences bracket each update without interleaving explicit ticks.

| Query | Candidate charged-cycle increase across three paired updates |
| --- | ---: |
| Primary key | +1.435–1.533% |
| Indexed equality | +1.833–1.854% |
| Indexed range | +1.385–1.414% |
| Primary-key `IN` | +0.952–1.016% |
| Count | +1.084–1.483% |
| Grouped count | +0.539–0.603% |

The [phase samples](phases.csv) demonstrate redistribution between intervals.
For `IN`, repeat 2, candidate instructions before request-ready fall 480,114,
while the local read interval rises 479,255; request-complete differs by only
−708, yet charged cycles rise 177,909. Partial-counter cancellation alone is
therefore not sufficient to establish whole-call parity. Updates persist cache
and allocation changes and are not interchangeable with query-message costs.
[Query replay](query-replay.csv) reproduces every previous warmed local count.

The earlier [IC phase investigation](../../../../../../../design/0.257-typed-query-explain/0.257-aggregate-phase-perf.md)
documents deterministic memory-page charges and similar boundary movement.
These new samples are consistent with that mechanism, but do not identify
individual page touches or justify subtracting inferred page costs.

## Allocated memory and source trace

[Memory samples](heap.csv), after fixture load and before each of five successive
primary-key query/update pairs:

- Baseline allocated Wasm heap: 4,587,520 bytes (70 pages), unchanged.
- Candidate allocated Wasm heap: 4,784,128 bytes (73 pages), unchanged.
- Difference: +196,608 bytes / 192 KiB, already present before these reads.
- Stable memory: 39,911,424 bytes for both, unchanged.

Allocated linear memory is not live retained metadata. These samples establish
no per-read growth in this sequence, not absence of leaks in all workloads.
The source still returns an existing accepted runtime root when store roots
match; the changed snapshot handoff is in runtime construction. No new warm
schema-validation loop, query execution route or row scan was introduced by
the candidate. This source observation does not count runtime cache hits.

## Follow-up and reproduction

Investigate setup-time allocation lifetime/high-water behavior next, using the
same complete-query counters to judge any fix. Distinguish retained objects
from allocation layout before changing the shared representation. Do not add
another cache, padding workaround or counter merely to recover a local sample.
The metadata-only gain remains valid; broad query improvement remains unclaimed.

Temporarily wire [the archived probe](probe.rs.txt) as the
`catalog_phase_measurement_tmp` child module of `sql_perf_audit`. Run its three
named tests individually against the frozen artifacts from receipt 02, using
`ICYDB_QUERY_MEASUREMENT_DIR` and PocketIC 16.0.0. The whole-query protocol reuses
`group_path_measurement_contract`'s distinct-caller/epoch approach. No Wasm
rebuild or additional compiler instrumentation is required.

All three focused probes pass with result parity. Temporary native module
wiring was removed after capture. Twenty-six isolated fixtures were released;
shared networks were untouched. Full release validation remains user-owned.
Raw Wasm remains 4,485,070 → 4,483,015 bytes (−2,055), with one more defined
function. This investigation adds documentation/evidence only; runtime
complexity is unchanged. No wall-clock performance metrics were used.

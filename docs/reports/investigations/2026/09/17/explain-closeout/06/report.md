# Published 0.257.22 — whole-call and multi-entity qualification

Closeout disposition, 2026-09-17: the user accepted the diagnostic trade-off and
finite coverage below and authorized closing 0.257. The four cost-gate misses
remain unchanged; the measurements and original recommendation are preserved.

2026-09-17 · published source `f3bd969be9dd7087a0c00e2655d703c397756616`.
Measurement-only follow-up authorized by the user. Production, actor, fixture,
dependency and maintained test sources are unchanged after temporary probe removal.

## Verdict

The requested measurement pass is complete. All result/report comparisons pass.
Whole-endpoint typed diagnostics remain 18.70–20.96% more expensive than matched
SQL in instructions, down from the previously recorded 27.34–30.02%. Ordinary
queries in the maintained multi-entity actor improve in all six cases, and all
18 paired update samples consume fewer charged cycles. These finite fixtures
do not establish a general application or schema-size guarantee.

The unchanged local diagnostic gate still fails four warm equality/sort samples.
Recommend accepting the measured diagnostic trade-off and bounded coverage at
closeout; this is a recommendation, not user acceptance or a passing gate.
No further optimization is authorized by these results.

## Whole-endpoint explain instructions

Reuse the unchanged [whole-call probe](../01/whole-call-probe.rs.txt). Each
endpoint performs three explains/renders: cold, warm, warm within one request.
Four epochs of thirteen distinct callers avoid query-result-cache reuse;
management statistics report 39 calls from three completed epochs per case.
All 468 executed calls return matching reports across typed/SQL/mixed actors;
351 are represented in these statistics. Do not divide these costs by three
and call the result an isolated cold or warm query.

| Query | Typed instructions/endpoint | SQL | Mixed typed | Typed vs SQL |
| --- | ---: | ---: | ---: | ---: |
| Primary-key equality | 5,073,893 | 4,213,634 | 5,099,121 | +20.42% |
| Scan/sort | 5,117,221 | 4,230,597 | 5,142,511 | +20.96% |
| Grouped COUNT | 5,296,537 | 4,461,991 | 5,321,354 | +18.70% |

[Raw totals](whole-explain.csv). Typed whole-endpoint instructions fall
9.00–9.37% against receipt 01's historical totals; those historical actors were
not rerun here. Current typed/SQL actors are freshly matched. Charged cycles
for diagnostic query messages are not measured; update-cycle measurements below
are a different workload and must not be substituted.

The maintained local gate was run once on the rebuilt actors. It compares typed
planning against SQL's render-inclusive total plus 5%; all reports and the mixed
actor's ordinary SQL read pass, but warm equality/sort still miss. Full
[local samples](local-samples.txt) retain this distinction. No threshold changes.

## Maintained multi-entity workload

Reuse the unmodified `canister_audit_sql_perf` actor, six-row `PerfAuditUser`
dataset and six queries from the [existing runner](../../catalog-selection/03/probe.rs.txt).
The actor includes other registered entities; this is not a one-entity schema.
The baseline is the hash-verified landed artifact from
[catalog receipt 06](../../catalog-selection/06/report.md), remeasured using the
same current host as 0.257.22. This is a cumulative artifact comparison, not
isolated attribution to the checksum change or typed binding optimization.

| Query | Baseline whole-query instructions | 0.257.22 | Change |
| --- | ---: | ---: | ---: |
| Primary key | 8,439,772 | 8,378,197 | −0.73% |
| Indexed equality | 9,289,987 | 9,189,202 | −1.08% |
| Indexed range, limit 3 | 9,158,555 | 9,096,541 | −0.68% |
| Primary-key IN | 8,684,027 | 8,617,966 | −0.76% |
| Count | 8,685,784 | 8,583,846 | −1.17% |
| Grouped count | 9,141,699 | 9,081,726 | −0.66% |

[Whole-query totals](whole-query.csv) cover 624 executed calls, 468 reported
calls across 12 fresh fixtures. Every result matches the update warm-up and
opposite artifact. The baseline totals reproduce the historical receipt exactly.

The separate phase probe uses twelve fresh fixtures and brackets three updates
per query/artifact with cycle balances, after settling deferred setup charges.
All 18 paired samples improve by **0.33–0.58% charged cycles**; every result
matches. [Complete phase/cycle samples](phases.csv) include all 36 updates.
These updates execute reads and retain caches; they are not query-message
cycles, mutation throughput or arbitrary production-workload evidence.

The phase probe also records first-after-setup and post-warm-up local query
counters in the local samples. Setup can already populate metadata caches, so
this is **not fully cold multi-entity startup**. One warmed indexed-range local
counter rises despite its whole-query improvement; use the whole-query numbers,
not partial-counter magnitudes, for this comparison.

## Raw Wasm and provenance

| Artifact | Raw post-link bytes | Defined functions |
| --- | ---: | ---: |
| Typed explain, 0.257.22 | 2,510,841 | 6,412 |
| SQL explain, 0.257.22 | 3,384,200 | 8,515 |
| Mixed typed + SQL, 0.257.22 | 3,408,318 | 8,599 |
| Multi-entity baseline | 4,478,067 | 10,487 |
| Multi-entity 0.257.22 | 4,479,557 | 10,491 |

Typed is 873,359 raw bytes smaller than matched SQL (25.81%). Multi-entity raw
Wasm grows 1,490 bytes and four functions versus its baseline. These are audit
actors, not composed Toko Miner savings. Published builds supersede candidate
build sizes for this measurement; do not attribute release-build differences
solely to the last six-line change. This measurement slice changes no runtime.

Rust 1.98.1; Binaryen 132; `wasm-release`, `wasm32-unknown-unknown`, locked/offline,
no default features. Typed actor features: `typed-explain-measurement`,
`sql-explain-measurement`, and `sql,typed-explain-measurement`. Multi-entity:
`test-admin-api,candid-export`. Post-link flags: `-Oz --enable-bulk-memory
--enable-sign-ext --enable-nontrapping-float-to-int
--one-caller-inline-max-function-size=0`.

Cargo.lock SHA-256: `50ad1354b6291d166efad3ab7816abf2e6ecc6a287973d07a449924f5a7b8f63`.
Its diff from published 0.257.21 contains only 48 workspace-version updates;
external locked dependencies are unchanged. [Artifact hashes](sha256.txt).
Logs and frozen artifacts: `target/closeout-25722-measurement/`.

Temporarily wire the linked explain probe beside `typed_explain_measurement`,
setting `ICYDB_EXPLAIN_WASM_DIR` to the artifact directory. Wire the linked
multi-entity runner beside `sql_perf_audit`, setting `ICYDB_QUERY_MEASUREMENT_DIR`
to `baseline.wasm` and `candidate.wasm`; select only its phase and whole-query
tests. The heap test is omitted. The only phase-runner addition is logging
`first.instructions` immediately after its first `query_perf` call. No actor
instrumentation or production endpoint is added.

## Validation and remaining decision

Three focused probes pass; the separately refreshed diagnostic cost gate fails
the same four samples. Thirty-six disposable PocketIC fixtures were created and
released; shared local networks were untouched. No native timing metric, full
repository suite or downstream application test was run. Temporary wiring was
removed; formatting, whitespace, invariants and all 41 checked local links pass.
The handoff changes ten documentation/evidence files, approximately 300 net
lines; zero retained code changes and neutral runtime complexity.

Remaining unmeasured scope is explicit: fully cold multi-entity startup,
diagnostic query charged cycles, arbitrary schema/data scale, non-explain and
SQL-only control rebuilds, and composed application attribution. Existing C1-N,
C1-M and other accepted deferrals remain; this does not prove complete resource
bounds. Recommend accepting this finite qualification scope alongside the
diagnostic cost trade-off. Both require the user's closeout decision; 0.258 is
not started and the checksum-table experiment remains unimplemented.

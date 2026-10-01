# 0.264.1 — Intersection fallback cost and partial-feature compilation

The maintained intersection policy already selects the planner-preferred first
index when exact overlap cannot help or its evidence is unavailable. Previously
it constructed every child stream, retained them in a vector and discarded all
but the first. The changed traversal owner constructs only the chosen stream.
It still consumes and validates every lowered prefix binding. Exact overlap
probes and their complete physical-prefix/Present-row checks are unchanged.

This is the smallest justified response to the dense-cost finding. It reuses
the existing fallback decision instead of adding entity-density thresholds,
full-scan alternatives, a route pin or configuration. Logical selection, read
admission, residual evaluation and continuation identity stay unchanged. Dense
index versus full-scan ranking remains a separate cost-policy question; this
change removes unused work rather than claiming to settle that question.

## Matched artifacts and measurement scope

Freeze the workspace source context after the user-published 0.264.0 boundary
and `lazy_static` lockfile update. Both artifacts include the same captured
parallel A3 cleanup and partial-feature gating context. Restore only
`executor/stream/access/traversal.rs` from `bdec3835d` for the baseline and use
the candidate traversal for the second build. The real implementation remains
in the workspace; the isolated checkout is for measurement only.

Further P2 gate repairs and the parallel A4 aggregate work arrived after the
freeze. P2 changes are test-only compilation differences for these Wasm inputs;
A4 is excluded from both measured modules. These are controlled traversal
measurements, not a final combined-release module qualification. Current
package versions are 0.264.0, Rust is 1.98.1, Binaryen is 132, SQL/Candid are
enabled and both use the same canonical local wasm-release pipeline and paths.
Candid bytes match. Do not compare these artifacts with older S1/S2 modules
built under different release/dependency contexts.

| Artifact | Raw Wasm bytes | SHA-256 |
| --- | ---: | --- |
| Baseline | 4,499,276 | `698f791aa6d3652941983c7b8b3dfa97d6cf09691b3a85dd133f3ce4ffd3ca28` |
| Candidate | 4,499,444 | `e26fd4ae2503004d918c716e6253dd8957caceb35c5cf427a6d3677e8cfb9ac6` |

Raw delta: **+168 bytes (+0.0037%)**. The existing unchanged host matrix uses
explicit frozen artifacts on PocketIC 16.0.0. Seven fresh actors per run cover
seven 160-row populations, two/three predicates and ASC/DESC. Fixture writes,
folds and deferred fees settle outside measurement intervals. Retain the same
SQL and dynamic endpoints/windows; work counters are correctness evidence,
not performance proxies.

Wide queries project 1 MiB payloads inside the instruction window, then discard
them before the compact ID/cursor/work reply. Whole-call cycles include that
compact reply, not full payload wire costs. Every issued cursor is independently
replayed against its exact expected suffix. Residual and authored-limit controls
run on nonwide populations.

## Warmed costs

Compare each second identical sample. Sum wide page costs over one complete
main traversal, excluding independent suffix-verification calls. Baseline and
candidate both have 38 main page steps and the same page boundaries.

Baseline, candidate and independent candidate-repeat runs pass. All 116
candidate costs repeat exactly; the [CSV](p1-costs.csv) retains 232 unique
baseline/candidate observations. Each run validates all ten issued tokens
against their complete expected suffixes.

| Workload | Shapes | Instruction change | Charged-cycle change |
| --- | ---: | ---: | ---: |
| Dense dynamic | 4 | −3.53% to −1.45% | −3.12% to −1.32% |
| Selective/disjoint dynamic | 16 | −16.96% to effectively unchanged | −7.24% to effectively unchanged |
| Complete wide traversal | 8 | −1.10% to effectively unchanged | −0.99% to effectively unchanged |
| SQL controls | 20 | −15.99% to effectively unchanged | −5.03% to effectively unchanged |

The largest dense reduction is three predicates, ASC: instructions decrease
from 26,997,729 to 26,045,058 and cycles from 35,655,390 to 34,541,793. A
near-identical wide control in each direction increases by two instructions/two cycles; retain
the exact receipt rather than claiming all shapes decrease. No mainnet or
sustained-write conclusion follows from these read measurements.

## Partial-feature compilation owner

The SQL-only native build reproduced 65 dead-code warnings. Migration execution,
planning, transform and validation modules were compiled under blanket `test`
gates even though their execution consumers require `migration`. Compile those
owners and their migration-only constructors/mutation helpers under that feature.
Keep current persisted migration records, marker/recovery operations and their
codec tests available in partial builds. No maintained test is deleted; planner
and transform tests run with their migration owner enabled.

Direct data-position publication and migration relation/index helpers use the
same feature boundary. The general recovered-row test helper retains its test
gate because no-default recovery/data-store tests consume it. Gates on exports
match their definitions. No blanket dead-code suppression is introduced, and
production feature behavior and persisted representations are unchanged.

The initial gate change exposed mismatched re-exports and an overly narrow
recovered-row helper; both were corrected. Further migration mutation methods
became unused once their test owner stopped compiling in SQL-only builds; their
gates now match that owner too. A transient formatting attempt overlapped the
creation of a parallel test module; formatting succeeded once the file existed.

## Validation and handoff

All three IC runs and exact repeat/receipt checks pass. The refreshed SQL-only
cardinality selection passes all 85 tests, including the six intersection tests.
Six persisted migration record tests pass in the no-default build and 24
migration-feature codec/planner/transform/lineage tests pass: 115 focused test
executions across these configurations, without warnings. Strict core lint passes
with all features, SQL-only and no-default configurations. Formatting, schema
authority, panic/format policy, documentation-reference and diff checks pass.

An earlier cardinality selection found two failures in parallel aggregate
fixtures; A4 corrected them to use maintained `MOD` syntax. The refreshed
85-test selection confirms both corrections. Three disposable PocketIC servers
were started and stopped. Full suites and Cargo bumps remain user-owned.

P1/P2 touch 20 owned files with approximately 450 added net lines, chiefly this
qualification, CSV receipts and release/status notes. The 15 Rust files grow
17 net lines: 16 in production traversal and one net feature-gate attribute.
No independent behavior axis is added; unused stream construction is removed
and migration-only test compilation follows its existing feature owner.

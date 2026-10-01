# P4 — Dense selection crossover and design decision

Authorized by continuation on 2026-10-01. This is the next bounded landing in
the [tracker](0.264-status.md), following the [P3 study](dense-selection.md).
The outcome is reproducible cost evidence and one build/no-build decision;
production scan selection is outside this landing.

## Bounded controls and ownership

Extend the maintained SQL-perf streaming
[fixture](../../../canisters/audit/sql_perf/src/seek_intersection.rs) and
[manual IC host test](../../../testing/integration/tests/seek_intersection.rs).
Keep the same schema, strict predicates, trusted page endpoint, projection and
page envelopes. Add finite fixture cases, not a new canister, benchmark framework,
public query option or production route. The expected-ID table is independent
of the loader's membership function. Run matched indexed/forced-scan artifacts
and independently repeat the scan. Qualify issued cursor suffixes separately
from measured traversal costs.

Cases 7–12 cover 16/80/112/128/144/160 matches out of 160 small rows: 10%, 50%,
70%, 80%, 90% and 100%. Cases 13/14 hold 80%/100% density with 32 KiB payloads;
15/16 hold 80%/100% density with 1 MiB payloads in a bounded 20-row population.
Case 17 has 512/640 small rows; 18/19 retain 128/160 with late/scattered matches.
Every row, including nonmatches, carries the case's payload width. Test ASC/DESC
and authored limits absent/1/5, with a three-predicate all-match control.

This bounded set isolates effects that density alone cannot describe without
creating a Cartesian product of all schema kinds, widths, predicates and limits.
Wider rows exercise the maintained id/payload projection; returned payloads are
discarded before compact wire replies. Samples report engine instructions and
whole-call cycles. Raw non-gzipped Wasm is the size metric; elapsed time and work
counters are not performance proxies.

## Matched artifacts and method

Freeze the current workspace Rust, manifests and dependency lock before building
both modules through the canonical local wasm-release pipeline. This pair
includes the completed P1/P2 and A4/A5 work; compare within this pair, not against
older modules that omit those changes. Rust 1.98.1, Binaryen 132, Cargo version
0.264.0, SQL and Candid export, accepted schema and populations are held fixed.

| Artifact | Raw Wasm bytes | SHA-256 |
| --- | ---: | --- |
| Indexed | 4,499,801 | `dca5f9eda839bc7d7ecd2f48b764da58fd06ad19997d5c715d9ea13904be99dc` |
| Counterfactual scan | 4,500,253 | `80999d58cc97b7192b6c71dafd5a341da5b914bf225ae9e8190408805787d4c5` |

Experimental raw delta: **+452 bytes (+0.010%)**. This measures the isolated
reselection wiring, not a maintained policy or the incremental fixture/release
footprint. Candid matches. Only the same two session query files used in P3 are
changed in the temporary counterfactual: pass current lane to the existing
selection owner, then finalize trusted intersections as FullScan. Strict
predicates, projection, order and execution primitives remain current. The
experimental files are restored and all 1,728 captured Rust/manifest inputs
match the workspace. A final rebuild after fixture lint fixes reproduces both
artifact hashes exactly. The implementation and fixtures live in the workspace;
only the counterfactual is isolated.

Compare the second identical sample of each page. Sum all main-page costs for
a complete traversal; exclude fixture setup, EXPLAIN and independent suffix
replays. Each run covers 84 query shapes and 92 main page steps, recording 184
instruction/cycle observations. Four wide, unlimited shapes span three pages;
every one of their eight issued tokens is checked against its exact suffix.
Wide controls project both id and payload; small controls project id only.
Width/projection comparisons therefore qualify maintained workloads, not a pure
row-width-only causal estimate. No covering-query or mainnet claim is made.

## Cost crossover

Negative deltas favor scanning. Ranges include ASC/DESC; the all-match small
control also includes two and three predicates. The full receipt is
[p4-costs.csv](p4-costs.csv). The independently repeated scan reproduces all
184 observations exactly, including the first and second samples.

| Complete result, no authored limit | Scan instruction change | Scan cycle change |
| --- | ---: | ---: |
| Small, 16/160 match (10%) | +192.66% to +194.68% | +87.93% to +88.50% |
| Small, 80/160 match (50%) | +25.06% to +25.89% | +16.72% to +17.55% |
| Small, 112/160 match (70%) | −0.32% to −0.26% | −0.24% to −0.20% |
| Small, 128/160 match (80%), early/late/scattered | −9.26% to −8.50% | −7.02% to −6.54% |
| Small, 144/160 match (90%) | −16.72% to −16.56% | −12.81% to −12.75% |
| Small, 160/160 match (100%) | −22.57% to −22.39% | −17.96% to −17.69% |
| Small, 512/640 match (80%) | −3.46% to −3.30% | −3.24% to −3.03% |
| 32 KiB payload projection, 128/160 match (80%) | +10.10% to +10.23% | +8.95% to +9.29% |
| 32 KiB payload projection, 160/160 match (100%) | −5.86% to −5.71% | −5.25% to −5.22% |
| 1 MiB payload projection, 16/20 match (80%) | −0.65% to +22.17% | −0.04% to +20.18% |
| 1 MiB payload projection, 20/20 match (100%) | +7.20% to +8.25% | +6.86% to +7.46% |

The apparent 70% crossover for small, unlimited controls saves only 0.20–0.24%
cycles before any new evidence/freshness cost. It is not a justified threshold.
Even all-match evidence does not guarantee a saving in the maintained payload
projection. Larger populations shrink the unlimited benefit at the same density.

| Short-result control | Scan cycle change |
| --- | ---: |
| 10% small, LIMIT 1, early matches ASC / DESC | −13.63% / +104.05% |
| 80% small, LIMIT 1, 160 rows, early matches ASC / DESC | −24.34% / −2.79% |
| 80% small, LIMIT 1, 160 rows, late matches ASC / DESC | −2.21% / −22.32% |
| 80% small, LIMIT 1, 160 rows, scattered ASC / DESC | −24.10% / −22.75% |
| 80% small, LIMIT 1, 640 rows, early matches ASC / DESC | −22.88% / +81.37% |
| 80% 32 KiB payload projection, LIMIT 1, ASC / DESC | −2.47% / +54.93% |
| 100% 1 MiB payload projection, LIMIT 1, ASC / DESC | +56.48% / +38.51% |

LIMIT 5 retains the same placement/population sensitivity: for 512/640 early
matches, ASC saves 22.83% cycles while DESC adds 79.66%. Exact counts do not
encode the distance to the first matches in the requested order. Across all 84
shapes, scan deltas range from −55.52% to +283.20% instructions and −24.46% to
+104.05% cycles. Both directions return the exact authored result; entry counters
confirm traversal but are not a performance proxy.

These counterfactual costs exclude an implemented cost selector, its exact
evidence refresh, cache-key change and selected-route authentication overhead.
Those costs remain unmeasured, as does final combined-release size.

## Design decision boundary

Keep public indexed-read admission unchanged. Any candidate scan policy must
be decided once in planning and projected into admission, EXPLAIN, physical
execution, cache identity/freshness and authenticated continuation. Reuse exact
visible entity/prefix counts and existing execution primitives. Do not introduce
a fake index pin, new persisted statistics, caller configuration or an
executor-local classifier.

Before selecting a rule, compare gains with evidence/preparation cost and check
whether placement, limit and width reverse the decision for equal counts.
If counts alone cannot support an affordable automatic rule, retain current
selection and record a no-build conclusion rather than fit another threshold.

## One selection contract, if later justified

The simplest current alternative is to keep indexed selection. A future cost
selector would extend the existing access-choice/cardinality owner and canonical
plan finalizer, not add another executor classifier. The following is one
contract, not independently selectable modes:

| Boundary | Canonical responsibility |
| --- | --- |
| Eligibility | The existing read-admission policy supplies whether a primary scan is allowed before candidate ranking. Public indexed reads retain their current rule. Derive cache identity from that capability, not diagnostic accounting or caller cache-fill order. |
| Evidence | The registry returns exact visible entity and eligible prefix counts under the same admitted database incarnation, accepted root and cardinality lifecycle. Retain indexed selection when evidence is unavailable or shape/probe budgets fail. Do not scan rows to manufacture statistics. |
| Selection | The access-choice/cardinality owner ranks only routes that preserve predicates, ordering, projection and execution budgets. The plan finalizer publishes one selected route to admission, EXPLAIN and physical execution. |
| Initial-request freshness | Cached structural preparation may remain reusable; a population-sensitive choice must recheck existing mutation/publication authority after ordinary writes, including retained SQL templates. Use an existing store/entity revision conservatively before considering a new persisted stamp. |
| Continuation | An authenticated selected-route identity represents an actual index or primary scan directly. A resumed page validates admission and accepted authority, then honors that route rather than reranking against new counts. Replace the current version-1 shape in place; no fake index, second pin or predecessor decoder. |

The demonstrated need would be a repeatable net saving after evidence and
freshness costs, across the eligible maintained query families. The state-space
change would couple admission eligibility, indexed/primary route identity and
fresh-initial/pinned-resume preparation with schema/index lifecycle and writes.
No new statistics, configuration, public query option, execution primitive or
persisted state is warranted. Implementing any of those distinctions without a
qualified rule would add complexity without a bounded benefit.

## Verdict — keep indexed selection

**No build of automatic scan selection in 0.264.1.** Keep the existing indexed
plan and the completed P1 fallback improvement. Counts alone do not choose
consistently across limits, population, match placement and width/projection.
Restricting to a convenient subset would fit these fixtures; broadening a cost
model would require more evidence and the three coupled ownership changes above.
The current measured benefit does not establish an affordable general rule.

This closes the P4 decision rather than leaving an unfinished runtime feature.
The maintained fixture and manual test preserve reproducible evidence for a
future explicitly authorized design. No production heuristic, cache axis,
cursor format, persisted state or public scan admission change lands here.
A future proposal must demonstrate net savings with evidence/freshness costs,
covering and adjacent eligible query families before reopening implementation.
The next generic continuation is the current line's read-only closeout audit.

## Validation and footprint

Indexed and scan crossover runs pass: exact answers, ASC/DESC, authored limits,
identical page boundaries and all issued suffixes. The focused original sparse/
rotated polling controls also pass after sharing the page/suffix protocol.
The independent scan repeat passes with all 184 costs reproduced exactly.
Focused host compilation and strict host/all-feature actor lint pass; duplicate expected arms and missing fixture
`const fn` findings were fixed, with required `make clippy` recovery passing.
Authority, schema, format and read-admission guards pass. Documentation
references, formatting and diff gates pass. Full repository suites are user-owned.

The two audit/test Rust files add 100 net lines, sharing the existing measurement
protocol rather than adding a second implementation. Eight files change,
approximately 710 added net lines, chiefly design and 368 CSV observations.
Status and active release notes complete the handoff. Library runtime complexity
stays unchanged; fixture coverage grows. Four disposable PocketIC servers were
started and stopped; no local ICP network was changed. Cargo versions remain
unchanged.

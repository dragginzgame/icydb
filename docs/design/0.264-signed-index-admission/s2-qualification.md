# S2 — Complete signed-query IC qualification and scoped closeout

The maintained IC matrix qualifies signed equality admission across seven
populations, two/three predicates and ASC/DESC. Exact results, repeated page
work, every issued cursor suffix, residuals and authored limits pass. Selective
queries benefit; dense matches expose an existing planner tradeoff. No new
executor route or planner tuning is included in this qualification.

## Controlled artifacts and protocol

Reuse the two frozen [S1 artifacts and build context](s1-qualification.md).
Baseline raw Wasm is 4,501,195 bytes; candidate is 4,501,556 bytes: **+361 bytes
(+0.008%)**. Their SHA-256 identities are respectively
`b6b5e86d012668157b0ba508108ff889c90cc408d646b68a3f1d75fc88d62ee8`
and `6e5140ce179087c952b40cec63d8ea428cc3f5bbded5525afec579c8d530349b`.
Only the signed normalizer differs between these matched builds. Cargo versions
are 0.263.0, Rust is 1.98.1, Binaryen is 132, SQL/Candid are enabled and the
canonical local wasm-release pipeline trims build paths identically.

The candidate source and manifests matched the workspace at S2 entry. Further
parallel A2 cleanup arrived during qualification; it remains outside these
frozen artifacts. The signed implementation and direct native fixtures remain
unchanged. A1 is held constant in both artifacts; neither A1 nor A2 has an
isolated cost delta here. Raw bytes above describe this controlled normalizer
comparison, not a final combined-release module measurement.

Use the existing `current_path_wasm_cost_matrix` host test and SQL-perf actor on
PocketIC 16.0.0. Install a fresh actor per population, with 160 rows and the same
accepted schema. Load four rows per write; wide loads settle each batch.
Writes, folds and deferred fees settle outside each sample interval. Populations
are spaced sparse (0), disjoint (1), late overlap (2), dense (3), wide late (4),
rotated late (5), and wide rotated (6).

Each nonwide shape has two SQL samples and two dynamic page samples. Each wide
page also has two samples, including resumed pages. Every engine-issued token
is independently replayed to exhaustion and checked against the exact suffix
of an independent answer table. The small shapes additionally check residual
filtering and authored limits 1 and 5. SQL EXPLAIN confirms logical intersection
selection; native accepted-catalog tests establish actual signed index admission.
Physical work counters check maintained behavior and are not performance metrics.

Wide rows carry 1 MiB payloads. Engine projection of id/payload is inside the
instruction window; the endpoint then discards payloads and replies with compact
IDs, work and continuation. Whole-call charged cycles therefore include that
compact reply, not a full payload wire reply. Instructions and cycles measure
different existing windows; do not infer one from the other.

## Matched results

Baseline, candidate and independent candidate-repeat runs pass. Each records
38 main page steps and 40 SQL observations: 116 cost samples per artifact.
All 116 candidate samples repeat exactly. The [CSV](s2-costs.csv) retains
232 before/after records; the earlier S1's 64 records reproduce exactly.
Every run checks ten issued cursor tokens against their complete suffixes.

Use the second identical sample at each endpoint/page. For wide shapes sum those
samples over the complete main traversal; exclude independent suffix-verification
calls from traversal costs. Boundaries match: wide late two-predicate queries
return 7/7/2 rows; the other six wide shapes return 7/5 rows.

| Dynamic workload | Shapes | Instruction change | Charged-cycle change |
| --- | ---: | ---: | ---: |
| Selective/disjoint small rows | 16 | −74.0% to −48.6% | −52.3% to −34.7% |
| Dense small rows (128/160 match) | 4 | +10.5% to +11.3% | +7.5% to +8.3% |
| Complete wide-row traversal | 8 | −29.0% to −16.6% | −26.9% to −15.2% |

| Representative dynamic shape | Instructions before → after | Cycles before → after |
| --- | ---: | ---: |
| Disjoint, three predicates, DESC | 20,938,613 → 6,086,846 | 29,499,112 → 14,805,250 |
| Dense, two predicates, DESC | 23,654,989 → 26,336,714 | 32,308,194 → 34,992,080 |
| Wide late, two predicates, ASC, all three pages | 323,445,922 → 230,120,444 | 350,469,458 → 257,305,493 |
| Wide rotated, three predicates, ASC, both pages | 210,415,873 → 175,516,847 | 227,186,659 → 192,608,705 |

Twenty warmed SQL controls range from −3.84% to +5.66% instructions and −0.08%
to +0.84% cycles. Five are cost-identical. Maximum instruction increase is late
overlap, three predicates, ASC; maximum cycle increase is rotated late, two
predicates, DESC. Costs are interleaved with dynamic queries, so these controls
do not isolate a SQL-only mechanism. No uniform SQL benefit is claimed.

Dense regressions are explicit measured tradeoffs. Index eligibility establishes
semantic validity, not that indexed execution always beats a scan. A follow-up
can assess dense selection in the existing planner using matched IC costs;
adding heuristics or routes is outside this line's normalization outcome.

## Owner and documentation audit

Accepted field contracts and query kinds remain the proof authority. Only
positive Eq/In over signed 8–64-bit Int64 atoms narrow; relation unwrapping and
every inspected membership atom use the existing preparation budget. No runtime
generated-model reconstruction or unaccepted-field proof was added. Null/mixed
operands, wider kinds, collections, field comparisons and ranges remain unproved.

Key-item matching and index literal compilation still require their maintained
coercion contracts. Index lifecycle visibility, cardinality admission and request
budgets remain unchanged. The existing cursorless exact-prefix probe bounds two
or three children to 256 total entries and validates every physical prefix and
Present row witness. Continuations retain ordinary ordered traversal; no seek
protocol, fallback decoder or cursor representation is introduced.

The active query contract now explains normalization versus schema-agnostic
intent and index selection. Design/status and current release notes reconcile
the two completed signed-query slices. Published 0.263 notes and historical
measurement receipts remain evidence of their original source. No further code
correction was identified in the signed admission scope.

## Validation and handoff boundary

All three focused IC runs, exact repeat/receipt comparisons, formatting,
authority, persisted-format policy, documentation-reference and diff checks
pass. The unchanged signed source retains S1's 120 focused native tests and
strict lint qualification. Three disposable PocketIC servers were started and
stopped. No validation failure remains in S2.

The parallel A2 cleanup has its own passing validation and completion receipts
in the [tracker](0.264-status.md); its implementation is not in the frozen
measurement builds. S2's footprint is eight documentation/receipt files,
approximately +425 net lines, with no Rust changes. Execution shape is unchanged;
documentation grows to retain
the broader evidence and tradeoffs.

The authorized line is scoped-ready with S1/S2/A1/A2 complete. Full repository
suites, Cargo version bumps and publication remain user-owned.
Mainnet costs, sustained write throughput and a final combined-release raw module
are unmeasured by this qualification.

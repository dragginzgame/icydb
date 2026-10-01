# S3 — Full qualification and 0.263 closeout

**Verdict: scoped-ready.** All three planned landings are complete. Ordinary
polling is the maintained stream contract; the dormant seek prototype is
retired. No further implementation is required for this line's chosen outcome.
Full release validation, version mutation and publication remain user-owned.

## Full matched IC qualification

Reuse the unchanged S1/S2 runner across all seven frozen populations and 28
two/three-child directional shapes. Cover spaced, disjoint, late, rotated and
dense populations; real payload-driven pages; residuals and authored total
limits. Exact expected outputs, repeated page work and all ten issued cursor
transitions' suffixes pass on both artifacts. Cursors are engine-issued and
replayed within their own canister; no cross-artifact cursor compatibility is
claimed. Small total limits remain distinct from page-size controls.

Each full run records 40 SQL samples and 38 twice-measured page steps: 116
instruction/cycle samples. Baseline, candidate and an independent full candidate
repeat pass, measuring 348 samples. Baseline reproduces S1 exactly; every
candidate sample reproduces exactly in the repeat, including all S2 controls.
The [232 before/after receipts](s3-costs.csv) retain each artifact's costs once;
the identical repeat does not duplicate those rows.

Artifact identity and configuration are unchanged from the [S2 receipt](s2-decision.md):
canonical SQL-perf wasm-release, SQL/Candid on, local profile, Cargo 0.262.2,
Rust 1.98.1 and PocketIC 16.0.0. Frozen baseline is 4,504,970 raw bytes;
candidate is 4,500,351: **−4,619 bytes (−0.10%)**. Staged candidate and frozen
candidate bytes agree; Candid remains identical. No rebuild or source change
was needed for this qualification.

Instructions retain each endpoint's counter window; cycles are whole-update
balance debits after settling. Setup, writes, folds and deferred charges stay
outside samples. Payload projection is measured inside execution, then the
endpoint returns compact IDs/work/cursor data. Full payload reply wire cost is
not measured. SQL and dynamic counter windows differ; no frontend ratio is
inferred. These are query costs, not write-throughput or mainnet measurements.

The table compares second identical samples, candidate minus baseline. Ranges
span the individual shapes/page steps rather than aggregate throughput.

| Surface | Shapes / page steps | Instruction delta | Charged cycle delta |
| --- | ---: | ---: | ---: |
| SQL small/dense controls | 20 | −278,804 to +145,069 | −69,317 to +6,365 |
| Dynamic small/dense scan pages | 20 | −100,021 to +122,629 | −87,095 to +63,554 |
| Dynamic payload scan pages | 18 | −2,979 to −1,946 | −2,979 to −1,946 |

Ten SQL shapes are cost-identical. Eighteen of twenty small/dense dynamic page
shapes and every payload page use fewer instructions/cycles. Mixed controls
remain explicit: the largest second-sample instruction increase is **2.56%**
(late three-child SQL DESC; its charged cycles decrease). The largest cycle
increase is **0.21%** (spaced three-child dynamic ASC); SQL's largest cycle
increase is 0.02%. These reproducible deltas remain unattributed. Retirement
is accepted for reduced maintained state and raw size, with the recorded cost
tradeoffs; no general query-speed or integrated-seeking improvement is claimed.

## Final authority and call-graph audit

| Current owner | Maintained boundary |
| --- | --- |
| [Physical leaves](../../../crates/icydb-core/src/db/executor/stream/access/physical.rs) | Concrete bounded polling, page refill bounds, authenticated primary resume and index suffix anchors |
| [Ordered contracts](../../../crates/icydb-core/src/db/executor/stream/key/contracts.rs) and [composites](../../../crates/icydb-core/src/db/executor/stream/key/composite.rs) | One polling contract, ordered alignment/deduplication, observers and budget forwarding |
| [Traversal](../../../crates/icydb-core/src/db/executor/stream/access/traversal.rs) | Accepted-root admission of two/three exact prefixes and at most 256 entries; cursorless direct probing excluded by continuation boundaries |
| [Structural scans](../../../crates/icydb-core/src/db/executor/stream/access/scan.rs) | Complete-prefix count agreement and Present row witnesses; no skipped integrity work |

The production/test/script call graph has no remaining retired protocol,
adapter, pending-target state or seek-only accounting references. Composite
lookahead heads remain necessary to ordinary alignment and are owned there.
No new actionable finding remains in the 0.263 slice; no compatibility path,
planner widening, public API, persisted state or cursor format was added.

Eligibility remains a qualification limit: the signed IC dynamic fixture scans;
SQL strict predicates select logical intersections. Native unsigned fixtures
qualify admitted intersections and their resumed polling. **Resumed admitted-
intersection IC costs remain unqualified**, and no mainnet cost is established.
A schema-bound signed secondary lookup decision is an independent follow-up;
any future seek proposal needs an admitted IC workload and justified savings.

## Validation and handoff

All three full PocketIC runs pass. The unchanged source retains S2's 143 passing
focused native tests and core/integration Clippy receipts; they were not rerun
without a source change. Formatting readiness, layer authority, documentation
links, frozen artifact identity and diff checks pass. Full repository/workspace
suites were skipped under the agent rules. Three disposable PocketIC servers
were started and stopped; no ICP network was changed.

S3 changes only documentation and cost receipts. Runtime architecture/state-space
are unchanged; the line retains S2's simpler polling implementation. Prior dirty
worktree contents and Cargo versions are preserved. No commit, push or new
minor-version work was performed.

S3 footprint: six files, approximately +390 documentation/receipt lines. No
runtime code, behavior axis or test harness is added; implementation shape is
unchanged from S2's simpler polling path.

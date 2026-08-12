# Post-0.224 Design Programme Disposition

Status: documentation coordination only; not implementation authority

Reorganization cut: 2026-08-12

This map records how the former proposed `0.226 Continuous Journal Convergence
And Explicit Startup Readiness` umbrella was split. The focused design in each
numbered directory is the only proposed authority for that line. This file
cannot authorize implementation or satisfy a predecessor closeout.

The former `0.225 Production Ledger Query Capability Audit` has moved to the
non-authoritative ideas area as the
[production-ledger query capability intake](production-ledger-query-capability-intake.md).
Its current dispositions are maintained separately in the
[query-capability roadmap](query-capability-roadmap.md).

## Immediate Dependency Order

```text
reported ready/complete 0.224 closeout
        |
        v
0.225 Explicit Startup Readiness And Replicated Recovery Driving
        |
        v
0.226 Digest-Bound Journal Validation And Convergence Bounds
        |
        v
0.227 Continuous Journal Convergence And Bounded Backlog Admission
        |
        v
0.228 Durable Exact Cardinality Generations
        |
        v
query-capability roadmap beginning provisionally at 0.229
```

Numeric order is also the implementation authorization order. A later document
does not authorize crossing an unfinished minor boundary.

## Focused Design Authorities

| Line | Sole proposed outcome | Explicit exclusions |
| --- | --- | --- |
| [0.225](../0.225-explicit-startup-readiness-and-replicated-recovery-driving/0.225-design.md) | Two-state startup readiness, dedicated pending error, no incidental query/`db!()` recovery, dedicated replicated driver, single-flight/trap/timer/application contract | New cursor, online convergence, backlog, cardinality |
| [0.226](../0.226-digest-bound-journal-validation-and-convergence-bounds/0.226-design.md) | Raw inspection, digest-bound durable Validate/Apply, exact interruption safety, truthful physical/instruction/fanout/overshoot bounds | Online scheduling, backlog pressure, cardinality |
| [0.227](../0.227-continuous-journal-convergence-and-bounded-backlog-admission/0.227-design.md) | Online reuse of 0.226, journal-positioned overlay visibility, post-commit scheduling, exact tail aggregates, fixed backlog ceiling and pre-marker pressure | Cardinality and planner statistics |
| [0.228](../0.228-durable-exact-cardinality-generations/0.228-design.md) | Exact entity/index-prefix generations, bounded populated build, publication/invalidation/incremental maintenance, conservative fallback | Approximate statistics, histograms, optimizer work |

## Exact Former-Section Disposition

The rows below cover every top-level and named subsection of the retired
umbrella. Shared project constraints are labelled as constraints rather than
assigned a second semantic owner.

| Former umbrella section | Disposition |
| --- | --- |
| Title, status, predecessor, and related authority | Replaced by this programme map and the independent planning/predecessor gates in each of 0.225–0.228. The stale requirement for a completed query-audit 0.225 was deleted. |
| `Planning Status And Authorization` | Replaced by line-local authorization gates: 0.225 follows 0.224; each later immediate line follows the preceding accepted closeout. |
| `Decision Summary` — readiness state, pending error, query/`db!()` behavior, driver, and application contract | 0.225 sole authority. |
| `Decision Summary` — durable validation/apply and truthful bounds | 0.226 sole authority. |
| `Decision Summary` — post-commit continuous convergence and fixed debt envelope | 0.227 sole authority; it reuses 0.225 scheduling and the 0.226 engine. |
| `Decision Summary` — durable exact cardinality and populated build | 0.228 sole authority. |
| `User Outcome` — explicit readiness, cheap pending calls, timer separation, and application restoration | 0.225. |
| `User Outcome` — exact interrupted convergence and corruption safety | 0.226. |
| `User Outcome` — bounded steady-state debt and future startup pages | 0.227. |
| `User Outcome` — exact populated-store planning evidence | 0.228. |
| `Incident And Current Limitation` — 16,715-batch startup/query/timer incident | 0.225 retains only startup/readiness/timer evidence; 0.227 retains the tail-history fact only for debt convergence. |
| `Incident And Current Limitation` — late-record validation after earlier apply | 0.226. |
| `Incident And Current Limitation` — cardinality unavailable after populated reopen | 0.228. |
| `Incident And Current Limitation` — post-decode 8 MiB accounting | 0.226. |
| Resolved 0.222.3 accepted-enum corruption evidence | Preserved as predecessor/fixture provenance in the intake and historical 0.223 records; not an acceptance outcome of any new line. |
| `No-Build And Alternatives Gate` / `Demonstrated need` | Split into each line's local no-build and complexity gate. No umbrella necessity claim remains. |
| `Rejected: raise the upgrade instruction limit` | 0.225 for readiness/caller cost and 0.226 for truthful page safety; shared external-limit context, not a second feature owner. |
| `Rejected: keep startup-only resumable folding as the final design` | 0.227. |
| `Rejected: restore automatic whole-store rebuild` | 0.225 excludes startup rebuild; 0.228 excludes reconstruction and owns only a separate bounded evidence build. |
| `Rejected: synchronously fold every complete commit before returning` | 0.227. |
| `Rejected: caller-selected compaction or recovery tuning` | Programme-wide hard constraint, enforced by the relevant state-space/complexity gates in 0.225–0.227. |
| `Rejected: reconstruct cardinality from generated models` | 0.228. |
| `Canonical Authority And Ownership` — accepted schema, marker, journal, canonical stores, watermark | Existing maintained authority preserved; 0.226 owns convergence ordering and 0.227 reuses it. |
| `Canonical Authority And Ownership` — generated timer scheduling | 0.225 owns the driver mechanism; 0.227 may request it after commit but adds no scheduler. |
| `Canonical Authority And Ownership` — cardinality generations | 0.228; future 0.234 must extend this owner. |
| `Steady-State Journal Convergence` / `Commit relationship` | 0.227. |
| `Steady-State Journal Convergence` / `Online fold` | 0.227, using the 0.226 engine. The former unexplained overlay retention is made explicit as journal-positioned retirement. |
| `Steady-State Journal Convergence` / `Backlog admission` | 0.227. |
| `Explicit Startup And Readiness` / `Startup probe` | 0.225. |
| `Explicit Startup And Readiness` / `Application contract` | 0.225. |
| `Explicit Startup And Readiness` / `Driver behavior` | 0.225. |
| `Two-Phase Durable Batch Validation` / cursor | 0.226. |
| `Two-Phase Durable Batch Validation` / `Validate` | 0.226. |
| `Two-Phase Durable Batch Validation` / `Apply` | 0.226. |
| `Truthful Physical And Instruction Bounds` | 0.226. |
| `Durable Exact Cardinality` | 0.228. |
| `Durable Exact Cardinality` / `Existing populated stores` | 0.228. |
| `State-Space Delta` — readiness row | 0.225. |
| `State-Space Delta` — convergence phase row | 0.226. |
| `State-Space Delta` — backlog admission row | 0.227. |
| `State-Space Delta` — cardinality generation row | 0.228. |
| `Persisted And Public Hard Cuts` — readiness/error/`db!()` | 0.225. |
| `Persisted And Public Hard Cuts` — cursor/envelope and unfinished-cursor transition | 0.226. |
| `Persisted And Public Hard Cuts` — tail aggregates/overlay/pressure | 0.227. |
| `Persisted And Public Hard Cuts` — cardinality generation/build | 0.228. |
| Former 0.223 journal-tag warning | Remains historical/predecessor-line evidence. None of 0.225–0.228 owns a compatibility map or casual renumbering. |
| `Scope` | Replaced completely by the four non-overlapping line-local scope/non-goal sections. |
| `Retained Adjacent Toko Ingestion Feedback` | Removed from the immediate programme. Preserved as Candidate 9 evidence and conditional 0.237 promotion in the query-capability roadmap. |
| `Landing Plan` Patch 1 | Replaced by a distinct frozen current-state Patch 1 in every line. Each freezes only that line's owner and gates. |
| `Landing Plan` Patch 2 | Expanded into 0.225 Patches 2–5; 0.225 Patch 6 is cross-owner real-canister closeout. |
| `Landing Plan` Patch 3 | Expanded into 0.226 Patches 2–6; 0.226 Patch 7 is cross-owner real-canister closeout. |
| `Landing Plan` Patch 4 | Expanded into 0.227 Patches 2–6; 0.227 Patch 7 is cross-owner real-canister closeout. |
| `Landing Plan` Patch 5 | Expanded into 0.228 Patches 2–5; 0.228 Patch 6 is cross-owner real-canister closeout. |
| `Landing Plan` Patch 6 | Replaced by each line's final patch, containing only that line's cross-owner real-canister evidence and closeout. |
| `Baseline And Acceptance Measurements` | Split by measured invariant: startup/timers to 0.225; batch/cursor/fanout/overshoot to 0.226; write/tail/overlay/residual pages to 0.227; build/count/planner/storage to 0.228. Each line separately owns raw Wasm and complexity reporting. |
| `Validation Matrix` / `Startup and scheduling` | 0.225. |
| `Validation Matrix` / `Validation and apply` | 0.226. |
| `Validation Matrix` / `Online convergence` | 0.227. |
| `Validation Matrix` / `Cardinality` | 0.228. |
| `Validation Matrix` / `Real integration` | Split across the final patch of each line. Toko evidence is retained only when it tests the receiving line's invariant. |
| `Complexity And Maintenance Gate` | Replaced by four line-local demonstrated-need, simplest-alternative, canonical-owner, state-delta, and failure gates. |
| `Audit Finding Disposition` | Replaced by the section rows above and the four design scope tables; no finding remains owned by the retired umbrella. |
| `Promotion Gate` | Replaced by four sequential line-local promotion gates. The former umbrella cannot authorize any code work. |

## Former Invariant Disposition

| Former invariant | Current disposition |
| --- | --- |
| 1. Accepted schema snapshots remain authority | Programme-wide architecture constraint; restated in all affected designs without creating new ownership. |
| 2. One convergence engine owns online/startup folding | 0.226 owns the engine; 0.227 owns reuse. |
| 3. Normal commit retains marker/journal atomicity | Existing authority preserved and explicitly constrained by 0.227. |
| 4. New commits cannot exceed fixed debt | 0.227. |
| 5. `Ready` requires no recovery debt plus reconciliation | 0.225. |
| 6. Queries never execute recovery mutations | 0.225. |
| 7. Complete Validate precedes canonical mutation | 0.226. |
| 8. Cursor binds exact batch identity/phase/ordinal | 0.226. |
| 9. Partial Apply is hidden by live projection | Startup gating belongs to 0.225; online overlay visibility/retirement belongs to 0.227. |
| 10. Canonical row/index transitions advance together | Existing record-family semantics preserved and interruption proof owned by 0.226. |
| 11. Watermark never passes unapplied work | 0.226. |
| 12. Exact cardinality is consumed only when synchronized | 0.228. |
| 13. Missing cardinality is conservative | 0.228. |
| 14. Whole-store integrity stays explicit and outside startup | 0.225 startup non-goal; 0.228 build is explicitly evidence construction, not repair. |
| 15. Malformed state remains typed corruption | 0.226 for convergence; 0.228 rejects malformed derived evidence without changing row/index authority. |
| 16. Generated models never reconstruct runtime authority | Programme-wide architecture constraint. |
| 17. Production paths contain no panics | Enforced locally in every line for the production paths it changes. |
| 18. Raw Wasm is primary | Enforced by every line's measurement gate. |

## Authority Collision Resolution

Three collisions in the former programme are now resolved:

1. Scheduling is not owned twice: 0.225 owns the replicated driver and
   single-flight/trap policy; 0.227 only requests that driver after commit or
   pressure.
2. Convergence is not implemented twice: 0.226 owns raw inspection,
   Validate/Apply, cursor, watermark, and terminal safety; 0.227 reuses it for
   online operation.
3. Exact cardinality is not duplicated by future planner statistics: 0.228 is
   the generation/build/staleness authority; provisional 0.234 must extend it.

One missing detail was discovered in the umbrella: it required online folding
to preserve live overlays but did not own a bounded way to retire them while
writes remain continuous. 0.227 now owns exact journal-position provenance and
retirement, subject to Patch 1 confirming the smallest maintained shape.

## Approval Boundary

No unresolved choice blocks this documentation reorganization. Numeric limits,
whether 0.226 needs physical chunks within its one current digest envelope, and
the smallest exact overlay-position representation are evidence-dependent
Patch 1 decisions that require review at their respective promotion gates.
They are not authorization to implement now.

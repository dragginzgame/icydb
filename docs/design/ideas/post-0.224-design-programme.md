# Post-0.224 Design Programme Disposition

Status: documentation coordination only; not implementation authority

Reorganization cut: 2026-08-14

This map records how the former proposed `0.226 Continuous Journal Convergence
And Explicit Startup Readiness` umbrella was split. The focused design in each
numbered directory is the only proposed authority for that line. This file
cannot authorize implementation or satisfy a predecessor closeout.

The independent Explorer least-privilege read-authority finding, including its
framework/application production incident evidence, now owns 0.226. IcyDB's design remains
framework-neutral; no Canic design or release is its predecessor or promotion
gate. The separate framework-neutral lifecycle-participant need now owns 0.227
so read authority is not gated by lifecycle engineering. The three later
former-umbrella outcomes and every provisional roadmap slot have therefore
moved up by one minor number from the first reorganization, and by two from the
original umbrella allocation, without changing their substantive ownership.

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
0.226 Application-Scoped SQL And Schema Read Authority
        |
        v
0.227 Framework-Neutral IcyDB Lifecycle Participation
        |
        v
0.228 Fingerprint-Bound Journal Validation And Convergence Bounds
        |
        v
0.229 Continuous Journal Convergence And Bounded Backlog Admission
        |
        v
0.230 Durable Exact Cardinality Generations
        |
        v
query-capability roadmap beginning provisionally at 0.231
```

Numeric order is also the IcyDB implementation authorization order. Canic and
Explorer may independently improve their application-authorization and client
flows, but neither can block or authorize an IcyDB line. A later document does
not authorize crossing an unfinished minor boundary.

## Focused Design Authorities

| Line | Sole proposed outcome | Explicit exclusions |
| --- | --- | --- |
| [0.225](../0.225-explicit-startup-readiness-and-replicated-recovery-driving/0.225-design.md) | Two-state startup readiness, dedicated pending error, no incidental query/`db!()` recovery, dedicated replicated driver, single-flight/trap/timer/application contract | New cursor, online convergence, backlog, cardinality |
| [0.226](../0.226-application-scoped-sql-and-schema-read-authority/0.226-design.md) | `guard(path)` on the established authorization field, replacement-not-union, bounded maintained allowlist matching, distinct SQL/schema denial, unchanged read-only semantics/ABI, and empirical standard-method discovery | Public SQL, within-SQL policy, lifecycle participation, controller assignment, bound parameters/continuations, IcyDB auth storage, external authentication/session protocols, or endpoint metadata |
| [0.227](../0.227-framework-neutral-lifecycle-participation/0.227-design.md) | Root-only non-exporting IcyDB lifecycle participant, exact hidden callbacks, duplicate/trap/retained-Watchdog-claim safety, and framework-neutral composition evidence | Read authorization, external-framework lifecycle APIs, generic callback registry, persisted lifecycle state, or timer-provider migration |
| [0.228](../0.228-digest-bound-journal-validation-and-convergence-bounds/0.228-design.md) | IC-native raw inspection, full-batch validation, retained-versus-streaming same-message Apply, optional direct-offset paged fallback, existing typed startup-failure publication, actual message-boundary safety, truthful worst-feasible heap/stable/instruction/fanout/overshoot bounds, and universal 0.227 reinstall-only hard cut | Durable Validate or new physical record index unless separately promoted, online scheduling, backlog pressure, cardinality |
| [0.229](../0.229-continuous-journal-convergence-and-bounded-backlog-admission/0.229-design.md) | Online reuse of 0.228, journal-positioned overlay visibility, post-commit scheduling, exact tail aggregates, fixed backlog ceiling and pre-marker pressure | Cardinality and planner statistics |
| [0.230](../0.230-durable-exact-cardinality-generations/0.230-design.md) | Exact entity/index-prefix generations, bounded populated build, publication/invalidation/incremental maintenance, conservative fallback | Approximate statistics, histograms, optimizer work |

## Deferred Adjacent Design Notes

The [bounded application batch progress](bounded-application-batch-progress.md)
intake records the E273 gap for application validation plus typed writes. It is
unnumbered, does not alter the immediate 0.225-0.230 dependency order, and is
not implementation authority. A later current-surface audit must decide
whether the demonstrated workload narrows to provisional 0.239 bounded
idempotent ingestion or requires a separate roadmap disposition.

## Exact Former-Section Disposition

The rows below cover every top-level and named subsection of the retired
umbrella. Shared project constraints are labelled as constraints rather than
assigned a second semantic owner.

| Former umbrella section | Disposition |
| --- | --- |
| Title, status, predecessor, and related authority | Replaced by this programme map and the independent planning/predecessor gates in each immediate line. The stale requirement for a completed query-audit 0.225 was deleted. The newly inserted 0.226 read-authority and 0.227 lifecycle-participant lines are independent adjacent findings, not relocated umbrella sections. |
| `Planning Status And Authorization` | Replaced by line-local authorization gates: 0.225 follows 0.224; each later immediate line follows the preceding accepted closeout. |
| `Decision Summary` — readiness state, pending error, query/`db!()` behavior, driver, and application contract | 0.225 sole authority. |
| `Decision Summary` — complete pre-Apply validation and truthful IC bounds | 0.228 sole authority; Patch 1 selects one-shot validation unless measured evidence requires a new decision. |
| `Decision Summary` — post-commit continuous convergence and fixed debt envelope | 0.229 sole authority; it reuses 0.225 scheduling and the 0.228 engine. |
| `Decision Summary` — durable exact cardinality and populated build | 0.230 sole authority. |
| `User Outcome` — explicit readiness, cheap pending calls, timer separation, and application restoration | 0.225. |
| `User Outcome` — exact interrupted convergence and corruption safety | 0.228. |
| `User Outcome` — bounded steady-state debt and future startup pages | 0.229. |
| `User Outcome` — exact populated-store planning evidence | 0.230. |
| `Incident And Current Limitation` — 16,715-batch startup/query/timer incident | 0.225 retains only startup/readiness/timer evidence; 0.229 retains the tail-history fact only for debt convergence. |
| `Incident And Current Limitation` — late-record validation after earlier apply | 0.228. |
| `Incident And Current Limitation` — cardinality unavailable after populated reopen | 0.230. |
| `Incident And Current Limitation` — post-decode 8 MiB accounting | 0.228. |
| Resolved 0.222.3 accepted-enum corruption evidence | Preserved as predecessor/fixture provenance in the intake and historical 0.223 records; not an acceptance outcome of any new line. |
| `No-Build And Alternatives Gate` / `Demonstrated need` | Split into each line's local no-build and complexity gate. No umbrella necessity claim remains. |
| `Rejected: raise the upgrade instruction limit` | 0.225 for readiness/caller cost and 0.228 for truthful page safety; shared external-limit context, not a second feature owner. |
| `Rejected: keep startup-only resumable folding as the final design` | 0.229. |
| `Rejected: restore automatic whole-store rebuild` | 0.225 excludes startup rebuild; 0.230 excludes reconstruction and owns only a separate bounded evidence build. |
| `Rejected: synchronously fold every complete commit before returning` | 0.229. |
| `Rejected: caller-selected compaction or recovery tuning` | Programme-wide hard constraint, enforced by the relevant state-space/complexity gates in 0.225–0.229. |
| `Rejected: reconstruct cardinality from generated models` | 0.230. |
| `Canonical Authority And Ownership` — accepted schema, marker, journal, canonical stores, watermark | Existing maintained authority preserved; 0.228 owns convergence ordering and 0.229 reuses it. |
| `Canonical Authority And Ownership` — generated timer scheduling | 0.225 owns the driver mechanism; 0.229 may request it after commit but adds no scheduler. |
| `Canonical Authority And Ownership` — cardinality generations | 0.230; future 0.236 must extend this owner. |
| `Steady-State Journal Convergence` / `Commit relationship` | 0.229. |
| `Steady-State Journal Convergence` / `Online fold` | 0.229, using the 0.228 engine. The former unexplained overlay retention is made explicit as journal-positioned retirement. |
| `Steady-State Journal Convergence` / `Backlog admission` | 0.229. |
| `Explicit Startup And Readiness` / `Startup probe` | 0.225. |
| `Explicit Startup And Readiness` / `Application contract` | 0.225. |
| `Explicit Startup And Readiness` / `Driver behavior` | 0.225. |
| `Two-Phase Durable Batch Validation` / cursor | Superseded by 0.228's cursor-free complete-batch Validate/Apply route and exact fold watermark. |
| `Two-Phase Durable Batch Validation` / `Validate` | 0.228 owns complete read-only validation before the first canonical mutation. |
| `Two-Phase Durable Batch Validation` / `Apply` | 0.228 owns one complete same-message mechanical Apply with no durable paging or convergence cursor. |
| `Truthful Physical And Instruction Bounds` | 0.228. |
| `Durable Exact Cardinality` | 0.230. |
| `Durable Exact Cardinality` / `Existing populated stores` | 0.230. |
| `State-Space Delta` — readiness row | 0.225. |
| `State-Space Delta` — convergence phase row | Deleted by 0.228's reduced default; no phase state is added unless separately authorized after measurement. |
| `State-Space Delta` — backlog admission row | 0.229. |
| `State-Space Delta` — cardinality generation row | 0.230. |
| `Persisted And Public Hard Cuts` — readiness/error/`db!()` | 0.225. |
| `Persisted And Public Hard Cuts` — cursor/envelope and unfinished-cursor transition | 0.228. |
| `Persisted And Public Hard Cuts` — tail aggregates/overlay/pressure | 0.229. |
| `Persisted And Public Hard Cuts` — cardinality generation/build | 0.230. |
| Former 0.223 journal-tag warning | Remains historical/predecessor-line evidence. None of 0.225–0.230 owns a compatibility map or casual renumbering. |
| `Scope` | Replaced completely by the four non-overlapping former-umbrella scope/non-goal sections. Independent 0.226 read authority and 0.227 lifecycle participation have separate scopes and do not absorb journal or cardinality work. |
| `Retained Adjacent reference application Ingestion Feedback` | Removed from the immediate programme. Preserved as Candidate 9 evidence and conditional 0.239 promotion in the query-capability roadmap. |
| `Landing Plan` Patch 1 | Replaced by a distinct frozen current-state Patch 1 in every line. Each freezes only that line's owner and gates. |
| `Landing Plan` Patch 2 | Expanded into 0.225 Patches 2–5; 0.225 Patch 6 is cross-owner real-canister closeout. |
| `Landing Plan` Patch 3 | Replaced by 0.228's measurement-first four-patch provisional line; only its no-build Patch 1 is authorized. |
| `Landing Plan` Patch 4 | Expanded into 0.229 Patches 2–6; 0.229 Patch 7 is cross-owner real-canister closeout. |
| `Landing Plan` Patch 5 | Expanded into 0.230 Patches 2–5; 0.230 Patch 6 is cross-owner real-canister closeout. |
| `Landing Plan` Patch 6 | Replaced by each line's final patch, containing only that line's cross-owner real-canister evidence and closeout. |
| `Baseline And Acceptance Measurements` | Split by measured invariant: startup/timers to 0.225; batch/cursor/fanout/overshoot to 0.228; write/tail/overlay/residual pages to 0.229; build/count/planner/storage to 0.230. Each line separately owns raw Wasm and complexity reporting. |
| `Validation Matrix` / `Startup and scheduling` | 0.225. |
| `Validation Matrix` / `Validation and apply` | 0.228. |
| `Validation Matrix` / `Online convergence` | 0.229. |
| `Validation Matrix` / `Cardinality` | 0.230. |
| `Validation Matrix` / `Real integration` | Split across the final patch of each line. Reference-application evidence is retained only when it tests the receiving line's invariant. |
| `Complexity And Maintenance Gate` | Replaced by four former-umbrella line-local gates plus independent 0.226 read-authority and 0.227 lifecycle-participant need/alternative/owner/state-delta/failure gates. |
| `Audit Finding Disposition` | Replaced by the section rows above and six immediate design scope tables; no finding remains owned by the retired umbrella. |
| `Promotion Gate` | Replaced by six sequential immediate line-local promotion gates. The former umbrella and this programme map cannot authorize any code work. |

## Former Invariant Disposition

| Former invariant | Current disposition |
| --- | --- |
| 1. Accepted schema snapshots remain authority | Programme-wide architecture constraint; restated in all affected designs without creating new ownership. |
| 2. One convergence engine owns online/startup folding | 0.228 owns the engine; 0.229 owns reuse. |
| 3. Normal commit retains marker/journal atomicity | Existing authority preserved and explicitly constrained by 0.229. |
| 4. New commits cannot exceed fixed debt | 0.229. |
| 5. `Ready` requires no recovery debt plus reconciliation | 0.225. |
| 6. Queries never execute recovery mutations | 0.225. |
| 7. Complete Validate precedes canonical mutation | 0.228. |
| 8. Cursor binds exact batch identity/phase/ordinal | Superseded by 0.228's immutable complete-batch identity, preflighted Apply, and fold watermark; no convergence cursor or durable phase remains. |
| 9. Partial Apply is hidden by live projection | Startup gating belongs to 0.225; online overlay visibility/retirement belongs to 0.229. |
| 10. Canonical row/index transitions advance together | Existing record-family semantics preserved and interruption proof owned by 0.228. |
| 11. Watermark never passes unapplied work | 0.228. |
| 12. Exact cardinality is consumed only when synchronized | 0.230. |
| 13. Missing cardinality is conservative | 0.230. |
| 14. Whole-store integrity stays explicit and outside startup | 0.225 startup non-goal; 0.230 build is explicitly evidence construction, not repair. |
| 15. Malformed state remains typed corruption | 0.228 for convergence; 0.230 rejects malformed derived evidence without changing row/index authority. |
| 16. Generated models never reconstruct runtime authority | Programme-wide architecture constraint. |
| 17. Production paths contain no panics | Enforced locally in every line for the production paths it changes. |
| 18. Raw Wasm is primary | Enforced by every line's measurement gate. |

## Authority Collision Resolution

Three collisions in the former programme are now resolved:

1. Scheduling is not owned twice: 0.225 owns the replicated driver and
   single-flight/trap policy; 0.229 only requests that driver after commit or
   pressure.
2. Convergence is not implemented twice: 0.228 owns raw inspection, one-shot
   Validate, complete same-message Apply, watermark, and terminal safety with
   no convergence cursor; 0.229 reuses it for online operation.
3. Exact cardinality is not duplicated by future planner statistics: 0.230 is
   the generation/build/staleness authority; provisional 0.236 must extend it.

One missing detail was discovered in the umbrella: it required online folding
to preserve live overlays but did not own a bounded way to retire them while
writes remain continuous. 0.229 now owns exact journal-position provenance and
retirement, subject to Patch 1 confirming the smallest maintained shape.

## Approval Boundary

The documentation reorganization and 0.226 implementation/closeout are
complete. 0.226 proved the generated guard, froze the IcyDB-only
wrapper/allowlist/raw-Wasm gates, and retained empirical standard-method
probing without a second discovery artifact. No Canic ABI, release, tracker,
or lifecycle owner entered that line.

0.227 Patches 1-4 are accepted. They implement one hidden module containing two
synchronous callbacks plus one volatile duplicate latch, then prove empty and
populated same-release recovery under a framework-neutral single-owner root.
Canic composition remains independently owned and is not an IcyDB release
gate. The fully ready participant's narrow instruction headroom became the
focused but now historical
[0.237 accepted-schema runtime observation and cold-root scaling design](../0.237-accepted-schema-runtime-observation-and-cold-root-scaling/0.237-design.md),
which measured a 1.892% cold-read improvement against a frozen 25% gate and
closed as an unreleased no-build with its implementation reverted.
0.228 Patch 1 independently reproduced the same scaling direction: temporarily
co-locating an unrelated 64-index entity more than doubled one three-index
recovery probe. That historical evidence no longer justifies active 0.237 work;
future optimization requires a new baseline and separate design without
becoming a second recovery executor or changing 0.228's corruption protocol.
The later explicit 0.237 assignment belongs instead to the independent
[SQL query performance hotspot rediscovery design](../0.237-sql-query-performance-hotspot-rediscovery/0.237-design.md)
and does not revive the accepted-schema candidate.

For later lines, 0.228 first measures whether a complete batch can validate,
prepare, Apply, and retire in one IC message. An Apply cursor, durable Validate,
or physical-format expansion requires progressively stronger evidence and
separate authorization. The smallest exact 0.229 overlay-position
representation remains its own evidence-dependent Patch 1 decision. None of
these decisions authorize implementation now.

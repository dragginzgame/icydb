# Recurring Audit — Recovery Consistency And Replay Equivalence

## Identity And Scope

- Report scope: `recovery-consistency`
- Method: `RECOVERY-5 + DOMAIN-1`
- Report: `docs/reports/recurring/YYYY/MM/DD/recovery-consistency/<run>/report.md`

Apply [audit governance](../../README.md): scope, immutable reports, finding
ownership, shared evidence, authorization, and executed-test evidence. Freeze
this method before execution. Record the requested baseline or change trigger,
source snapshot including dirty inputs, selected storage capabilities and
mutation families, and exclusions with reasons.

This audit owns final-state equivalence, replay idempotence, interruption
recovery, and durable progress. State-transition integrity owns general legal
entry/publication sequencing; index integrity owns membership semantics; error
taxonomy owns diagnostic projection. Reuse their compatible proof without
copying findings or treating an adjacent PASS as recovery evidence.

A requested baseline samples every family below. It does not qualify every
operation, feature combination, platform failure, or unrelated stored object.
An unavailable required sample is a verification gap, not an exclusion.

## 1. Discover Authority And Define Equivalence

Follow current production callers rather than historical names. Starting owners
(relative to `crates/icydb-core/src/`) are:

| Family | Owners to inspect | Required baseline observation |
| --- | --- | --- |
| Live row mutation and durable commit | `db/session/write.rs`, `db/executor/mutation/commit_window.rs`, `db/commit/{prepare,guard}.rs` | Insert, update/replace, delete, index and reverse-relation effects |
| Marker replay and journal convergence | `db/commit/recovery.rs`, `db/journal/`, `db/commit/store/` | Direct replay versus journaled live projection/fold, retained marker, tail controls, watermark and retirement |
| Accepted schema publication | `db/commit/schema_publication.rs`, `db/schema/mutation/user_index_domain.rs`, `db/schema/store.rs` | Accepted root and derived index domain recovered together, including interrupted domain replacement |
| Physical migration and compound controls | `db/schema/application/`, `db/schema/application.rs`, `db/schema/migration*`, mutation-progress owners | Candidate generation/progress and receipt/lineage controls bound to the exact committed operation |
| Startup readiness and failure | `db/startup/`, `db/mod.rs` | Pure admission, bounded recovery driver, failure containment and final readiness |

Inventory the selected marker/journal record families and their concrete live
and replay entrypoints. Record whether a store is persistent, journaled, or
live-only; do not promise heap-only rows survive heap loss unless durable
operation payloads actually reconstruct them.

Define the observable state for each comparison before selecting tests:
accepted schema identity; rows and keys; exact index/reverse entries or an
independent membership oracle; identity high-water and mutation revisions;
control receipts/progress; marker state, journal tail, and fold watermark.
Distinguish logical live state from canonical state before convergence.

Equivalent final state does not require byte-identical transient layouts or
identical live/replay operation order. Name the safety dependencies that must
hold: accepted authority before interpretation, complete preflight before
protected writes, durable ownership before application, and verified effects
before retirement/readiness. Any ordering difference needs an explicit reason.

## 2. Trace Durable Ownership Through Live And Replay Paths

Produce one table: family, live flow, recovery flow, carried authority,
observable postcondition, deliberate difference, and evidence or gap.

Check the applicable boundaries:

- Before durable admission, rejected proposals leave protected storage unchanged.
- Marker publication binds the exact batches and database-control effects.
  In-process cleanup and volatile stages are not durable recovery authority.
- Successful live commits can retire the marker while committed journal tails
  remain. Tail controls and fold watermarks then own convergence; normal live
  visibility may remain admitted. Fresh startup must reconstruct readiness.
- Recovery distinguishes Replay, Fold, and Verify; stage loss must be safe at
  each selected cut. Already-folded batches must not be appended or consumed
  again merely because the marker remains.
- Fold order respects database commit order and per-tail sequence. Validate a
  complete selected batch before applying its first canonical effect; advance
  watermark and retire the batch only with its applied effects.
- Live and canonical accepted contracts must be selected intentionally when
  preparing row/index transitions. Generated metadata is not recovery authority.
- Identity ranges, mutation revisions, unique-value handoffs, and reverse
  relation changes must not be consumed twice or lose a predecessor effect.
- Verification checks marker-owned terminal effects and tail completion before
  clearing marker authority. Do not require an unrelated whole-database scan.
- A returned apply/clear error retains durable authority and its required
  recovery wake-up. Corruption remains fail-closed; a valid rejected proposal
  and a corrupted accepted state need not have the same class or origin.

Trace the actual production driver and callers. Test-only wrappers may call
recovery directly; do not mistake them for production admission paths.

## 3. Interruption And Idempotence Matrix

For each selected case, record cut, retained durable/volatile state, expected
recovered state, proof, and remaining limitation. A baseline includes:

1. Marker persisted before application.
2. Journal published before complete live effects.
3. A row/index/reverse-effect prefix applied.
4. All rows applied before marker retirement, and control-state materialization.
5. Successful marker retirement with committed tails still awaiting convergence.
6. Loss of volatile recovery stage after Replay and after a completed Fold.
7. Repeated replay of an already-applied effect while authority is retained.
8. Late malformed row/record rejection before canonical writes or watermark
   advancement; repeated verification failure retaining the admission barrier.

Cover direct and journaled paths where maintained, mixed-entity effects, a
unique-value handoff, and relation removal/replacement as well as creation.
Use exact state comparisons against uninterrupted execution where available;
otherwise name the independent expected-state assertions and their limits.
A second call after marker deletion proves quiescence, not retained-marker
idempotence. Checking only row counts does not prove index or reverse equality.

Distinguish native returned-error injection, loss of volatile stage, reopening
stable-memory fixtures, and actual IC traps/upgrades. Native proof cannot certify
platform rollback, timer delivery, or arbitrary process interruption mid-message.
Record an untested platform assumption rather than silently claiming it passed.

## 4. Schema And Derived-State Recovery

Trace accepted-before preflight, candidate publication, staged index deltas,
control receipts, and final accepted visibility using current source ordering.
Do not insist the accepted-root write is physically last when a guarded atomic
message can publish it earlier safely.

Require a populated user-index-domain publication sample interrupted after
marker persistence and during partial derived-state publication. Verify exact
accepted-after schema/index effects, unrelated-domain preservation, admission
until completion, and repeated recovery. Staging tests, journal codec round
trips, metadata-only rename replay, and row-update recovery do not independently
prove this publication path. Missing interruption proof remains a named gap.

For physical migration, sample exact-plan row rewrite interruption, isolated
candidate generations, final validation/publication, and a compound control
handoff. Identify plan/revision bindings and prove incomplete candidate state
cannot become ordinary accepted runtime authority. Scope deeper abort or nested
migration matrices to their changes; do not infer them from one rewrite test.

## 5. Focused Verification

Inspect current assertions and feature gates before listing and running tests
under [executed-test evidence](../../README.md#executed-test-evidence). Candidate
owners, not fixed selectors:

| Obligation | Candidate tests |
| --- | --- |
| Five maintained mixed-entity cuts and progress effects | `db/session/write.rs` |
| Direct/journaled replay, stage loss, uniqueness, partial predicates | `db/session/write/identity_pre_key_tests/replay_construction_tests/` and its parent module |
| Nested relation insert/replace/delete recovery | `db/session/write/identity_pre_key_tests/nested_relation_tests.rs` |
| Durable tail accounting, replay and reopen | `db/journal/store.rs` |
| Schema replay/fold, interrupted compound publication, domain staging | `db/schema/store/tests.rs`, `db/commit/schema_publication.rs`, `db/schema/mutation/tests/`; locate domain-publication interruption proof separately |
| Physical rewrite and publication/control handoff | `db/schema/application.rs` and its test modules |
| Malformed persisted input and retained failure | `db/commit/store/tests.rs`, `db/tests/persisted_format_corpus.rs`, session/startup tests |

Use focused core `--lib --features sql,migration` selections when both schema
surfaces are included. List and count each required selector with the execution
configuration; require all five mixed-entity cuts, not a single family match.
Full repository/workspace suites remain user-owned. Static authority checks can
support source conclusions but do not establish replay behavior.

## Report Contract And Method Change

Include metadata/baseline/comparability, the declared observable state and scope,
owner/flow table, interruption matrix, schema recovery evidence, findings and
verdict, exact verification commands/counts, and follow-up. Apply shared
severities and PASS / PASS WITH FINDINGS / FAIL / BLOCKED verdicts. Distinguish
missing evidence from demonstrated corruption or replay divergence.

RECOVERY-5 replaces Method V4's identical-order/sole-marker assumptions with
capability-aware state equivalence and explicit journal ownership. It requires
stage-loss proof, separates native from platform evidence, and retains the
populated index-publication interruption obligation. Comparisons to V4 are
non-comparable for these changes; affected deltas are N/A (method change).
Retain marker-before-apply and fail-closed admission as qualitative anchors.

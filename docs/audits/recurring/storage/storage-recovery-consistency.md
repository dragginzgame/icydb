# Recurring Audit: Recovery Consistency & Replay Equivalence

Apply [Domain Scope And Change Triggers](../../README.md#domain-scope-and-change-triggers)
to all inventories, checks, and output sections below. Record selected and
excluded obligations before analysis; broad coverage requires a requested baseline.

## Purpose

Verify that recovery replay and startup recovery produce exactly the same
structural state, invariants, and side effects as normal mutation execution and
accepted schema mutation publication.

Recovery must be:

* semantically equivalent
* invariant-equivalent
* mutation-order equivalent
* idempotent
* deterministic
* fail-closed before exposing partially recovered state

This audit does NOT evaluate:

* performance
* style
* refactors
* new features

Only correctness equivalence.

---

## Core Principle

For every mutation family:

> recovery replay must be indistinguishable from normal execution in final
> state, invariant guarantees, and durable marker behavior.

For schema mutation startup work:

> before marker authority, complete accepted-after index work must remain
> zero-write and accepted-before stays authoritative; after marker authority,
> guarded recovery must finish accepted-after schema and derived state forward
> before the index store can become Ready.

If replay and execution differ in:

* operation ordering
* validation ordering
* error classification
* reverse-index mutation
* index-entry construction
* marker lifecycle
* staged physical-store visibility
* accepted snapshot publication ordering

that is a correctness risk.

---

## Current Ownership Boundary

This audit must use the current live recovery boundary, not historical names.

Primary owners:

* `db/mod.rs`
  * `ensure_recovery_admitted`
* `db/startup/*`
  * pure readiness observation and the sole replicated page driver
* `db/commit/recovery.rs`
  * `continue_recovery`, called only by the startup driver
* `db/commit/guard.rs`
  * `begin_commit`
  * `begin_single_row_commit`
  * `finish_commit`
  * `CommitGuard`
  * `CommitApplyGuard`
* `db/commit/store/*`
  * commit-marker persistence and decode
* `db/executor/mutation/commit_window.rs`
  * commit-window open/apply orchestration
* save/delete mutation executors that feed prepared row ops into commit-window
  application
* recovery replay logic and accepted schema-transition replay surfaces when
  they reuse the same marker protocol
* `db/schema/reconcile.rs`
  * startup accepted snapshot reconciliation
* `db/schema/reconcile/user_index_domain.rs`
  * accepted-catalog row scan and zero-write startup/SQL staging adapter
* `db/schema/mutation/user_index_domain.rs`
  * complete accepted-before/accepted-after derived-index projection
  * bounded precommit validation and raw replacement payload
* `db/commit/schema_publication.rs`
  * marker-first accepted-schema and mechanical user-index-domain publication

Historical names such as `ensure_recovered_for_write` are obsolete and must not
be used as the audit frame.

---

## Scope

Analyze:

* `CommitMarker`
* `CommitRowOp`
* `PreparedRowCommitOp`
* `begin_commit`
* `begin_single_row_commit`
* `finish_commit`
* `ensure_recovery_admitted`
* generated lifecycle watchdog registration and startup-driver dispatch
* `continue_recovery`
* commit-window open/apply orchestration
* save / replace / delete executor flows
* reverse-relation index mutation
* index-entry mutation
* commit-marker persistence
* recovery replay logic
* schema mutation startup field-path index rebuild
* staged schema mutation physical-store work
* accepted schema snapshot publication after physical work
* startup fail-closed behavior for partial or mismatched physical work

---

## Required Analysis

### 1. Mutation Inventory

Enumerate the mutation types affected by the declared scope:

* insert
* replace
* delete
* reverse relation update
* index entry creation
* index entry removal
* commit marker transitions
* supported schema mutation field-path index rebuild
* accepted snapshot publication transition

Produce:

| Mutation Type | Normal Execution Entry Point | Recovery Entry Point |
| ------------- | ---------------------------- | -------------------- |

### 2. Side-by-Side Flow Comparison

For each mutation type, construct a side-by-side flow:

| Phase | Normal Execution | Recovery Replay | Identical? | Risk |
| ----- | ---------------- | --------------- | ---------- | ---- |

Phases must include:

1. pre-mutation invariant checks
2. referential integrity validation
3. unique constraint validation
4. reverse relation mutation
5. index entry mutation
6. store mutation
7. commit marker write / persistence
8. finalization / marker clear
9. staged physical-store validation
10. runtime invalidation
11. accepted snapshot publication

You must explicitly compare:

* operation ordering
* validation ordering
* error propagation behavior
* error classification type

### 3. Invariant Enforcement Parity

For each invariant relevant to mutation, compare:

* identity match
* key namespace
* index id consistency
* component arity
* reverse relation symmetry
* unique constraint enforcement
* expected-key vs decoded-key match

Produce:

| Invariant | Enforced in Normal | Enforced in Recovery | Enforced at Same Phase? | Risk |
| --------- | ------------------ | -------------------- | ----------------------- | ---- |

Flag:

* enforced only in normal path
* enforced only in recovery
* enforced in different phase ordering
* enforced after mutation in one path but before mutation in the other

### 4. Mutation Ordering Verification

Verify:

* reverse-index mutation occurs in the same relative place
* index mutation order is identical
* commit marker transitions occur in identical relative position
* no recovery path performs mutation earlier than validation
* success clears marker authority immediately
* failure preserves marker authority durably
* schema mutation physical-store publication occurs before accepted-after
  schema visibility
* accepted-after schema publication is blocked when row, schema, or physical
  store revalidation fails

Produce:

| Mutation | Normal Order | Recovery Order | Equivalent? | Risk |
| -------- | ------------ | -------------- | ----------- | ---- |

### 5. Error Classification Equivalence

For each failure scenario, compare classification:

* unique violation
* referential integrity violation
* corrupt commit marker
* corrupt index entry
* invalid commit phase
* double-apply replay
* failed apply with marker still present
* staged schema mutation physical work that is not publishable
* ready physical index store that is not referenced by the accepted snapshot
* accepted snapshot that references missing or mismatched physical index state

Produce:

| Failure Scenario | Normal Error Type | Recovery Error Type | Equivalent? | Risk |
| ---------------- | ----------------- | ------------------- | ----------- | ---- |

### 6. Divergence Detection

Explicitly attempt to find:

* mutation performed twice on replay
* reverse index applied twice
* store mutation skipped during replay
* validation skipped in recovery
* recovery reorders operations
* recovery fails to enforce invariants enforced in executor
* best-effort rollback being treated as durable authority
* commit marker partially applied state handled differently in replay
* staged schema mutation work being treated as runtime-visible
* generated index metadata being used to recover accepted schema authority
* ready-but-unreferenced physical index stores being silently exposed

Produce:

## Divergence Risks

Each item must include:

* location
* difference
* consequence
* risk level

### 7. Idempotence Verification

Verify:

1. replaying the same durable marker twice yields identical state
2. replay does not:
   * duplicate index entries
   * duplicate reverse-index entries
   * duplicate store rows
3. replay respects:
   * commit phase transitions
   * already-applied marker detection

Produce:

| Scenario | Idempotent? | Why / Why Not | Risk |
| -------- | ----------- | ------------- | ---- |

### 8. Partial Failure Symmetry

Simulate:

1. failure after reverse-index mutation but before store write
2. failure after store write but before `finish_commit`
3. failure between `begin_commit` and first index mutation
4. failure during replace
5. failure during delete

Verify:

* recovery resumes safely
* no invariant violation
* no orphaned index entries
* no orphaned reverse entries
* no double application

Produce:

| Failure Point | Recovery Outcome | Safe? | Risk |
| ------------- | ---------------- | ----- | ---- |

### 9. Schema Mutation Startup Recovery

For supported field-path schema mutation startup publication, verify:

1. complete accepted-before and accepted-after user-index domains derive from
   accepted catalog contracts plus authoritative rows without physical writes
2. the observed accepted-before physical domain matches row-derived truth
   exactly before marker persistence
3. accepted identity, store ownership, uniqueness, resource bounds, and final
   raw entries are validated before marker persistence
4. marker-first apply marks the backing store Building, replaces only the
   affected entity/user domain, and marks Ready only after completion
5. interruption after marker persistence selects accepted-after and runs the
   complete forward recovery rebuild
6. failed recovery leaves the marker present and the derived store non-Ready;
   retry clears and rebuilds forward without restoring a before-image
7. no recovery path reconstructs accepted schema/index authority from generated
   model metadata

Produce:

| Schema Mutation State | Startup Decision | Snapshot Visible? | Physical Store Visible? | Risk |
| --------------------- | ---------------- | ----------------- | ----------------------- | ---- |

---

## Attack and Boundary Questions

Answer each question applicable to the declared scope explicitly:

* Is commit-marker durability the sole durable authority, or does any
  in-process rollback path incorrectly act like a second authority?
* Can a successful apply leave a persisted marker behind?
* Can a failed apply clear the marker incorrectly?
* Can replay observe marker state without corresponding row-op ownership?
* Can ordinary admission perform recovery work instead of returning startup pending?
* Can any recovery path other than the generated replicated driver call
  `continue_recovery`?
* Can accepted schema-transition replay and normal replay diverge on the same
  marker contract?
* Can schema mutation startup touch physical index state before the marker owns
  the accepted-after candidate?
* Can marker-owned or Building schema mutation work become runtime-visible
  before forward recovery completes?
* Can ready-but-unreferenced physical index state be silently treated as
  accepted?
* Can generated model/index metadata be used to recover accepted runtime
  authority?

If any answer is unclear, mark it as risk.

---

## Required Output Sections

0. Run Metadata + Comparability Note
1. Mutation Inventory
2. Side-by-Side Flow Tables
3. Invariant Enforcement Parity Table
4. Ordering Equivalence Table
5. Error Classification Equivalence Table
6. Divergence Risks
7. Idempotence Verification
8. Partial Failure Symmetry Table
9. Schema Mutation Startup Recovery Table
10. Attack and Boundary Answers
11. Verdict And Findings
12. Verification Readout (`PASS` / `FAIL` / `BLOCKED`)

Reports must include all applicable sections even when the verification commands
pass; summarize excluded sections and their reasons once. Do not collapse the
report into a smoke-test-only summary.

Run metadata must include:

* compared baseline report path
  * daily baseline rule: first run of day compares to latest prior comparable
    report or `N/A`
  * same-day reruns compare to that day’s `storage-recovery-consistency.md`
    baseline
* method tag/version
* comparability status (`comparable` or `non-comparable` with reason)

Apply [Findings And Verdicts](../../README.md#findings-and-verdicts).
Summarize the supported verdict, each finding's consequence and severity, and
any unresolved verification. Keep owner, disposition, and action trigger with
the finding.

---

## Baseline Verification Selection

Apply [Executed-Test Evidence](../../README.md#executed-test-evidence) before
accepting any test result. Select current tests by the obligations below; these
paths locate owners and candidate proofs, and do not themselves establish coverage.
Record missing behavioral proof explicitly rather than dropping a required row.

Paths beginning with `db/` are relative to `crates/icydb-core/src/`.
Core unit selections use `-p icydb-core --lib --features sql`; physical migration
proof also enables `migration`. Select a named integration target separately
when the obligation crosses the canister boundary.

| Proof obligation | Current source/test owners |
| --- | --- |
| Mixed-entity row/reverse state after each of five interruption points | `db/session/write.rs` (`mixed_entity_recovery_after_` family) |
| Journal replay, retirement, and reopen preserve exact control state | `db/journal/store.rs` (`exact_controls_append_replay_retire_and_reopen`) |
| Accepted schema replay/fold is idempotent | `db/schema/store/tests.rs` (`journaled_schema_candidate_replay_and_fold_are_idempotent`) |
| Recovery gates readiness until the exact schema receipt | `db/startup/mod.rs` |
| Malformed marker/row state fails closed | `db/tests/persisted_format_corpus.rs`, `db/commit/store/tests.rs` |
| Field/expression/unique index replacement preserves accepted domain truth | `db/schema/mutation/tests/user_index_domain.rs`, `db/schema/mutation/tests/planning.rs` |
| Physical migration resumes and publishes one complete candidate | `db/schema/application.rs` (`physical_migration_rewrite_recovers_and_publishes_one_complete_candidate`) |

The mixed-entity interruption family must execute all five maintained cases.
Index-domain staging tests do not alone prove interrupted publication: trace
`db/commit/schema_publication.rs` and `db/commit/recovery.rs` and select the
corresponding interruption proof as well. Add focused replay/apply evidence for
new mutation families, including nested relation projection when affected.

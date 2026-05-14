# Weekly Audit: Recovery Consistency & Replay Equivalence

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

> accepted snapshot visibility must remain old until physical work is validated,
> runtime invalidation has completed, and the physical index store can be proven
> ready for the accepted-after snapshot.

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
  * `ensure_recovered`
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
* `db/schema/reconcile/startup_field_path.rs`
  * supported field-path schema mutation startup adapter
  * startup rebuild gate
  * startup publication decision
  * physical-store revalidation before accepted snapshot insertion
* `db/schema/mutation/field_path/*`
  * staged, isolated, and published field-path index-store contracts
  * rollback/discard reports
* `db/schema/mutation/runner.rs`
  * runner phases, publication readiness, and developer diagnostics

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
* `ensure_recovered`
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

Enumerate all mutation types:

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

1. old accepted snapshot remains visible until the field-path runner report is
   publishable
2. startup row/schema gate revalidation runs before physical work and before
   accepted snapshot publication
3. final physical-store revalidation runs immediately before schema-store
   insertion
4. target index entries match runner output before accepted-after publication
5. index store is ready before accepted-after publication
6. stale, staged, building, or mismatched physical-store state is discarded when
   proven unreferenced, otherwise startup fails closed
7. no recovery path reconstructs accepted schema/index authority from generated
   model metadata

Produce:

| Schema Mutation State | Startup Decision | Snapshot Visible? | Physical Store Visible? | Risk |
| --------------------- | ---------------- | ----------------- | ----------------------- | ---- |

---

## Attack and Boundary Questions

Every run must answer these explicitly:

* Is commit-marker durability the sole durable authority, or does any
  in-process rollback path incorrectly act like a second authority?
* Can a successful apply leave a persisted marker behind?
* Can a failed apply clear the marker incorrectly?
* Can replay observe marker state without corresponding row-op ownership?
* Can recovery proceed before `ensure_recovered` gates write-side entry?
* Can accepted schema-transition replay and normal replay diverge on the same
  marker contract?
* Can schema mutation startup publish the accepted-after snapshot before
  physical-store readiness is proven?
* Can staged or building schema mutation physical work become runtime-visible
  after restart?
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
11. Overall Recovery Risk Index (1-10, lower is better)
12. Verification Readout (`PASS` / `FAIL` / `PARTIAL` / `BLOCKED`)

Reports must include all required sections even when the verification commands
pass. Do not collapse the report into a smoke-test-only summary.

Run metadata must include:

* compared baseline report path
  * daily baseline rule: first run of day compares to latest prior comparable
    report or `N/A`
  * same-day reruns compare to that day’s `storage-recovery-consistency.md`
    baseline
* method tag/version
* comparability status (`comparable` or `non-comparable` with reason)

Interpretation:

* `1-3` = Low risk / structurally healthy
* `4-6` = Moderate risk / manageable pressure
* `7-8` = High risk / requires monitoring
* `9-10` = Critical risk / structural instability

---

## Baseline Verification Commands

Start with:

* `cargo test -p icydb-core recovery_replay_is_idempotent -- --nocapture`
* `cargo test -p icydb-core recovery_replay_interrupted_conflicting_unique_batch_fails_closed -- --nocapture`
* `cargo test -p icydb-core unique_conflict_classification_parity_holds_between_live_apply_and_replay -- --nocapture`
* `cargo test -p icydb-core commit_marker_* -- --nocapture`
* `cargo test -p icydb-core commit_forward_apply_and_replay_preserve_identical_store_state_for_mixed_marker_sequence -- --nocapture`
* `cargo test -p icydb-core conditional_index_forward_apply_and_replay_preserve_identical_store_state_for_membership_matrix -- --nocapture`
* `cargo test -p icydb-core recovery_replay_updates_old_nullable_row_before_image_with_accepted_contract -- --nocapture`
* `cargo test -p icydb-core schema::reconcile --features sql -- --nocapture`
* `cargo test -p icydb-core schema_mutation_publication_boundary_uses_runner_preflight --features sql -- --nocapture`

Add targeted replay/apply tests for any newly widened mutation surface. Add
targeted schema mutation startup tests when supported physical mutation
publication changes.

# WEEKLY AUDIT — Invariant Preservation (icydb-core)

Canonical report scope:

* `invariant-preservation`

Use this exact scope for report files:

* `docs/reports/recurring/YYYY/MM/DD/invariant-preservation/<run>/report.md`

Do not introduce alternate names such as `core-invariants`,
`runtime-invariants`, or `invariant-audit` for this recurring pass.

## Purpose

Verify that **all structural, ordering, identity, and mutation invariants** in `icydb-core`:

* Exist explicitly
* Are enforced exactly once
* Are enforced at the correct boundary
* Are enforced in both normal execution and recovery
* Cannot drift silently

This is a correctness audit only.

Do NOT discuss:

* Performance
* Style
* DRY
* Refactoring
* Architecture redesign (unless invariant violation is found)

---

# Phase 0 — Establish the Invariant Registry

Before analysis, enumerate all invariants in the system.

You must not assume them.
You must list them explicitly.

Classify invariants into categories:

### A. Identity Invariants

* Entity primary key matches storage key
* Index id consistency
* Key namespace consistency
* Component arity stability
* Expected-key vs decoded-entity match

### B. Ordering Invariants

* Raw index key lexicographic ordering is canonical
* Logical ordering matches raw ordering
* Cursor resume is strictly monotonic
* Bound inclusivity semantics preserved
* Envelope containment preserved

### C. Structural Invariants

* AccessPath shape stability
* Plan shape immutability after validation
* No widening of predicate envelope
* Unique constraint guarantees
* Reverse relation symmetry

### D. Mutation Invariants

* Save mutates index + store consistently
* Delete removes index + store consistently
* Reverse index mutation symmetry
* Referential integrity enforcement

### E. Recovery Invariants

* Replay is idempotent
* Replay does not widen envelope
* Replay does not duplicate index entries
* Replay restores exact structural shape
* Replay error classification matches runtime

### F. Accepted Runtime Authority Invariants

* Accepted row contracts own runtime row decode and row emission
* Accepted schema/index contracts own runtime planner, executor, write,
  explain, cache, cursor, uniqueness, and recovery authority
* Generated `EntityModel` / `IndexModel` metadata is proposal-only,
  reconciliation-only, model-only convenience, or test-only after schema
  acceptance
* Runtime boundary validation consumes caller-selected `SchemaInfo`
* Runtime fingerprints derive from accepted persisted snapshots, not generated
  model metadata

### G. Catalog Mutation Invariants

* Accepted schema transitions are classified by schema-owned mutation plans
* Metadata-only and supported SQL DDL metadata transitions may publish only
  after exact accepted identity, revision, and mutation-plan preflight
* Field-path and deterministic expression-index additions may publish only
  after the complete accepted-before and accepted-after user-index domains are
  staged, validated, and installed atomically with the schema candidate
* Constraint and field-metadata SQL DDL frontends use schema-owned admission
  and publication semantics rather than defining independent mutation rules
* Feature-gated physical migrations own one exact persisted plan, isolated
  candidate generations, bounded validation and rewrite progress, final
  physical validation, and complete candidate publication
* Migration abort and recovery affect only the exact candidate owned by the
  persisted migration record and never expose incomplete physical state
* Unsupported or incompatible mutations fail closed before write/read staging
* Startup recovery completes journal and commit-marker recovery before schema
  reconciliation or physical migration resumes

Produce:

| Invariant | Category | Subsystem(s) Impacted |

This becomes the baseline for all checks.

---

# Phase 1 — Boundary Mapping

Identify all boundary crossings:

* serialize → deserialize
* RawIndexKey encode → decode
* identity types → storage key
* planner → executable plan
* executable plan → executor
* save executor → commit
* delete executor → commit
* commit → recovery replay
* cursor decode → cursor planning
* reverse-relation mutation
* index store read → index key interpretation
* accepted snapshot → `SchemaInfo`
* generated proposal → accepted reconciliation
* mutation plan → publication status
* accepted schema fingerprint → planner/cache/commit identity

For each boundary:

| Boundary | Input Assumptions | Output Guarantees |

---

# Phase 2 — Invariant Enforcement Mapping

For each invariant:

You must identify:

A. Where it is assumed
B. Where it is enforced
C. Whether enforcement is:

* Exactly once
* At the narrowest boundary
* Too early
* Too late
* Duplicated
* Missing

Produce:

| Invariant | Assumed At | Enforced At | Exactly Once? | Narrowest Boundary? | Correct Error Class? | Risk |

---

# Phase 3 — Symmetry & Recovery Audit

For each invariant:

Verify:

1. Enforced in normal execution
2. Enforced in recovery replay
3. Enforced in cursor continuation
4. Enforced in reverse relation mutation
5. Enforced in index encode/decode

Produce:

| Invariant | Normal Exec | Recovery | Cursor | Reverse Index | Risk |

Flag any invariant enforced only in forward execution.

---

# Phase 4 — High-Risk Focus Areas

Explicitly deep-audit:

## A. Cursor Envelope Safety

* Anchor cannot escape original envelope
* Bound conversion uses Excluded
* Upper bound never modified
* Index id cannot change
* Namespace cannot change
* Arity cannot change

## B. Index Key Ordering Guarantees

* Encode preserves lexicographic order
* Decode does not reinterpret ordering
* No ordering assumptions outside raw key compare
* Composite prefix ordering preserved

## C. Reverse Relation Index Correctness

* Reverse index updated symmetrically on save
* Reverse index updated symmetrically on delete
* Reverse index consistent during recovery
* No orphaned reverse entries

## D. Recovery Idempotence

* Replay twice produces identical state
* Index and store match after replay
* No duplicate index keys
* No widening of access path

## E. Expected-Key vs Decoded-Entity Match

* Decoded entity key must equal storage key
* Enforced before returning entity
* Enforced during recovery
* Error classification correct

## F. Accepted Runtime Authority Preservation

* Accepted runtime paths do not reopen generated row/index authority
* Runtime `SchemaInfo` comes from accepted snapshots
* Cursor and access-plan invariant validation use accepted schema info
* Unique/index/reverse/recovery preflight use accepted contracts
* Model-only helpers remain explicitly named and outside runtime lanes

## G. Catalog Mutation Publication Safety

* Metadata-only and empty-entity transitions retain their exact admission proof
* Field-path and deterministic expression-index additions stage and validate
  one complete accepted-before/accepted-after user-index domain
* Constraint and field-metadata SQL DDL remain schema-owned frontends
* Unsupported nullability/type/key changes fail closed
* Mutation-plan fingerprints are deterministic and semantic
* Candidate schema and physical generations publish as one operation
* Physical migration validation, rewrite, abort, recovery, and final
  publication remain bound to one persisted exact plan
* Transition metrics distinguish exact, accepted, rejected, and migration
  outcomes without becoming publication authority

---

# Phase 5 — Enforcement Quality Evaluation

Flag invariants that are:

* Enforced in multiple layers
* Enforced after mutation
* Enforced only implicitly
* Enforced via assumption rather than explicit check
* Not enforced on corrupted input
* Not enforced in recovery
* Not covered by tests

Produce sections:

---

## High Risk Invariants

Invariants where:

* Missing enforcement
* Late enforcement
* Recovery asymmetry
* Multiple enforcement sites with drift risk

---

## Redundant Enforcement

Invariants enforced in:

* Planner + executor
* Executor + store
* Store + recovery

Highlight potential drift pressure.

---

## Missing Enforcement

Any invariant that:

* Is assumed but never explicitly validated
* Is only validated in one path
* Is not validated during replay
* Is not validated during cursor continuation

---

# Phase 6 — Drift Sensitivity Analysis

For each invariant, assess:

| Invariant | Sensitive To | Drift Risk |

Examples:

* Adding DESC
* Adding composite access paths
* Adding new index types
* Adding new commit markers
* Adding new error classes
* Extending accepted schema mutation publication beyond current metadata,
  field/index, constraint, or physical-migration paths
* Adding SQL DDL frontends over schema mutations

This anticipates silent invariant erosion.

---

# Required Verification Baseline

Every run must include source inspection plus current live verification.

Required commands:

* `bash scripts/ci/check-memory-id-invariants.sh`
* `bash scripts/ci/check-layer-authority-invariants.sh`
* `bash scripts/ci/check-index-range-spec-invariants.sh`
* `cargo test -p icydb-core --lib mixed_entity_recovery_after_ --features sql -- --nocapture`
* `cargo test -p icydb-core --lib exact_controls_append_replay_retire_and_reopen --features sql -- --nocapture`
* `cargo test -p icydb-core --lib journaled_schema_candidate_replay_and_fold_are_idempotent --features sql -- --nocapture`
* `cargo test -p icydb-core --lib completed_recovery_stays_recovering_until_exact_generated_schema_receipt_then_is_ready --features sql -- --nocapture`
* `cargo test -p icydb-core --lib persisted_row_envelope_malformed_corpus_fails_closed --features sql -- --nocapture`
* `cargo test -p icydb-core --lib schema::mutation --features sql -- --nocapture`
* `cargo test -p icydb-core --lib physical_migration_rewrite_recovers_and_publishes_one_complete_candidate --features "sql migration" -- --nocapture`
* `cargo test -p icydb-core --lib accepted_index_missing_row_is_typed_store_corruption --features "sql diagnostics" -- --nocapture`
* `cargo test -p icydb-core --lib index::envelope::tests --features sql -- --nocapture`
* `cargo test -p icydb-core --lib index_key_ordering_ --features sql -- --nocapture`
* `cargo test -p icydb-core --lib mixed_relation_validation_uses_the_complete_final_row_overlay --features sql -- --nocapture`

Every required test command must execute at least one test. The
`mixed_entity_recovery_after_` family must execute all five maintained
interruption points. A successful command that reports `running 0 tests` is an
audit failure, not verification evidence. Record the executed count for every
selector in the report.

If a required selector no longer matches live tests, stop the run, mark that
verification `FAIL`, and revise this definition before rerunning the audit. Do
not substitute an unrecorded replacement inside a report.

---

# Final Output Structure

0. Run Metadata + Comparability Note
1. Invariant Registry (complete list)
2. Boundary Map
3. Enforcement Mapping Table
4. Recovery Symmetry Table
5. Accepted Authority Preservation Table
6. Catalog Mutation Publication Table
7. High Risk Invariants
8. Redundant Enforcement
9. Missing Enforcement
10. Drift Sensitivity Summary
11. Overall Invariant Risk Index (1–10, lower is better)
12. Verification Readout (`PASS`/`FAIL`/`BLOCKED`)

Run metadata must include:

- compared baseline report path (daily baseline rule: first run of day compares
  to latest prior comparable report or `N/A`; same-day reruns compare to that
  day's `invariant-preservation.md` baseline)
- method tag/version
- comparability status (`comparable` or `non-comparable` with reason)

Interpretation:
1–3  = Low risk / structurally healthy
4–6  = Moderate risk / manageable pressure
7–8  = High risk / requires monitoring
9–10 = Critical risk / structural instability

---

# Anti-Shallow Requirement

Do NOT:

* Say “looks correct”
* Say “appears enforced”
* Provide generic statements
* Skip enforcement location
* Skip recovery symmetry check

Every invariant must:

* Be named
* Be mapped
* Be located
* Be proven

# WEEKLY AUDIT — Index Integrity

`icydb-core`

Canonical report scope:

* `index-integrity`

Use this exact scope for report files:

* `docs/audits/reports/YYYY-MM/YYYY-MM-DD/index-integrity.md`

Do not introduce alternate names such as `access-index-integrity`,
`index-audit`, or `index-authority` for this recurring pass.

## Purpose

Verify that the index subsystem preserves:

* Ordering guarantees
* Namespace isolation
* Index id containment
* Mutation symmetry
* Unique enforcement equivalence
* Recovery idempotence
* Row/index atomic coupling
* Accepted schema/index authority after schema reconciliation
* Catalog mutation readiness for index add/drop/rebuild decisions

This is a correctness audit only.

Do NOT discuss:

* Performance
* Refactoring
* Style
* Code aesthetics

---

# Core Principle

The index layer must be:

* Deterministic
* Lexicographically ordered
* Namespace-isolated
* Idempotent under replay
* Coupled 1:1 with row mutations
* Authorized by accepted schema/index contracts after reconciliation

If index state diverges from row state, integrity is compromised.
If runtime index behavior reopens generated model authority after schema
acceptance, catalog authority is compromised.

---

# STEP 0 — Index Invariant Registry

Enumerate all index-level invariants before analysis.

Categories:

### A. Ordering Invariants

* Raw key ordering is lexicographic
* Component ordering is stable
* Prefix encoding preserves sort order
* No implicit logical ordering layer

### B. Namespace Invariants

* Index id is encoded in key
* Key namespace prevents cross-index decode
* No key can decode as different index id
* Index id mismatch is detected

### C. Structural Invariants

* Key encode/decode symmetry
* Entry layout stable
* No partial decode acceptance
* Component arity fixed per index

### D. Mutation Invariants

* Row mutation → index mutation (always)
* Index mutation → row mutation (never standalone)
* Reverse index symmetry
* Unique enforcement consistency

### E. Recovery Invariants

* Replay is idempotent
* Replay enforces same unique checks
* Replay enforces same reverse mutation logic
* No duplicate entry creation
* No partial mutation replay

### F. Accepted Authority Invariants

* Runtime visible index selection is accepted-schema-backed
* Runtime key construction uses accepted index contracts
* Runtime uniqueness validation uses accepted index contracts
* Runtime reverse index operations use accepted contracts
* Explain/cache identity reflects accepted index fingerprints
* Missing accepted index metadata after reconciliation fails closed
* Generated `EntityModel` / `IndexModel` metadata is proposal-only,
  reconciliation-only, model-only convenience, or test-only

### G. Catalog Mutation Invariants

* Add/drop index mutation plans classify rebuild requirements explicitly
* Unsupported index mutations fail closed before planner/write visibility
* Accepted snapshot rewrites do not publish half-visible indexes
* Index fingerprint changes invalidate affected planner/cache surfaces
* Recovery/rebuild can identify the accepted contract that owns each index

Produce:

| Invariant | Category | Enforced Where |

---

# STEP 1 — Key Encoding & Ordering Audit

## 1A. Encode/Decode Symmetry

Verify:

* `encode(decode(key)) == key`
* `decode(encode(logical_key)) == logical_key`
* No silent truncation
* No partial decode acceptance
* No component count mismatch acceptance

Produce:

| Key Type | Symmetric? | Failure Mode | Risk |

---

## 1B. Lexicographic Ordering Proof

Verify:

* Byte ordering corresponds to logical component ordering
* Prefix encoding does not break sort order
* Float handling is canonical
* Signed types preserve lexicographic ordering
* Composite keys compare component-wise lexicographically

Explicitly test reasoning for:

* Negative vs positive numbers
* Zero values
* Composite key prefix differences
* Variable-length encoding boundaries

Produce:

| Case | Lexicographically Stable? | Why | Risk |

---

# STEP 2 — Namespace & Index ID Isolation

Verify:

* Index id embedded in key namespace
* No two index ids share overlapping key prefixes
* Decode path validates index id before interpreting payload
* Index id mismatch produces invariant error
* No key from index A can decode as valid key of index B

Attempt to find:

* Cross-index decode acceptance
* Key collision across namespaces
* Prefix confusion
* Partial prefix collisions

Produce:

| Scenario | Can Cross-Decode? | Prevented Where | Risk |

---

# STEP 3 — IndexStore Entry Layout

Map:

* Raw entry layout
* Key bytes
* Payload bytes
* Any fingerprint or marker data
* Entry boundaries

Verify:

* Entry layout is deterministic
* Entry layout is stable across replay
* No variable-length ambiguity
* Decode cannot misalign entry boundary

Produce:

| Entry Component | Layout Stable? | Decode Safe? | Risk |

---

# STEP 4 — Reverse Relation Index Integrity

Verify:

* Reverse entries created during save
* Reverse entries removed during delete
* Replace flow handles both old + new reverse entries
* Recovery applies identical reverse mutation logic
* No reverse entry can exist without corresponding forward relation

Attempt to find:

* Orphan reverse entries
* Reverse duplication on replay
* Reverse mutation ordering mismatch

Produce:

| Flow | Reverse Mutation Symmetric? | Orphan Risk | Replay Risk |

---

# STEP 5 — Unique Index Enforcement

Verify:

* Unique violation detected before mutation
* Unique violation classification consistent
* Recovery re-enforces unique constraint
* Replay does not skip unique validation
* Live apply and replay apply preserve unique-conflict class parity
* Replace handles same-value update correctly
* Delete + reinsert same value allowed

Attempt to find:

* Replay bypass of unique check
* Double insert allowed
* Replace violating unique invariant
* Partial unique mutation during prepare

Produce:

| Scenario | Unique Enforced? | Recovery Enforced? | Live/Replay Class Parity? | Risk |

---

# STEP 6 — Partial-Update Index Membership Transitions

Verify partial-update membership transitions for indexed fields:

* old indexed field value -> new indexed field value
* old `null` -> new value
* old value -> `null`
* update removing membership from one index and adding membership to another
* update with unchanged indexed field (must produce no index mutation)

Invariant:

* Prepared index ops remain minimal, correct, and symmetric.

Produce:

| Transition Case | Prepared Ops Minimal? | Symmetric? | Risk |

---

# STEP 7 — Mixed Unique + Reverse + Secondary Interaction

Verify one update that simultaneously:

* violates unique index
* modifies reverse relation
* modifies secondary index

Required ordering invariant:

* No side-effectful mutation or prepared index side effects before uniqueness verdict.

Produce:

| Scenario | Uniqueness Verdict Happens First? | Any Side Effects Before Verdict? | Risk |

---

# STEP 8 — Accepted Runtime Index Authority

Verify runtime index behavior does not consume generated model/index metadata
after accepted schema reconciliation.

Allowed generated lanes:

* proposal derivation
* reconciliation comparison
* model-only convenience APIs
* tests/fixtures

Runtime lanes that must be accepted-schema-backed:

* visible index selection
* planner candidate construction
* predicate/index membership
* key shape and key encoding
* forward index writes
* reverse index writes
* uniqueness validation
* recovery/rebuild
* explain projection
* query cache identity/fingerprints

Search targets:

* `E::MODEL.indexes`
* `EntityModel::indexes`
* `IndexModel`
* `from_generated_index`
* `accepted_field_path_indexes`
* `VisibleIndexes`
* `SchemaInfo`
* `fingerprint`
* `unique`
* `reverse_index`
* `index_key`

Produce:

| Runtime Lane | Authority Source | Generated Metadata Present? | Allowed? | Risk |

Required fail-closed checks:

| Scenario | Expected Behavior | Evidence | Risk |
| --- | --- | --- | --- |
| Accepted index metadata missing after reconciliation | incompatible accepted-schema error | guard/test/source evidence | High if absent |
| Stale accepted index fingerprint | planner/cache invalidation or rejection | guard/test/source evidence | High if absent |
| Dropped secondary index still present in store | invisible to planning; rebuild/recovery owns cleanup plan | guard/test/source evidence | Moderate |
| Runtime write sees generated-only index metadata | rejected or unreachable outside model-only lane | guard/test/source evidence | High if absent |

---

# STEP 9 — Catalog Mutation Readiness

Verify index mutations are classified before any runtime visibility change.

Initial mutation cases:

* add non-unique field-path index
* add non-unique deterministic expression index
* drop non-required secondary index
* change uniqueness
* change key shape or key slot order
* change partial/filter predicate
* change expression text/fingerprint

Produce:

| Mutation | Compatibility | Rebuild Requirement | Runtime Visibility Boundary | Risk |

Required classifications:

* metadata-only safe
* supported startup rebuild publication for the single non-unique field-path
  index-add path
* index rebuild required but still blocked for expression-index rebuilds and
  secondary-index cleanup
* full data rewrite required
* unsupported pre-1.0
* incompatible/fail-closed

Verify:

* mutation plans do not silently hide persisted indexes with missing metadata
* the supported field-path index-add path publishes only after target-scoped
  physical validation, runtime invalidation, startup rebuild-gate revalidation,
  physical-store publication, final target-index physical-store revalidation,
  and accepted snapshot insertion
* required rebuilds outside the supported path are explicit and remain blocked
* planner/cache invalidation is attached to accepted fingerprint changes
* unsupported changes fail before accepted snapshot publication

---

# STEP 10 — Row ↔ Index Coupling

Verify:

* No index mutation without corresponding row mutation
* No row mutation without index mutation
* Reverse mutation tightly coupled
* Mutation ordering consistent between save and recovery
* Partial failure cannot leave index without row or row without index

Simulate:

1. Failure after index insert before row write
2. Failure after row write before index insert
3. Failure during reverse index update
4. Replay after partial commit

Produce:

| Failure Point | Divergence Possible? | Prevented? | Risk |

---

# STEP 11 — Recovery Replay Equivalence

Compare:

Normal Save Path vs Replay Path
Normal Delete Path vs Replay Path
Normal Replace Path vs Replay Path

For each:

| Phase | Normal | Replay | Equivalent? | Risk |

Verify:

* Same invariant checks
* Same mutation order
* Same error classification
* Same reverse mutation logic
* Idempotence
* Same unique-conflict class (`Conflict`) with boundary-owned origin

Required scenario lock:

| Scenario | Live Path Expected | Replay Path Expected | Evidence |
| --- | --- | --- | --- |
| Duplicate unique insert (`new key`, same unique value) | `ErrorClass::Conflict`, index-owned origin | `ErrorClass::Conflict`, recovery-owned origin | commit + mutation parity tests |

---

# STEP 12 — Explicit Attack Scenarios

Attempt to find:

* Key collisions across index ids
* Component arity confusion
* Namespace prefix overlap
* Partial decode acceptance
* Index id mismatch vulnerability
* Reverse orphan after replay
* Double unique insert on replay
* Delete skipping reverse cleanup
* Replace partial mutation
* Runtime planner consuming generated-only index metadata
* Runtime write consuming generated-only index metadata
* Accepted snapshot missing required index contract metadata
* Stale index fingerprint reused for query cache identity
* Dropped index remaining visible to planner/explain
* Recovery rebuild using generated model metadata instead of accepted contracts

For each:

| Attack | Possible? | Why / Why Not | Risk |

---

# STEP 13 — High Risk Mutation Paths

Identify:

* Complex flows with multiple mutation phases
* Replace flow with dual mutation
* Recovery mutation entry points
* Reverse mutation code paths
* Accepted snapshot publication after index mutation classification
* Planner/cache invalidation after index contract fingerprint changes

Produce:

| Path | Complexity | Divergence Risk | Risk Level |

---

# STEP 14 — Storage-Layer Assumptions

Explicitly list assumptions such as:

* Stable memory write atomicity per entry
* Deterministic iteration order
* Key comparison strictly byte-wise
* No external mutation of raw storage
* No concurrent writes

Produce:

| Assumption | Required For | Violation Impact |

---

# STEP 15 — Cross-Layer Continuation Stability (Executor + Index)

This is not index-only. Run as a cross-layer check with cursor/ordering semantics.

Scenario:

* delete rows inside an active paginated index-range window

Verify continuation anchor behavior does not:

* resurrect deleted rows
* skip next eligible row
* duplicate rows across pages

Produce:

| Scenario | Resurrection? | Skip? | Duplicate? | Risk |

---

# STEP 16 — Required Verification Commands

Run focused checks first. If one fails, record it as `FAIL` or `BLOCKED` and do
not keep rerunning the same failing suite in the same audit pass.

Minimum verification set:

* `cargo test -p icydb-core load_cursor_pagination_pk_order_inverted_key_range_returns_empty_without_scan --features sql -- --nocapture`
* `cargo test -p icydb-core index_range_aggregate_fast_path_specs_reject_non_exact_range_arity --features sql -- --nocapture`
* `cargo test -p icydb-core index_range_aggregate_fast_path_specs_reject_prefix_spec_presence --features sql -- --nocapture`
* `cargo test -p icydb-core cross_layer_canonical_ordering_is_consistent --features sql -- --nocapture`
* `cargo test -p icydb-core unique_conflict_classification_parity_holds_between_live_apply_and_replay --features sql -- --nocapture`
* `cargo test -p icydb-core recovery_replay_interrupted_conflicting_unique_batch_fails_closed --features sql -- --nocapture`
* `cargo test -p icydb-core load_cursor_live_state_delete_between_pages_can_shrink_remaining_results --features sql -- --nocapture`
* `cargo test -p icydb-core --test write_boundary_guards -- --nocapture`

If the audit touches mutation-readiness claims, also run:

* `cargo test -p icydb-core schema::mutation --features sql -- --nocapture`
* `cargo test -p icydb-core schema::reconcile --features sql -- --nocapture`

---

# Required Output Sections

0. Run Metadata + Comparability Note
1. Index Invariant Registry
2. Encode/Decode Symmetry Table
3. Ordering Stability Analysis
4. Namespace Isolation Table
5. Entry Layout Analysis
6. Reverse Relation Integrity
7. Unique Enforcement Equivalence
8. Partial-Update Membership Transitions
9. Mixed Unique+Reverse+Secondary Ordering Check
10. Accepted Runtime Index Authority
11. Catalog Mutation Readiness
12. Row/Index Coupling Analysis
13. Replay Equivalence Table
14. Cross-Layer Continuation Stability
15. High Risk Mutation Paths
16. Storage-Layer Assumptions
17. Overall Index Risk Index (1–10, lower is better)
18. Verification Readout (`PASS`/`FAIL`/`BLOCKED`)

Run metadata must include:

- compared baseline report path (daily baseline rule: first run of day compares
  to latest prior comparable report or `N/A`; same-day reruns compare to that
  day's `index-integrity.md` baseline)
- method tag/version
- comparability status (`comparable` or `non-comparable` with reason)

---

# Scoring Model

Interpretation:
1–3  = Low risk / structurally healthy
4–6  = Moderate risk / manageable pressure
7–8  = High risk / requires monitoring
9–10 = Critical risk / structural instability

---

# Why This Version Is Stronger

It forces:

* Byte-level reasoning
* Namespace isolation proof
* Encode/decode symmetry proof
* Accepted runtime authority proof
* Catalog mutation readiness proof
* Replay equivalence proof
* Reverse symmetry mapping
* Mutation ordering comparison
* Explicit attack simulation
* Storage assumption declaration

Index integrity failures are catastrophic.

This audit must be the most rigorous one you run.

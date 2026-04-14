# IcyDB Security Audit Proposal

## Purpose

Define a recurring security audit for IcyDB that focuses on the real security-sensitive boundaries in the current system rather than treating security as a generic add-on.

This audit is intended to complement the existing correctness and architecture audits by answering a distinct question:

**Can an untrusted caller, malformed SQL payload, forged continuation token, replayed recovery state, misconfigured stable-memory ownership, or incorrect cache reuse cause unauthorized data exposure outside the validated query, cursor, namespace, or continuation contract, invalid state transition, cross-namespace contamination, integrity loss, or planner-contract bypass that permits attacker-controlled expensive shapes the product contract says must be rejected?**

This is not:

* a general style audit
* a performance audit
* a feature roadmap
* a broad refactor checklist

It is a strict security-boundary and fail-closed behavior audit.

The audit should be read through the normal security lenses:

* **Confidentiality**: no unauthorized data exposure across public SQL, cursor, cache, or namespace boundaries
* **Integrity**: no invalid durable-state transition, replay drift, or cross-store contamination
* **Availability**: no planner-contract bypass that permits attacker-controlled expensive query shapes the product contract says must be rejected

---

## Why This Audit Should Exist

IcyDB already has strong audit coverage around:

* recovery consistency and replay equivalence
* invariant preservation
* index integrity
* cursor ordering and continuation safety
* layer authority and architecture text-scan invariants
* memory-id invariants

Those are strong foundations, but they are distributed across correctness and architecture tracks. The missing piece is one explicit **security audit** that treats the public SQL/cursor/stable-memory surface as an adversarial boundary and verifies fail-closed behavior end to end.

The project now has enough public SQL surface, canister wiring, continuation support, recovery machinery, and stable-memory contracts that this is justified.

---

## Audit Name

# Security Audit — Public Boundary, Continuation, and Durable-State Safety

Short name:

`security-boundary`

---

## Scope

Primary scope:

* SQL canister/public query entrypoints
* SQL compile/validate/execute boundary for untrusted input
* continuation / cursor token decode and boundary validation
* index-envelope containment and continuation monotonicity
* stable-memory commit-marker and recovery boundaries
* replay equivalence and interrupted-write recovery safety
* memory-id and namespace isolation invariants
* error containment and fail-closed classification at public boundaries
* resource-bound policy checks where unbounded execution could become an abuse vector

Out of scope unless IcyDB later adds these features:

* authentication / subject-caller identity binding
* tenant isolation / per-principal authorization
* token trust chains
* capability scopes

If those surfaces are added later, they should become separate invariant audits rather than being mixed into this one.

---

## Security Model

Assume all of the following are adversarial or untrusted unless proven otherwise:

* raw SQL text
* continuation cursor bytes
* cursor boundary payload contents
* external/public query parameters
* canister caller timing and retry behavior
* interrupted execution around mutation boundaries
* stale or malformed replay state
* incorrect canister memory-id configuration

Assume trusted storage may become **corrupt** but not maliciously self-healing. Recovery and replay must fail closed when corruption or mismatch is detected.

---

## Core Security Invariants

### 1. Public SQL input must fail closed before execution

No malformed, unsupported, or semantically invalid SQL may reach execution as a partially interpreted request.

Required property:

`parse -> lower -> validate -> plan -> execute`

must reject unsupported or malformed input before execution side effects occur.

### 2. Continuation tokens must not widen access scope

A forged, stale, or mismatched continuation token must not:

* escape the original index envelope
* replay earlier rows
* skip required ordering validation
* alter entity/order identity
* cross query signatures

### 3. Recovery and replay must preserve mutation integrity

Interrupted write/replay paths must not produce a state that differs from the canonical committed outcome, and failed replay must fail closed rather than partially healing incorrectly.

### 4. Stable-memory boundaries must remain explicit and isolated

Commit-marker storage must remain pinned to its configured memory id and must not auto-discover or drift into unrelated memory ranges or data-store/index-store slots.

### 5. Index and relation side effects must preserve symmetry under failure and replay

Unique, reverse-index, and relation side effects must preserve the same safety behavior in normal apply and recovery replay.

### 6. Error classification must not hide security-relevant failure modes

Malformed public input must remain invalid-input/cursor-domain failure.
Persisted corruption must remain corruption.
Unsupported feature usage must not be misclassified as success or downgraded into benign behavior.

### 7. Resource-bound rules must block abuse-prone unbounded shapes

When the product contract says a shape must be bounded for safety/resource reasons, planning must reject the unbounded form rather than relying on execution luck.

### 8. Public execution surface must not collapse query and update semantics

A shared compilation/cache path must not:

* make update-only behavior appear valid in query context
* misrepresent query-only non-persistence semantics as durable warming
* erase public entrypoint differences in a way that weakens safety guarantees

---

## Audit Structure

### Section A — Public SQL Boundary

Goal:

Verify that untrusted SQL text is fully contained by parser/lowering/validation/planning boundaries before any execution side effects occur.

Check:

* unsupported syntax remains fail-closed
* unsupported semantic shapes remain fail-closed
* invalid field/order/grouping/predicate forms do not reach executor mutation/load paths
* `EXPLAIN` does not bypass core validation policy
* direct expression widening is not accidentally admitted through alias or normalization paths
* normalization remains cache-neutral, so equivalent admitted forms do not become different compiled semantic identities

Search targets:

* SQL canister/public endpoint entrypoints
* `compile_sql` / `compile_sql_command` / `query_from_sql` / `execute_sql`
* parser clause admission
* lowering normalization
* planner validation
* session SQL execution routing

Required outcomes:

* one canonical SQL compile path
* no alternate path that skips validation
* no execution of partially validated SQL

### Section A1 — Cache Identity and Surface Isolation

Goal:

Verify that query compilation and shared lower-plan reuse remain surface-correct and cannot create cross-surface semantic contamination.

In the current architecture, compiled-command cache correctness is in scope. Shared lower-plan cache checks apply only where that cache exists in the audited code snapshot.

Check:

* compiled-command cache keys remain distinct for `query` vs `update`
* admitted normalization happens before compiled-command cache lookup/insertion
* equivalent admitted SQL forms canonicalize onto the same structural query/order identity
* shared lower-plan cache does not alias semantically distinct SQL and builder/fluent query APIs
* `query` calls must not be documented or relied upon as creating persistent cache state on the IC
* update-warmed cache reuse does not widen semantics for later `query` calls

Search targets:

* SQL compiled-command cache keys
* SQL normalization and canonical order/query representation
* shared lower query-plan cache keys
* query/update canister entrypoint behavior
* live canister tests covering cache warm/reuse boundaries

Required outcomes:

* no cross-surface cache aliasing
* no duplicate compiled identity for equivalent admitted forms
* no persistence semantics claimed for standalone `query` misses
* no cache reuse that weakens planner-owned boundaries

### Section B — Continuation / Cursor Tamper Resistance

Goal:

Verify that continuation tokens and cursor boundaries cannot be forged or widened into unauthorized traversal.

Check:

* token decode errors stay invalid-input/cursor-domain failures
* signature/order/entity mismatches reject
* anchor containment is enforced
* continuation must strictly advance beyond anchor
* boundary payload values must match canonical order typing
* cursor direction/window/offset mismatches reject
* cursor helpers do not re-derive looser semantics than planner-owned order contracts

Search targets:

* continuation token decode
* cursor boundary validation
* index envelope helpers
* cursor spine/planned cursor validation
* order cursor helpers

Required outcomes:

* no token replay that duplicates or omits rows outside contract
* no out-of-envelope advancement
* no cross-order or cross-query cursor reuse

### Section C — Recovery / Replay / Durable Atomicity

Goal:

Verify that interrupted writes and replayed commit markers preserve atomicity and integrity.

Check:

* `ensure_recovered` runs before operation-specific planning/apply where required
* replay remains idempotent
* marker lifecycle is durable and authoritative
* interrupted conflicting unique batches fail closed
* replay does not downgrade invariant failures
* recovery retry cannot silently widen state drift

Search targets:

* commit marker guard lifecycle
* recovery entrypoints
* replay row-op application
* rollback helpers
* replay parity tests

Required outcomes:

* normal and replay paths are semantically equivalent
* no partial-apply state is accepted as success
* marker cleanup occurs only on safe completion

### Section D — Stable-Memory and Namespace Isolation

Goal:

Verify that stable-memory ownership boundaries remain explicit and cannot drift across unrelated regions.

Check:

* commit memory id is explicitly configured
* recovery configures commit memory before access
* allocator does not scan arbitrary ranges for commit anchor discovery
* memory-id mismatch fails closed
* memory registry ownership is explicit

Search targets:

* commit memory configuration
* canister attributes / derived memory id requirements
* memory invariant scripts
* recovery initialization

Required outcomes:

* no runtime auto-discovery fallback
* no cross-store memory collision risk by design
* CI invariants still encode the intended boundary

### Section E — Error Containment and Public Failure Semantics

Goal:

Verify that security-relevant failures stay in the correct error domain and do not become silent permissive behavior.

Check:

* malformed cursor stays cursor/input domain
* persisted decode issues stay corruption
* unsupported features stay unsupported
* invariant violations are not surfaced as harmless user mistakes when they indicate internal safety failure
* public mappings preserve the underlying security meaning

Search targets:

* `ErrorClass`, `ErrorOrigin`, `InternalError`
* cursor decode/plan errors
* query/intent/public error mappings
* corruption constructors

Required outcomes:

* no corruption downgrade
* no invalid-input upgrade into success-like fallback
* no domain confusion at public boundaries

### Section F — Resource Abuse and Denial-of-Service Guardrails

Goal:

Verify that public query shapes with explicit boundedness/resource requirements still reject attacker-controlled expensive forms before heavy execution.

Check:

* grouped unbounded `ORDER BY` remains rejected
* continuation-compatible resource contracts remain intact
* route/resource policy is enforced in planner, not only execution
* newly widened SQL shapes do not accidentally bypass boundedness guards
* denial-of-service-prone query shapes remain fail-closed when the product contract says they must be bounded

Search targets:

* grouped plan resource checks
* route/resource compliance docs/tests
* SQL widening patches affecting order/grouping/continuation

Required outcomes:

* explicit contract violations reject before heavy execution
* no planner/executor policy drift

---

## Report Template

Every generated report should include:

* Scope
* Compared baseline report path
* Code snapshot identifier
* Method tag/version
* Comparability status
* Auditor
* Run timestamp
* Branch / worktree state

Then:

### Findings Table

| Check | Evidence | Status | Risk |
| ----- | -------- | ------ | ---- |

### Assumptions Validated

Examples:

* no auth/tenant model present in this snapshot
* cache scope present/absent as expected for the audited code snapshot
* public SQL entrypoints identified and unchanged from expected ownership boundaries
* continuation token trust model unchanged from prior baseline unless explicitly noted

### Structural Hotspots

List concrete files/modules carrying security-sensitive authority.

### Hub Module Pressure

List high-fan-in modules that could amplify security drift.

### Early Warning Signals

Examples:

* new public SQL side paths
* cursor helper duplication
* replay-specific semantic divergence
* memory-id helper spread beyond canonical owner
* error-domain flattening
* new canister/public entrypoints without invariant coverage

### Dependency Fan-In Pressure

List the heaviest security-sensitive hubs.

### Risk Score

Normalized `X / 10` score.

### Verification Readout

Use normalized statuses:

* `PASS`
* `FAIL`
* `PARTIAL`
* `BLOCKED`

### Follow-Up Actions

If any result is `FAIL`/`PARTIAL` or risk score is `>= 5`, include owner, action, and target report run.

If not needed, state:

`No follow-up actions required.`

---

## Baseline Verification Commands

Start with the checks IcyDB already uses as security-adjacent evidence:

* `bash scripts/ci/check-index-range-spec-invariants.sh`
* `bash scripts/ci/check-memory-id-invariants.sh`
* `bash scripts/ci/check-field-projection-invariants.sh`
* `bash scripts/ci/check-layer-authority-invariants.sh`
* `bash scripts/ci/check-architecture-text-scan-invariants.sh`
* `cargo test -p icydb-core recovery_replay_is_idempotent -- --nocapture`
* `cargo test -p icydb-core anchor_containment_guard_rejects_out_of_envelope_anchor -- --nocapture`
* `cargo test -p icydb-core unique_conflict_classification_parity_holds_between_live_apply_and_replay -- --nocapture`
* `cargo test -p icydb-core recovery_replay_interrupted_conflicting_unique_batch_fails_closed -- --nocapture`
* `cargo test -p icydb-core grouped_plan_rejects_validation_shape_matrix -- --nocapture`
* `cargo test -p icydb-core sql_query_surfaces_reject_non_query_statement_lanes_matrix -- --nocapture`
* `cargo test -p icydb-core grouped_select_helper_cursor_rejection_matrix_preserves_cursor_plan_taxonomy -- --nocapture`
* `cargo test -p icydb-core shared_query_plan_cache_is_reused_by_fluent_and_sql_select_surfaces -- --nocapture`
* `cargo test -p icydb-core sql_compile_cache_keeps_query_and_update_surfaces_separate -- --nocapture`

Then add targeted SQL/cursor boundary tests for any newly widened public surface.

For current architecture, this should also include live canister checks for:

* malformed SQL at the public canister boundary
* forged or mismatched continuation payloads
* update-warms-query cache reuse without standalone `query` persistence claims
* query-lane mutation rejection at the public SQL canister boundary

---

## Recommended First Run Focus

If you only do one initial pass, make it this:

1. public SQL compile/execute containment
2. continuation cursor tamper resistance
3. recovery replay equivalence
4. memory-id isolation
5. error-domain containment

That gives the highest security value for the current IcyDB architecture.

---

## Initial Risk Read

Directionally, IcyDB appears to have good security-adjacent foundations already:

* strong fail-closed bias
* explicit invariant scripts in CI
* recovery/replay parity tests
* explicit memory-id architecture
* cursor envelope containment checks
* explicit error-domain taxonomy

The main remaining risk is not obvious missing guardrails. It is **surface growth drift**:

* SQL surface widening could accidentally create alternate public semantic paths
* cache identity drift could allow equivalent admitted forms to fragment into different compiled artifacts
* cross-surface reuse could alias semantics that should remain distinct
* cursor/order helpers could regrow parallel validation logic
* recovery and replay semantics could drift under future mutation or SQL feature work
* stable-memory helpers could spread beyond their canonical owner boundary

That is exactly why a dedicated security audit now makes sense.

---

## Final Recommendation

Yes, I think IcyDB should have a dedicated security audit.

Not because the current code looks obviously unsafe, but because the project now has enough public SQL, continuation, and durable-state complexity that security properties should be checked directly rather than inferred from correctness and architecture audits.

The right audit for IcyDB is **not** an auth-token audit.
It is a **public-boundary, cursor-tamper-resistance, recovery-integrity, and stable-memory-isolation audit**.

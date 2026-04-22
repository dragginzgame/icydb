# Weekly Audit: Security Boundary & Fail-Closed Behavior

## Purpose

Define a recurring security audit for IcyDB that focuses on the real
security-sensitive boundaries in the current system rather than treating
security as a generic add-on.

This audit complements the existing correctness and architecture audits by
answering a distinct question:

**Can untrusted SQL input, forged continuation tokens, malformed cursor payloads,
replayed recovery state, misconfigured memory ownership, or cross-surface cache
drift cause unauthorized data exposure outside the validated query, cursor,
namespace, or continuation contract, invalid durable-state transition,
cross-store contamination, integrity loss, or fail-open admission of shapes the
product contract says must reject?**

This is not:

* a style audit
* a performance audit
* a feature roadmap
* a generic refactor checklist

It is a strict public-boundary and fail-closed behavior audit.

---

## Security Lenses

Evaluate findings through these lenses:

* **Confidentiality**
  * no unauthorized data exposure across public SQL, cursor, cache, explain, or
    namespace boundaries
* **Integrity**
  * no invalid durable-state transition, replay drift, cross-store
    contamination, or mutation asymmetry
* **Availability**
  * no planner-contract bypass that admits attacker-controlled expensive shapes
    the product contract says must be rejected

---

## Current Ownership Boundary

This audit must use the current live owner surfaces, not historical helper
names.

Primary owners:

* `db/session/sql/mod.rs`
  * `execute_sql_query`
  * `execute_sql_update`
  * `compile_sql_query`
  * `compile_sql_update`
  * SQL compile-cache identity
* `db/session/sql/execute/*`
  * `execute_compiled_sql`
  * SQL query/update routing
  * grouped SQL execution routing
* `db/session/sql/explain.rs`
  * explain-only SQL boundary
* `db/sql/lowering/mod.rs`
  * `compile_sql_command`
* `db/session/query.rs`
  * typed/fluent query execution
  * cursor token ingress at the session boundary
* `db/cursor/*`
  * cursor token decode
  * scalar/grouped cursor preparation
  * index-range anchor validation
* `db/index/envelope/*`
  * continuation envelope containment
* `db/commit/*`
  * commit marker storage and lifecycle
  * `begin_commit`
  * `finish_commit`
* `db/mod.rs`
  * `ensure_recovered`
* `db/commit/store/*`
  * commit-memory ownership and marker persistence

Historical helper names such as `query_from_sql(...)` and `execute_sql(...)`
must not be used as the primary audit frame.

---

## Scope

Primary scope:

* public SQL query and update entrypoints
* SQL compile, lower, validate, and execute containment for untrusted input
* continuation / cursor token decode and boundary validation
* index-envelope containment and continuation monotonicity
* durable commit-marker and recovery boundaries
* replay equivalence and interrupted-write recovery safety
* memory-id and namespace isolation invariants
* error containment and fail-closed classification at public boundaries
* resource-bound policy checks where unbounded execution could become an abuse
  vector

Out of scope unless IcyDB later adds them:

* caller-auth identity binding
* tenant isolation
* capability scopes
* token trust chains

Do not score the system down for lacking auth/tenant features that are not part
of the current product contract.

---

## Threat Model

Assume the following are adversarial or untrusted unless proven otherwise:

* raw SQL text
* continuation cursor strings and bytes
* cursor boundary payload contents
* public query/update parameters
* caller retry behavior and timing
* interrupted execution around mutation boundaries
* stale or malformed replay state
* incorrect canister memory-id configuration

Assume trusted storage may become corrupt but not magically self-healing.
Recovery and replay must fail closed when corruption or mismatch is detected.

---

## Core Security Invariants

### 1. Public SQL input must fail closed before execution

Malformed, unsupported, or semantically invalid SQL must not reach execution as
partially interpreted work.

Required property:

`parse -> prepare -> lower -> validate -> plan -> execute`

must reject unsupported or malformed input before side effects occur.

### 2. Query and update surfaces must remain distinct

The public SQL query lane must reject state-changing statements.
The public SQL update lane must reject read-only and explain/introspection
statements.

### 3. Continuation tokens must not widen access scope

A forged, stale, or mismatched continuation token must not:

* escape the original envelope
* replay earlier rows outside the contract
* alter entity/order/signature identity
* cross scalar/grouped lanes

### 4. Recovery and replay must preserve mutation integrity

Interrupted write/replay paths must not produce a state that differs from the
canonical committed outcome, and failed replay must fail closed rather than
silently healing into a different state.

### 5. Stable-memory boundaries must remain explicit and isolated

Commit-marker storage must remain pinned to its configured memory id and must
not auto-discover or drift into unrelated memory ranges or store regions.

### 6. Error classification must preserve security meaning

Malformed public input must remain input/cursor-domain failure.
Persisted corruption must remain corruption.
Unsupported feature usage must not be misclassified as success or downgraded
into permissive behavior.

### 7. Resource-bound rules must block abuse-prone shapes

When the product contract says a shape must be bounded for safety/resource
reasons, planning must reject the unbounded form before heavy execution.

### 8. Cache reuse must not weaken surface contracts

Compiled-command cache reuse and shared query-plan cache reuse must not alias
semantically distinct query/update or typed/SQL surfaces in a way that weakens
planner-owned boundaries.

---

## Audit Structure

### Section A — Public SQL Boundary

Goal:

Verify that untrusted SQL text is fully contained by parser, lowering,
validation, and session execution boundaries before any execution side effects
occur.

Check:

* unsupported syntax remains fail-closed
* unsupported semantic shapes remain fail-closed
* invalid field/order/grouping/predicate forms do not reach executor mutation
  or load paths
* `EXPLAIN` does not bypass core validation policy
* query/update lane separation is enforced explicitly
* equivalent admitted forms do not gain separate semantic identity through
  cache-key drift

Search targets:

* `execute_sql_query`
* `execute_sql_update`
* `compile_sql_query`
* `compile_sql_update`
* `compile_sql_command`
* session SQL execute routing
* parser and lowering normalization

Required outcomes:

* one canonical SQL compile path per admitted surface
* no alternate path that skips validation
* no execution of partially validated SQL
* no query/update surface confusion

### Section B — Cache Identity and Surface Isolation

Goal:

Verify that compilation and shared lower-plan reuse remain surface-correct and
cannot create cross-surface semantic contamination.

Check:

* compiled-command cache keys remain distinct for `query` vs `update`
* admitted normalization happens before compiled-command cache insertion
* equivalent admitted SQL forms canonicalize onto the same structural identity
* shared query-plan cache does not alias semantically distinct SQL and
  typed/fluent surfaces
* query calls are not described as creating durable cache state on the IC
* update-warmed reuse does not widen semantics for later query calls

Search targets:

* SQL compiled-command cache keys
* shared query-plan cache keys
* compile/query/update session boundaries
* cache reuse tests and canister checks

Required outcomes:

* no cross-surface cache aliasing
* no duplicate compiled identity for equivalent admitted forms
* no persistence semantics claimed for standalone query misses
* no cache reuse that weakens planner-owned boundaries

### Section C — Continuation / Cursor Tamper Resistance

Goal:

Verify that continuation tokens and cursor boundaries cannot be forged or
widened into unauthorized traversal.

Check:

* token decode errors stay invalid-input/cursor-domain failures
* signature/order/entity mismatches reject
* grouped/scalar lane mismatches reject
* anchor containment is enforced
* continuation strictly advances beyond the anchor
* boundary payload values match canonical order typing
* cursor direction/window/offset mismatches reject
* cursor helpers do not re-derive looser semantics than planner-owned order
  contracts

Search targets:

* `decode_optional_cursor_token`
* grouped cursor decode/preparation
* cursor spine and revalidation boundaries
* anchor validation
* envelope helpers

Required outcomes:

* no token replay that duplicates or omits rows outside contract
* no out-of-envelope advancement
* no cross-order or cross-query cursor reuse

### Section D — Recovery / Replay / Durable Atomicity

Goal:

Verify that interrupted writes and replayed commit markers preserve atomicity
and integrity.

Check:

* `ensure_recovered` runs before write entry when required
* replay remains idempotent
* marker lifecycle is durable and authoritative
* interrupted conflicting unique batches fail closed
* replay does not downgrade invariant failures
* recovery retry cannot silently widen state drift

Search targets:

* `ensure_recovered`
* `begin_commit`
* `finish_commit`
* commit guard lifecycle
* replay row-op application
* replay parity tests

Required outcomes:

* normal and replay paths are semantically equivalent
* no partial-apply state is accepted as success
* marker cleanup occurs only on safe completion

### Section E — Stable-Memory and Namespace Isolation

Goal:

Verify that stable-memory ownership boundaries remain explicit and cannot drift
across unrelated regions.

Check:

* commit memory id is explicitly configured
* recovery configures commit memory before access
* allocator does not scan arbitrary ranges for commit marker discovery
* memory-id mismatch fails closed
* memory registry ownership is explicit

Search targets:

* commit memory configuration
* commit store owner boundary
* memory-id invariant scripts
* recovery initialization

Required outcomes:

* no runtime auto-discovery fallback
* no cross-store memory collision risk by design
* CI invariants still encode the intended boundary

### Section F — Error Containment and Public Failure Semantics

Goal:

Verify that security-relevant failures stay in the correct error domain and do
not become silent permissive behavior.

Check:

* malformed cursor stays cursor/input domain
* persisted decode issues stay corruption
* unsupported features stay unsupported
* invariant violations are not surfaced as harmless user mistakes when they
  indicate internal safety failure
* public mappings preserve underlying security meaning

Search targets:

* `ErrorClass`
* `ErrorOrigin`
* `InternalError`
* cursor decode and cursor plan errors
* query/public error mappings
* corruption constructors

Required outcomes:

* no corruption downgrade
* no invalid-input upgrade into success-like fallback
* no domain confusion at public boundaries

### Section G — Resource Abuse Guardrails

Goal:

Verify that public query shapes with explicit boundedness/resource requirements
still reject attacker-controlled expensive forms before heavy execution.

Check:

* grouped unbounded `ORDER BY` remains rejected
* continuation-compatible resource contracts remain intact
* route/resource policy is enforced in planner, not only execution
* widened SQL shapes do not accidentally bypass boundedness guards

Search targets:

* grouped plan resource checks
* route/resource compliance docs and tests
* SQL widening patches affecting order, grouping, and continuation

Required outcomes:

* explicit contract violations reject before heavy execution
* no planner/executor policy drift

---

## Report Contract

Every generated report must include:

## 0. Run Metadata + Comparability Note

- compared baseline report path
  - daily baseline rule: first run of day compares to latest prior comparable
    report or `N/A`
  - same-day reruns compare to that day’s `security-boundary.md` baseline
- code snapshot identifier
- method tag/version
- comparability status
- auditor
- run timestamp
- branch / worktree state

## 1. Findings Table

| Check | Evidence | Status | Risk |
| ----- | -------- | ------ | ---- |

## 2. Assumptions Validated

Examples:

* no auth/tenant model present in this snapshot
* cache scope present/absent as expected for the audited code snapshot
* public SQL entrypoints identified and unchanged from expected ownership
  boundaries
* continuation token trust model unchanged unless explicitly noted

## 3. Structural Hotspots

List concrete files/modules carrying security-sensitive authority.

## 4. Early Warning Signals

Examples:

* new public SQL side paths
* query/update surface blending
* cursor helper duplication
* replay-specific semantic divergence
* memory-id helper spread beyond canonical owner
* error-domain flattening
* new public entrypoints without invariant coverage

## 5. Risk Score

Normalized `X / 10` score.

## 6. Verification Readout

Use normalized statuses:

* `PASS`
* `FAIL`
* `PARTIAL`
* `BLOCKED`

## 7. Follow-Up Actions

If any result is `FAIL`/`PARTIAL` or risk score is `>= 5`, include owner,
action, and target report run.

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

Then add targeted checks for any newly widened public surface.

For the current architecture, this should also include live canister checks
for:

* malformed SQL at the public canister boundary
* forged or mismatched continuation payloads
* update-warms-query cache reuse without standalone query persistence claims
* query-lane mutation rejection at the public SQL canister boundary

---

## Recommended First-Run Focus

If only one pass is possible, start with:

1. public SQL compile/execute containment
2. query/update surface separation
3. continuation cursor tamper resistance
4. recovery replay equivalence
5. memory-id isolation
6. error-domain containment

That gives the highest security value for the current IcyDB architecture.

# Recurring Audit: Security Boundary & Fail-Closed Behavior

Apply [Domain Scope And Change Triggers](../../README.md#domain-scope-and-change-triggers)
to all inventories, checks, and output sections below. Record selected and
excluded obligations before analysis; broad coverage requires a requested baseline.

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

## Audit Identity

Definition path: `docs/audits/recurring/security/security-audit.md`

Report scope: `security-boundary`

Current method tag: `Security Boundary Method V3`

Use `docs/reports/recurring/YYYY/MM/DD/security-boundary/<run>/report.md`.
Run `01` is the daily baseline; same-day reruns use `02`, `03`, and so on.

Method V3 uses owner-based proof selection, shared executed-test evidence, and
qualitative findings/verdicts. It retains public-boundary, resource-policy,
cache, and fail-closed obligations. Reports comparing with fixed-selector
baselines must describe the proof changes and mark affected verification deltas
non-comparable.

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
  * `execute_trusted_sql_query`
  * `execute_trusted_sql_mutation`
  * `execute_trusted_sql_exact_update`
  * `execute_trusted_sql_prefix_update`
* `db/session/mutation_job.rs`
  * `start_trusted_sql_mutation_job`
  * `advance_trusted_mutation_job`
  * `mutation_job_state`
  * `acknowledge_mutation_job`
* `db/session/sql/resumable_update.rs`
  * canonical intent and private continuation binding
  * bounded Forward batches and stable Verify
  * `compile_sql_query`
  * `compile_sql_mutation`
  * SQL compile-cache identity
* `db/session/sql/execute/*`
  * `execute_compiled_sql`
  * SQL query/mutation routing
  * grouped SQL execution routing
* `db/session/sql/execute/explain.rs`
  * explain-only SQL boundary
* `db/sql/lowering/mod.rs`
  * `compile_sql_command`
* `db/session/query/mod.rs`
* `db/session/query/*`
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
  * `ensure_recovery_admitted`
* `db/startup/*`
  * pure readiness observation and the private replicated driver
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

Do not report missing auth/tenant features as defects when they are not part
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

### 2. SQL query and mutation contracts must remain distinct

The public SQL query lane must reject state-changing statements.
The broad trusted mutation lane must reject read-only, explain/introspection,
and `UPDATE` statements. Trusted `UPDATE` must enter through the exact or
intentional-prefix contract, and those lanes must reject every other statement
family and incompatible SQL window.

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

* `execute_trusted_sql_query`
* `execute_trusted_sql_exact_update`
* `execute_trusted_sql_prefix_update`
* `start_trusted_sql_mutation_job`
* `advance_trusted_mutation_job`
* `mutation_job_state`
* `acknowledge_mutation_job`
* `compile_sql_query`
* `compile_sql_mutation`
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

* `ensure_recovery_admitted` rejects pending work before write planning
* ordinary query/update paths cannot invoke `continue_recovery`
* replay remains idempotent
* marker lifecycle is durable and authoritative
* interrupted conflicting unique batches fail closed
* replay does not downgrade invariant failures
* recovery retry cannot silently widen state drift

Search targets:

* `ensure_recovery_admitted`
* `continue_recovery`
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

## 5. Verdict And Findings

Apply [Findings And Verdicts](../../README.md#findings-and-verdicts).
Justify each finding's severity with the affected trust boundary, reachable
failure or exposure, and current evidence.

## 6. Verification Readout

Use normalized statuses:

* `PASS`
* `FAIL`
* `BLOCKED`

Use `PARTIAL` only in the findings table when a reviewed area has mixed
evidence. Verification commands must resolve to `PASS`, `FAIL`, or `BLOCKED`.

## 7. Follow-Up Actions

For every actionable finding or unresolved `FAIL`/`BLOCKED` verification,
include the owner, action or accepted disposition, and reconsideration trigger.
An unknown boundary needs a proof action; do not present it as an established
security failure without evidence.

If not needed, state:

`No follow-up actions required.`

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
| Query/update lane separation and rejected SQL field roles | `db/session/tests/unit_ordering.rs`, `db/session/sql/` |
| Grouped admission cannot bypass resource policy | `db/query/plan/validate/grouped/`, `db/executor/group/tests.rs` |
| Forged cursor identity, direction, offset, and envelope bounds reject | `db/cursor/tests/mod.rs`, `db/index/envelope/tests.rs` |
| Corrupt persisted rows/markers fail closed | `db/tests/persisted_format_corpus.rs`, `db/commit/store/tests.rs` |
| Interrupted mutation/recovery preserves final row and relation state | `db/session/write.rs` (`mixed_entity_recovery_after_` family) |
| Query reuse remains bound to accepted authority and current parameters | `db/session/tests/unit_ordering.rs` |
| Public endpoint authorization and read authority | `testing/integration/tests/sql_guard.rs`, `testing/integration/tests/read_authority.rs` |

The memory-id, layer-authority, and index-range invariant scripts remain static
boundary evidence. A cache parameter-rebinding test does not by itself prove
schema invalidation or query/update isolation: select evidence for each claim.

Then add targeted checks for any newly widened public surface.

Use the current `scripts/ci/` inventory as authoritative. Do not retain
historical removed script names in this baseline; if a guardrail moved into a
broader invariant script, record the live script that owns it.

For the current architecture, this should also include live canister checks
for:

* malformed SQL at the public canister boundary
* forged or mismatched continuation payloads
* update-warms-query cache reuse without standalone query persistence claims
* query-lane mutation rejection at the public SQL canister boundary

### Read-Only Run Mode

Apply [Authorization And Read-Only Work](../../README.md#authorization-and-read-only-work).
Live canister tests may mutate fixtures or warm caches even when their purpose
is to verify read behavior. Run them only within authorized validation scope.
An existing network does not authorize such effects during inspection-only work.
Record unavailable or unauthorized proof as `BLOCKED` and continue static
inspection without weakening the security claim's evidence requirements.

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

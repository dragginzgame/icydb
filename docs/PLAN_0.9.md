# Referential Integrity Release Roadmap (0.9.x)

`0.9.x` is the **Referential Integrity release**.

The 0.9 series focuses on closing correctness gaps that remained after 0.8,
while preserving the explicit 0.8.x contract.

No implicit transactional behavior is introduced.

---

### 1. Strong Referential Integrity - Delete-Time Validation

IcyDB 0.9 will enforce delete-time referential integrity for schema-declared
strong relations.

**Goals**

* Reject deletes that would leave dangling strong references
* Perform validation deterministically before commit
* Introduce reverse indexes for strong-relation delete validation to avoid full source-store scans
* Preserve explicit weak-relation behavior (no existence validation)
* Keep semantics validation-only (no implicit cascades)

**Outcomes**

* Strong relation correctness holds for both save and delete paths
* Dangling-reference creation is blocked by executor validation
* Delete-time validation uses reverse-index lookups for predictable scale
* RI behavior remains explicit and schema-driven

**Non-Goals**

* Cascading deletes
* Implicit graph traversal or relational query behavior

---

### 2. Explicit Transaction Semantics (Opt-In Surface)

IcyDB 0.9 will define and ship explicit transaction-facing semantics without
silently changing existing non-transaction APIs.

**Goals**

* Keep transactional behavior explicit and opt-in
* Preserve existing fail-fast, non-atomic batch helper semantics unless users adopt explicit transaction APIs
* Ship formal semantics, recovery behavior, and failure-mode tests alongside any transactional surface

**Outcomes**

* A clear boundary between existing non-atomic helpers and explicit transactional behavior
* Predictable migration path for users who need stronger multi-mutation guarantees

**Non-Goals**

* Silent upgrades of existing batch helpers to transactional behavior
* Implicit retries or hidden recovery policy at API boundaries
* Distributed/cross-canister transactions

---

### 3. Pagination Efficiency Without Semantic Drift

IcyDB 0.9 will optimize cursor-pagination execution while preserving the 0.8
pagination contract.

**Goals**

* Reduce full candidate-set work for large ordered paged scans
* Preserve canonical ordering and continuation-signature compatibility checks
* Keep forward-only, live-state continuation semantics unchanged

**Outcomes**

* Better large-result pagination performance without changing user-visible cursor rules
* Lower regression risk via parity tests between optimized and baseline execution paths

**Non-Goals**

* Snapshot isolation across pages
* Backward or bidirectional cursors
* Changes to cursor token semantics or validation contract

---

### 4. Contract Hardening and Diagnostics

IcyDB 0.9 will continue tightening structural guardrails that prevent planner
and executor drift.

**Goals**

* Expand structural regression coverage around post-access execution phases
* Keep error classification explicit (`Unsupported`, `Corruption`, `Internal`) at execution boundaries
* Improve diagnostics where contract violations or corruption are detected

**Outcomes**

* Faster detection of behavior regressions in query/mutation boundaries
* Clearer operator-facing signals when failures are user input vs corruption vs engine bugs

**Non-Goals**

* Relaxing existing correctness invariants
* Reintroducing implicit behavior at executor boundaries

---

## Invariants Preserved from 0.8

0.9 is a strengthening release, not a reinterpretation.

The following invariants remain stable:

* Mutation determinism
* Executor validation order
* Query planning contract
* Cursor token structure
* Error taxonomy
* Non-atomic helper semantics

---

## Explicit Non-Goals (0.9.x)

The following remain out of scope:

* Implicit transactional behavior
* Cascading deletes
* Authorization or capability-based identity models
* Relational joins/query planning
* Snapshot-consistent cross-request pagination guarantees

---

## Summary

0.9.x is a correctness-and-boundaries release.

It prioritizes delete-time strong-relation validation, explicit transaction
semantics (opt-in), and pagination performance improvements that preserve the
0.8 query contract.

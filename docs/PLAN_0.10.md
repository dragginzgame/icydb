# Data Integrity Release Roadmap (0.10.x)

`0.10.x` is the **Data Integrity release**.

The 0.10 series focuses on **physical durability, schema evolution safety, and explicit no-data-loss guarantees**.

If 0.9 strengthens logical correctness,
0.10 strengthens **format stability and upgrade survivability**.

This release series ensures that:

> No upgrade, migration, or format evolution can silently corrupt or orphan persisted data.

No implicit data rewrites are introduced.

All structural changes must be explicit, recoverable, and observable.

---

## Progress Snapshot (as of TBD)

Estimated completion toward the `0.10.x` goals in this plan:

* Stable Row Format & Versioning: **0%**
* Commit Marker Versioning & Replay Compatibility: **0%**
* Explicit Migration Engine (Row-Op Based): **0%**
* Corruption Detection & Integrity Tooling: **0%**

Overall estimated progress: **0%**

---

# 1. Stable Row Format & Schema Evolution

IcyDB 0.10 will introduce explicit row-format versioning and backward-compatible decode rules.

## Goals

* Add explicit version tagging to persisted row format
* Guarantee backward-compatible decoding across upgrades
* Prevent decode panics from rendering data unreachable
* Formalize allowed schema evolution rules
* Ensure index derivation changes do not orphan data

## Outcomes

* Persisted rows include explicit format version
* Decode paths handle at least N-1 version safely
* Structural corruption is detected, not silently ignored
* Schema evolution rules are documented and enforced
* Format-breaking changes require explicit migration plan

## Non-Goals

* Automatic field-level migrations
* Implicit schema rewrite on upgrade
* Transparent structural coercion without version awareness

---

# 2. Commit Marker Stability & Replay Guarantees

IcyDB 0.10 will formalize the commit protocol wire format and recovery guarantees across upgrades.

## Goals

* Version the `CommitMarker` format explicitly
* Guarantee replay compatibility across minor upgrades
* Prevent marker decode failure from bricking recovery
* Freeze core commit semantics

## Outcomes

* Commit markers include explicit protocol version
* Recovery path supports version-aware decoding
* Commit replay semantics are formally documented
* Marker format changes require compatibility strategy

## Non-Goals

* Changing commit atomicity semantics
* Rewriting recovery model
* Introducing distributed transaction semantics

---

# 3. Explicit Migration Engine (No Data Loss)

IcyDB 0.10 introduces an explicit migration execution model built on the existing row-op commit protocol.

Migrations must be:

* Deterministic
* Recoverable
* Replayable
* Non-lossy

## Goals

* Define a `MigrationPlan` abstraction
* Execute migrations as row-op streams
* Use existing commit marker + recovery path
* Prevent partial migration states
* Ensure migration rollback safety

## Outcomes

* Migrations are structurally equivalent to normal row mutations
* Migration execution is crash-safe
* Data transformation is explicit and reviewable
* Upgrade failures cannot silently destroy data

## Non-Goals

* Automatic destructive migrations
* In-place schema rewrite without rollback path
* Best-effort migration without recovery guarantees

---

# 4. Corruption Detection & Integrity Tooling

IcyDB 0.10 strengthens detection and observability of structural corruption.

Silent corruption is unacceptable.

## Goals

* Detect invalid row format during decode
* Detect index/data divergence
* Detect reverse-index inconsistencies
* Provide integrity scan utilities
* Expand error taxonomy for corruption classes

## Outcomes

* Corruption results in explicit `Corruption` errors
* Integrity scans can validate:

  * Data ↔ Index consistency
  * Forward ↔ Reverse relation consistency
* Snapshot tools expose structural counts
* Operators can distinguish corruption vs misuse

## Non-Goals

* Silent repair without operator visibility
* Automatic best-guess repair of corrupted data
* Masking corruption as user error

---

# 5. Upgrade & Recovery Hardening

IcyDB 0.10 formalizes the no-data-loss invariant across canister upgrades.

## Goals

* Guarantee upgrade does not invalidate existing rows
* Guarantee recovery replay is version-stable
* Ensure recovery never discards valid persisted rows
* Add stress tests for crash-mid-migration scenarios

## Outcomes

* Upgrade safety becomes a tested invariant
* Crash during migration is recoverable
* Replay semantics remain deterministic
* Marker + row version compatibility is validated

## Non-Goals

* Snapshot isolation across upgrades
* Cross-canister migration coordination
* Rolling format upgrades without compatibility window

---

# Invariants Introduced in 0.10

The following become explicit structural guarantees:

* Persisted rows are versioned
* Commit markers are versioned
* Decode failure is explicit and classified
* Migrations are commit-protocol-driven
* No upgrade may silently orphan persisted data
* No recovery path may discard valid rows without classification

---

# Explicit Non-Goals (0.10.x)

The following remain out of scope:

* Cross-canister transactional upgrades
* Distributed schema coordination
* Automatic cascade repair of corrupted relations
* Automatic index rebuild on structural drift (manual tooling only)

---

# Summary

0.10.x is the **Data Integrity release**.

If 0.9 ensures the engine behaves correctly,
0.10 ensures the engine survives change without losing data.

It formalizes:

* Row format stability
* Commit replay compatibility
* Explicit migration semantics
* Corruption detection boundaries
* Upgrade safety guarantees

0.10.x is not about new features.

It is about making IcyDB structurally resilient.

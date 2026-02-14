# IcyDB Roadmap

This document describes the **long-term direction** of IcyDB.

It intentionally does **not** redefine the current contract.
All guarantees, invariants, and limits for released versions are defined in:

- `docs/ATOMICITY.md`
- `docs/REF_INTEGRITY.md`

This roadmap describes **where the system is going**, not what is currently guaranteed.
Implementation cleanup tasks supporting this direction are tracked separately.

---

## Current State (0.8.x)

As of the 0.8 series:

- Single-entity save and delete operations are **atomic**
- Save-time referential integrity is enforced **only for strong relations**
- Batch write helpers are **fail-fast and non-atomic**
- Atomicity and recovery guarantees are scoped to the current executor and commit model

No transactional guarantees exist beyond what is explicitly documented in the 0.8 contract.

---

## Direction

The project direction remains stable and intentional:

- **Typed-entity–first APIs**
  - Typed schemas are the canonical source of truth
  - Structural models are derived, internal representations
- **Deterministic planning and execution**
  - Identical inputs must produce identical plans
  - Ordering and validation rules are explicit and enforced
- **Explicit invariants**
  - Correctness properties are schema-declared
  - Enforcement is mechanical, bounded, and testable
- **Clear API boundaries**
  - Public APIs are stable and typed
  - Engine internals remain flexible and non-contractual

This direction governs all future feature work.

---

## Planned for 0.9.x

- **Delete-side referential integrity enforcement for strong relations** is targeted for an early `0.9.x` release.
- **Reverse indexes for strong relations** are included in the 0.9 delete-side RI scope to avoid scan-based delete validation.
- Scope remains validation-only (no implicit cascades), unless a later spec says otherwise.

---

## Explicit Goals

### Transactions

**Multi-entity transactions are a project goal.**

Future releases may introduce transactional semantics that span multiple entities
and/or multiple mutations.

This goal does **not** change the 0.8 contract.

Specifically:

- Current batch helpers remain non-atomic
- No implicit transactional behavior is introduced
- No partial transaction guarantees exist today

Any transactional feature must ship with:

- a formal semantics specification
- updated atomicity and recovery documentation
- explicitly named APIs (no silent upgrades)
- migration guidance for existing users
- tests covering failure, replay, and recovery scenarios

Transactions will be introduced only when the above conditions are met.

---

## Non-Goals (Near Term)

The following are explicitly **not** goals for the near term:

- Implicit or inferred transactional behavior
- Relaxing existing atomicity guarantees
- Introducing relational query semantics
- Hiding failure modes behind retries or recovery logic

Correctness remains explicit, not magical.

---

## Summary

IcyDB evolves deliberately:

- current guarantees are strict and limited
- future power comes with explicit semantics
- nothing silently changes underneath users

The roadmap is directional, not contractual.

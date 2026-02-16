# IcyDB Roadmap

This document describes the **long-term direction** of IcyDB.

It intentionally does **not** redefine the current contract.
All guarantees, invariants, and limits for released versions are defined in:

- `docs/ATOMICITY.md`
- `docs/REF_INTEGRITY.md`
- `docs/TRANSACTION_SEMANTICS.md`

This roadmap describes **where the system is going**, not what is currently guaranteed.
Implementation cleanup tasks supporting this direction are tracked separately.

---

## Current State (0.8.x)

As of the 0.8 series:

- Single-entity save and delete operations are **atomic**
- Save-time referential integrity is enforced **only for strong relations**
- `*_many_non_atomic` batch helpers are fail-fast and non-atomic
- `*_many_atomic` batch helpers are atomic for a **single entity type per call**
- Atomicity and recovery guarantees are scoped to the current executor and commit model

No multi-entity transaction guarantees exist beyond what is explicitly documented.

---

## Direction

The project direction remains stable and intentional:

- **Typed-entity-first APIs**
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

## Planned for 0.9.x - Strengthening Release

`0.9.x` is the **Strengthening release**.

- Consolidates and hardens existing correctness boundaries from 0.8.
- Finalizes delete-side strong relation validation and supporting diagnostics.
- Keeps transaction semantics explicit and opt-in.
- Continues pagination performance work without semantic drift.

See `docs/old/PLAN_0.9.md` for the detailed `0.9.x` plan.

---

## Planned for 0.10.x - Index Keys Release

`0.10.x` is the **Index Keys release**.

- Canonical variable-length ordered secondary index keys.
- Versioned migration from fixed-slot key encoding.
- Ordered traversal/range scan correctness at byte-order level.

See `docs/PLAN_0.10.md` for the detailed `0.10.x` plan.

---

## Planned for 0.11.x - Data Integrity Release

`0.11.x` is the **Data Integrity release**.

- Row format versioning and backward-compatible decode rules.
- Commit marker compatibility and replay safety across upgrades.
- Explicit migration execution and corruption-detection tooling.

See `docs/PLAN_0.11.md` for the detailed `0.11.x` plan.

---

## Explicit Goals

### Transactions

**Multi-entity transactions are a project goal.**

Future releases may introduce transactional semantics that span multiple entities
and/or multiple mutations.

This goal does **not** change the 0.8 contract.

Specifically:

- Existing `*_many_non_atomic` helpers remain fail-fast and non-atomic
- Any stronger batch semantics are opt-in (for example, `*_many_atomic`) and currently single-entity-type only
- No implicit transactional behavior is introduced
- No multi-entity transaction guarantees exist today

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

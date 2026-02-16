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

## Current State (0.10.2)

As of `0.10.2` (2026-02-16):

- Single-entity save and delete operations are **atomic**
- Save-time referential integrity is enforced **only for strong relations**
- Delete-time referential integrity for strong relations is enforced
- `*_many_non_atomic` batch helpers are fail-fast and non-atomic
- `*_many_atomic` batch helpers are atomic for a **single entity type per call**
- Atomicity and recovery guarantees are scoped to the current executor and commit model
- `0.9.x` strengthening work is shipped (see `docs/status/0.9-status.md`)
- `0.10.x` index-key ordering work is shipped (see `docs/status/0.10-status.md`)

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

## Shipped in 0.9.x - Strengthening Release

`0.9.x` is the **Strengthening release**.

- Consolidates and hardens existing correctness boundaries from 0.8.
- Finalizes delete-side strong relation validation and supporting diagnostics.
- Keeps transaction semantics explicit and opt-in.
- Continues pagination performance work without semantic drift.

See `docs/design/0.9-referential-integrity-v1.md` for the detailed `0.9.x`
plan.
Current shipped status: `docs/status/0.9-status.md`.

---

## Shipped in 0.10.x - Index Keys Release

`0.10.x` is the **Index Keys release**.

- Canonical variable-length ordered secondary index keys.
- Versioned migration from fixed-slot key encoding.
- Ordered traversal/range scan correctness at byte-order level.

See `docs/design/0.10-index-ordering.md` for the detailed `0.10.x` plan.
Current shipped status: `docs/status/0.10-status.md`.

---

## Planned for 0.11.x - Secondary Range Pushdown Release

`0.11.x` is the **Secondary Range Pushdown release**.

- Secondary index range pushdown for `>`, `>=`, `<`, `<=`, and `BETWEEN`.
- Composite prefix + range eligibility with deterministic bounds.
- Pagination and fallback parity preservation under bounded range traversal.

See `docs/design/0.11-range-pushdown.md` for the detailed `0.11.x` plan.
Current tracking status: `docs/status/0.11-status.md`.

---

## Future Milestone (Post-0.11) - Data Integrity

Data-integrity hardening has moved to a future milestone after 0.11.

- Row format versioning and backward-compatible decode rules.
- Commit marker compatibility and replay safety across upgrades.
- Explicit migration execution and corruption-detection tooling.

See `docs/design/data-integrity-v1.md` for the detailed deferred plan.

---

## Future Architectural Cleanup (Post-0.11)

### Structural Identity Projection for Plan DRYness

There is a known DRY risk in keeping canonical ordering and hash encoding as
separate structural implementations for access-path identity.

Target direction:

- Introduce a single structural identity projection, for example:
  `fn structural_identity(&self) -> IdentityParts`.
- Define `IdentityParts` as deterministic and fully ordered.
- Ensure `IdentityParts` includes index name, index fields, prefix values, and
  range bounds (including bound discriminants).
- Make canonical normalization compare `IdentityParts`.
- Make plan hash/fingerprint encoding serialize `IdentityParts`.

Why this matters:

- Removes cross-module drift risk between normalization and hashing.
- Prevents plan-cache instability from mismatched structural encodings.
- Reduces repeated match logic when access-path shapes evolve.

Scope note:

- This is an architectural refactor, not a quick deduplication patch.

---

## Explicit Goals

### Transactions

**Multi-entity transactions are a project goal.**

Future releases may introduce transactional semantics that span multiple entities
and/or multiple mutations.

This goal does **not** change the current 0.10 contract.

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

### Signed Binary Cursors

Signed opaque binary cursors are a future hardening goal.

Target direction:

- Move continuation tokens to an opaque binary envelope instead of a structured
  payload surface.
- Require signature verification for every cursor (unsigned binary cursors are
  not acceptable).
- Bind signature input to both boundary bytes and canonical query-plan
  signature to prevent tampering/rebinding.

Conceptual format:

- `cursor = base64(mac || key_bytes)` where `mac = HMAC(secret, key_bytes ||
  plan_signature)`

Why this is valuable:

- Smaller payloads and less serialization overhead.
- No field-name/type exposure in public cursor data.
- Stronger resistance to client-crafted boundary jumps.

Adoption trigger:

- Prioritize this when continuation payload shape leaks internal planning
  details (field names, composite component layout, or similar internals).

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

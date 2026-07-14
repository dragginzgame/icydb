# IcyDB Transaction Semantics (Current Batch-Helper Contract)

This document defines the explicit write semantics for IcyDB batch helpers in
the current line.

It is intentionally narrow: it covers what the current APIs guarantee today.
The atomic batch surface is strictly single-entity-type; it is not multi-entity
transaction support.
The broader operator-facing durability boundary is defined in
`docs/contracts/DURABILITY.md`.

This document does not define database-session transactions. IcyDB does not
provide Postgres-style transaction blocks, isolation levels, or automatic
rollback when a canister update method returns `Err`. If application code
performs a successful write and then returns `Err` later in the same update
method, that prior write remains committed unless the application explicitly
compensates for it.

---

## Scope

Covered by this document:

* Single-entity batch save helpers:
  * `insert_many_atomic`
  * `update_many_atomic`
  * `replace_many_atomic`
  * `insert_many_non_atomic`
  * `update_many_non_atomic`
  * `replace_many_non_atomic`
* Failure behavior
* Recovery behavior

Out of scope:

* Multi-entity transactions
* Cross-canister transactions
* Multi-message transaction protocols

---

## API Lanes

IcyDB now has two explicit lanes for batch writes.

### Atomic lane (`*_many_atomic`)

* Scope: one entity type per call
* Contract: all-or-nothing for that batch
* If any item fails before commit, no row from that batch is persisted
* Uses commit-marker-bound journal batches and recovery folding for durable
  correctness
* Not a multi-entity transaction

### Non-atomic lane (`*_many_non_atomic`)

* Scope: one entity type per call
* Contract: fail-fast convenience helper
* Earlier items may commit before a later item fails
* No transactional rollback across batch items
* Not a multi-entity transaction

---

## Atomic Lane Execution Model

For `*_many_atomic`, execution is split into two phases:

### Phase 1: Pre-commit (fallible)

For each item in request order:

* run sanitize/validate/invariant checks
* run strong-relation validation
* build the logical row operation and its current journal record from accepted
  durable state plus the new payload
* reject duplicate keys within the same batch request

If any step fails, execution returns an error and does not open a commit window.

### Phase 2: Apply (infallible by construction)

After all row operations are staged:

* preflight commit-row preparation is performed
* the commit marker containing current journal batches is persisted
* marker-bound journal batches are appended
* prepared row operations are applied mechanically in request order
* marker is cleared on successful finish

No new fallible semantics are introduced after marker persistence.

---

## Failure and Recovery Semantics

### Pre-commit failure

* Returns an error
* Persists no row from the atomic batch

### Failure after marker persistence

* Marker-bound journal publication remains authoritative
* Guarded read/write entrypoints publish and fold pending marker batches before
  normal execution
* Durable end state converges to the marker-described journal state

This follows the same commit/recovery model documented in `docs/contracts/ATOMICITY.md`.

---

## Ordering and Visibility Guarantees

For one `*_many_atomic` call:

* Row-ops are applied in request order within that atomic batch.
* Rows staged during pre-commit are not visible as committed state through
  guarded query/session entrypoints.
* The batch becomes visible through guarded query/session entrypoints only after
  commit completion.
* Direct raw-store access that bypasses guarded entrypoints remains out of
  contract (see `docs/contracts/ATOMICITY.md`).

---

## Edge Cases (Current Behavior)

### Duplicate keys inside one atomic batch request

* Rejected before commit
* No partial rows from that request are persisted

### Insert conflict with existing row

* Atomic lane: whole batch fails, no new rows from that request are persisted
* Non-atomic lane: already-committed prefix remains committed

### Update on missing row

* Atomic lane: whole batch fails, no rows from that request are persisted
* Non-atomic lane: already-committed prefix remains committed

### Strong-relation checks and staged rows

Strong-relation validation is performed against currently persisted target
stores during pre-commit validation.

Rows staged inside the same atomic batch are not treated as visible relation
targets during that validation pass.

---

## Non-Goals

This API surface does not provide:

* implicit upgrades of old helpers
* hidden retries or inferred recovery policy at API boundaries
* multi-entity atomicity
* multi-entity transaction coordination (kept separate due higher complexity)

Any expansion beyond this requires a new explicit transaction spec.

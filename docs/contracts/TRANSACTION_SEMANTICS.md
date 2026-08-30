# IcyDB Transaction Semantics (Current Batch-Helper Contract)

This document defines the explicit write semantics for IcyDB batch helpers in
the current line.

It is intentionally narrow: it covers what the current APIs guarantee today.
The atomic batch surface may span at most 64 accepted entities from one
captured runtime root and one registered store. It is not a cross-store,
cross-canister, or multi-message transaction system.
The broader operator-facing durability boundary is defined in
`docs/contracts/DURABILITY.md`. Per-row strictness and mutation ingress are
defined in `docs/contracts/WRITE_ADMISSION.md`.

This document does not define database-session transactions. IcyDB does not
provide Postgres-style transaction blocks, isolation levels, or automatic
rollback when a canister update method returns `Err`. If application code
performs a successful write and then returns `Err` later in the same update
method, that prior write remains committed unless the application explicitly
compensates for it.

---

## Scope

Covered by this document:

* `execute_trusted_structural_mutation_batch`
* `execute_trusted_structural_mutation_batch_rows`
* `execute_trusted_structural_insert_batch`
* Failure behavior
* Recovery behavior

Out of scope:

* Cross-store transactions
* Cross-canister transactions
* Multi-message transaction protocols

---

## API Lanes

IcyDB exposes one canonical maintained batch-write lane:
`execute_trusted_structural_mutation_batch`.

* Scope: one accepted runtime root and store per call, with at most 64 distinct
  entities.
* Input: field-name-driven structural inserts, updates, replacements, and
  deletes in deterministic request order.
* Contract: all-or-nothing for that batch.
* If any item fails before commit, no row from the batch is persisted.
* The operation uses commit-marker-bound journal batches and recovery folding
  for durable correctness.
* It is not a database-session or cross-store transaction.

`execute_trusted_structural_insert_batch` is the insert-only convenience shape
over that canonical lane. `execute_trusted_structural_mutation_batch_rows`
checks one generated binding and same-entity target set before dispatch, then
projects the ordered canonical results; it does not add another mutation or
commit lane. Single structural mutations use the same owner. No alternate
generated-entity batch lane is maintained.

### Application read/compute/write boundary

The atomic batch closes partial-publication failure windows; it does not make a
calculation from an earlier application read current. A canister application
that derives a conservation-sensitive split, merge, or transfer must:

1. authorize the caller;
2. perform any asynchronous work;
3. read the current accepted rows;
4. calculate the complete same-store mutation set; and
5. submit that set synchronously, without an `await` or logical interleaving
   point between the final read and batch admission.

After any `await`, the application must re-read and recompute before submitting
the batch. This is the maintained non-interleaved flow; IcyDB does not add a
hidden compare-and-set, retry, or cross-store transaction around it.

### SQL exact and prefix update

Trusted exact SQL `UPDATE` first proves selector exhaustion within the caller's
positive `require_affected_at_most` assertion and the engine's independent
scanned-key budget. Selection uses authoritative primary-key traversal. The
complete selected set then uses the same single-entity atomic preparation and
marker boundary as an atomic batch; either cap-plus-one overflow or any other
pre-marker failure changes no rows.

Trusted prefix SQL `UPDATE` is also atomic for its selected batch, but its
ordered `LIMIT` deliberately selects only one prefix. Prefix success makes no
claim about matching rows beyond that window.

---

## Atomic Lane Execution Model

For one structural mutation batch, execution is split into two phases:

### Phase 1: Pre-commit (fallible)

For each item in request order:

* apply the complete `docs/contracts/WRITE_ADMISSION.md` contract
* resolve it from the same captured accepted root and store, then bind it to
  its own exact accepted entity and schema head
* preserve its insert/update/replace/delete intent and materialize its save
  after-image or delete before-image under one operation timestamp
* reject duplicate target keys across every operation-kind combination
* build one complete final-row overlay and logical row-operation set

Constraint, unique-index, relation, and derived-index preparation observe the
complete final overlay. An updated or deleted old row releases its previous
membership. A relation source uses its staged final image, and relation target
lookup sees inserted or updated targets while treating deleted targets as
absent.

Public value conversion and exact Candid response-size validation complete
before the marker is opened. The current bounds are 4,096 operations, 64
distinct entities, 16 MiB
of cumulative encoded keys plus canonical before/after rows, 4 MiB per row,
and 1 MiB for the encoded ordered result vector of
`execute_trusted_structural_mutation_batch`. Existing single-mutation and
insert-only convenience calls retain their prior response behavior; SQL
`RETURNING` owns an independent query response bound.

If any step fails, execution returns an error and does not open a commit window.

### Phase 2: Apply (infallible by construction)

After all row operations are staged:

* the complete mixed `CommitRowOp` set is preflighted
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

This follows the same commit/recovery model documented in
`docs/contracts/ATOMICITY.md`.

---

## Ordering and Visibility Guarantees

For one structural mutation batch:

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

* The whole batch fails and no new rows from that request are persisted.

### Update on missing row

* The whole batch fails and no rows from that request are persisted.

### Relation checks and staged rows

Relation validation reads the complete batch-final overlay before falling back
to committed accepted storage. A staged source removal or reference update is
therefore authoritative for delete safety, and save-side target validation can
see a staged target creation while treating a staged target delete as absent.

### Result rows

* Inserts, updates, and replacements return their final after-images.
* Deletes return their removed before-images.
* Rows retain request order.
* The general batch returns one `DynamicMutationResult` per request, each with
  exactly one row and a zero-or-one `affected_rows` count.
* The mixed typed builder retains that same vector and issues sealed
  entity-typed handles for ordered dynamic-result and generated-row lookup. A
  handle from another result owner fails closed; it creates no second result
  or execution protocol.

### Identity

Only actual Identity-generating inserts consume tentative allocation ordinals.
Each participating Identity owner has one statement-local cursor; all exact
owner ranges publish under the same marker. Interleaved updates, replacements,
and deletes do not create gaps. Rejection before marker publication consumes
no value; committed mixed journal proof filters existing-key puts and deletes
from each exact contiguous generated range.

---

## Non-Goals

This API surface does not provide:

* implicit upgrades of old helpers
* hidden retries or inferred recovery policy at API boundaries
* cross-store or cross-canister atomicity
* transaction blocks, partial rollback, or multi-message coordination

Any expansion beyond this requires a new explicit transaction spec.

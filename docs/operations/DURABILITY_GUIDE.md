# IcyDB Durability Operator Guide

This guide turns the durability contracts into operational rules for canister
authors and operators.

Normative contracts:

- `docs/contracts/DURABILITY.md`
- `docs/contracts/ATOMICITY.md`
- `docs/contracts/TRANSACTION_SEMANTICS.md`
- `docs/contracts/PERSISTED_FORMAT_POLICY.md`
- `docs/contracts/PERSISTED_FORMAT_INVENTORY.md`

## Storage Choice

Use `storage(journaled(...))` for durable user data.

Journaled stores persist through the commit marker, journal tail, and canonical
stable-memory stores. They are the production durability lane.

Use `storage(heap())` only when volatility is intentional.

Heap stores are live Rust memory. They are useful for tests, caches, scratch
state, and explicit performance comparisons. They do not recover rows or
indexes across upgrade/reinitialization and must not be used for durable user
state.

## Memory ID Checklist

For each canister:

- reserve one `commit_memory_id`;
- reserve four memory IDs per `journaled` store:
  - `data_memory_id`;
  - `index_memory_id`;
  - `schema_memory_id`;
  - `journal_memory_id`;
- do not reuse these IDs for non-IcyDB stable-memory structures;
- do not change memory IDs after data has been written unless the change is
  part of an explicit migration plan.

Memory IDs are durable allocation identity. Reusing or remapping them can make
valid data unreadable or point IcyDB at unrelated bytes.

## Write Atomicity

IcyDB write atomicity is scoped to one IcyDB mutation operation or explicit
atomic batch helper.

Safe assumptions:

- a single save/delete operation is atomic;
- `*_many_atomic` is all-or-nothing for one entity type in one call;
- guarded reads and writes recover marker-authorized interrupted commits before
  normal access proceeds.

Unsafe assumptions:

- a canister update method is not a database transaction block;
- returning `Err` after a successful IcyDB write does not undo that write;
- `*_many_non_atomic` may leave an already committed prefix;
- multiple IcyDB writes in one update method are not automatically rolled back
  together if a later write or application check fails.

When callers need all-or-nothing behavior for a same-entity batch, use the
atomic batch helpers. When partial progress is acceptable and intentional, use
the non-atomic helpers and document the prefix-commit behavior at the call site.

## Async And Reentry

IcyDB mutation and recovery runtime paths are synchronous.

Do not design a write wrapper that assumes IcyDB will preserve a larger
transaction across an `await`, cross-canister call, timer, background task, or
multi-message workflow. If a workflow needs compensation after a later awaited
step fails, that compensation belongs to application logic.

## Guarded Access

Use generated IcyDB entrypoints, `DbSession`, and public typed/fluent APIs for
normal reads and writes. These routes enforce the guarded recovery boundary.

Avoid direct raw-store or index access in production code. Direct access that
bypasses guarded recovery is outside the contract and can observe startup,
interrupted-recovery, or stale derived-index state.

Startup rule:

- the database is fully consistent after the first successful guarded recovery
  pass;
- before that pass, a leftover marker or journal tail may still represent
  recovery work;
- tools that inspect raw stable memory before guarded recovery own the risk.

## Recovery Guarantees

Current recovery is designed for internally produced interrupted states:

- interruption after marker write;
- interruption around marker-bound journal append;
- interruption around journal-tail fold and fold watermark persistence;
- interruption during derived secondary-index rebuild/fold;
- marker-cleared readiness restoration on guarded reentry.

Recovery is not a general repair tool for arbitrary hostile stable-memory
images. It does not promise to detect every well-formed-but-wrong value without
a future checksum or integrity-scan design.

## Backup, Restore, And Import

Raw stable-memory backup, restore, and import are not supported product
surfaces in the current line.

Supported:

- normal IC stable-memory preservation for the same canister;
- normal canister upgrade preservation when memory IDs and generated wiring
  remain stable;
- guarded recovery of IcyDB-produced interrupted commit/recovery state.

Unsupported:

- importing raw stable-memory bytes from another canister;
- accepting untrusted backup images;
- version-gap restore from old internal formats;
- treating malformed external bytes as recoverable production data.

If an operator builds custom backup tooling today, it is outside IcyDB's
supported compatibility guarantee. Future import support must define trust,
format compatibility, corruption detection, and resource limits first.

## Checksums

The current line does not write persisted checksums.

Current protection is structural validation, bounded fallible decoding, and
guarded recovery. Checksums remain a future persisted-format feature and must
be classified under `docs/contracts/PERSISTED_FORMAT_POLICY.md` before being
added.

## Recovery Size Limits

0.190 added a checked 256-row secondary-index rebuild characterization. 0.191
raises the simple secondary-index host floor to 1,024 rows, adds a
128-row-per-shape mixed ordinary, conditional, and expression index rebuild
floor, and adds a PocketIC same-WASM upgrade/reentry instruction probe over the
32-row journaled `sql_perf` fixture. Treat those as regression floors and
audit budgets, not production limits.

Until IcyDB publishes a production recovery-size bound or streaming
fold/rebuild design:

- avoid claiming arbitrary large-index recovery is budget-certified;
- monitor canister instruction and memory pressure around large schema/index
  changes;
- prefer smaller, explicit operational rollout windows for large data/index
  migrations;
- treat recovery failure as fail-closed: reads and writes should not proceed on
  partially recovered state.

The future streaming recovery follow-up is tracked in
`docs/design/0.191-durability-productization-format-policy/streaming-recovery-followup.md`.

## Quick Checklist

Before deploying durable IcyDB data:

- use `storage(journaled(...))` for durable stores;
- keep `heap()` stores limited to intentionally volatile state;
- reserve stable-memory IDs and do not reuse them;
- use generated/session APIs instead of direct raw-store access;
- use `*_many_atomic` when a same-entity batch must be all-or-nothing;
- document any `*_many_non_atomic` use as prefix-commit behavior;
- do not rely on returned `Err` to roll back prior successful writes;
- do not claim raw backup/import support;
- do not claim checksum-backed corruption detection;
- do not claim production recovery-size guarantees beyond published evidence.

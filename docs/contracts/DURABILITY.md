# IcyDB Durability Contract

This document defines the current operator-facing durability contract for
IcyDB on the Internet Computer.

It is normative for the current line. It consolidates the tested reliability
model from `docs/contracts/ATOMICITY.md`,
`docs/contracts/TRANSACTION_SEMANTICS.md`, and the 0.189/0.190 recovery proof
work into one operational boundary.

Evidence sources:

- `docs/design/0.189-mega-audit-2/0.189-design.md`
- `docs/design/0.189-mega-audit-2/audit-results.md`
- `docs/design/0.190-ic-reliability-followup/0.190-design.md`
- `docs/design/0.190-ic-reliability-followup/0.190-evidence.md`
- `docs/design/archive/0.191-durability-productization-format-policy/0.191-evidence.md`

## Scope

This contract applies to IcyDB stores that run inside one canister update
message and persist through Internet Computer stable memory.

In scope:

- `journaled` stores and their commit/recovery protocol;
- volatile `heap` stores and their explicit non-durability;
- guarded recovery before reads and writes;
- single-message mutation atomicity;
- recovery from internally produced interrupted commit state;
- documented recovery-size evidence and limits.

Out of scope in the current line:

- PostgreSQL or SQLite transaction blocks;
- POSIX file-database durability semantics;
- multi-process database locking;
- filesystem `fsync`, directory sync, or rename-order guarantees;
- cross-canister or cross-message transactions;
- hostile raw stable-memory import as a supported product feature;
- backup/restore/import tooling with compatibility guarantees.

## Storage Modes

### `journaled`

`journaled` storage is the durable production storage mode.

Committed mutations are persisted through IcyDB's commit marker, journal, and
canonical stable-memory stores. Guarded read and write entrypoints must run
recovery before accessing durable row or index state when a marker or recovery
authority may be present.

The durability guarantee is scoped to the IcyDB mutation operation or explicit
atomic batch helper being executed. It does not make the surrounding canister
method transactional.

### `heap`

`heap` storage is volatile live memory.

It has no stable allocation identity, no durable row or index recovery, and no
upgrade-survival guarantee. It is appropriate for tests, caches, and explicitly
volatile state. It must not be described as durable production storage.

## Mutation Atomicity

IcyDB mutation execution is synchronous and must not `await`, yield, spawn
threads, or perform canister calls during the mutation or recovery runtime path.

Atomicity is provided by IcyDB's commit discipline, not by relying on IC trap
rollback as normal control flow:

- fallible validation, planning, decoding, and row-op preparation happen before
  durable mutation;
- after the commit marker is durable, recovery authority belongs to the marker
  and journal protocol;
- guarded reentry must converge to the marker-authorized final state;
- reads and writes must not observe partial durable state through guarded
  entrypoints.

Returning `Err` from application code after a successful IcyDB write does not
roll back that write. The `Err` is an application response value, not a database
transaction abort.

Batch helpers have explicit lanes:

- `*_many_atomic` is all-or-nothing for one entity type in one call;
- `*_many_non_atomic` is fail-fast and may leave an already committed prefix.

## Guarded Recovery

Recovery is a required system step before guarded reads or writes proceed when
durable recovery authority is present.

Recovery may use:

- commit markers;
- journal tails;
- fold watermarks;
- canonical row stores;
- derived index rebuild/fold authority.

The recovery proof established for the current line covers internally produced
interrupted states. It does not claim to repair arbitrary hostile stable-memory
images.

Direct raw-store or index access that bypasses guarded recovery is outside this
contract and may observe transient or stale state during startup or interrupted
recovery windows.

## Backup, Restore, And Import Scope

IcyDB does not currently provide a supported backup/restore/import product
surface for raw stable-memory images.

The supported durability boundary is:

- the same canister's stable memory under normal IC execution and upgrade
  preservation;
- internally produced interrupted commit or recovery states;
- guarded recovery before IcyDB read/write entrypoints.

The current line does not guarantee that IcyDB can safely accept a raw stable
memory image copied from another canister, another version, another runtime, or
an untrusted external source. If such an image is supplied by custom tooling,
it is treated as operator-owned risk unless and until IcyDB ships an explicit
import contract.

An import/restore feature must define:

- trusted versus hostile input assumptions;
- persisted-format compatibility rules;
- corruption-detection requirements;
- version-gap behavior;
- recovery-size and resource limits.

## Checksum Decision

The current line does not add persisted checksums.

This is an explicit no-checksum-now decision, not a claim that checksums are
unnecessary forever. The current supported boundary is same-canister stable
memory plus fail-closed decoding and guarded recovery of internally produced
states. The 0.189/0.190 evidence strengthened marker, journal, row, schema,
index, and structural decode failure behavior without changing persisted bytes.

Adding checksums later is a persisted-format change and must be classified
under `docs/contracts/PERSISTED_FORMAT_POLICY.md` before implementation.

Future checksum design must state whether checksums cover marker/journal only
or also row, schema, index, and structural value envelopes, and whether the
goal is accidental-corruption detection, hostile-import rejection, or both.

## Recovery Size And Scale Limits

0.190 provided a checked host characterization for secondary-index rebuild
recovery over a 256-row dataset. 0.191 raises that simple secondary-index host
regression floor to 1,024 rows, adds a 128-row-per-shape mixed ordinary,
conditional, and expression index rebuild floor, and adds a PocketIC same-WASM
upgrade/reentry instruction probe over a 32-row journaled `sql_perf` fixture.
These are proof shapes and regression budgets, not production IC
instruction-budget guarantees.

Until production recovery-size measurements or streaming rebuild/fold designs
land, operators should treat large recovery work as bounded by canister
instruction and memory budgets. If recovery cannot complete, guarded reads and
writes must fail rather than proceed on partially recovered state.

## Stable Memory Partitioning

Stable-memory memory IDs are part of the durable store allocation contract.

Operators and generated canister code must keep IcyDB-owned memory IDs
partitioned from other stable-memory users. Reusing or remapping IcyDB memory
IDs outside the generated/store-registry contract can corrupt durable state and
is outside the recovery guarantee.

## Host And Non-IC Environments

Host tests and non-IC environments are supported for deterministic validation
of IcyDB's single-threaded runtime model. They are not POSIX database servers.

The durability contract remains IC-native:

- one logical actor;
- serialized update execution;
- no production threads in write/recovery paths;
- stable memory as the durable substrate.

Multi-process access to the same backing bytes, file-lock behavior, and
filesystem crash-order guarantees are not part of this contract.

## Format Policy

Persisted-format compatibility rules are defined in
`docs/contracts/PERSISTED_FORMAT_POLICY.md`. The active persisted-surface
checklist is maintained in `docs/contracts/PERSISTED_FORMAT_INVENTORY.md`.

No persisted-format change is introduced by this document.

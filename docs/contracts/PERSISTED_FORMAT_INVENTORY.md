# IcyDB Persisted Format Inventory

This inventory names the durable byte surfaces that must be reviewed when a
change touches IcyDB stable-memory compatibility.

It complements `docs/contracts/PERSISTED_FORMAT_POLICY.md`. The policy defines
how changes are classified; this inventory defines which surfaces are in scope
for that classification.

## Scope

This document is a review checklist, not a byte-level layout specification.
The source modules listed below remain the authority for exact encodings,
bounds, and validation behavior.

Every row in this inventory currently follows the default pre-`1.0.0` posture:
one active internal format, unknown future versions fail closed, and no legacy
fallback is added unless a design explicitly classifies the change otherwise.

## Active Durable Surfaces

| Surface | Owner Modules | Durable Role | Current Posture |
| --- | --- | --- | --- |
| Stable-memory allocation roles | `crates/icydb-core/src/traits/mod.rs`, `crates/icydb-schema/src/node/canister.rs`, `crates/icydb-schema-derive/src/node/canister.rs` | Assigns the commit memory domain and each journaled store's data, index, schema, and journal memories. | Stable identity contract; remapping is outside the durability guarantee. |
| Commit control slot | `crates/icydb-core/src/db/commit/store/control_slot.rs` | Durable presence envelope for pending commit-marker bytes. | Versioned canonical envelope; empty bytes mean no marker; malformed bytes fail closed. |
| Commit marker payload | `crates/icydb-core/src/db/commit/marker.rs`, `crates/icydb-core/src/db/commit/store/marker_envelope.rs` | Marker-bound journal publication and recovery authority. | Versioned marker envelope with bounded current journal batches; unsupported versions fail closed. |
| Journal tail batches and sequences | `crates/icydb-core/src/db/journal/codec.rs`, `crates/icydb-core/src/db/journal/store.rs` | Marker-bound row/schema replay records and durable fold order. | Versioned, bounded batch payloads stored under ordered sequence/chunk keys. |
| Fold watermark | `crates/icydb-core/src/db/journal/store.rs`, `crates/icydb-core/src/db/commit/recovery.rs` | Records journal-tail fold progress for recovery/reentry. | Durable recovery authority; guarded recovery must reconcile it with marker, journal, rows, and indexes. |
| Raw data-store keys | `crates/icydb-core/src/db/data/store.rs`, `crates/icydb-core/src/db/data/structural_field/primary_key_component/` | Primary-key identity for row storage and commit/journal records. | Structural key encoding is persisted identity and must remain accepted-schema backed. |
| Raw row envelopes | `crates/icydb-core/src/db/codec/mod.rs`, `crates/icydb-core/src/db/data/row.rs`, `crates/icydb-core/src/db/data/structural_row.rs` | Canonical row payload bytes in stable data stores. | The sole current row envelope carries a non-zero entity-local row-layout stamp. Accepted decode requires an admitted stamp and its exact physical slot count before field traversal; unstamped, old-current-form, short-current, long, zero, future, and below-floor rows fail closed. |
| Structural field payloads | `crates/icydb-core/src/db/data/structural_field/` | Per-field persisted payloads inside canonical rows and keys. | Structural Binary v1 plus accepted-field decode authority; malformed payloads fail closed. |
| Value-storage payloads | `crates/icydb-core/src/db/data/structural_field/value_storage/` | Recursive canonical storage for accepted enum and exact-composite values that cannot use scalar fast paths. | Local extension tags, bounded recursion, bounded allocation, exact accepted-kind validation, and fallible decode. It is not a heterogeneous public field contract. |
| Accepted schema snapshots | `crates/icydb-core/src/db/schema/codec.rs`, `crates/icydb-core/src/db/schema/snapshot.rs`, `crates/icydb-core/src/db/schema/store.rs`, `crates/icydb-core/src/db/schema/composite_catalog.rs` | Runtime schema authority for row layout, exact field contracts, nominal enum/composite catalogs, indexes, and relations. | One current version-1 Candid wire with the required `ICYX` contract profile; current accepted bundle/root and enum catalog envelopes use hard-cut magic identities and version 1. Every entity carries current/history-floor layout versions and every field separately carries introduction layout, future insert-default, and frozen historical-fill facts. Nominal composite and temporal identities are fingerprinted contract facts; old forms do not decode, and generated models are not runtime authority. |
| Secondary-index keys | `crates/icydb-core/src/db/index/key/codec/`, `crates/icydb-core/src/db/index/key/ordered/`, `crates/icydb-core/src/db/index/envelope/` | Ordered materialized index identity and scan boundaries. | Bounded raw-key codec; canonical ordering and continuation envelopes are index-owned. |
| Secondary-index entries | `crates/icydb-core/src/db/index/entry.rs`, `crates/icydb-core/src/db/index/store.rs` | Row-existence witness for each materialized secondary key. | Bounded one-byte witness payload; stale entries are repaired by guarded recovery/rebuild. |
| Cursor tokens | `crates/icydb-core/src/db/cursor/token/codec.rs`, `crates/icydb-core/src/db/cursor/token/value.rs`, `crates/icydb-core/src/db/cursor/validated.rs` | User-visible continuation bytes that may cross call boundaries. | Versioned bounded token wire; query signature, direction, window, and boundary validation fail closed. |

## Review Rule

Any change touching an owner module above must answer these questions before it
lands:

- Does this change alter durable bytes, durable key ordering, durable memory
  identity, or user-visible continuation bytes?
- If yes, which classification from
  `docs/contracts/PERSISTED_FORMAT_POLICY.md` applies?
- Does the change need malformed-input coverage, compatibility tests, migration
  ordering, or explicit pre-`1.0.0` hard-cut release notes?
- Does the change affect backup/import, checksum, recovery-size, or streaming
  recovery claims?

If the answer is "no persisted-format change", the design, changelog, or
evidence note should say so when the surrounding work touches commit, recovery,
row, schema, index, cursor, or structural-value code.

## Non-Surfaces

The following are not persisted-format surfaces by themselves:

- in-heap query caches and compiled command caches;
- runtime execution plans;
- SQL EXPLAIN rendering strings;
- diagnostics-only counters;
- test-only failpoint names and injected failure sites;
- generated model descriptors, except where they allocate stable-memory IDs or
  propose schema that later becomes an accepted schema snapshot.

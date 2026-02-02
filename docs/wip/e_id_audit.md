─ Worked for 7m 08s ────────────────────────────────────────────────────────────────────────────────────────────────────

• Executive Summary

  - Right direction: EntityId/EntityKind::id exist (crates/icydb-core/src/traits/mod.rs), LogicalPlan/ExecutablePlan and
    core Response are already keyed on E::Id (crates/icydb-core/src/db/query/plan/logical.rs, crates/icydb-core/src/db/
    query/plan/executable.rs, crates/icydb-core/src/db/response.rs), and IndexEntry has moved to E::Id internally
    (crates/icydb-core/src/db/index/entry.rs).
  - Still wrong: most public/query/executor surfaces are split between PrimaryKey = Ref<E> and E::Id, and storage types
    leak into planner/executor layers (e.g., Ref<E> wraps StorageKey and exposes raw storage; DataKey returns Ref<E>;
    planner constructs Ref from Value). This breaks the intended identity separation and creates compile-time
    mismatches.
  - Main gap: a single, explicit translation boundary between semantic id (E::Id) and storage key (StorageKey/DataKey)
    is missing; several modules currently assume E::Id == Ref<E>.

  Identity Map

  - Ref<E> — mixed/ambiguous: defined as typed identity but is a thin wrapper over StorageKey with raw accessors
    (crates/icydb-core/src/types/ref.rs); used in planner/executor/index layers as semantic identity and in storage/
    index encoding as raw storage identity.
  - StorageKey — storage identity: fixed-width, ordered on-disk key (crates/icydb-core/src/db/store/storage_key.rs);
    used in indexing, RI checks, and Value conversions (crates/icydb-core/src/db/index/store.rs, crates/icydb-core/src/
    db/store/entity_ref.rs, crates/icydb-core/src/value/mod.rs).
  - DataKey / RawDataKey — storage identity: entity name + StorageKey for stable-memory keys (crates/icydb-core/src/db/
    store/data_key.rs); currently accepts E::Id but uses key.raw() and exposes key::<E>() -> Ref<E>, so it still mixes
    semantic and storage identity.
  - PrimaryKey — semantic identity (legacy): still emitted by derive (crates/icydb-schema-derive/src/imp/entity.rs) and
    used by planner/intent/executors/public APIs (crates/icydb-core/src/db/query/intent/mod.rs, crates/icydb-core/src/
    db/query/plan/planner.rs, crates/icydb-core/src/db/executor/save.rs, crates/icydb/src/db/session/*.rs), but
    currently aliases Ref<E>.
  - key()/primary_key() on entities — semantic identity (legacy): generated in derive (crates/icydb-schema-derive/src/
    imp/entity.rs) and used by index/save/delete logic; should map to id()/set_id() but currently returns Ref<E>.
  - id()/set_id() on entities — semantic identity (new): defined on EntityKind (crates/icydb-core/src/traits/mod.rs),
    used in some executor paths (crates/icydb-core/src/db/executor/context.rs) but not wired consistently.

  Layer-by-Layer Analysis

  - Query planner (AccessPath/AccessPlan/ExecutablePlan): currently builds AccessPlan<E::PrimaryKey> and constructs Ref
    from Value (crates/icydb-core/src/db/query/plan/planner.rs), while ExecutablePlan is E::Id (crates/icydb-core/src/
    db/query/plan/executable.rs); should plan directly in E::Id and convert Value -> E::Id at the planner boundary;
    translation to storage should happen only when building DataKey in execution.
  - Executors (Load/Save/Delete): load/delete execute on LogicalPlan<E::Id> (crates/icydb-core/src/db/executor/load.rs,
    crates/icydb-core/src/db/executor/delete.rs), but save/delete still use entity.key() and PrimaryKey=Ref<E> (crates/
    icydb-core/src/db/executor/save.rs, crates/icydb-core/src/db/executor/delete.rs); should consistently use E::Id and
    only translate to DataKey inside context/storage.
  - Context / row loading / deserialization: context is the right place for E::Id <-> DataKey translation, but
    deserialize_rows currently compares DataKey::key::<E>() (a Ref<E>) to entry.id() (an E::Id) and even references an
    undefined entity_key variable (crates/icydb-core/src/db/executor/context.rs); should decode DataKey to StorageKey,
    then translate to E::Id and compare against entry.id().
  - Index logic (IndexEntry, apply/remove, resolve paths): IndexEntry already stores E::Id and expects
    E::id_to_storage_key / E::id_from_storage_key conversions (crates/icydb-core/src/db/index/entry.rs), while index/
    plan and index/store still use PrimaryKey=Ref<E> and Ref::from_raw (crates/icydb-core/src/db/index/plan.rs, crates/
    icydb-core/src/db/index/store.rs); should make index entry encode/decode the sole storage translation point, and
    have index stores/executors operate on E::Id.
  - Storage (DataKey, RawDataKey, StorageKey): should remain storage-only (crates/icydb-core/src/db/store/data_key.rs,
    crates/icydb-core/src/db/store/storage_key.rs); translation to/from semantic ids should be confined to storage
    boundary helpers (e.g., context).
  - Entity models and views: derive still emits PrimaryKey + key()/primary_key() (crates/icydb-schema-derive/src/imp/
    entity.rs) while the trait expects Id + id()/set_id() (crates/icydb-core/src/traits/mod.rs); should converge on Id
    and keep Ref<T> only as a semantic reference field type, not as a storage key.

  Invariant Checklist

  - Row key consistency: DataKey’s storage key must match the entity payload id on decode (crates/icydb-core/src/db/
    executor/context.rs).
  - Index integrity: index entries must reference valid entity identities; unique entries must contain exactly one id,
    and stored row keys must match indexed keys (crates/icydb-core/src/db/index/entry.rs, crates/icydb-core/src/db/
    index/plan.rs).
  - Ordering stability: AccessPath::KeyRange relies on Ord for the id type; E::Id ordering must match storage key
    ordering used by DataKey scans or range scans will be incorrect (crates/icydb-core/src/db/executor/context.rs,
    crates/icydb-core/src/db/store/storage_key.rs).
  - No executor dependency on storage layout: StorageKey::as_value and Ref::as_value are explicitly diagnostic-only
    (crates/icydb-core/src/db/store/storage_key.rs, crates/icydb-core/src/types/ref.rs); ensure executor logic doesn’t
    rely on storage layout.
  - RI enforcement is pre-commit and based on strong references only (crates/icydb-core/src/db/query/plan/refs/mod.rs,
    crates/icydb-core/src/db/executor/save.rs, docs/REF_INTEGRITY_v2.md).
  - Storage format invariants: StorageKey::STORED_SIZE_BYTES, DataKey::STORED_SIZE_BYTES, and raw encoding sizes are
    protocol-level and must not change (crates/icydb-core/src/db/store/storage_key.rs, crates/icydb-core/src/db/store/
    data_key.rs).

  Blast Radius Assessment

  - Identity-critical modules: crates/icydb-core/src/types/ref.rs, crates/icydb-core/src/traits/mod.rs, crates/icydb-
    core/src/db/store/*.rs, crates/icydb-core/src/db/query/plan/*.rs, crates/icydb-core/src/db/query/intent/mod.rs,
    crates/icydb-core/src/db/executor/*.rs, crates/icydb-core/src/db/index/*.rs, crates/icydb-schema-derive/src/imp/
    entity.rs, crates/icydb/src/db/*.
  - Likely untouched: commit/recovery plumbing (crates/icydb-core/src/db/commit/*.rs), serialization core (crates/icydb-
    core/src/serialize/*), most value types, metrics/obs.
  - Blocking vs incremental: adding translation APIs (E::Id ↔ StorageKey, E::Id ↔ Value) is blocking; after that,
    planner/executor/session/public surfaces can be migrated incrementally.

  Proposed Migration Strategy (phased)

  - Phase 1: introduce explicit translation APIs for E::Id (id ↔ StorageKey, id ↔ Value) at the trait level and wire
    DataKey/IndexEntry to use them; keep storage formats unchanged (crates/icydb-core/src/traits/mod.rs, crates/icydb-
    core/src/db/store/data_key.rs, crates/icydb-core/src/db/index/entry.rs).
  - Phase 2: convert planner/intent/session surfaces to use E::Id end-to-end and eliminate PrimaryKey=Ref<E> in those
    layers; update explain/validate/invariants to rely only on FieldValue for E::Id (crates/icydb-core/src/db/query/*,
    crates/icydb-core/src/db/query/session/*.rs, crates/icydb/src/db/session/*.rs).
  - Phase 3: confine StorageKey/DataKey to storage and indexing; remove Ref::raw usage outside storage boundaries,
    adjust context to be the only id↔storage translator, and ensure index store returns E::Id or DataKey without
    exposing StorageKey (crates/icydb-core/src/db/executor/context.rs, crates/icydb-core/src/db/index/store.rs).
  - Phase 4: cleanup legacy API and derive output (PrimaryKey, key()/primary_key()), update public Response/session APIs
    to surface E::Id, and align docs/tests (crates/icydb-schema-derive/src/imp/entity.rs, crates/icydb/src/db/
    response.rs, docs/REF_INTEGRITY_v2.md).

  High-Risk Areas to Double-Check

  - Planner ↔ executor type mismatch: Query/planner still use PrimaryKey=Ref<E> while session/executable plan use E::Id
    (crates/icydb-core/src/db/query/intent/mod.rs, crates/icydb-core/src/db/query/session/*.rs, crates/icydb-core/src/
    db/query/plan/planner.rs).
  - Index entry encode/decode vs index plan/store API drift: IndexEntry uses E::Id but index plan/store still expects
    Ref/PrimaryKey and raw keys (crates/icydb-core/src/db/index/entry.rs, crates/icydb-core/src/db/index/plan.rs,
    crates/icydb-core/src/db/index/store.rs).
  - Context row decoding: key mismatch check currently references an undefined variable and mixes Ref with E::Id
    (crates/icydb-core/src/db/executor/context.rs); this is the core invariant boundary.
  - Range scans: if E::Id ordering diverges from StorageKey ordering, KeyRange semantics and index scans will break
    (crates/icydb-core/src/db/executor/context.rs, crates/icydb-core/src/db/store/storage_key.rs).
  - Referential integrity checks: EntityReferences yields StorageKey from Value while RI spec frames Ref<T> as the
    identity (crates/icydb-core/src/db/query/plan/refs/mod.rs, crates/icydb-core/src/db/store/entity_ref.rs, docs/
    REF_INTEGRITY_v2.md).

  If you want, I can follow up with a concrete “translation boundary spec” (trait signatures + responsibility map)
  without touching code.

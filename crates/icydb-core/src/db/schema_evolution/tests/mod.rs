//! Module: db::schema_evolution::tests
//! Covers schema-evolution descriptor planning, registry guards, and migration execution handoff.
//! Does not own: lower migration-engine recovery tests.
//! Boundary: schema_evolution -> migration plan -> migration execution.

use crate::{
    db::{
        Db, DbSession, EntityRuntimeHooks,
        commit::{
            clear_commit_marker_for_tests, clear_migration_state_bytes,
            init_commit_store_for_tests, prepare_row_commit_for_entity_with_structural_readers,
        },
        data::{CanonicalRow, DataKey, DataStore, RawDataKey},
        identity::{EntityName, IndexName},
        index::IndexStore,
        registry::{StoreHandle, StoreRegistry},
        relation::validate_delete_strong_relations_for_source,
        schema_evolution::{
            MigrationRegistry, SchemaDataTransformation, SchemaMigrationDescriptor,
            SchemaMigrationEntityTarget, SchemaMigrationExecutionOutcome, SchemaMigrationPlanner,
            SchemaMigrationRowOp, SchemaMigrationStepIntent,
        },
    },
    error::{ErrorClass, ErrorOrigin},
    model::field::FieldKind,
    testing::test_memory,
    traits::{EntityKind, Path},
    types::Ulid,
};
use icydb_derive::{FieldProjection, PersistedRow};
use serde::Deserialize;
use std::cell::RefCell;

//
// SchemaEvolutionTestCanister
//

crate::test_canister! {
    ident = SchemaEvolutionTestCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

//
// SchemaEvolutionTestStore
//

crate::test_store! {
    ident = SchemaEvolutionTestStore,
    canister = SchemaEvolutionTestCanister,
}

///
/// SchemaEvolutionEntity
///
/// Minimal entity used to prove the schema-evolution layer can plan explicit
/// row rewrites and delegate execution to the existing migration engine.
///

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct SchemaEvolutionEntity {
    id: Ulid,
    rank: u32,
}

crate::test_entity_schema! {
    ident = SchemaEvolutionEntity,
    id = Ulid,
    id_field = id,
    entity_name = "SchemaEvolutionEntity",
    entity_tag = crate::testing::MIGRATION_ENTITY_TAG,
    pk_index = 0,
    fields = [("id", FieldKind::Ulid), ("rank", FieldKind::Uint)],
    indexes = [],
    store = SchemaEvolutionTestStore,
    canister = SchemaEvolutionTestCanister,
}

static ENTITY_RUNTIME_HOOKS: &[EntityRuntimeHooks<SchemaEvolutionTestCanister>] =
    &[EntityRuntimeHooks::new(
        SchemaEvolutionEntity::ENTITY_TAG,
        <SchemaEvolutionEntity as crate::traits::EntitySchema>::MODEL,
        SchemaEvolutionEntity::PATH,
        SchemaEvolutionTestStore::PATH,
        prepare_row_commit_for_entity_with_structural_readers::<SchemaEvolutionEntity>,
        validate_delete_strong_relations_for_source::<SchemaEvolutionEntity>,
    )];

thread_local! {
    static DATA_STORE: RefCell<DataStore> = RefCell::new(DataStore::init(test_memory(51)));
    static INDEX_STORE: RefCell<IndexStore> = RefCell::new(IndexStore::init(test_memory(52)));
    static STORE_REGISTRY: StoreRegistry = {
        let mut reg = StoreRegistry::new();
        reg.register_store(SchemaEvolutionTestStore::PATH, &DATA_STORE, &INDEX_STORE)
            .expect("schema-evolution test store registration should succeed");
        reg
    };
}

static DB: Db<SchemaEvolutionTestCanister> =
    Db::new_with_hooks(&STORE_REGISTRY, ENTITY_RUNTIME_HOOKS);

fn session() -> DbSession<SchemaEvolutionTestCanister> {
    DbSession::new_with_hooks(&STORE_REGISTRY, ENTITY_RUNTIME_HOOKS)
}

fn with_schema_evolution_store<R>(f: impl FnOnce(StoreHandle) -> R) -> R {
    DB.with_store_registry(|reg| reg.try_get_store(SchemaEvolutionTestStore::PATH).map(f))
        .expect("schema-evolution test store access should succeed")
}

fn reset_schema_evolution_state() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    clear_commit_marker_for_tests().expect("commit marker reset should succeed");
    clear_migration_state_bytes().expect("migration state reset should succeed");

    with_schema_evolution_store(|store| {
        store.with_data_mut(DataStore::clear);
        store.with_index_mut(IndexStore::clear);
    });
}

fn schema_evolution_data_key(id: Ulid) -> RawDataKey {
    DataKey::try_new::<SchemaEvolutionEntity>(id)
        .expect("schema-evolution test data key should build")
        .to_raw()
        .expect("schema-evolution test data key should encode")
}

fn schema_evolution_row_bytes(entity: &SchemaEvolutionEntity) -> Vec<u8> {
    CanonicalRow::from_entity(entity)
        .expect("schema-evolution test row should encode")
        .into_raw_row()
        .as_bytes()
        .to_vec()
}

fn schema_evolution_target() -> SchemaMigrationEntityTarget {
    SchemaMigrationEntityTarget::for_entity::<SchemaEvolutionEntity>()
        .expect("schema-evolution entity target should build")
}

fn add_rank_index_descriptor(id: Ulid) -> SchemaMigrationDescriptor {
    let target = schema_evolution_target();
    let entity_name = target.name();
    let index = IndexName::try_from_parts(&entity_name, &["rank"])
        .expect("schema-evolution index name should build");
    let row = SchemaEvolutionEntity { id, rank: 17 };
    let row_op = SchemaMigrationRowOp::insert(
        target,
        schema_evolution_data_key(row.id).as_bytes().to_vec(),
        schema_evolution_row_bytes(&row),
    );
    let migration_id = EntityName::try_from_str("AddRankIndex")
        .expect("schema-evolution migration id should build");

    SchemaMigrationDescriptor::new(
        migration_id,
        1,
        "add rank index and backfill affected rows",
        SchemaMigrationStepIntent::add_index(index),
        Some(SchemaDataTransformation::explicit_row_ops(vec![row_op])),
    )
    .expect("schema-evolution descriptor should build")
}

#[test]
fn schema_migration_registry_skips_already_applied_descriptor() {
    reset_schema_evolution_state();
    let descriptor = add_rank_index_descriptor(Ulid::from_u128(10_001));
    let mut registry = MigrationRegistry::new();
    registry.record_applied(descriptor.migration_id(), descriptor.version());
    let planner = SchemaMigrationPlanner::new(Vec::new())
        .expect("empty planner should build because registry short-circuits first");

    let outcome = session()
        .execute_schema_migration_descriptor(&mut registry, &planner, &descriptor, usize::MAX)
        .expect("already-applied schema migration should skip before planning");

    assert_eq!(outcome, SchemaMigrationExecutionOutcome::AlreadyApplied);
    assert!(
        registry.is_applied(descriptor.migration_id(), descriptor.version()),
        "registry must retain completed migration key after skip",
    );
}

#[test]
fn schema_migration_descriptor_produces_deterministic_migration_plan_shape() {
    let descriptor = add_rank_index_descriptor(Ulid::from_u128(10_002));
    let planner = SchemaMigrationPlanner::new(vec![schema_evolution_target()])
        .expect("schema-evolution planner should build");

    let first = planner
        .plan(&descriptor)
        .expect("first schema migration plan should derive");
    let second = planner
        .plan(&descriptor)
        .expect("second schema migration plan should derive");

    assert_eq!(first.id(), second.id());
    assert_eq!(first.version(), second.version());
    assert_eq!(first.len(), second.len());
}

#[test]
fn schema_migration_invalid_schema_change_is_rejected_before_execution() {
    let target = schema_evolution_target();
    let entity_name = target.name();
    let invalid_index = IndexName::try_from_parts(&entity_name, &["missing_field"])
        .expect("structural index identity should build before schema validation");
    let descriptor = SchemaMigrationDescriptor::new(
        EntityName::try_from_str("InvalidIndex")
            .expect("schema-evolution migration id should build"),
        1,
        "invalid index should fail schema compatibility",
        SchemaMigrationStepIntent::add_index(invalid_index),
        Some(SchemaDataTransformation::explicit_row_ops(vec![
            SchemaMigrationRowOp::insert(target, vec![0], vec![1]),
        ])),
    )
    .expect("descriptor shape should build before planner validation");
    let planner =
        SchemaMigrationPlanner::new(vec![target]).expect("schema-evolution planner should build");

    let err = planner
        .plan(&descriptor)
        .expect_err("unknown index field must fail before migration execution");

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Planner);
    assert!(
        err.message.contains("missing_field"),
        "schema compatibility error should identify the missing field: {err:?}",
    );
}

#[test]
fn schema_migration_add_index_pipeline_executes_and_registry_prevents_rerun() {
    reset_schema_evolution_state();
    let inserted_id = Ulid::from_u128(10_003);
    let descriptor = add_rank_index_descriptor(inserted_id);
    let planner = SchemaMigrationPlanner::new(vec![schema_evolution_target()])
        .expect("schema-evolution planner should build");
    let mut registry = MigrationRegistry::new();

    let outcome = session()
        .execute_schema_migration_descriptor(&mut registry, &planner, &descriptor, usize::MAX)
        .expect("schema migration should execute through lower migration engine");
    let Some(migration_outcome) = outcome.migration_outcome() else {
        panic!("first schema migration execution must delegate to db::migration");
    };

    assert_eq!(
        migration_outcome.state(),
        crate::db::MigrationRunState::Complete,
    );
    assert!(
        registry.is_applied(descriptor.migration_id(), descriptor.version()),
        "registry must record completed schema migration after lower migration completion",
    );

    let stored_row = with_schema_evolution_store(|store| {
        store.with_data(|data_store| {
            data_store
                .get(&schema_evolution_data_key(inserted_id))
                .map(|row| row.as_bytes().to_vec())
        })
    });
    assert!(
        stored_row.is_some(),
        "lower migration execution should apply the descriptor-derived row op",
    );

    let second = session()
        .execute_schema_migration_descriptor(&mut registry, &planner, &descriptor, usize::MAX)
        .expect("completed schema migration should skip on repeat");

    assert_eq!(second, SchemaMigrationExecutionOutcome::AlreadyApplied);
}

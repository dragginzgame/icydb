//! Module: db::executor::tests::support
//! Owns shared executor test fixtures, stores, entity schemas, and reset
//! helpers reused across the topical executor owner suites.
//! Does not own: the topical assertions themselves.
//! Boundary: keeps reusable executor test support out of the owner `mod.rs`
//! wiring file.

pub(in crate::db::executor::tests) use crate::{
    db::{
        Db, DbSession, EntityRuntimeHooks,
        commit::{
            CommitMarker, begin_commit, commit_marker_present, ensure_recovered,
            init_commit_store_for_tests, reset_commit_marker_test_journal_sequence,
        },
        data::DataStore,
        executor::{
            DeleteExecutor, LoadExecutor, PreparedExecutionPlan, SaveExecutor,
            ScalarTerminalBoundaryRequest,
        },
        index::IndexStore,
        journal::JournalTailStore,
        predicate::MissingRowPolicy,
        query::intent::Query,
        registry::StoreRegistry,
        relation::validate_delete_strong_relations_for_source,
        schema::SchemaStore,
    },
    entity::{EntityKind, EntityValue},
    error::InternalError,
    metrics::sink::{MetricsEvent, MetricsSink, with_shared_metrics_sink},
    model::{
        field::{FieldKind, RelationStrength},
        index::IndexModel,
    },
    testing::test_memory,
    traits::Path,
    types::{Ulid, Unit},
};
use icydb_derive::{FieldProjection, PersistedRow};
use serde::Deserialize;
use std::{cell::RefCell, rc::Rc};

///
/// ScanBudgetCaptureSink
///
/// Small metrics sink shared by executor owner tests that assert row-scan
/// budgets.
///

#[derive(Default)]
pub(in crate::db::executor::tests) struct ScanBudgetCaptureSink {
    events: RefCell<Vec<MetricsEvent>>,
}

impl ScanBudgetCaptureSink {
    fn into_events(self) -> Vec<MetricsEvent> {
        self.events.into_inner()
    }
}

impl MetricsSink for ScanBudgetCaptureSink {
    fn record(&self, event: MetricsEvent) {
        self.events.borrow_mut().push(event);
    }
}

// Sum `RowsScanned` metrics for one entity path under the shared executor test sink.
pub(in crate::db::executor::tests) fn rows_scanned_for_entity(
    events: &[MetricsEvent],
    entity_path: &'static str,
) -> usize {
    events.iter().fold(0usize, |acc, event| {
        let scanned = match event {
            MetricsEvent::RowsScanned {
                entity_path: path,
                rows_scanned,
            } if *path == entity_path => usize::try_from(*rows_scanned).unwrap_or(usize::MAX),
            _ => 0,
        };

        acc.saturating_add(scanned)
    })
}

// Run one closure under the shared executor test sink and return both output and
// scanned-row total for the requested entity path.
pub(in crate::db::executor::tests) fn capture_rows_scanned_for_entity<R>(
    entity_path: &'static str,
    run: impl FnOnce() -> R,
) -> (R, usize) {
    let sink = Rc::new(ScanBudgetCaptureSink::default());
    let output = with_shared_metrics_sink(sink.clone(), run);
    let sink = Rc::try_unwrap(sink)
        .unwrap_or_else(|_| panic!("scan budget metrics sink should have one owner after capture"));
    let rows_scanned = rows_scanned_for_entity(&sink.into_events(), entity_path);

    (output, rows_scanned)
}

// Execute one shared COUNT scalar terminal from executor owner tests.
pub(in crate::db::executor::tests) fn execute_count_terminal<E>(
    load: &LoadExecutor<E>,
    plan: PreparedExecutionPlan<E>,
) -> Result<u32, InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_scalar_terminal_request(plan, ScalarTerminalBoundaryRequest::Count)?
        .into_count()
}

// Execute one shared EXISTS scalar terminal from executor owner tests.
pub(in crate::db::executor::tests) fn execute_exists_terminal<E>(
    load: &LoadExecutor<E>,
    plan: PreparedExecutionPlan<E>,
) -> Result<bool, InternalError>
where
    E: EntityKind + EntityValue,
{
    load.execute_scalar_terminal_request(plan, ScalarTerminalBoundaryRequest::Exists)?
        .into_exists()
}

crate::test_canister! {
    vis = pub(in crate::db::executor::tests),
    ident = TestCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

crate::test_store! {
    vis = pub(in crate::db::executor::tests),
    ident = TestDataStore,
    canister = TestCanister,
}

thread_local! {
    pub(in crate::db::executor::tests) static TEST_DATA_STORE: RefCell<DataStore> =
        RefCell::new(DataStore::init_journaled(test_memory(0)));
    pub(in crate::db::executor::tests) static TEST_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init_journaled(test_memory(1)));
    pub(in crate::db::executor::tests) static TEST_SCHEMA_STORE: RefCell<SchemaStore> =
        RefCell::new(SchemaStore::init_journaled(test_memory(2)));
    pub(in crate::db::executor::tests) static TEST_JOURNAL_STORE: RefCell<JournalTailStore> =
        RefCell::new(JournalTailStore::init(test_memory(3)));
    pub(in crate::db::executor::tests) static STORE_REGISTRY: StoreRegistry = {
        let mut reg = StoreRegistry::new();
        reg.register_journaled_store(
            TestDataStore::PATH,
            &TEST_DATA_STORE,
            &TEST_INDEX_STORE,
            &TEST_SCHEMA_STORE,
            &TEST_JOURNAL_STORE,
            crate::db::StoreAllocationIdentities::new_journaled(
                crate::db::StoreAllocationIdentity::new(0, "icydb.test.executor.data.v1"),
                crate::db::StoreAllocationIdentity::new(1, "icydb.test.executor.index.v1"),
                crate::db::StoreAllocationIdentity::new(2, "icydb.test.executor.schema.v1"),
                crate::db::StoreAllocationIdentity::new(3, "icydb.test.executor.journal.v1"),
            ),
            crate::db::StoreRuntimeStorageCapabilities::journaled(),
        )
            .expect("test store registration should succeed");
        reg
    };
}

pub(in crate::db::executor::tests) static DB: Db<TestCanister> = Db::new(&STORE_REGISTRY);

///
/// SimpleEntity
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
pub(in crate::db::executor::tests) struct SimpleEntity {
    pub(in crate::db::executor::tests) id: Ulid,
}

crate::test_entity! {
    ident = SimpleEntity,
    entity_name = "SimpleEntity",
    tag = crate::testing::SIMPLE_ENTITY_TAG,
    store = TestDataStore,
    canister = TestCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

///
/// SingletonUnitEntity
///
/// Executor-lifecycle singleton fixture used to keep runtime `singleton()` load
/// behavior covered on the current executor harness.
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
pub(in crate::db::executor::tests) struct SingletonUnitEntity {
    pub(in crate::db::executor::tests) id: Unit,
    pub(in crate::db::executor::tests) label: String,
}

crate::test_singleton_entity! {
    ident = SingletonUnitEntity,
    entity_name = "SingletonUnitEntity",
    tag = crate::testing::SINGLETON_UNIT_ENTITY_TAG,
    store = TestDataStore,
    canister = TestCanister,
    key_type = Unit,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Unit => FieldKind::Unit },
        crate::test_field! { label: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [],
}

///
/// IndexedMetricsEntity
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
pub(in crate::db::executor::tests) struct IndexedMetricsEntity {
    pub(in crate::db::executor::tests) id: Ulid,
    pub(in crate::db::executor::tests) tag: u32,
    pub(in crate::db::executor::tests) label: String,
}

pub(in crate::db::executor::tests) static INDEXED_METRICS_INDEX_FIELDS: [&str; 1] = ["tag"];
pub(in crate::db::executor::tests) static INDEXED_METRICS_INDEX_MODELS: [IndexModel; 1] =
    [IndexModel::generated(
        "tag",
        TestDataStore::PATH,
        &INDEXED_METRICS_INDEX_FIELDS,
        false,
    )];

crate::test_entity! {
    ident = IndexedMetricsEntity,
    entity_name = "IndexedMetricsEntity",
    tag = crate::testing::INDEXED_METRICS_ENTITY_TAG,
    store = TestDataStore,
    canister = TestCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { tag: u32 => FieldKind::Nat64 },
        crate::test_field! { label: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [&INDEXED_METRICS_INDEX_MODELS[0]],
}

///
/// PushdownParityEntity
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
pub(in crate::db::executor::tests) struct PushdownParityEntity {
    pub(in crate::db::executor::tests) id: Ulid,
    pub(in crate::db::executor::tests) group: u32,
    pub(in crate::db::executor::tests) rank: u32,
    pub(in crate::db::executor::tests) label: String,
}

pub(in crate::db::executor::tests) static PUSHDOWN_PARITY_INDEX_FIELDS: [&str; 2] =
    ["group", "rank"];
pub(in crate::db::executor::tests) static PUSHDOWN_PARITY_INDEX_MODELS: [IndexModel; 1] =
    [IndexModel::generated(
        "group_rank",
        TestDataStore::PATH,
        &PUSHDOWN_PARITY_INDEX_FIELDS,
        false,
    )];

crate::test_entity! {
    ident = PushdownParityEntity,
    entity_name = "PushdownParityEntity",
    tag = crate::testing::PUSHDOWN_PARITY_ENTITY_TAG,
    store = TestDataStore,
    canister = TestCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { group: u32 => FieldKind::Nat64 },
        crate::test_field! { rank: u32 => FieldKind::Nat64 },
        crate::test_field! { label: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [&PUSHDOWN_PARITY_INDEX_MODELS[0]],
}

///
/// UniqueIndexRangeEntity
///
/// Executor snapshot fixture for unique secondary range access. This keeps the
/// index-range execution snapshot coverage local to the executor test harness.
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
pub(in crate::db::executor::tests) struct UniqueIndexRangeEntity {
    pub(in crate::db::executor::tests) id: Ulid,
    pub(in crate::db::executor::tests) code: u32,
    pub(in crate::db::executor::tests) label: String,
}

pub(in crate::db::executor::tests) static UNIQUE_INDEX_RANGE_INDEX_FIELDS: [&str; 1] = ["code"];
pub(in crate::db::executor::tests) static UNIQUE_INDEX_RANGE_INDEX_MODELS: [IndexModel; 1] =
    [IndexModel::generated(
        "code_unique",
        TestDataStore::PATH,
        &UNIQUE_INDEX_RANGE_INDEX_FIELDS,
        true,
    )];

crate::test_entity! {
    ident = UniqueIndexRangeEntity,
    entity_name = "UniqueIndexRangeEntity",
    tag = crate::testing::UNIQUE_INDEX_RANGE_ENTITY_TAG,
    store = TestDataStore,
    canister = TestCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { code: u32 => FieldKind::Nat64 },
        crate::test_field! { label: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [&UNIQUE_INDEX_RANGE_INDEX_MODELS[0]],
}

///
/// PhaseEntity
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
pub(in crate::db::executor::tests) struct PhaseEntity {
    pub(in crate::db::executor::tests) id: Ulid,
    pub(in crate::db::executor::tests) opt_rank: Option<u32>,
    pub(in crate::db::executor::tests) rank: u32,
    pub(in crate::db::executor::tests) tags: Vec<u32>,
    pub(in crate::db::executor::tests) label: String,
}

pub(in crate::db::executor::tests) static PHASE_TAG_KIND: FieldKind = FieldKind::Nat64;

crate::test_entity! {
    ident = PhaseEntity,
    entity_name = "PhaseEntity",
    tag = crate::testing::PHASE_ENTITY_TAG,
    store = TestDataStore,
    canister = TestCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! {
            opt_rank: Option<u32> => FieldKind::Nat64,
            options = crate::testing::TestFieldModelOptions::DEFAULT.with_nullable(true),
        },
        crate::test_field! { rank: u32 => FieldKind::Nat64 },
        crate::test_field! { tags: Vec<u32> => FieldKind::List(&PHASE_TAG_KIND) },
        crate::test_field! { label: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [],
}

// Clear the test data store and any pending commit marker between runs.
pub(in crate::db::executor::tests) fn reset_store() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    ensure_recovered(&DB).expect("write-side recovery should succeed");
    TEST_DATA_STORE.with(|store| store.borrow_mut().clear());
    TEST_INDEX_STORE.with(|store| store.borrow_mut().clear());
    TEST_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
    reset_commit_marker_test_journal_sequence();
}

crate::test_canister! {
    vis = pub(in crate::db::executor::tests),
    ident = RelationTestCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

crate::test_store! {
    vis = pub(in crate::db::executor::tests),
    ident = RelationSourceStore,
    canister = RelationTestCanister,
}

crate::test_store! {
    vis = pub(in crate::db::executor::tests),
    ident = RelationTargetStore,
    canister = RelationTestCanister,
}

thread_local! {
    pub(in crate::db::executor::tests) static REL_SOURCE_DATA_STORE: RefCell<DataStore> =
        RefCell::new(DataStore::init_journaled(test_memory(40)));
    pub(in crate::db::executor::tests) static REL_TARGET_DATA_STORE: RefCell<DataStore> =
        RefCell::new(DataStore::init_journaled(test_memory(41)));
    pub(in crate::db::executor::tests) static REL_SOURCE_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init_journaled(test_memory(42)));
    pub(in crate::db::executor::tests) static REL_TARGET_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init_journaled(test_memory(43)));
    pub(in crate::db::executor::tests) static REL_SOURCE_SCHEMA_STORE: RefCell<SchemaStore> =
        RefCell::new(SchemaStore::init_journaled(test_memory(44)));
    pub(in crate::db::executor::tests) static REL_TARGET_SCHEMA_STORE: RefCell<SchemaStore> =
        RefCell::new(SchemaStore::init_journaled(test_memory(45)));
    pub(in crate::db::executor::tests) static REL_SOURCE_JOURNAL_STORE: RefCell<JournalTailStore> =
        RefCell::new(JournalTailStore::init(test_memory(46)));
    pub(in crate::db::executor::tests) static REL_TARGET_JOURNAL_STORE: RefCell<JournalTailStore> =
        RefCell::new(JournalTailStore::init(test_memory(47)));
    pub(in crate::db::executor::tests) static REL_STORE_REGISTRY: StoreRegistry = {
        let mut reg = StoreRegistry::new();
        reg.register_journaled_store(
            RelationSourceStore::PATH,
            &REL_SOURCE_DATA_STORE,
            &REL_SOURCE_INDEX_STORE,
            &REL_SOURCE_SCHEMA_STORE,
            &REL_SOURCE_JOURNAL_STORE,
            crate::db::StoreAllocationIdentities::new_journaled(
                crate::db::StoreAllocationIdentity::new(
                    40,
                    "icydb.test.relation_source.data.v1",
                ),
                crate::db::StoreAllocationIdentity::new(
                    42,
                    "icydb.test.relation_source.index.v1",
                ),
                crate::db::StoreAllocationIdentity::new(
                    44,
                    "icydb.test.relation_source.schema.v1",
                ),
                crate::db::StoreAllocationIdentity::new(
                    46,
                    "icydb.test.relation_source.journal.v1",
                ),
            ),
            crate::db::StoreRuntimeStorageCapabilities::journaled(),
        )
        .expect("relation source store registration should succeed");
        reg.register_journaled_store(
            RelationTargetStore::PATH,
            &REL_TARGET_DATA_STORE,
            &REL_TARGET_INDEX_STORE,
            &REL_TARGET_SCHEMA_STORE,
            &REL_TARGET_JOURNAL_STORE,
            crate::db::StoreAllocationIdentities::new_journaled(
                crate::db::StoreAllocationIdentity::new(
                    41,
                    "icydb.test.relation_target.data.v1",
                ),
                crate::db::StoreAllocationIdentity::new(
                    43,
                    "icydb.test.relation_target.index.v1",
                ),
                crate::db::StoreAllocationIdentity::new(
                    45,
                    "icydb.test.relation_target.schema.v1",
                ),
                crate::db::StoreAllocationIdentity::new(
                    47,
                    "icydb.test.relation_target.journal.v1",
                ),
            ),
            crate::db::StoreRuntimeStorageCapabilities::journaled(),
        )
        .expect("relation target store registration should succeed");
        reg
    };
}

pub(in crate::db::executor::tests) static REL_ENTITY_RUNTIME_HOOKS: &[EntityRuntimeHooks<
    RelationTestCanister,
>] = &[
    EntityRuntimeHooks::new(
        RelationTargetEntity::ENTITY_TAG,
        <RelationTargetEntity as crate::entity::EntityDeclaration>::MODEL,
        RelationTargetEntity::PATH,
        RelationTargetStore::PATH,
        validate_delete_strong_relations_for_source::<RelationTargetEntity>,
    ),
    EntityRuntimeHooks::new(
        RelationSourceEntity::ENTITY_TAG,
        <RelationSourceEntity as crate::entity::EntityDeclaration>::MODEL,
        RelationSourceEntity::PATH,
        RelationSourceStore::PATH,
        validate_delete_strong_relations_for_source::<RelationSourceEntity>,
    ),
    EntityRuntimeHooks::new(
        CompositeRelationTargetEntity::ENTITY_TAG,
        <CompositeRelationTargetEntity as crate::entity::EntityDeclaration>::MODEL,
        CompositeRelationTargetEntity::PATH,
        RelationTargetStore::PATH,
        validate_delete_strong_relations_for_source::<CompositeRelationTargetEntity>,
    ),
    EntityRuntimeHooks::new(
        CompositeRelationSourceEntity::ENTITY_TAG,
        <CompositeRelationSourceEntity as crate::entity::EntityDeclaration>::MODEL,
        CompositeRelationSourceEntity::PATH,
        RelationSourceStore::PATH,
        validate_delete_strong_relations_for_source::<CompositeRelationSourceEntity>,
    ),
    EntityRuntimeHooks::new(
        OptionalCompositeRelationSourceEntity::ENTITY_TAG,
        <OptionalCompositeRelationSourceEntity as crate::entity::EntityDeclaration>::MODEL,
        OptionalCompositeRelationSourceEntity::PATH,
        RelationSourceStore::PATH,
        validate_delete_strong_relations_for_source::<OptionalCompositeRelationSourceEntity>,
    ),
    EntityRuntimeHooks::new(
        CompositePkRelationSourceEntity::ENTITY_TAG,
        <CompositePkRelationSourceEntity as crate::entity::EntityDeclaration>::MODEL,
        CompositePkRelationSourceEntity::PATH,
        RelationSourceStore::PATH,
        validate_delete_strong_relations_for_source::<CompositePkRelationSourceEntity>,
    ),
    EntityRuntimeHooks::new(
        WeakSingleRelationSourceEntity::ENTITY_TAG,
        <WeakSingleRelationSourceEntity as crate::entity::EntityDeclaration>::MODEL,
        WeakSingleRelationSourceEntity::PATH,
        RelationSourceStore::PATH,
        validate_delete_strong_relations_for_source::<WeakSingleRelationSourceEntity>,
    ),
    EntityRuntimeHooks::new(
        WeakOptionalRelationSourceEntity::ENTITY_TAG,
        <WeakOptionalRelationSourceEntity as crate::entity::EntityDeclaration>::MODEL,
        WeakOptionalRelationSourceEntity::PATH,
        RelationSourceStore::PATH,
        validate_delete_strong_relations_for_source::<WeakOptionalRelationSourceEntity>,
    ),
    EntityRuntimeHooks::new(
        WeakListRelationSourceEntity::ENTITY_TAG,
        <WeakListRelationSourceEntity as crate::entity::EntityDeclaration>::MODEL,
        WeakListRelationSourceEntity::PATH,
        RelationSourceStore::PATH,
        validate_delete_strong_relations_for_source::<WeakListRelationSourceEntity>,
    ),
];

pub(in crate::db::executor::tests) static REL_DB: Db<RelationTestCanister> =
    Db::new_with_hooks(&REL_STORE_REGISTRY, REL_ENTITY_RUNTIME_HOOKS);

///
/// RelationTargetEntity
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
pub(in crate::db::executor::tests) struct RelationTargetEntity {
    pub(in crate::db::executor::tests) id: Ulid,
}

crate::test_entity! {
    ident = RelationTargetEntity,
    entity_name = "RelationTargetEntity",
    tag = crate::testing::RELATION_TARGET_ENTITY_TAG,
    store = RelationTargetStore,
    canister = RelationTestCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
    ],
    indexes = [],
}

///
/// RelationSourceEntity
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
pub(in crate::db::executor::tests) struct RelationSourceEntity {
    pub(in crate::db::executor::tests) id: Ulid,
    pub(in crate::db::executor::tests) target: Ulid,
}

crate::test_entity! {
    ident = RelationSourceEntity,
    entity_name = "RelationSourceEntity",
    tag = crate::testing::RELATION_SOURCE_ENTITY_TAG,
    store = RelationSourceStore,
    canister = RelationTestCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { target: Ulid => FieldKind::Relation {
            target_path: RelationTargetEntity::PATH,
            target_entity_name: <RelationTargetEntity as crate::entity::EntityDeclaration>::MODEL.name(),
            target_entity_tag: RelationTargetEntity::ENTITY_TAG,
            target_store_path: RelationTargetStore::PATH,
            key_kind: &FieldKind::Ulid,
            strength: RelationStrength::Strong,
        } },
    ],
    indexes = [],
}

///
/// CompositeRelationTargetKey
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::db::executor::tests) struct CompositeRelationTargetKey {
    pub(in crate::db::executor::tests) tenant_id: u64,
    pub(in crate::db::executor::tests) local_id: u64,
}

impl crate::db::KeyValueCodec for CompositeRelationTargetKey {
    fn to_key_value(&self) -> crate::value::Value {
        crate::value::Value::List(vec![
            crate::value::Value::Nat64(self.tenant_id),
            crate::value::Value::Nat64(self.local_id),
        ])
    }

    fn from_key_value(value: &crate::value::Value) -> Option<Self> {
        let crate::value::Value::List(values) = value else {
            return None;
        };
        let [
            crate::value::Value::Nat64(tenant_id),
            crate::value::Value::Nat64(local_id),
        ] = values.as_slice()
        else {
            return None;
        };

        Some(Self {
            tenant_id: *tenant_id,
            local_id: *local_id,
        })
    }
}

impl crate::db::PrimaryKeyEncode for CompositeRelationTargetKey {
    fn to_primary_key_value(
        &self,
    ) -> Result<crate::db::PrimaryKeyValue, crate::db::PrimaryKeyEncodeError> {
        let composite = crate::db::CompositePrimaryKeyValue::try_from_components(&[
            crate::db::PrimaryKeyComponent::Nat64(self.tenant_id),
            crate::db::PrimaryKeyComponent::Nat64(self.local_id),
        ])?;

        Ok(crate::db::PrimaryKeyValue::Composite(composite))
    }
}

impl crate::db::PrimaryKeyDecode for CompositeRelationTargetKey {
    fn from_primary_key_value(key: &crate::db::PrimaryKeyValue) -> Result<Self, InternalError> {
        let crate::db::PrimaryKeyValue::Composite(composite) = key else {
            return Err(InternalError::store_corruption());
        };
        let [
            crate::db::PrimaryKeyComponent::Nat64(tenant_id),
            crate::db::PrimaryKeyComponent::Nat64(local_id),
        ] = composite.components()
        else {
            return Err(InternalError::store_corruption());
        };

        Ok(Self {
            tenant_id: *tenant_id,
            local_id: *local_id,
        })
    }
}

impl crate::db::EntityKeyBytes for CompositeRelationTargetKey {
    const BYTE_LEN: usize = 16;

    fn write_bytes(&self, out: &mut [u8]) -> Result<(), crate::db::EntityKeyBytesError> {
        crate::db::validate_entity_key_bytes_buffer(out, Self::BYTE_LEN)?;
        out[..8].copy_from_slice(&self.tenant_id.to_be_bytes());
        out[8..16].copy_from_slice(&self.local_id.to_be_bytes());

        Ok(())
    }
}

///
/// CompositeRelationTargetEntity
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
pub(in crate::db::executor::tests) struct CompositeRelationTargetEntity {
    pub(in crate::db::executor::tests) tenant_id: u64,
    pub(in crate::db::executor::tests) local_id: u64,
    pub(in crate::db::executor::tests) label: String,
}

crate::test_entity! {
    ident = CompositeRelationTargetEntity,
    entity_name = "CompositeRelationTargetEntity",
    tag = crate::testing::COMPOSITE_RELATION_TARGET_ENTITY_TAG,
    store = RelationTargetStore,
    canister = RelationTestCanister,
    key_type = CompositeRelationTargetKey,
    primary_key = [tenant_id, local_id],
    fields = [
        crate::test_field! { tenant_id: u64 => FieldKind::Nat64 },
        crate::test_field! { local_id: u64 => FieldKind::Nat64 },
        crate::test_field! { label: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [],
    relations = [],
    entity_value = key(|entity: &CompositeRelationTargetEntity| CompositeRelationTargetKey {
            tenant_id: entity.tenant_id,
            local_id: entity.local_id,
    }),
}

///
/// CompositeRelationSourceEntity
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
pub(in crate::db::executor::tests) struct CompositeRelationSourceEntity {
    pub(in crate::db::executor::tests) id: Ulid,
    pub(in crate::db::executor::tests) target_tenant_id: u64,
    pub(in crate::db::executor::tests) target_local_id: u64,
}

crate::test_entity! {
    ident = CompositeRelationSourceEntity,
    entity_name = "CompositeRelationSourceEntity",
    tag = crate::testing::COMPOSITE_RELATION_SOURCE_ENTITY_TAG,
    store = RelationSourceStore,
    canister = RelationTestCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { target_tenant_id: u64 => FieldKind::Nat64 },
        crate::test_field! { target_local_id: u64 => FieldKind::Nat64 },
    ],
    indexes = [],
    relations = [
        crate::test_relation! {
            name = "target",
            target = CompositeRelationTargetEntity,
            fields = [target_tenant_id, target_local_id],
        },
    ],
}

///
/// OptionalCompositeRelationSourceEntity
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
pub(in crate::db::executor::tests) struct OptionalCompositeRelationSourceEntity {
    pub(in crate::db::executor::tests) id: Ulid,
    pub(in crate::db::executor::tests) target_tenant_id: Option<u64>,
    pub(in crate::db::executor::tests) target_local_id: Option<u64>,
}

crate::test_entity! {
    ident = OptionalCompositeRelationSourceEntity,
    entity_name = "OptionalCompositeRelationSourceEntity",
    tag = crate::testing::OPTIONAL_COMPOSITE_RELATION_SOURCE_ENTITY_TAG,
    store = RelationSourceStore,
    canister = RelationTestCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! {
            target_tenant_id: Option<u64> => FieldKind::Nat64,
            options = crate::testing::TestFieldModelOptions::DEFAULT.with_nullable(true),
        },
        crate::test_field! {
            target_local_id: Option<u64> => FieldKind::Nat64,
            options = crate::testing::TestFieldModelOptions::DEFAULT.with_nullable(true),
        },
    ],
    indexes = [],
    relations = [
        crate::test_relation! {
            name = "target",
            target = CompositeRelationTargetEntity,
            fields = [target_tenant_id, target_local_id],
        },
    ],
}

///
/// CompositePkRelationSourceKey
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::db::executor::tests) struct CompositePkRelationSourceKey {
    pub(in crate::db::executor::tests) tenant_id: u64,
    pub(in crate::db::executor::tests) source_local_id: u64,
}

impl crate::db::KeyValueCodec for CompositePkRelationSourceKey {
    fn to_key_value(&self) -> crate::value::Value {
        crate::value::Value::List(vec![
            crate::value::Value::Nat64(self.tenant_id),
            crate::value::Value::Nat64(self.source_local_id),
        ])
    }

    fn from_key_value(value: &crate::value::Value) -> Option<Self> {
        let crate::value::Value::List(values) = value else {
            return None;
        };
        let [
            crate::value::Value::Nat64(tenant_id),
            crate::value::Value::Nat64(source_local_id),
        ] = values.as_slice()
        else {
            return None;
        };

        Some(Self {
            tenant_id: *tenant_id,
            source_local_id: *source_local_id,
        })
    }
}

impl crate::db::PrimaryKeyEncode for CompositePkRelationSourceKey {
    fn to_primary_key_value(
        &self,
    ) -> Result<crate::db::PrimaryKeyValue, crate::db::PrimaryKeyEncodeError> {
        let composite = crate::db::CompositePrimaryKeyValue::try_from_components(&[
            crate::db::PrimaryKeyComponent::Nat64(self.tenant_id),
            crate::db::PrimaryKeyComponent::Nat64(self.source_local_id),
        ])?;

        Ok(crate::db::PrimaryKeyValue::Composite(composite))
    }
}

impl crate::db::PrimaryKeyDecode for CompositePkRelationSourceKey {
    fn from_primary_key_value(key: &crate::db::PrimaryKeyValue) -> Result<Self, InternalError> {
        let crate::db::PrimaryKeyValue::Composite(composite) = key else {
            return Err(InternalError::store_corruption());
        };
        let [
            crate::db::PrimaryKeyComponent::Nat64(tenant_id),
            crate::db::PrimaryKeyComponent::Nat64(source_local_id),
        ] = composite.components()
        else {
            return Err(InternalError::store_corruption());
        };

        Ok(Self {
            tenant_id: *tenant_id,
            source_local_id: *source_local_id,
        })
    }
}

impl crate::db::EntityKeyBytes for CompositePkRelationSourceKey {
    const BYTE_LEN: usize = 16;

    fn write_bytes(&self, out: &mut [u8]) -> Result<(), crate::db::EntityKeyBytesError> {
        crate::db::validate_entity_key_bytes_buffer(out, Self::BYTE_LEN)?;
        out[..8].copy_from_slice(&self.tenant_id.to_be_bytes());
        out[8..16].copy_from_slice(&self.source_local_id.to_be_bytes());

        Ok(())
    }
}

///
/// CompositePkRelationSourceEntity
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
pub(in crate::db::executor::tests) struct CompositePkRelationSourceEntity {
    pub(in crate::db::executor::tests) tenant: u64,
    pub(in crate::db::executor::tests) source_local: u64,
    pub(in crate::db::executor::tests) target_tenant: u64,
    pub(in crate::db::executor::tests) target_local: u64,
}

crate::test_entity! {
    ident = CompositePkRelationSourceEntity,
    entity_name = "CompositePkRelationSourceEntity",
    tag = crate::testing::COMPOSITE_PK_RELATION_SOURCE_ENTITY_TAG,
    store = RelationSourceStore,
    canister = RelationTestCanister,
    key_type = CompositePkRelationSourceKey,
    primary_key = [tenant, source_local],
    fields = [
        crate::test_field! { tenant: u64 => FieldKind::Nat64 },
        crate::test_field! { source_local: u64 => FieldKind::Nat64 },
        crate::test_field! { target_tenant: u64 => FieldKind::Nat64 },
        crate::test_field! { target_local: u64 => FieldKind::Nat64 },
    ],
    indexes = [],
    relations = [
        crate::test_relation! {
            name = "target",
            target = CompositeRelationTargetEntity,
            fields = [target_tenant, target_local],
        },
    ],
    entity_value = key(|entity: &CompositePkRelationSourceEntity| CompositePkRelationSourceKey {
            tenant_id: entity.tenant,
            source_local_id: entity.source_local,
    }),
}

///
/// WeakSingleRelationSourceEntity
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
pub(in crate::db::executor::tests) struct WeakSingleRelationSourceEntity {
    pub(in crate::db::executor::tests) id: Ulid,
    pub(in crate::db::executor::tests) target: Ulid,
}

crate::test_entity! {
    ident = WeakSingleRelationSourceEntity,
    entity_name = "WeakSingleRelationSourceEntity",
    tag = crate::testing::WEAK_SINGLE_RELATION_SOURCE_ENTITY_TAG,
    store = RelationSourceStore,
    canister = RelationTestCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { target: Ulid => FieldKind::Relation {
            target_path: RelationTargetEntity::PATH,
            target_entity_name: <RelationTargetEntity as crate::entity::EntityDeclaration>::MODEL.name(),
            target_entity_tag: RelationTargetEntity::ENTITY_TAG,
            target_store_path: RelationTargetStore::PATH,
            key_kind: &FieldKind::Ulid,
            strength: RelationStrength::Weak,
        } },
    ],
    indexes = [],
}

///
/// WeakOptionalRelationSourceEntity
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
pub(in crate::db::executor::tests) struct WeakOptionalRelationSourceEntity {
    pub(in crate::db::executor::tests) id: Ulid,
    pub(in crate::db::executor::tests) target: Option<Ulid>,
}

crate::test_entity! {
    ident = WeakOptionalRelationSourceEntity,
    entity_name = "WeakOptionalRelationSourceEntity",
    tag = crate::testing::WEAK_OPTIONAL_RELATION_SOURCE_ENTITY_TAG,
    store = RelationSourceStore,
    canister = RelationTestCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { target: Option<Ulid> => FieldKind::Relation {
            target_path: RelationTargetEntity::PATH,
            target_entity_name: <RelationTargetEntity as crate::entity::EntityDeclaration>::MODEL.name(),
            target_entity_tag: RelationTargetEntity::ENTITY_TAG,
            target_store_path: RelationTargetStore::PATH,
            key_kind: &FieldKind::Ulid,
            strength: RelationStrength::Weak,
        } },
    ],
    indexes = [],
}

///
/// WeakListRelationSourceEntity
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
pub(in crate::db::executor::tests) struct WeakListRelationSourceEntity {
    pub(in crate::db::executor::tests) id: Ulid,
    pub(in crate::db::executor::tests) targets: Vec<Ulid>,
}

pub(in crate::db::executor::tests) static REL_WEAK_LIST_TARGET_KIND: FieldKind =
    FieldKind::Relation {
        target_path: RelationTargetEntity::PATH,
        target_entity_name: <RelationTargetEntity as crate::entity::EntityDeclaration>::MODEL
            .name(),
        target_entity_tag: RelationTargetEntity::ENTITY_TAG,
        target_store_path: RelationTargetStore::PATH,
        key_kind: &FieldKind::Ulid,
        strength: RelationStrength::Weak,
    };

crate::test_entity! {
    ident = WeakListRelationSourceEntity,
    entity_name = "WeakListRelationSourceEntity",
    tag = crate::testing::WEAK_LIST_RELATION_SOURCE_ENTITY_TAG,
    store = RelationSourceStore,
    canister = RelationTestCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { targets: Vec<Ulid> => FieldKind::List(&REL_WEAK_LIST_TARGET_KIND) },
    ],
    indexes = [],
}

///
/// WeakSetRelationSourceEntity
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
pub(in crate::db::executor::tests) struct WeakSetRelationSourceEntity {
    pub(in crate::db::executor::tests) id: Ulid,
    pub(in crate::db::executor::tests) targets: Vec<Ulid>,
}

pub(in crate::db::executor::tests) static REL_WEAK_SET_TARGET_KIND: FieldKind =
    FieldKind::Relation {
        target_path: RelationTargetEntity::PATH,
        target_entity_name: <RelationTargetEntity as crate::entity::EntityDeclaration>::MODEL
            .name(),
        target_entity_tag: RelationTargetEntity::ENTITY_TAG,
        target_store_path: RelationTargetStore::PATH,
        key_kind: &FieldKind::Ulid,
        strength: RelationStrength::Weak,
    };

crate::test_entity! {
    ident = WeakSetRelationSourceEntity,
    entity_name = "WeakSetRelationSourceEntity",
    tag = crate::testing::WEAK_SET_RELATION_SOURCE_ENTITY_TAG,
    store = RelationSourceStore,
    canister = RelationTestCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { targets: Vec<Ulid> => FieldKind::Set(&REL_WEAK_SET_TARGET_KIND) },
    ],
    indexes = [],
}

// Clear relation test stores and any pending commit marker between runs.
pub(in crate::db::executor::tests) fn reset_relation_stores() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    ensure_recovered(&REL_DB).expect("relation write-side recovery should succeed");
    REL_DB.with_store_registry(|reg| {
        reg.try_get_store(RelationSourceStore::PATH)
            .map(|store| {
                store.with_data_mut(DataStore::clear);
                store.with_index_mut(IndexStore::clear);
                if let Some(journal_store) = store.journal_tail_store() {
                    journal_store.with_borrow_mut(JournalTailStore::clear);
                }
            })
            .expect("relation source store access should succeed");
        reg.try_get_store(RelationTargetStore::PATH)
            .map(|store| {
                store.with_data_mut(DataStore::clear);
                store.with_index_mut(IndexStore::clear);
                if let Some(journal_store) = store.journal_tail_store() {
                    journal_store.with_borrow_mut(JournalTailStore::clear);
                }
            })
            .expect("relation target store access should succeed");
    });
    reset_commit_marker_test_journal_sequence();
}

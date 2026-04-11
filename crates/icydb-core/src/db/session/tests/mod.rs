//! Module: db::session::tests
//! Responsibility: integration-style unit coverage for the session query, SQL,
//! explain, cursor, and write boundaries over shared in-memory fixtures.
//! Does not own: production session behavior outside this test module.
//! Boundary: verifies public and crate-visible session contracts while keeping fixture wiring local.

mod sql_aggregate;
mod sql_explain;
mod sql_grouped;
mod sql_surface;

use super::*;
use crate::{
    db::{
        Db, MissingRowPolicy, PlanError,
        access::lower_index_range_specs,
        commit::{ensure_recovered, init_commit_store_for_tests},
        cursor::{CursorPlanError, IndexScanContinuationInput},
        data::{DataKey, DataStore},
        direction::Direction,
        executor::{ExecutorPlanError, assemble_load_execution_node_descriptor},
        index::{IndexKey, IndexStore, key_within_envelope},
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        query::{
            builder::{PreparedFluentNumericFieldStrategy, PreparedFluentProjectionStrategy},
            explain::{
                ExplainAccessPath, ExplainExecutionNodeDescriptor, ExplainExecutionNodeType,
            },
            intent::StructuralQuery,
            plan::{
                AggregateKind, FieldSlot,
                expr::{Expr, ProjectionField},
            },
        },
        registry::{StoreHandle, StoreRegistry},
        response::EntityResponse,
        sql::lowering::{LoweredSqlQuery, apply_lowered_select_shape},
    },
    error::{ErrorClass, ErrorDetail, ErrorOrigin, QueryErrorDetail},
    metrics::sink::{MetricsEvent, MetricsSink, with_metrics_sink},
    model::{
        field::FieldKind,
        index::{IndexExpression, IndexKeyItem, IndexModel, IndexPredicateMetadata},
    },
    serialize::serialized_len,
    testing::test_memory,
    traits::{EntitySchema, Path},
    types::{Date, Duration, EntityTag, Id, Timestamp, Ulid},
    value::{StorageKey, Value},
};
use icydb_derive::{FieldProjection, PersistedRow};
use serde::{Deserialize, Serialize};
use std::{cell::RefCell, collections::BTreeMap, sync::LazyLock};

crate::test_canister! {
    ident = SessionSqlCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

crate::test_store! {
    ident = SessionSqlStore,
    canister = SessionSqlCanister,
}

crate::test_store! {
    ident = IndexedSessionSqlStore,
    canister = SessionSqlCanister,
}

thread_local! {
    static SESSION_SQL_DATA_STORE: RefCell<DataStore> =
        RefCell::new(DataStore::init(test_memory(160)));
    static SESSION_SQL_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init(test_memory(161)));
    static SESSION_SQL_STORE_REGISTRY: StoreRegistry = {
        let mut reg = StoreRegistry::new();
        reg.register_store(
            SessionSqlStore::PATH,
            &SESSION_SQL_DATA_STORE,
            &SESSION_SQL_INDEX_STORE,
        )
        .expect("SQL session test store registration should succeed");
        reg
    };
    static INDEXED_SESSION_SQL_DATA_STORE: RefCell<DataStore> =
        RefCell::new(DataStore::init(test_memory(162)));
    static INDEXED_SESSION_SQL_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init(test_memory(163)));
    static INDEXED_SESSION_SQL_STORE_REGISTRY: StoreRegistry = {
        let mut reg = StoreRegistry::new();
        reg.register_store(
            IndexedSessionSqlStore::PATH,
            &INDEXED_SESSION_SQL_DATA_STORE,
            &INDEXED_SESSION_SQL_INDEX_STORE,
        )
        .expect("indexed SQL session test store registration should succeed");
        reg
    };
}

static SESSION_SQL_DB: Db<SessionSqlCanister> = Db::new(&SESSION_SQL_STORE_REGISTRY);
static INDEXED_SESSION_SQL_DB: Db<SessionSqlCanister> =
    Db::new(&INDEXED_SESSION_SQL_STORE_REGISTRY);
static ACTIVE_TRUE_PREDICATE: LazyLock<Predicate> =
    LazyLock::new(|| Predicate::eq("active".to_string(), true.into()));
static FILTERED_EXPRESSION_SESSION_SQL_ROWS: [(u128, &str, bool, &str, &str, u64); 5] = [
    (9_231, "alpha", false, "gold", "bramble", 10),
    (9_232, "bravo-user", true, "gold", "bravo", 20),
    (9_233, "bristle-user", true, "gold", "bristle", 30),
    (9_234, "brisk-user", true, "silver", "Brisk", 40),
    (9_235, "charlie-user", true, "gold", "charlie", 50),
];

fn active_true_predicate() -> &'static Predicate {
    &ACTIVE_TRUE_PREDICATE
}

const fn active_true_predicate_metadata() -> IndexPredicateMetadata {
    IndexPredicateMetadata::generated("active = true", active_true_predicate)
}

///
/// SessionSqlEntity
///
/// Test entity used to lock end-to-end reduced SQL session behavior.
///

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct SessionSqlEntity {
    id: Ulid,
    name: String,
    age: u64,
}

///
/// IndexedSessionSqlEntity
///
/// Indexed SQL session fixture used to lock strict text-prefix execution over a
/// real secondary `name` index.
///

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct IndexedSessionSqlEntity {
    id: Ulid,
    name: String,
    age: u64,
}

///
/// FilteredIndexedSessionSqlEntity
///
/// Filtered indexed SQL session fixture used to lock guarded order-only
/// fallback against one real `name` index with the `active = true` predicate.
///

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct FilteredIndexedSessionSqlEntity {
    id: Ulid,
    name: String,
    active: bool,
    tier: String,
    handle: String,
    age: u64,
}

///
/// CompositeIndexedSessionSqlEntity
///
/// Composite indexed SQL session fixture used to lock multi-component
/// covering-read execution on a real secondary `(code, serial)` index.
///

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct CompositeIndexedSessionSqlEntity {
    id: Ulid,
    code: String,
    serial: u64,
    note: String,
}

///
/// ExpressionIndexedSessionSqlEntity
///
/// Expression-indexed SQL session fixture used to lock `ORDER BY LOWER(field)`
/// planning and execution against one real expression-key secondary index.
///

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct ExpressionIndexedSessionSqlEntity {
    id: Ulid,
    name: String,
    age: u64,
}

///
/// SessionAggregateEntity
///
/// Session-facing aggregate fixture used to revive the old session projection
/// and ranked terminal contracts under the live `db::session` owner.
///

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct SessionAggregateEntity {
    id: Ulid,
    group: u64,
    rank: u64,
    label: String,
}

///
/// SessionExplainEntity
///
/// Indexed session-local aggregate fixture used to keep seek and execution
/// explain contracts under the `db::session` owner.
///

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct SessionExplainEntity {
    id: Ulid,
    group: u64,
    rank: u64,
    label: String,
}

///
/// SessionDeterministicChoiceEntity
///
/// Session-local deterministic-choice fixture used to lock prefix-family
/// planner ranking through the recovered session-visible index boundary.
///

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct SessionDeterministicChoiceEntity {
    id: Ulid,
    tier: String,
    handle: String,
    label: String,
}

///
/// SessionDeterministicRangeEntity
///
/// Session-local deterministic-choice fixture used to lock range-family
/// planner ranking through the recovered session-visible index boundary.
///

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct SessionDeterministicRangeEntity {
    id: Ulid,
    tier: String,
    score: u64,
    handle: String,
    label: String,
}

///
/// SessionUniquePrefixOffsetEntity
///
/// Session-local unique-prefix fixture used to lock offset-aware ordered load
/// admission on one unique secondary `(tier, handle)` route.
///

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct SessionUniquePrefixOffsetEntity {
    id: Ulid,
    tier: String,
    handle: String,
    note: String,
}

///
/// SessionOrderOnlyChoiceEntity
///
/// Session-local deterministic-choice fixture used to lock order-only
/// fallback ranking through the recovered session-visible index boundary.
///

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct SessionOrderOnlyChoiceEntity {
    id: Ulid,
    alpha: String,
    beta: String,
}

///
/// SessionTemporalEntity
///
/// Session-local temporal fixture used to keep Date/Timestamp/Duration
/// projection and grouped aggregate semantics under the live `db::session`
/// owner instead of the pruned aggregate session matrix.
///

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct SessionTemporalEntity {
    id: Ulid,
    occurred_on: Date,
    occurred_at: Timestamp,
    elapsed: Duration,
}

static INDEXED_SESSION_SQL_INDEX_FIELDS: [&str; 1] = ["name"];
static INDEXED_SESSION_SQL_INDEX_MODELS: [IndexModel; 1] = [IndexModel::generated(
    "name",
    IndexedSessionSqlStore::PATH,
    &INDEXED_SESSION_SQL_INDEX_FIELDS,
    false,
)];
static FILTERED_INDEXED_SESSION_SQL_INDEX_MODELS: [IndexModel; 1] =
    [IndexModel::generated_with_ordinal_and_predicate(
        0,
        "name_active_only",
        IndexedSessionSqlStore::PATH,
        &INDEXED_SESSION_SQL_INDEX_FIELDS,
        false,
        Some(active_true_predicate_metadata()),
    )];
static FILTERED_INDEXED_SESSION_SQL_COMPOSITE_INDEX_FIELDS: [&str; 2] = ["tier", "handle"];
static FILTERED_INDEXED_SESSION_SQL_COMPOSITE_INDEX_MODELS: [IndexModel; 1] =
    [IndexModel::generated_with_ordinal_and_predicate(
        1,
        "tier_handle_active_only",
        IndexedSessionSqlStore::PATH,
        &FILTERED_INDEXED_SESSION_SQL_COMPOSITE_INDEX_FIELDS,
        false,
        Some(active_true_predicate_metadata()),
    )];
static FILTERED_INDEXED_SESSION_SQL_EXPRESSION_INDEX_FIELDS: [&str; 1] = ["handle"];
static FILTERED_INDEXED_SESSION_SQL_EXPRESSION_INDEX_KEY_ITEMS: [IndexKeyItem; 1] =
    [IndexKeyItem::Expression(IndexExpression::Lower("handle"))];
static FILTERED_INDEXED_SESSION_SQL_EXPRESSION_INDEX_MODELS: [IndexModel; 1] = [
    IndexModel::generated_with_ordinal_and_key_items_and_predicate(
        2,
        "handle_lower_active_only",
        IndexedSessionSqlStore::PATH,
        &FILTERED_INDEXED_SESSION_SQL_EXPRESSION_INDEX_FIELDS,
        Some(&FILTERED_INDEXED_SESSION_SQL_EXPRESSION_INDEX_KEY_ITEMS),
        false,
        Some(active_true_predicate_metadata()),
    ),
];
static FILTERED_INDEXED_SESSION_SQL_COMPOSITE_EXPRESSION_INDEX_FIELDS: [&str; 2] =
    ["tier", "handle"];
static FILTERED_INDEXED_SESSION_SQL_COMPOSITE_EXPRESSION_INDEX_KEY_ITEMS: [IndexKeyItem; 2] = [
    IndexKeyItem::Field("tier"),
    IndexKeyItem::Expression(IndexExpression::Lower("handle")),
];
static FILTERED_INDEXED_SESSION_SQL_COMPOSITE_EXPRESSION_INDEX_MODELS: [IndexModel; 1] = [
    IndexModel::generated_with_ordinal_and_key_items_and_predicate(
        3,
        "tier_handle_lower_active_only",
        IndexedSessionSqlStore::PATH,
        &FILTERED_INDEXED_SESSION_SQL_COMPOSITE_EXPRESSION_INDEX_FIELDS,
        Some(&FILTERED_INDEXED_SESSION_SQL_COMPOSITE_EXPRESSION_INDEX_KEY_ITEMS),
        false,
        Some(active_true_predicate_metadata()),
    ),
];
static COMPOSITE_INDEXED_SESSION_SQL_INDEX_FIELDS: [&str; 2] = ["code", "serial"];
static COMPOSITE_INDEXED_SESSION_SQL_INDEX_MODELS: [IndexModel; 1] = [IndexModel::generated(
    "code_serial",
    IndexedSessionSqlStore::PATH,
    &COMPOSITE_INDEXED_SESSION_SQL_INDEX_FIELDS,
    false,
)];
static EXPRESSION_INDEXED_SESSION_SQL_INDEX_FIELDS: [&str; 1] = ["name"];
static EXPRESSION_INDEXED_SESSION_SQL_INDEX_KEY_ITEMS: [IndexKeyItem; 1] =
    [IndexKeyItem::Expression(IndexExpression::Lower("name"))];
static EXPRESSION_INDEXED_SESSION_SQL_INDEX_MODELS: [IndexModel; 1] =
    [IndexModel::generated_with_key_items(
        "name_lower",
        IndexedSessionSqlStore::PATH,
        &EXPRESSION_INDEXED_SESSION_SQL_INDEX_FIELDS,
        &EXPRESSION_INDEXED_SESSION_SQL_INDEX_KEY_ITEMS,
        false,
    )];
static SESSION_EXPLAIN_INDEX_FIELDS: [&str; 2] = ["group", "rank"];
static SESSION_EXPLAIN_INDEX_MODELS: [IndexModel; 1] = [IndexModel::generated(
    "group_rank",
    IndexedSessionSqlStore::PATH,
    &SESSION_EXPLAIN_INDEX_FIELDS,
    false,
)];
static SESSION_DETERMINISTIC_CHOICE_LABEL_INDEX_FIELDS: [&str; 2] = ["tier", "label"];
static SESSION_DETERMINISTIC_CHOICE_HANDLE_INDEX_FIELDS: [&str; 2] = ["tier", "handle"];
static SESSION_DETERMINISTIC_CHOICE_INDEX_MODELS: [IndexModel; 2] = [
    IndexModel::generated_with_ordinal(
        0,
        "a_tier_label_idx",
        IndexedSessionSqlStore::PATH,
        &SESSION_DETERMINISTIC_CHOICE_LABEL_INDEX_FIELDS,
        false,
    ),
    IndexModel::generated_with_ordinal(
        1,
        "z_tier_handle_idx",
        IndexedSessionSqlStore::PATH,
        &SESSION_DETERMINISTIC_CHOICE_HANDLE_INDEX_FIELDS,
        false,
    ),
];
static SESSION_DETERMINISTIC_RANGE_HANDLE_INDEX_FIELDS: [&str; 3] = ["tier", "score", "handle"];
static SESSION_DETERMINISTIC_RANGE_LABEL_INDEX_FIELDS: [&str; 3] = ["tier", "score", "label"];
static SESSION_DETERMINISTIC_RANGE_INDEX_MODELS: [IndexModel; 2] = [
    IndexModel::generated_with_ordinal(
        0,
        "a_tier_score_handle_idx",
        IndexedSessionSqlStore::PATH,
        &SESSION_DETERMINISTIC_RANGE_HANDLE_INDEX_FIELDS,
        false,
    ),
    IndexModel::generated_with_ordinal(
        1,
        "z_tier_score_label_idx",
        IndexedSessionSqlStore::PATH,
        &SESSION_DETERMINISTIC_RANGE_LABEL_INDEX_FIELDS,
        false,
    ),
];
static SESSION_UNIQUE_PREFIX_OFFSET_INDEX_FIELDS: [&str; 2] = ["tier", "handle"];
static SESSION_UNIQUE_PREFIX_OFFSET_INDEX_MODELS: [IndexModel; 1] = [IndexModel::generated(
    "tier_handle_unique",
    IndexedSessionSqlStore::PATH,
    &SESSION_UNIQUE_PREFIX_OFFSET_INDEX_FIELDS,
    true,
)];
static SESSION_ORDER_ONLY_CHOICE_BETA_INDEX_FIELDS: [&str; 1] = ["beta"];
static SESSION_ORDER_ONLY_CHOICE_ALPHA_INDEX_FIELDS: [&str; 1] = ["alpha"];
static SESSION_ORDER_ONLY_CHOICE_INDEX_MODELS: [IndexModel; 2] = [
    IndexModel::generated_with_ordinal(
        0,
        "a_beta_idx",
        IndexedSessionSqlStore::PATH,
        &SESSION_ORDER_ONLY_CHOICE_BETA_INDEX_FIELDS,
        false,
    ),
    IndexModel::generated_with_ordinal(
        1,
        "z_alpha_idx",
        IndexedSessionSqlStore::PATH,
        &SESSION_ORDER_ONLY_CHOICE_ALPHA_INDEX_FIELDS,
        false,
    ),
];

crate::test_entity_schema! {
    ident = SessionSqlEntity,
    id = Ulid,
    id_field = id,
    entity_name = "SessionSqlEntity",
    entity_tag = crate::testing::SESSION_SQL_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("name", FieldKind::Text),
        ("age", FieldKind::Uint),
    ],
    indexes = [],
    store = SessionSqlStore,
    canister = SessionSqlCanister,
}

crate::test_entity_schema! {
    ident = IndexedSessionSqlEntity,
    id = Ulid,
    id_field = id,
    entity_name = "IndexedSessionSqlEntity",
    entity_tag = EntityTag::new(0x1033),
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("name", FieldKind::Text),
        ("age", FieldKind::Uint),
    ],
    indexes = [&INDEXED_SESSION_SQL_INDEX_MODELS[0]],
    store = IndexedSessionSqlStore,
    canister = SessionSqlCanister,
}

crate::test_entity_schema! {
    ident = CompositeIndexedSessionSqlEntity,
    id = Ulid,
    id_field = id,
    entity_name = "CompositeIndexedSessionSqlEntity",
    entity_tag = EntityTag::new(0x1037),
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("code", FieldKind::Text),
        ("serial", FieldKind::Uint),
        ("note", FieldKind::Text),
    ],
    indexes = [&COMPOSITE_INDEXED_SESSION_SQL_INDEX_MODELS[0]],
    store = IndexedSessionSqlStore,
    canister = SessionSqlCanister,
}

crate::test_entity_schema! {
    ident = FilteredIndexedSessionSqlEntity,
    id = Ulid,
    id_field = id,
    entity_name = "FilteredIndexedSessionSqlEntity",
    entity_tag = EntityTag::new(0x1039),
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("name", FieldKind::Text),
        ("active", FieldKind::Bool),
        ("tier", FieldKind::Text),
        ("handle", FieldKind::Text),
        ("age", FieldKind::Uint),
    ],
    indexes = [
        &FILTERED_INDEXED_SESSION_SQL_INDEX_MODELS[0],
        &FILTERED_INDEXED_SESSION_SQL_COMPOSITE_INDEX_MODELS[0],
        &FILTERED_INDEXED_SESSION_SQL_EXPRESSION_INDEX_MODELS[0],
        &FILTERED_INDEXED_SESSION_SQL_COMPOSITE_EXPRESSION_INDEX_MODELS[0],
    ],
    store = IndexedSessionSqlStore,
    canister = SessionSqlCanister,
}

crate::test_entity_schema! {
    ident = ExpressionIndexedSessionSqlEntity,
    id = Ulid,
    id_field = id,
    entity_name = "ExpressionIndexedSessionSqlEntity",
    entity_tag = EntityTag::new(0x1038),
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("name", FieldKind::Text),
        ("age", FieldKind::Uint),
    ],
    indexes = [&EXPRESSION_INDEXED_SESSION_SQL_INDEX_MODELS[0]],
    store = IndexedSessionSqlStore,
    canister = SessionSqlCanister,
}

crate::test_entity_schema! {
    ident = SessionAggregateEntity,
    id = Ulid,
    id_field = id,
    entity_name = "SessionAggregateEntity",
    entity_tag = EntityTag::new(0x1034),
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("group", FieldKind::Uint),
        ("rank", FieldKind::Uint),
        ("label", FieldKind::Text),
    ],
    indexes = [],
    store = SessionSqlStore,
    canister = SessionSqlCanister,
}

crate::test_entity_schema! {
    ident = SessionExplainEntity,
    id = Ulid,
    id_field = id,
    entity_name = "SessionExplainEntity",
    entity_tag = EntityTag::new(0x1035),
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("group", FieldKind::Uint),
        ("rank", FieldKind::Uint),
        ("label", FieldKind::Text),
    ],
    indexes = [&SESSION_EXPLAIN_INDEX_MODELS[0]],
    store = IndexedSessionSqlStore,
    canister = SessionSqlCanister,
}

crate::test_entity_schema! {
    ident = SessionTemporalEntity,
    id = Ulid,
    id_field = id,
    entity_name = "SessionTemporalEntity",
    entity_tag = EntityTag::new(0x1036),
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("occurred_on", FieldKind::Date),
        ("occurred_at", FieldKind::Timestamp),
        ("elapsed", FieldKind::Duration),
    ],
    indexes = [],
    store = SessionSqlStore,
    canister = SessionSqlCanister,
}

crate::test_entity_schema! {
    ident = SessionDeterministicChoiceEntity,
    id = Ulid,
    id_field = id,
    entity_name = "SessionDeterministicChoiceEntity",
    entity_tag = EntityTag::new(0x1040),
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("tier", FieldKind::Text),
        ("handle", FieldKind::Text),
        ("label", FieldKind::Text),
    ],
    indexes = [
        &SESSION_DETERMINISTIC_CHOICE_INDEX_MODELS[0],
        &SESSION_DETERMINISTIC_CHOICE_INDEX_MODELS[1],
    ],
    store = IndexedSessionSqlStore,
    canister = SessionSqlCanister,
}

crate::test_entity_schema! {
    ident = SessionDeterministicRangeEntity,
    id = Ulid,
    id_field = id,
    entity_name = "SessionDeterministicRangeEntity",
    entity_tag = EntityTag::new(0x1041),
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("tier", FieldKind::Text),
        ("score", FieldKind::Uint),
        ("handle", FieldKind::Text),
        ("label", FieldKind::Text),
    ],
    indexes = [
        &SESSION_DETERMINISTIC_RANGE_INDEX_MODELS[0],
        &SESSION_DETERMINISTIC_RANGE_INDEX_MODELS[1],
    ],
    store = IndexedSessionSqlStore,
    canister = SessionSqlCanister,
}

crate::test_entity_schema! {
    ident = SessionUniquePrefixOffsetEntity,
    id = Ulid,
    id_field = id,
    entity_name = "SessionUniquePrefixOffsetEntity",
    entity_tag = EntityTag::new(0x1043),
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("tier", FieldKind::Text),
        ("handle", FieldKind::Text),
        ("note", FieldKind::Text),
    ],
    indexes = [&SESSION_UNIQUE_PREFIX_OFFSET_INDEX_MODELS[0]],
    store = IndexedSessionSqlStore,
    canister = SessionSqlCanister,
}

crate::test_entity_schema! {
    ident = SessionOrderOnlyChoiceEntity,
    id = Ulid,
    id_field = id,
    entity_name = "SessionOrderOnlyChoiceEntity",
    entity_tag = EntityTag::new(0x1042),
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("alpha", FieldKind::Text),
        ("beta", FieldKind::Text),
    ],
    indexes = [
        &SESSION_ORDER_ONLY_CHOICE_INDEX_MODELS[0],
        &SESSION_ORDER_ONLY_CHOICE_INDEX_MODELS[1],
    ],
    store = IndexedSessionSqlStore,
    canister = SessionSqlCanister,
}

// Reset all session SQL fixture state between tests to preserve deterministic assertions.
fn reset_session_sql_store() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    ensure_recovered(&SESSION_SQL_DB).expect("write-side recovery should succeed");
    SESSION_SQL_DATA_STORE.with(|store| store.borrow_mut().clear());
    SESSION_SQL_INDEX_STORE.with(|store| {
        let mut store = store.borrow_mut();
        store.clear();
        store.mark_ready();
    });
}

fn sql_session() -> DbSession<SessionSqlCanister> {
    DbSession::new(SESSION_SQL_DB)
}

fn reset_indexed_session_sql_store() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    ensure_recovered(&INDEXED_SESSION_SQL_DB).expect("write-side recovery should succeed");
    INDEXED_SESSION_SQL_DATA_STORE.with(|store| store.borrow_mut().clear());
    INDEXED_SESSION_SQL_INDEX_STORE.with(|store| {
        let mut store = store.borrow_mut();
        store.clear();
        store.mark_ready();
    });
}

fn indexed_sql_session() -> DbSession<SessionSqlCanister> {
    DbSession::new(INDEXED_SESSION_SQL_DB)
}

// Resolve the indexed SQL store handle through the recovered DB boundary.
fn indexed_session_sql_store_handle() -> StoreHandle {
    INDEXED_SESSION_SQL_DB
        .recovered_store(IndexedSessionSqlStore::PATH)
        .expect("indexed SQL store should recover")
}

// Mark the indexed SQL secondary index as Building so planner visibility drops
// it from secondary-index planning.
fn mark_indexed_session_sql_index_building() {
    indexed_session_sql_store_handle().mark_index_building();
}

// Mark the indexed SQL secondary index as Dropping so planner visibility drops
// it from secondary-index planning.
fn mark_indexed_session_sql_index_dropping() {
    indexed_session_sql_store_handle().with_index_mut(IndexStore::mark_dropping);
}

#[test]
fn session_select_one_returns_constant_without_execution_metrics() {
    let session = sql_session();
    let sink = SessionMetricsCaptureSink::default();
    let value = with_metrics_sink(&sink, || session.select_one());
    let events = sink.into_events();

    assert_eq!(value, Value::Int(1), "select_one should return constant 1");
    assert!(
        events.is_empty(),
        "select_one should bypass planner and executor metrics emission",
    );
}

#[test]
fn session_show_indexes_reports_primary_and_secondary_indexes() {
    reset_session_sql_store();
    reset_indexed_session_sql_store();
    let session = sql_session();
    let indexed_session = indexed_sql_session();

    assert_eq!(
        session.show_indexes::<SessionSqlEntity>(),
        vec!["PRIMARY KEY (id) [state=ready]".to_string()],
        "entities without secondary indexes should only report primary key metadata",
    );
    assert_eq!(
        indexed_session.show_indexes::<IndexedSessionSqlEntity>(),
        vec![
            "PRIMARY KEY (id) [state=ready]".to_string(),
            "INDEX name (name) [state=ready]".to_string(),
        ],
        "entities with one secondary index should report both primary and index rows",
    );
}

#[test]
fn session_show_indexes_sql_reports_runtime_index_state_transitions() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    assert_eq!(
        dispatch_show_indexes_sql::<IndexedSessionSqlEntity>(
            &session,
            "SHOW INDEXES IndexedSessionSqlEntity",
        )
        .expect("SHOW INDEXES should succeed for ready index"),
        vec![
            "PRIMARY KEY (id) [state=ready]".to_string(),
            "INDEX name (name) [state=ready]".to_string(),
        ],
        "SHOW INDEXES should expose the default ready lifecycle state on the runtime metadata surface",
    );

    mark_indexed_session_sql_index_building();
    assert_eq!(
        dispatch_show_indexes_sql::<IndexedSessionSqlEntity>(
            &session,
            "SHOW INDEXES IndexedSessionSqlEntity",
        )
        .expect("SHOW INDEXES should succeed for building index"),
        vec![
            "PRIMARY KEY (id) [state=building]".to_string(),
            "INDEX name (name) [state=building]".to_string(),
        ],
        "SHOW INDEXES should expose Building while planner visibility removes the index from covering routes",
    );

    mark_indexed_session_sql_index_dropping();
    assert_eq!(
        dispatch_show_indexes_sql::<IndexedSessionSqlEntity>(
            &session,
            "SHOW INDEXES IndexedSessionSqlEntity",
        )
        .expect("SHOW INDEXES should succeed for dropping index"),
        vec![
            "PRIMARY KEY (id) [state=dropping]".to_string(),
            "INDEX name (name) [state=dropping]".to_string(),
        ],
        "SHOW INDEXES should expose Dropping while planner visibility removes the index from covering routes",
    );
}

#[test]
fn session_describe_entity_reports_fields_and_indexes() {
    let session = sql_session();
    let indexed_session = indexed_sql_session();

    let plain = session.describe_entity::<SessionSqlEntity>();
    assert_eq!(plain.entity_name(), "SessionSqlEntity");
    assert_eq!(plain.primary_key(), "id");
    assert_eq!(plain.fields().len(), 3);
    assert!(plain.fields().iter().any(|field| {
        field.name() == "age" && field.kind() == "uint" && field.queryable() && !field.primary_key()
    }));
    assert!(
        plain.indexes().is_empty(),
        "entities without secondary indexes should not emit describe index rows",
    );
    assert!(
        plain.relations().is_empty(),
        "non-relation entities should not emit relation describe rows",
    );

    let indexed = indexed_session.describe_entity::<IndexedSessionSqlEntity>();
    assert_eq!(indexed.entity_name(), "IndexedSessionSqlEntity");
    assert_eq!(indexed.primary_key(), "id");
    assert_eq!(
        indexed.indexes(),
        vec![crate::db::EntityIndexDescription {
            name: "name".to_string(),
            unique: false,
            fields: vec!["name".to_string()],
        }],
        "secondary index metadata should be projected for describe consumers",
    );
}

#[test]
fn session_trace_query_reports_plan_hash_and_route_summary() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(
        &session,
        &[("Sam", 30), ("Sasha", 24), ("Mira", 40), ("Soren", 18)],
    );

    let query = session
        .query_from_sql::<IndexedSessionSqlEntity>(
            "SELECT * FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name ASC LIMIT 2",
        )
        .expect("trace-query SQL fixture should lower");
    let expected_hash = query
        .plan_hash_hex()
        .expect("query plan hash should derive from planner contracts");
    let query_explain = query
        .explain()
        .expect("query explain for trace parity should succeed");
    let trace = session
        .trace_query(&query)
        .expect("session trace_query should succeed");
    let trace_explain = trace.explain();

    assert_eq!(
        trace.plan_hash(),
        expected_hash,
        "trace payload must project the same hash as direct plan-hash derivation",
    );
    assert_eq!(
        trace_explain.access(),
        query_explain.access(),
        "trace explain access path should preserve planner-selected access shape",
    );
    assert!(
        trace.access_strategy().starts_with("Index")
            || trace.access_strategy().starts_with("PrimaryKeyRange")
            || trace.access_strategy() == "FullScan"
            || trace.access_strategy().starts_with("Union(")
            || trace.access_strategy().starts_with("Intersection("),
        "trace access strategy summary should provide a human-readable selected access hint",
    );
    assert!(
        matches!(
            trace.execution_strategy(),
            Some(crate::db::TraceExecutionStrategy::Ordered)
        ),
        "ordered load shapes should project ordered execution strategy in trace payload",
    );
    assert!(
        matches!(
            trace_explain.order_pushdown(),
            crate::db::query::explain::ExplainOrderPushdown::EligibleSecondaryIndex { .. }
                | crate::db::query::explain::ExplainOrderPushdown::Rejected(_)
                | crate::db::query::explain::ExplainOrderPushdown::MissingModelContext
        ),
        "trace explain output must carry planner pushdown eligibility diagnostics",
    );
}

#[test]
fn fluent_load_explain_execution_surface_adapters_are_available() {
    reset_session_sql_store();
    let session = sql_session();
    let query = session
        .load::<SessionSqlEntity>()
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::Eq,
            Value::Ulid(Ulid::from_u128(9_201)),
            CoercionId::Strict,
        )))
        .order_by("id");
    let descriptor = query
        .explain_execution()
        .expect("fluent execution descriptor explain should build");

    let text = query
        .explain_execution_text()
        .expect("fluent execution text explain should build");
    assert!(
        text.contains("ByKeyLookup"),
        "fluent execution text surface should include root node type",
    );
    assert_eq!(
        text,
        descriptor.render_text_tree(),
        "fluent execution text surface should be canonical descriptor text rendering",
    );

    let json = query
        .explain_execution_json()
        .expect("fluent execution json explain should build");
    assert!(
        json.contains("\"node_type\":\"ByKeyLookup\""),
        "fluent execution json surface should include canonical root node type",
    );
    assert_eq!(
        json,
        descriptor.render_json_canonical(),
        "fluent execution json surface should be canonical descriptor json rendering",
    );

    let verbose = query
        .explain_execution_verbose()
        .expect("fluent execution verbose explain should build");
    assert!(
        verbose.contains("diag.r.secondary_order_pushdown="),
        "fluent execution verbose surface should include diagnostics",
    );
}

#[test]
fn session_fluent_verbose_prefix_choice_prefers_order_compatible_index_when_rank_ties() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let verbose = session
        .load::<SessionDeterministicChoiceEntity>()
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "tier",
            CompareOp::Eq,
            Value::Text("gold".to_string()),
            CoercionId::Strict,
        )))
        .order_by("handle")
        .order_by("id")
        .explain_execution_verbose()
        .expect("session deterministic prefix verbose explain should build");

    let diagnostics = session_verbose_diagnostics_map(&verbose);

    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen"),
        Some(&"IndexPrefix(z_tier_handle_idx)".to_string()),
        "session fluent verbose explain must project the session-visible order-compatible prefix index",
    );
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen_reason"),
        Some(&"order_compatible_preferred".to_string()),
        "session fluent verbose explain must report the canonical order-compatibility tie-break when prefix rank ties",
    );
    assert!(
        diagnostics
            .get("diag.r.access_choice_rejections")
            .is_some_and(|rejections| {
                rejections.contains("index:a_tier_label_idx=order_compatible_preferred")
            }),
        "session fluent verbose explain must report the lexicographically earlier but order-incompatible prefix index as planner-rejected for the same canonical reason",
    );
}

#[test]
fn session_fluent_verbose_range_choice_prefers_order_compatible_index_when_rank_ties() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let verbose = session
        .load::<SessionDeterministicRangeEntity>()
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "tier",
                CompareOp::Eq,
                Value::Text("gold".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "score",
                CompareOp::Gt,
                Value::Uint(10),
                CoercionId::Strict,
            )),
        ]))
        .order_by("score")
        .order_by("label")
        .order_by("id")
        .explain_execution_verbose()
        .expect("session deterministic range verbose explain should build");

    let diagnostics = session_verbose_diagnostics_map(&verbose);

    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen"),
        Some(&"IndexRange(z_tier_score_label_idx)".to_string()),
        "session fluent verbose explain must project the session-visible order-compatible range index",
    );
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen_reason"),
        Some(&"order_compatible_preferred".to_string()),
        "session fluent verbose explain must report the canonical order-compatibility tie-break when range rank ties",
    );
    assert!(
        diagnostics
            .get("diag.r.access_choice_rejections")
            .is_some_and(|rejections| {
                rejections.contains("index:a_tier_score_handle_idx=order_compatible_preferred")
            }),
        "session fluent verbose explain must report the lexicographically earlier but order-incompatible range index as planner-rejected for the same canonical reason",
    );
}

#[test]
fn session_fluent_verbose_range_choice_desc_prefers_order_compatible_index_when_rank_ties() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let verbose = session
        .load::<SessionDeterministicRangeEntity>()
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "tier",
                CompareOp::Eq,
                Value::Text("gold".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "score",
                CompareOp::Gt,
                Value::Uint(10),
                CoercionId::Strict,
            )),
        ]))
        .order_by_desc("score")
        .order_by_desc("label")
        .order_by_desc("id")
        .explain_execution_verbose()
        .expect("session descending deterministic range verbose explain should build");

    let diagnostics = session_verbose_diagnostics_map(&verbose);

    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen"),
        Some(&"IndexRange(z_tier_score_label_idx)".to_string()),
        "session descending verbose explain must project the session-visible order-compatible range index",
    );
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen_reason"),
        Some(&"order_compatible_preferred".to_string()),
        "session descending verbose explain must report the canonical order-compatibility tie-break when range rank ties",
    );
    assert!(
        diagnostics
            .get("diag.r.access_choice_rejections")
            .is_some_and(|rejections| {
                rejections.contains("index:a_tier_score_handle_idx=order_compatible_preferred")
            }),
        "session descending verbose explain must report the lexicographically earlier but order-incompatible range index as planner-rejected for the same canonical reason",
    );
}

#[test]
fn session_fluent_verbose_equality_prefix_suffix_order_prefers_order_compatible_index_when_rank_ties()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let verbose = session
        .load::<SessionDeterministicRangeEntity>()
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "tier",
                CompareOp::Eq,
                Value::Text("gold".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "score",
                CompareOp::Eq,
                Value::Uint(20),
                CoercionId::Strict,
            )),
        ]))
        .order_by("label")
        .order_by("id")
        .explain_execution_verbose()
        .expect("session deterministic equality-prefix suffix-order verbose explain should build");

    let diagnostics = session_verbose_diagnostics_map(&verbose);

    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen"),
        Some(&"IndexPrefix(z_tier_score_label_idx)".to_string()),
        "session fluent verbose explain must project the session-visible order-compatible equality-prefix suffix-order index",
    );
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen_reason"),
        Some(&"order_compatible_preferred".to_string()),
        "session fluent verbose explain must report the canonical order-compatibility tie-break when equality-prefix suffix-order rank ties",
    );
    assert!(
        diagnostics
            .get("diag.r.access_choice_rejections")
            .is_some_and(|rejections| {
                rejections.contains("index:a_tier_score_handle_idx=order_compatible_preferred")
            }),
        "session fluent verbose explain must report the lexicographically earlier but order-incompatible equality-prefix suffix-order index as planner-rejected for the same canonical reason",
    );
}

#[test]
fn session_fluent_verbose_equality_prefix_suffix_order_desc_prefers_order_compatible_index_when_rank_ties()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let verbose = session
        .load::<SessionDeterministicRangeEntity>()
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "tier",
                CompareOp::Eq,
                Value::Text("gold".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "score",
                CompareOp::Eq,
                Value::Uint(20),
                CoercionId::Strict,
            )),
        ]))
        .order_by_desc("label")
        .order_by_desc("id")
        .explain_execution_verbose()
        .expect("session descending deterministic equality-prefix suffix-order verbose explain should build");

    let diagnostics = session_verbose_diagnostics_map(&verbose);

    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen"),
        Some(&"IndexPrefix(z_tier_score_label_idx)".to_string()),
        "session descending verbose explain must project the session-visible order-compatible equality-prefix suffix-order index",
    );
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen_reason"),
        Some(&"order_compatible_preferred".to_string()),
        "session descending verbose explain must report the canonical order-compatibility tie-break when equality-prefix suffix-order rank ties",
    );
    assert_eq!(
        diagnostics.get("diag.r.load_order_route_contract"),
        Some(&"materialized_boundary".to_string()),
        "session descending verbose explain must expose the materialized-boundary route contract for descending non-unique equality-prefix suffix-order shapes",
    );
    assert_eq!(
        diagnostics.get("diag.r.load_order_route_reason"),
        Some(&"descending_non_unique_secondary_prefix_not_admitted".to_string()),
        "session descending verbose explain must expose the planner-owned materialized-boundary reason for descending non-unique equality-prefix suffix-order shapes",
    );
    assert!(
        diagnostics
            .get("diag.r.access_choice_rejections")
            .is_some_and(|rejections| {
                rejections.contains("index:a_tier_score_handle_idx=order_compatible_preferred")
            }),
        "session descending verbose explain must report the lexicographically earlier but order-incompatible equality-prefix suffix-order index as planner-rejected for the same canonical reason",
    );
}

#[test]
fn session_fluent_verbose_order_only_choice_prefers_order_compatible_index_when_rank_ties() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let verbose = session
        .load::<SessionOrderOnlyChoiceEntity>()
        .order_by("alpha")
        .order_by("id")
        .explain_execution_verbose()
        .expect("session deterministic order-only verbose explain should build");

    let diagnostics = session_verbose_diagnostics_map(&verbose);

    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen"),
        Some(&"IndexRange(z_alpha_idx)".to_string()),
        "session fluent verbose explain must project the session-visible order-compatible fallback index",
    );
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen_reason"),
        Some(&"order_compatible_preferred".to_string()),
        "session fluent verbose explain must report the canonical order-compatibility tie-break when order-only ranking ties",
    );
    assert_eq!(
        diagnostics.get("diag.r.load_order_route_contract"),
        Some(&"direct_streaming".to_string()),
        "session fluent verbose explain must expose the direct ordered-load route contract for admitted order-only fallback shapes",
    );
    assert_eq!(
        diagnostics.get("diag.r.load_order_route_reason"),
        Some(&"none".to_string()),
        "session fluent verbose explain must keep direct order-only fallback admission reason-free once the chosen route is already streaming-safe",
    );
    assert!(
        diagnostics
            .get("diag.r.access_choice_rejections")
            .is_some_and(|rejections| {
                rejections.contains("index:a_beta_idx=order_compatible_preferred")
            }),
        "session fluent verbose explain must report the lexicographically earlier but order-incompatible fallback index as planner-rejected for the same canonical reason",
    );
}

#[test]
fn session_fluent_verbose_composite_order_only_choice_prefers_order_compatible_index_when_rank_ties()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let verbose = session
        .load::<SessionDeterministicChoiceEntity>()
        .order_by("tier")
        .order_by("handle")
        .order_by("id")
        .explain_execution_verbose()
        .expect("session deterministic composite order-only verbose explain should build");

    let diagnostics = session_verbose_diagnostics_map(&verbose);

    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen"),
        Some(&"IndexRange(z_tier_handle_idx)".to_string()),
        "session fluent verbose explain must project the session-visible order-compatible composite fallback index",
    );
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen_reason"),
        Some(&"order_compatible_preferred".to_string()),
        "session fluent verbose explain must report the canonical order-compatibility tie-break when composite order-only ranking ties",
    );
    assert!(
        diagnostics
            .get("diag.r.access_choice_rejections")
            .is_some_and(|rejections| {
                rejections.contains("index:a_tier_label_idx=order_compatible_preferred")
            }),
        "session fluent verbose explain must report the lexicographically earlier but order-incompatible composite fallback index as planner-rejected for the same canonical reason",
    );
}

#[test]
fn session_fluent_verbose_composite_order_only_choice_desc_prefers_order_compatible_index_when_rank_ties()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    let verbose = session
        .load::<SessionDeterministicChoiceEntity>()
        .order_by_desc("tier")
        .order_by_desc("handle")
        .order_by_desc("id")
        .explain_execution_verbose()
        .expect(
            "session descending deterministic composite order-only verbose explain should build",
        );

    let diagnostics = session_verbose_diagnostics_map(&verbose);

    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen"),
        Some(&"IndexRange(z_tier_handle_idx)".to_string()),
        "session descending verbose explain must project the session-visible order-compatible composite fallback index",
    );
    assert_eq!(
        diagnostics.get("diag.r.access_choice_chosen_reason"),
        Some(&"order_compatible_preferred".to_string()),
        "session descending verbose explain must report the canonical order-compatibility tie-break when composite order-only ranking ties",
    );
    assert!(
        diagnostics
            .get("diag.r.access_choice_rejections")
            .is_some_and(|rejections| {
                rejections.contains("index:a_tier_label_idx=order_compatible_preferred")
            }),
        "session descending verbose explain must report the lexicographically earlier but order-incompatible composite fallback index as planner-rejected for the same canonical reason",
    );
}

#[test]
fn session_explain_execution_equality_prefix_suffix_order_uses_top_n_seek_on_chosen_prefix_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let descriptor = session
        .load::<SessionDeterministicRangeEntity>()
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "tier",
                CompareOp::Eq,
                Value::Text("gold".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "score",
                CompareOp::Eq,
                Value::Uint(20),
                CoercionId::Strict,
            )),
        ]))
        .order_by("label")
        .order_by("id")
        .limit(2)
        .explain_execution()
        .expect(
            "session deterministic equality-prefix suffix-order explain_execution should build",
        );

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexPrefixScan,
        "equality-prefix suffix-order roots should stay on the chosen index-prefix route",
    );
    assert!(
        descriptor.access_strategy().is_some_and(
            |access| matches!(access, ExplainAccessPath::IndexPrefix { name, .. } if *name == "z_tier_score_label_idx")
        ),
        "equality-prefix suffix-order roots should expose the chosen order-compatible composite index",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "equality-prefix suffix-order roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_some(),
        "equality-prefix suffix-order roots should derive Top-N seek for bounded ordered windows",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_none(),
        "equality-prefix suffix-order prefix routes must not pretend to be index-range limit-pushdown shapes",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "equality-prefix suffix-order roots should keep access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_equality_prefix_suffix_order_desc_materializes_order_on_chosen_prefix_route()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let descriptor = session
        .load::<SessionDeterministicRangeEntity>()
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "tier",
                CompareOp::Eq,
                Value::Text("gold".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "score",
                CompareOp::Eq,
                Value::Uint(20),
                CoercionId::Strict,
            )),
        ]))
        .order_by_desc("label")
        .order_by_desc("id")
        .limit(2)
        .explain_execution()
        .expect(
            "session descending deterministic equality-prefix suffix-order explain_execution should build",
        );

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexPrefixScan,
        "descending equality-prefix suffix-order roots should stay on the chosen index-prefix route",
    );
    assert!(
        descriptor.access_strategy().is_some_and(
            |access| matches!(access, ExplainAccessPath::IndexPrefix { name, .. } if *name == "z_tier_score_label_idx")
        ),
        "descending equality-prefix suffix-order roots should expose the chosen order-compatible composite index",
    );
    assert_eq!(
        descriptor.node_properties().get("scan_dir"),
        Some(&Value::Text("desc".to_string())),
        "descending equality-prefix suffix-order roots should expose the descending scan direction",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending equality-prefix suffix-order roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        )
        .is_some(),
        "descending equality-prefix suffix-order roots should fail closed to a materialized order stage on the chosen prefix route",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_none(),
        "descending equality-prefix suffix-order prefix routes must not pretend to be index-range limit-pushdown shapes",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_none(),
        "descending equality-prefix suffix-order roots should stay off the ascending prefix Top-N seek shape",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_none(),
        "descending equality-prefix suffix-order roots should not claim access-satisfied ordering once they materialize sort order",
    );
}

#[test]
fn session_execute_equality_prefix_suffix_order_offset_windows_preserve_ordered_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic equality-prefix suffix-order dataset so
    // the offset window can validate the retained ordered page on both scan
    // directions.
    for (id, tier, score, handle, label) in [
        (9_041_u128, "gold", 20_u64, "h-amber", "amber"),
        (9_042_u128, "gold", 20_u64, "h-bravo", "bravo"),
        (9_043_u128, "gold", 20_u64, "h-charlie", "charlie"),
        (9_044_u128, "gold", 20_u64, "h-delta", "delta"),
        (9_045_u128, "silver", 20_u64, "h-echo", "echo"),
    ] {
        session
            .insert(SessionDeterministicRangeEntity {
                id: Ulid::from_u128(id),
                tier: tier.to_string(),
                score,
                handle: handle.to_string(),
                label: label.to_string(),
            })
            .expect("equality-prefix suffix-order offset seed insert should succeed");
    }

    // Phase 2: execute one ascending and one descending offset window on the
    // same equality-prefix suffix-order shape.
    let asc = session
        .load::<SessionDeterministicRangeEntity>()
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "tier",
                CompareOp::Eq,
                Value::Text("gold".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "score",
                CompareOp::Eq,
                Value::Uint(20),
                CoercionId::Strict,
            )),
        ]))
        .order_by("label")
        .order_by("id")
        .offset(1)
        .limit(2)
        .execute()
        .expect("ascending equality-prefix suffix-order offset window should execute");
    let desc = session
        .load::<SessionDeterministicRangeEntity>()
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "tier",
                CompareOp::Eq,
                Value::Text("gold".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "score",
                CompareOp::Eq,
                Value::Uint(20),
                CoercionId::Strict,
            )),
        ]))
        .order_by_desc("label")
        .order_by_desc("id")
        .offset(1)
        .limit(2)
        .execute()
        .expect("descending equality-prefix suffix-order offset window should execute");

    let asc_labels = asc
        .iter()
        .map(|row| row.entity_ref().label.as_str())
        .collect::<Vec<_>>();
    let desc_labels = desc
        .iter()
        .map(|row| row.entity_ref().label.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        asc_labels,
        vec!["bravo", "charlie"],
        "ascending equality-prefix suffix-order offset windows should preserve the chosen suffix order",
    );
    assert_eq!(
        desc_labels,
        vec!["charlie", "bravo"],
        "descending equality-prefix suffix-order offset windows should preserve the reversed ordered window even when execution falls back downstream",
    );
}

#[test]
fn session_explain_execution_equality_prefix_suffix_order_offset_uses_top_n_seek_on_chosen_prefix_route()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let descriptor = session
        .load::<SessionDeterministicRangeEntity>()
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "tier",
                CompareOp::Eq,
                Value::Text("gold".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "score",
                CompareOp::Eq,
                Value::Uint(20),
                CoercionId::Strict,
            )),
        ]))
        .order_by("label")
        .order_by("id")
        .offset(1)
        .limit(2)
        .explain_execution()
        .expect("session equality-prefix suffix-order offset explain_execution should build");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexPrefixScan,
        "equality-prefix suffix-order offset roots should stay on the chosen index-prefix route",
    );
    assert!(
        descriptor.access_strategy().is_some_and(
            |access| matches!(access, ExplainAccessPath::IndexPrefix { name, .. } if *name == "z_tier_score_label_idx")
        ),
        "equality-prefix suffix-order offset roots should expose the chosen order-compatible composite index",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "equality-prefix suffix-order offset roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_some(),
        "equality-prefix suffix-order offset roots should derive Top-N seek for bounded ordered windows",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "equality-prefix suffix-order offset roots should keep access-satisfied ordering",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        )
        .is_none(),
        "equality-prefix suffix-order offset roots should stay off the materialized order fallback lane",
    );
}

#[test]
fn session_explain_execution_equality_prefix_suffix_order_desc_offset_materializes_order_on_chosen_prefix_route()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let descriptor = session
        .load::<SessionDeterministicRangeEntity>()
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "tier",
                CompareOp::Eq,
                Value::Text("gold".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "score",
                CompareOp::Eq,
                Value::Uint(20),
                CoercionId::Strict,
            )),
        ]))
        .order_by_desc("label")
        .order_by_desc("id")
        .offset(1)
        .limit(2)
        .explain_execution()
        .expect(
            "session descending equality-prefix suffix-order offset explain_execution should build",
        );

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexPrefixScan,
        "descending equality-prefix suffix-order offset roots should stay on the chosen index-prefix route",
    );
    assert!(
        descriptor.access_strategy().is_some_and(
            |access| matches!(access, ExplainAccessPath::IndexPrefix { name, .. } if *name == "z_tier_score_label_idx")
        ),
        "descending equality-prefix suffix-order offset roots should expose the chosen order-compatible composite index",
    );
    assert_eq!(
        descriptor.node_properties().get("scan_dir"),
        Some(&Value::Text("desc".to_string())),
        "descending equality-prefix suffix-order offset roots should expose the descending scan direction",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending equality-prefix suffix-order offset roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        )
        .is_some(),
        "descending equality-prefix suffix-order offset roots should fail closed to a materialized order stage on the chosen prefix route",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_none(),
        "descending equality-prefix suffix-order offset roots should stay off the ascending prefix Top-N seek shape",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_none(),
        "descending equality-prefix suffix-order offset roots should not claim access-satisfied ordering once they materialize sort order",
    );
}

#[test]
fn session_execute_unique_prefix_offset_windows_preserve_ordered_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_unique_prefix_offset_session_entities(
        &session,
        &[
            (9_881, "gold", "amber", "A"),
            (9_882, "gold", "bravo", "B"),
            (9_883, "gold", "charlie", "C"),
            (9_884, "gold", "delta", "D"),
            (9_885, "silver", "echo", "E"),
        ],
    );

    let asc = session
        .load::<SessionUniquePrefixOffsetEntity>()
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "tier",
            CompareOp::Eq,
            Value::Text("gold".to_string()),
            CoercionId::Strict,
        )))
        .order_by("handle")
        .order_by("id")
        .limit(2)
        .offset(1)
        .execute()
        .expect("unique-prefix ascending offset window should execute");
    let asc_handles = asc
        .iter()
        .map(|row| row.entity_ref().handle.clone())
        .collect::<Vec<_>>();

    let desc = session
        .load::<SessionUniquePrefixOffsetEntity>()
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "tier",
            CompareOp::Eq,
            Value::Text("gold".to_string()),
            CoercionId::Strict,
        )))
        .order_by_desc("handle")
        .order_by_desc("id")
        .limit(2)
        .offset(1)
        .execute()
        .expect("unique-prefix descending offset window should execute");
    let desc_handles = desc
        .iter()
        .map(|row| row.entity_ref().handle.clone())
        .collect::<Vec<_>>();

    assert_eq!(
        asc_handles,
        vec!["bravo".to_string(), "charlie".to_string()],
        "unique-prefix ascending offset windows should preserve the secondary index order without materialized drift",
    );
    assert_eq!(
        desc_handles,
        vec!["charlie".to_string(), "bravo".to_string()],
        "unique-prefix descending offset windows should preserve the reversed secondary index order without materialized drift",
    );
}

#[test]
fn session_explain_execution_unique_prefix_offset_uses_top_n_seek() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let descriptor = session
        .load::<SessionUniquePrefixOffsetEntity>()
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "tier",
            CompareOp::Eq,
            Value::Text("gold".to_string()),
            CoercionId::Strict,
        )))
        .order_by("handle")
        .order_by("id")
        .limit(2)
        .offset(1)
        .explain_execution()
        .expect("session unique-prefix offset explain_execution should build");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexPrefixScan,
        "unique-prefix offset roots should stay on the chosen index-prefix route",
    );
    assert!(
        descriptor.access_strategy().is_some_and(
            |access| matches!(access, ExplainAccessPath::IndexPrefix { name, .. } if *name == "tier_handle_unique")
        ),
        "unique-prefix offset roots should expose the chosen unique composite index",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "unique-prefix offset roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_some(),
        "unique-prefix offset roots should derive one offset-aware Top-N seek window",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "unique-prefix offset roots should keep access-satisfied ordering",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        )
        .is_none(),
        "unique-prefix offset roots should stay off the materialized order fallback lane",
    );
}

#[test]
fn session_explain_execution_unique_prefix_offset_desc_uses_top_n_seek() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let descriptor = session
        .load::<SessionUniquePrefixOffsetEntity>()
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "tier",
            CompareOp::Eq,
            Value::Text("gold".to_string()),
            CoercionId::Strict,
        )))
        .order_by_desc("handle")
        .order_by_desc("id")
        .limit(2)
        .offset(1)
        .explain_execution()
        .expect("session descending unique-prefix offset explain_execution should build");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexPrefixScan,
        "descending unique-prefix offset roots should stay on the chosen index-prefix route",
    );
    assert!(
        descriptor.access_strategy().is_some_and(
            |access| matches!(access, ExplainAccessPath::IndexPrefix { name, .. } if *name == "tier_handle_unique")
        ),
        "descending unique-prefix offset roots should expose the chosen unique composite index",
    );
    assert_eq!(
        descriptor.node_properties().get("scan_dir"),
        Some(&Value::Text("desc".to_string())),
        "descending unique-prefix offset roots should expose the descending scan direction",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending unique-prefix offset roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_some(),
        "descending unique-prefix offset roots should derive one offset-aware Top-N seek window",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "descending unique-prefix offset roots should keep access-satisfied ordering",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        )
        .is_none(),
        "descending unique-prefix offset roots should stay off the materialized order fallback lane",
    );
}

#[test]
fn session_explain_execution_range_choice_uses_bounded_index_range_hints() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let descriptor = session
        .load::<SessionDeterministicRangeEntity>()
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "tier",
                CompareOp::Eq,
                Value::Text("gold".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "score",
                CompareOp::Gt,
                Value::Uint(10),
                CoercionId::Strict,
            )),
        ]))
        .order_by("score")
        .order_by("label")
        .order_by("id")
        .limit(2)
        .explain_execution()
        .expect("session deterministic range explain_execution should build");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "range-choice roots should stay on the chosen index-range route",
    );
    assert!(
        descriptor
            .access_strategy()
            .is_some_and(
                |access| matches!(access, ExplainAccessPath::IndexRange { name, .. } if *name == "z_tier_score_label_idx")
            ),
        "range-choice roots should expose the chosen order-compatible range index",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "range-choice roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_some(),
        "range-choice roots should derive bounded index-range limit pushdown",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_none(),
        "range-choice roots should stay off the prefix-only Top-N seek shape",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "range-choice roots should keep access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_range_choice_desc_uses_bounded_index_range_hints() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let descriptor = session
        .load::<SessionDeterministicRangeEntity>()
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "tier",
                CompareOp::Eq,
                Value::Text("gold".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "score",
                CompareOp::Gt,
                Value::Uint(10),
                CoercionId::Strict,
            )),
        ]))
        .order_by_desc("score")
        .order_by_desc("label")
        .order_by_desc("id")
        .limit(2)
        .explain_execution()
        .expect("session descending deterministic range explain_execution should build");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "descending range-choice roots should stay on the chosen index-range route",
    );
    assert!(
        descriptor
            .access_strategy()
            .is_some_and(
                |access| matches!(access, ExplainAccessPath::IndexRange { name, .. } if *name == "z_tier_score_label_idx")
            ),
        "descending range-choice roots should expose the chosen order-compatible range index",
    );
    assert_eq!(
        descriptor.node_properties().get("scan_dir"),
        Some(&Value::Text("desc".to_string())),
        "descending range-choice roots should expose the descending scan direction",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending range-choice roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_some(),
        "descending range-choice roots should derive bounded index-range limit pushdown",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_none(),
        "descending range-choice roots should stay off the prefix-only Top-N seek shape",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "descending range-choice roots should keep access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_range_choice_offset_uses_bounded_index_range_hints() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let descriptor = session
        .load::<SessionDeterministicRangeEntity>()
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "tier",
                CompareOp::Eq,
                Value::Text("gold".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "score",
                CompareOp::Gt,
                Value::Uint(10),
                CoercionId::Strict,
            )),
        ]))
        .order_by("score")
        .order_by("label")
        .order_by("id")
        .offset(1)
        .limit(2)
        .explain_execution()
        .expect("session deterministic range offset explain_execution should build");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "range-choice offset roots should stay on the chosen index-range route",
    );
    assert!(
        descriptor
            .access_strategy()
            .is_some_and(
                |access| matches!(access, ExplainAccessPath::IndexRange { name, .. } if *name == "z_tier_score_label_idx")
            ),
        "range-choice offset roots should expose the chosen order-compatible range index",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "range-choice offset roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_some(),
        "range-choice offset roots should derive bounded index-range limit pushdown",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_none(),
        "range-choice offset roots should stay off the prefix-only Top-N seek shape",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "range-choice offset roots should keep access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_range_choice_desc_offset_uses_bounded_index_range_hints() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let descriptor = session
        .load::<SessionDeterministicRangeEntity>()
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "tier",
                CompareOp::Eq,
                Value::Text("gold".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "score",
                CompareOp::Gt,
                Value::Uint(10),
                CoercionId::Strict,
            )),
        ]))
        .order_by_desc("score")
        .order_by_desc("label")
        .order_by_desc("id")
        .offset(1)
        .limit(2)
        .explain_execution()
        .expect("session descending deterministic range offset explain_execution should build");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "descending range-choice offset roots should stay on the chosen index-range route",
    );
    assert!(
        descriptor
            .access_strategy()
            .is_some_and(
                |access| matches!(access, ExplainAccessPath::IndexRange { name, .. } if *name == "z_tier_score_label_idx")
            ),
        "descending range-choice offset roots should expose the chosen order-compatible range index",
    );
    assert_eq!(
        descriptor.node_properties().get("scan_dir"),
        Some(&Value::Text("desc".to_string())),
        "descending range-choice offset roots should expose the descending scan direction",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending range-choice offset roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_some(),
        "descending range-choice offset roots should derive bounded index-range limit pushdown",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_none(),
        "descending range-choice offset roots should stay off the prefix-only Top-N seek shape",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "descending range-choice offset roots should keep access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_composite_order_only_choice_uses_bounded_index_range_hints() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let descriptor = session
        .load::<SessionDeterministicChoiceEntity>()
        .order_by("tier")
        .order_by("handle")
        .order_by("id")
        .limit(2)
        .explain_execution()
        .expect("session deterministic composite order-only explain_execution should build");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "composite order-only roots should stay on the chosen index-range fallback route",
    );
    assert!(
        descriptor
            .access_strategy()
            .is_some_and(
                |access| matches!(access, ExplainAccessPath::IndexRange { name, .. } if *name == "z_tier_handle_idx")
            ),
        "composite order-only roots should expose the chosen order-compatible fallback index",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "composite order-only roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_some(),
        "composite order-only roots should derive bounded index-range limit pushdown",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_some(),
        "composite order-only roots should also derive Top-N seek for bounded ordered windows",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "composite order-only roots should keep access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_order_only_choice_offset_uses_bounded_index_range_hints() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let descriptor = session
        .load::<SessionOrderOnlyChoiceEntity>()
        .order_by("alpha")
        .order_by("id")
        .offset(1)
        .limit(2)
        .explain_execution()
        .expect("session deterministic order-only offset explain_execution should build");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "order-only offset roots should stay on the chosen index-range fallback route",
    );
    assert!(
        descriptor
            .access_strategy()
            .is_some_and(
                |access| matches!(access, ExplainAccessPath::IndexRange { name, .. } if *name == "z_alpha_idx")
            ),
        "order-only offset roots should expose the chosen order-compatible fallback index",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "order-only offset roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_some(),
        "order-only offset roots should derive bounded index-range limit pushdown",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_some(),
        "order-only offset roots should also derive Top-N seek for bounded ordered windows",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "order-only offset roots should keep access-satisfied ordering",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        )
        .is_none(),
        "order-only offset roots should stay off the materialized order fallback lane",
    );
}

#[test]
fn session_execute_order_only_offset_windows_preserve_ordered_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_order_only_choice_session_entities(
        &session,
        &[
            (9_971, "delta", "alpha"),
            (9_972, "alpha", "echo"),
            (9_973, "bravo", "delta"),
            (9_974, "foxtrot", "golf"),
            (9_975, "charlie", "charlie"),
            (9_976, "hotel", "india"),
        ],
    );

    let asc = session
        .load::<SessionOrderOnlyChoiceEntity>()
        .order_by("alpha")
        .order_by("id")
        .offset(1)
        .limit(2)
        .execute()
        .expect("ascending order-only offset window should execute");
    let asc_alpha = asc
        .iter()
        .map(|row| row.entity_ref().alpha.as_str())
        .collect::<Vec<_>>();
    let asc_paged = session
        .load::<SessionOrderOnlyChoiceEntity>()
        .order_by("alpha")
        .order_by("id")
        .offset(1)
        .limit(2)
        .execute_paged()
        .expect("ascending order-only offset paged window should execute");
    let asc_paged_alpha = asc_paged
        .iter()
        .map(|row| row.entity_ref().alpha.as_str())
        .collect::<Vec<_>>();

    let desc = session
        .load::<SessionOrderOnlyChoiceEntity>()
        .order_by_desc("alpha")
        .order_by_desc("id")
        .offset(1)
        .limit(2)
        .execute()
        .expect("descending order-only offset window should execute");
    let desc_alpha = desc
        .iter()
        .map(|row| row.entity_ref().alpha.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        asc_paged_alpha,
        vec!["bravo", "charlie"],
        "order-only paged windows should preserve the same shifted fallback index order",
    );
    assert_eq!(
        asc_alpha,
        vec!["bravo", "charlie"],
        "ascending order-only offset windows should preserve the shifted fallback index order",
    );
    assert_eq!(
        desc_alpha,
        vec!["foxtrot", "delta"],
        "descending order-only offset windows should preserve the reversed shifted fallback index order",
    );
}

#[test]
fn session_explain_execution_order_only_choice_desc_offset_uses_bounded_index_range_hints() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let descriptor = session
        .load::<SessionOrderOnlyChoiceEntity>()
        .order_by_desc("alpha")
        .order_by_desc("id")
        .offset(1)
        .limit(2)
        .explain_execution()
        .expect(
            "session descending deterministic order-only offset explain_execution should build",
        );

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "descending order-only offset roots should stay on the chosen index-range fallback route",
    );
    assert!(
        descriptor
            .access_strategy()
            .is_some_and(
                |access| matches!(access, ExplainAccessPath::IndexRange { name, .. } if *name == "z_alpha_idx")
            ),
        "descending order-only offset roots should expose the chosen order-compatible fallback index",
    );
    assert_eq!(
        descriptor.node_properties().get("scan_dir"),
        Some(&Value::Text("desc".to_string())),
        "descending order-only offset roots should expose the descending scan direction",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending order-only offset roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_some(),
        "descending order-only offset roots should derive bounded index-range limit pushdown",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_some(),
        "descending order-only offset roots should also derive Top-N seek for bounded ordered windows",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "descending order-only offset roots should keep access-satisfied ordering",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        )
        .is_none(),
        "descending order-only offset roots should stay off the materialized order fallback lane",
    );
}

#[test]
fn session_explain_execution_composite_order_only_choice_desc_uses_bounded_index_range_hints() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let descriptor = session
        .load::<SessionDeterministicChoiceEntity>()
        .order_by_desc("tier")
        .order_by_desc("handle")
        .order_by_desc("id")
        .limit(2)
        .explain_execution()
        .expect(
            "session descending deterministic composite order-only explain_execution should build",
        );

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "descending composite order-only roots should stay on the chosen index-range fallback route",
    );
    assert!(
        descriptor
            .access_strategy()
            .is_some_and(
                |access| matches!(access, ExplainAccessPath::IndexRange { name, .. } if *name == "z_tier_handle_idx")
            ),
        "descending composite order-only roots should expose the chosen order-compatible fallback index",
    );
    assert_eq!(
        descriptor.node_properties().get("scan_dir"),
        Some(&Value::Text("desc".to_string())),
        "descending composite order-only roots should expose the descending scan direction",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending composite order-only roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_some(),
        "descending composite order-only roots should derive bounded index-range limit pushdown",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_some(),
        "descending composite order-only roots should also derive Top-N seek for bounded ordered windows",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "descending composite order-only roots should keep access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_composite_order_only_choice_offset_uses_bounded_index_range_hints() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let descriptor = session
        .load::<SessionDeterministicChoiceEntity>()
        .order_by("tier")
        .order_by("handle")
        .order_by("id")
        .offset(1)
        .limit(2)
        .explain_execution()
        .expect("session deterministic composite order-only offset explain_execution should build");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "composite order-only offset roots should stay on the chosen index-range fallback route",
    );
    assert!(
        descriptor
            .access_strategy()
            .is_some_and(
                |access| matches!(access, ExplainAccessPath::IndexRange { name, .. } if *name == "z_tier_handle_idx")
            ),
        "composite order-only offset roots should expose the chosen order-compatible fallback index",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "composite order-only offset roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_some(),
        "composite order-only offset roots should derive bounded index-range limit pushdown",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_some(),
        "composite order-only offset roots should also derive Top-N seek for bounded ordered windows",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "composite order-only offset roots should keep access-satisfied ordering",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        )
        .is_none(),
        "composite order-only offset roots should stay off the materialized order fallback lane",
    );
}

#[test]
fn session_execute_composite_order_only_offset_windows_preserve_ordered_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one minimal composite order-only dataset so the offset
    // window must skip the first ordered row instead of merely truncating.
    for (id, tier, handle, label) in [
        (9_981_u128, "gold", "bravo", "amber"),
        (9_982_u128, "gold", "charlie", "bravo"),
        (9_983_u128, "silver", "delta", "delta"),
    ] {
        session
            .insert(SessionDeterministicChoiceEntity {
                id: Ulid::from_u128(id),
                tier: tier.to_string(),
                handle: handle.to_string(),
                label: label.to_string(),
            })
            .expect("composite order-only offset seed insert should succeed");
    }

    // Phase 2: assert the compiled query still carries the logical offset so
    // the runtime check isolates window application, not planning.
    let planned = session
        .load::<SessionDeterministicChoiceEntity>()
        .order_by("tier")
        .order_by("handle")
        .order_by("id")
        .offset(1)
        .limit(2)
        .planned()
        .expect("composite order-only offset plan should build");
    assert_eq!(
        planned.explain().page(),
        &crate::db::query::explain::ExplainPagination::Page {
            limit: Some(2),
            offset: 1,
        },
        "composite order-only offset plans must preserve the logical offset at the planner boundary",
    );

    // Phase 3: execute the public entity surface and lock the shifted ordered
    // window directly.
    let response = session
        .load::<SessionDeterministicChoiceEntity>()
        .order_by("tier")
        .order_by("handle")
        .order_by("id")
        .offset(1)
        .limit(2)
        .execute()
        .expect("composite order-only offset window should execute");
    let handles = response
        .iter()
        .map(|row| row.entity_ref().handle.as_str())
        .collect::<Vec<_>>();
    let paged = session
        .load::<SessionDeterministicChoiceEntity>()
        .order_by("tier")
        .order_by("handle")
        .order_by("id")
        .offset(1)
        .limit(2)
        .execute_paged()
        .expect("composite order-only offset paged window should execute");
    let paged_handles = paged
        .iter()
        .map(|row| row.entity_ref().handle.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        paged_handles,
        vec!["charlie", "delta"],
        "composite order-only paged windows should preserve the same shifted index order",
    );
    assert_eq!(
        handles,
        vec!["charlie", "delta"],
        "composite order-only offset windows should preserve the shifted index order on the public entity surface",
    );
}

#[test]
fn session_explain_execution_composite_order_only_choice_desc_offset_uses_bounded_index_range_hints()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    let descriptor = session
        .load::<SessionDeterministicChoiceEntity>()
        .order_by_desc("tier")
        .order_by_desc("handle")
        .order_by_desc("id")
        .offset(1)
        .limit(2)
        .explain_execution()
        .expect(
            "session descending deterministic composite order-only offset explain_execution should build",
        );

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "descending composite order-only offset roots should stay on the chosen index-range fallback route",
    );
    assert!(
        descriptor
            .access_strategy()
            .is_some_and(
                |access| matches!(access, ExplainAccessPath::IndexRange { name, .. } if *name == "z_tier_handle_idx")
            ),
        "descending composite order-only offset roots should expose the chosen order-compatible fallback index",
    );
    assert_eq!(
        descriptor.node_properties().get("scan_dir"),
        Some(&Value::Text("desc".to_string())),
        "descending composite order-only offset roots should expose the descending scan direction",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending composite order-only offset roots should expose secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_some(),
        "descending composite order-only offset roots should derive bounded index-range limit pushdown",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_some(),
        "descending composite order-only offset roots should also derive Top-N seek for bounded ordered windows",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "descending composite order-only offset roots should keep access-satisfied ordering",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        )
        .is_none(),
        "descending composite order-only offset roots should stay off the materialized order fallback lane",
    );
}

fn unsupported_sql_dispatch_query_error(message: &'static str) -> QueryError {
    QueryError::execute(crate::error::InternalError::classified(
        ErrorClass::Unsupported,
        ErrorOrigin::Query,
        message,
    ))
}

fn dispatch_projection_columns<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> Result<Vec<String>, QueryError>
where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue + crate::traits::EntityKind,
{
    match session.execute_sql_dispatch::<E>(sql)? {
        SqlDispatchResult::Projection { columns, .. }
        | SqlDispatchResult::ProjectionText { columns, .. }
        | SqlDispatchResult::Grouped { columns, .. } => Ok(columns),
        SqlDispatchResult::Explain(_)
        | SqlDispatchResult::Describe(_)
        | SqlDispatchResult::ShowIndexes(_)
        | SqlDispatchResult::ShowColumns(_)
        | SqlDispatchResult::ShowEntities(_) => Err(unsupported_sql_dispatch_query_error(
            "projection column dispatch only supports row-producing SELECT or DELETE statements",
        )),
    }
}

fn dispatch_projection_rows<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> Result<Vec<Vec<Value>>, QueryError>
where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue,
{
    match session.execute_sql_dispatch::<E>(sql)? {
        SqlDispatchResult::Projection { rows, .. } => Ok(rows),
        SqlDispatchResult::ProjectionText { .. } | SqlDispatchResult::Grouped { .. } => {
            Err(unsupported_sql_dispatch_query_error(
                "projection row dispatch only supports value-row SQL projection payloads",
            ))
        }
        SqlDispatchResult::Explain(_)
        | SqlDispatchResult::Describe(_)
        | SqlDispatchResult::ShowIndexes(_)
        | SqlDispatchResult::ShowColumns(_)
        | SqlDispatchResult::ShowEntities(_) => Err(unsupported_sql_dispatch_query_error(
            "projection row dispatch only supports row-producing SELECT or DELETE statements",
        )),
    }
}

fn dispatch_explain_sql<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> Result<String, QueryError>
where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue,
{
    match session.execute_sql_dispatch::<E>(sql)? {
        SqlDispatchResult::Explain(explain) => Ok(explain),
        SqlDispatchResult::Projection { .. }
        | SqlDispatchResult::ProjectionText { .. }
        | SqlDispatchResult::Grouped { .. }
        | SqlDispatchResult::Describe(_)
        | SqlDispatchResult::ShowIndexes(_)
        | SqlDispatchResult::ShowColumns(_)
        | SqlDispatchResult::ShowEntities(_) => Err(unsupported_sql_dispatch_query_error(
            "EXPLAIN dispatch requires an EXPLAIN statement",
        )),
    }
}

// Parse one verbose explain payload into `diag.*` key/value pairs so session
// tests can assert planner-choice diagnostics without snapshotting the full
// rendered tree.
fn session_verbose_diagnostics_map(verbose: &str) -> BTreeMap<String, String> {
    let mut diagnostics = BTreeMap::new();
    for line in verbose.lines() {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        if !key.starts_with("diag.") {
            continue;
        }
        diagnostics.insert(key.to_string(), value.to_string());
    }

    diagnostics
}

fn dispatch_describe_sql<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> Result<EntitySchemaDescription, QueryError>
where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue,
{
    match session.execute_sql_dispatch::<E>(sql)? {
        SqlDispatchResult::Describe(description) => Ok(description),
        SqlDispatchResult::Projection { .. }
        | SqlDispatchResult::ProjectionText { .. }
        | SqlDispatchResult::Grouped { .. }
        | SqlDispatchResult::Explain(_)
        | SqlDispatchResult::ShowIndexes(_)
        | SqlDispatchResult::ShowColumns(_)
        | SqlDispatchResult::ShowEntities(_) => Err(unsupported_sql_dispatch_query_error(
            "DESCRIBE dispatch requires a DESCRIBE statement",
        )),
    }
}

fn dispatch_show_indexes_sql<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> Result<Vec<String>, QueryError>
where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue,
{
    match session.execute_sql_dispatch::<E>(sql)? {
        SqlDispatchResult::ShowIndexes(indexes) => Ok(indexes),
        SqlDispatchResult::Projection { .. }
        | SqlDispatchResult::ProjectionText { .. }
        | SqlDispatchResult::Grouped { .. }
        | SqlDispatchResult::Explain(_)
        | SqlDispatchResult::Describe(_)
        | SqlDispatchResult::ShowColumns(_)
        | SqlDispatchResult::ShowEntities(_) => Err(unsupported_sql_dispatch_query_error(
            "SHOW INDEXES dispatch requires a SHOW INDEXES statement",
        )),
    }
}

fn dispatch_show_columns_sql<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> Result<Vec<EntityFieldDescription>, QueryError>
where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue,
{
    match session.execute_sql_dispatch::<E>(sql)? {
        SqlDispatchResult::ShowColumns(columns) => Ok(columns),
        SqlDispatchResult::Projection { .. }
        | SqlDispatchResult::ProjectionText { .. }
        | SqlDispatchResult::Grouped { .. }
        | SqlDispatchResult::Explain(_)
        | SqlDispatchResult::Describe(_)
        | SqlDispatchResult::ShowIndexes(_)
        | SqlDispatchResult::ShowEntities(_) => Err(unsupported_sql_dispatch_query_error(
            "SHOW COLUMNS dispatch requires a SHOW COLUMNS statement",
        )),
    }
}

fn dispatch_show_entities_sql(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> Result<Vec<String>, QueryError> {
    let route = session.sql_statement_route(sql)?;
    if !route.is_show_entities() {
        return Err(unsupported_sql_dispatch_query_error(
            "SHOW ENTITIES dispatch requires a SHOW ENTITIES statement",
        ));
    }

    Ok(session.show_entities())
}

// Seed one deterministic SQL fixture dataset used by matrix tests.
fn seed_session_sql_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(&'static str, u64)],
) {
    for (name, age) in rows {
        session
            .insert(SessionSqlEntity {
                id: Ulid::generate(),
                name: (*name).to_string(),
                age: *age,
            })
            .expect("seed insert should succeed");
    }
}

// Seed one deterministic indexed SQL fixture dataset used by text-prefix tests.
fn seed_indexed_session_sql_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(&'static str, u64)],
) {
    for (name, age) in rows {
        session
            .insert(IndexedSessionSqlEntity {
                id: Ulid::generate(),
                name: (*name).to_string(),
                age: *age,
            })
            .expect("indexed seed insert should succeed");
    }
}

// Seed one deterministic unique-prefix dataset used by offset-aware ordered
// secondary-prefix session tests.
fn seed_unique_prefix_offset_session_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(u128, &'static str, &'static str, &'static str)],
) {
    for (id, tier, handle, note) in rows.iter().copied() {
        session
            .insert(SessionUniquePrefixOffsetEntity {
                id: Ulid::from_u128(id),
                tier: tier.to_string(),
                handle: handle.to_string(),
                note: note.to_string(),
            })
            .expect("unique-prefix offset seed insert should succeed");
    }
}

// Seed one deterministic single-field order-only dataset used by offset-aware
// fallback index-order session tests.
fn seed_order_only_choice_session_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(u128, &'static str, &'static str)],
) {
    for (id, alpha, beta) in rows.iter().copied() {
        session
            .insert(SessionOrderOnlyChoiceEntity {
                id: Ulid::from_u128(id),
                alpha: alpha.to_string(),
                beta: beta.to_string(),
            })
            .expect("order-only choice seed insert should succeed");
    }
}

// Seed one deterministic filtered-indexed SQL fixture dataset used by guarded
// order-only fallback tests.
fn seed_filtered_indexed_session_sql_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(u128, &'static str, bool, u64)],
) {
    for (id, name, active, age) in rows.iter().copied() {
        session
            .insert(FilteredIndexedSessionSqlEntity {
                id: Ulid::from_u128(id),
                name: name.to_string(),
                active,
                tier: "standard".to_string(),
                handle: format!("handle-{name}"),
                age,
            })
            .expect("filtered indexed seed insert should succeed");
    }
}

// Seed one deterministic filtered composite-indexed SQL fixture dataset used by
// equality-prefix plus bounded-text window tests.
fn seed_filtered_composite_indexed_session_sql_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(u128, &'static str, bool, &'static str, &'static str, u64)],
) {
    for (id, name, active, tier, handle, age) in rows.iter().copied() {
        session
            .insert(FilteredIndexedSessionSqlEntity {
                id: Ulid::from_u128(id),
                name: name.to_string(),
                active,
                tier: tier.to_string(),
                handle: handle.to_string(),
                age,
            })
            .expect("filtered composite indexed seed insert should succeed");
    }
}

// Seed the canonical mixed-case filtered expression fixture used by the
// guarded `LOWER(handle)` query tests.
fn seed_filtered_expression_indexed_session_sql_entities(session: &DbSession<SessionSqlCanister>) {
    seed_filtered_composite_indexed_session_sql_entities(
        session,
        &FILTERED_EXPRESSION_SESSION_SQL_ROWS,
    );
}

// Inspect the raw index-range scan chosen by the filtered expression order-only
// SQL route so tests can assert both index isolation and scan order directly.
fn inspect_filtered_expression_order_only_raw_scan(
    session: &DbSession<SessionSqlCanister>,
) -> (Vec<(StorageKey, Vec<StorageKey>)>, Vec<Ulid>) {
    let plan = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT id, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect("filtered expression-order SQL query should lower")
        .plan()
        .expect("filtered expression-order SQL query should plan")
        .into_inner();
    let lowered_specs =
        lower_index_range_specs(FilteredIndexedSessionSqlEntity::ENTITY_TAG, &plan.access)
            .expect("filtered expression-order access plan should lower to one raw index range");
    let [spec] = lowered_specs.as_slice() else {
        panic!("filtered expression-order access plan should use exactly one index-range spec");
    };
    let store = INDEXED_SESSION_SQL_DB
        .recovered_store(IndexedSessionSqlStore::PATH)
        .expect("filtered expression indexed store should recover");

    // Inspect stored entries that fall inside the lowered raw envelope.
    let entries_in_range = store.with_index(|index_store| {
        index_store
            .entries()
            .into_iter()
            .filter(|(raw_key, _)| key_within_envelope(raw_key, spec.lower(), spec.upper()))
            .map(|(raw_key, raw_entry)| {
                let decoded_key =
                    IndexKey::try_from_raw(&raw_key).expect("filtered expression test key");
                let decoded_ids = raw_entry
                    .decode_keys()
                    .expect("filtered expression test entry")
                    .into_iter()
                    .collect::<Vec<_>>();

                (
                    decoded_key
                        .primary_storage_key()
                        .expect("primary storage key"),
                    decoded_ids,
                )
            })
            .collect::<Vec<_>>()
    });

    // Then inspect the actual scan order produced by the shared raw range resolver.
    let keys = store
        .with_index(|index_store| {
            index_store.resolve_data_values_in_raw_range_limited(
                FilteredIndexedSessionSqlEntity::ENTITY_TAG,
                spec.index(),
                (spec.lower(), spec.upper()),
                IndexScanContinuationInput::new(None, Direction::Asc),
                4,
                None,
            )
        })
        .expect("filtered expression index range scan should succeed");
    let scanned_ids = keys
        .into_iter()
        .map(|key: DataKey| match key.storage_key() {
            StorageKey::Ulid(id) => id,
            other => panic!(
                "filtered expression fixture keys should stay on ULID primary keys: {other:?}"
            ),
        })
        .collect::<Vec<_>>();

    (entries_in_range, scanned_ids)
}

// Seed one deterministic aggregate fixture dataset used by revived session aggregate tests.
fn seed_session_aggregate_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(u128, u64, u64)],
) {
    for (id, group, rank) in rows {
        session
            .insert(SessionAggregateEntity {
                id: Ulid::from_u128(*id),
                group: *group,
                rank: *rank,
                label: format!("group-{group}-rank-{rank}"),
            })
            .expect("aggregate seed insert should succeed");
    }
}

fn seed_session_explain_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(u128, u64, u64)],
) {
    for (id, group, rank) in rows.iter().copied() {
        session
            .insert(SessionExplainEntity {
                id: Ulid::from_u128(id),
                group,
                rank,
                label: format!("g{group}-r{rank}"),
            })
            .expect("session explain fixture insert should succeed");
    }
}

fn seed_composite_indexed_session_sql_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(u128, &str, u64)],
) {
    for (id, code, serial) in rows.iter().copied() {
        session
            .insert(CompositeIndexedSessionSqlEntity {
                id: Ulid::from_u128(id),
                code: code.to_string(),
                serial,
                note: format!("note-{code}-{serial}"),
            })
            .expect("composite indexed SQL fixture insert should succeed");
    }
}

fn seed_expression_indexed_session_sql_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(u128, &str, u64)],
) {
    for (id, name, age) in rows.iter().copied() {
        session
            .insert(ExpressionIndexedSessionSqlEntity {
                id: Ulid::from_u128(id),
                name: name.to_string(),
                age,
            })
            .expect("expression indexed SQL fixture insert should succeed");
    }
}

fn seed_session_temporal_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(u128, Date, Timestamp, Duration)],
) {
    for (id, occurred_on, occurred_at, elapsed) in rows.iter().copied() {
        session
            .insert(SessionTemporalEntity {
                id: Ulid::from_u128(id),
                occurred_on,
                occurred_at,
                elapsed,
            })
            .expect("session temporal fixture insert should succeed");
    }
}

fn session_aggregate_group_predicate(group: u64) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        "group",
        CompareOp::Eq,
        Value::Uint(group),
        CoercionId::Strict,
    ))
}

fn session_aggregate_values_by_rank(
    response: &EntityResponse<SessionAggregateEntity>,
) -> Vec<Value> {
    response
        .iter()
        .map(|row| Value::Uint(row.entity_ref().rank))
        .collect()
}

fn session_aggregate_values_by_rank_with_ids(
    response: &EntityResponse<SessionAggregateEntity>,
) -> Vec<(Ulid, Value)> {
    response
        .iter()
        .map(|row| (row.id().key(), Value::Uint(row.entity_ref().rank)))
        .collect()
}

fn session_aggregate_first_value_by_rank(
    response: &EntityResponse<SessionAggregateEntity>,
) -> Option<Value> {
    response
        .iter()
        .next()
        .map(|row| Value::Uint(row.entity_ref().rank))
}

fn session_aggregate_last_value_by_rank(
    response: &EntityResponse<SessionAggregateEntity>,
) -> Option<Value> {
    response
        .iter()
        .last()
        .map(|row| Value::Uint(row.entity_ref().rank))
}

fn session_aggregate_ids(response: &EntityResponse<SessionAggregateEntity>) -> Vec<Ulid> {
    response.iter().map(|row| row.id().key()).collect()
}

// Keep aggregate terminal explain comparisons stable without relying on one
// derived `Debug` surface that may reorder fields across formatter changes.
fn session_aggregate_terminal_plan_snapshot(
    plan: &crate::db::ExplainAggregateTerminalPlan,
) -> String {
    let execution = plan.execution();
    let node = plan.execution_node_descriptor();
    let descriptor_json = node.render_json_canonical();

    format!(
        concat!(
            "terminal={:?}\n",
            "query_access={:?}\n",
            "query_order_by={:?}\n",
            "query_page={:?}\n",
            "query_grouping={:?}\n",
            "query_pushdown={:?}\n",
            "query_consistency={:?}\n",
            "execution_aggregation={:?}\n",
            "execution_mode={:?}\n",
            "execution_ordering_source={:?}\n",
            "execution_limit={:?}\n",
            "execution_cursor={}\n",
            "execution_covering_projection={}\n",
            "execution_node_properties={:?}\n",
            "execution_node_json={}",
        ),
        plan.terminal(),
        plan.query().access(),
        plan.query().order_by(),
        plan.query().page(),
        plan.query().grouping(),
        plan.query().order_pushdown(),
        plan.query().consistency(),
        execution.aggregation(),
        execution.execution_mode(),
        execution.ordering_source(),
        execution.limit(),
        execution.cursor(),
        execution.covering_projection(),
        execution.node_properties(),
        descriptor_json,
    )
}

// Recursively search the execution descriptor tree for one node type.
fn explain_execution_contains_node_type(
    descriptor: &ExplainExecutionNodeDescriptor,
    node_type: ExplainExecutionNodeType,
) -> bool {
    if descriptor.node_type() == node_type {
        return true;
    }

    descriptor
        .children()
        .iter()
        .any(|child| explain_execution_contains_node_type(child, node_type))
}

// Walk the execution descriptor tree in pre-order and return the first matching node.
fn explain_execution_find_first_node(
    descriptor: &ExplainExecutionNodeDescriptor,
    node_type: ExplainExecutionNodeType,
) -> Option<&ExplainExecutionNodeDescriptor> {
    if descriptor.node_type() == node_type {
        return Some(descriptor);
    }

    for child in descriptor.children() {
        if let Some(found) = explain_execution_find_first_node(child, node_type) {
            return Some(found);
        }
    }

    None
}

// Build one store-backed execution descriptor json payload for reduced SQL so
// tests can lock the structured execution-explain surface separately from the
// text EXPLAIN EXECUTION renderer.
fn store_backed_execution_descriptor_json_for_sql<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> String
where
    E: PersistedRow<Canister = SessionSqlCanister>
        + EntityValue
        + crate::traits::EntityKind<Canister = SessionSqlCanister>,
{
    let parsed = session
        .parse_sql_statement(sql)
        .expect("store-backed execution descriptor sql should parse");
    let lowered = parsed
        .lower_query_lane_for_entity(E::MODEL.name(), E::MODEL.primary_key.name)
        .expect("store-backed execution descriptor sql should lower");
    let LoweredSqlQuery::Select(select) = lowered
        .query()
        .cloned()
        .expect("store-backed execution descriptor should lower one query shape")
    else {
        panic!("store-backed execution descriptor helper only supports SELECT");
    };
    let structural = apply_lowered_select_shape(
        StructuralQuery::new(E::MODEL, MissingRowPolicy::Ignore),
        select,
    )
    .expect("store-backed execution descriptor structural query should bind");
    let plan = structural
        .build_plan()
        .expect("store-backed execution descriptor plan should build");
    let descriptor = assemble_load_execution_node_descriptor(
        E::MODEL.fields(),
        E::MODEL.primary_key().name(),
        &plan,
    )
    .expect("store-backed execution descriptor should assemble");

    descriptor.render_json_canonical()
}

#[derive(Default)]
struct SessionMetricsCaptureSink {
    events: RefCell<Vec<MetricsEvent>>,
}

impl SessionMetricsCaptureSink {
    fn into_events(self) -> Vec<MetricsEvent> {
        self.events.into_inner()
    }
}

impl MetricsSink for SessionMetricsCaptureSink {
    fn record(&self, event: MetricsEvent) {
        self.events.borrow_mut().push(event);
    }
}

fn rows_scanned_for_entity(events: &[MetricsEvent], entity_path: &'static str) -> usize {
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

fn capture_rows_scanned_for_entity<R>(
    entity_path: &'static str,
    run: impl FnOnce() -> R,
) -> (R, usize) {
    let sink = SessionMetricsCaptureSink::default();
    let output = with_metrics_sink(&sink, run);
    let rows_scanned = rows_scanned_for_entity(&sink.into_events(), entity_path);

    (output, rows_scanned)
}

fn session_aggregate_raw_row(id: Ulid) -> crate::db::data::RawRow {
    let raw_key = DataKey::try_new::<SessionAggregateEntity>(id)
        .expect("session aggregate data key should build")
        .to_raw()
        .expect("session aggregate data key should encode");

    SESSION_SQL_DATA_STORE.with(|store| {
        store
            .borrow()
            .get(&raw_key)
            .expect("session aggregate row should exist")
    })
}

fn session_aggregate_persisted_payload_bytes_for_ids(ids: Vec<Ulid>) -> u64 {
    ids.into_iter().fold(0u64, |acc, id| {
        acc.saturating_add(u64::try_from(session_aggregate_raw_row(id).len()).unwrap_or(u64::MAX))
    })
}

fn session_aggregate_serialized_field_payload_bytes_for_rows(
    response: &EntityResponse<SessionAggregateEntity>,
    field: &str,
) -> u64 {
    response.iter().fold(0u64, |acc, row| {
        let value = match field {
            "group" => Value::Uint(row.entity_ref().group),
            "rank" => Value::Uint(row.entity_ref().rank),
            "label" => Value::Text(row.entity_ref().label.clone()),
            other => panic!("session aggregate field should resolve: {other}"),
        };
        let value_len =
            serialized_len(&value).expect("session aggregate field value should encode");

        acc.saturating_add(u64::try_from(value_len).unwrap_or(u64::MAX))
    })
}

fn session_aggregate_expected_nth_by_rank_id(
    response: &EntityResponse<SessionAggregateEntity>,
    ordinal: usize,
) -> Option<Ulid> {
    let mut ordered = response
        .iter()
        .map(|row| (row.entity_ref().rank, row.id().key()))
        .collect::<Vec<_>>();
    ordered.sort_unstable_by(|(left_rank, left_id), (right_rank, right_id)| {
        left_rank
            .cmp(right_rank)
            .then_with(|| left_id.cmp(right_id))
    });

    ordered.into_iter().nth(ordinal).map(|(_, id)| id)
}

fn session_aggregate_expected_median_by_rank_id(
    response: &EntityResponse<SessionAggregateEntity>,
) -> Option<Ulid> {
    let mut ordered = response
        .iter()
        .map(|row| (row.entity_ref().rank, row.id().key()))
        .collect::<Vec<_>>();
    ordered.sort_unstable_by(|(left_rank, left_id), (right_rank, right_id)| {
        left_rank
            .cmp(right_rank)
            .then_with(|| left_id.cmp(right_id))
    });
    let median_index = if ordered.len() % 2 == 0 {
        ordered.len().saturating_div(2).saturating_sub(1)
    } else {
        ordered.len().saturating_div(2)
    };

    ordered.into_iter().nth(median_index).map(|(_, id)| id)
}

fn session_aggregate_expected_count_distinct_by_rank(
    response: &EntityResponse<SessionAggregateEntity>,
) -> u32 {
    u32::try_from(
        response
            .iter()
            .map(|row| row.entity_ref().rank)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
    )
    .expect("session aggregate distinct rank cardinality should fit in u32")
}

fn session_aggregate_expected_min_max_by_rank_ids(
    response: &EntityResponse<SessionAggregateEntity>,
) -> Option<(Ulid, Ulid)> {
    let mut ordered = response
        .iter()
        .map(|row| (row.entity_ref().rank, row.id().key()))
        .collect::<Vec<_>>();
    ordered.sort_unstable_by(|(left_rank, left_id), (right_rank, right_id)| {
        left_rank
            .cmp(right_rank)
            .then_with(|| left_id.cmp(right_id))
    });

    ordered
        .first()
        .zip(ordered.last())
        .map(|((_, min_id), (_, max_id))| (*min_id, *max_id))
}

///
/// SessionAggregateProjectionTerminal
///
/// Selects one session aggregate projection terminal for execute-parity tests.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SessionAggregateProjectionTerminal {
    ValuesBy,
    ValuesByWithIds,
    DistinctValuesBy,
}

///
/// SessionAggregateRankTerminal
///
/// Selects top-vs-bottom ranked row orientation for session aggregate tests.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SessionAggregateRankTerminal {
    Top,
    Bottom,
}

///
/// SessionAggregateRankOutput
///
/// Selects one ranked terminal projection shape for parity checks.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SessionAggregateRankOutput {
    Values,
    ValuesWithIds,
}

///
/// SessionAggregateResult
///
/// Small session-local result carrier used to compare aggregate terminal forms
/// without depending on the old executor aggregate harness types.
///

#[derive(Clone, Debug, Eq, PartialEq)]
enum SessionAggregateResult {
    Ids(Vec<Ulid>),
    Values(Vec<Value>),
    ValuesWithIds(Vec<(Ulid, Value)>),
}

fn run_session_aggregate_projection_terminal(
    session: &DbSession<SessionSqlCanister>,
    terminal: SessionAggregateProjectionTerminal,
) -> Result<SessionAggregateResult, QueryError> {
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by_desc("id")
            .offset(1)
            .limit(4)
    };

    match terminal {
        SessionAggregateProjectionTerminal::ValuesBy => Ok(SessionAggregateResult::Values(
            load_window().values_by("rank")?,
        )),
        SessionAggregateProjectionTerminal::ValuesByWithIds => {
            Ok(SessionAggregateResult::ValuesWithIds(
                load_window()
                    .values_by_with_ids("rank")?
                    .into_iter()
                    .map(|(id, value)| (id.key(), value))
                    .collect(),
            ))
        }
        SessionAggregateProjectionTerminal::DistinctValuesBy => Ok(SessionAggregateResult::Values(
            load_window().distinct_values_by("rank")?,
        )),
    }
}

fn run_session_aggregate_rank_terminal(
    session: &DbSession<SessionSqlCanister>,
    terminal: SessionAggregateRankTerminal,
    output: SessionAggregateRankOutput,
) -> Result<SessionAggregateResult, QueryError> {
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by_desc("id")
            .offset(0)
            .limit(5)
    };

    match (terminal, output) {
        (SessionAggregateRankTerminal::Top, SessionAggregateRankOutput::Values) => Ok(
            SessionAggregateResult::Values(load_window().top_k_by_values("rank", 3)?),
        ),
        (SessionAggregateRankTerminal::Bottom, SessionAggregateRankOutput::Values) => Ok(
            SessionAggregateResult::Values(load_window().bottom_k_by_values("rank", 3)?),
        ),
        (SessionAggregateRankTerminal::Top, SessionAggregateRankOutput::ValuesWithIds) => {
            Ok(SessionAggregateResult::ValuesWithIds(
                load_window()
                    .top_k_by_with_ids("rank", 3)?
                    .into_iter()
                    .map(|(id, value)| (id.key(), value))
                    .collect(),
            ))
        }
        (SessionAggregateRankTerminal::Bottom, SessionAggregateRankOutput::ValuesWithIds) => {
            Ok(SessionAggregateResult::ValuesWithIds(
                load_window()
                    .bottom_k_by_with_ids("rank", 3)?
                    .into_iter()
                    .map(|(id, value)| (id.key(), value))
                    .collect(),
            ))
        }
    }
}

// Execute one scalar SQL query and return `(name, age)` tuples in response order.
fn execute_sql_name_age_rows(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> Vec<(String, u64)> {
    session
        .execute_sql::<SessionSqlEntity>(sql)
        .expect("scalar SQL execution should succeed")
        .iter()
        .map(|row| (row.entity_ref().name.clone(), row.entity_ref().age))
        .collect()
}

// Assert one explain payload contains every required token for one case.
fn assert_explain_contains_tokens(explain: &str, tokens: &[&str], context: &str) {
    for token in tokens {
        assert!(
            explain.contains(token),
            "explain matrix case missing token `{token}`: {context}",
        );
    }
}

// Assert query-surface cursor errors remain wrapped under QueryError::Plan(PlanError::Cursor).
fn assert_query_error_is_cursor_plan(
    err: QueryError,
    predicate: impl FnOnce(&CursorPlanError) -> bool,
) {
    assert!(matches!(
        err,
        QueryError::Plan(plan_err)
            if matches!(
                plan_err.as_ref(),
                PlanError::Cursor(inner) if predicate(inner.as_ref())
            )
    ));
}

// Assert both session conversion paths preserve the same cursor-plan variant payload.
fn assert_cursor_mapping_parity(
    build: impl Fn() -> CursorPlanError,
    predicate: impl Fn(&CursorPlanError) -> bool + Copy,
) {
    let mapped_via_executor =
        QueryError::from_executor_plan_error(ExecutorPlanError::from(build()));
    assert_query_error_is_cursor_plan(mapped_via_executor, predicate);

    let mapped_via_plan = QueryError::from(PlanError::from(build()));
    assert_query_error_is_cursor_plan(mapped_via_plan, predicate);
}

// Assert SQL parser unsupported-feature labels remain preserved through
// query-facing execution error detail payloads.
fn assert_sql_unsupported_feature_detail(err: QueryError, expected_feature: &'static str) {
    let QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(internal)) =
        err
    else {
        panic!("expected query execution unsupported error variant");
    };

    assert_eq!(internal.class(), ErrorClass::Unsupported);
    assert_eq!(internal.origin(), ErrorOrigin::Query);
    assert!(
        matches!(
            internal.detail(),
            Some(ErrorDetail::Query(QueryErrorDetail::UnsupportedSqlFeature { feature }))
                if *feature == expected_feature
        ),
        "unsupported SQL feature detail label should be preserved",
    );
}

// Assert one SQL surface result fails with the unsupported execution boundary.
fn assert_unsupported_sql_surface_result<T>(result: Result<T, QueryError>, context: &str) {
    let Err(err) = result else {
        panic!("{context}");
    };
    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "unsupported SQL surface case should map to unsupported execution class: {context}",
    );
}

const fn unsupported_sql_feature_cases() -> [(&'static str, &'static str); 7] {
    [
        (
            "SELECT * FROM SessionSqlEntity JOIN other ON SessionSqlEntity.id = other.id",
            "JOIN",
        ),
        (
            "SELECT \"name\" FROM SessionSqlEntity",
            "quoted identifiers",
        ),
        ("SELECT * FROM SessionSqlEntity alias", "table aliases"),
        (
            "SELECT * FROM SessionSqlEntity WHERE name LIKE '%Al'",
            "LIKE patterns beyond trailing '%' prefix form",
        ),
        (
            "SELECT * FROM SessionSqlEntity WHERE LOWER(name) LIKE '%Al'",
            "LIKE patterns beyond trailing '%' prefix form",
        ),
        (
            "SELECT * FROM SessionSqlEntity WHERE UPPER(name) LIKE '%Al'",
            "LIKE patterns beyond trailing '%' prefix form",
        ),
        (
            "SELECT * FROM SessionSqlEntity WHERE STARTS_WITH(TRIM(name), 'Al')",
            "STARTS_WITH first argument forms beyond plain or LOWER/UPPER field wrappers",
        ),
    ]
}

#[test]
fn session_cursor_error_mapping_parity_boundary_arity() {
    assert_cursor_mapping_parity(
        || CursorPlanError::continuation_cursor_boundary_arity_mismatch(2, 1),
        |inner| {
            matches!(
                inner,
                CursorPlanError::ContinuationCursorBoundaryArityMismatch {
                    expected: 2,
                    found: 1
                }
            )
        },
    );
}

#[test]
fn session_cursor_error_mapping_parity_window_mismatch() {
    assert_cursor_mapping_parity(
        || CursorPlanError::continuation_cursor_window_mismatch(8, 3),
        |inner| {
            matches!(
                inner,
                CursorPlanError::ContinuationCursorWindowMismatch {
                    expected_offset: 8,
                    actual_offset: 3
                }
            )
        },
    );
}

#[test]
fn session_cursor_error_mapping_parity_decode_reason() {
    assert_cursor_mapping_parity(
        || {
            CursorPlanError::invalid_continuation_cursor(
                crate::db::codec::cursor::CursorDecodeError::OddLength,
            )
        },
        |inner| {
            matches!(
                inner,
                CursorPlanError::InvalidContinuationCursor {
                    reason: crate::db::codec::cursor::CursorDecodeError::OddLength
                }
            )
        },
    );
}

#[test]
fn session_cursor_error_mapping_parity_primary_key_type_mismatch() {
    assert_cursor_mapping_parity(
        || {
            CursorPlanError::continuation_cursor_primary_key_type_mismatch(
                "id",
                "ulid",
                Some(crate::value::Value::Text("not-a-ulid".to_string())),
            )
        },
        |inner| {
            matches!(
                inner,
                CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch {
                    field,
                    expected,
                    value: Some(crate::value::Value::Text(value))
                } if field == "id" && expected == "ulid" && value == "not-a-ulid"
            )
        },
    );
}

#[test]
fn session_cursor_error_mapping_parity_matrix_preserves_cursor_variants() {
    // Keep one matrix-level canary test name so cross-module audit references remain stable.
    assert_cursor_mapping_parity(
        || CursorPlanError::continuation_cursor_boundary_arity_mismatch(2, 1),
        |inner| {
            matches!(
                inner,
                CursorPlanError::ContinuationCursorBoundaryArityMismatch {
                    expected: 2,
                    found: 1
                }
            )
        },
    );
}

#[test]
fn execute_sql_select_star_honors_order_limit_offset() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "older".to_string(),
            age: 37,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "younger".to_string(),
            age: 19,
        })
        .expect("seed insert should succeed");

    let response = session
        .execute_sql::<SessionSqlEntity>(
            "SELECT * FROM SessionSqlEntity ORDER BY age ASC LIMIT 1 OFFSET 1",
        )
        .expect("SELECT * should execute");

    assert_eq!(response.count(), 1, "window should return one row");
    let row = response
        .iter()
        .next()
        .expect("windowed result should include one row");
    assert_eq!(
        row.entity_ref().name,
        "older",
        "ordered window should return the second age-ordered row",
    );
}

#[test]
fn execute_sql_scalar_matrix_queries_match_expected_rows() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed deterministic rows for scalar matrix cases.
    seed_session_sql_entities(
        &session,
        &[
            ("scalar-matrix-a", 10),
            ("scalar-matrix-b", 20),
            ("scalar-matrix-c", 30),
            ("scalar-matrix-d", 40),
        ],
    );

    // Phase 2: execute table-driven scalar SQL cases.
    let cases = vec![
        (
            "SELECT * \
             FROM SessionSqlEntity \
             ORDER BY age DESC LIMIT 2 OFFSET 1",
            vec![
                ("scalar-matrix-c".to_string(), 30_u64),
                ("scalar-matrix-b".to_string(), 20_u64),
            ],
        ),
        (
            "SELECT * \
             FROM SessionSqlEntity \
             WHERE age >= 20 \
             ORDER BY age ASC LIMIT 2",
            vec![
                ("scalar-matrix-b".to_string(), 20_u64),
                ("scalar-matrix-c".to_string(), 30_u64),
            ],
        ),
        (
            "SELECT DISTINCT * \
             FROM SessionSqlEntity \
             WHERE age >= 30 \
             ORDER BY age DESC",
            vec![
                ("scalar-matrix-d".to_string(), 40_u64),
                ("scalar-matrix-c".to_string(), 30_u64),
            ],
        ),
        (
            "SELECT * \
             FROM public.SessionSqlEntity \
             WHERE age < 25 \
             ORDER BY age ASC",
            vec![
                ("scalar-matrix-a".to_string(), 10_u64),
                ("scalar-matrix-b".to_string(), 20_u64),
            ],
        ),
    ];

    // Phase 3: assert scalar row payload order and values for each query.
    for (sql, expected_rows) in cases {
        let actual_rows = execute_sql_name_age_rows(&session, sql);
        assert_eq!(actual_rows, expected_rows, "scalar matrix case: {sql}");
    }
}

#[test]
fn execute_sql_delete_honors_predicate_order_and_limit() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "first-minor".to_string(),
            age: 16,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "second-minor".to_string(),
            age: 17,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "adult".to_string(),
            age: 42,
        })
        .expect("seed insert should succeed");

    let deleted = session
        .execute_sql::<SessionSqlEntity>(
            "DELETE FROM SessionSqlEntity WHERE age < 20 ORDER BY age ASC LIMIT 1",
        )
        .expect("DELETE should execute");

    assert_eq!(deleted.count(), 1, "delete limit should remove one row");
    assert_eq!(
        deleted
            .iter()
            .next()
            .expect("deleted row should exist")
            .entity_ref()
            .age,
        16,
        "ordered delete should remove the youngest matching row first",
    );

    let remaining = session
        .load::<SessionSqlEntity>()
        .order_by("age")
        .execute()
        .expect("post-delete load should succeed");
    let remaining_ages = remaining
        .iter()
        .map(|row| row.entity_ref().age)
        .collect::<Vec<_>>();

    assert_eq!(
        remaining_ages,
        vec![17, 42],
        "delete window semantics should preserve non-deleted rows",
    );
}

#[test]
fn execute_sql_delete_matrix_queries_match_deleted_and_remaining_rows() {
    // Phase 1: define one shared seed dataset and table-driven DELETE cases.
    let seed_rows = [
        ("delete-matrix-a", 10_u64),
        ("delete-matrix-b", 20_u64),
        ("delete-matrix-c", 30_u64),
        ("delete-matrix-d", 40_u64),
    ];
    let cases = vec![
        (
            "DELETE FROM SessionSqlEntity \
             WHERE age >= 20 \
             ORDER BY age ASC LIMIT 1",
            vec![("delete-matrix-b".to_string(), 20_u64)],
            vec![
                ("delete-matrix-a".to_string(), 10_u64),
                ("delete-matrix-c".to_string(), 30_u64),
                ("delete-matrix-d".to_string(), 40_u64),
            ],
        ),
        (
            "DELETE FROM SessionSqlEntity \
             WHERE age >= 20 \
             ORDER BY age DESC LIMIT 2",
            vec![
                ("delete-matrix-d".to_string(), 40_u64),
                ("delete-matrix-c".to_string(), 30_u64),
            ],
            vec![
                ("delete-matrix-a".to_string(), 10_u64),
                ("delete-matrix-b".to_string(), 20_u64),
            ],
        ),
        (
            "DELETE FROM SessionSqlEntity \
             WHERE age >= 100 \
             ORDER BY age ASC LIMIT 1",
            vec![],
            vec![
                ("delete-matrix-a".to_string(), 10_u64),
                ("delete-matrix-b".to_string(), 20_u64),
                ("delete-matrix-c".to_string(), 30_u64),
                ("delete-matrix-d".to_string(), 40_u64),
            ],
        ),
    ];

    // Phase 2: execute each DELETE case from a fresh seeded store.
    for (sql, expected_deleted, expected_remaining) in cases {
        reset_session_sql_store();
        let session = sql_session();
        seed_session_sql_entities(&session, &seed_rows);

        let deleted = session
            .execute_sql::<SessionSqlEntity>(sql)
            .expect("delete matrix SQL execution should succeed");
        let deleted_rows = deleted
            .iter()
            .map(|row| (row.entity_ref().name.clone(), row.entity_ref().age))
            .collect::<Vec<_>>();
        let remaining_rows =
            execute_sql_name_age_rows(&session, "SELECT * FROM SessionSqlEntity ORDER BY age ASC");

        assert_eq!(
            deleted_rows, expected_deleted,
            "delete matrix deleted rows: {sql}"
        );
        assert_eq!(
            remaining_rows, expected_remaining,
            "delete matrix remaining rows: {sql}",
        );
    }
}

#[test]
fn query_from_sql_select_field_projection_lowers_to_scalar_field_selection() {
    reset_session_sql_store();
    let session = sql_session();

    let query = session
        .query_from_sql::<SessionSqlEntity>("SELECT name, age FROM SessionSqlEntity")
        .expect("field-list SQL query should lower");
    let projection = query
        .plan()
        .expect("field-list SQL plan should build")
        .projection_spec();
    let field_names = projection
        .fields()
        .map(|field| match field {
            ProjectionField::Scalar {
                expr: Expr::Field(field),
                alias: None,
            } => field.as_str().to_string(),
            other @ ProjectionField::Scalar { .. } => {
                panic!("field-list SQL projection should lower to plain field exprs: {other:?}")
            }
        })
        .collect::<Vec<_>>();

    assert_eq!(field_names, vec!["name".to_string(), "age".to_string()]);
}

#[test]
fn query_from_sql_rejects_computed_text_projection_in_current_lane() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .query_from_sql::<SessionSqlEntity>("SELECT TRIM(name) FROM SessionSqlEntity")
        .expect_err(
            "query_from_sql should stay on the structural lowered-query lane and reject computed text projection forms",
        );

    assert!(
        err.to_string()
            .contains("query_from_sql does not accept computed text projection"),
        "query_from_sql should reject computed text projection with an actionable boundary message",
    );
}

#[test]
fn execute_sql_rejects_computed_text_projection_in_current_lane() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql::<SessionSqlEntity>("SELECT TRIM(name) FROM SessionSqlEntity")
        .expect_err("execute_sql should keep computed text projection on the dispatch-owned lane");

    assert!(
        err.to_string()
            .contains("execute_sql rejects computed text projection"),
        "execute_sql should reject computed text projection with an actionable boundary message",
    );
}

#[test]
fn query_from_sql_rejects_global_aggregate_execution_in_current_lane() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .query_from_sql::<SessionSqlEntity>("SELECT COUNT(*) FROM SessionSqlEntity")
        .expect_err(
            "query_from_sql should keep global aggregate execution on the dedicated aggregate lane",
        );

    assert!(
        err.to_string()
            .contains("query_from_sql rejects global aggregate SELECT"),
        "query_from_sql should reject global aggregate execution with an aggregate-lane boundary message",
    );
}

#[test]
fn query_from_sql_strict_like_prefix_lowers_to_strict_starts_with_intent() {
    reset_session_sql_store();
    let session = sql_session();

    let sql_query = session
        .query_from_sql::<SessionSqlEntity>("SELECT * FROM SessionSqlEntity WHERE name LIKE 'Al%'")
        .expect("strict LIKE prefix SQL query should lower");
    let fluent_query = crate::db::query::intent::Query::<SessionSqlEntity>::new(
        crate::db::predicate::MissingRowPolicy::Ignore,
    )
    .filter(Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::StartsWith,
        Value::Text("Al".to_string()),
        CoercionId::Strict,
    )));

    assert_eq!(
        sql_query
            .plan()
            .expect("strict LIKE SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent strict starts-with plan should build")
            .into_inner(),
        "plain LIKE 'prefix%' SQL lowering and fluent strict starts-with query must produce identical normalized planned intent",
    );
}

#[test]
fn query_from_sql_direct_starts_with_lowers_to_strict_starts_with_intent() {
    reset_session_sql_store();
    let session = sql_session();

    let sql_query = session
        .query_from_sql::<SessionSqlEntity>(
            "SELECT * FROM SessionSqlEntity WHERE STARTS_WITH(name, 'Al')",
        )
        .expect("direct STARTS_WITH SQL query should lower");
    let fluent_query = crate::db::query::intent::Query::<SessionSqlEntity>::new(
        crate::db::predicate::MissingRowPolicy::Ignore,
    )
    .filter(Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::StartsWith,
        Value::Text("Al".to_string()),
        CoercionId::Strict,
    )));

    assert_eq!(
        sql_query
            .plan()
            .expect("direct STARTS_WITH SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent strict starts-with plan should build")
            .into_inner(),
        "direct STARTS_WITH SQL lowering and fluent strict starts-with query must produce identical normalized planned intent",
    );
}

#[test]
fn query_from_sql_direct_lower_starts_with_lowers_to_casefold_starts_with_intent() {
    reset_session_sql_store();
    let session = sql_session();

    let sql_query = session
        .query_from_sql::<SessionSqlEntity>(
            "SELECT * FROM SessionSqlEntity WHERE STARTS_WITH(LOWER(name), 'Al')",
        )
        .expect("direct LOWER(field) STARTS_WITH SQL query should lower");
    let fluent_query = crate::db::query::intent::Query::<SessionSqlEntity>::new(
        crate::db::predicate::MissingRowPolicy::Ignore,
    )
    .filter(Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::StartsWith,
        Value::Text("Al".to_string()),
        CoercionId::TextCasefold,
    )));

    assert_eq!(
        sql_query
            .plan()
            .expect("direct LOWER(field) STARTS_WITH SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent text-casefold starts-with plan should build")
            .into_inner(),
        "direct LOWER(field) STARTS_WITH SQL lowering and fluent text-casefold starts-with query must produce identical normalized planned intent",
    );
}

#[test]
fn query_from_sql_direct_upper_starts_with_lowers_to_casefold_starts_with_intent() {
    reset_session_sql_store();
    let session = sql_session();

    let sql_query = session
        .query_from_sql::<SessionSqlEntity>(
            "SELECT * FROM SessionSqlEntity WHERE STARTS_WITH(UPPER(name), 'AL')",
        )
        .expect("direct UPPER(field) STARTS_WITH SQL query should lower");
    let fluent_query = crate::db::query::intent::Query::<SessionSqlEntity>::new(
        crate::db::predicate::MissingRowPolicy::Ignore,
    )
    .filter(Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::StartsWith,
        Value::Text("AL".to_string()),
        CoercionId::TextCasefold,
    )));

    assert_eq!(
        sql_query
            .plan()
            .expect("direct UPPER(field) STARTS_WITH SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent text-casefold starts-with plan should build")
            .into_inner(),
        "direct UPPER(field) STARTS_WITH SQL lowering and fluent text-casefold starts-with query must produce identical normalized planned intent",
    );
}

#[test]
fn query_from_sql_lower_like_prefix_lowers_to_casefold_starts_with_intent() {
    reset_session_sql_store();
    let session = sql_session();

    let sql_query = session
        .query_from_sql::<SessionSqlEntity>(
            "SELECT * FROM SessionSqlEntity WHERE LOWER(name) LIKE 'Al%'",
        )
        .expect("LOWER(field) LIKE prefix SQL query should lower");
    let fluent_query = crate::db::query::intent::Query::<SessionSqlEntity>::new(
        crate::db::predicate::MissingRowPolicy::Ignore,
    )
    .filter(Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::StartsWith,
        Value::Text("Al".to_string()),
        CoercionId::TextCasefold,
    )));

    assert_eq!(
        sql_query
            .plan()
            .expect("LOWER(field) LIKE SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent text-casefold starts-with plan should build")
            .into_inner(),
        "LOWER(field) LIKE 'prefix%' SQL lowering and fluent text-casefold starts-with query must produce identical normalized planned intent",
    );
}

#[test]
fn query_from_sql_upper_like_prefix_lowers_to_casefold_starts_with_intent() {
    reset_session_sql_store();
    let session = sql_session();

    let sql_query = session
        .query_from_sql::<SessionSqlEntity>(
            "SELECT * FROM SessionSqlEntity WHERE UPPER(name) LIKE 'AL%'",
        )
        .expect("UPPER(field) LIKE prefix SQL query should lower");
    let fluent_query = crate::db::query::intent::Query::<SessionSqlEntity>::new(
        crate::db::predicate::MissingRowPolicy::Ignore,
    )
    .filter(Predicate::Compare(ComparePredicate::with_coercion(
        "name",
        CompareOp::StartsWith,
        Value::Text("AL".to_string()),
        CoercionId::TextCasefold,
    )));

    assert_eq!(
        sql_query
            .plan()
            .expect("UPPER(field) LIKE SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent text-casefold starts-with plan should build")
            .into_inner(),
        "UPPER(field) LIKE 'prefix%' SQL lowering and fluent text-casefold starts-with query must produce identical normalized planned intent",
    );
}

#[test]
fn query_from_sql_upper_text_range_lowers_to_casefold_ordered_bounds_intent() {
    reset_session_sql_store();
    let session = sql_session();

    let sql_query = session
        .query_from_sql::<SessionSqlEntity>(
            "SELECT * FROM SessionSqlEntity WHERE UPPER(name) >= 'AL' AND UPPER(name) < 'AM'",
        )
        .expect("UPPER(field) ordered text-range SQL query should lower");
    let fluent_query = crate::db::query::intent::Query::<SessionSqlEntity>::new(
        crate::db::predicate::MissingRowPolicy::Ignore,
    )
    .filter(Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Gte,
            Value::Text("AL".to_string()),
            CoercionId::TextCasefold,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Lt,
            Value::Text("AM".to_string()),
            CoercionId::TextCasefold,
        )),
    ]));

    assert_eq!(
        sql_query
            .plan()
            .expect("UPPER(field) ordered text-range SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent text-casefold ordered bounds plan should build")
            .into_inner(),
        "UPPER(field) ordered text bounds must lower onto the same normalized casefold range intent as the fluent query",
    );
}

#[test]
fn query_from_sql_lower_text_range_lowers_to_casefold_ordered_bounds_intent() {
    reset_session_sql_store();
    let session = sql_session();

    let sql_query = session
        .query_from_sql::<SessionSqlEntity>(
            "SELECT * FROM SessionSqlEntity WHERE LOWER(name) >= 'al' AND LOWER(name) < 'am'",
        )
        .expect("LOWER(field) ordered text-range SQL query should lower");
    let fluent_query = crate::db::query::intent::Query::<SessionSqlEntity>::new(
        crate::db::predicate::MissingRowPolicy::Ignore,
    )
    .filter(Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Gte,
            Value::Text("al".to_string()),
            CoercionId::TextCasefold,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Lt,
            Value::Text("am".to_string()),
            CoercionId::TextCasefold,
        )),
    ]));

    assert_eq!(
        sql_query
            .plan()
            .expect("LOWER(field) ordered text-range SQL plan should build")
            .into_inner(),
        fluent_query
            .plan()
            .expect("fluent text-casefold ordered bounds plan should build")
            .into_inner(),
        "LOWER(field) ordered text bounds must lower onto the same normalized casefold range intent as the fluent query",
    );
}

#[test]
fn execute_sql_projection_strict_like_prefix_matches_indexed_covering_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one mixed prefix dataset under a real secondary text index.
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    // Phase 2: execute the strict indexed LIKE projection and compare with the
    // casefold fallback shape that already succeeds in the reported repro.
    let strict_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE name LIKE 'S%'",
    )
    .expect("strict indexed LIKE projection should execute");
    let casefold_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE UPPER(name) LIKE 'S%'",
    )
    .expect("casefold LIKE projection should execute");

    let expected_rows = vec![
        vec![Value::Text("Sonja She-Devil".to_string())],
        vec![Value::Text("Stamm Bladecaster".to_string())],
        vec![Value::Text("Syra Child of Nature".to_string())],
        vec![Value::Text("Sir Edward Lion".to_string())],
        vec![Value::Text("Sethra Bhoaghail".to_string())],
    ];

    assert_eq!(
        strict_rows, expected_rows,
        "strict indexed LIKE prefix projection must return the matching secondary-index rows",
    );
    assert_eq!(
        strict_rows, casefold_rows,
        "strict indexed LIKE prefix execution must match the casefold fallback result set for already-uppercase prefixes",
    );
}

#[test]
fn execute_sql_entity_strict_like_prefix_matches_projection_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic uppercase-prefix dataset under the same
    // secondary text index used by the projection regression.
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    // Phase 2: verify entity-row execution agrees with the projection surface
    // for the repaired strict LIKE prefix path.
    let projected_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name ASC",
    )
    .expect("strict LIKE prefix projection should execute");
    let entity_rows = session
        .execute_sql::<IndexedSessionSqlEntity>(
            "SELECT * FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name ASC",
        )
        .expect("strict LIKE prefix entity query should execute");
    let entity_projected_names = entity_rows
        .iter()
        .map(|row| vec![Value::Text(row.entity_ref().name.clone())])
        .collect::<Vec<_>>();

    assert_eq!(entity_projected_names, projected_rows);
}

#[test]
fn session_explain_execution_strict_like_prefix_covering_query_uses_index_range_access() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic prefix dataset so EXPLAIN EXECUTION can
    // lock the bounded secondary-index route for the repaired strict LIKE path.
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    // Phase 2: require the ordered-and-limited strict LIKE query to stay on
    // the shared index-range covering route instead of regressing to a
    // materialized full scan.
    let descriptor = session
        .query_from_sql::<IndexedSessionSqlEntity>(
            "SELECT name FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name ASC, id ASC LIMIT 2",
        )
        .expect("strict LIKE prefix covering SQL query should lower")
        .explain_execution()
        .expect("strict LIKE prefix covering SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "ordered strict LIKE prefix queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "ordered strict LIKE prefix projections should keep the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "ordered strict LIKE prefix explain roots should expose the covering-read route label",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "ordered strict LIKE prefix roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "ordered strict LIKE prefix roots should report access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_strict_like_prefix_desc_covering_query_uses_index_range_access() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic prefix dataset so the descending strict
    // LIKE route cannot silently fall back to a reverse materialized sort.
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    // Phase 2: require the descending ordered-and-limited strict LIKE query to
    // stay on the same bounded index-range covering route.
    let descriptor = session
        .query_from_sql::<IndexedSessionSqlEntity>(
            "SELECT name FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name DESC, id DESC LIMIT 2",
        )
        .expect("descending strict LIKE prefix covering SQL query should lower")
        .explain_execution()
        .expect("descending strict LIKE prefix covering SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "descending strict LIKE prefix queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "descending strict LIKE prefix projections should keep the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "descending strict LIKE prefix explain roots should expose the covering-read route label",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending strict LIKE prefix roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "descending strict LIKE prefix roots should report access-satisfied ordering",
    );
}

#[test]
fn execute_sql_projection_strict_text_range_matches_indexed_covering_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one mixed prefix dataset under the same real secondary
    // text index used by the strict LIKE regression so bounded range forms can
    // be checked against the already-proven prefix route.
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    // Phase 2: execute the equivalent ordered strict text range and require it
    // to match the repaired strict LIKE prefix result exactly.
    let range_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE name >= 'S' AND name < 'T' ORDER BY name ASC, id ASC",
    )
    .expect("strict text-range projection should execute");
    let prefix_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name ASC, id ASC",
    )
    .expect("strict LIKE prefix projection should execute");

    let expected_rows = vec![
        vec![Value::Text("Sethra Bhoaghail".to_string())],
        vec![Value::Text("Sir Edward Lion".to_string())],
        vec![Value::Text("Sonja She-Devil".to_string())],
        vec![Value::Text("Stamm Bladecaster".to_string())],
        vec![Value::Text("Syra Child of Nature".to_string())],
    ];

    assert_eq!(
        range_rows, expected_rows,
        "ordered strict text ranges must return the matching secondary-index rows",
    );
    assert_eq!(
        range_rows, prefix_rows,
        "ordered strict text ranges must stay in parity with the equivalent strict LIKE prefix route",
    );
}

#[test]
fn execute_sql_entity_strict_text_range_matches_projection_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic uppercase-prefix dataset under the same
    // secondary text index used by the explicit text-range regression.
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    // Phase 2: verify entity-row execution agrees with the projection surface
    // for the explicit bounded text-range form.
    let projected_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE name >= 'S' AND name < 'T' ORDER BY name ASC, id ASC",
    )
    .expect("strict text-range projection should execute");
    let entity_rows = session
        .execute_sql::<IndexedSessionSqlEntity>(
            "SELECT * FROM IndexedSessionSqlEntity WHERE name >= 'S' AND name < 'T' ORDER BY name ASC, id ASC",
        )
        .expect("strict text-range entity query should execute");
    let entity_projected_names = entity_rows
        .iter()
        .map(|row| vec![Value::Text(row.entity_ref().name.clone())])
        .collect::<Vec<_>>();

    assert_eq!(entity_projected_names, projected_rows);
}

#[test]
fn session_explain_execution_strict_text_range_covering_query_uses_index_range_access() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic prefix dataset so EXPLAIN EXECUTION can
    // lock the bounded secondary-index route for the explicit text-range form.
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    // Phase 2: require the ordered-and-limited explicit text range to stay on
    // the shared index-range covering route instead of regressing to one
    // materialized full scan.
    let descriptor = session
        .query_from_sql::<IndexedSessionSqlEntity>(
            "SELECT name FROM IndexedSessionSqlEntity WHERE name >= 'S' AND name < 'T' ORDER BY name ASC, id ASC LIMIT 2",
        )
        .expect("strict text-range covering SQL query should lower")
        .explain_execution()
        .expect("strict text-range covering SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "ordered strict text-range queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "ordered strict text-range projections should keep the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "ordered strict text-range explain roots should expose the covering-read route label",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "ordered strict text-range roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "ordered strict text-range roots should report access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_strict_text_range_desc_covering_query_uses_index_range_access() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic prefix dataset so the descending
    // explicit text-range route cannot silently fall back to a reverse
    // materialized sort.
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    // Phase 2: require the descending ordered-and-limited explicit text range
    // to stay on the same bounded index-range covering route.
    let descriptor = session
        .query_from_sql::<IndexedSessionSqlEntity>(
            "SELECT name FROM IndexedSessionSqlEntity WHERE name >= 'S' AND name < 'T' ORDER BY name DESC, id DESC LIMIT 2",
        )
        .expect("descending strict text-range covering SQL query should lower")
        .explain_execution()
        .expect("descending strict text-range covering SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "descending strict text-range queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "descending strict text-range projections should keep the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "descending strict text-range explain roots should expose the covering-read route label",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending strict text-range roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "descending strict text-range roots should report access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_equivalent_strict_prefix_forms_preserve_covering_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic uppercase-prefix dataset so the three
    // accepted strict spellings all run against the same indexed row set.
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    // Phase 2: require strict LIKE, direct STARTS_WITH, and explicit text
    // ranges to preserve the same covering index-range route on one ordered
    // limited window.
    let queries = [
        (
            "strict LIKE prefix",
            "SELECT name FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name ASC, id ASC LIMIT 2",
        ),
        (
            "direct STARTS_WITH",
            "SELECT name FROM IndexedSessionSqlEntity WHERE STARTS_WITH(name, 'S') ORDER BY name ASC, id ASC LIMIT 2",
        ),
        (
            "strict text range",
            "SELECT name FROM IndexedSessionSqlEntity WHERE name >= 'S' AND name < 'T' ORDER BY name ASC, id ASC LIMIT 2",
        ),
    ];

    for (context, sql) in queries {
        let descriptor = session
            .query_from_sql::<IndexedSessionSqlEntity>(sql)
            .unwrap_or_else(|err| panic!("{context} SQL query should lower: {err:?}"))
            .explain_execution()
            .unwrap_or_else(|err| {
                panic!("{context} SQL explain_execution should succeed: {err:?}")
            });

        assert_eq!(
            descriptor.node_type(),
            ExplainExecutionNodeType::IndexRangeScan,
            "{context} queries should stay on the shared index-range root",
        );
        assert_eq!(
            descriptor.covering_scan(),
            Some(true),
            "{context} projections should keep the explicit covering-read route",
        );
        assert_eq!(
            descriptor.node_properties().get("cov_read_route"),
            Some(&Value::Text("covering_read".to_string())),
            "{context} explain roots should expose the covering-read route label",
        );
        assert!(
            explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::SecondaryOrderPushdown
            )
            .is_some(),
            "{context} roots should report secondary order pushdown",
        );
        assert!(
            explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::OrderByAccessSatisfied
            )
            .is_some(),
            "{context} roots should report access-satisfied ordering",
        );
    }
}

#[test]
fn session_explain_execution_equivalent_desc_strict_prefix_forms_preserve_covering_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic uppercase-prefix dataset so the three
    // accepted descending spellings all run against the same indexed row set.
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    // Phase 2: require strict LIKE, direct STARTS_WITH, and explicit text
    // ranges to preserve the same reverse covering index-range route.
    let queries = [
        (
            "descending strict LIKE prefix",
            "SELECT name FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name DESC, id DESC LIMIT 2",
        ),
        (
            "descending direct STARTS_WITH",
            "SELECT name FROM IndexedSessionSqlEntity WHERE STARTS_WITH(name, 'S') ORDER BY name DESC, id DESC LIMIT 2",
        ),
        (
            "descending strict text range",
            "SELECT name FROM IndexedSessionSqlEntity WHERE name >= 'S' AND name < 'T' ORDER BY name DESC, id DESC LIMIT 2",
        ),
    ];

    for (context, sql) in queries {
        let descriptor = session
            .query_from_sql::<IndexedSessionSqlEntity>(sql)
            .unwrap_or_else(|err| panic!("{context} SQL query should lower: {err:?}"))
            .explain_execution()
            .unwrap_or_else(|err| {
                panic!("{context} SQL explain_execution should succeed: {err:?}")
            });

        assert_eq!(
            descriptor.node_type(),
            ExplainExecutionNodeType::IndexRangeScan,
            "{context} queries should stay on the shared index-range root",
        );
        assert_eq!(
            descriptor.covering_scan(),
            Some(true),
            "{context} projections should keep the explicit covering-read route",
        );
        assert_eq!(
            descriptor.node_properties().get("cov_read_route"),
            Some(&Value::Text("covering_read".to_string())),
            "{context} explain roots should expose the covering-read route label",
        );
        assert!(
            explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::SecondaryOrderPushdown
            )
            .is_some(),
            "{context} roots should report secondary order pushdown",
        );
        assert!(
            explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::OrderByAccessSatisfied
            )
            .is_some(),
            "{context} roots should report access-satisfied ordering",
        );
    }
}

#[test]
fn execute_sql_projection_index_coverable_primary_key_and_prefix_field_matches_entity_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic equality-prefix dataset on the indexed
    // `name` field so the projection lane can stay on the same query shape as
    // the hot canister attribution benchmark.
    seed_indexed_session_sql_entities(
        &session,
        &[("alice", 10), ("alice", 20), ("bob", 30), ("carol", 40)],
    );

    // Phase 2: verify the projection lane returns the same `(id, name)` row
    // as the entity lane for an index-covered equality-prefix query.
    let sql =
        "SELECT id, name FROM IndexedSessionSqlEntity WHERE name = 'alice' ORDER BY id LIMIT 1";
    let projected_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(&session, sql)
        .expect("index-covered projection query should execute");
    let entity_rows = session
        .execute_sql::<IndexedSessionSqlEntity>(sql)
        .expect("index-covered entity query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| {
            vec![
                Value::Ulid(row.entity_ref().id),
                Value::Text(row.entity_ref().name.clone()),
            ]
        })
        .collect::<Vec<_>>();

    assert_eq!(entity_projected_rows, projected_rows);
}

#[test]
fn execute_sql_projection_index_coverable_secondary_order_field_matches_entity_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic ordered secondary-index dataset on the
    // indexed `name` field so the projection lane can stay on the same
    // coverable order-by-name shape tracked by PocketIC attribution.
    seed_indexed_session_sql_entities(
        &session,
        &[("carol", 10), ("alice", 20), ("bob", 30), ("dora", 40)],
    );

    // Phase 2: verify the projection lane returns the same ordered `name`
    // row as the entity lane for a direct secondary-index covering query.
    let sql = "SELECT name FROM IndexedSessionSqlEntity ORDER BY name ASC LIMIT 1";
    let projected_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(&session, sql)
        .expect("secondary-order covering projection query should execute");
    let entity_rows = session
        .execute_sql::<IndexedSessionSqlEntity>(sql)
        .expect("secondary-order covering entity query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| vec![Value::Text(row.entity_ref().name.clone())])
        .collect::<Vec<_>>();

    assert_eq!(entity_projected_rows, projected_rows);
}

#[test]
fn execute_sql_projection_index_coverable_secondary_order_field_with_offset_matches_entity_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic ordered secondary-index dataset so the
    // covering projection lane can validate post-filter pagination against the
    // entity lane on the same index-ordered shape.
    seed_indexed_session_sql_entities(
        &session,
        &[("carol", 10), ("alice", 20), ("bob", 30), ("dora", 40)],
    );

    // Phase 2: verify the projection lane preserves the same ordered page
    // window as the entity lane for a direct secondary-index covering query.
    let sql = "SELECT name FROM IndexedSessionSqlEntity ORDER BY name ASC LIMIT 2 OFFSET 1";
    let projected_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(&session, sql)
        .expect("secondary-order covering projection page query should execute");
    let entity_rows = session
        .execute_sql::<IndexedSessionSqlEntity>(sql)
        .expect("secondary-order covering entity page query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| vec![Value::Text(row.entity_ref().name.clone())])
        .collect::<Vec<_>>();

    assert_eq!(entity_projected_rows, projected_rows);
}

#[test]
fn session_explain_execution_order_only_covering_query_uses_index_range_access() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic secondary-order dataset so the SQL lane
    // can prove planner-selected order-only index access instead of a
    // materialized full scan that merely returns the same first row.
    seed_indexed_session_sql_entities(
        &session,
        &[("carol", 10), ("alice", 20), ("bob", 30), ("dora", 40)],
    );

    // Phase 2: require EXPLAIN EXECUTION to surface the shared planner/runtime
    // order-only index-range path for one coverable `ORDER BY name, id` query.
    let descriptor = session
        .query_from_sql::<IndexedSessionSqlEntity>(
            "SELECT name FROM IndexedSessionSqlEntity ORDER BY name ASC LIMIT 1",
        )
        .expect("order-only covering SQL query should lower")
        .explain_execution()
        .expect("order-only covering SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "order-only single-field secondary queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "order-only coverable projections should keep the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "order-only covering explain roots should expose the covering-read route label",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "order-only index-range roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "order-only index-range roots should report access-satisfied ordering",
    );
}

#[test]
fn execute_sql_projection_order_only_filtered_covering_query_returns_guarded_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered-index dataset where the
    // lexicographically earliest row is inactive so the guarded query can only
    // stay correct if it respects the filtered-index predicate.
    seed_filtered_indexed_session_sql_entities(
        &session,
        &[
            (9_201, "amber", false, 10),
            (9_202, "bravo", true, 20),
            (9_203, "charlie", true, 30),
            (9_204, "delta", false, 40),
        ],
    );

    // Phase 2: require the projection lane to return only the guarded active
    // subset under the order-only `ORDER BY name, id` shape.
    let sql = "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true ORDER BY name ASC, id ASC LIMIT 2";
    let projected_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(&session, sql)
        .expect("filtered order-only covering projection query should execute");

    assert_eq!(
        projected_rows,
        vec![
            vec![Value::Text("bravo".to_string())],
            vec![Value::Text("charlie".to_string())],
        ],
        "guarded order-only covering queries should return only rows admitted by the filtered index predicate",
    );
}

#[test]
fn session_explain_execution_order_only_filtered_covering_query_uses_index_range_access() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered-index dataset so EXPLAIN
    // EXECUTION can prove the guarded query uses the filtered secondary index
    // instead of one materialized full scan.
    seed_filtered_indexed_session_sql_entities(
        &session,
        &[
            (9_201, "amber", false, 10),
            (9_202, "bravo", true, 20),
            (9_203, "charlie", true, 30),
            (9_204, "delta", false, 40),
        ],
    );

    // Phase 2: require the guarded order-only SQL lane to surface the shared
    // planner/runtime index-range covering route.
    let descriptor = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true ORDER BY name ASC, id ASC LIMIT 2",
        )
        .expect("filtered order-only covering SQL query should lower")
        .explain_execution()
        .expect("filtered order-only covering SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "guarded filtered-order queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "guarded filtered-order coverable projections should keep the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "guarded filtered-order explain roots should expose the covering-read route label",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "guarded filtered-order index-range roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "guarded filtered-order index-range roots should report access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_order_only_filtered_desc_residual_query_fails_closed_before_top_n() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered composite dataset where the
    // descending `handle` order uses the `tier, handle` index, but the extra
    // `age >= 20` predicate must remain residual.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_221, "amber", false, "gold", "bramble", 10),
            (9_222, "bravo", true, "gold", "bravo", 20),
            (9_223, "charlie", true, "gold", "bristle", 30),
            (9_224, "delta", false, "silver", "brisk", 40),
            (9_225, "echo", true, "silver", "Brisk", 50),
        ],
    );

    // Phase 2: require the residual descending filtered composite order-only
    // shape to keep the secondary-prefix route while failing closed before
    // Top-N derivation.
    let descriptor = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT id, tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND age >= 20 ORDER BY handle DESC, id DESC LIMIT 2",
        )
        .expect("descending filtered composite residual order-only SQL query should lower")
        .explain_execution()
        .expect(
            "descending filtered composite residual order-only SQL explain_execution should succeed",
        );

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexPrefixScan,
        "descending filtered composite residual order-only queries should keep the filtered index-prefix root",
    );
    assert_eq!(
        descriptor.execution_mode(),
        crate::db::ExplainExecutionMode::Materialized,
        "descending filtered composite residual order-only queries should stay materialized",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(false),
        "descending filtered composite residual order-only projections should materialize rows because the residual filter needs non-index fields",
    );
    assert_eq!(
        descriptor.node_properties().get("prefix_len"),
        Some(&Value::Uint(1)),
        "descending filtered composite residual order-only roots should report one equality-prefix slot",
    );
    assert_eq!(
        descriptor.node_properties().get("prefix_values"),
        Some(&Value::List(vec![Value::Text("gold".to_string())])),
        "descending filtered composite residual order-only roots should expose the concrete equality-prefix value",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::ResidualPredicateFilter
        )
        .is_some(),
        "descending filtered composite residual order-only roots should expose the residual filter stage",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending filtered composite residual order-only roots should still report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        )
        .is_some(),
        "descending filtered composite residual order-only roots should fail closed to a materialized sort",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_none(),
        "descending filtered composite residual order-only roots must not derive Top-N seek",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_none(),
        "descending filtered composite residual order-only roots must not report access-satisfied ordering after failing closed",
    );
}

#[test]
fn session_explain_execution_order_only_filtered_query_without_guard_falls_back_to_full_scan() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered-index dataset so the unguarded
    // order-only query would be observably wrong if it silently reused the
    // filtered secondary index without proving the guard predicate.
    seed_filtered_indexed_session_sql_entities(
        &session,
        &[
            (9_201, "amber", false, 10),
            (9_202, "bravo", true, 20),
            (9_203, "charlie", true, 30),
            (9_204, "delta", false, 40),
        ],
    );

    // Phase 2: require the unguarded `ORDER BY name, id` query to stay on the
    // fail-closed full-scan path instead of silently borrowing the filtered
    // index order.
    let descriptor = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT name FROM FilteredIndexedSessionSqlEntity ORDER BY name ASC, id ASC LIMIT 2",
        )
        .expect("unguarded filtered-order SQL query should lower")
        .explain_execution()
        .expect("unguarded filtered-order SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::FullScan,
        "unguarded filtered-order queries must fail closed to the full-scan root",
    );
    assert_ne!(
        descriptor.covering_scan(),
        Some(true),
        "unguarded filtered-order queries must not claim the covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("materialized".to_string())),
        "unguarded filtered-order explains must surface the explicit materialized fallback route",
    );
}

#[test]
fn execute_sql_projection_filtered_equivalent_strict_prefix_forms_match_guarded_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered-index dataset where only the
    // guarded active rows should participate in the bounded `br...` window.
    seed_filtered_indexed_session_sql_entities(
        &session,
        &[
            (9_201, "amber", false, 10),
            (9_202, "bravo", true, 20),
            (9_203, "bristle", true, 30),
            (9_204, "broom", false, 40),
            (9_205, "charlie", true, 50),
        ],
    );

    // Phase 2: require strict LIKE, direct STARTS_WITH, and explicit text
    // ranges to return the same guarded filtered-index rows.
    let like_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 2",
    )
    .expect("filtered strict LIKE prefix projection should execute");
    let starts_with_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND STARTS_WITH(name, 'br') ORDER BY name ASC, id ASC LIMIT 2",
    )
    .expect("filtered direct STARTS_WITH projection should execute");
    let range_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND name >= 'br' AND name < 'bs' ORDER BY name ASC, id ASC LIMIT 2",
    )
    .expect("filtered strict text-range projection should execute");

    let expected_rows = vec![
        vec![Value::Text("bravo".to_string())],
        vec![Value::Text("bristle".to_string())],
    ];

    assert_eq!(
        like_rows, expected_rows,
        "filtered strict LIKE prefix projection must return the guarded bounded rows",
    );
    assert_eq!(
        starts_with_rows, like_rows,
        "filtered direct STARTS_WITH projection must preserve row parity with strict LIKE",
    );
    assert_eq!(
        range_rows, like_rows,
        "filtered strict text-range projection must preserve row parity with strict LIKE",
    );
}

#[test]
fn execute_sql_projection_filtered_equivalent_desc_strict_prefix_forms_match_guarded_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered-index dataset where the same
    // guarded `br...` window can prove descending parity across the accepted
    // bounded text spellings.
    seed_filtered_indexed_session_sql_entities(
        &session,
        &[
            (9_201, "amber", false, 10),
            (9_202, "bravo", true, 20),
            (9_203, "bristle", true, 30),
            (9_204, "broom", false, 40),
            (9_205, "charlie", true, 50),
        ],
    );

    // Phase 2: require strict LIKE, direct STARTS_WITH, and explicit text
    // ranges to return the same reverse guarded filtered-index window.
    let like_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 2",
    )
    .expect("descending filtered strict LIKE prefix projection should execute");
    let starts_with_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND STARTS_WITH(name, 'br') ORDER BY name DESC, id DESC LIMIT 2",
    )
    .expect("descending filtered direct STARTS_WITH projection should execute");
    let range_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND name >= 'br' AND name < 'bs' ORDER BY name DESC, id DESC LIMIT 2",
    )
    .expect("descending filtered strict text-range projection should execute");

    let expected_rows = vec![
        vec![Value::Text("bristle".to_string())],
        vec![Value::Text("bravo".to_string())],
    ];

    assert_eq!(
        like_rows, expected_rows,
        "descending filtered strict LIKE prefix projection must return the guarded reverse bounded rows",
    );
    assert_eq!(
        starts_with_rows, like_rows,
        "descending filtered direct STARTS_WITH projection must preserve row parity with strict LIKE",
    );
    assert_eq!(
        range_rows, like_rows,
        "descending filtered strict text-range projection must preserve row parity with strict LIKE",
    );
}

#[test]
fn session_explain_execution_filtered_equivalent_strict_prefix_forms_preserve_covering_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered-index dataset so the guarded
    // bounded text forms all target the same filtered secondary index window.
    seed_filtered_indexed_session_sql_entities(
        &session,
        &[
            (9_201, "amber", false, 10),
            (9_202, "bravo", true, 20),
            (9_203, "bristle", true, 30),
            (9_204, "broom", false, 40),
            (9_205, "charlie", true, 50),
        ],
    );

    // Phase 2: require strict LIKE, direct STARTS_WITH, and explicit text
    // ranges to preserve the same covering filtered index-range route.
    let queries = [
        (
            "filtered strict LIKE prefix",
            "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 2",
        ),
        (
            "filtered direct STARTS_WITH",
            "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND STARTS_WITH(name, 'br') ORDER BY name ASC, id ASC LIMIT 2",
        ),
        (
            "filtered strict text range",
            "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND name >= 'br' AND name < 'bs' ORDER BY name ASC, id ASC LIMIT 2",
        ),
    ];

    for (context, sql) in queries {
        let descriptor = session
            .query_from_sql::<FilteredIndexedSessionSqlEntity>(sql)
            .unwrap_or_else(|err| panic!("{context} SQL query should lower: {err:?}"))
            .explain_execution()
            .unwrap_or_else(|err| {
                panic!("{context} SQL explain_execution should succeed: {err:?}")
            });

        assert_eq!(
            descriptor.node_type(),
            ExplainExecutionNodeType::IndexRangeScan,
            "{context} queries should stay on the shared index-range root",
        );
        assert_eq!(
            descriptor.covering_scan(),
            Some(true),
            "{context} projections should keep the explicit covering-read route",
        );
        assert_eq!(
            descriptor.node_properties().get("cov_read_route"),
            Some(&Value::Text("covering_read".to_string())),
            "{context} explain roots should expose the covering-read route label",
        );
        assert!(
            explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::SecondaryOrderPushdown
            )
            .is_some(),
            "{context} roots should report secondary order pushdown",
        );
        assert!(
            explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::OrderByAccessSatisfied
            )
            .is_some(),
            "{context} roots should report access-satisfied ordering",
        );
    }
}

#[test]
fn session_explain_execution_filtered_equivalent_desc_strict_prefix_forms_preserve_covering_route()
{
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered-index dataset so the guarded
    // bounded text forms all target the same reverse filtered secondary index
    // window.
    seed_filtered_indexed_session_sql_entities(
        &session,
        &[
            (9_201, "amber", false, 10),
            (9_202, "bravo", true, 20),
            (9_203, "bristle", true, 30),
            (9_204, "broom", false, 40),
            (9_205, "charlie", true, 50),
        ],
    );

    // Phase 2: require strict LIKE, direct STARTS_WITH, and explicit text
    // ranges to preserve the same reverse covering filtered index-range route.
    let queries = [
        (
            "descending filtered strict LIKE prefix",
            "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 2",
        ),
        (
            "descending filtered direct STARTS_WITH",
            "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND STARTS_WITH(name, 'br') ORDER BY name DESC, id DESC LIMIT 2",
        ),
        (
            "descending filtered strict text range",
            "SELECT name FROM FilteredIndexedSessionSqlEntity WHERE active = true AND name >= 'br' AND name < 'bs' ORDER BY name DESC, id DESC LIMIT 2",
        ),
    ];

    for (context, sql) in queries {
        let descriptor = session
            .query_from_sql::<FilteredIndexedSessionSqlEntity>(sql)
            .unwrap_or_else(|err| panic!("{context} SQL query should lower: {err:?}"))
            .explain_execution()
            .unwrap_or_else(|err| {
                panic!("{context} SQL explain_execution should succeed: {err:?}")
            });

        assert_eq!(
            descriptor.node_type(),
            ExplainExecutionNodeType::IndexRangeScan,
            "{context} queries should stay on the shared reverse index-range root",
        );
        assert_eq!(
            descriptor.covering_scan(),
            Some(true),
            "{context} projections should keep the explicit reverse covering-read route",
        );
        assert_eq!(
            descriptor.node_properties().get("cov_read_route"),
            Some(&Value::Text("covering_read".to_string())),
            "{context} explain roots should expose the reverse covering-read route label",
        );
        assert!(
            explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::SecondaryOrderPushdown
            )
            .is_some(),
            "{context} roots should report secondary order pushdown",
        );
        assert!(
            explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::OrderByAccessSatisfied
            )
            .is_some(),
            "{context} roots should report access-satisfied reverse ordering",
        );
    }
}

#[test]
fn execute_sql_projection_filtered_composite_equivalent_strict_prefix_forms_match_guarded_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered composite-index dataset where
    // the guard, equality prefix, and bounded text window together isolate one
    // ordered `(tier, handle)` range on the filtered secondary index.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_221, "alpha", false, "gold", "bramble", 10),
            (9_222, "bravo-user", true, "gold", "bravo", 20),
            (9_223, "bristle-user", true, "gold", "bristle", 30),
            (9_224, "brisk-user", true, "silver", "brisk", 40),
            (9_225, "charlie-user", true, "gold", "charlie", 50),
        ],
    );

    // Phase 2: require strict LIKE, direct STARTS_WITH, and explicit text
    // ranges to return the same guarded equality-prefix filtered window.
    let like_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2",
    )
    .expect("filtered composite strict LIKE prefix projection should execute");
    let starts_with_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND STARTS_WITH(handle, 'br') ORDER BY handle ASC, id ASC LIMIT 2",
    )
    .expect("filtered composite direct STARTS_WITH projection should execute");
    let range_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND handle >= 'br' AND handle < 'bs' ORDER BY handle ASC, id ASC LIMIT 2",
    )
    .expect("filtered composite strict text-range projection should execute");

    let expected_rows = vec![
        vec![
            Value::Text("gold".to_string()),
            Value::Text("bravo".to_string()),
        ],
        vec![
            Value::Text("gold".to_string()),
            Value::Text("bristle".to_string()),
        ],
    ];

    assert_eq!(
        like_rows, expected_rows,
        "filtered composite strict LIKE prefix projection must return the guarded equality-prefix rows",
    );
    assert_eq!(
        starts_with_rows, like_rows,
        "filtered composite direct STARTS_WITH projection must preserve row parity with strict LIKE",
    );
    assert_eq!(
        range_rows, like_rows,
        "filtered composite strict text-range projection must preserve row parity with strict LIKE",
    );
}

#[test]
fn execute_sql_projection_filtered_composite_equivalent_desc_strict_prefix_forms_match_guarded_rows()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered composite-index dataset where
    // the reverse bounded window still depends on the same guard and equality
    // prefix before traversing the `handle` suffix.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_221, "alpha", false, "gold", "bramble", 10),
            (9_222, "bravo-user", true, "gold", "bravo", 20),
            (9_223, "bristle-user", true, "gold", "bristle", 30),
            (9_224, "brisk-user", true, "silver", "brisk", 40),
            (9_225, "charlie-user", true, "gold", "charlie", 50),
        ],
    );

    // Phase 2: require strict LIKE, direct STARTS_WITH, and explicit text
    // ranges to return the same reverse guarded equality-prefix window.
    let like_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2",
    )
    .expect("descending filtered composite strict LIKE prefix projection should execute");
    let starts_with_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND STARTS_WITH(handle, 'br') ORDER BY handle DESC, id DESC LIMIT 2",
    )
    .expect("descending filtered composite direct STARTS_WITH projection should execute");
    let range_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND handle >= 'br' AND handle < 'bs' ORDER BY handle DESC, id DESC LIMIT 2",
    )
    .expect("descending filtered composite strict text-range projection should execute");

    let expected_rows = vec![
        vec![
            Value::Text("gold".to_string()),
            Value::Text("bristle".to_string()),
        ],
        vec![
            Value::Text("gold".to_string()),
            Value::Text("bravo".to_string()),
        ],
    ];

    assert_eq!(
        like_rows, expected_rows,
        "descending filtered composite strict LIKE prefix projection must return the guarded reverse equality-prefix rows",
    );
    assert_eq!(
        starts_with_rows, like_rows,
        "descending filtered composite direct STARTS_WITH projection must preserve row parity with strict LIKE",
    );
    assert_eq!(
        range_rows, like_rows,
        "descending filtered composite strict text-range projection must preserve row parity with strict LIKE",
    );
}

#[test]
fn session_explain_execution_filtered_composite_equivalent_strict_prefix_forms_preserve_covering_route()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered composite-index dataset so the
    // guard and equality prefix drive the same bounded `handle` suffix window.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_221, "alpha", false, "gold", "bramble", 10),
            (9_222, "bravo-user", true, "gold", "bravo", 20),
            (9_223, "bristle-user", true, "gold", "bristle", 30),
            (9_224, "brisk-user", true, "silver", "brisk", 40),
            (9_225, "charlie-user", true, "gold", "charlie", 50),
        ],
    );

    // Phase 2: require the three accepted bounded text spellings to preserve
    // one shared composite filtered covering route with a one-slot equality prefix.
    let queries = [
        (
            "filtered composite strict LIKE prefix",
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2",
        ),
        (
            "filtered composite direct STARTS_WITH",
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND STARTS_WITH(handle, 'br') ORDER BY handle ASC, id ASC LIMIT 2",
        ),
        (
            "filtered composite strict text range",
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND handle >= 'br' AND handle < 'bs' ORDER BY handle ASC, id ASC LIMIT 2",
        ),
    ];

    for (context, sql) in queries {
        let descriptor = session
            .query_from_sql::<FilteredIndexedSessionSqlEntity>(sql)
            .unwrap_or_else(|err| panic!("{context} SQL query should lower: {err:?}"))
            .explain_execution()
            .unwrap_or_else(|err| {
                panic!("{context} SQL explain_execution should succeed: {err:?}")
            });

        assert_eq!(
            descriptor.node_type(),
            ExplainExecutionNodeType::IndexRangeScan,
            "{context} queries should stay on the shared composite index-range root",
        );
        assert_eq!(
            descriptor.covering_scan(),
            Some(true),
            "{context} projections should keep the explicit composite covering-read route",
        );
        assert_eq!(
            descriptor.node_properties().get("cov_read_route"),
            Some(&Value::Text("covering_read".to_string())),
            "{context} explain roots should expose the composite covering-read route label",
        );
        assert_eq!(
            descriptor.node_properties().get("prefix_len"),
            Some(&Value::Uint(1)),
            "{context} explain roots should report one equality-prefix slot before the bounded text suffix",
        );
        assert_eq!(
            descriptor.node_properties().get("prefix_values"),
            Some(&Value::List(vec![Value::Text("gold".to_string())])),
            "{context} explain roots should expose the concrete equality-prefix value before the bounded text suffix",
        );
        assert!(
            explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::SecondaryOrderPushdown
            )
            .is_some(),
            "{context} roots should report secondary order pushdown",
        );
        assert!(
            explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::OrderByAccessSatisfied
            )
            .is_some(),
            "{context} roots should report access-satisfied suffix ordering",
        );
    }
}

#[test]
fn session_explain_execution_filtered_composite_equivalent_desc_strict_prefix_forms_preserve_covering_route()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered composite-index dataset so the
    // reverse bounded route still depends on the same guard and equality prefix.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_221, "alpha", false, "gold", "bramble", 10),
            (9_222, "bravo-user", true, "gold", "bravo", 20),
            (9_223, "bristle-user", true, "gold", "bristle", 30),
            (9_224, "brisk-user", true, "silver", "brisk", 40),
            (9_225, "charlie-user", true, "gold", "charlie", 50),
        ],
    );

    // Phase 2: require the reverse bounded text spellings to preserve one
    // shared composite filtered covering route with the same equality prefix.
    let queries = [
        (
            "descending filtered composite strict LIKE prefix",
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2",
        ),
        (
            "descending filtered composite direct STARTS_WITH",
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND STARTS_WITH(handle, 'br') ORDER BY handle DESC, id DESC LIMIT 2",
        ),
        (
            "descending filtered composite strict text range",
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND handle >= 'br' AND handle < 'bs' ORDER BY handle DESC, id DESC LIMIT 2",
        ),
    ];

    for (context, sql) in queries {
        let descriptor = session
            .query_from_sql::<FilteredIndexedSessionSqlEntity>(sql)
            .unwrap_or_else(|err| panic!("{context} SQL query should lower: {err:?}"))
            .explain_execution()
            .unwrap_or_else(|err| {
                panic!("{context} SQL explain_execution should succeed: {err:?}")
            });

        assert_eq!(
            descriptor.node_type(),
            ExplainExecutionNodeType::IndexRangeScan,
            "{context} queries should stay on the shared reverse composite index-range root",
        );
        assert_eq!(
            descriptor.covering_scan(),
            Some(true),
            "{context} projections should keep the explicit reverse composite covering-read route",
        );
        assert_eq!(
            descriptor.node_properties().get("cov_read_route"),
            Some(&Value::Text("covering_read".to_string())),
            "{context} explain roots should expose the reverse composite covering-read route label",
        );
        assert_eq!(
            descriptor.node_properties().get("prefix_len"),
            Some(&Value::Uint(1)),
            "{context} explain roots should report the same equality-prefix slot before reverse bounded traversal",
        );
        assert_eq!(
            descriptor.node_properties().get("prefix_values"),
            Some(&Value::List(vec![Value::Text("gold".to_string())])),
            "{context} explain roots should expose the same concrete equality-prefix value before reverse bounded traversal",
        );
        assert!(
            explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::SecondaryOrderPushdown
            )
            .is_some(),
            "{context} roots should report secondary order pushdown",
        );
        assert!(
            explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::OrderByAccessSatisfied
            )
            .is_some(),
            "{context} roots should report access-satisfied reverse suffix ordering",
        );
    }
}

#[test]
fn execute_sql_projection_filtered_composite_order_only_covering_query_returns_guarded_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered composite-index dataset where
    // the guarded `tier = 'gold'` equality prefix should expose one ordered
    // `handle` suffix window without needing an extra bounded text predicate.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_221, "alpha", false, "gold", "bramble", 10),
            (9_222, "bravo-user", true, "gold", "bravo", 20),
            (9_223, "bristle-user", true, "gold", "bristle", 30),
            (9_224, "brisk-user", true, "silver", "brisk", 40),
            (9_225, "charlie-user", true, "gold", "charlie", 50),
        ],
    );

    // Phase 2: require the projection lane to return only the guarded
    // equality-prefix subset under the `ORDER BY handle, id` suffix shape.
    let sql = "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2";
    let projected_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(&session, sql)
        .expect("filtered composite order-only covering projection query should execute");

    assert_eq!(
        projected_rows,
        vec![
            vec![
                Value::Text("gold".to_string()),
                Value::Text("bravo".to_string()),
            ],
            vec![
                Value::Text("gold".to_string()),
                Value::Text("bristle".to_string()),
            ],
        ],
        "guarded filtered composite order-only queries should return only rows admitted by the equality-prefix filtered window",
    );
}

#[test]
fn execute_sql_projection_filtered_composite_order_only_desc_covering_query_returns_guarded_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered composite-index dataset where
    // reverse traversal still depends on the same guarded `tier = 'gold'`
    // equality prefix before ordering by the `handle` suffix.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_221, "alpha", false, "gold", "bramble", 10),
            (9_222, "bravo-user", true, "gold", "bravo", 20),
            (9_223, "bristle-user", true, "gold", "bristle", 30),
            (9_224, "brisk-user", true, "silver", "brisk", 40),
            (9_225, "charlie-user", true, "gold", "charlie", 50),
        ],
    );

    // Phase 2: require reverse ordered projection rows from the same guarded
    // equality-prefix composite window.
    let sql = "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2";
    let projected_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(&session, sql)
        .expect(
            "descending filtered composite order-only covering projection query should execute",
        );

    assert_eq!(
        projected_rows,
        vec![
            vec![
                Value::Text("gold".to_string()),
                Value::Text("charlie".to_string()),
            ],
            vec![
                Value::Text("gold".to_string()),
                Value::Text("bristle".to_string()),
            ],
        ],
        "descending guarded filtered composite order-only queries should return the reverse equality-prefix window",
    );
}

#[test]
fn session_explain_execution_order_only_filtered_composite_covering_query_uses_index_prefix_access()
{
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered composite-index dataset so
    // EXPLAIN EXECUTION can prove the guarded equality-prefix query uses the
    // composite filtered secondary index instead of materializing rows.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_221, "alpha", false, "gold", "bramble", 10),
            (9_222, "bravo-user", true, "gold", "bravo", 20),
            (9_223, "bristle-user", true, "gold", "bristle", 30),
            (9_224, "brisk-user", true, "silver", "brisk", 40),
            (9_225, "charlie-user", true, "gold", "charlie", 50),
        ],
    );
    // Phase 2: require the guarded composite order-only SQL lane to surface
    // the shared index-prefix covering route with access-satisfied suffix ordering.
    let descriptor = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT id, tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2",
        )
        .expect("filtered composite order-only covering SQL query should lower")
        .explain_execution()
        .expect("filtered composite order-only covering SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexPrefixScan,
        "guarded filtered composite-order queries should stay on the shared index-prefix root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "guarded filtered composite-order coverable projections should keep the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "guarded filtered composite-order explain roots should expose the covering-read route label",
    );
    assert_eq!(
        descriptor.node_properties().get("prefix_len"),
        Some(&Value::Uint(1)),
        "guarded filtered composite-order explain roots should report one equality-prefix slot",
    );
    assert_eq!(
        descriptor.node_properties().get("prefix_values"),
        Some(&Value::List(vec![Value::Text("gold".to_string())])),
        "guarded filtered composite-order explain roots should expose the concrete equality-prefix value",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "guarded filtered composite-order roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "guarded filtered composite-order roots should report access-satisfied suffix ordering",
    );
}

#[test]
fn session_explain_execution_order_only_filtered_composite_desc_covering_query_uses_index_prefix_access()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered composite-index dataset so
    // reverse EXPLAIN EXECUTION can prove the same guarded equality-prefix
    // route instead of a materialized reverse sort.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_221, "alpha", false, "gold", "bramble", 10),
            (9_222, "bravo-user", true, "gold", "bravo", 20),
            (9_223, "bristle-user", true, "gold", "bristle", 30),
            (9_224, "brisk-user", true, "silver", "brisk", 40),
            (9_225, "charlie-user", true, "gold", "charlie", 50),
        ],
    );

    // Phase 2: require the descending guarded composite order-only SQL lane
    // to surface the shared index-prefix covering route.
    let descriptor = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2",
        )
        .expect("descending filtered composite order-only covering SQL query should lower")
        .explain_execution()
        .expect("descending filtered composite order-only covering SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexPrefixScan,
        "descending guarded filtered composite-order queries should stay on the shared index-prefix root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "descending guarded filtered composite-order coverable projections should keep the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "descending guarded filtered composite-order explain roots should expose the covering-read route label",
    );
    assert_eq!(
        descriptor.node_properties().get("prefix_len"),
        Some(&Value::Uint(1)),
        "descending guarded filtered composite-order explain roots should report one equality-prefix slot",
    );
    assert_eq!(
        descriptor.node_properties().get("prefix_values"),
        Some(&Value::List(vec![Value::Text("gold".to_string())])),
        "descending guarded filtered composite-order explain roots should expose the concrete equality-prefix value",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending guarded filtered composite-order roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_none(),
        "descending guarded filtered composite-order roots should fail closed on reverse suffix streaming safety",
    );
}

#[test]
fn session_explain_execution_order_only_filtered_composite_desc_offset_query_stays_on_materialized_boundary()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered composite-index dataset where
    // the `tier = 'gold'` equality prefix keeps the suffix order meaningful
    // even though reverse offset paging must still stay on the shared
    // materialized boundary.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_221, "alpha", false, "gold", "bramble", 10),
            (9_222, "bravo-user", true, "gold", "bravo", 20),
            (9_223, "bristle-user", true, "gold", "bristle", 30),
            (9_224, "brisk-user", true, "silver", "brisk", 40),
            (9_225, "charlie-user", true, "gold", "charlie", 50),
        ],
    );

    // Phase 2: require the descending offset composite filtered order-only
    // shape to keep the index-prefix route but stop before Top-N derivation.
    let descriptor = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2 OFFSET 1",
        )
        .expect("descending filtered composite order-only offset SQL query should lower")
        .explain_execution()
        .expect(
            "descending filtered composite order-only offset SQL explain_execution should succeed",
        );

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexPrefixScan,
        "descending filtered composite order-only offset queries should keep the shared index-prefix root",
    );
    assert_eq!(
        descriptor.execution_mode(),
        crate::db::ExplainExecutionMode::Materialized,
        "descending filtered composite order-only offset queries should stay on the materialized boundary",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "descending filtered composite order-only offset projections should keep the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("prefix_len"),
        Some(&Value::Uint(1)),
        "descending filtered composite order-only offset roots should report one equality-prefix slot",
    );
    assert_eq!(
        descriptor.node_properties().get("prefix_values"),
        Some(&Value::List(vec![Value::Text("gold".to_string())])),
        "descending filtered composite order-only offset roots should expose the concrete equality-prefix value",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending filtered composite order-only offset roots should still report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByMaterializedSort
        )
        .is_some(),
        "descending filtered composite order-only offset roots should stay on the materialized boundary sort contract",
    );
    assert!(
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_none(),
        "descending filtered composite order-only offset roots must not derive Top-N seek",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_none(),
        "descending filtered composite order-only offset roots must not report direct access-satisfied ordering",
    );
    let limit_node = explain_execution_find_first_node(
        &descriptor,
        ExplainExecutionNodeType::LimitOffset,
    )
    .expect("descending filtered composite order-only offset roots should expose one limit node");
    assert_eq!(
        limit_node.node_properties().get("offset"),
        Some(&Value::Uint(1)),
        "descending filtered composite order-only offset roots should expose the retained offset window",
    );
}

#[test]
fn execute_sql_projection_filtered_expression_order_only_covering_query_returns_guarded_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered dataset where the active rows
    // include one mixed-case `handle` value so `ORDER BY LOWER(handle)` has one
    // real expression-ordering contract to preserve.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_231, "alpha", false, "gold", "bramble", 10),
            (9_232, "bravo-user", true, "gold", "bravo", 20),
            (9_233, "bristle-user", true, "gold", "bristle", 30),
            (9_234, "brisk-user", true, "silver", "Brisk", 40),
            (9_235, "charlie-user", true, "gold", "charlie", 50),
        ],
    );

    // Phase 2: require the projection lane to keep the guarded active-only
    // window on the filtered `LOWER(handle)` route.
    let sql = "SELECT id, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 2";
    let projected_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(&session, sql)
        .expect("filtered expression order-only projection query should execute");
    let entity_rows = session
        .execute_sql::<FilteredIndexedSessionSqlEntity>(sql)
        .expect("filtered expression order-only entity query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| {
            vec![
                Value::Ulid(row.id().key()),
                Value::Text(row.entity_ref().handle.clone()),
            ]
        })
        .collect::<Vec<_>>();
    let expected_rows = vec![
        vec![
            Value::Ulid(Ulid::from_u128(9_232)),
            Value::Text("bravo".to_string()),
        ],
        vec![
            Value::Ulid(Ulid::from_u128(9_234)),
            Value::Text("Brisk".to_string()),
        ],
    ];

    assert_eq!(
        entity_projected_rows, expected_rows,
        "guarded filtered expression order-only entity queries should preserve the canonical LOWER(handle) window",
    );
    assert_eq!(
        projected_rows, expected_rows,
        "guarded filtered expression order-only projection queries should preserve the canonical LOWER(handle) window",
    );
}

#[test]
fn execute_sql_projection_filtered_expression_order_only_desc_covering_query_returns_guarded_rows()
{
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed the same mixed-case filtered dataset so reverse
    // `LOWER(handle)` traversal keeps the same guarded route.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_231, "alpha", false, "gold", "bramble", 10),
            (9_232, "bravo-user", true, "gold", "bravo", 20),
            (9_233, "bristle-user", true, "gold", "bristle", 30),
            (9_234, "brisk-user", true, "silver", "Brisk", 40),
            (9_235, "charlie-user", true, "gold", "charlie", 50),
        ],
    );

    // Phase 2: require reverse ordered projection rows from the same guarded
    // filtered expression window.
    let sql = "SELECT id, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true ORDER BY LOWER(handle) DESC, id DESC LIMIT 2";
    let projected_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(&session, sql)
        .expect("descending filtered expression order-only projection query should execute");
    let entity_rows = session
        .execute_sql::<FilteredIndexedSessionSqlEntity>(sql)
        .expect("descending filtered expression order-only entity query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| {
            vec![
                Value::Ulid(row.id().key()),
                Value::Text(row.entity_ref().handle.clone()),
            ]
        })
        .collect::<Vec<_>>();
    let expected_rows = vec![
        vec![
            Value::Ulid(Ulid::from_u128(9_235)),
            Value::Text("charlie".to_string()),
        ],
        vec![
            Value::Ulid(Ulid::from_u128(9_233)),
            Value::Text("bristle".to_string()),
        ],
    ];

    assert_eq!(
        entity_projected_rows, expected_rows,
        "descending guarded filtered expression order-only entity queries should preserve the reverse LOWER(handle) window",
    );
    assert_eq!(
        projected_rows, expected_rows,
        "descending guarded filtered expression order-only projection queries should preserve the reverse LOWER(handle) window",
    );
}

#[test]
fn execute_sql_projection_filtered_expression_equivalent_prefix_forms_match_guarded_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one mixed-case filtered dataset so the casefold
    // `STARTS_WITH(LOWER(handle), ...)` spellings share one real guarded route.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_231, "alpha", false, "gold", "bramble", 10),
            (9_232, "bravo-user", true, "gold", "bravo", 20),
            (9_233, "bristle-user", true, "gold", "bristle", 30),
            (9_234, "brisk-user", true, "silver", "Brisk", 40),
            (9_235, "charlie-user", true, "gold", "charlie", 50),
        ],
    );

    // Phase 2: require the accepted filtered expression prefix spellings to
    // keep one guarded projection result set.
    let like_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT id, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
    )
    .expect("filtered expression LIKE prefix projection should execute");
    let starts_with_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT id, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
    )
    .expect("filtered expression STARTS_WITH projection should execute");
    let range_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT id, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
    )
    .expect("filtered expression text-range projection should execute");
    let entity_rows = session
        .execute_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT id, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect("filtered expression LIKE prefix entity query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| {
            vec![
                Value::Ulid(row.id().key()),
                Value::Text(row.entity_ref().handle.clone()),
            ]
        })
        .collect::<Vec<_>>();
    let expected_rows = vec![
        vec![
            Value::Ulid(Ulid::from_u128(9_232)),
            Value::Text("bravo".to_string()),
        ],
        vec![
            Value::Ulid(Ulid::from_u128(9_234)),
            Value::Text("Brisk".to_string()),
        ],
    ];

    assert_eq!(
        starts_with_rows, like_rows,
        "guarded filtered expression STARTS_WITH and LIKE prefix projections should stay in parity",
    );
    assert_eq!(
        range_rows, like_rows,
        "guarded filtered expression text-range and LIKE prefix projections should stay in parity",
    );
    assert_eq!(
        entity_projected_rows, expected_rows,
        "guarded filtered expression prefix entity queries should preserve the canonical LOWER(handle) window",
    );
    assert_eq!(
        like_rows, expected_rows,
        "guarded filtered expression prefix projection queries should preserve the canonical LOWER(handle) window",
    );
}

#[test]
fn execute_sql_projection_filtered_expression_equivalent_desc_prefix_forms_match_guarded_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed the same mixed-case filtered dataset so reverse casefold
    // prefix traversal stays on the same guarded route.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_231, "alpha", false, "gold", "bramble", 10),
            (9_232, "bravo-user", true, "gold", "bravo", 20),
            (9_233, "bristle-user", true, "gold", "bristle", 30),
            (9_234, "brisk-user", true, "silver", "Brisk", 40),
            (9_235, "charlie-user", true, "gold", "charlie", 50),
        ],
    );

    // Phase 2: require the accepted descending filtered expression prefix
    // spellings to keep one reverse guarded projection result set.
    let like_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT id, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
    )
    .expect("descending filtered expression LIKE prefix projection should execute");
    let starts_with_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT id, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
    )
    .expect("descending filtered expression STARTS_WITH projection should execute");
    let range_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT id, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
    )
    .expect("descending filtered expression text-range projection should execute");
    let entity_rows = session
        .execute_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT id, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        )
        .expect("descending filtered expression LIKE prefix entity query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| {
            vec![
                Value::Ulid(row.id().key()),
                Value::Text(row.entity_ref().handle.clone()),
            ]
        })
        .collect::<Vec<_>>();
    let expected_rows = vec![
        vec![
            Value::Ulid(Ulid::from_u128(9_233)),
            Value::Text("bristle".to_string()),
        ],
        vec![
            Value::Ulid(Ulid::from_u128(9_234)),
            Value::Text("Brisk".to_string()),
        ],
    ];

    assert_eq!(
        starts_with_rows, like_rows,
        "descending guarded filtered expression STARTS_WITH and LIKE prefix projections should stay in parity",
    );
    assert_eq!(
        range_rows, like_rows,
        "descending guarded filtered expression text-range and LIKE prefix projections should stay in parity",
    );
    assert_eq!(
        entity_projected_rows, expected_rows,
        "descending guarded filtered expression prefix entity queries should preserve the reverse LOWER(handle) window",
    );
    assert_eq!(
        like_rows, expected_rows,
        "descending guarded filtered expression prefix projection queries should preserve the reverse LOWER(handle) window",
    );
}

#[test]
fn execute_sql_filtered_expression_index_range_scan_preserves_lower_handle_order() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic filtered dataset whose canonical
    // `LOWER(handle), id` order differs from primary-key order.
    seed_filtered_expression_indexed_session_sql_entities(&session);

    // Phase 2: lower the shared filtered expression-order SQL shape and
    // inspect the raw index-range scan order directly.
    let (entries_in_range_keys, scanned_ids) =
        inspect_filtered_expression_order_only_raw_scan(&session);

    assert_eq!(
        entries_in_range_keys,
        vec![
            (
                StorageKey::Ulid(Ulid::from_u128(9_232)),
                vec![StorageKey::Ulid(Ulid::from_u128(9_232))]
            ),
            (
                StorageKey::Ulid(Ulid::from_u128(9_234)),
                vec![StorageKey::Ulid(Ulid::from_u128(9_234))]
            ),
            (
                StorageKey::Ulid(Ulid::from_u128(9_233)),
                vec![StorageKey::Ulid(Ulid::from_u128(9_233))]
            ),
            (
                StorageKey::Ulid(Ulid::from_u128(9_235)),
                vec![StorageKey::Ulid(Ulid::from_u128(9_235))]
            ),
        ],
        "filtered expression raw bounds must isolate the expression index instead of bleeding into sibling filtered indexes",
    );
    assert_eq!(
        scanned_ids,
        vec![
            Ulid::from_u128(9_232),
            Ulid::from_u128(9_234),
            Ulid::from_u128(9_233),
            Ulid::from_u128(9_235),
        ],
        "raw filtered expression index-range scans must preserve canonical LOWER(handle), id order before later pagination/windowing",
    );
}

#[test]
fn session_explain_execution_order_only_filtered_expression_covering_query_uses_index_range_access()
{
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one mixed-case filtered dataset so EXPLAIN EXECUTION can
    // prove the guarded `LOWER(handle)` order-only route.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_231, "alpha", false, "gold", "bramble", 10),
            (9_232, "bravo-user", true, "gold", "bravo", 20),
            (9_233, "bristle-user", true, "gold", "bristle", 30),
            (9_234, "brisk-user", true, "silver", "Brisk", 40),
            (9_235, "charlie-user", true, "gold", "charlie", 50),
        ],
    );

    // Phase 2: require the guarded filtered expression order-only SQL lane to
    // surface the shared index-range covering route.
    let descriptor = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT id, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect("filtered expression order-only SQL query should lower")
        .explain_execution()
        .expect("filtered expression order-only SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "guarded filtered expression-order queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("materialized".to_string())),
        "guarded filtered expression-order explain roots should expose the materialized route label",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "guarded filtered expression-order roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "guarded filtered expression-order roots should report access-satisfied LOWER(handle) ordering",
    );
}

#[test]
fn session_explain_execution_filtered_expression_prefix_covering_query_uses_index_range_access() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one mixed-case filtered dataset so EXPLAIN EXECUTION can
    // prove the guarded casefold prefix route stays on the same expression index.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_231, "alpha", false, "gold", "bramble", 10),
            (9_232, "bravo-user", true, "gold", "bravo", 20),
            (9_233, "bristle-user", true, "gold", "bristle", 30),
            (9_234, "brisk-user", true, "silver", "Brisk", 40),
            (9_235, "charlie-user", true, "gold", "charlie", 50),
        ],
    );

    // Phase 2: require the guarded filtered expression prefix SQL lane to
    // surface the shared index-range covering route and access-satisfied order.
    let descriptor = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT id, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect("filtered expression prefix SQL query should lower")
        .explain_execution()
        .expect("filtered expression prefix SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "guarded filtered expression-prefix queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("materialized".to_string())),
        "guarded filtered expression-prefix explain roots should expose the materialized route label",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "guarded filtered expression-prefix roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "guarded filtered expression-prefix roots should report access-satisfied LOWER(handle) ordering",
    );
}

#[test]
fn session_explain_execution_filtered_expression_text_range_query_uses_index_range_access() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed the guarded mixed-case dataset so EXPLAIN EXECUTION can
    // prove explicit casefold bounds stay on the same expression index route.
    seed_filtered_expression_indexed_session_sql_entities(&session);

    // Phase 2: require the guarded filtered expression text-range lane to stay
    // on the shared index-range root with access-satisfied ordering.
    let descriptor = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT id, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect("filtered expression text-range SQL query should lower")
        .explain_execution()
        .expect("filtered expression text-range SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "guarded filtered expression text-range queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("materialized".to_string())),
        "guarded filtered expression text-range explain roots should expose the materialized route label",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "guarded filtered expression text-range roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "guarded filtered expression text-range roots should report access-satisfied LOWER(handle) ordering",
    );
}

#[test]
fn execute_sql_projection_filtered_composite_expression_order_only_query_returns_guarded_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed the canonical mixed-case filtered expression dataset so
    // the guarded `tier = 'gold'` window traverses one `LOWER(handle)` suffix.
    seed_filtered_expression_indexed_session_sql_entities(&session);

    // Phase 2: require the projection lane to keep the guarded equality-prefix
    // window on the filtered composite `tier, LOWER(handle)` route.
    let sql = "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2";
    let projected_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(&session, sql)
        .expect("filtered composite expression order-only projection query should execute");

    assert_eq!(
        projected_rows,
        vec![
            vec![
                Value::Text("gold".to_string()),
                Value::Text("bravo".to_string()),
            ],
            vec![
                Value::Text("gold".to_string()),
                Value::Text("bristle".to_string()),
            ],
        ],
        "guarded filtered composite expression order-only projections should preserve the canonical LOWER(handle) suffix window",
    );
}

#[test]
fn execute_sql_projection_filtered_composite_expression_order_only_pagination_matches_entity_rows()
{
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one larger mixed-case filtered dataset so the guarded
    // composite expression route has enough admitted rows to exercise several
    // LIMIT/OFFSET windows while still carrying inactive and wrong-tier noise.
    seed_filtered_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_281, "amber-user", true, "gold", "Amber", 10),
            (9_282, "alpha-user", false, "gold", "alpha", 20),
            (9_283, "bravo-user", true, "gold", "bravo", 30),
            (9_284, "bravo-shadow", true, "silver", "Bravo", 40),
            (9_285, "charlie-user", true, "gold", "CHARLIE", 50),
            (9_286, "delta-user", true, "gold", "delta", 60),
            (9_287, "echo-user", true, "gold", "Echo", 70),
            (9_288, "foxtrot-user", false, "gold", "foxtrot", 80),
            (9_289, "golf-user", true, "gold", "golf", 90),
            (9_290, "hotel-user", true, "gold", "Hotel", 100),
            (9_291, "india-user", true, "gold", "india", 110),
        ],
    );

    // Phase 2: derive the canonical full ordered entity result so each paged
    // projection window can be checked against the same structural order.
    let base_sql = "SELECT id, tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC";
    let full_entity_rows = session
        .execute_sql::<FilteredIndexedSessionSqlEntity>(base_sql)
        .expect("filtered composite expression baseline entity query should execute");
    let expected_projected_rows = full_entity_rows
        .iter()
        .map(|row| {
            vec![
                Value::Ulid(row.id().key()),
                Value::Text(row.entity_ref().tier.clone()),
                Value::Text(row.entity_ref().handle.clone()),
            ]
        })
        .collect::<Vec<_>>();

    assert_eq!(
        expected_projected_rows.len(),
        8,
        "the guarded composite expression fixture should admit eight ordered gold rows",
    );

    // Phase 3: compare several paged windows against both the entity lane and
    // the expected ordered prefix so pagination cannot silently skip or repeat rows.
    let mut concatenated_projection_pages = Vec::new();
    for (offset, limit) in [(0_u64, 3_u64), (3, 3), (6, 3)] {
        let paged_sql = format!("{base_sql} LIMIT {limit} OFFSET {offset}");
        let projected_rows =
            dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(&session, &paged_sql)
                .expect("filtered composite expression paged projection query should execute");
        let paged_entity_rows = session
            .execute_sql::<FilteredIndexedSessionSqlEntity>(&paged_sql)
            .expect("filtered composite expression paged entity query should execute");
        let expected_page = paged_entity_rows
            .iter()
            .map(|row| {
                vec![
                    Value::Ulid(row.id().key()),
                    Value::Text(row.entity_ref().tier.clone()),
                    Value::Text(row.entity_ref().handle.clone()),
                ]
            })
            .collect::<Vec<_>>();

        assert_eq!(
            projected_rows, expected_page,
            "paged composite expression projections must match the entity lane for LIMIT {limit} OFFSET {offset}",
        );

        concatenated_projection_pages.extend(projected_rows);
    }

    assert_eq!(
        concatenated_projection_pages, expected_projected_rows,
        "concatenated paged composite expression projections must preserve the canonical ordered guarded result set without missing or repeated rows",
    );
}

#[test]
fn execute_sql_projection_filtered_composite_expression_order_only_desc_query_returns_guarded_rows()
{
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: reuse the same guarded mixed-case dataset so reverse
    // `LOWER(handle)` traversal keeps the same equality-prefix route.
    seed_filtered_expression_indexed_session_sql_entities(&session);

    // Phase 2: require the reverse projection lane to keep the guarded
    // `tier = 'gold'` equality-prefix window on the same composite route.
    let sql = "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2";
    let projected_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(&session, sql)
        .expect(
            "descending filtered composite expression order-only projection query should execute",
        );

    assert_eq!(
        projected_rows,
        vec![
            vec![
                Value::Text("gold".to_string()),
                Value::Text("charlie".to_string()),
            ],
            vec![
                Value::Text("gold".to_string()),
                Value::Text("bristle".to_string()),
            ],
        ],
        "descending guarded filtered composite expression order-only projections should preserve the reverse LOWER(handle) suffix window",
    );
}

#[test]
fn execute_sql_projection_filtered_composite_expression_equivalent_prefix_forms_match_guarded_rows()
{
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed the canonical mixed-case filtered dataset so the guarded
    // equality prefix and casefolded suffix range share one real route.
    seed_filtered_expression_indexed_session_sql_entities(&session);

    // Phase 2: require the accepted bounded casefold spellings to keep one
    // guarded equality-prefix projection result set.
    let like_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
    )
    .expect("filtered composite expression LIKE prefix projection should execute");
    let starts_with_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
    )
    .expect("filtered composite expression STARTS_WITH projection should execute");
    let range_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
    )
    .expect("filtered composite expression text-range projection should execute");

    assert_eq!(
        starts_with_rows, like_rows,
        "guarded filtered composite expression STARTS_WITH and LIKE prefix projections should stay in parity",
    );
    assert_eq!(
        range_rows, like_rows,
        "guarded filtered composite expression text-range and LIKE prefix projections should stay in parity",
    );
    assert_eq!(
        like_rows,
        vec![
            vec![
                Value::Text("gold".to_string()),
                Value::Text("bravo".to_string()),
            ],
            vec![
                Value::Text("gold".to_string()),
                Value::Text("bristle".to_string()),
            ],
        ],
        "guarded filtered composite expression prefix projections should preserve the canonical LOWER(handle) equality-prefix window",
    );
}

#[test]
fn execute_sql_projection_filtered_composite_expression_equivalent_desc_prefix_forms_match_guarded_rows()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed the same guarded mixed-case dataset so reverse casefold
    // prefix traversal stays on the same equality-prefix route.
    seed_filtered_expression_indexed_session_sql_entities(&session);

    // Phase 2: require the accepted descending bounded casefold spellings to
    // keep one reverse guarded equality-prefix result set.
    let like_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
    )
    .expect("descending filtered composite expression LIKE prefix projection should execute");
    let starts_with_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
    )
    .expect("descending filtered composite expression STARTS_WITH projection should execute");
    let range_rows = dispatch_projection_rows::<FilteredIndexedSessionSqlEntity>(
        &session,
        "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
    )
    .expect("descending filtered composite expression text-range projection should execute");

    assert_eq!(
        starts_with_rows, like_rows,
        "descending guarded filtered composite expression STARTS_WITH and LIKE prefix projections should stay in parity",
    );
    assert_eq!(
        range_rows, like_rows,
        "descending guarded filtered composite expression text-range and LIKE prefix projections should stay in parity",
    );
    assert_eq!(
        like_rows,
        vec![
            vec![
                Value::Text("gold".to_string()),
                Value::Text("bristle".to_string()),
            ],
            vec![
                Value::Text("gold".to_string()),
                Value::Text("bravo".to_string()),
            ],
        ],
        "descending guarded filtered composite expression prefix projections should preserve the reverse LOWER(handle) equality-prefix window",
    );
}

#[test]
fn session_explain_execution_filtered_composite_expression_order_only_query_uses_index_range_access()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed the guarded mixed-case dataset so EXPLAIN EXECUTION can
    // prove the filtered composite `tier, LOWER(handle)` order-only route.
    seed_filtered_expression_indexed_session_sql_entities(&session);

    // Phase 2: require the guarded equality-prefix composite expression lane
    // to stay on one shared index-prefix root with materialized projection rows.
    let descriptor = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect("filtered composite expression order-only SQL query should lower")
        .explain_execution()
        .expect("filtered composite expression order-only SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexPrefixScan,
        "guarded filtered composite expression order-only queries should stay on the shared index-prefix root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(false),
        "guarded filtered composite expression projections should materialize original handle values instead of claiming a covering route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("materialized".to_string())),
        "guarded filtered composite expression explain roots should expose the materialized route label",
    );
    assert_eq!(
        descriptor.node_properties().get("prefix_len"),
        Some(&Value::Uint(1)),
        "guarded filtered composite expression explain roots should report one equality-prefix slot",
    );
    assert_eq!(
        descriptor.node_properties().get("prefix_values"),
        Some(&Value::List(vec![Value::Text("gold".to_string())])),
        "guarded filtered composite expression explain roots should expose the concrete equality-prefix value",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "guarded filtered composite expression roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "guarded filtered composite expression roots should report access-satisfied LOWER(handle) ordering",
    );
}

#[test]
fn session_explain_execution_filtered_composite_expression_prefix_query_uses_index_range_access() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed the guarded mixed-case dataset so EXPLAIN EXECUTION can
    // prove the filtered composite bounded `LOWER(handle)` suffix route.
    seed_filtered_expression_indexed_session_sql_entities(&session);

    // Phase 2: require the guarded equality-prefix composite expression prefix
    // lane to preserve the same shared index-range root.
    let descriptor = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect("filtered composite expression prefix SQL query should lower")
        .explain_execution()
        .expect("filtered composite expression prefix SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "guarded filtered composite expression-prefix queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(false),
        "guarded filtered composite expression-prefix projections should materialize original handle values instead of claiming a covering route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("materialized".to_string())),
        "guarded filtered composite expression-prefix explain roots should expose the materialized route label",
    );
    assert_eq!(
        descriptor.node_properties().get("prefix_len"),
        Some(&Value::Uint(1)),
        "guarded filtered composite expression-prefix explain roots should report one equality-prefix slot",
    );
    assert_eq!(
        descriptor.node_properties().get("prefix_values"),
        Some(&Value::List(vec![Value::Text("gold".to_string())])),
        "guarded filtered composite expression-prefix explain roots should expose the concrete equality-prefix value",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "guarded filtered composite expression-prefix roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "guarded filtered composite expression-prefix roots should report access-satisfied LOWER(handle) ordering",
    );
}

#[test]
fn session_explain_execution_filtered_composite_expression_prefix_key_only_keeps_bounded_route_parity()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed the guarded mixed-case dataset so both projection shapes
    // share the same filtered composite expression route family.
    seed_filtered_expression_indexed_session_sql_entities(&session);

    // Phase 2: collect the fuller materialized sibling and the narrower
    // key-only covering sibling from the same guarded prefix shape.
    let full_descriptor = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect("filtered composite expression prefix full-projection SQL query should lower")
        .explain_execution()
        .expect(
            "filtered composite expression prefix full-projection SQL explain_execution should succeed",
        );
    let key_only_descriptor = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT id, tier FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect("filtered composite expression prefix key-only SQL query should lower")
        .explain_execution()
        .expect(
            "filtered composite expression prefix key-only SQL explain_execution should succeed",
        );

    // Phase 3: require the shared bounded route contract to stay in parity
    // even though the projection surface differs.
    assert_eq!(
        full_descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "filtered composite expression prefix full-projection roots should stay on the shared index-range root",
    );
    assert_eq!(
        key_only_descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "filtered composite expression prefix key-only roots should stay on the shared index-range root",
    );
    assert_eq!(
        full_descriptor.node_properties().get("prefix_len"),
        key_only_descriptor.node_properties().get("prefix_len"),
        "filtered composite expression prefix siblings should keep the same equality-prefix arity",
    );
    assert_eq!(
        full_descriptor.node_properties().get("prefix_values"),
        key_only_descriptor.node_properties().get("prefix_values"),
        "filtered composite expression prefix siblings should keep the same equality-prefix value contract",
    );
    assert_eq!(
        full_descriptor.node_properties().get("fetch"),
        key_only_descriptor.node_properties().get("fetch"),
        "filtered composite expression prefix siblings should keep the same bounded fetch contract at the scan root",
    );
    assert_eq!(
        full_descriptor.node_properties().get("fast_path"),
        key_only_descriptor.node_properties().get("fast_path"),
        "filtered composite expression prefix siblings should keep the same fast-path route label",
    );
    assert_eq!(
        full_descriptor.node_properties().get("fast_reason"),
        key_only_descriptor.node_properties().get("fast_reason"),
        "filtered composite expression prefix siblings should keep the same fast-path eligibility reason",
    );
    assert_eq!(
        explain_execution_find_first_node(&full_descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_some(),
        explain_execution_find_first_node(&key_only_descriptor, ExplainExecutionNodeType::TopNSeek)
            .is_some(),
        "filtered composite expression prefix siblings should either both derive Top-N seek or both fail closed before it",
    );
    assert_eq!(
        explain_execution_find_first_node(
            &full_descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_some(),
        explain_execution_find_first_node(
            &key_only_descriptor,
            ExplainExecutionNodeType::IndexRangeLimitPushdown
        )
        .is_some(),
        "filtered composite expression prefix siblings should either both derive index-range limit pushdown or both stay off that fast path",
    );
    assert_eq!(
        explain_execution_find_first_node(
            &full_descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        explain_execution_find_first_node(
            &key_only_descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "filtered composite expression prefix siblings should keep the same access-satisfied ordering contract",
    );
}

#[test]
fn execute_sql_dispatch_filtered_composite_expression_prefix_key_only_keeps_trace_scan_parity() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session().debug();

    // Phase 1: seed the guarded mixed-case dataset so both projection shapes
    // execute against the same filtered composite expression route.
    seed_filtered_expression_indexed_session_sql_entities(&session);

    // Phase 2: execute the fuller materialized sibling and the narrower
    // key-only covering sibling with trace enabled.
    let full_query = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect("filtered composite expression prefix full-projection SQL query should lower");
    let key_only_query = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT id, tier FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect("filtered composite expression prefix key-only SQL query should lower");
    let full_execution = session
        .execute_load_query_paged_with_trace(&full_query, None)
        .expect(
            "filtered composite expression prefix full-projection traced execution should succeed",
        );
    let key_only_execution = session
        .execute_load_query_paged_with_trace(&key_only_query, None)
        .expect("filtered composite expression prefix key-only traced execution should succeed");
    let full_trace = full_execution
        .execution_trace()
        .expect("filtered composite expression prefix full-projection execution should emit trace");
    let key_only_trace = key_only_execution
        .execution_trace()
        .expect("filtered composite expression prefix key-only execution should emit trace");

    // Phase 3: require the narrowed projection not to widen access traversal
    // or lose the same coarse optimization label.
    assert_eq!(
        full_trace.optimization(),
        key_only_trace.optimization(),
        "filtered composite expression prefix siblings should keep the same coarse execution optimization label",
    );
    assert_eq!(
        full_trace.keys_scanned(),
        key_only_trace.keys_scanned(),
        "filtered composite expression prefix siblings should scan the same bounded key count",
    );
}

#[test]
fn session_explain_execution_filtered_composite_expression_text_range_query_uses_index_range_access()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed the guarded mixed-case dataset so EXPLAIN EXECUTION can
    // prove explicit casefold bounds keep the equality-prefix expression route.
    seed_filtered_expression_indexed_session_sql_entities(&session);

    // Phase 2: require the guarded equality-prefix composite expression
    // text-range lane to preserve the shared index-range root.
    let descriptor = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT tier, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect("filtered composite expression text-range SQL query should lower")
        .explain_execution()
        .expect("filtered composite expression text-range SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "guarded filtered composite expression text-range queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(false),
        "guarded filtered composite expression text-range projections should materialize original handle values instead of claiming a covering route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("materialized".to_string())),
        "guarded filtered composite expression text-range explain roots should expose the materialized route label",
    );
    assert_eq!(
        descriptor.node_properties().get("prefix_len"),
        Some(&Value::Uint(1)),
        "guarded filtered composite expression text-range explain roots should report one equality-prefix slot",
    );
    assert_eq!(
        descriptor.node_properties().get("prefix_values"),
        Some(&Value::List(vec![Value::Text("gold".to_string())])),
        "guarded filtered composite expression text-range explain roots should expose the concrete equality-prefix value",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "guarded filtered composite expression text-range roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "guarded filtered composite expression text-range roots should report access-satisfied LOWER(handle) ordering",
    );
}

#[test]
fn session_explain_execution_filtered_composite_expression_key_only_strict_text_range_query_uses_covering_read_route()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed the guarded mixed-case dataset so the key-only bounded
    // sibling can prove true covering eligibility on the same equality-prefix
    // expression route.
    seed_filtered_expression_indexed_session_sql_entities(&session);

    // Phase 2: require the session-backed query-builder explain to reuse the
    // planner-proven covering-read route for the narrower `(id, tier)` projection.
    let descriptor = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT id, tier FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect("filtered composite expression key-only strict text-range SQL query should lower")
        .explain_execution()
        .expect(
            "filtered composite expression key-only strict text-range SQL explain_execution should succeed",
        );

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "guarded filtered composite expression key-only strict text-range queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "guarded filtered composite expression key-only strict text-range queries should expose the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "guarded filtered composite expression key-only strict text-range explain roots should expose the covering-read route label",
    );
    assert_eq!(
        descriptor.node_properties().get("prefix_len"),
        Some(&Value::Uint(1)),
        "guarded filtered composite expression key-only strict text-range explain roots should report one equality-prefix slot",
    );
    assert_eq!(
        descriptor.node_properties().get("prefix_values"),
        Some(&Value::List(vec![Value::Text("gold".to_string())])),
        "guarded filtered composite expression key-only strict text-range explain roots should expose the concrete equality-prefix value",
    );
    let projection_node =
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::CoveringRead)
            .expect(
                "guarded filtered composite expression key-only strict text-range explain should emit a covering-read node",
            );
    assert_eq!(
        projection_node.node_properties().get("covering_fields"),
        Some(&Value::List(vec![
            Value::Text("id".to_string()),
            Value::Text("tier".to_string()),
        ])),
        "guarded filtered composite expression key-only strict text-range covering nodes should expose the projected field list",
    );
    assert_eq!(
        projection_node.node_properties().get("existing_row_mode"),
        Some(&Value::Text("planner_proven".to_string())),
        "session-backed filtered composite expression key-only strict text-range explain should inherit the planner-proven covering mode",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "guarded filtered composite expression key-only strict text-range explain roots should report access-satisfied LOWER(handle) ordering",
    );
}

#[test]
fn session_explain_execution_filtered_composite_expression_key_only_strict_text_range_desc_query_uses_covering_read_route()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed the guarded mixed-case dataset so the descending bounded
    // sibling stays on the same honest filtered composite covering family.
    seed_filtered_expression_indexed_session_sql_entities(&session);

    // Phase 2: require the session-backed query-builder explain to reuse the
    // planner-proven reverse covering-read route for the narrower projection.
    let descriptor = session
        .query_from_sql::<FilteredIndexedSessionSqlEntity>(
            "SELECT id, tier FROM FilteredIndexedSessionSqlEntity WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        )
        .expect(
            "descending filtered composite expression key-only strict text-range SQL query should lower",
        )
        .explain_execution()
        .expect(
            "descending filtered composite expression key-only strict text-range SQL explain_execution should succeed",
        );

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "descending guarded filtered composite expression key-only strict text-range queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "descending guarded filtered composite expression key-only strict text-range queries should expose the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "descending guarded filtered composite expression key-only strict text-range explain roots should expose the covering-read route label",
    );
    assert_eq!(
        descriptor.node_properties().get("prefix_len"),
        Some(&Value::Uint(1)),
        "descending guarded filtered composite expression key-only strict text-range explain roots should report one equality-prefix slot",
    );
    assert_eq!(
        descriptor.node_properties().get("prefix_values"),
        Some(&Value::List(vec![Value::Text("gold".to_string())])),
        "descending guarded filtered composite expression key-only strict text-range explain roots should expose the concrete equality-prefix value",
    );
    let projection_node = explain_execution_find_first_node(
        &descriptor,
        ExplainExecutionNodeType::CoveringRead,
    )
    .expect(
        "descending guarded filtered composite expression key-only strict text-range explain should emit a covering-read node",
    );
    assert_eq!(
        projection_node.node_properties().get("covering_fields"),
        Some(&Value::List(vec![
            Value::Text("id".to_string()),
            Value::Text("tier".to_string()),
        ])),
        "descending guarded filtered composite expression key-only strict text-range covering nodes should expose the projected field list",
    );
    assert_eq!(
        projection_node.node_properties().get("existing_row_mode"),
        Some(&Value::Text("planner_proven".to_string())),
        "descending session-backed filtered composite expression key-only strict text-range explain should inherit the planner-proven covering mode",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "descending guarded filtered composite expression key-only strict text-range explain roots should report access-satisfied LOWER(handle) ordering",
    );
}

#[test]
fn session_explain_execution_order_only_composite_covering_query_uses_index_range_access() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic composite-index dataset so the SQL lane
    // can prove planner-selected order-only access on the live `(code, serial)` index.
    seed_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_221_u128, "alpha", 2),
            (9_222, "alpha", 1),
            (9_223, "beta", 1),
        ],
    );

    // Phase 2: require EXPLAIN EXECUTION to surface the shared order-only
    // composite index-range root and covering-read route.
    let descriptor = session
        .query_from_sql::<CompositeIndexedSessionSqlEntity>(
            "SELECT id, code, serial FROM CompositeIndexedSessionSqlEntity ORDER BY code ASC, serial ASC, id ASC LIMIT 2",
        )
        .expect("composite order-only covering SQL query should lower")
        .explain_execution()
        .expect("composite order-only covering SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "order-only composite secondary queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "order-only composite coverable projections should keep the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "order-only composite explain roots should expose the covering-read route label",
    );
    let projection_node =
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::CoveringRead)
            .expect("order-only composite explain tree should emit a covering-read node");
    assert_eq!(
        projection_node.node_properties().get("existing_row_mode"),
        Some(&Value::Text("planner_proven".to_string())),
        "session-backed order-only composite covering nodes should inherit the planner-proven covering mode",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "order-only composite index-range roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "order-only composite index-range roots should report access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_order_only_composite_desc_covering_query_uses_index_range_access() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic composite-index dataset so the SQL lane
    // can prove planner-selected descending order-only access on the live
    // `(code, serial)` index instead of materializing a reverse sort.
    seed_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_231_u128, "alpha", 2),
            (9_232, "alpha", 1),
            (9_233, "beta", 1),
        ],
    );

    // Phase 2: require EXPLAIN EXECUTION to surface the shared descending
    // order-only composite index-range root and covering-read route.
    let descriptor = session
        .query_from_sql::<CompositeIndexedSessionSqlEntity>(
            "SELECT id, code, serial FROM CompositeIndexedSessionSqlEntity ORDER BY code DESC, serial DESC, id DESC LIMIT 2",
        )
        .expect("descending composite order-only covering SQL query should lower")
        .explain_execution()
        .expect("descending composite order-only covering SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "descending order-only composite secondary queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "descending order-only composite coverable projections should keep the explicit covering-read route",
    );
    let projection_node =
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::CoveringRead)
            .expect(
                "descending order-only composite explain tree should emit a covering-read node",
            );
    assert_eq!(
        projection_node.node_properties().get("existing_row_mode"),
        Some(&Value::Text("planner_proven".to_string())),
        "descending session-backed order-only composite covering nodes should inherit the planner-proven covering mode",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "descending order-only composite explain roots should expose the covering-read route label",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "descending order-only composite index-range roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "descending order-only composite index-range roots should report access-satisfied ordering",
    );
}

#[test]
fn execute_sql_projection_expression_order_query_matches_entity_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic mixed-case dataset so expression order
    // semantics disagree with primary-key order instead of accidentally
    // matching one tie-break-only fallback.
    seed_expression_indexed_session_sql_entities(
        &session,
        &[
            (9_243_u128, "sam", 10),
            (9_244, "Alex", 20),
            (9_241, "bob", 30),
            (9_242, "zoe", 40),
        ],
    );

    // Phase 2: verify the projection lane keeps the same `LOWER(name), id`
    // ordering contract as the entity lane and the explicit expected window on
    // the matching expression index.
    let sql = "SELECT id, name FROM ExpressionIndexedSessionSqlEntity ORDER BY LOWER(name) ASC, id ASC LIMIT 2";
    let projected_rows =
        dispatch_projection_rows::<ExpressionIndexedSessionSqlEntity>(&session, sql)
            .expect("expression-order projection query should execute");
    let entity_rows = session
        .execute_sql::<ExpressionIndexedSessionSqlEntity>(sql)
        .expect("expression-order entity query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| {
            vec![
                Value::Ulid(row.id().key()),
                Value::Text(row.entity_ref().name.clone()),
            ]
        })
        .collect::<Vec<_>>();
    let expected_rows = vec![
        vec![
            Value::Ulid(Ulid::from_u128(9_244)),
            Value::Text("Alex".to_string()),
        ],
        vec![
            Value::Ulid(Ulid::from_u128(9_241)),
            Value::Text("bob".to_string()),
        ],
    ];

    assert_eq!(
        entity_projected_rows, expected_rows,
        "entity execution must honor the LOWER(name), id ordering contract",
    );
    assert_eq!(
        projected_rows, expected_rows,
        "projection execution must honor the LOWER(name), id ordering contract",
    );
}

#[test]
fn execute_sql_expression_order_index_range_scan_preserves_lower_name_order() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic mixed-case dataset whose primary-key
    // order disagrees with canonical `LOWER(name), id` traversal.
    seed_expression_indexed_session_sql_entities(
        &session,
        &[
            (9_243_u128, "sam", 10),
            (9_244, "Alex", 20),
            (9_241, "bob", 30),
            (9_242, "zoe", 40),
        ],
    );

    // Phase 2: lower the expression-order SQL shape to its shared index-range
    // access contract and inspect the raw index scan order directly.
    let plan = session
        .query_from_sql::<ExpressionIndexedSessionSqlEntity>(
            "SELECT id, name FROM ExpressionIndexedSessionSqlEntity ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
        )
        .expect("expression-order SQL query should lower")
        .plan()
        .expect("expression-order SQL query should plan")
        .into_inner();
    let lowered_specs =
        lower_index_range_specs(ExpressionIndexedSessionSqlEntity::ENTITY_TAG, &plan.access)
            .expect("expression-order access plan should lower to one raw index range");
    let [spec] = lowered_specs.as_slice() else {
        panic!("expression-order access plan should use exactly one index-range spec");
    };
    let store = INDEXED_SESSION_SQL_DB
        .recovered_store(IndexedSessionSqlStore::PATH)
        .expect("expression-order indexed store should recover");
    let keys = store
        .with_index(|index_store| {
            index_store.resolve_data_values_in_raw_range_limited(
                ExpressionIndexedSessionSqlEntity::ENTITY_TAG,
                spec.index(),
                (spec.lower(), spec.upper()),
                IndexScanContinuationInput::new(None, Direction::Asc),
                3,
                None,
            )
        })
        .expect("expression-order index range scan should succeed");
    let scanned_ids = keys
        .into_iter()
        .map(|key: DataKey| match key.storage_key() {
            StorageKey::Ulid(id) => id,
            other => {
                panic!("expression-order fixture keys should stay on ULID primary keys: {other:?}")
            }
        })
        .collect::<Vec<_>>();

    assert_eq!(
        scanned_ids,
        vec![
            Ulid::from_u128(9_244),
            Ulid::from_u128(9_241),
            Ulid::from_u128(9_243),
        ],
        "raw expression-index range scans must preserve the canonical LOWER(name), id order before later pagination/windowing",
    );
}

#[test]
fn execute_sql_projection_expression_order_desc_query_matches_entity_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: reuse one deterministic mixed-case dataset whose primary-key
    // order disagrees with reverse expression order.
    seed_expression_indexed_session_sql_entities(
        &session,
        &[
            (9_243_u128, "sam", 10),
            (9_244, "Alex", 20),
            (9_241, "bob", 30),
            (9_242, "zoe", 40),
        ],
    );

    // Phase 2: verify descending expression order stays explicit on both the
    // projection and entity lanes.
    let sql = "SELECT id, name FROM ExpressionIndexedSessionSqlEntity ORDER BY LOWER(name) DESC, id DESC LIMIT 2";
    let projected_rows =
        dispatch_projection_rows::<ExpressionIndexedSessionSqlEntity>(&session, sql)
            .expect("descending expression-order projection query should execute");
    let entity_rows = session
        .execute_sql::<ExpressionIndexedSessionSqlEntity>(sql)
        .expect("descending expression-order entity query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| {
            vec![
                Value::Ulid(row.id().key()),
                Value::Text(row.entity_ref().name.clone()),
            ]
        })
        .collect::<Vec<_>>();
    let expected_rows = vec![
        vec![
            Value::Ulid(Ulid::from_u128(9_242)),
            Value::Text("zoe".to_string()),
        ],
        vec![
            Value::Ulid(Ulid::from_u128(9_243)),
            Value::Text("sam".to_string()),
        ],
    ];

    assert_eq!(
        entity_projected_rows, expected_rows,
        "descending entity execution must honor the LOWER(name), id ordering contract",
    );
    assert_eq!(
        projected_rows, expected_rows,
        "descending projection execution must honor the LOWER(name), id ordering contract",
    );
}

#[test]
fn session_explain_execution_order_only_expression_query_uses_index_range_access() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic mixed-case dataset so EXPLAIN EXECUTION
    // can prove the expression-index route instead of a materialized fallback.
    seed_expression_indexed_session_sql_entities(
        &session,
        &[
            (9_251_u128, "sam", 10),
            (9_252, "Alex", 20),
            (9_253, "bob", 30),
        ],
    );

    // Phase 2: require EXPLAIN EXECUTION to surface the shared order-only
    // expression index-range root and access-satisfied ordering markers.
    let descriptor = session
        .query_from_sql::<ExpressionIndexedSessionSqlEntity>(
            "SELECT id, name FROM ExpressionIndexedSessionSqlEntity ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
        )
        .expect("expression order-only SQL query should lower")
        .explain_execution()
        .expect("expression order-only SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "expression order-only queries should stay on the shared index-range root",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::SecondaryOrderPushdown
        )
        .is_some(),
        "expression order-only index-range roots should report secondary order pushdown",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "expression order-only index-range roots should report access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_order_only_expression_key_only_query_uses_covering_read_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic mixed-case dataset so the key-only
    // expression-order sibling can prove true covering eligibility without
    // claiming original `name` reconstruction from the lowered key.
    seed_expression_indexed_session_sql_entities(
        &session,
        &[
            (9_261_u128, "sam", 10),
            (9_262, "Alex", 20),
            (9_263, "bob", 30),
        ],
    );

    // Phase 2: require the session-backed query-builder explain to reuse the
    // planner-proven covering-read route for the `id`-only projection.
    let descriptor = session
        .query_from_sql::<ExpressionIndexedSessionSqlEntity>(
            "SELECT id FROM ExpressionIndexedSessionSqlEntity ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
        )
        .expect("expression key-only order SQL query should lower")
        .explain_execution()
        .expect("expression key-only order SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "expression key-only order queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "expression key-only order queries should expose the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "expression key-only order explain roots should expose the covering-read route label",
    );
    let projection_node =
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::CoveringRead)
            .expect("expression key-only order explain tree should emit a covering-read node");
    assert_eq!(
        projection_node.node_properties().get("covering_fields"),
        Some(&Value::List(vec![Value::Text("id".to_string())])),
        "expression key-only order covering nodes should expose the projected field list",
    );
    assert_eq!(
        projection_node.node_properties().get("existing_row_mode"),
        Some(&Value::Text("planner_proven".to_string())),
        "session-backed expression key-only order explain should inherit the planner-proven covering mode",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "expression key-only order explain roots should report access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_order_only_expression_key_only_desc_query_uses_covering_read_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic mixed-case dataset so the descending
    // key-only expression-order sibling stays on the same honest covering
    // family.
    seed_expression_indexed_session_sql_entities(
        &session,
        &[
            (9_264_u128, "sam", 10),
            (9_265, "Alex", 20),
            (9_266, "bob", 30),
        ],
    );

    // Phase 2: require the session-backed query-builder explain to surface the
    // covering route and the planner-proven existing-row mode.
    let descriptor = session
        .query_from_sql::<ExpressionIndexedSessionSqlEntity>(
            "SELECT id FROM ExpressionIndexedSessionSqlEntity ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
        )
        .expect("descending expression key-only order SQL query should lower")
        .explain_execution()
        .expect("descending expression key-only order SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "descending expression key-only order queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "descending expression key-only order queries should expose the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "descending expression key-only order explain roots should expose the covering-read route label",
    );
    let projection_node = explain_execution_find_first_node(
        &descriptor,
        ExplainExecutionNodeType::CoveringRead,
    )
    .expect("descending expression key-only order explain tree should emit a covering-read node");
    assert_eq!(
        projection_node.node_properties().get("covering_fields"),
        Some(&Value::List(vec![Value::Text("id".to_string())])),
        "descending expression key-only order covering nodes should expose the projected field list",
    );
    assert_eq!(
        projection_node.node_properties().get("existing_row_mode"),
        Some(&Value::Text("planner_proven".to_string())),
        "descending session-backed expression key-only order explain should inherit the planner-proven covering mode",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "descending expression key-only order explain roots should report access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_expression_key_only_strict_text_range_query_uses_covering_read_route()
{
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic mixed-case dataset so the bounded
    // expression-key sibling can prove true covering eligibility without
    // claiming original `name` reconstruction from the lowered key.
    seed_expression_indexed_session_sql_entities(
        &session,
        &[
            (9_267_u128, "sam", 10),
            (9_268, "Alex", 20),
            (9_269, "amy", 30),
            (9_270, "bob", 40),
        ],
    );

    // Phase 2: require the session-backed query-builder explain to reuse the
    // planner-proven covering-read route for the bounded `id`-only projection.
    let descriptor = session
        .query_from_sql::<ExpressionIndexedSessionSqlEntity>(
            "SELECT id FROM ExpressionIndexedSessionSqlEntity WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
        )
        .expect("expression key-only strict text-range SQL query should lower")
        .explain_execution()
        .expect("expression key-only strict text-range SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "expression key-only strict text-range queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "expression key-only strict text-range queries should expose the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "expression key-only strict text-range explain roots should expose the covering-read route label",
    );
    let projection_node = explain_execution_find_first_node(
        &descriptor,
        ExplainExecutionNodeType::CoveringRead,
    )
    .expect("expression key-only strict text-range explain tree should emit a covering-read node");
    assert_eq!(
        projection_node.node_properties().get("covering_fields"),
        Some(&Value::List(vec![Value::Text("id".to_string())])),
        "expression key-only strict text-range covering nodes should expose the projected field list",
    );
    assert_eq!(
        projection_node.node_properties().get("existing_row_mode"),
        Some(&Value::Text("planner_proven".to_string())),
        "session-backed expression key-only strict text-range explain should inherit the planner-proven covering mode",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "expression key-only strict text-range explain roots should report access-satisfied ordering",
    );
}

#[test]
fn session_explain_execution_expression_key_only_strict_text_range_desc_query_uses_covering_read_route()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic mixed-case dataset so the descending
    // bounded expression-key sibling stays on the same honest covering family.
    seed_expression_indexed_session_sql_entities(
        &session,
        &[
            (9_271_u128, "sam", 10),
            (9_272, "Alex", 20),
            (9_273, "amy", 30),
            (9_274, "bob", 40),
        ],
    );

    // Phase 2: require the session-backed query-builder explain to surface the
    // covering route and the planner-proven existing-row mode.
    let descriptor = session
        .query_from_sql::<ExpressionIndexedSessionSqlEntity>(
            "SELECT id FROM ExpressionIndexedSessionSqlEntity WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
        )
        .expect("descending expression key-only strict text-range SQL query should lower")
        .explain_execution()
        .expect(
            "descending expression key-only strict text-range SQL explain_execution should succeed",
        );

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "descending expression key-only strict text-range queries should stay on the shared index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "descending expression key-only strict text-range queries should expose the explicit covering-read route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "descending expression key-only strict text-range explain roots should expose the covering-read route label",
    );
    let projection_node = explain_execution_find_first_node(
        &descriptor,
        ExplainExecutionNodeType::CoveringRead,
    )
    .expect(
        "descending expression key-only strict text-range explain tree should emit a covering-read node",
    );
    assert_eq!(
        projection_node.node_properties().get("covering_fields"),
        Some(&Value::List(vec![Value::Text("id".to_string())])),
        "descending expression key-only strict text-range covering nodes should expose the projected field list",
    );
    assert_eq!(
        projection_node.node_properties().get("existing_row_mode"),
        Some(&Value::Text("planner_proven".to_string())),
        "descending session-backed expression key-only strict text-range explain should inherit the planner-proven covering mode",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::OrderByAccessSatisfied
        )
        .is_some(),
        "descending expression key-only strict text-range explain roots should report access-satisfied ordering",
    );
}

#[test]
fn session_sql_expression_order_without_matching_index_stays_fail_closed() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql::<SessionSqlEntity>(
            "SELECT id, name FROM SessionSqlEntity ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
        )
        .expect_err("expression order without one matching index should fail closed");

    assert!(
        err.to_string()
            .contains("expression ORDER BY requires a matching index-backed access order"),
        "expression-order failures should preserve the explicit fail-closed policy message",
    );
}

#[test]
fn execute_sql_projection_index_coverable_multi_component_matches_entity_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic composite-index dataset so the SQL
    // projection lane must decode both indexed components from one secondary
    // `(code, serial)` access path.
    seed_composite_indexed_session_sql_entities(
        &session,
        &[
            (9_201_u128, "alpha", 2),
            (9_202, "alpha", 1),
            (9_203, "beta", 1),
        ],
    );

    // Phase 2: verify the projection lane returns the same `(id, code,
    // serial)` rows as the entity lane for a direct composite covering query.
    let sql = "SELECT id, code, serial FROM CompositeIndexedSessionSqlEntity ORDER BY code ASC, serial ASC, id ASC LIMIT 2";
    let projected_rows =
        dispatch_projection_rows::<CompositeIndexedSessionSqlEntity>(&session, sql)
            .expect("multi-component covering projection query should execute");
    let entity_rows = session
        .execute_sql::<CompositeIndexedSessionSqlEntity>(sql)
        .expect("multi-component covering entity query should execute");
    let entity_projected_rows = entity_rows
        .iter()
        .map(|row| {
            vec![
                Value::Ulid(row.entity_ref().id),
                Value::Text(row.entity_ref().code.clone()),
                Value::Uint(row.entity_ref().serial),
            ]
        })
        .collect::<Vec<_>>();

    assert_eq!(entity_projected_rows, projected_rows);
}

#[test]
fn execute_sql_dispatch_explain_execution_secondary_non_covering_age_projection_stays_off_removed_authority_labels()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    for (id, name, age) in [
        (9_220_u128, "alice", 10_u64),
        (9_221, "bob", 20),
        (9_222, "carol", 30),
    ] {
        session
            .insert(IndexedSessionSqlEntity {
                id: Ulid::from_u128(id),
                name: name.to_string(),
                age,
            })
            .expect("indexed SQL non-covering explain fixture insert should succeed");
    }

    let explain = dispatch_explain_sql::<IndexedSessionSqlEntity>(
        &session,
        "EXPLAIN EXECUTION SELECT age FROM IndexedSessionSqlEntity ORDER BY name ASC LIMIT 2",
    )
    .expect("non-covering secondary-order age projection EXPLAIN EXECUTION should execute");

    assert!(
        explain.contains("cov_read_route=Text(\"materialized\")")
            && !explain.contains("authority_decision")
            && !explain.contains("authority_reason")
            && !explain.contains("index_state"),
        "single-component non-covering secondary-order explain should stay off the removed authority-label surface: {explain}",
    );
    assert!(
        !explain.contains("witness_validated") && !explain.contains("storage_existence_witness"),
        "single-component non-covering secondary-order explain must not surface legacy authority labels: {explain}",
    );
}

#[test]
fn store_backed_execution_descriptor_json_secondary_non_covering_age_projection_stays_off_removed_authority_labels()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    for (id, name, age) in [
        (9_223_u128, "alice", 10_u64),
        (9_224, "bob", 20),
        (9_225, "carol", 30),
    ] {
        session
            .insert(IndexedSessionSqlEntity {
                id: Ulid::from_u128(id),
                name: name.to_string(),
                age,
            })
            .expect("indexed SQL non-covering descriptor fixture insert should succeed");
    }

    let descriptor_json = store_backed_execution_descriptor_json_for_sql::<IndexedSessionSqlEntity>(
        &session,
        "SELECT age FROM IndexedSessionSqlEntity ORDER BY name ASC LIMIT 2",
    );

    assert!(
        !descriptor_json.contains("\"authority_decision\"")
            && !descriptor_json.contains("\"authority_reason\"")
            && !descriptor_json.contains("\"index_state\""),
        "store-backed execution descriptor json should keep the single-component non-covering route off the removed authority-label surface: {descriptor_json}",
    );
}

#[test]
fn execute_sql_dispatch_explain_execution_secondary_covering_stays_off_removed_authority_labels() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    for (id, name, age) in [
        (9_226_u128, "alice", 10_u64),
        (9_227, "bob", 20),
        (9_228, "carol", 30),
    ] {
        session
            .insert(IndexedSessionSqlEntity {
                id: Ulid::from_u128(id),
                name: name.to_string(),
                age,
            })
            .expect("indexed SQL covering explain surface fixture insert should succeed");
    }

    let explain = dispatch_explain_sql::<IndexedSessionSqlEntity>(
        &session,
        "EXPLAIN EXECUTION SELECT id, name FROM IndexedSessionSqlEntity ORDER BY name ASC, id ASC LIMIT 2",
    )
    .expect("secondary covering EXPLAIN EXECUTION should execute");

    assert!(
        explain.contains("CoveringRead")
            && explain.contains("existing_row_mode=Text(\"planner_proven\")")
            && !explain.contains("authority_decision")
            && !explain.contains("authority_reason")
            && !explain.contains("index_state"),
        "store-backed secondary covering explain should stay off the removed authority-label surface: {explain}",
    );
}

#[test]
fn store_backed_execution_descriptor_json_secondary_covering_stays_off_removed_authority_labels() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    for (id, name, age) in [
        (9_229_u128, "alice", 10_u64),
        (9_230, "bob", 20),
        (9_231, "carol", 30),
    ] {
        session
            .insert(IndexedSessionSqlEntity {
                id: Ulid::from_u128(id),
                name: name.to_string(),
                age,
            })
            .expect("indexed SQL covering descriptor surface fixture insert should succeed");
    }

    let descriptor_json = store_backed_execution_descriptor_json_for_sql::<IndexedSessionSqlEntity>(
        &session,
        "SELECT id, name FROM IndexedSessionSqlEntity ORDER BY name ASC, id ASC LIMIT 2",
    );

    assert!(
        descriptor_json.contains("\"existing_row_mode\":\"Text(\\\"planner_proven\\\")\"")
            && !descriptor_json.contains("\"authority_decision\"")
            && !descriptor_json.contains("\"authority_reason\"")
            && !descriptor_json.contains("\"index_state\""),
        "store-backed secondary covering descriptor json should stay off the removed authority-label surface: {descriptor_json}",
    );
}

#[test]
fn execute_sql_dispatch_explain_execution_secondary_covering_order_field_building_index_becomes_planner_invisible()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    for (id, name, age) in [
        (9_460_u128, "alice", 10_u64),
        (9_461, "bob", 20),
        (9_462, "carol", 30),
    ] {
        session
            .insert(IndexedSessionSqlEntity {
                id: Ulid::from_u128(id),
                name: name.to_string(),
                age,
            })
            .expect("indexed SQL building-state explain fixture insert should succeed");
    }
    mark_indexed_session_sql_index_building();

    let explain = dispatch_explain_sql::<IndexedSessionSqlEntity>(
        &session,
        "EXPLAIN EXECUTION SELECT id, name FROM IndexedSessionSqlEntity ORDER BY name ASC, id ASC LIMIT 2",
    )
    .expect("building-index secondary covering EXPLAIN EXECUTION should execute");

    assert!(
        explain.contains("FullScan")
            && explain.contains("OrderByMaterializedSort")
            && !explain.contains("CoveringRead")
            && !explain.contains("existing_row_mode")
            && !explain.contains("authority_decision")
            && !explain.contains("authority_reason")
            && !explain.contains("index_state"),
        "building indexes must disappear from planner visibility and explain as a materialized full-scan fallback: {explain}",
    );
    assert!(
        !explain.contains("witness_validated") && !explain.contains("storage_existence_witness"),
        "building indexes must not leave legacy secondary covering labels behind once they are planner-invisible: {explain}",
    );

    let projected_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT id, name FROM IndexedSessionSqlEntity ORDER BY name ASC, id ASC LIMIT 2",
    )
    .expect("building-index secondary covering query should execute");

    assert_eq!(
        projected_rows,
        vec![
            vec![
                Value::Ulid(Ulid::from_u128(9_460)),
                Value::Text("alice".to_string()),
            ],
            vec![
                Value::Ulid(Ulid::from_u128(9_461)),
                Value::Text("bob".to_string()),
            ],
        ],
        "planner-invisibility fallback should preserve the same ordered query output",
    );
}

#[test]
fn execute_sql_projection_direct_starts_with_matches_indexed_like_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic uppercase-prefix dataset under the same
    // secondary text index used by the strict LIKE prefix regression.
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    // Phase 2: prove the new direct spelling returns the same indexed
    // projection rows as the established strict LIKE prefix path.
    let direct_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE STARTS_WITH(name, 'S') ORDER BY name ASC",
    )
    .expect("direct STARTS_WITH projection should execute");
    let like_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name ASC",
    )
    .expect("strict LIKE prefix projection should execute");
    let range_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE name >= 'S' AND name < 'T' ORDER BY name ASC",
    )
    .expect("strict text-range projection should execute");

    assert_eq!(
        direct_rows, like_rows,
        "direct STARTS_WITH projection should match the established strict LIKE prefix result set",
    );
    assert_eq!(
        direct_rows, range_rows,
        "direct STARTS_WITH projection should match the equivalent strict text-range result set",
    );
}

#[test]
fn execute_sql_entity_direct_starts_with_matches_indexed_like_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Phase 1: seed one deterministic uppercase-prefix dataset under the same
    // secondary text index used by the strict LIKE prefix regression.
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    // Phase 2: prove the direct spelling keeps entity-row execution aligned
    // with the established strict LIKE prefix path.
    let direct_rows = session
        .execute_sql::<IndexedSessionSqlEntity>(
            "SELECT * FROM IndexedSessionSqlEntity WHERE STARTS_WITH(name, 'S') ORDER BY name ASC",
        )
        .expect("direct STARTS_WITH entity query should execute");
    let like_rows = session
        .execute_sql::<IndexedSessionSqlEntity>(
            "SELECT * FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name ASC",
        )
        .expect("strict LIKE prefix entity query should execute");
    let range_rows = session
        .execute_sql::<IndexedSessionSqlEntity>(
            "SELECT * FROM IndexedSessionSqlEntity WHERE name >= 'S' AND name < 'T' ORDER BY name ASC",
        )
        .expect("strict text-range entity query should execute");

    assert_eq!(direct_rows.len(), like_rows.len());
    for (direct, like) in direct_rows.iter().zip(like_rows.iter()) {
        assert_eq!(
            direct.entity_ref(),
            like.entity_ref(),
            "direct STARTS_WITH entity rows should match strict LIKE prefix entity rows",
        );
    }
    assert_eq!(direct_rows.len(), range_rows.len());
    for (direct, range) in direct_rows.iter().zip(range_rows.iter()) {
        assert_eq!(
            direct.entity_ref(),
            range.entity_ref(),
            "direct STARTS_WITH entity rows should match strict text-range entity rows",
        );
    }
}

#[test]
fn execute_sql_projection_direct_lower_starts_with_matches_indexed_lower_like_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    let like_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE LOWER(name) LIKE 's%' ORDER BY name ASC",
    )
    .expect("LOWER(field) LIKE projection should execute");

    let starts_with_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE STARTS_WITH(LOWER(name), 's') ORDER BY name ASC",
    )
    .expect("direct LOWER(field) STARTS_WITH projection should execute");

    assert_eq!(
        starts_with_rows, like_rows,
        "direct LOWER(field) STARTS_WITH projection should match the established casefold LIKE prefix result set",
    );
}

#[test]
fn execute_sql_projection_direct_lower_text_range_matches_indexed_lower_like_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    let like_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE LOWER(name) LIKE 's%' ORDER BY name ASC",
    )
    .expect("LOWER(field) LIKE projection should execute");

    let range_rows = dispatch_projection_rows::<IndexedSessionSqlEntity>(
        &session,
        "SELECT name FROM IndexedSessionSqlEntity WHERE LOWER(name) >= 's' AND LOWER(name) < 't' ORDER BY name ASC",
    )
    .expect("LOWER(field) ordered text-range projection should execute");

    assert_eq!(
        range_rows, like_rows,
        "LOWER(field) ordered text-range projection should match the established casefold LIKE prefix result set",
    );
}

#[test]
fn session_explain_execution_direct_lower_text_range_keeps_expression_index_range_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    let descriptor = session
        .query_from_sql::<IndexedSessionSqlEntity>(
            "SELECT name FROM IndexedSessionSqlEntity WHERE LOWER(name) >= 's' AND LOWER(name) < 't' ORDER BY name ASC",
        )
        .expect("LOWER(field) ordered text-range SQL query should lower")
        .explain_execution()
        .expect("LOWER(field) ordered text-range SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "LOWER(field) ordered text-range queries should stay on the shared expression index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(false),
        "LOWER(field) ordered text-range projections should still materialize raw field rows from the expression index route",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::ResidualPredicateFilter
        )
        .is_some(),
        "LOWER(field) ordered text-range explain roots should keep the residual filter stage",
    );
}

#[test]
fn session_explain_execution_direct_lower_equivalent_prefix_forms_preserve_expression_index_route()
{
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    let cases = [
        (
            "SELECT name FROM IndexedSessionSqlEntity WHERE LOWER(name) LIKE 's%' ORDER BY name ASC",
            "LOWER(field) LIKE explain route",
        ),
        (
            "SELECT name FROM IndexedSessionSqlEntity WHERE STARTS_WITH(LOWER(name), 's') ORDER BY name ASC",
            "direct LOWER(field) STARTS_WITH explain route",
        ),
        (
            "SELECT name FROM IndexedSessionSqlEntity WHERE LOWER(name) >= 's' AND LOWER(name) < 't' ORDER BY name ASC",
            "LOWER(field) ordered text-range explain route",
        ),
    ];

    for (sql, context) in cases {
        let descriptor = session
            .query_from_sql::<IndexedSessionSqlEntity>(sql)
            .unwrap_or_else(|err| panic!("{context} should lower: {err}"))
            .explain_execution()
            .unwrap_or_else(|err| panic!("{context} should explain_execution: {err}"));

        assert_eq!(
            descriptor.node_type(),
            ExplainExecutionNodeType::IndexRangeScan,
            "{context} should keep the shared expression index-range root",
        );
        assert_eq!(
            descriptor.covering_scan(),
            Some(false),
            "{context} should keep the non-covering materialized route",
        );
        assert!(
            explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::ResidualPredicateFilter
            )
            .is_some(),
            "{context} should keep the residual filter stage",
        );
    }
}

#[test]
fn execute_sql_entity_direct_upper_starts_with_matches_indexed_upper_like_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    let like_rows = session
        .execute_sql::<IndexedSessionSqlEntity>(
            "SELECT * FROM IndexedSessionSqlEntity WHERE UPPER(name) LIKE 'S%' ORDER BY name ASC",
        )
        .expect("UPPER(field) LIKE entity query should execute");

    let starts_with_rows = session
        .execute_sql::<IndexedSessionSqlEntity>(
            "SELECT * FROM IndexedSessionSqlEntity WHERE STARTS_WITH(UPPER(name), 'S') ORDER BY name ASC",
        )
        .expect("direct UPPER(field) STARTS_WITH entity query should execute");

    assert_eq!(starts_with_rows.len(), like_rows.len());
    for (starts_with, like) in starts_with_rows.iter().zip(like_rows.iter()) {
        assert_eq!(
            starts_with.entity_ref(),
            like.entity_ref(),
            "direct UPPER(field) STARTS_WITH entity rows should match the established casefold LIKE prefix entity rows",
        );
    }
}

#[test]
fn execute_sql_entity_direct_upper_text_range_matches_indexed_upper_like_rows() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    let like_rows = session
        .execute_sql::<IndexedSessionSqlEntity>(
            "SELECT * FROM IndexedSessionSqlEntity WHERE UPPER(name) LIKE 'S%' ORDER BY name ASC",
        )
        .expect("UPPER(field) LIKE entity query should execute");

    let range_rows = session
        .execute_sql::<IndexedSessionSqlEntity>(
            "SELECT * FROM IndexedSessionSqlEntity WHERE UPPER(name) >= 'S' AND UPPER(name) < 'T' ORDER BY name ASC",
        )
        .expect("UPPER(field) ordered text-range entity query should execute");

    assert_eq!(range_rows.len(), like_rows.len());
    for (range_row, like_row) in range_rows.iter().zip(like_rows.iter()) {
        assert_eq!(
            range_row.entity_ref(),
            like_row.entity_ref(),
            "UPPER(field) ordered text-range entity rows should match the established casefold LIKE prefix entity rows",
        );
    }
}

#[test]
fn session_explain_execution_direct_upper_text_range_keeps_expression_index_range_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    let descriptor = session
        .query_from_sql::<IndexedSessionSqlEntity>(
            "SELECT name FROM IndexedSessionSqlEntity WHERE UPPER(name) >= 'S' AND UPPER(name) < 'T' ORDER BY name ASC",
        )
        .expect("UPPER(field) ordered text-range SQL query should lower")
        .explain_execution()
        .expect("UPPER(field) ordered text-range SQL explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexRangeScan,
        "UPPER(field) ordered text-range queries should stay on the shared expression index-range root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(false),
        "UPPER(field) ordered text-range projections should still materialize raw field rows from the expression index route",
    );
    assert!(
        explain_execution_find_first_node(
            &descriptor,
            ExplainExecutionNodeType::ResidualPredicateFilter
        )
        .is_some(),
        "UPPER(field) ordered text-range explain roots should keep the residual filter stage",
    );
}

#[test]
fn session_explain_execution_direct_upper_equivalent_prefix_forms_preserve_expression_index_route()
{
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(
        &session,
        &[
            ("Sonja She-Devil", 10),
            ("Stamm Bladecaster", 20),
            ("Syra Child of Nature", 30),
            ("Sir Edward Lion", 40),
            ("Sethra Bhoaghail", 50),
            ("Aldren", 60),
        ],
    );

    let cases = [
        (
            "SELECT name FROM IndexedSessionSqlEntity WHERE UPPER(name) LIKE 'S%' ORDER BY name ASC",
            "UPPER(field) LIKE explain route",
        ),
        (
            "SELECT name FROM IndexedSessionSqlEntity WHERE STARTS_WITH(UPPER(name), 'S') ORDER BY name ASC",
            "direct UPPER(field) STARTS_WITH explain route",
        ),
        (
            "SELECT name FROM IndexedSessionSqlEntity WHERE UPPER(name) >= 'S' AND UPPER(name) < 'T' ORDER BY name ASC",
            "UPPER(field) ordered text-range explain route",
        ),
    ];

    for (sql, context) in cases {
        let descriptor = session
            .query_from_sql::<IndexedSessionSqlEntity>(sql)
            .unwrap_or_else(|err| panic!("{context} should lower: {err}"))
            .explain_execution()
            .unwrap_or_else(|err| panic!("{context} should explain_execution: {err}"));

        assert_eq!(
            descriptor.node_type(),
            ExplainExecutionNodeType::IndexRangeScan,
            "{context} should keep the shared expression index-range root",
        );
        assert_eq!(
            descriptor.covering_scan(),
            Some(false),
            "{context} should keep the non-covering materialized route",
        );
        assert!(
            explain_execution_find_first_node(
                &descriptor,
                ExplainExecutionNodeType::ResidualPredicateFilter
            )
            .is_some(),
            "{context} should keep the residual filter stage",
        );
    }
}

#[test]
fn execute_sql_delete_direct_starts_with_family_matches_indexed_like_delete_rows() {
    // Phase 1: define the accepted direct predicate family and the established
    // equivalent bounded LIKE spellings they should continue to match.
    let cases = [
        (
            "DELETE FROM IndexedSessionSqlEntity WHERE STARTS_WITH(name, 'S') ORDER BY name ASC LIMIT 2",
            "DELETE FROM IndexedSessionSqlEntity WHERE name LIKE 'S%' ORDER BY name ASC LIMIT 2",
            "strict direct STARTS_WITH delete",
        ),
        (
            "DELETE FROM IndexedSessionSqlEntity WHERE STARTS_WITH(LOWER(name), 's') ORDER BY name ASC LIMIT 2",
            "DELETE FROM IndexedSessionSqlEntity WHERE LOWER(name) LIKE 's%' ORDER BY name ASC LIMIT 2",
            "direct LOWER(field) STARTS_WITH delete",
        ),
        (
            "DELETE FROM IndexedSessionSqlEntity WHERE LOWER(name) >= 's' AND LOWER(name) < 't' ORDER BY name ASC LIMIT 2",
            "DELETE FROM IndexedSessionSqlEntity WHERE LOWER(name) LIKE 's%' ORDER BY name ASC LIMIT 2",
            "direct LOWER(field) ordered text-range delete",
        ),
        (
            "DELETE FROM IndexedSessionSqlEntity WHERE STARTS_WITH(UPPER(name), 'S') ORDER BY name ASC LIMIT 2",
            "DELETE FROM IndexedSessionSqlEntity WHERE UPPER(name) LIKE 'S%' ORDER BY name ASC LIMIT 2",
            "direct UPPER(field) STARTS_WITH delete",
        ),
        (
            "DELETE FROM IndexedSessionSqlEntity WHERE UPPER(name) >= 'S' AND UPPER(name) < 'T' ORDER BY name ASC LIMIT 2",
            "DELETE FROM IndexedSessionSqlEntity WHERE UPPER(name) LIKE 'S%' ORDER BY name ASC LIMIT 2",
            "direct UPPER(field) ordered text-range delete",
        ),
    ];

    // Phase 2: run the direct and LIKE deletes against separate fresh seeds so
    // both the deleted rows and surviving rows must stay identical.
    for (direct_sql, like_sql, context) in cases {
        let run_delete = |sql: &str| {
            reset_indexed_session_sql_store();
            let session = indexed_sql_session();
            seed_indexed_session_sql_entities(
                &session,
                &[
                    ("Sonja She-Devil", 10),
                    ("Stamm Bladecaster", 20),
                    ("Syra Child of Nature", 30),
                    ("Sir Edward Lion", 40),
                    ("Sethra Bhoaghail", 50),
                    ("Aldren", 60),
                ],
            );

            let deleted_rows = session
                .execute_sql::<IndexedSessionSqlEntity>(sql)
                .expect("indexed STARTS_WITH/LIKE delete should execute");
            let deleted_names = deleted_rows
                .iter()
                .map(|row| row.entity_ref().name.clone())
                .collect::<Vec<_>>();
            let remaining_names = session
                .load::<IndexedSessionSqlEntity>()
                .order_by("name")
                .execute()
                .expect("post-delete indexed load should succeed")
                .iter()
                .map(|row| row.entity_ref().name.clone())
                .collect::<Vec<_>>();

            (deleted_names, remaining_names)
        };

        let direct = run_delete(direct_sql);
        let like = run_delete(like_sql);

        assert_eq!(
            direct, like,
            "bounded direct STARTS_WITH delete should match the established LIKE delete semantics: {context}",
        );
    }
}

#[test]
fn execute_sql_select_field_projection_currently_returns_entity_shaped_rows() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "projected-row".to_string(),
            age: 29,
        })
        .expect("seed insert should succeed");

    let response = session
        .execute_sql::<SessionSqlEntity>(
            "SELECT name FROM SessionSqlEntity ORDER BY age ASC LIMIT 1",
        )
        .expect("field-list SQL projection should execute");
    let row = response
        .iter()
        .next()
        .expect("field-list SQL projection response should contain one row");

    assert_eq!(
        row.entity_ref().name,
        "projected-row",
        "field-list SQL projection should still return entity rows in this baseline",
    );
    assert_eq!(
        row.entity_ref().age,
        29,
        "field-list SQL projection should preserve full entity payload until projection response shaping is introduced",
    );
}

#[test]
fn sql_projection_columns_select_field_list_returns_canonical_labels() {
    reset_session_sql_store();
    let session = sql_session();

    let columns = dispatch_projection_columns::<SessionSqlEntity>(
        &session,
        "SELECT name, age FROM SessionSqlEntity",
    )
    .expect("field-list SQL projection columns should derive");

    assert_eq!(columns, vec!["name".to_string(), "age".to_string()]);
}

#[test]
fn sql_projection_columns_select_star_returns_entity_model_order() {
    reset_session_sql_store();
    let session = sql_session();

    let columns =
        dispatch_projection_columns::<SessionSqlEntity>(&session, "SELECT * FROM SessionSqlEntity")
            .expect("star SQL projection columns should derive");

    assert_eq!(
        columns,
        vec!["id".to_string(), "name".to_string(), "age".to_string()]
    );
}

#[test]
fn sql_projection_columns_delete_returns_entity_model_order() {
    reset_session_sql_store();
    let session = sql_session();

    let columns = dispatch_projection_columns::<SessionSqlEntity>(
        &session,
        "DELETE FROM SessionSqlEntity WHERE age > 10",
    )
    .expect("delete SQL columns should derive from full entity row shape");

    assert_eq!(
        columns,
        vec!["id".to_string(), "name".to_string(), "age".to_string()],
        "delete SQL should project full entity columns in model order",
    );
}

#[test]
fn execute_sql_projection_select_field_list_returns_projection_shaped_rows() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "projection-surface".to_string(),
            age: 33,
        })
        .expect("seed insert should succeed");

    let response = dispatch_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT name FROM SessionSqlEntity ORDER BY age ASC LIMIT 1",
    )
    .expect("projection SQL execution should succeed");
    let row = response
        .first()
        .expect("projection SQL response should contain one row");

    assert_eq!(response.len(), 1);
    assert_eq!(
        row.as_slice(),
        [Value::Text("projection-surface".to_string())],
        "projection SQL response should carry only projected field values in declaration order",
    );
}

#[test]
fn execute_sql_projection_trim_ltrim_rtrim_lower_upper_and_length_dispatch_from_session_boundary() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "  Ada  ".to_string(),
            age: 33,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "\tBob".to_string(),
            age: 21,
        })
        .expect("seed insert should succeed");

    let columns = dispatch_projection_columns::<SessionSqlEntity>(
        &session,
        "SELECT TRIM(name), LTRIM(name), RTRIM(name), LOWER(name), UPPER(name), LENGTH(name), age FROM SessionSqlEntity ORDER BY age DESC",
    )
    .expect("computed SQL projection columns should derive");
    let rows = dispatch_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT TRIM(name), LTRIM(name), RTRIM(name), LOWER(name), UPPER(name), LENGTH(name), age FROM SessionSqlEntity ORDER BY age DESC",
    )
    .expect("computed SQL projection rows should execute");

    assert_eq!(
        columns,
        vec![
            "TRIM(name)".to_string(),
            "LTRIM(name)".to_string(),
            "RTRIM(name)".to_string(),
            "LOWER(name)".to_string(),
            "UPPER(name)".to_string(),
            "LENGTH(name)".to_string(),
            "age".to_string(),
        ],
    );
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Text("Ada".to_string()),
                Value::Text("Ada  ".to_string()),
                Value::Text("  Ada".to_string()),
                Value::Text("  ada  ".to_string()),
                Value::Text("  ADA  ".to_string()),
                Value::Uint(7),
                Value::Uint(33),
            ],
            vec![
                Value::Text("Bob".to_string()),
                Value::Text("Bob".to_string()),
                Value::Text("\tBob".to_string()),
                Value::Text("\tbob".to_string()),
                Value::Text("\tBOB".to_string()),
                Value::Uint(4),
                Value::Uint(21),
            ],
        ],
    );
}

#[test]
fn execute_sql_projection_left_and_right_dispatch_from_session_boundary() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "  Ada  ".to_string(),
            age: 33,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "\tBob".to_string(),
            age: 21,
        })
        .expect("seed insert should succeed");

    let columns = dispatch_projection_columns::<SessionSqlEntity>(
        &session,
        "SELECT LEFT(name, 2), RIGHT(name, 3), LEFT(name, NULL) FROM SessionSqlEntity ORDER BY age DESC",
    )
    .expect("left/right SQL projection columns should derive");
    let rows = dispatch_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT LEFT(name, 2), RIGHT(name, 3), LEFT(name, NULL) FROM SessionSqlEntity ORDER BY age DESC",
    )
    .expect("left/right SQL projection rows should execute");

    assert_eq!(
        columns,
        vec![
            "LEFT(name, 2)".to_string(),
            "RIGHT(name, 3)".to_string(),
            "LEFT(name, NULL)".to_string(),
        ],
    );
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Text("  ".to_string()),
                Value::Text("a  ".to_string()),
                Value::Null,
            ],
            vec![
                Value::Text("\tB".to_string()),
                Value::Text("Bob".to_string()),
                Value::Null,
            ],
        ],
    );
}

#[test]
fn execute_sql_projection_starts_ends_and_position_dispatch_from_session_boundary() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "  Ada  ".to_string(),
            age: 33,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "\tBob".to_string(),
            age: 21,
        })
        .expect("seed insert should succeed");

    let columns = dispatch_projection_columns::<SessionSqlEntity>(
        &session,
        "SELECT STARTS_WITH(name, ' '), ENDS_WITH(name, 'b'), CONTAINS(name, 'da'), POSITION('da', name), POSITION(NULL, name) FROM SessionSqlEntity ORDER BY age DESC",
    )
    .expect("text predicate SQL projection columns should derive");
    let rows = dispatch_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT STARTS_WITH(name, ' '), ENDS_WITH(name, 'b'), CONTAINS(name, 'da'), POSITION('da', name), POSITION(NULL, name) FROM SessionSqlEntity ORDER BY age DESC",
    )
    .expect("text predicate SQL projection rows should execute");

    assert_eq!(
        columns,
        vec![
            "STARTS_WITH(name, ' ')".to_string(),
            "ENDS_WITH(name, 'b')".to_string(),
            "CONTAINS(name, 'da')".to_string(),
            "POSITION('da', name)".to_string(),
            "POSITION(NULL, name)".to_string(),
        ],
    );
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Bool(true),
                Value::Bool(false),
                Value::Bool(true),
                Value::Uint(4),
                Value::Null,
            ],
            vec![
                Value::Bool(false),
                Value::Bool(true),
                Value::Bool(false),
                Value::Uint(0),
                Value::Null,
            ],
        ],
    );
}

#[test]
fn execute_sql_projection_replace_dispatch_from_session_boundary() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "  Ada  ".to_string(),
            age: 33,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "\tBob".to_string(),
            age: 21,
        })
        .expect("seed insert should succeed");

    let columns = dispatch_projection_columns::<SessionSqlEntity>(
        &session,
        "SELECT REPLACE(name, 'A', 'E'), REPLACE(name, NULL, 'x') FROM SessionSqlEntity ORDER BY age DESC",
    )
    .expect("replace SQL projection columns should derive");
    let rows = dispatch_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT REPLACE(name, 'A', 'E'), REPLACE(name, NULL, 'x') FROM SessionSqlEntity ORDER BY age DESC",
    )
    .expect("replace SQL projection rows should execute");

    assert_eq!(
        columns,
        vec![
            "REPLACE(name, 'A', 'E')".to_string(),
            "REPLACE(name, NULL, 'x')".to_string(),
        ],
    );
    assert_eq!(
        rows,
        vec![
            vec![Value::Text("  Eda  ".to_string()), Value::Null],
            vec![Value::Text("\tBob".to_string()), Value::Null],
        ],
    );
}

#[test]
fn execute_sql_projection_substring_dispatch_from_session_boundary() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "  Ada  ".to_string(),
            age: 33,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "\tBob".to_string(),
            age: 21,
        })
        .expect("seed insert should succeed");

    let columns = dispatch_projection_columns::<SessionSqlEntity>(
        &session,
        "SELECT SUBSTRING(name, 3, 3), SUBSTRING(name, 3), SUBSTRING(name, NULL, 2) FROM SessionSqlEntity ORDER BY age DESC",
    )
    .expect("substring SQL projection columns should derive");
    let rows = dispatch_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT SUBSTRING(name, 3, 3), SUBSTRING(name, 3), SUBSTRING(name, NULL, 2) FROM SessionSqlEntity ORDER BY age DESC",
    )
    .expect("substring SQL projection rows should execute");

    assert_eq!(
        columns,
        vec![
            "SUBSTRING(name, 3, 3)".to_string(),
            "SUBSTRING(name, 3)".to_string(),
            "SUBSTRING(name, NULL, 2)".to_string(),
        ],
    );
    assert_eq!(
        rows,
        vec![
            vec![
                Value::Text("Ada".to_string()),
                Value::Text("Ada  ".to_string()),
                Value::Null,
            ],
            vec![
                Value::Text("ob".to_string()),
                Value::Text("ob".to_string()),
                Value::Null,
            ],
        ],
    );
}

#[test]
fn execute_sql_projection_select_star_returns_all_fields_in_model_order() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "projection-star".to_string(),
            age: 41,
        })
        .expect("seed insert should succeed");

    let response = dispatch_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT * FROM SessionSqlEntity ORDER BY age ASC LIMIT 1",
    )
    .expect("projection SQL star execution should succeed");
    let row = response
        .first()
        .expect("projection SQL star response should contain one row");

    assert_eq!(response.len(), 1);
    assert_eq!(
        row.len(),
        3,
        "SELECT * projection response should include all model fields",
    );
    assert!(matches!(row[0], Value::Ulid(_)));
    assert_eq!(row[1], Value::Text("projection-star".to_string()));
    assert_eq!(row[2], Value::Uint(41));
}

#[test]
fn execute_sql_select_schema_qualified_entity_executes() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "schema-qualified".to_string(),
            age: 41,
        })
        .expect("seed insert should succeed");

    let response = session
        .execute_sql::<SessionSqlEntity>(
            "SELECT * FROM public.SessionSqlEntity ORDER BY age ASC LIMIT 1",
        )
        .expect("schema-qualified entity SQL should execute");

    assert_eq!(response.len(), 1);
}

#[test]
fn execute_sql_projection_select_table_qualified_fields_executes() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "qualified-projection".to_string(),
            age: 42,
        })
        .expect("seed insert should succeed");

    let response = dispatch_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT SessionSqlEntity.name \
         FROM SessionSqlEntity \
         WHERE SessionSqlEntity.age >= 40 \
         ORDER BY SessionSqlEntity.age DESC LIMIT 1",
    )
    .expect("table-qualified projection SQL should execute");
    let row = response
        .first()
        .expect("table-qualified projection SQL response should contain one row");

    assert_eq!(response.len(), 1);
    assert_eq!(row, &[Value::Text("qualified-projection".to_string())]);
}

#[test]
fn execute_sql_projection_select_field_list_honors_order_limit_offset_window() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed deterministic age-ordered rows.
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "projection-window-a".to_string(),
            age: 10,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "projection-window-b".to_string(),
            age: 20,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "projection-window-c".to_string(),
            age: 30,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "projection-window-d".to_string(),
            age: 40,
        })
        .expect("seed insert should succeed");

    // Phase 2: execute one projection query with explicit window controls.
    let response = dispatch_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT name, age \
         FROM SessionSqlEntity \
         ORDER BY age DESC LIMIT 2 OFFSET 1",
    )
    .expect("projection SQL window execution should succeed");
    let rows = response;

    // Phase 3: assert projected row payloads follow ordered window semantics.
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0],
        [
            Value::Text("projection-window-c".to_string()),
            Value::Uint(30)
        ],
    );
    assert_eq!(
        rows[1],
        [
            Value::Text("projection-window-b".to_string()),
            Value::Uint(20)
        ],
    );
}

#[test]
fn execute_sql_projection_delete_returns_deleted_rows() {
    reset_session_sql_store();
    let session = sql_session();

    seed_session_sql_entities(
        &session,
        &[
            ("projection-delete-a", 10_u64),
            ("projection-delete-b", 20_u64),
            ("projection-delete-c", 30_u64),
        ],
    );

    let projection = dispatch_projection_rows::<SessionSqlEntity>(
        &session,
        "DELETE FROM SessionSqlEntity ORDER BY age LIMIT 1",
    )
    .expect("projection SQL execution should support delete statements");
    let rows = projection;

    assert!(
        rows.len() == 1,
        "delete projection should return exactly one deleted row",
    );
    assert!(
        matches!(rows[0].first(), Some(Value::Ulid(_))),
        "delete projection should expose the deleted row id in the first projected column",
    );
    assert_eq!(
        &rows[0][1..],
        &[
            Value::Text("projection-delete-a".to_string()),
            Value::Uint(10)
        ],
        "delete projection should return the deleted entity fields in declared model order",
    );
}

#[test]
fn execute_sql_select_field_projection_unknown_field_fails_with_plan_error() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql::<SessionSqlEntity>("SELECT missing_field FROM SessionSqlEntity")
        .expect_err("unknown projected fields should fail planner validation");

    assert!(
        matches!(err, QueryError::Plan(_)),
        "unknown projected fields should surface planner-domain query errors: {err:?}",
    );
}

#[test]
fn execute_sql_rejects_aggregate_projection_in_current_slice() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql::<SessionSqlEntity>("SELECT COUNT(*) FROM SessionSqlEntity")
        .expect_err("global aggregate SQL projection should remain lowering-gated");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "global aggregate SQL projection should fail at reduced lowering boundary",
    );
    assert!(
        err.to_string()
            .contains("execute_sql rejects global aggregate SELECT"),
        "execute_sql should preserve the dedicated aggregate-lane boundary message",
    );
}

#[test]
fn execute_sql_dispatch_returns_projection_payload_for_global_aggregate_execution() {
    reset_session_sql_store();
    let session = sql_session();

    let payload = session
        .execute_sql_dispatch::<SessionSqlEntity>("SELECT COUNT(*) FROM SessionSqlEntity")
        .expect(
            "execute_sql_dispatch should execute global aggregate SQL through projection payload",
        );

    let SqlDispatchResult::Projection {
        columns,
        rows,
        row_count,
    } = payload
    else {
        panic!(
            "execute_sql_dispatch should return one projection payload for global aggregate SQL"
        );
    };

    assert_eq!(
        columns,
        vec!["COUNT(*)".to_string()],
        "global aggregate dispatch payload should preserve aggregate projection label",
    );
    assert_eq!(
        rows,
        vec![vec![Value::Uint(0)]],
        "global aggregate dispatch payload should preserve empty-store scalar aggregate value",
    );
    assert_eq!(
        row_count, 1,
        "global aggregate dispatch payload should expose one scalar aggregate row",
    );
}

#[test]
fn execute_sql_rejects_table_alias_forms_in_reduced_parser() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql::<SessionSqlEntity>("SELECT * FROM SessionSqlEntity alias")
        .expect_err("table aliases should be rejected by reduced SQL parser");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "table alias usage should fail closed through unsupported SQL boundary",
    );
}

#[test]
fn execute_sql_rejects_quoted_identifiers_in_reduced_parser() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql::<SessionSqlEntity>("SELECT \"name\" FROM SessionSqlEntity")
        .expect_err("quoted identifiers should be rejected by reduced SQL parser");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "quoted identifiers should fail closed through unsupported SQL boundary",
    );
}

#[test]
fn execute_sql_select_distinct_star_executes() {
    reset_session_sql_store();
    let session = sql_session();

    let id_a = Ulid::generate();
    let id_b = Ulid::generate();
    session
        .insert(SessionSqlEntity {
            id: id_a,
            name: "distinct-a".to_string(),
            age: 20,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: id_b,
            name: "distinct-b".to_string(),
            age: 20,
        })
        .expect("seed insert should succeed");

    let response = session
        .execute_sql::<SessionSqlEntity>("SELECT DISTINCT * FROM SessionSqlEntity ORDER BY id ASC")
        .expect("SELECT DISTINCT * should execute");
    assert_eq!(response.len(), 2);
}

#[test]
fn execute_sql_projection_select_distinct_with_pk_field_list_executes() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "distinct-pk-a".to_string(),
            age: 25,
        })
        .expect("seed insert should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::generate(),
            name: "distinct-pk-b".to_string(),
            age: 25,
        })
        .expect("seed insert should succeed");

    let response = dispatch_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT DISTINCT id, age FROM SessionSqlEntity ORDER BY id ASC",
    )
    .expect("SELECT DISTINCT field-list with PK should execute");
    assert_eq!(response.len(), 2);
    assert_eq!(response[0].len(), 2);
}

#[test]
fn execute_sql_rejects_distinct_without_pk_projection_in_current_slice() {
    reset_session_sql_store();
    let session = sql_session();

    let err = session
        .execute_sql::<SessionSqlEntity>("SELECT DISTINCT age FROM SessionSqlEntity")
        .expect_err("SELECT DISTINCT without PK in projection should remain lowering-gated");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(
                _
            ))
        ),
        "distinct SQL gating should map to unsupported execution error boundary",
    );
}

#[test]
fn execute_sql_projection_matrix_queries_match_expected_projected_rows() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed deterministic rows used by matrix projections.
    seed_session_sql_entities(
        &session,
        &[
            ("matrix-a", 10),
            ("matrix-b", 20),
            ("matrix-c", 30),
            ("matrix-d", 40),
        ],
    );

    // Phase 2: execute table-driven projection SQL cases.
    let cases = vec![
        (
            "SELECT name, age \
             FROM SessionSqlEntity \
             ORDER BY age DESC LIMIT 2 OFFSET 1",
            vec![
                vec![Value::Text("matrix-c".to_string()), Value::Uint(30)],
                vec![Value::Text("matrix-b".to_string()), Value::Uint(20)],
            ],
        ),
        (
            "SELECT age \
             FROM SessionSqlEntity \
             WHERE age >= 20 \
             ORDER BY age ASC LIMIT 2",
            vec![vec![Value::Uint(20)], vec![Value::Uint(30)]],
        ),
        (
            "SELECT name \
             FROM SessionSqlEntity \
             WHERE age < 25 \
             ORDER BY age ASC",
            vec![
                vec![Value::Text("matrix-a".to_string())],
                vec![Value::Text("matrix-b".to_string())],
            ],
        ),
    ];

    // Phase 3: assert projected row payloads for each SQL input.
    for (sql, expected_rows) in cases {
        let response = dispatch_projection_rows::<SessionSqlEntity>(&session, sql)
            .expect("projection matrix SQL execution should succeed");
        let actual_rows = response;

        assert_eq!(actual_rows, expected_rows, "projection matrix case: {sql}");
    }
}

#[test]
fn session_aggregate_projection_terminals_match_execute_projection() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_321, 7, 10),
            (8_322, 7, 10),
            (8_323, 7, 20),
            (8_324, 7, 30),
            (8_325, 7, 40),
            (8_326, 8, 99),
        ],
    );
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by_desc("id")
            .offset(1)
            .limit(4)
    };

    // Phase 1: establish the execute() window as the shared parity baseline.
    let expected = load_window()
        .execute()
        .expect("session aggregate execute-projection baseline should succeed");

    // Phase 2: compare every projection terminal against the execute baseline.
    let values = run_session_aggregate_projection_terminal(
        &session,
        SessionAggregateProjectionTerminal::ValuesBy,
    )
    .expect("session values_by(rank) should succeed");
    let values_with_ids = run_session_aggregate_projection_terminal(
        &session,
        SessionAggregateProjectionTerminal::ValuesByWithIds,
    )
    .expect("session values_by_with_ids(rank) should succeed");
    let distinct_values = run_session_aggregate_projection_terminal(
        &session,
        SessionAggregateProjectionTerminal::DistinctValuesBy,
    )
    .expect("session distinct_values_by(rank) should succeed");

    assert_eq!(
        values,
        SessionAggregateResult::Values(session_aggregate_values_by_rank(&expected)),
        "session values_by(rank) should match execute() projection order",
    );
    assert_eq!(
        values_with_ids,
        SessionAggregateResult::ValuesWithIds(session_aggregate_values_by_rank_with_ids(&expected)),
        "session values_by_with_ids(rank) should match execute() projection order",
    );
    assert_eq!(
        distinct_values,
        SessionAggregateResult::Values(vec![Value::Uint(30), Value::Uint(20), Value::Uint(10),]),
        "session distinct_values_by(rank) should preserve first-observed dedup order",
    );
}

#[test]
fn session_aggregate_distinct_values_by_matches_values_by_first_observed_dedup() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_341, 7, 10),
            (8_342, 7, 10),
            (8_343, 7, 20),
            (8_344, 7, 30),
            (8_345, 7, 20),
            (8_346, 8, 99),
        ],
    );
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by_desc("id")
            .offset(1)
            .limit(4)
    };

    let values = load_window()
        .values_by("rank")
        .expect("session values_by(rank) should succeed");
    let distinct_values = load_window()
        .distinct_values_by("rank")
        .expect("session distinct_values_by(rank) should succeed");
    let mut expected_distinct = Vec::new();
    for value in &values {
        if expected_distinct.iter().any(|existing| existing == value) {
            continue;
        }
        expected_distinct.push(value.clone());
    }

    assert!(
        values.len() >= distinct_values.len(),
        "values_by(field).len() must be >= distinct_values_by(field).len()",
    );
    assert_eq!(
        distinct_values, expected_distinct,
        "distinct_values_by(field) must equal values_by(field) deduped by first occurrence",
    );
}

#[test]
fn session_aggregate_values_by_unknown_field_fails_before_scan_budget_consumption() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_3391, 7, 10),
            (8_3392, 7, 20),
            (8_3393, 7, 30),
            (8_3394, 8, 99),
        ],
    );
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by_desc("id")
            .offset(0)
            .limit(3)
    };

    let (result, scanned_rows) =
        capture_rows_scanned_for_entity(SessionAggregateEntity::PATH, || {
            load_window().values_by("missing_field")
        });
    let Err(err) = result else {
        panic!("session values_by(missing_field) should be rejected");
    };

    assert!(
        matches!(err, QueryError::Execute(_)),
        "session unknown-field projection should remain an execute-domain error: {err:?}",
    );
    assert_eq!(
        scanned_rows, 0,
        "session unknown-field projection should fail before scan-budget consumption",
    );
    assert!(
        err.to_string().contains("unknown aggregate target field"),
        "session unknown-field projection should preserve explicit field taxonomy: {err:?}",
    );
}

#[test]
fn session_aggregate_take_matches_execute_prefix() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_3601, 7, 10),
            (8_3602, 7, 20),
            (8_3603, 7, 30),
            (8_3604, 7, 40),
            (8_3605, 7, 50),
            (8_3606, 8, 99),
        ],
    );
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by_desc("id")
            .offset(1)
            .limit(4)
    };

    let expected = load_window()
        .execute()
        .expect("baseline execute for session aggregate take should succeed");
    let take_two = load_window()
        .take(2)
        .expect("session aggregate take(2) should succeed");
    let take_ten = load_window()
        .take(10)
        .expect("session aggregate take(10) should succeed");

    assert_eq!(
        session_aggregate_ids(&take_two),
        session_aggregate_ids(&expected)
            .into_iter()
            .take(2)
            .collect::<Vec<_>>(),
        "session aggregate take(2) should match the execute() prefix",
    );
    assert_eq!(
        session_aggregate_ids(&take_ten),
        session_aggregate_ids(&expected),
        "session aggregate take(k) above response size should preserve the full response",
    );
}

#[test]
fn session_aggregate_top_and_bottom_k_match_execute_field_ordering() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_3701, 7, 20),
            (8_3702, 7, 40),
            (8_3703, 7, 40),
            (8_3704, 7, 10),
            (8_3705, 7, 30),
            (8_3706, 8, 99),
        ],
    );
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by_desc("id")
            .offset(0)
            .limit(5)
    };
    let expected = load_window()
        .execute()
        .expect("baseline execute for ranked session aggregate parity should succeed");
    let mut descending_rank = expected
        .iter()
        .map(|row| (row.entity_ref().rank, row.id().key()))
        .collect::<Vec<_>>();
    let mut ascending_rank = descending_rank.clone();
    descending_rank.sort_unstable_by(|(left_rank, left_id), (right_rank, right_id)| {
        right_rank
            .cmp(left_rank)
            .then_with(|| left_id.cmp(right_id))
    });
    ascending_rank.sort_unstable_by(|(left_rank, left_id), (right_rank, right_id)| {
        left_rank
            .cmp(right_rank)
            .then_with(|| left_id.cmp(right_id))
    });

    let actual_top = load_window()
        .top_k_by("rank", 3)
        .expect("session aggregate top_k_by(rank, 3) should succeed");
    let actual_bottom = load_window()
        .bottom_k_by("rank", 3)
        .expect("session aggregate bottom_k_by(rank, 3) should succeed");

    assert_eq!(
        session_aggregate_ids(&actual_top),
        descending_rank
            .into_iter()
            .take(3)
            .map(|(_, id)| id)
            .collect::<Vec<_>>(),
        "session aggregate top_k_by(rank, 3) should match deterministic rank-desc ordering",
    );
    assert_eq!(
        session_aggregate_ids(&actual_bottom),
        ascending_rank
            .into_iter()
            .take(3)
            .map(|(_, id)| id)
            .collect::<Vec<_>>(),
        "session aggregate bottom_k_by(rank, 3) should match deterministic rank-asc ordering",
    );
}

#[test]
fn session_aggregate_ranked_projection_terminals_match_ranked_rows() {
    let cases = [
        (
            &[
                (8_3771, 7, 20),
                (8_3772, 7, 40),
                (8_3773, 7, 40),
                (8_3774, 7, 10),
                (8_3775, 7, 30),
                (8_3776, 8, 99),
            ][..],
            SessionAggregateRankTerminal::Top,
            SessionAggregateRankOutput::Values,
        ),
        (
            &[
                (8_3781, 7, 20),
                (8_3782, 7, 40),
                (8_3783, 7, 40),
                (8_3784, 7, 10),
                (8_3785, 7, 30),
                (8_3786, 8, 99),
            ][..],
            SessionAggregateRankTerminal::Bottom,
            SessionAggregateRankOutput::Values,
        ),
        (
            &[
                (8_3807, 7, 20),
                (8_3808, 7, 40),
                (8_3809, 7, 40),
                (8_3810, 7, 10),
                (8_3811, 7, 30),
                (8_3812, 8, 99),
            ][..],
            SessionAggregateRankTerminal::Top,
            SessionAggregateRankOutput::ValuesWithIds,
        ),
        (
            &[
                (8_3813, 7, 20),
                (8_3814, 7, 40),
                (8_3815, 7, 40),
                (8_3816, 7, 10),
                (8_3817, 7, 30),
                (8_3818, 8, 99),
            ][..],
            SessionAggregateRankTerminal::Bottom,
            SessionAggregateRankOutput::ValuesWithIds,
        ),
    ];

    // Phase 1: use the ranked row response as the parity baseline for each case.
    for (rows, terminal, output) in cases {
        reset_session_sql_store();
        let session = sql_session();
        seed_session_aggregate_entities(&session, rows);
        let load_window = || {
            session
                .load::<SessionAggregateEntity>()
                .filter(session_aggregate_group_predicate(7))
                .order_by_desc("id")
                .offset(0)
                .limit(5)
        };
        let ranked_rows = match terminal {
            SessionAggregateRankTerminal::Top => load_window()
                .top_k_by("rank", 3)
                .expect("session aggregate top_k_by(rank, 3) should succeed"),
            SessionAggregateRankTerminal::Bottom => load_window()
                .bottom_k_by("rank", 3)
                .expect("session aggregate bottom_k_by(rank, 3) should succeed"),
        };

        // Phase 2: compare the projection terminal against ranked-row projection.
        let actual = run_session_aggregate_rank_terminal(&session, terminal, output)
            .expect("session ranked projection terminal should succeed");
        let expected = match output {
            SessionAggregateRankOutput::Values => {
                SessionAggregateResult::Values(session_aggregate_values_by_rank(&ranked_rows))
            }
            SessionAggregateRankOutput::ValuesWithIds => SessionAggregateResult::ValuesWithIds(
                session_aggregate_values_by_rank_with_ids(&ranked_rows),
            ),
        };
        assert_eq!(
            actual, expected,
            "session ranked projection terminal should match ranked-row projection",
        );
    }
}

#[test]
fn session_aggregate_terminal_value_projection_matches_execute_projection() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_3511, 7, 10),
            (8_3512, 7, 10),
            (8_3513, 7, 20),
            (8_3514, 7, 30),
            (8_3515, 7, 40),
            (8_3516, 8, 99),
        ],
    );
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by_desc("id")
            .offset(1)
            .limit(4)
    };

    let expected = load_window()
        .execute()
        .expect("baseline execute for first/last value projection should succeed");
    let first = load_window()
        .first_value_by("rank")
        .expect("session aggregate first_value_by(rank) should succeed");
    let last = load_window()
        .last_value_by("rank")
        .expect("session aggregate last_value_by(rank) should succeed");

    assert_eq!(
        first,
        session_aggregate_first_value_by_rank(&expected),
        "session aggregate first_value_by(rank) should match execute() projection order",
    );
    assert_eq!(
        last,
        session_aggregate_last_value_by_rank(&expected),
        "session aggregate last_value_by(rank) should match execute() projection order",
    );
}

#[test]
#[expect(clippy::too_many_lines)]
fn session_aggregate_ranked_terminals_are_invariant_to_base_order_direction() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_3711, 7, 10),
            (8_3712, 7, 40),
            (8_3713, 7, 20),
            (8_3714, 7, 30),
            (8_3715, 7, 40),
            (8_3716, 8, 99),
        ],
    );

    // Phase 1: capture the ascending base-order outputs.
    let asc_top_ids = SessionAggregateResult::Ids(session_aggregate_ids(
        &session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by("id")
            .top_k_by("rank", 3)
            .expect("ascending session aggregate top_k_by should succeed"),
    ));
    let asc_bottom_ids = SessionAggregateResult::Ids(session_aggregate_ids(
        &session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by("id")
            .bottom_k_by("rank", 3)
            .expect("ascending session aggregate bottom_k_by should succeed"),
    ));
    let asc_top_values = SessionAggregateResult::Values(
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by("id")
            .top_k_by_values("rank", 3)
            .expect("ascending session aggregate top_k_by_values should succeed"),
    );
    let asc_bottom_values = SessionAggregateResult::Values(
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by("id")
            .bottom_k_by_values("rank", 3)
            .expect("ascending session aggregate bottom_k_by_values should succeed"),
    );
    let asc_top_values_with_ids = SessionAggregateResult::ValuesWithIds(
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by("id")
            .top_k_by_with_ids("rank", 3)
            .expect("ascending session aggregate top_k_by_with_ids should succeed")
            .into_iter()
            .map(|(id, value)| (id.key(), value))
            .collect(),
    );
    let asc_bottom_values_with_ids = SessionAggregateResult::ValuesWithIds(
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by("id")
            .bottom_k_by_with_ids("rank", 3)
            .expect("ascending session aggregate bottom_k_by_with_ids should succeed")
            .into_iter()
            .map(|(id, value)| (id.key(), value))
            .collect(),
    );

    // Phase 2: assert parity against descending base-order outputs.
    assert_eq!(
        asc_top_ids,
        SessionAggregateResult::Ids(session_aggregate_ids(
            &session
                .load::<SessionAggregateEntity>()
                .filter(session_aggregate_group_predicate(7))
                .order_by_desc("id")
                .top_k_by("rank", 3)
                .expect("descending session aggregate top_k_by should succeed"),
        )),
        "session aggregate top_k_by(rank, 3) should be invariant to base order direction",
    );
    assert_eq!(
        asc_bottom_ids,
        SessionAggregateResult::Ids(session_aggregate_ids(
            &session
                .load::<SessionAggregateEntity>()
                .filter(session_aggregate_group_predicate(7))
                .order_by_desc("id")
                .bottom_k_by("rank", 3)
                .expect("descending session aggregate bottom_k_by should succeed"),
        )),
        "session aggregate bottom_k_by(rank, 3) should be invariant to base order direction",
    );
    assert_eq!(
        asc_top_values,
        SessionAggregateResult::Values(
            session
                .load::<SessionAggregateEntity>()
                .filter(session_aggregate_group_predicate(7))
                .order_by_desc("id")
                .top_k_by_values("rank", 3)
                .expect("descending session aggregate top_k_by_values should succeed"),
        ),
        "session aggregate top_k_by_values(rank, 3) should be invariant to base order direction",
    );
    assert_eq!(
        asc_bottom_values,
        SessionAggregateResult::Values(
            session
                .load::<SessionAggregateEntity>()
                .filter(session_aggregate_group_predicate(7))
                .order_by_desc("id")
                .bottom_k_by_values("rank", 3)
                .expect("descending session aggregate bottom_k_by_values should succeed"),
        ),
        "session aggregate bottom_k_by_values(rank, 3) should be invariant to base order direction",
    );
    assert_eq!(
        asc_top_values_with_ids,
        SessionAggregateResult::ValuesWithIds(
            session
                .load::<SessionAggregateEntity>()
                .filter(session_aggregate_group_predicate(7))
                .order_by_desc("id")
                .top_k_by_with_ids("rank", 3)
                .expect("descending session aggregate top_k_by_with_ids should succeed")
                .into_iter()
                .map(|(id, value)| (id.key(), value))
                .collect(),
        ),
        "session aggregate top_k_by_with_ids(rank, 3) should be invariant to base order direction",
    );
    assert_eq!(
        asc_bottom_values_with_ids,
        SessionAggregateResult::ValuesWithIds(
            session
                .load::<SessionAggregateEntity>()
                .filter(session_aggregate_group_predicate(7))
                .order_by_desc("id")
                .bottom_k_by_with_ids("rank", 3)
                .expect("descending session aggregate bottom_k_by_with_ids should succeed")
                .into_iter()
                .map(|(id, value)| (id.key(), value))
                .collect(),
        ),
        "session aggregate bottom_k_by_with_ids(rank, 3) should be invariant to base order direction",
    );
}

#[test]
fn session_aggregate_bytes_matches_execute_window_persisted_payload_sum() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_951, 7, 10),
            (8_952, 7, 20),
            (8_953, 7, 35),
            (8_954, 8, 99),
            (8_955, 7, 50),
        ],
    );
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by("rank")
            .offset(1)
            .limit(2)
    };

    let expected_ids = load_window()
        .execute()
        .expect("baseline execute for session bytes parity should succeed")
        .ids()
        .map(|id| id.key())
        .collect();
    let expected_bytes = session_aggregate_persisted_payload_bytes_for_ids(expected_ids);
    let actual_bytes = load_window()
        .bytes()
        .expect("session bytes terminal should succeed");

    assert_eq!(
        actual_bytes, expected_bytes,
        "session bytes parity should match persisted payload byte sum of the effective window",
    );
}

#[test]
fn session_aggregate_bytes_empty_window_returns_zero() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(&session, &[(8_961, 7, 10), (8_962, 7, 20), (8_963, 8, 99)]);

    let actual_bytes = session
        .load::<SessionAggregateEntity>()
        .filter(session_aggregate_group_predicate(999))
        .order_by("rank")
        .bytes()
        .expect("session bytes terminal should succeed for empty windows");

    assert_eq!(
        actual_bytes, 0,
        "session bytes terminal should return zero for empty windows",
    );
}

#[test]
fn session_aggregate_bytes_by_matches_execute_window_serialized_field_sum() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_971, 7, 10),
            (8_972, 7, 20),
            (8_973, 7, 35),
            (8_974, 8, 99),
            (8_975, 7, 50),
        ],
    );
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by("rank")
            .offset(1)
            .limit(2)
    };

    let expected_response = load_window()
        .execute()
        .expect("baseline execute for session bytes_by parity should succeed");
    let expected_bytes =
        session_aggregate_serialized_field_payload_bytes_for_rows(&expected_response, "rank");
    let actual_bytes = load_window()
        .bytes_by("rank")
        .expect("session bytes_by(rank) terminal should succeed");

    assert_eq!(
        actual_bytes, expected_bytes,
        "session bytes_by(rank) parity should match serialized field byte sum of the effective window",
    );
}

#[test]
fn session_aggregate_bytes_by_empty_window_returns_zero() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(&session, &[(8_991, 7, 10), (8_992, 7, 20), (8_993, 8, 99)]);

    let actual_bytes = session
        .load::<SessionAggregateEntity>()
        .filter(session_aggregate_group_predicate(999))
        .order_by("rank")
        .bytes_by("rank")
        .expect("session bytes_by(rank) terminal should succeed for empty windows");

    assert_eq!(
        actual_bytes, 0,
        "session bytes_by(rank) terminal should return zero for empty windows",
    );
}

#[test]
fn session_aggregate_bytes_by_unknown_field_fails_before_scan_budget_consumption() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_901, 7, 10),
            (8_902, 7, 20),
            (8_903, 7, 30),
            (8_904, 8, 99),
        ],
    );
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by_desc("id")
            .offset(0)
            .limit(3)
    };

    let (result, scanned_rows) =
        capture_rows_scanned_for_entity(SessionAggregateEntity::PATH, || {
            load_window().bytes_by("missing_field")
        });
    let Err(err) = result else {
        panic!("session bytes_by(missing_field) should be rejected");
    };

    assert!(
        matches!(err, QueryError::Execute(_)),
        "session unknown-field bytes_by should remain an execute-domain error: {err:?}",
    );
    assert_eq!(
        scanned_rows, 0,
        "session unknown-field bytes_by should fail before scan-budget consumption",
    );
    assert!(
        err.to_string().contains("unknown aggregate target field"),
        "session unknown-field bytes_by should preserve explicit field taxonomy: {err:?}",
    );
}

#[test]
fn session_aggregate_explain_bytes_by_projects_terminal_metadata_for_filtered_shape() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_905, 7, 20),
            (8_906, 7, 20),
            (8_907, 7, 30),
            (8_908, 8, 20),
        ],
    );

    let descriptor = session
        .load::<SessionAggregateEntity>()
        .filter(Predicate::And(vec![
            session_aggregate_group_predicate(7),
            Predicate::Compare(ComparePredicate::with_coercion(
                "rank",
                CompareOp::Eq,
                Value::from(20u64),
                CoercionId::Strict,
            )),
        ]))
        .explain_bytes_by("rank")
        .expect("session bytes_by explain should succeed for filtered shapes");

    assert_eq!(
        descriptor.node_properties().get("terminal"),
        Some(&Value::from("bytes_by")),
        "session bytes_by explain should project the terminal label",
    );
    assert_eq!(
        descriptor.node_properties().get("terminal_field"),
        Some(&Value::from("rank")),
        "session bytes_by explain should preserve the requested terminal field",
    );
    assert_eq!(
        descriptor.node_properties().get("terminal_projection_mode"),
        Some(&Value::from("field_materialized")),
        "filtered session bytes_by explain should project the current materialized mode label",
    );
    assert_eq!(
        descriptor.node_properties().get("terminal_index_only"),
        Some(&Value::from(false)),
        "filtered session bytes_by explain should project index-only=false under current planner access",
    );
}

#[test]
fn session_aggregate_explain_bytes_by_projects_materialized_mode_for_strict_queries() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(&session, &[(8_911, 7, 20), (8_912, 7, 20), (8_913, 8, 30)]);

    let descriptor = session
        .load_with_consistency::<SessionAggregateEntity>(crate::db::MissingRowPolicy::Error)
        .filter(Predicate::And(vec![
            session_aggregate_group_predicate(7),
            Predicate::Compare(ComparePredicate::with_coercion(
                "rank",
                CompareOp::Eq,
                Value::from(20u64),
                CoercionId::Strict,
            )),
        ]))
        .explain_bytes_by("rank")
        .expect("session bytes_by explain should succeed for strict load shapes");

    assert_eq!(
        descriptor.node_properties().get("terminal_projection_mode"),
        Some(&Value::from("field_materialized")),
        "strict session bytes_by explain should fail closed to materialized projection mode",
    );
    assert_eq!(
        descriptor.node_properties().get("terminal_index_only"),
        Some(&Value::from(false)),
        "strict session bytes_by explain should project index-only=false",
    );
}

#[test]
fn session_aggregate_explain_bytes_by_unknown_field_fails_before_planning() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(&session, &[(8_914, 7, 10), (8_915, 7, 20)]);

    let result = session
        .load::<SessionAggregateEntity>()
        .filter(session_aggregate_group_predicate(7))
        .explain_bytes_by("missing_field");

    let Err(err) = result else {
        panic!("session bytes_by explain for unknown fields should fail closed");
    };
    assert!(
        matches!(err, QueryError::Execute(_)),
        "session unknown-field bytes_by explain should remain an execute-domain failure: {err:?}"
    );
    assert!(
        err.to_string().contains("unknown aggregate target field"),
        "session unknown-field bytes_by explain should preserve field taxonomy: {err:?}",
    );
}

#[test]
fn session_aggregate_terminal_explain_reports_standard_route_for_exists() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (9_421, 7, 10),
            (9_422, 7, 20),
            (9_423, 7, 30),
            (9_424, 8, 99),
        ],
    );

    let exists_terminal_plan = session
        .load::<SessionAggregateEntity>()
        .filter(session_aggregate_group_predicate(7))
        .order_by("rank")
        .order_by("id")
        .explain_exists()
        .expect("session explain_exists should succeed");

    assert_eq!(exists_terminal_plan.terminal(), AggregateKind::Exists);
    assert!(matches!(
        exists_terminal_plan.execution().ordering_source(),
        crate::db::ExplainExecutionOrderingSource::AccessOrder
            | crate::db::ExplainExecutionOrderingSource::Materialized
    ));

    let exists_execution = exists_terminal_plan.execution();
    assert_eq!(exists_execution.aggregation(), AggregateKind::Exists);
    assert!(matches!(
        exists_execution.ordering_source(),
        crate::db::ExplainExecutionOrderingSource::AccessOrder
            | crate::db::ExplainExecutionOrderingSource::Materialized
    ));
    assert_eq!(
        exists_execution.access_strategy(),
        exists_terminal_plan.query().access()
    );
    assert!(matches!(
        exists_execution.execution_mode(),
        crate::db::ExplainExecutionMode::Streaming | crate::db::ExplainExecutionMode::Materialized
    ));
    assert_eq!(exists_execution.limit(), None);
    assert!(!exists_execution.cursor());
    assert!(
        !exists_execution.covering_projection(),
        "ordered exists explain shape should not mark index-only covering projection",
    );
    let exists_node = exists_terminal_plan.execution_node_descriptor();
    assert_eq!(
        exists_node.node_type(),
        crate::db::ExplainExecutionNodeType::AggregateExists
    );
    assert_eq!(
        exists_node.execution_mode(),
        exists_execution.execution_mode()
    );
    assert_eq!(
        exists_node.access_strategy(),
        Some(exists_execution.access_strategy())
    );
    assert!(
        exists_node
            .render_text_tree()
            .contains("AggregateExists execution_mode="),
        "text tree should render the standard aggregate node label",
    );
}

#[test]
fn session_aggregate_terminal_explain_not_exists_alias_matches_exists_plan() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (9_431, 7, 10),
            (9_432, 7, 20),
            (9_433, 7, 30),
            (9_434, 8, 99),
        ],
    );
    let query = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by("rank")
            .order_by("id")
    };

    let exists_plan = query()
        .explain_exists()
        .expect("session explain_exists should succeed");
    let not_exists_plan = query()
        .explain_not_exists()
        .expect("session explain_not_exists should succeed");

    assert_eq!(
        not_exists_plan.terminal(),
        AggregateKind::Exists,
        "not_exists explain alias should remain backed by exists terminal execution",
    );
    assert_eq!(
        session_aggregate_terminal_plan_snapshot(&not_exists_plan),
        session_aggregate_terminal_plan_snapshot(&exists_plan),
        "not_exists explain alias must remain plan-identical to exists explain",
    );
}

#[test]
fn session_sql_global_aggregate_explain_execution_stays_off_secondary_authority_surface() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(&session, &[(9_451, 7, 10), (9_452, 7, 20), (9_453, 8, 99)]);

    let explain = dispatch_explain_sql::<SessionAggregateEntity>(
        &session,
        "EXPLAIN EXECUTION SELECT COUNT(*) FROM SessionAggregateEntity",
    )
    .expect("global aggregate EXPLAIN EXECUTION should succeed");

    assert!(
        !explain.contains("authority_decision")
            && !explain.contains("authority_reason")
            && !explain.contains("index_state"),
        "aggregate EXPLAIN EXECUTION should stay off the removed secondary-read label surface",
    );
}

#[test]
fn session_sql_filtered_global_aggregate_explain_execution_hides_non_ready_secondary_indexes_from_planner_visibility()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(
        &session,
        &[("Sam", 30), ("Sasha", 24), ("Soren", 18), ("Mira", 40)],
    );
    let sql = "EXPLAIN EXECUTION SELECT COUNT(*) FROM IndexedSessionSqlEntity WHERE name = 'Sam'";

    let ready_explain = dispatch_explain_sql::<IndexedSessionSqlEntity>(&session, sql)
        .expect("filtered aggregate EXPLAIN EXECUTION should succeed while the index is ready");
    assert!(
        ready_explain.contains("AggregateCount execution_mode=")
            && ready_explain.contains("access=IndexPrefix"),
        "ready filtered aggregate EXPLAIN EXECUTION should keep the planner-visible name index: {ready_explain}",
    );
    assert!(
        !ready_explain.contains("access=FullScan")
            && !ready_explain.contains("authority_decision")
            && !ready_explain.contains("authority_reason")
            && !ready_explain.contains("index_state"),
        "ready filtered aggregate EXPLAIN EXECUTION should stay off both the full-scan fallback and the removed secondary-read label surface: {ready_explain}",
    );

    mark_indexed_session_sql_index_building();

    let building_explain = dispatch_explain_sql::<IndexedSessionSqlEntity>(&session, sql)
        .expect("filtered aggregate EXPLAIN EXECUTION should still succeed once the shared index becomes building");
    assert!(
        building_explain.contains("AggregateCount execution_mode=")
            && building_explain.contains("access=FullScan"),
        "building filtered aggregate EXPLAIN EXECUTION should fall back to FullScan once the name index becomes planner-invisible: {building_explain}",
    );
    assert!(
        !building_explain.contains("access=IndexPrefix")
            && !building_explain.contains("authority_decision")
            && !building_explain.contains("authority_reason")
            && !building_explain.contains("index_state"),
        "building filtered aggregate EXPLAIN EXECUTION should not keep the hidden index or any removed secondary-read labels: {building_explain}",
    );
}

#[test]
fn session_aggregate_exists_explain_hides_non_ready_secondary_indexes_from_planner_visibility() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_session_explain_entities(
        &session,
        &[
            (9_461, 7, 10),
            (9_462, 7, 20),
            (9_463, 7, 30),
            (9_464, 8, 99),
        ],
    );
    let load_window = || {
        session
            .load::<SessionExplainEntity>()
            .filter(Predicate::Compare(ComparePredicate::with_coercion(
                "group",
                CompareOp::Eq,
                Value::from(7u64),
                CoercionId::Strict,
            )))
            .order_by("rank")
            .order_by("id")
    };

    let ready_plan = load_window()
        .explain_exists()
        .expect("indexed aggregate exists explain should succeed while the index is ready");
    assert!(
        matches!(
            ready_plan.query().access(),
            ExplainAccessPath::IndexPrefix { name, .. } if *name == "group_rank"
        ),
        "ready aggregate exists planning should keep the composite secondary index visible",
    );
    assert_eq!(
        ready_plan.execution().access_strategy(),
        ready_plan.query().access(),
        "ready aggregate exists execution should inherit the planner-owned access path",
    );

    mark_indexed_session_sql_index_building();

    let building_plan = load_window().explain_exists().expect(
        "aggregate exists explain should still succeed when the shared index becomes building",
    );
    assert!(
        matches!(building_plan.query().access(), ExplainAccessPath::FullScan),
        "non-ready aggregate exists planning must hide the secondary index instead of planning a downgraded shortcut",
    );
    assert_eq!(
        building_plan.execution().access_strategy(),
        building_plan.query().access(),
        "non-ready aggregate exists execution should inherit the fallback planner path",
    );
    assert!(
        load_window()
            .exists()
            .expect("aggregate exists should still execute after planner visibility fallback"),
        "planner visibility fallback must preserve aggregate exists correctness",
    );
}

#[test]
fn session_aggregate_terminal_explain_first_last_preserve_order_shape_parity() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (9_441, 7, 10),
            (9_442, 7, 20),
            (9_443, 7, 30),
            (9_444, 8, 99),
        ],
    );
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by("rank")
            .order_by("id")
    };

    let first_plan = load_window()
        .explain_first()
        .expect("session explain_first should succeed");
    let last_plan = load_window()
        .explain_last()
        .expect("session explain_last should succeed");

    assert_eq!(first_plan.terminal(), AggregateKind::First);
    assert_eq!(last_plan.terminal(), AggregateKind::Last);
    assert_eq!(
        first_plan.execution().ordering_source(),
        crate::db::ExplainExecutionOrderingSource::Materialized,
        "first explain should remain on the materialized ordering source",
    );
    assert_eq!(
        last_plan.execution().ordering_source(),
        crate::db::ExplainExecutionOrderingSource::Materialized,
        "last explain should remain on the materialized ordering source",
    );
    assert_eq!(
        first_plan.query().access(),
        last_plan.query().access(),
        "first vs last explain should preserve access-shape parity for equivalent windows",
    );
    assert_eq!(first_plan.query().order_by(), last_plan.query().order_by());
    assert_eq!(first_plan.query().page(), last_plan.query().page());
    assert_eq!(first_plan.query().grouping(), last_plan.query().grouping());
    assert_eq!(
        first_plan.query().order_pushdown(),
        last_plan.query().order_pushdown()
    );
    assert_eq!(
        first_plan.query().consistency(),
        last_plan.query().consistency()
    );
    assert_eq!(
        first_plan.execution().access_strategy(),
        last_plan.execution().access_strategy(),
    );
    assert_eq!(
        first_plan.execution().execution_mode(),
        last_plan.execution().execution_mode(),
        "first vs last explains should agree on execution-mode classification",
    );
    assert_eq!(
        first_plan.execution().ordering_source(),
        last_plan.execution().ordering_source(),
        "first vs last explains should agree on ordering-source classification",
    );
    assert_eq!(first_plan.execution().limit(), None);
    assert_eq!(last_plan.execution().limit(), None);
    assert!(!first_plan.execution().cursor());
    assert!(!last_plan.execution().cursor());

    let first_node = first_plan.execution_node_descriptor();
    let last_node = last_plan.execution_node_descriptor();
    assert_eq!(
        first_node.node_type(),
        crate::db::ExplainExecutionNodeType::AggregateFirst
    );
    assert_eq!(
        last_node.node_type(),
        crate::db::ExplainExecutionNodeType::AggregateLast
    );
    assert_eq!(first_node.execution_mode(), last_node.execution_mode());
    assert_eq!(first_node.access_strategy(), last_node.access_strategy());
    assert_eq!(first_node.ordering_source(), last_node.ordering_source());
    assert_eq!(first_node.limit(), last_node.limit());
    assert_eq!(first_node.cursor(), last_node.cursor());
    assert_eq!(first_node.covering_scan(), last_node.covering_scan());
    assert_eq!(first_node.rows_expected(), last_node.rows_expected());
    assert_eq!(
        first_node.node_properties(),
        last_node.node_properties(),
        "first vs last descriptor metadata should remain stable for equivalent windows",
    );
}

// Matrix-style explain contract test that keeps strict-pushdown, residual, and
// limit-zero behavior together on one session-local indexed fixture.
#[expect(clippy::too_many_lines)]
#[test]
fn session_explain_execution_predicate_stage_and_limit_zero_matrix_is_stable() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(
        &session,
        &[("Sam", 30), ("Sasha", 24), ("Soren", 18), ("Mira", 40)],
    );

    let strict_prefilter = session
        .load::<IndexedSessionSqlEntity>()
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Eq,
            Value::Text("Sam".to_string()),
            CoercionId::Strict,
        )))
        .order_by("name")
        .order_by("id")
        .explain_execution()
        .expect("strict indexed prefilter explain_execution should succeed");
    assert!(
        explain_execution_contains_node_type(
            &strict_prefilter,
            ExplainExecutionNodeType::IndexPredicatePrefilter,
        ),
        "strict index-compatible predicate should emit a prefilter stage node",
    );
    assert!(
        !explain_execution_contains_node_type(
            &strict_prefilter,
            ExplainExecutionNodeType::ResidualPredicateFilter,
        ),
        "strict index-compatible predicate should not emit a residual stage node",
    );
    let strict_prefilter_node = explain_execution_find_first_node(
        &strict_prefilter,
        ExplainExecutionNodeType::IndexPredicatePrefilter,
    )
    .expect("strict index-compatible predicate should project a prefilter node");
    assert!(
        strict_prefilter_node
            .node_properties()
            .contains_key("pushdown"),
        "strict prefilter node should expose pushed predicate summary metadata",
    );

    let residual = session
        .load::<IndexedSessionSqlEntity>()
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::Eq,
                Value::Text("Sasha".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "age",
                CompareOp::Eq,
                Value::Uint(24),
                CoercionId::Strict,
            )),
        ]))
        .order_by("name")
        .order_by("id")
        .explain_execution()
        .expect("mixed indexed and non-indexed predicate explain_execution should succeed");
    assert!(
        explain_execution_contains_node_type(
            &residual,
            ExplainExecutionNodeType::ResidualPredicateFilter,
        ),
        "mixed index/non-index predicate should emit a residual stage node",
    );
    let residual_node = explain_execution_find_first_node(
        &residual,
        ExplainExecutionNodeType::ResidualPredicateFilter,
    )
    .expect("mixed index/non-index predicate should project a residual node");
    assert!(
        residual_node.predicate_pushdown().is_some(),
        "residual node should report pushed access predicate separately from the residual filter",
    );

    let limit_zero = session
        .load::<IndexedSessionSqlEntity>()
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Eq,
            Value::Text("Sam".to_string()),
            CoercionId::Strict,
        )))
        .order_by("name")
        .order_by("id")
        .limit(0)
        .explain_execution()
        .expect("limit-zero explain_execution should succeed");
    if let Some(top_n_node) =
        explain_execution_find_first_node(&limit_zero, ExplainExecutionNodeType::TopNSeek)
    {
        assert_eq!(
            top_n_node.node_properties().get("fetch"),
            Some(&Value::from(0u64)),
            "limit-zero top-n nodes should freeze the fetch=0 contract",
        );
    } else {
        assert!(
            explain_execution_contains_node_type(
                &limit_zero,
                ExplainExecutionNodeType::OrderByMaterializedSort,
            ),
            "limit-zero routes without top-n seek should still expose materialized order fallback",
        );
    }
    let limit_node =
        explain_execution_find_first_node(&limit_zero, ExplainExecutionNodeType::LimitOffset)
            .expect("limit-zero route should emit a limit/offset node");
    assert_eq!(limit_node.limit(), Some(0));
}

#[test]
fn session_explain_execution_access_root_matrix_is_stable() {
    reset_session_sql_store();
    let session = sql_session();
    session
        .insert(SessionSqlEntity {
            id: Ulid::from_u128(9_701),
            name: "alpha".to_string(),
            age: 21,
        })
        .expect("by-key session seed should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::from_u128(9_702),
            name: "beta".to_string(),
            age: 22,
        })
        .expect("by-key session seed should succeed");

    let by_key = session
        .load::<SessionSqlEntity>()
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::Eq,
            Value::Ulid(Ulid::from_u128(9_701)),
            CoercionId::Strict,
        )))
        .order_by("id")
        .explain_execution()
        .expect("by-key explain_execution should succeed");
    assert_eq!(
        by_key.node_type(),
        ExplainExecutionNodeType::ByKeyLookup,
        "single id predicate should keep by-key execution root",
    );

    reset_indexed_session_sql_store();
    let indexed_session = indexed_sql_session();
    seed_indexed_session_sql_entities(
        &indexed_session,
        &[("Sam", 30), ("Sasha", 24), ("Soren", 18), ("Mira", 40)],
    );

    let prefix = indexed_session
        .load::<IndexedSessionSqlEntity>()
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Eq,
            Value::Text("Sam".to_string()),
            CoercionId::Strict,
        )))
        .order_by("name")
        .order_by("id")
        .explain_execution()
        .expect("index-prefix explain_execution should succeed");
    assert_eq!(
        prefix.node_type(),
        ExplainExecutionNodeType::IndexPrefixScan,
        "strict equality on the indexed field should keep index-prefix root",
    );

    let multi = indexed_session
        .load::<IndexedSessionSqlEntity>()
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::In,
            Value::List(vec![
                Value::Text("Sam".to_string()),
                Value::Text("Sasha".to_string()),
            ]),
            CoercionId::Strict,
        )))
        .order_by("name")
        .order_by("id")
        .explain_execution()
        .expect("index-multi explain_execution should succeed");
    assert_eq!(
        multi.node_type(),
        ExplainExecutionNodeType::IndexMultiLookup,
        "IN predicate on the indexed field should keep index-multi root",
    );
    assert_eq!(
        multi.node_properties().get("prefix_values"),
        Some(&Value::List(vec![
            Value::Text("Sam".to_string()),
            Value::Text("Sasha".to_string()),
        ])),
        "index-multi roots should expose canonical IN prefix values",
    );
}

#[test]
fn session_explain_execution_covering_scan_requires_coverable_projection_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let entity_descriptor = session
        .load::<IndexedSessionSqlEntity>()
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Eq,
            Value::Text("Sam".to_string()),
            CoercionId::Strict,
        )))
        .explain_execution()
        .expect("unordered strict index-prefix entity explain_execution should succeed");

    assert_eq!(
        entity_descriptor.covering_scan(),
        Some(false),
        "all-field entity loads should stay on the materialized route even when access stays index-backed",
    );
    assert_eq!(
        entity_descriptor.node_properties().get("cov_scan_reason"),
        Some(&Value::Text("proj_not_cov".to_string())),
        "entity explain roots should report the non-coverable projection reason explicitly",
    );
    assert_eq!(
        entity_descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("materialized".to_string())),
        "entity explain roots should expose the materialized covering-read route label",
    );

    let projected_descriptor = session
        .query_from_sql::<IndexedSessionSqlEntity>(
            "SELECT id, name FROM IndexedSessionSqlEntity WHERE name = 'Sam' ORDER BY id ASC LIMIT 1",
        )
        .expect("coverable SQL projection query should lower")
        .explain_execution()
        .expect("coverable SQL projection explain_execution should succeed");

    assert_eq!(
        projected_descriptor.covering_scan(),
        Some(true),
        "coverable projected reads should report the explicit covering-read route",
    );
    assert_eq!(
        projected_descriptor
            .node_properties()
            .get("cov_scan_reason"),
        Some(&Value::Text("cover_read_route".to_string())),
        "coverable projection roots should report the covering-read route reason",
    );
    assert_eq!(
        projected_descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "coverable projection roots should expose the explicit covering-read route label",
    );
    let projection_node = explain_execution_find_first_node(
        &projected_descriptor,
        ExplainExecutionNodeType::CoveringRead,
    )
    .expect("coverable projection explain trees should emit an explicit covering-read node");
    assert_eq!(
        projection_node.projection(),
        Some("covering_read"),
        "projection node should label the covering-read terminal route explicitly",
    );
    assert_eq!(
        projection_node.node_properties().get("covering_order"),
        Some(&Value::Text("primary_key_asc".to_string())),
        "projection node should report the planner-owned covering order contract",
    );
    assert_eq!(
        projection_node.node_properties().get("covering_fields"),
        Some(&Value::List(vec![
            Value::Text("id".to_string()),
            Value::Text("name".to_string()),
        ])),
        "projection node should expose the canonical projected field order",
    );
    assert_eq!(
        projection_node.node_properties().get("covering_sources"),
        Some(&Value::List(vec![
            Value::Text("primary_key".to_string()),
            Value::Text("constant".to_string()),
        ])),
        "projection node should expose planner-owned field-source metadata",
    );
    assert_eq!(
        projection_node.node_properties().get("existing_row_mode"),
        Some(&Value::Text("planner_proven".to_string())),
        "projection node should expose the planner-proven existing-row mode explicitly",
    );
}

#[test]
fn execute_sql_projection_primary_key_covering_full_scan_returns_ordered_ids() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed deterministic primary-key order.
    session
        .insert(SessionSqlEntity {
            id: Ulid::from_u128(9_801),
            name: "alpha".to_string(),
            age: 21,
        })
        .expect("PK-covering session seed should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::from_u128(9_802),
            name: "beta".to_string(),
            age: 22,
        })
        .expect("PK-covering session seed should succeed");

    // Phase 2: execute the PK-only projection through the SQL dispatch lane.
    let rows = dispatch_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT id FROM SessionSqlEntity ORDER BY id ASC LIMIT 1",
    )
    .expect("PK-only covering projection query should execute");

    // Phase 3: preserve the canonical ordered window on the projection output.
    assert_eq!(rows, vec![vec![Value::Ulid(Ulid::from_u128(9_801))]]);
}

#[test]
fn session_explain_execution_primary_key_covering_full_scan_is_planner_proven() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::from_u128(9_811),
            name: "alpha".to_string(),
            age: 21,
        })
        .expect("PK-covering session seed should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::from_u128(9_812),
            name: "beta".to_string(),
            age: 22,
        })
        .expect("PK-covering session seed should succeed");

    let descriptor = session
        .query_from_sql::<SessionSqlEntity>(
            "SELECT id FROM SessionSqlEntity ORDER BY id ASC LIMIT 1",
        )
        .expect("PK-only covering query should lower")
        .explain_execution()
        .expect("PK-only covering explain_execution should succeed");

    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "PK-only primary-store projection should expose the explicit covering route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "PK-only primary-store projection should surface the covering-read route label",
    );
    let projection_node =
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::CoveringRead)
            .expect("PK-only covering explain tree should emit a covering-read node");
    assert_eq!(
        projection_node.node_properties().get("covering_fields"),
        Some(&Value::List(vec![Value::Text("id".to_string())])),
        "PK-only covering explain should expose the projected field list",
    );
    assert_eq!(
        projection_node.node_properties().get("covering_sources"),
        Some(&Value::List(vec![Value::Text("primary_key".to_string())])),
        "PK-only covering explain should expose the primary-key field source",
    );
    assert_eq!(
        projection_node.node_properties().get("existing_row_mode"),
        Some(&Value::Text("planner_proven".to_string())),
        "PK-only primary-store covering should surface the planner-proven row mode",
    );
}

#[test]
fn session_explain_execution_primary_key_covering_by_key_is_row_check_required() {
    let query = crate::db::query::intent::Query::<SessionSqlEntity>::new(
        crate::db::predicate::MissingRowPolicy::Ignore,
    )
    .select_fields(["id"])
    .filter(Predicate::Compare(ComparePredicate::with_coercion(
        "id",
        CompareOp::Eq,
        Value::Ulid(Ulid::from_u128(9_811)),
        CoercionId::Strict,
    )))
    .order_by("id");

    let descriptor = query
        .explain_execution()
        .expect("PK-only covering by-key explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::ByKeyLookup,
        "PK-only exact-key projection should explain through the by-key root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "PK-only by-key projection should expose the explicit covering route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "PK-only by-key projection should surface the covering-read route label",
    );
    let projection_node =
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::CoveringRead)
            .expect("PK-only by-key explain tree should emit a covering-read node");
    assert_eq!(
        projection_node.node_properties().get("covering_fields"),
        Some(&Value::List(vec![Value::Text("id".to_string())])),
        "PK-only by-key explain should expose the projected field list",
    );
    assert_eq!(
        projection_node.node_properties().get("covering_sources"),
        Some(&Value::List(vec![Value::Text("primary_key".to_string())])),
        "PK-only by-key explain should expose the primary-key field source",
    );
    assert_eq!(
        projection_node.node_properties().get("existing_row_mode"),
        Some(&Value::Text("row_check_required".to_string())),
        "PK-only by-key covering should surface the explicit row-check mode",
    );
}

#[test]
fn session_explain_execution_primary_key_covering_by_keys_is_row_check_required() {
    let query = crate::db::query::intent::Query::<SessionSqlEntity>::new(
        crate::db::predicate::MissingRowPolicy::Ignore,
    )
    .select_fields(["id"])
    .filter(Predicate::Compare(ComparePredicate::with_coercion(
        "id",
        CompareOp::In,
        Value::List(vec![
            Value::Ulid(Ulid::from_u128(9_811)),
            Value::Ulid(Ulid::from_u128(9_813)),
        ]),
        CoercionId::Strict,
    )))
    .order_by("id");

    let descriptor = query
        .explain_execution()
        .expect("PK-only covering by-keys explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::ByKeysLookup,
        "PK-only exact-key-set projection should explain through the by-keys root",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "PK-only by-keys projection should expose the explicit covering route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "PK-only by-keys projection should surface the covering-read route label",
    );
    let projection_node =
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::CoveringRead)
            .expect("PK-only by-keys explain tree should emit a covering-read node");
    assert_eq!(
        projection_node.node_properties().get("covering_fields"),
        Some(&Value::List(vec![Value::Text("id".to_string())])),
        "PK-only by-keys explain should expose the projected field list",
    );
    assert_eq!(
        projection_node.node_properties().get("covering_sources"),
        Some(&Value::List(vec![Value::Text("primary_key".to_string())])),
        "PK-only by-keys explain should expose the primary-key field source",
    );
    assert_eq!(
        projection_node.node_properties().get("existing_row_mode"),
        Some(&Value::Text("row_check_required".to_string())),
        "PK-only by-keys covering should surface the explicit row-check mode",
    );
}

#[test]
fn session_explain_execution_primary_key_covering_key_range_is_planner_proven() {
    let query = crate::db::query::intent::Query::<SessionSqlEntity>::new(
        crate::db::predicate::MissingRowPolicy::Ignore,
    )
    .select_fields(["id"])
    .filter(Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::Gte,
            Value::Ulid(Ulid::from_u128(9_811)),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::Lt,
            Value::Ulid(Ulid::from_u128(9_813)),
            CoercionId::Strict,
        )),
    ]))
    .order_by("id")
    .limit(1);

    let descriptor = query
        .explain_execution()
        .expect("PK-only covering key-range explain_execution should succeed");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::PrimaryKeyRangeScan,
        "PK-only bounded primary-key projection should explain through the primary-key range scan node",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "PK-only primary-key range should expose the explicit covering route",
    );
    assert_eq!(
        descriptor.node_properties().get("cov_read_route"),
        Some(&Value::Text("covering_read".to_string())),
        "PK-only primary-key range should surface the covering-read route label",
    );
    let projection_node =
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::CoveringRead)
            .expect("PK-only covering key-range explain tree should emit a covering-read node");
    assert_eq!(
        projection_node.node_properties().get("covering_fields"),
        Some(&Value::List(vec![Value::Text("id".to_string())])),
        "PK-only key-range covering explain should expose the projected field list",
    );
    assert_eq!(
        projection_node.node_properties().get("covering_sources"),
        Some(&Value::List(vec![Value::Text("primary_key".to_string())])),
        "PK-only key-range covering explain should expose the primary-key field source",
    );
    assert_eq!(
        projection_node.node_properties().get("existing_row_mode"),
        Some(&Value::Text("planner_proven".to_string())),
        "PK-only key-range covering should surface the planner-proven row mode",
    );
}

#[test]
fn session_count_full_scan_ignores_other_entities_in_shared_store() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Seed two entity types into the same underlying store so the COUNT fast
    // path must stay scoped to the requested entity tag.
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);
    seed_session_explain_entities(&session, &[(9_501, 7, 10), (9_502, 7, 20)]);

    let expected = session
        .load::<SessionExplainEntity>()
        .execute()
        .expect("shared-store execute should succeed")
        .count();
    let actual = session
        .load::<SessionExplainEntity>()
        .count()
        .expect("shared-store count should succeed");

    assert_eq!(
        actual, expected,
        "full-scan count must ignore rows that belong to sibling entities sharing the same store",
    );
    assert_eq!(
        actual, 2,
        "shared-store count should report only the SessionExplainEntity rows",
    );
}

#[test]
fn session_explain_execution_projects_descriptor_tree_for_ordered_limited_index_access() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(
        &session,
        &[("Sam", 30), ("Sasha", 24), ("Soren", 18), ("Mira", 40)],
    );

    let descriptor = session
        .load::<IndexedSessionSqlEntity>()
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Eq,
            Value::Text("Sam".to_string()),
            CoercionId::Strict,
        )))
        .order_by("name")
        .order_by("id")
        .offset(1)
        .limit(2)
        .explain_execution()
        .expect("ordered limited indexed explain_execution should succeed");

    assert!(
        descriptor.access_strategy().is_some(),
        "execution descriptor root should carry one canonical access projection",
    );
    assert!(matches!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexPrefixScan | ExplainExecutionNodeType::IndexRangeScan
    ));
    assert_eq!(
        descriptor.covering_scan(),
        Some(false),
        "ordered scalar load execution roots should report explicit non-covering status",
    );

    let limit_node = descriptor
        .children()
        .iter()
        .find(|child| child.node_type() == ExplainExecutionNodeType::LimitOffset)
        .expect("paged shape should project one limit/offset node");
    assert_eq!(limit_node.limit(), Some(2));
    assert_eq!(
        limit_node.node_properties().get("offset"),
        Some(&Value::from(1u64)),
        "limit/offset node should keep logical offset metadata",
    );

    let order_node = descriptor
        .children()
        .iter()
        .find(|child| {
            child.node_type() == ExplainExecutionNodeType::OrderByAccessSatisfied
                || child.node_type() == ExplainExecutionNodeType::OrderByMaterializedSort
        })
        .expect("ordered shape should project one ORDER BY execution node");
    let _ = order_node;

    let text_tree = descriptor.render_text_tree();
    assert!(
        text_tree.contains(" execution_mode="),
        "base text rendering should include root access node label",
    );
    assert!(
        text_tree.contains(" access="),
        "base text rendering should include projected access summary",
    );
    assert!(
        text_tree.contains("LimitOffset execution_mode=") && text_tree.contains("limit=2"),
        "base text rendering should include limit node details",
    );

    let descriptor_json = descriptor.render_json_canonical();
    assert!(
        descriptor_json.contains("\"children\":["),
        "json rendering should include descriptor children array",
    );
    assert!(
        descriptor_json.contains("\"LimitOffset\""),
        "json rendering should include pipeline nodes from the descriptor tree",
    );
}

#[test]
fn session_explain_execution_hides_non_ready_secondary_indexes_from_planner_visibility() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(
        &session,
        &[("Sam", 30), ("Sasha", 24), ("Soren", 18), ("Mira", 40)],
    );

    // Phase 1: build one query shape that would normally plan through the
    // secondary `name` index if that index remained planner-visible.
    let query = session
        .load::<IndexedSessionSqlEntity>()
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Eq,
            Value::Text("Sam".to_string()),
            CoercionId::Strict,
        )))
        .order_by("name")
        .order_by("id")
        .limit(1);

    // Phase 2: flip the recovered store out of the ready/visible state after
    // query construction so the explain path must re-read planner visibility
    // instead of freezing the old secondary-index set on the builder.
    mark_indexed_session_sql_index_building();

    let descriptor = query
        .explain_execution()
        .expect("non-ready secondary index explain_execution should succeed");

    // Phase 3: require the planner-owned descriptor root to stay off all
    // secondary access nodes once the index is no longer visible.
    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::FullScan,
        "non-ready secondary indexes must disappear from planner visibility instead of downgrading in execution",
    );
    assert_ne!(
        descriptor.covering_scan(),
        Some(true),
        "non-ready secondary indexes must not leave behind a covering-read route",
    );

    let rows = query
        .execute()
        .expect("non-ready secondary index query should still execute");

    assert_eq!(
        rows.len(),
        1,
        "planner visibility fallback must preserve the bounded query window",
    );
    assert_eq!(
        rows[0].entity_ref().name,
        "Sam",
        "planner visibility fallback must preserve the filtered row identity",
    );
    assert_eq!(
        rows[0].entity_ref().age,
        30,
        "planner visibility fallback must preserve the projected entity payload",
    );
}

#[test]
fn session_planning_hides_non_ready_secondary_indexes_from_access_selection() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(
        &session,
        &[("Sam", 30), ("Sasha", 24), ("Soren", 18), ("Mira", 40)],
    );

    let query = Query::<IndexedSessionSqlEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Eq,
            Value::Text("Sam".to_string()),
            CoercionId::Strict,
        )))
        .order_by("name")
        .order_by("id")
        .limit(1);

    mark_indexed_session_sql_index_building();

    let visible_indexes = session
        .visible_indexes_for_store_model(
            IndexedSessionSqlStore::PATH,
            <IndexedSessionSqlEntity as crate::traits::EntitySchema>::MODEL,
        )
        .expect("non-ready store should still resolve planner-visible index slice");
    assert!(
        visible_indexes.as_slice().is_empty(),
        "planner boundary must hide non-ready secondary indexes before access selection",
    );

    let compiled = query
        .plan_with_visible_indexes(&visible_indexes)
        .expect("planning with no visible secondary indexes should still succeed");
    assert!(
        matches!(compiled.explain().access(), ExplainAccessPath::FullScan),
        "planner output must fall back to FullScan once the secondary index is no longer ready",
    );
}

#[test]
#[expect(clippy::too_many_lines)]
fn session_terminal_explain_seek_labels_for_min_and_max_are_stable() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_session_explain_entities(
        &session,
        &[
            (9_401, 7, 10),
            (9_402, 7, 20),
            (9_403, 7, 30),
            (9_404, 8, 99),
        ],
    );

    let min_terminal_plan = session
        .load::<SessionExplainEntity>()
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::from(7u64),
            CoercionId::Strict,
        )))
        .order_by("rank")
        .order_by("id")
        .explain_min()
        .expect("session explain_min should succeed");
    assert_eq!(min_terminal_plan.terminal(), AggregateKind::Min);
    assert!(matches!(
        min_terminal_plan.execution().ordering_source(),
        crate::db::ExplainExecutionOrderingSource::IndexSeekFirst { fetch: 1 }
    ));
    let min_execution = min_terminal_plan.execution();
    assert_eq!(min_execution.aggregation(), AggregateKind::Min);
    assert!(matches!(
        min_execution.ordering_source(),
        crate::db::ExplainExecutionOrderingSource::IndexSeekFirst { fetch: 1 }
    ));
    assert_eq!(
        min_execution.execution_mode(),
        crate::db::ExplainExecutionMode::Materialized
    );
    let min_node = min_terminal_plan.execution_node_descriptor();
    assert_eq!(
        min_node.node_type(),
        ExplainExecutionNodeType::AggregateSeekFirst
    );
    assert_eq!(min_node.execution_mode(), min_execution.execution_mode());
    assert!(
        min_node
            .render_text_tree()
            .contains("AggregateSeekFirst execution_mode=Materialized"),
        "seek-first explain text should expose the canonical seek-first label",
    );
    assert!(
        min_node
            .render_json_canonical()
            .contains("\"node_type\":\"AggregateSeekFirst\"")
            && min_node
                .render_json_canonical()
                .contains("\"fetch\":\"Uint(1)\""),
        "seek-first explain json should expose the canonical seek fetch contract",
    );

    let max_terminal_plan = session
        .load::<SessionExplainEntity>()
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::from(7u64),
            CoercionId::Strict,
        )))
        .order_by_desc("rank")
        .order_by_desc("id")
        .explain_max()
        .expect("session explain_max should succeed");
    assert_eq!(max_terminal_plan.terminal(), AggregateKind::Max);
    assert!(matches!(
        max_terminal_plan.execution().ordering_source(),
        crate::db::ExplainExecutionOrderingSource::IndexSeekLast { fetch: 1 }
    ));
    let max_execution = max_terminal_plan.execution();
    assert_eq!(max_execution.aggregation(), AggregateKind::Max);
    assert!(matches!(
        max_execution.ordering_source(),
        crate::db::ExplainExecutionOrderingSource::IndexSeekLast { fetch: 1 }
    ));
    assert_eq!(
        max_execution.execution_mode(),
        crate::db::ExplainExecutionMode::Materialized
    );
    let max_node = max_terminal_plan.execution_node_descriptor();
    assert_eq!(
        max_node.node_type(),
        ExplainExecutionNodeType::AggregateSeekLast
    );
    assert_eq!(max_node.execution_mode(), max_execution.execution_mode());
    assert!(
        max_node
            .render_text_tree()
            .contains("AggregateSeekLast execution_mode=Materialized"),
        "seek-last explain text should expose the canonical seek-last label",
    );
    assert!(
        max_node
            .render_json_canonical()
            .contains("\"node_type\":\"AggregateSeekLast\"")
            && max_node
                .render_json_canonical()
                .contains("\"fetch\":\"Uint(1)\""),
        "seek-last explain json should expose the canonical seek fetch contract",
    );
}

#[test]
fn session_explain_execution_text_and_json_surface_for_strict_index_prefix_shape() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_session_explain_entities(
        &session,
        &[
            (9_741, 7, 10),
            (9_742, 7, 20),
            (9_743, 7, 30),
            (9_744, 8, 40),
        ],
    );
    let query = session
        .load::<SessionExplainEntity>()
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::from(7u64),
            CoercionId::Strict,
        )))
        .order_by("rank")
        .order_by("id")
        .offset(1)
        .limit(2);

    let text_tree = query
        .explain_execution_text()
        .expect("strict index-prefix execution text explain should succeed");
    assert!(
        text_tree.contains("IndexPrefixScan execution_mode="),
        "execution text should expose the canonical index-prefix root label",
    );
    assert!(
        text_tree.contains("LimitOffset execution_mode=") && text_tree.contains("limit=2"),
        "execution text should expose the paged terminal node",
    );
    assert!(
        text_tree.contains("IndexPredicatePrefilter execution_mode=")
            || text_tree.contains("ResidualPredicateFilter execution_mode="),
        "execution text should expose one predicate-stage node",
    );

    let descriptor_json = query
        .explain_execution_json()
        .expect("strict index-prefix execution json explain should succeed");
    assert!(
        descriptor_json.contains("\"node_type\":\"IndexPrefixScan\""),
        "execution json should expose the canonical index-prefix root node type",
    );
    assert!(
        descriptor_json.contains("\"node_type\":\"LimitOffset\""),
        "execution json should expose the paged terminal node type",
    );
    assert!(
        descriptor_json.contains("\"node_type\":\"IndexPredicatePrefilter\"")
            || descriptor_json.contains("\"node_type\":\"ResidualPredicateFilter\""),
        "execution json should expose one predicate-stage node type",
    );
}

#[test]
fn session_aggregate_ranked_rows_are_invariant_to_insertion_order() {
    let rows_a = [
        (8_3961, 7, 10),
        (8_3962, 7, 40),
        (8_3963, 7, 20),
        (8_3964, 7, 30),
        (8_3965, 7, 40),
    ];
    let rows_b = [
        (8_3965, 7, 40),
        (8_3963, 7, 20),
        (8_3961, 7, 10),
        (8_3964, 7, 30),
        (8_3962, 7, 40),
    ];
    let ranked_ids_for = |rows: &[(u128, u64, u64)]| {
        reset_session_sql_store();
        let session = sql_session();
        seed_session_aggregate_entities(&session, rows);
        let top_ids = session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by("id")
            .top_k_by("rank", 3)
            .expect("session aggregate top_k_by(rank, 3) insertion-order test should succeed");
        let bottom_ids = session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by("id")
            .bottom_k_by("rank", 3)
            .expect("session aggregate bottom_k_by(rank, 3) insertion-order test should succeed");

        (
            session_aggregate_ids(&top_ids),
            session_aggregate_ids(&bottom_ids),
        )
    };

    assert_eq!(
        ranked_ids_for(&rows_a).0,
        ranked_ids_for(&rows_b).0,
        "session aggregate top_k_by(rank, 3) should be invariant to seed insertion order",
    );
    assert_eq!(
        ranked_ids_for(&rows_a).1,
        ranked_ids_for(&rows_b).1,
        "session aggregate bottom_k_by(rank, 3) should be invariant to seed insertion order",
    );
}

#[test]
#[expect(clippy::too_many_lines)]
fn session_aggregate_identity_terminals_match_execute() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_8501, 7, 10),
            (8_8502, 7, 20),
            (8_8503, 7, 30),
            (8_8504, 7, 40),
            (8_8505, 8, 99),
        ],
    );
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by("id")
            .offset(1)
            .limit(3)
    };
    let expected = load_window()
        .execute()
        .expect("session aggregate identity baseline execute should succeed");
    let expected_ids = session_aggregate_ids(&expected);

    assert_eq!(
        load_window()
            .count()
            .expect("session aggregate count should succeed"),
        expected.count(),
        "session aggregate count should match execute() cardinality",
    );
    assert_eq!(
        load_window()
            .exists()
            .expect("session aggregate exists should succeed"),
        !expected.is_empty(),
        "session aggregate exists should match execute() emptiness",
    );
    assert_eq!(
        load_window()
            .not_exists()
            .expect("session aggregate not_exists should succeed"),
        expected.is_empty(),
        "session aggregate not_exists should match execute() emptiness",
    );
    assert_eq!(
        load_window()
            .is_empty()
            .expect("session aggregate is_empty should succeed"),
        expected.is_empty(),
        "session aggregate is_empty should match execute() emptiness",
    );
    assert_eq!(
        load_window()
            .min()
            .expect("session aggregate min should succeed")
            .map(|id| id.key()),
        expected_ids.iter().copied().min(),
        "session aggregate min should match execute() minimum id",
    );
    assert_eq!(
        load_window()
            .max()
            .expect("session aggregate max should succeed")
            .map(|id| id.key()),
        expected_ids.iter().copied().max(),
        "session aggregate max should match execute() maximum id",
    );
    assert_eq!(
        load_window()
            .min_by("id")
            .expect("session aggregate min_by(id) should succeed")
            .map(|id| id.key()),
        expected_ids.iter().copied().min(),
        "session aggregate min_by(id) should match execute() minimum id",
    );
    assert_eq!(
        load_window()
            .max_by("id")
            .expect("session aggregate max_by(id) should succeed")
            .map(|id| id.key()),
        expected_ids.iter().copied().max(),
        "session aggregate max_by(id) should match execute() maximum id",
    );
    assert_eq!(
        load_window()
            .nth_by("id", 1)
            .expect("session aggregate nth_by(id, 1) should succeed")
            .map(|id| id.key()),
        expected_ids.get(1).copied(),
        "session aggregate nth_by(id, 1) should match ordered execute() ids",
    );
    assert_eq!(
        load_window()
            .first()
            .expect("session aggregate first should succeed")
            .map(|id| id.key()),
        expected.id().map(|id| id.key()),
        "session aggregate first should match execute() head id",
    );
    assert_eq!(
        load_window()
            .last()
            .expect("session aggregate last should succeed")
            .map(|id| id.key()),
        expected_ids.last().copied(),
        "session aggregate last should match execute() tail id",
    );
}

#[test]
fn session_aggregate_exists_not_exists_and_is_empty_share_early_stop_scan_budget() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_401, 7, 10),
            (8_402, 7, 20),
            (8_403, 7, 30),
            (8_404, 7, 40),
            (8_405, 7, 50),
            (8_406, 7, 60),
        ],
    );
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .order_by("id")
            .offset(2)
    };

    // Keep the three existence aliases on one live fixture so the shared
    // early-stop scan budget stays explicit at the session owner boundary.
    let (actual_exists, exists_rows_scanned) =
        capture_rows_scanned_for_entity(SessionAggregateEntity::PATH, || load_window().exists());
    let (actual_not_exists, not_exists_rows_scanned) =
        capture_rows_scanned_for_entity(SessionAggregateEntity::PATH, || {
            load_window().not_exists()
        });
    let (actual_is_empty, is_empty_rows_scanned) =
        capture_rows_scanned_for_entity(SessionAggregateEntity::PATH, || load_window().is_empty());

    assert!(
        actual_exists.expect("session aggregate exists should succeed"),
        "window should report at least one matching row",
    );
    assert!(
        !actual_not_exists.expect("session aggregate not_exists should succeed"),
        "not_exists should be false when one matching row is present",
    );
    assert!(
        !actual_is_empty.expect("session aggregate is_empty should succeed"),
        "is_empty should be false when one matching row is present",
    );
    assert_eq!(
        exists_rows_scanned, 3,
        "exists should stop after offset + 1 rows on a non-empty ordered window",
    );
    assert_eq!(
        not_exists_rows_scanned, exists_rows_scanned,
        "not_exists should preserve exists scan-budget behavior",
    );
    assert_eq!(
        is_empty_rows_scanned, exists_rows_scanned,
        "is_empty should preserve exists scan-budget behavior",
    );
}

#[test]
fn session_aggregate_primary_key_is_null_lowers_to_empty_access_without_scan() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(&session, &[(8_411, 7, 10), (8_412, 7, 20), (8_413, 8, 30)]);
    let null_pk_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(Predicate::IsNull {
                field: "id".to_string(),
            })
    };

    // This impossible primary-key shape should lower to empty access before any
    // session aggregate terminal consumes scan budget.
    let (actual_count, count_rows_scanned) =
        capture_rows_scanned_for_entity(SessionAggregateEntity::PATH, || null_pk_window().count());
    let (actual_exists, exists_rows_scanned) =
        capture_rows_scanned_for_entity(SessionAggregateEntity::PATH, || null_pk_window().exists());
    let (actual_not_exists, not_exists_rows_scanned) =
        capture_rows_scanned_for_entity(SessionAggregateEntity::PATH, || {
            null_pk_window().not_exists()
        });

    assert_eq!(
        actual_count.expect("count should succeed for primary_key IS NULL"),
        0,
        "primary_key IS NULL should match no rows",
    );
    assert!(
        !actual_exists.expect("exists should succeed for primary_key IS NULL"),
        "exists should be false for primary_key IS NULL windows",
    );
    assert!(
        actual_not_exists.expect("not_exists should succeed for primary_key IS NULL"),
        "not_exists should be true for primary_key IS NULL windows",
    );
    assert_eq!(
        count_rows_scanned, 0,
        "count should not scan rows when planner lowers primary_key IS NULL to empty access",
    );
    assert_eq!(
        exists_rows_scanned, 0,
        "exists should not scan rows when planner lowers primary_key IS NULL to empty access",
    );
    assert_eq!(
        not_exists_rows_scanned, 0,
        "not_exists should not scan rows when planner lowers primary_key IS NULL to empty access",
    );
}

#[test]
fn session_aggregate_primary_key_is_null_or_id_eq_matches_id_eq_branch_parity() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_421, 7, 10),
            (8_422, 7, 20),
            (8_423, 7, 30),
            (8_424, 8, 99),
        ],
    );
    let target = Value::Ulid(Ulid::from_u128(8_423));
    let eq_id_predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "id",
        CompareOp::Eq,
        target,
        CoercionId::Strict,
    ));
    let or_predicate = Predicate::Or(vec![
        Predicate::IsNull {
            field: "id".to_string(),
        },
        eq_id_predicate.clone(),
    ]);
    let strict_eq_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(eq_id_predicate.clone())
            .order_by("id")
    };
    let null_or_eq_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(or_predicate.clone())
            .order_by("id")
    };

    let expected = strict_eq_window()
        .execute()
        .expect("strict id equality execute should succeed");
    let actual = null_or_eq_window()
        .execute()
        .expect("null-or-id execute should succeed");
    assert_eq!(
        actual.ids().collect::<Vec<_>>(),
        expected.ids().collect::<Vec<_>>(),
        "null-or-id result set should match the strict id-equality branch",
    );

    let expected_count = strict_eq_window().count().expect("count should succeed");
    let actual_count = null_or_eq_window().count().expect("count should succeed");
    assert_eq!(
        actual_count, expected_count,
        "null-or-id count should match strict id-equality count",
    );
    let (expected_exists, expected_exists_rows_scanned) =
        capture_rows_scanned_for_entity(SessionAggregateEntity::PATH, || {
            strict_eq_window().exists()
        });
    let (actual_exists, actual_exists_rows_scanned) =
        capture_rows_scanned_for_entity(SessionAggregateEntity::PATH, || {
            null_or_eq_window().exists()
        });
    assert_eq!(
        actual_exists.expect("exists should succeed"),
        expected_exists.expect("exists should succeed"),
        "null-or-id exists should match strict id-equality exists",
    );
    assert_eq!(
        actual_exists_rows_scanned, expected_exists_rows_scanned,
        "null-or-id exists should preserve strict id-equality scan budget",
    );
}

#[test]
fn session_aggregate_min_by_unknown_field_fails_before_scan_budget_consumption() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_901, 7, 10),
            (8_902, 7, 20),
            (8_903, 7, 30),
            (8_904, 8, 99),
        ],
    );
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by_desc("id")
            .offset(0)
            .limit(3)
    };

    let (result, scanned_rows) =
        capture_rows_scanned_for_entity(SessionAggregateEntity::PATH, || {
            load_window().min_by("missing_field")
        });
    let Err(err) = result else {
        panic!("session min_by(missing_field) should be rejected");
    };

    assert!(
        matches!(err, QueryError::Execute(_)),
        "session unknown-field min_by should remain an execute-domain error: {err:?}",
    );
    assert_eq!(
        scanned_rows, 0,
        "session unknown-field min_by should fail before scan-budget consumption",
    );
    assert!(
        err.to_string().contains("unknown aggregate target field"),
        "session unknown-field min_by should preserve explicit field taxonomy: {err:?}",
    );
}

#[test]
fn session_aggregate_new_field_aggregates_match_execute_projection() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_311, 7, 10),
            (8_312, 7, 10),
            (8_313, 7, 20),
            (8_314, 7, 30),
            (8_315, 7, 40),
            (8_316, 8, 99),
        ],
    );
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by_desc("id")
            .offset(1)
            .limit(4)
    };
    let expected = load_window()
        .execute()
        .expect("session aggregate new-field baseline execute should succeed");

    assert_eq!(
        load_window()
            .median_by("rank")
            .expect("session aggregate median_by(rank) should succeed")
            .map(|id| id.key()),
        session_aggregate_expected_median_by_rank_id(&expected),
        "session aggregate median_by(rank) should match deterministic execute() parity",
    );
    assert_eq!(
        load_window()
            .count_distinct_by("rank")
            .expect("session aggregate count_distinct_by(rank) should succeed"),
        session_aggregate_expected_count_distinct_by_rank(&expected),
        "session aggregate count_distinct_by(rank) should match execute() distinct projection cardinality",
    );
    assert_eq!(
        load_window()
            .min_max_by("rank")
            .expect("session aggregate min_max_by(rank) should succeed")
            .map(|(min_id, max_id)| (min_id.key(), max_id.key())),
        session_aggregate_expected_min_max_by_rank_ids(&expected),
        "session aggregate min_max_by(rank) should match execute() min/max parity",
    );
}

#[test]
fn session_aggregate_numeric_field_aggregates_match_execute_projection() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_121, 7, 10),
            (8_122, 7, 20),
            (8_123, 7, 35),
            (8_124, 8, 99),
        ],
    );

    let expected_response = session
        .load::<SessionAggregateEntity>()
        .filter(session_aggregate_group_predicate(7))
        .order_by("rank")
        .execute()
        .expect("session aggregate numeric baseline execute should succeed");

    // Recompute the numeric terminals from the materialized window so the
    // session-facing aggregate contract stays aligned with execute() parity.
    let mut expected_sum = crate::types::Decimal::ZERO;
    let mut expected_count = 0u64;
    for row in expected_response {
        let rank =
            crate::types::Decimal::from_num(row.entity().rank).expect("rank decimal should build");
        expected_sum += rank;
        expected_count = expected_count.saturating_add(1);
    }
    let expected_sum_decimal = expected_sum;
    let expected_sum = Some(expected_sum_decimal);
    let expected_avg = if expected_count == 0 {
        None
    } else {
        Some(
            expected_sum_decimal
                / crate::types::Decimal::from_num(expected_count)
                    .expect("count decimal should build"),
        )
    };

    assert_eq!(
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by("rank")
            .sum_by("rank")
            .expect("session aggregate sum_by(rank) should succeed"),
        expected_sum,
        "session aggregate sum_by(rank) should match execute() projection parity",
    );
    assert_eq!(
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by("rank")
            .avg_by("rank")
            .expect("session aggregate avg_by(rank) should succeed"),
        expected_avg,
        "session aggregate avg_by(rank) should match execute() projection parity",
    );
}

#[test]
fn session_aggregate_numeric_field_prepared_strategy_explain_projects_sum_shape() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_2121, 7, 10),
            (8_2122, 7, 20),
            (8_2123, 7, 35),
            (8_2124, 8, 99),
        ],
    );
    let query = session
        .load::<SessionAggregateEntity>()
        .filter(session_aggregate_group_predicate(7))
        .order_by("rank");
    let rank_slot = FieldSlot::resolve(SessionAggregateEntity::MODEL, "rank")
        .expect("rank field slot should resolve");
    let explain = session
        .explain_query_prepared_aggregate_terminal_with_visible_indexes(
            query.query(),
            &PreparedFluentNumericFieldStrategy::sum_by_slot(rank_slot),
        )
        .expect("prepared numeric explain should build");
    let node = explain.execution_node_descriptor();

    assert_eq!(
        explain.terminal(),
        AggregateKind::Sum,
        "prepared numeric explain should preserve the SUM terminal kind",
    );
    assert_eq!(
        node.node_type(),
        ExplainExecutionNodeType::AggregateSum,
        "prepared numeric explain should project the SUM execution node type",
    );
    assert_eq!(
        node.node_properties().get("proj_field"),
        Some(&Value::from("rank")),
        "prepared numeric explain should preserve the projected numeric field label",
    );
}

#[test]
fn session_aggregate_numeric_field_explain_uses_prepared_strategy_projection() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_2221, 7, 10),
            (8_2222, 7, 20),
            (8_2223, 7, 20),
            (8_2224, 8, 99),
        ],
    );
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by("rank")
    };
    let rank_slot = FieldSlot::resolve(SessionAggregateEntity::MODEL, "rank")
        .expect("rank field slot should resolve");
    let prepared_sum = session
        .explain_query_prepared_aggregate_terminal_with_visible_indexes(
            load_window().query(),
            &PreparedFluentNumericFieldStrategy::sum_by_slot(rank_slot.clone()),
        )
        .expect("prepared numeric SUM explain should build");
    let prepared_avg_distinct = session
        .explain_query_prepared_aggregate_terminal_with_visible_indexes(
            load_window().query(),
            &PreparedFluentNumericFieldStrategy::avg_distinct_by_slot(rank_slot),
        )
        .expect("prepared numeric AVG DISTINCT explain should build");
    let public_sum = load_window()
        .explain_sum_by("rank")
        .expect("public fluent SUM explain should build");
    let public_avg_distinct = load_window()
        .explain_avg_distinct_by("rank")
        .expect("public fluent AVG DISTINCT explain should build");

    assert_eq!(
        session_aggregate_terminal_plan_snapshot(&public_sum),
        session_aggregate_terminal_plan_snapshot(&prepared_sum),
        "public fluent SUM explain should project from the same prepared numeric strategy",
    );
    assert_eq!(
        session_aggregate_terminal_plan_snapshot(&public_avg_distinct),
        session_aggregate_terminal_plan_snapshot(&prepared_avg_distinct),
        "public fluent AVG DISTINCT explain should project from the same prepared numeric strategy",
    );
}

#[test]
fn session_aggregate_projection_terminal_explain_uses_prepared_strategy_projection() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_2321, 7, 10),
            (8_2322, 7, 20),
            (8_2323, 7, 20),
            (8_2324, 8, 99),
        ],
    );
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by("rank")
    };
    let rank_slot = FieldSlot::resolve(SessionAggregateEntity::MODEL, "rank")
        .expect("rank field slot should resolve");
    let prepared_count_distinct = session
        .explain_query_prepared_projection_terminal_with_visible_indexes(
            load_window().query(),
            &PreparedFluentProjectionStrategy::count_distinct_by_slot(rank_slot.clone()),
        )
        .expect("prepared projection COUNT DISTINCT explain should build");
    let prepared_last_value = session
        .explain_query_prepared_projection_terminal_with_visible_indexes(
            load_window().query(),
            &PreparedFluentProjectionStrategy::last_value_by_slot(rank_slot),
        )
        .expect("prepared projection terminal-value explain should build");
    let public_count_distinct = load_window()
        .explain_count_distinct_by("rank")
        .expect("public fluent COUNT DISTINCT explain should build");
    let public_last_value = load_window()
        .explain_last_value_by("rank")
        .expect("public fluent last_value_by explain should build");

    assert_eq!(
        public_count_distinct.render_json_canonical(),
        prepared_count_distinct.render_json_canonical(),
        "public fluent COUNT DISTINCT explain should project from the prepared projection strategy",
    );
    assert_eq!(
        public_last_value.render_json_canonical(),
        prepared_last_value.render_json_canonical(),
        "public fluent last_value_by explain should project from the prepared projection strategy",
    );
}

#[test]
fn session_aggregate_nth_by_rank_uses_deterministic_rank_and_id_ordering() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_4041, 7, 10),
            (8_4042, 7, 10),
            (8_4043, 7, 20),
            (8_4044, 7, 30),
            (8_4045, 8, 99),
        ],
    );
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_by_desc("id")
            .limit(4)
    };
    let expected = load_window()
        .execute()
        .expect("session aggregate nth_by baseline execute should succeed");

    assert_eq!(
        load_window()
            .nth_by("rank", 0)
            .expect("session aggregate nth_by(rank, 0) should succeed")
            .map(|id| id.key()),
        session_aggregate_expected_nth_by_rank_id(&expected, 0),
        "session aggregate nth_by(rank, 0) should use rank-asc then id-asc ordering",
    );
    assert_eq!(
        load_window()
            .nth_by("rank", 1)
            .expect("session aggregate nth_by(rank, 1) should succeed")
            .map(|id| id.key()),
        session_aggregate_expected_nth_by_rank_id(&expected, 1),
        "session aggregate nth_by(rank, 1) should advance through equal-rank ties deterministically",
    );
    assert_eq!(
        load_window()
            .nth_by("rank", 4)
            .expect("session aggregate nth_by(rank, 4) should succeed")
            .map(|id| id.key()),
        None,
        "session aggregate nth_by(rank, ordinal) should return None beyond the effective window",
    );
}

#[test]
fn session_temporal_entities_and_projection_values_preserve_semantic_types() {
    reset_session_sql_store();
    let session = sql_session();
    let day_one = Date::new_checked(2025, 10, 19).expect("date should build");
    let day_two = Date::new_checked(2025, 10, 20).expect("date should build");
    let at_one = Timestamp::from_millis(1_760_868_000_000);
    let at_two = Timestamp::from_millis(1_760_954_400_000);
    let elapsed_one = Duration::from_millis(1_500);
    let elapsed_two = Duration::from_millis(2_750);
    seed_session_temporal_entities(
        &session,
        &[
            (8_941, day_one, at_one, elapsed_one),
            (8_942, day_two, at_two, elapsed_two),
        ],
    );

    // Phase 1: lock semantic entity-field projection types and values.
    let response = session
        .load::<SessionTemporalEntity>()
        .order_by("id")
        .execute()
        .expect("temporal execute should succeed");
    let entities = response.entities();
    assert_eq!(entities.len(), 2, "temporal fixture should return two rows");
    let first = &entities[0];
    let second = &entities[1];
    let _: Date = first.occurred_on;
    let _: Timestamp = first.occurred_at;
    let _: Duration = first.elapsed;
    assert_eq!(first.occurred_on, day_one);
    assert_eq!(second.occurred_on, day_two);
    assert_eq!(first.occurred_at, at_one);
    assert_eq!(second.occurred_at, at_two);
    assert_eq!(first.elapsed, elapsed_one);
    assert_eq!(second.elapsed, elapsed_two);

    // Phase 2: lock scalar projection value typing for temporal fields.
    let day_values = session
        .load::<SessionTemporalEntity>()
        .order_by("id")
        .values_by("occurred_on")
        .expect("occurred_on projection should succeed");
    let at_values = session
        .load::<SessionTemporalEntity>()
        .order_by("id")
        .values_by("occurred_at")
        .expect("occurred_at projection should succeed");
    let elapsed_values = session
        .load::<SessionTemporalEntity>()
        .order_by("id")
        .values_by("elapsed")
        .expect("elapsed projection should succeed");
    assert_eq!(day_values, vec![Value::Date(day_one), Value::Date(day_two)]);
    assert_eq!(
        at_values,
        vec![Value::Timestamp(at_one), Value::Timestamp(at_two)]
    );
    assert_eq!(
        elapsed_values,
        vec![Value::Duration(elapsed_one), Value::Duration(elapsed_two)]
    );
}

#[test]
fn session_temporal_grouped_keys_preserve_semantic_types() {
    reset_session_sql_store();
    let session = sql_session();
    let day_one = Date::new_checked(2025, 10, 19).expect("date should build");
    let day_two = Date::new_checked(2025, 10, 20).expect("date should build");
    let at_one = Timestamp::from_millis(1_760_868_000_000);
    let at_two = Timestamp::from_millis(1_760_954_400_000);
    let elapsed_one = Duration::from_millis(1_500);
    let elapsed_two = Duration::from_millis(2_750);
    seed_session_temporal_entities(
        &session,
        &[
            (8_943, day_one, at_one, elapsed_one),
            (8_944, day_one, at_two, elapsed_one),
            (8_945, day_two, at_two, elapsed_two),
        ],
    );

    // Phase 1: group by Date and lock semantic key typing in grouped output.
    let by_day = session
        .load::<SessionTemporalEntity>()
        .group_by("occurred_on")
        .expect("group_by(occurred_on) should resolve")
        .aggregate(crate::db::count())
        .execute_grouped()
        .expect("grouped by occurred_on should execute");
    assert_eq!(
        by_day
            .rows()
            .iter()
            .map(crate::db::GroupedRow::group_key)
            .collect::<Vec<_>>(),
        vec![&[Value::Date(day_one)][..], &[Value::Date(day_two)][..]],
        "grouped Date keys should stay semantic Date values",
    );
    assert_eq!(
        by_day
            .rows()
            .iter()
            .map(crate::db::GroupedRow::aggregate_values)
            .collect::<Vec<_>>(),
        vec![&[Value::Uint(2)][..], &[Value::Uint(1)][..]],
        "grouped Date counts should match fixture cardinality",
    );

    // Phase 2: group by Timestamp and lock semantic key typing in grouped output.
    let by_timestamp = session
        .load::<SessionTemporalEntity>()
        .group_by("occurred_at")
        .expect("group_by(occurred_at) should resolve")
        .aggregate(crate::db::count())
        .execute_grouped()
        .expect("grouped by occurred_at should execute");
    assert_eq!(
        by_timestamp
            .rows()
            .iter()
            .map(crate::db::GroupedRow::group_key)
            .collect::<Vec<_>>(),
        vec![
            &[Value::Timestamp(at_one)][..],
            &[Value::Timestamp(at_two)][..]
        ],
        "grouped Timestamp keys should stay semantic Timestamp values",
    );
    assert_eq!(
        by_timestamp
            .rows()
            .iter()
            .map(crate::db::GroupedRow::aggregate_values)
            .collect::<Vec<_>>(),
        vec![&[Value::Uint(1)][..], &[Value::Uint(2)][..]],
        "grouped Timestamp counts should match fixture cardinality",
    );

    // Phase 3: group by Duration and lock semantic key typing in grouped output.
    let by_duration = session
        .load::<SessionTemporalEntity>()
        .group_by("elapsed")
        .expect("group_by(elapsed) should resolve")
        .aggregate(crate::db::count())
        .execute_grouped()
        .expect("grouped by elapsed should execute");
    assert_eq!(
        by_duration
            .rows()
            .iter()
            .map(crate::db::GroupedRow::group_key)
            .collect::<Vec<_>>(),
        vec![
            &[Value::Duration(elapsed_one)][..],
            &[Value::Duration(elapsed_two)][..]
        ],
        "grouped Duration keys should stay semantic Duration values",
    );
    assert_eq!(
        by_duration
            .rows()
            .iter()
            .map(crate::db::GroupedRow::aggregate_values)
            .collect::<Vec<_>>(),
        vec![&[Value::Uint(2)][..], &[Value::Uint(1)][..]],
        "grouped Duration counts should match fixture cardinality",
    );
}

#[test]
fn session_temporal_distinct_projection_values_preserve_semantic_types() {
    reset_session_sql_store();
    let session = sql_session();
    let day_one = Date::new_checked(2025, 10, 19).expect("date should build");
    let day_two = Date::new_checked(2025, 10, 20).expect("date should build");
    let at_one = Timestamp::from_millis(1_760_868_000_000);
    let at_two = Timestamp::from_millis(1_760_954_400_000);
    let elapsed_one = Duration::from_millis(1_500);
    let elapsed_two = Duration::from_millis(2_750);
    seed_session_temporal_entities(
        &session,
        &[
            (8_946, day_one, at_one, elapsed_one),
            (8_947, day_one, at_two, elapsed_one),
            (8_948, day_two, at_two, elapsed_two),
        ],
    );

    // Phase 1: lock Date/Timestamp/Duration distinct projection typing and
    // first-observed value ordering under one deterministic window.
    let distinct_days = session
        .load::<SessionTemporalEntity>()
        .order_by("id")
        .distinct_values_by("occurred_on")
        .expect("distinct_values_by(occurred_on) should succeed");
    let distinct_timestamps = session
        .load::<SessionTemporalEntity>()
        .order_by("id")
        .distinct_values_by("occurred_at")
        .expect("distinct_values_by(occurred_at) should succeed");
    let distinct_durations = session
        .load::<SessionTemporalEntity>()
        .order_by("id")
        .distinct_values_by("elapsed")
        .expect("distinct_values_by(elapsed) should succeed");

    // Phase 2: assert semantic temporal value variants are preserved across
    // distinct projection boundaries.
    assert_eq!(
        distinct_days,
        vec![Value::Date(day_one), Value::Date(day_two)],
        "distinct Date projections should stay semantic Date values",
    );
    assert_eq!(
        distinct_timestamps,
        vec![Value::Timestamp(at_one), Value::Timestamp(at_two)],
        "distinct Timestamp projections should stay semantic Timestamp values",
    );
    assert_eq!(
        distinct_durations,
        vec![Value::Duration(elapsed_one), Value::Duration(elapsed_two)],
        "distinct Duration projections should stay semantic Duration values",
    );
}

#[test]
fn session_temporal_first_last_projection_values_preserve_semantic_types() {
    reset_session_sql_store();
    let session = sql_session();
    let day_one = Date::new_checked(2025, 10, 19).expect("date should build");
    let day_two = Date::new_checked(2025, 10, 20).expect("date should build");
    let at_one = Timestamp::from_millis(1_760_868_000_000);
    let at_two = Timestamp::from_millis(1_760_954_400_000);
    let elapsed_one = Duration::from_millis(1_500);
    let elapsed_two = Duration::from_millis(2_750);
    seed_session_temporal_entities(
        &session,
        &[
            (8_949, day_one, at_one, elapsed_one),
            (8_950, day_two, at_two, elapsed_two),
        ],
    );
    let load_window = || session.load::<SessionTemporalEntity>().order_by("id");

    // Phase 1: lock first-value temporal projection typing for scalar terminals.
    let first_day = load_window()
        .first_value_by("occurred_on")
        .expect("first_value_by(occurred_on) should succeed");
    let first_timestamp = load_window()
        .first_value_by("occurred_at")
        .expect("first_value_by(occurred_at) should succeed");
    let first_duration = load_window()
        .first_value_by("elapsed")
        .expect("first_value_by(elapsed) should succeed");

    // Phase 2: lock last-value temporal projection typing for scalar terminals.
    let last_day = load_window()
        .last_value_by("occurred_on")
        .expect("last_value_by(occurred_on) should succeed");
    let last_timestamp = load_window()
        .last_value_by("occurred_at")
        .expect("last_value_by(occurred_at) should succeed");
    let last_duration = load_window()
        .last_value_by("elapsed")
        .expect("last_value_by(elapsed) should succeed");

    assert_eq!(first_day, Some(Value::Date(day_one)));
    assert_eq!(first_timestamp, Some(Value::Timestamp(at_one)));
    assert_eq!(first_duration, Some(Value::Duration(elapsed_one)));
    assert_eq!(last_day, Some(Value::Date(day_two)));
    assert_eq!(last_timestamp, Some(Value::Timestamp(at_two)));
    assert_eq!(last_duration, Some(Value::Duration(elapsed_two)));
}

#[test]
fn session_temporal_values_with_ids_preserve_semantic_types() {
    reset_session_sql_store();
    let session = sql_session();
    let day_one = Date::new_checked(2025, 10, 19).expect("date should build");
    let day_two = Date::new_checked(2025, 10, 20).expect("date should build");
    let at_one = Timestamp::from_millis(1_760_868_000_000);
    let at_two = Timestamp::from_millis(1_760_954_400_000);
    let elapsed_one = Duration::from_millis(1_500);
    let elapsed_two = Duration::from_millis(2_750);
    let id_one = Id::<SessionTemporalEntity>::from_key(Ulid::from_u128(8_951));
    let id_two = Id::<SessionTemporalEntity>::from_key(Ulid::from_u128(8_952));
    seed_session_temporal_entities(
        &session,
        &[
            (8_951, day_one, at_one, elapsed_one),
            (8_952, day_two, at_two, elapsed_two),
        ],
    );
    let load_window = || session.load::<SessionTemporalEntity>().order_by("id");

    // Phase 1: lock temporal typing for id/value projection pairs.
    let day_pairs = load_window()
        .values_by_with_ids("occurred_on")
        .expect("values_by_with_ids(occurred_on) should succeed");
    let timestamp_pairs = load_window()
        .values_by_with_ids("occurred_at")
        .expect("values_by_with_ids(occurred_at) should succeed");
    let duration_pairs = load_window()
        .values_by_with_ids("elapsed")
        .expect("values_by_with_ids(elapsed) should succeed");

    // Phase 2: assert semantic temporal variants are preserved alongside ids.
    assert_eq!(
        day_pairs,
        vec![
            (id_one, Value::Date(day_one)),
            (id_two, Value::Date(day_two))
        ]
    );
    assert_eq!(
        timestamp_pairs,
        vec![
            (id_one, Value::Timestamp(at_one)),
            (id_two, Value::Timestamp(at_two))
        ]
    );
    assert_eq!(
        duration_pairs,
        vec![
            (id_one, Value::Duration(elapsed_one)),
            (id_two, Value::Duration(elapsed_two))
        ]
    );
}

#[test]
#[expect(clippy::too_many_lines)]
fn session_temporal_ranked_projection_values_preserve_semantic_types() {
    reset_session_sql_store();
    let session = sql_session();
    let day_one = Date::new_checked(2025, 10, 19).expect("date should build");
    let day_two = Date::new_checked(2025, 10, 20).expect("date should build");
    let day_three = Date::new_checked(2025, 10, 21).expect("date should build");
    let at_one = Timestamp::from_millis(1_760_868_000_000);
    let at_two = Timestamp::from_millis(1_760_954_400_000);
    let at_three = Timestamp::from_millis(1_761_040_800_000);
    let elapsed_one = Duration::from_millis(1_500);
    let elapsed_two = Duration::from_millis(2_750);
    let elapsed_three = Duration::from_millis(4_100);
    let id_one = Id::<SessionTemporalEntity>::from_key(Ulid::from_u128(8_953));
    let id_two = Id::<SessionTemporalEntity>::from_key(Ulid::from_u128(8_954));
    let id_three = Id::<SessionTemporalEntity>::from_key(Ulid::from_u128(8_955));
    seed_session_temporal_entities(
        &session,
        &[
            (8_953, day_one, at_one, elapsed_one),
            (8_954, day_two, at_two, elapsed_two),
            (8_955, day_three, at_three, elapsed_three),
        ],
    );
    let load_window = || session.load::<SessionTemporalEntity>();

    // Phase 1: lock temporal value typing for ranked value projections.
    let top_days = load_window()
        .top_k_by_values("occurred_on", 2)
        .expect("top_k_by_values(occurred_on) should succeed");
    let bottom_days = load_window()
        .bottom_k_by_values("occurred_on", 2)
        .expect("bottom_k_by_values(occurred_on) should succeed");
    let top_timestamps = load_window()
        .top_k_by_values("occurred_at", 2)
        .expect("top_k_by_values(occurred_at) should succeed");
    let bottom_timestamps = load_window()
        .bottom_k_by_values("occurred_at", 2)
        .expect("bottom_k_by_values(occurred_at) should succeed");
    let top_durations = load_window()
        .top_k_by_values("elapsed", 2)
        .expect("top_k_by_values(elapsed) should succeed");
    let bottom_durations = load_window()
        .bottom_k_by_values("elapsed", 2)
        .expect("bottom_k_by_values(elapsed) should succeed");

    assert_eq!(top_days, vec![Value::Date(day_three), Value::Date(day_two)]);
    assert_eq!(
        bottom_days,
        vec![Value::Date(day_one), Value::Date(day_two)]
    );
    assert_eq!(
        top_timestamps,
        vec![Value::Timestamp(at_three), Value::Timestamp(at_two)]
    );
    assert_eq!(
        bottom_timestamps,
        vec![Value::Timestamp(at_one), Value::Timestamp(at_two)]
    );
    assert_eq!(
        top_durations,
        vec![Value::Duration(elapsed_three), Value::Duration(elapsed_two)]
    );
    assert_eq!(
        bottom_durations,
        vec![Value::Duration(elapsed_one), Value::Duration(elapsed_two)]
    );

    // Phase 2: lock temporal value typing for ranked id/value projections.
    let top_day_pairs = load_window()
        .top_k_by_with_ids("occurred_on", 2)
        .expect("top_k_by_with_ids(occurred_on) should succeed");
    let bottom_day_pairs = load_window()
        .bottom_k_by_with_ids("occurred_on", 2)
        .expect("bottom_k_by_with_ids(occurred_on) should succeed");
    let top_timestamp_pairs = load_window()
        .top_k_by_with_ids("occurred_at", 2)
        .expect("top_k_by_with_ids(occurred_at) should succeed");
    let bottom_duration_pairs = load_window()
        .bottom_k_by_with_ids("elapsed", 2)
        .expect("bottom_k_by_with_ids(elapsed) should succeed");

    assert_eq!(
        top_day_pairs,
        vec![
            (id_three, Value::Date(day_three)),
            (id_two, Value::Date(day_two))
        ]
    );
    assert_eq!(
        bottom_day_pairs,
        vec![
            (id_one, Value::Date(day_one)),
            (id_two, Value::Date(day_two))
        ]
    );
    assert_eq!(
        top_timestamp_pairs,
        vec![
            (id_three, Value::Timestamp(at_three)),
            (id_two, Value::Timestamp(at_two))
        ]
    );
    assert_eq!(
        bottom_duration_pairs,
        vec![
            (id_one, Value::Duration(elapsed_one)),
            (id_two, Value::Duration(elapsed_two))
        ]
    );
}

#[test]
fn session_temporal_ranked_row_terminals_preserve_semantic_types() {
    reset_session_sql_store();
    let session = sql_session();
    let day_one = Date::new_checked(2025, 10, 19).expect("date should build");
    let day_two = Date::new_checked(2025, 10, 20).expect("date should build");
    let day_three = Date::new_checked(2025, 10, 21).expect("date should build");
    let at_one = Timestamp::from_millis(1_760_868_000_000);
    let at_two = Timestamp::from_millis(1_760_954_400_000);
    let at_three = Timestamp::from_millis(1_761_040_800_000);
    let elapsed_one = Duration::from_millis(1_500);
    let elapsed_two = Duration::from_millis(2_750);
    let elapsed_three = Duration::from_millis(4_100);
    seed_session_temporal_entities(
        &session,
        &[
            (8_956, day_one, at_one, elapsed_one),
            (8_957, day_two, at_two, elapsed_two),
            (8_958, day_three, at_three, elapsed_three),
        ],
    );
    let load_window = || session.load::<SessionTemporalEntity>();

    // Phase 1: lock top-k row terminal typing and ordering for temporal ranking.
    let top_response = load_window()
        .top_k_by("occurred_on", 2)
        .expect("top_k_by(occurred_on, 2) should succeed");
    let top_entities = top_response.entities();
    assert_eq!(top_entities.len(), 2, "top_k_by should return two rows");
    let _: Date = top_entities[0].occurred_on;
    let _: Timestamp = top_entities[0].occurred_at;
    let _: Duration = top_entities[0].elapsed;
    assert_eq!(top_entities[0].occurred_on, day_three);
    assert_eq!(top_entities[1].occurred_on, day_two);
    assert_eq!(top_entities[0].occurred_at, at_three);
    assert_eq!(top_entities[1].occurred_at, at_two);
    assert_eq!(top_entities[0].elapsed, elapsed_three);
    assert_eq!(top_entities[1].elapsed, elapsed_two);

    // Phase 2: lock bottom-k row terminal typing and ordering for temporal ranking.
    let bottom_response = load_window()
        .bottom_k_by("elapsed", 2)
        .expect("bottom_k_by(elapsed, 2) should succeed");
    let bottom_entities = bottom_response.entities();
    assert_eq!(
        bottom_entities.len(),
        2,
        "bottom_k_by should return two rows"
    );
    let _: Date = bottom_entities[0].occurred_on;
    let _: Timestamp = bottom_entities[0].occurred_at;
    let _: Duration = bottom_entities[0].elapsed;
    assert_eq!(bottom_entities[0].elapsed, elapsed_one);
    assert_eq!(bottom_entities[1].elapsed, elapsed_two);
    assert_eq!(bottom_entities[0].occurred_on, day_one);
    assert_eq!(bottom_entities[1].occurred_on, day_two);
    assert_eq!(bottom_entities[0].occurred_at, at_one);
    assert_eq!(bottom_entities[1].occurred_at, at_two);
}

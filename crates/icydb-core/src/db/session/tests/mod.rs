//! Module: db::session::tests
//! Responsibility: integration-style unit coverage for the session query, SQL,
//! explain, cursor, and write boundaries over shared in-memory fixtures.
//! Does not own: production session behavior outside this test module.
//! Boundary: verifies public and crate-visible session contracts while keeping fixture wiring local.

mod aggregate_explain;
mod aggregate_identity;
mod aggregate_terminals;
mod authority_labels;
mod composite_covering;
mod cursor;
mod direct_starts_with;
mod explain_execution;
mod expression_index;
mod filtered_composite_expression;
mod filtered_composite_order;
mod filtered_expression;
mod filtered_prefix;
mod indexed_covering;
mod indexed_prefix;
mod prefix_offsets;
mod query_lowering;
mod range_choice_offsets;
mod sql_aggregate;
mod sql_delete;
mod sql_explain;
mod sql_grouped;
mod sql_projection;
mod sql_scalar;
mod sql_surface;
mod sql_write;
mod temporal;
mod verbose_route_choice;

use super::*;
use crate::{
    db::{
        Db, MissingRowPolicy, PagedGroupedExecutionWithTrace, PlanError,
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
            intent::{Query, StructuralQuery},
            plan::{
                AggregateKind, FieldSlot,
                expr::{Expr, ProjectionField},
            },
        },
        registry::{StoreHandle, StoreRegistry},
        response::EntityResponse,
        sql::{
            lowering::{
                LoweredSqlQuery, apply_lowered_select_shape, bind_lowered_sql_query,
                is_sql_global_aggregate_statement, lower_sql_command_from_prepared_statement,
                prepare_sql_statement,
            },
            parser::{SqlSelectItem, SqlStatement, parse_sql},
        },
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

// Shared projected-row shape used by the session SQL projection tests.
type ProjectedRows = Vec<Vec<Value>>;

// Normalize the two common test call sites onto one session reference:
// standalone `DbSession` values from `sql_session()` and borrowed session
// parameters inside helper functions.
trait SessionSqlRef {
    fn db_session(&self) -> &DbSession<SessionSqlCanister>;
}

impl SessionSqlRef for DbSession<SessionSqlCanister> {
    fn db_session(&self) -> &DbSession<SessionSqlCanister> {
        self
    }
}

impl SessionSqlRef for &DbSession<SessionSqlCanister> {
    fn db_session(&self) -> &DbSession<SessionSqlCanister> {
        self
    }
}

impl SessionSqlRef for &&DbSession<SessionSqlCanister> {
    fn db_session(&self) -> &DbSession<SessionSqlCanister> {
        self
    }
}

// Lower one executable SELECT/DELETE-shaped SQL statement into a structural
// query so the lowering tests can compare canonical query intent directly.
fn lower_select_query_for_tests<E>(
    _session: &impl SessionSqlRef,
    sql: &str,
) -> Result<Query<E>, QueryError>
where
    E: crate::traits::EntityKind<Canister = SessionSqlCanister>,
{
    let statement = parse_sql(sql).map_err(QueryError::from_sql_parse_error)?;

    match &statement {
        SqlStatement::Insert(_) => {
            return Err(QueryError::unsupported_query(
                "SQL query lowering rejects INSERT; use execute_sql_update::<E>() or typed writes",
            ));
        }
        SqlStatement::Update(_) => {
            return Err(QueryError::unsupported_query(
                "SQL query lowering rejects UPDATE; use execute_sql_update::<E>() or typed writes",
            ));
        }
        SqlStatement::Delete(delete) if delete.returning.is_some() => {
            return Err(QueryError::unsupported_query(
                "SQL query lowering rejects DELETE RETURNING; use execute_sql_update::<E>()",
            ));
        }
        SqlStatement::Explain(_) => {
            return Err(QueryError::unsupported_query(
                "SQL query lowering rejects EXPLAIN; use execute_sql_query::<E>()",
            ));
        }
        SqlStatement::Describe(_) => {
            return Err(QueryError::unsupported_query(
                "SQL query lowering rejects DESCRIBE; use execute_sql_query::<E>()",
            ));
        }
        SqlStatement::ShowIndexes(_) => {
            return Err(QueryError::unsupported_query(
                "SQL query lowering rejects SHOW INDEXES; use execute_sql_query::<E>()",
            ));
        }
        SqlStatement::ShowColumns(_) => {
            return Err(QueryError::unsupported_query(
                "SQL query lowering rejects SHOW COLUMNS; use execute_sql_query::<E>()",
            ));
        }
        SqlStatement::ShowEntities(_) => {
            return Err(QueryError::unsupported_query(
                "SQL query lowering rejects SHOW ENTITIES; use execute_sql_query::<E>()",
            ));
        }
        SqlStatement::Select(statement)
            if sql_select_has_text_function(statement) && statement.group_by.is_empty() =>
        {
            return Err(QueryError::unsupported_query(
                "SQL query lowering does not accept computed text projection",
            ));
        }
        SqlStatement::Delete(_) | SqlStatement::Select(_) => {}
    }

    if is_sql_global_aggregate_statement(&statement) {
        return Err(QueryError::unsupported_query(
            "SQL query lowering rejects global aggregate SELECT",
        ));
    }

    let lowered = lower_sql_command_from_prepared_statement(
        prepare_sql_statement(statement, E::MODEL.name())
            .map_err(QueryError::from_sql_lowering_error)?,
        E::MODEL.primary_key.name,
    )
    .map_err(QueryError::from_sql_lowering_error)?;
    let Some(query) = lowered.query().cloned() else {
        return Err(QueryError::unsupported_query(
            "SQL query lowering accepts SELECT or DELETE only",
        ));
    };

    bind_lowered_sql_query::<E>(query, MissingRowPolicy::Ignore)
        .map_err(QueryError::from_sql_lowering_error)
}

// Execute one scalar SELECT through the old entity-row contract used by the
// legacy session tests without reintroducing any live lane-shaped runtime API.
fn execute_scalar_select_for_tests<E>(
    session: &impl SessionSqlRef,
    sql: &str,
) -> Result<EntityResponse<E>, QueryError>
where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue,
{
    let session = session.db_session();
    let statement = parse_sql(sql).map_err(QueryError::from_sql_parse_error)?;

    match &statement {
        SqlStatement::Delete(_) => {
            return Err(QueryError::unsupported_query(
                "scalar SELECT helper rejects DELETE; use execute_sql_update::<E>()",
            ));
        }
        SqlStatement::Explain(_) => {
            return Err(QueryError::unsupported_query(
                "scalar SELECT helper rejects EXPLAIN",
            ));
        }
        SqlStatement::Describe(_) => {
            return Err(QueryError::unsupported_query(
                "scalar SELECT helper rejects DESCRIBE",
            ));
        }
        SqlStatement::ShowIndexes(_) => {
            return Err(QueryError::unsupported_query(
                "scalar SELECT helper rejects SHOW INDEXES",
            ));
        }
        SqlStatement::ShowColumns(_) => {
            return Err(QueryError::unsupported_query(
                "scalar SELECT helper rejects SHOW COLUMNS",
            ));
        }
        SqlStatement::ShowEntities(_) => {
            return Err(QueryError::unsupported_query(
                "scalar SELECT helper rejects SHOW ENTITIES",
            ));
        }
        SqlStatement::Insert(_) => {
            return Err(QueryError::unsupported_query(
                "scalar SELECT helper rejects INSERT; use execute_sql_update::<E>()",
            ));
        }
        SqlStatement::Update(_) => {
            return Err(QueryError::unsupported_query(
                "scalar SELECT helper rejects UPDATE; use execute_sql_update::<E>()",
            ));
        }
        SqlStatement::Select(statement)
            if sql_select_has_text_function(statement) && statement.group_by.is_empty() =>
        {
            return Err(QueryError::unsupported_query(
                "scalar SELECT helper rejects computed text projection",
            ));
        }
        SqlStatement::Select(_) => {}
    }

    if is_sql_global_aggregate_statement(&statement) {
        return Err(QueryError::unsupported_query(
            "scalar SELECT helper rejects global aggregate SELECT",
        ));
    }

    let query = lower_select_query_for_tests::<E>(&session, sql)?;

    if query.has_grouping() {
        return Err(QueryError::unsupported_query(
            "scalar SELECT helper rejects grouped SELECT",
        ));
    }

    session.execute_query(&query)
}

// Execute one grouped SELECT through the grouped query runtime while preserving
// the cursor behavior and grouped computed projection coverage expected by the
// legacy session SQL matrix.
fn execute_grouped_select_for_tests<E>(
    session: &impl SessionSqlRef,
    sql: &str,
    cursor_token: Option<&str>,
) -> Result<PagedGroupedExecutionWithTrace, QueryError>
where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue,
{
    let session = session.db_session();
    let statement = parse_sql(sql).map_err(QueryError::from_sql_parse_error)?;

    match &statement {
        SqlStatement::Delete(_) => {
            return Err(QueryError::unsupported_query(
                "grouped SELECT helper rejects DELETE; use execute_sql_update::<E>()",
            ));
        }
        SqlStatement::Explain(_) => {
            return Err(QueryError::unsupported_query(
                "grouped SELECT helper rejects EXPLAIN",
            ));
        }
        SqlStatement::Describe(_) => {
            return Err(QueryError::unsupported_query(
                "grouped SELECT helper rejects DESCRIBE",
            ));
        }
        SqlStatement::ShowIndexes(_) => {
            return Err(QueryError::unsupported_query(
                "grouped SELECT helper rejects SHOW INDEXES",
            ));
        }
        SqlStatement::ShowColumns(_) => {
            return Err(QueryError::unsupported_query(
                "grouped SELECT helper rejects SHOW COLUMNS",
            ));
        }
        SqlStatement::ShowEntities(_) => {
            return Err(QueryError::unsupported_query(
                "grouped SELECT helper rejects SHOW ENTITIES",
            ));
        }
        SqlStatement::Insert(_) => {
            return Err(QueryError::unsupported_query(
                "grouped SELECT helper rejects INSERT; use execute_sql_update::<E>()",
            ));
        }
        SqlStatement::Update(_) => {
            return Err(QueryError::unsupported_query(
                "grouped SELECT helper rejects UPDATE; use execute_sql_update::<E>()",
            ));
        }
        SqlStatement::Select(statement)
            if sql_select_has_text_function(statement) && statement.group_by.is_empty() =>
        {
            return Err(QueryError::unsupported_query(
                "grouped SELECT helper rejects scalar computed text projection",
            ));
        }
        SqlStatement::Select(_) => {}
    }

    if is_sql_global_aggregate_statement(&statement) {
        return Err(QueryError::unsupported_query(
            "grouped SELECT helper rejects global aggregate SELECT",
        ));
    }

    session.execute_grouped_sql_query_for_tests::<E>(sql, cursor_token)
}

fn sql_select_has_text_function(statement: &crate::db::sql::parser::SqlSelectStatement) -> bool {
    matches!(
        &statement.projection,
        crate::db::sql::parser::SqlProjection::Items(items)
            if items
                .iter()
                .any(|item| matches!(item, SqlSelectItem::TextFunction(_)))
    )
}

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
/// SessionSqlWriteEntity
///
/// Numeric-key SQL write fixture used to lock reduced `INSERT` / `UPDATE`
/// statement semantics against literal-addressable primary keys.
///

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct SessionSqlWriteEntity {
    id: u64,
    name: String,
    age: u64,
}

///
/// SessionSqlGeneratedFieldEntity
///
/// SQL write fixture used to prove schema-owned insert generation is not
/// limited to primary keys and does not reuse general Rust default semantics.
///

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct SessionSqlGeneratedFieldEntity {
    id: u64,
    token: Ulid,
    name: String,
}

///
/// SessionSqlGeneratedTimestampEntity
///
/// SQL write fixture used to lock one second schema-owned generator family so
/// reduced SQL can omit explicit insert-time timestamps without consuming
/// ordinary field defaults.
///

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct SessionSqlGeneratedTimestampEntity {
    id: u64,
    created_on_insert: Timestamp,
    name: String,
}

///
/// SessionSqlManagedWriteEntity
///
/// SQL write fixture used to lock explicit managed timestamp rejection while
/// still allowing the write lane to synthesize those fields automatically.
///

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct SessionSqlManagedWriteEntity {
    id: u64,
    name: String,
    created_at: Timestamp,
    updated_at: Timestamp,
}

///
/// SessionSqlSignedWriteEntity
///
/// Signed-key SQL write fixture used to lock numeric literal widening at the
/// reduced SQL write boundary without broadening the production numeric
/// surface.
///

#[derive(
    Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, PersistedRow, Serialize,
)]
struct SessionSqlSignedWriteEntity {
    id: i64,
    delta: i64,
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
        ("id", FieldKind::Ulid, @generated crate::model::field::FieldInsertGeneration::Ulid),
        ("name", FieldKind::Text),
        ("age", FieldKind::Uint),
    ],
    indexes = [],
    store = SessionSqlStore,
    canister = SessionSqlCanister,
}

crate::test_entity_schema! {
    ident = SessionSqlWriteEntity,
    id = u64,
    id_field = id,
    entity_name = "SessionSqlWriteEntity",
    entity_tag = EntityTag::new(0x1044),
    pk_index = 0,
    fields = [
        ("id", FieldKind::Uint),
        ("name", FieldKind::Text),
        ("age", FieldKind::Uint),
    ],
    indexes = [],
    store = SessionSqlStore,
    canister = SessionSqlCanister,
}

crate::test_entity_schema! {
    ident = SessionSqlGeneratedFieldEntity,
    id = u64,
    id_field = id,
    entity_name = "SessionSqlGeneratedFieldEntity",
    entity_tag = EntityTag::new(0x1045),
    pk_index = 0,
    fields = [
        ("id", FieldKind::Uint),
        ("token", FieldKind::Ulid, @generated crate::model::field::FieldInsertGeneration::Ulid),
        ("name", FieldKind::Text),
    ],
    indexes = [],
    store = SessionSqlStore,
    canister = SessionSqlCanister,
}

crate::test_entity_schema! {
    ident = SessionSqlGeneratedTimestampEntity,
    id = u64,
    id_field = id,
    entity_name = "SessionSqlGeneratedTimestampEntity",
    entity_tag = EntityTag::new(0x1047),
    pk_index = 0,
    fields = [
        ("id", FieldKind::Uint),
        ("created_on_insert", FieldKind::Timestamp, @generated crate::model::field::FieldInsertGeneration::Timestamp),
        ("name", FieldKind::Text),
    ],
    indexes = [],
    store = SessionSqlStore,
    canister = SessionSqlCanister,
}

crate::test_entity_schema! {
    ident = SessionSqlManagedWriteEntity,
    id = u64,
    id_field = id,
    entity_name = "SessionSqlManagedWriteEntity",
    entity_tag = EntityTag::new(0x1046),
    pk_index = 0,
    fields = [
        ("id", FieldKind::Uint),
        ("name", FieldKind::Text),
        ("created_at", FieldKind::Timestamp, @managed crate::model::field::FieldWriteManagement::CreatedAt),
        ("updated_at", FieldKind::Timestamp, @managed crate::model::field::FieldWriteManagement::UpdatedAt),
    ],
    indexes = [],
    store = SessionSqlStore,
    canister = SessionSqlCanister,
}

crate::test_entity_schema! {
    ident = SessionSqlSignedWriteEntity,
    id = i64,
    id_field = id,
    entity_name = "SessionSqlSignedWriteEntity",
    entity_tag = EntityTag::new(0x1048),
    pk_index = 0,
    fields = [
        ("id", FieldKind::Int),
        ("delta", FieldKind::Int),
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
        statement_show_indexes_sql::<IndexedSessionSqlEntity>(
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
        statement_show_indexes_sql::<IndexedSessionSqlEntity>(
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
        statement_show_indexes_sql::<IndexedSessionSqlEntity>(
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

    let query = lower_select_query_for_tests::<IndexedSessionSqlEntity>(
        &session,
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
            trace.execution_family(),
            Some(crate::db::TraceExecutionFamily::Ordered)
        ),
        "ordered load shapes should project ordered execution family in trace payload",
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

fn unsupported_sql_statement_query_error(message: &'static str) -> QueryError {
    QueryError::execute(crate::error::InternalError::classified(
        ErrorClass::Unsupported,
        ErrorOrigin::Query,
        message,
    ))
}

fn parse_sql_statement_for_tests(
    _session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> Result<SqlStatement, QueryError> {
    crate::db::session::sql::parse_sql_statement(sql)
}

fn execute_sql_statement_for_tests<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> Result<SqlStatementResult, QueryError>
where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue,
{
    let statement = parse_sql_statement_for_tests(session, sql)?;

    session.execute_sql_statement_inner::<E>(&statement)
}

fn statement_projection_columns<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> Result<Vec<String>, QueryError>
where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue + crate::traits::EntityKind,
{
    match execute_sql_statement_for_tests::<E>(session, sql)? {
        SqlStatementResult::Projection { columns, .. }
        | SqlStatementResult::ProjectionText { columns, .. }
        | SqlStatementResult::Grouped { columns, .. } => Ok(columns),
        SqlStatementResult::Count { .. }
        | SqlStatementResult::Explain(_)
        | SqlStatementResult::Describe(_)
        | SqlStatementResult::ShowIndexes(_)
        | SqlStatementResult::ShowColumns(_)
        | SqlStatementResult::ShowEntities(_) => Err(unsupported_sql_statement_query_error(
            "projection column SQL only supports row-producing SQL statements",
        )),
    }
}

fn statement_projection_rows<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> Result<Vec<Vec<Value>>, QueryError>
where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue,
{
    match execute_sql_statement_for_tests::<E>(session, sql)? {
        SqlStatementResult::Projection { rows, .. } => Ok(rows),
        SqlStatementResult::ProjectionText { .. } | SqlStatementResult::Grouped { .. } => {
            Err(unsupported_sql_statement_query_error(
                "projection row SQL only supports value-row SQL projection payloads",
            ))
        }
        SqlStatementResult::Count { .. }
        | SqlStatementResult::Explain(_)
        | SqlStatementResult::Describe(_)
        | SqlStatementResult::ShowIndexes(_)
        | SqlStatementResult::ShowColumns(_)
        | SqlStatementResult::ShowEntities(_) => Err(unsupported_sql_statement_query_error(
            "projection row SQL only supports row-producing SQL statements",
        )),
    }
}

fn statement_explain_sql<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> Result<String, QueryError>
where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue,
{
    match execute_sql_statement_for_tests::<E>(session, sql)? {
        SqlStatementResult::Explain(explain) => Ok(explain),
        SqlStatementResult::Count { .. }
        | SqlStatementResult::Projection { .. }
        | SqlStatementResult::ProjectionText { .. }
        | SqlStatementResult::Grouped { .. }
        | SqlStatementResult::Describe(_)
        | SqlStatementResult::ShowIndexes(_)
        | SqlStatementResult::ShowColumns(_)
        | SqlStatementResult::ShowEntities(_) => Err(unsupported_sql_statement_query_error(
            "EXPLAIN SQL requires an EXPLAIN statement",
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

fn statement_describe_sql<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> Result<EntitySchemaDescription, QueryError>
where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue,
{
    match execute_sql_statement_for_tests::<E>(session, sql)? {
        SqlStatementResult::Describe(description) => Ok(description),
        SqlStatementResult::Count { .. }
        | SqlStatementResult::Projection { .. }
        | SqlStatementResult::ProjectionText { .. }
        | SqlStatementResult::Grouped { .. }
        | SqlStatementResult::Explain(_)
        | SqlStatementResult::ShowIndexes(_)
        | SqlStatementResult::ShowColumns(_)
        | SqlStatementResult::ShowEntities(_) => Err(unsupported_sql_statement_query_error(
            "DESCRIBE SQL requires a DESCRIBE statement",
        )),
    }
}

fn statement_show_indexes_sql<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> Result<Vec<String>, QueryError>
where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue,
{
    match execute_sql_statement_for_tests::<E>(session, sql)? {
        SqlStatementResult::ShowIndexes(indexes) => Ok(indexes),
        SqlStatementResult::Count { .. }
        | SqlStatementResult::Projection { .. }
        | SqlStatementResult::ProjectionText { .. }
        | SqlStatementResult::Grouped { .. }
        | SqlStatementResult::Explain(_)
        | SqlStatementResult::Describe(_)
        | SqlStatementResult::ShowColumns(_)
        | SqlStatementResult::ShowEntities(_) => Err(unsupported_sql_statement_query_error(
            "SHOW INDEXES SQL requires a SHOW INDEXES statement",
        )),
    }
}

fn statement_show_columns_sql<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> Result<Vec<EntityFieldDescription>, QueryError>
where
    E: PersistedRow<Canister = SessionSqlCanister> + EntityValue,
{
    match execute_sql_statement_for_tests::<E>(session, sql)? {
        SqlStatementResult::ShowColumns(columns) => Ok(columns),
        SqlStatementResult::Count { .. }
        | SqlStatementResult::Projection { .. }
        | SqlStatementResult::ProjectionText { .. }
        | SqlStatementResult::Grouped { .. }
        | SqlStatementResult::Explain(_)
        | SqlStatementResult::Describe(_)
        | SqlStatementResult::ShowIndexes(_)
        | SqlStatementResult::ShowEntities(_) => Err(unsupported_sql_statement_query_error(
            "SHOW COLUMNS SQL requires a SHOW COLUMNS statement",
        )),
    }
}

fn statement_show_entities_sql(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> Result<Vec<String>, QueryError> {
    let statement = parse_sql_statement_for_tests(session, sql)?;
    if !matches!(statement, SqlStatement::ShowEntities(_)) {
        return Err(unsupported_sql_statement_query_error(
            "SHOW ENTITIES SQL requires a SHOW ENTITIES statement",
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
    let plan = lower_select_query_for_tests::<FilteredIndexedSessionSqlEntity>(&session,
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

// Build one store-backed execution descriptor for reduced SQL so tests can
// assert the structured execution surface without snapshot-locking the JSON
// renderer.
fn store_backed_execution_descriptor_for_sql<E>(
    _session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> ExplainExecutionNodeDescriptor
where
    E: PersistedRow<Canister = SessionSqlCanister>
        + EntityValue
        + crate::traits::EntityKind<Canister = SessionSqlCanister>,
{
    let statement = crate::db::session::sql::parse_sql_statement(sql)
        .expect("store-backed execution descriptor sql should parse");
    let lowered = lower_sql_command_from_prepared_statement(
        prepare_sql_statement(statement, E::MODEL.name())
            .expect("store-backed execution descriptor sql should prepare"),
        E::MODEL.primary_key.name,
    )
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
    assemble_load_execution_node_descriptor(E::MODEL.fields(), E::MODEL.primary_key().name(), &plan)
        .expect("store-backed execution descriptor should assemble")
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
    execute_scalar_select_for_tests::<SessionSqlEntity>(&session, sql)
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
    predicate: impl Fn(&CursorPlanError) -> bool,
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
    predicate: impl Fn(&CursorPlanError) -> bool,
) {
    let mapped_via_executor =
        QueryError::from_executor_plan_error(ExecutorPlanError::from(build()));
    assert_query_error_is_cursor_plan(mapped_via_executor, &predicate);

    let mapped_via_plan = QueryError::from(PlanError::from(build()));
    assert_query_error_is_cursor_plan(mapped_via_plan, &predicate);
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

const fn unsupported_sql_feature_cases() -> [(&'static str, &'static str); 6] {
    [
        (
            "SELECT * FROM SessionSqlEntity JOIN other ON SessionSqlEntity.id = other.id",
            "JOIN",
        ),
        (
            "SELECT \"name\" FROM SessionSqlEntity",
            "quoted identifiers",
        ),
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

//! Module: db::session::tests
//! Responsibility: integration-style unit coverage for the session query, SQL,
//! explain, cursor, and write boundaries over shared in-memory fixtures.
//! Does not own: production session behavior outside this test module.
//! Boundary: verifies public and crate-visible session contracts while keeping fixture wiring local.

mod aggregate_explain;
mod aggregate_identity;
mod aggregate_terminals;
#[cfg(feature = "sql-explain")]
mod authority_labels;
mod branch_set;
mod composite_covering;
mod cursor;
mod direct_starts_with;
#[cfg(feature = "sql")]
mod execution_convergence;
mod execution_hot_path_bench;
mod execution_spine_guard;
mod explain_cache_convergence;
mod explain_execution;
mod expression_index;
mod filtered_composite_expression;
mod filtered_composite_order;
mod filtered_expression;
mod filtered_prefix;
mod heap_runtime;
mod indexed_covering;
mod indexed_prefix;
mod journaled_runtime;
mod lane_metrics;
mod predicate_convergence;
mod prefix_offsets;
mod query_lowering;
mod range_choice_offsets;
mod read_admission;
mod sql_aggregate;
mod sql_blob;
mod sql_delete;
#[cfg(feature = "sql-explain")]
mod sql_explain;
mod sql_grouped;
mod sql_projection;
mod sql_scalar;
mod sql_surface;
mod sql_write;
#[cfg(feature = "sql")]
mod sqlite_comparison;
mod storage_backend_perf;
mod temporal;
mod verbose_route_choice;

use super::*;
#[cfg(feature = "sql-explain")]
use crate::db::executor::assemble_load_execution_node_descriptor;
#[cfg(feature = "sql-explain")]
use crate::db::{
    query::intent::StructuralQuery,
    sql::lowering::{LoweredSqlQuery, apply_lowered_select_shape_for_model_only},
};
use crate::{
    db::{
        Db, EntityCatalogDescription, EntityFieldDescription, EntityRuntimeHooks,
        EntitySchemaDescription, MemoryCatalogDescription, MissingRowPolicy,
        PagedGroupedExecutionWithTrace, PersistedByKindCodec, PlanError, QueryError,
        StoreCatalogDescription,
        access::lower_access,
        commit::{
            ensure_recovered, init_commit_store_for_tests,
            reset_commit_marker_test_journal_sequence,
        },
        cursor::CursorPlanError,
        data::{
            DataStore, DecodedDataStoreKey, decode_structural_value_storage_bytes,
            encode_structural_value_storage_bytes,
        },
        direction::Direction,
        executor::ExecutorPlanError,
        index::{IndexKey, IndexStore, IndexStoreVisit, key_within_envelope},
        journal::{JournalBatch, JournalSequence, JournalTailStore, JournalTailVisit},
        key_taxonomy::PrimaryKeyComponent,
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        query::{
            builder::{
                AvgDistinctBySlotTerminal, CountDistinctBySlotTerminal, LastValueBySlotTerminal,
                SumBySlotTerminal,
            },
            explain::{
                ExplainAccessPath, ExplainExecutionNodeDescriptor, ExplainExecutionNodeType,
            },
            expr::FilterExpr,
            intent::Query,
            plan::{
                AggregateKind, FieldSlot,
                expr::{Expr, ProjectionField},
                validate::{ExprPlanError, PlanUserError},
            },
        },
        registry::StoreRegistry,
        response::EntityResponse,
        schema::{
            FieldId, PersistedSchemaSnapshot, SchemaFieldSlot, SchemaRowLayout, SchemaStore,
            SchemaValidationOperator, ValidateError, compiled_schema_proposal_for_model,
            publish_test_accepted_schema_snapshot,
        },
        sql::{
            lowering::{
                bind_lowered_sql_query_for_model_only,
                lower_sql_command_from_prepared_statement_for_model_only, prepare_sql_statement,
            },
            parser::{SqlExpr, SqlStatement},
        },
    },
    entity::EntityDeclaration,
    error::{ErrorClass, ErrorDetail, ErrorOrigin, QueryErrorDetail},
    metrics::sink::{MetricsEvent, MetricsSink, PlanKind, with_shared_metrics_sink},
    model::{
        field::{FieldKind, FieldModel, FieldStorageDecode, RelationStrength},
        index::{IndexExpression, IndexKeyItem, IndexModel, IndexPredicateMetadata},
    },
    testing::test_memory,
    traits::{FieldTypeMeta, Path},
    types::{Blob, Date, Duration, EntityTag, Float64, Id, Principal, Timestamp, Ulid},
    value::{OutputValue, RuntimeValueDecode, RuntimeValueEncode, Value},
};
use ic_stable_structures::{DefaultMemoryImpl, memory_manager::VirtualMemory};
use icydb_derive::{FieldProjection, PersistedRow};
use icydb_diagnostic_code::{
    DiagnosticCode, DiagnosticDetail, SqlFeatureCode, SqlLoweringCode, SqlWriteBoundaryCode,
};
use serde::Deserialize;
use std::{cell::RefCell, collections::BTreeMap, fmt::Debug, rc::Rc, sync::LazyLock};

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

crate::test_store! {
    ident = HeapSessionSqlStore,
    canister = SessionSqlCanister,
}

crate::test_store! {
    ident = JournaledSessionSqlStore,
    canister = SessionSqlCanister,
}

thread_local! {
    static SESSION_SQL_DATA_STORE: RefCell<DataStore> =
        RefCell::new(DataStore::init_journaled(test_memory(160)));
    static SESSION_SQL_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init_journaled(test_memory(161)));
    static SESSION_SQL_SCHEMA_STORE: RefCell<SchemaStore> =
        RefCell::new(SchemaStore::init_journaled(test_memory(164)));
    static SESSION_SQL_JOURNAL_STORE: RefCell<JournalTailStore> =
        RefCell::new(JournalTailStore::init(test_memory(184)));
    static SESSION_SQL_STORE_REGISTRY: StoreRegistry = {
        let mut reg = StoreRegistry::new();
        reg.register_journaled_store(
            SessionSqlStore::PATH,
            &SESSION_SQL_DATA_STORE,
            &SESSION_SQL_INDEX_STORE,
            &SESSION_SQL_SCHEMA_STORE,
            &SESSION_SQL_JOURNAL_STORE,
            crate::db::StoreAllocationIdentities::new_journaled(
                crate::db::StoreAllocationIdentity::new(160, "icydb.test.session.data.v1"),
                crate::db::StoreAllocationIdentity::new(161, "icydb.test.session.index.v1"),
                crate::db::StoreAllocationIdentity::new(164, "icydb.test.session.schema.v1"),
                crate::db::StoreAllocationIdentity::new(184, "icydb.test.session.journal.v1"),
            ),
            crate::db::StoreRuntimeStorageCapabilities::journaled(),
        )
        .expect("SQL session test store registration should succeed");
        reg
    };
    static INDEXED_SESSION_SQL_DATA_STORE: RefCell<DataStore> =
        RefCell::new(DataStore::init_journaled(test_memory(162)));
    static INDEXED_SESSION_SQL_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init_journaled(test_memory(163)));
    static INDEXED_SESSION_SQL_SCHEMA_STORE: RefCell<SchemaStore> =
        RefCell::new(SchemaStore::init_journaled(test_memory(165)));
    static INDEXED_SESSION_SQL_JOURNAL_STORE: RefCell<JournalTailStore> =
        RefCell::new(JournalTailStore::init(test_memory(185)));
    static INDEXED_SESSION_SQL_STORE_REGISTRY: StoreRegistry = {
        let mut reg = StoreRegistry::new();
        reg.register_journaled_store(
            IndexedSessionSqlStore::PATH,
            &INDEXED_SESSION_SQL_DATA_STORE,
            &INDEXED_SESSION_SQL_INDEX_STORE,
            &INDEXED_SESSION_SQL_SCHEMA_STORE,
            &INDEXED_SESSION_SQL_JOURNAL_STORE,
            crate::db::StoreAllocationIdentities::new_journaled(
                crate::db::StoreAllocationIdentity::new(
                    162,
                    "icydb.test.indexed_session.data.v1",
                ),
                crate::db::StoreAllocationIdentity::new(
                    163,
                    "icydb.test.indexed_session.index.v1",
                ),
                crate::db::StoreAllocationIdentity::new(
                    165,
                    "icydb.test.indexed_session.schema.v1",
                ),
                crate::db::StoreAllocationIdentity::new(
                    185,
                    "icydb.test.indexed_session.journal.v1",
                ),
            ),
            crate::db::StoreRuntimeStorageCapabilities::journaled(),
        )
        .expect("indexed SQL session test store registration should succeed");
        reg
    };
    static HEAP_SESSION_SQL_DATA_STORE: RefCell<DataStore> =
        const { RefCell::new(DataStore::init_heap()) };
    static HEAP_SESSION_SQL_INDEX_STORE: RefCell<IndexStore> =
        const { RefCell::new(IndexStore::init_heap()) };
    static HEAP_SESSION_SQL_SCHEMA_STORE: RefCell<SchemaStore> =
        const { RefCell::new(SchemaStore::init_heap()) };
    static HEAP_SESSION_SQL_STORE_REGISTRY: StoreRegistry = {
        let mut reg = StoreRegistry::new();
        reg.register_store(
            HeapSessionSqlStore::PATH,
            &HEAP_SESSION_SQL_DATA_STORE,
            &HEAP_SESSION_SQL_INDEX_STORE,
            &HEAP_SESSION_SQL_SCHEMA_STORE,
            crate::db::StoreAllocationIdentities::absent(),
            crate::db::StoreRuntimeStorageCapabilities::heap(),
        )
        .expect("heap SQL session test store registration should succeed");
        reg
    };
    static MIXED_HEAP_RELATION_STORE_REGISTRY: StoreRegistry = {
        let mut reg = StoreRegistry::new();
        reg.register_journaled_store(
            SessionSqlStore::PATH,
            &SESSION_SQL_DATA_STORE,
            &SESSION_SQL_INDEX_STORE,
            &SESSION_SQL_SCHEMA_STORE,
            &SESSION_SQL_JOURNAL_STORE,
            crate::db::StoreAllocationIdentities::new_journaled(
                crate::db::StoreAllocationIdentity::new(160, "icydb.test.session.data.v1"),
                crate::db::StoreAllocationIdentity::new(161, "icydb.test.session.index.v1"),
                crate::db::StoreAllocationIdentity::new(164, "icydb.test.session.schema.v1"),
                crate::db::StoreAllocationIdentity::new(184, "icydb.test.session.journal.v1"),
            ),
            crate::db::StoreRuntimeStorageCapabilities::journaled(),
        )
        .expect("mixed relation durable store registration should succeed");
        reg.register_store(
            HeapSessionSqlStore::PATH,
            &HEAP_SESSION_SQL_DATA_STORE,
            &HEAP_SESSION_SQL_INDEX_STORE,
            &HEAP_SESSION_SQL_SCHEMA_STORE,
            crate::db::StoreAllocationIdentities::absent(),
            crate::db::StoreRuntimeStorageCapabilities::heap(),
        )
        .expect("mixed relation heap store registration should succeed");
        reg
    };
    static JOURNALED_SESSION_SQL_DATA_STORE: RefCell<DataStore> =
        RefCell::new(DataStore::init_journaled(test_memory(180)));
    static JOURNALED_SESSION_SQL_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init_journaled(test_memory(181)));
    // Retain the schema allocation across store-wrapper reinitialization so
    // restart tests preserve the canonical accepted root.
    static JOURNALED_SESSION_SQL_SCHEMA_MEMORY: VirtualMemory<DefaultMemoryImpl> =
        test_memory(182);
    static JOURNALED_SESSION_SQL_SCHEMA_STORE: RefCell<SchemaStore> =
        RefCell::new(SchemaStore::init_journaled(
            JOURNALED_SESSION_SQL_SCHEMA_MEMORY.with(Clone::clone),
        ));
    static JOURNALED_SESSION_SQL_JOURNAL_STORE: RefCell<JournalTailStore> =
        RefCell::new(JournalTailStore::init(test_memory(183)));
    static JOURNALED_SESSION_SQL_STORE_REGISTRY: StoreRegistry = {
        let mut reg = StoreRegistry::new();
        reg.register_journaled_store(
            JournaledSessionSqlStore::PATH,
            &JOURNALED_SESSION_SQL_DATA_STORE,
            &JOURNALED_SESSION_SQL_INDEX_STORE,
            &JOURNALED_SESSION_SQL_SCHEMA_STORE,
            &JOURNALED_SESSION_SQL_JOURNAL_STORE,
            crate::db::StoreAllocationIdentities::new_journaled(
                crate::db::StoreAllocationIdentity::new(
                    180,
                    "icydb.test.journaled_session.data.v1",
                ),
                crate::db::StoreAllocationIdentity::new(
                    181,
                    "icydb.test.journaled_session.index.v1",
                ),
                crate::db::StoreAllocationIdentity::new(
                    182,
                    "icydb.test.journaled_session.schema.v1",
                ),
                crate::db::StoreAllocationIdentity::new(
                    183,
                    "icydb.test.journaled_session.journal.v1",
                ),
            ),
            crate::db::StoreRuntimeStorageCapabilities::journaled(),
        )
        .expect("journaled SQL session test store registration should succeed");
        reg
    };
    static MIXED_JOURNALED_RELATION_STORE_REGISTRY: StoreRegistry = {
        let mut reg = StoreRegistry::new();
        reg.register_journaled_store(
            SessionSqlStore::PATH,
            &SESSION_SQL_DATA_STORE,
            &SESSION_SQL_INDEX_STORE,
            &SESSION_SQL_SCHEMA_STORE,
            &SESSION_SQL_JOURNAL_STORE,
            crate::db::StoreAllocationIdentities::new_journaled(
                crate::db::StoreAllocationIdentity::new(160, "icydb.test.session.data.v1"),
                crate::db::StoreAllocationIdentity::new(161, "icydb.test.session.index.v1"),
                crate::db::StoreAllocationIdentity::new(164, "icydb.test.session.schema.v1"),
                crate::db::StoreAllocationIdentity::new(184, "icydb.test.session.journal.v1"),
            ),
            crate::db::StoreRuntimeStorageCapabilities::journaled(),
        )
        .expect("mixed journaled relation durable store registration should succeed");
        reg.register_journaled_store(
            JournaledSessionSqlStore::PATH,
            &JOURNALED_SESSION_SQL_DATA_STORE,
            &JOURNALED_SESSION_SQL_INDEX_STORE,
            &JOURNALED_SESSION_SQL_SCHEMA_STORE,
            &JOURNALED_SESSION_SQL_JOURNAL_STORE,
            crate::db::StoreAllocationIdentities::new_journaled(
                crate::db::StoreAllocationIdentity::new(
                    180,
                    "icydb.test.journaled_session.data.v1",
                ),
                crate::db::StoreAllocationIdentity::new(
                    181,
                    "icydb.test.journaled_session.index.v1",
                ),
                crate::db::StoreAllocationIdentity::new(
                    182,
                    "icydb.test.journaled_session.schema.v1",
                ),
                crate::db::StoreAllocationIdentity::new(
                    183,
                    "icydb.test.journaled_session.journal.v1",
                ),
            ),
            crate::db::StoreRuntimeStorageCapabilities::journaled(),
        )
        .expect("mixed journaled relation journaled store registration should succeed");
        reg
    };
}

static SESSION_SQL_RUNTIME_HOOKS: &[EntityRuntimeHooks<SessionSqlCanister>] = &[
    EntityRuntimeHooks::for_entity::<SessionSqlEntity>(),
    EntityRuntimeHooks::for_entity::<SessionSqlFieldPathEntity>(),
    EntityRuntimeHooks::for_entity::<SessionSqlRecordFieldPathEntity>(),
    EntityRuntimeHooks::for_entity::<SessionNullableSqlEntity>(),
    EntityRuntimeHooks::for_entity::<SessionSqlWriteEntity>(),
    EntityRuntimeHooks::for_entity::<SessionSqlCompositeWriteEntity>(),
    EntityRuntimeHooks::for_entity::<SessionSqlBlobEntity>(),
    EntityRuntimeHooks::for_entity::<SessionSqlGeneratedFieldEntity>(),
    EntityRuntimeHooks::for_entity::<SessionSqlGeneratedTimestampEntity>(),
    EntityRuntimeHooks::for_entity::<SessionSqlManagedWriteEntity>(),
    EntityRuntimeHooks::for_entity::<SessionSqlGeneratedKeyManagedWriteEntity>(),
    EntityRuntimeHooks::for_entity::<SessionSqlSignedWriteEntity>(),
    EntityRuntimeHooks::for_entity::<SessionSqlSelfRelationEntity>(),
    EntityRuntimeHooks::for_entity::<SessionSqlMixedNumericCompareEntity>(),
    EntityRuntimeHooks::for_entity::<SessionSqlBoolCompareEntity>(),
    EntityRuntimeHooks::for_entity::<SessionSqlFloatCompareEntity>(),
    EntityRuntimeHooks::for_entity::<SessionSqlFieldBoundRangeEntity>(),
    EntityRuntimeHooks::for_entity::<SessionPrincipalKeyEntity>(),
    EntityRuntimeHooks::for_entity::<SessionAggregateEntity>(),
    EntityRuntimeHooks::for_entity::<SessionTemporalEntity>(),
];
static INDEXED_SESSION_SQL_RUNTIME_HOOKS: &[EntityRuntimeHooks<SessionSqlCanister>] = &[
    EntityRuntimeHooks::for_entity::<IndexedSessionSqlEntity>(),
    EntityRuntimeHooks::for_entity::<CompositeIndexedSessionSqlEntity>(),
    EntityRuntimeHooks::for_entity::<BranchIndexedSessionSqlEntity>(),
    #[cfg(feature = "diagnostics")]
    EntityRuntimeHooks::for_entity::<ExplicitPkSuffixIndexedSessionSqlEntity>(),
    EntityRuntimeHooks::for_entity::<FilteredIndexedSessionSqlEntity>(),
    EntityRuntimeHooks::for_entity::<ExpressionIndexedSessionSqlEntity>(),
    EntityRuntimeHooks::for_entity::<SessionExplainEntity>(),
    EntityRuntimeHooks::for_entity::<SessionDeterministicChoiceEntity>(),
    EntityRuntimeHooks::for_entity::<SessionDeterministicRangeEntity>(),
    EntityRuntimeHooks::for_entity::<SessionRangeStrengthEntity>(),
    EntityRuntimeHooks::for_entity::<SessionResidualRankingEntity>(),
    EntityRuntimeHooks::for_entity::<SessionUniquePrefixOffsetEntity>(),
    EntityRuntimeHooks::for_entity::<SessionOrderOnlyChoiceEntity>(),
];
static HEAP_SESSION_SQL_RUNTIME_HOOKS: &[EntityRuntimeHooks<SessionSqlCanister>] =
    &[EntityRuntimeHooks::for_entity::<HeapSessionSqlEntity>()];
static SESSION_SQL_DB: Db<SessionSqlCanister> =
    Db::new_with_hooks(&SESSION_SQL_STORE_REGISTRY, SESSION_SQL_RUNTIME_HOOKS);
static INDEXED_SESSION_SQL_DB: Db<SessionSqlCanister> = Db::new_with_hooks(
    &INDEXED_SESSION_SQL_STORE_REGISTRY,
    INDEXED_SESSION_SQL_RUNTIME_HOOKS,
);
static HEAP_SESSION_SQL_DB: Db<SessionSqlCanister> = Db::new_with_hooks(
    &HEAP_SESSION_SQL_STORE_REGISTRY,
    HEAP_SESSION_SQL_RUNTIME_HOOKS,
);
static MIXED_HEAP_RELATION_RUNTIME_HOOKS: &[EntityRuntimeHooks<SessionSqlCanister>] = &[
    EntityRuntimeHooks::for_entity::<SessionSqlSelfRelationEntity>(),
    EntityRuntimeHooks::for_entity::<HeapSessionSqlEntity>(),
    EntityRuntimeHooks::for_entity::<DurableSessionSqlSourceToHeapTargetEntity>(),
    EntityRuntimeHooks::for_entity::<DurableSessionSqlWeakSourceToHeapTargetEntity>(),
    EntityRuntimeHooks::for_entity::<HeapSessionSqlSourceToDurableTargetEntity>(),
    EntityRuntimeHooks::for_entity::<HeapSessionSqlSourceToHeapTargetEntity>(),
];
static MIXED_HEAP_RELATION_DB: Db<SessionSqlCanister> = Db::new_with_hooks(
    &MIXED_HEAP_RELATION_STORE_REGISTRY,
    MIXED_HEAP_RELATION_RUNTIME_HOOKS,
);
static JOURNALED_SESSION_SQL_RUNTIME_HOOKS: &[EntityRuntimeHooks<SessionSqlCanister>] =
    &[EntityRuntimeHooks::for_entity::<JournaledSessionSqlEntity>()];
static JOURNALED_SESSION_SQL_DB: Db<SessionSqlCanister> = Db::new_with_hooks(
    &JOURNALED_SESSION_SQL_STORE_REGISTRY,
    JOURNALED_SESSION_SQL_RUNTIME_HOOKS,
);
static MIXED_JOURNALED_RELATION_RUNTIME_HOOKS: &[EntityRuntimeHooks<SessionSqlCanister>] = &[
    EntityRuntimeHooks::for_entity::<JournaledSessionSqlEntity>(),
    EntityRuntimeHooks::for_entity::<DurableSessionSqlSourceToJournaledTargetEntity>(),
];
static MIXED_JOURNALED_RELATION_DB: Db<SessionSqlCanister> = Db::new_with_hooks(
    &MIXED_JOURNALED_RELATION_STORE_REGISTRY,
    MIXED_JOURNALED_RELATION_RUNTIME_HOOKS,
);
static ACTIVE_TRUE_PREDICATE: LazyLock<Predicate> =
    LazyLock::new(|| Predicate::eq("active".to_string(), true.into()));
static ACTIVE_TRUE_AND_ARCHIVED_FALSE_PREDICATE: LazyLock<Predicate> = LazyLock::new(|| {
    Predicate::And(vec![
        Predicate::eq("active".to_string(), true.into()),
        Predicate::eq("archived".to_string(), false.into()),
    ])
});
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

///
/// SelectTestSurface
///
/// One test-only classifier that keeps SQL lowering, scalar, and grouped helper
/// rejection contracts on one shared parsed-statement path.
///

#[derive(Clone, Copy, Eq, PartialEq)]
enum SelectTestSurface {
    Lowering,
    Scalar,
    Grouped,
}

impl SelectTestSurface {
    // Build one helper-specific statement-family rejection error while
    // preserving the unsupported-query taxonomy.
    fn reject_statement(self, statement_kind: &'static str) -> QueryError {
        let _ = (self, statement_kind);

        QueryError::unsupported_query()
    }

    // Return the helper-specific unsupported-query error for one parsed SQL
    // statement shape so each test surface keeps its rejection taxonomy.
    fn statement_rejection(self, statement: &SqlStatement) -> Option<QueryError> {
        match statement {
            SqlStatement::Insert(_) => Some(self.reject_statement("INSERT")),
            SqlStatement::Update(_) => Some(self.reject_statement("UPDATE")),
            SqlStatement::Delete(delete)
                if self == Self::Lowering && delete.returning.is_some() =>
            {
                Some(QueryError::unsupported_query())
            }
            SqlStatement::Delete(_) if self != Self::Lowering => {
                Some(self.reject_statement("DELETE"))
            }
            #[cfg(feature = "sql-explain")]
            SqlStatement::Explain(_) => Some(self.reject_statement("EXPLAIN")),
            SqlStatement::Describe(_) => Some(self.reject_statement("DESCRIBE")),
            SqlStatement::ShowIndexes(_) => Some(self.reject_statement("SHOW INDEXES")),
            SqlStatement::ShowColumns(_) => Some(self.reject_statement("SHOW COLUMNS")),
            SqlStatement::ShowEntities(_) => Some(self.reject_statement("SHOW ENTITIES")),
            SqlStatement::ShowStores(_) => Some(self.reject_statement("SHOW STORES")),
            SqlStatement::ShowMemory(_) => Some(self.reject_statement("SHOW MEMORY")),
            SqlStatement::Select(statement)
                if sql_select_has_text_specific_computed_projection(statement)
                    && self == Self::Lowering =>
            {
                statement
                    .group_by
                    .is_empty()
                    .then(QueryError::unsupported_query)
            }
            SqlStatement::Select(statement)
                if sql_select_has_text_specific_computed_projection(statement)
                    && self == Self::Scalar =>
            {
                statement
                    .group_by
                    .is_empty()
                    .then(QueryError::unsupported_query)
            }
            SqlStatement::Select(statement)
                if sql_select_has_grouped_helper_rejected_computed_projection(statement)
                    && self == Self::Grouped =>
            {
                Some(QueryError::unsupported_query())
            }
            _ => None,
        }
    }

    // Return the helper-specific global aggregate boundary error when the
    // parsed statement stays on the aggregate-only execution lane.
    fn global_aggregate_rejection(self) -> QueryError {
        let _ = self;
        QueryError::unsupported_query()
    }
}

// Parse one test helper SQL surface and apply the shared lane-specific
// rejection matrix before lowering or execution begins.
fn parse_select_test_statement(
    session: &impl SessionSqlRef,
    sql: &str,
    surface: SelectTestSurface,
) -> Result<SqlStatement, QueryError> {
    let statement = parse_sql_statement_for_tests(session.db_session(), sql)?;

    // Phase 1: preserve each helper's explicit statement-family boundary.
    if let Some(err) = surface.statement_rejection(&statement) {
        return Err(err);
    }

    // Phase 2: keep global aggregate execution on the dedicated aggregate lane.
    if statement.is_global_aggregate_lane_shape() {
        return Err(surface.global_aggregate_rejection());
    }

    Ok(statement)
}

// Lower one already-validated SQL statement into the structural query shape
// shared by the scalar and grouped test helpers.
fn lower_select_statement_for_tests<E>(statement: SqlStatement) -> Result<Query<E>, QueryError>
where
    E: crate::entity::EntityKind<Canister = SessionSqlCanister>,
{
    let lowered = lower_sql_command_from_prepared_statement_for_model_only(
        prepare_sql_statement(&statement, E::MODEL.name())
            .map_err(QueryError::from_sql_lowering_error)?,
        E::MODEL,
    )
    .map_err(QueryError::from_sql_lowering_error)?;
    let Some(query) = lowered.query().cloned() else {
        return Err(QueryError::unsupported_query());
    };

    bind_lowered_sql_query_for_model_only::<E>(query, MissingRowPolicy::Ignore)
        .map_err(QueryError::from_sql_lowering_error)
}

// Lower one executable SELECT/DELETE-shaped SQL statement into a structural
// query so the lowering tests can compare canonical query intent directly.
fn lower_select_query_for_tests<E>(
    session: &impl SessionSqlRef,
    sql: &str,
) -> Result<Query<E>, QueryError>
where
    E: crate::entity::EntityKind<Canister = SessionSqlCanister>,
{
    let statement = parse_select_test_statement(session, sql, SelectTestSurface::Lowering)?;

    lower_select_statement_for_tests::<E>(statement)
}

// Execute one scalar SELECT through the typed entity-row response contract used
// by session tests without reintroducing any live lane-shaped runtime API.
fn execute_scalar_select_for_tests<E>(
    session: &impl SessionSqlRef,
    sql: &str,
) -> Result<EntityResponse<E>, QueryError>
where
    E: PersistedRow<Canister = SessionSqlCanister>,
{
    let session = session.db_session();
    let statement = parse_select_test_statement(&session, sql, SelectTestSurface::Scalar)?;
    let query = lower_select_statement_for_tests::<E>(statement)?;

    if query.has_grouping() {
        return Err(QueryError::unsupported_query());
    }

    session.execute_scalar_query_rows(&query)
}

// Execute one grouped SELECT through the grouped query runtime while keeping
// grouped-only routing and rejection contracts explicit at the helper boundary.
fn execute_grouped_select_for_tests<E>(
    session: &impl SessionSqlRef,
    sql: &str,
    cursor_token: Option<&str>,
) -> Result<PagedGroupedExecutionWithTrace, QueryError>
where
    E: PersistedRow<Canister = SessionSqlCanister>,
{
    let session = session.db_session();
    let statement = parse_select_test_statement(&session, sql, SelectTestSurface::Grouped)?;
    let query = lower_select_statement_for_tests::<E>(statement)?;
    if !query.has_grouping() {
        return Err(QueryError::unsupported_query());
    }

    session.execute_grouped(&query, cursor_token)
}

fn sql_select_has_text_specific_computed_projection(
    statement: &crate::db::sql::parser::SqlSelectStatement,
) -> bool {
    matches!(
        &statement.projection,
        crate::db::sql::parser::SqlProjection::Items(items)
            if items
                .iter()
                .any(|item| sql_expr_contains_text_specific_computed_projection(&SqlExpr::from_select_item(item)))
    )
}

// Keep the grouped helper fail-closed for computed projection expressions over
// grouped raw fields, but allow pure post-aggregate computed expressions so
// alias-based wrapped aggregate ordering can stay aligned with direct terms.
fn sql_select_has_grouped_helper_rejected_computed_projection(
    statement: &crate::db::sql::parser::SqlSelectStatement,
) -> bool {
    matches!(
        &statement.projection,
        crate::db::sql::parser::SqlProjection::Items(items)
            if items
                .iter()
                .any(|item| {
                    sql_select_item_is_grouped_helper_rejected_computed_projection(
                        item,
                        statement.group_by.as_slice(),
                    )
                })
    )
}

// Treat field and aggregate leaves as helper-owned grouped projection shapes,
// and only reject computed expressions that are not pure post-aggregate work.
fn sql_select_item_is_grouped_helper_rejected_computed_projection(
    item: &crate::db::sql::parser::SqlSelectItem,
    group_by: &[String],
) -> bool {
    match item {
        crate::db::sql::parser::SqlSelectItem::Field(_)
        | crate::db::sql::parser::SqlSelectItem::Aggregate(_) => false,
        crate::db::sql::parser::SqlSelectItem::Expr(expr) => {
            !sql_expr_is_grouped_post_aggregate_projection(expr)
                && !sql_expr_is_grouped_key_projection(expr, group_by)
        }
    }
}

// Admit one computed grouped projection only when it is derived entirely from
// aggregate leaves and wrapper expressions instead of raw grouped field access.
fn sql_expr_is_grouped_post_aggregate_projection(expr: &SqlExpr) -> bool {
    expr.contains_aggregate() && !sql_expr_has_direct_field_outside_aggregate(expr)
}

// Keep computed grouped-key projections on the additive-key lane when they
// depend only on grouped key fields and avoid text wrappers.
fn sql_expr_is_grouped_key_projection(expr: &SqlExpr, group_by: &[String]) -> bool {
    !sql_expr_contains_text_specific_computed_projection(expr)
        && sql_expr_references_only_group_fields(expr, group_by)
}

// Detect whether one expression stays entirely on grouped key field access so
// the grouped helper can keep the additive-key projection lane.
fn sql_expr_references_only_group_fields(expr: &SqlExpr, group_by: &[String]) -> bool {
    match expr {
        SqlExpr::Field(field) => group_by.iter().any(|group_field| group_field == field),
        SqlExpr::FieldPath { .. } | SqlExpr::Aggregate(_) => false,
        SqlExpr::Literal(_) | SqlExpr::Param { .. } => true,
        SqlExpr::Membership { expr, .. }
        | SqlExpr::Like { expr, .. }
        | SqlExpr::NullTest { expr, .. }
        | SqlExpr::Unary { expr, .. } => sql_expr_references_only_group_fields(expr, group_by),
        SqlExpr::FunctionCall { args, .. } => args
            .iter()
            .all(|arg| sql_expr_references_only_group_fields(arg, group_by)),
        SqlExpr::Binary { left, right, .. } => {
            sql_expr_references_only_group_fields(left, group_by)
                && sql_expr_references_only_group_fields(right, group_by)
        }
        SqlExpr::Case { arms, else_expr } => {
            arms.iter().all(|arm| {
                sql_expr_references_only_group_fields(&arm.condition, group_by)
                    && sql_expr_references_only_group_fields(&arm.result, group_by)
            }) && else_expr
                .as_ref()
                .is_none_or(|else_expr| sql_expr_references_only_group_fields(else_expr, group_by))
        }
    }
}

// Detect raw grouped field access that escapes aggregate ownership so the
// grouped helper keeps field-derived computed projections on the grouped-field
// boundary.
fn sql_expr_has_direct_field_outside_aggregate(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::Field(_) | SqlExpr::FieldPath { .. } => true,
        SqlExpr::Aggregate(_) | SqlExpr::Literal(_) | SqlExpr::Param { .. } => false,
        SqlExpr::Membership { expr, .. }
        | SqlExpr::Like { expr, .. }
        | SqlExpr::NullTest { expr, .. }
        | SqlExpr::Unary { expr, .. } => sql_expr_has_direct_field_outside_aggregate(expr),
        SqlExpr::FunctionCall { args, .. } => {
            args.iter().any(sql_expr_has_direct_field_outside_aggregate)
        }
        SqlExpr::Binary { left, right, .. } => {
            sql_expr_has_direct_field_outside_aggregate(left)
                || sql_expr_has_direct_field_outside_aggregate(right)
        }
        SqlExpr::Case { arms, else_expr } => {
            arms.iter().any(|arm| {
                sql_expr_has_direct_field_outside_aggregate(&arm.condition)
                    || sql_expr_has_direct_field_outside_aggregate(&arm.result)
            }) || else_expr
                .as_ref()
                .is_some_and(|else_expr| sql_expr_has_direct_field_outside_aggregate(else_expr))
        }
    }
}

fn sql_expr_contains_text_specific_computed_projection(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::Field(_)
        | SqlExpr::FieldPath { .. }
        | SqlExpr::Aggregate(_)
        | SqlExpr::Literal(_)
        | SqlExpr::Param { .. } => false,
        SqlExpr::Membership { expr, .. }
        | SqlExpr::Like { expr, .. }
        | SqlExpr::NullTest { expr, .. }
        | SqlExpr::Unary { expr, .. } => sql_expr_contains_text_specific_computed_projection(expr),
        SqlExpr::FunctionCall { function, args } => {
            !matches!(function, crate::db::sql::parser::SqlScalarFunction::Round)
                || args
                    .iter()
                    .any(sql_expr_contains_text_specific_computed_projection)
        }
        SqlExpr::Binary { left, right, .. } => {
            sql_expr_contains_text_specific_computed_projection(left)
                || sql_expr_contains_text_specific_computed_projection(right)
        }
        SqlExpr::Case { arms, else_expr } => {
            arms.iter().any(|arm| {
                sql_expr_contains_text_specific_computed_projection(&arm.condition)
                    || sql_expr_contains_text_specific_computed_projection(&arm.result)
            }) || else_expr.as_ref().is_some_and(|else_expr| {
                sql_expr_contains_text_specific_computed_projection(else_expr)
            })
        }
    }
}

fn active_true_predicate() -> &'static Predicate {
    &ACTIVE_TRUE_PREDICATE
}

fn active_true_and_archived_false_predicate() -> &'static Predicate {
    &ACTIVE_TRUE_AND_ARCHIVED_FALSE_PREDICATE
}

const fn active_true_predicate_metadata() -> IndexPredicateMetadata {
    IndexPredicateMetadata::generated("active = true", active_true_predicate)
}

const fn active_true_and_archived_false_predicate_metadata() -> IndexPredicateMetadata {
    IndexPredicateMetadata::generated(
        "active = true AND archived = false",
        active_true_and_archived_false_predicate,
    )
}

///
/// SessionSqlEntity
///
/// Test entity used to lock end-to-end reduced SQL session behavior.
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct SessionSqlEntity {
    id: Ulid,
    name: String,
    age: u64,
}

///
/// SessionSqlFieldPathEntity
///
/// SessionSqlFieldPathEntity keeps one typed value-storage profile on the SQL
/// session fixture surface so scan-only FieldPath predicate execution can be
/// verified through the public SQL path without changing the shared scalar
/// fixture.
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct SessionSqlFieldPathEntity {
    id: Ulid,
    name: String,
    profile: SessionSqlFieldPathProfile,
}

impl Default for SessionSqlFieldPathEntity {
    fn default() -> Self {
        Self {
            id: Ulid::from_u128(0),
            name: String::new(),
            profile: SessionSqlFieldPathProfile::default(),
        }
    }
}

///
/// SessionSqlFieldPathProfile
///
/// SessionSqlFieldPathProfile is the typed structured field used by scan-only
/// FieldPath predicate tests.
/// The rank state lets tests distinguish a missing `rank` key from an explicit
/// SQL NULL while keeping the persisted field statically typed.
///

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
struct SessionSqlFieldPathProfile {
    rank: SessionSqlFieldPathRank,
}

impl SessionSqlFieldPathProfile {
    fn rank(value: Value) -> Self {
        match value {
            Value::Int64(value) => Self {
                rank: SessionSqlFieldPathRank::Value(value),
            },
            Value::Null => Self {
                rank: SessionSqlFieldPathRank::Null,
            },
            _ => Self::default(),
        }
    }
}

///
/// SessionSqlFieldPathRank
///
/// SessionSqlFieldPathRank models the three nested field states needed by the
/// SQL FieldPath predicate fixture.
/// It keeps missing-key and explicit-NULL semantics distinct without using a
/// nested option type in the persisted test wrapper.
///

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
enum SessionSqlFieldPathRank {
    #[default]
    Missing,
    Null,
    Value(i64),
}

impl FieldTypeMeta for SessionSqlFieldPathProfile {
    const KIND: FieldKind = FieldKind::Structured { queryable: false };
    const NESTED_FIELDS: &'static [FieldModel] = &[FieldModel::generated("rank", FieldKind::Int64)];
    const STORAGE_DECODE: FieldStorageDecode = FieldStorageDecode::Value;
}

impl RuntimeValueEncode for SessionSqlFieldPathProfile {
    fn to_value(&self) -> Value {
        let rank = match self.rank {
            SessionSqlFieldPathRank::Missing => return Value::Map(Vec::new()),
            SessionSqlFieldPathRank::Null => Value::Null,
            SessionSqlFieldPathRank::Value(value) => Value::Int64(value),
        };

        Value::Map(vec![(Value::Text("rank".to_string()), rank)])
    }
}

impl RuntimeValueDecode for SessionSqlFieldPathProfile {
    fn from_value(value: &Value) -> Option<Self> {
        let Value::Map(entries) = value else {
            return None;
        };

        let rank = session_sql_profile_record_entry(entries.as_slice(), "rank");
        let rank = match rank {
            Some(Value::Int64(rank)) => SessionSqlFieldPathRank::Value(*rank),
            Some(Value::Null) => SessionSqlFieldPathRank::Null,
            Some(_) => return None,
            None => SessionSqlFieldPathRank::Missing,
        };

        Some(Self { rank })
    }
}

///
/// SessionSqlProfileRecord
///
/// SessionSqlProfileRecord is the typed structured field fixture used by SQL
/// FieldPath projection tests.
/// It persists through the same structured value-storage lane as generated
/// records while keeping this core test module independent from schema macro
/// expansion.
///

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
struct SessionSqlProfileRecord {
    rank: i64,
    nickname: String,
}

impl FieldTypeMeta for SessionSqlProfileRecord {
    const KIND: FieldKind = FieldKind::Structured { queryable: false };
    const NESTED_FIELDS: &'static [FieldModel] = &[
        FieldModel::generated("rank", FieldKind::Int64),
        FieldModel::generated("nickname", FieldKind::Text { max_len: None }),
    ];
    const STORAGE_DECODE: FieldStorageDecode = FieldStorageDecode::Value;
}

impl RuntimeValueEncode for SessionSqlProfileRecord {
    fn to_value(&self) -> Value {
        Value::Map(vec![
            (
                Value::Text("nickname".to_string()),
                Value::Text(self.nickname.clone()),
            ),
            (Value::Text("rank".to_string()), Value::Int64(self.rank)),
        ])
    }
}

impl RuntimeValueDecode for SessionSqlProfileRecord {
    fn from_value(value: &Value) -> Option<Self> {
        let Value::Map(entries) = value else {
            return None;
        };

        let rank = session_sql_profile_record_entry(entries.as_slice(), "rank")?;
        let nickname = session_sql_profile_record_entry(entries.as_slice(), "nickname")?;
        let Value::Int64(rank) = rank else {
            return None;
        };
        let Value::Text(nickname) = nickname else {
            return None;
        };

        Some(Self {
            rank: *rank,
            nickname: nickname.clone(),
        })
    }
}

impl PersistedByKindCodec for SessionSqlProfileRecord {
    fn encode_persisted_slot_payload_by_kind(
        &self,
        kind: FieldKind,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        if !matches!(kind, FieldKind::Structured { queryable: false }) {
            return Err(InternalError::persisted_row_field_encode_failed(
                field_name,
                format!("field kind {kind:?} does not accept SQL profile record payload"),
            ));
        }

        encode_structural_value_storage_bytes(&self.to_value())
            .map_err(|err| InternalError::persisted_row_field_encode_failed(field_name, err))
    }

    fn decode_persisted_option_slot_payload_by_kind(
        bytes: &[u8],
        kind: FieldKind,
        field_name: &'static str,
    ) -> Result<Option<Self>, InternalError> {
        if !matches!(kind, FieldKind::Structured { queryable: false }) {
            return Err(InternalError::persisted_row_field_decode_failed(
                field_name,
                format!("field kind {kind:?} does not decode as SQL profile record payload"),
            ));
        }

        let value = decode_structural_value_storage_bytes(bytes)
            .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))?;
        if matches!(value, Value::Null) {
            return Ok(None);
        }

        Self::from_value(&value).map(Some).ok_or_else(|| {
            InternalError::persisted_row_field_decode_failed(
                field_name,
                "payload does not match SQL profile record",
            )
        })
    }
}

// Resolve one typed record field from the runtime `Value::Map` projection used
// by this test fixture's manual runtime-value bridge.
fn session_sql_profile_record_entry<'a>(
    entries: &'a [(Value, Value)],
    field: &str,
) -> Option<&'a Value> {
    entries.iter().find_map(|(key, value)| match key {
        Value::Text(key) if key == field => Some(value),
        _ => None,
    })
}

///
/// SessionSqlRecordFieldPathEntity
///
/// SessionSqlRecordFieldPathEntity stores a typed record-like profile field so
/// SQL projection tests can prove `SELECT profile.subfield` works when the
/// root field is not the untyped `Value` fixture.
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct SessionSqlRecordFieldPathEntity {
    id: Ulid,
    name: String,
    profile: SessionSqlProfileRecord,
}

///
/// SessionNullableSqlEntity
///
/// Session-local nullable scalar fixture used to prove the live SQL session
/// path distinguishes null tests from ordinary comparison-to-NULL spellings on
/// persisted nullable text data.
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct SessionNullableSqlEntity {
    id: Ulid,
    name: String,
    nickname: Option<String>,
}

///
/// SessionSqlWriteEntity
///
/// Numeric-key SQL write fixture used to lock reduced `INSERT` / `UPDATE`
/// statement semantics against literal-addressable primary keys.
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct SessionSqlWriteEntity {
    id: u64,
    name: String,
    age: u64,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SessionSqlCompositeWriteEntityKey {
    tenant_id: u64,
    local_id: u64,
}

impl crate::db::KeyValueCodec for SessionSqlCompositeWriteEntityKey {
    fn to_key_value(&self) -> Value {
        Value::List(vec![
            Value::Nat64(self.tenant_id),
            Value::Nat64(self.local_id),
        ])
    }

    fn from_key_value(value: &Value) -> Option<Self> {
        let Value::List(values) = value else {
            return None;
        };
        let [Value::Nat64(tenant_id), Value::Nat64(local_id)] = values.as_slice() else {
            return None;
        };

        Some(Self {
            tenant_id: *tenant_id,
            local_id: *local_id,
        })
    }
}

impl crate::db::PrimaryKeyEncode for SessionSqlCompositeWriteEntityKey {
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

impl crate::db::PrimaryKeyDecode for SessionSqlCompositeWriteEntityKey {
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

impl crate::db::EntityKeyBytes for SessionSqlCompositeWriteEntityKey {
    const BYTE_LEN: usize = 16;

    fn write_bytes(&self, out: &mut [u8]) -> Result<(), crate::db::EntityKeyBytesError> {
        crate::db::validate_entity_key_bytes_buffer(out, Self::BYTE_LEN)?;
        out[..8].copy_from_slice(&self.tenant_id.to_be_bytes());
        out[8..16].copy_from_slice(&self.local_id.to_be_bytes());

        Ok(())
    }
}

///
/// SessionSqlCompositeWriteEntity
///
/// Composite-key SQL write fixture used to prove reduced SQL write lanes bind
/// all ordered primary-key components at insert/update target boundaries.
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct SessionSqlCompositeWriteEntity {
    tenant_id: u64,
    local_id: u64,
    name: String,
    age: u64,
}

///
/// SessionSqlBlobEntity
///
/// SessionSqlBlobEntity gives SQL write/read tests a focused large-payload
/// fixture with ordinary metadata columns next to thumbnail and chunk blobs.
/// Tests use it to exercise row-wide mutation paths without overloading the
/// small scalar write fixture.
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct SessionSqlBlobEntity {
    id: Ulid,
    label: String,
    bucket: u64,
    thumbnail: Blob,
    chunk: Blob,
}

///
/// SessionSqlGeneratedFieldEntity
///
/// SQL write fixture used to prove schema-owned insert generation is not
/// limited to primary keys and does not reuse general Rust default semantics.
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
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

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
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

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct SessionSqlManagedWriteEntity {
    id: u64,
    name: String,
    created_at: Timestamp,
    updated_at: Timestamp,
}

///
/// SessionSqlGeneratedKeyManagedWriteEntity
///
/// SQL write fixture used to lock `INSERT ... SELECT` synthesis when the
/// primary key and lifecycle timestamps are all SQL-owned omitted fields.
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct SessionSqlGeneratedKeyManagedWriteEntity {
    id: Ulid,
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

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct SessionSqlSignedWriteEntity {
    id: i64,
    delta: i64,
}

///
/// SessionSqlSelfRelationEntity
///
/// SQL write fixture used to document that statement-atomic structural batches
/// still validate strong relation targets against committed stores only.
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct SessionSqlSelfRelationEntity {
    id: u64,
    parent: Option<u64>,
}

///
/// SessionSqlMixedNumericCompareEntity
///
/// Mixed numeric compare fixture used to lock field-to-field equality widening
/// across signed and unsigned scalar fields on the live SQL session path.
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct SessionSqlMixedNumericCompareEntity {
    id: Ulid,
    label: String,
    left_score: u64,
    right_score: i64,
}

///
/// SessionSqlBoolCompareEntity
///
/// Bool field-compare fixture used to lock semantic rejection for ordered
/// field-to-field boolean predicates on the live SQL session path.
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct SessionSqlBoolCompareEntity {
    id: Ulid,
    label: String,
    active: bool,
    archived: bool,
}

///
/// SessionSqlFloatCompareEntity
///
/// Float-backed scalar compare fixture used to lock ordered numeric literal
/// widening when reduced SQL parses one decimal-looking literal for one float
/// field predicate.
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct SessionSqlFloatCompareEntity {
    id: Ulid,
    label: String,
    dodge_chance: Float64,
}

///
/// SessionSqlFieldBoundRangeEntity
///
/// Numeric field-bound range fixture used to lock `BETWEEN` / `NOT BETWEEN`
/// lowering and execution when both bounds come from sibling fields.
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct SessionSqlFieldBoundRangeEntity {
    id: Ulid,
    label: String,
    score: u64,
    min_score: u64,
    max_score: u64,
}

///
/// IndexedSessionSqlEntity
///
/// Indexed SQL session fixture used to lock strict text-prefix execution over a
/// real secondary `name` index.
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct IndexedSessionSqlEntity {
    id: Ulid,
    name: String,
    age: u64,
}

///
/// SessionPrincipalKeyEntity
///
/// External scalar primary-key fixture used to lock Principal-key lookup
/// planning through the public session boundary.
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct SessionPrincipalKeyEntity {
    pid: Principal,
    user_id: Ulid,
    label: String,
}

///
/// FilteredIndexedSessionSqlEntity
///
/// Filtered indexed SQL session fixture used to lock guarded order-only
/// fallback against one real `name` index with the `active = true` predicate.
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
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

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct CompositeIndexedSessionSqlEntity {
    id: Ulid,
    code: String,
    serial: u64,
    note: String,
}

///
/// BranchIndexedSessionSqlEntity
///
/// Composite indexed SQL session fixture used to lock branch-aware
/// `(collection_id, stage, id)` execution for generated list/page shapes.
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct BranchIndexedSessionSqlEntity {
    id: Ulid,
    collection_id: String,
    stage: String,
    title: String,
}

///
/// ExplicitPkSuffixIndexedSessionSqlEntity
///
/// Indexed SQL fixture mirroring generated/audit schemas that redundantly put
/// the primary key at the end of a secondary index, such as `(bucket, id)`.
///

#[cfg(feature = "diagnostics")]
#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct ExplicitPkSuffixIndexedSessionSqlEntity {
    id: Ulid,
    bucket: u64,
    label: String,
}

///
/// ExpressionIndexedSessionSqlEntity
///
/// Expression-indexed SQL session fixture used to lock `ORDER BY LOWER(field)`
/// planning and execution against one real expression-key secondary index.
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct ExpressionIndexedSessionSqlEntity {
    id: Ulid,
    name: String,
    age: u64,
}

///
/// SessionAggregateEntity
///
/// Session-facing aggregate fixture covering projection and ranked terminal
/// contracts under the live `db::session` owner.
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
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

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
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

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
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

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct SessionDeterministicRangeEntity {
    id: Ulid,
    tier: String,
    score: u64,
    handle: String,
    label: String,
}

///
/// SessionRangeStrengthEntity
///
/// Session-local range-strength fixture used to lock bounded-range ranking
/// through the recovered session-visible index boundary.
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct SessionRangeStrengthEntity {
    id: Ulid,
    tier: String,
    score: u64,
    label: String,
}

///
/// SessionResidualRankingEntity
///
/// Session-local residual-ranking fixture used to lock same-score filtered
/// index competition when one route discharges more residual predicate work.
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct SessionResidualRankingEntity {
    id: Ulid,
    active: bool,
    archived: bool,
    tier: String,
    label: String,
}

///
/// SessionUniquePrefixOffsetEntity
///
/// Session-local unique-prefix fixture used to lock offset-aware ordered load
/// admission on one unique secondary `(tier, handle)` route.
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
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

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
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

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
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
        1,
        "name_active_only",
        IndexedSessionSqlStore::PATH,
        &INDEXED_SESSION_SQL_INDEX_FIELDS,
        false,
        Some(active_true_predicate_metadata()),
    )];
static FILTERED_INDEXED_SESSION_SQL_COMPOSITE_INDEX_FIELDS: [&str; 2] = ["tier", "handle"];
static FILTERED_INDEXED_SESSION_SQL_COMPOSITE_INDEX_MODELS: [IndexModel; 1] =
    [IndexModel::generated_with_ordinal_and_predicate(
        2,
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
        3,
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
        4,
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
static BRANCH_INDEXED_SESSION_SQL_INDEX_FIELDS: [&str; 3] = ["collection_id", "stage", "id"];
static BRANCH_INDEXED_SESSION_SQL_INDEX_MODELS: [IndexModel; 1] = [IndexModel::generated(
    "collection_stage_id",
    IndexedSessionSqlStore::PATH,
    &BRANCH_INDEXED_SESSION_SQL_INDEX_FIELDS,
    false,
)];
#[cfg(feature = "diagnostics")]
static EXPLICIT_PK_SUFFIX_INDEXED_SESSION_SQL_INDEX_FIELDS: [&str; 2] = ["bucket", "id"];
#[cfg(feature = "diagnostics")]
static EXPLICIT_PK_SUFFIX_INDEXED_SESSION_SQL_INDEX_MODELS: [IndexModel; 1] =
    [IndexModel::generated(
        "bucket_id",
        IndexedSessionSqlStore::PATH,
        &EXPLICIT_PK_SUFFIX_INDEXED_SESSION_SQL_INDEX_FIELDS,
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
        1,
        "a_tier_label_idx",
        IndexedSessionSqlStore::PATH,
        &SESSION_DETERMINISTIC_CHOICE_LABEL_INDEX_FIELDS,
        false,
    ),
    IndexModel::generated_with_ordinal(
        2,
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
        1,
        "a_tier_score_handle_idx",
        IndexedSessionSqlStore::PATH,
        &SESSION_DETERMINISTIC_RANGE_HANDLE_INDEX_FIELDS,
        false,
    ),
    IndexModel::generated_with_ordinal(
        2,
        "z_tier_score_label_idx",
        IndexedSessionSqlStore::PATH,
        &SESSION_DETERMINISTIC_RANGE_LABEL_INDEX_FIELDS,
        false,
    ),
];
static SESSION_RANGE_STRENGTH_LABEL_INDEX_FIELDS: [&str; 2] = ["tier", "label"];
static SESSION_RANGE_STRENGTH_SCORE_INDEX_FIELDS: [&str; 2] = ["tier", "score"];
static SESSION_RANGE_STRENGTH_INDEX_MODELS: [IndexModel; 2] = [
    IndexModel::generated_with_ordinal(
        1,
        "a_tier_label_idx",
        IndexedSessionSqlStore::PATH,
        &SESSION_RANGE_STRENGTH_LABEL_INDEX_FIELDS,
        false,
    ),
    IndexModel::generated_with_ordinal(
        2,
        "z_tier_score_idx",
        IndexedSessionSqlStore::PATH,
        &SESSION_RANGE_STRENGTH_SCORE_INDEX_FIELDS,
        false,
    ),
];
static SESSION_RESIDUAL_RANKING_INDEX_FIELDS: [&str; 1] = ["tier"];
static SESSION_RESIDUAL_RANKING_INDEX_MODELS: [IndexModel; 2] = [
    IndexModel::generated_with_ordinal_and_predicate(
        1,
        "a_tier_active_idx",
        IndexedSessionSqlStore::PATH,
        &SESSION_RESIDUAL_RANKING_INDEX_FIELDS,
        false,
        Some(active_true_predicate_metadata()),
    ),
    IndexModel::generated_with_ordinal_and_predicate(
        2,
        "z_tier_active_live_idx",
        IndexedSessionSqlStore::PATH,
        &SESSION_RESIDUAL_RANKING_INDEX_FIELDS,
        false,
        Some(active_true_and_archived_false_predicate_metadata()),
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
        1,
        "a_beta_idx",
        IndexedSessionSqlStore::PATH,
        &SESSION_ORDER_ONLY_CHOICE_BETA_INDEX_FIELDS,
        false,
    ),
    IndexModel::generated_with_ordinal(
        2,
        "z_alpha_idx",
        IndexedSessionSqlStore::PATH,
        &SESSION_ORDER_ONLY_CHOICE_ALPHA_INDEX_FIELDS,
        false,
    ),
];
static HEAP_SESSION_SQL_INDEX_FIELDS: [&str; 1] = ["name"];
static HEAP_SESSION_SQL_INDEX_MODELS: [IndexModel; 1] = [IndexModel::generated(
    "name",
    HeapSessionSqlStore::PATH,
    &HEAP_SESSION_SQL_INDEX_FIELDS,
    false,
)];
static JOURNALED_SESSION_SQL_INDEX_FIELDS: [&str; 1] = ["name"];
static JOURNALED_SESSION_SQL_INDEX_MODELS: [IndexModel; 1] = [IndexModel::generated(
    "name",
    JournaledSessionSqlStore::PATH,
    &JOURNALED_SESSION_SQL_INDEX_FIELDS,
    false,
)];

crate::test_entity! {
    ident = SessionSqlEntity,
    entity_name = "SessionSqlEntity",
    tag = crate::testing::SESSION_SQL_ENTITY_TAG,
    store = SessionSqlStore,
    canister = SessionSqlCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! {
            id: Ulid => FieldKind::Ulid,
            options = crate::testing::TestFieldModelOptions::DEFAULT
                .with_insert_generation(crate::model::field::FieldInsertGeneration::Ulid),
        },
        crate::test_field! { name: String => FieldKind::Text { max_len: None } },
        crate::test_field! { age: u64 => FieldKind::Nat64 },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

crate::test_entity! {
    ident = SessionSqlFieldPathEntity,
    entity_name = "SessionSqlFieldPathEntity",
    tag = crate::testing::SESSION_SQL_FIELD_PATH_ENTITY_TAG,
    store = SessionSqlStore,
    canister = SessionSqlCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! {
            id: Ulid => FieldKind::Ulid,
            options = crate::testing::TestFieldModelOptions::DEFAULT
                .with_insert_generation(crate::model::field::FieldInsertGeneration::Ulid),
        },
        crate::test_field! { name: String => FieldKind::Text { max_len: None } },
        crate::test_field! {
            profile: SessionSqlProfileRecord => FieldKind::Structured { queryable: false },
            options = crate::testing::TestFieldModelOptions::DEFAULT
                .with_storage_decode(FieldStorageDecode::Value),
        },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

impl SessionSqlRecordFieldPathEntity {
    const PROFILE_NESTED_FIELDS: [FieldModel; 2] = [
        FieldModel::generated("rank", FieldKind::Int64),
        FieldModel::generated("nickname", FieldKind::Text { max_len: None }),
    ];
}

crate::test_entity! {
    ident = SessionSqlRecordFieldPathEntity,
    entity_name = "SessionSqlRecordFieldPathEntity",
    tag = EntityTag::new(0x1057),
    store = SessionSqlStore,
    canister = SessionSqlCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! {
            id: Ulid => FieldKind::Ulid,
            options = crate::testing::TestFieldModelOptions::DEFAULT
                .with_insert_generation(crate::model::field::FieldInsertGeneration::Ulid),
        },
        crate::test_field! { name: String => FieldKind::Text { max_len: None } },
        crate::test_field! {
            profile: SessionSqlProfileRecord => FieldKind::Structured { queryable: false },
            options = crate::testing::TestFieldModelOptions::DEFAULT
                .with_storage_decode(FieldStorageDecode::Value)
                .with_nested_fields(&SessionSqlRecordFieldPathEntity::PROFILE_NESTED_FIELDS),
        },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

crate::test_entity! {
    ident = SessionNullableSqlEntity,
    entity_name = "SessionNullableSqlEntity",
    tag = EntityTag::new(0x104C),
    store = SessionSqlStore,
    canister = SessionSqlCanister,
    version = 2,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { name: String => FieldKind::Text { max_len: None } },
        crate::test_field! {
            nickname: Option<String> => FieldKind::Text { max_len: None },
            options = crate::testing::TestFieldModelOptions::DEFAULT.with_nullable(true),
        },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

crate::test_entity! {
    ident = SessionSqlWriteEntity,
    entity_name = "SessionSqlWriteEntity",
    tag = EntityTag::new(0x1044),
    store = SessionSqlStore,
    canister = SessionSqlCanister,
    key_type = u64,
    primary_key = [id],
    fields = [
        crate::test_field! { id: u64 => FieldKind::Nat64 },
        crate::test_field! { name: String => FieldKind::Text { max_len: None } },
        crate::test_field! { age: u64 => FieldKind::Nat64 },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

crate::test_entity! {
    ident = SessionSqlCompositeWriteEntity,
    entity_name = "SessionSqlCompositeWriteEntity",
    tag = EntityTag::new(0x1059),
    store = SessionSqlStore,
    canister = SessionSqlCanister,
    key_type = SessionSqlCompositeWriteEntityKey,
    primary_key = [tenant_id, local_id],
    fields = [
        crate::test_field! { tenant_id: u64 => FieldKind::Nat64 },
        crate::test_field! { local_id: u64 => FieldKind::Nat64 },
        crate::test_field! { name: String => FieldKind::Text { max_len: None } },
        crate::test_field! { age: u64 => FieldKind::Nat64 },
    ],
    indexes = [],
    relations = [],
    entity_value = key(|entity: &SessionSqlCompositeWriteEntity| SessionSqlCompositeWriteEntityKey {
        tenant_id: entity.tenant_id,
        local_id: entity.local_id,
    }),
}

crate::test_entity! {
    ident = SessionSqlBlobEntity,
    entity_name = "SessionSqlBlobEntity",
    tag = EntityTag::new(0x1058),
    store = SessionSqlStore,
    canister = SessionSqlCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! {
            id: Ulid => FieldKind::Ulid,
            options = crate::testing::TestFieldModelOptions::DEFAULT
                .with_insert_generation(crate::model::field::FieldInsertGeneration::Ulid),
        },
        crate::test_field! { label: String => FieldKind::Text { max_len: None } },
        crate::test_field! { bucket: u64 => FieldKind::Nat64 },
        crate::test_field! { thumbnail: Blob => FieldKind::Blob { max_len: None } },
        crate::test_field! { chunk: Blob => FieldKind::Blob { max_len: None } },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

crate::test_entity! {
    ident = SessionSqlGeneratedFieldEntity,
    entity_name = "SessionSqlGeneratedFieldEntity",
    tag = EntityTag::new(0x1045),
    store = SessionSqlStore,
    canister = SessionSqlCanister,
    key_type = u64,
    primary_key = [id],
    fields = [
        crate::test_field! { id: u64 => FieldKind::Nat64 },
        crate::test_field! {
            token: Ulid => FieldKind::Ulid,
            options = crate::testing::TestFieldModelOptions::DEFAULT
                .with_insert_generation(crate::model::field::FieldInsertGeneration::Ulid),
        },
        crate::test_field! { name: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

crate::test_entity! {
    ident = SessionSqlGeneratedTimestampEntity,
    entity_name = "SessionSqlGeneratedTimestampEntity",
    tag = EntityTag::new(0x1047),
    store = SessionSqlStore,
    canister = SessionSqlCanister,
    key_type = u64,
    primary_key = [id],
    fields = [
        crate::test_field! { id: u64 => FieldKind::Nat64 },
        crate::test_field! {
            created_on_insert: Timestamp => FieldKind::Timestamp,
            options = crate::testing::TestFieldModelOptions::DEFAULT
                .with_insert_generation(crate::model::field::FieldInsertGeneration::Timestamp),
        },
        crate::test_field! { name: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

crate::test_entity! {
    ident = SessionSqlManagedWriteEntity,
    entity_name = "SessionSqlManagedWriteEntity",
    tag = EntityTag::new(0x1046),
    store = SessionSqlStore,
    canister = SessionSqlCanister,
    key_type = u64,
    primary_key = [id],
    fields = [
        crate::test_field! { id: u64 => FieldKind::Nat64 },
        crate::test_field! { name: String => FieldKind::Text { max_len: None } },
        crate::test_field! {
            created_at: Timestamp => FieldKind::Timestamp,
            options = crate::testing::TestFieldModelOptions::DEFAULT
                .with_write_management(crate::model::field::FieldWriteManagement::CreatedAt),
        },
        crate::test_field! {
            updated_at: Timestamp => FieldKind::Timestamp,
            options = crate::testing::TestFieldModelOptions::DEFAULT
                .with_write_management(crate::model::field::FieldWriteManagement::UpdatedAt),
        },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

crate::test_entity! {
    ident = SessionSqlGeneratedKeyManagedWriteEntity,
    entity_name = "SessionSqlGeneratedKeyManagedWriteEntity",
    tag = EntityTag::new(0x1051),
    store = SessionSqlStore,
    canister = SessionSqlCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! {
            id: Ulid => FieldKind::Ulid,
            options = crate::testing::TestFieldModelOptions::DEFAULT
                .with_insert_generation(crate::model::field::FieldInsertGeneration::Ulid),
        },
        crate::test_field! { name: String => FieldKind::Text { max_len: None } },
        crate::test_field! {
            created_at: Timestamp => FieldKind::Timestamp,
            options = crate::testing::TestFieldModelOptions::DEFAULT
                .with_write_management(crate::model::field::FieldWriteManagement::CreatedAt),
        },
        crate::test_field! {
            updated_at: Timestamp => FieldKind::Timestamp,
            options = crate::testing::TestFieldModelOptions::DEFAULT
                .with_write_management(crate::model::field::FieldWriteManagement::UpdatedAt),
        },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

crate::test_entity! {
    ident = SessionSqlSignedWriteEntity,
    entity_name = "SessionSqlSignedWriteEntity",
    tag = EntityTag::new(0x1048),
    store = SessionSqlStore,
    canister = SessionSqlCanister,
    key_type = i64,
    primary_key = [id],
    fields = [
        crate::test_field! { id: i64 => FieldKind::Int64 },
        crate::test_field! { delta: i64 => FieldKind::Int64 },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

static SESSION_SQL_SELF_RELATION_PARENT_KIND: FieldKind = FieldKind::Relation {
    target_path: SessionSqlSelfRelationEntity::PATH,
    target_entity_name: "SessionSqlSelfRelationEntity",
    target_entity_tag: SessionSqlSelfRelationEntity::ENTITY_TAG,
    target_store_path: SessionSqlStore::PATH,
    key_kind: &FieldKind::Nat64,
    strength: RelationStrength::Strong,
};

crate::test_entity! {
    ident = SessionSqlSelfRelationEntity,
    entity_name = "SessionSqlSelfRelationEntity",
    tag = EntityTag::new(0x105F),
    store = SessionSqlStore,
    canister = SessionSqlCanister,
    key_type = u64,
    primary_key = [id],
    fields = [
        crate::test_field! { id: u64 => FieldKind::Nat64 },
        crate::test_field! {
            parent: Option<u64> => SESSION_SQL_SELF_RELATION_PARENT_KIND,
            options = crate::testing::TestFieldModelOptions::DEFAULT.with_nullable(true),
        },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

crate::test_entity! {
    ident = SessionSqlMixedNumericCompareEntity,
    entity_name = "SessionSqlMixedNumericCompareEntity",
    tag = EntityTag::new(0x1049),
    store = SessionSqlStore,
    canister = SessionSqlCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! {
            id: Ulid => FieldKind::Ulid,
            options = crate::testing::TestFieldModelOptions::DEFAULT
                .with_insert_generation(crate::model::field::FieldInsertGeneration::Ulid),
        },
        crate::test_field! { label: String => FieldKind::Text { max_len: None } },
        crate::test_field! { left_score: u64 => FieldKind::Nat64 },
        crate::test_field! { right_score: i64 => FieldKind::Int64 },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

crate::test_entity! {
    ident = SessionSqlBoolCompareEntity,
    entity_name = "SessionSqlBoolCompareEntity",
    tag = EntityTag::new(0x104A),
    store = SessionSqlStore,
    canister = SessionSqlCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! {
            id: Ulid => FieldKind::Ulid,
            options = crate::testing::TestFieldModelOptions::DEFAULT
                .with_insert_generation(crate::model::field::FieldInsertGeneration::Ulid),
        },
        crate::test_field! { label: String => FieldKind::Text { max_len: None } },
        crate::test_field! { active: bool => FieldKind::Bool },
        crate::test_field! { archived: bool => FieldKind::Bool },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

crate::test_entity! {
    ident = SessionSqlFloatCompareEntity,
    entity_name = "SessionSqlFloatCompareEntity",
    tag = EntityTag::new(0x104D),
    store = SessionSqlStore,
    canister = SessionSqlCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! {
            id: Ulid => FieldKind::Ulid,
            options = crate::testing::TestFieldModelOptions::DEFAULT
                .with_insert_generation(crate::model::field::FieldInsertGeneration::Ulid),
        },
        crate::test_field! { label: String => FieldKind::Text { max_len: None } },
        crate::test_field! { dodge_chance: Float64 => FieldKind::Float64 },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

crate::test_entity! {
    ident = SessionSqlFieldBoundRangeEntity,
    entity_name = "SessionSqlFieldBoundRangeEntity",
    tag = EntityTag::new(0x104B),
    store = SessionSqlStore,
    canister = SessionSqlCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! {
            id: Ulid => FieldKind::Ulid,
            options = crate::testing::TestFieldModelOptions::DEFAULT
                .with_insert_generation(crate::model::field::FieldInsertGeneration::Ulid),
        },
        crate::test_field! { label: String => FieldKind::Text { max_len: None } },
        crate::test_field! { score: u64 => FieldKind::Nat64 },
        crate::test_field! { min_score: u64 => FieldKind::Nat64 },
        crate::test_field! { max_score: u64 => FieldKind::Nat64 },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

crate::test_entity! {
    ident = IndexedSessionSqlEntity,
    entity_name = "IndexedSessionSqlEntity",
    tag = EntityTag::new(0x1033),
    store = IndexedSessionSqlStore,
    canister = SessionSqlCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { name: String => FieldKind::Text { max_len: None } },
        crate::test_field! { age: u64 => FieldKind::Nat64 },
    ],
    indexes = [&INDEXED_SESSION_SQL_INDEX_MODELS[0]],
    relations = [],
    entity_value = id_field(id),
}

crate::test_entity! {
    ident = SessionPrincipalKeyEntity,
    entity_name = "SessionPrincipalKeyEntity",
    tag = EntityTag::new(0x1077),
    store = SessionSqlStore,
    canister = SessionSqlCanister,
    key_type = Principal,
    primary_key = [pid],
    fields = [
        crate::test_field! { pid: Principal => FieldKind::Principal },
        crate::test_field! { user_id: Ulid => FieldKind::Ulid },
        crate::test_field! { label: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(pid),
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct HeapSessionSqlEntity {
    id: u64,
    name: String,
    age: u64,
}

crate::test_entity! {
    ident = HeapSessionSqlEntity,
    entity_name = "HeapSessionSqlEntity",
    tag = EntityTag::new(0x1070),
    store = HeapSessionSqlStore,
    canister = SessionSqlCanister,
    key_type = u64,
    primary_key = [id],
    fields = [
        crate::test_field! { id: u64 => FieldKind::Nat64 },
        crate::test_field! { name: String => FieldKind::Text { max_len: None } },
        crate::test_field! { age: u64 => FieldKind::Nat64 },
    ],
    indexes = [&HEAP_SESSION_SQL_INDEX_MODELS[0]],
    relations = [],
    entity_value = id_field(id),
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct JournaledSessionSqlEntity {
    id: u64,
    name: String,
    age: u64,
}

crate::test_entity! {
    ident = JournaledSessionSqlEntity,
    entity_name = "JournaledSessionSqlEntity",
    tag = EntityTag::new(0x1075),
    store = JournaledSessionSqlStore,
    canister = SessionSqlCanister,
    key_type = u64,
    primary_key = [id],
    fields = [
        crate::test_field! { id: u64 => FieldKind::Nat64 },
        crate::test_field! { name: String => FieldKind::Text { max_len: None } },
        crate::test_field! { age: u64 => FieldKind::Nat64 },
    ],
    indexes = [&JOURNALED_SESSION_SQL_INDEX_MODELS[0]],
    relations = [],
    entity_value = id_field(id),
}

static SESSION_SQL_HEAP_TARGET_RELATION_KIND: FieldKind = FieldKind::Relation {
    target_path: HeapSessionSqlEntity::PATH,
    target_entity_name: "HeapSessionSqlEntity",
    target_entity_tag: HeapSessionSqlEntity::ENTITY_TAG,
    target_store_path: HeapSessionSqlStore::PATH,
    key_kind: &FieldKind::Nat64,
    strength: RelationStrength::Strong,
};

static SESSION_SQL_HEAP_TARGET_WEAK_RELATION_KIND: FieldKind = FieldKind::Relation {
    target_path: HeapSessionSqlEntity::PATH,
    target_entity_name: "HeapSessionSqlEntity",
    target_entity_tag: HeapSessionSqlEntity::ENTITY_TAG,
    target_store_path: HeapSessionSqlStore::PATH,
    key_kind: &FieldKind::Nat64,
    strength: RelationStrength::Weak,
};

static SESSION_SQL_DURABLE_TARGET_RELATION_KIND: FieldKind = FieldKind::Relation {
    target_path: SessionSqlSelfRelationEntity::PATH,
    target_entity_name: "SessionSqlSelfRelationEntity",
    target_entity_tag: SessionSqlSelfRelationEntity::ENTITY_TAG,
    target_store_path: SessionSqlStore::PATH,
    key_kind: &FieldKind::Nat64,
    strength: RelationStrength::Strong,
};

static SESSION_SQL_JOURNALED_TARGET_RELATION_KIND: FieldKind = FieldKind::Relation {
    target_path: JournaledSessionSqlEntity::PATH,
    target_entity_name: "JournaledSessionSqlEntity",
    target_entity_tag: JournaledSessionSqlEntity::ENTITY_TAG,
    target_store_path: JournaledSessionSqlStore::PATH,
    key_kind: &FieldKind::Nat64,
    strength: RelationStrength::Strong,
};

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct DurableSessionSqlSourceToHeapTargetEntity {
    id: u64,
    target_id: u64,
}

crate::test_entity! {
    ident = DurableSessionSqlSourceToHeapTargetEntity,
    entity_name = "DurableSessionSqlSourceToHeapTargetEntity",
    tag = EntityTag::new(0x1071),
    store = SessionSqlStore,
    canister = SessionSqlCanister,
    key_type = u64,
    primary_key = [id],
    fields = [
        crate::test_field! { id: u64 => FieldKind::Nat64 },
        crate::test_field! { target_id: u64 => SESSION_SQL_HEAP_TARGET_RELATION_KIND },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct DurableSessionSqlSourceToJournaledTargetEntity {
    id: u64,
    target_id: u64,
}

crate::test_entity! {
    ident = DurableSessionSqlSourceToJournaledTargetEntity,
    entity_name = "DurableSessionSqlSourceToJournaledTargetEntity",
    tag = EntityTag::new(0x1076),
    store = SessionSqlStore,
    canister = SessionSqlCanister,
    key_type = u64,
    primary_key = [id],
    fields = [
        crate::test_field! { id: u64 => FieldKind::Nat64 },
        crate::test_field! { target_id: u64 => SESSION_SQL_JOURNALED_TARGET_RELATION_KIND },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct DurableSessionSqlWeakSourceToHeapTargetEntity {
    id: u64,
    target_id: u64,
}

crate::test_entity! {
    ident = DurableSessionSqlWeakSourceToHeapTargetEntity,
    entity_name = "DurableSessionSqlWeakSourceToHeapTargetEntity",
    tag = EntityTag::new(0x1074),
    store = SessionSqlStore,
    canister = SessionSqlCanister,
    key_type = u64,
    primary_key = [id],
    fields = [
        crate::test_field! { id: u64 => FieldKind::Nat64 },
        crate::test_field! { target_id: u64 => SESSION_SQL_HEAP_TARGET_WEAK_RELATION_KIND },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct HeapSessionSqlSourceToDurableTargetEntity {
    id: u64,
    target_id: u64,
}

crate::test_entity! {
    ident = HeapSessionSqlSourceToDurableTargetEntity,
    entity_name = "HeapSessionSqlSourceToDurableTargetEntity",
    tag = EntityTag::new(0x1073),
    store = HeapSessionSqlStore,
    canister = SessionSqlCanister,
    key_type = u64,
    primary_key = [id],
    fields = [
        crate::test_field! { id: u64 => FieldKind::Nat64 },
        crate::test_field! { target_id: u64 => SESSION_SQL_DURABLE_TARGET_RELATION_KIND },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct HeapSessionSqlSourceToHeapTargetEntity {
    id: u64,
    target_id: u64,
}

crate::test_entity! {
    ident = HeapSessionSqlSourceToHeapTargetEntity,
    entity_name = "HeapSessionSqlSourceToHeapTargetEntity",
    tag = EntityTag::new(0x1072),
    store = HeapSessionSqlStore,
    canister = SessionSqlCanister,
    key_type = u64,
    primary_key = [id],
    fields = [
        crate::test_field! { id: u64 => FieldKind::Nat64 },
        crate::test_field! { target_id: u64 => SESSION_SQL_HEAP_TARGET_RELATION_KIND },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

crate::test_entity! {
    ident = CompositeIndexedSessionSqlEntity,
    entity_name = "CompositeIndexedSessionSqlEntity",
    tag = EntityTag::new(0x1037),
    store = IndexedSessionSqlStore,
    canister = SessionSqlCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { code: String => FieldKind::Text { max_len: None } },
        crate::test_field! { serial: u64 => FieldKind::Nat64 },
        crate::test_field! { note: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [&COMPOSITE_INDEXED_SESSION_SQL_INDEX_MODELS[0]],
    relations = [],
    entity_value = id_field(id),
}

crate::test_entity! {
    ident = BranchIndexedSessionSqlEntity,
    entity_name = "BranchIndexedSessionSqlEntity",
    tag = EntityTag::new(0x105E),
    store = IndexedSessionSqlStore,
    canister = SessionSqlCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { collection_id: String => FieldKind::Text { max_len: None } },
        crate::test_field! { stage: String => FieldKind::Text { max_len: None } },
        crate::test_field! { title: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [&BRANCH_INDEXED_SESSION_SQL_INDEX_MODELS[0]],
    relations = [],
    entity_value = id_field(id),
}

#[cfg(feature = "diagnostics")]
crate::test_entity! {
    ident = ExplicitPkSuffixIndexedSessionSqlEntity,
    entity_name = "ExplicitPkSuffixIndexedSessionSqlEntity",
    tag = EntityTag::new(0x104a),
    store = IndexedSessionSqlStore,
    canister = SessionSqlCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { bucket: u64 => FieldKind::Nat64 },
        crate::test_field! { label: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [&EXPLICIT_PK_SUFFIX_INDEXED_SESSION_SQL_INDEX_MODELS[0]],
    relations = [],
    entity_value = id_field(id),
}

crate::test_entity! {
    ident = FilteredIndexedSessionSqlEntity,
    entity_name = "FilteredIndexedSessionSqlEntity",
    tag = EntityTag::new(0x1039),
    store = IndexedSessionSqlStore,
    canister = SessionSqlCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { name: String => FieldKind::Text { max_len: None } },
        crate::test_field! { active: bool => FieldKind::Bool },
        crate::test_field! { tier: String => FieldKind::Text { max_len: None } },
        crate::test_field! { handle: String => FieldKind::Text { max_len: None } },
        crate::test_field! { age: u64 => FieldKind::Nat64 },
    ],
    indexes = [
        &FILTERED_INDEXED_SESSION_SQL_INDEX_MODELS[0],
        &FILTERED_INDEXED_SESSION_SQL_COMPOSITE_INDEX_MODELS[0],
        &FILTERED_INDEXED_SESSION_SQL_EXPRESSION_INDEX_MODELS[0],
        &FILTERED_INDEXED_SESSION_SQL_COMPOSITE_EXPRESSION_INDEX_MODELS[0],
    ],
    relations = [],
    entity_value = id_field(id),
}

crate::test_entity! {
    ident = ExpressionIndexedSessionSqlEntity,
    entity_name = "ExpressionIndexedSessionSqlEntity",
    tag = EntityTag::new(0x1038),
    store = IndexedSessionSqlStore,
    canister = SessionSqlCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { name: String => FieldKind::Text { max_len: None } },
        crate::test_field! { age: u64 => FieldKind::Nat64 },
    ],
    indexes = [&EXPRESSION_INDEXED_SESSION_SQL_INDEX_MODELS[0]],
    relations = [],
    entity_value = id_field(id),
}

crate::test_entity! {
    ident = SessionAggregateEntity,
    entity_name = "SessionAggregateEntity",
    tag = EntityTag::new(0x1034),
    store = SessionSqlStore,
    canister = SessionSqlCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { group: u64 => FieldKind::Nat64 },
        crate::test_field! { rank: u64 => FieldKind::Nat64 },
        crate::test_field! { label: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

crate::test_entity! {
    ident = SessionExplainEntity,
    entity_name = "SessionExplainEntity",
    tag = EntityTag::new(0x1035),
    store = IndexedSessionSqlStore,
    canister = SessionSqlCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { group: u64 => FieldKind::Nat64 },
        crate::test_field! { rank: u64 => FieldKind::Nat64 },
        crate::test_field! { label: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [&SESSION_EXPLAIN_INDEX_MODELS[0]],
    relations = [],
    entity_value = id_field(id),
}

crate::test_entity! {
    ident = SessionTemporalEntity,
    entity_name = "SessionTemporalEntity",
    tag = EntityTag::new(0x1036),
    store = SessionSqlStore,
    canister = SessionSqlCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { occurred_on: Date => FieldKind::Date },
        crate::test_field! { occurred_at: Timestamp => FieldKind::Timestamp },
        crate::test_field! { elapsed: Duration => FieldKind::Duration },
    ],
    indexes = [],
    relations = [],
    entity_value = id_field(id),
}

crate::test_entity! {
    ident = SessionDeterministicChoiceEntity,
    entity_name = "SessionDeterministicChoiceEntity",
    tag = EntityTag::new(0x1040),
    store = IndexedSessionSqlStore,
    canister = SessionSqlCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { tier: String => FieldKind::Text { max_len: None } },
        crate::test_field! { handle: String => FieldKind::Text { max_len: None } },
        crate::test_field! { label: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [
        &SESSION_DETERMINISTIC_CHOICE_INDEX_MODELS[0],
        &SESSION_DETERMINISTIC_CHOICE_INDEX_MODELS[1],
    ],
    relations = [],
    entity_value = id_field(id),
}

crate::test_entity! {
    ident = SessionDeterministicRangeEntity,
    entity_name = "SessionDeterministicRangeEntity",
    tag = EntityTag::new(0x1041),
    store = IndexedSessionSqlStore,
    canister = SessionSqlCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { tier: String => FieldKind::Text { max_len: None } },
        crate::test_field! { score: u64 => FieldKind::Nat64 },
        crate::test_field! { handle: String => FieldKind::Text { max_len: None } },
        crate::test_field! { label: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [
        &SESSION_DETERMINISTIC_RANGE_INDEX_MODELS[0],
        &SESSION_DETERMINISTIC_RANGE_INDEX_MODELS[1],
    ],
    relations = [],
    entity_value = id_field(id),
}

crate::test_entity! {
    ident = SessionRangeStrengthEntity,
    entity_name = "SessionRangeStrengthEntity",
    tag = EntityTag::new(0x104F),
    store = IndexedSessionSqlStore,
    canister = SessionSqlCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { tier: String => FieldKind::Text { max_len: None } },
        crate::test_field! { score: u64 => FieldKind::Nat64 },
        crate::test_field! { label: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [
        &SESSION_RANGE_STRENGTH_INDEX_MODELS[0],
        &SESSION_RANGE_STRENGTH_INDEX_MODELS[1],
    ],
    relations = [],
    entity_value = id_field(id),
}

crate::test_entity! {
    ident = SessionResidualRankingEntity,
    entity_name = "SessionResidualRankingEntity",
    tag = EntityTag::new(0x1050),
    store = IndexedSessionSqlStore,
    canister = SessionSqlCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { active: bool => FieldKind::Bool },
        crate::test_field! { archived: bool => FieldKind::Bool },
        crate::test_field! { tier: String => FieldKind::Text { max_len: None } },
        crate::test_field! { label: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [
        &SESSION_RESIDUAL_RANKING_INDEX_MODELS[0],
        &SESSION_RESIDUAL_RANKING_INDEX_MODELS[1],
    ],
    relations = [],
    entity_value = id_field(id),
}

crate::test_entity! {
    ident = SessionUniquePrefixOffsetEntity,
    entity_name = "SessionUniquePrefixOffsetEntity",
    tag = EntityTag::new(0x1043),
    store = IndexedSessionSqlStore,
    canister = SessionSqlCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { tier: String => FieldKind::Text { max_len: None } },
        crate::test_field! { handle: String => FieldKind::Text { max_len: None } },
        crate::test_field! { note: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [&SESSION_UNIQUE_PREFIX_OFFSET_INDEX_MODELS[0]],
    relations = [],
    entity_value = id_field(id),
}

crate::test_entity! {
    ident = SessionOrderOnlyChoiceEntity,
    entity_name = "SessionOrderOnlyChoiceEntity",
    tag = EntityTag::new(0x1042),
    store = IndexedSessionSqlStore,
    canister = SessionSqlCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { alpha: String => FieldKind::Text { max_len: None } },
        crate::test_field! { beta: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [
        &SESSION_ORDER_ONLY_CHOICE_INDEX_MODELS[0],
        &SESSION_ORDER_ONLY_CHOICE_INDEX_MODELS[1],
    ],
    relations = [],
    entity_value = id_field(id),
}

// Reset all session SQL fixture state between tests to preserve deterministic assertions.
fn reset_session_sql_store() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    ensure_recovered(&SESSION_SQL_DB).expect("write-side recovery should succeed");
    let session = sql_session();
    session.clear_query_plan_cache_for_tests();
    session.clear_sql_caches_for_tests();
    SESSION_SQL_DATA_STORE.with(|store| store.borrow_mut().clear());
    SESSION_SQL_INDEX_STORE.with(|store| {
        let mut store = store.borrow_mut();
        store.clear();
        store.mark_ready();
    });
    SESSION_SQL_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
    reset_commit_marker_test_journal_sequence();
}

fn sql_session() -> DbSession<SessionSqlCanister> {
    DbSession::new(SESSION_SQL_DB)
}

// Install a generated-compatible snapshot prefix that intentionally omits the
// non-key write fields. SQL session paths must reject this as unsupported
// schema evolution before they compile or stage row work against stale layout.
fn install_session_sql_write_old_accepted_schema_prefix() {
    let proposal =
        compiled_schema_proposal_for_model(<SessionSqlWriteEntity as EntityDeclaration>::MODEL);
    let expected = proposal.initial_persisted_schema_snapshot();
    let stored_prefix_row_layout = SchemaRowLayout::new(
        expected.row_layout().version(),
        vec![(FieldId::new(1), SchemaFieldSlot::new(0))],
    );
    let stored_prefix = PersistedSchemaSnapshot::new(
        expected.version(),
        expected.entity_path().to_string(),
        expected.entity_name().to_string(),
        expected.first_primary_key_field_id(),
        stored_prefix_row_layout,
        vec![expected.fields()[0].clone()],
    );
    SESSION_SQL_SCHEMA_STORE.with_borrow_mut(|store| {
        publish_test_accepted_schema_snapshot(
            store,
            SessionSqlWriteEntity::ENTITY_TAG,
            SessionSqlWriteEntity::PATH,
            SessionSqlStore::PATH,
            <SessionSqlWriteEntity as EntityDeclaration>::MODEL,
            stored_prefix,
        )
        .expect("unsupported but well-formed old SQL write schema should publish");
    });
    DbSession::<SessionSqlCanister>::clear_accepted_schema_query_cache_for_tests();
}

fn reset_indexed_session_sql_store() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    ensure_recovered(&INDEXED_SESSION_SQL_DB).expect("write-side recovery should succeed");
    let session = indexed_sql_session();
    session.clear_query_plan_cache_for_tests();
    session.clear_sql_caches_for_tests();
    INDEXED_SESSION_SQL_DATA_STORE.with(|store| store.borrow_mut().clear());
    INDEXED_SESSION_SQL_INDEX_STORE.with(|store| {
        let mut store = store.borrow_mut();
        store.clear();
        store.mark_ready();
    });
    INDEXED_SESSION_SQL_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
    reset_commit_marker_test_journal_sequence();
}

// Hide secondary indexes without clearing the underlying rows so tests can
// compare ready-index routes with forced full-scan fallbacks.
fn hide_indexed_session_indexes() {
    INDEXED_SESSION_SQL_DB
        .recovered_store(IndexedSessionSqlStore::PATH)
        .expect("indexed SQL store should recover")
        .mark_index_building();
    let session = indexed_sql_session();
    session.clear_query_plan_cache_for_tests();
    session.clear_sql_caches_for_tests();
}

fn indexed_sql_session() -> DbSession<SessionSqlCanister> {
    DbSession::new(INDEXED_SESSION_SQL_DB)
}

fn reset_heap_session_sql_store() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    HEAP_SESSION_SQL_DATA_STORE.with_borrow_mut(DataStore::clear);
    HEAP_SESSION_SQL_INDEX_STORE.with_borrow_mut(|store| {
        store.clear();
        store.mark_ready();
    });
    HEAP_SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    ensure_recovered(&HEAP_SESSION_SQL_DB).expect("heap write-side recovery should succeed");
    let session = heap_sql_session();
    session.clear_query_plan_cache_for_tests();
    session.clear_sql_caches_for_tests();
}

fn reinitialize_heap_session_sql_store() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    HEAP_SESSION_SQL_DATA_STORE.with_borrow_mut(|store| *store = DataStore::init_heap());
    HEAP_SESSION_SQL_INDEX_STORE.with_borrow_mut(|store| *store = IndexStore::init_heap());
    HEAP_SESSION_SQL_SCHEMA_STORE.with_borrow_mut(|store| *store = SchemaStore::init_heap());
    ensure_recovered(&HEAP_SESSION_SQL_DB)
        .expect("heap write-side recovery after reinit should succeed");
    let session = heap_sql_session();
    session.clear_query_plan_cache_for_tests();
    session.clear_sql_caches_for_tests();
}

fn heap_sql_session() -> DbSession<SessionSqlCanister> {
    DbSession::new(HEAP_SESSION_SQL_DB)
}

fn reset_journaled_session_sql_store() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    JOURNALED_SESSION_SQL_DATA_STORE.with_borrow_mut(DataStore::clear);
    JOURNALED_SESSION_SQL_INDEX_STORE.with_borrow_mut(|store| {
        store.clear();
        store.mark_ready();
    });
    JOURNALED_SESSION_SQL_SCHEMA_STORE.with_borrow_mut(SchemaStore::clear);
    JOURNALED_SESSION_SQL_JOURNAL_STORE.with_borrow_mut(JournalTailStore::clear);
    reset_commit_marker_test_journal_sequence();
    ensure_recovered(&JOURNALED_SESSION_SQL_DB)
        .expect("journaled write-side recovery boundary should initialize");
    let session = journaled_sql_session();
    session.clear_query_plan_cache_for_tests();
    session.clear_sql_caches_for_tests();
}

fn reinitialize_journaled_session_sql_store() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    JOURNALED_SESSION_SQL_DATA_STORE
        .with_borrow_mut(|store| *store = DataStore::init_journaled(test_memory(180)));
    JOURNALED_SESSION_SQL_INDEX_STORE
        .with_borrow_mut(|store| *store = IndexStore::init_journaled(test_memory(181)));
    JOURNALED_SESSION_SQL_SCHEMA_STORE.with_borrow_mut(|store| {
        *store =
            SchemaStore::init_journaled(JOURNALED_SESSION_SQL_SCHEMA_MEMORY.with(Clone::clone));
    });
    let marker = crate::db::commit::CommitMarker::new(Vec::new())
        .expect("empty recovery marker should build");
    crate::db::commit::begin_commit(marker)
        .expect("empty recovery marker should force journaled startup rebuild");
    ensure_recovered(&JOURNALED_SESSION_SQL_DB)
        .expect("journaled recovery should rebuild live projections from tail");
    let session = journaled_sql_session();
    session.clear_query_plan_cache_for_tests();
    session.clear_sql_caches_for_tests();
}

fn journaled_sql_session() -> DbSession<SessionSqlCanister> {
    DbSession::new(JOURNALED_SESSION_SQL_DB)
}

fn reset_mixed_heap_relation_stores() {
    reset_session_sql_store();
    reset_heap_session_sql_store();
    ensure_recovered(&MIXED_HEAP_RELATION_DB)
        .expect("mixed heap relation recovery boundary should initialize");
    let session = mixed_heap_relation_sql_session();
    session.clear_query_plan_cache_for_tests();
    session.clear_sql_caches_for_tests();
}

fn mixed_heap_relation_sql_session() -> DbSession<SessionSqlCanister> {
    DbSession::new(MIXED_HEAP_RELATION_DB)
}

fn reset_mixed_journaled_relation_stores() {
    reset_session_sql_store();
    reset_journaled_session_sql_store();
    ensure_recovered(&MIXED_JOURNALED_RELATION_DB)
        .expect("mixed journaled relation recovery boundary should initialize");
    let session = mixed_journaled_relation_sql_session();
    session.clear_query_plan_cache_for_tests();
    session.clear_sql_caches_for_tests();
}

fn mixed_journaled_relation_sql_session() -> DbSession<SessionSqlCanister> {
    DbSession::new(MIXED_JOURNALED_RELATION_DB)
}

// Resolve the indexed SQL store handle through the recovered DB boundary.
#[test]
fn session_select_one_returns_constant_without_execution_metrics() {
    let session = sql_session();
    let (value, events) = capture_session_metrics(|| session.select_one());

    assert_eq!(
        value,
        Value::Int64(1),
        "select_one should return constant 1"
    );
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
        vec!["PRIMARY KEY (id) [state=ready] [origin=generated]".to_string()],
        "entities without secondary indexes should only report primary key metadata",
    );
    assert_eq!(
        indexed_session.show_indexes::<IndexedSessionSqlEntity>(),
        vec![
            "PRIMARY KEY (id) [state=ready] [origin=generated]".to_string(),
            "INDEX name (name) [state=ready] [origin=generated]".to_string(),
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
            "SHOW INDEXES FROM IndexedSessionSqlEntity",
        )
        .expect("SHOW INDEXES FROM should succeed for ready index"),
        vec![
            "PRIMARY KEY (id) [state=ready] [origin=generated]".to_string(),
            "INDEX name (name) [state=ready] [origin=generated]".to_string(),
        ],
        "SHOW INDEXES FROM should expose the default ready lifecycle state on the runtime metadata surface",
    );

    INDEXED_SESSION_SQL_DB
        .recovered_store(IndexedSessionSqlStore::PATH)
        .expect("indexed SQL store should recover")
        .mark_index_building();
    assert_eq!(
        statement_show_indexes_sql::<IndexedSessionSqlEntity>(
            &session,
            "SHOW INDEXES FROM IndexedSessionSqlEntity",
        )
        .expect("SHOW INDEXES FROM should succeed for building index"),
        vec![
            "PRIMARY KEY (id) [state=building] [origin=generated]".to_string(),
            "INDEX name (name) [state=building] [origin=generated]".to_string(),
        ],
        "SHOW INDEXES FROM should expose Building while planner visibility removes the index from covering routes",
    );

    INDEXED_SESSION_SQL_DB
        .recovered_store(IndexedSessionSqlStore::PATH)
        .expect("indexed SQL store should recover")
        .with_index_mut(IndexStore::mark_dropping);
    assert_eq!(
        statement_show_indexes_sql::<IndexedSessionSqlEntity>(
            &session,
            "SHOW INDEXES FROM IndexedSessionSqlEntity",
        )
        .expect("SHOW INDEXES FROM should succeed for dropping index"),
        vec![
            "PRIMARY KEY (id) [state=dropping] [origin=generated]".to_string(),
            "INDEX name (name) [state=dropping] [origin=generated]".to_string(),
        ],
        "SHOW INDEXES FROM should expose Dropping while planner visibility removes the index from covering routes",
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
        field.name() == "age"
            && field.kind() == "nat64"
            && field.queryable()
            && !field.primary_key()
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
            origin: "generated".to_string(),
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
    assert_eq!(
        trace.reuse().artifact_class(),
        crate::db::TraceReuseArtifactClass::SharedPreparedQueryPlan,
        "trace reuse surface should report the shipped shared prepared-plan artifact class",
    );
    assert!(
        !trace.reuse().is_hit(),
        "first trace build should miss shared prepared-plan reuse before the cache is warm",
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

fn unsupported_sql_statement_query_error(_message: &'static str) -> QueryError {
    QueryError::execute(crate::error::InternalError::classified(
        ErrorClass::Unsupported,
        ErrorOrigin::Query,
    ))
}

fn assert_numeric_overflow_query_error(err: QueryError) {
    let QueryError::Execute(execute) = err else {
        panic!("numeric overflow should be reported as a query execution error");
    };
    let internal = execute.as_internal();

    assert_eq!(
        internal.class(),
        ErrorClass::Unsupported,
        "numeric overflow class drifted: message={} detail={:?}",
        internal.message(),
        internal.detail(),
    );
    assert_eq!(internal.origin(), ErrorOrigin::Query);
    assert!(
        matches!(
            internal.detail(),
            Some(ErrorDetail::Query(QueryErrorDetail::NumericOverflow))
        ),
        "numeric overflow should preserve structured query error detail"
    );
}

fn assert_numeric_not_representable_query_error(err: QueryError) {
    let QueryError::Execute(execute) = err else {
        panic!("non-representable numeric result should be reported as a query execution error");
    };
    let internal = execute.as_internal();

    assert_eq!(
        internal.class(),
        ErrorClass::Unsupported,
        "numeric representation class drifted: message={} detail={:?}",
        internal.message(),
        internal.detail(),
    );
    assert_eq!(internal.origin(), ErrorOrigin::Query);
    assert!(
        matches!(
            internal.detail(),
            Some(ErrorDetail::Query(
                QueryErrorDetail::NumericNotRepresentable
            ))
        ),
        "numeric representation failures should preserve structured query error detail"
    );
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
    E: PersistedRow<Canister = SessionSqlCanister>,
{
    session.execute_sql_statement_inner::<E>(sql)
}

///
/// SqlStatementPayloadKind
///
/// One test-only statement-result payload selector used by the session test
/// boundary to keep SQL result extraction on one shared path.
///

#[derive(Clone, Copy)]
enum SqlStatementPayloadKind {
    ProjectionColumns,
    ProjectionRows,
    #[cfg(feature = "sql-explain")]
    Explain,
    Describe,
    ShowIndexes,
    ShowColumns,
    ShowEntities,
    ShowStores,
    ShowMemory,
}

impl SqlStatementPayloadKind {
    /// Return the stable unsupported-surface message for one extraction shape.
    #[must_use]
    const fn unsupported_message(self) -> &'static str {
        match self {
            Self::ProjectionColumns => {
                "projection column SQL only supports row-producing SQL statements"
            }
            Self::ProjectionRows => {
                "projection row SQL only supports value-row SQL projection payloads"
            }
            #[cfg(feature = "sql-explain")]
            Self::Explain => "EXPLAIN SQL requires an EXPLAIN statement",
            Self::Describe => "DESCRIBE SQL requires a DESCRIBE statement",
            Self::ShowIndexes => "SHOW INDEXES FROM SQL requires a SHOW INDEXES FROM statement",
            Self::ShowColumns => "SHOW COLUMNS SQL requires a SHOW COLUMNS statement",
            Self::ShowEntities => "SHOW ENTITIES SQL requires a SHOW ENTITIES statement",
            Self::ShowStores => "SHOW STORES SQL requires a SHOW STORES statement",
            Self::ShowMemory => "SHOW MEMORY SQL requires a SHOW MEMORY statement",
        }
    }
}

// Route one already-executed SQL statement result through one test-only payload
// extractor so the session test boundary stops repeating full enum fanout for
// every projection, explain, and schema-surface helper.
fn extract_sql_statement_payload<T>(
    result: SqlStatementResult,
    kind: SqlStatementPayloadKind,
    extract: impl FnOnce(SqlStatementResult) -> Option<T>,
) -> Result<T, QueryError> {
    extract(result).ok_or_else(|| unsupported_sql_statement_query_error(kind.unsupported_message()))
}

fn statement_projection_columns<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> Result<Vec<String>, QueryError>
where
    E: PersistedRow<Canister = SessionSqlCanister>,
{
    extract_sql_statement_payload(
        execute_sql_statement_for_tests::<E>(session, sql)?,
        SqlStatementPayloadKind::ProjectionColumns,
        |result| match result {
            SqlStatementResult::Projection { columns, .. }
            | SqlStatementResult::Grouped { columns, .. } => Some(columns),
            _ => None,
        },
    )
}

fn statement_projection_rows<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> Result<Vec<Vec<Value>>, QueryError>
where
    E: PersistedRow<Canister = SessionSqlCanister>,
{
    extract_sql_statement_payload(
        execute_sql_statement_for_tests::<E>(session, sql)?,
        SqlStatementPayloadKind::ProjectionRows,
        |result| match result {
            SqlStatementResult::Projection { rows, .. } => Some(
                rows.into_iter()
                    .map(|row| row.into_iter().map(runtime_output).collect())
                    .collect(),
            ),
            _ => None,
        },
    )
}

fn output(value: Value) -> OutputValue {
    OutputValue::from(value)
}

fn outputs(values: Vec<Value>) -> Vec<OutputValue> {
    values.into_iter().map(output).collect()
}

fn outputs_with_ids<E>(values: Vec<(Id<E>, Value)>) -> Vec<(Id<E>, OutputValue)>
where
    E: PersistedRow,
{
    values
        .into_iter()
        .map(|(id, value)| (id, output(value)))
        .collect()
}

fn runtime_output(value: OutputValue) -> Value {
    match value {
        OutputValue::Account(value) => Value::Account(value),
        OutputValue::Blob(value) => Value::Blob(value),
        OutputValue::Bool(value) => Value::Bool(value),
        OutputValue::Date(value) => Value::Date(value),
        OutputValue::Decimal(value) => Value::Decimal(value),
        OutputValue::Duration(value) => Value::Duration(value),
        OutputValue::Enum(_) => panic!("test output conversion requires accepted catalog IDs"),
        OutputValue::Float32(value) => Value::Float32(value),
        OutputValue::Float64(value) => Value::Float64(value),
        OutputValue::Int64(value) => Value::Int64(value),
        OutputValue::Int128(value) => Value::Int128(value),
        OutputValue::IntBig(value) => Value::IntBig(value),
        OutputValue::List(values) => Value::List(values.into_iter().map(runtime_output).collect()),
        OutputValue::Map(entries) => Value::Map(
            entries
                .into_iter()
                .map(|(key, value)| (runtime_output(key), runtime_output(value)))
                .collect(),
        ),
        OutputValue::Null => Value::Null,
        OutputValue::Principal(value) => Value::Principal(value),
        OutputValue::Subaccount(value) => Value::Subaccount(value),
        OutputValue::Text(value) => Value::Text(value),
        OutputValue::Timestamp(value) => Value::Timestamp(value),
        OutputValue::Nat64(value) => Value::Nat64(value),
        OutputValue::Nat128(value) => Value::Nat128(value),
        OutputValue::NatBig(value) => Value::NatBig(value),
        OutputValue::Ulid(value) => Value::Ulid(value),
        OutputValue::Unit => Value::Unit,
    }
}

fn runtime_outputs(values: &[OutputValue]) -> Vec<Value> {
    values.iter().cloned().map(runtime_output).collect()
}

// Execute one projection SQL statement and require exactly one scalar output
// cell from the first row so scalar aggregate tests can share one extraction
// helper.
fn statement_projection_scalar_value<Entity>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> Result<Value, QueryError>
where
    Entity: PersistedRow<Canister = SessionSqlCanister>,
{
    let unsupported = || {
        unsupported_sql_statement_query_error(
            "scalar projection SQL requires one row with exactly one scalar value",
        )
    };
    let mut row = statement_projection_rows::<Entity>(session, sql)?
        .into_iter()
        .next()
        .ok_or_else(unsupported)?;
    if row.len() != 1 {
        return Err(unsupported());
    }
    row.pop().ok_or_else(unsupported)
}

// Execute one scalar projection SQL statement and assert it emits the expected
// public scalar value.
fn assert_session_sql_scalar_value<Entity>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    expected: Value,
    context: &str,
) where
    Entity: PersistedRow<Canister = SessionSqlCanister>,
{
    let actual = statement_projection_scalar_value::<Entity>(session, sql)
        .unwrap_or_else(|err| panic!("{context} scalar SQL should execute: {err}"));

    assert_eq!(
        actual, expected,
        "{context} should preserve the expected scalar value",
    );
}

#[cfg(feature = "sql-explain")]
fn statement_explain_sql<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> Result<String, QueryError>
where
    E: PersistedRow<Canister = SessionSqlCanister>,
{
    extract_sql_statement_payload(
        execute_sql_statement_for_tests::<E>(session, sql)?,
        SqlStatementPayloadKind::Explain,
        |result| match result {
            SqlStatementResult::Explain(explain) => Some(explain),
            _ => None,
        },
    )
}

#[cfg(not(feature = "sql-explain"))]
fn statement_explain_sql<E>(
    _session: &DbSession<SessionSqlCanister>,
    _sql: &str,
) -> Result<String, QueryError>
where
    E: PersistedRow<Canister = SessionSqlCanister>,
{
    let _ = core::marker::PhantomData::<E>;

    Err(QueryError::unsupported_query())
}

// Execute one EXPLAIN SQL statement and assert the public surface keeps the
// requested stable tokens.
fn assert_session_sql_explain_tokens<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
    tokens: &[&str],
    require_json_object: bool,
    context: &str,
) where
    E: PersistedRow<Canister = SessionSqlCanister>,
{
    let explain = statement_explain_sql::<E>(session, sql)
        .unwrap_or_else(|err| panic!("{context} explain SQL should succeed: {err}"));

    if require_json_object {
        assert!(
            explain.starts_with('{') && explain.ends_with('}'),
            "{context} should render one JSON object payload",
        );
    }

    assert_explain_contains_tokens(explain.as_str(), tokens, context);
}

// Execute one aliased-vs-canonical SQL pair through the provided session test
// runner and assert both spellings preserve the same public output.
fn assert_session_sql_alias_matches_canonical<T>(
    session: &DbSession<SessionSqlCanister>,
    runner: impl Fn(&DbSession<SessionSqlCanister>, &str) -> Result<T, QueryError>,
    aliased_sql: &str,
    canonical_sql: &str,
    context: &str,
) where
    T: Debug + PartialEq,
{
    let aliased = runner(session, aliased_sql)
        .unwrap_or_else(|err| panic!("{context} aliased SQL should succeed: {err:?}"));
    let canonical = runner(session, canonical_sql)
        .unwrap_or_else(|err| panic!("{context} canonical SQL should succeed: {err:?}"));

    assert_eq!(
        aliased, canonical,
        "{context} should normalize to the same public output",
    );
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
    E: PersistedRow<Canister = SessionSqlCanister>,
{
    extract_sql_statement_payload(
        execute_sql_statement_for_tests::<E>(session, sql)?,
        SqlStatementPayloadKind::Describe,
        |result| match result {
            SqlStatementResult::Describe(description) => Some(description),
            _ => None,
        },
    )
}

fn statement_show_indexes_sql<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> Result<Vec<String>, QueryError>
where
    E: PersistedRow<Canister = SessionSqlCanister>,
{
    extract_sql_statement_payload(
        execute_sql_statement_for_tests::<E>(session, sql)?,
        SqlStatementPayloadKind::ShowIndexes,
        |result| match result {
            SqlStatementResult::ShowIndexes(indexes) => Some(indexes),
            _ => None,
        },
    )
}

fn statement_show_columns_sql<E>(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> Result<Vec<EntityFieldDescription>, QueryError>
where
    E: PersistedRow<Canister = SessionSqlCanister>,
{
    extract_sql_statement_payload(
        execute_sql_statement_for_tests::<E>(session, sql)?,
        SqlStatementPayloadKind::ShowColumns,
        |result| match result {
            SqlStatementResult::ShowColumns(columns) => Some(columns),
            _ => None,
        },
    )
}

fn statement_show_entities_sql(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> Result<Vec<EntityCatalogDescription>, QueryError> {
    extract_sql_statement_payload(
        execute_sql_statement_for_tests::<SessionSqlEntity>(session, sql)?,
        SqlStatementPayloadKind::ShowEntities,
        |result| match result {
            SqlStatementResult::ShowEntities { entities, .. } => Some(entities),
            _ => None,
        },
    )
}

fn statement_show_stores_sql(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> Result<Vec<StoreCatalogDescription>, QueryError> {
    extract_sql_statement_payload(
        execute_sql_statement_for_tests::<SessionSqlEntity>(session, sql)?,
        SqlStatementPayloadKind::ShowStores,
        |result| match result {
            SqlStatementResult::ShowStores { stores, .. } => Some(stores),
            _ => None,
        },
    )
}

fn statement_show_memory_sql(
    session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> Result<Vec<MemoryCatalogDescription>, QueryError> {
    extract_sql_statement_payload(
        execute_sql_statement_for_tests::<SessionSqlEntity>(session, sql)?,
        SqlStatementPayloadKind::ShowMemory,
        |result| match result {
            SqlStatementResult::ShowMemory(memory) => Some(memory),
            _ => None,
        },
    )
}

// Insert one fixture row stream through one shared session-owner helper so the
// deterministic SQL fixture seeds do not all repeat the same insert loop and
// panic wiring.
fn insert_session_fixture_rows<E, R>(
    session: &DbSession<SessionSqlCanister>,
    rows: impl IntoIterator<Item = R>,
    mut build: impl FnMut(R) -> E,
    context: &str,
) where
    E: PersistedRow<Canister = SessionSqlCanister>,
{
    for row in rows {
        session
            .insert(build(row))
            .unwrap_or_else(|err| panic!("{context} fixture insert should succeed: {err}"));
    }
}

// Seed one deterministic SQL fixture dataset used by matrix tests.
fn seed_session_sql_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(&'static str, u64)],
) {
    insert_session_fixture_rows(
        session,
        rows.iter().copied(),
        |(name, age)| SessionSqlEntity {
            id: Ulid::generate(),
            name: name.to_string(),
            age,
        },
        "seed",
    );
}

// Seed one deterministic structured SQL fixture dataset used by FieldPath
// predicate execution tests.
fn seed_session_sql_field_path_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(&'static str, SessionSqlFieldPathProfile)],
) {
    insert_session_fixture_rows(
        session,
        rows.iter().cloned(),
        |(name, profile)| SessionSqlFieldPathEntity {
            id: Ulid::generate(),
            name: name.to_string(),
            profile,
        },
        "field path seed",
    );
}

// Seed one deterministic typed-record SQL fixture dataset used by FieldPath
// projection execution tests.
fn seed_session_sql_record_field_path_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(&'static str, i64, &'static str)],
) {
    insert_session_fixture_rows(
        session,
        rows.iter().copied(),
        |(name, rank, nickname)| SessionSqlRecordFieldPathEntity {
            id: Ulid::generate(),
            name: name.to_string(),
            profile: SessionSqlProfileRecord {
                rank,
                nickname: nickname.to_string(),
            },
        },
        "record field path seed",
    );
}

// Seed one deterministic nullable SQL fixture dataset used by NULL-semantics
// execution tests on persisted nullable text fields.
fn seed_nullable_session_sql_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(&'static str, Option<&'static str>)],
) {
    insert_session_fixture_rows(
        session,
        rows.iter().copied(),
        |(name, nickname)| SessionNullableSqlEntity {
            id: Ulid::generate(),
            name: name.to_string(),
            nickname: nickname.map(str::to_string),
        },
        "nullable seed",
    );
}

// Seed one deterministic float-backed SQL fixture dataset used by ordered
// numeric literal widening tests on float fields.
fn seed_session_sql_float_compare_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(&'static str, f64)],
) {
    insert_session_fixture_rows(
        session,
        rows.iter().copied(),
        |(label, dodge_chance)| SessionSqlFloatCompareEntity {
            id: Ulid::generate(),
            label: label.to_string(),
            dodge_chance: Float64::try_new(dodge_chance)
                .expect("float compare seed should stay finite"),
        },
        "float compare seed",
    );
}

// Seed one deterministic indexed SQL fixture dataset used by text-prefix tests.
fn seed_indexed_session_sql_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(&'static str, u64)],
) {
    insert_session_fixture_rows(
        session,
        rows.iter().copied(),
        |(name, age)| IndexedSessionSqlEntity {
            id: Ulid::generate(),
            name: name.to_string(),
            age,
        },
        "indexed seed",
    );
}

// Seed one deterministic unique-prefix dataset used by offset-aware ordered
// secondary-prefix session tests.
fn seed_unique_prefix_offset_session_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(u128, &'static str, &'static str, &'static str)],
) {
    insert_session_fixture_rows(
        session,
        rows.iter().copied(),
        |(id, tier, handle, note)| SessionUniquePrefixOffsetEntity {
            id: Ulid::from_u128(id),
            tier: tier.to_string(),
            handle: handle.to_string(),
            note: note.to_string(),
        },
        "unique-prefix offset seed",
    );
}

// Seed one deterministic single-field order-only dataset used by offset-aware
// fallback index-order session tests.
fn seed_order_only_choice_session_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(u128, &'static str, &'static str)],
) {
    insert_session_fixture_rows(
        session,
        rows.iter().copied(),
        |(id, alpha, beta)| SessionOrderOnlyChoiceEntity {
            id: Ulid::from_u128(id),
            alpha: alpha.to_string(),
            beta: beta.to_string(),
        },
        "order-only choice seed",
    );
}

// Seed one deterministic filtered-indexed SQL fixture dataset used by guarded
// order-only fallback tests.
fn seed_filtered_indexed_session_sql_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(u128, &'static str, bool, u64)],
) {
    insert_session_fixture_rows(
        session,
        rows.iter().copied(),
        |(id, name, active, age)| FilteredIndexedSessionSqlEntity {
            id: Ulid::from_u128(id),
            name: name.to_string(),
            active,
            tier: "standard".to_string(),
            handle: format!("handle-{name}"),
            age,
        },
        "filtered indexed seed",
    );
}

// Seed one deterministic filtered composite-indexed SQL fixture dataset used by
// equality-prefix plus bounded-text window tests.
fn seed_filtered_composite_indexed_session_sql_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(u128, &'static str, bool, &'static str, &'static str, u64)],
) {
    insert_session_fixture_rows(
        session,
        rows.iter().copied(),
        |(id, name, active, tier, handle, age)| FilteredIndexedSessionSqlEntity {
            id: Ulid::from_u128(id),
            name: name.to_string(),
            active,
            tier: tier.to_string(),
            handle: handle.to_string(),
            age,
        },
        "filtered composite indexed seed",
    );
}

// Seed the canonical mixed-case filtered expression fixture used by the
// guarded `LOWER(handle)` query tests.
// Inspect the raw index-range scan chosen by the filtered expression order-only
// SQL route so tests can assert both index isolation and scan order directly.
fn inspect_filtered_expression_order_only_raw_scan(
    session: &DbSession<SessionSqlCanister>,
) -> (Vec<(PrimaryKeyComponent, PrimaryKeyComponent)>, Vec<Ulid>) {
    let plan = lower_select_query_for_tests::<FilteredIndexedSessionSqlEntity>(&session,
            "SELECT id, handle FROM FilteredIndexedSessionSqlEntity WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        )
        .expect("filtered expression-order SQL query should lower")
        .access_plan_for_test()
        .expect("filtered expression-order SQL query should plan");
    let lowered_access = lower_access(FilteredIndexedSessionSqlEntity::ENTITY_TAG, &plan.access)
        .expect("filtered expression-order access plan should lower to one raw index range");
    let [spec] = lowered_access.index_range_specs() else {
        panic!("filtered expression-order access plan should use exactly one index-range spec");
    };
    let store = INDEXED_SESSION_SQL_DB
        .recovered_store(IndexedSessionSqlStore::PATH)
        .expect("filtered expression indexed store should recover");

    // Inspect stored entries that fall inside the lowered raw envelope.
    let entries_in_range = store.with_index(|index_store| {
        let mut entries = Vec::new();
        let _: Result<(), std::convert::Infallible> =
            index_store.visit_entries(|raw_key, raw_entry| {
                if !key_within_envelope(raw_key, spec.lower(), spec.upper()) {
                    return Ok(IndexStoreVisit::Continue);
                }

                let decoded_key =
                    IndexKey::try_from_raw(raw_key).expect("filtered expression test key");
                let decoded_id = raw_entry
                    .decode_row_identity(raw_key)
                    .expect("filtered expression test entry")
                    .primary_key_value()
                    .scalar_component()
                    .expect("filtered expression test scalar entry");

                entries.push((
                    decoded_key
                        .primary_key_value()
                        .expect("primary-key value")
                        .scalar_component()
                        .expect("filtered expression decoded key should be scalar"),
                    decoded_id,
                ));
                Ok(IndexStoreVisit::Continue)
            });
        entries
    });

    // Then inspect the actual scan order produced by the shared raw range resolver.
    let keys = store.with_index(|index_store| {
        let mut keys = Vec::new();
        index_store
            .visit_raw_entries_in_range(
                (spec.lower(), spec.upper()),
                Direction::Asc,
                |raw_key, raw_entry| {
                    let entry = raw_entry
                        .decode_row_identity(raw_key)
                        .expect("filtered expression index range scan entry");
                    keys.push(DecodedDataStoreKey::new(
                        FilteredIndexedSessionSqlEntity::ENTITY_TAG,
                        entry.primary_key_value(),
                    ));
                    if keys.len() == 4 {
                        return Ok(true);
                    }

                    Ok(false)
                },
            )
            .expect("filtered expression index range scan should succeed");

        keys
    });
    let scanned_ids = keys
        .into_iter()
        .map(
            |key: DecodedDataStoreKey| match key.primary_key_value().scalar_component() {
                Some(PrimaryKeyComponent::Ulid(id)) => id,
                other => panic!(
                    "filtered expression fixture keys should stay on ULID primary keys: {other:?}"
                ),
            },
        )
        .collect::<Vec<_>>();

    (entries_in_range, scanned_ids)
}

// Seed one deterministic aggregate fixture dataset used by session aggregate tests.
fn seed_session_aggregate_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(u128, u64, u64)],
) {
    insert_session_fixture_rows(
        session,
        rows.iter().copied(),
        |(id, group, rank)| SessionAggregateEntity {
            id: Ulid::from_u128(id),
            group,
            rank,
            label: format!("group-{group}-rank-{rank}"),
        },
        "aggregate seed",
    );
}

fn seed_session_explain_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(u128, u64, u64)],
) {
    insert_session_fixture_rows(
        session,
        rows.iter().copied(),
        |(id, group, rank)| SessionExplainEntity {
            id: Ulid::from_u128(id),
            group,
            rank,
            label: format!("g{group}-r{rank}"),
        },
        "session explain",
    );
}

fn seed_composite_indexed_session_sql_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(u128, &str, u64)],
) {
    insert_session_fixture_rows(
        session,
        rows.iter().copied(),
        |(id, code, serial)| CompositeIndexedSessionSqlEntity {
            id: Ulid::from_u128(id),
            code: code.to_string(),
            serial,
            note: format!("note-{code}-{serial}"),
        },
        "composite indexed SQL",
    );
}

fn seed_branch_indexed_session_sql_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(u128, &str, &str, &str)],
) {
    insert_session_fixture_rows(
        session,
        rows.iter().copied(),
        |(id, collection_id, stage, title)| BranchIndexedSessionSqlEntity {
            id: Ulid::from_u128(id),
            collection_id: collection_id.to_string(),
            stage: stage.to_string(),
            title: title.to_string(),
        },
        "branch indexed SQL",
    );
}

#[cfg(feature = "diagnostics")]
fn seed_explicit_pk_suffix_indexed_session_sql_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(u128, u64, &str)],
) {
    insert_session_fixture_rows(
        session,
        rows.iter().copied(),
        |(id, bucket, label)| ExplicitPkSuffixIndexedSessionSqlEntity {
            id: Ulid::from_u128(id),
            bucket,
            label: label.to_string(),
        },
        "explicit primary-key suffix indexed SQL",
    );
}

fn seed_expression_indexed_session_sql_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(u128, &str, u64)],
) {
    insert_session_fixture_rows(
        session,
        rows.iter().copied(),
        |(id, name, age)| ExpressionIndexedSessionSqlEntity {
            id: Ulid::from_u128(id),
            name: name.to_string(),
            age,
        },
        "expression indexed SQL",
    );
}

fn seed_session_temporal_entities(
    session: &DbSession<SessionSqlCanister>,
    rows: &[(u128, Date, Timestamp, Duration)],
) {
    insert_session_fixture_rows(
        session,
        rows.iter().copied(),
        |(id, occurred_on, occurred_at, elapsed)| SessionTemporalEntity {
            id: Ulid::from_u128(id),
            occurred_on,
            occurred_at,
            elapsed,
        },
        "session temporal",
    );
}

fn session_aggregate_group_filter(group: u64) -> FilterExpr {
    FilterExpr::eq("group", group)
}

fn session_aggregate_values_by_rank(
    response: &EntityResponse<SessionAggregateEntity>,
) -> Vec<OutputValue> {
    response
        .iter()
        .map(|row| output(Value::Nat64(row.entity_ref().rank)))
        .collect()
}

fn session_aggregate_values_by_rank_with_ids(
    response: &EntityResponse<SessionAggregateEntity>,
) -> Vec<(Ulid, OutputValue)> {
    response
        .iter()
        .map(|row| (row.id().key(), output(Value::Nat64(row.entity_ref().rank))))
        .collect()
}

fn session_aggregate_first_value_by_rank(
    response: &EntityResponse<SessionAggregateEntity>,
) -> Option<OutputValue> {
    response
        .iter()
        .next()
        .map(|row| output(Value::Nat64(row.entity_ref().rank)))
}

fn session_aggregate_last_value_by_rank(
    response: &EntityResponse<SessionAggregateEntity>,
) -> Option<OutputValue> {
    response
        .iter()
        .last()
        .map(|row| output(Value::Nat64(row.entity_ref().rank)))
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
#[cfg(feature = "sql-explain")]
fn store_backed_execution_descriptor_for_sql<E>(
    _session: &DbSession<SessionSqlCanister>,
    sql: &str,
) -> ExplainExecutionNodeDescriptor
where
    E: PersistedRow<Canister = SessionSqlCanister>,
{
    let statement = crate::db::session::sql::parse_sql_statement(sql)
        .expect("store-backed execution descriptor sql should parse");
    let lowered = lower_sql_command_from_prepared_statement_for_model_only(
        prepare_sql_statement(&statement, E::MODEL.name())
            .expect("store-backed execution descriptor sql should prepare"),
        E::MODEL,
    )
    .expect("store-backed execution descriptor sql should lower");
    let LoweredSqlQuery::Select(select) = lowered
        .query()
        .cloned()
        .expect("store-backed execution descriptor should lower one query shape")
    else {
        panic!("store-backed execution descriptor helper only supports SELECT");
    };
    let structural = apply_lowered_select_shape_for_model_only(
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

fn capture_session_metrics<R>(run: impl FnOnce() -> R) -> (R, Vec<MetricsEvent>) {
    let sink = Rc::new(SessionMetricsCaptureSink::default());
    let output = with_shared_metrics_sink(sink.clone(), run);
    let sink = Rc::try_unwrap(sink)
        .unwrap_or_else(|_| panic!("session metrics sink should have one owner after capture"));

    (output, sink.into_events())
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
    let (output, events) = capture_session_metrics(run);
    let rows_scanned = rows_scanned_for_entity(&events, entity_path);

    (output, rows_scanned)
}

fn session_aggregate_raw_row(id: Ulid) -> crate::db::data::RawRow {
    let raw_key = DecodedDataStoreKey::try_new::<SessionAggregateEntity>(id)
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
            "group" => Value::Nat64(row.entity_ref().group),
            "rank" => Value::Nat64(row.entity_ref().rank),
            "label" => Value::Text(row.entity_ref().label.clone()),
            other => panic!("session aggregate field should resolve: {other}"),
        };
        let value_len = encode_structural_value_storage_bytes(&value)
            .expect("session aggregate field value should encode")
            .len();

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
/// without depending on executor aggregate harness internals.
///

#[derive(Clone, Debug, Eq, PartialEq)]
enum SessionAggregateResult {
    Ids(Vec<Ulid>),
    Values(Vec<OutputValue>),
    ValuesWithIds(Vec<(Ulid, OutputValue)>),
}

fn run_session_aggregate_projection_terminal(
    session: &DbSession<SessionSqlCanister>,
    terminal: SessionAggregateProjectionTerminal,
) -> Result<SessionAggregateResult, QueryError> {
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .trusted_read_unchecked()
            .filter(session_aggregate_group_filter(7))
            .order_term(crate::db::desc("id"))
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
            .trusted_read_unchecked()
            .filter(session_aggregate_group_filter(7))
            .order_term(crate::db::desc("id"))
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
            "explain matrix case missing token `{token}`: {context}: {explain}",
        );
    }
}

// Assert query-surface cursor errors remain wrapped under QueryError::Plan(PlanError::Cursor).
fn assert_query_error_is_cursor_plan(
    err: QueryError,
    predicate: impl Fn(&CursorPlanError) -> bool,
) {
    let diagnostic = err.diagnostic();
    assert_eq!(
        diagnostic.code(),
        icydb_diagnostic_code::DiagnosticCode::QueryInvalidContinuationCursor,
    );
    assert_eq!(
        diagnostic.origin(),
        icydb_diagnostic_code::ErrorOrigin::Cursor,
    );
    std::assert_matches!(
        err,
        QueryError::Plan(plan_err)
            if matches!(
                plan_err.as_ref(),
                PlanError::Cursor(inner) if predicate(inner.as_ref())
            )
    );
}

// Assert both session conversion paths preserve the same cursor-plan variant payload.
fn assert_cursor_mapping_parity(
    build: impl Fn() -> CursorPlanError,
    predicate: impl Fn(&CursorPlanError) -> bool,
) {
    let mapped_via_executor =
        query::query_error_from_executor_plan_error(ExecutorPlanError::from(build()));
    assert_query_error_is_cursor_plan(mapped_via_executor, &predicate);

    let mapped_via_plan = QueryError::from(PlanError::from(build()));
    assert_query_error_is_cursor_plan(mapped_via_plan, &predicate);
}

// Assert SQL parser unsupported-feature codes remain preserved through
// query-facing execution error detail payloads.
fn assert_sql_unsupported_feature_detail(err: QueryError, expected_feature: SqlFeatureCode) {
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
        "unsupported SQL feature detail code should be preserved",
    );
}

// Assert SQL lowering compact codes remain preserved through query-facing
// execution error detail payloads.
fn assert_sql_lowering_detail(err: QueryError, expected_reason: SqlLoweringCode) {
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
            Some(ErrorDetail::Query(QueryErrorDetail::SqlLowering { reason }))
                if *reason == expected_reason
        ),
        "SQL lowering detail code should be preserved",
    );
}

// Assert SQL write boundary codes remain preserved through query-facing
// execution error detail payloads.
fn assert_sql_write_boundary_detail(err: QueryError, expected_boundary: SqlWriteBoundaryCode) {
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
            Some(ErrorDetail::Query(QueryErrorDetail::SqlWriteBoundary { boundary }))
                if *boundary == expected_boundary
        ),
        "SQL write boundary detail code should be preserved: expected {expected_boundary:?}, got {:?}",
        internal.detail(),
    );
}

// Assert one query error is reported through the unsupported execution boundary
// with stable compact diagnostic identity instead of message text.
fn assert_unsupported_query_execution_diagnostic(err: QueryError, context: &str) {
    let QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(internal)) =
        err
    else {
        panic!("{context}: expected unsupported query execution error");
    };

    assert_eq!(
        internal.class(),
        ErrorClass::Unsupported,
        "{context}: unsupported execution class drifted",
    );
    assert_eq!(
        internal.origin(),
        ErrorOrigin::Query,
        "{context}: unsupported execution origin drifted",
    );

    let diagnostic = internal.diagnostic();
    assert_eq!(
        diagnostic.code().class(),
        icydb_diagnostic_code::ErrorClass::Unsupported,
        "{context}: compact diagnostic class drifted",
    );
    assert_eq!(
        diagnostic.origin(),
        icydb_diagnostic_code::ErrorOrigin::Query,
        "{context}: compact diagnostic origin drifted",
    );
}

// Assert one query error is the generic unsupported-query boundary, used by
// test-only helper lanes that do not yet have a narrower structured code.
fn assert_runtime_unsupported_query_execution_diagnostic(err: QueryError, context: &str) {
    let QueryError::Execute(crate::db::query::intent::QueryExecutionError::Unsupported(internal)) =
        err
    else {
        panic!("{context}: expected unsupported query execution error");
    };

    assert_eq!(
        internal.class(),
        ErrorClass::Unsupported,
        "{context}: unsupported execution class drifted",
    );
    assert_eq!(
        internal.origin(),
        ErrorOrigin::Query,
        "{context}: unsupported execution origin drifted",
    );

    let diagnostic = internal.diagnostic();
    assert_eq!(
        diagnostic.code(),
        DiagnosticCode::RuntimeUnsupported,
        "{context}: generic unsupported diagnostic code drifted",
    );
    assert_eq!(
        diagnostic.origin(),
        icydb_diagnostic_code::ErrorOrigin::Query,
        "{context}: generic unsupported diagnostic origin drifted",
    );
    assert_eq!(
        diagnostic.detail(),
        None,
        "{context}: generic unsupported boundary should not invent detail",
    );
}

// Assert one SQL lowering/planning failure stays a structured unknown-field
// planner error instead of relying on rendered text.
fn assert_query_plan_expr_unknown_field(err: QueryError, expected_field: &str, context: &str) {
    let diagnostic = err.diagnostic();
    assert_eq!(
        diagnostic.code(),
        DiagnosticCode::QueryPlan,
        "{context}: unknown-field diagnostic code drifted",
    );
    assert_eq!(
        diagnostic.origin(),
        icydb_diagnostic_code::ErrorOrigin::Query,
        "{context}: unknown-field diagnostic origin drifted",
    );

    let QueryError::Plan(plan) = err else {
        panic!("{context}: expected query plan error");
    };
    let PlanError::User(user) = *plan else {
        panic!("{context}: expected user-facing plan error");
    };
    let field = match *user {
        PlanUserError::Expr(expr) => {
            let ExprPlanError::UnknownField { field } = *expr else {
                panic!("{context}: expected expression unknown-field error");
            };
            field
        }
        PlanUserError::PredicateInvalid(validate) => {
            let ValidateError::UnknownField { field } = *validate else {
                panic!("{context}: expected predicate unknown-field error");
            };
            field
        }
        _ => panic!("{context}: expected expression or predicate unknown-field plan error"),
    };

    assert_eq!(
        field, expected_field,
        "{context}: unknown-field identity drifted",
    );
}

// Assert one SQL predicate/schema validation failure stays attached to a
// structured field identity instead of relying on rendered text.
fn assert_query_plan_predicate_invalid_field(err: QueryError, expected_field: &str, context: &str) {
    let diagnostic = err.diagnostic();
    assert_eq!(
        diagnostic.code(),
        DiagnosticCode::QueryPlan,
        "{context}: predicate validation diagnostic code drifted",
    );
    assert_eq!(
        diagnostic.origin(),
        icydb_diagnostic_code::ErrorOrigin::Query,
        "{context}: predicate validation diagnostic origin drifted",
    );

    let QueryError::Plan(plan) = err else {
        panic!("{context}: expected query plan error");
    };
    let PlanError::User(user) = *plan else {
        panic!("{context}: expected user-facing plan error");
    };
    let PlanUserError::PredicateInvalid(validate) = *user else {
        panic!("{context}: expected predicate validation plan error");
    };
    let (ValidateError::UnknownField { field }
    | ValidateError::NonQueryableFieldType { field }
    | ValidateError::DuplicateField { field }
    | ValidateError::InvalidPrimaryKey { field }
    | ValidateError::InvalidPrimaryKeyType { field }
    | ValidateError::IndexFieldUnknown { field, .. }
    | ValidateError::IndexFieldNotQueryable { field, .. }
    | ValidateError::IndexFieldMapNotQueryable { field, .. }
    | ValidateError::IndexFieldDuplicate { field, .. }
    | ValidateError::InvalidOperator { field, .. }
    | ValidateError::InvalidCoercion { field, .. }
    | ValidateError::InvalidLiteral { field, .. }) = *validate
    else {
        panic!("{context}: predicate validation error did not carry a field identity");
    };

    assert_eq!(
        field, expected_field,
        "{context}: predicate validation field identity drifted",
    );
}

// Assert one SQL surface result fails with the unsupported execution boundary.
fn assert_unsupported_sql_surface_result<T>(result: Result<T, QueryError>, context: &str) {
    let Err(err) = result else {
        panic!("{context}");
    };
    assert_unsupported_query_execution_diagnostic(err, context);
}

const fn unsupported_sql_feature_cases() -> [(&'static str, SqlFeatureCode); 5] {
    [
        (
            "SELECT * FROM SessionSqlEntity JOIN other ON SessionSqlEntity.id = other.id",
            SqlFeatureCode::Join,
        ),
        (
            "SELECT \"name\" FROM SessionSqlEntity",
            SqlFeatureCode::QuotedIdentifiers,
        ),
        (
            "SELECT * FROM SessionSqlEntity WHERE name LIKE '%Al'",
            SqlFeatureCode::LikePatternBeyondTrailingPrefix,
        ),
        (
            "SELECT * FROM SessionSqlEntity WHERE LOWER(name) LIKE '%Al'",
            SqlFeatureCode::LikePatternBeyondTrailingPrefix,
        ),
        (
            "SELECT * FROM SessionSqlEntity WHERE UPPER(name) LIKE '%Al'",
            SqlFeatureCode::LikePatternBeyondTrailingPrefix,
        ),
    ]
}

const fn unsupported_sql_parser_feature_cases() -> [(&'static str, SqlFeatureCode); 2] {
    [
        (
            "SELECT * FROM SessionSqlEntity JOIN other ON SessionSqlEntity.id = other.id",
            SqlFeatureCode::Join,
        ),
        (
            "SELECT \"name\" FROM SessionSqlEntity",
            SqlFeatureCode::QuotedIdentifiers,
        ),
    ]
}

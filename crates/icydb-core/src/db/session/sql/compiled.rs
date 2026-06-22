//! Module: db::session::sql::compiled
//! Responsibility: session-owned compiled SQL command artifacts.
//! Does not own: SQL parsing/lowering or execution dispatch.
//! Boundary: carries generic-free compiled SQL state between session compile and execute phases.

use crate::db::{
    access::LoweredIndexPrefixCardinalitySpec,
    commit::CommitSchemaFingerprint,
    executor::{EntityAuthority, SharedPreparedExecutionPlan},
    query::intent::StructuralQuery,
    schema::{AcceptedSchemaSnapshot, SchemaVersion},
    session::{AcceptedSchemaCatalogContext, sql::projection::SqlProjectionContract},
    sql::{
        lowering::{LoweredSqlCommand, StructuralSqlGlobalAggregateCommand},
        parser::{SqlInsertStatement, SqlReturningProjection, SqlUpdateStatement},
    },
};
use std::sync::{Arc, OnceLock};

#[derive(Debug)]
pub(in crate::db) struct SqlSelectPlanCacheEntry {
    schema_fingerprint_method_version: u8,
    schema_fingerprint: CommitSchemaFingerprint,
    prepared_plan: SharedPreparedExecutionPlan,
    projection: SqlProjectionContract,
}

impl SqlSelectPlanCacheEntry {
    #[must_use]
    pub(in crate::db) const fn new(
        schema_fingerprint_method_version: u8,
        schema_fingerprint: CommitSchemaFingerprint,
        prepared_plan: SharedPreparedExecutionPlan,
        projection: SqlProjectionContract,
    ) -> Self {
        Self {
            schema_fingerprint_method_version,
            schema_fingerprint,
            prepared_plan,
            projection,
        }
    }

    #[must_use]
    pub(in crate::db) const fn schema_fingerprint_method_version(&self) -> u8 {
        self.schema_fingerprint_method_version
    }

    #[must_use]
    pub(in crate::db) const fn schema_fingerprint(&self) -> CommitSchemaFingerprint {
        self.schema_fingerprint
    }

    #[must_use]
    pub(in crate::db) fn prepared_plan(&self) -> SharedPreparedExecutionPlan {
        self.prepared_plan.clone()
    }

    #[must_use]
    pub(in crate::db) fn projection(&self) -> SqlProjectionContract {
        self.projection.clone()
    }
}

#[derive(Debug)]
pub(in crate::db) struct SqlGlobalAggregatePlanCacheEntry {
    schema_fingerprint_method_version: u8,
    schema_fingerprint: CommitSchemaFingerprint,
    prepared_plan: SharedPreparedExecutionPlan,
}

impl SqlGlobalAggregatePlanCacheEntry {
    #[must_use]
    pub(in crate::db) const fn new(
        schema_fingerprint_method_version: u8,
        schema_fingerprint: CommitSchemaFingerprint,
        prepared_plan: SharedPreparedExecutionPlan,
    ) -> Self {
        Self {
            schema_fingerprint_method_version,
            schema_fingerprint,
            prepared_plan,
        }
    }

    #[must_use]
    pub(in crate::db) const fn schema_fingerprint_method_version(&self) -> u8 {
        self.schema_fingerprint_method_version
    }

    #[must_use]
    pub(in crate::db) const fn schema_fingerprint(&self) -> CommitSchemaFingerprint {
        self.schema_fingerprint
    }

    #[must_use]
    pub(in crate::db) fn prepared_plan(&self) -> SharedPreparedExecutionPlan {
        self.prepared_plan.clone()
    }
}

#[derive(Debug)]
pub(in crate::db) struct SqlGlobalAggregateCountPlanCacheEntry {
    schema_fingerprint_method_version: u8,
    schema_fingerprint: CommitSchemaFingerprint,
    prefix_specs: Arc<[LoweredIndexPrefixCardinalitySpec]>,
}

impl SqlGlobalAggregateCountPlanCacheEntry {
    #[must_use]
    pub(in crate::db) const fn new(
        schema_fingerprint_method_version: u8,
        schema_fingerprint: CommitSchemaFingerprint,
        prefix_specs: Arc<[LoweredIndexPrefixCardinalitySpec]>,
    ) -> Self {
        Self {
            schema_fingerprint_method_version,
            schema_fingerprint,
            prefix_specs,
        }
    }

    #[must_use]
    pub(in crate::db) const fn schema_fingerprint_method_version(&self) -> u8 {
        self.schema_fingerprint_method_version
    }

    #[must_use]
    pub(in crate::db) const fn schema_fingerprint(&self) -> CommitSchemaFingerprint {
        self.schema_fingerprint
    }

    #[must_use]
    pub(in crate::db) fn prefix_specs(&self) -> &[LoweredIndexPrefixCardinalitySpec] {
        self.prefix_specs.as_ref()
    }
}

///
/// CompiledSqlCommand
///
/// CompiledSqlCommand is the generic-free SQL compile artifact stored in the
/// session SQL cache and later dispatched by the SQL execution boundary.
/// It deliberately carries syntax-surface commands, not executor scratch state.
///

#[derive(Clone, Debug)]
pub(in crate::db) enum CompiledSqlCommand {
    Select {
        query: Arc<StructuralQuery>,
        plan_cache: Arc<OnceLock<Arc<SqlSelectPlanCacheEntry>>>,
    },
    Delete {
        query: Arc<StructuralQuery>,
        returning: Option<SqlReturningProjection>,
    },
    GlobalAggregate {
        command: Arc<StructuralSqlGlobalAggregateCommand>,
        plan_cache: Arc<OnceLock<Arc<SqlGlobalAggregatePlanCacheEntry>>>,
        count_plan_cache: Arc<OnceLock<Arc<SqlGlobalAggregateCountPlanCacheEntry>>>,
    },
    Explain(Box<LoweredSqlCommand>),
    Insert(CompiledSqlInsertCommand),
    Update(SqlUpdateStatement),
    DescribeEntity,
    ShowIndexesEntity,
    ShowColumnsEntity,
    ShowEntities {
        entity: Option<String>,
        verbose: bool,
    },
    ShowStores {
        verbose: bool,
    },
    ShowMemory,
}

///
/// CompiledSqlInsertCommand
///
/// CompiledSqlInsertCommand carries one normalized INSERT statement plus the
/// optional bound source query for `INSERT ... SELECT`.
/// VALUES inserts keep no source query; SELECT inserts reuse the compiled
/// source artifact during execution instead of preparing and binding it again.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct CompiledSqlInsertCommand {
    statement: SqlInsertStatement,
    source_query: Option<Arc<StructuralQuery>>,
}

impl CompiledSqlInsertCommand {
    /// Build one compiled INSERT command from its normalized statement and
    /// optional already-bound source query.
    #[must_use]
    pub(in crate::db) fn new(
        statement: SqlInsertStatement,
        source_query: Option<StructuralQuery>,
    ) -> Self {
        Self {
            statement,
            source_query: source_query.map(Arc::new),
        }
    }

    /// Borrow the normalized INSERT syntax surface.
    #[must_use]
    pub(in crate::db) const fn statement(&self) -> &SqlInsertStatement {
        &self.statement
    }

    /// Borrow the bound INSERT SELECT source query when this command uses a
    /// SELECT source.
    #[must_use]
    pub(in crate::db) fn source_query(&self) -> Option<&StructuralQuery> {
        self.source_query.as_deref()
    }
}

impl CompiledSqlCommand {
    /// Return whether this command executes through the singleton global
    /// aggregate lane.
    #[must_use]
    pub(in crate::db::session::sql) const fn is_global_aggregate(&self) -> bool {
        matches!(self, Self::GlobalAggregate { .. })
    }

    /// Return whether this command mutates rows.
    #[must_use]
    pub(in crate::db::session::sql) const fn is_mutation(&self) -> bool {
        matches!(
            self,
            Self::Delete { .. } | Self::Insert(_) | Self::Update(_)
        )
    }

    /// Return whether this command returns result rows to the caller.
    #[must_use]
    pub(in crate::db::session::sql) const fn returns_rows(&self) -> bool {
        match self {
            Self::Select { .. } | Self::GlobalAggregate { .. } => true,
            Self::Delete { returning, .. } => returning.is_some(),
            Self::Insert(command) => command.statement().returning.is_some(),
            Self::Update(statement) => statement.returning.is_some(),
            Self::Explain(_)
            | Self::DescribeEntity
            | Self::ShowIndexesEntity
            | Self::ShowColumnsEntity
            | Self::ShowEntities { .. }
            | Self::ShowStores { .. }
            | Self::ShowMemory => false,
        }
    }

    #[must_use]
    pub(in crate::db) fn select(query: StructuralQuery) -> Self {
        Self::Select {
            query: Arc::new(query),
            plan_cache: Arc::new(OnceLock::new()),
        }
    }

    #[must_use]
    pub(in crate::db) fn global_aggregate(command: StructuralSqlGlobalAggregateCommand) -> Self {
        Self::GlobalAggregate {
            command: Arc::new(command),
            plan_cache: Arc::new(OnceLock::new()),
            count_plan_cache: Arc::new(OnceLock::new()),
        }
    }

    #[must_use]
    pub(in crate::db) fn cached_select_plan(
        &self,
        schema_fingerprint_method_version: u8,
        schema_fingerprint: CommitSchemaFingerprint,
    ) -> Option<(SharedPreparedExecutionPlan, SqlProjectionContract)> {
        let Self::Select { plan_cache, .. } = self else {
            return None;
        };
        let entry = plan_cache.get()?;
        if entry.schema_fingerprint_method_version() != schema_fingerprint_method_version
            || entry.schema_fingerprint() != schema_fingerprint
        {
            return None;
        }

        Some((entry.prepared_plan(), entry.projection()))
    }

    pub(in crate::db) fn set_cached_select_plan(
        &self,
        schema_fingerprint_method_version: u8,
        schema_fingerprint: CommitSchemaFingerprint,
        prepared_plan: SharedPreparedExecutionPlan,
        projection: SqlProjectionContract,
    ) {
        if let Self::Select { plan_cache, .. } = self {
            let _ = plan_cache.set(Arc::new(SqlSelectPlanCacheEntry::new(
                schema_fingerprint_method_version,
                schema_fingerprint,
                prepared_plan,
                projection,
            )));
        }
    }

    #[must_use]
    pub(in crate::db) fn cached_global_aggregate_plan(
        &self,
        schema_fingerprint_method_version: u8,
        schema_fingerprint: CommitSchemaFingerprint,
    ) -> Option<SharedPreparedExecutionPlan> {
        let Self::GlobalAggregate { plan_cache, .. } = self else {
            return None;
        };
        let entry = plan_cache.get()?;
        if entry.schema_fingerprint_method_version() != schema_fingerprint_method_version
            || entry.schema_fingerprint() != schema_fingerprint
        {
            return None;
        }

        Some(entry.prepared_plan())
    }

    #[must_use]
    pub(in crate::db) fn cached_global_aggregate_count_plan(
        &self,
        schema_fingerprint_method_version: u8,
        schema_fingerprint: CommitSchemaFingerprint,
    ) -> Option<Arc<SqlGlobalAggregateCountPlanCacheEntry>> {
        let Self::GlobalAggregate {
            count_plan_cache, ..
        } = self
        else {
            return None;
        };
        let entry = count_plan_cache.get()?;
        if entry.schema_fingerprint_method_version() != schema_fingerprint_method_version
            || entry.schema_fingerprint() != schema_fingerprint
        {
            return None;
        }

        Some(Arc::clone(entry))
    }

    pub(in crate::db) fn set_cached_global_aggregate_plan(
        &self,
        schema_fingerprint_method_version: u8,
        schema_fingerprint: CommitSchemaFingerprint,
        prepared_plan: SharedPreparedExecutionPlan,
    ) {
        if let Self::GlobalAggregate { plan_cache, .. } = self {
            let _ = plan_cache.set(Arc::new(SqlGlobalAggregatePlanCacheEntry::new(
                schema_fingerprint_method_version,
                schema_fingerprint,
                prepared_plan,
            )));
        }
    }

    pub(in crate::db) fn set_cached_global_aggregate_count_plan(
        &self,
        entry: Arc<SqlGlobalAggregateCountPlanCacheEntry>,
    ) {
        if let Self::GlobalAggregate {
            count_plan_cache, ..
        } = self
        {
            let _ = count_plan_cache.set(entry);
        }
    }
}

///
/// SqlCompiledCommandExecutionContext
///
/// SqlCompiledCommandExecutionContext carries the accepted schema facts loaded
/// while compiling one SQL command through to immediate execution. Query calls
/// cannot rely on heap cache writes persisting, so the cold path must avoid
/// reloading the same accepted schema between compile and plan lookup.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct SqlCompiledCommandExecutionContext {
    command: CompiledSqlCommand,
    catalog: AcceptedSchemaCatalogContext,
    accepted_authority: Option<EntityAuthority>,
}

impl SqlCompiledCommandExecutionContext {
    #[must_use]
    pub(in crate::db) fn new(
        command: CompiledSqlCommand,
        catalog: AcceptedSchemaCatalogContext,
        accepted_authority: Option<EntityAuthority>,
    ) -> Self {
        let context = Self {
            command,
            catalog,
            accepted_authority,
        };
        debug_assert_eq!(
            context.schema_version(),
            context.accepted_schema().persisted_snapshot().version()
        );

        context
    }

    #[must_use]
    pub(in crate::db) const fn command(&self) -> &CompiledSqlCommand {
        &self.command
    }

    #[must_use]
    pub(in crate::db) fn into_command(self) -> CompiledSqlCommand {
        self.command
    }

    #[must_use]
    pub(in crate::db) const fn accepted_schema(&self) -> &AcceptedSchemaSnapshot {
        self.catalog.snapshot()
    }

    #[must_use]
    pub(in crate::db) const fn accepted_catalog(&self) -> &AcceptedSchemaCatalogContext {
        &self.catalog
    }

    #[must_use]
    pub(in crate::db) const fn schema_version(&self) -> SchemaVersion {
        self.catalog.schema_version()
    }

    #[must_use]
    pub(in crate::db) const fn schema_fingerprint(&self) -> CommitSchemaFingerprint {
        self.catalog.fingerprint()
    }

    #[must_use]
    pub(in crate::db) const fn schema_fingerprint_method_version(&self) -> u8 {
        self.catalog.fingerprint_method_version()
    }

    #[must_use]
    pub(in crate::db) const fn accepted_authority(&self) -> Option<&EntityAuthority> {
        self.accepted_authority.as_ref()
    }
}

//! Generic-free compiled SQL command artifacts.
//! Does not own: accepted-schema execution context handoff.

use super::cache::{
    SqlCompiledSchemaFingerprint, SqlGlobalAggregateCountPlanCacheEntry,
    SqlGlobalAggregatePlanCacheEntry, SqlSelectPlanCacheEntry,
};
#[cfg(feature = "sql-explain")]
use crate::db::sql::lowering::LoweredSqlCommand;
use crate::db::{
    executor::SharedPreparedExecutionPlan,
    query::intent::StructuralQuery,
    session::sql::projection::SqlProjectionContract,
    sql::{
        lowering::SqlGlobalAggregateCommand,
        parser::{SqlInsertStatement, SqlReturningProjection, SqlUpdateStatement},
    },
};
use std::{
    rc::Rc,
    sync::{Arc, OnceLock},
};

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
        plan_cache: Rc<OnceLock<Rc<SqlSelectPlanCacheEntry>>>,
    },
    Delete {
        query: Arc<StructuralQuery>,
        returning: Option<SqlReturningProjection>,
    },
    GlobalAggregate {
        command: Arc<SqlGlobalAggregateCommand>,
        plan_cache: Rc<OnceLock<Rc<SqlGlobalAggregatePlanCacheEntry>>>,
        count_plan_cache: Rc<OnceLock<Rc<SqlGlobalAggregateCountPlanCacheEntry>>>,
    },
    #[cfg(feature = "sql-explain")]
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
            #[cfg(feature = "sql-explain")]
            Self::Explain(_) => false,
            Self::DescribeEntity
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
            plan_cache: Rc::new(OnceLock::new()),
        }
    }

    #[must_use]
    pub(in crate::db) fn global_aggregate(command: SqlGlobalAggregateCommand) -> Self {
        Self::GlobalAggregate {
            command: Arc::new(command),
            plan_cache: Rc::new(OnceLock::new()),
            count_plan_cache: Rc::new(OnceLock::new()),
        }
    }

    #[must_use]
    pub(in crate::db) fn cached_select_plan(
        &self,
        schema_fingerprint: SqlCompiledSchemaFingerprint,
    ) -> Option<(SharedPreparedExecutionPlan, SqlProjectionContract)> {
        let Self::Select { plan_cache, .. } = self else {
            return None;
        };
        let entry = plan_cache.get()?;
        if !entry.schema_fingerprint.matches(schema_fingerprint) {
            return None;
        }

        Some((entry.prepared_plan(), entry.projection()))
    }

    pub(in crate::db) fn set_cached_select_plan(
        &self,
        schema_fingerprint: SqlCompiledSchemaFingerprint,
        prepared_plan: SharedPreparedExecutionPlan,
        projection: SqlProjectionContract,
    ) {
        if let Self::Select { plan_cache, .. } = self {
            let _ = plan_cache.set(Rc::new(SqlSelectPlanCacheEntry::new(
                schema_fingerprint,
                prepared_plan,
                projection,
            )));
        }
    }

    #[must_use]
    pub(in crate::db) fn cached_global_aggregate_plan(
        &self,
        schema_fingerprint: SqlCompiledSchemaFingerprint,
    ) -> Option<SharedPreparedExecutionPlan> {
        let Self::GlobalAggregate { plan_cache, .. } = self else {
            return None;
        };
        let entry = plan_cache.get()?;
        if !entry.schema_fingerprint.matches(schema_fingerprint) {
            return None;
        }

        Some(entry.prepared_plan())
    }

    #[must_use]
    pub(in crate::db) fn cached_global_aggregate_count_plan(
        &self,
        schema_fingerprint: SqlCompiledSchemaFingerprint,
    ) -> Option<Rc<SqlGlobalAggregateCountPlanCacheEntry>> {
        let Self::GlobalAggregate {
            count_plan_cache, ..
        } = self
        else {
            return None;
        };
        let entry = count_plan_cache.get()?;
        if !entry.schema_fingerprint.matches(schema_fingerprint) {
            return None;
        }

        Some(Rc::clone(entry))
    }

    pub(in crate::db) fn set_cached_global_aggregate_plan(
        &self,
        schema_fingerprint: SqlCompiledSchemaFingerprint,
        prepared_plan: SharedPreparedExecutionPlan,
    ) {
        if let Self::GlobalAggregate { plan_cache, .. } = self {
            let _ = plan_cache.set(Rc::new(SqlGlobalAggregatePlanCacheEntry::new(
                schema_fingerprint,
                prepared_plan,
            )));
        }
    }

    pub(in crate::db) fn set_cached_global_aggregate_count_plan(
        &self,
        entry: Rc<SqlGlobalAggregateCountPlanCacheEntry>,
    ) {
        if let Self::GlobalAggregate {
            count_plan_cache, ..
        } = self
        {
            let _ = count_plan_cache.set(entry);
        }
    }
}

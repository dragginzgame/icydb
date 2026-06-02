//! Module: db::session::sql::compiled
//! Responsibility: session-owned compiled SQL command artifacts.
//! Does not own: SQL parsing/lowering or execution dispatch.
//! Boundary: carries generic-free compiled SQL state between session compile and execute phases.

use crate::db::{
    commit::CommitSchemaFingerprint,
    executor::{EntityAuthority, SharedPreparedExecutionPlan},
    query::intent::StructuralQuery,
    schema::AcceptedSchemaSnapshot,
    session::AcceptedSchemaCatalogContext,
    sql::{
        lowering::{LoweredSqlCommand, StructuralSqlGlobalAggregateCommand},
        parser::{SqlInsertStatement, SqlReturningProjection, SqlUpdateStatement},
    },
};
use std::sync::{Arc, OnceLock};

#[derive(Debug)]
pub(in crate::db) struct SqlSelectPlanCacheEntry {
    schema_fingerprint: CommitSchemaFingerprint,
    prepared_plan: SharedPreparedExecutionPlan,
    projection: SqlProjectionContract,
}

impl SqlSelectPlanCacheEntry {
    #[must_use]
    pub(in crate::db) const fn new(
        schema_fingerprint: CommitSchemaFingerprint,
        prepared_plan: SharedPreparedExecutionPlan,
        projection: SqlProjectionContract,
    ) -> Self {
        Self {
            schema_fingerprint,
            prepared_plan,
            projection,
        }
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
        command: Box<StructuralSqlGlobalAggregateCommand>,
    },
    Explain(Box<LoweredSqlCommand>),
    Insert(SqlInsertStatement),
    Update(SqlUpdateStatement),
    DescribeEntity,
    ShowIndexesEntity,
    ShowColumnsEntity,
    ShowEntities {
        verbose: bool,
    },
    ShowStores {
        verbose: bool,
    },
    ShowMemory,
}

impl CompiledSqlCommand {
    #[must_use]
    pub(in crate::db) fn select(query: StructuralQuery) -> Self {
        Self::Select {
            query: Arc::new(query),
            plan_cache: Arc::new(OnceLock::new()),
        }
    }

    #[must_use]
    pub(in crate::db) fn cached_select_plan(
        &self,
        schema_fingerprint: CommitSchemaFingerprint,
    ) -> Option<(SharedPreparedExecutionPlan, SqlProjectionContract)> {
        let Self::Select { plan_cache, .. } = self else {
            return None;
        };
        let entry = plan_cache.get()?;
        if entry.schema_fingerprint() != schema_fingerprint {
            return None;
        }

        Some((entry.prepared_plan(), entry.projection()))
    }

    pub(in crate::db) fn set_cached_select_plan(
        &self,
        schema_fingerprint: CommitSchemaFingerprint,
        prepared_plan: SharedPreparedExecutionPlan,
        projection: SqlProjectionContract,
    ) {
        if let Self::Select { plan_cache, .. } = self {
            let _ = plan_cache.set(Arc::new(SqlSelectPlanCacheEntry::new(
                schema_fingerprint,
                prepared_plan,
                projection,
            )));
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
    pub(in crate::db) const fn new(
        command: CompiledSqlCommand,
        catalog: AcceptedSchemaCatalogContext,
        accepted_authority: Option<EntityAuthority>,
    ) -> Self {
        Self {
            command,
            catalog,
            accepted_authority,
        }
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
    pub(in crate::db) const fn schema_fingerprint(&self) -> CommitSchemaFingerprint {
        self.catalog.fingerprint()
    }

    #[must_use]
    pub(in crate::db) const fn accepted_authority(&self) -> Option<&EntityAuthority> {
        self.accepted_authority.as_ref()
    }
}

///
/// SqlProjectionContract
///
/// SqlProjectionContract is the outward SQL projection contract derived from
/// one shared lower prepared plan. SQL execution keeps this wrapper so
/// statement shaping stays owner-local while prepared-plan reuse lives below it.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct SqlProjectionContract {
    columns: Vec<String>,
    fixed_scales: Vec<Option<u32>>,
}

impl SqlProjectionContract {
    #[must_use]
    pub(in crate::db) const fn new(columns: Vec<String>, fixed_scales: Vec<Option<u32>>) -> Self {
        Self {
            columns,
            fixed_scales,
        }
    }

    #[must_use]
    pub(in crate::db) fn into_components(self) -> (Vec<String>, Vec<Option<u32>>) {
        (self.columns, self.fixed_scales)
    }
}

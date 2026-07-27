//! Module: session
//! Responsibility: user-facing query/write execution facade over db executors.
//! Does not own: planning semantics, cursor validation rules, or storage mutation protocol.
//! Boundary: converts fluent/query intent calls into executor operations and response DTOs.

mod accepted_schema;
mod bounded_cache;
mod catalog;
mod integrity;
mod query;
mod response;
#[cfg(feature = "sql")]
mod sql;
mod write;

use crate::{
    db::{Db, EntityRegistration, StoreRegistry},
    metrics::sink::{MetricsSink, with_metrics_sink},
    traits::CanisterKind,
    value::Value,
};
use std::thread::LocalKey;

pub(in crate::db) use accepted_schema::AcceptedSchemaCatalogContext;
#[cfg(feature = "diagnostics")]
pub use query::{
    DirectDataRowAttribution, GroupedCountAttribution, GroupedExecutionAttribution,
    KernelRowAttribution, ScalarAggregateAttribution,
};
pub(in crate::db) use response::finalize_structural_grouped_projection_result;
#[cfg(feature = "sql")]
pub(in crate::db) use response::sql_grouped_cursor_from_bytes;
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub use sql::{
    SqlCompileAttribution, SqlExecutionAttribution, SqlHybridCoveringAttribution,
    SqlOutputBlobAttribution, SqlPureCoveringAttribution, SqlQueryCacheAttribution,
    SqlQueryExecutionAttribution,
};
#[cfg(feature = "sql")]
pub use sql::{
    SqlConstraintValidationPage, SqlConstraintValidationRevisionStatus,
    SqlConstraintValidationState, SqlDdlExecutionStatus, SqlDdlMutationKind,
    SqlDdlPreparationReport, SqlIntegrityError, SqlStatementDispatch, SqlStatementResult,
    SqlStatementShellSurface, SqlStatementSurface, TrustedResumableUpdateContinuation,
    TrustedResumableUpdatePhase, TrustedResumableUpdateReceipt,
    TrustedResumableUpdateRestartReason, sql_statement_dispatch, sql_statement_entity_name,
    sql_statement_shell_surface, sql_statement_surface,
};
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub use sql::{SqlProjectionMaterializationMetrics, with_sql_projection_materialization_metrics};
#[cfg(feature = "sql")]
pub(in crate::db::session) use write::{
    AcceptedStructuralMutation, AcceptedStructuralMutationTarget,
    structural_data_key_from_runtime_values,
};

///
/// DbSession
///
/// Session-scoped database handle with policy (debug, metrics) and execution routing.
///

pub struct DbSession<C: CanisterKind> {
    db: Db<C>,
    debug: bool,
    metrics: Option<&'static dyn MetricsSink>,
}

impl<C: CanisterKind> DbSession<C> {
    /// Construct one session facade for a database handle.
    #[must_use]
    pub(crate) const fn new(db: Db<C>) -> Self {
        Self {
            db,
            debug: false,
            metrics: None,
        }
    }

    /// Construct one session facade from store and entity registrations.
    #[must_use]
    pub const fn new_with_registrations(
        store: &'static LocalKey<StoreRegistry>,
        entity_registrations: &'static [EntityRegistration<C>],
    ) -> Self {
        Self::new(Db::new_with_registrations(store, entity_registrations))
    }

    /// Enable debug execution behavior where supported by executors.
    #[must_use]
    pub const fn debug(mut self) -> Self {
        self.debug = true;
        self
    }

    /// Attach one metrics sink for all session-executed operations.
    #[must_use]
    pub const fn metrics_sink(mut self, sink: &'static dyn MetricsSink) -> Self {
        self.metrics = Some(sink);
        self
    }

    fn with_metrics<T>(&self, f: impl FnOnce() -> T) -> T {
        if let Some(sink) = self.metrics {
            with_metrics_sink(sink, f)
        } else {
            f()
        }
    }

    /// Return one constant scalar row equivalent to SQL `SELECT 1`.
    ///
    /// This terminal bypasses query planning and access routing entirely.
    #[must_use]
    pub const fn select_one(&self) -> Value {
        Value::Int64(1)
    }
}

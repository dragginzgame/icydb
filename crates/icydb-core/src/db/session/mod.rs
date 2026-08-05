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

#[cfg(all(test, feature = "sql", feature = "diagnostics"))]
mod tests;

use crate::metrics::sink::with_metrics_sink;
use crate::{
    db::{Db, StoreRegistry},
    metrics::sink::MetricsSink,
    traits::CanisterKind,
};
use std::thread::LocalKey;

pub(in crate::db) use accepted_schema::AcceptedSchemaCatalogContext;
#[cfg(all(test, feature = "sql", feature = "diagnostics"))]
pub(in crate::db) use accepted_schema::{
    AcceptedSchemaRuntimeBuildCounts, accepted_schema_runtime_build_counts_for_tests,
    reset_accepted_schema_runtime_build_counts_for_tests,
};
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub use query::{
    DirectDataRowAttribution, GroupedCountAttribution, GroupedExecutionAttribution,
    KernelRowAttribution, ScalarAggregateAttribution,
};
#[doc(hidden)]
pub use query::{
    MAX_TYPED_EXACT_KEY_BATCH_INPUT_BYTES, MAX_TYPED_EXACT_KEY_BATCH_ITEMS,
    MAX_TYPED_EXACT_KEY_BATCH_RESULT_BYTES, MAX_TYPED_EXACT_KEY_BATCH_STORED_BYTES,
};
pub(in crate::db) use response::finalize_structural_grouped_projection_result;
pub(in crate::db) use response::grouped_cursor_from_bytes;
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
    /// Construct one session facade over a sealed runtime store registry.
    #[must_use]
    pub const fn new(store: &'static LocalKey<StoreRegistry>) -> Self {
        Self {
            db: Db::new(store),
            debug: false,
            metrics: None,
        }
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
}

//! Module: session
//! Responsibility: user-facing query/write execution facade over db executors.
//! Does not own: planning semantics, cursor validation rules, or storage mutation protocol.
//! Boundary: converts fluent/query intent calls into executor operations and response DTOs.

mod accepted_schema;
mod bounded_cache;
mod catalog;
mod integrity;
mod query;
mod read_set;
mod request;
mod response;
mod resumable_job;
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
pub use request::RequestExecutionRoot;
pub(in crate::db) use request::RequestExecutionScope;
pub(in crate::db) use response::finalize_structural_grouped_projection_result;
pub(in crate::db) use response::grouped_cursor_from_bytes;
#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub use sql::{
    SqlCompileAttribution, SqlDistinctProjectionAttribution, SqlExecutionAttribution,
    SqlHybridCoveringAttribution, SqlOutputBlobAttribution, SqlPureCoveringAttribution,
    SqlQueryCacheAttribution, SqlQueryExecutionAttribution,
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
    pub fn new(
        store: &'static LocalKey<StoreRegistry>,
        request_root: &RequestExecutionRoot,
    ) -> Self {
        Self {
            db: Db::new(store, request_root.scope()),
            debug: false,
            metrics: None,
        }
    }

    /// Advance generated startup recovery without admitting ordinary database work.
    #[doc(hidden)]
    pub fn __continue_startup_recovery(&self) -> Result<bool, crate::error::InternalError> {
        self.db.continue_startup_recovery()
    }

    /// Construct a session from the active synchronous request scope.
    ///
    /// Generated zero-argument `db!()` wiring uses this entry. `None` means
    /// that the caller did not establish a request execution boundary.
    #[doc(hidden)]
    #[must_use]
    pub fn __new_from_current_request(store: &'static LocalKey<StoreRegistry>) -> Option<Self> {
        request::current_request_scope().map(|scope| Self {
            db: Db::new(store, scope),
            debug: false,
            metrics: None,
        })
    }

    /// Enable bounded request-wide query diagnostics without resetting prior counters.
    ///
    /// Returns `true` only when this call enabled collection. Every session
    /// derived from the same request root observes the same diagnostic state.
    #[cfg(feature = "diagnostics")]
    #[must_use]
    pub fn enable_request_diagnostics(&self) -> bool {
        self.db.request_execution_scope().enable_diagnostics()
    }

    /// Snapshot bounded request-wide query diagnostics when collection is enabled.
    #[cfg(feature = "diagnostics")]
    #[must_use]
    pub fn request_diagnostics(&self) -> Option<crate::db::RequestDiagnostics> {
        self.db.request_execution_scope().diagnostics_snapshot()
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

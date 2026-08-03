//! Module: db::session
//!
//! Responsibility: public session, typed-query, SQL, and structural-write facade.
//! Does not own: core execution, storage engines, or planner semantics.
//! Boundary: wraps core sessions with stable generated-code and application APIs.

mod catalog;
pub(crate) mod generated;
mod integrity;
#[cfg(feature = "sql")]
mod sql;
mod write;

use crate::{metrics::MetricsSink, traits::CanisterKind};

use icydb_core as core;

// re-exports
pub use integrity::IntegrityCheckError;
#[cfg(feature = "sql")]
pub use integrity::SqlIntegrityError;
#[cfg(feature = "sql")]
pub use sql::{
    SqlExecutionPerfAttribution, SqlPureCoveringPerfAttribution, SqlQueryPerfAttribution,
};
pub use write::{
    OutputRow, StructuralMutation, StructuralPatch, TypedAdapterError, TypedBindingError,
    TypedEntityAdapter, TypedEntityBinding, TypedRowAdapter, TypedRowError, TypedWrite,
    TypedWriteAdapter, TypedWriteError, WriteCell,
};
#[doc(hidden)]
pub use write::{TypedFieldBindingRequest, TypedFieldType};

///
/// DbSession
///
/// Public facade for typed/dynamic query adaptation, optional SQL execution,
/// and structural mutation policy.
/// Wraps the core session and converts core results and errors into the
/// outward-facing `icydb` response surface.
///

pub struct DbSession<C: CanisterKind> {
    inner: core::db::DbSession<C>,
}

impl<C: CanisterKind> DbSession<C> {
    // ------------------------------------------------------------------
    // Session configuration
    // ------------------------------------------------------------------

    #[must_use]
    pub const fn new(session: core::db::DbSession<C>) -> Self {
        Self { inner: session }
    }

    #[must_use]
    pub const fn debug(mut self) -> Self {
        self.inner = self.inner.debug();
        self
    }

    #[must_use]
    pub fn metrics_sink(mut self, sink: &'static dyn MetricsSink) -> Self {
        self.inner = self.inner.metrics_sink(sink);
        self
    }

    /// Execute one trusted entity-name-driven dynamic read.
    pub fn execute_trusted_dynamic_query(
        &self,
        request: &crate::db::DynamicQuery,
    ) -> Result<crate::db::RowProjectionOutput, crate::Error> {
        self.inner
            .execute_trusted_dynamic_query(request)
            .map_err(Into::into)
    }

    /// Execute one ordinary entity-name-driven bounded read.
    pub fn execute_public_dynamic_query(
        &self,
        request: &crate::db::DynamicQuery,
    ) -> Result<crate::db::RowProjectionOutput, crate::Error> {
        self.inner
            .execute_public_dynamic_query(request)
            .map_err(Into::into)
    }

    /// Execute one ordinary entity-name-driven bounded grouped read.
    pub fn execute_public_dynamic_grouped_query(
        &self,
        request: &crate::db::DynamicQuery,
    ) -> Result<crate::db::GroupedQueryOutput, crate::Error> {
        self.inner
            .execute_public_dynamic_grouped_query(request)
            .map_err(Into::into)
    }

    /// Execute one trusted entity-name-driven grouped read.
    pub fn execute_trusted_dynamic_grouped_query(
        &self,
        request: &crate::db::DynamicQuery,
    ) -> Result<crate::db::GroupedQueryOutput, crate::Error> {
        self.inner
            .execute_trusted_dynamic_grouped_query(request)
            .map_err(Into::into)
    }

    pub(crate) fn execute_public_typed_dynamic_query(
        &self,
        binding: &TypedEntityBinding,
        request: &crate::db::DynamicQuery,
    ) -> Result<Option<crate::db::RowProjectionOutput>, crate::Error> {
        self.inner
            .execute_public_dynamic_query_for_typed_binding(binding.inner(), request)
            .map_err(Into::into)
    }

    pub(crate) fn execute_public_typed_dynamic_grouped_query(
        &self,
        binding: &TypedEntityBinding,
        request: &crate::db::DynamicQuery,
    ) -> Result<Option<crate::db::GroupedQueryOutput>, crate::Error> {
        self.inner
            .execute_public_dynamic_grouped_query_for_typed_binding(binding.inner(), request)
            .map_err(Into::into)
    }
}

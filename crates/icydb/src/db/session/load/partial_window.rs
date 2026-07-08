//! Module: db::session::load::partial_window
//!
//! Responsibility: public facade wrapper for deliberately partial row-window
//! fluent load reads.
//! Does not own: read admission, query planning, or row materialization.
//! Boundary: exposes only row-window materialization and diagnostics, not
//! semantic page/complete/exact aggregate terminals.

use crate::{
    db::{
        ExplainExecutionNodeDescriptor,
        query::{CompiledQuery, ExplainPlan, PlannedQuery, QueryTracePlan},
        response::{QueryResponse, Response},
    },
    error::Error,
    traits::Entity,
};

use icydb_core as core;

///
/// PartialWindowLoadQuery
///
/// Facade wrapper for deliberately partial row-window reads.
/// It exposes materialization and diagnostics, but not semantic terminals such
/// as paging, complete collection, existence, or exact aggregates.
///

pub struct PartialWindowLoadQuery<'a, E: Entity> {
    pub(super) inner: core::db::PartialWindowLoadQuery<'a, E>,
}

impl<E: Entity> PartialWindowLoadQuery<'_, E> {
    /// Execute this partial window with diagnostics attribution.
    #[cfg(feature = "diagnostics")]
    #[doc(hidden)]
    pub fn execute_with_attribution(
        &self,
    ) -> Result<(QueryResponse<E>, crate::db::QueryExecutionAttribution), Error> {
        let (result, attribution) = self.inner.execute_with_attribution()?;

        Ok((QueryResponse::from_core(result), attribution))
    }

    /// Mark this partial window as trusted and bypass the default bounded read
    /// gate.
    ///
    /// Use this only for controller/admin maintenance code that owns its
    /// authorization and resource policy. Caller-facing list endpoints should
    /// use `page(limit)` / `next_page(limit, cursor)` instead of trusted
    /// partial windows.
    #[must_use]
    pub fn trusted_read_unchecked(mut self) -> Self {
        self.inner = self.inner.trusted_read_unchecked();
        self
    }

    /// Execute this deliberately partial row window.
    ///
    /// Scalar queries return `QueryResponse::Rows`; grouped queries return
    /// `QueryResponse::Grouped`. Use `into_rows()` or `into_grouped()` when
    /// the endpoint expects one concrete shape.
    pub fn execute(&self) -> Result<QueryResponse<E>, Error> {
        Ok(QueryResponse::from_core(self.inner.execute()?))
    }

    /// Execute this deliberately partial row window as scalar entity rows.
    pub fn execute_rows(&self) -> Result<Response<E>, Error> {
        Ok(Response::from_core(self.inner.execute_rows()?))
    }

    /// Return the stable plan hash for this partial-window query.
    pub fn plan_hash_hex(&self) -> Result<String, Error> {
        Ok(self.inner.plan_hash_hex()?)
    }

    /// Build one trace payload without executing the partial-window query.
    pub fn trace(&self) -> Result<QueryTracePlan, Error> {
        Ok(self.inner.trace()?)
    }

    /// Build the validated logical plan without compiling execution details.
    pub fn planned(&self) -> Result<PlannedQuery<E>, Error> {
        Ok(self.inner.planned()?)
    }

    /// Build the compiled executable plan for this partial-window query.
    pub fn plan(&self) -> Result<CompiledQuery<E>, Error> {
        Ok(self.inner.plan()?)
    }

    /// Build logical explain metadata for the current partial-window query.
    pub fn explain(&self) -> Result<ExplainPlan, Error> {
        Ok(self.inner.explain()?)
    }

    /// Explain the execution shape without executing the partial-window query.
    pub fn explain_execution(&self) -> Result<ExplainExecutionNodeDescriptor, Error> {
        Ok(self.inner.explain_execution()?)
    }

    /// Render execution explain output as a compact text tree.
    pub fn explain_execution_text(&self) -> Result<String, Error> {
        Ok(self.inner.explain_execution_text()?)
    }

    /// Render execution explain output as canonical JSON.
    pub fn explain_execution_json(&self) -> Result<String, Error> {
        Ok(self.inner.explain_execution_json()?)
    }

    /// Render execution explain output as a verbose text tree.
    pub fn explain_execution_verbose(&self) -> Result<String, Error> {
        Ok(self.inner.explain_execution_verbose()?)
    }
}

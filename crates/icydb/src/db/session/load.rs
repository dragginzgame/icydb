//! Module: db::session::load
//!
//! Responsibility: public session and fluent query facade.
//! Does not own: core execution, storage engines, or planner semantics.
//! Boundary: wraps core sessions with stable generated-code and application APIs.

use crate::{
    db::{
        AdminBatchRequest, ExplainAggregateTerminalPlan, ExplainExecutionNodeDescriptor,
        PageRequest,
        query::{
            AggregateExpr, CompareOp, CompiledQuery, ExplainPlan, FilterExpr, PlannedQuery, Query,
            QueryTracePlan,
        },
        response::{PagedResponse, QueryResponse, Response},
        session::macros::{impl_session_materialization_methods, impl_session_query_shape_methods},
    },
    error::Error,
    traits::{Entity, SingletonEntity},
    types::{Decimal, Id},
    value::InputValue,
};

use icydb_core as core;

///
/// FluentLoadQuery
///
/// Session-bound fluent wrapper for typed load queries.
/// This facade keeps query shaping and execution on the public `icydb`
/// surface while delegating planning and execution to `icydb-core`.
///

pub struct FluentLoadQuery<'a, E: Entity> {
    pub(crate) inner: core::db::FluentLoadQuery<'a, E>,
}

impl<'a, E: Entity> FluentLoadQuery<'a, E> {
    // ------------------------------------------------------------------
    // Intent inspection
    // ------------------------------------------------------------------

    #[must_use]
    pub const fn query(&self) -> &Query<E> {
        self.inner.query()
    }

    // ------------------------------------------------------------------
    // Primary-key access (semantic)
    // ------------------------------------------------------------------

    impl_session_query_shape_methods!();

    // ------------------------------------------------------------------
    // Query refinement
    // ------------------------------------------------------------------

    /// Skip a number of rows in the ordered result stream.
    ///
    /// Scalar pagination requires explicit ordering; combine `offset` and/or
    /// `limit` with `order_term(...)` or planning fails for scalar loads.
    /// GROUP BY pagination uses canonical grouped-key order by default.
    #[must_use]
    pub fn offset(mut self, offset: u32) -> Self {
        self.inner = self.inner.offset(offset);
        self
    }

    /// Return a deliberately partial row window.
    ///
    /// This is the hard-cut replacement for raw public read `.limit(...)` on
    /// load queries. Use it only when the endpoint contract is "the first N
    /// rows under this order." Use `page(...)` for public pages,
    /// `collect_complete()` for complete small sets, and exact aggregate
    /// helpers for semantic aggregates.
    #[must_use]
    pub fn partial_window(mut self, limit: u32) -> Self {
        self.inner = self.inner.limit(limit);
        self
    }

    /// Mark this fluent read as trusted and bypass the default bounded read gate.
    ///
    /// Use this only for controller/admin maintenance code that has its own
    /// authorization and resource policy. Application-facing reads should stay
    /// on the normal bounded execution path through `execute`, `execute_rows`,
    /// `page(...)`, or terminal helpers.
    #[must_use]
    pub fn trusted_read_unchecked(mut self) -> Self {
        self.inner = self.inner.trusted_read_unchecked();
        self
    }

    /// Add one grouped key field.
    pub fn group_by(mut self, field: impl AsRef<str>) -> Result<Self, Error> {
        self.inner = self.inner.group_by(field)?;
        Ok(self)
    }

    /// Add one grouped aggregate terminal.
    #[must_use]
    pub fn aggregate(mut self, aggregate: AggregateExpr) -> Self {
        self.inner = self.inner.aggregate(aggregate);
        self
    }

    /// Override grouped hard limits for grouped execution budget enforcement.
    #[must_use]
    pub fn grouped_limits(mut self, max_groups: u64, max_group_bytes: u64) -> Self {
        self.inner = self.inner.grouped_limits(max_groups, max_group_bytes);
        self
    }

    /// Add one grouped HAVING compare clause over one grouped key field.
    pub fn having_group(
        mut self,
        field: impl AsRef<str>,
        op: CompareOp,
        value: InputValue,
    ) -> Result<Self, Error> {
        self.inner = self.inner.having_group(field, op, value)?;
        Ok(self)
    }

    /// Add one grouped HAVING compare clause over one grouped aggregate output.
    pub fn having_aggregate(
        mut self,
        aggregate_index: usize,
        op: CompareOp,
        value: InputValue,
    ) -> Result<Self, Error> {
        self.inner = self.inner.having_aggregate(aggregate_index, op, value)?;
        Ok(self)
    }

    // ------------------------------------------------------------------
    // Execution primitives
    // ------------------------------------------------------------------
    impl_session_materialization_methods!();

    /// Enter typed cursor-pagination mode for this request-owned page.
    ///
    /// Cursor pagination requires explicit ordering and disallows a prior raw
    /// `limit(...)`; `PageRequest` owns the page size and cursor.
    /// Continuation is best-effort and forward-only over live state:
    /// deterministic per request under canonical ordering, with no
    /// snapshot/version pinned across requests.
    pub fn page(self, request: PageRequest) -> Result<PagedLoadQuery<'a, E>, Error> {
        Ok(PagedLoadQuery {
            inner: self.inner.page(request)?,
        })
    }

    /// Execute a trusted/admin cursor batch with an engine-owned batch size.
    ///
    /// This terminal is only for reads that have already opted into
    /// `trusted_read_unchecked()`. Application-facing list endpoints should
    /// use `page(PageRequest::...)`.
    pub fn admin_batch(self, request: AdminBatchRequest) -> Result<PagedResponse<E>, Error>
    where
        E: Entity,
    {
        let execution = self.inner.admin_batch(request)?;
        let read_intent = execution.read_intent();
        let (response, continuation_cursor) = execution.into_response_and_cursor();
        let next_cursor = continuation_cursor.as_deref().map(core::db::encode_cursor);

        Ok(PagedResponse::new(
            response.entities(),
            next_cursor,
            read_intent,
        ))
    }

    /// Execute as a scalar row load through the default bounded read-admission
    /// gate.
    ///
    /// Grouped queries return grouped rows through `execute().into_grouped()`;
    /// this method is for scalar entity-row reads.
    pub fn execute_rows(&self) -> Result<Response<E>, Error>
    where
        E: Entity,
    {
        Ok(Response::from_core(self.inner.execute_rows()?))
    }

    /// Return the stable plan hash for this query.
    pub fn plan_hash_hex(&self) -> Result<String, Error> {
        Ok(self.inner.plan_hash_hex()?)
    }

    /// Build one trace payload without executing the query.
    pub fn trace(&self) -> Result<QueryTracePlan, Error> {
        Ok(self.inner.trace()?)
    }

    /// Build the validated logical plan without compiling execution details.
    pub fn planned(&self) -> Result<PlannedQuery<E>, Error> {
        Ok(self.inner.planned()?)
    }

    /// Build the compiled executable plan for this query.
    pub fn plan(&self) -> Result<CompiledQuery<E>, Error> {
        Ok(self.inner.plan()?)
    }

    /// Build logical explain metadata for the current query.
    pub fn explain(&self) -> Result<ExplainPlan, Error> {
        Ok(self.inner.explain()?)
    }

    // ------------------------------------------------------------------
    // Aggregation helpers
    // ------------------------------------------------------------------

    /// Return whether at least one matching row exists.
    pub fn exists(&self) -> Result<bool, Error>
    where
        E: Entity,
    {
        Ok(self.inner.exists()?)
    }

    /// Return whether at least one matching row exists with terminal
    /// diagnostics attribution.
    #[cfg(feature = "diagnostics")]
    #[doc(hidden)]
    pub fn exists_with_attribution(
        &self,
    ) -> Result<(bool, crate::db::FluentTerminalExecutionAttribution), Error>
    where
        E: Entity,
    {
        Ok(self.inner.exists_with_attribution()?)
    }

    /// Explain scalar `exists()` routing without executing the terminal.
    pub fn explain_exists(&self) -> Result<ExplainAggregateTerminalPlan, Error>
    where
        E: Entity,
    {
        Ok(self.inner.explain_exists()?)
    }

    /// Return whether no matching row exists.
    pub fn not_exists(&self) -> Result<bool, Error>
    where
        E: Entity,
    {
        Ok(self.inner.not_exists()?)
    }

    /// Return all matching rows if the complete result fits in the default
    /// public-read small-set cap.
    ///
    /// This semantic terminal rejects a prior `partial_window(...)`; use
    /// `execute_rows()` when returning a partial row window is the endpoint
    /// contract.
    pub fn collect_complete(&self) -> Result<Vec<E>, Error>
    where
        E: Entity,
    {
        Ok(self.inner.collect_complete()?)
    }

    /// Return all matching rows with query diagnostics attribution if the
    /// complete result fits in the default public-read small-set cap.
    #[cfg(feature = "diagnostics")]
    #[doc(hidden)]
    pub fn collect_complete_with_attribution(
        &self,
    ) -> Result<(Vec<E>, crate::db::QueryExecutionAttribution), Error>
    where
        E: Entity,
    {
        Ok(self.inner.collect_complete_with_attribution()?)
    }

    /// Return the exact number of matching rows.
    ///
    /// This semantic aggregate rejects a prior partial row window because an
    /// exact count must not mean "count the first N rows."
    pub fn count_exact(&self) -> Result<u32, Error>
    where
        E: Entity,
    {
        Ok(self.inner.count_exact()?)
    }

    /// Explain exact count routing without executing the terminal.
    pub fn explain_count_exact(&self) -> Result<ExplainAggregateTerminalPlan, Error>
    where
        E: Entity,
    {
        Ok(self.inner.explain_count_exact()?)
    }

    /// Return the exact row count with terminal diagnostics attribution.
    #[cfg(feature = "diagnostics")]
    #[doc(hidden)]
    pub fn count_exact_with_attribution(
        &self,
    ) -> Result<(u32, crate::db::FluentTerminalExecutionAttribution), Error>
    where
        E: Entity,
    {
        Ok(self.inner.count_exact_with_attribution()?)
    }

    /// Explain scalar `not_exists()` routing without executing the terminal.
    pub fn explain_not_exists(&self) -> Result<ExplainAggregateTerminalPlan, Error>
    where
        E: Entity,
    {
        Ok(self.inner.explain_not_exists()?)
    }

    /// Explain the execution shape without executing the query.
    pub fn explain_execution(&self) -> Result<ExplainExecutionNodeDescriptor, Error>
    where
        E: Entity,
    {
        Ok(self.inner.explain_execution()?)
    }

    /// Render execution explain output as a compact text tree.
    pub fn explain_execution_text(&self) -> Result<String, Error>
    where
        E: Entity,
    {
        Ok(self.inner.explain_execution_text()?)
    }

    /// Render execution explain output as canonical JSON.
    pub fn explain_execution_json(&self) -> Result<String, Error>
    where
        E: Entity,
    {
        Ok(self.inner.explain_execution_json()?)
    }

    /// Render execution explain output as a verbose text tree.
    pub fn explain_execution_verbose(&self) -> Result<String, Error>
    where
        E: Entity,
    {
        Ok(self.inner.explain_execution_verbose()?)
    }

    /// Return the exact minimum identifier under deterministic response ordering.
    ///
    /// This semantic aggregate rejects a prior partial row window because an
    /// exact minimum must not mean "minimum over the first N rows."
    pub fn min_exact(&self) -> Result<Option<Id<E>>, Error>
    where
        E: Entity,
    {
        Ok(self.inner.min_exact()?)
    }

    /// Explain exact `min_exact()` routing without executing the terminal.
    pub fn explain_min_exact(&self) -> Result<ExplainAggregateTerminalPlan, Error>
    where
        E: Entity,
    {
        Ok(self.inner.explain_min_exact()?)
    }

    /// Return the identifier with the exact minimum `field` value.
    ///
    /// This semantic aggregate rejects a prior partial row window because an
    /// exact minimum must not mean "minimum over the first N rows."
    pub fn min_exact_by(&self, field: impl AsRef<str>) -> Result<Option<Id<E>>, Error>
    where
        E: Entity,
    {
        Ok(self.inner.min_exact_by(field)?)
    }

    /// Explain exact `min_exact_by(field)` routing without executing the terminal.
    pub fn explain_min_exact_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, Error>
    where
        E: Entity,
    {
        Ok(self.inner.explain_min_exact_by(field)?)
    }

    /// Return the exact maximum identifier under deterministic response ordering.
    ///
    /// This semantic aggregate rejects a prior partial row window because an
    /// exact maximum must not mean "maximum over the first N rows."
    pub fn max_exact(&self) -> Result<Option<Id<E>>, Error>
    where
        E: Entity,
    {
        Ok(self.inner.max_exact()?)
    }

    /// Explain exact `max_exact()` routing without executing the terminal.
    pub fn explain_max_exact(&self) -> Result<ExplainAggregateTerminalPlan, Error>
    where
        E: Entity,
    {
        Ok(self.inner.explain_max_exact()?)
    }

    /// Return the identifier with the exact maximum `field` value.
    ///
    /// This semantic aggregate rejects a prior partial row window because an
    /// exact maximum must not mean "maximum over the first N rows."
    pub fn max_exact_by(&self, field: impl AsRef<str>) -> Result<Option<Id<E>>, Error>
    where
        E: Entity,
    {
        Ok(self.inner.max_exact_by(field)?)
    }

    /// Explain exact `max_exact_by(field)` routing without executing the terminal.
    pub fn explain_max_exact_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, Error>
    where
        E: Entity,
    {
        Ok(self.inner.explain_max_exact_by(field)?)
    }

    /// Return the exact sum of `field` over matching rows.
    ///
    /// This semantic aggregate rejects a prior partial row window because an
    /// exact sum must not mean "sum the first N rows."
    pub fn sum_exact(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, Error>
    where
        E: Entity,
    {
        Ok(self.inner.sum_exact(field)?)
    }

    /// Explain exact sum routing without executing the terminal.
    pub fn explain_sum_exact(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, Error>
    where
        E: Entity,
    {
        Ok(self.inner.explain_sum_exact(field)?)
    }

    /// Return the exact average of `field` over matching rows.
    ///
    /// This semantic aggregate rejects a prior partial row window because an
    /// exact average must not mean "average the first N rows."
    pub fn avg_exact(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, Error>
    where
        E: Entity,
    {
        Ok(self.inner.avg_exact(field)?)
    }

    /// Explain exact `avg_exact(field)` routing without executing the terminal.
    pub fn explain_avg_exact(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, Error>
    where
        E: Entity,
    {
        Ok(self.inner.explain_avg_exact(field)?)
    }

    /// Materialize zero or one entity, failing when more than one row matches.
    pub fn try_one(&self) -> Result<Option<E>, Error>
    where
        E: Entity,
    {
        icydb_core::db::ResponseCardinalityExt::try_entity(self.inner.execute_rows()?)
            .map_err(Into::into)
    }
}

impl<E: Entity + SingletonEntity> FluentLoadQuery<'_, E> {
    /// Load the singleton entity.
    #[must_use]
    pub fn only(mut self) -> Self
    where
        E::Key: Default,
    {
        self.inner = self.inner.only();
        self
    }
}

///
/// PagedLoadQuery
///
/// Facade wrapper for cursor-pagination mode over typed load queries.
/// Returns typed entity items plus an opaque continuation cursor.
///

pub struct PagedLoadQuery<'a, E: Entity> {
    pub(crate) inner: core::db::PagedLoadQuery<'a, E>,
}

impl<E: Entity> PagedLoadQuery<'_, E> {
    #[must_use]
    pub const fn query(&self) -> &Query<E> {
        self.inner.query()
    }

    /// Execute in cursor-pagination mode through the default bounded
    /// read-admission gate.
    ///
    /// Continuation is best-effort and forward-only over live state:
    /// deterministic per request under canonical ordering, with no
    /// snapshot/version pinned across requests.
    pub fn execute(self) -> Result<PagedResponse<E>, Error>
    where
        E: Entity,
    {
        let execution = self.inner.execute()?;
        let read_intent = execution.read_intent();
        let (response, continuation_cursor) = execution.into_response_and_cursor();
        let next_cursor = continuation_cursor.as_deref().map(core::db::encode_cursor);

        Ok(PagedResponse::new(
            response.entities(),
            next_cursor,
            read_intent,
        ))
    }
}

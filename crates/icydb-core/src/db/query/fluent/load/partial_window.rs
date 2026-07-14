//! Module: query::fluent::load::partial_window
//! Responsibility: fluent partial row-window wrapper APIs.
//! Does not own: planner semantic validation or runtime execution internals.
//! Boundary: exposes deliberately partial materialization and diagnostics only.

use crate::{
    db::{
        EntityResponse, PersistedRow,
        query::{
            explain::{ExplainExecutionNodeDescriptor, ExplainPlan},
            fluent::load::{FluentLoadQuery, LoadQueryResult},
            intent::QueryError,
            trace::QueryTracePlan,
        },
    },
    entity::{EntityKind, EntityValue},
};

#[cfg(feature = "diagnostics")]
use crate::db::QueryExecutionAttribution;

///
/// PartialWindowLoadQuery
///
/// Session-bound partial row-window wrapper.
/// This wrapper exposes materialization and diagnostics, but not semantic read
/// terminals such as pages, complete collection, existence, or exact
/// aggregates.
///

pub struct PartialWindowLoadQuery<'a, E>
where
    E: EntityKind,
{
    inner: FluentLoadQuery<'a, E>,
}

impl<'a, E> PartialWindowLoadQuery<'a, E>
where
    E: EntityKind,
{
    pub(in crate::db) const fn new(inner: FluentLoadQuery<'a, E>) -> Self {
        Self { inner }
    }

    /// Borrow the current immutable query intent.
    /// Execute this partial window with diagnostics attribution.
    #[cfg(feature = "diagnostics")]
    #[doc(hidden)]
    pub fn execute_with_attribution(
        &self,
    ) -> Result<(LoadQueryResult<E>, QueryExecutionAttribution), QueryError>
    where
        E: PersistedRow,
    {
        self.inner.execute_with_attribution()
    }

    /// Mark this partial row-window read as trusted.
    ///
    /// Use this only for controller/admin maintenance code or internal test
    /// harnesses that have their own authorization and resource bounds.
    #[must_use]
    pub fn trusted_read_unchecked(mut self) -> Self {
        self.inner = self.inner.trusted_read_unchecked();
        self
    }

    /// Build explain metadata for the current partial-window query.
    pub fn explain(&self) -> Result<ExplainPlan, QueryError> {
        self.inner.explain()
    }

    /// Return the stable plan hash for this partial-window query.
    pub fn plan_hash_hex(&self) -> Result<String, QueryError> {
        self.inner.plan_hash_hex()
    }

    /// Build one trace payload without executing the partial-window query.
    pub fn trace(&self) -> Result<QueryTracePlan, QueryError> {
        self.inner.trace()
    }
}

impl<E> PartialWindowLoadQuery<'_, E>
where
    E: PersistedRow,
{
    /// Execute this partial-window query using the session's policy settings.
    pub fn execute(&self) -> Result<LoadQueryResult<E>, QueryError>
    where
        E: EntityValue,
    {
        self.inner.execute()
    }

    /// Execute this partial-window query through the scalar rows-only session
    /// boundary.
    pub fn execute_rows(&self) -> Result<EntityResponse<E>, QueryError>
    where
        E: EntityValue,
    {
        self.inner.execute_rows()
    }

    /// Explain scalar load execution shape without executing the partial-window
    /// query.
    pub fn explain_execution(&self) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        self.inner.explain_execution()
    }

    /// Explain scalar load execution shape as deterministic text.
    pub fn explain_execution_text(&self) -> Result<String, QueryError>
    where
        E: EntityValue,
    {
        self.inner.explain_execution_text()
    }

    /// Explain scalar load execution shape as canonical JSON.
    pub fn explain_execution_json(&self) -> Result<String, QueryError>
    where
        E: EntityValue,
    {
        self.inner.explain_execution_json()
    }

    /// Explain scalar load execution shape as verbose text with diagnostics.
    pub fn explain_execution_verbose(&self) -> Result<String, QueryError>
    where
        E: EntityValue,
    {
        self.inner.explain_execution_verbose()
    }
}

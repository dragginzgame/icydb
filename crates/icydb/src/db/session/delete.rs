use crate::{
    db::{
        PersistedRow, Row,
        query::{
            CompiledQuery, ExplainPlan, PlannedQuery, Query, QueryTracePlan,
            expr::{FilterExpr, SortExpr},
            predicate::Predicate,
        },
        response::Response,
        session::macros::{impl_session_materialization_methods, impl_session_query_shape_methods},
    },
    error::Error,
    traits::{EntityValue, SingletonEntity},
    types::Id,
};
use icydb_core as core;

///
/// SessionDeleteQuery
///
/// Session-bound fluent wrapper for typed delete queries.
/// This facade keeps delete query shaping and execution on the public
/// `icydb` surface while delegating planning and enforcement to `icydb-core`.
///

pub struct SessionDeleteQuery<'a, E: PersistedRow> {
    pub(crate) inner: core::db::FluentDeleteQuery<'a, E>,
}

impl<E: PersistedRow> SessionDeleteQuery<'_, E> {
    // ------------------------------------------------------------------
    // Intent inspection
    // ------------------------------------------------------------------

    #[must_use]
    pub const fn query(&self) -> &Query<E> {
        self.inner.query()
    }

    // ------------------------------------------------------------------
    // Primary-key access (query shaping)
    // ------------------------------------------------------------------

    impl_session_query_shape_methods!();

    // ------------------------------------------------------------------
    // Query refinement
    // ------------------------------------------------------------------

    // ------------------------------------------------------------------
    // Execution primitives
    // ------------------------------------------------------------------
    impl_session_materialization_methods!();

    /// Return the stable plan hash for this query.
    pub fn plan_hash_hex(&self) -> Result<String, Error> {
        Ok(self.inner.plan_hash_hex()?)
    }

    /// Build one trace payload without executing the query.
    pub fn trace(&self) -> Result<QueryTracePlan, Error> {
        Ok(self.inner.trace()?)
    }

    /// Build logical explain metadata for the current query.
    pub fn explain(&self) -> Result<ExplainPlan, Error> {
        Ok(self.inner.explain()?)
    }

    /// Build the validated logical plan without compiling execution details.
    pub fn planned(&self) -> Result<PlannedQuery<E>, Error> {
        Ok(self.inner.planned()?)
    }

    /// Build the compiled executable plan for this query.
    pub fn plan(&self) -> Result<CompiledQuery<E>, Error> {
        Ok(self.inner.plan()?)
    }

    /// Execute this delete while returning only the affected-row count.
    #[doc(hidden)]
    pub fn execute_count_only(&self) -> Result<u32, Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.execute_count_only()?)
    }
}

impl<E: PersistedRow + SingletonEntity> SessionDeleteQuery<'_, E> {
    /// Delete the singleton entity.
    #[must_use]
    pub fn only(mut self) -> Self
    where
        E::Key: Default,
    {
        self.inner = self.inner.only();
        self
    }
}

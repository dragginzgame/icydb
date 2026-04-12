use crate::{
    db::{
        DbSession, MutationResult, PersistedRow,
        query::{
            CompiledQuery, ExplainPlan, PlannedQuery, Query, QueryTracePlan,
            expr::{FilterExpr, SortExpr},
            predicate::Predicate,
        },
        session::macros::impl_session_query_shape_methods,
        sql::SqlQueryRowsOutput,
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

// Fluent delete returning selection kept private so the public surface only
// exposes the query wrapper types and the shared SQL-style row payload.
#[derive(Clone, Debug)]
enum DeleteReturningSelection {
    All,
    Fields(Vec<String>),
}

///
/// SessionDeleteReturningQuery
///
/// Session-bound fluent wrapper for typed delete queries that explicitly
/// request deleted rows. This keeps fluent `DELETE ... RETURNING` on the same
/// outward projection contract as SQL dispatch instead of inventing a second
/// row-returning result family.
///

pub struct SessionDeleteReturningQuery<'a, E: PersistedRow> {
    inner: core::db::FluentDeleteQuery<'a, E>,
    selection: DeleteReturningSelection,
}

impl<'a, E: PersistedRow> SessionDeleteQuery<'a, E> {
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

    /// Return every declared field from each deleted row.
    #[must_use]
    pub fn returning_all(self) -> SessionDeleteReturningQuery<'a, E> {
        SessionDeleteReturningQuery {
            inner: self.inner,
            selection: DeleteReturningSelection::All,
        }
    }

    /// Return one explicit field list from each deleted row.
    #[must_use]
    pub fn returning<I, S>(self, fields: I) -> SessionDeleteReturningQuery<'a, E>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        SessionDeleteReturningQuery {
            inner: self.inner,
            selection: DeleteReturningSelection::Fields(
                fields
                    .into_iter()
                    .map(|field| field.as_ref().to_string())
                    .collect(),
            ),
        }
    }

    /// Execute this delete under the shared mutation result contract.
    pub fn execute(&self) -> Result<MutationResult<E>, Error>
    where
        E: EntityValue,
    {
        Ok(MutationResult::from_count(self.inner.execute()?))
    }

    /// Return true when no rows were affected.
    pub fn is_empty(&self) -> Result<bool, Error>
    where
        E: EntityValue,
    {
        Ok(self.execute()?.is_empty())
    }

    /// Return the affected-row count.
    pub fn count(&self) -> Result<u32, Error>
    where
        E: EntityValue,
    {
        Ok(self.execute()?.count())
    }

    /// Require exactly one affected row.
    pub fn require_one(&self) -> Result<(), Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.require_one()?)
    }

    /// Require at least one affected row.
    pub fn require_some(&self) -> Result<(), Error>
    where
        E: EntityValue,
    {
        Ok(self.inner.require_some()?)
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

impl<E: PersistedRow> SessionDeleteReturningQuery<'_, E> {
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

    /// Execute this delete and return one SQL-style projection payload.
    pub fn execute(&self) -> Result<SqlQueryRowsOutput, Error>
    where
        E: EntityValue,
    {
        // Phase 1: materialize deleted entities on the shared typed delete
        // executor boundary so fluent returning follows the same delete
        // semantics as typed query execution.
        let deleted = self.inner.execute_rows()?.entities();

        // Phase 2: narrow those deleted entities onto the explicit
        // row-returning projection contract requested by the fluent surface.
        match &self.selection {
            DeleteReturningSelection::All => {
                DbSession::<E::Canister>::sql_query_rows_output_from_entities::<E>(
                    E::PATH.to_string(),
                    deleted,
                    None,
                )
            }
            DeleteReturningSelection::Fields(fields) => {
                DbSession::<E::Canister>::sql_query_rows_output_from_entities::<E>(
                    E::PATH.to_string(),
                    deleted,
                    Some(fields.as_slice()),
                )
            }
        }
    }

    /// Return true when the returning payload contains no rows.
    pub fn is_empty(&self) -> Result<bool, Error>
    where
        E: EntityValue,
    {
        Ok(self.execute()?.row_count == 0)
    }

    /// Return the number of deleted rows included in the returning payload.
    pub fn count(&self) -> Result<u32, Error>
    where
        E: EntityValue,
    {
        Ok(self.execute()?.row_count)
    }
}

impl<E: PersistedRow + SingletonEntity> SessionDeleteReturningQuery<'_, E> {
    /// Delete the singleton entity and return deleted rows.
    #[must_use]
    pub fn only(mut self) -> Self
    where
        E::Key: Default,
    {
        self.inner = self.inner.only();
        self
    }
}

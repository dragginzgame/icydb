//! Module: db::session::sql
//!
//! Responsibility: public `DbSession` SQL facade methods.
//! Does not own: SQL lowering, SQL planner semantics, or public read policy.
//! Boundary: wraps core SQL execution with public response conversion.

use crate::{
    db::{session::DbSession, sql::SqlQueryResult},
    error::Error,
    traits::CanisterKind,
};

use icydb_core as core;

impl<C: CanisterKind> DbSession<C> {
    fn sql_query_result_from_statement(
        statement: core::db::SqlStatementResult,
        entity: String,
    ) -> SqlQueryResult {
        crate::db::sql::sql_query_result_from_statement(statement, entity)
    }

    fn sql_query_result_from_dispatch(
        statement: core::db::SqlStatementResult,
        dispatch: &core::db::SqlStatementDispatch<'_>,
    ) -> SqlQueryResult {
        Self::sql_query_result_from_statement(
            statement,
            dispatch.entity_name().unwrap_or_default().to_string(),
        )
    }

    /// Execute one trusted/admin reduced SQL query against accepted catalog authority.
    ///
    /// This helper does not make caller-controlled SQL public-safe. Public
    /// endpoints should prefer ordinary typed/dynamic reads, or use an
    /// application-owned SQL allowlist before entering this trusted lane.
    pub fn execute_trusted_sql_query(&self, sql: &str) -> Result<SqlQueryResult, Error> {
        let dispatch = core::db::sql_statement_dispatch(sql)?;
        self.execute_trusted_sql_query_dispatch(&dispatch)
    }

    /// Execute one generated query from its admitted parsed dispatch artifact.
    #[doc(hidden)]
    pub fn execute_trusted_sql_query_dispatch(
        &self,
        dispatch: &core::db::SqlStatementDispatch<'_>,
    ) -> Result<SqlQueryResult, Error> {
        let (result, entity) = self
            .inner
            .execute_trusted_sql_query_with_entity_name(dispatch)?;
        Ok(Self::sql_query_result_from_statement(result, entity))
    }

    /// Execute one trusted SQL `INSERT` or `DELETE` against one entity type.
    ///
    /// `UPDATE` requires an explicit exact or prefix contract and is rejected
    /// by this broad mutation surface.
    pub fn execute_trusted_sql_mutation(&self, sql: &str) -> Result<SqlQueryResult, Error> {
        let dispatch = core::db::sql_statement_dispatch(sql)?;
        Ok(Self::sql_query_result_from_dispatch(
            self.inner
                .execute_trusted_sql_mutation_dispatch(&dispatch)?,
            &dispatch,
        ))
    }

    /// Execute one trusted exact complete-set SQL `UPDATE`.
    ///
    /// `require_affected_at_most` is a positive assertion about the complete
    /// target, not a selection limit. If one extra match exists, the call
    /// rejects before mutation. Exact selection uses authoritative primary-key
    /// traversal. The affected-row and scanned-key ceilings are independently
    /// enforced and are currently 4,096 each.
    pub fn execute_trusted_sql_exact_update(
        &self,
        sql: &str,
        require_affected_at_most: u32,
    ) -> Result<SqlQueryResult, Error> {
        let dispatch = core::db::sql_statement_dispatch(sql)?;
        Ok(Self::sql_query_result_from_dispatch(
            self.inner
                .execute_trusted_sql_exact_update_dispatch(&dispatch, require_affected_at_most)?,
            &dispatch,
        ))
    }

    /// Execute one intentional primary-key-ordered prefix SQL `UPDATE`.
    ///
    /// The statement must carry a positive bounded `LIMIT`; only that ordered
    /// prefix is mutated and no complete-target claim is made.
    pub fn execute_trusted_sql_prefix_update(&self, sql: &str) -> Result<SqlQueryResult, Error> {
        let dispatch = core::db::sql_statement_dispatch(sql)?;
        Ok(Self::sql_query_result_from_dispatch(
            self.inner
                .execute_trusted_sql_prefix_update_dispatch(&dispatch)?,
            &dispatch,
        ))
    }

    /// Execute one generated primary-key update from its parsed dispatch artifact.
    #[doc(hidden)]
    pub fn execute_sql_public_primary_key_update(
        &self,
        dispatch: &core::db::SqlStatementDispatch<'_>,
    ) -> Result<SqlQueryResult, Error> {
        Ok(Self::sql_query_result_from_dispatch(
            self.inner.execute_sql_public_primary_key_update(dispatch)?,
            dispatch,
        ))
    }

    /// Execute one generated bounded update from its parsed dispatch artifact.
    #[doc(hidden)]
    pub fn execute_sql_public_bounded_update(
        &self,
        dispatch: &core::db::SqlStatementDispatch<'_>,
    ) -> Result<SqlQueryResult, Error> {
        Ok(Self::sql_query_result_from_dispatch(
            self.inner.execute_sql_public_bounded_update(dispatch)?,
            dispatch,
        ))
    }

    /// Execute one public primary-key-only SQL `DELETE` against one entity type.
    #[doc(hidden)]
    pub fn execute_sql_public_primary_key_delete(
        &self,
        sql: &str,
    ) -> Result<SqlQueryResult, Error> {
        let dispatch = core::db::sql_statement_dispatch(sql)?;
        Ok(Self::sql_query_result_from_dispatch(
            self.inner
                .execute_sql_public_primary_key_delete(&dispatch)?,
            &dispatch,
        ))
    }

    /// Execute one bounded deterministic public SQL `DELETE`.
    #[doc(hidden)]
    pub fn execute_sql_public_bounded_delete(&self, sql: &str) -> Result<SqlQueryResult, Error> {
        let dispatch = core::db::sql_statement_dispatch(sql)?;
        Ok(Self::sql_query_result_from_dispatch(
            self.inner.execute_sql_public_bounded_delete(&dispatch)?,
            &dispatch,
        ))
    }

    /// Execute one administrative SQL DDL statement against accepted catalog
    /// authority selected by the statement's entity name.
    ///
    /// The caller must enforce controller or equivalent administrative
    /// authorization before accepting caller-controlled SQL.
    pub fn execute_admin_sql_ddl(&self, sql: &str) -> Result<SqlQueryResult, Error> {
        let dispatch = core::db::sql_statement_dispatch(sql)?;
        self.execute_admin_sql_ddl_dispatch(&dispatch)
    }

    /// Execute one generated DDL command from its parsed dispatch artifact.
    #[doc(hidden)]
    pub fn execute_admin_sql_ddl_dispatch(
        &self,
        dispatch: &core::db::SqlStatementDispatch<'_>,
    ) -> Result<SqlQueryResult, Error> {
        Ok(Self::sql_query_result_from_dispatch(
            self.inner.execute_admin_sql_ddl_dispatch(dispatch)?,
            dispatch,
        ))
    }
}

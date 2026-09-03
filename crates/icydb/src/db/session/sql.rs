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
        let entity = core::db::sql_statement_entity_name(sql)?.unwrap_or_default();
        Ok(Self::sql_query_result_from_statement(
            self.inner.execute_trusted_sql_mutation(sql)?,
            entity,
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
        let entity = core::db::sql_statement_entity_name(sql)?.unwrap_or_default();
        Ok(Self::sql_query_result_from_statement(
            self.inner
                .execute_trusted_sql_exact_update(sql, require_affected_at_most)?,
            entity,
        ))
    }

    /// Execute one intentional primary-key-ordered prefix SQL `UPDATE`.
    ///
    /// The statement must carry a positive bounded `LIMIT`; only that ordered
    /// prefix is mutated and no complete-target claim is made.
    pub fn execute_trusted_sql_prefix_update(&self, sql: &str) -> Result<SqlQueryResult, Error> {
        let entity = core::db::sql_statement_entity_name(sql)?.unwrap_or_default();
        Ok(Self::sql_query_result_from_statement(
            self.inner.execute_trusted_sql_prefix_update(sql)?,
            entity,
        ))
    }

    /// Execute one public primary-key-only SQL `UPDATE` against one entity type.
    #[doc(hidden)]
    pub fn execute_sql_public_primary_key_update(
        &self,
        sql: &str,
    ) -> Result<SqlQueryResult, Error> {
        let entity = core::db::sql_statement_entity_name(sql)?.unwrap_or_default();
        Ok(Self::sql_query_result_from_statement(
            self.inner.execute_sql_public_primary_key_update(sql)?,
            entity,
        ))
    }

    /// Execute one bounded deterministic public SQL `UPDATE`.
    #[doc(hidden)]
    pub fn execute_sql_public_bounded_update(&self, sql: &str) -> Result<SqlQueryResult, Error> {
        let entity = core::db::sql_statement_entity_name(sql)?.unwrap_or_default();
        Ok(Self::sql_query_result_from_statement(
            self.inner.execute_sql_public_bounded_update(sql)?,
            entity,
        ))
    }

    /// Execute one public primary-key-only SQL `DELETE` against one entity type.
    #[doc(hidden)]
    pub fn execute_sql_public_primary_key_delete(
        &self,
        sql: &str,
    ) -> Result<SqlQueryResult, Error> {
        let entity = core::db::sql_statement_entity_name(sql)?.unwrap_or_default();
        Ok(Self::sql_query_result_from_statement(
            self.inner.execute_sql_public_primary_key_delete(sql)?,
            entity,
        ))
    }

    /// Execute one bounded deterministic public SQL `DELETE`.
    #[doc(hidden)]
    pub fn execute_sql_public_bounded_delete(&self, sql: &str) -> Result<SqlQueryResult, Error> {
        let entity = core::db::sql_statement_entity_name(sql)?.unwrap_or_default();
        Ok(Self::sql_query_result_from_statement(
            self.inner.execute_sql_public_bounded_delete(sql)?,
            entity,
        ))
    }

    /// Execute one administrative SQL DDL statement against accepted catalog
    /// authority selected by the statement's entity name.
    ///
    /// The caller must enforce controller or equivalent administrative
    /// authorization before accepting caller-controlled SQL.
    pub fn execute_admin_sql_ddl(&self, sql: &str) -> Result<SqlQueryResult, Error> {
        let entity = core::db::sql_statement_entity_name(sql)?.unwrap_or_default();
        Ok(Self::sql_query_result_from_statement(
            self.inner.execute_admin_sql_ddl(sql)?,
            entity,
        ))
    }
}

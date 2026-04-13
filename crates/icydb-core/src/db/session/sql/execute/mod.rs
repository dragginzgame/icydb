//! Module: db::session::sql::execute
//! Responsibility: session-owned SQL execution entrypoints that bind lowered SQL
//! commands onto structural planning, execution, and outward result shaping.
//! Does not own: SQL parsing or executor runtime internals.
//! Boundary: centralizes authority-aware SQL execution routing while owner-local
//! submodules keep aggregate, write, and explain details out of the root.

mod aggregate;
mod lowered;
mod route;
mod write;

use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        executor::EntityAuthority,
        query::{intent::StructuralQuery, plan::AccessPlannedQuery},
        session::sql::{
            SqlStatementResult,
            projection::{
                SqlProjectionPayload, execute_sql_projection_rows_for_canister,
                projection_labels_from_projection_spec,
            },
        },
        sql::parser::SqlStatement,
    },
    traits::{CanisterKind, EntityValue},
};

#[cfg(feature = "perf-attribution")]
pub use lowered::LoweredSqlStatementExecutorAttribution;

impl<C: CanisterKind> DbSession<C> {
    // Build the shared structural SQL projection execution inputs once so
    // value-row and rendered-row statement surfaces only differ in final packaging.
    fn prepare_structural_sql_projection_execution(
        &self,
        query: StructuralQuery,
        authority: EntityAuthority,
    ) -> Result<(Vec<String>, AccessPlannedQuery), QueryError> {
        // Phase 1: build the structural access plan once and freeze its outward
        // column contract for all projection materialization surfaces.
        let (_, plan) =
            self.build_structural_plan_with_visible_indexes_for_authority(query, authority)?;
        let projection = plan.projection_spec(authority.model());
        let columns = projection_labels_from_projection_spec(&projection);

        Ok((columns, plan))
    }

    // Execute one structural SQL load query and return only row-oriented SQL
    // projection values, keeping typed projection rows out of the shared SQL
    // query-lane path.
    pub(in crate::db::session::sql) fn execute_structural_sql_projection(
        &self,
        query: StructuralQuery,
        authority: EntityAuthority,
    ) -> Result<SqlProjectionPayload, QueryError> {
        // Phase 1: build the shared structural plan and outward column contract once.
        let (columns, plan) = self.prepare_structural_sql_projection_execution(query, authority)?;

        // Phase 2: execute the shared structural load path with the already
        // derived projection semantics.
        let projected =
            execute_sql_projection_rows_for_canister(&self.db, self.debug, authority, plan)
                .map_err(QueryError::execute)?;
        let (rows, row_count) = projected.into_parts();

        Ok(SqlProjectionPayload::new(columns, rows, row_count))
    }

    /// Execute one parsed reduced SQL statement into one unified SQL payload.
    pub(in crate::db) fn execute_sql_statement_inner<E>(
        &self,
        sql_statement: &SqlStatement,
    ) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        match sql_statement {
            SqlStatement::Select(_) | SqlStatement::Delete(_) => self
                .execute_sql_query_route_for_authority(
                    sql_statement,
                    EntityAuthority::for_type::<E>(),
                    "execute_sql_statement accepts SELECT or DELETE only",
                    |session, select, authority, grouped_surface| {
                        if grouped_surface {
                            return session.execute_lowered_sql_grouped_statement_select_core(
                                select, authority,
                            );
                        }

                        let payload =
                            session.execute_lowered_sql_projection_core(select, authority)?;
                        Ok(payload.into_statement_result())
                    },
                    |session, delete, _authority| {
                        let SqlStatement::Delete(statement) = sql_statement else {
                            return Err(QueryError::invariant(
                                "DELETE SQL route must carry parsed DELETE statement",
                            ));
                        };

                        session.execute_sql_delete_statement::<E>(delete, statement)
                    },
                ),
            SqlStatement::Insert(statement) => self.execute_sql_insert_statement::<E>(statement),
            SqlStatement::Update(statement) => self.execute_sql_update_statement::<E>(statement),
            SqlStatement::Explain(_) => self.execute_sql_explain_route_for_authority(
                sql_statement,
                EntityAuthority::for_type::<E>(),
            ),
            SqlStatement::Describe(_) => {
                Ok(SqlStatementResult::Describe(self.describe_entity::<E>()))
            }
            SqlStatement::ShowIndexes(_) => {
                Ok(SqlStatementResult::ShowIndexes(self.show_indexes::<E>()))
            }
            SqlStatement::ShowColumns(_) => {
                Ok(SqlStatementResult::ShowColumns(self.show_columns::<E>()))
            }
            SqlStatement::ShowEntities(_) => {
                Ok(SqlStatementResult::ShowEntities(self.show_entities()))
            }
        }
    }
}

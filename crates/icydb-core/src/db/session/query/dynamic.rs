//! Module: db::session::query::dynamic
//! Responsibility: lower and execute public dynamic reads against accepted schema.
//! Does not own: query planning, accepted schema construction, or row projection.
//! Boundary: entity-name requests converge on the shared structural read lane.

use crate::{
    db::{
        DbSession, DynamicQuery, DynamicQueryResult, MissingRowPolicy, QueryError,
        query::intent::StructuralQuery, session::sql::SqlStatementResult,
    },
    error::InternalError,
    traits::CanisterKind,
};

#[derive(Clone, Copy)]
enum DynamicReadLane {
    Public,
    Trusted,
}

impl<C: CanisterKind> DbSession<C> {
    fn execute_dynamic_query(
        &self,
        request: &DynamicQuery,
        lane: DynamicReadLane,
    ) -> Result<DynamicQueryResult, QueryError> {
        if request.entity().is_empty() {
            return Err(QueryError::execute(
                InternalError::query_invalid_logical_plan(),
            ));
        }

        let catalog = self
            .accepted_schema_catalog_context_for_entity_name(Some(request.entity()))
            .map_err(QueryError::execute)?;
        let schema = catalog.accepted_schema_info();
        let mut query = StructuralQuery::new(MissingRowPolicy::Ignore);
        if let Some(filter) = request.filter_expr() {
            query = query.filter_for_schema(&schema, filter.clone());
        }
        for order in request.order_terms() {
            query = query.order_term(order.clone());
        }
        if !request.selected_fields().is_empty() {
            query = query.select_fields(request.selected_fields().iter().cloned());
        }
        if let Some(limit) = request.row_limit() {
            query = query.limit(limit);
        }

        let authority = catalog
            .accepted_entity_authority()
            .map_err(QueryError::execute)?;
        let (payload, _) = match lane {
            DynamicReadLane::Public => self.execute_public_projection_from_structural_query(
                query,
                authority,
                catalog.snapshot(),
            )?,
            DynamicReadLane::Trusted => self
                .execute_sql_projection_from_structural_query_without_sql_compiled_cache(
                    query,
                    authority,
                    catalog.snapshot(),
                )?,
        };
        let result = payload.into_statement_result()?;
        let SqlStatementResult::Projection {
            columns,
            rows,
            row_count,
            ..
        } = result
        else {
            return Err(QueryError::invariant());
        };

        Ok(DynamicQueryResult {
            entity: request.entity().to_string(),
            columns,
            rows,
            row_count,
        })
    }

    /// Execute one ordinary entity-name-driven dynamic read.
    ///
    /// The selected accepted plan must satisfy the built-in bounded public-read
    /// policy before any row is executed.
    pub fn execute_public_dynamic_query(
        &self,
        request: &DynamicQuery,
    ) -> Result<DynamicQueryResult, QueryError> {
        self.execute_dynamic_query(request, DynamicReadLane::Public)
    }

    /// Execute one trusted entity-name-driven dynamic read.
    ///
    /// This uses accepted schema, planner, executor, and projection authority
    /// only. Like trusted SQL reads, callers own authorization and resource
    /// policy.
    pub fn execute_trusted_dynamic_query(
        &self,
        request: &DynamicQuery,
    ) -> Result<DynamicQueryResult, QueryError> {
        self.execute_dynamic_query(request, DynamicReadLane::Trusted)
    }
}

//! Module: db::session::sql::dispatch
//! Responsibility: module-local ownership and contracts for db::session::sql::dispatch.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod computed;
mod lowered;

use crate::{
    db::{
        DbSession, MissingRowPolicy, PersistedRow, Query, QueryError,
        executor::{
            EntityAuthority, execute_sql_projection_rows_for_canister,
            execute_sql_projection_text_rows_for_canister,
        },
        identifiers_tail_match,
        query::intent::StructuralQuery,
        session::sql::{
            SqlDispatchResult, SqlParsedStatement, SqlStatementRoute,
            aggregate::{
                SqlAggregateSurface, parsed_requires_dedicated_sql_aggregate_lane,
                unsupported_sql_aggregate_lane_message,
            },
            computed_projection,
            projection::{
                SqlProjectionPayload, projection_labels_from_entity_model,
                projection_labels_from_projection_spec, sql_projection_rows_from_kernel_rows,
            },
        },
        sql::lowering::{
            LoweredSqlCommand, LoweredSqlQuery, bind_lowered_sql_query,
            lower_sql_command_from_prepared_statement,
        },
    },
    traits::{CanisterKind, EntityKind, EntityValue},
};

///
/// GeneratedSqlDispatchAttempt
///
/// Hidden generated-query dispatch envelope used by the facade helper to keep
/// generated route ownership in core while preserving the public EXPLAIN error
/// rewrite contract at the outer boundary.
///

#[doc(hidden)]
pub struct GeneratedSqlDispatchAttempt {
    entity_name: &'static str,
    explain_order_field: Option<&'static str>,
    result: Result<SqlDispatchResult, QueryError>,
}

impl GeneratedSqlDispatchAttempt {
    // Build one generated-query dispatch attempt with optional explain-hint context.
    const fn new(
        entity_name: &'static str,
        explain_order_field: Option<&'static str>,
        result: Result<SqlDispatchResult, QueryError>,
    ) -> Self {
        Self {
            entity_name,
            explain_order_field,
            result,
        }
    }

    /// Borrow the resolved entity name for this generated-query attempt.
    #[must_use]
    pub const fn entity_name(&self) -> &'static str {
        self.entity_name
    }

    /// Borrow the suggested deterministic order field for EXPLAIN rewrites.
    #[must_use]
    pub const fn explain_order_field(&self) -> Option<&'static str> {
        self.explain_order_field
    }

    /// Consume and return the generated-query dispatch result.
    pub fn into_result(self) -> Result<SqlDispatchResult, QueryError> {
        self.result
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::session::sql) enum SqlGroupingSurface {
    Scalar,
    Dispatch,
    GeneratedQuerySurface,
    Grouped,
}

const fn unsupported_sql_grouping_message(surface: SqlGroupingSurface) -> &'static str {
    match surface {
        SqlGroupingSurface::Scalar => {
            "execute_sql rejects grouped SELECT; use execute_sql_grouped(...)"
        }
        SqlGroupingSurface::Dispatch => {
            "execute_sql_dispatch rejects grouped SELECT execution; use execute_sql_grouped(...)"
        }
        SqlGroupingSurface::GeneratedQuerySurface => {
            "generated SQL query surface rejects grouped SELECT execution; use execute_sql_grouped(...)"
        }
        SqlGroupingSurface::Grouped => "execute_sql_grouped requires grouped SQL query intent",
    }
}

// Enforce the generated canister query contract that empty SQL is unsupported
// before any parser/lowering work occurs.
fn trim_generated_query_sql_input(sql: &str) -> Result<&str, QueryError> {
    let sql_trimmed = sql.trim();
    if sql_trimmed.is_empty() {
        return Err(QueryError::unsupported_query(
            "query endpoint requires a non-empty SQL string",
        ));
    }

    Ok(sql_trimmed)
}

// Render the generated-surface entity list from the descriptor table instead
// of assuming every session-visible entity belongs on the public query export.
fn generated_sql_entities(authorities: &[EntityAuthority]) -> Vec<String> {
    let mut entities = Vec::with_capacity(authorities.len());

    for authority in authorities {
        entities.push(authority.model().name().to_string());
    }

    entities
}

// Resolve one generated query route onto the descriptor-owned authority table.
fn authority_for_generated_sql_route(
    route: &SqlStatementRoute,
    authorities: &[EntityAuthority],
) -> Result<EntityAuthority, QueryError> {
    let sql_entity = route.entity();

    for authority in authorities {
        if identifiers_tail_match(sql_entity, authority.model().name()) {
            return Ok(*authority);
        }
    }

    Err(unsupported_generated_sql_entity_error(
        sql_entity,
        authorities,
    ))
}

// Keep the generated query-surface unsupported-entity contract stable while
// moving authority lookup out of the build-generated shim.
fn unsupported_generated_sql_entity_error(
    entity_name: &str,
    authorities: &[EntityAuthority],
) -> QueryError {
    let mut supported = String::new();

    for (index, authority) in authorities.iter().enumerate() {
        if index != 0 {
            supported.push_str(", ");
        }

        supported.push_str(authority.model().name());
    }

    QueryError::unsupported_query(format!(
        "query endpoint does not support entity '{entity_name}'; supported: {supported}"
    ))
}

impl<C: CanisterKind> DbSession<C> {
    // Execute one structural SQL load query and return only row-oriented SQL
    // projection values, keeping typed projection rows out of the shared SQL
    // query-lane path.
    fn execute_structural_sql_projection(
        &self,
        query: StructuralQuery,
        authority: EntityAuthority,
    ) -> Result<SqlProjectionPayload, QueryError> {
        // Phase 1: build the structural access plan once and reuse its
        // projection contract for both labels and row materialization.
        let visible_indexes =
            self.visible_indexes_for_store_model(authority.store_path(), authority.model())?;
        let plan = query.build_plan_with_visible_indexes(&visible_indexes)?;
        let projection = plan.projection_spec(authority.model());
        let columns = projection_labels_from_projection_spec(&projection);

        // Phase 2: execute the shared structural load path with the already
        // derived projection semantics.
        let projected = execute_sql_projection_rows_for_canister(
            &self.db,
            self.debug,
            authority.model(),
            projection,
            authority,
            plan,
        )
        .map_err(QueryError::execute)?;
        let (rows, row_count) = projected.into_parts();

        Ok(SqlProjectionPayload::new(columns, rows, row_count))
    }

    // Execute one structural SQL load query and return render-ready text rows
    // for the dispatch lane when the terminal short path can prove them
    // directly.
    fn execute_structural_sql_projection_text(
        &self,
        query: StructuralQuery,
        authority: EntityAuthority,
    ) -> Result<SqlDispatchResult, QueryError> {
        // Phase 1: build the structural access plan once and reuse its
        // projection contract for both labels and text-row materialization.
        let visible_indexes =
            self.visible_indexes_for_store_model(authority.store_path(), authority.model())?;
        let plan = query.build_plan_with_visible_indexes(&visible_indexes)?;
        let projection = plan.projection_spec(authority.model());
        let columns = projection_labels_from_projection_spec(&projection);

        // Phase 2: execute the shared structural load path with the already
        // derived projection semantics while preferring rendered SQL rows.
        let projected = execute_sql_projection_text_rows_for_canister(
            &self.db,
            self.debug,
            authority.model(),
            projection,
            authority,
            plan,
        )
        .map_err(QueryError::execute)?;
        let (rows, row_count) = projected.into_parts();

        Ok(SqlDispatchResult::ProjectionText {
            columns,
            rows,
            row_count,
        })
    }

    // Execute one typed SQL delete query while keeping the row payload on the
    // typed delete executor boundary that still owns non-runtime-hook delete
    // commit-window application.
    fn execute_typed_sql_delete<E>(&self, query: &Query<E>) -> Result<SqlDispatchResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let plan = self
            .compile_query_with_visible_indexes(query)?
            .into_executable();
        let deleted = self
            .with_metrics(|| self.delete_executor::<E>().execute_sql_projection(plan))
            .map_err(QueryError::execute)?;
        let (rows, row_count) = deleted.into_parts();
        let rows = sql_projection_rows_from_kernel_rows(rows);

        Ok(SqlProjectionPayload::new(
            projection_labels_from_entity_model(E::MODEL),
            rows,
            row_count,
        )
        .into_dispatch_result())
    }

    // Validate that one SQL-derived query intent matches the grouped/scalar
    // execution surface that is about to consume it.
    pub(in crate::db::session::sql) fn ensure_sql_query_grouping<E>(
        query: &Query<E>,
        surface: SqlGroupingSurface,
    ) -> Result<(), QueryError>
    where
        E: EntityKind,
    {
        match (surface, query.has_grouping()) {
            (
                SqlGroupingSurface::Scalar
                | SqlGroupingSurface::Dispatch
                | SqlGroupingSurface::GeneratedQuerySurface,
                false,
            )
            | (SqlGroupingSurface::Grouped, true) => Ok(()),
            (
                SqlGroupingSurface::Scalar
                | SqlGroupingSurface::Dispatch
                | SqlGroupingSurface::GeneratedQuerySurface,
                true,
            )
            | (SqlGroupingSurface::Grouped, false) => Err(QueryError::unsupported_query(
                unsupported_sql_grouping_message(surface),
            )),
        }
    }

    // Validate one lowered shared SQL query shape against the grouped/scalar
    // contract for surfaces that do not materialize a typed `Query<E>`.
    pub(in crate::db::session::sql) fn ensure_lowered_sql_query_grouping(
        lowered: &LoweredSqlCommand,
        surface: SqlGroupingSurface,
    ) -> Result<(), QueryError> {
        let Some(query) = lowered.query() else {
            return Ok(());
        };

        match (surface, query.has_grouping()) {
            (
                SqlGroupingSurface::Scalar
                | SqlGroupingSurface::Dispatch
                | SqlGroupingSurface::GeneratedQuerySurface,
                false,
            )
            | (SqlGroupingSurface::Grouped, true) => Ok(()),
            (
                SqlGroupingSurface::Scalar
                | SqlGroupingSurface::Dispatch
                | SqlGroupingSurface::GeneratedQuerySurface,
                true,
            )
            | (SqlGroupingSurface::Grouped, false) => Err(QueryError::unsupported_query(
                unsupported_sql_grouping_message(surface),
            )),
        }
    }

    /// Execute one reduced SQL statement into one unified SQL dispatch payload.
    pub fn execute_sql_dispatch<E>(&self, sql: &str) -> Result<SqlDispatchResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let parsed = self.parse_sql_statement(sql)?;

        self.execute_sql_dispatch_parsed::<E>(&parsed)
    }

    /// Execute one parsed reduced SQL statement into one unified SQL payload.
    pub fn execute_sql_dispatch_parsed<E>(
        &self,
        parsed: &SqlParsedStatement,
    ) -> Result<SqlDispatchResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        match parsed.route() {
            SqlStatementRoute::Query { .. } => {
                if parsed_requires_dedicated_sql_aggregate_lane(parsed) {
                    return Err(QueryError::unsupported_query(
                        unsupported_sql_aggregate_lane_message(
                            SqlAggregateSurface::ExecuteSqlDispatch,
                        ),
                    ));
                }

                if let Some(plan) =
                    computed_projection::computed_sql_projection_plan(&parsed.statement)?
                {
                    return self.execute_computed_sql_projection_dispatch::<E>(plan);
                }

                // Phase 1: keep typed dispatch on the shared lowered query lane
                // for plain `SELECT`, and only pay typed query binding on the
                // `DELETE` branch that still owns typed commit semantics.
                let lowered = parsed
                    .lower_query_lane_for_entity(E::MODEL.name(), E::MODEL.primary_key.name)?;

                Self::ensure_lowered_sql_query_grouping(&lowered, SqlGroupingSurface::Dispatch)?;

                // Phase 2: dispatch `SELECT` directly from the lowered shape so
                // typed SQL projection does not rebuild and discard a typed
                // `Query<E>` before returning to the structural executor path.
                match lowered.into_query() {
                    Some(LoweredSqlQuery::Select(select)) => self
                        .execute_lowered_sql_dispatch_select_core(
                            select,
                            EntityAuthority::for_type::<E>(),
                        ),
                    Some(LoweredSqlQuery::Delete(delete)) => {
                        let typed_query = bind_lowered_sql_query::<E>(
                            LoweredSqlQuery::Delete(delete),
                            MissingRowPolicy::Ignore,
                        )
                        .map_err(QueryError::from_sql_lowering_error)?;

                        self.execute_typed_sql_delete(&typed_query)
                    }
                    None => Err(QueryError::unsupported_query(
                        "execute_sql_dispatch accepts SELECT or DELETE only",
                    )),
                }
            }
            SqlStatementRoute::Explain { .. } => {
                if let Some((mode, plan)) =
                    computed_projection::computed_sql_projection_explain_plan(&parsed.statement)?
                {
                    return self
                        .explain_computed_sql_projection_dispatch::<E>(mode, plan)
                        .map(SqlDispatchResult::Explain);
                }

                let lowered = lower_sql_command_from_prepared_statement(
                    parsed.prepare(E::MODEL.name())?,
                    E::MODEL.primary_key.name,
                )
                .map_err(QueryError::from_sql_lowering_error)?;
                if let Some(explain) = self.explain_lowered_sql_execution_for_authority(
                    &lowered,
                    EntityAuthority::for_type::<E>(),
                )? {
                    return Ok(SqlDispatchResult::Explain(explain));
                }

                self.explain_lowered_sql_for_authority(&lowered, EntityAuthority::for_type::<E>())
                    .map(SqlDispatchResult::Explain)
            }
            SqlStatementRoute::Describe { .. } => {
                Ok(SqlDispatchResult::Describe(self.describe_entity::<E>()))
            }
            SqlStatementRoute::ShowIndexes { .. } => {
                Ok(SqlDispatchResult::ShowIndexes(self.show_indexes::<E>()))
            }
            SqlStatementRoute::ShowColumns { .. } => {
                Ok(SqlDispatchResult::ShowColumns(self.show_columns::<E>()))
            }
            SqlStatementRoute::ShowEntities => {
                Ok(SqlDispatchResult::ShowEntities(self.show_entities()))
            }
        }
    }

    /// Execute one parsed reduced SQL statement through the generated canister
    /// query/explain surface for one already-resolved dynamic authority.
    ///
    /// This keeps the canister SQL facade on the same reduced SQL ownership
    /// boundary as typed dispatch without forcing the outer facade to reopen
    /// typed-generic routing just to preserve parity for computed projections.
    #[doc(hidden)]
    pub fn execute_generated_query_surface_dispatch_for_authority(
        &self,
        parsed: &SqlParsedStatement,
        authority: EntityAuthority,
    ) -> Result<SqlDispatchResult, QueryError> {
        match parsed.route() {
            SqlStatementRoute::Query { .. } => {
                if parsed_requires_dedicated_sql_aggregate_lane(parsed) {
                    return Err(QueryError::unsupported_query(
                        unsupported_sql_aggregate_lane_message(
                            SqlAggregateSurface::GeneratedQuerySurface,
                        ),
                    ));
                }

                if let Some(plan) =
                    computed_projection::computed_sql_projection_plan(&parsed.statement)?
                {
                    return self
                        .execute_computed_sql_projection_dispatch_for_authority(plan, authority);
                }

                let lowered = parsed.lower_query_lane_for_entity(
                    authority.model().name(),
                    authority.model().primary_key.name,
                )?;

                Self::ensure_lowered_sql_query_grouping(
                    &lowered,
                    SqlGroupingSurface::GeneratedQuerySurface,
                )?;

                self.execute_lowered_sql_dispatch_query_for_authority(lowered, authority)
            }
            SqlStatementRoute::Explain { .. } => {
                if let Some((mode, plan)) =
                    computed_projection::computed_sql_projection_explain_plan(&parsed.statement)?
                {
                    return self
                        .explain_computed_sql_projection_dispatch_for_authority(
                            mode, plan, authority,
                        )
                        .map(SqlDispatchResult::Explain);
                }

                let lowered = parsed.lower_query_lane_for_entity(
                    authority.model().name(),
                    authority.model().primary_key.name,
                )?;
                if let Some(explain) =
                    self.explain_lowered_sql_execution_for_authority(&lowered, authority)?
                {
                    return Ok(SqlDispatchResult::Explain(explain));
                }

                self.explain_lowered_sql_for_authority(&lowered, authority)
                    .map(SqlDispatchResult::Explain)
            }
            SqlStatementRoute::Describe { .. }
            | SqlStatementRoute::ShowIndexes { .. }
            | SqlStatementRoute::ShowColumns { .. }
            | SqlStatementRoute::ShowEntities => Err(QueryError::unsupported_query(
                "generated SQL query surface requires query or EXPLAIN statement lanes",
            )),
        }
    }

    /// Execute one raw SQL string through the generated canister query surface.
    ///
    /// This hidden helper keeps parse, route, authority, and metadata/query
    /// dispatch ownership in core so the build-generated `sql_dispatch` shim
    /// stays close to a pure descriptor table plus public ABI wrapper.
    #[doc(hidden)]
    #[must_use]
    pub fn execute_generated_query_surface_sql(
        &self,
        sql: &str,
        authorities: &[EntityAuthority],
    ) -> GeneratedSqlDispatchAttempt {
        // Phase 1: normalize and parse once so every generated route family
        // shares the same SQL ownership boundary.
        let sql_trimmed = match trim_generated_query_sql_input(sql) {
            Ok(sql_trimmed) => sql_trimmed,
            Err(err) => return GeneratedSqlDispatchAttempt::new("", None, Err(err)),
        };
        let parsed = match self.parse_sql_statement(sql_trimmed) {
            Ok(parsed) => parsed,
            Err(err) => return GeneratedSqlDispatchAttempt::new("", None, Err(err)),
        };

        // Phase 2: keep SHOW ENTITIES descriptor-owned and resolve all other
        // generated routes against the emitted authority table exactly once.
        if matches!(parsed.route(), SqlStatementRoute::ShowEntities) {
            return GeneratedSqlDispatchAttempt::new(
                "",
                None,
                Ok(SqlDispatchResult::ShowEntities(generated_sql_entities(
                    authorities,
                ))),
            );
        }
        let authority = match authority_for_generated_sql_route(parsed.route(), authorities) {
            Ok(authority) => authority,
            Err(err) => return GeneratedSqlDispatchAttempt::new("", None, Err(err)),
        };

        // Phase 3: dispatch the resolved route through the existing query,
        // explain, and metadata helpers without rebuilding route ownership in
        // the generated build output.
        let entity_name = authority.model().name();
        let explain_order_field = parsed
            .route()
            .is_explain()
            .then_some(authority.model().primary_key.name);
        let result = match parsed.route() {
            SqlStatementRoute::Query { .. } | SqlStatementRoute::Explain { .. } => {
                self.execute_generated_query_surface_dispatch_for_authority(&parsed, authority)
            }
            SqlStatementRoute::Describe { .. } => Ok(SqlDispatchResult::Describe(
                self.describe_entity_model(authority.model()),
            )),
            SqlStatementRoute::ShowIndexes { .. } => Ok(SqlDispatchResult::ShowIndexes(
                self.show_indexes_for_store_model(authority.store_path(), authority.model()),
            )),
            SqlStatementRoute::ShowColumns { .. } => Ok(SqlDispatchResult::ShowColumns(
                self.show_columns_for_model(authority.model()),
            )),
            SqlStatementRoute::ShowEntities => unreachable!(
                "SHOW ENTITIES is handled before authority resolution for generated query dispatch"
            ),
        };

        GeneratedSqlDispatchAttempt::new(entity_name, explain_order_field, result)
    }
}

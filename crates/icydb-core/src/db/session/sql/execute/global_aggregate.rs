//! Module: db::session::sql::execute::global_aggregate
//! Responsibility: SQL global aggregate executor adaptation and response shaping.
//! Does not own: SQL aggregate semantic lowering, HAVING evaluation, projection evaluation, or reducers.
//! Boundary: adapts lowered SQL aggregate intent onto executor-owned structural aggregate execution.

use crate::{
    db::{
        DbSession, PersistedRow, Query, QueryError,
        executor::{
            StructuralAggregateRequest, StructuralAggregateTerminal,
            StructuralAggregateTerminalKind,
        },
        query::plan::AggregateKind,
        session::sql::{
            SqlCacheAttribution, SqlStatementResult,
            projection::{
                projection_fixed_scales_from_projection_spec,
                projection_labels_from_projection_spec,
                sql_projection_statement_result_from_value_rows,
            },
        },
        sql::lowering::{
            PreparedSqlScalarAggregatePlanFragment, PreparedSqlScalarAggregateStrategy,
            SqlGlobalAggregateCommandCore,
        },
    },
    traits::{CanisterKind, EntityValue},
};

// Convert one prepared SQL aggregate strategy into the executor terminal DTO at
// the session boundary so SQL lowering stays executor-neutral.
fn build_structural_aggregate_terminal_from_sql_strategy(
    strategy: PreparedSqlScalarAggregateStrategy,
) -> Result<StructuralAggregateTerminal, &'static str> {
    let (descriptor, target_slot, input_expr, filter_expr, distinct_input) =
        strategy.into_aggregate_plan_parts();

    let kind = match descriptor {
        PreparedSqlScalarAggregatePlanFragment::CountRows => {
            StructuralAggregateTerminalKind::CountRows
        }
        PreparedSqlScalarAggregatePlanFragment::CountField => {
            StructuralAggregateTerminalKind::CountValues
        }
        PreparedSqlScalarAggregatePlanFragment::NumericField {
            kind: AggregateKind::Sum,
        } => StructuralAggregateTerminalKind::Sum,
        PreparedSqlScalarAggregatePlanFragment::NumericField {
            kind: AggregateKind::Avg,
        } => StructuralAggregateTerminalKind::Avg,
        PreparedSqlScalarAggregatePlanFragment::ExtremalWinnerField {
            kind: AggregateKind::Min,
        } => StructuralAggregateTerminalKind::Min,
        PreparedSqlScalarAggregatePlanFragment::ExtremalWinnerField {
            kind: AggregateKind::Max,
        } => StructuralAggregateTerminalKind::Max,
        PreparedSqlScalarAggregatePlanFragment::NumericField { .. }
        | PreparedSqlScalarAggregatePlanFragment::ExtremalWinnerField { .. } => {
            return Err("prepared SQL scalar aggregate strategy drifted outside SQL support");
        }
    };

    Ok(StructuralAggregateTerminal::new(
        kind,
        target_slot,
        input_expr,
        filter_expr,
        distinct_input,
    ))
}

impl<C: CanisterKind> DbSession<C> {
    // Execute one prepared SQL aggregate command through executor-owned
    // structural aggregate execution, then shape the completed rows into the
    // SQL projection payload.
    pub(in crate::db::session::sql::execute) fn execute_global_aggregate_statement<E>(
        &self,
        command: SqlGlobalAggregateCommandCore,
    ) -> Result<(SqlStatementResult, SqlCacheAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (query, strategies, projection, having) = command.into_execution_parts();
        let columns = projection_labels_from_projection_spec(&projection);
        let fixed_scales = projection_fixed_scales_from_projection_spec(&projection);
        let schema_info = self
            .accepted_schema_info_for_entity::<E>()
            .map_err(QueryError::execute)?;
        let terminals = strategies
            .into_iter()
            .map(|strategy| {
                build_structural_aggregate_terminal_from_sql_strategy(strategy)
                    .map_err(QueryError::invariant)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let request = StructuralAggregateRequest::new(terminals, projection, having, schema_info);
        let query = Query::<E>::from_inner(query);
        let (prepared_plan, cache_attribution) =
            self.cached_shared_query_plan_for_entity::<E>(&query)?;
        let result = self
            .with_metrics(|| {
                self.load_executor::<E>()
                    .execute_structural_aggregate_result(&prepared_plan, request)
            })
            .map_err(QueryError::execute)?;
        let rows = result.into_value_rows();
        let row_count = u32::try_from(rows.len()).unwrap_or(u32::MAX);

        Ok((
            sql_projection_statement_result_from_value_rows(columns, fixed_scales, rows, row_count),
            SqlCacheAttribution::from_shared_query_plan_cache(cache_attribution),
        ))
    }
}

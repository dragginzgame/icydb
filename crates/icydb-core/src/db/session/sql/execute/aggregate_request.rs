//! Module: db::session::sql::execute::aggregate_request
//! Responsibility: adapt lowered SQL global aggregate terminals to executor requests.
//! Does not own: plan resolution, direct count fast paths, or aggregate execution.
//! Boundary: keeps SQL-to-executor aggregate DTO construction out of execution orchestration.

use crate::db::{
    QueryError,
    executor::{
        StructuralAggregateRequest, StructuralAggregateTerminal, StructuralAggregateTerminalKind,
    },
    query::plan::AggregateKind,
    schema::SchemaInfo,
    session::sql::{SqlProjectionContract, projection::projection_contract_from_projection_spec},
    sql::lowering::{
        PreparedSqlScalarAggregatePlanFragment, PreparedSqlScalarAggregateStrategy,
        SqlGlobalAggregateCommand,
    },
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SqlAggregateTerminalBuildError {
    UnsupportedStrategyDrift,
}

pub(super) struct PreparedAggregateRequestBundle {
    request: StructuralAggregateRequest,
    projection: SqlProjectionContract,
}

impl PreparedAggregateRequestBundle {
    pub(super) fn from_global_command(
        command: &SqlGlobalAggregateCommand,
        schema_info: SchemaInfo,
    ) -> Result<Self, QueryError> {
        let projection = command.projection();
        let terminals = command
            .strategies()
            .iter()
            .cloned()
            .map(|strategy| {
                build_structural_aggregate_terminal_from_sql_strategy(strategy)
                    .map_err(|_err| QueryError::invariant())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let request = StructuralAggregateRequest::new(
            terminals,
            projection.clone(),
            command.having().cloned(),
            schema_info,
        );

        Ok(Self {
            request,
            projection: projection_contract_from_projection_spec(projection),
        })
    }

    pub(super) fn into_parts(self) -> (StructuralAggregateRequest, SqlProjectionContract) {
        let Self {
            request,
            projection,
        } = self;

        (request, projection)
    }
}

// Convert one prepared SQL aggregate strategy into the executor terminal DTO at
// the session boundary so SQL lowering stays executor-neutral.
fn build_structural_aggregate_terminal_from_sql_strategy(
    strategy: PreparedSqlScalarAggregateStrategy,
) -> Result<StructuralAggregateTerminal, SqlAggregateTerminalBuildError> {
    let (descriptor, target_slot, input_expr, filter_expr, distinct_input) =
        strategy.into_structural_terminal_inputs();

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
            return Err(SqlAggregateTerminalBuildError::UnsupportedStrategyDrift);
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

//! Module: db::sql::lowering
//! Responsibility: reduced SQL statement lowering into canonical query intent.
//! Does not own: SQL tokenization/parsing, planner validation policy, or executor semantics.
//! Boundary: frontend-only translation from parsed SQL statement contracts to `Query<E>`.

mod aggregate;
mod analysis;
mod expr;
mod normalize;
#[cfg(test)]
mod order_expr;
mod predicate;
mod prepare;
mod select;

///
/// TESTS
///

#[cfg(test)]
mod tests;

use crate::db::{
    query::intent::QueryError,
    sql::parser::{SqlExplainMode, SqlParseError, SqlStatement},
};
#[cfg(test)]
use crate::{
    db::{predicate::MissingRowPolicy, query::intent::Query},
    traits::EntityKind,
};
use icydb_diagnostic_code::SqlLoweringCode;

///
/// SqlParameterPlacementReason
///
/// Compact reason for unsupported SQL parameter placement diagnostics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlParameterPlacementReason {
    BindingUnsupported,
    UnboundExpressionLowering,
}

pub(in crate::db::sql::lowering) use aggregate::LoweredSqlGlobalAggregateCommand;
pub(in crate::db) use aggregate::compile_sql_global_aggregate_command_from_prepared_with_schema;
pub(crate) use aggregate::{
    PreparedSqlScalarAggregatePlanFragment, PreparedSqlScalarAggregateStrategy,
};
pub(crate) use aggregate::{
    SqlGlobalAggregateCommand, bind_lowered_sql_explain_global_aggregate_with_schema,
};
#[cfg(test)]
pub(crate) use aggregate::{
    TypedSqlGlobalAggregateCommand, compile_sql_global_aggregate_command_for_model_only,
};
pub(in crate::db::sql::lowering) use analysis::{
    AnalyzedLoweredExpr, LoweredExprAnalysis, LoweredExprSourceRef, analyze_lowered_expr,
};
#[cfg(test)]
pub(in crate::db::sql::lowering) use order_expr::{
    lower_grouped_post_aggregate_order_expr_text, lower_supported_order_expr_text,
};
pub(in crate::db) use prepare::bind_sql_select_statement_structural_with_schema;
#[cfg(test)]
pub(crate) use prepare::lower_sql_command_from_prepared_statement_for_model_only;
pub(crate) use prepare::{
    extract_prepared_sql_insert_statement, extract_prepared_sql_update_statement,
    lower_prepared_sql_delete_statement, lower_prepared_sql_select_statement_with_schema,
    lower_sql_command_from_prepared_statement_with_schema, prepare_sql_statement,
};
pub(crate) use select::LoweredDeleteShape;
pub(in crate::db::sql::lowering) use select::LoweredSqlFilter;
#[cfg(test)]
pub(in crate::db::sql::lowering) use select::apply_lowered_base_query_shape_for_model_only;
pub(in crate::db::sql::lowering) use select::apply_lowered_base_query_shape_with_schema;
#[cfg(test)]
pub(in crate::db) use select::apply_lowered_select_shape_for_model_only;
#[cfg(test)]
pub(in crate::db) use select::bind_lowered_sql_query_for_model_only;
pub(in crate::db::sql::lowering) use select::validate_base_query_sql_capabilities;
pub(crate) use select::{LoweredBaseQueryShape, LoweredSelectShape};
pub(in crate::db) use select::{
    bind_lowered_sql_delete_query_structural_with_schema,
    bind_lowered_sql_query_structural_with_schema,
    bind_lowered_sql_select_query_structural_with_schema,
    bind_sql_delete_statement_structural_with_schema,
    bind_sql_update_selector_query_structural_with_schema,
};

///
/// LoweredSqlCommand
///
/// Generic-free SQL command shape after reduced SQL parsing and entity-route
/// normalization.
/// This keeps statement-shape lowering shared across entities before typed
/// `Query<E>` binding happens at the execution boundary.
///
#[derive(Clone, Debug)]
pub struct LoweredSqlCommand(pub(in crate::db::sql::lowering) LoweredSqlCommandInner);

#[derive(Clone, Debug)]
#[cfg_attr(not(test), expect(dead_code))]
pub(in crate::db::sql::lowering) enum LoweredSqlCommandInner {
    Query(LoweredSqlQuery),
    Explain {
        mode: SqlExplainMode,
        verbose: bool,
        query: LoweredSqlQuery,
    },
    ExplainGlobalAggregate {
        mode: SqlExplainMode,
        verbose: bool,
        command: LoweredSqlGlobalAggregateCommand,
    },
    DescribeEntity,
    ShowIndexesEntity,
    ShowColumnsEntity,
    ShowEntities,
    ShowStores,
    ShowMemory,
}

///
/// SqlCommand
///
/// Test-only typed SQL command shell over the shared lowered SQL surface.
/// Runtime dispatch now consumes `LoweredSqlCommand` directly, but lowering
/// tests still validate typed binding behavior on this local envelope.
///
#[cfg(test)]
#[derive(Debug)]
pub(crate) enum SqlCommand<E: EntityKind> {
    Query(Query<E>),
    GlobalAggregate(TypedSqlGlobalAggregateCommand<E>),
    Explain {
        mode: SqlExplainMode,
        verbose: bool,
        query: Query<E>,
    },
    ExplainGlobalAggregate {
        mode: SqlExplainMode,
        verbose: bool,
        command: TypedSqlGlobalAggregateCommand<E>,
    },
    DescribeEntity,
    ShowIndexesEntity,
    ShowColumnsEntity,
    ShowEntities,
    ShowStores,
    ShowMemory,
}

impl LoweredSqlCommand {
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn query(&self) -> Option<&LoweredSqlQuery> {
        match &self.0 {
            LoweredSqlCommandInner::Query(query) => Some(query),
            LoweredSqlCommandInner::Explain { .. }
            | LoweredSqlCommandInner::ExplainGlobalAggregate { .. }
            | LoweredSqlCommandInner::DescribeEntity
            | LoweredSqlCommandInner::ShowIndexesEntity
            | LoweredSqlCommandInner::ShowColumnsEntity
            | LoweredSqlCommandInner::ShowEntities
            | LoweredSqlCommandInner::ShowStores
            | LoweredSqlCommandInner::ShowMemory => None,
        }
    }

    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn into_query(self) -> Option<LoweredSqlQuery> {
        match self.0 {
            LoweredSqlCommandInner::Query(query) => Some(query),
            LoweredSqlCommandInner::Explain { .. }
            | LoweredSqlCommandInner::ExplainGlobalAggregate { .. }
            | LoweredSqlCommandInner::DescribeEntity
            | LoweredSqlCommandInner::ShowIndexesEntity
            | LoweredSqlCommandInner::ShowColumnsEntity
            | LoweredSqlCommandInner::ShowEntities
            | LoweredSqlCommandInner::ShowStores
            | LoweredSqlCommandInner::ShowMemory => None,
        }
    }

    #[must_use]
    pub(in crate::db) const fn explain_query(
        &self,
    ) -> Option<(SqlExplainMode, bool, &LoweredSqlQuery)> {
        match &self.0 {
            LoweredSqlCommandInner::Explain {
                mode,
                verbose,
                query,
            } => Some((*mode, *verbose, query)),
            LoweredSqlCommandInner::Query(_)
            | LoweredSqlCommandInner::ExplainGlobalAggregate { .. }
            | LoweredSqlCommandInner::DescribeEntity
            | LoweredSqlCommandInner::ShowIndexesEntity
            | LoweredSqlCommandInner::ShowColumnsEntity
            | LoweredSqlCommandInner::ShowEntities
            | LoweredSqlCommandInner::ShowStores
            | LoweredSqlCommandInner::ShowMemory => None,
        }
    }
}

///
/// LoweredSqlQuery
///
/// Generic-free executable SQL query shape prepared before typed query binding.
/// Select and delete lowering stay shared until the final `Query<E>` build.
///
#[derive(Clone, Debug)]
pub(crate) enum LoweredSqlQuery {
    Select(LoweredSelectShape),
    Delete(LoweredBaseQueryShape),
}

///
/// SqlLoweringError
///
/// SQL frontend lowering failures before planner validation/execution.
///
#[derive(Debug)]
pub(crate) enum SqlLoweringError {
    Parse(SqlParseError),

    Query(Box<QueryError>),

    EntityMismatch {
        sql_entity: String,
        expected_entity: String,
    },

    UnsupportedSelectProjection,

    UnsupportedSelectDistinct,

    DistinctOrderByRequiresProjectedTuple,

    UnsupportedGlobalAggregateProjection,

    GlobalAggregateDoesNotSupportGroupBy,

    UnsupportedSelectGroupBy,

    GroupedProjectionRequiresExplicitList,

    GroupedProjectionRequiresAggregate,

    GroupedProjectionReferencesNonGroupField {
        index: usize,
    },

    GroupedProjectionScalarAfterAggregate {
        index: usize,
    },

    HavingRequiresGroupBy,

    UnsupportedSelectHaving,

    UnsupportedAggregateInputExpressions,

    UnsupportedWhereExpression,

    UnknownField {
        field: String,
    },

    UnsupportedParameterPlacement {
        index: Option<usize>,
        reason: SqlParameterPlacementReason,
    },

    UnsupportedSqlDdl,

    UnexpectedQueryLaneStatement,
}

impl SqlLoweringError {
    /// Construct one entity-mismatch SQL lowering error.
    fn entity_mismatch(sql_entity: impl Into<String>, expected_entity: impl Into<String>) -> Self {
        Self::EntityMismatch {
            sql_entity: sql_entity.into(),
            expected_entity: expected_entity.into(),
        }
    }

    /// Construct one unsupported SELECT projection SQL lowering error.
    const fn unsupported_select_projection() -> Self {
        Self::UnsupportedSelectProjection
    }

    /// Construct one query-lane lowering misuse error.
    pub(crate) const fn unexpected_query_lane_statement() -> Self {
        Self::UnexpectedQueryLaneStatement
    }

    /// Construct one unsupported SELECT DISTINCT SQL lowering error.
    const fn unsupported_select_distinct() -> Self {
        Self::UnsupportedSelectDistinct
    }

    /// Construct one DISTINCT ORDER BY projection-derivability SQL lowering error.
    const fn distinct_order_by_requires_projected_tuple() -> Self {
        Self::DistinctOrderByRequiresProjectedTuple
    }

    /// Construct one unsupported global aggregate projection SQL lowering error.
    const fn unsupported_global_aggregate_projection() -> Self {
        Self::UnsupportedGlobalAggregateProjection
    }

    /// Construct one unsupported SQL WHERE expression lowering error.
    pub(crate) const fn unsupported_where_expression() -> Self {
        Self::UnsupportedWhereExpression
    }

    /// Construct one global-aggregate-GROUP-BY SQL lowering error.
    const fn global_aggregate_does_not_support_group_by() -> Self {
        Self::GlobalAggregateDoesNotSupportGroupBy
    }

    /// Construct one unsupported SELECT GROUP BY shape SQL lowering error.
    const fn unsupported_select_group_by() -> Self {
        Self::UnsupportedSelectGroupBy
    }

    /// Construct one grouped-projection-explicit-list SQL lowering error.
    const fn grouped_projection_requires_explicit_list() -> Self {
        Self::GroupedProjectionRequiresExplicitList
    }

    /// Construct one grouped-projection-missing-aggregate SQL lowering error.
    const fn grouped_projection_requires_aggregate() -> Self {
        Self::GroupedProjectionRequiresAggregate
    }

    /// Construct one grouped projection non-group-field SQL lowering error.
    const fn grouped_projection_references_non_group_field(index: usize) -> Self {
        Self::GroupedProjectionReferencesNonGroupField { index }
    }

    /// Construct one grouped projection scalar-after-aggregate SQL lowering error.
    const fn grouped_projection_scalar_after_aggregate(index: usize) -> Self {
        Self::GroupedProjectionScalarAfterAggregate { index }
    }

    /// Construct one HAVING-requires-GROUP-BY SQL lowering error.
    const fn having_requires_group_by() -> Self {
        Self::HavingRequiresGroupBy
    }

    /// Construct one unsupported SELECT HAVING shape SQL lowering error.
    const fn unsupported_select_having() -> Self {
        Self::UnsupportedSelectHaving
    }

    /// Construct one aggregate-input execution seam SQL lowering error.
    const fn unsupported_aggregate_input_expressions() -> Self {
        Self::UnsupportedAggregateInputExpressions
    }

    /// Construct one unknown-field SQL lowering error.
    pub(crate) fn unknown_field(field: impl Into<String>) -> Self {
        Self::UnknownField {
            field: field.into(),
        }
    }

    /// Construct one unsupported parameter placement SQL lowering error.
    pub(crate) const fn unsupported_parameter_placement(
        index: Option<usize>,
        reason: SqlParameterPlacementReason,
    ) -> Self {
        Self::UnsupportedParameterPlacement { index, reason }
    }

    /// Construct one unsupported SQL DDL lowering error.
    pub(crate) const fn unsupported_sql_ddl() -> Self {
        Self::UnsupportedSqlDdl
    }

    /// Return the compact public diagnostic reason for lowering failures that
    /// do not need dynamic message payloads at the public boundary.
    pub(crate) const fn compact_diagnostic_code(&self) -> Option<SqlLoweringCode> {
        match self {
            Self::EntityMismatch {
                sql_entity,
                expected_entity,
            } => {
                let _ = (sql_entity, expected_entity);
                Some(SqlLoweringCode::EntityMismatch)
            }
            Self::UnsupportedSelectProjection => Some(SqlLoweringCode::SelectProjectionShape),
            Self::UnsupportedSelectDistinct => Some(SqlLoweringCode::SelectDistinct),
            Self::DistinctOrderByRequiresProjectedTuple => {
                Some(SqlLoweringCode::DistinctOrderByProjection)
            }
            Self::UnsupportedGlobalAggregateProjection => {
                Some(SqlLoweringCode::GlobalAggregateProjection)
            }
            Self::GlobalAggregateDoesNotSupportGroupBy => {
                Some(SqlLoweringCode::GlobalAggregateGroupBy)
            }
            Self::UnsupportedSelectGroupBy => Some(SqlLoweringCode::SelectGroupByShape),
            Self::GroupedProjectionRequiresExplicitList => {
                Some(SqlLoweringCode::GroupedProjectionExplicitListRequired)
            }
            Self::GroupedProjectionRequiresAggregate => {
                Some(SqlLoweringCode::GroupedProjectionAggregateRequired)
            }
            Self::GroupedProjectionReferencesNonGroupField { index } => {
                let _ = index;
                Some(SqlLoweringCode::GroupedProjectionNonGroupField)
            }
            Self::GroupedProjectionScalarAfterAggregate { index } => {
                let _ = index;
                Some(SqlLoweringCode::GroupedProjectionScalarAfterAggregate)
            }
            Self::HavingRequiresGroupBy => Some(SqlLoweringCode::HavingRequiresGroupBy),
            Self::UnsupportedSelectHaving => Some(SqlLoweringCode::SelectHavingShape),
            Self::UnsupportedAggregateInputExpressions => {
                Some(SqlLoweringCode::AggregateInputExpressions)
            }
            Self::UnsupportedWhereExpression => Some(SqlLoweringCode::WhereExpressionShape),
            Self::UnsupportedParameterPlacement { index, reason } => {
                let _ = (index, reason);
                Some(SqlLoweringCode::ParameterPlacement)
            }
            Self::UnsupportedSqlDdl => Some(SqlLoweringCode::SqlDdlExecutionUnsupported),
            Self::Parse(_)
            | Self::Query(_)
            | Self::UnknownField { .. }
            | Self::UnexpectedQueryLaneStatement => None,
        }
    }
}

impl From<QueryError> for SqlLoweringError {
    fn from(value: QueryError) -> Self {
        Self::Query(Box::new(value))
    }
}

impl From<SqlParseError> for SqlLoweringError {
    fn from(value: SqlParseError) -> Self {
        Self::Parse(value)
    }
}

///
/// PreparedSqlStatement
///
/// SQL statement envelope after entity-scope normalization and
/// entity-match validation for one target entity descriptor.
///
/// This pre-lowering contract is entity-agnostic and reusable across
/// dynamic SQL route branches before typed `Query<E>` binding.
///
#[derive(Clone, Debug)]
pub(crate) struct PreparedSqlStatement {
    pub(in crate::db::sql::lowering) statement: SqlStatement,
}

impl PreparedSqlStatement {
    /// Borrow one prepared SQL statement in its normalized parsed form.
    #[must_use]
    pub(in crate::db) const fn statement(&self) -> &SqlStatement {
        &self.statement
    }

    /// Consume one prepared SQL statement back into its normalized parsed form.
    #[must_use]
    pub(in crate::db) fn into_statement(self) -> SqlStatement {
        self.statement
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LoweredSqlLaneKind {
    Query,
    Explain,
    Describe,
    ShowIndexes,
    ShowColumns,
    ShowEntities,
    ShowStores,
    ShowMemory,
}

/// Parse and lower one SQL statement into canonical query intent for `E`.
#[cfg(test)]
pub(crate) fn compile_sql_command<E: EntityKind>(
    sql: &str,
    consistency: MissingRowPolicy,
) -> Result<SqlCommand<E>, SqlLoweringError> {
    let statement = crate::db::sql::parser::parse_sql(sql)?;
    let prepared = prepare_sql_statement(&statement, E::MODEL.name())?;

    if prepared.statement().is_global_aggregate_lane_shape() {
        return Ok(SqlCommand::GlobalAggregate(
            aggregate::compile_sql_global_aggregate_command_from_prepared_for_model_only::<E>(
                prepared,
                consistency,
            )?,
        ));
    }

    let lowered = lower_sql_command_from_prepared_statement_for_model_only(prepared, E::MODEL)?;

    // Keep the test-only typed envelope local to the single public test entry
    // point instead of preserving a private forwarding chain.
    match lowered.0 {
        LoweredSqlCommandInner::Query(query) => Ok(SqlCommand::Query(
            bind_lowered_sql_query_for_model_only::<E>(query, consistency)?,
        )),
        LoweredSqlCommandInner::ExplainGlobalAggregate {
            mode,
            verbose,
            command,
        } => Ok(SqlCommand::ExplainGlobalAggregate {
            mode,
            verbose,
            command: aggregate::bind_lowered_sql_global_aggregate_command_for_model_only::<E>(
                command,
                consistency,
            )?,
        }),
        LoweredSqlCommandInner::Explain {
            mode,
            verbose,
            query,
        } => Ok(SqlCommand::Explain {
            mode,
            verbose,
            query: bind_lowered_sql_query_for_model_only::<E>(query, consistency)?,
        }),
        LoweredSqlCommandInner::DescribeEntity => Ok(SqlCommand::DescribeEntity),
        LoweredSqlCommandInner::ShowIndexesEntity => Ok(SqlCommand::ShowIndexesEntity),
        LoweredSqlCommandInner::ShowColumnsEntity => Ok(SqlCommand::ShowColumnsEntity),
        LoweredSqlCommandInner::ShowEntities => Ok(SqlCommand::ShowEntities),
        LoweredSqlCommandInner::ShowStores => Ok(SqlCommand::ShowStores),
        LoweredSqlCommandInner::ShowMemory => Ok(SqlCommand::ShowMemory),
    }
}

pub(crate) const fn lowered_sql_command_lane(command: &LoweredSqlCommand) -> LoweredSqlLaneKind {
    match command.0 {
        LoweredSqlCommandInner::Query(_) => LoweredSqlLaneKind::Query,
        LoweredSqlCommandInner::Explain { .. }
        | LoweredSqlCommandInner::ExplainGlobalAggregate { .. } => LoweredSqlLaneKind::Explain,
        LoweredSqlCommandInner::DescribeEntity => LoweredSqlLaneKind::Describe,
        LoweredSqlCommandInner::ShowIndexesEntity => LoweredSqlLaneKind::ShowIndexes,
        LoweredSqlCommandInner::ShowColumnsEntity => LoweredSqlLaneKind::ShowColumns,
        LoweredSqlCommandInner::ShowEntities => LoweredSqlLaneKind::ShowEntities,
        LoweredSqlCommandInner::ShowStores => LoweredSqlLaneKind::ShowStores,
        LoweredSqlCommandInner::ShowMemory => LoweredSqlLaneKind::ShowMemory,
    }
}

//! Module: db::sql::lowering
//! Responsibility: reduced SQL statement lowering into canonical query intent.
//! Does not own: SQL tokenization/parsing, planner validation policy, or executor semantics.
//! Boundary: frontend-only translation from parsed SQL statement contracts to
//! accepted-schema-owned structural query intent.

mod aggregate;
mod analysis;
mod ast_depth;
mod expr;
mod normalize;
mod predicate;
mod prepare;
mod select;

///
/// TESTS
///

#[cfg(feature = "sql")]
use crate::db::sql::parser::SqlExplainMode;
use crate::db::{
    query::intent::QueryError,
    sql::parser::{SqlParseError, SqlStatement},
};
use icydb_diagnostic_code::{DiagnosticFactTag, QueryFieldRole, SqlLoweringCode};

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

#[cfg(feature = "sql")]
pub(in crate::db::sql::lowering) use aggregate::LoweredSqlGlobalAggregateCommand;
pub(crate) use aggregate::SqlGlobalAggregateCommand;
#[cfg(feature = "sql")]
pub(crate) use aggregate::bind_lowered_sql_explain_global_aggregate_with_schema;
pub(in crate::db) use aggregate::compile_sql_global_aggregate_command_from_prepared_with_schema;
pub(crate) use aggregate::{
    PreparedSqlScalarAggregatePlanFragment, PreparedSqlScalarAggregateStrategy,
};
pub(in crate::db::sql::lowering) use analysis::{
    AnalyzedLoweredExpr, LoweredExprAnalysis, LoweredExprSourceRef, analyze_lowered_expr,
};
pub(in crate::db) use prepare::bind_sql_select_statement_structural_with_schema;
#[cfg(feature = "sql")]
pub(crate) use prepare::lower_sql_explain_command_from_prepared_statement_with_schema;
pub(crate) use prepare::{
    extract_prepared_sql_insert_statement, extract_prepared_sql_update_statement,
    lower_prepared_sql_delete_statement, lower_prepared_sql_select_statement_with_schema,
    prepare_sql_statement,
};
pub(crate) use select::LoweredDeleteShape;
pub(in crate::db::sql::lowering) use select::LoweredSqlFilter;
pub(in crate::db::sql::lowering) use select::apply_lowered_base_query_shape_with_schema;
#[cfg(feature = "sql")]
pub(in crate::db) use select::bind_lowered_sql_query_structural_with_schema;
pub(in crate::db::sql::lowering) use select::validate_base_query_sql_capabilities;
pub(crate) use select::{LoweredBaseQueryShape, LoweredSelectShape};
pub(in crate::db) use select::{
    bind_lowered_sql_delete_query_structural_with_schema,
    bind_lowered_sql_select_query_structural_with_schema,
    bind_sql_delete_statement_structural_with_schema,
    bind_sql_update_selector_query_structural_with_schema,
};

///
/// LoweredSqlCommand
///
/// Generic-free SQL command shape after reduced SQL parsing and entity-route
/// normalization.
/// This keeps statement-shape lowering shared across entities before accepted
/// schema binding happens at the execution boundary.
///
#[derive(Clone, Debug)]
pub struct LoweredSqlCommand(pub(in crate::db::sql::lowering) LoweredSqlCommandInner);

#[derive(Clone, Debug)]
pub(in crate::db::sql::lowering) enum LoweredSqlCommandInner {
    #[cfg(feature = "sql")]
    Explain {
        mode: SqlExplainMode,
        verbose: bool,
        query: Box<LoweredSqlQuery>,
    },
    #[cfg(feature = "sql")]
    ExplainGlobalAggregate {
        mode: SqlExplainMode,
        verbose: bool,
        command: Box<LoweredSqlGlobalAggregateCommand>,
    },
}

impl LoweredSqlCommand {
    #[cfg(feature = "sql")]
    #[must_use]
    pub(in crate::db) const fn is_explain_lane(&self) -> bool {
        matches!(
            self.0,
            LoweredSqlCommandInner::Explain { .. }
                | LoweredSqlCommandInner::ExplainGlobalAggregate { .. }
        )
    }

    #[cfg(feature = "sql")]
    #[must_use]
    pub(in crate::db) fn explain_query(&self) -> Option<(SqlExplainMode, bool, &LoweredSqlQuery)> {
        match &self.0 {
            LoweredSqlCommandInner::Explain {
                mode,
                verbose,
                query,
            } => Some((*mode, *verbose, query.as_ref())),
            LoweredSqlCommandInner::ExplainGlobalAggregate { .. } => None,
        }
    }
}

///
/// LoweredSqlQuery
///
/// Generic-free executable SQL query shape prepared for accepted-schema-owned
/// explain planning.
///
#[cfg(feature = "sql")]
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
        role: QueryFieldRole,
        field: String,
    },

    UnsupportedParameterPlacement {
        index: Option<usize>,
        reason: SqlParameterPlacementReason,
    },

    UnsupportedSqlDdl,

    #[cfg(feature = "sql")]
    UnexpectedQueryLaneStatement,
}

impl SqlLoweringError {
    /// Project retained SQL positions into production-safe numeric facts.
    pub(crate) fn diagnostic_facts(&self) -> Vec<(DiagnosticFactTag, u64)> {
        match self {
            Self::GroupedProjectionReferencesNonGroupField { index }
            | Self::GroupedProjectionScalarAfterAggregate { index } => {
                vec![(DiagnosticFactTag::ProjectionIndex, *index as u64)]
            }
            Self::UnsupportedParameterPlacement {
                index: Some(index), ..
            } => vec![(DiagnosticFactTag::ParameterIndex, *index as u64)],
            Self::Parse(_)
            | Self::Query(_)
            | Self::EntityMismatch { .. }
            | Self::UnsupportedSelectProjection
            | Self::UnsupportedSelectDistinct
            | Self::DistinctOrderByRequiresProjectedTuple
            | Self::UnsupportedGlobalAggregateProjection
            | Self::GlobalAggregateDoesNotSupportGroupBy
            | Self::UnsupportedSelectGroupBy
            | Self::GroupedProjectionRequiresExplicitList
            | Self::GroupedProjectionRequiresAggregate
            | Self::HavingRequiresGroupBy
            | Self::UnsupportedSelectHaving
            | Self::UnsupportedAggregateInputExpressions
            | Self::UnsupportedWhereExpression
            | Self::UnknownField { .. }
            | Self::UnsupportedParameterPlacement { index: None, .. }
            | Self::UnsupportedSqlDdl => Vec::new(),
            #[cfg(feature = "sql")]
            Self::UnexpectedQueryLaneStatement => Vec::new(),
        }
    }

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

    #[cfg(feature = "sql")]
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
    pub(crate) fn unknown_field(role: QueryFieldRole, field: impl Into<String>) -> Self {
        Self::UnknownField {
            role,
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
            Self::Parse(_) | Self::Query(_) | Self::UnknownField { .. } => None,
            #[cfg(feature = "sql")]
            Self::UnexpectedQueryLaneStatement => None,
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
/// This pre-lowering contract is entity-agnostic and reusable across dynamic
/// SQL route branches before accepted-schema structural binding.
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

#[cfg(all(test, feature = "sql"))]
mod tests {
    use super::{SqlLoweringError, SqlParameterPlacementReason};
    use crate::db::QueryError;
    use icydb_diagnostic_code::DiagnosticFactTag;

    #[test]
    fn lowering_projection_error_retains_projection_index() {
        let error = SqlLoweringError::grouped_projection_references_non_group_field(4);
        assert_eq!(
            error.diagnostic_facts(),
            vec![(DiagnosticFactTag::ProjectionIndex, 4)],
        );

        let query_error = QueryError::from_sql_lowering_error(error);
        assert_eq!(
            query_error.diagnostic_facts(),
            vec![(DiagnosticFactTag::ProjectionIndex, 4)],
        );
    }

    #[test]
    fn lowering_parameter_error_retains_parameter_index_only_when_known() {
        let known = SqlLoweringError::unsupported_parameter_placement(
            Some(3),
            SqlParameterPlacementReason::BindingUnsupported,
        );
        assert_eq!(
            known.diagnostic_facts(),
            vec![(DiagnosticFactTag::ParameterIndex, 3)],
        );

        let unknown = SqlLoweringError::unsupported_parameter_placement(
            None,
            SqlParameterPlacementReason::UnboundExpressionLowering,
        );
        assert!(unknown.diagnostic_facts().is_empty());
    }
}

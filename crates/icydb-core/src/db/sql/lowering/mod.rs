//! Module: db::sql::lowering
//! Responsibility: reduced SQL statement lowering into canonical query intent.
//! Does not own: SQL tokenization/parsing, planner validation policy, or executor semantics.
//! Boundary: frontend-only translation from parsed SQL statement contracts to `Query<E>`.

mod aggregate;
mod analysis;
mod expr;
mod normalize;
mod predicate;
mod prepare;
mod select;

///
/// TESTS
///

#[cfg(test)]
mod tests;

#[cfg(test)]
use crate::{
    db::{predicate::MissingRowPolicy, query::intent::Query},
    traits::EntityKind,
};
use crate::{
    db::{
        query::intent::QueryError,
        sql::parser::{SqlExplainMode, SqlStatement},
    },
    value::Value,
};
use thiserror::Error as ThisError;

pub(in crate::db::sql::lowering) use aggregate::LoweredSqlGlobalAggregateCommand;
pub(in crate::db) use aggregate::compile_sql_global_aggregate_command_core_from_prepared;
pub(in crate::db) use aggregate::is_sql_global_aggregate_statement;
#[cfg(test)]
pub(crate) use aggregate::{
    PreparedSqlScalarAggregateDescriptorShape, PreparedSqlScalarAggregateDomain,
    PreparedSqlScalarAggregateEmptySetBehavior, PreparedSqlScalarAggregateOrderingRequirement,
    PreparedSqlScalarAggregateRowSource, SqlGlobalAggregateCommand,
    compile_sql_global_aggregate_command,
};
pub(crate) use aggregate::{
    PreparedSqlScalarAggregateRuntimeDescriptor, PreparedSqlScalarAggregateStrategy,
    SqlGlobalAggregateCommandCore, bind_lowered_sql_explain_global_aggregate_structural,
};
pub(in crate::db::sql::lowering) use analysis::{LoweredExprAnalysis, analyze_lowered_expr};
pub(in crate::db) use expr::{
    PreparedSqlPredicateTemplateShape, sql_expr_is_compound_boolean_shape,
    sql_expr_prepared_predicate_template_shape,
};
pub(in crate::db) use predicate::lower_sql_where_expr;
pub(in crate::db) use prepare::prepared_sql_simple_range_slots;
pub(in crate::db) use prepare::sql_statement_contains_any_literal;
pub(crate) use prepare::{lower_sql_command_from_prepared_statement, prepare_sql_statement};
pub(in crate::db::sql::lowering) use select::apply_lowered_base_query_shape;
#[cfg(test)]
pub(in crate::db) use select::apply_lowered_select_shape;
pub(crate) use select::{LoweredBaseQueryShape, LoweredSelectShape};
pub(in crate::db) use select::{
    bind_lowered_sql_query, bind_lowered_sql_query_structural,
    bind_lowered_sql_select_query_structural, canonicalize_sql_predicate_for_model,
    canonicalize_strict_sql_literal_for_kind,
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
    GlobalAggregate(SqlGlobalAggregateCommand<E>),
    Explain {
        mode: SqlExplainMode,
        verbose: bool,
        query: Query<E>,
    },
    ExplainGlobalAggregate {
        mode: SqlExplainMode,
        verbose: bool,
        command: SqlGlobalAggregateCommand<E>,
    },
    DescribeEntity,
    ShowIndexesEntity,
    ShowColumnsEntity,
    ShowEntities,
}

impl LoweredSqlCommand {
    #[must_use]
    #[allow(dead_code)]
    pub(in crate::db) const fn query(&self) -> Option<&LoweredSqlQuery> {
        match &self.0 {
            LoweredSqlCommandInner::Query(query) => Some(query),
            LoweredSqlCommandInner::Explain { .. }
            | LoweredSqlCommandInner::ExplainGlobalAggregate { .. }
            | LoweredSqlCommandInner::DescribeEntity
            | LoweredSqlCommandInner::ShowIndexesEntity
            | LoweredSqlCommandInner::ShowColumnsEntity
            | LoweredSqlCommandInner::ShowEntities => None,
        }
    }

    #[must_use]
    pub(in crate::db) fn into_query(self) -> Option<LoweredSqlQuery> {
        match self.0 {
            LoweredSqlCommandInner::Query(query) => Some(query),
            LoweredSqlCommandInner::Explain { .. }
            | LoweredSqlCommandInner::ExplainGlobalAggregate { .. }
            | LoweredSqlCommandInner::DescribeEntity
            | LoweredSqlCommandInner::ShowIndexesEntity
            | LoweredSqlCommandInner::ShowColumnsEntity
            | LoweredSqlCommandInner::ShowEntities => None,
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
            | LoweredSqlCommandInner::ShowEntities => None,
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
#[derive(Debug, ThisError)]
pub(crate) enum SqlLoweringError {
    #[error("{0}")]
    Parse(#[from] crate::db::sql::parser::SqlParseError),

    #[error("{0}")]
    Query(Box<QueryError>),

    #[error("SQL entity '{sql_entity}' does not match requested entity type '{expected_entity}'")]
    EntityMismatch {
        sql_entity: String,
        expected_entity: &'static str,
    },

    #[error(
        "unsupported SQL SELECT projection; supported forms are SELECT *, field lists, global aggregate terminal lists, or grouped aggregate shapes"
    )]
    UnsupportedSelectProjection,

    #[error("unsupported SQL SELECT DISTINCT")]
    UnsupportedSelectDistinct,

    #[error("SELECT DISTINCT ORDER BY terms must be derivable from the projected distinct tuple")]
    DistinctOrderByRequiresProjectedTuple,

    #[error(
        "unsupported global aggregate SQL projection; supported forms are aggregate projections such as COUNT(*), SUM(field), AVG(expr), or scalar wrappers over aggregate results"
    )]
    UnsupportedGlobalAggregateProjection,

    #[error("global aggregate SQL does not support GROUP BY")]
    GlobalAggregateDoesNotSupportGroupBy,

    #[error("unsupported SQL GROUP BY projection shape")]
    UnsupportedSelectGroupBy,

    #[error("grouped SELECT requires an explicit projection list")]
    GroupedProjectionRequiresExplicitList,

    #[error("grouped SELECT projection must include at least one aggregate expression")]
    GroupedProjectionRequiresAggregate,

    #[error(
        "grouped projection expression at index={index} references fields outside GROUP BY keys"
    )]
    GroupedProjectionReferencesNonGroupField { index: usize },

    #[error(
        "grouped projection expression at index={index} appears after aggregate expressions started"
    )]
    GroupedProjectionScalarAfterAggregate { index: usize },

    #[error("HAVING requires GROUP BY")]
    HavingRequiresGroupBy,

    #[error("unsupported SQL HAVING shape")]
    UnsupportedSelectHaving,

    #[error("aggregate input expressions are not executable in this release")]
    UnsupportedAggregateInputExpressions,

    #[error("unsupported SQL WHERE expression shape")]
    UnsupportedWhereExpression,

    #[error("unknown field '{field}'")]
    UnknownField { field: String },

    #[error("{message}")]
    UnsupportedParameterPlacement { message: String },

    #[error("query-lane lowering reached a non query-compatible statement")]
    UnexpectedQueryLaneStatement,
}

impl SqlLoweringError {
    /// Construct one entity-mismatch SQL lowering error.
    fn entity_mismatch(sql_entity: impl Into<String>, expected_entity: &'static str) -> Self {
        Self::EntityMismatch {
            sql_entity: sql_entity.into(),
            expected_entity,
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
    pub(crate) fn unsupported_parameter_placement(
        index: Option<usize>,
        message: impl Into<String>,
    ) -> Self {
        let message = match index {
            Some(index) => format!("parameter slot ${index}: {}", message.into()),
            None => message.into(),
        };

        Self::UnsupportedParameterPlacement { message }
    }
}

impl From<QueryError> for SqlLoweringError {
    fn from(value: QueryError) -> Self {
        Self::Query(Box::new(value))
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

///
/// PreparedSqlParameterTypeFamily
///
/// Stable bind-time type family for one prepared SQL parameter slot.
/// This keeps v1 validation coarse and deterministic while the prepared SQL
/// surface remains restricted to compare-family value-insensitive positions.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum PreparedSqlParameterTypeFamily {
    Numeric,
    Text,
    Bool,
}

///
/// PreparedSqlParameterContract
///
/// Frozen bind contract for one prepared SQL parameter slot.
/// The contract is inferred once during prepare and reused unchanged for every
/// execution of the prepared SQL query shape.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct PreparedSqlParameterContract {
    index: usize,
    type_family: PreparedSqlParameterTypeFamily,
    null_allowed: bool,
    template_binding: Option<Value>,
}

impl PreparedSqlParameterContract {
    #[must_use]
    pub(in crate::db) const fn new(
        index: usize,
        type_family: PreparedSqlParameterTypeFamily,
        null_allowed: bool,
        template_binding: Option<Value>,
    ) -> Self {
        Self {
            index,
            type_family,
            null_allowed,
            template_binding,
        }
    }

    #[must_use]
    pub(in crate::db) const fn index(&self) -> usize {
        self.index
    }

    #[must_use]
    pub(in crate::db) const fn type_family(&self) -> PreparedSqlParameterTypeFamily {
        self.type_family
    }

    #[must_use]
    pub(in crate::db) const fn null_allowed(&self) -> bool {
        self.null_allowed
    }

    #[must_use]
    pub(in crate::db) const fn template_binding(&self) -> Option<&Value> {
        self.template_binding.as_ref()
    }
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

    /// Collect frozen parameter contracts from one prepared SQL statement.
    pub(in crate::db) fn parameter_contracts(
        &self,
        model: &'static crate::model::entity::EntityModel,
    ) -> Result<Vec<PreparedSqlParameterContract>, SqlLoweringError> {
        prepare::collect_prepared_statement_parameter_contracts(&self.statement, model)
    }

    /// Report whether this prepared SQL statement uses parameterized general
    /// expression shapes that must stay off template lanes.
    #[must_use]
    pub(in crate::db) fn uses_general_template_expr_parameters(&self) -> bool {
        prepare::prepared_statement_uses_general_template_expr_parameters(&self.statement)
    }

    /// Rebind one prepared SQL statement back to a literal-backed SQL shape.
    pub(in crate::db) fn bind_literals(
        &self,
        bindings: &[Value],
    ) -> Result<SqlStatement, QueryError> {
        prepare::bind_prepared_statement_literals(&self.statement, bindings)
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
}

/// Parse and lower one SQL statement into canonical query intent for `E`.
#[cfg(test)]
pub(crate) fn compile_sql_command<E: EntityKind>(
    sql: &str,
    consistency: MissingRowPolicy,
) -> Result<SqlCommand<E>, SqlLoweringError> {
    let statement = crate::db::sql::parser::parse_sql(sql)?;
    let prepared = prepare_sql_statement(statement, E::MODEL.name())?;

    if aggregate::is_sql_global_aggregate_statement(prepared.statement()) {
        return Ok(SqlCommand::GlobalAggregate(
            aggregate::compile_sql_global_aggregate_command_from_prepared::<E>(
                prepared,
                consistency,
            )?,
        ));
    }

    let lowered = lower_sql_command_from_prepared_statement(prepared, E::MODEL)?;

    // Keep the test-only typed envelope local to the single public test entry
    // point instead of preserving a private forwarding chain.
    match lowered.0 {
        LoweredSqlCommandInner::Query(query) => Ok(SqlCommand::Query(bind_lowered_sql_query::<E>(
            query,
            consistency,
        )?)),
        LoweredSqlCommandInner::ExplainGlobalAggregate {
            mode,
            verbose,
            command,
        } => Ok(SqlCommand::ExplainGlobalAggregate {
            mode,
            verbose,
            command: aggregate::bind_lowered_sql_global_aggregate_command::<E>(
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
            query: bind_lowered_sql_query::<E>(query, consistency)?,
        }),
        LoweredSqlCommandInner::DescribeEntity => Ok(SqlCommand::DescribeEntity),
        LoweredSqlCommandInner::ShowIndexesEntity => Ok(SqlCommand::ShowIndexesEntity),
        LoweredSqlCommandInner::ShowColumnsEntity => Ok(SqlCommand::ShowColumnsEntity),
        LoweredSqlCommandInner::ShowEntities => Ok(SqlCommand::ShowEntities),
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
    }
}

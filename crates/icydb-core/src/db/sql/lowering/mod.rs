//! Module: db::sql::lowering
//! Responsibility: reduced SQL statement lowering into canonical query intent.
//! Does not own: SQL tokenization/parsing, planner validation policy, or executor semantics.
//! Boundary: frontend-only translation from parsed SQL statement contracts to `Query<E>`.

mod aggregate;
mod normalize;
mod prepare;
mod select;

///
/// TESTS
///

#[cfg(test)]
mod tests;

use crate::db::{
    query::intent::QueryError,
    sql::parser::{SqlExplainMode, SqlStatement},
};
#[cfg(test)]
use crate::{
    db::{predicate::MissingRowPolicy, query::intent::Query},
    traits::EntityKind,
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
    TypedSqlGlobalAggregateTerminal, compile_sql_global_aggregate_command,
};
pub(crate) use aggregate::{
    PreparedSqlScalarAggregateRuntimeDescriptor, PreparedSqlScalarAggregateStrategy,
    SqlGlobalAggregateCommandCore, bind_lowered_sql_explain_global_aggregate_structural,
};
pub(crate) use prepare::{lower_sql_command_from_prepared_statement, prepare_sql_statement};
pub(in crate::db) use select::LoweredSelectQueryShape;
pub(in crate::db::sql::lowering) use select::apply_lowered_base_query_shape;
#[cfg(test)]
pub(in crate::db) use select::apply_lowered_select_shape;
pub(crate) use select::{LoweredBaseQueryShape, LoweredSelectShape};
pub(in crate::db) use select::{
    bind_lowered_sql_query, bind_lowered_sql_query_structural,
    bind_lowered_sql_select_query_structural, canonicalize_sql_predicate_for_model,
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
        query: LoweredSqlQuery,
    },
    ExplainGlobalAggregate {
        mode: SqlExplainMode,
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
    Explain {
        mode: SqlExplainMode,
        query: Query<E>,
    },
    ExplainGlobalAggregate {
        mode: SqlExplainMode,
        command: SqlGlobalAggregateCommand<E>,
    },
    DescribeEntity,
    ShowIndexesEntity,
    ShowColumnsEntity,
    ShowEntities,
}

impl LoweredSqlCommand {
    #[must_use]
    #[cfg_attr(not(any(test, feature = "perf-attribution")), allow(dead_code))]
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
    pub(in crate::db) const fn explain_query(&self) -> Option<(SqlExplainMode, &LoweredSqlQuery)> {
        match &self.0 {
            LoweredSqlCommandInner::Explain { mode, query } => Some((*mode, query)),
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

impl LoweredSqlQuery {
    // Surface the lowered query execution family without re-deriving it from
    // grouped fields or statement syntax in downstream layers.
    #[cfg(test)]
    pub(crate) const fn select_shape(&self) -> Option<LoweredSelectQueryShape> {
        match self {
            Self::Select(select) => Some(select.shape()),
            Self::Delete(_) => None,
        }
    }
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
        "unsupported SQL SELECT projection; supported forms are SELECT *, field lists, or grouped aggregate shapes"
    )]
    UnsupportedSelectProjection,

    #[error("unsupported SQL SELECT DISTINCT")]
    UnsupportedSelectDistinct,

    #[error("unsupported SQL GROUP BY projection shape")]
    UnsupportedSelectGroupBy,

    #[error("unsupported SQL HAVING shape")]
    UnsupportedSelectHaving,

    #[error("ORDER BY alias '{alias}' does not resolve to a supported order target")]
    UnsupportedOrderByAlias { alias: String },

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

    /// Construct one unsupported SELECT GROUP BY shape SQL lowering error.
    const fn unsupported_select_group_by() -> Self {
        Self::UnsupportedSelectGroupBy
    }

    /// Construct one unsupported SELECT HAVING shape SQL lowering error.
    const fn unsupported_select_having() -> Self {
        Self::UnsupportedSelectHaving
    }

    /// Construct one unsupported ORDER BY alias SQL lowering error.
    fn unsupported_order_by_alias(alias: impl Into<String>) -> Self {
        Self::UnsupportedOrderByAlias {
            alias: alias.into(),
        }
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

impl PreparedSqlStatement {
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
}

/// Parse and lower one SQL statement into canonical query intent for `E`.
#[cfg(test)]
pub(crate) fn compile_sql_command<E: EntityKind>(
    sql: &str,
    consistency: MissingRowPolicy,
) -> Result<SqlCommand<E>, SqlLoweringError> {
    let statement = crate::db::sql::parser::parse_sql(sql)?;

    compile_sql_command_from_statement::<E>(statement, consistency)
}

/// Lower one parsed SQL statement into canonical query intent for `E`.
#[cfg(test)]
pub(crate) fn compile_sql_command_from_statement<E: EntityKind>(
    statement: SqlStatement,
    consistency: MissingRowPolicy,
) -> Result<SqlCommand<E>, SqlLoweringError> {
    let prepared = prepare_sql_statement(statement, E::MODEL.name())?;

    compile_sql_command_from_prepared_statement::<E>(prepared, consistency)
}

/// Lower one prepared SQL statement into canonical query intent for `E`.
#[cfg(test)]
pub(crate) fn compile_sql_command_from_prepared_statement<E: EntityKind>(
    prepared: PreparedSqlStatement,
    consistency: MissingRowPolicy,
) -> Result<SqlCommand<E>, SqlLoweringError> {
    let lowered = lower_sql_command_from_prepared_statement(prepared, E::MODEL.primary_key.name)?;

    bind_lowered_sql_command::<E>(lowered, consistency)
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

/// Bind one shared generic-free SQL command shape to the typed query surface.
#[cfg(test)]
pub(crate) fn bind_lowered_sql_command<E: EntityKind>(
    lowered: LoweredSqlCommand,
    consistency: MissingRowPolicy,
) -> Result<SqlCommand<E>, SqlLoweringError> {
    match lowered.0 {
        LoweredSqlCommandInner::Query(query) => Ok(SqlCommand::Query(bind_lowered_sql_query::<E>(
            query,
            consistency,
        )?)),
        LoweredSqlCommandInner::Explain { mode, query } => Ok(SqlCommand::Explain {
            mode,
            query: bind_lowered_sql_query::<E>(query, consistency)?,
        }),
        LoweredSqlCommandInner::ExplainGlobalAggregate { mode, command } => {
            Ok(SqlCommand::ExplainGlobalAggregate {
                mode,
                command: aggregate::bind_lowered_sql_global_aggregate_command::<E>(
                    command,
                    consistency,
                )?,
            })
        }
        LoweredSqlCommandInner::DescribeEntity => Ok(SqlCommand::DescribeEntity),
        LoweredSqlCommandInner::ShowIndexesEntity => Ok(SqlCommand::ShowIndexesEntity),
        LoweredSqlCommandInner::ShowColumnsEntity => Ok(SqlCommand::ShowColumnsEntity),
        LoweredSqlCommandInner::ShowEntities => Ok(SqlCommand::ShowEntities),
    }
}

#[cfg(test)]
use crate::{db::query::intent::Query, traits::EntityKind};
use crate::{
    db::{
        predicate::MissingRowPolicy,
        query::{
            intent::StructuralQuery,
            plan::expr::{Expr, ProjectionSpec},
        },
        sql::{
            lowering::{
                PreparedSqlStatement, SqlLoweringError,
                aggregate::{
                    command::{
                        LoweredSqlGlobalAggregateCommand, lower_global_aggregate_select_shape,
                    },
                    strategy::PreparedSqlScalarAggregateStrategy,
                },
                apply_lowered_base_query_shape,
            },
            parser::SqlStatement,
        },
    },
    model::entity::EntityModel,
};

///
/// SqlGlobalAggregateCommand
///
/// Lowered global SQL aggregate command carrying base query shape plus terminal.
///
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct SqlGlobalAggregateCommand<E: EntityKind> {
    query: Query<E>,
    terminals: Vec<PreparedSqlScalarAggregateStrategy>,
    projection: ProjectionSpec,
    having: Option<Expr>,
    output_remap: Vec<usize>,
}

#[cfg(test)]
impl<E: EntityKind> SqlGlobalAggregateCommand<E> {
    /// Borrow the lowered base query shape for aggregate execution.
    #[must_use]
    pub(crate) const fn query(&self) -> &Query<E> {
        &self.query
    }

    /// Borrow the lowered aggregate terminals.
    #[must_use]
    pub(crate) fn terminals(&self) -> &[PreparedSqlScalarAggregateStrategy] {
        self.terminals.as_slice()
    }

    /// Borrow the canonical output projection contract for this global aggregate command.
    #[must_use]
    #[cfg(test)]
    pub(crate) const fn projection(&self) -> &ProjectionSpec {
        &self.projection
    }

    /// Borrow the optional global aggregate HAVING expression.
    #[must_use]
    #[cfg(test)]
    pub(crate) const fn having(&self) -> Option<&Expr> {
        self.having.as_ref()
    }

    /// Borrow the output-to-unique-terminal remap preserved from original SQL projection order.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn output_remap(&self) -> &[usize] {
        self.output_remap.as_slice()
    }

    /// Borrow the first lowered aggregate terminal for single-terminal callers.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn terminal(&self) -> &PreparedSqlScalarAggregateStrategy {
        self.terminals
            .first()
            .expect("global aggregate command must contain at least one terminal")
    }
}

///
/// SqlGlobalAggregateCommandCore
///
/// Generic-free lowered global aggregate command bound onto the structural
/// query surface.
/// This keeps global aggregate EXPLAIN on the shared query/explain path until
/// a typed boundary is strictly required.
///
#[derive(Clone, Debug)]
pub(crate) struct SqlGlobalAggregateCommandCore {
    query: StructuralQuery,
    strategies: Vec<PreparedSqlScalarAggregateStrategy>,
    projection: ProjectionSpec,
    having: Option<Expr>,
}

impl SqlGlobalAggregateCommandCore {
    /// Borrow the structural query payload for aggregate explain/execution.
    #[must_use]
    pub(in crate::db) const fn query(&self) -> &StructuralQuery {
        &self.query
    }

    /// Borrow prepared structural SQL scalar aggregate strategies.
    #[must_use]
    pub(in crate::db) const fn strategies(&self) -> &[PreparedSqlScalarAggregateStrategy] {
        self.strategies.as_slice()
    }

    /// Move the structural aggregate execution parts out of this command.
    #[must_use]
    pub(in crate::db) fn into_execution_parts(
        self,
    ) -> (
        StructuralQuery,
        Vec<PreparedSqlScalarAggregateStrategy>,
        ProjectionSpec,
        Option<Expr>,
    ) {
        (self.query, self.strategies, self.projection, self.having)
    }
}

impl LoweredSqlGlobalAggregateCommand {
    /// Bind this lowered aggregate command onto one entity-owned typed query.
    #[cfg(test)]
    fn into_typed<E: EntityKind>(
        self,
        consistency: MissingRowPolicy,
    ) -> Result<SqlGlobalAggregateCommand<E>, SqlLoweringError> {
        let Self {
            query,
            terminals,
            projection,
            having,
            output_remap,
        } = self;

        let terminals = terminals
            .into_iter()
            .map(|terminal| {
                PreparedSqlScalarAggregateStrategy::from_lowered_terminal(E::MODEL, terminal)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(SqlGlobalAggregateCommand {
            query: Query::from_inner(apply_lowered_base_query_shape(
                StructuralQuery::new(E::MODEL, consistency),
                query,
            )),
            terminals,
            projection,
            having,
            output_remap,
        })
    }

    /// Bind this lowered aggregate command onto the structural query surface
    /// used by aggregate explain and dynamic SQL execution.
    fn into_structural(
        self,
        model: &'static EntityModel,
        consistency: MissingRowPolicy,
    ) -> Result<SqlGlobalAggregateCommandCore, SqlLoweringError> {
        let Self {
            query,
            terminals,
            projection,
            having,
            #[cfg(test)]
                output_remap: _,
        } = self;

        let strategies = terminals
            .into_iter()
            .map(|terminal| {
                PreparedSqlScalarAggregateStrategy::from_lowered_terminal(model, terminal)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(SqlGlobalAggregateCommandCore {
            query: apply_lowered_base_query_shape(StructuralQuery::new(model, consistency), query),
            strategies,
            projection,
            having,
        })
    }
}

/// Parse and lower one SQL statement into global aggregate execution command for `E`.
#[cfg(test)]
pub(crate) fn compile_sql_global_aggregate_command<E: EntityKind>(
    sql: &str,
    consistency: MissingRowPolicy,
) -> Result<SqlGlobalAggregateCommand<E>, SqlLoweringError> {
    let statement = crate::db::sql::parser::parse_sql(sql)?;
    let prepared = crate::db::sql::lowering::prepare_sql_statement(statement, E::MODEL.name())?;

    compile_sql_global_aggregate_command_from_prepared::<E>(prepared, consistency)
}

// Lower one already-prepared SQL statement into the constrained global
// aggregate command envelope so callers that already parsed and routed the
// statement do not pay the parser again.
#[cfg(test)]
pub(crate) fn compile_sql_global_aggregate_command_from_prepared<E: EntityKind>(
    prepared: PreparedSqlStatement,
    consistency: MissingRowPolicy,
) -> Result<SqlGlobalAggregateCommand<E>, SqlLoweringError> {
    let SqlStatement::Select(statement) = prepared.statement else {
        return Err(SqlLoweringError::unsupported_select_projection());
    };

    bind_lowered_sql_global_aggregate_command::<E>(
        lower_global_aggregate_select_shape(statement)?,
        consistency,
    )
}

// Lower one already-prepared SQL statement into the generic-free global
// aggregate command envelope so dynamic SQL surfaces can share the same
// aggregate-shape authority before choosing their outward payload contract.
pub(in crate::db) fn compile_sql_global_aggregate_command_core_from_prepared(
    prepared: PreparedSqlStatement,
    model: &'static EntityModel,
    consistency: MissingRowPolicy,
) -> Result<SqlGlobalAggregateCommandCore, SqlLoweringError> {
    let SqlStatement::Select(statement) = prepared.statement else {
        return Err(SqlLoweringError::unsupported_select_projection());
    };

    bind_lowered_sql_global_aggregate_command_structural(
        model,
        lower_global_aggregate_select_shape(statement)?,
        consistency,
    )
}

#[cfg(test)]
pub(in crate::db::sql::lowering) fn bind_lowered_sql_global_aggregate_command<E: EntityKind>(
    lowered: LoweredSqlGlobalAggregateCommand,
    consistency: MissingRowPolicy,
) -> Result<SqlGlobalAggregateCommand<E>, SqlLoweringError> {
    lowered.into_typed::<E>(consistency)
}

pub(in crate::db::sql::lowering::aggregate) fn bind_lowered_sql_global_aggregate_command_structural(
    model: &'static EntityModel,
    lowered: LoweredSqlGlobalAggregateCommand,
    consistency: MissingRowPolicy,
) -> Result<SqlGlobalAggregateCommandCore, SqlLoweringError> {
    lowered.into_structural(model, consistency)
}

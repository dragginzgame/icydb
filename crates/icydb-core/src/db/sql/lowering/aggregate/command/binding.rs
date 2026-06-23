#[cfg(test)]
use crate::db::sql::lowering::apply_lowered_base_query_shape_for_model_only;
#[cfg(test)]
use crate::{db::query::intent::Query, traits::EntityKind};

use crate::{
    db::{
        predicate::MissingRowPolicy,
        query::{
            intent::StructuralQuery,
            plan::{
                AggregateKind,
                expr::{Expr, ProjectionField, ProjectionSpec},
            },
        },
        schema::SchemaInfo,
        sql::{
            lowering::{
                PreparedSqlStatement, SqlLoweringError,
                aggregate::{
                    command::{
                        LoweredSqlGlobalAggregateCommand, lower_global_aggregate_select_shape,
                    },
                    strategy::{
                        PreparedSqlScalarAggregatePlanFragment, PreparedSqlScalarAggregateStrategy,
                    },
                },
                apply_lowered_base_query_shape_with_schema, validate_base_query_sql_capabilities,
            },
            parser::SqlStatement,
        },
    },
    model::entity::EntityModel,
};

///
/// TypedSqlGlobalAggregateCommand
///
/// Lowered global SQL aggregate command carrying base query shape plus terminal.
///
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct TypedSqlGlobalAggregateCommand<E: EntityKind> {
    query: Query<E>,
    terminals: Vec<PreparedSqlScalarAggregateStrategy>,
    projection: ProjectionSpec,
    having: Option<Expr>,
    output_remap: Vec<usize>,
}

#[cfg(test)]
impl<E: EntityKind> TypedSqlGlobalAggregateCommand<E> {
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
    pub(in crate::db) const fn projection(&self) -> &ProjectionSpec {
        &self.projection
    }

    /// Borrow the optional global aggregate HAVING expression.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn having(&self) -> Option<&Expr> {
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
/// AggregateShapeFacts
///
/// Precomputed aggregate shape facts consumed by runtime and EXPLAIN.
/// Keeping these facts separate from the command avoids making the command
/// itself own singleton fast-path classification logic.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct AggregateShapeFacts {
    direct_count_rows: bool,
    direct_count_cardinality_metadata_candidate: bool,
}

impl AggregateShapeFacts {
    fn from_parts(
        query: &StructuralQuery,
        strategies: &[PreparedSqlScalarAggregateStrategy],
        projection: &ProjectionSpec,
        having: Option<&Expr>,
    ) -> Self {
        let direct_count_rows = having.is_none()
            && Self::has_direct_count_rows_strategy(strategies)
            && Self::has_direct_count_rows_projection(projection);

        Self {
            direct_count_rows,
            direct_count_cardinality_metadata_candidate: direct_count_rows
                && query.direct_count_cardinality_prefix_candidate(),
        }
    }

    /// Return whether this command is the singleton direct `COUNT(*)` shape.
    #[must_use]
    pub(in crate::db) const fn is_direct_count_rows(self) -> bool {
        self.direct_count_rows
    }

    /// Return whether direct prefix-cardinality metadata may answer this command.
    #[must_use]
    pub(in crate::db) const fn is_direct_count_cardinality_metadata_candidate(self) -> bool {
        self.direct_count_cardinality_metadata_candidate
    }

    fn has_direct_count_rows_strategy(strategies: &[PreparedSqlScalarAggregateStrategy]) -> bool {
        let [strategy] = strategies else {
            return false;
        };

        strategy.plan_fragment() == PreparedSqlScalarAggregatePlanFragment::CountRows
            && strategy.filter_expr().is_none()
    }

    fn has_direct_count_rows_projection(projection: &ProjectionSpec) -> bool {
        let mut fields = projection.fields();
        let Some(ProjectionField::Scalar {
            expr: Expr::Aggregate(aggregate),
            ..
        }) = fields.next()
        else {
            return false;
        };

        fields.next().is_none()
            && aggregate.kind() == AggregateKind::Count
            && aggregate.target_field().is_none()
            && aggregate.input_expr().is_none()
            && aggregate.filter_expr().is_none()
            && !aggregate.is_distinct()
    }
}

///
/// SqlGlobalAggregateCommand
///
/// Generic-free lowered global aggregate command bound onto the structural
/// query surface.
/// This keeps global aggregate EXPLAIN on the shared query/explain path until
/// a typed boundary is strictly required.
///
#[derive(Clone, Debug)]
pub(crate) struct SqlGlobalAggregateCommand {
    query: StructuralQuery,
    strategies: Vec<PreparedSqlScalarAggregateStrategy>,
    projection: ProjectionSpec,
    having: Option<Expr>,
    facts: AggregateShapeFacts,
}

impl SqlGlobalAggregateCommand {
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

    /// Borrow the canonical output projection for aggregate execution.
    #[must_use]
    pub(in crate::db) const fn projection(&self) -> &ProjectionSpec {
        &self.projection
    }

    /// Borrow the optional global aggregate HAVING expression.
    #[must_use]
    pub(in crate::db) const fn having(&self) -> Option<&Expr> {
        self.having.as_ref()
    }

    /// Borrow precomputed command facts consumed by runtime and EXPLAIN.
    #[must_use]
    pub(in crate::db) const fn facts(&self) -> AggregateShapeFacts {
        self.facts
    }
}

impl LoweredSqlGlobalAggregateCommand {
    /// Bind this lowered aggregate command onto one entity-owned typed query.
    #[cfg(test)]
    fn into_typed_for_model_only<E: EntityKind>(
        self,
        consistency: MissingRowPolicy,
    ) -> Result<TypedSqlGlobalAggregateCommand<E>, SqlLoweringError> {
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
                PreparedSqlScalarAggregateStrategy::from_lowered_terminal_with_schema(
                    E::MODEL,
                    SchemaInfo::cached_for_generated_entity_model(E::MODEL),
                    terminal,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(TypedSqlGlobalAggregateCommand {
            query: Query::from_inner(apply_lowered_base_query_shape_for_model_only(
                StructuralQuery::new(E::MODEL, consistency),
                query,
            )),
            terminals,
            projection,
            having,
            output_remap,
        })
    }

    /// Bind this lowered aggregate command onto the accepted schema surface
    /// used by aggregate explain and dynamic SQL execution.
    fn into_command_with_schema(
        self,
        model: &'static EntityModel,
        consistency: MissingRowPolicy,
        schema: &SchemaInfo,
    ) -> Result<SqlGlobalAggregateCommand, SqlLoweringError> {
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
                PreparedSqlScalarAggregateStrategy::from_lowered_terminal_with_schema(
                    model, schema, terminal,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        validate_base_query_sql_capabilities(schema, &query)?;

        let query = apply_lowered_base_query_shape_with_schema(
            StructuralQuery::new(model, consistency),
            query,
            schema,
        );
        let facts = AggregateShapeFacts::from_parts(
            &query,
            strategies.as_slice(),
            &projection,
            having.as_ref(),
        );

        Ok(SqlGlobalAggregateCommand {
            query,
            strategies,
            projection,
            having,
            facts,
        })
    }
}

/// Parse and lower one SQL statement into global aggregate execution command for `E`.
#[cfg(test)]
pub(crate) fn compile_sql_global_aggregate_command_for_model_only<E: EntityKind>(
    sql: &str,
    consistency: MissingRowPolicy,
) -> Result<TypedSqlGlobalAggregateCommand<E>, SqlLoweringError> {
    let statement = crate::db::sql::parser::parse_sql(sql)?;
    let prepared = crate::db::sql::lowering::prepare_sql_statement(&statement, E::MODEL.name())?;

    compile_sql_global_aggregate_command_from_prepared_for_model_only::<E>(prepared, consistency)
}

// Lower one already-prepared SQL statement into the constrained global
// aggregate command envelope so callers that already parsed and routed the
// statement do not pay the parser again.
#[cfg(test)]
pub(crate) fn compile_sql_global_aggregate_command_from_prepared_for_model_only<E: EntityKind>(
    prepared: PreparedSqlStatement,
    consistency: MissingRowPolicy,
) -> Result<TypedSqlGlobalAggregateCommand<E>, SqlLoweringError> {
    let SqlStatement::Select(statement) = prepared.statement else {
        return Err(SqlLoweringError::unsupported_select_projection());
    };

    bind_lowered_sql_global_aggregate_command_for_model_only::<E>(
        lower_global_aggregate_select_shape(statement)?,
        consistency,
    )
}

/// Lower one already-prepared SQL statement into the generic-free global
/// aggregate command envelope with an explicit schema capability projection.
pub(in crate::db) fn compile_sql_global_aggregate_command_from_prepared_with_schema(
    prepared: PreparedSqlStatement,
    model: &'static EntityModel,
    consistency: MissingRowPolicy,
    schema: &SchemaInfo,
) -> Result<SqlGlobalAggregateCommand, SqlLoweringError> {
    let SqlStatement::Select(statement) = prepared.statement else {
        return Err(SqlLoweringError::unsupported_select_projection());
    };

    bind_lowered_sql_global_aggregate_command_with_schema(
        model,
        lower_global_aggregate_select_shape(statement)?,
        consistency,
        schema,
    )
}

#[cfg(test)]
pub(in crate::db::sql::lowering) fn bind_lowered_sql_global_aggregate_command_for_model_only<
    E: EntityKind,
>(
    lowered: LoweredSqlGlobalAggregateCommand,
    consistency: MissingRowPolicy,
) -> Result<TypedSqlGlobalAggregateCommand<E>, SqlLoweringError> {
    lowered.into_typed_for_model_only::<E>(consistency)
}

pub(in crate::db::sql::lowering::aggregate) fn bind_lowered_sql_global_aggregate_command_with_schema(
    model: &'static EntityModel,
    lowered: LoweredSqlGlobalAggregateCommand,
    consistency: MissingRowPolicy,
    schema: &SchemaInfo,
) -> Result<SqlGlobalAggregateCommand, SqlLoweringError> {
    lowered.into_command_with_schema(model, consistency, schema)
}

use crate::db::{
    predicate::MissingRowPolicy,
    query::{
        intent::StructuralQuery,
        plan::{
            AggregateKind, OrderDirection, OrderSpec, OrderTerm,
            expr::{Expr, ProjectionField, ProjectionSpec},
        },
    },
    schema::SchemaInfo,
    sql::{
        lowering::{
            PreparedSqlStatement, SqlLoweringError,
            aggregate::{
                command::{LoweredSqlGlobalAggregateCommand, lower_global_aggregate_select_shape},
                strategy::{
                    PreparedSqlScalarAggregatePlanFragment, PreparedSqlScalarAggregateStrategy,
                },
            },
            apply_lowered_base_query_shape_with_schema, validate_base_query_sql_capabilities,
        },
        parser::SqlStatement,
    },
};

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
        schema: &SchemaInfo,
        query: &StructuralQuery,
        strategies: &[PreparedSqlScalarAggregateStrategy],
        projection: &ProjectionSpec,
        having: Option<&Expr>,
    ) -> Self {
        let direct_count_rows = having.is_none()
            && Self::has_direct_count_rows_strategy(schema, strategies)
            && Self::has_direct_count_rows_projection(projection);

        Self {
            direct_count_rows,
            direct_count_cardinality_metadata_candidate: direct_count_rows
                && query.direct_count_cardinality_prefix_candidate(),
        }
    }

    /// Return whether the command's only aggregate is schema-proven row-equivalent COUNT.
    #[must_use]
    pub(in crate::db) const fn is_direct_count_rows(self) -> bool {
        self.direct_count_rows
    }

    /// Return whether direct prefix-cardinality metadata may answer this command.
    #[must_use]
    pub(in crate::db) const fn is_direct_count_cardinality_metadata_candidate(self) -> bool {
        self.direct_count_cardinality_metadata_candidate
    }

    fn has_direct_count_rows_strategy(
        schema: &SchemaInfo,
        strategies: &[PreparedSqlScalarAggregateStrategy],
    ) -> bool {
        let [strategy] = strategies else {
            return false;
        };

        if strategy.filter_expr().is_some() {
            return false;
        }

        match strategy.plan_fragment() {
            PreparedSqlScalarAggregatePlanFragment::CountRows => true,
            PreparedSqlScalarAggregatePlanFragment::CountField => {
                strategy
                    .target_slot()
                    .and_then(|slot| schema.accepted_field_is_nullable(slot.field()))
                    == Some(false)
            }
            PreparedSqlScalarAggregatePlanFragment::NumericField { .. }
            | PreparedSqlScalarAggregatePlanFragment::ExtremalWinnerField { .. } => false,
        }
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
    /// Bind this lowered aggregate command onto the accepted schema surface
    /// used by aggregate explain and dynamic SQL execution.
    fn into_command_with_schema(
        self,
        consistency: MissingRowPolicy,
        schema: &SchemaInfo,
    ) -> Result<SqlGlobalAggregateCommand, SqlLoweringError> {
        let Self {
            query,
            terminals,
            projection,
            having,
        } = self;

        let strategies = terminals
            .into_iter()
            .map(|terminal| {
                PreparedSqlScalarAggregateStrategy::from_lowered_terminal_with_schema(
                    schema, terminal,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        validate_base_query_sql_capabilities(schema, &query)?;

        let extrema_input_order = indexed_extrema_input_order(schema, &query, &strategies);

        let mut query = apply_lowered_base_query_shape_with_schema(
            StructuralQuery::new(consistency),
            query,
            schema,
        );
        if let Some(order) = extrema_input_order {
            query = query.order_spec(order);
        }
        let facts = AggregateShapeFacts::from_parts(
            schema,
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

// Project one internal, order-insensitive extrema access intent only when the
// accepted schema proves the complete direct-field target + primary-key index
// order. The canonical access planner still selects the physical index.
fn indexed_extrema_input_order(
    schema: &SchemaInfo,
    query: &crate::db::sql::lowering::LoweredBaseQueryShape,
    strategies: &[PreparedSqlScalarAggregateStrategy],
) -> Option<OrderSpec> {
    if query.limit.is_some() || query.offset.is_some() || !query.order_by.is_empty() {
        return None;
    }
    let [strategy] = strategies else {
        return None;
    };
    if strategy.filter_expr().is_some() {
        return None;
    }
    let target = strategy.target_slot()?.field();
    if schema.accepted_field_is_nullable(target) != Some(false) {
        return None;
    }
    let direction = match strategy.plan_fragment() {
        PreparedSqlScalarAggregatePlanFragment::ExtremalWinnerField {
            kind: AggregateKind::Min,
        } => OrderDirection::Asc,
        PreparedSqlScalarAggregatePlanFragment::ExtremalWinnerField {
            kind: AggregateKind::Max,
        } if schema.scalar_primary_key_name() == Some(target) => OrderDirection::Desc,
        PreparedSqlScalarAggregatePlanFragment::CountRows
        | PreparedSqlScalarAggregatePlanFragment::CountField
        | PreparedSqlScalarAggregatePlanFragment::NumericField { .. }
        | PreparedSqlScalarAggregatePlanFragment::ExtremalWinnerField { .. } => return None,
    };

    let mut ordered_fields = Vec::with_capacity(1 + schema.primary_key_names().len());
    ordered_fields.push(target);
    ordered_fields.extend(
        schema
            .primary_key_names()
            .iter()
            .map(String::as_str)
            .filter(|primary_key| *primary_key != target),
    );
    let target_is_primary = schema.scalar_primary_key_name() == Some(target);
    let secondary_order_exists = schema.field_path_indexes().iter().any(|index| {
        index.predicate_sql().is_none()
            && index.fields().len() == ordered_fields.len()
            && index
                .fields()
                .iter()
                .zip(&ordered_fields)
                .all(|(field, expected)| {
                    field.path().len() == 1
                        && field.path()[0] == field.field_name()
                        && field.field_name() == *expected
                })
    });
    if !target_is_primary && !secondary_order_exists {
        return None;
    }

    Some(OrderSpec {
        fields: ordered_fields
            .into_iter()
            .map(|field| OrderTerm::field(field, direction))
            .collect(),
    })
}

/// Lower one already-prepared SQL statement into the generic-free global
/// aggregate command envelope with an explicit schema capability projection.
pub(in crate::db) fn compile_sql_global_aggregate_command_from_prepared_with_schema(
    prepared: PreparedSqlStatement,
    consistency: MissingRowPolicy,
    schema: &SchemaInfo,
) -> Result<SqlGlobalAggregateCommand, SqlLoweringError> {
    let SqlStatement::Select(statement) = prepared.statement else {
        return Err(SqlLoweringError::unsupported_select_projection());
    };

    bind_lowered_sql_global_aggregate_command_with_schema(
        lower_global_aggregate_select_shape(statement)?,
        consistency,
        schema,
    )
}

pub(in crate::db::sql::lowering::aggregate) fn bind_lowered_sql_global_aggregate_command_with_schema(
    lowered: LoweredSqlGlobalAggregateCommand,
    consistency: MissingRowPolicy,
    schema: &SchemaInfo,
) -> Result<SqlGlobalAggregateCommand, SqlLoweringError> {
    lowered.into_command_with_schema(consistency, schema)
}

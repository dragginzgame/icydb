use crate::{
    db::{
        query::{
            builder::{
                AggregateExpr,
                aggregate::{avg, count, count_by, max_by, min_by, sum},
            },
            plan::expr::{Expr, canonicalize_aggregate_input_expr},
        },
        schema::SchemaInfo,
        sql::{
            lowering::{
                SqlLoweringError,
                aggregate::{
                    distinct::{apply_distinct_marker, reject_distinct_filter_pairing},
                    grouped::validate_grouped_aggregate_scalar_subexpressions,
                    lowering::{
                        aggregate_shape::LoweredSqlAggregateShape, apply_aggregate_filter_expr,
                    },
                },
                expr::{SqlExprPhase, lower_sql_expr},
                predicate::lower_sql_pre_aggregate_bool_expr,
            },
            parser::{SqlAggregateCall, SqlAggregateKind, SqlExpr},
        },
    },
    model::entity::EntityModel,
};

fn lower_sql_aggregate_shape(
    call: SqlAggregateCall,
) -> Result<LoweredSqlAggregateShape, SqlLoweringError> {
    let SqlAggregateCall {
        kind,
        input,
        filter_expr,
        distinct,
    } = call;
    let filter_expr = filter_expr
        .map(|expr| lower_sql_pre_aggregate_bool_expr(expr.as_ref()))
        .transpose()?;

    reject_distinct_filter_pairing(distinct, filter_expr.as_ref())?;

    match input.map(|input| *input) {
        None if kind.supports_star_input() && !distinct => {
            Ok(LoweredSqlAggregateShape::CountRows { filter_expr })
        }
        Some(SqlExpr::Field(field)) if matches!(kind, SqlAggregateKind::Count) => {
            Ok(LoweredSqlAggregateShape::CountField {
                field,
                filter_expr,
                distinct,
            })
        }
        Some(SqlExpr::Field(field)) if kind.lowers_shared_field_target_shape() => {
            Ok(LoweredSqlAggregateShape::FieldTarget {
                kind,
                field,
                filter_expr,
                distinct,
            })
        }
        Some(input) => Ok(LoweredSqlAggregateShape::ExpressionInput {
            kind,
            input_expr: canonicalize_aggregate_input_expr(
                kind.aggregate_kind(),
                lower_sql_expr(&input, SqlExprPhase::PreAggregate)?,
            ),
            filter_expr,
            distinct,
        }),
        _ => Err(SqlLoweringError::unsupported_select_projection()),
    }
}

pub(in crate::db::sql::lowering) fn lower_aggregate_call(
    call: SqlAggregateCall,
) -> Result<AggregateExpr, SqlLoweringError> {
    match lower_sql_aggregate_shape(call)? {
        LoweredSqlAggregateShape::CountRows { filter_expr } => {
            Ok(apply_aggregate_filter_expr(count(), filter_expr))
        }
        LoweredSqlAggregateShape::CountField {
            field,
            filter_expr,
            distinct,
        } => {
            let aggregate = apply_distinct_marker(count_by(field), distinct);

            Ok(apply_aggregate_filter_expr(aggregate, filter_expr))
        }
        LoweredSqlAggregateShape::FieldTarget {
            kind,
            field,
            filter_expr,
            distinct,
        } => kind.lower_field_target_aggregate(field, filter_expr, distinct),
        LoweredSqlAggregateShape::ExpressionInput {
            kind,
            input_expr,
            filter_expr,
            distinct,
        } => Ok(apply_aggregate_filter_expr(
            kind.lower_expression_owned_aggregate(input_expr, distinct),
            filter_expr,
        )),
    }
}

// Lower one grouped aggregate call while validating its model-bound scalar
// subexpressions before grouped execution can compile them into reducer state.
pub(in crate::db::sql::lowering) fn lower_grouped_aggregate_call(
    model: &'static EntityModel,
    schema: &SchemaInfo,
    call: SqlAggregateCall,
) -> Result<AggregateExpr, SqlLoweringError> {
    let aggregate = lower_aggregate_call(call)?;

    validate_grouped_aggregate_scalar_subexpressions(model, schema, &aggregate)?;

    Ok(aggregate)
}

impl SqlAggregateKind {
    // Lower one field-target aggregate call through the parser-owned aggregate
    // taxonomy so SQL lowering keeps the supported field-target family on the
    // enum instead of repeating the kind ladder at callsites.
    fn lower_field_target_aggregate(
        self,
        field: String,
        filter_expr: Option<Expr>,
        distinct: bool,
    ) -> Result<AggregateExpr, SqlLoweringError> {
        let aggregate = match self {
            Self::Count => return Err(SqlLoweringError::unsupported_select_projection()),
            Self::Sum => apply_distinct_marker(sum(field), distinct),
            Self::Avg => apply_distinct_marker(avg(field), distinct),
            Self::Min => apply_distinct_marker(min_by(field), distinct),
            Self::Max => apply_distinct_marker(max_by(field), distinct),
        };

        Ok(apply_aggregate_filter_expr(aggregate, filter_expr))
    }

    // Lower one expression-owned aggregate call through the parser-owned
    // aggregate taxonomy so SQL lowering reuses the enum's planner-kind
    // mapping instead of reopening it as a free function.
    fn lower_expression_owned_aggregate(self, input_expr: Expr, distinct: bool) -> AggregateExpr {
        let aggregate = AggregateExpr::from_expression_input(self.aggregate_kind(), input_expr);

        apply_distinct_marker(aggregate, distinct)
    }
}

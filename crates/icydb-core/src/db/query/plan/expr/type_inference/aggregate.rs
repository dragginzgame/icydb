use crate::{
    db::query::{
        builder::aggregate::AggregateExpr,
        plan::{
            AggregateKind, PlanError,
            expr::{
                Expr, NumericSubtype,
                type_inference::{ExprType, infer_expr_type},
            },
            validate::ExprPlanError,
        },
    },
    db::schema::SchemaInfo,
};

pub(super) fn infer_aggregate_expr_type(
    aggregate: &AggregateExpr,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let kind = aggregate.kind();
    let input_expr = aggregate.input_expr();

    match kind {
        AggregateKind::Count => Ok(ExprType::Numeric(NumericSubtype::Integer)),
        AggregateKind::Exists => Ok(ExprType::Bool),
        AggregateKind::Sum | AggregateKind::Avg => infer_sum_aggregate_type(input_expr, schema),
        AggregateKind::Min | AggregateKind::Max | AggregateKind::First | AggregateKind::Last => {
            infer_target_field_aggregate_type(input_expr, schema)
        }
    }
}

fn infer_sum_aggregate_type(
    input_expr: Option<&Expr>,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let Some(input_expr) = input_expr else {
        return Err(PlanError::from(ExprPlanError::aggregate_target_required()));
    };

    let inferred = infer_expr_type(input_expr, schema)?;

    match (input_expr, &inferred) {
        (Expr::Field(_), ExprType::Numeric(_)) => {}
        (Expr::Field(_), _) => {
            return Err(PlanError::from(
                ExprPlanError::non_numeric_aggregate_target(),
            ));
        }
        (_, ExprType::Numeric(_)) => {}
        _ => {
            return Err(PlanError::from(
                ExprPlanError::non_numeric_aggregate_target(),
            ));
        }
    }

    Ok(inferred)
}

fn infer_target_field_aggregate_type(
    input_expr: Option<&Expr>,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let Some(input_expr) = input_expr else {
        // Bootstrap behavior: target-less extrema/value terminals stay unresolved.
        return Ok(ExprType::Unknown);
    };

    infer_expr_type(input_expr, schema)
}

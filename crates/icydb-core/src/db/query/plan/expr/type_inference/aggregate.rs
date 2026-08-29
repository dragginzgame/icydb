use crate::{
    db::query::{
        builder::aggregate::AggregateExpr,
        plan::{
            AggregateKind, PlanError,
            expr::{
                Expr, NumericSubtype,
                type_inference::{ExprType, infer_expr_type},
            },
            validate::{ExprPlanError, ExprPlanTypeClass},
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
        AggregateKind::Sum | AggregateKind::Avg => {
            infer_sum_aggregate_type(kind, input_expr, schema)
        }
        AggregateKind::Min | AggregateKind::Max | AggregateKind::First | AggregateKind::Last => {
            infer_target_field_aggregate_type(input_expr, schema)
        }
    }
}

fn infer_sum_aggregate_type(
    kind: AggregateKind,
    input_expr: Option<&Expr>,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let Some(input_expr) = input_expr else {
        return Err(PlanError::from(ExprPlanError::aggregate_target_required(
            kind,
        )));
    };

    let inferred = infer_expr_type(input_expr, schema)?;

    if !sum_like_input_type_supported(kind, &inferred) {
        return Err(PlanError::from(
            ExprPlanError::non_numeric_aggregate_target(
                kind,
                ExprPlanTypeClass::from_expr_type(&inferred),
            ),
        ));
    }

    Ok(inferred)
}

const fn sum_like_input_type_supported(kind: AggregateKind, inferred: &ExprType) -> bool {
    matches!(inferred, ExprType::Numeric(_))
        || matches!((kind, inferred), (AggregateKind::Sum, ExprType::U256))
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

#[cfg(test)]
mod tests {
    use super::sum_like_input_type_supported;
    use crate::db::query::plan::{AggregateKind, expr::type_inference::ExprType};

    #[test]
    fn u256_is_admitted_only_by_sum() {
        assert!(sum_like_input_type_supported(
            AggregateKind::Sum,
            &ExprType::U256,
        ));
        assert!(!sum_like_input_type_supported(
            AggregateKind::Avg,
            &ExprType::U256,
        ));
    }
}

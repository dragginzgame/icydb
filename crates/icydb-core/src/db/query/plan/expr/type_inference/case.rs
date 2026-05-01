use crate::db::{
    query::plan::{
        PlanError,
        expr::{
            CaseWhenArm, Expr,
            type_inference::{ExprType, infer_expr_type, unify::unify_case_branch_types},
        },
        validate::ExprPlanError,
    },
    schema::SchemaInfo,
};

pub(super) fn infer_case_expr_type(
    when_then_arms: &[CaseWhenArm],
    else_expr: &Expr,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let mut result_type = infer_expr_type(else_expr, schema)?;

    for arm in when_then_arms {
        let condition_type = infer_expr_type(arm.condition(), schema)?;
        if !matches!(condition_type, ExprType::Bool) {
            return Err(PlanError::from(ExprPlanError::invalid_case_condition_type(
                format!("{condition_type:?}"),
            )));
        }

        let branch_type = infer_expr_type(arm.result(), schema)?;
        result_type =
            unify_case_branch_types((&branch_type, arm.result()), (&result_type, else_expr))?;
    }

    Ok(result_type)
}

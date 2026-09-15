use crate::db::{
    QueryError,
    query::plan::{
        PlanError,
        expr::{
            CaseWhenArm, Expr,
            type_inference::{ExprType, infer_expr_type, unify::unify_case_branch_types},
        },
        validate::ExprPlanError,
    },
    query::preparation::PreparationWork,
    schema::SchemaInfo,
};

pub(super) fn infer_case_expr_type(
    when_then_arms: &[CaseWhenArm],
    else_expr: &Expr,
    schema: &SchemaInfo,
    work: &PreparationWork<'_>,
) -> Result<ExprType, QueryError> {
    let mut result_type = infer_expr_type(else_expr, schema, work)?;
    let mut result_branch_index = None;

    for (arm_index, arm) in when_then_arms.iter().enumerate() {
        let condition_type = infer_expr_type(arm.condition(), schema, work)?;
        // A known null is UNKNOWN in a boolean condition, not a cast
        // into the CASE result family. Every result branch is still checked.
        if !matches!(condition_type, ExprType::Bool | ExprType::Null) {
            return Err(PlanError::from(ExprPlanError::invalid_case_condition_type(
                arm_index,
                &condition_type,
            ))
            .into());
        }

        let branch_type = infer_expr_type(arm.result(), schema, work)?;
        result_type = unify_case_branch_types(
            (Some(arm_index), &branch_type),
            (result_branch_index, &result_type),
        )?;
        result_branch_index = Some(arm_index);
    }

    Ok(result_type)
}

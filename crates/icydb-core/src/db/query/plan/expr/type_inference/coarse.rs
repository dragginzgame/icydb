use crate::db::{
    query::plan::{
        PlanError,
        expr::{
            Expr, ExprCoarseTypeFamily, Function, FunctionTypeInferenceShape,
            type_inference::{
                ExprType, coarse_family_for_expr_type, infer_expr_type,
                unify::{unify_case_branch_types, unify_coalesce_expr_types},
            },
        },
        validate::ExprPlanError,
    },
    schema::SchemaInfo,
};

/// Infer one planner-owned coarse family directly from one expression subtree.
pub(in crate::db::query::plan::expr) fn infer_expr_coarse_family(
    expr: &Expr,
    schema: &SchemaInfo,
) -> Result<Option<ExprCoarseTypeFamily>, PlanError> {
    let inferred = infer_expr_type(expr, schema)?;

    Ok(coarse_family_for_expr_type(&inferred))
}

/// Infer one planner-owned coarse family from the lowerable searched `CASE`
/// result branches that are already visible at a caller boundary.
pub(in crate::db::query::plan::expr) fn infer_case_result_exprs_coarse_family<'a>(
    result_exprs: impl IntoIterator<Item = &'a Expr>,
    schema: &SchemaInfo,
) -> Result<Option<ExprCoarseTypeFamily>, PlanError> {
    infer_folded_exprs_coarse_family(result_exprs, schema, |current, current_expr, next, expr| {
        unify_case_branch_types((&next, expr), (&current, current_expr))
    })
}

/// Infer one planner-owned coarse family from the lowerable arguments of a
/// dynamic-result scalar function whose result family depends on shared
/// argument unification instead of a fixed signature table.
pub(in crate::db::query::plan::expr) fn infer_dynamic_function_result_exprs_coarse_family(
    function: Function,
    args: &[Expr],
    schema: &SchemaInfo,
) -> Result<Option<ExprCoarseTypeFamily>, PlanError> {
    match function.type_inference_shape() {
        FunctionTypeInferenceShape::DynamicCoalesce | FunctionTypeInferenceShape::DynamicNullIf => {
            infer_folded_exprs_coarse_family(args.iter(), schema, |current, _, next, _| {
                unify_coalesce_expr_types(current, next)
            })
        }
        _ => Err(PlanError::from(ExprPlanError::invalid_function_argument(
            function.canonical_label(),
            args.len(),
            "function is outside the dynamic partial-inference surface".to_string(),
        ))),
    }
}

// Fold one visible expression list through planner-owned type inference and one
// caller-supplied unification rule, then project the final planner type onto a
// coarse family for boundary consumers such as prepared fallback typing.
fn infer_folded_exprs_coarse_family<'a, F>(
    exprs: impl IntoIterator<Item = &'a Expr>,
    schema: &SchemaInfo,
    mut fold: F,
) -> Result<Option<ExprCoarseTypeFamily>, PlanError>
where
    F: FnMut(ExprType, &'a Expr, ExprType, &'a Expr) -> Result<ExprType, PlanError>,
{
    let mut resolved: Option<(ExprType, &'a Expr)> = None;

    for expr in exprs {
        let next = infer_expr_type(expr, schema)?;
        resolved = Some(match resolved {
            None => (next, expr),
            Some((current, current_expr)) => (fold(current, current_expr, next, expr)?, expr),
        });
    }

    Ok(resolved
        .as_ref()
        .and_then(|(expr_type, _)| coarse_family_for_expr_type(expr_type)))
}

/// Return the shared expected coarse family for one fixed-arity scalar
/// function argument when planner typing defines that contract explicitly.
#[must_use]
pub(in crate::db::query::plan::expr) fn function_arg_coarse_family(
    function: Function,
    index: usize,
) -> Option<ExprCoarseTypeFamily> {
    function.type_inference_shape().arg_coarse_family(index)
}

/// Return the shared coarse result family for one scalar function when planner
/// typing fixes that family independently of argument-specific unification.
#[must_use]
pub(in crate::db::query::plan::expr) const fn function_result_coarse_family(
    function: Function,
) -> Option<ExprCoarseTypeFamily> {
    function.type_inference_shape().result_coarse_family()
}

/// Return the shared argument family for dynamic-result scalar functions once
/// planner typing has already resolved their result family.
#[must_use]
pub(in crate::db::query::plan::expr) const fn dynamic_function_arg_coarse_family(
    function: Function,
    result_family: ExprCoarseTypeFamily,
) -> Option<ExprCoarseTypeFamily> {
    function
        .type_inference_shape()
        .dynamic_arg_coarse_family(result_family)
}

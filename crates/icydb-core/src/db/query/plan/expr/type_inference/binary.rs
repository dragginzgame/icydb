use crate::db::{
    query::plan::{
        PlanError,
        expr::{
            BinaryOp, Expr,
            type_inference::{
                ExprType, infer_expr_type,
                unify::{blob_opaque_compatible, infer_numeric_result_subtype},
            },
        },
        validate::ExprPlanError,
    },
    schema::SchemaInfo,
};

pub(super) fn infer_binary_expr_type(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let left_ty = infer_expr_type(left, schema)?;
    let right_ty = infer_expr_type(right, schema)?;

    match op {
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div => {
            if !left_ty.is_numeric_eligible() || !right_ty.is_numeric_eligible() {
                return Err(invalid_binary_operands(op, &left_ty, &right_ty));
            }

            Ok(ExprType::Numeric(infer_numeric_result_subtype(
                op, &left_ty, &right_ty,
            )))
        }
        BinaryOp::Or | BinaryOp::And => {
            if !matches!(left_ty, ExprType::Bool) || !matches!(right_ty, ExprType::Bool) {
                return Err(invalid_binary_operands(op, &left_ty, &right_ty));
            }

            Ok(ExprType::Bool)
        }
        BinaryOp::Eq | BinaryOp::Ne => {
            if !binary_equality_comparable(&left_ty, &right_ty) {
                return Err(invalid_binary_operands(op, &left_ty, &right_ty));
            }

            Ok(ExprType::Bool)
        }
        BinaryOp::Lt | BinaryOp::Lte | BinaryOp::Gt | BinaryOp::Gte => {
            if !binary_order_comparable(&left_ty, &right_ty) {
                return Err(invalid_binary_operands(op, &left_ty, &right_ty));
            }

            Ok(ExprType::Bool)
        }
    }
}

// Binary type inference keeps one shared planner-facing operand mismatch error
// so arithmetic, boolean, and equality lanes cannot drift in diagnostics.
fn invalid_binary_operands(op: BinaryOp, left: &ExprType, right: &ExprType) -> PlanError {
    PlanError::from(ExprPlanError::invalid_binary_operands(
        op.canonical_label(),
        format!("{left:?}"),
        format!("{right:?}"),
    ))
}

const fn binary_equality_comparable(left: &ExprType, right: &ExprType) -> bool {
    if left.is_numeric_eligible() && right.is_numeric_eligible() {
        return true;
    }

    #[cfg(test)]
    if matches!((left, right), (ExprType::Null, ExprType::Null)) {
        return true;
    }

    if blob_opaque_compatible(left, right) {
        return true;
    }

    matches!(
        (left, right),
        (ExprType::Bool, ExprType::Bool)
            | (ExprType::Blob, ExprType::Blob)
            | (ExprType::Text, ExprType::Text)
            | (ExprType::Collection, ExprType::Collection)
            | (ExprType::Structured, ExprType::Structured)
            | (ExprType::Opaque, ExprType::Opaque)
    )
}

const fn binary_order_comparable(left: &ExprType, right: &ExprType) -> bool {
    if left.is_numeric_eligible() && right.is_numeric_eligible() {
        return true;
    }

    matches!((left, right), (ExprType::Text, ExprType::Text))
}

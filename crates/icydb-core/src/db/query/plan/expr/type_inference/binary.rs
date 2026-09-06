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

    // Comparisons with a known null have a boolean (possibly UNKNOWN)
    // result. Still infer both children first so null cannot hide bad operands
    // or missing fields. This is shared expression typing, not a SQL cast.
    if matches!(
        op,
        BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Lt | BinaryOp::Lte | BinaryOp::Gt | BinaryOp::Gte
    ) && (matches!(left_ty, ExprType::Null) || matches!(right_ty, ExprType::Null))
    {
        return Ok(ExprType::Bool);
    }

    match op {
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div => {
            // NULL propagates only after both subtrees and the other operand's
            // numeric family have been checked; it cannot hide a bad sibling.
            if matches!(left_ty, ExprType::Null) || matches!(right_ty, ExprType::Null) {
                if matches!(
                    left_ty,
                    ExprType::Null | ExprType::Numeric(_) | ExprType::U256
                ) && matches!(
                    right_ty,
                    ExprType::Null | ExprType::Numeric(_) | ExprType::U256
                ) {
                    return Ok(ExprType::Null);
                }
                return Err(invalid_binary_operands(op, &left_ty, &right_ty));
            }
            if matches!((&left_ty, &right_ty), (ExprType::U256, ExprType::U256)) {
                return Ok(ExprType::U256);
            }
            if !left_ty.is_numeric_eligible() || !right_ty.is_numeric_eligible() {
                return Err(invalid_binary_operands(op, &left_ty, &right_ty));
            }

            Ok(ExprType::Numeric(infer_numeric_result_subtype(
                op, &left_ty, &right_ty,
            )))
        }
        BinaryOp::Or | BinaryOp::And => {
            if !matches!(left_ty, ExprType::Bool | ExprType::Null)
                || !matches!(right_ty, ExprType::Bool | ExprType::Null)
            {
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
    PlanError::from(ExprPlanError::invalid_binary_operands(op, left, right))
}

const fn binary_equality_comparable(left: &ExprType, right: &ExprType) -> bool {
    if left.is_numeric_eligible() && right.is_numeric_eligible() {
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
            | (ExprType::U256, ExprType::U256)
    )
}

const fn binary_order_comparable(left: &ExprType, right: &ExprType) -> bool {
    if left.is_numeric_eligible() && right.is_numeric_eligible() {
        return true;
    }

    matches!(
        (left, right),
        (ExprType::Text, ExprType::Text) | (ExprType::U256, ExprType::U256)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u256_planner_type_is_strictly_comparable_without_numeric_widening() {
        assert!(binary_equality_comparable(&ExprType::U256, &ExprType::U256));
        assert!(binary_order_comparable(&ExprType::U256, &ExprType::U256));
        assert!(!ExprType::U256.is_numeric_eligible());
    }

    #[test]
    fn u256_planner_type_does_not_widen_opaque_ordering() {
        assert!(!binary_order_comparable(
            &ExprType::Opaque,
            &ExprType::Opaque,
        ));
        assert!(!binary_order_comparable(&ExprType::U256, &ExprType::Opaque,));
    }
}

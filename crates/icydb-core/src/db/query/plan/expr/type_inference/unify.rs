use crate::{
    db::query::plan::{
        PlanError,
        expr::{BinaryOp, Expr, NumericSubtype, type_inference::ExprType},
        validate::ExprPlanError,
    },
    value::Value,
};

pub(super) fn unify_coalesce_expr_types(
    current: ExprType,
    next: ExprType,
) -> Result<ExprType, PlanError> {
    match (current, next) {
        (ExprType::Numeric(left), ExprType::Numeric(right)) => {
            Ok(ExprType::Numeric(match (left, right) {
                (NumericSubtype::Decimal, _) | (_, NumericSubtype::Decimal) => {
                    NumericSubtype::Decimal
                }
                (NumericSubtype::Float, _) | (_, NumericSubtype::Float) => NumericSubtype::Float,
                (NumericSubtype::Unknown, other) | (other, NumericSubtype::Unknown) => other,
                (NumericSubtype::Integer, NumericSubtype::Integer) => NumericSubtype::Integer,
            }))
        }
        (ExprType::Blob, ExprType::Blob) => Ok(ExprType::Blob),
        (ExprType::Text, ExprType::Text) => Ok(ExprType::Text),
        (ExprType::Bool, ExprType::Bool) => Ok(ExprType::Bool),
        (ExprType::Collection, ExprType::Collection) => Ok(ExprType::Collection),
        (ExprType::Structured, ExprType::Structured) => Ok(ExprType::Structured),
        (ExprType::Opaque, ExprType::Opaque) => Ok(ExprType::Opaque),
        (ExprType::Blob, ExprType::Opaque) | (ExprType::Opaque, ExprType::Blob) => {
            Ok(ExprType::Opaque)
        }
        (ExprType::Unknown, other) | (other, ExprType::Unknown) => Ok(other),
        #[cfg(test)]
        (ExprType::Null, other) | (other, ExprType::Null) => Ok(other),
        (left, right) => Err(PlanError::from(ExprPlanError::invalid_function_argument(
            "COALESCE",
            0,
            format!("incompatible argument types {left:?} and {right:?}"),
        ))),
    }
}

pub(super) fn unify_case_branch_types(
    left: (&ExprType, &Expr),
    right: (&ExprType, &Expr),
) -> Result<ExprType, PlanError> {
    let (left_type, left_expr) = left;
    let (right_type, right_expr) = right;

    if left_type == right_type {
        return Ok(left_type.clone());
    }

    if case_branch_is_null_only(left_type, left_expr) {
        return Ok(right_type.clone());
    }
    if case_branch_is_null_only(right_type, right_expr) {
        return Ok(left_type.clone());
    }

    if left_type.is_numeric_eligible() && right_type.is_numeric_eligible() {
        return Ok(ExprType::Numeric(infer_numeric_result_subtype(
            BinaryOp::Add,
            left_type,
            right_type,
        )));
    }

    if blob_opaque_compatible(left_type, right_type) {
        return Ok(ExprType::Opaque);
    }

    Err(PlanError::from(
        ExprPlanError::incompatible_case_branch_types(
            format!("{left_type:?}"),
            format!("{right_type:?}"),
        ),
    ))
}

pub(super) const fn blob_opaque_compatible(left: &ExprType, right: &ExprType) -> bool {
    matches!(
        (left, right),
        (ExprType::Blob, ExprType::Opaque) | (ExprType::Opaque, ExprType::Blob)
    )
}

pub(super) const fn infer_numeric_result_subtype(
    op: BinaryOp,
    left: &ExprType,
    right: &ExprType,
) -> NumericSubtype {
    if matches!(op, BinaryOp::Div) {
        return NumericSubtype::Decimal;
    }

    let left_subtype = left.numeric_subtype();
    let right_subtype = right.numeric_subtype();
    let (Some(left_subtype), Some(right_subtype)) = (left_subtype, right_subtype) else {
        return if let Some(left_subtype) = left_subtype {
            left_subtype
        } else if let Some(right_subtype) = right_subtype {
            right_subtype
        } else {
            NumericSubtype::Integer
        };
    };

    match (left_subtype, right_subtype) {
        (NumericSubtype::Integer, NumericSubtype::Integer) => NumericSubtype::Integer,
        (NumericSubtype::Float, NumericSubtype::Float) => NumericSubtype::Float,
        (NumericSubtype::Decimal, NumericSubtype::Decimal) => NumericSubtype::Decimal,
        _ => NumericSubtype::Unknown,
    }
}

#[cfg(test)]
const fn case_branch_is_null_only(branch_type: &ExprType, expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(Value::Null)) || matches!(branch_type, ExprType::Null)
}

#[cfg(not(test))]
const fn case_branch_is_null_only(_branch_type: &ExprType, expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(Value::Null))
}

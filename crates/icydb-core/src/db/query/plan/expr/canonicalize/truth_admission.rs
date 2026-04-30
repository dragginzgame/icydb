//! Pure truth-admission predicates for planner boolean canonicalization.
//!
//! This module must stay below normalization and CASE lowering in the
//! dependency graph. It may inspect expression shape, but it must not call
//! normalization, CASE lowering, or rewrite entrypoints.

use crate::{
    db::{
        predicate::CompareOp,
        query::plan::expr::{
            BinaryOp, BooleanFunctionShape, Expr, Function, UnaryOp,
            function_is_compare_operand_coarse_family,
        },
    },
    value::Value,
};

///
/// TruthWrapperScope
///
/// TruthWrapperScope bounds equality-wrapper collapse to the caller's boolean
/// context.
/// Scalar `WHERE` and grouped `HAVING` share the admitted `= TRUE` / `= FALSE`
/// wrapper family, but grouped `HAVING` admits a slightly wider condition set.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::query::plan::expr::canonicalize) enum TruthWrapperScope {
    ScalarWhere,
    GroupedHaving,
}

///
/// TruthAdmission
///
/// TruthAdmission is the single planner authority for deciding which
/// expression families may act as boolean conditions during canonicalization.
/// It keeps scalar `WHERE`, scalar compare operands, and grouped `HAVING`
/// admission distinct so callers do not copy partial truth tables.
///

pub(in crate::db::query::plan::expr::canonicalize) struct TruthAdmission;

impl TruthAdmission {
    /// Return whether one expression is admitted as a scalar `WHERE`
    /// condition.
    pub(in crate::db::query::plan::expr::canonicalize) fn is_scalar_condition(expr: &Expr) -> bool {
        match expr {
            Expr::Field(_) | Expr::FieldPath(_) | Expr::Literal(Value::Bool(_) | Value::Null) => {
                true
            }
            Expr::Unary {
                op: UnaryOp::Not,
                expr,
            } => Self::is_scalar_condition(expr.as_ref()),
            Expr::Binary {
                op: BinaryOp::Eq,
                left,
                right,
            } if matches!(right.as_ref(), Expr::Literal(Value::Bool(true | false))) => {
                Self::is_scalar_condition(left.as_ref())
            }
            Expr::Binary {
                op: BinaryOp::Eq,
                left,
                right,
            } if matches!(left.as_ref(), Expr::Literal(Value::Bool(true | false))) => {
                Self::is_scalar_condition(right.as_ref())
            }
            Expr::Binary {
                op: BinaryOp::And | BinaryOp::Or,
                left,
                right,
            } => {
                Self::is_scalar_condition(left.as_ref())
                    && Self::is_scalar_condition(right.as_ref())
            }
            Expr::Binary { op, left, right }
                if truth_condition_binary_compare_op(*op).is_some() =>
            {
                Self::is_scalar_compare_operand(left.as_ref())
                    && Self::is_scalar_compare_operand(right.as_ref())
            }
            Expr::Binary { .. } => false,
            Expr::FunctionCall { function, args } => {
                scalar_truth_function_call_is_admitted(*function, args.as_slice())
            }
            Expr::Case {
                when_then_arms,
                else_expr,
            } => {
                when_then_arms.iter().all(|arm| {
                    Self::is_scalar_condition(arm.condition())
                        && Self::is_scalar_condition(arm.result())
                }) && Self::is_scalar_condition(else_expr.as_ref())
            }
            Expr::Aggregate(_) | Expr::Literal(_) => false,
            #[cfg(test)]
            Expr::Alias { expr, .. } => Self::is_scalar_condition(expr.as_ref()),
        }
    }

    /// Return whether one expression is admitted as a scalar compare operand.
    pub(in crate::db::query::plan::expr::canonicalize) fn is_scalar_compare_operand(
        expr: &Expr,
    ) -> bool {
        match expr {
            Expr::Field(_) | Expr::FieldPath(_) | Expr::Literal(_) => true,
            Expr::FunctionCall { function, args }
                if function_is_compare_operand_coarse_family(*function) =>
            {
                args.iter().all(Self::is_scalar_compare_operand)
            }
            Expr::Binary { op, left, right } if op.is_numeric_arithmetic() => {
                Self::is_scalar_compare_operand(left.as_ref())
                    && Self::is_scalar_compare_operand(right.as_ref())
            }
            Expr::Case {
                when_then_arms,
                else_expr,
            } => {
                when_then_arms.iter().all(|arm| {
                    Self::is_scalar_condition(arm.condition())
                        && Self::is_scalar_compare_operand(arm.result())
                }) && Self::is_scalar_compare_operand(else_expr.as_ref())
            }
            Expr::Aggregate(_)
            | Expr::Unary { .. }
            | Expr::FunctionCall { .. }
            | Expr::Binary { .. } => false,
            #[cfg(test)]
            Expr::Alias { expr, .. } => Self::is_scalar_compare_operand(expr.as_ref()),
        }
    }

    /// Return whether one expression is admitted as a grouped `HAVING`
    /// condition for truth-wrapper collapse.
    pub(in crate::db::query::plan::expr::canonicalize) fn is_grouped_condition(
        expr: &Expr,
    ) -> bool {
        match expr {
            Expr::Field(_) | Expr::FieldPath(_) | Expr::Literal(Value::Bool(_) | Value::Null) => {
                true
            }
            Expr::Unary {
                op: UnaryOp::Not,
                expr,
            } => Self::is_grouped_condition(expr.as_ref()),
            Expr::Binary {
                op: BinaryOp::And | BinaryOp::Or,
                left,
                right,
            } => {
                Self::is_grouped_condition(left.as_ref())
                    || Self::is_grouped_condition(right.as_ref())
            }
            Expr::Binary { op, .. } if truth_condition_binary_compare_op(*op).is_some() => true,
            Expr::Binary { .. } => false,
            Expr::Case {
                when_then_arms,
                else_expr,
            } => {
                when_then_arms.iter().all(|arm| {
                    Self::is_grouped_condition(arm.condition())
                        && Self::is_grouped_condition(arm.result())
                }) && Self::is_grouped_condition(else_expr.as_ref())
            }
            Expr::FunctionCall { function, args } => match function.boolean_function_shape() {
                Some(BooleanFunctionShape::TruthCoalesce) => {
                    args.iter().all(Self::is_grouped_condition)
                }
                Some(
                    BooleanFunctionShape::NullTest
                    | BooleanFunctionShape::FieldPredicate
                    | BooleanFunctionShape::TextPredicate,
                ) => true,
                Some(BooleanFunctionShape::CollectionContains) | None => false,
            },
            Expr::Aggregate(_) | Expr::Literal(_) => false,
            #[cfg(test)]
            Expr::Alias { expr, .. } => Self::is_grouped_condition(expr.as_ref()),
        }
    }
}

/// Resolve one planner truth-condition compare operator onto the binary
/// expression family used by normalized expression trees.
#[must_use]
pub(in crate::db) const fn truth_condition_compare_binary_op(op: CompareOp) -> Option<BinaryOp> {
    match op {
        CompareOp::Eq => Some(BinaryOp::Eq),
        CompareOp::Ne => Some(BinaryOp::Ne),
        CompareOp::Lt => Some(BinaryOp::Lt),
        CompareOp::Lte => Some(BinaryOp::Lte),
        CompareOp::Gt => Some(BinaryOp::Gt),
        CompareOp::Gte => Some(BinaryOp::Gte),
        CompareOp::In
        | CompareOp::NotIn
        | CompareOp::Contains
        | CompareOp::StartsWith
        | CompareOp::EndsWith => None,
    }
}

/// Resolve one planner binary compare operator back onto the admitted
/// truth-condition compare family.
#[must_use]
pub(in crate::db) const fn truth_condition_binary_compare_op(op: BinaryOp) -> Option<CompareOp> {
    match op {
        BinaryOp::Eq => Some(CompareOp::Eq),
        BinaryOp::Ne => Some(CompareOp::Ne),
        BinaryOp::Lt => Some(CompareOp::Lt),
        BinaryOp::Lte => Some(CompareOp::Lte),
        BinaryOp::Gt => Some(CompareOp::Gt),
        BinaryOp::Gte => Some(CompareOp::Gte),
        BinaryOp::And
        | BinaryOp::Or
        | BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div => None,
    }
}

/// Report whether one planner expression belongs to the admitted scalar-WHERE
/// truth-condition family.
pub(in crate::db) fn scalar_where_truth_condition_is_admitted(expr: &Expr) -> bool {
    TruthAdmission::is_scalar_condition(expr)
}

// Keep scalar truth-condition admission aligned with the bounded boolean
// function family already consumed by scalar WHERE lowering and predicate
// compilation.
fn scalar_truth_function_call_is_admitted(function: Function, args: &[Expr]) -> bool {
    bool_function_args_match(
        function,
        args,
        TruthAdmission::is_scalar_condition,
        TruthAdmission::is_scalar_compare_operand,
        false,
    )
}

// Validate the shared boolean-function argument skeleton while letting callers
// supply their own truth-context and compare-operand admission predicates.
pub(in crate::db::query::plan::expr::canonicalize) fn bool_function_args_match(
    function: Function,
    args: &[Expr],
    truth_arg: impl Fn(&Expr) -> bool,
    compare_arg: impl Fn(&Expr) -> bool,
    truth_coalesce_requires_args: bool,
) -> bool {
    match function.boolean_function_shape() {
        Some(BooleanFunctionShape::TruthCoalesce) => {
            (!truth_coalesce_requires_args || !args.is_empty()) && args.iter().all(truth_arg)
        }
        Some(BooleanFunctionShape::NullTest) => {
            matches!(args, [arg] if compare_arg(arg))
        }
        Some(BooleanFunctionShape::TextPredicate) => {
            matches!(args, [left, right] if compare_arg(left) && compare_arg(right))
        }
        Some(BooleanFunctionShape::FieldPredicate) => {
            matches!(args, [Expr::Field(_)])
        }
        Some(BooleanFunctionShape::CollectionContains) => {
            matches!(args, [Expr::Field(_), Expr::Literal(_)])
        }
        None => false,
    }
}

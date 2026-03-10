//! Module: query::plan::expr::type_inference
//! Responsibility: infer deterministic planner expression type classes from schema and AST.
//! Does not own: runtime projection evaluation or expression execution behavior.
//! Boundary: returns planner-domain type information and typed plan errors.

use crate::{
    db::{
        numeric::field_kind_supports_expr_numeric,
        query::{
            builder::aggregate::AggregateExpr,
            plan::{
                AggregateKind, PlanError,
                expr::ast::{BinaryOp, Expr, FieldId, UnaryOp},
                validate::ExprPlanError,
            },
        },
        schema::SchemaInfo,
    },
    model::field::FieldKind,
    value::Value,
};

///
/// ExprType
///
/// Minimal deterministic expression type classification for planner inference.
/// This intentionally remains coarse in the bootstrap phase.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ExprType {
    Bool,
    Numeric(NumericSubtype),
    Text,
    Null,
    Collection,
    Structured,
    Opaque,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NumericSubtype {
    Integer,
    Float,
    Decimal,
    Unknown,
}

impl ExprType {
    // Eligibility answers "can this participate in numeric-only operators?".
    // Subtype answers "which numeric family?" and may remain unresolved.
    const fn is_numeric_eligible(&self) -> bool {
        matches!(self, Self::Numeric(_))
    }

    const fn numeric_subtype(&self) -> Option<NumericSubtype> {
        match self {
            Self::Numeric(subtype) => Some(*subtype),
            _ => None,
        }
    }
}

/// Infer expression type deterministically from canonical expression shape.
pub(crate) fn infer_expr_type(expr: &Expr, schema: &SchemaInfo) -> Result<ExprType, PlanError> {
    match expr {
        Expr::Field(field) => infer_field_expr_type(field, schema),
        Expr::Literal(value) => Ok(infer_literal_type(value)),
        Expr::Aggregate(aggregate) => infer_aggregate_expr_type(aggregate, schema),
        Expr::Alias { expr, .. } => infer_expr_type(expr.as_ref(), schema),
        Expr::Unary { op, expr } => infer_unary_expr_type(*op, expr.as_ref(), schema),
        Expr::Binary { op, left, right } => {
            infer_binary_expr_type(*op, left.as_ref(), right.as_ref(), schema)
        }
    }
}

fn infer_field_expr_type(field: &FieldId, schema: &SchemaInfo) -> Result<ExprType, PlanError> {
    let field_name = field.as_str();
    let Some(field_kind) = schema.field_kind(field_name) else {
        return Err(PlanError::from(ExprPlanError::UnknownExprField {
            field: field_name.to_string(),
        }));
    };

    Ok(expr_type_from_field_kind(field_kind))
}

fn infer_aggregate_expr_type(
    aggregate: &AggregateExpr,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let kind = aggregate.kind();
    let target_field = aggregate.target_field();

    match kind {
        AggregateKind::Count => Ok(ExprType::Numeric(NumericSubtype::Integer)),
        AggregateKind::Exists => Ok(ExprType::Bool),
        AggregateKind::Sum => infer_sum_aggregate_type(target_field, schema),
        AggregateKind::Min | AggregateKind::Max | AggregateKind::First | AggregateKind::Last => {
            infer_target_field_aggregate_type(kind, target_field, schema)
        }
    }
}

fn infer_sum_aggregate_type(
    target_field: Option<&str>,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let Some(field_name) = target_field else {
        return Err(PlanError::from(ExprPlanError::AggregateTargetRequired {
            kind: "sum".to_string(),
        }));
    };

    let Some(field_kind) = schema.field_kind(field_name) else {
        return Err(PlanError::from(ExprPlanError::UnknownExprField {
            field: field_name.to_string(),
        }));
    };

    if !field_kind_supports_expr_numeric(field_kind) {
        return Err(PlanError::from(ExprPlanError::NonNumericAggregateTarget {
            kind: "sum".to_string(),
            field: field_name.to_string(),
        }));
    }

    Ok(expr_type_from_field_kind(field_kind))
}

fn infer_target_field_aggregate_type(
    kind: AggregateKind,
    target_field: Option<&str>,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let Some(field_name) = target_field else {
        // Bootstrap behavior: target-less extrema/value terminals stay unresolved.
        return Ok(ExprType::Unknown);
    };

    let Some(field_kind) = schema.field_kind(field_name) else {
        return Err(PlanError::from(ExprPlanError::UnknownExprField {
            field: field_name.to_string(),
        }));
    };

    let _ = kind;
    Ok(expr_type_from_field_kind(field_kind))
}

fn infer_unary_expr_type(
    op: UnaryOp,
    expr: &Expr,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let inner = infer_expr_type(expr, schema)?;

    match op {
        UnaryOp::Neg => {
            if !inner.is_numeric_eligible() {
                return Err(PlanError::from(ExprPlanError::InvalidUnaryOperand {
                    op: "neg".to_string(),
                    found: format!("{inner:?}"),
                }));
            }

            Ok(ExprType::Numeric(
                inner.numeric_subtype().unwrap_or(NumericSubtype::Unknown),
            ))
        }
        UnaryOp::Not => {
            if !matches!(inner, ExprType::Bool) {
                return Err(PlanError::from(ExprPlanError::InvalidUnaryOperand {
                    op: "not".to_string(),
                    found: format!("{inner:?}"),
                }));
            }

            Ok(ExprType::Bool)
        }
    }
}

fn infer_binary_expr_type(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let left_ty = infer_expr_type(left, schema)?;
    let right_ty = infer_expr_type(right, schema)?;

    match op {
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div => {
            if !binary_numeric_compatible(&left_ty, &right_ty) {
                return Err(PlanError::from(ExprPlanError::InvalidBinaryOperands {
                    op: binary_op_name(op).to_string(),
                    left: format!("{left_ty:?}"),
                    right: format!("{right_ty:?}"),
                }));
            }

            Ok(ExprType::Numeric(infer_numeric_result_subtype(
                op, &left_ty, &right_ty,
            )))
        }
        BinaryOp::And | BinaryOp::Or => {
            if !matches!(left_ty, ExprType::Bool) || !matches!(right_ty, ExprType::Bool) {
                return Err(PlanError::from(ExprPlanError::InvalidBinaryOperands {
                    op: binary_op_name(op).to_string(),
                    left: format!("{left_ty:?}"),
                    right: format!("{right_ty:?}"),
                }));
            }

            Ok(ExprType::Bool)
        }
        BinaryOp::Eq | BinaryOp::Ne => {
            if !binary_equality_comparable(&left_ty, &right_ty) {
                return Err(PlanError::from(ExprPlanError::InvalidBinaryOperands {
                    op: binary_op_name(op).to_string(),
                    left: format!("{left_ty:?}"),
                    right: format!("{right_ty:?}"),
                }));
            }

            Ok(ExprType::Bool)
        }
        BinaryOp::Lt | BinaryOp::Lte | BinaryOp::Gt | BinaryOp::Gte => {
            if !binary_order_comparable(&left_ty, &right_ty) {
                return Err(PlanError::from(ExprPlanError::InvalidBinaryOperands {
                    op: binary_op_name(op).to_string(),
                    left: format!("{left_ty:?}"),
                    right: format!("{right_ty:?}"),
                }));
            }

            Ok(ExprType::Bool)
        }
    }
}

const fn binary_numeric_compatible(left: &ExprType, right: &ExprType) -> bool {
    left.is_numeric_eligible() && right.is_numeric_eligible()
}

const fn binary_equality_comparable(left: &ExprType, right: &ExprType) -> bool {
    if left.is_numeric_eligible() && right.is_numeric_eligible() {
        return true;
    }

    matches!(
        (left, right),
        (ExprType::Bool, ExprType::Bool)
            | (ExprType::Text, ExprType::Text)
            | (ExprType::Null, ExprType::Null)
            | (ExprType::Collection, ExprType::Collection)
            | (ExprType::Structured, ExprType::Structured)
            | (ExprType::Opaque, ExprType::Opaque)
    )
}

const fn binary_order_comparable(left: &ExprType, right: &ExprType) -> bool {
    if left.is_numeric_eligible() && right.is_numeric_eligible() {
        return true;
    }

    matches!(
        (left, right),
        (ExprType::Bool, ExprType::Bool) | (ExprType::Text, ExprType::Text)
    )
}

const fn infer_numeric_result_subtype(
    _op: BinaryOp,
    left: &ExprType,
    right: &ExprType,
) -> NumericSubtype {
    let (Some(left_subtype), Some(right_subtype)) =
        (left.numeric_subtype(), right.numeric_subtype())
    else {
        return NumericSubtype::Unknown;
    };

    match (left_subtype, right_subtype) {
        (NumericSubtype::Integer, NumericSubtype::Integer) => NumericSubtype::Integer,
        (NumericSubtype::Float, NumericSubtype::Float) => NumericSubtype::Float,
        (NumericSubtype::Decimal, NumericSubtype::Decimal) => NumericSubtype::Decimal,
        _ => NumericSubtype::Unknown,
    }
}

const fn infer_literal_type(value: &Value) -> ExprType {
    match value {
        Value::Bool(_) => ExprType::Bool,
        Value::Text(_) | Value::Enum(_) => ExprType::Text,
        Value::Int(_)
        | Value::Int128(_)
        | Value::IntBig(_)
        | Value::Uint(_)
        | Value::Uint128(_)
        | Value::UintBig(_)
        | Value::Duration(_)
        | Value::Timestamp(_) => ExprType::Numeric(NumericSubtype::Integer),
        Value::Float32(_) | Value::Float64(_) => ExprType::Numeric(NumericSubtype::Float),
        Value::Decimal(_) => ExprType::Numeric(NumericSubtype::Decimal),
        Value::List(_) | Value::Map(_) => ExprType::Collection,
        Value::Null => ExprType::Null,
        Value::Account(_)
        | Value::Blob(_)
        | Value::Date(_)
        | Value::Principal(_)
        | Value::Subaccount(_)
        | Value::Ulid(_)
        | Value::Unit => ExprType::Opaque,
    }
}

fn expr_type_from_field_kind(kind: &FieldKind) -> ExprType {
    match kind {
        FieldKind::Bool => ExprType::Bool,
        FieldKind::Int
        | FieldKind::Int128
        | FieldKind::IntBig
        | FieldKind::Uint
        | FieldKind::Uint128
        | FieldKind::UintBig
        | FieldKind::Duration
        | FieldKind::Timestamp => ExprType::Numeric(NumericSubtype::Integer),
        FieldKind::Float32 | FieldKind::Float64 => ExprType::Numeric(NumericSubtype::Float),
        FieldKind::Decimal { .. } => ExprType::Numeric(NumericSubtype::Decimal),
        FieldKind::Text | FieldKind::Enum { .. } => ExprType::Text,
        FieldKind::List(_) | FieldKind::Set(_) | FieldKind::Map { .. } => ExprType::Collection,
        FieldKind::Structured { .. } => ExprType::Structured,
        FieldKind::Relation { key_kind, .. } => expr_type_from_field_kind(key_kind),
        FieldKind::Account
        | FieldKind::Blob
        | FieldKind::Date
        | FieldKind::Principal
        | FieldKind::Subaccount
        | FieldKind::Ulid
        | FieldKind::Unit => ExprType::Opaque,
    }
}

const fn binary_op_name(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Add => "add",
        BinaryOp::Sub => "sub",
        BinaryOp::Mul => "mul",
        BinaryOp::Div => "div",
        BinaryOp::And => "and",
        BinaryOp::Or => "or",
        BinaryOp::Eq => "eq",
        BinaryOp::Ne => "ne",
        BinaryOp::Lt => "lt",
        BinaryOp::Lte => "lte",
        BinaryOp::Gt => "gt",
        BinaryOp::Gte => "gte",
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;

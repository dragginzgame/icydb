//! Module: query::plan::expr::type_inference
//! Responsibility: infer deterministic planner expression type classes from schema and AST.
//! Does not own: runtime projection evaluation or expression execution behavior.
//! Boundary: returns planner-domain type information and typed plan errors.

#[cfg(test)]
use crate::db::query::plan::expr::ast::{BinaryOp, UnaryOp};
#[cfg(test)]
use crate::value::Value;
use crate::{
    db::{
        numeric::field_kind_supports_expr_numeric,
        query::{
            builder::aggregate::AggregateExpr,
            plan::{
                AggregateKind, PlanError,
                expr::ast::{Expr, FieldId},
                validate::ExprPlanError,
            },
        },
        schema::SchemaInfo,
    },
    model::field::FieldKind,
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
    #[cfg(test)]
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
    #[cfg(test)]
    Unknown,
}

impl ExprType {
    // Eligibility answers "can this participate in numeric-only operators?".
    // Subtype answers "which numeric family?" and may remain unresolved.
    #[cfg(test)]
    const fn is_numeric_eligible(&self) -> bool {
        matches!(self, Self::Numeric(_))
    }

    #[cfg(test)]
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
        #[cfg(test)]
        Expr::Literal(value) => Ok(infer_literal_type(value)),
        Expr::Aggregate(aggregate) => infer_aggregate_expr_type(aggregate, schema),
        #[cfg(test)]
        Expr::Alias { expr, .. } => infer_expr_type(expr.as_ref(), schema),
        #[cfg(test)]
        Expr::Unary { op, expr } => infer_unary_expr_type(*op, expr.as_ref(), schema),
        #[cfg(test)]
        Expr::Binary { op, left, right } => {
            infer_binary_expr_type(*op, left.as_ref(), right.as_ref(), schema)
        }
    }
}

fn resolve_expr_field_kind<'a>(
    field_name: &str,
    schema: &'a SchemaInfo,
) -> Result<&'a FieldKind, PlanError> {
    schema
        .field_kind(field_name)
        .ok_or_else(|| PlanError::from(ExprPlanError::unknown_expr_field(field_name)))
}

fn infer_field_expr_type(field: &FieldId, schema: &SchemaInfo) -> Result<ExprType, PlanError> {
    let field_name = field.as_str();
    let field_kind = resolve_expr_field_kind(field_name, schema)?;

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
        AggregateKind::Sum => infer_sum_aggregate_type(target_field, schema, "sum"),
        AggregateKind::Avg => infer_sum_aggregate_type(target_field, schema, "avg"),
        AggregateKind::Min | AggregateKind::Max | AggregateKind::First | AggregateKind::Last => {
            infer_target_field_aggregate_type(kind, target_field, schema)
        }
    }
}

fn infer_sum_aggregate_type(
    target_field: Option<&str>,
    schema: &SchemaInfo,
    aggregate_name: &str,
) -> Result<ExprType, PlanError> {
    let Some(field_name) = target_field else {
        return Err(PlanError::from(ExprPlanError::aggregate_target_required(
            aggregate_name,
        )));
    };

    let field_kind = resolve_expr_field_kind(field_name, schema)?;

    if !field_kind_supports_expr_numeric(field_kind) {
        return Err(PlanError::from(
            ExprPlanError::non_numeric_aggregate_target(aggregate_name, field_name),
        ));
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

    let field_kind = resolve_expr_field_kind(field_name, schema)?;

    let _ = kind;
    Ok(expr_type_from_field_kind(field_kind))
}

#[cfg(test)]
fn infer_unary_expr_type(
    op: UnaryOp,
    expr: &Expr,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let inner = infer_expr_type(expr, schema)?;

    match op {
        UnaryOp::Not => {
            if !matches!(inner, ExprType::Bool) {
                return Err(PlanError::from(ExprPlanError::invalid_unary_operand(
                    "not",
                    format!("{inner:?}"),
                )));
            }

            Ok(ExprType::Bool)
        }
    }
}

#[cfg(test)]
fn infer_binary_expr_type(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let left_ty = infer_expr_type(left, schema)?;
    let right_ty = infer_expr_type(right, schema)?;

    match op {
        BinaryOp::Add | BinaryOp::Mul => {
            if !binary_numeric_compatible(&left_ty, &right_ty) {
                return Err(PlanError::from(ExprPlanError::invalid_binary_operands(
                    binary_op_name(op),
                    format!("{left_ty:?}"),
                    format!("{right_ty:?}"),
                )));
            }

            Ok(ExprType::Numeric(infer_numeric_result_subtype(
                op, &left_ty, &right_ty,
            )))
        }
        BinaryOp::And => {
            if !matches!(left_ty, ExprType::Bool) || !matches!(right_ty, ExprType::Bool) {
                return Err(PlanError::from(ExprPlanError::invalid_binary_operands(
                    binary_op_name(op),
                    format!("{left_ty:?}"),
                    format!("{right_ty:?}"),
                )));
            }

            Ok(ExprType::Bool)
        }
        BinaryOp::Eq => {
            if !binary_equality_comparable(&left_ty, &right_ty) {
                return Err(PlanError::from(ExprPlanError::invalid_binary_operands(
                    binary_op_name(op),
                    format!("{left_ty:?}"),
                    format!("{right_ty:?}"),
                )));
            }

            Ok(ExprType::Bool)
        }
    }
}

#[cfg(test)]
const fn binary_numeric_compatible(left: &ExprType, right: &ExprType) -> bool {
    left.is_numeric_eligible() && right.is_numeric_eligible()
}

#[cfg(test)]
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

#[cfg(test)]
const fn infer_numeric_result_subtype(
    _op: BinaryOp,
    left: &ExprType,
    right: &ExprType,
) -> NumericSubtype {
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

#[cfg(test)]
const fn binary_op_name(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Add => "add",
        BinaryOp::Mul => "mul",
        BinaryOp::And => "and",
        BinaryOp::Eq => "eq",
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;

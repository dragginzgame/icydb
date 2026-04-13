//! Module: query::plan::expr::type_inference
//! Responsibility: infer deterministic planner expression type classes from schema and AST.
//! Does not own: runtime projection evaluation or expression execution behavior.
//! Boundary: returns planner-domain type information and typed plan errors.

#[cfg(test)]
use crate::db::query::plan::expr::ast::UnaryOp;
use crate::value::Value;
use crate::{
    db::{
        numeric::field_kind_supports_expr_numeric,
        query::{
            builder::aggregate::AggregateExpr,
            plan::{
                AggregateKind, PlanError,
                expr::ast::{BinaryOp, Expr, FieldId, Function},
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
        Expr::FunctionCall { function, args } => {
            infer_function_expr_type(*function, args.as_slice(), schema)
        }
        Expr::Aggregate(aggregate) => infer_aggregate_expr_type(aggregate, schema),
        #[cfg(test)]
        Expr::Alias { expr, .. } => infer_expr_type(expr.as_ref(), schema),
        #[cfg(test)]
        Expr::Unary { op, expr } => infer_unary_expr_type(*op, expr.as_ref(), schema),
        Expr::Binary { op, left, right } => {
            infer_binary_expr_type(*op, left.as_ref(), right.as_ref(), schema)
        }
    }
}

fn infer_function_expr_type(
    function: Function,
    args: &[Expr],
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let arg_types = args
        .iter()
        .map(|arg| infer_expr_type(arg, schema))
        .collect::<Result<Vec<_>, _>>()?;

    match function {
        Function::Trim
        | Function::Ltrim
        | Function::Rtrim
        | Function::Lower
        | Function::Upper
        | Function::Left
        | Function::Right
        | Function::Replace
        | Function::Substring => {
            validate_text_function_args(function, arg_types.as_slice())?;

            Ok(ExprType::Text)
        }
        Function::Length | Function::Position => {
            validate_text_function_args(function, arg_types.as_slice())?;

            Ok(ExprType::Numeric(NumericSubtype::Integer))
        }
        Function::Round => {
            validate_numeric_round_function_args(arg_types.as_slice())?;

            Ok(ExprType::Numeric(NumericSubtype::Decimal))
        }
        Function::StartsWith | Function::EndsWith | Function::Contains => {
            validate_text_function_args(function, arg_types.as_slice())?;

            Ok(ExprType::Bool)
        }
    }
}

fn validate_text_function_args(function: Function, args: &[ExprType]) -> Result<(), PlanError> {
    for (index, arg) in args.iter().enumerate() {
        #[cfg(test)]
        if matches!(arg, ExprType::Null) {
            continue;
        }

        let text_positions = match function {
            Function::Trim
            | Function::Ltrim
            | Function::Rtrim
            | Function::Lower
            | Function::Upper
            | Function::Length
            | Function::Left
            | Function::Right
            | Function::Substring => &[0][..],
            Function::StartsWith | Function::EndsWith | Function::Contains | Function::Position => {
                &[0, 1][..]
            }
            Function::Replace => &[0, 1, 2],
            Function::Round => &[][..],
        };

        let numeric_positions = match function {
            Function::Left | Function::Right => &[1][..],
            Function::Substring => &[1, 2],
            _ => &[][..],
        };

        if text_positions.contains(&index) && !matches!(arg, ExprType::Text) {
            return Err(PlanError::from(ExprPlanError::invalid_function_argument(
                function.sql_label(),
                index,
                format!("{arg:?}"),
            )));
        }
        let numeric_compatible = matches!(arg, ExprType::Numeric(_)) || {
            #[cfg(test)]
            {
                matches!(arg, ExprType::Null)
            }
            #[cfg(not(test))]
            {
                false
            }
        };

        if numeric_positions.contains(&index) && !numeric_compatible {
            return Err(PlanError::from(ExprPlanError::invalid_function_argument(
                function.sql_label(),
                index,
                format!("{arg:?}"),
            )));
        }
    }

    Ok(())
}

fn validate_numeric_round_function_args(args: &[ExprType]) -> Result<(), PlanError> {
    if args.len() != 2 {
        return Err(PlanError::from(ExprPlanError::invalid_function_argument(
            "ROUND",
            args.len(),
            format!("expected exactly 2 args, found {}", args.len()),
        )));
    }

    if !matches!(args[0], ExprType::Numeric(_)) {
        return Err(PlanError::from(ExprPlanError::invalid_function_argument(
            "ROUND",
            0,
            format!("{:?}", args[0]),
        )));
    }

    let scale_compatible = matches!(args[1], ExprType::Numeric(NumericSubtype::Integer)) || {
        #[cfg(test)]
        {
            matches!(args[1], ExprType::Null)
        }
        #[cfg(not(test))]
        {
            false
        }
    };

    if !scale_compatible {
        return Err(PlanError::from(ExprPlanError::invalid_function_argument(
            "ROUND",
            1,
            format!("{:?}", args[1]),
        )));
    }

    Ok(())
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
        #[cfg(test)]
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
        #[cfg(test)]
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

const fn binary_numeric_compatible(left: &ExprType, right: &ExprType) -> bool {
    left.is_numeric_eligible() && right.is_numeric_eligible()
}

#[cfg(test)]
const fn binary_equality_comparable(left: &ExprType, right: &ExprType) -> bool {
    if left.is_numeric_eligible() && right.is_numeric_eligible() {
        return true;
    }

    #[cfg(test)]
    if matches!((left, right), (ExprType::Null, ExprType::Null)) {
        return true;
    }

    matches!(
        (left, right),
        (ExprType::Bool, ExprType::Bool)
            | (ExprType::Text, ExprType::Text)
            | (ExprType::Collection, ExprType::Collection)
            | (ExprType::Structured, ExprType::Structured)
            | (ExprType::Opaque, ExprType::Opaque)
    )
}

const fn infer_numeric_result_subtype(
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
        Value::Null => {
            #[cfg(test)]
            {
                ExprType::Null
            }
            #[cfg(not(test))]
            {
                ExprType::Unknown
            }
        }
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
        #[cfg(test)]
        BinaryOp::And => "and",
        #[cfg(test)]
        BinaryOp::Eq => "eq",
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;

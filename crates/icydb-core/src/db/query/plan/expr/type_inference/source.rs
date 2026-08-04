use crate::{
    db::{
        query::plan::{
            PlanError,
            expr::{FieldId, FieldPath, NumericSubtype, type_inference::ExprType},
            validate::ExprPlanError,
        },
        schema::{FieldType, SchemaInfo},
    },
    value::Value,
};
use icydb_schema::ScalarKind;

pub(super) fn infer_field_expr_type(
    field: &FieldId,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let field_name = field.as_str();
    let field_type = schema
        .field(field_name)
        .ok_or_else(|| PlanError::from(ExprPlanError::unknown_expr_field(field_name)))?;

    Ok(expr_type_from_field_type(field_type))
}

pub(super) fn infer_field_path_expr_type(
    path: &FieldPath,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let root = path.root().as_str();
    if schema.field(root).is_none() {
        return Err(PlanError::from(ExprPlanError::unknown_expr_field(root)));
    }

    if !schema.field_has_nested_paths(root) {
        return Ok(ExprType::Unknown);
    }

    let field_type = schema.nested_field_type(root, path.segments());

    field_type.map_or_else(
        || {
            Err(PlanError::from(ExprPlanError::unknown_expr_field(
                render_field_path(path),
            )))
        },
        |field_type| Ok(expr_type_from_field_type(&field_type)),
    )
}

pub(super) fn render_field_path(path: &FieldPath) -> String {
    let mut label = path.root().as_str().to_string();
    for segment in path.segments() {
        label.push('.');
        label.push_str(segment);
    }

    label
}

pub(super) const fn infer_literal_type(value: &Value) -> ExprType {
    match value {
        Value::Bool(_) => ExprType::Bool,
        Value::Text(_) | Value::Enum(_) => ExprType::Text,
        Value::Blob(_) => ExprType::Blob,
        Value::Int64(_)
        | Value::Int128(_)
        | Value::IntBig(_)
        | Value::Nat64(_)
        | Value::Nat128(_)
        | Value::NatBig(_)
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
        | Value::Date(_)
        | Value::Principal(_)
        | Value::Subaccount(_)
        | Value::Ulid(_)
        | Value::Unit => ExprType::Opaque,
    }
}

pub(super) const fn expr_type_from_field_type(field_type: &FieldType) -> ExprType {
    match field_type {
        FieldType::Scalar(ScalarKind::Blob) => ExprType::Blob,
        FieldType::Scalar(ScalarKind::Bool) => ExprType::Bool,
        FieldType::Scalar(
            ScalarKind::Duration
            | ScalarKind::Int
            | ScalarKind::Int128
            | ScalarKind::IntBig
            | ScalarKind::Timestamp
            | ScalarKind::Nat
            | ScalarKind::Nat128
            | ScalarKind::NatBig,
        ) => ExprType::Numeric(NumericSubtype::Integer),
        FieldType::Scalar(ScalarKind::Float32 | ScalarKind::Float64) => {
            ExprType::Numeric(NumericSubtype::Float)
        }
        FieldType::Scalar(ScalarKind::Decimal) => ExprType::Numeric(NumericSubtype::Decimal),
        FieldType::Scalar(ScalarKind::Enum | ScalarKind::Text) => ExprType::Text,
        FieldType::List(_) | FieldType::Set(_) | FieldType::Map { .. } => ExprType::Collection,
        FieldType::Composite => ExprType::Structured,
        FieldType::Scalar(
            ScalarKind::Account
            | ScalarKind::Date
            | ScalarKind::Principal
            | ScalarKind::Subaccount
            | ScalarKind::Ulid
            | ScalarKind::Unit,
        ) => ExprType::Opaque,
    }
}

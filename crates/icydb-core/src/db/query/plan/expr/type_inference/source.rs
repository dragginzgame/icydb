use crate::{
    db::{
        QueryError,
        query::plan::{
            PlanError,
            expr::{FieldId, FieldPath, NumericSubtype, type_inference::ExprType},
            validate::ExprPlanError,
        },
        query::preparation::PreparationWork,
        schema::{FieldType, SchemaInfo},
    },
    value::Value,
};
use icydb_schema::ScalarKind;

pub(super) fn infer_field_expr_type(
    field: &FieldId,
    schema: &SchemaInfo,
    work: &PreparationWork<'_>,
) -> Result<ExprType, QueryError> {
    let field_name = field.as_str();
    let Some(field_type) = schema.field(field_name) else {
        return Err(PlanError::from(ExprPlanError::unknown_expr_field(
            work.copy_text(field_name)?,
        ))
        .into());
    };

    Ok(expr_type_from_field_type(field_type))
}

pub(super) fn infer_field_path_expr_type(
    path: &FieldPath,
    schema: &SchemaInfo,
    work: &PreparationWork<'_>,
) -> Result<ExprType, QueryError> {
    let root = path.root().as_str();
    let Some(nested_fields) = schema.nested_fields(root) else {
        return Err(
            PlanError::from(ExprPlanError::unknown_expr_field(work.copy_text(root)?)).into(),
        );
    };

    if nested_fields.is_empty() {
        return Ok(ExprType::Unknown);
    }

    let field_type = nested_fields.field_type(path.segments().iter().map(String::as_str));

    if let Some(field_type) = field_type {
        return Ok(expr_type_from_field_type(&field_type));
    }
    let label = work.render_text(|out| {
        out.write_str(root)?;
        for segment in path.segments() {
            out.write_str(".")?;
            out.write_str(segment)?;
        }
        Ok(())
    })?;
    Err(PlanError::from(ExprPlanError::unknown_expr_field(label)).into())
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
        Value::Null => ExprType::Null,
        Value::U256(_) => ExprType::U256,
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
        FieldType::Scalar(ScalarKind::U256) => ExprType::U256,
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

use crate::{
    db::{
        query::plan::{
            PlanError,
            expr::{FieldId, FieldPath, NumericSubtype, type_inference::ExprType},
            validate::ExprPlanError,
        },
        schema::SchemaInfo,
    },
    model::{
        FieldKindCategory, FieldKindNumericClass, FieldKindScalarClass, classify_field_kind,
        field::{FieldKind, FieldModel},
    },
    value::Value,
};

pub(super) fn resolve_expr_field_kind<'a>(
    field_name: &str,
    schema: &'a SchemaInfo,
) -> Result<&'a FieldKind, PlanError> {
    schema
        .field_kind(field_name)
        .ok_or_else(|| PlanError::from(ExprPlanError::unknown_expr_field(field_name)))
}

pub(super) fn infer_field_expr_type(
    field: &FieldId,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let field_name = field.as_str();
    let field_kind = resolve_expr_field_kind(field_name, schema)?;

    Ok(expr_type_from_field_kind(field_kind))
}

pub(super) fn infer_field_path_expr_type(
    path: &FieldPath,
    schema: &SchemaInfo,
) -> Result<ExprType, PlanError> {
    let root = path.root().as_str();
    let nested_fields = schema
        .field_nested_fields(root)
        .ok_or_else(|| PlanError::from(ExprPlanError::unknown_expr_field(root)))?;

    if nested_fields.is_empty() {
        return Ok(ExprType::Unknown);
    }

    let field_kind =
        resolve_nested_field_path_kind(nested_fields, path.segments()).ok_or_else(|| {
            PlanError::from(ExprPlanError::unknown_expr_field(render_field_path(path)))
        })?;

    Ok(expr_type_from_field_kind(&field_kind))
}

fn resolve_nested_field_path_kind(fields: &[FieldModel], segments: &[String]) -> Option<FieldKind> {
    let (segment, rest) = segments.split_first()?;
    let field = fields
        .iter()
        .find(|field| field.name() == segment.as_str())?;

    if rest.is_empty() {
        return Some(field.kind());
    }

    resolve_nested_field_path_kind(field.nested_fields(), rest)
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
        | Value::Date(_)
        | Value::Principal(_)
        | Value::Subaccount(_)
        | Value::Ulid(_)
        | Value::Unit => ExprType::Opaque,
    }
}

pub(super) const fn expr_type_from_field_kind(kind: &FieldKind) -> ExprType {
    if matches!(kind, FieldKind::Blob) {
        return ExprType::Blob;
    }

    match classify_field_kind(kind).category() {
        FieldKindCategory::Scalar(FieldKindScalarClass::Boolean)
        | FieldKindCategory::Relation(FieldKindScalarClass::Boolean) => ExprType::Bool,
        FieldKindCategory::Scalar(FieldKindScalarClass::Numeric(
            FieldKindNumericClass::Signed64
            | FieldKindNumericClass::Unsigned64
            | FieldKindNumericClass::SignedWide
            | FieldKindNumericClass::UnsignedWide
            | FieldKindNumericClass::DurationLike
            | FieldKindNumericClass::TimestampLike,
        ))
        | FieldKindCategory::Relation(FieldKindScalarClass::Numeric(
            FieldKindNumericClass::Signed64
            | FieldKindNumericClass::Unsigned64
            | FieldKindNumericClass::SignedWide
            | FieldKindNumericClass::UnsignedWide
            | FieldKindNumericClass::DurationLike
            | FieldKindNumericClass::TimestampLike,
        )) => ExprType::Numeric(NumericSubtype::Integer),
        FieldKindCategory::Scalar(FieldKindScalarClass::Numeric(
            FieldKindNumericClass::FloatLike,
        ))
        | FieldKindCategory::Relation(FieldKindScalarClass::Numeric(
            FieldKindNumericClass::FloatLike,
        )) => ExprType::Numeric(NumericSubtype::Float),
        FieldKindCategory::Scalar(FieldKindScalarClass::Numeric(
            FieldKindNumericClass::DecimalLike,
        ))
        | FieldKindCategory::Relation(FieldKindScalarClass::Numeric(
            FieldKindNumericClass::DecimalLike,
        )) => ExprType::Numeric(NumericSubtype::Decimal),
        FieldKindCategory::Scalar(FieldKindScalarClass::Text)
        | FieldKindCategory::Relation(FieldKindScalarClass::Text) => ExprType::Text,
        FieldKindCategory::Collection => ExprType::Collection,
        FieldKindCategory::Structured { .. } => ExprType::Structured,
        FieldKindCategory::Scalar(
            FieldKindScalarClass::OrderedOpaque | FieldKindScalarClass::Opaque,
        )
        | FieldKindCategory::Relation(
            FieldKindScalarClass::OrderedOpaque | FieldKindScalarClass::Opaque,
        ) => ExprType::Opaque,
    }
}

use crate::{
    db::contracts::{CompareOp, ComparePredicate, Predicate, SchemaInfo, ValidateError},
    model::field::FieldKind,
    value::Value,
};

///
/// Normalize enum literals in predicates against schema enum metadata.
///
/// Contract:
/// - strict enum literals (`path = Some`) must match the schema enum path
/// - loose enum literals (`path = None`) are resolved once at filter construction
/// - predicate semantics stay strict at runtime (`Eq` is unchanged)
///
pub(crate) fn normalize_enum_literals(
    schema: &SchemaInfo,
    predicate: &Predicate,
) -> Result<Predicate, ValidateError> {
    match predicate {
        Predicate::True => Ok(Predicate::True),
        Predicate::False => Ok(Predicate::False),
        Predicate::And(children) => {
            let mut normalized = Vec::with_capacity(children.len());
            for child in children {
                normalized.push(normalize_enum_literals(schema, child)?);
            }

            Ok(Predicate::And(normalized))
        }
        Predicate::Or(children) => {
            let mut normalized = Vec::with_capacity(children.len());
            for child in children {
                normalized.push(normalize_enum_literals(schema, child)?);
            }

            Ok(Predicate::Or(normalized))
        }
        Predicate::Not(inner) => Ok(Predicate::Not(Box::new(normalize_enum_literals(
            schema, inner,
        )?))),
        Predicate::Compare(cmp) => Ok(Predicate::Compare(normalize_compare(schema, cmp)?)),
        Predicate::IsNull { field } => Ok(Predicate::IsNull {
            field: field.clone(),
        }),
        Predicate::IsMissing { field } => Ok(Predicate::IsMissing {
            field: field.clone(),
        }),
        Predicate::IsEmpty { field } => Ok(Predicate::IsEmpty {
            field: field.clone(),
        }),
        Predicate::IsNotEmpty { field } => Ok(Predicate::IsNotEmpty {
            field: field.clone(),
        }),
        Predicate::TextContains { field, value } => Ok(Predicate::TextContains {
            field: field.clone(),
            value: value.clone(),
        }),
        Predicate::TextContainsCi { field, value } => Ok(Predicate::TextContainsCi {
            field: field.clone(),
            value: value.clone(),
        }),
    }
}

fn normalize_compare(
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
) -> Result<ComparePredicate, ValidateError> {
    let Some(field_kind) = schema.field_kind(&cmp.field) else {
        return Ok(cmp.clone());
    };

    let value = normalize_compare_value_for_kind(&cmp.field, cmp.op, &cmp.value, field_kind)?;

    Ok(ComparePredicate {
        field: cmp.field.clone(),
        op: cmp.op,
        value,
        coercion: cmp.coercion.clone(),
    })
}

fn normalize_compare_value_for_kind(
    field: &str,
    op: CompareOp,
    value: &Value,
    field_kind: &FieldKind,
) -> Result<Value, ValidateError> {
    match op {
        CompareOp::In | CompareOp::NotIn => {
            let Value::List(values) = value else {
                return Ok(value.clone());
            };

            let mut normalized = Vec::with_capacity(values.len());
            for item in values {
                normalized.push(normalize_value_for_kind(field, item, field_kind)?);
            }

            Ok(Value::List(normalized))
        }
        CompareOp::Contains => {
            let element_kind = match field_kind {
                FieldKind::List(inner) | FieldKind::Set(inner) => *inner,
                _ => return Ok(value.clone()),
            };

            normalize_value_for_kind(field, value, element_kind)
        }
        _ => normalize_value_for_kind(field, value, field_kind),
    }
}

fn normalize_value_for_kind(
    field: &str,
    value: &Value,
    expected_kind: &FieldKind,
) -> Result<Value, ValidateError> {
    match expected_kind {
        FieldKind::Enum { path } => normalize_enum_value(field, value, path),
        FieldKind::Relation { key_kind, .. } => normalize_value_for_kind(field, value, key_kind),
        FieldKind::List(inner) => {
            let Value::List(values) = value else {
                return Ok(value.clone());
            };

            let mut normalized = Vec::with_capacity(values.len());
            for item in values {
                normalized.push(normalize_value_for_kind(field, item, inner)?);
            }

            Ok(Value::List(normalized))
        }
        FieldKind::Set(inner) => {
            let Value::List(values) = value else {
                return Ok(value.clone());
            };

            let mut normalized = Vec::with_capacity(values.len());
            for item in values {
                normalized.push(normalize_value_for_kind(field, item, inner)?);
            }

            // Canonicalize set literals to match persisted set encoding:
            // deterministic order + deduplicated members.
            normalized.sort_by(Value::canonical_cmp);
            normalized.dedup();

            Ok(Value::List(normalized))
        }
        FieldKind::Map {
            key,
            value: map_value,
        } => {
            let Value::Map(entries) = value else {
                return Ok(value.clone());
            };

            let mut normalized = Vec::with_capacity(entries.len());
            for (entry_key, entry_value) in entries {
                let key = normalize_value_for_kind(field, entry_key, key)?;
                let value = normalize_value_for_kind(field, entry_value, map_value)?;
                normalized.push((key, value));
            }

            Ok(Value::Map(normalized))
        }
        FieldKind::Account
        | FieldKind::Blob
        | FieldKind::Bool
        | FieldKind::Date
        | FieldKind::Decimal { .. }
        | FieldKind::Duration
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Int
        | FieldKind::Int128
        | FieldKind::IntBig
        | FieldKind::Principal
        | FieldKind::Subaccount
        | FieldKind::Text
        | FieldKind::Timestamp
        | FieldKind::Uint
        | FieldKind::Uint128
        | FieldKind::UintBig
        | FieldKind::Ulid
        | FieldKind::Unit
        | FieldKind::Structured { .. } => Ok(value.clone()),
    }
}

fn normalize_enum_value(
    field: &str,
    value: &Value,
    expected_path: &str,
) -> Result<Value, ValidateError> {
    let Value::Enum(enum_value) = value else {
        return Ok(value.clone());
    };

    if let Some(path) = enum_value.path.as_deref() {
        if path != expected_path {
            return Err(ValidateError::invalid_literal(
                field,
                "enum path does not match field enum type",
            ));
        }

        return Ok(value.clone());
    }

    let mut normalized = enum_value.clone();
    normalized.path = Some(expected_path.to_string());
    Ok(Value::Enum(normalized))
}

///
/// TESTS
///

#[cfg(test)]
mod tests;

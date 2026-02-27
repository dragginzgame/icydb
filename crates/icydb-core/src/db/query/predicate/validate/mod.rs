#[cfg(test)]
use crate::model::entity::EntityModel;
use crate::{
    db::{
        contracts::{FieldType, SchemaInfo, ValidateError, literal_matches_type},
        query::predicate::{
            CompareOp, ComparePredicate, Predicate, UnsupportedQueryFeature,
            coercion::{CoercionId, CoercionSpec, supports_coercion},
        },
    },
    value::{CoercionFamilyExt, Value},
};

#[cfg(test)]
mod tests;

/// Reject policy-level non-queryable features before planning.
pub(crate) fn reject_unsupported_query_features(
    predicate: &Predicate,
) -> Result<(), UnsupportedQueryFeature> {
    match predicate {
        Predicate::True
        | Predicate::False
        | Predicate::Compare(_)
        | Predicate::IsNull { .. }
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. }
        | Predicate::TextContains { .. }
        | Predicate::TextContainsCi { .. } => Ok(()),
        Predicate::And(children) | Predicate::Or(children) => {
            for child in children {
                reject_unsupported_query_features(child)?;
            }

            Ok(())
        }
        Predicate::Not(inner) => reject_unsupported_query_features(inner),
    }
}

/// Validates a predicate against the provided schema information.
pub(crate) fn validate(schema: &SchemaInfo, predicate: &Predicate) -> Result<(), ValidateError> {
    reject_unsupported_query_features(predicate)?;

    match predicate {
        Predicate::True | Predicate::False => Ok(()),
        Predicate::And(children) | Predicate::Or(children) => {
            for child in children {
                validate(schema, child)?;
            }
            Ok(())
        }
        Predicate::Not(inner) => validate(schema, inner),
        Predicate::Compare(cmp) => validate_compare(schema, cmp),
        Predicate::IsNull { field } | Predicate::IsMissing { field } => {
            let _field_type = ensure_field(schema, field)?;
            Ok(())
        }
        Predicate::IsEmpty { field } => {
            let field_type = ensure_field(schema, field)?;
            if field_type.is_text() || field_type.is_collection() {
                Ok(())
            } else {
                Err(ValidateError::invalid_operator(field, "is_empty"))
            }
        }
        Predicate::IsNotEmpty { field } => {
            let field_type = ensure_field(schema, field)?;
            if field_type.is_text() || field_type.is_collection() {
                Ok(())
            } else {
                Err(ValidateError::invalid_operator(field, "is_not_empty"))
            }
        }
        Predicate::TextContains { field, value } => {
            validate_text_contains(schema, field, value, "text_contains")
        }
        Predicate::TextContainsCi { field, value } => {
            validate_text_contains(schema, field, value, "text_contains_ci")
        }
    }
}

/// Builds schema information from a model and validates a predicate against it.
#[cfg(test)]
pub(crate) fn validate_model(
    model: &EntityModel,
    predicate: &Predicate,
) -> Result<(), ValidateError> {
    let schema = SchemaInfo::from_entity_model(model)?;
    validate(&schema, predicate)
}

fn validate_compare(schema: &SchemaInfo, cmp: &ComparePredicate) -> Result<(), ValidateError> {
    let field_type = ensure_field(schema, &cmp.field)?;

    match cmp.op {
        CompareOp::Eq | CompareOp::Ne => {
            validate_eq_ne(&cmp.field, field_type, &cmp.value, &cmp.coercion)
        }
        CompareOp::Lt | CompareOp::Lte | CompareOp::Gt | CompareOp::Gte => {
            validate_ordering(&cmp.field, field_type, &cmp.value, &cmp.coercion, cmp.op)
        }
        CompareOp::In | CompareOp::NotIn => {
            validate_in(&cmp.field, field_type, &cmp.value, &cmp.coercion, cmp.op)
        }
        CompareOp::Contains => validate_contains(&cmp.field, field_type, &cmp.value, &cmp.coercion),
        CompareOp::StartsWith | CompareOp::EndsWith => {
            validate_text_compare(&cmp.field, field_type, &cmp.value, &cmp.coercion, cmp.op)
        }
    }
}

fn validate_eq_ne(
    field: &str,
    field_type: &FieldType,
    value: &Value,
    coercion: &CoercionSpec,
) -> Result<(), ValidateError> {
    if field_type.is_list_like() {
        ensure_list_literal(field, value, field_type)?;
    } else {
        ensure_scalar_literal(field, value)?;
    }

    ensure_coercion(field, field_type, value, coercion)
}

fn validate_ordering(
    field: &str,
    field_type: &FieldType,
    value: &Value,
    coercion: &CoercionSpec,
    op: CompareOp,
) -> Result<(), ValidateError> {
    if matches!(coercion.id, CoercionId::CollectionElement) {
        return Err(ValidateError::InvalidCoercion {
            field: field.to_string(),
            coercion: coercion.id,
        });
    }

    if !field_type.is_orderable() {
        return Err(ValidateError::invalid_operator(field, format!("{op:?}")));
    }

    ensure_scalar_literal(field, value)?;

    ensure_coercion(field, field_type, value, coercion)
}

/// Validate list membership predicates.
fn validate_in(
    field: &str,
    field_type: &FieldType,
    value: &Value,
    coercion: &CoercionSpec,
    op: CompareOp,
) -> Result<(), ValidateError> {
    if field_type.is_collection() {
        return Err(ValidateError::invalid_operator(field, format!("{op:?}")));
    }

    let Value::List(items) = value else {
        return Err(ValidateError::invalid_literal(
            field,
            "expected list literal",
        ));
    };

    for item in items {
        ensure_coercion(field, field_type, item, coercion)?;
    }

    Ok(())
}

/// Validate collection containment predicates on list/set fields.
fn validate_contains(
    field: &str,
    field_type: &FieldType,
    value: &Value,
    coercion: &CoercionSpec,
) -> Result<(), ValidateError> {
    if field_type.is_text() {
        // CONTRACT: text substring matching uses TextContains/TextContainsCi only.
        return Err(ValidateError::invalid_operator(
            field,
            format!("{:?}", CompareOp::Contains),
        ));
    }

    let element_type = match field_type {
        FieldType::List(inner) | FieldType::Set(inner) => inner.as_ref(),
        _ => {
            return Err(ValidateError::invalid_operator(
                field,
                format!("{:?}", CompareOp::Contains),
            ));
        }
    };

    if matches!(coercion.id, CoercionId::TextCasefold) {
        // CONTRACT: case-insensitive coercion never applies to structured values.
        return Err(ValidateError::InvalidCoercion {
            field: field.to_string(),
            coercion: coercion.id,
        });
    }

    ensure_coercion(field, element_type, value, coercion)
}

/// Validate text prefix/suffix comparisons.
fn validate_text_compare(
    field: &str,
    field_type: &FieldType,
    value: &Value,
    coercion: &CoercionSpec,
    op: CompareOp,
) -> Result<(), ValidateError> {
    if !field_type.is_text() {
        return Err(ValidateError::invalid_operator(field, format!("{op:?}")));
    }

    ensure_text_literal(field, value)?;

    ensure_coercion(field, field_type, value, coercion)
}

/// Validate substring predicates on text fields.
fn validate_text_contains(
    schema: &SchemaInfo,
    field: &str,
    value: &Value,
    op: &str,
) -> Result<(), ValidateError> {
    let field_type = ensure_field(schema, field)?;
    if !field_type.is_text() {
        return Err(ValidateError::invalid_operator(field, op));
    }

    ensure_text_literal(field, value)?;

    Ok(())
}

fn ensure_field<'a>(schema: &'a SchemaInfo, field: &str) -> Result<&'a FieldType, ValidateError> {
    let field_type = schema
        .field(field)
        .ok_or_else(|| ValidateError::UnknownField {
            field: field.to_string(),
        })?;

    if matches!(field_type, FieldType::Map { .. }) {
        return Err(UnsupportedQueryFeature::MapPredicate {
            field: field.to_string(),
        }
        .into());
    }

    if !field_type.value_kind().is_queryable() {
        return Err(ValidateError::NonQueryableFieldType {
            field: field.to_string(),
        });
    }

    Ok(field_type)
}

// Ensure the literal is text to match text-only operators.
fn ensure_text_literal(field: &str, value: &Value) -> Result<(), ValidateError> {
    if !matches!(value, Value::Text(_)) {
        return Err(ValidateError::invalid_literal(
            field,
            "expected text literal",
        ));
    }

    Ok(())
}

// Reject list literals when scalar comparisons are required.
fn ensure_scalar_literal(field: &str, value: &Value) -> Result<(), ValidateError> {
    if matches!(value, Value::List(_)) {
        return Err(ValidateError::invalid_literal(
            field,
            "expected scalar literal",
        ));
    }

    Ok(())
}

pub(super) fn ensure_coercion(
    field: &str,
    field_type: &FieldType,
    literal: &Value,
    coercion: &CoercionSpec,
) -> Result<(), ValidateError> {
    if matches!(coercion.id, CoercionId::TextCasefold) && !field_type.is_text() {
        // CONTRACT: case-insensitive coercions are text-only.
        return Err(ValidateError::InvalidCoercion {
            field: field.to_string(),
            coercion: coercion.id,
        });
    }

    // NOTE:
    // NumericWiden eligibility is registry-authoritative.
    // CoercionFamily::Numeric is intentionally NOT sufficient.
    // This prevents validation/runtime divergence for Date, IntBig, UintBig.
    if matches!(coercion.id, CoercionId::NumericWiden)
        && (!field_type.supports_numeric_coercion() || !literal.supports_numeric_coercion())
    {
        return Err(ValidateError::InvalidCoercion {
            field: field.to_string(),
            coercion: coercion.id,
        });
    }

    if !matches!(coercion.id, CoercionId::NumericWiden) {
        let left_family =
            field_type
                .coercion_family()
                .ok_or_else(|| ValidateError::NonQueryableFieldType {
                    field: field.to_string(),
                })?;
        let right_family = literal.coercion_family();

        if !supports_coercion(left_family, right_family, coercion.id) {
            return Err(ValidateError::InvalidCoercion {
                field: field.to_string(),
                coercion: coercion.id,
            });
        }
    }

    if matches!(
        coercion.id,
        CoercionId::Strict | CoercionId::CollectionElement
    ) && !literal_matches_type(literal, field_type)
    {
        return Err(ValidateError::invalid_literal(
            field,
            "literal type does not match field type",
        ));
    }

    Ok(())
}

fn ensure_list_literal(
    field: &str,
    literal: &Value,
    field_type: &FieldType,
) -> Result<(), ValidateError> {
    if !literal_matches_type(literal, field_type) {
        return Err(ValidateError::invalid_literal(
            field,
            "list literal does not match field element type",
        ));
    }

    Ok(())
}

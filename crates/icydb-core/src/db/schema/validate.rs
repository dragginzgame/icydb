//! Module: db::schema::validate
//! Responsibility: schema-aware predicate validation and unsupported-feature rejection.
//! Does not own: planner routing decisions or executor runtime filtering behavior.
//! Boundary: validates predicate/type semantics before planning and execution.

use crate::{
    db::schema::types::ScalarType,
    db::{
        predicate::{
            CoercionId, CoercionSpec, CompareFieldsPredicate, CompareOp, ComparePredicate,
            Predicate, UnsupportedQueryFeature, supports_coercion,
        },
        schema::{FieldType, SchemaInfo, ValidateError, literal_matches_type},
    },
    value::{CoercionFamilyExt, Value},
};

/// Reject policy-level non-queryable features before planning.
pub(crate) fn reject_unsupported_query_features(
    predicate: &Predicate,
) -> Result<(), UnsupportedQueryFeature> {
    match predicate {
        Predicate::True
        | Predicate::False
        | Predicate::Compare(_)
        | Predicate::CompareFields(_)
        | Predicate::IsNull { .. }
        | Predicate::IsNotNull { .. }
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
        Predicate::CompareFields(cmp) => validate_compare_fields(schema, cmp),
        Predicate::IsNull { field }
        | Predicate::IsNotNull { field }
        | Predicate::IsMissing { field } => {
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

fn validate_compare(schema: &SchemaInfo, cmp: &ComparePredicate) -> Result<(), ValidateError> {
    let field_type = ensure_field(schema, &cmp.field)?;

    if cmp.op.is_equality_family() {
        validate_eq_ne(&cmp.field, field_type, &cmp.value, &cmp.coercion)
    } else if cmp.op.is_ordering_family() {
        validate_ordering(&cmp.field, field_type, &cmp.value, &cmp.coercion, cmp.op)
    } else if cmp.op.is_membership_family() {
        validate_in(&cmp.field, field_type, &cmp.value, &cmp.coercion, cmp.op)
    } else if cmp.op.is_contains_family() {
        validate_contains(&cmp.field, field_type, &cmp.value, &cmp.coercion)
    } else {
        validate_text_compare(&cmp.field, field_type, &cmp.value, &cmp.coercion, cmp.op)
    }
}

fn validate_compare_fields(
    schema: &SchemaInfo,
    cmp: &CompareFieldsPredicate,
) -> Result<(), ValidateError> {
    let left_type = ensure_field(schema, &cmp.left_field)?;
    let right_type = ensure_field(schema, &cmp.right_field)?;

    if !cmp.op.supports_field_compare() {
        Err(ValidateError::invalid_operator(
            &cmp.left_field,
            format!("{:?}", cmp.op),
        ))
    } else if cmp.op.is_equality_family() {
        validate_compare_fields_eq_ne(
            &cmp.left_field,
            left_type,
            &cmp.right_field,
            right_type,
            &cmp.coercion,
        )
    } else {
        validate_compare_fields_ordering(
            &cmp.left_field,
            left_type,
            &cmp.right_field,
            right_type,
            &cmp.coercion,
            cmp.op,
        )
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

fn validate_compare_fields_eq_ne(
    left_field: &str,
    left_type: &FieldType,
    right_field: &str,
    right_type: &FieldType,
    coercion: &CoercionSpec,
) -> Result<(), ValidateError> {
    if !field_types_support_field_compare_eq_ne(left_type, right_type) {
        return Err(ValidateError::invalid_literal(
            left_field,
            format!("cannot compare field '{left_field}' with field '{right_field}'").as_str(),
        ));
    }

    if !compare_fields_coercion_supported(left_type, right_type, coercion) {
        return Err(ValidateError::InvalidCoercion {
            field: left_field.to_string(),
            coercion: coercion.id,
        });
    }

    Ok(())
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

fn validate_compare_fields_ordering(
    left_field: &str,
    left_type: &FieldType,
    right_field: &str,
    right_type: &FieldType,
    coercion: &CoercionSpec,
    op: CompareOp,
) -> Result<(), ValidateError> {
    if !field_types_support_field_compare_ordering(left_type, right_type) {
        return Err(ValidateError::invalid_operator(
            left_field,
            format!("{op:?} against field '{right_field}'"),
        ));
    }

    if !compare_fields_coercion_supported(left_type, right_type, coercion) {
        return Err(ValidateError::InvalidCoercion {
            field: left_field.to_string(),
            coercion: coercion.id,
        });
    }

    Ok(())
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

const fn field_types_support_field_compare_eq_ne(left: &FieldType, right: &FieldType) -> bool {
    field_types_are_both_numeric(left, right)
        || field_types_are_both_text(left, right)
        || field_types_are_both_bool(left, right)
}

const fn field_types_support_field_compare_ordering(left: &FieldType, right: &FieldType) -> bool {
    field_types_are_both_numeric(left, right) || field_types_are_both_text(left, right)
}

const fn field_types_are_both_text(left: &FieldType, right: &FieldType) -> bool {
    left.is_text() && right.is_text()
}

const fn field_types_are_both_numeric(left: &FieldType, right: &FieldType) -> bool {
    left.supports_numeric_coercion() && right.supports_numeric_coercion()
}

const fn field_types_are_both_bool(left: &FieldType, right: &FieldType) -> bool {
    matches!(left, FieldType::Scalar(ScalarType::Bool))
        && matches!(right, FieldType::Scalar(ScalarType::Bool))
}

const fn compare_fields_coercion_supported(
    left_type: &FieldType,
    right_type: &FieldType,
    coercion: &CoercionSpec,
) -> bool {
    match coercion.id {
        CoercionId::Strict => {
            field_types_are_both_text(left_type, right_type)
                || field_types_are_both_bool(left_type, right_type)
        }
        CoercionId::NumericWiden => field_types_are_both_numeric(left_type, right_type),
        CoercionId::CollectionElement | CoercionId::TextCasefold => false,
    }
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

fn ensure_coercion(
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

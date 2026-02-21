use crate::{
    db::query::predicate::{CompareOp, ComparePredicate, Predicate, SchemaInfo, ValidateError},
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
        FieldKind::List(inner) | FieldKind::Set(inner) => {
            let Value::List(values) = value else {
                return Ok(value.clone());
            };

            let mut normalized = Vec::with_capacity(values.len());
            for item in values {
                normalized.push(normalize_value_for_kind(field, item, inner)?);
            }

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
mod tests {
    use super::normalize_enum_literals;
    use crate::{
        db::query::predicate::{
            CoercionId, CompareOp, ComparePredicate, Predicate,
            validate::{SchemaInfo, ValidateError},
        },
        model::field::{FieldKind, FieldModel},
        test_support::entity_model_from_static,
        types::Ulid,
        value::{Value, ValueEnum},
    };

    static ENUM_FIELDS: [FieldModel; 2] = [
        FieldModel {
            name: "id",
            kind: FieldKind::Ulid,
        },
        FieldModel {
            name: "stage",
            kind: FieldKind::Enum {
                path: "tests::Stage",
            },
        },
    ];
    static ENUM_INDEXES: [&crate::model::index::IndexModel; 0] = [];
    static ENUM_MODEL: crate::model::entity::EntityModel = entity_model_from_static(
        "tests::EnumEntity",
        "EnumEntity",
        &ENUM_FIELDS[0],
        &ENUM_FIELDS,
        &ENUM_INDEXES,
    );
    static MULTI_ENUM_FIELDS: [FieldModel; 3] = [
        FieldModel {
            name: "id",
            kind: FieldKind::Ulid,
        },
        FieldModel {
            name: "stage",
            kind: FieldKind::Enum {
                path: "tests::Stage",
            },
        },
        FieldModel {
            name: "status",
            kind: FieldKind::Enum {
                path: "tests::Status",
            },
        },
    ];
    static MULTI_ENUM_INDEXES: [&crate::model::index::IndexModel; 0] = [];
    static MULTI_ENUM_MODEL: crate::model::entity::EntityModel = entity_model_from_static(
        "tests::MultiEnumEntity",
        "MultiEnumEntity",
        &MULTI_ENUM_FIELDS[0],
        &MULTI_ENUM_FIELDS,
        &MULTI_ENUM_INDEXES,
    );

    fn schema() -> SchemaInfo {
        SchemaInfo::from_entity_model(&ENUM_MODEL).expect("enum test schema should be valid")
    }

    fn multi_enum_schema() -> SchemaInfo {
        SchemaInfo::from_entity_model(&MULTI_ENUM_MODEL)
            .expect("multi-enum test schema should be valid")
    }

    fn eq(value: Value) -> Predicate {
        Predicate::Compare(ComparePredicate::with_coercion(
            "stage",
            CompareOp::Eq,
            value,
            CoercionId::Strict,
        ))
    }

    #[test]
    fn strict_filter_matches_strict_enum() {
        let predicate = eq(Value::Enum(ValueEnum::new("Active", Some("tests::Stage"))));
        let normalized = normalize_enum_literals(&schema(), &predicate).expect("strict enum");
        assert_eq!(normalized, predicate);
    }

    #[test]
    fn loose_filter_resolves_enum_path() {
        let predicate = eq(Value::Enum(ValueEnum::loose("Active")));
        let normalized = normalize_enum_literals(&schema(), &predicate).expect("loose enum");
        assert_eq!(
            normalized,
            eq(Value::Enum(ValueEnum::new("Active", Some("tests::Stage"))))
        );
    }

    #[test]
    fn strict_filter_with_wrong_path_fails() {
        let predicate = eq(Value::Enum(ValueEnum::new("Active", Some("wrong::Path"))));
        let err = normalize_enum_literals(&schema(), &predicate).expect_err("wrong enum path");
        assert!(matches!(err, ValidateError::InvalidLiteral { field, .. } if field == "stage"));
    }

    #[test]
    fn stage_in_filter_resolves_loose_values() {
        let predicate = Predicate::Compare(ComparePredicate::with_coercion(
            "stage",
            CompareOp::In,
            Value::List(vec![
                Value::Enum(ValueEnum::loose("Draft")),
                Value::Enum(ValueEnum::new("Active", Some("tests::Stage"))),
            ]),
            CoercionId::Strict,
        ));

        let normalized = normalize_enum_literals(&schema(), &predicate).expect("enum list");
        let expected = Predicate::Compare(ComparePredicate::with_coercion(
            "stage",
            CompareOp::In,
            Value::List(vec![
                Value::Enum(ValueEnum::new("Draft", Some("tests::Stage"))),
                Value::Enum(ValueEnum::new("Active", Some("tests::Stage"))),
            ]),
            CoercionId::Strict,
        ));

        assert_eq!(normalized, expected);
    }

    #[test]
    fn unknown_fields_are_left_for_schema_validation() {
        let predicate = Predicate::Compare(ComparePredicate::with_coercion(
            "unknown",
            CompareOp::Eq,
            Value::Ulid(Ulid::nil()),
            CoercionId::Strict,
        ));
        let normalized = normalize_enum_literals(&schema(), &predicate).expect("unknown field");
        assert_eq!(normalized, predicate);
    }

    #[test]
    fn normalization_is_idempotent() {
        let predicate = eq(Value::Enum(ValueEnum::loose("Active")));

        let once = normalize_enum_literals(&schema(), &predicate).expect("first normalize");
        let twice = normalize_enum_literals(&schema(), &once).expect("second normalize");

        assert_eq!(once, twice);
    }

    #[test]
    fn loose_resolution_is_field_scoped_for_shared_variant_names() {
        let predicate = Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "stage",
                CompareOp::Eq,
                Value::Enum(ValueEnum::loose("Active")),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "status",
                CompareOp::Eq,
                Value::Enum(ValueEnum::loose("Active")),
                CoercionId::Strict,
            )),
        ]);

        let normalized = normalize_enum_literals(&multi_enum_schema(), &predicate)
            .expect("field-scoped normalization");
        let Predicate::And(children) = normalized else {
            panic!("expected AND predicate");
        };
        assert_eq!(children.len(), 2);

        let Predicate::Compare(stage_cmp) = &children[0] else {
            panic!("expected first compare predicate");
        };
        let Value::Enum(stage) = &stage_cmp.value else {
            panic!("expected first enum value");
        };
        assert_eq!(stage.path.as_deref(), Some("tests::Stage"));

        let Predicate::Compare(status_cmp) = &children[1] else {
            panic!("expected second compare predicate");
        };
        let Value::Enum(status) = &status_cmp.value else {
            panic!("expected second enum value");
        };
        assert_eq!(status.path.as_deref(), Some("tests::Status"));
    }
}

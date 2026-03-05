//! Module: predicate::normalize
//! Responsibility: deterministic predicate normalization and enum-literal adjustment.
//! Does not own: runtime evaluation or schema field-slot resolution.
//! Boundary: normalize before validation/planning/fingerprinting.

use crate::{
    db::predicate::{
        CompareOp, ComparePredicate, Predicate, SchemaInfo, ValidateError,
        encoding::encode_predicate_sort_key,
    },
    model::field::FieldKind,
    value::Value,
};

/// Normalize a predicate into a canonical, deterministic form.
///
/// Normalization guarantees:
/// - Logical equivalence is preserved
/// - Nested AND / OR nodes are flattened
/// - Neutral elements are removed (True / False)
/// - Double negation is eliminated
/// - Child predicates are deterministically ordered
///
/// Note: this pass does not normalize literal values (numeric width, collation).
/// Ordering uses the structural `Value` representation.
///
/// This is used to ensure:
/// - stable planner output
/// - consistent caching / equality checks
/// - predictable test behavior
#[must_use]
pub(in crate::db) fn normalize(predicate: &Predicate) -> Predicate {
    // Normalize recursively while preserving logical equivalence.
    match predicate {
        Predicate::True => Predicate::True,
        Predicate::False => Predicate::False,

        Predicate::And(children) => normalize_and(children),
        Predicate::Or(children) => normalize_or(children),
        Predicate::Not(inner) => normalize_not(inner),

        Predicate::Compare(cmp) => Predicate::Compare(normalize_compare(cmp)),

        Predicate::IsNull { field } => Predicate::IsNull {
            field: field.clone(),
        },
        Predicate::IsMissing { field } => Predicate::IsMissing {
            field: field.clone(),
        },
        Predicate::IsEmpty { field } => Predicate::IsEmpty {
            field: field.clone(),
        },
        Predicate::IsNotEmpty { field } => Predicate::IsNotEmpty {
            field: field.clone(),
        },
        Predicate::TextContains { field, value } => Predicate::TextContains {
            field: field.clone(),
            value: value.clone(),
        },
        Predicate::TextContainsCi { field, value } => Predicate::TextContainsCi {
            field: field.clone(),
            value: value.clone(),
        },
    }
}

///
/// Normalize enum literals in predicates against schema enum metadata.
///
/// Contract:
/// - strict enum literals (`path = Some`) must match the schema enum path
/// - loose enum literals (`path = None`) are resolved once at filter construction
/// - predicate semantics stay strict at runtime (`Eq` is unchanged)
///
pub(in crate::db) fn normalize_enum_literals(
    schema: &SchemaInfo,
    predicate: &Predicate,
) -> Result<Predicate, ValidateError> {
    // Enum literal normalization only rewrites enum payload shape, not operators.
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
        Predicate::Compare(cmp) => Ok(Predicate::Compare(normalize_compare_with_schema(
            schema, cmp,
        )?)),
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

/// Normalize a comparison predicate by cloning its components.
///
/// This function exists primarily for symmetry and future-proofing
/// (e.g. if comparison-level rewrites are introduced later).
fn normalize_compare(cmp: &ComparePredicate) -> ComparePredicate {
    ComparePredicate {
        field: cmp.field.clone(),
        op: cmp.op,
        value: cmp.value.clone(),
        coercion: cmp.coercion.clone(),
    }
}

fn normalize_compare_with_schema(
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
/// Normalize a NOT expression.
///
/// Eliminates double negation:
///     NOT (NOT x)  →  x
///
fn normalize_not(inner: &Predicate) -> Predicate {
    let normalized = normalize(inner);

    if let Predicate::Not(double) = normalized {
        return normalize(&double);
    }

    Predicate::Not(Box::new(normalized))
}

///
/// Normalize an AND expression.
///
/// Rules:
/// - AND(True, x)        → x
/// - AND(False, x)       → False
/// - AND(AND(a, b), c)   → AND(a, b, c)
/// - AND()               → True
///
/// Children are sorted deterministically.
///
fn normalize_and(children: &[Predicate]) -> Predicate {
    let mut out = Vec::new();

    for child in children {
        let normalized = normalize(child);

        match normalized {
            Predicate::True => {}
            Predicate::False => return Predicate::False,
            Predicate::And(grandchildren) => out.extend(grandchildren),
            other => out.push(other),
        }
    }

    if out.is_empty() {
        return Predicate::True;
    }

    // Evaluate cheaper predicates first to reduce average short-circuit work
    // while keeping deterministic ordering under the canonical sort key.
    out.sort_by_cached_key(|predicate| (predicate_eval_cost_rank(predicate), sort_key(predicate)));
    out.dedup();

    if out.len() == 1 {
        return out.remove(0);
    }

    Predicate::And(out)
}

///
/// Normalize an OR expression.
///
/// Rules:
/// - OR(False, x)       → x
/// - OR(True, x)        → True
/// - OR(OR(a, b), c)    → OR(a, b, c)
/// - OR()               → False
///
/// Children are sorted deterministically.
///
fn normalize_or(children: &[Predicate]) -> Predicate {
    let mut out = Vec::new();

    for child in children {
        let normalized = normalize(child);

        match normalized {
            Predicate::False => {}
            Predicate::True => return Predicate::True,
            Predicate::Or(grandchildren) => out.extend(grandchildren),
            other => out.push(other),
        }
    }

    if out.is_empty() {
        return Predicate::False;
    }

    // Evaluate cheaper predicates first to reduce average short-circuit work
    // while keeping deterministic ordering under the canonical sort key.
    out.sort_by_cached_key(|predicate| (predicate_eval_cost_rank(predicate), sort_key(predicate)));
    out.dedup();

    if out.len() == 1 {
        return out.remove(0);
    }

    Predicate::Or(out)
}

// Return a stable heuristic rank for predicate evaluation cost. Lower ranks
// are evaluated first after normalization.
const fn predicate_eval_cost_rank(predicate: &Predicate) -> u8 {
    match predicate {
        Predicate::True | Predicate::False => 0,
        Predicate::Compare(_)
        | Predicate::IsNull { .. }
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. } => 1,
        Predicate::Not(_) => 2,
        Predicate::TextContains { .. } | Predicate::TextContainsCi { .. } => 3,
        Predicate::And(_) | Predicate::Or(_) => 4,
    }
}

///
/// Generate a deterministic, length-prefixed key for a predicate.
///
/// This key is used **only for sorting**, not for display.
/// Ordering ensures:
/// - planner determinism
/// - stable normalization
/// - predictable equality
///
fn sort_key(predicate: &Predicate) -> Vec<u8> {
    encode_predicate_sort_key(predicate)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::predicate::{ComparePredicate, Predicate, normalize},
        value::Value,
    };

    #[test]
    fn normalize_and_dedups_identical_children_and_collapses_to_singleton() {
        let duplicated = Predicate::And(vec![
            Predicate::eq("rank".to_string(), Value::Uint(7)),
            Predicate::eq("rank".to_string(), Value::Uint(7)),
        ]);

        let normalized = normalize(&duplicated);

        assert_eq!(
            normalized,
            Predicate::Compare(ComparePredicate::eq("rank".to_string(), Value::Uint(7))),
            "identical AND children should collapse to one predicate",
        );
    }

    #[test]
    fn normalize_or_dedups_identical_children_and_collapses_to_singleton() {
        let duplicated = Predicate::Or(vec![
            Predicate::eq("rank".to_string(), Value::Uint(7)),
            Predicate::eq("rank".to_string(), Value::Uint(7)),
        ]);

        let normalized = normalize(&duplicated);

        assert_eq!(
            normalized,
            Predicate::Compare(ComparePredicate::eq("rank".to_string(), Value::Uint(7))),
            "identical OR children should collapse to one predicate",
        );
    }

    #[test]
    fn normalize_and_orders_cheaper_predicates_before_text_contains() {
        let mixed = Predicate::And(vec![
            Predicate::TextContains {
                field: "name".to_string(),
                value: Value::Text("ada".to_string()),
            },
            Predicate::eq("rank".to_string(), Value::Uint(7)),
        ]);

        let normalized = normalize(&mixed);
        let Predicate::And(children) = normalized else {
            panic!("normalized mixed predicate should remain AND with two children");
        };
        assert_eq!(
            children.len(),
            2,
            "mixed AND should keep exactly two children"
        );
        assert!(
            matches!(children[0], Predicate::Compare(_)),
            "cheap compare predicate should be evaluated before text-contains predicate",
        );
        assert!(
            matches!(children[1], Predicate::TextContains { .. }),
            "text-contains predicate should be placed after cheap compare predicate",
        );
    }
}

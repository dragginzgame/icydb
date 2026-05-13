//! Module: predicate::normalize
//! Responsibility: deterministic predicate normalization and enum-literal adjustment.
//! Does not own: runtime evaluation or schema field-slot resolution.
//! Boundary: normalize before validation/planning/fingerprinting.

use crate::{
    db::{
        predicate::{
            CoercionId, CoercionSpec, CompareOp, ComparePredicate, MembershipCompareLeaf,
            Predicate, collapse_membership_compare_leaves, encoding::encode_predicate_sort_key,
            simplify::simplify_and_compare_constraints,
        },
        schema::{SchemaInfo, ValidateError},
    },
    model::{classify_field_kind, field::FieldKind},
    types::{Int, Int128, Nat, Nat128},
    value::{Value, canonicalize_value_set},
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

        Predicate::Compare(cmp) => Predicate::Compare(cmp.clone()),
        Predicate::CompareFields(cmp) => Predicate::CompareFields(cmp.clone()),

        Predicate::IsNull { field } => Predicate::IsNull {
            field: field.clone(),
        },
        Predicate::IsNotNull { field } => Predicate::IsNotNull {
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
        Predicate::CompareFields(cmp) => Ok(Predicate::CompareFields(
            normalize_compare_fields_with_schema(schema, cmp),
        )),
        Predicate::IsNull { field } => Ok(Predicate::IsNull {
            field: field.clone(),
        }),
        Predicate::IsNotNull { field } => Ok(Predicate::IsNotNull {
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

fn normalize_compare_with_schema(
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
) -> Result<ComparePredicate, ValidateError> {
    let Some(field_kind) = schema.field_kind(&cmp.field) else {
        return Ok(cmp.clone());
    };

    let value = normalize_compare_value_for_kind(
        &cmp.field,
        cmp.op,
        &cmp.value,
        field_kind,
        cmp.coercion(),
    )?;
    Ok(ComparePredicate {
        field: cmp.field.clone(),
        op: cmp.op,
        value,
        coercion: cmp.coercion.clone(),
    })
}

fn normalize_compare_fields_with_schema(
    schema: &SchemaInfo,
    cmp: &crate::db::predicate::CompareFieldsPredicate,
) -> crate::db::predicate::CompareFieldsPredicate {
    let Some(left_kind) = schema.field_kind(&cmp.left_field) else {
        return cmp.clone();
    };
    let Some(right_kind) = schema.field_kind(&cmp.right_field) else {
        return cmp.clone();
    };

    let left_field = cmp.left_field.clone();
    let right_field = cmp.right_field.clone();
    let coercion =
        normalize_compare_fields_coercion(cmp.op, left_kind, right_kind, cmp.coercion.id);

    crate::db::predicate::CompareFieldsPredicate::with_coercion(
        left_field,
        cmp.op,
        right_field,
        coercion,
    )
}

const fn normalize_compare_fields_coercion(
    op: CompareOp,
    left_kind: &FieldKind,
    right_kind: &FieldKind,
    current: CoercionId,
) -> CoercionId {
    if op.is_equality_family() {
        if field_kinds_support_numeric_widen(left_kind, right_kind) {
            CoercionId::NumericWiden
        } else {
            current
        }
    } else if op.is_ordering_family() {
        if matches!(left_kind, FieldKind::Text { .. })
            && matches!(right_kind, FieldKind::Text { .. })
        {
            CoercionId::Strict
        } else {
            current
        }
    } else {
        current
    }
}

const fn field_kinds_support_numeric_widen(left_kind: &FieldKind, right_kind: &FieldKind) -> bool {
    classify_field_kind(left_kind).supports_predicate_numeric_widen()
        && classify_field_kind(right_kind).supports_predicate_numeric_widen()
}

fn normalize_compare_value_for_kind(
    field: &str,
    op: CompareOp,
    value: &Value,
    field_kind: &FieldKind,
    coercion: &CoercionSpec,
) -> Result<Value, ValidateError> {
    match op {
        CompareOp::In | CompareOp::NotIn => {
            let Value::List(values) = value else {
                return Ok(value.clone());
            };

            let Value::List(mut normalized) =
                normalize_list_value_for_kind(field, values.as_slice(), field_kind, coercion, op)?
            else {
                unreachable!("normalized compare-list kind should always return list value");
            };

            // Membership predicates are set-shaped: duplicates and input order
            // must not survive normalization because planner/cache identity and
            // runtime semantics both treat these lists as canonical value sets.
            canonicalize_value_set(&mut normalized);

            Ok(Value::List(normalized))
        }
        CompareOp::Contains => {
            let element_kind = match field_kind {
                FieldKind::List(inner) | FieldKind::Set(inner) => *inner,
                _ => return Ok(value.clone()),
            };

            normalize_value_for_kind(field, value, element_kind, coercion, op)
        }
        _ => normalize_value_for_kind(field, value, field_kind, coercion, op),
    }
}

fn normalize_value_for_kind(
    field: &str,
    value: &Value,
    expected_kind: &FieldKind,
    coercion: &CoercionSpec,
    op: CompareOp,
) -> Result<Value, ValidateError> {
    match expected_kind {
        FieldKind::Enum { path, .. } => normalize_enum_value(field, value, path),
        FieldKind::Relation { key_kind, .. } => {
            normalize_value_for_kind(field, value, key_kind, coercion, op)
        }
        FieldKind::List(inner) => {
            let Value::List(values) = value else {
                return Ok(value.clone());
            };

            normalize_list_value_for_kind(field, values.as_slice(), inner, coercion, op)
        }
        FieldKind::Set(inner) => {
            let Value::List(values) = value else {
                return Ok(value.clone());
            };

            let Value::List(mut normalized) =
                normalize_list_value_for_kind(field, values.as_slice(), inner, coercion, op)?
            else {
                unreachable!("normalized list kind should always return list value");
            };

            // Canonical set literal normalization must match the same
            // deterministic sort + dedup rule used by access planning.
            canonicalize_value_set(&mut normalized);

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
                let key = normalize_value_for_kind(field, entry_key, key, coercion, op)?;
                let value = normalize_value_for_kind(field, entry_value, map_value, coercion, op)?;
                normalized.push((key, value));
            }

            Ok(Value::Map(normalized))
        }
        FieldKind::Account
        | FieldKind::Blob { .. }
        | FieldKind::Bool
        | FieldKind::Date
        | FieldKind::Decimal { .. }
        | FieldKind::Duration
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Principal
        | FieldKind::Subaccount
        | FieldKind::Text { .. }
        | FieldKind::Timestamp
        | FieldKind::Ulid
        | FieldKind::Unit
        | FieldKind::Structured { .. } => Ok(value.clone()),
        FieldKind::Int
        | FieldKind::Int128
        | FieldKind::IntBig
        | FieldKind::Nat
        | FieldKind::Nat128
        | FieldKind::NatBig => Ok(normalize_numeric_value_for_kind(
            value,
            expected_kind,
            coercion,
            op,
        )),
    }
}

// Canonicalize equality-like numeric literals onto the runtime field kind so
// planner identity does not depend on parser-chosen integer wrappers. Ordered
// NumericWiden comparisons keep their original transport shape because their
// literal wrapper is still part of the current planner contract.
fn normalize_numeric_value_for_kind(
    value: &Value,
    expected_kind: &FieldKind,
    coercion: &CoercionSpec,
    op: CompareOp,
) -> Value {
    if matches!(coercion.id, CoercionId::NumericWiden)
        && matches!(
            op,
            CompareOp::Lt | CompareOp::Lte | CompareOp::Gt | CompareOp::Gte
        )
    {
        return value.clone();
    }

    if !value.supports_numeric_coercion() {
        return value.clone();
    }

    let normalized = match expected_kind {
        FieldKind::Int => value
            .to_numeric_decimal()
            .and_then(<i64 as crate::traits::NumericValue>::try_from_decimal)
            .map(Value::Int),
        FieldKind::Int128 => value
            .to_numeric_decimal()
            .and_then(<Int128 as crate::traits::NumericValue>::try_from_decimal)
            .map(Value::Int128),
        FieldKind::IntBig => value
            .to_numeric_decimal()
            .and_then(<Int as crate::traits::NumericValue>::try_from_decimal)
            .map(Value::IntBig),
        FieldKind::Nat => value
            .to_numeric_decimal()
            .and_then(<u64 as crate::traits::NumericValue>::try_from_decimal)
            .map(Value::Nat),
        FieldKind::Nat128 => value
            .to_numeric_decimal()
            .and_then(<Nat128 as crate::traits::NumericValue>::try_from_decimal)
            .map(Value::Nat128),
        FieldKind::NatBig => value
            .to_numeric_decimal()
            .and_then(<Nat as crate::traits::NumericValue>::try_from_decimal)
            .map(Value::NatBig),
        _ => None,
    };

    normalized.unwrap_or_else(|| value.clone())
}

// Normalize one list-shaped literal by recursively rewriting each item against
// the expected element kind while preserving list cardinality and order.
fn normalize_list_value_for_kind(
    field: &str,
    values: &[Value],
    expected_kind: &FieldKind,
    coercion: &CoercionSpec,
    op: CompareOp,
) -> Result<Value, ValidateError> {
    let mut normalized = Vec::with_capacity(values.len());
    for item in values {
        normalized.push(normalize_value_for_kind(
            field,
            item,
            expected_kind,
            coercion,
            op,
        )?);
    }

    Ok(Value::List(normalized))
}

fn normalize_enum_value(
    field: &str,
    value: &Value,
    expected_path: &str,
) -> Result<Value, ValidateError> {
    let Value::Enum(enum_value) = value else {
        return Ok(value.clone());
    };

    if let Some(path) = enum_value.path() {
        if path != expected_path {
            return Err(ValidateError::invalid_literal(
                field,
                "enum path does not match field enum type",
            ));
        }

        return Ok(value.clone());
    }

    let mut normalized = enum_value.clone();
    normalized.set_path(Some(expected_path.to_string()));
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

    // Compare-pair simplification scans all conjunction children directly, so
    // it does not require a pre-sorted shape to preserve semantics.
    let Some(mut out) = simplify_and_compare_constraints(out) else {
        return Predicate::False;
    };

    // Canonicalize after simplification because compare folding can replace or
    // remove children and therefore change deterministic evaluation order.
    canonicalize_predicate_children_for_eval(&mut out);

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

    // Canonicalize disjunction children once before OR-specific rewrites so the
    // collapse-to-IN check sees one deterministic shape.
    canonicalize_predicate_children_for_eval(&mut out);

    // Collapse canonical same-field equality disjunctions into one IN compare
    // at the predicate authority boundary.
    if let Some(collapsed) = collapse_same_field_or_eq_to_in(out.as_slice()) {
        return collapsed;
    }

    if out.len() == 1 {
        return out.remove(0);
    }

    Predicate::Or(out)
}

// Collapse `field = a OR field = b ...` into `field IN [a, b, ...]` when:
// - all children are equality compares
// - all children target the same field
// - all children share one supported coercion family
// - all equality literals are scalar-ish (not list/map payloads)
fn collapse_same_field_or_eq_to_in(children: &[Predicate]) -> Option<Predicate> {
    if children.len() < 2 {
        return None;
    }

    let mut leaves = Vec::with_capacity(children.len());

    for child in children {
        let Predicate::Compare(compare) = child else {
            return None;
        };
        if compare.op != CompareOp::Eq {
            return None;
        }
        if !matches!(
            compare.coercion.id,
            CoercionId::Strict | CoercionId::TextCasefold
        ) {
            return None;
        }
        if !or_eq_compare_value_is_in_safe(&compare.value) {
            return None;
        }
        leaves.push(MembershipCompareLeaf::new(
            compare.field.as_str(),
            compare.value.clone(),
            compare.coercion.id,
        ));
    }

    collapse_membership_compare_leaves(leaves, CompareOp::In).map(Predicate::Compare)
}

// Keep OR->IN canonicalization fail-closed for collection/map literals because
// list-like equality remains a distinct validation/runtime surface from `IN`.
const fn or_eq_compare_value_is_in_safe(value: &Value) -> bool {
    !matches!(value, Value::List(_) | Value::Map(_))
}

// Return a stable heuristic rank for predicate evaluation cost. Lower ranks
// are evaluated first after normalization.
const fn predicate_eval_cost_rank(predicate: &Predicate) -> u8 {
    match predicate {
        Predicate::True | Predicate::False => 0,
        Predicate::Compare(_)
        | Predicate::CompareFields(_)
        | Predicate::IsNull { .. }
        | Predicate::IsNotNull { .. }
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. } => 1,
        Predicate::Not(_) => 2,
        Predicate::TextContains { .. } | Predicate::TextContainsCi { .. } => 3,
        Predicate::And(_) | Predicate::Or(_) => 4,
    }
}

// Canonicalize predicate child ordering for deterministic normalization and
// cheap-first short-circuit behavior.
fn canonicalize_predicate_children_for_eval(out: &mut Vec<Predicate>) {
    out.sort_by(canonical_cmp_predicate_for_eval);
    out.dedup();
}

// Compare predicate children with the same deterministic rank-first ordering
// used by normalization, without routing through the cached-key tuple surface.
fn canonical_cmp_predicate_for_eval(left: &Predicate, right: &Predicate) -> std::cmp::Ordering {
    let rank = predicate_eval_cost_rank(left).cmp(&predicate_eval_cost_rank(right));
    if rank != std::cmp::Ordering::Equal {
        return rank;
    }

    sort_key(left).cmp(&sort_key(right))
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
        db::predicate::{
            CoercionId, CoercionSpec, CompareOp, ComparePredicate, Predicate, normalize,
            normalize::{normalize_compare_value_for_kind, normalize_value_for_kind},
        },
        model::field::FieldKind,
        value::Value,
    };

    #[test]
    fn normalize_and_dedups_identical_children_and_collapses_to_singleton() {
        let duplicated = Predicate::And(vec![
            Predicate::eq("rank".to_string(), Value::Nat(7)),
            Predicate::eq("rank".to_string(), Value::Nat(7)),
        ]);

        let normalized = normalize(&duplicated);

        assert_eq!(
            normalized,
            Predicate::Compare(ComparePredicate::eq("rank".to_string(), Value::Nat(7))),
            "identical AND children should collapse to one predicate",
        );
    }

    #[test]
    fn normalize_or_dedups_identical_children_and_collapses_to_singleton() {
        let duplicated = Predicate::Or(vec![
            Predicate::eq("rank".to_string(), Value::Nat(7)),
            Predicate::eq("rank".to_string(), Value::Nat(7)),
        ]);

        let normalized = normalize(&duplicated);

        assert_eq!(
            normalized,
            Predicate::Compare(ComparePredicate::eq("rank".to_string(), Value::Nat(7))),
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
            Predicate::eq("rank".to_string(), Value::Nat(7)),
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

    #[test]
    fn normalize_and_conflicting_eq_literals_collapses_to_false() {
        let predicate = Predicate::And(vec![
            Predicate::eq("rank".to_string(), Value::Nat(1)),
            Predicate::eq("rank".to_string(), Value::Nat(2)),
        ]);

        let normalized = normalize(&predicate);

        assert_eq!(
            normalized,
            Predicate::False,
            "conflicting equalities in conjunction must collapse to false",
        );
    }

    #[test]
    fn normalize_and_tightens_lower_bounds() {
        let predicate = Predicate::And(vec![
            Predicate::Compare(ComparePredicate::gt("rank".to_string(), Value::Nat(3))),
            Predicate::Compare(ComparePredicate::gte("rank".to_string(), Value::Nat(5))),
        ]);

        let normalized = normalize(&predicate);

        assert_eq!(
            normalized,
            Predicate::Compare(ComparePredicate::gte("rank".to_string(), Value::Nat(5))),
            "conjunction should keep the stricter lower bound",
        );
    }

    #[test]
    fn normalize_and_tightens_upper_bounds() {
        let predicate = Predicate::And(vec![
            Predicate::Compare(ComparePredicate::lt("rank".to_string(), Value::Nat(9))),
            Predicate::Compare(ComparePredicate::lte("rank".to_string(), Value::Nat(7))),
        ]);

        let normalized = normalize(&predicate);

        assert_eq!(
            normalized,
            Predicate::Compare(ComparePredicate::lte("rank".to_string(), Value::Nat(7))),
            "conjunction should keep the stricter upper bound",
        );
    }

    #[test]
    fn normalize_and_eq_with_satisfied_bound_collapses_to_eq() {
        let predicate = Predicate::And(vec![
            Predicate::eq("rank".to_string(), Value::Nat(7)),
            Predicate::Compare(ComparePredicate::gt("rank".to_string(), Value::Nat(5))),
        ]);

        let normalized = normalize(&predicate);

        assert_eq!(
            normalized,
            Predicate::Compare(ComparePredicate::eq("rank".to_string(), Value::Nat(7))),
            "equality should subsume compatible lower-bound constraints",
        );
    }

    #[test]
    fn normalize_and_eq_with_conflicting_bound_collapses_to_false() {
        let predicate = Predicate::And(vec![
            Predicate::eq("rank".to_string(), Value::Nat(3)),
            Predicate::Compare(ComparePredicate::gt("rank".to_string(), Value::Nat(5))),
        ]);

        let normalized = normalize(&predicate);

        assert_eq!(
            normalized,
            Predicate::False,
            "equality conflicting with a bound must collapse to false",
        );
    }

    #[test]
    fn normalize_and_equal_lower_and_upper_collapse_to_eq() {
        let predicate = Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "rank",
                CompareOp::Gte,
                Value::Nat(11),
                crate::db::predicate::CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "rank",
                CompareOp::Lte,
                Value::Nat(11),
                crate::db::predicate::CoercionId::Strict,
            )),
        ]);

        let normalized = normalize(&predicate);

        assert_eq!(
            normalized,
            Predicate::Compare(ComparePredicate::eq("rank".to_string(), Value::Nat(11))),
            "matching inclusive lower/upper bounds should collapse to equality",
        );
    }

    #[test]
    fn normalize_and_crossed_bounds_collapse_to_false() {
        let predicate = Predicate::And(vec![
            Predicate::Compare(ComparePredicate::gt("rank".to_string(), Value::Nat(9))),
            Predicate::Compare(ComparePredicate::lt("rank".to_string(), Value::Nat(5))),
        ]);

        let normalized = normalize(&predicate);

        assert_eq!(
            normalized,
            Predicate::False,
            "crossed lower/upper bounds must collapse to false",
        );
    }

    #[test]
    fn normalize_or_same_field_eq_collapses_to_in() {
        let predicate = Predicate::Or(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "tag",
                CompareOp::Eq,
                Value::Text("beta".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "tag",
                CompareOp::Eq,
                Value::Text("alpha".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "tag",
                CompareOp::Eq,
                Value::Text("beta".to_string()),
                CoercionId::Strict,
            )),
        ]);

        let normalized = normalize(&predicate);
        let Predicate::Compare(compare) = normalized else {
            panic!("same-field strict OR-equality should collapse to one IN compare");
        };

        assert_eq!(compare.field, "tag".to_string());
        assert_eq!(compare.op, CompareOp::In);
        assert_eq!(compare.coercion.id, CoercionId::Strict);
        let Value::List(mut values) = compare.value else {
            panic!("collapsed OR-equality compare should carry list literal");
        };
        values.sort_by(Value::canonical_cmp);
        assert_eq!(
            values,
            vec![
                Value::Text("alpha".to_string()),
                Value::Text("beta".to_string()),
            ],
            "same-field strict OR-equality should collapse to deduplicated IN-list members",
        );
    }

    #[test]
    fn normalize_or_mixed_eq_coercions_do_not_collapse_to_in() {
        let predicate = Predicate::Or(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "tag",
                CompareOp::Eq,
                Value::Text("alpha".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "tag",
                CompareOp::Eq,
                Value::Text("beta".to_string()),
                CoercionId::TextCasefold,
            )),
        ]);

        let normalized = normalize(&predicate);
        let Predicate::Or(children) = normalized else {
            panic!("mixed coercion OR-equality should remain OR in canonical form");
        };

        assert_eq!(children.len(), 2);
    }

    #[test]
    fn normalize_or_list_equality_literals_do_not_collapse_to_in() {
        let predicate = Predicate::Or(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "tags",
                CompareOp::Eq,
                Value::List(vec![Value::Text("a".to_string())]),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "tags",
                CompareOp::Eq,
                Value::List(vec![Value::Text("b".to_string())]),
                CoercionId::Strict,
            )),
        ]);

        let normalized = normalize(&predicate);
        let Predicate::Or(children) = normalized else {
            panic!("list-literal OR-equality should remain OR in canonical form");
        };

        assert_eq!(children.len(), 2);
    }

    #[test]
    fn normalize_value_for_set_kind_canonicalizes_members() {
        let normalized = normalize_value_for_kind(
            "tags",
            &Value::List(vec![
                Value::Text("beta".to_string()),
                Value::Text("alpha".to_string()),
                Value::Text("beta".to_string()),
            ]),
            &FieldKind::Set(&FieldKind::Text { max_len: None }),
            &CoercionSpec::new(CoercionId::Strict),
            CompareOp::Eq,
        )
        .expect("set literal normalization should succeed");

        assert_eq!(
            normalized,
            Value::List(vec![
                Value::Text("alpha".to_string()),
                Value::Text("beta".to_string()),
            ]),
            "set literal normalization should sort and deduplicate members",
        );
    }

    #[test]
    fn normalize_compare_value_for_in_kind_canonicalizes_members() {
        let normalized = normalize_compare_value_for_kind(
            "rank",
            CompareOp::In,
            &Value::List(vec![
                Value::Nat(3),
                Value::Nat(1),
                Value::Nat(3),
                Value::Nat(2),
            ]),
            &FieldKind::Nat,
            &CoercionSpec::new(CoercionId::Strict),
        )
        .expect("IN literal normalization should succeed");

        assert_eq!(
            normalized,
            Value::List(vec![Value::Nat(1), Value::Nat(2), Value::Nat(3)]),
            "IN literal normalization should sort and deduplicate members",
        );
    }

    #[test]
    fn normalize_compare_value_for_not_in_kind_canonicalizes_members() {
        let normalized = normalize_compare_value_for_kind(
            "rank",
            CompareOp::NotIn,
            &Value::List(vec![
                Value::Nat(3),
                Value::Nat(1),
                Value::Nat(3),
                Value::Nat(2),
            ]),
            &FieldKind::Nat,
            &CoercionSpec::new(CoercionId::Strict),
        )
        .expect("NOT IN literal normalization should succeed");

        assert_eq!(
            normalized,
            Value::List(vec![Value::Nat(1), Value::Nat(2), Value::Nat(3)]),
            "NOT IN literal normalization should sort and deduplicate members",
        );
    }
}

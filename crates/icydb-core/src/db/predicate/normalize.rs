//! Module: predicate::normalize
//! Responsibility: deterministic predicate normalization and enum-literal adjustment.
//! Does not own: runtime evaluation or schema field-slot resolution.
//! Boundary: normalize before validation/planning/fingerprinting.

use crate::{
    db::predicate::{
        CompareOp, ComparePredicate, Predicate, SchemaInfo, ValidateError, compare_eq,
        compare_order, encoding::encode_predicate_sort_key,
    },
    model::field::FieldKind,
    value::Value,
};
use std::cmp::Ordering;

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

    // Evaluate cheaper predicates first to reduce average short-circuit work
    // while keeping deterministic ordering under the canonical sort key.
    out.sort_by_cached_key(|predicate| (predicate_eval_cost_rank(predicate), sort_key(predicate)));
    out.dedup();
    let Some(mut out) = simplify_and_compare_constraints(out) else {
        return Predicate::False;
    };
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

#[derive(Clone)]
enum ComparePairSimplification {
    NoChange,
    Contradiction,
    KeepFirst,
    KeepSecond,
    ReplaceFirst(ComparePredicate),
    ReplaceSecond(ComparePredicate),
}

// Simplify conjunction-local compare predicates over the same field/coercion
// domain. This pass is conservative: unsupported or incomparable pairs are
// preserved unchanged.
fn simplify_and_compare_constraints(mut predicates: Vec<Predicate>) -> Option<Vec<Predicate>> {
    loop {
        let mut changed = false;
        'scan: for i in 0..predicates.len() {
            for j in i.saturating_add(1)..predicates.len() {
                let (Predicate::Compare(left), Predicate::Compare(right)) =
                    (&predicates[i], &predicates[j])
                else {
                    continue;
                };
                if left.field != right.field || left.coercion != right.coercion {
                    continue;
                }

                match simplify_compare_pair_for_and(left, right) {
                    ComparePairSimplification::NoChange => continue,
                    ComparePairSimplification::Contradiction => return None,
                    ComparePairSimplification::KeepFirst => {
                        predicates.remove(j);
                    }
                    ComparePairSimplification::KeepSecond => {
                        predicates.remove(i);
                    }
                    ComparePairSimplification::ReplaceFirst(replacement) => {
                        predicates[i] = Predicate::Compare(replacement);
                        predicates.remove(j);
                    }
                    ComparePairSimplification::ReplaceSecond(replacement) => {
                        predicates[j] = Predicate::Compare(replacement);
                        predicates.remove(i);
                    }
                }

                changed = true;
                break 'scan;
            }
        }

        if !changed {
            break;
        }
    }

    Some(predicates)
}

// Simplify one pair of compare predicates in an AND clause.
fn simplify_compare_pair_for_and(
    left: &ComparePredicate,
    right: &ComparePredicate,
) -> ComparePairSimplification {
    match (left.op, right.op) {
        (CompareOp::Eq, CompareOp::Eq) => simplify_eq_eq_pair(left, right),
        (CompareOp::Eq, _) => simplify_eq_with_constraint_pair(left, right, true),
        (_, CompareOp::Eq) => simplify_eq_with_constraint_pair(right, left, false),
        _ => simplify_constraint_constraint_pair(left, right),
    }
}

// Simplify `field = a AND field = b`.
fn simplify_eq_eq_pair(
    left: &ComparePredicate,
    right: &ComparePredicate,
) -> ComparePairSimplification {
    match compare_eq(&left.value, &right.value, &left.coercion) {
        Some(true) => ComparePairSimplification::KeepFirst,
        Some(false) => ComparePairSimplification::Contradiction,
        None => ComparePairSimplification::NoChange,
    }
}

// Simplify `field = a AND field <op> b` where `<op>` is one inequality bound.
//
// `eq_is_first` indicates whether `eq` is the left/first pair item.
fn simplify_eq_with_constraint_pair(
    eq: &ComparePredicate,
    constraint: &ComparePredicate,
    eq_is_first: bool,
) -> ComparePairSimplification {
    let Some(ordering) = compare_order(&eq.value, &constraint.value, &eq.coercion) else {
        return ComparePairSimplification::NoChange;
    };
    let satisfies = match constraint.op {
        CompareOp::Gt => ordering.is_gt(),
        CompareOp::Gte => ordering.is_gt() || ordering.is_eq(),
        CompareOp::Lt => ordering.is_lt(),
        CompareOp::Lte => ordering.is_lt() || ordering.is_eq(),
        CompareOp::Eq
        | CompareOp::Ne
        | CompareOp::In
        | CompareOp::NotIn
        | CompareOp::Contains
        | CompareOp::StartsWith
        | CompareOp::EndsWith => return ComparePairSimplification::NoChange,
    };

    if !satisfies {
        return ComparePairSimplification::Contradiction;
    }
    if eq_is_first {
        ComparePairSimplification::KeepFirst
    } else {
        ComparePairSimplification::KeepSecond
    }
}

// Simplify inequality-pair combinations in conjunctions:
// - tighter lower-bound retention (`>`, `>=`)
// - tighter upper-bound retention (`<`, `<=`)
// - lower/upper contradiction detection
// - lower/upper equality collapse (`>= a AND <= a -> = a`)
fn simplify_constraint_constraint_pair(
    left: &ComparePredicate,
    right: &ComparePredicate,
) -> ComparePairSimplification {
    let left_lower = lower_bound_inclusive(left.op);
    let right_lower = lower_bound_inclusive(right.op);
    let left_upper = upper_bound_inclusive(left.op);
    let right_upper = upper_bound_inclusive(right.op);

    if left_lower.is_some() && right_lower.is_some() {
        return simplify_two_lower_bounds(left, right);
    }
    if left_upper.is_some() && right_upper.is_some() {
        return simplify_two_upper_bounds(left, right);
    }
    if left_lower.is_some() && right_upper.is_some() {
        return simplify_lower_upper_pair(left, right);
    }
    if left_upper.is_some() && right_lower.is_some() {
        return match simplify_lower_upper_pair(right, left) {
            ComparePairSimplification::KeepFirst => ComparePairSimplification::KeepSecond,
            ComparePairSimplification::KeepSecond => ComparePairSimplification::KeepFirst,
            ComparePairSimplification::ReplaceFirst(cmp) => {
                ComparePairSimplification::ReplaceSecond(cmp)
            }
            ComparePairSimplification::ReplaceSecond(cmp) => {
                ComparePairSimplification::ReplaceFirst(cmp)
            }
            ComparePairSimplification::NoChange => ComparePairSimplification::NoChange,
            ComparePairSimplification::Contradiction => ComparePairSimplification::Contradiction,
        };
    }

    ComparePairSimplification::NoChange
}

fn simplify_two_lower_bounds(
    left: &ComparePredicate,
    right: &ComparePredicate,
) -> ComparePairSimplification {
    let Some(ordering) = compare_order(&left.value, &right.value, &left.coercion) else {
        return ComparePairSimplification::NoChange;
    };
    let Some(left_inclusive) = lower_bound_inclusive(left.op) else {
        return ComparePairSimplification::NoChange;
    };
    let Some(right_inclusive) = lower_bound_inclusive(right.op) else {
        return ComparePairSimplification::NoChange;
    };

    match ordering {
        Ordering::Greater => ComparePairSimplification::KeepFirst,
        Ordering::Less => ComparePairSimplification::KeepSecond,
        Ordering::Equal => {
            if !left_inclusive && right_inclusive {
                ComparePairSimplification::KeepFirst
            } else if left_inclusive && !right_inclusive {
                ComparePairSimplification::KeepSecond
            } else {
                ComparePairSimplification::KeepFirst
            }
        }
    }
}

fn simplify_two_upper_bounds(
    left: &ComparePredicate,
    right: &ComparePredicate,
) -> ComparePairSimplification {
    let Some(ordering) = compare_order(&left.value, &right.value, &left.coercion) else {
        return ComparePairSimplification::NoChange;
    };
    let Some(left_inclusive) = upper_bound_inclusive(left.op) else {
        return ComparePairSimplification::NoChange;
    };
    let Some(right_inclusive) = upper_bound_inclusive(right.op) else {
        return ComparePairSimplification::NoChange;
    };

    match ordering {
        Ordering::Less => ComparePairSimplification::KeepFirst,
        Ordering::Greater => ComparePairSimplification::KeepSecond,
        Ordering::Equal => {
            if !left_inclusive && right_inclusive {
                ComparePairSimplification::KeepFirst
            } else if left_inclusive && !right_inclusive {
                ComparePairSimplification::KeepSecond
            } else {
                ComparePairSimplification::KeepFirst
            }
        }
    }
}

// Simplify `lower AND upper`, where `lower` is one of (`>`,`>=`) and `upper`
// is one of (`<`,`<=`).
fn simplify_lower_upper_pair(
    lower: &ComparePredicate,
    upper: &ComparePredicate,
) -> ComparePairSimplification {
    let Some(ordering) = compare_order(&lower.value, &upper.value, &lower.coercion) else {
        return ComparePairSimplification::NoChange;
    };
    let Some(lower_inclusive) = lower_bound_inclusive(lower.op) else {
        return ComparePairSimplification::NoChange;
    };
    let Some(upper_inclusive) = upper_bound_inclusive(upper.op) else {
        return ComparePairSimplification::NoChange;
    };

    match ordering {
        Ordering::Less => ComparePairSimplification::NoChange,
        Ordering::Greater => ComparePairSimplification::Contradiction,
        Ordering::Equal => {
            if lower_inclusive && upper_inclusive {
                ComparePairSimplification::ReplaceFirst(ComparePredicate {
                    field: lower.field.clone(),
                    op: CompareOp::Eq,
                    value: lower.value.clone(),
                    coercion: lower.coercion.clone(),
                })
            } else {
                ComparePairSimplification::Contradiction
            }
        }
    }
}

const fn lower_bound_inclusive(op: CompareOp) -> Option<bool> {
    match op {
        CompareOp::Gt => Some(false),
        CompareOp::Gte => Some(true),
        CompareOp::Eq
        | CompareOp::Ne
        | CompareOp::Lt
        | CompareOp::Lte
        | CompareOp::In
        | CompareOp::NotIn
        | CompareOp::Contains
        | CompareOp::StartsWith
        | CompareOp::EndsWith => None,
    }
}

const fn upper_bound_inclusive(op: CompareOp) -> Option<bool> {
    match op {
        CompareOp::Lt => Some(false),
        CompareOp::Lte => Some(true),
        CompareOp::Eq
        | CompareOp::Ne
        | CompareOp::Gt
        | CompareOp::Gte
        | CompareOp::In
        | CompareOp::NotIn
        | CompareOp::Contains
        | CompareOp::StartsWith
        | CompareOp::EndsWith => None,
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::predicate::{CompareOp, ComparePredicate, Predicate, normalize},
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

    #[test]
    fn normalize_and_conflicting_eq_literals_collapses_to_false() {
        let predicate = Predicate::And(vec![
            Predicate::eq("rank".to_string(), Value::Uint(1)),
            Predicate::eq("rank".to_string(), Value::Uint(2)),
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
            Predicate::Compare(ComparePredicate::gt("rank".to_string(), Value::Uint(3))),
            Predicate::Compare(ComparePredicate::gte("rank".to_string(), Value::Uint(5))),
        ]);

        let normalized = normalize(&predicate);

        assert_eq!(
            normalized,
            Predicate::Compare(ComparePredicate::gte("rank".to_string(), Value::Uint(5))),
            "conjunction should keep the stricter lower bound",
        );
    }

    #[test]
    fn normalize_and_tightens_upper_bounds() {
        let predicate = Predicate::And(vec![
            Predicate::Compare(ComparePredicate::lt("rank".to_string(), Value::Uint(9))),
            Predicate::Compare(ComparePredicate::lte("rank".to_string(), Value::Uint(7))),
        ]);

        let normalized = normalize(&predicate);

        assert_eq!(
            normalized,
            Predicate::Compare(ComparePredicate::lte("rank".to_string(), Value::Uint(7))),
            "conjunction should keep the stricter upper bound",
        );
    }

    #[test]
    fn normalize_and_eq_with_satisfied_bound_collapses_to_eq() {
        let predicate = Predicate::And(vec![
            Predicate::eq("rank".to_string(), Value::Uint(7)),
            Predicate::Compare(ComparePredicate::gt("rank".to_string(), Value::Uint(5))),
        ]);

        let normalized = normalize(&predicate);

        assert_eq!(
            normalized,
            Predicate::Compare(ComparePredicate::eq("rank".to_string(), Value::Uint(7))),
            "equality should subsume compatible lower-bound constraints",
        );
    }

    #[test]
    fn normalize_and_eq_with_conflicting_bound_collapses_to_false() {
        let predicate = Predicate::And(vec![
            Predicate::eq("rank".to_string(), Value::Uint(3)),
            Predicate::Compare(ComparePredicate::gt("rank".to_string(), Value::Uint(5))),
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
                Value::Uint(11),
                crate::db::predicate::CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "rank",
                CompareOp::Lte,
                Value::Uint(11),
                crate::db::predicate::CoercionId::Strict,
            )),
        ]);

        let normalized = normalize(&predicate);

        assert_eq!(
            normalized,
            Predicate::Compare(ComparePredicate::eq("rank".to_string(), Value::Uint(11))),
            "matching inclusive lower/upper bounds should collapse to equality",
        );
    }

    #[test]
    fn normalize_and_crossed_bounds_collapse_to_false() {
        let predicate = Predicate::And(vec![
            Predicate::Compare(ComparePredicate::gt("rank".to_string(), Value::Uint(9))),
            Predicate::Compare(ComparePredicate::lt("rank".to_string(), Value::Uint(5))),
        ]);

        let normalized = normalize(&predicate);

        assert_eq!(
            normalized,
            Predicate::False,
            "crossed lower/upper bounds must collapse to false",
        );
    }
}

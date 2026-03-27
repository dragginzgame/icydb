//! Module: query::plan::planner::range
//! Responsibility: planner range-constraint extraction and index-range candidate derivation.
//! Does not own: runtime range traversal execution or cursor persistence format.
//! Boundary: computes planner-side range constraints from predicate semantics.

use crate::{
    db::{
        access::SemanticIndexRangeSpec,
        numeric::compare_numeric_or_strict_order,
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate, canonical_cmp},
        query::plan::planner::{index_literal_matches_schema, sorted_indexes},
        schema::{FieldType, SchemaInfo},
    },
    model::{entity::EntityModel, index::IndexModel},
    value::Value,
};
use std::{cmp::Ordering, ops::Bound};

///
/// RangeConstraint
/// One-field bounded interval used for index-range candidate extraction.
///

#[derive(Clone, Debug, Eq, PartialEq)]
struct RangeConstraint {
    lower: Bound<Value>,
    upper: Bound<Value>,
}

impl Default for RangeConstraint {
    fn default() -> Self {
        Self {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        }
    }
}

///
/// IndexFieldConstraint
/// Per-index-field constraint classification while extracting range candidates.
///

#[derive(Clone, Debug, Eq, PartialEq)]
enum IndexFieldConstraint {
    None,
    Eq(Value),
    Range(RangeConstraint),
}

///
/// CachedCompare
///
/// Compare predicate plus precomputed planner-side schema compatibility.
///

#[derive(Clone)]
struct CachedCompare<'a> {
    cmp: &'a ComparePredicate,
    literal_compatible: bool,
}

// Build one deterministic secondary-range candidate from a normalized AND-group.
//
// Extraction contract:
// - Every child must be a Compare predicate.
// - Supported operators are Eq/Gt/Gte/Lt/Lte only.
// - For a chosen index: fields 0..k must be Eq, field k must be Range,
//   fields after k must be unconstrained.
pub(in crate::db::query::plan::planner) fn index_range_from_and(
    model: &EntityModel,
    schema: &SchemaInfo,
    children: &[Predicate],
    query_predicate: &Predicate,
) -> Option<SemanticIndexRangeSpec> {
    let mut compares = Vec::with_capacity(children.len());
    for child in children {
        let Predicate::Compare(cmp) = child else {
            return None;
        };
        if !matches!(
            cmp.op,
            CompareOp::Eq | CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte
        ) {
            return None;
        }
        if !matches!(
            cmp.coercion.id,
            CoercionId::Strict | CoercionId::NumericWiden
        ) {
            return None;
        }
        compares.push(CachedCompare {
            cmp,
            literal_compatible: index_literal_matches_schema(schema, &cmp.field, &cmp.value),
        });
    }

    let mut best: Option<(
        usize,
        &'static IndexModel,
        usize,
        Vec<Value>,
        RangeConstraint,
    )> = None;
    for index in sorted_indexes(model, query_predicate) {
        let Some((range_slot, prefix, range)) =
            index_range_candidate_for_index(index, schema, &compares)
        else {
            continue;
        };

        let prefix_len = prefix.len();
        match best {
            None => best = Some((prefix_len, index, range_slot, prefix, range)),
            Some((best_len, best_index, _, _, _))
                if prefix_len > best_len
                    || (prefix_len == best_len && index.name() < best_index.name()) =>
            {
                best = Some((prefix_len, index, range_slot, prefix, range));
            }
            _ => {}
        }
    }

    best.map(|(_, index, range_slot, prefix, range)| {
        let field_slots = (0..=range_slot).collect();

        SemanticIndexRangeSpec::new(*index, field_slots, prefix, range.lower, range.upper)
    })
}

// Extract an index-range candidate for one concrete index by walking index
// fields directly instead of materializing a temporary per-field constraint
// vector that is only consumed once.
fn index_range_candidate_for_index(
    index: &'static IndexModel,
    schema: &SchemaInfo,
    compares: &[CachedCompare<'_>],
) -> Option<(usize, Vec<Value>, RangeConstraint)> {
    let mut prefix = Vec::new();
    let mut range: Option<RangeConstraint> = None;
    let mut range_position = None;

    for (position, field_name) in index.fields().iter().enumerate() {
        let constraint = field_constraint_for_index_field(index, schema, field_name, compares)?;
        match constraint {
            IndexFieldConstraint::Eq(value) if range.is_none() => {
                prefix.push(value);
            }
            IndexFieldConstraint::Range(candidate) if range.is_none() => {
                range = Some(candidate);
                range_position = Some(position);
            }
            IndexFieldConstraint::None if range.is_none() => return None,
            IndexFieldConstraint::None => {}
            _ => return None,
        }
    }

    let (Some(range_position), Some(range)) = (range_position, range) else {
        return None;
    };
    if prefix.len() >= index.fields().len() {
        return None;
    }

    Some((range_position, prefix, range))
}

// Build the effective constraint class for one concrete index field from the
// compare predicates that target it.
fn field_constraint_for_index_field(
    index: &'static IndexModel,
    schema: &SchemaInfo,
    field_name: &&'static str,
    compares: &[CachedCompare<'_>],
) -> Option<IndexFieldConstraint> {
    let mut constraint = IndexFieldConstraint::None;
    let field_type = schema.field(field_name)?;

    for cached in compares {
        let cmp = cached.cmp;
        if cmp.field.as_str() != *field_name {
            continue;
        }
        if strict_field_range_requires_full_scan(field_type, cmp.coercion.id, cmp.op) {
            return None;
        }
        if !cached.literal_compatible || !index.is_field_indexable(field_name, cmp.op) {
            return None;
        }

        match cmp.op {
            CompareOp::Eq => match &constraint {
                IndexFieldConstraint::None => {
                    constraint = IndexFieldConstraint::Eq(cmp.value.clone());
                }
                IndexFieldConstraint::Eq(existing) => {
                    if existing != &cmp.value {
                        return None;
                    }
                }
                IndexFieldConstraint::Range(_) => return None,
            },
            CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
                let mut range = match &constraint {
                    IndexFieldConstraint::None => RangeConstraint::default(),
                    IndexFieldConstraint::Eq(_) => return None,
                    IndexFieldConstraint::Range(existing) => existing.clone(),
                };
                if !merge_range_constraint(&mut range, cmp.op, &cmp.value) {
                    return None;
                }
                constraint = IndexFieldConstraint::Range(range);
            }
            _ => return None,
        }
    }

    Some(constraint)
}

// Merge one comparison operator into one bounded range under shared numeric
// comparison semantics for numeric values and strict ordering semantics for
// non-numeric values.
fn merge_range_constraint(existing: &mut RangeConstraint, op: CompareOp, value: &Value) -> bool {
    let merged = match op {
        CompareOp::Gt => merge_lower_bound(&mut existing.lower, Bound::Excluded(value.clone())),
        CompareOp::Gte => merge_lower_bound(&mut existing.lower, Bound::Included(value.clone())),
        CompareOp::Lt => merge_upper_bound(&mut existing.upper, Bound::Excluded(value.clone())),
        CompareOp::Lte => merge_upper_bound(&mut existing.upper, Bound::Included(value.clone())),
        _ => false,
    };
    if !merged {
        return false;
    }

    range_bounds_are_compatible(existing)
}

fn merge_lower_bound(existing: &mut Bound<Value>, candidate: Bound<Value>) -> bool {
    let replace = match (&candidate, &*existing) {
        (Bound::Unbounded, _) => false,
        (_, Bound::Unbounded) => true,
        (
            Bound::Included(left) | Bound::Excluded(left),
            Bound::Included(right) | Bound::Excluded(right),
        ) => match compare_range_bound_values(left, right) {
            Some(Ordering::Greater) => true,
            Some(Ordering::Less) => false,
            Some(Ordering::Equal) => {
                matches!(candidate, Bound::Excluded(_)) && matches!(existing, Bound::Included(_))
            }
            None => return false,
        },
    };

    if replace {
        *existing = candidate;
    }

    true
}

fn merge_upper_bound(existing: &mut Bound<Value>, candidate: Bound<Value>) -> bool {
    let replace = match (&candidate, &*existing) {
        (Bound::Unbounded, _) => false,
        (_, Bound::Unbounded) => true,
        (
            Bound::Included(left) | Bound::Excluded(left),
            Bound::Included(right) | Bound::Excluded(right),
        ) => match compare_range_bound_values(left, right) {
            Some(Ordering::Less) => true,
            Some(Ordering::Greater) => false,
            Some(Ordering::Equal) => {
                matches!(candidate, Bound::Excluded(_)) && matches!(existing, Bound::Included(_))
            }
            None => return false,
        },
    };

    if replace {
        *existing = candidate;
    }

    true
}

// Validate interval shape and reject empty or incomparable intervals.
fn range_bounds_are_compatible(range: &RangeConstraint) -> bool {
    let (Some(lower), Some(upper)) = (bound_value(&range.lower), bound_value(&range.upper)) else {
        return true;
    };

    let Some(ordering) = compare_range_bound_values(lower, upper) else {
        return false;
    };

    match ordering {
        Ordering::Less => true,
        Ordering::Greater => false,
        Ordering::Equal => {
            matches!(range.lower, Bound::Included(_)) && matches!(range.upper, Bound::Included(_))
        }
    }
}

const fn bound_value(bound: &Bound<Value>) -> Option<&Value> {
    match bound {
        Bound::Included(value) | Bound::Excluded(value) => Some(value),
        Bound::Unbounded => None,
    }
}

fn compare_range_bound_values(left: &Value, right: &Value) -> Option<Ordering> {
    if let Some(ordering) = compare_numeric_or_strict_order(left, right) {
        return Some(ordering);
    }

    if std::mem::discriminant(left) == std::mem::discriminant(right) {
        return Some(canonical_cmp(left, right));
    }

    None
}

fn strict_field_range_requires_full_scan(
    field_type: &FieldType,
    coercion: CoercionId,
    op: CompareOp,
) -> bool {
    // Raw secondary-index key ordering includes per-component length framing, so
    // strict field-key text ranges are not lexicographically preserved at the
    // raw-byte scan boundary. Fail closed for ordered text range extraction.
    coercion == CoercionId::Strict
        && field_type.is_text()
        && matches!(
            op,
            CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte
        )
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            numeric::compare_numeric_or_strict_order,
            query::plan::planner::range::compare_range_bound_values,
        },
        value::Value,
    };
    use std::cmp::Ordering;

    #[test]
    fn range_bound_numeric_compare_reuses_shared_numeric_authority() {
        let left = Value::Int(10);
        let right = Value::Uint(10);

        assert_eq!(
            compare_range_bound_values(&left, &right),
            compare_numeric_or_strict_order(&left, &right),
            "planner range numeric bounds should delegate to shared numeric comparator",
        );
    }

    #[test]
    fn range_bound_mixed_non_numeric_values_are_incomparable() {
        assert_eq!(
            compare_range_bound_values(&Value::Text("x".to_string()), &Value::Uint(1)),
            None,
            "mixed non-numeric variants should remain incomparable in range planning",
        );
    }

    #[test]
    fn range_bound_same_variant_non_numeric_uses_strict_ordering() {
        assert_eq!(
            compare_range_bound_values(
                &Value::Text("a".to_string()),
                &Value::Text("b".to_string())
            ),
            Some(Ordering::Less),
            "same-variant non-numeric bounds should use strict value ordering",
        );
    }
}

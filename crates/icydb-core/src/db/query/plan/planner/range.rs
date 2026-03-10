//! Module: query::plan::planner::range
//! Responsibility: planner range-constraint extraction and index-range candidate derivation.
//! Does not own: runtime range traversal execution or cursor persistence format.
//! Boundary: computes planner-side range constraints from predicate semantics.

use crate::{
    db::{
        access::SemanticIndexRangeSpec,
        numeric::compare_numeric_order,
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate, canonical_cmp},
        query::plan::planner::{index_literal_matches_schema, sorted_indexes},
        schema::SchemaInfo,
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
    for index in sorted_indexes(model) {
        let Some((range_slot, prefix, range)) = index_range_candidate_for_index(index, &compares)
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

// Extract an index-range candidate for one concrete index.
fn index_range_candidate_for_index(
    index: &'static IndexModel,
    compares: &[CachedCompare<'_>],
) -> Option<(usize, Vec<Value>, RangeConstraint)> {
    // Phase 1: classify each index field as Eq/Range/None for this compare set.
    let constraints = classify_index_field_constraints(index, compares)?;

    // Phase 2: materialize deterministic prefix+range shape from constraints.
    select_prefix_and_range(index.fields().len(), &constraints)
}

// Build per-field constraint classes for one index from compare predicates.
fn classify_index_field_constraints(
    index: &'static IndexModel,
    compares: &[CachedCompare<'_>],
) -> Option<Vec<IndexFieldConstraint>> {
    let mut constraints = vec![IndexFieldConstraint::None; index.fields().len()];

    for cached in compares {
        let cmp = cached.cmp;
        let Some(position) = index
            .fields()
            .iter()
            .position(|field| *field == cmp.field.as_str())
        else {
            continue;
        };

        if !cached.literal_compatible || !index.is_field_indexable(cmp.field.as_str(), cmp.op) {
            return None;
        }

        match cmp.op {
            CompareOp::Eq => match &mut constraints[position] {
                IndexFieldConstraint::None => {
                    constraints[position] = IndexFieldConstraint::Eq(cmp.value.clone());
                }
                IndexFieldConstraint::Eq(existing) => {
                    if existing != &cmp.value {
                        return None;
                    }
                }
                IndexFieldConstraint::Range(_) => return None,
            },
            CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
                let mut range = match &constraints[position] {
                    IndexFieldConstraint::None => RangeConstraint::default(),
                    IndexFieldConstraint::Eq(_) => return None,
                    IndexFieldConstraint::Range(existing) => existing.clone(),
                };
                if !merge_range_constraint(&mut range, cmp.op, &cmp.value) {
                    return None;
                }
                constraints[position] = IndexFieldConstraint::Range(range);
            }
            _ => return None,
        }
    }

    Some(constraints)
}

// Convert classified constraints into one valid prefix+range candidate shape.
fn select_prefix_and_range(
    field_count: usize,
    constraints: &[IndexFieldConstraint],
) -> Option<(usize, Vec<Value>, RangeConstraint)> {
    let mut prefix = Vec::new();
    let mut range: Option<RangeConstraint> = None;
    let mut range_position = None;

    for (position, constraint) in constraints.iter().enumerate() {
        match constraint {
            IndexFieldConstraint::Eq(value) if range.is_none() => {
                prefix.push(value.clone());
            }
            IndexFieldConstraint::Range(candidate) if range.is_none() => {
                range = Some(candidate.clone());
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
    if range_position >= field_count {
        return None;
    }
    if prefix.len() >= field_count {
        return None;
    }

    Some((range_position, prefix, range))
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
    if left.supports_numeric_coercion() || right.supports_numeric_coercion() {
        return compare_numeric_order(left, right);
    }

    if let Some(ordering) = Value::strict_order_cmp(left, right) {
        return Some(ordering);
    }

    if std::mem::discriminant(left) == std::mem::discriminant(right) {
        return Some(canonical_cmp(left, right));
    }

    None
}

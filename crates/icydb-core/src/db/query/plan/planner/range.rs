use crate::{
    db::query::{
        plan::{
            AccessPath,
            planner::{index_prefix_literal_is_compatible, sorted_indexes},
        },
        predicate::{
            CoercionId, CompareOp, ComparePredicate, Predicate, SchemaInfo, coercion::canonical_cmp,
        },
    },
    model::{entity::EntityModel, index::IndexModel},
    value::{CoercionFamily, CoercionFamilyExt, Value},
};
use std::{mem::discriminant, ops::Bound};

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
) -> Option<AccessPath<Value>> {
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
        compares.push(cmp);
    }

    let mut best: Option<(usize, &'static IndexModel, Vec<Value>, RangeConstraint)> = None;
    for index in sorted_indexes(model) {
        let Some((prefix, range)) = index_range_candidate_for_index(schema, index, &compares)
        else {
            continue;
        };

        let prefix_len = prefix.len();
        match best {
            None => best = Some((prefix_len, index, prefix, range)),
            Some((best_len, best_index, _, _))
                if prefix_len > best_len
                    || (prefix_len == best_len && index.name < best_index.name) =>
            {
                best = Some((prefix_len, index, prefix, range));
            }
            _ => {}
        }
    }

    best.map(|(_, index, prefix, range)| AccessPath::IndexRange {
        index: *index,
        prefix,
        lower: range.lower,
        upper: range.upper,
    })
}

// Validate one range literal for index-range planning.
pub(in crate::db::query::plan::planner) fn index_range_literal_is_compatible(
    schema: &SchemaInfo,
    field: &str,
    value: &Value,
) -> bool {
    index_prefix_literal_is_compatible(schema, field, value)
}

// Extract an index-range candidate for one concrete index.
fn index_range_candidate_for_index(
    schema: &SchemaInfo,
    index: &'static IndexModel,
    compares: &[&ComparePredicate],
) -> Option<(Vec<Value>, RangeConstraint)> {
    // Phase 1: classify each index field as Eq/Range/None for this compare set.
    let constraints = classify_index_field_constraints(schema, index, compares)?;

    // Phase 2: materialize deterministic prefix+range shape from constraints.
    select_prefix_and_range(index.fields.len(), &constraints)
}

// Build per-field constraint classes for one index from compare predicates.
fn classify_index_field_constraints(
    schema: &SchemaInfo,
    index: &'static IndexModel,
    compares: &[&ComparePredicate],
) -> Option<Vec<IndexFieldConstraint>> {
    let mut constraints = vec![IndexFieldConstraint::None; index.fields.len()];

    for cmp in compares {
        let Some(position) = index
            .fields
            .iter()
            .position(|field| *field == cmp.field.as_str())
        else {
            continue;
        };
        let field = index.fields[position];
        if !index_range_literal_is_compatible(schema, field, &cmp.value) {
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
) -> Option<(Vec<Value>, RangeConstraint)> {
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

    Some((prefix, range))
}

// Merge one comparison operator into a bounded range without widening semantics.
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
    if !bounds_numeric_variants_compatible(existing, &candidate) {
        return false;
    }

    let replace = match (&candidate, &*existing) {
        (Bound::Unbounded, _) => false,
        (_, Bound::Unbounded) => true,
        (
            Bound::Included(left) | Bound::Excluded(left),
            Bound::Included(right) | Bound::Excluded(right),
        ) => match canonical_cmp(left, right) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Equal => {
                matches!(candidate, Bound::Excluded(_)) && matches!(existing, Bound::Included(_))
            }
        },
    };

    if replace {
        *existing = candidate;
    }

    true
}

fn merge_upper_bound(existing: &mut Bound<Value>, candidate: Bound<Value>) -> bool {
    if !bounds_numeric_variants_compatible(existing, &candidate) {
        return false;
    }

    let replace = match (&candidate, &*existing) {
        (Bound::Unbounded, _) => false,
        (_, Bound::Unbounded) => true,
        (
            Bound::Included(left) | Bound::Excluded(left),
            Bound::Included(right) | Bound::Excluded(right),
        ) => match canonical_cmp(left, right) {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Greater => false,
            std::cmp::Ordering::Equal => {
                matches!(candidate, Bound::Excluded(_)) && matches!(existing, Bound::Included(_))
            }
        },
    };

    if replace {
        *existing = candidate;
    }

    true
}

// Validate interval shape and reject empty/mixed-numeric intervals.
fn range_bounds_are_compatible(range: &RangeConstraint) -> bool {
    let (Some(lower), Some(upper)) = (bound_value(&range.lower), bound_value(&range.upper)) else {
        return true;
    };

    if !numeric_variants_compatible(lower, upper) {
        return false;
    }

    !range_is_empty(range)
}

// Return true when a bounded range is empty under canonical value ordering.
fn range_is_empty(range: &RangeConstraint) -> bool {
    let (Some(lower), Some(upper)) = (bound_value(&range.lower), bound_value(&range.upper)) else {
        return false;
    };

    match canonical_cmp(lower, upper) {
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Equal => {
            !matches!(range.lower, Bound::Included(_)) || !matches!(range.upper, Bound::Included(_))
        }
    }
}

const fn bound_value(bound: &Bound<Value>) -> Option<&Value> {
    match bound {
        Bound::Included(value) | Bound::Excluded(value) => Some(value),
        Bound::Unbounded => None,
    }
}

fn bounds_numeric_variants_compatible(left: &Bound<Value>, right: &Bound<Value>) -> bool {
    match (bound_value(left), bound_value(right)) {
        (Some(left), Some(right)) => numeric_variants_compatible(left, right),
        _ => true,
    }
}

fn numeric_variants_compatible(left: &Value, right: &Value) -> bool {
    if left.coercion_family() != CoercionFamily::Numeric
        || right.coercion_family() != CoercionFamily::Numeric
    {
        return true;
    }

    discriminant(left) == discriminant(right)
}

use crate::{
    db::{
        numeric::compare_numeric_or_strict_order,
        predicate::{CompareOp, canonical_cmp},
        query::plan::planner::range::RangeConstraint,
    },
    value::Value,
};
use std::{cmp::Ordering, ops::Bound};

pub(in crate::db::query::plan::planner::range) fn merge_range_constraint(
    existing: &mut RangeConstraint,
    op: CompareOp,
    value: &Value,
) -> bool {
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

// Merge one pre-built bounded interval into the current constraint so
// STARTS_WITH can share the same compatibility checks as explicit inequalities.
pub(in crate::db::query::plan::planner::range) fn merge_range_constraint_bounds(
    existing: &mut RangeConstraint,
    candidate: &RangeConstraint,
) -> bool {
    if !merge_lower_bound(&mut existing.lower, candidate.lower.clone()) {
        return false;
    }
    if !merge_upper_bound(&mut existing.upper, candidate.upper.clone()) {
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
    let lower = match &range.lower {
        Bound::Included(value) | Bound::Excluded(value) => value,
        Bound::Unbounded => return true,
    };
    let upper = match &range.upper {
        Bound::Included(value) | Bound::Excluded(value) => value,
        Bound::Unbounded => return true,
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

pub(in crate::db::query::plan::planner::range) fn compare_range_bound_values(
    left: &Value,
    right: &Value,
) -> Option<Ordering> {
    if let Some(ordering) = compare_numeric_or_strict_order(left, right) {
        return Some(ordering);
    }

    if std::mem::discriminant(left) == std::mem::discriminant(right) {
        return Some(canonical_cmp(left, right));
    }

    None
}

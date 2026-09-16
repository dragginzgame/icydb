use crate::{
    db::{
        numeric::compare_numeric_or_strict_order,
        predicate::{CompareOp, canonical_cmp},
        query::{construction::ConstructionBudget, plan::planner::range::RangeConstraint},
    },
    error::InternalError,
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
use std::{cmp::Ordering, ops::Bound};

pub(in crate::db::query::plan::planner::range) fn merge_range_constraint(
    existing: &mut RangeConstraint,
    op: CompareOp,
    value: Value,
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    let merged = match op {
        CompareOp::Gt => merge_lower_bound(&mut existing.lower, Bound::Excluded(value), budget)?,
        CompareOp::Gte => merge_lower_bound(&mut existing.lower, Bound::Included(value), budget)?,
        CompareOp::Lt => merge_upper_bound(&mut existing.upper, Bound::Excluded(value), budget)?,
        CompareOp::Lte => merge_upper_bound(&mut existing.upper, Bound::Included(value), budget)?,
        _ => false,
    };
    if !merged {
        return Ok(false);
    }

    range_bounds_are_compatible(existing, budget)
}

// Merge one pre-built bounded interval into the current constraint so
// STARTS_WITH can share the same compatibility checks as explicit inequalities.
pub(in crate::db::query::plan::planner::range) fn merge_range_constraint_bounds(
    existing: &mut RangeConstraint,
    candidate: RangeConstraint,
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    if !merge_lower_bound(&mut existing.lower, candidate.lower, budget)? {
        return Ok(false);
    }
    if !merge_upper_bound(&mut existing.upper, candidate.upper, budget)? {
        return Ok(false);
    }

    range_bounds_are_compatible(existing, budget)
}

fn merge_lower_bound(
    existing: &mut Bound<Value>,
    candidate: Bound<Value>,
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    let replace = match (&candidate, &*existing) {
        (Bound::Unbounded, _) => false,
        (_, Bound::Unbounded) => true,
        (
            Bound::Included(left) | Bound::Excluded(left),
            Bound::Included(right) | Bound::Excluded(right),
        ) => match compare_range_bound_values(left, right, budget)? {
            Some(Ordering::Greater) => true,
            Some(Ordering::Less) => false,
            Some(Ordering::Equal) => {
                matches!(candidate, Bound::Excluded(_)) && matches!(existing, Bound::Included(_))
            }
            None => return Ok(false),
        },
    };

    if replace {
        *existing = candidate;
    }

    Ok(true)
}

fn merge_upper_bound(
    existing: &mut Bound<Value>,
    candidate: Bound<Value>,
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    let replace = match (&candidate, &*existing) {
        (Bound::Unbounded, _) => false,
        (_, Bound::Unbounded) => true,
        (
            Bound::Included(left) | Bound::Excluded(left),
            Bound::Included(right) | Bound::Excluded(right),
        ) => match compare_range_bound_values(left, right, budget)? {
            Some(Ordering::Less) => true,
            Some(Ordering::Greater) => false,
            Some(Ordering::Equal) => {
                matches!(candidate, Bound::Excluded(_)) && matches!(existing, Bound::Included(_))
            }
            None => return Ok(false),
        },
    };

    if replace {
        *existing = candidate;
    }

    Ok(true)
}

// Validate interval shape and reject empty or incomparable intervals.
fn range_bounds_are_compatible(
    range: &RangeConstraint,
    budget: &dyn ConstructionBudget,
) -> Result<bool, InternalError> {
    let lower = match &range.lower {
        Bound::Included(value) | Bound::Excluded(value) => value,
        Bound::Unbounded => return Ok(true),
    };
    let upper = match &range.upper {
        Bound::Included(value) | Bound::Excluded(value) => value,
        Bound::Unbounded => return Ok(true),
    };

    let Some(ordering) = compare_range_bound_values(lower, upper, budget)? else {
        return Ok(false);
    };

    Ok(match ordering {
        Ordering::Less => true,
        Ordering::Greater => false,
        Ordering::Equal => {
            matches!(range.lower, Bound::Included(_)) && matches!(range.upper, Bound::Included(_))
        }
    })
}

pub(in crate::db::query::plan::planner::range) fn compare_range_bound_values(
    left: &Value,
    right: &Value,
    budget: &dyn ConstructionBudget,
) -> Result<Option<Ordering>, InternalError> {
    budget.charge(Resource::PredicateExpressionSteps, 1)?;
    let same_variant = std::mem::discriminant(left) == std::mem::discriminant(right);
    if same_variant {
        budget.admit_value_comparison(left)?;
    }
    if let Some(ordering) = compare_numeric_or_strict_order(left, right) {
        return Ok(Some(ordering));
    }

    if same_variant {
        // Strict comparison can traverse a prefix before declining. Admit the
        // canonical retry separately, without changing either comparator.
        budget.admit_value_comparison(left)?;
        return Ok(Some(canonical_cmp(left, right)));
    }

    Ok(None)
}

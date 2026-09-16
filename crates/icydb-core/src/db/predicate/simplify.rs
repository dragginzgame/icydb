//! Module: predicate::simplify
//! Responsibility: conjunction-local predicate simplification over compare constraints.
//! Does not own: recursive predicate normalization or schema literal canonicalization.
//! Boundary: reusable AND-constraint simplification pass consumed by normalization.

#[cfg(test)]
mod tests;

use crate::db::predicate::{
    CompareOp, ComparePredicate, Predicate, compare_eq, compare_order, eval_ordered_compare_result,
};
use std::cmp::Ordering;

#[derive(Clone, Copy)]
enum ComparePairSimplification {
    NoChange,
    Contradiction,
    KeepFirst,
    KeepSecond,
    ReplaceFirst(CompareOp),
    ReplaceSecond(CompareOp),
}

/// Simplify conjunction-local compare predicates over the same field/coercion domain.
///
/// This pass is conservative:
/// - unsupported or incomparable pairs are preserved
/// - contradictions are detected and returned as `None`
/// - tighter/equivalent constraints are folded to one canonical compare shape
#[must_use]
pub(in crate::db::predicate) fn simplify_and_compare_constraints(
    mut predicates: Vec<Predicate>,
) -> Option<Vec<Predicate>> {
    // Earlier pairs are settled while their surviving operands are unchanged.
    // Deletion cannot make those pairs reducible; only replacement invalidates
    // that proof and requires a full restart to preserve first-pair precedence.
    let mut i = 0;
    'left: while i < predicates.len() {
        // Only Compare/Compare pairs can reduce. Keep other children in place,
        // but do not scan their suffix; removals/restarts recheck the new left.
        if !matches!(&predicates[i], Predicate::Compare(_)) {
            i += 1;
            continue;
        }
        let mut j = i.saturating_add(1);
        while j < predicates.len() {
            let simplification = match (&predicates[i], &predicates[j]) {
                (Predicate::Compare(left), Predicate::Compare(right))
                    if left.field == right.field && left.coercion == right.coercion =>
                {
                    simplify_compare_pair_for_and(left, right)
                }
                _ => ComparePairSimplification::NoChange,
            };
            match simplification {
                ComparePairSimplification::NoChange => j += 1,
                ComparePairSimplification::Contradiction => return None,
                ComparePairSimplification::KeepFirst => {
                    // The next right operand shifts into this same position.
                    predicates.remove(j);
                }
                ComparePairSimplification::KeepSecond => {
                    // Visit the new left row, without revisiting settled rows.
                    predicates.remove(i);
                    continue 'left;
                }
                ComparePairSimplification::ReplaceFirst(replacement) => {
                    // Classification proved this is a comparison. Only its
                    // operator changes; keep the owned operand and metadata.
                    if let Predicate::Compare(compare) = &mut predicates[i] {
                        compare.op = replacement;
                    }
                    predicates.remove(j);
                    i = 0;
                    continue 'left;
                }
                ComparePairSimplification::ReplaceSecond(replacement) => {
                    if let Predicate::Compare(compare) = &mut predicates[j] {
                        compare.op = replacement;
                    }
                    predicates.remove(i);
                    i = 0;
                    continue 'left;
                }
            }
        }
        i += 1;
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
    // Unsupported operators cannot simplify here; avoid coercion and comparison
    // work (including casefold allocation) whose result would be discarded.
    if !constraint.op.is_ordering_family() {
        return ComparePairSimplification::NoChange;
    }
    let Some(ordering) = compare_order(&eq.value, &constraint.value, &eq.coercion) else {
        return ComparePairSimplification::NoChange;
    };

    if !eval_ordered_compare_result(constraint.op, ordering) {
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
    let left_lower = left.op.lower_bound_inclusive();
    let right_lower = right.op.lower_bound_inclusive();
    let left_upper = left.op.upper_bound_inclusive();
    let right_upper = right.op.upper_bound_inclusive();

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
    let Some(left_inclusive) = left.op.lower_bound_inclusive() else {
        return ComparePairSimplification::NoChange;
    };
    let Some(right_inclusive) = right.op.lower_bound_inclusive() else {
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
    let Some(left_inclusive) = left.op.upper_bound_inclusive() else {
        return ComparePairSimplification::NoChange;
    };
    let Some(right_inclusive) = right.op.upper_bound_inclusive() else {
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
    let Some(lower_inclusive) = lower.op.lower_bound_inclusive() else {
        return ComparePairSimplification::NoChange;
    };
    let Some(upper_inclusive) = upper.op.upper_bound_inclusive() else {
        return ComparePairSimplification::NoChange;
    };

    match ordering {
        Ordering::Less => ComparePairSimplification::NoChange,
        Ordering::Greater => ComparePairSimplification::Contradiction,
        Ordering::Equal => {
            if lower_inclusive && upper_inclusive {
                ComparePairSimplification::ReplaceFirst(CompareOp::Eq)
            } else {
                ComparePairSimplification::Contradiction
            }
        }
    }
}

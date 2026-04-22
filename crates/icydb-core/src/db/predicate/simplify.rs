//! Module: predicate::simplify
//! Responsibility: conjunction-local predicate simplification over compare constraints.
//! Does not own: recursive predicate normalization or schema literal canonicalization.
//! Boundary: reusable AND-constraint simplification pass consumed by normalization.

use crate::db::predicate::{CompareOp, ComparePredicate, Predicate, compare_eq, compare_order};
use std::cmp::Ordering;

#[derive(Clone)]
enum ComparePairSimplification {
    NoChange,
    Contradiction,
    KeepFirst,
    KeepSecond,
    ReplaceFirst(ComparePredicate),
    ReplaceSecond(ComparePredicate),
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

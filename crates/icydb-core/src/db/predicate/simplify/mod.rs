//! Module: predicate::simplify
//! Responsibility: conjunction-local predicate simplification over compare constraints.
//! Does not own: recursive predicate normalization or schema literal canonicalization.
//! Boundary: reusable AND-constraint simplification pass consumed by normalization.

#[cfg(test)]
mod tests;

use crate::{
    db::{
        predicate::{
            CoercionId, CompareOp, ComparePredicate, Predicate, compare_eq, compare_order,
            eval_ordered_compare_result,
        },
        resource::construction::admit_value_comparison,
    },
    value::{Value, lower_text_construction_allowance},
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
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
pub(in crate::db::predicate) fn simplify_and_compare_constraints<E>(
    mut predicates: Vec<Predicate>,
    charge: &mut impl FnMut(Resource, u64) -> Result<(), E>,
) -> Result<Option<Vec<Predicate>>, E> {
    // Earlier pairs are settled while their surviving operands are unchanged.
    // Deletion cannot make those pairs reducible. After replacement, revisit
    // only pairs involving the changed operand within the settled prefix, then
    // resume at its frontier. This retains first-pair precedence without scanning
    // every unchanged prefix pair again for each equal-bound collapse.
    let mut revisit = None;
    let mut i = 0;
    'left: while i < predicates.len() {
        if revisit.is_some_and(|(frontier, _)| i >= frontier) {
            revisit = None;
        }
        // Only Compare/Compare pairs can reduce. Keep other children in place,
        // but do not scan their suffix; removals/restarts recheck the new left.
        if !matches!(&predicates[i], Predicate::Compare(_)) {
            // Replacements can revisit this prefix many times. Each skipped
            // shell needs admission even though it performs no pair comparison.
            charge(Resource::PredicateExpressionSteps, 1)?;
            i += 1;
            continue;
        }
        let mut j = revisit.map_or_else(|| i.saturating_add(1), |(_, changed)| changed);
        while j < predicates.len() {
            charge(Resource::PredicateExpressionSteps, 1)?;
            let simplification = match (&predicates[i], &predicates[j]) {
                (Predicate::Compare(left), Predicate::Compare(right)) => {
                    // Admit the borrowed label before checking the pair's domain.
                    charge(Resource::PredicateExpressionSteps, left.field.len() as u64)?;
                    if left.field == right.field && left.coercion == right.coercion {
                        simplify_compare_pair_for_and(left, right, charge)?
                    } else {
                        ComparePairSimplification::NoChange
                    }
                }
                _ => ComparePairSimplification::NoChange,
            };
            match simplification {
                ComparePairSimplification::NoChange => {
                    if revisit.is_some() {
                        break;
                    }
                    j += 1;
                }
                ComparePairSimplification::Contradiction => return Ok(None),
                ComparePairSimplification::KeepFirst => {
                    // The next right operand shifts into this same position.
                    remove_predicate(&mut predicates, j, charge)?;
                    if let Some((frontier, _)) = revisit.take() {
                        // The changed operand is gone; the original prefix
                        // proof holds again, including pairs with its survivor.
                        i = frontier;
                        continue 'left;
                    }
                }
                ComparePairSimplification::KeepSecond => {
                    // Visit the new left row, without revisiting settled rows.
                    remove_predicate(&mut predicates, i, charge)?;
                    revisit = revisit.map(|(frontier, changed)| (frontier - 1, changed - 1));
                    continue 'left;
                }
                ComparePairSimplification::ReplaceFirst(replacement) => {
                    // Admit the shift before changing either operand. Since
                    // j > i, removing j leaves the retained position unchanged.
                    remove_predicate(&mut predicates, j, charge)?;
                    if let Predicate::Compare(compare) = &mut predicates[i] {
                        compare.op = replacement;
                    }
                    revisit = Some((i, i));
                    i = 0;
                    continue 'left;
                }
                ComparePairSimplification::ReplaceSecond(replacement) => {
                    // Removing the earlier operand moves the retained one to
                    // j - 1. Admission precedes both removal and replacement.
                    remove_predicate(&mut predicates, i, charge)?;
                    if let Predicate::Compare(compare) = &mut predicates[j - 1] {
                        compare.op = replacement;
                    }
                    revisit = Some((i, j - 1));
                    i = 0;
                    continue 'left;
                }
            }
        }
        i += 1;
    }

    Ok(Some(predicates))
}

// Callers supply a visited index. Stable removal moves each following fixed-size
// predicate shell once; payloads stay owned and are neither copied nor compared.
fn remove_predicate<E>(
    predicates: &mut Vec<Predicate>,
    index: usize,
    charge: &mut impl FnMut(Resource, u64) -> Result<(), E>,
) -> Result<(), E> {
    let shifted = predicates.len() - index - 1;
    charge(Resource::PredicateExpressionSteps, shifted as u64)?;
    predicates.remove(index);
    Ok(())
}

// Simplify one pair of compare predicates in an AND clause.
fn simplify_compare_pair_for_and<E>(
    left: &ComparePredicate,
    right: &ComparePredicate,
    charge: &mut impl FnMut(Resource, u64) -> Result<(), E>,
) -> Result<ComparePairSimplification, E> {
    Ok(match (left.op, right.op) {
        (CompareOp::Eq, CompareOp::Eq) => simplify_eq_eq_pair(left, right, charge)?,
        (CompareOp::Eq, _) => simplify_eq_with_constraint_pair(left, right, true, charge)?,
        (_, CompareOp::Eq) => simplify_eq_with_constraint_pair(right, left, false, charge)?,
        _ => simplify_constraint_constraint_pair(left, right, charge)?,
    })
}

// Simplify `field = a AND field = b`.
fn simplify_eq_eq_pair<E>(
    left: &ComparePredicate,
    right: &ComparePredicate,
    charge: &mut impl FnMut(Resource, u64) -> Result<(), E>,
) -> Result<ComparePairSimplification, E> {
    admit_compare_values(left, right, charge)?;
    Ok(
        match compare_eq(&left.value, &right.value, &left.coercion) {
            Some(true) => ComparePairSimplification::KeepFirst,
            Some(false) => ComparePairSimplification::Contradiction,
            None => ComparePairSimplification::NoChange,
        },
    )
}

// Simplify `field = a AND field <op> b` where `<op>` is one inequality bound.
//
// `eq_is_first` indicates whether `eq` is the left/first pair item.
fn simplify_eq_with_constraint_pair<E>(
    eq: &ComparePredicate,
    constraint: &ComparePredicate,
    eq_is_first: bool,
    charge: &mut impl FnMut(Resource, u64) -> Result<(), E>,
) -> Result<ComparePairSimplification, E> {
    // Unsupported operators cannot simplify here; avoid coercion and comparison
    // work (including casefold allocation) whose result would be discarded.
    if !constraint.op.is_ordering_family() {
        return Ok(ComparePairSimplification::NoChange);
    }
    admit_compare_values(eq, constraint, charge)?;
    let Some(ordering) = compare_order(&eq.value, &constraint.value, &eq.coercion) else {
        return Ok(ComparePairSimplification::NoChange);
    };

    if !eval_ordered_compare_result(constraint.op, ordering) {
        return Ok(ComparePairSimplification::Contradiction);
    }
    Ok(if eq_is_first {
        ComparePairSimplification::KeepFirst
    } else {
        ComparePairSimplification::KeepSecond
    })
}

// Simplify inequality-pair combinations in conjunctions:
// - tighter lower-bound retention (`>`, `>=`)
// - tighter upper-bound retention (`<`, `<=`)
// - lower/upper contradiction detection
// - lower/upper equality collapse (`>= a AND <= a -> = a`)
fn simplify_constraint_constraint_pair<E>(
    left: &ComparePredicate,
    right: &ComparePredicate,
    charge: &mut impl FnMut(Resource, u64) -> Result<(), E>,
) -> Result<ComparePairSimplification, E> {
    let left_lower = left.op.lower_bound_inclusive();
    let right_lower = right.op.lower_bound_inclusive();
    let left_upper = left.op.upper_bound_inclusive();
    let right_upper = right.op.upper_bound_inclusive();

    if let (Some(left_inclusive), Some(right_inclusive)) = (left_lower, right_lower) {
        return simplify_same_direction_bounds(
            left,
            right,
            left_inclusive,
            right_inclusive,
            Ordering::Greater,
            charge,
        );
    }
    if let (Some(left_inclusive), Some(right_inclusive)) = (left_upper, right_upper) {
        return simplify_same_direction_bounds(
            left,
            right,
            left_inclusive,
            right_inclusive,
            Ordering::Less,
            charge,
        );
    }
    if left_lower.is_some() && right_upper.is_some() {
        return simplify_lower_upper_pair(left, right, charge);
    }
    if left_upper.is_some() && right_lower.is_some() {
        return Ok(match simplify_lower_upper_pair(right, left, charge)? {
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
        });
    }

    Ok(ComparePairSimplification::NoChange)
}

// Greater lower bounds and lesser upper bounds are tighter. Equal endpoints
// retain the first operand unless only the second is strict; charge/compare once.
fn simplify_same_direction_bounds<E>(
    left: &ComparePredicate,
    right: &ComparePredicate,
    left_inclusive: bool,
    right_inclusive: bool,
    tighter_ordering: Ordering,
    charge: &mut impl FnMut(Resource, u64) -> Result<(), E>,
) -> Result<ComparePairSimplification, E> {
    admit_compare_values(left, right, charge)?;
    let Some(ordering) = compare_order(&left.value, &right.value, &left.coercion) else {
        return Ok(ComparePairSimplification::NoChange);
    };

    let keep_first = match ordering {
        Ordering::Equal => !left_inclusive || right_inclusive,
        ordering => ordering == tighter_ordering,
    };
    Ok(if keep_first {
        ComparePairSimplification::KeepFirst
    } else {
        ComparePairSimplification::KeepSecond
    })
}

// Simplify `lower AND upper`, where `lower` is one of (`>`,`>=`) and `upper`
// is one of (`<`,`<=`).
fn simplify_lower_upper_pair<E>(
    lower: &ComparePredicate,
    upper: &ComparePredicate,
    charge: &mut impl FnMut(Resource, u64) -> Result<(), E>,
) -> Result<ComparePairSimplification, E> {
    admit_compare_values(lower, upper, charge)?;
    let Some(ordering) = compare_order(&lower.value, &upper.value, &lower.coercion) else {
        return Ok(ComparePairSimplification::NoChange);
    };
    let Some(lower_inclusive) = lower.op.lower_bound_inclusive() else {
        return Ok(ComparePairSimplification::NoChange);
    };
    let Some(upper_inclusive) = upper.op.upper_bound_inclusive() else {
        return Ok(ComparePairSimplification::NoChange);
    };

    Ok(match ordering {
        Ordering::Less => ComparePairSimplification::NoChange,
        Ordering::Greater => ComparePairSimplification::Contradiction,
        Ordering::Equal => {
            if lower_inclusive && upper_inclusive {
                ComparePairSimplification::ReplaceFirst(CompareOp::Eq)
            } else {
                ComparePairSimplification::Contradiction
            }
        }
    })
}

// Structural comparisons inspect at most the admitted left tree. Casefolding
// additionally constructs both text operands; use the transform owner's bound
// before either allocation and admit the maximum folded comparison extent.
fn admit_compare_values<E>(
    left: &ComparePredicate,
    right: &ComparePredicate,
    charge: &mut impl FnMut(Resource, u64) -> Result<(), E>,
) -> Result<(), E> {
    if left.coercion.id == CoercionId::TextCasefold {
        for value in [&left.value, &right.value] {
            let Value::Text(text) = value else {
                return Ok(());
            };
            let (backing, steps) = lower_text_construction_allowance(text.len());
            charge(Resource::TemporaryBytes, backing)?;
            charge(Resource::PredicateExpressionSteps, steps)?;
        }
        if let Value::Text(text) = &left.value {
            charge(
                Resource::PredicateExpressionSteps,
                (text.len() as u64).saturating_mul(2),
            )?;
        }
        return Ok(());
    }
    admit_value_comparison(&left.value, charge)
}

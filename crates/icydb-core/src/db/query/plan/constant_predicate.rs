//! Module: query::plan::constant_predicate
//! Responsibility: planner-owned constant predicate folding helpers.
//! Does not own: predicate normalization or access-path routing.
//! Boundary: folds canonical TRUE/FALSE predicates before access planning.

use crate::db::predicate::Predicate;

/// Fold canonical constant predicates before access routing.
///
/// Contract:
/// - `Some(Predicate::True)` is elided to `None`
/// - `Some(Predicate::False)` is preserved so explain semantics remain explicit
/// - all other predicates are passed through unchanged
#[must_use]
pub(in crate::db::query) fn fold_constant_predicate(
    predicate: Option<Predicate>,
) -> Option<Predicate> {
    match predicate {
        Some(Predicate::True) => None,
        other => other,
    }
}

/// Return true when the normalized predicate is a canonical constant false.
#[must_use]
pub(in crate::db::query) const fn predicate_is_constant_false(
    predicate: Option<&Predicate>,
) -> bool {
    matches!(predicate, Some(Predicate::False))
}

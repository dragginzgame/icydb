//! Module: contracts
//! Responsibility: shared db-level semantic contracts used across subsystems.
//! Does not own: predicate runtime/validation semantics (moved to `db::predicate`).
//! Boundary: retains only non-predicate helpers.

mod semantics;
#[cfg(test)]
mod tests;

pub(in crate::db) use semantics::canonical_value_compare;

/// Return the first violated rule error in rule declaration order.
pub(in crate::db) fn first_violated_rule<R, C, E>(rules: &[R], ctx: C) -> Option<E>
where
    C: Copy,
    R: Fn(C) -> Option<E>,
{
    for rule in rules {
        if let Some(err) = rule(ctx) {
            return Some(err);
        }
    }

    None
}

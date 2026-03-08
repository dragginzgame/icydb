//! Module: contracts::rules
//! Responsibility: shared first-violation rule evaluation helper.
//! Does not own: domain-specific rule definitions.
//! Boundary: generic ordered rule traversal used by validation/planner modules.

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

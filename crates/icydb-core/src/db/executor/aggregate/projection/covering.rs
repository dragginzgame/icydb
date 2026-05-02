//! Module: db::executor::aggregate::projection::covering
//! Defines covering-index helpers used by aggregate projection execution.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::{
    access::AccessPlan,
    query::plan::{
        CoveringProjectionContext, OrderSpec,
        covering_index_adjacent_distinct_eligible as plan_adjacent,
        covering_index_projection_context as plan_covering_context,
    },
};

// Derive one planner-owned covering projection context from executor plan
// contracts without duplicating order-shape interpretation in executor code.
pub(super) fn covering_index_projection_context<K>(
    access: &AccessPlan<K>,
    order: Option<&OrderSpec>,
    target_field: &str,
    primary_key_name: &'static str,
) -> Option<CoveringProjectionContext> {
    plan_covering_context(access, order, target_field, primary_key_name)
}

// Return whether adjacent dedupe is safe for one covering context.
pub(super) const fn covering_index_adjacent_distinct_eligible(
    context: CoveringProjectionContext,
) -> bool {
    plan_adjacent(context)
}

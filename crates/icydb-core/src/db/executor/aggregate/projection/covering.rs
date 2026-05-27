//! Module: db::executor::aggregate::projection::covering
//! Defines covering-index helpers used by aggregate projection execution.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::{
    access::AccessPlan,
    query::plan::{
        CoveringProjectionFacts, OrderSpec,
        covering_index_adjacent_distinct_eligible as plan_adjacent,
        covering_index_projection_facts_with_primary_key_names as plan_covering_facts,
    },
};

// Derive one planner-owned covering projection fact bundle from executor plan
// contracts without duplicating order-shape interpretation in executor code.
pub(super) fn covering_index_projection_facts<K>(
    access: &AccessPlan<K>,
    order: Option<&OrderSpec>,
    target_field: &str,
    primary_key_names: &[&str],
) -> Option<CoveringProjectionFacts> {
    plan_covering_facts(access, order, target_field, primary_key_names)
}

// Return whether adjacent dedupe is safe for one covering fact bundle.
pub(super) const fn covering_index_adjacent_distinct_eligible(
    facts: CoveringProjectionFacts,
) -> bool {
    plan_adjacent(facts)
}

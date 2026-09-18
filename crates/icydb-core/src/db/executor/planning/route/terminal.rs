//! Module: executor::planning::route::terminal
//! Responsibility: route-owned terminal fast-path contracts.
//! Does not own: terminal execution mechanics.
//! Boundary: canonical terminal eligibility derivation consumed by load/aggregate terminals.

use crate::db::{
    executor::{
        EntityAuthority, planning::preparation::covering_strict_predicate_compatible_for_plan,
    },
    query::plan::{AccessPlannedQuery, CoveringReadExecutionPlan},
};

/// Derive one route-owned scalar load terminal fast-path contract directly from
/// accepted entity authority and the planner-owned covering-read contract.
pub(in crate::db::executor) fn derive_load_terminal_fast_path_contract_for_plan(
    authority: &EntityAuthority,
    plan: &AccessPlannedQuery,
) -> Option<CoveringReadExecutionPlan> {
    if !plan.scalar_plan().mode.is_load() {
        return None;
    }

    let strict_predicate_compatible = covering_strict_predicate_compatible_for_plan(plan).ok()?;

    authority.covering_read_execution_plan(plan, strict_predicate_compatible)
}

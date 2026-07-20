//! Module: executor::planning::route::terminal
//! Responsibility: route-owned terminal fast-path contracts.
//! Does not own: terminal execution mechanics.
//! Boundary: canonical terminal eligibility derivation consumed by load/aggregate terminals.

use crate::db::{
    access::ExecutableAccessPlan,
    direction::Direction,
    executor::{
        EntityAuthority,
        planning::preparation::covering_strict_predicate_compatible_for_plan,
        route::{
            direct_primary_key_lookup_shape_supported, primary_key_stream_window_shape_supported,
        },
    },
    query::plan::{
        AccessPlannedQuery, CoveringReadExecutionPlan,
        index_covering_existing_rows_terminal_eligible,
    },
};
use crate::value::Value;

///
/// BytesTerminalFastPathContract
///
/// Route-owned `bytes()` fast-path contract.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum BytesTerminalFastPathContract {
    PrimaryKeyWindow(Direction),
    OrderedKeyStreamWindow(Direction),
}

///
/// CountTerminalFastPathContract
///
/// Route-owned `count()` fast-path contract.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum CountTerminalFastPathContract {
    PrimaryKeyCardinality,
    PrimaryKeyExistingRows(Direction),
    IndexCoveringExistingRows(Direction),
}

/// Derive one route-owned `count()` terminal fast-path contract from structural plan state.
///
/// Secondary index-covered COUNT still uses existing-row checks so stale
/// secondary keys preserve materialized-count parity and strict missing-row
/// corruption diagnostics.
pub(in crate::db::executor) fn derive_count_terminal_fast_path_contract_for_model(
    plan: &AccessPlannedQuery,
    executable_access: &ExecutableAccessPlan<'_, Value>,
    strict_predicate_compatible: bool,
) -> Option<CountTerminalFastPathContract> {
    let shape_facts = executable_access.shape_facts().single_path_facts()?;

    (plan.has_no_distinct()
        && !plan.has_any_residual_filter()
        && primary_key_stream_window_shape_supported(&shape_facts))
    .then_some(CountTerminalFastPathContract::PrimaryKeyCardinality)
    .or_else(|| {
        let direction = plan.unordered_or_primary_key_order_direction()?;
        (!plan.has_any_residual_filter() && direct_primary_key_lookup_shape_supported(&shape_facts))
            .then_some(CountTerminalFastPathContract::PrimaryKeyExistingRows(
                direction,
            ))
    })
    .or_else(|| {
        index_covering_existing_rows_terminal_eligible(plan, strict_predicate_compatible).then_some(
            CountTerminalFastPathContract::IndexCoveringExistingRows(Direction::Asc),
        )
    })
}

/// Derive one route-owned `exists()` terminal fast-path contract from structural plan state.
///
/// `EXISTS` result sensitivity under missing rows is not the same invariant as
/// probe-free covering window stability, so it must be classified separately.
pub(in crate::db::executor) fn derive_exists_terminal_fast_path_direction_for_model(
    plan: &AccessPlannedQuery,
    strict_predicate_compatible: bool,
) -> Option<Direction> {
    index_covering_existing_rows_terminal_eligible(plan, strict_predicate_compatible)
        .then_some(Direction::Asc)
}

/// Derive one route-owned scalar load terminal fast-path contract from the
/// planner-owned covering-read contract.
pub(in crate::db::executor) fn derive_load_terminal_fast_path_contract(
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
    strict_predicate_compatible: bool,
) -> Option<CoveringReadExecutionPlan> {
    authority.covering_read_execution_plan(plan, strict_predicate_compatible)
}

/// Derive one route-owned scalar load terminal fast-path contract directly from
/// one structural model + plan boundary.
pub(in crate::db::executor) fn derive_load_terminal_fast_path_contract_for_plan(
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
) -> Option<CoveringReadExecutionPlan> {
    if !plan.scalar_plan().mode.is_load() {
        return None;
    }

    let strict_predicate_compatible = covering_strict_predicate_compatible_for_plan(plan);

    derive_load_terminal_fast_path_contract(authority, plan, strict_predicate_compatible)
}

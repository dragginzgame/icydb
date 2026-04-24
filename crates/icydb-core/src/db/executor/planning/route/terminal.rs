//! Module: db::executor::planning::route::terminal
//! Responsibility: route-owned terminal fast-path contracts.
//! Does not own: terminal execution mechanics.
//! Boundary: canonical terminal eligibility derivation consumed by load/aggregate terminals.

use crate::db::{
    access::LoweredAccess,
    direction::Direction,
    executor::{
        EntityAuthority, ExecutionPreparation, planning::preparation::slot_map_for_model_plan,
    },
    query::plan::{
        AccessPlannedQuery, CoveringReadExecutionPlan, covering_strict_predicate_compatible,
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

///
/// ExistsTerminalFastPathContract
///
/// Route-owned `exists()` fast-path contract.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum ExistsTerminalFastPathContract {
    IndexCoveringExistingRows(Direction),
}

///
/// LoadTerminalFastPathContract
///
/// Route-owned scalar load terminal fast-path contract.
/// This keeps planner-selected covering-read eligibility explicit so EXPLAIN
/// and later runtime consumers do not rediscover it ad hoc.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum LoadTerminalFastPathContract {
    CoveringRead(CoveringReadExecutionPlan),
}

/// Derive one route-owned `count()` terminal fast-path contract from structural plan state.
///
/// Aggregate existing-row shortcuts are still a separate correctness problem
/// from secondary covering. Planner-owned index visibility now owns lifecycle
/// correctness globally, but COUNT/EXISTS missing-row sensitivity still needs
/// its own classification before those terminals are simplified further.
pub(in crate::db::executor) fn derive_count_terminal_fast_path_contract_for_model(
    plan: &AccessPlannedQuery,
    lowered_access: &LoweredAccess<'_, Value>,
    strict_predicate_compatible: bool,
) -> Option<CountTerminalFastPathContract> {
    let capabilities = lowered_access
        .executable()
        .capabilities()
        .single_path_capabilities()?;

    (plan.has_no_distinct()
        && !plan.has_any_residual_filter()
        && capabilities.primary_key_cardinality().is_some())
    .then_some(CountTerminalFastPathContract::PrimaryKeyCardinality)
    .or_else(|| {
        let direction = plan.unordered_or_primary_key_order_direction()?;
        (!plan.has_any_residual_filter() && capabilities.has_direct_primary_key_lookup()).then_some(
            CountTerminalFastPathContract::PrimaryKeyExistingRows(direction),
        )
    })
    .or_else(|| {
        index_covering_existing_rows_terminal_eligible(plan, strict_predicate_compatible).then_some(
            CountTerminalFastPathContract::IndexCoveringExistingRows(Direction::Asc),
        )
    })
}

/// Derive one route-owned `exists()` terminal fast-path contract from structural plan state.
///
/// `0.70` intentionally leaves this path outside the new index-validity gate.
/// `EXISTS` result sensitivity under missing rows is not the same invariant as
/// probe-free covering window stability, so it must be classified separately.
pub(in crate::db::executor) fn derive_exists_terminal_fast_path_contract_for_model(
    plan: &AccessPlannedQuery,
    strict_predicate_compatible: bool,
) -> Option<ExistsTerminalFastPathContract> {
    index_covering_existing_rows_terminal_eligible(plan, strict_predicate_compatible).then_some(
        ExistsTerminalFastPathContract::IndexCoveringExistingRows(Direction::Asc),
    )
}

/// Derive one route-owned scalar load terminal fast-path contract from the
/// planner-owned covering-read contract.
pub(in crate::db::executor) fn derive_load_terminal_fast_path_contract(
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
    strict_predicate_compatible: bool,
) -> Option<LoadTerminalFastPathContract> {
    authority
        .covering_read_execution_plan(plan, strict_predicate_compatible)
        .map(LoadTerminalFastPathContract::CoveringRead)
}

/// Derive one route-owned scalar load terminal fast-path contract directly from
/// one structural model + plan boundary.
pub(in crate::db::executor) fn derive_load_terminal_fast_path_contract_for_plan(
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
) -> Option<LoadTerminalFastPathContract> {
    if !plan.scalar_plan().mode.is_load() {
        return None;
    }

    let execution_preparation =
        ExecutionPreparation::from_covering_route_plan(plan, slot_map_for_model_plan(plan));
    let strict_predicate_compatible = covering_strict_predicate_compatible(
        plan,
        execution_preparation
            .predicate_capability_profile()
            .map(crate::db::predicate::PredicateCapabilityProfile::index),
    );

    derive_load_terminal_fast_path_contract(authority, plan, strict_predicate_compatible)
}

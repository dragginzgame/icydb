//! Module: executor::route::terminal
//! Responsibility: route-owned terminal fast-path contracts.
//! Does not own: terminal execution mechanics.
//! Boundary: canonical terminal eligibility derivation consumed by load/aggregate terminals.

use crate::{
    db::{
        access::single_path_capabilities,
        direction::Direction,
        query::plan::{AccessPlannedQuery, index_covering_existing_rows_terminal_eligible},
    },
    model::entity::EntityModel,
};

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

// Return whether the structural plan still carries a residual predicate.
const fn plan_has_predicate(plan: &AccessPlannedQuery) -> bool {
    plan.scalar_plan().predicate.is_some()
}

// Return whether the structural plan clears both residual-predicate and DISTINCT gates.
const fn plan_has_no_predicate_or_distinct(plan: &AccessPlannedQuery) -> bool {
    !plan_has_predicate(plan) && !plan.scalar_plan().distinct
}

// Return one canonical scan direction for unordered plans or primary-key-only ordering.
fn unordered_or_primary_key_order_direction_for_model(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
) -> Option<Direction> {
    let Some(order) = plan.scalar_plan().order.as_ref() else {
        return Some(Direction::Asc);
    };

    order
        .primary_key_only_direction(model.primary_key().name)
        .map(|direction| match direction {
            crate::db::query::plan::OrderDirection::Asc => Direction::Asc,
            crate::db::query::plan::OrderDirection::Desc => Direction::Desc,
        })
}

/// Derive one route-owned `count()` terminal fast-path contract from structural plan state.
pub(in crate::db::executor) fn derive_count_terminal_fast_path_contract_for_model(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
    strict_predicate_compatible: bool,
) -> Option<CountTerminalFastPathContract> {
    let access_strategy = plan.access.resolve_strategy();
    let capabilities = access_strategy.as_path().map(single_path_capabilities)?;

    (plan_has_no_predicate_or_distinct(plan)
        && capabilities.supports_count_terminal_primary_key_cardinality())
    .then_some(CountTerminalFastPathContract::PrimaryKeyCardinality)
    .or_else(|| {
        let direction = unordered_or_primary_key_order_direction_for_model(model, plan)?;
        (!plan_has_predicate(plan)
            && capabilities.supports_count_terminal_primary_key_existing_rows())
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
pub(in crate::db::executor) fn derive_exists_terminal_fast_path_contract_for_model(
    plan: &AccessPlannedQuery,
    strict_predicate_compatible: bool,
) -> Option<ExistsTerminalFastPathContract> {
    index_covering_existing_rows_terminal_eligible(plan, strict_predicate_compatible).then_some(
        ExistsTerminalFastPathContract::IndexCoveringExistingRows(Direction::Asc),
    )
}

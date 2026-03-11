//! Module: executor::route::terminal
//! Responsibility: route-owned terminal fast-path contracts.
//! Does not own: terminal execution mechanics.
//! Boundary: canonical terminal eligibility derivation consumed by load/aggregate terminals.

use crate::{
    db::{
        access::single_path_capabilities,
        direction::Direction,
        executor::{ExecutablePlan, pipeline::contracts::LoadExecutor},
        query::plan::OrderDirection,
    },
    traits::{EntityKind, EntityValue},
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

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Derive one route-owned `bytes()` terminal fast-path contract.
    pub(in crate::db::executor) fn derive_bytes_terminal_fast_path_contract(
        plan: &ExecutablePlan<E>,
    ) -> Option<BytesTerminalFastPathContract> {
        (!plan.has_predicate() && !plan.is_distinct()).then_some(())?;

        let direction = bytes_terminal_direction_from_order::<E>(plan)?;
        let access_strategy = plan.access().resolve_strategy();
        let capabilities = access_strategy.as_path().map(single_path_capabilities)?;

        capabilities
            .supports_bytes_terminal_primary_key_window()
            .then_some(BytesTerminalFastPathContract::PrimaryKeyWindow(direction))
            .or_else(|| {
                capabilities
                    .supports_bytes_terminal_ordered_key_stream_window()
                    .then_some(BytesTerminalFastPathContract::OrderedKeyStreamWindow(
                        direction,
                    ))
            })
    }

    // Derive one route-owned `count()` terminal fast-path contract.
    pub(in crate::db::executor) fn derive_count_terminal_fast_path_contract(
        plan: &ExecutablePlan<E>,
    ) -> Option<CountTerminalFastPathContract> {
        let access_strategy = plan.access().resolve_strategy();
        let capabilities = access_strategy.as_path().map(single_path_capabilities)?;

        (!plan.is_distinct()
            && !plan.has_predicate()
            && capabilities.supports_count_terminal_primary_key_cardinality())
        .then_some(CountTerminalFastPathContract::PrimaryKeyCardinality)
        .or_else(|| {
            (!plan.has_predicate()
                && primary_key_count_order_supported_for_terminal::<E>(plan)
                && capabilities.supports_count_terminal_primary_key_existing_rows())
            .then_some(CountTerminalFastPathContract::PrimaryKeyExistingRows(
                count_stream_direction_for_terminal::<E>(plan),
            ))
        })
        .or_else(|| {
            plan.index_covering_existing_rows_terminal_eligible()
                .then_some(CountTerminalFastPathContract::IndexCoveringExistingRows(
                    Direction::Asc,
                ))
        })
    }

    // Derive one route-owned `exists()` terminal fast-path contract.
    pub(in crate::db::executor) fn derive_exists_terminal_fast_path_contract(
        plan: &ExecutablePlan<E>,
    ) -> Option<ExistsTerminalFastPathContract> {
        plan.index_covering_existing_rows_terminal_eligible()
            .then_some(ExistsTerminalFastPathContract::IndexCoveringExistingRows(
                Direction::Asc,
            ))
    }
}

// Map the planner ORDER BY contract to one direction for bytes terminal routing.
fn bytes_terminal_direction_from_order<E>(plan: &ExecutablePlan<E>) -> Option<Direction>
where
    E: EntityKind,
{
    let Some(order) = plan.order_spec() else {
        return Some(Direction::Asc);
    };
    match order.fields.as_slice() {
        [(field, order_direction)] if field == E::MODEL.primary_key.name => {
            Some(direction_from_order(*order_direction))
        }
        _ => None,
    }
}

// Keep primary-key direct COUNT fast path scoped to unordered or PK-only ordering contracts.
fn primary_key_count_order_supported_for_terminal<E>(plan: &ExecutablePlan<E>) -> bool
where
    E: EntityKind,
{
    let Some(order) = plan.order_spec() else {
        return true;
    };
    matches!(
        order.fields.as_slice(),
        [(field, _)] if field == E::MODEL.primary_key.name
    )
}

// Derive COUNT key-stream traversal direction from the PK-only order contract.
fn count_stream_direction_for_terminal<E>(plan: &ExecutablePlan<E>) -> Direction
where
    E: EntityKind,
{
    let Some(order) = plan.order_spec() else {
        return Direction::Asc;
    };
    match order.fields.as_slice() {
        [(field, order_direction)] if field == E::MODEL.primary_key.name => {
            direction_from_order(*order_direction)
        }
        _ => Direction::Asc,
    }
}

// Convert one ORDER BY direction contract into execution traversal direction.
const fn direction_from_order(order_direction: OrderDirection) -> Direction {
    match order_direction {
        OrderDirection::Asc => Direction::Asc,
        OrderDirection::Desc => Direction::Desc,
    }
}

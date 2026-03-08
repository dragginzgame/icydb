//! Module: executor::route::terminal
//! Responsibility: route-owned terminal fast-path contracts.
//! Does not own: terminal execution mechanics.
//! Boundary: canonical terminal eligibility derivation consumed by load/aggregate terminals.

use crate::{
    db::{
        access::single_path_capabilities,
        direction::Direction,
        executor::{ExecutablePlan, load::LoadExecutor},
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
        if plan.has_predicate() || plan.is_distinct() {
            return None;
        }

        let direction = bytes_terminal_direction_from_order::<E>(plan)?;
        let access_strategy = plan.access().resolve_strategy();
        let capabilities = access_strategy.as_path().map(single_path_capabilities)?;

        if capabilities.supports_bytes_terminal_primary_key_window() {
            return Some(BytesTerminalFastPathContract::PrimaryKeyWindow(direction));
        }
        if capabilities.supports_bytes_terminal_ordered_key_stream_window() {
            return Some(BytesTerminalFastPathContract::OrderedKeyStreamWindow(
                direction,
            ));
        }

        None
    }

    // Derive one route-owned `count()` terminal fast-path contract.
    pub(in crate::db::executor) fn derive_count_terminal_fast_path_contract(
        plan: &ExecutablePlan<E>,
    ) -> Option<CountTerminalFastPathContract> {
        let access_strategy = plan.access().resolve_strategy();
        let capabilities = access_strategy.as_path().map(single_path_capabilities)?;

        if !plan.is_distinct()
            && !plan.has_predicate()
            && capabilities.supports_count_terminal_primary_key_cardinality()
        {
            return Some(CountTerminalFastPathContract::PrimaryKeyCardinality);
        }

        if !plan.has_predicate()
            && primary_key_count_order_supported_for_terminal::<E>(plan)
            && capabilities.supports_count_terminal_primary_key_existing_rows()
        {
            return Some(CountTerminalFastPathContract::PrimaryKeyExistingRows(
                count_stream_direction_for_terminal::<E>(plan),
            ));
        }

        if plan.order_spec().is_some() {
            return None;
        }
        if !capabilities.supports_index_covering_existing_rows_terminal() {
            return None;
        }
        if !plan.has_predicate() || plan.execution_preparation().strict_mode().is_some() {
            return Some(CountTerminalFastPathContract::IndexCoveringExistingRows(
                Direction::Asc,
            ));
        }

        None
    }

    // Derive one route-owned `exists()` terminal fast-path contract.
    pub(in crate::db::executor) fn derive_exists_terminal_fast_path_contract(
        plan: &ExecutablePlan<E>,
    ) -> Option<ExistsTerminalFastPathContract> {
        if plan.order_spec().is_some() {
            return None;
        }

        let access_strategy = plan.access().resolve_strategy();
        let capabilities = access_strategy.as_path().map(single_path_capabilities)?;
        if !capabilities.supports_index_covering_existing_rows_terminal() {
            return None;
        }
        if !plan.has_predicate() || plan.execution_preparation().strict_mode().is_some() {
            return Some(ExistsTerminalFastPathContract::IndexCoveringExistingRows(
                Direction::Asc,
            ));
        }

        None
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
    if order.fields.len() != 1 {
        return None;
    }
    let (field, order_direction) = &order.fields[0];
    if field != E::MODEL.primary_key.name {
        return None;
    }

    Some(direction_from_order(*order_direction))
}

// Keep primary-key direct COUNT fast path scoped to unordered or PK-only ordering contracts.
fn primary_key_count_order_supported_for_terminal<E>(plan: &ExecutablePlan<E>) -> bool
where
    E: EntityKind,
{
    let Some(order) = plan.order_spec() else {
        return true;
    };
    if order.fields.len() != 1 {
        return false;
    }

    order.fields[0].0 == E::MODEL.primary_key.name
}

// Derive COUNT key-stream traversal direction from the PK-only order contract.
fn count_stream_direction_for_terminal<E>(plan: &ExecutablePlan<E>) -> Direction
where
    E: EntityKind,
{
    let Some(order) = plan.order_spec() else {
        return Direction::Asc;
    };
    if order.fields.len() != 1 || order.fields[0].0 != E::MODEL.primary_key.name {
        return Direction::Asc;
    }

    direction_from_order(order.fields[0].1)
}

// Convert one ORDER BY direction contract into execution traversal direction.
const fn direction_from_order(order_direction: OrderDirection) -> Direction {
    match order_direction {
        OrderDirection::Asc => Direction::Asc,
        OrderDirection::Desc => Direction::Desc,
    }
}

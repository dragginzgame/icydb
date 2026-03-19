//! Module: executor::route::terminal
//! Responsibility: route-owned terminal fast-path contracts.
//! Does not own: terminal execution mechanics.
//! Boundary: canonical terminal eligibility derivation consumed by load/aggregate terminals.

use crate::{
    db::{
        access::single_path_capabilities,
        direction::Direction,
        executor::{ExecutablePlan, pipeline::contracts::LoadExecutor},
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
    // Derive one route-owned `count()` terminal fast-path contract.
    pub(in crate::db::executor) fn derive_count_terminal_fast_path_contract(
        plan: &ExecutablePlan<E>,
    ) -> Option<CountTerminalFastPathContract> {
        let access_strategy = plan.access().resolve_strategy();
        let capabilities = access_strategy.as_path().map(single_path_capabilities)?;

        (plan.has_no_predicate_or_distinct()
            && capabilities.supports_count_terminal_primary_key_cardinality())
        .then_some(CountTerminalFastPathContract::PrimaryKeyCardinality)
        .or_else(|| {
            let direction = plan.unordered_or_primary_key_order_direction()?;
            (!plan.has_predicate()
                && capabilities.supports_count_terminal_primary_key_existing_rows())
            .then_some(CountTerminalFastPathContract::PrimaryKeyExistingRows(
                direction,
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

//! Module: executor::route::terminal
//! Responsibility: route-owned terminal fast-path contracts.
//! Does not own: terminal execution mechanics.
//! Boundary: canonical terminal eligibility derivation consumed by load/aggregate terminals.

use crate::{
    db::{
        access::single_path_capabilities,
        direction::Direction,
        executor::{
            ExecutionPreparation, authority::derive_secondary_covering_authority_profile,
            preparation::slot_map_for_model_plan,
        },
        predicate::IndexPredicateCapability,
        query::plan::{
            AccessPlannedQuery, CoveringExistingRowMode, CoveringReadExecutionPlan,
            covering_read_execution_plan, index_covering_existing_rows_terminal_eligible,
        },
        registry::StoreHandle,
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

// Promote one narrow secondary covering cohort onto witness-backed authority
// when the resolved store pair is synchronized and the centralized route-owned
// authority profile marks the covering contract as explicitly admissible.
// Validity is one extra gate here because one synchronized witness is still
// not enough if the index itself is mid-build or mid-drop.
pub(in crate::db::executor) fn promote_load_terminal_fast_path_with_secondary_authority_witness(
    store: StoreHandle,
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
    load_terminal_fast_path: &mut Option<LoadTerminalFastPathContract>,
) {
    let Some(LoadTerminalFastPathContract::CoveringRead(covering)) = load_terminal_fast_path else {
        return;
    };
    let authority_profile = derive_secondary_covering_authority_profile(model, plan, covering);

    if !store.index_is_valid()
        || !store.secondary_covering_authoritative()
        || !authority_profile.supports_witness_validated()
    {
        return;
    }

    covering.existing_row_mode = CoveringExistingRowMode::WitnessValidated;
}

// Promote one narrow stale-fallback secondary covering cohort onto an
// explicit storage-owned existence witness when the synchronized pair witness
// is unavailable but the centralized route-owned authority profile still
// marks the stale cohort as explicitly admissible.
// Even that explicit witness must fail closed unless the index is query-visible
// as `Valid`; incomplete or dropping indexes must keep the probe-based path.
pub(in crate::db::executor) fn promote_load_terminal_fast_path_with_storage_existence_witness(
    store: StoreHandle,
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
    load_terminal_fast_path: &mut Option<LoadTerminalFastPathContract>,
) {
    let Some(LoadTerminalFastPathContract::CoveringRead(covering)) = load_terminal_fast_path else {
        return;
    };
    let authority_profile = derive_secondary_covering_authority_profile(model, plan, covering);

    if !store.index_is_valid()
        || store.secondary_covering_authoritative()
        || !store.secondary_existence_witness_authoritative()
        || !authority_profile.supports_storage_existence_witness()
    {
        return;
    }

    covering.existing_row_mode = CoveringExistingRowMode::StorageExistenceWitness;
}

// Return whether the structural plan still carries a residual predicate.
fn plan_has_predicate(plan: &AccessPlannedQuery) -> bool {
    plan.has_residual_predicate()
}

// Return whether the structural plan clears the DISTINCT gate.
const fn plan_has_no_distinct(plan: &AccessPlannedQuery) -> bool {
    !plan.scalar_plan().distinct
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
///
/// `0.70` intentionally does not apply `IndexState::Valid` gating here yet.
/// These aggregate existing-row shortcuts are a separate correctness problem
/// from probe-free covering reads and still need their own missing-row
/// sensitivity classification.
pub(in crate::db::executor) fn derive_count_terminal_fast_path_contract_for_model(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
    strict_predicate_compatible: bool,
) -> Option<CountTerminalFastPathContract> {
    let access_strategy = plan.access.resolve_strategy();
    let capabilities = access_strategy.as_path().map(single_path_capabilities)?;

    (plan_has_no_distinct(plan)
        && !plan_has_predicate(plan)
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
pub(in crate::db::executor) fn derive_load_terminal_fast_path_contract_for_model(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
    strict_predicate_compatible: bool,
) -> Option<LoadTerminalFastPathContract> {
    covering_read_execution_plan(
        model,
        plan,
        model.primary_key.name,
        strict_predicate_compatible,
    )
    .map(LoadTerminalFastPathContract::CoveringRead)
}

/// Derive one route-owned scalar load terminal fast-path contract directly from
/// one structural model + plan boundary.
pub(in crate::db::executor) fn derive_load_terminal_fast_path_contract_for_model_plan(
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
) -> Option<LoadTerminalFastPathContract> {
    if !plan.scalar_plan().mode.is_load() {
        return None;
    }

    let execution_preparation =
        ExecutionPreparation::from_plan(model, plan, slot_map_for_model_plan(model, plan));
    let strict_predicate_compatible = !plan.has_residual_predicate()
        || execution_preparation
            .predicate_capability_profile()
            .is_some_and(|profile| profile.index() == IndexPredicateCapability::FullyIndexable);

    derive_load_terminal_fast_path_contract_for_model(model, plan, strict_predicate_compatible)
}

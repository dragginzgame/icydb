//! Module: executor::route::terminal
//! Responsibility: route-owned terminal fast-path contracts.
//! Does not own: terminal execution mechanics.
//! Boundary: canonical terminal eligibility derivation consumed by load/aggregate terminals.

use crate::{
    db::{
        access::single_path_capabilities,
        direction::Direction,
        executor::{ExecutionPreparation, preparation::slot_map_for_model_plan},
        predicate::IndexPredicateCapability,
        query::plan::{
            AccessPlannedQuery, CoveringExistingRowMode, CoveringProjectionOrder,
            CoveringReadExecutionPlan, CoveringReadFieldSource, covering_read_execution_plan,
            index_covering_existing_rows_terminal_eligible,
        },
        registry::StoreHandle,
    },
    model::entity::EntityModel,
};
use std::ops::Bound;
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

///
/// SecondaryWitnessValidatedCoveringCohort
///
/// Route-owned classifier for the explicit secondary witness-backed covering
/// cohorts.
/// Each variant names one admitted covering family so widening stays
/// centralized in one owner instead of growing more structural booleans.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SecondaryWitnessValidatedCoveringCohort {
    OrderOnlySingleField,
    CompositeOrderOnly,
    EqualityPrefixPrimaryKeyOrder,
    BoundedRangeSingleField,
    CompositeEqualityPrefixSuffixOrder,
    CompositeBoundedRangeSuffixOrder,
}

impl SecondaryWitnessValidatedCoveringCohort {
    // Return whether one planner-owned covering-order contract matches this
    // explicit witness-backed secondary cohort.
    const fn matches_order_contract(self, order_contract: CoveringProjectionOrder) -> bool {
        matches!(
            (self, order_contract),
            (
                Self::OrderOnlySingleField | Self::BoundedRangeSingleField,
                CoveringProjectionOrder::IndexOrder(_)
            ) | (
                Self::CompositeOrderOnly
                    | Self::CompositeEqualityPrefixSuffixOrder
                    | Self::CompositeBoundedRangeSuffixOrder,
                CoveringProjectionOrder::IndexOrder(Direction::Asc | Direction::Desc)
            ) | (
                Self::EqualityPrefixPrimaryKeyOrder,
                CoveringProjectionOrder::PrimaryKeyOrder(_)
            )
        )
    }

    // Return whether one covering field-source layout matches this explicit
    // witness-backed secondary cohort.
    const fn matches_field_source_counts(
        self,
        field_count: usize,
        component_field_count: usize,
        constant_field_count: usize,
    ) -> bool {
        if field_count == 0 {
            return false;
        }

        match self {
            Self::OrderOnlySingleField | Self::BoundedRangeSingleField => {
                component_field_count <= 1
                    && constant_field_count == 0
                    && component_field_count <= field_count
            }
            Self::CompositeOrderOnly => {
                component_field_count <= 2
                    && constant_field_count == 0
                    && component_field_count <= field_count
            }
            Self::EqualityPrefixPrimaryKeyOrder => {
                component_field_count == 0 && constant_field_count <= 1
            }
            Self::CompositeEqualityPrefixSuffixOrder | Self::CompositeBoundedRangeSuffixOrder => {
                component_field_count <= 1
                    && constant_field_count <= 1
                    && component_field_count.saturating_add(constant_field_count) <= field_count
            }
        }
    }

    // Return the expected decoded index-component slot for one projected
    // component field when this cohort uses one.
    const fn component_index_supported(self, component_index: usize) -> bool {
        match self {
            Self::OrderOnlySingleField | Self::BoundedRangeSingleField => component_index == 0,
            Self::CompositeOrderOnly => component_index <= 1,
            Self::EqualityPrefixPrimaryKeyOrder => false,
            Self::CompositeEqualityPrefixSuffixOrder | Self::CompositeBoundedRangeSuffixOrder => {
                component_index == 1
            }
        }
    }
}

// Promote one narrow secondary covering cohort onto witness-backed authority
// when the resolved store pair is synchronized and the route contract is
// otherwise already explicit covering-read.
pub(in crate::db::executor) fn promote_load_terminal_fast_path_with_secondary_authority_witness(
    store: StoreHandle,
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
    load_terminal_fast_path: &mut Option<LoadTerminalFastPathContract>,
) {
    let Some(LoadTerminalFastPathContract::CoveringRead(covering)) = load_terminal_fast_path else {
        return;
    };
    if !store.secondary_covering_authoritative()
        || !secondary_witness_validated_covering_eligible(model, plan, covering)
    {
        return;
    }

    covering.existing_row_mode = CoveringExistingRowMode::WitnessValidated;
}

// Promote one narrow stale-fallback secondary covering cohort onto an
// explicit storage-owned existence witness when the synchronized pair witness
// is unavailable but the storage witness is authoritative.
pub(in crate::db::executor) fn promote_load_terminal_fast_path_with_storage_existence_witness(
    store: StoreHandle,
    plan: &AccessPlannedQuery,
    load_terminal_fast_path: &mut Option<LoadTerminalFastPathContract>,
) {
    let Some(LoadTerminalFastPathContract::CoveringRead(covering)) = load_terminal_fast_path else {
        return;
    };
    if store.secondary_covering_authoritative()
        || !store.secondary_existence_witness_authoritative()
        || !secondary_storage_existence_witness_covering_eligible(plan, covering)
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

// Return whether the current covering route matches the kept explicit
// storage-owned existence-witness prototype: one stale order-only secondary
// route that projects the first index component and may additionally project
// the primary key.
fn secondary_storage_existence_witness_covering_eligible(
    plan: &AccessPlannedQuery,
    covering: &CoveringReadExecutionPlan,
) -> bool {
    if !plan_has_no_distinct(plan)
        || plan_has_predicate(plan)
        || covering.existing_row_mode != CoveringExistingRowMode::RequiresRowPresenceCheck
        || !matches!(
            covering.order_contract,
            CoveringProjectionOrder::IndexOrder(_)
        )
        || covering.fields.len() > 2
    {
        return false;
    }

    let eligible_access_shape = matches!(
        plan.access.as_index_prefix_path(),
        Some((index, prefix_values)) if index.fields().len() == 1 && prefix_values.is_empty()
    ) || matches!(
        plan.access.as_index_range_path(),
        Some((index, prefix_values, Bound::Unbounded, Bound::Unbounded))
            if index.fields().len() == 1 && prefix_values.is_empty()
    );
    if !eligible_access_shape {
        return false;
    }

    let mut index_component_count = 0usize;
    let mut primary_key_count = 0usize;

    for field in &covering.fields {
        match field.source {
            CoveringReadFieldSource::IndexComponent { component_index: 0 } => {
                index_component_count = index_component_count.saturating_add(1);
            }
            CoveringReadFieldSource::PrimaryKey => {
                primary_key_count = primary_key_count.saturating_add(1);
            }
            _ => return false,
        }
    }

    index_component_count == 1
        && primary_key_count <= 1
        && covering.fields.len() == index_component_count + primary_key_count
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

// Return whether one covering-read contract matches the first explicit
// witness-backed secondary authority cohort.
fn secondary_witness_validated_covering_eligible(
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
    covering: &CoveringReadExecutionPlan,
) -> bool {
    // Phase 1: keep the explicit witness-backed cohorts narrow so the
    // authority upgrade stays explicit.
    if !plan.scalar_plan().mode.is_load()
        || !plan_predicate_is_absent_or_fully_indexable(model, plan)
        || plan.scalar_plan().distinct
        || covering.existing_row_mode != CoveringExistingRowMode::RequiresRowPresenceCheck
    {
        return false;
    }

    // Phase 2: classify one explicit secondary witness cohort. The classifier
    // is the policy owner; the checks below only validate that the current
    // covering contract actually matches the admitted cohort.
    let Some(cohort) = secondary_witness_validated_covering_cohort(plan, covering) else {
        return false;
    };

    // Phase 3: require the narrow covering-source layouts that current runtime
    // covering execution already knows how to emit under witness-backed
    // authority.
    let Some(primary_key_slot) = model
        .fields
        .iter()
        .position(|field| field.name == model.primary_key().name)
    else {
        return false;
    };
    let mut component_field_count = 0usize;
    let mut constant_field_count = 0usize;
    for field in &covering.fields {
        match field.source {
            CoveringReadFieldSource::PrimaryKey => {
                if field.field_slot.index != primary_key_slot {
                    return false;
                }
            }
            CoveringReadFieldSource::IndexComponent { component_index } => {
                if !cohort.component_index_supported(component_index) {
                    return false;
                }
                component_field_count = component_field_count.saturating_add(1);
            }
            CoveringReadFieldSource::Constant(_) => {
                constant_field_count = constant_field_count.saturating_add(1);
            }
        }
    }

    cohort.matches_field_source_counts(
        covering.fields.len(),
        component_field_count,
        constant_field_count,
    )
}

// Return whether the current scalar predicate is either absent or fully
// index-compatible on the chosen access route.
fn plan_predicate_is_absent_or_fully_indexable(
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
) -> bool {
    if plan.scalar_plan().predicate.is_none() {
        return true;
    }

    let execution_preparation =
        ExecutionPreparation::from_plan(model, plan, slot_map_for_model_plan(model, plan));

    execution_preparation
        .predicate_capability_profile()
        .is_some_and(|profile| profile.index() == IndexPredicateCapability::FullyIndexable)
}

// Classify one explicit secondary witness-backed covering cohort from the
// structural access route plus planner-owned covering order contract.
fn secondary_witness_validated_covering_cohort(
    plan: &AccessPlannedQuery,
    covering: &CoveringReadExecutionPlan,
) -> Option<SecondaryWitnessValidatedCoveringCohort> {
    if let Some((index, prefix_values)) = plan.access.as_index_prefix_path() {
        let cohort = match prefix_values.len() {
            0 if index.fields().len() == 1 => {
                Some(SecondaryWitnessValidatedCoveringCohort::OrderOnlySingleField)
            }
            0 if index.fields().len() == 2 => {
                Some(SecondaryWitnessValidatedCoveringCohort::CompositeOrderOnly)
            }
            1 if index.fields().len() == 1 => {
                Some(SecondaryWitnessValidatedCoveringCohort::EqualityPrefixPrimaryKeyOrder)
            }
            1 if index.fields().len() == 2 => {
                Some(SecondaryWitnessValidatedCoveringCohort::CompositeEqualityPrefixSuffixOrder)
            }
            _ => None,
        };

        return cohort.filter(|cohort| cohort.matches_order_contract(covering.order_contract));
    }

    if let Some((index, prefix_values, _, _)) = plan.access.as_index_range_path() {
        let cohort = match (index.fields().len(), prefix_values.len()) {
            (1, 0) => Some(SecondaryWitnessValidatedCoveringCohort::BoundedRangeSingleField),
            (2, 0) => Some(SecondaryWitnessValidatedCoveringCohort::CompositeOrderOnly),
            (2, 1) => {
                Some(SecondaryWitnessValidatedCoveringCohort::CompositeBoundedRangeSuffixOrder)
            }
            _ => None,
        }?;

        return cohort
            .matches_order_contract(covering.order_contract)
            .then_some(cohort);
    }

    None
}

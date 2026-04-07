use crate::{
    db::{
        direction::Direction,
        executor::{ExecutionPreparation, preparation::slot_map_for_model_plan},
        predicate::IndexPredicateCapability,
        query::plan::{
            AccessPlannedQuery, CoveringExistingRowMode, CoveringProjectionOrder,
            CoveringReadExecutionPlan, CoveringReadFieldSource,
        },
    },
    model::entity::EntityModel,
};
use std::ops::Bound;

// Classify one explicit secondary witness-backed cohort. These cohorts are
// already shipped, so widening stays evidence-backed instead of rediscovering
// route policy from structural booleans in the terminal module.
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

// Classify one explicit stale storage-witness cohort. These cohorts are kept
// intentionally narrow and should grow only from measured evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StorageExistenceWitnessCoveringCohort {
    OrderOnlySingleField,
    CompositeOrderOnly,
    CompositeLeadingComponentOrderOnly,
    CompositeEqualityPrefixSuffixOrder,
    CompositeEqualityPrefixLeadingComponent,
}

///
/// SecondaryCoveringAuthorityProfile
///
/// SecondaryCoveringAuthorityProfile is the centralized route-owned authority
/// summary for one already-derived covering-read contract.
/// It keeps witness-validated and storage-existence-witness eligibility in one
/// structural policy bundle so terminal routing no longer rediscover those
/// cohorts independently.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db::executor) struct SecondaryCoveringAuthorityProfile {
    witness_validated_cohort: Option<SecondaryWitnessValidatedCoveringCohort>,
    storage_existence_witness_cohort: Option<StorageExistenceWitnessCoveringCohort>,
}

impl SecondaryCoveringAuthorityProfile {
    // Build one empty profile for routes that stay on probe-required
    // authority.
    const fn none() -> Self {
        Self {
            witness_validated_cohort: None,
            storage_existence_witness_cohort: None,
        }
    }

    // Return whether this route matches one explicit witness-backed secondary
    // cohort.
    pub(in crate::db::executor) const fn supports_witness_validated(self) -> bool {
        self.witness_validated_cohort.is_some()
    }

    // Return whether this route matches one explicit stale storage-witness
    // cohort.
    pub(in crate::db::executor) const fn supports_storage_existence_witness(self) -> bool {
        self.storage_existence_witness_cohort.is_some()
    }
}

// Derive one centralized route-owned authority profile from the structural
// plan plus planner-owned covering contract. This is intentionally
// conservative and only returns the explicit cohorts already kept in `0.69`.
pub(in crate::db::executor) fn derive_secondary_covering_authority_profile(
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
    covering: &CoveringReadExecutionPlan,
) -> SecondaryCoveringAuthorityProfile {
    if covering.existing_row_mode != CoveringExistingRowMode::RequiresRowPresenceCheck {
        return SecondaryCoveringAuthorityProfile::none();
    }

    SecondaryCoveringAuthorityProfile {
        witness_validated_cohort: secondary_witness_validated_covering_cohort(
            model, plan, covering,
        ),
        storage_existence_witness_cohort: storage_existence_witness_covering_cohort(plan, covering),
    }
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
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
    covering: &CoveringReadExecutionPlan,
) -> Option<SecondaryWitnessValidatedCoveringCohort> {
    if !plan.scalar_plan().mode.is_load()
        || !plan_predicate_is_absent_or_fully_indexable(model, plan)
        || plan.scalar_plan().distinct
    {
        return None;
    }

    let cohort = if let Some((index, prefix_values)) = plan.access.as_index_prefix_path() {
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

        cohort.filter(|cohort| cohort.matches_order_contract(covering.order_contract))
    } else if let Some((index, prefix_values, _, _)) = plan.access.as_index_range_path() {
        let cohort = match (index.fields().len(), prefix_values.len()) {
            (1, 0) => Some(SecondaryWitnessValidatedCoveringCohort::BoundedRangeSingleField),
            (2, 0) => Some(SecondaryWitnessValidatedCoveringCohort::CompositeOrderOnly),
            (2, 1) => {
                Some(SecondaryWitnessValidatedCoveringCohort::CompositeBoundedRangeSuffixOrder)
            }
            _ => None,
        }?;

        cohort
            .matches_order_contract(covering.order_contract)
            .then_some(cohort)
    } else {
        None
    }?;

    let primary_key_slot = model
        .fields
        .iter()
        .position(|field| field.name == model.primary_key().name)?;
    let mut component_field_count = 0usize;
    let mut constant_field_count = 0usize;
    for field in &covering.fields {
        match field.source {
            CoveringReadFieldSource::PrimaryKey => {
                if field.field_slot.index != primary_key_slot {
                    return None;
                }
            }
            CoveringReadFieldSource::IndexComponent { component_index } => {
                if !cohort.component_index_supported(component_index) {
                    return None;
                }
                component_field_count = component_field_count.saturating_add(1);
            }
            CoveringReadFieldSource::Constant(_) => {
                constant_field_count = constant_field_count.saturating_add(1);
            }
        }
    }

    cohort
        .matches_field_source_counts(
            covering.fields.len(),
            component_field_count,
            constant_field_count,
        )
        .then_some(cohort)
}

// Classify one explicit stale storage-witness cohort from the structural
// access route plus covering field-source contract.
fn storage_existence_witness_covering_cohort(
    plan: &AccessPlannedQuery,
    covering: &CoveringReadExecutionPlan,
) -> Option<StorageExistenceWitnessCoveringCohort> {
    if plan.scalar_plan().distinct || plan.has_residual_predicate() {
        return None;
    }

    storage_existence_witness_equality_prefix_covering_cohort(plan, covering).or_else(|| {
        let index_field_count = storage_existence_witness_index_field_count(plan)?;

        let mut component_zero_count = 0usize;
        let mut component_one_count = 0usize;
        let mut primary_key_count = 0usize;

        for field in &covering.fields {
            match field.source {
                CoveringReadFieldSource::IndexComponent { component_index: 0 } => {
                    component_zero_count = component_zero_count.saturating_add(1);
                }
                CoveringReadFieldSource::IndexComponent { component_index: 1 } => {
                    component_one_count = component_one_count.saturating_add(1);
                }
                CoveringReadFieldSource::PrimaryKey => {
                    primary_key_count = primary_key_count.saturating_add(1);
                }
                _ => return None,
            }
        }

        match index_field_count {
            1 => (matches!(
                covering.order_contract,
                CoveringProjectionOrder::IndexOrder(_)
            ) && component_zero_count == 1
                && component_one_count == 0
                && primary_key_count <= 1
                && covering.fields.len() == component_zero_count + primary_key_count)
                .then_some(StorageExistenceWitnessCoveringCohort::OrderOnlySingleField),
            2 => {
                let full_composite = matches!(
                    covering.order_contract,
                    CoveringProjectionOrder::IndexOrder(Direction::Asc | Direction::Desc)
                ) && component_zero_count == 1
                    && component_one_count == 1
                    && primary_key_count <= 1
                    && covering.fields.len()
                        == component_zero_count + component_one_count + primary_key_count;
                let leading_component_plus_pk = matches!(
                    covering.order_contract,
                    CoveringProjectionOrder::IndexOrder(Direction::Asc | Direction::Desc)
                ) && component_zero_count == 1
                    && component_one_count == 0
                    && primary_key_count == 1
                    && covering.fields.len() == component_zero_count + primary_key_count;

                if full_composite {
                    Some(StorageExistenceWitnessCoveringCohort::CompositeOrderOnly)
                } else if leading_component_plus_pk {
                    Some(StorageExistenceWitnessCoveringCohort::CompositeLeadingComponentOrderOnly)
                } else {
                    None
                }
            }
            _ => None,
        }
    })
}

// Classify the narrow measured equality-prefix stale cohorts. These are kept
// explicit instead of inferred from adjacency because the authority invariant
// depends on the final bounded window, not the surface syntax alone.
fn storage_existence_witness_equality_prefix_covering_cohort(
    plan: &AccessPlannedQuery,
    covering: &CoveringReadExecutionPlan,
) -> Option<StorageExistenceWitnessCoveringCohort> {
    let (index, prefix_values) = plan.access.as_index_prefix_path()?;
    if index.fields().len() != 2 || prefix_values.len() != 1 {
        return None;
    }

    let mut component_zero_count = 0usize;
    let mut component_one_count = 0usize;
    let mut constant_count = 0usize;
    let mut primary_key_count = 0usize;

    for field in &covering.fields {
        match field.source {
            CoveringReadFieldSource::IndexComponent { component_index: 0 } => {
                component_zero_count = component_zero_count.saturating_add(1);
            }
            CoveringReadFieldSource::IndexComponent { component_index: 1 } => {
                component_one_count = component_one_count.saturating_add(1);
            }
            CoveringReadFieldSource::IndexComponent { component_index: _ } => {
                return None;
            }
            CoveringReadFieldSource::Constant(_) => {
                constant_count = constant_count.saturating_add(1);
            }
            CoveringReadFieldSource::PrimaryKey => {
                primary_key_count = primary_key_count.saturating_add(1);
            }
        }
    }

    let suffix_order = matches!(
        covering.order_contract,
        CoveringProjectionOrder::IndexOrder(Direction::Asc | Direction::Desc)
    );
    let equality_prefix_suffix_order = suffix_order
        && component_zero_count == 0
        && component_one_count == 1
        && constant_count == 1
        && primary_key_count == 1
        && covering.fields.len()
            == component_one_count
                .saturating_add(constant_count)
                .saturating_add(primary_key_count);
    let equality_prefix_constant_plus_pk = matches!(
        covering.order_contract,
        CoveringProjectionOrder::IndexOrder(Direction::Asc | Direction::Desc)
    ) && component_zero_count == 0
        && component_one_count == 0
        && constant_count == 1
        && primary_key_count == 1
        && covering.fields.len() == constant_count.saturating_add(primary_key_count);

    if equality_prefix_suffix_order {
        Some(StorageExistenceWitnessCoveringCohort::CompositeEqualityPrefixSuffixOrder)
    } else if equality_prefix_constant_plus_pk {
        Some(StorageExistenceWitnessCoveringCohort::CompositeEqualityPrefixLeadingComponent)
    } else {
        None
    }
}

// Return the admitted index-field cardinality for one stale storage-witness
// access shape. The current prototype allows only unbounded secondary
// order-only scans with no equality prefix.
fn storage_existence_witness_index_field_count(plan: &AccessPlannedQuery) -> Option<usize> {
    match plan.access.as_index_prefix_path() {
        Some((index, [])) => Some(index.fields().len()),
        _ => match plan.access.as_index_range_path() {
            Some((index, [], Bound::Unbounded, Bound::Unbounded)) => Some(index.fields().len()),
            _ => None,
        },
    }
}

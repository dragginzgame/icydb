use crate::{
    db::{
        direction::Direction,
        executor::{ExecutionPreparation, preparation::slot_map_for_model_plan},
        index::IndexState,
        predicate::IndexPredicateCapability,
        query::plan::{
            AccessPlannedQuery, CoveringExistingRowMode, CoveringProjectionOrder,
            CoveringReadExecutionPlan, CoveringReadFieldSource,
        },
        registry::StoreHandle,
    },
    model::entity::EntityModel,
};
use std::ops::Bound;

///
/// AuthorityDecision
///
/// High-level read-authority decision for one store-backed secondary load.
/// This stays intentionally small in `0.70.2`: either the route may stay
/// probe free, or it must fail closed back to row checks.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum AuthorityDecision {
    ProbeFree,
    RowCheckRequired,
}

///
/// AuthorityReason
///
/// Stable reason vocabulary paired with `AuthorityDecision`.
/// These labels intentionally match the external `EXPLAIN` surface so route
/// selection and inspection stay on one shared classification story.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum AuthorityReason {
    ProbeRequired,
    IndexNotValid,
    SynchronizedPairWitness,
    StaleStorageExistenceWitness,
    AuthoritativeWitnessUnavailable,
}

///
/// SecondaryReadAuthorityOwner
///
/// Explicit ownership split for store-backed secondary read authority.
/// `0.70.2` keeps the flat classifier on the single-component line, while the
/// older richer covering profile remains the authority owner for broader
/// composite/stale covering cohorts until a wider invariant is proven.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum SecondaryReadAuthorityOwner {
    FlatSingleComponentClassifier,
    FlatCompositeWitnessValidatedClassifier,
    RichCoveringProfile,
}

///
/// AuthorityContext
///
/// Minimal structural context used by the `0.70.2` authority classifier.
/// This keeps the new decision point small while still preserving the already
/// shipped witness-backed covering semantics for the single-component line.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct AuthorityContext {
    index_state: IndexState,
    is_covering: bool,
    is_classifier_supported_shape: bool,
    probe_free_existing_row_mode: Option<CoveringExistingRowMode>,
}

impl AuthorityContext {
    // Build one compact authority context from the route-owned structural
    // inputs already available at the store-backed load boundary.
    const fn new(
        index_state: IndexState,
        is_covering: bool,
        is_classifier_supported_shape: bool,
        probe_free_existing_row_mode: Option<CoveringExistingRowMode>,
    ) -> Self {
        Self {
            index_state,
            is_covering,
            is_classifier_supported_shape,
            probe_free_existing_row_mode,
        }
    }
}

// Return the preserved authority classification for one already-resolved
// probe-free covering mode.
const fn probe_free_mode_authority_classification(
    existing_row_mode: CoveringExistingRowMode,
) -> Option<(AuthorityDecision, AuthorityReason)> {
    match existing_row_mode {
        CoveringExistingRowMode::WitnessValidated => Some((
            AuthorityDecision::ProbeFree,
            AuthorityReason::SynchronizedPairWitness,
        )),
        CoveringExistingRowMode::StorageExistenceWitness => Some((
            AuthorityDecision::ProbeFree,
            AuthorityReason::StaleStorageExistenceWitness,
        )),
        _ => None,
    }
}

// Return one already-promoted probe-free covering mode when the route has
// resolved it before the centralized classifier runs.
const fn preserved_probe_free_existing_row_mode(
    existing_row_mode: CoveringExistingRowMode,
) -> Option<CoveringExistingRowMode> {
    match existing_row_mode {
        CoveringExistingRowMode::WitnessValidated
        | CoveringExistingRowMode::StorageExistenceWitness => Some(existing_row_mode),
        _ => None,
    }
}

// Return the stable external label for one centralized authority reason.
pub(in crate::db::executor) const fn authority_reason_label(
    reason: AuthorityReason,
) -> &'static str {
    match reason {
        AuthorityReason::ProbeRequired => "probe_required",
        AuthorityReason::IndexNotValid => "index_not_valid",
        AuthorityReason::SynchronizedPairWitness => "synchronized_pair_witness",
        AuthorityReason::StaleStorageExistenceWitness => "stale_storage_existence_witness",
        AuthorityReason::AuthoritativeWitnessUnavailable => "authoritative_witness_unavailable",
    }
}

// Return the stable external decision label for one centralized authority
// classification while keeping the current flat `EXPLAIN` vocabulary intact.
pub(in crate::db::executor) const fn authority_decision_label(
    decision: AuthorityDecision,
    reason: AuthorityReason,
) -> &'static str {
    match (decision, reason) {
        (AuthorityDecision::ProbeFree, AuthorityReason::SynchronizedPairWitness) => {
            "witness_validated"
        }
        (AuthorityDecision::ProbeFree, AuthorityReason::StaleStorageExistenceWitness) => {
            "storage_existence_witness"
        }
        _ => "row_check_required",
    }
}

// Return whether one structural access path still runs on a single-component
// secondary index.
fn secondary_access_is_single_component(plan: &AccessPlannedQuery) -> bool {
    match plan.access.as_index_prefix_path() {
        Some((index, _)) => index.fields().len() == 1,
        None => match plan.access.as_index_range_path() {
            Some((index, _, _, _)) => index.fields().len() == 1,
            None => false,
        },
    }
}

// Return whether one covering contract matches the narrow composite
// witness-validated family that `0.70.2` can state cleanly without the richer
// stale witness structure.
fn secondary_classifier_owns_composite_witness_validated_family(
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
    load_terminal_fast_path: Option<&crate::db::executor::route::LoadTerminalFastPathContract>,
) -> bool {
    let Some(crate::db::executor::route::LoadTerminalFastPathContract::CoveringRead(covering)) =
        load_terminal_fast_path
    else {
        return false;
    };

    secondary_witness_validated_covering_cohort(model, plan, covering)
        == Some(SecondaryWitnessValidatedCoveringCohort::CompositeOrderOnly)
}

// Return which authority mechanism owns one concrete store-backed secondary
// read. `0.70.2` now admits one explicit composite synchronized-witness family
// into the flat classifier, while stale composite families remain profile-owned.
pub(in crate::db::executor) fn secondary_read_authority_owner(
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
    load_terminal_fast_path: Option<&crate::db::executor::route::LoadTerminalFastPathContract>,
    store: StoreHandle,
) -> SecondaryReadAuthorityOwner {
    if secondary_access_is_single_component(plan) {
        SecondaryReadAuthorityOwner::FlatSingleComponentClassifier
    } else if store.secondary_covering_authoritative()
        && secondary_classifier_owns_composite_witness_validated_family(
            model,
            plan,
            load_terminal_fast_path,
        )
    {
        SecondaryReadAuthorityOwner::FlatCompositeWitnessValidatedClassifier
    } else {
        SecondaryReadAuthorityOwner::RichCoveringProfile
    }
}

// Classify one compact secondary-read authority context. The current `0.70.2`
// rule is intentionally narrow:
// - non-covering or non-single-component routes stay on row checks
// - invalid indexes fail closed
// - only the already-shipped witness-backed covering modes become probe free
pub(in crate::db::executor) const fn classify_authority(
    context: AuthorityContext,
) -> (AuthorityDecision, AuthorityReason) {
    // The classifier is monotonic: once the route already carries an explicit
    // probe-free mode, classification must preserve that mode instead of
    // downgrading it through a second structural pass.
    if let Some(probe_free_existing_row_mode) = context.probe_free_existing_row_mode
        && let Some(classification) =
            probe_free_mode_authority_classification(probe_free_existing_row_mode)
    {
        return classification;
    }

    if !context.is_covering || !context.is_classifier_supported_shape {
        return (
            AuthorityDecision::RowCheckRequired,
            AuthorityReason::ProbeRequired,
        );
    }

    if !matches!(context.index_state, IndexState::Valid) {
        return (
            AuthorityDecision::RowCheckRequired,
            AuthorityReason::IndexNotValid,
        );
    }

    match context.probe_free_existing_row_mode {
        Some(CoveringExistingRowMode::WitnessValidated) => (
            AuthorityDecision::ProbeFree,
            AuthorityReason::SynchronizedPairWitness,
        ),
        Some(CoveringExistingRowMode::StorageExistenceWitness) => (
            AuthorityDecision::ProbeFree,
            AuthorityReason::StaleStorageExistenceWitness,
        ),
        _ => (
            AuthorityDecision::RowCheckRequired,
            AuthorityReason::AuthoritativeWitnessUnavailable,
        ),
    }
}

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

    // Resolve the final probe-free existing-row mode this explicit authority
    // profile may use for one concrete store pair.
    pub(in crate::db::executor) fn promoted_existing_row_mode_for_store(
        self,
        store: StoreHandle,
    ) -> Option<CoveringExistingRowMode> {
        // Phase 1: fail closed unless the index itself is query-visible as
        // `Valid`; synchronized witness bits alone are not enough while the
        // store is still building or dropping.
        if !store.index_is_valid() {
            return None;
        }

        // Phase 2: prefer the stronger synchronized pair witness whenever it
        // exists so routes do not drift onto the narrower stale witness path.
        if store.secondary_covering_authoritative() && self.supports_witness_validated() {
            return Some(CoveringExistingRowMode::WitnessValidated);
        }

        // Phase 3: only use the stale storage witness when the synchronized
        // pair witness is absent and the measured stale cohort is explicitly
        // admitted by this authority profile.
        if !store.secondary_covering_authoritative()
            && store.secondary_existence_witness_authoritative()
            && self.supports_storage_existence_witness()
        {
            return Some(CoveringExistingRowMode::StorageExistenceWitness);
        }

        None
    }
}

// Resolve one already-admitted probe-free covering mode for a concrete store
// pair, if any, without widening the existing authority cohorts.
fn secondary_covering_probe_free_mode_for_store(
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
    covering: &CoveringReadExecutionPlan,
    store: StoreHandle,
) -> Option<CoveringExistingRowMode> {
    derive_secondary_covering_authority_profile(model, plan, covering)
        .promoted_existing_row_mode_for_store(store)
}

// Resolve the probe-free mode for the one explicit composite synchronized
// witness family now owned by the flat classifier. This stays narrow on
// purpose: stale composite storage-witness cohorts remain profile-owned.
fn composite_witness_validated_probe_free_mode_for_store(
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
    load_terminal_fast_path: Option<&crate::db::executor::route::LoadTerminalFastPathContract>,
    store: StoreHandle,
) -> Option<CoveringExistingRowMode> {
    let Some(crate::db::executor::route::LoadTerminalFastPathContract::CoveringRead(covering)) =
        load_terminal_fast_path
    else {
        return None;
    };

    preserved_probe_free_existing_row_mode(covering.existing_row_mode).or_else(|| {
        (store.index_is_valid()
            && store.secondary_covering_authoritative()
            && secondary_classifier_owns_composite_witness_validated_family(
                model,
                plan,
                load_terminal_fast_path,
            ))
        .then_some(CoveringExistingRowMode::WitnessValidated)
    })
}

// Classify one store-backed secondary load through the centralized `0.70.2`
// authority decision point.
pub(in crate::db::executor) fn classify_secondary_read_authority(
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
    load_terminal_fast_path: Option<&crate::db::executor::route::LoadTerminalFastPathContract>,
    store: StoreHandle,
) -> (AuthorityDecision, AuthorityReason) {
    let authority_owner =
        secondary_read_authority_owner(model, plan, load_terminal_fast_path, store);
    let probe_free_existing_row_mode = match authority_owner {
        SecondaryReadAuthorityOwner::FlatSingleComponentClassifier => {
            match load_terminal_fast_path {
                Some(crate::db::executor::route::LoadTerminalFastPathContract::CoveringRead(
                    covering,
                )) => preserved_probe_free_existing_row_mode(covering.existing_row_mode).or_else(
                    || secondary_covering_probe_free_mode_for_store(model, plan, covering, store),
                ),
                None => None,
            }
        }
        SecondaryReadAuthorityOwner::FlatCompositeWitnessValidatedClassifier => {
            composite_witness_validated_probe_free_mode_for_store(
                model,
                plan,
                load_terminal_fast_path,
                store,
            )
        }
        SecondaryReadAuthorityOwner::RichCoveringProfile => None,
    };
    let context = AuthorityContext::new(
        store.index_state(),
        matches!(
            load_terminal_fast_path,
            Some(crate::db::executor::route::LoadTerminalFastPathContract::CoveringRead(_))
        ),
        !matches!(
            authority_owner,
            SecondaryReadAuthorityOwner::RichCoveringProfile
        ),
        probe_free_existing_row_mode,
    );

    classify_authority(context)
}

// Return the canonical covering existing-row mode for one single-component
// secondary classifier result. Route promotion should consume this mapping
// instead of reconstructing probe-free modes from local decision checks.
pub(in crate::db::executor) fn classify_secondary_read_existing_row_mode(
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
    load_terminal_fast_path: Option<&crate::db::executor::route::LoadTerminalFastPathContract>,
    store: StoreHandle,
) -> Option<CoveringExistingRowMode> {
    if !matches!(
        secondary_read_authority_owner(model, plan, load_terminal_fast_path, store),
        SecondaryReadAuthorityOwner::FlatSingleComponentClassifier
            | SecondaryReadAuthorityOwner::FlatCompositeWitnessValidatedClassifier
    ) {
        return None;
    }

    let (decision, reason) =
        classify_secondary_read_authority(model, plan, load_terminal_fast_path, store);

    Some(match (decision, reason) {
        (AuthorityDecision::ProbeFree, AuthorityReason::SynchronizedPairWitness) => {
            CoveringExistingRowMode::WitnessValidated
        }
        (AuthorityDecision::ProbeFree, AuthorityReason::StaleStorageExistenceWitness) => {
            CoveringExistingRowMode::StorageExistenceWitness
        }
        _ => CoveringExistingRowMode::RequiresRowPresenceCheck,
    })
}

// Return the canonical flat EXPLAIN labels for one store-backed secondary read
// classification. `EXPLAIN` must consume this classifier output directly
// instead of reconstructing authority labels from route-local flags.
pub(in crate::db::executor) fn classify_secondary_read_authority_explain_labels(
    model: &'static EntityModel,
    plan: &AccessPlannedQuery,
    load_terminal_fast_path: Option<&crate::db::executor::route::LoadTerminalFastPathContract>,
    store: StoreHandle,
) -> (&'static str, &'static str) {
    let (decision, reason) =
        classify_secondary_read_authority(model, plan, load_terminal_fast_path, store);

    (
        authority_decision_label(decision, reason),
        authority_reason_label(reason),
    )
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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::{
        executor::authority::read::{
            AuthorityContext, AuthorityDecision, AuthorityReason, classify_authority,
        },
        index::IndexState,
        query::plan::CoveringExistingRowMode,
    };

    #[test]
    fn classify_authority_preserves_witness_validated_probe_free_mode() {
        let context = AuthorityContext::new(
            IndexState::Valid,
            true,
            true,
            Some(CoveringExistingRowMode::WitnessValidated),
        );

        assert_eq!(
            classify_authority(context),
            (
                AuthorityDecision::ProbeFree,
                AuthorityReason::SynchronizedPairWitness,
            ),
            "the centralized classifier must preserve an already-promoted witness_validated mode",
        );
    }

    #[test]
    fn classify_authority_preserves_storage_existence_witness_probe_free_mode() {
        let context = AuthorityContext::new(
            IndexState::Valid,
            true,
            true,
            Some(CoveringExistingRowMode::StorageExistenceWitness),
        );

        assert_eq!(
            classify_authority(context),
            (
                AuthorityDecision::ProbeFree,
                AuthorityReason::StaleStorageExistenceWitness,
            ),
            "the centralized classifier must preserve an already-promoted storage_existence_witness mode",
        );
    }

    #[test]
    fn classify_authority_never_downgrades_an_already_probe_free_mode() {
        for existing_row_mode in [
            CoveringExistingRowMode::WitnessValidated,
            CoveringExistingRowMode::StorageExistenceWitness,
        ] {
            let context =
                AuthorityContext::new(IndexState::Valid, true, true, Some(existing_row_mode));
            let (decision, _) = classify_authority(context);

            assert_ne!(
                decision,
                AuthorityDecision::RowCheckRequired,
                "the centralized classifier must never downgrade an already probe-free route",
            );
        }
    }
}

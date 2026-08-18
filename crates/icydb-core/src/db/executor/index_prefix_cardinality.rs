//! Module: executor::index_prefix_cardinality
//! Responsibility: executor-local probes over lowered index-prefix metadata.
//! Does not own: count planning or index metadata maintenance.
//! Boundary: fail-open helpers for runtime branch pruning.

use crate::{
    db::{
        access::{AccessPath, IndexShapeDetails, LoweredIndexPrefixSpec},
        data::DataStore,
        executor::route::IndexPrefixChildExpansionHint,
        index::{IndexId, IndexKey, IndexKeyKind, UserIndexPrefixCardinalityKey},
        integrity::DatabaseIncarnationId,
        query::plan::AccessPlannedQuery,
        registry::StoreHandle,
        schema::cardinality_generation::CardinalityAcceptedRootIdentity,
    },
    error::InternalError,
    types::EntityTag,
    value::Value,
};
use std::ops::Bound;

#[derive(Clone, Copy, Debug)]
pub(in crate::db) struct LoweredIndexPrefixCardinalityPlan<'a> {
    index_id: IndexId,
    prefix_len: usize,
    specs: &'a [LoweredIndexPrefixSpec],
}

impl<'a> LoweredIndexPrefixCardinalityPlan<'a> {
    #[must_use]
    pub(in crate::db) const fn index_id(&self) -> IndexId {
        self.index_id
    }

    #[must_use]
    pub(in crate::db) const fn prefix_len(&self) -> usize {
        self.prefix_len
    }

    #[must_use]
    pub(in crate::db) const fn specs(&self) -> &'a [LoweredIndexPrefixSpec] {
        self.specs
    }
}

pub(in crate::db::executor) struct ExpandedIndexPrefixFamily {
    index: IndexShapeDetails,
    specs: Vec<LoweredIndexPrefixSpec>,
}

impl ExpandedIndexPrefixFamily {
    fn new(
        index: &IndexShapeDetails,
        target_prefix_len: usize,
        specs: Vec<LoweredIndexPrefixSpec>,
    ) -> Self {
        Self {
            index: index.with_slot_arity(target_prefix_len),
            specs,
        }
    }

    #[must_use]
    pub(in crate::db::executor) const fn index(&self) -> &IndexShapeDetails {
        &self.index
    }

    #[must_use]
    pub(in crate::db::executor) const fn specs(&self) -> &[LoweredIndexPrefixSpec] {
        self.specs.as_slice()
    }

    #[must_use]
    pub(in crate::db::executor) fn into_specs(self) -> Vec<LoweredIndexPrefixSpec> {
        self.specs
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum IndexBranchLiveness {
    ProvenEmpty,
    PossiblyLive,
    UnknownConservative(IndexBranchUnknownReason),
}

impl IndexBranchLiveness {
    #[must_use]
    pub(in crate::db::executor) const fn should_scan(self) -> bool {
        match self {
            Self::ProvenEmpty => false,
            Self::PossiblyLive => true,
            Self::UnknownConservative(reason) => {
                let _ = reason;
                true
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum IndexBranchUnknownReason {
    MissingPrefixCardinalityKey,
    MissingGenerationCompatibleCardinality,
}

#[must_use]
pub(in crate::db::executor) fn lowered_index_prefix_liveness(
    store: StoreHandle,
    spec: &LoweredIndexPrefixSpec,
) -> IndexBranchLiveness {
    let Some(cardinality_key) =
        user_index_prefix_cardinality_key_from_lowered_spec(spec, spec.prefix_components().len())
    else {
        return IndexBranchLiveness::UnknownConservative(
            IndexBranchUnknownReason::MissingPrefixCardinalityKey,
        );
    };
    let data_generation = store.with_data(DataStore::generation);
    match store.exact_user_index_prefix_count(
        data_generation,
        IndexKeyKind::User,
        cardinality_key.index_id(),
        cardinality_key.prefix_components(),
    ) {
        Some(0) => IndexBranchLiveness::ProvenEmpty,
        Some(_) => IndexBranchLiveness::PossiblyLive,
        None => IndexBranchLiveness::UnknownConservative(
            IndexBranchUnknownReason::MissingGenerationCompatibleCardinality,
        ),
    }
}

/// Return exact visible cardinalities for one same-index prefix family.
#[must_use]
pub(in crate::db::executor) fn lowered_index_prefix_exact_cardinalities<'a>(
    store: StoreHandle,
    specs: impl IntoIterator<Item = &'a LoweredIndexPrefixSpec>,
) -> Option<Vec<u64>> {
    let cardinality_keys = specs
        .into_iter()
        .map(|spec| {
            user_index_prefix_cardinality_key_from_lowered_spec(
                spec,
                spec.prefix_components().len(),
            )
        })
        .collect::<Option<Vec<_>>>()?;
    let data_generation = store.with_data(DataStore::generation);
    store.exact_user_index_prefix_key_counts(data_generation, &cardinality_keys)
}

/// Return exact cardinalities through accepted authority admitted in this request.
#[must_use]
pub(in crate::db::executor) fn lowered_index_prefix_exact_cardinalities_for_admitted_root<'a>(
    store: StoreHandle,
    database_incarnation: DatabaseIncarnationId,
    accepted_root: CardinalityAcceptedRootIdentity,
    specs: impl IntoIterator<Item = &'a LoweredIndexPrefixSpec>,
) -> Option<Vec<u64>> {
    let cardinality_keys = specs
        .into_iter()
        .map(|spec| {
            user_index_prefix_cardinality_key_from_lowered_spec(
                spec,
                spec.prefix_components().len(),
            )
        })
        .collect::<Option<Vec<_>>>()?;
    let data_generation = store.with_data(DataStore::generation);
    store.exact_user_index_prefix_key_counts_for_admitted_root(
        database_incarnation,
        accepted_root,
        data_generation,
        &cardinality_keys,
    )
}

/// Prove only synchronized generation availability for one same-index family.
///
/// The caller must retain all branches because this boundary deliberately does
/// not read or expose their individual counts.
#[must_use]
pub(in crate::db::executor) fn lowered_index_prefix_family_has_ready_generation(
    store: StoreHandle,
    database_incarnation: DatabaseIncarnationId,
    accepted_root: CardinalityAcceptedRootIdentity,
    specs: &[LoweredIndexPrefixSpec],
) -> bool {
    let Some(first) = specs.first() else {
        return false;
    };
    let Some(index_id) = exact_cardinality_index_id_from_lowered_spec(first) else {
        return false;
    };
    if first.prefix_components().is_empty() {
        return false;
    }
    for spec in &specs[1..] {
        if spec.prefix_components().is_empty()
            || exact_cardinality_index_id_from_lowered_spec(spec) != Some(index_id)
        {
            return false;
        }
    }
    let data_generation = store.with_data(DataStore::generation);
    store.user_index_prefix_family_has_ready_generation(
        database_incarnation,
        accepted_root,
        data_generation,
        IndexKeyKind::User,
        index_id,
        specs.iter().map(LoweredIndexPrefixSpec::prefix_components),
    )
}

/// Expand each exact parent prefix by one metadata-proven child slot.
///
/// This is the shared runtime side of the sparse prefix-family route contract:
/// route planning proves that one exact child slot makes the remaining index
/// suffix match primary-key order, and this helper enumerates those child
/// prefixes only when synchronized cardinality metadata can prove the complete
/// bounded child set.
pub(in crate::db::executor) fn expand_index_prefix_family_with_exact_child_prefixes(
    store: StoreHandle,
    entity_tag: EntityTag,
    index: &IndexShapeDetails,
    specs: &[LoweredIndexPrefixSpec],
    expansion: IndexPrefixChildExpansionHint,
) -> Result<Option<ExpandedIndexPrefixFamily>, InternalError> {
    if index.slot_arity().saturating_add(1) != expansion.target_prefix_len() {
        return Err(InternalError::query_executor_invariant());
    }
    if expansion.target_prefix_len() >= index.key_arity() {
        return Err(InternalError::query_executor_invariant());
    }

    let total_cap = expansion.max_child_prefixes();
    if total_cap == 0 {
        return Ok(None);
    }

    let data_generation = store.with_data(DataStore::generation);
    let index_id =
        IndexId::new_with_generation(entity_tag, index.ordinal(), index.physical_generation());

    for spec in specs {
        if spec.prefix_components().len().saturating_add(1) != expansion.target_prefix_len() {
            return Err(InternalError::query_executor_invariant());
        }
    }

    let Some(child_prefixes) = store.exact_user_index_child_prefixes_for_parent_set(
        data_generation,
        index_id,
        specs.iter().map(LoweredIndexPrefixSpec::prefix_components),
        total_cap,
    ) else {
        return Ok(None);
    };

    let mut expanded_specs = Vec::with_capacity(child_prefixes.len());
    for child_prefix in child_prefixes {
        expanded_specs.push(LoweredIndexPrefixSpec::from_raw_component_prefix(
            entity_tag,
            index.index_contract(),
            IndexKeyKind::User,
            child_prefix,
        )?);
    }

    Ok(Some(ExpandedIndexPrefixFamily::new(
        index,
        expansion.target_prefix_len(),
        expanded_specs,
    )))
}

pub(in crate::db) fn user_index_prefix_cardinality_keys_from_plan(
    plan: LoweredIndexPrefixCardinalityPlan<'_>,
) -> Option<Vec<UserIndexPrefixCardinalityKey>> {
    let prefix_len = plan.prefix_len();
    let mut keys = Vec::with_capacity(plan.specs().len());
    for spec in plan.specs() {
        let prefix_components = spec.prefix_components().get(..prefix_len)?.to_vec();
        keys.push(UserIndexPrefixCardinalityKey::new(
            plan.index_id(),
            prefix_components,
        ));
    }

    (!keys.is_empty()).then_some(keys)
}

fn user_index_prefix_cardinality_key_from_lowered_spec(
    spec: &LoweredIndexPrefixSpec,
    prefix_len: usize,
) -> Option<UserIndexPrefixCardinalityKey> {
    if prefix_len == 0 {
        return None;
    }

    let index_id = exact_cardinality_index_id_from_lowered_spec(spec)?;
    let prefix_components = spec.prefix_components().get(..prefix_len)?.to_vec();

    Some(UserIndexPrefixCardinalityKey::new(
        index_id,
        prefix_components,
    ))
}

pub(in crate::db) fn exact_count_cardinality_prefixes_for_plan<'specs>(
    entity_tag: crate::types::EntityTag,
    plan: &AccessPlannedQuery,
    index_prefix_specs: &'specs [LoweredIndexPrefixSpec],
    allow_ordered_plan: bool,
) -> Option<LoweredIndexPrefixCardinalityPlan<'specs>> {
    // COUNT page windows only need exact candidate cardinality. ORDER BY
    // affects row identity, but not COUNT window size once residual filtering
    // and DISTINCT are already ruled out. EXISTS keeps ordered plans on the
    // kernel path because missing-row policy and stale index entries can affect
    // which candidate first proves existence.
    if !plan.has_no_distinct()
        || (!allow_ordered_plan && plan.scalar_plan().order.is_some())
        || plan.has_any_residual_filter()
    {
        return None;
    }

    let path = plan.access.as_path()?;
    let contract = cardinality_prefix_contract_for_path(path)?;

    exact_cardinality_plan_from_lowered_specs(
        entity_tag,
        index_prefix_specs,
        contract.index_ordinal,
        contract.physical_generation,
        contract.expected_prefix_specs,
        contract.prefix_len,
    )
}

#[derive(Clone, Copy)]
struct CardinalityPrefixContract {
    index_ordinal: u16,
    physical_generation: u64,
    expected_prefix_specs: usize,
    prefix_len: usize,
}

fn cardinality_prefix_contract_for_path(
    path: &AccessPath<Value>,
) -> Option<CardinalityPrefixContract> {
    if let Some((index, values)) = path.as_index_prefix_contract() {
        return Some(CardinalityPrefixContract {
            index_ordinal: index.ordinal(),
            physical_generation: index.physical_generation(),
            expected_prefix_specs: 1,
            prefix_len: values.len(),
        });
    }
    if let Some((index, values)) = path.as_index_multi_lookup_contract() {
        return Some(CardinalityPrefixContract {
            index_ordinal: index.ordinal(),
            physical_generation: index.physical_generation(),
            expected_prefix_specs: values.len(),
            prefix_len: 1,
        });
    }
    if let Some(spec) = path.as_index_branch_set_spec() {
        return Some(CardinalityPrefixContract {
            index_ordinal: spec.index_ref().ordinal(),
            physical_generation: spec.index_ref().physical_generation(),
            expected_prefix_specs: spec.branch_count(),
            prefix_len: spec.branch_prefix_len(),
        });
    }

    None
}

fn exact_cardinality_plan_from_lowered_specs(
    entity_tag: crate::types::EntityTag,
    specs: &[LoweredIndexPrefixSpec],
    index_ordinal: u16,
    physical_generation: u64,
    expected_prefix_specs: usize,
    prefix_len: usize,
) -> Option<LoweredIndexPrefixCardinalityPlan<'_>> {
    if prefix_len == 0 || specs.len() != expected_prefix_specs {
        return None;
    }
    for spec in specs {
        spec.prefix_components().get(..prefix_len)?;
    }

    Some(LoweredIndexPrefixCardinalityPlan {
        index_id: IndexId::new_with_generation(entity_tag, index_ordinal, physical_generation),
        prefix_len,
        specs,
    })
}

fn exact_cardinality_index_id_from_lowered_spec(spec: &LoweredIndexPrefixSpec) -> Option<IndexId> {
    if let Some((index_id, key_kind)) = spec.deferred_cardinality_source() {
        return (key_kind == IndexKeyKind::User).then_some(index_id);
    }
    if spec.prefix_components().is_empty() {
        return None;
    }

    let Ok(Bound::Included(raw_key)) = spec.lower() else {
        return None;
    };
    let key = IndexKey::try_from_raw(raw_key).ok()?;
    if key.key_kind() != IndexKeyKind::User
        || key.component_count() < spec.prefix_components().len()
    {
        return None;
    }

    Some(*key.index_id())
}

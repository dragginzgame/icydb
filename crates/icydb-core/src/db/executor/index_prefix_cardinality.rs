//! Module: executor::index_prefix_cardinality
//! Responsibility: executor-local probes over lowered index-prefix metadata.
//! Does not own: count planning or index metadata maintenance.
//! Boundary: fail-open helpers for runtime branch pruning.

#[cfg(feature = "sql")]
use crate::db::access::LoweredIndexPrefixCardinalitySpec;
use crate::{
    db::{
        access::{AccessPath, IndexShapeDetails, LoweredIndexPrefixSpec},
        data::DataStore,
        executor::route::IndexPrefixChildExpansionHint,
        index::{IndexId, IndexKey, IndexKeyKind, IndexStore},
        query::plan::AccessPlannedQuery,
        registry::StoreHandle,
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

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::db::executor) struct LoweredIndexPrefixCardinalityKey {
    index_id: IndexId,
    prefix_components: Vec<Vec<u8>>,
}

impl LoweredIndexPrefixCardinalityKey {
    #[must_use]
    pub(in crate::db::executor) const fn index_id(&self) -> IndexId {
        self.index_id
    }

    #[must_use]
    pub(in crate::db::executor) const fn prefix_components(&self) -> &[Vec<u8>] {
        self.prefix_components.as_slice()
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
    #[cfg(feature = "sql")]
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
    let data_generation = store.with_data(DataStore::generation);
    store.with_index(|index_store| {
        lowered_index_prefix_liveness_at_generation(index_store, data_generation, spec)
    })
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
    let index_id = IndexId::new(entity_tag, index.ordinal());

    for spec in specs {
        if spec.prefix_components().len().saturating_add(1) != expansion.target_prefix_len() {
            return Err(InternalError::query_executor_invariant());
        }
    }

    let Some(child_prefixes) = store.with_index(|index_store| {
        index_store.exact_child_prefixes_for_parent_set(
            data_generation,
            IndexKeyKind::User,
            index_id,
            specs.iter().map(LoweredIndexPrefixSpec::prefix_components),
            total_cap,
        )
    }) else {
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

#[cfg(feature = "sql")]
pub(in crate::db) fn lowered_index_prefix_cardinality_specs_from_plan(
    plan: LoweredIndexPrefixCardinalityPlan<'_>,
) -> Option<Vec<LoweredIndexPrefixCardinalitySpec>> {
    let prefix_len = plan.prefix_len();
    let mut specs = Vec::with_capacity(plan.specs().len());
    for spec in plan.specs() {
        let prefix_components = spec.prefix_components().get(..prefix_len)?.to_vec();
        specs.push(LoweredIndexPrefixCardinalitySpec::new(
            plan.index_id(),
            prefix_components,
        ));
    }

    (!specs.is_empty()).then_some(specs)
}

pub(in crate::db::executor) fn lowered_index_prefix_liveness_at_generation(
    index_store: &IndexStore,
    data_generation: u64,
    spec: &LoweredIndexPrefixSpec,
) -> IndexBranchLiveness {
    let Some(cardinality_key) =
        lowered_index_prefix_cardinality_key(spec, spec.prefix_components().len())
    else {
        return IndexBranchLiveness::UnknownConservative(
            IndexBranchUnknownReason::MissingPrefixCardinalityKey,
        );
    };

    match index_store.exact_prefix_cardinality(
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

pub(in crate::db::executor) fn lowered_index_prefix_cardinality_key(
    spec: &LoweredIndexPrefixSpec,
    prefix_len: usize,
) -> Option<LoweredIndexPrefixCardinalityKey> {
    if prefix_len == 0 {
        return None;
    }

    let index_id = exact_cardinality_index_id_from_lowered_spec(spec)?;
    let prefix_components = spec.prefix_components().get(..prefix_len)?.to_vec();

    Some(LoweredIndexPrefixCardinalityKey {
        index_id,
        prefix_components,
    })
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
        contract.expected_prefix_specs,
        contract.prefix_len,
    )
}

#[derive(Clone, Copy)]
struct CardinalityPrefixContract {
    index_ordinal: u16,
    expected_prefix_specs: usize,
    prefix_len: usize,
}

fn cardinality_prefix_contract_for_path(
    path: &AccessPath<Value>,
) -> Option<CardinalityPrefixContract> {
    if let Some((index, values)) = path.as_index_prefix_contract() {
        return Some(CardinalityPrefixContract {
            index_ordinal: index.ordinal(),
            expected_prefix_specs: 1,
            prefix_len: values.len(),
        });
    }
    if let Some((index, values)) = path.as_index_multi_lookup_contract() {
        return Some(CardinalityPrefixContract {
            index_ordinal: index.ordinal(),
            expected_prefix_specs: values.len(),
            prefix_len: 1,
        });
    }
    if let Some(spec) = path.as_index_branch_set_spec() {
        return Some(CardinalityPrefixContract {
            index_ordinal: spec.index_ref().ordinal(),
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
        index_id: IndexId::new(entity_tag, index_ordinal),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{
            access::SemanticIndexAccessContract,
            index::{IndexEntryValue, RawIndexStoreKey},
            key_taxonomy::{
                EncodedIndexComponent, EncodedPrimaryKey, IndexStoreKey, PrimaryKeyComponent,
            },
        },
        model::index::IndexModel,
    };

    const LIVENESS_ENTITY: EntityTag = EntityTag::new(0x1951);
    const LIVENESS_INDEX_FIELDS: [&str; 1] = ["collection_id"];
    const LIVENESS_INDEX: IndexModel = IndexModel::generated_with_ordinal(
        3,
        "executor::liveness::collection_id",
        "executor::liveness::Store",
        &LIVENESS_INDEX_FIELDS,
        false,
    );

    fn liveness_index_contract() -> SemanticIndexAccessContract {
        SemanticIndexAccessContract::model_only_from_generated_index(LIVENESS_INDEX)
    }

    fn liveness_spec(component: &[u8], key_kind: IndexKeyKind) -> LoweredIndexPrefixSpec {
        LoweredIndexPrefixSpec::from_raw_component_prefix(
            LIVENESS_ENTITY,
            liveness_index_contract(),
            key_kind,
            vec![component.to_vec()],
        )
        .expect("test prefix spec should lower")
    }

    fn liveness_raw_key(component: &[u8], primary_key: u64) -> RawIndexStoreKey {
        IndexStoreKey::new(
            IndexId::new(LIVENESS_ENTITY, LIVENESS_INDEX.ordinal()),
            vec![EncodedIndexComponent::from_canonical_bytes(
                component.to_vec(),
            )],
            EncodedPrimaryKey::encode(PrimaryKeyComponent::Nat64(primary_key))
                .expect("test primary key should encode"),
        )
        .to_raw()
        .expect("test index key should encode")
    }

    #[test]
    fn index_branch_liveness_reports_possibly_live_for_synchronized_non_empty_prefix() {
        let mut store = IndexStore::init_heap();
        let component = b"live";
        store.insert(liveness_raw_key(component, 1), IndexEntryValue::presence());
        store.mark_prefix_cardinality_data_generation(7);

        assert_eq!(
            lowered_index_prefix_liveness_at_generation(
                &store,
                7,
                &liveness_spec(component, IndexKeyKind::User),
            ),
            IndexBranchLiveness::PossiblyLive,
        );
    }

    #[test]
    fn index_branch_liveness_reports_proven_empty_for_synchronized_zero_prefix() {
        let mut store = IndexStore::init_heap();
        store.mark_prefix_cardinality_data_generation(7);

        assert_eq!(
            lowered_index_prefix_liveness_at_generation(
                &store,
                7,
                &liveness_spec(b"missing", IndexKeyKind::User),
            ),
            IndexBranchLiveness::ProvenEmpty,
        );
    }

    #[test]
    fn index_branch_liveness_keeps_generation_mismatch_conservative() {
        let mut store = IndexStore::init_heap();
        let component = b"live";
        store.insert(liveness_raw_key(component, 1), IndexEntryValue::presence());
        store.mark_prefix_cardinality_data_generation(7);

        assert_eq!(
            lowered_index_prefix_liveness_at_generation(
                &store,
                8,
                &liveness_spec(component, IndexKeyKind::User),
            ),
            IndexBranchLiveness::UnknownConservative(
                IndexBranchUnknownReason::MissingGenerationCompatibleCardinality,
            ),
        );
    }

    #[test]
    fn index_branch_liveness_keeps_non_user_prefixes_conservative() {
        let mut store = IndexStore::init_heap();
        store.mark_prefix_cardinality_data_generation(7);

        assert_eq!(
            lowered_index_prefix_liveness_at_generation(
                &store,
                7,
                &liveness_spec(b"system", IndexKeyKind::System),
            ),
            IndexBranchLiveness::UnknownConservative(
                IndexBranchUnknownReason::MissingPrefixCardinalityKey,
            ),
        );
    }
}

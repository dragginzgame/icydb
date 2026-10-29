//! Module: db::access::shape_facts
//! Responsibility: immutable access-shape facts over semantic and executable contracts.
//! Does not own: planner semantics or physical stream execution behavior.
//! Boundary: access-layer shape authority consumed by executor route/load/stream modules.

use crate::db::access::{
    AccessPath, AccessPathKind, AccessPlan, ExecutableAccessNode, ExecutableAccessPlan,
    ExecutionPathPayload, SemanticIndexAccessContract, SemanticIndexKeyItem,
};

// Project whether traversal can safely reverse the underlying access shape.
const fn has_reversible_traversal_shape_for_path_kind(kind: AccessPathKind) -> bool {
    !matches!(kind, AccessPathKind::ByKeys)
}

///
/// SinglePathAccessShapeFacts
///
/// Access-shape fact snapshot for one executable access path.
/// This projects one passive execution descriptor into immutable structural
/// data so downstream layers can derive their own route policy without
/// re-matching raw access variants.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct SinglePathAccessShapeFacts {
    kind: AccessPathKind,
    is_by_keys_empty: bool,
    index_prefix_details: Option<IndexShapeDetails>,
    index_range_details: Option<IndexShapeDetails>,
    index_key_items_for_slot_map: Option<IndexShapeDetails>,
    index_prefix_spec_count: usize,
    consumes_index_range_spec: bool,
}

impl SinglePathAccessShapeFacts {
    /// Return the coarse access-path kind represented by this shape snapshot.
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> AccessPathKind {
        self.kind
    }

    #[must_use]
    pub(in crate::db) const fn has_reversible_traversal_shape(&self) -> bool {
        has_reversible_traversal_shape_for_path_kind(self.kind)
    }

    #[must_use]
    pub(in crate::db) fn index_prefix_details(&self) -> Option<IndexShapeDetails> {
        self.index_prefix_details.clone()
    }

    #[must_use]
    pub(in crate::db) fn index_range_details(&self) -> Option<IndexShapeDetails> {
        self.index_range_details.clone()
    }

    #[must_use]
    pub(in crate::db) fn index_key_items_for_slot_map(&self) -> Option<IndexShapeDetails> {
        self.index_key_items_for_slot_map.clone()
    }

    #[must_use]
    pub(in crate::db) const fn index_prefix_spec_count(&self) -> usize {
        self.index_prefix_spec_count
    }

    #[must_use]
    pub(in crate::db) const fn consumes_index_range_spec(&self) -> bool {
        self.consumes_index_range_spec
    }
}

///
/// IndexShapeDetails
///
/// Named shape details for one index-backed path shape.
/// Carries index identity together with slot arity to avoid tuple-position drift.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct IndexShapeDetails {
    index: SemanticIndexAccessContract,
    slot_arity: usize,
}

impl IndexShapeDetails {
    #[must_use]
    pub(in crate::db) const fn from_access_contract(
        index: SemanticIndexAccessContract,
        slot_arity: usize,
    ) -> Self {
        Self { index, slot_arity }
    }

    #[must_use]
    pub(in crate::db) fn name(&self) -> &str {
        self.index.name()
    }

    #[must_use]
    pub(in crate::db) fn index_contract(&self) -> SemanticIndexAccessContract {
        self.index.clone()
    }

    #[must_use]
    pub(in crate::db) fn ordinal(&self) -> u16 {
        self.index.ordinal()
    }

    /// Return the isolated physical key generation.
    #[must_use]
    pub(in crate::db) fn physical_generation(&self) -> u64 {
        self.index.physical_generation()
    }

    #[must_use]
    pub(in crate::db) fn is_unique(&self) -> bool {
        self.index.is_unique()
    }

    #[must_use]
    pub(in crate::db) fn key_items(&self) -> &[SemanticIndexKeyItem] {
        self.index.key_items()
    }

    #[must_use]
    pub(in crate::db) fn key_arity(&self) -> usize {
        self.index.key_arity()
    }

    #[must_use]
    pub(in crate::db) fn key_field_at(&self, component_index: usize) -> Option<&str> {
        self.index.key_field_at(component_index)
    }

    #[must_use]
    pub(in crate::db) fn first_key_field(&self) -> Option<&str> {
        self.key_field_at(0)
    }

    #[must_use]
    pub(in crate::db) const fn slot_arity(&self) -> usize {
        self.slot_arity
    }

    #[must_use]
    pub(in crate::db) fn with_slot_arity(&self, slot_arity: usize) -> Self {
        Self {
            index: self.index.clone(),
            slot_arity,
        }
    }
}

///
/// AccessShapeFacts
///
/// Access-shape facts for one semantic or executable access plan.
/// This captures plan-level structural flags and single-path facts while
/// leaving route, aggregate, and fetch-hint policy outside the access layer.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AccessShapeFacts {
    single_path: Option<SinglePathAccessShapeFacts>,
    first_index_range_details: Option<IndexShapeDetails>,
    all_paths_support_reverse_traversal: bool,
}

impl AccessShapeFacts {
    /// Borrow the single-path fact snapshot when this access plan is one path.
    #[must_use]
    pub(in crate::db) fn single_path_facts(&self) -> Option<SinglePathAccessShapeFacts> {
        self.single_path.clone()
    }

    #[must_use]
    pub(in crate::db) const fn is_single_path(&self) -> bool {
        self.single_path.is_some()
    }

    #[must_use]
    pub(in crate::db) fn first_index_range_details(&self) -> Option<IndexShapeDetails> {
        self.first_index_range_details.clone()
    }

    #[must_use]
    pub(in crate::db) const fn is_composite(&self) -> bool {
        self.single_path.is_none()
    }

    #[must_use]
    pub(in crate::db) const fn all_paths_support_reverse_traversal(&self) -> bool {
        self.all_paths_support_reverse_traversal
    }

    #[must_use]
    pub(in crate::db) const fn has_selected_index_access_path(&self) -> bool {
        match &self.single_path {
            Some(path) => path.index_prefix_details.is_some() || path.index_range_details.is_some(),
            None => false,
        }
    }

    #[must_use]
    pub(in crate::db) const fn has_single_path_index_range_access_path(&self) -> bool {
        match &self.single_path {
            Some(path) => path.index_range_details.is_some(),
            None => false,
        }
    }

    #[must_use]
    pub(in crate::db) fn single_path_index_prefix_details(&self) -> Option<IndexShapeDetails> {
        match &self.single_path {
            Some(path) => path.index_prefix_details(),
            None => None,
        }
    }

    #[must_use]
    pub(in crate::db) fn single_path_index_range_details(&self) -> Option<IndexShapeDetails> {
        match &self.single_path {
            Some(path) => path.index_range_details(),
            None => None,
        }
    }
}

const fn is_by_keys_empty_from_payload<K>(payload: &ExecutionPathPayload<'_, K>) -> bool {
    matches!(payload, ExecutionPathPayload::ByKeys(keys) if keys.is_empty())
}

const fn index_prefix_spec_count_from_payload<K>(payload: &ExecutionPathPayload<'_, K>) -> usize {
    match payload {
        ExecutionPathPayload::IndexPrefix { .. } => 1,
        ExecutionPathPayload::IndexMultiLookup { value_count, .. } => *value_count,
        ExecutionPathPayload::IndexBranchSet { branch_count, .. } => *branch_count,
        ExecutionPathPayload::ByKey(_)
        | ExecutionPathPayload::ByKeys(_)
        | ExecutionPathPayload::KeyRange { .. }
        | ExecutionPathPayload::IndexRange { .. }
        | ExecutionPathPayload::FullScan => 0,
    }
}

fn derive_single_path_access_shape_facts_from_inputs(
    kind: AccessPathKind,
    is_by_keys_empty: bool,
    index_prefix_details: Option<IndexShapeDetails>,
    index_range_details: Option<IndexShapeDetails>,
    index_prefix_spec_count: usize,
) -> SinglePathAccessShapeFacts {
    let index_key_items_for_slot_map = match (&index_prefix_details, &index_range_details) {
        (Some(details), None) | (None, Some(details)) => Some(details.clone()),
        (None, None) => None,
        (Some(prefix_details), Some(_)) => Some(prefix_details.clone()),
    };
    let consumes_index_range_spec = index_range_details.is_some();

    SinglePathAccessShapeFacts {
        kind,
        is_by_keys_empty,
        index_prefix_details,
        index_range_details,
        index_key_items_for_slot_map,
        index_prefix_spec_count,
        consumes_index_range_spec,
    }
}

/// Derive immutable access-shape facts for one executable access path.
#[must_use]
fn derive_access_path_shape_facts<K>(
    path: &ExecutionPathPayload<'_, K>,
) -> SinglePathAccessShapeFacts {
    // Phase 1: derive fact projection from execution-path shape.
    let kind = path.kind();

    // Phase 2: derive payload-dependent shape metadata.
    let index_prefix_details = path.index_prefix_details();
    let index_range_details = path.index_range_details();

    derive_single_path_access_shape_facts_from_inputs(
        kind,
        is_by_keys_empty_from_payload(path),
        index_prefix_details,
        index_range_details,
        index_prefix_spec_count_from_payload(path),
    )
}

/// Derive immutable access-shape facts for one semantic access path.
#[must_use]
fn derive_semantic_access_path_shape_facts<K>(path: &AccessPath<K>) -> SinglePathAccessShapeFacts {
    let payload = ExecutionPathPayload::from_access_path(path);

    derive_access_path_shape_facts(&payload)
}

// Both tree representations project the same passive path facts.
fn summarize_path_runtime_shape(
    facts: &SinglePathAccessShapeFacts,
) -> (Option<IndexShapeDetails>, bool) {
    (
        facts.index_range_details(),
        facts.has_reversible_traversal_shape(),
    )
}

// Preserve the first available range detail while visiting every child's reverse flag.
fn summarize_child_runtime_shapes(
    children: impl Iterator<Item = (Option<IndexShapeDetails>, bool)>,
) -> (Option<IndexShapeDetails>, bool) {
    let mut first_range = None;
    let mut all_reverse = true;
    for (range, reverse) in children {
        if first_range.is_none() {
            first_range = range;
        }
        all_reverse &= reverse;
    }
    (first_range, all_reverse)
}

fn summarize_access_plan_runtime_shape<K>(
    access: &ExecutableAccessPlan<'_, K>,
) -> (Option<IndexShapeDetails>, bool) {
    match access.node() {
        ExecutableAccessNode::Path(path) => summarize_path_runtime_shape(&path.shape_facts()),
        ExecutableAccessNode::Union(children) | ExecutableAccessNode::Intersection(children) => {
            summarize_child_runtime_shapes(children.iter().map(summarize_access_plan_runtime_shape))
        }
    }
}

fn summarize_semantic_access_plan_runtime_shape<K>(
    access: &AccessPlan<K>,
) -> (Option<IndexShapeDetails>, bool) {
    match access {
        AccessPlan::Path(path) => summarize_path_runtime_shape(&path.shape_facts()),
        AccessPlan::Union(children) | AccessPlan::Intersection(children) => {
            summarize_child_runtime_shapes(
                children
                    .iter()
                    .map(summarize_semantic_access_plan_runtime_shape),
            )
        }
    }
}

/// Derive immutable access-shape facts for one executable access plan.
#[must_use]
fn derive_access_shape_facts<K>(access: &ExecutableAccessPlan<'_, K>) -> AccessShapeFacts {
    let single_path = match access.node() {
        ExecutableAccessNode::Path(path) => Some(path.shape_facts()),
        ExecutableAccessNode::Union(_) | ExecutableAccessNode::Intersection(_) => None,
    };
    let (first_index_range_details, all_paths_support_reverse_traversal) =
        summarize_access_plan_runtime_shape(access);

    AccessShapeFacts {
        single_path,
        first_index_range_details,
        all_paths_support_reverse_traversal,
    }
}

/// Derive immutable access-shape facts for one semantic access plan.
#[must_use]
fn derive_semantic_access_shape_facts<K>(access: &AccessPlan<K>) -> AccessShapeFacts {
    let single_path = match access {
        AccessPlan::Path(path) => Some(path.shape_facts()),
        AccessPlan::Union(_) | AccessPlan::Intersection(_) => None,
    };
    let (first_index_range_details, all_paths_support_reverse_traversal) =
        summarize_semantic_access_plan_runtime_shape(access);

    AccessShapeFacts {
        single_path,
        first_index_range_details,
        all_paths_support_reverse_traversal,
    }
}

impl<K> AccessPath<K> {
    /// Project immutable access-shape facts for this semantic access path.
    #[must_use]
    pub(in crate::db) fn shape_facts(&self) -> SinglePathAccessShapeFacts {
        derive_semantic_access_path_shape_facts(self)
    }
}

impl<K> AccessPlan<K> {
    /// Project immutable access-shape facts for this semantic access plan.
    #[must_use]
    pub(in crate::db) fn shape_facts(&self) -> AccessShapeFacts {
        derive_semantic_access_shape_facts(self)
    }
}

impl<K> ExecutionPathPayload<'_, K> {
    /// Project immutable access-shape facts for this executable access path.
    #[must_use]
    pub(in crate::db) fn shape_facts(&self) -> SinglePathAccessShapeFacts {
        derive_access_path_shape_facts(self)
    }
}

impl<K> ExecutableAccessPlan<'_, K> {
    /// Project immutable access-shape facts for this executable access plan.
    #[must_use]
    pub(in crate::db) fn shape_facts(&self) -> AccessShapeFacts {
        derive_access_shape_facts(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::access::path::SemanticIndexAccessContractInner;
    use std::sync::Arc;

    fn detail(name: &str) -> IndexShapeDetails {
        IndexShapeDetails::from_access_contract(
            SemanticIndexAccessContract {
                inner: Arc::new(SemanticIndexAccessContractInner {
                    ordinal: 0,
                    physical_generation: 1,
                    name: name.into(),
                    store_path: "test".into(),
                    key_items: vec![SemanticIndexKeyItem::Field("field".into())],
                    unique: false,
                    predicate_semantics: None,
                }),
            },
            1,
        )
    }

    #[test]
    fn child_summary_keeps_first_range_and_checks_later_reverse_flags() {
        let first = detail("first");
        let second = detail("second");
        assert_eq!(
            summarize_child_runtime_shapes(std::iter::empty()),
            (None, true)
        );
        assert_eq!(
            summarize_child_runtime_shapes(
                [
                    (None, true),
                    (Some(first.clone()), true),
                    (Some(second), false),
                ]
                .into_iter()
            ),
            (Some(first), false)
        );
    }

    #[test]
    fn semantic_and_executable_summaries_agree_for_empty_and_nested_trees() {
        for (plan, reverse) in [
            (AccessPlan::<u64>::Union(vec![]), true),
            (AccessPlan::Intersection(vec![]), true),
            (AccessPlan::full_scan(), true),
            (AccessPlan::by_keys(vec![1, 2]), false),
            (
                AccessPlan::Union(vec![
                    AccessPlan::full_scan(),
                    AccessPlan::Intersection(vec![AccessPlan::by_keys(vec![1])]),
                ]),
                false,
            ),
            (
                AccessPlan::Intersection(vec![AccessPlan::full_scan(), AccessPlan::Union(vec![])]),
                true,
            ),
        ] {
            let semantic = plan.shape_facts();
            let executable = plan.executable_contract().shape_facts();
            assert_eq!(semantic, executable);
            assert_eq!(semantic.all_paths_support_reverse_traversal(), reverse);
        }
    }
}

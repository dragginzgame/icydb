//! Module: db::access::shape_facts
//! Responsibility: immutable access-shape facts over semantic and executable contracts.
//! Does not own: planner semantics or physical stream execution behavior.
//! Boundary: access-layer shape authority consumed by executor route/load/stream modules.

use crate::db::access::{
    AccessPath, AccessPathKind, AccessPlan, ExecutableAccessNode, ExecutableAccessPlan,
    ExecutionPathPayload, SemanticIndexAccessContract, SemanticIndexKeyItemsRef,
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
    pub(in crate::db) const fn is_by_keys_empty(&self) -> bool {
        self.is_by_keys_empty
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
    pub(in crate::db) fn ordinal(&self) -> u16 {
        self.index.ordinal()
    }

    #[must_use]
    pub(in crate::db) fn is_unique(&self) -> bool {
        self.index.is_unique()
    }

    #[must_use]
    pub(in crate::db) fn key_items(&self) -> SemanticIndexKeyItemsRef<'_> {
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

fn summarize_access_plan_runtime_shape<K>(
    access: &ExecutableAccessPlan<'_, K>,
) -> (Option<IndexShapeDetails>, bool) {
    match access.node() {
        ExecutableAccessNode::Path(path) => {
            let shape_facts = path.shape_facts();

            (
                shape_facts.index_range_details(),
                shape_facts.has_reversible_traversal_shape(),
            )
        }
        ExecutableAccessNode::Union(children) | ExecutableAccessNode::Intersection(children) => {
            let mut first_index_range_details = None;
            let mut all_paths_support_reverse_traversal = true;
            for child in children {
                let (child_index_range_details, child_reverse_supported) =
                    summarize_access_plan_runtime_shape(child);

                if first_index_range_details.is_none() {
                    first_index_range_details = child_index_range_details;
                }
                all_paths_support_reverse_traversal &= child_reverse_supported;
            }

            (
                first_index_range_details,
                all_paths_support_reverse_traversal,
            )
        }
    }
}

fn summarize_semantic_access_plan_runtime_shape<K>(
    access: &AccessPlan<K>,
) -> (Option<IndexShapeDetails>, bool) {
    match access {
        AccessPlan::Path(path) => {
            let shape_facts = path.shape_facts();

            (
                shape_facts.index_range_details(),
                shape_facts.has_reversible_traversal_shape(),
            )
        }
        AccessPlan::Union(children) | AccessPlan::Intersection(children) => {
            let mut first_index_range_details = None;
            let mut all_paths_support_reverse_traversal = true;
            for child in children {
                let (child_index_range_details, child_reverse_supported) =
                    summarize_semantic_access_plan_runtime_shape(child);

                if first_index_range_details.is_none() {
                    first_index_range_details = child_index_range_details;
                }
                all_paths_support_reverse_traversal &= child_reverse_supported;
            }

            (
                first_index_range_details,
                all_paths_support_reverse_traversal,
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

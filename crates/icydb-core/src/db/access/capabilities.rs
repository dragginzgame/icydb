//! Module: db::access::capabilities
//! Responsibility: access-shape capability facts over semantic and executable contracts.
//! Does not own: planner semantics or physical stream execution behavior.
//! Boundary: access-layer shape authority consumed by executor route/load/stream modules.

use crate::{
    db::access::{
        AccessPath, AccessPathKind, AccessPlan, ExecutableAccessNode, ExecutableAccessPlan,
        ExecutionPathPayload,
    },
    model::index::IndexModel,
};

// Project primary-key stream-window shape from the executable path kind.
const fn has_primary_key_stream_window_for_path_kind(kind: AccessPathKind) -> bool {
    matches!(kind, AccessPathKind::KeyRange | AccessPathKind::FullScan)
}

// Project whether traversal can safely reverse the underlying access shape.
const fn has_reversible_traversal_shape_for_path_kind(kind: AccessPathKind) -> bool {
    !matches!(kind, AccessPathKind::ByKeys)
}

// Project whether COUNT can use a direct structural pushdown for this shape.
const fn has_count_pushdown_shape_for_path_kind(kind: AccessPathKind) -> bool {
    matches!(kind, AccessPathKind::KeyRange | AccessPathKind::FullScan)
}

// Project whether this shape can use a primary-scan fetch hint.
const fn has_primary_scan_fetch_hint_shape_for_path_kind(kind: AccessPathKind) -> bool {
    matches!(
        kind,
        AccessPathKind::ByKey | AccessPathKind::KeyRange | AccessPathKind::FullScan
    )
}

// Project whether the path directly addresses primary keys.
const fn is_key_direct_access_for_path_kind(kind: AccessPathKind) -> bool {
    matches!(kind, AccessPathKind::ByKey | AccessPathKind::ByKeys)
}

///
/// SinglePathAccessCapabilities
///
/// Runtime shape-fact snapshot for one executable access path.
/// This projects one passive execution descriptor into immutable capability
/// data so route/load/stream helpers consume one access-owned fact surface.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct SinglePathAccessCapabilities {
    kind: AccessPathKind,
    is_by_keys_empty: bool,
    index_prefix_details: Option<IndexShapeDetails>,
    index_range_details: Option<IndexShapeDetails>,
    index_fields_for_slot_map: Option<&'static [&'static str]>,
    index_prefix_spec_count: usize,
    consumes_index_range_spec: bool,
}

impl SinglePathAccessCapabilities {
    /// Return whether this path can produce an ordered key-stream window directly.
    #[must_use]
    pub(in crate::db) const fn has_ordered_key_stream_window(&self) -> bool {
        matches!(
            self.kind,
            AccessPathKind::ByKey
                | AccessPathKind::ByKeys
                | AccessPathKind::IndexPrefix
                | AccessPathKind::IndexMultiLookup
                | AccessPathKind::IndexRange
        )
    }

    /// Return the primary-key cardinality fact exposed by this access shape.
    #[must_use]
    pub(in crate::db) const fn primary_key_cardinality(
        &self,
    ) -> Option<PrimaryKeyCardinalityShape> {
        if self.has_primary_key_stream_window() {
            Some(PrimaryKeyCardinalityShape::PrimaryKeyWindow)
        } else {
            None
        }
    }

    /// Return whether this path can count existing primary-key stream rows directly.
    #[must_use]
    pub(in crate::db) const fn has_direct_primary_key_lookup(&self) -> bool {
        matches!(self.kind, AccessPathKind::ByKey | AccessPathKind::ByKeys)
    }

    /// Return whether this path requires one top-N lookahead row in unpaged mode.
    #[must_use]
    pub(in crate::db) const fn requires_top_n_seek_lookahead(&self) -> bool {
        matches!(
            self.kind,
            AccessPathKind::ByKeys | AccessPathKind::IndexMultiLookup
        )
    }

    /// Return whether numeric field aggregates can safely use one direct
    /// key-stream fold in unpaged mode.
    #[must_use]
    pub(in crate::db) const fn has_streaming_numeric_fold_shape(&self) -> bool {
        matches!(
            self.kind,
            AccessPathKind::ByKey
                | AccessPathKind::ByKeys
                | AccessPathKind::FullScan
                | AccessPathKind::KeyRange
                | AccessPathKind::IndexPrefix
                | AccessPathKind::IndexRange
        )
    }

    /// Return whether numeric field aggregates can safely use one direct
    /// key-stream fold for paged primary-key-ordered windows.
    #[must_use]
    pub(in crate::db) const fn has_paged_primary_key_numeric_fold_shape(&self) -> bool {
        matches!(
            self.kind,
            AccessPathKind::ByKey
                | AccessPathKind::ByKeys
                | AccessPathKind::FullScan
                | AccessPathKind::KeyRange
        )
    }

    /// Return whether this path is a primary-key stream-window shape.
    /// This does not imply the emitted stream is guaranteed PK-ordered.
    #[must_use]
    pub(in crate::db) const fn has_primary_key_stream_window(&self) -> bool {
        has_primary_key_stream_window_for_path_kind(self.kind)
    }

    #[must_use]
    pub(in crate::db) const fn has_count_pushdown_shape(&self) -> bool {
        has_count_pushdown_shape_for_path_kind(self.kind)
    }

    #[must_use]
    pub(in crate::db) const fn has_primary_scan_fetch_hint_shape(&self) -> bool {
        has_primary_scan_fetch_hint_shape_for_path_kind(self.kind)
    }

    #[must_use]
    pub(in crate::db) const fn has_reversible_traversal_shape(&self) -> bool {
        has_reversible_traversal_shape_for_path_kind(self.kind)
    }

    #[must_use]
    pub(in crate::db) const fn is_key_direct_access(&self) -> bool {
        is_key_direct_access_for_path_kind(self.kind)
    }

    #[must_use]
    pub(in crate::db) const fn is_by_keys_empty(&self) -> bool {
        self.is_by_keys_empty
    }

    #[must_use]
    pub(in crate::db) const fn index_prefix_details(&self) -> Option<IndexShapeDetails> {
        self.index_prefix_details
    }

    #[must_use]
    pub(in crate::db) const fn index_range_details(&self) -> Option<IndexShapeDetails> {
        self.index_range_details
    }

    #[must_use]
    pub(in crate::db) const fn index_fields_for_slot_map(&self) -> Option<&'static [&'static str]> {
        self.index_fields_for_slot_map
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
/// Named shape details for one index-backed path capability.
/// Carries index identity together with slot arity to avoid tuple-position drift.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct IndexShapeDetails {
    index: IndexModel,
    slot_arity: usize,
}

impl IndexShapeDetails {
    #[must_use]
    pub(in crate::db) const fn new(index: IndexModel, slot_arity: usize) -> Self {
        Self { index, slot_arity }
    }

    #[must_use]
    pub(in crate::db) const fn index(self) -> IndexModel {
        self.index
    }

    #[must_use]
    pub(in crate::db) const fn slot_arity(self) -> usize {
        self.slot_arity
    }
}

///
/// PrimaryKeyCardinalityShape
///
/// Access-owned primary-key cardinality fact for one executable path.
/// Executor route policy consumes this as structural evidence and decides
/// whether a terminal may turn it into a cardinality shortcut.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum PrimaryKeyCardinalityShape {
    PrimaryKeyWindow,
}

///
/// AccessCapabilities
///
/// Access-shape capability descriptor for one semantic or executable access plan.
/// This captures both plan-level shape flags and single-path capabilities so
/// downstream helpers do not branch on raw access-plan structure repeatedly.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct AccessCapabilities {
    single_path: Option<SinglePathAccessCapabilities>,
    first_index_range_details: Option<IndexShapeDetails>,
    all_paths_support_reverse_traversal: bool,
}

impl AccessCapabilities {
    /// Borrow the single-path capability snapshot when this access plan is one path.
    #[must_use]
    pub(in crate::db) const fn single_path_capabilities(
        &self,
    ) -> Option<SinglePathAccessCapabilities> {
        self.single_path
    }

    #[must_use]
    pub(in crate::db) const fn is_single_path(&self) -> bool {
        self.single_path.is_some()
    }

    #[must_use]
    pub(in crate::db) const fn first_index_range_details(&self) -> Option<IndexShapeDetails> {
        self.first_index_range_details
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
    pub(in crate::db) const fn single_path_index_prefix_details(
        &self,
    ) -> Option<IndexShapeDetails> {
        match self.single_path {
            Some(path) => path.index_prefix_details(),
            None => None,
        }
    }

    #[must_use]
    pub(in crate::db) const fn single_path_index_range_details(&self) -> Option<IndexShapeDetails> {
        match self.single_path {
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
        ExecutionPathPayload::ByKey(_)
        | ExecutionPathPayload::ByKeys(_)
        | ExecutionPathPayload::KeyRange { .. }
        | ExecutionPathPayload::IndexRange { .. }
        | ExecutionPathPayload::FullScan => 0,
    }
}

const fn derive_capabilities_from_parts(
    kind: AccessPathKind,
    is_by_keys_empty: bool,
    index_prefix_details: Option<IndexShapeDetails>,
    index_range_details: Option<IndexShapeDetails>,
    index_prefix_spec_count: usize,
) -> SinglePathAccessCapabilities {
    let index_fields_for_slot_map = match (index_prefix_details, index_range_details) {
        (Some(details), None) | (None, Some(details)) => Some(details.index().fields()),
        (None, None) => None,
        (Some(prefix_details), Some(_)) => Some(prefix_details.index().fields()),
    };

    SinglePathAccessCapabilities {
        kind,
        is_by_keys_empty,
        index_prefix_details,
        index_range_details,
        index_fields_for_slot_map,
        index_prefix_spec_count,
        consumes_index_range_spec: index_range_details.is_some(),
    }
}

/// Derive immutable runtime capabilities for one executable access path.
#[must_use]
const fn derive_access_path_capabilities<K>(
    path: &ExecutionPathPayload<'_, K>,
) -> SinglePathAccessCapabilities {
    // Phase 1: derive capability projection from execution-path shape.
    let kind = path.kind();

    // Phase 2: derive payload-dependent shape metadata.
    let index_prefix_details = path.index_prefix_details();
    let index_range_details = path.index_range_details();

    derive_capabilities_from_parts(
        kind,
        is_by_keys_empty_from_payload(path),
        index_prefix_details,
        index_range_details,
        index_prefix_spec_count_from_payload(path),
    )
}

/// Derive immutable runtime capabilities for one semantic access path.
#[must_use]
const fn derive_semantic_access_path_capabilities<K>(
    path: &AccessPath<K>,
) -> SinglePathAccessCapabilities {
    let payload = ExecutionPathPayload::from_access_path(path);

    derive_access_path_capabilities(&payload)
}

fn summarize_access_plan_runtime_shape<K>(
    access: &ExecutableAccessPlan<'_, K>,
) -> (Option<IndexShapeDetails>, bool) {
    match access.node() {
        ExecutableAccessNode::Path(path) => {
            let capabilities = path.capabilities();

            (
                capabilities.index_range_details(),
                capabilities.has_reversible_traversal_shape(),
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
            let capabilities = path.capabilities();

            (
                capabilities.index_range_details(),
                capabilities.has_reversible_traversal_shape(),
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

/// Derive immutable runtime access capabilities for one executable access plan.
#[must_use]
fn derive_access_capabilities<K>(access: &ExecutableAccessPlan<'_, K>) -> AccessCapabilities {
    let single_path = match access.node() {
        ExecutableAccessNode::Path(path) => Some(path.capabilities()),
        ExecutableAccessNode::Union(_) | ExecutableAccessNode::Intersection(_) => None,
    };
    let (first_index_range_details, all_paths_support_reverse_traversal) =
        summarize_access_plan_runtime_shape(access);

    AccessCapabilities {
        single_path,
        first_index_range_details,
        all_paths_support_reverse_traversal,
    }
}

/// Derive immutable runtime access capabilities for one semantic access plan.
#[must_use]
fn derive_semantic_access_capabilities<K>(access: &AccessPlan<K>) -> AccessCapabilities {
    let single_path = match access {
        AccessPlan::Path(path) => Some(path.capabilities()),
        AccessPlan::Union(_) | AccessPlan::Intersection(_) => None,
    };
    let (first_index_range_details, all_paths_support_reverse_traversal) =
        summarize_semantic_access_plan_runtime_shape(access);

    AccessCapabilities {
        single_path,
        first_index_range_details,
        all_paths_support_reverse_traversal,
    }
}

impl<K> AccessPath<K> {
    /// Project immutable runtime capabilities for this semantic access path.
    #[must_use]
    pub(in crate::db) const fn capabilities(&self) -> SinglePathAccessCapabilities {
        derive_semantic_access_path_capabilities(self)
    }
}

impl<K> AccessPlan<K> {
    /// Project immutable runtime capabilities for this semantic access plan.
    #[must_use]
    pub(in crate::db) fn capabilities(&self) -> AccessCapabilities {
        derive_semantic_access_capabilities(self)
    }
}

impl<K> ExecutionPathPayload<'_, K> {
    /// Project immutable runtime capabilities for this executable access path.
    #[must_use]
    pub(in crate::db) const fn capabilities(&self) -> SinglePathAccessCapabilities {
        derive_access_path_capabilities(self)
    }
}

impl<K> ExecutableAccessPlan<'_, K> {
    /// Project immutable runtime capabilities for this executable access plan.
    #[must_use]
    pub(in crate::db) fn capabilities(&self) -> AccessCapabilities {
        derive_access_capabilities(self)
    }
}

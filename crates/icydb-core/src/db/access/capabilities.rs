//! Module: db::access::capabilities
//! Responsibility: route-facing capability projection over executable access contracts.
//! Does not own: planner semantics or physical stream execution behavior.
//! Boundary: access-layer capability authority consumed by executor route/load/stream modules.

use crate::{
    db::access::{
        AccessPathKind, ExecutableAccessNode, ExecutableAccessPath, ExecutableAccessPlan,
        ExecutionPathPayload,
    },
    metrics::sink::PlanKind,
    model::index::IndexModel,
};

///
/// AccessScanKind
///
/// Structural scan-family descriptor for executable access shapes.
/// This intentionally captures routing shape only, not policy constraints.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum AccessScanKind {
    Keys,
    Range,
    Index,
    FullScan,
    Composite,
}

///
/// AccessPlanKind
///
/// Canonical runtime discriminant for executable access plans.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum AccessPlanKind {
    Path(AccessPathKind),
    Union,
    Intersection,
}

// Keep path-kind classification local to access capabilities so dispatch stays
// a structural variant projection instead of a route-policy owner.
const fn scan_kind_for_path_kind(kind: AccessPathKind) -> AccessScanKind {
    match kind {
        AccessPathKind::ByKey | AccessPathKind::ByKeys => AccessScanKind::Keys,
        AccessPathKind::KeyRange => AccessScanKind::Range,
        AccessPathKind::IndexPrefix
        | AccessPathKind::IndexMultiLookup
        | AccessPathKind::IndexRange => AccessScanKind::Index,
        AccessPathKind::FullScan => AccessScanKind::FullScan,
    }
}

// Project route-facing PK stream access from the executable path kind.
const fn supports_pk_stream_access_for_path_kind(kind: AccessPathKind) -> bool {
    matches!(kind, AccessPathKind::KeyRange | AccessPathKind::FullScan)
}

// Project whether traversal can safely reverse the underlying access shape.
const fn supports_reverse_traversal_for_path_kind(kind: AccessPathKind) -> bool {
    !matches!(kind, AccessPathKind::ByKeys)
}

// Project whether COUNT can use a direct structural pushdown for this shape.
const fn supports_count_pushdown_shape_for_path_kind(kind: AccessPathKind) -> bool {
    matches!(kind, AccessPathKind::KeyRange | AccessPathKind::FullScan)
}

// Project whether route planning may use a primary-scan fetch hint.
const fn supports_primary_scan_fetch_hint_for_path_kind(kind: AccessPathKind) -> bool {
    matches!(
        kind,
        AccessPathKind::ByKey | AccessPathKind::KeyRange | AccessPathKind::FullScan
    )
}

// Project whether the path directly addresses primary keys.
const fn is_key_direct_access_for_path_kind(kind: AccessPathKind) -> bool {
    matches!(kind, AccessPathKind::ByKey | AccessPathKind::ByKeys)
}

impl AccessPlanKind {
    /// Return one structural scan-family descriptor for this plan kind.
    #[must_use]
    pub(in crate::db) const fn scan_kind(self) -> AccessScanKind {
        match self {
            Self::Path(kind) => scan_kind_for_path_kind(kind),
            Self::Union | Self::Intersection => AccessScanKind::Composite,
        }
    }

    /// Project one plan kind into coarse plan-kind metrics.
    #[must_use]
    pub(in crate::db) const fn metrics_kind(self) -> PlanKind {
        match self.scan_kind() {
            AccessScanKind::Keys => PlanKind::Keys,
            AccessScanKind::Range => PlanKind::Range,
            AccessScanKind::Index => PlanKind::Index,
            AccessScanKind::FullScan | AccessScanKind::Composite => PlanKind::FullScan,
        }
    }
}

///
/// SinglePathAccessCapabilities
///
/// Runtime capability snapshot for one executable access path.
/// This projects one passive execution descriptor into immutable capability
/// data so route/load/stream helpers consume one authority surface.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[expect(clippy::struct_excessive_bools)]
pub(in crate::db) struct SinglePathAccessCapabilities {
    kind: AccessPathKind,
    supports_pk_stream_access: bool,
    supports_reverse_traversal: bool,
    supports_count_pushdown_shape: bool,
    supports_primary_scan_fetch_hint: bool,
    is_key_direct_access: bool,
    is_by_keys_empty: bool,
    index_prefix_details: Option<IndexShapeDetails>,
    index_range_details: Option<IndexShapeDetails>,
    index_fields_for_slot_map: Option<&'static [&'static str]>,
    index_prefix_spec_count: usize,
    consumes_index_range_spec: bool,
}

impl SinglePathAccessCapabilities {
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> AccessPathKind {
        self.kind
    }

    /// Return whether this path supports the `bytes()` PK-store window fast path.
    #[must_use]
    pub(in crate::db) const fn supports_bytes_terminal_primary_key_window(&self) -> bool {
        matches!(
            self.kind,
            AccessPathKind::FullScan | AccessPathKind::KeyRange
        )
    }

    /// Return whether this path supports the `bytes()` ordered-key-stream fast path.
    #[must_use]
    pub(in crate::db) const fn supports_bytes_terminal_ordered_key_stream_window(&self) -> bool {
        matches!(
            self.kind,
            AccessPathKind::ByKey
                | AccessPathKind::ByKeys
                | AccessPathKind::IndexPrefix
                | AccessPathKind::IndexMultiLookup
                | AccessPathKind::IndexRange
        )
    }

    /// Return whether this path supports COUNT cardinality from PK store metadata.
    #[must_use]
    pub(in crate::db) const fn supports_count_terminal_primary_key_cardinality(&self) -> bool {
        self.supports_bytes_terminal_primary_key_window()
    }

    /// Return whether this path supports COUNT over existing PK-key streams.
    #[must_use]
    pub(in crate::db) const fn supports_count_terminal_primary_key_existing_rows(&self) -> bool {
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
    pub(in crate::db) const fn supports_streaming_numeric_fold(&self) -> bool {
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
    pub(in crate::db) const fn supports_streaming_numeric_fold_for_paged_primary_key_window(
        &self,
    ) -> bool {
        matches!(
            self.kind,
            AccessPathKind::ByKey
                | AccessPathKind::ByKeys
                | AccessPathKind::FullScan
                | AccessPathKind::KeyRange
        )
    }

    /// Return true when this path can drive fast-path PK stream access directly.
    /// This does not imply the emitted stream is guaranteed PK-ordered.
    #[must_use]
    pub(in crate::db) const fn supports_pk_stream_access(&self) -> bool {
        self.supports_pk_stream_access
    }

    #[must_use]
    pub(in crate::db) const fn supports_count_pushdown_shape(&self) -> bool {
        self.supports_count_pushdown_shape
    }

    #[must_use]
    pub(in crate::db) const fn supports_primary_scan_fetch_hint(&self) -> bool {
        self.supports_primary_scan_fetch_hint
    }

    #[must_use]
    pub(in crate::db) const fn supports_reverse_traversal(&self) -> bool {
        self.supports_reverse_traversal
    }

    #[must_use]
    pub(in crate::db) const fn is_key_direct_access(&self) -> bool {
        self.is_key_direct_access
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
    pub(in crate::db) const fn index_prefix_model(&self) -> Option<IndexModel> {
        match self.index_prefix_details {
            Some(details) => Some(details.index()),
            None => None,
        }
    }

    #[must_use]
    pub(in crate::db) const fn index_range_model(&self) -> Option<IndexModel> {
        match self.index_range_details {
            Some(details) => Some(details.index()),
            None => None,
        }
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
/// AccessCapabilities
///
/// Route-facing capability descriptor for one executable access plan.
/// This captures both plan-level shape flags and single-path capabilities so
/// route helpers do not branch on raw access-plan structure repeatedly.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct AccessCapabilities {
    plan_kind: AccessPlanKind,
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
        matches!(
            self.plan_kind,
            AccessPlanKind::Union | AccessPlanKind::Intersection
        )
    }

    #[must_use]
    pub(in crate::db) const fn all_paths_support_reverse_traversal(&self) -> bool {
        self.all_paths_support_reverse_traversal
    }

    #[must_use]
    pub(in crate::db) const fn single_path_supports_pk_stream_access(&self) -> bool {
        match self.single_path {
            Some(path) => path.supports_pk_stream_access(),
            None => false,
        }
    }

    #[must_use]
    pub(in crate::db) const fn single_path_supports_count_pushdown_shape(&self) -> bool {
        match self.single_path {
            Some(path) => path.supports_count_pushdown_shape(),
            None => false,
        }
    }

    #[must_use]
    pub(in crate::db) const fn single_path_index_prefix_details(
        &self,
    ) -> Option<(IndexModel, usize)> {
        match self.single_path {
            Some(path) => match path.index_prefix_details() {
                Some(details) => Some((details.index(), details.slot_arity())),
                None => None,
            },
            None => None,
        }
    }

    #[must_use]
    pub(in crate::db) const fn single_path_index_range_details(
        &self,
    ) -> Option<(IndexModel, usize)> {
        match self.single_path {
            Some(path) => match path.index_range_details() {
                Some(details) => Some((details.index(), details.slot_arity())),
                None => None,
            },
            None => None,
        }
    }

    #[must_use]
    pub(in crate::db) const fn has_index_path(&self) -> bool {
        self.single_path_index_prefix_details().is_some()
            || self.single_path_index_range_details().is_some()
    }

    #[must_use]
    pub(in crate::db) const fn prefix_scan(&self) -> bool {
        self.single_path_index_prefix_details().is_some()
    }

    #[must_use]
    pub(in crate::db) const fn range_scan(&self) -> bool {
        self.single_path_index_range_details().is_some()
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

/// Derive immutable runtime capabilities for one executable access path.
#[must_use]
const fn derive_access_path_capabilities<K>(
    path: &ExecutableAccessPath<'_, K>,
) -> SinglePathAccessCapabilities {
    // Phase 1: derive capability projection from execution-path shape.
    let kind = path.kind();

    // Phase 2: derive payload-dependent shape metadata.
    let index_prefix_details = match path.index_prefix_details() {
        Some((index, slot_arity)) => Some(IndexShapeDetails::new(index, slot_arity)),
        None => None,
    };
    let index_range_details = match path.index_range_details() {
        Some((index, slot_arity)) => Some(IndexShapeDetails::new(index, slot_arity)),
        None => None,
    };
    let index_fields_for_slot_map = match (index_prefix_details, index_range_details) {
        (Some(details), None) | (None, Some(details)) => Some(details.index().fields()),
        (None, None) => None,
        (Some(prefix_details), Some(_)) => Some(prefix_details.index().fields()),
    };

    SinglePathAccessCapabilities {
        kind,
        supports_pk_stream_access: supports_pk_stream_access_for_path_kind(kind),
        supports_reverse_traversal: supports_reverse_traversal_for_path_kind(kind),
        supports_count_pushdown_shape: supports_count_pushdown_shape_for_path_kind(kind),
        supports_primary_scan_fetch_hint: supports_primary_scan_fetch_hint_for_path_kind(kind),
        is_key_direct_access: is_key_direct_access_for_path_kind(kind),
        is_by_keys_empty: is_by_keys_empty_from_payload(path.payload()),
        index_prefix_details,
        index_range_details,
        index_fields_for_slot_map,
        index_prefix_spec_count: index_prefix_spec_count_from_payload(path.payload()),
        consumes_index_range_spec: index_range_details.is_some(),
    }
}

fn summarize_access_plan_runtime_shape<K>(
    access: &ExecutableAccessPlan<'_, K>,
) -> (Option<IndexShapeDetails>, bool) {
    match access.node() {
        ExecutableAccessNode::Path(path) => (
            path.capabilities().index_range_details(),
            path.capabilities().supports_reverse_traversal(),
        ),
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

/// Derive immutable runtime access capabilities for one executable access plan.
#[must_use]
fn derive_access_capabilities<K>(access: &ExecutableAccessPlan<'_, K>) -> AccessCapabilities {
    let plan_kind = dispatch_access_plan_kind(access);
    let single_path = match access.node() {
        ExecutableAccessNode::Path(path) => Some(path.capabilities()),
        ExecutableAccessNode::Union(_) | ExecutableAccessNode::Intersection(_) => None,
    };
    let (first_index_range_details, all_paths_support_reverse_traversal) =
        summarize_access_plan_runtime_shape(access);

    AccessCapabilities {
        plan_kind,
        single_path,
        first_index_range_details,
        all_paths_support_reverse_traversal,
    }
}

impl<K> ExecutableAccessPath<'_, K> {
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

    /// Project coarse plan-kind metrics for this executable access plan.
    #[must_use]
    pub(in crate::db) const fn metrics_kind(&self) -> PlanKind {
        dispatch_access_plan_kind(self).metrics_kind()
    }
}

/// Dispatch one executable access plan into its plan-kind discriminant.
#[must_use]
pub(in crate::db) const fn dispatch_access_plan_kind<K>(
    access: &ExecutableAccessPlan<'_, K>,
) -> AccessPlanKind {
    match access.node() {
        ExecutableAccessNode::Path(path) => AccessPlanKind::Path(path.capabilities().kind()),
        ExecutableAccessNode::Union(_) => AccessPlanKind::Union,
        ExecutableAccessNode::Intersection(_) => AccessPlanKind::Intersection,
    }
}

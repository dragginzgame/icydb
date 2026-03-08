//! Module: db::access::capabilities
//! Responsibility: route-facing capability projection over executable access contracts.
//! Does not own: planner semantics or physical stream execution behavior.
//! Boundary: access-layer capability authority consumed by executor route/load/stream modules.

use crate::{
    db::access::{
        AccessPathKind, ExecutableAccessNode, ExecutableAccessPath, ExecutableAccessPlan,
        ExecutionPathKind, ExecutionPathPayload,
    },
    model::index::IndexModel,
    obs::sink::PlanKind,
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

impl AccessPathKind {
    /// Return one structural scan-family descriptor for this path kind.
    #[must_use]
    pub(in crate::db) const fn scan_kind(self) -> AccessScanKind {
        match self {
            Self::ByKey | Self::ByKeys => AccessScanKind::Keys,
            Self::KeyRange => AccessScanKind::Range,
            Self::IndexPrefix | Self::IndexMultiLookup | Self::IndexRange => AccessScanKind::Index,
            Self::FullScan => AccessScanKind::FullScan,
        }
    }
}

impl AccessPlanKind {
    /// Return one structural scan-family descriptor for this plan kind.
    #[must_use]
    pub(in crate::db) const fn scan_kind(self) -> AccessScanKind {
        match self {
            Self::Path(kind) => kind.scan_kind(),
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
    stream: SinglePathStreamCapabilities,
    pushdown: SinglePathPushdownCapabilities,
    supports_primary_scan_fetch_hint: bool,
    is_key_direct_access: bool,
    is_by_keys_empty: bool,
    index_prefix_details: Option<IndexShapeDetails>,
    index_range_details: Option<IndexShapeDetails>,
    index_fields_for_slot_map: Option<&'static [&'static str]>,
    index_prefix_spec_count: usize,
    consumes_index_range_spec: bool,
}

///
/// SinglePathStreamCapabilities
///
/// Stream-oriented capability flags for one executable access path.
/// These flags represent access feasibility guarantees.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SinglePathStreamCapabilities {
    supports_pk_stream_access: bool,
    supports_reverse_traversal: bool,
}

///
/// SinglePathPushdownCapabilities
///
/// Pushdown-oriented capability flags for one executable access path.
/// This isolates pushdown affordances from stream-ordering semantics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SinglePathPushdownCapabilities {
    supports_count_pushdown_shape: bool,
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

    /// Return true when this path can drive fast-path PK stream access directly.
    /// This does not imply the emitted stream is guaranteed PK-ordered.
    #[must_use]
    pub(in crate::db) const fn supports_pk_stream_access(&self) -> bool {
        self.stream.supports_pk_stream_access
    }

    #[must_use]
    pub(in crate::db) const fn supports_count_pushdown_shape(&self) -> bool {
        self.pushdown.supports_count_pushdown_shape
    }

    #[must_use]
    pub(in crate::db) const fn supports_primary_scan_fetch_hint(&self) -> bool {
        self.supports_primary_scan_fetch_hint
    }

    #[must_use]
    pub(in crate::db) const fn supports_reverse_traversal(&self) -> bool {
        self.stream.supports_reverse_traversal
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
    #[must_use]
    pub(in crate::db) const fn single_path(&self) -> Option<SinglePathAccessCapabilities> {
        self.single_path
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
}

const fn derive_access_path_kind_from_execution_kind(kind: ExecutionPathKind) -> AccessPathKind {
    match kind {
        ExecutionPathKind::ByKey => AccessPathKind::ByKey,
        ExecutionPathKind::ByKeys => AccessPathKind::ByKeys,
        ExecutionPathKind::KeyRange => AccessPathKind::KeyRange,
        ExecutionPathKind::IndexPrefix => AccessPathKind::IndexPrefix,
        ExecutionPathKind::IndexMultiLookup => AccessPathKind::IndexMultiLookup,
        ExecutionPathKind::IndexRange => AccessPathKind::IndexRange,
        ExecutionPathKind::FullScan => AccessPathKind::FullScan,
    }
}

const fn is_by_keys_empty_from_payload<K>(payload: &ExecutionPathPayload<'_, K>) -> bool {
    matches!(payload, ExecutionPathPayload::ByKeys(keys) if keys.is_empty())
}

const fn index_prefix_spec_count_from_payload<K>(payload: &ExecutionPathPayload<'_, K>) -> usize {
    match payload {
        ExecutionPathPayload::IndexPrefix => 1,
        ExecutionPathPayload::IndexMultiLookup { value_count } => *value_count,
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
    // Phase 1: derive static capability projection from execution-path shape.
    let kind = derive_access_path_kind_from_execution_kind(path.kind());
    let (stream, pushdown, supports_primary_scan_fetch_hint, is_key_direct_access) = match kind {
        AccessPathKind::ByKey => (
            SinglePathStreamCapabilities {
                supports_pk_stream_access: false,
                supports_reverse_traversal: true,
            },
            SinglePathPushdownCapabilities {
                supports_count_pushdown_shape: false,
            },
            true,
            true,
        ),
        AccessPathKind::ByKeys => (
            SinglePathStreamCapabilities {
                supports_pk_stream_access: false,
                supports_reverse_traversal: false,
            },
            SinglePathPushdownCapabilities {
                supports_count_pushdown_shape: false,
            },
            false,
            true,
        ),
        AccessPathKind::KeyRange | AccessPathKind::FullScan => (
            SinglePathStreamCapabilities {
                supports_pk_stream_access: true,
                supports_reverse_traversal: true,
            },
            SinglePathPushdownCapabilities {
                supports_count_pushdown_shape: true,
            },
            true,
            false,
        ),
        AccessPathKind::IndexPrefix
        | AccessPathKind::IndexMultiLookup
        | AccessPathKind::IndexRange => (
            SinglePathStreamCapabilities {
                supports_pk_stream_access: false,
                supports_reverse_traversal: true,
            },
            SinglePathPushdownCapabilities {
                supports_count_pushdown_shape: false,
            },
            false,
            false,
        ),
    };

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
        stream,
        pushdown,
        supports_primary_scan_fetch_hint,
        is_key_direct_access,
        is_by_keys_empty: is_by_keys_empty_from_payload(path.payload()),
        index_prefix_details,
        index_range_details,
        index_fields_for_slot_map,
        index_prefix_spec_count: index_prefix_spec_count_from_payload(path.payload()),
        consumes_index_range_spec: index_range_details.is_some(),
    }
}

fn access_plan_first_index_range_details_internal<K>(
    access: &ExecutableAccessPlan<'_, K>,
) -> Option<IndexShapeDetails> {
    match access.node() {
        ExecutableAccessNode::Path(path) => path.capabilities().index_range_details(),
        ExecutableAccessNode::Union(children) | ExecutableAccessNode::Intersection(children) => {
            children
                .iter()
                .find_map(access_plan_first_index_range_details_internal)
        }
    }
}

fn access_plan_supports_reverse_traversal_internal<K>(
    access: &ExecutableAccessPlan<'_, K>,
) -> bool {
    match access.node() {
        ExecutableAccessNode::Path(path) => path.capabilities().supports_reverse_traversal(),
        ExecutableAccessNode::Union(children) | ExecutableAccessNode::Intersection(children) => {
            children
                .iter()
                .all(access_plan_supports_reverse_traversal_internal)
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

    AccessCapabilities {
        plan_kind,
        single_path,
        first_index_range_details: access_plan_first_index_range_details_internal(access),
        all_paths_support_reverse_traversal: access_plan_supports_reverse_traversal_internal(
            access,
        ),
    }
}

impl<K> ExecutableAccessPath<'_, K> {
    /// Project immutable runtime capabilities for this executable access path.
    #[must_use]
    pub(in crate::db) const fn capabilities(&self) -> SinglePathAccessCapabilities {
        derive_access_path_capabilities(self)
    }
}

/// Project immutable runtime capabilities for one executable access path.
#[must_use]
pub(in crate::db) const fn single_path_capabilities<K>(
    path: &ExecutableAccessPath<'_, K>,
) -> SinglePathAccessCapabilities {
    path.capabilities()
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

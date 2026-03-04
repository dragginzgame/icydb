//! Module: db::executor::access_dispatcher
//! Responsibility: runtime dispatch boundary over normalized executable access contracts.
//! Does not own: planner semantics or physical stream execution behavior.
//! Boundary: executor modules query access-shape capabilities through this module.

use crate::{
    db::executor::{
        ExecutableAccessNode, ExecutableAccessPath, ExecutableAccessPlan, ExecutionBounds,
        ExecutionPathKind, ExecutionPathPayload,
    },
    model::index::IndexModel,
    obs::sink::PlanKind,
};

///
/// AccessPathKind
///
/// Canonical runtime discriminant for executable access paths.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum AccessPathKind {
    ByKey,
    ByKeys,
    KeyRange,
    IndexPrefix,
    IndexRange,
    FullScan,
}

///
/// AccessScanKind
///
/// Structural scan-family descriptor for executable access shapes.
/// This intentionally captures routing shape only, not policy constraints.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum AccessScanKind {
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
pub(in crate::db::executor) enum AccessPlanKind {
    Path(AccessPathKind),
    Union,
    Intersection,
}

impl AccessPathKind {
    /// Return one structural scan-family descriptor for this path kind.
    #[must_use]
    pub(in crate::db::executor) const fn scan_kind(self) -> AccessScanKind {
        match self {
            Self::ByKey | Self::ByKeys => AccessScanKind::Keys,
            Self::KeyRange => AccessScanKind::Range,
            Self::IndexPrefix | Self::IndexRange => AccessScanKind::Index,
            Self::FullScan => AccessScanKind::FullScan,
        }
    }

    /// Return true when this path kind semantically relies on ordered traversal.
    #[must_use]
    pub(in crate::db::executor) const fn requires_order(self) -> bool {
        !matches!(self, Self::ByKey | Self::ByKeys)
    }

    /// Return true when this path kind supports strict continuation advancement.
    #[must_use]
    #[expect(clippy::unused_self)]
    pub(in crate::db::executor) const fn supports_strict_resume(self) -> bool {
        true
    }
}

impl AccessPlanKind {
    /// Return one structural scan-family descriptor for this plan kind.
    #[must_use]
    pub(in crate::db::executor) const fn scan_kind(self) -> AccessScanKind {
        match self {
            Self::Path(kind) => kind.scan_kind(),
            Self::Union | Self::Intersection => AccessScanKind::Composite,
        }
    }

    /// Return true when this plan kind semantically relies on ordered traversal.
    #[must_use]
    pub(in crate::db::executor) const fn requires_order(self) -> bool {
        match self {
            Self::Path(kind) => kind.requires_order(),
            Self::Union | Self::Intersection => true,
        }
    }

    /// Return true when this plan kind supports strict continuation advancement.
    #[must_use]
    pub(in crate::db::executor) const fn supports_strict_resume(self) -> bool {
        match self {
            Self::Path(kind) => kind.supports_strict_resume(),
            Self::Union | Self::Intersection => true,
        }
    }

    /// Project one plan kind into coarse plan-kind metrics.
    #[must_use]
    pub(in crate::db::executor) const fn metrics_kind(self) -> PlanKind {
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
pub(in crate::db::executor) struct SinglePathAccessCapabilities {
    kind: AccessPathKind,
    stream: SinglePathStreamCapabilities,
    pushdown: SinglePathPushdownCapabilities,
    supports_primary_scan_fetch_hint: bool,
    is_key_direct_access: bool,
    is_by_keys_empty: bool,
    index_prefix_details: Option<IndexShapeDetails>,
    index_range_details: Option<IndexShapeDetails>,
    index_fields_for_slot_map: Option<&'static [&'static str]>,
    consumes_index_prefix_spec: bool,
    consumes_index_range_spec: bool,
}

///
/// SinglePathStreamCapabilities
///
/// Stream-oriented capability flags for one executable access path.
/// These flags represent access feasibility and ordering guarantees.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SinglePathStreamCapabilities {
    supports_pk_stream_access: bool,
    supports_reverse_traversal: bool,
    is_pk_ordered_stream: bool,
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
    pub(in crate::db::executor) const fn kind(&self) -> AccessPathKind {
        self.kind
    }

    /// Return true when this path can drive fast-path PK stream access directly.
    /// This does not imply the emitted stream is guaranteed PK-ordered.
    #[must_use]
    pub(in crate::db::executor) const fn supports_pk_stream_access(&self) -> bool {
        self.stream.supports_pk_stream_access
    }

    #[must_use]
    pub(in crate::db::executor) const fn supports_count_pushdown_shape(&self) -> bool {
        self.pushdown.supports_count_pushdown_shape
    }

    #[must_use]
    pub(in crate::db::executor) const fn supports_primary_scan_fetch_hint(&self) -> bool {
        self.supports_primary_scan_fetch_hint
    }

    #[must_use]
    pub(in crate::db::executor) const fn supports_reverse_traversal(&self) -> bool {
        self.stream.supports_reverse_traversal
    }

    /// Return true when this path guarantees stream order by primary key.
    /// This does not imply fast-path PK stream access is available.
    #[must_use]
    pub(in crate::db::executor) const fn is_pk_ordered_stream(&self) -> bool {
        self.stream.is_pk_ordered_stream
    }

    #[must_use]
    pub(in crate::db::executor) const fn is_key_direct_access(&self) -> bool {
        self.is_key_direct_access
    }

    #[must_use]
    pub(in crate::db::executor) const fn is_by_keys_empty(&self) -> bool {
        self.is_by_keys_empty
    }

    #[must_use]
    pub(in crate::db::executor) const fn index_prefix_details(&self) -> Option<IndexShapeDetails> {
        self.index_prefix_details
    }

    #[must_use]
    pub(in crate::db::executor) const fn index_range_details(&self) -> Option<IndexShapeDetails> {
        self.index_range_details
    }

    #[must_use]
    pub(in crate::db::executor) const fn index_prefix_model(&self) -> Option<IndexModel> {
        match self.index_prefix_details {
            Some(details) => Some(details.index()),
            None => None,
        }
    }

    #[must_use]
    pub(in crate::db::executor) const fn index_range_model(&self) -> Option<IndexModel> {
        match self.index_range_details {
            Some(details) => Some(details.index()),
            None => None,
        }
    }

    #[must_use]
    pub(in crate::db::executor) const fn index_fields_for_slot_map(
        &self,
    ) -> Option<&'static [&'static str]> {
        self.index_fields_for_slot_map
    }

    #[must_use]
    pub(in crate::db::executor) const fn consumes_index_prefix_spec(&self) -> bool {
        self.consumes_index_prefix_spec
    }

    #[must_use]
    pub(in crate::db::executor) const fn consumes_index_range_spec(&self) -> bool {
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
pub(in crate::db::executor) struct IndexShapeDetails {
    index: IndexModel,
    slot_arity: usize,
}

impl IndexShapeDetails {
    #[must_use]
    pub(in crate::db::executor) const fn new(index: IndexModel, slot_arity: usize) -> Self {
        Self { index, slot_arity }
    }

    #[must_use]
    pub(in crate::db::executor) const fn index(self) -> IndexModel {
        self.index
    }

    #[must_use]
    pub(in crate::db::executor) const fn slot_arity(self) -> usize {
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
pub(in crate::db::executor) struct AccessCapabilities {
    plan_kind: AccessPlanKind,
    single_path: Option<SinglePathAccessCapabilities>,
    first_index_range_details: Option<IndexShapeDetails>,
    all_paths_support_reverse_traversal: bool,
    all_paths_pk_ordered_stream: bool,
}

impl AccessCapabilities {
    #[must_use]
    pub(in crate::db::executor) const fn single_path(
        &self,
    ) -> Option<SinglePathAccessCapabilities> {
        self.single_path
    }

    #[must_use]
    pub(in crate::db::executor) const fn first_index_range_details(
        &self,
    ) -> Option<IndexShapeDetails> {
        self.first_index_range_details
    }

    #[must_use]
    pub(in crate::db::executor) const fn is_composite(&self) -> bool {
        matches!(
            self.plan_kind,
            AccessPlanKind::Union | AccessPlanKind::Intersection
        )
    }

    #[must_use]
    pub(in crate::db::executor) const fn all_paths_support_reverse_traversal(&self) -> bool {
        self.all_paths_support_reverse_traversal
    }

    #[must_use]
    pub(in crate::db::executor) const fn all_paths_pk_ordered_stream(&self) -> bool {
        self.all_paths_pk_ordered_stream
    }
}

const fn derive_access_path_kind_from_execution_kind(kind: ExecutionPathKind) -> AccessPathKind {
    match kind {
        ExecutionPathKind::ByKey => AccessPathKind::ByKey,
        ExecutionPathKind::ByKeys => AccessPathKind::ByKeys,
        ExecutionPathKind::KeyRange => AccessPathKind::KeyRange,
        ExecutionPathKind::IndexPrefix => AccessPathKind::IndexPrefix,
        ExecutionPathKind::IndexRange => AccessPathKind::IndexRange,
        ExecutionPathKind::FullScan => AccessPathKind::FullScan,
    }
}

const fn supports_pk_stream_access(kind: AccessPathKind) -> bool {
    matches!(kind, AccessPathKind::KeyRange | AccessPathKind::FullScan)
}

const fn supports_count_pushdown_shape(kind: AccessPathKind) -> bool {
    matches!(kind, AccessPathKind::KeyRange | AccessPathKind::FullScan)
}

const fn supports_primary_scan_fetch_hint(kind: AccessPathKind) -> bool {
    matches!(
        kind,
        AccessPathKind::ByKey | AccessPathKind::KeyRange | AccessPathKind::FullScan
    )
}

const fn supports_reverse_traversal(kind: AccessPathKind) -> bool {
    matches!(
        kind,
        AccessPathKind::ByKey
            | AccessPathKind::KeyRange
            | AccessPathKind::IndexPrefix
            | AccessPathKind::IndexRange
            | AccessPathKind::FullScan
    )
}

const fn is_pk_ordered_stream() -> bool {
    true
}

const fn is_key_direct_access(kind: AccessPathKind) -> bool {
    matches!(kind, AccessPathKind::ByKey | AccessPathKind::ByKeys)
}

const fn is_by_keys_empty_from_payload<K>(payload: &ExecutionPathPayload<'_, K>) -> bool {
    matches!(payload, ExecutionPathPayload::ByKeys(keys) if keys.is_empty())
}

const fn index_prefix_details_from_bounds(bounds: ExecutionBounds) -> Option<IndexShapeDetails> {
    match bounds {
        ExecutionBounds::IndexPrefix { index, prefix_len } => {
            Some(IndexShapeDetails::new(index, prefix_len))
        }
        ExecutionBounds::Unbounded
        | ExecutionBounds::PrimaryKeyRange
        | ExecutionBounds::IndexRange { .. } => None,
    }
}

const fn index_range_details_from_bounds(bounds: ExecutionBounds) -> Option<IndexShapeDetails> {
    match bounds {
        ExecutionBounds::IndexRange { index, prefix_len } => {
            Some(IndexShapeDetails::new(index, prefix_len))
        }
        ExecutionBounds::Unbounded
        | ExecutionBounds::PrimaryKeyRange
        | ExecutionBounds::IndexPrefix { .. } => None,
    }
}

const fn index_fields_for_slot_map_from_bounds(
    bounds: ExecutionBounds,
) -> Option<&'static [&'static str]> {
    match bounds {
        ExecutionBounds::IndexPrefix { index, .. } | ExecutionBounds::IndexRange { index, .. } => {
            Some(index.fields)
        }
        ExecutionBounds::Unbounded | ExecutionBounds::PrimaryKeyRange => None,
    }
}

/// Derive immutable runtime capabilities for one executable access path.
#[must_use]
pub(in crate::db::executor) const fn derive_access_path_capabilities<K>(
    path: &ExecutableAccessPath<'_, K>,
) -> SinglePathAccessCapabilities {
    let kind = derive_access_path_kind_from_execution_kind(path.kind());
    let bounds = path.bounds();

    SinglePathAccessCapabilities {
        kind,
        stream: SinglePathStreamCapabilities {
            supports_pk_stream_access: supports_pk_stream_access(kind),
            supports_reverse_traversal: supports_reverse_traversal(kind),
            is_pk_ordered_stream: is_pk_ordered_stream(),
        },
        pushdown: SinglePathPushdownCapabilities {
            supports_count_pushdown_shape: supports_count_pushdown_shape(kind),
        },
        supports_primary_scan_fetch_hint: supports_primary_scan_fetch_hint(kind),
        is_key_direct_access: is_key_direct_access(kind),
        is_by_keys_empty: is_by_keys_empty_from_payload(path.payload()),
        index_prefix_details: index_prefix_details_from_bounds(bounds),
        index_range_details: index_range_details_from_bounds(bounds),
        index_fields_for_slot_map: index_fields_for_slot_map_from_bounds(bounds),
        consumes_index_prefix_spec: matches!(bounds, ExecutionBounds::IndexPrefix { .. }),
        consumes_index_range_spec: matches!(bounds, ExecutionBounds::IndexRange { .. }),
    }
}

fn access_plan_first_index_range_details_internal<K>(
    access: &ExecutableAccessPlan<'_, K>,
) -> Option<IndexShapeDetails> {
    match access.node() {
        ExecutableAccessNode::Path(path) => {
            let capabilities = derive_access_path_capabilities(path);
            capabilities.index_range_details()
        }
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
        ExecutableAccessNode::Path(path) => {
            let capabilities = derive_access_path_capabilities(path);
            capabilities.supports_reverse_traversal()
        }
        ExecutableAccessNode::Union(children) | ExecutableAccessNode::Intersection(children) => {
            children
                .iter()
                .all(access_plan_supports_reverse_traversal_internal)
        }
    }
}

fn access_plan_is_pk_ordered_stream_internal<K>(access: &ExecutableAccessPlan<'_, K>) -> bool {
    match access.node() {
        ExecutableAccessNode::Path(path) => {
            let capabilities = derive_access_path_capabilities(path);
            capabilities.is_pk_ordered_stream()
        }
        ExecutableAccessNode::Union(children) | ExecutableAccessNode::Intersection(children) => {
            children
                .iter()
                .all(access_plan_is_pk_ordered_stream_internal)
        }
    }
}

/// Derive immutable runtime access capabilities for one executable access plan.
#[must_use]
pub(in crate::db::executor) fn derive_access_capabilities<K>(
    access: &ExecutableAccessPlan<'_, K>,
) -> AccessCapabilities {
    let plan_kind = dispatch_access_plan_kind(access);
    debug_assert!(
        !plan_kind.requires_order() || plan_kind.supports_strict_resume(),
        "access invariant: ordered scan families must preserve strict resume support",
    );
    let single_path = match access.node() {
        ExecutableAccessNode::Path(path) => Some(derive_access_path_capabilities(path)),
        ExecutableAccessNode::Union(_) | ExecutableAccessNode::Intersection(_) => None,
    };

    AccessCapabilities {
        plan_kind,
        single_path,
        first_index_range_details: access_plan_first_index_range_details_internal(access),
        all_paths_support_reverse_traversal: access_plan_supports_reverse_traversal_internal(
            access,
        ),
        all_paths_pk_ordered_stream: access_plan_is_pk_ordered_stream_internal(access),
    }
}

/// Dispatch one executable access plan into its plan-kind discriminant.
#[must_use]
pub(in crate::db::executor) const fn dispatch_access_plan_kind<K>(
    access: &ExecutableAccessPlan<'_, K>,
) -> AccessPlanKind {
    match access.node() {
        ExecutableAccessNode::Path(path) => {
            AccessPlanKind::Path(derive_access_path_capabilities(path).kind())
        }
        ExecutableAccessNode::Union(_) => AccessPlanKind::Union,
        ExecutableAccessNode::Intersection(_) => AccessPlanKind::Intersection,
    }
}

/// Map one executable access plan to coarse plan-kind metrics.
#[must_use]
pub(in crate::db::executor) const fn access_plan_metrics_kind<K>(
    access: &ExecutableAccessPlan<'_, K>,
) -> PlanKind {
    dispatch_access_plan_kind(access).metrics_kind()
}

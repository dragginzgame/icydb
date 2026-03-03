//! Module: db::executor::access_dispatcher
//! Responsibility: runtime dispatch boundary over normalized executable access contracts.
//! Does not own: planner semantics or physical stream execution behavior.
//! Boundary: executor modules query access-shape capabilities through this module.

use crate::{
    db::executor::{
        ExecutableAccessNode, ExecutableAccessPath, ExecutableAccessPlan,
        trace::ExecutionAccessPathVariant,
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

    /// Project one path kind into trace-surface variant shape.
    #[must_use]
    pub(in crate::db::executor) const fn execution_access_path_variant(
        self,
    ) -> ExecutionAccessPathVariant {
        match self {
            Self::ByKey => ExecutionAccessPathVariant::ByKey,
            Self::ByKeys => ExecutionAccessPathVariant::ByKeys,
            Self::KeyRange => ExecutionAccessPathVariant::KeyRange,
            Self::IndexPrefix => ExecutionAccessPathVariant::IndexPrefix,
            Self::IndexRange => ExecutionAccessPathVariant::IndexRange,
            Self::FullScan => ExecutionAccessPathVariant::FullScan,
        }
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

    /// Project one plan kind into trace-surface variant shape.
    #[must_use]
    pub(in crate::db::executor) const fn execution_access_path_variant(
        self,
    ) -> ExecutionAccessPathVariant {
        match self {
            Self::Path(kind) => kind.execution_access_path_variant(),
            Self::Union => ExecutionAccessPathVariant::Union,
            Self::Intersection => ExecutionAccessPathVariant::Intersection,
        }
    }
}

///
/// SinglePathAccessCapabilities
///
/// Runtime capability snapshot for one executable access path.
/// This projects strategy methods into immutable data so route planners can
/// consume one descriptor instead of re-deriving path capabilities ad hoc.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct SinglePathAccessCapabilities {
    stream: SinglePathStreamCapabilities,
    pushdown: SinglePathPushdownCapabilities,
    index_prefix_details: Option<IndexShapeDetails>,
    index_range_details: Option<IndexShapeDetails>,
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

///
/// AccessPathRuntimeStrategy
///
/// Runtime capability surface for one executable access path.
///

pub(in crate::db::executor) trait AccessPathRuntimeStrategy<K> {
    fn kind(&self) -> AccessPathKind;

    /// Return true when this path can drive PK stream access directly.
    /// This does not imply PK-order guarantees in the emitted stream.
    fn supports_pk_stream_access(&self) -> bool;

    fn supports_count_pushdown_shape(&self) -> bool;

    fn supports_primary_scan_fetch_hint(&self) -> bool;

    fn supports_reverse_traversal(&self) -> bool;

    /// Return true when this path guarantees PK-ordered stream output.
    /// This does not imply PK stream fast-path access is available.
    fn is_pk_ordered_stream(&self) -> bool;

    fn is_key_direct_access(&self) -> bool;

    fn is_by_keys_empty(&self) -> bool;

    fn index_prefix_model(&self) -> Option<IndexModel>;

    fn index_range_model(&self) -> Option<IndexModel>;

    fn index_prefix_details(&self) -> Option<(IndexModel, usize)>;

    fn index_range_details(&self) -> Option<(IndexModel, usize)>;

    fn index_fields_for_slot_map(&self) -> Option<&'static [&'static str]>;

    fn consumes_index_prefix_spec(&self) -> bool;

    fn consumes_index_range_spec(&self) -> bool;
}

impl<K> AccessPathRuntimeStrategy<K> for ExecutableAccessPath<'_, K> {
    fn kind(&self) -> AccessPathKind {
        match self.kind() {
            crate::db::executor::ExecutionPathKind::ByKey => AccessPathKind::ByKey,
            crate::db::executor::ExecutionPathKind::ByKeys => AccessPathKind::ByKeys,
            crate::db::executor::ExecutionPathKind::KeyRange => AccessPathKind::KeyRange,
            crate::db::executor::ExecutionPathKind::IndexPrefix => AccessPathKind::IndexPrefix,
            crate::db::executor::ExecutionPathKind::IndexRange => AccessPathKind::IndexRange,
            crate::db::executor::ExecutionPathKind::FullScan => AccessPathKind::FullScan,
        }
    }

    fn supports_pk_stream_access(&self) -> bool {
        self.supports_pk_stream_access()
    }

    fn supports_count_pushdown_shape(&self) -> bool {
        self.supports_count_pushdown_shape()
    }

    fn supports_primary_scan_fetch_hint(&self) -> bool {
        self.supports_primary_scan_fetch_hint()
    }

    fn supports_reverse_traversal(&self) -> bool {
        self.supports_reverse_traversal()
    }

    fn is_pk_ordered_stream(&self) -> bool {
        self.is_pk_ordered_stream()
    }

    fn is_key_direct_access(&self) -> bool {
        self.is_key_direct_access()
    }

    fn is_by_keys_empty(&self) -> bool {
        self.is_by_keys_empty()
    }

    fn index_prefix_model(&self) -> Option<IndexModel> {
        self.index_prefix_details().map(|(index, _)| index)
    }

    fn index_range_model(&self) -> Option<IndexModel> {
        self.index_range_details().map(|(index, _)| index)
    }

    fn index_prefix_details(&self) -> Option<(IndexModel, usize)> {
        self.index_prefix_details()
    }

    fn index_range_details(&self) -> Option<(IndexModel, usize)> {
        self.index_range_details()
    }

    fn index_fields_for_slot_map(&self) -> Option<&'static [&'static str]> {
        self.index_fields_for_slot_map()
    }

    fn consumes_index_prefix_spec(&self) -> bool {
        self.consumes_index_prefix_spec()
    }

    fn consumes_index_range_spec(&self) -> bool {
        self.consumes_index_range_spec()
    }
}

/// Dispatch one executable access path into a runtime strategy carrier.
#[must_use]
pub(in crate::db::executor) const fn dispatch_access_path<'a, K>(
    path: &'a ExecutableAccessPath<'a, K>,
) -> &'a ExecutableAccessPath<'a, K> {
    path
}

fn derive_single_path_access_capabilities<K>(
    path: &ExecutableAccessPath<'_, K>,
) -> SinglePathAccessCapabilities {
    let dispatched = dispatch_access_path(path);
    let strategy: &dyn AccessPathRuntimeStrategy<K> = dispatched;

    SinglePathAccessCapabilities {
        stream: SinglePathStreamCapabilities {
            supports_pk_stream_access: strategy.supports_pk_stream_access(),
            supports_reverse_traversal: strategy.supports_reverse_traversal(),
            is_pk_ordered_stream: strategy.is_pk_ordered_stream(),
        },
        pushdown: SinglePathPushdownCapabilities {
            supports_count_pushdown_shape: strategy.supports_count_pushdown_shape(),
        },
        index_prefix_details: strategy
            .index_prefix_details()
            .map(|(index, slot_arity)| IndexShapeDetails::new(index, slot_arity)),
        index_range_details: strategy
            .index_range_details()
            .map(|(index, slot_arity)| IndexShapeDetails::new(index, slot_arity)),
    }
}

fn access_plan_first_index_range_details_internal<K>(
    access: &ExecutableAccessPlan<'_, K>,
) -> Option<IndexShapeDetails> {
    match access.node() {
        ExecutableAccessNode::Path(path) => {
            let capabilities = derive_single_path_access_capabilities(path);
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
            let capabilities = derive_single_path_access_capabilities(path);
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
            let capabilities = derive_single_path_access_capabilities(path);
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
        ExecutableAccessNode::Path(path) => Some(derive_single_path_access_capabilities(path)),
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
pub(in crate::db::executor) fn dispatch_access_plan_kind<K>(
    access: &ExecutableAccessPlan<'_, K>,
) -> AccessPlanKind {
    match access.node() {
        ExecutableAccessNode::Path(path) => {
            let dispatched = dispatch_access_path(path);
            let strategy: &dyn AccessPathRuntimeStrategy<K> = dispatched;

            AccessPlanKind::Path(strategy.kind())
        }
        ExecutableAccessNode::Union(_) => AccessPlanKind::Union,
        ExecutableAccessNode::Intersection(_) => AccessPlanKind::Intersection,
    }
}

/// Map one executable access plan to coarse plan-kind metrics.
#[must_use]
pub(in crate::db::executor) fn access_plan_metrics_kind<K>(
    access: &ExecutableAccessPlan<'_, K>,
) -> PlanKind {
    dispatch_access_plan_kind(access).metrics_kind()
}

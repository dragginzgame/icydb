//! Module: db::executor::access_dispatcher
//! Responsibility: runtime dispatch boundary over normalized executable access contracts.
//! Does not own: planner semantics or physical stream execution behavior.
//! Boundary: executor modules query access-shape capabilities through this module.

use crate::{
    db::executor::{ExecutableAccessNode, ExecutableAccessPath, ExecutableAccessPlan},
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

///
/// AccessPathRuntimeStrategy
///
/// Runtime capability surface for one executable access path.
///

pub(in crate::db::executor) trait AccessPathRuntimeStrategy<K> {
    fn kind(&self) -> AccessPathKind;

    fn supports_pk_stream_access(&self) -> bool;

    fn supports_count_pushdown_shape(&self) -> bool;

    fn supports_primary_scan_fetch_hint(&self) -> bool;

    fn supports_reverse_traversal(&self) -> bool;

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

/// Return the first encountered index-range details from this executable plan.
#[must_use]
pub(in crate::db::executor) fn access_plan_first_index_range_details<K>(
    access: &ExecutableAccessPlan<'_, K>,
) -> Option<(IndexModel, usize)> {
    match access.node() {
        ExecutableAccessNode::Path(path) => {
            let dispatched = dispatch_access_path(path);
            let strategy: &dyn AccessPathRuntimeStrategy<K> = dispatched;

            strategy.index_range_details()
        }
        ExecutableAccessNode::Union(children) | ExecutableAccessNode::Intersection(children) => {
            children
                .iter()
                .find_map(access_plan_first_index_range_details)
        }
    }
}

/// Map one executable access plan to coarse plan-kind metrics.
#[must_use]
pub(in crate::db::executor) fn access_plan_metrics_kind<K>(
    access: &ExecutableAccessPlan<'_, K>,
) -> PlanKind {
    match dispatch_access_plan_kind(access) {
        AccessPlanKind::Path(AccessPathKind::ByKey | AccessPathKind::ByKeys) => PlanKind::Keys,
        AccessPlanKind::Path(AccessPathKind::KeyRange) => PlanKind::Range,
        AccessPlanKind::Path(AccessPathKind::IndexPrefix | AccessPathKind::IndexRange) => {
            PlanKind::Index
        }
        AccessPlanKind::Path(AccessPathKind::FullScan)
        | AccessPlanKind::Union
        | AccessPlanKind::Intersection => PlanKind::FullScan,
    }
}

/// Return true when this executable plan is a composite access shape.
#[must_use]
pub(in crate::db::executor) fn is_composite_access_plan<K>(
    access: &ExecutableAccessPlan<'_, K>,
) -> bool {
    matches!(
        dispatch_access_plan_kind(access),
        AccessPlanKind::Union | AccessPlanKind::Intersection
    )
}

/// Return true when every executable path under this plan supports reverse traversal.
#[must_use]
pub(in crate::db::executor) fn access_plan_supports_reverse_traversal<K>(
    access: &ExecutableAccessPlan<'_, K>,
) -> bool {
    match access.node() {
        ExecutableAccessNode::Path(path) => {
            let dispatched = dispatch_access_path(path);
            let strategy: &dyn AccessPathRuntimeStrategy<K> = dispatched;
            strategy.supports_reverse_traversal()
        }
        ExecutableAccessNode::Union(children) | ExecutableAccessNode::Intersection(children) => {
            children.iter().all(access_plan_supports_reverse_traversal)
        }
    }
}

/// Return true when every executable path under this plan preserves PK stream ordering.
#[must_use]
pub(in crate::db::executor) fn access_plan_is_pk_ordered_stream<K>(
    access: &ExecutableAccessPlan<'_, K>,
) -> bool {
    match access.node() {
        ExecutableAccessNode::Path(path) => {
            let dispatched = dispatch_access_path(path);
            let strategy: &dyn AccessPathRuntimeStrategy<K> = dispatched;
            strategy.is_pk_ordered_stream()
        }
        ExecutableAccessNode::Union(children) | ExecutableAccessNode::Intersection(children) => {
            children.iter().all(access_plan_is_pk_ordered_stream)
        }
    }
}

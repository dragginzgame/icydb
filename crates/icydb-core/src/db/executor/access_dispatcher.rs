//! Module: db::executor::access_dispatcher
//! Responsibility: single runtime dispatch boundary from `AccessPath`/`AccessPlan`.
//! Does not own: planner semantics or physical stream execution behavior.
//! Boundary: executor modules query trait-based access strategy capabilities here.

use crate::{
    db::access::{AccessPath, AccessPlan},
    model::index::IndexModel,
    obs::sink::PlanKind,
};

///
/// AccessPathKind
///
/// Canonical runtime discriminant for `AccessPath`.
/// Executor modules must route path-shape decisions through this boundary.
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
/// Canonical runtime discriminant for `AccessPlan`.
/// Path variants are represented by `Path(AccessPathKind)`.
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
/// Trait-based runtime strategy surface for one `AccessPath`.
/// All executor path-shape capability checks should use this trait instead of
/// matching `AccessPath` directly in call sites.
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

///
/// AccessPathStrategy
///
/// Concrete strategy carrier for one borrowed `AccessPath`.
/// This enum is internal to the dispatcher and is exposed as a trait object at
/// call sites.
///

pub(in crate::db::executor) enum AccessPathStrategy<'a, K> {
    ByKey,
    ByKeys {
        keys: &'a [K],
    },
    KeyRange,
    IndexPrefix {
        index: IndexModel,
        prefix_len: usize,
    },
    IndexRange {
        index: IndexModel,
        prefix_len: usize,
    },
    FullScan,
}

impl<K> AccessPathRuntimeStrategy<K> for AccessPathStrategy<'_, K> {
    fn kind(&self) -> AccessPathKind {
        match self {
            Self::ByKey => AccessPathKind::ByKey,
            Self::ByKeys { .. } => AccessPathKind::ByKeys,
            Self::KeyRange => AccessPathKind::KeyRange,
            Self::IndexPrefix { .. } => AccessPathKind::IndexPrefix,
            Self::IndexRange { .. } => AccessPathKind::IndexRange,
            Self::FullScan => AccessPathKind::FullScan,
        }
    }

    fn supports_pk_stream_access(&self) -> bool {
        matches!(self, Self::KeyRange | Self::FullScan)
    }

    fn supports_count_pushdown_shape(&self) -> bool {
        matches!(self, Self::KeyRange | Self::FullScan)
    }

    fn supports_primary_scan_fetch_hint(&self) -> bool {
        matches!(self, Self::ByKey | Self::KeyRange | Self::FullScan)
    }

    fn supports_reverse_traversal(&self) -> bool {
        matches!(
            self,
            Self::ByKey
                | Self::KeyRange
                | Self::IndexPrefix { .. }
                | Self::IndexRange { .. }
                | Self::FullScan
        )
    }

    fn is_pk_ordered_stream(&self) -> bool {
        matches!(
            self,
            Self::ByKey
                | Self::ByKeys { .. }
                | Self::KeyRange
                | Self::IndexPrefix { .. }
                | Self::IndexRange { .. }
                | Self::FullScan
        )
    }

    fn is_key_direct_access(&self) -> bool {
        matches!(self, Self::ByKey | Self::ByKeys { .. })
    }

    fn is_by_keys_empty(&self) -> bool {
        match self {
            Self::ByKeys { keys } => keys.is_empty(),
            _ => false,
        }
    }

    fn index_prefix_model(&self) -> Option<IndexModel> {
        match self {
            Self::IndexPrefix { index, .. } => Some(*index),
            _ => None,
        }
    }

    fn index_range_model(&self) -> Option<IndexModel> {
        match self {
            Self::IndexRange { index, .. } => Some(*index),
            _ => None,
        }
    }

    fn index_prefix_details(&self) -> Option<(IndexModel, usize)> {
        match self {
            Self::IndexPrefix { index, prefix_len } => Some((*index, *prefix_len)),
            _ => None,
        }
    }

    fn index_range_details(&self) -> Option<(IndexModel, usize)> {
        match self {
            Self::IndexRange { index, prefix_len } => Some((*index, *prefix_len)),
            _ => None,
        }
    }

    fn index_fields_for_slot_map(&self) -> Option<&'static [&'static str]> {
        match self {
            Self::IndexPrefix { index, .. } | Self::IndexRange { index, .. } => Some(index.fields),
            _ => None,
        }
    }

    fn consumes_index_prefix_spec(&self) -> bool {
        matches!(self, Self::IndexPrefix { .. })
    }

    fn consumes_index_range_spec(&self) -> bool {
        matches!(self, Self::IndexRange { .. })
    }
}

/// Dispatch one runtime path into its canonical strategy carrier.
#[must_use]
pub(in crate::db::executor) const fn dispatch_access_path<K>(
    path: &AccessPath<K>,
) -> AccessPathStrategy<'_, K> {
    match path {
        AccessPath::ByKey(_) => AccessPathStrategy::ByKey,
        AccessPath::ByKeys(keys) => AccessPathStrategy::ByKeys {
            keys: keys.as_slice(),
        },
        AccessPath::KeyRange { .. } => AccessPathStrategy::KeyRange,
        AccessPath::IndexPrefix { index, values } => AccessPathStrategy::IndexPrefix {
            index: *index,
            prefix_len: values.len(),
        },
        AccessPath::IndexRange { spec } => AccessPathStrategy::IndexRange {
            index: *spec.index(),
            prefix_len: spec.prefix_values().len(),
        },
        AccessPath::FullScan => AccessPathStrategy::FullScan,
    }
}

/// Dispatch one runtime plan into its canonical plan-kind discriminant.
#[must_use]
pub(in crate::db::executor) fn dispatch_access_plan_kind<K>(
    access: &AccessPlan<K>,
) -> AccessPlanKind {
    match access {
        AccessPlan::Path(path) => {
            let dispatched = dispatch_access_path(path.as_ref());
            let strategy: &dyn AccessPathRuntimeStrategy<K> = &dispatched;

            AccessPlanKind::Path(strategy.kind())
        }
        AccessPlan::Union(_) => AccessPlanKind::Union,
        AccessPlan::Intersection(_) => AccessPlanKind::Intersection,
    }
}

/// Return the first encountered index-range details from this plan.
#[must_use]
pub(in crate::db::executor) fn access_plan_first_index_range_details<K>(
    access: &AccessPlan<K>,
) -> Option<(IndexModel, usize)> {
    match access {
        AccessPlan::Path(path) => {
            let dispatched = dispatch_access_path(path.as_ref());
            let strategy: &dyn AccessPathRuntimeStrategy<K> = &dispatched;

            strategy.index_range_details()
        }
        AccessPlan::Union(children) | AccessPlan::Intersection(children) => children
            .iter()
            .find_map(access_plan_first_index_range_details),
    }
}

/// Map one access plan to coarse plan-kind metrics.
#[must_use]
pub(in crate::db::executor) fn access_plan_metrics_kind<K>(access: &AccessPlan<K>) -> PlanKind {
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

/// Return true when this plan is a composite access shape.
#[must_use]
pub(in crate::db::executor) fn is_composite_access_plan<K>(access: &AccessPlan<K>) -> bool {
    matches!(
        dispatch_access_plan_kind(access),
        AccessPlanKind::Union | AccessPlanKind::Intersection
    )
}

/// Return true when every path under this plan supports reverse traversal.
#[must_use]
pub(in crate::db::executor) fn access_plan_supports_reverse_traversal<K>(
    access: &AccessPlan<K>,
) -> bool {
    match access {
        AccessPlan::Path(path) => {
            let dispatched = dispatch_access_path(path.as_ref());
            let strategy: &dyn AccessPathRuntimeStrategy<K> = &dispatched;
            strategy.supports_reverse_traversal()
        }
        AccessPlan::Union(children) | AccessPlan::Intersection(children) => {
            children.iter().all(access_plan_supports_reverse_traversal)
        }
    }
}

/// Return true when every path under this plan preserves canonical PK stream ordering.
#[must_use]
pub(in crate::db::executor) fn access_plan_is_pk_ordered_stream<K>(access: &AccessPlan<K>) -> bool {
    match access {
        AccessPlan::Path(path) => {
            let dispatched = dispatch_access_path(path.as_ref());
            let strategy: &dyn AccessPathRuntimeStrategy<K> = &dispatched;
            strategy.is_pk_ordered_stream()
        }
        AccessPlan::Union(children) | AccessPlan::Intersection(children) => {
            children.iter().all(access_plan_is_pk_ordered_stream)
        }
    }
}

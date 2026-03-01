//! Module: executor::trace::projection
//! Responsibility: map load-path runtime inputs into trace-surface enum projections.
//! Does not own: execution routing decisions or observability storage policy.
//! Boundary: pure projection helpers used by `ExecutionTrace`.

use crate::db::{
    access::AccessPlan,
    direction::Direction,
    executor::route::order_direction_from_direction,
    executor::{
        AccessPathKind, AccessPlanKind, dispatch_access_plan_kind,
        trace::ExecutionAccessPathVariant,
    },
    query::plan::OrderDirection,
};

/// Project access-plan shape into trace-level access-path variant.
pub(super) fn access_path_variant<K>(access: &AccessPlan<K>) -> ExecutionAccessPathVariant {
    match dispatch_access_plan_kind(access) {
        AccessPlanKind::Path(kind) => match kind {
            AccessPathKind::ByKey => ExecutionAccessPathVariant::ByKey,
            AccessPathKind::ByKeys => ExecutionAccessPathVariant::ByKeys,
            AccessPathKind::KeyRange => ExecutionAccessPathVariant::KeyRange,
            AccessPathKind::IndexPrefix => ExecutionAccessPathVariant::IndexPrefix,
            AccessPathKind::IndexRange => ExecutionAccessPathVariant::IndexRange,
            AccessPathKind::FullScan => ExecutionAccessPathVariant::FullScan,
        },
        AccessPlanKind::Union => ExecutionAccessPathVariant::Union,
        AccessPlanKind::Intersection => ExecutionAccessPathVariant::Intersection,
    }
}

/// Project runtime direction into trace-level order direction.
pub(super) const fn execution_order_direction(direction: Direction) -> OrderDirection {
    order_direction_from_direction(direction)
}

use crate::{
    db::{
        cursor::CursorBoundary,
        direction::Direction,
        executor::{
            ExecutionKernel, RangeToken,
            aggregate::{AggregateKind, AggregateSpec},
            load::LoadExecutor,
            traversal::derive_primary_scan_direction,
        },
        query::plan::AccessPlannedQuery,
    },
    traits::{EntityKind, EntityValue},
};

use crate::db::executor::route::{ContinuationMode, RouteCapabilities, RouteWindowPlan};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    pub(super) fn derive_load_route_direction(plan: &AccessPlannedQuery<E::Key>) -> Direction {
        derive_primary_scan_direction(plan.scalar_plan().order.as_ref())
    }

    pub(super) fn derive_aggregate_route_direction(
        plan: &AccessPlannedQuery<E::Key>,
        spec: &AggregateSpec,
    ) -> Direction {
        if spec.target_field().is_some() {
            return match spec.kind() {
                AggregateKind::Min => Direction::Asc,
                AggregateKind::Max => Direction::Desc,
                AggregateKind::Count
                | AggregateKind::Exists
                | AggregateKind::First
                | AggregateKind::Last => Self::derive_load_route_direction(plan),
            };
        }

        Self::derive_load_route_direction(plan)
    }

    pub(super) const fn derive_continuation_mode(
        cursor_boundary: Option<&CursorBoundary>,
        index_range_anchor: Option<&RangeToken>,
    ) -> ContinuationMode {
        match (cursor_boundary, index_range_anchor) {
            (_, Some(_)) => ContinuationMode::IndexRangeAnchor,
            (Some(_), None) => ContinuationMode::CursorBoundary,
            (None, None) => ContinuationMode::Initial,
        }
    }

    pub(super) fn derive_route_window(
        plan: &AccessPlannedQuery<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
    ) -> RouteWindowPlan {
        let effective_offset = ExecutionKernel::effective_page_offset(plan, cursor_boundary);
        let limit = plan.scalar_plan().page.as_ref().and_then(|page| page.limit);

        RouteWindowPlan::new(effective_offset, limit)
    }

    // Route-owned aggregate non-count streaming gate.
    // Field-target extrema uses route capability flags directly; non-target
    // terminals use the shared streaming-safe/pushdown/index-range route gates.
    pub(super) fn aggregate_non_count_streaming_allowed(
        aggregate_spec: Option<&AggregateSpec>,
        capabilities: RouteCapabilities,
        secondary_pushdown_eligible: bool,
        index_range_limit_enabled: bool,
    ) -> bool {
        if let Some(spec) = aggregate_spec
            && spec.target_field().is_some()
        {
            return match spec.kind() {
                AggregateKind::Min => capabilities.field_min_fast_path_eligible,
                AggregateKind::Max => capabilities.field_max_fast_path_eligible,
                AggregateKind::Count
                | AggregateKind::Exists
                | AggregateKind::First
                | AggregateKind::Last => false,
            };
        }

        capabilities.streaming_access_shape_safe
            || secondary_pushdown_eligible
            || index_range_limit_enabled
    }

    // Route-owned load streaming gate.
    // Load execution remains streaming when canonical streaming-safe shapes
    // apply or when route enabled index-range limit pushdown.
    pub(super) const fn load_streaming_allowed(
        capabilities: RouteCapabilities,
        index_range_limit_enabled: bool,
    ) -> bool {
        capabilities.streaming_access_shape_safe || index_range_limit_enabled
    }
}

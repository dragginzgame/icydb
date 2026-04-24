//! Module: db::executor::planning::route::hints::aggregate
//! Defines aggregate-routing hints used to explain and classify chosen
//! executor routes.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::{
    direction::Direction,
    executor::{
        aggregate::field_target_is_tie_free_probe_target,
        route::{
            AccessWindow, AggregateRouteShape, AggregateSeekSpec, RouteCapabilities,
            aggregate_bounded_probe_fetch_hint, aggregate_supports_bounded_probe_hint,
            direction_allows_physical_fetch_hint,
        },
    },
    query::plan::{AccessPlannedQuery, AggregateKind},
};

pub(in crate::db::executor::planning::route) const fn count_pushdown_fetch_hint(
    access_window: AccessWindow,
    capabilities: RouteCapabilities,
) -> Option<usize> {
    if capabilities.bounded_probe_hint_safe {
        crate::db::executor::planning::route::hints::load::bounded_window_fetch_hint(access_window)
    } else {
        None
    }
}

pub(in crate::db::executor::planning::route) fn aggregate_probe_fetch_hint(
    plan: &AccessPlannedQuery,
    aggregate: AggregateRouteShape<'_>,
    direction: Direction,
    desc_physical_reverse_supported: bool,
    capabilities: RouteCapabilities,
    access_window: AccessWindow,
) -> Option<usize> {
    let kind = aggregate.kind();
    aggregate_probe_shape_supported(plan, aggregate, direction, capabilities).then_some(())?;

    (aggregate_supports_bounded_probe_hint(kind)
        && direction_allows_physical_fetch_hint(direction, desc_physical_reverse_supported)
        && capabilities.bounded_probe_hint_safe)
        .then_some(())?;

    aggregate_probe_window_fetch_hint(kind, direction, access_window)
}

pub(in crate::db::executor::planning::route) fn aggregate_seek_spec(
    plan: &AccessPlannedQuery,
    aggregate: AggregateRouteShape<'_>,
    direction: Direction,
    desc_physical_reverse_supported: bool,
    capabilities: RouteCapabilities,
    access_window: AccessWindow,
) -> Option<AggregateSeekSpec> {
    aggregate.kind().is_extrema().then_some(())?;
    let fetch = aggregate_probe_fetch_hint(
        plan,
        aggregate,
        direction,
        desc_physical_reverse_supported,
        capabilities,
        access_window,
    )?;

    Some(match direction {
        Direction::Asc => AggregateSeekSpec::First { fetch },
        Direction::Desc => AggregateSeekSpec::Last { fetch },
    })
}

// Apply the route capability snapshot to the aggregate probe shape before the
// bounded fetch-hint layer interprets the access window.
fn aggregate_probe_shape_supported(
    plan: &AccessPlannedQuery,
    aggregate: AggregateRouteShape<'_>,
    direction: Direction,
    capabilities: RouteCapabilities,
) -> bool {
    match (aggregate.target_field(), aggregate.kind(), direction) {
        (Some(_), AggregateKind::Min, Direction::Asc) => capabilities.field_min_fast_path_eligible,
        (Some(_), AggregateKind::Max, Direction::Desc) => {
            capabilities.field_max_fast_path_eligible
                && field_target_max_probe_shape_is_tie_free(plan, aggregate)
        }
        (Some(_), _, _) => false,
        (None, _, _) => true,
    }
}

// Convert one route access window into the bounded aggregate probe fetch hint.
fn aggregate_probe_window_fetch_hint(
    kind: AggregateKind,
    direction: Direction,
    access_window: AccessWindow,
) -> Option<usize> {
    if access_window.is_zero_window() {
        return Some(0);
    }

    let offset = access_window.lower_bound();
    let page_limit = access_window
        .page_limit()
        .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));

    aggregate_bounded_probe_fetch_hint(kind, direction, offset, page_limit)
}

fn field_target_max_probe_shape_is_tie_free(
    plan: &AccessPlannedQuery,
    aggregate: AggregateRouteShape<'_>,
) -> bool {
    let access_capabilities = plan.access_strategy().capabilities();
    let index_model = access_capabilities
        .single_path_index_prefix_details()
        .or_else(|| access_capabilities.single_path_index_range_details())
        .map(|(index, _)| index);

    field_target_is_tie_free_probe_target(aggregate, index_model)
}

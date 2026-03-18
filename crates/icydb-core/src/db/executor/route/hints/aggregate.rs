//! Module: db::executor::route::hints::aggregate
//! Responsibility: module-local ownership and contracts for db::executor::route::hints::aggregate.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        direction::Direction,
        executor::{
            aggregate::field_target_is_tie_free_probe_target_for_model,
            route::{
                AccessWindow, AggregateSeekSpec, RouteCapabilities,
                aggregate_bounded_probe_fetch_hint, aggregate_supports_bounded_probe_hint,
                direction_allows_physical_fetch_hint,
            },
        },
        query::builder::AggregateExpr,
        query::plan::{AccessPlannedQuery, AggregateKind},
    },
    model::entity::EntityModel,
};

pub(in crate::db::executor::route) const fn count_pushdown_fetch_hint(
    access_window: AccessWindow,
    capabilities: RouteCapabilities,
) -> Option<usize> {
    if capabilities.bounded_probe_hint_safe {
        crate::db::executor::route::hints::load::bounded_window_fetch_hint(access_window)
    } else {
        None
    }
}

pub(in crate::db::executor::route) fn aggregate_probe_fetch_hint_for_model(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
    aggregate: &AggregateExpr,
    direction: Direction,
    capabilities: RouteCapabilities,
    access_window: AccessWindow,
) -> Option<usize> {
    let kind = aggregate.kind();
    let field_target_eligible = match (aggregate.target_field(), kind, direction) {
        (Some(_), AggregateKind::Min, Direction::Asc) => capabilities.field_min_fast_path_eligible,
        (Some(_), AggregateKind::Max, Direction::Desc) => {
            capabilities.field_max_fast_path_eligible
                && field_target_max_probe_shape_is_tie_free_for_model(model, plan, aggregate)
        }
        (Some(_), _, _) => false,
        (None, _, _) => true,
    };
    field_target_eligible.then_some(())?;

    (aggregate_supports_bounded_probe_hint(kind)
        && direction_allows_physical_fetch_hint(
            direction,
            capabilities.desc_physical_reverse_supported,
        )
        && capabilities.bounded_probe_hint_safe)
        .then_some(())?;

    if access_window.is_zero_window() {
        Some(0)
    } else {
        let offset = access_window.lower_bound();
        let page_limit = access_window
            .page_limit()
            .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));

        aggregate_bounded_probe_fetch_hint(kind, direction, offset, page_limit)
    }
}

pub(in crate::db::executor::route) fn aggregate_seek_spec_for_model(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
    aggregate: &AggregateExpr,
    direction: Direction,
    capabilities: RouteCapabilities,
    access_window: AccessWindow,
) -> Option<AggregateSeekSpec> {
    aggregate.kind().is_extrema().then_some(())?;
    let fetch = aggregate_probe_fetch_hint_for_model(
        model,
        plan,
        aggregate,
        direction,
        capabilities,
        access_window,
    )?;

    Some(match direction {
        Direction::Asc => AggregateSeekSpec::First { fetch },
        Direction::Desc => AggregateSeekSpec::Last { fetch },
    })
}

fn field_target_max_probe_shape_is_tie_free_for_model(
    model: &EntityModel,
    plan: &AccessPlannedQuery,
    aggregate: &AggregateExpr,
) -> bool {
    aggregate.target_field().is_some_and(|target_field| {
        let access_class = plan.access_strategy().class();
        let index_model = access_class
            .single_path_index_prefix_details()
            .or_else(|| access_class.single_path_index_range_details())
            .map(|(index, _)| index);

        field_target_is_tie_free_probe_target_for_model(model, target_field, index_model)
    })
}

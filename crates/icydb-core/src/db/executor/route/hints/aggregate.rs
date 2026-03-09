use crate::{
    db::{
        direction::Direction,
        executor::{
            load::LoadExecutor,
            route::{
                AccessWindow, AggregateSeekSpec, RouteCapabilities,
                aggregate_bounded_probe_fetch_hint, aggregate_supports_bounded_probe_hint,
                direction_allows_physical_fetch_hint,
            },
        },
        query::builder::AggregateExpr,
        query::plan::{AccessPlannedQuery, AggregateKind},
    },
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    pub(in crate::db::executor::route) const fn count_pushdown_fetch_hint(
        access_window: AccessWindow,
        capabilities: RouteCapabilities,
    ) -> Option<usize> {
        if capabilities.bounded_probe_hint_safe {
            Self::bounded_window_fetch_hint(access_window)
        } else {
            None
        }
    }

    pub(in crate::db::executor::route) fn aggregate_probe_fetch_hint(
        plan: &AccessPlannedQuery<E::Key>,
        aggregate: &AggregateExpr,
        direction: Direction,
        capabilities: RouteCapabilities,
        access_window: AccessWindow,
    ) -> Option<usize> {
        let kind = aggregate.kind();
        // Field-target extrema probe hints require deterministic tie coverage.
        // MIN(field) ASC can always short-circuit on the first existing row.
        // MAX(field) DESC can short-circuit only when ties are impossible.
        let field_target_eligible = match (aggregate.target_field(), kind, direction) {
            (Some(_), AggregateKind::Min, Direction::Asc) => {
                capabilities.field_min_fast_path_eligible
            }
            (Some(_), AggregateKind::Max, Direction::Desc) => {
                capabilities.field_max_fast_path_eligible
                    && Self::field_target_max_probe_shape_is_tie_free(plan, aggregate)
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

    // Build an explicit aggregate seek contract when bounded aggregate probe
    // hints are eligible for one extrema terminal shape.
    pub(in crate::db::executor::route) fn aggregate_seek_spec(
        plan: &AccessPlannedQuery<E::Key>,
        aggregate: &AggregateExpr,
        direction: Direction,
        capabilities: RouteCapabilities,
        access_window: AccessWindow,
    ) -> Option<AggregateSeekSpec> {
        aggregate.kind().is_extrema().then_some(())?;
        let fetch = Self::aggregate_probe_fetch_hint(
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

    // Field-target MAX probe hints are safe only when the chosen path guarantees
    // no duplicate target values can appear in traversal order.
    fn field_target_max_probe_shape_is_tie_free(
        plan: &AccessPlannedQuery<E::Key>,
        aggregate: &AggregateExpr,
    ) -> bool {
        aggregate.target_field().is_some_and(|target_field| {
            let access_class = plan.access_strategy().class();
            let index_model = access_class
                .single_path_index_prefix_details()
                .or_else(|| access_class.single_path_index_range_details())
                .map(|(index, _)| index);

            Self::is_tie_free_probe_target(target_field, index_model)
        })
    }

    // One canonical tie-free target guard for bounded MAX(field) probe hints.
    // Tie-free means:
    // - target is primary key, or
    // - target is backed by a unique single-field leading index.
    fn is_tie_free_probe_target(
        target_field: &str,
        index_model: Option<crate::model::index::IndexModel>,
    ) -> bool {
        (target_field == E::MODEL.primary_key.name)
            || index_model.is_some_and(|index_model| {
                index_model.is_unique()
                    && index_model.fields().len() == 1
                    && index_model
                        .fields()
                        .first()
                        .is_some_and(|field| *field == target_field)
            })
    }
}

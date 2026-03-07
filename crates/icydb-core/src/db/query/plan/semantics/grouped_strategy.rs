use crate::db::{
    access::AccessPlan,
    query::plan::{AccessPlannedQuery, FieldSlot, GroupAggregateSpec, OrderSpec},
};

///
/// GroupedPlanStrategyHint
///
/// Planner-side grouped execution strategy hint projected from logical + access shape.
/// Executor routing may revalidate this hint against runtime capability constraints.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupedPlanStrategyHint {
    HashGroup,
    OrderedGroup,
}

/// Project one grouped execution strategy hint from one access-planned query.
#[must_use]
pub(in crate::db::query::plan) fn grouped_plan_strategy_hint<K>(
    plan: &AccessPlannedQuery<K>,
) -> Option<GroupedPlanStrategyHint> {
    let grouped = plan.grouped_plan()?;
    if grouped.scalar.distinct {
        return Some(GroupedPlanStrategyHint::HashGroup);
    }
    if grouped.scalar.predicate.is_some() {
        return Some(GroupedPlanStrategyHint::HashGroup);
    }
    if !grouped_aggregates_streaming_compatible(grouped.group.aggregates.as_slice()) {
        return Some(GroupedPlanStrategyHint::HashGroup);
    }
    if !crate::db::query::plan::semantics::group_having::grouped_having_streaming_compatible(
        grouped.having.as_ref(),
    ) {
        return Some(GroupedPlanStrategyHint::HashGroup);
    }
    if !grouped_order_prefix_matches_group_fields(
        grouped.scalar.order.as_ref(),
        grouped.group.group_fields.as_slice(),
    ) {
        return Some(GroupedPlanStrategyHint::HashGroup);
    }
    if grouped_access_path_proves_group_order(grouped.group.group_fields.as_slice(), &plan.access) {
        return Some(GroupedPlanStrategyHint::OrderedGroup);
    }

    Some(GroupedPlanStrategyHint::HashGroup)
}

fn grouped_aggregates_streaming_compatible(aggregates: &[GroupAggregateSpec]) -> bool {
    aggregates
        .iter()
        .all(GroupAggregateSpec::streaming_compatible_v1)
}

fn grouped_order_prefix_matches_group_fields(
    order: Option<&OrderSpec>,
    group_fields: &[FieldSlot],
) -> bool {
    let Some(order) = order else {
        return true;
    };
    if order.fields.len() < group_fields.len() {
        return false;
    }

    group_fields
        .iter()
        .zip(order.fields.iter())
        .all(|(group_field, (order_field, _))| order_field == group_field.field())
}

fn grouped_access_path_proves_group_order<K>(
    group_fields: &[FieldSlot],
    access: &AccessPlan<K>,
) -> bool {
    // Derive grouped-order evidence from the normalized executable access contract so
    // planner strategy hints do not branch on raw AccessPath variants directly.
    let executable = access.resolve_strategy();
    let Some(path) = executable.as_path() else {
        return false;
    };
    let Some((index, prefix_len)) = path.index_prefix_details() else {
        return false;
    };
    let required_end = prefix_len.saturating_add(group_fields.len());
    if required_end > index.fields().len() {
        return false;
    }

    group_fields
        .iter()
        .zip(index.fields()[prefix_len..required_end].iter())
        .all(|(group_field, index_field)| group_field.field() == *index_field)
}

use crate::{
    db::{
        access::AccessPlan,
        direction::Direction,
        executor::{
            ExecutablePlan, aggregate::materialized_distinct::insert_materialized_distinct_value,
            group::GroupKeySet,
        },
        query::plan::{OrderDirection, OrderSpec, PageSpec},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::Value,
};

#[derive(Clone, Copy)]
pub(super) enum CoveringProjectionOrder {
    IndexOrder(Direction),
    PrimaryKeyOrder(Direction),
}

///
/// CoveringProjectionContext
///
/// Covering projection metadata derived from one executable access/order shape.
/// This context keeps distinct strategy decisions local to projection runtime
/// and avoids re-deriving index-position contracts across terminal paths.
///

#[derive(Clone, Copy)]
pub(super) struct CoveringProjectionContext {
    pub(super) component_index: usize,
    pub(super) prefix_len: usize,
    pub(super) order_contract: CoveringProjectionOrder,
}

///
/// CoveringProjectionValues
///
/// Covering projection decoded values plus the context that produced them.
/// Distinct terminals use this bundle to choose between adjacent-key dedupe
/// and first-observed canonical dedupe without recomputing shape checks.
///

pub(super) struct CoveringProjectionValues {
    pub(super) values: Vec<Value>,
    pub(super) context: CoveringProjectionContext,
}

// Derive covering-projection access context (index-field position + output order
// contract) from one index-backed path and scalar ORDER BY shape.
pub(super) fn covering_index_projection_context<E>(
    access: &AccessPlan<E::Key>,
    plan: &ExecutablePlan<E>,
    target_field: &str,
) -> Option<CoveringProjectionContext>
where
    E: EntityKind + EntityValue,
{
    let (index_fields, prefix_len, path_kind_is_range) =
        if let Some((index, values)) = access.as_index_prefix_path() {
            (index.fields(), values.len(), false)
        } else if let Some((index, prefix_values, _, _)) = access.as_index_range_path() {
            (index.fields(), prefix_values.len(), true)
        } else {
            return None;
        };
    let component_index = index_fields
        .iter()
        .position(|field| *field == target_field)?;

    let order_contract = covering_projection_order_contract(
        plan.order_spec(),
        index_fields,
        prefix_len,
        E::MODEL.primary_key.name,
        path_kind_is_range,
    )?;

    Some(CoveringProjectionContext {
        component_index,
        prefix_len,
        order_contract,
    })
}

// Resolve one output-order contract that keeps index-projected values aligned
// with load post-access ordering semantics.
fn covering_projection_order_contract(
    order: Option<&OrderSpec>,
    index_fields: &[&'static str],
    prefix_len: usize,
    primary_key_name: &'static str,
    path_kind_is_range: bool,
) -> Option<CoveringProjectionOrder> {
    let Some(order) = order else {
        return Some(CoveringProjectionOrder::PrimaryKeyOrder(Direction::Asc));
    };
    let (first_order_field, first_order_direction) = order.fields.first()?;
    let direction = match first_order_direction {
        OrderDirection::Asc => Direction::Asc,
        OrderDirection::Desc => Direction::Desc,
    };
    if order
        .fields
        .iter()
        .any(|(_, order_direction)| order_direction != first_order_direction)
    {
        return None;
    }

    if order.fields.len() == 1 && first_order_field == primary_key_name {
        return Some(CoveringProjectionOrder::PrimaryKeyOrder(direction));
    }

    let mut expected_suffix = Vec::with_capacity(index_fields.len().saturating_sub(prefix_len) + 1);
    expected_suffix.extend(index_fields.iter().skip(prefix_len).copied());
    expected_suffix.push(primary_key_name);
    let actual_fields = order
        .fields
        .iter()
        .map(|(field, _)| field.as_str())
        .collect::<Vec<_>>();
    if actual_fields == expected_suffix {
        return Some(CoveringProjectionOrder::IndexOrder(direction));
    }

    if path_kind_is_range {
        return None;
    }

    let mut expected_full = Vec::with_capacity(index_fields.len() + 1);
    expected_full.extend(index_fields.iter().copied());
    expected_full.push(primary_key_name);
    (actual_fields == expected_full).then_some(CoveringProjectionOrder::IndexOrder(direction))
}

pub(super) fn scalar_window_for_covering_projection(
    page: Option<&PageSpec>,
) -> (usize, Option<usize>) {
    let Some(page) = page else {
        return (0, None);
    };

    let offset = usize::try_from(page.offset).unwrap_or(usize::MAX);
    let limit = page
        .limit
        .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));

    (offset, limit)
}

// Return whether one covering distinct projection can use adjacent-key dedupe.
//
// Safety contract:
// - output order must remain in index traversal order (no primary-key reorder),
// - target projection field must be the first unbound index component.
//
// Under this shape, equal projected values are contiguous in the effective
// covering value stream, so adjacent dedupe is equivalent to first-observed
// canonical dedupe.
pub(super) const fn covering_index_adjacent_distinct_eligible(
    context: CoveringProjectionContext,
) -> bool {
    matches!(
        context.order_contract,
        CoveringProjectionOrder::IndexOrder(_)
    ) && context.component_index == context.prefix_len
}

pub(super) fn dedup_values_preserving_first(
    values: Vec<Value>,
) -> Result<Vec<Value>, InternalError> {
    let mut seen = GroupKeySet::default();
    let mut out = Vec::new();
    for value in values {
        if !insert_materialized_distinct_value(&mut seen, &value)? {
            continue;
        }
        out.push(value);
    }

    Ok(out)
}

pub(super) fn dedup_adjacent_values(values: Vec<Value>) -> Vec<Value> {
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        if out.last().is_some_and(|previous| previous == &value) {
            continue;
        }
        out.push(value);
    }

    out
}

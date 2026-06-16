use crate::{
    db::{
        data::DecodedDataStoreKey,
        direction::Direction,
        executor::{
            apply_offset_limit_window,
            projection::covering::contracts::{
                CoveringProjectionOrder, CoveringReadField, CoveringReadFieldSource, PageSpec,
            },
        },
    },
    error::InternalError,
    value::Value,
};
use std::collections::BTreeMap;

pub(super) struct CoveringScanWindow {
    pub(super) direction: Direction,
    pub(super) limit: usize,
    pub(super) page_skip_count: usize,
    pub(super) page_window_applied: bool,
}

pub(super) fn covering_scan_window(
    order_contract: CoveringProjectionOrder,
    branch_set_access: bool,
    page_window_allowed_for_route: bool,
    distinct: bool,
    page: Option<&PageSpec>,
) -> CoveringScanWindow {
    let page_window_can_apply = page_window_allowed_for_route
        && !distinct
        && covering_index_scan_order_can_apply_page_window(order_contract, branch_set_access);

    CoveringScanWindow {
        direction: crate::db::executor::covering_projection_scan_direction(order_contract),
        limit: covering_scan_limit(page_window_can_apply, page),
        page_skip_count: covering_scan_time_page_skip_count(page_window_can_apply, page),
        page_window_applied: covering_scan_time_page_window_applied(page_window_can_apply, page),
    }
}

pub(super) fn apply_covering_page_window<T>(
    distinct: bool,
    page: Option<&PageSpec>,
    page_window_already_applied: bool,
    rows: &mut Vec<T>,
) {
    if distinct {
        // DISTINCT paging is deferred to the projection materializer after
        // projected-row deduplication over the ordered stream.
        return;
    }
    if page_window_already_applied {
        return;
    }

    let Some(page) = page else {
        return;
    };

    apply_offset_limit_window(rows, page.offset, page.limit);
}

const fn covering_index_scan_order_can_apply_page_window(
    order_contract: CoveringProjectionOrder,
    branch_set_access: bool,
) -> bool {
    matches!(order_contract, CoveringProjectionOrder::IndexOrder(_))
        || (branch_set_access
            && matches!(order_contract, CoveringProjectionOrder::PrimaryKeyOrder(_)))
}

fn covering_scan_limit(page_window_can_apply: bool, page: Option<&PageSpec>) -> usize {
    let Some(page) = page else {
        return usize::MAX;
    };
    if !page_window_can_apply {
        return usize::MAX;
    }
    let Some(limit) = page.limit else {
        return usize::MAX;
    };

    page.offset
        .saturating_add(limit)
        .max(1)
        .try_into()
        .unwrap_or(usize::MAX)
}

fn covering_scan_time_page_skip_count(
    page_window_can_apply: bool,
    page: Option<&PageSpec>,
) -> usize {
    if !page_window_can_apply {
        return 0;
    }

    page.map_or(0, |page| usize::try_from(page.offset).unwrap_or(usize::MAX))
}

fn covering_scan_time_page_window_applied(
    page_window_can_apply: bool,
    page: Option<&PageSpec>,
) -> bool {
    if !page_window_can_apply {
        return false;
    }

    page.is_some_and(|page| page.offset != 0 || page.limit.is_some())
}

pub(super) fn covering_projection_component_indices(fields: &[CoveringReadField]) -> Vec<usize> {
    let mut component_indices = Vec::with_capacity(fields.len());

    for field in fields {
        let component_index = match &field.source {
            CoveringReadFieldSource::IndexComponent { component_index }
            | CoveringReadFieldSource::IndexExpressionComponent { component_index } => {
                component_index
            }
            CoveringReadFieldSource::PrimaryKey { .. }
            | CoveringReadFieldSource::Constant(_)
            | CoveringReadFieldSource::RowField => continue,
        };
        if component_indices.contains(component_index) {
            continue;
        }

        component_indices.push(*component_index);
    }

    component_indices
}

pub(super) fn project_covering_row_from_decoded_values(
    data_key: &DecodedDataStoreKey,
    fields: &[CoveringReadField],
    component_indices: &[usize],
    decoded_values: &[Value],
) -> Result<Vec<Value>, InternalError> {
    if component_indices.len() != decoded_values.len() {
        return Err(InternalError::query_executor_invariant());
    }

    let mut projected = Vec::with_capacity(fields.len());

    for field in fields {
        let value = match &field.source {
            CoveringReadFieldSource::IndexComponent { component_index }
            | CoveringReadFieldSource::IndexExpressionComponent { component_index } => {
                let Some(position) = component_indices
                    .iter()
                    .position(|candidate| candidate == component_index)
                else {
                    return Err(InternalError::query_executor_invariant());
                };

                decoded_values
                    .get(position)
                    .cloned()
                    .ok_or_else(InternalError::query_executor_invariant)?
            }
            CoveringReadFieldSource::PrimaryKey { component_index } => {
                data_key.primary_key_component_runtime_value(*component_index)?
            }
            CoveringReadFieldSource::Constant(value) => value.clone(),
            CoveringReadFieldSource::RowField => {
                return Err(InternalError::query_executor_invariant());
            }
        };
        projected.push(value);
    }

    Ok(projected)
}

pub(super) fn project_covering_row_from_owned_decoded_values(
    data_key: &DecodedDataStoreKey,
    fields: &[CoveringReadField],
    component_indices: &[usize],
    decoded_values: Vec<Value>,
) -> Result<Vec<Value>, InternalError> {
    if component_indices.len() != decoded_values.len() {
        return Err(InternalError::query_executor_invariant());
    }

    let mut projected = Vec::with_capacity(fields.len());
    let mut decoded_values = decoded_values;
    let mut remaining_component_uses =
        covering_component_position_use_counts(fields, component_indices);

    for field in fields {
        let value = match &field.source {
            CoveringReadFieldSource::IndexComponent { component_index }
            | CoveringReadFieldSource::IndexExpressionComponent { component_index } => {
                let Some(position) = component_indices
                    .iter()
                    .position(|candidate| candidate == component_index)
                else {
                    return Err(InternalError::query_executor_invariant());
                };

                take_or_clone_last_component_value(
                    decoded_values.as_mut_slice(),
                    remaining_component_uses.as_mut_slice(),
                    position,
                )?
            }
            CoveringReadFieldSource::PrimaryKey { component_index } => {
                data_key.primary_key_component_runtime_value(*component_index)?
            }
            CoveringReadFieldSource::Constant(value) => value.clone(),
            CoveringReadFieldSource::RowField => {
                return Err(InternalError::query_executor_invariant());
            }
        };
        projected.push(value);
    }

    Ok(projected)
}

pub(super) fn project_covering_row_from_single_decoded_value(
    data_key: &DecodedDataStoreKey,
    fields: &[CoveringReadField],
    component_index: usize,
    decoded_value: Value,
) -> Result<Vec<Value>, InternalError> {
    let mut projected = Vec::with_capacity(fields.len());
    let mut decoded_value = Some(decoded_value);

    // Count matching output cells first so the final occurrence can consume the
    // owned decoded value while earlier duplicate columns keep cloning.
    let mut remaining_component_uses = fields
        .iter()
        .filter(|field| {
            matches!(
                &field.source,
                CoveringReadFieldSource::IndexComponent {
                    component_index: field_component_index
                }
                    | CoveringReadFieldSource::IndexExpressionComponent {
                    component_index: field_component_index
                } if *field_component_index == component_index
            )
        })
        .count();

    for field in fields {
        let value = match &field.source {
            CoveringReadFieldSource::IndexComponent {
                component_index: field_component_index,
            }
            | CoveringReadFieldSource::IndexExpressionComponent {
                component_index: field_component_index,
            } => {
                if *field_component_index != component_index {
                    return Err(InternalError::query_executor_invariant());
                }

                // Each projected column owns its value. Duplicate references
                // clone until the last use, where ownership can move into the
                // output row directly.
                remaining_component_uses = remaining_component_uses.saturating_sub(1);
                if remaining_component_uses == 0 {
                    decoded_value
                        .take()
                        .ok_or_else(InternalError::query_executor_invariant)?
                } else {
                    decoded_value
                        .clone()
                        .ok_or_else(InternalError::query_executor_invariant)?
                }
            }
            CoveringReadFieldSource::PrimaryKey { component_index } => {
                data_key.primary_key_component_runtime_value(*component_index)?
            }
            CoveringReadFieldSource::Constant(value) => value.clone(),
            CoveringReadFieldSource::RowField => {
                return Err(InternalError::query_executor_invariant());
            }
        };
        projected.push(value);
    }

    Ok(projected)
}

fn covering_component_position_use_counts(
    fields: &[CoveringReadField],
    component_indices: &[usize],
) -> Vec<usize> {
    let mut counts = vec![0; component_indices.len()];

    for field in fields {
        let component_index = match &field.source {
            CoveringReadFieldSource::IndexComponent { component_index }
            | CoveringReadFieldSource::IndexExpressionComponent { component_index } => {
                component_index
            }
            CoveringReadFieldSource::PrimaryKey { .. }
            | CoveringReadFieldSource::Constant(_)
            | CoveringReadFieldSource::RowField => continue,
        };
        if let Some(position) = component_indices
            .iter()
            .position(|candidate| candidate == component_index)
        {
            counts[position] += 1;
        }
    }

    counts
}

fn take_or_clone_last_component_value(
    decoded_values: &mut [Value],
    remaining_component_uses: &mut [usize],
    position: usize,
) -> Result<Value, InternalError> {
    let Some(remaining) = remaining_component_uses.get_mut(position) else {
        return Err(InternalError::query_executor_invariant());
    };

    // Projected columns are independently owned. Duplicate references clone
    // until the final component use can move out of the decoded row vector.
    *remaining = remaining.saturating_sub(1);
    if *remaining == 0 {
        let Some(value) = decoded_values.get_mut(position) else {
            return Err(InternalError::query_executor_invariant());
        };

        return Ok(std::mem::replace(value, Value::Null));
    }

    decoded_values
        .get(position)
        .cloned()
        .ok_or_else(InternalError::query_executor_invariant)
}

pub(super) fn decode_hybrid_covering_components(
    component_indices: &[usize],
    components: std::sync::Arc<[Vec<u8>]>,
) -> Result<BTreeMap<usize, Value>, InternalError> {
    let mut decoded = BTreeMap::new();

    for (component_index, component) in component_indices.iter().copied().zip(components.iter()) {
        let Some(value) =
            crate::db::executor::decode_covering_projection_component(component.as_slice())?
        else {
            return Err(InternalError::query_executor_invariant());
        };
        decoded.insert(component_index, value);
    }

    Ok(decoded)
}

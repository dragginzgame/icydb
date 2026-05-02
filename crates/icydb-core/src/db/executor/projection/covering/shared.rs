use crate::{
    db::{
        data::DataKey,
        query::plan::{CoveringReadField, CoveringReadFieldSource},
    },
    error::InternalError,
    value::{Value, storage_key_as_runtime_value},
};
use std::collections::BTreeMap;

#[cfg(feature = "sql")]
pub(super) fn covering_projection_component_indices(fields: &[CoveringReadField]) -> Vec<usize> {
    let mut component_indices = Vec::new();

    for field in fields {
        let CoveringReadFieldSource::IndexComponent { component_index } = &field.source else {
            continue;
        };
        if component_indices.contains(component_index) {
            continue;
        }

        component_indices.push(*component_index);
    }

    component_indices
}

#[cfg(feature = "sql")]
pub(super) fn project_covering_row_from_decoded_values(
    data_key: &DataKey,
    fields: &[CoveringReadField],
    component_indices: &[usize],
    decoded_values: &[Value],
) -> Result<Vec<Value>, InternalError> {
    if component_indices.len() != decoded_values.len() {
        return Err(InternalError::query_executor_invariant(
            "covering projection component decode arity mismatch",
        ));
    }

    let mut projected = Vec::with_capacity(fields.len());

    for field in fields {
        let value = match &field.source {
            CoveringReadFieldSource::IndexComponent { component_index } => {
                let Some(position) = component_indices
                    .iter()
                    .position(|candidate| candidate == component_index)
                else {
                    return Err(InternalError::query_executor_invariant(
                        "covering projection missing decoded covering component",
                    ));
                };

                decoded_values.get(position).cloned().ok_or_else(|| {
                    InternalError::query_executor_invariant(
                        "covering projection decoded component position out of bounds",
                    )
                })?
            }
            CoveringReadFieldSource::PrimaryKey => {
                storage_key_as_runtime_value(&data_key.storage_key())
            }
            CoveringReadFieldSource::Constant(value) => value.clone(),
            CoveringReadFieldSource::RowField => {
                return Err(InternalError::query_executor_invariant(
                    "pure covering projection unexpectedly reached row-backed field source",
                ));
            }
        };
        projected.push(value);
    }

    Ok(projected)
}

#[cfg(feature = "sql")]
pub(super) fn project_covering_row_from_owned_decoded_values(
    data_key: &DataKey,
    fields: &[CoveringReadField],
    component_indices: &[usize],
    decoded_values: Vec<Value>,
) -> Result<Vec<Value>, InternalError> {
    if component_indices.len() != decoded_values.len() {
        return Err(InternalError::query_executor_invariant(
            "covering projection component decode arity mismatch",
        ));
    }

    let mut projected = Vec::with_capacity(fields.len());
    let mut decoded_values = decoded_values;
    let mut remaining_component_uses =
        covering_component_position_use_counts(fields, component_indices);

    for field in fields {
        let value = match &field.source {
            CoveringReadFieldSource::IndexComponent { component_index } => {
                let Some(position) = component_indices
                    .iter()
                    .position(|candidate| candidate == component_index)
                else {
                    return Err(InternalError::query_executor_invariant(
                        "covering projection missing decoded covering component",
                    ));
                };

                take_or_clone_last_component_value(
                    decoded_values.as_mut_slice(),
                    remaining_component_uses.as_mut_slice(),
                    position,
                    "covering projection decoded component position out of bounds",
                )?
            }
            CoveringReadFieldSource::PrimaryKey => {
                storage_key_as_runtime_value(&data_key.storage_key())
            }
            CoveringReadFieldSource::Constant(value) => value.clone(),
            CoveringReadFieldSource::RowField => {
                return Err(InternalError::query_executor_invariant(
                    "pure covering projection unexpectedly reached row-backed field source",
                ));
            }
        };
        projected.push(value);
    }

    Ok(projected)
}

#[cfg(feature = "sql")]
pub(super) fn project_covering_row_from_single_decoded_value(
    data_key: &DataKey,
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
                } if *field_component_index == component_index
            )
        })
        .count();

    for field in fields {
        let value = match &field.source {
            CoveringReadFieldSource::IndexComponent {
                component_index: field_component_index,
            } => {
                if *field_component_index != component_index {
                    return Err(InternalError::query_executor_invariant(
                        "covering projection missing decoded covering component",
                    ));
                }

                // Each projected column owns its value. Duplicate references
                // clone until the last use, where ownership can move into the
                // output row directly.
                remaining_component_uses = remaining_component_uses.saturating_sub(1);
                if remaining_component_uses == 0 {
                    decoded_value.take().ok_or_else(|| {
                        InternalError::query_executor_invariant(
                            "covering projection decoded component was already consumed",
                        )
                    })?
                } else {
                    decoded_value.clone().ok_or_else(|| {
                        InternalError::query_executor_invariant(
                            "covering projection decoded component was already consumed",
                        )
                    })?
                }
            }
            CoveringReadFieldSource::PrimaryKey => {
                storage_key_as_runtime_value(&data_key.storage_key())
            }
            CoveringReadFieldSource::Constant(value) => value.clone(),
            CoveringReadFieldSource::RowField => {
                return Err(InternalError::query_executor_invariant(
                    "pure covering projection unexpectedly reached row-backed field source",
                ));
            }
        };
        projected.push(value);
    }

    Ok(projected)
}

#[cfg(feature = "sql")]
fn covering_component_position_use_counts(
    fields: &[CoveringReadField],
    component_indices: &[usize],
) -> Vec<usize> {
    let mut counts = vec![0; component_indices.len()];

    for field in fields {
        let CoveringReadFieldSource::IndexComponent { component_index } = &field.source else {
            continue;
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

#[cfg(feature = "sql")]
fn take_or_clone_last_component_value(
    decoded_values: &mut [Value],
    remaining_component_uses: &mut [usize],
    position: usize,
    missing_message: &'static str,
) -> Result<Value, InternalError> {
    let Some(remaining) = remaining_component_uses.get_mut(position) else {
        return Err(InternalError::query_executor_invariant(missing_message));
    };

    // Projected columns are independently owned. Duplicate references clone
    // until the final component use can move out of the decoded row vector.
    *remaining = remaining.saturating_sub(1);
    if *remaining == 0 {
        let Some(value) = decoded_values.get_mut(position) else {
            return Err(InternalError::query_executor_invariant(missing_message));
        };

        return Ok(std::mem::replace(value, Value::Null));
    }

    decoded_values
        .get(position)
        .cloned()
        .ok_or_else(|| InternalError::query_executor_invariant(missing_message))
}

#[cfg(feature = "sql")]
pub(super) fn decode_hybrid_covering_components(
    component_indices: &[usize],
    components: std::sync::Arc<[Vec<u8>]>,
) -> Result<BTreeMap<usize, Value>, InternalError> {
    let mut decoded = BTreeMap::new();

    for (component_index, component) in component_indices.iter().copied().zip(components.iter()) {
        let Some(value) =
            crate::db::executor::decode_covering_projection_component(component.as_slice())?
        else {
            return Err(InternalError::query_executor_invariant(
                "hybrid projection expected one decodable covering component payload",
            ));
        };
        decoded.insert(component_index, value);
    }

    Ok(decoded)
}

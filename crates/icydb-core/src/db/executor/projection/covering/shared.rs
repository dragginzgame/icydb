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
pub(super) fn project_covering_row_from_single_decoded_value(
    data_key: &DataKey,
    fields: &[CoveringReadField],
    component_index: usize,
    decoded_value: &Value,
) -> Result<Vec<Value>, InternalError> {
    let mut projected = Vec::with_capacity(fields.len());

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

                decoded_value.clone()
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

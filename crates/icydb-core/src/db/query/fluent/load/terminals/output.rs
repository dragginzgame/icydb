//! Module: query::fluent::load::terminals::output
//! Responsibility: convert runtime projection values into facade output values.
//! Does not own: terminal execution, projection planning, or row materialization.
//! Boundary: preserves terminal result order while mapping value representations.

use crate::{
    db::PersistedRow,
    types::Id,
    value::{OutputValue, Value},
};

// Convert one runtime projection value into the public output boundary type.
pub(super) fn output(value: Value) -> OutputValue {
    OutputValue::from(value)
}

// Convert one ordered runtime projection vector into the public output form.
pub(super) fn output_values(values: Vec<Value>) -> Vec<OutputValue> {
    values.into_iter().map(output).collect()
}

// Convert one ordered runtime `(id, value)` projection vector into the public output form.
pub(super) fn output_values_with_ids<E: PersistedRow>(
    values: Vec<(Id<E>, Value)>,
) -> Vec<(Id<E>, OutputValue)> {
    values
        .into_iter()
        .map(|(id, value)| (id, output(value)))
        .collect()
}

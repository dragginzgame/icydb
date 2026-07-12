//! Module: query::fluent::load::terminals::output
//! Responsibility: convert runtime projection values into facade output values.
//! Does not own: terminal execution, projection planning, or row materialization.
//! Boundary: preserves terminal result order while mapping value representations.

use crate::{
    db::{DbSession, PersistedRow, QueryError, schema::output_value_from_runtime},
    types::Id,
    value::{OutputValue, Value},
};

// Convert one runtime projection value through the accepted catalog that owns
// its store-local enum IDs.
pub(super) fn output<E>(
    session: &DbSession<E::Canister>,
    value: Value,
) -> Result<OutputValue, QueryError>
where
    E: PersistedRow,
{
    let schema = session
        .accepted_schema_info_for_entity::<E>()
        .map_err(QueryError::execute)?;
    let catalog = schema.enum_catalog().ok_or_else(QueryError::invariant)?;
    output_value_from_runtime(catalog, &value).map_err(|_error| QueryError::invariant())
}

// Convert one ordered runtime projection vector into the public output form.
pub(super) fn output_values<E>(
    session: &DbSession<E::Canister>,
    values: Vec<Value>,
) -> Result<Vec<OutputValue>, QueryError>
where
    E: PersistedRow,
{
    let schema = session
        .accepted_schema_info_for_entity::<E>()
        .map_err(QueryError::execute)?;
    let catalog = schema.enum_catalog().ok_or_else(QueryError::invariant)?;
    values
        .iter()
        .map(|value| {
            output_value_from_runtime(catalog, value).map_err(|_error| QueryError::invariant())
        })
        .collect()
}

// Convert one ordered runtime `(id, value)` projection vector into the public output form.
pub(super) fn output_values_with_ids<E: PersistedRow>(
    session: &DbSession<E::Canister>,
    values: Vec<(Id<E>, Value)>,
) -> Result<Vec<(Id<E>, OutputValue)>, QueryError> {
    let schema = session
        .accepted_schema_info_for_entity::<E>()
        .map_err(QueryError::execute)?;
    let catalog = schema.enum_catalog().ok_or_else(QueryError::invariant)?;
    values
        .into_iter()
        .map(|(id, value)| {
            output_value_from_runtime(catalog, &value)
                .map(|output| (id, output))
                .map_err(|_error| QueryError::invariant())
        })
        .collect()
}

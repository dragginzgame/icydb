//! Session-owned conversion from runtime rows to public output values.

use crate::{
    db::{
        QueryError,
        schema::{AcceptedEnumCatalog, output_value_from_runtime},
    },
    value::{OutputValue, Value},
};

// Convert one owned runtime row at the session boundary. All callers use the
// accepted enum catalog and map resolution failure to the same typed invariant.
pub(in crate::db::session) fn output_row_from_runtime(
    catalog: &AcceptedEnumCatalog,
    row: Vec<Value>,
) -> Result<Vec<OutputValue>, QueryError> {
    row.into_iter()
        .map(|value| {
            output_value_from_runtime(catalog, value).map_err(|_error| QueryError::invariant())
        })
        .collect()
}

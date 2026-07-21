//! Module: session::response::grouped
//! Responsibility: grouped paged response finalization.
//! Does not own: grouped execution, aggregate evaluation, or public response DTO shape.
//! Boundary: converts executor grouped results into traced public grouped page envelopes.

#[cfg(feature = "sql")]
use crate::db::cursor::encode_cursor;
use crate::db::{
    GroupedRow, PagedGroupedExecutionWithTrace, QueryError,
    diagnostics::ExecutionTrace,
    executor::{PageCursor, RuntimeGroupedRow, StructuralGroupedProjectionResult},
    schema::{AcceptedEnumCatalog, output_value_from_runtime},
};

// Encode one grouped executor cursor into the raw cursor bytes stored by core
// paged grouped response DTOs. The response layer receives opaque bytes only;
// external string formatting is left to the SQL/facade surfaces.
fn encode_grouped_page_cursor(cursor: Option<PageCursor>) -> Result<Option<Vec<u8>>, QueryError> {
    cursor
        .map(|token| {
            let Some(token) = token.as_grouped() else {
                return Err(QueryError::grouped_paged_emitted_scalar_continuation());
            };

            token
                .encode()
                .map_err(|_err| QueryError::serialize_internal())
        })
        .transpose()
}

// Convert one executor-owned grouped runtime carrier into the public grouped row
// DTO at the session boundary. This preserves `db::response` as DTO-only while
// keeping executor internals out of public response construction.
fn grouped_row_from_runtime_row(
    catalog: &AcceptedEnumCatalog,
    row: RuntimeGroupedRow,
) -> Result<GroupedRow, QueryError> {
    let (group_key, aggregate_values) = row.into_group_key_and_aggregate_values();
    let group_key = group_key
        .iter()
        .map(|value| {
            output_value_from_runtime(catalog, value).map_err(|_error| QueryError::invariant())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let aggregate_values = aggregate_values
        .iter()
        .map(|value| {
            output_value_from_runtime(catalog, value).map_err(|_error| QueryError::invariant())
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(GroupedRow::new(group_key, aggregate_values))
}

// Convert one ordered executor grouped page row vector without changing row
// order. SQL and fluent grouped response finalizers both use this adapter.
fn grouped_rows_from_runtime_rows(
    catalog: &AcceptedEnumCatalog,
    rows: Vec<RuntimeGroupedRow>,
) -> Result<Vec<GroupedRow>, QueryError> {
    rows.into_iter()
        .map(|row| grouped_row_from_runtime_row(catalog, row))
        .collect()
}

// Finalize one executor-owned structural grouped projection result for response
// shaping without exposing grouped cursor-page fields to session callers.
pub(in crate::db) fn finalize_structural_grouped_projection_result(
    result: StructuralGroupedProjectionResult,
    trace: Option<ExecutionTrace>,
) -> Result<PagedGroupedExecutionWithTrace, QueryError> {
    let (rows, next_cursor, value_catalog) = result.into_rows_and_cursor();
    let next_cursor = encode_grouped_page_cursor(next_cursor)?;
    let rows = grouped_rows_from_runtime_rows(value_catalog.enum_catalog(), rows)?;

    Ok(PagedGroupedExecutionWithTrace::new(
        rows,
        next_cursor,
        trace,
    ))
}

// Convert core grouped cursor bytes into the SQL statement surface's external
// cursor string. The byte payload already came from the cursor-owned grouped
// encoder above, so this is only lowercase-hex display formatting.
#[cfg(feature = "sql")]
pub(in crate::db) fn sql_grouped_cursor_from_bytes(cursor: Option<Vec<u8>>) -> Option<String> {
    cursor.as_deref().map(encode_cursor)
}

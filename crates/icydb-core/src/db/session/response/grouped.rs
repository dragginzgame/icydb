//! Module: session::response::grouped
//! Responsibility: grouped paged response finalization.
//! Does not own: grouped execution, aggregate evaluation, or public response DTO shape.
//! Boundary: converts executor grouped results into traced public grouped page envelopes.

use crate::db::executor::StructuralGroupedProjectionResult;
use crate::db::{
    GroupedRow, PagedGroupedExecutionWithTrace, QueryError,
    cursor::encode_cursor,
    diagnostics::ExecutionTrace,
    executor::{PageCursor, RuntimeGroupedRow},
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

            token.encode().map_err(|err| {
                QueryError::serialize_internal(format!(
                    "failed to serialize grouped continuation cursor: {err}"
                ))
            })
        })
        .transpose()
}

// Convert one executor-owned grouped runtime carrier into the public grouped row
// DTO at the session boundary. This preserves `db::response` as DTO-only while
// keeping executor internals out of public response construction.
fn grouped_row_from_runtime_row(row: RuntimeGroupedRow) -> GroupedRow {
    let (group_key, aggregate_values) = row.into_parts();

    GroupedRow::new(group_key, aggregate_values)
}

// Convert one ordered executor grouped page row vector without changing row
// order. SQL and fluent grouped response finalizers both use this adapter.
fn grouped_rows_from_runtime_rows(rows: Vec<RuntimeGroupedRow>) -> Vec<GroupedRow> {
    rows.into_iter().map(grouped_row_from_runtime_row).collect()
}

// Finalize one executor-owned structural grouped projection result for response
// shaping without exposing grouped cursor-page fields to session callers.
pub(in crate::db) fn finalize_structural_grouped_projection_result(
    result: StructuralGroupedProjectionResult,
    trace: Option<ExecutionTrace>,
) -> Result<PagedGroupedExecutionWithTrace, QueryError> {
    let (rows, next_cursor) = result.into_parts();
    let next_cursor = encode_grouped_page_cursor(next_cursor)?;

    Ok(PagedGroupedExecutionWithTrace::new(
        grouped_rows_from_runtime_rows(rows),
        next_cursor,
        trace,
    ))
}

// Convert core grouped cursor bytes into the SQL statement surface's external
// cursor string. The byte payload already came from the cursor-owned grouped
// encoder above, so this is only lowercase-hex display formatting.
pub(in crate::db) fn sql_grouped_cursor_from_bytes(cursor: Option<Vec<u8>>) -> Option<String> {
    cursor.as_deref().map(encode_cursor)
}

//! Module: session::response::scalar
//! Responsibility: scalar paged response finalization.
//! Does not own: scalar execution, cursor validation grammar, or public response DTO shape.
//! Boundary: converts executor scalar pages into traced public scalar page envelopes.

use crate::{
    db::{
        PagedLoadExecutionWithTrace, QueryError,
        diagnostics::ExecutionTrace,
        executor::{CursorPage, PageCursor},
    },
    traits::EntityKind,
};

// Encode one scalar executor cursor into the raw cursor bytes stored by core
// paged response DTOs. Cursor grammar and binary encoding remain cursor-owned;
// this helper only validates that the executor emitted the cursor family
// expected by the scalar response surface.
fn encode_scalar_page_cursor(cursor: Option<PageCursor>) -> Result<Option<Vec<u8>>, QueryError> {
    cursor
        .map(|token| {
            let Some(token) = token.as_scalar() else {
                return Err(QueryError::scalar_paged_emitted_grouped_continuation());
            };

            token.encode().map_err(|err| {
                QueryError::serialize_internal(format!(
                    "failed to serialize continuation cursor: {err}"
                ))
            })
        })
        .transpose()
}

// Finalize one scalar executor cursor page into the public core paged response
// envelope. Trace data is attached last so observability cannot affect rows or
// cursor bytes.
pub(in crate::db) fn finalize_scalar_paged_execution<E: EntityKind>(
    page: CursorPage<E>,
    trace: Option<ExecutionTrace>,
) -> Result<PagedLoadExecutionWithTrace<E>, QueryError> {
    let next_cursor = encode_scalar_page_cursor(page.next_cursor)?;

    Ok(PagedLoadExecutionWithTrace::new(
        page.items,
        next_cursor,
        trace,
    ))
}

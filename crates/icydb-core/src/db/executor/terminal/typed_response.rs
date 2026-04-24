//! Module: executor::terminal::typed_response
//! Responsibility: final typed response and cursor-page materialization.
//! Does not own: row-byte decoding, route execution, or pagination policy.
//! Boundary: executor terminal code consumes structural data rows and produces public response DTOs.

use crate::{
    db::{
        PersistedRow,
        data::{DataRow, decode_raw_row_for_entity_key},
        executor::{CursorPage, PageCursor},
        response::{EntityResponse, Row},
    },
    error::InternalError,
    traits::EntityValue,
    types::Id,
};

/// Decode persisted data rows into one typed entity response through one structural loop.
pub(in crate::db) fn decode_data_rows_into_entity_response<E>(
    rows: Vec<DataRow>,
) -> Result<EntityResponse<E>, InternalError>
where
    E: PersistedRow + EntityValue,
{
    let mut decoded_rows = Vec::with_capacity(rows.len());

    // Phase 1: walk the structural row vector once and decode each row at the
    // final typed boundary.
    for row in rows {
        let (data_key, raw_row) = row;
        let (expected_key, entity) = decode_raw_row_for_entity_key::<E>(&data_key, &raw_row)?;
        decoded_rows.push(Row::new(Id::from_key(expected_key), entity));
    }

    Ok(EntityResponse::new(decoded_rows))
}

/// Decode persisted data rows into one typed cursor page at the final typed boundary.
pub(in crate::db) fn decode_data_rows_into_cursor_page<E>(
    rows: Vec<DataRow>,
    next_cursor: Option<PageCursor>,
) -> Result<CursorPage<E>, InternalError>
where
    E: PersistedRow + EntityValue,
{
    Ok(CursorPage {
        items: decode_data_rows_into_entity_response::<E>(rows)?,
        next_cursor,
    })
}

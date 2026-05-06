//! Module: executor::terminal::typed_response
//! Responsibility: final typed response and cursor-page materialization.
//! Does not own: row-byte decoding, route execution, or pagination policy.
//! Boundary: executor terminal code consumes structural data rows and produces public response DTOs.

use crate::{
    db::{
        PersistedRow,
        data::{DataKey, DataRow, RawRow, decode_raw_row_for_entity_key_with_structural_contract},
        executor::{CursorPage, PageCursor, terminal::RowLayout},
        response::{EntityResponse, Row},
    },
    error::InternalError,
    traits::EntityValue,
    types::Id,
};

/// Decode persisted data rows into one typed entity response through one structural loop.
pub(in crate::db::executor) fn decode_data_rows_into_entity_response<E>(
    row_layout: &RowLayout,
    rows: Vec<DataRow>,
) -> Result<EntityResponse<E>, InternalError>
where
    E: PersistedRow + EntityValue,
{
    let mut decoded_rows = Vec::with_capacity(rows.len());

    // Phase 1: walk the structural row vector once and decode each row at the
    // final typed boundary.
    for row in rows {
        decoded_rows.push(decode_data_row_into_response_row::<E>(row_layout, row)?);
    }

    Ok(EntityResponse::new(decoded_rows))
}

// Decode one structural data row into one typed entity.
//
// Current-layout rows stay on the ordinary generated typed decoder. If that
// fails, accepted-schema plans get one structural normalization attempt so old
// append-only nullable rows can still cross the public typed response boundary.
pub(in crate::db::executor) fn decode_data_row_entity_with_layout<E>(
    row_layout: &RowLayout,
    data_key: &DataKey,
    raw_row: &RawRow,
) -> Result<(E::Key, E), InternalError>
where
    E: PersistedRow + EntityValue,
{
    decode_raw_row_for_entity_key_with_structural_contract::<E>(
        data_key,
        raw_row,
        row_layout.contract().clone(),
    )
}

// Decode one structural data row into one typed response row.
fn decode_data_row_into_response_row<E>(
    row_layout: &RowLayout,
    row: DataRow,
) -> Result<Row<E>, InternalError>
where
    E: PersistedRow + EntityValue,
{
    let (data_key, raw_row) = row;
    let (expected_key, entity) =
        decode_data_row_entity_with_layout::<E>(row_layout, &data_key, &raw_row)?;

    Ok(Row::new(Id::from_key(expected_key), entity))
}

/// Decode persisted data rows into one typed cursor page at the final typed boundary.
pub(in crate::db::executor) fn decode_data_rows_into_cursor_page<E>(
    row_layout: &RowLayout,
    rows: Vec<DataRow>,
    next_cursor: Option<PageCursor>,
) -> Result<CursorPage<E>, InternalError>
where
    E: PersistedRow + EntityValue,
{
    Ok(CursorPage {
        items: decode_data_rows_into_entity_response::<E>(row_layout, rows)?,
        next_cursor,
    })
}

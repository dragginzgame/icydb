//! Module: data::entity_decode
//! Responsibility: shared entity decode + key-consistency checks.
//! Does not own: storage byte decoding, commit policy, or query errors.
//! Boundary: data helpers used by store/executor decode paths.

use crate::{
    db::{
        data::{DataKey, DataRow, PersistedRow, RawRow},
        executor::CursorPage,
        executor::PageCursor,
        response::{EntityResponse, Row},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
};
use std::fmt::Display;

/// Decode one persisted `(DataKey, RawRow)` pair into one typed entity row.
///
/// This is the shared typed edge for callers that already separated row
/// ownership from executor/runtime traversal and only need canonical decode
/// diagnostics plus key-consistency validation.
pub(in crate::db) fn decode_raw_row_for_entity_key<E>(
    data_key: &DataKey,
    raw_row: &RawRow,
) -> Result<(E::Key, E), InternalError>
where
    E: PersistedRow + EntityValue,
{
    // Phase 1: recover the expected typed key from structural row identity.
    let expected_key = data_key.try_key::<E>()?;

    // Phase 2: decode entity bytes and classify persisted decode failures once.
    let entity = RawRow::try_decode::<E>(raw_row).map_err(|err| {
        InternalError::serialize_corruption(decode_failure_message(data_key, err))
    })?;

    // Phase 3: enforce key consistency before returning the typed row.
    let actual_key = entity.id().key();
    if expected_key != actual_key {
        let expected = format_entity_key_for_mismatch::<E>(expected_key);
        let found = format_entity_key_for_mismatch::<E>(actual_key);
        return Err(InternalError::store_corruption(key_mismatch_message(
            expected, found,
        )));
    }

    Ok((expected_key, entity))
}

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

// Build the canonical row-decode failure message for one persisted row.
fn decode_failure_message(data_key: &DataKey, err: impl Display) -> String {
    format!("failed to deserialize row: {data_key} ({err})")
}

// Build the canonical row-key mismatch message for one persisted row.
fn key_mismatch_message(expected: impl Display, actual: impl Display) -> String {
    format!("row key mismatch: expected {expected}, found {actual}")
}

/// Format an entity key for mismatch diagnostics using canonical `DataKey`
/// formatting when possible, and `Debug` fallback otherwise.
pub(in crate::db) fn format_entity_key_for_mismatch<E>(key: E::Key) -> String
where
    E: EntityKind,
    E::Key: std::fmt::Debug,
{
    // Prefer canonical DataKey formatting when key encoding is available.
    // Fall back to Debug so mismatch diagnostics remain informative.
    DataKey::try_new::<E>(key).map_or_else(|_| format!("{key:?}"), |key| key.to_string())
}

//! Module: data::entity_decode
//! Responsibility: shared entity decode + key-consistency checks.
//! Does not own: storage byte decoding, commit policy, or query errors.
//! Boundary: data helpers used by store/executor decode paths.

use crate::{
    db::{
        data::{DataKey, DataRow, RawRow},
        response::{EntityResponse, Row},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
};
use std::{fmt::Display, mem::ManuallyDrop};

///
/// PersistedEntityRow
///
/// PersistedEntityRow is the structural persisted-row envelope shared by
/// executor decode boundaries.
/// It owns the authoritative `DataKey` plus raw row bytes so typed callers can
/// keep only entity-key extraction and typed decode at the outer shell.
///

#[derive(Debug)]
pub(in crate::db) struct PersistedEntityRow {
    data_key: DataKey,
    raw_row: RawRow,
}

impl PersistedEntityRow {
    /// Build one owned persisted-row envelope from a `(DataKey, RawRow)` pair.
    #[must_use]
    pub(in crate::db) fn from_data_row(row: DataRow) -> Self {
        let (data_key, raw_row) = row;

        Self { data_key, raw_row }
    }

    /// Return the owned row parts after structural decode work completes.
    #[must_use]
    pub(in crate::db) fn into_parts(self) -> (DataKey, RawRow) {
        (self.data_key, self.raw_row)
    }
}

///
/// ErasedEntityResponseBuilder
///
/// ErasedEntityResponseBuilder owns the type-erased row buffer used by the
/// shared `Vec<DataRow> -> EntityResponse<E>` decode loop.
/// It keeps the generic-free row iteration in one place while typed leaf
/// functions perform entity decode and row construction at the edge.
///

struct ErasedEntityResponseBuilder {
    state: *mut (),
    push_row: unsafe fn(*mut (), DataRow) -> Result<(), InternalError>,
    drop_state: unsafe fn(*mut ()),
}

impl ErasedEntityResponseBuilder {
    // Allocate one erased typed row buffer for one concrete entity response.
    fn new<E>(capacity: usize) -> Self
    where
        E: EntityKind + EntityValue,
    {
        let rows = Vec::<Row<E>>::with_capacity(capacity);

        Self {
            state: Box::into_raw(Box::new(rows)).cast(),
            push_row: push_decoded_entity_row::<E>,
            drop_state: drop_entity_response_state::<E>,
        }
    }

    // Push one decoded typed row through the builder leaf vtable.
    fn push_row(&mut self, row: DataRow) -> Result<(), InternalError> {
        // SAFETY:
        // - the erased state was allocated by `Self::new::<E>`
        // - `push_row` was paired with the same concrete `E`
        unsafe { (self.push_row)(self.state, row) }
    }

    // Finish one typed entity response and reclaim the owned erased row buffer.
    fn finish<E>(self) -> EntityResponse<E>
    where
        E: EntityKind + EntityValue,
    {
        let this = ManuallyDrop::new(self);

        // SAFETY:
        // - the builder state was allocated as `Vec<Row<E>>` in `Self::new::<E>`
        // - the caller finishes with the same concrete `E` used at construction
        let rows = unsafe { *Box::from_raw(this.state.cast::<Vec<Row<E>>>()) };

        EntityResponse::new(rows)
    }
}

impl Drop for ErasedEntityResponseBuilder {
    fn drop(&mut self) {
        // SAFETY:
        // - `drop_state` matches the concrete allocation created in `Self::new::<E>`
        // - `finish` suppresses this drop path via `ManuallyDrop`
        unsafe { (self.drop_state)(self.state) };
    }
}

/// Decode one persisted `(DataKey, RawRow)` pair into one typed entity row.
///
/// This is the shared typed edge for callers that already separated row
/// ownership from executor/runtime traversal and only need canonical decode
/// diagnostics plus key-consistency validation.
#[inline(never)]
pub(in crate::db) fn decode_raw_row_for_entity_key<E>(
    data_key: &DataKey,
    raw_row: &RawRow,
) -> Result<(E::Key, E), InternalError>
where
    E: EntityKind + EntityValue,
{
    let expected_key = data_key.try_key::<E>()?;
    let entity = decode_and_validate_entity_key::<E, _, _, _, _>(
        expected_key,
        || RawRow::try_decode::<E>(raw_row),
        |err| InternalError::serialize_corruption(decode_failure_message(data_key, err)),
        |expected_key, actual_key| {
            let expected = format_entity_key_for_mismatch::<E>(expected_key);
            let found = format_entity_key_for_mismatch::<E>(actual_key);

            InternalError::store_corruption(key_mismatch_message(expected, found))
        },
    )?;

    Ok((expected_key, entity))
}

/// Decode one entity and enforce key consistency against an expected key.
///
/// Callers provide decode and error-formatting closures so boundary-specific
/// diagnostics and error classes remain unchanged.
#[inline(never)]
pub(in crate::db) fn decode_and_validate_entity_key<
    E,
    DecodeFn,
    DecodeErr,
    DecodeErrMap,
    MismatchErrMap,
>(
    expected_key: E::Key,
    decode_entity: DecodeFn,
    map_decode_error: DecodeErrMap,
    map_key_mismatch: MismatchErrMap,
) -> Result<E, InternalError>
where
    E: EntityKind + EntityValue,
    DecodeFn: FnOnce() -> Result<E, DecodeErr>,
    DecodeErrMap: FnOnce(DecodeErr) -> InternalError,
    MismatchErrMap: FnOnce(E::Key, E::Key) -> InternalError,
{
    // Phase 1: decode row bytes using caller-owned error mapping.
    let entity = decode_entity().map_err(map_decode_error)?;

    // Phase 2: enforce expected-key identity before returning the entity.
    ensure_entity_key_match::<E, _>(expected_key, entity.id().key(), map_key_mismatch)?;

    Ok(entity)
}

/// Decode one persisted row into one typed response row.
#[inline(never)]
pub(in crate::db) fn decode_data_row_into_entity_row<E>(
    row: DataRow,
) -> Result<Row<E>, InternalError>
where
    E: EntityKind + EntityValue,
{
    let row = PersistedEntityRow::from_data_row(row);
    let (data_key, raw_row) = row.into_parts();
    let (expected_key, entity) = decode_raw_row_for_entity_key::<E>(&data_key, &raw_row)?;

    Ok(Row::new(Id::from_key(expected_key), entity))
}

/// Decode persisted data rows into one typed entity response through one structural loop.
#[inline(never)]
pub(in crate::db) fn decode_data_rows_into_entity_response<E>(
    rows: Vec<DataRow>,
) -> Result<EntityResponse<E>, InternalError>
where
    E: EntityKind + EntityValue,
{
    let mut builder = ErasedEntityResponseBuilder::new::<E>(rows.len());

    // Phase 1: walk the structural row vector once without a generic loop body.
    for row in rows {
        builder.push_row(row)?;
    }

    Ok(builder.finish::<E>())
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

// Enforce expected-vs-actual entity key equality and delegate mismatch mapping.
fn ensure_entity_key_match<E, MismatchErrMap>(
    expected_key: E::Key,
    actual_key: E::Key,
    map_key_mismatch: MismatchErrMap,
) -> Result<(), InternalError>
where
    E: EntityKind,
    MismatchErrMap: FnOnce(E::Key, E::Key) -> InternalError,
{
    if expected_key != actual_key {
        return Err(map_key_mismatch(expected_key, actual_key));
    }

    Ok(())
}

// Decode one persisted row into one typed response row and append it to the
// erased response-state buffer selected by the builder vtable.
unsafe fn push_decoded_entity_row<E>(state: *mut (), row: DataRow) -> Result<(), InternalError>
where
    E: EntityKind + EntityValue,
{
    let row = decode_data_row_into_entity_row::<E>(row)?;

    // SAFETY:
    // - `state` originates from `ErasedEntityResponseBuilder::new::<E>`
    // - this leaf is only paired with that same concrete `E`
    let rows = unsafe { &mut *state.cast::<Vec<Row<E>>>() };
    rows.push(row);

    Ok(())
}

// Drop one erased typed response-state buffer when decode aborts before finish.
unsafe fn drop_entity_response_state<E>(state: *mut ())
where
    E: EntityKind + EntityValue,
{
    // SAFETY:
    // - `state` originates from `ErasedEntityResponseBuilder::new::<E>`
    // - this drop leaf is only paired with that same concrete `E`
    drop(unsafe { Box::from_raw(state.cast::<Vec<Row<E>>>()) });
}

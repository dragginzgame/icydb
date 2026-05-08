//! Module: data::entity_decode
//! Responsibility: shared entity decode + key-consistency checks.
//! Does not own: storage byte decoding, commit policy, or query errors.
//! Boundary: data helpers used by store/executor decode paths.

use crate::{
    db::data::{DataKey, PersistedRow, RawRow, StructuralRowContract, StructuralSlotReader},
    error::InternalError,
    traits::{EntityKind, EntityValue},
};
use std::fmt::Display;

/// Decode one persisted `(DataKey, RawRow)` pair through an explicit row contract.
///
/// This is the accepted-schema counterpart to the generated typed edge above.
/// Callers pass the already-selected structural row contract so typed
/// materialization can consume saved-schema field facts instead of reopening
/// `E::MODEL` for row-shape authority.
pub(in crate::db) fn decode_raw_row_for_entity_key_with_contract<E>(
    data_key: &DataKey,
    raw_row: &RawRow,
    contract: StructuralRowContract,
) -> Result<(E::Key, E), InternalError>
where
    E: PersistedRow + EntityValue,
{
    // Phase 1: recover the expected typed key from structural row identity.
    let expected_key = data_key.try_key::<E>()?;

    // Phase 2: decode entity bytes through the caller-selected structural row
    // contract and classify persisted decode failures once.
    let mut slots = StructuralSlotReader::from_raw_row_with_validated_contract(raw_row, contract)
        .map_err(|err| {
        InternalError::serialize_corruption(decode_failure_message(data_key, err))
    })?;
    let entity = E::materialize_from_slots(&mut slots).map_err(|err| {
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

// Build the canonical row-decode failure message for one persisted row.
fn decode_failure_message(data_key: &DataKey, err: impl Display) -> String {
    format!("failed to deserialize row: {data_key} ({err})")
}

// Build the canonical row-key mismatch message for one persisted row.
fn key_mismatch_message(expected: impl Display, actual: impl Display) -> String {
    format!("row key mismatch: expected {expected}, found {actual}")
}

// Format an entity key for mismatch diagnostics using canonical `DataKey`
// formatting when possible, and `Debug` fallback otherwise.
fn format_entity_key_for_mismatch<E>(key: E::Key) -> String
where
    E: EntityKind,
    E::Key: std::fmt::Debug,
{
    // Prefer canonical DataKey formatting when key encoding is available.
    // Fall back to Debug so mismatch diagnostics remain informative.
    DataKey::try_new::<E>(key).map_or_else(|_| format!("{key:?}"), |key| key.to_string())
}

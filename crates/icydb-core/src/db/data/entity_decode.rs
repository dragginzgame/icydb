//! Module: data::entity_decode
//! Responsibility: shared entity decode + key-consistency checks.
//! Does not own: storage byte decoding, commit policy, or query errors.
//! Boundary: data helpers used by store/executor decode paths.

use crate::{
    db::data::{
        DecodedDataStoreKey, PersistedRow, RawRow, StructuralRowContract, StructuralSlotReader,
    },
    error::InternalError,
};

/// Decode one persisted `(DecodedDataStoreKey, RawRow)` pair through an explicit row contract.
///
/// This is the accepted-schema counterpart to the generated typed edge above.
/// Callers pass the already-selected structural row contract so typed
/// materialization can consume saved-schema field facts instead of reopening
/// `E::MODEL` for row-shape authority.
pub(in crate::db) fn decode_raw_row_for_entity_key_with_contract<E>(
    data_key: &DecodedDataStoreKey,
    raw_row: &RawRow,
    contract: StructuralRowContract,
) -> Result<(E::Key, E), InternalError>
where
    E: PersistedRow,
{
    // Phase 1: recover the expected typed key from structural row identity.
    let expected_key = data_key.try_key::<E>()?;

    // Phase 2: decode entity bytes through the caller-selected structural row
    // contract and classify persisted decode failures once.
    let mut slots = StructuralSlotReader::from_raw_row_with_validated_contract(raw_row, contract)
        .map_err(|_| InternalError::persisted_row_decode_corruption())?;
    let entity = E::materialize_from_slots(&mut slots)
        .map_err(|_| InternalError::persisted_row_decode_corruption())?;

    // Phase 3: enforce key consistency before returning the typed row.
    let actual_key = entity.id().key();
    if expected_key != actual_key {
        return Err(InternalError::persisted_row_key_mismatch());
    }

    Ok((expected_key, entity))
}

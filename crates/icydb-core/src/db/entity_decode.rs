use crate::{
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

/// Decode one entity and enforce key consistency against an expected key.
///
/// Callers provide decode and error-formatting closures so boundary-specific
/// diagnostics and error classes remain unchanged.
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
    let entity = decode_entity().map_err(map_decode_error)?;
    ensure_entity_key_match::<E, _>(expected_key, entity.id().key(), map_key_mismatch)?;

    Ok(entity)
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

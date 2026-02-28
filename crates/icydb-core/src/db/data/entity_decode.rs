//! Module: data::entity_decode
//! Responsibility: shared entity decode + key-consistency checks.
//! Does not own: storage byte decoding, commit policy, or query errors.
//! Boundary: data helpers used by store/executor decode paths.

use crate::{
    db::data::DataKey,
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
    // Phase 1: decode row bytes using caller-owned error mapping.
    let entity = decode_entity().map_err(map_decode_error)?;

    // Phase 2: enforce expected-key identity before returning the entity.
    ensure_entity_key_match::<E, _>(expected_key, entity.id().key(), map_key_mismatch)?;

    Ok(entity)
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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        error::{ErrorClass, ErrorOrigin, InternalError},
        model::field::FieldKind,
        traits::EntityValue,
        types::Ulid,
    };
    use icydb_derive::FieldProjection;
    use serde::{Deserialize, Serialize};

    crate::test_canister! {
        ident = TestCanister,
        commit_memory_id = crate::testing::test_commit_memory_id(),
    }

    crate::test_store! {
        ident = TestStore,
        canister = TestCanister,
    }

    #[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq, Serialize)]
    struct ProbeEntity {
        id: Ulid,
    }

    crate::test_entity_schema! {
        ident = ProbeEntity,
        id = Ulid,
        id_field = id,
        entity_name = "ProbeEntity",
        primary_key = "id",
        pk_index = 0,
        fields = [("id", FieldKind::Ulid)],
        indexes = [],
        store = TestStore,
        canister = TestCanister,
    }

    #[test]
    fn decode_and_validate_entity_key_returns_entity_on_success() {
        let expected = Ulid::from_u128(1);
        let entity = decode_and_validate_entity_key::<ProbeEntity, _, _, _, _>(
            expected,
            || Ok::<ProbeEntity, &'static str>(ProbeEntity { id: expected }),
            |_: &'static str| unreachable!("decode-error mapping must not run on success"),
            |_, _| unreachable!("mismatch mapping must not run on success"),
        )
        .expect("success path should return decoded entity");

        assert_eq!(entity.id().key(), expected);
    }

    #[test]
    fn decode_and_validate_entity_key_maps_decode_failure() {
        let expected = Ulid::from_u128(2);
        let err = decode_and_validate_entity_key::<ProbeEntity, _, _, _, _>(
            expected,
            || Err("decode_failed"),
            |source| InternalError::serialize_corruption(format!("decode map: {source}")),
            |_, _| unreachable!("mismatch mapping must not run on decode failure"),
        )
        .expect_err("decode failure must map into InternalError");

        assert_eq!(err.class, ErrorClass::Corruption);
        assert_eq!(err.origin, ErrorOrigin::Serialize);
        assert_eq!(err.message, "decode map: decode_failed");
    }

    #[test]
    fn decode_and_validate_entity_key_maps_key_mismatch() {
        let expected = Ulid::from_u128(3);
        let actual = Ulid::from_u128(4);
        let err = decode_and_validate_entity_key::<ProbeEntity, _, _, _, _>(
            expected,
            || Ok::<ProbeEntity, &'static str>(ProbeEntity { id: actual }),
            |_: &'static str| {
                unreachable!("decode-error mapping must not run for successful decode")
            },
            |expected_key, actual_key| {
                InternalError::store_corruption(format!(
                    "mismatch: {expected_key:?} != {actual_key:?}"
                ))
            },
        )
        .expect_err("key mismatch must map into InternalError");

        assert_eq!(err.class, ErrorClass::Corruption);
        assert_eq!(err.origin, ErrorOrigin::Store);
        assert_eq!(err.message, format!("mismatch: {expected:?} != {actual:?}"));
    }
}

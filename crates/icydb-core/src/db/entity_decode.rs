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
    let entity = decode_entity().map_err(map_decode_error)?;
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
        model::{
            entity::EntityModel,
            field::{FieldKind, FieldModel},
            index::IndexModel,
        },
        test_fixtures::entity_model_from_static,
        traits::{
            AsView, CanisterKind, EntityIdentity, EntityKey, EntityKind, EntityPlacement,
            EntitySchema, EntityValue, Path, SanitizeAuto, SanitizeCustom, StoreKind, ValidateAuto,
            ValidateCustom, Visitable,
        },
        types::{Id, Ulid},
    };
    use icydb_derive::FieldValues;
    use serde::{Deserialize, Serialize};

    struct TestCanister;

    impl Path for TestCanister {
        const PATH: &'static str = "entity_decode_tests::TestCanister";
    }

    impl CanisterKind for TestCanister {}

    struct TestStore;

    impl Path for TestStore {
        const PATH: &'static str = "entity_decode_tests::TestStore";
    }

    impl StoreKind for TestStore {
        type Canister = TestCanister;
    }

    #[derive(Clone, Debug, Default, Deserialize, FieldValues, PartialEq, Serialize)]
    struct ProbeEntity {
        id: Ulid,
    }

    impl AsView for ProbeEntity {
        type ViewType = Self;

        fn as_view(&self) -> Self::ViewType {
            self.clone()
        }

        fn from_view(view: Self::ViewType) -> Self {
            view
        }
    }

    impl SanitizeAuto for ProbeEntity {}
    impl SanitizeCustom for ProbeEntity {}
    impl ValidateAuto for ProbeEntity {}
    impl ValidateCustom for ProbeEntity {}
    impl Visitable for ProbeEntity {}

    impl Path for ProbeEntity {
        const PATH: &'static str = "entity_decode_tests::ProbeEntity";
    }

    impl EntityKey for ProbeEntity {
        type Key = Ulid;
    }

    impl EntityIdentity for ProbeEntity {
        const ENTITY_NAME: &'static str = "ProbeEntity";
        const PRIMARY_KEY: &'static str = "id";
    }

    static PROBE_FIELDS: [FieldModel; 1] = [FieldModel {
        name: "id",
        kind: FieldKind::Ulid,
    }];
    static PROBE_FIELD_NAMES: [&str; 1] = ["id"];
    static PROBE_INDEXES: [&IndexModel; 0] = [];
    static PROBE_MODEL: EntityModel = entity_model_from_static(
        "entity_decode_tests::ProbeEntity",
        "ProbeEntity",
        &PROBE_FIELDS[0],
        &PROBE_FIELDS,
        &PROBE_INDEXES,
    );

    impl EntitySchema for ProbeEntity {
        const MODEL: &'static EntityModel = &PROBE_MODEL;
        const FIELDS: &'static [&'static str] = &PROBE_FIELD_NAMES;
        const INDEXES: &'static [&'static IndexModel] = &PROBE_INDEXES;
    }

    impl EntityPlacement for ProbeEntity {
        type Store = TestStore;
        type Canister = TestCanister;
    }

    impl EntityKind for ProbeEntity {}

    impl EntityValue for ProbeEntity {
        fn id(&self) -> Id<Self> {
            Id::from_key(self.id)
        }
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

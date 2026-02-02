use crate::prelude::*;

///
/// StoreTestEntity
///

#[entity(
    store = "TestDataStore",
    pk = "id",
    fields(field(ident = "id", value(item(prim = "Nat64"))))
)]
pub struct StoreTestEntity {}

impl StoreTestEntity {
    #[must_use]
    pub fn new(id: u64) -> Self {
        Self {
            id,
            ..Default::default()
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::{
        __internal::core::{
            db::store::{DataKey, RawDataKey, RawRow, RowDecodeError},
            traits::Storable,
        },
        serialize,
    };
    use std::borrow::Cow;

    #[test]
    fn raw_data_key_roundtrip_via_bytes() {
        let data_key = DataKey::try_new::<StoreTestEntity>(42).unwrap();
        let raw = data_key.to_raw().expect("data key encode");
        let bytes = raw.to_bytes();

        let decoded = RawDataKey::from_bytes(Cow::Borrowed(&bytes));
        let decoded_key = DataKey::try_from_raw(&decoded).unwrap();

        assert_eq!(decoded_key, data_key);
    }

    #[test]
    fn raw_row_roundtrip_via_bytes() {
        let entity = StoreTestEntity::new(42);
        let bytes = serialize(&entity).unwrap();
        let row = RawRow::try_new(bytes).unwrap();
        let decoded = row.try_decode::<StoreTestEntity>().unwrap();

        assert_eq!(decoded.id, entity.id);
    }

    #[test]
    fn raw_row_rejects_truncated_payload() {
        let entity = StoreTestEntity::new(7);
        let mut bytes = serialize(&entity).unwrap();
        bytes.pop();

        let row = RawRow::try_new(bytes).unwrap();
        let err = row.try_decode::<StoreTestEntity>().unwrap_err();

        assert!(matches!(err, RowDecodeError::Deserialize));
    }
}

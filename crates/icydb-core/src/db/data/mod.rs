//! Module: data
//! Responsibility: typed row-key and row-byte storage primitives.
//! Does not own: commit orchestration, query semantics, or relation validation.
//! Boundary: commit/executor -> data (one-way).

mod entity_decode;
mod key;
mod persisted_row;
mod row;
mod store;
mod structural_field;
mod structural_row;

// re-exports (Tier-3 → Tier-2 boundary)
pub(crate) use crate::value::{StorageKey, StorageKeyDecodeError, StorageKeyEncodeError};
pub(in crate::db) use entity_decode::{
    PersistedEntityRow, decode_data_rows_into_entity_response, decode_raw_row_for_entity_key,
};
pub(crate) use key::{DataKey, RawDataKey};
pub use persisted_row::{
    PersistedRow, PersistedScalar, ScalarSlotValueRef, ScalarValueRef, SlotReader, SlotWriter,
    UpdatePatch, decode_persisted_option_scalar_slot_payload, decode_persisted_scalar_slot_payload,
    decode_persisted_slot_payload, encode_persisted_option_scalar_slot_payload,
    encode_persisted_scalar_slot_payload, encode_persisted_slot_payload,
};
pub(in crate::db) use persisted_row::{
    SerializedUpdatePatch, StructuralSlotReader, apply_serialized_update_patch_to_raw_row,
    apply_update_patch_to_raw_row, decode_slot_value_by_contract,
    serialize_entity_slots_as_update_patch, serialize_update_patch_fields,
};
pub(crate) use row::{DataRow, RawRow};
pub use store::DataStore;
pub(in crate::db) use structural_field::{
    decode_relation_target_storage_keys_bytes, decode_storage_key_field_bytes,
    decode_structural_field_by_kind_bytes, decode_structural_value_storage_bytes,
};
pub(in crate::db) use structural_row::{
    StructuralRowDecodeError, StructuralRowFieldBytes, decode_structural_row_cbor,
};

#[cfg(test)]
macro_rules! impl_scalar_only_test_slot_reader_get_value {
    () => {
        fn get_value(
            &mut self,
            _slot: usize,
        ) -> Result<Option<crate::value::Value>, crate::error::InternalError> {
            panic!("scalar predicate test reader should not route through get_value")
        }
    };
}

#[cfg(test)]
pub(crate) use impl_scalar_only_test_slot_reader_get_value;

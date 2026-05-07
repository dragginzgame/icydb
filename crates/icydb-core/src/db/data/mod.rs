//! Module: data
//! Responsibility: typed row-key and row-byte storage primitives.
//! Does not own: commit orchestration, query semantics, or relation validation.
//! Boundary: commit/executor -> data (one-way).

mod entity_decode;
mod key;
mod persisted_row;
mod row;
mod storage;
mod store;
mod structural_field;
mod structural_row;

// re-exports (Tier-3 → Tier-2 boundary)
pub(in crate::db) use crate::value::{StorageKey, StorageKeyDecodeError, StorageKeyEncodeError};
pub(in crate::db) use entity_decode::decode_raw_row_for_entity_key;
pub(in crate::db) use key::DataKey;
pub(crate) use key::RawDataKey;
pub(in crate::db) use persisted_row::decode_runtime_value_from_accepted_field_contract;
pub(in crate::db) use persisted_row::{
    CanonicalSlotReader, FieldSlot, SerializedStructuralPatch, StructuralSlotReader,
    apply_serialized_structural_patch_to_raw_row,
    apply_serialized_structural_patch_to_raw_row_with_accepted_contract, canonical_row_from_entity,
    canonical_row_from_raw_row_with_accepted_decode_contract,
    canonical_row_from_raw_row_with_structural_contract, canonical_row_from_stored_raw_row,
    canonical_row_from_structural_slot_reader_with_accepted_contract,
    decode_dense_raw_row_with_contract, decode_sparse_indexed_raw_row_with_contract,
    decode_sparse_raw_row_with_contract, decode_sparse_required_slot_with_contract,
    materialize_entity_from_serialized_structural_patch,
    serialize_complete_structural_patch_fields_with_accepted_contract,
    serialize_entity_slots_as_complete_serialized_patch, serialize_structural_patch_fields,
    serialize_structural_patch_fields_with_accepted_contract,
};
pub use persisted_row::{
    PersistedRow, PersistedScalar, ScalarSlotValueRef, ScalarValueRef, SlotReader, SlotWriter,
    StructuralPatch, decode_persisted_many_slot_payload_by_meta,
    decode_persisted_option_scalar_slot_payload, decode_persisted_option_slot_payload_by_kind,
    decode_persisted_option_slot_payload_by_meta, decode_persisted_scalar_slot_payload,
    decode_persisted_slot_payload_by_kind, decode_persisted_slot_payload_by_meta,
    decode_persisted_structured_many_slot_payload, decode_persisted_structured_slot_payload,
    decode_slot_into_runtime_value, encode_persisted_many_slot_payload_by_meta,
    encode_persisted_option_scalar_slot_payload, encode_persisted_option_slot_payload_by_meta,
    encode_persisted_scalar_slot_payload, encode_persisted_slot_payload_by_kind,
    encode_persisted_slot_payload_by_meta, encode_persisted_structured_many_slot_payload,
    encode_persisted_structured_slot_payload, encode_runtime_value_into_slot,
};
pub(in crate::db) use row::CanonicalRow;
pub(in crate::db) use row::{DataRow, RawRow};
pub use store::DataStore;
pub(in crate::db) use structural_field::{
    FieldDecodeError, ValueStorageView, accepted_kind_supports_storage_key_binary, decode_enum,
    decode_relation_target_storage_keys_bytes, decode_storage_key_binary_value_bytes,
    decode_storage_key_field_bytes, decode_structural_field_by_accepted_kind_bytes,
    decode_structural_field_by_kind_bytes, decode_structural_value_storage_bytes,
    decode_value_storage_list_item_slices, decode_value_storage_map_entry_slices,
    decode_value_storage_text, encode_enum, encode_storage_key_binary_value_bytes,
    encode_structural_field_by_accepted_kind_bytes, encode_structural_field_by_kind_bytes,
    encode_structural_value_storage_bytes, encode_structural_value_storage_null_bytes,
    encode_value_storage_list_item_slices, encode_value_storage_map_entry_slices,
    encode_value_storage_text, supports_storage_key_binary_kind,
    validate_storage_key_binary_value_bytes, validate_structural_field_by_accepted_kind_bytes,
    validate_structural_field_by_kind_bytes, validate_structural_value_storage_bytes,
    value_storage_bytes_are_null,
};
pub(in crate::db::data) use structural_row::{
    SparseRequiredRowFieldBytes, StructuralRowDecodeError, StructuralRowFieldBytes,
};
pub(in crate::db) use structural_row::{
    StructuralFieldDecodeContract, StructuralRowContract, decode_structural_row_payload,
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
pub(in crate::db) use impl_scalar_only_test_slot_reader_get_value;
#[cfg(feature = "diagnostics")]
pub use persisted_row::{StructuralReadMetrics, with_structural_read_metrics};
#[cfg(all(test, not(feature = "diagnostics")))]
pub(crate) use persisted_row::{StructuralReadMetrics, with_structural_read_metrics};

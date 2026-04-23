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
    decode_data_rows_into_cursor_page, decode_data_rows_into_entity_response,
    decode_raw_row_for_entity_key,
};
pub(crate) use key::{DataKey, RawDataKey};
pub(in crate::db) use persisted_row::{
    CanonicalSlotReader, SerializedUpdatePatch, StructuralSlotReader,
    apply_serialized_update_patch_to_raw_row, canonical_row_from_entity,
    canonical_row_from_stored_raw_row, canonical_row_from_structural_slot_reader,
    decode_dense_raw_row_with_contract, decode_sparse_indexed_raw_row_with_contract,
    decode_sparse_raw_row_with_contract, decode_sparse_required_slot_with_contract,
    decode_sparse_required_slot_with_contract_and_fields,
    materialize_entity_from_serialized_update_patch,
    serialize_entity_slots_as_complete_serialized_patch, serialize_update_patch_fields,
};
pub use persisted_row::{
    PersistedRow, PersistedScalar, ScalarSlotValueRef, ScalarValueRef, SlotReader, SlotWriter,
    UpdatePatch, decode_persisted_custom_many_slot_payload, decode_persisted_custom_slot_payload,
    decode_persisted_non_null_slot_payload_by_kind, decode_persisted_option_scalar_slot_payload,
    decode_persisted_option_slot_payload_by_kind, decode_persisted_option_slot_payload_by_meta,
    decode_persisted_scalar_slot_payload, decode_persisted_slot_payload_by_kind,
    decode_persisted_slot_payload_by_meta, encode_persisted_custom_many_slot_payload,
    encode_persisted_custom_slot_payload, encode_persisted_option_scalar_slot_payload,
    encode_persisted_option_slot_payload_by_meta, encode_persisted_scalar_slot_payload,
    encode_persisted_slot_payload_by_kind, encode_persisted_slot_payload_by_meta,
};
pub(in crate::db) use row::{CanonicalRow, SelectiveRowRead};
pub(crate) use row::{DataRow, RawRow};
pub use store::DataStore;
pub(in crate::db) use structural_field::{
    decode_account, decode_blob_field_by_kind_bytes, decode_bool_field_by_kind_bytes,
    decode_date_field_by_kind_bytes, decode_decimal, decode_decimal_field_by_kind_bytes,
    decode_duration_field_by_kind_bytes, decode_enum, decode_float32_field_by_kind_bytes,
    decode_float64_field_by_kind_bytes, decode_int, decode_int_big_field_by_kind_bytes,
    decode_int128, decode_int128_field_by_kind_bytes, decode_list_field_items, decode_list_item,
    decode_map_entry, decode_map_field_entries, decode_nat, decode_nat128,
    decode_nat128_field_by_kind_bytes, decode_optional_storage_key_field_bytes,
    decode_relation_target_storage_keys_bytes, decode_storage_key_binary_value_bytes,
    decode_storage_key_field_bytes, decode_structural_field_by_kind_bytes,
    decode_structural_value_storage_blob_bytes, decode_structural_value_storage_bool_bytes,
    decode_structural_value_storage_bytes, decode_structural_value_storage_date_bytes,
    decode_structural_value_storage_duration_bytes, decode_structural_value_storage_float32_bytes,
    decode_structural_value_storage_float64_bytes, decode_structural_value_storage_i64_bytes,
    decode_structural_value_storage_principal_bytes,
    decode_structural_value_storage_subaccount_bytes,
    decode_structural_value_storage_timestamp_bytes, decode_structural_value_storage_u64_bytes,
    decode_structural_value_storage_ulid_bytes, decode_structural_value_storage_unit_bytes,
    decode_text, decode_text_field_by_kind_bytes, decode_uint_big_field_by_kind_bytes,
    encode_account, encode_blob_field_by_kind_bytes, encode_bool_field_by_kind_bytes,
    encode_date_field_by_kind_bytes, encode_decimal, encode_decimal_field_by_kind_bytes,
    encode_duration_field_by_kind_bytes, encode_enum, encode_float32_field_by_kind_bytes,
    encode_float64_field_by_kind_bytes, encode_int, encode_int_big_field_by_kind_bytes,
    encode_int128, encode_int128_field_by_kind_bytes, encode_list_field_items, encode_list_item,
    encode_map_entry, encode_map_field_entries, encode_nat, encode_nat128,
    encode_nat128_field_by_kind_bytes, encode_storage_key_binary_value_bytes,
    encode_storage_key_field_bytes, encode_structural_field_by_kind_bytes,
    encode_structural_value_storage_blob_bytes, encode_structural_value_storage_bool_bytes,
    encode_structural_value_storage_bytes, encode_structural_value_storage_date_bytes,
    encode_structural_value_storage_duration_bytes, encode_structural_value_storage_float32_bytes,
    encode_structural_value_storage_float64_bytes, encode_structural_value_storage_i64_bytes,
    encode_structural_value_storage_null_bytes, encode_structural_value_storage_principal_bytes,
    encode_structural_value_storage_subaccount_bytes,
    encode_structural_value_storage_timestamp_bytes, encode_structural_value_storage_u64_bytes,
    encode_structural_value_storage_ulid_bytes, encode_structural_value_storage_unit_bytes,
    encode_text, encode_text_field_by_kind_bytes, encode_uint_big_field_by_kind_bytes,
    structural_value_storage_bytes_are_null, supports_storage_key_binary_kind,
    validate_storage_key_binary_value_bytes, validate_structural_field_by_kind_bytes,
    validate_structural_value_storage_bytes,
};
pub(in crate::db) use structural_row::{
    SparseRequiredRowFieldBytes, StructuralRowContract, StructuralRowDecodeError,
    StructuralRowFieldBytes, decode_structural_row_payload,
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
#[cfg(feature = "diagnostics")]
pub use persisted_row::{StructuralReadMetrics, with_structural_read_metrics};
#[cfg(all(test, not(feature = "diagnostics")))]
pub(crate) use persisted_row::{StructuralReadMetrics, with_structural_read_metrics};

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
    materialize_entity_from_serialized_update_patch,
    serialize_entity_slots_as_complete_serialized_patch, serialize_update_patch_fields,
};
pub use persisted_row::{
    PersistedRow, PersistedScalar, ScalarSlotValueRef, ScalarValueRef, SlotReader, SlotWriter,
    UpdatePatch, decode_persisted_custom_many_slot_payload, decode_persisted_custom_slot_payload,
    decode_persisted_non_null_slot_payload, decode_persisted_option_scalar_slot_payload,
    decode_persisted_option_slot_payload, decode_persisted_scalar_slot_payload,
    decode_persisted_slot_payload, encode_persisted_custom_many_slot_payload,
    encode_persisted_custom_slot_payload, encode_persisted_option_scalar_slot_payload,
    encode_persisted_scalar_slot_payload, encode_persisted_slot_payload,
};
pub(in crate::db) use row::CanonicalRow;
pub(crate) use row::{DataRow, RawRow};
pub use store::DataStore;
pub(in crate::db) use structural_field::{
    decode_relation_target_storage_keys_bytes, decode_storage_key_field_bytes,
    decode_structural_field_by_kind_bytes, decode_structural_value_storage_bytes,
    validate_structural_field_by_kind_bytes, validate_structural_value_storage_bytes,
};
pub(in crate::db) use structural_row::{
    StructuralRowContract, StructuralRowDecodeError, StructuralRowFieldBytes,
    decode_structural_row_cbor,
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
#[cfg(feature = "structural-read-metrics")]
pub use persisted_row::{StructuralReadMetrics, with_structural_read_metrics};
#[cfg(all(test, not(feature = "structural-read-metrics")))]
pub(crate) use persisted_row::{StructuralReadMetrics, with_structural_read_metrics};

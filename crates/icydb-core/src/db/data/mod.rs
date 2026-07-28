//! Module: data
//! Responsibility: typed row-key and row-byte storage primitives.
//! Does not own: commit orchestration, query semantics, or relation validation.
//! Boundary: commit/executor -> data (one-way).

mod key;
mod persisted_row;
mod row;
mod storage;
mod store;
mod structural_field;
mod structural_row;

// re-exports (Tier-3 → Tier-2 boundary)
#[cfg(test)]
pub(in crate::db) use crate::db::key_taxonomy::PrimaryKeyComponent;
pub(crate) use crate::db::key_taxonomy::RawDataStoreKey;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use key::primary_key_value_from_structural_value;
pub(in crate::db) use key::{DecodedDataStoreKey, DecodedDataStoreKeyDecodeError};
#[cfg(feature = "sql")]
pub(in crate::db) use persisted_row::AcceptedFixedUpdatePatch;
pub(in crate::db) use persisted_row::decode_admitted_value_from_accepted_field_contract;
pub(in crate::db) use persisted_row::encode_accepted_value_ref_for_accepted_field_contract;
pub(in crate::db) use persisted_row::encode_canonical_value_for_accepted_field_contract;
#[cfg(feature = "sql")]
pub(in crate::db) use persisted_row::encode_input_value_for_accepted_field_contract;
pub(in crate::db) use persisted_row::validate_default_payload_for_accepted_field_contract;
pub(in crate::db) use persisted_row::{
    AcceptedFieldWriteProvenance, AcceptedMutationIntentPatch, CanonicalSlotReader, FieldSlot,
    StructuralSlotReader, canonical_row_from_raw_row_with_accepted_decode_contract,
    canonical_row_from_stored_raw_row,
    canonical_row_from_structural_slot_reader_with_accepted_contract,
    resolve_existing_replace_structural_patch_with_accepted_contract,
    resolve_insert_structural_patch_with_accepted_contract,
    resolve_update_structural_patch_with_accepted_contract,
};
pub(in crate::db) use persisted_row::{ScalarSlotValueRef, ScalarValueRef, SlotReader};
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use persisted_row::{
    decode_dense_raw_row_with_contract, decode_sparse_indexed_raw_row_with_contract,
    decode_sparse_required_slot_with_contract,
};
pub(in crate::db) use persisted_row::{
    decode_runtime_value_from_accepted_field_contract, decode_runtime_value_from_row_contract,
};
pub(in crate::db) use persisted_row::{
    decode_validated_check_literal_payload, encode_input_value_for_candidate_field_contract,
};
pub(in crate::db) use row::CanonicalRow;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use row::DataRow;
pub(in crate::db) use row::RawRow;
pub use store::DataStore;
pub(in crate::db) use store::StoreVisit;
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) use structural_field::FieldDecodeError;
#[cfg(test)]
pub(in crate::db) use structural_field::decode_canonical_value_storage_bytes;
pub(in crate::db) use structural_field::{
    ValueStorageView, accepted_kind_supports_primary_key_component_binary,
    decode_accepted_relation_target_primary_key_components_bytes,
    decode_structural_field_by_accepted_kind_bytes, decode_structural_value_storage_bytes,
    encode_structural_field_by_accepted_kind_bytes, encode_structural_value_storage_null_bytes,
    validate_structural_field_by_accepted_kind_bytes, validate_structural_value_storage_bytes,
    value_storage_bytes_are_null,
};
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::data) use structural_row::SparseRequiredRowFieldBytes;
pub(in crate::db::data) use structural_row::StructuralRowFieldBytes;
pub(in crate::db) use structural_row::{AcceptedStructuralRowAuthority, StructuralRowContract};

#[cfg(feature = "diagnostics")]
pub use persisted_row::{StructuralReadMetrics, with_structural_read_metrics};
#[cfg(all(test, not(feature = "diagnostics")))]
pub(crate) use persisted_row::{StructuralReadMetrics, with_structural_read_metrics};

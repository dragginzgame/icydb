//! Module: data
//! Responsibility: typed row-key and row-byte storage primitives.
//! Does not own: commit orchestration, query semantics, or relation validation.
//! Boundary: commit/executor -> data (one-way).

mod entity_decode;
mod key;
mod row;
mod store;
mod structural_field;
mod structural_row;

// re-exports (Tier-3 → Tier-2 boundary)
pub(crate) use crate::value::{StorageKey, StorageKeyDecodeError, StorageKeyEncodeError};
pub(in crate::db) use entity_decode::{
    PersistedEntityRow, decode_and_validate_entity_key, decode_data_rows_into_entity_response,
    decode_raw_row_for_entity_key,
};
pub(crate) use key::{DataKey, RawDataKey};
pub(crate) use row::{DataRow, RawRow};
pub use store::DataStore;
pub(in crate::db) use structural_field::decode_structural_field_value;
pub(in crate::db) use structural_row::{
    StructuralRowDecodeError, StructuralRowObject, StructuralRowSlots, decode_structural_row_cbor,
    unwrap_structural_row_cbor_tags,
};

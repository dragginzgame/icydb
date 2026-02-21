mod entity_decode;
mod key;
mod row;
mod storage_key;
mod store;

// re-exports (Tier-3 → Tier-2 boundary)
pub(in crate::db) use entity_decode::{
    decode_and_validate_entity_key, format_entity_key_for_mismatch,
};
pub(crate) use key::{DataKey, RawDataKey};
pub(crate) use row::{DataRow, MAX_ROW_BYTES, RawRow};
pub(crate) use storage_key::{StorageKey, StorageKeyEncodeError};
pub use store::DataStore;
